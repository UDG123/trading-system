"""
Webhook Route — Receives and processes TradingView/LuxAlgo alerts.
Entry point for the entire trading pipeline.

Latency tracking: Captures the delta between TradingView's alert timestamp
and our server arrival time. This feeds into the ML training data for
NovaQuant schema alignment.
"""
import json
import time
import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request, BackgroundTasks
from sqlalchemy.orm import Session

from app.database import get_db, SessionLocal
from app.schemas import TradingViewAlert, SignalResponse
from app.services.signal_validator import SignalValidator
from app.config import WEBHOOK_SECRET, SYMBOL_ALIASES, get_desk_for_symbol
from app.models.signal import Signal
from app.models.trade import Trade

logger = logging.getLogger("TradingSystem.Webhook")
router = APIRouter()


def _calculate_webhook_latency(tv_time_str) -> Optional[int]:
    """
    Calculate milliseconds between TradingView alert fire and server arrival.

    TradingView sends `{time}` as a UTC Unix timestamp string in SECONDS
    (e.g. "1710432000" or "1710432000.123"). Some LuxAlgo versions send
    milliseconds directly. We handle both.

    Returns:
        Latency in milliseconds, or None if TV time is missing/unparseable.
    """
    if not tv_time_str:
        return None

    try:
        arrival_ms = int(time.time() * 1000)
        tv_val = float(str(tv_time_str).strip())

        # Detect if value is in seconds or milliseconds
        # Unix seconds are ~1.7 billion, milliseconds are ~1.7 trillion
        if tv_val > 1e12:
            # Already in milliseconds
            tv_ms = int(tv_val)
        else:
            # Convert seconds to milliseconds
            tv_ms = int(tv_val * 1000)

        latency = arrival_ms - tv_ms

        # Sanity check: latency should be 0-60,000ms (0-60 seconds)
        # If negative or huge, the TV time is garbage
        if latency < -5000 or latency > 300_000:
            logger.debug(
                f"Webhook latency out of range: {latency}ms "
                f"(tv_time={tv_time_str}, arrival={arrival_ms})"
            )
            return None

        return max(0, latency)

    except (ValueError, TypeError) as e:
        logger.debug(f"Could not parse TV time '{tv_time_str}': {e}")
        return None


@router.post("/webhook", response_model=SignalResponse)
async def receive_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Primary webhook endpoint for TradingView alerts.
    Pipeline: Receive → Authenticate → Validate → Log → Route → Pipeline
    """
    # ── 0. Capture arrival time immediately ──
    arrival_time = time.time()

    # ── 1. Parse raw body ──
    try:
        raw_body = await request.body()
        raw_text = raw_body.decode("utf-8")
        payload = json.loads(raw_text)
    except json.JSONDecodeError:
        logger.warning(f"Invalid JSON received: {raw_body[:200]}")
        raise HTTPException(status_code=400, detail="Invalid JSON payload")
    except Exception as e:
        logger.error(f"Request parse error: {e}")
        raise HTTPException(status_code=400, detail="Could not read request body")

    # ── 2. Authenticate ──
    secret = payload.get("secret", "")
    if secret != WEBHOOK_SECRET:
        logger.warning(f"Auth failed from {request.client.host}")
        raise HTTPException(status_code=401, detail="Invalid webhook secret")

    # ── 3. Calculate webhook latency ──
    tv_time = payload.get("time")
    webhook_latency_ms = _calculate_webhook_latency(tv_time)

    # ── 4. Parse into schema ──
    try:
        alert = TradingViewAlert(**payload)
    except Exception as e:
        logger.warning(f"Schema validation failed: {e}")
        _log_invalid_signal(db, raw_text, str(e))
        return SignalResponse(
            status="rejected",
            is_valid=False,
            validation_errors=[str(e)],
            message="Payload schema validation failed",
        )

    # ── 5. Normalize symbol ──
    raw_symbol = alert.symbol
    if alert.exchange:
        raw_symbol = f"{alert.exchange}:{alert.symbol}"
    normalized = SYMBOL_ALIASES.get(raw_symbol, alert.symbol.replace("/", ""))

    # ── 6. Route to desk(s) ──
    desks = get_desk_for_symbol(normalized)

    # ── 7. Validate signal ──
    validator = SignalValidator()
    is_valid, errors = validator.validate(alert, normalized, desks)

    # ── 8. Determine direction from alert type ──
    direction = _extract_direction(alert.alert_type)

    # ── 9. Log to PostgreSQL ──
    processing_time_ms = int((time.time() - arrival_time) * 1000)

    signal = Signal(
        received_at=datetime.now(timezone.utc),
        source="tradingview",
        raw_payload=raw_text,
        symbol=alert.symbol,
        symbol_normalized=normalized,
        timeframe=alert.timeframe,
        alert_type=alert.alert_type,
        direction=direction,
        price=alert.price,
        tp1=alert.tp1,
        tp2=alert.tp2,
        sl1=alert.sl1,
        sl2=alert.sl2,
        smart_trail=alert.smart_trail,
        desk_id=desks[0] if len(desks) == 1 else None,
        desks_matched=desks,
        is_valid=is_valid,
        validation_errors=errors if errors else None,
        status="VALIDATED" if is_valid else "REJECTED",
        processing_time_ms=processing_time_ms,
        webhook_latency_ms=webhook_latency_ms,
    )

    db.add(signal)
    db.commit()
    db.refresh(signal)

    # ── 10. Log result ──
    latency_str = f"{webhook_latency_ms}ms" if webhook_latency_ms is not None else "N/A"

    if is_valid:
        logger.info(
            f"SIGNAL #{signal.id} | {normalized} | {alert.alert_type} | "
            f"{direction} | Desks: {desks} | Price: {alert.price} | "
            f"Latency: {latency_str} | Processed: {processing_time_ms}ms"
        )

        # ── 11. Trigger Phase 2 pipeline in background ──
        entry_types = {
            "bullish_confirmation", "bearish_confirmation",
            "bullish_plus", "bearish_plus",
            "bullish_confirmation_plus", "bearish_confirmation_plus",
            "contrarian_bullish", "contrarian_bearish",
            "confirmation_turn_plus",
            "confirmation_turn_bullish", "confirmation_turn_bearish",
        }
        if alert.alert_type in entry_types:
            background_tasks.add_task(
                _run_pipeline_background, signal.id, webhook_latency_ms
            )
            logger.info(f"Pipeline queued for signal #{signal.id}")

        # ── 12. Process EXIT signals — flag open trades for closure ──
        exit_types = {
            "bullish_exit", "bearish_exit",
            "take_profit", "stop_loss", "smart_trail_cross",
        }
        if alert.alert_type in exit_types and desks:
            background_tasks.add_task(
                _process_exit_signal, normalized, alert.alert_type, desks, signal.id
            )

    else:
        logger.warning(
            f"REJECTED #{signal.id} | {normalized} | {alert.alert_type} | "
            f"Errors: {errors} | Latency: {latency_str}"
        )

    return SignalResponse(
        status="accepted" if is_valid else "rejected",
        signal_id=signal.id,
        symbol=normalized,
        alert_type=alert.alert_type,
        desks_matched=desks,
        is_valid=is_valid,
        validation_errors=errors if errors else None,
        message=(
            f"Signal logged and routed to {len(desks)} desk(s)"
            if is_valid
            else f"Signal rejected: {'; '.join(errors or [])}"
        ),
    )


def _extract_direction(alert_type: str) -> Optional[str]:
    """Derive trade direction from alert type."""
    at = alert_type.lower()
    if "bullish" in at:
        return "LONG"
    if "bearish" in at:
        return "SHORT"
    if "exit" in at:
        return "EXIT"
    if at in ("take_profit", "stop_loss", "smart_trail_cross"):
        return "EXIT"
    return None


def _log_invalid_signal(db: Session, raw_text: str, error: str):
    """Log malformed signals for debugging."""
    try:
        signal = Signal(
            received_at=datetime.now(timezone.utc),
            source="tradingview",
            raw_payload=raw_text[:5000],
            symbol="UNKNOWN",
            symbol_normalized="UNKNOWN",
            timeframe="UNKNOWN",
            alert_type="PARSE_ERROR",
            is_valid=False,
            validation_errors=[error[:500]],
            status="REJECTED",
        )
        db.add(signal)
        db.commit()
    except Exception as e:
        logger.error(f"Failed to log invalid signal: {e}")


async def _run_pipeline_background(signal_id: int, webhook_latency_ms: int = None):
    """Run the Phase 2 pipeline in a background task.
    Passes webhook_latency_ms down so ML logger can capture it."""
    from app.services.pipeline import process_signal

    db = SessionLocal()
    try:
        result = await process_signal(
            signal_id, db,
            webhook_latency_ms=webhook_latency_ms,
        )
        logger.info(
            f"Pipeline result for #{signal_id}: "
            f"{json.dumps({k: v.get('decision', 'N/A') for k, v in result.get('results', {}).items()})}"
        )
    except Exception as e:
        logger.error(f"Background pipeline error for #{signal_id}: {e}", exc_info=True)
    finally:
        db.close()


async def _process_exit_signal(
    symbol: str, alert_type: str, desks: list, signal_id: int
):
    """
    When a LuxAlgo exit signal fires, flag matching open trades for closure.
    The EA polls /api/trades/exits and closes them on MT5.

    Logic:
    - bullish_exit → close LONG positions on this symbol
    - bearish_exit → close SHORT positions on this symbol
    - take_profit / stop_loss / smart_trail_cross → close ALL positions on this symbol
    """
    db = SessionLocal()
    try:
        # Determine which direction to close
        if "bullish_exit" in alert_type:
            close_direction = "LONG"
        elif "bearish_exit" in alert_type:
            close_direction = "SHORT"
        else:
            close_direction = None  # close both directions

        # Find matching open trades
        query = db.query(Trade).filter(
            Trade.symbol == symbol,
            Trade.status.in_(["EXECUTED", "OPEN"]),
        )

        if desks:
            query = query.filter(Trade.desk_id.in_(desks))

        if close_direction:
            query = query.filter(Trade.direction == close_direction)

        open_trades = query.all()

        if not open_trades:
            logger.debug(
                f"Exit signal #{signal_id} for {symbol} ({alert_type}): "
                f"no matching open trades to close"
            )
            return

        # Flag each for closure
        for trade in open_trades:
            trade.status = "CLOSE_REQUESTED"
            trade.close_reason = f"EXIT_{alert_type.upper()}"
            logger.info(
                f"EXIT FLAGGED | Trade #{trade.id} | {trade.symbol} {trade.direction} | "
                f"Ticket: {trade.mt5_ticket} | Reason: {alert_type} | Signal #{signal_id}"
            )

        db.commit()

        logger.info(
            f"Exit signal #{signal_id}: {len(open_trades)} trades flagged for closure "
            f"({symbol} {alert_type})"
        )

    except Exception as e:
        logger.error(f"Exit signal processing failed: {e}", exc_info=True)
    finally:
        db.close()


@router.get("/signal/{signal_id}")
async def get_signal_status(signal_id: int, db: Session = Depends(get_db)):
    """Get the current status and pipeline results for a signal."""
    signal = db.query(Signal).filter(Signal.id == signal_id).first()
    if not signal:
        raise HTTPException(status_code=404, detail="Signal not found")

    return {
        "id": signal.id,
        "symbol": signal.symbol_normalized,
        "alert_type": signal.alert_type,
        "direction": signal.direction,
        "price": signal.price,
        "status": signal.status,
        "desk_id": signal.desk_id,
        "desks_matched": signal.desks_matched,
        "ml_score": signal.ml_score,
        "consensus_score": signal.consensus_score,
        "claude_decision": signal.claude_decision,
        "claude_reasoning": signal.claude_reasoning,
        "position_size_pct": signal.position_size_pct,
        "processing_time_ms": signal.processing_time_ms,
        "webhook_latency_ms": signal.webhook_latency_ms,
        "received_at": signal.received_at.isoformat() if signal.received_at else None,
    }
