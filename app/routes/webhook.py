"""
Webhook Route - Receives and processes TradingView/LuxAlgo alerts.
This is the entry point for the entire trading pipeline.
"""
import json
import time
import asyncio
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request, BackgroundTasks
from sqlalchemy.orm import Session

from app.database import get_db, SessionLocal
from app.schemas import TradingViewAlert, SignalResponse
from app.services.signal_validator import SignalValidator
from app.config import WEBHOOK_SECRET, SYMBOL_ALIASES, get_desk_for_symbol
from app.models.signal import Signal

logger = logging.getLogger("TradingSystem.Webhook")
router = APIRouter()


@router.post("/webhook", response_model=SignalResponse)
async def receive_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Primary webhook endpoint for TradingView alerts.
    Pipeline: Receive → Validate → Log → Route to desk(s)
    """
    start_time = time.time()

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

    # ── 3. Parse into schema ──
    try:
        alert = TradingViewAlert(**payload)
    except Exception as e:
        logger.warning(f"Schema validation failed: {e}")
        # Still log the raw signal for debugging
        _log_invalid_signal(db, raw_text, str(e))
        return SignalResponse(
            status="rejected",
            is_valid=False,
            validation_errors=[str(e)],
            message="Payload schema validation failed",
        )

    # ── 4. Normalize symbol ──
    raw_symbol = alert.symbol
    if alert.exchange:
        raw_symbol = f"{alert.exchange}:{alert.symbol}"
    normalized = SYMBOL_ALIASES.get(raw_symbol, alert.symbol.replace("/", ""))

    # ── 5. Route to desk(s) ──
    desks = get_desk_for_symbol(normalized)

    # ── 6. Validate signal ──
    validator = SignalValidator()
    is_valid, errors = validator.validate(alert, normalized, desks)

    # ── 7. Determine direction from alert type ──
    direction = _extract_direction(alert.alert_type)

    # ── 8. Log to PostgreSQL ──
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
        processing_time_ms=int((time.time() - start_time) * 1000),
    )

    db.add(signal)
    db.commit()
    db.refresh(signal)

    # ── 9. Log result ──
    if is_valid:
        logger.info(
            f"SIGNAL #{signal.id} | {normalized} | {alert.alert_type} | "
            f"{direction} | Desks: {desks} | Price: {alert.price}"
        )

        # ── 10. Trigger Phase 2 pipeline in background ──
        # Only entry signals go through the full pipeline
        entry_types = {
            "bullish_confirmation", "bearish_confirmation",
            "bullish_plus", "bearish_plus",
            "contrarian_bullish", "contrarian_bearish",
            "confirmation_turn_plus",
        }
        if alert.alert_type in entry_types:
            background_tasks.add_task(
                _run_pipeline_background, signal.id
            )
            logger.info(f"Pipeline queued for signal #{signal.id}")

    else:
        logger.warning(
            f"REJECTED #{signal.id} | {normalized} | {alert.alert_type} | "
            f"Errors: {errors}"
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


def _extract_direction(alert_type: str) -> str | None:
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


async def _run_pipeline_background(signal_id: int):
    """Run the Phase 2 pipeline in a background task."""
    from app.services.pipeline import process_signal

    db = SessionLocal()
    try:
        result = await process_signal(signal_id, db)
        logger.info(
            f"Pipeline result for #{signal_id}: "
            f"{json.dumps({k: v.get('decision', 'N/A') for k, v in result.get('results', {}).items()})}"
        )
    except Exception as e:
        logger.error(f"Background pipeline error for #{signal_id}: {e}", exc_info=True)
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
        "received_at": signal.received_at.isoformat() if signal.received_at else None,
    }
