"""
Webhook Route - Receives TradingView/LuxAlgo alerts.
Entry point for the OniQuant trading pipeline.
"""
import json
import time
import re
import math
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

_NAN_RE = re.compile(r':\s*NaN\b', re.IGNORECASE)
_INF_RE = re.compile(r':\s*-?Infinity\b', re.IGNORECASE)


def _calc_latency(tv_time_str) -> Optional[int]:
    """Calculate ms between TV alert fire and server arrival."""
    if not tv_time_str:
        return None
    try:
        arrival_ms = int(time.time() * 1000)
        tv_val = float(str(tv_time_str).strip())
        tv_ms = int(tv_val) if tv_val > 1e12 else int(tv_val * 1000)
        latency = arrival_ms - tv_ms
        if latency < -5000 or latency > 300_000:
            return None
        return max(0, latency)
    except (ValueError, TypeError):
        return None


def _parse_body(raw: str) -> Optional[dict]:
    """Parse JSON, sanitizing NaN/Infinity if needed."""
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        pass
    try:
        cleaned = _NAN_RE.sub(': null', raw)
        cleaned = _INF_RE.sub(': null', cleaned)
        return json.loads(cleaned)
    except (json.JSONDecodeError, ValueError):
        pass
    return None


def _map_fields(p: dict) -> dict:
    """Map LuxAlgo field names to OniQuant schema."""
    if "ticker" in p and "symbol" not in p:
        p["symbol"] = p.pop("ticker")
    if "bartime" in p and "time" not in p:
        p["time"] = p.pop("bartime")
    if "alert_type" not in p:
        for k in ("alert", "signal", "type", "message", "condition"):
            if k in p:
                p["alert_type"] = p.pop(k)
                break
    if "timeframe" not in p:
        for k in ("interval", "tf"):
            if k in p:
                p["timeframe"] = p.pop(k)
                break
    if "ohlcv" in p and isinstance(p["ohlcv"], dict):
        ohlcv = p.pop("ohlcv")
        if "close" in ohlcv and "price" not in p:
            p["price"] = ohlcv["close"]
        if "volume" in ohlcv and "volume" not in p:
            p["volume"] = ohlcv["volume"]
    if "close" in p and "price" not in p:
        p["price"] = p.pop("close")
    p.pop("bar_color", None)
    return p


def _clean_na(p: dict) -> dict:
    """Convert 'na'/'NaN' strings to None for indicator fields."""
    for key in ("tp1", "tp2", "sl1", "sl2", "smart_trail"):
        val = p.get(key)
        if val is None:
            continue
        if isinstance(val, str):
            if val.lower() in ("nan", "na", "n/a", ""):
                p[key] = None
            else:
                try:
                    p[key] = float(val)
                except ValueError:
                    p[key] = None
        elif isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
            p[key] = None
    return p


def _direction(alert_type: str) -> Optional[str]:
    """Derive trade direction from alert type."""
    at = alert_type.lower()
    if "bullish" in at:
        return "LONG"
    if "bearish" in at:
        return "SHORT"
    if "exit" in at or at in ("take_profit", "stop_loss", "smart_trail_cross"):
        return "EXIT"
    return None


# ─── PATH-AUTHENTICATED ENDPOINT (for LuxAlgo S&O/PAC/OM) ───

@router.post("/webhook/{path_secret}", response_model=SignalResponse)
async def webhook_path_auth(
    path_secret: str,
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """LuxAlgo alerts - secret in URL path since alert() overrides message body."""
    if path_secret != WEBHOOK_SECRET:
        logger.warning(f"Path auth failed from {request.client.host}")
        raise HTTPException(status_code=401, detail="Invalid webhook secret")
    return await _handle(request, background_tasks, db, authed=True)


# ─── BODY-AUTHENTICATED ENDPOINT (for MSE) ───

@router.post("/webhook", response_model=SignalResponse)
async def webhook_body_auth(
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """MSE alerts - secret inside JSON body."""
    return await _handle(request, background_tasks, db, authed=False)


# ─── SHARED HANDLER ───

async def _handle(
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session,
    authed: bool = False,
):
    """Process any webhook: JSON, NaN JSON, or plain text."""
    arrival = time.time()

    # Read body
    try:
        raw = (await request.body()).decode("utf-8").strip()
    except Exception as e:
        logger.error(f"Read error: {e}")
        raise HTTPException(status_code=400, detail="Could not read request body")

    if not raw:
        raise HTTPException(status_code=400, detail="Empty body")

    logger.debug(f"Webhook ({len(raw)} chars): {raw[:300]}")

    # Parse
    payload = _parse_body(raw)

    if payload and isinstance(payload, dict):
        payload = _map_fields(payload)
        payload = _clean_na(payload)
    else:
        payload = {
            "symbol": request.query_params.get("symbol", "UNKNOWN"),
            "exchange": request.query_params.get("exchange", ""),
            "timeframe": request.query_params.get("timeframe", "60"),
            "alert_type": raw,
            "price": 0,
        }
        logger.info(f"Plain text wrapped: {raw[:100]}")

    # Auth
    if not authed:
        secret = payload.get("secret", "")
        q_secret = request.query_params.get("secret", "")
        if secret != WEBHOOK_SECRET and q_secret != WEBHOOK_SECRET:
            logger.warning(f"Auth failed from {request.client.host}")
            raise HTTPException(status_code=401, detail="Invalid webhook secret")

    payload["secret"] = WEBHOOK_SECRET

    # Latency
    latency_ms = _calc_latency(payload.get("time"))

    # Schema
    try:
        alert = TradingViewAlert(**payload)
    except Exception as e:
        logger.warning(f"Schema failed: {e}")
        _log_bad_signal(db, raw, str(e))
        return SignalResponse(
            status="rejected",
            is_valid=False,
            validation_errors=[str(e)],
            message="Schema validation failed",
        )

    # Normalize symbol
    raw_sym = alert.symbol
    if alert.exchange:
        raw_sym = f"{alert.exchange}:{alert.symbol}"
    normalized = SYMBOL_ALIASES.get(raw_sym, alert.symbol.replace("/", ""))

    # Route to desks
    desks = get_desk_for_symbol(normalized)
    if alert.desk and alert.desk != "auto":
        from app.config import DESKS
        if alert.desk in DESKS:
            desks = [alert.desk]

    # Validate
    validator = SignalValidator()
    is_valid, errors = validator.validate(alert, normalized, desks)

    # Direction
    direction = _direction(alert.alert_type)

    # Log to DB
    proc_ms = int((time.time() - arrival) * 1000)
    signal = Signal(
        received_at=datetime.now(timezone.utc),
        source="tradingview",
        raw_payload=raw,
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
        processing_time_ms=proc_ms,
        webhook_latency_ms=latency_ms,
    )
    db.add(signal)
    db.commit()
    db.refresh(signal)

    lat_str = f"{latency_ms}ms" if latency_ms is not None else "N/A"

    if is_valid:
        logger.info(
            f"SIGNAL #{signal.id} | {normalized} | {alert.alert_type} | "
            f"{direction} | Desks: {desks} | Price: {alert.price} | "
            f"Latency: {lat_str} | Proc: {proc_ms}ms"
        )

        entry_types = {
            "bullish_confirmation", "bearish_confirmation",
            "bullish_plus", "bearish_plus",
            "bullish_confirmation_plus", "bearish_confirmation_plus",
            "contrarian_bullish", "contrarian_bearish",
            "confirmation_turn_plus",
            "confirmation_turn_bullish", "confirmation_turn_bearish",
            "mse_bullish_confirmation", "mse_bearish_confirmation",
        }
        if alert.alert_type in entry_types:
            background_tasks.add_task(_run_pipeline, signal.id, latency_ms)
            logger.info(f"Pipeline queued for #{signal.id}")

        exit_types = {
            "bullish_exit", "bearish_exit",
            "take_profit", "stop_loss", "smart_trail_cross",
        }
        if alert.alert_type in exit_types and desks:
            background_tasks.add_task(
                _process_exit, normalized, alert.alert_type, desks, signal.id
            )
    else:
        logger.warning(
            f"REJECTED #{signal.id} | {normalized} | {alert.alert_type} | "
            f"Errors: {errors} | Latency: {lat_str}"
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


# ─── HELPERS ───

def _log_bad_signal(db: Session, raw: str, error: str):
    """Log malformed signals for debugging."""
    try:
        sig = Signal(
            received_at=datetime.now(timezone.utc),
            source="tradingview",
            raw_payload=raw[:5000],
            symbol="UNKNOWN",
            symbol_normalized="UNKNOWN",
            timeframe="UNKNOWN",
            alert_type="PARSE_ERROR",
            is_valid=False,
            validation_errors=[error[:500]],
            status="REJECTED",
        )
        db.add(sig)
        db.commit()
    except Exception as e:
        logger.error(f"Failed to log bad signal: {e}")


async def _run_pipeline(signal_id: int, latency_ms: int = None):
    """Run Phase 2 pipeline in background."""
    from app.services.pipeline import process_signal
    db = SessionLocal()
    try:
        result = await process_signal(signal_id, db, webhook_latency_ms=latency_ms)
        logger.info(
            f"Pipeline #{signal_id}: "
            f"{json.dumps({k: v.get('decision', 'N/A') for k, v in result.get('results', {}).items()})}"
        )
    except Exception as e:
        logger.error(f"Pipeline error #{signal_id}: {e}", exc_info=True)
    finally:
        db.close()


async def _process_exit(symbol: str, alert_type: str, desks: list, signal_id: int):
    """Flag open trades for closure when exit signal fires."""
    db = SessionLocal()
    try:
        if "bullish_exit" in alert_type:
            close_dir = "LONG"
        elif "bearish_exit" in alert_type:
            close_dir = "SHORT"
        else:
            close_dir = None

        query = db.query(Trade).filter(
            Trade.symbol == symbol,
            Trade.status.in_(["EXECUTED", "OPEN"]),
        )
        if desks:
            query = query.filter(Trade.desk_id.in_(desks))
        if close_dir:
            query = query.filter(Trade.direction == close_dir)

        trades = query.all()
        if not trades:
            return

        for t in trades:
            t.status = "CLOSE_REQUESTED"
            t.close_reason = f"EXIT_{alert_type.upper()}"
            logger.info(
                f"EXIT | Trade #{t.id} | {t.symbol} {t.direction} | "
                f"Ticket: {t.mt5_ticket} | {alert_type} | Signal #{signal_id}"
            )

        db.commit()
        logger.info(f"Exit #{signal_id}: {len(trades)} trades flagged ({symbol})")
    except Exception as e:
        logger.error(f"Exit processing failed: {e}", exc_info=True)
    finally:
        db.close()


@router.get("/signal/{signal_id}")
async def get_signal(signal_id: int, db: Session = Depends(get_db)):
    """Get signal status."""
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
