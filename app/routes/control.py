"""
Control Route — Kill switch, desk management, VPS status.
Accessible from /docs or Telegram. Independent of webhooks.
"""
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.config import DESKS
from app.models.desk_state import DeskState
from app.services.zmq_bridge import ZMQBridge
from app.services.telegram_bot import TelegramBot
from app.services.trade_reporter import TradeReporter

logger = logging.getLogger("TradingSystem.Control")
router = APIRouter()

# Shared instances
_zmq: ZMQBridge = None
_telegram: TelegramBot = None
_reporter: TradeReporter = None


def _get_zmq():
    global _zmq
    if _zmq is None:
        _zmq = ZMQBridge()
    return _zmq


def _get_telegram():
    global _telegram
    if _telegram is None:
        _telegram = TelegramBot()
    return _telegram


def _get_reporter():
    global _reporter
    if _reporter is None:
        _reporter = TradeReporter()
    return _reporter


@router.post("/kill-switch")
async def kill_switch(
    scope: str = "ALL",
    db: Session = Depends(get_db),
):
    """
    Emergency kill switch. Closes all positions and halts trading.
    scope: "ALL" for entire firm, or a desk ID like "DESK1_SCALPER"
    """
    zmq_bridge = _get_zmq()
    telegram = _get_telegram()

    # Notify via Telegram
    await telegram.alert_kill_switch(scope, "API")

    # Send to VPS
    zmq_result = await zmq_bridge.send_kill_switch(scope)

    # Update desk states
    if scope == "ALL":
        desks = db.query(DeskState).all()
        for state in desks:
            state.is_active = False
        db.commit()
    else:
        state = db.query(DeskState).filter(DeskState.desk_id == scope).first()
        if state:
            state.is_active = False
            db.commit()

    logger.warning(f"KILL SWITCH executed | Scope: {scope}")

    return {
        "status": "executed",
        "scope": scope,
        "vps_response": zmq_result,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@router.post("/desk/{desk_id}/pause")
async def pause_desk(
    desk_id: str,
    hours: int = 2,
    db: Session = Depends(get_db),
):
    """Pause a desk for N hours (default 2)."""
    if desk_id not in DESKS:
        raise HTTPException(status_code=404, detail=f"Unknown desk: {desk_id}")

    state = db.query(DeskState).filter(DeskState.desk_id == desk_id).first()
    if not state:
        state = DeskState(desk_id=desk_id, is_active=True)
        db.add(state)

    state.is_paused = True
    state.pause_until = datetime.now(timezone.utc) + timedelta(hours=hours)
    db.commit()

    telegram = _get_telegram()
    await telegram.send_message(
        f"⏸️ <b>{desk_id}</b> paused for {hours} hours via API."
    )

    return {
        "status": "paused",
        "desk_id": desk_id,
        "pause_until": state.pause_until.isoformat(),
    }


@router.post("/desk/{desk_id}/resume")
async def resume_desk(
    desk_id: str,
    db: Session = Depends(get_db),
):
    """Resume a paused desk."""
    if desk_id not in DESKS:
        raise HTTPException(status_code=404, detail=f"Unknown desk: {desk_id}")

    state = db.query(DeskState).filter(DeskState.desk_id == desk_id).first()
    if state:
        state.is_paused = False
        state.is_active = True
        state.pause_until = None
        db.commit()

    telegram = _get_telegram()
    await telegram.send_message(f"▶️ <b>{desk_id}</b> resumed via API.")

    return {"status": "resumed", "desk_id": desk_id}


@router.get("/vps/status")
async def vps_status():
    """Check VPS connection status and MT5 state."""
    zmq_bridge = _get_zmq()
    result = await zmq_bridge.get_vps_status()
    return result


@router.get("/desks")
async def list_desks(db: Session = Depends(get_db)):
    """List all desks with their current state."""
    result = []
    for desk_id, desk in DESKS.items():
        state = db.query(DeskState).filter(DeskState.desk_id == desk_id).first()
        result.append({
            "desk_id": desk_id,
            "name": desk["name"],
            "style": desk["style"],
            "symbols": desk["symbols"],
            "risk_pct": desk.get("risk_pct"),
            "is_active": state.is_active if state else True,
            "is_paused": state.is_paused if state else False,
            "trades_today": state.trades_today if state else 0,
            "daily_pnl": state.daily_pnl if state else 0,
            "consecutive_losses": state.consecutive_losses if state else 0,
            "size_modifier": state.size_modifier if state else 1.0,
            "open_positions": state.open_positions if state else 0,
        })
    return {"desks": result}


@router.post("/report/daily")
async def trigger_daily_report(db: Session = Depends(get_db)):
    """Send daily performance report to all Telegram channels."""
    reporter = _get_reporter()
    await reporter.send_daily_report(db)
    return {"status": "sent", "report": "daily"}


@router.post("/report/weekly")
async def trigger_weekly_report(db: Session = Depends(get_db)):
    """Send weekly performance report to all Telegram channels."""
    reporter = _get_reporter()
    await reporter.send_weekly_report(db)
    return {"status": "sent", "report": "weekly"}


@router.post("/report/monthly")
async def trigger_monthly_report(db: Session = Depends(get_db)):
    """Send monthly performance report to all Telegram channels."""
    reporter = _get_reporter()
    await reporter.send_monthly_report(db)
    return {"status": "sent", "report": "monthly"}


# ─────────────────────────────────────────
# ML TRAINING DATA EXPORT
# ─────────────────────────────────────────

@router.get("/ml/export")
async def export_ml_data(
    db: Session = Depends(get_db),
    desk_id: str = None,
    symbol: str = None,
    labeled_only: bool = True,
    limit: int = None,
):
    """
    Export ML training data as JSON.
    Ready to load into pandas: pd.DataFrame(response["data"])

    Query params:
      ?desk_id=DESK1_SCALPER&symbol=EURUSD&labeled_only=true&limit=1000
    """
    from app.services.ml_data_logger import MLDataLogger
    data = MLDataLogger.get_training_data(
        db, labeled_only=labeled_only, desk_id=desk_id, symbol=symbol, limit=limit,
    )
    return {"count": len(data), "data": data}


@router.get("/ml/stats")
async def ml_data_stats(db: Session = Depends(get_db)):
    """Quick stats on collected ML training data."""
    from app.services.ml_data_logger import MLDataLogger
    return MLDataLogger.get_stats(db)
