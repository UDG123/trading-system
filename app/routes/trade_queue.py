"""
Trade Queue Route — API for the VPS to poll for pending trades
and report execution results back to Railway.

The VPS client calls:
  GET  /api/trades/pending     — fetch next batch of approved trades
  POST /api/trades/executed    — report trade was placed on MT5
  POST /api/trades/closed      — report trade was closed
  POST /api/trades/update      — update PnL / partial close
  GET  /api/trades/open        — list all open trades
"""
import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Header
from sqlalchemy.orm import Session
from pydantic import BaseModel

from app.database import get_db
from app.config import WEBHOOK_SECRET
from app.models.signal import Signal
from app.models.trade import Trade
from app.models.desk_state import DeskState

logger = logging.getLogger("TradingSystem.TradeQueue")
router = APIRouter()


# ─────────────────────────────────────────
# Request/Response Models
# ─────────────────────────────────────────

class ExecutionReport(BaseModel):
    signal_id: int
    desk_id: str
    symbol: str
    direction: str
    mt5_ticket: int
    entry_price: float
    lot_size: float
    stop_loss: float
    take_profit: Optional[float] = None

class CloseReport(BaseModel):
    mt5_ticket: int
    exit_price: float
    pnl_dollars: float
    pnl_pips: Optional[float] = None
    close_reason: str  # "TP", "SL", "TRAIL", "MANUAL", "KILL_SWITCH", "SIGNAL"
    symbol: Optional[str] = None      # for sim trades
    desk_id: Optional[str] = None     # for sim trades
    direction: Optional[str] = None   # for sim trades

class UpdateReport(BaseModel):
    mt5_ticket: int
    current_price: Optional[float] = None
    current_pnl: Optional[float] = None
    new_sl: Optional[float] = None
    new_tp: Optional[float] = None
    partial_close_pct: Optional[float] = None


def _verify_secret(x_api_key: str = Header(None)):
    """Verify VPS is authorized."""
    if x_api_key != WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Invalid API key")


# ─────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────

@router.get("/trades/pending")
async def get_pending_trades(
    db: Session = Depends(get_db),
    x_api_key: str = Header(None),
):
    """
    VPS polls this to get approved trades waiting for execution.
    Returns signals with status = 'DECIDED' (approved by CTO).
    """
    _verify_secret(x_api_key)

    pending = (
        db.query(Signal)
        .filter(Signal.status == "DECIDED")
        .filter(Signal.claude_decision.in_(["EXECUTE", "REDUCE"]))
        .order_by(Signal.received_at.asc())
        .limit(10)
        .all()
    )

    trades = []
    for sig in pending:
        trades.append({
            "signal_id": sig.id,
            "symbol": sig.symbol_normalized,
            "direction": sig.direction,
            "desk_id": sig.desk_id,
            "price": sig.price,
            "stop_loss": sig.sl1,
            "take_profit_1": sig.tp1,
            "take_profit_2": sig.tp2,
            "smart_trail": sig.smart_trail,
            "risk_pct": sig.position_size_pct,
            "claude_decision": sig.claude_decision,
            "consensus_score": sig.consensus_score,
            "ml_score": sig.ml_score,
            "received_at": sig.received_at.isoformat() if sig.received_at else None,
        })

        # Mark as queued so we don't send it again
        sig.status = "QUEUED"

    if trades:
        db.commit()
        logger.info(f"VPS polled: {len(trades)} pending trades sent")

    return {"pending": trades, "count": len(trades)}


@router.post("/trades/executed")
async def report_execution(
    report: ExecutionReport,
    db: Session = Depends(get_db),
    x_api_key: str = Header(None),
):
    """VPS reports that a trade was successfully placed on MT5."""
    _verify_secret(x_api_key)

    # Update signal status
    signal = db.query(Signal).filter(Signal.id == report.signal_id).first()
    if not signal:
        raise HTTPException(status_code=404, detail="Signal not found")

    signal.status = "EXECUTED"
    signal.execution_price = report.entry_price
    signal.execution_time = datetime.now(timezone.utc)

    # Create trade record
    trade = Trade(
        signal_id=report.signal_id,
        desk_id=report.desk_id,
        symbol=report.symbol,
        direction=report.direction,
        mt5_ticket=report.mt5_ticket,
        entry_price=report.entry_price,
        lot_size=report.lot_size,
        risk_pct=signal.position_size_pct or 0,
        risk_dollars=0,  # calculated VPS-side
        stop_loss=report.stop_loss,
        take_profit_1=report.take_profit,
        status="OPEN",
        opened_at=datetime.now(timezone.utc),
    )
    db.add(trade)

    # Update signal FK
    signal.trade_id = trade.id

    # Update desk state
    desk_state = db.query(DeskState).filter(
        DeskState.desk_id == report.desk_id
    ).first()
    if desk_state:
        desk_state.trades_today = (desk_state.trades_today or 0) + 1
        desk_state.open_positions = (desk_state.open_positions or 0) + 1

    db.commit()

    logger.info(
        f"EXECUTED | Signal #{report.signal_id} | "
        f"{report.symbol} {report.direction} | "
        f"Ticket: {report.mt5_ticket} | "
        f"Entry: {report.entry_price} | Lots: {report.lot_size}"
    )

    return {"status": "recorded", "trade_id": trade.id}


@router.post("/trades/closed")
async def report_close(
    report: CloseReport,
    db: Session = Depends(get_db),
    x_api_key: str = Header(None),
):
    """VPS reports that a trade was closed."""
    _verify_secret(x_api_key)

    trade = db.query(Trade).filter(
        Trade.mt5_ticket == report.mt5_ticket
    ).first()

    # Handle sim trades by ticket range:
    # 800000-899999: OniAI virtual
    # 900000-998999: Server sim (handled by server_simulator.py)
    # 999000+: MT5 sim (from EA)
    is_mt5_sim = report.mt5_ticket >= 999000
    is_server_sim = 900000 <= report.mt5_ticket < 999000
    is_oniai = 800000 <= report.mt5_ticket < 900000

    if not trade and not is_mt5_sim and not is_oniai:
        raise HTTPException(status_code=404, detail="Trade not found")

    if trade:
        trade.exit_price = report.exit_price
        trade.pnl_dollars = report.pnl_dollars
        trade.pnl_pips = report.pnl_pips
        trade.close_reason = report.close_reason
        if is_oniai:
            trade.status = "ONIAI_CLOSED"
        elif is_server_sim:
            trade.status = "SRV_CLOSED"
        else:
            trade.status = "CLOSED"
        trade.closed_at = datetime.now(timezone.utc)

        # Update desk state
        desk_state = db.query(DeskState).filter(
            DeskState.desk_id == trade.desk_id
        ).first()
        if desk_state:
            desk_state.open_positions = max(0, (desk_state.open_positions or 0) - 1)
            desk_state.daily_pnl = (desk_state.daily_pnl or 0) + report.pnl_dollars

            if report.pnl_dollars < 0:
                desk_state.daily_loss = (desk_state.daily_loss or 0) + report.pnl_dollars
                desk_state.consecutive_losses = (desk_state.consecutive_losses or 0) + 1
            else:
                desk_state.consecutive_losses = 0

        # Update signal status
        signal = db.query(Signal).filter(Signal.id == trade.signal_id).first()
        if signal:
            signal.status = "CLOSED"

        db.commit()

    elif is_mt5_sim:
        # EA reported a sim close — create Trade record for reporting
        mt5_trade = Trade(
            signal_id=0,
            desk_id=report.desk_id or "UNKNOWN",
            symbol=report.symbol or "?",
            direction=report.direction or "?",
            mt5_ticket=report.mt5_ticket,
            entry_price=0,
            lot_size=0,
            risk_pct=0,
            risk_dollars=0,
            stop_loss=0,
            exit_price=report.exit_price,
            pnl_dollars=report.pnl_dollars,
            pnl_pips=report.pnl_pips,
            close_reason=f"MT5_{report.close_reason or 'UNKNOWN'}",
            status="MT5_CLOSED",
            opened_at=datetime.now(timezone.utc),
            closed_at=datetime.now(timezone.utc),
        )
        db.add(mt5_trade)
        db.commit()
        trade = mt5_trade

    # Determine symbol and desk for notification
    sym = trade.symbol if trade else (report.symbol or "?")
    desk = trade.desk_id if trade else (report.desk_id or "?")

    sim_tag = ""
    if is_mt5_sim:
        sim_tag = " [MT5]"
    elif is_server_sim:
        sim_tag = " [SRV]"
    elif is_oniai:
        sim_tag = " [OniAI]"
    logger.info(
        f"CLOSED{sim_tag} | Ticket {report.mt5_ticket} | "
        f"{sym} | PnL: ${report.pnl_dollars:+.2f} | "
        f"Reason: {report.close_reason}"
    )

    # Send Telegram notification
    try:
        from app.services.telegram_bot import TelegramBot
        telegram = TelegramBot()
        await telegram.notify_trade_exit(
            symbol=sym,
            desk_id=desk,
            pnl=report.pnl_dollars,
            reason=report.close_reason,
        )
        await telegram.close()
    except Exception as e:
        logger.error(f"Telegram close notification failed: {e}")

    return {"status": "recorded", "pnl": report.pnl_dollars}


@router.post("/trades/update")
async def update_trade(
    report: UpdateReport,
    db: Session = Depends(get_db),
    x_api_key: str = Header(None),
):
    """VPS reports a trade update (trailing SL move, partial close, etc.)."""
    _verify_secret(x_api_key)

    trade = db.query(Trade).filter(
        Trade.mt5_ticket == report.mt5_ticket
    ).first()
    if not trade:
        raise HTTPException(status_code=404, detail="Trade not found")

    if report.new_sl is not None:
        trade.stop_loss = report.new_sl
    if report.new_tp is not None:
        trade.take_profit = report.new_tp
    if report.current_pnl is not None:
        trade.unrealized_pnl = report.current_pnl
    if report.partial_close_pct is not None:
        trade.partial_close_pct = report.partial_close_pct

    db.commit()
    return {"status": "updated"}


@router.get("/trades/open")
async def list_open_trades(
    db: Session = Depends(get_db),
    x_api_key: str = Header(None),
):
    """List all currently open trades across all desks."""
    _verify_secret(x_api_key)

    trades = db.query(Trade).filter(Trade.status == "OPEN").all()

    return {
        "open_trades": [
            {
                "id": t.id,
                "mt5_ticket": t.mt5_ticket,
                "signal_id": t.signal_id,
                "desk_id": t.desk_id,
                "symbol": t.symbol,
                "direction": t.direction,
                "entry_price": t.entry_price,
                "lot_size": t.lot_size,
                "stop_loss": t.stop_loss,
                "take_profit": t.take_profit_1,
                "unrealized_pnl": t.unrealized_pnl,
                "opened_at": t.opened_at.isoformat() if t.opened_at else None,
            }
            for t in trades
        ],
        "count": len(trades),
    }
