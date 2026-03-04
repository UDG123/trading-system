"""
Dashboard Route - Desk-level overview for Telegram and monitoring.
"""
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.database import get_db
from app.schemas import DashboardResponse, DeskSummary
from app.models.signal import Signal
from app.models.desk_state import DeskState
from app.config import DESKS

logger = logging.getLogger("TradingSystem.Dashboard")
router = APIRouter()


@router.get("/dashboard", response_model=DashboardResponse)
async def get_dashboard(db: Session = Depends(get_db)):
    """Return firm-wide dashboard with per-desk summaries."""
    today_start = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    # Total signals today
    total_signals = (
        db.query(func.count(Signal.id))
        .filter(Signal.received_at >= today_start)
        .scalar()
    ) or 0

    # Build desk summaries
    desk_summaries = []
    total_pnl = 0.0
    total_trades = 0

    for desk_id, desk_config in DESKS.items():
        state = db.query(DeskState).filter(DeskState.desk_id == desk_id).first()

        if state:
            summary = DeskSummary(
                desk_id=desk_id,
                name=desk_config["name"],
                is_active=state.is_active,
                trades_today=state.trades_today,
                daily_pnl=state.daily_pnl,
                consecutive_losses=state.consecutive_losses,
                size_modifier=state.size_modifier,
                open_positions=state.open_positions,
            )
            total_pnl += state.daily_pnl
            total_trades += state.trades_today
        else:
            # Desk state not yet initialized
            summary = DeskSummary(
                desk_id=desk_id,
                name=desk_config["name"],
                is_active=True,
                trades_today=0,
                daily_pnl=0.0,
                consecutive_losses=0,
                size_modifier=1.0,
                open_positions=0,
            )

        desk_summaries.append(summary)

    return DashboardResponse(
        firm_status="OPERATIONAL",
        total_signals_today=total_signals,
        total_trades_today=total_trades,
        total_daily_pnl=round(total_pnl, 2),
        desks=desk_summaries,
    )
