"""
DeskState Model - Runtime state for each trading desk.
Tracks daily losses, consecutive losses, active status, etc.
"""
from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, JSON
from app.database import Base


class DeskState(Base):
    __tablename__ = "desk_states"

    id = Column(Integer, primary_key=True, autoincrement=True)
    desk_id = Column(String(30), unique=True, nullable=False, index=True)

    # ── Status ──
    is_active = Column(Boolean, default=True, nullable=False)
    is_paused = Column(Boolean, default=False, nullable=False)
    pause_until = Column(DateTime(timezone=True), nullable=True)

    # ── Daily counters (reset at midnight CET) ──
    trades_today = Column(Integer, default=0)
    daily_pnl = Column(Float, default=0.0)
    daily_loss = Column(Float, default=0.0)
    last_reset_date = Column(String(10), nullable=True)  # "2025-06-15"

    # ── Consecutive loss tracking ──
    consecutive_losses = Column(Integer, default=0)
    size_modifier = Column(Float, default=1.0)  # multiplier from loss protection

    # ── Open positions ──
    open_positions = Column(Integer, default=0)

    # ── Audit ──
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    def __repr__(self):
        return (
            f"<DeskState {self.desk_id} | Active: {self.is_active} | "
            f"Trades: {self.trades_today} | PnL: {self.daily_pnl}>"
        )
