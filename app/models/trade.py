"""
Trade Model - Tracks all executed trades and their lifecycle.
"""
from datetime import datetime, timezone
from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Text, Boolean, JSON, Index,
)
from app.database import Base


class Trade(Base):
    __tablename__ = "trades"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # ── Link to originating signal ──
    signal_id = Column(Integer, nullable=False, index=True)

    # ── Trade identification ──
    desk_id = Column(String(30), nullable=False, index=True)
    symbol = Column(String(20), nullable=False, index=True)
    direction = Column(String(10), nullable=False)  # LONG / SHORT
    mt5_ticket = Column(Integer, nullable=True, unique=True)  # Legacy ticket ID — used for unique trade identification

    # ── Sizing ──
    lot_size = Column(Float, nullable=False)
    risk_pct = Column(Float, nullable=False)
    risk_dollars = Column(Float, nullable=False)
    position_size_modifier = Column(Float, default=1.0)  # from consensus scoring

    # ── Levels ──
    entry_price = Column(Float, nullable=True)
    stop_loss = Column(Float, nullable=False)
    take_profit_1 = Column(Float, nullable=True)
    take_profit_2 = Column(Float, nullable=True)
    trailing_stop = Column(Float, nullable=True)

    # ── Lifecycle timestamps ──
    opened_at = Column(DateTime(timezone=True), nullable=True)
    closed_at = Column(DateTime(timezone=True), nullable=True)

    # ── Result ──
    exit_price = Column(Float, nullable=True)
    pnl_dollars = Column(Float, nullable=True)
    pnl_pips = Column(Float, nullable=True)
    status = Column(
        String(20), default="PENDING", nullable=False, index=True,
    )
    # PENDING → OPEN → PARTIAL_CLOSE → CLOSED → CANCELLED

    # ── Pyramiding ──
    is_pyramid = Column(Boolean, default=False)
    pyramid_layer = Column(Integer, default=1)  # 1, 2, or 3
    parent_trade_id = Column(Integer, nullable=True)

    # ── Partial close tracking ──
    partial_closes = Column(JSON, nullable=True)
    # [{"pct": 33, "price": 1.1050, "pnl": 150.0, "at": "2025-..."}]
    partial_close_pct = Column(Float, nullable=True)  # total % closed so far

    # ── Live tracking ──
    unrealized_pnl = Column(Float, nullable=True)
    close_reason = Column(String(30), nullable=True)  # TP, SL, TRAIL, MANUAL, KILL_SWITCH

    # ── Audit ──
    claude_reasoning = Column(Text, nullable=True)
    consensus_score = Column(Integer, nullable=True)

    __table_args__ = (
        Index("ix_trades_desk_status", "desk_id", "status"),
        Index("ix_trades_symbol_open", "symbol", "opened_at"),
    )

    def __repr__(self):
        return (
            f"<Trade {self.id} | {self.symbol} {self.direction} | "
            f"{self.status} | PnL: {self.pnl_dollars}>"
        )
