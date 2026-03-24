"""
PendingSignal Model — Parked signals awaiting optimal entry (pullback/retracement).
Signals are parked when CTO approves but price is overextended from EMA or RSI is extreme.
Re-evaluated every 30s by PendingEngine scheduler.
"""
from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, Boolean, JSON, Index
from app.database import Base


class PendingSignal(Base):
    __tablename__ = "pending_signals"

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))
    signal_id = Column(Integer, nullable=False)

    # Signal identity
    symbol = Column(String(20), nullable=False)
    direction = Column(String(10), nullable=False)
    alert_type = Column(String(50))
    desk_id = Column(String(30), nullable=False)
    timeframe = Column(String(10))

    # Original signal price and levels
    signal_price = Column(Float, nullable=False)
    entry_target = Column(Float, nullable=False)  # calculated optimal entry
    stop_loss = Column(Float)
    take_profit_1 = Column(Float)
    take_profit_2 = Column(Float)

    # Entry method
    entry_method = Column(String(30))  # MAE_OFFSET, EMA_PULLBACK, FVG_FILL, LIMIT_ZONE

    # Lifecycle
    status = Column(String(20), default="PENDING")  # PENDING, TRIGGERED, EXPIRED, CANCELLED
    ttl_expiry = Column(DateTime(timezone=True), nullable=False)
    triggered_at = Column(DateTime(timezone=True))
    trigger_price = Column(Float)

    # Context preserved from original signal
    consensus_score = Column(Integer)
    ml_score = Column(Float)
    enrichment_snapshot = Column(JSON)

    # Re-evaluation tracking
    check_count = Column(Integer, default=0)
    last_checked = Column(DateTime(timezone=True))

    __table_args__ = (
        Index("ix_pending_status", "status", "ttl_expiry"),
        Index("ix_pending_symbol", "symbol", "status"),
    )

    def __repr__(self):
        return (
            f"<PendingSignal {self.id} | {self.symbol} {self.direction} | "
            f"{self.entry_method} → {self.entry_target} | {self.status}>"
        )
