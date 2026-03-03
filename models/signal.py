"""
Signal Model - Every incoming LuxAlgo alert is logged here.
This is the primary audit trail for the entire system.
"""
from datetime import datetime, timezone
from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Text, Boolean, JSON,
    Index,
)
from app.database import Base


class Signal(Base):
    __tablename__ = "signals"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # ── Source identification ──
    received_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
        index=True,
    )
    source = Column(String(50), default="tradingview", nullable=False)

    # ── Alert payload (raw from TradingView) ──
    raw_payload = Column(Text, nullable=False)  # full JSON string for audit

    # ── Parsed fields ──
    symbol = Column(String(20), nullable=False, index=True)
    symbol_normalized = Column(String(20), nullable=False, index=True)
    timeframe = Column(String(10), nullable=False)
    alert_type = Column(String(50), nullable=False, index=True)
    direction = Column(String(10), nullable=True)  # LONG / SHORT / EXIT / null
    price = Column(Float, nullable=True)
    tp1 = Column(Float, nullable=True)
    tp2 = Column(Float, nullable=True)
    sl1 = Column(Float, nullable=True)
    sl2 = Column(Float, nullable=True)
    smart_trail = Column(Float, nullable=True)

    # ── Desk routing ──
    desk_id = Column(String(30), nullable=True, index=True)
    desks_matched = Column(JSON, nullable=True)  # list of desk IDs this applies to

    # ── Validation ──
    is_valid = Column(Boolean, default=False, nullable=False)
    validation_errors = Column(JSON, nullable=True)  # list of error strings

    # ── Processing status ──
    status = Column(
        String(20),
        default="RECEIVED",
        nullable=False,
        index=True,
    )
    # RECEIVED → VALIDATED → ENRICHED → SCORED → DECIDED → EXECUTED → REJECTED

    # ── ML scoring (Phase 2) ──
    ml_score = Column(Float, nullable=True)
    ml_method = Column(String(20), nullable=True)  # "model" or "rule_based"

    # ── Enrichment data (Phase 2) ──
    enrichment_data = Column(JSON, nullable=True)  # full enrichment snapshot

    # ── Claude decision (Phase 2) ──
    claude_decision = Column(String(20), nullable=True)  # EXECUTE / SKIP / REDUCE
    claude_reasoning = Column(Text, nullable=True)
    consensus_score = Column(Integer, nullable=True)
    position_size_pct = Column(Float, nullable=True)

    # ── Execution result (Phase 3) ──
    trade_id = Column(Integer, nullable=True)  # FK to trades table
    execution_price = Column(Float, nullable=True)
    execution_time = Column(DateTime(timezone=True), nullable=True)

    # ── Metadata ──
    processing_time_ms = Column(Integer, nullable=True)

    __table_args__ = (
        Index("ix_signals_desk_status", "desk_id", "status"),
        Index("ix_signals_symbol_time", "symbol_normalized", "received_at"),
    )

    def __repr__(self):
        return (
            f"<Signal {self.id} | {self.symbol_normalized} | "
            f"{self.alert_type} | {self.status}>"
        )
