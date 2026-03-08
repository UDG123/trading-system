"""
ML Training Data Model
Stores every signal with full context for future ML model training.
Each row = one signal event with all features and outcome.
"""
from datetime import datetime, timezone
from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Text, Boolean, JSON, Index,
)
from app.database import Base


class MLTradeLog(Base):
    __tablename__ = "ml_trade_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    # ── Signal Features ──
    signal_id = Column(Integer, nullable=True, index=True)
    symbol = Column(String(20), nullable=False, index=True)
    direction = Column(String(10), nullable=False)        # LONG / SHORT
    timeframe = Column(String(10), nullable=True)          # 5M, 1H, 4H
    alert_type = Column(String(50), nullable=True)         # bullish_confirmation, etc.
    entry_price = Column(Float, nullable=True)
    sl_price = Column(Float, nullable=True)
    tp1_price = Column(Float, nullable=True)
    tp2_price = Column(Float, nullable=True)
    sl_pips = Column(Float, nullable=True)
    tp1_pips = Column(Float, nullable=True)
    rr_ratio = Column(Float, nullable=True)                # TP1/SL

    # ── Market Context at Signal Time ──
    desk_id = Column(String(30), nullable=False, index=True)
    session = Column(String(20), nullable=True)            # LONDON, NEW_YORK, OVERLAP, ASIAN
    day_of_week = Column(Integer, nullable=True)           # 0=Mon, 6=Sun
    hour_utc = Column(Integer, nullable=True)
    atr_value = Column(Float, nullable=True)               # ATR(14) at signal time
    spread_at_entry = Column(Float, nullable=True)         # spread in pips
    volatility_regime = Column(String(20), nullable=True)  # LOW, NORMAL, HIGH, EXTREME

    # ── Pipeline Scores ──
    ml_score = Column(Float, nullable=True)                # ML scorer output
    ml_method = Column(String(30), nullable=True)          # which ML method used
    consensus_score = Column(Integer, nullable=True)       # 0-10
    consensus_tier = Column(String(10), nullable=True)     # HIGH, MEDIUM, LOW
    claude_decision = Column(String(20), nullable=True)    # EXECUTE, REDUCE, SKIP, HOLD
    claude_confidence = Column(Float, nullable=True)       # 0-1
    claude_reasoning = Column(Text, nullable=True)

    # ── Risk State at Signal Time ──
    risk_pct = Column(Float, nullable=True)
    lot_size = Column(Float, nullable=True)
    risk_dollars = Column(Float, nullable=True)
    open_positions_desk = Column(Integer, nullable=True)
    open_positions_total = Column(Integer, nullable=True)
    daily_pnl_at_entry = Column(Float, nullable=True)
    daily_loss_at_entry = Column(Float, nullable=True)
    consecutive_losses = Column(Integer, nullable=True)
    size_modifier = Column(Float, nullable=True)           # from consecutive loss scaling

    # ── Filter Results ──
    approved = Column(Boolean, nullable=False, default=False)
    filter_blocked = Column(Boolean, nullable=False, default=False)
    block_reason = Column(String(100), nullable=True)      # which filter blocked
    is_oniai = Column(Boolean, nullable=False, default=False)

    # ── Outcome (filled when trade closes) ──
    outcome = Column(String(10), nullable=True)            # WIN, LOSS, BE (breakeven)
    pnl_pips = Column(Float, nullable=True)
    pnl_dollars = Column(Float, nullable=True)
    exit_price = Column(Float, nullable=True)
    exit_reason = Column(String(30), nullable=True)        # SL_HIT, TP1_HIT, TP2_HIT, TRAILING, TIME_EXIT
    hold_time_minutes = Column(Float, nullable=True)
    max_favorable_pips = Column(Float, nullable=True)      # max pips in profit direction (MFE)
    max_adverse_pips = Column(Float, nullable=True)        # max pips against (MAE)

    # ── Multi-Profile Outcomes ──
    srv100_pnl_pips = Column(Float, nullable=True)
    srv100_exit_reason = Column(String(30), nullable=True)
    srv30_pnl_pips = Column(Float, nullable=True)
    srv30_exit_reason = Column(String(30), nullable=True)
    mt5_pnl_pips = Column(Float, nullable=True)
    mt5_exit_reason = Column(String(30), nullable=True)
    oniai_pnl_pips = Column(Float, nullable=True)
    oniai_exit_reason = Column(String(30), nullable=True)

    # ── Correlation Context ──
    correlated_open_count = Column(Integer, nullable=True)  # same-group positions open
    correlation_group = Column(String(30), nullable=True)

    # ── Raw Data (JSON blob for anything else) ──
    raw_signal_data = Column(JSON, nullable=True)          # full webhook payload
    raw_enrichment = Column(JSON, nullable=True)           # TwelveData enrichment
    raw_ml_result = Column(JSON, nullable=True)            # full ML scorer output
    raw_consensus = Column(JSON, nullable=True)            # full consensus output
    raw_claude_response = Column(JSON, nullable=True)      # full CTO response

    __table_args__ = (
        Index("ix_ml_log_symbol_date", "symbol", "created_at"),
        Index("ix_ml_log_desk_date", "desk_id", "created_at"),
        Index("ix_ml_log_outcome", "outcome"),
    )
