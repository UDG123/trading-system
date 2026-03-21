"""
Backtest Result Model — Virtual ledger for strategy stress tests.
Each row is a simulated 'Strike' from the backtesting engine.
"""
from datetime import datetime, timezone
from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Boolean, Text,
    Index,
)
from app.database import Base


class BacktestResult(Base):
    __tablename__ = "backtest_results"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # ── Run metadata ──
    run_id = Column(String(64), nullable=False, index=True)
    run_timestamp = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    # ── Signal info ──
    symbol = Column(String(20), nullable=False)
    timeframe = Column(String(10), nullable=False, default="15M")
    direction = Column(String(10), nullable=False)  # BUY / SELL
    signal_type = Column(String(50), nullable=False)  # ema_cross_bullish, rsi_reversal, etc.
    signal_timestamp = Column(DateTime(timezone=True), nullable=False)

    # ── Price levels ──
    entry_price = Column(Float, nullable=False)
    stop_loss = Column(Float, nullable=False)
    take_profit = Column(Float, nullable=False)
    exit_price = Column(Float)

    # ── Hurst Gate ──
    hurst_exponent = Column(Float)
    hurst_vetoed = Column(Boolean, default=False, nullable=False)

    # ── Outcome ──
    outcome = Column(String(10))  # WIN / LOSS / VETOED
    pnl_pips = Column(Float)
    pnl_dollars = Column(Float)
    risk_reward = Column(Float)

    # ── Context ──
    rsi_at_entry = Column(Float)
    ema_fast = Column(Float)
    ema_slow = Column(Float)
    atr_at_entry = Column(Float)

    # ── Audit ──
    notes = Column(Text)

    __table_args__ = (
        Index("ix_backtest_run_symbol", "run_id", "symbol"),
    )
