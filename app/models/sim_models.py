"""
Simulation Engine Models — sim_profiles, sim_orders, sim_positions,
sim_equity_snapshots, and spread_reference tables for the shadow
simulation environment.
"""
from datetime import datetime, timezone
from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Boolean, Date, ForeignKey,
)
from sqlalchemy.dialects.postgresql import JSONB
from app.database import Base


class SimProfile(Base):
    __tablename__ = "sim_profiles"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(30), unique=True, nullable=False)
    initial_balance = Column(Float, nullable=False, default=100000)
    current_balance = Column(Float, nullable=False, default=100000)
    leverage = Column(Integer, nullable=False, default=100)
    risk_pct = Column(Float, nullable=False, default=1.0)
    max_daily_loss_pct = Column(Float, nullable=False, default=5.0)
    max_total_loss_pct = Column(Float, nullable=False, default=10.0)
    trailing_drawdown = Column(Boolean, nullable=False, default=False)
    is_active = Column(Boolean, nullable=False, default=True)

    def __repr__(self):
        return (
            f"<SimProfile {self.name} | bal={self.current_balance} | "
            f"lev={self.leverage}x | risk={self.risk_pct}%>"
        )


class SimOrder(Base):
    __tablename__ = "sim_orders"

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    # ── Relationships ──
    profile_id = Column(Integer, ForeignKey("sim_profiles.id"), nullable=False)
    shadow_signal_id = Column(Integer, ForeignKey("shadow_signals.id"))
    signal_id = Column(Integer)

    # ── Identity ──
    symbol = Column(String(20), nullable=False)
    direction = Column(String(10), nullable=False)
    desk_id = Column(String(30))
    order_type = Column(String(20), nullable=False, default="MARKET")

    # ── Requested ──
    requested_price = Column(Float)
    requested_qty = Column(Float)
    requested_sl = Column(Float)
    requested_tp1 = Column(Float)
    requested_tp2 = Column(Float)

    # ── Execution simulation ──
    fill_price = Column(Float)
    fill_qty = Column(Float)
    spread_cost = Column(Float)
    slippage_cost = Column(Float)
    commission = Column(Float)
    simulated_latency_ms = Column(Integer)

    # ── Sizing ──
    lot_size = Column(Float)
    risk_pct = Column(Float)
    risk_dollars = Column(Float)
    position_size_modifier = Column(Float, default=1.0)

    # ── Status ──
    status = Column(String(20), nullable=False, default="PENDING")
    reject_reason = Column(String(200))
    filled_at = Column(DateTime(timezone=True))

    def __repr__(self):
        return (
            f"<SimOrder {self.id} | {self.symbol} {self.direction} | "
            f"{self.status} | profile={self.profile_id}>"
        )


class SimPosition(Base):
    __tablename__ = "sim_positions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )

    # ── Relationships ──
    profile_id = Column(Integer, ForeignKey("sim_profiles.id"), nullable=False)
    order_id = Column(Integer, ForeignKey("sim_orders.id"))
    shadow_signal_id = Column(Integer)
    symbol = Column(String(20), nullable=False)
    direction = Column(String(10), nullable=False)
    desk_id = Column(String(30))

    # ── Entry ──
    entry_price = Column(Float)
    entry_time = Column(DateTime(timezone=True))
    lot_size = Column(Float)

    # ── Levels ──
    stop_loss = Column(Float)
    take_profit_1 = Column(Float)
    take_profit_2 = Column(Float)
    trailing_stop = Column(Float)
    breakeven_price = Column(Float)

    # ── Live tracking ──
    current_price = Column(Float)
    unrealized_pnl = Column(Float)
    max_favorable_pips = Column(Float)      # MFE
    max_adverse_pips = Column(Float)        # MAE

    # ── Partial close (3-tier system) ──
    partial_close_pct = Column(Float)
    partial_pnl = Column(Float)
    exit_tier = Column(Integer)                 # 1, 2, or 3 — which tier triggered final close
    partial_pnl_tier1 = Column(Float)           # PnL from 33% close at 1R
    partial_pnl_tier2 = Column(Float)           # PnL from 33% close at 3R
    partial_pnl_tier3 = Column(Float)           # PnL from 34% trailing close
    time_based_exit = Column(Boolean, default=False)  # True if zombie trade time-exit

    # ── Exit ──
    exit_price = Column(Float)
    exit_time = Column(DateTime(timezone=True))
    exit_reason = Column(String(30))        # SL_HIT / TP1_HIT / TP2_HIT / TRAILING / TIME_EXIT / SESSION_CLOSE

    # ── Result ──
    realized_pnl_pips = Column(Float)
    realized_pnl_dollars = Column(Float)
    hold_time_minutes = Column(Float)
    commission_total = Column(Float)
    swap_total = Column(Float)
    net_pnl = Column(Float)

    # ── Status ──
    status = Column(String(20), nullable=False, default="OPEN")
    risk_flags = Column(JSONB)

    def __repr__(self):
        return (
            f"<SimPosition {self.id} | {self.symbol} {self.direction} | "
            f"{self.status} | pnl={self.net_pnl}>"
        )


class SimEquitySnapshot(Base):
    __tablename__ = "sim_equity_snapshots"

    id = Column(Integer, primary_key=True, autoincrement=True)
    profile_id = Column(Integer, ForeignKey("sim_profiles.id"), nullable=False)
    snapshot_time = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
    equity = Column(Float)
    balance = Column(Float)
    unrealized_pnl = Column(Float)
    daily_pnl = Column(Float)
    daily_high = Column(Float)
    high_water_mark = Column(Float)
    daily_drawdown_pct = Column(Float)
    total_drawdown_pct = Column(Float)
    open_positions = Column(Integer)
    snapshot_date = Column(Date)

    def __repr__(self):
        return (
            f"<SimEquitySnapshot profile={self.profile_id} | "
            f"equity={self.equity} | dd={self.total_drawdown_pct}%>"
        )


class SpreadReference(Base):
    __tablename__ = "spread_reference"

    symbol = Column(String(20), primary_key=True)
    asset_class = Column(String(20), nullable=False)
    base_spread_pips = Column(Float, nullable=False)
    asian_mult = Column(Float, nullable=False, default=1.8)
    london_mult = Column(Float, nullable=False, default=1.0)
    ny_mult = Column(Float, nullable=False, default=1.0)
    overlap_mult = Column(Float, nullable=False, default=0.8)
    post_ny_mult = Column(Float, nullable=False, default=1.5)
    commission_per_lot = Column(Float, nullable=False, default=0)

    def __repr__(self):
        return (
            f"<SpreadReference {self.symbol} | {self.asset_class} | "
            f"spread={self.base_spread_pips} pips>"
        )
