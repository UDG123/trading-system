"""
VirtualBroker — Realistic trade execution simulator.
Models dynamic spreads, slippage, commission, fill probability, and latency.
Replaces simple LocalBroker/PaperExecutor for all simulation.
"""
import math
import random
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, List

from sqlalchemy import func
from sqlalchemy.orm import Session

from app.models.shadow_signal import ShadowSignal
from app.models.sim_models import (
    SimProfile, SimOrder, SimPosition, SimEquitySnapshot, SpreadReference,
)
from app.config import (
    get_pip_info, calculate_lot_size, DESKS, CAPITAL_PER_ACCOUNT,
)

logger = logging.getLogger("TradingSystem.VirtualBroker")

MAX_OPEN_PER_DESK = 5


class VirtualBroker:
    """Realistic trade execution simulator for shadow sim engine."""

    def __init__(self, db_session_factory):
        self._db_factory = db_session_factory

    async def execute_signal(
        self,
        db: Session,
        shadow_signal_id: int,
        profile_name: str = "SRV_100",
    ) -> Dict:
        """Execute a shadow signal through sim for a specific profile."""
        # Load signal and profile
        signal = db.query(ShadowSignal).filter(ShadowSignal.id == shadow_signal_id).first()
        if not signal:
            return {"status": "ERROR", "error": "Shadow signal not found"}

        profile = db.query(SimProfile).filter(SimProfile.name == profile_name).first()
        if not profile or not profile.is_active:
            return {"status": "SKIPPED", "error": f"Profile {profile_name} inactive or missing"}

        # Risk checks
        reject = self._check_risk_limits(db, profile, signal)
        if reject:
            order = SimOrder(
                profile_id=profile.id,
                shadow_signal_id=shadow_signal_id,
                symbol=signal.symbol,
                direction=signal.direction or "LONG",
                desk_id=signal.desk_id,
                status="REJECTED",
                reject_reason=reject,
            )
            db.add(order)
            db.flush()
            return {"status": "REJECTED", "reason": reject}

        # Position sizing
        pip_size, pip_value = get_pip_info(signal.symbol)
        entry = signal.price or 0
        sl = signal.sl1 or (entry * 0.99 if signal.direction == "LONG" else entry * 1.01)
        sl_pips = abs(entry - sl) / pip_size if pip_size > 0 and entry and sl else 50

        lot_size = calculate_lot_size(
            desk_id=signal.desk_id or "DESK2_INTRADAY",
            symbol=signal.symbol,
            risk_pct=profile.risk_pct,
            sl_pips=sl_pips,
            account_capital=profile.current_balance,
        )

        # Simulate fill
        hour = signal.created_at.hour if signal.created_at else datetime.now(timezone.utc).hour
        session = self._get_session_name(hour)
        fill = self._simulate_fill(
            symbol=signal.symbol,
            direction=signal.direction or "LONG",
            requested_price=entry,
            order_type="MARKET",
            session=session,
            db=db,
        )

        if not fill.get("filled", True):
            order = SimOrder(
                profile_id=profile.id,
                shadow_signal_id=shadow_signal_id,
                symbol=signal.symbol,
                direction=signal.direction or "LONG",
                desk_id=signal.desk_id,
                requested_price=entry,
                status="REJECTED",
                reject_reason="Fill simulation failed",
            )
            db.add(order)
            db.flush()
            return {"status": "REJECTED", "reason": "No fill"}

        risk_dollars = profile.current_balance * (profile.risk_pct / 100)

        # Create order
        order = SimOrder(
            profile_id=profile.id,
            shadow_signal_id=shadow_signal_id,
            symbol=signal.symbol,
            direction=signal.direction or "LONG",
            desk_id=signal.desk_id,
            order_type="MARKET",
            requested_price=entry,
            requested_qty=lot_size,
            requested_sl=signal.sl1,
            requested_tp1=signal.tp1,
            requested_tp2=signal.tp2,
            fill_price=fill["fill_price"],
            fill_qty=lot_size,
            spread_cost=fill["spread_cost"],
            slippage_cost=fill["slippage_cost"],
            commission=fill["commission"],
            simulated_latency_ms=fill["latency_ms"],
            lot_size=lot_size,
            risk_pct=profile.risk_pct,
            risk_dollars=round(risk_dollars, 2),
            position_size_modifier=1.0,
            status="FILLED",
            filled_at=datetime.now(timezone.utc),
        )
        db.add(order)
        db.flush()

        # Create position
        position = SimPosition(
            profile_id=profile.id,
            order_id=order.id,
            shadow_signal_id=shadow_signal_id,
            symbol=signal.symbol,
            direction=signal.direction or "LONG",
            desk_id=signal.desk_id,
            entry_price=fill["fill_price"],
            entry_time=datetime.now(timezone.utc),
            lot_size=lot_size,
            stop_loss=signal.sl1,
            take_profit_1=signal.tp1,
            take_profit_2=signal.tp2,
            trailing_stop=signal.smart_trail,
            current_price=fill["fill_price"],
            unrealized_pnl=0.0,
            max_favorable_pips=0.0,
            max_adverse_pips=0.0,
            status="OPEN",
        )
        db.add(position)

        # Deduct commission
        profile.current_balance -= fill["commission"]

        db.flush()

        logger.debug(
            f"SIM TRADE | {profile_name} | {signal.symbol} {signal.direction} | "
            f"fill={fill['fill_price']} | spread={fill['spread_cost']:.5f} | "
            f"slip={fill['slippage_cost']:.5f} | lot={lot_size}"
        )

        return {
            "status": "FILLED",
            "trade_id": position.id,
            "order_id": order.id,
            "fill_price": fill["fill_price"],
            "spread_cost": fill["spread_cost"],
            "slippage_cost": fill["slippage_cost"],
            "lot_size": lot_size,
            "profile": profile_name,
        }

    async def execute_for_all_profiles(
        self, db: Session, shadow_signal_id: int
    ) -> Dict:
        """Execute signal for each active sim profile."""
        profiles = db.query(SimProfile).filter(SimProfile.is_active == True).all()
        results = {}
        for profile in profiles:
            try:
                result = await self.execute_signal(db, shadow_signal_id, profile.name)
                results[profile.name] = result
            except Exception as e:
                logger.debug(f"Sim execute failed for {profile.name}: {e}")
                results[profile.name] = {"status": "ERROR", "error": str(e)}
        return results

    def _check_risk_limits(
        self, db: Session, profile: SimProfile, signal: ShadowSignal
    ) -> Optional[str]:
        """Check risk limits. Returns reject reason or None."""
        # Check open positions per desk
        desk_open = (
            db.query(func.count(SimPosition.id))
            .filter(
                SimPosition.profile_id == profile.id,
                SimPosition.desk_id == signal.desk_id,
                SimPosition.status == "OPEN",
            )
            .scalar() or 0
        )
        if desk_open >= MAX_OPEN_PER_DESK:
            return f"Max open positions ({MAX_OPEN_PER_DESK}) for desk {signal.desk_id}"

        # Check daily drawdown
        today = datetime.now(timezone.utc).date()
        daily_snap = (
            db.query(SimEquitySnapshot)
            .filter(
                SimEquitySnapshot.profile_id == profile.id,
                SimEquitySnapshot.snapshot_date == today,
            )
            .order_by(SimEquitySnapshot.snapshot_time.desc())
            .first()
        )
        if daily_snap and daily_snap.daily_drawdown_pct:
            if abs(daily_snap.daily_drawdown_pct) >= profile.max_daily_loss_pct:
                return f"Daily drawdown {daily_snap.daily_drawdown_pct:.1f}% >= limit {profile.max_daily_loss_pct}%"

        # Check total drawdown
        if daily_snap and daily_snap.total_drawdown_pct:
            if abs(daily_snap.total_drawdown_pct) >= profile.max_total_loss_pct:
                return f"Total drawdown {daily_snap.total_drawdown_pct:.1f}% >= limit {profile.max_total_loss_pct}%"

        return None

    def _simulate_fill(
        self,
        symbol: str,
        direction: str,
        requested_price: float,
        order_type: str,
        session: str,
        db: Session = None,
    ) -> Dict:
        """Simulate realistic order execution."""
        if not requested_price or requested_price <= 0:
            return {"filled": False}

        pip_size, _ = get_pip_info(symbol)

        # Get spread reference
        base_spread_pips = 1.5  # default
        commission_per_lot = 0.0
        session_mult = 1.0

        if db:
            spread_ref = db.query(SpreadReference).filter(SpreadReference.symbol == symbol).first()
            if spread_ref:
                base_spread_pips = spread_ref.base_spread_pips
                commission_per_lot = spread_ref.commission_per_lot
                hour = datetime.now(timezone.utc).hour
                session_mult = self._get_session_multiplier_from_ref(spread_ref, hour)

        # Dynamic spread = base × session × volatility jitter
        vol_factor = 1.0 + random.uniform(0, 0.3)
        effective_spread_pips = base_spread_pips * session_mult * vol_factor
        half_spread = (effective_spread_pips * pip_size) / 2

        # Slippage for MARKET orders
        if order_type == "MARKET":
            # LogNormal slippage, capped at 2x base spread
            slippage_pips = min(
                random.lognormvariate(mu=-1.2, sigma=0.5),
                base_spread_pips * 2,
            )
            slippage = slippage_pips * pip_size
        else:
            # LIMIT: no slippage but fill_probability = 0.85
            if random.random() > 0.85:
                return {"filled": False}
            slippage = 0.0

        # Fill price: adverse direction
        if direction == "LONG":
            fill_price = requested_price + half_spread + slippage
        else:
            fill_price = requested_price - half_spread - slippage

        # Commission
        commission = commission_per_lot  # per lot, applied at order level

        # Latency
        latency_ms = int(random.lognormvariate(mu=4.5, sigma=0.5))  # ~90ms median

        return {
            "fill_price": round(fill_price, 5),
            "spread_cost": round(effective_spread_pips * pip_size, 5),
            "slippage_cost": round(slippage, 5),
            "commission": round(commission, 2),
            "latency_ms": min(latency_ms, 5000),
            "filled": True,
        }

    def _get_session_multiplier_from_ref(
        self, spread_ref: SpreadReference, hour_utc: int
    ) -> float:
        """Get session-based spread multiplier from reference data."""
        if 0 <= hour_utc < 7:
            return spread_ref.asian_mult
        elif 7 <= hour_utc < 12:
            return spread_ref.london_mult
        elif 12 <= hour_utc < 16:
            return spread_ref.overlap_mult
        elif 16 <= hour_utc < 21:
            return spread_ref.ny_mult
        else:
            return spread_ref.post_ny_mult

    def _get_session_name(self, hour_utc: int) -> str:
        if 0 <= hour_utc < 7:
            return "ASIAN"
        elif 7 <= hour_utc < 12:
            return "LONDON"
        elif 12 <= hour_utc < 16:
            return "OVERLAP"
        elif 16 <= hour_utc < 21:
            return "NEW_YORK"
        else:
            return "POST_NY"

    async def check_exits(
        self, db: Session, current_prices: Dict[str, float]
    ) -> List[Dict]:
        """Check all open positions for exit conditions."""
        closed = []

        open_positions = (
            db.query(SimPosition)
            .filter(SimPosition.status == "OPEN")
            .all()
        )

        for pos in open_positions:
            price = current_prices.get(pos.symbol)
            if not price:
                continue

            pip_size, pip_value = get_pip_info(pos.symbol)
            if pip_size <= 0:
                continue

            direction = pos.direction.upper()

            # Update current price and unrealized PnL
            if direction == "LONG":
                unrealized_pips = (price - pos.entry_price) / pip_size
            else:
                unrealized_pips = (pos.entry_price - price) / pip_size

            pos.current_price = price
            pos.unrealized_pnl = round(unrealized_pips * pip_value * pos.lot_size, 2)

            # Update MFE/MAE
            if direction == "LONG":
                fav = (price - pos.entry_price) / pip_size
                adv = (pos.entry_price - price) / pip_size
            else:
                fav = (pos.entry_price - price) / pip_size
                adv = (price - pos.entry_price) / pip_size

            pos.max_favorable_pips = max(pos.max_favorable_pips or 0, fav)
            pos.max_adverse_pips = max(pos.max_adverse_pips or 0, adv)

            # Check exit conditions
            exit_reason = None
            exit_price = None

            # SL hit
            if pos.stop_loss:
                if direction == "LONG" and price <= pos.stop_loss:
                    exit_reason = "SL_HIT"
                    exit_price = pos.stop_loss
                elif direction == "SHORT" and price >= pos.stop_loss:
                    exit_reason = "SL_HIT"
                    exit_price = pos.stop_loss

            # TP1 hit — partial close 50%
            if not exit_reason and pos.take_profit_1 and not pos.partial_close_pct:
                if direction == "LONG" and price >= pos.take_profit_1:
                    pos.partial_close_pct = 50.0
                    partial_pips = (pos.take_profit_1 - pos.entry_price) / pip_size
                    pos.partial_pnl = round(partial_pips * pip_value * pos.lot_size * 0.5, 2)
                    # Move SL to breakeven
                    pos.breakeven_price = pos.entry_price
                    pos.stop_loss = pos.entry_price
                    pos.status = "PARTIAL"
                elif direction == "SHORT" and price <= pos.take_profit_1:
                    pos.partial_close_pct = 50.0
                    partial_pips = (pos.entry_price - pos.take_profit_1) / pip_size
                    pos.partial_pnl = round(partial_pips * pip_value * pos.lot_size * 0.5, 2)
                    pos.breakeven_price = pos.entry_price
                    pos.stop_loss = pos.entry_price
                    pos.status = "PARTIAL"

            # TP2 hit — close remaining
            if not exit_reason and pos.take_profit_2:
                if direction == "LONG" and price >= pos.take_profit_2:
                    exit_reason = "TP2_HIT"
                    exit_price = pos.take_profit_2
                elif direction == "SHORT" and price <= pos.take_profit_2:
                    exit_reason = "TP2_HIT"
                    exit_price = pos.take_profit_2

            # Trailing stop
            if not exit_reason and pos.trailing_stop:
                if direction == "LONG" and price <= pos.trailing_stop:
                    exit_reason = "TRAILING"
                    exit_price = pos.trailing_stop
                elif direction == "SHORT" and price >= pos.trailing_stop:
                    exit_reason = "TRAILING"
                    exit_price = pos.trailing_stop

            # Time exit
            if not exit_reason and pos.entry_time:
                desk = DESKS.get(pos.desk_id, {})
                max_hold = desk.get("max_hold_hours", 24)
                if datetime.now(timezone.utc) - pos.entry_time > timedelta(hours=max_hold):
                    exit_reason = "TIME_EXIT"
                    exit_price = price

            # Close position if exit triggered
            if exit_reason:
                exit_price = exit_price or price
                if direction == "LONG":
                    pnl_pips = (exit_price - pos.entry_price) / pip_size
                else:
                    pnl_pips = (pos.entry_price - exit_price) / pip_size

                # Account for partial close
                remaining_pct = 1.0 - (pos.partial_close_pct or 0) / 100
                pnl_dollars = round(pnl_pips * pip_value * pos.lot_size * remaining_pct, 2)
                total_pnl = pnl_dollars + (pos.partial_pnl or 0)
                hold_minutes = 0.0
                if pos.entry_time:
                    hold_minutes = (datetime.now(timezone.utc) - pos.entry_time).total_seconds() / 60

                pos.exit_price = exit_price
                pos.exit_time = datetime.now(timezone.utc)
                pos.exit_reason = exit_reason
                pos.realized_pnl_pips = round(pnl_pips, 2)
                pos.realized_pnl_dollars = pnl_dollars
                pos.hold_time_minutes = round(hold_minutes, 1)
                pos.commission_total = 0.0  # Already deducted at order
                pos.swap_total = 0.0
                pos.net_pnl = total_pnl
                pos.status = "CLOSED"

                # Update profile balance
                profile = db.query(SimProfile).filter(SimProfile.id == pos.profile_id).first()
                if profile:
                    profile.current_balance += total_pnl

                closed.append({
                    "position_id": pos.id,
                    "profile_id": pos.profile_id,
                    "symbol": pos.symbol,
                    "direction": pos.direction,
                    "exit_reason": exit_reason,
                    "pnl_pips": round(pnl_pips, 2),
                    "pnl_dollars": total_pnl,
                    "hold_minutes": round(hold_minutes, 1),
                })

        if closed:
            db.flush()
            logger.debug(f"Sim exits: {len(closed)} positions closed")

        return closed

    async def take_equity_snapshot(self, db: Session) -> None:
        """Take equity snapshots for all active profiles."""
        profiles = db.query(SimProfile).filter(SimProfile.is_active == True).all()
        today = datetime.now(timezone.utc).date()

        for profile in profiles:
            # Sum unrealized PnL
            unrealized = (
                db.query(func.coalesce(func.sum(SimPosition.unrealized_pnl), 0))
                .filter(
                    SimPosition.profile_id == profile.id,
                    SimPosition.status.in_(["OPEN", "PARTIAL"]),
                )
                .scalar() or 0
            )

            equity = profile.current_balance + float(unrealized)

            # Get today's previous snapshots for daily tracking
            prev_snap = (
                db.query(SimEquitySnapshot)
                .filter(
                    SimEquitySnapshot.profile_id == profile.id,
                    SimEquitySnapshot.snapshot_date == today,
                )
                .order_by(SimEquitySnapshot.snapshot_time.desc())
                .first()
            )

            daily_high = equity
            high_water_mark = equity
            daily_pnl = equity - profile.initial_balance

            if prev_snap:
                daily_high = max(equity, prev_snap.daily_high or equity)
                high_water_mark = max(equity, prev_snap.high_water_mark or equity)
                # daily_pnl from start of day
                first_snap = (
                    db.query(SimEquitySnapshot)
                    .filter(
                        SimEquitySnapshot.profile_id == profile.id,
                        SimEquitySnapshot.snapshot_date == today,
                    )
                    .order_by(SimEquitySnapshot.snapshot_time.asc())
                    .first()
                )
                if first_snap:
                    daily_pnl = equity - (first_snap.equity or profile.initial_balance)

            daily_dd = 0.0
            if daily_high > 0:
                daily_dd = ((daily_high - equity) / daily_high) * 100

            total_dd = 0.0
            if high_water_mark > 0:
                total_dd = ((high_water_mark - equity) / high_water_mark) * 100

            open_count = (
                db.query(func.count(SimPosition.id))
                .filter(
                    SimPosition.profile_id == profile.id,
                    SimPosition.status.in_(["OPEN", "PARTIAL"]),
                )
                .scalar() or 0
            )

            snap = SimEquitySnapshot(
                profile_id=profile.id,
                equity=round(equity, 2),
                balance=round(profile.current_balance, 2),
                unrealized_pnl=round(float(unrealized), 2),
                daily_pnl=round(daily_pnl, 2),
                daily_high=round(daily_high, 2),
                high_water_mark=round(high_water_mark, 2),
                daily_drawdown_pct=round(daily_dd, 2),
                total_drawdown_pct=round(total_dd, 2),
                open_positions=open_count,
                snapshot_date=today,
            )
            db.add(snap)

            # Check daily drawdown halt
            if daily_dd >= profile.max_daily_loss_pct:
                profile.is_active = False
                logger.warning(
                    f"DAILY DD HALT | {profile.name} | DD={daily_dd:.1f}% >= "
                    f"{profile.max_daily_loss_pct}% | profile deactivated"
                )

        db.flush()

    async def daily_reset(self, db: Session) -> None:
        """Reset daily PnL tracking. Re-enable daily-halted profiles."""
        profiles = db.query(SimProfile).filter(SimProfile.is_active == False).all()
        for profile in profiles:
            # Check if halted by total drawdown
            latest = (
                db.query(SimEquitySnapshot)
                .filter(SimEquitySnapshot.profile_id == profile.id)
                .order_by(SimEquitySnapshot.snapshot_time.desc())
                .first()
            )
            if latest and latest.total_drawdown_pct and latest.total_drawdown_pct >= profile.max_total_loss_pct:
                continue  # Don't re-enable total drawdown halt

            profile.is_active = True
            logger.info(f"Daily reset: re-enabled {profile.name}")

        db.flush()
