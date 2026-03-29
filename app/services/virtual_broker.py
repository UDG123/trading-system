"""
VirtualBroker — Realistic trade execution simulator.
Models dynamic spreads, slippage, commission, fill probability, and latency.
Used for shadow simulation and backtesting data collection.
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

    # ── Zombie trade time limits (minutes) for 0.5R check ──
    ZOMBIE_LIMITS = {
        "DESK1_SCALPER": 50,
        "DESK2_INTRADAY": 240,
        "DESK3_SWING": 4320,       # 3 days
        "DESK4_GOLD": 120,
        "DESK5_ALTS": 360,
        "DESK6_EQUITIES": 1440,    # 1 day
    }

    async def check_exits(
        self, db: Session, current_prices: Dict[str, float]
    ) -> List[Dict]:
        """
        Three-tier partial profit exit system.

        Tier 1: Close 33% at 1R, move SL to breakeven.
        Tier 2: Close 33% at 3R, trail SL to 1R level.
        Tier 3: Trail remaining 34% with chandelier stop
                (highest_high(22) - ATR(14)*3 for longs).
        Plus zombie trade time-based exits.
        """
        closed = []

        open_positions = (
            db.query(SimPosition)
            .filter(SimPosition.status.in_(["OPEN", "PARTIAL"]))
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
            entry = pos.entry_price
            original_sl = pos.stop_loss or entry  # fallback

            # Risk distance (1R) in price terms
            risk_distance = abs(entry - original_sl) if original_sl else 0
            if risk_distance <= 0:
                risk_distance = entry * 0.01  # 1% fallback

            # Current pips
            if direction == "LONG":
                unrealized_pips = (price - entry) / pip_size
            else:
                unrealized_pips = (entry - price) / pip_size

            pos.current_price = price
            pos.unrealized_pnl = round(unrealized_pips * pip_value * pos.lot_size, 2)

            # Update MFE/MAE
            if direction == "LONG":
                fav = (price - entry) / pip_size
                adv = (entry - price) / pip_size
            else:
                fav = (entry - price) / pip_size
                adv = (price - entry) / pip_size
            pos.max_favorable_pips = max(pos.max_favorable_pips or 0, fav)
            pos.max_adverse_pips = max(pos.max_adverse_pips or 0, adv)

            # Current R-multiple
            current_r = unrealized_pips * pip_size / risk_distance if risk_distance > 0 else 0

            partial_pct = pos.partial_close_pct or 0
            exit_reason = None
            exit_price = None

            # ── SL hit (always checked first) ──
            if pos.stop_loss:
                if direction == "LONG" and price <= pos.stop_loss:
                    exit_reason = "SL_HIT"
                    exit_price = pos.stop_loss
                elif direction == "SHORT" and price >= pos.stop_loss:
                    exit_reason = "SL_HIT"
                    exit_price = pos.stop_loss

            # ── Tier 1: Close 33% at 1R, move SL to breakeven ──
            if not exit_reason and partial_pct < 33:
                tier1_target = entry + risk_distance if direction == "LONG" else entry - risk_distance
                hit = (direction == "LONG" and price >= tier1_target) or \
                      (direction == "SHORT" and price <= tier1_target)
                if hit:
                    tier1_pips = risk_distance / pip_size  # 1R in pips
                    tier1_pnl = round(tier1_pips * pip_value * pos.lot_size * 0.33, 2)
                    pos.partial_close_pct = 33.0
                    pos.partial_pnl = (pos.partial_pnl or 0) + tier1_pnl
                    pos.partial_pnl_tier1 = tier1_pnl
                    # Move SL to breakeven
                    pos.stop_loss = entry
                    pos.breakeven_price = entry
                    pos.status = "PARTIAL"
                    logger.debug(
                        f"TIER1 | {pos.symbol} {direction} | 33% closed at 1R | "
                        f"PnL: ${tier1_pnl} | SL→BE"
                    )

            # ── Tier 2: Close 33% at 3R, trail SL to 1R level ──
            if not exit_reason and 33 <= partial_pct < 66:
                tier2_target = entry + (risk_distance * 3) if direction == "LONG" else entry - (risk_distance * 3)
                hit = (direction == "LONG" and price >= tier2_target) or \
                      (direction == "SHORT" and price <= tier2_target)
                if hit:
                    tier2_pips = (risk_distance * 3) / pip_size
                    tier2_pnl = round(tier2_pips * pip_value * pos.lot_size * 0.33, 2)
                    pos.partial_close_pct = 66.0
                    pos.partial_pnl = (pos.partial_pnl or 0) + tier2_pnl
                    pos.partial_pnl_tier2 = tier2_pnl
                    # Trail SL to 1R profit level
                    if direction == "LONG":
                        pos.stop_loss = entry + risk_distance
                    else:
                        pos.stop_loss = entry - risk_distance
                    pos.status = "PARTIAL"
                    logger.debug(
                        f"TIER2 | {pos.symbol} {direction} | 33% closed at 3R | "
                        f"PnL: ${tier2_pnl} | SL→1R"
                    )

            # ── Tier 3: Trail remaining 34% with chandelier stop ──
            if not exit_reason and partial_pct >= 66:
                chandelier_stop = self._compute_chandelier_stop(
                    pos.symbol, direction, db
                )
                if chandelier_stop:
                    pos.trailing_stop = chandelier_stop
                    if direction == "LONG" and price <= chandelier_stop:
                        exit_reason = "TRAILING_T3"
                        exit_price = chandelier_stop
                        pos.exit_tier = 3
                    elif direction == "SHORT" and price >= chandelier_stop:
                        exit_reason = "TRAILING_T3"
                        exit_price = chandelier_stop
                        pos.exit_tier = 3

            # ── Zombie trade time-based exit ──
            if not exit_reason and pos.entry_time:
                zombie_limit = self.ZOMBIE_LIMITS.get(pos.desk_id, 1440)
                hold_mins = (datetime.now(timezone.utc) - pos.entry_time).total_seconds() / 60
                if hold_mins >= zombie_limit and current_r < 0.5:
                    exit_reason = "ZOMBIE_EXIT"
                    exit_price = price
                    pos.time_based_exit = True
                    logger.debug(
                        f"ZOMBIE | {pos.symbol} {direction} | {hold_mins:.0f}min | "
                        f"R={current_r:.2f} < 0.5R — closing"
                    )

            # ── Hard time exit (desk max_hold_hours) ──
            if not exit_reason and pos.entry_time:
                desk = DESKS.get(pos.desk_id, {})
                max_hold = desk.get("max_hold_hours", 24)
                if datetime.now(timezone.utc) - pos.entry_time > timedelta(hours=max_hold):
                    exit_reason = "TIME_EXIT"
                    exit_price = price
                    pos.time_based_exit = True

            # ── Close position if full exit triggered ──
            if exit_reason:
                pos.exit_reason = exit_reason
                if not pos.exit_tier:
                    if exit_reason == "SL_HIT" and partial_pct < 33:
                        pos.exit_tier = 0  # never made it to tier 1
                    elif exit_reason == "SL_HIT" and partial_pct < 66:
                        pos.exit_tier = 1
                    elif exit_reason == "SL_HIT":
                        pos.exit_tier = 2
                result = self._close_position(pos, exit_price or price, pip_size, pip_value, db)
                closed.append(result)

        if closed:
            db.flush()
            logger.debug(f"Sim exits: {len(closed)} positions closed")

        return closed

    def _close_position(
        self, pos: SimPosition, exit_price: float,
        pip_size: float, pip_value: float, db: Session,
    ) -> Dict:
        """Finalize position close with partial PnL accounting."""
        direction = pos.direction.upper()

        if direction == "LONG":
            pnl_pips = (exit_price - pos.entry_price) / pip_size
        else:
            pnl_pips = (pos.entry_price - exit_price) / pip_size

        remaining_pct = 1.0 - (pos.partial_close_pct or 0) / 100
        final_pnl = round(pnl_pips * pip_value * pos.lot_size * remaining_pct, 2)
        total_pnl = final_pnl + (pos.partial_pnl or 0)

        # Store tier 3 PnL if this is the trailing close of the last 34%
        if pos.partial_close_pct and pos.partial_close_pct >= 66:
            pos.partial_pnl_tier3 = final_pnl
            if not pos.exit_tier:
                pos.exit_tier = 3

        hold_minutes = 0.0
        if pos.entry_time:
            hold_minutes = (datetime.now(timezone.utc) - pos.entry_time).total_seconds() / 60

        pos.exit_price = exit_price
        pos.exit_time = datetime.now(timezone.utc)
        # exit_reason already set on pos by check_exits before calling this method
        pos.realized_pnl_pips = round(pnl_pips, 2)
        pos.realized_pnl_dollars = final_pnl
        pos.hold_time_minutes = round(hold_minutes, 1)
        pos.commission_total = 0.0
        pos.swap_total = 0.0
        pos.net_pnl = round(total_pnl, 2)
        pos.status = "CLOSED"

        # Update profile balance
        profile = db.query(SimProfile).filter(SimProfile.id == pos.profile_id).first()
        if profile:
            profile.current_balance += total_pnl

        return {
            "position_id": pos.id,
            "profile_id": pos.profile_id,
            "symbol": pos.symbol,
            "direction": pos.direction,
            "exit_reason": pos.exit_reason,
            "exit_tier": pos.exit_tier,
            "pnl_pips": round(pnl_pips, 2),
            "pnl_dollars": round(total_pnl, 2),
            "tier1_pnl": pos.partial_pnl_tier1,
            "tier2_pnl": pos.partial_pnl_tier2,
            "tier3_pnl": pos.partial_pnl_tier3,
            "time_based": pos.time_based_exit or False,
            "hold_minutes": round(hold_minutes, 1),
        }

    def _compute_chandelier_stop(
        self, symbol: str, direction: str, db: Session,
        lookback: int = 22, atr_mult: float = 3.0,
    ) -> Optional[float]:
        """
        Chandelier stop for Tier 3 trailing.
        Long:  highest_high(22) - ATR(14) * 3.0
        Short: lowest_low(22) + ATR(14) * 3.0
        """
        try:
            from sqlalchemy import text
            # Use 1H candles for chandelier computation
            rows = db.execute(
                text("""
                    SELECT high, low, close FROM ohlcv_1h
                    WHERE symbol = :sym
                    ORDER BY time DESC
                    LIMIT :n
                """),
                {"sym": symbol, "n": lookback + 14},
            ).fetchall()

            if not rows or len(rows) < lookback:
                return None

            import numpy as np
            highs = np.array([float(r[0]) for r in reversed(rows)])
            lows = np.array([float(r[1]) for r in reversed(rows)])
            closes = np.array([float(r[2]) for r in reversed(rows)])

            # ATR(14)
            tr = np.maximum(
                highs[1:] - lows[1:],
                np.maximum(
                    np.abs(highs[1:] - closes[:-1]),
                    np.abs(lows[1:] - closes[:-1]),
                ),
            )
            atr = float(np.mean(tr[-14:])) if len(tr) >= 14 else float(np.mean(tr))

            if direction == "LONG":
                highest = float(np.max(highs[-lookback:]))
                return round(highest - atr * atr_mult, 5)
            else:
                lowest = float(np.min(lows[-lookback:]))
                return round(lowest + atr * atr_mult, 5)

        except Exception:
            return None

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
