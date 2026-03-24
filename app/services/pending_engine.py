"""
Pending Entry Engine — Parks signals for pullback entries and re-evaluates periodically.

When a CTO-approved signal fires at breakout but price is overextended (RSI extreme
or too far from EMA50), the signal is "parked" with a calculated entry target.
A scheduler checks every 30s if price has pulled back to the target.

If triggered → creates SIM_OPEN trade + Telegram alert.
If TTL expires → marks as EXPIRED.
If SL hit while pending → marks as CANCELLED.
"""
import logging
from datetime import datetime, timezone, timedelta

from app.models.pending_signal import PendingSignal

logger = logging.getLogger("TradingSystem.PendingEngine")


class PendingEngine:
    """Parks signals for pullback entries and re-evaluates periodically."""

    # TTL by desk style (in minutes)
    TTL_MAP = {
        "DESK1_SCALPER": 15,      # 3 bars on 5M
        "DESK2_INTRADAY": 180,    # 3 hours
        "DESK3_SWING": 720,       # 12 hours
        "DESK4_GOLD": 120,        # 2 hours (intra default)
        "DESK5_ALTS": 360,        # 6 hours
        "DESK6_EQUITIES": 180,    # 3 hours
    }

    def should_park(self, signal_data, enrichment, decision) -> bool:
        """Decide if signal should be parked for better entry."""
        # Park if: CTO approved but entry timing is suboptimal
        if decision.get("decision") not in ("EXECUTE", "REDUCE"):
            return False

        # Don't park EXIT signals
        if signal_data.get("direction") == "EXIT":
            return False

        # Park if price is extended beyond EMA50 (overextended entry)
        ema50 = enrichment.get("ema50")
        price = signal_data.get("price", 0)
        if ema50 and price:
            extension_pct = abs(price - ema50) / ema50 * 100
            if extension_pct > 0.3:  # more than 0.3% from EMA50
                return True

        # Park if RSI is extreme against direction
        rsi = enrichment.get("rsi")
        direction = signal_data.get("direction", "")
        if rsi:
            if direction == "LONG" and rsi > 65:
                return True
            if direction == "SHORT" and rsi < 35:
                return True

        return False

    def calculate_entry_target(self, signal_data, enrichment, desk_id) -> tuple:
        """Calculate optimal entry level. Returns (entry_price, method)."""
        price = signal_data.get("price", 0)
        direction = signal_data.get("direction", "LONG")
        atr = enrichment.get("atr")
        ema50 = enrichment.get("ema50")

        if not price or price <= 0:
            return price, "IMMEDIATE"

        # Method 1: MAE offset (use 50% of ATR as pullback target)
        if atr and atr > 0:
            offset = atr * 0.5
            if direction == "LONG":
                target = price - offset
            else:
                target = price + offset
            return round(target, 5), "MAE_OFFSET"

        # Method 2: EMA pullback
        if ema50:
            midpoint = (price + ema50) / 2
            return round(midpoint, 5), "EMA_PULLBACK"

        return price, "IMMEDIATE"

    async def park_signal(self, db, signal_id, signal_data, enrichment, decision, desk_id):
        """Park a signal for later re-evaluation."""
        entry_target, method = self.calculate_entry_target(signal_data, enrichment, desk_id)

        ttl_minutes = self.TTL_MAP.get(desk_id, 120)
        expiry = datetime.now(timezone.utc) + timedelta(minutes=ttl_minutes)

        pending = PendingSignal(
            signal_id=signal_id,
            symbol=signal_data.get("symbol"),
            direction=signal_data.get("direction"),
            alert_type=signal_data.get("alert_type"),
            desk_id=desk_id,
            timeframe=signal_data.get("timeframe"),
            signal_price=signal_data.get("price"),
            entry_target=entry_target,
            stop_loss=signal_data.get("sl1"),
            take_profit_1=signal_data.get("tp1"),
            take_profit_2=signal_data.get("tp2"),
            entry_method=method,
            ttl_expiry=expiry,
            consensus_score=decision.get("consensus_score"),
            ml_score=decision.get("ml_score"),
            enrichment_snapshot=enrichment,
        )
        db.add(pending)
        db.flush()

        logger.info(
            f"PARKED | #{pending.id} | {signal_data.get('symbol')} {signal_data.get('direction')} | "
            f"Target: {entry_target} ({method}) | TTL: {ttl_minutes}min | Desk: {desk_id}"
        )
        return pending.id

    async def check_pending(self, db, price_service):
        """Re-evaluate all pending signals against current prices. Called every 30s by scheduler."""
        now = datetime.now(timezone.utc)

        # Expire old signals
        expired = db.query(PendingSignal).filter(
            PendingSignal.status == "PENDING",
            PendingSignal.ttl_expiry < now,
        ).all()
        for p in expired:
            p.status = "EXPIRED"
            logger.info(f"EXPIRED | Pending #{p.id} | {p.symbol} | TTL reached")

        # Check active pending signals
        active = db.query(PendingSignal).filter(
            PendingSignal.status == "PENDING",
            PendingSignal.ttl_expiry >= now,
        ).all()

        if not active:
            return []

        # Batch price fetch
        symbols = list(set(p.symbol for p in active))
        prices = await price_service.get_prices_batch(symbols)

        triggered = []
        for p in active:
            current_price = prices.get(p.symbol)
            if not current_price:
                continue

            p.check_count += 1
            p.last_checked = now

            # Check if price has reached entry target
            if p.direction == "LONG" and current_price <= p.entry_target:
                p.status = "TRIGGERED"
                p.triggered_at = now
                p.trigger_price = current_price
                triggered.append(p)
                logger.info(
                    f"TRIGGERED | Pending #{p.id} | {p.symbol} LONG | "
                    f"Price {current_price} <= target {p.entry_target}"
                )
            elif p.direction == "SHORT" and current_price >= p.entry_target:
                p.status = "TRIGGERED"
                p.triggered_at = now
                p.trigger_price = current_price
                triggered.append(p)
                logger.info(
                    f"TRIGGERED | Pending #{p.id} | {p.symbol} SHORT | "
                    f"Price {current_price} >= target {p.entry_target}"
                )

            # Also check if SL was hit while pending (cancel if so)
            if p.stop_loss:
                if p.direction == "LONG" and current_price <= p.stop_loss:
                    p.status = "CANCELLED"
                    logger.info(f"CANCELLED | Pending #{p.id} | {p.symbol} | SL hit while pending")
                elif p.direction == "SHORT" and current_price >= p.stop_loss:
                    p.status = "CANCELLED"
                    logger.info(f"CANCELLED | Pending #{p.id} | {p.symbol} | SL hit while pending")

        db.commit()
        return triggered
