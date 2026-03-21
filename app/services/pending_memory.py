"""
Pending Memory Engine — OniQuant v5.9
Parks signals that meet analysis criteria but not yet market conditions.
Evaluates parked signals on each new tick/signal and auto-expires via TTL.
"""
import logging
from datetime import datetime, timezone, timedelta
from typing import List, Optional, Dict

from sqlalchemy.orm import Session

from app.models.signal import Signal

logger = logging.getLogger("TradingSystem.PendingMemory")

# Default TTL per desk style (hours)
_DEFAULT_TTL_HOURS = {
    "DESK1_SCALPER": 0.5,    # 30 min — scalps expire fast
    "DESK2_INTRADAY": 4,     # 4 hours
    "DESK3_SWING": 48,       # 2 days
    "DESK4_GOLD": 6,         # 6 hours (mixed timeframes)
    "DESK5_ALTS": 8,         # 8 hours
    "DESK6_EQUITIES": 6,     # 6 hours (US market day)
}


class PendingSignalManager:
    """
    Manages signals parked with status=PENDING_CONDITIONS.

    Lifecycle:
      1. park_signal()   → sets status to PENDING_CONDITIONS + ttl_expiry
      2. evaluate_pending_signals() → called periodically to check conditions
      3. Expired signals are moved to EXPIRED status automatically
    """

    def park_signal(
        self,
        db: Session,
        signal: Signal,
        reason: str = "",
        ttl_hours: Optional[float] = None,
    ) -> Signal:
        """
        Park a signal in pending memory.

        Args:
            db: Database session
            signal: The Signal ORM object to park
            reason: Why it's being parked (e.g. "price_not_at_level")
            ttl_hours: Override TTL; defaults to desk-based TTL
        """
        if ttl_hours is None:
            ttl_hours = _DEFAULT_TTL_HOURS.get(signal.desk_id, 4)

        signal.status = "PENDING_CONDITIONS"
        signal.ttl_expiry = datetime.now(timezone.utc) + timedelta(hours=ttl_hours)

        db.commit()

        logger.info(
            f"PARKED | signal={signal.id} {signal.symbol_normalized} "
            f"desk={signal.desk_id} reason={reason} "
            f"ttl={ttl_hours}h expires={signal.ttl_expiry.strftime('%H:%M UTC')}"
        )

        return signal

    def get_pending_signals(self, db: Session, desk_id: Optional[str] = None) -> List[Signal]:
        """Retrieve all non-expired pending signals, optionally filtered by desk."""
        now = datetime.now(timezone.utc)
        query = db.query(Signal).filter(
            Signal.status == "PENDING_CONDITIONS",
        )
        if desk_id:
            query = query.filter(Signal.desk_id == desk_id)

        # Only return signals that haven't expired
        query = query.filter(
            (Signal.ttl_expiry.is_(None)) | (Signal.ttl_expiry > now)
        )

        return query.order_by(Signal.received_at.asc()).all()

    def evaluate_pending_signals(
        self,
        db: Session,
        current_prices: Optional[Dict[str, float]] = None,
    ) -> List[Signal]:
        """
        Evaluate all pending signals:
          1. Expire any past-TTL signals
          2. Check if market conditions are now met for remaining signals

        Args:
            db: Database session
            current_prices: Dict of symbol → current price (optional)

        Returns:
            List of signals that are now ready for execution
        """
        now = datetime.now(timezone.utc)
        ready = []

        # Fetch all PENDING_CONDITIONS signals
        pending = db.query(Signal).filter(
            Signal.status == "PENDING_CONDITIONS",
        ).all()

        expired_count = 0
        for signal in pending:
            # Check TTL expiry
            if signal.ttl_expiry and signal.ttl_expiry <= now:
                signal.status = "EXPIRED"
                expired_count += 1
                logger.info(
                    f"EXPIRED | signal={signal.id} {signal.symbol_normalized} "
                    f"desk={signal.desk_id} (TTL reached)"
                )
                continue

            # Check if market conditions are met
            if current_prices and self._conditions_met(signal, current_prices):
                ready.append(signal)
                logger.info(
                    f"READY | signal={signal.id} {signal.symbol_normalized} "
                    f"desk={signal.desk_id} — conditions met, releasing from memory"
                )

        if expired_count > 0:
            db.commit()
            logger.info(f"Expired {expired_count} pending signal(s)")

        return ready

    def _conditions_met(self, signal: Signal, current_prices: Dict[str, float]) -> bool:
        """
        Check if market conditions now satisfy the parked signal's requirements.

        Current logic: price must be within 0.1% of the original signal price
        (i.e. the market has returned to the signal's entry zone).

        This is intentionally simple — extend with ATR bands, VWAP proximity,
        or order-book depth checks as the system matures.
        """
        symbol = signal.symbol_normalized
        price_now = current_prices.get(symbol)

        if price_now is None or signal.price is None or signal.price == 0:
            return False

        # Check if current price is within 0.1% of the signal entry price
        deviation = abs(price_now - signal.price) / signal.price
        return deviation <= 0.001

    def expire_all(self, db: Session, desk_id: Optional[str] = None) -> int:
        """Force-expire all pending signals (e.g. on kill-switch)."""
        query = db.query(Signal).filter(Signal.status == "PENDING_CONDITIONS")
        if desk_id:
            query = query.filter(Signal.desk_id == desk_id)

        signals = query.all()
        for s in signals:
            s.status = "EXPIRED"

        db.commit()
        logger.info(f"Force-expired {len(signals)} pending signal(s) (desk={desk_id or 'ALL'})")
        return len(signals)

    def sync_on_startup(self, db: Session) -> List[Signal]:
        """
        Recover pending signals from PostgreSQL on startup.
        Loads all non-expired PENDING_CONDITIONS signals into the worker loop
        so the bot 'remembers' its intentions after a restart/deployment.

        Returns:
            List of active pending signals recovered from DB
        """
        now = datetime.now(timezone.utc)

        # First, expire any signals that passed their TTL during downtime
        expired = db.query(Signal).filter(
            Signal.status == "PENDING_CONDITIONS",
            Signal.ttl_expiry.isnot(None),
            Signal.ttl_expiry <= now,
        ).all()

        for s in expired:
            s.status = "EXPIRED"

        if expired:
            db.commit()
            logger.info(f"Startup: expired {len(expired)} signal(s) that timed out during downtime")

        # Load remaining active pending signals
        active = self.get_pending_signals(db)

        if active:
            symbols = {}
            for s in active:
                symbols[s.symbol_normalized] = symbols.get(s.symbol_normalized, 0) + 1

            logger.info(
                f"Startup: recovered {len(active)} pending signal(s) from PostgreSQL | "
                f"Symbols: {symbols}"
            )
        else:
            logger.info("Startup: no pending signals to recover")

        return active

    def calculate_wait_time_mins(self, signal: Signal) -> Optional[float]:
        """
        Calculate how long a signal waited in pending memory before release.
        Used to populate ml_trade_logs.pending_wait_time_mins.
        """
        if signal.status != "PENDING_CONDITIONS":
            return None
        if not signal.received_at:
            return None

        now = datetime.now(timezone.utc)
        delta = now - signal.received_at.replace(tzinfo=timezone.utc) if signal.received_at.tzinfo is None else now - signal.received_at
        return round(delta.total_seconds() / 60.0, 2)
