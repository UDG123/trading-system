"""
Triple-Barrier Labeler — Implements Lopez de Prado's triple-barrier method
to label shadow signals with WIN/LOSS/TIMEOUT outcomes.

Three barriers per signal:
  - Upper barrier (TP): profitable exit
  - Lower barrier (SL): loss exit
  - Vertical barrier (timeout): max hold bars

The label is determined by whichever barrier is touched first.
"""
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, List, Dict

from sqlalchemy import text, func
from sqlalchemy.orm import Session

from app.models.shadow_signal import ShadowSignal
from app.config import get_pip_info, get_atr_settings, DESKS

logger = logging.getLogger("TradingSystem.TripleBarrierLabeler")

# Max hold bars per desk (1-minute bars)
DESK_MAX_HOLD_BARS = {
    "DESK1_SCALPER": 90,        # 1.5h × 60
    "DESK2_INTRADAY": 480,      # 8h × 60
    "DESK3_SWING": 7200,        # 120h × 60
    "DESK4_GOLD": 360,          # 6h × 60 (default intra)
    "DESK5_ALTS": 1440,         # 24h × 60
    "DESK6_EQUITIES": 420,      # 7h × 60
}

# Minimum age before labeling (give trades time to resolve)
MIN_AGE_HOURS = 2


class TripleBarrierLabeler:
    """Labels shadow signals using the triple-barrier method."""

    def __init__(self, db_session_factory):
        self._db_factory = db_session_factory

    async def label_batch(self, limit: int = 500) -> int:
        """
        Label a batch of unlabeled shadow signals.
        Returns count of newly labeled signals.
        """
        db = self._db_factory()
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(hours=MIN_AGE_HOURS)

            unlabeled = (
                db.query(ShadowSignal)
                .filter(
                    ShadowSignal.tb_label.is_(None),
                    ShadowSignal.created_at < cutoff,
                )
                .order_by(ShadowSignal.created_at.asc())
                .limit(limit)
                .all()
            )

            if not unlabeled:
                return 0

            labeled_count = 0
            batch_count = 0

            for signal in unlabeled:
                try:
                    success = self._label_single(db, signal)
                    if success:
                        labeled_count += 1
                        batch_count += 1

                    # Commit in batches of 50
                    if batch_count >= 50:
                        db.commit()
                        batch_count = 0
                except Exception as e:
                    logger.debug(f"Label failed for shadow #{signal.id}: {e}")
                    db.rollback()

            # Final commit
            if batch_count > 0:
                db.commit()

            if labeled_count > 0:
                logger.info(f"Triple-barrier labeled {labeled_count}/{len(unlabeled)} signals")

            return labeled_count

        except Exception as e:
            logger.error(f"Label batch error: {e}")
            db.rollback()
            return 0
        finally:
            db.close()

    def _label_single(self, db: Session, signal: ShadowSignal) -> bool:
        """
        Label a single shadow signal using triple-barrier method.
        Returns True if successfully labeled.
        """
        if not signal.price or not signal.symbol or not signal.direction:
            return False

        entry_price = signal.price
        direction = signal.direction.upper()
        symbol = signal.symbol
        desk_id = signal.desk_id or "DESK2_INTRADAY"
        timeframe = signal.timeframe or "1H"

        pip_size, _ = get_pip_info(symbol)
        if pip_size <= 0:
            pip_size = 0.0001

        # Determine barriers
        atr_cfg = get_atr_settings(desk_id, symbol, timeframe)
        sl_mult = atr_cfg.get("sl_mult", 2.0)
        tp_mult = atr_cfg.get("tp1_mult", 4.0)

        # Use explicit SL/TP if available, otherwise ATR-computed
        if signal.sl1 and signal.tp1:
            if direction == "LONG":
                upper_barrier = signal.tp1
                lower_barrier = signal.sl1
            else:
                upper_barrier = signal.sl1  # For SHORT, SL is above
                lower_barrier = signal.tp1  # TP is below
        elif signal.atr and signal.atr > 0:
            atr = signal.atr
            if direction == "LONG":
                upper_barrier = entry_price + (atr * tp_mult)
                lower_barrier = entry_price - (atr * sl_mult)
            else:
                upper_barrier = entry_price + (atr * sl_mult)
                lower_barrier = entry_price - (atr * tp_mult)
        else:
            # Fallback: use percentage-based barriers
            sl_pct = 0.01  # 1%
            tp_pct = 0.02  # 2%
            if direction == "LONG":
                upper_barrier = entry_price * (1 + tp_pct)
                lower_barrier = entry_price * (1 - sl_pct)
            else:
                upper_barrier = entry_price * (1 + sl_pct)
                lower_barrier = entry_price * (1 - tp_pct)

        max_hold_bars = self._get_max_hold_bars(desk_id, timeframe)

        # Get OHLCV bars from ohlcv_1m table
        bars = self._get_ohlcv_bars(db, symbol, signal.created_at, max_hold_bars)

        if not bars or len(bars) < 5:
            # Not enough data to label
            return False

        # Walk through bars checking barriers
        label = None
        barrier_hit = None
        hold_bars = 0
        exit_price = None

        # Track MFE and MAE
        max_favorable = 0.0
        max_adverse = 0.0

        for i, bar in enumerate(bars):
            bar_high = float(bar["high"])
            bar_low = float(bar["low"])
            bar_close = float(bar["close"])
            hold_bars = i + 1

            # Update MFE/MAE
            if direction == "LONG":
                favorable = (bar_high - entry_price) / pip_size
                adverse = (entry_price - bar_low) / pip_size
            else:
                favorable = (entry_price - bar_low) / pip_size
                adverse = (bar_high - entry_price) / pip_size

            max_favorable = max(max_favorable, favorable)
            max_adverse = max(max_adverse, adverse)

            # Check barriers
            if direction == "LONG":
                if bar_high >= upper_barrier:
                    label = 1  # WIN
                    barrier_hit = "TP"
                    exit_price = upper_barrier
                    break
                if bar_low <= lower_barrier:
                    label = -1  # LOSS
                    barrier_hit = "SL"
                    exit_price = lower_barrier
                    break
            else:  # SHORT
                if bar_low <= lower_barrier:
                    label = 1  # WIN (TP hit for short)
                    barrier_hit = "TP"
                    exit_price = lower_barrier
                    break
                if bar_high >= upper_barrier:
                    label = -1  # LOSS (SL hit for short)
                    barrier_hit = "SL"
                    exit_price = upper_barrier
                    break

            # Vertical barrier (timeout)
            if hold_bars >= max_hold_bars:
                exit_price = bar_close
                if direction == "LONG":
                    ret = (bar_close - entry_price) / pip_size
                else:
                    ret = (entry_price - bar_close) / pip_size
                label = 0  # TIMEOUT, but sign by return
                barrier_hit = "TIMEOUT"
                break

        if label is None:
            # Didn't reach any barrier (not enough bars)
            return False

        # Compute return
        if exit_price:
            if direction == "LONG":
                tb_return = (exit_price - entry_price) / pip_size
            else:
                tb_return = (entry_price - exit_price) / pip_size
        else:
            tb_return = 0.0

        hold_minutes = hold_bars * 1.0  # 1-minute bars

        # Update shadow signal
        signal.tb_label = label
        signal.tb_return = round(tb_return, 2)
        signal.tb_barrier_hit = barrier_hit
        signal.tb_hold_bars = hold_bars
        signal.tb_hold_minutes = round(hold_minutes, 1)
        signal.tb_max_favorable = round(max_favorable, 2)
        signal.tb_max_adverse = round(max_adverse, 2)
        signal.tb_labeled_at = datetime.now(timezone.utc)
        signal.meta_label = (label == 1)

        return True

    def _get_max_hold_bars(self, desk_id: str, timeframe: str) -> int:
        """Map desk max hold hours to 1-minute bars, adjusted by timeframe."""
        base_bars = DESK_MAX_HOLD_BARS.get(desk_id, 480)

        # Adjust by timeframe (base assumes 1M bars)
        tf = timeframe.upper() if timeframe else ""
        if "5" in tf and "15" not in tf:
            # 5M signals get more bars (5x)
            return base_bars
        elif "15" in tf:
            return base_bars
        elif tf in ("1H", "60"):
            return base_bars
        elif "4" in tf:
            return base_bars
        elif "D" in tf or "W" in tf:
            return min(base_bars * 5, 14400)  # Cap at 10 days

        return base_bars

    def _get_ohlcv_bars(
        self, db: Session, symbol: str, start_time: datetime, max_bars: int
    ) -> List[Dict]:
        """
        Fetch 1-minute OHLCV bars from the ohlcv_1m table.
        Returns list of dicts with open, high, low, close, volume.
        """
        try:
            result = db.execute(
                text("""
                    SELECT time, open, high, low, close, volume
                    FROM ohlcv_1m
                    WHERE symbol = :symbol
                      AND time >= :start_time
                    ORDER BY time ASC
                    LIMIT :max_bars
                """),
                {"symbol": symbol, "start_time": start_time, "max_bars": max_bars},
            )

            bars = []
            for row in result:
                bars.append({
                    "time": row[0],
                    "open": row[1],
                    "high": row[2],
                    "low": row[3],
                    "close": row[4],
                    "volume": row[5],
                })
            return bars

        except Exception as e:
            logger.debug(f"OHLCV fetch failed for {symbol}: {e}")
            return []

    def _compute_mfe_mae(
        self,
        bars: List[Dict],
        entry_price: float,
        direction: str,
        pip_size: float,
    ) -> Tuple[float, float]:
        """
        Compute MFE (max favorable excursion) and MAE (max adverse excursion) in pips.
        """
        max_favorable = 0.0
        max_adverse = 0.0

        for bar in bars:
            high = float(bar["high"])
            low = float(bar["low"])

            if direction == "LONG":
                favorable = (high - entry_price) / pip_size
                adverse = (entry_price - low) / pip_size
            else:
                favorable = (entry_price - low) / pip_size
                adverse = (high - entry_price) / pip_size

            max_favorable = max(max_favorable, favorable)
            max_adverse = max(max_adverse, adverse)

        return round(max_favorable, 2), round(max_adverse, 2)
