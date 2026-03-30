"""
Mean Reversion Strategy — active only during RANGING HMM regime.
Uses Bollinger Band bounce + Keltner Channel confirmation + RSI extremes.
"""
import logging
from typing import Dict, Optional

logger = logging.getLogger("TradingSystem.SignalEngine.MeanReversion")

# Only trade when price is inside Keltner Channel (confirms range, not breakout)
MIN_RSI_LONG = 35   # RSI below this → oversold → LONG candidate
MAX_RSI_SHORT = 65  # RSI above this → overbought → SHORT candidate


class MeanReversionStrategy:
    """Generates mean-reversion signals during RANGING regime."""

    def evaluate(
        self, symbol: str, timeframe: str, desk_id: str, indicators: Dict,
    ) -> Optional[Dict]:
        """
        Check for mean-reversion setup.
        Returns dict with direction, alert_type, sl, tp1, tp2, strategy
        or None if no setup.
        """
        price = indicators.get("price", 0)
        bb_lower = indicators.get("bb_lower")
        bb_upper = indicators.get("bb_upper")
        bb_mid = indicators.get("bb_mid")
        kc_lower = indicators.get("kc_lower")
        kc_upper = indicators.get("kc_upper")
        rsi = indicators.get("rsi", 50)

        if not all([price, bb_lower, bb_upper, bb_mid, kc_lower, kc_upper]):
            return None

        # Confirm range-bound: BB must be INSIDE KC (squeeze or near-squeeze)
        bb_inside_kc = bb_upper < kc_upper and bb_lower > kc_lower
        # Relaxed: BB width < 1.5x KC width
        bb_width = bb_upper - bb_lower
        kc_width = kc_upper - kc_lower
        range_confirmed = bb_inside_kc or (kc_width > 0 and bb_width < kc_width * 1.5)

        if not range_confirmed:
            return None

        direction = None
        alert_type = None

        # LONG: Price at lower BB + RSI oversold
        if price <= bb_lower * 1.002 and rsi < MIN_RSI_LONG:
            direction = "LONG"
            alert_type = "contrarian_bullish"

        # SHORT: Price at upper BB + RSI overbought
        elif price >= bb_upper * 0.998 and rsi > MAX_RSI_SHORT:
            direction = "SHORT"
            alert_type = "contrarian_bearish"

        if not direction:
            return None

        # Compute SL/TP for mean reversion (BB-based)
        if direction == "LONG":
            sl = round(bb_lower * 0.998, 5)    # SL below lower BB
            tp1 = round(bb_mid, 5)              # TP1 at BB midline (20 SMA)
            tp2 = round(bb_upper * 0.998, 5)    # TP2 at upper BB
        else:
            sl = round(bb_upper * 1.002, 5)     # SL above upper BB
            tp1 = round(bb_mid, 5)              # TP1 at BB midline
            tp2 = round(bb_lower * 1.002, 5)    # TP2 at lower BB

        logger.info(
            f"MEAN REV | {symbol} {direction} | RSI={rsi:.1f} | "
            f"Price={price} BB=[{bb_lower:.5f}, {bb_upper:.5f}] | "
            f"Desk: {desk_id}"
        )

        return {
            "direction": direction,
            "alert_type": alert_type,
            "sl": sl,
            "tp1": tp1,
            "tp2": tp2,
            "strategy": "mean_reversion",
        }
