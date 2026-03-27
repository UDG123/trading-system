"""
Smart Money Concepts (SMC) Analyzer
Detects BOS, CHoCH, FVG, Order Blocks, Swing Points, and Liquidity Sweeps.
Uses smartmoneyconcepts library where available, with manual fallbacks.
"""
import logging
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger("TradingSystem.SignalEngine.SMC")

# Swing detection lookback
SWING_LOOKBACK = 5


class SMCAnalyzer:
    """Smart Money Concepts analysis on OHLCV data."""

    def analyze(self, df: pd.DataFrame, symbol: str) -> Optional[Dict]:
        """
        Run full SMC analysis on a DataFrame.
        Returns dict with structure events and levels.
        """
        if df is None or len(df) < 20:
            return None

        try:
            high = df["high"].astype(float).values
            low = df["low"].astype(float).values
            close = df["close"].astype(float).values
            open_ = df["open"].astype(float).values

            result: Dict = {}

            # Try smartmoneyconcepts library first
            smc_available = False
            try:
                import smartmoneyconcepts as smc
                smc_available = True
            except ImportError:
                pass

            # ── Swing Highs / Lows ──
            swing_highs, swing_lows = self._detect_swings(high, low, SWING_LOOKBACK)
            result["swing_highs"] = swing_highs[-3:] if swing_highs else []
            result["swing_lows"] = swing_lows[-3:] if swing_lows else []

            # ── Break of Structure (BOS) ──
            bos = self._detect_bos(high, low, close, swing_highs, swing_lows)
            result["bos_bull"] = bos.get("bull", False)
            result["bos_bear"] = bos.get("bear", False)
            result["bos_level"] = bos.get("level")

            # ── Change of Character (CHoCH) ──
            choch = self._detect_choch(high, low, close, swing_highs, swing_lows)
            result["choch_bull"] = choch.get("bull", False)
            result["choch_bear"] = choch.get("bear", False)

            # ── Fair Value Gaps (FVG) ──
            fvgs = self._detect_fvg(high, low, open_, close)
            result["fvg_bull"] = [f for f in fvgs if f["type"] == "bull"][-3:]
            result["fvg_bear"] = [f for f in fvgs if f["type"] == "bear"][-3:]
            result["fvg_at_price"] = self._fvg_at_price(fvgs, close[-1])

            # ── Order Blocks ──
            obs = self._detect_order_blocks(high, low, open_, close)
            result["ob_bull"] = [o for o in obs if o["type"] == "bull"][-3:]
            result["ob_bear"] = [o for o in obs if o["type"] == "bear"][-3:]
            result["ob_at_price"] = self._ob_at_price(obs, close[-1])

            # ── Liquidity Sweeps ──
            result["liq_sweep_high"] = self._detect_liquidity_sweep(
                high, low, close, swing_highs, direction="high"
            )
            result["liq_sweep_low"] = self._detect_liquidity_sweep(
                high, low, close, swing_lows, direction="low"
            )

            return result

        except Exception as e:
            logger.debug(f"SMC analysis failed for {symbol}: {e}")
            return None

    def _detect_swings(
        self, high: np.ndarray, low: np.ndarray, lookback: int
    ) -> tuple:
        """Detect swing highs and lows using N-bar lookback."""
        swing_highs: List[Dict] = []
        swing_lows: List[Dict] = []

        for i in range(lookback, len(high) - lookback):
            # Swing high: higher than all N bars before and after
            if high[i] == max(high[i - lookback:i + lookback + 1]):
                swing_highs.append({"index": i, "price": float(high[i])})

            # Swing low: lower than all N bars before and after
            if low[i] == min(low[i - lookback:i + lookback + 1]):
                swing_lows.append({"index": i, "price": float(low[i])})

        return swing_highs, swing_lows

    def _detect_bos(
        self, high: np.ndarray, low: np.ndarray, close: np.ndarray,
        swing_highs: List[Dict], swing_lows: List[Dict],
    ) -> Dict:
        """
        Break of Structure: price breaks the most recent swing high (bullish BOS)
        or swing low (bearish BOS) in the direction of the trend.
        """
        result = {"bull": False, "bear": False, "level": None}
        latest_close = close[-1]

        # Bullish BOS: close breaks above most recent swing high
        if swing_highs:
            last_sh = swing_highs[-1]
            if latest_close > last_sh["price"] and last_sh["index"] < len(close) - 1:
                result["bull"] = True
                result["level"] = last_sh["price"]

        # Bearish BOS: close breaks below most recent swing low
        if swing_lows:
            last_sl = swing_lows[-1]
            if latest_close < last_sl["price"] and last_sl["index"] < len(close) - 1:
                result["bear"] = True
                result["level"] = last_sl["price"]

        return result

    def _detect_choch(
        self, high: np.ndarray, low: np.ndarray, close: np.ndarray,
        swing_highs: List[Dict], swing_lows: List[Dict],
    ) -> Dict:
        """
        Change of Character: break in the opposite direction of the prevailing trend.
        Bullish CHoCH: In a downtrend (lower lows), price breaks above a swing high.
        Bearish CHoCH: In an uptrend (higher highs), price breaks below a swing low.
        """
        result = {"bull": False, "bear": False}

        # Need at least 2 swing points to determine trend
        if len(swing_lows) >= 2 and len(swing_highs) >= 1:
            # Downtrend: consecutive lower lows
            if swing_lows[-1]["price"] < swing_lows[-2]["price"]:
                # CHoCH bull: price breaks above last swing high during downtrend
                last_sh = swing_highs[-1]
                if close[-1] > last_sh["price"]:
                    result["bull"] = True

        if len(swing_highs) >= 2 and len(swing_lows) >= 1:
            # Uptrend: consecutive higher highs
            if swing_highs[-1]["price"] > swing_highs[-2]["price"]:
                # CHoCH bear: price breaks below last swing low during uptrend
                last_sl = swing_lows[-1]
                if close[-1] < last_sl["price"]:
                    result["bear"] = True

        return result

    def _detect_fvg(
        self, high: np.ndarray, low: np.ndarray,
        open_: np.ndarray, close: np.ndarray,
    ) -> List[Dict]:
        """
        Fair Value Gaps: imbalance zones where the high of bar[i-2] < low of bar[i]
        (bullish) or low of bar[i-2] > high of bar[i] (bearish).
        Only return FVGs from the last 20 bars.
        """
        fvgs = []
        start = max(2, len(high) - 20)

        for i in range(start, len(high)):
            # Bullish FVG: gap between bar[i-2] high and bar[i] low
            if low[i] > high[i - 2]:
                fvgs.append({
                    "type": "bull",
                    "top": float(low[i]),
                    "bottom": float(high[i - 2]),
                    "index": i,
                })
            # Bearish FVG: gap between bar[i] high and bar[i-2] low
            if high[i] < low[i - 2]:
                fvgs.append({
                    "type": "bear",
                    "top": float(low[i - 2]),
                    "bottom": float(high[i]),
                    "index": i,
                })

        return fvgs

    def _detect_order_blocks(
        self, high: np.ndarray, low: np.ndarray,
        open_: np.ndarray, close: np.ndarray,
    ) -> List[Dict]:
        """
        Order Blocks: last opposing candle before a strong move.
        Bullish OB: last bearish candle before a bullish impulse.
        Bearish OB: last bullish candle before a bearish impulse.
        """
        obs = []
        start = max(1, len(high) - 20)

        for i in range(start, len(high) - 1):
            body_size = abs(close[i + 1] - open_[i + 1])
            prev_body = abs(close[i] - open_[i])
            atr_approx = np.mean(high[max(0, i - 13):i + 1] - low[max(0, i - 13):i + 1])

            # Strong move = body > 1.5x ATR
            if body_size < atr_approx * 1.5:
                continue

            # Bullish OB: bearish candle (close < open) before bullish impulse
            if close[i] < open_[i] and close[i + 1] > open_[i + 1]:
                obs.append({
                    "type": "bull",
                    "top": float(open_[i]),
                    "bottom": float(close[i]),
                    "index": i,
                })

            # Bearish OB: bullish candle (close > open) before bearish impulse
            if close[i] > open_[i] and close[i + 1] < open_[i + 1]:
                obs.append({
                    "type": "bear",
                    "top": float(close[i]),
                    "bottom": float(open_[i]),
                    "index": i,
                })

        return obs

    def _fvg_at_price(self, fvgs: List[Dict], price: float) -> Optional[str]:
        """Check if price is inside any active FVG. Returns 'bull' or 'bear' or None."""
        for fvg in reversed(fvgs):
            if fvg["bottom"] <= price <= fvg["top"]:
                return fvg["type"]
        return None

    def _ob_at_price(self, obs: List[Dict], price: float) -> Optional[str]:
        """Check if price is inside any active order block."""
        for ob in reversed(obs):
            if ob["bottom"] <= price <= ob["top"]:
                return ob["type"]
        return None

    def _detect_liquidity_sweep(
        self, high: np.ndarray, low: np.ndarray, close: np.ndarray,
        swing_points: List[Dict], direction: str,
    ) -> bool:
        """
        Liquidity sweep: price wicks beyond a swing point but closes back inside.
        direction='high': price wicks above swing high, closes below.
        direction='low': price wicks below swing low, closes above.
        """
        if not swing_points:
            return False

        latest_idx = len(close) - 1
        for sp in reversed(swing_points):
            # Only check recent swing points
            if sp["index"] < latest_idx - 20:
                break

            if direction == "high":
                # Wick above swing high but close below it
                if high[latest_idx] > sp["price"] and close[latest_idx] < sp["price"]:
                    return True
            elif direction == "low":
                # Wick below swing low but close above it
                if low[latest_idx] < sp["price"] and close[latest_idx] > sp["price"]:
                    return True

        return False
