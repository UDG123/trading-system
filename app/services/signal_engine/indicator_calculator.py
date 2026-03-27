"""
Technical Indicator Calculator
Computes all indicators using the `ta` library + custom implementations.
Returns a flat dict of indicator values for a given symbol-timeframe DataFrame.
"""
import logging
from typing import Dict, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger("TradingSystem.SignalEngine.Indicators")

# Minimum bars required to compute indicators
MIN_BARS = 50


def wavetrend(high: pd.Series, low: pd.Series, close: pd.Series,
              channel_length: int = 9, avg_length: int = 12) -> tuple:
    """WaveTrend oscillator (LazyBear implementation)."""
    hlc3 = (high + low + close) / 3
    esa = hlc3.ewm(span=channel_length, adjust=False).mean()
    d = abs(hlc3 - esa).ewm(span=channel_length, adjust=False).mean()
    ci = (hlc3 - esa) / (0.015 * d.replace(0, np.nan))
    ci = ci.fillna(0)
    wt1 = ci.ewm(span=avg_length, adjust=False).mean()
    wt2 = wt1.rolling(window=4).mean()
    return wt1, wt2


def supertrend(high: pd.Series, low: pd.Series, close: pd.Series,
               atr_period: int = 10, multiplier: float = 3.0) -> pd.DataFrame:
    """
    SuperTrend indicator computed manually from ATR.
    Returns DataFrame with columns 'supertrend' (value) and 'direction' (1=up, -1=down).
    """
    from ta.volatility import AverageTrueRange
    atr = AverageTrueRange(high=high, low=low, close=close, window=atr_period).average_true_range()

    hl2 = (high + low) / 2
    upper_band = hl2 + multiplier * atr
    lower_band = hl2 - multiplier * atr

    st_direction = pd.Series(np.ones(len(close)), index=close.index)  # 1 = bullish
    st_value = pd.Series(np.nan, index=close.index)

    final_upper = upper_band.copy()
    final_lower = lower_band.copy()

    for i in range(1, len(close)):
        # Lower band logic
        if lower_band.iloc[i] > final_lower.iloc[i - 1] or close.iloc[i - 1] < final_lower.iloc[i - 1]:
            final_lower.iloc[i] = lower_band.iloc[i]
        else:
            final_lower.iloc[i] = final_lower.iloc[i - 1]

        # Upper band logic
        if upper_band.iloc[i] < final_upper.iloc[i - 1] or close.iloc[i - 1] > final_upper.iloc[i - 1]:
            final_upper.iloc[i] = upper_band.iloc[i]
        else:
            final_upper.iloc[i] = final_upper.iloc[i - 1]

        # Direction
        if st_direction.iloc[i - 1] == 1:
            if close.iloc[i] < final_lower.iloc[i]:
                st_direction.iloc[i] = -1
            else:
                st_direction.iloc[i] = 1
        else:
            if close.iloc[i] > final_upper.iloc[i]:
                st_direction.iloc[i] = 1
            else:
                st_direction.iloc[i] = -1

    # Value: lower band when bullish, upper band when bearish
    for i in range(len(close)):
        st_value.iloc[i] = final_lower.iloc[i] if st_direction.iloc[i] == 1 else final_upper.iloc[i]

    return pd.DataFrame({"supertrend": st_value, "direction": st_direction}, index=close.index)


def _safe_last(series: Optional[pd.Series]) -> Optional[float]:
    """Extract the last non-NaN value from a Series, or None."""
    if series is None or series.empty:
        return None
    val = series.iloc[-1]
    if pd.isna(val):
        return None
    return float(val)


class IndicatorCalculator:
    """Computes technical indicators on OHLCV DataFrames."""

    def compute(self, df: pd.DataFrame, symbol: str, timeframe: str) -> Optional[Dict]:
        """
        Compute all indicators for a symbol-timeframe pair.
        Returns flat dict with all indicator values, or None if insufficient data.
        """
        if df is None or len(df) < MIN_BARS:
            return None

        try:
            from ta.trend import EMAIndicator, MACD, ADXIndicator
            from ta.momentum import RSIIndicator, StochRSIIndicator
            from ta.volatility import AverageTrueRange, BollingerBands, KeltnerChannel
        except ImportError:
            logger.error("ta library not installed")
            return None

        try:
            close = df["close"].astype(float)
            high = df["high"].astype(float)
            low = df["low"].astype(float)
            volume = df["volume"].astype(float)
            latest = close.iloc[-1]

            result: Dict = {"price": latest}

            # ── EMAs ──
            ema21_series = EMAIndicator(close=close, window=21).ema_indicator()
            ema50_series = EMAIndicator(close=close, window=50).ema_indicator()
            ema200_series = EMAIndicator(close=close, window=200).ema_indicator() if len(close) >= 200 else None

            result["ema21"] = _safe_last(ema21_series)
            result["ema50"] = _safe_last(ema50_series)
            result["ema200"] = _safe_last(ema200_series)

            # EMA alignment score
            result["ema_aligned_bull"] = (
                result["ema21"] is not None and result["ema50"] is not None
                and latest > result["ema21"] > result["ema50"]
            )
            result["ema_aligned_bear"] = (
                result["ema21"] is not None and result["ema50"] is not None
                and latest < result["ema21"] < result["ema50"]
            )
            # Full stack alignment includes EMA200
            result["ema_full_bull"] = (
                result["ema_aligned_bull"]
                and result["ema200"] is not None
                and result["ema50"] > result["ema200"]
            )
            result["ema_full_bear"] = (
                result["ema_aligned_bear"]
                and result["ema200"] is not None
                and result["ema50"] < result["ema200"]
            )

            # EMA slope (positive = rising)
            if ema50_series is not None and len(ema50_series) >= 5:
                v1 = ema50_series.iloc[-1]
                v5 = ema50_series.iloc[-5]
                result["ema50_slope"] = float(v1 - v5) if not (pd.isna(v1) or pd.isna(v5)) else 0.0
            else:
                result["ema50_slope"] = 0.0

            # ── SuperTrend (manual from ATR) ──
            st = supertrend(high, low, close, atr_period=10, multiplier=3.0)
            result["supertrend_direction"] = int(st["direction"].iloc[-1])
            result["supertrend_value"] = float(st["supertrend"].iloc[-1])
            if len(st) >= 2:
                result["supertrend_flip"] = bool(
                    st["direction"].iloc[-2] != st["direction"].iloc[-1]
                )
            else:
                result["supertrend_flip"] = False

            # ── RSI ──
            rsi_series = RSIIndicator(close=close, window=14).rsi()
            rsi_val = _safe_last(rsi_series)
            result["rsi"] = rsi_val if rsi_val is not None else 50.0
            result["rsi_ob"] = result["rsi"] > 70
            result["rsi_os"] = result["rsi"] < 30

            # ── Stochastic RSI ──
            stoch_rsi = StochRSIIndicator(close=close, window=14, smooth1=3, smooth2=3)
            stoch_k = _safe_last(stoch_rsi.stochrsi_k())
            stoch_d = _safe_last(stoch_rsi.stochrsi_d())
            result["stoch_rsi_k"] = stoch_k if stoch_k is not None else 50.0
            result["stoch_rsi_d"] = stoch_d if stoch_d is not None else 50.0

            # ── ADX ──
            adx_ind = ADXIndicator(high=high, low=low, close=close, window=14)
            adx_val = _safe_last(adx_ind.adx())
            result["adx"] = adx_val if adx_val is not None else 20.0
            result["adx_trending"] = result["adx"] > 25

            # ── MACD ──
            macd_ind = MACD(close=close, window_slow=26, window_fast=12, window_sign=9)
            macd_hist_series = macd_ind.macd_diff()
            macd_line_series = macd_ind.macd()
            macd_signal_series = macd_ind.macd_signal()

            macd_hist_val = _safe_last(macd_hist_series)
            result["macd_hist"] = macd_hist_val if macd_hist_val is not None else 0.0
            result["macd_line"] = _safe_last(macd_line_series) or 0.0
            result["macd_signal"] = _safe_last(macd_signal_series) or 0.0

            # Histogram growing in direction
            if macd_hist_series is not None and len(macd_hist_series) >= 2:
                prev_hist = macd_hist_series.iloc[-2]
                curr_hist = macd_hist_series.iloc[-1]
                if not (pd.isna(prev_hist) or pd.isna(curr_hist)):
                    result["macd_hist_growing_bull"] = bool(curr_hist > prev_hist and curr_hist > 0)
                    result["macd_hist_growing_bear"] = bool(curr_hist < prev_hist and curr_hist < 0)
                else:
                    result["macd_hist_growing_bull"] = False
                    result["macd_hist_growing_bear"] = False
            else:
                result["macd_hist_growing_bull"] = False
                result["macd_hist_growing_bear"] = False

            # ── WaveTrend (custom) ──
            wt1, wt2 = wavetrend(high, low, close)
            result["wavetrend_1"] = float(wt1.iloc[-1]) if not wt1.empty else 0.0
            result["wavetrend_2"] = float(wt2.iloc[-1]) if not wt2.empty else 0.0
            result["wavetrend_ob"] = result["wavetrend_1"] > 60
            result["wavetrend_os"] = result["wavetrend_1"] < -60
            # Cross
            if len(wt1) >= 2 and len(wt2) >= 2:
                result["wavetrend_bull_cross"] = bool(
                    wt1.iloc[-2] < wt2.iloc[-2] and wt1.iloc[-1] > wt2.iloc[-1]
                )
                result["wavetrend_bear_cross"] = bool(
                    wt1.iloc[-2] > wt2.iloc[-2] and wt1.iloc[-1] < wt2.iloc[-1]
                )
            else:
                result["wavetrend_bull_cross"] = False
                result["wavetrend_bear_cross"] = False

            # ── ATR ──
            atr_series = AverageTrueRange(high=high, low=low, close=close, window=14).average_true_range()
            atr_val = _safe_last(atr_series)
            result["atr"] = atr_val if atr_val is not None else 0.0
            result["atr_pct"] = (result["atr"] / latest * 100) if latest > 0 else 0.0

            # ── Bollinger Bands ──
            bb = BollingerBands(close=close, window=20, window_dev=2)
            result["bb_upper"] = _safe_last(bb.bollinger_hband())
            result["bb_lower"] = _safe_last(bb.bollinger_lband())
            result["bb_mid"] = _safe_last(bb.bollinger_mavg())
            result["bb_width"] = _safe_last(bb.bollinger_wband())

            # ── Keltner Channels (for squeeze detection) ──
            kc = KeltnerChannel(high=high, low=low, close=close, window=20, window_atr=20)
            result["kc_upper"] = _safe_last(kc.keltner_channel_hband())
            result["kc_lower"] = _safe_last(kc.keltner_channel_lband())

            # Squeeze detection: BB inside KC
            if (result["bb_upper"] and result["kc_upper"]
                    and result["bb_lower"] and result["kc_lower"]):
                result["squeeze"] = bool(
                    result["bb_upper"] < result["kc_upper"]
                    and result["bb_lower"] > result["kc_lower"]
                )
            else:
                result["squeeze"] = False

            # ── RVOL (Relative Volume) ──
            if len(volume) >= 20 and volume.iloc[-1] > 0:
                avg_vol = volume.iloc[-21:-1].mean()
                result["rvol"] = float(volume.iloc[-1] / avg_vol) if avg_vol > 0 else 1.0
            else:
                result["rvol"] = 1.0

            return result

        except Exception as e:
            logger.debug(f"Indicator computation failed for {symbol} {timeframe}: {e}")
            return None
