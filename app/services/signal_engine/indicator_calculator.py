"""
Technical Indicator Calculator
Computes all indicators using pandas-ta + custom implementations.
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
            import pandas_ta as ta
        except ImportError:
            logger.error("pandas-ta not installed")
            return None

        try:
            close = df["close"].astype(float)
            high = df["high"].astype(float)
            low = df["low"].astype(float)
            volume = df["volume"].astype(float)
            latest = close.iloc[-1]

            result: Dict = {"price": latest}

            # ── EMAs ──
            ema21 = ta.ema(close, length=21)
            ema50 = ta.ema(close, length=50)
            ema200 = ta.ema(close, length=200) if len(close) >= 200 else None

            result["ema21"] = float(ema21.iloc[-1]) if ema21 is not None and not ema21.empty else None
            result["ema50"] = float(ema50.iloc[-1]) if ema50 is not None and not ema50.empty else None
            result["ema200"] = float(ema200.iloc[-1]) if ema200 is not None and not ema200.empty else None

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
            if ema50 is not None and len(ema50) >= 5:
                result["ema50_slope"] = float(ema50.iloc[-1] - ema50.iloc[-5])
            else:
                result["ema50_slope"] = 0.0

            # ── SuperTrend ──
            st = ta.supertrend(high, low, close, length=10, multiplier=3.0)
            if st is not None and not st.empty:
                st_dir_col = [c for c in st.columns if "SUPERTd" in c]
                st_val_col = [c for c in st.columns if "SUPERT_" in c and "SUPERTd" not in c]
                result["supertrend_direction"] = int(st[st_dir_col[0]].iloc[-1]) if st_dir_col else 0
                result["supertrend_value"] = float(st[st_val_col[0]].iloc[-1]) if st_val_col else None
                # Detect SuperTrend flip (direction change on last bar)
                if st_dir_col and len(st) >= 2:
                    prev_dir = st[st_dir_col[0]].iloc[-2]
                    curr_dir = st[st_dir_col[0]].iloc[-1]
                    result["supertrend_flip"] = bool(prev_dir != curr_dir)
                else:
                    result["supertrend_flip"] = False
            else:
                result["supertrend_direction"] = 0
                result["supertrend_value"] = None
                result["supertrend_flip"] = False

            # ── RSI ──
            rsi = ta.rsi(close, length=14)
            result["rsi"] = float(rsi.iloc[-1]) if rsi is not None and not rsi.empty else 50.0
            result["rsi_ob"] = result["rsi"] > 70  # overbought
            result["rsi_os"] = result["rsi"] < 30  # oversold

            # ── Stochastic RSI ──
            stoch_rsi = ta.stochrsi(close, length=14, rsi_length=14, k=3, d=3)
            if stoch_rsi is not None and not stoch_rsi.empty:
                k_col = [c for c in stoch_rsi.columns if "STOCHRSIk" in c]
                d_col = [c for c in stoch_rsi.columns if "STOCHRSId" in c]
                result["stoch_rsi_k"] = float(stoch_rsi[k_col[0]].iloc[-1]) if k_col else 50.0
                result["stoch_rsi_d"] = float(stoch_rsi[d_col[0]].iloc[-1]) if d_col else 50.0
            else:
                result["stoch_rsi_k"] = 50.0
                result["stoch_rsi_d"] = 50.0

            # ── ADX ──
            adx = ta.adx(high, low, close, length=14)
            if adx is not None and not adx.empty:
                adx_col = [c for c in adx.columns if c.startswith("ADX_")]
                result["adx"] = float(adx[adx_col[0]].iloc[-1]) if adx_col else 20.0
            else:
                result["adx"] = 20.0
            result["adx_trending"] = result["adx"] > 25

            # ── MACD ──
            macd = ta.macd(close, fast=12, slow=26, signal=9)
            if macd is not None and not macd.empty:
                hist_col = [c for c in macd.columns if "MACDh" in c]
                macd_col = [c for c in macd.columns if c.startswith("MACD_")]
                signal_col = [c for c in macd.columns if "MACDs" in c]
                result["macd_hist"] = float(macd[hist_col[0]].iloc[-1]) if hist_col else 0.0
                result["macd_line"] = float(macd[macd_col[0]].iloc[-1]) if macd_col else 0.0
                result["macd_signal"] = float(macd[signal_col[0]].iloc[-1]) if signal_col else 0.0
                # Histogram growing in direction
                if hist_col and len(macd) >= 2:
                    prev_hist = macd[hist_col[0]].iloc[-2]
                    curr_hist = macd[hist_col[0]].iloc[-1]
                    result["macd_hist_growing_bull"] = bool(curr_hist > prev_hist and curr_hist > 0)
                    result["macd_hist_growing_bear"] = bool(curr_hist < prev_hist and curr_hist < 0)
                else:
                    result["macd_hist_growing_bull"] = False
                    result["macd_hist_growing_bear"] = False
            else:
                result["macd_hist"] = 0.0
                result["macd_line"] = 0.0
                result["macd_signal"] = 0.0
                result["macd_hist_growing_bull"] = False
                result["macd_hist_growing_bear"] = False

            # ── WaveTrend ──
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
            atr = ta.atr(high, low, close, length=14)
            result["atr"] = float(atr.iloc[-1]) if atr is not None and not atr.empty else 0.0
            result["atr_pct"] = (result["atr"] / latest * 100) if latest > 0 else 0.0

            # ── Bollinger Bands ──
            bb = ta.bbands(close, length=20, std=2.0)
            if bb is not None and not bb.empty:
                bbu = [c for c in bb.columns if "BBU" in c]
                bbl = [c for c in bb.columns if "BBL" in c]
                bbm = [c for c in bb.columns if "BBM" in c]
                result["bb_upper"] = float(bb[bbu[0]].iloc[-1]) if bbu else None
                result["bb_lower"] = float(bb[bbl[0]].iloc[-1]) if bbl else None
                result["bb_mid"] = float(bb[bbm[0]].iloc[-1]) if bbm else None
                bb_width = [c for c in bb.columns if "BBB" in c]
                result["bb_width"] = float(bb[bb_width[0]].iloc[-1]) if bb_width else None
            else:
                result["bb_upper"] = None
                result["bb_lower"] = None
                result["bb_mid"] = None
                result["bb_width"] = None

            # ── Keltner Channels (for squeeze detection) ──
            kc = ta.kc(high, low, close, length=20, scalar=1.5)
            if kc is not None and not kc.empty:
                kcu = [c for c in kc.columns if "KCU" in c]
                kcl = [c for c in kc.columns if "KCL" in c]
                result["kc_upper"] = float(kc[kcu[0]].iloc[-1]) if kcu else None
                result["kc_lower"] = float(kc[kcl[0]].iloc[-1]) if kcl else None
            else:
                result["kc_upper"] = None
                result["kc_lower"] = None

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
