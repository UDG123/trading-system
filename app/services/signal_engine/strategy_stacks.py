"""
Strategy Stacks — 5 indicator-based signal generation strategies.
Each stack is a pure function: takes indicators dict, returns signal candidate or None.

Stack A: EMA(9/21) crossover + RSI(14) + ADX(14) trend filter
Stack B: SuperTrend + MACD(12,26,9) + EMA(200) trend continuation
Stack C: BB(20,2) squeeze breakout + RSI confirmation
Stack D: BB(20,2) + RSI(14) + Williams %R(14) mean reversion (ADX<20)
Stack E: Ichimoku(9,26,52) + RSI(14) for FX pairs
"""
import logging
from typing import Dict, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger("TradingSystem.SignalEngine.Stacks")


# ═══════════════════════════════════════════════════════════════
# ADX + ATR Regime Detector (lightweight, per-scan)
# ═══════════════════════════════════════════════════════════════

def detect_regime_adx_atr(indicators: Dict) -> str:
    """
    Classify market regime from ADX + ATR.
      TRENDING:     ADX > 25
      RANGING:      ADX < 20
      VOLATILE:     ATR > 2× 50-bar average (via atr_ratio > 2.0)
      TRANSITIONAL: everything else (ADX 20-25)
    """
    adx = indicators.get("adx", 20)
    atr_ratio = indicators.get("atr_ratio", 1.0)
    if atr_ratio is None:
        atr_ratio = 1.0

    if atr_ratio > 2.0:
        return "VOLATILE"
    if adx > 25:
        return "TRENDING"
    if adx < 20:
        return "RANGING"
    return "TRANSITIONAL"


def select_stacks_for_regime(regime: str, symbol: str = "") -> list:
    """Map regime → applicable strategy stacks."""
    from app.services.ohlcv_ingester import CRYPTO_SYMBOLS

    if regime == "TRENDING":
        stacks = ["A", "B"]
        # Stack E (Ichimoku) only for FX pairs (not crypto/equity/commodity)
        if symbol not in CRYPTO_SYMBOLS and not symbol.startswith(("NAS", "US3", "WTI")):
            stacks.append("E")
        return stacks
    elif regime == "RANGING":
        return ["D"]
    elif regime == "TRANSITIONAL":
        return ["C"]
    elif regime == "VOLATILE":
        return ["A", "B"]  # Trend-following in vol — ride the momentum
    return ["A", "C"]  # Fallback


# ═══════════════════════════════════════════════════════════════
# Stack A: EMA Crossover + RSI + ADX Trend Filter
# ═══════════════════════════════════════════════════════════════

def stack_a_ema_crossover(df: pd.DataFrame, indicators: Dict) -> Optional[Dict]:
    """
    EMA(9/21) crossover with RSI confirmation and ADX trend filter.
    ADX > 25 required. RSI must confirm direction (not counter-extreme).
    """
    if df is None or len(df) < 30:
        return None

    adx = indicators.get("adx", 0)
    if adx < 25:
        return None

    close = df["close"].astype(float)
    ema9 = close.ewm(span=9, adjust=False).mean()
    ema21 = close.ewm(span=21, adjust=False).mean()

    # Crossover detection on last 2 bars
    if len(ema9) < 2:
        return None

    prev_diff = float(ema9.iloc[-2] - ema21.iloc[-2])
    curr_diff = float(ema9.iloc[-1] - ema21.iloc[-1])

    rsi = indicators.get("rsi", 50)
    direction = None

    # Bullish crossover: EMA9 crosses above EMA21
    if prev_diff <= 0 and curr_diff > 0 and rsi < 70:
        direction = "LONG"
    # Bearish crossover: EMA9 crosses below EMA21
    elif prev_diff >= 0 and curr_diff < 0 and rsi > 30:
        direction = "SHORT"

    if not direction:
        return None

    return {
        "direction": direction,
        "strategy": "stack_a_ema_crossover",
        "alert_type": f"{'bullish' if direction == 'LONG' else 'bearish'}_confirmation",
        "confidence": min(1.0, adx / 40),  # ADX-based confidence
    }


# ═══════════════════════════════════════════════════════════════
# Stack B: SuperTrend + MACD + EMA(200) Trend Continuation
# ═══════════════════════════════════════════════════════════════

def stack_b_supertrend_macd(df: pd.DataFrame, indicators: Dict) -> Optional[Dict]:
    """
    SuperTrend direction + MACD histogram growing + price above/below EMA200.
    Triple confirmation trend continuation.
    """
    if df is None or len(df) < 200:
        return None

    st_dir = indicators.get("supertrend_direction", 0)
    st_flip = indicators.get("supertrend_flip", False)
    macd_bull = indicators.get("macd_hist_growing_bull", False)
    macd_bear = indicators.get("macd_hist_growing_bear", False)
    ema200 = indicators.get("ema200")
    price = indicators.get("price", 0)

    if not ema200 or price <= 0:
        return None

    direction = None

    # LONG: SuperTrend bullish + MACD growing bull + price > EMA200
    if st_dir == 1 and macd_bull and price > ema200:
        direction = "LONG"
    # SHORT: SuperTrend bearish + MACD growing bear + price < EMA200
    elif st_dir == -1 and macd_bear and price < ema200:
        direction = "SHORT"

    # Bonus: SuperTrend just flipped → stronger signal
    if not direction:
        return None

    alert_type = "bullish_plus" if direction == "LONG" else "bearish_plus"
    if st_flip:
        alert_type = f"{'bullish' if direction == 'LONG' else 'bearish'}_confirmation_plus"

    return {
        "direction": direction,
        "strategy": "stack_b_supertrend_macd",
        "alert_type": alert_type,
        "confidence": 0.8 if st_flip else 0.6,
    }


# ═══════════════════════════════════════════════════════════════
# Stack C: BB Squeeze Breakout + RSI Confirmation
# ═══════════════════════════════════════════════════════════════

def stack_c_squeeze_breakout(df: pd.DataFrame, indicators: Dict) -> Optional[Dict]:
    """
    BB squeeze detection (BB inside KC) followed by breakout.
    RSI confirms direction of the breakout.
    Designed for TRANSITIONAL regime (ADX 20-25).
    """
    if df is None or len(df) < 30:
        return None

    squeeze = indicators.get("squeeze", False)
    bb_upper = indicators.get("bb_upper")
    bb_lower = indicators.get("bb_lower")
    price = indicators.get("price", 0)
    rsi = indicators.get("rsi", 50)

    if not bb_upper or not bb_lower or price <= 0:
        return None

    # Look for squeeze release: was in squeeze recently, now breaking out
    # Check previous bars for squeeze state
    close = df["close"].astype(float)
    high = df["high"].astype(float)

    # Current bar breaking above/below BB
    direction = None

    if price > bb_upper and rsi > 55:
        direction = "LONG"
    elif price < bb_lower and rsi < 45:
        direction = "SHORT"

    # Extra confirmation: squeeze should be active or just released
    if not direction:
        return None
    if not squeeze and indicators.get("bb_width", 0) and indicators.get("bb_width") > 0.05:
        # Wide bands + no squeeze = not a breakout, just noise
        return None

    return {
        "direction": direction,
        "strategy": "stack_c_squeeze_breakout",
        "alert_type": f"{'bullish' if direction == 'LONG' else 'bearish'}_confirmation",
        "confidence": 0.55,
    }


# ═══════════════════════════════════════════════════════════════
# Stack D: BB + RSI + Williams %R Mean Reversion (ADX<20)
# ═══════════════════════════════════════════════════════════════

def stack_d_mean_reversion(df: pd.DataFrame, indicators: Dict) -> Optional[Dict]:
    """
    Mean reversion at BB extremes with RSI + Williams %R triple confirmation.
    Only active when ADX < 20 (ranging market).
    """
    if df is None or len(df) < 30:
        return None

    adx = indicators.get("adx", 25)
    if adx >= 20:
        return None  # Only for ranging markets

    price = indicators.get("price", 0)
    bb_upper = indicators.get("bb_upper")
    bb_lower = indicators.get("bb_lower")
    bb_mid = indicators.get("bb_mid")
    rsi = indicators.get("rsi", 50)

    if not all([price, bb_upper, bb_lower, bb_mid]):
        return None

    # Compute Williams %R manually: %R = (Highest High - Close) / (Highest High - Lowest Low) * -100
    close = df["close"].astype(float)
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    period = 14

    if len(close) < period:
        return None

    hh = float(high.iloc[-period:].max())
    ll = float(low.iloc[-period:].min())
    williams_r = ((hh - float(close.iloc[-1])) / (hh - ll) * -100) if (hh - ll) > 0 else -50

    direction = None

    # LONG: price at lower BB + RSI oversold + Williams %R oversold (<-80)
    if price <= bb_lower * 1.003 and rsi < 35 and williams_r < -80:
        direction = "LONG"
    # SHORT: price at upper BB + RSI overbought + Williams %R overbought (>-20)
    elif price >= bb_upper * 0.997 and rsi > 65 and williams_r > -20:
        direction = "SHORT"

    if not direction:
        return None

    return {
        "direction": direction,
        "strategy": "stack_d_mean_reversion",
        "alert_type": f"contrarian_{'bullish' if direction == 'LONG' else 'bearish'}",
        "confidence": 0.50,
        "tp1": round(bb_mid, 5),
        "sl": round((bb_lower * 0.997) if direction == "LONG" else (bb_upper * 1.003), 5),
    }


# ═══════════════════════════════════════════════════════════════
# Stack E: Ichimoku + RSI for FX Pairs
# ═══════════════════════════════════════════════════════════════

def stack_e_ichimoku(df: pd.DataFrame, indicators: Dict) -> Optional[Dict]:
    """
    Ichimoku Cloud alignment + RSI confirmation.
    Price above cloud + RSI > 50 → LONG. Price below cloud + RSI < 50 → SHORT.
    Best for FX pairs on 1H+ timeframes.
    """
    if df is None or len(df) < 60:
        return None

    above_cloud = indicators.get("price_above_cloud")
    below_cloud = indicators.get("price_below_cloud")
    in_cloud = indicators.get("price_in_cloud")
    rsi = indicators.get("rsi", 50)
    adx = indicators.get("adx", 0)

    # Require clear cloud position (not inside cloud)
    if in_cloud or (above_cloud is None):
        return None

    # ADX > 20 minimum for Ichimoku trend trades
    if adx < 20:
        return None

    direction = None

    if above_cloud and rsi > 50:
        direction = "LONG"
    elif below_cloud and rsi < 50:
        direction = "SHORT"

    if not direction:
        return None

    # Extra: check Ichimoku span A vs span B for cloud color
    span_a = indicators.get("ichimoku_span_a")
    span_b = indicators.get("ichimoku_span_b")
    cloud_bullish = span_a and span_b and span_a > span_b

    # Cloud color should match direction
    if direction == "LONG" and not cloud_bullish:
        return None
    if direction == "SHORT" and cloud_bullish:
        return None

    return {
        "direction": direction,
        "strategy": "stack_e_ichimoku",
        "alert_type": f"{'bullish' if direction == 'LONG' else 'bearish'}_confirmation",
        "confidence": 0.65,
    }


# ═══════════════════════════════════════════════════════════════
# Stack Runner — evaluates applicable stacks for a symbol
# ═══════════════════════════════════════════════════════════════

STACK_FUNCTIONS = {
    "A": stack_a_ema_crossover,
    "B": stack_b_supertrend_macd,
    "C": stack_c_squeeze_breakout,
    "D": stack_d_mean_reversion,
    "E": stack_e_ichimoku,
}


def run_stacks(
    df: pd.DataFrame,
    indicators: Dict,
    symbol: str,
    regime: str = None,
) -> list:
    """
    Run applicable strategy stacks based on regime.
    Returns list of signal candidate dicts (may be empty).
    """
    if not indicators:
        return []

    if not regime:
        regime = detect_regime_adx_atr(indicators)

    applicable = select_stacks_for_regime(regime, symbol)
    candidates = []

    for stack_id in applicable:
        func = STACK_FUNCTIONS.get(stack_id)
        if not func:
            continue

        try:
            result = func(df, indicators)
            if result:
                result["regime"] = regime
                result["stack_id"] = stack_id
                candidates.append(result)
        except Exception as e:
            logger.debug(f"Stack {stack_id} error for {symbol}: {e}")

    return candidates
