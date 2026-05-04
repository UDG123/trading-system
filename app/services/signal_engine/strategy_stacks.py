"""
Strategy Stacks — indicator-based signal generation strategies.

This module now separates FX desks by role, not just timeframe:
- DESK1_SCALPER: micro momentum / breakout / range reversion only.
- DESK2_INTRADAY: session trend continuation, pullbacks, squeeze breakouts.
- DESK3_SWING: higher-timeframe structural trend continuation.

The stack functions remain pure and backwards compatible. Existing callers can
still call run_stacks(df, indicators, symbol, regime). Desk-aware callers should
pass desk_id so the same FX pair is treated differently by each desk.
"""
import logging
from typing import Dict, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger("TradingSystem.SignalEngine.Stacks")


FX_DESK_ROLES = {
    "DESK1_SCALPER": "FX_SCALP",
    "DESK2_INTRADAY": "FX_INTRADAY",
    "DESK3_SWING": "FX_SWING",
}


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


def select_stacks_for_regime(regime: str, symbol: str = "", desk_id: str = "") -> list:
    """Map desk + regime → applicable strategy stacks.

    This is the key FX desk separation:
    - Desk 1 scalps fast bursts and range fades only.
    - Desk 2 takes intraday trend/transition continuation.
    - Desk 3 avoids noisy scalps and focuses HTF trend/cloud confirmation.
    """
    from app.services.ohlcv_ingester import CRYPTO_SYMBOLS

    # FX desk role separation. Same symbols, different engines.
    if desk_id == "DESK1_SCALPER":
        if regime == "RANGING":
            return ["D"]          # range fade only
        if regime in {"TRENDING", "VOLATILE"}:
            return ["A", "C"]    # fast burst + squeeze breakout
        return ["C"]              # transitional breakout only

    if desk_id == "DESK2_INTRADAY":
        if regime == "RANGING":
            return ["D"]          # reduced-size reversion via pipeline sizing
        if regime == "TRANSITIONAL":
            return ["C"]          # squeeze expansion
        return ["A", "B"]        # trend continuation / pullback proxy

    if desk_id == "DESK3_SWING":
        if regime == "RANGING":
            return []             # swing desk abstains in range
        if symbol not in CRYPTO_SYMBOLS and not symbol.startswith(("NAS", "US3", "WTI")):
            return ["B", "E"]    # HTF trend + Ichimoku
        return ["B"]

    # Default cross-asset legacy behavior.
    if regime == "TRENDING":
        stacks = ["A", "B"]
        if symbol not in CRYPTO_SYMBOLS and not symbol.startswith(("NAS", "US3", "WTI")):
            stacks.append("E")
        return stacks
    elif regime == "RANGING":
        return ["D"]
    elif regime == "TRANSITIONAL":
        return ["C"]
    elif regime == "VOLATILE":
        return ["A", "B"]
    return ["A", "C"]


# ═══════════════════════════════════════════════════════════════
# Stack A: EMA Crossover + RSI + ADX Trend Filter
# ═══════════════════════════════════════════════════════════════

def stack_a_ema_crossover(df: pd.DataFrame, indicators: Dict) -> Optional[Dict]:
    if df is None or len(df) < 30:
        return None

    adx = indicators.get("adx", 0)
    if adx < 25:
        return None

    close = df["close"].astype(float)
    ema9 = close.ewm(span=9, adjust=False).mean()
    ema21 = close.ewm(span=21, adjust=False).mean()

    if len(ema9) < 2:
        return None

    prev_diff = float(ema9.iloc[-2] - ema21.iloc[-2])
    curr_diff = float(ema9.iloc[-1] - ema21.iloc[-1])

    rsi = indicators.get("rsi", 50)
    direction = None

    if prev_diff <= 0 and curr_diff > 0 and rsi < 70:
        direction = "LONG"
    elif prev_diff >= 0 and curr_diff < 0 and rsi > 30:
        direction = "SHORT"

    if not direction:
        return None

    return {
        "direction": direction,
        "strategy": "stack_a_ema_crossover",
        "alert_type": f"{'bullish' if direction == 'LONG' else 'bearish'}_confirmation",
        "confidence": min(1.0, adx / 40),
    }


# ═══════════════════════════════════════════════════════════════
# Stack B: SuperTrend + MACD + EMA(200) Trend Continuation
# ═══════════════════════════════════════════════════════════════

def stack_b_supertrend_macd(df: pd.DataFrame, indicators: Dict) -> Optional[Dict]:
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
    if st_dir == 1 and macd_bull and price > ema200:
        direction = "LONG"
    elif st_dir == -1 and macd_bear and price < ema200:
        direction = "SHORT"

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
    if df is None or len(df) < 30:
        return None

    squeeze = indicators.get("squeeze", False)
    bb_upper = indicators.get("bb_upper")
    bb_lower = indicators.get("bb_lower")
    price = indicators.get("price", 0)
    rsi = indicators.get("rsi", 50)

    if not bb_upper or not bb_lower or price <= 0:
        return None

    direction = None
    if price > bb_upper and rsi > 55:
        direction = "LONG"
    elif price < bb_lower and rsi < 45:
        direction = "SHORT"

    if not direction:
        return None
    if not squeeze and indicators.get("bb_width", 0) and indicators.get("bb_width") > 0.05:
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
    if df is None or len(df) < 30:
        return None

    adx = indicators.get("adx", 25)
    if adx >= 20:
        return None

    price = indicators.get("price", 0)
    bb_upper = indicators.get("bb_upper")
    bb_lower = indicators.get("bb_lower")
    bb_mid = indicators.get("bb_mid")
    rsi = indicators.get("rsi", 50)

    if not all([price, bb_upper, bb_lower, bb_mid]):
        return None

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
    if price <= bb_lower * 1.003 and rsi < 35 and williams_r < -80:
        direction = "LONG"
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
    if df is None or len(df) < 60:
        return None

    above_cloud = indicators.get("price_above_cloud")
    below_cloud = indicators.get("price_below_cloud")
    in_cloud = indicators.get("price_in_cloud")
    rsi = indicators.get("rsi", 50)
    adx = indicators.get("adx", 0)

    if in_cloud or (above_cloud is None):
        return None
    if adx < 20:
        return None

    direction = None
    if above_cloud and rsi > 50:
        direction = "LONG"
    elif below_cloud and rsi < 50:
        direction = "SHORT"

    if not direction:
        return None

    span_a = indicators.get("ichimoku_span_a")
    span_b = indicators.get("ichimoku_span_b")
    cloud_bullish = span_a and span_b and span_a > span_b

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
    desk_id: str = "",
    mode: str = "",
) -> list:
    """Run desk-aware strategy stacks.

    Backwards compatible with old callers, but when desk_id is supplied the
    same FX pair becomes a different engine on DESK1/2/3 rather than merely a
    different chart zoom.
    """
    if not indicators:
        return []

    if not regime:
        regime = detect_regime_adx_atr(indicators)

    applicable = select_stacks_for_regime(regime, symbol, desk_id=desk_id)
    desk_role = FX_DESK_ROLES.get(desk_id, desk_id or "GENERIC")
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
                result["desk_role"] = desk_role
                result["desk_mode"] = desk_role
                result["strategy_mode"] = result.get("strategy")
                result["mode_reason"] = f"{desk_role} selected stack {stack_id} in {regime} regime"
                result["quality_hints"] = [
                    f"desk_role:{desk_role}",
                    f"regime:{regime}",
                    f"stack:{stack_id}",
                ]
                candidates.append(result)
        except Exception as e:
            logger.debug(f"Stack {stack_id} error for {symbol}: {e}")

    return candidates
