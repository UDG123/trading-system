"""DESK4_GOLD mode router and precision scoring.

This module is intentionally dependency-light. It must never crash scanner startup:
if XAUUSD data is incomplete, it logs and returns no candidates unless debug mode
allows a near-threshold diagnostic candidate.
"""
from __future__ import annotations

import logging
import os
from typing import Dict, List, Optional

import pandas as pd

from app.config import SIGNAL_DEBUG_MODE, MIN_TIMEFRAME_BARS

logger = logging.getLogger("TradingSystem.SignalEngine.GoldModes")

GOLD_SYMBOL = "XAUUSD"


_MODE_SPECS = {
    "GOLD_SCALP": {
        "entry_tfs": ["1M", "5M"],
        "confirm_tf": "15M",
        "bias_tf": "1H",
        "strategy_mode": "fast_momentum_vwap_ema_reclaim",
        "rr": 1.6,
        "atr_mult": 1.2,
        "min_bars": 35,
    },
    "GOLD_INTRADAY": {
        "entry_tfs": ["15M"],
        "confirm_tf": "1H",
        "bias_tf": "4H",
        "strategy_mode": "supertrend_macd_ema200_continuation",
        "rr": 2.0,
        "atr_mult": 1.6,
        "min_bars": 50,
    },
    "GOLD_SWING": {
        "entry_tfs": ["4H"],
        "confirm_tf": "D",
        "bias_tf": "W",
        "strategy_mode": "ema50_ema200_donchian_chandelier",
        "rr": 2.5,
        "atr_mult": 2.4,
        "min_bars": 60,
    },
}


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _last_float(df: Optional[pd.DataFrame], col: str, default: float = 0.0) -> float:
    try:
        if df is None or df.empty or col not in df.columns:
            return default
        return float(df[col].iloc[-1])
    except Exception:
        return default


def _ema(series: pd.Series, span: int) -> float:
    try:
        if series is None or len(series) < max(5, span // 2):
            return float(series.iloc[-1]) if series is not None and len(series) else 0.0
        return float(series.ewm(span=span, adjust=False).mean().iloc[-1])
    except Exception:
        return 0.0


def _atr_like(df: Optional[pd.DataFrame], window: int = 14) -> float:
    try:
        if df is None or len(df) < 3:
            return 0.0
        high = pd.to_numeric(df["high"], errors="coerce")
        low = pd.to_numeric(df["low"], errors="coerce")
        close = pd.to_numeric(df["close"], errors="coerce")
        prev_close = close.shift(1)
        tr = pd.concat([(high - low).abs(), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
        val = tr.rolling(window=min(window, len(tr))).mean().iloc[-1]
        return float(val) if pd.notna(val) and val > 0 else float((high - low).tail(window).mean())
    except Exception:
        return 0.0


def _direction(entry: pd.DataFrame, confirm: Optional[pd.DataFrame], bias: Optional[pd.DataFrame]) -> tuple[str, List[str], float]:
    close = pd.to_numeric(entry["close"], errors="coerce")
    price = float(close.iloc[-1])
    ema20 = _ema(close, 20)
    ema50 = _ema(close, 50)
    ema200 = _ema(close, 200)

    hints: List[str] = []
    score = 55.0

    if price > ema20 > ema50:
        direction = "BUY"
        hints.append("entry_trend_up")
        score += 8
    elif price < ema20 < ema50:
        direction = "SELL"
        hints.append("entry_trend_down")
        score += 8
    elif price >= ema20:
        direction = "BUY"
        hints.append("weak_entry_above_ema20")
    else:
        direction = "SELL"
        hints.append("weak_entry_below_ema20")

    if ema200 and ((direction == "BUY" and price > ema200) or (direction == "SELL" and price < ema200)):
        hints.append("ema200_aligned")
        score += 6
    else:
        hints.append("ema200_mixed")
        score -= 4

    for name, frame in (("confirm", confirm), ("bias", bias)):
        if frame is None or len(frame) < 20:
            hints.append(f"{name}_missing_or_short")
            score -= 5
            continue
        f_close = pd.to_numeric(frame["close"], errors="coerce")
        f_price = float(f_close.iloc[-1])
        f_ema20 = _ema(f_close, 20)
        f_ema50 = _ema(f_close, 50)
        aligned = (direction == "BUY" and f_price > f_ema20 >= f_ema50) or (direction == "SELL" and f_price < f_ema20 <= f_ema50)
        if aligned:
            hints.append(f"{name}_aligned")
            score += 7
        else:
            hints.append(f"{name}_mixed")
            score -= 4

    return direction, hints, max(0.0, min(100.0, score))


def _make_candidate(
    mode: str,
    entry_tf: str,
    entry: pd.DataFrame,
    confirm: Optional[pd.DataFrame],
    bias: Optional[pd.DataFrame],
    regime: str,
    spread_ok: bool,
    missing: List[str],
) -> Dict:
    spec = _MODE_SPECS[mode]
    direction, hints, quality_score = _direction(entry, confirm, bias)
    price = _last_float(entry, "close")
    atr = _atr_like(entry)
    if atr <= 0:
        atr = max(price * 0.0015, 0.1)

    sl_dist = atr * float(spec["atr_mult"])
    tp_dist = sl_dist * float(spec["rr"])
    if direction == "BUY":
        stop_loss = price - sl_dist
        take_profit = price + tp_dist
        alert_type = "bullish_confirmation"
    else:
        stop_loss = price + sl_dist
        take_profit = price - tp_dist
        alert_type = "bearish_confirmation"

    if not spread_ok:
        quality_score -= 8
        hints.append("spread_warning")
    if regime.upper() in {"TRENDING", "TREND_UP", "TREND_DOWN", "VOLATILE"}:
        quality_score += 4
        hints.append("regime_supportive")
    elif regime.upper() in {"RANGING", "RANGE"} and mode == "GOLD_SCALP":
        quality_score += 2
        hints.append("range_fade_allowed")
    else:
        quality_score -= 3
        hints.append("regime_mixed")

    quality_score = max(0.0, min(100.0, quality_score))
    return {
        "symbol": GOLD_SYMBOL,
        "desk_id": "DESK4_GOLD",
        "desk_mode": mode,
        "strategy_mode": str(spec["strategy_mode"]),
        "stack_id": mode,
        "strategy": str(spec["strategy_mode"]),
        "timeframe": entry_tf,
        "direction": direction,
        "alert_type": alert_type,
        "price": round(price, 3),
        "entry": round(price, 3),
        "stop_loss": round(stop_loss, 3),
        "take_profit": round(take_profit, 3),
        "sl": round(stop_loss, 3),
        "tp1": round(take_profit, 3),
        "tp2": round(price + (take_profit - price) * 1.5, 3) if direction == "BUY" else round(price - (price - take_profit) * 1.5, 3),
        "atr": round(atr, 4),
        "confidence": round(quality_score / 100.0, 4),
        "confluence_score": round(quality_score, 2),
        "quality_score": round(quality_score, 2),
        "regime": regime,
        "volatility_state": "NORMAL_VOL",
        "mode_reason": f"{mode} {entry_tf} EMA structure + MTF confirmation",
        "quality_hints": hints,
        "required_timeframes_present": not missing,
        "missing_timeframes": missing,
        "signal_debug": SIGNAL_DEBUG_MODE,
    }


def scan_gold_modes(
    symbol: str = GOLD_SYMBOL,
    regime: str = "TRANSITIONAL",
    spread_ok: bool = True,
    macro_blackout: bool = False,
    macro_score: float = 0.0,
    timeframe_state: Optional[Dict[str, bool]] = None,
    candle_manager=None,
) -> List[Dict]:
    """Return Gold desk candidates.

    Existing DeskScanner currently passes only symbol/regime/spread/timeframe_state.
    If candle_manager is not provided, this function returns safe debug metadata only
    when SIGNAL_DEBUG_MODE is enabled and required TF state looks available; otherwise
    it returns [] instead of crashing.
    """
    symbol = (symbol or "").upper()
    if symbol != GOLD_SYMBOL:
        return []
    if macro_blackout:
        logger.info("GOLD reject | macro_blackout")
        return []

    timeframe_state = timeframe_state or {}
    out: List[Dict] = []

    # Backward-compatible path: caller did not provide frames. Return light candidates
    # only if the scanner has already validated the entry frame and debug mode is on.
    if candle_manager is None:
        if not SIGNAL_DEBUG_MODE:
            logger.debug("GOLD skip | candle_manager_not_provided")
            return []
        for mode, spec in _MODE_SPECS.items():
            required = list(spec["entry_tfs"]) + [str(spec["confirm_tf"]), str(spec["bias_tf"])]
            missing = [tf for tf in required if timeframe_state and not timeframe_state.get(tf, False)]
            if missing:
                logger.info("GOLD DEBUG reject | %s | missing_tfs=%s", mode, missing)
                continue
            out.append({
                "symbol": GOLD_SYMBOL,
                "desk_id": "DESK4_GOLD",
                "desk_mode": mode,
                "strategy_mode": str(spec["strategy_mode"]),
                "stack_id": mode,
                "strategy": str(spec["strategy_mode"]),
                "direction": "BUY",
                "alert_type": "bullish_confirmation",
                "confidence": 0.58,
                "confluence_score": 58.0,
                "quality_score": 58.0,
                "regime": regime,
                "volatility_state": "UNKNOWN",
                "mode_reason": "debug_candidate_without_frame_access",
                "quality_hints": ["debug_mode", "scanner_validated_entry_frame"],
                "required_timeframes_present": True,
                "missing_timeframes": [],
                "signal_debug": True,
            })
        return out

    for mode, spec in _MODE_SPECS.items():
        for entry_tf in spec["entry_tfs"]:
            entry = candle_manager.get_dataframe(GOLD_SYMBOL, entry_tf)
            confirm = candle_manager.get_dataframe(GOLD_SYMBOL, str(spec["confirm_tf"]))
            bias = candle_manager.get_dataframe(GOLD_SYMBOL, str(spec["bias_tf"]))
            min_bars = max(int(spec["min_bars"]), min(MIN_TIMEFRAME_BARS, 50))
            missing: List[str] = []
            if entry is None or len(entry) < min_bars:
                missing.append(entry_tf)
            if confirm is None or len(confirm) < max(25, min_bars // 2):
                missing.append(str(spec["confirm_tf"]))
            if bias is None or len(bias) < max(25, min_bars // 2):
                missing.append(str(spec["bias_tf"]))
            if missing:
                logger.info("GOLD reject | %s %s | missing_tfs=%s", mode, entry_tf, missing)
                continue
            out.append(_make_candidate(mode, entry_tf, entry, confirm, bias, regime, spread_ok, missing))

    logger.info("GOLD scan complete | candidates=%s regime=%s", len(out), regime)
    return out
