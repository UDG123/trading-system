"""DESK4_GOLD mode router and precision scoring.

Gold is treated as a three-mode XAUUSD-only desk:
- GOLD_SCALP: 1M/5M entries with 15M confirmation and 1H bias
- GOLD_INTRADAY: 15M entries with 1H confirmation and 4H bias
- GOLD_SWING: 4H entries with D/W bias

This module intentionally avoids broker execution. It only produces signal candidates for
the paper-trading signal pipeline.
"""
from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd

from app.services.signal_engine.debug_config import get_signal_debug_config

logger = logging.getLogger("TradingSystem.SignalEngine.GoldModes")

GOLD_SYMBOL = "XAUUSD"


def _last(df: Optional[pd.DataFrame], col: str, default: float = 0.0) -> float:
    try:
        if df is None or df.empty or col not in df.columns:
            return default
        val = df[col].iloc[-1]
        return float(val) if pd.notna(val) else default
    except Exception:
        return default


def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def _atr(df: pd.DataFrame, length: int = 14) -> pd.Series:
    high = df["high"].astype(float)
    low = df["low"].astype(float)
    close = df["close"].astype(float)
    prev_close = close.shift(1)
    tr = pd.concat([(high - low), (high - prev_close).abs(), (low - prev_close).abs()], axis=1).max(axis=1)
    return tr.rolling(length).mean()


def _rsi(close: pd.Series, length: int = 14) -> pd.Series:
    delta = close.diff()
    up = delta.clip(lower=0).rolling(length).mean()
    down = (-delta.clip(upper=0)).rolling(length).mean()
    rs = up / down.replace(0, pd.NA)
    return 100 - (100 / (1 + rs))


def _macd_hist(close: pd.Series) -> pd.Series:
    fast = _ema(close, 12)
    slow = _ema(close, 26)
    macd = fast - slow
    signal = _ema(macd, 9)
    return macd - signal


def _bb_width(df: pd.DataFrame, length: int = 20) -> float:
    close = df["close"].astype(float)
    mid = close.rolling(length).mean()
    std = close.rolling(length).std()
    upper = mid + 2 * std
    lower = mid - 2 * std
    width = (upper - lower) / mid.replace(0, pd.NA)
    return float(width.iloc[-1]) if len(width) and pd.notna(width.iloc[-1]) else 999.0


def _vol_state(df: pd.DataFrame) -> str:
    if df is None or len(df) < 30:
        return "UNKNOWN"
    atr = _atr(df, 14)
    if atr.dropna().empty:
        return "UNKNOWN"
    latest = atr.iloc[-1]
    p70 = atr.dropna().rolling(50).quantile(0.70).iloc[-1] if len(atr.dropna()) >= 50 else atr.dropna().quantile(0.70)
    p30 = atr.dropna().rolling(50).quantile(0.30).iloc[-1] if len(atr.dropna()) >= 50 else atr.dropna().quantile(0.30)
    if latest >= p70:
        return "HIGH_VOL"
    if latest <= p30:
        return "LOW_VOL"
    return "NORMAL_VOL"


def _regime_from_frames(entry: pd.DataFrame, bias: Optional[pd.DataFrame]) -> str:
    df = bias if bias is not None and len(bias) >= 80 else entry
    if df is None or len(df) < 50:
        return "UNKNOWN"
    close = df["close"].astype(float)
    ema50 = _ema(close, 50)
    ema200 = _ema(close, 200) if len(close) >= 200 else _ema(close, 100)
    width = _bb_width(df)
    slope = ema50.iloc[-1] - ema50.iloc[-10] if len(ema50) >= 10 else 0
    if width < 0.006:
        return "RANGING"
    if close.iloc[-1] > ema50.iloc[-1] > ema200.iloc[-1] and slope > 0:
        return "TREND_UP"
    if close.iloc[-1] < ema50.iloc[-1] < ema200.iloc[-1] and slope < 0:
        return "TREND_DOWN"
    return "MIXED"


@dataclass
class GoldModeSpec:
    mode: str
    entry_tfs: List[str]
    confirm_tf: str
    bias_tf: str
    min_score: float
    atr_sl_mult: float
    atr_tp_mult: float


GOLD_MODES = [
    GoldModeSpec("GOLD_SCALP", ["1M", "5M"], "15M", "1H", 58.0, 1.2, 1.8),
    GoldModeSpec("GOLD_INTRADAY", ["15M"], "1H", "4H", 62.0, 1.6, 2.4),
    GoldModeSpec("GOLD_SWING", ["4H"], "D", "D", 65.0, 2.2, 3.2),
]


def _frame(candle_manager: Any, tf: str) -> Optional[pd.DataFrame]:
    try:
        return candle_manager.get_dataframe(GOLD_SYMBOL, tf)
    except Exception:
        return None


def _score_direction(entry: pd.DataFrame, confirm: Optional[pd.DataFrame], bias: Optional[pd.DataFrame], spec: GoldModeSpec) -> Dict[str, Any]:
    close = entry["close"].astype(float)
    price = float(close.iloc[-1])
    ema9 = _ema(close, 9).iloc[-1]
    ema21 = _ema(close, 21).iloc[-1]
    ema50 = _ema(close, 50).iloc[-1]
    ema200 = _ema(close, 200).iloc[-1] if len(close) >= 200 else _ema(close, 100).iloc[-1]
    rsi = _rsi(close).iloc[-1] if len(close) >= 20 else 50
    macd = _macd_hist(close).iloc[-1] if len(close) >= 35 else 0
    atr = _atr(entry).iloc[-1] if len(entry) >= 20 else max(price * 0.0015, 1.0)
    width = _bb_width(entry)
    regime = _regime_from_frames(entry, bias)
    vol_state = _vol_state(entry)

    long_score = 0.0
    short_score = 0.0
    reasons_long: List[str] = []
    reasons_short: List[str] = []

    if price > ema9 > ema21:
        long_score += 18; reasons_long.append("fast_ema_stack_up")
    if price < ema9 < ema21:
        short_score += 18; reasons_short.append("fast_ema_stack_down")
    if price > ema50:
        long_score += 10; reasons_long.append("above_ema50")
    if price < ema50:
        short_score += 10; reasons_short.append("below_ema50")
    if price > ema200:
        long_score += 12; reasons_long.append("above_ema200")
    if price < ema200:
        short_score += 12; reasons_short.append("below_ema200")
    if macd > 0:
        long_score += 12; reasons_long.append("macd_positive")
    if macd < 0:
        short_score += 12; reasons_short.append("macd_negative")
    if 50 <= rsi <= 72:
        long_score += 8; reasons_long.append("rsi_bullish_not_extreme")
    if 28 <= rsi <= 50:
        short_score += 8; reasons_short.append("rsi_bearish_not_extreme")

    if confirm is not None and len(confirm) >= 50:
        cclose = confirm["close"].astype(float)
        cema50 = _ema(cclose, 50).iloc[-1]
        if cclose.iloc[-1] > cema50:
            long_score += 14; reasons_long.append("confirm_tf_bullish")
        if cclose.iloc[-1] < cema50:
            short_score += 14; reasons_short.append("confirm_tf_bearish")

    if bias is not None and len(bias) >= 50:
        bclose = bias["close"].astype(float)
        bema50 = _ema(bclose, 50).iloc[-1]
        if bclose.iloc[-1] > bema50:
            long_score += 14; reasons_long.append("bias_tf_bullish")
        if bclose.iloc[-1] < bema50:
            short_score += 14; reasons_short.append("bias_tf_bearish")

    if width < 0.008:
        long_score += 4; short_score += 4
        reasons_long.append("bb_squeeze_ready"); reasons_short.append("bb_squeeze_ready")

    # Regime intelligence: reward alignment, punish mismatch.
    if regime == "TREND_UP":
        long_score += 10; short_score -= 8; reasons_long.append("regime_trend_up")
    elif regime == "TREND_DOWN":
        short_score += 10; long_score -= 8; reasons_short.append("regime_trend_down")
    elif regime == "RANGING" and spec.mode == "GOLD_SCALP":
        # allow scalp range fades only if RSI is stretched
        if rsi < 35:
            long_score += 8; reasons_long.append("range_fade_oversold")
        if rsi > 65:
            short_score += 8; reasons_short.append("range_fade_overbought")
    elif regime == "MIXED":
        long_score -= 4; short_score -= 4

    direction = "BUY" if long_score >= short_score else "SELL"
    score = max(long_score, short_score)
    reasons = reasons_long if direction == "BUY" else reasons_short

    sl = price - spec.atr_sl_mult * atr if direction == "BUY" else price + spec.atr_sl_mult * atr
    tp = price + spec.atr_tp_mult * atr if direction == "BUY" else price - spec.atr_tp_mult * atr

    return {
        "direction": direction,
        "score": round(max(0.0, min(score, 100.0)), 2),
        "entry": price,
        "stop_loss": round(sl, 3),
        "take_profit": round(tp, 3),
        "atr": round(float(atr), 3),
        "regime": regime,
        "volatility_state": vol_state,
        "mode_reason": ",".join(reasons[:6]),
        "quality_hints": reasons,
    }


def scan_gold_modes(candle_manager: Any) -> List[Dict[str, Any]]:
    cfg = get_signal_debug_config()
    candidates: List[Dict[str, Any]] = []

    for spec in GOLD_MODES:
        for entry_tf in spec.entry_tfs:
            entry = _frame(candle_manager, entry_tf)
            confirm = _frame(candle_manager, spec.confirm_tf)
            bias = _frame(candle_manager, spec.bias_tf)

            missing = []
            if entry is None or len(entry) < cfg.min_timeframe_bars:
                missing.append(entry_tf)
            if confirm is None or len(confirm) < cfg.min_timeframe_bars:
                missing.append(spec.confirm_tf)
            if bias is None or len(bias) < cfg.min_timeframe_bars:
                missing.append(spec.bias_tf)

            if missing and not cfg.signal_debug_mode:
                logger.info("GOLD reject | %s %s | missing_tfs=%s", spec.mode, entry_tf, missing)
                continue
            if entry is None or len(entry) < max(30, min(cfg.min_timeframe_bars, 50)):
                logger.info("GOLD reject | %s %s | no_entry_frame", spec.mode, entry_tf)
                continue

            result = _score_direction(entry, confirm, bias, spec)
            min_score = min(spec.min_score, cfg.min_confluence_score) if cfg.signal_debug_mode else spec.min_score
            accepted = result["score"] >= min_score and not missing
            if cfg.signal_debug_mode and result["score"] >= (min_score - 10):
                accepted = True

            if not accepted:
                logger.info(
                    "GOLD reject | mode=%s tf=%s score=%.1f min=%.1f regime=%s reason=score_below_threshold missing=%s",
                    spec.mode, entry_tf, result["score"], min_score, result["regime"], missing,
                )
                continue

            candidate = {
                "symbol": GOLD_SYMBOL,
                "symbol_normalized": GOLD_SYMBOL,
                "desk_id": "DESK4_GOLD",
                "desk_mode": spec.mode,
                "strategy_mode": spec.mode.lower(),
                "timeframe": entry_tf,
                "direction": result["direction"],
                "entry": result["entry"],
                "stop_loss": result["stop_loss"],
                "take_profit": result["take_profit"],
                "confluence_score": result["score"],
                "quality_score": result["score"],
                "strategy_id": f"gold_{spec.mode.lower()}_{entry_tf.lower()}",
                "strategy": f"Gold {spec.mode} {entry_tf}",
                "mode_reason": result["mode_reason"],
                "quality_hints": result["quality_hints"],
                "regime": result["regime"],
                "volatility_state": result["volatility_state"],
                "atr": result["atr"],
                "required_timeframes_present": not bool(missing),
                "missing_timeframes": missing,
                "signal_debug": cfg.signal_debug_mode,
                "alert_type": "GOLD_SIGNAL",
                "desks_matched": ["DESK4_GOLD"],
            }
            logger.info(
                "GOLD candidate | mode=%s tf=%s dir=%s score=%.1f regime=%s reason=%s",
                spec.mode, entry_tf, result["direction"], result["score"], result["regime"], result["mode_reason"],
            )
            candidates.append(candidate)

    return candidates
