from typing import Dict, List

from app.config import SIGNAL_DEBUG_MODE

GOLD_SYMBOL = "XAUUSD"


def _mode_payload(mode: str, strategy: str, reason: str, hints: List[str], rejected: str | None = None, tf_ok: bool = True) -> Dict:
    return {
        "desk_id": "DESK4_GOLD",
        "desk_mode": mode,
        "strategy_mode": strategy,
        "mode_reason": reason,
        "quality_hints": hints,
        "rejection_reason": rejected,
        "required_timeframes_present": tf_ok,
        "signal_debug": SIGNAL_DEBUG_MODE,
    }


def scan_gold_modes(symbol: str, regime: str, spread_ok: bool = True, macro_blackout: bool = False, macro_score: float = 0.0, timeframe_state: Dict[str, bool] | None = None) -> List[Dict]:
    if symbol != GOLD_SYMBOL:
        return []

    timeframe_state = timeframe_state or {}
    out: List[Dict] = []
    spread_warning = None if spread_ok else "spread_unacceptable"

    scalp_tf_ok = any(timeframe_state.get(tf, False) for tf in ["1M", "5M"]) and timeframe_state.get("15M", False) and timeframe_state.get("1H", False)
    intraday_tf_ok = timeframe_state.get("15M", False) and timeframe_state.get("1H", False) and timeframe_state.get("4H", False)
    swing_tf_ok = timeframe_state.get("4H", False) and timeframe_state.get("D", False)

    if spread_ok and not macro_blackout and scalp_tf_ok:
        out.append(_mode_payload("GOLD_SCALP", "fast_momentum_vwap_ema", f"{regime} scalp with EMA/VWAP reclaim and squeeze breakout", ["entry:1M/5M", "confirm:15M", "bias:1H", "atr_sl_tp", "range_fade_only_when_ranging"] ))
    elif SIGNAL_DEBUG_MODE:
        out.append(_mode_payload("GOLD_SCALP", "fast_momentum_vwap_ema", "debug_soft_pass", ["candidate_debug"], rejected=spread_warning or ("missing_required_timeframes" if not scalp_tf_ok else "macro_blackout"), tf_ok=scalp_tf_ok))

    regime_ok = regime in {"TRENDING", "VOLATILE", "TRANSITIONAL"}
    if (spread_ok and regime_ok and intraday_tf_ok) or (SIGNAL_DEBUG_MODE and spread_ok and intraday_tf_ok):
        rej = None if regime_ok else "regime_misaligned"
        reason = f"intraday ema200/macd/supertrend continuation macro={macro_score:.2f}" if regime_ok else "debug_soft_pass"
        out.append(_mode_payload("GOLD_INTRADAY", "supertrend_macd_ema200", reason, ["entry:15M", "confirm:1H", "bias:4H", "atr_sl_tp", "pullback_or_squeeze"], rejected=rej, tf_ok=intraday_tf_ok))

    if swing_tf_ok and regime in {"TRENDING", "TRANSITIONAL"}:
        out.append(_mode_payload("GOLD_SWING", "ema50_ema200_donchian", "higher-timeframe continuation with wider ATR stops", ["entry:4H", "confirm:D", "bias:D/W", "avoid_ltf_noise", "atr_wide_stops"], tf_ok=swing_tf_ok))
    elif SIGNAL_DEBUG_MODE and swing_tf_ok:
        out.append(_mode_payload("GOLD_SWING", "ema50_ema200_donchian", "debug_soft_pass", ["candidate_debug"], rejected="regime_misaligned", tf_ok=swing_tf_ok))

    if not spread_ok and SIGNAL_DEBUG_MODE:
        for c in out:
            c["quality_hints"].append("warning:spread_missing_or_unacceptable")

    return out
