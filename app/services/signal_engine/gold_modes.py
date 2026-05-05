"""DESK4_GOLD mode router and precision scoring.

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

            missing = []
            bars_by_tf = {
                entry_tf: 0 if entry is None else len(entry),
                spec.confirm_tf: 0 if confirm is None else len(confirm),
                spec.bias_tf: 0 if bias is None else len(bias),
            }
            if entry is None or len(entry) < cfg.min_timeframe_bars:
                missing.append(entry_tf)
            if confirm is None or len(confirm) < cfg.min_timeframe_bars:
                missing.append(spec.confirm_tf)
            if bias is None or len(bias) < cfg.min_timeframe_bars:
                missing.append(spec.bias_tf)

            if missing and not cfg.signal_debug_mode:
                logger.info("GOLD reject | %s %s | missing_tfs=%s bars=%s", spec.mode, entry_tf, missing, bars_by_tf)
                continue
            if entry is None or len(entry) < max(30, min(cfg.min_timeframe_bars, 50)):
                logger.info("GOLD reject | %s %s | no_entry_frame bars=%s", spec.mode, entry_tf, bars_by_tf)
                continue

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
