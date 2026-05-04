from typing import Dict, List


GOLD_SYMBOL = "XAUUSD"


def scan_gold_modes(symbol: str, regime: str, spread_ok: bool = True, macro_blackout: bool = False, macro_score: float = 0.0) -> List[Dict]:
    if symbol != GOLD_SYMBOL:
        return []

    candidates = []
    if spread_ok and not macro_blackout:
        candidates.append({
            "desk_id": "DESK4_GOLD", "desk_mode": "GOLD_SCALP", "strategy_mode": "fast_momentum_vwap_ema",
            "mode_reason": f"{regime} scalp setup with squeeze/reclaim eligibility",
            "quality_hints": ["spread_ok", "short_hold", "entry:1M/5M confirm:15M bias:1H"],
        })
    if spread_ok and regime in {"TRENDING", "VOLATILE", "TRANSITIONAL"}:
        candidates.append({
            "desk_id": "DESK4_GOLD", "desk_mode": "GOLD_INTRADAY", "strategy_mode": "supertrend_macd_ema200",
            "mode_reason": f"regime-aligned intraday continuation; macro_score={macro_score:.2f}",
            "quality_hints": ["spread_ok", "macro_placeholder", "entry:15M confirm:1H bias:4H"],
        })
    if regime in {"TRENDING", "TRANSITIONAL"}:
        candidates.append({
            "desk_id": "DESK4_GOLD", "desk_mode": "GOLD_SWING", "strategy_mode": "ema_ichimoku_donchian",
            "mode_reason": "higher-timeframe continuation only; wider ATR stops",
            "quality_hints": ["entry:4H confirm:1D bias:1D/1W", "no_ltf_noise"],
        })
    return candidates
