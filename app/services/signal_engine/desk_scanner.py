"""
Desk Scanner — per-desk scan function that loads OHLCV, computes indicators,
checks strategy stacks, and emits raw signal candidates.

Includes:
- Desk-aware FX stack routing for DESK1/2/3
- Gold mode routing for DESK4_GOLD
- Cross-desk bias alignment: DESK3 sets HTF bias; DESK2/1 adjust
- Redis-stream payload builder retained for engine compatibility
"""
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List

import os
from app.config import DESKS, get_desk_for_symbol, get_atr_settings
from app.services.signal_engine.indicator_calculator import IndicatorCalculator
from app.services.signal_engine.strategy_stacks import run_stacks, detect_regime_adx_atr
from app.services.signal_engine.market_hours_filter import is_valid_trading_hour
from app.services.signal_engine.candle_manager import CandleManager
from app.services.signal_engine.gold_modes import scan_gold_modes
from app.services.signal_engine.cross_desk_bias import GLOBAL_CROSS_DESK_BIAS

logger = logging.getLogger("TradingSystem.SignalEngine.DeskScanner")

SCAN_INTERVALS = {
    "DESK1_SCALPER": 60,
    "DESK2_INTRADAY": 300,
    "DESK3_SWING": 900,
    "DESK4_GOLD": 120,
    "DESK5_ALTS": 300,
    "DESK6_EQUITIES": 900,
}


class DeskScanner:
    """Scans symbols for a desk using desk-aware strategy stacks."""

    def __init__(self, candle_manager: CandleManager):
        self._cm = candle_manager
        self._calc = IndicatorCalculator()
        self._scan_count = 0
        self._signal_count = 0

    def scan_desk(self, desk_id: str, regime_cache: Dict[str, str] = None) -> List[Dict]:
        desk = DESKS.get(desk_id)
        if not desk:
            return []

        symbols = desk.get("symbols", [])
        desk_tfs = desk.get("timeframes", {})
        entry_tf = self._get_entry_tf(desk_tfs)
        now_utc = datetime.now(timezone.utc)
        candidates: List[Dict] = []
        regime_cache = regime_cache or {}

        for symbol in symbols:
            if not is_valid_trading_hour(symbol, desk_id, now_utc):
                continue

            df = self._cm.get_dataframe(symbol, entry_tf)
            if df is None or len(df) < 50:
                continue

            regime = regime_cache.get(symbol)
            indicators = self._calc.compute(df, symbol, entry_tf, regime=regime)
            if not indicators:
                continue

            if not regime:
                regime = detect_regime_adx_atr(indicators)

            if desk_id == "DESK4_GOLD":
                gold_candidates = scan_gold_modes(symbol=symbol, regime=regime or "TRANSITIONAL", spread_ok=True)
                for result in gold_candidates:
                    price = float(df["close"].iloc[-1])
                    atr = float(indicators.get("atr", 0) or 0)
                    result.update({
                        "symbol": symbol,
                        "desk_id": desk_id,
                        "timeframe": entry_tf,
                        "direction": result.get("direction", "LONG"),
                        "alert_type": result.get("alert_type", "bullish_confirmation"),
                        "confidence": result.get("confidence", 0.6),
                        "price": price,
                        "atr": atr,
                        "regime": regime,
                        "stack_id": result.get("stack_id", "GOLD_MODE"),
                    })
                    self._apply_atr_targets(result, desk_id, symbol, entry_tf, price, atr)
                    candidates.append(result)
                    self._signal_count += 1
                continue

            stack_results = run_stacks(df, indicators, symbol, regime, desk_id=desk_id)

            for result in stack_results:
                price = indicators.get("price", 0)
                atr = indicators.get("atr", 0)
                self._apply_atr_targets(result, desk_id, symbol, entry_tf, price, atr)

                if result.get("sl") and result.get("tp1") and price:
                    sl_dist = abs(price - result["sl"])
                    tp_dist = abs(result["tp1"] - price)
                    if sl_dist > 0 and tp_dist / sl_dist < 1.5:
                        continue

                result.update({
                    "symbol": symbol,
                    "desk_id": desk_id,
                    "timeframe": entry_tf,
                    "price": price,
                    "atr": atr,
                    "regime": regime,
                })

                GLOBAL_CROSS_DESK_BIAS.update_from_candidate(result)
                result = GLOBAL_CROSS_DESK_BIAS.apply(result)

                if result.get("blocked_by_bias") and os.getenv("ENABLE_HARD_BIAS_FILTER", "false").lower() in {"1","true","yes","on"}:
                    logger.info(
                        "BIAS BLOCK | %s %s %s counter to %s",
                        desk_id, symbol, result.get("direction"), result.get("cross_desk_bias"),
                    )
                    continue

                candidates.append(result)
                self._signal_count += 1

        self._scan_count += 1
        if candidates:
            logger.info(
                "SCAN | %s | %s candidates from %s symbols | %s",
                desk_id, len(candidates), len(symbols), self._regime_summary(candidates),
            )
        return candidates

    @staticmethod
    def _apply_atr_targets(result: Dict, desk_id: str, symbol: str, timeframe: str, price: float, atr: float) -> None:
        if result.get("sl") or not price or not atr:
            return
        cfg = get_atr_settings(desk_id, symbol, timeframe)
        sl_mult = cfg.get("sl_mult", 2.0)
        tp1_mult = cfg.get("tp1_mult", 4.0)
        tp2_mult = cfg.get("tp2_mult", 6.0)
        if result.get("direction") == "LONG":
            result["sl"] = round(price - atr * sl_mult, 5)
            result["tp1"] = round(price + atr * tp1_mult, 5)
            result["tp2"] = round(price + atr * tp2_mult, 5)
        else:
            result["sl"] = round(price + atr * sl_mult, 5)
            result["tp1"] = round(price - atr * tp1_mult, 5)
            result["tp2"] = round(price - atr * tp2_mult, 5)

    @staticmethod
    def build_signal_payload(candidate: Dict) -> Dict:
        """Convert a raw candidate into the Redis Stream payload format."""
        symbol = candidate["symbol"]
        direction = candidate["direction"]
        desk_id = candidate["desk_id"]
        desks_matched = get_desk_for_symbol(symbol)
        if desk_id not in desks_matched:
            desks_matched.append(desk_id)

        confidence = float(candidate.get("confidence", 0.5) or 0.5)
        desk_cfg = DESKS.get(desk_id, {})
        desk_role = candidate.get("desk_role") or desk_cfg.get("role")
        desk_mode = candidate.get("desk_mode") or desk_role
        strategy_mode = candidate.get("strategy_mode") or candidate.get("strategy") or candidate.get("stack_id") or "unknown"
        return {
            "symbol": symbol,
            "symbol_normalized": symbol,
            "exchange": "",
            "timeframe": candidate.get("timeframe", "1H"),
            "alert_type": candidate.get("alert_type", f"{'bullish' if direction == 'LONG' else 'bearish'}_confirmation"),
            "direction": direction,
            "price": candidate.get("price", 0),
            "tp1": candidate.get("tp1"),
            "tp2": candidate.get("tp2"),
            "sl1": candidate.get("sl"),
            "sl2": None,
            "smart_trail": None,
            "volume": None,
            "desks_matched": desks_matched,
            "webhook_latency_ms": 0,
            "time": str(int(time.time() * 1000)),
            "source": "internal_engine",
            "confluence_score": confidence * 10,
            "strategy_id": candidate.get("strategy", candidate.get("strategy_mode", "unknown")),
            "quality_score": confidence * 100,
            "quality_tier": "HIGH" if confidence > 0.7 else "MEDIUM",
            "quality_size_mult": 1.0 if confidence > 0.7 else 0.5,
            "regime": candidate.get("regime", "UNKNOWN"),
            "stack_id": candidate.get("stack_id", "?"),
            "desk_mode": desk_mode,
            "desk_role": desk_role,
            "strategy_mode": strategy_mode,
            "mode_reason": candidate.get("mode_reason") or "internal_engine_scan",
            "quality_hints": candidate.get("quality_hints", []),
            "cross_desk_bias": candidate.get("cross_desk_bias") or "NEUTRAL",
            "bias_alignment": candidate.get("bias_alignment") or "NEUTRAL",
            "bias_action": candidate.get("bias_action") or "PASS",
            "bias_size_mult": candidate.get("bias_size_mult", 1.0),
            "blocked_by_bias": bool(candidate.get("blocked_by_bias", False)),
        }

    @staticmethod
    def _get_entry_tf(desk_tfs: Dict) -> str:
        entry = desk_tfs.get("entry", "1H")
        return entry.split(",")[0].strip().upper()

    @staticmethod
    def _regime_summary(candidates: list) -> str:
        regimes = {}
        for c in candidates:
            r = c.get("regime", "?")
            regimes[r] = regimes.get(r, 0) + 1
        return " ".join(f"{k}={v}" for k, v in regimes.items())

    @property
    def stats(self) -> Dict:
        return {"scans": self._scan_count, "signals": self._signal_count}
