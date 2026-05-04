"""
Desk Scanner — per-desk scan function that loads OHLCV, resamples to desk
timeframes, computes indicators, checks strategy stacks, and emits raw
signal candidates.

Scan intervals:
  DESK1: every 60s   (scalper — 1M entry)
  DESK2: every 5min  (intraday — 15M entry)
  DESK3: every 15min (swing — 4H entry)
  DESK4: every 2min  (gold multi-TF)
  DESK5: every 5min  (momentum — 1H entry)
  DESK6: every 15min (equities — 1H entry)
"""
import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

from app.config import DESKS, get_desk_for_symbol, get_atr_settings
from app.services.signal_engine.indicator_calculator import IndicatorCalculator
from app.services.signal_engine.strategy_stacks import run_stacks, detect_regime_adx_atr
from app.services.signal_engine.market_hours_filter import is_valid_trading_hour
from app.services.signal_engine.candle_manager import CandleManager

logger = logging.getLogger("TradingSystem.SignalEngine.DeskScanner")

# Scan intervals per desk (seconds)
SCAN_INTERVALS = {
    "DESK1_SCALPER":  60,
    "DESK2_INTRADAY": 300,
    "DESK3_SWING":    900,
    "DESK4_GOLD":     120,
    "DESK5_ALTS":     300,
    "DESK6_EQUITIES": 900,
}


class DeskScanner:
    """Scans all symbols for a desk using strategy stacks."""

    def __init__(self, candle_manager: CandleManager):
        self._cm = candle_manager
        self._calc = IndicatorCalculator()
        self._scan_count = 0
        self._signal_count = 0

    def scan_desk(
        self, desk_id: str, regime_cache: Dict[str, str] = None,
    ) -> List[Dict]:
        """
        Scan all symbols for a desk. Returns list of raw signal candidates.

        Each candidate has: symbol, direction, alert_type, strategy, confidence,
        desk_id, timeframe, price, sl, tp1, tp2, regime, stack_id.
        """
        desk = DESKS.get(desk_id)
        if not desk:
            return []

        symbols = desk.get("symbols", [])
        desk_tfs = desk.get("timeframes", {})
        entry_tf = self._get_entry_tf(desk_tfs)
        now_utc = datetime.now(timezone.utc)
        candidates = []
        regime_cache = regime_cache or {}

        for symbol in symbols:
            # Market hours filter
            if not is_valid_trading_hour(symbol, desk_id, now_utc):
                continue

            # Get entry timeframe DataFrame
            df = self._cm.get_dataframe(symbol, entry_tf)
            if df is None or len(df) < 50:
                continue

            # Compute indicators
            regime = regime_cache.get(symbol)
            indicators = self._calc.compute(df, symbol, entry_tf, regime=regime)
            if not indicators:
                continue

            # Detect regime from ADX/ATR if not cached
            if not regime:
                regime = detect_regime_adx_atr(indicators)

            # Run applicable strategy stacks
            stack_results = run_stacks(df, indicators, symbol, regime)

            for result in stack_results:
                # Compute ATR-based SL/TP if not provided by stack
                price = indicators.get("price", 0)
                atr = indicators.get("atr", 0)

                if "sl" not in result and price > 0 and atr > 0:
                    cfg = get_atr_settings(desk_id, symbol, entry_tf)
                    sl_mult = cfg.get("sl_mult", 2.0)
                    tp1_mult = cfg.get("tp1_mult", 4.0)
                    tp2_mult = cfg.get("tp2_mult", 6.0)

                    if result["direction"] == "LONG":
                        result["sl"] = round(price - atr * sl_mult, 5)
                        result["tp1"] = round(price + atr * tp1_mult, 5)
                        result["tp2"] = round(price + atr * tp2_mult, 5)
                    else:
                        result["sl"] = round(price + atr * sl_mult, 5)
                        result["tp1"] = round(price - atr * tp1_mult, 5)
                        result["tp2"] = round(price - atr * tp2_mult, 5)

                # R:R check
                if result.get("sl") and result.get("tp1") and price > 0:
                    sl_dist = abs(price - result["sl"])
                    tp_dist = abs(result["tp1"] - price)
                    if sl_dist > 0 and tp_dist / sl_dist < 1.5:
                        continue  # Skip bad R:R

                result["symbol"] = symbol
                result["desk_id"] = desk_id
                result["timeframe"] = entry_tf
                result["price"] = price
                result["atr"] = atr

                candidates.append(result)
                self._signal_count += 1

        self._scan_count += 1
        if candidates:
            logger.info(
                f"SCAN | {desk_id} | {len(candidates)} candidates from "
                f"{len(symbols)} symbols | Regime mix: "
                f"{self._regime_summary(candidates)}"
            )

        return candidates

    @staticmethod
    def build_signal_payload(candidate: Dict) -> Dict:
        """Convert a raw candidate into the Redis Stream payload format."""
        symbol = candidate["symbol"]
        direction = candidate["direction"]
        desk_id = candidate["desk_id"]
        desks_matched = get_desk_for_symbol(symbol)
        if desk_id not in desks_matched:
            desks_matched.append(desk_id)

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
            "source": "python_engine",
            "confluence_score": candidate.get("confidence", 0.5) * 10,
            "strategy_id": candidate.get("strategy", "unknown"),
            "quality_score": candidate.get("confidence", 0.5) * 100,
            "quality_tier": "HIGH" if candidate.get("confidence", 0) > 0.7 else "MEDIUM",
            "quality_size_mult": 1.0 if candidate.get("confidence", 0) > 0.7 else 0.5,
            "regime": candidate.get("regime", "UNKNOWN"),
            "stack_id": candidate.get("stack_id", "?"),
        }

    @staticmethod
    def _get_entry_tf(desk_tfs: Dict) -> str:
        """Extract entry timeframe from desk config."""
        entry = desk_tfs.get("entry", "1H")
        return entry.split(",")[0].strip().upper()

    @staticmethod
    def _regime_summary(candidates: list) -> str:
        """Summarize regime distribution in candidates."""
        regimes = {}
        for c in candidates:
            r = c.get("regime", "?")
            regimes[r] = regimes.get(r, 0) + 1
        return " ".join(f"{k}={v}" for k, v in regimes.items())

    @property
    def stats(self) -> Dict:
        return {"scans": self._scan_count, "signals": self._signal_count}
