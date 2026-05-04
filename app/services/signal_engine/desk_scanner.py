"""
Desk Scanner — per-desk scan function.

Now includes:
- Desk-aware strategy routing
- Gold mode routing
- Cross-desk bias alignment (DESK3 → DESK2 → DESK1)
"""
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List

from app.config import DESKS
from app.services.signal_engine.indicator_calculator import IndicatorCalculator
from app.services.signal_engine.strategy_stacks import run_stacks, detect_regime_adx_atr
from app.services.signal_engine.market_hours_filter import is_valid_trading_hour
from app.services.signal_engine.candle_manager import CandleManager
from app.services.signal_engine.gold_modes import scan_gold_modes
from app.services.signal_engine.cross_desk_bias import GLOBAL_CROSS_DESK_BIAS

logger = logging.getLogger("TradingSystem.SignalEngine.DeskScanner")


class DeskScanner:
    def __init__(self, candle_manager: CandleManager):
        self._cm = candle_manager
        self._calc = IndicatorCalculator()

    def scan_desk(self, desk_id: str, regime_cache: Dict[str, str] = None) -> List[Dict]:
        desk = DESKS.get(desk_id)
        if not desk:
            return []

        symbols = desk.get("symbols", [])
        entry_tf = desk.get("timeframes", {}).get("entry", "1H")
        now_utc = datetime.now(timezone.utc)
        candidates = []

        for symbol in symbols:
            if not is_valid_trading_hour(symbol, desk_id, now_utc):
                continue

            df = self._cm.get_dataframe(symbol, entry_tf)
            if df is None or len(df) < 50:
                continue

            indicators = self._calc.compute(df, symbol, entry_tf)
            if not indicators:
                continue

            regime = detect_regime_adx_atr(indicators)

            if desk_id == "DESK4_GOLD":
                gold = scan_gold_modes(symbol=symbol, regime=regime, spread_ok=True)
                for g in gold:
                    g.update({
                        "symbol": symbol,
                        "desk_id": desk_id,
                        "direction": "LONG",
                        "confidence": 0.6,
                        "price": float(df["close"].iloc[-1]),
                        "regime": regime,
                    })
                    candidates.append(g)
                continue

            stack_results = run_stacks(df, indicators, symbol, regime, desk_id=desk_id)

            for result in stack_results:
                result["symbol"] = symbol
                result["desk_id"] = desk_id
                result["price"] = indicators.get("price")
                result["regime"] = regime

                GLOBAL_CROSS_DESK_BIAS.update_from_candidate(result)
                result = GLOBAL_CROSS_DESK_BIAS.apply(result)

                candidates.append(result)

        return candidates
