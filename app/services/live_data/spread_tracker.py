from collections import defaultdict, deque
from typing import Deque, Dict, Optional


class SpreadTracker:
    def __init__(self, maxlen: int = 200, desk_limits_bps: Optional[Dict[str, float]] = None):
        self._spreads: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=maxlen))
        self._desk_limits_bps = desk_limits_bps or {
            "DESK1_SCALPER": 20.0,
            "DESK2_INTRADAY": 35.0,
            "DESK3_SWING": 60.0,
            "DESK4_GOLD": 30.0,
        }

    def update(self, symbol: str, bid: float, ask: float) -> None:
        if bid <= 0 or ask <= 0 or ask < bid:
            return
        self._spreads[symbol].append(ask - bid)

    def current_spread(self, symbol: str) -> float:
        q = self._spreads.get(symbol)
        return q[-1] if q else 0.0

    def spread_bps(self, symbol: str, mid_price: float) -> float:
        if mid_price <= 0:
            return 0.0
        return (self.current_spread(symbol) / mid_price) * 10_000

    def rolling_percentile(self, symbol: str) -> float:
        q = self._spreads.get(symbol)
        if not q:
            return 0.0
        cur = q[-1]
        ordered = sorted(q)
        return sum(1 for x in ordered if x <= cur) / len(ordered)

    def is_spread_acceptable(self, symbol: str, desk_id: str, mode: str = None, mid_price: float = 0.0) -> bool:
        bps = self.spread_bps(symbol, mid_price) if mid_price > 0 else 0.0
        limit = self._desk_limits_bps.get(desk_id, 40.0)
        if mode == "GOLD_SCALP":
            limit = min(limit, 20.0)
        return bps <= limit if bps > 0 else self.current_spread(symbol) > 0
