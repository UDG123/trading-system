from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from app.services.live_data.normalizer import TickEvent, QuoteEvent, BarEvent

_TF_MINUTES = {"1M": 1, "5M": 5, "15M": 15, "1H": 60, "4H": 240}


@dataclass
class _BarState:
    open: float
    high: float
    low: float
    close: float
    volume: float


class BarAggregator:
    def __init__(self):
        self._states: Dict[Tuple[str, str, datetime], _BarState] = {}
        self._latest_bucket: Dict[Tuple[str, str], datetime] = {}

    def _bucket_start(self, ts: datetime, timeframe: str) -> datetime:
        ts = ts.astimezone(timezone.utc)
        minutes = _TF_MINUTES[timeframe]
        total = ts.hour * 60 + ts.minute
        start_total = (total // minutes) * minutes
        return ts.replace(hour=start_total // 60, minute=start_total % 60, second=0, microsecond=0)

    def ingest_tick(self, ev: TickEvent) -> List[BarEvent]:
        return self._ingest(ev.symbol, ev.price, ev.size, ev.ts, ev.provider)

    def ingest_quote(self, ev: QuoteEvent) -> List[BarEvent]:
        mid = (ev.bid + ev.ask) / 2 if ev.bid and ev.ask else 0
        return self._ingest(ev.symbol, mid, ev.bid_size + ev.ask_size, ev.ts, ev.provider)

    def _ingest(self, symbol: str, price: float, volume: float, ts: datetime, provider: str) -> List[BarEvent]:
        completed: List[BarEvent] = []
        if price <= 0:
            return completed
        for tf in _TF_MINUTES:
            b = self._bucket_start(ts, tf)
            k = (symbol, tf, b)
            st = self._states.get(k)
            if st is None:
                self._states[k] = _BarState(price, price, price, price, volume)
            else:
                st.high = max(st.high, price); st.low = min(st.low, price); st.close = price; st.volume += volume

            latest_key = (symbol, tf)
            prev_latest = self._latest_bucket.get(latest_key)
            if prev_latest is None or b > prev_latest:
                self._latest_bucket[latest_key] = b
                if prev_latest is not None:
                    prev_state = self._states.pop((symbol, tf, prev_latest), None)
                    if prev_state:
                        completed.append(BarEvent(symbol, tf, prev_state.open, prev_state.high, prev_state.low, prev_state.close, prev_state.volume, prev_latest, provider))
        return completed

    async def publish_completed_bars(self, redis, bars: List[BarEvent], stream: str = "live:bars") -> None:
        for b in bars:
            await redis.xadd(stream, {
                "symbol": b.symbol, "timeframe": b.timeframe, "time": b.ts.isoformat(),
                "open": b.open, "high": b.high, "low": b.low, "close": b.close, "volume": b.volume,
                "provider": b.provider,
            })
