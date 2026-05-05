from __future__ import annotations

import math
from datetime import datetime, timedelta, timezone

from app.services.data_providers.base import MarketDataProvider, TF_TO_MINUTES, standard_quote

BASE_PRICES = {
    "EURUSD": 1.0850, "GBPUSD": 1.2650, "USDJPY": 155.20, "AUDUSD": 0.6620,
    "USDCHF": 0.9100, "XAUUSD": 2325.0, "BTCUSD": 65000.0, "ETHUSD": 3200.0,
    "NAS100": 18200.0, "US30": 39000.0, "AAPL": 185.0, "MSFT": 420.0, "NVDA": 900.0, "TSLA": 175.0,
}


class MockProvider(MarketDataProvider):
    provider_name = "mock"

    async def get_candles(self, symbol: str, timeframe: str, limit: int = 300) -> list[dict]:
        symbol = symbol.upper()
        minutes = TF_TO_MINUTES.get(timeframe.upper(), 60)
        base = BASE_PRICES.get(symbol, 100.0)
        now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
        candles = []
        for i in range(limit):
            idx = i - limit + 1
            t = now + timedelta(minutes=idx * minutes)
            wave = math.sin(i / 8.0) * base * 0.001
            trend = i * base * 0.00002
            close = base + wave + trend
            open_ = close - math.cos(i / 6.0) * base * 0.0004
            high = max(open_, close) + base * 0.0007
            low = min(open_, close) - base * 0.0007
            candles.append({
                "time": t.isoformat(), "open": round(open_, 5), "high": round(high, 5),
                "low": round(low, 5), "close": round(close, 5), "volume": float(1000 + i), "provider": self.provider_name,
            })
        return candles

    async def get_quote(self, symbol: str) -> dict:
        candles = await self.get_candles(symbol, "1M", 1)
        mid = candles[-1]["close"]
        spread = mid * 0.0001
        return standard_quote(symbol.upper(), mid, self.provider_name, mid - spread / 2, mid + spread / 2)

    async def healthcheck(self) -> dict:
        return {"provider": self.provider_name, "status": "ok", "mode": "deterministic_mock"}
