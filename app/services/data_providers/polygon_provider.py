from __future__ import annotations

import logging

import httpx

from app.services.data_providers.base import MarketDataProvider, standard_quote
from app.services.data_providers.mock_provider import MockProvider

logger = logging.getLogger("TradingSystem.DataProviders.Polygon")


class PolygonProvider(MarketDataProvider):
    """Light Polygon adapter. Falls back to mock candles until asset mappings are expanded."""

    provider_name = "polygon"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self._client = httpx.AsyncClient(timeout=15.0)
        self._fallback = MockProvider()

    async def get_candles(self, symbol: str, timeframe: str, limit: int = 300) -> list[dict]:
        logger.debug("Polygon candle support is stubbed for %s %s; using mock-compatible data", symbol, timeframe)
        candles = await self._fallback.get_candles(symbol, timeframe, limit)
        for candle in candles:
            candle["provider"] = self.provider_name
        return candles

    async def get_quote(self, symbol: str) -> dict:
        candles = await self.get_candles(symbol, "1M", 1)
        if not candles:
            return {}
        return standard_quote(symbol.upper(), candles[-1]["close"], self.provider_name)

    async def healthcheck(self) -> dict:
        return {"provider": self.provider_name, "status": "stubbed", "message": "Polygon adapter degrades to synthetic candles"}
