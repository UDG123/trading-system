from __future__ import annotations

import logging

import httpx

from app.services.data_providers.base import MarketDataProvider, standard_candle, standard_quote, with_retries
from app.services.ohlcv_ingester import TD_MAP

logger = logging.getLogger("TradingSystem.DataProviders.TwelveData")

TF_TO_TD = {"1M": "1min", "5M": "5min", "15M": "15min", "30M": "30min", "1H": "1h", "4H": "4h", "D": "1day", "W": "1week"}


class TwelveDataProvider(MarketDataProvider):
    provider_name = "twelvedata"

    def __init__(self, api_key: str):
        self.api_key = api_key
        self._client = httpx.AsyncClient(timeout=15.0)

    def _symbol(self, symbol: str) -> str:
        return TD_MAP.get(symbol.upper(), symbol.upper())

    async def get_candles(self, symbol: str, timeframe: str, limit: int = 300) -> list[dict]:
        interval = TF_TO_TD.get(timeframe.upper())
        if not interval:
            return []

        async def request():
            resp = await self._client.get("https://api.twelvedata.com/time_series", params={
                "symbol": self._symbol(symbol), "interval": interval, "outputsize": limit,
                "apikey": self.api_key, "format": "JSON", "dp": 5,
            })
            resp.raise_for_status()
            return resp.json()

        data = await with_retries(request)
        if not data or data.get("status") == "error":
            logger.debug("TwelveData returned no candles for %s %s: %s", symbol, timeframe, data)
            return []
        candles = []
        for row in reversed(data.get("values", [])):
            try:
                candles.append(standard_candle(row, self.provider_name))
            except Exception:
                continue
        return candles

    async def get_quote(self, symbol: str) -> dict:
        async def request():
            resp = await self._client.get("https://api.twelvedata.com/quote", params={"symbol": self._symbol(symbol), "apikey": self.api_key})
            resp.raise_for_status()
            return resp.json()
        data = await with_retries(request)
        if not data:
            candles = await self.get_candles(symbol, "1M", 1)
            if not candles:
                return {}
            return standard_quote(symbol.upper(), candles[-1]["close"], self.provider_name)
        bid = float(data["bid"]) if data.get("bid") not in {None, ""} else None
        ask = float(data["ask"]) if data.get("ask") not in {None, ""} else None
        mid = float(data.get("close") or ((bid + ask) / 2 if bid and ask else 0))
        return standard_quote(symbol.upper(), mid, self.provider_name, bid, ask)

    async def healthcheck(self) -> dict:
        quote = await self.get_quote("EURUSD")
        return {"provider": self.provider_name, "status": "ok" if quote else "degraded"}
