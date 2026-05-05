"""Market data provider abstraction for the internal signal engine."""
from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any

from app.config import DATA_PROVIDER, POLYGON_API_KEY, TWELVEDATA_API_KEY

logger = logging.getLogger("TradingSystem.DataProviders")


TF_TO_MINUTES = {"1M": 1, "5M": 5, "15M": 15, "30M": 30, "1H": 60, "4H": 240, "D": 1440, "W": 10080}


class MarketDataProvider(ABC):
    provider_name = "base"

    @abstractmethod
    async def get_candles(self, symbol: str, timeframe: str, limit: int = 300) -> list[dict]:
        """Return candles in standard candle format."""

    @abstractmethod
    async def get_quote(self, symbol: str) -> dict:
        """Return a quote in standard quote format."""

    @abstractmethod
    async def healthcheck(self) -> dict:
        """Return provider health details without raising."""


async def with_retries(coro_factory, attempts: int = 2, base_delay: float = 0.25) -> Any:
    last_exc = None
    for attempt in range(attempts):
        try:
            return await coro_factory()
        except Exception as exc:  # providers must degrade gracefully
            last_exc = exc
            if attempt + 1 < attempts:
                await asyncio.sleep(base_delay * (2 ** attempt))
    logger.debug("Provider request failed after retries: %s", last_exc)
    return None


def iso_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalize_time(value: Any) -> str:
    if isinstance(value, datetime):
        dt = value
    else:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).isoformat()


def standard_candle(raw: dict, provider: str) -> dict:
    return {
        "time": normalize_time(raw.get("time") or raw.get("datetime") or raw.get("timestamp")),
        "open": float(raw["open"]),
        "high": float(raw["high"]),
        "low": float(raw["low"]),
        "close": float(raw["close"]),
        "volume": None if raw.get("volume") is None else float(raw.get("volume") or 0),
        "provider": provider,
    }


def standard_quote(symbol: str, mid: float, provider: str, bid: float | None = None, ask: float | None = None) -> dict:
    return {"symbol": symbol, "bid": bid, "ask": ask, "mid": float(mid), "timestamp": iso_now(), "provider": provider}


def get_market_data_provider() -> MarketDataProvider:
    selected = (DATA_PROVIDER or "TWELVEDATA").upper()
    if selected == "TWELVEDATA" and TWELVEDATA_API_KEY:
        from app.services.data_providers.twelvedata_provider import TwelveDataProvider
        return TwelveDataProvider(TWELVEDATA_API_KEY)
    if selected == "POLYGON" and POLYGON_API_KEY:
        from app.services.data_providers.polygon_provider import PolygonProvider
        return PolygonProvider(POLYGON_API_KEY)
    from app.services.data_providers.mock_provider import MockProvider
    reason = "missing API key" if selected in {"TWELVEDATA", "POLYGON"} else f"unsupported DATA_PROVIDER={selected}"
    logger.warning("Using mock market data provider (%s)", reason)
    return MockProvider()
