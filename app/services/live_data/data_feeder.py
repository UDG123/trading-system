from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from typing import Protocol, Optional

logger = logging.getLogger("TradingSystem.LiveData.Feeder")


class ProviderAdapter(Protocol):
    name: str
    async def connect(self) -> None: ...
    async def subscribe(self, symbols: list[str]) -> None: ...
    async def recv(self) -> dict: ...
    async def backfill(self, symbol: str, timeframe: str, limit: int = 500) -> list[dict]: ...


@dataclass
class FeedHealth:
    last_message_ts: float = 0.0
    heartbeat_sec: int = 15
    stale_after_sec: int = 45

    def is_stale(self) -> bool:
        return self.last_message_ts > 0 and (time.time() - self.last_message_ts) > self.stale_after_sec


class DataFeeder:
    """Provider-agnostic live feeder skeleton (websocket-first)."""

    def __init__(self, adapter: ProviderAdapter):
        self.adapter = adapter
        self.health = FeedHealth()
        self._running = False

    async def run(self, symbols: list[str], on_message):
        self._running = True
        backoff = 1
        while self._running:
            try:
                await self.adapter.connect()
                await self.adapter.subscribe(symbols)
                backoff = 1
                while self._running:
                    msg = await self.adapter.recv()
                    self.health.last_message_ts = time.time()
                    await on_message(msg)
                    if self.health.is_stale():
                        raise ConnectionError("stale feed")
            except Exception as e:
                logger.warning("Feeder reconnect for %s: %s", getattr(self.adapter, "name", "adapter"), e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30)

    def stop(self):
        self._running = False

    async def historical_recovery(self, symbol: str, timeframe: str, limit: int = 500) -> list[dict]:
        # TODO: wire adapters for TwelveData, Polygon, Bybit; optional Binance/Coinbase/Kraken.
        # REST is recovery/backfill only; live should be websocket stream.
        return await self.adapter.backfill(symbol, timeframe, limit)
