"""Internal signal scanner worker.

Live/historical provider -> CandleManager -> DeskScanner -> Redis stream
`oniquant_alerts`. TradingView is intentionally not required.
"""
from __future__ import annotations

import asyncio
import logging
import os
import signal
import time
from dataclasses import dataclass
from typing import Optional

import redis.asyncio as aioredis

from app.config import (
    DESKS,
    INTERNAL_SCANNER_INTERVAL_SECONDS,
    INTERNAL_SCANNER_MAX_SYMBOLS_PER_CYCLE,
    INTERNAL_SIGNAL_DEDUP_MINUTES,
    REDIS_URL,
    WORKER_SCANNER_ENABLED,
)
from app.core.redis_bus import publish_signal
from app.services.data_providers.base import MarketDataProvider, get_market_data_provider
from app.services.signal_engine.candle_manager import CandleManager
from app.services.signal_engine.desk_scanner import DeskScanner

logger = logging.getLogger("TradingSystem.ScannerWorker")
logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))

_shutdown = asyncio.Event()


@dataclass
class ScanSummary:
    scanned_symbols: int = 0
    candidates_emitted: int = 0
    skipped_due_to_data: int = 0
    errors: int = 0

    def as_dict(self) -> dict:
        return {
            "scanned_symbols": self.scanned_symbols,
            "candidates_emitted": self.candidates_emitted,
            "skipped_due_to_data": self.skipped_due_to_data,
            "errors": self.errors,
        }


class InternalScannerWorker:
    def __init__(
        self,
        provider: Optional[MarketDataProvider] = None,
        candle_manager: Optional[CandleManager] = None,
        redis=None,
        dedup_cooldown_seconds: Optional[int] = None,
    ):
        self.provider = provider or get_market_data_provider()
        self.candle_manager = candle_manager or CandleManager()
        self.scanner = DeskScanner(self.candle_manager)
        self.redis = redis
        self.dedup_cooldown_seconds = dedup_cooldown_seconds or INTERNAL_SIGNAL_DEDUP_MINUTES * 60
        self._dedup_seen: dict[str, float] = {}

    async def start(self) -> None:
        if not WORKER_SCANNER_ENABLED:
            logger.info("Internal scanner worker disabled by WORKER_SCANNER_ENABLED=false")
            return
        if self.redis is None:
            self.redis = aioredis.from_url(REDIS_URL, decode_responses=False)
        logger.info("Internal scanner started | provider=%s | interval=%ss", self.provider.provider_name, INTERNAL_SCANNER_INTERVAL_SECONDS)
        while not _shutdown.is_set():
            started = time.monotonic()
            summary = await self.run_cycle()
            logger.info("Scanner cycle summary | %s", summary.as_dict())
            elapsed = time.monotonic() - started
            await asyncio.sleep(max(1, INTERNAL_SCANNER_INTERVAL_SECONDS - elapsed))

    async def run_cycle(self, publish: bool = True, max_symbols: Optional[int] = None) -> ScanSummary:
        summary = ScanSummary()
        limit = max_symbols or INTERNAL_SCANNER_MAX_SYMBOLS_PER_CYCLE
        scanned_this_cycle = 0
        for desk_id, desk in DESKS.items():
            if scanned_this_cycle >= limit:
                break
            entry_tf = DeskScanner._get_entry_tf(desk.get("timeframes", {}))
            symbols = desk.get("symbols", [])
            loaded_for_desk = 0
            for symbol in symbols:
                if scanned_this_cycle >= limit:
                    break
                scanned_this_cycle += 1
                summary.scanned_symbols += 1
                try:
                    candles = await self.provider.get_candles(symbol, entry_tf, limit=300)
                    if not candles:
                        summary.skipped_due_to_data += 1
                        continue
                    loaded = self.candle_manager.update_dataframe(symbol, entry_tf, candles)
                    if loaded <= 0:
                        summary.skipped_due_to_data += 1
                        continue
                    loaded_for_desk += 1
                except Exception as exc:
                    summary.errors += 1
                    logger.warning("Scanner data load failed | %s %s %s | %s", desk_id, symbol, entry_tf, exc)
            if not loaded_for_desk:
                continue
            try:
                candidates = self.scanner.scan_desk(desk_id)
            except Exception as exc:
                summary.errors += 1
                logger.warning("Desk scan failed | %s | %s", desk_id, exc, exc_info=True)
                continue
            for candidate in candidates:
                try:
                    if not self._dedup_allowed(candidate):
                        continue
                    payload = self.scanner.build_signal_payload(candidate)
                    payload["source"] = "internal_engine"
                    if publish:
                        await publish_signal(self.redis, payload)
                    summary.candidates_emitted += 1
                except Exception as exc:
                    summary.errors += 1
                    logger.warning("Candidate publish failed | %s | %s", candidate, exc)
        return summary

    def _dedup_allowed(self, candidate: dict) -> bool:
        key = ":".join([
            str(candidate.get("symbol")), str(candidate.get("desk_id")), str(candidate.get("direction")),
            str(candidate.get("strategy", candidate.get("strategy_mode", candidate.get("strategy_id", "unknown")))),
            str(candidate.get("timeframe")),
        ])
        now = time.time()
        last_seen = self._dedup_seen.get(key, 0)
        if now - last_seen < self.dedup_cooldown_seconds:
            return False
        self._dedup_seen[key] = now
        # occasional pruning
        cutoff = now - self.dedup_cooldown_seconds
        self._dedup_seen = {k: v for k, v in self._dedup_seen.items() if v >= cutoff}
        return True

    async def stop(self) -> None:
        if self.redis is not None and hasattr(self.redis, "aclose"):
            await self.redis.aclose()


def _handle_shutdown(*_):
    _shutdown.set()


async def main() -> None:
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _handle_shutdown)
        except NotImplementedError:
            pass
    worker = InternalScannerWorker()
    try:
        await worker.start()
    finally:
        await worker.stop()


if __name__ == "__main__":
    asyncio.run(main())
