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
from datetime import datetime
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
from app.core.event_bus import EventBus
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
    blocked_candidates: int = 0
    approved_signals: int = 0
    skipped_due_to_data: int = 0
    errors: int = 0

    def as_dict(self) -> dict:
        return {
            "scanned_symbols": self.scanned_symbols,
            "candidates_emitted": self.candidates_emitted,
            "blocked_candidates": self.blocked_candidates,
            "approved_signals": self.approved_signals,
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
        self.event_bus = EventBus(redis_client=redis)

    async def start(self) -> None:
        if not WORKER_SCANNER_ENABLED:
            logger.info("Internal scanner worker disabled by WORKER_SCANNER_ENABLED=false")
            return
        if self.redis is None:
            self.redis = aioredis.from_url(REDIS_URL, decode_responses=False)
        self.event_bus = EventBus(redis_client=self.redis)
        logger.info("Internal scanner started | provider=%s | interval=%ss", self.provider.provider_name, INTERNAL_SCANNER_INTERVAL_SECONDS)
        while not _shutdown.is_set():
            started = time.monotonic()
            summary = await self.run_cycle()
            logger.info("desk_scan_summary | %s", summary.as_dict())
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
            skip_reasons = {}
            for symbol in symbols:
                if scanned_this_cycle >= limit:
                    break
                scanned_this_cycle += 1
                summary.scanned_symbols += 1
                try:
                    candles = await self.provider.get_candles(symbol, entry_tf, limit=300)
                    if not candles or len(candles) < 10:
                        summary.skipped_due_to_data += 1
                        skip_reasons[symbol] = "insufficient_bars"
                        await self.event_bus.emit_pipeline_health({"event_type":"pipeline.health","desk":desk_id,"symbol":symbol,"timestamp":time.time(),"reason":"insufficient_bars"})
                        continue
                    loaded = self.candle_manager.update_dataframe(symbol, entry_tf, candles)
                    if loaded <= 0:
                        summary.skipped_due_to_data += 1
                        skip_reasons[symbol] = "insufficient_bars"
                        continue
                    loaded_for_desk += 1
                except Exception as exc:
                    summary.errors += 1
                    skip_reasons[symbol] = f"load_error:{exc}"
                    logger.warning("Scanner data load failed | %s %s %s | %s", desk_id, symbol, entry_tf, exc)
            if not loaded_for_desk:
                logger.info("desk_scan_summary | %s", {"desk": desk_id, "symbols_configured": len(symbols), "symbols_ready": 0, "skip_reasons": skip_reasons})
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
                    candidate_id = f"{candidate.get('desk_id')}:{candidate.get('symbol')}:{candidate.get('timeframe')}:{int(time.time()*1000)}"
                    generated = {"event_type":"candidate.generated","candidate_id":candidate_id,"desk":candidate.get("desk_id"),"symbol":candidate.get("symbol"),"side":candidate.get("direction"),"timeframe":candidate.get("timeframe"),"timestamp":datetime.utcnow().isoformat(),"provenance":{"source":"scanner_worker"},"candidate":candidate}
                    await self.event_bus.emit_candidate_generated(generated)
                    summary.candidates_emitted += 1
                    if publish:
                        await publish_signal(self.redis, payload)
                    if payload.get("quality_blocked"):
                        summary.blocked_candidates += 1
                        blocked = {"event_type":"candidate.blocked","candidate_id":candidate_id,"desk":candidate.get("desk_id"),"symbol":candidate.get("symbol"),"side":candidate.get("direction"),"timestamp":datetime.utcnow().isoformat(),"probability":payload.get("signal_probability"),"final_quality":payload.get("final_signal_quality"),"block_reasons":payload.get("quality_block_reasons") or ["quality_gate"],"gate_config_snapshot":{"min_probability":os.getenv("MIN_SIGNAL_PROBABILITY","0.52"),"min_quality":os.getenv("MIN_FINAL_SIGNAL_QUALITY","50")},"candidate_ref":generated}
                        await self.event_bus.emit_candidate_blocked(blocked)
                    else:
                        summary.approved_signals += 1
                        approved={"event_type":"signal.approved","signal_id":payload.get("event_id",candidate_id),"candidate_id":candidate_id,"desk":candidate.get("desk_id"),"symbol":candidate.get("symbol"),"side":candidate.get("direction"),"timestamp":datetime.utcnow().isoformat(),"probability":payload.get("signal_probability"),"final_quality":payload.get("final_signal_quality"),"provenance":{"source":"scanner_worker"}}
                        await self.event_bus.emit_signal_approved(approved)
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
