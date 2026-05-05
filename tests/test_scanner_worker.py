import asyncio
import json

from app.services.data_providers.mock_provider import MockProvider
from app.workers.scanner_worker import InternalScannerWorker


class MemoryRedis:
    def __init__(self):
        self.events = []

    async def xadd(self, stream, fields):
        self.events.append((stream, fields))
        return "1-0"


def test_scanner_worker_emits_payloads(monkeypatch):
    async def run():
        import app.services.signal_engine.desk_scanner as desk_scanner_module
        monkeypatch.setattr(desk_scanner_module, "is_valid_trading_hour", lambda *args, **kwargs: True)
        redis = MemoryRedis()
        worker = InternalScannerWorker(provider=MockProvider(), redis=redis, dedup_cooldown_seconds=0)
        summary = await worker.run_cycle(max_symbols=16)
        assert summary.scanned_symbols > 0
        assert redis.events
        stream, fields = redis.events[0]
        assert stream == "oniquant_alerts"
        payload = json.loads(fields["payload"])
        assert payload["source"] == "internal_engine"
        assert payload["symbol"]
        assert "desks_matched" in payload
        for key in ["desk_mode", "desk_role", "strategy_mode", "mode_reason", "quality_hints", "cross_desk_bias", "bias_alignment", "bias_action", "bias_size_mult"]:
            assert key in payload
    asyncio.run(run())


def test_scanner_dedup_cooldown(monkeypatch):
    async def run():
        import app.services.signal_engine.desk_scanner as desk_scanner_module
        monkeypatch.setattr(desk_scanner_module, "is_valid_trading_hour", lambda *args, **kwargs: True)
        redis = MemoryRedis()
        worker = InternalScannerWorker(provider=MockProvider(), redis=redis, dedup_cooldown_seconds=900)
        first = await worker.run_cycle(max_symbols=16)
        second = await worker.run_cycle(max_symbols=16)
        assert first.candidates_emitted >= 1
        assert second.candidates_emitted == 0
    asyncio.run(run())
