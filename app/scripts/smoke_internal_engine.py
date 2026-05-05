"""Smoke test for the self-contained internal signal engine.

Runs without TradingView, broker execution, Redis, or provider API keys.
"""
from __future__ import annotations

import asyncio
import json
import os

from app.config import ENABLE_TRADINGVIEW_WEBHOOK
from app.services.data_providers.mock_provider import MockProvider
from app.workers.scanner_worker import InternalScannerWorker

REQUIRED_PAYLOAD_FIELDS = {
    "symbol", "symbol_normalized", "timeframe", "alert_type", "direction", "price",
    "desks_matched", "source", "strategy_id", "desk_mode", "desk_role", "strategy_mode",
    "mode_reason", "quality_hints", "cross_desk_bias", "bias_alignment", "bias_action", "bias_size_mult",
}


class MemoryRedis:
    def __init__(self):
        self.events = []

    async def xadd(self, stream, fields):
        self.events.append((stream, fields))
        return f"0-{len(self.events)}"


async def main() -> int:
    os.environ.setdefault("SIGNAL_SOURCE", "INTERNAL_ENGINE")
    provider = MockProvider()
    redis = MemoryRedis()

    # Keep smoke deterministic even when run outside the normal market-hours window.
    import app.services.signal_engine.desk_scanner as desk_scanner_module
    desk_scanner_module.is_valid_trading_hour = lambda *args, **kwargs: True

    worker = InternalScannerWorker(provider=provider, redis=redis, dedup_cooldown_seconds=0)

    # Prove mock data is available for requested symbols.
    for symbol in ["EURUSD", "GBPUSD", "USDJPY", "XAUUSD"]:
        candles = await provider.get_candles(symbol, "1M", 300)
        assert candles and candles[-1]["provider"] == "mock", f"mock candles missing for {symbol}"

    summary = await worker.run_cycle(publish=True, max_symbols=20)
    payloads = [json.loads(fields["payload"]) for stream, fields in redis.events if stream == "oniquant_alerts"]
    print("summary=", summary.as_dict())
    print("published=", len(payloads))

    if not payloads:
        print("No candidate generated. This can happen if indicator stacks find no setup; Gold mode should usually emit with mock XAUUSD data.")
        return 1

    missing = REQUIRED_PAYLOAD_FIELDS - set(payloads[0])
    assert not missing, f"payload missing required fields: {sorted(missing)}"

    gold_payloads = [p for p in payloads if p.get("desk_mode") in {"GOLD_SCALP", "GOLD_INTRADAY", "GOLD_SWING"}]
    assert gold_payloads, "expected at least one Gold mode candidate"
    assert all(p["symbol"] == "XAUUSD" for p in gold_payloads), "Gold mode emitted non-XAUUSD candidate"
    assert ENABLE_TRADINGVIEW_WEBHOOK is False, "TradingView should be disabled by default"

    print("first_payload=", json.dumps(payloads[0], indent=2, default=str))
    print("TradingView required: false")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
