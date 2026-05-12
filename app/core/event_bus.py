from __future__ import annotations

import logging
from datetime import datetime, timezone

from app.utils.json_safe import dumps_json_safe, to_json_safe

logger = logging.getLogger("TradingSystem.EventBus")


class EventBus:
    def __init__(self, redis_client=None):
        self.redis = redis_client

    async def _xadd(self, stream: str, envelope: dict) -> str | None:
        if not self.redis:
            logger.error("pipeline_emit_failed", extra={"event": "pipeline_emit_failed", "stream": stream, "reason": "redis_unavailable"})
            return None
        payload = to_json_safe(envelope)
        fields = {
            "event_type": payload.get("event_type", "unknown"),
            "desk": payload.get("desk", ""),
            "symbol": payload.get("symbol", ""),
            "timestamp": payload.get("timestamp", datetime.now(timezone.utc).isoformat()),
            "payload_json": dumps_json_safe(payload),
        }
        try:
            mid = await self.redis.xadd(stream, fields)
            logger.info("signal_emitted", extra={"event": "signal_emitted", "stream": stream, "event_type": fields["event_type"]})
            return mid
        except Exception:
            logger.exception("pipeline_emit_failed", extra={"event": "pipeline_emit_failed", "stream": stream, "event_type": fields["event_type"]})
            return None

    async def emit_candidate_generated(self, envelope: dict):
        await self._xadd("candidates:all", envelope)
        await self._xadd(f"candidates:{envelope.get('desk','unknown')}", envelope)

    async def emit_candidate_blocked(self, envelope: dict):
        await self._xadd("signals:blocked", envelope)

    async def emit_signal_approved(self, envelope: dict):
        await self._xadd("signals:approved", envelope)
        await self._xadd(f"signals:{envelope.get('desk','unknown')}", envelope)

    async def emit_pipeline_health(self, envelope: dict):
        await self._xadd("pipeline:health", envelope)
