"""
Signal Deduplication Filter
Prevents duplicate signals using Redis SET with TTL.
Shared key space with webhook dedup — catches dupes from both sources.
"""
import hashlib
import logging
from typing import Dict

logger = logging.getLogger("TradingSystem.SignalEngine.Dedup")

DEDUP_PREFIX = "dedup:"
DEDUP_TTL_SECONDS = 60


class DedupFilter:
    """Redis-based deduplication matching webhook.py's pattern."""

    def __init__(self, redis_pool):
        self.redis = redis_pool

    async def is_duplicate(self, signal: Dict) -> bool:
        """Check if signal is a duplicate. Returns True if already seen."""
        key_parts = (
            str(signal.get("symbol_normalized", "")),
            str(signal.get("alert_type", "")),
            str(signal.get("timeframe", "")),
            str(signal.get("direction", "")),
        )
        hash_val = hashlib.sha256("|".join(key_parts).encode()).hexdigest()[:16]
        dedup_key = f"{DEDUP_PREFIX}{hash_val}"

        is_new = await self.redis.set(dedup_key, b"1", ex=DEDUP_TTL_SECONDS, nx=True)
        if not is_new:
            logger.debug(
                f"DEDUP HIT | {signal.get('symbol_normalized')} "
                f"{signal.get('alert_type')} {signal.get('direction')}"
            )
            return True
        return False
