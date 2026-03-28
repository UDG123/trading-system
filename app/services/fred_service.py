"""
FRED Service — Fetches daily DXY and VIX from the Federal Reserve FRED API.
Used for intermarket regime context. Cached in Redis with 4-hour TTL.
"""
import os
import logging
from typing import Optional

import httpx

logger = logging.getLogger("TradingSystem.FREDService")

FRED_API_KEY = os.getenv("FRED_API_KEY", "")
FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
REDIS_TTL = 14400  # 4 hours

# FRED series IDs
VIX_SERIES = "VIXCLS"
DXY_SERIES = "DTWEXBGS"


class FREDService:
    """Lightweight FRED API client for daily VIX and DXY."""

    def __init__(self, redis_pool=None):
        self.client = httpx.AsyncClient(timeout=10.0)
        self.redis = redis_pool

    async def close(self) -> None:
        await self.client.aclose()

    async def get_vix(self) -> Optional[float]:
        """Get latest VIX close from FRED (cached 4h in Redis)."""
        return await self._get_series(VIX_SERIES, "fred:vix")

    async def get_dxy(self) -> Optional[float]:
        """Get latest DXY value from FRED (cached 4h in Redis)."""
        return await self._get_series(DXY_SERIES, "fred:dxy")

    async def _get_series(self, series_id: str, cache_key: str) -> Optional[float]:
        """Fetch a FRED series, using Redis cache if available."""
        # Check cache first
        if self.redis:
            try:
                cached = await self.redis.get(cache_key)
                if cached is not None:
                    return float(cached)
            except Exception:
                pass

        # Fetch from FRED
        if not FRED_API_KEY:
            return None

        try:
            resp = await self.client.get(
                FRED_BASE_URL,
                params={
                    "series_id": series_id,
                    "api_key": FRED_API_KEY,
                    "file_type": "json",
                    "sort_order": "desc",
                    "limit": 5,  # Get a few in case latest is "."
                },
            )
            if resp.status_code != 200:
                return None

            data = resp.json()
            observations = data.get("observations", [])
            for obs in observations:
                value = obs.get("value", ".")
                if value != ".":
                    result = float(value)
                    # Cache in Redis
                    if self.redis:
                        try:
                            await self.redis.set(cache_key, str(result), ex=REDIS_TTL)
                        except Exception:
                            pass
                    logger.debug(f"FRED {series_id} = {result}")
                    return result

        except Exception as e:
            logger.debug(f"FRED fetch failed for {series_id}: {e}")

        return None
