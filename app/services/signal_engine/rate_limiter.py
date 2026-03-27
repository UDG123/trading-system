"""
TwelveData API Credit Tracker
Grow plan: 800 credits/day, 55 requests/minute.
Tracks usage and prevents exceeding limits.
"""
import time
import logging
from collections import deque

logger = logging.getLogger("TradingSystem.SignalEngine.RateLimiter")


class RateLimiter:
    """Track TwelveData API credit usage and enforce rate limits."""

    def __init__(self, daily_limit: int = 800, per_minute_limit: int = 55):
        self.daily_limit = daily_limit
        self.per_minute_limit = per_minute_limit
        self._minute_window: deque = deque()  # timestamps of recent requests
        self._daily_count = 0
        self._daily_reset_ts = 0.0  # epoch when daily counter resets

    def _reset_daily_if_needed(self) -> None:
        now = time.time()
        # Reset at midnight UTC (86400-second windows)
        if now - self._daily_reset_ts > 86400:
            self._daily_count = 0
            self._daily_reset_ts = now
            logger.info("Rate limiter daily counter reset")

    def _prune_minute_window(self) -> None:
        cutoff = time.time() - 60.0
        while self._minute_window and self._minute_window[0] < cutoff:
            self._minute_window.popleft()

    def can_request(self) -> bool:
        """Check if we can make another API request."""
        self._reset_daily_if_needed()
        self._prune_minute_window()

        if self._daily_count >= self.daily_limit:
            return False
        if len(self._minute_window) >= self.per_minute_limit:
            return False
        return True

    def record_request(self, credits: int = 1) -> None:
        """Record that an API request was made."""
        self._reset_daily_if_needed()
        self._minute_window.append(time.time())
        self._daily_count += credits

    @property
    def daily_remaining(self) -> int:
        self._reset_daily_if_needed()
        return max(0, self.daily_limit - self._daily_count)

    @property
    def minute_remaining(self) -> int:
        self._prune_minute_window()
        return max(0, self.per_minute_limit - len(self._minute_window))

    def seconds_until_minute_slot(self) -> float:
        """Seconds to wait until a per-minute slot opens."""
        self._prune_minute_window()
        if len(self._minute_window) < self.per_minute_limit:
            return 0.0
        oldest = self._minute_window[0]
        return max(0.0, 60.0 - (time.time() - oldest))
