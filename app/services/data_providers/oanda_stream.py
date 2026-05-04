"""
OANDA v20 Streaming Client — converts tick stream to 1-minute OHLCV bars
for FX pairs and metals. Requires OANDA practice/live account token.

Covers: 14 FX pairs + XAUUSD + XAGUSD (16 instruments total)
"""
import asyncio
import logging
import os
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional

import httpx
from sqlalchemy import text

logger = logging.getLogger("TradingSystem.OANDAStream")

OANDA_API_KEY = os.getenv("OANDA_API_KEY", "")
OANDA_ACCOUNT_ID = os.getenv("OANDA_ACCOUNT_ID", "")
OANDA_ENV = os.getenv("OANDA_ENV", "practice")  # "practice" or "live"

OANDA_STREAM_URL = {
    "practice": "https://stream-fxpractice.oanda.com",
    "live": "https://stream-fxtrade.oanda.com",
}
OANDA_REST_URL = {
    "practice": "https://api-fxpractice.oanda.com",
    "live": "https://api-fxtrade.oanda.com",
}

# Internal symbol → OANDA instrument
OANDA_MAP = {
    "EURUSD": "EUR_USD", "GBPUSD": "GBP_USD", "USDJPY": "USD_JPY",
    "USDCHF": "USD_CHF", "AUDUSD": "AUD_USD", "USDCAD": "USD_CAD",
    "NZDUSD": "NZD_USD", "EURGBP": "EUR_GBP", "EURJPY": "EUR_JPY",
    "GBPJPY": "GBP_JPY", "AUDJPY": "AUD_JPY", "GBPCAD": "GBP_CAD",
    "CADJPY": "CAD_JPY", "NZDJPY": "NZD_JPY",
    "XAUUSD": "XAU_USD", "XAGUSD": "XAG_USD",
}

# Reverse lookup
_OANDA_REVERSE = {v: k for k, v in OANDA_MAP.items()}


class OANDATickAggregator:
    """Aggregates OANDA ticks into 1-minute OHLCV bars."""

    def __init__(self):
        self._current_bars: Dict[str, Dict] = {}  # symbol → building bar
        self._current_minute: Dict[str, datetime] = {}

    def process_tick(self, symbol: str, price: float, time: datetime) -> Optional[Dict]:
        """Process a tick. Returns completed bar dict when minute rolls over, else None."""
        bar_minute = time.replace(second=0, microsecond=0)

        if symbol not in self._current_bars or self._current_minute.get(symbol) != bar_minute:
            # New minute — emit previous bar if exists
            completed = None
            if symbol in self._current_bars:
                completed = self._current_bars[symbol].copy()

            # Start new bar
            self._current_bars[symbol] = {
                "time": bar_minute.strftime("%Y-%m-%d %H:%M:%S"),
                "symbol": symbol,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "volume": 1,
            }
            self._current_minute[symbol] = bar_minute
            return completed

        # Update current bar
        bar = self._current_bars[symbol]
        bar["high"] = max(bar["high"], price)
        bar["low"] = min(bar["low"], price)
        bar["close"] = price
        bar["volume"] += 1
        return None


class OANDAStream:
    """Streams OANDA pricing data and writes 1-min OHLCV bars."""

    def __init__(self, db_session_factory):
        self._db_factory = db_session_factory
        self._running = False
        self._aggregator = OANDATickAggregator()
        self._bars_written = 0

    async def run(self) -> None:
        """Connect to OANDA streaming API and process ticks."""
        if not OANDA_API_KEY or not OANDA_ACCOUNT_ID:
            logger.info("OANDA stream disabled (no API key or account ID)")
            return

        self._running = True
        instruments = ",".join(OANDA_MAP.values())
        base_url = OANDA_STREAM_URL.get(OANDA_ENV, OANDA_STREAM_URL["practice"])
        url = f"{base_url}/v3/accounts/{OANDA_ACCOUNT_ID}/pricing/stream"

        logger.info(f"OANDA stream starting | {len(OANDA_MAP)} instruments")

        while self._running:
            try:
                async with httpx.AsyncClient(timeout=None) as client:
                    async with client.stream(
                        "GET", url,
                        params={"instruments": instruments},
                        headers={"Authorization": f"Bearer {OANDA_API_KEY}"},
                    ) as resp:
                        async for line in resp.aiter_lines():
                            if not self._running:
                                break
                            await self._process_line(line)
            except Exception as e:
                logger.warning(f"OANDA stream error, reconnecting in 5s: {e}")
                await asyncio.sleep(5)

    async def _process_line(self, line: str) -> None:
        """Parse OANDA streaming JSON line."""
        import orjson
        try:
            data = orjson.loads(line)
        except Exception:
            return

        if data.get("type") != "PRICE":
            return

        instrument = data.get("instrument", "")
        internal_sym = _OANDA_REVERSE.get(instrument)
        if not internal_sym:
            return

        # Use mid price (average of bid and ask)
        bids = data.get("bids", [])
        asks = data.get("asks", [])
        if not bids or not asks:
            return

        bid = float(bids[0].get("price", 0))
        ask = float(asks[0].get("price", 0))
        mid = (bid + ask) / 2

        time_str = data.get("time", "")
        try:
            tick_time = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
        except Exception:
            tick_time = datetime.now(timezone.utc)

        # Aggregate into 1-min bar
        completed = self._aggregator.process_tick(internal_sym, mid, tick_time)
        if completed:
            self._write_bar(completed)

    def _write_bar(self, bar: Dict) -> None:
        """Write completed 1-min bar to PostgreSQL."""
        db = self._db_factory()
        try:
            db.execute(
                text("""
                    INSERT INTO ohlcv_1m (time, symbol, open, high, low, close, volume)
                    VALUES (:time, :symbol, :open, :high, :low, :close, :volume)
                    ON CONFLICT (time, symbol) DO UPDATE SET
                        high = GREATEST(ohlcv_1m.high, EXCLUDED.high),
                        low = LEAST(ohlcv_1m.low, EXCLUDED.low),
                        close = EXCLUDED.close,
                        volume = EXCLUDED.volume
                """),
                bar,
            )
            db.commit()
            self._bars_written += 1
        except Exception as e:
            db.rollback()
            logger.debug(f"OANDA bar write failed: {e}")
        finally:
            db.close()

    def stop(self) -> None:
        self._running = False

    @property
    def stats(self) -> Dict:
        return {"bars_written": self._bars_written, "instruments": len(OANDA_MAP)}
