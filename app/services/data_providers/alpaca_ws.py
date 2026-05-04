"""
Alpaca WebSocket Client — streams 1-minute bars for US equities.
Uses Alpaca's market data WebSocket (free tier with account).

Symbols: NVDA, AAPL, TSLA, MSFT, AMZN, META, GOOGL, NFLX, AMD
"""
import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Dict

from sqlalchemy import text

logger = logging.getLogger("TradingSystem.AlpacaWS")

ALPACA_API_KEY = os.getenv("ALPACA_API_KEY", "")
ALPACA_SECRET_KEY = os.getenv("ALPACA_SECRET_KEY", "")
ALPACA_DATA_URL = os.getenv("ALPACA_DATA_URL", "wss://stream.data.alpaca.markets/v2/iex")

EQUITY_SYMBOLS = ["NVDA", "AAPL", "TSLA", "MSFT", "AMZN", "META", "GOOGL", "NFLX", "AMD"]


class AlpacaBarStream:
    """Streams 1-minute bars from Alpaca market data WebSocket."""

    def __init__(self, db_session_factory):
        self._db_factory = db_session_factory
        self._running = False
        self._bars_written = 0

    async def run(self) -> None:
        """Connect and stream bars."""
        if not ALPACA_API_KEY or not ALPACA_SECRET_KEY:
            logger.info("Alpaca WS disabled (no API key)")
            return

        self._running = True
        logger.info(f"Alpaca WS starting | {len(EQUITY_SYMBOLS)} equities")

        while self._running:
            try:
                await self._connect_and_stream()
            except Exception as e:
                logger.warning(f"Alpaca WS error, reconnecting in 5s: {e}")
                await asyncio.sleep(5)

    async def _connect_and_stream(self) -> None:
        """Single WebSocket connection lifecycle."""
        import websockets
        import orjson

        async with websockets.connect(ALPACA_DATA_URL) as ws:
            # Authenticate
            auth_msg = {
                "action": "auth",
                "key": ALPACA_API_KEY,
                "secret": ALPACA_SECRET_KEY,
            }
            await ws.send(orjson.dumps(auth_msg).decode())

            # Wait for auth response
            auth_resp = orjson.loads(await ws.recv())
            logger.debug(f"Alpaca auth response: {auth_resp}")

            # Subscribe to 1-minute bars
            sub_msg = {
                "action": "subscribe",
                "bars": EQUITY_SYMBOLS,
            }
            await ws.send(orjson.dumps(sub_msg).decode())
            logger.info(f"Alpaca WS subscribed to {len(EQUITY_SYMBOLS)} bar streams")

            async for raw_msg in ws:
                if not self._running:
                    break

                try:
                    messages = orjson.loads(raw_msg)
                except Exception:
                    continue

                if not isinstance(messages, list):
                    continue

                for msg in messages:
                    if msg.get("T") == "b":  # bar message
                        self._process_bar(msg)

    def _process_bar(self, bar: Dict) -> None:
        """Process a single Alpaca bar message."""
        try:
            symbol = bar.get("S", "")  # Ticker symbol
            if symbol not in EQUITY_SYMBOLS:
                return

            # Alpaca bar format
            bar_time = bar.get("t", "")  # ISO timestamp
            open_ = float(bar.get("o", 0))
            high = float(bar.get("h", 0))
            low = float(bar.get("l", 0))
            close = float(bar.get("c", 0))
            volume = float(bar.get("v", 0))

            if not bar_time or open_ <= 0:
                return

            # Convert timestamp
            try:
                dt = datetime.fromisoformat(bar_time.replace("Z", "+00:00"))
                time_str = dt.strftime("%Y-%m-%d %H:%M:%S")
            except Exception:
                time_str = bar_time

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
                    {
                        "time": time_str,
                        "symbol": symbol,
                        "open": open_,
                        "high": high,
                        "low": low,
                        "close": close,
                        "volume": volume,
                    },
                )
                db.commit()
                self._bars_written += 1
            except Exception as e:
                db.rollback()
                logger.debug(f"Alpaca bar write failed: {e}")
            finally:
                db.close()

        except Exception as e:
            logger.debug(f"Alpaca bar process error: {e}")

    def stop(self) -> None:
        self._running = False

    @property
    def stats(self) -> Dict:
        return {"bars_written": self._bars_written, "symbols": len(EQUITY_SYMBOLS)}
