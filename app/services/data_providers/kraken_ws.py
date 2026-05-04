"""
Kraken WebSocket Client — streams 1-minute OHLC candles for crypto pairs.
Uses ccxt for WebSocket connection. Writes completed candles to ohlcv_1m.

Pairs: BTC/USD, ETH/USD, SOL/USD, XRP/USD, LINK/USD
"""
import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Optional

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger("TradingSystem.KrakenWS")

# Internal symbol → Kraken WebSocket symbol
KRAKEN_MAP = {
    "BTCUSD": "XBT/USD",
    "ETHUSD": "ETH/USD",
    "SOLUSD": "SOL/USD",
    "XRPUSD": "XRP/USD",
    "LINKUSD": "LINK/USD",
}

KRAKEN_WS_URL = "wss://ws.kraken.com/v2"


class KrakenOHLCStream:
    """Streams 1-minute OHLC candles from Kraken WebSocket (no auth needed)."""

    def __init__(self, db_session_factory):
        self._db_factory = db_session_factory
        self._running = False
        self._bars_written = 0

    async def run(self) -> None:
        """Connect to Kraken WS and stream OHLC candles indefinitely."""
        self._running = True
        symbols = list(KRAKEN_MAP.values())

        logger.info(f"Kraken WS starting | {len(symbols)} pairs")

        while self._running:
            try:
                await self._connect_and_stream(symbols)
            except Exception as e:
                logger.warning(f"Kraken WS error, reconnecting in 5s: {e}")
                await asyncio.sleep(5)

    async def _connect_and_stream(self, symbols: list) -> None:
        """Single WebSocket connection lifecycle."""
        import websockets
        import orjson

        async with websockets.connect(KRAKEN_WS_URL) as ws:
            # Subscribe to OHLC channel (1-minute interval)
            subscribe_msg = {
                "method": "subscribe",
                "params": {
                    "channel": "ohlc",
                    "symbol": symbols,
                    "interval": 1,  # 1 minute
                },
            }
            await ws.send(orjson.dumps(subscribe_msg).decode())
            logger.info(f"Kraken WS subscribed to {len(symbols)} OHLC streams")

            async for raw_msg in ws:
                if not self._running:
                    break

                try:
                    msg = orjson.loads(raw_msg)
                except Exception:
                    continue

                # Skip system/subscription messages
                if msg.get("channel") != "ohlc":
                    continue

                data = msg.get("data", [])
                for candle in data:
                    await self._process_candle(candle)

    async def _process_candle(self, candle: Dict) -> None:
        """Process a single OHLC candle from Kraken."""
        try:
            kraken_sym = candle.get("symbol", "")
            # Reverse lookup: Kraken symbol → internal symbol
            internal_sym = None
            for k, v in KRAKEN_MAP.items():
                if v == kraken_sym:
                    internal_sym = k
                    break

            if not internal_sym:
                return

            # Kraken v2 OHLC format
            bar_time = candle.get("timestamp")
            open_ = float(candle.get("open", 0))
            high = float(candle.get("high", 0))
            low = float(candle.get("low", 0))
            close = float(candle.get("close", 0))
            volume = float(candle.get("volume", 0))

            if not bar_time or open_ <= 0:
                return

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
                        "time": bar_time,
                        "symbol": internal_sym,
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
                logger.debug(f"Kraken bar write failed: {e}")
            finally:
                db.close()

        except Exception as e:
            logger.debug(f"Kraken candle process error: {e}")

    def stop(self) -> None:
        self._running = False

    @property
    def stats(self) -> Dict:
        return {"bars_written": self._bars_written, "pairs": len(KRAKEN_MAP)}
