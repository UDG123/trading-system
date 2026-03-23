"""
OHLCV Ingester — Fetches and stores historical OHLCV data from
TwelveData and Binance (crypto) into ohlcv_1m.
"""
import os
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, List

import httpx
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.config import DESKS

logger = logging.getLogger("TradingSystem.OHLCVIngester")

TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY", "")

# TwelveData symbol mapping
TD_MAP = {
    "EURUSD": "EUR/USD", "USDJPY": "USD/JPY", "GBPUSD": "GBP/USD",
    "USDCHF": "USD/CHF", "AUDUSD": "AUD/USD", "USDCAD": "USD/CAD",
    "NZDUSD": "NZD/USD", "EURJPY": "EUR/JPY", "GBPJPY": "GBP/JPY",
    "AUDJPY": "AUD/JPY", "EURGBP": "EUR/GBP", "EURAUD": "EUR/AUD",
    "GBPAUD": "GBP/AUD", "EURCHF": "EUR/CHF", "CADJPY": "CAD/JPY",
    "NZDJPY": "NZD/JPY", "GBPCAD": "GBP/CAD", "AUDCAD": "AUD/CAD",
    "AUDNZD": "AUD/NZD", "CHFJPY": "CHF/JPY", "EURNZD": "EUR/NZD",
    "GBPNZD": "GBP/NZD", "GBPCHF": "GBP/CHF", "AUDCHF": "AUD/CHF",
    "NZDCHF": "NZD/CHF", "CADCHF": "CAD/CHF", "NZDCAD": "NZD/CAD",
    "EURCAD": "EUR/CAD",
    "XAUUSD": "XAU/USD", "XAGUSD": "XAG/USD",
    "WTIUSD": "WTI/USD", "US30": "DIA", "US100": "QQQ",
    "NAS100": "QQQ", "GER40": "DAX", "UK100": "FTSE",
    "JPN225": "N225",
}

CRYPTO_SYMBOLS = {"BTCUSD", "ETHUSD", "SOLUSD", "XRPUSD", "LINKUSD"}
EQUITY_SYMBOLS = {"TSLA", "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "NFLX", "AMD"}

BINANCE_MAP = {
    "BTCUSD": "BTCUSDT", "ETHUSD": "ETHUSDT",
    "SOLUSD": "SOLUSDT", "XRPUSD": "XRPUSDT", "LINKUSD": "LINKUSDT",
}


class OHLCVIngester:
    """Fetches and stores OHLCV data from multiple providers."""

    def __init__(self):
        self.client = httpx.AsyncClient(timeout=15.0)

    async def close(self):
        await self.client.aclose()

    async def ingest_symbol(self, db: Session, symbol: str, days_back: int = 30) -> int:
        """Fetch and store 1-min OHLCV data for a symbol. Returns bars inserted."""
        end = datetime.now(timezone.utc)
        start = end - timedelta(days=days_back)

        if symbol in CRYPTO_SYMBOLS:
            bars = await self._fetch_binance_klines(symbol, "1m", start, end)
        elif symbol in EQUITY_SYMBOLS:
            bars = await self._fetch_twelvedata(symbol, "1min", start, end)
        else:
            bars = await self._fetch_twelvedata(symbol, "1min", start, end)

        if not bars:
            return 0

        return self._upsert_bars(db, symbol, bars)

    async def ingest_all_symbols(self, db: Session, days_back: int = 30) -> Dict:
        """Ingest all symbols from DESKS config."""
        all_symbols = set()
        for desk in DESKS.values():
            all_symbols.update(desk.get("symbols", []))

        results = {}
        for symbol in sorted(all_symbols):
            try:
                count = await self.ingest_symbol(db, symbol, days_back)
                results[symbol] = count
                # Rate limiting
                await asyncio.sleep(1.0)
            except Exception as e:
                logger.debug(f"Ingest failed for {symbol}: {e}")
                results[symbol] = 0

        db.commit()
        total = sum(results.values())
        logger.info(f"OHLCV ingest complete: {total} bars across {len(results)} symbols")
        return results

    async def backfill(
        self, db: Session, symbol: str, start_date: datetime, end_date: datetime
    ) -> int:
        """Backfill historical data in chunks."""
        total = 0
        current = start_date
        chunk_days = 3  # ~4320 bars per chunk

        while current < end_date:
            chunk_end = min(current + timedelta(days=chunk_days), end_date)
            try:
                if symbol in CRYPTO_SYMBOLS:
                    bars = await self._fetch_binance_klines(symbol, "1m", current, chunk_end)
                else:
                    bars = await self._fetch_twelvedata(symbol, "1min", current, chunk_end)

                if bars:
                    total += self._upsert_bars(db, symbol, bars)
                    db.commit()

                await asyncio.sleep(1.5)  # Rate limit
            except Exception as e:
                logger.debug(f"Backfill chunk failed for {symbol}: {e}")

            current = chunk_end

        return total

    async def _fetch_twelvedata(
        self, symbol: str, interval: str, start: datetime, end: datetime
    ) -> List[Dict]:
        """Fetch from TwelveData time_series endpoint."""
        if not TWELVEDATA_API_KEY:
            return []

        td_sym = TD_MAP.get(symbol, symbol)
        bars = []

        try:
            resp = await self.client.get(
                "https://api.twelvedata.com/time_series",
                params={
                    "symbol": td_sym,
                    "interval": interval,
                    "start_date": start.strftime("%Y-%m-%d %H:%M:%S"),
                    "end_date": end.strftime("%Y-%m-%d %H:%M:%S"),
                    "outputsize": 5000,
                    "apikey": TWELVEDATA_API_KEY,
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                for v in reversed(data.get("values", [])):
                    bars.append({
                        "time": v.get("datetime"),
                        "open": float(v.get("open", 0)),
                        "high": float(v.get("high", 0)),
                        "low": float(v.get("low", 0)),
                        "close": float(v.get("close", 0)),
                        "volume": float(v.get("volume", 0)),
                    })
        except Exception as e:
            logger.debug(f"TwelveData fetch failed for {symbol}: {e}")

        return bars

    async def _fetch_binance_klines(
        self, symbol: str, interval: str, start: datetime, end: datetime
    ) -> List[Dict]:
        """Fetch from Binance klines endpoint."""
        binance_sym = BINANCE_MAP.get(symbol)
        if not binance_sym:
            return []

        bars = []
        start_ms = int(start.timestamp() * 1000)
        end_ms = int(end.timestamp() * 1000)

        try:
            resp = await self.client.get(
                "https://api.binance.com/api/v3/klines",
                params={
                    "symbol": binance_sym,
                    "interval": interval,
                    "startTime": start_ms,
                    "endTime": end_ms,
                    "limit": 1000,
                },
            )
            if resp.status_code == 200:
                for k in resp.json():
                    bars.append({
                        "time": datetime.fromtimestamp(k[0] / 1000, tz=timezone.utc).strftime(
                            "%Y-%m-%d %H:%M:%S"
                        ),
                        "open": float(k[1]),
                        "high": float(k[2]),
                        "low": float(k[3]),
                        "close": float(k[4]),
                        "volume": float(k[5]),
                    })
        except Exception as e:
            logger.debug(f"Binance fetch failed for {symbol}: {e}")

        return bars

    def _upsert_bars(self, db: Session, symbol: str, bars: List[Dict]) -> int:
        """Insert bars into ohlcv_1m with ON CONFLICT DO NOTHING."""
        if not bars:
            return 0

        inserted = 0
        for bar in bars:
            try:
                result = db.execute(
                    text("""
                        INSERT INTO ohlcv_1m (time, symbol, open, high, low, close, volume)
                        VALUES (:time, :symbol, :open, :high, :low, :close, :volume)
                        ON CONFLICT (time, symbol) DO NOTHING
                    """),
                    {
                        "time": bar["time"],
                        "symbol": symbol,
                        "open": bar["open"],
                        "high": bar["high"],
                        "low": bar["low"],
                        "close": bar["close"],
                        "volume": bar["volume"],
                    },
                )
                if result.rowcount > 0:
                    inserted += 1
            except Exception:
                pass

        return inserted

    async def continuous_feed(self, db_session_factory, interval_seconds: int = 60) -> None:
        """
        Background task: fetch latest 1-min bar for all symbols periodically.
        Uses TwelveData batched price endpoint.
        """
        all_symbols = set()
        for desk in DESKS.values():
            all_symbols.update(desk.get("symbols", []))

        while True:
            try:
                db = db_session_factory()
                try:
                    for symbol in all_symbols:
                        try:
                            if symbol in CRYPTO_SYMBOLS:
                                bars = await self._fetch_binance_klines(
                                    symbol, "1m",
                                    datetime.now(timezone.utc) - timedelta(minutes=2),
                                    datetime.now(timezone.utc),
                                )
                            else:
                                bars = await self._fetch_twelvedata(
                                    symbol, "1min",
                                    datetime.now(timezone.utc) - timedelta(minutes=2),
                                    datetime.now(timezone.utc),
                                )

                            if bars:
                                self._upsert_bars(db, symbol, bars)
                        except Exception:
                            pass

                    db.commit()
                finally:
                    db.close()

            except Exception as e:
                logger.debug(f"Continuous feed error: {e}")

            await asyncio.sleep(interval_seconds)
