"""
OHLCV Candle Manager
Fetches candles from TwelveData, stores in PostgreSQL, maintains rolling
DataFrames in memory for fast indicator computation.
Reuses symbol mappings from ohlcv_ingester.
"""
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

import httpx
import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.config import DESKS
from app.services.ohlcv_ingester import TD_MAP, CRYPTO_SYMBOLS, EQUITY_SYMBOLS, BYBIT_MAP
from app.services.signal_engine.rate_limiter import RateLimiter

logger = logging.getLogger("TradingSystem.SignalEngine.CandleManager")

# TwelveData interval strings
TF_TO_TD_INTERVAL = {
    "1M": "1min", "5M": "5min", "15M": "15min",
    "1H": "1h", "4H": "4h", "D": "1day", "W": "1week",
}

# DB table per timeframe
TF_TO_TABLE = {
    "1M": "ohlcv_1m", "5M": "ohlcv_5m", "15M": "ohlcv_15m",
    "1H": "ohlcv_1h", "4H": "ohlcv_4h", "D": "ohlcv_1d", "W": "ohlcv_1w",
}

# Rolling window sizes (bars kept in memory)
LOOKBACK_BARS = {
    "1M": 2000,  # ~33 hours — expanded for free data providers
    "5M": 2000,  # ~7 days
    "15M": 2000, # ~21 days
    "1H": 2000,  # ~83 days
    "4H": 500,   # ~83 days
    "D": 500,    # ~2 years
    "W": 104,    # ~2 years
}


class CandleManager:
    """Manages OHLCV data for signal generation."""

    def __init__(self, db_session_factory, rate_limiter: RateLimiter):
        self._db_factory = db_session_factory
        self._rate_limiter = rate_limiter
        self._client = httpx.AsyncClient(timeout=15.0)
        self._api_key: Optional[str] = None

        # In-memory cache: {(symbol, timeframe): DataFrame}
        self._frames: Dict[tuple, pd.DataFrame] = {}
        # Track last fetched timestamp per symbol-TF
        self._last_fetch: Dict[tuple, datetime] = {}

    async def close(self) -> None:
        await self._client.aclose()

    def _get_api_key(self) -> str:
        if self._api_key is None:
            import os
            self._api_key = os.getenv("TWELVEDATA_API_KEY", "")
        return self._api_key

    # ── Public API ──

    def get_dataframe(self, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        """Get the in-memory DataFrame for a symbol-timeframe pair."""
        return self._frames.get((symbol, timeframe))

    def get_all_timeframes(self, symbol: str) -> Dict[str, pd.DataFrame]:
        """Get all cached timeframes for a symbol."""
        result = {}
        for (sym, tf), df in self._frames.items():
            if sym == symbol and len(df) > 0:
                result[tf] = df
        return result

    async def initial_backfill(self, symbols: List[str], timeframes: List[str]) -> None:
        """Load historical data from DB into memory for all symbol-TF pairs."""
        db = self._db_factory()
        try:
            for symbol in symbols:
                for tf in timeframes:
                    df = self._load_from_db(db, symbol, tf)
                    if df is not None and len(df) > 0:
                        self._frames[(symbol, tf)] = df
                        logger.debug(
                            f"Backfill | {symbol} {tf} | {len(df)} bars from DB"
                        )
                    else:
                        # Fetch from API if DB is empty
                        bars = await self._fetch_bars(symbol, tf, outputsize=LOOKBACK_BARS.get(tf, 200))
                        if bars:
                            self._store_and_cache(db, symbol, tf, bars)
                            logger.info(f"Backfill API | {symbol} {tf} | {len(bars)} bars fetched")
        finally:
            db.close()

        total = len(self._frames)
        logger.info(f"Backfill complete | {total} symbol-TF pairs loaded")

    async def fetch_latest(self, symbol: str, timeframe: str) -> int:
        """Fetch latest candles for a symbol-TF. Returns count of new bars."""
        bars = await self._fetch_bars(symbol, timeframe, outputsize=5)
        if not bars:
            return 0

        db = self._db_factory()
        try:
            new_count = self._store_and_cache(db, symbol, timeframe, bars)
        finally:
            db.close()

        self._last_fetch[(symbol, timeframe)] = datetime.now(timezone.utc)
        return new_count

    # ── TwelveData Fetch ──

    async def _fetch_bars(
        self, symbol: str, timeframe: str, outputsize: int = 200
    ) -> List[Dict]:
        """Fetch OHLCV bars from TwelveData."""
        if not self._rate_limiter.can_request():
            wait = self._rate_limiter.seconds_until_minute_slot()
            logger.debug(f"Rate limited, waiting {wait:.1f}s for {symbol} {timeframe}")
            return []

        td_interval = TF_TO_TD_INTERVAL.get(timeframe)
        if not td_interval:
            logger.warning(f"Unknown timeframe: {timeframe}")
            return []

        # Map symbol to TwelveData format
        td_symbol = TD_MAP.get(symbol)

        # Equities use their ticker directly
        if not td_symbol and symbol in EQUITY_SYMBOLS:
            td_symbol = symbol

        # Crypto: TwelveData uses BTC/USD format
        if not td_symbol and symbol in CRYPTO_SYMBOLS:
            crypto_map = {
                "BTCUSD": "BTC/USD", "ETHUSD": "ETH/USD",
                "SOLUSD": "SOL/USD", "XRPUSD": "XRP/USD", "LINKUSD": "LINK/USD",
            }
            td_symbol = crypto_map.get(symbol)

        if not td_symbol:
            logger.warning(f"No TwelveData mapping for {symbol}")
            return []

        try:
            resp = await self._client.get(
                "https://api.twelvedata.com/time_series",
                params={
                    "symbol": td_symbol,
                    "interval": td_interval,
                    "outputsize": outputsize,
                    "apikey": self._get_api_key(),
                    "format": "JSON",
                    "dp": 5,
                },
            )
            self._rate_limiter.record_request()
            data = resp.json()

            if data.get("status") == "error":
                logger.debug(f"TwelveData error for {symbol} {timeframe}: {data.get('message')}")
                return []

            values = data.get("values", [])
            if not values:
                return []

            bars = []
            for v in values:
                try:
                    bars.append({
                        "time": v["datetime"],
                        "open": float(v["open"]),
                        "high": float(v["high"]),
                        "low": float(v["low"]),
                        "close": float(v["close"]),
                        "volume": float(v.get("volume", 0) or 0),
                    })
                except (ValueError, KeyError):
                    continue

            return bars

        except Exception as e:
            logger.debug(f"Fetch failed for {symbol} {timeframe}: {e}")
            return []

    # ── Database Storage ──

    def _store_and_cache(
        self, db: Session, symbol: str, timeframe: str, bars: List[Dict]
    ) -> int:
        """Store bars in DB and update in-memory cache. Returns new bar count."""
        table = TF_TO_TABLE.get(timeframe)
        if not table or not bars:
            return 0

        # Upsert into DB — batch insert with rollback on failure
        try:
            for bar in bars:
                db.execute(
                    text(f"""
                        INSERT INTO {table} (time, symbol, open, high, low, close, volume)
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
            db.commit()
        except Exception as e:
            db.rollback()
            logger.debug(f"DB insert failed for {symbol} {timeframe} ({table}): {e}")

        # Update in-memory DataFrame
        new_df = pd.DataFrame(bars)
        new_df["time"] = pd.to_datetime(new_df["time"], utc=True)
        new_df = new_df.sort_values("time").reset_index(drop=True)

        key = (symbol, timeframe)
        existing = self._frames.get(key)
        if existing is not None and len(existing) > 0:
            combined = pd.concat([existing, new_df], ignore_index=True)
            combined = combined.drop_duplicates(subset=["time"], keep="last")
            combined = combined.sort_values("time").reset_index(drop=True)
        else:
            combined = new_df

        # Trim to lookback
        max_bars = LOOKBACK_BARS.get(timeframe, 200)
        if len(combined) > max_bars:
            combined = combined.tail(max_bars).reset_index(drop=True)

        self._frames[key] = combined
        return len(bars)

    def _load_from_db(self, db: Session, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        """Load recent bars from DB into a DataFrame."""
        table = TF_TO_TABLE.get(timeframe)
        if not table:
            return None

        max_bars = LOOKBACK_BARS.get(timeframe, 200)
        try:
            result = db.execute(
                text(f"""
                    SELECT time, open, high, low, close, volume
                    FROM {table}
                    WHERE symbol = :symbol
                    ORDER BY time DESC
                    LIMIT :limit
                """),
                {"symbol": symbol, "limit": max_bars},
            ).fetchall()

            if not result:
                return None

            df = pd.DataFrame(result, columns=["time", "open", "high", "low", "close", "volume"])
            df["time"] = pd.to_datetime(df["time"], utc=True)
            df = df.sort_values("time").reset_index(drop=True)
            for col in ["open", "high", "low", "close", "volume"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            return df

        except Exception as e:
            logger.debug(f"DB load failed for {symbol} {timeframe}: {e}")
            return None

    # ── Utility ──

    @staticmethod
    def get_all_symbols() -> List[str]:
        """Get deduplicated list of all symbols across all desks."""
        symbols = set()
        for desk_id, desk in DESKS.items():
            symbols.update(desk.get("symbols", []))
        return sorted(symbols)

    @staticmethod
    def get_desk_timeframes(desk_id: str) -> List[str]:
        """Get the timeframes a desk uses (bias, confirmation, entry)."""
        desk = DESKS.get(desk_id, {})
        tfs = desk.get("timeframes", {})
        result = set()
        for tf_str in tfs.values():
            # Handle comma-separated TFs like "5M,15M,1H"
            for t in tf_str.split(","):
                t = t.strip().upper()
                if t in TF_TO_TD_INTERVAL:
                    result.add(t)
        return sorted(result, key=lambda x: list(TF_TO_TD_INTERVAL.keys()).index(x)
                       if x in TF_TO_TD_INTERVAL else 99)

    @staticmethod
    def get_required_timeframes() -> List[str]:
        """Get all unique timeframes needed across all desks."""
        all_tfs = set()
        for desk_id in DESKS:
            all_tfs.update(CandleManager.get_desk_timeframes(desk_id))
        return sorted(all_tfs, key=lambda x: list(TF_TO_TD_INTERVAL.keys()).index(x)
                       if x in TF_TO_TD_INTERVAL else 99)
