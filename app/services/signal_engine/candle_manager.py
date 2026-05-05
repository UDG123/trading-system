"""
OHLCV Candle Manager
Fetches candles from TwelveData, stores in PostgreSQL, maintains rolling
DataFrames in memory for fast indicator computation.
Reuses symbol mappings from ohlcv_ingester.
"""
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

import httpx
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.config import DESKS
from app.services.ohlcv_ingester import TD_MAP, CRYPTO_SYMBOLS, EQUITY_SYMBOLS
from app.services.signal_engine.rate_limiter import RateLimiter

logger = logging.getLogger("TradingSystem.SignalEngine.CandleManager")

TF_TO_TD_INTERVAL = {
    "1M": "1min", "5M": "5min", "15M": "15min",
    "1H": "1h", "4H": "4h", "D": "1day", "W": "1week",
}

TF_TO_TABLE = {
    "1M": "ohlcv_1m", "5M": "ohlcv_5m", "15M": "ohlcv_15m",
    "1H": "ohlcv_1h", "4H": "ohlcv_4h", "D": "ohlcv_1d", "W": "ohlcv_1w",
}

LOOKBACK_BARS = {
    "1M": 2000,
    "5M": 2000,
    "15M": 2000,
    "1H": 2000,
    "4H": 500,
    "D": 500,
    "W": 104,
}


class CandleManager:
    """Manages OHLCV data for signal generation."""

    def __init__(self, db_session_factory=None, rate_limiter: RateLimiter = None):
        if db_session_factory is None:
            try:
                from app.database import SessionLocal
                db_session_factory = SessionLocal
            except Exception:
                db_session_factory = lambda: None
        self._db_factory = db_session_factory
        self._rate_limiter = rate_limiter or RateLimiter()
        self._client = httpx.AsyncClient(timeout=15.0)
        self._api_key: Optional[str] = None
        self._frames: Dict[tuple, pd.DataFrame] = {}
        self._last_fetch: Dict[tuple, datetime] = {}

    async def close(self) -> None:
        await self._client.aclose()

    def _get_api_key(self) -> str:
        if self._api_key is None:
            import os
            self._api_key = os.getenv("TWELVEDATA_API_KEY", "")
        return self._api_key

    def get_dataframe(self, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        return self._frames.get((symbol, timeframe))

    def get_all_timeframes(self, symbol: str) -> Dict[str, pd.DataFrame]:
        return {tf: df for (sym, tf), df in self._frames.items() if sym == symbol and len(df) > 0}

    def update_dataframe(self, symbol: str, timeframe: str, candles: List[Dict]) -> int:
        if not candles:
            return 0
        normalized = []
        for candle in candles:
            try:
                normalized.append({
                    "time": candle["time"],
                    "open": float(candle["open"]),
                    "high": float(candle["high"]),
                    "low": float(candle["low"]),
                    "close": float(candle["close"]),
                    "volume": float(candle.get("volume", 0) or 0),
                    "provider": candle.get("provider"),
                })
            except (KeyError, TypeError, ValueError):
                continue
        if not normalized:
            return 0
        new_df = pd.DataFrame(normalized)
        new_df["time"] = pd.to_datetime(new_df["time"], utc=True, errors="coerce")
        new_df = new_df.dropna(subset=["time"])
        new_df = new_df.sort_values("time").drop_duplicates(subset=["time"], keep="last").reset_index(drop=True)
        for col in ["open", "high", "low", "close", "volume"]:
            new_df[col] = pd.to_numeric(new_df[col], errors="coerce")

        key = (symbol, timeframe)
        existing = self._frames.get(key)
        combined = pd.concat([existing, new_df], ignore_index=True) if existing is not None and len(existing) > 0 else new_df
        combined = combined.drop_duplicates(subset=["time"], keep="last").sort_values("time").reset_index(drop=True)
        self._frames[key] = combined.tail(LOOKBACK_BARS.get(timeframe, 300)).reset_index(drop=True)
        return len(new_df)

    load_candles = update_dataframe

    async def initial_backfill(self, symbols: List[str], timeframes: List[str]) -> None:
        db = self._db_factory()
        loaded_pairs = 0
        try:
            for symbol in symbols:
                for tf in timeframes:
                    df = self._load_from_db(db, symbol, tf)
                    if df is not None and len(df) > 0:
                        self._frames[(symbol, tf)] = df
                        loaded_pairs += 1
                        logger.info(
                            "Backfill DB | %s %s | bars=%s | first=%s | last=%s",
                            symbol, tf, len(df), df["time"].iloc[0], df["time"].iloc[-1],
                        )
                        continue

                    bars = await self._fetch_bars(symbol, tf, outputsize=LOOKBACK_BARS.get(tf, 200))
                    if bars:
                        count = self._store_and_cache(db, symbol, tf, bars)
                        if count > 0 and self._frames.get((symbol, tf)) is not None:
                            loaded_pairs += 1
                        logger.info("Backfill API | %s %s | %s bars fetched/cached", symbol, tf, len(bars))
        finally:
            if db is not None:
                db.close()

        logger.info("Backfill complete | %s symbol-TF pairs loaded", loaded_pairs)

    async def fetch_latest(self, symbol: str, timeframe: str) -> int:
        bars = await self._fetch_bars(symbol, timeframe, outputsize=5)
        if not bars:
            return 0
        db = self._db_factory()
        try:
            new_count = self._store_and_cache(db, symbol, timeframe, bars)
        finally:
            if db is not None:
                db.close()
        self._last_fetch[(symbol, timeframe)] = datetime.now(timezone.utc)
        return new_count

    async def _fetch_bars(self, symbol: str, timeframe: str, outputsize: int = 200) -> List[Dict]:
        if not self._rate_limiter.can_request():
            wait = self._rate_limiter.seconds_until_minute_slot()
            logger.debug("Rate limited, waiting %.1fs for %s %s", wait, symbol, timeframe)
            return []

        td_interval = TF_TO_TD_INTERVAL.get(timeframe)
        if not td_interval:
            logger.warning("Unknown timeframe: %s", timeframe)
            return []

        td_symbol = TD_MAP.get(symbol)
        if not td_symbol and symbol in EQUITY_SYMBOLS:
            td_symbol = symbol
        if not td_symbol and symbol in CRYPTO_SYMBOLS:
            td_symbol = {
                "BTCUSD": "BTC/USD", "ETHUSD": "ETH/USD",
                "SOLUSD": "SOL/USD", "XRPUSD": "XRP/USD", "LINKUSD": "LINK/USD",
            }.get(symbol)
        if not td_symbol:
            logger.warning("No TwelveData mapping for %s", symbol)
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
            if not isinstance(data, dict):
                logger.warning(
                    "Malformed TwelveData response | %s %s | type=%s",
                    symbol, timeframe, type(data).__name__,
                )
                return []

            if data.get("status") == "error" or data.get("code") in (400, 401, 403, 429):
                logger.warning("TwelveData error for %s %s: %s", symbol, timeframe, data.get("message", data))
                return []

            values = data.get("values", [])
            if not values:
                logger.debug("TwelveData empty values for %s %s | keys=%s", symbol, timeframe, list(data.keys()))
                return []

            bars = []
            # TwelveData returns newest-first. Cache oldest-first.
            for v in reversed(values):
                try:
                    bars.append({
                        "time": v["datetime"],
                        "open": float(v["open"]),
                        "high": float(v["high"]),
                        "low": float(v["low"]),
                        "close": float(v["close"]),
                        "volume": float(v.get("volume", 0) or 0),
                        "provider": "twelvedata",
                    })
                except (ValueError, KeyError, TypeError):
                    logger.warning("Malformed bar skipped | %s %s | row=%s", symbol, timeframe, v)
                    continue
            if not bars:
                logger.warning("TwelveData parsed 0 bars for %s %s from %s values", symbol, timeframe, len(values))
            else:
                logger.info(
                    "Fetched bars | %s %s | count=%s | first=%s | last=%s",
                    symbol, timeframe, len(bars), bars[0]["time"], bars[-1]["time"],
                )
            return bars
        except Exception as e:
            logger.debug("Fetch failed for %s %s: %s", symbol, timeframe, e)
            return []

    def _store_and_cache(self, db: Session, symbol: str, timeframe: str, bars: List[Dict]) -> int:
        table = TF_TO_TABLE.get(timeframe)
        if not table or not bars:
            return 0

        inserted = 0
        if db is not None:
            try:
                for bar in bars:
                    result = db.execute(
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
                            "volume": bar.get("volume", 0),
                        },
                    )
                    if getattr(result, "rowcount", 0) > 0:
                        inserted += 1
                db.commit()
            except Exception as e:
                db.rollback()
                logger.warning("DB insert failed for %s %s (%s): %s; continuing with memory cache", symbol, timeframe, table, e)

        cached = self.update_dataframe(symbol, timeframe, bars)
        return max(inserted, cached)

    def _load_from_db(self, db: Session, symbol: str, timeframe: str) -> Optional[pd.DataFrame]:
        table = TF_TO_TABLE.get(timeframe)
        if not table or db is None:
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
            df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
            df = df.dropna(subset=["time"]).sort_values("time").reset_index(drop=True)
            for col in ["open", "high", "low", "close", "volume"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            if df.empty:
                logger.warning("DB load empty after cleanup | %s %s", symbol, timeframe)
                return None
            nan_cols = df[["open", "high", "low", "close", "volume"]].isna().sum().to_dict()
            if any(v > 0 for v in nan_cols.values()):
                logger.warning("DB candle NaNs detected | %s %s | %s", symbol, timeframe, nan_cols)
                df = df.dropna(subset=["open", "high", "low", "close"]).reset_index(drop=True)
            if not df["time"].is_monotonic_increasing:
                logger.warning("DB candle ordering corrected | %s %s", symbol, timeframe)
                df = df.sort_values("time").reset_index(drop=True)
            return df
        except Exception as e:
            logger.debug("DB load failed for %s %s: %s", symbol, timeframe, e)
            return None

    @staticmethod
    def get_all_symbols() -> List[str]:
        symbols = set()
        for desk in DESKS.values():
            symbols.update(desk.get("symbols", []))
        return sorted(symbols)

    @staticmethod
    def get_desk_timeframes(desk_id: str) -> List[str]:
        desk = DESKS.get(desk_id, {})
        tfs = desk.get("timeframes", {})
        result = set()
        for tf_str in tfs.values():
            for t in tf_str.split(","):
                t = t.strip().upper()
                if t in TF_TO_TD_INTERVAL:
                    result.add(t)
        order = list(TF_TO_TD_INTERVAL.keys())
        return sorted(result, key=lambda x: order.index(x) if x in order else 99)

    @staticmethod
    def get_required_timeframes() -> List[str]:
        all_tfs = set()
        for desk_id in DESKS:
            all_tfs.update(CandleManager.get_desk_timeframes(desk_id))
        order = list(TF_TO_TD_INTERVAL.keys())
        return sorted(all_tfs, key=lambda x: order.index(x) if x in order else 99)
