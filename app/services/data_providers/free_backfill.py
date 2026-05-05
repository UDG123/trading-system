"""Free/cache-first historical backfill provider.

Purpose:
- Reduce/no dependence on TwelveData paid credits.
- Provide enough candles for paper-trading signal proof-of-concept.
- Use free/delayed public sources where available.

Primary source: yfinance, if installed.
This module fails softly if yfinance is unavailable so the app can still boot.
"""
from __future__ import annotations

import logging
from datetime import timezone
from typing import Dict, List, Optional

import pandas as pd

logger = logging.getLogger("TradingSystem.FreeBackfill")

YF_SYMBOL_MAP: Dict[str, str] = {
    "AAPL": "AAPL",
    "MSFT": "MSFT",
    "NVDA": "NVDA",
    "TSLA": "TSLA",
    "AMZN": "AMZN",
    "META": "META",
    "GOOGL": "GOOGL",
    "NFLX": "NFLX",
    "AMD": "AMD",
    "EURUSD": "EURUSD=X",
    "GBPUSD": "GBPUSD=X",
    "USDJPY": "JPY=X",
    "AUDUSD": "AUDUSD=X",
    "XAUUSD": "GC=F",
    "XAGUSD": "SI=F",
    "WTIUSD": "CL=F",
    "NAS100": "QQQ",
    "US30": "DIA",
    "SPX500": "SPY",
    "BTCUSD": "BTC-USD",
    "ETHUSD": "ETH-USD",
    "SOLUSD": "SOL-USD",
    "XRPUSD": "XRP-USD",
    "LINKUSD": "LINK-USD",
}

TF_TO_YF_INTERVAL = {
    "1M": "1m",
    "5M": "5m",
    "15M": "15m",
    "1H": "1h",
    "4H": "1h",  # fetch 1h then resample locally
    "D": "1d",
    "W": "1wk",
}

TF_TO_PERIOD = {
    "1M": "7d",   # Yahoo intraday limits
    "5M": "60d",
    "15M": "60d",
    "1H": "730d",
    "4H": "730d",
    "D": "5y",
    "W": "10y",
}


def _import_yfinance():
    try:
        import yfinance as yf  # type: ignore
        return yf
    except Exception as exc:
        logger.info("yfinance unavailable; free backfill skipped: %s", exc)
        return None


def _normalize_df(df: pd.DataFrame, provider: str = "yfinance") -> List[Dict]:
    if df is None or df.empty:
        return []
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0] if isinstance(c, tuple) else c for c in df.columns]
    rename = {"Open": "open", "High": "high", "Low": "low", "Close": "close", "Volume": "volume"}
    df = df.rename(columns=rename)
    needed = ["open", "high", "low", "close"]
    if not all(col in df.columns for col in needed):
        return []
    if df.index.tz is None:
        df.index = df.index.tz_localize(timezone.utc)
    else:
        df.index = df.index.tz_convert(timezone.utc)
    df = df.dropna(subset=needed).sort_index()
    bars: List[Dict] = []
    for ts, row in df.iterrows():
        try:
            bars.append({
                "time": ts.isoformat(),
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row.get("volume", 0) or 0),
                "provider": provider,
            })
        except Exception:
            continue
    return bars


def _resample_bars(bars: List[Dict], rule: str) -> List[Dict]:
    if not bars:
        return []
    df = pd.DataFrame(bars)
    df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")
    df = df.dropna(subset=["time"]).set_index("time").sort_index()
    out = df.resample(rule).agg({
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum",
    }).dropna(subset=["open", "high", "low", "close"])
    out["provider"] = "yfinance_resampled"
    return _normalize_df(out.rename(columns={"open":"Open","high":"High","low":"Low","close":"Close","volume":"Volume"}), "yfinance_resampled")


async def fetch_free_bars(symbol: str, timeframe: str, outputsize: int = 500) -> List[Dict]:
    yf_symbol = YF_SYMBOL_MAP.get(symbol.upper())
    if not yf_symbol:
        return []
    yf = _import_yfinance()
    if yf is None:
        return []

    tf = timeframe.upper()
    interval = TF_TO_YF_INTERVAL.get(tf)
    period = TF_TO_PERIOD.get(tf, "60d")
    if not interval:
        return []

    try:
        df = yf.download(
            tickers=yf_symbol,
            interval=interval,
            period=period,
            progress=False,
            auto_adjust=False,
            threads=False,
        )
        bars = _normalize_df(df, "yfinance")
        if tf == "4H" and bars:
            bars = _resample_bars(bars, "4h")
        if outputsize and len(bars) > outputsize:
            bars = bars[-outputsize:]
        if bars:
            logger.info("Free backfill | %s(%s) %s | bars=%s", symbol, yf_symbol, timeframe, len(bars))
        return bars
    except Exception as exc:
        logger.info("Free backfill failed | %s %s | %s", symbol, timeframe, exc)
        return []
