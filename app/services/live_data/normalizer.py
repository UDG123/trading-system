from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional


@dataclass(frozen=True)
class TickEvent:
    symbol: str
    price: float
    size: float
    ts: datetime
    provider: str


@dataclass(frozen=True)
class QuoteEvent:
    symbol: str
    bid: float
    ask: float
    bid_size: float
    ask_size: float
    ts: datetime
    provider: str


@dataclass(frozen=True)
class BarEvent:
    symbol: str
    timeframe: str
    open: float
    high: float
    low: float
    close: float
    volume: float
    ts: datetime
    provider: str


def normalize_ts(value: Any) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        if value > 10_000_000_000:
            value = value / 1000.0
        return datetime.fromtimestamp(value, tz=timezone.utc)
    if isinstance(value, str):
        v = value.replace("Z", "+00:00")
        dt = datetime.fromisoformat(v)
        return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    raise ValueError(f"Unsupported timestamp: {value}")


def normalize_symbol(provider_symbol: str, provider: str, symbol_map: Optional[Dict[str, str]] = None) -> str:
    symbol_map = symbol_map or {}
    key = f"{provider}:{provider_symbol}".upper()
    if key in symbol_map:
        return symbol_map[key]
    return provider_symbol.replace("/", "").replace("-", "").upper()


def normalize_tick(raw: Dict[str, Any], provider: str, symbol_map: Optional[Dict[str, str]] = None) -> TickEvent:
    return TickEvent(
        symbol=normalize_symbol(str(raw.get("symbol", "")), provider, symbol_map),
        price=float(raw.get("price", raw.get("last", 0.0))),
        size=float(raw.get("size", raw.get("volume", 0.0))),
        ts=normalize_ts(raw.get("ts", raw.get("timestamp", raw.get("time")))),
        provider=provider,
    )


def normalize_quote(raw: Dict[str, Any], provider: str, symbol_map: Optional[Dict[str, str]] = None) -> QuoteEvent:
    return QuoteEvent(
        symbol=normalize_symbol(str(raw.get("symbol", "")), provider, symbol_map),
        bid=float(raw.get("bid", 0.0)),
        ask=float(raw.get("ask", 0.0)),
        bid_size=float(raw.get("bid_size", 0.0)),
        ask_size=float(raw.get("ask_size", 0.0)),
        ts=normalize_ts(raw.get("ts", raw.get("timestamp", raw.get("time")))),
        provider=provider,
    )


def normalize_bar(raw: Dict[str, Any], provider: str, timeframe: str, symbol_map: Optional[Dict[str, str]] = None) -> BarEvent:
    return BarEvent(
        symbol=normalize_symbol(str(raw.get("symbol", "")), provider, symbol_map),
        timeframe=timeframe.upper(),
        open=float(raw.get("open", 0.0)),
        high=float(raw.get("high", 0.0)),
        low=float(raw.get("low", 0.0)),
        close=float(raw.get("close", 0.0)),
        volume=float(raw.get("volume", 0.0)),
        ts=normalize_ts(raw.get("ts", raw.get("timestamp", raw.get("time")))),
        provider=provider,
    )
