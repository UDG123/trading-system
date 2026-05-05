"""
MCP Market Data Provider Adapter

Optional provider bridge for OniQuant's Python-native signal generator.
This adapter treats MCP as a market-data abstraction layer, not as an execution layer.

Supported MCP server response styles:
1. JSON-RPC 2.0 Streamable HTTP style endpoint:
   POST {MCP_MARKET_DATA_URL}
   {"jsonrpc":"2.0","id":"...","method":"tools/call","params":{"name":"historical_ohlcv","arguments":{...}}}

2. Simple HTTP tool endpoint fallback:
   POST {MCP_MARKET_DATA_URL}/historical_ohlcv
   {"symbol":"EURUSD","timeframe":"1M",...}

The adapter normalizes returned OHLCV rows and feeds CandleManager.update_dataframe().
It does not place trades and does not add broker execution.
"""
from __future__ import annotations

import asyncio
import logging
import os
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional

import httpx

logger = logging.getLogger("TradingSystem.MCPMarketData")

DEFAULT_TIMEFRAMES = ["1M", "5M", "15M", "1H", "4H", "D", "W"]
DEFAULT_SYMBOLS = [
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "XAUUSD",
    "BTCUSD", "ETHUSD", "SOLUSD", "XRPUSD", "LINKUSD",
    "AAPL", "MSFT", "NVDA", "TSLA",
]

TF_TO_MCP = {
    "1M": "1m",
    "5M": "5m",
    "15M": "15m",
    "1H": "1h",
    "4H": "4h",
    "D": "1d",
    "W": "1w",
}


@dataclass
class MCPProviderStats:
    connected: bool = False
    backfill_calls: int = 0
    bars_loaded: int = 0
    errors: int = 0
    last_ok: Optional[str] = None
    last_error: Optional[str] = None


class MCPMarketDataProvider:
    """Pulls normalized market data from an MCP-compatible market-data server."""

    def __init__(
        self,
        candle_manager: Any,
        base_url: Optional[str] = None,
        api_key: Optional[str] = None,
        symbols: Optional[Iterable[str]] = None,
        timeframes: Optional[Iterable[str]] = None,
        provider_order: Optional[str] = None,
        poll_interval_seconds: Optional[int] = None,
        timeout_seconds: Optional[float] = None,
    ) -> None:
        self.candle_manager = candle_manager
        self.base_url = (base_url or os.getenv("MCP_MARKET_DATA_URL") or os.getenv("MCP_BASE_URL") or "").rstrip("/")
        self.api_key = api_key if api_key is not None else os.getenv("MCP_API_KEY", "")
        self.provider_order = provider_order or os.getenv("MCP_PROVIDER_ORDER", "polygon,alpaca,twelvedata")
        self.poll_interval_seconds = poll_interval_seconds or int(os.getenv("MCP_MARKET_DATA_POLL_SECONDS", "300"))
        self.timeout_seconds = timeout_seconds or float(os.getenv("MCP_TIMEOUT_SECONDS", "15"))
        self.symbols = list(symbols or self._env_list("MCP_MARKET_DATA_SYMBOLS", DEFAULT_SYMBOLS))
        self.timeframes = list(timeframes or self._env_list("MCP_MARKET_DATA_TIMEFRAMES", DEFAULT_TIMEFRAMES))
        self.stats = MCPProviderStats()
        self._running = False
        self._client: Optional[httpx.AsyncClient] = None

    @staticmethod
    def enabled() -> bool:
        return os.getenv("ENABLE_MCP_MARKET_DATA", "false").lower() in {"1", "true", "yes", "on"}

    @staticmethod
    def _env_list(name: str, default: Iterable[str]) -> List[str]:
        raw = os.getenv(name, "")
        if not raw.strip():
            return list(default)
        return [part.strip().upper() for part in raw.split(",") if part.strip()]

    def _headers(self) -> Dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    async def run(self) -> None:
        """Run a conservative periodic MCP backfill/gap-fill loop."""
        if not self.base_url:
            logger.info("MCP market data disabled (MCP_MARKET_DATA_URL/MCP_BASE_URL missing)")
            return

        self._running = True
        self._client = httpx.AsyncClient(timeout=self.timeout_seconds, headers=self._headers())
        logger.info(
            "MCP market data starting | symbols=%s | timeframes=%s | poll=%ss | providers=%s",
            len(self.symbols), self.timeframes, self.poll_interval_seconds, self.provider_order,
        )

        try:
            await self.health_check()
            await self.backfill_once(limit=int(os.getenv("MCP_BACKFILL_LIMIT", "300")))
            while self._running:
                await asyncio.sleep(self.poll_interval_seconds)
                await self.backfill_once(limit=int(os.getenv("MCP_INCREMENTAL_LIMIT", "10")))
        finally:
            if self._client:
                await self._client.aclose()
            logger.info("MCP market data stopped | stats=%s", self.stats)

    async def health_check(self) -> bool:
        if not self._client:
            return False
        try:
            payload = await self._call_tool("provider_health", {"provider_order": self.provider_order})
            self.stats.connected = True
            self.stats.last_ok = datetime.now(timezone.utc).isoformat()
            logger.info("MCP provider health OK | %s", payload if payload else "no structured payload")
            return True
        except Exception as exc:
            # Some MCP servers may not expose provider_health yet. Do not fail startup.
            self.stats.last_error = str(exc)
            logger.warning("MCP provider health unavailable, continuing: %s", exc)
            return False

    async def backfill_once(self, limit: int = 300) -> int:
        loaded_total = 0
        for symbol in self.symbols:
            for timeframe in self.timeframes:
                if not self._running:
                    return loaded_total
                try:
                    bars = await self.fetch_ohlcv(symbol, timeframe, limit=limit)
                    if bars:
                        loaded = self.candle_manager.update_dataframe(symbol, timeframe, bars)
                        loaded_total += loaded
                        self.stats.bars_loaded += loaded
                        logger.info("MCP bars loaded | %s %s | bars=%s", symbol, timeframe, loaded)
                except Exception as exc:
                    self.stats.errors += 1
                    self.stats.last_error = str(exc)
                    logger.warning("MCP backfill failed | %s %s | %s", symbol, timeframe, exc)
                await asyncio.sleep(float(os.getenv("MCP_INTER_REQUEST_DELAY_SECONDS", "0.25")))
        return loaded_total

    async def fetch_ohlcv(self, symbol: str, timeframe: str, limit: int = 300) -> List[Dict[str, Any]]:
        args = {
            "symbol": symbol,
            "timeframe": TF_TO_MCP.get(timeframe, timeframe.lower()),
            "internal_timeframe": timeframe,
            "limit": limit,
            "provider_order": self.provider_order,
        }
        payload = await self._call_tool("historical_ohlcv", args)
        self.stats.backfill_calls += 1
        bars = self._extract_bars(payload)
        normalized = self._normalize_bars(bars)
        if normalized:
            self.stats.last_ok = datetime.now(timezone.utc).isoformat()
        return normalized

    async def _call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        if not self._client:
            raise RuntimeError("MCP client not initialized")

        # Try JSON-RPC tools/call first.
        rpc_payload = {
            "jsonrpc": "2.0",
            "id": f"oniquant-{tool_name}-{int(time.time() * 1000)}",
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
        }
        resp = await self._client.post(self.base_url, json=rpc_payload)
        if resp.status_code < 400:
            data = resp.json()
            if "error" in data:
                raise RuntimeError(data["error"])
            return self._unwrap_mcp_result(data.get("result", data))

        # Fallback for simple HTTP tool servers.
        fallback = await self._client.post(f"{self.base_url}/{tool_name}", json=arguments)
        fallback.raise_for_status()
        return fallback.json()

    @staticmethod
    def _unwrap_mcp_result(result: Any) -> Dict[str, Any]:
        if not isinstance(result, dict):
            return {"data": result}
        structured = result.get("structuredContent") or result.get("structured_content")
        if isinstance(structured, dict):
            return structured
        content = result.get("content")
        if isinstance(content, list):
            for item in content:
                if isinstance(item, dict):
                    if isinstance(item.get("json"), dict):
                        return item["json"]
                    if item.get("type") == "text" and item.get("text"):
                        # Avoid importing json unless text response actually appears.
                        import json
                        try:
                            parsed = json.loads(item["text"])
                            if isinstance(parsed, dict):
                                return parsed
                        except Exception:
                            pass
        return result

    @staticmethod
    def _extract_bars(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
        for key in ("bars", "ohlcv", "values", "data", "result"):
            val = payload.get(key) if isinstance(payload, dict) else None
            if isinstance(val, list):
                return val
        return []

    @staticmethod
    def _normalize_bars(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        normalized: List[Dict[str, Any]] = []
        for row in rows:
            try:
                ts = row.get("time") or row.get("datetime") or row.get("timestamp") or row.get("ts_event")
                if ts is None:
                    continue
                normalized.append({
                    "time": ts,
                    "open": float(row.get("open", row.get("o"))),
                    "high": float(row.get("high", row.get("h"))),
                    "low": float(row.get("low", row.get("l"))),
                    "close": float(row.get("close", row.get("c"))),
                    "volume": float(row.get("volume", row.get("v", 0)) or 0),
                    "provider": row.get("provider", "mcp"),
                })
            except Exception:
                continue
        return normalized

    def stop(self) -> None:
        self._running = False
