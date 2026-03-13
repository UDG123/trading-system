"""
Server-Side Trade Simulator — 4-Provider Asset-Class Router
Institutional-grade price feed with JIT polling, circuit breakers,
adaptive throttling, and Binance WebSocket for crypto.

Provider Architecture:
  Crypto    → Binance WebSocket (real-time, 0 REST calls)
  Forex     → TwelveData batch (primary) → FMP batch (fallback)
  Metals    → TwelveData batch (primary) → FMP batch (fallback)
  Stocks    → Finnhub individual (primary) → FMP batch (fallback)
  Indices   → FMP batch (primary) → TwelveData batch (fallback)
  Oil/Cu    → FMP batch (primary) → TwelveData batch (fallback)

Rate Budgets:
  TwelveData: 8 credits/min, 800/day — forex/metals only
  Finnhub:    60 req/min — US stocks only
  FMP:        250 req/day — indices, commodities, universal fallback
  Binance WS: Unlimited — crypto streaming
"""
import os
import json
import asyncio
import logging
import time as _time
from datetime import datetime, timezone
from typing import Dict, Optional, Set, List
from collections import deque

import httpx
from sqlalchemy.orm import Session

from app.config import (
    DESKS, CAPITAL_PER_ACCOUNT,
    PORTFOLIO_CAPITAL_PER_DESK, get_pip_info,
    SYMBOL_ASSET_CLASS,
    TWELVEDATA_API_KEY, FINNHUB_API_KEY,
    BINANCE_WS_URL, BINANCE_REST_URL,
)
from app.database import SessionLocal
from app.models.trade import Trade
from app.models.desk_state import DeskState
from app.services.telegram_bot import TelegramBot

logger = logging.getLogger("TradingSystem.Simulator")

FMP_API_KEY = os.getenv("FMP_API_KEY", "")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ASSET CLASS CLASSIFICATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CRYPTO_SYMBOLS = {"BTCUSD", "ETHUSD", "SOLUSD", "XRPUSD", "LINKUSD"}

FOREX_SYMBOLS = {
    "EURUSD", "USDJPY", "GBPUSD", "USDCHF", "AUDUSD", "USDCAD",
    "NZDUSD", "EURJPY", "GBPJPY", "AUDJPY", "EURGBP", "EURAUD",
    "GBPAUD", "EURCHF", "CADJPY", "NZDJPY", "GBPCAD", "AUDCAD",
    "AUDNZD", "CHFJPY", "EURNZD", "GBPNZD",
}

METAL_SYMBOLS = {"XAUUSD", "XAGUSD"}

STOCK_SYMBOLS = {"TSLA", "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "NFLX", "AMD"}

INDEX_SYMBOLS = {"US30", "US100", "NAS100"}

COMMODITY_SYMBOLS = {"WTIUSD", "XCUUSD"}


def classify_symbol(sym: str) -> str:
    if sym in CRYPTO_SYMBOLS: return "CRYPTO"
    if sym in FOREX_SYMBOLS: return "FOREX"
    if sym in METAL_SYMBOLS: return "METAL"
    if sym in STOCK_SYMBOLS: return "STOCK"
    if sym in INDEX_SYMBOLS: return "INDEX"
    if sym in COMMODITY_SYMBOLS: return "COMMODITY"
    return "UNKNOWN"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PROVIDER SYMBOL MAPS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# TwelveData (forex + metals)
TD_MAP = {
    "EURUSD": "EUR/USD", "USDJPY": "USD/JPY", "GBPUSD": "GBP/USD",
    "USDCHF": "USD/CHF", "AUDUSD": "AUD/USD", "USDCAD": "USD/CAD",
    "NZDUSD": "NZD/USD", "EURJPY": "EUR/JPY", "GBPJPY": "GBP/JPY",
    "AUDJPY": "AUD/JPY", "EURGBP": "EUR/GBP", "EURAUD": "EUR/AUD",
    "GBPAUD": "GBP/AUD", "EURCHF": "EUR/CHF", "CADJPY": "CAD/JPY",
    "NZDJPY": "NZD/JPY", "GBPCAD": "GBP/CAD", "AUDCAD": "AUD/CAD",
    "AUDNZD": "AUD/NZD", "CHFJPY": "CHF/JPY", "EURNZD": "EUR/NZD",
    "GBPNZD": "GBP/NZD",
    "XAUUSD": "XAU/USD", "XAGUSD": "XAG/USD",
}
TD_REVERSE = {v: k for k, v in TD_MAP.items()}

# Finnhub (US stocks only — free tier)
FH_MAP = {
    "TSLA": "TSLA", "AAPL": "AAPL", "MSFT": "MSFT",
    "NVDA": "NVDA", "AMZN": "AMZN", "META": "META",
    "GOOGL": "GOOGL", "NFLX": "NFLX", "AMD": "AMD",
}

# FMP (indices, commodities, universal fallback)
FMP_MAP = {
    # Indices
    "US30": "^DJI", "US100": "^GSPC", "NAS100": "^IXIC",
    # Commodities
    "WTIUSD": "CLUSD", "XCUUSD": "HGUSD",
    # Metals (fallback)
    "XAUUSD": "XAUUSD", "XAGUSD": "XAGUSD",
    # Forex (fallback)
    "EURUSD": "EURUSD", "USDJPY": "USDJPY", "GBPUSD": "GBPUSD",
    "USDCHF": "USDCHF", "AUDUSD": "AUDUSD", "USDCAD": "USDCAD",
    "NZDUSD": "NZDUSD", "EURJPY": "EURJPY", "GBPJPY": "GBPJPY",
    "AUDJPY": "AUDJPY", "EURGBP": "EURGBP", "EURAUD": "EURAUD",
    "GBPAUD": "GBPAUD", "EURCHF": "EURCHF", "CADJPY": "CADJPY",
    "NZDJPY": "NZDJPY", "GBPCAD": "GBPCAD", "AUDCAD": "AUDCAD",
    "AUDNZD": "AUDNZD", "CHFJPY": "CHFJPY", "EURNZD": "EURNZD",
    "GBPNZD": "GBPNZD",
    # Stocks (fallback)
    "TSLA": "TSLA", "AAPL": "AAPL", "MSFT": "MSFT",
    "NVDA": "NVDA", "AMZN": "AMZN", "META": "META",
    "GOOGL": "GOOGL", "NFLX": "NFLX", "AMD": "AMD",
}
FMP_REVERSE = {v: k for k, v in FMP_MAP.items()}

# Binance WebSocket (crypto)
BINANCE_STREAMS = {
    "BTCUSD": "btcusdt", "ETHUSD": "ethusdt",
    "SOLUSD": "solusdt", "XRPUSD": "xrpusdt", "LINKUSD": "linkusdt",
}
BINANCE_REVERSE = {v.replace("@ticker", ""): k for k, v in BINANCE_STREAMS.items()}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CIRCUIT BREAKER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class CircuitBreaker:
    """Per-provider circuit breaker: CLOSED → OPEN → HALF_OPEN → CLOSED."""

    def __init__(self, name: str, failure_threshold: int = 5, cooldown: float = 60.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.cooldown = cooldown
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self.consecutive_failures = 0
        self.last_failure_time = 0.0
        self.total_success = 0
        self.total_fail = 0
        self.extra_stats: Dict = {}

    def allow_request(self) -> bool:
        if self.state == "CLOSED":
            return True
        if self.state == "OPEN":
            if _time.time() - self.last_failure_time >= self.cooldown:
                self.state = "HALF_OPEN"
                logger.info(f"CB {self.name}: OPEN → HALF_OPEN (testing)")
                return True
            return False
        if self.state == "HALF_OPEN":
            return True
        return False

    def record_success(self):
        self.total_success += 1
        self.consecutive_failures = 0
        if self.state == "HALF_OPEN":
            self.state = "CLOSED"
            logger.info(f"CB {self.name}: HALF_OPEN → CLOSED (recovered)")

    def record_failure(self):
        self.total_fail += 1
        self.consecutive_failures += 1
        self.last_failure_time = _time.time()
        if self.state == "HALF_OPEN":
            self.state = "OPEN"
            self.cooldown = min(self.cooldown * 2, 300)
            logger.warning(f"CB {self.name}: HALF_OPEN → OPEN (cooldown: {self.cooldown}s)")
        elif self.consecutive_failures >= self.failure_threshold:
            self.state = "OPEN"
            logger.warning(f"CB {self.name}: CLOSED → OPEN ({self.consecutive_failures} failures)")

    def get_stats(self) -> Dict:
        return {
            "state": self.state,
            "success": self.total_success,
            "fail": self.total_fail,
            "consecutive_failures": self.consecutive_failures,
            **self.extra_stats,
        }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ADAPTIVE THROTTLE FOR TWELVEDATA
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TDThrottle:
    """Track TwelveData credit usage, adapt polling interval."""

    def __init__(self):
        self.credits_log: deque = deque()  # (timestamp, credits_used)
        self.daily_credits = 0
        self.daily_reset_date = ""

    def record_credits(self, count: int):
        now = _time.time()
        self.credits_log.append((now, count))
        self.daily_credits += count
        # Reset daily counter at midnight
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today != self.daily_reset_date:
            self.daily_credits = count
            self.daily_reset_date = today
        # Prune old entries (>60s)
        while self.credits_log and self.credits_log[0][0] < now - 60:
            self.credits_log.popleft()

    def credits_last_60s(self) -> int:
        now = _time.time()
        while self.credits_log and self.credits_log[0][0] < now - 60:
            self.credits_log.popleft()
        return sum(c for _, c in self.credits_log)

    def get_interval(self) -> float:
        used = self.credits_last_60s()
        if self.daily_credits > 700:
            return 45.0  # preservation mode
        if used >= 7:
            return 30.0  # conservative
        if used >= 5:
            return 20.0  # cautious
        return 15.0  # aggressive


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MARKET-AWARE CACHE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _get_cache_ttl(asset_class: str) -> float:
    """Market-aware TTL: don't waste API calls outside trading hours."""
    now = datetime.now(timezone.utc)
    hour = now.hour
    weekday = now.weekday()  # 0=Mon, 6=Sun

    if asset_class == "CRYPTO":
        return 10.0  # 24/7

    # Weekend — everything except crypto is closed
    if weekday >= 5:  # Saturday/Sunday
        return 3600.0

    if asset_class in ("FOREX", "METAL"):
        # Forex: Sun 21:00 → Fri 21:00 UTC
        return 15.0  # during the week

    if asset_class in ("STOCK", "INDEX"):
        # US market hours: 13:30-21:00 UTC
        if 13 <= hour < 21:
            return 15.0
        return 300.0  # after hours

    if asset_class == "COMMODITY":
        if 13 <= hour < 21:
            return 20.0
        return 300.0

    return 30.0


class PriceCache:
    """In-memory price cache with market-aware TTLs."""

    def __init__(self):
        self._data: Dict[str, Dict] = {}

    def get(self, symbol: str) -> Optional[float]:
        entry = self._data.get(symbol)
        if not entry:
            return None
        age = _time.time() - entry["time"]
        ttl = _get_cache_ttl(classify_symbol(symbol))
        if age <= ttl:
            return entry["price"]
        return None  # expired

    def get_stale(self, symbol: str) -> Optional[float]:
        """Return price even if expired (for fallback)."""
        entry = self._data.get(symbol)
        return entry["price"] if entry else None

    def set(self, symbol: str, price: float):
        self._data[symbol] = {"price": price, "time": _time.time()}

    def age(self, symbol: str) -> Optional[float]:
        entry = self._data.get(symbol)
        return _time.time() - entry["time"] if entry else None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# BINANCE WEBSOCKET MANAGER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class BinanceWSManager:
    """Maintains a persistent WebSocket connection to Binance for crypto prices."""

    def __init__(self, cache: PriceCache):
        self.cache = cache
        self.running = False
        self._task = None
        self._connected = False
        self._reconnect_delay = 1.0

    @property
    def connected(self) -> bool:
        return self._connected

    async def start(self):
        self.running = True
        self._task = asyncio.create_task(self._run())
        logger.info("Binance WebSocket manager started")

    async def stop(self):
        self.running = False
        if self._task:
            self._task.cancel()
        logger.info("Binance WebSocket manager stopped")

    async def _run(self):
        while self.running:
            try:
                await self._connect()
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._connected = False
                logger.warning(f"Binance WS error: {e} | reconnect in {self._reconnect_delay}s")
                await asyncio.sleep(self._reconnect_delay)
                self._reconnect_delay = min(self._reconnect_delay * 2, 60.0)

    async def _connect(self):
        try:
            import websockets
        except ImportError:
            logger.error("websockets package not installed — Binance WS disabled")
            self.running = False
            return

        streams = [f"{s}@ticker" for s in BINANCE_STREAMS.values()]
        url = BINANCE_WS_URL + "/" + "/".join(streams)

        async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
            self._connected = True
            self._reconnect_delay = 1.0
            logger.info(f"Binance WS connected | {len(streams)} streams")

            async for message in ws:
                if not self.running:
                    break
                try:
                    data = json.loads(message)
                    stream = data.get("s", "").lower()  # e.g. "btcusdt"
                    price_str = data.get("c")  # last price
                    if stream and price_str:
                        # Map back to internal symbol
                        for internal, binance in BINANCE_STREAMS.items():
                            if stream == binance:
                                price = float(price_str)
                                if price > 0:
                                    self.cache.set(internal, price)
                                break
                except (json.JSONDecodeError, ValueError, KeyError):
                    pass

        self._connected = False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PROVIDER STATS (exposed for /providers + /health)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_circuit_breakers: Dict[str, CircuitBreaker] = {}
_td_throttle = TDThrottle()


def get_provider_stats() -> Dict:
    """Return stats for all providers."""
    stats = {}
    for name, cb in _circuit_breakers.items():
        stats[name] = cb.get_stats()
    stats["td_throttle"] = {
        "credits_60s": _td_throttle.credits_last_60s(),
        "daily_credits": _td_throttle.daily_credits,
    }
    return stats


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# OPEN TRADE STATUSES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
OPEN_STATUSES = ["EXECUTED", "OPEN", "ONIAI_OPEN", "VIRTUAL_OPEN"]
HTTP_TIMEOUT = 8.0


class ServerSimulator:
    """
    4-provider asset-class-routed JIT trade monitor.
    Binance WS for crypto, TD for forex, FH for stocks, FMP for indices/commodities.
    """

    def __init__(self):
        global _circuit_breakers
        self.telegram = TelegramBot()
        self.client = httpx.AsyncClient(timeout=HTTP_TIMEOUT)
        self.running = True
        self.trade_state: Dict[int, Dict] = {}
        self.cache = PriceCache()
        self.binance_ws = BinanceWSManager(self.cache)

        # Circuit breakers
        _circuit_breakers = {
            "twelvedata": CircuitBreaker("twelvedata", failure_threshold=5, cooldown=60),
            "finnhub": CircuitBreaker("finnhub", failure_threshold=5, cooldown=30),
            "fmp": CircuitBreaker("fmp", failure_threshold=5, cooldown=60),
            "binance_ws": CircuitBreaker("binance_ws", failure_threshold=3, cooldown=30),
        }
        self.cb = _circuit_breakers

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ASSET-CLASS PRICE ROUTER
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def fetch_prices(self, tickers: Set[str]) -> Dict[str, float]:
        """Route each symbol to the best provider by asset class."""
        prices: Dict[str, float] = {}
        remaining: Set[str] = set()

        # ── 1. Check cache first (includes Binance WS prices) ──
        for sym in tickers:
            cached = self.cache.get(sym)
            if cached:
                prices[sym] = cached
            else:
                remaining.add(sym)

        if not remaining:
            return prices

        # ── 2. Classify remaining by asset class ──
        forex_metals = set()
        stocks = set()
        indices_commodities = set()
        crypto_rest = set()  # crypto WS miss — use REST fallback

        for sym in remaining:
            cls = classify_symbol(sym)
            if cls in ("FOREX", "METAL"):
                forex_metals.add(sym)
            elif cls == "STOCK":
                stocks.add(sym)
            elif cls in ("INDEX", "COMMODITY"):
                indices_commodities.add(sym)
            elif cls == "CRYPTO":
                crypto_rest.add(sym)
            else:
                indices_commodities.add(sym)  # default to FMP

        # ── 3. Fetch by provider (parallel where possible) ──
        tasks = []

        if forex_metals:
            tasks.append(self._fetch_forex_metals(forex_metals))
        if stocks:
            tasks.append(self._fetch_stocks(stocks))
        if indices_commodities:
            tasks.append(self._fetch_indices_commodities(indices_commodities))
        if crypto_rest:
            tasks.append(self._fetch_crypto_rest(crypto_rest))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, dict):
                prices.update(result)
                for sym, price in result.items():
                    self.cache.set(sym, price)

        # ── 4. Stale fallback for anything still missing ──
        still_missing = tickers - set(prices.keys())
        for sym in still_missing:
            stale = self.cache.get_stale(sym)
            if stale:
                prices[sym] = stale

        return prices

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # PROVIDER: TwelveData (forex + metals)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def _fetch_forex_metals(self, tickers: Set[str]) -> Dict[str, float]:
        """Primary: TwelveData batch. Fallback: FMP batch."""
        prices = {}

        # Try TwelveData
        if TWELVEDATA_API_KEY and self.cb["twelvedata"].allow_request():
            td_prices = await self._td_batch(tickers)
            prices.update(td_prices)

        # FMP fallback for misses
        missing = tickers - set(prices.keys())
        if missing and FMP_API_KEY and self.cb["fmp"].allow_request():
            fmp_prices = await self._fmp_batch(missing)
            prices.update(fmp_prices)

        return prices

    async def _td_batch(self, tickers: Set[str]) -> Dict[str, float]:
        """Batched TwelveData /price call."""
        td_symbols = []
        td_to_internal = {}
        for t in tickers:
            td_sym = TD_MAP.get(t)
            if td_sym:
                td_symbols.append(td_sym)
                td_to_internal[td_sym] = t

        if not td_symbols:
            return {}

        try:
            resp = await self.client.get(
                "https://api.twelvedata.com/price",
                params={"symbol": ",".join(td_symbols), "apikey": TWELVEDATA_API_KEY},
            )
            _td_throttle.record_credits(len(td_symbols))
            self.cb["twelvedata"].extra_stats["batch_calls"] = \
                self.cb["twelvedata"].extra_stats.get("batch_calls", 0) + 1

            if resp.status_code != 200:
                self.cb["twelvedata"].record_failure()
                return {}

            data = resp.json()
            if isinstance(data, dict) and data.get("status") == "error":
                self.cb["twelvedata"].record_failure()
                return {}

            prices = {}
            if len(td_symbols) == 1:
                price_str = data.get("price")
                if price_str:
                    try:
                        p = float(price_str)
                        if p > 0:
                            prices[td_to_internal[td_symbols[0]]] = p
                            self.cb["twelvedata"].record_success()
                    except (ValueError, TypeError):
                        pass
            else:
                for td_sym, td_data in data.items():
                    internal = td_to_internal.get(td_sym)
                    if not internal:
                        internal = TD_REVERSE.get(td_sym)
                    if not internal or not isinstance(td_data, dict):
                        continue
                    if td_data.get("status") == "error":
                        continue
                    try:
                        p = float(td_data.get("price", 0))
                        if p > 0:
                            prices[internal] = p
                            self.cb["twelvedata"].record_success()
                    except (ValueError, TypeError):
                        pass

            return prices

        except httpx.TimeoutException:
            self.cb["twelvedata"].record_failure()
            logger.debug(f"TD timeout for {len(td_symbols)} symbols")
            return {}
        except Exception as e:
            self.cb["twelvedata"].record_failure()
            logger.debug(f"TD error: {e}")
            return {}

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # PROVIDER: Finnhub (US stocks)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def _fetch_stocks(self, tickers: Set[str]) -> Dict[str, float]:
        """Primary: Finnhub individual. Fallback: FMP batch."""
        prices = {}

        if FINNHUB_API_KEY and self.cb["finnhub"].allow_request():
            for sym in tickers:
                fh_sym = FH_MAP.get(sym)
                if not fh_sym:
                    continue
                try:
                    resp = await self.client.get(
                        "https://finnhub.io/api/v1/quote",
                        params={"symbol": fh_sym, "token": FINNHUB_API_KEY},
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        p = data.get("c", 0)
                        if p and float(p) > 0:
                            prices[sym] = float(p)
                            self.cb["finnhub"].record_success()
                        else:
                            self.cb["finnhub"].record_failure()
                    else:
                        self.cb["finnhub"].record_failure()
                except (httpx.TimeoutException, httpx.RequestError):
                    self.cb["finnhub"].record_failure()

        # FMP fallback for misses
        missing = tickers - set(prices.keys())
        if missing and FMP_API_KEY and self.cb["fmp"].allow_request():
            fmp_prices = await self._fmp_batch(missing)
            prices.update(fmp_prices)

        return prices

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # PROVIDER: FMP (indices, commodities, universal fallback)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def _fetch_indices_commodities(self, tickers: Set[str]) -> Dict[str, float]:
        """Primary: FMP batch. Fallback: TwelveData."""
        prices = {}

        if FMP_API_KEY and self.cb["fmp"].allow_request():
            prices = await self._fmp_batch(tickers)

        # TD fallback for misses
        missing = tickers - set(prices.keys())
        if missing and TWELVEDATA_API_KEY and self.cb["twelvedata"].allow_request():
            td_prices = await self._td_batch(missing)
            prices.update(td_prices)

        return prices

    async def _fmp_batch(self, tickers: Set[str]) -> Dict[str, float]:
        """Batched FMP /quote call."""
        fmp_symbols = []
        fmp_to_internal = {}
        for t in tickers:
            fmp_sym = FMP_MAP.get(t, t)
            fmp_symbols.append(fmp_sym)
            fmp_to_internal[fmp_sym] = t

        if not fmp_symbols:
            return {}

        try:
            resp = await self.client.get(
                f"https://financialmodelingprep.com/api/v3/quote/{','.join(fmp_symbols)}",
                params={"apikey": FMP_API_KEY},
            )
            self.cb["fmp"].extra_stats["batch_calls"] = \
                self.cb["fmp"].extra_stats.get("batch_calls", 0) + 1

            if resp.status_code != 200:
                self.cb["fmp"].record_failure()
                return {}

            data = resp.json()
            if not isinstance(data, list):
                self.cb["fmp"].record_failure()
                return {}

            prices = {}
            for item in data:
                fmp_sym = item.get("symbol", "")
                internal = fmp_to_internal.get(fmp_sym)
                if not internal:
                    internal = FMP_REVERSE.get(fmp_sym)
                if not internal:
                    continue
                p = item.get("price", 0)
                if p and float(p) > 0:
                    prices[internal] = float(p)
                    self.cb["fmp"].record_success()

            return prices

        except httpx.TimeoutException:
            self.cb["fmp"].record_failure()
            return {}
        except Exception as e:
            self.cb["fmp"].record_failure()
            logger.debug(f"FMP error: {e}")
            return {}

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # PROVIDER: Binance REST (crypto fallback)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def _fetch_crypto_rest(self, tickers: Set[str]) -> Dict[str, float]:
        """REST fallback when Binance WS is disconnected."""
        prices = {}
        for sym in tickers:
            binance_sym = BINANCE_STREAMS.get(sym, "").upper()
            if not binance_sym:
                continue
            try:
                resp = await self.client.get(
                    f"{BINANCE_REST_URL}/api/v3/ticker/price",
                    params={"symbol": binance_sym.upper()},
                )
                if resp.status_code == 200:
                    data = resp.json()
                    p = float(data.get("price", 0))
                    if p > 0:
                        prices[sym] = p
            except Exception:
                pass
        return prices

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # TRADE STATE + EVALUATION (preserved from previous version)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def _init_trade_state(self, trade: Trade) -> Dict:
        return {
            "current_sl": trade.stop_loss or 0,
            "high_water": trade.entry_price or 0,
            "low_water": trade.entry_price or 0,
            "tp1_hit": False,
            "partial_closed": False,
            "original_sl": trade.stop_loss or 0,
        }

    async def _check_executed_trade(self, trade: Trade, price: float, db: Session):
        if not price or not trade.entry_price:
            return
        if trade.id not in self.trade_state:
            self.trade_state[trade.id] = self._init_trade_state(trade)

        state = self.trade_state[trade.id]
        entry = trade.entry_price
        sl = state["current_sl"]
        tp1 = trade.take_profit_1 or 0
        tp2 = getattr(trade, "take_profit_2", 0) or 0
        is_long = (trade.direction or "").upper() in ["LONG", "BUY"]
        pip_size, pip_value = get_pip_info(trade.symbol)

        if is_long and price > state["high_water"]:
            state["high_water"] = price
        if not is_long and (price < state["low_water"] or state["low_water"] == 0):
            state["low_water"] = price

        if not state["tp1_hit"] and tp1 > 0:
            tp1_reached = (is_long and price >= tp1) or (not is_long and price <= tp1)
            if tp1_reached:
                state["tp1_hit"] = True
                buffer = 2 * pip_size
                state["current_sl"] = (entry + buffer) if is_long else (entry - buffer)
                sl = state["current_sl"]
                logger.info(f"SIM #{trade.id} | TP1 HIT @ {tp1} | SL → BE: {sl}")

        if state["tp1_hit"]:
            trail_dist = abs(entry - state["original_sl"])
            if trail_dist > 0:
                if is_long:
                    new_sl = state["high_water"] - trail_dist
                    if new_sl > state["current_sl"]:
                        state["current_sl"] = new_sl
                        sl = new_sl
                else:
                    new_sl = state["low_water"] + trail_dist
                    if new_sl < state["current_sl"]:
                        state["current_sl"] = new_sl
                        sl = new_sl

        hit_sl = sl > 0 and ((is_long and price <= sl) or (not is_long and price >= sl))
        hit_tp2 = state["tp1_hit"] and tp2 > 0 and (
            (is_long and price >= tp2) or (not is_long and price <= tp2)
        )
        time_expired = self._is_time_expired(trade)

        if not (hit_sl or hit_tp2 or time_expired):
            return

        if hit_tp2:
            exit_price, reason = tp2, "SRV_TP2_HIT"
        elif hit_sl:
            exit_price = sl
            reason = "SRV_TRAILING_SL" if state["tp1_hit"] else "SRV_SL_HIT"
        else:
            exit_price, reason = price, "SRV_TIME_EXIT"

        pnl_pips = ((exit_price - entry) if is_long else (entry - exit_price)) / pip_size
        pnl_dollars = pnl_pips * pip_value * (trade.lot_size or 0.1)

        await self._close_trade(
            trade, db,
            exit_price=exit_price, pnl_pips=pnl_pips, pnl_dollars=pnl_dollars,
            reason=reason, status="SRV_CLOSED",
            mfe_pips=abs(
                (state["high_water"] - entry) if is_long else (entry - state["low_water"])
            ) / pip_size if pip_size else None,
            mae_pips=abs(
                (entry - state["low_water"]) if is_long else (state["high_water"] - entry)
            ) / pip_size if pip_size else None,
        )

    async def _check_virtual_trade(self, trade: Trade, price: float, db: Session):
        if not price or not trade.entry_price:
            return
        entry = trade.entry_price
        sl = trade.stop_loss or 0
        tp1 = trade.take_profit_1 or 0
        is_long = (trade.direction or "").upper() in ["LONG", "BUY"]
        pip_size, pip_value = get_pip_info(trade.symbol)

        hit_sl = sl > 0 and ((is_long and price <= sl) or (not is_long and price >= sl))
        hit_tp = tp1 > 0 and ((is_long and price >= tp1) or (not is_long and price <= tp1))
        time_expired = self._is_time_expired(trade)

        if not (hit_sl or hit_tp or time_expired):
            return

        if hit_tp:
            exit_price, reason = tp1, "ONIAI_TP1_HIT"
        elif hit_sl:
            exit_price, reason = sl, "ONIAI_SL_HIT"
        else:
            exit_price, reason = price, "ONIAI_TIME_EXIT"

        pnl_pips = ((exit_price - entry) if is_long else (entry - exit_price)) / pip_size
        pnl_dollars = pnl_pips * pip_value * (trade.lot_size or 0.1)

        await self._close_trade(
            trade, db,
            exit_price=exit_price, pnl_pips=pnl_pips, pnl_dollars=pnl_dollars,
            reason=reason, status="ONIAI_CLOSED",
        )

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # UNIFIED TRADE CLOSE
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def _close_trade(
        self, trade: Trade, db: Session, *,
        exit_price: float, pnl_pips: float, pnl_dollars: float,
        reason: str, status: str,
        mfe_pips: float = None, mae_pips: float = None,
    ):
        trade.exit_price = round(exit_price, 5)
        trade.pnl_dollars = round(pnl_dollars, 2)
        trade.pnl_pips = round(pnl_pips, 1)
        trade.close_reason = reason
        trade.status = status
        trade.closed_at = datetime.now(timezone.utc)

        desk_state = db.query(DeskState).filter(DeskState.desk_id == trade.desk_id).first()
        if desk_state:
            desk_state.open_positions = max(0, (desk_state.open_positions or 0) - 1)
            desk_state.daily_pnl = (desk_state.daily_pnl or 0) + pnl_dollars
            if pnl_dollars < 0:
                desk_state.daily_loss = (desk_state.daily_loss or 0) + pnl_dollars
                desk_state.consecutive_losses = (desk_state.consecutive_losses or 0) + 1
            else:
                desk_state.consecutive_losses = 0
        db.commit()

        if trade.id in self.trade_state:
            del self.trade_state[trade.id]

        tag = "\U0001f916 OniAI" if "ONIAI" in reason else "\U0001f5a5\ufe0f SIM"
        hold_str = ""
        if trade.opened_at:
            hold_min = (trade.closed_at - trade.opened_at).total_seconds() / 60
            hold_str = f" | Held: {hold_min:.0f}m"
        logger.info(
            f"{tag} CLOSED | #{trade.id} | {trade.symbol} {trade.direction} | "
            f"Entry: {trade.entry_price} | Exit: {exit_price} | "
            f"PnL: ${pnl_dollars:+.2f} ({pnl_pips:+.1f}p) | {reason}{hold_str}"
        )

        try:
            from app.services.ml_data_logger import MLDataLogger
            duration = 0
            if trade.opened_at:
                duration = (trade.closed_at - trade.opened_at).total_seconds() / 60
            MLDataLogger.label_outcome(
                db=db, trade_id=trade.id, signal_id=trade.signal_id,
                exit_price=exit_price, exit_reason=reason,
                pnl_pips=round(pnl_pips, 1), pnl_dollars=round(pnl_dollars, 2),
                trade_duration_minutes=duration,
                max_favorable_pips=mfe_pips, max_adverse_pips=mae_pips,
            )
            db.commit()
        except Exception as e:
            logger.debug(f"ML outcome log failed: {e}")

        try:
            await self.telegram.notify_trade_exit(
                symbol=trade.symbol, desk_id=trade.desk_id,
                pnl=pnl_dollars, reason=reason,
            )
        except Exception as e:
            logger.error(f"Telegram close failed: {e}")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # TIME EXIT HELPERS
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def _is_time_expired(self, trade: Trade) -> bool:
        desk = DESKS.get(trade.desk_id, {})
        max_hold = desk.get("max_hold_hours", 24)
        if not trade.opened_at:
            return False
        elapsed = (datetime.now(timezone.utc) - trade.opened_at).total_seconds()
        return elapsed >= max_hold * 3600

    async def _force_time_exit(self, trade: Trade, db: Session):
        exit_price = trade.entry_price or 0
        is_oniai = trade.status in ("ONIAI_OPEN", "VIRTUAL_OPEN")
        reason = "ONIAI_TIME_EXIT_NOPRICE" if is_oniai else "SRV_TIME_EXIT_NOPRICE"
        status = "ONIAI_CLOSED" if is_oniai else "SRV_CLOSED"
        elapsed = 0
        if trade.opened_at:
            elapsed = (datetime.now(timezone.utc) - trade.opened_at).total_seconds() / 3600
        logger.warning(
            f"TIME EXIT (no price) | #{trade.id} | {trade.symbol} "
            f"{trade.direction} | Held {elapsed:.1f}h"
        )
        await self._close_trade(
            trade, db, exit_price=exit_price, pnl_pips=0.0, pnl_dollars=0.0,
            reason=reason, status=status,
        )

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # MAIN LOOP
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def run(self):
        """4-provider JIT loop with adaptive throttling."""
        # Start Binance WebSocket
        await self.binance_ws.start()

        logger.info(
            f"4-Provider JIT Simulator started | "
            f"TD: {'ON' if TWELVEDATA_API_KEY else 'OFF'} | "
            f"FH: {'ON' if FINNHUB_API_KEY else 'OFF'} | "
            f"FMP: {'ON' if FMP_API_KEY else 'OFF'} | "
            f"Binance WS: ON"
        )

        while self.running:
            try:
                db = SessionLocal()
                try:
                    open_trades = (
                        db.query(Trade)
                        .filter(Trade.status.in_(OPEN_STATUSES))
                        .all()
                    )

                    if not open_trades:
                        await asyncio.sleep(15)
                        continue

                    tickers = set(t.symbol for t in open_trades if t.symbol)
                    prices = await self.fetch_prices(tickers)

                    for trade in open_trades:
                        price = prices.get(trade.symbol)
                        is_virtual = trade.status in ("ONIAI_OPEN", "VIRTUAL_OPEN")

                        if price:
                            if is_virtual:
                                await self._check_virtual_trade(trade, price, db)
                            else:
                                await self._check_executed_trade(trade, price, db)
                        else:
                            if self._is_time_expired(trade):
                                await self._force_time_exit(trade, db)

                    logger.debug(
                        f"JIT cycle | {len(open_trades)} trades | "
                        f"{len(prices)}/{len(tickers)} prices | "
                        f"TD credits/60s: {_td_throttle.credits_last_60s()} | "
                        f"BinanceWS: {'✓' if self.binance_ws.connected else '✗'}"
                    )

                finally:
                    db.close()

            except Exception as e:
                logger.error(f"Simulator error: {e}", exc_info=True)

            # Adaptive sleep based on TD credit usage
            interval = _td_throttle.get_interval()
            await asyncio.sleep(interval)

    async def stop(self):
        self.running = False
        await self.binance_ws.stop()
        await self.client.aclose()
        await self.telegram.close()
        logger.info("4-Provider JIT Simulator stopped")
