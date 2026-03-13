"""
NovaQuant Price Feed — Continuous Multi-Provider Price Service
Maintains a live price cache updated in the background.

Architecture:
  - Background async loop polls prices continuously
  - ACTIVE tier (open trades): every 10 seconds
  - WARM tier (desk symbols): every 30 seconds
  - Provider rotation spreads load across rate limits
  - Instant reads from cache — no API call at query time
  - Stale detection alerts when prices go dark

Rate Limit Budget:
  - TwelveData: 8/min (primary forex/commodities)
  - Finnhub: 60/min (forex/stocks/crypto backup)
  - CoinGecko: 10/min (crypto — free, no key)
  - Yahoo: ~unlimited but throttles (universal fallback)
  - ExchangeRate: ~50/hour (forex — free, no key)
  - Alpha Vantage: 25/day (emergency only)
"""
import os
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Set, List

import httpx

logger = logging.getLogger("TradingSystem.PriceService")

TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY", "")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY", "")

ACTIVE_INTERVAL = 10
WARM_INTERVAL = 30
STALE_THRESHOLD = 120
PROVIDER_COOLDOWN = 5

_provider_stats: Dict[str, Dict] = {
    "twelvedata": {"success": 0, "fail": 0, "last_used": None},
    "finnhub": {"success": 0, "fail": 0, "last_used": None},
    "alphavantage": {"success": 0, "fail": 0, "last_used": None},
    "yahoo": {"success": 0, "fail": 0, "last_used": None},
    "coingecko": {"success": 0, "fail": 0, "last_used": None},
    "exchangerate": {"success": 0, "fail": 0, "last_used": None},
}

TWELVEDATA_MAP = {
    "EURUSD": "EUR/USD", "USDJPY": "USD/JPY", "GBPUSD": "GBP/USD",
    "USDCHF": "USD/CHF", "AUDUSD": "AUD/USD", "USDCAD": "USD/CAD",
    "NZDUSD": "NZD/USD", "EURJPY": "EUR/JPY", "GBPJPY": "GBP/JPY",
    "AUDJPY": "AUD/JPY", "EURGBP": "EUR/GBP", "EURAUD": "EUR/AUD",
    "GBPAUD": "GBP/AUD", "EURCHF": "EUR/CHF", "CADJPY": "CAD/JPY",
    "NZDJPY": "NZD/JPY", "GBPCAD": "GBP/CAD", "AUDCAD": "AUD/CAD",
    "AUDNZD": "AUD/NZD", "CHFJPY": "CHF/JPY", "EURNZD": "EUR/NZD",
    "GBPNZD": "GBP/NZD",
    "XAUUSD": "XAU/USD", "XAGUSD": "XAG/USD",
    "WTIUSD": "CL", "XCUUSD": "HG",
    "US30": "DJI", "US100": "IXIC", "NAS100": "IXIC",
    "BTCUSD": "BTC/USD", "ETHUSD": "ETH/USD",
    "SOLUSD": "SOL/USD", "XRPUSD": "XRP/USD", "LINKUSD": "LINK/USD",
    "TSLA": "TSLA", "AAPL": "AAPL", "MSFT": "MSFT",
    "NVDA": "NVDA", "AMZN": "AMZN", "META": "META",
    "GOOGL": "GOOGL", "NFLX": "NFLX", "AMD": "AMD",
}

FINNHUB_MAP = {
    "EURUSD": "OANDA:EUR_USD", "USDJPY": "OANDA:USD_JPY",
    "GBPUSD": "OANDA:GBP_USD", "USDCHF": "OANDA:USD_CHF",
    "AUDUSD": "OANDA:AUD_USD", "USDCAD": "OANDA:USD_CAD",
    "NZDUSD": "OANDA:NZD_USD", "EURJPY": "OANDA:EUR_JPY",
    "GBPJPY": "OANDA:GBP_JPY", "AUDJPY": "OANDA:AUD_JPY",
    "EURGBP": "OANDA:EUR_GBP", "EURAUD": "OANDA:EUR_AUD",
    "GBPAUD": "OANDA:GBP_AUD", "EURCHF": "OANDA:EUR_CHF",
    "CADJPY": "OANDA:CAD_JPY", "NZDJPY": "OANDA:NZD_JPY",
    "GBPCAD": "OANDA:GBP_CAD", "AUDCAD": "OANDA:AUD_CAD",
    "AUDNZD": "OANDA:AUD_NZD", "CHFJPY": "OANDA:CHF_JPY",
    "EURNZD": "OANDA:EUR_NZD", "GBPNZD": "OANDA:GBP_NZD",
    "BTCUSD": "BINANCE:BTCUSDT", "ETHUSD": "BINANCE:ETHUSDT",
    "SOLUSD": "BINANCE:SOLUSDT", "XRPUSD": "BINANCE:XRPUSDT",
    "LINKUSD": "BINANCE:LINKUSDT",
    "TSLA": "TSLA", "AAPL": "AAPL", "MSFT": "MSFT",
    "NVDA": "NVDA", "AMZN": "AMZN", "META": "META",
    "GOOGL": "GOOGL", "NFLX": "NFLX", "AMD": "AMD",
}

COINGECKO_MAP = {
    "BTCUSD": "bitcoin", "ETHUSD": "ethereum",
    "SOLUSD": "solana", "XRPUSD": "ripple",
    "LINKUSD": "chainlink",
}

YAHOO_MAP = {
    "EURUSD": "EURUSD=X", "USDJPY": "USDJPY=X", "GBPUSD": "GBPUSD=X",
    "USDCHF": "USDCHF=X", "AUDUSD": "AUDUSD=X", "USDCAD": "USDCAD=X",
    "NZDUSD": "NZDUSD=X", "EURJPY": "EURJPY=X", "GBPJPY": "GBPJPY=X",
    "AUDJPY": "AUDJPY=X", "EURGBP": "EURGBP=X", "EURAUD": "EURAUD=X",
    "GBPAUD": "GBPAUD=X", "EURCHF": "EURCHF=X", "CADJPY": "CADJPY=X",
    "NZDJPY": "NZDJPY=X", "GBPCAD": "GBPCAD=X", "AUDCAD": "AUDCAD=X",
    "AUDNZD": "AUDNZD=X", "CHFJPY": "CHFJPY=X", "EURNZD": "EURNZD=X",
    "GBPNZD": "GBPNZD=X",
    "XAUUSD": "GC=F", "XAGUSD": "SI=F",
    "WTIUSD": "CL=F", "XCUUSD": "HG=F",
    "US30": "YM=F", "US100": "NQ=F", "NAS100": "NQ=F",
    "BTCUSD": "BTC-USD", "ETHUSD": "ETH-USD",
    "SOLUSD": "SOL-USD", "XRPUSD": "XRP-USD", "LINKUSD": "LINK-USD",
    "TSLA": "TSLA", "AAPL": "AAPL", "MSFT": "MSFT",
    "NVDA": "NVDA", "AMZN": "AMZN", "META": "META",
    "GOOGL": "GOOGL", "NFLX": "NFLX", "AMD": "AMD",
}

EXCHANGERATE_PAIRS = {
    "EURUSD": ("EUR", "USD"), "USDJPY": ("USD", "JPY"),
    "GBPUSD": ("GBP", "USD"), "USDCHF": ("USD", "CHF"),
    "AUDUSD": ("AUD", "USD"), "USDCAD": ("USD", "CAD"),
    "NZDUSD": ("NZD", "USD"), "EURGBP": ("EUR", "GBP"),
    "EURJPY": ("EUR", "JPY"), "GBPJPY": ("GBP", "JPY"),
    "AUDJPY": ("AUD", "JPY"), "EURAUD": ("EUR", "AUD"),
    "GBPAUD": ("GBP", "AUD"), "EURCHF": ("EUR", "CHF"),
    "CADJPY": ("CAD", "JPY"), "NZDJPY": ("NZD", "JPY"),
    "GBPCAD": ("GBP", "CAD"), "AUDCAD": ("AUD", "CAD"),
    "AUDNZD": ("AUD", "NZD"), "CHFJPY": ("CHF", "JPY"),
    "EURNZD": ("EUR", "NZD"), "GBPNZD": ("GBP", "NZD"),
}

CRYPTO_SYMBOLS = {"BTCUSD", "ETHUSD", "SOLUSD", "XRPUSD", "LINKUSD"}
EQUITY_SYMBOLS = {"TSLA", "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "NFLX", "AMD"}
INDEX_SYMBOLS = {"US30", "US100", "NAS100"}
COMMODITY_SYMBOLS = {"XAUUSD", "XAGUSD", "WTIUSD", "XCUUSD"}


class PriceService:
    """Continuous price feed with background polling. Reads are instant from cache."""

    def __init__(self):
        self.client = httpx.AsyncClient(timeout=8.0)
        self.running = False
        self._feed_task = None
        self._cache: Dict[str, Dict] = {}
        self._active_symbols: Set[str] = set()
        self._provider_last_call: Dict[str, datetime] = {}
        self._td_calls_this_minute: int = 0
        self._td_minute_start: datetime = datetime.now(timezone.utc)
        self._last_provider: str = ""

    # ━━━ PUBLIC API ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def get_price(self, symbol: str) -> Optional[float]:
        """Get latest price. Returns from cache, falls back to live fetch."""
        cached = self._cache.get(symbol)
        now = datetime.now(timezone.utc)

        if cached:
            age = (now - cached["updated_at"]).total_seconds()
            if age < STALE_THRESHOLD:
                return cached["price"]
            # Stale — try live, fall back to stale
            price = await self._fetch_price(symbol)
            if price:
                return price
            logger.warning(f"Stale price for {symbol} ({age:.0f}s), using cached {cached['price']}")
            return cached["price"]

        # No cache — live fetch
        return await self._fetch_price(symbol)

    async def get_prices_batch(self, symbols: list) -> Dict[str, float]:
        prices = {}
        for sym in symbols:
            price = await self.get_price(sym)
            if price:
                prices[sym] = price
        return prices

    def get_cached_price(self, symbol: str) -> Optional[float]:
        """Sync read from cache. No API call."""
        cached = self._cache.get(symbol)
        return cached["price"] if cached else None

    def get_cache_age(self, symbol: str) -> Optional[float]:
        cached = self._cache.get(symbol)
        if cached:
            return (datetime.now(timezone.utc) - cached["updated_at"]).total_seconds()
        return None

    def set_active_symbols(self, symbols: Set[str]):
        """Update which symbols have open trades (polled at ACTIVE_INTERVAL)."""
        if symbols != self._active_symbols:
            added = symbols - self._active_symbols
            removed = self._active_symbols - symbols
            if added:
                logger.info(f"Price feed ACTIVE +{added}")
            if removed:
                logger.info(f"Price feed ACTIVE -{removed}")
            self._active_symbols = symbols.copy()

    def get_stats(self) -> Dict:
        now = datetime.now(timezone.utc)
        stale = fresh = 0
        for data in self._cache.values():
            if (now - data["updated_at"]).total_seconds() > STALE_THRESHOLD:
                stale += 1
            else:
                fresh += 1
        return {
            "providers": _provider_stats.copy(),
            "cache_size": len(self._cache),
            "fresh": fresh, "stale": stale,
            "active_symbols": len(self._active_symbols),
            "feed_running": self.running,
        }

    def get_stale_symbols(self) -> List[str]:
        now = datetime.now(timezone.utc)
        return [
            f"{sym} ({(now - data['updated_at']).total_seconds():.0f}s)"
            for sym, data in self._cache.items()
            if (now - data["updated_at"]).total_seconds() > STALE_THRESHOLD
        ]

    # ━━━ BACKGROUND FEED ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def start_feed(self):
        if self.running:
            return
        self.running = True
        self._feed_task = asyncio.create_task(self._feed_loop())
        logger.info(
            f"NovaQuant price feed started | Active: {ACTIVE_INTERVAL}s | "
            f"Warm: {WARM_INTERVAL}s | Stale: {STALE_THRESHOLD}s"
        )

    async def stop_feed(self):
        self.running = False
        if self._feed_task:
            self._feed_task.cancel()
            try:
                await self._feed_task
            except asyncio.CancelledError:
                pass
        logger.info("NovaQuant price feed stopped")

    async def _feed_loop(self):
        cycle = 0
        while self.running:
            try:
                # ACTIVE tier: open trade symbols every cycle (~10s)
                if self._active_symbols:
                    for sym in list(self._active_symbols):
                        await self._update_symbol(sym)
                        await asyncio.sleep(0.5)

                # WARM tier: all other symbols every 3rd cycle (~30s)
                if cycle % 3 == 0:
                    warm = set(TWELVEDATA_MAP.keys()) | set(FINNHUB_MAP.keys())
                    warm -= self._active_symbols
                    for sym in list(warm):
                        await self._update_symbol(sym)
                        await asyncio.sleep(0.5)

                # Health log every 30 cycles (~5 min)
                if cycle % 30 == 0 and cycle > 0:
                    stats = self.get_stats()
                    stale = self.get_stale_symbols()
                    logger.info(
                        f"Price feed | {stats['fresh']} fresh, {stats['stale']} stale | "
                        f"Active: {stats['active_symbols']} | "
                        f"{'Stale: ' + ', '.join(stale[:5]) if stale else 'All fresh'}"
                    )

                cycle += 1
            except Exception as e:
                logger.error(f"Price feed error: {e}")

            await asyncio.sleep(ACTIVE_INTERVAL)

    async def _update_symbol(self, symbol: str):
        price = await self._fetch_price(symbol)
        if price and price > 0:
            self._cache[symbol] = {
                "price": price,
                "updated_at": datetime.now(timezone.utc),
                "provider": self._last_provider,
            }

    # ━━━ PROVIDER ENGINE ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def _fetch_price(self, symbol: str) -> Optional[float]:
        providers = self._get_providers(symbol)
        for name, fn in providers:
            if not self._can_call(name):
                continue
            try:
                price = await fn(symbol)
                if price and price > 0:
                    self._record(name, True)
                    self._last_provider = name
                    return price
                self._record(name, False)
            except Exception as e:
                self._record(name, False)
                logger.debug(f"{name} failed for {symbol}: {e}")
        return None

    def _get_providers(self, symbol: str) -> list:
        p = []
        if symbol in CRYPTO_SYMBOLS:
            if symbol in COINGECKO_MAP: p.append(("coingecko", self._fetch_coingecko))
            if FINNHUB_API_KEY and symbol in FINNHUB_MAP: p.append(("finnhub", self._fetch_finnhub))
            if symbol in YAHOO_MAP: p.append(("yahoo", self._fetch_yahoo))
        elif symbol in EQUITY_SYMBOLS or symbol in INDEX_SYMBOLS:
            if FINNHUB_API_KEY and symbol in FINNHUB_MAP: p.append(("finnhub", self._fetch_finnhub))
            if TWELVEDATA_API_KEY and symbol in TWELVEDATA_MAP: p.append(("twelvedata", self._fetch_twelvedata))
            if symbol in YAHOO_MAP: p.append(("yahoo", self._fetch_yahoo))
        elif symbol in COMMODITY_SYMBOLS:
            if TWELVEDATA_API_KEY and symbol in TWELVEDATA_MAP: p.append(("twelvedata", self._fetch_twelvedata))
            if symbol in YAHOO_MAP: p.append(("yahoo", self._fetch_yahoo))
            if FINNHUB_API_KEY and symbol in FINNHUB_MAP: p.append(("finnhub", self._fetch_finnhub))
        else:  # forex
            if TWELVEDATA_API_KEY and symbol in TWELVEDATA_MAP: p.append(("twelvedata", self._fetch_twelvedata))
            if FINNHUB_API_KEY and symbol in FINNHUB_MAP: p.append(("finnhub", self._fetch_finnhub))
            if symbol in YAHOO_MAP: p.append(("yahoo", self._fetch_yahoo))
            if symbol in EXCHANGERATE_PAIRS: p.append(("exchangerate", self._fetch_exchangerate))
        if not p and symbol in YAHOO_MAP:
            p.append(("yahoo", self._fetch_yahoo))
        return p

    def _can_call(self, provider: str) -> bool:
        now = datetime.now(timezone.utc)
        if provider == "twelvedata":
            if now - self._td_minute_start > timedelta(minutes=1):
                self._td_calls_this_minute = 0
                self._td_minute_start = now
            return self._td_calls_this_minute < 7
        last = self._provider_last_call.get(provider)
        if not last:
            return True
        elapsed = (now - last).total_seconds()
        limits = {"coingecko": 6, "exchangerate": 60, "alphavantage": 300, "finnhub": 1, "yahoo": 1}
        return elapsed >= limits.get(provider, 1)

    def _record(self, provider: str, success: bool):
        now = datetime.now(timezone.utc)
        self._provider_last_call[provider] = now
        if provider == "twelvedata":
            self._td_calls_this_minute += 1
        key = "success" if success else "fail"
        _provider_stats[provider][key] += 1
        _provider_stats[provider]["last_used"] = now

    # ━━━ PROVIDER IMPLEMENTATIONS ━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def _fetch_twelvedata(self, symbol: str) -> Optional[float]:
        td = TWELVEDATA_MAP.get(symbol, symbol)
        try:
            r = await self.client.get("https://api.twelvedata.com/price", params={"symbol": td, "apikey": TWELVEDATA_API_KEY})
            if r.status_code == 200:
                d = r.json()
                if "price" in d and d.get("status") != "error":
                    p = float(d["price"])
                    if p > 0: return p
        except (ValueError, TypeError): pass
        try:
            r = await self.client.get("https://api.twelvedata.com/quote", params={"symbol": td, "apikey": TWELVEDATA_API_KEY})
            if r.status_code == 200:
                d = r.json()
                c = d.get("close") or d.get("previous_close")
                if c and d.get("status") != "error":
                    p = float(c)
                    if p > 0: return p
        except (ValueError, TypeError): pass
        return None

    async def _fetch_finnhub(self, symbol: str) -> Optional[float]:
        fh = FINNHUB_MAP.get(symbol)
        if not fh: return None
        r = await self.client.get("https://finnhub.io/api/v1/quote", params={"symbol": fh, "token": FINNHUB_API_KEY})
        if r.status_code == 200:
            p = r.json().get("c", 0)
            if p and p > 0: return float(p)
        return None

    async def _fetch_alphavantage(self, symbol: str) -> Optional[float]:
        pair = EXCHANGERATE_PAIRS.get(symbol)
        if not pair: return None
        f, t = pair
        r = await self.client.get("https://www.alphavantage.co/query", params={"function": "CURRENCY_EXCHANGE_RATE", "from_currency": f, "to_currency": t, "apikey": ALPHA_VANTAGE_API_KEY})
        if r.status_code == 200:
            rate = r.json().get("Realtime Currency Exchange Rate", {}).get("5. Exchange Rate")
            if rate: return float(rate)
        return None

    async def _fetch_yahoo(self, symbol: str) -> Optional[float]:
        yf = YAHOO_MAP.get(symbol)
        if not yf: return None
        try:
            r = await self.client.get(f"https://query1.finance.yahoo.com/v8/finance/chart/{yf}", params={"interval": "1m", "range": "1d"}, headers={"User-Agent": "OniQuant/1.0"})
            if r.status_code == 200:
                result = r.json().get("chart", {}).get("result", [])
                if result:
                    p = result[0].get("meta", {}).get("regularMarketPrice")
                    if p and p > 0: return float(p)
        except Exception: pass
        return None

    async def _fetch_coingecko(self, symbol: str) -> Optional[float]:
        cid = COINGECKO_MAP.get(symbol)
        if not cid: return None
        r = await self.client.get("https://api.coingecko.com/api/v3/simple/price", params={"ids": cid, "vs_currencies": "usd"})
        if r.status_code == 200:
            p = r.json().get(cid, {}).get("usd")
            if p: return float(p)
        return None

    async def _fetch_exchangerate(self, symbol: str) -> Optional[float]:
        pair = EXCHANGERATE_PAIRS.get(symbol)
        if not pair: return None
        f, t = pair
        r = await self.client.get(f"https://open.er-api.com/v6/latest/{f}")
        if r.status_code == 200:
            rate = r.json().get("rates", {}).get(t)
            if rate: return float(rate)
        return None

    async def close(self):
        await self.stop_feed()
        await self.client.aclose()
