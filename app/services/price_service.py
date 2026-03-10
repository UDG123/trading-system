"""
Multi-Provider Price Service
Fetches live prices from multiple APIs with automatic fallback.

Priority order:
1. TwelveData (primary — best forex/commodity coverage)
2. Finnhub (backup — good forex, free tier generous)
3. Alpha Vantage (backup — slower but reliable)
4. CoinGecko (crypto only — free, no key needed)
5. ExchangeRate API (forex only — free, no key needed)

Caches prices for 15 seconds to avoid rate limits.
Falls through providers automatically on failure.
"""
import os
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional

import httpx

logger = logging.getLogger("TradingSystem.PriceService")

# ── API Keys (set in Railway variables) ──
TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY", "")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")
ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY", "")

# ── Cache ──
_price_cache: Dict[str, Dict] = {}
_cache_expiry: Dict[str, datetime] = {}
CACHE_TTL_SECONDS = 15

# ── Provider stats (for monitoring) ──
_provider_stats: Dict[str, Dict] = {
    "twelvedata": {"success": 0, "fail": 0},
    "finnhub": {"success": 0, "fail": 0},
    "alphavantage": {"success": 0, "fail": 0},
    "yahoo": {"success": 0, "fail": 0},
    "coingecko": {"success": 0, "fail": 0},
    "exchangerate": {"success": 0, "fail": 0},
}

# ── Symbol Mappings ──

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
    # US stocks
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
    # US stocks (free tier)
    "TSLA": "TSLA", "AAPL": "AAPL", "MSFT": "MSFT",
    "NVDA": "NVDA", "AMZN": "AMZN", "META": "META",
    "GOOGL": "GOOGL", "NFLX": "NFLX", "AMD": "AMD",
}

COINGECKO_MAP = {
    "BTCUSD": "bitcoin", "ETHUSD": "ethereum",
    "SOLUSD": "solana", "XRPUSD": "ripple",
    "LINKUSD": "chainlink",
}

# Yahoo Finance symbol mapping (covers EVERYTHING — no API key needed)
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

# Forex pairs that can use ExchangeRate API
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


class PriceService:
    """
    Multi-provider price fetcher with automatic fallback.
    Call get_price(symbol) — it tries each provider in order.
    """

    def __init__(self):
        self.client = httpx.AsyncClient(timeout=8.0)

    async def close(self):
        await self.client.aclose()

    # ─────────────────────────────────────────
    # PUBLIC API
    # ─────────────────────────────────────────

    async def get_price(self, symbol: str) -> Optional[float]:
        """Get current price for a symbol. Tries providers in priority order."""
        now = datetime.now(timezone.utc)

        # Check cache first
        if symbol in _price_cache:
            if _cache_expiry.get(symbol, now) > now:
                return _price_cache[symbol].get("price")

        # Try providers in order
        price = None
        providers = self._get_providers(symbol)

        for provider_name, provider_fn in providers:
            try:
                price = await provider_fn(symbol)
                if price and price > 0:
                    _price_cache[symbol] = {"price": price, "time": now}
                    _cache_expiry[symbol] = now + timedelta(seconds=CACHE_TTL_SECONDS)
                    _provider_stats[provider_name]["success"] += 1
                    return price
            except Exception as e:
                _provider_stats[provider_name]["fail"] += 1
                logger.debug(f"Price provider {provider_name} failed for {symbol}: {e}")
                continue

        # Return stale cache if all providers fail
        if symbol in _price_cache:
            logger.warning(f"All providers failed for {symbol}, using stale cache")
            return _price_cache[symbol].get("price")

        logger.debug(f"No price available for {symbol} (all providers exhausted)")
        return None

    async def get_prices_batch(self, symbols: list) -> Dict[str, float]:
        """Get prices for multiple symbols efficiently."""
        prices = {}
        for sym in symbols:
            price = await self.get_price(sym)
            if price:
                prices[sym] = price
        return prices

    def get_stats(self) -> Dict:
        """Return provider success/fail stats for monitoring."""
        return _provider_stats.copy()

    # ─────────────────────────────────────────
    # PROVIDER SELECTION
    # ─────────────────────────────────────────

    def _get_providers(self, symbol: str) -> list:
        """Return ordered list of (name, fn) providers for a symbol."""
        providers = []

        # TwelveData — primary for everything
        if TWELVEDATA_API_KEY and symbol in TWELVEDATA_MAP:
            providers.append(("twelvedata", self._fetch_twelvedata))

        # Finnhub — good forex/crypto backup
        if FINNHUB_API_KEY and symbol in FINNHUB_MAP:
            providers.append(("finnhub", self._fetch_finnhub))

        # Alpha Vantage — forex backup
        if ALPHA_VANTAGE_API_KEY and symbol in EXCHANGERATE_PAIRS:
            providers.append(("alphavantage", self._fetch_alphavantage))

        # Yahoo Finance — covers everything, no key needed
        if symbol in YAHOO_MAP:
            providers.append(("yahoo", self._fetch_yahoo))

        # CoinGecko — crypto (free, no key)
        if symbol in COINGECKO_MAP:
            providers.append(("coingecko", self._fetch_coingecko))

        # ExchangeRate — forex (free, no key)
        if symbol in EXCHANGERATE_PAIRS:
            providers.append(("exchangerate", self._fetch_exchangerate))

        return providers

    # ─────────────────────────────────────────
    # PROVIDER IMPLEMENTATIONS
    # ─────────────────────────────────────────

    async def _fetch_twelvedata(self, symbol: str) -> Optional[float]:
        """TwelveData — 800 req/day free, 8/min."""
        td_symbol = TWELVEDATA_MAP.get(symbol, symbol)

        # Try /price endpoint first
        try:
            resp = await self.client.get(
                "https://api.twelvedata.com/price",
                params={"symbol": td_symbol, "apikey": TWELVEDATA_API_KEY},
            )
            if resp.status_code == 200:
                data = resp.json()
                if "price" in data and data.get("status") != "error":
                    price = float(data["price"])
                    if price > 0:
                        return price
        except (ValueError, TypeError):
            pass

        # Fallback: try /quote endpoint (different format)
        try:
            resp = await self.client.get(
                "https://api.twelvedata.com/quote",
                params={"symbol": td_symbol, "apikey": TWELVEDATA_API_KEY},
            )
            if resp.status_code == 200:
                data = resp.json()
                close = data.get("close") or data.get("previous_close")
                if close and data.get("status") != "error":
                    price = float(close)
                    if price > 0:
                        return price
        except (ValueError, TypeError):
            pass

        return None

    async def _fetch_finnhub(self, symbol: str) -> Optional[float]:
        """Finnhub — 60 req/min free."""
        fh_symbol = FINNHUB_MAP.get(symbol)
        if not fh_symbol:
            return None
        resp = await self.client.get(
            "https://finnhub.io/api/v1/quote",
            params={"symbol": fh_symbol, "token": FINNHUB_API_KEY},
        )
        if resp.status_code == 200:
            data = resp.json()
            price = data.get("c", 0)  # current price
            if price and price > 0:
                return float(price)
        return None

    async def _fetch_alphavantage(self, symbol: str) -> Optional[float]:
        """Alpha Vantage — 25 req/day free (slow, last resort for forex)."""
        pair = EXCHANGERATE_PAIRS.get(symbol)
        if not pair:
            return None
        from_cur, to_cur = pair
        resp = await self.client.get(
            "https://www.alphavantage.co/query",
            params={
                "function": "CURRENCY_EXCHANGE_RATE",
                "from_currency": from_cur,
                "to_currency": to_cur,
                "apikey": ALPHA_VANTAGE_API_KEY,
            },
        )
        if resp.status_code == 200:
            data = resp.json()
            rate_data = data.get("Realtime Currency Exchange Rate", {})
            rate = rate_data.get("5. Exchange Rate")
            if rate:
                return float(rate)
        return None

    async def _fetch_yahoo(self, symbol: str) -> Optional[float]:
        """Yahoo Finance — free, no key, covers everything."""
        yf_symbol = YAHOO_MAP.get(symbol)
        if not yf_symbol:
            return None
        try:
            resp = await self.client.get(
                f"https://query1.finance.yahoo.com/v8/finance/chart/{yf_symbol}",
                params={"interval": "1m", "range": "1d"},
                headers={"User-Agent": "OniQuant/1.0"},
            )
            if resp.status_code == 200:
                data = resp.json()
                result = data.get("chart", {}).get("result", [])
                if result:
                    meta = result[0].get("meta", {})
                    price = meta.get("regularMarketPrice")
                    if price and price > 0:
                        return float(price)
        except Exception:
            pass
        return None

    async def _fetch_coingecko(self, symbol: str) -> Optional[float]:
        """CoinGecko — free, no key, 10-30 req/min. Crypto only."""
        coin_id = COINGECKO_MAP.get(symbol)
        if not coin_id:
            return None
        resp = await self.client.get(
            "https://api.coingecko.com/api/v3/simple/price",
            params={"ids": coin_id, "vs_currencies": "usd"},
        )
        if resp.status_code == 200:
            data = resp.json()
            price = data.get(coin_id, {}).get("usd")
            if price:
                return float(price)
        return None

    async def _fetch_exchangerate(self, symbol: str) -> Optional[float]:
        """ExchangeRate API — free, no key, ~1500 req/month. Forex only."""
        pair = EXCHANGERATE_PAIRS.get(symbol)
        if not pair:
            return None
        from_cur, to_cur = pair
        resp = await self.client.get(
            f"https://open.er-api.com/v6/latest/{from_cur}",
        )
        if resp.status_code == 200:
            data = resp.json()
            rates = data.get("rates", {})
            rate = rates.get(to_cur)
            if rate:
                return float(rate)
        return None
