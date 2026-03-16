"""
PriceService — On-Demand 4-Provider Price Fetch
Used by the pipeline to get a live market price at signal entry time.
NOT a continuous feed. Single get_price() call per signal.

Same 4-provider routing as the JIT simulator:
  Crypto    → Binance REST (no key, 1 call)
  Forex     → TwelveData (1 credit)  → FMP fallback
  Metals    → TwelveData (1 credit)  → FMP fallback
  Stocks    → Finnhub (1 call)       → FMP fallback
  Indices   → FMP (1 call)           → TwelveData fallback
  Oil/Cu    → FMP (1 call)           → TwelveData fallback
"""
import os
import logging
from datetime import datetime, timezone
from typing import Dict, Optional

import httpx

logger = logging.getLogger("TradingSystem.PriceService")

TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY", "")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")
FMP_API_KEY = os.getenv("FMP_API_KEY", "")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ASSET CLASSIFICATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CRYPTO = {"BTCUSD", "ETHUSD", "SOLUSD", "XRPUSD", "LINKUSD"}
STOCKS = {"TSLA", "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "NFLX", "AMD"}
INDICES = {"US30", "US100", "NAS100"}
COMMODITIES = {"WTIUSD", "XCUUSD"}
METALS = {"XAUUSD", "XAGUSD"}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SYMBOL MAPS (same as simulator)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

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

FH_MAP = {
    "TSLA": "TSLA", "AAPL": "AAPL", "MSFT": "MSFT",
    "NVDA": "NVDA", "AMZN": "AMZN", "META": "META",
    "GOOGL": "GOOGL", "NFLX": "NFLX", "AMD": "AMD",
}

FMP_MAP = {
    "US30": "^DJI", "US100": "^GSPC", "NAS100": "^IXIC",
    "WTIUSD": "CLUSD", "XCUUSD": "HGUSD",
    "XAUUSD": "XAUUSD", "XAGUSD": "XAGUSD",
    "EURUSD": "EURUSD", "USDJPY": "USDJPY", "GBPUSD": "GBPUSD",
    "USDCHF": "USDCHF", "AUDUSD": "AUDUSD", "USDCAD": "USDCAD",
    "NZDUSD": "NZDUSD", "EURJPY": "EURJPY", "GBPJPY": "GBPJPY",
    "AUDJPY": "AUDJPY", "EURGBP": "EURGBP", "EURAUD": "EURAUD",
    "GBPAUD": "GBPAUD", "EURCHF": "EURCHF", "CADJPY": "CADJPY",
    "NZDJPY": "NZDJPY", "GBPCAD": "GBPCAD", "AUDCAD": "AUDCAD",
    "AUDNZD": "AUDNZD", "CHFJPY": "CHFJPY", "EURNZD": "EURNZD",
    "GBPNZD": "GBPNZD",
    "TSLA": "TSLA", "AAPL": "AAPL", "MSFT": "MSFT",
    "NVDA": "NVDA", "AMZN": "AMZN", "META": "META",
    "GOOGL": "GOOGL", "NFLX": "NFLX", "AMD": "AMD",
}

BINANCE_MAP = {
    "BTCUSD": "BTCUSDT", "ETHUSD": "ETHUSDT",
    "SOLUSD": "SOLUSDT", "XRPUSD": "XRPUSDT", "LINKUSD": "LINKUSDT",
}


class PriceService:
    """
    On-demand price fetcher for pipeline entry price.
    Routes by asset class. Single call per signal, not continuous.
    """

    def __init__(self):
        self.client = httpx.AsyncClient(timeout=8.0)

    async def close(self):
        await self.client.aclose()

    async def get_price(self, symbol: str) -> Optional[float]:
        """
        Fetch a single live price. Routes to the best provider by asset class.
        Returns None if all providers fail.
        """
        sym = symbol.upper()

        if sym in CRYPTO:
            return await self._try_chain(sym, [self._binance, self._fmp])
        elif sym in STOCKS:
            return await self._try_chain(sym, [self._finnhub, self._fmp])
        elif sym in INDICES or sym in COMMODITIES:
            return await self._try_chain(sym, [self._fmp, self._twelvedata])
        elif sym in METALS:
            return await self._try_chain(sym, [self._twelvedata, self._fmp])
        else:
            # Forex (default)
            return await self._try_chain(sym, [self._twelvedata, self._fmp])

    async def _try_chain(self, symbol: str, providers: list) -> Optional[float]:
        """Try providers in order, return first success."""
        for provider_fn in providers:
            try:
                price = await provider_fn(symbol)
                if price and price > 0:
                    return price
            except Exception as e:
                logger.debug(f"Price fetch failed for {symbol}: {e}")
        return None

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # PROVIDERS
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def _twelvedata(self, symbol: str) -> Optional[float]:
        """TwelveData single-symbol price fetch."""
        if not TWELVEDATA_API_KEY:
            return None
        td_sym = TD_MAP.get(symbol)
        if not td_sym:
            return None
        try:
            resp = await self.client.get(
                "https://api.twelvedata.com/price",
                params={"symbol": td_sym, "apikey": TWELVEDATA_API_KEY},
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") != "error" and "price" in data:
                    return float(data["price"])
        except (httpx.TimeoutException, httpx.RequestError):
            pass
        return None

    async def _finnhub(self, symbol: str) -> Optional[float]:
        """Finnhub single-symbol quote (US stocks only)."""
        if not FINNHUB_API_KEY:
            return None
        fh_sym = FH_MAP.get(symbol)
        if not fh_sym:
            return None
        try:
            resp = await self.client.get(
                "https://finnhub.io/api/v1/quote",
                params={"symbol": fh_sym, "token": FINNHUB_API_KEY},
            )
            if resp.status_code == 200:
                data = resp.json()
                price = data.get("c", 0)
                if price and float(price) > 0:
                    return float(price)
        except (httpx.TimeoutException, httpx.RequestError):
            pass
        return None

    async def _fmp(self, symbol: str) -> Optional[float]:
        """FMP single-symbol quote (universal fallback)."""
        if not FMP_API_KEY:
            return None
        fmp_sym = FMP_MAP.get(symbol, symbol)
        try:
            resp = await self.client.get(
                f"https://financialmodelingprep.com/api/v3/quote/{fmp_sym}",
                params={"apikey": FMP_API_KEY},
            )
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list) and data:
                    price = data[0].get("price", 0)
                    if price and float(price) > 0:
                        return float(price)
        except (httpx.TimeoutException, httpx.RequestError):
            pass
        return None

    async def _binance(self, symbol: str) -> Optional[float]:
        """Binance REST single-symbol price (crypto, no key needed)."""
        binance_sym = BINANCE_MAP.get(symbol)
        if not binance_sym:
            return None
        try:
            resp = await self.client.get(
                "https://api.binance.com/api/v3/ticker/price",
                params={"symbol": binance_sym},
            )
            if resp.status_code == 200:
                data = resp.json()
                price = float(data.get("price", 0))
                if price > 0:
                    return price
        except (httpx.TimeoutException, httpx.RequestError):
            pass
        return None
