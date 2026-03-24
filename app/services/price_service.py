"""
PriceService — On-Demand Price Fetch (TwelveData + Binance)
Used by the pipeline to get a live market price at signal entry time.
NOT a continuous feed. Single get_price() call per signal.

Also provides get_prices_batch() for the sim exit checker to fetch
all open-position prices in a SINGLE TwelveData API call.

Routing:
  Single price: Crypto → Binance REST (no key, 1 call)
                Everything else → TwelveData (1 credit)
  Batch prices: ALL symbols → TwelveData multi-symbol (1 credit total)
"""
import os
import logging
from typing import Optional, Dict, List

import httpx

logger = logging.getLogger("TradingSystem.PriceService")

TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY", "")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ASSET CLASSIFICATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CRYPTO = {"BTCUSD", "ETHUSD", "SOLUSD", "XRPUSD", "LINKUSD"}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SYMBOL MAPS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

TD_MAP = {
    "US30": "DIA", "US500": "SPY", "US100": "QQQ", "NAS100": "QQQ",
    "GER40": "DAX", "UK100": "UKX", "JPN225": "NI225",
    "XAUUSD": "XAU/USD", "XAGUSD": "XAG/USD",
    "WTIUSD": "WTI/USD", "XCUUSD": "COPPER",
    "BTCUSD": "BTC/USD", "ETHUSD": "ETH/USD", "SOLUSD": "SOL/USD",
    "XRPUSD": "XRP/USD", "LINKUSD": "LINK/USD",
    "EURUSD": "EUR/USD", "GBPUSD": "GBP/USD", "USDJPY": "USD/JPY",
    "USDCHF": "USD/CHF", "AUDUSD": "AUD/USD", "USDCAD": "USD/CAD",
    "NZDUSD": "NZD/USD", "EURGBP": "EUR/GBP", "EURJPY": "EUR/JPY",
    "GBPJPY": "GBP/JPY", "AUDJPY": "AUD/JPY", "NZDJPY": "NZD/JPY",
    "EURAUD": "EUR/AUD", "EURNZD": "EUR/NZD", "GBPAUD": "GBP/AUD",
    "GBPNZD": "GBP/NZD", "GBPCAD": "GBP/CAD", "EURCAD": "EUR/CAD",
    "AUDCAD": "AUD/CAD", "AUDNZD": "AUD/NZD", "CADCHF": "CAD/CHF",
    "CADJPY": "CAD/JPY", "CHFJPY": "CHF/JPY", "NZDCAD": "NZD/CAD",
    "EURCHF": "EUR/CHF", "GBPCHF": "GBP/CHF", "AUDCHF": "AUD/CHF",
    "NZDCHF": "NZD/CHF",
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
        Crypto → Binance REST. Everything else → TwelveData.
        Returns None if provider fails.
        """
        sym = symbol.upper()

        if sym in CRYPTO:
            return await self._binance(sym)
        else:
            return await self._twelvedata(sym)

    async def get_prices_batch(self, symbols: List[str]) -> Dict[str, float]:
        """
        Fetch prices for multiple symbols in a SINGLE TwelveData API call.
        Uses the multi-symbol /price endpoint: ?symbol=EUR/USD,GBP/USD,...

        Returns dict mapping original symbol → price (only successful fetches).
        Costs 1 API credit regardless of how many symbols are requested.
        """
        if not symbols or not TWELVEDATA_API_KEY:
            return {}

        # Build mapping: original symbol → TwelveData symbol
        td_to_original: Dict[str, str] = {}
        for sym in symbols:
            td_sym = TD_MAP.get(sym.upper(), sym.upper())
            td_to_original[td_sym] = sym

        td_symbols_csv = ",".join(td_to_original.keys())

        try:
            resp = await self.client.get(
                "https://api.twelvedata.com/price",
                params={"symbol": td_symbols_csv, "apikey": TWELVEDATA_API_KEY},
            )
            if resp.status_code != 200:
                logger.warning(f"Batch price fetch HTTP {resp.status_code}")
                return {}

            data = resp.json()

            prices: Dict[str, float] = {}

            if len(td_to_original) == 1:
                # Single symbol: TwelveData returns {"price": "1.0850"}
                td_sym = next(iter(td_to_original))
                original_sym = td_to_original[td_sym]
                if data.get("status") != "error" and "price" in data:
                    prices[original_sym] = float(data["price"])
            else:
                # Multiple symbols: {"EUR/USD": {"price": "1.0850"}, ...}
                for td_sym, original_sym in td_to_original.items():
                    entry = data.get(td_sym, {})
                    if isinstance(entry, dict) and "price" in entry:
                        try:
                            prices[original_sym] = float(entry["price"])
                        except (ValueError, TypeError):
                            pass
                    # Skip symbols that returned errors ({"code": 400, ...})

            logger.info(
                f"Sim exit checker: fetched {len(prices)} prices in 1 batch call "
                f"({len(td_to_original)} requested)"
            )
            return prices

        except (httpx.TimeoutException, httpx.RequestError) as e:
            logger.warning(f"Batch price fetch failed: {e}")
            return {}

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # PROVIDERS
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def _twelvedata(self, symbol: str) -> Optional[float]:
        """TwelveData single-symbol price fetch."""
        if not TWELVEDATA_API_KEY:
            return None
        td_sym = TD_MAP.get(symbol, symbol)
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
