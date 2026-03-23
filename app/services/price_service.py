"""
PriceService — On-Demand 2-Provider Price Fetch
Used by the pipeline to get a live market price at signal entry time.
NOT a continuous feed. Single get_price() call per signal.

Routing:
  Crypto         → Binance REST (no key, 1 call)
  Everything else → TwelveData (1 credit)
"""
import os
import logging
from typing import Optional

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
    "EURUSD": "EUR/USD", "USDJPY": "USD/JPY", "GBPUSD": "GBP/USD",
    "USDCHF": "USD/CHF", "AUDUSD": "AUD/USD", "USDCAD": "USD/CAD",
    "NZDUSD": "NZD/USD", "EURJPY": "EUR/JPY", "GBPJPY": "GBP/JPY",
    "AUDJPY": "AUD/JPY", "EURGBP": "EUR/GBP", "EURAUD": "EUR/AUD",
    "GBPAUD": "GBP/AUD", "EURCHF": "EUR/CHF", "CADJPY": "CAD/JPY",
    "NZDJPY": "NZD/JPY", "GBPCAD": "GBP/CAD", "AUDCAD": "AUD/CAD",
    "AUDNZD": "AUD/NZD", "CHFJPY": "CHF/JPY", "EURNZD": "EUR/NZD",
    "GBPNZD": "GBP/NZD",
    "XAUUSD": "XAU/USD", "XAGUSD": "XAG/USD",
    "WTIUSD": "CL", "XCUUSD": "COPPER",
    "US30": "DJI", "US100": "IXIC", "NAS100": "IXIC",
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
