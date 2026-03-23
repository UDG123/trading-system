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
    "US30": "DJI", "US500": "SPX", "US100": "NDX", "NAS100": "NDX",
    "GER40": "DAX", "UK100": "UKX", "JPN225": "NI225",
    "XAUUSD": "XAU/USD", "XAGUSD": "XAG/USD",
    "WTIUSD": "WTI/USD", "XCUUSD": "COPPER",
    "BTCUSD": "BTC/USD", "ETHUSD": "ETH/USD", "SOLUSD": "SOL/USD",
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
