"""
TwelveData Enrichment Service
Fetches market context for each signal: ATR, RSI, volume profile,
volatility regime, session detection, and intermarket data.
"""
import logging
from datetime import datetime, timezone
from typing import Dict, Optional

import httpx

from app.config import TWELVEDATA_API_KEY

logger = logging.getLogger("TradingSystem.Enricher")

BASE_URL = "https://api.twelvedata.com"

# TwelveData symbol mapping (our internal → TwelveData format)
TD_SYMBOLS = {
    "EURUSD": "EUR/USD",
    "USDJPY": "USD/JPY",
    "GBPUSD": "GBP/USD",
    "USDCHF": "USD/CHF",
    "AUDUSD": "AUD/USD",
    "USDCAD": "USD/CAD",
    "NZDUSD": "NZD/USD",
    "EURJPY": "EUR/JPY",
    "GBPJPY": "GBP/JPY",
    "AUDJPY": "AUD/JPY",
    "XAUUSD": "XAU/USD",
    "BTCUSD": "BTC/USD",
    "ETHUSD": "ETH/USD",
    "US30": "DJI",
    "US100": "IXIC",
    "NAS100": "IXIC",
    "TSLA": "TSLA",
}

# Intermarket instruments for macro context
INTERMARKET = {
    "DXY": "DX",       # Dollar index
    "VIX": "VIX",       # Volatility index
    "US10Y": "TNXF",    # 10Y bond yield
    "OIL": "CL",        # Crude oil
    "BTCDOM": "BTC/USD", # BTC as proxy for crypto sentiment
}

# Timeframe mapping to TwelveData intervals
TD_INTERVALS = {
    "1M": "1min", "5M": "5min", "15M": "15min", "30M": "30min",
    "1H": "1h", "4H": "4h", "D": "1day", "W": "1week",
}


class TwelveDataEnricher:
    """Enriches signals with market data from TwelveData API."""

    def __init__(self):
        self.api_key = TWELVEDATA_API_KEY
        self.client = httpx.AsyncClient(timeout=10.0)

    async def enrich(self, symbol: str, timeframe: str, price: float) -> Dict:
        """
        Fetch market context for a signal. Returns enrichment dict.
        Falls back to empty/default values if API unavailable.
        """
        if not self.api_key:
            logger.warning("No TwelveData API key — returning defaults")
            return self._default_enrichment(symbol, price)

        td_symbol = TD_SYMBOLS.get(symbol, symbol)
        td_interval = TD_INTERVALS.get(timeframe, "1h")

        enrichment = {}

        try:
            # Fetch technical indicators in parallel
            atr_data, rsi_data, quote_data = await self._fetch_parallel(
                td_symbol, td_interval
            )

            # ── ATR (Average True Range) — volatility measure ──
            if atr_data and "value" in atr_data:
                atr = float(atr_data["value"])
                enrichment["atr"] = round(atr, 5)
                enrichment["atr_pct"] = round((atr / price) * 100, 4)
            else:
                enrichment["atr"] = None
                enrichment["atr_pct"] = None

            # ── RSI — momentum/overbought/oversold ──
            if rsi_data and "value" in rsi_data:
                rsi = float(rsi_data["value"])
                enrichment["rsi"] = round(rsi, 2)
                enrichment["rsi_zone"] = self._classify_rsi(rsi)
            else:
                enrichment["rsi"] = None
                enrichment["rsi_zone"] = "UNKNOWN"

            # ── Quote data — spread and volume ──
            if quote_data:
                enrichment["bid"] = float(quote_data.get("bid", 0) or 0)
                enrichment["ask"] = float(quote_data.get("ask", 0) or 0)
                enrichment["spread"] = round(
                    enrichment["ask"] - enrichment["bid"], 5
                )
                enrichment["volume"] = int(quote_data.get("volume", 0) or 0)
            else:
                enrichment["bid"] = None
                enrichment["ask"] = None
                enrichment["spread"] = None
                enrichment["volume"] = None

        except Exception as e:
            logger.error(f"TwelveData enrichment failed for {symbol}: {e}")
            return self._default_enrichment(symbol, price)

        # ── Volatility regime detection ──
        enrichment["volatility_regime"] = self._detect_volatility_regime(
            enrichment.get("atr_pct")
        )

        # ── Session detection ──
        enrichment["active_session"] = self._detect_session()
        enrichment["is_kill_zone"] = self._is_kill_zone()
        enrichment["kill_zone_type"] = self._kill_zone_type()

        # ── Intermarket snapshot ──
        enrichment["intermarket"] = await self._fetch_intermarket()

        logger.info(
            f"Enriched {symbol}: ATR={enrichment.get('atr')} "
            f"RSI={enrichment.get('rsi')} "
            f"Regime={enrichment.get('volatility_regime')} "
            f"Session={enrichment.get('active_session')}"
        )

        return enrichment

    async def _fetch_parallel(self, symbol: str, interval: str):
        """Fetch ATR, RSI, and quote data."""
        atr_data = None
        rsi_data = None
        quote_data = None

        try:
            # ATR
            resp = await self.client.get(
                f"{BASE_URL}/atr",
                params={
                    "symbol": symbol,
                    "interval": interval,
                    "time_period": 14,
                    "apikey": self.api_key,
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                if "values" in data and data["values"]:
                    atr_data = data["values"][0]
        except Exception as e:
            logger.debug(f"ATR fetch failed: {e}")

        try:
            # RSI
            resp = await self.client.get(
                f"{BASE_URL}/rsi",
                params={
                    "symbol": symbol,
                    "interval": interval,
                    "time_period": 14,
                    "apikey": self.api_key,
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                if "values" in data and data["values"]:
                    rsi_data = data["values"][0]
        except Exception as e:
            logger.debug(f"RSI fetch failed: {e}")

        try:
            # Real-time quote
            resp = await self.client.get(
                f"{BASE_URL}/quote",
                params={
                    "symbol": symbol,
                    "apikey": self.api_key,
                },
            )
            if resp.status_code == 200:
                quote_data = resp.json()
        except Exception as e:
            logger.debug(f"Quote fetch failed: {e}")

        return atr_data, rsi_data, quote_data

    async def _fetch_intermarket(self) -> Dict:
        """Fetch intermarket snapshot: DXY, VIX, bonds, oil."""
        result = {}
        if not self.api_key:
            return result

        for name, symbol in INTERMARKET.items():
            try:
                resp = await self.client.get(
                    f"{BASE_URL}/quote",
                    params={"symbol": symbol, "apikey": self.api_key},
                )
                if resp.status_code == 200:
                    data = resp.json()
                    result[name] = {
                        "price": float(data.get("close", 0) or 0),
                        "change_pct": float(data.get("percent_change", 0) or 0),
                    }
            except Exception:
                result[name] = None

        return result

    def _classify_rsi(self, rsi: float) -> str:
        """Classify RSI into trading zones."""
        if rsi >= 80:
            return "EXTREME_OVERBOUGHT"
        if rsi >= 70:
            return "OVERBOUGHT"
        if rsi >= 55:
            return "BULLISH"
        if rsi >= 45:
            return "NEUTRAL"
        if rsi >= 30:
            return "BEARISH"
        if rsi >= 20:
            return "OVERSOLD"
        return "EXTREME_OVERSOLD"

    def _detect_volatility_regime(self, atr_pct: Optional[float]) -> str:
        """
        Classify market volatility regime from ATR as % of price.
        Thresholds calibrated for forex (adjust per asset class).
        """
        if atr_pct is None:
            return "UNKNOWN"
        if atr_pct > 1.5:
            return "HIGH_VOLATILITY"
        if atr_pct > 0.5:
            return "TRENDING"
        if atr_pct > 0.2:
            return "NORMAL"
        return "LOW_VOLATILITY"

    def _detect_session(self) -> str:
        """Detect which trading session is currently active."""
        now = datetime.now(timezone.utc)
        hour = now.hour

        # UTC-based session windows
        # Sydney: 21:00-06:00 UTC
        # Tokyo:  00:00-09:00 UTC
        # London: 07:00-16:00 UTC
        # NY:     12:00-21:00 UTC

        sessions = []
        if 7 <= hour < 16:
            sessions.append("LONDON")
        if 12 <= hour < 21:
            sessions.append("NEW_YORK")
        if hour >= 21 or hour < 6:
            sessions.append("SYDNEY")
        if 0 <= hour < 9:
            sessions.append("TOKYO")

        if "LONDON" in sessions and "NEW_YORK" in sessions:
            return "LONDON_NY_OVERLAP"
        if sessions:
            return sessions[0]
        return "OFF_HOURS"

    def _is_kill_zone(self) -> bool:
        """Check if we're in a kill zone (high-probability window)."""
        kz_type = self._kill_zone_type()
        return kz_type != "NONE"

    def _kill_zone_type(self) -> str:
        """
        Identify specific kill zone.
        London Open: 07:00-08:00 UTC
        NY Open: 12:00-13:00 UTC
        Overlap: 12:00-16:00 UTC
        """
        now = datetime.now(timezone.utc)
        hour = now.hour
        minute = now.minute

        # London/NY overlap (strongest)
        if 12 <= hour < 16:
            return "OVERLAP"
        # London open first 60 minutes
        if hour == 7 or (hour == 8 and minute < 30):
            return "LONDON_OPEN"
        # NY open first 60 minutes
        if hour == 12 or (hour == 13 and minute < 30):
            return "NY_OPEN"
        return "NONE"

    def _default_enrichment(self, symbol: str, price: float) -> Dict:
        """Return default enrichment when API is unavailable."""
        return {
            "atr": None,
            "atr_pct": None,
            "rsi": None,
            "rsi_zone": "UNKNOWN",
            "bid": None,
            "ask": None,
            "spread": None,
            "volume": None,
            "volatility_regime": "UNKNOWN",
            "active_session": self._detect_session(),
            "is_kill_zone": self._is_kill_zone(),
            "kill_zone_type": self._kill_zone_type(),
            "intermarket": {},
        }

    async def close(self):
        """Close the HTTP client."""
        await self.client.aclose()
