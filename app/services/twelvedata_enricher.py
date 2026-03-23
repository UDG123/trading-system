"""
TwelveData Enrichment Service
Fetches market context for each signal: ATR, RSI, volume profile,
volatility regime, session detection, Hurst exponent, and intermarket data.

Includes per-symbol enrichment cache (60s TTL) to avoid blowing
TwelveData's 55 req/min rate limit when multiple desks enrich the
same symbol within a single signal burst.
"""
import time
import logging
from datetime import datetime, timezone
from typing import Dict, Optional, List

import httpx
import numpy as np

from app.config import TWELVEDATA_API_KEY

logger = logging.getLogger("TradingSystem.Enricher")

BASE_URL = "https://api.twelvedata.com"

# ── Per-symbol enrichment cache (60s TTL) ──
# Prevents duplicate TwelveData calls when the same symbol is enriched
# for multiple desks within the same signal burst.
_enrichment_cache: Dict[str, dict] = {}
CACHE_TTL_SECONDS = 60

# TwelveData symbol mapping (our internal → TwelveData format)
SYMBOL_MAP = {
    "US30": "DJI",
    "US500": "SPX",
    "US100": "NDX",
    "NAS100": "NDX",
    "GER40": "DAX",
    "UK100": "UKX",
    "JPN225": "NI225",
    "XAUUSD": "XAU/USD",
    "XAGUSD": "XAG/USD",
    "BTCUSD": "BTC/USD",
    "ETHUSD": "ETH/USD",
    "SOLUSD": "SOL/USD",
    "EURUSD": "EUR/USD",
    "GBPUSD": "GBP/USD",
    "USDJPY": "USD/JPY",
    "USDCHF": "USD/CHF",
    "AUDUSD": "AUD/USD",
    "USDCAD": "USD/CAD",
    "NZDUSD": "NZD/USD",
    "EURGBP": "EUR/GBP",
    "EURJPY": "EUR/JPY",
    "GBPJPY": "GBP/JPY",
    "AUDJPY": "AUD/JPY",
    "NZDJPY": "NZD/JPY",
    "EURAUD": "EUR/AUD",
    "EURNZD": "EUR/NZD",
    "GBPAUD": "GBP/AUD",
    "GBPNZD": "GBP/NZD",
    "GBPCAD": "GBP/CAD",
    "EURCAD": "EUR/CAD",
    "AUDCAD": "AUD/CAD",
    "AUDNZD": "AUD/NZD",
    "CADCHF": "CAD/CHF",
    "CADJPY": "CAD/JPY",
    "CHFJPY": "CHF/JPY",
    "NZDCAD": "NZD/CAD",
    "EURCHF": "EUR/CHF",
    "GBPCHF": "GBP/CHF",
    "AUDCHF": "AUD/CHF",
    "NZDCHF": "NZD/CHF",
    "WTIUSD": "WTI/USD",
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
        Uses per-symbol cache (60s TTL) to avoid duplicate API calls.
        """
        # ── Cache check ──
        now = time.time()
        cached = _enrichment_cache.get(symbol)
        if cached and (now - cached["timestamp"]) < CACHE_TTL_SECONDS:
            age = now - cached["timestamp"]
            logger.info(f"Enrichment cache HIT for {symbol} (age: {age:.1f}s)")
            return cached["data"]

        if not self.api_key:
            logger.warning("No TwelveData API key — returning defaults")
            return self._default_enrichment(symbol, price)

        td_symbol = SYMBOL_MAP.get(symbol, symbol)
        td_interval = TD_INTERVALS.get(timeframe, "1h")

        enrichment = {}

        try:
            # Fetch technical indicators in parallel
            atr_data, rsi_data, adx_data, quote_data, ema50_data, ema200_data = await self._fetch_parallel(
                td_symbol, td_interval
            )

            # ── ATR (Average True Range) — volatility measure ──
            if atr_data and ("atr" in atr_data or "value" in atr_data):
                atr = float(atr_data.get("atr") or atr_data.get("value"))
                enrichment["atr"] = round(atr, 5)
                enrichment["atr_pct"] = round((atr / price) * 100, 4) if price > 0 else None
            else:
                enrichment["atr"] = None
                enrichment["atr_pct"] = None

            # ── RSI — momentum/overbought/oversold ──
            if rsi_data and ("rsi" in rsi_data or "value" in rsi_data):
                rsi = float(rsi_data.get("rsi") or rsi_data.get("value"))
                enrichment["rsi"] = round(rsi, 2)
                enrichment["rsi_zone"] = self._classify_rsi(rsi)
            else:
                enrichment["rsi"] = None
                enrichment["rsi_zone"] = "UNKNOWN"

            # ── ADX (Average Directional Index) — trend strength ──
            if adx_data and ("adx" in adx_data or "value" in adx_data):
                adx_val = float(adx_data.get("adx") or adx_data.get("value"))
                enrichment["adx"] = round(adx_val, 2)
            else:
                enrichment["adx"] = None

            # ── EMA 50 & 200 — trend detection ──
            ema50 = None
            ema200 = None
            if ema50_data and ("ema" in ema50_data or "value" in ema50_data):
                ema50 = float(ema50_data.get("ema") or ema50_data.get("value"))
                enrichment["ema50"] = round(ema50, 5)
            else:
                enrichment["ema50"] = None

            if ema200_data and ("ema" in ema200_data or "value" in ema200_data):
                ema200 = float(ema200_data.get("ema") or ema200_data.get("value"))
                enrichment["ema200"] = round(ema200, 5)
            else:
                enrichment["ema200"] = None

            # ── Trend classification ──
            enrichment["trend"] = self._classify_trend(price, ema50, ema200)

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

        # ── Hurst exponent — market character ──
        enrichment["hurst_exponent"] = await self._fetch_and_calc_hurst(
            td_symbol, td_interval
        )

        logger.info(
            f"Enriched {symbol}: ATR={enrichment.get('atr')} "
            f"RSI={enrichment.get('rsi')} ADX={enrichment.get('adx')} "
            f"Trend={enrichment.get('trend')} "
            f"Regime={enrichment.get('volatility_regime')} "
            f"Session={enrichment.get('active_session')} "
            f"Hurst={enrichment.get('hurst_exponent')}"
        )

        # ── Cache store ──
        _enrichment_cache[symbol] = {"data": enrichment, "timestamp": time.time()}

        return enrichment

    async def _fetch_parallel(self, symbol: str, interval: str):
        """Fetch ATR, RSI, ADX, EMA, and quote data."""
        atr_data = None
        rsi_data = None
        adx_data = None
        quote_data = None
        ema50_data = None
        ema200_data = None

        try:
            resp = await self.client.get(
                f"{BASE_URL}/atr",
                params={
                    "symbol": symbol, "interval": interval,
                    "time_period": 14, "apikey": self.api_key,
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                if "values" in data and data["values"]:
                    atr_data = data["values"][0]
                    logger.debug(f"ATR raw keys: {list(atr_data.keys())}")
                elif "status" in data and data["status"] == "error":
                    logger.warning(f"ATR API error: {data.get('message', 'unknown')}")
        except Exception as e:
            logger.debug(f"ATR fetch failed: {e}")

        try:
            resp = await self.client.get(
                f"{BASE_URL}/rsi",
                params={
                    "symbol": symbol, "interval": interval,
                    "time_period": 14, "apikey": self.api_key,
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                if "values" in data and data["values"]:
                    rsi_data = data["values"][0]
                    logger.debug(f"RSI raw keys: {list(rsi_data.keys())}")
                elif "status" in data and data["status"] == "error":
                    logger.warning(f"RSI API error: {data.get('message', 'unknown')}")
        except Exception as e:
            logger.debug(f"RSI fetch failed: {e}")

        try:
            resp = await self.client.get(
                f"{BASE_URL}/adx",
                params={
                    "symbol": symbol, "interval": interval,
                    "time_period": 14, "apikey": self.api_key,
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                if "values" in data and data["values"]:
                    adx_data = data["values"][0]
                    logger.debug(f"ADX raw keys: {list(adx_data.keys())}")
        except Exception as e:
            logger.debug(f"ADX fetch failed: {e}")

        try:
            resp = await self.client.get(
                f"{BASE_URL}/ema",
                params={
                    "symbol": symbol, "interval": interval,
                    "time_period": 50, "apikey": self.api_key,
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                if "values" in data and data["values"]:
                    ema50_data = data["values"][0]
        except Exception as e:
            logger.debug(f"EMA50 fetch failed: {e}")

        try:
            resp = await self.client.get(
                f"{BASE_URL}/ema",
                params={
                    "symbol": symbol, "interval": interval,
                    "time_period": 200, "apikey": self.api_key,
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                if "values" in data and data["values"]:
                    ema200_data = data["values"][0]
        except Exception as e:
            logger.debug(f"EMA200 fetch failed: {e}")

        try:
            resp = await self.client.get(
                f"{BASE_URL}/quote",
                params={"symbol": symbol, "apikey": self.api_key},
            )
            if resp.status_code == 200:
                quote_data = resp.json()
        except Exception as e:
            logger.debug(f"Quote fetch failed: {e}")

        return atr_data, rsi_data, adx_data, quote_data, ema50_data, ema200_data

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

    @staticmethod
    def calculate_hurst(series: List[float]) -> Optional[float]:
        """
        Calculate the Hurst exponent using Rescaled Range (R/S) analysis.

        Interpretation:
          H < 0.5  → Mean-reverting (choppy/ranging market)
          H ≈ 0.5  → Random walk (no memory)
          H > 0.5  → Trending (persistent momentum)

        Requires at least 20 data points for a meaningful estimate.
        Returns None if input is insufficient or invalid.
        """
        if not series or len(series) < 20:
            return None

        prices = np.array(series, dtype=np.float64)
        prices = prices[np.isfinite(prices)]
        if len(prices) < 20:
            return None

        # Compute on log-returns (standard quant practice)
        # This measures persistence of *returns*, not price levels
        ts = np.diff(np.log(prices[prices > 0]))
        if len(ts) < 20:
            return None

        # Use sub-series of increasing size to compute R/S at each scale
        min_window = 10
        max_window = len(ts)

        # Generate window sizes (powers of 2 up to max, plus intermediates)
        window_sizes = []
        size = min_window
        while size <= max_window // 2:
            window_sizes.append(size)
            size = int(size * 1.5)
        if not window_sizes:
            return None

        rs_values = []
        ns_values = []

        for n in window_sizes:
            # Split into non-overlapping sub-series of length n
            num_subseries = len(ts) // n
            if num_subseries == 0:
                continue

            rs_list = []
            for i in range(num_subseries):
                subseries = ts[i * n : (i + 1) * n]
                mean = np.mean(subseries)
                deviations = subseries - mean
                cumulative = np.cumsum(deviations)
                r = np.max(cumulative) - np.min(cumulative)
                s = np.std(subseries, ddof=1)
                if s > 0:
                    rs_list.append(r / s)

            if rs_list:
                rs_values.append(np.mean(rs_list))
                ns_values.append(n)

        if len(ns_values) < 2:
            return None

        # Linear regression of log(R/S) vs log(n) → slope is Hurst exponent
        log_n = np.log(ns_values)
        log_rs = np.log(rs_values)

        # Least squares: H = slope of best-fit line
        coeffs = np.polyfit(log_n, log_rs, 1)
        hurst = float(coeffs[0])

        # Clamp to valid range [0, 1]
        return round(max(0.0, min(1.0, hurst)), 4)

    async def _fetch_and_calc_hurst(self, symbol: str, interval: str) -> Optional[float]:
        """Fetch recent close prices from TwelveData and compute Hurst exponent."""
        try:
            resp = await self.client.get(
                f"{BASE_URL}/time_series",
                params={
                    "symbol": symbol,
                    "interval": interval,
                    "outputsize": 100,
                    "apikey": self.api_key,
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                values = data.get("values", [])
                if values:
                    closes = [float(v["close"]) for v in reversed(values) if v.get("close")]
                    return self.calculate_hurst(closes)
        except Exception as e:
            logger.debug(f"Hurst calculation failed: {e}")
        return None

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

    def _classify_trend(self, price: float, ema50: Optional[float], ema200: Optional[float]) -> str:
        """
        Classify trend from price vs EMA 50/200.
        STRONG_UP:   Price > EMA50 > EMA200
        UP:          Price > EMA50, EMA50 < EMA200
        WEAK_UP:     Price > EMA200, Price < EMA50
        STRONG_DOWN: Price < EMA50 < EMA200
        DOWN:        Price < EMA50, EMA50 > EMA200
        WEAK_DOWN:   Price < EMA200, Price > EMA50
        RANGING:     Price near both EMAs
        """
        if not ema50 and not ema200:
            return "UNKNOWN"

        if ema50 and ema200:
            if price > ema50 > ema200:
                return "STRONG_UP"
            elif price < ema50 < ema200:
                return "STRONG_DOWN"
            elif price > ema50 and ema50 < ema200:
                return "UP"
            elif price < ema50 and ema50 > ema200:
                return "DOWN"
            elif price > ema200 and price < ema50:
                return "WEAK_UP"
            elif price < ema200 and price > ema50:
                return "WEAK_DOWN"
            else:
                return "RANGING"
        elif ema50:
            if price > ema50:
                return "UP"
            else:
                return "DOWN"

        return "UNKNOWN"

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
            "adx": None,
            "ema50": None,
            "ema200": None,
            "trend": "UNKNOWN",
            "bid": None,
            "ask": None,
            "spread": None,
            "volume": None,
            "volatility_regime": "UNKNOWN",
            "active_session": self._detect_session(),
            "is_kill_zone": self._is_kill_zone(),
            "kill_zone_type": self._kill_zone_type(),
            "intermarket": {},
            "hurst_exponent": None,
        }

    async def enrich_from_mse(self, symbol: str, mse_data: Dict, price: float) -> Dict:
        """
        Build enrichment from MSE pre-computed data + intermarket-only fetch.
        Skips ATR, RSI, EMA, volume API calls — those are already in mse_data.
        Only fetches DXY, VIX, US10Y, OIL (what Pine Script can't see).

        Saves 3-4 TwelveData API credits and ~2 seconds per signal.
        """
        # ── Cache check ──
        now = time.time()
        cached = _enrichment_cache.get(symbol)
        if cached and (now - cached["timestamp"]) < CACHE_TTL_SECONDS:
            age = now - cached["timestamp"]
            logger.info(f"Enrichment cache HIT for {symbol} (age: {age:.1f}s)")
            return cached["data"]

        enrichment = {}

        try:
            # ── Technical data from MSE (no API calls needed) ──
            atr = mse_data.get("atr")
            rsi = mse_data.get("rsi")

            enrichment["atr"] = round(float(atr), 5) if atr is not None else None
            enrichment["atr_pct"] = round((float(atr) / price) * 100, 4) if atr and price else None
            enrichment["rsi"] = round(float(rsi), 2) if rsi is not None else None
            enrichment["rsi_zone"] = self._classify_rsi(float(rsi)) if rsi is not None else "UNKNOWN"

            # ADX from MSE
            adx = mse_data.get("adx")
            enrichment["adx"] = round(float(adx), 2) if adx is not None else None

            # EMA values aren't raw numbers from MSE — but we have the classification
            ema_slope = mse_data.get("ema50_slope", "flat")
            ema200_pos = mse_data.get("ema200_pos", "unknown")
            enrichment["ema50"] = None   # Raw value not available from MSE
            enrichment["ema200"] = None  # Raw value not available from MSE

            # Map MSE trend data to pipeline's trend classification
            htf_trend = mse_data.get("htf_trend", "neutral")
            if ema200_pos == "above" and ema_slope == "rising":
                enrichment["trend"] = "STRONG_UP"
            elif ema200_pos == "above":
                enrichment["trend"] = "UP"
            elif ema200_pos == "below" and ema_slope == "falling":
                enrichment["trend"] = "STRONG_DOWN"
            elif ema200_pos == "below":
                enrichment["trend"] = "DOWN"
            else:
                enrichment["trend"] = "RANGING"

            # Quote data — MSE doesn't have bid/ask spread
            enrichment["bid"] = None
            enrichment["ask"] = None
            enrichment["spread"] = None
            enrichment["volume"] = None

            # Volatility regime from MSE's regime classification
            mse_regime = mse_data.get("regime", "unknown")
            if "volatile" in mse_regime:
                enrichment["volatility_regime"] = "HIGH_VOLATILITY"
            elif "trending" in mse_regime:
                enrichment["volatility_regime"] = "TRENDING"
            elif "quiet" in mse_regime:
                enrichment["volatility_regime"] = "NORMAL"
            else:
                enrichment["volatility_regime"] = self._detect_volatility_regime(
                    enrichment.get("atr_pct")
                )

            # Session from MSE (already detected in Pine Script)
            mse_session = mse_data.get("session", "none")
            session_map = {
                "london_killzone": "LONDON",
                "ny_killzone": "NEW_YORK",
                "london_ny_overlap": "LONDON_NY_OVERLAP",
                "asian": "TOKYO",
                "off_session": "OFF_HOURS",
            }
            enrichment["active_session"] = session_map.get(mse_session, self._detect_session())
            enrichment["is_kill_zone"] = mse_session not in ("off_session", "none", "asian")
            kill_zone_map = {
                "london_killzone": "LONDON_OPEN",
                "ny_killzone": "NY_OPEN",
                "london_ny_overlap": "OVERLAP",
            }
            enrichment["kill_zone_type"] = kill_zone_map.get(mse_session, "NONE")

            # ── MSE confluence score as extra metadata ──
            enrichment["mse_confluence"] = mse_data.get("confluence_score")
            enrichment["mse_adx"] = mse_data.get("adx")
            enrichment["mse_rvol"] = mse_data.get("rvol")
            enrichment["mse_macd_hist"] = mse_data.get("macd_hist")
            enrichment["mse_htf_trend"] = htf_trend
            enrichment["mse_regime"] = mse_regime

        except Exception as e:
            logger.error(f"MSE enrichment mapping failed: {e}")
            # Fall through to intermarket — don't lose everything

        # ── Intermarket: the ONLY API calls we make ──
        enrichment["intermarket"] = await self._fetch_intermarket()

        logger.info(
            f"MSE-enriched {symbol}: RSI={enrichment.get('rsi')} "
            f"Trend={enrichment.get('trend')} "
            f"Regime={enrichment.get('volatility_regime')} "
            f"Session={enrichment.get('active_session')} "
            f"Confluence={enrichment.get('mse_confluence')} "
            f"[intermarket-only fetch — saved ~4 TD credits]"
        )

        # ── Cache store ──
        _enrichment_cache[symbol] = {"data": enrichment, "timestamp": time.time()}

        return enrichment

    async def fetch_intermarket_only(self) -> Dict:
        """
        Fetch ONLY intermarket data (DXY, VIX, bonds, oil).
        For use when technical data is already available from another source.
        Returns the same format as the intermarket section of full enrichment.
        """
        return await self._fetch_intermarket()

    async def close(self):
        """Close the HTTP client."""
        await self.client.aclose()
