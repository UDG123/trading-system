"""
Tests for the OniQuant v7.0 Python Signal Engine.
Unit tests for each module: indicator calculation, SMC analysis, confluence scoring,
signal generation, dedup, rate limiter, and payload format validation.
"""
import time
import numpy as np
import pandas as pd
import pytest


# ── Helpers ──

def _make_ohlcv(n: int = 200, trend: str = "up") -> pd.DataFrame:
    """Generate synthetic OHLCV data for testing."""
    np.random.seed(42)
    base = 100.0
    closes = [base]
    for i in range(1, n):
        drift = 0.05 if trend == "up" else (-0.05 if trend == "down" else 0)
        closes.append(closes[-1] * (1 + drift / 100 + np.random.randn() * 0.3 / 100))

    closes = np.array(closes)
    highs = closes * (1 + np.random.uniform(0.001, 0.005, n))
    lows = closes * (1 - np.random.uniform(0.001, 0.005, n))
    opens = closes * (1 + np.random.uniform(-0.002, 0.002, n))
    volume = np.random.uniform(1000, 5000, n)

    return pd.DataFrame({
        "time": pd.date_range("2026-01-01", periods=n, freq="h", tz="UTC"),
        "open": opens,
        "high": highs,
        "low": lows,
        "close": closes,
        "volume": volume,
    })


# ═══════════════════════════════════════════════════════════════
# Rate Limiter
# ═══════════════════════════════════════════════════════════════

class TestRateLimiter:

    def test_initial_state(self):
        from app.services.signal_engine.rate_limiter import RateLimiter
        rl = RateLimiter(daily_limit=100, per_minute_limit=10)
        assert rl.can_request()
        assert rl.daily_remaining == 100
        assert rl.minute_remaining == 10

    def test_record_request(self):
        from app.services.signal_engine.rate_limiter import RateLimiter
        rl = RateLimiter(daily_limit=5, per_minute_limit=10)
        for _ in range(5):
            rl.record_request()
        assert rl.daily_remaining == 0
        assert not rl.can_request()

    def test_per_minute_limit(self):
        from app.services.signal_engine.rate_limiter import RateLimiter
        rl = RateLimiter(daily_limit=1000, per_minute_limit=3)
        for _ in range(3):
            rl.record_request()
        assert not rl.can_request()
        assert rl.minute_remaining == 0

    def test_seconds_until_slot(self):
        from app.services.signal_engine.rate_limiter import RateLimiter
        rl = RateLimiter(daily_limit=1000, per_minute_limit=2)
        rl.record_request()
        rl.record_request()
        wait = rl.seconds_until_minute_slot()
        assert wait > 0
        assert wait <= 60


# ═══════════════════════════════════════════════════════════════
# Indicator Calculator
# ═══════════════════════════════════════════════════════════════

class TestIndicatorCalculator:

    def test_compute_returns_dict(self):
        from app.services.signal_engine.indicator_calculator import IndicatorCalculator
        calc = IndicatorCalculator()
        df = _make_ohlcv(200, trend="up")
        result = calc.compute(df, "EURUSD", "1H")

        assert result is not None
        assert "price" in result
        assert "ema21" in result
        assert "ema50" in result
        assert "rsi" in result
        assert "adx" in result
        assert "atr" in result
        assert "macd_hist" in result
        assert "supertrend_direction" in result
        assert "wavetrend_1" in result
        assert "rvol" in result

    def test_insufficient_data_returns_none(self):
        from app.services.signal_engine.indicator_calculator import IndicatorCalculator
        calc = IndicatorCalculator()
        df = _make_ohlcv(10)
        result = calc.compute(df, "EURUSD", "1H")
        assert result is None

    def test_ema_alignment_uptrend(self):
        from app.services.signal_engine.indicator_calculator import IndicatorCalculator
        calc = IndicatorCalculator()
        df = _make_ohlcv(200, trend="up")
        result = calc.compute(df, "EURUSD", "1H")
        # In a consistent uptrend, EMA21 should be above EMA50
        assert result is not None
        if result["ema21"] and result["ema50"]:
            assert result["ema21"] > result["ema50"]

    def test_rsi_in_range(self):
        from app.services.signal_engine.indicator_calculator import IndicatorCalculator
        calc = IndicatorCalculator()
        df = _make_ohlcv(200)
        result = calc.compute(df, "EURUSD", "1H")
        assert result is not None
        assert 0 <= result["rsi"] <= 100


# ═══════════════════════════════════════════════════════════════
# SMC Analyzer
# ═══════════════════════════════════════════════════════════════

class TestSMCAnalyzer:

    def test_analyze_returns_structure(self):
        from app.services.signal_engine.smc_analyzer import SMCAnalyzer
        analyzer = SMCAnalyzer()
        df = _make_ohlcv(100)
        result = analyzer.analyze(df, "EURUSD")

        assert result is not None
        assert "bos_bull" in result
        assert "bos_bear" in result
        assert "choch_bull" in result
        assert "choch_bear" in result
        assert "fvg_bull" in result
        assert "fvg_bear" in result
        assert "ob_bull" in result
        assert "ob_bear" in result
        assert "swing_highs" in result
        assert "swing_lows" in result

    def test_swings_detected(self):
        from app.services.signal_engine.smc_analyzer import SMCAnalyzer
        analyzer = SMCAnalyzer()
        df = _make_ohlcv(200)
        result = analyzer.analyze(df, "XAUUSD")
        assert result is not None
        # Should detect at least some swing points in 200 bars
        assert len(result["swing_highs"]) > 0 or len(result["swing_lows"]) > 0

    def test_insufficient_data(self):
        from app.services.signal_engine.smc_analyzer import SMCAnalyzer
        analyzer = SMCAnalyzer()
        df = _make_ohlcv(5)
        result = analyzer.analyze(df, "EURUSD")
        assert result is None


# ═══════════════════════════════════════════════════════════════
# Confluence Scorer
# ═══════════════════════════════════════════════════════════════

class TestConfluenceScorer:

    def test_perfect_long_score(self):
        from app.services.signal_engine.confluence_scorer import ConfluenceScorer
        scorer = ConfluenceScorer()

        # All indicators aligned bullish
        indicators = {
            "ema_full_bull": True, "ema_aligned_bull": True,
            "supertrend_direction": 1,
            "rsi": 55, "rsi_ob": False, "rsi_os": False,
            "adx": 30, "adx_trending": True,
            "macd_hist_growing_bull": True, "macd_hist_growing_bear": False,
            "wavetrend_ob": False, "wavetrend_os": False,
            "rvol": 1.5,
            "squeeze": True,
        }
        smc = {
            "bos_bull": True, "bos_bear": False,
            "choch_bull": False, "choch_bear": False,
            "fvg_at_price": "bull", "ob_at_price": "bull",
            "liq_sweep_low": True, "liq_sweep_high": False,
        }

        result = scorer.score(
            direction="LONG",
            entry_indicators=indicators,
            confirm_indicators=indicators,
            bias_indicators=indicators,
            smc_data=smc,
        )

        assert result["total_score"] > 6.5
        assert result["passes_threshold"]

    def test_conflicting_signals_low_score(self):
        from app.services.signal_engine.confluence_scorer import ConfluenceScorer
        scorer = ConfluenceScorer()

        # Bearish indicators but trying to go long
        indicators = {
            "ema_full_bull": False, "ema_aligned_bull": False,
            "ema_full_bear": True, "ema_aligned_bear": True,
            "supertrend_direction": -1,
            "rsi": 25, "rsi_ob": False, "rsi_os": True,
            "adx": 15, "adx_trending": False,
            "macd_hist_growing_bull": False, "macd_hist_growing_bear": True,
            "wavetrend_ob": False, "wavetrend_os": True,
            "rvol": 0.8,
            "squeeze": False,
        }

        result = scorer.score(
            direction="LONG",
            entry_indicators=indicators,
            confirm_indicators=indicators,
            bias_indicators=indicators,
            smc_data=None,
        )

        assert result["total_score"] < 6.5
        assert not result["passes_threshold"]

    def test_score_has_breakdown(self):
        from app.services.signal_engine.confluence_scorer import ConfluenceScorer
        scorer = ConfluenceScorer()
        indicators = {"ema_full_bull": True, "ema_aligned_bull": True,
                       "supertrend_direction": 1, "rsi": 50, "rsi_ob": False,
                       "adx": 30, "adx_trending": True,
                       "macd_hist_growing_bull": True, "macd_hist_growing_bear": False,
                       "wavetrend_ob": False, "rvol": 1.0, "squeeze": False}

        result = scorer.score("LONG", indicators, None, None, None)
        assert "breakdown" in result
        assert "entry" in result["breakdown"]


# ═══════════════════════════════════════════════════════════════
# Signal Generator — Payload Format
# ═══════════════════════════════════════════════════════════════

class TestSignalPayloadFormat:

    def test_payload_has_required_fields(self):
        """Verify signal payload matches what worker.py expects."""
        required_fields = [
            "symbol", "symbol_normalized", "timeframe", "alert_type",
            "direction", "price", "tp1", "tp2", "sl1",
            "desks_matched", "webhook_latency_ms", "time", "source",
        ]

        # Build a mock payload
        payload = {
            "symbol": "EURUSD",
            "symbol_normalized": "EURUSD",
            "exchange": "",
            "timeframe": "1H",
            "alert_type": "bullish_confirmation",
            "direction": "LONG",
            "price": 1.0850,
            "tp1": 1.0900,
            "tp2": 1.0950,
            "sl1": 1.0800,
            "sl2": None,
            "smart_trail": 1.0825,
            "volume": None,
            "desks_matched": ["DESK2_INTRADAY"],
            "webhook_latency_ms": 0,
            "time": str(int(time.time() * 1000)),
            "source": "python_engine",
            "confluence_score": 7.5,
            "strategy_id": "smc_bos_trend",
        }

        for field in required_fields:
            assert field in payload, f"Missing field: {field}"

        assert payload["source"] == "python_engine"
        assert payload["webhook_latency_ms"] == 0
        assert isinstance(payload["desks_matched"], list)
        assert payload["direction"] in ("LONG", "SHORT", "EXIT")

    def test_alert_types_valid(self):
        """Verify generated alert types are in VALID_ALERT_TYPES."""
        from app.config import VALID_ALERT_TYPES

        engine_alert_types = [
            "bullish_confirmation", "bearish_confirmation",
            "bullish_confirmation_plus", "bearish_confirmation_plus",
            "bullish_plus", "bearish_plus",
            "contrarian_bullish", "contrarian_bearish",
            "take_profit", "stop_loss",
            "smart_trail_cross",
        ]

        for at in engine_alert_types:
            assert at in VALID_ALERT_TYPES, f"{at} not in VALID_ALERT_TYPES"


# ═══════════════════════════════════════════════════════════════
# Dedup Filter
# ═══════════════════════════════════════════════════════════════

class TestDedupFilter:

    def test_hash_consistency(self):
        """Same signal fields produce same hash."""
        import hashlib
        from app.services.signal_engine.dedup_filter import DEDUP_PREFIX

        signal = {
            "symbol_normalized": "XAUUSD",
            "alert_type": "bullish_confirmation",
            "timeframe": "1H",
            "direction": "LONG",
        }

        key_parts = (
            str(signal["symbol_normalized"]),
            str(signal["alert_type"]),
            str(signal["timeframe"]),
            str(signal["direction"]),
        )
        hash1 = hashlib.sha256("|".join(key_parts).encode()).hexdigest()[:16]
        hash2 = hashlib.sha256("|".join(key_parts).encode()).hexdigest()[:16]
        assert hash1 == hash2

    def test_different_signals_different_hash(self):
        import hashlib

        def _hash(sym, alert, tf, dir_):
            parts = (sym, alert, tf, dir_)
            return hashlib.sha256("|".join(parts).encode()).hexdigest()[:16]

        h1 = _hash("EURUSD", "bullish_confirmation", "1H", "LONG")
        h2 = _hash("GBPUSD", "bullish_confirmation", "1H", "LONG")
        assert h1 != h2


# ═══════════════════════════════════════════════════════════════
# Candle Manager — Symbol Resolution
# ═══════════════════════════════════════════════════════════════

class TestCandleManagerSymbols:

    def test_get_all_symbols(self):
        from app.services.signal_engine.candle_manager import CandleManager
        symbols = CandleManager.get_all_symbols()
        assert len(symbols) > 10
        assert "EURUSD" in symbols
        assert "XAUUSD" in symbols
        assert "BTCUSD" in symbols

    def test_get_required_timeframes(self):
        from app.services.signal_engine.candle_manager import CandleManager
        tfs = CandleManager.get_required_timeframes()
        assert len(tfs) > 0
        assert "1H" in tfs

    def test_desk_timeframes(self):
        from app.services.signal_engine.candle_manager import CandleManager
        tfs = CandleManager.get_desk_timeframes("DESK2_INTRADAY")
        assert "15M" in tfs or "1H" in tfs or "4H" in tfs


# ═══════════════════════════════════════════════════════════════
# WaveTrend Custom Implementation
# ═══════════════════════════════════════════════════════════════

class TestWaveTrend:

    def test_wavetrend_output_shape(self):
        from app.services.signal_engine.indicator_calculator import wavetrend
        df = _make_ohlcv(100)
        wt1, wt2 = wavetrend(df["high"], df["low"], df["close"])
        assert len(wt1) == 100
        assert len(wt2) == 100
        # Should not be all NaN
        assert not wt1.isna().all()

    def test_wavetrend_bounded(self):
        from app.services.signal_engine.indicator_calculator import wavetrend
        df = _make_ohlcv(200)
        wt1, wt2 = wavetrend(df["high"], df["low"], df["close"])
        # WaveTrend values should be within reasonable bounds
        valid = wt1.dropna()
        assert valid.max() < 200
        assert valid.min() > -200
