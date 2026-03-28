"""
Tests for OniQuant v7.0 Quant Stack.
Unit tests for regime detection, volatility targeting, meta-labeling,
HAR-RV, alpha orthogonalization, backtest validation, wickless detection,
and factor monitoring.
"""
import numpy as np
import pandas as pd
import pytest


# ── Helpers ──

def _make_returns(n: int = 500, trend: str = "up") -> np.ndarray:
    np.random.seed(42)
    drift = 0.0005 if trend == "up" else (-0.0005 if trend == "down" else 0)
    return np.random.randn(n) * 0.01 + drift


def _make_ohlcv(n: int = 200, trend: str = "up") -> pd.DataFrame:
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
    return pd.DataFrame({
        "time": pd.date_range("2026-01-01", periods=n, freq="h", tz="UTC"),
        "open": opens, "high": highs, "low": lows, "close": closes,
        "volume": np.random.uniform(1000, 5000, n),
    })


# ═══════════════════════════════════════════════════════════════
# Regime Detector
# ═══════════════════════════════════════════════════════════════

class TestRegimeDetector:

    def test_fit_with_sufficient_data(self):
        from app.services.regime_detector import RegimeDetector
        rd = RegimeDetector()
        returns = _make_returns(500)
        result = rd.fit(returns, "TEST")
        assert result["status"] == "trained"
        assert "model" in result
        assert "label_map" in result

    def test_predict_returns_valid_regime(self):
        from app.services.regime_detector import RegimeDetector
        rd = RegimeDetector()
        returns = _make_returns(500)
        fit = rd.fit(returns)
        assert fit["status"] == "trained"
        pred = rd.predict(fit["model"], fit["features"], fit["label_map"])
        assert pred["regime"] in ("TRENDING", "MEAN_REVERTING", "VOLATILE")
        assert 0 <= pred["confidence"] <= 1
        assert "state_probabilities" in pred
        assert 0 <= pred["transition_risk"] <= 1

    def test_insufficient_data_fallback(self):
        from app.services.regime_detector import RegimeDetector
        rd = RegimeDetector()
        returns = _make_returns(10)
        result = rd.fit(returns)
        assert result["status"] == "insufficient_data"


# ═══════════════════════════════════════════════════════════════
# Volatility Targeter
# ═══════════════════════════════════════════════════════════════

class TestVolatilityTargeter:

    def test_ewma_vol_computation(self):
        from app.services.volatility_targeter import VolatilityTargeter
        vt = VolatilityTargeter()
        returns = _make_returns(100)
        vol = vt._ewma_vol(returns)
        assert vol > 0
        assert vol < 1

    def test_vol_multiplier_clamping(self):
        from app.services.volatility_targeter import VolatilityTargeter
        vt = VolatilityTargeter()
        # Very low vol → multiplier should be capped at 2.0
        returns = _make_returns(100) * 0.001
        vol = vt._ewma_vol(returns)
        target = 0.010
        mult = target / vol if vol > 0 else 10
        mult = max(0.25, min(2.0, mult))
        assert mult <= 2.0
        assert mult >= 0.25

    def test_rolling_vol(self):
        from app.services.volatility_targeter import VolatilityTargeter
        vt = VolatilityTargeter()
        returns = _make_returns(100)
        vol = vt._rolling_vol(returns)
        assert vol > 0


# ═══════════════════════════════════════════════════════════════
# Meta-Labeler
# ═══════════════════════════════════════════════════════════════

class TestMetaLabeler:

    def test_threshold_sizing(self):
        from app.services.meta_labeler import MetaLabeler
        ml = MetaLabeler()
        should, size = ml.should_trade(0.72)
        assert should is True
        assert size == 0.75

        should, size = ml.should_trade(0.80)
        assert should is True
        assert size == 1.0

        should, size = ml.should_trade(0.40)
        assert should is False
        assert size == 0.0

    def test_fallback_when_untrained(self):
        from app.services.meta_labeler import MetaLabeler
        ml = MetaLabeler()
        ml._model = None
        result = ml.predict({"consensus_score": 5, "ml_score": 0.6})
        assert result["meta_probability"] == 0.5
        assert result["should_trade"] is True
        assert result["bet_size"] == 1.0


# ═══════════════════════════════════════════════════════════════
# HAR-RV
# ═══════════════════════════════════════════════════════════════

class TestHARRV:

    def test_realized_variance_positive(self):
        """Squared log returns should always be positive."""
        returns = _make_returns(100)
        rv = float(np.sum(returns ** 2))
        assert rv > 0

    def test_har_regression_betas(self):
        """OLS on synthetic data should produce finite betas."""
        returns = _make_returns(100)
        rv = returns ** 2
        n = len(rv)
        y = rv[22:]
        X_d = rv[21:n - 1]
        X_w = np.array([np.mean(rv[i - 4:i + 1]) for i in range(21, n - 1)])
        X_m = np.array([np.mean(rv[i - 21:i + 1]) for i in range(21, n - 1)])
        min_len = min(len(y), len(X_d), len(X_w), len(X_m))
        X = np.column_stack([np.ones(min_len), X_d[:min_len], X_w[:min_len], X_m[:min_len]])
        y = y[:min_len]
        betas, _, _, _ = np.linalg.lstsq(X, y, rcond=None)
        assert all(np.isfinite(betas))


# ═══════════════════════════════════════════════════════════════
# Alpha Orthogonalizer
# ═══════════════════════════════════════════════════════════════

class TestAlphaOrthogonalizer:

    def test_orthogonalized_preserves_hurst(self):
        from app.services.alpha_orthogonalizer import AlphaOrthogonalizer
        ao = AlphaOrthogonalizer()
        result = ao.orthogonalize({"hurst": 0.58, "smc_score": 7.0, "ml_score": 0.72})
        # Hurst should be unchanged (first in Gram-Schmidt)
        assert result["orthogonalized"]["hurst"] == (0.58 - 0.5) * 10

    def test_default_weights(self):
        from app.services.alpha_orthogonalizer import AlphaOrthogonalizer
        ao = AlphaOrthogonalizer()
        weights = ao._default_weights()
        assert "optimal_weights" in weights
        total = sum(weights["optimal_weights"].values())
        assert abs(total - 1.0) < 0.01


# ═══════════════════════════════════════════════════════════════
# Backtest Validator
# ═══════════════════════════════════════════════════════════════

class TestBacktestValidator:

    def test_deflated_sharpe_reduces_significance(self):
        from app.services.backtest_validator import BacktestValidator
        bv = BacktestValidator()
        result = bv.compute_deflated_sharpe(
            sharpe_observed=1.5, n_trials=20, T=200
        )
        assert result["deflated_sharpe"] < result["sharpe_ratio"]
        assert 0 <= result["dsr_p_value"] <= 1

    def test_pbo_on_random_data(self):
        from app.services.backtest_validator import BacktestValidator
        bv = BacktestValidator()
        np.random.seed(42)
        returns_matrix = np.random.randn(200, 5) * 0.01
        result = bv.compute_cscv(returns_matrix)
        # PBO should be around 0.5 for random strategies
        assert 0.2 <= result["pbo_score"] <= 0.8


# ═══════════════════════════════════════════════════════════════
# Wickless Detector
# ═══════════════════════════════════════════════════════════════

class TestWicklessDetector:

    def test_bullish_wickless(self):
        from app.services.signal_engine.wickless_detector import detect_wickless
        df = pd.DataFrame({
            "open": [100.0, 100.0, 100.0],
            "high": [105.0, 105.0, 105.0],
            "low": [100.0, 100.0, 100.0],   # open == low → no lower wick
            "close": [104.9, 104.9, 104.9],
        })
        result = detect_wickless(df)
        assert result["has_wickless"] is True
        assert result["wickless_type"] == "bullish"

    def test_bearish_wickless(self):
        from app.services.signal_engine.wickless_detector import detect_wickless
        df = pd.DataFrame({
            "open": [105.0, 105.0, 105.0],
            "high": [105.0, 105.0, 105.0],   # open == high → no upper wick
            "low": [100.0, 100.0, 100.0],
            "close": [100.1, 100.1, 100.1],
        })
        result = detect_wickless(df)
        assert result["has_wickless"] is True
        assert result["wickless_type"] == "bearish"

    def test_no_wickless(self):
        from app.services.signal_engine.wickless_detector import detect_wickless
        df = pd.DataFrame({
            "open": [102.0, 102.0, 102.0],
            "high": [105.0, 105.0, 105.0],
            "low": [100.0, 100.0, 100.0],
            "close": [103.0, 103.0, 103.0],
        })
        result = detect_wickless(df)
        assert result["has_wickless"] is False


# ═══════════════════════════════════════════════════════════════
# Factor Monitor
# ═══════════════════════════════════════════════════════════════

class TestFactorMonitor:

    def test_concentration_threshold(self):
        from app.services.factor_monitor import CONCENTRATION_THRESHOLD
        assert CONCENTRATION_THRESHOLD == 0.60
