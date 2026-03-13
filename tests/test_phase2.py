"""
Phase 2 Tests - Enrichment, ML scoring, consensus, CTO, and pipeline.
Run with: pytest tests/ -v
"""
import json
import pytest
from unittest.mock import patch, AsyncMock, MagicMock
import os

os.environ["DATABASE_URL"] = "sqlite:///./test_phase2.db"
os.environ["WEBHOOK_SECRET"] = "test-secret"

from app.services.ml_scorer import MLScorer
from app.services.consensus_scorer import ConsensusScorer
from app.services.claude_cto import ClaudeCTO
from app.services.risk_filter import HardRiskFilter
from app.services.twelvedata_enricher import TwelveDataEnricher
from app.config import DESKS


# ─────────────────────────────────────────────
# ML SCORER
# ─────────────────────────────────────────────
class TestMLScorer:
    def setup_method(self):
        self.scorer = MLScorer()

    def test_rule_based_score_returns_valid_range(self):
        signal = {
            "direction": "LONG",
            "alert_type": "bullish_confirmation",
            "price": 1.1050,
            "tp1": 1.1100,
            "sl1": 1.1020,
        }
        enrichment = {
            "rsi": 45,
            "atr_pct": 0.5,
            "is_kill_zone": False,
            "kill_zone_type": "NONE",
            "active_session": "LONDON",
            "volatility_regime": "NORMAL",
            "spread": 0.00012,
            "volume": 50000,
            "intermarket": {},
        }
        result = self.scorer.score(signal, enrichment, "DESK1_SCALPER")
        assert 0.0 <= result["ml_score"] <= 1.0
        assert result["ml_method"] == "rule_based"
        assert "ml_features" in result

    def test_plus_signal_scores_higher(self):
        base_signal = {
            "direction": "LONG",
            "alert_type": "bullish_confirmation",
            "price": 1.1050,
            "tp1": 1.1100,
            "sl1": 1.1020,
        }
        plus_signal = {**base_signal, "alert_type": "bullish_plus"}
        enrichment = {
            "rsi": 50, "atr_pct": 0.5, "is_kill_zone": False,
            "kill_zone_type": "NONE", "active_session": "LONDON",
            "volatility_regime": "NORMAL", "spread": 0, "volume": 0,
            "intermarket": {},
        }
        base_score = self.scorer.score(base_signal, enrichment, "DESK1_SCALPER")
        plus_score = self.scorer.score(plus_signal, enrichment, "DESK1_SCALPER")
        assert plus_score["ml_score"] > base_score["ml_score"]

    def test_kill_zone_boosts_score(self):
        signal = {
            "direction": "LONG",
            "alert_type": "bullish_confirmation",
            "price": 1.1050, "tp1": 1.1100, "sl1": 1.1020,
        }
        no_kz = {
            "rsi": 50, "atr_pct": 0.5, "is_kill_zone": False,
            "kill_zone_type": "NONE", "active_session": "LONDON",
            "volatility_regime": "NORMAL", "spread": 0, "volume": 0,
            "intermarket": {},
        }
        with_kz = {**no_kz, "is_kill_zone": True, "kill_zone_type": "OVERLAP"}
        score_no_kz = self.scorer.score(signal, no_kz, "DESK1_SCALPER")
        score_kz = self.scorer.score(signal, with_kz, "DESK1_SCALPER")
        assert score_kz["ml_score"] > score_no_kz["ml_score"]

    def test_bad_rr_lowers_score(self):
        """Bad risk/reward (TP closer than SL) should lower score."""
        good_rr = {
            "direction": "LONG", "alert_type": "bullish_confirmation",
            "price": 1.1050, "tp1": 1.1150, "sl1": 1.1020,
        }
        bad_rr = {
            "direction": "LONG", "alert_type": "bullish_confirmation",
            "price": 1.1050, "tp1": 1.1060, "sl1": 1.1000,
        }
        enrichment = {
            "rsi": 50, "atr_pct": 0.5, "is_kill_zone": False,
            "kill_zone_type": "NONE", "active_session": "LONDON",
            "volatility_regime": "NORMAL", "spread": 0, "volume": 0,
            "intermarket": {},
        }
        good = self.scorer.score(good_rr, enrichment, "DESK1_SCALPER")
        bad = self.scorer.score(bad_rr, enrichment, "DESK1_SCALPER")
        assert good["ml_score"] > bad["ml_score"]


# ─────────────────────────────────────────────
# CONSENSUS SCORER
# ─────────────────────────────────────────────
class TestConsensusScorer:
    def setup_method(self):
        self.scorer = ConsensusScorer()

    def test_basic_entry_signal_gets_points(self):
        signal = {
            "symbol": "EURUSD",
            "direction": "LONG",
            "alert_type": "bullish_confirmation",
            "timeframe": "5M",
        }
        enrichment = {
            "kill_zone_type": "NONE",
            "rsi": 50,
        }
        ml = {"ml_score": 0.55}
        result = self.scorer.score(signal, enrichment, "DESK1_SCALPER", ml)
        assert result["total_score"] >= 1  # at least entry trigger point
        assert "entry_trigger" in result["breakdown"]

    def test_plus_signal_gets_bonus(self):
        signal = {
            "symbol": "EURUSD",
            "direction": "LONG",
            "alert_type": "bullish_plus",
            "timeframe": "5M",
        }
        enrichment = {"kill_zone_type": "NONE", "rsi": 50}
        ml = {"ml_score": 0.55}
        result = self.scorer.score(signal, enrichment, "DESK1_SCALPER", ml)
        assert "bullish_bearish_plus" in result["breakdown"]
        assert result["total_score"] >= 2

    def test_kill_zone_overlap_bonus(self):
        signal = {
            "symbol": "EURUSD",
            "direction": "LONG",
            "alert_type": "bullish_confirmation",
            "timeframe": "5M",
        }
        enrichment = {"kill_zone_type": "OVERLAP", "rsi": 50}
        ml = {"ml_score": 0.70}
        result = self.scorer.score(signal, enrichment, "DESK1_SCALPER", ml)
        assert "kill_zone_overlap" in result["breakdown"]
        assert result["breakdown"]["kill_zone_overlap"] == 2

    def test_high_ml_gets_confirmation(self):
        signal = {
            "symbol": "EURUSD",
            "direction": "LONG",
            "alert_type": "bullish_confirmation",
            "timeframe": "5M",
        }
        enrichment = {"kill_zone_type": "NONE", "rsi": 50}
        ml = {"ml_score": 0.85}
        result = self.scorer.score(signal, enrichment, "DESK1_SCALPER", ml)
        assert "ml_confirm" in result["breakdown"]
        assert "ml_confirm_strong" in result["breakdown"]

    def test_score_tiers(self):
        """Verify score → tier mapping."""
        scorer = self.scorer

        # Build a signal that should score HIGH (7+)
        signal = {
            "symbol": "EURUSD",
            "direction": "LONG",
            "alert_type": "bullish_plus",  # +1 entry, +1 plus
            "timeframe": "5M",
        }
        enrichment = {
            "kill_zone_type": "OVERLAP",  # +2
            "rsi": 25,  # oversold bullish = +1
        }
        ml = {"ml_score": 0.85}  # +1, +1 strong
        recent = [
            {"symbol": "EURUSD", "timeframe": "15M", "direction": "LONG",
             "alert_type": "bullish_confirmation"},  # bias match +3
        ]
        result = scorer.score(signal, enrichment, "DESK1_SCALPER", ml, recent)
        assert result["total_score"] >= 7
        assert result["tier"] == "HIGH"
        assert result["size_multiplier"] == 1.0


# ─────────────────────────────────────────────
# CLAUDE CTO - HARD RULES
# ─────────────────────────────────────────────
class TestClaudeCTOHardRules:
    def setup_method(self):
        self.cto = ClaudeCTO()

    def test_skip_when_consensus_zero(self):
        result = self.cto._check_hard_rules(
            signal_data={"sl1": 1.1020},
            consensus={"total_score": 0},
            desk_state={"is_active": True, "is_paused": False,
                        "daily_loss": 0, "consecutive_losses": 0},
            firm_risk={"firm_drawdown_exceeded": False},
        )
        assert result is not None
        assert result["decision"] == "SKIP"

    def test_allow_when_consensus_1(self):
        result = self.cto._check_hard_rules(
            signal_data={"sl1": 1.1020},
            consensus={"total_score": 1},
            desk_state={"is_active": True, "is_paused": False,
                        "daily_loss": 0, "consecutive_losses": 0},
            firm_risk={"firm_drawdown_exceeded": False},
        )
        assert result is None  # no hard skip, proceed to Claude

    def test_skip_when_desk_paused(self):
        result = self.cto._check_hard_rules(
            signal_data={"sl1": 1.1020},
            consensus={"total_score": 8},
            desk_state={"is_active": True, "is_paused": True,
                        "daily_loss": 0, "consecutive_losses": 0},
            firm_risk={"firm_drawdown_exceeded": False},
        )
        assert result is not None
        assert result["decision"] == "SKIP"

    def test_skip_when_no_stop_loss(self):
        result = self.cto._check_hard_rules(
            signal_data={"sl1": None},
            consensus={"total_score": 8},
            desk_state={"is_active": True, "is_paused": False,
                        "daily_loss": 0, "consecutive_losses": 0},
            firm_risk={"firm_drawdown_exceeded": False},
        )
        assert result is not None
        assert result["decision"] == "SKIP"

    def test_skip_when_daily_loss_exceeded(self):
        result = self.cto._check_hard_rules(
            signal_data={"sl1": 1.1020},
            consensus={"total_score": 8},
            desk_state={"is_active": True, "is_paused": False,
                        "daily_loss": -4600, "consecutive_losses": 0},
            firm_risk={"firm_drawdown_exceeded": False},
        )
        assert result is not None
        assert result["decision"] == "SKIP"

    def test_passes_when_all_clear(self):
        result = self.cto._check_hard_rules(
            signal_data={"sl1": 1.1020},
            consensus={"total_score": 8},
            desk_state={"is_active": True, "is_paused": False,
                        "daily_loss": -1000, "consecutive_losses": 1},
            firm_risk={"firm_drawdown_exceeded": False},
        )
        assert result is None  # None means no hard skip

    def test_rule_based_fallback_high_consensus(self):
        result = self.cto._rule_based_decision(
            signal_data={"symbol": "EURUSD", "direction": "LONG"},
            enrichment={"volatility_regime": "NORMAL"},
            ml_result={"ml_score": 0.72},
            consensus={"total_score": 8, "tier": "HIGH", "size_multiplier": 1.0},
            desk_state={"consecutive_losses": 0, "size_modifier": 1.0},
        )
        assert result["decision"] == "EXECUTE"
        assert result["size_multiplier"] > 0

    def test_rule_based_fallback_skip_tier(self):
        result = self.cto._rule_based_decision(
            signal_data={"symbol": "EURUSD", "direction": "LONG"},
            enrichment={"volatility_regime": "NORMAL"},
            ml_result={"ml_score": 0.55},
            consensus={"total_score": 1, "tier": "SKIP", "size_multiplier": 0.0},
            desk_state={"consecutive_losses": 0, "size_modifier": 1.0},
        )
        assert result["decision"] == "SKIP"

    def test_consecutive_losses_reduce_size(self):
        result = self.cto._rule_based_decision(
            signal_data={"symbol": "EURUSD", "direction": "LONG"},
            enrichment={"volatility_regime": "NORMAL"},
            ml_result={"ml_score": 0.72},
            consensus={"total_score": 8, "tier": "HIGH", "size_multiplier": 1.0},
            desk_state={"consecutive_losses": 3, "size_modifier": 0.50},
        )
        assert result["size_multiplier"] < 1.0


# ─────────────────────────────────────────────
# TWELVEDATA ENRICHER
# ─────────────────────────────────────────────
class TestTwelveDataEnricher:
    def setup_method(self):
        self.enricher = TwelveDataEnricher()

    def test_session_detection(self):
        session = self.enricher._detect_session()
        assert session in (
            "LONDON", "NEW_YORK", "LONDON_NY_OVERLAP",
            "SYDNEY", "TOKYO", "OFF_HOURS",
        )

    def test_rsi_classification(self):
        assert self.enricher._classify_rsi(85) == "EXTREME_OVERBOUGHT"
        assert self.enricher._classify_rsi(75) == "OVERBOUGHT"
        assert self.enricher._classify_rsi(60) == "BULLISH"
        assert self.enricher._classify_rsi(50) == "NEUTRAL"
        assert self.enricher._classify_rsi(35) == "BEARISH"
        assert self.enricher._classify_rsi(25) == "OVERSOLD"
        assert self.enricher._classify_rsi(15) == "EXTREME_OVERSOLD"

    def test_volatility_regime_detection(self):
        assert self.enricher._detect_volatility_regime(2.0) == "HIGH_VOLATILITY"
        assert self.enricher._detect_volatility_regime(0.8) == "TRENDING"
        assert self.enricher._detect_volatility_regime(0.3) == "NORMAL"
        assert self.enricher._detect_volatility_regime(0.1) == "LOW_VOLATILITY"
        assert self.enricher._detect_volatility_regime(None) == "UNKNOWN"

    def test_default_enrichment(self):
        result = self.enricher._default_enrichment("EURUSD", 1.1050)
        assert "active_session" in result
        assert "volatility_regime" in result
        assert result["rsi"] is None
        assert result["atr"] is None


# ─────────────────────────────────────────────
# INTEGRATION: FULL PIPELINE FLOW (mocked)
# ─────────────────────────────────────────────
class TestPipelineIntegration:
    """Test the pipeline flow with mocked external services."""

    def test_consensus_feeds_into_cto(self):
        """Verify consensus output drives CTO decision correctly."""
        consensus = ConsensusScorer()
        cto = ClaudeCTO()

        signal = {
            "symbol": "XAUUSD",
            "direction": "LONG",
            "alert_type": "bullish_plus",
            "timeframe": "15M",
            "sl1": 2640.0,
        }
        enrichment = {
            "kill_zone_type": "OVERLAP",
            "rsi": 35,
            "volatility_regime": "TRENDING",
        }
        ml = {"ml_score": 0.75}

        # Get consensus
        cons_result = consensus.score(
            signal, enrichment, "DESK4_GOLD", ml
        )

        # Feed into CTO rule-based
        decision = cto._rule_based_decision(
            signal_data=signal,
            enrichment=enrichment,
            ml_result=ml,
            consensus=cons_result,
            desk_state={"consecutive_losses": 0, "size_modifier": 1.0},
        )

        # With plus signal + overlap + ML confirmation, should execute
        assert decision["decision"] in ("EXECUTE", "REDUCE")
        assert decision["size_multiplier"] > 0
