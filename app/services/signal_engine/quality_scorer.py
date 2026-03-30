"""
Signal Quality Scorer — composite 0-100 score replacing heavy CTO filtering.

Components:
  MTF Confluence:            25 pts max
  Volatility Regime:         20 pts max
  S/R Proximity:             15 pts max
  Candle Quality:            10 pts max
  CatBoost Probability:      20 pts max
  Volume Confirmation:       10 pts max
                             ───────────
  Total:                    100 pts max

Emission rules:
  >= 75: EMIT full size
  >= THRESHOLD (default 65): EMIT 0.5x size
  < THRESHOLD: SKIP
"""
import os
import logging
from typing import Dict, Optional, Tuple

logger = logging.getLogger("TradingSystem.SignalEngine.QualityScorer")

QUALITY_SCORE_THRESHOLD = int(os.getenv("QUALITY_SCORE_THRESHOLD", "65"))

# Signal type classification for regime matching
MOMENTUM_ALERT_TYPES = {
    "bullish_confirmation", "bearish_confirmation",
    "bullish_confirmation_plus", "bearish_confirmation_plus",
    "bullish_plus", "bearish_plus",
}
MEAN_REVERSION_ALERT_TYPES = {
    "contrarian_bullish", "contrarian_bearish",
}


class SignalQualityScorer:
    """Computes composite quality score for signal candidates."""

    def score(
        self,
        confluence: Dict,
        indicators: Dict,
        smc: Optional[Dict],
        alert_type: str,
        catboost_proba: Optional[float] = None,
    ) -> Dict:
        """
        Compute quality score from 0-100.

        Returns dict with total score, component breakdown, emission decision,
        and size multiplier.
        """
        breakdown = {}

        # 1. MTF Confluence (25 pts)
        breakdown["mtf_confluence"] = self._score_confluence(confluence)

        # 2. Volatility Regime Suitability (20 pts)
        regime = confluence.get("regime", "UNKNOWN")
        direction = confluence.get("direction", "")
        breakdown["regime_suitability"] = self._score_regime(regime, alert_type, direction, indicators)

        # 3. S/R Proximity (15 pts)
        breakdown["sr_proximity"] = self._score_sr_proximity(indicators, smc)

        # 4. Candle Quality (10 pts)
        breakdown["candle_quality"] = self._score_candle(indicators)

        # 5. CatBoost Probability (20 pts)
        breakdown["catboost"] = self._score_catboost(catboost_proba)

        # 6. Volume Confirmation (10 pts)
        breakdown["volume"] = self._score_volume(indicators)

        total = sum(breakdown.values())
        total = min(100, max(0, round(total, 1)))

        # Emission decision
        if total >= 75:
            emit = True
            size_mult = 1.0
            tier = "HIGH"
        elif total >= QUALITY_SCORE_THRESHOLD:
            emit = True
            size_mult = 0.5
            tier = "MEDIUM"
        else:
            emit = False
            size_mult = 0.0
            tier = "SKIP"

        return {
            "quality_score": total,
            "tier": tier,
            "emit": emit,
            "size_multiplier": size_mult,
            "threshold": QUALITY_SCORE_THRESHOLD,
            "breakdown": breakdown,
        }

    @staticmethod
    def _score_confluence(confluence: Dict) -> float:
        """MTF Confluence: 25 pts max. Maps abs(score) from [0.3, 1.0] to [0, 25]."""
        raw = abs(confluence.get("confluence_score", 0))
        if raw <= 0.3:
            return 0.0
        # Linear map: 0.3 → 0, 1.0 → 25
        return round(min(25.0, (raw - 0.3) / 0.7 * 25), 1)

    @staticmethod
    def _score_regime(regime: str, alert_type: str, direction: str = "", indicators: Dict = None) -> float:
        """Volatility Regime Suitability: 20 pts max. Ichimoku alignment bonus ±5."""
        is_momentum = alert_type in MOMENTUM_ALERT_TYPES
        is_mean_rev = alert_type in MEAN_REVERSION_ALERT_TYPES

        if regime in ("TRENDING_UP", "TRENDING_DOWN"):
            if is_momentum:
                score = 20.0
            elif is_mean_rev:
                score = 5.0
            else:
                score = 12.0
        elif regime == "RANGING":
            if is_mean_rev:
                score = 20.0
            elif is_momentum:
                score = 5.0
            else:
                score = 12.0
        else:
            score = 10.0

        # Ichimoku alignment bonus/penalty
        if indicators and direction:
            above = indicators.get("price_above_cloud")
            below = indicators.get("price_below_cloud")
            if above is not None:
                if direction == "LONG" and above:
                    score += 5.0   # Long with price above cloud — aligned
                elif direction == "SHORT" and below:
                    score += 5.0   # Short with price below cloud — aligned
                elif direction == "SHORT" and above:
                    score -= 5.0   # Short against cloud — penalize
                elif direction == "LONG" and below:
                    score -= 5.0   # Long against cloud — penalize

        return max(0.0, min(20.0, round(score, 1)))

    @staticmethod
    def _score_sr_proximity(indicators: Dict, smc: Optional[Dict]) -> float:
        """S/R Proximity: 15 pts max. Near FVG/OB = confirmation."""
        if not smc:
            return 8.0

        # Check if price is at an FVG or Order Block
        at_fvg = smc.get("fvg_at_price") is not None
        at_ob = smc.get("ob_at_price") is not None
        has_bos = smc.get("bos_bull", False) or smc.get("bos_bear", False)

        if at_fvg and at_ob:
            return 15.0  # Price at both FVG and OB — strong S/R
        elif at_fvg or at_ob:
            return 12.0  # Price at one S/R level
        elif has_bos:
            return 10.0  # BOS confirms structure even without exact level
        else:
            return 8.0   # No-man's-land

    @staticmethod
    def _score_candle(indicators: Dict) -> float:
        """Candle Quality: 10 pts max. Based on body ratio."""
        body_ratio = indicators.get("candle_body_ratio")
        if body_ratio is None:
            # Compute from raw OHLCV if available
            price = indicators.get("price", 0)
            # Can't compute without open — use neutral score
            return 6.0

        if body_ratio > 0.7:
            return 10.0  # Wickless/strong candle
        elif body_ratio > 0.3:
            return 6.0   # Normal candle
        else:
            return 2.0   # Doji/indecision

    @staticmethod
    def _score_catboost(proba: Optional[float]) -> float:
        """CatBoost Probability: 20 pts max. Maps [0.5, 1.0] to [0, 20]."""
        if proba is None:
            return 10.0  # No model available — neutral score
        if proba <= 0.5:
            return 0.0
        # Linear map: 0.5 → 0, 1.0 → 20
        return round(min(20.0, (proba - 0.5) / 0.5 * 20), 1)

    @staticmethod
    def _score_volume(indicators: Dict) -> float:
        """Volume Confirmation: 10 pts max."""
        rvol = indicators.get("rvol", 1.0)
        if rvol > 1.5:
            return 10.0
        elif rvol > 1.0:
            return 6.0
        else:
            return 3.0
