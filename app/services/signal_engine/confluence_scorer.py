"""
Multi-Timeframe Confluence Scorer
The core edge: combines indicators across 3-4 timeframes into a single score.
Signal threshold: >= 6.5/10 to generate a signal.
"""
import logging
from typing import Dict, Optional

logger = logging.getLogger("TradingSystem.SignalEngine.Confluence")

# Weights per timeframe role
TF_WEIGHTS = {
    "entry": 0.30,
    "confirmation": 0.35,
    "bias": 0.25,
    "structure": 0.10,
}

# Minimum score to emit a signal
CONFLUENCE_THRESHOLD = 6.5


class ConfluenceScorer:
    """Multi-timeframe confluence scoring engine."""

    def score(
        self,
        direction: str,
        entry_indicators: Optional[Dict],
        confirm_indicators: Optional[Dict],
        bias_indicators: Optional[Dict],
        smc_data: Optional[Dict],
    ) -> Dict:
        """
        Compute weighted confluence score across timeframes.

        Args:
            direction: "LONG" or "SHORT"
            entry_indicators: Indicators from entry timeframe
            confirm_indicators: Indicators from confirmation timeframe
            bias_indicators: Indicators from bias/HTF timeframe
            smc_data: Smart Money Concepts from entry timeframe

        Returns:
            Dict with total_score, component scores, and breakdown.
        """
        is_long = direction == "LONG"

        entry_score = self._score_timeframe(entry_indicators, is_long) if entry_indicators else 0.0
        confirm_score = self._score_timeframe(confirm_indicators, is_long) if confirm_indicators else 0.0
        bias_score = self._score_timeframe(bias_indicators, is_long) if bias_indicators else 0.0
        structure_score = self._score_smc(smc_data, is_long) if smc_data else 0.0

        total = (
            entry_score * TF_WEIGHTS["entry"]
            + confirm_score * TF_WEIGHTS["confirmation"]
            + bias_score * TF_WEIGHTS["bias"]
            + structure_score * TF_WEIGHTS["structure"]
        )

        return {
            "total_score": round(total, 2),
            "passes_threshold": total >= CONFLUENCE_THRESHOLD,
            "entry_score": round(entry_score, 2),
            "confirm_score": round(confirm_score, 2),
            "bias_score": round(bias_score, 2),
            "structure_score": round(structure_score, 2),
            "direction": direction,
            "breakdown": {
                "entry": self._get_breakdown(entry_indicators, is_long) if entry_indicators else {},
                "confirm": self._get_breakdown(confirm_indicators, is_long) if confirm_indicators else {},
                "bias": self._get_breakdown(bias_indicators, is_long) if bias_indicators else {},
                "smc": self._get_smc_breakdown(smc_data, is_long) if smc_data else {},
            },
        }

    def _score_timeframe(self, ind: Dict, is_long: bool) -> float:
        """Score a single timeframe's indicators (0-10 scale)."""
        score = 0.0

        # EMA alignment: +2
        if is_long:
            if ind.get("ema_full_bull"):
                score += 2.0
            elif ind.get("ema_aligned_bull"):
                score += 1.5
        else:
            if ind.get("ema_full_bear"):
                score += 2.0
            elif ind.get("ema_aligned_bear"):
                score += 1.5

        # SuperTrend direction matches: +1
        st_dir = ind.get("supertrend_direction", 0)
        if (is_long and st_dir == 1) or (not is_long and st_dir == -1):
            score += 1.0

        # RSI not counter-trend extreme: +1
        rsi = ind.get("rsi", 50)
        if is_long and not ind.get("rsi_ob", False):
            score += 1.0
        elif not is_long and not ind.get("rsi_os", False):
            score += 1.0

        # ADX trending: +1
        if ind.get("adx_trending", False):
            score += 1.0

        # MACD histogram growing in direction: +1
        if is_long and ind.get("macd_hist_growing_bull", False):
            score += 1.0
        elif not is_long and ind.get("macd_hist_growing_bear", False):
            score += 1.0

        # WaveTrend not counter-signal: +1
        if is_long and not ind.get("wavetrend_ob", False):
            score += 1.0
        elif not is_long and not ind.get("wavetrend_os", False):
            score += 1.0

        # Volume confirmation (RVOL > 1.2): +1
        if ind.get("rvol", 1.0) > 1.2:
            score += 1.0

        # Squeeze breakout bonus: +1
        if ind.get("squeeze", False):
            score += 1.0

        return min(score, 10.0)

    def _score_smc(self, smc: Dict, is_long: bool) -> float:
        """Score SMC structure (0-10 scale)."""
        score = 0.0

        # BOS in direction: +3
        if is_long and smc.get("bos_bull", False):
            score += 3.0
        elif not is_long and smc.get("bos_bear", False):
            score += 3.0

        # CHoCH (reversal) in direction: +2
        if is_long and smc.get("choch_bull", False):
            score += 2.0
        elif not is_long and smc.get("choch_bear", False):
            score += 2.0

        # FVG at entry level: +2
        fvg = smc.get("fvg_at_price")
        if fvg == "bull" and is_long:
            score += 2.0
        elif fvg == "bear" and not is_long:
            score += 2.0

        # Order Block at entry level: +2
        ob = smc.get("ob_at_price")
        if ob == "bull" and is_long:
            score += 2.0
        elif ob == "bear" and not is_long:
            score += 2.0

        # Liquidity sweep (stop hunt confirmation): +1
        if is_long and smc.get("liq_sweep_low", False):
            score += 1.0
        elif not is_long and smc.get("liq_sweep_high", False):
            score += 1.0

        return min(score, 10.0)

    def _get_breakdown(self, ind: Dict, is_long: bool) -> Dict:
        """Get readable breakdown of scoring for logging."""
        return {
            "ema_aligned": ind.get("ema_full_bull" if is_long else "ema_full_bear", False),
            "supertrend_match": (
                (ind.get("supertrend_direction") == 1 and is_long)
                or (ind.get("supertrend_direction") == -1 and not is_long)
            ),
            "rsi": ind.get("rsi"),
            "adx_trending": ind.get("adx_trending"),
            "macd_growing": ind.get("macd_hist_growing_bull" if is_long else "macd_hist_growing_bear"),
            "rvol": ind.get("rvol"),
            "squeeze": ind.get("squeeze"),
        }

    def _get_smc_breakdown(self, smc: Dict, is_long: bool) -> Dict:
        return {
            "bos": smc.get("bos_bull" if is_long else "bos_bear", False),
            "choch": smc.get("choch_bull" if is_long else "choch_bear", False),
            "fvg_at_price": smc.get("fvg_at_price"),
            "ob_at_price": smc.get("ob_at_price"),
            "liq_sweep": smc.get("liq_sweep_low" if is_long else "liq_sweep_high", False),
        }
