"""
Consensus Scoring Engine
Implements the multi-timeframe consensus scoring system from the architecture.
Produces a score that determines position sizing: HIGH (7+), MEDIUM (4-6),
LOW (2-3), or SKIP (below 2).
"""
import logging
from typing import Dict, List, Optional

from app.config import DESKS, SCORE_WEIGHTS, SCORE_THRESHOLDS

logger = logging.getLogger("TradingSystem.Consensus")


class ConsensusScorer:
    """
    Calculates multi-timeframe consensus score for a signal.
    Each component adds or subtracts points based on alignment.
    """

    def score(
        self,
        signal_data: Dict,
        enrichment: Dict,
        desk_id: str,
        ml_result: Dict,
        recent_signals: Optional[List[Dict]] = None,
    ) -> Dict:
        """
        Calculate consensus score. Returns:
        - total_score: int
        - tier: HIGH / MEDIUM / LOW / SKIP
        - size_multiplier: 1.0 / 0.5 / 0.25 / 0.0
        - breakdown: dict of individual score components
        """
        desk = DESKS.get(desk_id, {})
        breakdown = {}
        total = 0

        # ── 1. Entry trigger fires (+2) ──
        # A LuxAlgo confirmation signal is a real, validated entry
        entry_types = {
            "bullish_confirmation", "bearish_confirmation",
            "bullish_plus", "bearish_plus",
            "bullish_confirmation_plus", "bearish_confirmation_plus",
            "contrarian_bullish", "contrarian_bearish",
        }
        if signal_data.get("alert_type") in entry_types:
            breakdown["entry_trigger"] = 2  # base 2 for any valid entry
            total += 2

        # ── 1b. Signal has SL and TP (+1 bonus) ──
        # Signals with defined risk levels are higher quality
        if signal_data.get("sl1") and signal_data.get("tp1"):
            breakdown["defined_risk"] = 1
            total += 1

        # ── 2. Bullish+ or Bearish+ signal type (+1) ──
        if "plus" in signal_data.get("alert_type", ""):
            breakdown["bullish_bearish_plus"] = SCORE_WEIGHTS["bullish_bearish_plus"]
            total += breakdown["bullish_bearish_plus"]

        # ── 3. Confirmation Turn+ present (+2) ──
        if signal_data.get("alert_type") == "confirmation_turn_plus":
            breakdown["confirmation_turn_plus"] = SCORE_WEIGHTS["confirmation_turn_plus"]
            total += breakdown["confirmation_turn_plus"]

        # Check recent signals for confirmation turn on same symbol
        if recent_signals:
            for rs in recent_signals:
                if (
                    rs.get("symbol") == signal_data.get("symbol")
                    and rs.get("alert_type") == "confirmation_turn_plus"
                    and "confirmation_turn_plus" not in breakdown
                ):
                    breakdown["confirmation_turn_plus"] = SCORE_WEIGHTS["confirmation_turn_plus"]
                    total += breakdown["confirmation_turn_plus"]
                    break

        # ── 4. Timeframe alignment scoring ──
        tf_scores = self._score_timeframe_alignment(
            signal_data, desk, recent_signals
        )
        breakdown.update(tf_scores)
        total += sum(tf_scores.values())

        # ── 5. Kill Zone bonuses ──
        kz_type = enrichment.get("kill_zone_type", "NONE")
        if kz_type == "OVERLAP":
            breakdown["kill_zone_overlap"] = SCORE_WEIGHTS["kill_zone_overlap"]
            total += breakdown["kill_zone_overlap"]
        elif kz_type in ("LONDON_OPEN", "NY_OPEN"):
            breakdown["kill_zone_single"] = SCORE_WEIGHTS["kill_zone_single"]
            total += breakdown["kill_zone_single"]

        # ── 6. ML classifier confirmation (+1 per timeframe) ──
        ml_score = ml_result.get("ml_score", 0.5)
        if ml_score >= 0.65:
            breakdown["ml_confirm"] = SCORE_WEIGHTS["ml_confirm_per_tf"]
            total += breakdown["ml_confirm"]
        if ml_score >= 0.80:
            # Strong ML confirmation = extra point
            breakdown["ml_confirm_strong"] = SCORE_WEIGHTS["ml_confirm_per_tf"]
            total += breakdown["ml_confirm_strong"]

        # ── 7. Correlation confirmation (+2) ──
        corr_score = self._check_correlation(
            signal_data, enrichment, recent_signals
        )
        if corr_score > 0:
            breakdown["correlation_confirm"] = corr_score
            total += corr_score

        # ── 8. Liquidity sweep detection (+3) ──
        if self._detect_liquidity_sweep(signal_data, enrichment):
            breakdown["liquidity_sweep"] = SCORE_WEIGHTS["liquidity_sweep"]
            total += breakdown["liquidity_sweep"]

        # ── 9. Conflicting higher timeframe (-3) ──
        htf_conflict = self._check_htf_conflict(
            signal_data, desk, recent_signals
        )
        if htf_conflict:
            breakdown["conflicting_htf"] = SCORE_WEIGHTS["conflicting_htf"]
            total += breakdown["conflicting_htf"]

        # ── 10. RSI divergence bonus ──
        rsi_bonus = self._check_rsi_alignment(signal_data, enrichment)
        if rsi_bonus != 0:
            breakdown["rsi_alignment"] = rsi_bonus
            total += rsi_bonus

        # ── Determine tier and size multiplier ──
        if total >= SCORE_THRESHOLDS["HIGH"]:
            tier = "HIGH"
            size_mult = 1.0
        elif total >= SCORE_THRESHOLDS["MEDIUM"]:
            tier = "MEDIUM"
            size_mult = 0.5
        elif total >= SCORE_THRESHOLDS["LOW"]:
            tier = "LOW"
            size_mult = 0.25
        else:
            tier = "SKIP"
            size_mult = 0.0

        result = {
            "total_score": total,
            "tier": tier,
            "size_multiplier": size_mult,
            "breakdown": breakdown,
            "desk_id": desk_id,
            "ml_score": ml_score,
        }

        logger.info(
            f"Consensus {desk_id} | {signal_data.get('symbol')} | "
            f"Score: {total} ({tier}) | Size: {size_mult*100:.0f}% | "
            f"Breakdown: {breakdown}"
        )

        return result

    def _score_timeframe_alignment(
        self, signal_data: Dict, desk: Dict, recent_signals: Optional[List]
    ) -> Dict:
        """
        Check if higher timeframes agree with signal direction.
        +3 for bias TF match, +2 for confirmation TF match.
        """
        scores = {}
        direction = signal_data.get("direction")
        signal_tf = signal_data.get("timeframe", "")
        desk_tfs = desk.get("timeframes", {})

        if not recent_signals or not direction:
            return scores

        # Look for matching signals on bias timeframe
        bias_tf = desk_tfs.get("bias", "")
        conf_tf = desk_tfs.get("confirmation", "")

        for rs in recent_signals:
            if rs.get("symbol") != signal_data.get("symbol"):
                continue

            rs_dir = rs.get("direction")
            rs_tf = rs.get("timeframe", "")

            # Bias timeframe matches direction
            if rs_tf == bias_tf and rs_dir == direction:
                if "bias_match" not in scores:
                    scores["bias_match"] = SCORE_WEIGHTS["bias_match"]

            # Confirmation timeframe matches
            if rs_tf == conf_tf and rs_dir == direction:
                if "setup_match" not in scores:
                    scores["setup_match"] = SCORE_WEIGHTS["setup_match"]

        return scores

    def _check_correlation(
        self, signal_data: Dict, enrichment: Dict, recent_signals: Optional[List]
    ) -> int:
        """
        Check if correlated assets confirm the signal direction.
        E.g., EURUSD bullish + DXY falling = confirmation.
        """
        direction = signal_data.get("direction")
        symbol = signal_data.get("symbol", "")
        intermarket = enrichment.get("intermarket", {})

        if not direction or not intermarket:
            return 0

        # USD pairs: DXY falling confirms non-USD bullish
        usd_pairs_bullish_when_dxy_falls = [
            "EURUSD", "GBPUSD", "AUDUSD", "NZDUSD"
        ]
        usd_pairs_bullish_when_dxy_rises = [
            "USDJPY", "USDCHF", "USDCAD"
        ]

        dxy = intermarket.get("DXY")
        if dxy and dxy.get("change_pct"):
            dxy_falling = dxy["change_pct"] < -0.1
            dxy_rising = dxy["change_pct"] > 0.1

            if symbol in usd_pairs_bullish_when_dxy_falls:
                if direction == "LONG" and dxy_falling:
                    return SCORE_WEIGHTS["correlation_confirm"]
                if direction == "SHORT" and dxy_rising:
                    return SCORE_WEIGHTS["correlation_confirm"]

            if symbol in usd_pairs_bullish_when_dxy_rises:
                if direction == "LONG" and dxy_rising:
                    return SCORE_WEIGHTS["correlation_confirm"]
                if direction == "SHORT" and dxy_falling:
                    return SCORE_WEIGHTS["correlation_confirm"]

        # Gold: VIX rising + gold bullish = confirmation
        if symbol == "XAUUSD":
            vix = intermarket.get("VIX")
            if vix and vix.get("change_pct"):
                if direction == "LONG" and vix["change_pct"] > 0.5:
                    return SCORE_WEIGHTS["correlation_confirm"]

        # Also check recent signals on correlated pairs
        if recent_signals:
            correlated = self._get_correlated_pairs(symbol)
            for rs in recent_signals:
                if (
                    rs.get("symbol") in correlated
                    and rs.get("direction") == direction
                ):
                    return SCORE_WEIGHTS["correlation_confirm"]

        return 0

    def _detect_liquidity_sweep(
        self, signal_data: Dict, enrichment: Dict
    ) -> bool:
        """
        Detect potential liquidity sweep: sharp spike through a level
        followed by a reversal signal. Contrarian signals after rapid
        moves are the primary indicator.
        """
        alert_type = signal_data.get("alert_type", "")
        atr_pct = enrichment.get("atr_pct")

        # Contrarian signal + high ATR suggests liquidity sweep
        if "contrarian" in alert_type and atr_pct and atr_pct > 1.0:
            return True

        # Plus signal in high volatility could be post-sweep
        if "plus" in alert_type and atr_pct and atr_pct > 1.5:
            return True

        return False

    def _check_htf_conflict(
        self, signal_data: Dict, desk: Dict, recent_signals: Optional[List]
    ) -> bool:
        """Check if higher timeframe signals conflict with this signal."""
        if not recent_signals:
            return False

        direction = signal_data.get("direction")
        symbol = signal_data.get("symbol")
        bias_tf = desk.get("timeframes", {}).get("bias", "")

        for rs in recent_signals:
            if (
                rs.get("symbol") == symbol
                and rs.get("timeframe") == bias_tf
                and rs.get("direction") is not None
                and rs.get("direction") != direction
                and rs.get("direction") != "EXIT"
            ):
                return True

        return False

    def _check_rsi_alignment(
        self, signal_data: Dict, enrichment: Dict
    ) -> int:
        """Bonus/penalty for RSI alignment with signal direction."""
        rsi = enrichment.get("rsi")
        direction = signal_data.get("direction")

        if rsi is None or direction is None:
            return 0

        # Bullish from oversold = +1 bonus
        if direction == "LONG" and rsi < 30:
            return 1
        # Bearish from overbought = +1 bonus
        if direction == "SHORT" and rsi > 70:
            return 1
        # Bullish into overbought = -1 penalty
        if direction == "LONG" and rsi > 80:
            return -1
        # Bearish into oversold = -1 penalty
        if direction == "SHORT" and rsi < 20:
            return -1

        return 0

    def _get_correlated_pairs(self, symbol: str) -> List[str]:
        """Return list of correlated symbols for cross-confirmation."""
        correlation_map = {
            "EURUSD": ["GBPUSD", "NZDUSD"],
            "GBPUSD": ["EURUSD"],
            "AUDUSD": ["NZDUSD"],
            "NZDUSD": ["AUDUSD"],
            "USDJPY": ["EURJPY", "GBPJPY"],
            "EURJPY": ["USDJPY", "GBPJPY"],
            "GBPJPY": ["USDJPY", "EURJPY"],
            "AUDJPY": ["EURJPY"],
            "US30": ["US100", "NAS100"],
            "US100": ["US30", "NAS100"],
            "NAS100": ["US30", "US100"],
            "BTCUSD": ["ETHUSD"],
            "ETHUSD": ["BTCUSD"],
        }
        return correlation_map.get(symbol, [])
