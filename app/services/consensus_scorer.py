"""
Consensus Scoring Engine v5.9 — Institutional Floor Scoring
Produces a score: HIGH (8+), MEDIUM (5-7), LOW (3-4), SKIP (<3).

Changes from v5.8:
- Plus signals score +2 (doubled from +1)
- Kill zone overlap scores +3 (up from +2)
- HTF conflict scores -4 (up from -3)
- RSI alignment +2/-2 (up from +1/-1)
- NEW: ADX trending bonus (+1), ranging penalty (-1)
- NEW: VIX elevated penalty (-2) for indices/equities
- NEW: VIX gold bullish bonus (+1) when VIX > 25
- NEW: DXY headwind penalty (-1) for USD pairs
- NEW: Multi-analyst agreement bonus (+2)
- Thresholds raised: HIGH=8, MEDIUM=5, LOW=3 (pre-filtering makes signals higher quality)
"""
import logging
from typing import Dict, List, Optional

from app.config import DESKS, SCORE_WEIGHTS, SCORE_THRESHOLDS, VIX_REGIMES

logger = logging.getLogger("TradingSystem.Consensus")


class ConsensusScorer:

    def score(
        self,
        signal_data: Dict,
        enrichment: Dict,
        desk_id: str,
        ml_result: Dict,
        recent_signals: Optional[List[Dict]] = None,
    ) -> Dict:
        desk = DESKS.get(desk_id, {})
        breakdown = {}
        total = 0

        alert_type = signal_data.get("alert_type", "")

        # ── 1. Entry trigger — differentiate normal vs plus ──
        entry_types = {
            "bullish_confirmation", "bearish_confirmation",
            "bullish_plus", "bearish_plus",
            "bullish_confirmation_plus", "bearish_confirmation_plus",
            "contrarian_bullish", "contrarian_bearish",
        }
        if alert_type in entry_types:
            if "plus" in alert_type:
                breakdown["entry_trigger"] = SCORE_WEIGHTS.get("entry_trigger_plus", 2)
            else:
                breakdown["entry_trigger"] = SCORE_WEIGHTS.get("entry_trigger_normal", 1)
            total += breakdown["entry_trigger"]

        # ── 1b. LuxAlgo provided SL and TP (+1) ──
        if signal_data.get("sl1") and signal_data.get("tp1"):
            breakdown["defined_risk"] = SCORE_WEIGHTS.get("defined_sl_tp", 1)
            total += breakdown["defined_risk"]

        # ── 2. Confirmation Turn+ bonus ──
        if alert_type == "confirmation_turn_plus":
            breakdown["confirmation_turn_plus"] = SCORE_WEIGHTS.get("confirmation_turn_plus", 2)
            total += breakdown["confirmation_turn_plus"]

        if recent_signals:
            for rs in recent_signals:
                if (
                    rs.get("symbol") == signal_data.get("symbol")
                    and rs.get("alert_type") == "confirmation_turn_plus"
                    and "confirmation_turn_plus" not in breakdown
                ):
                    breakdown["confirmation_turn_plus"] = SCORE_WEIGHTS.get("confirmation_turn_plus", 2)
                    total += breakdown["confirmation_turn_plus"]
                    break

        # ── 3. Timeframe alignment ──
        tf_scores = self._score_timeframe_alignment(signal_data, desk, recent_signals)
        breakdown.update(tf_scores)
        total += sum(tf_scores.values())

        # ── 4. Kill Zone bonuses ──
        kz_type = enrichment.get("kill_zone_type", "NONE")
        if kz_type == "OVERLAP":
            breakdown["kill_zone_overlap"] = SCORE_WEIGHTS.get("kill_zone_overlap", 3)
            total += breakdown["kill_zone_overlap"]
        elif kz_type in ("LONDON_OPEN", "NY_OPEN"):
            breakdown["kill_zone_single"] = SCORE_WEIGHTS.get("kill_zone_single", 1)
            total += breakdown["kill_zone_single"]

        # ── 5. ML classifier confirmation ──
        ml_score = ml_result.get("ml_score", 0.5)
        if ml_score >= 0.65:
            breakdown["ml_confirm"] = SCORE_WEIGHTS.get("ml_confirm_per_tf", 1)
            total += breakdown["ml_confirm"]
        if ml_score >= 0.80:
            breakdown["ml_confirm_strong"] = SCORE_WEIGHTS.get("ml_confirm_per_tf", 1)
            total += breakdown["ml_confirm_strong"]

        # ── 6. Correlation confirmation ──
        corr_score = self._check_correlation(signal_data, enrichment, recent_signals)
        if corr_score > 0:
            breakdown["correlation_confirm"] = corr_score
            total += corr_score

        # ── 7. Liquidity sweep detection ──
        if self._detect_liquidity_sweep(signal_data, enrichment):
            breakdown["liquidity_sweep"] = SCORE_WEIGHTS.get("liquidity_sweep", 3)
            total += breakdown["liquidity_sweep"]

        # ── 8. HTF conflict ──
        if self._check_htf_conflict(signal_data, desk, recent_signals):
            breakdown["conflicting_htf"] = SCORE_WEIGHTS.get("conflicting_htf", -4)
            total += breakdown["conflicting_htf"]

        # ── 9. RSI alignment (v5.9: stronger weights) ──
        rsi_bonus = self._check_rsi_alignment(signal_data, enrichment)
        if rsi_bonus != 0:
            breakdown["rsi_alignment"] = rsi_bonus
            total += rsi_bonus

        # ── 10. NEW: ADX trend strength ──
        adx = enrichment.get("adx")
        if adx is not None:
            if adx >= 25:
                breakdown["adx_trending"] = SCORE_WEIGHTS.get("adx_trending", 1)
                total += breakdown["adx_trending"]
            elif adx < 20:
                breakdown["adx_ranging"] = SCORE_WEIGHTS.get("adx_ranging", -1)
                total += breakdown["adx_ranging"]

        # ── 11. NEW: VIX regime for indices/equities/momentum ──
        vix_level = 0
        intermarket = enrichment.get("intermarket", {})
        vix_data = intermarket.get("VIX", {})
        if vix_data and vix_data.get("price"):
            vix_level = float(vix_data["price"])

        if vix_level > VIX_REGIMES.get("ELEVATED", 25):
            if desk_id in ("DESK5_ALTS", "DESK6_EQUITIES"):
                breakdown["vix_elevated"] = SCORE_WEIGHTS.get("vix_elevated", -2)
                total += breakdown["vix_elevated"]

            # Gold gets a bonus when VIX is high (safe-haven)
            if desk_id == "DESK4_GOLD" and signal_data.get("direction") == "LONG":
                breakdown["vix_gold_bullish"] = SCORE_WEIGHTS.get("vix_gold_bullish", 1)
                total += breakdown["vix_gold_bullish"]

        # ── 12. NEW: DXY headwind for USD pairs ──
        dxy_headwind = self._check_dxy_headwind(signal_data, enrichment)
        if dxy_headwind:
            breakdown["dxy_headwind"] = SCORE_WEIGHTS.get("dxy_headwind", -1)
            total += breakdown["dxy_headwind"]

        # ── 13. NEW: Multi-analyst agreement ──
        # If another signal on the same symbol from a different TF agrees in direction
        if recent_signals:
            agreement = self._check_multi_analyst_agreement(signal_data, desk_id, recent_signals)
            if agreement:
                breakdown["multi_analyst_agree"] = SCORE_WEIGHTS.get("multi_analyst_agree", 2)
                total += breakdown["multi_analyst_agree"]

        # ── Determine tier ──
        if total >= SCORE_THRESHOLDS["HIGH"]:
            tier, size_mult = "HIGH", 1.0
        elif total >= SCORE_THRESHOLDS["MEDIUM"]:
            tier, size_mult = "MEDIUM", 0.6
        elif total >= SCORE_THRESHOLDS["LOW"]:
            tier, size_mult = "LOW", 0.3
        else:
            tier, size_mult = "SKIP", 0.0

        result = {
            "total_score": total,
            "tier": tier,
            "size_multiplier": size_mult,
            "breakdown": breakdown,
            "desk_id": desk_id,
            "ml_score": ml_score,
            "vix_level": vix_level,
        }

        logger.info(
            f"Consensus {desk_id} | {signal_data.get('symbol')} | "
            f"Score: {total} ({tier}) | Size: {size_mult*100:.0f}% | "
            f"Breakdown: {breakdown}"
        )

        return result

    # ─────────────────────────────────────────────────
    # HELPER METHODS
    # ─────────────────────────────────────────────────

    def _score_timeframe_alignment(self, signal_data, desk, recent_signals):
        scores = {}
        direction = signal_data.get("direction")
        desk_tfs = desk.get("timeframes", {})
        if not recent_signals or not direction:
            return scores

        bias_tf = desk_tfs.get("bias", "")
        conf_tf = desk_tfs.get("confirmation", "")

        for rs in recent_signals:
            if rs.get("symbol") != signal_data.get("symbol"):
                continue
            rs_dir = rs.get("direction")
            rs_tf = rs.get("timeframe", "")

            if rs_tf == bias_tf and rs_dir == direction and "bias_match" not in scores:
                scores["bias_match"] = SCORE_WEIGHTS.get("bias_match", 3)
            if rs_tf == conf_tf and rs_dir == direction and "setup_match" not in scores:
                scores["setup_match"] = SCORE_WEIGHTS.get("setup_match", 2)

        return scores

    def _check_correlation(self, signal_data, enrichment, recent_signals):
        direction = signal_data.get("direction")
        symbol = signal_data.get("symbol", "")
        intermarket = enrichment.get("intermarket", {})
        if not direction or not intermarket:
            return 0

        usd_long_when_dxy_falls = ["EURUSD", "GBPUSD", "AUDUSD", "NZDUSD"]
        usd_long_when_dxy_rises = ["USDJPY", "USDCHF", "USDCAD"]

        dxy = intermarket.get("DXY")
        if dxy and dxy.get("change_pct"):
            dxy_falling = dxy["change_pct"] < -0.1
            dxy_rising = dxy["change_pct"] > 0.1
            if symbol in usd_long_when_dxy_falls:
                if (direction == "LONG" and dxy_falling) or (direction == "SHORT" and dxy_rising):
                    return SCORE_WEIGHTS.get("correlation_confirm", 2)
            if symbol in usd_long_when_dxy_rises:
                if (direction == "LONG" and dxy_rising) or (direction == "SHORT" and dxy_falling):
                    return SCORE_WEIGHTS.get("correlation_confirm", 2)

        if symbol == "XAUUSD":
            vix = intermarket.get("VIX")
            if vix and vix.get("change_pct"):
                if direction == "LONG" and vix["change_pct"] > 0.5:
                    return SCORE_WEIGHTS.get("correlation_confirm", 2)

        if recent_signals:
            correlated = self._get_correlated_pairs(symbol)
            for rs in recent_signals:
                if rs.get("symbol") in correlated and rs.get("direction") == direction:
                    return SCORE_WEIGHTS.get("correlation_confirm", 2)

        return 0

    def _detect_liquidity_sweep(self, signal_data, enrichment):
        alert_type = signal_data.get("alert_type", "")
        atr_pct = enrichment.get("atr_pct")
        if "contrarian" in alert_type and atr_pct and atr_pct > 1.0:
            return True
        if "plus" in alert_type and atr_pct and atr_pct > 1.5:
            return True
        return False

    def _check_htf_conflict(self, signal_data, desk, recent_signals):
        if not recent_signals:
            return False
        direction = signal_data.get("direction")
        symbol = signal_data.get("symbol")
        bias_tf = desk.get("timeframes", {}).get("bias", "")
        for rs in recent_signals:
            if (
                rs.get("symbol") == symbol
                and rs.get("timeframe") == bias_tf
                and rs.get("direction") not in (None, "EXIT", direction)
            ):
                return True
        return False

    def _check_rsi_alignment(self, signal_data, enrichment):
        rsi = enrichment.get("rsi")
        direction = signal_data.get("direction")
        if rsi is None or direction is None:
            return 0
        # v5.9: stronger bonuses and penalties
        if direction == "LONG" and rsi < 30:
            return SCORE_WEIGHTS.get("rsi_aligned", 2)
        if direction == "SHORT" and rsi > 70:
            return SCORE_WEIGHTS.get("rsi_aligned", 2)
        if direction == "LONG" and rsi > 75:
            return SCORE_WEIGHTS.get("rsi_counter", -2)
        if direction == "SHORT" and rsi < 25:
            return SCORE_WEIGHTS.get("rsi_counter", -2)
        return 0

    def _check_dxy_headwind(self, signal_data, enrichment):
        """DXY moving against the trade direction for USD pairs."""
        direction = signal_data.get("direction")
        symbol = signal_data.get("symbol", "")
        intermarket = enrichment.get("intermarket", {})
        dxy = intermarket.get("DXY")
        if not dxy or not dxy.get("change_pct"):
            return False

        dxy_change = dxy["change_pct"]
        usd_long_when_dxy_falls = ["EURUSD", "GBPUSD", "AUDUSD", "NZDUSD"]
        usd_long_when_dxy_rises = ["USDJPY", "USDCHF", "USDCAD"]

        # Headwind = DXY moving against your trade
        if symbol in usd_long_when_dxy_falls:
            if direction == "LONG" and dxy_change > 0.15:
                return True
            if direction == "SHORT" and dxy_change < -0.15:
                return True
        if symbol in usd_long_when_dxy_rises:
            if direction == "LONG" and dxy_change < -0.15:
                return True
            if direction == "SHORT" and dxy_change > 0.15:
                return True
        return False

    def _check_multi_analyst_agreement(self, signal_data, desk_id, recent_signals):
        """Check if another signal on the same symbol from a different TF agrees."""
        symbol = signal_data.get("symbol")
        direction = signal_data.get("direction")
        signal_tf = signal_data.get("timeframe", "")

        if not recent_signals or not direction:
            return False

        for rs in recent_signals:
            if (
                rs.get("symbol") == symbol
                and rs.get("direction") == direction
                and rs.get("timeframe") != signal_tf
                and rs.get("timeframe")  # must have a different TF
            ):
                return True
        return False

    def _get_correlated_pairs(self, symbol):
        correlation_map = {
            "EURUSD": ["GBPUSD", "NZDUSD"],
            "GBPUSD": ["EURUSD"],
            "AUDUSD": ["NZDUSD", "AUDCAD"],
            "NZDUSD": ["AUDUSD"],
            "USDCHF": ["EURUSD"],  # inverse
            "EURCHF": ["EURUSD", "USDCHF"],
            "USDJPY": ["EURJPY", "GBPJPY"],
            "EURJPY": ["USDJPY", "GBPJPY", "AUDJPY"],
            "GBPJPY": ["USDJPY", "EURJPY"],
            "AUDJPY": ["EURJPY", "NZDJPY"],
            "CADJPY": ["EURJPY", "GBPJPY"],
            "NZDJPY": ["AUDJPY", "CADJPY"],
            "CHFJPY": ["EURJPY"],
            "EURGBP": ["EURUSD", "GBPUSD"],
            "EURAUD": ["EURUSD", "AUDUSD"],
            "GBPAUD": ["GBPUSD", "AUDUSD"],
            "GBPCAD": ["GBPUSD", "USDCAD"],
            "GBPNZD": ["GBPUSD", "NZDUSD"],
            "EURNZD": ["EURUSD", "NZDUSD"],
            "AUDCAD": ["AUDUSD", "USDCAD"],
            "NAS100": ["US30", "BTCUSD"],
            "US30":   ["NAS100"],
            "BTCUSD": ["ETHUSD", "NAS100", "SOLUSD"],
            "ETHUSD": ["BTCUSD", "SOLUSD"],
            "SOLUSD": ["BTCUSD", "ETHUSD"],
            "XRPUSD": ["BTCUSD"],
            "LINKUSD": ["BTCUSD", "ETHUSD"],
            "XAUUSD": ["XAGUSD"],
            "XAGUSD": ["XAUUSD"],
            "WTIUSD": [],
        }
        return correlation_map.get(symbol, [])
