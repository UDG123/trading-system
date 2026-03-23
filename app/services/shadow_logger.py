"""
ShadowLogger — Logs EVERY signal to shadow_signals with full feature context.
Runs in PARALLEL with the existing pipeline, not replacing it.
Gate values are logged as FEATURE COLUMNS, not used as filters.
"would_block" boolean flags record what the live pipeline WOULD have done.
"""
import logging
from datetime import datetime, timezone
from typing import Dict, Optional, List

from sqlalchemy.orm import Session

from app.models.shadow_signal import ShadowSignal
from app.config import get_pip_info, CORRELATION_GROUPS

logger = logging.getLogger("TradingSystem.ShadowLogger")

# Gate thresholds (mirrored from worker/pipeline for would_block computation)
HURST_CHOP_THRESHOLD = 0.52
INDICATOR_ALIGNMENT_MIN = 0.70
CONSENSUS_MIN_SCORE = 3


class ShadowLogger:
    """Logs every signal to shadow_signals regardless of gate decisions."""

    def __init__(self, db_session_factory=None):
        self._db_factory = db_session_factory

    async def log_signal(
        self,
        db: Session,
        payload: Dict,
        enrichment: Optional[Dict] = None,
        ml_result: Optional[Dict] = None,
        consensus: Optional[Dict] = None,
        decision: Optional[Dict] = None,
        desk_state=None,
        desk_id: str = None,
    ) -> int:
        """
        Log a signal with full context to shadow_signals.
        Returns the new shadow_signal_id.
        """
        enrichment = enrichment or {}
        ml_result = ml_result or {}
        consensus = consensus or {}
        decision = decision or {}

        symbol = payload.get("symbol_normalized", payload.get("symbol", ""))
        direction = payload.get("direction")
        price = float(payload.get("price", 0) or 0)

        # Gate feature values
        hurst = enrichment.get("hurst_exponent")
        alignment = enrichment.get("indicator_alignment")
        cons_score = consensus.get("total_score")
        cons_tier = consensus.get("tier")
        ml_score = ml_result.get("ml_score")
        claude_dec = decision.get("decision")
        claude_conf = decision.get("confidence")
        claude_reason = decision.get("reasoning", "")

        # Compute would_block flags
        hurst_would_block = hurst is not None and hurst < HURST_CHOP_THRESHOLD
        alignment_would_block = alignment is not None and alignment < INDICATOR_ALIGNMENT_MIN
        consensus_would_block = cons_score is not None and cons_score < CONSENSUS_MIN_SCORE
        cto_would_skip = claude_dec == "SKIP"

        # Live pipeline approved = all gates passed
        live_pipeline_approved = (
            not hurst_would_block
            and not alignment_would_block
            and not consensus_would_block
            and not cto_would_skip
        )

        # Intermarket data
        intermarket = enrichment.get("intermarket", {})
        vix_data = intermarket.get("VIX", {}) or {}
        dxy_data = intermarket.get("DXY", {}) or {}

        # Correlation group
        corr_group = None
        corr_count = 0
        for group_name, group in CORRELATION_GROUPS.items():
            if symbol in group.get("symbols", []):
                corr_group = group_name
                break

        # Build feature vector
        feature_vector = self.compute_feature_vector(
            payload, enrichment, ml_result, consensus
        )

        record = ShadowSignal(
            created_at=datetime.now(timezone.utc),
            # Signal identity
            symbol=symbol[:20] if symbol else "",
            direction=direction,
            alert_type=str(payload.get("alert_type", ""))[:50],
            timeframe=str(payload.get("timeframe", ""))[:10],
            desk_id=desk_id or (payload.get("desks_matched", [None]) or [None])[0],
            price=price,
            # LuxAlgo levels
            sl1=payload.get("sl1"),
            tp1=payload.get("tp1"),
            tp2=payload.get("tp2"),
            smart_trail=payload.get("smart_trail"),
            # Gate values as features
            hurst_exponent=hurst,
            indicator_alignment=alignment,
            consensus_score=cons_score,
            consensus_tier=cons_tier,
            ml_score=ml_score,
            claude_decision=claude_dec,
            claude_confidence=claude_conf,
            claude_reasoning=claude_reason[:2000] if claude_reason else None,
            # Would-block flags
            hurst_would_block=hurst_would_block,
            alignment_would_block=alignment_would_block,
            consensus_would_block=consensus_would_block,
            cto_would_skip=cto_would_skip,
            live_pipeline_approved=live_pipeline_approved,
            # Market context
            rsi=enrichment.get("rsi"),
            adx=enrichment.get("adx"),
            atr=enrichment.get("atr"),
            atr_pct=enrichment.get("atr_pct"),
            ema50=enrichment.get("ema50"),
            ema200=enrichment.get("ema200"),
            trend=enrichment.get("trend"),
            volatility_regime=enrichment.get("volatility_regime"),
            active_session=enrichment.get("active_session"),
            kill_zone_type=enrichment.get("kill_zone_type"),
            vix_level=float(vix_data.get("price", 0)) if vix_data else None,
            dxy_change_pct=float(dxy_data.get("change_pct", 0)) if dxy_data else None,
            # Cross-asset
            correlated_open_count=corr_count,
            correlation_group=corr_group,
            # Volume
            volume=enrichment.get("volume"),
            rvol_multiplier=enrichment.get("mse_rvol"),
            vwap_z_score=enrichment.get("vwap_z_score"),
            # Latency
            webhook_latency_ms=payload.get("webhook_latency_ms"),
            # Raw data
            raw_payload=payload,
            enrichment_data=enrichment,
            feature_vector=feature_vector,
            desks_matched=payload.get("desks_matched"),
        )

        db.add(record)
        db.flush()

        logger.debug(
            f"Shadow logged #{record.id} | {symbol} {direction} | "
            f"approved={live_pipeline_approved} | H={hurst} | "
            f"cons={cons_score} | ml={ml_score}"
        )

        return record.id

    def update_post_pipeline(
        self,
        db: Session,
        shadow_id: int,
        enrichment: Optional[Dict] = None,
        ml_result: Optional[Dict] = None,
        consensus: Optional[Dict] = None,
        decision: Optional[Dict] = None,
        live_approved: bool = None,
    ):
        """Update shadow record with data available after pipeline completes."""
        record = db.query(ShadowSignal).filter(ShadowSignal.id == shadow_id).first()
        if not record:
            return

        enrichment = enrichment or {}
        ml_result = ml_result or {}
        consensus = consensus or {}
        decision = decision or {}

        # Update fields that may have been None at initial log time
        if record.hurst_exponent is None and enrichment.get("hurst_exponent") is not None:
            record.hurst_exponent = enrichment["hurst_exponent"]
            record.hurst_would_block = enrichment["hurst_exponent"] < HURST_CHOP_THRESHOLD

        if record.ml_score is None and ml_result.get("ml_score") is not None:
            record.ml_score = ml_result["ml_score"]

        if record.consensus_score is None and consensus.get("total_score") is not None:
            record.consensus_score = consensus["total_score"]
            record.consensus_tier = consensus.get("tier")
            record.consensus_would_block = consensus["total_score"] < CONSENSUS_MIN_SCORE

        if record.claude_decision is None and decision.get("decision") is not None:
            record.claude_decision = decision["decision"]
            record.claude_confidence = decision.get("confidence")
            record.claude_reasoning = (decision.get("reasoning") or "")[:2000]
            record.cto_would_skip = decision["decision"] == "SKIP"

        if live_approved is not None:
            record.live_pipeline_approved = live_approved

        # Update enrichment data
        if enrichment and not record.enrichment_data:
            record.enrichment_data = enrichment
            record.rsi = enrichment.get("rsi")
            record.adx = enrichment.get("adx")
            record.atr = enrichment.get("atr")
            record.atr_pct = enrichment.get("atr_pct")
            record.ema50 = enrichment.get("ema50")
            record.ema200 = enrichment.get("ema200")
            record.trend = enrichment.get("trend")
            record.volatility_regime = enrichment.get("volatility_regime")
            record.active_session = enrichment.get("active_session")
            record.kill_zone_type = enrichment.get("kill_zone_type")

            intermarket = enrichment.get("intermarket", {})
            vix_data = intermarket.get("VIX", {}) or {}
            dxy_data = intermarket.get("DXY", {}) or {}
            if vix_data:
                record.vix_level = float(vix_data.get("price", 0))
            if dxy_data:
                record.dxy_change_pct = float(dxy_data.get("change_pct", 0))

        # Recompute feature vector with full data
        if enrichment or ml_result or consensus:
            record.feature_vector = self.compute_feature_vector(
                record.raw_payload or {},
                enrichment or record.enrichment_data or {},
                ml_result,
                consensus,
            )

    def compute_feature_vector(
        self,
        signal_data: Dict,
        enrichment: Dict,
        ml_result: Dict,
        consensus: Dict,
    ) -> Dict:
        """
        Compute ~40 derived features suitable for ML training.
        Returns flat dict.
        """
        now = datetime.now(timezone.utc)
        hour = now.hour
        dow = now.weekday()

        direction = signal_data.get("direction", "")
        alert_type = str(signal_data.get("alert_type", "")).lower()
        price = float(signal_data.get("price", 0) or 0)
        sl1 = signal_data.get("sl1")
        tp1 = signal_data.get("tp1")

        # R:R ratio
        rr_ratio = 0.0
        sl_atr_ratio = 0.0
        if price and sl1 and tp1:
            risk = abs(price - float(sl1))
            reward = abs(float(tp1) - price)
            rr_ratio = round(reward / risk, 2) if risk > 0 else 0.0

        atr = enrichment.get("atr")
        if atr and atr > 0 and price and sl1:
            sl_atr_ratio = round(abs(price - float(sl1)) / float(atr), 2)

        rsi = enrichment.get("rsi")
        rsi_val = float(rsi) if rsi is not None else 50.0

        hurst = enrichment.get("hurst_exponent")
        cons_score = consensus.get("total_score", 0) or 0
        ml_score = (ml_result or {}).get("ml_score", 0.5) or 0.5
        claude_conf = ((signal_data if "confidence" in signal_data else {})
                       .get("confidence", 0.5))
        # Try from consensus/decision dicts
        if claude_conf == 0.5:
            claude_conf = 0.5

        # Pipeline agreement: how many scoring layers agree
        ml_bullish = ml_score > 0.6
        consensus_bullish = cons_score >= 5
        decision = signal_data.get("claude_decision") or ""
        claude_bullish = decision in ("EXECUTE", "REDUCE")
        pipeline_agreement = sum([ml_bullish, consensus_bullish, claude_bullish])

        f = {
            # Signal quality
            "is_plus_signal": "plus" in alert_type,
            "is_confirmation": "confirmation" in alert_type,
            "is_contrarian": "contrarian" in alert_type,
            "rr_ratio": rr_ratio,
            "sl_atr_ratio": sl_atr_ratio,
            # Market regime
            "hurst_exponent": hurst,
            "vol_is_high": enrichment.get("volatility_regime") in ("HIGH_VOLATILITY", "HIGH", "EXTREME"),
            "rsi": rsi_val,
            "rsi_overbought": rsi_val > 70,
            "rsi_oversold": rsi_val < 30,
            "rsi_supports_direction": (
                (rsi_val < 60 if direction == "LONG" else rsi_val > 40)
                if direction else True
            ),
            "adx": enrichment.get("adx"),
            "atr_pct": enrichment.get("atr_pct"),
            "trend": enrichment.get("trend"),
            # Temporal
            "hour_utc": hour,
            "day_of_week": dow,
            "is_london": 7 <= hour < 12,
            "is_overlap": 12 <= hour < 16,
            "is_ny": 16 <= hour < 21,
            "is_asian": 0 <= hour < 7,
            "is_friday": dow == 4,
            "is_monday": dow == 0,
            # Scores
            "ml_score": ml_score,
            "consensus_score": cons_score,
            "claude_confidence": claude_conf,
            "pipeline_agreement": pipeline_agreement,
            # Intermarket
            "vix_level": enrichment.get("intermarket", {}).get("VIX", {}).get("price") if enrichment.get("intermarket") else None,
            "dxy_change_pct": enrichment.get("intermarket", {}).get("DXY", {}).get("change_pct") if enrichment.get("intermarket") else None,
            # Volume
            "rvol_multiplier": enrichment.get("mse_rvol"),
            "vwap_z_score": enrichment.get("vwap_z_score"),
            "volume": enrichment.get("volume"),
            # Session
            "active_session": enrichment.get("active_session"),
            "kill_zone_type": enrichment.get("kill_zone_type"),
            "volatility_regime": enrichment.get("volatility_regime"),
        }

        return f

    def get_unlabeled_signals(
        self, db: Session, limit: int = 1000
    ) -> List[ShadowSignal]:
        """Return shadow signals where tb_label IS NULL, ordered by created_at ASC."""
        return (
            db.query(ShadowSignal)
            .filter(ShadowSignal.tb_label.is_(None))
            .order_by(ShadowSignal.created_at.asc())
            .limit(limit)
            .all()
        )
