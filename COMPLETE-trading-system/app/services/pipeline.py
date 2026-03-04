"""
Signal Processing Pipeline
Orchestrates the full Phase 2 pipeline for validated signals:
Enrichment → ML Scoring → Consensus Scoring → Claude CTO → Risk Filter

Runs asynchronously after webhook logs the validated signal.
"""
import json
import time
import logging
from datetime import datetime, timezone
from typing import Dict

from sqlalchemy.orm import Session

from app.services.twelvedata_enricher import TwelveDataEnricher
from app.services.ml_scorer import MLScorer
from app.services.consensus_scorer import ConsensusScorer
from app.services.claude_cto import ClaudeCTO
from app.services.risk_filter import HardRiskFilter
from app.services.zmq_bridge import ZMQBridge
from app.services.telegram_bot import TelegramBot
from app.models.signal import Signal

logger = logging.getLogger("TradingSystem.Pipeline")

# Shared service instances (initialized once)
_enricher: TwelveDataEnricher = None
_ml_scorer: MLScorer = None
_consensus: ConsensusScorer = None
_cto: ClaudeCTO = None
_risk_filter: HardRiskFilter = None
_zmq_bridge: ZMQBridge = None
_telegram: TelegramBot = None


def _get_services():
    """Lazy-initialize shared service instances."""
    global _enricher, _ml_scorer, _consensus, _cto, _risk_filter, _zmq_bridge, _telegram
    if _enricher is None:
        _enricher = TwelveDataEnricher()
    if _ml_scorer is None:
        _ml_scorer = MLScorer()
    if _consensus is None:
        _consensus = ConsensusScorer()
    if _cto is None:
        _cto = ClaudeCTO()
    if _risk_filter is None:
        _risk_filter = HardRiskFilter()
    if _zmq_bridge is None:
        _zmq_bridge = ZMQBridge()
    if _telegram is None:
        _telegram = TelegramBot()
    return _enricher, _ml_scorer, _consensus, _cto, _risk_filter, _zmq_bridge, _telegram


async def process_signal(signal_id: int, db: Session) -> Dict:
    """
    Run the full Phase 2 pipeline on a validated signal.
    Updates the signal record at each stage.

    Pipeline stages:
    1. Load signal from DB
    2. Enrich with TwelveData market data
    3. Score with ML model
    4. Calculate multi-timeframe consensus
    5. Claude CTO makes final decision
    6. Hard risk filter validates
    7. Update signal record with all results

    Returns pipeline result dict.
    """
    pipeline_start = time.time()
    enricher, ml_scorer, consensus_scorer, cto, risk_filter, zmq_bridge, telegram = _get_services()

    # ── 1. Load signal ──
    signal = db.query(Signal).filter(Signal.id == signal_id).first()
    if not signal:
        logger.error(f"Signal {signal_id} not found")
        return {"status": "error", "message": "Signal not found"}

    if signal.status == "REJECTED":
        return {"status": "skipped", "message": "Signal already rejected"}

    signal_data = {
        "symbol": signal.symbol_normalized,
        "timeframe": signal.timeframe,
        "alert_type": signal.alert_type,
        "direction": signal.direction,
        "price": signal.price,
        "tp1": signal.tp1,
        "tp2": signal.tp2,
        "sl1": signal.sl1,
        "sl2": signal.sl2,
        "smart_trail": signal.smart_trail,
    }

    desks = signal.desks_matched or []
    if not desks:
        signal.status = "REJECTED"
        signal.validation_errors = ["No desk match for pipeline processing"]
        db.commit()
        return {"status": "rejected", "message": "No desk match"}

    logger.info(
        f"═══ PIPELINE START | Signal #{signal_id} | "
        f"{signal.symbol_normalized} {signal.alert_type} | "
        f"Desks: {desks} ═══"
    )

    # Process for each matched desk
    results = {}

    for desk_id in desks:
        desk_start = time.time()

        logger.info(f"── Processing for {desk_id} ──")

        try:
            # ── 2. Enrich with market data ──
            signal.status = "ENRICHING"
            db.commit()

            enrichment = await enricher.enrich(
                signal.symbol_normalized,
                signal.timeframe,
                signal.price,
            )

            signal.status = "ENRICHED"
            db.commit()

            # ── 3. ML model scoring ──
            signal.status = "SCORING"
            db.commit()

            ml_result = ml_scorer.score(signal_data, enrichment, desk_id)
            signal.ml_score = ml_result["ml_score"]

            signal.status = "SCORED"
            db.commit()

            # ── 4. Get recent signals for consensus ──
            recent_signals = risk_filter.get_recent_signals(
                db, signal.symbol_normalized
            )

            # ── 5. Consensus scoring ──
            consensus = consensus_scorer.score(
                signal_data, enrichment, desk_id, ml_result, recent_signals
            )
            signal.consensus_score = consensus["total_score"]

            # ── 6. Get desk state and firm risk ──
            desk_state = risk_filter.get_desk_state(db, desk_id)
            firm_risk = risk_filter.get_firm_risk(db)

            # ── 7. Claude CTO decision ──
            signal.status = "DECIDING"
            db.commit()

            decision = await cto.decide(
                signal_data, enrichment, ml_result,
                consensus, desk_state, firm_risk,
            )

            signal.claude_decision = decision["decision"]
            signal.claude_reasoning = decision.get("reasoning", "")

            # ── 8. Hard risk filter ──
            approved, rejection_reason, trade_params = risk_filter.validate_trade(
                decision, signal_data, desk_state, desk_id,
            )

            if approved:
                signal.status = "DECIDED"
                signal.position_size_pct = trade_params.get("risk_pct")
                signal.desk_id = desk_id

                # ── 9. Send trade to MT5 via ZeroMQ ──
                zmq_result = await zmq_bridge.send_trade(trade_params, signal.id)
                if zmq_result.get("status") == "sent":
                    signal.status = "EXECUTED"

                # ── 10. Telegram notification ──
                await telegram.notify_trade_entry(trade_params, decision)

            else:
                signal.status = "REJECTED"
                signal.claude_decision = "SKIP"
                signal.claude_reasoning = (
                    f"Risk filter: {rejection_reason}. "
                    f"Original CTO: {decision.get('reasoning', '')}"
                )

            db.commit()

            desk_time = int((time.time() - desk_start) * 1000)

            results[desk_id] = {
                "decision": decision["decision"] if approved else "SKIP",
                "approved": approved,
                "rejection_reason": rejection_reason,
                "consensus_score": consensus["total_score"],
                "consensus_tier": consensus["tier"],
                "ml_score": ml_result["ml_score"],
                "ml_method": ml_result["ml_method"],
                "size_multiplier": decision.get("size_multiplier", 0),
                "trade_params": trade_params if approved else None,
                "reasoning": decision.get("reasoning", ""),
                "risk_flags": decision.get("risk_flags", []),
                "enrichment_summary": {
                    "rsi": enrichment.get("rsi"),
                    "volatility_regime": enrichment.get("volatility_regime"),
                    "session": enrichment.get("active_session"),
                    "kill_zone": enrichment.get("kill_zone_type"),
                },
                "processing_time_ms": desk_time,
            }

            logger.info(
                f"── {desk_id} COMPLETE | Decision: {results[desk_id]['decision']} | "
                f"Consensus: {consensus['total_score']} ({consensus['tier']}) | "
                f"ML: {ml_result['ml_score']:.2f} | "
                f"{desk_time}ms ──"
            )

        except Exception as e:
            logger.error(f"Pipeline error for {desk_id}: {e}", exc_info=True)
            signal.status = "ERROR"
            db.commit()
            results[desk_id] = {
                "decision": "SKIP",
                "approved": False,
                "error": str(e),
            }

    # ── Update total processing time ──
    total_ms = int((time.time() - pipeline_start) * 1000)
    signal.processing_time_ms = total_ms
    db.commit()

    logger.info(
        f"═══ PIPELINE COMPLETE | Signal #{signal_id} | "
        f"Total: {total_ms}ms | "
        f"Results: {json.dumps({k: v['decision'] for k, v in results.items()})} ═══"
    )

    return {
        "status": "processed",
        "signal_id": signal_id,
        "results": results,
        "total_processing_time_ms": total_ms,
    }
