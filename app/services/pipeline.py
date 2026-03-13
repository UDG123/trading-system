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
from app.services.price_service import PriceService
from app.models.signal import Signal
from app.config import (
    DESKS, CAPITAL_PER_ACCOUNT, PORTFOLIO_CAPITAL_PER_DESK,
    get_pip_info, calculate_lot_size,
)
from app.services.ml_data_logger import MLDataLogger

logger = logging.getLogger("TradingSystem.Pipeline")

# Shared service instances (initialized once)
_enricher: TwelveDataEnricher = None
_ml_scorer: MLScorer = None
_consensus: ConsensusScorer = None
_cto: ClaudeCTO = None
_risk_filter: HardRiskFilter = None
_zmq_bridge: ZMQBridge = None
_telegram: TelegramBot = None
_price_service: PriceService = None


def _get_services():
    """Lazy-initialize shared service instances."""
    global _enricher, _ml_scorer, _consensus, _cto, _risk_filter, _zmq_bridge, _telegram, _price_service
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
    if _price_service is None:
        _price_service = PriceService()
    return _enricher, _ml_scorer, _consensus, _cto, _risk_filter, _zmq_bridge, _telegram, _price_service


async def process_signal(signal_id: int, db: Session, webhook_latency_ms: int = None) -> Dict:
    """
    Run the full Phase 2 pipeline on a validated signal.
    Updates the signal record at each stage.

    Args:
        signal_id: ID of the validated signal in the DB.
        db: SQLAlchemy session.
        webhook_latency_ms: Milliseconds between TV alert fire and server arrival.
                           Passed from webhook route for ML training data.

    Pipeline stages:
    1. Load signal from DB
    2. Enrich with TwelveData market data
    2b. Fetch live market price (replaces TV candle close)
    3. Score with ML model
    4. Calculate multi-timeframe consensus
    5. Claude CTO makes final decision
    6. Hard risk filter validates
    7. Update signal record with all results

    Returns pipeline result dict.
    """
    pipeline_start = time.time()
    enricher, ml_scorer, consensus_scorer, cto, risk_filter, zmq_bridge, telegram, price_service = _get_services()

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
        "webhook_latency_ms": webhook_latency_ms,
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
        signal._ml_trade_id = None

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

            # ── 2b. Fetch LIVE market price (replaces TradingView candle close) ──
            try:
                live_price = await price_service.get_price(signal.symbol_normalized)
                if live_price and live_price > 0:
                    logger.info(
                        f"Live price for {signal.symbol_normalized}: {live_price} "
                        f"(TV close was {signal_data.get('price')})"
                    )
                    signal_data["price"] = live_price
                else:
                    logger.warning(
                        f"Live price unavailable for {signal.symbol_normalized}, "
                        f"using TradingView close: {signal_data.get('price')}"
                    )
            except Exception as e:
                logger.warning(f"Live price fetch failed for {signal.symbol_normalized}: {e}")

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
                decision, signal_data, desk_state, desk_id, db=db,
            )

            # Attach enrichment context for notifications
            if trade_params:
                trade_params["trend"] = enrichment.get("trend", "UNKNOWN")
                trade_params["rsi"] = enrichment.get("rsi")
                trade_params["rsi_zone"] = enrichment.get("rsi_zone", "UNKNOWN")
                trade_params["volatility_regime"] = enrichment.get("volatility_regime", "UNKNOWN")
                trade_params["ema50"] = enrichment.get("ema50")
                trade_params["ema200"] = enrichment.get("ema200")
                trade_params["session"] = enrichment.get("active_session", "UNKNOWN")

            if approved:
                signal.status = "DECIDED"
                signal.position_size_pct = trade_params.get("risk_pct")
                signal.desk_id = desk_id

                # ── 9. Create Trade record for server-side sim ──
                from app.models.trade import Trade as TradeModel

                # Calculate lot size based on desk method
                entry_price = signal_data.get("price", 0)
                sl_price = signal_data.get("sl1", 0)
                pip_size, pip_value = get_pip_info(signal_data.get("symbol", ""))
                sl_pips = abs(float(entry_price) - float(sl_price)) / pip_size if entry_price and sl_price and pip_size else 0

                desk_capital = PORTFOLIO_CAPITAL_PER_DESK.get(desk_id, CAPITAL_PER_ACCOUNT)
                risk_pct = trade_params.get("risk_pct", 1.0)
                lot_size = calculate_lot_size(
                    desk_id=desk_id,
                    symbol=signal_data.get("symbol", ""),
                    risk_pct=risk_pct,
                    sl_pips=sl_pips,
                    account_capital=desk_capital,
                )
                risk_dollars = desk_capital * (risk_pct / 100)

                trade_record = TradeModel(
                    signal_id=signal.id,
                    desk_id=desk_id,
                    symbol=signal_data.get("symbol"),
                    direction=signal_data.get("direction"),
                    mt5_ticket=900000 + signal.id,  # 900000+ = server sim
                    entry_price=entry_price,
                    lot_size=lot_size,
                    risk_pct=risk_pct,
                    risk_dollars=risk_dollars,
                    stop_loss=signal_data.get("sl1"),
                    take_profit_1=signal_data.get("tp1"),
                    take_profit_2=signal_data.get("tp2"),
                    status="EXECUTED",
                    opened_at=datetime.now(timezone.utc),
                )
                db.add(trade_record)
                db.flush()
                signal._ml_trade_id = trade_record.id
                zmq_result = await zmq_bridge.send_trade(trade_params, signal.id)
                if zmq_result.get("status") == "sent":
                    signal.status = "EXECUTED"

                # ── 11. Telegram notification ──
                await telegram.notify_trade_entry(trade_params, decision)

            elif decision.get("decision") in ["EXECUTE", "REDUCE"]:
                # ── CTO approved but risk filter blocked ──
                # Track as OniAI virtual trade for accuracy data
                signal.status = "ONIAI_VIRTUAL"
                signal.position_size_pct = decision.get("size_multiplier", 1.0)
                signal.desk_id = desk_id
                signal.claude_reasoning = (
                    f"[OniAI] CTO approved but blocked: {rejection_reason}. "
                    f"Original: {decision.get('reasoning', '')}"
                )

                # Build virtual trade params for notification + tracking
                desk = DESKS.get(desk_id, {})
                virtual_params = {
                    "desk_id": desk_id,
                    "symbol": signal_data.get("symbol"),
                    "direction": signal_data.get("direction"),
                    "price": signal_data.get("price"),
                    "timeframe": signal_data.get("timeframe"),
                    "alert_type": signal_data.get("alert_type"),
                    "risk_pct": desk.get("risk_pct", 1.0),
                    "risk_dollars": CAPITAL_PER_ACCOUNT * (desk.get("risk_pct", 1.0) / 100),
                    "stop_loss": signal_data.get("sl1"),
                    "take_profit_1": signal_data.get("tp1"),
                    "take_profit_2": signal_data.get("tp2"),
                    "trailing_stop_pips": desk.get("trailing_stop_pips"),
                    "max_hold_hours": desk.get("max_hold_hours"),
                    "size_multiplier": decision.get("size_multiplier", 1.0),
                    "claude_decision": decision.get("decision"),
                    "claude_reasoning": decision.get("reasoning"),
                    "confidence": decision.get("confidence"),
                    "is_oniai": True,
                    "block_reason": rejection_reason,
                    "trend": enrichment.get("trend", "UNKNOWN"),
                    "rsi": enrichment.get("rsi"),
                    "rsi_zone": enrichment.get("rsi_zone", "UNKNOWN"),
                    "volatility_regime": enrichment.get("volatility_regime", "UNKNOWN"),
                    "session": enrichment.get("active_session", "UNKNOWN"),
                }

                # Send OniAI Telegram notification
                await telegram.notify_oniai_signal(virtual_params, decision)

                # Create a virtual Trade record for tracking
                from app.models.trade import Trade as TradeModel

                # Use the blocking desk's profile for lot sizing (apples to apples)
                oni_entry = signal_data.get("price", 0)
                oni_sl = signal_data.get("sl1", 0)
                oni_pip_size, oni_pip_value = get_pip_info(signal_data.get("symbol", ""))
                oni_sl_pips = abs(float(oni_entry) - float(oni_sl)) / oni_pip_size if oni_entry and oni_sl and oni_pip_size else 0
                oni_risk_pct = desk.get("risk_pct", 1.0)
                oni_lot = calculate_lot_size(
                    desk_id=desk_id,
                    symbol=signal_data.get("symbol", ""),
                    risk_pct=oni_risk_pct,
                    sl_pips=oni_sl_pips,
                    account_capital=CAPITAL_PER_ACCOUNT,
                    profile="SRV_100",  # same profile as blocking desk
                )

                virtual_trade = TradeModel(
                    signal_id=signal.id,
                    desk_id=desk_id,
                    symbol=signal_data.get("symbol"),
                    direction=signal_data.get("direction"),
                    mt5_ticket=800000 + signal.id,
                    entry_price=signal_data.get("price", 0),
                    lot_size=oni_lot,
                    risk_pct=oni_risk_pct,
                    risk_dollars=CAPITAL_PER_ACCOUNT * (oni_risk_pct / 100),
                    stop_loss=signal_data.get("sl1"),
                    take_profit_1=signal_data.get("tp1"),
                    take_profit_2=signal_data.get("tp2"),
                    status="ONIAI_OPEN",
                    opened_at=datetime.now(timezone.utc),
                    close_reason=f"ONIAI|{rejection_reason}",
                )
                db.add(virtual_trade)
                db.flush()
                signal._ml_trade_id = virtual_trade.id

                logger.info(
                    f"OniAI VIRTUAL | {desk_id} | {signal_data.get('symbol')} "
                    f"{signal_data.get('direction')} | Blocked: {rejection_reason}"
                )

            else:
                signal.status = "REJECTED"
                signal.claude_decision = "SKIP"
                signal.claude_reasoning = (
                    f"Risk filter: {rejection_reason}. "
                    f"Original CTO: {decision.get('reasoning', '')}"
                )

            db.commit()

            # ── ML Training Data: log everything for future model training ──
            try:
                from app.services.ml_data_logger import MLDataLogger
                _ml_logger = MLDataLogger()
                _ml_logger.log_signal(
                    db=db,
                    signal_id=signal.id,
                    signal_data=signal_data,
                    enrichment=enrichment,
                    ml_result=ml_result,
                    consensus=consensus,
                    decision=decision,
                    risk_approved=approved,
                    risk_block_reason=rejection_reason,
                    trade_params=trade_params if approved else {},
                    desk_state=desk_state,
                )
                db.commit()
            except Exception as e:
                logger.debug(f"ML data logging failed: {e}")

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
