"""
Signal Processing Pipeline — SIMULATION MODE
Orchestrates the full pipeline for validated signals:
Enrichment → ML Scoring → Consensus Scoring → Claude CTO → Simulated Trade

ALL CTO-approved signals become simulated trades in the database.
No execution limits, no max_open blocking. Every approved signal is
tracked and broadcast to Telegram for profitability analysis.

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
from app.services.telegram_bot import TelegramBot
from app.services.price_service import PriceService
from app.models.signal import Signal
from app.config import (
    DESKS, CAPITAL_PER_ACCOUNT, PORTFOLIO_CAPITAL_PER_DESK,
    get_pip_info, calculate_lot_size, get_atr_settings,
    VIX_REGIMES, DESK_DAILY_HARD_STOP_PCT,
)
from app.services.ml_data_logger import MLDataLogger

logger = logging.getLogger("TradingSystem.Pipeline")

# Shared service instances (initialized once)
_enricher: TwelveDataEnricher = None
_ml_scorer: MLScorer = None
_consensus: ConsensusScorer = None
_cto: ClaudeCTO = None
_risk_filter: HardRiskFilter = None
_telegram: TelegramBot = None
_price_service: PriceService = None


def _get_services():
    """Lazy-initialize shared service instances."""
    global _enricher, _ml_scorer, _consensus, _cto, _risk_filter, _telegram, _price_service
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
    if _telegram is None:
        _telegram = TelegramBot()
    if _price_service is None:
        _price_service = PriceService()
    return _enricher, _ml_scorer, _consensus, _cto, _risk_filter, _telegram, _price_service


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
    enricher, ml_scorer, consensus_scorer, cto, risk_filter, telegram, price_service = _get_services()

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

            # Check if this signal came from the MSE (has pre-computed data)
            mse_data = None
            try:
                raw = json.loads(signal.raw_payload) if signal.raw_payload else {}
                mse_data = raw.get("mse")
            except (json.JSONDecodeError, TypeError):
                pass

            if mse_data:
                # MSE signal: use pre-computed technicals, fetch intermarket only
                enrichment = await enricher.enrich_from_mse(
                    signal.symbol_normalized,
                    mse_data,
                    signal.price,
                )
                logger.info(
                    f"MSE fast-path enrichment for {signal.symbol_normalized} "
                    f"(confluence={mse_data.get('confluence_score', '?')})"
                )
            else:
                # Standard LuxAlgo signal: full TwelveData enrichment
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

            # ── 2b. Calculate SL/TP from ATR using desk-specific multipliers ──
            price = signal_data.get("price", 0)
            atr = enrichment.get("atr")
            direction = signal_data.get("direction", "LONG")
            timeframe = signal_data.get("timeframe", "")
            symbol = signal_data.get("symbol", "")

            # Get desk+symbol+timeframe specific ATR settings
            atr_cfg = get_atr_settings(desk_id, symbol, timeframe)
            sl_mult = atr_cfg.get("sl_mult", 2.0)
            tp1_mult = atr_cfg.get("tp1_mult", 4.0)
            tp2_mult = atr_cfg.get("tp2_mult", 6.0)

            if price > 0 and atr and atr > 0:
                if not signal_data.get("sl1"):
                    sl_distance = atr * sl_mult
                    if direction == "LONG":
                        signal_data["sl1"] = round(price - sl_distance, 5)
                    else:
                        signal_data["sl1"] = round(price + sl_distance, 5)
                    logger.info(
                        f"ATR SL | {desk_id} | {sl_mult}x ATR({atr:.5f}) = "
                        f"{signal_data['sl1']} (R:R target 1:{tp1_mult/sl_mult:.1f})"
                    )

                if not signal_data.get("tp1"):
                    tp_distance = atr * tp1_mult
                    if direction == "LONG":
                        signal_data["tp1"] = round(price + tp_distance, 5)
                    else:
                        signal_data["tp1"] = round(price - tp_distance, 5)
                    logger.info(f"ATR TP1 | {desk_id} | {tp1_mult}x ATR = {signal_data['tp1']}")

                if not signal_data.get("tp2"):
                    tp2_distance = atr * tp2_mult
                    if direction == "LONG":
                        signal_data["tp2"] = round(price + tp2_distance, 5)
                    else:
                        signal_data["tp2"] = round(price - tp2_distance, 5)

                # ── R:R floor check — reject if below minimum ──
                min_rr = atr_cfg.get("min_rr", 1.5)
                entry = price
                sl = signal_data.get("sl1", 0)
                tp = signal_data.get("tp1", 0)
                if sl and tp and entry:
                    sl_dist = abs(entry - sl)
                    tp_dist = abs(tp - entry)
                    actual_rr = tp_dist / sl_dist if sl_dist > 0 else 0
                    if actual_rr < min_rr and sl_dist > 0:
                        logger.warning(
                            f"R:R {actual_rr:.2f} below minimum {min_rr} for {desk_id} — "
                            f"adjusting TP1 to meet floor"
                        )
                        # Adjust TP to meet minimum R:R
                        required_tp_dist = sl_dist * min_rr
                        if direction == "LONG":
                            signal_data["tp1"] = round(entry + required_tp_dist, 5)
                        else:
                            signal_data["tp1"] = round(entry - required_tp_dist, 5)

            # ── 2c. VIX regime check — halt momentum/equity desks if VIX too high ──
            vix_halt = DESKS.get(desk_id, {}).get("vix_halt_above")
            if vix_halt:
                vix_data = enrichment.get("intermarket", {}).get("VIX", {})
                vix_price = float(vix_data.get("price", 0)) if vix_data else 0
                if vix_price > vix_halt:
                    signal.status = "REJECTED"
                    signal.validation_errors = [f"VIX {vix_price:.1f} > halt threshold {vix_halt}"]
                    db.commit()
                    results[desk_id] = {
                        "decision": "SKIP", "approved": False,
                        "rejection_reason": f"VIX halt: {vix_price:.1f} > {vix_halt}",
                    }
                    logger.warning(f"VIX HALT | {desk_id} | VIX={vix_price:.1f} > {vix_halt}")
                    continue

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

            # ── 8. Risk flags (advisory only — does NOT block trades) ──
            risk_flags = []
            session_ok, session_msg = risk_filter._check_session(desk_id, DESKS.get(desk_id, {}))
            if not session_ok:
                risk_flags.append(f"session: {session_msg}")

            if db:
                corr_ok, corr_msg = risk_filter._check_correlation(
                    db, signal_data.get("symbol"), signal_data.get("direction")
                )
                if not corr_ok:
                    risk_flags.append(f"correlation: {corr_msg}")

            # Build trade params for ALL CTO-approved signals
            desk = DESKS.get(desk_id, {})
            risk_pct = desk.get("risk_pct", 1.0)
            size_mult = decision.get("size_multiplier", 1.0)
            desk_modifier = desk_state.get("size_modifier", 1.0)
            effective_risk_pct = min(risk_pct * size_mult * desk_modifier, risk_pct)
            risk_dollars = CAPITAL_PER_ACCOUNT * (effective_risk_pct / 100)

            trade_params = {
                "desk_id": desk_id,
                "symbol": signal_data.get("symbol"),
                "direction": signal_data.get("direction"),
                "price": signal_data.get("price"),
                "timeframe": signal_data.get("timeframe"),
                "alert_type": signal_data.get("alert_type"),
                "risk_pct": round(effective_risk_pct, 4),
                "risk_dollars": round(risk_dollars, 2),
                "stop_loss": signal_data.get("sl1"),
                "take_profit_1": signal_data.get("tp1"),
                "take_profit_2": signal_data.get("tp2"),
                "trailing_stop_pips": desk.get("trailing_stop_pips"),
                "max_hold_hours": desk.get("max_hold_hours"),
                "size_multiplier": round(size_mult * desk_modifier, 4),
                "claude_decision": decision.get("decision"),
                "claude_reasoning": decision.get("reasoning"),
                "confidence": decision.get("confidence"),
                "trend": enrichment.get("trend", "UNKNOWN"),
                "rsi": enrichment.get("rsi"),
                "rsi_zone": enrichment.get("rsi_zone", "UNKNOWN"),
                "volatility_regime": enrichment.get("volatility_regime", "UNKNOWN"),
                "ema50": enrichment.get("ema50"),
                "ema200": enrichment.get("ema200"),
                "session": enrichment.get("active_session", "UNKNOWN"),
                "risk_flags": risk_flags,
            }

            # Desk index for unique ticket generation
            desk_idx = desks.index(desk_id) if desk_id in desks else 0

            if decision.get("decision") in ("EXECUTE", "REDUCE"):
                # ── 9. CTO APPROVED → Create simulated trade ──
                signal.status = "SIM_OPEN"
                signal.position_size_pct = trade_params.get("risk_pct")
                signal.desk_id = desk_id

                from app.models.trade import Trade as TradeModel

                # Calculate lot size
                entry_price = signal_data.get("price", 0)
                sl_price = signal_data.get("sl1", 0)
                pip_size, pip_value = get_pip_info(signal_data.get("symbol", ""))
                sl_pips = abs(float(entry_price) - float(sl_price)) / pip_size if entry_price and sl_price and pip_size else 0

                desk_capital = PORTFOLIO_CAPITAL_PER_DESK.get(desk_id, CAPITAL_PER_ACCOUNT)
                lot_size = calculate_lot_size(
                    desk_id=desk_id,
                    symbol=signal_data.get("symbol", ""),
                    risk_pct=effective_risk_pct,
                    sl_pips=sl_pips,
                    account_capital=desk_capital,
                )

                # Unique ticket: 900000 + signal_id * 10 + desk_index
                trade_record = TradeModel(
                    signal_id=signal.id,
                    desk_id=desk_id,
                    symbol=signal_data.get("symbol"),
                    direction=signal_data.get("direction"),
                    mt5_ticket=900000 + signal.id * 10 + desk_idx,
                    entry_price=entry_price,
                    lot_size=lot_size,
                    risk_pct=effective_risk_pct,
                    risk_dollars=risk_dollars,
                    stop_loss=signal_data.get("sl1"),
                    take_profit_1=signal_data.get("tp1"),
                    take_profit_2=signal_data.get("tp2"),
                    status="SIM_OPEN",
                    opened_at=datetime.now(timezone.utc),
                    close_reason="|".join(risk_flags) if risk_flags else None,
                )
                db.add(trade_record)
                db.flush()
                signal._ml_trade_id = trade_record.id

                # ── 10. Telegram notification ──
                await telegram.notify_trade_entry(trade_params, decision)

                logger.info(
                    f"SIM TRADE #{trade_record.id} | {desk_id} | "
                    f"{signal_data.get('symbol')} {signal_data.get('direction')} | "
                    f"Lot: {lot_size} | Risk: ${risk_dollars:.2f} | "
                    f"Flags: {risk_flags if risk_flags else 'none'}"
                )

                approved = True
                rejection_reason = None

            else:
                # ── CTO said SKIP ──
                signal.status = "REJECTED"
                signal.claude_decision = "SKIP"
                signal.claude_reasoning = decision.get("reasoning", "")
                approved = False
                rejection_reason = decision.get("reasoning", "CTO SKIP")

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
