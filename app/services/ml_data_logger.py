"""
ML Data Logger — Records + Enriches Every Signal

Pipeline calls log_signal() with all data at once.
Feature engineer adds 40+ derived features automatically.
Outcome logged when trade closes.
Export via /api/ml/export or /api/ml/stats.
"""
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

from sqlalchemy import func, desc
from sqlalchemy.orm import Session

from app.models.ml_trade_log import MLTradeLog
from app.config import DESKS, get_pip_info

logger = logging.getLogger("TradingSystem.MLDataLogger")


class MLDataLogger:

    def log_signal(
        self, db: Session, signal_id: int = None,
        signal_data: Dict = None, desk_id: str = None,
        enrichment: Dict = None, ml_result: Dict = None,
        consensus: Dict = None, decision: Dict = None,
        risk_approved: bool = None, risk_block_reason: str = None,
        trade_params: Dict = None, desk_state=None,
    ) -> int:
        """All-in-one: log signal + scores + filter + run feature enrichment."""
        now = datetime.now(timezone.utc)
        signal_data = signal_data or {}
        enrichment = enrichment or {}
        ml_result = ml_result or {}
        consensus = consensus or {}
        decision = decision or {}
        trade_params = trade_params or {}

        symbol = signal_data.get("symbol", "")
        pip_size, _ = get_pip_info(symbol)
        entry = float(signal_data.get("price", 0) or 0)
        sl = float(signal_data.get("sl1", 0) or 0)
        tp1 = float(signal_data.get("tp1", 0) or 0)
        tp2 = float(signal_data.get("tp2", 0) or 0)
        sl_pips = abs(entry - sl) / pip_size if entry and sl and pip_size else 0
        tp1_pips = abs(tp1 - entry) / pip_size if entry and tp1 and pip_size else 0
        rr = round(tp1_pips / sl_pips, 2) if sl_pips > 0 else 0

        atr = enrichment.get("atr")
        atr_avg = enrichment.get("atr_avg")
        vol_regime = None
        if atr and atr_avg and atr_avg > 0:
            ratio = atr / atr_avg
            vol_regime = (
                "LOW" if ratio < 0.6 else
                "NORMAL" if ratio < 1.2 else
                "HIGH" if ratio < 2.0 else "EXTREME"
            )

        is_oniai = (
            risk_approved is False
            and decision.get("decision") in ["EXECUTE", "REDUCE"]
        )

        record = MLTradeLog(
            signal_id=signal_id,
            symbol=symbol,
            direction=signal_data.get("direction", ""),
            timeframe=signal_data.get("timeframe", ""),
            alert_type=signal_data.get("alert_type", ""),
            desk_id=desk_id or signal_data.get("desk_id", ""),
            entry_price=entry,
            sl_price=sl, tp1_price=tp1, tp2_price=tp2,
            sl_pips=round(sl_pips, 1), tp1_pips=round(tp1_pips, 1), rr_ratio=rr,
            session=self._get_session(now.hour),
            day_of_week=now.weekday(), hour_utc=now.hour,
            atr_value=atr, rsi_value=enrichment.get("rsi"),
            volatility_regime=vol_regime,
            raw_enrichment=enrichment,
            ml_score=ml_result.get("ml_score"),
            ml_method=ml_result.get("ml_method"),
            raw_ml_result=ml_result,
            consensus_score=consensus.get("total_score"),
            consensus_tier=consensus.get("tier"),
            consensus_components=consensus.get("components"),
            raw_consensus=consensus,
            claude_decision=decision.get("decision"),
            claude_confidence=decision.get("confidence"),
            claude_reasoning=(decision.get("reasoning") or "")[:500],
            claude_size_multiplier=decision.get("size_multiplier"),
            raw_claude_response={
                "decision": decision.get("decision"),
                "confidence": decision.get("confidence"),
                "reasoning": decision.get("reasoning"),
                "size_multiplier": decision.get("size_multiplier"),
            },
            open_positions_desk=getattr(desk_state, "open_positions", 0) if desk_state else 0,
            daily_pnl_at_entry=getattr(desk_state, "daily_pnl", 0) if desk_state else 0,
            daily_loss_at_entry=getattr(desk_state, "daily_loss", 0) if desk_state else 0,
            consecutive_losses=getattr(desk_state, "consecutive_losses", 0) if desk_state else 0,
            size_modifier=getattr(desk_state, "size_modifier", 1.0) if desk_state else 1.0,
            approved=risk_approved if risk_approved is not None else False,
            filter_blocked=not risk_approved if risk_approved is not None else False,
            block_reason=risk_block_reason,
            is_oniai=is_oniai,
            lot_size=trade_params.get("lot_size"),
            risk_pct=trade_params.get("risk_pct"),
            risk_dollars=trade_params.get("risk_dollars"),
            raw_signal_data=signal_data,
        )

        db.add(record)
        db.flush()

        # Run feature enrichment (adds 40+ derived features)
        try:
            from app.services.feature_engineer import FeatureEngineer
            FeatureEngineer().enrich_record(db, record.id)
        except Exception as e:
            logger.debug(f"Feature enrichment failed for #{record.id}: {e}")

        return record.id

    def log_outcome(
        self, db: Session, signal_id: int = None, trade_id: int = None,
        pnl_pips: float = None, pnl_dollars: float = None,
        exit_price: float = None, exit_reason: str = None,
        hold_time_minutes: float = None,
        max_favorable_pips: float = None, max_adverse_pips: float = None,
        profile: str = None,
    ):
        """Log trade outcome when it closes."""
        record = None
        if signal_id:
            record = (
                db.query(MLTradeLog)
                .filter(MLTradeLog.signal_id == signal_id)
                .order_by(desc(MLTradeLog.id)).first()
            )
        if not record:
            return

        record.trade_id = trade_id
        record.exit_price = exit_price
        record.exit_reason = exit_reason
        record.pnl_pips = pnl_pips
        record.pnl_dollars = pnl_dollars
        record.hold_time_minutes = hold_time_minutes
        record.max_favorable_pips = max_favorable_pips
        record.max_adverse_pips = max_adverse_pips

        if pnl_pips is not None:
            record.outcome = "WIN" if pnl_pips > 1 else ("LOSS" if pnl_pips < -1 else "BE")

        if profile == "SRV_100":
            record.srv100_pnl_pips = pnl_pips
            record.srv100_exit_reason = exit_reason
        elif profile == "SRV_30":
            record.srv30_pnl_pips = pnl_pips
            record.srv30_exit_reason = exit_reason
        elif profile == "MT5_1M":
            record.mt5_pnl_pips = pnl_pips
            record.mt5_exit_reason = exit_reason
        if record.is_oniai:
            record.oniai_pnl_pips = pnl_pips
            record.oniai_exit_reason = exit_reason

    @staticmethod
    def get_training_data(db: Session, completed_only: bool = True, limit: int = 10000) -> List[Dict]:
        """Export training data as JSON-serializable list."""
        query = db.query(MLTradeLog)
        if completed_only:
            query = query.filter(MLTradeLog.outcome.isnot(None))
        records = query.order_by(desc(MLTradeLog.created_at)).limit(limit).all()

        data = []
        for r in records:
            row = {
                "id": r.id,
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "signal_id": r.signal_id, "symbol": r.symbol,
                "direction": r.direction, "timeframe": r.timeframe,
                "desk_id": r.desk_id, "alert_type": r.alert_type,
                "entry_price": r.entry_price,
                "sl_price": r.sl_price, "tp1_price": r.tp1_price,
                "sl_pips": r.sl_pips, "tp1_pips": r.tp1_pips, "rr_ratio": r.rr_ratio,
                "session": r.session, "day_of_week": r.day_of_week, "hour_utc": r.hour_utc,
                "atr_value": r.atr_value, "volatility_regime": r.volatility_regime,
                "rsi_value": r.rsi_value,
                "ml_score": r.ml_score, "consensus_score": r.consensus_score,
                "consensus_tier": r.consensus_tier,
                "claude_decision": r.claude_decision,
                "claude_confidence": r.claude_confidence,
                "approved": r.approved, "filter_blocked": r.filter_blocked,
                "block_reason": r.block_reason, "is_oniai": r.is_oniai,
                "lot_size": r.lot_size, "risk_pct": r.risk_pct,
                "consecutive_losses": r.consecutive_losses,
                "size_modifier": r.size_modifier,
                "outcome": r.outcome,
                "pnl_pips": r.pnl_pips, "pnl_dollars": r.pnl_dollars,
                "exit_reason": r.exit_reason,
                "hold_time_minutes": r.hold_time_minutes,
                "max_favorable_pips": r.max_favorable_pips,
                "max_adverse_pips": r.max_adverse_pips,
                "srv100_pnl_pips": r.srv100_pnl_pips,
                "srv30_pnl_pips": r.srv30_pnl_pips,
                "mt5_pnl_pips": r.mt5_pnl_pips,
                "oniai_pnl_pips": r.oniai_pnl_pips,
            }
            # Add enriched features if present
            enrich = r.raw_enrichment or {}
            if "ml_features" in enrich:
                row["features"] = enrich["ml_features"]
            data.append(row)

        return data

    @staticmethod
    def get_stats(db: Session) -> Dict:
        total = db.query(func.count(MLTradeLog.id)).scalar()
        completed = db.query(func.count(MLTradeLog.id)).filter(MLTradeLog.outcome.isnot(None)).scalar()
        wins = db.query(func.count(MLTradeLog.id)).filter(MLTradeLog.outcome == "WIN").scalar()
        losses = db.query(func.count(MLTradeLog.id)).filter(MLTradeLog.outcome == "LOSS").scalar()
        blocked = db.query(func.count(MLTradeLog.id)).filter(MLTradeLog.filter_blocked == True).scalar()
        avg_pnl = db.query(func.avg(MLTradeLog.pnl_pips)).filter(MLTradeLog.outcome.isnot(None)).scalar()
        avg_win = db.query(func.avg(MLTradeLog.pnl_pips)).filter(MLTradeLog.outcome == "WIN").scalar()
        avg_loss = db.query(func.avg(MLTradeLog.pnl_pips)).filter(MLTradeLog.outcome == "LOSS").scalar()

        desk_stats = {}
        for desk_id in DESKS:
            dt = db.query(func.count(MLTradeLog.id)).filter(
                MLTradeLog.desk_id == desk_id, MLTradeLog.outcome.isnot(None)).scalar()
            dw = db.query(func.count(MLTradeLog.id)).filter(
                MLTradeLog.desk_id == desk_id, MLTradeLog.outcome == "WIN").scalar()
            if dt > 0:
                desk_stats[desk_id] = {"total": dt, "wins": dw, "win_rate": round(dw / dt * 100, 1)}

        return {
            "total_records": total, "completed": completed, "pending": total - completed,
            "wins": wins, "losses": losses,
            "win_rate": round(wins / completed * 100, 1) if completed > 0 else 0,
            "avg_pnl_pips": round(avg_pnl, 1) if avg_pnl else 0,
            "avg_win_pips": round(avg_win, 1) if avg_win else 0,
            "avg_loss_pips": round(avg_loss, 1) if avg_loss else 0,
            "filter_blocked": blocked, "per_desk": desk_stats,
        }

    def _get_session(self, hour_utc: int) -> str:
        if 0 <= hour_utc < 7: return "ASIAN"
        elif 7 <= hour_utc < 12: return "LONDON"
        elif 12 <= hour_utc < 16: return "OVERLAP"
        elif 16 <= hour_utc < 21: return "NEW_YORK"
        else: return "LATE_NY"
