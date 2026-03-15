"""
ML Data Export API
GET /api/ml/export?format=csv&days=30
GET /api/ml/stats
"""
import csv
import io
import os
import logging
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, Depends, Query, Header, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.ml_trade_log import MLTradeLog

logger = logging.getLogger("TradingSystem.MLExport")
router = APIRouter()

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

def _verify(key):
    if key != WEBHOOK_SECRET:
        raise HTTPException(status_code=401)

@router.get("/ml/export")
async def export_ml_data(
    format: str = Query("csv"),
    days: int = Query(30, ge=1, le=365),
    desk: str = Query(None),
    symbol: str = Query(None),
    db: Session = Depends(get_db),
    x_api_key: str = Header(None),
):
    _verify(x_api_key)
    since = datetime.now(timezone.utc) - timedelta(days=days)
    query = db.query(MLTradeLog).filter(MLTradeLog.created_at >= since)
    if desk:
        query = query.filter(MLTradeLog.desk_id == desk)
    if symbol:
        query = query.filter(MLTradeLog.symbol == symbol)
    logs = query.order_by(MLTradeLog.created_at.asc()).all()

    if format == "json":
        data = [{
            "id": l.id, "created_at": l.created_at.isoformat() if l.created_at else None,
            "symbol": l.symbol, "direction": l.direction, "timeframe": l.timeframe,
            "alert_type": l.alert_type, "desk_id": l.desk_id,
            "entry_price": l.entry_price, "sl_pips": l.sl_pips, "tp1_pips": l.tp1_pips,
            "rr_ratio": l.rr_ratio, "session": l.session, "day_of_week": l.day_of_week,
            "hour_utc": l.hour_utc, "atr_value": l.atr_value, "spread_at_entry": l.spread_at_entry,
            "volatility_regime": l.volatility_regime, "ml_score": l.ml_score,
            "consensus_score": l.consensus_score, "consensus_tier": l.consensus_tier,
            "claude_decision": l.claude_decision, "claude_confidence": l.claude_confidence,
            "risk_pct": l.risk_pct, "lot_size": l.lot_size,
            "consecutive_losses": l.consecutive_losses,
            "approved": l.approved, "filter_blocked": l.filter_blocked,
            "block_reason": l.block_reason, "is_oniai": l.is_oniai,
            "outcome": l.outcome, "pnl_pips": l.pnl_pips, "pnl_dollars": l.pnl_dollars,
            "exit_reason": l.exit_reason, "hold_time_minutes": l.hold_time_minutes,
            "max_favorable_pips": l.max_favorable_pips, "max_adverse_pips": l.max_adverse_pips,
            "srv100_pnl_pips": l.srv100_pnl_pips, "srv30_pnl_pips": l.srv30_pnl_pips,
            "mt5_pnl_pips": l.mt5_pnl_pips, "oniai_pnl_pips": l.oniai_pnl_pips,
            "correlation_group": l.correlation_group,
        } for l in logs]
        return JSONResponse(content={"count": len(data), "data": data})

    # CSV
    output = io.StringIO()
    w = csv.writer(output)
    headers = [
        "id","created_at","symbol","direction","timeframe","alert_type","desk_id",
        "entry_price","sl_pips","tp1_pips","rr_ratio","session","day_of_week","hour_utc",
        "atr_value","spread_at_entry","volatility_regime","ml_score","consensus_score",
        "consensus_tier","claude_decision","claude_confidence","risk_pct","lot_size",
        "open_positions_desk","daily_pnl_at_entry","consecutive_losses","size_modifier",
        "approved","filter_blocked","block_reason","is_oniai","outcome","pnl_pips",
        "pnl_dollars","exit_reason","hold_time_minutes","max_favorable_pips",
        "max_adverse_pips","srv100_pnl_pips","srv30_pnl_pips","mt5_pnl_pips",
        "oniai_pnl_pips","correlation_group",
    ]
    w.writerow(headers)
    for l in logs:
        w.writerow([
            l.id, l.created_at.isoformat() if l.created_at else "",
            l.symbol, l.direction, l.timeframe, l.alert_type, l.desk_id,
            l.entry_price, l.sl_pips, l.tp1_pips, l.rr_ratio,
            l.session, l.day_of_week, l.hour_utc, l.atr_value,
            l.spread_at_entry, l.volatility_regime, l.ml_score, l.consensus_score,
            l.consensus_tier, l.claude_decision, l.claude_confidence,
            l.risk_pct, l.lot_size, l.open_positions_desk,
            l.daily_pnl_at_entry, l.consecutive_losses, l.size_modifier,
            l.approved, l.filter_blocked, l.block_reason, l.is_oniai,
            l.outcome, l.pnl_pips, l.pnl_dollars, l.exit_reason,
            l.hold_time_minutes, l.max_favorable_pips, l.max_adverse_pips,
            l.srv100_pnl_pips, l.srv30_pnl_pips, l.mt5_pnl_pips, l.oniai_pnl_pips,
            l.correlation_group,
        ])
    output.seek(0)
    fname = f"oniquant_ml_{days}d_{datetime.now().strftime('%Y%m%d')}.csv"
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={fname}"},
    )

@router.post("/ml/enrich")
async def batch_enrich(
    limit: int = Query(500, ge=1, le=5000),
    db: Session = Depends(get_db),
    x_api_key: str = Header(None),
):
    """Retroactively enrich ML records with derived features."""
    _verify(x_api_key)
    from app.services.feature_engineer import FeatureEngineer
    count = FeatureEngineer().batch_enrich(db, limit=limit)
    return {"enriched": count}


@router.get("/ml/stats")
async def ml_stats(
    days: int = Query(30, ge=1, le=365),
    db: Session = Depends(get_db),
    x_api_key: str = Header(None),
):
    _verify(x_api_key)
    since = datetime.now(timezone.utc) - timedelta(days=days)
    total = db.query(func.count(MLTradeLog.id)).filter(MLTradeLog.created_at >= since).scalar()
    with_outcome = db.query(func.count(MLTradeLog.id)).filter(
        MLTradeLog.created_at >= since, MLTradeLog.outcome.isnot(None)).scalar()
    wins = db.query(func.count(MLTradeLog.id)).filter(
        MLTradeLog.created_at >= since, MLTradeLog.outcome == "WIN").scalar()
    losses = db.query(func.count(MLTradeLog.id)).filter(
        MLTradeLog.created_at >= since, MLTradeLog.outcome == "LOSS").scalar()
    return {
        "period_days": days, "total_signals": total,
        "with_outcome": with_outcome, "wins": wins, "losses": losses,
        "win_rate": round(wins / with_outcome * 100, 1) if with_outcome > 0 else 0,
    }
