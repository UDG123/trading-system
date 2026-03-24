"""
Simulation Engine API Routes
Dashboard, control, and data export endpoints for the shadow sim engine.
"""
import csv
import io
import os
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, Query, Header, HTTPException, BackgroundTasks
from fastapi.responses import StreamingResponse, JSONResponse
from sqlalchemy import func, case, and_
from sqlalchemy.orm import Session

from app.database import get_db, SessionLocal
from app.models.shadow_signal import ShadowSignal
from app.models.sim_models import (
    SimProfile, SimOrder, SimPosition, SimEquitySnapshot, SpreadReference,
)
from app.config import DESKS

logger = logging.getLogger("TradingSystem.SimRoutes")
router = APIRouter()

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

# In-memory backtest results cache
_backtest_results = {}
_backtest_counter = 0


def _verify(key):
    if not WEBHOOK_SECRET or key != WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Invalid API key")


# ═══════════════════════════════════════════════════════════════
# 1. SIM PROFILES
# ═══════════════════════════════════════════════════════════════

@router.get("/sim/profiles")
async def list_sim_profiles(db: Session = Depends(get_db)):
    """List all sim profiles with current state."""
    profiles = db.query(SimProfile).all()
    result = []
    for p in profiles:
        # Get unrealized PnL
        unrealized = (
            db.query(func.coalesce(func.sum(SimPosition.unrealized_pnl), 0))
            .filter(
                SimPosition.profile_id == p.id,
                SimPosition.status.in_(["OPEN", "PARTIAL"]),
            )
            .scalar() or 0
        )
        equity = p.current_balance + float(unrealized)

        # Open positions count
        open_count = (
            db.query(func.count(SimPosition.id))
            .filter(
                SimPosition.profile_id == p.id,
                SimPosition.status.in_(["OPEN", "PARTIAL"]),
            )
            .scalar() or 0
        )

        # Today's PnL from closed positions
        today = datetime.now(timezone.utc).date()
        daily_pnl = (
            db.query(func.coalesce(func.sum(SimPosition.net_pnl), 0))
            .filter(
                SimPosition.profile_id == p.id,
                SimPosition.status == "CLOSED",
                func.date(SimPosition.exit_time) == today,
            )
            .scalar() or 0
        )

        # Latest drawdown snapshot
        snap = (
            db.query(SimEquitySnapshot)
            .filter(SimEquitySnapshot.profile_id == p.id)
            .order_by(SimEquitySnapshot.snapshot_time.desc())
            .first()
        )

        result.append({
            "name": p.name,
            "balance": round(p.current_balance, 2),
            "equity": round(equity, 2),
            "initial_balance": round(p.initial_balance, 2),
            "daily_pnl": round(float(daily_pnl), 2),
            "daily_drawdown_pct": round(snap.daily_drawdown_pct, 2) if snap and snap.daily_drawdown_pct else 0,
            "total_drawdown_pct": round(snap.total_drawdown_pct, 2) if snap and snap.total_drawdown_pct else 0,
            "open_positions": open_count,
            "is_active": p.is_active,
            "leverage": p.leverage,
            "risk_pct": p.risk_pct,
        })

    return {"profiles": result}


# ═══════════════════════════════════════════════════════════════
# 2. SIM POSITIONS
# ═══════════════════════════════════════════════════════════════

@router.get("/sim/positions")
async def list_sim_positions(
    profile: str = Query("SRV_100"),
    status: str = Query("OPEN"),
    db: Session = Depends(get_db),
):
    """List sim positions for a profile."""
    p = db.query(SimProfile).filter(SimProfile.name == profile).first()
    if not p:
        raise HTTPException(status_code=404, detail=f"Profile {profile} not found")

    query = db.query(SimPosition).filter(SimPosition.profile_id == p.id)
    if status:
        query = query.filter(SimPosition.status == status.upper())

    positions = query.order_by(SimPosition.created_at.desc()).limit(200).all()

    return {
        "profile": profile,
        "count": len(positions),
        "positions": [
            {
                "id": pos.id,
                "symbol": pos.symbol,
                "direction": pos.direction,
                "desk_id": pos.desk_id,
                "entry_price": pos.entry_price,
                "current_price": pos.current_price,
                "unrealized_pnl": pos.unrealized_pnl,
                "entry_time": pos.entry_time.isoformat() if pos.entry_time else None,
                "stop_loss": pos.stop_loss,
                "take_profit_1": pos.take_profit_1,
                "take_profit_2": pos.take_profit_2,
                "lot_size": pos.lot_size,
                "status": pos.status,
                "max_favorable_pips": pos.max_favorable_pips,
                "max_adverse_pips": pos.max_adverse_pips,
            }
            for pos in positions
        ],
    }


# ═══════════════════════════════════════════════════════════════
# 3. SIM TRADES (closed)
# ═══════════════════════════════════════════════════════════════

@router.get("/sim/trades")
async def list_sim_trades(
    profile: str = Query("SRV_100"),
    days: int = Query(7, ge=1, le=365),
    db: Session = Depends(get_db),
):
    """Closed trade history for a profile."""
    p = db.query(SimProfile).filter(SimProfile.name == profile).first()
    if not p:
        raise HTTPException(status_code=404, detail=f"Profile {profile} not found")

    since = datetime.now(timezone.utc) - timedelta(days=days)

    positions = (
        db.query(SimPosition)
        .filter(
            SimPosition.profile_id == p.id,
            SimPosition.status == "CLOSED",
            SimPosition.exit_time >= since,
        )
        .order_by(SimPosition.exit_time.desc())
        .limit(500)
        .all()
    )

    # Get corresponding orders for spread/slippage data
    trades = []
    for pos in positions:
        order = None
        if pos.order_id:
            order = db.query(SimOrder).filter(SimOrder.id == pos.order_id).first()

        trades.append({
            "id": pos.id,
            "symbol": pos.symbol,
            "direction": pos.direction,
            "desk_id": pos.desk_id,
            "entry_price": pos.entry_price,
            "exit_price": pos.exit_price,
            "pnl_pips": pos.realized_pnl_pips,
            "pnl_dollars": pos.realized_pnl_dollars,
            "net_pnl": pos.net_pnl,
            "exit_reason": pos.exit_reason,
            "hold_minutes": pos.hold_time_minutes,
            "mfe": pos.max_favorable_pips,
            "mae": pos.max_adverse_pips,
            "spread": order.spread_cost if order else None,
            "slippage": order.slippage_cost if order else None,
            "commission": order.commission if order else None,
            "entry_time": pos.entry_time.isoformat() if pos.entry_time else None,
            "exit_time": pos.exit_time.isoformat() if pos.exit_time else None,
        })

    return {"profile": profile, "days": days, "count": len(trades), "trades": trades}


# ═══════════════════════════════════════════════════════════════
# 4. EQUITY CURVE
# ═══════════════════════════════════════════════════════════════

@router.get("/sim/equity")
async def get_equity_curve(
    profile: str = Query("SRV_100"),
    days: int = Query(30, ge=1, le=365),
    db: Session = Depends(get_db),
):
    """Equity curve data for a profile."""
    p = db.query(SimProfile).filter(SimProfile.name == profile).first()
    if not p:
        raise HTTPException(status_code=404, detail=f"Profile {profile} not found")

    since = datetime.now(timezone.utc) - timedelta(days=days)

    snapshots = (
        db.query(SimEquitySnapshot)
        .filter(
            SimEquitySnapshot.profile_id == p.id,
            SimEquitySnapshot.snapshot_time >= since,
        )
        .order_by(SimEquitySnapshot.snapshot_time.asc())
        .all()
    )

    return {
        "profile": profile,
        "days": days,
        "count": len(snapshots),
        "data": [
            {
                "time": s.snapshot_time.isoformat() if s.snapshot_time else None,
                "date": s.snapshot_date.isoformat() if s.snapshot_date else None,
                "equity": s.equity,
                "balance": s.balance,
                "unrealized_pnl": s.unrealized_pnl,
                "daily_pnl": s.daily_pnl,
                "drawdown_pct": s.total_drawdown_pct,
                "daily_drawdown_pct": s.daily_drawdown_pct,
                "open_positions": s.open_positions,
            }
            for s in snapshots
        ],
    }


# ═══════════════════════════════════════════════════════════════
# 5. PERFORMANCE METRICS
# ═══════════════════════════════════════════════════════════════

@router.get("/sim/metrics")
async def get_sim_metrics(
    profile: str = Query("SRV_100"),
    days: int = Query(30, ge=1, le=365),
    db: Session = Depends(get_db),
):
    """Compute performance metrics for a profile."""
    p = db.query(SimProfile).filter(SimProfile.name == profile).first()
    if not p:
        raise HTTPException(status_code=404, detail=f"Profile {profile} not found")

    since = datetime.now(timezone.utc) - timedelta(days=days)

    closed = (
        db.query(SimPosition)
        .filter(
            SimPosition.profile_id == p.id,
            SimPosition.status == "CLOSED",
            SimPosition.exit_time >= since,
        )
        .all()
    )

    total = len(closed)
    if total == 0:
        return {"profile": profile, "days": days, "total_trades": 0, "message": "No closed trades"}

    wins = [t for t in closed if (t.realized_pnl_pips or 0) > 0]
    losses = [t for t in closed if (t.realized_pnl_pips or 0) <= 0]

    gross_profit = sum(t.realized_pnl_pips or 0 for t in wins)
    gross_loss = abs(sum(t.realized_pnl_pips or 0 for t in losses))
    profit_factor = round(gross_profit / gross_loss, 2) if gross_loss > 0 else float("inf")

    pnl_list = [t.net_pnl or 0 for t in closed]
    avg_hold = sum(t.hold_time_minutes or 0 for t in closed) / total
    avg_mfe = sum(t.max_favorable_pips or 0 for t in closed) / total
    avg_mae = sum(t.max_adverse_pips or 0 for t in closed) / total

    # Per-desk
    per_desk = {}
    for t in closed:
        d = t.desk_id or "UNKNOWN"
        if d not in per_desk:
            per_desk[d] = {"trades": 0, "wins": 0, "pnl": 0.0}
        per_desk[d]["trades"] += 1
        if (t.realized_pnl_pips or 0) > 0:
            per_desk[d]["wins"] += 1
        per_desk[d]["pnl"] += t.net_pnl or 0
    for d in per_desk:
        s = per_desk[d]
        s["win_rate"] = round(s["wins"] / s["trades"] * 100, 1) if s["trades"] > 0 else 0
        s["pnl"] = round(s["pnl"], 2)

    return {
        "profile": profile,
        "days": days,
        "total_trades": total,
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(len(wins) / total * 100, 1),
        "profit_factor": profit_factor,
        "total_pnl": round(sum(pnl_list), 2),
        "expectancy": round(sum(pnl_list) / total, 2),
        "avg_hold_minutes": round(avg_hold, 1),
        "avg_mfe_pips": round(avg_mfe, 2),
        "avg_mae_pips": round(avg_mae, 2),
        "per_desk": per_desk,
    }


# ═══════════════════════════════════════════════════════════════
# 6. SHADOW SIGNAL STATS
# ═══════════════════════════════════════════════════════════════

@router.get("/shadow/stats")
async def shadow_stats(db: Session = Depends(get_db)):
    """Shadow signal collection statistics."""
    today_start = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    total = db.query(func.count(ShadowSignal.id)).scalar() or 0
    today_count = (
        db.query(func.count(ShadowSignal.id))
        .filter(ShadowSignal.created_at >= today_start)
        .scalar() or 0
    )
    labeled = db.query(func.count(ShadowSignal.id)).filter(ShadowSignal.tb_label.isnot(None)).scalar() or 0
    unlabeled = total - labeled

    # Win/loss/timeout distribution
    wins = db.query(func.count(ShadowSignal.id)).filter(ShadowSignal.tb_label == 1).scalar() or 0
    losses = db.query(func.count(ShadowSignal.id)).filter(ShadowSignal.tb_label == -1).scalar() or 0
    timeouts = db.query(func.count(ShadowSignal.id)).filter(ShadowSignal.tb_label == 0).scalar() or 0
    win_rate = round(wins / labeled * 100, 1) if labeled > 0 else 0

    # Gate block rates
    hurst_blocks = db.query(func.count(ShadowSignal.id)).filter(ShadowSignal.hurst_would_block == True).scalar() or 0
    align_blocks = db.query(func.count(ShadowSignal.id)).filter(ShadowSignal.alignment_would_block == True).scalar() or 0
    cons_blocks = db.query(func.count(ShadowSignal.id)).filter(ShadowSignal.consensus_would_block == True).scalar() or 0
    cto_blocks = db.query(func.count(ShadowSignal.id)).filter(ShadowSignal.cto_would_skip == True).scalar() or 0
    approved = db.query(func.count(ShadowSignal.id)).filter(ShadowSignal.live_pipeline_approved == True).scalar() or 0

    gate_block_rates = {}
    if total > 0:
        gate_block_rates = {
            "hurst_would_block": round(hurst_blocks / total * 100, 1),
            "alignment_would_block": round(align_blocks / total * 100, 1),
            "consensus_would_block": round(cons_blocks / total * 100, 1),
            "cto_would_skip": round(cto_blocks / total * 100, 1),
            "live_approved_pct": round(approved / total * 100, 1),
        }

    # Per-desk breakdown
    desk_rows = (
        db.query(
            ShadowSignal.desk_id,
            func.count(ShadowSignal.id).label("count"),
        )
        .group_by(ShadowSignal.desk_id)
        .order_by(func.count(ShadowSignal.id).desc())
        .limit(10)
        .all()
    )
    per_desk = {r.desk_id: r.count for r in desk_rows if r.desk_id}

    # Top symbols
    sym_rows = (
        db.query(
            ShadowSignal.symbol,
            func.count(ShadowSignal.id).label("count"),
        )
        .group_by(ShadowSignal.symbol)
        .order_by(func.count(ShadowSignal.id).desc())
        .limit(5)
        .all()
    )
    top_symbols = {r.symbol: r.count for r in sym_rows if r.symbol}

    return {
        "total": total,
        "today": today_count,
        "labeled": labeled,
        "unlabeled": unlabeled,
        "wins": wins,
        "losses": losses,
        "timeouts": timeouts,
        "win_rate": win_rate,
        "gate_block_rates": gate_block_rates,
        "per_desk": per_desk,
        "top_symbols": top_symbols,
    }


# ═══════════════════════════════════════════════════════════════
# 7. SHADOW EXPORT
# ═══════════════════════════════════════════════════════════════

@router.get("/shadow/export")
async def export_shadow_signals(
    format: str = Query("csv"),
    days: int = Query(30, ge=1, le=365),
    labeled_only: bool = Query(False),
    db: Session = Depends(get_db),
    x_api_key: str = Header(None),
):
    """Export shadow signals for ML training."""
    _verify(x_api_key)

    since = datetime.now(timezone.utc) - timedelta(days=days)
    query = db.query(ShadowSignal).filter(ShadowSignal.created_at >= since)
    if labeled_only:
        query = query.filter(ShadowSignal.tb_label.isnot(None))

    signals = query.order_by(ShadowSignal.created_at.asc()).all()

    if format == "json":
        data = []
        for s in signals:
            row = {
                "id": s.id,
                "created_at": s.created_at.isoformat() if s.created_at else None,
                "symbol": s.symbol, "direction": s.direction,
                "alert_type": s.alert_type, "timeframe": s.timeframe,
                "desk_id": s.desk_id, "price": s.price,
                "sl1": s.sl1, "tp1": s.tp1, "tp2": s.tp2,
                "hurst_exponent": s.hurst_exponent,
                "indicator_alignment": s.indicator_alignment,
                "consensus_score": s.consensus_score,
                "ml_score": s.ml_score,
                "claude_decision": s.claude_decision,
                "claude_confidence": s.claude_confidence,
                "hurst_would_block": s.hurst_would_block,
                "alignment_would_block": s.alignment_would_block,
                "consensus_would_block": s.consensus_would_block,
                "cto_would_skip": s.cto_would_skip,
                "live_pipeline_approved": s.live_pipeline_approved,
                "rsi": s.rsi, "adx": s.adx, "atr": s.atr,
                "trend": s.trend, "volatility_regime": s.volatility_regime,
                "active_session": s.active_session,
                "tb_label": s.tb_label, "tb_return": s.tb_return,
                "tb_barrier_hit": s.tb_barrier_hit,
                "tb_hold_bars": s.tb_hold_bars,
                "tb_max_favorable": s.tb_max_favorable,
                "tb_max_adverse": s.tb_max_adverse,
                "meta_label": s.meta_label,
            }
            # Flatten feature_vector
            fv = s.feature_vector or {}
            for k, v in fv.items():
                row[f"fv_{k}"] = v
            data.append(row)

        return JSONResponse(content={"count": len(data), "data": data})

    # CSV export
    output = io.StringIO()
    w = csv.writer(output)

    headers = [
        "id", "created_at", "symbol", "direction", "alert_type", "timeframe",
        "desk_id", "price", "sl1", "tp1", "tp2",
        "hurst_exponent", "indicator_alignment", "consensus_score",
        "ml_score", "claude_decision", "claude_confidence",
        "hurst_would_block", "alignment_would_block",
        "consensus_would_block", "cto_would_skip", "live_pipeline_approved",
        "rsi", "adx", "atr", "atr_pct", "trend", "volatility_regime",
        "active_session", "kill_zone_type", "vix_level", "dxy_change_pct",
        "rvol_multiplier", "vwap_z_score", "volume",
        "tb_label", "tb_return", "tb_barrier_hit", "tb_hold_bars",
        "tb_hold_minutes", "tb_max_favorable", "tb_max_adverse", "meta_label",
    ]
    # Add feature_vector keys from first signal if available
    fv_keys = []
    if signals and signals[0].feature_vector:
        fv_keys = sorted(signals[0].feature_vector.keys())
        headers.extend([f"fv_{k}" for k in fv_keys])

    w.writerow(headers)
    for s in signals:
        row = [
            s.id, s.created_at.isoformat() if s.created_at else "",
            s.symbol, s.direction, s.alert_type, s.timeframe,
            s.desk_id, s.price, s.sl1, s.tp1, s.tp2,
            s.hurst_exponent, s.indicator_alignment, s.consensus_score,
            s.ml_score, s.claude_decision, s.claude_confidence,
            s.hurst_would_block, s.alignment_would_block,
            s.consensus_would_block, s.cto_would_skip, s.live_pipeline_approved,
            s.rsi, s.adx, s.atr, s.atr_pct, s.trend, s.volatility_regime,
            s.active_session, s.kill_zone_type, s.vix_level, s.dxy_change_pct,
            s.rvol_multiplier, s.vwap_z_score, s.volume,
            s.tb_label, s.tb_return, s.tb_barrier_hit, s.tb_hold_bars,
            s.tb_hold_minutes, s.tb_max_favorable, s.tb_max_adverse, s.meta_label,
        ]
        fv = s.feature_vector or {}
        for k in fv_keys:
            row.append(fv.get(k))
        w.writerow(row)

    output.seek(0)
    fname = f"shadow_signals_{days}d_{datetime.now().strftime('%Y%m%d')}.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={fname}"},
    )


# ═══════════════════════════════════════════════════════════════
# 8. BACKTEST
# ═══════════════════════════════════════════════════════════════

async def _run_backtest_task(backtest_id: int, params: dict):
    """Background backtest execution."""
    from app.services.backtester import SignalReplayBacktester

    try:
        _backtest_results[backtest_id]["status"] = "running"
        bt = SignalReplayBacktester(SessionLocal, params.get("profile", "SRV_100"))
        result = await bt.run(
            start_date=datetime.fromisoformat(params["start_date"]),
            end_date=datetime.fromisoformat(params["end_date"]),
            filters=params.get("filters"),
        )
        _backtest_results[backtest_id] = {**result, "backtest_id": backtest_id}
    except Exception as e:
        _backtest_results[backtest_id] = {
            "backtest_id": backtest_id,
            "status": "error",
            "error": str(e),
        }


@router.post("/backtest")
async def start_backtest(
    body: dict,
    background_tasks: BackgroundTasks,
    x_api_key: str = Header(None),
):
    """Start a backtest. Returns backtest_id for polling."""
    _verify(x_api_key)

    global _backtest_counter
    _backtest_counter += 1
    bt_id = _backtest_counter

    if "start_date" not in body or "end_date" not in body:
        raise HTTPException(status_code=400, detail="start_date and end_date required")

    _backtest_results[bt_id] = {"backtest_id": bt_id, "status": "queued"}
    background_tasks.add_task(_run_backtest_task, bt_id, body)

    return {"backtest_id": bt_id, "status": "running"}


@router.get("/backtest/{backtest_id}")
async def get_backtest_results(backtest_id: int):
    """Get backtest results by ID."""
    if backtest_id not in _backtest_results:
        raise HTTPException(status_code=404, detail="Backtest not found")
    return _backtest_results[backtest_id]


# ═══════════════════════════════════════════════════════════════
# 9. PROFILE RESET
# ═══════════════════════════════════════════════════════════════

@router.post("/sim/reset")
async def reset_sim_profile(
    profile: str = Query("SRV_100"),
    db: Session = Depends(get_db),
    x_api_key: str = Header(None),
):
    """Reset a sim profile to initial state."""
    _verify(x_api_key)

    p = db.query(SimProfile).filter(SimProfile.name == profile).first()
    if not p:
        raise HTTPException(status_code=404, detail=f"Profile {profile} not found")

    # Close all open positions
    open_positions = (
        db.query(SimPosition)
        .filter(
            SimPosition.profile_id == p.id,
            SimPosition.status.in_(["OPEN", "PARTIAL"]),
        )
        .all()
    )
    for pos in open_positions:
        pos.status = "CLOSED"
        pos.exit_reason = "RESET"
        pos.exit_time = datetime.now(timezone.utc)

    # Reset balance
    p.current_balance = p.initial_balance
    p.is_active = True

    # Clear equity history
    db.query(SimEquitySnapshot).filter(SimEquitySnapshot.profile_id == p.id).delete()

    db.commit()

    return {
        "status": "reset",
        "profile": profile,
        "balance": p.current_balance,
        "positions_closed": len(open_positions),
    }


# ═══════════════════════════════════════════════════════════════
# 10. OHLCV INGEST
# ═══════════════════════════════════════════════════════════════

@router.post("/ohlcv/ingest")
async def trigger_ohlcv_ingest(
    body: dict,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    x_api_key: str = Header(None),
):
    """Trigger OHLCV data ingestion for specified symbols."""
    _verify(x_api_key)

    symbols = body.get("symbols", [])
    days_back = body.get("days_back", 30)

    async def _ingest():
        import asyncio
        from app.services.ohlcv_ingester import OHLCVIngester
        ingester = OHLCVIngester()
        _db = SessionLocal()
        total_bars = 0
        try:
            if symbols:
                for i, sym in enumerate(symbols):
                    count = await ingester.ingest_symbol(_db, sym, days_back)
                    _db.commit()
                    total_bars += count
                    logger.info(f"OHLCV ingest | {sym}: {count} bars ({i+1}/{len(symbols)})")
                    if i < len(symbols) - 1:
                        await asyncio.sleep(1.0)  # Rate limit: stay under 55/min
            else:
                await ingester.ingest_all_symbols(_db, days_back)
            logger.info(f"OHLCV ingest complete | {len(symbols)} symbols | {total_bars} total bars")
        except Exception as e:
            logger.error(f"OHLCV ingest failed: {e}", exc_info=True)
        finally:
            _db.close()
            await ingester.close()

    background_tasks.add_task(_ingest)

    return {
        "status": "ingesting",
        "symbols": symbols if symbols else "all",
        "days_back": days_back,
    }


# ═══════════════════════════════════════════════════════════════
# 11. OHLCV STATS
# ═══════════════════════════════════════════════════════════════

@router.get("/ohlcv/stats")
async def ohlcv_stats(db: Session = Depends(get_db)):
    """OHLCV data collection statistics."""
    try:
        from sqlalchemy import text
        result = db.execute(text("""
            SELECT
                symbol,
                COUNT(*) as bar_count,
                MIN(time) as earliest,
                MAX(time) as latest
            FROM ohlcv_1m
            GROUP BY symbol
            ORDER BY bar_count DESC
        """))
        rows = result.fetchall()

        total_bars = sum(r[1] for r in rows)
        symbols = {
            r[0]: {
                "bars": r[1],
                "earliest": r[2].isoformat() if r[2] else None,
                "latest": r[3].isoformat() if r[3] else None,
            }
            for r in rows
        }

        return {
            "total_bars": total_bars,
            "symbols_covered": len(symbols),
            "per_symbol": symbols,
        }
    except Exception as e:
        return {"total_bars": 0, "symbols_covered": 0, "error": str(e)}
