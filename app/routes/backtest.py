"""
Backtest API Route — Trigger and retrieve Zero-Key Simulation results.
"""
import logging
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.database import get_db

logger = logging.getLogger("TradingSystem.BacktestRoute")

router = APIRouter(prefix="/api/backtest", tags=["backtest"])


@router.get("/results/{run_id}")
def get_backtest_results(run_id: str, db: Session = Depends(get_db)):
    """Retrieve results for a specific backtest run."""
    from app.models.backtest_result import BacktestResult

    results = db.query(BacktestResult).filter(
        BacktestResult.run_id == run_id
    ).all()

    if not results:
        return {"error": f"No results found for run_id={run_id}"}

    trades = []
    for r in results:
        trades.append({
            "id": r.id,
            "symbol": r.symbol,
            "direction": r.direction,
            "signal_type": r.signal_type,
            "entry_price": r.entry_price,
            "exit_price": r.exit_price,
            "stop_loss": r.stop_loss,
            "take_profit": r.take_profit,
            "hurst_exponent": r.hurst_exponent,
            "hurst_vetoed": r.hurst_vetoed,
            "outcome": r.outcome,
            "pnl_pips": r.pnl_pips,
            "pnl_dollars": r.pnl_dollars,
            "risk_reward": r.risk_reward,
            "rsi_at_entry": r.rsi_at_entry,
            "signal_timestamp": r.signal_timestamp.isoformat() if r.signal_timestamp else None,
        })

    return {
        "run_id": run_id,
        "total_trades": len(trades),
        "trades": trades,
    }


@router.get("/runs")
def list_backtest_runs(db: Session = Depends(get_db)):
    """List all backtest runs with summary stats."""
    from app.models.backtest_result import BacktestResult

    from sqlalchemy import Integer, case
    runs = db.query(
        BacktestResult.run_id,
        func.min(BacktestResult.run_timestamp).label("started_at"),
        func.count(BacktestResult.id).label("total_signals"),
        func.sum(case((BacktestResult.hurst_vetoed == True, 1), else_=0)).label("vetoed"),
    ).group_by(BacktestResult.run_id).order_by(
        func.min(BacktestResult.run_timestamp).desc()
    ).all()

    return [{
        "run_id": r.run_id,
        "started_at": r.started_at.isoformat() if r.started_at else None,
        "total_signals": r.total_signals,
    } for r in runs]
