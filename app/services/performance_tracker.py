"""
OniQuant v5.9 — Daily Performance Tracker
Queries ml_trade_logs for the last 24 hours and generates a health-check
PnL report proving whether Hurst/RVOL filters hit the 69.5% win-rate target.

Scheduled via APScheduler at 17:00 Toronto (EDT) time.
Delivered to the OniQuant Portfolio Telegram channel.
"""
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

from sqlalchemy import func, case
from sqlalchemy.orm import Session

from app.models.ml_trade_log import MLTradeLog

logger = logging.getLogger("TradingSystem.PerformanceTracker")

# Win-rate target for the Hurst/RVOL filter strategy
WIN_RATE_TARGET = 69.5
HURST_CHOP_THRESHOLD = 0.52


def query_last_24h(db: Session) -> Dict:
    """
    Query ml_trade_logs for the last 24 hours and compute:
      - Total trades
      - Aggregate win rate
      - Win rate per desk (1-6)
      - Hurst filter effectiveness
      - Net PnL and profit factor

    Returns a dict with all computed metrics.
    """
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)

    # ── Aggregate stats ──
    agg = db.query(
        func.count(MLTradeLog.id).label("total"),
        func.count(case((MLTradeLog.outcome == "WIN", 1))).label("wins"),
        func.count(case((MLTradeLog.outcome == "LOSS", 1))).label("losses"),
        func.count(case((MLTradeLog.outcome == "BE", 1))).label("breakevens"),
        func.coalesce(func.sum(MLTradeLog.pnl_pips), 0).label("total_pips"),
        func.coalesce(func.sum(MLTradeLog.pnl_dollars), 0).label("total_dollars"),
        func.coalesce(func.avg(MLTradeLog.hurst_exponent), 0).label("avg_hurst"),
        func.coalesce(func.avg(MLTradeLog.rvol_multiplier), 0).label("avg_rvol"),
    ).filter(
        MLTradeLog.created_at >= cutoff,
        MLTradeLog.outcome.isnot(None),
    ).one()

    total = agg.total or 0
    wins = agg.wins or 0
    losses = agg.losses or 0
    win_rate = (wins / total * 100) if total > 0 else 0.0

    # Profit factor
    winning_pnl = db.query(
        func.coalesce(func.sum(MLTradeLog.pnl_dollars), 0)
    ).filter(
        MLTradeLog.created_at >= cutoff,
        MLTradeLog.outcome == "WIN",
    ).scalar() or 0

    losing_pnl = abs(db.query(
        func.coalesce(func.sum(MLTradeLog.pnl_dollars), 0)
    ).filter(
        MLTradeLog.created_at >= cutoff,
        MLTradeLog.outcome == "LOSS",
    ).scalar() or 0)

    profit_factor = winning_pnl / max(losing_pnl, 0.01)

    # ── Per-desk breakdown ──
    desk_rows = db.query(
        MLTradeLog.desk_id,
        func.count(MLTradeLog.id).label("total"),
        func.count(case((MLTradeLog.outcome == "WIN", 1))).label("wins"),
        func.count(case((MLTradeLog.outcome == "LOSS", 1))).label("losses"),
        func.coalesce(func.sum(MLTradeLog.pnl_pips), 0).label("pnl_pips"),
        func.coalesce(func.sum(MLTradeLog.pnl_dollars), 0).label("pnl_dollars"),
        func.coalesce(func.avg(MLTradeLog.hurst_exponent), 0).label("avg_hurst"),
        func.coalesce(func.avg(MLTradeLog.rvol_multiplier), 0).label("avg_rvol"),
    ).filter(
        MLTradeLog.created_at >= cutoff,
        MLTradeLog.outcome.isnot(None),
    ).group_by(MLTradeLog.desk_id).all()

    desks = {}
    for r in desk_rows:
        desk_total = r.total or 0
        desk_wins = r.wins or 0
        desk_wr = (desk_wins / desk_total * 100) if desk_total > 0 else 0.0
        desks[r.desk_id] = {
            "total": desk_total,
            "wins": desk_wins,
            "losses": r.losses or 0,
            "win_rate": round(desk_wr, 1),
            "pnl_pips": round(float(r.pnl_pips), 1),
            "pnl_dollars": round(float(r.pnl_dollars), 2),
            "avg_hurst": round(float(r.avg_hurst), 3),
            "avg_rvol": round(float(r.avg_rvol), 2),
        }

    # ── Hurst filter effectiveness ──
    vetoed_count = db.query(func.count(MLTradeLog.id)).filter(
        MLTradeLog.created_at >= cutoff,
        MLTradeLog.block_reason.like("%Hurst%"),
    ).scalar() or 0

    return {
        "period": "24h",
        "cutoff": cutoff.isoformat(),
        "total_trades": total,
        "wins": wins,
        "losses": losses,
        "breakevens": agg.breakevens or 0,
        "win_rate": round(win_rate, 1),
        "target_win_rate": WIN_RATE_TARGET,
        "target_met": win_rate >= WIN_RATE_TARGET,
        "profit_factor": round(profit_factor, 2),
        "net_pnl_pips": round(float(agg.total_pips), 1),
        "net_pnl_dollars": round(float(agg.total_dollars), 2),
        "avg_hurst": round(float(agg.avg_hurst), 3),
        "avg_rvol": round(float(agg.avg_rvol), 2),
        "hurst_vetoed": vetoed_count,
        "desks": desks,
    }


def format_daily_report(metrics: Dict) -> str:
    """
    Format the daily performance metrics into an institutional-grade
    Telegram HTML message for the Portfolio channel.
    """
    wr = metrics["win_rate"]
    target = metrics["target_win_rate"]
    pnl = metrics["net_pnl_dollars"]

    # Target badge
    if wr >= target:
        target_badge = "🎯 TARGET HIT"
    elif wr >= 60:
        target_badge = "📈 ON TRACK"
    elif wr >= 50:
        target_badge = "⚠️ BELOW TARGET"
    else:
        target_badge = "🔴 CRITICAL"

    pnl_emoji = "✅" if pnl >= 0 else "❌"

    # Desk breakdown
    desk_emoji = {
        "DESK1_SCALPER": "🟢", "DESK2_INTRADAY": "🟡",
        "DESK3_SWING": "🔵", "DESK4_GOLD": "🔴",
        "DESK5_ALTS": "⚫", "DESK6_EQUITIES": "⚪",
    }

    desk_lines = []
    for desk_id in sorted(metrics["desks"].keys()):
        d = metrics["desks"][desk_id]
        emoji = desk_emoji.get(desk_id, "▪️")
        pnl_ind = "✅" if d["pnl_dollars"] >= 0 else "❌"
        desk_lines.append(
            f"  {emoji} <b>{desk_id}</b>\n"
            f"     {d['wins']}W / {d['losses']}L ({d['win_rate']:.0f}%) | "
            f"{pnl_ind} {d['pnl_pips']:+.1f} pips | "
            f"${d['pnl_dollars']:+,.0f} | "
            f"H̄={d['avg_hurst']:.2f}"
        )

    now = datetime.now(timezone.utc)
    msg = (
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>DAILY PnL REPORT</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━\n"
        f"\n"
        f"  📅 {now.strftime('%A, %B %d %Y')}\n"
        f"  🎯 Win Rate: <b>{wr:.1f}%</b> (target: {target}%) {target_badge}\n"
        f"  {pnl_emoji} Net P&L: <b>${pnl:+,.2f}</b> ({metrics['net_pnl_pips']:+.1f} pips)\n"
        f"  📈 Trades: {metrics['total_trades']} ({metrics['wins']}W / {metrics['losses']}L / {metrics['breakevens']}BE)\n"
        f"  📊 Profit Factor: <b>{metrics['profit_factor']:.2f}</b>\n"
        f"  🌊 Avg Hurst: {metrics['avg_hurst']:.3f} | Chop Vetoes: {metrics['hurst_vetoed']}\n"
        f"  📶 Avg RVOL: {metrics['avg_rvol']:.2f}x\n"
        f"\n"
        f"┌─ <b>DESK BREAKDOWN</b>\n"
    )
    for line in desk_lines:
        msg += f"│\n│{line}\n"
    if not desk_lines:
        msg += "│  No completed trades in last 24h\n"
    msg += (
        f"└─────────────────────\n"
        f"\n"
        f"  ⚙️ Hurst Chop Zone: H < {HURST_CHOP_THRESHOLD}\n"
        f"  🤖 Zero-Key Oracle v5.9\n"
        f"━━━━━━━━━━━━━━━━━━━━━"
    )

    return msg


async def run_daily_performance_report(db_session_factory):
    """
    Full pipeline: query → compute → format → broadcast.
    Called by APScheduler at 17:00 Toronto time.
    """
    from app.services.telegram_notifications import TelegramService

    db = db_session_factory()
    try:
        metrics = query_last_24h(db)
        report = format_daily_report(metrics)

        tg = TelegramService()
        await tg.send_to_portfolio(report)

        logger.info(
            f"Daily performance report sent | "
            f"WR={metrics['win_rate']:.1f}% | "
            f"PnL=${metrics['net_pnl_dollars']:+,.2f} | "
            f"Trades={metrics['total_trades']} | "
            f"Target={'MET' if metrics['target_met'] else 'MISSED'}"
        )
    except Exception as e:
        logger.error(f"Daily performance report failed: {e}", exc_info=True)
    finally:
        db.close()
