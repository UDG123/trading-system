"""
Telegram Command Route — Bot Commands from Chat
Handles incoming commands from any authorized Telegram channel or private chat.
Set your Telegram bot's webhook to: https://YOUR-URL/api/telegram
"""
import logging
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session

from app.database import get_db
from app.config import (
    TELEGRAM_CHAT_ID, TELEGRAM_DESK_CHANNELS,
    TELEGRAM_PORTFOLIO_CHAT, TELEGRAM_SYSTEM_CHAT, DESKS,
)
from app.services.telegram_bot import TelegramBot

logger = logging.getLogger("TradingSystem.TelegramRoute")
router = APIRouter()

# Shared service instances
_telegram: TelegramBot = None

# Authorized chat IDs (all desk channels + portfolio + private)
AUTHORIZED_CHATS = set()


def _init_authorized_chats():
    global AUTHORIZED_CHATS
    if TELEGRAM_CHAT_ID:
        AUTHORIZED_CHATS.add(str(TELEGRAM_CHAT_ID))
    if TELEGRAM_PORTFOLIO_CHAT:
        AUTHORIZED_CHATS.add(str(TELEGRAM_PORTFOLIO_CHAT))
    if TELEGRAM_SYSTEM_CHAT:
        AUTHORIZED_CHATS.add(str(TELEGRAM_SYSTEM_CHAT))
    for chat_id in TELEGRAM_DESK_CHANNELS.values():
        if chat_id:
            AUTHORIZED_CHATS.add(str(chat_id))


_init_authorized_chats()

# Short desk aliases for commands
DESK_ALIASES = {
    "scalper": "DESK1_SCALPER",
    "intraday": "DESK2_INTRADAY",
    "swing": "DESK3_SWING",
    "gold": "DESK4_GOLD",
    "alts": "DESK5_ALTS",
    "equities": "DESK6_EQUITIES",
    "1": "DESK1_SCALPER",
    "2": "DESK2_INTRADAY",
    "3": "DESK3_SWING",
    "4": "DESK4_GOLD",
    "5": "DESK5_ALTS",
    "6": "DESK6_EQUITIES",
}


def _resolve_desk(arg: str) -> str:
    """Resolve a desk alias to full desk ID."""
    upper = arg.upper()
    if upper in DESKS:
        return upper
    lower = arg.lower()
    return DESK_ALIASES.get(lower, upper)


def _get_telegram():
    global _telegram
    if _telegram is None:
        _telegram = TelegramBot()
    return _telegram


@router.post("/telegram")
async def telegram_webhook(request: Request, db: Session = Depends(get_db)):
    """Receives updates from Telegram Bot API webhook."""
    try:
        update = await request.json()
    except Exception:
        return {"ok": True}

    # Handle messages from groups/channels/private
    message = (
        update.get("message")
        or update.get("channel_post")
        or {}
    )
    chat_id = str(message.get("chat", {}).get("id", ""))
    text = message.get("text", "").strip()

    # Also handle from user in private chat
    from_user = message.get("from", {}).get("id", "")

    # Security check
    if chat_id not in AUTHORIZED_CHATS:
        logger.warning(f"Unauthorized Telegram chat: {chat_id}")
        return {"ok": True}

    if not text.startswith("/"):
        return {"ok": True}

    telegram = _get_telegram()

    # Parse command (strip @botname if present)
    parts = text.split()
    command = parts[0].lower().split("@")[0]
    args = parts[1:] if len(parts) > 1 else []

    try:
        if command == "/status":
            await _handle_status(telegram, db)

        elif command == "/desk" and args:
            desk_id = _resolve_desk(args[0])
            await _handle_desk(telegram, db, desk_id)

        elif command == "/kill":
            scope = _resolve_desk(args[0]) if args else "ALL"
            await _handle_kill(telegram, db, scope, "Telegram")

        elif command == "/pause" and args:
            desk_id = _resolve_desk(args[0])
            hours = int(args[1]) if len(args) > 1 else 2
            await _handle_pause(telegram, db, desk_id, hours)

        elif command == "/resume" and args:
            desk_id = _resolve_desk(args[0])
            await _handle_resume(telegram, db, desk_id)

        elif command == "/daily":
            await _handle_report(telegram, db, "daily")

        elif command == "/weekly":
            await _handle_report(telegram, db, "weekly")

        elif command == "/monthly":
            await _handle_report(telegram, db, "monthly")

        elif command == "/desks":
            await _handle_desks_list(telegram, db)

        elif command == "/providers":
            await _handle_providers(telegram)

        elif command == "/diag":
            await _handle_diagnostics(telegram)

        elif command == "/mlstats":
            await _handle_mlstats(telegram, db)

        elif command == "/health":
            await _handle_health(telegram)

        elif command == "/sim":
            await _handle_sim(telegram, db, args)

        elif command == "/shadow":
            await _handle_shadow(telegram, db)

        elif command == "/labels":
            await _handle_labels(telegram, db)

        elif command == "/train":
            await _handle_train(telegram, db)

        elif command == "/backtest":
            await _handle_backtest_cmd(telegram, db, args)

        elif command == "/ohlcv":
            await _handle_ohlcv(telegram, db)

        elif command == "/help":
            await _handle_help(telegram, chat_id)

        else:
            await telegram.send_message(
                "❓ Unknown command. Send /help for available commands.",
                chat_id=chat_id,
            )

    except Exception as e:
        logger.error(f"Telegram command error: {e}", exc_info=True)
        await telegram.send_message(
            f"⚠️ Error: {str(e)[:200]}", chat_id=chat_id
        )

    return {"ok": True}


# ─────────────────────────────────────────
# COMMAND HANDLERS
# ─────────────────────────────────────────

async def _handle_status(telegram: TelegramBot, db: Session):
    """Send firm-wide dashboard."""
    from app.models.desk_state import DeskState

    desk_lines = []
    total_pnl = 0
    total_trades = 0

    for desk_id, desk in DESKS.items():
        emoji = {
            "DESK1_SCALPER": "🟢", "DESK2_INTRADAY": "🟡",
            "DESK3_SWING": "🔵", "DESK4_GOLD": "🔴",
            "DESK5_ALTS": "⚫", "DESK6_EQUITIES": "⚪",
        }.get(desk_id, "📊")
        label = {
            "DESK1_SCALPER": "Scalper", "DESK2_INTRADAY": "Intraday",
            "DESK3_SWING": "Swing", "DESK4_GOLD": "Gold",
            "DESK5_ALTS": "Alts", "DESK6_EQUITIES": "Equities",
        }.get(desk_id, desk_id)

        state = db.query(DeskState).filter(DeskState.desk_id == desk_id).first()

        if state:
            status = "🟢" if state.is_active and not state.is_paused else "🔴"
            if state.is_paused:
                status = "⏸️"
            pnl = state.daily_pnl or 0
            trades = state.trades_today or 0
            total_pnl += pnl
            total_trades += trades
            desk_lines.append(
                f"{status} {emoji} {label:<10} "
                f"${pnl:+,.0f}  ·  {trades}t  ·  "
                f"L{state.consecutive_losses or 0}"
            )
        else:
            desk_lines.append(
                f"🟢 {emoji} {label:<10} $0  ·  0t  ·  L0"
            )

    text = (
        f"🏛 ONIQUANT STATUS\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"📅 {datetime.now(timezone.utc).strftime('%b %d, %Y · %H:%M UTC')}\n\n"
    )
    text += "\n".join(desk_lines)
    text += (
        f"\n\n━━━━━━━━━━━━━━━━━━━━━━━\n"
        f"💰 Firm PnL   ${total_pnl:+,.2f}\n"
        f"🔄 Trades     {total_trades}"
    )

    await telegram._send_to_system(text)


async def _handle_desk(telegram: TelegramBot, db: Session, desk_id: str):
    """Send single desk status."""
    from app.models.desk_state import DeskState

    if desk_id not in DESKS:
        await telegram._send_to_system(
            f"❓ Unknown desk: {desk_id}\n\nUse /desks to see all desks."
        )
        return

    desk = DESKS[desk_id]
    state = db.query(DeskState).filter(DeskState.desk_id == desk_id).first()

    emoji = {
        "DESK1_SCALPER": "🟢", "DESK2_INTRADAY": "🟡",
        "DESK3_SWING": "🔵", "DESK4_GOLD": "🔴",
        "DESK5_ALTS": "⚫", "DESK6_EQUITIES": "⚪",
    }.get(desk_id, "📊")
    label = {
        "DESK1_SCALPER": "SCALPER", "DESK2_INTRADAY": "INTRADAY",
        "DESK3_SWING": "SWING", "DESK4_GOLD": "GOLD",
        "DESK5_ALTS": "ALTS", "DESK6_EQUITIES": "EQUITIES",
    }.get(desk_id, desk_id)

    if state:
        status = "🟢 Active" if state.is_active and not state.is_paused else "🔴 Inactive"
        if state.is_paused:
            status = f"⏸️ Paused until {state.pause_until.strftime('%H:%M UTC') if state.pause_until else '?'}"

        text = (
            f"{emoji} {label} DESK\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🚦 Status     {status}\n"
            f"🔄 Trades     {state.trades_today}\n"
            f"💰 Daily PnL  ${state.daily_pnl:+,.2f}\n"
            f"📉 Daily Loss ${abs(state.daily_loss or 0):,.2f}\n"
            f"🔥 Loss Streak {state.consecutive_losses}\n"
            f"📊 Size Mod   {(state.size_modifier or 1) * 100:.0f}%\n"
            f"📈 Open Pos   {state.open_positions}\n\n"
            f"🎯 Symbols    {', '.join(desk.get('symbols', []))}"
        )
    else:
        text = (
            f"{emoji} {label} DESK\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"No activity yet today.\n\n"
            f"🎯 Symbols    {', '.join(desk.get('symbols', []))}"
        )

    await telegram._send_to_desk(desk_id, text)


async def _handle_kill(
    telegram: TelegramBot,
    db: Session, scope: str, triggered_by: str
):
    """Execute kill switch."""
    from app.models.desk_state import DeskState

    await telegram.alert_kill_switch(scope, triggered_by)

    if scope == "ALL":
        desks = db.query(DeskState).all()
        for state in desks:
            state.is_active = False
        db.commit()
    else:
        if scope not in DESKS:
            await telegram._send_to_system(f"❓ Unknown desk: {scope}")
            return
        state = db.query(DeskState).filter(DeskState.desk_id == scope).first()
        if state:
            state.is_active = False
            db.commit()


async def _handle_pause(
    telegram: TelegramBot, db: Session, desk_id: str, hours: int = 2
):
    """Pause a desk."""
    from app.models.desk_state import DeskState

    if desk_id not in DESKS:
        await telegram._send_to_system(f"❓ Unknown desk: {desk_id}")
        return

    state = db.query(DeskState).filter(DeskState.desk_id == desk_id).first()
    if not state:
        state = DeskState(desk_id=desk_id, is_active=True)
        db.add(state)

    state.is_paused = True
    state.pause_until = datetime.now(timezone.utc) + timedelta(hours=hours)
    db.commit()

    label = {
        "DESK1_SCALPER": "SCALPER", "DESK2_INTRADAY": "INTRADAY",
        "DESK3_SWING": "SWING", "DESK4_GOLD": "GOLD",
        "DESK5_ALTS": "ALTS", "DESK6_EQUITIES": "EQUITIES",
    }.get(desk_id, desk_id)

    await telegram._send_to_system(
        f"⏸️ {label} paused for {hours} hours\n"
        f"Resumes at {state.pause_until.strftime('%H:%M UTC')}"
    )
    await telegram._send_to_desk(desk_id,
        f"⏸️ {label} paused for {hours} hours"
    )


async def _handle_resume(telegram: TelegramBot, db: Session, desk_id: str):
    """Resume a paused desk."""
    from app.models.desk_state import DeskState

    if desk_id not in DESKS:
        await telegram._send_to_system(f"❓ Unknown desk: {desk_id}")
        return

    state = db.query(DeskState).filter(DeskState.desk_id == desk_id).first()
    if state:
        state.is_paused = False
        state.is_active = True
        state.pause_until = None
        db.commit()

    label = {
        "DESK1_SCALPER": "SCALPER", "DESK2_INTRADAY": "INTRADAY",
        "DESK3_SWING": "SWING", "DESK4_GOLD": "GOLD",
        "DESK5_ALTS": "ALTS", "DESK6_EQUITIES": "EQUITIES",
    }.get(desk_id, desk_id)

    await telegram._send_to_system(f"▶️ {label} resumed. Trading active.")
    await telegram._send_to_desk(desk_id, f"▶️ {label} resumed.")


async def _handle_report(telegram: TelegramBot, db: Session, period: str):
    """Trigger a report manually."""
    from app.services.trade_reporter import TradeReporter

    reporter = TradeReporter()
    if period == "daily":
        await reporter.send_daily_report(db)
    elif period == "weekly":
        await reporter.send_weekly_report(db)
    elif period == "monthly":
        await reporter.send_monthly_report(db)
    await reporter.close()


async def _handle_desks_list(telegram: TelegramBot, db: Session):
    """List all desks with shortcuts."""
    from app.models.desk_state import DeskState

    text = (
        f"📋 ALL DESKS\n"
        f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    )
    for desk_id, desk in DESKS.items():
        emoji = {
            "DESK1_SCALPER": "🟢", "DESK2_INTRADAY": "🟡",
            "DESK3_SWING": "🔵", "DESK4_GOLD": "🔴",
            "DESK5_ALTS": "⚫", "DESK6_EQUITIES": "⚪",
        }.get(desk_id, "📊")
        short = desk_id.split("_", 1)[1].lower() if "_" in desk_id else desk_id
        num = desk_id.replace("DESK", "").split("_")[0]
        syms = len(desk.get("symbols", []))
        text += f"{emoji} {short} · {syms} symbols · /{num}\n"

    text += (
        f"\n💡 Use short names in commands:\n"
        f"/desk scalper\n"
        f"/kill gold\n"
        f"/pause intraday 4"
    )
    await telegram._send_to_system(text)


async def _handle_mlstats(telegram: TelegramBot, db: Session):
    """Show ML training data statistics."""
    from app.services.ml_data_logger import MLDataLogger
    stats = MLDataLogger.get_stats(db)

    wr = stats.get("win_rate", 0)
    bar = "▓" * int(wr / 10) + "░" * (10 - int(wr / 10))

    text_msg = (
        "🧠 ML TRAINING DATA\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📊 Total Records    {stats.get('total_records', 0)}\n"
        f"✅ Completed        {stats.get('completed', 0)}\n"
        f"⏳ Pending          {stats.get('pending', 0)}\n\n"
        f"🏆 Win/Loss         {stats.get('wins', 0)}W — {stats.get('losses', 0)}L\n"
        f"📊 Win Rate         {wr:.1f}%\n"
        f"📈 Avg Win          +{stats.get('avg_win_pips', 0):.1f}p\n"
        f"📉 Avg Loss         {stats.get('avg_loss_pips', 0):.1f}p\n"
        f"💰 Avg PnL/Trade    {stats.get('avg_pnl_pips', 0):+.1f}p\n"
        f"🚫 Filter Blocked   {stats.get('filter_blocked', 0)}\n\n"
        f"🔋 {bar} {wr:.0f}%\n\n"
    )

    desk_stats = stats.get("per_desk", {})
    if desk_stats:
        text_msg += "Per Desk:\n"
        for desk_id, ds in desk_stats.items():
            label = desk_id.split("_", 1)[1] if "_" in desk_id else desk_id
            text_msg += f"  {label:<12} {ds['wins']}W/{ds['total']}T  {ds['win_rate']:.0f}%\n"

    text_msg += (
        f"\n💾 Export: /api/ml/export\n"
        f"📊 Stats: /api/ml/stats"
    )
    await telegram._send_to_system(text_msg)



async def _handle_diagnostics(telegram: TelegramBot):
    """Run full system diagnostic."""
    from app.services.diagnostics import DiagnosticsService
    diag = DiagnosticsService()
    report = await diag.run_full_diagnostic()
    await diag.stop()
    await telegram._send_to_system(report)


async def _handle_health(telegram: TelegramBot):
    """Quick health check — one line per component."""
    import os
    from app.database import SessionLocal
    from sqlalchemy import text

    checks = []

    # Database
    try:
        db = SessionLocal()
        db.execute(text("SELECT 1"))
        db.close()
        checks.append("🟢 Database")
    except:
        checks.append("🔴 Database")

    # API Keys
    keys_ok = all([
        os.getenv("WEBHOOK_SECRET"),
        os.getenv("TELEGRAM_BOT_TOKEN"),
        os.getenv("TWELVEDATA_API_KEY"),
    ])
    checks.append(f"{'🟢' if keys_ok else '🔴'} API Keys")

    # Diagnostics
    try:
        from app.main import _diag_task
        running = _diag_task and not _diag_task.done()
        checks.append(f"{'🟢' if running else '🔴'} Diagnostics")
    except:
        checks.append("🟡 Diagnostics (unknown)")

    text_msg = (
        "⚡ QUICK HEALTH\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        + "\n".join(checks)
        + f"\n\n🕐 {datetime.now(timezone.utc).strftime('%H:%M UTC')}"
    )
    await telegram._send_to_system(text_msg)


async def _handle_providers(telegram: TelegramBot):
    """Show price provider health stats."""
    import os

    text = (
        "📡 PRICE PROVIDERS\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    )

    # API key status
    td_key = "✅" if os.getenv("TWELVEDATA_API_KEY") else "❌"
    fh_key = "✅" if os.getenv("FINNHUB_API_KEY") else "❌"
    fmp_key = "✅" if os.getenv("FMP_API_KEY") else "❌"

    text += (
        f"API Keys:\n"
        f"TwelveData {td_key}\n"
        f"Finnhub {fh_key}\n"
        f"FMP {fmp_key}\n"
    )

    await telegram._send_to_system(text)


async def _handle_sim(telegram: TelegramBot, db: Session, args: list):
    """Show sim profile summaries."""
    from app.models.sim_models import SimProfile, SimPosition, SimEquitySnapshot
    from sqlalchemy import func

    profiles = db.query(SimProfile).all()
    if not profiles:
        await telegram._send_to_system("📊 No sim profiles configured yet.")
        return

    today = datetime.now(timezone.utc).date()
    lines = []
    for p in profiles:
        equity_adj = (
            db.query(func.coalesce(func.sum(SimPosition.unrealized_pnl), 0))
            .filter(SimPosition.profile_id == p.id, SimPosition.status.in_(["OPEN", "PARTIAL"]))
            .scalar() or 0
        )
        equity = p.current_balance + float(equity_adj)
        open_ct = (
            db.query(func.count(SimPosition.id))
            .filter(SimPosition.profile_id == p.id, SimPosition.status.in_(["OPEN", "PARTIAL"]))
            .scalar() or 0
        )
        daily_pnl = (
            db.query(func.coalesce(func.sum(SimPosition.net_pnl), 0))
            .filter(SimPosition.profile_id == p.id, SimPosition.status == "CLOSED",
                    func.date(SimPosition.exit_time) == today)
            .scalar() or 0
        )
        status = "🟢" if p.is_active else "🔴"
        pnl_icon = "✅" if float(daily_pnl) >= 0 else "❌"
        lines.append(
            f"{status} {p.name} ({p.leverage}x)\n"
            f"  💰 ${equity:,.0f} | {pnl_icon} ${float(daily_pnl):+,.0f} today\n"
            f"  📈 {open_ct} open | Risk {p.risk_pct}%"
        )

    text = (
        "🎮 SIM PROFILES\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        + "\n\n".join(lines)
        + f"\n\n📊 API: /api/sim/profiles"
    )
    await telegram._send_to_system(text)


async def _handle_shadow(telegram: TelegramBot, db: Session):
    """Show shadow signal stats."""
    from app.models.shadow_signal import ShadowSignal
    from sqlalchemy import func

    today_start = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)

    total = db.query(func.count(ShadowSignal.id)).scalar() or 0
    today_ct = db.query(func.count(ShadowSignal.id)).filter(ShadowSignal.created_at >= today_start).scalar() or 0
    labeled = db.query(func.count(ShadowSignal.id)).filter(ShadowSignal.tb_label.isnot(None)).scalar() or 0
    wins = db.query(func.count(ShadowSignal.id)).filter(ShadowSignal.tb_label == 1).scalar() or 0
    approved = db.query(func.count(ShadowSignal.id)).filter(ShadowSignal.live_pipeline_approved == True).scalar() or 0
    hurst_blocks = db.query(func.count(ShadowSignal.id)).filter(ShadowSignal.hurst_would_block == True).scalar() or 0

    wr = round(wins / labeled * 100, 1) if labeled > 0 else 0
    approve_pct = round(approved / total * 100, 1) if total > 0 else 0
    hurst_pct = round(hurst_blocks / total * 100, 1) if total > 0 else 0

    text = (
        "👻 SHADOW SIGNALS\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"📊 Total          {total}\n"
        f"📅 Today          {today_ct}\n"
        f"🏷 Labeled        {labeled}\n"
        f"🏆 Win Rate       {wr}%\n"
        f"✅ Live Approved  {approve_pct}%\n"
        f"🌊 Hurst Blocked  {hurst_pct}%\n\n"
        f"📊 API: /api/shadow/stats"
    )
    await telegram._send_to_system(text)


async def _handle_labels(telegram: TelegramBot, db: Session):
    """Trigger triple-barrier labeling."""
    from app.services.triple_barrier_labeler import TripleBarrierLabeler
    from app.database import SessionLocal

    await telegram._send_to_system("🏷 Running triple-barrier labeler...")

    try:
        labeler = TripleBarrierLabeler(SessionLocal)
        count = await labeler.label_batch(limit=500)
        await telegram._send_to_system(
            f"🏷 Labeling complete\n"
            f"Labeled: {count} signals"
        )
    except Exception as e:
        await telegram._send_to_system(f"⚠️ Labeling error: {str(e)[:200]}")


async def _handle_train(telegram: TelegramBot, db: Session):
    """Trigger ML model training."""
    from app.services.ml_trainer import MLTrainer
    from app.database import SessionLocal

    await telegram._send_to_system("🧠 Training ML models (CatBoost + XGBoost)...")

    try:
        trainer = MLTrainer(SessionLocal)
        result = await trainer.train()
        status = result.get("status", "unknown")

        if status == "trained":
            text = (
                "🧠 ML TRAINING COMPLETE\n"
                "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"🏆 Model: {result.get('model_type')}\n"
                f"📊 Samples: {result.get('sample_count')}\n"
                f"🔧 Features: {result.get('feature_count')}\n"
                f"📈 CatBoost AUC: {result.get('catboost_auc', 0):.4f}\n"
                f"📈 XGBoost AUC: {result.get('xgboost_auc', 0):.4f}\n"
            )
            top = result.get("top_features", [])[:5]
            if top:
                text += "\nTop Features:\n"
                for name, imp in top:
                    text += f"  • {name}: {imp:.4f}\n"
        elif status == "insufficient_data":
            text = (
                f"⚠️ Not enough labeled data.\n"
                f"Have: {result.get('count')} | Need: {result.get('required')}\n"
                f"Run /labels first to label shadow signals."
            )
        else:
            text = f"⚠️ Training status: {status}\n{result.get('error', '')}"

        await telegram._send_to_system(text)
    except Exception as e:
        await telegram._send_to_system(f"⚠️ Training error: {str(e)[:200]}")


async def _handle_backtest_cmd(telegram: TelegramBot, db: Session, args: list):
    """Show backtest info or recent results."""
    text = (
        "🔬 BACKTEST\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "Use the API to run backtests:\n\n"
        "POST /api/backtest\n"
        "{"
        '"start_date": "2025-01-01",'
        '"end_date": "2025-03-01",'
        '"profile": "SRV_100"'
        "}\n\n"
        "Then poll: GET /api/backtest/{id}"
    )
    await telegram._send_to_system(text)


async def _handle_ohlcv(telegram: TelegramBot, db: Session):
    """Show OHLCV data stats."""
    from sqlalchemy import text as sql_text

    try:
        result = db.execute(sql_text("""
            SELECT symbol, COUNT(*) as bars,
                   MIN(time) as earliest, MAX(time) as latest
            FROM ohlcv_1m
            GROUP BY symbol ORDER BY bars DESC LIMIT 10
        """))
        rows = result.fetchall()

        if not rows:
            await telegram._send_to_system(
                "📊 OHLCV DATA\n\nNo data ingested yet.\n"
                "Use POST /api/ohlcv/ingest to start."
            )
            return

        total = sum(r[1] for r in rows)
        lines = []
        for r in rows:
            lines.append(f"  {r[0]:<8} {r[1]:>8,} bars")

        text = (
            "📊 OHLCV DATA\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"Total bars: {total:,}\n"
            f"Symbols: {len(rows)}\n\n"
            + "\n".join(lines)
        )
        await telegram._send_to_system(text)
    except Exception as e:
        await telegram._send_to_system(
            "📊 OHLCV DATA\n\nNo data yet (table may not exist).\n"
            "Run migration first, then POST /api/ohlcv/ingest"
        )


async def _handle_help(telegram: TelegramBot, chat_id: str):
    """Show available commands."""
    text = (
        "🏛 ONIQUANT COMMANDS\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "📊 STATUS\n"
        "/status — Firm dashboard\n"
        "/desk scalper — Single desk\n"
        "/desks — List all desks\n"
        "/providers — Price feed health\n"
        "/health — Quick system check\n"
        "/diag — Full diagnostic report\n"
        "/mlstats — ML training data stats\n\n"
        "🛑 CONTROL\n"
        "/kill — Kill ALL desks\n"
        "/kill gold — Kill one desk\n"
        "/pause scalper — Pause 2h\n"
        "/pause scalper 4 — Pause 4h\n"
        "/resume scalper — Resume\n\n"
        "📊 REPORTS\n"
        "/daily — Daily report\n"
        "/weekly — Weekly report\n"
        "/monthly — Monthly report\n\n"
        "🎮 SIMULATION\n"
        "/sim — Sim profile status\n"
        "/shadow — Shadow signal stats\n"
        "/labels — Run triple-barrier labeler\n"
        "/train — Train ML models\n"
        "/backtest — Backtest info\n"
        "/ohlcv — OHLCV data stats\n\n"
        "💡 SHORTCUTS\n"
        "scalper intraday swing gold alts equities\n"
        "or just numbers: 1 2 3 4 5 6"
    )
    await telegram.send_message(text, chat_id=chat_id)
