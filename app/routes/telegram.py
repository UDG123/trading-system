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
                f"{status} {emoji} <b>{label:<10}</b> "
                f"${pnl:+,.0f}  ·  {trades}t  ·  "
                f"L{state.consecutive_losses or 0}"
            )
        else:
            desk_lines.append(
                f"🟢 {emoji} <b>{label:<10}</b> $0  ·  0t  ·  L0"
            )

    text = (
        f"🏛 <b>ONIQUANT STATUS</b>\n"
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
            f"{emoji} <b>{label} DESK</b>\n"
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
            f"{emoji} <b>{label} DESK</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"<i>No activity yet today.</i>\n\n"
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
        f"⏸️ <b>{label}</b> paused for {hours} hours\n"
        f"Resumes at {state.pause_until.strftime('%H:%M UTC')}"
    )
    await telegram._send_to_desk(desk_id,
        f"⏸️ <b>{label}</b> paused for {hours} hours"
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

    await telegram._send_to_system(f"▶️ <b>{label}</b> resumed. Trading active.")
    await telegram._send_to_desk(desk_id, f"▶️ <b>{label}</b> resumed.")


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
        f"📋 <b>ALL DESKS</b>\n"
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
        text += f"{emoji} <b>{short}</b> · {syms} symbols · /{num}\n"

    text += (
        f"\n💡 Use short names in commands:\n"
        f"<code>/desk scalper</code>\n"
        f"<code>/kill gold</code>\n"
        f"<code>/pause intraday 4</code>"
    )
    await telegram._send_to_system(text)


async def _handle_mlstats(telegram: TelegramBot, db: Session):
    """Show ML training data statistics."""
    from app.services.ml_data_logger import MLDataLogger
    stats = MLDataLogger.get_stats(db)

    wr = stats.get("win_rate", 0)
    bar = "▓" * int(wr / 10) + "░" * (10 - int(wr / 10))

    text_msg = (
        "🧠 <b>ML TRAINING DATA</b>\n"
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
        text_msg += "<b>Per Desk:</b>\n"
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
        "⚡ <b>QUICK HEALTH</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        + "\n".join(checks)
        + f"\n\n🕐 {datetime.now(timezone.utc).strftime('%H:%M UTC')}"
    )
    await telegram._send_to_system(text_msg)


async def _handle_providers(telegram: TelegramBot):
    """Show price provider health stats."""
    import os

    text = (
        "📡 <b>PRICE PROVIDERS</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    )

    # API key status
    td_key = "✅" if os.getenv("TWELVEDATA_API_KEY") else "❌"
    fh_key = "✅" if os.getenv("FINNHUB_API_KEY") else "❌"
    fmp_key = "✅" if os.getenv("FMP_API_KEY") else "❌"

    text += (
        f"<b>API Keys:</b>\n"
        f"TwelveData {td_key}\n"
        f"Finnhub {fh_key}\n"
        f"FMP {fmp_key}\n"
    )

    await telegram._send_to_system(text)


async def _handle_help(telegram: TelegramBot, chat_id: str):
    """Show available commands."""
    text = (
        "🏛 <b>ONIQUANT COMMANDS</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        "📊 <b>STATUS</b>\n"
        "/status — Firm dashboard\n"
        "/desk scalper — Single desk\n"
        "/desks — List all desks\n"
        "/providers — Price feed health\n"
        "/health — Quick system check\n"
        "/diag — Full diagnostic report\n"
        "/mlstats — ML training data stats\n\n"
        "🛑 <b>CONTROL</b>\n"
        "/kill — Kill ALL desks\n"
        "/kill gold — Kill one desk\n"
        "/pause scalper — Pause 2h\n"
        "/pause scalper 4 — Pause 4h\n"
        "/resume scalper — Resume\n\n"
        "📊 <b>REPORTS</b>\n"
        "/daily — Daily report\n"
        "/weekly — Weekly report\n"
        "/monthly — Monthly report\n"
        "/mlstats — ML training data stats\n\n"
        "💡 <b>SHORTCUTS</b>\n"
        "<code>scalper intraday swing gold alts equities</code>\n"
        "or just numbers: <code>1 2 3 4 5 6</code>"
    )
    await telegram.send_message(text, chat_id=chat_id)
