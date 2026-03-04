"""
Telegram Command Route
Handles incoming commands from Telegram webhook.
Set your Telegram bot's webhook to: https://YOUR-URL/api/telegram
"""
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, Request
from sqlalchemy.orm import Session

from app.database import get_db
from app.config import TELEGRAM_CHAT_ID, DESKS
from app.services.telegram_bot import TelegramBot
from app.services.zmq_bridge import ZMQBridge
from app.models.desk_state import DeskState

logger = logging.getLogger("TradingSystem.TelegramRoute")
router = APIRouter()

# Shared service instances
_telegram: TelegramBot = None
_zmq: ZMQBridge = None


def _get_telegram():
    global _telegram
    if _telegram is None:
        _telegram = TelegramBot()
    return _telegram


def _get_zmq():
    global _zmq
    if _zmq is None:
        _zmq = ZMQBridge()
    return _zmq


@router.post("/telegram")
async def telegram_webhook(request: Request, db: Session = Depends(get_db)):
    """
    Receives updates from Telegram Bot API webhook.
    Parses commands and executes them.
    """
    try:
        update = await request.json()
    except Exception:
        return {"ok": True}

    message = update.get("message", {})
    chat_id = str(message.get("chat", {}).get("id", ""))
    text = message.get("text", "").strip()

    # Security: only respond to our authorized chat
    if chat_id != TELEGRAM_CHAT_ID:
        logger.warning(f"Unauthorized Telegram chat: {chat_id}")
        return {"ok": True}

    if not text.startswith("/"):
        return {"ok": True}

    telegram = _get_telegram()
    zmq_bridge = _get_zmq()

    # Parse command
    parts = text.split()
    command = parts[0].lower()
    args = parts[1:] if len(parts) > 1 else []

    try:
        if command == "/status":
            await _handle_status(telegram, db)

        elif command == "/desk" and args:
            await _handle_desk(telegram, db, args[0].upper())

        elif command == "/kill":
            scope = args[0].upper() if args else "ALL"
            await _handle_kill(telegram, zmq_bridge, db, scope, "Telegram")

        elif command == "/pause" and args:
            await _handle_pause(telegram, db, args[0].upper())

        elif command == "/resume" and args:
            await _handle_resume(telegram, db, args[0].upper())

        elif command == "/help":
            await _handle_help(telegram)

        else:
            await telegram.send_message(
                "❓ Unknown command. Send /help for available commands."
            )

    except Exception as e:
        logger.error(f"Telegram command error: {e}", exc_info=True)
        await telegram.send_message(f"⚠️ Error: {str(e)[:200]}")

    return {"ok": True}


async def _handle_status(telegram: TelegramBot, db: Session):
    """Send firm-wide dashboard."""
    from app.routes.dashboard import get_dashboard
    dashboard = await get_dashboard(db)
    await telegram.send_status(dashboard.model_dump())


async def _handle_desk(telegram: TelegramBot, db: Session, desk_id: str):
    """Send single desk status."""
    if desk_id not in DESKS:
        await telegram.send_message(
            f"❓ Unknown desk: {desk_id}\n\n"
            f"Available desks:\n" +
            "\n".join(f"• {did} — {d['name']}" for did, d in DESKS.items())
        )
        return

    state = db.query(DeskState).filter(DeskState.desk_id == desk_id).first()
    desk = DESKS[desk_id]

    if state:
        status = "🟢 Active" if state.is_active else "🔴 Inactive"
        if state.is_paused:
            status = "⏸️ Paused"

        text = (
            f"📋 <b>{desk['name']}</b> ({desk_id})\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"<b>Status:</b> {status}\n"
            f"<b>Style:</b> {desk.get('style', '?')}\n"
            f"<b>Trades Today:</b> {state.trades_today}\n"
            f"<b>Daily PnL:</b> ${state.daily_pnl:+.2f}\n"
            f"<b>Daily Loss:</b> ${abs(state.daily_loss):.2f}\n"
            f"<b>Consecutive Losses:</b> {state.consecutive_losses}\n"
            f"<b>Size Modifier:</b> {state.size_modifier*100:.0f}%\n"
            f"<b>Open Positions:</b> {state.open_positions}\n"
            f"<b>Symbols:</b> {', '.join(desk.get('symbols', []))}"
        )
    else:
        text = f"📋 <b>{desk['name']}</b> — No activity yet today."

    await telegram.send_message(text)


async def _handle_kill(
    telegram: TelegramBot, zmq_bridge: ZMQBridge,
    db: Session, scope: str, triggered_by: str
):
    """Execute kill switch."""
    # Notify first
    await telegram.alert_kill_switch(scope, triggered_by)

    # Send to VPS
    result = await zmq_bridge.send_kill_switch(scope)

    # Update desk states in DB
    if scope == "ALL":
        desks = db.query(DeskState).all()
        for state in desks:
            state.is_active = False
        db.commit()
        await telegram.send_message(
            f"🛑 Kill switch executed for ALL desks.\n"
            f"VPS response: {result.get('status', 'unknown')}"
        )
    else:
        state = db.query(DeskState).filter(DeskState.desk_id == scope).first()
        if state:
            state.is_active = False
            db.commit()
            await telegram.send_message(
                f"🛑 Kill switch executed for {scope}.\n"
                f"VPS response: {result.get('status', 'unknown')}"
            )
        else:
            await telegram.send_message(f"❓ Unknown desk: {scope}")


async def _handle_pause(telegram: TelegramBot, db: Session, desk_id: str):
    """Pause a desk for 2 hours."""
    from datetime import timedelta

    if desk_id not in DESKS:
        await telegram.send_message(f"❓ Unknown desk: {desk_id}")
        return

    state = db.query(DeskState).filter(DeskState.desk_id == desk_id).first()
    if not state:
        state = DeskState(desk_id=desk_id, is_active=True)
        db.add(state)

    state.is_paused = True
    state.pause_until = datetime.now(timezone.utc) + timedelta(hours=2)
    db.commit()

    await telegram.send_message(
        f"⏸️ <b>{desk_id}</b> paused for 2 hours.\n"
        f"Resumes at: {state.pause_until.strftime('%H:%M UTC')}"
    )


async def _handle_resume(telegram: TelegramBot, db: Session, desk_id: str):
    """Resume a paused desk."""
    if desk_id not in DESKS:
        await telegram.send_message(f"❓ Unknown desk: {desk_id}")
        return

    state = db.query(DeskState).filter(DeskState.desk_id == desk_id).first()
    if state:
        state.is_paused = False
        state.is_active = True
        state.pause_until = None
        db.commit()

    await telegram.send_message(f"▶️ <b>{desk_id}</b> resumed. Trading active.")


async def _handle_help(telegram: TelegramBot):
    """Show available commands."""
    text = (
        "🤖 <b>TRADING SYSTEM COMMANDS</b>\n"
        "━━━━━━━━━━━━━━━━━\n\n"
        "/status — Firm-wide dashboard\n"
        "/desk DESK_ID — Single desk status\n"
        "/kill — Kill switch (all desks)\n"
        "/kill DESK_ID — Kill single desk\n"
        "/pause DESK_ID — Pause desk 2 hours\n"
        "/resume DESK_ID — Resume paused desk\n"
        "/help — Show this message\n\n"
        "<b>Desk IDs:</b>\n"
        "DESK1_SCALPER\n"
        "DESK2_INTRADAY\n"
        "DESK3_SWING\n"
        "DESK4_GOLD\n"
        "DESK5_ALTS\n"
        "DESK6_EQUITIES"
    )
    await telegram.send_message(text)
