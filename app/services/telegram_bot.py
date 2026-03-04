"""
Telegram Bot Service
Push notifications for trade events + interactive kill switch and desk controls.
Uses push-only notifications (no polling) via direct API calls.

Commands (sent via Telegram to the bot):
  /status    — Firm-wide dashboard
  /desk      — Individual desk status
  /kill      — Emergency kill switch (all desks)
  /kill DESK1_SCALPER — Kill single desk
  /pause DESK1_SCALPER — Pause desk 2 hours
  /resume DESK1_SCALPER — Resume paused desk
  /help      — Show commands
"""
import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, Optional

import httpx

from app.config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, DESKS

logger = logging.getLogger("TradingSystem.Telegram")

TELEGRAM_API = "https://api.telegram.org/bot{token}"


class TelegramBot:
    """
    Sends institutional-style notifications to Telegram.
    All communication is push-only (no polling loop).
    """

    def __init__(self):
        self.token = TELEGRAM_BOT_TOKEN
        self.chat_id = TELEGRAM_CHAT_ID
        self.client = httpx.AsyncClient(timeout=10.0)
        self.enabled = bool(self.token and self.chat_id)

        if not self.enabled:
            logger.warning(
                "Telegram not configured — notifications disabled. "
                "Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID."
            )

    async def send_message(self, text: str, parse_mode: str = "HTML") -> bool:
        """Send a message to the configured Telegram chat."""
        if not self.enabled:
            logger.debug(f"[TG-DISABLED] {text[:100]}")
            return False

        try:
            resp = await self.client.post(
                f"{TELEGRAM_API.format(token=self.token)}/sendMessage",
                json={
                    "chat_id": self.chat_id,
                    "text": text,
                    "parse_mode": parse_mode,
                    "disable_web_page_preview": True,
                },
            )
            if resp.status_code == 200:
                return True
            else:
                logger.error(f"Telegram API error: {resp.status_code} {resp.text[:200]}")
                return False
        except Exception as e:
            logger.error(f"Telegram send failed: {e}")
            return False

    # ─────────────────────────────────────────
    # TRADE NOTIFICATIONS
    # ─────────────────────────────────────────

    async def notify_trade_entry(self, trade_params: Dict, decision: Dict):
        """Notify when a trade is approved and sent to MT5."""
        symbol = trade_params.get("symbol", "?")
        direction = trade_params.get("direction", "?")
        desk = trade_params.get("desk_id", "?")
        risk = trade_params.get("risk_dollars", 0)
        risk_pct = trade_params.get("risk_pct", 0)
        sl = trade_params.get("stop_loss", "?")
        tp1 = trade_params.get("take_profit_1", "?")
        confidence = decision.get("confidence", "?")
        reasoning = decision.get("reasoning", "")[:200]

        text = (
            f"🟢 <b>TRADE ENTRY</b>\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"<b>Desk:</b> {desk}\n"
            f"<b>Symbol:</b> {symbol} {direction}\n"
            f"<b>Risk:</b> ${risk:.2f} ({risk_pct:.2f}%)\n"
            f"<b>SL:</b> {sl} | <b>TP1:</b> {tp1}\n"
            f"<b>Confidence:</b> {confidence}\n"
            f"<b>Reasoning:</b> {reasoning}"
        )
        await self.send_message(text)

    async def notify_trade_exit(
        self, symbol: str, desk_id: str, pnl: float, reason: str
    ):
        """Notify when a trade is closed."""
        emoji = "🟢" if pnl >= 0 else "🔴"
        text = (
            f"{emoji} <b>TRADE EXIT</b>\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"<b>Desk:</b> {desk_id}\n"
            f"<b>Symbol:</b> {symbol}\n"
            f"<b>PnL:</b> ${pnl:+.2f}\n"
            f"<b>Reason:</b> {reason}"
        )
        await self.send_message(text)

    async def notify_signal_skip(
        self, symbol: str, desk_id: str, reason: str, score: int
    ):
        """Notify when a signal is skipped (optional — can be noisy)."""
        text = (
            f"⚪ <b>SIGNAL SKIPPED</b>\n"
            f"<b>Desk:</b> {desk_id} | <b>Symbol:</b> {symbol}\n"
            f"<b>Score:</b> {score} | <b>Reason:</b> {reason[:150]}"
        )
        await self.send_message(text)

    # ─────────────────────────────────────────
    # RISK ALERTS
    # ─────────────────────────────────────────

    async def alert_drawdown(self, desk_id: str, daily_loss: float, level: str):
        """Alert when desk drawdown hits warning levels."""
        text = (
            f"⚠️ <b>DRAWDOWN ALERT</b>\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"<b>Desk:</b> {desk_id}\n"
            f"<b>Daily Loss:</b> ${abs(daily_loss):.2f}\n"
            f"<b>Level:</b> {level}\n"
            f"Action required if loss continues."
        )
        await self.send_message(text)

    async def alert_consecutive_losses(
        self, desk_id: str, count: int, action: str
    ):
        """Alert on consecutive loss milestones."""
        text = (
            f"🔴 <b>CONSECUTIVE LOSSES</b>\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"<b>Desk:</b> {desk_id}\n"
            f"<b>Streak:</b> {count} losses\n"
            f"<b>Action:</b> {action}"
        )
        await self.send_message(text)

    async def alert_firm_drawdown(self, total_loss: float, level: str):
        """Alert on firm-wide drawdown."""
        text = (
            f"🚨 <b>FIRM-WIDE DRAWDOWN</b>\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"<b>Total Daily Loss:</b> ${abs(total_loss):.2f}\n"
            f"<b>Level:</b> {level}\n"
            f"All desks reduced to 50% size."
        )
        await self.send_message(text)

    async def alert_kill_switch(self, scope: str, triggered_by: str):
        """Alert when kill switch is triggered."""
        text = (
            f"🛑 <b>KILL SWITCH ACTIVATED</b>\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"<b>Scope:</b> {scope}\n"
            f"<b>Triggered by:</b> {triggered_by}\n"
            f"All positions being closed. Trading halted."
        )
        await self.send_message(text)

    # ─────────────────────────────────────────
    # REPORTS
    # ─────────────────────────────────────────

    async def send_daily_report(self, report: Dict):
        """Send end-of-day summary at midnight."""
        text = (
            f"📊 <b>DAILY CLOSE REPORT</b>\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"<b>Date:</b> {report.get('date', 'N/A')}\n"
            f"<b>Total Trades:</b> {report.get('total_trades', 0)}\n"
            f"<b>Win Rate:</b> {report.get('win_rate', 0):.1%}\n"
            f"<b>Firm PnL:</b> ${report.get('total_pnl', 0):+.2f}\n\n"
        )

        for desk_id, desk_data in report.get("desks", {}).items():
            name = DESKS.get(desk_id, {}).get("name", desk_id)
            text += (
                f"<b>{name}:</b> "
                f"{desk_data.get('trades', 0)} trades | "
                f"${desk_data.get('pnl', 0):+.2f}\n"
            )

        await self.send_message(text)

    async def send_weekly_memo(self, memo: str):
        """Send weekly strategy review memo."""
        # Split into chunks if too long (Telegram limit is 4096 chars)
        header = "📋 <b>WEEKLY STRATEGY MEMO</b>\n━━━━━━━━━━━━━━━━━\n\n"

        if len(header + memo) <= 4000:
            await self.send_message(header + memo)
        else:
            await self.send_message(header + memo[:3900] + "\n\n<i>(continued...)</i>")
            # Send remaining in chunks
            remaining = memo[3900:]
            while remaining:
                chunk = remaining[:4000]
                remaining = remaining[4000:]
                await self.send_message(chunk)

    # ─────────────────────────────────────────
    # STATUS
    # ─────────────────────────────────────────

    async def send_status(self, dashboard: Dict):
        """Send current firm status."""
        text = (
            f"📈 <b>FIRM STATUS</b>\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"<b>Status:</b> {dashboard.get('firm_status', '?')}\n"
            f"<b>Signals Today:</b> {dashboard.get('total_signals_today', 0)}\n"
            f"<b>Trades Today:</b> {dashboard.get('total_trades_today', 0)}\n"
            f"<b>Daily PnL:</b> ${dashboard.get('total_daily_pnl', 0):+.2f}\n\n"
        )

        for desk in dashboard.get("desks", []):
            status = "🟢" if desk.get("is_active") else "🔴"
            text += (
                f"{status} <b>{desk.get('name', '?')}:</b> "
                f"{desk.get('trades_today', 0)} trades | "
                f"${desk.get('daily_pnl', 0):+.2f} | "
                f"Losses: {desk.get('consecutive_losses', 0)}\n"
            )

        await self.send_message(text)

    async def close(self):
        """Close HTTP client."""
        await self.client.aclose()
