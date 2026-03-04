"""
Telegram Bot Service — Per-Desk Channel Routing
Push notifications for trade events + interactive kill switch and desk controls.

Routing:
  - Trade entries/exits/skips → desk-specific channel
  - Drawdown, kill switch, daily reports → Portfolio channel
  - Status queries → Portfolio channel
"""
import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, Optional

import httpx

from app.config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    TELEGRAM_DESK_CHANNELS,
    TELEGRAM_PORTFOLIO_CHAT,
    DESKS,
)

logger = logging.getLogger("TradingSystem.Telegram")

TELEGRAM_API = "https://api.telegram.org/bot{token}"


class TelegramBot:
    """
    Sends institutional-style notifications to Telegram.
    Routes desk-specific signals to individual channels.
    Sends firm-wide alerts to the Portfolio channel.
    """

    def __init__(self):
        self.token = TELEGRAM_BOT_TOKEN
        self.portfolio_chat = TELEGRAM_PORTFOLIO_CHAT
        self.desk_channels = TELEGRAM_DESK_CHANNELS
        self.fallback_chat = TELEGRAM_CHAT_ID
        self.client = httpx.AsyncClient(timeout=10.0)
        self.enabled = bool(self.token)

        if not self.enabled:
            logger.warning(
                "Telegram not configured — notifications disabled. "
                "Set TELEGRAM_BOT_TOKEN."
            )
        else:
            logger.info(
                f"Telegram enabled | "
                f"{len(self.desk_channels)} desk channels + Portfolio"
            )

    def _get_desk_chat(self, desk_id: str) -> str:
        """Get the chat ID for a specific desk, with fallback."""
        chat_id = self.desk_channels.get(desk_id)
        if chat_id:
            return chat_id
        return self.portfolio_chat or self.fallback_chat

    async def send_message(
        self, text: str, chat_id: str = None, parse_mode: str = "HTML"
    ) -> bool:
        """Send a message to a specific chat (or portfolio by default)."""
        if not self.enabled:
            logger.debug(f"[TG-DISABLED] {text[:100]}")
            return False

        target = chat_id or self.portfolio_chat or self.fallback_chat
        if not target:
            logger.warning("No Telegram chat ID configured")
            return False

        try:
            resp = await self.client.post(
                f"{TELEGRAM_API.format(token=self.token)}/sendMessage",
                json={
                    "chat_id": target,
                    "text": text,
                    "parse_mode": parse_mode,
                    "disable_web_page_preview": True,
                },
            )
            if resp.status_code == 200:
                return True
            else:
                logger.error(
                    f"Telegram API error: {resp.status_code} {resp.text[:200]}"
                )
                return False
        except Exception as e:
            logger.error(f"Telegram send failed: {e}")
            return False

    async def _send_to_desk(self, desk_id: str, text: str) -> bool:
        """Send to a desk-specific channel."""
        chat_id = self._get_desk_chat(desk_id)
        return await self.send_message(text, chat_id=chat_id)

    async def _send_to_portfolio(self, text: str) -> bool:
        """Send to the portfolio channel."""
        return await self.send_message(text, chat_id=self.portfolio_chat)

    async def _send_to_all(self, text: str) -> None:
        """Send to all desk channels AND portfolio (for critical alerts)."""
        for desk_id, chat_id in self.desk_channels.items():
            await self.send_message(text, chat_id=chat_id)
        await self._send_to_portfolio(text)

    # ─────────────────────────────────────────
    # TRADE NOTIFICATIONS → Desk Channel
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

        desk_name = DESKS.get(desk, {}).get("name", desk)

        text = (
            f"🟢 <b>TRADE ENTRY</b>\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"<b>Desk:</b> {desk_name}\n"
            f"<b>Symbol:</b> {symbol} {direction}\n"
            f"<b>Risk:</b> ${risk:.2f} ({risk_pct:.2f}%)\n"
            f"<b>SL:</b> {sl} | <b>TP1:</b> {tp1}\n"
            f"<b>Confidence:</b> {confidence}\n"
            f"<b>Reasoning:</b> {reasoning}"
        )
        await self._send_to_desk(desk, text)
        # Portfolio gets a one-line summary
        summary = (
            f"🟢 <b>{desk_name}</b> | {symbol} {direction} | "
            f"Risk: ${risk:.2f} ({risk_pct:.2f}%)"
        )
        await self._send_to_portfolio(summary)

    async def notify_trade_exit(
        self, symbol: str, desk_id: str, pnl: float, reason: str
    ):
        """Notify when a trade is closed."""
        emoji = "🟢" if pnl >= 0 else "🔴"
        desk_name = DESKS.get(desk_id, {}).get("name", desk_id)

        text = (
            f"{emoji} <b>TRADE EXIT</b>\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"<b>Desk:</b> {desk_name}\n"
            f"<b>Symbol:</b> {symbol}\n"
            f"<b>PnL:</b> ${pnl:+.2f}\n"
            f"<b>Reason:</b> {reason}"
        )
        await self._send_to_desk(desk_id, text)
        summary = (
            f"{emoji} <b>{desk_name}</b> | {symbol} closed | "
            f"PnL: ${pnl:+.2f} | {reason}"
        )
        await self._send_to_portfolio(summary)

    async def notify_signal_skip(
        self, symbol: str, desk_id: str, reason: str, score: int
    ):
        """Notify when a signal is skipped (desk channel only)."""
        desk_name = DESKS.get(desk_id, {}).get("name", desk_id)
        text = (
            f"⚪ <b>SIGNAL SKIPPED</b>\n"
            f"<b>Desk:</b> {desk_name} | <b>Symbol:</b> {symbol}\n"
            f"<b>Score:</b> {score} | <b>Reason:</b> {reason[:150]}"
        )
        await self._send_to_desk(desk_id, text)

    # ─────────────────────────────────────────
    # RISK ALERTS → Portfolio + Affected Desk
    # ─────────────────────────────────────────

    async def alert_drawdown(self, desk_id: str, daily_loss: float, level: str):
        """Alert when desk drawdown hits warning levels."""
        desk_name = DESKS.get(desk_id, {}).get("name", desk_id)
        text = (
            f"⚠️ <b>DRAWDOWN ALERT</b>\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"<b>Desk:</b> {desk_name}\n"
            f"<b>Daily Loss:</b> ${abs(daily_loss):.2f}\n"
            f"<b>Level:</b> {level}\n"
            f"Action required if loss continues."
        )
        await self._send_to_desk(desk_id, text)
        await self._send_to_portfolio(text)

    async def alert_consecutive_losses(
        self, desk_id: str, count: int, action: str
    ):
        """Alert on consecutive loss milestones."""
        desk_name = DESKS.get(desk_id, {}).get("name", desk_id)
        text = (
            f"🔴 <b>CONSECUTIVE LOSSES</b>\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"<b>Desk:</b> {desk_name}\n"
            f"<b>Streak:</b> {count} losses\n"
            f"<b>Action:</b> {action}"
        )
        await self._send_to_desk(desk_id, text)
        await self._send_to_portfolio(text)

    async def alert_firm_drawdown(self, total_loss: float, level: str):
        """Alert on firm-wide drawdown → ALL channels."""
        text = (
            f"🚨 <b>FIRM-WIDE DRAWDOWN</b>\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"<b>Total Daily Loss:</b> ${abs(total_loss):.2f}\n"
            f"<b>Level:</b> {level}\n"
            f"All desks reduced to 50% size."
        )
        await self._send_to_all(text)

    async def alert_kill_switch(self, scope: str, triggered_by: str):
        """Alert when kill switch is triggered → ALL channels."""
        text = (
            f"🛑 <b>KILL SWITCH ACTIVATED</b>\n"
            f"━━━━━━━━━━━━━━━━━\n"
            f"<b>Scope:</b> {scope}\n"
            f"<b>Triggered by:</b> {triggered_by}\n"
            f"All positions being closed. Trading halted."
        )
        if scope == "ALL":
            await self._send_to_all(text)
        else:
            await self._send_to_desk(scope, text)
            await self._send_to_portfolio(text)

    # ─────────────────────────────────────────
    # REPORTS → Portfolio + Desk Summaries
    # ─────────────────────────────────────────

    async def send_daily_report(self, report: Dict):
        """Send end-of-day summary to portfolio + desk channels."""
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
            emoji = "🟢" if desk_data.get("pnl", 0) >= 0 else "🔴"
            text += (
                f"{emoji} <b>{name}:</b> "
                f"{desk_data.get('trades', 0)} trades | "
                f"${desk_data.get('pnl', 0):+.2f}\n"
            )

        await self._send_to_portfolio(text)

        # Desk-specific daily summaries
        for desk_id, desk_data in report.get("desks", {}).items():
            name = DESKS.get(desk_id, {}).get("name", desk_id)
            pnl = desk_data.get("pnl", 0)
            emoji = "🟢" if pnl >= 0 else "🔴"
            desk_text = (
                f"📊 <b>DAILY SUMMARY</b>\n"
                f"━━━━━━━━━━━━━━━━━\n"
                f"<b>Desk:</b> {name}\n"
                f"<b>Trades:</b> {desk_data.get('trades', 0)}\n"
                f"<b>Win Rate:</b> {desk_data.get('win_rate', 0):.1%}\n"
                f"{emoji} <b>PnL:</b> ${pnl:+.2f}"
            )
            await self._send_to_desk(desk_id, desk_text)

    async def send_weekly_memo(self, memo: str):
        """Send weekly strategy review memo to portfolio."""
        header = "📋 <b>WEEKLY STRATEGY MEMO</b>\n━━━━━━━━━━━━━━━━━\n\n"

        if len(header + memo) <= 4000:
            await self._send_to_portfolio(header + memo)
        else:
            await self._send_to_portfolio(
                header + memo[:3900] + "\n\n<i>(continued...)</i>"
            )
            remaining = memo[3900:]
            while remaining:
                chunk = remaining[:4000]
                remaining = remaining[4000:]
                await self._send_to_portfolio(chunk)

    # ─────────────────────────────────────────
    # STATUS → Portfolio Channel
    # ─────────────────────────────────────────

    async def send_status(self, dashboard: Dict):
        """Send current firm status to portfolio channel."""
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

        await self._send_to_portfolio(text)

    async def close(self):
        """Close HTTP client."""
        await self.client.aclose()
