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
        """Notify when a trade is approved — FTMO dashboard card style."""
        symbol = trade_params.get("symbol", "?")
        direction = trade_params.get("direction", "?")
        desk = trade_params.get("desk_id", "?")
        price = trade_params.get("price", trade_params.get("entry_price", 0))
        risk = trade_params.get("risk_dollars", 0)
        risk_pct = trade_params.get("risk_pct", 0)
        sl = trade_params.get("stop_loss", 0)
        tp1 = trade_params.get("take_profit_1", 0)
        tp2 = trade_params.get("take_profit_2", 0)
        confidence = decision.get("confidence", 0)
        reasoning = decision.get("reasoning", "")[:200]

        desk_name = DESKS.get(desk, {}).get("name", desk)
        desk_emoji = {
            "DESK1_SCALPER": "🟢", "DESK2_INTRADAY": "🟡",
            "DESK3_SWING": "🔵", "DESK4_GOLD": "🔴",
            "DESK5_ALTS": "⚫", "DESK6_EQUITIES": "⚪",
        }.get(desk, "📊")
        desk_label = {
            "DESK1_SCALPER": "SCALPER", "DESK2_INTRADAY": "INTRADAY",
            "DESK3_SWING": "SWING", "DESK4_GOLD": "GOLD",
            "DESK5_ALTS": "ALTS", "DESK6_EQUITIES": "EQUITIES",
        }.get(desk, desk)

        arrow = "↑" if direction.upper() in ["LONG", "BUY"] else "↓"

        # Calculate pip distances and projections
        proj_section = ""
        rr_line = ""
        try:
            entry_f = float(price) if price else 0
            sl_f = float(sl) if sl else 0
            tp1_f = float(tp1) if tp1 else 0
            tp2_f = float(tp2) if tp2 else 0

            if entry_f > 0:
                sym = str(symbol).upper()
                is_jpy = "JPY" in sym
                is_gold = "XAU" in sym
                is_silver = "XAG" in sym
                is_index = any(x in sym for x in ["US30", "US100", "NAS", "SPX"])
                is_crypto = any(x in sym for x in ["BTC", "ETH"])

                if is_jpy:
                    pip_size, pip_val = 0.01, 6.67
                elif is_gold:
                    pip_size, pip_val = 0.1, 10.0
                elif is_silver:
                    pip_size, pip_val = 0.01, 50.0
                elif is_index:
                    pip_size, pip_val = 1.0, 1.0
                elif is_crypto:
                    pip_size, pip_val = 1.0, 1.0
                else:
                    pip_size, pip_val = 0.0001, 10.0

                sl_pips = abs(entry_f - sl_f) / pip_size if sl_f > 0 else 0
                tp1_pips = abs(tp1_f - entry_f) / pip_size if tp1_f > 0 else 0
                tp2_pips = abs(tp2_f - entry_f) / pip_size if tp2_f > 0 else 0
                rr = round(tp1_pips / sl_pips, 1) if sl_pips > 0 else 0

                rr_line = f"\n⚖️ R:R      1:{rr}"

                # Projections
                lots = [0.01, 0.1, 1.0]
                sl_l = [round(sl_pips * pip_val * l, 0) for l in lots]
                tp1_l = [round(tp1_pips * pip_val * l, 0) for l in lots]
                tp2_l = [round(tp2_pips * pip_val * l, 0) for l in lots]

                proj_section = "\n━━━ PROJECTED P&L ━━━━━━\n\n"

                if tp2_pips > 0:
                    proj_section += (
                        f"<code>"
                        f"💵 0.01    -${sl_l[0]:.0f}  →  +${tp1_l[0]:.0f}  →  +${tp2_l[0]:.0f}\n"
                        f"💵 0.10    -${sl_l[1]:.0f}  →  +${tp1_l[1]:.0f}  →  +${tp2_l[1]:.0f}\n"
                        f"💵 1.00    -${sl_l[2]:,.0f} →  +${tp1_l[2]:,.0f} →  +${tp2_l[2]:,.0f}"
                        f"</code>"
                    )
                else:
                    proj_section += (
                        f"<code>"
                        f"💵 0.01    -${sl_l[0]:.0f}  →  +${tp1_l[0]:.0f}\n"
                        f"💵 0.10    -${sl_l[1]:.0f}  →  +${tp1_l[1]:.0f}\n"
                        f"💵 1.00    -${sl_l[2]:,.0f} →  +${tp1_l[2]:,.0f}"
                        f"</code>"
                    )

                # SL/TP pip labels
                sl = f"{sl}  (-{sl_pips:.0f}p)"
                tp1 = f"{tp1}  (+{tp1_pips:.0f}p)"
                if tp2_f > 0:
                    tp2 = f"{tp2}  (+{tp2_pips:.0f}p)"

        except Exception as e:
            logger.debug(f"Profit projection calc failed: {e}")

        # Confidence bar
        conf_pct = float(confidence) * 100 if confidence else 0
        conf_filled = int(conf_pct / 10)
        conf_bar = "▓" * conf_filled + "░" * (10 - conf_filled)

        # Build message
        text = (
            f"📊 <b>TRADE SIGNAL</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{desk_emoji} <b>{desk_label}</b> · {symbol} {arrow} <b>{direction}</b>\n\n"
            f"💲 Entry    {price}\n"
            f"🛑 SL       {sl}\n"
            f"🎯 TP1      {tp1}\n"
        )
        if tp2 and str(tp2) != "0" and str(tp2) != "None":
            text += f"🎯 TP2      {tp2}\n"

        text += (
            f"{rr_line}\n"
            f"💰 Risk     ${risk:.2f} ({risk_pct:.2f}%)\n"
            f"📊 Confidence  {confidence}\n"
        )

        text += proj_section

        text += (
            f"\n\n🧠 {reasoning}\n\n"
            f"🔋 {conf_bar} {conf_pct:.0f}%"
        )

        await self._send_to_desk(desk, text)

        # Portfolio summary
        summary = (
            f"{desk_emoji} <b>{desk_label}</b> · {symbol} {arrow} {direction} "
            f"@ {price} · ${risk:.0f} risk"
        )
        await self._send_to_portfolio(summary)

    async def notify_trade_exit(
        self, symbol: str, desk_id: str, pnl: float, reason: str
    ):
        """Notify when a trade is closed — dashboard style."""
        is_win = pnl >= 0
        emoji = "🟩" if is_win else "🟥"
        result = "WIN" if is_win else "LOSS"
        desk_name = DESKS.get(desk_id, {}).get("name", desk_id)
        desk_emoji = {
            "DESK1_SCALPER": "🟢", "DESK2_INTRADAY": "🟡",
            "DESK3_SWING": "🔵", "DESK4_GOLD": "🔴",
            "DESK5_ALTS": "⚫", "DESK6_EQUITIES": "⚪",
        }.get(desk_id, "📊")
        desk_label = {
            "DESK1_SCALPER": "SCALPER", "DESK2_INTRADAY": "INTRADAY",
            "DESK3_SWING": "SWING", "DESK4_GOLD": "GOLD",
            "DESK5_ALTS": "ALTS", "DESK6_EQUITIES": "EQUITIES",
        }.get(desk_id, desk_id)

        sim_tag = " 🔬" if "SIM" in reason else ""
        clean_reason = reason.replace("SIM_", "")

        text = (
            f"{emoji} <b>TRADE CLOSED — {result}</b>{sim_tag}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{desk_emoji} <b>{desk_label}</b> · {symbol}\n\n"
            f"💰 PnL      <b>${pnl:+,.2f}</b>\n"
            f"📋 Reason   {clean_reason}\n"
        )
        await self._send_to_desk(desk_id, text)
        summary = (
            f"{emoji} <b>{desk_name}</b> | {symbol} closed | "
            f"PnL: ${pnl:+.2f} | {reason}"
        )
        await self._send_to_portfolio(summary)

    async def notify_oniai_signal(self, trade_params: Dict, decision: Dict):
        """Notify when CTO approved a signal but risk filter blocked execution.
        Tracked as OniAI virtual trade for accuracy data."""
        symbol = trade_params.get("symbol", "?")
        direction = trade_params.get("direction", "?")
        desk = trade_params.get("desk_id", "?")
        price = trade_params.get("price", 0)
        risk = trade_params.get("risk_dollars", 0)
        risk_pct = trade_params.get("risk_pct", 0)
        sl = trade_params.get("stop_loss", 0)
        tp1 = trade_params.get("take_profit_1", 0)
        tp2 = trade_params.get("take_profit_2", 0)
        confidence = decision.get("confidence", 0)
        reasoning = decision.get("reasoning", "")[:150]
        block_reason = trade_params.get("block_reason", "Unknown")

        desk_emoji = {
            "DESK1_SCALPER": "🟢", "DESK2_INTRADAY": "🟡",
            "DESK3_SWING": "🔵", "DESK4_GOLD": "🔴",
            "DESK5_ALTS": "⚫", "DESK6_EQUITIES": "⚪",
        }.get(desk, "📊")
        desk_label = {
            "DESK1_SCALPER": "SCALPER", "DESK2_INTRADAY": "INTRADAY",
            "DESK3_SWING": "SWING", "DESK4_GOLD": "GOLD",
            "DESK5_ALTS": "ALTS", "DESK6_EQUITIES": "EQUITIES",
        }.get(desk, desk)

        arrow = "↑" if direction.upper() in ["LONG", "BUY"] else "↓"

        # Calculate projections
        proj = ""
        try:
            entry_f = float(price) if price else 0
            sl_f = float(sl) if sl else 0
            tp1_f = float(tp1) if tp1 else 0

            if entry_f > 0 and sl_f > 0 and tp1_f > 0:
                sym = str(symbol).upper()
                if "JPY" in sym: pip_size, pip_val = 0.01, 6.67
                elif "XAU" in sym: pip_size, pip_val = 0.1, 10.0
                elif "XAG" in sym: pip_size, pip_val = 0.01, 50.0
                else: pip_size, pip_val = 0.0001, 10.0

                sl_pips = abs(entry_f - sl_f) / pip_size
                tp1_pips = abs(tp1_f - entry_f) / pip_size
                rr = round(tp1_pips / sl_pips, 1) if sl_pips > 0 else 0

                proj = (
                    f"\n⚖️ R:R 1:{rr} · SL {sl_pips:.0f}p · TP1 {tp1_pips:.0f}p"
                )
        except:
            pass

        text = (
            f"🤖 <b>OniAI SIGNAL</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{desk_emoji} <b>{desk_label}</b> · {symbol} {arrow} <b>{direction}</b>\n\n"
            f"💲 Entry    {price}\n"
            f"🛑 SL       {sl}\n"
            f"🎯 TP1      {tp1}\n"
            f"📊 Conf     {confidence}"
            f"{proj}\n\n"
            f"⚠️ <b>Not executed:</b> {block_reason}\n"
            f"🔬 Tracking as virtual trade\n\n"
            f"🧠 {reasoning}"
        )

        await self._send_to_desk(desk, text)

        summary = (
            f"🤖 <b>OniAI</b> · {desk_label} · {symbol} {arrow} {direction} "
            f"@ {price} · ⚠️ {block_reason}"
        )
        await self._send_to_portfolio(summary)

    async def notify_signal_skip(
        self, symbol: str, desk_id: str, reason: str, score: int
    ):
        """Notify when a signal is skipped (desk channel only)."""
        desk_label = {
            "DESK1_SCALPER": "SCALPER", "DESK2_INTRADAY": "INTRADAY",
            "DESK3_SWING": "SWING", "DESK4_GOLD": "GOLD",
            "DESK5_ALTS": "ALTS", "DESK6_EQUITIES": "EQUITIES",
        }.get(desk_id, desk_id)
        desk_emoji = {
            "DESK1_SCALPER": "🟢", "DESK2_INTRADAY": "🟡",
            "DESK3_SWING": "🔵", "DESK4_GOLD": "🔴",
            "DESK5_ALTS": "⚫", "DESK6_EQUITIES": "⚪",
        }.get(desk_id, "📊")

        text = (
            f"⏭️ <b>SIGNAL SKIPPED</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"{desk_emoji} <b>{desk_label}</b> · {symbol}\n\n"
            f"📊 Score    {score}\n"
            f"📋 Reason   {reason[:150]}"
        )
        await self._send_to_desk(desk_id, text)

    # ─────────────────────────────────────────
    # RISK ALERTS → Portfolio + Affected Desk
    # ─────────────────────────────────────────

    async def alert_drawdown(self, desk_id: str, daily_loss: float, level: str):
        """Alert when desk drawdown hits warning levels."""
        desk_label = {
            "DESK1_SCALPER": "SCALPER", "DESK2_INTRADAY": "INTRADAY",
            "DESK3_SWING": "SWING", "DESK4_GOLD": "GOLD",
            "DESK5_ALTS": "ALTS", "DESK6_EQUITIES": "EQUITIES",
        }.get(desk_id, desk_id)
        text = (
            f"⚠️ <b>DRAWDOWN ALERT</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 <b>{desk_label}</b>\n\n"
            f"📉 Daily Loss   ${abs(daily_loss):.2f}\n"
            f"🚦 Level        {level}\n\n"
            f"⚠️ Action required if loss continues."
        )
        await self._send_to_desk(desk_id, text)
        await self._send_to_portfolio(text)

    async def alert_consecutive_losses(
        self, desk_id: str, count: int, action: str
    ):
        """Alert on consecutive loss milestones."""
        desk_label = {
            "DESK1_SCALPER": "SCALPER", "DESK2_INTRADAY": "INTRADAY",
            "DESK3_SWING": "SWING", "DESK4_GOLD": "GOLD",
            "DESK5_ALTS": "ALTS", "DESK6_EQUITIES": "EQUITIES",
        }.get(desk_id, desk_id)
        text = (
            f"🔴 <b>LOSS STREAK</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 <b>{desk_label}</b>\n\n"
            f"🔥 Streak     {count} consecutive losses\n"
            f"⚡ Action     {action}"
        )
        await self._send_to_desk(desk_id, text)
        await self._send_to_portfolio(text)

    async def alert_firm_drawdown(self, total_loss: float, level: str):
        """Alert on firm-wide drawdown → ALL channels."""
        text = (
            f"🚨 <b>FIRM DRAWDOWN</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"📉 Total Loss   ${abs(total_loss):.2f}\n"
            f"🚦 Level        {level}\n\n"
            f"⚡ All desks reduced to 50% size."
        )
        await self._send_to_all(text)

    async def alert_kill_switch(self, scope: str, triggered_by: str):
        """Alert when kill switch is triggered → ALL channels."""
        text = (
            f"🛑 <b>KILL SWITCH ACTIVATED</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"🎯 Scope       {scope}\n"
            f"⚡ Triggered   {triggered_by}\n\n"
            f"🛑 All positions closing. Trading halted."
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
