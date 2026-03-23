"""
Trade Reporter — FTMO Dashboard-Style Reports
Daily, Weekly, Monthly performance reports to Telegram.
"""
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

from sqlalchemy.orm import Session
from sqlalchemy import func, and_

from app.config import DESKS, get_pip_info
from app.models.trade import Trade
from app.models.signal import Signal
from app.services.telegram_bot import TelegramBot

logger = logging.getLogger("TradingSystem.Reporter")


class TradeReporter:
    """Generates FTMO dashboard-style trade reports for Telegram."""

    def __init__(self):
        self.telegram = TelegramBot()

    # ─────────────────────────────────────────
    # HELPERS
    # ─────────────────────────────────────────

    def _pip_value(self, symbol: str) -> float:
        """Pip value per standard lot — delegates to config."""
        _, pip_val = get_pip_info(symbol)
        return pip_val

    def _desk_emoji(self, desk_id: str) -> str:
        emojis = {
            "DESK1_SCALPER": "🟢",
            "DESK2_INTRADAY": "🟡",
            "DESK3_SWING": "🔵",
            "DESK4_GOLD": "🔴",
            "DESK5_ALTS": "⚫",
            "DESK6_EQUITIES": "⚪",
        }
        return emojis.get(desk_id, "📊")

    def _desk_label(self, desk_id: str) -> str:
        labels = {
            "DESK1_SCALPER": "SCALPER",
            "DESK2_INTRADAY": "INTRADAY",
            "DESK3_SWING": "SWING",
            "DESK4_GOLD": "GOLD",
            "DESK5_ALTS": "ALTS",
            "DESK6_EQUITIES": "EQUITIES",
        }
        return labels.get(desk_id, desk_id)

    def _progress_bar(self, win_rate: float) -> str:
        """Generate a progress bar from win rate (0-100)."""
        filled = int(win_rate / 10)
        empty = 10 - filled
        return "▓" * filled + "░" * empty

    # ─────────────────────────────────────────
    # DESK REPORT
    # ─────────────────────────────────────────

    def _format_desk_report(
        self, desk_id: str, trades: List[Trade], period_label: str,
        period_type: str = "Daily"
    ) -> str:
        emoji = self._desk_emoji(desk_id)
        label = self._desk_label(desk_id)

        if not trades:
            return (
                f"📊 {label} DESK\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━\n"
                f"📅 {period_label}\n\n"
                f"No closed trades this period"
            )

        # Split trades into categories by close_reason prefix / status
        srv_trades = [t for t in trades if (t.close_reason or "").startswith("SRV_") or t.status == "SRV_CLOSED"]
        mt5_trades = [t for t in trades if (t.close_reason or "").startswith("MT5_") or t.status == "MT5_CLOSED"]
        oniai_trades = [t for t in trades if (t.close_reason or "").startswith("ONIAI") or t.status == "ONIAI_CLOSED"]

        # Anything not categorized goes to SRV (server sim is default)
        categorized_ids = set(t.id for t in srv_trades + mt5_trades + oniai_trades)
        uncategorized = [t for t in trades if t.id not in categorized_ids]
        srv_trades.extend(uncategorized)

        text = (
            f"📊 {label} DESK\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📅 {period_label}\n"
        )

        # Server sim section
        if srv_trades:
            text += f"\n🖥️ SERVER SIM\n"
            text += self._format_trade_section(srv_trades, desk_id, "SRV")

        # MT5 sim section
        if mt5_trades:
            text += f"\n\n📟 MT5 SIM\n"
            text += self._format_trade_section(mt5_trades, desk_id, "MT5")

        # OniAI section
        if oniai_trades:
            text += f"\n\n🤖 OniAI VIRTUAL\n"
            text += self._format_trade_section(oniai_trades, desk_id, "OniAI")

        return text

    def _format_trade_section(
        self, trades: List[Trade], desk_id: str, section_label: str
    ) -> str:
        """Format a section of trades (real or OniAI)."""
        trade_lines = []
        total_pips_won = 0
        total_pips_lost = 0
        wins = 0
        losses = 0

        for t in trades:
            pips = t.pnl_pips or 0
            direction = (t.direction or "?").lower()
            symbol = t.symbol or "?"
            arrow = "↑" if direction in ["long", "buy"] else "↓"
            reason = t.close_reason or ""
            sim_tag = ""
            if "SRV_" in reason:
                sim_tag = " 🖥️"
            elif "MT5_" in reason:
                sim_tag = " 📟"
            elif "ONIAI" in reason:
                sim_tag = " 🤖"

            if pips >= 0:
                trade_lines.append(
                    f"🟩 {symbol} {arrow} {direction}    +{abs(pips):.0f}p{sim_tag}"
                )
                total_pips_won += pips
                wins += 1
            else:
                trade_lines.append(
                    f"🟥 {symbol} {arrow} {direction}    {pips:.0f}p{sim_tag}"
                )
                total_pips_lost += pips
                losses += 1

        net_pips = total_pips_won + total_pips_lost
        win_rate = (wins / len(trades) * 100) if trades else 0

        avg_rr = 0
        if losses > 0 and total_pips_lost != 0:
            avg_rr = abs(total_pips_won / total_pips_lost)

        symbols_traded = set(t.symbol for t in trades if t.symbol)
        avg_pip_value = 10.0
        if symbols_traded:
            avg_pip_value = sum(
                self._pip_value(s) for s in symbols_traded
            ) / len(symbols_traded)

        lot_01 = net_pips * avg_pip_value * 0.01
        lot_10 = net_pips * avg_pip_value * 0.1
        lot_100 = net_pips * avg_pip_value * 1.0

        bar = self._progress_bar(win_rate)

        text = "\n".join(trade_lines)

        text += (
            f"\n\n━━━ {section_label} PERFORMANCE ━━━\n\n"
            f"📈 Profit    +{total_pips_won:.0f} pips\n"
            f"📉 Loss       {total_pips_lost:.0f} pips\n"
            f"💰 Net       {'+' if net_pips >= 0 else ''}{net_pips:.0f} pips\n\n"
            f"🏆 Record    {wins}W — {losses}L\n"
            f"📊 Win Rate  {win_rate:.0f}%\n"
            f"⚖️ Avg RR    1:{avg_rr:.1f}\n\n"
            f"━━━ PROJECTED P&L ━━━━━━\n\n"
            f"💵 0.01 lot    {'+' if lot_01 >= 0 else ''}"
            f"${abs(lot_01):,.0f}\n"
            f"💵 0.10 lot    {'+' if lot_10 >= 0 else ''}"
            f"${abs(lot_10):,.0f}\n"
            f"💵 1.00 lot    {'+' if lot_100 >= 0 else ''}"
            f"${abs(lot_100):,.0f}\n\n"
            f"🔋 {bar} {win_rate:.0f}%"
        )

        return text

    # ─────────────────────────────────────────
    # PORTFOLIO REPORT
    # ─────────────────────────────────────────

    def _format_portfolio_report(
        self, all_trades: Dict[str, List[Trade]], period_label: str
    ) -> str:
        # Separate SRV vs MT5 vs OniAI across all desks
        srv_by_desk = {}
        mt5_by_desk = {}
        oniai_by_desk = {}
        for desk_id, trades in all_trades.items():
            srv = []
            mt5 = []
            oni = []
            for t in trades:
                cr = t.close_reason or ""
                if cr.startswith("ONIAI") or t.status == "ONIAI_CLOSED":
                    oni.append(t)
                elif cr.startswith("MT5_") or t.status == "MT5_CLOSED":
                    mt5.append(t)
                else:
                    srv.append(t)
            srv_by_desk[desk_id] = srv
            mt5_by_desk[desk_id] = mt5
            oniai_by_desk[desk_id] = oni

        text = (
            f"🏛 ONIQUANT — {period_label.split('·')[0].strip().upper()}\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📅 {period_label}\n"
        )

        # Server sim section
        has_srv = any(len(t) > 0 for t in srv_by_desk.values())
        if has_srv:
            text += f"\n🖥️ SERVER SIM"
            text += self._format_portfolio_section(srv_by_desk, "SRV")

        # MT5 sim section
        has_mt5 = any(len(t) > 0 for t in mt5_by_desk.values())
        if has_mt5:
            text += f"\n\n📟 MT5 SIM"
            text += self._format_portfolio_section(mt5_by_desk, "MT5")

        # OniAI section
        has_oniai = any(len(t) > 0 for t in oniai_by_desk.values())
        if has_oniai:
            text += f"\n\n🤖 OniAI VIRTUAL"
            text += self._format_portfolio_section(oniai_by_desk, "OniAI")

        if not has_srv and not has_mt5 and not has_oniai:
            text += "\n\nNo closed trades this period"

        return text

    def _format_portfolio_section(
        self, trades_by_desk: Dict[str, List[Trade]], section_label: str
    ) -> str:
        """Format a portfolio section (executed or OniAI)."""
        total_trades = 0
        total_wins = 0
        total_pips_won = 0
        total_pips_lost = 0
        desk_lines = []

        for desk_id, trades in trades_by_desk.items():
            if not trades:
                continue

            emoji = self._desk_emoji(desk_id)
            label = self._desk_label(desk_id)

            desk_pips = sum((t.pnl_pips or 0) for t in trades)
            desk_wins = sum(1 for t in trades if (t.pnl_pips or 0) >= 0)
            wr = (desk_wins / len(trades) * 100) if trades else 0

            total_trades += len(trades)
            total_wins += desk_wins
            total_pips_won += sum(
                (t.pnl_pips or 0) for t in trades if (t.pnl_pips or 0) >= 0
            )
            total_pips_lost += sum(
                (t.pnl_pips or 0) for t in trades if (t.pnl_pips or 0) < 0
            )

            pip_sign = "+" if desk_pips >= 0 else ""
            desk_lines.append(
                f"{emoji} {label:<12} 📈 {pip_sign}{desk_pips:.0f}p  "
                f"🔄 {len(trades)}t  🏆 {wr:.0f}%"
            )

        if not desk_lines:
            return f"\n\nNo {section_label.lower()} trades this period"

        net_pips = total_pips_won + total_pips_lost
        total_losses = total_trades - total_wins
        overall_wr = (total_wins / total_trades * 100) if total_trades else 0
        bar = self._progress_bar(overall_wr)

        lot_01 = net_pips * 10 * 0.01
        lot_10 = net_pips * 10 * 0.1
        lot_100 = net_pips * 10 * 1.0

        text = "\n\n"
        text += "\n".join(desk_lines)

        text += (
            f"\n\n━━━ {section_label} METRICS ━━━━━━\n\n"
            f"🔄 Trades     {total_trades}\n"
            f"🏆 Record     {total_wins}W — {total_losses}L\n"
            f"📊 Win Rate   {overall_wr:.0f}%\n"
            f"💰 Net Pips   {'+' if net_pips >= 0 else ''}{net_pips:.0f}\n\n"
            f"━━━ PROJECTED P&L ━━━━━━\n\n"
            f"💵 0.01 lot    {'+' if lot_01 >= 0 else ''}"
            f"${abs(lot_01):,.0f}\n"
            f"💵 0.10 lot    {'+' if lot_10 >= 0 else ''}"
            f"${abs(lot_10):,.0f}\n"
            f"💵 1.00 lot    {'+' if lot_100 >= 0 else ''}"
            f"${abs(lot_100):,.0f}\n\n"
            f"🔋 {bar} {overall_wr:.0f}%"
        )

        return text

    # ─────────────────────────────────────────
    # QUERY + SEND
    # ─────────────────────────────────────────

    def _get_closed_trades(
        self, db: Session, since: datetime, until: datetime = None
    ) -> Dict[str, List[Trade]]:
        """Get closed trades grouped by desk."""
        if until is None:
            until = datetime.now(timezone.utc)

        trades = (
            db.query(Trade)
            .filter(
                Trade.status.in_(["CLOSED", "SRV_CLOSED", "MT5_CLOSED", "ONIAI_CLOSED"]),
                Trade.closed_at >= since,
                Trade.closed_at <= until,
            )
            .order_by(Trade.closed_at.asc())
            .all()
        )

        by_desk = {}
        for desk_id in DESKS:
            by_desk[desk_id] = [t for t in trades if t.desk_id == desk_id]

        return by_desk

    async def send_daily_report(self, db: Session):
        """Send daily report to all channels."""
        now = datetime.now(timezone.utc)
        start_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
        trades_by_desk = self._get_closed_trades(db, start_of_day)
        date_str = now.strftime("%b %d, %Y")
        period = f"Daily · {date_str}"

        for desk_id, trades in trades_by_desk.items():
            if not trades:
                continue
            report = self._format_desk_report(desk_id, trades, period, "Daily")
            await self.telegram._send_to_desk(desk_id, report)

        portfolio = self._format_portfolio_report(trades_by_desk, period)
        await self.telegram._send_to_portfolio(portfolio)
        logger.info(f"Daily report sent for {date_str}")

    async def send_weekly_report(self, db: Session):
        """Send weekly report to all channels."""
        now = datetime.now(timezone.utc)
        start_of_week = now - timedelta(days=now.weekday())
        start_of_week = start_of_week.replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        trades_by_desk = self._get_closed_trades(db, start_of_week)
        week_start = start_of_week.strftime("%b %d")
        week_end = now.strftime("%b %d, %Y")
        period = f"Weekly · {week_start} — {week_end}"

        for desk_id, trades in trades_by_desk.items():
            if not trades:
                continue
            report = self._format_desk_report(desk_id, trades, period, "Weekly")
            await self.telegram._send_to_desk(desk_id, report)

        portfolio = self._format_portfolio_report(trades_by_desk, period)
        await self.telegram._send_to_portfolio(portfolio)
        logger.info(f"Weekly report sent for {period}")

    async def send_monthly_report(self, db: Session):
        """Send monthly report to all channels."""
        now = datetime.now(timezone.utc)
        start_of_month = now.replace(
            day=1, hour=0, minute=0, second=0, microsecond=0
        )
        trades_by_desk = self._get_closed_trades(db, start_of_month)
        period = f"Monthly · {now.strftime('%B %Y')}"

        for desk_id, trades in trades_by_desk.items():
            if not trades:
                continue
            report = self._format_desk_report(desk_id, trades, period, "Monthly")
            await self.telegram._send_to_desk(desk_id, report)

        portfolio = self._format_portfolio_report(trades_by_desk, period)
        await self.telegram._send_to_portfolio(portfolio)
        logger.info(f"Monthly report sent for {period}")

    async def close(self):
        await self.telegram.close()
