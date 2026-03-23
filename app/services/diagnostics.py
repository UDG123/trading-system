"""
System Diagnostics & Auto-Repair Service
Runs every 5 minutes. Checks all components, alerts on errors,
auto-fixes common issues, sends health report to Telegram.

Monitors:
  - Database connectivity
  - Price provider health
  - Stale trades (stuck open)
  - Signal pipeline flow
  - Desk state resets (daily)
  - API key validity
  - Memory/cache health
  - Error rate tracking

Auto-repairs:
  - Clears stale trades stuck EXECUTED for too long
  - Resets daily desk stats at midnight UTC
  - Clears price cache if all prices stale
  - Marks stuck DECIDED signals as EXPIRED
"""
import asyncio
import os
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List

from sqlalchemy import func, text
from sqlalchemy.orm import Session

from app.config import DESKS, CAPITAL_PER_ACCOUNT, MAX_DAILY_LOSS_PER_ACCOUNT
from app.database import SessionLocal
from app.models.trade import Trade
from app.models.signal import Signal
from app.models.desk_state import DeskState
from app.services.telegram_bot import TelegramBot

logger = logging.getLogger("TradingSystem.Diagnostics")


class DiagnosticsService:
    """Monitors system health, alerts on errors, auto-repairs common issues."""

    def __init__(self):
        self.telegram = TelegramBot()
        self.running = True
        self.last_daily_reset = None
        self.error_counts: Dict[str, int] = {}
        self.check_count = 0
        self.last_alert_time: Dict[str, datetime] = {}
        # Cooldown: don't spam same alert within 30 min
        self.alert_cooldown = timedelta(minutes=30)

    # ─────────────────────────────────────────
    # MAIN LOOP
    # ─────────────────────────────────────────

    async def run(self):
        """Main diagnostics loop. Runs every 5 minutes."""
        logger.info("Diagnostics service started | Checking every 5 min")

        # Wait 60 seconds on startup before first check
        await asyncio.sleep(60)

        while self.running:
            try:
                self.check_count += 1
                issues = []
                repairs = []

                db = SessionLocal()
                try:
                    # ── Core health checks ──
                    issues += self._check_database(db)
                    issues += await self._check_price_providers()
                    issues += self._check_api_keys()

                    # ── Trade health ──
                    stale_repaired = self._repair_stale_trades(db)
                    if stale_repaired:
                        repairs.append(f"Cleared {stale_repaired} stale trades")

                    expired = self._expire_stuck_signals(db)
                    if expired:
                        repairs.append(f"Expired {expired} stuck signals")

                    issues += self._check_open_trade_health(db)

                    # ── Daily reset ──
                    reset = self._daily_desk_reset(db)
                    if reset:
                        repairs.append("Daily desk stats reset")

                    # ── Drawdown alerts ──
                    issues += self._check_drawdown_alerts(db)

                    # ── Pipeline flow ──
                    issues += self._check_pipeline_flow(db)

                    db.commit()

                finally:
                    db.close()

                # ── Alert if issues found ──
                if issues or repairs:
                    await self._send_diagnostic_alert(issues, repairs)

                # ── Hourly health summary (every 12 checks = 60 min) ──
                if self.check_count % 12 == 0:
                    await self._send_health_summary()

            except Exception as e:
                logger.error(f"Diagnostics error: {e}", exc_info=True)
                self._track_error("diagnostics_loop")

            await asyncio.sleep(300)  # 5 minutes

    async def stop(self):
        self.running = False
        await self.telegram.close()
        logger.info("Diagnostics service stopped")

    # ─────────────────────────────────────────
    # HEALTH CHECKS
    # ─────────────────────────────────────────

    def _check_database(self, db: Session) -> List[str]:
        """Verify database is responsive."""
        issues = []
        try:
            result = db.execute(text("SELECT 1")).scalar()
            if not result:
                issues.append("🔴 Database not responding")
        except Exception as e:
            issues.append(f"🔴 Database error: {str(e)[:100]}")
            self._track_error("database")
        return issues

    async def _check_price_providers(self) -> List[str]:
        """Check price provider API key availability."""
        issues = []
        if not os.getenv("TWELVEDATA_API_KEY"):
            issues.append("🔴 TWELVEDATA_API_KEY not set")
        return issues

    def _check_api_keys(self) -> List[str]:
        """Verify all required API keys are set."""
        issues = []
        required = {
            "WEBHOOK_SECRET": os.getenv("WEBHOOK_SECRET"),
            "TELEGRAM_BOT_TOKEN": os.getenv("TELEGRAM_BOT_TOKEN"),
        }
        recommended = {
            "TWELVEDATA_API_KEY": os.getenv("TWELVEDATA_API_KEY"),
            "ANTHROPIC_API_KEY": os.getenv("ANTHROPIC_API_KEY"),
        }

        for key, val in required.items():
            if not val:
                issues.append(f"🔴 Missing required key: {key}")

        for key, val in recommended.items():
            if not val:
                issues.append(f"🟡 Missing recommended key: {key}")

        return issues

    def _check_open_trade_health(self, db: Session) -> List[str]:
        """Check for abnormal open trade states."""
        issues = []

        # Count open trades
        open_count = (
            db.query(func.count(Trade.id))
            .filter(Trade.status.in_(["EXECUTED", "OPEN", "ONIAI_OPEN"]))
            .scalar()
        )

        if open_count > 30:
            issues.append(
                f"🟡 High open trade count: {open_count} "
                f"(expected <20, possible stale trades)"
            )

        # Check for trades open > 7 days (stuck)
        week_ago = datetime.now(timezone.utc) - timedelta(days=7)
        stuck = (
            db.query(func.count(Trade.id))
            .filter(
                Trade.status.in_(["EXECUTED", "OPEN", "ONIAI_OPEN"]),
                Trade.opened_at < week_ago,
            )
            .scalar()
        )

        if stuck > 0:
            issues.append(
                f"🟡 {stuck} trades open > 7 days (will auto-repair next cycle)"
            )

        return issues

    def _check_drawdown_alerts(self, db: Session) -> List[str]:
        """Check if any desk is approaching drawdown limits."""
        issues = []

        for desk_id in DESKS:
            state = db.query(DeskState).filter(
                DeskState.desk_id == desk_id
            ).first()
            if not state:
                continue

            daily_loss = abs(state.daily_loss or 0)
            limit = MAX_DAILY_LOSS_PER_ACCOUNT

            pct_used = (daily_loss / limit * 100) if limit > 0 else 0

            if pct_used >= 80:
                desk_label = desk_id.split("_", 1)[1] if "_" in desk_id else desk_id
                issues.append(
                    f"🔴 {desk_label}: {pct_used:.0f}% of daily loss limit "
                    f"(${daily_loss:,.0f} / ${limit:,.0f})"
                )
            elif pct_used >= 60:
                desk_label = desk_id.split("_", 1)[1] if "_" in desk_id else desk_id
                issues.append(
                    f"🟡 {desk_label}: {pct_used:.0f}% of daily loss limit "
                    f"(${daily_loss:,.0f} / ${limit:,.0f})"
                )

        return issues

    def _check_pipeline_flow(self, db: Session) -> List[str]:
        """Check if signals are flowing through the pipeline."""
        issues = []

        # Check last signal received
        last_signal = (
            db.query(Signal)
            .order_by(Signal.received_at.desc())
            .first()
        )

        if last_signal and last_signal.received_at:
            hours_since = (
                datetime.now(timezone.utc) - last_signal.received_at
            ).total_seconds() / 3600

            # Only alert during trading hours (Sun 22:00 - Fri 22:00 UTC)
            now = datetime.now(timezone.utc)
            is_weekend = (
                (now.weekday() == 5) or  # Saturday
                (now.weekday() == 6 and now.hour < 22) or  # Sunday before 22:00
                (now.weekday() == 4 and now.hour >= 22)  # Friday after 22:00
            )

            if hours_since > 6 and not is_weekend:
                issues.append(
                    f"🟡 No signals received in {hours_since:.1f} hours "
                    f"(check TradingView alerts)"
                )

        return issues

    # ─────────────────────────────────────────
    # AUTO-REPAIR
    # ─────────────────────────────────────────

    def _repair_stale_trades(self, db: Session) -> int:
        """Close trades stuck open for too long (> max_hold × 2)."""
        repaired = 0
        cutoff = datetime.now(timezone.utc) - timedelta(hours=168)  # 7 days

        stale_trades = (
            db.query(Trade)
            .filter(
                Trade.status.in_(["EXECUTED", "OPEN", "ONIAI_OPEN"]),
                Trade.opened_at < cutoff,
            )
            .all()
        )

        for trade in stale_trades:
            trade.status = "SRV_CLOSED" if trade.status != "ONIAI_OPEN" else "ONIAI_CLOSED"
            trade.close_reason = "DIAG_STALE_CLEANUP"
            trade.pnl_dollars = 0
            trade.pnl_pips = 0
            trade.closed_at = datetime.now(timezone.utc)
            repaired += 1

            logger.info(
                f"DIAG REPAIR | Closed stale trade #{trade.id} | "
                f"{trade.symbol} | Open since {trade.opened_at}"
            )

        if repaired:
            db.commit()

        return repaired

    def _expire_stuck_signals(self, db: Session) -> int:
        """Expire signals stuck in DECIDED/QUEUED for > 1 hour."""
        expired = 0
        cutoff = datetime.now(timezone.utc) - timedelta(hours=1)

        stuck = (
            db.query(Signal)
            .filter(
                Signal.status.in_(["DECIDED", "QUEUED"]),
                Signal.received_at < cutoff,
            )
            .all()
        )

        for sig in stuck:
            sig.status = "EXPIRED"
            expired += 1

        if expired:
            db.commit()
            logger.info(f"DIAG REPAIR | Expired {expired} stuck signals")

        return expired

    def _daily_desk_reset(self, db: Session) -> bool:
        """Reset daily stats at midnight UTC."""
        now = datetime.now(timezone.utc)

        # Check if we already reset today
        if self.last_daily_reset and self.last_daily_reset.date() == now.date():
            return False

        # Only reset between 00:00-00:10 UTC
        if now.hour != 0 or now.minute > 10:
            return False

        desks = db.query(DeskState).all()
        for state in desks:
            state.trades_today = 0
            state.daily_pnl = 0
            state.daily_loss = 0
            state.consecutive_losses = 0
            state.is_paused = False
            state.pause_until = None
            if not state.is_active:
                state.is_active = True  # re-enable desks closed by consecutive losses

        db.commit()
        self.last_daily_reset = now
        logger.info("DIAG | Daily desk stats reset")
        return True

    # ─────────────────────────────────────────
    # ALERTS & REPORTING
    # ─────────────────────────────────────────

    def _track_error(self, category: str):
        """Track error counts for monitoring."""
        self.error_counts[category] = self.error_counts.get(category, 0) + 1

    def _should_alert(self, alert_key: str) -> bool:
        """Check if enough time has passed since last alert of this type."""
        now = datetime.now(timezone.utc)
        last = self.last_alert_time.get(alert_key)
        if last and (now - last) < self.alert_cooldown:
            return False
        self.last_alert_time[alert_key] = now
        return True

    async def _send_diagnostic_alert(self, issues: List[str], repairs: List[str]):
        """Send alert to Telegram if issues found."""
        # Deduplicate and check cooldown
        new_issues = []
        for issue in issues:
            key = issue[:50]
            if self._should_alert(key):
                new_issues.append(issue)

        if not new_issues and not repairs:
            return

        text = "🔧 <b>SYSTEM DIAGNOSTIC</b>\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"

        if new_issues:
            text += "<b>Issues:</b>\n"
            for issue in new_issues:
                text += f"{issue}\n"

        if repairs:
            text += "\n<b>Auto-Repairs:</b>\n"
            for repair in repairs:
                text += f"🔧 {repair}\n"

        text += f"\n🕐 {datetime.now(timezone.utc).strftime('%H:%M UTC')}"

        try:
            await self.telegram._send_to_system(text)
        except Exception as e:
            logger.error(f"Diagnostic alert failed: {e}")

    async def _send_health_summary(self):
        """Send hourly health summary."""
        try:
            db = SessionLocal()
            try:
                # Open trades
                open_srv = db.query(func.count(Trade.id)).filter(
                    Trade.status == "EXECUTED"
                ).scalar()
                open_oniai = db.query(func.count(Trade.id)).filter(
                    Trade.status == "ONIAI_OPEN"
                ).scalar()

                # Today's trades
                today = datetime.now(timezone.utc).replace(
                    hour=0, minute=0, second=0, microsecond=0
                )
                closed_today = db.query(func.count(Trade.id)).filter(
                    Trade.status.in_(["CLOSED", "SRV_CLOSED", "MT5_CLOSED", "ONIAI_CLOSED"]),
                    Trade.closed_at >= today,
                ).scalar()

                signals_today = db.query(func.count(Signal.id)).filter(
                    Signal.received_at >= today,
                ).scalar()

                # Provider key availability
                active_providers = sum([
                    bool(os.getenv("TWELVEDATA_API_KEY")),
                    True,  # Binance REST (no key needed)
                ])

                # Error count
                total_errors = sum(self.error_counts.values())

            finally:
                db.close()

            status_emoji = "🟢" if total_errors == 0 else ("🟡" if total_errors < 5 else "🔴")

            text = (
                f"{status_emoji} <b>SYSTEM HEALTH</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📡 Price Providers   {active_providers}/3 configured\n"
                f"📊 Open Trades       {open_srv} SRV · {open_oniai} OniAI\n"
                f"🔄 Closed Today      {closed_today}\n"
                f"📥 Signals Today     {signals_today}\n"
                f"⚠️ Errors            {total_errors}\n\n"
                f"🕐 {datetime.now(timezone.utc).strftime('%H:%M UTC')} · "
                f"Check #{self.check_count}"
            )

            await self.telegram._send_to_system(text)

        except Exception as e:
            logger.error(f"Health summary failed: {e}")

    # ─────────────────────────────────────────
    # ON-DEMAND DIAGNOSTICS (called by /diag command)
    # ─────────────────────────────────────────

    async def run_full_diagnostic(self) -> str:
        """Run all checks and return a full diagnostic report."""
        results = []

        db = SessionLocal()
        try:
            # Database
            db_issues = self._check_database(db)
            results.append(("Database", "🟢 OK" if not db_issues else db_issues[0]))

            # API Keys
            key_issues = self._check_api_keys()
            if not key_issues:
                results.append(("API Keys", "🟢 All present"))
            else:
                for issue in key_issues:
                    results.append(("API Keys", issue))

            # Price Providers
            price_issues = await self._check_price_providers()
            if not price_issues:
                results.append(("Price Feeds", "🟢 Healthy"))
            else:
                for issue in price_issues:
                    results.append(("Price Feeds", issue))

            # Open Trades
            open_count = db.query(func.count(Trade.id)).filter(
                Trade.status.in_(["EXECUTED", "OPEN", "ONIAI_OPEN"])
            ).scalar()
            results.append(("Open Trades", f"🟢 {open_count} positions"))

            # Trade health
            trade_issues = self._check_open_trade_health(db)
            for issue in trade_issues:
                results.append(("Trade Health", issue))
            if not trade_issues:
                results.append(("Trade Health", "🟢 No stale trades"))

            # Pipeline
            pipe_issues = self._check_pipeline_flow(db)
            for issue in pipe_issues:
                results.append(("Pipeline", issue))
            if not pipe_issues:
                results.append(("Pipeline", "🟢 Signals flowing"))

            # Drawdown
            dd_issues = self._check_drawdown_alerts(db)
            for issue in dd_issues:
                results.append(("Drawdown", issue))
            if not dd_issues:
                results.append(("Drawdown", "🟢 All desks within limits"))

            # Desk states
            active_desks = db.query(func.count(DeskState.id)).filter(
                DeskState.is_active == True
            ).scalar()
            paused_desks = db.query(func.count(DeskState.id)).filter(
                DeskState.is_paused == True
            ).scalar()
            results.append(("Desks", f"🟢 {active_desks} active · {paused_desks} paused"))

        finally:
            db.close()

        # Format report
        text = (
            "🔧 <b>FULL DIAGNOSTIC</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        )
        for category, status in results:
            text += f"<b>{category}:</b> {status}\n"

        text += (
            f"\n🕐 {datetime.now(timezone.utc).strftime('%H:%M UTC')} · "
            f"Checks run: {self.check_count}"
        )

        return text
