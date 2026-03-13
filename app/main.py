"""
Autonomous Institutional Trading System - Phase 1
FastAPI Webhook Receiver for LuxAlgo TradingView Alerts
"""
import os
import logging
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.database import engine, Base, check_db_connection, SessionLocal
from app.routes.webhook import router as webhook_router
from app.routes.health import router as health_router
from app.routes.dashboard import router as dashboard_router
from app.routes.telegram import router as telegram_router
from app.routes.control import router as control_router
from app.routes.trade_queue import router as trade_queue_router
from app.routes.ml_export import router as ml_export_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("TradingSystem")

# Background task handles
_report_task = None
_simulator_task = None
_diag_task = None
_diag_service = None
_price_service = None


async def _auto_report_scheduler():
    """Background task that sends reports at scheduled times (UTC).
    Daily: 21:30 UTC (after NY close)
    Weekly: Friday 21:30 UTC
    Monthly: Last day of month 21:30 UTC
    """
    from app.services.trade_reporter import TradeReporter

    last_daily = None
    last_weekly = None
    last_monthly = None

    while True:
        try:
            await asyncio.sleep(60)  # check every minute

            now = datetime.now(timezone.utc)
            today = now.strftime("%Y-%m-%d")
            hour = now.hour
            minute = now.minute
            weekday = now.weekday()  # 0=Mon, 4=Fri
            day = now.day

            # Daily report at 21:30 UTC
            if hour == 21 and 30 <= minute < 31 and last_daily != today:
                last_daily = today
                db = SessionLocal()
                try:
                    reporter = TradeReporter()
                    await reporter.send_daily_report(db)
                    await reporter.close()
                    logger.info("Auto daily report sent")
                finally:
                    db.close()

            # Weekly report on Friday at 21:30 UTC
            if weekday == 4 and hour == 21 and 30 <= minute < 31 and last_weekly != today:
                last_weekly = today
                db = SessionLocal()
                try:
                    reporter = TradeReporter()
                    await reporter.send_weekly_report(db)
                    await reporter.close()
                    logger.info("Auto weekly report sent")
                finally:
                    db.close()

            # Monthly report on last day at 21:30 UTC
            import calendar
            last_day = calendar.monthrange(now.year, now.month)[1]
            if day == last_day and hour == 21 and 30 <= minute < 31 and last_monthly != today:
                last_monthly = today
                db = SessionLocal()
                try:
                    reporter = TradeReporter()
                    await reporter.send_monthly_report(db)
                    await reporter.close()
                    logger.info("Auto monthly report sent")
                finally:
                    db.close()

        except Exception as e:
            logger.error(f"Report scheduler error: {e}")
            await asyncio.sleep(60)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup and shutdown lifecycle."""
    logger.info("=" * 60)
    logger.info("AUTONOMOUS TRADING SYSTEM - INITIALIZING")
    logger.info("=" * 60)

    # Create tables (import all models first so they register with Base)
    from app.models.signal import Signal  # noqa
    from app.models.trade import Trade  # noqa
    from app.models.desk_state import DeskState  # noqa
    from app.models.ml_trade_log import MLTradeLog  # noqa — ensure table created
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables verified (including ml_training_data)")

    # Verify DB connection
    if check_db_connection():
        logger.info("PostgreSQL connection confirmed")
    else:
        logger.error("PostgreSQL connection FAILED - system degraded")

    logger.info("Phase 1 Webhook Receiver ONLINE")
    logger.info("=" * 60)

    # Start background report scheduler
    global _report_task, _simulator_task
    _report_task = asyncio.create_task(_auto_report_scheduler())
    logger.info("Report scheduler started (daily 21:30 UTC, weekly Fri, monthly last day)")

    # PriceService — available for pipeline's on-demand price fetch at entry
    # NO continuous feed — the JIT simulator handles its own batched pricing
    from app.services.price_service import PriceService
    global _price_service
    _price_service = PriceService()
    # NOTE: start_feed() intentionally NOT called — it burns API credits
    # polling ALL 49 symbols continuously. The simulator does JIT batched
    # calls only for symbols with open trades.
    logger.info("PriceService ready (on-demand only, no continuous feed)")

    # Start server-side trade simulator (batched 2-provider JIT — self-contained)
    from app.services.server_simulator import ServerSimulator
    _sim = ServerSimulator()
    _simulator_task = asyncio.create_task(_sim.run())
    logger.info("Batched JIT simulator started (TwelveData batch + Finnhub fallback)")

    # Start diagnostics service
    from app.services.diagnostics import DiagnosticsService
    global _diag_task, _diag_service
    _diag_service = DiagnosticsService()
    _diag_task = asyncio.create_task(_diag_service.run())
    logger.info("Diagnostics service started (checking every 5 min)")

    yield

    # Cancel background tasks
    if _report_task:
        _report_task.cancel()
    if _simulator_task:
        _simulator_task.cancel()
        await _sim.stop()
    if _price_service:
        await _price_service.close()
    if _diag_task:
        _diag_task.cancel()
        await _diag_service.stop()
    logger.info("Trading system shutting down")


app = FastAPI(
    title="Autonomous Institutional Trading System",
    description="Phase 1: Webhook Receiver & Signal Logger",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register routes
app.include_router(health_router, prefix="/api", tags=["Health"])
app.include_router(webhook_router, prefix="/api", tags=["Webhook"])
app.include_router(dashboard_router, prefix="/api", tags=["Dashboard"])
app.include_router(telegram_router, prefix="/api", tags=["Telegram"])
app.include_router(control_router, prefix="/api", tags=["Control"])
app.include_router(trade_queue_router, prefix="/api", tags=["Trade Queue"])
app.include_router(ml_export_router, prefix="/api", tags=["ML Data"])
