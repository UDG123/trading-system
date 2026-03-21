"""
OniQuant v5.9 — Zero-Key Oracle
FastAPI + Redis Stream Ingestor + LocalBroker (Virtual) + apscheduler
"""
import os
import logging
import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import uvloop
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import ORJSONResponse

# Install uvloop as the default event loop policy (before any loop creation)
uvloop.install()

from app.database import engine, Base, check_db_connection, SessionLocal
from app.routes.webhook import router as webhook_router
from app.routes.health import router as health_router
from app.routes.dashboard import router as dashboard_router
from app.routes.telegram import router as telegram_router
from app.routes.control import router as control_router
from app.routes.ml_export import router as ml_export_router
from app.routes.backtest import router as backtest_router

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("TradingSystem")

# Background task handles
_report_task = None
_diag_task = None
_diag_service = None
_price_service = None
_redis_pool = None
_scheduler = None


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
    from app.models.backtest_result import BacktestResult  # noqa — backtest virtual ledger
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables verified (including ml_training_data)")

    # Verify DB connection
    if check_db_connection():
        logger.info("PostgreSQL connection confirmed")
    else:
        logger.error("PostgreSQL connection FAILED - system degraded")

    # ── Redis connection pool (shared across webhook ingestor) ──
    import redis.asyncio as aioredis
    from app.config import REDIS_URL
    from app.routes.webhook import set_redis

    global _redis_pool
    _redis_pool = aioredis.from_url(
        REDIS_URL,
        decode_responses=False,
        max_connections=20,
    )
    set_redis(_redis_pool)
    # Verify connectivity
    try:
        await _redis_pool.ping()
        logger.info(f"Redis connected: {REDIS_URL.split('@')[-1] if '@' in REDIS_URL else REDIS_URL}")
    except Exception as e:
        logger.error(f"Redis connection FAILED: {e} — webhook ingestor degraded")

    # ── Pending Memory Engine — recover parked signals from DB ──
    from app.services.pending_memory import PendingSignalManager
    pending_mgr = PendingSignalManager()
    startup_db = SessionLocal()
    try:
        recovered = pending_mgr.sync_on_startup(startup_db)
        if recovered:
            logger.info(f"Pending Memory: {len(recovered)} signal(s) recovered from DB")
        else:
            logger.info("Pending Memory: clean slate — no pending signals")
    except Exception as e:
        logger.warning(f"Pending Memory startup sync failed: {e}")
    finally:
        startup_db.close()

    logger.info("OniQuant v5.9 Ingestor ONLINE (uvloop + orjson + Redis Streams)")
    logger.info("=" * 60)

    # Start background report scheduler
    global _report_task
    _report_task = asyncio.create_task(_auto_report_scheduler())
    logger.info("Report scheduler started (daily 21:30 UTC, weekly Fri, monthly last day)")

    # PriceService — available for pipeline's on-demand price fetch at entry
    from app.services.price_service import PriceService
    global _price_service
    _price_service = PriceService()
    logger.info("PriceService ready (on-demand only, no continuous feed)")

    # Start diagnostics service
    from app.services.diagnostics import DiagnosticsService
    global _diag_task, _diag_service
    _diag_service = DiagnosticsService()
    _diag_task = asyncio.create_task(_diag_service.run())
    logger.info("Diagnostics service started (checking every 5 min)")

    # ── Performance Digest Scheduler (5:00 PM Toronto Time daily) ──
    from apscheduler.schedulers.asyncio import AsyncIOScheduler
    from apscheduler.triggers.cron import CronTrigger
    from app.worker import send_daily_digest

    global _scheduler
    _scheduler = AsyncIOScheduler()
    _scheduler.add_job(
        send_daily_digest,
        trigger=CronTrigger(hour=17, minute=0, timezone="America/Toronto"),
        args=[SessionLocal],
        id="daily_pnl_digest",
        name="Daily PnL Digest (5 PM Toronto)",
        replace_existing=True,
    )
    _scheduler.start()
    logger.info("APScheduler started: Daily PnL Digest at 17:00 America/Toronto")

    yield

    # Cancel background tasks
    if _report_task:
        _report_task.cancel()
    if _price_service:
        await _price_service.close()
    if _diag_task:
        _diag_task.cancel()
        await _diag_service.stop()
    # Shutdown scheduler
    if _scheduler:
        _scheduler.shutdown(wait=False)
        logger.info("APScheduler stopped")
    # Close Redis pool
    if _redis_pool:
        await _redis_pool.aclose()
        logger.info("Redis pool closed")
    logger.info("Trading system shutting down")


app = FastAPI(
    title="OniQuant v5.9 — Zero-Key Oracle",
    description="Redis Stream Ingestor + LocalBroker + MCP Verification Gates",
    version="5.9.0",
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
app.include_router(ml_export_router, prefix="/api", tags=["ML Data"])
app.include_router(backtest_router, tags=["Backtest"])


# ─── Test Alpha Strike — verify dual-routing + Master Mix layout from mobile ───

@app.get("/test-alpha-strike", response_class=ORJSONResponse)
async def test_alpha_strike():
    """
    Fire a synthetic signal through TelegramService + OracleBridge
    to verify dual-routing (Portfolio + Desk) and the Master Mix layout.
    Hit from mobile browser: https://<railway-url>/test-alpha-strike
    """
    from app.services.telegram_notifications import TelegramService
    from app.services.oracle_bridge import OracleBridge

    # Synthetic signal data for layout verification
    test_data = {
        "desk_id": "DESK4_GOLD",
        "ml_score": 3,
        "hurst": 0.68,
        "direction": "LONG",
        "price": 2652.40,
        "rvol": 1.8,
        "vwap_z": 0.7,
        "ml_conf": 75,
        "sl": 2638.50,
        "tp1": 2680.00,
        "tv_link": "https://www.tradingview.com/chart/?symbol=XAUUSD",
    }

    # Format with Master Mix layout
    message = OracleBridge.format_strike_message(test_data)

    # Dual-route to Portfolio + DESK4_GOLD
    tg = TelegramService()
    await tg.broadcast_signal("DESK4_GOLD", message)

    return {
        "status": "sent",
        "layout": "Master Mix (OracleBridge)",
        "routing": ["TG_PORTFOLIO", "TG_DESK4_GOLD"],
        "message_preview": message[:200],
        "test_data": test_data,
    }
