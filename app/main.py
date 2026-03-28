"""
OniQuant v6.1 — Signal Generator
FastAPI + Redis Stream Ingestor + VirtualBroker (Shadow Sim) + apscheduler
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

from app.database import engine, Base, check_db_connection, SessionLocal, get_db
from app.routes.webhook import router as webhook_router
from app.routes.health import router as health_router
from app.routes.dashboard import router as dashboard_router
from app.routes.telegram import router as telegram_router
from app.routes.control import router as control_router
from app.routes.ml_export import router as ml_export_router

try:
    from app.routes.simulation import router as sim_router
except Exception as _sim_err:
    sim_router = None

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
_signal_engine_task = None


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
    logger.info("OniQuant v6.1 Signal Generator - INITIALIZING")
    logger.info("=" * 60)

    # Create tables (import all models first so they register with Base)
    from app.models.signal import Signal  # noqa
    from app.models.trade import Trade  # noqa
    from app.models.desk_state import DeskState  # noqa
    from app.models.ml_trade_log import MLTradeLog  # noqa — ensure table created
    try:
        from app.models.shadow_signal import ShadowSignal  # noqa — shadow sim engine
        from app.models.sim_models import (  # noqa — simulation environment
            SimProfile, SimOrder, SimPosition, SimEquitySnapshot, SpreadReference,
        )
    except Exception as e:
        logger.error(f"Shadow sim models failed to import: {e}")
    Base.metadata.create_all(bind=engine)
    logger.info("Database tables verified")

    # Create OHLCV tables for signal engine (not managed by SQLAlchemy ORM)
    from sqlalchemy import text as sa_text
    with engine.connect() as conn:
        for tf_table in ["ohlcv_5m", "ohlcv_15m", "ohlcv_1h", "ohlcv_4h", "ohlcv_1d", "ohlcv_1w"]:
            conn.execute(sa_text(f"""
                CREATE TABLE IF NOT EXISTS {tf_table} (
                    time TIMESTAMPTZ NOT NULL,
                    symbol VARCHAR(20) NOT NULL,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    volume DOUBLE PRECISION,
                    PRIMARY KEY (time, symbol)
                )
            """))
            conn.execute(sa_text(f"""
                CREATE INDEX IF NOT EXISTS ix_{tf_table}_symbol_time ON {tf_table} (symbol, time)
            """))
        conn.commit()
        logger.info("Signal engine OHLCV tables verified (5m, 15m, 1h, 4h, 1d, 1w)")

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

    logger.info("OniQuant v6.1.0 Signal Generator ONLINE (uvloop + orjson + Redis Streams)")
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
    # ── Shadow Sim Engine Scheduled Jobs ──
    try:
        from apscheduler.triggers.interval import IntervalTrigger

        async def _run_triple_barrier_labeler():
            """Label shadow signals with triple-barrier outcomes."""
            try:
                from app.services.triple_barrier_labeler import TripleBarrierLabeler
                labeler = TripleBarrierLabeler(SessionLocal)
                count = await labeler.label_batch(limit=500)
                if count > 0:
                    logger.info(f"Triple-barrier labeled {count} signals")
            except Exception as e:
                logger.debug(f"Triple-barrier labeler error: {e}")

        async def _run_equity_snapshot():
            """Take equity snapshots for all sim profiles."""
            try:
                from app.services.virtual_broker import VirtualBroker
                broker = VirtualBroker(SessionLocal)
                db = SessionLocal()
                try:
                    await broker.take_equity_snapshot(db)
                    db.commit()
                finally:
                    db.close()
            except Exception as e:
                logger.debug(f"Equity snapshot error: {e}")

        async def _run_sim_exit_checker():
            """Check sim positions for exit conditions.
            Uses batch TwelveData call (1 API credit) instead of per-symbol calls.
            """
            try:
                from app.services.virtual_broker import VirtualBroker
                broker = VirtualBroker(SessionLocal)
                db = SessionLocal()
                try:
                    from app.services.price_service import PriceService
                    from app.models.sim_models import SimPosition
                    open_syms = (
                        db.query(SimPosition.symbol)
                        .filter(SimPosition.status.in_(["OPEN", "PARTIAL"]))
                        .distinct()
                        .all()
                    )
                    if open_syms:
                        ps = PriceService()
                        symbols = [sym for (sym,) in open_syms]
                        prices = await ps.get_prices_batch(symbols)
                        await ps.close()
                        if prices:
                            closed = await broker.check_exits(db, prices)
                            db.commit()
                            if closed:
                                logger.debug(f"Sim exit checker: {len(closed)} positions closed")
                finally:
                    db.close()
            except Exception as e:
                logger.debug(f"Sim exit checker error: {e}")

        # ── Pending Entry Engine: re-evaluate parked signals every 30s ──
        async def _run_pending_check():
            try:
                from app.services.pending_engine import PendingEngine
                from app.services.price_service import PriceService
                from app.services.telegram_bot import TelegramBot
                from app.models.trade import Trade as TradeModel
                from app.config import (
                    DESKS, PORTFOLIO_CAPITAL_PER_DESK, CAPITAL_PER_ACCOUNT,
                    get_pip_info, calculate_lot_size,
                )
                engine = PendingEngine()
                ps = PriceService()
                db = SessionLocal()
                try:
                    triggered = await engine.check_pending(db, ps)
                    for p in triggered:
                        # Create SIM_OPEN trade for triggered pending signal
                        desk = DESKS.get(p.desk_id, {})
                        risk_pct = desk.get("risk_pct", 1.0)
                        desk_capital = PORTFOLIO_CAPITAL_PER_DESK.get(p.desk_id, CAPITAL_PER_ACCOUNT)
                        risk_dollars = desk_capital * (risk_pct / 100)
                        pip_size, pip_value = get_pip_info(p.symbol)
                        sl_pips = abs(float(p.trigger_price or p.entry_target) - float(p.stop_loss or 0)) / pip_size if p.stop_loss and pip_size else 0
                        lot_size = calculate_lot_size(
                            desk_id=p.desk_id, symbol=p.symbol,
                            risk_pct=risk_pct, sl_pips=sl_pips,
                            account_capital=desk_capital,
                        )
                        trade = TradeModel(
                            signal_id=p.signal_id,
                            desk_id=p.desk_id,
                            symbol=p.symbol,
                            direction=p.direction,
                            entry_price=p.trigger_price or p.entry_target,
                            lot_size=lot_size,
                            risk_pct=risk_pct,
                            risk_dollars=risk_dollars,
                            stop_loss=p.stop_loss,
                            take_profit_1=p.take_profit_1,
                            take_profit_2=p.take_profit_2,
                            status="SIM_OPEN",
                            opened_at=p.triggered_at,
                        )
                        db.add(trade)
                        db.flush()
                        # Send Telegram alert
                        tg = TelegramBot()
                        trade_params = {
                            "desk_id": p.desk_id, "symbol": p.symbol,
                            "direction": p.direction, "price": p.trigger_price,
                            "alert_type": p.alert_type, "risk_pct": risk_pct,
                            "risk_dollars": round(risk_dollars, 2),
                            "stop_loss": p.stop_loss,
                            "take_profit_1": p.take_profit_1,
                            "take_profit_2": p.take_profit_2,
                        }
                        await tg.notify_trade_entry(
                            trade_params,
                            {"decision": "EXECUTE", "reasoning": f"Pending #{p.id} triggered at pullback"},
                        )
                        logger.info(
                            f"PENDING→TRADE | #{p.id} → Trade #{trade.id} | "
                            f"{p.symbol} {p.direction} @ {p.trigger_price}"
                        )
                    db.commit()
                finally:
                    db.close()
                    await ps.close()
            except Exception as e:
                logger.debug(f"Pending engine check error: {e}")

        _scheduler.add_job(
            _run_pending_check,
            trigger=IntervalTrigger(seconds=30),
            id="pending_entry_check",
            name="Pending Entry Checker (every 30s)",
            replace_existing=True,
        )

        _scheduler.add_job(
            _run_triple_barrier_labeler,
            trigger=IntervalTrigger(minutes=30),
            id="triple_barrier_labeler",
            name="Triple-Barrier Labeler (every 30 min)",
            replace_existing=True,
        )
        _scheduler.add_job(
            _run_equity_snapshot,
            trigger=IntervalTrigger(minutes=15),
            id="equity_snapshot",
            name="Sim Equity Snapshot (every 15 min)",
            replace_existing=True,
        )
        _scheduler.add_job(
            _run_sim_exit_checker,
            trigger=IntervalTrigger(seconds=60),
            id="sim_exit_checker",
            name="Sim Exit Checker (every 60s)",
            replace_existing=True,
        )
        logger.info("Shadow sim jobs registered: pending-entry 30s | labeler 30min | equity 15min | exit-check 60s")
    except Exception as e:
        logger.error(f"Shadow sim scheduler jobs failed to register: {e}")

    _scheduler.start()
    logger.info("APScheduler started: Daily PnL Digest at 17:00 America/Toronto")

    # ── Python Signal Engine (v7.0) ──
    from app.config import SIGNAL_SOURCE
    global _signal_engine_task
    if SIGNAL_SOURCE in ("PYTHON_ONLY", "BOTH"):
        try:
            from app.services.signal_engine.engine import SignalEngine
            _signal_engine = SignalEngine(
                redis_pool=_redis_pool,
                db_session_factory=SessionLocal,
            )
            _signal_engine_task = asyncio.create_task(_signal_engine.run())
            logger.info(
                f"Python Signal Engine started (mode: {SIGNAL_SOURCE}) — "
                f"replacing TradingView alerts"
            )
        except Exception as e:
            logger.error(f"Signal Engine failed to start: {e}", exc_info=True)
    else:
        logger.info(f"Signal Engine disabled (SIGNAL_SOURCE={SIGNAL_SOURCE})")

    # ── Quant Stack Scheduled Jobs ──
    try:
        from apscheduler.triggers.cron import CronTrigger
        from apscheduler.triggers.interval import IntervalTrigger

        # HMM Regime Detector — daily retraining at 00:05 UTC
        async def _train_regime_detector():
            try:
                from app.services.regime_detector import RegimeDetector
                detector = RegimeDetector(redis_pool=_redis_pool)
                db = SessionLocal()
                try:
                    result = await detector.train_all(db)
                    logger.info(f"HMM regime training: {result}")
                finally:
                    db.close()
            except Exception as e:
                logger.debug(f"Regime training error: {e}")

        _scheduler.add_job(
            _train_regime_detector,
            trigger=CronTrigger(hour=0, minute=5, timezone="UTC"),
            id="hmm_regime_train",
            name="HMM Regime Detector (daily 00:05 UTC)",
            replace_existing=True,
        )

        # Meta-Labeler — weekly retraining Sunday 00:10 UTC
        async def _train_meta_labeler():
            try:
                from app.services.meta_labeler import MetaLabeler
                ml = MetaLabeler()
                db = SessionLocal()
                try:
                    result = ml.train(db)
                    logger.info(f"Meta-labeler training: {result}")
                finally:
                    db.close()
            except Exception as e:
                logger.debug(f"Meta-labeler training error: {e}")

        _scheduler.add_job(
            _train_meta_labeler,
            trigger=CronTrigger(day_of_week="sun", hour=0, minute=10, timezone="UTC"),
            id="meta_labeler_train",
            name="Meta-Labeler (weekly Sun 00:10 UTC)",
            replace_existing=True,
        )

        # Factor Monitor — hourly concentration check
        async def _check_factors():
            try:
                from app.services.factor_monitor import FactorMonitor
                fm = FactorMonitor()
                db = SessionLocal()
                try:
                    result = fm.check_concentration(db)
                    if result.get("concentration_warning"):
                        from app.services.telegram_bot import TelegramBot
                        tg = TelegramBot()
                        await tg._send_to_system(
                            f"⚠️ FACTOR CONCENTRATION ALERT\n"
                            f"Dominant: {result.get('dominant_factor')}\n"
                            f"Cross-desk corr: {result.get('cross_desk_correlation')}\n"
                            f"Action: {result.get('recommendation')}"
                        )
                finally:
                    db.close()
            except Exception as e:
                logger.debug(f"Factor monitor error: {e}")

        _scheduler.add_job(
            _check_factors,
            trigger=IntervalTrigger(minutes=60),
            id="factor_monitor",
            name="Factor Monitor (hourly)",
            replace_existing=True,
        )

        # FRED macro fetch — daily at 14:00 UTC
        async def _fetch_fred():
            try:
                from app.services.fred_service import FREDService
                fred = FREDService(redis_pool=_redis_pool)
                try:
                    vix = await fred.get_vix()
                    dxy = await fred.get_dxy()
                    logger.info(f"FRED fetch: VIX={vix}, DXY={dxy}")
                finally:
                    await fred.close()
            except Exception as e:
                logger.debug(f"FRED fetch error: {e}")

        _scheduler.add_job(
            _fetch_fred,
            trigger=CronTrigger(hour=14, minute=0, timezone="UTC"),
            id="fred_daily",
            name="FRED Daily Macro (14:00 UTC)",
            replace_existing=True,
        )

        logger.info(
            "Quant stack jobs registered: "
            "HMM daily | Meta-labeler weekly | Factor monitor hourly | FRED daily"
        )
    except Exception as e:
        logger.error(f"Quant stack scheduler jobs failed: {e}")

    yield

    # Cancel background tasks
    if _signal_engine_task and not _signal_engine_task.done():
        _signal_engine_task.cancel()
        logger.info("Signal Engine stopped")
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
    title="OniQuant v6.1 — Signal Generator",
    description="Pure signal generator: Redis Stream Ingestor + Shadow Pipeline + Telegram Alerts",
    version="6.1.0",
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
if sim_router is not None:
    app.include_router(sim_router, prefix="/api", tags=["Simulation"])
else:
    logging.getLogger("TradingSystem").error(
        f"Simulation router failed to load: {_sim_err}"
    )


# ─── Read-only query endpoints for MCP / dashboard ───

from fastapi import Depends
from sqlalchemy.orm import Session
from sqlalchemy import text


@app.get("/api/signals/recent", tags=["MCP"])
async def get_recent_signals(limit: int = 20, db: Session = Depends(get_db)):
    """Get the most recent signals processed by the pipeline."""
    rows = db.execute(
        text(
            "SELECT id, symbol, alert_type, direction, received_at, "
            "consensus_score, claude_decision, desk_id, status "
            "FROM signals ORDER BY received_at DESC LIMIT :limit"
        ),
        {"limit": limit},
    ).fetchall()
    return [dict(r._mapping) for r in rows]


@app.get("/api/stats/pipeline", tags=["MCP"])
async def get_pipeline_stats(db: Session = Depends(get_db)):
    """Get pipeline decision statistics for the last 24 hours."""
    rows = db.execute(
        text("""
            SELECT
                desk_id,
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE claude_decision = 'EXECUTE') as executes,
                COUNT(*) FILTER (WHERE claude_decision = 'SKIP') as skips,
                ROUND(AVG(consensus_score)::numeric, 1) as avg_consensus
            FROM signals
            WHERE received_at > NOW() - INTERVAL '24 hours'
            GROUP BY desk_id
        """)
    ).fetchall()
    return [dict(r._mapping) for r in rows]


@app.get("/api/stats/api-usage", tags=["MCP"])
async def get_api_usage():
    """Get TwelveData API usage estimate for the current minute."""
    try:
        from app.services.enricher import _enrichment_cache
        cache_size = len(_enrichment_cache) if hasattr(_enrichment_cache, '__len__') else 0
    except (ImportError, AttributeError):
        cache_size = 0
    return {
        "cache_entries": cache_size,
        "note": "Batch exit checker uses 1 credit/min, enrichment uses ~13/signal",
    }


# ─── Legacy EA stubs — silence 404 spam from external polling ───

@app.get("/api/trades/pending")
async def get_pending_trades():
    return []


@app.get("/api/trades/exits")
async def get_exit_commands():
    return []


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


# ─── MCP Server — expose read-only endpoints as Claude tools ───
# Must be mounted AFTER all routes are registered so fastapi-mcp discovers them.

from fastapi_mcp import FastApiMCP

mcp = FastApiMCP(
    app,
    name="OniQuant Oracle",
    description="OniQuant v6.1 Signal Generator — query signals, pipeline decisions, sim positions, and system health.",
    describe_all_responses=True,
    describe_full_response_schema=True,
    exclude_operations=[
        "webhook_path_auth",
        "webhook_body_auth",
        "kill_switch",
        "pause_desk",
        "resume_desk",
        "trigger_daily_report",
        "trigger_weekly_report",
        "trigger_monthly_report",
        "telegram_webhook",
        "start_backtest",
        "reset_sim_profile",
        "trigger_ohlcv_ingest",
        "batch_enrich",
        "get_pending_trades",
        "get_exit_commands",
        "test_alpha_strike",
        "test_synthetic_signal",
    ],
)
mcp.mount()
