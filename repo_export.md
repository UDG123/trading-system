# OniQuant v5.9 - Complete Repository

## `.gitignore`

```text
__pycache__/
*.py[cod]
*.so
.env
*.egg-info/
dist/
build/
.venv/
venv/
```

## `requirements.txt`

```text
fastapi==0.115.0
uvicorn[standard]==0.30.6
sqlalchemy==2.0.35
psycopg2-binary==2.9.9
pydantic==2.9.2
python-dotenv==1.0.1
httpx==0.27.2
scikit-learn==1.5.2
numpy==1.26.4
pyzmq==26.2.0
websockets==13.1
```

## `railway.json`

```json
{
  "$schema": "https://railway.com/railway.schema.json",
  "build": {
    "builder": "NIXPACKS"
  },
  "deploy": {
    "startCommand": "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}",
    "healthcheckPath": "/api/health",
    "healthcheckTimeout": 30,
    "restartPolicyType": "ON_FAILURE",
    "restartPolicyMaxRetries": 5
  }
}
```

## `Procfile`

```text
web: uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}
```

## `migrations/001_novaquant_schema_alignment.sql`

```sql
-- ============================================================
-- OniQuant → NovaQuant Schema Alignment Migration
-- Task 1: ALTER TABLE commands for ml_trade_logs + signals
-- ============================================================

-- 1a. Add webhook_latency_ms to ml_trade_logs
ALTER TABLE ml_trade_logs
ADD COLUMN IF NOT EXISTS webhook_latency_ms INTEGER DEFAULT NULL;

-- 1b. Add rationale JSONB column (NovaQuant schema — structured CTO reasoning)
ALTER TABLE ml_trade_logs
ADD COLUMN IF NOT EXISTS rationale JSONB DEFAULT '{}' NOT NULL;

-- 1c. Ensure status on trades table can accept VIRTUAL_OPEN
-- (The column is VARCHAR(20), VIRTUAL_OPEN is 12 chars — fits.
--  But let's widen to 30 to future-proof for all statuses.)
ALTER TABLE trades
ALTER COLUMN status TYPE VARCHAR(30);

-- 1d. Add webhook_latency_ms to signals table too (source of truth)
ALTER TABLE signals
ADD COLUMN IF NOT EXISTS webhook_latency_ms INTEGER DEFAULT NULL;

-- 1e. Add index for latency analysis
CREATE INDEX IF NOT EXISTS ix_signals_latency
ON signals (webhook_latency_ms)
WHERE webhook_latency_ms IS NOT NULL;

-- 1f. Add index for virtual trade queries
CREATE INDEX IF NOT EXISTS ix_trades_virtual
ON trades (status)
WHERE status IN ('VIRTUAL_OPEN', 'ONIAI_OPEN', 'EXECUTED', 'OPEN');

-- Verify
-- SELECT column_name, data_type, column_default
-- FROM information_schema.columns
-- WHERE table_name = 'ml_trade_logs' AND column_name IN ('webhook_latency_ms', 'rationale');
```

## `app/__init__.py`

```python
# Autonomous Institutional Trading System
```

## `app/main.py`

```python
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
    from app.services.price_service import PriceService
    global _price_service
    _price_service = PriceService()
    logger.info("PriceService ready (on-demand only, no continuous feed)")

    # Start 4-provider JIT simulator (Binance WS + TD + FH + FMP)
    from app.services.server_simulator import ServerSimulator
    _sim = ServerSimulator()
    _simulator_task = asyncio.create_task(_sim.run())
    logger.info("4-Provider JIT simulator started (Binance WS + TD + FH + FMP)")

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
```

## `app/config.py`

```python
"""
OniQuant v5.9 — Institutional Trading Floor Configuration
6 Desks · 8 Analysts · 1 CTO · $600K Under Management
"""
import os
from typing import Dict, List

# ─────────────────────────────────────────────────────────────
# ENVIRONMENT
# ─────────────────────────────────────────────────────────────
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgresql://postgres:postgres@localhost:5432/trading_system",
)
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "OniQuant_X9k7mP2w_2026")
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY", "")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")
FMP_API_KEY = os.getenv("FMP_API_KEY", "")
BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"
BINANCE_REST_URL = "https://api.binance.com"
BINANCE_WS_ENABLED = os.getenv("BINANCE_WS_ENABLED", "true").lower() in ("true", "1", "yes")
METAAPI_TOKEN = os.getenv("METAAPI_TOKEN", "")
METAAPI_ACCOUNT_ID = os.getenv("METAAPI_ACCOUNT_ID", "")
METAAPI_REGION = os.getenv("METAAPI_REGION", "new-york")

# ─────────────────────────────────────────────────────────────
# TELEGRAM
# ─────────────────────────────────────────────────────────────
TELEGRAM_DESK_CHANNELS = {
    "DESK1_SCALPER":   os.getenv("TG_DESK1", "-1003216826596"),
    "DESK2_INTRADAY":  os.getenv("TG_DESK2", "-1003789191641"),
    "DESK3_SWING":     os.getenv("TG_DESK3", "-1003813056839"),
    "DESK4_GOLD":      os.getenv("TG_DESK4", "-1003711906528"),
    "DESK5_ALTS":      os.getenv("TG_DESK5", "-1003868629189"),
    "DESK6_EQUITIES":  os.getenv("TG_DESK6", "-1003752836585"),
}
TELEGRAM_PORTFOLIO_CHAT = os.getenv("TG_PORTFOLIO", "-1003614474777")
TELEGRAM_SYSTEM_CHAT = os.getenv("TG_SYSTEM", "-1003710749613")

# ─────────────────────────────────────────────────────────────
# CAPITAL AND RISK
# ─────────────────────────────────────────────────────────────
ACCOUNTS = 6
CAPITAL_PER_ACCOUNT = 100_000
FIRM_CAPITAL = ACCOUNTS * CAPITAL_PER_ACCOUNT
PORTFOLIO_CAPITAL = 1_000_000

MAX_DAILY_LOSS_PCT = 5.0
MAX_TOTAL_LOSS_PCT = 10.0
MAX_DAILY_LOSS_PER_ACCOUNT = int(CAPITAL_PER_ACCOUNT * MAX_DAILY_LOSS_PCT / 100)
MAX_TOTAL_LOSS_PER_ACCOUNT = int(CAPITAL_PER_ACCOUNT * MAX_TOTAL_LOSS_PCT / 100)
FIRM_WIDE_DAILY_DRAWDOWN_HALT = 30_000

# Per-desk daily stop
DESK_DAILY_SOFT_STOP_PCT = 3.0
DESK_DAILY_HARD_STOP_PCT = 4.0

PIP_VALUES = {
    "FOREX": 10.0, "FOREX_JPY": 6.67, "GOLD": 10.0,
    "SILVER": 50.0, "WTI": 10.0, "COPPER": 2.50,
    "INDEX": 1.0, "CRYPTO": 1.0, "EQUITY": 1.0,
}
PIP_SIZES = {
    "FOREX": 0.0001, "FOREX_JPY": 0.01, "GOLD": 0.1,
    "SILVER": 0.01, "WTI": 0.01, "COPPER": 0.0001,
    "INDEX": 1.0, "CRYPTO": 1.0, "EQUITY": 0.01,
}
SYMBOL_ASSET_CLASS = {
    "XAUUSD": "GOLD", "XAGUSD": "SILVER",
    "WTIUSD": "WTI", "XCUUSD": "COMMODITY",
    "US30": "INDEX", "US100": "INDEX", "NAS100": "INDEX",
    "BTCUSD": "CRYPTO", "ETHUSD": "CRYPTO",
    "SOLUSD": "CRYPTO", "XRPUSD": "CRYPTO", "LINKUSD": "CRYPTO",
    "TSLA": "EQUITY", "AAPL": "EQUITY", "MSFT": "EQUITY",
    "NVDA": "EQUITY", "AMZN": "EQUITY", "META": "EQUITY",
    "GOOGL": "EQUITY", "NFLX": "EQUITY", "AMD": "EQUITY",
}


def get_pip_info(symbol: str):
    sym = symbol.upper()
    if "XAU" in sym:
        return PIP_SIZES["GOLD"], PIP_VALUES["GOLD"]
    elif "XAG" in sym:
        return PIP_SIZES["SILVER"], PIP_VALUES["SILVER"]
    elif "WTI" in sym:
        return PIP_SIZES["WTI"], PIP_VALUES["WTI"]
    elif "XCU" in sym or "COPPER" in sym:
        return PIP_SIZES["COPPER"], PIP_VALUES["COPPER"]
    elif "JPY" in sym:
        return PIP_SIZES["FOREX_JPY"], PIP_VALUES["FOREX_JPY"]
    elif any(x in sym for x in ["US30", "US100", "NAS"]):
        return PIP_SIZES["INDEX"], PIP_VALUES["INDEX"]
    elif any(x in sym for x in ["BTC", "ETH", "SOL", "XRP", "LINK"]):
        return PIP_SIZES["CRYPTO"], PIP_VALUES["CRYPTO"]
    elif any(x in sym for x in ["TSLA", "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "NFLX", "AMD"]):
        return PIP_SIZES["EQUITY"], PIP_VALUES["EQUITY"]
    else:
        return PIP_SIZES["FOREX"], PIP_VALUES["FOREX"]


def get_leverage(symbol: str, profile: str = "SRV_100") -> int:
    if profile == "SRV_30":
        return 30
    return 100


# ═══════════════════════════════════════════════════════════════
# ATR SETTINGS PER DESK
# ═══════════════════════════════════════════════════════════════
ATR_SETTINGS = {
    "DESK1_SCALPER":     {"atr_period": 10, "sl_mult": 1.5, "tp1_mult": 3.0, "tp2_mult": 4.5, "min_rr": 1.5},
    "DESK2_INTRADAY":    {"atr_period": 14, "sl_mult": 2.0, "tp1_mult": 4.0, "tp2_mult": 6.0, "min_rr": 1.5},
    "DESK3_SWING":       {"atr_period": 14, "sl_mult": 2.0, "tp1_mult": 4.0, "tp2_mult": 6.0, "min_rr": 2.0},
    "DESK4_GOLD":        {"atr_period": 14, "sl_mult": 2.0, "tp1_mult": 4.0, "tp2_mult": 6.0, "min_rr": 1.5},
    "DESK4_GOLD_SCALP":  {"atr_period": 10, "sl_mult": 2.0, "tp1_mult": 4.0, "tp2_mult": 6.0, "min_rr": 1.5},
    "DESK4_GOLD_INTRA":  {"atr_period": 14, "sl_mult": 2.5, "tp1_mult": 5.0, "tp2_mult": 7.5, "min_rr": 1.5},
    "DESK4_GOLD_SWING":  {"atr_period": 14, "sl_mult": 2.0, "tp1_mult": 4.0, "tp2_mult": 6.0, "min_rr": 2.0},
    "DESK5_ALTS":        {"atr_period": 14, "sl_mult": 2.0, "tp1_mult": 4.0, "tp2_mult": 6.0, "min_rr": 1.5},
    "DESK5_ALTS_CRYPTO": {"atr_period": 10, "sl_mult": 2.5, "tp1_mult": 5.0, "tp2_mult": 7.5, "min_rr": 1.5},
    "DESK6_EQUITIES":    {"atr_period": 14, "sl_mult": 2.0, "tp1_mult": 4.0, "tp2_mult": 6.0, "min_rr": 1.5},
}


def get_atr_settings(desk_id: str, symbol: str = "", timeframe: str = "") -> Dict:
    sym = symbol.upper()
    if desk_id == "DESK4_GOLD":
        tf = timeframe.upper() if timeframe else ""
        if "5" in tf or "1M" == tf:
            return ATR_SETTINGS["DESK4_GOLD_SCALP"]
        elif "15" in tf or tf == "1H" or tf == "60":
            return ATR_SETTINGS["DESK4_GOLD_INTRA"]
        elif "4" in tf or "D" in tf or "W" in tf:
            return ATR_SETTINGS["DESK4_GOLD_SWING"]
        return ATR_SETTINGS["DESK4_GOLD"]
    if desk_id == "DESK5_ALTS" and any(c in sym for c in ["BTC", "ETH", "SOL", "XRP", "LINK"]):
        return ATR_SETTINGS["DESK5_ALTS_CRYPTO"]
    return ATR_SETTINGS.get(desk_id, ATR_SETTINGS["DESK2_INTRADAY"])


# ═══════════════════════════════════════════════════════════════
# VIX REGIME THRESHOLDS
# ═══════════════════════════════════════════════════════════════
VIX_REGIMES = {
    "NORMAL_MAX": 20,
    "ELEVATED": 25,
    "HIGH": 30,
    "EXTREME": 35,
}

# ═══════════════════════════════════════════════════════════════
# PROFILES
# ═══════════════════════════════════════════════════════════════
SRV_100 = {
    "name": "Server 1:100", "tag": "🟦", "leverage": 100, "capital_per_desk": 100_000,
    "DESK1_SCALPER":  {"risk_pct": 0.50, "lot_method": "DYNAMIC", "max_trades_day": 20, "max_open": 5, "max_daily_loss_pct": 4.0, "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_1H", 5: "CLOSE_DESK"}, "trailing_stop_pips": 10},
    "DESK2_INTRADAY": {"risk_pct": 0.75, "lot_method": "DYNAMIC", "max_lot": 10.0, "max_trades_day": 10, "max_open": 5, "max_daily_loss_pct": 4.0, "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_2H", 5: "CLOSE_DESK"}, "trailing_stop_pips": 30},
    "DESK3_SWING":    {"risk_pct": 1.0,  "lot_method": "DYNAMIC", "max_lot": 10.0, "max_trades_day": 6, "max_open": 5, "max_daily_loss_pct": 4.0, "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_2H", 5: "CLOSE_DESK"}, "trailing_stop_pips": 80},
    "DESK4_GOLD":     {"sub_strategies": {"gold_scalp": {"risk_pct": 0.40, "max_trades": 8}, "gold_intra": {"risk_pct": 0.60, "max_trades": 6}, "gold_swing": {"risk_pct": 0.80, "max_trades": 3}}, "lot_method": "DYNAMIC", "max_lot": 5.0, "max_open": 6, "max_daily_loss_pct": 4.0, "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_2H", 5: "CLOSE_DESK"}},
    "DESK5_ALTS":     {"risk_pct": 0.75, "risk_pct_crypto": 0.50, "lot_method": "DYNAMIC", "max_lot": 10.0, "max_trades_day": 10, "max_open": 5, "max_daily_loss_pct": 4.0, "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_2H", 5: "CLOSE_DESK"}, "trailing_stop_pips": {"indices": 15, "crypto": 50}},
    "DESK6_EQUITIES": {"risk_pct": 0.75, "lot_method": "DYNAMIC", "max_lot": 10.0, "max_trades_day": 6, "max_open": 4, "max_daily_loss_pct": 4.0, "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_2H", 5: "CLOSE_DESK"}, "trailing_stop_pips": 25},
}

SRV_30 = {
    "name": "Server 1:30", "tag": "🟧", "leverage": 30, "capital_per_desk": 100_000,
    "DESK1_SCALPER":  {"risk_pct": 0.40, "lot_method": "DYNAMIC", "max_trades_day": 8, "max_open": 2, "max_daily_loss_pct": 4.0, "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_2H", 5: "CLOSE_DESK"}, "trailing_stop_pips": 10},
    "DESK2_INTRADAY": {"risk_pct": 0.60, "lot_method": "DYNAMIC", "max_lot": 3.0, "max_trades_day": 5, "max_open": 3, "max_daily_loss_pct": 4.0, "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_2H", 5: "CLOSE_DESK"}, "trailing_stop_pips": 30},
    "DESK3_SWING":    {"risk_pct": 0.75, "lot_method": "DYNAMIC", "max_lot": 3.0, "max_trades_day": 3, "max_open": 3, "max_daily_loss_pct": 4.0, "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_2H", 5: "CLOSE_DESK"}, "trailing_stop_pips": 80},
    "DESK4_GOLD":     {"sub_strategies": {"gold_scalp": {"risk_pct": 0.30, "max_trades": 4}, "gold_intra": {"risk_pct": 0.50, "max_trades": 3}, "gold_swing": {"risk_pct": 0.60, "max_trades": 1}}, "lot_method": "DYNAMIC", "max_lot": 2.0, "max_open": 2, "max_daily_loss_pct": 4.0, "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_2H", 5: "CLOSE_DESK"}},
    "DESK5_ALTS":     {"risk_pct": 0.50, "risk_pct_crypto": 0.40, "lot_method": "DYNAMIC", "max_lot": 3.0, "max_trades_day": 4, "max_open": 2, "max_daily_loss_pct": 4.0, "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_2H", 5: "CLOSE_DESK"}, "trailing_stop_pips": {"indices": 15, "crypto": 50}},
    "DESK6_EQUITIES": {"risk_pct": 0.50, "lot_method": "DYNAMIC", "max_lot": 3.0, "max_trades_day": 3, "max_open": 2, "max_daily_loss_pct": 4.0, "consecutive_loss_rules": {2: 0.75, 3: 0.50, 4: "PAUSE_2H", 5: "CLOSE_DESK"}, "trailing_stop_pips": 25},
}

MT5_1M = {
    "name": "MT5 $1M", "tag": "📟", "leverage": 100, "capital": 1_000_000,
    "max_daily_loss": 50_000, "max_total_loss": 100_000,
    "capital_per_desk": {
        "DESK1_SCALPER": 120_000, "DESK2_INTRADAY": 200_000, "DESK3_SWING": 200_000,
        "DESK4_GOLD": 180_000, "DESK5_ALTS": 150_000, "DESK6_EQUITIES": 150_000,
    },
    "DESK1_SCALPER":  {"risk_pct": 0.60, "lot_method": "DYNAMIC", "max_trades_day": 12, "max_open": 4},
    "DESK2_INTRADAY": {"risk_pct": 1.0,  "lot_method": "DYNAMIC", "max_trades_day": 8,  "max_open": 4},
    "DESK3_SWING":    {"risk_pct": 1.25, "lot_method": "DYNAMIC", "max_trades_day": 4,  "max_open": 4},
    "DESK4_GOLD":     {"sub_strategies": {"gold_scalp": {"risk_pct": 0.50, "max_trades": 8}, "gold_intra": {"risk_pct": 0.80, "max_trades": 5}, "gold_swing": {"risk_pct": 1.0, "max_trades": 3}}, "lot_method": "DYNAMIC", "max_open": 5},
    "DESK5_ALTS":     {"risk_pct": 1.0, "risk_pct_crypto": 0.60, "lot_method": "DYNAMIC", "max_trades_day": 8, "max_open": 4},
    "DESK6_EQUITIES": {"risk_pct": 1.0, "lot_method": "DYNAMIC", "max_trades_day": 6, "max_open": 4},
}

PORTFOLIO_CAPITAL_PER_DESK = MT5_1M.get("capital_per_desk", {})


def calculate_lot_size(desk_id: str, symbol: str, risk_pct: float, sl_pips: float, account_capital: float = None, profile: str = "SRV_100") -> float:
    if account_capital is None:
        account_capital = CAPITAL_PER_ACCOUNT
    pip_size, pip_value = get_pip_info(symbol)
    prof = SRV_100 if profile == "SRV_100" else (SRV_30 if profile == "SRV_30" else MT5_1M)
    desk_profile = prof.get(desk_id, {})
    if desk_profile.get("lot_method") == "FIXED" and desk_profile.get("fixed_lot"):
        return desk_profile["fixed_lot"]
    if sl_pips <= 0 or pip_value <= 0:
        return 0.01
    risk_dollars = account_capital * (risk_pct / 100)
    lot_size = risk_dollars / (sl_pips * pip_value)
    max_lot = desk_profile.get("max_lot", 10.0)
    lot_size = min(lot_size, max_lot)
    lot_size = max(lot_size, 0.01)
    return round(lot_size, 2)


CONSECUTIVE_LOSS_RULES = {2: 0.75, 3: 0.50, 4: "PAUSE_1H", 5: "CLOSE_DESK"}


# ═══════════════════════════════════════════════════════════════
# CONSENSUS SCORING v5.9
# ═══════════════════════════════════════════════════════════════
SCORE_THRESHOLDS = {
    "HIGH": 8,    # 100% size
    "MEDIUM": 5,  # 60% size
    "LOW": 3,     # 30% size
}

SCORE_WEIGHTS = {
    "entry_trigger_normal": 1,
    "entry_trigger_plus": 2,
    "confirmation_turn_plus": 2,
    "defined_sl_tp": 1,
    "bias_match": 3,
    "setup_match": 2,
    "conflicting_htf": -4,
    "kill_zone_overlap": 3,
    "kill_zone_single": 1,
    "ml_confirm_per_tf": 1,
    "correlation_confirm": 2,
    "liquidity_sweep": 3,
    "rsi_aligned": 2,
    "rsi_counter": -2,
    "adx_trending": 1,
    "adx_ranging": -1,
    "vix_elevated": -2,
    "vix_gold_bullish": 1,
    "dxy_headwind": -1,
    "multi_analyst_agree": 2,
    # Legacy keys (backwards compat)
    "bullish_bearish_plus": 2,
}


# ═══════════════════════════════════════════════════════════════
# DESK DEFINITIONS — FULL COVERAGE (40 symbols · 54 alerts)
# All desks: ML Classifier 34 + Overlay Filter + Once Per Bar Close
# OniAI virtual trades broadcast to Telegram for any CTO-approved
# signal that can't execute due to max_open limits.
# ═══════════════════════════════════════════════════════════════
DESKS: Dict[str, dict] = {
    "DESK1_SCALPER": {
        "name": "FX Scalper",
        "style": "Scalper",
        "symbols": ["EURUSD", "USDJPY", "GBPUSD", "USDCHF", "AUDUSD", "NZDUSD", "EURCHF"],
        "timeframes": {"bias": "15M", "confirmation": "5M", "entry": "1M"},
        "luxalgo_preset": "Scalper",
        "luxalgo_filter": "Smart Trail",
        "ml_classifier": 34,
        "sensitivity": 7,
        "trailing_stop_pips": 10,
        "max_trades_day": 20,
        "max_simultaneous": 5,
        "max_hold_hours": 1.5,
        "risk_pct": 0.5,
        "sessions": ["LONDON_OPEN", "NY_OPEN"],
        "alerts": [
            "bullish_confirmation", "bearish_confirmation",
            "bullish_confirmation_plus", "bearish_confirmation_plus",
            "bullish_exit", "bearish_exit", "take_profit", "stop_loss",
        ],
    },
    "DESK2_INTRADAY": {
        "name": "FX Intraday",
        "style": "Intraday",
        "symbols": [
            "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD",
            "NZDUSD", "EURGBP", "GBPCAD", "AUDCAD", "CHFJPY", "EURNZD",
        ],
        "timeframes": {"bias": "4H", "confirmation": "1H", "entry": "15M"},
        "luxalgo_preset": "Trend Trader",
        "luxalgo_filter": "Trend Catcher",
        "ml_classifier": 34,
        "sensitivity": 14,
        "trailing_stop_pips": 30,
        "max_trades_day": 10,
        "max_simultaneous": 5,
        "max_hold_hours": 8,
        "risk_pct": 0.75,
        "sessions": ["LONDON", "NEW_YORK"],
        "alerts": [
            "bullish_confirmation", "bearish_confirmation",
            "bullish_plus", "bearish_plus",
            "bullish_confirmation_plus", "bearish_confirmation_plus",
            "bullish_exit", "bearish_exit",
            "take_profit", "stop_loss", "confirmation_turn_plus",
        ],
    },
    "DESK3_SWING": {
        "name": "Swing Trader",
        "style": "Swing",
        "symbols": [
            "EURUSD", "GBPUSD", "AUDUSD", "USDCAD", "NZDUSD",
            "EURJPY", "GBPJPY", "AUDJPY", "EURAUD", "GBPAUD",
            "CADJPY", "NZDJPY", "GBPNZD",
        ],
        "timeframes": {"bias": "W", "confirmation": "D", "entry": "4H"},
        "luxalgo_preset": "Swing Trader",
        "luxalgo_filter": "Trend Catcher",
        "ml_classifier": 34,
        "sensitivity": 14,
        "trailing_stop_pips": 80,
        "max_simultaneous": 5,
        "max_trades_day": 6,
        "max_hold_hours": 120,
        "risk_pct": 1.0,
        "sessions": ["ALL"],
        "close_friday_utc": 20,
        "alerts": [
            "bullish_confirmation", "bearish_confirmation",
            "bullish_plus", "bearish_plus",
            "bullish_confirmation_plus", "bearish_confirmation_plus",
            "confirmation_turn_plus",
            "bullish_exit", "bearish_exit", "take_profit", "stop_loss",
        ],
    },
    "DESK4_GOLD": {
        "name": "Gold Specialist",
        "style": "Multi-TF Gold + Commodities",
        "symbols": ["XAUUSD", "XAGUSD", "WTIUSD"],
        "timeframes": {"bias": "D", "confirmation": "4H,1H", "entry": "5M,15M,1H"},
        "luxalgo_preset": "Trend Trader + Neo Cloud",
        "luxalgo_filter": "Smart Trail",
        "ml_classifier": 34,
        "sensitivity": 12,
        "trailing_stop_pips": 30,
        "max_trades_day": 15,
        "max_simultaneous": 6,
        "max_hold_hours": 120,
        "risk_pct": 0.6,
        "sessions": ["LONDON", "NEW_YORK", "LONDON_NY_OVERLAP"],
        "active_window": "07:00-21:00 UTC",
        "dxy_correlation_threshold": -0.70,
        "vix_bullish_bias_above": 25,
        "alerts": [
            "bullish_confirmation", "bearish_confirmation",
            "bullish_plus", "bearish_plus",
            "bullish_confirmation_plus", "bearish_confirmation_plus",
            "bullish_exit", "bearish_exit", "take_profit", "stop_loss",
        ],
        "sub_strategies": {
            "gold_scalp": {
                "timeframe": "5M", "sensitivity": 7, "filter": "Smart Trail",
                "risk_pct": 0.4, "max_trades": 8, "max_hold_hours": 1.5,
                "trailing_stop_pips": 15, "sessions": ["LONDON_NY_OVERLAP"],
                "symbols": ["XAUUSD"],
                "alerts": ["bullish_confirmation", "bearish_confirmation", "bullish_confirmation_plus", "bearish_confirmation_plus"],
            },
            "gold_intra": {
                "timeframe": "15M,1H", "sensitivity": 12, "filter": "Smart Trail",
                "risk_pct": 0.6, "max_trades": 6, "max_hold_hours": 6,
                "trailing_stop_pips": 30, "sessions": ["LONDON", "NEW_YORK", "LONDON_NY_OVERLAP"],
                "symbols": ["XAUUSD"],
                "alerts": ["bullish_confirmation", "bearish_confirmation", "bullish_plus", "bearish_plus", "bullish_confirmation_plus", "bearish_confirmation_plus"],
            },
            "gold_swing": {
                "timeframe": "4H", "sensitivity": 14, "filter": "Trend Catcher",
                "risk_pct": 0.8, "max_trades": 3, "max_hold_hours": 120,
                "trailing_stop_pips": 50, "sessions": ["LONDON", "NEW_YORK"],
                "symbols": ["XAUUSD", "XAGUSD", "WTIUSD"],
                "alerts": ["bullish_confirmation", "bearish_confirmation", "bullish_plus", "bearish_plus", "bullish_confirmation_plus", "bearish_confirmation_plus"],
            },
        },
    },
    "DESK5_ALTS": {
        "name": "Momentum",
        "style": "Index + Crypto + Commodity Trend",
        "symbols": ["NAS100", "US30", "BTCUSD", "ETHUSD", "SOLUSD", "XRPUSD", "LINKUSD", "WTIUSD"],
        "timeframes": {"bias": "D", "confirmation": "4H", "entry": "1H"},
        "luxalgo_preset": "Trend Trader + Trend Strength",
        "luxalgo_filter": "Smart Trail",
        "ml_classifier": 34,
        "sensitivity": 14,
        "sensitivity_crypto": 16,
        "trailing_stop_pips": {"indices": 15, "crypto": 50, "commodity": 30},
        "max_trades_day": 10,
        "max_simultaneous": 5,
        "max_hold_hours": 24,
        "max_hold_hours_indices": 8,
        "risk_pct": 0.75,
        "risk_pct_crypto": 0.50,
        "sessions": ["US_MARKET", "WEEKDAY_CRYPTO"],
        "no_weekend_crypto": True,
        "vix_halt_above": 30,
        "alerts": [
            "bullish_confirmation", "bearish_confirmation",
            "bullish_plus", "bearish_plus",
            "bullish_confirmation_plus", "bearish_confirmation_plus",
            "bullish_exit", "bearish_exit", "take_profit", "stop_loss",
        ],
    },
    "DESK6_EQUITIES": {
        "name": "Equities",
        "style": "Stock Trend",
        "symbols": ["NVDA", "AAPL", "TSLA", "MSFT", "AMZN", "META", "GOOGL", "NFLX", "AMD"],
        "timeframes": {"bias": "D", "confirmation": "4H", "entry": "1H"},
        "luxalgo_preset": "Trend Trader",
        "luxalgo_filter": "Smart Trail",
        "ml_classifier": 34,
        "sensitivity": 12,
        "trailing_stop_pips": 25,
        "max_trades_day": 6,
        "max_simultaneous": 4,
        "max_hold_hours": 7,
        "risk_pct": 0.75,
        "sessions": ["US_MARKET"],
        "active_window": "13:30-20:00 UTC",
        "vix_halt_above": 30,
        "require_vwap_alignment": True,
        "alerts": [
            "bullish_confirmation", "bearish_confirmation",
            "bullish_plus", "bearish_plus",
            "bullish_confirmation_plus", "bearish_confirmation_plus",
            "contrarian_bullish", "contrarian_bearish",
            "bullish_exit", "bearish_exit", "take_profit", "stop_loss",
        ],
    },
}


VALID_ALERT_TYPES = [
    "bullish_confirmation", "bearish_confirmation",
    "bullish_plus", "bearish_plus",
    "bullish_confirmation_plus", "bearish_confirmation_plus",
    "bullish_exit", "bearish_exit",
    "confirmation_turn_bullish", "confirmation_turn_bearish", "confirmation_turn_plus",
    "contrarian_bullish", "contrarian_bearish",
    "take_profit", "stop_loss", "smart_trail_cross",
    "smc_structure", "smc_bullish_bos", "smc_bearish_bos",
    "smc_bullish_choch", "smc_bearish_choch",
    "smc_bullish_fvg", "smc_bearish_fvg",
    "smc_equal_highs", "smc_equal_lows",
    "smc_bullish_ob_break", "smc_bearish_ob_break",
]


SESSION_WINDOWS = {
    "LONDON":            {"start": 7,  "end": 16},
    "LONDON_OPEN":       {"start": 7,  "end": 10},
    "NEW_YORK":          {"start": 12, "end": 21},
    "NY_OPEN":           {"start": 12, "end": 15},
    "LONDON_NY_OVERLAP": {"start": 12, "end": 16},
    "US_MARKET":         {"start": 13, "end": 20},
    "MARKET_HOURS_INDICES": {"start": 13, "end": 20},
    "WEEKDAY_CRYPTO":    {"start": 13, "end": 21},
    "24_7_CRYPTO":       {"start": 0,  "end": 24},
    "SYDNEY":            {"start": 21, "end": 6},
    "TOKYO":             {"start": 23, "end": 8},
    "ALL":               {"start": 0,  "end": 24},
}


CORRELATION_GROUPS = {
    "USD_WEAKNESS":    {"symbols": ["EURUSD", "GBPUSD", "AUDUSD", "NZDUSD"], "long_correlated": True},
    "USD_STRENGTH":    {"symbols": ["USDJPY", "USDCHF", "USDCAD"], "long_correlated": True},
    "JPY_CROSSES":     {"symbols": ["EURJPY", "GBPJPY", "AUDJPY", "CADJPY", "NZDJPY", "CHFJPY"], "long_correlated": True},
    "EUR_CROSSES":     {"symbols": ["EURAUD", "EURGBP", "EURCHF", "EURNZD"], "long_correlated": True},
    "GBP_CROSSES":     {"symbols": ["GBPAUD", "GBPCAD", "GBPNZD"], "long_correlated": True},
    "AUD_CROSSES":     {"symbols": ["AUDCAD", "AUDUSD", "NZDUSD"], "long_correlated": True},
    "TECH_STOCKS":     {"symbols": ["NVDA", "AAPL", "TSLA", "MSFT", "AMZN", "META", "GOOGL", "NFLX", "AMD"], "long_correlated": True},
    "INDICES_CRYPTO":  {"symbols": ["NAS100", "US30", "BTCUSD"], "long_correlated": True},
    "CRYPTO":          {"symbols": ["BTCUSD", "ETHUSD", "SOLUSD", "XRPUSD", "LINKUSD"], "long_correlated": True},
    "PRECIOUS_METALS": {"symbols": ["XAUUSD", "XAGUSD"], "long_correlated": True},
    "ENERGY":          {"symbols": ["WTIUSD"], "long_correlated": True},
}

MAX_CORRELATED_POSITIONS = 2

SYMBOL_ALIASES = {
    "FX:EURUSD": "EURUSD", "FX:USDJPY": "USDJPY", "FX:GBPUSD": "GBPUSD",
    "FX:USDCHF": "USDCHF", "FX:AUDUSD": "AUDUSD", "FX:USDCAD": "USDCAD",
    "FX:NZDUSD": "NZDUSD", "FX:EURJPY": "EURJPY", "FX:GBPJPY": "GBPJPY",
    "FX:AUDJPY": "AUDJPY", "FX:EURGBP": "EURGBP", "FX:EURAUD": "EURAUD",
    "FX:GBPAUD": "GBPAUD", "FX:EURCHF": "EURCHF", "FX:CADJPY": "CADJPY",
    "FX:NZDJPY": "NZDJPY", "FX:GBPCAD": "GBPCAD", "FX:AUDCAD": "AUDCAD",
    "FX:AUDNZD": "AUDNZD", "FX:CHFJPY": "CHFJPY", "FX:EURNZD": "EURNZD",
    "FX:GBPNZD": "GBPNZD",
    "COMEX:HG1!": "XCUUSD", "TVC:COPPER": "XCUUSD", "OANDA:XCUUSD": "XCUUSD", "FX:COPPER": "XCUUSD",
    "COINBASE:SOLUSD": "SOLUSD", "BINANCE:SOLUSDT": "SOLUSD",
    "COINBASE:XRPUSD": "XRPUSD", "BINANCE:XRPUSDT": "XRPUSD",
    "COINBASE:LINKUSD": "LINKUSD", "BINANCE:LINKUSDT": "LINKUSD",
    "OANDA:XAUUSD": "XAUUSD", "OANDA:XAGUSD": "XAGUSD", "FXOPEN:XAGUSD": "XAGUSD",
    "TVC:USOIL": "WTIUSD", "NYMEX:CL1!": "WTIUSD", "OANDA:WTICOUSD": "WTIUSD",
    "FX:USOIL": "WTIUSD", "CAPITALCOM:OIL_CRUDE": "WTIUSD",
    "TVC:DJI": "US30", "TVC:NDQ": "US100", "NASDAQ:NDX": "NAS100",
    "OANDA:NAS100USD": "NAS100", "PEPPERSTONE:NAS100": "NAS100",
    "COINBASE:BTCUSD": "BTCUSD", "COINBASE:ETHUSD": "ETHUSD",
    "NASDAQ:TSLA": "TSLA", "NASDAQ:AAPL": "AAPL", "NASDAQ:MSFT": "MSFT",
    "NASDAQ:NVDA": "NVDA", "NASDAQ:AMZN": "AMZN", "NASDAQ:META": "META",
    "NASDAQ:GOOGL": "GOOGL", "NASDAQ:NFLX": "NFLX", "NASDAQ:AMD": "AMD",
}


def get_desk_for_symbol(symbol: str) -> List[str]:
    normalized = SYMBOL_ALIASES.get(symbol, symbol.upper().replace("/", ""))
    return [desk_id for desk_id, desk in DESKS.items() if normalized in desk["symbols"]]
```

## `app/database.py`

```python
"""
PostgreSQL Database Connection via SQLAlchemy
"""
import logging
from sqlalchemy import create_engine, text
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import sessionmaker

from app.config import DATABASE_URL

logger = logging.getLogger("TradingSystem.DB")

engine = create_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    pool_recycle=300,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    """Dependency: yield a DB session, close on exit."""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def check_db_connection() -> bool:
    """Verify database is reachable."""
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return True
    except Exception as e:
        logger.error(f"DB connection check failed: {e}")
        return False
```

## `app/schemas.py`

```python
"""
Pydantic Schemas - Request/Response validation for webhook alerts.
"""
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field, field_validator


class TradingViewAlert(BaseModel):
    """
    Expected JSON payload from TradingView webhook.
    Configure your TradingView alert message as JSON matching this schema.

    Example TradingView alert message:
    {
        "secret": "your-webhook-secret",
        "symbol": "{{ticker}}",
        "exchange": "{{exchange}}",
        "timeframe": "{{interval}}",
        "alert_type": "bullish_confirmation",
        "price": {{close}},
        "time": "{{time}}",
        "tp1": {{plot("TP1")}},
        "tp2": {{plot("TP2")}},
        "sl1": {{plot("SL1")}},
        "sl2": {{plot("SL2")}},
        "smart_trail": {{plot("Smart Trail")}},
        "bar_index": {{bar_index}},
        "volume": {{volume}}
    }
    """
    # Authentication
    secret: str = Field(..., description="Webhook secret for authentication")

    # Core signal data
    symbol: str = Field(..., description="Ticker symbol from TradingView")
    exchange: Optional[str] = Field(None, description="Exchange identifier")
    timeframe: str = Field("60", description="Chart timeframe")
    alert_type: str = Field("unknown", description="LuxAlgo alert type")
    price: float = Field(0, ge=0, description="Current price at alert")
    time: Optional[str] = Field(None, description="Alert timestamp from TV")

    # LuxAlgo levels
    tp1: Optional[float] = Field(None, description="Take Profit 1")
    tp2: Optional[float] = Field(None, description="Take Profit 2")
    sl1: Optional[float] = Field(None, description="Stop Loss 1")
    sl2: Optional[float] = Field(None, description="Stop Loss 2")
    smart_trail: Optional[float] = Field(None, description="Smart Trail level")

    # Extra context
    bar_index: Optional[int] = Field(None, description="Bar index")
    volume: Optional[float] = Field(None, description="Volume at alert")

    # MSE (Master Signal Engine) enrichment — pre-computed by Pine Script
    desk: Optional[str] = Field(None, description="Desk assignment from MSE auto-detect")
    mse: Optional[dict] = Field(None, description="Pre-computed enrichment from OniQuant MSE")

    @field_validator("alert_type")
    @classmethod
    def normalize_alert_type(cls, v: str) -> str:
        """Normalize alert type to lowercase snake_case.
        Handles both direct alert_type codes and LuxAlgo {default} messages.
        """
        # LuxAlgo {default} message → internal alert_type mapping
        luxalgo_message_map = {
            "bullish confirmation signal": "bullish_confirmation",
            "bearish confirmation signal": "bearish_confirmation",
            "bullish+ confirmation signal": "bullish_plus",
            "bearish+ confirmation signal": "bearish_plus",
            "strong bullish confirmation signal": "bullish_plus",
            "strong bearish confirmation signal": "bearish_plus",
            "bullish confirmation+ signal": "bullish_confirmation_plus",
            "bearish confirmation+ signal": "bearish_confirmation_plus",
            "bullish exit signal": "bullish_exit",
            "bearish exit signal": "bearish_exit",
            "bullish contrarian signal": "contrarian_bullish",
            "bearish contrarian signal": "contrarian_bearish",
            "confirmation turn bullish": "confirmation_turn_bullish",
            "confirmation turn bearish": "confirmation_turn_bearish",
            "confirmation turn plus": "confirmation_turn_plus",
            # Turn variants without "Confirmation" prefix (LuxAlgo v7+)
            "bullish turn +": "confirmation_turn_bullish",
            "bearish turn +": "confirmation_turn_bearish",
            "bullish turn": "confirmation_turn_bullish",
            "bearish turn": "confirmation_turn_bearish",
            "bullish confirmation+": "bullish_confirmation_plus",
            "bearish confirmation+": "bearish_confirmation_plus",
            "take profit": "take_profit",
            "stop loss": "stop_loss",
            "smart trail cross": "smart_trail_cross",
            "smart trail crossed": "smart_trail_cross",
            # Confirmation exit variants (LuxAlgo v7+)
            "confirmation bullish exit": "bullish_exit",
            "confirmation bearish exit": "bearish_exit",
            "confirmation bullish exit signal": "bullish_exit",
            "confirmation bearish exit signal": "bearish_exit",
            # SMC structure alerts
            "internal bullish bos formed": "smc_bullish_bos",
            "bearish bos formed": "smc_bearish_bos",
            "internal bullish choch formed": "smc_bullish_choch",
            "bearish choch formed": "smc_bearish_choch",
            "bullish fvg formed": "smc_bullish_fvg",
            "bearish fvg formed": "smc_bearish_fvg",
            "equal highs detected": "smc_equal_highs",
            "equal lows detected": "smc_equal_lows",
            "price broke bullish internal ob": "smc_bullish_ob_break",
            "price broke bearish internal ob": "smc_bearish_ob_break",
            "price broke bullish swing ob": "smc_bullish_ob_break",
            "price broke bearish swing ob": "smc_bearish_ob_break",
        }

        cleaned = v.strip().lower()

        # Check if it's a LuxAlgo default message
        if cleaned in luxalgo_message_map:
            return luxalgo_message_map[cleaned]

        # Regex: "TP1 2327.589 Reached" or "SL1 80.932 Reached" → take_profit / stop_loss
        import re
        tp_match = re.match(r'^tp\d?\s+[\d.,]+\s+reached$', cleaned)
        if tp_match:
            return "take_profit"
        sl_match = re.match(r'^sl\d?\s+[\d.,]+\s+reached$', cleaned)
        if sl_match:
            return "stop_loss"
        # "Smart Trail X.XX Reached"
        if cleaned.startswith("smart trail") and "reached" in cleaned:
            return "smart_trail_cross"

        # Otherwise normalize to snake_case, truncate to 50 chars for DB safety
        result = cleaned.replace(" ", "_").replace("-", "_").replace("+", "_plus")
        return result[:50]

    @field_validator("symbol")
    @classmethod
    def clean_symbol(cls, v: str) -> str:
        return v.strip().upper()

    @field_validator("timeframe")
    @classmethod
    def clean_timeframe(cls, v: str) -> str:
        return v.strip().upper()


class SignalResponse(BaseModel):
    """Response returned after processing a webhook alert."""
    status: str  # "accepted" | "rejected" | "error"
    signal_id: Optional[int] = None
    symbol: Optional[str] = None
    alert_type: Optional[str] = None
    desks_matched: Optional[List[str]] = None
    is_valid: bool = False
    validation_errors: Optional[List[str]] = None
    message: str = ""


class HealthResponse(BaseModel):
    status: str
    database: str
    uptime_seconds: float
    signals_today: int
    version: str = "5.9.0"


class DeskSummary(BaseModel):
    desk_id: str
    name: str
    is_active: bool
    trades_today: int
    daily_pnl: float
    consecutive_losses: int
    size_modifier: float
    open_positions: int


class DashboardResponse(BaseModel):
    firm_status: str
    total_signals_today: int
    total_trades_today: int
    total_daily_pnl: float
    desks: List[DeskSummary]
```

## `app/models/__init__.py`

```python
from app.models.signal import Signal
from app.models.trade import Trade
from app.models.desk_state import DeskState
from app.models.ml_trade_log import MLTradeLog

__all__ = ["Signal", "Trade", "DeskState", "MLTradeLog"]
```

## `app/models/signal.py`

```python
"""
Signal Model - Every incoming LuxAlgo alert is logged here.
This is the primary audit trail for the entire system.
"""
from datetime import datetime, timezone
from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Text, Boolean, JSON,
    Index,
)
from app.database import Base


class Signal(Base):
    __tablename__ = "signals"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # ── Source identification ──
    received_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
        index=True,
    )
    source = Column(String(50), default="tradingview", nullable=False)

    # ── Alert payload (raw from TradingView) ──
    raw_payload = Column(Text, nullable=False)  # full JSON string for audit

    # ── Parsed fields ──
    symbol = Column(String(20), nullable=False, index=True)
    symbol_normalized = Column(String(20), nullable=False, index=True)
    timeframe = Column(String(10), nullable=False)
    alert_type = Column(String(50), nullable=False, index=True)
    direction = Column(String(10), nullable=True)  # LONG / SHORT / EXIT / null
    price = Column(Float, nullable=True)
    tp1 = Column(Float, nullable=True)
    tp2 = Column(Float, nullable=True)
    sl1 = Column(Float, nullable=True)
    sl2 = Column(Float, nullable=True)
    smart_trail = Column(Float, nullable=True)

    # ── Desk routing ──
    desk_id = Column(String(30), nullable=True, index=True)
    desks_matched = Column(JSON, nullable=True)  # list of desk IDs this applies to

    # ── Validation ──
    is_valid = Column(Boolean, default=False, nullable=False)
    validation_errors = Column(JSON, nullable=True)  # list of error strings

    # ── Processing status ──
    status = Column(
        String(20),
        default="RECEIVED",
        nullable=False,
        index=True,
    )
    # RECEIVED → VALIDATED → ENRICHED → SCORED → DECIDED → EXECUTED → REJECTED

    # ── ML scoring (Phase 2) ──
    ml_score = Column(Float, nullable=True)
    ml_method = Column(String(20), nullable=True)  # "model" or "rule_based"

    # ── Enrichment data (Phase 2) ──
    enrichment_data = Column(JSON, nullable=True)  # full enrichment snapshot

    # ── Claude decision (Phase 2) ──
    claude_decision = Column(String(20), nullable=True)  # EXECUTE / SKIP / REDUCE
    claude_reasoning = Column(Text, nullable=True)
    consensus_score = Column(Integer, nullable=True)
    position_size_pct = Column(Float, nullable=True)

    # ── Execution result (Phase 3) ──
    trade_id = Column(Integer, nullable=True)  # FK to trades table
    execution_price = Column(Float, nullable=True)
    execution_time = Column(DateTime(timezone=True), nullable=True)

    # ── Metadata ──
    processing_time_ms = Column(Integer, nullable=True)
    webhook_latency_ms = Column(Integer, nullable=True)  # TV alert time → server arrival

    __table_args__ = (
        Index("ix_signals_desk_status", "desk_id", "status"),
        Index("ix_signals_symbol_time", "symbol_normalized", "received_at"),
    )

    def __repr__(self):
        return (
            f"<Signal {self.id} | {self.symbol_normalized} | "
            f"{self.alert_type} | {self.status}>"
        )
```

## `app/models/trade.py`

```python
"""
Trade Model - Tracks all executed trades and their lifecycle.
Populated in Phase 3 when MT5 execution is connected.
"""
from datetime import datetime, timezone
from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Text, Boolean, JSON, Index,
)
from app.database import Base


class Trade(Base):
    __tablename__ = "trades"

    id = Column(Integer, primary_key=True, autoincrement=True)

    # ── Link to originating signal ──
    signal_id = Column(Integer, nullable=False, index=True)

    # ── Trade identification ──
    desk_id = Column(String(30), nullable=False, index=True)
    symbol = Column(String(20), nullable=False, index=True)
    direction = Column(String(10), nullable=False)  # LONG / SHORT
    mt5_ticket = Column(Integer, nullable=True, unique=True)

    # ── Sizing ──
    lot_size = Column(Float, nullable=False)
    risk_pct = Column(Float, nullable=False)
    risk_dollars = Column(Float, nullable=False)
    position_size_modifier = Column(Float, default=1.0)  # from consensus scoring

    # ── Levels ──
    entry_price = Column(Float, nullable=True)
    stop_loss = Column(Float, nullable=False)
    take_profit_1 = Column(Float, nullable=True)
    take_profit_2 = Column(Float, nullable=True)
    trailing_stop = Column(Float, nullable=True)

    # ── Lifecycle timestamps ──
    opened_at = Column(DateTime(timezone=True), nullable=True)
    closed_at = Column(DateTime(timezone=True), nullable=True)

    # ── Result ──
    exit_price = Column(Float, nullable=True)
    pnl_dollars = Column(Float, nullable=True)
    pnl_pips = Column(Float, nullable=True)
    status = Column(
        String(20), default="PENDING", nullable=False, index=True,
    )
    # PENDING → OPEN → PARTIAL_CLOSE → CLOSED → CANCELLED

    # ── Pyramiding ──
    is_pyramid = Column(Boolean, default=False)
    pyramid_layer = Column(Integer, default=1)  # 1, 2, or 3
    parent_trade_id = Column(Integer, nullable=True)

    # ── Partial close tracking ──
    partial_closes = Column(JSON, nullable=True)
    # [{"pct": 33, "price": 1.1050, "pnl": 150.0, "at": "2025-..."}]
    partial_close_pct = Column(Float, nullable=True)  # total % closed so far

    # ── Live tracking ──
    unrealized_pnl = Column(Float, nullable=True)
    close_reason = Column(String(30), nullable=True)  # TP, SL, TRAIL, MANUAL, KILL_SWITCH

    # ── Audit ──
    claude_reasoning = Column(Text, nullable=True)
    consensus_score = Column(Integer, nullable=True)

    __table_args__ = (
        Index("ix_trades_desk_status", "desk_id", "status"),
        Index("ix_trades_symbol_open", "symbol", "opened_at"),
    )

    def __repr__(self):
        return (
            f"<Trade {self.id} | {self.symbol} {self.direction} | "
            f"{self.status} | PnL: {self.pnl_dollars}>"
        )
```

## `app/models/desk_state.py`

```python
"""
DeskState Model - Runtime state for each trading desk.
Tracks daily losses, consecutive losses, active status, etc.
"""
from datetime import datetime, timezone
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, JSON
from app.database import Base


class DeskState(Base):
    __tablename__ = "desk_states"

    id = Column(Integer, primary_key=True, autoincrement=True)
    desk_id = Column(String(30), unique=True, nullable=False, index=True)

    # ── Status ──
    is_active = Column(Boolean, default=True, nullable=False)
    is_paused = Column(Boolean, default=False, nullable=False)
    pause_until = Column(DateTime(timezone=True), nullable=True)

    # ── Daily counters (reset at midnight CET) ──
    trades_today = Column(Integer, default=0)
    daily_pnl = Column(Float, default=0.0)
    daily_loss = Column(Float, default=0.0)
    last_reset_date = Column(String(10), nullable=True)  # "2025-06-15"

    # ── Consecutive loss tracking ──
    consecutive_losses = Column(Integer, default=0)
    size_modifier = Column(Float, default=1.0)  # multiplier from loss protection

    # ── Open positions ──
    open_positions = Column(Integer, default=0)

    # ── Audit ──
    updated_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        onupdate=lambda: datetime.now(timezone.utc),
    )

    def __repr__(self):
        return (
            f"<DeskState {self.desk_id} | Active: {self.is_active} | "
            f"Trades: {self.trades_today} | PnL: {self.daily_pnl}>"
        )
```

## `app/models/ml_trade_log.py`

```python
"""
ML Training Data Model
Stores every signal with full context for future ML model training.
Each row = one signal event with all features and outcome.
"""
from datetime import datetime, timezone
from sqlalchemy import (
    Column, Integer, String, Float, DateTime, Text, Boolean, JSON, Index,
)
from app.database import Base


class MLTradeLog(Base):
    __tablename__ = "ml_trade_logs"

    id = Column(Integer, primary_key=True, autoincrement=True)
    created_at = Column(DateTime(timezone=True), default=lambda: datetime.now(timezone.utc))

    # ── Signal Features ──
    signal_id = Column(Integer, nullable=True, index=True)
    symbol = Column(String(20), nullable=False, index=True)
    direction = Column(String(10), nullable=False)        # LONG / SHORT
    timeframe = Column(String(10), nullable=True)          # 5M, 1H, 4H
    alert_type = Column(String(50), nullable=True)         # bullish_confirmation, etc.
    entry_price = Column(Float, nullable=True)
    sl_price = Column(Float, nullable=True)
    tp1_price = Column(Float, nullable=True)
    tp2_price = Column(Float, nullable=True)
    sl_pips = Column(Float, nullable=True)
    tp1_pips = Column(Float, nullable=True)
    rr_ratio = Column(Float, nullable=True)                # TP1/SL

    # ── Market Context at Signal Time ──
    desk_id = Column(String(30), nullable=False, index=True)
    session = Column(String(20), nullable=True)            # LONDON, NEW_YORK, OVERLAP, ASIAN
    day_of_week = Column(Integer, nullable=True)           # 0=Mon, 6=Sun
    hour_utc = Column(Integer, nullable=True)
    atr_value = Column(Float, nullable=True)               # ATR(14) at signal time
    spread_at_entry = Column(Float, nullable=True)         # spread in pips
    volatility_regime = Column(String(20), nullable=True)  # LOW, NORMAL, HIGH, EXTREME

    # ── Pipeline Scores ──
    ml_score = Column(Float, nullable=True)                # ML scorer output
    ml_method = Column(String(30), nullable=True)          # which ML method used
    consensus_score = Column(Integer, nullable=True)       # 0-10
    consensus_tier = Column(String(10), nullable=True)     # HIGH, MEDIUM, LOW
    claude_decision = Column(String(20), nullable=True)    # EXECUTE, REDUCE, SKIP, HOLD
    claude_confidence = Column(Float, nullable=True)       # 0-1
    claude_reasoning = Column(Text, nullable=True)

    # ── Risk State at Signal Time ──
    risk_pct = Column(Float, nullable=True)
    lot_size = Column(Float, nullable=True)
    risk_dollars = Column(Float, nullable=True)
    open_positions_desk = Column(Integer, nullable=True)
    open_positions_total = Column(Integer, nullable=True)
    daily_pnl_at_entry = Column(Float, nullable=True)
    daily_loss_at_entry = Column(Float, nullable=True)
    consecutive_losses = Column(Integer, nullable=True)
    size_modifier = Column(Float, nullable=True)           # from consecutive loss scaling

    # ── Filter Results ──
    approved = Column(Boolean, nullable=False, default=False)
    filter_blocked = Column(Boolean, nullable=False, default=False)
    block_reason = Column(String(100), nullable=True)      # which filter blocked
    is_oniai = Column(Boolean, nullable=False, default=False)

    # ── Outcome (filled when trade closes) ──
    outcome = Column(String(10), nullable=True)            # WIN, LOSS, BE (breakeven)
    pnl_pips = Column(Float, nullable=True)
    pnl_dollars = Column(Float, nullable=True)
    exit_price = Column(Float, nullable=True)
    exit_reason = Column(String(30), nullable=True)        # SL_HIT, TP1_HIT, TP2_HIT, TRAILING, TIME_EXIT
    hold_time_minutes = Column(Float, nullable=True)
    max_favorable_pips = Column(Float, nullable=True)      # max pips in profit direction (MFE)
    max_adverse_pips = Column(Float, nullable=True)        # max pips against (MAE)

    # ── Multi-Profile Outcomes ──
    srv100_pnl_pips = Column(Float, nullable=True)
    srv100_exit_reason = Column(String(30), nullable=True)
    srv30_pnl_pips = Column(Float, nullable=True)
    srv30_exit_reason = Column(String(30), nullable=True)
    mt5_pnl_pips = Column(Float, nullable=True)
    mt5_exit_reason = Column(String(30), nullable=True)
    oniai_pnl_pips = Column(Float, nullable=True)
    oniai_exit_reason = Column(String(30), nullable=True)

    # ── Correlation Context ──
    correlated_open_count = Column(Integer, nullable=True)  # same-group positions open
    correlation_group = Column(String(30), nullable=True)

    # ── Raw Data (JSON blob for anything else) ──
    raw_signal_data = Column(JSON, nullable=True)          # full webhook payload
    raw_enrichment = Column(JSON, nullable=True)           # TwelveData enrichment
    raw_ml_result = Column(JSON, nullable=True)            # full ML scorer output
    raw_consensus = Column(JSON, nullable=True)            # full consensus output
    raw_claude_response = Column(JSON, nullable=True)      # full CTO response

    # ── NovaQuant Schema Alignment ──
    webhook_latency_ms = Column(Integer, nullable=True)    # TV alert → server arrival
    rationale = Column(JSON, nullable=True, default=dict)  # structured CTO reasoning (JSONB)

    __table_args__ = (
        Index("ix_ml_log_symbol_date", "symbol", "created_at"),
        Index("ix_ml_log_desk_date", "desk_id", "created_at"),
        Index("ix_ml_log_outcome", "outcome"),
    )
```

## `app/routes/webhook.py`

```python
"""
Webhook Route - Receives TradingView/LuxAlgo alerts.
Entry point for the OniQuant trading pipeline.
"""
import json
import time
import re
import math
import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request, BackgroundTasks
from sqlalchemy.orm import Session

from app.database import get_db, SessionLocal
from app.schemas import TradingViewAlert, SignalResponse
from app.services.signal_validator import SignalValidator
from app.config import WEBHOOK_SECRET, SYMBOL_ALIASES, get_desk_for_symbol
from app.models.signal import Signal
from app.models.trade import Trade

logger = logging.getLogger("TradingSystem.Webhook")
router = APIRouter()

_NAN_RE = re.compile(r':\s*NaN\b', re.IGNORECASE)
_INF_RE = re.compile(r':\s*-?Infinity\b', re.IGNORECASE)


def _calc_latency(tv_time_str) -> Optional[int]:
    """Calculate ms between TV alert fire and server arrival."""
    if not tv_time_str:
        return None
    try:
        arrival_ms = int(time.time() * 1000)
        tv_val = float(str(tv_time_str).strip())
        tv_ms = int(tv_val) if tv_val > 1e12 else int(tv_val * 1000)
        latency = arrival_ms - tv_ms
        if latency < -5000 or latency > 300_000:
            return None
        return max(0, latency)
    except (ValueError, TypeError):
        return None


def _parse_body(raw: str) -> Optional[dict]:
    """Parse JSON, sanitizing NaN/Infinity if needed."""
    if not raw:
        return None
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        pass
    try:
        cleaned = _NAN_RE.sub(': null', raw)
        cleaned = _INF_RE.sub(': null', cleaned)
        return json.loads(cleaned)
    except (json.JSONDecodeError, ValueError):
        pass
    return None


def _map_fields(p: dict) -> dict:
    """Map LuxAlgo field names to OniQuant schema."""
    if "ticker" in p and "symbol" not in p:
        p["symbol"] = p.pop("ticker")
    if "bartime" in p and "time" not in p:
        p["time"] = str(p.pop("bartime"))
    elif "time" in p and not isinstance(p["time"], str):
        p["time"] = str(p["time"])
    if "alert_type" not in p:
        for k in ("alert", "signal", "type", "message", "condition"):
            if k in p:
                p["alert_type"] = p.pop(k)
                break
    if "timeframe" not in p:
        for k in ("interval", "tf"):
            if k in p:
                p["timeframe"] = p.pop(k)
                break
    if "ohlcv" in p and isinstance(p["ohlcv"], dict):
        ohlcv = p.pop("ohlcv")
        if "close" in ohlcv and "price" not in p:
            p["price"] = ohlcv["close"]
        if "volume" in ohlcv and "volume" not in p:
            p["volume"] = ohlcv["volume"]
    if "close" in p and "price" not in p:
        p["price"] = p.pop("close")
    p.pop("bar_color", None)
    return p


def _clean_na(p: dict) -> dict:
    """Convert 'na'/'NaN' strings to None for indicator fields."""
    for key in ("tp1", "tp2", "sl1", "sl2", "smart_trail"):
        val = p.get(key)
        if val is None:
            continue
        if isinstance(val, str):
            if val.lower() in ("nan", "na", "n/a", ""):
                p[key] = None
            else:
                try:
                    p[key] = float(val)
                except ValueError:
                    p[key] = None
        elif isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
            p[key] = None
    return p


def _direction(alert_type: str) -> Optional[str]:
    """Derive trade direction from alert type."""
    at = alert_type.lower()
    if "bullish" in at:
        return "LONG"
    if "bearish" in at:
        return "SHORT"
    if "exit" in at or at in ("take_profit", "stop_loss", "smart_trail_cross"):
        return "EXIT"
    return None


# ─── PATH-AUTHENTICATED ENDPOINT (for LuxAlgo S&O/PAC/OM) ───

@router.post("/webhook/{path_secret}", response_model=SignalResponse)
async def webhook_path_auth(
    path_secret: str,
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """LuxAlgo alerts - secret in URL path since alert() overrides message body."""
    if path_secret != WEBHOOK_SECRET:
        logger.warning(f"Path auth failed from {request.client.host}")
        raise HTTPException(status_code=401, detail="Invalid webhook secret")
    return await _handle(request, background_tasks, db, authed=True)


# ─── BODY-AUTHENTICATED ENDPOINT (for MSE) ───

@router.post("/webhook", response_model=SignalResponse)
async def webhook_body_auth(
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """MSE alerts - secret inside JSON body."""
    return await _handle(request, background_tasks, db, authed=False)


# ─── SHARED HANDLER ───

async def _handle(
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session,
    authed: bool = False,
):
    """Process any webhook: JSON, NaN JSON, or plain text."""
    arrival = time.time()

    # Read body
    try:
        raw = (await request.body()).decode("utf-8").strip()
    except Exception as e:
        logger.error(f"Read error: {e}")
        raise HTTPException(status_code=400, detail="Could not read request body")

    if not raw:
        raise HTTPException(status_code=400, detail="Empty body")

    logger.debug(f"Webhook ({len(raw)} chars): {raw[:300]}")

    # Parse
    payload = _parse_body(raw)

    if payload and isinstance(payload, dict):
        payload = _map_fields(payload)
        payload = _clean_na(payload)
    else:
        payload = {
            "symbol": request.query_params.get("symbol", "UNKNOWN"),
            "exchange": request.query_params.get("exchange", ""),
            "timeframe": request.query_params.get("timeframe", "60"),
            "alert_type": raw,
            "price": 0,
        }
        logger.info(f"Plain text wrapped: {raw[:100]}")

    # Auth
    if not authed:
        secret = payload.get("secret", "")
        q_secret = request.query_params.get("secret", "")
        if secret != WEBHOOK_SECRET and q_secret != WEBHOOK_SECRET:
            logger.warning(f"Auth failed from {request.client.host}")
            raise HTTPException(status_code=401, detail="Invalid webhook secret")

    payload["secret"] = WEBHOOK_SECRET

    # Latency
    latency_ms = _calc_latency(payload.get("time"))

    # Schema
    try:
        alert = TradingViewAlert(**payload)
    except Exception as e:
        logger.warning(f"Schema failed: {e}")
        _log_bad_signal(db, raw, str(e))
        return SignalResponse(
            status="rejected",
            is_valid=False,
            validation_errors=[str(e)],
            message="Schema validation failed",
        )

    # Normalize symbol
    raw_sym = alert.symbol
    if alert.exchange:
        raw_sym = f"{alert.exchange}:{alert.symbol}"
    normalized = SYMBOL_ALIASES.get(raw_sym, alert.symbol.replace("/", ""))

    # Route to desks
    desks = get_desk_for_symbol(normalized)
    if alert.desk and alert.desk != "auto":
        from app.config import DESKS
        if alert.desk in DESKS:
            desks = [alert.desk]

    # Validate
    validator = SignalValidator()
    is_valid, errors = validator.validate(alert, normalized, desks)

    # Direction
    direction = _direction(alert.alert_type)

    # Log to DB — truncate fields to match DB column limits
    proc_ms = int((time.time() - arrival) * 1000)
    safe_alert_type = alert.alert_type[:50] if alert.alert_type else "unknown"
    safe_symbol = alert.symbol[:20] if alert.symbol else "UNKNOWN"
    safe_normalized = normalized[:20] if normalized else "UNKNOWN"
    safe_timeframe = alert.timeframe[:10] if alert.timeframe else "60"

    try:
        signal = Signal(
            received_at=datetime.now(timezone.utc),
            source="tradingview",
            raw_payload=raw[:5000],
            symbol=safe_symbol,
            symbol_normalized=safe_normalized,
            timeframe=safe_timeframe,
            alert_type=safe_alert_type,
            direction=direction,
            price=alert.price,
            tp1=alert.tp1,
            tp2=alert.tp2,
            sl1=alert.sl1,
            sl2=alert.sl2,
            smart_trail=alert.smart_trail,
            desk_id=desks[0] if len(desks) == 1 else None,
            desks_matched=desks,
            is_valid=is_valid,
            validation_errors=errors if errors else None,
            status="VALIDATED" if is_valid else "REJECTED",
            processing_time_ms=proc_ms,
            webhook_latency_ms=latency_ms,
        )
        db.add(signal)
        db.commit()
        db.refresh(signal)
    except Exception as e:
        db.rollback()
        logger.error(f"DB insert failed: {e}")
        return SignalResponse(
            status="error",
            is_valid=False,
            message=f"Database error: {str(e)[:200]}",
        )

    lat_str = f"{latency_ms}ms" if latency_ms is not None else "N/A"

    if is_valid:
        logger.info(
            f"SIGNAL #{signal.id} | {normalized} | {alert.alert_type} | "
            f"{direction} | Desks: {desks} | Price: {alert.price} | "
            f"Latency: {lat_str} | Proc: {proc_ms}ms"
        )

        entry_types = {
            "bullish_confirmation", "bearish_confirmation",
            "bullish_plus", "bearish_plus",
            "bullish_confirmation_plus", "bearish_confirmation_plus",
            "contrarian_bullish", "contrarian_bearish",
            "confirmation_turn_plus",
            "confirmation_turn_bullish", "confirmation_turn_bearish",
            "mse_bullish_confirmation", "mse_bearish_confirmation",
        }
        if alert.alert_type in entry_types:
            background_tasks.add_task(_run_pipeline, signal.id, latency_ms)
            logger.info(f"Pipeline queued for #{signal.id}")

        exit_types = {
            "bullish_exit", "bearish_exit",
            "take_profit", "stop_loss", "smart_trail_cross",
        }
        if alert.alert_type in exit_types and desks:
            background_tasks.add_task(
                _process_exit, normalized, alert.alert_type, desks, signal.id
            )
    else:
        logger.warning(
            f"REJECTED #{signal.id} | {normalized} | {alert.alert_type} | "
            f"Errors: {errors} | Latency: {lat_str}"
        )

    return SignalResponse(
        status="accepted" if is_valid else "rejected",
        signal_id=signal.id,
        symbol=normalized,
        alert_type=alert.alert_type,
        desks_matched=desks,
        is_valid=is_valid,
        validation_errors=errors if errors else None,
        message=(
            f"Signal logged and routed to {len(desks)} desk(s)"
            if is_valid
            else f"Signal rejected: {'; '.join(errors or [])}"
        ),
    )


# ─── HELPERS ───

def _log_bad_signal(db: Session, raw: str, error: str):
    """Log malformed signals for debugging."""
    try:
        sig = Signal(
            received_at=datetime.now(timezone.utc),
            source="tradingview",
            raw_payload=raw[:5000],
            symbol="UNKNOWN",
            symbol_normalized="UNKNOWN",
            timeframe="UNKNOWN",
            alert_type="PARSE_ERROR",
            is_valid=False,
            validation_errors=[error[:500]],
            status="REJECTED",
        )
        db.add(sig)
        db.commit()
    except Exception as e:
        logger.error(f"Failed to log bad signal: {e}")


async def _run_pipeline(signal_id: int, latency_ms: int = None):
    """Run Phase 2 pipeline in background."""
    from app.services.pipeline import process_signal
    db = SessionLocal()
    try:
        result = await process_signal(signal_id, db, webhook_latency_ms=latency_ms)
        logger.info(
            f"Pipeline #{signal_id}: "
            f"{json.dumps({k: v.get('decision', 'N/A') for k, v in result.get('results', {}).items()})}"
        )
    except Exception as e:
        logger.error(f"Pipeline error #{signal_id}: {e}", exc_info=True)
    finally:
        db.close()


async def _process_exit(symbol: str, alert_type: str, desks: list, signal_id: int):
    """Flag open trades for closure when exit signal fires."""
    db = SessionLocal()
    try:
        if "bullish_exit" in alert_type:
            close_dir = "LONG"
        elif "bearish_exit" in alert_type:
            close_dir = "SHORT"
        else:
            close_dir = None

        query = db.query(Trade).filter(
            Trade.symbol == symbol,
            Trade.status.in_(["EXECUTED", "OPEN"]),
        )
        if desks:
            query = query.filter(Trade.desk_id.in_(desks))
        if close_dir:
            query = query.filter(Trade.direction == close_dir)

        trades = query.all()
        if not trades:
            return

        for t in trades:
            t.status = "CLOSE_REQUESTED"
            t.close_reason = f"EXIT_{alert_type.upper()}"
            logger.info(
                f"EXIT | Trade #{t.id} | {t.symbol} {t.direction} | "
                f"Ticket: {t.mt5_ticket} | {alert_type} | Signal #{signal_id}"
            )

        db.commit()
        logger.info(f"Exit #{signal_id}: {len(trades)} trades flagged ({symbol})")
    except Exception as e:
        logger.error(f"Exit processing failed: {e}", exc_info=True)
    finally:
        db.close()


@router.get("/signal/{signal_id}")
async def get_signal(signal_id: int, db: Session = Depends(get_db)):
    """Get signal status."""
    signal = db.query(Signal).filter(Signal.id == signal_id).first()
    if not signal:
        raise HTTPException(status_code=404, detail="Signal not found")
    return {
        "id": signal.id,
        "symbol": signal.symbol_normalized,
        "alert_type": signal.alert_type,
        "direction": signal.direction,
        "price": signal.price,
        "status": signal.status,
        "desk_id": signal.desk_id,
        "desks_matched": signal.desks_matched,
        "ml_score": signal.ml_score,
        "consensus_score": signal.consensus_score,
        "claude_decision": signal.claude_decision,
        "claude_reasoning": signal.claude_reasoning,
        "position_size_pct": signal.position_size_pct,
        "processing_time_ms": signal.processing_time_ms,
        "webhook_latency_ms": signal.webhook_latency_ms,
        "received_at": signal.received_at.isoformat() if signal.received_at else None,
    }
```

## `app/routes/health.py`

```python
"""
Health Check Route - System status and database connectivity.
"""
import time
import logging
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.database import get_db, check_db_connection
from app.schemas import HealthResponse
from app.models.signal import Signal

logger = logging.getLogger("TradingSystem.Health")
router = APIRouter()

_start_time = time.time()


@router.get("/health", response_model=HealthResponse)
async def health_check(db: Session = Depends(get_db)):
    """System health check - used by Railway and monitoring."""
    db_ok = check_db_connection()

    # Count today's signals
    today_start = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    try:
        signals_today = (
            db.query(func.count(Signal.id))
            .filter(Signal.received_at >= today_start)
            .scalar()
        ) or 0
    except Exception:
        signals_today = -1

    return HealthResponse(
        status="operational" if db_ok else "degraded",
        database="connected" if db_ok else "disconnected",
        uptime_seconds=round(time.time() - _start_time, 1),
        signals_today=signals_today,
    )
```

## `app/routes/dashboard.py`

```python
"""
Dashboard Route - Desk-level overview for Telegram and monitoring.
"""
import logging
from datetime import datetime, timezone

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.database import get_db
from app.schemas import DashboardResponse, DeskSummary
from app.models.signal import Signal
from app.models.desk_state import DeskState
from app.config import DESKS

logger = logging.getLogger("TradingSystem.Dashboard")
router = APIRouter()


@router.get("/dashboard", response_model=DashboardResponse)
async def get_dashboard(db: Session = Depends(get_db)):
    """Return firm-wide dashboard with per-desk summaries."""
    today_start = datetime.now(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    # Total signals today
    total_signals = (
        db.query(func.count(Signal.id))
        .filter(Signal.received_at >= today_start)
        .scalar()
    ) or 0

    # Build desk summaries
    desk_summaries = []
    total_pnl = 0.0
    total_trades = 0

    for desk_id, desk_config in DESKS.items():
        state = db.query(DeskState).filter(DeskState.desk_id == desk_id).first()

        if state:
            summary = DeskSummary(
                desk_id=desk_id,
                name=desk_config["name"],
                is_active=state.is_active,
                trades_today=state.trades_today,
                daily_pnl=state.daily_pnl,
                consecutive_losses=state.consecutive_losses,
                size_modifier=state.size_modifier,
                open_positions=state.open_positions,
            )
            total_pnl += state.daily_pnl
            total_trades += state.trades_today
        else:
            # Desk state not yet initialized
            summary = DeskSummary(
                desk_id=desk_id,
                name=desk_config["name"],
                is_active=True,
                trades_today=0,
                daily_pnl=0.0,
                consecutive_losses=0,
                size_modifier=1.0,
                open_positions=0,
            )

        desk_summaries.append(summary)

    return DashboardResponse(
        firm_status="OPERATIONAL",
        total_signals_today=total_signals,
        total_trades_today=total_trades,
        total_daily_pnl=round(total_pnl, 2),
        desks=desk_summaries,
    )
```

## `app/routes/control.py`

```python
"""
Control Route — Kill switch, desk management, VPS status.
Accessible from /docs or Telegram. Independent of webhooks.
"""
import logging
from datetime import datetime, timezone, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.database import get_db
from app.config import DESKS
from app.models.desk_state import DeskState
from app.services.zmq_bridge import ZMQBridge
from app.services.telegram_bot import TelegramBot
from app.services.trade_reporter import TradeReporter

logger = logging.getLogger("TradingSystem.Control")
router = APIRouter()

# Shared instances
_zmq: ZMQBridge = None
_telegram: TelegramBot = None
_reporter: TradeReporter = None


def _get_zmq():
    global _zmq
    if _zmq is None:
        _zmq = ZMQBridge()
    return _zmq


def _get_telegram():
    global _telegram
    if _telegram is None:
        _telegram = TelegramBot()
    return _telegram


def _get_reporter():
    global _reporter
    if _reporter is None:
        _reporter = TradeReporter()
    return _reporter


@router.post("/kill-switch")
async def kill_switch(
    scope: str = "ALL",
    db: Session = Depends(get_db),
):
    """
    Emergency kill switch. Closes all positions and halts trading.
    scope: "ALL" for entire firm, or a desk ID like "DESK1_SCALPER"
    """
    zmq_bridge = _get_zmq()
    telegram = _get_telegram()

    # Notify via Telegram
    await telegram.alert_kill_switch(scope, "API")

    # Send to VPS
    zmq_result = await zmq_bridge.send_kill_switch(scope)

    # Update desk states
    if scope == "ALL":
        desks = db.query(DeskState).all()
        for state in desks:
            state.is_active = False
        db.commit()
    else:
        state = db.query(DeskState).filter(DeskState.desk_id == scope).first()
        if state:
            state.is_active = False
            db.commit()

    logger.warning(f"KILL SWITCH executed | Scope: {scope}")

    return {
        "status": "executed",
        "scope": scope,
        "vps_response": zmq_result,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }


@router.post("/desk/{desk_id}/pause")
async def pause_desk(
    desk_id: str,
    hours: int = 2,
    db: Session = Depends(get_db),
):
    """Pause a desk for N hours (default 2)."""
    if desk_id not in DESKS:
        raise HTTPException(status_code=404, detail=f"Unknown desk: {desk_id}")

    state = db.query(DeskState).filter(DeskState.desk_id == desk_id).first()
    if not state:
        state = DeskState(desk_id=desk_id, is_active=True)
        db.add(state)

    state.is_paused = True
    state.pause_until = datetime.now(timezone.utc) + timedelta(hours=hours)
    db.commit()

    telegram = _get_telegram()
    await telegram.send_message(
        f"⏸️ <b>{desk_id}</b> paused for {hours} hours via API."
    )

    return {
        "status": "paused",
        "desk_id": desk_id,
        "pause_until": state.pause_until.isoformat(),
    }


@router.post("/desk/{desk_id}/resume")
async def resume_desk(
    desk_id: str,
    db: Session = Depends(get_db),
):
    """Resume a paused desk."""
    if desk_id not in DESKS:
        raise HTTPException(status_code=404, detail=f"Unknown desk: {desk_id}")

    state = db.query(DeskState).filter(DeskState.desk_id == desk_id).first()
    if state:
        state.is_paused = False
        state.is_active = True
        state.pause_until = None
        db.commit()

    telegram = _get_telegram()
    await telegram.send_message(f"▶️ <b>{desk_id}</b> resumed via API.")

    return {"status": "resumed", "desk_id": desk_id}


@router.get("/vps/status")
async def vps_status():
    """Check VPS connection status and MT5 state."""
    zmq_bridge = _get_zmq()
    result = await zmq_bridge.get_vps_status()
    return result


@router.get("/desks")
async def list_desks(db: Session = Depends(get_db)):
    """List all desks with their current state."""
    result = []
    for desk_id, desk in DESKS.items():
        state = db.query(DeskState).filter(DeskState.desk_id == desk_id).first()
        result.append({
            "desk_id": desk_id,
            "name": desk["name"],
            "style": desk["style"],
            "symbols": desk["symbols"],
            "risk_pct": desk.get("risk_pct"),
            "is_active": state.is_active if state else True,
            "is_paused": state.is_paused if state else False,
            "trades_today": state.trades_today if state else 0,
            "daily_pnl": state.daily_pnl if state else 0,
            "consecutive_losses": state.consecutive_losses if state else 0,
            "size_modifier": state.size_modifier if state else 1.0,
            "open_positions": state.open_positions if state else 0,
        })
    return {"desks": result}


@router.post("/report/daily")
async def trigger_daily_report(db: Session = Depends(get_db)):
    """Send daily performance report to all Telegram channels."""
    reporter = _get_reporter()
    await reporter.send_daily_report(db)
    return {"status": "sent", "report": "daily"}


@router.post("/report/weekly")
async def trigger_weekly_report(db: Session = Depends(get_db)):
    """Send weekly performance report to all Telegram channels."""
    reporter = _get_reporter()
    await reporter.send_weekly_report(db)
    return {"status": "sent", "report": "weekly"}


@router.post("/report/monthly")
async def trigger_monthly_report(db: Session = Depends(get_db)):
    """Send monthly performance report to all Telegram channels."""
    reporter = _get_reporter()
    await reporter.send_monthly_report(db)
    return {"status": "sent", "report": "monthly"}


# ─────────────────────────────────────────
# ML TRAINING DATA EXPORT
# ─────────────────────────────────────────

@router.get("/ml/export")
async def export_ml_data(
    db: Session = Depends(get_db),
    desk_id: str = None,
    symbol: str = None,
    labeled_only: bool = True,
    limit: int = None,
):
    """
    Export ML training data as JSON.
    Ready to load into pandas: pd.DataFrame(response["data"])

    Query params:
      ?desk_id=DESK1_SCALPER&symbol=EURUSD&labeled_only=true&limit=1000
    """
    from app.services.ml_data_logger import MLDataLogger
    data = MLDataLogger.get_training_data(
        db, labeled_only=labeled_only, desk_id=desk_id, symbol=symbol, limit=limit,
    )
    return {"count": len(data), "data": data}


@router.get("/ml/stats")
async def ml_data_stats(db: Session = Depends(get_db)):
    """Quick stats on collected ML training data."""
    from app.services.ml_data_logger import MLDataLogger
    return MLDataLogger.get_stats(db)
```

## `app/routes/trade_queue.py`

```python
"""
Trade Queue Route — API for the VPS to poll for pending trades
and report execution results back to Railway.

The VPS client calls:
  GET  /api/trades/pending     — fetch next batch of approved trades
  POST /api/trades/executed    — report trade was placed on MT5
  POST /api/trades/closed      — report trade was closed
  POST /api/trades/update      — update PnL / partial close
  GET  /api/trades/open        — list all open trades
"""
import logging
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Header
from sqlalchemy.orm import Session
from pydantic import BaseModel

from app.database import get_db
from app.config import WEBHOOK_SECRET
from app.models.signal import Signal
from app.models.trade import Trade
from app.models.desk_state import DeskState

logger = logging.getLogger("TradingSystem.TradeQueue")
router = APIRouter()


# ─────────────────────────────────────────
# Request/Response Models
# ─────────────────────────────────────────

class ExecutionReport(BaseModel):
    signal_id: int
    desk_id: str
    symbol: str
    direction: str
    mt5_ticket: int
    entry_price: float
    lot_size: float
    stop_loss: float
    take_profit: Optional[float] = None

class CloseReport(BaseModel):
    mt5_ticket: int
    exit_price: float
    pnl_dollars: float
    pnl_pips: Optional[float] = None
    close_reason: str  # "TP", "SL", "TRAIL", "MANUAL", "KILL_SWITCH", "SIGNAL"
    symbol: Optional[str] = None      # for sim trades
    desk_id: Optional[str] = None     # for sim trades
    direction: Optional[str] = None   # for sim trades

class UpdateReport(BaseModel):
    mt5_ticket: int
    current_price: Optional[float] = None
    current_pnl: Optional[float] = None
    new_sl: Optional[float] = None
    new_tp: Optional[float] = None
    partial_close_pct: Optional[float] = None


def _verify_secret(x_api_key: str = Header(None)):
    """Verify VPS is authorized."""
    if x_api_key != WEBHOOK_SECRET:
        raise HTTPException(status_code=401, detail="Invalid API key")


# ─────────────────────────────────────────
# Endpoints
# ─────────────────────────────────────────

@router.get("/trades/pending")
async def get_pending_trades(
    db: Session = Depends(get_db),
    x_api_key: str = Header(None),
):
    """
    VPS polls this to get approved trades waiting for execution.
    Returns signals with status = 'DECIDED' (approved by CTO).
    """
    _verify_secret(x_api_key)

    pending = (
        db.query(Signal)
        .filter(Signal.status == "DECIDED")
        .filter(Signal.claude_decision.in_(["EXECUTE", "REDUCE"]))
        .order_by(Signal.received_at.asc())
        .limit(10)
        .all()
    )

    trades = []
    for sig in pending:
        trades.append({
            "signal_id": sig.id,
            "symbol": sig.symbol_normalized,
            "direction": sig.direction,
            "desk_id": sig.desk_id,
            "price": sig.price,
            "stop_loss": sig.sl1,
            "take_profit_1": sig.tp1,
            "take_profit_2": sig.tp2,
            "smart_trail": sig.smart_trail,
            "risk_pct": sig.position_size_pct,
            "claude_decision": sig.claude_decision,
            "consensus_score": sig.consensus_score,
            "ml_score": sig.ml_score,
            "received_at": sig.received_at.isoformat() if sig.received_at else None,
        })

        # Mark as queued so we don't send it again
        sig.status = "QUEUED"

    if trades:
        db.commit()
        logger.info(f"VPS polled: {len(trades)} pending trades sent")

    return {"pending": trades, "count": len(trades)}


@router.post("/trades/executed")
async def report_execution(
    report: ExecutionReport,
    db: Session = Depends(get_db),
    x_api_key: str = Header(None),
):
    """VPS reports that a trade was successfully placed on MT5."""
    _verify_secret(x_api_key)

    # Update signal status
    signal = db.query(Signal).filter(Signal.id == report.signal_id).first()
    if not signal:
        raise HTTPException(status_code=404, detail="Signal not found")

    signal.status = "EXECUTED"
    signal.execution_price = report.entry_price
    signal.execution_time = datetime.now(timezone.utc)

    # Create trade record
    trade = Trade(
        signal_id=report.signal_id,
        desk_id=report.desk_id,
        symbol=report.symbol,
        direction=report.direction,
        mt5_ticket=report.mt5_ticket,
        entry_price=report.entry_price,
        lot_size=report.lot_size,
        risk_pct=signal.position_size_pct or 0,
        risk_dollars=0,  # calculated VPS-side
        stop_loss=report.stop_loss,
        take_profit_1=report.take_profit,
        status="OPEN",
        opened_at=datetime.now(timezone.utc),
    )
    db.add(trade)

    # Update signal FK
    signal.trade_id = trade.id

    # Update desk state
    desk_state = db.query(DeskState).filter(
        DeskState.desk_id == report.desk_id
    ).first()
    if desk_state:
        desk_state.trades_today = (desk_state.trades_today or 0) + 1
        desk_state.open_positions = (desk_state.open_positions or 0) + 1

    db.commit()

    logger.info(
        f"EXECUTED | Signal #{report.signal_id} | "
        f"{report.symbol} {report.direction} | "
        f"Ticket: {report.mt5_ticket} | "
        f"Entry: {report.entry_price} | Lots: {report.lot_size}"
    )

    return {"status": "recorded", "trade_id": trade.id}


@router.post("/trades/closed")
async def report_close(
    report: CloseReport,
    db: Session = Depends(get_db),
    x_api_key: str = Header(None),
):
    """VPS reports that a trade was closed."""
    _verify_secret(x_api_key)

    trade = db.query(Trade).filter(
        Trade.mt5_ticket == report.mt5_ticket
    ).first()

    # Handle sim trades by ticket range:
    # 800000-899999: OniAI virtual
    # 900000-998999: Server sim (handled by server_simulator.py)
    # 999000+: MT5 sim (from EA)
    is_mt5_sim = report.mt5_ticket >= 999000
    is_server_sim = 900000 <= report.mt5_ticket < 999000
    is_oniai = 800000 <= report.mt5_ticket < 900000

    if not trade and not is_mt5_sim and not is_oniai:
        raise HTTPException(status_code=404, detail="Trade not found")

    if trade:
        trade.exit_price = report.exit_price
        trade.pnl_dollars = report.pnl_dollars
        trade.pnl_pips = report.pnl_pips
        trade.close_reason = report.close_reason
        if is_oniai:
            trade.status = "ONIAI_CLOSED"
        elif is_server_sim:
            trade.status = "SRV_CLOSED"
        else:
            trade.status = "CLOSED"
        trade.closed_at = datetime.now(timezone.utc)

        # Update desk state
        desk_state = db.query(DeskState).filter(
            DeskState.desk_id == trade.desk_id
        ).first()
        if desk_state:
            desk_state.open_positions = max(0, (desk_state.open_positions or 0) - 1)
            desk_state.daily_pnl = (desk_state.daily_pnl or 0) + report.pnl_dollars

            if report.pnl_dollars < 0:
                desk_state.daily_loss = (desk_state.daily_loss or 0) + report.pnl_dollars
                desk_state.consecutive_losses = (desk_state.consecutive_losses or 0) + 1
            else:
                desk_state.consecutive_losses = 0

        # Update signal status
        signal = db.query(Signal).filter(Signal.id == trade.signal_id).first()
        if signal:
            signal.status = "CLOSED"

        db.commit()

    elif is_mt5_sim:
        # EA reported a sim close — create Trade record for reporting
        mt5_trade = Trade(
            signal_id=0,
            desk_id=report.desk_id or "UNKNOWN",
            symbol=report.symbol or "?",
            direction=report.direction or "?",
            mt5_ticket=report.mt5_ticket,
            entry_price=0,
            lot_size=0,
            risk_pct=0,
            risk_dollars=0,
            stop_loss=0,
            exit_price=report.exit_price,
            pnl_dollars=report.pnl_dollars,
            pnl_pips=report.pnl_pips,
            close_reason=f"MT5_{report.close_reason or 'UNKNOWN'}",
            status="MT5_CLOSED",
            opened_at=datetime.now(timezone.utc),
            closed_at=datetime.now(timezone.utc),
        )
        db.add(mt5_trade)
        db.commit()
        trade = mt5_trade

    # Determine symbol and desk for notification
    sym = trade.symbol if trade else (report.symbol or "?")
    desk = trade.desk_id if trade else (report.desk_id or "?")

    sim_tag = ""
    if is_mt5_sim:
        sim_tag = " [MT5]"
    elif is_server_sim:
        sim_tag = " [SRV]"
    elif is_oniai:
        sim_tag = " [OniAI]"
    logger.info(
        f"CLOSED{sim_tag} | Ticket {report.mt5_ticket} | "
        f"{sym} | PnL: ${report.pnl_dollars:+.2f} | "
        f"Reason: {report.close_reason}"
    )

    # Send Telegram notification
    try:
        from app.services.telegram_bot import TelegramBot
        telegram = TelegramBot()
        await telegram.notify_trade_exit(
            symbol=sym,
            desk_id=desk,
            pnl=report.pnl_dollars,
            reason=report.close_reason,
        )
        await telegram.close()
    except Exception as e:
        logger.error(f"Telegram close notification failed: {e}")

    return {"status": "recorded", "pnl": report.pnl_dollars}


@router.post("/trades/update")
async def update_trade(
    report: UpdateReport,
    db: Session = Depends(get_db),
    x_api_key: str = Header(None),
):
    """VPS reports a trade update (trailing SL move, partial close, etc.)."""
    _verify_secret(x_api_key)

    trade = db.query(Trade).filter(
        Trade.mt5_ticket == report.mt5_ticket
    ).first()
    if not trade:
        raise HTTPException(status_code=404, detail="Trade not found")

    if report.new_sl is not None:
        trade.stop_loss = report.new_sl
    if report.new_tp is not None:
        trade.take_profit = report.new_tp
    if report.current_pnl is not None:
        trade.unrealized_pnl = report.current_pnl
    if report.partial_close_pct is not None:
        trade.partial_close_pct = report.partial_close_pct

    db.commit()
    return {"status": "updated"}


@router.get("/trades/open")
async def list_open_trades(
    db: Session = Depends(get_db),
    x_api_key: str = Header(None),
):
    """List all currently open trades across all desks."""
    _verify_secret(x_api_key)

    trades = db.query(Trade).filter(Trade.status == "OPEN").all()

    return {
        "open_trades": [
            {
                "id": t.id,
                "mt5_ticket": t.mt5_ticket,
                "signal_id": t.signal_id,
                "desk_id": t.desk_id,
                "symbol": t.symbol,
                "direction": t.direction,
                "entry_price": t.entry_price,
                "lot_size": t.lot_size,
                "stop_loss": t.stop_loss,
                "take_profit": t.take_profit_1,
                "unrealized_pnl": t.unrealized_pnl,
                "opened_at": t.opened_at.isoformat() if t.opened_at else None,
            }
            for t in trades
        ],
        "count": len(trades),
    }


# ─────────────────────────────────────────
# EXIT POLLING — EA polls for server-side close commands
# ─────────────────────────────────────────

@router.get("/trades/exits")
async def get_exit_commands(
    desk: str = "ALL",
    x_api_key: str = Header(None),
    db: Session = Depends(get_db),
):
    """
    Returns a list of MT5 tickets that the EA must close immediately.

    Trades get flagged for exit when:
    - LuxAlgo exit signal arrives for an open position
    - Server simulator detects time expiry (but only EA can close live)
    - Kill switch activated
    - Manual close command via Telegram

    The EA closes the position, then reports via POST /api/trades/closed.
    """
    _auth(x_api_key)

    # Find trades flagged for closure
    query = db.query(Trade).filter(
        Trade.status == "CLOSE_REQUESTED",
    )
    if desk != "ALL":
        query = query.filter(Trade.desk_id == desk)

    trades = query.all()

    if not trades:
        return {"exits": [], "count": 0}

    exits = []
    for t in trades:
        exits.append({
            "mt5_ticket": t.mt5_ticket,
            "signal_id": t.signal_id,
            "symbol": t.symbol,
            "desk_id": t.desk_id,
            "direction": t.direction,
            "close_reason": t.close_reason or "SERVER_EXIT",
        })

    logger.info(f"Exit commands: {len(exits)} trades to close for desk={desk}")

    return {
        "exits": exits,
        "count": len(exits),
    }

```

## `app/routes/telegram.py`

```python
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
from app.services.zmq_bridge import ZMQBridge

logger = logging.getLogger("TradingSystem.TelegramRoute")
router = APIRouter()

# Shared service instances
_telegram: TelegramBot = None
_zmq: ZMQBridge = None

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


def _get_zmq():
    global _zmq
    if _zmq is None:
        _zmq = ZMQBridge()
    return _zmq


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
    zmq_bridge = _get_zmq()

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
            await _handle_kill(telegram, zmq_bridge, db, scope, "Telegram")

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

        elif command == "/metaapi":
            await _handle_metaapi(telegram)

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
    telegram: TelegramBot, zmq_bridge: ZMQBridge,
    db: Session, scope: str, triggered_by: str
):
    """Execute kill switch."""
    from app.models.desk_state import DeskState

    await telegram.alert_kill_switch(scope, triggered_by)

    result = await zmq_bridge.send_kill_switch(scope)

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

    # Price providers (4-provider JIT: Binance WS + TD + FH + FMP)
    try:
        from app.services.server_simulator import get_provider_stats
        stats = get_provider_stats()
        td_ok = stats.get("twelvedata", {}).get("success", 0) > 0
        fh_ok = stats.get("finnhub", {}).get("success", 0) > 0
        fmp_ok = stats.get("fmp", {}).get("success", 0) > 0
        ws_ok = stats.get("kraken_ws", {}).get("state", "") != "OPEN"
        active = sum([td_ok, fh_ok, fmp_ok, ws_ok])
        checks.append(
            f"{'🟢' if ws_ok else '🔴'} Kraken WS  "
            f"{'🟢' if td_ok else '🔴'} TD  "
            f"{'🟢' if fh_ok else '🔴'} FH  "
            f"{'🟢' if fmp_ok else '🔴'} FMP  ({active}/4)"
        )
    except:
        checks.append("🟡 Prices (unknown)")

    # Server sim
    try:
        from app.main import _simulator_task
        running = _simulator_task and not _simulator_task.done()
        checks.append(f"{'🟢' if running else '🔴'} Server Simulator")
    except:
        checks.append("🟡 Server Simulator (unknown)")

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
    """Show price provider health stats — 4-provider asset-class router."""
    import os
    from app.services.server_simulator import get_provider_stats

    stats = get_provider_stats()

    text = (
        "📡 <b>PRICE PROVIDERS (4-Provider JIT)</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
    )

    # Provider details with roles
    providers = [
        ("kraken_ws", "Kraken WS", "Crypto streaming (+ CoinCap backup)"),
        ("twelvedata", "TwelveData", "Forex + metals"),
        ("finnhub", "Finnhub", "US stocks"),
        ("fmp", "FMP", "Indices + commodities"),
    ]

    for name, label, role in providers:
        s = stats.get(name, {})
        success = s.get("success", 0)
        fail = s.get("fail", 0)
        state = s.get("state", "CLOSED")
        total = success + fail

        if state == "OPEN":
            emoji = "🔴"
            rate = "CIRCUIT OPEN"
        elif total == 0:
            emoji = "⚪"
            rate = "No requests"
        else:
            pct = success / total * 100
            emoji = "🟢" if pct > 90 else ("🟡" if pct > 50 else "🔴")
            rate = f"{pct:.0f}%"

        extra = ""
        batches = s.get("batch_calls", 0)
        if batches:
            extra = f" · {batches}b"

        text += f"{emoji} <b>{label}</b> · {role}\n"
        text += f"    {success}✅ {fail}❌ · {rate}{extra}\n"

    # TD throttle status
    td_throttle = stats.get("td_throttle", {})
    credits_60s = td_throttle.get("credits_60s", 0)
    daily = td_throttle.get("daily_credits", 0)
    text += (
        f"\n⏱ <b>TD Throttle:</b> {credits_60s}/8 credits (60s) · {daily}/800 daily\n"
    )

    # API key status
    td_key = "✅" if os.getenv("TWELVEDATA_API_KEY") else "❌"
    fh_key = "✅" if os.getenv("FINNHUB_API_KEY") else "❌"
    fmp_key = "✅" if os.getenv("FMP_API_KEY") else "❌"

    text += (
        f"\n<b>API Keys:</b>\n"
        f"Binance WS ✅ (no key needed)\n"
        f"TwelveData {td_key} (8/min batched)\n"
        f"Finnhub {fh_key} (60/min individual)\n"
        f"FMP {fmp_key} (250/day batched)\n"
    )

    await telegram._send_to_system(text)


async def _handle_metaapi(telegram: TelegramBot):
    """Show MetaApi $1M demo account status."""
    from app.services.metaapi_executor import get_executor, is_enabled

    if not is_enabled():
        text = (
            "🔌 <b>METAAPI STATUS</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "❌ <b>Not configured</b>\n\n"
            "To enable MetaApi $1M demo execution:\n"
            "1. Sign up at metaapi.cloud (free tier)\n"
            "2. Add your broker demo account\n"
            "3. Add to Railway variables:\n"
            "   • <code>METAAPI_TOKEN</code>\n"
            "   • <code>METAAPI_ACCOUNT_ID</code>\n"
            "   • <code>METAAPI_REGION</code> (default: new-york)\n"
        )
        await telegram._send_to_system(text)
        return

    ma = get_executor()
    info = await ma.get_account_info()
    positions = await ma.get_positions()

    if "error" in info:
        text = (
            "🔌 <b>METAAPI STATUS</b>\n"
            "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
            f"❌ API Error: {info['error']}\n"
        )
        await telegram._send_to_system(text)
        return

    balance = info.get("balance", 0)
    equity = info.get("equity", 0)
    margin = info.get("margin", 0)
    free_margin = info.get("freeMargin", 0)
    pnl = equity - balance
    pnl_emoji = "🟢" if pnl >= 0 else "🔴"
    leverage = info.get("leverage", 0)
    broker = info.get("server", "Unknown")

    text = (
        "🔌 <b>METAAPI $1M DEMO</b>\n"
        "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
        f"🏦 Broker: {broker}\n"
        f"⚖️ Leverage: 1:{leverage}\n\n"
        f"💰 Balance:     ${balance:,.2f}\n"
        f"📊 Equity:      ${equity:,.2f}\n"
        f"{pnl_emoji} Floating PnL: ${pnl:+,.2f}\n"
        f"🔒 Margin:      ${margin:,.2f}\n"
        f"💵 Free Margin: ${free_margin:,.2f}\n\n"
        f"📌 Open positions: {len(positions)}\n"
    )

    if positions:
        text += "\n"
        for pos in positions[:10]:
            sym = pos.get("symbol", "?")
            ptype = pos.get("type", "")
            vol = pos.get("volume", 0)
            profit = pos.get("profit", 0)
            p_emoji = "🟢" if profit >= 0 else "🔴"
            direction = "BUY" if "BUY" in ptype.upper() else "SELL"
            text += f"  {p_emoji} {sym} {direction} {vol} → ${profit:+.2f}\n"
        if len(positions) > 10:
            text += f"  ... +{len(positions) - 10} more\n"

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
        "/metaapi — $1M demo account status\n"
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
```

## `app/routes/ml_export.py`

```python
"""
ML Data Export API
GET /api/ml/export?format=csv&days=30
GET /api/ml/stats
"""
import csv
import io
import os
import logging
from datetime import datetime, timezone, timedelta

from fastapi import APIRouter, Depends, Query, Header, HTTPException
from fastapi.responses import StreamingResponse, JSONResponse
from sqlalchemy import func
from sqlalchemy.orm import Session

from app.database import get_db
from app.models.ml_trade_log import MLTradeLog

logger = logging.getLogger("TradingSystem.MLExport")
router = APIRouter()

WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")

def _verify(key):
    if key != WEBHOOK_SECRET:
        raise HTTPException(status_code=401)

@router.get("/ml/export")
async def export_ml_data(
    format: str = Query("csv"),
    days: int = Query(30, ge=1, le=365),
    desk: str = Query(None),
    symbol: str = Query(None),
    db: Session = Depends(get_db),
    x_api_key: str = Header(None),
):
    _verify(x_api_key)
    since = datetime.now(timezone.utc) - timedelta(days=days)
    query = db.query(MLTradeLog).filter(MLTradeLog.created_at >= since)
    if desk:
        query = query.filter(MLTradeLog.desk_id == desk)
    if symbol:
        query = query.filter(MLTradeLog.symbol == symbol)
    logs = query.order_by(MLTradeLog.created_at.asc()).all()

    if format == "json":
        data = [{
            "id": l.id, "created_at": l.created_at.isoformat() if l.created_at else None,
            "symbol": l.symbol, "direction": l.direction, "timeframe": l.timeframe,
            "alert_type": l.alert_type, "desk_id": l.desk_id,
            "entry_price": l.entry_price, "sl_pips": l.sl_pips, "tp1_pips": l.tp1_pips,
            "rr_ratio": l.rr_ratio, "session": l.session, "day_of_week": l.day_of_week,
            "hour_utc": l.hour_utc, "atr_value": l.atr_value, "spread_at_entry": l.spread_at_entry,
            "volatility_regime": l.volatility_regime, "ml_score": l.ml_score,
            "consensus_score": l.consensus_score, "consensus_tier": l.consensus_tier,
            "claude_decision": l.claude_decision, "claude_confidence": l.claude_confidence,
            "risk_pct": l.risk_pct, "lot_size": l.lot_size,
            "consecutive_losses": l.consecutive_losses,
            "approved": l.approved, "filter_blocked": l.filter_blocked,
            "block_reason": l.block_reason, "is_oniai": l.is_oniai,
            "outcome": l.outcome, "pnl_pips": l.pnl_pips, "pnl_dollars": l.pnl_dollars,
            "exit_reason": l.exit_reason, "hold_time_minutes": l.hold_time_minutes,
            "max_favorable_pips": l.max_favorable_pips, "max_adverse_pips": l.max_adverse_pips,
            "srv100_pnl_pips": l.srv100_pnl_pips, "srv30_pnl_pips": l.srv30_pnl_pips,
            "mt5_pnl_pips": l.mt5_pnl_pips, "oniai_pnl_pips": l.oniai_pnl_pips,
            "correlation_group": l.correlation_group,
        } for l in logs]
        return JSONResponse(content={"count": len(data), "data": data})

    # CSV
    output = io.StringIO()
    w = csv.writer(output)
    headers = [
        "id","created_at","symbol","direction","timeframe","alert_type","desk_id",
        "entry_price","sl_pips","tp1_pips","rr_ratio","session","day_of_week","hour_utc",
        "atr_value","spread_at_entry","volatility_regime","ml_score","consensus_score",
        "consensus_tier","claude_decision","claude_confidence","risk_pct","lot_size",
        "open_positions_desk","daily_pnl_at_entry","consecutive_losses","size_modifier",
        "approved","filter_blocked","block_reason","is_oniai","outcome","pnl_pips",
        "pnl_dollars","exit_reason","hold_time_minutes","max_favorable_pips",
        "max_adverse_pips","srv100_pnl_pips","srv30_pnl_pips","mt5_pnl_pips",
        "oniai_pnl_pips","correlation_group",
    ]
    w.writerow(headers)
    for l in logs:
        w.writerow([
            l.id, l.created_at.isoformat() if l.created_at else "",
            l.symbol, l.direction, l.timeframe, l.alert_type, l.desk_id,
            l.entry_price, l.sl_pips, l.tp1_pips, l.rr_ratio,
            l.session, l.day_of_week, l.hour_utc, l.atr_value,
            l.spread_at_entry, l.volatility_regime, l.ml_score, l.consensus_score,
            l.consensus_tier, l.claude_decision, l.claude_confidence,
            l.risk_pct, l.lot_size, l.open_positions_desk,
            l.daily_pnl_at_entry, l.consecutive_losses, l.size_modifier,
            l.approved, l.filter_blocked, l.block_reason, l.is_oniai,
            l.outcome, l.pnl_pips, l.pnl_dollars, l.exit_reason,
            l.hold_time_minutes, l.max_favorable_pips, l.max_adverse_pips,
            l.srv100_pnl_pips, l.srv30_pnl_pips, l.mt5_pnl_pips, l.oniai_pnl_pips,
            l.correlation_group,
        ])
    output.seek(0)
    fname = f"oniquant_ml_{days}d_{datetime.now().strftime('%Y%m%d')}.csv"
    return StreamingResponse(
        iter([output.getvalue()]), media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={fname}"},
    )

@router.post("/ml/enrich")
async def batch_enrich(
    limit: int = Query(500, ge=1, le=5000),
    db: Session = Depends(get_db),
    x_api_key: str = Header(None),
):
    """Retroactively enrich ML records with derived features."""
    _verify(x_api_key)
    from app.services.feature_engineer import FeatureEngineer
    count = FeatureEngineer().batch_enrich(db, limit=limit)
    return {"enriched": count}


@router.get("/ml/stats")
async def ml_stats(
    days: int = Query(30, ge=1, le=365),
    db: Session = Depends(get_db),
    x_api_key: str = Header(None),
):
    _verify(x_api_key)
    since = datetime.now(timezone.utc) - timedelta(days=days)
    total = db.query(func.count(MLTradeLog.id)).filter(MLTradeLog.created_at >= since).scalar()
    with_outcome = db.query(func.count(MLTradeLog.id)).filter(
        MLTradeLog.created_at >= since, MLTradeLog.outcome.isnot(None)).scalar()
    wins = db.query(func.count(MLTradeLog.id)).filter(
        MLTradeLog.created_at >= since, MLTradeLog.outcome == "WIN").scalar()
    losses = db.query(func.count(MLTradeLog.id)).filter(
        MLTradeLog.created_at >= since, MLTradeLog.outcome == "LOSS").scalar()
    return {
        "period_days": days, "total_signals": total,
        "with_outcome": with_outcome, "wins": wins, "losses": losses,
        "win_rate": round(wins / with_outcome * 100, 1) if with_outcome > 0 else 0,
    }
```

## `app/services/__init__.py`

```python

```

## `app/services/pipeline.py`

```python
"""
Signal Processing Pipeline — SIMULATION MODE
Orchestrates the full pipeline for validated signals:
Enrichment → ML Scoring → Consensus Scoring → Claude CTO → Simulated Trade

ALL CTO-approved signals become simulated trades in the database.
No execution limits, no max_open blocking. Every approved signal is
tracked and broadcast to Telegram for profitability analysis.

Runs asynchronously after webhook logs the validated signal.
"""
import json
import time
import logging
from datetime import datetime, timezone
from typing import Dict

from sqlalchemy.orm import Session

from app.services.twelvedata_enricher import TwelveDataEnricher
from app.services.ml_scorer import MLScorer
from app.services.consensus_scorer import ConsensusScorer
from app.services.claude_cto import ClaudeCTO
from app.services.risk_filter import HardRiskFilter
from app.services.telegram_bot import TelegramBot
from app.services.price_service import PriceService
from app.models.signal import Signal
from app.config import (
    DESKS, CAPITAL_PER_ACCOUNT, PORTFOLIO_CAPITAL_PER_DESK,
    get_pip_info, calculate_lot_size, get_atr_settings,
    VIX_REGIMES, DESK_DAILY_HARD_STOP_PCT,
)
from app.services.ml_data_logger import MLDataLogger

logger = logging.getLogger("TradingSystem.Pipeline")

# Shared service instances (initialized once)
_enricher: TwelveDataEnricher = None
_ml_scorer: MLScorer = None
_consensus: ConsensusScorer = None
_cto: ClaudeCTO = None
_risk_filter: HardRiskFilter = None
_telegram: TelegramBot = None
_price_service: PriceService = None


def _get_services():
    """Lazy-initialize shared service instances."""
    global _enricher, _ml_scorer, _consensus, _cto, _risk_filter, _telegram, _price_service
    if _enricher is None:
        _enricher = TwelveDataEnricher()
    if _ml_scorer is None:
        _ml_scorer = MLScorer()
    if _consensus is None:
        _consensus = ConsensusScorer()
    if _cto is None:
        _cto = ClaudeCTO()
    if _risk_filter is None:
        _risk_filter = HardRiskFilter()
    if _telegram is None:
        _telegram = TelegramBot()
    if _price_service is None:
        _price_service = PriceService()
    return _enricher, _ml_scorer, _consensus, _cto, _risk_filter, _telegram, _price_service


async def process_signal(signal_id: int, db: Session, webhook_latency_ms: int = None) -> Dict:
    """
    Run the full Phase 2 pipeline on a validated signal.
    Updates the signal record at each stage.

    Args:
        signal_id: ID of the validated signal in the DB.
        db: SQLAlchemy session.
        webhook_latency_ms: Milliseconds between TV alert fire and server arrival.
                           Passed from webhook route for ML training data.

    Pipeline stages:
    1. Load signal from DB
    2. Enrich with TwelveData market data
    2b. Fetch live market price (replaces TV candle close)
    3. Score with ML model
    4. Calculate multi-timeframe consensus
    5. Claude CTO makes final decision
    6. Hard risk filter validates
    7. Update signal record with all results

    Returns pipeline result dict.
    """
    pipeline_start = time.time()
    enricher, ml_scorer, consensus_scorer, cto, risk_filter, telegram, price_service = _get_services()

    # ── 1. Load signal ──
    signal = db.query(Signal).filter(Signal.id == signal_id).first()
    if not signal:
        logger.error(f"Signal {signal_id} not found")
        return {"status": "error", "message": "Signal not found"}

    if signal.status == "REJECTED":
        return {"status": "skipped", "message": "Signal already rejected"}

    signal_data = {
        "symbol": signal.symbol_normalized,
        "timeframe": signal.timeframe,
        "alert_type": signal.alert_type,
        "direction": signal.direction,
        "price": signal.price,
        "tp1": signal.tp1,
        "tp2": signal.tp2,
        "sl1": signal.sl1,
        "sl2": signal.sl2,
        "smart_trail": signal.smart_trail,
        "webhook_latency_ms": webhook_latency_ms,
    }

    desks = signal.desks_matched or []
    if not desks:
        signal.status = "REJECTED"
        signal.validation_errors = ["No desk match for pipeline processing"]
        db.commit()
        return {"status": "rejected", "message": "No desk match"}

    logger.info(
        f"═══ PIPELINE START | Signal #{signal_id} | "
        f"{signal.symbol_normalized} {signal.alert_type} | "
        f"Desks: {desks} ═══"
    )

    # Process for each matched desk
    results = {}

    for desk_id in desks:
        desk_start = time.time()
        signal._ml_trade_id = None

        logger.info(f"── Processing for {desk_id} ──")

        try:
            # ── 2. Enrich with market data ──
            signal.status = "ENRICHING"
            db.commit()

            # Check if this signal came from the MSE (has pre-computed data)
            mse_data = None
            try:
                raw = json.loads(signal.raw_payload) if signal.raw_payload else {}
                mse_data = raw.get("mse")
            except (json.JSONDecodeError, TypeError):
                pass

            if mse_data:
                # MSE signal: use pre-computed technicals, fetch intermarket only
                enrichment = await enricher.enrich_from_mse(
                    signal.symbol_normalized,
                    mse_data,
                    signal.price,
                )
                logger.info(
                    f"MSE fast-path enrichment for {signal.symbol_normalized} "
                    f"(confluence={mse_data.get('confluence_score', '?')})"
                )
            else:
                # Standard LuxAlgo signal: full TwelveData enrichment
                enrichment = await enricher.enrich(
                    signal.symbol_normalized,
                    signal.timeframe,
                    signal.price,
                )

            signal.status = "ENRICHED"
            db.commit()

            # ── 2b. Fetch LIVE market price (replaces TradingView candle close) ──
            try:
                live_price = await price_service.get_price(signal.symbol_normalized)
                if live_price and live_price > 0:
                    logger.info(
                        f"Live price for {signal.symbol_normalized}: {live_price} "
                        f"(TV close was {signal_data.get('price')})"
                    )
                    signal_data["price"] = live_price
                else:
                    logger.warning(
                        f"Live price unavailable for {signal.symbol_normalized}, "
                        f"using TradingView close: {signal_data.get('price')}"
                    )
            except Exception as e:
                logger.warning(f"Live price fetch failed for {signal.symbol_normalized}: {e}")

            # ── 2b. Calculate SL/TP from ATR using desk-specific multipliers ──
            price = signal_data.get("price", 0)
            atr = enrichment.get("atr")
            direction = signal_data.get("direction", "LONG")
            timeframe = signal_data.get("timeframe", "")
            symbol = signal_data.get("symbol", "")

            # Get desk+symbol+timeframe specific ATR settings
            atr_cfg = get_atr_settings(desk_id, symbol, timeframe)
            sl_mult = atr_cfg.get("sl_mult", 2.0)
            tp1_mult = atr_cfg.get("tp1_mult", 4.0)
            tp2_mult = atr_cfg.get("tp2_mult", 6.0)

            if price > 0 and atr and atr > 0:
                if not signal_data.get("sl1"):
                    sl_distance = atr * sl_mult
                    if direction == "LONG":
                        signal_data["sl1"] = round(price - sl_distance, 5)
                    else:
                        signal_data["sl1"] = round(price + sl_distance, 5)
                    logger.info(
                        f"ATR SL | {desk_id} | {sl_mult}x ATR({atr:.5f}) = "
                        f"{signal_data['sl1']} (R:R target 1:{tp1_mult/sl_mult:.1f})"
                    )

                if not signal_data.get("tp1"):
                    tp_distance = atr * tp1_mult
                    if direction == "LONG":
                        signal_data["tp1"] = round(price + tp_distance, 5)
                    else:
                        signal_data["tp1"] = round(price - tp_distance, 5)
                    logger.info(f"ATR TP1 | {desk_id} | {tp1_mult}x ATR = {signal_data['tp1']}")

                if not signal_data.get("tp2"):
                    tp2_distance = atr * tp2_mult
                    if direction == "LONG":
                        signal_data["tp2"] = round(price + tp2_distance, 5)
                    else:
                        signal_data["tp2"] = round(price - tp2_distance, 5)

                # ── R:R floor check — reject if below minimum ──
                min_rr = atr_cfg.get("min_rr", 1.5)
                entry = price
                sl = signal_data.get("sl1", 0)
                tp = signal_data.get("tp1", 0)
                if sl and tp and entry:
                    sl_dist = abs(entry - sl)
                    tp_dist = abs(tp - entry)
                    actual_rr = tp_dist / sl_dist if sl_dist > 0 else 0
                    if actual_rr < min_rr and sl_dist > 0:
                        logger.warning(
                            f"R:R {actual_rr:.2f} below minimum {min_rr} for {desk_id} — "
                            f"adjusting TP1 to meet floor"
                        )
                        # Adjust TP to meet minimum R:R
                        required_tp_dist = sl_dist * min_rr
                        if direction == "LONG":
                            signal_data["tp1"] = round(entry + required_tp_dist, 5)
                        else:
                            signal_data["tp1"] = round(entry - required_tp_dist, 5)

            # ── 2c. VIX regime check — halt momentum/equity desks if VIX too high ──
            vix_halt = DESKS.get(desk_id, {}).get("vix_halt_above")
            if vix_halt:
                vix_data = enrichment.get("intermarket", {}).get("VIX", {})
                vix_price = float(vix_data.get("price", 0)) if vix_data else 0
                if vix_price > vix_halt:
                    signal.status = "REJECTED"
                    signal.validation_errors = [f"VIX {vix_price:.1f} > halt threshold {vix_halt}"]
                    db.commit()
                    results[desk_id] = {
                        "decision": "SKIP", "approved": False,
                        "rejection_reason": f"VIX halt: {vix_price:.1f} > {vix_halt}",
                    }
                    logger.warning(f"VIX HALT | {desk_id} | VIX={vix_price:.1f} > {vix_halt}")
                    continue

            # ── 3. ML model scoring ──
            signal.status = "SCORING"
            db.commit()

            ml_result = ml_scorer.score(signal_data, enrichment, desk_id)
            signal.ml_score = ml_result["ml_score"]

            signal.status = "SCORED"
            db.commit()

            # ── 4. Get recent signals for consensus ──
            recent_signals = risk_filter.get_recent_signals(
                db, signal.symbol_normalized
            )

            # ── 5. Consensus scoring ──
            consensus = consensus_scorer.score(
                signal_data, enrichment, desk_id, ml_result, recent_signals
            )
            signal.consensus_score = consensus["total_score"]

            # ── 6. Get desk state and firm risk ──
            desk_state = risk_filter.get_desk_state(db, desk_id)
            firm_risk = risk_filter.get_firm_risk(db)

            # ── 7. Claude CTO decision ──
            signal.status = "DECIDING"
            db.commit()

            decision = await cto.decide(
                signal_data, enrichment, ml_result,
                consensus, desk_state, firm_risk,
            )

            signal.claude_decision = decision["decision"]
            signal.claude_reasoning = decision.get("reasoning", "")

            # ── 8. Risk flags (advisory only — does NOT block trades) ──
            risk_flags = []
            session_ok, session_msg = risk_filter._check_session(desk_id, DESKS.get(desk_id, {}))
            if not session_ok:
                risk_flags.append(f"session: {session_msg}")

            if db:
                corr_ok, corr_msg = risk_filter._check_correlation(
                    db, signal_data.get("symbol"), signal_data.get("direction")
                )
                if not corr_ok:
                    risk_flags.append(f"correlation: {corr_msg}")

            # Build trade params for ALL CTO-approved signals
            desk = DESKS.get(desk_id, {})
            risk_pct = desk.get("risk_pct", 1.0)
            size_mult = decision.get("size_multiplier", 1.0)
            desk_modifier = desk_state.get("size_modifier", 1.0)
            effective_risk_pct = min(risk_pct * size_mult * desk_modifier, risk_pct)
            risk_dollars = CAPITAL_PER_ACCOUNT * (effective_risk_pct / 100)

            trade_params = {
                "desk_id": desk_id,
                "symbol": signal_data.get("symbol"),
                "direction": signal_data.get("direction"),
                "price": signal_data.get("price"),
                "timeframe": signal_data.get("timeframe"),
                "alert_type": signal_data.get("alert_type"),
                "risk_pct": round(effective_risk_pct, 4),
                "risk_dollars": round(risk_dollars, 2),
                "stop_loss": signal_data.get("sl1"),
                "take_profit_1": signal_data.get("tp1"),
                "take_profit_2": signal_data.get("tp2"),
                "trailing_stop_pips": desk.get("trailing_stop_pips"),
                "max_hold_hours": desk.get("max_hold_hours"),
                "size_multiplier": round(size_mult * desk_modifier, 4),
                "claude_decision": decision.get("decision"),
                "claude_reasoning": decision.get("reasoning"),
                "confidence": decision.get("confidence"),
                "trend": enrichment.get("trend", "UNKNOWN"),
                "rsi": enrichment.get("rsi"),
                "rsi_zone": enrichment.get("rsi_zone", "UNKNOWN"),
                "volatility_regime": enrichment.get("volatility_regime", "UNKNOWN"),
                "ema50": enrichment.get("ema50"),
                "ema200": enrichment.get("ema200"),
                "session": enrichment.get("active_session", "UNKNOWN"),
                "risk_flags": risk_flags,
            }

            # Desk index for unique ticket generation
            desk_idx = desks.index(desk_id) if desk_id in desks else 0

            if decision.get("decision") in ("EXECUTE", "REDUCE"):
                # ── 9. CTO APPROVED → Create simulated trade ──
                signal.status = "SIM_OPEN"
                signal.position_size_pct = trade_params.get("risk_pct")
                signal.desk_id = desk_id

                from app.models.trade import Trade as TradeModel

                # Calculate lot size
                entry_price = signal_data.get("price", 0)
                sl_price = signal_data.get("sl1", 0)
                pip_size, pip_value = get_pip_info(signal_data.get("symbol", ""))
                sl_pips = abs(float(entry_price) - float(sl_price)) / pip_size if entry_price and sl_price and pip_size else 0

                desk_capital = PORTFOLIO_CAPITAL_PER_DESK.get(desk_id, CAPITAL_PER_ACCOUNT)
                lot_size = calculate_lot_size(
                    desk_id=desk_id,
                    symbol=signal_data.get("symbol", ""),
                    risk_pct=effective_risk_pct,
                    sl_pips=sl_pips,
                    account_capital=desk_capital,
                )

                # Unique ticket: 900000 + signal_id * 10 + desk_index
                trade_record = TradeModel(
                    signal_id=signal.id,
                    desk_id=desk_id,
                    symbol=signal_data.get("symbol"),
                    direction=signal_data.get("direction"),
                    mt5_ticket=900000 + signal.id * 10 + desk_idx,
                    entry_price=entry_price,
                    lot_size=lot_size,
                    risk_pct=effective_risk_pct,
                    risk_dollars=risk_dollars,
                    stop_loss=signal_data.get("sl1"),
                    take_profit_1=signal_data.get("tp1"),
                    take_profit_2=signal_data.get("tp2"),
                    status="SIM_OPEN",
                    opened_at=datetime.now(timezone.utc),
                    close_reason="|".join(risk_flags) if risk_flags else None,
                )
                db.add(trade_record)
                db.flush()
                signal._ml_trade_id = trade_record.id

                # ── 10. Telegram notification ──
                await telegram.notify_trade_entry(trade_params, decision)

                logger.info(
                    f"SIM TRADE #{trade_record.id} | {desk_id} | "
                    f"{signal_data.get('symbol')} {signal_data.get('direction')} | "
                    f"Lot: {lot_size} | Risk: ${risk_dollars:.2f} | "
                    f"Flags: {risk_flags if risk_flags else 'none'}"
                )

                approved = True
                rejection_reason = None

            else:
                # ── CTO said SKIP ──
                signal.status = "REJECTED"
                signal.claude_decision = "SKIP"
                signal.claude_reasoning = decision.get("reasoning", "")
                approved = False
                rejection_reason = decision.get("reasoning", "CTO SKIP")

            db.commit()

            # ── ML Training Data: log everything for future model training ──
            try:
                from app.services.ml_data_logger import MLDataLogger
                _ml_logger = MLDataLogger()
                _ml_logger.log_signal(
                    db=db,
                    signal_id=signal.id,
                    signal_data=signal_data,
                    enrichment=enrichment,
                    ml_result=ml_result,
                    consensus=consensus,
                    decision=decision,
                    risk_approved=approved,
                    risk_block_reason=rejection_reason,
                    trade_params=trade_params if approved else {},
                    desk_state=desk_state,
                )
                db.commit()
            except Exception as e:
                logger.debug(f"ML data logging failed: {e}")

            desk_time = int((time.time() - desk_start) * 1000)

            results[desk_id] = {
                "decision": decision["decision"] if approved else "SKIP",
                "approved": approved,
                "rejection_reason": rejection_reason,
                "consensus_score": consensus["total_score"],
                "consensus_tier": consensus["tier"],
                "ml_score": ml_result["ml_score"],
                "ml_method": ml_result["ml_method"],
                "size_multiplier": decision.get("size_multiplier", 0),
                "trade_params": trade_params if approved else None,
                "reasoning": decision.get("reasoning", ""),
                "risk_flags": decision.get("risk_flags", []),
                "enrichment_summary": {
                    "rsi": enrichment.get("rsi"),
                    "volatility_regime": enrichment.get("volatility_regime"),
                    "session": enrichment.get("active_session"),
                    "kill_zone": enrichment.get("kill_zone_type"),
                },
                "processing_time_ms": desk_time,
            }

            logger.info(
                f"── {desk_id} COMPLETE | Decision: {results[desk_id]['decision']} | "
                f"Consensus: {consensus['total_score']} ({consensus['tier']}) | "
                f"ML: {ml_result['ml_score']:.2f} | "
                f"{desk_time}ms ──"
            )

        except Exception as e:
            logger.error(f"Pipeline error for {desk_id}: {e}", exc_info=True)
            signal.status = "ERROR"
            db.commit()
            results[desk_id] = {
                "decision": "SKIP",
                "approved": False,
                "error": str(e),
            }

    # ── Update total processing time ──
    total_ms = int((time.time() - pipeline_start) * 1000)
    signal.processing_time_ms = total_ms
    db.commit()

    logger.info(
        f"═══ PIPELINE COMPLETE | Signal #{signal_id} | "
        f"Total: {total_ms}ms | "
        f"Results: {json.dumps({k: v['decision'] for k, v in results.items()})} ═══"
    )

    return {
        "status": "processed",
        "signal_id": signal_id,
        "results": results,
        "total_processing_time_ms": total_ms,
    }
```

## `app/services/signal_validator.py`

```python
"""
Signal Validator - Validates incoming LuxAlgo alerts.
Checks: alert type validity, desk routing, session windows,
required fields, and basic sanity checks.
"""
import logging
from datetime import datetime, timezone
from typing import List, Tuple, Optional

from app.schemas import TradingViewAlert
from app.config import VALID_ALERT_TYPES, DESKS

logger = logging.getLogger("TradingSystem.Validator")


class SignalValidator:
    """Validates incoming signals before they enter the pipeline."""

    def validate(
        self,
        alert: TradingViewAlert,
        normalized_symbol: str,
        matched_desks: List[str],
    ) -> Tuple[bool, Optional[List[str]]]:
        """
        Run all validation checks. Returns (is_valid, errors).
        """
        errors: List[str] = []

        # 1. Alert type must be recognized
        if alert.alert_type not in VALID_ALERT_TYPES:
            errors.append(
                f"Unknown alert_type '{alert.alert_type}'. "
                f"Valid: {VALID_ALERT_TYPES}"
            )

        # 2. Must route to at least one desk
        if not matched_desks:
            errors.append(
                f"Symbol '{normalized_symbol}' does not match any desk"
            )

        # 3. Price should be positive (pipeline fetches live price as backup)
        if alert.price <= 0 and not matched_desks:
            errors.append(f"Invalid price: {alert.price} and no desk matched")

        # 4. For entry signals, we need at least SL1
        entry_types = {
            "bullish_confirmation", "bearish_confirmation",
            "bullish_plus", "bearish_plus",
            "bullish_confirmation_plus", "bearish_confirmation_plus",
            "contrarian_bullish", "contrarian_bearish",
        }
        # 4. For entry signals, warn if missing SL (pipeline will calculate from ATR)
        if alert.alert_type in entry_types and alert.sl1 is None:
            logger.warning(
                f"Entry signal '{alert.alert_type}' missing sl1 — pipeline will calculate from ATR"
            )

        # 5. SL/TP sanity: SL must be on correct side of price
        if alert.sl1 is not None:
            if "bullish" in alert.alert_type and alert.sl1 >= alert.price:
                errors.append(
                    f"Bullish signal but SL1 ({alert.sl1}) >= price ({alert.price})"
                )
            if "bearish" in alert.alert_type and alert.sl1 <= alert.price:
                errors.append(
                    f"Bearish signal but SL1 ({alert.sl1}) <= price ({alert.price})"
                )

        # 6. TP sanity: TP must be on correct side of price
        if alert.tp1 is not None:
            if "bullish" in alert.alert_type and alert.tp1 <= alert.price:
                errors.append(
                    f"Bullish signal but TP1 ({alert.tp1}) <= price ({alert.price})"
                )
            if "bearish" in alert.alert_type and alert.tp1 >= alert.price:
                errors.append(
                    f"Bearish signal but TP1 ({alert.tp1}) >= price ({alert.price})"
                )

        # 7. Filter desks that don't allow this alert type
        filtered_desks = []
        for desk_id in matched_desks:
            desk = DESKS.get(desk_id)
            if desk and desk.get("alerts") != "ALL":
                allowed = desk.get("alerts", [])
                if alert.alert_type not in allowed:
                    logger.debug(
                        f"Alert '{alert.alert_type}' not in {desk_id} allowed list — skipping desk"
                    )
                    continue
            filtered_desks.append(desk_id)
        matched_desks = filtered_desks

        if not matched_desks:
            errors.append(
                f"Alert '{alert.alert_type}' not allowed by any matched desk"
            )

        # 8. Timeframe sanity (basic - full session check is Phase 2)
        if not alert.timeframe:
            errors.append("Missing timeframe")

        is_valid = len(errors) == 0

        if errors:
            logger.debug(
                f"Validation failed for {normalized_symbol} "
                f"{alert.alert_type}: {errors}"
            )

        return is_valid, errors if errors else None
```

## `app/services/twelvedata_enricher.py`

```python
"""
TwelveData Enrichment Service
Fetches market context for each signal: ATR, RSI, volume profile,
volatility regime, session detection, and intermarket data.
"""
import logging
from datetime import datetime, timezone
from typing import Dict, Optional

import httpx

from app.config import TWELVEDATA_API_KEY

logger = logging.getLogger("TradingSystem.Enricher")

BASE_URL = "https://api.twelvedata.com"

# TwelveData symbol mapping (our internal → TwelveData format)
TD_SYMBOLS = {
    "EURUSD": "EUR/USD",
    "USDJPY": "USD/JPY",
    "GBPUSD": "GBP/USD",
    "USDCHF": "USD/CHF",
    "AUDUSD": "AUD/USD",
    "USDCAD": "USD/CAD",
    "NZDUSD": "NZD/USD",
    "EURJPY": "EUR/JPY",
    "GBPJPY": "GBP/JPY",
    "AUDJPY": "AUD/JPY",
    "XAUUSD": "XAU/USD",
    "BTCUSD": "BTC/USD",
    "ETHUSD": "ETH/USD",
    "US30": "DJI",
    "US100": "IXIC",
    "NAS100": "IXIC",
    "TSLA": "TSLA",
}

# Intermarket instruments for macro context
INTERMARKET = {
    "DXY": "DX",       # Dollar index
    "VIX": "VIX",       # Volatility index
    "US10Y": "TNXF",    # 10Y bond yield
    "OIL": "CL",        # Crude oil
    "BTCDOM": "BTC/USD", # BTC as proxy for crypto sentiment
}

# Timeframe mapping to TwelveData intervals
TD_INTERVALS = {
    "1M": "1min", "5M": "5min", "15M": "15min", "30M": "30min",
    "1H": "1h", "4H": "4h", "D": "1day", "W": "1week",
}


class TwelveDataEnricher:
    """Enriches signals with market data from TwelveData API."""

    def __init__(self):
        self.api_key = TWELVEDATA_API_KEY
        self.client = httpx.AsyncClient(timeout=10.0)

    async def enrich(self, symbol: str, timeframe: str, price: float) -> Dict:
        """
        Fetch market context for a signal. Returns enrichment dict.
        Falls back to empty/default values if API unavailable.
        """
        if not self.api_key:
            logger.warning("No TwelveData API key — returning defaults")
            return self._default_enrichment(symbol, price)

        td_symbol = TD_SYMBOLS.get(symbol, symbol)
        td_interval = TD_INTERVALS.get(timeframe, "1h")

        enrichment = {}

        try:
            # Fetch technical indicators in parallel
            atr_data, rsi_data, adx_data, quote_data, ema50_data, ema200_data = await self._fetch_parallel(
                td_symbol, td_interval
            )

            # ── ATR (Average True Range) — volatility measure ──
            if atr_data and ("atr" in atr_data or "value" in atr_data):
                atr = float(atr_data.get("atr") or atr_data.get("value"))
                enrichment["atr"] = round(atr, 5)
                enrichment["atr_pct"] = round((atr / price) * 100, 4) if price > 0 else None
            else:
                enrichment["atr"] = None
                enrichment["atr_pct"] = None

            # ── RSI — momentum/overbought/oversold ──
            if rsi_data and ("rsi" in rsi_data or "value" in rsi_data):
                rsi = float(rsi_data.get("rsi") or rsi_data.get("value"))
                enrichment["rsi"] = round(rsi, 2)
                enrichment["rsi_zone"] = self._classify_rsi(rsi)
            else:
                enrichment["rsi"] = None
                enrichment["rsi_zone"] = "UNKNOWN"

            # ── ADX (Average Directional Index) — trend strength ──
            if adx_data and ("adx" in adx_data or "value" in adx_data):
                adx_val = float(adx_data.get("adx") or adx_data.get("value"))
                enrichment["adx"] = round(adx_val, 2)
            else:
                enrichment["adx"] = None

            # ── EMA 50 & 200 — trend detection ──
            ema50 = None
            ema200 = None
            if ema50_data and ("ema" in ema50_data or "value" in ema50_data):
                ema50 = float(ema50_data.get("ema") or ema50_data.get("value"))
                enrichment["ema50"] = round(ema50, 5)
            else:
                enrichment["ema50"] = None

            if ema200_data and ("ema" in ema200_data or "value" in ema200_data):
                ema200 = float(ema200_data.get("ema") or ema200_data.get("value"))
                enrichment["ema200"] = round(ema200, 5)
            else:
                enrichment["ema200"] = None

            # ── Trend classification ──
            enrichment["trend"] = self._classify_trend(price, ema50, ema200)

            # ── Quote data — spread and volume ──
            if quote_data:
                enrichment["bid"] = float(quote_data.get("bid", 0) or 0)
                enrichment["ask"] = float(quote_data.get("ask", 0) or 0)
                enrichment["spread"] = round(
                    enrichment["ask"] - enrichment["bid"], 5
                )
                enrichment["volume"] = int(quote_data.get("volume", 0) or 0)
            else:
                enrichment["bid"] = None
                enrichment["ask"] = None
                enrichment["spread"] = None
                enrichment["volume"] = None

        except Exception as e:
            logger.error(f"TwelveData enrichment failed for {symbol}: {e}")
            return self._default_enrichment(symbol, price)

        # ── Volatility regime detection ──
        enrichment["volatility_regime"] = self._detect_volatility_regime(
            enrichment.get("atr_pct")
        )

        # ── Session detection ──
        enrichment["active_session"] = self._detect_session()
        enrichment["is_kill_zone"] = self._is_kill_zone()
        enrichment["kill_zone_type"] = self._kill_zone_type()

        # ── Intermarket snapshot ──
        enrichment["intermarket"] = await self._fetch_intermarket()

        logger.info(
            f"Enriched {symbol}: ATR={enrichment.get('atr')} "
            f"RSI={enrichment.get('rsi')} ADX={enrichment.get('adx')} "
            f"Trend={enrichment.get('trend')} "
            f"Regime={enrichment.get('volatility_regime')} "
            f"Session={enrichment.get('active_session')}"
        )

        return enrichment

    async def _fetch_parallel(self, symbol: str, interval: str):
        """Fetch ATR, RSI, ADX, EMA, and quote data."""
        atr_data = None
        rsi_data = None
        adx_data = None
        quote_data = None
        ema50_data = None
        ema200_data = None

        try:
            resp = await self.client.get(
                f"{BASE_URL}/atr",
                params={
                    "symbol": symbol, "interval": interval,
                    "time_period": 14, "apikey": self.api_key,
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                if "values" in data and data["values"]:
                    atr_data = data["values"][0]
                    logger.debug(f"ATR raw keys: {list(atr_data.keys())}")
                elif "status" in data and data["status"] == "error":
                    logger.warning(f"ATR API error: {data.get('message', 'unknown')}")
        except Exception as e:
            logger.debug(f"ATR fetch failed: {e}")

        try:
            resp = await self.client.get(
                f"{BASE_URL}/rsi",
                params={
                    "symbol": symbol, "interval": interval,
                    "time_period": 14, "apikey": self.api_key,
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                if "values" in data and data["values"]:
                    rsi_data = data["values"][0]
                    logger.debug(f"RSI raw keys: {list(rsi_data.keys())}")
                elif "status" in data and data["status"] == "error":
                    logger.warning(f"RSI API error: {data.get('message', 'unknown')}")
        except Exception as e:
            logger.debug(f"RSI fetch failed: {e}")

        try:
            resp = await self.client.get(
                f"{BASE_URL}/adx",
                params={
                    "symbol": symbol, "interval": interval,
                    "time_period": 14, "apikey": self.api_key,
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                if "values" in data and data["values"]:
                    adx_data = data["values"][0]
                    logger.debug(f"ADX raw keys: {list(adx_data.keys())}")
        except Exception as e:
            logger.debug(f"ADX fetch failed: {e}")

        try:
            resp = await self.client.get(
                f"{BASE_URL}/ema",
                params={
                    "symbol": symbol, "interval": interval,
                    "time_period": 50, "apikey": self.api_key,
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                if "values" in data and data["values"]:
                    ema50_data = data["values"][0]
        except Exception as e:
            logger.debug(f"EMA50 fetch failed: {e}")

        try:
            resp = await self.client.get(
                f"{BASE_URL}/ema",
                params={
                    "symbol": symbol, "interval": interval,
                    "time_period": 200, "apikey": self.api_key,
                },
            )
            if resp.status_code == 200:
                data = resp.json()
                if "values" in data and data["values"]:
                    ema200_data = data["values"][0]
        except Exception as e:
            logger.debug(f"EMA200 fetch failed: {e}")

        try:
            resp = await self.client.get(
                f"{BASE_URL}/quote",
                params={"symbol": symbol, "apikey": self.api_key},
            )
            if resp.status_code == 200:
                quote_data = resp.json()
        except Exception as e:
            logger.debug(f"Quote fetch failed: {e}")

        return atr_data, rsi_data, adx_data, quote_data, ema50_data, ema200_data

    async def _fetch_intermarket(self) -> Dict:
        """Fetch intermarket snapshot: DXY, VIX, bonds, oil."""
        result = {}
        if not self.api_key:
            return result

        for name, symbol in INTERMARKET.items():
            try:
                resp = await self.client.get(
                    f"{BASE_URL}/quote",
                    params={"symbol": symbol, "apikey": self.api_key},
                )
                if resp.status_code == 200:
                    data = resp.json()
                    result[name] = {
                        "price": float(data.get("close", 0) or 0),
                        "change_pct": float(data.get("percent_change", 0) or 0),
                    }
            except Exception:
                result[name] = None

        return result

    def _classify_rsi(self, rsi: float) -> str:
        """Classify RSI into trading zones."""
        if rsi >= 80:
            return "EXTREME_OVERBOUGHT"
        if rsi >= 70:
            return "OVERBOUGHT"
        if rsi >= 55:
            return "BULLISH"
        if rsi >= 45:
            return "NEUTRAL"
        if rsi >= 30:
            return "BEARISH"
        if rsi >= 20:
            return "OVERSOLD"
        return "EXTREME_OVERSOLD"

    def _classify_trend(self, price: float, ema50: Optional[float], ema200: Optional[float]) -> str:
        """
        Classify trend from price vs EMA 50/200.
        STRONG_UP:   Price > EMA50 > EMA200
        UP:          Price > EMA50, EMA50 < EMA200
        WEAK_UP:     Price > EMA200, Price < EMA50
        STRONG_DOWN: Price < EMA50 < EMA200
        DOWN:        Price < EMA50, EMA50 > EMA200
        WEAK_DOWN:   Price < EMA200, Price > EMA50
        RANGING:     Price near both EMAs
        """
        if not ema50 and not ema200:
            return "UNKNOWN"

        if ema50 and ema200:
            if price > ema50 > ema200:
                return "STRONG_UP"
            elif price < ema50 < ema200:
                return "STRONG_DOWN"
            elif price > ema50 and ema50 < ema200:
                return "UP"
            elif price < ema50 and ema50 > ema200:
                return "DOWN"
            elif price > ema200 and price < ema50:
                return "WEAK_UP"
            elif price < ema200 and price > ema50:
                return "WEAK_DOWN"
            else:
                return "RANGING"
        elif ema50:
            if price > ema50:
                return "UP"
            else:
                return "DOWN"

        return "UNKNOWN"

    def _detect_volatility_regime(self, atr_pct: Optional[float]) -> str:
        """
        Classify market volatility regime from ATR as % of price.
        Thresholds calibrated for forex (adjust per asset class).
        """
        if atr_pct is None:
            return "UNKNOWN"
        if atr_pct > 1.5:
            return "HIGH_VOLATILITY"
        if atr_pct > 0.5:
            return "TRENDING"
        if atr_pct > 0.2:
            return "NORMAL"
        return "LOW_VOLATILITY"

    def _detect_session(self) -> str:
        """Detect which trading session is currently active."""
        now = datetime.now(timezone.utc)
        hour = now.hour

        # UTC-based session windows
        # Sydney: 21:00-06:00 UTC
        # Tokyo:  00:00-09:00 UTC
        # London: 07:00-16:00 UTC
        # NY:     12:00-21:00 UTC

        sessions = []
        if 7 <= hour < 16:
            sessions.append("LONDON")
        if 12 <= hour < 21:
            sessions.append("NEW_YORK")
        if hour >= 21 or hour < 6:
            sessions.append("SYDNEY")
        if 0 <= hour < 9:
            sessions.append("TOKYO")

        if "LONDON" in sessions and "NEW_YORK" in sessions:
            return "LONDON_NY_OVERLAP"
        if sessions:
            return sessions[0]
        return "OFF_HOURS"

    def _is_kill_zone(self) -> bool:
        """Check if we're in a kill zone (high-probability window)."""
        kz_type = self._kill_zone_type()
        return kz_type != "NONE"

    def _kill_zone_type(self) -> str:
        """
        Identify specific kill zone.
        London Open: 07:00-08:00 UTC
        NY Open: 12:00-13:00 UTC
        Overlap: 12:00-16:00 UTC
        """
        now = datetime.now(timezone.utc)
        hour = now.hour
        minute = now.minute

        # London/NY overlap (strongest)
        if 12 <= hour < 16:
            return "OVERLAP"
        # London open first 60 minutes
        if hour == 7 or (hour == 8 and minute < 30):
            return "LONDON_OPEN"
        # NY open first 60 minutes
        if hour == 12 or (hour == 13 and minute < 30):
            return "NY_OPEN"
        return "NONE"

    def _default_enrichment(self, symbol: str, price: float) -> Dict:
        """Return default enrichment when API is unavailable."""
        return {
            "atr": None,
            "atr_pct": None,
            "rsi": None,
            "rsi_zone": "UNKNOWN",
            "adx": None,
            "ema50": None,
            "ema200": None,
            "trend": "UNKNOWN",
            "bid": None,
            "ask": None,
            "spread": None,
            "volume": None,
            "volatility_regime": "UNKNOWN",
            "active_session": self._detect_session(),
            "is_kill_zone": self._is_kill_zone(),
            "kill_zone_type": self._kill_zone_type(),
            "intermarket": {},
        }

    async def enrich_from_mse(self, symbol: str, mse_data: Dict, price: float) -> Dict:
        """
        Build enrichment from MSE pre-computed data + intermarket-only fetch.
        Skips ATR, RSI, EMA, volume API calls — those are already in mse_data.
        Only fetches DXY, VIX, US10Y, OIL (what Pine Script can't see).

        Saves 3-4 TwelveData API credits and ~2 seconds per signal.
        """
        enrichment = {}

        try:
            # ── Technical data from MSE (no API calls needed) ──
            atr = mse_data.get("atr")
            rsi = mse_data.get("rsi")

            enrichment["atr"] = round(float(atr), 5) if atr is not None else None
            enrichment["atr_pct"] = round((float(atr) / price) * 100, 4) if atr and price else None
            enrichment["rsi"] = round(float(rsi), 2) if rsi is not None else None
            enrichment["rsi_zone"] = self._classify_rsi(float(rsi)) if rsi is not None else "UNKNOWN"

            # ADX from MSE
            adx = mse_data.get("adx")
            enrichment["adx"] = round(float(adx), 2) if adx is not None else None

            # EMA values aren't raw numbers from MSE — but we have the classification
            ema_slope = mse_data.get("ema50_slope", "flat")
            ema200_pos = mse_data.get("ema200_pos", "unknown")
            enrichment["ema50"] = None   # Raw value not available from MSE
            enrichment["ema200"] = None  # Raw value not available from MSE

            # Map MSE trend data to pipeline's trend classification
            htf_trend = mse_data.get("htf_trend", "neutral")
            if ema200_pos == "above" and ema_slope == "rising":
                enrichment["trend"] = "STRONG_UP"
            elif ema200_pos == "above":
                enrichment["trend"] = "UP"
            elif ema200_pos == "below" and ema_slope == "falling":
                enrichment["trend"] = "STRONG_DOWN"
            elif ema200_pos == "below":
                enrichment["trend"] = "DOWN"
            else:
                enrichment["trend"] = "RANGING"

            # Quote data — MSE doesn't have bid/ask spread
            enrichment["bid"] = None
            enrichment["ask"] = None
            enrichment["spread"] = None
            enrichment["volume"] = None

            # Volatility regime from MSE's regime classification
            mse_regime = mse_data.get("regime", "unknown")
            if "volatile" in mse_regime:
                enrichment["volatility_regime"] = "HIGH_VOLATILITY"
            elif "trending" in mse_regime:
                enrichment["volatility_regime"] = "TRENDING"
            elif "quiet" in mse_regime:
                enrichment["volatility_regime"] = "NORMAL"
            else:
                enrichment["volatility_regime"] = self._detect_volatility_regime(
                    enrichment.get("atr_pct")
                )

            # Session from MSE (already detected in Pine Script)
            mse_session = mse_data.get("session", "none")
            session_map = {
                "london_killzone": "LONDON",
                "ny_killzone": "NEW_YORK",
                "london_ny_overlap": "LONDON_NY_OVERLAP",
                "asian": "TOKYO",
                "off_session": "OFF_HOURS",
            }
            enrichment["active_session"] = session_map.get(mse_session, self._detect_session())
            enrichment["is_kill_zone"] = mse_session not in ("off_session", "none", "asian")
            kill_zone_map = {
                "london_killzone": "LONDON_OPEN",
                "ny_killzone": "NY_OPEN",
                "london_ny_overlap": "OVERLAP",
            }
            enrichment["kill_zone_type"] = kill_zone_map.get(mse_session, "NONE")

            # ── MSE confluence score as extra metadata ──
            enrichment["mse_confluence"] = mse_data.get("confluence_score")
            enrichment["mse_adx"] = mse_data.get("adx")
            enrichment["mse_rvol"] = mse_data.get("rvol")
            enrichment["mse_macd_hist"] = mse_data.get("macd_hist")
            enrichment["mse_htf_trend"] = htf_trend
            enrichment["mse_regime"] = mse_regime

        except Exception as e:
            logger.error(f"MSE enrichment mapping failed: {e}")
            # Fall through to intermarket — don't lose everything

        # ── Intermarket: the ONLY API calls we make ──
        enrichment["intermarket"] = await self._fetch_intermarket()

        logger.info(
            f"MSE-enriched {symbol}: RSI={enrichment.get('rsi')} "
            f"Trend={enrichment.get('trend')} "
            f"Regime={enrichment.get('volatility_regime')} "
            f"Session={enrichment.get('active_session')} "
            f"Confluence={enrichment.get('mse_confluence')} "
            f"[intermarket-only fetch — saved ~4 TD credits]"
        )

        return enrichment

    async def fetch_intermarket_only(self) -> Dict:
        """
        Fetch ONLY intermarket data (DXY, VIX, bonds, oil).
        For use when technical data is already available from another source.
        Returns the same format as the intermarket section of full enrichment.
        """
        return await self._fetch_intermarket()

    async def close(self):
        """Close the HTTP client."""
        await self.client.aclose()
```

## `app/services/ml_scorer.py`

```python
"""
ML Signal Scorer
Trains on historical trade data from PostgreSQL. Outputs a probability
score (0.0–1.0) for each incoming signal. Retrains weekly.

Before enough trade history exists, uses a rule-based fallback scorer.
"""
import os
import json
import pickle
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Tuple

import numpy as np

from app.config import DESKS, SCORE_WEIGHTS

logger = logging.getLogger("TradingSystem.MLScorer")

MODEL_PATH = os.getenv("ML_MODEL_PATH", "/tmp/signal_model.pkl")
MIN_TRAINING_SAMPLES = 100  # need at least this many closed trades to train


class MLScorer:
    """
    Scores incoming signals with a probability of success.
    Uses scikit-learn RandomForest when trained, rule-based fallback otherwise.
    """

    def __init__(self):
        self.model = None
        self.feature_names = []
        self.is_trained = False
        self._load_model()

    def score(self, signal_data: Dict, enrichment: Dict, desk_id: str) -> Dict:
        """
        Score a signal. Returns dict with:
        - ml_score: float 0.0–1.0 (probability of profitable trade)
        - ml_features: dict of features used
        - ml_method: "model" or "rule_based"
        """
        features = self._extract_features(signal_data, enrichment, desk_id)

        if self.is_trained and self.model is not None:
            return self._model_score(features)
        else:
            return self._rule_based_score(features, signal_data, enrichment)

    def _extract_features(
        self, signal_data: Dict, enrichment: Dict, desk_id: str
    ) -> Dict:
        """Extract feature vector from signal + enrichment data."""
        desk = DESKS.get(desk_id, {})

        features = {
            # ── Signal features ──
            "is_bullish": 1.0 if signal_data.get("direction") == "LONG" else 0.0,
            "is_bearish": 1.0 if signal_data.get("direction") == "SHORT" else 0.0,
            "is_plus_signal": 1.0 if "plus" in signal_data.get("alert_type", "") else 0.0,
            "is_confirmation_turn": 1.0 if "confirmation_turn" in signal_data.get("alert_type", "") else 0.0,
            "is_contrarian": 1.0 if "contrarian" in signal_data.get("alert_type", "") else 0.0,

            # ── Risk/reward features ──
            "rr_ratio": self._calc_rr_ratio(signal_data),
            "sl_distance_pct": self._calc_sl_distance_pct(signal_data),

            # ── Market context features ──
            "rsi": enrichment.get("rsi", 50.0) or 50.0,
            "rsi_extreme": self._rsi_extreme_score(enrichment.get("rsi")),
            "adx": enrichment.get("adx", 20.0) or 20.0,
            "atr_pct": enrichment.get("atr_pct", 0.5) or 0.5,
            "spread": enrichment.get("spread", 0) or 0,
            "volume": enrichment.get("volume", 0) or 0,

            # ── Session features ──
            "is_kill_zone": 1.0 if enrichment.get("is_kill_zone") else 0.0,
            "is_overlap": 1.0 if enrichment.get("kill_zone_type") == "OVERLAP" else 0.0,
            "is_london": 1.0 if "LONDON" in enrichment.get("active_session", "") else 0.0,
            "is_ny": 1.0 if "NEW_YORK" in enrichment.get("active_session", "") else 0.0,

            # ── Volatility regime ──
            "regime_trending": 1.0 if enrichment.get("volatility_regime") == "TRENDING" else 0.0,
            "regime_high_vol": 1.0 if enrichment.get("volatility_regime") == "HIGH_VOLATILITY" else 0.0,
            "regime_low_vol": 1.0 if enrichment.get("volatility_regime") == "LOW_VOLATILITY" else 0.0,

            # ── Desk features ──
            "desk_risk_pct": desk.get("risk_pct", 1.0),

            # ── Intermarket features ──
            "dxy_change": self._get_intermarket_change(enrichment, "DXY"),
            "vix_level": self._get_intermarket_price(enrichment, "VIX"),
        }

        return features

    def _model_score(self, features: Dict) -> Dict:
        """Score using trained sklearn model."""
        try:
            # Build feature array in training order
            X = np.array(
                [[features.get(f, 0.0) for f in self.feature_names]]
            )
            proba = self.model.predict_proba(X)[0]
            # proba[1] = probability of class 1 (profitable trade)
            score = float(proba[1]) if len(proba) > 1 else float(proba[0])

            return {
                "ml_score": round(score, 4),
                "ml_features": features,
                "ml_method": "model",
            }
        except Exception as e:
            logger.error(f"Model scoring failed, falling back to rules: {e}")
            return self._rule_based_score(features, {}, {})

    def _rule_based_score(
        self, features: Dict, signal_data: Dict, enrichment: Dict
    ) -> Dict:
        """
        Rule-based scoring when model isn't trained yet.
        Outputs a probability-like score from 0.0 to 1.0.
        """
        score = 0.50  # baseline

        # Plus signals are stronger
        if features["is_plus_signal"]:
            score += 0.10
        if features["is_confirmation_turn"]:
            score += 0.08

        # Good risk/reward
        rr = features["rr_ratio"]
        if rr >= 3.0:
            score += 0.10
        elif rr >= 2.0:
            score += 0.05
        elif rr < 1.0:
            score -= 0.15

        # Kill zone bonus
        if features["is_overlap"]:
            score += 0.08
        elif features["is_kill_zone"]:
            score += 0.04

        # RSI alignment
        if features["is_bullish"] and features["rsi"] < 30:
            score += 0.07  # oversold + bullish = good
        elif features["is_bearish"] and features["rsi"] > 70:
            score += 0.07  # overbought + bearish = good
        elif features["is_bullish"] and features["rsi"] > 80:
            score -= 0.10  # overbought + bullish = risky
        elif features["is_bearish"] and features["rsi"] < 20:
            score -= 0.10  # oversold + bearish = risky

        # Volatility regime
        if features["regime_trending"]:
            score += 0.05  # trending markets favor trend signals
        if features["regime_high_vol"]:
            score -= 0.05  # high vol = wider stops, less predictable
        if features["regime_low_vol"]:
            score -= 0.03  # low vol = less opportunity

        # ADX trend strength
        adx = features.get("adx", 20.0)
        if adx >= 30:
            score += 0.06  # strong trend = higher probability
        elif adx >= 25:
            score += 0.03  # moderate trend
        elif adx < 18:
            score -= 0.04  # ranging market = lower probability

        # Contrarian signals are inherently riskier
        if features["is_contrarian"]:
            score -= 0.05

        # VIX elevated = general market fear
        vix = features.get("vix_level", 0)
        if vix > 30:
            score -= 0.05
        elif vix > 40:
            score -= 0.10

        # Clamp to 0.0–1.0
        score = max(0.0, min(1.0, score))

        return {
            "ml_score": round(score, 4),
            "ml_features": features,
            "ml_method": "rule_based",
        }

    # ────────────────────────────────────────
    # Training (called weekly by scheduler)
    # ────────────────────────────────────────

    def train(self, db_session) -> Dict:
        """
        Train model on historical trade data.
        Called by the weekly retraining scheduler.
        Returns training metrics.
        """
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.model_selection import cross_val_score
        from app.models.trade import Trade
        from app.models.signal import Signal

        # Fetch closed trades with their signals
        trades = (
            db_session.query(Trade)
            .filter(Trade.status == "CLOSED")
            .filter(Trade.pnl_dollars.isnot(None))
            .all()
        )

        if len(trades) < MIN_TRAINING_SAMPLES:
            logger.info(
                f"Only {len(trades)} closed trades — need {MIN_TRAINING_SAMPLES} to train"
            )
            return {
                "status": "insufficient_data",
                "samples": len(trades),
                "required": MIN_TRAINING_SAMPLES,
            }

        # Build training data
        X_rows = []
        y_labels = []

        for trade in trades:
            signal = (
                db_session.query(Signal)
                .filter(Signal.id == trade.signal_id)
                .first()
            )
            if not signal:
                continue

            # Reconstruct features from stored signal data
            signal_data = {
                "direction": signal.direction,
                "alert_type": signal.alert_type,
                "price": signal.price,
                "tp1": signal.tp1,
                "sl1": signal.sl1,
            }

            # Use stored enrichment if available, otherwise defaults
            enrichment = {}
            if signal.raw_payload:
                try:
                    payload = json.loads(signal.raw_payload)
                    enrichment = payload.get("enrichment", {})
                except (json.JSONDecodeError, AttributeError):
                    pass

            features = self._extract_features(
                signal_data, enrichment, trade.desk_id
            )
            X_rows.append(features)

            # Label: 1 = profitable, 0 = loss
            y_labels.append(1 if trade.pnl_dollars > 0 else 0)

        if not X_rows:
            return {"status": "no_valid_samples"}

        # Build numpy arrays
        self.feature_names = sorted(X_rows[0].keys())
        X = np.array(
            [[row.get(f, 0.0) for f in self.feature_names] for row in X_rows]
        )
        y = np.array(y_labels)

        # Train Random Forest
        model = RandomForestClassifier(
            n_estimators=200,
            max_depth=10,
            min_samples_leaf=5,
            random_state=42,
            class_weight="balanced",
        )
        model.fit(X, y)

        # Cross-validation score
        cv_scores = cross_val_score(model, X, y, cv=5, scoring="accuracy")

        # Save model
        self.model = model
        self.is_trained = True
        self._save_model()

        # Feature importance
        importance = dict(
            zip(self.feature_names, model.feature_importances_)
        )
        top_features = sorted(
            importance.items(), key=lambda x: x[1], reverse=True
        )[:10]

        metrics = {
            "status": "trained",
            "samples": len(X),
            "positive_rate": round(sum(y) / len(y), 4),
            "cv_accuracy_mean": round(float(cv_scores.mean()), 4),
            "cv_accuracy_std": round(float(cv_scores.std()), 4),
            "top_features": top_features,
            "trained_at": datetime.now(timezone.utc).isoformat(),
        }

        logger.info(
            f"ML Model trained: {metrics['samples']} samples, "
            f"CV accuracy: {metrics['cv_accuracy_mean']:.1%} "
            f"± {metrics['cv_accuracy_std']:.1%}"
        )

        return metrics

    # ────────────────────────────────────────
    # Helper calculations
    # ────────────────────────────────────────

    def _calc_rr_ratio(self, signal: Dict) -> float:
        """Calculate risk/reward ratio from signal levels."""
        price = signal.get("price", 0)
        tp1 = signal.get("tp1")
        sl1 = signal.get("sl1")
        if not price or not tp1 or not sl1:
            return 1.0
        risk = abs(price - sl1)
        reward = abs(tp1 - price)
        if risk == 0:
            return 0.0
        return round(reward / risk, 2)

    def _calc_sl_distance_pct(self, signal: Dict) -> float:
        """SL distance as percentage of price."""
        price = signal.get("price", 0)
        sl1 = signal.get("sl1")
        if not price or not sl1 or price == 0:
            return 0.0
        return round(abs(price - sl1) / price * 100, 4)

    def _rsi_extreme_score(self, rsi: Optional[float]) -> float:
        """How extreme is RSI? 0 = neutral, 1 = very extreme."""
        if rsi is None:
            return 0.0
        return round(abs(rsi - 50) / 50, 2)

    def _get_intermarket_change(self, enrichment: Dict, key: str) -> float:
        """Get percentage change for an intermarket instrument."""
        im = enrichment.get("intermarket", {})
        if im and key in im and im[key]:
            return float(im[key].get("change_pct", 0))
        return 0.0

    def _get_intermarket_price(self, enrichment: Dict, key: str) -> float:
        """Get current price for an intermarket instrument."""
        im = enrichment.get("intermarket", {})
        if im and key in im and im[key]:
            return float(im[key].get("price", 0))
        return 0.0

    # ────────────────────────────────────────
    # Model persistence
    # ────────────────────────────────────────

    def _save_model(self):
        """Persist model to disk."""
        try:
            with open(MODEL_PATH, "wb") as f:
                pickle.dump(
                    {
                        "model": self.model,
                        "feature_names": self.feature_names,
                        "saved_at": datetime.now(timezone.utc).isoformat(),
                    },
                    f,
                )
            logger.info(f"Model saved to {MODEL_PATH}")
        except Exception as e:
            logger.error(f"Failed to save model: {e}")

    def _load_model(self):
        """Load model from disk if available."""
        try:
            if os.path.exists(MODEL_PATH):
                with open(MODEL_PATH, "rb") as f:
                    data = pickle.load(f)
                self.model = data["model"]
                self.feature_names = data["feature_names"]
                self.is_trained = True
                logger.info(
                    f"Loaded ML model from {MODEL_PATH} "
                    f"(saved: {data.get('saved_at', 'unknown')})"
                )
        except Exception as e:
            logger.warning(f"Could not load model: {e}")
            self.is_trained = False
```

## `app/services/consensus_scorer.py`

```python
"""
Consensus Scoring Engine v5.9 — Institutional Floor Scoring
Produces a score: HIGH (8+), MEDIUM (5-7), LOW (3-4), SKIP (<3).

Changes from v5.8:
- Plus signals score +2 (doubled from +1)
- Kill zone overlap scores +3 (up from +2)
- HTF conflict scores -4 (up from -3)
- RSI alignment +2/-2 (up from +1/-1)
- NEW: ADX trending bonus (+1), ranging penalty (-1)
- NEW: VIX elevated penalty (-2) for indices/equities
- NEW: VIX gold bullish bonus (+1) when VIX > 25
- NEW: DXY headwind penalty (-1) for USD pairs
- NEW: Multi-analyst agreement bonus (+2)
- Thresholds raised: HIGH=8, MEDIUM=5, LOW=3 (pre-filtering makes signals higher quality)
"""
import logging
from typing import Dict, List, Optional

from app.config import DESKS, SCORE_WEIGHTS, SCORE_THRESHOLDS, VIX_REGIMES

logger = logging.getLogger("TradingSystem.Consensus")


class ConsensusScorer:

    def score(
        self,
        signal_data: Dict,
        enrichment: Dict,
        desk_id: str,
        ml_result: Dict,
        recent_signals: Optional[List[Dict]] = None,
    ) -> Dict:
        desk = DESKS.get(desk_id, {})
        breakdown = {}
        total = 0

        alert_type = signal_data.get("alert_type", "")

        # ── 1. Entry trigger — differentiate normal vs plus ──
        entry_types = {
            "bullish_confirmation", "bearish_confirmation",
            "bullish_plus", "bearish_plus",
            "bullish_confirmation_plus", "bearish_confirmation_plus",
            "contrarian_bullish", "contrarian_bearish",
        }
        if alert_type in entry_types:
            if "plus" in alert_type:
                breakdown["entry_trigger"] = SCORE_WEIGHTS.get("entry_trigger_plus", 2)
            else:
                breakdown["entry_trigger"] = SCORE_WEIGHTS.get("entry_trigger_normal", 1)
            total += breakdown["entry_trigger"]

        # ── 1b. LuxAlgo provided SL and TP (+1) ──
        if signal_data.get("sl1") and signal_data.get("tp1"):
            breakdown["defined_risk"] = SCORE_WEIGHTS.get("defined_sl_tp", 1)
            total += breakdown["defined_risk"]

        # ── 2. Confirmation Turn+ bonus ──
        if alert_type == "confirmation_turn_plus":
            breakdown["confirmation_turn_plus"] = SCORE_WEIGHTS.get("confirmation_turn_plus", 2)
            total += breakdown["confirmation_turn_plus"]

        if recent_signals:
            for rs in recent_signals:
                if (
                    rs.get("symbol") == signal_data.get("symbol")
                    and rs.get("alert_type") == "confirmation_turn_plus"
                    and "confirmation_turn_plus" not in breakdown
                ):
                    breakdown["confirmation_turn_plus"] = SCORE_WEIGHTS.get("confirmation_turn_plus", 2)
                    total += breakdown["confirmation_turn_plus"]
                    break

        # ── 3. Timeframe alignment ──
        tf_scores = self._score_timeframe_alignment(signal_data, desk, recent_signals)
        breakdown.update(tf_scores)
        total += sum(tf_scores.values())

        # ── 4. Kill Zone bonuses ──
        kz_type = enrichment.get("kill_zone_type", "NONE")
        if kz_type == "OVERLAP":
            breakdown["kill_zone_overlap"] = SCORE_WEIGHTS.get("kill_zone_overlap", 3)
            total += breakdown["kill_zone_overlap"]
        elif kz_type in ("LONDON_OPEN", "NY_OPEN"):
            breakdown["kill_zone_single"] = SCORE_WEIGHTS.get("kill_zone_single", 1)
            total += breakdown["kill_zone_single"]

        # ── 5. ML classifier confirmation ──
        ml_score = ml_result.get("ml_score", 0.5)
        if ml_score >= 0.65:
            breakdown["ml_confirm"] = SCORE_WEIGHTS.get("ml_confirm_per_tf", 1)
            total += breakdown["ml_confirm"]
        if ml_score >= 0.80:
            breakdown["ml_confirm_strong"] = SCORE_WEIGHTS.get("ml_confirm_per_tf", 1)
            total += breakdown["ml_confirm_strong"]

        # ── 6. Correlation confirmation ──
        corr_score = self._check_correlation(signal_data, enrichment, recent_signals)
        if corr_score > 0:
            breakdown["correlation_confirm"] = corr_score
            total += corr_score

        # ── 7. Liquidity sweep detection ──
        if self._detect_liquidity_sweep(signal_data, enrichment):
            breakdown["liquidity_sweep"] = SCORE_WEIGHTS.get("liquidity_sweep", 3)
            total += breakdown["liquidity_sweep"]

        # ── 8. HTF conflict ──
        if self._check_htf_conflict(signal_data, desk, recent_signals):
            breakdown["conflicting_htf"] = SCORE_WEIGHTS.get("conflicting_htf", -4)
            total += breakdown["conflicting_htf"]

        # ── 9. RSI alignment (v5.9: stronger weights) ──
        rsi_bonus = self._check_rsi_alignment(signal_data, enrichment)
        if rsi_bonus != 0:
            breakdown["rsi_alignment"] = rsi_bonus
            total += rsi_bonus

        # ── 10. NEW: ADX trend strength ──
        adx = enrichment.get("adx")
        if adx is not None:
            if adx >= 25:
                breakdown["adx_trending"] = SCORE_WEIGHTS.get("adx_trending", 1)
                total += breakdown["adx_trending"]
            elif adx < 20:
                breakdown["adx_ranging"] = SCORE_WEIGHTS.get("adx_ranging", -1)
                total += breakdown["adx_ranging"]

        # ── 11. NEW: VIX regime for indices/equities/momentum ──
        vix_level = 0
        intermarket = enrichment.get("intermarket", {})
        vix_data = intermarket.get("VIX", {})
        if vix_data and vix_data.get("price"):
            vix_level = float(vix_data["price"])

        if vix_level > VIX_REGIMES.get("ELEVATED", 25):
            if desk_id in ("DESK5_ALTS", "DESK6_EQUITIES"):
                breakdown["vix_elevated"] = SCORE_WEIGHTS.get("vix_elevated", -2)
                total += breakdown["vix_elevated"]

            # Gold gets a bonus when VIX is high (safe-haven)
            if desk_id == "DESK4_GOLD" and signal_data.get("direction") == "LONG":
                breakdown["vix_gold_bullish"] = SCORE_WEIGHTS.get("vix_gold_bullish", 1)
                total += breakdown["vix_gold_bullish"]

        # ── 12. NEW: DXY headwind for USD pairs ──
        dxy_headwind = self._check_dxy_headwind(signal_data, enrichment)
        if dxy_headwind:
            breakdown["dxy_headwind"] = SCORE_WEIGHTS.get("dxy_headwind", -1)
            total += breakdown["dxy_headwind"]

        # ── 13. NEW: Multi-analyst agreement ──
        # If another signal on the same symbol from a different TF agrees in direction
        if recent_signals:
            agreement = self._check_multi_analyst_agreement(signal_data, desk_id, recent_signals)
            if agreement:
                breakdown["multi_analyst_agree"] = SCORE_WEIGHTS.get("multi_analyst_agree", 2)
                total += breakdown["multi_analyst_agree"]

        # ── Determine tier ──
        if total >= SCORE_THRESHOLDS["HIGH"]:
            tier, size_mult = "HIGH", 1.0
        elif total >= SCORE_THRESHOLDS["MEDIUM"]:
            tier, size_mult = "MEDIUM", 0.6
        elif total >= SCORE_THRESHOLDS["LOW"]:
            tier, size_mult = "LOW", 0.3
        else:
            tier, size_mult = "SKIP", 0.0

        result = {
            "total_score": total,
            "tier": tier,
            "size_multiplier": size_mult,
            "breakdown": breakdown,
            "desk_id": desk_id,
            "ml_score": ml_score,
            "vix_level": vix_level,
        }

        logger.info(
            f"Consensus {desk_id} | {signal_data.get('symbol')} | "
            f"Score: {total} ({tier}) | Size: {size_mult*100:.0f}% | "
            f"Breakdown: {breakdown}"
        )

        return result

    # ─────────────────────────────────────────────────
    # HELPER METHODS
    # ─────────────────────────────────────────────────

    def _score_timeframe_alignment(self, signal_data, desk, recent_signals):
        scores = {}
        direction = signal_data.get("direction")
        desk_tfs = desk.get("timeframes", {})
        if not recent_signals or not direction:
            return scores

        bias_tf = desk_tfs.get("bias", "")
        conf_tf = desk_tfs.get("confirmation", "")

        for rs in recent_signals:
            if rs.get("symbol") != signal_data.get("symbol"):
                continue
            rs_dir = rs.get("direction")
            rs_tf = rs.get("timeframe", "")

            if rs_tf == bias_tf and rs_dir == direction and "bias_match" not in scores:
                scores["bias_match"] = SCORE_WEIGHTS.get("bias_match", 3)
            if rs_tf == conf_tf and rs_dir == direction and "setup_match" not in scores:
                scores["setup_match"] = SCORE_WEIGHTS.get("setup_match", 2)

        return scores

    def _check_correlation(self, signal_data, enrichment, recent_signals):
        direction = signal_data.get("direction")
        symbol = signal_data.get("symbol", "")
        intermarket = enrichment.get("intermarket", {})
        if not direction or not intermarket:
            return 0

        usd_long_when_dxy_falls = ["EURUSD", "GBPUSD", "AUDUSD", "NZDUSD"]
        usd_long_when_dxy_rises = ["USDJPY", "USDCHF", "USDCAD"]

        dxy = intermarket.get("DXY")
        if dxy and dxy.get("change_pct"):
            dxy_falling = dxy["change_pct"] < -0.1
            dxy_rising = dxy["change_pct"] > 0.1
            if symbol in usd_long_when_dxy_falls:
                if (direction == "LONG" and dxy_falling) or (direction == "SHORT" and dxy_rising):
                    return SCORE_WEIGHTS.get("correlation_confirm", 2)
            if symbol in usd_long_when_dxy_rises:
                if (direction == "LONG" and dxy_rising) or (direction == "SHORT" and dxy_falling):
                    return SCORE_WEIGHTS.get("correlation_confirm", 2)

        if symbol == "XAUUSD":
            vix = intermarket.get("VIX")
            if vix and vix.get("change_pct"):
                if direction == "LONG" and vix["change_pct"] > 0.5:
                    return SCORE_WEIGHTS.get("correlation_confirm", 2)

        if recent_signals:
            correlated = self._get_correlated_pairs(symbol)
            for rs in recent_signals:
                if rs.get("symbol") in correlated and rs.get("direction") == direction:
                    return SCORE_WEIGHTS.get("correlation_confirm", 2)

        return 0

    def _detect_liquidity_sweep(self, signal_data, enrichment):
        alert_type = signal_data.get("alert_type", "")
        atr_pct = enrichment.get("atr_pct")
        if "contrarian" in alert_type and atr_pct and atr_pct > 1.0:
            return True
        if "plus" in alert_type and atr_pct and atr_pct > 1.5:
            return True
        return False

    def _check_htf_conflict(self, signal_data, desk, recent_signals):
        if not recent_signals:
            return False
        direction = signal_data.get("direction")
        symbol = signal_data.get("symbol")
        bias_tf = desk.get("timeframes", {}).get("bias", "")
        for rs in recent_signals:
            if (
                rs.get("symbol") == symbol
                and rs.get("timeframe") == bias_tf
                and rs.get("direction") not in (None, "EXIT", direction)
            ):
                return True
        return False

    def _check_rsi_alignment(self, signal_data, enrichment):
        rsi = enrichment.get("rsi")
        direction = signal_data.get("direction")
        if rsi is None or direction is None:
            return 0
        # v5.9: stronger bonuses and penalties
        if direction == "LONG" and rsi < 30:
            return SCORE_WEIGHTS.get("rsi_aligned", 2)
        if direction == "SHORT" and rsi > 70:
            return SCORE_WEIGHTS.get("rsi_aligned", 2)
        if direction == "LONG" and rsi > 75:
            return SCORE_WEIGHTS.get("rsi_counter", -2)
        if direction == "SHORT" and rsi < 25:
            return SCORE_WEIGHTS.get("rsi_counter", -2)
        return 0

    def _check_dxy_headwind(self, signal_data, enrichment):
        """DXY moving against the trade direction for USD pairs."""
        direction = signal_data.get("direction")
        symbol = signal_data.get("symbol", "")
        intermarket = enrichment.get("intermarket", {})
        dxy = intermarket.get("DXY")
        if not dxy or not dxy.get("change_pct"):
            return False

        dxy_change = dxy["change_pct"]
        usd_long_when_dxy_falls = ["EURUSD", "GBPUSD", "AUDUSD", "NZDUSD"]
        usd_long_when_dxy_rises = ["USDJPY", "USDCHF", "USDCAD"]

        # Headwind = DXY moving against your trade
        if symbol in usd_long_when_dxy_falls:
            if direction == "LONG" and dxy_change > 0.15:
                return True
            if direction == "SHORT" and dxy_change < -0.15:
                return True
        if symbol in usd_long_when_dxy_rises:
            if direction == "LONG" and dxy_change < -0.15:
                return True
            if direction == "SHORT" and dxy_change > 0.15:
                return True
        return False

    def _check_multi_analyst_agreement(self, signal_data, desk_id, recent_signals):
        """Check if another signal on the same symbol from a different TF agrees."""
        symbol = signal_data.get("symbol")
        direction = signal_data.get("direction")
        signal_tf = signal_data.get("timeframe", "")

        if not recent_signals or not direction:
            return False

        for rs in recent_signals:
            if (
                rs.get("symbol") == symbol
                and rs.get("direction") == direction
                and rs.get("timeframe") != signal_tf
                and rs.get("timeframe")  # must have a different TF
            ):
                return True
        return False

    def _get_correlated_pairs(self, symbol):
        correlation_map = {
            "EURUSD": ["GBPUSD", "NZDUSD"],
            "GBPUSD": ["EURUSD"],
            "AUDUSD": ["NZDUSD", "AUDCAD"],
            "NZDUSD": ["AUDUSD"],
            "USDCHF": ["EURUSD"],  # inverse
            "EURCHF": ["EURUSD", "USDCHF"],
            "USDJPY": ["EURJPY", "GBPJPY"],
            "EURJPY": ["USDJPY", "GBPJPY", "AUDJPY"],
            "GBPJPY": ["USDJPY", "EURJPY"],
            "AUDJPY": ["EURJPY", "NZDJPY"],
            "CADJPY": ["EURJPY", "GBPJPY"],
            "NZDJPY": ["AUDJPY", "CADJPY"],
            "CHFJPY": ["EURJPY"],
            "EURGBP": ["EURUSD", "GBPUSD"],
            "EURAUD": ["EURUSD", "AUDUSD"],
            "GBPAUD": ["GBPUSD", "AUDUSD"],
            "GBPCAD": ["GBPUSD", "USDCAD"],
            "GBPNZD": ["GBPUSD", "NZDUSD"],
            "EURNZD": ["EURUSD", "NZDUSD"],
            "AUDCAD": ["AUDUSD", "USDCAD"],
            "NAS100": ["US30", "BTCUSD"],
            "US30":   ["NAS100"],
            "BTCUSD": ["ETHUSD", "NAS100", "SOLUSD"],
            "ETHUSD": ["BTCUSD", "SOLUSD"],
            "SOLUSD": ["BTCUSD", "ETHUSD"],
            "XRPUSD": ["BTCUSD"],
            "LINKUSD": ["BTCUSD", "ETHUSD"],
            "XAUUSD": ["XAGUSD"],
            "XAGUSD": ["XAUUSD"],
            "WTIUSD": [],
        }
        return correlation_map.get(symbol, [])
```

## `app/services/claude_cto.py`

```python
"""
Claude CTO Decision Engine
Claude operates as Chief Trading Officer of an institutional prop firm.
It receives the full signal context — enrichment, ML score, consensus score,
desk state, and firm-wide risk — and makes the final EXECUTE / SKIP / REDUCE
decision with institutional reasoning.
"""
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Optional

import httpx

from app.config import ANTHROPIC_API_KEY, DESKS, CAPITAL_PER_ACCOUNT

logger = logging.getLogger("TradingSystem.CTO")

CLAUDE_MODEL = "claude-sonnet-4-20250514"

# ─────────────────────────────────────────────────────────────
# SYSTEM PROMPT — CTO MODE
# ─────────────────────────────────────────────────────────────
CTO_SYSTEM_PROMPT = """You are the Chief Trading Officer (CTO) of OniQuant, a six-desk institutional prop firm managing $600,000 across six FTMO accounts ($100,000 each). You are the FLOOR BOSS. Eight analysts (bots) report to you.

YOUR DESKS:
- DESK1_SCALPER: FX 5M scalps. EURUSD, USDJPY, GBPUSD, AUDUSD. Fast in/out.
- DESK2_INTRADAY: FX 1H intraday. 5 major pairs. Best performing desk — protect this edge.
- DESK3_SWING: FX 4H swing trades. 8 diversified pairs. Hold 1-5 days.
- DESK4_GOLD: 3 analysts on XAUUSD — scalper (5M), intraday (15M/1H), position (4H). DXY and VIX context matters.
- DESK5_ALTS: NAS100 + BTC + ETH + SOL. VIX regime is critical — HALT if VIX > 30.
- DESK6_EQUITIES: NVDA, AAPL, TSLA, MSFT, AMZN, META. Close at session end.

YOUR MANDATE:
DEFAULT BIAS = EXECUTE. All signals reaching you have ALREADY been pre-filtered by LuxAlgo ML Classifier (level 3-4 trend-continuation only) and Smart Trail/Trend Catcher overlay filters. These are high-quality signals. Your job is to let them through unless there's a concrete reason not to.

DECISION OPTIONS:
- EXECUTE: Approve at recommended size (THIS IS YOUR DEFAULT)
- REDUCE: Approve at 25-75% size (specify multiplier)
- SKIP: Reject — ONLY for hard red flags listed below

DESK-SPECIFIC RULES:
- Scalper desks: Speed matters. Auto-EXECUTE if consensus >= 3 AND in kill zone.
- Intraday: Standard rules. REDUCE if RSI > 70 (long) or < 30 (short).
- Swing: Require Daily EMA alignment. SKIP if higher TF clearly conflicts.
- Gold: Check DXY + VIX. If multiple gold analysts agree on direction = extra confidence.
- Momentum (DESK5): VIX > 30 = auto-SKIP. VIX 25-30 = REDUCE 40%.
- Equities (DESK6): VIX > 30 = auto-SKIP.

RESPONSE FORMAT (ONLY valid JSON, no markdown):
{
    "decision": "EXECUTE" | "REDUCE" | "SKIP",
    "size_multiplier": 1.0,
    "reasoning": "2-3 sentence institutional reasoning",
    "risk_flags": ["flag1", "flag2"],
    "confidence": 0.0-1.0,
    "notes_for_log": "Brief note for the trade log"
}

HARD RULES (non-negotiable):
- Consensus score < 2: ALWAYS SKIP
- Desk paused or closed: ALWAYS SKIP
- Daily loss > $4,000: ALWAYS SKIP
- Consecutive losses >= 4: ALWAYS SKIP
- No stop loss: ALWAYS SKIP
- Everything else: EXECUTE or REDUCE, never SKIP
"""


class ClaudeCTO:
    """
    Claude CTO Decision Engine.
    Makes final trade decisions with institutional reasoning.
    Falls back to rule-based decisions if API is unavailable.
    """

    def __init__(self):
        self.api_key = ANTHROPIC_API_KEY
        self.client = httpx.AsyncClient(timeout=30.0)

    async def decide(
        self,
        signal_data: Dict,
        enrichment: Dict,
        ml_result: Dict,
        consensus: Dict,
        desk_state: Dict,
        firm_risk: Dict,
    ) -> Dict:
        """
        Make the final execution decision.
        Returns decision dict with reasoning.
        """
        # ── Hard rules that override everything ──
        hard_skip = self._check_hard_rules(
            signal_data, consensus, desk_state, firm_risk
        )
        if hard_skip:
            return hard_skip

        # ── Build context for Claude ──
        if self.api_key:
            try:
                return await self._claude_decision(
                    signal_data, enrichment, ml_result,
                    consensus, desk_state, firm_risk,
                )
            except Exception as e:
                logger.error(f"Claude API error, using rule-based fallback: {e}")
                return self._rule_based_decision(
                    signal_data, enrichment, ml_result,
                    consensus, desk_state,
                )
        else:
            logger.info("No Anthropic API key — using rule-based decisions")
            return self._rule_based_decision(
                signal_data, enrichment, ml_result,
                consensus, desk_state,
            )

    def _check_hard_rules(
        self,
        signal_data: Dict,
        consensus: Dict,
        desk_state: Dict,
        firm_risk: Dict,
    ) -> Optional[Dict]:
        """
        Non-negotiable rules that always result in SKIP.
        These override Claude's judgment — safety first.
        """
        reasons = []

        # Consensus below minimum — only skip if score is 0 (no valid signal at all)
        if consensus.get("total_score", 0) < 1:
            reasons.append(
                f"Consensus score {consensus.get('total_score', 0)} — no valid signal detected"
            )

        # Desk paused or closed
        if desk_state.get("is_paused"):
            reasons.append("Desk is currently paused")
        if not desk_state.get("is_active", True):
            reasons.append("Desk is closed for the day")

        # Approaching daily loss limit
        daily_loss = abs(desk_state.get("daily_loss", 0))
        if daily_loss >= 4500:
            reasons.append(
                f"Daily loss ${daily_loss:.0f} approaching FTMO limit of $5,000"
            )

        # Consecutive losses
        consec = desk_state.get("consecutive_losses", 0)
        if consec >= 5:
            reasons.append(
                f"Desk has {consec} consecutive losses — closed for day"
            )
        elif consec >= 4:
            reasons.append(
                f"Desk has {consec} consecutive losses — paused 2 hours"
            )

        # No stop loss
        if signal_data.get("sl1") is None:
            reasons.append("No stop loss level — naked trades prohibited")

        # Firm-wide daily drawdown halt
        if firm_risk.get("firm_drawdown_exceeded"):
            reasons.append("Firm-wide daily drawdown exceeded $30,000")

        if reasons:
            return {
                "decision": "SKIP",
                "size_multiplier": 0.0,
                "reasoning": "; ".join(reasons),
                "risk_flags": reasons,
                "confidence": 1.0,
                "notes_for_log": f"Hard rule SKIP: {reasons[0]}",
            }

        return None

    async def _claude_decision(
        self,
        signal_data: Dict,
        enrichment: Dict,
        ml_result: Dict,
        consensus: Dict,
        desk_state: Dict,
        firm_risk: Dict,
    ) -> Dict:
        """Call Claude API for institutional decision."""

        # Build the briefing for Claude
        desk_id = consensus.get("desk_id", "UNKNOWN")
        desk = DESKS.get(desk_id, {})

        briefing = {
            "signal": {
                "symbol": signal_data.get("symbol"),
                "direction": signal_data.get("direction"),
                "alert_type": signal_data.get("alert_type"),
                "timeframe": signal_data.get("timeframe"),
                "price": signal_data.get("price"),
                "stop_loss": signal_data.get("sl1"),
                "take_profit_1": signal_data.get("tp1"),
                "take_profit_2": signal_data.get("tp2"),
                "smart_trail": signal_data.get("smart_trail"),
            },
            "desk": {
                "id": desk_id,
                "name": desk.get("name"),
                "style": desk.get("style"),
                "risk_pct": desk.get("risk_pct"),
                "max_trades_day": desk.get("max_trades_day"),
            },
            "consensus": {
                "total_score": consensus.get("total_score"),
                "tier": consensus.get("tier"),
                "size_multiplier": consensus.get("size_multiplier"),
                "breakdown": consensus.get("breakdown"),
            },
            "ml_model": {
                "score": ml_result.get("ml_score"),
                "method": ml_result.get("ml_method"),
            },
            "market_context": {
                "rsi": enrichment.get("rsi"),
                "rsi_zone": enrichment.get("rsi_zone"),
                "atr_pct": enrichment.get("atr_pct"),
                "volatility_regime": enrichment.get("volatility_regime"),
                "active_session": enrichment.get("active_session"),
                "kill_zone": enrichment.get("kill_zone_type"),
                "spread": enrichment.get("spread"),
            },
            "intermarket": enrichment.get("intermarket", {}),
            "desk_state": {
                "trades_today": desk_state.get("trades_today", 0),
                "daily_pnl": desk_state.get("daily_pnl", 0),
                "consecutive_losses": desk_state.get("consecutive_losses", 0),
                "size_modifier": desk_state.get("size_modifier", 1.0),
                "open_positions": desk_state.get("open_positions", 0),
            },
            "firm_risk": {
                "total_firm_daily_pnl": firm_risk.get("total_daily_pnl", 0),
                "desks_with_correlated_exposure": firm_risk.get("correlated_desks", []),
                "firm_drawdown_level": firm_risk.get("drawdown_level", "NORMAL"),
            },
        }

        user_message = (
            f"SIGNAL BRIEFING — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n"
            f"{json.dumps(briefing, indent=2)}\n\n"
            f"Make your decision. Respond with JSON only."
        )

        # Call Claude
        resp = await self.client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": self.api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": CLAUDE_MODEL,
                "max_tokens": 500,
                "system": CTO_SYSTEM_PROMPT,
                "messages": [{"role": "user", "content": user_message}],
            },
        )

        if resp.status_code != 200:
            logger.error(
                f"Claude API returned {resp.status_code}: {resp.text[:300]}"
            )
            raise Exception(f"Claude API error: {resp.status_code}")

        # Parse response
        response_data = resp.json()
        content = response_data.get("content", [])
        text = ""
        for block in content:
            if block.get("type") == "text":
                text += block.get("text", "")

        # Parse JSON from Claude's response
        text = text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[-1].rsplit("```", 1)[0]

        try:
            decision = json.loads(text)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Claude response: {text[:200]}")
            raise Exception(f"Claude JSON parse error: {e}")

        # Validate required fields
        if decision.get("decision") not in ("EXECUTE", "REDUCE", "SKIP"):
            raise Exception(
                f"Invalid decision: {decision.get('decision')}"
            )

        # Ensure size_multiplier is within bounds
        mult = decision.get("size_multiplier", 1.0)
        decision["size_multiplier"] = max(0.0, min(1.0, float(mult)))

        logger.info(
            f"CTO Decision: {decision['decision']} | "
            f"Size: {decision['size_multiplier']*100:.0f}% | "
            f"Confidence: {decision.get('confidence', 'N/A')} | "
            f"{decision.get('reasoning', '')[:100]}"
        )

        return decision

    def _rule_based_decision(
        self,
        signal_data: Dict,
        enrichment: Dict,
        ml_result: Dict,
        consensus: Dict,
        desk_state: Dict,
    ) -> Dict:
        """
        Rule-based fallback when Claude API is unavailable.
        Follows the consensus scoring tiers strictly.
        """
        tier = consensus.get("tier", "SKIP")
        score = consensus.get("total_score", 0)
        ml_score = ml_result.get("ml_score", 0.5)
        size_mult = consensus.get("size_multiplier", 0.0)

        # Start with consensus recommendation
        if tier == "SKIP":
            return {
                "decision": "SKIP",
                "size_multiplier": 0.0,
                "reasoning": f"Consensus score {score} below minimum. Tier: SKIP.",
                "risk_flags": [],
                "confidence": 0.8,
                "notes_for_log": f"Rule-based SKIP: score {score}",
            }

        # ML score adjusts size, NOT whether to trade
        if tier == "HIGH" and ml_score >= 0.50:
            decision = "EXECUTE"
            reasoning = (
                f"Strong alignment: Consensus {score} ({tier}), "
                f"ML probability {ml_score:.2f}. Full size approved."
            )
        elif tier == "HIGH":
            decision = "EXECUTE"
            size_mult *= 0.75
            reasoning = (
                f"High consensus {score} with moderate ML {ml_score:.2f}. "
                f"Executing at 75% size."
            )
        elif tier == "MEDIUM":
            decision = "EXECUTE"
            reasoning = (
                f"MEDIUM consensus {score}, ML {ml_score:.2f}. "
                f"Executing at {size_mult*100:.0f}% size."
            )
        elif tier == "LOW":
            decision = "REDUCE"
            size_mult *= 0.5
            reasoning = (
                f"LOW consensus {score}, ML {ml_score:.2f}. "
                f"Reducing to {size_mult*100:.0f}% size."
            )
        else:
            decision = "EXECUTE"
            reasoning = (
                f"Consensus {score} ({tier}), ML {ml_score:.2f}. "
                f"Executing at {size_mult*100:.0f}% size."
            )

        # Consecutive loss size adjustment
        consec = desk_state.get("consecutive_losses", 0)
        state_modifier = desk_state.get("size_modifier", 1.0)
        if state_modifier < 1.0:
            size_mult *= state_modifier
            reasoning += (
                f" Size reduced to {state_modifier*100:.0f}% "
                f"due to {consec} consecutive losses."
            )

        # Volatility regime caution
        regime = enrichment.get("volatility_regime", "NORMAL")
        if regime == "HIGH_VOLATILITY" and tier != "HIGH":
            size_mult *= 0.75
            reasoning += " Size reduced 25% for high volatility regime."

        risk_flags = []
        if regime == "HIGH_VOLATILITY":
            risk_flags.append("high_volatility")
        if consec >= 2:
            risk_flags.append(f"consecutive_losses_{consec}")
        if ml_score < 0.50:
            risk_flags.append("below_average_ml_score")

        return {
            "decision": decision,
            "size_multiplier": round(max(0.0, min(1.0, size_mult)), 2),
            "reasoning": reasoning,
            "risk_flags": risk_flags,
            "confidence": round(ml_score * 0.6 + (score / 12) * 0.4, 2),
            "notes_for_log": (
                f"Rule-based {decision}: score={score} ml={ml_score:.2f}"
            ),
        }

    async def close(self):
        """Close the HTTP client."""
        await self.client.aclose()
```

## `app/services/risk_filter.py`

```python
"""
Hard Risk Filter
Independent safety layer that runs AFTER Claude's decision.
This mirrors the EA-side risk checks but runs server-side as a second line of defense.
Any trade that passes here will also be verified by the MT5 EA independently.
"""
import logging
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple, List

from sqlalchemy.orm import Session
from sqlalchemy import func

from app.config import (
    DESKS, CAPITAL_PER_ACCOUNT, MAX_DAILY_LOSS_PER_ACCOUNT,
    MAX_TOTAL_LOSS_PER_ACCOUNT, FIRM_WIDE_DAILY_DRAWDOWN_HALT,
    CONSECUTIVE_LOSS_RULES, SESSION_WINDOWS, CORRELATION_GROUPS,
    MAX_CORRELATED_POSITIONS,
)
from app.models.trade import Trade
from app.models.desk_state import DeskState

logger = logging.getLogger("TradingSystem.RiskFilter")


class HardRiskFilter:
    """
    Server-side risk filter. Independent of Claude's decision.
    Provides firm-wide risk state and final trade validation.
    """

    def get_desk_state(self, db: Session, desk_id: str) -> Dict:
        """
        Get current desk state for pipeline consumption.
        Creates desk state record if it doesn't exist.
        """
        state = db.query(DeskState).filter(DeskState.desk_id == desk_id).first()

        if not state:
            state = DeskState(
                desk_id=desk_id,
                is_active=True,
                is_paused=False,
                trades_today=0,
                daily_pnl=0.0,
                daily_loss=0.0,
                consecutive_losses=0,
                size_modifier=1.0,
                open_positions=0,
                last_reset_date=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            )
            db.add(state)
            db.commit()
            db.refresh(state)

        # Check if we need a daily reset
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if state.last_reset_date != today:
            state.trades_today = 0
            state.daily_pnl = 0.0
            state.daily_loss = 0.0
            state.consecutive_losses = 0
            state.size_modifier = 1.0
            state.is_paused = False
            state.is_active = True
            state.last_reset_date = today
            db.commit()

        # Check pause expiry
        if state.is_paused and state.pause_until:
            if datetime.now(timezone.utc) >= state.pause_until:
                state.is_paused = False
                state.pause_until = None
                db.commit()

        # Check max trades
        desk = DESKS.get(desk_id, {})
        max_trades = desk.get("max_trades_day", desk.get("max_simultaneous", 999))
        max_trades_hit = state.trades_today >= max_trades

        return {
            "is_active": state.is_active,
            "is_paused": state.is_paused,
            "trades_today": state.trades_today,
            "daily_pnl": state.daily_pnl,
            "daily_loss": state.daily_loss,
            "consecutive_losses": state.consecutive_losses,
            "size_modifier": state.size_modifier,
            "open_positions": state.open_positions,
            "max_trades_hit": max_trades_hit,
        }

    def get_firm_risk(self, db: Session) -> Dict:
        """
        Calculate firm-wide risk metrics across all desks.
        """
        total_daily_pnl = 0.0
        total_daily_loss = 0.0
        correlated_desks = []

        desk_states = db.query(DeskState).all()

        # Aggregate firm PnL
        for state in desk_states:
            total_daily_pnl += state.daily_pnl or 0
            total_daily_loss += abs(state.daily_loss or 0)

        # Check firm-wide drawdown
        firm_drawdown_exceeded = total_daily_loss >= FIRM_WIDE_DAILY_DRAWDOWN_HALT

        # Determine drawdown level
        if total_daily_loss >= FIRM_WIDE_DAILY_DRAWDOWN_HALT:
            drawdown_level = "CRITICAL"
        elif total_daily_loss >= FIRM_WIDE_DAILY_DRAWDOWN_HALT * 0.75:
            drawdown_level = "WARNING"
        elif total_daily_loss >= FIRM_WIDE_DAILY_DRAWDOWN_HALT * 0.5:
            drawdown_level = "ELEVATED"
        else:
            drawdown_level = "NORMAL"

        # TODO: Check correlated exposure across desks
        # (needs open positions data from MT5 — Phase 3)

        return {
            "total_daily_pnl": round(total_daily_pnl, 2),
            "total_daily_loss": round(total_daily_loss, 2),
            "firm_drawdown_exceeded": firm_drawdown_exceeded,
            "drawdown_level": drawdown_level,
            "correlated_desks": correlated_desks,
        }

    def validate_trade(
        self,
        decision: Dict,
        signal_data: Dict,
        desk_state: Dict,
        desk_id: str,
        db: Session = None,
    ) -> Tuple[bool, Optional[str], Dict]:
        """
        Final validation before sending to MT5.
        Returns (approved, rejection_reason, adjusted_params).
        """
        desk = DESKS.get(desk_id, {})

        # ── 1. Decision must be EXECUTE or REDUCE ──
        if decision.get("decision") == "SKIP":
            return False, "CTO decision: SKIP", {}

        # ── 2. Must have stop loss ──
        if signal_data.get("sl1") is None:
            return False, "No stop loss — trade rejected", {}

        # ── 3. Session enforcement ──
        session_ok, session_msg = self._check_session(desk_id, desk)
        if not session_ok:
            return False, session_msg, {}

        # ── 4. Correlation filter ──
        if db:
            corr_ok, corr_msg = self._check_correlation(
                db, signal_data.get("symbol"), signal_data.get("direction")
            )
            if not corr_ok:
                return False, corr_msg, {}

        # ── 5. Calculate position size ──
        risk_pct = desk.get("risk_pct", 1.0)
        size_mult = decision.get("size_multiplier", 1.0)
        desk_modifier = desk_state.get("size_modifier", 1.0)

        effective_risk_pct = risk_pct * size_mult * desk_modifier

        # Cap at desk's base risk
        effective_risk_pct = min(effective_risk_pct, risk_pct)

        risk_dollars = CAPITAL_PER_ACCOUNT * (effective_risk_pct / 100)

        # ── 6. Validate risk doesn't exceed daily limit ──
        remaining_daily_budget = MAX_DAILY_LOSS_PER_ACCOUNT - abs(
            desk_state.get("daily_loss", 0)
        )
        if risk_dollars > remaining_daily_budget:
            risk_dollars = remaining_daily_budget * 0.8  # leave 20% buffer
            if risk_dollars <= 0:
                return False, "Daily loss budget exhausted", {}

        # ── 7. Build trade parameters ──
        trade_params = {
            "desk_id": desk_id,
            "symbol": signal_data.get("symbol"),
            "direction": signal_data.get("direction"),
            "price": signal_data.get("price"),
            "timeframe": signal_data.get("timeframe"),
            "alert_type": signal_data.get("alert_type"),
            "risk_pct": round(effective_risk_pct, 4),
            "risk_dollars": round(risk_dollars, 2),
            "stop_loss": signal_data.get("sl1"),
            "take_profit_1": signal_data.get("tp1"),
            "take_profit_2": signal_data.get("tp2"),
            "trailing_stop_pips": desk.get("trailing_stop_pips"),
            "max_hold_hours": desk.get("max_hold_hours"),
            "size_multiplier": round(size_mult * desk_modifier, 4),
            "claude_decision": decision.get("decision"),
            "claude_reasoning": decision.get("reasoning"),
            "confidence": decision.get("confidence"),
        }

        logger.info(
            f"Trade APPROVED | {desk_id} | {signal_data.get('symbol')} "
            f"{signal_data.get('direction')} | Risk: ${risk_dollars:.2f} "
            f"({effective_risk_pct:.2f}%)"
        )

        return True, None, trade_params

    # ─────────────────────────────────────────
    # SESSION ENFORCEMENT
    # ─────────────────────────────────────────

    def _check_session(self, desk_id: str, desk: Dict) -> Tuple[bool, str]:
        """
        Hard-block trades outside allowed sessions.
        Returns (allowed, message).
        """
        sessions = desk.get("sessions", ["ALL"])
        if "ALL" in sessions:
            return True, ""

        now_utc = datetime.now(timezone.utc)
        current_hour = now_utc.hour

        for session_name in sessions:
            window = SESSION_WINDOWS.get(session_name)
            if not window:
                continue

            start = window["start"]
            end = window["end"]

            # Handle sessions that wrap midnight (e.g., Sydney 21-6)
            if start > end:
                if current_hour >= start or current_hour < end:
                    logger.debug(
                        f"Session OK: {session_name} ({start}:00-{end}:00 UTC), "
                        f"current: {current_hour}:00 UTC"
                    )
                    return True, ""
            else:
                if start <= current_hour < end:
                    logger.debug(
                        f"Session OK: {session_name} ({start}:00-{end}:00 UTC), "
                        f"current: {current_hour}:00 UTC"
                    )
                    return True, ""

        session_names = ", ".join(sessions)
        msg = (
            f"Outside trading session for {desk_id}. "
            f"Allowed: {session_names}. Current: {current_hour}:00 UTC"
        )
        logger.info(f"SESSION BLOCK | {msg}")
        return False, msg

    # ─────────────────────────────────────────
    # CORRELATION FILTER
    # ─────────────────────────────────────────

    def _check_correlation(
        self, db: Session, symbol: str, direction: str
    ) -> Tuple[bool, str]:
        """
        Check if taking this trade would create too much correlated exposure.
        Only allows MAX_CORRELATED_POSITIONS per correlation group per direction.
        """
        if not symbol or not direction:
            return True, ""

        # Find which correlation groups this symbol belongs to
        for group_name, group in CORRELATION_GROUPS.items():
            if symbol not in group.get("symbols", []):
                continue

            correlated_symbols = group["symbols"]

            # Count open trades in same direction for correlated symbols
            open_correlated = (
                db.query(Trade)
                .filter(
                    Trade.symbol.in_(correlated_symbols),
                    Trade.direction == direction,
                    Trade.status.in_(["OPEN", "EXECUTED", "SIM_OPEN"]),
                )
                .count()
            )

            if open_correlated >= MAX_CORRELATED_POSITIONS:
                msg = (
                    f"Correlation limit: {open_correlated} open {direction} "
                    f"positions in '{group_name}' group "
                    f"({', '.join(correlated_symbols)}). "
                    f"Max allowed: {MAX_CORRELATED_POSITIONS}"
                )
                logger.info(f"CORRELATION BLOCK | {msg}")
                return False, msg

        return True, ""

    def get_recent_signals(
        self, db: Session, symbol: str, hours: int = 24, limit: int = 20
    ):
        """Fetch recent signals for the same symbol (for consensus scoring)."""
        from app.models.signal import Signal

        cutoff = datetime.now(timezone.utc) - __import__("datetime").timedelta(hours=hours)

        signals = (
            db.query(Signal)
            .filter(
                Signal.symbol_normalized == symbol,
                Signal.received_at >= cutoff,
                Signal.is_valid == True,
            )
            .order_by(Signal.received_at.desc())
            .limit(limit)
            .all()
        )

        return [
            {
                "symbol": s.symbol_normalized,
                "timeframe": s.timeframe,
                "alert_type": s.alert_type,
                "direction": s.direction,
                "price": s.price,
                "received_at": s.received_at.isoformat() if s.received_at else None,
            }
            for s in signals
        ]
```

## `app/services/server_simulator.py`

```python
"""
Server-Side Trade Simulator — 4-Provider Asset-Class Router
Institutional-grade price feed with JIT polling, circuit breakers,
adaptive throttling, and Binance WebSocket for crypto.

Provider Architecture:
  Crypto    → Binance WebSocket (real-time, 0 REST calls)
  Forex     → TwelveData batch (primary) → FMP batch (fallback)
  Metals    → TwelveData batch (primary) → FMP batch (fallback)
  Stocks    → Finnhub individual (primary) → FMP batch (fallback)
  Indices   → FMP batch (primary) → TwelveData batch (fallback)
  Oil/Cu    → FMP batch (primary) → TwelveData batch (fallback)

Rate Budgets:
  TwelveData: 8 credits/min, 800/day — forex/metals only
  Finnhub:    60 req/min — US stocks only
  FMP:        250 req/day — indices, commodities, universal fallback
  Binance WS: Unlimited — crypto streaming
"""
import os
import json
import asyncio
import logging
import time as _time
from datetime import datetime, timezone
from typing import Dict, Optional, Set, List
from collections import deque

import httpx
from sqlalchemy.orm import Session

from app.config import (
    DESKS, CAPITAL_PER_ACCOUNT,
    PORTFOLIO_CAPITAL_PER_DESK, get_pip_info,
    SYMBOL_ASSET_CLASS,
    TWELVEDATA_API_KEY, FINNHUB_API_KEY,
    BINANCE_WS_URL, BINANCE_REST_URL,  # kept for legacy reference only
)
from app.database import SessionLocal
from app.models.trade import Trade
from app.models.desk_state import DeskState
from app.services.telegram_bot import TelegramBot

logger = logging.getLogger("TradingSystem.Simulator")

FMP_API_KEY = os.getenv("FMP_API_KEY", "")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ASSET CLASS CLASSIFICATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CRYPTO_SYMBOLS = {"BTCUSD", "ETHUSD", "SOLUSD", "XRPUSD", "LINKUSD"}

FOREX_SYMBOLS = {
    "EURUSD", "USDJPY", "GBPUSD", "USDCHF", "AUDUSD", "USDCAD",
    "NZDUSD", "EURJPY", "GBPJPY", "AUDJPY", "EURGBP", "EURAUD",
    "GBPAUD", "EURCHF", "CADJPY", "NZDJPY", "GBPCAD", "AUDCAD",
    "AUDNZD", "CHFJPY", "EURNZD", "GBPNZD",
}

METAL_SYMBOLS = {"XAUUSD", "XAGUSD"}

STOCK_SYMBOLS = {"TSLA", "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "NFLX", "AMD"}

INDEX_SYMBOLS = {"US30", "US100", "NAS100"}

COMMODITY_SYMBOLS = {"WTIUSD", "XCUUSD"}


def classify_symbol(sym: str) -> str:
    if sym in CRYPTO_SYMBOLS: return "CRYPTO"
    if sym in FOREX_SYMBOLS: return "FOREX"
    if sym in METAL_SYMBOLS: return "METAL"
    if sym in STOCK_SYMBOLS: return "STOCK"
    if sym in INDEX_SYMBOLS: return "INDEX"
    if sym in COMMODITY_SYMBOLS: return "COMMODITY"
    return "UNKNOWN"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PROVIDER SYMBOL MAPS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# TwelveData (forex + metals)
TD_MAP = {
    "EURUSD": "EUR/USD", "USDJPY": "USD/JPY", "GBPUSD": "GBP/USD",
    "USDCHF": "USD/CHF", "AUDUSD": "AUD/USD", "USDCAD": "USD/CAD",
    "NZDUSD": "NZD/USD", "EURJPY": "EUR/JPY", "GBPJPY": "GBP/JPY",
    "AUDJPY": "AUD/JPY", "EURGBP": "EUR/GBP", "EURAUD": "EUR/AUD",
    "GBPAUD": "GBP/AUD", "EURCHF": "EUR/CHF", "CADJPY": "CAD/JPY",
    "NZDJPY": "NZD/JPY", "GBPCAD": "GBP/CAD", "AUDCAD": "AUD/CAD",
    "AUDNZD": "AUD/NZD", "CHFJPY": "CHF/JPY", "EURNZD": "EUR/NZD",
    "GBPNZD": "GBP/NZD",
    "XAUUSD": "XAU/USD", "XAGUSD": "XAG/USD",
}
TD_REVERSE = {v: k for k, v in TD_MAP.items()}

# Finnhub (US stocks only — free tier)
FH_MAP = {
    "TSLA": "TSLA", "AAPL": "AAPL", "MSFT": "MSFT",
    "NVDA": "NVDA", "AMZN": "AMZN", "META": "META",
    "GOOGL": "GOOGL", "NFLX": "NFLX", "AMD": "AMD",
}

# FMP (indices, commodities, universal fallback)
FMP_MAP = {
    # Indices
    "US30": "^DJI", "US100": "^GSPC", "NAS100": "^IXIC",
    # Commodities
    "WTIUSD": "CLUSD", "XCUUSD": "HGUSD",
    # Metals (fallback)
    "XAUUSD": "XAUUSD", "XAGUSD": "XAGUSD",
    # Forex (fallback)
    "EURUSD": "EURUSD", "USDJPY": "USDJPY", "GBPUSD": "GBPUSD",
    "USDCHF": "USDCHF", "AUDUSD": "AUDUSD", "USDCAD": "USDCAD",
    "NZDUSD": "NZDUSD", "EURJPY": "EURJPY", "GBPJPY": "GBPJPY",
    "AUDJPY": "AUDJPY", "EURGBP": "EURGBP", "EURAUD": "EURAUD",
    "GBPAUD": "GBPAUD", "EURCHF": "EURCHF", "CADJPY": "CADJPY",
    "NZDJPY": "NZDJPY", "GBPCAD": "GBPCAD", "AUDCAD": "AUDCAD",
    "AUDNZD": "AUDNZD", "CHFJPY": "CHFJPY", "EURNZD": "EURNZD",
    "GBPNZD": "GBPNZD",
    # Stocks (fallback)
    "TSLA": "TSLA", "AAPL": "AAPL", "MSFT": "MSFT",
    "NVDA": "NVDA", "AMZN": "AMZN", "META": "META",
    "GOOGL": "GOOGL", "NFLX": "NFLX", "AMD": "AMD",
}
FMP_REVERSE = {v: k for k, v in FMP_MAP.items()}

# Kraken WebSocket v2 (crypto) — US-legal, no API key needed
KRAKEN_SYMBOLS = {
    "BTCUSD": "XBT/USD", "ETHUSD": "ETH/USD",
    "SOLUSD": "SOL/USD", "XRPUSD": "XRP/USD", "LINKUSD": "LINK/USD",
}
KRAKEN_REVERSE = {v: k for k, v in KRAKEN_SYMBOLS.items()}

# CoinCap WebSocket (crypto validation) — no auth, aggregated prices
COINCAP_SYMBOLS = {
    "BTCUSD": "bitcoin", "ETHUSD": "ethereum",
    "SOLUSD": "solana", "XRPUSD": "ripple", "LINKUSD": "chainlink",
}
COINCAP_REVERSE = {v: k for k, v in COINCAP_SYMBOLS.items()}

# Kraken REST (crypto fallback)
KRAKEN_REST_URL = "https://api.kraken.com"
KRAKEN_REST_PAIRS = {
    "BTCUSD": "XXBTZUSD", "ETHUSD": "XETHZUSD",
    "SOLUSD": "SOLUSD", "XRPUSD": "XXRPZUSD", "LINKUSD": "LINKUSD",
}

# Legacy — kept so BINANCE_STREAMS references don't break symbol mapping elsewhere
BINANCE_STREAMS = {
    "BTCUSD": "btcusdt", "ETHUSD": "ethusdt",
    "SOLUSD": "solusdt", "XRPUSD": "xrpusdt", "LINKUSD": "linkusdt",
}
BINANCE_REVERSE = {v.replace("@ticker", ""): k for k, v in BINANCE_STREAMS.items()}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CIRCUIT BREAKER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class CircuitBreaker:
    """Per-provider circuit breaker: CLOSED → OPEN → HALF_OPEN → CLOSED."""

    def __init__(self, name: str, failure_threshold: int = 5, cooldown: float = 60.0):
        self.name = name
        self.failure_threshold = failure_threshold
        self.cooldown = cooldown
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN
        self.consecutive_failures = 0
        self.last_failure_time = 0.0
        self.total_success = 0
        self.total_fail = 0
        self.extra_stats: Dict = {}

    def allow_request(self) -> bool:
        if self.state == "CLOSED":
            return True
        if self.state == "OPEN":
            if _time.time() - self.last_failure_time >= self.cooldown:
                self.state = "HALF_OPEN"
                logger.info(f"CB {self.name}: OPEN → HALF_OPEN (testing)")
                return True
            return False
        if self.state == "HALF_OPEN":
            return True
        return False

    def record_success(self):
        self.total_success += 1
        self.consecutive_failures = 0
        if self.state == "HALF_OPEN":
            self.state = "CLOSED"
            logger.info(f"CB {self.name}: HALF_OPEN → CLOSED (recovered)")

    def record_failure(self):
        self.total_fail += 1
        self.consecutive_failures += 1
        self.last_failure_time = _time.time()
        if self.state == "HALF_OPEN":
            self.state = "OPEN"
            self.cooldown = min(self.cooldown * 2, 300)
            logger.warning(f"CB {self.name}: HALF_OPEN → OPEN (cooldown: {self.cooldown}s)")
        elif self.consecutive_failures >= self.failure_threshold:
            self.state = "OPEN"
            logger.warning(f"CB {self.name}: CLOSED → OPEN ({self.consecutive_failures} failures)")

    def get_stats(self) -> Dict:
        return {
            "state": self.state,
            "success": self.total_success,
            "fail": self.total_fail,
            "consecutive_failures": self.consecutive_failures,
            **self.extra_stats,
        }


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ADAPTIVE THROTTLE FOR TWELVEDATA
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class TDThrottle:
    """Track TwelveData credit usage, adapt polling interval."""

    def __init__(self):
        self.credits_log: deque = deque()  # (timestamp, credits_used)
        self.daily_credits = 0
        self.daily_reset_date = ""

    def record_credits(self, count: int):
        now = _time.time()
        self.credits_log.append((now, count))
        self.daily_credits += count
        # Reset daily counter at midnight
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if today != self.daily_reset_date:
            self.daily_credits = count
            self.daily_reset_date = today
        # Prune old entries (>60s)
        while self.credits_log and self.credits_log[0][0] < now - 60:
            self.credits_log.popleft()

    def credits_last_60s(self) -> int:
        now = _time.time()
        while self.credits_log and self.credits_log[0][0] < now - 60:
            self.credits_log.popleft()
        return sum(c for _, c in self.credits_log)

    def get_interval(self) -> float:
        used = self.credits_last_60s()
        if self.daily_credits > 700:
            return 45.0  # preservation mode
        if used >= 7:
            return 30.0  # conservative
        if used >= 5:
            return 20.0  # cautious
        return 15.0  # aggressive


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# MARKET-AWARE CACHE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def _get_cache_ttl(asset_class: str) -> float:
    """Market-aware TTL: don't waste API calls outside trading hours."""
    now = datetime.now(timezone.utc)
    hour = now.hour
    weekday = now.weekday()  # 0=Mon, 6=Sun

    if asset_class == "CRYPTO":
        return 10.0  # 24/7

    # Weekend — everything except crypto is closed
    if weekday >= 5:  # Saturday/Sunday
        return 3600.0

    if asset_class in ("FOREX", "METAL"):
        # Forex: Sun 21:00 → Fri 21:00 UTC
        return 15.0  # during the week

    if asset_class in ("STOCK", "INDEX"):
        # US market hours: 13:30-21:00 UTC
        if 13 <= hour < 21:
            return 15.0
        return 300.0  # after hours

    if asset_class == "COMMODITY":
        if 13 <= hour < 21:
            return 20.0
        return 300.0

    return 30.0


class PriceCache:
    """In-memory price cache with market-aware TTLs."""

    def __init__(self):
        self._data: Dict[str, Dict] = {}

    def get(self, symbol: str) -> Optional[float]:
        entry = self._data.get(symbol)
        if not entry:
            return None
        age = _time.time() - entry["time"]
        ttl = _get_cache_ttl(classify_symbol(symbol))
        if age <= ttl:
            return entry["price"]
        return None  # expired

    def get_stale(self, symbol: str) -> Optional[float]:
        """Return price even if expired (for fallback)."""
        entry = self._data.get(symbol)
        return entry["price"] if entry else None

    def set(self, symbol: str, price: float):
        self._data[symbol] = {"price": price, "time": _time.time()}

    def age(self, symbol: str) -> Optional[float]:
        entry = self._data.get(symbol)
        return _time.time() - entry["time"] if entry else None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CRYPTO WEBSOCKET MANAGER (Kraken primary + CoinCap validation)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class CryptoWSManager:
    """
    Dual WebSocket feed for crypto prices.
    Primary:    Kraken WS v2 (wss://ws.kraken.com/v2) — no API key, US-legal
    Secondary:  CoinCap WS (wss://ws.coincap.io) — no auth, aggregated prices

    If Kraken drops, CoinCap keeps prices flowing.
    If both agree within 0.5%, prices are validated.
    """

    KRAKEN_WS = "wss://ws.kraken.com/v2"
    COINCAP_WS = "wss://ws.coincap.io/prices?assets=bitcoin,ethereum,solana,ripple,chainlink"

    def __init__(self, cache: PriceCache):
        self.cache = cache
        self.running = False
        self._kraken_task = None
        self._coincap_task = None
        self._kraken_connected = False
        self._coincap_connected = False
        self._kraken_reconnect_delay = 1.0
        self._coincap_reconnect_delay = 1.0

    @property
    def connected(self) -> bool:
        return self._kraken_connected or self._coincap_connected

    @property
    def kraken_connected(self) -> bool:
        return self._kraken_connected

    @property
    def coincap_connected(self) -> bool:
        return self._coincap_connected

    async def start(self):
        self.running = True
        self._kraken_task = asyncio.create_task(self._run_kraken())
        self._coincap_task = asyncio.create_task(self._run_coincap())
        logger.info("Crypto WS manager started (Kraken v2 + CoinCap)")

    async def stop(self):
        self.running = False
        for task in [self._kraken_task, self._coincap_task]:
            if task:
                task.cancel()
        logger.info("Crypto WS manager stopped")

    # ── Kraken WebSocket v2 ──

    async def _run_kraken(self):
        while self.running:
            try:
                await self._connect_kraken()
                # If we get here, connection closed cleanly — wait before reconnecting
                self._kraken_connected = False
                logger.info("Kraken WS disconnected cleanly | reconnecting in 5s")
                await asyncio.sleep(5.0)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._kraken_connected = False
                logger.warning(f"Kraken WS error: {type(e).__name__}: {e} | reconnect in {self._kraken_reconnect_delay}s")
                await asyncio.sleep(self._kraken_reconnect_delay)
                self._kraken_reconnect_delay = min(self._kraken_reconnect_delay * 2, 60.0)

    async def _connect_kraken(self):
        try:
            import websockets
        except ImportError:
            logger.error("websockets package not installed — Kraken WS disabled")
            self.running = False
            return

        symbols = list(KRAKEN_SYMBOLS.values())  # ["XBT/USD", "ETH/USD", ...]
        logger.info(f"Kraken WS connecting to {self.KRAKEN_WS}...")

        async with websockets.connect(
            self.KRAKEN_WS,
            ping_interval=20,
            ping_timeout=10,
            close_timeout=5,
        ) as ws:
            # Subscribe to ticker channel
            subscribe_msg = json.dumps({
                "method": "subscribe",
                "params": {
                    "channel": "ticker",
                    "symbol": symbols,
                    "event_trigger": "trades",
                    "snapshot": True,
                }
            })
            await ws.send(subscribe_msg)
            self._kraken_connected = True
            self._kraken_reconnect_delay = 1.0
            logger.info(f"Kraken WS v2 connected | {len(symbols)} symbols: {symbols}")

            async for message in ws:
                if not self.running:
                    break
                try:
                    data = json.loads(message)
                    channel = data.get("channel")
                    method = data.get("method")

                    if channel == "ticker" and data.get("type") in ("snapshot", "update"):
                        for item in data.get("data", []):
                            symbol = item.get("symbol")  # e.g. "XBT/USD"
                            last_price = item.get("last")
                            if symbol and last_price:
                                internal = KRAKEN_REVERSE.get(symbol)
                                if internal:
                                    price = float(last_price)
                                    if price > 0:
                                        self.cache.set(internal, price)

                    elif method == "subscribe":
                        if data.get("success"):
                            logger.debug(f"Kraken subscribed: {data.get('result', {}).get('symbol', '?')}")
                        else:
                            logger.warning(f"Kraken subscribe failed: {data.get('error')}")

                    elif channel == "heartbeat":
                        pass  # Normal keepalive

                except (json.JSONDecodeError, ValueError, KeyError, TypeError) as e:
                    logger.debug(f"Kraken message parse error: {e}")

        self._kraken_connected = False

    # ── CoinCap WebSocket (validation + backup) ──

    async def _run_coincap(self):
        while self.running:
            try:
                await self._connect_coincap()
                # Clean disconnect — wait before reconnecting
                self._coincap_connected = False
                logger.debug("CoinCap WS disconnected | reconnecting in 10s")
                await asyncio.sleep(10.0)
            except asyncio.CancelledError:
                break
            except Exception as e:
                self._coincap_connected = False
                logger.debug(f"CoinCap WS error: {e} | reconnect in {self._coincap_reconnect_delay}s")
                await asyncio.sleep(self._coincap_reconnect_delay)
                self._coincap_reconnect_delay = min(self._coincap_reconnect_delay * 2, 60.0)

    async def _connect_coincap(self):
        try:
            import websockets
        except ImportError:
            return

        async with websockets.connect(self.COINCAP_WS, ping_interval=30, ping_timeout=15) as ws:
            self._coincap_connected = True
            self._coincap_reconnect_delay = 1.0
            logger.info("CoinCap WS connected (validation feed)")

            async for message in ws:
                if not self.running:
                    break
                try:
                    # CoinCap sends: {"bitcoin":"97345.12","ethereum":"3421.50",...}
                    data = json.loads(message)
                    for coincap_name, price_str in data.items():
                        internal = COINCAP_REVERSE.get(coincap_name)
                        if internal and price_str:
                            price = float(price_str)
                            if price > 0:
                                # Only write to cache if Kraken hasn't updated recently
                                # (Kraken is primary, CoinCap is backup)
                                existing = self.cache.get(internal)
                                if existing is None:
                                    self.cache.set(internal, price)
                except (json.JSONDecodeError, ValueError, KeyError, TypeError):
                    pass

        self._coincap_connected = False


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# PROVIDER STATS (exposed for /providers + /health)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_circuit_breakers: Dict[str, CircuitBreaker] = {}
_td_throttle = TDThrottle()


def get_provider_stats() -> Dict:
    """Return stats for all providers."""
    stats = {}
    for name, cb in _circuit_breakers.items():
        stats[name] = cb.get_stats()
    stats["td_throttle"] = {
        "credits_60s": _td_throttle.credits_last_60s(),
        "daily_credits": _td_throttle.daily_credits,
    }
    return stats


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# OPEN TRADE STATUSES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
OPEN_STATUSES = ["EXECUTED", "OPEN", "ONIAI_OPEN", "VIRTUAL_OPEN", "SIM_OPEN"]
HTTP_TIMEOUT = 8.0


class ServerSimulator:
    """
    4-provider asset-class-routed JIT trade monitor.
    Binance WS for crypto, TD for forex, FH for stocks, FMP for indices/commodities.
    """

    def __init__(self):
        global _circuit_breakers
        self.telegram = TelegramBot()
        self.client = httpx.AsyncClient(timeout=HTTP_TIMEOUT)
        self.running = True
        self.trade_state: Dict[int, Dict] = {}
        self.cache = PriceCache()
        self.crypto_ws = CryptoWSManager(self.cache)

        # Circuit breakers
        _circuit_breakers = {
            "twelvedata": CircuitBreaker("twelvedata", failure_threshold=5, cooldown=60),
            "finnhub": CircuitBreaker("finnhub", failure_threshold=5, cooldown=30),
            "fmp": CircuitBreaker("fmp", failure_threshold=5, cooldown=60),
            "kraken_ws": CircuitBreaker("kraken_ws", failure_threshold=3, cooldown=30),
        }
        self.cb = _circuit_breakers

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # ASSET-CLASS PRICE ROUTER
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def fetch_prices(self, tickers: Set[str]) -> Dict[str, float]:
        """Route each symbol to the best provider by asset class."""
        prices: Dict[str, float] = {}
        remaining: Set[str] = set()

        # ── 1. Check cache first (includes Binance WS prices) ──
        for sym in tickers:
            cached = self.cache.get(sym)
            if cached:
                prices[sym] = cached
            else:
                remaining.add(sym)

        if not remaining:
            return prices

        # ── 2. Classify remaining by asset class ──
        forex_metals = set()
        stocks = set()
        indices_commodities = set()
        crypto_rest = set()  # crypto WS miss — use REST fallback

        for sym in remaining:
            cls = classify_symbol(sym)
            if cls in ("FOREX", "METAL"):
                forex_metals.add(sym)
            elif cls == "STOCK":
                stocks.add(sym)
            elif cls in ("INDEX", "COMMODITY"):
                indices_commodities.add(sym)
            elif cls == "CRYPTO":
                crypto_rest.add(sym)
            else:
                indices_commodities.add(sym)  # default to FMP

        # ── 3. Fetch by provider (parallel where possible) ──
        tasks = []

        if forex_metals:
            tasks.append(self._fetch_forex_metals(forex_metals))
        if stocks:
            tasks.append(self._fetch_stocks(stocks))
        if indices_commodities:
            tasks.append(self._fetch_indices_commodities(indices_commodities))
        if crypto_rest:
            tasks.append(self._fetch_crypto_rest(crypto_rest))

        results = await asyncio.gather(*tasks, return_exceptions=True)

        for result in results:
            if isinstance(result, dict):
                prices.update(result)
                for sym, price in result.items():
                    self.cache.set(sym, price)

        # ── 4. Stale fallback for anything still missing ──
        still_missing = tickers - set(prices.keys())
        for sym in still_missing:
            stale = self.cache.get_stale(sym)
            if stale:
                prices[sym] = stale

        return prices

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # PROVIDER: TwelveData (forex + metals)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def _fetch_forex_metals(self, tickers: Set[str]) -> Dict[str, float]:
        """Primary: TwelveData batch. Fallback: FMP batch."""
        prices = {}

        # Try TwelveData
        if TWELVEDATA_API_KEY and self.cb["twelvedata"].allow_request():
            td_prices = await self._td_batch(tickers)
            prices.update(td_prices)

        # FMP fallback for misses
        missing = tickers - set(prices.keys())
        if missing and FMP_API_KEY and self.cb["fmp"].allow_request():
            fmp_prices = await self._fmp_batch(missing)
            prices.update(fmp_prices)

        return prices

    async def _td_batch(self, tickers: Set[str]) -> Dict[str, float]:
        """Batched TwelveData /price call."""
        td_symbols = []
        td_to_internal = {}
        for t in tickers:
            td_sym = TD_MAP.get(t)
            if td_sym:
                td_symbols.append(td_sym)
                td_to_internal[td_sym] = t

        if not td_symbols:
            return {}

        try:
            resp = await self.client.get(
                "https://api.twelvedata.com/price",
                params={"symbol": ",".join(td_symbols), "apikey": TWELVEDATA_API_KEY},
            )
            _td_throttle.record_credits(len(td_symbols))
            self.cb["twelvedata"].extra_stats["batch_calls"] = \
                self.cb["twelvedata"].extra_stats.get("batch_calls", 0) + 1

            if resp.status_code != 200:
                self.cb["twelvedata"].record_failure()
                return {}

            data = resp.json()
            if isinstance(data, dict) and data.get("status") == "error":
                self.cb["twelvedata"].record_failure()
                return {}

            prices = {}
            if len(td_symbols) == 1:
                price_str = data.get("price")
                if price_str:
                    try:
                        p = float(price_str)
                        if p > 0:
                            prices[td_to_internal[td_symbols[0]]] = p
                            self.cb["twelvedata"].record_success()
                    except (ValueError, TypeError):
                        pass
            else:
                for td_sym, td_data in data.items():
                    internal = td_to_internal.get(td_sym)
                    if not internal:
                        internal = TD_REVERSE.get(td_sym)
                    if not internal or not isinstance(td_data, dict):
                        continue
                    if td_data.get("status") == "error":
                        continue
                    try:
                        p = float(td_data.get("price", 0))
                        if p > 0:
                            prices[internal] = p
                            self.cb["twelvedata"].record_success()
                    except (ValueError, TypeError):
                        pass

            return prices

        except httpx.TimeoutException:
            self.cb["twelvedata"].record_failure()
            logger.debug(f"TD timeout for {len(td_symbols)} symbols")
            return {}
        except Exception as e:
            self.cb["twelvedata"].record_failure()
            logger.debug(f"TD error: {e}")
            return {}

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # PROVIDER: Finnhub (US stocks)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def _fetch_stocks(self, tickers: Set[str]) -> Dict[str, float]:
        """Primary: Finnhub individual. Fallback: FMP batch."""
        prices = {}

        if FINNHUB_API_KEY and self.cb["finnhub"].allow_request():
            for sym in tickers:
                fh_sym = FH_MAP.get(sym)
                if not fh_sym:
                    continue
                try:
                    resp = await self.client.get(
                        "https://finnhub.io/api/v1/quote",
                        params={"symbol": fh_sym, "token": FINNHUB_API_KEY},
                    )
                    if resp.status_code == 200:
                        data = resp.json()
                        p = data.get("c", 0)
                        if p and float(p) > 0:
                            prices[sym] = float(p)
                            self.cb["finnhub"].record_success()
                        else:
                            self.cb["finnhub"].record_failure()
                    else:
                        self.cb["finnhub"].record_failure()
                except (httpx.TimeoutException, httpx.RequestError):
                    self.cb["finnhub"].record_failure()

        # FMP fallback for misses
        missing = tickers - set(prices.keys())
        if missing and FMP_API_KEY and self.cb["fmp"].allow_request():
            fmp_prices = await self._fmp_batch(missing)
            prices.update(fmp_prices)

        return prices

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # PROVIDER: FMP (indices, commodities, universal fallback)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def _fetch_indices_commodities(self, tickers: Set[str]) -> Dict[str, float]:
        """Primary: FMP batch. Fallback: TwelveData."""
        prices = {}

        if FMP_API_KEY and self.cb["fmp"].allow_request():
            prices = await self._fmp_batch(tickers)

        # TD fallback for misses
        missing = tickers - set(prices.keys())
        if missing and TWELVEDATA_API_KEY and self.cb["twelvedata"].allow_request():
            td_prices = await self._td_batch(missing)
            prices.update(td_prices)

        return prices

    async def _fmp_batch(self, tickers: Set[str]) -> Dict[str, float]:
        """Batched FMP /quote call."""
        fmp_symbols = []
        fmp_to_internal = {}
        for t in tickers:
            fmp_sym = FMP_MAP.get(t, t)
            fmp_symbols.append(fmp_sym)
            fmp_to_internal[fmp_sym] = t

        if not fmp_symbols:
            return {}

        try:
            resp = await self.client.get(
                f"https://financialmodelingprep.com/api/v3/quote/{','.join(fmp_symbols)}",
                params={"apikey": FMP_API_KEY},
            )
            self.cb["fmp"].extra_stats["batch_calls"] = \
                self.cb["fmp"].extra_stats.get("batch_calls", 0) + 1

            if resp.status_code != 200:
                self.cb["fmp"].record_failure()
                return {}

            data = resp.json()
            if not isinstance(data, list):
                self.cb["fmp"].record_failure()
                return {}

            prices = {}
            for item in data:
                fmp_sym = item.get("symbol", "")
                internal = fmp_to_internal.get(fmp_sym)
                if not internal:
                    internal = FMP_REVERSE.get(fmp_sym)
                if not internal:
                    continue
                p = item.get("price", 0)
                if p and float(p) > 0:
                    prices[internal] = float(p)
                    self.cb["fmp"].record_success()

            return prices

        except httpx.TimeoutException:
            self.cb["fmp"].record_failure()
            return {}
        except Exception as e:
            self.cb["fmp"].record_failure()
            logger.debug(f"FMP error: {e}")
            return {}

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # PROVIDER: Kraken REST (crypto fallback when WS is down)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def _fetch_crypto_rest(self, tickers: Set[str]) -> Dict[str, float]:
        """REST fallback using Kraken public API — no API key needed."""
        prices = {}
        # Batch all crypto into one Kraken request
        kraken_pairs = []
        pair_to_internal = {}
        for sym in tickers:
            kpair = KRAKEN_REST_PAIRS.get(sym)
            if kpair:
                kraken_pairs.append(kpair)
                pair_to_internal[kpair] = sym

        if not kraken_pairs:
            return prices

        try:
            resp = await self.client.get(
                f"{KRAKEN_REST_URL}/0/public/Ticker",
                params={"pair": ",".join(kraken_pairs)},
            )
            if resp.status_code == 200:
                data = resp.json()
                result = data.get("result", {})
                for kpair, info in result.items():
                    # Kraken returns keys that may differ from input (e.g., XXBTZUSD)
                    # Match by checking all our mappings
                    for req_pair, internal in pair_to_internal.items():
                        if kpair == req_pair or kpair.replace("X", "").replace("Z", "") in req_pair:
                            last_trade = info.get("c", [None])[0]  # "c" = last trade [price, volume]
                            if last_trade:
                                p = float(last_trade)
                                if p > 0:
                                    prices[internal] = p
                            break
        except Exception as e:
            logger.debug(f"Kraken REST error: {e}")

        return prices

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # TRADE STATE + EVALUATION (preserved from previous version)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def _init_trade_state(self, trade: Trade) -> Dict:
        return {
            "current_sl": trade.stop_loss or 0,
            "high_water": trade.entry_price or 0,
            "low_water": trade.entry_price or 0,
            "tp1_hit": False,
            "partial_closed": False,
            "original_sl": trade.stop_loss or 0,
        }

    async def _check_executed_trade(self, trade: Trade, price: float, db: Session):
        if not price or not trade.entry_price:
            return
        if trade.id not in self.trade_state:
            self.trade_state[trade.id] = self._init_trade_state(trade)

        state = self.trade_state[trade.id]
        entry = trade.entry_price
        sl = state["current_sl"]
        tp1 = trade.take_profit_1 or 0
        tp2 = getattr(trade, "take_profit_2", 0) or 0
        is_long = (trade.direction or "").upper() in ["LONG", "BUY"]
        pip_size, pip_value = get_pip_info(trade.symbol)

        if is_long and price > state["high_water"]:
            state["high_water"] = price
        if not is_long and (price < state["low_water"] or state["low_water"] == 0):
            state["low_water"] = price

        if not state["tp1_hit"] and tp1 > 0:
            tp1_reached = (is_long and price >= tp1) or (not is_long and price <= tp1)
            if tp1_reached:
                state["tp1_hit"] = True
                buffer = 2 * pip_size
                state["current_sl"] = (entry + buffer) if is_long else (entry - buffer)
                sl = state["current_sl"]
                logger.info(f"SIM #{trade.id} | TP1 HIT @ {tp1} | SL → BE: {sl}")

                # MetaApi: move SL to breakeven on broker demo
                try:
                    from app.services.metaapi_executor import get_executor, is_enabled
                    if is_enabled():
                        await get_executor().modify_position(
                            trade_id=trade.id, stop_loss=sl,
                        )
                except Exception:
                    pass  # non-blocking

        if state["tp1_hit"]:
            trail_dist = abs(entry - state["original_sl"])
            if trail_dist > 0:
                if is_long:
                    new_sl = state["high_water"] - trail_dist
                    if new_sl > state["current_sl"]:
                        state["current_sl"] = new_sl
                        sl = new_sl
                else:
                    new_sl = state["low_water"] + trail_dist
                    if new_sl < state["current_sl"]:
                        state["current_sl"] = new_sl
                        sl = new_sl

        hit_sl = sl > 0 and ((is_long and price <= sl) or (not is_long and price >= sl))
        hit_tp2 = state["tp1_hit"] and tp2 > 0 and (
            (is_long and price >= tp2) or (not is_long and price <= tp2)
        )
        time_expired = self._is_time_expired(trade)

        if not (hit_sl or hit_tp2 or time_expired):
            return

        # Determine prefix based on trade origin
        is_sim = trade.status == "SIM_OPEN"
        pfx = "SIM" if is_sim else "SRV"

        if hit_tp2:
            exit_price, reason = tp2, f"{pfx}_TP2_HIT"
        elif hit_sl:
            exit_price = sl
            reason = f"{pfx}_TRAILING_SL" if state["tp1_hit"] else f"{pfx}_SL_HIT"
        else:
            exit_price, reason = price, f"{pfx}_TIME_EXIT"

        pnl_pips = ((exit_price - entry) if is_long else (entry - exit_price)) / pip_size
        pnl_dollars = pnl_pips * pip_value * (trade.lot_size or 0.1)

        await self._close_trade(
            trade, db,
            exit_price=exit_price, pnl_pips=pnl_pips, pnl_dollars=pnl_dollars,
            reason=reason, status=f"{pfx}_CLOSED",
            mfe_pips=abs(
                (state["high_water"] - entry) if is_long else (entry - state["low_water"])
            ) / pip_size if pip_size else None,
            mae_pips=abs(
                (entry - state["low_water"]) if is_long else (state["high_water"] - entry)
            ) / pip_size if pip_size else None,
        )

    async def _check_virtual_trade(self, trade: Trade, price: float, db: Session):
        if not price or not trade.entry_price:
            return
        entry = trade.entry_price
        sl = trade.stop_loss or 0
        tp1 = trade.take_profit_1 or 0
        is_long = (trade.direction or "").upper() in ["LONG", "BUY"]
        pip_size, pip_value = get_pip_info(trade.symbol)

        hit_sl = sl > 0 and ((is_long and price <= sl) or (not is_long and price >= sl))
        hit_tp = tp1 > 0 and ((is_long and price >= tp1) or (not is_long and price <= tp1))
        time_expired = self._is_time_expired(trade)

        if not (hit_sl or hit_tp or time_expired):
            return

        if hit_tp:
            exit_price, reason = tp1, "ONIAI_TP1_HIT"
        elif hit_sl:
            exit_price, reason = sl, "ONIAI_SL_HIT"
        else:
            exit_price, reason = price, "ONIAI_TIME_EXIT"

        pnl_pips = ((exit_price - entry) if is_long else (entry - exit_price)) / pip_size
        pnl_dollars = pnl_pips * pip_value * (trade.lot_size or 0.1)

        await self._close_trade(
            trade, db,
            exit_price=exit_price, pnl_pips=pnl_pips, pnl_dollars=pnl_dollars,
            reason=reason, status="ONIAI_CLOSED",
        )

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # UNIFIED TRADE CLOSE
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def _close_trade(
        self, trade: Trade, db: Session, *,
        exit_price: float, pnl_pips: float, pnl_dollars: float,
        reason: str, status: str,
        mfe_pips: float = None, mae_pips: float = None,
    ):
        trade.exit_price = round(exit_price, 5)
        trade.pnl_dollars = round(pnl_dollars, 2)
        trade.pnl_pips = round(pnl_pips, 1)
        trade.close_reason = reason
        trade.status = status
        trade.closed_at = datetime.now(timezone.utc)

        desk_state = db.query(DeskState).filter(DeskState.desk_id == trade.desk_id).first()
        if desk_state:
            desk_state.open_positions = max(0, (desk_state.open_positions or 0) - 1)
            desk_state.daily_pnl = (desk_state.daily_pnl or 0) + pnl_dollars
            if pnl_dollars < 0:
                desk_state.daily_loss = (desk_state.daily_loss or 0) + pnl_dollars
                desk_state.consecutive_losses = (desk_state.consecutive_losses or 0) + 1
            else:
                desk_state.consecutive_losses = 0
        db.commit()

        if trade.id in self.trade_state:
            del self.trade_state[trade.id]

        tag = "\U0001f916 OniAI" if "ONIAI" in reason else ("\U0001f4ca SIM" if "SIM" in reason else "\U0001f5a5\ufe0f SRV")
        hold_str = ""
        if trade.opened_at:
            hold_min = (trade.closed_at - trade.opened_at).total_seconds() / 60
            hold_str = f" | Held: {hold_min:.0f}m"
        logger.info(
            f"{tag} CLOSED | #{trade.id} | {trade.symbol} {trade.direction} | "
            f"Entry: {trade.entry_price} | Exit: {exit_price} | "
            f"PnL: ${pnl_dollars:+.2f} ({pnl_pips:+.1f}p) | {reason}{hold_str}"
        )

        try:
            from app.services.ml_data_logger import MLDataLogger
            duration = 0
            if trade.opened_at:
                duration = (trade.closed_at - trade.opened_at).total_seconds() / 60
            MLDataLogger().log_outcome(
                db=db, trade_id=trade.id, signal_id=trade.signal_id,
                exit_price=exit_price, exit_reason=reason,
                pnl_pips=round(pnl_pips, 1), pnl_dollars=round(pnl_dollars, 2),
                hold_time_minutes=duration,
                max_favorable_pips=mfe_pips, max_adverse_pips=mae_pips,
            )
            db.commit()
        except Exception as e:
            logger.debug(f"ML outcome log failed: {e}")

        try:
            await self.telegram.notify_trade_exit(
                symbol=trade.symbol, desk_id=trade.desk_id,
                pnl=pnl_dollars, reason=reason,
            )
        except Exception as e:
            logger.error(f"Telegram close failed: {e}")

        # ── MetaApi $1M demo: close position ──
        try:
            from app.services.metaapi_executor import get_executor, is_enabled
            if is_enabled():
                await get_executor().close_trade(
                    trade_id=trade.id, symbol=trade.symbol or "",
                )
        except Exception as e:
            logger.debug(f"MetaApi close failed (non-blocking): {e}")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # TIME EXIT HELPERS
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def _is_time_expired(self, trade: Trade) -> bool:
        desk = DESKS.get(trade.desk_id, {})
        max_hold = desk.get("max_hold_hours", 24)
        if not trade.opened_at:
            return False
        elapsed = (datetime.now(timezone.utc) - trade.opened_at).total_seconds()
        return elapsed >= max_hold * 3600

    async def _force_time_exit(self, trade: Trade, db: Session):
        exit_price = trade.entry_price or 0
        is_sim = trade.status == "SIM_OPEN"
        is_oniai = trade.status in ("ONIAI_OPEN", "VIRTUAL_OPEN")
        if is_sim:
            reason = "SIM_TIME_EXIT_NOPRICE"
            status = "SIM_CLOSED"
        elif is_oniai:
            reason = "ONIAI_TIME_EXIT_NOPRICE"
            status = "ONIAI_CLOSED"
        else:
            reason = "SRV_TIME_EXIT_NOPRICE"
            status = "SRV_CLOSED"
        elapsed = 0
        if trade.opened_at:
            elapsed = (datetime.now(timezone.utc) - trade.opened_at).total_seconds() / 3600
        logger.warning(
            f"TIME EXIT (no price) | #{trade.id} | {trade.symbol} "
            f"{trade.direction} | Held {elapsed:.1f}h"
        )
        await self._close_trade(
            trade, db, exit_price=exit_price, pnl_pips=0.0, pnl_dollars=0.0,
            reason=reason, status=status,
        )

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # MAIN LOOP
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def run(self):
        """4-provider JIT loop with adaptive throttling."""
        # Start crypto WebSocket feeds (Kraken + CoinCap — US-legal, no API key)
        await self.crypto_ws.start()

        logger.info(
            f"4-Provider JIT Simulator started | "
            f"TD: {'ON' if TWELVEDATA_API_KEY else 'OFF'} | "
            f"FH: {'ON' if FINNHUB_API_KEY else 'OFF'} | "
            f"FMP: {'ON' if FMP_API_KEY else 'OFF'} | "
            f"Crypto WS: Kraken + CoinCap"
        )

        while self.running:
            try:
                db = SessionLocal()
                try:
                    open_trades = (
                        db.query(Trade)
                        .filter(Trade.status.in_(OPEN_STATUSES))
                        .all()
                    )

                    if not open_trades:
                        await asyncio.sleep(15)
                        continue

                    tickers = set(t.symbol for t in open_trades if t.symbol)
                    prices = await self.fetch_prices(tickers)

                    for trade in open_trades:
                        price = prices.get(trade.symbol)
                        is_virtual = trade.status in ("ONIAI_OPEN", "VIRTUAL_OPEN")

                        if price:
                            if is_virtual:
                                await self._check_virtual_trade(trade, price, db)
                            else:
                                # SIM_OPEN, EXECUTED, OPEN — all get full trailing/TP1/TP2 logic
                                await self._check_executed_trade(trade, price, db)
                        else:
                            if self._is_time_expired(trade):
                                await self._force_time_exit(trade, db)

                    logger.debug(
                        f"JIT cycle | {len(open_trades)} trades | "
                        f"{len(prices)}/{len(tickers)} prices | "
                        f"TD credits/60s: {_td_throttle.credits_last_60s()} | "
                        f"Kraken: {'✓' if self.crypto_ws.kraken_connected else '✗'} "
                        f"CoinCap: {'✓' if self.crypto_ws.coincap_connected else '✗'}"
                    )

                finally:
                    db.close()

            except Exception as e:
                logger.error(f"Simulator error: {e}", exc_info=True)

            # Adaptive sleep based on TD credit usage
            interval = _td_throttle.get_interval()
            await asyncio.sleep(interval)

    async def stop(self):
        self.running = False
        await self.crypto_ws.stop()
        await self.client.aclose()
        await self.telegram.close()
        logger.info("4-Provider JIT Simulator stopped")
```

## `app/services/telegram_bot.py`

```python
"""
Telegram Bot Service — Dashboard-Style Notifications v2.0
Per-desk channel routing with System channel for health/risk alerts.

Routing:
  - Trade entries/exits/skips/OniAI -> desk-specific channel
  - Entry/exit summaries, daily briefs, weekly memos -> Portfolio channel
  - Drawdown, kill switch, health, diagnostics -> System channel
  - Firm drawdown, kill switch -> ALL channels
"""
import logging
import time
from datetime import datetime, timezone
from typing import Dict, Optional

import httpx

from app.config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    TELEGRAM_DESK_CHANNELS,
    TELEGRAM_PORTFOLIO_CHAT,
    TELEGRAM_SYSTEM_CHAT,
    DESKS,
    get_pip_info,
)

logger = logging.getLogger("TradingSystem.Telegram")

TELEGRAM_API = "https://api.telegram.org/bot{token}"

BAR = "\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501\u2501"
SCORE_FILLED = "\U0001f7ea"
SCORE_EMPTY = "\u2b1c"

DESK_EMOJI = {
    "DESK1_SCALPER": "\U0001f7e2", "DESK2_INTRADAY": "\U0001f7e1",
    "DESK3_SWING": "\U0001f535", "DESK4_GOLD": "\U0001f534",
    "DESK5_ALTS": "\u26ab", "DESK6_EQUITIES": "\u26aa",
}
DESK_LABEL = {
    "DESK1_SCALPER": "SCALPER", "DESK2_INTRADAY": "INTRADAY",
    "DESK3_SWING": "SWING", "DESK4_GOLD": "GOLD",
    "DESK5_ALTS": "ALTS", "DESK6_EQUITIES": "EQUITIES",
}

TF_MAP = {
    "1": "1M", "5": "5M", "15": "15M", "30": "30M",
    "60": "1H", "240": "4H", "1440": "D",
    "M1": "1M", "M5": "5M", "M15": "15M", "M30": "30M",
    "H1": "1H", "H4": "4H", "D1": "D", "W1": "W",
    "1M": "1M", "5M": "5M", "15M": "15M", "30M": "30M",
    "1H": "1H", "4H": "4H", "D": "D", "W": "W",
}

TREND_EMOJI = {
    "STRONG_UP": "\U0001f7e2\U0001f4c8", "UP": "\U0001f7e2\u2197\ufe0f",
    "WEAK_UP": "\U0001f7e1\u2197\ufe0f",
    "STRONG_DOWN": "\U0001f534\U0001f4c9", "DOWN": "\U0001f534\u2198\ufe0f",
    "WEAK_DOWN": "\U0001f7e1\u2198\ufe0f",
    "RANGING": "\u26aa\u2194\ufe0f", "UNKNOWN": "\u2753",
}


def _score_bar(score, max_score=10):
    filled = min(int(float(score) if score else 0), max_score)
    return SCORE_FILLED * filled + SCORE_EMPTY * (max_score - filled)


def _score_label(score):
    s = int(float(score)) if score else 0
    if s >= 8: return "ELITE"
    if s >= 7: return "STRONG"
    if s >= 5: return "MODERATE"
    return "WEAK"


def _pnl_emoji(pnl): return "\u2705" if pnl >= 0 else "\u274c"
def _desk_dot(pnl): return "\U0001f7e2" if pnl > 0 else ("\U0001f534" if pnl < 0 else "\u26aa")
def _format_tf(tf): return TF_MAP.get(str(tf).strip().upper(), str(tf))


def _gold_sub(desk, timeframe):
    if desk != "DESK4_GOLD": return ""
    tf = str(timeframe).upper()
    if tf in ["5", "5M", "M5"]: return " \u26a1 SCALP"
    if tf in ["60", "1H", "H1"]: return " \U0001f4c8 INTRA"
    if tf in ["240", "4H", "H4"]: return " \U0001f3db SWING"
    return ""


def _trend_align(direction, trend):
    is_long = direction.upper() in ["LONG", "BUY"]
    up = trend in ["STRONG_UP", "UP", "WEAK_UP"]
    down = trend in ["STRONG_DOWN", "DOWN", "WEAK_DOWN"]
    if (is_long and up) or (not is_long and down): return "\u2705 WITH TREND"
    if (is_long and down) or (not is_long and up): return "\u26a0\ufe0f COUNTER-TREND"
    return "\u2194\ufe0f NEUTRAL"


def _conf_bar(confidence):
    pct = float(confidence) * 100 if confidence else 0
    filled = int(pct / 10)
    return "\u2593" * filled + "\u2591" * (10 - filled) + f" {pct:.0f}%"


class TelegramBot:
    def __init__(self):
        self.token = TELEGRAM_BOT_TOKEN
        self.portfolio_chat = TELEGRAM_PORTFOLIO_CHAT
        self.system_chat = TELEGRAM_SYSTEM_CHAT
        self.desk_channels = TELEGRAM_DESK_CHANNELS
        self.fallback_chat = TELEGRAM_CHAT_ID
        self.client = httpx.AsyncClient(timeout=10.0)
        self.enabled = bool(self.token)
        if not self.enabled:
            logger.warning("Telegram not configured. Set TELEGRAM_BOT_TOKEN.")
        else:
            logger.info(f"Telegram enabled | {len(self.desk_channels)} desks + Portfolio + System")

    def _get_desk_chat(self, desk_id):
        return self.desk_channels.get(desk_id) or self.portfolio_chat or self.fallback_chat

    def _format_timeframe(self, tf):
        return _format_tf(tf)

    async def send_message(self, text, chat_id=None, parse_mode="HTML"):
        if not self.enabled:
            logger.debug(f"[TG-DISABLED] {text[:100]}")
            return False
        target = chat_id or self.portfolio_chat or self.fallback_chat
        if not target:
            return False
        try:
            resp = await self.client.post(
                f"{TELEGRAM_API.format(token=self.token)}/sendMessage",
                json={"chat_id": target, "text": text, "parse_mode": parse_mode, "disable_web_page_preview": True},
            )
            if resp.status_code == 200:
                return True
            logger.error(f"Telegram API error: {resp.status_code} {resp.text[:200]}")
            return False
        except Exception as e:
            logger.error(f"Telegram send failed: {e}")
            return False

    async def _send_to_desk(self, desk_id, text):
        return await self.send_message(text, chat_id=self._get_desk_chat(desk_id))

    async def _send_to_portfolio(self, text):
        return await self.send_message(text, chat_id=self.portfolio_chat)

    async def _send_to_system(self, text):
        return await self.send_message(text, chat_id=self.system_chat)

    async def _send_to_all(self, text):
        for chat_id in self.desk_channels.values():
            await self.send_message(text, chat_id=chat_id)
        await self._send_to_portfolio(text)
        await self._send_to_system(text)

    # ━━━ TRADE ENTRY ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def notify_trade_entry(self, trade_params: Dict, decision: Dict):
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
        timeframe = trade_params.get("timeframe", "?")
        trend = trade_params.get("trend", "UNKNOWN")
        rsi_val = trade_params.get("rsi")
        exec_time = trade_params.get("exec_time_s", 0)

        dl = DESK_LABEL.get(desk, desk)
        de = DESK_EMOJI.get(desk, "\U0001f4ca")
        tf = _format_tf(timeframe)
        gold = _gold_sub(desk, timeframe)
        te = TREND_EMOJI.get(trend, "\u2753")
        tl = trend.replace("_", " ")
        align = _trend_align(direction, trend)
        dir_e = "\U0001f7e2" if direction.upper() in ["LONG", "BUY"] else "\U0001f534"

        sl_d, tp1_d, tp2_d = str(sl), str(tp1), ""
        if tp2 and str(tp2) not in ["0", "None", ""]:
            tp2_d = str(tp2)
        proj = ""
        rr_line = ""
        rr_val = 0

        try:
            ef = float(price) if price else 0
            sf = float(sl) if sl else 0
            t1f = float(tp1) if tp1 else 0
            t2f = float(tp2) if tp2 else 0
            if ef > 0:
                ps, pv = get_pip_info(str(symbol))
                sp = abs(ef - sf) / ps if sf > 0 else 0
                t1p = abs(t1f - ef) / ps if t1f > 0 else 0
                t2p = abs(t2f - ef) / ps if t2f > 0 else 0
                rr_val = round(t1p / sp, 1) if sp > 0 else 0
                rr_line = f"   \u2696\ufe0f  RR             1:{rr_val}\n"
                sl_d = f"{sl}  (\u2212{sp:.0f}p)"
                tp1_d = f"{tp1}  (+{t1p:.0f}p)"
                if t2f > 0:
                    tp2_d = f"{tp2}  (+{t2p:.0f}p)"
                lots = [0.01, 0.1, 1.0]
                sl_l = [round(sp * pv * l, 0) for l in lots]
                t1_l = [round(t1p * pv * l, 0) for l in lots]
                t2_l = [round(t2p * pv * l, 0) for l in lots]
                proj = "\n\u2501\u2501\u2501 PROJECTED P&L \u2501\u2501\u2501\u2501\u2501\u2501\n\n"
                if t2p > 0:
                    proj += f"<code>\U0001f4b5 0.01    -${sl_l[0]:.0f}  \u2192  +${t1_l[0]:.0f}  \u2192  +${t2_l[0]:.0f}\n\U0001f4b5 0.10    -${sl_l[1]:.0f}  \u2192  +${t1_l[1]:.0f}  \u2192  +${t2_l[1]:.0f}\n\U0001f4b5 1.00    -${sl_l[2]:,.0f} \u2192  +${t1_l[2]:,.0f} \u2192  +${t2_l[2]:,.0f}</code>"
                else:
                    proj += f"<code>\U0001f4b5 0.01    -${sl_l[0]:.0f}  \u2192  +${t1_l[0]:.0f}\n\U0001f4b5 0.10    -${sl_l[1]:.0f}  \u2192  +${t1_l[1]:.0f}\n\U0001f4b5 1.00    -${sl_l[2]:,.0f} \u2192  +${t1_l[2]:,.0f}</code>"
        except Exception as e:
            logger.debug(f"Projection calc failed: {e}")

        score = int(float(confidence) * 10) if confidence else 0
        sb = _score_bar(score)
        sl_lbl = _score_label(score)
        cb = _conf_bar(confidence)
        now = datetime.now(timezone.utc).strftime("%H:%M UTC")
        rsi_line = f"   \U0001f4c9  RSI            {rsi_val:.0f}\n" if rsi_val is not None else ""
        ex = f"\u23f1 {exec_time:.1f}s  \u00b7  " if exec_time and exec_time > 0 else ""

        text = (
            f"<b>\u27d0 OniQuant</b>\n"
            f"<b>{BAR}</b>\n"
            f"{dir_e} <b>{symbol}  \u00b7  {direction}  \u00b7  {price}</b>\n"
            f"{de} <b>{dl}</b>{gold} \u00b7 {tf} \u00b7 {te} {tl}\n"
            f"\U0001f9ed {align}\n\n"
            f"   \U0001f534  SL     <code>{sl_d}</code>\n"
            f"   \U0001f3af  TP1    <code>{tp1_d}</code>\n"
        )
        if tp2_d:
            text += f"   \U0001f3af  TP2    <code>{tp2_d}</code>\n"
        text += (
            f"\n   \U0001f4b0  <b>${risk:.2f}</b>  \u00b7  {risk_pct:.2f}%\n"
            f"{rr_line}{rsi_line}"
            f"<b>{BAR}</b>\n"
            f"{sb}  <b>{score}/10</b>  {sl_lbl}\n"
            f"{proj}\n\n"
            f"<i>{reasoning}</i>\n\n"
            f"{ex}<i>{dl}{gold}  \u00b7  {now}</i>\n"
            f"\U0001f50b {cb}"
        )
        await self._send_to_desk(desk, text)

        summary = (
            f"\u27d0 {dir_e} <b>{symbol}</b> {direction} \u00b7 ${risk:.0f} \u00b7 RR {rr_val}\n"
            f"     {sb} <b>{score}</b>/10 \u00b7 {dl}{gold}"
        )
        await self._send_to_portfolio(summary)

    # ━━━ TRADE EXIT ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def notify_trade_exit(self, symbol, desk_id, pnl, reason):
        is_win = pnl >= 0
        re = "\u2705" if is_win else "\u274c"
        dl = DESK_LABEL.get(desk_id, desk_id)
        de = DESK_EMOJI.get(desk_id, "\U0001f4ca")
        dn = DESKS.get(desk_id, {}).get("name", desk_id)

        src = ""
        cr = reason
        if "SRV_" in reason: src = " \U0001f5a5\ufe0f"; cr = reason.replace("SRV_", "")
        elif "MT5_" in reason: src = " \U0001f4df"; cr = reason.replace("MT5_", "")
        elif "ONIAI_" in reason: src = " \U0001f916"; cr = reason.replace("ONIAI_", "")

        text = (
            f"<b>\u27d0 OniQuant</b>\n"
            f"<b>{BAR}</b>\n"
            f"{re} <b>{symbol}  \u00b7  CLOSED  \u00b7  ${pnl:+,.2f}</b>{src}\n\n"
            f"   {de}  Desk     {dl}\n"
            f"   \U0001f4cb  Reason   {cr}\n"
            f"<b>{BAR}</b>\n"
            f"<i>{datetime.now(timezone.utc).strftime('%H:%M UTC')}</i>"
        )
        await self._send_to_desk(desk_id, text)
        await self._send_to_portfolio(f"\u27d0 {re} <b>{symbol}</b> ${pnl:+.2f} \u00b7 {cr} \u00b7 {dl}")

    # ━━━ OniAI VIRTUAL TRADE ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def notify_oniai_signal(self, trade_params: Dict, decision: Dict):
        symbol = trade_params.get("symbol", "?")
        direction = trade_params.get("direction", "?")
        desk = trade_params.get("desk_id", "?")
        price = trade_params.get("price", 0)
        sl = trade_params.get("stop_loss", 0)
        tp1 = trade_params.get("take_profit_1", 0)
        confidence = decision.get("confidence", 0)
        reasoning = decision.get("reasoning", "")[:150]
        block_reason = trade_params.get("block_reason", "Unknown")
        timeframe = trade_params.get("timeframe", "?")
        trend = trade_params.get("trend", "UNKNOWN")

        dl = DESK_LABEL.get(desk, desk)
        de = DESK_EMOJI.get(desk, "\U0001f4ca")
        tf = _format_tf(timeframe)
        gold = _gold_sub(desk, timeframe)
        te = TREND_EMOJI.get(trend, "\u2753")
        tl = trend.replace("_", " ")
        align = _trend_align(direction, trend)
        arrow = "\u2191" if direction.upper() in ["LONG", "BUY"] else "\u2193"

        proj = ""
        try:
            ef, sf, t1f = float(price or 0), float(sl or 0), float(tp1 or 0)
            if ef > 0 and sf > 0 and t1f > 0:
                ps, pv = get_pip_info(str(symbol))
                sp = abs(ef - sf) / ps
                t1p = abs(t1f - ef) / ps
                rr = round(t1p / sp, 1) if sp > 0 else 0
                proj = f"\n   \u2696\ufe0f  RR 1:{rr} \u00b7 SL {sp:.0f}p \u00b7 TP1 {t1p:.0f}p"
        except:
            pass

        text = (
            f"<b>\u27d0 OniQuant</b>\n"
            f"<b>{BAR}</b>\n"
            f"\U0001f916 <b>OniAI SIGNAL  \u00b7  {symbol} {arrow} {direction}</b>\n"
            f"{de} <b>{dl}</b>{gold} \u00b7 {tf} \u00b7 {te} {tl}\n"
            f"\U0001f9ed {align}\n\n"
            f"   \U0001f4b2  Entry    <code>{price}</code>\n"
            f"   \U0001f534  SL      <code>{sl}</code>\n"
            f"   \U0001f3af  TP1     <code>{tp1}</code>\n"
            f"   \U0001f4ca  Conf    {confidence}{proj}\n\n"
            f"   \u26a0\ufe0f  <b>Not executed:</b> {block_reason}\n"
            f"   \U0001f52c  Tracking as virtual trade\n"
            f"<b>{BAR}</b>\n"
            f"<i>{reasoning}</i>"
        )
        await self._send_to_desk(desk, text)
        await self._send_to_portfolio(
            f"\u27d0 \U0001f916 <b>OniAI</b> \u00b7 {dl}{gold} \u00b7 {symbol} {arrow} {direction} @ {price} \u00b7 \u26a0\ufe0f {block_reason}"
        )

    # ━━━ PARTIAL CLOSE ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def notify_partial_close(self, symbol, desk_id, pnl, pct_closed=50, be_price=0):
        text = (
            f"<b>\u27d0 OniQuant</b>\n"
            f"<b>{BAR}</b>\n"
            f"\U0001f4d0 <b>{symbol}  \u00b7  PARTIAL CLOSE</b>\n\n"
            f"   \u2705  {pct_closed:.0f}% closed at TP1    <b>${pnl:+.2f}</b>\n"
            f"   \U0001f6e1\ufe0f  SL \u2192 breakeven       <code>{be_price}</code>\n"
            f"<b>{BAR}</b>\n"
            f"<i>Remaining {100 - pct_closed:.0f}% running to TP2</i>"
        )
        await self._send_to_desk(desk_id, text)

    # ━━━ SIGNAL SKIP ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def notify_signal_skip(self, symbol, desk_id, reason, score, timeframe="?"):
        dl = DESK_LABEL.get(desk_id, desk_id)
        de = DESK_EMOJI.get(desk_id, "\U0001f4ca")
        tf = _format_tf(timeframe)
        gold = _gold_sub(desk_id, timeframe)
        sb = _score_bar(score)
        sl = _score_label(score)

        text = (
            f"<b>\u27d0 OniQuant</b>\n"
            f"<b>{BAR}</b>\n"
            f"\u2298 <b>{symbol}  \u00b7  SKIPPED</b>\n"
            f"{de} <b>{dl}</b>{gold} \u00b7 {tf}\n\n"
            f"   {sb}  <b>{score}/10</b>  {sl}\n"
            f"<i>{reason[:200]}</i>"
        )
        await self._send_to_desk(desk_id, text)

    # ━━━ RISK ALERTS -> System Channel ━━━━━━━━━━━━━━━━━━━━━━━━

    async def alert_drawdown(self, desk_id, daily_loss, level):
        dl = DESK_LABEL.get(desk_id, desk_id)
        text = (
            f"<b>\u27d0 OniQuant</b>\n<b>{BAR}</b>\n"
            f"\u26a0\ufe0f <b>DRAWDOWN  \u00b7  {dl}</b>\n\n"
            f"   \U0001f4c9  Daily loss     <b>\u2212${abs(daily_loss):.2f}</b>\n"
            f"   \U0001f4ca  Level          {level}\n"
            f"<b>{BAR}</b>\n<i>Action required if loss continues.</i>"
        )
        await self._send_to_system(text)
        await self._send_to_desk(desk_id, text)

    async def alert_consecutive_losses(self, desk_id, count, action):
        dl = DESK_LABEL.get(desk_id, desk_id)
        text = (
            f"<b>\u27d0 OniQuant</b>\n<b>{BAR}</b>\n"
            f"\U0001f534 <b>LOSS STREAK  \u00b7  {dl}</b>\n\n"
            f"   \U0001f53b  Streak         {count} consecutive\n"
            f"   \u2699\ufe0f  Action         {action}\n<b>{BAR}</b>"
        )
        await self._send_to_system(text)
        await self._send_to_desk(desk_id, text)

    async def alert_desk_paused(self, desk_id, reason):
        dl = DESK_LABEL.get(desk_id, desk_id)
        text = (
            f"<b>\u27d0 OniQuant</b>\n<b>{BAR}</b>\n"
            f"\u23f8\ufe0f <b>DESK PAUSED  \u00b7  {dl}</b>\n\n"
            f"   \U0001f53b  {reason}\n   \U0001f512  Manual resume required\n<b>{BAR}</b>"
        )
        await self._send_to_system(text)
        await self._send_to_desk(desk_id, text)

    async def alert_desk_resumed(self, desk_id):
        dl = DESK_LABEL.get(desk_id, desk_id)
        text = (
            f"<b>\u27d0 OniQuant</b>\n<b>{BAR}</b>\n"
            f"\u25b6\ufe0f <b>DESK RESUMED  \u00b7  {dl}</b>\n\n"
            f"   \u2705  Size reset to 100%\n   \u2705  Loss streak cleared\n<b>{BAR}</b>"
        )
        await self._send_to_system(text)
        await self._send_to_desk(desk_id, text)

    async def alert_firm_drawdown(self, total_loss, level):
        text = (
            f"<b>\u27d0 OniQuant</b>\n<b>{BAR}</b>\n"
            f"\U0001f6a8 <b>FIRM DRAWDOWN</b>\n\n"
            f"   \U0001f4c9  Total loss     <b>\u2212${abs(total_loss):.2f}</b>\n"
            f"   \U0001f4ca  Halt limit     {level}\n"
            f"   \u2699\ufe0f  All desks      \u2192 50% size\n<b>{BAR}</b>"
        )
        await self._send_to_all(text)

    async def alert_kill_switch(self, scope, triggered_by):
        text = (
            f"<b>\u27d0 OniQuant</b>\n<b>{BAR}</b>\n"
            f"\u26d4 <b>KILL SWITCH  \u00b7  {scope}</b>\n\n"
            f"   \U0001f4cb  {triggered_by}\n"
            f"   \U0001f512  Closing all positions\n"
            f"   \U0001f6ab  Trading halted\n<b>{BAR}</b>"
        )
        if scope == "ALL":
            await self._send_to_all(text)
        else:
            await self._send_to_system(text)
            await self._send_to_desk(scope, text)

    # ━━━ SYSTEM HEALTH -> System Channel ━━━━━━━━━━━━━━━━━━━━━━

    async def send_health_status(self, components: Dict):
        all_ok = all(v.get("status") == "ok" for v in components.values())
        he = "\U0001f6e0" if all_ok else "\u26a0\ufe0f"
        hl = "SYSTEM STATUS" if all_ok else "SYSTEM DEGRADED"
        lines = []
        for name, info in components.items():
            dot = "\U0001f7e2" if info.get("status") == "ok" else "\U0001f534"
            lines.append(f"   {name:<12} {dot}  {info.get('detail', '')}")
        now = datetime.now(timezone.utc).strftime("%H:%M UTC")
        text = f"<b>\u27d0 OniQuant</b>\n<b>{BAR}</b>\n{he} <b>{hl}</b>\n\n" + "\n".join(lines) + f"\n<b>{BAR}</b>\n<i>{now}</i>"
        await self._send_to_system(text)

    async def send_auto_repair(self, failed_service, fallback_service, detail=""):
        now = datetime.now(timezone.utc).strftime("%H:%M UTC")
        text = (
            f"<b>\u27d0 OniQuant</b>\n<b>{BAR}</b>\n"
            f"\U0001f527 <b>AUTO-REPAIR</b>\n\n"
            f"   \u274c  {failed_service:<16} timeout\n"
            f"   \u2705  {fallback_service:<16} active (fallback)\n"
            f"<b>{BAR}</b>\n<i>{detail}  \u00b7  {now}</i>"
        )
        await self._send_to_system(text)

    async def send_heartbeat(self, uptime="", trades_today=0, errors=0, sim_trades=0):
        text = (
            f"<b>\u27d0 OniQuant</b>\n<b>{BAR}</b>\n"
            f"\U0001f493 <b>HEARTBEAT</b>  \u00b7  \u2705\n\n"
            f"   \U0001f550  Uptime         {uptime}\n"
            f"   \U0001f4ca  Trades today   {trades_today}\n"
            f"   \u274c  Errors         {errors}\n"
            f"   \U0001f504  Sim trades     {sim_trades}\n<b>{BAR}</b>"
        )
        await self._send_to_system(text)

    # ━━━ DAILY BRIEF -> Portfolio + Desk Summaries ━━━━━━━━━━━━

    async def send_daily_report(self, report: Dict):
        date = report.get("date", "N/A")
        tp = report.get("total_pnl", 0)
        tt = report.get("total_trades", 0)
        wr = report.get("win_rate", 0)
        pe = _pnl_emoji(tp)
        best = report.get("best_trade", {})
        worst = report.get("worst_trade", {})

        text = (
            f"<b>\u27d0 OniQuant  \u00b7  Daily Brief</b>\n<b>{BAR}</b>\n"
            f"\U0001f4c5 <b>{date}</b>\n\n"
            f"   \U0001f4b0  Firm PnL       {pe} <b>${tp:+.2f}</b>\n"
            f"   \U0001f4ca  Trades         {tt}\n"
            f"   \U0001f3af  Win Rate       {wr:.0%}\n"
            f"<b>{BAR}</b>\n<b>DESK BREAKDOWN</b>\n\n"
        )
        for did, dd in report.get("desks", {}).items():
            n = DESKS.get(did, {}).get("name", did)
            p = dd.get("pnl", 0)
            t = dd.get("trades", 0)
            text += f"   {_desk_dot(p)} {n:<14} <b>${p:+.2f}</b>    {t} trades\n"
        text += f"<b>{BAR}</b>\n"
        if best: text += f"\U0001f3c6 Best   {best.get('symbol', '?')}  ${best.get('pnl', 0):+.2f}\n"
        if worst: text += f"\U0001f480 Worst  {worst.get('symbol', '?')}  ${worst.get('pnl', 0):+.2f}"
        await self._send_to_portfolio(text)

        for did, dd in report.get("desks", {}).items():
            n = DESKS.get(did, {}).get("name", did)
            p = dd.get("pnl", 0)
            t = dd.get("trades", 0)
            w = dd.get("win_rate", 0)
            dt = (
                f"<b>\u27d0 {n}  \u00b7  Daily</b>\n<b>{BAR}</b>\n"
                f"\U0001f4c5 <b>{date}</b>\n\n"
                f"   \U0001f4b0  PnL            {_pnl_emoji(p)} <b>${p:+.2f}</b>\n"
                f"   \U0001f4ca  Trades         {t}\n"
                f"   \U0001f3af  Win Rate       {w:.0%}\n<b>{BAR}</b>"
            )
            await self._send_to_desk(did, dt)

    # ━━━ WEEKLY MEMO -> Portfolio ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def send_weekly_memo(self, memo):
        header = f"<b>\u27d0 OniQuant  \u00b7  Weekly Memo</b>\n<b>{BAR}</b>\n\n"
        if len(header + memo) <= 4000:
            await self._send_to_portfolio(header + memo)
        else:
            await self._send_to_portfolio(header + memo[:3900] + "\n\n<i>(continued...)</i>")
            rem = memo[3900:]
            while rem:
                await self._send_to_portfolio(rem[:4000])
                rem = rem[4000:]

    # ━━━ STATUS -> System Channel ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def send_status(self, dashboard: Dict):
        text = (
            f"<b>\u27d0 OniQuant</b>\n<b>{BAR}</b>\n"
            f"\U0001f4c8 <b>FIRM STATUS</b>\n\n"
            f"   Status:     {dashboard.get('firm_status', '?')}\n"
            f"   Signals:    {dashboard.get('total_signals_today', 0)}\n"
            f"   Trades:     {dashboard.get('total_trades_today', 0)}\n"
            f"   Daily PnL:  ${dashboard.get('total_daily_pnl', 0):+.2f}\n"
            f"<b>{BAR}</b>\n"
        )
        for d in dashboard.get("desks", []):
            dot = "\U0001f7e2" if d.get("is_active") else "\U0001f534"
            text += f"   {dot} {d.get('name', '?'):<14} {d.get('trades_today', 0)} trades  ${d.get('daily_pnl', 0):+.2f}  L:{d.get('consecutive_losses', 0)}\n"
        await self._send_to_system(text)

    async def close(self):
        await self.client.aclose()
```

## `app/services/trade_reporter.py`

```python
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
                f"<i>No closed trades this period</i>"
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
            text += f"\n🖥️ <b>SERVER SIM</b>\n"
            text += self._format_trade_section(srv_trades, desk_id, "SRV")

        # MT5 sim section
        if mt5_trades:
            text += f"\n\n📟 <b>MT5 SIM</b>\n"
            text += self._format_trade_section(mt5_trades, desk_id, "MT5")

        # OniAI section
        if oniai_trades:
            text += f"\n\n🤖 <b>OniAI VIRTUAL</b>\n"
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
            f"🏛 <b>ONIQUANT — {period_label.split('·')[0].strip().upper()}</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━━━\n"
            f"📅 {period_label}\n"
        )

        # Server sim section
        has_srv = any(len(t) > 0 for t in srv_by_desk.values())
        if has_srv:
            text += f"\n🖥️ <b>SERVER SIM</b>"
            text += self._format_portfolio_section(srv_by_desk, "SRV")

        # MT5 sim section
        has_mt5 = any(len(t) > 0 for t in mt5_by_desk.values())
        if has_mt5:
            text += f"\n\n📟 <b>MT5 SIM</b>"
            text += self._format_portfolio_section(mt5_by_desk, "MT5")

        # OniAI section
        has_oniai = any(len(t) > 0 for t in oniai_by_desk.values())
        if has_oniai:
            text += f"\n\n🤖 <b>OniAI VIRTUAL</b>"
            text += self._format_portfolio_section(oniai_by_desk, "OniAI")

        if not has_srv and not has_mt5 and not has_oniai:
            text += "\n\n<i>No closed trades this period</i>"

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
            return f"\n\n<i>No {section_label.lower()} trades this period</i>"

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
```

## `app/services/ml_data_logger.py`

```python
"""
ML Data Logger — Records + Enriches Every Signal

Pipeline calls log_signal() with all data at once.
Feature engineer adds 40+ derived features automatically.
Outcome logged when trade closes.
Export via /api/ml/export or /api/ml/stats.
"""
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional

from sqlalchemy import func, desc
from sqlalchemy.orm import Session

from app.models.ml_trade_log import MLTradeLog
from app.config import DESKS, get_pip_info

logger = logging.getLogger("TradingSystem.MLDataLogger")


class MLDataLogger:

    def log_signal(
        self, db: Session, signal_id: int = None,
        signal_data: Dict = None, desk_id: str = None,
        enrichment: Dict = None, ml_result: Dict = None,
        consensus: Dict = None, decision: Dict = None,
        risk_approved: bool = None, risk_block_reason: str = None,
        trade_params: Dict = None, desk_state=None,
    ) -> int:
        """All-in-one: log signal + scores + filter + run feature enrichment."""
        now = datetime.now(timezone.utc)
        signal_data = signal_data or {}
        enrichment = enrichment or {}
        ml_result = ml_result or {}
        consensus = consensus or {}
        decision = decision or {}
        trade_params = trade_params or {}

        symbol = signal_data.get("symbol", "")
        pip_size, _ = get_pip_info(symbol)
        entry = float(signal_data.get("price", 0) or 0)
        sl = float(signal_data.get("sl1", 0) or 0)
        tp1 = float(signal_data.get("tp1", 0) or 0)
        tp2 = float(signal_data.get("tp2", 0) or 0)
        sl_pips = abs(entry - sl) / pip_size if entry and sl and pip_size else 0
        tp1_pips = abs(tp1 - entry) / pip_size if entry and tp1 and pip_size else 0
        rr = round(tp1_pips / sl_pips, 2) if sl_pips > 0 else 0

        atr = enrichment.get("atr")
        atr_avg = enrichment.get("atr_avg")
        vol_regime = None
        if atr and atr_avg and atr_avg > 0:
            ratio = atr / atr_avg
            vol_regime = (
                "LOW" if ratio < 0.6 else
                "NORMAL" if ratio < 1.2 else
                "HIGH" if ratio < 2.0 else "EXTREME"
            )

        is_oniai = (
            risk_approved is False
            and decision.get("decision") in ["EXECUTE", "REDUCE"]
        )

        record = MLTradeLog(
            signal_id=signal_id,
            symbol=symbol,
            direction=signal_data.get("direction", ""),
            timeframe=signal_data.get("timeframe", ""),
            alert_type=signal_data.get("alert_type", ""),
            desk_id=desk_id or signal_data.get("desk_id", ""),
            entry_price=entry,
            sl_price=sl, tp1_price=tp1, tp2_price=tp2,
            sl_pips=round(sl_pips, 1), tp1_pips=round(tp1_pips, 1), rr_ratio=rr,
            session=self._get_session(now.hour),
            day_of_week=now.weekday(), hour_utc=now.hour,
            atr_value=atr, rsi_value=enrichment.get("rsi"),
            volatility_regime=vol_regime,
            raw_enrichment=enrichment,
            ml_score=ml_result.get("ml_score"),
            ml_method=ml_result.get("ml_method"),
            raw_ml_result=ml_result,
            consensus_score=consensus.get("total_score"),
            consensus_tier=consensus.get("tier"),
            consensus_components=consensus.get("components"),
            raw_consensus=consensus,
            claude_decision=decision.get("decision"),
            claude_confidence=decision.get("confidence"),
            claude_reasoning=(decision.get("reasoning") or "")[:500],
            claude_size_multiplier=decision.get("size_multiplier"),
            raw_claude_response={
                "decision": decision.get("decision"),
                "confidence": decision.get("confidence"),
                "reasoning": decision.get("reasoning"),
                "size_multiplier": decision.get("size_multiplier"),
            },
            open_positions_desk=getattr(desk_state, "open_positions", 0) if desk_state else 0,
            daily_pnl_at_entry=getattr(desk_state, "daily_pnl", 0) if desk_state else 0,
            daily_loss_at_entry=getattr(desk_state, "daily_loss", 0) if desk_state else 0,
            consecutive_losses=getattr(desk_state, "consecutive_losses", 0) if desk_state else 0,
            size_modifier=getattr(desk_state, "size_modifier", 1.0) if desk_state else 1.0,
            approved=risk_approved if risk_approved is not None else False,
            filter_blocked=not risk_approved if risk_approved is not None else False,
            block_reason=risk_block_reason,
            is_oniai=is_oniai,
            lot_size=trade_params.get("lot_size"),
            risk_pct=trade_params.get("risk_pct"),
            risk_dollars=trade_params.get("risk_dollars"),
            raw_signal_data=signal_data,
        )

        db.add(record)
        db.flush()

        # Run feature enrichment (adds 40+ derived features)
        try:
            from app.services.feature_engineer import FeatureEngineer
            FeatureEngineer().enrich_record(db, record.id)
        except Exception as e:
            logger.debug(f"Feature enrichment failed for #{record.id}: {e}")

        return record.id

    def log_outcome(
        self, db: Session, signal_id: int = None, trade_id: int = None,
        pnl_pips: float = None, pnl_dollars: float = None,
        exit_price: float = None, exit_reason: str = None,
        hold_time_minutes: float = None,
        max_favorable_pips: float = None, max_adverse_pips: float = None,
        profile: str = None,
    ):
        """Log trade outcome when it closes."""
        record = None
        if signal_id:
            record = (
                db.query(MLTradeLog)
                .filter(MLTradeLog.signal_id == signal_id)
                .order_by(desc(MLTradeLog.id)).first()
            )
        if not record:
            return

        record.trade_id = trade_id
        record.exit_price = exit_price
        record.exit_reason = exit_reason
        record.pnl_pips = pnl_pips
        record.pnl_dollars = pnl_dollars
        record.hold_time_minutes = hold_time_minutes
        record.max_favorable_pips = max_favorable_pips
        record.max_adverse_pips = max_adverse_pips

        if pnl_pips is not None:
            record.outcome = "WIN" if pnl_pips > 1 else ("LOSS" if pnl_pips < -1 else "BE")

        if profile == "SRV_100":
            record.srv100_pnl_pips = pnl_pips
            record.srv100_exit_reason = exit_reason
        elif profile == "SRV_30":
            record.srv30_pnl_pips = pnl_pips
            record.srv30_exit_reason = exit_reason
        elif profile == "MT5_1M":
            record.mt5_pnl_pips = pnl_pips
            record.mt5_exit_reason = exit_reason
        if record.is_oniai:
            record.oniai_pnl_pips = pnl_pips
            record.oniai_exit_reason = exit_reason

    @staticmethod
    def get_training_data(db: Session, completed_only: bool = True, limit: int = 10000) -> List[Dict]:
        """Export training data as JSON-serializable list."""
        query = db.query(MLTradeLog)
        if completed_only:
            query = query.filter(MLTradeLog.outcome.isnot(None))
        records = query.order_by(desc(MLTradeLog.created_at)).limit(limit).all()

        data = []
        for r in records:
            row = {
                "id": r.id,
                "created_at": r.created_at.isoformat() if r.created_at else None,
                "signal_id": r.signal_id, "symbol": r.symbol,
                "direction": r.direction, "timeframe": r.timeframe,
                "desk_id": r.desk_id, "alert_type": r.alert_type,
                "entry_price": r.entry_price,
                "sl_price": r.sl_price, "tp1_price": r.tp1_price,
                "sl_pips": r.sl_pips, "tp1_pips": r.tp1_pips, "rr_ratio": r.rr_ratio,
                "session": r.session, "day_of_week": r.day_of_week, "hour_utc": r.hour_utc,
                "atr_value": r.atr_value, "volatility_regime": r.volatility_regime,
                "rsi_value": r.rsi_value,
                "ml_score": r.ml_score, "consensus_score": r.consensus_score,
                "consensus_tier": r.consensus_tier,
                "claude_decision": r.claude_decision,
                "claude_confidence": r.claude_confidence,
                "approved": r.approved, "filter_blocked": r.filter_blocked,
                "block_reason": r.block_reason, "is_oniai": r.is_oniai,
                "lot_size": r.lot_size, "risk_pct": r.risk_pct,
                "consecutive_losses": r.consecutive_losses,
                "size_modifier": r.size_modifier,
                "outcome": r.outcome,
                "pnl_pips": r.pnl_pips, "pnl_dollars": r.pnl_dollars,
                "exit_reason": r.exit_reason,
                "hold_time_minutes": r.hold_time_minutes,
                "max_favorable_pips": r.max_favorable_pips,
                "max_adverse_pips": r.max_adverse_pips,
                "srv100_pnl_pips": r.srv100_pnl_pips,
                "srv30_pnl_pips": r.srv30_pnl_pips,
                "mt5_pnl_pips": r.mt5_pnl_pips,
                "oniai_pnl_pips": r.oniai_pnl_pips,
            }
            # Add enriched features if present
            enrich = r.raw_enrichment or {}
            if "ml_features" in enrich:
                row["features"] = enrich["ml_features"]
            data.append(row)

        return data

    @staticmethod
    def get_stats(db: Session) -> Dict:
        total = db.query(func.count(MLTradeLog.id)).scalar()
        completed = db.query(func.count(MLTradeLog.id)).filter(MLTradeLog.outcome.isnot(None)).scalar()
        wins = db.query(func.count(MLTradeLog.id)).filter(MLTradeLog.outcome == "WIN").scalar()
        losses = db.query(func.count(MLTradeLog.id)).filter(MLTradeLog.outcome == "LOSS").scalar()
        blocked = db.query(func.count(MLTradeLog.id)).filter(MLTradeLog.filter_blocked == True).scalar()
        avg_pnl = db.query(func.avg(MLTradeLog.pnl_pips)).filter(MLTradeLog.outcome.isnot(None)).scalar()
        avg_win = db.query(func.avg(MLTradeLog.pnl_pips)).filter(MLTradeLog.outcome == "WIN").scalar()
        avg_loss = db.query(func.avg(MLTradeLog.pnl_pips)).filter(MLTradeLog.outcome == "LOSS").scalar()

        desk_stats = {}
        for desk_id in DESKS:
            dt = db.query(func.count(MLTradeLog.id)).filter(
                MLTradeLog.desk_id == desk_id, MLTradeLog.outcome.isnot(None)).scalar()
            dw = db.query(func.count(MLTradeLog.id)).filter(
                MLTradeLog.desk_id == desk_id, MLTradeLog.outcome == "WIN").scalar()
            if dt > 0:
                desk_stats[desk_id] = {"total": dt, "wins": dw, "win_rate": round(dw / dt * 100, 1)}

        return {
            "total_records": total, "completed": completed, "pending": total - completed,
            "wins": wins, "losses": losses,
            "win_rate": round(wins / completed * 100, 1) if completed > 0 else 0,
            "avg_pnl_pips": round(avg_pnl, 1) if avg_pnl else 0,
            "avg_win_pips": round(avg_win, 1) if avg_win else 0,
            "avg_loss_pips": round(avg_loss, 1) if avg_loss else 0,
            "filter_blocked": blocked, "per_desk": desk_stats,
        }

    def _get_session(self, hour_utc: int) -> str:
        if 0 <= hour_utc < 7: return "ASIAN"
        elif 7 <= hour_utc < 12: return "LONDON"
        elif 12 <= hour_utc < 16: return "OVERLAP"
        elif 16 <= hour_utc < 21: return "NEW_YORK"
        else: return "LATE_NY"
```

## `app/services/feature_engineer.py`

```python
"""
ML Feature Engineer — Transforms Raw Data into ML-Ready Features

Runs at two points:
  1. REAL-TIME: During pipeline, adds features to each new signal
  2. BATCH: On demand, retroactively enriches historical records

Feature categories:
  A. Signal Quality Features (from raw signal)
  B. Market Regime Features (from enrichment data)
  C. Historical Performance Features (from past trades)
  D. Time Pattern Features (from timestamp analysis)
  E. Correlation & Portfolio Features (from open positions)
  F. Derived Ratios & Scores (computed from other features)
"""
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, List

from sqlalchemy import func, and_, desc
from sqlalchemy.orm import Session

from app.models.ml_trade_log import MLTradeLog
from app.models.trade import Trade
from app.models.signal import Signal
from app.config import (
    DESKS, CORRELATION_GROUPS, get_pip_info,
    CAPITAL_PER_ACCOUNT,
)

logger = logging.getLogger("TradingSystem.FeatureEngineer")


class FeatureEngineer:
    """Computes derived features for ML training data."""

    def enrich_record(self, db: Session, log_id: int):
        """
        Add all derived features to an ML training record.
        Called after pipeline completes for a signal.
        """
        record = db.query(MLTradeLog).filter(MLTradeLog.id == log_id).first()
        if not record:
            return

        features = {}

        # A. Signal Quality
        features.update(self._signal_quality_features(record))

        # B. Market Regime
        features.update(self._market_regime_features(record))

        # C. Historical Performance (lookback on past trades)
        features.update(self._historical_features(db, record))

        # D. Time Patterns
        features.update(self._time_pattern_features(db, record))

        # E. Correlation & Portfolio
        features.update(self._correlation_features(db, record))

        # F. Derived Ratios
        features.update(self._derived_ratios(record, features))

        # Store all features in the raw_enrichment JSON blob
        existing = record.raw_enrichment or {}
        existing["ml_features"] = features
        record.raw_enrichment = existing

        logger.debug(f"Feature enrichment | #{log_id} | {len(features)} features")

    def batch_enrich(self, db: Session, limit: int = 500) -> int:
        """
        Retroactively enrich records missing features.
        Run periodically or on-demand via /api/ml/enrich.
        """
        records = (
            db.query(MLTradeLog)
            .filter(
                MLTradeLog.raw_enrichment.is_(None)
                | ~MLTradeLog.raw_enrichment.contains("ml_features")
            )
            .order_by(desc(MLTradeLog.id))
            .limit(limit)
            .all()
        )

        count = 0
        for record in records:
            try:
                self.enrich_record(db, record.id)
                count += 1
            except Exception as e:
                logger.debug(f"Batch enrich failed for #{record.id}: {e}")

        if count:
            db.commit()
            logger.info(f"Batch enriched {count} ML records")

        return count

    # ─────────────────────────────────────────
    # A. SIGNAL QUALITY FEATURES
    # ─────────────────────────────────────────

    def _signal_quality_features(self, record: MLTradeLog) -> Dict:
        """Features derived from the raw signal itself."""
        f = {}

        sl_pips = record.sl_pips or 0
        tp1_pips = record.tp1_pips or 0
        rr = record.rr_ratio or 0

        # Risk/reward classification
        f["rr_tier"] = (
            "EXCELLENT" if rr >= 3.0 else
            "GOOD" if rr >= 2.0 else
            "FAIR" if rr >= 1.5 else
            "POOR" if rr >= 1.0 else
            "BAD"
        )

        # SL tightness relative to ATR
        atr = record.atr_value or 0
        if atr > 0:
            pip_size, _ = get_pip_info(record.symbol or "")
            atr_pips = atr / pip_size if pip_size else 0
            f["sl_atr_ratio"] = round(sl_pips / atr_pips, 2) if atr_pips > 0 else 0
            f["tp1_atr_ratio"] = round(tp1_pips / atr_pips, 2) if atr_pips > 0 else 0
            f["sl_tight"] = f["sl_atr_ratio"] < 1.0  # tighter than 1 ATR
            f["sl_wide"] = f["sl_atr_ratio"] > 2.5   # wider than 2.5 ATR
        else:
            f["sl_atr_ratio"] = 0
            f["tp1_atr_ratio"] = 0
            f["sl_tight"] = False
            f["sl_wide"] = False

        # Signal type strength
        alert = record.alert_type or ""
        f["is_plus_signal"] = "plus" in alert.lower()
        f["is_confirmation"] = "confirmation" in alert.lower()
        f["is_contrarian"] = "contrarian" in alert.lower()
        f["is_exit_signal"] = "exit" in alert.lower()

        # Consensus quality
        f["consensus_above_7"] = (record.consensus_score or 0) >= 7
        f["consensus_below_4"] = (record.consensus_score or 0) < 4
        f["claude_high_conf"] = (record.claude_confidence or 0) >= 0.75
        f["claude_low_conf"] = (record.claude_confidence or 0) < 0.5

        # Pipeline agreement score (do ML + consensus + Claude all agree?)
        ml_bullish = (record.ml_score or 0) > 0.6
        consensus_bullish = (record.consensus_score or 0) >= 5
        claude_bullish = record.claude_decision in ["EXECUTE", "REDUCE"]
        agreement = sum([ml_bullish, consensus_bullish, claude_bullish])
        f["pipeline_agreement"] = agreement  # 0-3
        f["full_agreement"] = agreement == 3

        return f

    # ─────────────────────────────────────────
    # B. MARKET REGIME FEATURES
    # ─────────────────────────────────────────

    def _market_regime_features(self, record: MLTradeLog) -> Dict:
        """Features about current market conditions."""
        f = {}

        vol = record.volatility_regime or "NORMAL"
        f["vol_is_low"] = vol == "LOW"
        f["vol_is_high"] = vol in ["HIGH", "EXTREME"]
        f["vol_is_extreme"] = vol == "EXTREME"

        # RSI zones
        rsi = record.rsi_value
        if rsi is not None:
            f["rsi_overbought"] = rsi > 70
            f["rsi_oversold"] = rsi < 30
            f["rsi_neutral"] = 40 <= rsi <= 60
            f["rsi_value"] = round(rsi, 1)
        else:
            f["rsi_overbought"] = False
            f["rsi_oversold"] = False
            f["rsi_neutral"] = True
            f["rsi_value"] = 50.0

        # Direction vs RSI alignment
        direction = (record.direction or "").upper()
        if direction in ["LONG", "BUY"]:
            f["rsi_supports_direction"] = (rsi or 50) < 60  # not overbought
        else:
            f["rsi_supports_direction"] = (rsi or 50) > 40  # not oversold

        return f

    # ─────────────────────────────────────────
    # C. HISTORICAL PERFORMANCE FEATURES
    # ─────────────────────────────────────────

    def _historical_features(self, db: Session, record: MLTradeLog) -> Dict:
        """Lookback features from recent trade history."""
        f = {}
        symbol = record.symbol
        desk_id = record.desk_id
        now = record.created_at or datetime.now(timezone.utc)

        # Last 20 completed trades for this desk
        recent = (
            db.query(MLTradeLog)
            .filter(
                MLTradeLog.desk_id == desk_id,
                MLTradeLog.outcome.isnot(None),
                MLTradeLog.created_at < now,
            )
            .order_by(desc(MLTradeLog.created_at))
            .limit(20)
            .all()
        )

        if recent:
            wins = sum(1 for r in recent if r.outcome == "WIN")
            total = len(recent)
            f["desk_recent_win_rate"] = round(wins / total * 100, 1)
            f["desk_recent_trades"] = total

            # Recent avg pnl
            pnls = [r.pnl_pips for r in recent if r.pnl_pips is not None]
            f["desk_recent_avg_pnl"] = round(sum(pnls) / len(pnls), 1) if pnls else 0

            # Current streak
            streak = 0
            streak_type = None
            for r in recent:
                if streak_type is None:
                    streak_type = r.outcome
                    streak = 1
                elif r.outcome == streak_type:
                    streak += 1
                else:
                    break
            f["current_streak"] = streak
            f["streak_is_winning"] = streak_type == "WIN"
            f["streak_is_losing"] = streak_type == "LOSS"
        else:
            f["desk_recent_win_rate"] = 50.0
            f["desk_recent_trades"] = 0
            f["desk_recent_avg_pnl"] = 0
            f["current_streak"] = 0
            f["streak_is_winning"] = False
            f["streak_is_losing"] = False

        # Symbol-specific recent performance (last 10)
        sym_recent = (
            db.query(MLTradeLog)
            .filter(
                MLTradeLog.symbol == symbol,
                MLTradeLog.outcome.isnot(None),
                MLTradeLog.created_at < now,
            )
            .order_by(desc(MLTradeLog.created_at))
            .limit(10)
            .all()
        )

        if sym_recent:
            sym_wins = sum(1 for r in sym_recent if r.outcome == "WIN")
            f["symbol_recent_win_rate"] = round(sym_wins / len(sym_recent) * 100, 1)
            sym_pnls = [r.pnl_pips for r in sym_recent if r.pnl_pips is not None]
            f["symbol_recent_avg_pnl"] = round(sum(sym_pnls) / len(sym_pnls), 1) if sym_pnls else 0
        else:
            f["symbol_recent_win_rate"] = 50.0
            f["symbol_recent_avg_pnl"] = 0

        # OniAI vs executed comparison (last 30 days)
        month_ago = now - timedelta(days=30)
        oniai_wins = db.query(func.count(MLTradeLog.id)).filter(
            MLTradeLog.desk_id == desk_id,
            MLTradeLog.is_oniai == True,
            MLTradeLog.outcome == "WIN",
            MLTradeLog.created_at >= month_ago,
        ).scalar()
        oniai_total = db.query(func.count(MLTradeLog.id)).filter(
            MLTradeLog.desk_id == desk_id,
            MLTradeLog.is_oniai == True,
            MLTradeLog.outcome.isnot(None),
            MLTradeLog.created_at >= month_ago,
        ).scalar()

        f["oniai_30d_win_rate"] = round(oniai_wins / oniai_total * 100, 1) if oniai_total > 0 else 50.0
        f["oniai_30d_count"] = oniai_total

        return f

    # ─────────────────────────────────────────
    # D. TIME PATTERN FEATURES
    # ─────────────────────────────────────────

    def _time_pattern_features(self, db: Session, record: MLTradeLog) -> Dict:
        """Features from time-of-day and day-of-week patterns."""
        f = {}
        hour = record.hour_utc or 0
        dow = record.day_of_week or 0

        # Session buckets
        f["is_asian"] = 0 <= hour < 7
        f["is_london"] = 7 <= hour < 12
        f["is_overlap"] = 12 <= hour < 16
        f["is_new_york"] = 16 <= hour < 21
        f["is_late_session"] = hour >= 21

        # Day of week
        f["is_monday"] = dow == 0
        f["is_friday"] = dow == 4
        f["is_midweek"] = dow in [1, 2, 3]  # Tue/Wed/Thu

        # Historical win rate for this session + symbol combo
        session = record.session
        symbol = record.symbol

        session_perf = (
            db.query(
                func.count(MLTradeLog.id).label("total"),
                func.sum(
                    func.cast(MLTradeLog.outcome == "WIN", type_=None)
                ).label("wins"),
            )
            .filter(
                MLTradeLog.symbol == symbol,
                MLTradeLog.session == session,
                MLTradeLog.outcome.isnot(None),
            )
            .first()
        )

        if session_perf and session_perf.total and session_perf.total > 5:
            wins = session_perf.wins or 0
            f["session_symbol_win_rate"] = round(wins / session_perf.total * 100, 1)
            f["session_symbol_trades"] = session_perf.total
        else:
            f["session_symbol_win_rate"] = 50.0
            f["session_symbol_trades"] = 0

        # Hour-of-day win rate for this desk
        hour_perf = (
            db.query(func.count(MLTradeLog.id))
            .filter(
                MLTradeLog.desk_id == record.desk_id,
                MLTradeLog.hour_utc == hour,
                MLTradeLog.outcome == "WIN",
            )
            .scalar()
        )
        hour_total = (
            db.query(func.count(MLTradeLog.id))
            .filter(
                MLTradeLog.desk_id == record.desk_id,
                MLTradeLog.hour_utc == hour,
                MLTradeLog.outcome.isnot(None),
            )
            .scalar()
        )
        f["hour_win_rate"] = round(hour_perf / hour_total * 100, 1) if hour_total > 5 else 50.0

        return f

    # ─────────────────────────────────────────
    # E. CORRELATION & PORTFOLIO FEATURES
    # ─────────────────────────────────────────

    def _correlation_features(self, db: Session, record: MLTradeLog) -> Dict:
        """Features about current portfolio exposure."""
        f = {}

        # Total open positions across all desks
        total_open = (
            db.query(func.count(Trade.id))
            .filter(Trade.status.in_(["EXECUTED", "OPEN"]))
            .scalar()
        )
        f["total_open_positions"] = total_open
        f["portfolio_crowded"] = total_open >= 10

        # Same-symbol open positions
        same_sym_open = (
            db.query(func.count(Trade.id))
            .filter(
                Trade.status.in_(["EXECUTED", "OPEN"]),
                Trade.symbol == record.symbol,
            )
            .scalar()
        )
        f["same_symbol_open"] = same_sym_open
        f["symbol_already_open"] = same_sym_open > 0

        # Correlation group exposure
        for group_name, group in CORRELATION_GROUPS.items():
            if record.symbol in group.get("symbols", []):
                group_open = (
                    db.query(func.count(Trade.id))
                    .filter(
                        Trade.status.in_(["EXECUTED", "OPEN"]),
                        Trade.symbol.in_(group["symbols"]),
                    )
                    .scalar()
                )
                f["corr_group_open"] = group_open
                f["corr_group_name"] = group_name
                f["corr_group_crowded"] = group_open >= 2
                break
        else:
            f["corr_group_open"] = 0
            f["corr_group_name"] = None
            f["corr_group_crowded"] = False

        # Portfolio daily PnL state
        from app.models.desk_state import DeskState
        total_daily_pnl = (
            db.query(func.sum(DeskState.daily_pnl)).scalar() or 0
        )
        f["portfolio_daily_pnl"] = round(total_daily_pnl, 2)
        f["portfolio_in_profit"] = total_daily_pnl > 0
        f["portfolio_in_drawdown"] = total_daily_pnl < -1000

        return f

    # ─────────────────────────────────────────
    # F. DERIVED RATIOS & COMPOSITE SCORES
    # ─────────────────────────────────────────

    def _derived_ratios(self, record: MLTradeLog, features: Dict) -> Dict:
        """Composite scores combining multiple features."""
        f = {}

        # Signal quality score (0-100)
        quality = 50  # baseline

        # R:R bonus
        rr_tier = features.get("rr_tier", "FAIR")
        quality += {"EXCELLENT": 20, "GOOD": 10, "FAIR": 0, "POOR": -10, "BAD": -20}.get(rr_tier, 0)

        # Pipeline agreement bonus
        agreement = features.get("pipeline_agreement", 1)
        quality += (agreement - 1) * 15  # +15 for 2 agree, +30 for all 3

        # Volatility fit
        if features.get("vol_is_extreme"):
            quality -= 15
        elif features.get("vol_is_low"):
            quality -= 10

        # Session fit
        if features.get("is_overlap"):
            quality += 10  # London-NY overlap is best
        elif features.get("is_late_session"):
            quality -= 10

        # Historical desk performance
        desk_wr = features.get("desk_recent_win_rate", 50)
        if desk_wr > 60:
            quality += 10
        elif desk_wr < 40:
            quality -= 10

        # Streak penalty
        if features.get("streak_is_losing") and features.get("current_streak", 0) >= 3:
            quality -= 15

        f["signal_quality_score"] = max(0, min(100, quality))

        # Confidence composite (combine Claude + ML + consensus)
        claude_conf = record.claude_confidence or 0.5
        ml_score = record.ml_score or 0.5
        consensus = (record.consensus_score or 5) / 10
        f["composite_confidence"] = round(
            (claude_conf * 0.4 + ml_score * 0.3 + consensus * 0.3), 3
        )

        # Risk-adjusted expectancy estimate
        # E = (win_rate × avg_win) - (loss_rate × avg_loss)
        wr = features.get("desk_recent_win_rate", 50) / 100
        avg_win = features.get("desk_recent_avg_pnl", 20) if features.get("desk_recent_avg_pnl", 0) > 0 else 20
        avg_loss = abs(features.get("desk_recent_avg_pnl", -10)) if features.get("desk_recent_avg_pnl", 0) < 0 else 10
        f["expected_value_pips"] = round(wr * avg_win - (1 - wr) * avg_loss, 1)

        # Trade-or-skip recommendation (binary for ML target engineering)
        f["should_trade"] = (
            f["signal_quality_score"] >= 55
            and f["composite_confidence"] >= 0.55
            and not features.get("vol_is_extreme")
            and not features.get("portfolio_in_drawdown")
        )

        return f
```

## `app/services/zmq_bridge.py`

```python
"""
ZeroMQ Bridge — Railway Side
Sends approved trade commands to the MT5 VPS over ZeroMQ.
Also receives execution confirmations back from the VPS.

The VPS runs a ZeroMQ PULL socket. Railway connects with a PUSH socket.
A separate REQ/REP pair handles status queries and kill switch commands.
"""
import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, Optional

import zmq
import zmq.asyncio

logger = logging.getLogger("TradingSystem.ZMQBridge")

VPS_HOST = os.getenv("VPS_HOST", "")  # e.g., "tcp://123.45.67.89:5555"
VPS_COMMAND_PORT = os.getenv("VPS_COMMAND_PORT", "5555")  # PUSH/PULL for trades
VPS_CONTROL_PORT = os.getenv("VPS_CONTROL_PORT", "5556")  # REQ/REP for control


class ZMQBridge:
    """
    Sends trade commands to MT5 VPS via ZeroMQ.
    Falls back to logging-only mode if VPS is not configured.
    """

    def __init__(self):
        self.context = None
        self.push_socket = None
        self.req_socket = None
        self.connected = False
        self.vps_host = VPS_HOST

        if self.vps_host:
            self._connect()
        else:
            logger.warning(
                "VPS_HOST not set — ZeroMQ bridge in LOG-ONLY mode. "
                "Trades will be logged but not sent to MT5."
            )

    def _connect(self):
        """Establish ZeroMQ connections to VPS."""
        try:
            self.context = zmq.asyncio.Context()

            # PUSH socket for sending trade commands
            self.push_socket = self.context.socket(zmq.PUSH)
            self.push_socket.setsockopt(zmq.SNDTIMEO, 5000)  # 5s timeout
            self.push_socket.setsockopt(zmq.LINGER, 1000)
            push_addr = f"tcp://{self.vps_host}:{VPS_COMMAND_PORT}"
            self.push_socket.connect(push_addr)

            # REQ socket for control commands (kill switch, status)
            self.req_socket = self.context.socket(zmq.REQ)
            self.req_socket.setsockopt(zmq.RCVTIMEO, 5000)
            self.req_socket.setsockopt(zmq.SNDTIMEO, 5000)
            self.req_socket.setsockopt(zmq.LINGER, 1000)
            ctrl_addr = f"tcp://{self.vps_host}:{VPS_CONTROL_PORT}"
            self.req_socket.connect(ctrl_addr)

            self.connected = True
            logger.info(
                f"ZeroMQ connected to VPS at {push_addr} (trades) "
                f"and {ctrl_addr} (control)"
            )
        except Exception as e:
            logger.error(f"ZeroMQ connection failed: {e}")
            self.connected = False

    async def send_trade(self, trade_params: Dict, signal_id: int) -> Dict:
        """
        Send an approved trade command to the MT5 VPS.
        Returns execution result or log-only confirmation.
        """
        command = {
            "type": "TRADE",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "signal_id": signal_id,
            "action": "OPEN",
            "symbol": trade_params.get("symbol"),
            "direction": trade_params.get("direction"),
            "risk_pct": trade_params.get("risk_pct"),
            "risk_dollars": trade_params.get("risk_dollars"),
            "stop_loss": trade_params.get("stop_loss"),
            "take_profit_1": trade_params.get("take_profit_1"),
            "take_profit_2": trade_params.get("take_profit_2"),
            "trailing_stop_pips": trade_params.get("trailing_stop_pips"),
            "size_multiplier": trade_params.get("size_multiplier"),
            "desk_id": trade_params.get("desk_id"),
            "claude_decision": trade_params.get("claude_decision"),
            "confidence": trade_params.get("confidence"),
        }

        if not self.connected or not self.push_socket:
            logger.info(
                f"[LOG-ONLY] Trade command for signal #{signal_id}: "
                f"{command['symbol']} {command['direction']} "
                f"Risk: ${command['risk_dollars']}"
            )
            return {
                "status": "logged",
                "message": "VPS not connected — trade logged only",
                "command": command,
            }

        try:
            await self.push_socket.send_json(command)
            logger.info(
                f"Trade SENT to VPS | Signal #{signal_id} | "
                f"{command['symbol']} {command['direction']} | "
                f"Risk: ${command['risk_dollars']}"
            )
            return {
                "status": "sent",
                "message": "Trade command sent to MT5 VPS",
                "command": command,
            }
        except zmq.error.Again:
            logger.error(f"ZMQ timeout sending trade for signal #{signal_id}")
            return {"status": "timeout", "message": "VPS did not respond"}
        except Exception as e:
            logger.error(f"ZMQ send error: {e}")
            return {"status": "error", "message": str(e)}

    async def send_close(
        self, symbol: str, desk_id: str, reason: str
    ) -> Dict:
        """Send a close/exit command for a position."""
        command = {
            "type": "TRADE",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "action": "CLOSE",
            "symbol": symbol,
            "desk_id": desk_id,
            "reason": reason,
        }

        if not self.connected:
            logger.info(f"[LOG-ONLY] Close command: {symbol} on {desk_id}")
            return {"status": "logged", "command": command}

        try:
            await self.push_socket.send_json(command)
            logger.info(f"Close SENT to VPS | {symbol} on {desk_id} | {reason}")
            return {"status": "sent", "command": command}
        except Exception as e:
            logger.error(f"ZMQ close send error: {e}")
            return {"status": "error", "message": str(e)}

    async def send_kill_switch(self, scope: str = "ALL") -> Dict:
        """
        Emergency kill switch. Closes all positions and stops trading.
        scope: "ALL" = entire firm, or a desk_id for single desk.
        """
        command = {
            "type": "KILL_SWITCH",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "scope": scope,
        }

        if not self.connected or not self.req_socket:
            logger.warning(f"[LOG-ONLY] Kill switch triggered: {scope}")
            return {"status": "logged", "message": "VPS not connected"}

        try:
            await self.req_socket.send_json(command)
            response = await self.req_socket.recv_json()
            logger.warning(
                f"KILL SWITCH EXECUTED | Scope: {scope} | "
                f"Response: {response}"
            )
            return {"status": "executed", "response": response}
        except Exception as e:
            logger.error(f"Kill switch ZMQ error: {e}")
            return {"status": "error", "message": str(e)}

    async def get_vps_status(self) -> Dict:
        """Query the VPS for current status (open positions, connection, etc.)."""
        command = {
            "type": "STATUS",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        if not self.connected or not self.req_socket:
            return {
                "status": "disconnected",
                "message": "VPS not configured or unreachable",
            }

        try:
            await self.req_socket.send_json(command)
            response = await self.req_socket.recv_json()
            return {"status": "connected", "vps_data": response}
        except zmq.error.Again:
            return {"status": "timeout", "message": "VPS did not respond"}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    async def toggle_desk(self, desk_id: str, active: bool) -> Dict:
        """Enable or disable a specific desk on the VPS."""
        command = {
            "type": "TOGGLE_DESK",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "desk_id": desk_id,
            "active": active,
        }

        if not self.connected or not self.req_socket:
            return {"status": "logged", "message": "VPS not connected"}

        try:
            await self.req_socket.send_json(command)
            response = await self.req_socket.recv_json()
            logger.info(f"Desk toggle: {desk_id} → {'ON' if active else 'OFF'}")
            return {"status": "executed", "response": response}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def close(self):
        """Clean up ZeroMQ sockets."""
        if self.push_socket:
            self.push_socket.close()
        if self.req_socket:
            self.req_socket.close()
        if self.context:
            self.context.term()
```

## `app/services/price_service.py`

```python
"""
PriceService — On-Demand 4-Provider Price Fetch
Used by the pipeline to get a live market price at signal entry time.
NOT a continuous feed. Single get_price() call per signal.

Same 4-provider routing as the JIT simulator:
  Crypto    → Binance REST (no key, 1 call)
  Forex     → TwelveData (1 credit)  → FMP fallback
  Metals    → TwelveData (1 credit)  → FMP fallback
  Stocks    → Finnhub (1 call)       → FMP fallback
  Indices   → FMP (1 call)           → TwelveData fallback
  Oil/Cu    → FMP (1 call)           → TwelveData fallback
"""
import os
import logging
from datetime import datetime, timezone
from typing import Dict, Optional

import httpx

logger = logging.getLogger("TradingSystem.PriceService")

TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY", "")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")
FMP_API_KEY = os.getenv("FMP_API_KEY", "")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# ASSET CLASSIFICATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CRYPTO = {"BTCUSD", "ETHUSD", "SOLUSD", "XRPUSD", "LINKUSD"}
STOCKS = {"TSLA", "AAPL", "MSFT", "NVDA", "AMZN", "META", "GOOGL", "NFLX", "AMD"}
INDICES = {"US30", "US100", "NAS100"}
COMMODITIES = {"WTIUSD", "XCUUSD"}
METALS = {"XAUUSD", "XAGUSD"}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SYMBOL MAPS (same as simulator)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

TD_MAP = {
    "EURUSD": "EUR/USD", "USDJPY": "USD/JPY", "GBPUSD": "GBP/USD",
    "USDCHF": "USD/CHF", "AUDUSD": "AUD/USD", "USDCAD": "USD/CAD",
    "NZDUSD": "NZD/USD", "EURJPY": "EUR/JPY", "GBPJPY": "GBP/JPY",
    "AUDJPY": "AUD/JPY", "EURGBP": "EUR/GBP", "EURAUD": "EUR/AUD",
    "GBPAUD": "GBP/AUD", "EURCHF": "EUR/CHF", "CADJPY": "CAD/JPY",
    "NZDJPY": "NZD/JPY", "GBPCAD": "GBP/CAD", "AUDCAD": "AUD/CAD",
    "AUDNZD": "AUD/NZD", "CHFJPY": "CHF/JPY", "EURNZD": "EUR/NZD",
    "GBPNZD": "GBP/NZD",
    "XAUUSD": "XAU/USD", "XAGUSD": "XAG/USD",
}

FH_MAP = {
    "TSLA": "TSLA", "AAPL": "AAPL", "MSFT": "MSFT",
    "NVDA": "NVDA", "AMZN": "AMZN", "META": "META",
    "GOOGL": "GOOGL", "NFLX": "NFLX", "AMD": "AMD",
}

FMP_MAP = {
    "US30": "^DJI", "US100": "^GSPC", "NAS100": "^IXIC",
    "WTIUSD": "CLUSD", "XCUUSD": "HGUSD",
    "XAUUSD": "XAUUSD", "XAGUSD": "XAGUSD",
    "EURUSD": "EURUSD", "USDJPY": "USDJPY", "GBPUSD": "GBPUSD",
    "USDCHF": "USDCHF", "AUDUSD": "AUDUSD", "USDCAD": "USDCAD",
    "NZDUSD": "NZDUSD", "EURJPY": "EURJPY", "GBPJPY": "GBPJPY",
    "AUDJPY": "AUDJPY", "EURGBP": "EURGBP", "EURAUD": "EURAUD",
    "GBPAUD": "GBPAUD", "EURCHF": "EURCHF", "CADJPY": "CADJPY",
    "NZDJPY": "NZDJPY", "GBPCAD": "GBPCAD", "AUDCAD": "AUDCAD",
    "AUDNZD": "AUDNZD", "CHFJPY": "CHFJPY", "EURNZD": "EURNZD",
    "GBPNZD": "GBPNZD",
    "TSLA": "TSLA", "AAPL": "AAPL", "MSFT": "MSFT",
    "NVDA": "NVDA", "AMZN": "AMZN", "META": "META",
    "GOOGL": "GOOGL", "NFLX": "NFLX", "AMD": "AMD",
}

BINANCE_MAP = {
    "BTCUSD": "BTCUSDT", "ETHUSD": "ETHUSDT",
    "SOLUSD": "SOLUSDT", "XRPUSD": "XRPUSDT", "LINKUSD": "LINKUSDT",
}


class PriceService:
    """
    On-demand price fetcher for pipeline entry price.
    Routes by asset class. Single call per signal, not continuous.
    """

    def __init__(self):
        self.client = httpx.AsyncClient(timeout=8.0)

    async def close(self):
        await self.client.aclose()

    async def get_price(self, symbol: str) -> Optional[float]:
        """
        Fetch a single live price. Routes to the best provider by asset class.
        Returns None if all providers fail.
        """
        sym = symbol.upper()

        if sym in CRYPTO:
            return await self._try_chain(sym, [self._binance, self._fmp])
        elif sym in STOCKS:
            return await self._try_chain(sym, [self._finnhub, self._fmp])
        elif sym in INDICES or sym in COMMODITIES:
            return await self._try_chain(sym, [self._fmp, self._twelvedata])
        elif sym in METALS:
            return await self._try_chain(sym, [self._twelvedata, self._fmp])
        else:
            # Forex (default)
            return await self._try_chain(sym, [self._twelvedata, self._fmp])

    async def _try_chain(self, symbol: str, providers: list) -> Optional[float]:
        """Try providers in order, return first success."""
        for provider_fn in providers:
            try:
                price = await provider_fn(symbol)
                if price and price > 0:
                    return price
            except Exception as e:
                logger.debug(f"Price fetch failed for {symbol}: {e}")
        return None

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # PROVIDERS
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def _twelvedata(self, symbol: str) -> Optional[float]:
        """TwelveData single-symbol price fetch."""
        if not TWELVEDATA_API_KEY:
            return None
        td_sym = TD_MAP.get(symbol)
        if not td_sym:
            return None
        try:
            resp = await self.client.get(
                "https://api.twelvedata.com/price",
                params={"symbol": td_sym, "apikey": TWELVEDATA_API_KEY},
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") != "error" and "price" in data:
                    return float(data["price"])
        except (httpx.TimeoutException, httpx.RequestError):
            pass
        return None

    async def _finnhub(self, symbol: str) -> Optional[float]:
        """Finnhub single-symbol quote (US stocks only)."""
        if not FINNHUB_API_KEY:
            return None
        fh_sym = FH_MAP.get(symbol)
        if not fh_sym:
            return None
        try:
            resp = await self.client.get(
                "https://finnhub.io/api/v1/quote",
                params={"symbol": fh_sym, "token": FINNHUB_API_KEY},
            )
            if resp.status_code == 200:
                data = resp.json()
                price = data.get("c", 0)
                if price and float(price) > 0:
                    return float(price)
        except (httpx.TimeoutException, httpx.RequestError):
            pass
        return None

    async def _fmp(self, symbol: str) -> Optional[float]:
        """FMP single-symbol quote (universal fallback)."""
        if not FMP_API_KEY:
            return None
        fmp_sym = FMP_MAP.get(symbol, symbol)
        try:
            resp = await self.client.get(
                f"https://financialmodelingprep.com/api/v3/quote/{fmp_sym}",
                params={"apikey": FMP_API_KEY},
            )
            if resp.status_code == 200:
                data = resp.json()
                if isinstance(data, list) and data:
                    price = data[0].get("price", 0)
                    if price and float(price) > 0:
                        return float(price)
        except (httpx.TimeoutException, httpx.RequestError):
            pass
        return None

    async def _binance(self, symbol: str) -> Optional[float]:
        """Binance REST single-symbol price (crypto, no key needed)."""
        binance_sym = BINANCE_MAP.get(symbol)
        if not binance_sym:
            return None
        try:
            resp = await self.client.get(
                "https://api.binance.com/api/v3/ticker/price",
                params={"symbol": binance_sym},
            )
            if resp.status_code == 200:
                data = resp.json()
                price = float(data.get("price", 0))
                if price > 0:
                    return price
        except (httpx.TimeoutException, httpx.RequestError):
            pass
        return None
```

## `app/services/diagnostics.py`

```python
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
        """Check price provider health from 4-provider JIT simulator stats."""
        issues = []
        try:
            from app.services.server_simulator import get_provider_stats
            stats = get_provider_stats()

            for name in ["twelvedata", "finnhub", "fmp", "kraken_ws"]:
                s = stats.get(name, {})
                success = s.get("success", 0)
                fail = s.get("fail", 0)
                state = s.get("state", "CLOSED")
                total = success + fail

                if state == "OPEN":
                    issues.append(f"🔴 {name} circuit OPEN (tripped)")

                if total == 0:
                    continue

                fail_rate = fail / total if total > 0 else 0

                if fail_rate > 0.5 and total > 10:
                    issues.append(
                        f"🟡 {name}: {fail_rate*100:.0f}% failure rate ({fail}/{total})"
                    )

                if fail_rate > 0.9 and total > 20:
                    issues.append(
                        f"🔴 {name} DOWN: {fail_rate*100:.0f}% failures"
                    )

            # TD daily budget warning
            td_throttle = stats.get("td_throttle", {})
            daily = td_throttle.get("daily_credits", 0)
            if daily > 600:
                issues.append(f"🟡 TwelveData daily budget: {daily}/800 credits used")

        except Exception as e:
            logger.debug(f"Price provider check failed: {e}")
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

                # Price provider stats (4-provider JIT)
                from app.services.server_simulator import get_provider_stats
                pstats = get_provider_stats()
                td_ok = pstats.get("twelvedata", {}).get("success", 0) > 0
                fh_ok = pstats.get("finnhub", {}).get("success", 0) > 0
                fmp_ok = pstats.get("fmp", {}).get("success", 0) > 0
                ws_ok = pstats.get("kraken_ws", {}).get("state", "") != "OPEN"
                active_providers = sum([td_ok, fh_ok, fmp_ok, ws_ok])

                # Error count
                total_errors = sum(self.error_counts.values())

            finally:
                db.close()

            status_emoji = "🟢" if total_errors == 0 else ("🟡" if total_errors < 5 else "🔴")

            text = (
                f"{status_emoji} <b>SYSTEM HEALTH</b>\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📡 Price Providers   {active_providers}/4 active\n"
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
```

## `app/services/metaapi_executor.py`

```python
"""
MetaApi Cloud Executor — $1M Demo Account Bridge
Executes approved trades on a real broker demo via MetaApi REST API.
Runs parallel to the server simulator: simulator does math, MetaApi gets real fills.

Setup:
  1. Sign up at metaapi.cloud (free tier = 1 account)
  2. Add your broker demo account (IC Markets, etc.)
  3. Copy METAAPI_TOKEN + METAAPI_ACCOUNT_ID to Railway variables
  4. Trades auto-execute on your demo — no VPS, no EA, no MT5

REST API Reference:
  Base: https://mt-client-api-v1.{region}.agiliumtrade.ai
  Auth: auth-token header
  Trade: POST /users/current/accounts/{id}/trade
  Positions: GET /users/current/accounts/{id}/positions
  Account: GET /users/current/accounts/{id}/account-information
"""
import os
import logging
from datetime import datetime, timezone
from typing import Dict, Optional

import httpx

logger = logging.getLogger("TradingSystem.MetaApi")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONFIG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

METAAPI_TOKEN = os.getenv("METAAPI_TOKEN", "")
METAAPI_ACCOUNT_ID = os.getenv("METAAPI_ACCOUNT_ID", "")
METAAPI_REGION = os.getenv("METAAPI_REGION", "new-york")

# Base URL for MetaApi client REST API
METAAPI_BASE = f"https://mt-client-api-v1.{METAAPI_REGION}.agiliumtrade.ai"


def is_enabled() -> bool:
    """Check if MetaApi execution is configured."""
    return bool(METAAPI_TOKEN and METAAPI_ACCOUNT_ID)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SYMBOL MAPPING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# OniQuant internal → Broker symbol (IC Markets MT5 default)
# Most brokers use raw symbol names. If your broker appends
# suffixes (e.g., EURUSDm, EURUSD.raw), override with
# METAAPI_SYMBOL_SUFFIX env var.

SYMBOL_SUFFIX = os.getenv("METAAPI_SYMBOL_SUFFIX", "")

# Symbols that need special mapping (broker uses different name)
SYMBOL_OVERRIDE = {
    "US30": "DJ30",
    "US100": "USTEC",
    "NAS100": "USTEC",
    "WTIUSD": "XTIUSD",
    "XCUUSD": "COPPER",
    "BTCUSD": "BTCUSD",
    "ETHUSD": "ETHUSD",
    "SOLUSD": "SOLUSD",
    "XRPUSD": "XRPUSD",
    "LINKUSD": "LINKUSD",
}


def to_broker_symbol(internal: str) -> str:
    """Convert OniQuant symbol to broker symbol."""
    mapped = SYMBOL_OVERRIDE.get(internal, internal)
    return mapped + SYMBOL_SUFFIX


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# LOT SIZING FOR $1M ACCOUNT
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

METAAPI_CAPITAL = 1_000_000  # $1M demo account
METAAPI_RISK_PCT = 1.0       # 1% risk per trade (conservative for combined desks)


def scale_lot_size(original_lot: float, original_capital: float) -> float:
    """
    Scale lot size from $100K desk to $1M account proportionally.
    $100K @ 0.5 lots → $1M @ 5.0 lots (10x)
    """
    if not original_capital or original_capital <= 0:
        return original_lot
    ratio = METAAPI_CAPITAL / original_capital
    scaled = round(original_lot * ratio, 2)
    # Clamp to reasonable bounds
    return max(0.01, min(scaled, 100.0))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# EXECUTOR
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class MetaApiExecutor:
    """
    Sends trade orders to MetaApi cloud for execution on a broker demo.
    Fire-and-forget: if MetaApi fails, the server simulator still works.
    """

    def __init__(self):
        self.client = httpx.AsyncClient(timeout=15.0)
        self._headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "auth-token": METAAPI_TOKEN,
        }
        self._trade_url = (
            f"{METAAPI_BASE}/users/current/accounts/{METAAPI_ACCOUNT_ID}/trade"
        )
        self._positions_url = (
            f"{METAAPI_BASE}/users/current/accounts/{METAAPI_ACCOUNT_ID}/positions"
        )
        self._account_url = (
            f"{METAAPI_BASE}/users/current/accounts/{METAAPI_ACCOUNT_ID}"
            f"/account-information"
        )
        # Map OniQuant trade_id → MetaApi positionId for close routing
        self._position_map: Dict[int, str] = {}

    async def close(self):
        """Cleanup."""
        await self.client.aclose()

    # ── OPEN TRADE ──────────────────────────────────────────────

    async def open_trade(
        self,
        trade_id: int,
        symbol: str,
        direction: str,
        lot_size: float,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
        desk_capital: float = 100_000,
        comment: str = "",
    ) -> Dict:
        """
        Execute a market order on MetaApi.
        Returns {"success": True, "orderId": "...", "positionId": "..."} or error.
        """
        if not is_enabled():
            return {"success": False, "error": "MetaApi not configured"}

        broker_symbol = to_broker_symbol(symbol)
        scaled_lot = scale_lot_size(lot_size, desk_capital)
        is_buy = direction.upper() in ("LONG", "BUY")

        body = {
            "actionType": "ORDER_TYPE_BUY" if is_buy else "ORDER_TYPE_SELL",
            "symbol": broker_symbol,
            "volume": scaled_lot,
            "comment": comment or f"OQ#{trade_id}",
        }
        if stop_loss and stop_loss > 0:
            body["stopLoss"] = round(stop_loss, 5)
        if take_profit and take_profit > 0:
            body["takeProfit"] = round(take_profit, 5)

        try:
            resp = await self.client.post(
                self._trade_url,
                json=body,
                headers=self._headers,
            )

            if resp.status_code == 200:
                data = resp.json()
                position_id = data.get("positionId", "")
                order_id = data.get("orderId", "")
                string_code = data.get("stringCode", "")

                if string_code == "TRADE_RETCODE_DONE":
                    # Track for close routing
                    if position_id:
                        self._position_map[trade_id] = position_id

                    logger.info(
                        f"MetaApi OPEN | #{trade_id} | {broker_symbol} "
                        f"{'BUY' if is_buy else 'SELL'} {scaled_lot} lots | "
                        f"Order: {order_id} | Position: {position_id}"
                    )
                    return {
                        "success": True,
                        "orderId": order_id,
                        "positionId": position_id,
                        "stringCode": string_code,
                        "volume": scaled_lot,
                        "broker_symbol": broker_symbol,
                    }
                else:
                    msg = data.get("message", string_code)
                    logger.warning(
                        f"MetaApi REJECT | #{trade_id} | {broker_symbol} | {msg}"
                    )
                    return {"success": False, "error": msg, "data": data}
            else:
                logger.warning(
                    f"MetaApi HTTP {resp.status_code} | #{trade_id} | "
                    f"{resp.text[:200]}"
                )
                return {"success": False, "error": f"HTTP {resp.status_code}"}

        except httpx.TimeoutException:
            logger.warning(f"MetaApi TIMEOUT | #{trade_id} | {broker_symbol}")
            return {"success": False, "error": "timeout"}
        except Exception as e:
            logger.error(f"MetaApi ERROR | #{trade_id} | {e}")
            return {"success": False, "error": str(e)}

    # ── CLOSE TRADE ─────────────────────────────────────────────

    async def close_trade(
        self,
        trade_id: int,
        symbol: str = "",
    ) -> Dict:
        """
        Close a position on MetaApi by trade_id (looks up positionId).
        Falls back to close-by-symbol if positionId unknown.
        """
        if not is_enabled():
            return {"success": False, "error": "MetaApi not configured"}

        position_id = self._position_map.pop(trade_id, None)

        if position_id:
            body = {
                "actionType": "POSITION_CLOSE_ID",
                "positionId": position_id,
            }
        elif symbol:
            # Fallback: close all positions for this symbol
            broker_symbol = to_broker_symbol(symbol)
            body = {
                "actionType": "POSITIONS_CLOSE_SYMBOL",
                "symbol": broker_symbol,
            }
        else:
            return {"success": False, "error": "No positionId or symbol"}

        try:
            resp = await self.client.post(
                self._trade_url,
                json=body,
                headers=self._headers,
            )

            if resp.status_code == 200:
                data = resp.json()
                string_code = data.get("stringCode", "")
                if string_code == "TRADE_RETCODE_DONE":
                    logger.info(
                        f"MetaApi CLOSE | #{trade_id} | "
                        f"Position: {position_id or 'by-symbol'}"
                    )
                    return {"success": True, "data": data}
                else:
                    msg = data.get("message", string_code)
                    logger.warning(f"MetaApi CLOSE REJECT | #{trade_id} | {msg}")
                    return {"success": False, "error": msg}
            else:
                return {"success": False, "error": f"HTTP {resp.status_code}"}

        except Exception as e:
            logger.error(f"MetaApi CLOSE ERROR | #{trade_id} | {e}")
            return {"success": False, "error": str(e)}

    # ── MODIFY SL/TP ────────────────────────────────────────────

    async def modify_position(
        self,
        trade_id: int,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
    ) -> Dict:
        """Update SL/TP on an open MetaApi position (e.g., move to breakeven)."""
        if not is_enabled():
            return {"success": False, "error": "MetaApi not configured"}

        position_id = self._position_map.get(trade_id)
        if not position_id:
            return {"success": False, "error": "No positionId tracked"}

        body = {
            "actionType": "POSITION_MODIFY",
            "positionId": position_id,
        }
        if stop_loss is not None:
            body["stopLoss"] = round(stop_loss, 5)
        if take_profit is not None:
            body["takeProfit"] = round(take_profit, 5)

        try:
            resp = await self.client.post(
                self._trade_url,
                json=body,
                headers=self._headers,
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get("stringCode") == "TRADE_RETCODE_DONE":
                    logger.debug(
                        f"MetaApi MODIFY | #{trade_id} | SL={stop_loss} TP={take_profit}"
                    )
                    return {"success": True, "data": data}
            return {"success": False, "error": f"HTTP {resp.status_code}"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # ── ACCOUNT INFO ────────────────────────────────────────────

    async def get_account_info(self) -> Dict:
        """Fetch MetaApi demo account balance/equity/margin."""
        if not is_enabled():
            return {"error": "MetaApi not configured"}

        try:
            resp = await self.client.get(
                self._account_url,
                headers=self._headers,
            )
            if resp.status_code == 200:
                return resp.json()
            return {"error": f"HTTP {resp.status_code}"}
        except Exception as e:
            return {"error": str(e)}

    async def get_positions(self) -> list:
        """Fetch all open positions on MetaApi demo."""
        if not is_enabled():
            return []

        try:
            resp = await self.client.get(
                self._positions_url,
                headers=self._headers,
            )
            if resp.status_code == 200:
                return resp.json()
            return []
        except Exception as e:
            logger.debug(f"MetaApi positions error: {e}")
            return []


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SINGLETON
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_executor: Optional[MetaApiExecutor] = None


def get_executor() -> MetaApiExecutor:
    """Get or create the singleton MetaApi executor."""
    global _executor
    if _executor is None:
        _executor = MetaApiExecutor()
    return _executor
```

## `app/utils/__init__.py`

```python

```

## `tests/test_phase1.py`

```python
"""
Phase 1 Tests - Webhook receiver and signal validation.
Run with: pytest tests/ -v
"""
import json
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch

# Patch DATABASE_URL before importing app
import os
os.environ["DATABASE_URL"] = "sqlite:///./test.db"
os.environ["WEBHOOK_SECRET"] = "test-secret"

from app.main import app
from app.database import engine, Base
from app.services.signal_validator import SignalValidator
from app.schemas import TradingViewAlert

client = TestClient(app)


@pytest.fixture(autouse=True)
def setup_db():
    """Create tables before each test, drop after."""
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


# ─────────────────────────────────────────────
# HEALTH CHECK
# ─────────────────────────────────────────────
def test_health_check():
    resp = client.get("/api/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] in ("operational", "degraded")
    assert "database" in data


# ─────────────────────────────────────────────
# WEBHOOK - VALID SIGNALS
# ─────────────────────────────────────────────
def _make_payload(**overrides):
    base = {
        "secret": "test-secret",
        "symbol": "EURUSD",
        "exchange": "FX",
        "timeframe": "5M",
        "alert_type": "bullish_confirmation",
        "price": 1.1050,
        "tp1": 1.1080,
        "tp2": 1.1120,
        "sl1": 1.1020,
        "sl2": 1.1000,
        "smart_trail": 1.1035,
    }
    base.update(overrides)
    return base


def test_valid_bullish_signal():
    resp = client.post("/api/webhook", json=_make_payload())
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "accepted"
    assert data["is_valid"] is True
    assert data["signal_id"] is not None
    assert "DESK1_SCALPER" in data["desks_matched"]


def test_valid_bearish_signal():
    payload = _make_payload(
        alert_type="bearish_confirmation",
        tp1=1.1020,
        tp2=1.0990,
        sl1=1.1080,
        sl2=1.1100,
    )
    resp = client.post("/api/webhook", json=payload)
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "accepted"
    assert data["is_valid"] is True


def test_gold_signal_routes_to_desk4():
    payload = _make_payload(
        symbol="XAUUSD",
        exchange="OANDA",
        alert_type="bullish_plus",
        price=2650.50,
        tp1=2670.00,
        tp2=2690.00,
        sl1=2640.00,
        sl2=2630.00,
    )
    resp = client.post("/api/webhook", json=payload)
    data = resp.json()
    assert data["is_valid"] is True
    assert "DESK4_GOLD" in data["desks_matched"]


def test_eurusd_routes_to_multiple_desks():
    """EURUSD is on both Desk 1 and Desk 2."""
    payload = _make_payload(
        symbol="EURUSD",
        exchange=None,
    )
    resp = client.post("/api/webhook", json=payload)
    data = resp.json()
    assert len(data["desks_matched"]) >= 2


# ─────────────────────────────────────────────
# WEBHOOK - REJECTED SIGNALS
# ─────────────────────────────────────────────
def test_wrong_secret():
    payload = _make_payload(secret="wrong-secret")
    resp = client.post("/api/webhook", json=payload)
    assert resp.status_code == 401


def test_invalid_json():
    resp = client.post(
        "/api/webhook",
        content="not json at all",
        headers={"Content-Type": "application/json"},
    )
    assert resp.status_code == 400


def test_unknown_symbol():
    payload = _make_payload(symbol="FAKEUSD", exchange=None)
    resp = client.post("/api/webhook", json=payload)
    data = resp.json()
    assert data["status"] == "rejected"
    assert data["is_valid"] is False


def test_bullish_sl_above_price():
    """SL above price on bullish = invalid."""
    payload = _make_payload(sl1=1.1100)  # price is 1.1050
    resp = client.post("/api/webhook", json=payload)
    data = resp.json()
    assert data["is_valid"] is False


def test_missing_sl_on_entry():
    """Entry signals require SL."""
    payload = _make_payload(sl1=None, sl2=None)
    resp = client.post("/api/webhook", json=payload)
    data = resp.json()
    assert data["is_valid"] is False


# ─────────────────────────────────────────────
# SIGNAL VALIDATOR UNIT TESTS
# ─────────────────────────────────────────────
def test_validator_unknown_alert_type():
    alert = TradingViewAlert(
        secret="x",
        symbol="EURUSD",
        timeframe="5M",
        alert_type="fake_signal",
        price=1.1050,
    )
    v = SignalValidator()
    valid, errors = v.validate(alert, "EURUSD", ["DESK1_SCALPER"])
    assert valid is False
    assert any("Unknown alert_type" in e for e in errors)


def test_validator_no_desk_match():
    alert = TradingViewAlert(
        secret="x",
        symbol="FAKEUSD",
        timeframe="5M",
        alert_type="bullish_confirmation",
        price=1.1050,
        sl1=1.1020,
    )
    v = SignalValidator()
    valid, errors = v.validate(alert, "FAKEUSD", [])
    assert valid is False
    assert any("does not match any desk" in e for e in errors)


# ─────────────────────────────────────────────
# DASHBOARD
# ─────────────────────────────────────────────
def test_dashboard():
    resp = client.get("/api/dashboard")
    assert resp.status_code == 200
    data = resp.json()
    assert data["firm_status"] == "OPERATIONAL"
    assert len(data["desks"]) == 6
```

## `tests/test_phase2.py`

```python
"""
Phase 2 Tests - Enrichment, ML scoring, consensus, CTO, and pipeline.
Run with: pytest tests/ -v
"""
import json
import pytest
from unittest.mock import patch, AsyncMock, MagicMock
import os

os.environ["DATABASE_URL"] = "sqlite:///./test_phase2.db"
os.environ["WEBHOOK_SECRET"] = "test-secret"

from app.services.ml_scorer import MLScorer
from app.services.consensus_scorer import ConsensusScorer
from app.services.claude_cto import ClaudeCTO
from app.services.risk_filter import HardRiskFilter
from app.services.twelvedata_enricher import TwelveDataEnricher
from app.config import DESKS


# ─────────────────────────────────────────────
# ML SCORER
# ─────────────────────────────────────────────
class TestMLScorer:
    def setup_method(self):
        self.scorer = MLScorer()

    def test_rule_based_score_returns_valid_range(self):
        signal = {
            "direction": "LONG",
            "alert_type": "bullish_confirmation",
            "price": 1.1050,
            "tp1": 1.1100,
            "sl1": 1.1020,
        }
        enrichment = {
            "rsi": 45,
            "atr_pct": 0.5,
            "is_kill_zone": False,
            "kill_zone_type": "NONE",
            "active_session": "LONDON",
            "volatility_regime": "NORMAL",
            "spread": 0.00012,
            "volume": 50000,
            "intermarket": {},
        }
        result = self.scorer.score(signal, enrichment, "DESK1_SCALPER")
        assert 0.0 <= result["ml_score"] <= 1.0
        assert result["ml_method"] == "rule_based"
        assert "ml_features" in result

    def test_plus_signal_scores_higher(self):
        base_signal = {
            "direction": "LONG",
            "alert_type": "bullish_confirmation",
            "price": 1.1050,
            "tp1": 1.1100,
            "sl1": 1.1020,
        }
        plus_signal = {**base_signal, "alert_type": "bullish_plus"}
        enrichment = {
            "rsi": 50, "atr_pct": 0.5, "is_kill_zone": False,
            "kill_zone_type": "NONE", "active_session": "LONDON",
            "volatility_regime": "NORMAL", "spread": 0, "volume": 0,
            "intermarket": {},
        }
        base_score = self.scorer.score(base_signal, enrichment, "DESK1_SCALPER")
        plus_score = self.scorer.score(plus_signal, enrichment, "DESK1_SCALPER")
        assert plus_score["ml_score"] > base_score["ml_score"]

    def test_kill_zone_boosts_score(self):
        signal = {
            "direction": "LONG",
            "alert_type": "bullish_confirmation",
            "price": 1.1050, "tp1": 1.1100, "sl1": 1.1020,
        }
        no_kz = {
            "rsi": 50, "atr_pct": 0.5, "is_kill_zone": False,
            "kill_zone_type": "NONE", "active_session": "LONDON",
            "volatility_regime": "NORMAL", "spread": 0, "volume": 0,
            "intermarket": {},
        }
        with_kz = {**no_kz, "is_kill_zone": True, "kill_zone_type": "OVERLAP"}
        score_no_kz = self.scorer.score(signal, no_kz, "DESK1_SCALPER")
        score_kz = self.scorer.score(signal, with_kz, "DESK1_SCALPER")
        assert score_kz["ml_score"] > score_no_kz["ml_score"]

    def test_bad_rr_lowers_score(self):
        """Bad risk/reward (TP closer than SL) should lower score."""
        good_rr = {
            "direction": "LONG", "alert_type": "bullish_confirmation",
            "price": 1.1050, "tp1": 1.1150, "sl1": 1.1020,
        }
        bad_rr = {
            "direction": "LONG", "alert_type": "bullish_confirmation",
            "price": 1.1050, "tp1": 1.1060, "sl1": 1.1000,
        }
        enrichment = {
            "rsi": 50, "atr_pct": 0.5, "is_kill_zone": False,
            "kill_zone_type": "NONE", "active_session": "LONDON",
            "volatility_regime": "NORMAL", "spread": 0, "volume": 0,
            "intermarket": {},
        }
        good = self.scorer.score(good_rr, enrichment, "DESK1_SCALPER")
        bad = self.scorer.score(bad_rr, enrichment, "DESK1_SCALPER")
        assert good["ml_score"] > bad["ml_score"]


# ─────────────────────────────────────────────
# CONSENSUS SCORER
# ─────────────────────────────────────────────
class TestConsensusScorer:
    def setup_method(self):
        self.scorer = ConsensusScorer()

    def test_basic_entry_signal_gets_points(self):
        signal = {
            "symbol": "EURUSD",
            "direction": "LONG",
            "alert_type": "bullish_confirmation",
            "timeframe": "5M",
        }
        enrichment = {
            "kill_zone_type": "NONE",
            "rsi": 50,
        }
        ml = {"ml_score": 0.55}
        result = self.scorer.score(signal, enrichment, "DESK1_SCALPER", ml)
        assert result["total_score"] >= 1  # at least entry trigger point
        assert "entry_trigger" in result["breakdown"]

    def test_plus_signal_gets_bonus(self):
        signal = {
            "symbol": "EURUSD",
            "direction": "LONG",
            "alert_type": "bullish_plus",
            "timeframe": "5M",
        }
        enrichment = {"kill_zone_type": "NONE", "rsi": 50}
        ml = {"ml_score": 0.55}
        result = self.scorer.score(signal, enrichment, "DESK1_SCALPER", ml)
        assert "bullish_bearish_plus" in result["breakdown"]
        assert result["total_score"] >= 2

    def test_kill_zone_overlap_bonus(self):
        signal = {
            "symbol": "EURUSD",
            "direction": "LONG",
            "alert_type": "bullish_confirmation",
            "timeframe": "5M",
        }
        enrichment = {"kill_zone_type": "OVERLAP", "rsi": 50}
        ml = {"ml_score": 0.70}
        result = self.scorer.score(signal, enrichment, "DESK1_SCALPER", ml)
        assert "kill_zone_overlap" in result["breakdown"]
        assert result["breakdown"]["kill_zone_overlap"] == 2

    def test_high_ml_gets_confirmation(self):
        signal = {
            "symbol": "EURUSD",
            "direction": "LONG",
            "alert_type": "bullish_confirmation",
            "timeframe": "5M",
        }
        enrichment = {"kill_zone_type": "NONE", "rsi": 50}
        ml = {"ml_score": 0.85}
        result = self.scorer.score(signal, enrichment, "DESK1_SCALPER", ml)
        assert "ml_confirm" in result["breakdown"]
        assert "ml_confirm_strong" in result["breakdown"]

    def test_score_tiers(self):
        """Verify score → tier mapping."""
        scorer = self.scorer

        # Build a signal that should score HIGH (7+)
        signal = {
            "symbol": "EURUSD",
            "direction": "LONG",
            "alert_type": "bullish_plus",  # +1 entry, +1 plus
            "timeframe": "5M",
        }
        enrichment = {
            "kill_zone_type": "OVERLAP",  # +2
            "rsi": 25,  # oversold bullish = +1
        }
        ml = {"ml_score": 0.85}  # +1, +1 strong
        recent = [
            {"symbol": "EURUSD", "timeframe": "15M", "direction": "LONG",
             "alert_type": "bullish_confirmation"},  # bias match +3
        ]
        result = scorer.score(signal, enrichment, "DESK1_SCALPER", ml, recent)
        assert result["total_score"] >= 7
        assert result["tier"] == "HIGH"
        assert result["size_multiplier"] == 1.0


# ─────────────────────────────────────────────
# CLAUDE CTO - HARD RULES
# ─────────────────────────────────────────────
class TestClaudeCTOHardRules:
    def setup_method(self):
        self.cto = ClaudeCTO()

    def test_skip_when_consensus_zero(self):
        result = self.cto._check_hard_rules(
            signal_data={"sl1": 1.1020},
            consensus={"total_score": 0},
            desk_state={"is_active": True, "is_paused": False,
                        "daily_loss": 0, "consecutive_losses": 0},
            firm_risk={"firm_drawdown_exceeded": False},
        )
        assert result is not None
        assert result["decision"] == "SKIP"

    def test_allow_when_consensus_1(self):
        result = self.cto._check_hard_rules(
            signal_data={"sl1": 1.1020},
            consensus={"total_score": 1},
            desk_state={"is_active": True, "is_paused": False,
                        "daily_loss": 0, "consecutive_losses": 0},
            firm_risk={"firm_drawdown_exceeded": False},
        )
        assert result is None  # no hard skip, proceed to Claude

    def test_skip_when_desk_paused(self):
        result = self.cto._check_hard_rules(
            signal_data={"sl1": 1.1020},
            consensus={"total_score": 8},
            desk_state={"is_active": True, "is_paused": True,
                        "daily_loss": 0, "consecutive_losses": 0},
            firm_risk={"firm_drawdown_exceeded": False},
        )
        assert result is not None
        assert result["decision"] == "SKIP"

    def test_skip_when_no_stop_loss(self):
        result = self.cto._check_hard_rules(
            signal_data={"sl1": None},
            consensus={"total_score": 8},
            desk_state={"is_active": True, "is_paused": False,
                        "daily_loss": 0, "consecutive_losses": 0},
            firm_risk={"firm_drawdown_exceeded": False},
        )
        assert result is not None
        assert result["decision"] == "SKIP"

    def test_skip_when_daily_loss_exceeded(self):
        result = self.cto._check_hard_rules(
            signal_data={"sl1": 1.1020},
            consensus={"total_score": 8},
            desk_state={"is_active": True, "is_paused": False,
                        "daily_loss": -4600, "consecutive_losses": 0},
            firm_risk={"firm_drawdown_exceeded": False},
        )
        assert result is not None
        assert result["decision"] == "SKIP"

    def test_passes_when_all_clear(self):
        result = self.cto._check_hard_rules(
            signal_data={"sl1": 1.1020},
            consensus={"total_score": 8},
            desk_state={"is_active": True, "is_paused": False,
                        "daily_loss": -1000, "consecutive_losses": 1},
            firm_risk={"firm_drawdown_exceeded": False},
        )
        assert result is None  # None means no hard skip

    def test_rule_based_fallback_high_consensus(self):
        result = self.cto._rule_based_decision(
            signal_data={"symbol": "EURUSD", "direction": "LONG"},
            enrichment={"volatility_regime": "NORMAL"},
            ml_result={"ml_score": 0.72},
            consensus={"total_score": 8, "tier": "HIGH", "size_multiplier": 1.0},
            desk_state={"consecutive_losses": 0, "size_modifier": 1.0},
        )
        assert result["decision"] == "EXECUTE"
        assert result["size_multiplier"] > 0

    def test_rule_based_fallback_skip_tier(self):
        result = self.cto._rule_based_decision(
            signal_data={"symbol": "EURUSD", "direction": "LONG"},
            enrichment={"volatility_regime": "NORMAL"},
            ml_result={"ml_score": 0.55},
            consensus={"total_score": 1, "tier": "SKIP", "size_multiplier": 0.0},
            desk_state={"consecutive_losses": 0, "size_modifier": 1.0},
        )
        assert result["decision"] == "SKIP"

    def test_consecutive_losses_reduce_size(self):
        result = self.cto._rule_based_decision(
            signal_data={"symbol": "EURUSD", "direction": "LONG"},
            enrichment={"volatility_regime": "NORMAL"},
            ml_result={"ml_score": 0.72},
            consensus={"total_score": 8, "tier": "HIGH", "size_multiplier": 1.0},
            desk_state={"consecutive_losses": 3, "size_modifier": 0.50},
        )
        assert result["size_multiplier"] < 1.0


# ─────────────────────────────────────────────
# TWELVEDATA ENRICHER
# ─────────────────────────────────────────────
class TestTwelveDataEnricher:
    def setup_method(self):
        self.enricher = TwelveDataEnricher()

    def test_session_detection(self):
        session = self.enricher._detect_session()
        assert session in (
            "LONDON", "NEW_YORK", "LONDON_NY_OVERLAP",
            "SYDNEY", "TOKYO", "OFF_HOURS",
        )

    def test_rsi_classification(self):
        assert self.enricher._classify_rsi(85) == "EXTREME_OVERBOUGHT"
        assert self.enricher._classify_rsi(75) == "OVERBOUGHT"
        assert self.enricher._classify_rsi(60) == "BULLISH"
        assert self.enricher._classify_rsi(50) == "NEUTRAL"
        assert self.enricher._classify_rsi(35) == "BEARISH"
        assert self.enricher._classify_rsi(25) == "OVERSOLD"
        assert self.enricher._classify_rsi(15) == "EXTREME_OVERSOLD"

    def test_volatility_regime_detection(self):
        assert self.enricher._detect_volatility_regime(2.0) == "HIGH_VOLATILITY"
        assert self.enricher._detect_volatility_regime(0.8) == "TRENDING"
        assert self.enricher._detect_volatility_regime(0.3) == "NORMAL"
        assert self.enricher._detect_volatility_regime(0.1) == "LOW_VOLATILITY"
        assert self.enricher._detect_volatility_regime(None) == "UNKNOWN"

    def test_default_enrichment(self):
        result = self.enricher._default_enrichment("EURUSD", 1.1050)
        assert "active_session" in result
        assert "volatility_regime" in result
        assert result["rsi"] is None
        assert result["atr"] is None


# ─────────────────────────────────────────────
# INTEGRATION: FULL PIPELINE FLOW (mocked)
# ─────────────────────────────────────────────
class TestPipelineIntegration:
    """Test the pipeline flow with mocked external services."""

    def test_consensus_feeds_into_cto(self):
        """Verify consensus output drives CTO decision correctly."""
        consensus = ConsensusScorer()
        cto = ClaudeCTO()

        signal = {
            "symbol": "XAUUSD",
            "direction": "LONG",
            "alert_type": "bullish_plus",
            "timeframe": "15M",
            "sl1": 2640.0,
        }
        enrichment = {
            "kill_zone_type": "OVERLAP",
            "rsi": 35,
            "volatility_regime": "TRENDING",
        }
        ml = {"ml_score": 0.75}

        # Get consensus
        cons_result = consensus.score(
            signal, enrichment, "DESK4_GOLD", ml
        )

        # Feed into CTO rule-based
        decision = cto._rule_based_decision(
            signal_data=signal,
            enrichment=enrichment,
            ml_result=ml,
            consensus=cons_result,
            desk_state={"consecutive_losses": 0, "size_modifier": 1.0},
        )

        # With plus signal + overlap + ML confirmation, should execute
        assert decision["decision"] in ("EXECUTE", "REDUCE")
        assert decision["size_multiplier"] > 0
```

## `tests/test_phase3.py`

```python
"""
Phase 3 Tests - Trade queue API endpoints for VPS integration.
"""
import json
import pytest
import os

os.environ["DATABASE_URL"] = "sqlite:///./test_phase3.db"
os.environ["WEBHOOK_SECRET"] = "test-secret"

from fastapi.testclient import TestClient
from app.main import app
from app.database import Base, engine

client = TestClient(app)

@pytest.fixture(autouse=True)
def setup_db():
    Base.metadata.create_all(bind=engine)
    yield
    Base.metadata.drop_all(bind=engine)


def _create_decided_signal():
    """Helper: create a signal that's been through the pipeline and approved."""
    resp = client.post("/api/webhook", json={
        "secret": "test-secret",
        "symbol": "EURUSD",
        "exchange": "FX",
        "timeframe": "5M",
        "alert_type": "bullish_confirmation",
        "price": 1.1050,
        "tp1": 1.1080,
        "tp2": 1.1120,
        "sl1": 1.1020,
        "sl2": 1.1000,
    })
    assert resp.status_code == 200
    signal_id = resp.json()["signal_id"]

    from app.database import SessionLocal
    from app.models.signal import Signal
    db = SessionLocal()
    signal = db.query(Signal).filter(Signal.id == signal_id).first()
    signal.status = "DECIDED"
    signal.claude_decision = "EXECUTE"
    signal.consensus_score = 8
    signal.ml_score = 0.72
    signal.position_size_pct = 0.5
    signal.desk_id = "DESK1_SCALPER"
    db.commit()
    db.close()

    return signal_id


def test_pending_trades_empty():
    resp = client.get("/api/trades/pending", headers={"X-API-Key": "test-secret"})
    assert resp.status_code == 200
    assert resp.json()["count"] == 0


def test_pending_trades_auth_required():
    resp = client.get("/api/trades/pending", headers={"X-API-Key": "wrong-key"})
    assert resp.status_code == 401


def test_pending_trades_returns_decided():
    signal_id = _create_decided_signal()
    resp = client.get("/api/trades/pending", headers={"X-API-Key": "test-secret"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 1
    trade = data["pending"][0]
    assert trade["signal_id"] == signal_id
    assert trade["symbol"] == "EURUSD"
    assert trade["direction"] == "LONG"
    assert trade["desk_id"] == "DESK1_SCALPER"


def test_pending_trade_marked_queued():
    _create_decided_signal()
    resp1 = client.get("/api/trades/pending", headers={"X-API-Key": "test-secret"})
    assert resp1.json()["count"] == 1
    resp2 = client.get("/api/trades/pending", headers={"X-API-Key": "test-secret"})
    assert resp2.json()["count"] == 0


def test_report_execution():
    signal_id = _create_decided_signal()
    client.get("/api/trades/pending", headers={"X-API-Key": "test-secret"})

    resp = client.post("/api/trades/executed", json={
        "signal_id": signal_id,
        "desk_id": "DESK1_SCALPER",
        "symbol": "EURUSD",
        "direction": "LONG",
        "mt5_ticket": 12345678,
        "entry_price": 1.1052,
        "lot_size": 0.15,
        "stop_loss": 1.1020,
        "take_profit": 1.1080,
    }, headers={"X-API-Key": "test-secret"})

    assert resp.status_code == 200
    assert resp.json()["status"] == "recorded"

    sig_resp = client.get(f"/api/signal/{signal_id}")
    assert sig_resp.json()["status"] == "EXECUTED"


def test_report_close():
    signal_id = _create_decided_signal()
    client.get("/api/trades/pending", headers={"X-API-Key": "test-secret"})

    client.post("/api/trades/executed", json={
        "signal_id": signal_id,
        "desk_id": "DESK1_SCALPER",
        "symbol": "EURUSD",
        "direction": "LONG",
        "mt5_ticket": 99999,
        "entry_price": 1.1052,
        "lot_size": 0.15,
        "stop_loss": 1.1020,
        "take_profit": 1.1080,
    }, headers={"X-API-Key": "test-secret"})

    resp = client.post("/api/trades/closed", json={
        "mt5_ticket": 99999,
        "exit_price": 1.1078,
        "pnl_dollars": 39.0,
        "pnl_pips": 26.0,
        "close_reason": "TP",
    }, headers={"X-API-Key": "test-secret"})

    assert resp.status_code == 200
    assert resp.json()["pnl"] == 39.0


def test_open_trades_listing():
    signal_id = _create_decided_signal()
    client.get("/api/trades/pending", headers={"X-API-Key": "test-secret"})

    client.post("/api/trades/executed", json={
        "signal_id": signal_id,
        "desk_id": "DESK1_SCALPER",
        "symbol": "EURUSD",
        "direction": "LONG",
        "mt5_ticket": 55555,
        "entry_price": 1.1052,
        "lot_size": 0.15,
        "stop_loss": 1.1020,
    }, headers={"X-API-Key": "test-secret"})

    resp = client.get("/api/trades/open", headers={"X-API-Key": "test-secret"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 1
    assert data["open_trades"][0]["mt5_ticket"] == 55555


def test_full_trade_lifecycle():
    signal_id = _create_decided_signal()

    r1 = client.get("/api/trades/pending", headers={"X-API-Key": "test-secret"})
    assert r1.json()["count"] == 1

    r2 = client.post("/api/trades/executed", json={
        "signal_id": signal_id,
        "desk_id": "DESK1_SCALPER",
        "symbol": "EURUSD",
        "direction": "LONG",
        "mt5_ticket": 77777,
        "entry_price": 1.1052,
        "lot_size": 0.10,
        "stop_loss": 1.1020,
        "take_profit": 1.1080,
    }, headers={"X-API-Key": "test-secret"})
    assert r2.json()["status"] == "recorded"

    r3 = client.get("/api/trades/open", headers={"X-API-Key": "test-secret"})
    assert r3.json()["count"] == 1

    r4 = client.post("/api/trades/closed", json={
        "mt5_ticket": 77777,
        "exit_price": 1.1080,
        "pnl_dollars": 28.0,
        "close_reason": "TP",
    }, headers={"X-API-Key": "test-secret"})
    assert r4.json()["status"] == "recorded"

    r5 = client.get("/api/trades/open", headers={"X-API-Key": "test-secret"})
    assert r5.json()["count"] == 0

    r6 = client.get(f"/api/signal/{signal_id}")
    assert r6.json()["status"] == "CLOSED"
```

## `vps/AutonomousTradingSystem_EA.mq5`

```cpp
//+------------------------------------------------------------------+
//|                                    AutonomousTradingSystem_EA.mq5 |
//|                              Institutional Autonomous Trading Sys |
//|                         Polls Railway API for approved trades     |
//+------------------------------------------------------------------+
#property copyright "Autonomous Trading System"
#property version   "3.2"
#property strict

//+------------------------------------------------------------------+
//| INPUT PARAMETERS                                                 |
//+------------------------------------------------------------------+
input string   InpRailwayURL     = "https://trading-system-production-4566.up.railway.app";
input string   InpAPIKey         = "";
input string   InpDeskFilter     = "ALL";
input int      InpPollSeconds    = 5;
input int      InpTrailSeconds   = 3;
input int      InpSlippage       = 30;
input double   InpMaxDailyLoss   = 5000.0;
input int      InpMaxPositions   = 10;
input double   InpPartialPct     = 50.0;
input bool     InpMoveSLtoBE     = true;
input int      InpMagicNumber    = 777777;
input bool     InpLiveMode       = false;

//+------------------------------------------------------------------+
//| GLOBAL VARIABLES                                                  |
//+------------------------------------------------------------------+
datetime g_lastPoll       = 0;
datetime g_lastTrailCheck = 0;
datetime g_lastExitPoll   = 0;
datetime g_lastHeartbeat  = 0;
double   g_dailyPnL       = 0;
double   g_dailyLoss      = 0;
bool     g_killSwitch     = false;
int      g_todayDate       = 0;
string   g_logPrefix       = "[ATS] ";

//--- Simulated trade tracking
struct SimTrade
{
   int      signalId;
   string   symbol;
   string   brokerSymbol;
   string   direction;
   string   deskId;
   double   entryPrice;
   double   stopLoss;
   double   currentSL;
   double   takeProfit1;
   double   takeProfit2;
   double   lotSize;
   double   highWaterMark;
   double   lowWaterMark;
   datetime openTime;
   bool     active;
   bool     tp1Hit;
   int      trailingPips;
};

SimTrade g_simTrades[];
int      g_simTradeCount = 0;

//--- Symbol mapping
string   g_symbolMapFrom[];
string   g_symbolMapTo[];

//+------------------------------------------------------------------+
//| Expert initialization                                            |
//+------------------------------------------------------------------+
int OnInit()
{
   if(InpAPIKey == "" || InpAPIKey == "YOUR-WEBHOOK-SECRET-HERE")
   {
      Alert("ERROR: Set your Railway API key in EA inputs!");
      return(INIT_PARAMETERS_INCORRECT);
   }
   if(InpRailwayURL == "")
   {
      Alert("ERROR: Set your Railway URL in EA inputs!");
      return(INIT_PARAMETERS_INCORRECT);
   }

   InitSymbolMap();
   EventSetMillisecondTimer(1000);
   ResetDailyCounters();

   Print(g_logPrefix, "========================================");
   Print(g_logPrefix, "AUTONOMOUS TRADING SYSTEM EA v3.2");
   Print(g_logPrefix, "Railway: ", InpRailwayURL);
   Print(g_logPrefix, "Poll interval: ", InpPollSeconds, "s");
   Print(g_logPrefix, "Desk filter: ", InpDeskFilter);
   Print(g_logPrefix, "Live mode: ", InpLiveMode ? "YES — REAL TRADES" : "SIMULATION");
   Print(g_logPrefix, "Magic number: ", InpMagicNumber);
   Print(g_logPrefix, "NEW: Live time exits + exit signal polling");
   Print(g_logPrefix, "========================================");
   Print(g_logPrefix, "Enable WebRequest: Tools > Options > Expert Advisors");
   Print(g_logPrefix, "Add: ", InpRailwayURL);

   return(INIT_SUCCEEDED);
}

//+------------------------------------------------------------------+
//| Expert deinitialization                                          |
//+------------------------------------------------------------------+
void OnDeinit(const int reason)
{
   EventKillTimer();
   Print(g_logPrefix, "EA stopped. Reason: ", reason);
}

//+------------------------------------------------------------------+
//| Timer event — main loop                                          |
//+------------------------------------------------------------------+
void OnTimer()
{
   //--- New day check
   MqlDateTime dtNow;
   TimeToStruct(TimeCurrent(), dtNow);
   int today = dtNow.day;
   if(today != g_todayDate)
   {
      ResetDailyCounters();
      g_todayDate = today;
   }

   if(g_killSwitch) return;

   //--- Daily loss check
   UpdateDailyPnL();
   if(g_dailyLoss <= -InpMaxDailyLoss)
   {
      Print(g_logPrefix, "DAILY LOSS LIMIT HIT: $", DoubleToString(g_dailyLoss, 2));
      g_killSwitch = true;
      CloseAllPositions("DAILY_LIMIT");
      ReportKillSwitch("daily_loss_limit");
      return;
   }

   datetime now = TimeCurrent();

   //--- Poll for pending entry trades
   if(now - g_lastPoll >= InpPollSeconds)
   {
      PollPendingTrades();
      g_lastPoll = now;
   }

   //--- Poll for server-side exit commands (same interval as entries)
   if(now - g_lastExitPoll >= InpPollSeconds)
   {
      PollExitCommands();
      g_lastExitPoll = now;
   }

   //--- Manage open positions: trailing, partial close, TIME EXITS
   if(now - g_lastTrailCheck >= InpTrailSeconds)
   {
      ManageOpenPositions();
      MonitorSimulatedTrades();
      g_lastTrailCheck = now;
   }

   //--- Heartbeat
   if(now - g_lastHeartbeat >= 60)
   {
      SendHeartbeat();
      g_lastHeartbeat = now;
   }

   //--- Check for closed positions to report
   CheckForClosedPositions();
}

//+------------------------------------------------------------------+
//| SYMBOL MAPPING                                                   |
//+------------------------------------------------------------------+
void InitSymbolMap()
{
   AddSymbolMap("EURUSD", "EURUSD");
   AddSymbolMap("USDJPY", "USDJPY");
   AddSymbolMap("GBPUSD", "GBPUSD");
   AddSymbolMap("USDCHF", "USDCHF");
   AddSymbolMap("AUDUSD", "AUDUSD");
   AddSymbolMap("USDCAD", "USDCAD");
   AddSymbolMap("NZDUSD", "NZDUSD");
   AddSymbolMap("EURJPY", "EURJPY");
   AddSymbolMap("GBPJPY", "GBPJPY");
   AddSymbolMap("AUDJPY", "AUDJPY");
   AddSymbolMap("EURGBP", "EURGBP");
   AddSymbolMap("EURAUD", "EURAUD");
   AddSymbolMap("GBPAUD", "GBPAUD");
   AddSymbolMap("EURCHF", "EURCHF");
   AddSymbolMap("CADJPY", "CADJPY");
   AddSymbolMap("NZDJPY", "NZDJPY");
   AddSymbolMap("GBPCAD", "GBPCAD");
   AddSymbolMap("AUDCAD", "AUDCAD");
   AddSymbolMap("AUDNZD", "AUDNZD");
   AddSymbolMap("CHFJPY", "CHFJPY");
   AddSymbolMap("EURNZD", "EURNZD");
   AddSymbolMap("GBPNZD", "GBPNZD");
   AddSymbolMap("XAUUSD", "XAUUSD");
   AddSymbolMap("XAGUSD", "XAGUSD");
   AddSymbolMap("WTIUSD", "WTIUSD");
   AddSymbolMap("XCUUSD", "XCUUSD");
   AddSymbolMap("BTCUSD", "BTCUSD");
   AddSymbolMap("ETHUSD", "ETHUSD");
   AddSymbolMap("SOLUSD", "SOLUSD");
   AddSymbolMap("XRPUSD", "XRPUSD");
   AddSymbolMap("LINKUSD", "LINKUSD");
   AddSymbolMap("US30",   "US30");
   AddSymbolMap("US100",  "US100");
   AddSymbolMap("NAS100", "NAS100");
   AddSymbolMap("TSLA",   "TSLA");
   AddSymbolMap("AAPL",   "AAPL");
   AddSymbolMap("MSFT",   "MSFT");
   AddSymbolMap("NVDA",   "NVDA");
   AddSymbolMap("AMZN",   "AMZN");
   AddSymbolMap("META",   "META");
   AddSymbolMap("GOOGL",  "GOOGL");
   AddSymbolMap("NFLX",   "NFLX");
   AddSymbolMap("AMD",    "AMD");
}

void AddSymbolMap(string from, string to)
{
   int size = ArraySize(g_symbolMapFrom);
   ArrayResize(g_symbolMapFrom, size + 1);
   ArrayResize(g_symbolMapTo, size + 1);
   g_symbolMapFrom[size] = from;
   g_symbolMapTo[size]   = to;
}

string MapSymbol(string systemSymbol)
{
   for(int i = 0; i < ArraySize(g_symbolMapFrom); i++)
   {
      if(g_symbolMapFrom[i] == systemSymbol)
         return g_symbolMapTo[i];
   }
   return systemSymbol;
}

//+------------------------------------------------------------------+
//| GET MAX HOLD SECONDS FOR A DESK                                  |
//+------------------------------------------------------------------+
int GetMaxHoldSeconds(string deskId)
{
   if(StringFind(deskId, "SCALPER") >= 0)       return 2 * 3600;
   if(StringFind(deskId, "INTRADAY") >= 0)      return 8 * 3600;
   if(StringFind(deskId, "GOLD") >= 0)           return 12 * 3600;
   if(StringFind(deskId, "SWING") >= 0)          return 72 * 3600;
   return 24 * 3600;  // Alts, Equities default
}

//+------------------------------------------------------------------+
//| EXTRACT DESK ID FROM POSITION COMMENT                            |
//+------------------------------------------------------------------+
string GetDeskFromComment(string comment)
{
   // Comment format: "ATS|signalId|DESK1_SCALPER"
   int pos1 = StringFind(comment, "|");
   if(pos1 < 0) return "";
   int pos2 = StringFind(comment, "|", pos1 + 1);
   if(pos2 < 0) return "";
   return StringSubstr(comment, pos2 + 1);
}

//+------------------------------------------------------------------+
//| POLL RAILWAY FOR PENDING TRADES                                  |
//+------------------------------------------------------------------+
void PollPendingTrades()
{
   string url = InpRailwayURL + "/api/trades/pending";
   string headers = "X-API-Key: " + InpAPIKey + "\r\nContent-Type: application/json\r\n";
   char   postData[];
   char   result[];
   string resultHeaders;

   int res = WebRequest("GET", url, headers, 10000, postData, result, resultHeaders);

   if(res == -1)
   {
      int err = GetLastError();
      if(err == 4014)
         Print(g_logPrefix, "WebRequest BLOCKED — add URL to Tools > Options > Expert Advisors");
      else
         Print(g_logPrefix, "Poll failed. Error: ", err);
      return;
   }

   if(res != 200) return;

   string response = CharArrayToString(result);
   int count = ExtractInt(response, "\"count\":");
   if(count == 0) return;

   Print(g_logPrefix, "=== ", count, " PENDING TRADE(S) FOUND ===");

   string pendingArray = ExtractArray(response, "\"pending\":");
   if(pendingArray == "") return;

   string trades[];
   int numTrades = SplitJsonArray(pendingArray, trades);

   for(int i = 0; i < numTrades; i++)
      ProcessPendingTrade(trades[i]);
}

//+------------------------------------------------------------------+
//| POLL FOR SERVER-SIDE EXIT COMMANDS                               |
//| Checks /api/trades/exits for trades flagged for closure by       |
//| LuxAlgo exit signals, time limits, or kill switch.               |
//+------------------------------------------------------------------+
void PollExitCommands()
{
   if(!InpLiveMode) return;  // sim trades don't need exit polling

   string deskParam = InpDeskFilter;
   string url = InpRailwayURL + "/api/trades/exits?desk=" + deskParam;
   string headers = "X-API-Key: " + InpAPIKey + "\r\nContent-Type: application/json\r\n";
   char   postData[];
   char   result[];
   string resultHeaders;

   int res = WebRequest("GET", url, headers, 10000, postData, result, resultHeaders);

   if(res != 200) return;

   string response = CharArrayToString(result);
   int count = ExtractInt(response, "\"count\":");
   if(count == 0) return;

   Print(g_logPrefix, "=== ", count, " EXIT COMMAND(S) RECEIVED ===");

   string exitsArray = ExtractArray(response, "\"exits\":");
   if(exitsArray == "") return;

   string exits[];
   int numExits = SplitJsonArray(exitsArray, exits);

   for(int i = 0; i < numExits; i++)
   {
      int mt5Ticket     = ExtractInt(exits[i], "\"mt5_ticket\":");
      string symbol     = ExtractString(exits[i], "\"symbol\":");
      string closeReason = ExtractString(exits[i], "\"close_reason\":");

      if(mt5Ticket <= 0) continue;

      Print(g_logPrefix, "EXIT CMD | Ticket: ", mt5Ticket,
            " | ", symbol, " | Reason: ", closeReason);

      bool closed = ClosePositionByTicket((ulong)mt5Ticket);
      if(closed)
      {
         Print(g_logPrefix, "EXIT EXECUTED | Ticket ", mt5Ticket, " closed");
      }
      else
      {
         Print(g_logPrefix, "EXIT FAILED | Ticket ", mt5Ticket,
               " — may already be closed or not found");
      }
   }
}

//+------------------------------------------------------------------+
//| CLOSE A SPECIFIC POSITION BY TICKET                              |
//+------------------------------------------------------------------+
bool ClosePositionByTicket(ulong ticket)
{
   //--- Search open positions for this ticket
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong posTicket = PositionGetTicket(i);
      if(posTicket != ticket) continue;
      if(PositionGetInteger(POSITION_MAGIC) != InpMagicNumber) continue;

      MqlTradeRequest request = {};
      MqlTradeResult  result  = {};

      request.action   = TRADE_ACTION_DEAL;
      request.position = posTicket;
      request.symbol   = PositionGetString(POSITION_SYMBOL);
      request.volume   = PositionGetDouble(POSITION_VOLUME);
      request.deviation = InpSlippage;
      request.magic    = InpMagicNumber;

      long posType = PositionGetInteger(POSITION_TYPE);
      if(posType == POSITION_TYPE_BUY)
      {
         request.type  = ORDER_TYPE_SELL;
         request.price = SymbolInfoDouble(request.symbol, SYMBOL_BID);
      }
      else
      {
         request.type  = ORDER_TYPE_BUY;
         request.price = SymbolInfoDouble(request.symbol, SYMBOL_ASK);
      }

      if(OrderSend(request, result))
      {
         if(result.retcode == TRADE_RETCODE_DONE)
         {
            Print(g_logPrefix, "Closed ticket ", posTicket,
                  " | ", request.symbol, " | ", result.comment);
            return true;
         }
      }

      Print(g_logPrefix, "Close FAILED ticket ", posTicket,
            " | Code: ", result.retcode, " | ", result.comment);
      return false;
   }

   // Position not found in open positions
   return false;
}

//+------------------------------------------------------------------+
//| PROCESS A SINGLE PENDING TRADE                                   |
//+------------------------------------------------------------------+
void ProcessPendingTrade(string &tradeJson)
{
   int    signalId   = ExtractInt(tradeJson, "\"signal_id\":");
   string symbol     = ExtractString(tradeJson, "\"symbol\":");
   string direction  = ExtractString(tradeJson, "\"direction\":");
   string deskId     = ExtractString(tradeJson, "\"desk_id\":");
   double price      = ExtractDouble(tradeJson, "\"price\":");
   double sl         = ExtractDouble(tradeJson, "\"stop_loss\":");
   double tp1        = ExtractDouble(tradeJson, "\"take_profit_1\":");
   double tp2        = ExtractDouble(tradeJson, "\"take_profit_2\":");
   double riskPct    = ExtractDouble(tradeJson, "\"risk_pct\":");

   Print(g_logPrefix, "Processing: Signal #", signalId,
         " | ", symbol, " ", direction,
         " | SL: ", sl, " | TP1: ", tp1,
         " | Risk: ", riskPct, "%",
         " | Desk: ", deskId);

   if(InpDeskFilter != "ALL" && deskId != InpDeskFilter)
   {
      Print(g_logPrefix, "SKIP: Trade for ", deskId, " not this EA (", InpDeskFilter, ")");
      return;
   }

   string brokerSymbol = MapSymbol(symbol);

   if(!SymbolSelect(brokerSymbol, true))
   {
      Print(g_logPrefix, "ERROR: Symbol ", brokerSymbol, " not found on broker");
      return;
   }

   if(CountOpenPositions() >= InpMaxPositions)
   {
      Print(g_logPrefix, "MAX POSITIONS reached (", InpMaxPositions, "). Skipping.");
      return;
   }

   double lotSize = CalculateLotSize(brokerSymbol, sl, riskPct);
   if(lotSize <= 0)
   {
      Print(g_logPrefix, "ERROR: Invalid lot size");
      return;
   }

   if(InpLiveMode)
   {
      ulong ticket = ExecuteTrade(brokerSymbol, direction, lotSize, sl, tp1, signalId, deskId);
      if(ticket > 0)
      {
         ReportExecution(signalId, deskId, symbol, direction, (int)ticket,
                         GetEntryPrice(ticket), lotSize, sl, tp1);
      }
   }
   else
   {
      Print(g_logPrefix, "SIMULATION: Would execute ", direction, " ",
            DoubleToString(lotSize, 2), " lots ", brokerSymbol,
            " SL=", sl, " TP=", tp1);

      int idx = ArraySize(g_simTrades);
      ArrayResize(g_simTrades, idx + 1);
      g_simTrades[idx].signalId     = signalId;
      g_simTrades[idx].symbol       = symbol;
      g_simTrades[idx].brokerSymbol = brokerSymbol;
      g_simTrades[idx].direction    = direction;
      g_simTrades[idx].deskId       = deskId;
      g_simTrades[idx].entryPrice   = SymbolInfoDouble(brokerSymbol, SYMBOL_BID);
      g_simTrades[idx].stopLoss     = sl;
      g_simTrades[idx].currentSL    = sl;
      g_simTrades[idx].takeProfit1  = tp1;
      g_simTrades[idx].takeProfit2  = tp2;
      g_simTrades[idx].lotSize      = lotSize;
      g_simTrades[idx].highWaterMark = g_simTrades[idx].entryPrice;
      g_simTrades[idx].lowWaterMark  = g_simTrades[idx].entryPrice;
      g_simTrades[idx].openTime     = TimeCurrent();
      g_simTrades[idx].active       = true;
      g_simTrades[idx].tp1Hit       = false;
      g_simTrades[idx].trailingPips = 0;
      g_simTradeCount++;

      Print(g_logPrefix, "SIM TRADE #", signalId, " opened | ",
            symbol, " ", direction, " @ ", DoubleToString(g_simTrades[idx].entryPrice, 5),
            " | SL: ", sl, " | TP: ", tp1);

      ReportExecution(signalId, deskId, symbol, direction, 999000 + signalId,
                      g_simTrades[idx].entryPrice, lotSize, sl, tp1);
   }
}

//+------------------------------------------------------------------+
//| EXECUTE A TRADE ON MT5                                           |
//+------------------------------------------------------------------+
ulong ExecuteTrade(string symbol, string direction, double lots,
                   double sl, double tp, int signalId, string deskId)
{
   MqlTradeRequest request = {};
   MqlTradeResult  result  = {};

   request.action    = TRADE_ACTION_DEAL;
   request.symbol    = symbol;
   request.volume    = lots;
   request.deviation = InpSlippage;
   request.magic     = InpMagicNumber;
   request.comment   = "ATS|" + IntegerToString(signalId) + "|" + deskId;

   double ask = SymbolInfoDouble(symbol, SYMBOL_ASK);
   double bid = SymbolInfoDouble(symbol, SYMBOL_BID);

   if(direction == "LONG")
   {
      request.type  = ORDER_TYPE_BUY;
      request.price = ask;
   }
   else if(direction == "SHORT")
   {
      request.type  = ORDER_TYPE_SELL;
      request.price = bid;
   }
   else
   {
      Print(g_logPrefix, "ERROR: Unknown direction: ", direction);
      return 0;
   }

   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);
   request.sl = NormalizeDouble(sl, digits);
   if(tp > 0)
      request.tp = NormalizeDouble(tp, digits);

   if(!OrderSend(request, result))
   {
      Print(g_logPrefix, "ORDER FAILED | Error: ", result.retcode, " | ", result.comment);
      return 0;
   }

   if(result.retcode == TRADE_RETCODE_DONE || result.retcode == TRADE_RETCODE_PLACED)
   {
      Print(g_logPrefix, "ORDER FILLED | Ticket: ", result.deal,
            " | ", symbol, " ", direction, " | Price: ", result.price, " | Lots: ", lots);
      return result.deal;
   }

   Print(g_logPrefix, "ORDER REJECTED | Code: ", result.retcode, " | ", result.comment);
   return 0;
}

//+------------------------------------------------------------------+
//| CALCULATE LOT SIZE                                               |
//+------------------------------------------------------------------+
double CalculateLotSize(string symbol, double slPrice, double riskPct)
{
   if(slPrice <= 0 || riskPct <= 0) return 0;

   double accountBalance = AccountInfoDouble(ACCOUNT_BALANCE);
   double riskDollars    = accountBalance * (riskPct / 100.0);

   double currentPrice = SymbolInfoDouble(symbol, SYMBOL_ASK);
   if(currentPrice <= 0) return 0;

   double slDistance = MathAbs(currentPrice - slPrice);
   if(slDistance <= 0) return 0;

   double tickValue = SymbolInfoDouble(symbol, SYMBOL_TRADE_TICK_VALUE);
   double tickSize  = SymbolInfoDouble(symbol, SYMBOL_TRADE_TICK_SIZE);
   if(tickValue <= 0 || tickSize <= 0) return 0;

   double slInTicks = slDistance / tickSize;
   double lots = riskDollars / (slInTicks * tickValue);

   double minLot  = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MIN);
   double maxLot  = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MAX);
   double lotStep = SymbolInfoDouble(symbol, SYMBOL_VOLUME_STEP);

   lots = MathFloor(lots / lotStep) * lotStep;
   lots = MathMax(minLot, MathMin(maxLot, lots));

   return NormalizeDouble(lots, 2);
}

//+------------------------------------------------------------------+
//| MANAGE OPEN POSITIONS                                            |
//| Handles: TP1 partial close, breakeven, AND time-based exits.     |
//| Time exits now work in BOTH live and simulation mode.            |
//+------------------------------------------------------------------+
void ManageOpenPositions()
{
   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0) continue;
      if(PositionGetInteger(POSITION_MAGIC) != InpMagicNumber) continue;

      string symbol    = PositionGetString(POSITION_SYMBOL);
      double openPrice = PositionGetDouble(POSITION_PRICE_OPEN);
      double currentSL = PositionGetDouble(POSITION_SL);
      double currentTP = PositionGetDouble(POSITION_TP);
      double volume    = PositionGetDouble(POSITION_VOLUME);
      long   posType   = PositionGetInteger(POSITION_TYPE);
      string comment   = PositionGetString(POSITION_COMMENT);
      datetime openTime = (datetime)PositionGetInteger(POSITION_TIME);

      double currentPrice;
      if(posType == POSITION_TYPE_BUY)
         currentPrice = SymbolInfoDouble(symbol, SYMBOL_BID);
      else
         currentPrice = SymbolInfoDouble(symbol, SYMBOL_ASK);

      int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);

      //--- TP1 PARTIAL CLOSE + BREAKEVEN MOVE ---
      if(InpPartialPct > 0 && currentTP > 0)
      {
         bool tp1Hit = false;
         if(posType == POSITION_TYPE_BUY && currentPrice >= currentTP)
            tp1Hit = true;
         if(posType == POSITION_TYPE_SELL && currentPrice <= currentTP)
            tp1Hit = true;

         if(tp1Hit && !IsPartialClosed(comment))
         {
            double closeVolume = NormalizeDouble(volume * (InpPartialPct / 100.0), 2);
            double minLot = SymbolInfoDouble(symbol, SYMBOL_VOLUME_MIN);
            if(closeVolume >= minLot)
            {
               PartialClose(ticket, symbol, posType, closeVolume);

               if(InpMoveSLtoBE)
               {
                  double beSL = NormalizeDouble(openPrice, digits);
                  ModifySLTP(ticket, symbol, beSL, 0);
                  Print(g_logPrefix, "SL moved to BE: ", beSL, " on ticket ", ticket);
               }
            }
         }
      }

      //--- TIME-BASED EXIT (LIVE MODE) ---
      // This was previously trapped in MonitorSimulatedTrades().
      // Now runs on REAL positions in live mode too.
      string deskId = GetDeskFromComment(comment);
      if(deskId == "" && InpDeskFilter != "ALL")
         deskId = InpDeskFilter;  // fallback to EA's desk filter

      int maxHoldSeconds = GetMaxHoldSeconds(deskId);
      if(maxHoldSeconds > 0 && openTime > 0)
      {
         int elapsed = (int)(TimeCurrent() - openTime);
         if(elapsed >= maxHoldSeconds)
         {
            Print(g_logPrefix, "TIME EXIT | Ticket: ", ticket,
                  " | ", symbol,
                  " | Held: ", elapsed / 3600, "h ", (elapsed % 3600) / 60, "m",
                  " | Max: ", maxHoldSeconds / 3600, "h",
                  " | Desk: ", deskId);

            // Close the position at market
            bool closed = ClosePositionByTicket(ticket);
            if(closed)
            {
               Print(g_logPrefix, "TIME EXIT EXECUTED | Ticket ", ticket, " closed");
               // Railway will get the close report via CheckForClosedPositions()
            }
            else
            {
               Print(g_logPrefix, "TIME EXIT FAILED | Ticket ", ticket);
            }
         }
      }
   }
}

//+------------------------------------------------------------------+
//| PARTIAL CLOSE A POSITION                                         |
//+------------------------------------------------------------------+
void PartialClose(ulong ticket, string symbol, long posType, double volume)
{
   MqlTradeRequest request = {};
   MqlTradeResult  result  = {};

   request.action   = TRADE_ACTION_DEAL;
   request.symbol   = symbol;
   request.volume   = volume;
   request.position = ticket;
   request.deviation = InpSlippage;
   request.magic    = InpMagicNumber;

   if(posType == POSITION_TYPE_BUY)
   {
      request.type  = ORDER_TYPE_SELL;
      request.price = SymbolInfoDouble(symbol, SYMBOL_BID);
   }
   else
   {
      request.type  = ORDER_TYPE_BUY;
      request.price = SymbolInfoDouble(symbol, SYMBOL_ASK);
   }

   if(OrderSend(request, result))
   {
      if(result.retcode == TRADE_RETCODE_DONE)
         Print(g_logPrefix, "PARTIAL CLOSE | Ticket: ", ticket,
               " | Closed: ", volume, " lots | ", symbol);
   }
   else
   {
      Print(g_logPrefix, "Partial close FAILED | ", result.retcode, " | ", result.comment);
   }
}

//+------------------------------------------------------------------+
//| MODIFY SL/TP OF A POSITION                                      |
//+------------------------------------------------------------------+
void ModifySLTP(ulong ticket, string symbol, double newSL, double newTP)
{
   MqlTradeRequest request = {};
   MqlTradeResult  result  = {};

   request.action   = TRADE_ACTION_SLTP;
   request.symbol   = symbol;
   request.position = ticket;

   int digits = (int)SymbolInfoInteger(symbol, SYMBOL_DIGITS);

   request.sl = (newSL > 0) ? NormalizeDouble(newSL, digits) : PositionGetDouble(POSITION_SL);
   request.tp = (newTP > 0) ? NormalizeDouble(newTP, digits) : PositionGetDouble(POSITION_TP);

   if(!OrderSend(request, result))
      Print(g_logPrefix, "Modify FAILED | ", result.retcode, " | ", result.comment);
}

//+------------------------------------------------------------------+
//| CHECK FOR POSITIONS THAT HAVE BEEN CLOSED                        |
//+------------------------------------------------------------------+
void CheckForClosedPositions()
{
   datetime dayStart = StringToTime(TimeToString(TimeCurrent(), TIME_DATE));
   if(!HistorySelect(dayStart, TimeCurrent())) return;

   for(int i = HistoryDealsTotal() - 1; i >= 0; i--)
   {
      ulong dealTicket = HistoryDealGetTicket(i);
      if(dealTicket == 0) continue;
      if(HistoryDealGetInteger(dealTicket, DEAL_MAGIC) != InpMagicNumber) continue;

      long entry = HistoryDealGetInteger(dealTicket, DEAL_ENTRY);
      if(entry != DEAL_ENTRY_OUT) continue;

      string dealComment = HistoryDealGetString(dealTicket, DEAL_COMMENT);
      if(StringFind(dealComment, "REPORTED") >= 0) continue;

      string symbol     = HistoryDealGetString(dealTicket, DEAL_SYMBOL);
      double profit     = HistoryDealGetDouble(dealTicket, DEAL_PROFIT);
      double swap       = HistoryDealGetDouble(dealTicket, DEAL_SWAP);
      double commission = HistoryDealGetDouble(dealTicket, DEAL_COMMISSION);
      double exitPrice  = HistoryDealGetDouble(dealTicket, DEAL_PRICE);
      long   posId      = HistoryDealGetInteger(dealTicket, DEAL_POSITION_ID);

      double totalPnl = profit + swap + commission;
      string reason   = DetermineCloseReason(dealTicket);

      Print(g_logPrefix, "CLOSED DEAL | Ticket: ", posId,
            " | ", symbol, " | PnL: $", DoubleToString(totalPnl, 2),
            " | Reason: ", reason);

      ReportClose((int)posId, exitPrice, totalPnl, 0, reason);
   }
}

string DetermineCloseReason(ulong dealTicket)
{
   long reason = HistoryDealGetInteger(dealTicket, DEAL_REASON);
   switch((int)reason)
   {
      case DEAL_REASON_SL:      return "SL";
      case DEAL_REASON_TP:      return "TP";
      case DEAL_REASON_SO:      return "STOP_OUT";
      case DEAL_REASON_CLIENT:  return "MANUAL";
      case DEAL_REASON_EXPERT:  return "EA_TIME_EXIT";
      default:                  return "UNKNOWN";
   }
}

//+------------------------------------------------------------------+
//| MONITOR SIMULATED TRADES (sim mode only)                         |
//+------------------------------------------------------------------+
void MonitorSimulatedTrades()
{
   if(InpLiveMode) return;

   for(int i = ArraySize(g_simTrades) - 1; i >= 0; i--)
   {
      if(!g_simTrades[i].active) continue;

      string sym = g_simTrades[i].brokerSymbol;
      double bid = SymbolInfoDouble(sym, SYMBOL_BID);
      double ask = SymbolInfoDouble(sym, SYMBOL_ASK);
      if(bid == 0) continue;

      double entry  = g_simTrades[i].entryPrice;
      double sl     = g_simTrades[i].currentSL;
      double tp1    = g_simTrades[i].takeProfit1;
      double tp2    = g_simTrades[i].takeProfit2;
      string dir    = g_simTrades[i].direction;
      double point  = SymbolInfoDouble(sym, SYMBOL_POINT);
      if(point == 0) point = 0.00001;

      bool   isLong = (dir == "LONG" || dir == "BUY");
      double currentPrice = isLong ? bid : ask;

      // Update water marks
      if(isLong && bid > g_simTrades[i].highWaterMark)
         g_simTrades[i].highWaterMark = bid;
      if(!isLong && ask < g_simTrades[i].lowWaterMark)
         g_simTrades[i].lowWaterMark = ask;

      // TP1 hit → breakeven + partial close
      if(!g_simTrades[i].tp1Hit && tp1 > 0)
      {
         bool tp1Reached = isLong ? (bid >= tp1) : (ask <= tp1);
         if(tp1Reached)
         {
            g_simTrades[i].tp1Hit = true;

            if(InpMoveSLtoBE)
            {
               double buffer = 2 * point;
               g_simTrades[i].currentSL = isLong ? (entry + buffer) : (entry - buffer);
               Print(g_logPrefix, "SIM #", g_simTrades[i].signalId,
                     " | TP1 HIT @ ", DoubleToString(tp1, 5),
                     " | SL → BE: ", DoubleToString(g_simTrades[i].currentSL, 5));
            }

            if(InpPartialPct > 0)
            {
               double closedLots = NormalizeDouble(g_simTrades[i].lotSize * (InpPartialPct / 100.0), 2);
               double partialPnl = isLong ? (tp1 - entry) : (entry - tp1);
               partialPnl = partialPnl / point * closedLots * SymbolInfoDouble(sym, SYMBOL_TRADE_TICK_VALUE);
               g_simTrades[i].lotSize -= closedLots;

               Print(g_logPrefix, "SIM #", g_simTrades[i].signalId,
                     " | PARTIAL CLOSE ", DoubleToString(InpPartialPct, 0), "% @ TP1",
                     " | Closed: ", DoubleToString(closedLots, 2), " lots",
                     " | PnL: $", DoubleToString(partialPnl, 2));
            }

            if(tp2 > 0)
               g_simTrades[i].takeProfit1 = tp2;

            sl = g_simTrades[i].currentSL;
         }
      }

      // Trailing stop after TP1
      if(g_simTrades[i].tp1Hit)
      {
         double trailDistance = MathAbs(entry - g_simTrades[i].stopLoss);
         if(trailDistance > 0)
         {
            double newTrailSL;
            if(isLong)
            {
               newTrailSL = g_simTrades[i].highWaterMark - trailDistance;
               if(newTrailSL > g_simTrades[i].currentSL)
                  g_simTrades[i].currentSL = newTrailSL;
            }
            else
            {
               newTrailSL = g_simTrades[i].lowWaterMark + trailDistance;
               if(newTrailSL < g_simTrades[i].currentSL)
                  g_simTrades[i].currentSL = newTrailSL;
            }
            sl = g_simTrades[i].currentSL;
         }
      }

      // Check SL
      bool hitSL = false;
      if(sl > 0)
      {
         if(isLong && bid <= sl)  hitSL = true;
         if(!isLong && ask >= sl) hitSL = true;
      }

      // Check TP2
      bool hitTP2 = false;
      if(g_simTrades[i].tp1Hit && tp2 > 0)
      {
         if(isLong && bid >= tp2)  hitTP2 = true;
         if(!isLong && ask <= tp2) hitTP2 = true;
      }

      // Time exit
      bool timeExpired = false;
      int maxHoldSeconds = GetMaxHoldSeconds(g_simTrades[i].deskId);
      if(maxHoldSeconds > 0)
      {
         int elapsed = (int)(TimeCurrent() - g_simTrades[i].openTime);
         if(elapsed >= maxHoldSeconds)
         {
            timeExpired = true;
            Print(g_logPrefix, "SIM #", g_simTrades[i].signalId,
                  " | TIME EXIT after ", elapsed / 3600, "h | Desk: ", g_simTrades[i].deskId);
         }
      }

      // Close if triggered
      if(hitSL || hitTP2 || timeExpired)
      {
         double exitPrice;
         if(hitTP2)          exitPrice = tp2;
         else if(hitSL)      exitPrice = sl;
         else                exitPrice = isLong ? bid : ask;

         double pnlPips    = isLong ? (exitPrice - entry) / point : (entry - exitPrice) / point;
         double pnlDollars = pnlPips * g_simTrades[i].lotSize * SymbolInfoDouble(sym, SYMBOL_TRADE_TICK_VALUE);

         string reason;
         if(hitTP2)               reason = "TP2_HIT";
         else if(timeExpired)     reason = "TIME_EXIT";
         else if(g_simTrades[i].tp1Hit) reason = "TRAILING_SL";
         else                     reason = "SL_HIT";

         string emoji = (pnlDollars >= 0) ? "WIN" : "LOSS";

         Print(g_logPrefix, "SIM CLOSED | #", g_simTrades[i].signalId,
               " | ", g_simTrades[i].symbol, " ", dir,
               " | Entry: ", DoubleToString(entry, 5),
               " | Exit: ", DoubleToString(exitPrice, 5),
               " | PnL: $", DoubleToString(pnlDollars, 2),
               " (", DoubleToString(pnlPips, 1), "p)",
               " | ", emoji, " — ", reason);

         ReportClose(999000 + g_simTrades[i].signalId, exitPrice,
                     pnlDollars, pnlPips, "SIM_" + reason,
                     g_simTrades[i].symbol, g_simTrades[i].deskId, dir);

         g_simTrades[i].active = false;
         g_simTradeCount--;
      }
   }
}

//+------------------------------------------------------------------+
//| CLOSE ALL POSITIONS — emergency kill switch                      |
//+------------------------------------------------------------------+
void CloseAllPositions(string reason)
{
   Print(g_logPrefix, "!!! CLOSING ALL POSITIONS — Reason: ", reason, " !!!");

   for(int i = PositionsTotal() - 1; i >= 0; i--)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket == 0) continue;
      if(PositionGetInteger(POSITION_MAGIC) != InpMagicNumber) continue;

      ClosePositionByTicket(ticket);
   }
}

//+------------------------------------------------------------------+
//| REPORT EXECUTION TO RAILWAY                                      |
//+------------------------------------------------------------------+
void ReportExecution(int signalId, string deskId, string symbol,
                     string direction, int mt5Ticket, double entryPrice,
                     double lotSize, double sl, double tp)
{
   string json = "{"
      + "\"signal_id\":" + IntegerToString(signalId) + ","
      + "\"desk_id\":\"" + deskId + "\","
      + "\"symbol\":\"" + symbol + "\","
      + "\"direction\":\"" + direction + "\","
      + "\"mt5_ticket\":" + IntegerToString(mt5Ticket) + ","
      + "\"entry_price\":" + DoubleToString(entryPrice, 5) + ","
      + "\"lot_size\":" + DoubleToString(lotSize, 2) + ","
      + "\"stop_loss\":" + DoubleToString(sl, 5) + ","
      + "\"take_profit\":" + DoubleToString(tp, 5)
      + "}";

   PostToRailway(InpRailwayURL + "/api/trades/executed", json, "Execution report");
}

//+------------------------------------------------------------------+
//| REPORT TRADE CLOSE TO RAILWAY                                    |
//+------------------------------------------------------------------+
void ReportClose(int mt5Ticket, double exitPrice, double pnl,
                 double pnlPips, string reason,
                 string symbol = "", string deskId = "", string direction = "")
{
   string json = "{"
      + "\"mt5_ticket\":" + IntegerToString(mt5Ticket) + ","
      + "\"exit_price\":" + DoubleToString(exitPrice, 5) + ","
      + "\"pnl_dollars\":" + DoubleToString(pnl, 2) + ","
      + "\"pnl_pips\":" + DoubleToString(pnlPips, 1) + ","
      + "\"close_reason\":\"" + reason + "\"";

   if(symbol != "")    json += ",\"symbol\":\"" + symbol + "\"";
   if(deskId != "")    json += ",\"desk_id\":\"" + deskId + "\"";
   if(direction != "") json += ",\"direction\":\"" + direction + "\"";

   json += "}";

   PostToRailway(InpRailwayURL + "/api/trades/closed", json, "Close report");
}

//+------------------------------------------------------------------+
//| REPORT KILL SWITCH TO RAILWAY                                    |
//+------------------------------------------------------------------+
void ReportKillSwitch(string reason)
{
   string json = "{\"scope\":\"ALL\",\"reason\":\"" + reason + "\"}";
   PostToRailway(InpRailwayURL + "/api/kill-switch?scope=ALL", json, "Kill switch");
}

//+------------------------------------------------------------------+
//| SEND HEARTBEAT TO RAILWAY                                        |
//+------------------------------------------------------------------+
void SendHeartbeat()
{
   string url = InpRailwayURL + "/api/health";
   string headers = "X-API-Key: " + InpAPIKey + "\r\n";
   char   postData[];
   char   result[];
   string resultHeaders;

   int res = WebRequest("GET", url, headers, 5000, postData, result, resultHeaders);
   if(res != 200)
      Print(g_logPrefix, "HEARTBEAT FAILED | HTTP: ", res);
}

//+------------------------------------------------------------------+
//| GENERIC POST TO RAILWAY                                          |
//+------------------------------------------------------------------+
void PostToRailway(string url, string jsonBody, string description)
{
   string headers = "X-API-Key: " + InpAPIKey
                  + "\r\nContent-Type: application/json\r\n";

   char postData[];
   StringToCharArray(jsonBody, postData, 0, WHOLE_ARRAY, CP_UTF8);
   ArrayResize(postData, ArraySize(postData) - 1);

   char   result[];
   string resultHeaders;

   int res = WebRequest("POST", url, headers, 10000, postData, result, resultHeaders);

   if(res == 200 || res == 201)
      Print(g_logPrefix, description, " sent successfully");
   else
   {
      string response = CharArrayToString(result);
      Print(g_logPrefix, description, " FAILED | HTTP: ", res, " | ", response);
   }
}

//+------------------------------------------------------------------+
//| UTILITIES                                                        |
//+------------------------------------------------------------------+
int CountOpenPositions()
{
   int count = 0;
   for(int i = 0; i < PositionsTotal(); i++)
   {
      ulong ticket = PositionGetTicket(i);
      if(ticket > 0 && PositionGetInteger(POSITION_MAGIC) == InpMagicNumber)
         count++;
   }
   return count;
}

double GetEntryPrice(ulong dealTicket)
{
   if(HistoryDealSelect(dealTicket))
      return HistoryDealGetDouble(dealTicket, DEAL_PRICE);
   return 0;
}

bool IsPartialClosed(string comment)
{
   return (StringFind(comment, "PARTIAL") >= 0);
}

void UpdateDailyPnL()
{
   g_dailyPnL  = 0;
   g_dailyLoss = 0;

   datetime dayStart = StringToTime(TimeToString(TimeCurrent(), TIME_DATE));
   if(!HistorySelect(dayStart, TimeCurrent())) return;

   for(int i = 0; i < HistoryDealsTotal(); i++)
   {
      ulong ticket = HistoryDealGetTicket(i);
      if(ticket == 0) continue;
      if(HistoryDealGetInteger(ticket, DEAL_MAGIC) != InpMagicNumber) continue;

      long entry = HistoryDealGetInteger(ticket, DEAL_ENTRY);
      if(entry != DEAL_ENTRY_OUT) continue;

      double profit = HistoryDealGetDouble(ticket, DEAL_PROFIT)
                    + HistoryDealGetDouble(ticket, DEAL_SWAP)
                    + HistoryDealGetDouble(ticket, DEAL_COMMISSION);

      g_dailyPnL += profit;
      if(profit < 0) g_dailyLoss += profit;
   }
}

void ResetDailyCounters()
{
   g_dailyPnL   = 0;
   g_dailyLoss  = 0;
   g_killSwitch = false;
   MqlDateTime dtReset;
   TimeToStruct(TimeCurrent(), dtReset);
   g_todayDate  = dtReset.day;
   Print(g_logPrefix, "Daily counters reset");
}

//+------------------------------------------------------------------+
//| JSON PARSING UTILITIES                                           |
//+------------------------------------------------------------------+
string ExtractString(string &json, string key)
{
   int pos = StringFind(json, key);
   if(pos < 0) return "";
   pos += StringLen(key);
   while(pos < StringLen(json) && (StringGetCharacter(json, pos) == ' '
         || StringGetCharacter(json, pos) == '"'))
      pos++;
   int endPos = StringFind(json, "\"", pos);
   if(endPos < 0) endPos = StringLen(json);
   return StringSubstr(json, pos, endPos - pos);
}

int ExtractInt(string &json, string key)
{
   int pos = StringFind(json, key);
   if(pos < 0) return 0;
   pos += StringLen(key);
   while(pos < StringLen(json) && StringGetCharacter(json, pos) == ' ')
      pos++;
   string numStr = "";
   while(pos < StringLen(json))
   {
      ushort ch = StringGetCharacter(json, pos);
      if((ch >= '0' && ch <= '9') || ch == '-')
         numStr += ShortToString(ch);
      else break;
      pos++;
   }
   return (int)StringToInteger(numStr);
}

double ExtractDouble(string &json, string key)
{
   int pos = StringFind(json, key);
   if(pos < 0) return 0;
   pos += StringLen(key);
   while(pos < StringLen(json) && StringGetCharacter(json, pos) == ' ')
      pos++;
   if(StringSubstr(json, pos, 4) == "null") return 0;
   string numStr = "";
   while(pos < StringLen(json))
   {
      ushort ch = StringGetCharacter(json, pos);
      if((ch >= '0' && ch <= '9') || ch == '.' || ch == '-')
         numStr += ShortToString(ch);
      else break;
      pos++;
   }
   return StringToDouble(numStr);
}

string ExtractArray(string &json, string key)
{
   int pos = StringFind(json, key);
   if(pos < 0) return "";
   pos += StringLen(key);
   while(pos < StringLen(json) && StringGetCharacter(json, pos) == ' ')
      pos++;
   if(StringGetCharacter(json, pos) != '[') return "";
   int depth = 1;
   int start = pos + 1;
   pos++;
   while(pos < StringLen(json) && depth > 0)
   {
      ushort ch = StringGetCharacter(json, pos);
      if(ch == '[') depth++;
      if(ch == ']') depth--;
      pos++;
   }
   return StringSubstr(json, start, pos - start - 1);
}

int SplitJsonArray(string &arrayContent, string &items[])
{
   int count = 0;
   int pos = 0;
   int len = StringLen(arrayContent);
   while(pos < len)
   {
      int start = StringFind(arrayContent, "{", pos);
      if(start < 0) break;
      int depth = 1;
      int end = start + 1;
      while(end < len && depth > 0)
      {
         ushort ch = StringGetCharacter(arrayContent, end);
         if(ch == '{') depth++;
         if(ch == '}') depth--;
         end++;
      }
      string item = StringSubstr(arrayContent, start, end - start);
      ArrayResize(items, count + 1);
      items[count] = item;
      count++;
      pos = end;
   }
   return count;
}
//+------------------------------------------------------------------+
```

