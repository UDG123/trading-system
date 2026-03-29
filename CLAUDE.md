# CLAUDE.md — OniQuant v7.0 System Reference

## What This Is

OniQuant is a **signal generator** — it generates trading signals, scores them through multiple quantitative filters, runs them past a Claude CTO decision layer, and outputs approved signals as Telegram alerts. There is no execution layer; no broker connections; no real trades. Output is Telegram messages + shadow simulation only.

Deployed on **Railway** as two services: a FastAPI web process and a Redis Stream worker process.

---

## Architecture

```
Signal Sources                Pipeline                          Output
─────────────────    ──────────────────────────────    ──────────────────
TradingView Alert ─┐                                   ┌─ Telegram Alert
                   ├→ Redis Stream ─→ Worker ─→ Pipeline ─→ Shadow Sim
Python Engine ─────┘   (oniquant_alerts)                └─ ML Training Log
```

### Data Flow

1. **Signal source** pushes JSON to Redis Stream `oniquant_alerts`
   - TradingView webhooks (`POST /api/webhook/{secret}`)
   - Python signal engine (background task in FastAPI process)
   - Controlled by `SIGNAL_SOURCE` env var: `TV_ONLY | PYTHON_ONLY | BOTH`

2. **Worker** (`app/worker.py`) consumes from stream, creates Signal record, calls pipeline

3. **Pipeline** (`app/services/pipeline.py`) runs the full evaluation chain:
   - Signal dedup (DB query, 15-min window)
   - TwelveData enrichment (technicals, intermarket)
   - HMM regime detection (optional, gates DESK2/3 in mean-reverting regime)
   - Live price fetch (replaces TV candle close)
   - ATR-based SL/TP calculation
   - VIX regime check (hard halt on DESK5/6 at VIX >= 35)
   - Per-asset Hurst chop filter
   - Intermarket filters (VIX graduated sizing, DXY consensus adjustment)
   - ML scoring (CatBoost/XGBoost)
   - Consensus scoring (multi-TF weighted)
   - Pre-screen gate (consensus < 2 skips Claude API)
   - Claude CTO decision (EXECUTE / REDUCE / SKIP)
   - Meta-labeler filter (optional, P(correct) >= 0.55)
   - Volatility targeting (optional, EWMA/GARCH per-desk)
   - Portfolio risk caps (12 max positions, 10% max risk)
   - Pending entry engine (parks overextended signals for pullback)
   - Trade record creation (SIM_OPEN) + Telegram notification

4. **Shadow sim** records every signal for ML training regardless of pipeline outcome

5. **Virtual broker** simulates trade lifecycle (entries, exits, P&L) across 3 profiles

---

## The 6 Desks

| Desk | Style | Symbols | Entry TF | Confirm TF | Bias TF | Risk % | Sessions |
|------|-------|---------|----------|------------|---------|--------|----------|
| DESK1_SCALPER | FX Scalper | EURUSD, USDJPY, GBPUSD, USDCHF, AUDUSD | 1M | 5M | 1H | 0.50% | London Open, NY Open |
| DESK2_INTRADAY | FX Intraday | EURUSD, GBPUSD, USDJPY, AUDUSD, USDCAD, NZDUSD, EURGBP, GBPCAD | 15M | 1H | 4H | 0.75% | London, New York |
| DESK3_SWING | Swing Trader | 13 FX pairs (majors + crosses) | 4H | D | W | 1.00% | All sessions |
| DESK4_GOLD | Gold Specialist | XAUUSD, XAGUSD, WTIUSD | Multi (5M/15M/1H/4H) | 4H/1H | D | 0.60% | London, NY, Overlap |
| DESK5_ALTS | Momentum | NAS100, US30, BTCUSD, ETHUSD, SOLUSD, XRPUSD, LINKUSD, WTIUSD | 1H | 4H | D | 0.75% | US Market, Weekday Crypto |
| DESK6_EQUITIES | Stock Trend | NVDA, AAPL, TSLA, MSFT, AMZN, META, GOOGL, NFLX, AMD | 1H | 4H | D | 0.75% | US Market (14:00-16:00, 19:00-20:00 UTC) |

**Total unique symbols: ~40** (some overlap across desks, e.g. EURUSD on Desks 1, 2, 3).

---

## Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `DATABASE_URL` | `postgresql://...localhost` | PostgreSQL connection |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis connection (Streams + cache) |
| `WEBHOOK_SECRET` | `OniQuant_X9k7mP2w_2026` | TradingView webhook auth |
| `TELEGRAM_BOT_TOKEN` | `""` | Telegram bot API token |
| `TELEGRAM_CHAT_ID` | `""` | Default Telegram chat |
| `TG_DESK1` through `TG_DESK6` | (hardcoded IDs) | Per-desk Telegram channels |
| `TG_PORTFOLIO` | (hardcoded) | Portfolio summary channel |
| `TG_SYSTEM` | (hardcoded) | System alerts channel |
| `ANTHROPIC_API_KEY` | `""` | Claude CTO decision engine |
| `TWELVEDATA_API_KEY` | `""` | Market data (800 credits/day Grow plan) |
| `FRED_API_KEY` | `""` | Federal Reserve data (VIX, DXY daily) |
| `SIGNAL_SOURCE` | `BOTH` | `TV_ONLY`, `PYTHON_ONLY`, or `BOTH` |
| `SHADOW_MODE` | `COLLECT` | `COLLECT`, `ML_GATE`, or `DISABLED` |
| `SHADOW_TRADE_ALL` | `true` | Shadow sim all signals |
| `WORKER_ID` | `worker-{pid}` | Redis consumer name |
| `DEDUP_WINDOW_MINUTES` | `15` | Pipeline-level dedup window |
| `ENGINE_DAILY_CREDITS` | `500` | TwelveData budget for signal engine |
| `BYBIT_WS_ENABLED` | `true` | Bybit WebSocket (not currently used) |

---

## Database Tables

### Core Tables (SQLAlchemy ORM)
| Table | Model | Purpose |
|-------|-------|---------|
| `signals` | `Signal` | Every incoming alert — audit trail |
| `trades` | `Trade` | Pipeline-approved trades (SIM_OPEN) |
| `desk_states` | `DeskState` | Runtime state per desk |
| `ml_trade_logs` | `MLTradeLog` | ML training data (features + outcomes) |
| `shadow_signals` | `ShadowSignal` | Every signal logged with full context for ML |
| `pending_signals` | `PendingSignal` | Parked signals awaiting pullback entry |

### Simulation Tables (SQLAlchemy ORM)
| Table | Model | Purpose |
|-------|-------|---------|
| `sim_profiles` | `SimProfile` | Virtual broker profiles (SRV_100, SRV_30, MT5_1M) |
| `sim_orders` | `SimOrder` | Pending/filled sim orders |
| `sim_positions` | `SimPosition` | Open/closed sim positions |
| `sim_equity_snapshots` | `SimEquitySnapshot` | 15-min equity snapshots |
| `spread_reference` | `SpreadReference` | Stored spread data per symbol |

### OHLCV Tables (raw SQL, not ORM)
| Table | Created In | Purpose |
|-------|------------|---------|
| `ohlcv_1m` | `ohlcv_ingester.py` | 1-minute candles (existing) |
| `ohlcv_5m` | `main.py` lifespan | 5-minute candles (signal engine) |
| `ohlcv_15m` | `main.py` lifespan | 15-minute candles |
| `ohlcv_1h` | `main.py` lifespan | 1-hour candles |
| `ohlcv_4h` | `main.py` lifespan | 4-hour candles |
| `ohlcv_1d` | `main.py` lifespan | Daily candles |
| `ohlcv_1w` | `main.py` lifespan | Weekly candles |

All OHLCV tables share the schema: `(time TIMESTAMPTZ, symbol VARCHAR(20), open, high, low, close, volume DOUBLE PRECISION)` with `PRIMARY KEY (time, symbol)`.

### v7 Columns Added to Existing Tables

These columns are added via `ALTER TABLE ... ADD COLUMN IF NOT EXISTS` in `main.py` lifespan (runs every deploy):

**shadow_signals** — 11 new columns:
- `hmm_regime`, `hmm_confidence`, `hmm_state_probs` (JSONB)
- `meta_probability`, `meta_should_trade`, `meta_bet_size`
- `vol_multiplier`, `realized_vol`, `vol_method`
- `har_rv_forecast`, `har_rv_r_squared`

**ml_trade_logs** — 5 new columns:
- `hmm_regime`, `meta_probability`, `vol_multiplier`
- `har_rv_used` (boolean), `orthogonal_weights` (JSONB)

---

## Python Signal Engine (`app/services/signal_engine/`)

Generates signals internally, replacing TradingView dependency. Runs as a background `asyncio.create_task` in the FastAPI process.

| Module | Purpose |
|--------|---------|
| `engine.py` | Main orchestrator — polling loops per timeframe |
| `candle_manager.py` | OHLCV fetch from TwelveData, DB storage, in-memory DataFrame cache |
| `indicator_calculator.py` | `ta` library: EMA 21/50/200, SuperTrend, RSI, StochRSI, ADX, MACD, BB, KC, WaveTrend, ATR, RVOL |
| `smc_analyzer.py` | Smart Money Concepts: BOS, CHoCH, FVG, Order Blocks, Swing Points, Liquidity Sweeps |
| `confluence_scorer.py` | Multi-TF weighted score: entry 30%, confirm 35%, bias 25%, structure 10%. Threshold: 6.5/10 |
| `signal_generator.py` | Maps conditions to alert types, SL/TP from ATR, session/weekend/cooldown/R:R filters |
| `wickless_detector.py` | Marubozu candle detection for confluence bonus |
| `dedup_filter.py` | Redis SET dedup (shared key space with webhook route) |
| `rate_limiter.py` | TwelveData credit tracking (500/day engine budget, 50/min) |

### Polling Schedule
| Timeframe | Interval | Symbols |
|-----------|----------|---------|
| 1M | 65s | DESK1 scalper symbols (5) |
| 5M | 305s | DESK1 + DESK4 scalp (6) |
| 15M | 905s | DESK2 + DESK4 intra (10) |
| 1H | 305s | All 40 symbols |
| 4H | 905s | DESK3 + DESK5 + DESK6 (25) |
| D | 3600s | All 40 symbols |
| W | 14400s | DESK3 symbols (13) |

---

## Quant Stack (v7)

| Service | Purpose | Schedule |
|---------|---------|----------|
| `regime_detector.py` | 3-state Gaussian HMM (TRENDING / MEAN_REVERTING / VOLATILE) | Daily 00:05 UTC |
| `volatility_targeter.py` | Per-desk vol estimation (EWMA / GARCH / GJR-GARCH / rolling), sizing [0.25x, 2.0x] | On-demand per signal |
| `meta_labeler.py` | CatBoost P(correct\|signal), threshold 0.55, Kelly-inspired sizing | Weekly Sun 00:10 UTC |
| `har_rv.py` | HAR realized volatility forecast for forward-looking SL/TP | On-demand per signal |
| `alpha_orthogonalizer.py` | Gram-Schmidt on Hurst/SMC/ML + Ridge optimal weights | On-demand |
| `backtest_validator.py` | CSCV (PBO) + Deflated Sharpe Ratio | On-demand via API |
| `factor_monitor.py` | Cross-desk factor concentration alerts | Hourly |
| `fred_service.py` | Daily VIX + DXY from FRED API, Redis 4h cache | Daily 14:00 UTC |

---

## Scheduled Jobs (APScheduler)

| Job | Trigger | Purpose |
|-----|---------|---------|
| Daily PnL Digest | Cron 17:00 America/Toronto | Telegram daily report |
| Pending Entry Check | Interval 30s | Re-evaluate parked signals |
| Triple-Barrier Labeler | Interval 30min | Label shadow signals with outcomes |
| Equity Snapshot | Interval 15min | Sim equity curve data points |
| Sim Exit Checker | Interval 60s | Check sim positions for SL/TP/trail hits |
| HMM Regime Train | Cron 00:05 UTC | Retrain HMM on all symbols |
| Meta-Labeler Train | Cron Sun 00:10 UTC | Retrain CatBoost meta-model |
| Factor Monitor | Interval 60min | Check factor concentration |
| FRED Daily | Cron 14:00 UTC | Fetch VIX + DXY |

---

## Data Providers

| Provider | Used For | Auth | Budget |
|----------|----------|------|--------|
| TwelveData | FX, commodities, indices, equities OHLCV + enrichment | API key | 800 credits/day, 55/min |
| Bybit | Crypto spot prices + OHLCV (BTC, ETH, SOL, XRP, LINK) | None (public API) | Unlimited |
| FRED | Daily VIX (VIXCLS) + DXY (DTWEXBGS) | API key | Unlimited |
| Anthropic | Claude CTO decision engine | API key | Per-token billing |

---

## API Endpoints (24 total)

| Group | Endpoints |
|-------|-----------|
| Health | `GET /health`, `GET /dashboard` |
| Webhooks | `POST /webhook/{secret}`, `POST /webhook`, `GET /signal/{id}`, `GET /test-signal` |
| Control | `POST /kill-switch`, `POST /desk/{id}/pause`, `POST /desk/{id}/resume`, `GET /desks`, `POST /report/{period}` |
| ML Export | `GET /ml/export`, `POST /ml/enrich`, `GET /ml/stats` |
| Simulation | `GET /sim/profiles`, `GET /sim/positions`, `GET /sim/trades`, `GET /sim/equity`, `GET /sim/metrics`, `POST /sim/reset` |
| Shadow | `GET /shadow/stats`, `GET /shadow/export` |
| Backtest | `POST /backtest`, `GET /backtest/{id}` |
| OHLCV | `POST /ohlcv/ingest`, `GET /ohlcv/stats` |
| Telegram | `POST /telegram` (handles 13+ bot commands) |

---

## Key Files

| File | Purpose |
|------|---------|
| `app/main.py` | FastAPI app, lifespan (startup/shutdown), scheduler, signal engine launch |
| `app/worker.py` | Redis Stream consumer, signal processing, daily digest |
| `app/services/pipeline.py` | Full signal evaluation pipeline (the core logic) |
| `app/services/claude_cto.py` | Claude API integration + rule-based fallback |
| `app/config.py` | All desk definitions, symbol mappings, risk parameters |
| `app/services/price_service.py` | On-demand price fetch (TwelveData + Bybit) |
| `app/services/ohlcv_ingester.py` | Historical OHLCV data (TwelveData + Bybit) |
| `app/services/twelvedata_enricher.py` | Market data enrichment (technicals, Hurst, intermarket) |
| `app/services/risk_filter.py` | Hard risk checks (session, correlation, portfolio caps) |
| `app/services/telegram_bot.py` | Telegram notification formatting and sending |
| `app/services/virtual_broker.py` | Shadow simulation trade execution |
| `app/services/shadow_logger.py` | Logs every signal to shadow_signals for ML |

---

## Running Locally

```bash
# Web process
uvicorn app.main:app --host 0.0.0.0 --port 8080

# Worker process (separate terminal)
python -m app.worker
```

Both require `DATABASE_URL`, `REDIS_URL`, and `TWELVEDATA_API_KEY` at minimum.

---

## Migrations

Run automatically on startup via `main.py` lifespan (`CREATE TABLE IF NOT EXISTS`, `ALTER TABLE ... ADD COLUMN IF NOT EXISTS`). The `migrations/` directory contains reference SQL files but they are not executed separately.

| File | Content |
|------|---------|
| `001_novaquant_schema_alignment.sql` | Initial schema alignment |
| `002_alpha_metadata_schema.sql` | Hurst, RVOL, VWAP columns |
| `003_shadow_sim_engine.sql` | Shadow sim tables |
| `004_pending_entry_engine.sql` | pending_signals table |
| `005_signal_engine_candles.sql` | OHLCV tables (5m through 1w) |
| `006_quant_stack_v7.sql` | HMM, meta-label, vol target, HAR-RV columns |
