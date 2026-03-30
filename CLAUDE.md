# CLAUDE.md — OniQuant v7.1 System Reference

## What This Is

OniQuant is a **signal generator** — it generates trading signals, scores them through quantitative filters, and outputs approved signals as Telegram alerts. There is no execution layer; no broker connections; no real trades. Output is Telegram messages + shadow simulation only.

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

### Signal Engine Flow (Python-native path)

```
OHLCV Fetch (TwelveData/Bybit)
  → Market Hours Filter (skip closed markets — saves API credits)
  → Indicator Computation (ta library: EMA, RSI, ADX, MACD, SuperTrend, etc.)
  → SMC Analysis (BOS, CHoCH, FVG, Order Blocks)
  → HMM Regime Detection (TRENDING_UP / TRENDING_DOWN / RANGING)
  → MTF Confluence Scoring (weighted [-1,+1] across 5M/15M/1H/4H/D)
  → Signal Quality Scorer (composite 0-100 gate)
  → Redis Stream → Worker → Pipeline → Telegram
```

### Pipeline Flow (downstream, both TV and Python signals)

1. Signal dedup (DB query, 15-min window)
2. TwelveData enrichment (technicals, intermarket via FRED + TwelveData)
3. HMM regime detection (gates DESK2/3 in RANGING, adjusts sizing)
4. Live price fetch (replaces TV candle close)
5. ATR-based SL/TP (regime-aware: tighter stops in RANGING)
6. VIX regime check (hard halt on DESK5/6 at VIX >= 35, graduated sizing)
7. Per-asset Hurst chop filter
8. Intermarket filters (VIX graduated sizing, DXY consensus adjustment)
9. ML scoring (CatBoost/XGBoost)
10. Consensus scoring
11. Pre-screen gate (consensus < 2 skips Claude API)
12. Claude CTO decision (EXECUTE / REDUCE / SKIP)
13. Meta-labeler filter (CatBoost P(correct) >= 0.55, 22 features)
14. Volatility targeting (EWMA/GARCH per-desk, [0.25x, 2.0x])
15. Portfolio risk caps (12 max positions, 10% max risk)
16. Pending entry engine (parks overextended signals for pullback)
17. Trade record creation (SIM_OPEN) + Telegram notification

---

## The 6 Desks

| Desk | Style | Symbols | Entry TF | Risk % | Market Hours (UTC) |
|------|-------|---------|----------|--------|--------------------|
| DESK1_SCALPER | FX Scalper | EURUSD, USDJPY, GBPUSD, USDCHF, AUDUSD | 1M | 0.50% | 07:00-16:00 |
| DESK2_INTRADAY | FX Intraday | EURUSD, GBPUSD, USDJPY, AUDUSD, USDCAD, NZDUSD, EURGBP, GBPCAD | 15M | 0.75% | 07:00-20:00 |
| DESK3_SWING | Swing Trader | 13 FX pairs (majors + crosses) | 4H | 1.00% | Unrestricted |
| DESK4_GOLD | Gold Specialist | XAUUSD, XAGUSD, WTIUSD | Multi (5M/15M/1H/4H) | 0.60% | 13:00-17:00 |
| DESK5_ALTS | Momentum | NAS100, US30, BTCUSD, ETHUSD, SOLUSD, XRPUSD, LINKUSD, WTIUSD | 1H | 0.75% | 07:00-20:00 (crypto 24/7) |
| DESK6_EQUITIES | Stock Trend | NVDA, AAPL, TSLA, MSFT, AMZN, META, GOOGL, NFLX, AMD | 1H | 0.75% | 15:00-20:00 |

**Total unique symbols: ~40**. Gold gets 1.2x confidence boost Tue-Thu. Crypto exempt from all day/weekend filters.

---

## Environment Variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `DATABASE_URL` | `postgresql://...localhost` | PostgreSQL connection |
| `REDIS_URL` | `redis://localhost:6379/0` | Redis (Streams + regime cache) |
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
| `MTF_SCORING` | `WEIGHTED` | `WEIGHTED` (continuous ±1) or `LEGACY` (binary 0-10) |
| `QUALITY_SCORE_THRESHOLD` | `65` | Signal quality gate (0-100, >= threshold to emit) |
| `SHADOW_MODE` | `COLLECT` | `COLLECT`, `ML_GATE`, or `DISABLED` |
| `SHADOW_TRADE_ALL` | `true` | Shadow sim all signals |
| `WORKER_ID` | `worker-{pid}` | Redis consumer name |
| `DEDUP_WINDOW_MINUTES` | `15` | Pipeline-level dedup window |
| `ENGINE_DAILY_CREDITS` | `500` | TwelveData budget for signal engine |
| `BYBIT_WS_ENABLED` | `true` | Bybit WebSocket (not currently used) |

---

## Python Signal Engine (`app/services/signal_engine/`)

| Module | Purpose |
|--------|---------|
| `engine.py` | Main orchestrator — polling loops per timeframe, market hours pre-filter |
| `candle_manager.py` | OHLCV fetch (TwelveData + Bybit), DB storage, in-memory DataFrame cache |
| `indicator_calculator.py` | `ta` library: EMA 21/50/200, SuperTrend, RSI, StochRSI, ADX, MACD, BB, KC, WaveTrend, ATR, RVOL |
| `smc_analyzer.py` | Smart Money Concepts: BOS, CHoCH, FVG, Order Blocks, Swing Points, Liquidity Sweeps |
| `mtf_confluence.py` | **Weighted continuous scoring** [-1,+1] across 5 timeframes. Threshold ±0.30 |
| `confluence_scorer.py` | Legacy binary scorer (0-10 scale, 6.5 threshold). Used when `MTF_SCORING=LEGACY` |
| `regime_detector.py` | **3-state HMM** (TRENDING_UP/DOWN, RANGING) using Garman-Klass vol + momentum features |
| `quality_scorer.py` | **Composite 0-100 gate**: MTF(25) + Regime(20) + S/R(15) + Candle(10) + ML(20) + Volume(10) |
| `feature_engineer.py` | 19 engineered features: volatility (GK, Parkinson, ATR ratio), microstructure, momentum, calendar |
| `signal_generator.py` | Orchestrates scoring → quality gate → payload. Regime-aware SL/direction filtering |
| `market_hours_filter.py` | Per-desk UTC windows + day-of-week rules. Runs BEFORE API calls to save credits |
| `wickless_detector.py` | Marubozu candle detection for confluence bonus |
| `dedup_filter.py` | Redis SET dedup (shared key space with webhook route) |
| `rate_limiter.py` | TwelveData credit tracking (500/day engine budget, 50/min) |

### MTF Confluence Scoring (WEIGHTED mode)

Per-timeframe directional score [-1,+1] from:
- EMA_8 vs EMA_21 crossover (weight 0.40)
- RSI position relative to 50 (weight 0.30)
- Price vs EMA_50 (weight 0.30)

Timeframe weights: 5M=0.10, 15M=0.15, 1H=0.25, 4H=0.25, D=0.20. ADX(14) >= 20 required on 1H.

### Signal Quality Scoring (0-100)

| Component | Max | Logic |
|-----------|-----|-------|
| MTF Confluence | 25 | abs(score) mapped [0.3,1.0]→[0,25] |
| Regime Suitability | 20 | Trend+momentum=20, mismatch=5 |
| S/R Proximity | 15 | At FVG+OB=15, BOS=10, nothing=8 |
| Candle Quality | 10 | body_ratio >0.7=10, doji=2 |
| CatBoost Probability | 20 | proba mapped [0.5,1.0]→[0,20] |
| Volume Confirmation | 10 | RVOL >1.5x=10, below avg=3 |

Emit at >= 75 (full size) or >= 65 (0.5x). Configurable via `QUALITY_SCORE_THRESHOLD`.

### HMM Regime Detection

3-state Gaussian HMM on 1H data (252 bars). Features: log returns, Garman-Klass volatility, 20-bar momentum.

| Regime | Size Mult | SL ATR | Signal Type |
|--------|-----------|--------|-------------|
| TRENDING_UP | 1.2x | 2.5x | Momentum, favor LONG |
| TRENDING_DOWN | 1.2x | 2.5x | Momentum, favor SHORT |
| RANGING | 0.7x | 2.0x (tighter) | Mean-reversion |

Cached in Redis (`regime:{symbol}`, 4h TTL). Retrained every 4 hours.

---

## 3-Tier Partial Exit System (Virtual Broker)

| Tier | Size | Target | After Hit |
|------|------|--------|-----------|
| Tier 1 | 33% | 1R (risk distance) | SL → breakeven |
| Tier 2 | 33% | 3R | SL → 1R profit level |
| Tier 3 | 34% | Chandelier trail | highest_high(22) - ATR(14)*3 for longs |

Zombie trade exits (close if < 0.5R after time limit): DESK1=50min, DESK2=4h, DESK3=3d, DESK4=2h, DESK5=6h, DESK6=1d.

---

## Quant Stack Services

| Service | Purpose | Schedule |
|---------|---------|----------|
| `regime_detector.py` (signal_engine) | 3-state HMM per symbol | Every 4 hours |
| `regime_detector.py` (services) | Pipeline-level regime gate | On-demand |
| `volatility_targeter.py` | Per-desk vol estimation, sizing [0.25x, 2.0x] | On-demand |
| `meta_labeler.py` | CatBoost P(correct), 22 features, TimeSeriesSplit CV | Weekly Sun 00:10 UTC |
| `har_rv.py` | HAR realized volatility forecast | On-demand |
| `alpha_orthogonalizer.py` | Gram-Schmidt + Ridge optimal weights | On-demand |
| `backtest_validator.py` | CSCV (PBO) + Deflated Sharpe Ratio | On-demand via API |
| `factor_monitor.py` | Cross-desk factor concentration alerts | Hourly |
| `fred_service.py` | Daily VIX + DXY from FRED API, Redis 4h cache | Daily 14:00 UTC |
| `feature_engineer.py` | 19 engineered features for CatBoost training | On-demand |

### Meta-Labeler Features (22 total)

Core (10): consensus_score, ml_score, hurst, rsi, adx, atr_pct, rvol, hour_utc, day_of_week, vix_level
Enhanced (12): garman_klass_vol, parkinson_vol, atr_ratio, close_location_value, candle_body_ratio, upper_wick_ratio, lower_wick_ratio, zscore_20, zscore_50, roc_5, roc_20, mtf_confluence_score

CatBoost config: depth=6, lr=0.03, iterations=1000, early_stopping=50, auto_class_weights=Balanced, scale_pos_weight from class ratio, TimeSeriesSplit(5) CV.

---

## Scheduled Jobs (APScheduler)

| Job | Trigger | Purpose |
|-----|---------|---------|
| Daily PnL Digest | Cron 17:00 America/Toronto | v7 digest with quality scores, regimes, exit tiers |
| Pending Entry Check | Interval 30s | Re-evaluate parked signals |
| Triple-Barrier Labeler | Interval 30min | Label shadow signals with outcomes |
| Equity Snapshot | Interval 15min | Sim equity curve data points |
| Sim Exit Checker | Interval 60s | 3-tier partial exits + chandelier trailing |
| HMM Regime Train | Interval 4h | Retrain HMM on all symbols (1H data) |
| Meta-Labeler Train | Cron Sun 00:10 UTC | Retrain CatBoost meta-model |
| Factor Monitor | Interval 60min | Check factor concentration |
| FRED Daily | Cron 14:00 UTC | Fetch VIX + DXY |

---

## Market Hours Filter

Runs BEFORE TwelveData API calls in the signal engine to save credits.

| Desk | UTC Window | Day Filter |
|------|-----------|------------|
| DESK1_SCALPER | 07:00-16:00 | Mon-Fri |
| DESK2_INTRADAY | 07:00-20:00 | Mon-Fri |
| DESK3_SWING | Unrestricted | Sun 22:00+ → Fri 20:00 |
| DESK4_GOLD | 13:00-17:00 | Mon-Fri, 1.2x boost Tue-Thu |
| DESK5_ALTS | 07:00-20:00 (crypto 24/7) | Crypto exempt |
| DESK6_EQUITIES | 15:00-20:00 | Mon-Fri |

Also filters sim exit checker (no price fetches for closed markets).

---

## Data Providers

| Provider | Used For | Auth | Budget |
|----------|----------|------|--------|
| TwelveData | FX, commodities, indices, equities OHLCV + enrichment | API key | 800 credits/day, 55/min |
| Bybit | Crypto spot prices + OHLCV (BTC, ETH, SOL, XRP, LINK) | None (public v5 API) | Unlimited |
| FRED | Daily VIX (VIXCLS) + DXY (DTWEXBGS) | API key | Unlimited |
| Anthropic | Claude CTO decision engine | API key | Per-token billing |

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

### Simulation Tables
| Table | Model | Purpose |
|-------|-------|---------|
| `sim_profiles` | `SimProfile` | Virtual broker profiles (SRV_100, SRV_30, MT5_1M) |
| `sim_orders` | `SimOrder` | Pending/filled sim orders |
| `sim_positions` | `SimPosition` | Open/closed sim positions with 3-tier exit tracking |
| `sim_equity_snapshots` | `SimEquitySnapshot` | 15-min equity snapshots |
| `spread_reference` | `SpreadReference` | Stored spread data per symbol |

### OHLCV Tables (raw SQL)
`ohlcv_1m`, `ohlcv_5m`, `ohlcv_15m`, `ohlcv_1h`, `ohlcv_4h`, `ohlcv_1d`, `ohlcv_1w` — all `PRIMARY KEY (time, symbol)`.

### Auto-Migrated Columns (34 ALTER TABLE statements on startup)

**shadow_signals** (11 columns): hmm_regime, hmm_confidence, hmm_state_probs, meta_probability, meta_should_trade, meta_bet_size, vol_multiplier, realized_vol, vol_method, har_rv_forecast, har_rv_r_squared

**ml_trade_logs** (17 columns): hmm_regime, regime_label, meta_probability, vol_multiplier, har_rv_used, orthogonal_weights, hurst_exponent, quality_score, mtf_confluence_score, vol_regime, garman_klass_vol, bars_since_regime_change, exit_tier, time_based_exit, partial_pnl_tier1/2/3

**sim_positions** (5 columns): exit_tier, partial_pnl_tier1/2/3, time_based_exit

**signals** (1 column): regime_label

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
| `app/main.py` | FastAPI app, lifespan (startup/shutdown/auto-migration), scheduler, signal engine launch |
| `app/worker.py` | Redis Stream consumer, signal processing, v7 daily digest |
| `app/services/pipeline.py` | Full signal evaluation pipeline (the core logic) |
| `app/services/claude_cto.py` | Claude API integration + rule-based fallback |
| `app/config.py` | All desk definitions, symbol mappings, risk parameters, feature flags |
| `app/services/price_service.py` | On-demand price fetch (TwelveData + Bybit) |
| `app/services/ohlcv_ingester.py` | Historical OHLCV data (TwelveData + Bybit) |
| `app/services/twelvedata_enricher.py` | Market data enrichment (technicals, Hurst, intermarket via FRED) |
| `app/services/risk_filter.py` | Hard risk checks (session, correlation, portfolio caps) |
| `app/services/virtual_broker.py` | Shadow sim with 3-tier partial exits + chandelier trailing |
| `app/services/telegram_bot.py` | Telegram notification formatting and sending |
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

## Dependencies

```
fastapi==0.115.0    uvicorn[standard]==0.30.6   sqlalchemy==2.0.35
psycopg2-binary==2.9.9   pydantic==2.9.2   httpx==0.27.2
numpy==1.26.4   pandas==2.2.3   scikit-learn==1.5.2
catboost==1.2.7   xgboost==2.1.3   ta==0.11.0
hmmlearn>=0.3.3   arch>=7.0   statsmodels>=0.14.0
talipp>=2.7.0   ruptures>=1.1.10   quantstats>=0.0.62   river>=0.23.0
redis[hiredis]==5.2.1   orjson==3.10.12   apscheduler==3.11.2
uvloop==0.21.0   websockets==13.1   smartmoneyconcepts>=0.0.8
```

Key constraint: `numpy==1.26.4` (catboost requires <2.0). All packages verified compatible.

---

## Migrations

Run automatically on startup via `main.py` lifespan. The `migrations/` directory contains reference SQL only.

| File | Content |
|------|---------|
| `001_novaquant_schema_alignment.sql` | Initial schema alignment |
| `002_alpha_metadata_schema.sql` | Hurst, RVOL, VWAP columns |
| `003_shadow_sim_engine.sql` | Shadow sim tables |
| `004_pending_entry_engine.sql` | pending_signals table |
| `005_signal_engine_candles.sql` | OHLCV tables (5m through 1w) |
| `006_quant_stack_v7.sql` | HMM, meta-label, vol target, HAR-RV columns |
