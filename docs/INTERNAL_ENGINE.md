# Internal Signal Engine

OniQuant no longer depends on TradingView alerts as its primary signal source. The default signal source is:

```env
SIGNAL_SOURCE=INTERNAL_ENGINE
ENABLE_TRADINGVIEW_WEBHOOK=false
LIVE_TRADING_ENABLED=false
```

TradingView webhook code remains in the repo for backward compatibility, but it is optional and OFF by default. Missing `WEBHOOK_SECRET` must not prevent startup when TradingView is disabled.

## Data provider flow

```text
Live / historical data provider
→ Candle Manager
→ Desk Scanner
→ Strategy Stacks
→ Cross-Desk Bias
→ Pipeline v2
→ VirtualBroker paper simulation
→ PostgreSQL logs
→ Telegram desk/portfolio/system channels
```

Provider modules live under `app/services/data_providers/`:

- `base.py` defines the `MarketDataProvider` interface and provider factory.
- `twelvedata_provider.py` fetches TwelveData candles/quotes when `TWELVEDATA_API_KEY` is present.
- `polygon_provider.py` is a safe stub that degrades to mock-compatible candles until full mappings are implemented.
- `mock_provider.py` generates deterministic synthetic candles for local development, CI, and API-key-free operation.

If `DATA_PROVIDER=TWELVEDATA` but `TWELVEDATA_API_KEY` is empty, the factory uses `MockProvider` automatically.

## Scanner worker flow

Run the scanner as a separate process:

```bash
python -m app.workers.scanner_worker
```

The worker:

1. Reads desks and symbols from `app.config.DESKS`.
2. Resolves each desk entry timeframe.
3. Fetches provider candles.
4. Loads candles into `CandleManager.update_dataframe(symbol, timeframe, candles)`.
5. Calls `DeskScanner.scan_desk(desk_id)`.
6. Converts candidates with `DeskScanner.build_signal_payload(candidate)`.
7. Publishes worker-compatible JSON to Redis stream `oniquant_alerts`.
8. Deduplicates by `symbol + desk_id + direction + strategy_id + timeframe` for 15 minutes by default.

The worker logs every cycle with scanned symbols, emitted candidates, skipped data, and errors.

## Redis stream population

Internal payloads are published to:

```text
oniquant_alerts
```

via:

```python
await publish_signal(redis, payload)
```

The existing `app.worker` consumer reads the same stream, creates `Signal` rows, runs `PipelineV2`, paper-simulates via `VirtualBroker`, and sends/logs Telegram notifications safely.

## Desk behavior

- `DESK1_SCALPER`: FX scalper, entry timeframe `1M`.
- `DESK2_INTRADAY`: FX intraday, entry timeframe `15M`.
- `DESK3_SWING`: FX swing, entry timeframe `4H`; sets higher-timeframe cross-desk bias.
- `DESK4_GOLD`: `XAUUSD` only; emits `GOLD_SCALP`, `GOLD_INTRADAY`, and `GOLD_SWING` metadata.
- `DESK5_ALTS`: crypto/indices from config.
- `DESK6_EQUITIES`: equities from config.

Internal payloads include `desk_mode`, `desk_role`, `strategy_mode`, `mode_reason`, `quality_hints`, `cross_desk_bias`, `bias_alignment`, `bias_action`, and `bias_size_mult`. If no bias exists, payloads pass as neutral.

## Required environment variables

```env
SIGNAL_SOURCE=INTERNAL_ENGINE
ENABLE_TRADINGVIEW_WEBHOOK=false
WEBHOOK_SECRET=

LIVE_DATA_ENABLED=true
DATA_PROVIDER=TWELVEDATA
TWELVEDATA_API_KEY=
POLYGON_API_KEY=

WORKER_SCANNER_ENABLED=true
WORKER_PIPELINE_ENABLED=true
WORKER_EXECUTION_ENABLED=true
INTERNAL_SCANNER_INTERVAL_SECONDS=60
INTERNAL_SCANNER_MAX_SYMBOLS_PER_CYCLE=50

LIVE_TRADING_ENABLED=false
```

## Local run

```bash
python -m app.scripts.smoke_internal_engine
python -m app.workers.scanner_worker
python -m app.worker
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

For local development without Redis, use the smoke script. For full flow, run Redis and Postgres.

## Railway service layout

Recommended services:

### Service 1: web

```bash
uvicorn app.main:app --host 0.0.0.0 --port $PORT
```

### Service 2: worker

```bash
python -m app.worker
```

### Service 3: scanner

```bash
python -m app.workers.scanner_worker
```

Add Railway Postgres and Redis plugins. Railway variables can be pasted through the Variables RAW editor from `.env`-style content. Railway exposes variables to both build and runtime.

## Safety

This system remains PAPER / SHADOW simulation only by default. Do not enable live broker execution unless a separate live-trading implementation and approval workflow are added.
