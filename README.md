# OniQuant / Lux Prop Engine (Paper-First)

## Run
- API: `uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}`
- Scanner worker: `python -m app.workers.scanner_worker`
- Pipeline worker: `python -m app.workers.pipeline_worker`
- Execution worker: `python -m app.workers.execution_worker`

## Railway
1. Set `DATABASE_URL`, `REDIS_URL`, `PORT`.
2. Keep `LIVE_TRADING_ENABLED=false`.
3. Deploy via Dockerfile/railway.json.

## Redis Streams flow
`oniquant:signals:raw -> validated/rejected -> orders:paper -> orders:filled`

## Safety
Paper mode is default. Live mode requires both `LIVE_TRADING_ENABLED=true` and broker credentials.

## Webhook test
`curl -X POST http://localhost:8000/webhook/tradingview -H 'content-type: application/json' -d '{"symbol":"XAUUSD","timeframe":"5m","side":"buy","entry":2300,"stop_loss":2295,"take_profit":2310}'`

## MCP
Use `/mcp/tools` to list registered tools. Execution tools are disabled by default.
