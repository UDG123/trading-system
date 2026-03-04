# Autonomous Institutional Trading System — Phase 1

## Webhook Receiver & Signal Logger

FastAPI service that receives LuxAlgo TradingView webhook alerts, validates them, routes them to the correct trading desk, and logs everything to PostgreSQL.

---

## Architecture

```
TradingView Alert
       │
       ▼
  FastAPI /api/webhook  (Railway)
       │
       ├─► Authenticate (webhook secret)
       ├─► Parse & validate payload
       ├─► Normalize symbol
       ├─► Route to desk(s)
       ├─► Validate signal (SL/TP sanity, alert type, desk rules)
       ├─► Log to PostgreSQL
       │
       ▼
  Signal stored with status: VALIDATED or REJECTED
```

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/webhook` | Receive TradingView alerts |
| `GET`  | `/api/health` | System health check |
| `GET`  | `/api/dashboard` | Firm-wide desk overview |
| `GET`  | `/docs` | Interactive API docs (Swagger) |

---

## Deploy to Railway

### 1. Create Railway Project

```bash
# Install Railway CLI
npm install -g @railway/cli

# Login
railway login

# Initialize project
cd trading-system
railway init
```

### 2. Add PostgreSQL

```bash
railway add --plugin postgresql
```

Railway automatically sets `DATABASE_URL` for you.

### 3. Set Environment Variables

```bash
railway variables set WEBHOOK_SECRET=your-strong-random-secret
```

### 4. Deploy

```bash
railway up
```

### 5. Get Your Webhook URL

```bash
railway domain
```

Your webhook URL will be: `https://your-app.up.railway.app/api/webhook`

---

## TradingView Alert Setup

### Alert Message JSON

In TradingView, set your LuxAlgo alert's **Message** field to:

```json
{
    "secret": "your-strong-random-secret",
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
```

### Alert Types to Configure

Create separate alerts for each type per symbol/timeframe. Set `alert_type` to one of:

- `bullish_confirmation` / `bearish_confirmation`
- `bullish_plus` / `bearish_plus`
- `bullish_exit` / `bearish_exit`
- `confirmation_turn_bullish` / `confirmation_turn_bearish` / `confirmation_turn_plus`
- `contrarian_bullish` / `contrarian_bearish`
- `take_profit` / `stop_loss`
- `smart_trail_cross`

### Webhook URL

Set the alert's **Webhook URL** to:
```
https://your-app.up.railway.app/api/webhook
```

---

## Local Development

```bash
# Clone and setup
cd trading-system
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# Set environment
cp .env.example .env
# Edit .env with your local PostgreSQL credentials

# Run
uvicorn app.main:app --reload --port 8000

# Test
pytest tests/ -v
```

### Test with curl

```bash
curl -X POST http://localhost:8000/api/webhook \
  -H "Content-Type: application/json" \
  -d '{
    "secret": "your-secret",
    "symbol": "EURUSD",
    "exchange": "FX",
    "timeframe": "5M",
    "alert_type": "bullish_confirmation",
    "price": 1.1050,
    "tp1": 1.1080,
    "tp2": 1.1120,
    "sl1": 1.1020,
    "sl2": 1.1000,
    "smart_trail": 1.1035
  }'
```

---

## Six Trading Desks

| Desk | Name | Symbols | Style |
|------|------|---------|-------|
| 1 | Rapid Execution | EUR/USD, USD/JPY, GBP/USD, USD/CHF | Scalper |
| 2 | Intraday Momentum | AUD/USD, USD/CAD, NZD/USD, EUR/USD | Intraday |
| 3 | Strategic Positioning | EUR/JPY, GBP/JPY, AUD/JPY, USD/CAD | Swing |
| 4 | Commodities | XAU/USD | Gold Trend |
| 5 | Alternative Assets | US30, US100, NAS100, BTC, ETH | Index/Crypto |
| 6 | Equities | TSLA, Mega Cap Basket | Stock Trend |

---

## Phase Roadmap

- [x] **Phase 1** — Webhook receiver, validation, PostgreSQL logging
- [ ] **Phase 2** — TwelveData enrichment, ML scoring, Claude CTO engine
- [ ] **Phase 3** — ZeroMQ bridge, MT5 EA execution
- [ ] **Phase 4** — Telegram bot, kill switch, dashboard
- [ ] **Phase 5** — Weekly retraining, strategy memos, board reports
 
