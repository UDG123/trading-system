# Railway Deployment Guide — Zero Coding Background

This guide walks you through every single click and command to get your trading system live on Railway. You're building on a Mac, so all instructions are Mac-specific.

---

## What You're About to Do

You're going to put your trading system code on GitHub (think of it as cloud storage for code), then connect GitHub to Railway (the server that runs your code 24/7). Railway will also give you a PostgreSQL database and a public URL that TradingView will send alerts to.

Total time: roughly 20–30 minutes.

---

## Part 1: Install the Tools You Need

### 1.1 — Install Homebrew (Mac's package manager)

Open the **Terminal** app. You can find it by pressing **Cmd + Space**, typing "Terminal", and hitting Enter.

Copy and paste this entire line into Terminal and press Enter:

```
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
```

It will ask for your Mac password (the one you use to log in). Type it and press Enter. You won't see the characters as you type — that's normal.

Wait for it to finish. It may take a few minutes.

**Important:** After it finishes, it may show you two commands under "Next steps" that start with `echo` and `eval`. Copy and paste each of those lines into Terminal and press Enter. This makes Homebrew available everywhere.

### 1.2 — Install Git

Still in Terminal, type:

```
brew install git
```

Press Enter and wait for it to finish.

### 1.3 — Install Node.js (needed for Railway CLI)

```
brew install node
```

### 1.4 — Install Railway CLI

```
npm install -g @railway/cli
```

If you get a "permission denied" error, try:

```
sudo npm install -g @railway/cli
```

It will ask for your Mac password again.

---

## Part 2: Create Your GitHub Account and Repository

### 2.1 — Create a GitHub Account

1. Go to [https://github.com](https://github.com) in your browser
2. Click **Sign Up**
3. Follow the steps: enter email, create password, pick a username
4. Verify your email

### 2.2 — Create a New Repository

1. Once logged in, click the **+** icon in the top-right corner
2. Click **New repository**
3. Fill in:
   - **Repository name:** `trading-system`
   - **Description:** `Autonomous institutional trading system`
   - **Visibility:** Select **Private** (important — this is your trading system)
   - Do **NOT** check "Add a README file" (we already have one)
4. Click **Create repository**
5. You'll see a page with setup instructions. **Keep this page open** — you'll need the URL shown (it looks like `https://github.com/YOUR-USERNAME/trading-system.git`)

### 2.3 — Upload Your Code to GitHub

First, download the `trading-system` folder from this conversation (click the folder link above).

Open Terminal and navigate to where you downloaded it. If it's in your Downloads folder:

```
cd ~/Downloads/trading-system
```

Now run these commands one at a time, pressing Enter after each. Replace `YOUR-USERNAME` with your actual GitHub username:

```
git init
```

```
git add .
```

```
git commit -m "Phase 1: webhook receiver and signal logger"
```

```
git branch -M main
```

```
git remote add origin https://github.com/YOUR-USERNAME/trading-system.git
```

```
git push -u origin main
```

It may ask for your GitHub username and password. For the password, you'll need to use a **Personal Access Token** instead of your regular password:

1. Go to [https://github.com/settings/tokens](https://github.com/settings/tokens)
2. Click **Generate new token (classic)**
3. Give it a name like "Railway deployment"
4. Check the **repo** checkbox (full control of private repositories)
5. Click **Generate token**
6. **Copy the token immediately** — you won't see it again
7. Paste it when Terminal asks for your password

After `git push` succeeds, refresh your GitHub repository page. You should see all your files there.

---

## Part 3: Set Up Railway

### 3.1 — Create a Railway Account

1. Go to [https://railway.com](https://railway.com)
2. Click **Login** in the top right
3. Click **Login with GitHub**
4. Authorize Railway to access your GitHub
5. You may need to enter a credit card. Railway has a free tier but requires payment info. The Hobby plan ($5/month) is more than enough for this system

### 3.2 — Create a New Project

1. Once logged into Railway's dashboard, click **New Project**
2. Click **Deploy from GitHub repo**
3. If you don't see your repository, click **Configure GitHub App** and grant Railway access to your `trading-system` repo
4. Select **trading-system** from the list
5. Railway will start building your app. **It will fail** — that's okay, we need to add the database first

### 3.3 — Add PostgreSQL Database

1. Inside your Railway project, click **New** (or the **+** button) in the project canvas
2. Click **Database**
3. Click **Add PostgreSQL**
4. A PostgreSQL box will appear in your project canvas
5. Click on the PostgreSQL box
6. Go to the **Variables** tab
7. You'll see `DATABASE_URL` — Railway generates this automatically

### 3.4 — Connect the Database to Your App

1. Click on your **trading-system** service (the other box in the canvas)
2. Go to the **Variables** tab
3. Click **New Variable**
4. Click the **Add Reference** button (it looks like a small link icon)
5. Select the PostgreSQL service
6. Select `DATABASE_URL`
7. Click **Add**

This tells your app where the database is. The variable will show as `${{Postgres.DATABASE_URL}}`.

### 3.5 — Add Your Webhook Secret

Still in the **Variables** tab of your trading-system service:

1. Click **New Variable**
2. For the variable name, type: `WEBHOOK_SECRET`
3. For the value, create a strong random string. You can generate one by going to [https://randomkeygen.com](https://randomkeygen.com) and copying one of the 256-bit WEP keys, or just make up something long like: `MyTrad1ngSyst3m_S3cret_2025!`
4. Click **Add**

**Write this secret down somewhere safe.** You'll need to put the exact same secret in your TradingView alerts.

### 3.6 — Trigger a Redeploy

1. Go to the **Deployments** tab of your trading-system service
2. Click the three dots on the latest (failed) deployment
3. Click **Redeploy**

Or you can click **Settings** → scroll down to **Deploy** → click **Redeploy**

### 3.7 — Wait for Build to Succeed

Watch the build logs. You should see:

```
AUTONOMOUS TRADING SYSTEM - INITIALIZING
Database tables verified
PostgreSQL connection confirmed
Phase 1 Webhook Receiver ONLINE
```

If you see these lines, your system is live.

### 3.8 — Get Your Public URL

1. Click on your **trading-system** service
2. Go to the **Settings** tab
3. Scroll to **Networking** → **Public Networking**
4. Click **Generate Domain**
5. Railway will give you a URL like: `trading-system-production-xxxx.up.railway.app`

Your webhook endpoint is:
```
https://trading-system-production-xxxx.up.railway.app/api/webhook
```

---

## Part 4: Verify Everything Works

### 4.1 — Check the Health Endpoint

Open your browser and go to:

```
https://YOUR-RAILWAY-URL.up.railway.app/api/health
```

You should see something like:
```json
{
  "status": "operational",
  "database": "connected",
  "uptime_seconds": 123.4,
  "signals_today": 0,
  "version": "1.0.0"
}
```

### 4.2 — Check the API Docs

Go to:
```
https://YOUR-RAILWAY-URL.up.railway.app/docs
```

This shows an interactive page where you can test the webhook manually.

### 4.3 — Send a Test Webhook

Open Terminal on your Mac and run this command. Replace `YOUR-RAILWAY-URL` with your actual Railway URL and `YOUR-SECRET` with the webhook secret you set:

```
curl -X POST https://YOUR-RAILWAY-URL.up.railway.app/api/webhook \
  -H "Content-Type: application/json" \
  -d '{
    "secret": "YOUR-SECRET",
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

You should get back:
```json
{
  "status": "accepted",
  "signal_id": 1,
  "symbol": "EURUSD",
  "alert_type": "bullish_confirmation",
  "desks_matched": ["DESK1_SCALPER", "DESK2_INTRADAY"],
  "is_valid": true,
  "message": "Signal logged and routed to 2 desk(s)"
}
```

If you see this, your system is live, validating signals, routing them to the correct desks, and logging everything to the database.

### 4.4 — Check the Dashboard

Go to:
```
https://YOUR-RAILWAY-URL.up.railway.app/api/dashboard
```

You'll see all six desks listed with their status.

---

## Part 5: Connect TradingView

### 5.1 — Create a TradingView Alert

1. Open TradingView and go to a chart with LuxAlgo Signals & Overlays loaded
2. Click the **Alert** button (clock icon with a +)
3. Set your alert condition to the LuxAlgo signal you want (e.g., "Bullish Confirmation")
4. In the **Notifications** section, check **Webhook URL**
5. Paste your webhook URL:
   ```
   https://YOUR-RAILWAY-URL.up.railway.app/api/webhook
   ```
6. In the **Message** field, paste this JSON (change the `alert_type` to match whichever signal you're creating the alert for):

```json
{
    "secret": "YOUR-WEBHOOK-SECRET",
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

7. Click **Create**

### 5.2 — Alert Types Reference

You need a separate alert for each signal type. Here are the `alert_type` values to use:

| Signal Type | alert_type Value |
|---|---|
| Bullish Confirmation | `bullish_confirmation` |
| Bearish Confirmation | `bearish_confirmation` |
| Bullish+ | `bullish_plus` |
| Bearish+ | `bearish_plus` |
| Bullish Exit | `bullish_exit` |
| Bearish Exit | `bearish_exit` |
| Confirmation Turn+ | `confirmation_turn_plus` |
| Contrarian Bullish | `contrarian_bullish` |
| Contrarian Bearish | `contrarian_bearish` |
| Take Profit | `take_profit` |
| Stop Loss | `stop_loss` |
| Smart Trail Cross | `smart_trail_cross` |

### 5.3 — Alerts You Need Per Desk

**Desk 1 (Scalper)** — EUR/USD, USD/JPY, GBP/USD, USD/CHF on 1M, 5M, 15M:
Create alerts for: bullish_confirmation, bearish_confirmation, bullish_exit, bearish_exit, take_profit, stop_loss

**Desk 2 (Intraday)** — AUD/USD, USD/CAD, NZD/USD, EUR/USD on 15M, 1H, 4H:
Create alerts for: all confirmations, bullish_plus, bearish_plus, exits, take_profit, stop_loss, confirmation_turn_plus

**Desk 3 (Swing)** — EUR/JPY, GBP/JPY, AUD/JPY, USD/CAD on 4H, Daily, Weekly:
Create alerts for: bullish_plus, bearish_plus, confirmation_turn_plus, take_profit, stop_loss

**Desk 4 (Gold)** — XAU/USD on 15M, 1H, 4H, Daily:
Create alerts for: all confirmations, bullish_plus, bearish_plus, contrarian signals, take_profit, stop_loss

**Desk 5 (Alts)** — US30, US100, NAS100, BTCUSD, ETHUSD on 1H, 4H, Daily:
Create alerts for: all alert types

**Desk 6 (Equities)** — TSLA on 1H, 4H, Daily:
Create alerts for: bullish_confirmation, bearish_confirmation, bullish_plus, bearish_plus, exits, take_profit, stop_loss

---

## Troubleshooting

### "Build failed" on Railway
Go to the **Deployments** tab and click the failed deployment to see logs. Common fixes:
- Make sure `DATABASE_URL` is referenced from the PostgreSQL service
- Make sure all files were pushed to GitHub correctly

### "Invalid webhook secret" when testing
The `secret` in your curl command or TradingView alert must exactly match the `WEBHOOK_SECRET` variable you set in Railway. Check for extra spaces.

### "Connection refused" or timeout
Make sure you generated a public domain in Railway Settings → Networking.

### TradingView says "Webhook request failed"
Check that your Railway app is running (green status in the dashboard). Go to the Deployments tab and check the logs for errors.

### Want to see your database?
In Railway, click on the PostgreSQL service, then go to the **Data** tab. You can browse the `signals` table to see every alert that came in.

---

## What's Next (Phase 2)

Once alerts are flowing in and you can see them in the database, we'll build:
1. TwelveData market data enrichment
2. ML model signal scoring
3. Claude CTO decision engine
4. The consensus scoring system

All of these plug into the same webhook pipeline — validated signals will continue through the new stages before reaching MT5.
