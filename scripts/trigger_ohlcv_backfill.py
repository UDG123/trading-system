#!/usr/bin/env python3
"""
Trigger OHLCV 1-minute candle backfill for priority symbols.

Usage (against deployed API):
  python scripts/trigger_ohlcv_backfill.py https://your-app.up.railway.app YOUR_WEBHOOK_SECRET

Usage (local):
  python scripts/trigger_ohlcv_backfill.py http://localhost:8000 YOUR_WEBHOOK_SECRET
"""
import sys
import httpx

PRIORITY_SYMBOLS = [
    "EURUSD", "GBPUSD", "USDJPY", "AUDUSD", "USDCAD",
    "NZDUSD", "EURCHF", "XAUUSD", "USDCHF", "EURJPY",
    "GBPJPY", "AUDJPY", "EURNZD", "EURAUD", "EURGBP",
]

def main():
    if len(sys.argv) < 3:
        print("Usage: python scripts/trigger_ohlcv_backfill.py <BASE_URL> <WEBHOOK_SECRET>")
        sys.exit(1)

    base_url = sys.argv[1].rstrip("/")
    secret = sys.argv[2]

    payload = {
        "symbols": PRIORITY_SYMBOLS,
        "interval": "1min",
        "outputsize": 500,
    }

    print(f"Triggering OHLCV backfill for {len(PRIORITY_SYMBOLS)} symbols...")
    print(f"  Endpoint: POST {base_url}/api/ohlcv/ingest")
    print(f"  Interval: 1min | Outputsize: 500 bars/symbol")
    print(f"  Expected: ~{len(PRIORITY_SYMBOLS) * 500} total candles")
    print()

    resp = httpx.post(
        f"{base_url}/api/ohlcv/ingest",
        json=payload,
        headers={"x-api-key": secret},
        timeout=30.0,
    )

    print(f"Status: {resp.status_code}")
    print(f"Response: {resp.json()}")

    if resp.status_code in (200, 202):
        print("\nBackfill triggered successfully (runs in background).")
        print("Monitor progress via: GET /api/ohlcv/stats")
    else:
        print("\nBackfill trigger failed.")
        sys.exit(1)


if __name__ == "__main__":
    main()
