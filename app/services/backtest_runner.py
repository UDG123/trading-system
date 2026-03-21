"""
OniQuant v5.9 — Zero-Key Simulation Runner
CLI entrypoint for the 30-Day Strategy Stress Test.

Usage:
    python -m app.services.backtest_runner [--days 30] [--no-db] [--output report.md]
"""
import argparse
import logging
import sys
from datetime import datetime, timezone

from app.services.backtester import Backtester, YFINANCE_SYMBOLS, HURST_VETO_THRESHOLD

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("TradingSystem.BacktestRunner")


def run_backtest(days: int = 30, persist_to_db: bool = True, output_path: str = None) -> str:
    """
    Execute the full 30-day stress test pipeline.
    Returns the markdown report.
    """
    bt = Backtester()
    logger.info(f"Starting Zero-Key Simulation | Run: {bt.run_id} | Period: {days}d")

    all_signals = []

    # ── STEP 1: Fetch data + generate signals for each instrument ──
    for symbol in YFINANCE_SYMBOLS:
        try:
            df = bt.fetch_candles(symbol, days=days)
        except Exception as e:
            logger.error(f"Failed to fetch {symbol}: {e}")
            continue

        # ── STEP 2: Generate mock LuxAlgo signals ──
        signals = bt.generate_signals(df, symbol)

        # ── STEP 3: Simulate outcomes for ALL signals (baseline) ──
        # First, simulate what would happen if all signals were taken (no filter)
        signals = bt.simulate_outcomes(signals, df)

        # Save baseline outcomes before Hurst gate
        for sig in signals:
            sig["baseline_outcome"] = sig["outcome"]
            sig["baseline_pnl_dollars"] = sig.get("pnl_dollars", 0)
            sig["baseline_pnl_pips"] = sig.get("pnl_pips", 0)

        # ── STEP 3b: Apply Hurst Gate ──
        passed, vetoed = bt.apply_hurst_gate(signals)

        # Mark vetoed signals
        for sig in vetoed:
            sig["outcome"] = "VETOED"

        all_signals.extend(signals)

    if not all_signals:
        logger.error("No signals generated across any instrument — aborting")
        return "# No data available for backtest"

    total = len(all_signals)
    vetoed_count = len([s for s in all_signals if s.get("hurst_vetoed")])
    passed_count = total - vetoed_count

    logger.info(
        f"Pipeline complete: {total} total signals | "
        f"{passed_count} passed | {vetoed_count} vetoed"
    )

    # ── STEP 4: Persist to PostgreSQL ──
    if persist_to_db:
        try:
            from app.database import SessionLocal
            from app.models.backtest_result import BacktestResult
            from app.database import engine, Base

            # Create table if not exists
            BacktestResult.__table__.create(bind=engine, checkfirst=True)

            db = SessionLocal()
            try:
                bt.record_to_db(all_signals, db)
                logger.info("Results persisted to PostgreSQL")
            finally:
                db.close()
        except Exception as e:
            logger.warning(f"DB persistence skipped: {e}")

    # ── STEP 5: Generate report ──
    report = bt.generate_report(all_signals)

    # Write to file if requested
    if output_path:
        with open(output_path, "w") as f:
            f.write(report)
        logger.info(f"Report written to {output_path}")
    else:
        # Default output location
        default_path = f"backtest_report_{bt.run_id}.md"
        with open(default_path, "w") as f:
            f.write(report)
        logger.info(f"Report written to {default_path}")

    return report


def main():
    parser = argparse.ArgumentParser(
        description="OniQuant v5.9 — Zero-Key Simulation (30-Day Stress Test)"
    )
    parser.add_argument("--days", type=int, default=30, help="Lookback period in days (default: 30)")
    parser.add_argument("--no-db", action="store_true", help="Skip PostgreSQL persistence")
    parser.add_argument("--output", type=str, default=None, help="Output file path for the report")
    args = parser.parse_args()

    report = run_backtest(
        days=args.days,
        persist_to_db=not args.no_db,
        output_path=args.output,
    )

    print("\n" + report)


if __name__ == "__main__":
    main()
