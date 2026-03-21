"""
OniQuant v5.9 — Zero-Key Simulation Backtester
30-Day Strategy Stress Test with Hurst Gate filtering.

Fetches 15m candle data via yfinance, generates mock EMA-Cross/RSI signals,
applies the Hurst Exponent precision gate, and records results to PostgreSQL.
"""
import logging
import uuid
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import yfinance as yf

from app.config import get_pip_info, SYMBOL_ASSET_CLASS, PIP_VALUES
from app.services.twelvedata_enricher import TwelveDataEnricher

logger = logging.getLogger("TradingSystem.Backtester")

# ── yfinance symbol mapping ──
YFINANCE_SYMBOLS = {
    "XAUUSD": "GC=F",       # Gold futures
    "EURUSD": "EURUSD=X",   # EUR/USD forex
    "BTCUSD": "BTC-USD",    # Bitcoin
}

# ── Strategy parameters ──
EMA_FAST_PERIOD = 9
EMA_SLOW_PERIOD = 21
RSI_PERIOD = 14
RSI_OB = 70  # Overbought
RSI_OS = 30  # Oversold
HURST_VETO_THRESHOLD = 0.55
HURST_WINDOW = 100  # bars for Hurst calculation
ATR_PERIOD = 14
RISK_PER_TRADE = 1000  # $1,000 risk per trade (1% of $100K account)


class Backtester:
    """
    Runs a zero-key (no API keys needed) backtest simulation using yfinance
    for data and EMA-Cross/RSI for signal generation.
    """

    def __init__(self):
        self.run_id = f"bt_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
        self.hurst_calculator = TwelveDataEnricher.calculate_hurst

    # ─────────────────────────────────────────────────────────────
    # STEP 1: Data Retrieval (PriceFeeder via yfinance)
    # ─────────────────────────────────────────────────────────────

    def fetch_candles(self, symbol: str, days: int = 30) -> pd.DataFrame:
        """
        Fetch 15-minute candle data for the last N days via yfinance.
        yfinance allows up to 60 days of intraday data.
        """
        yf_symbol = YFINANCE_SYMBOLS.get(symbol)
        if not yf_symbol:
            raise ValueError(f"No yfinance mapping for {symbol}")

        logger.info(f"Fetching {days}d of 15m candles for {symbol} ({yf_symbol})")

        ticker = yf.Ticker(yf_symbol)
        df = ticker.history(period=f"{days}d", interval="15m")

        if df.empty:
            raise ValueError(f"No data returned for {yf_symbol}")

        df.index = pd.to_datetime(df.index, utc=True)
        df = df.rename(columns={
            "Open": "open", "High": "high", "Low": "low",
            "Close": "close", "Volume": "volume",
        })

        logger.info(f"Fetched {len(df)} candles for {symbol} "
                     f"({df.index[0]} → {df.index[-1]})")
        return df[["open", "high", "low", "close", "volume"]]

    # ─────────────────────────────────────────────────────────────
    # STEP 2: Signal Simulation (Mock LuxAlgo EMA-Cross/RSI)
    # ─────────────────────────────────────────────────────────────

    def generate_signals(self, df: pd.DataFrame, symbol: str) -> List[Dict]:
        """
        Generate mock trading signals based on EMA crossover + RSI confirmation.
        Simulates the 81 LuxAlgo alert types hitting the pipeline.

        Signal logic:
          BUY:  EMA9 crosses above EMA21 AND RSI < 70 (not overbought)
          SELL: EMA9 crosses below EMA21 AND RSI > 30 (not oversold)
        """
        closes = df["close"].values
        highs = df["high"].values
        lows = df["low"].values

        # Calculate indicators
        ema_fast = self._ema(closes, EMA_FAST_PERIOD)
        ema_slow = self._ema(closes, EMA_SLOW_PERIOD)
        rsi = self._rsi(closes, RSI_PERIOD)
        atr = self._atr(highs, lows, closes, ATR_PERIOD)

        signals = []
        # Start after the longest lookback period
        start_idx = max(EMA_SLOW_PERIOD, RSI_PERIOD, ATR_PERIOD, HURST_WINDOW) + 1

        for i in range(start_idx, len(df)):
            direction = None
            signal_type = None

            # ── EMA Cross Bullish ──
            if (ema_fast[i] > ema_slow[i] and
                    ema_fast[i - 1] <= ema_slow[i - 1] and
                    rsi[i] < RSI_OB):
                direction = "BUY"
                signal_type = "ema_cross_bullish"

            # ── EMA Cross Bearish ──
            elif (ema_fast[i] < ema_slow[i] and
                  ema_fast[i - 1] >= ema_slow[i - 1] and
                  rsi[i] > RSI_OS):
                direction = "SELL"
                signal_type = "ema_cross_bearish"

            # ── RSI Reversal Bullish (oversold bounce) ──
            elif (rsi[i] > RSI_OS and rsi[i - 1] <= RSI_OS and
                  ema_fast[i] > ema_slow[i]):
                direction = "BUY"
                signal_type = "rsi_reversal_bullish"

            # ── RSI Reversal Bearish (overbought rejection) ──
            elif (rsi[i] < RSI_OB and rsi[i - 1] >= RSI_OB and
                  ema_fast[i] < ema_slow[i]):
                direction = "SELL"
                signal_type = "rsi_reversal_bearish"

            if direction is None:
                continue

            # ATR-based SL/TP (1.5x ATR stop, 2x ATR target → 1.33 R:R)
            current_atr = atr[i]
            if current_atr <= 0:
                continue

            entry = closes[i]
            if direction == "BUY":
                sl = entry - 1.5 * current_atr
                tp = entry + 2.0 * current_atr
            else:
                sl = entry + 1.5 * current_atr
                tp = entry - 2.0 * current_atr

            # Hurst exponent over trailing window
            hurst_series = closes[max(0, i - HURST_WINDOW):i + 1].tolist()
            hurst = self.hurst_calculator(hurst_series)

            signals.append({
                "symbol": symbol,
                "timestamp": df.index[i],
                "direction": direction,
                "signal_type": signal_type,
                "entry_price": entry,
                "stop_loss": sl,
                "take_profit": tp,
                "rsi": rsi[i],
                "ema_fast": ema_fast[i],
                "ema_slow": ema_slow[i],
                "atr": current_atr,
                "hurst": hurst,
            })

        logger.info(f"Generated {len(signals)} signals for {symbol}")
        return signals

    # ─────────────────────────────────────────────────────────────
    # STEP 3: Precision Gate (Hurst Exponent Veto)
    # ─────────────────────────────────────────────────────────────

    def apply_hurst_gate(self, signals: List[Dict]) -> Tuple[List[Dict], List[Dict]]:
        """
        Apply the Hurst Gate: veto signals where H < 0.55.
        Returns (passed_signals, vetoed_signals).
        """
        passed = []
        vetoed = []

        for sig in signals:
            h = sig.get("hurst")
            if h is None or h < HURST_VETO_THRESHOLD:
                sig["hurst_vetoed"] = True
                vetoed.append(sig)
            else:
                sig["hurst_vetoed"] = False
                passed.append(sig)

        logger.info(
            f"Hurst Gate: {len(passed)} passed, {len(vetoed)} vetoed "
            f"({len(vetoed) / max(len(signals), 1) * 100:.1f}% filtered)"
        )
        return passed, vetoed

    # ─────────────────────────────────────────────────────────────
    # STEP 4: Trade Simulation (walk-forward outcome)
    # ─────────────────────────────────────────────────────────────

    def simulate_outcomes(self, signals: List[Dict], df: pd.DataFrame) -> List[Dict]:
        """
        Walk forward through candle data to determine if each signal
        hits TP or SL first. Returns signals with outcome filled in.
        """
        for sig in signals:
            if sig.get("hurst_vetoed"):
                sig["outcome"] = "VETOED"
                sig["exit_price"] = None
                sig["pnl_pips"] = 0.0
                sig["pnl_dollars"] = 0.0
                sig["risk_reward"] = 0.0
                continue

            entry_time = sig["timestamp"]
            entry = sig["entry_price"]
            sl = sig["stop_loss"]
            tp = sig["take_profit"]
            direction = sig["direction"]

            # Get candles after entry
            future = df[df.index > entry_time]
            outcome = "LOSS"  # default if neither hit
            exit_price = sl  # worst case

            for _, candle in future.iterrows():
                if direction == "BUY":
                    if candle["low"] <= sl:
                        outcome = "LOSS"
                        exit_price = sl
                        break
                    if candle["high"] >= tp:
                        outcome = "WIN"
                        exit_price = tp
                        break
                else:  # SELL
                    if candle["high"] >= sl:
                        outcome = "LOSS"
                        exit_price = sl
                        break
                    if candle["low"] <= tp:
                        outcome = "WIN"
                        exit_price = tp
                        break

            # Calculate PnL
            pip_size, pip_value = self._get_pip_info(sig["symbol"])
            if direction == "BUY":
                pnl_pips = (exit_price - entry) / pip_size
            else:
                pnl_pips = (entry - exit_price) / pip_size

            # Dollar PnL based on risk sizing
            risk_pips = abs(entry - sl) / pip_size
            if risk_pips > 0:
                dollars_per_pip = RISK_PER_TRADE / risk_pips
                pnl_dollars = pnl_pips * dollars_per_pip
                rr = abs(pnl_pips / risk_pips) if outcome == "WIN" else -1.0
            else:
                pnl_dollars = 0.0
                rr = 0.0

            sig["outcome"] = outcome
            sig["exit_price"] = exit_price
            sig["pnl_pips"] = round(pnl_pips, 2)
            sig["pnl_dollars"] = round(pnl_dollars, 2)
            sig["risk_reward"] = round(rr, 2)

        return signals

    # ─────────────────────────────────────────────────────────────
    # STEP 5: Virtual Ledger (DB persistence)
    # ─────────────────────────────────────────────────────────────

    def record_to_db(self, signals: List[Dict], db_session) -> int:
        """
        Write all strikes to the backtest_results table.
        Returns number of rows inserted.
        """
        from app.models.backtest_result import BacktestResult

        count = 0
        for sig in signals:
            row = BacktestResult(
                run_id=self.run_id,
                symbol=sig["symbol"],
                timeframe="15M",
                direction=sig["direction"],
                signal_type=sig["signal_type"],
                signal_timestamp=sig["timestamp"],
                entry_price=sig["entry_price"],
                stop_loss=sig["stop_loss"],
                take_profit=sig["take_profit"],
                exit_price=sig.get("exit_price"),
                hurst_exponent=sig.get("hurst"),
                hurst_vetoed=sig.get("hurst_vetoed", False),
                outcome=sig.get("outcome"),
                pnl_pips=sig.get("pnl_pips"),
                pnl_dollars=sig.get("pnl_dollars"),
                risk_reward=sig.get("risk_reward"),
                rsi_at_entry=sig.get("rsi"),
                ema_fast=sig.get("ema_fast"),
                ema_slow=sig.get("ema_slow"),
                atr_at_entry=sig.get("atr"),
            )
            db_session.add(row)
            count += 1

        db_session.commit()
        logger.info(f"Recorded {count} strikes to backtest_results (run={self.run_id})")
        return count

    # ─────────────────────────────────────────────────────────────
    # STEP 6: Report Generation
    # ─────────────────────────────────────────────────────────────

    def generate_report(self, all_signals: List[Dict]) -> str:
        """
        Generate a markdown audit report comparing baseline (no Hurst gate)
        vs filtered (with Hurst gate) performance.
        """
        # Split into baseline (all non-vetoed outcomes) and full set
        baseline_trades = [s for s in all_signals if not s.get("hurst_vetoed")]
        all_trades_as_if_taken = []

        # For baseline comparison: simulate as if ALL signals were taken
        for sig in all_signals:
            # Create a copy pretending Hurst gate didn't exist
            baseline_sig = dict(sig)
            if baseline_sig["outcome"] == "VETOED":
                # Need to re-evaluate — these were vetoed, so we need
                # to know what WOULD have happened
                baseline_sig["_needs_baseline_eval"] = True
            all_trades_as_if_taken.append(baseline_sig)

        # Separate by symbol for per-asset breakdown
        symbols = sorted(set(s["symbol"] for s in all_signals))

        report_lines = [
            "# OniQuant v5.9 — Zero-Key Simulation Report",
            f"**Run ID:** `{self.run_id}`",
            f"**Date:** {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}",
            f"**Period:** 30-Day | 15M Candles",
            f"**Instruments:** {', '.join(symbols)}",
            f"**Strategy:** EMA({EMA_FAST_PERIOD}/{EMA_SLOW_PERIOD}) Cross + RSI({RSI_PERIOD}) Confirmation",
            f"**Hurst Gate:** Veto if H < {HURST_VETO_THRESHOLD}",
            "",
            "---",
            "",
            "## Executive Summary",
            "",
        ]

        # ── Baseline metrics (all signals, no filter) ──
        baseline_all = [s for s in all_signals]
        baseline_outcomes = self._compute_metrics(baseline_all, include_vetoed_as_traded=True)

        # ── Filtered metrics (Hurst gate applied) ──
        filtered = [s for s in all_signals if not s.get("hurst_vetoed")]
        filtered_outcomes = self._compute_metrics(filtered, include_vetoed_as_traded=False)

        # ── Vetoed analysis ──
        vetoed = [s for s in all_signals if s.get("hurst_vetoed")]
        vetoed_count = len(vetoed)
        total_count = len(all_signals)
        filter_pct = (vetoed_count / max(total_count, 1)) * 100

        # How many vetoed signals WOULD have been losses?
        vetoed_would_lose = len([s for s in all_signals
                                 if s.get("hurst_vetoed") and s.get("baseline_outcome") == "LOSS"])
        vetoed_would_win = len([s for s in all_signals
                                if s.get("hurst_vetoed") and s.get("baseline_outcome") == "WIN"])

        losing_filter_rate = (vetoed_would_lose / max(vetoed_count, 1)) * 100

        report_lines.extend([
            f"| Metric | Baseline (No Filter) | With Hurst Gate | Improvement |",
            f"|--------|---------------------|-----------------|-------------|",
            f"| Total Signals | {baseline_outcomes['total']} | {filtered_outcomes['total']} | -{vetoed_count} filtered |",
            f"| Win Rate | {baseline_outcomes['win_rate']:.1f}% | {filtered_outcomes['win_rate']:.1f}% | {filtered_outcomes['win_rate'] - baseline_outcomes['win_rate']:+.1f}% |",
            f"| Profit Factor | {baseline_outcomes['profit_factor']:.2f} | {filtered_outcomes['profit_factor']:.2f} | {filtered_outcomes['profit_factor'] - baseline_outcomes['profit_factor']:+.2f} |",
            f"| Max Drawdown | ${baseline_outcomes['max_drawdown']:,.2f} | ${filtered_outcomes['max_drawdown']:,.2f} | ${baseline_outcomes['max_drawdown'] - filtered_outcomes['max_drawdown']:,.2f} saved |",
            f"| Net PnL | ${baseline_outcomes['net_pnl']:,.2f} | ${filtered_outcomes['net_pnl']:,.2f} | ${filtered_outcomes['net_pnl'] - baseline_outcomes['net_pnl']:+,.2f} |",
            "",
            "---",
            "",
            "## Hurst Gate Analysis",
            "",
            f"- **Total signals generated:** {total_count}",
            f"- **Signals vetoed (H < {HURST_VETO_THRESHOLD}):** {vetoed_count} ({filter_pct:.1f}%)",
            f"- **Of vetoed signals that WOULD have been losses:** {vetoed_would_lose} ({losing_filter_rate:.1f}%)",
            f"- **Of vetoed signals that WOULD have been wins:** {vetoed_would_win}",
            f"- **Goal: Filter 30%+ of losing trades:** {'ACHIEVED' if losing_filter_rate >= 30 else 'NOT MET'} ({losing_filter_rate:.1f}%)",
            "",
            "---",
            "",
            "## Per-Symbol Breakdown",
            "",
        ])

        for sym in symbols:
            sym_all = [s for s in all_signals if s["symbol"] == sym]
            sym_filtered = [s for s in sym_all if not s.get("hurst_vetoed")]
            sym_vetoed = [s for s in sym_all if s.get("hurst_vetoed")]
            sym_baseline = self._compute_metrics(sym_all, include_vetoed_as_traded=True)
            sym_filt = self._compute_metrics(sym_filtered, include_vetoed_as_traded=False)

            report_lines.extend([
                f"### {sym}",
                "",
                f"| Metric | Baseline | Filtered |",
                f"|--------|----------|----------|",
                f"| Signals | {sym_baseline['total']} | {sym_filt['total']} |",
                f"| Win Rate | {sym_baseline['win_rate']:.1f}% | {sym_filt['win_rate']:.1f}% |",
                f"| Profit Factor | {sym_baseline['profit_factor']:.2f} | {sym_filt['profit_factor']:.2f} |",
                f"| Max Drawdown | ${sym_baseline['max_drawdown']:,.2f} | ${sym_filt['max_drawdown']:,.2f} |",
                f"| Net PnL | ${sym_baseline['net_pnl']:,.2f} | ${sym_filt['net_pnl']:,.2f} |",
                f"| Vetoed | — | {len(sym_vetoed)} |",
                f"| Avg Hurst (passed) | — | {np.mean([s['hurst'] for s in sym_filtered if s.get('hurst')]):.4f}" if sym_filtered and any(s.get('hurst') for s in sym_filtered) else f"| Avg Hurst | — | N/A |",
                "",
            ])

        report_lines.extend([
            "---",
            "",
            "## Conclusion",
            "",
        ])

        if losing_filter_rate >= 30:
            report_lines.append(
                f"The Hurst Gate (H < {HURST_VETO_THRESHOLD} veto) successfully filtered "
                f"**{losing_filter_rate:.1f}%** of would-be losing trades, exceeding the 30% target. "
                f"Win rate improved from {baseline_outcomes['win_rate']:.1f}% to {filtered_outcomes['win_rate']:.1f}%, "
                f"and profit factor from {baseline_outcomes['profit_factor']:.2f} to {filtered_outcomes['profit_factor']:.2f}."
            )
        else:
            report_lines.append(
                f"The Hurst Gate filtered {losing_filter_rate:.1f}% of would-be losing trades, "
                f"below the 30% target. Consider adjusting the threshold or combining with "
                f"additional filters (ADX, session, volatility regime)."
            )

        report_lines.extend([
            "",
            "---",
            f"*Generated by OniQuant v5.9 Backtester | Run: {self.run_id}*",
        ])

        return "\n".join(report_lines)

    # ─────────────────────────────────────────────────────────────
    # Internal helpers
    # ─────────────────────────────────────────────────────────────

    def _compute_metrics(self, signals: List[Dict], include_vetoed_as_traded: bool = False) -> Dict:
        """Compute win rate, profit factor, max drawdown, net PnL."""
        trades = []
        for s in signals:
            if s.get("hurst_vetoed") and not include_vetoed_as_traded:
                continue
            # For baseline mode, use baseline_outcome if available
            outcome = s.get("baseline_outcome") if include_vetoed_as_traded and s.get("hurst_vetoed") else s.get("outcome")
            pnl = s.get("baseline_pnl_dollars") if include_vetoed_as_traded and s.get("hurst_vetoed") else s.get("pnl_dollars", 0)
            if outcome in ("WIN", "LOSS"):
                trades.append({"outcome": outcome, "pnl": pnl or 0})

        if not trades:
            return {
                "total": 0, "wins": 0, "losses": 0,
                "win_rate": 0.0, "profit_factor": 0.0,
                "max_drawdown": 0.0, "net_pnl": 0.0,
            }

        wins = [t for t in trades if t["outcome"] == "WIN"]
        losses = [t for t in trades if t["outcome"] == "LOSS"]
        gross_profit = sum(t["pnl"] for t in wins)
        gross_loss = abs(sum(t["pnl"] for t in losses))
        profit_factor = gross_profit / max(gross_loss, 0.01)

        # Max drawdown (equity curve)
        equity = 0
        peak = 0
        max_dd = 0
        for t in trades:
            equity += t["pnl"]
            if equity > peak:
                peak = equity
            dd = peak - equity
            if dd > max_dd:
                max_dd = dd

        return {
            "total": len(trades),
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": (len(wins) / len(trades)) * 100,
            "profit_factor": round(profit_factor, 2),
            "max_drawdown": round(max_dd, 2),
            "net_pnl": round(sum(t["pnl"] for t in trades), 2),
        }

    @staticmethod
    def _ema(data: np.ndarray, period: int) -> np.ndarray:
        """Calculate Exponential Moving Average."""
        ema = np.full_like(data, np.nan, dtype=np.float64)
        if len(data) < period:
            return ema
        ema[period - 1] = np.mean(data[:period])
        multiplier = 2.0 / (period + 1)
        for i in range(period, len(data)):
            ema[i] = data[i] * multiplier + ema[i - 1] * (1 - multiplier)
        return ema

    @staticmethod
    def _rsi(data: np.ndarray, period: int = 14) -> np.ndarray:
        """Calculate Relative Strength Index."""
        rsi = np.full_like(data, np.nan, dtype=np.float64)
        deltas = np.diff(data)
        if len(deltas) < period:
            return rsi

        gains = np.where(deltas > 0, deltas, 0.0)
        losses = np.where(deltas < 0, -deltas, 0.0)

        avg_gain = np.mean(gains[:period])
        avg_loss = np.mean(losses[:period])

        if avg_loss == 0:
            rsi[period] = 100.0
        else:
            rs = avg_gain / avg_loss
            rsi[period] = 100.0 - (100.0 / (1.0 + rs))

        for i in range(period, len(deltas)):
            avg_gain = (avg_gain * (period - 1) + gains[i]) / period
            avg_loss = (avg_loss * (period - 1) + losses[i]) / period
            if avg_loss == 0:
                rsi[i + 1] = 100.0
            else:
                rs = avg_gain / avg_loss
                rsi[i + 1] = 100.0 - (100.0 / (1.0 + rs))

        return rsi

    @staticmethod
    def _atr(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int = 14) -> np.ndarray:
        """Calculate Average True Range."""
        atr = np.full_like(closes, np.nan, dtype=np.float64)
        if len(closes) < period + 1:
            return atr

        tr = np.zeros(len(closes))
        tr[0] = highs[0] - lows[0]
        for i in range(1, len(closes)):
            hl = highs[i] - lows[i]
            hc = abs(highs[i] - closes[i - 1])
            lc = abs(lows[i] - closes[i - 1])
            tr[i] = max(hl, hc, lc)

        atr[period] = np.mean(tr[1:period + 1])
        for i in range(period + 1, len(closes)):
            atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period

        return atr

    @staticmethod
    def _get_pip_info(symbol: str) -> Tuple[float, float]:
        """Get pip size and value for a symbol."""
        pip_info = get_pip_info(symbol)
        if pip_info:
            return pip_info
        # Fallback
        return (0.0001, 10.0)
