"""
SignalReplayBacktester — Replays historical shadow_signals through the
virtual broker against historical OHLCV data.

Tests how the signal pipeline WOULD HAVE performed with full execution
simulation including spreads, slippage, and position management.
"""
import math
import random
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, List

import numpy as np
from sqlalchemy import text, func, and_
from sqlalchemy.orm import Session

from app.models.shadow_signal import ShadowSignal
from app.models.sim_models import SimProfile
from app.config import get_pip_info, DESKS
from app.services.simulation.fill_model import estimate_fill, resolve_same_bar_exit

logger = logging.getLogger("TradingSystem.Backtester")


class SignalReplayBacktester:
    """Replays recorded signals chronologically with full execution simulation."""

    def __init__(self, db_session_factory, profile_name: str = "SRV_100"):
        self._db_factory = db_session_factory
        self._profile_name = profile_name
        self._backtest_profile = None

    async def run(
        self,
        start_date: datetime,
        end_date: datetime,
        filters: Optional[Dict] = None,
    ) -> Dict:
        """
        Run a signal replay backtest.
        Returns comprehensive results dict with performance metrics.
        """
        from app.services.virtual_broker import VirtualBroker

        db = self._db_factory()
        try:
            # Create backtest profile
            ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
            profile = SimProfile(
                name=f"BT_{self._profile_name}_{ts}"[:30],
                initial_balance=100000,
                current_balance=100000,
                leverage=100,
                risk_pct=1.0,
                is_active=True,
            )
            db.add(profile)
            db.flush()
            self._backtest_profile = profile

            # Query signals
            query = (
                db.query(ShadowSignal)
                .filter(
                    ShadowSignal.created_at >= start_date,
                    ShadowSignal.created_at <= end_date,
                )
            )

            filters = filters or {}
            if filters.get("desk_id"):
                query = query.filter(ShadowSignal.desk_id == filters["desk_id"])
            if filters.get("symbol"):
                query = query.filter(ShadowSignal.symbol == filters["symbol"])
            if filters.get("min_consensus_score"):
                query = query.filter(ShadowSignal.consensus_score >= filters["min_consensus_score"])
            if filters.get("min_ml_score"):
                query = query.filter(ShadowSignal.ml_score >= filters["min_ml_score"])
            if filters.get("hurst_min"):
                query = query.filter(ShadowSignal.hurst_exponent >= filters["hurst_min"])
            if filters.get("hurst_max"):
                query = query.filter(ShadowSignal.hurst_exponent <= filters["hurst_max"])

            signals = query.order_by(ShadowSignal.created_at.asc()).all()

            if not signals:
                return {"status": "no_signals", "count": 0}

            # Replay each signal
            trades = []
            broker = VirtualBroker(self._db_factory)

            for signal in signals:
                try:
                    # Get historical price at signal time
                    hist_price = self._get_historical_price(db, signal.symbol, signal.created_at)
                    if not hist_price:
                        continue

                    # Execute through virtual broker
                    result = await broker.execute_signal(db, signal.id, profile.name)
                    if result.get("status") != "FILLED":
                        continue

                    # Walk forward to find exit
                    exit_result = self._walk_forward_exits(
                        db, signal, result, hist_price, profile.initial_balance
                    )

                    if exit_result:
                        # Update profile balance
                        profile.current_balance += exit_result.get("pnl_dollars", 0)
                        trades.append(exit_result)

                except Exception as e:
                    logger.debug(f"Backtest signal {signal.id} failed: {e}")

            db.commit()

            # Compute metrics
            metrics = self._compute_metrics(trades)
            metrics["signal_count"] = len(signals)
            metrics["trade_count"] = len(trades)
            metrics["profile_name"] = profile.name
            metrics["start_date"] = start_date.isoformat()
            metrics["end_date"] = end_date.isoformat()
            metrics["final_balance"] = round(profile.current_balance, 2)
            metrics["total_return_pct"] = round(
                (profile.current_balance - profile.initial_balance) / profile.initial_balance * 100, 2
            )

            return metrics

        except Exception as e:
            logger.error(f"Backtest failed: {e}")
            return {"status": "error", "error": str(e)}
        finally:
            db.close()

    def _walk_forward_exits(
        self,
        db: Session,
        signal: ShadowSignal,
        trade_result: Dict,
        entry_price: float,
        profile_initial_balance: float = 100000.0,
    ) -> Optional[Dict]:
        """Walk forward through OHLCV to find exit."""
        desk_id = signal.desk_id or "DESK2_INTRADAY"
        desk = DESKS.get(desk_id, {})
        max_hold_hours = desk.get("max_hold_hours", 24)
        max_bars = int(max_hold_hours * 60)

        pip_size, pip_value = get_pip_info(signal.symbol)
        if pip_size <= 0:
            return None

        direction = (signal.direction or "LONG").upper()
        sl = signal.sl1
        tp1 = signal.tp1
        tp2 = signal.tp2

        # Get bars
        bars = self._get_ohlcv_bars(db, signal.symbol, signal.created_at, max_bars)
        if not bars:
            return None

        commission_per_trade = 0.5
        simulated_latency_ms = 150
        simulated_slippage_bps = 1.5

        max_fav = 0.0
        max_adv = 0.0
        exit_price = None
        exit_reason = None
        hold_bars = 0

        for i, bar in enumerate(bars):
            h = float(bar[2])  # high
            l = float(bar[3])  # low
            c = float(bar[4])  # close
            hold_bars = i + 1

            # MFE/MAE
            if direction == "LONG":
                fav = (h - entry_price) / pip_size
                adv = (entry_price - l) / pip_size
            else:
                fav = (entry_price - l) / pip_size
                adv = (h - entry_price) / pip_size

            max_fav = max(max_fav, fav)
            max_adv = max(max_adv, adv)

                        # Conservative same-candle resolution for TP/SL ambiguity
            if sl and tp1:
                if direction == "LONG":
                    same = resolve_same_bar_exit(direction, l, h, sl, tp1)
                else:
                    same = resolve_same_bar_exit(direction, l, h, tp1, sl)
                if same == "SL":
                    exit_price = sl
                    exit_reason = "SL_HIT_SAME_BAR"
                    break
                if same == "TP":
                    exit_price = tp1
                    exit_reason = "TP1_HIT_SAME_BAR"
                    break

            # Check SL
            if sl:
                if direction == "LONG" and l <= sl:
                    exit_price = sl
                    exit_reason = "SL_HIT"
                    break
                elif direction == "SHORT" and h >= sl:
                    exit_price = sl
                    exit_reason = "SL_HIT"
                    break

            # Check TP1
            if tp1:
                if direction == "LONG" and h >= tp1:
                    exit_price = tp1
                    exit_reason = "TP1_HIT"
                    break
                elif direction == "SHORT" and l <= tp1:
                    exit_price = tp1
                    exit_reason = "TP1_HIT"
                    break

        # Timeout
        if exit_price is None and bars:
            # pessimistic timeout exit: long exits at bid, short exits at ask
            last_close = float(bars[-1][4])
            synthetic_spread = max(last_close * 0.0002, pip_size * 0.5)
            bid = last_close - synthetic_spread / 2
            ask = last_close + synthetic_spread / 2
            side = "SELL" if direction == "LONG" else "BUY"
            fill = estimate_fill(side=side, bid=bid, ask=ask, latency_ms=simulated_latency_ms, slippage_bps=simulated_slippage_bps)
            exit_price = fill.fill_price
            exit_reason = "TIMEOUT_EXIT"

        if exit_price is None:
            return None

        if direction == "LONG":
            pnl_pips = (exit_price - entry_price) / pip_size
        else:
            pnl_pips = (entry_price - exit_price) / pip_size

        lot_size = trade_result.get("lot_size", 0.01)
        gross_pnl = pnl_pips * pip_value * lot_size
        spread_cost = max((simulated_slippage_bps / 10000) * entry_price * pip_value * lot_size, 0)
        pnl_dollars = round(gross_pnl - spread_cost - commission_per_trade, 2)
        hold_minutes = hold_bars * 1.0

        return {
            "symbol": signal.symbol,
            "direction": direction,
            "desk_id": desk_id,
            "entry_price": entry_price,
            "exit_price": exit_price,
            "exit_reason": exit_reason,
            "pnl_pips": round(pnl_pips, 2),
            "pnl_dollars": pnl_dollars,
            "hold_minutes": round(hold_minutes, 1),
            "mfe": round(max_fav, 2),
            "mae": round(max_adv, 2),
            "r_multiple": round((pnl_pips / max_adv), 2) if max_adv > 0 else 0,
            "lot_size": lot_size,
            "session": signal.active_session,
            "hour_utc": signal.created_at.hour if signal.created_at else 0,
            "day_of_week": signal.created_at.weekday() if signal.created_at else 0,
        }

    def _compute_metrics(self, trades: List[Dict]) -> Dict:
        """Compute comprehensive performance metrics."""
        if not trades:
            return {"status": "no_trades"}

        wins = [t for t in trades if t["pnl_pips"] > 0]
        losses = [t for t in trades if t["pnl_pips"] <= 0]
        total = len(trades)

        win_rate = len(wins) / total * 100 if total > 0 else 0
        pnl_pips_list = [t["pnl_pips"] for t in trades]
        pnl_dollars_list = [t["pnl_dollars"] for t in trades]

        gross_profit = sum(t["pnl_pips"] for t in wins) if wins else 0
        gross_loss = abs(sum(t["pnl_pips"] for t in losses)) if losses else 1
        profit_factor = gross_profit / gross_loss if gross_loss > 0 else float("inf")

        expectancy = sum(pnl_pips_list) / total if total > 0 else 0
        expectancy_dollars = sum(pnl_dollars_list) / total if total > 0 else 0

        # Max consecutive
        max_consec_wins = max_consec_losses = 0
        current_streak = 0
        current_type = None
        for t in trades:
            win = t["pnl_pips"] > 0
            if current_type is None or win != current_type:
                current_type = win
                current_streak = 1
            else:
                current_streak += 1
            if win:
                max_consec_wins = max(max_consec_wins, current_streak)
            else:
                max_consec_losses = max(max_consec_losses, current_streak)

        # Sharpe ratio (annualized from daily returns)
        if len(pnl_dollars_list) > 1:
            arr = np.array(pnl_dollars_list)
            sharpe = (arr.mean() / arr.std()) * math.sqrt(252) if arr.std() > 0 else 0
            # Sortino
            downside = arr[arr < 0]
            sortino = (arr.mean() / downside.std()) * math.sqrt(252) if len(downside) > 1 and downside.std() > 0 else 0
        else:
            sharpe = 0
            sortino = 0

        # Max drawdown
        equity_curve = np.cumsum([0] + pnl_dollars_list)
        peak = np.maximum.accumulate(equity_curve)
        drawdown = peak - equity_curve
        max_dd = float(np.max(drawdown)) if len(drawdown) > 0 else 0
        max_dd_pct = (max_dd / (100000 + float(peak[np.argmax(drawdown)]))) * 100 if max_dd > 0 else 0

        # Per-desk breakdown
        desk_stats = {}
        for t in trades:
            d = t.get("desk_id", "UNKNOWN")
            if d not in desk_stats:
                desk_stats[d] = {"trades": 0, "wins": 0, "pnl": 0}
            desk_stats[d]["trades"] += 1
            if t["pnl_pips"] > 0:
                desk_stats[d]["wins"] += 1
            desk_stats[d]["pnl"] += t["pnl_pips"]

        for d in desk_stats:
            s = desk_stats[d]
            s["win_rate"] = round(s["wins"] / s["trades"] * 100, 1) if s["trades"] > 0 else 0
            s["pnl"] = round(s["pnl"], 1)

        avg_mfe = sum(t.get("mfe", 0) for t in trades) / total if total > 0 else 0
        avg_r_multiple = sum(t.get("r_multiple", 0) for t in trades) / total if total > 0 else 0
        avg_mae = sum(t.get("mae", 0) for t in trades) / total if total > 0 else 0
        avg_win_hold = sum(t["hold_minutes"] for t in wins) / len(wins) if wins else 0
        avg_loss_hold = sum(t["hold_minutes"] for t in losses) / len(losses) if losses else 0
        calmar = ((sum(pnl_dollars_list)/100000.0)*100)/(max_dd_pct if max_dd_pct>0 else 1e-9) if total>0 else 0

        return {
            "status": "completed",
            "total_trades": total,
            "wins": len(wins),
            "losses": len(losses),
            "win_rate": round(win_rate, 1),
            "profit_factor": round(profit_factor, 2),
            "expectancy_pips": round(expectancy, 2),
            "expectancy_dollars": round(expectancy_dollars, 2),
            "total_pnl_pips": round(sum(pnl_pips_list), 1),
            "total_pnl_dollars": round(sum(pnl_dollars_list), 2),
            "max_consecutive_wins": max_consec_wins,
            "max_consecutive_losses": max_consec_losses,
            "sharpe_ratio": round(sharpe, 2),
            "sortino_ratio": round(sortino, 2),
            "calmar_ratio": round(calmar, 2),
            "max_drawdown_dollars": round(max_dd, 2),
            "max_drawdown_pct": round(max_dd_pct, 2),
            "avg_mfe_pips": round(avg_mfe, 2),
            "avg_mae_pips": round(avg_mae, 2),
            "avg_r_multiple": round(avg_r_multiple, 2),
            "avg_win_hold_minutes": round(avg_win_hold,1),
            "avg_loss_hold_minutes": round(avg_loss_hold,1),
            "hold_time_win_loss_delta": round(avg_win_hold-avg_loss_hold,1),
            "mfe_mae_ratio": round(avg_mfe / avg_mae, 2) if avg_mae > 0 else 0,
            "avg_hold_minutes": round(sum(t["hold_minutes"] for t in trades) / total, 1),
            "best_trade_pips": round(max(pnl_pips_list), 2),
            "worst_trade_pips": round(min(pnl_pips_list), 2),
            "per_desk": desk_stats,
        }

    async def run_monte_carlo(
        self, trades: List[Dict], n_simulations: int = 10000
    ) -> Dict:
        """Bootstrap resampling of trade results."""
        if not trades or len(trades) < 10:
            return {"status": "insufficient_trades"}

        pnl_list = [t["pnl_dollars"] for t in trades]
        n_trades = len(pnl_list)

        final_equities = []
        max_drawdowns = []
        win_rates = []

        for _ in range(n_simulations):
            sample = random.choices(pnl_list, k=n_trades)
            equity_curve = np.cumsum([100000] + sample)
            peak = np.maximum.accumulate(equity_curve)
            dd = peak - equity_curve
            max_dd = float(np.max(dd))

            final_equities.append(float(equity_curve[-1]))
            max_drawdowns.append(max_dd)
            win_rates.append(sum(1 for p in sample if p > 0) / len(sample) * 100)

        fe = np.array(final_equities)
        mdd = np.array(max_drawdowns)
        wr = np.array(win_rates)

        return {
            "status": "completed",
            "n_simulations": n_simulations,
            "final_equity_p5": round(float(np.percentile(fe, 5)), 2),
            "final_equity_p50": round(float(np.percentile(fe, 50)), 2),
            "final_equity_p95": round(float(np.percentile(fe, 95)), 2),
            "max_drawdown_p50": round(float(np.percentile(mdd, 50)), 2),
            "max_drawdown_p75": round(float(np.percentile(mdd, 75)), 2),
            "max_drawdown_p95": round(float(np.percentile(mdd, 95)), 2),
            "win_rate_p5": round(float(np.percentile(wr, 5)), 1),
            "win_rate_p50": round(float(np.percentile(wr, 50)), 1),
            "probability_of_ruin": round(float(np.mean(fe <= 0)) * 100, 2),
        }

    def _get_historical_price(
        self, db: Session, symbol: str, at_time: datetime
    ) -> Optional[float]:
        """Get the close price from ohlcv_1m nearest to the given time."""
        try:
            result = db.execute(
                text("""
                    SELECT close FROM ohlcv_1m
                    WHERE symbol = :symbol
                      AND time <= :at_time
                    ORDER BY time DESC
                    LIMIT 1
                """),
                {"symbol": symbol, "at_time": at_time},
            )
            row = result.fetchone()
            return float(row[0]) if row else None
        except Exception:
            return None

    def _get_ohlcv_bars(
        self, db: Session, symbol: str, start: datetime, max_bars: int
    ) -> list:
        """Fetch OHLCV bars as raw tuples."""
        try:
            result = db.execute(
                text("""
                    SELECT time, open, high, low, close, volume
                    FROM ohlcv_1m
                    WHERE symbol = :symbol AND time >= :start
                    ORDER BY time ASC
                    LIMIT :limit
                """),
                {"symbol": symbol, "start": start, "limit": max_bars},
            )
            return result.fetchall()
        except Exception:
            return []
