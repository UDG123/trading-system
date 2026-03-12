"""
ML Feature Engineer — Transforms Raw Data into ML-Ready Features

Runs at two points:
  1. REAL-TIME: During pipeline, adds features to each new signal
  2. BATCH: On demand, retroactively enriches historical records

Feature categories:
  A. Signal Quality Features (from raw signal)
  B. Market Regime Features (from enrichment data)
  C. Historical Performance Features (from past trades)
  D. Time Pattern Features (from timestamp analysis)
  E. Correlation & Portfolio Features (from open positions)
  F. Derived Ratios & Scores (computed from other features)
"""
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, List

from sqlalchemy import func, and_, desc
from sqlalchemy.orm import Session

from app.models.ml_trade_log import MLTradeLog
from app.models.trade import Trade
from app.models.signal import Signal
from app.config import (
    DESKS, CORRELATION_GROUPS, get_pip_info,
    CAPITAL_PER_ACCOUNT,
)

logger = logging.getLogger("TradingSystem.FeatureEngineer")


class FeatureEngineer:
    """Computes derived features for ML training data."""

    def enrich_record(self, db: Session, log_id: int):
        """
        Add all derived features to an ML training record.
        Called after pipeline completes for a signal.
        """
        record = db.query(MLTradeLog).filter(MLTradeLog.id == log_id).first()
        if not record:
            return

        features = {}

        # A. Signal Quality
        features.update(self._signal_quality_features(record))

        # B. Market Regime
        features.update(self._market_regime_features(record))

        # C. Historical Performance (lookback on past trades)
        features.update(self._historical_features(db, record))

        # D. Time Patterns
        features.update(self._time_pattern_features(db, record))

        # E. Correlation & Portfolio
        features.update(self._correlation_features(db, record))

        # F. Derived Ratios
        features.update(self._derived_ratios(record, features))

        # Store all features in the raw_enrichment JSON blob
        existing = record.raw_enrichment or {}
        existing["ml_features"] = features
        record.raw_enrichment = existing

        logger.debug(f"Feature enrichment | #{log_id} | {len(features)} features")

    def batch_enrich(self, db: Session, limit: int = 500) -> int:
        """
        Retroactively enrich records missing features.
        Run periodically or on-demand via /api/ml/enrich.
        """
        records = (
            db.query(MLTradeLog)
            .filter(
                MLTradeLog.raw_enrichment.is_(None)
                | ~MLTradeLog.raw_enrichment.contains("ml_features")
            )
            .order_by(desc(MLTradeLog.id))
            .limit(limit)
            .all()
        )

        count = 0
        for record in records:
            try:
                self.enrich_record(db, record.id)
                count += 1
            except Exception as e:
                logger.debug(f"Batch enrich failed for #{record.id}: {e}")

        if count:
            db.commit()
            logger.info(f"Batch enriched {count} ML records")

        return count

    # ─────────────────────────────────────────
    # A. SIGNAL QUALITY FEATURES
    # ─────────────────────────────────────────

    def _signal_quality_features(self, record: MLTradeLog) -> Dict:
        """Features derived from the raw signal itself."""
        f = {}

        sl_pips = record.sl_pips or 0
        tp1_pips = record.tp1_pips or 0
        rr = record.rr_ratio or 0

        # Risk/reward classification
        f["rr_tier"] = (
            "EXCELLENT" if rr >= 3.0 else
            "GOOD" if rr >= 2.0 else
            "FAIR" if rr >= 1.5 else
            "POOR" if rr >= 1.0 else
            "BAD"
        )

        # SL tightness relative to ATR
        atr = record.atr_value or 0
        if atr > 0:
            pip_size, _ = get_pip_info(record.symbol or "")
            atr_pips = atr / pip_size if pip_size else 0
            f["sl_atr_ratio"] = round(sl_pips / atr_pips, 2) if atr_pips > 0 else 0
            f["tp1_atr_ratio"] = round(tp1_pips / atr_pips, 2) if atr_pips > 0 else 0
            f["sl_tight"] = f["sl_atr_ratio"] < 1.0  # tighter than 1 ATR
            f["sl_wide"] = f["sl_atr_ratio"] > 2.5   # wider than 2.5 ATR
        else:
            f["sl_atr_ratio"] = 0
            f["tp1_atr_ratio"] = 0
            f["sl_tight"] = False
            f["sl_wide"] = False

        # Signal type strength
        alert = record.alert_type or ""
        f["is_plus_signal"] = "plus" in alert.lower()
        f["is_confirmation"] = "confirmation" in alert.lower()
        f["is_contrarian"] = "contrarian" in alert.lower()
        f["is_exit_signal"] = "exit" in alert.lower()

        # Consensus quality
        f["consensus_above_7"] = (record.consensus_score or 0) >= 7
        f["consensus_below_4"] = (record.consensus_score or 0) < 4
        f["claude_high_conf"] = (record.claude_confidence or 0) >= 0.75
        f["claude_low_conf"] = (record.claude_confidence or 0) < 0.5

        # Pipeline agreement score (do ML + consensus + Claude all agree?)
        ml_bullish = (record.ml_score or 0) > 0.6
        consensus_bullish = (record.consensus_score or 0) >= 5
        claude_bullish = record.claude_decision in ["EXECUTE", "REDUCE"]
        agreement = sum([ml_bullish, consensus_bullish, claude_bullish])
        f["pipeline_agreement"] = agreement  # 0-3
        f["full_agreement"] = agreement == 3

        return f

    # ─────────────────────────────────────────
    # B. MARKET REGIME FEATURES
    # ─────────────────────────────────────────

    def _market_regime_features(self, record: MLTradeLog) -> Dict:
        """Features about current market conditions."""
        f = {}

        vol = record.volatility_regime or "NORMAL"
        f["vol_is_low"] = vol == "LOW"
        f["vol_is_high"] = vol in ["HIGH", "EXTREME"]
        f["vol_is_extreme"] = vol == "EXTREME"

        # RSI zones
        rsi = record.rsi_value
        if rsi is not None:
            f["rsi_overbought"] = rsi > 70
            f["rsi_oversold"] = rsi < 30
            f["rsi_neutral"] = 40 <= rsi <= 60
            f["rsi_value"] = round(rsi, 1)
        else:
            f["rsi_overbought"] = False
            f["rsi_oversold"] = False
            f["rsi_neutral"] = True
            f["rsi_value"] = 50.0

        # Direction vs RSI alignment
        direction = (record.direction or "").upper()
        if direction in ["LONG", "BUY"]:
            f["rsi_supports_direction"] = (rsi or 50) < 60  # not overbought
        else:
            f["rsi_supports_direction"] = (rsi or 50) > 40  # not oversold

        return f

    # ─────────────────────────────────────────
    # C. HISTORICAL PERFORMANCE FEATURES
    # ─────────────────────────────────────────

    def _historical_features(self, db: Session, record: MLTradeLog) -> Dict:
        """Lookback features from recent trade history."""
        f = {}
        symbol = record.symbol
        desk_id = record.desk_id
        now = record.created_at or datetime.now(timezone.utc)

        # Last 20 completed trades for this desk
        recent = (
            db.query(MLTradeLog)
            .filter(
                MLTradeLog.desk_id == desk_id,
                MLTradeLog.outcome.isnot(None),
                MLTradeLog.created_at < now,
            )
            .order_by(desc(MLTradeLog.created_at))
            .limit(20)
            .all()
        )

        if recent:
            wins = sum(1 for r in recent if r.outcome == "WIN")
            total = len(recent)
            f["desk_recent_win_rate"] = round(wins / total * 100, 1)
            f["desk_recent_trades"] = total

            # Recent avg pnl
            pnls = [r.pnl_pips for r in recent if r.pnl_pips is not None]
            f["desk_recent_avg_pnl"] = round(sum(pnls) / len(pnls), 1) if pnls else 0

            # Current streak
            streak = 0
            streak_type = None
            for r in recent:
                if streak_type is None:
                    streak_type = r.outcome
                    streak = 1
                elif r.outcome == streak_type:
                    streak += 1
                else:
                    break
            f["current_streak"] = streak
            f["streak_is_winning"] = streak_type == "WIN"
            f["streak_is_losing"] = streak_type == "LOSS"
        else:
            f["desk_recent_win_rate"] = 50.0
            f["desk_recent_trades"] = 0
            f["desk_recent_avg_pnl"] = 0
            f["current_streak"] = 0
            f["streak_is_winning"] = False
            f["streak_is_losing"] = False

        # Symbol-specific recent performance (last 10)
        sym_recent = (
            db.query(MLTradeLog)
            .filter(
                MLTradeLog.symbol == symbol,
                MLTradeLog.outcome.isnot(None),
                MLTradeLog.created_at < now,
            )
            .order_by(desc(MLTradeLog.created_at))
            .limit(10)
            .all()
        )

        if sym_recent:
            sym_wins = sum(1 for r in sym_recent if r.outcome == "WIN")
            f["symbol_recent_win_rate"] = round(sym_wins / len(sym_recent) * 100, 1)
            sym_pnls = [r.pnl_pips for r in sym_recent if r.pnl_pips is not None]
            f["symbol_recent_avg_pnl"] = round(sum(sym_pnls) / len(sym_pnls), 1) if sym_pnls else 0
        else:
            f["symbol_recent_win_rate"] = 50.0
            f["symbol_recent_avg_pnl"] = 0

        # OniAI vs executed comparison (last 30 days)
        month_ago = now - timedelta(days=30)
        oniai_wins = db.query(func.count(MLTradeLog.id)).filter(
            MLTradeLog.desk_id == desk_id,
            MLTradeLog.is_oniai == True,
            MLTradeLog.outcome == "WIN",
            MLTradeLog.created_at >= month_ago,
        ).scalar()
        oniai_total = db.query(func.count(MLTradeLog.id)).filter(
            MLTradeLog.desk_id == desk_id,
            MLTradeLog.is_oniai == True,
            MLTradeLog.outcome.isnot(None),
            MLTradeLog.created_at >= month_ago,
        ).scalar()

        f["oniai_30d_win_rate"] = round(oniai_wins / oniai_total * 100, 1) if oniai_total > 0 else 50.0
        f["oniai_30d_count"] = oniai_total

        return f

    # ─────────────────────────────────────────
    # D. TIME PATTERN FEATURES
    # ─────────────────────────────────────────

    def _time_pattern_features(self, db: Session, record: MLTradeLog) -> Dict:
        """Features from time-of-day and day-of-week patterns."""
        f = {}
        hour = record.hour_utc or 0
        dow = record.day_of_week or 0

        # Session buckets
        f["is_asian"] = 0 <= hour < 7
        f["is_london"] = 7 <= hour < 12
        f["is_overlap"] = 12 <= hour < 16
        f["is_new_york"] = 16 <= hour < 21
        f["is_late_session"] = hour >= 21

        # Day of week
        f["is_monday"] = dow == 0
        f["is_friday"] = dow == 4
        f["is_midweek"] = dow in [1, 2, 3]  # Tue/Wed/Thu

        # Historical win rate for this session + symbol combo
        session = record.session
        symbol = record.symbol

        session_perf = (
            db.query(
                func.count(MLTradeLog.id).label("total"),
                func.sum(
                    func.cast(MLTradeLog.outcome == "WIN", type_=None)
                ).label("wins"),
            )
            .filter(
                MLTradeLog.symbol == symbol,
                MLTradeLog.session == session,
                MLTradeLog.outcome.isnot(None),
            )
            .first()
        )

        if session_perf and session_perf.total and session_perf.total > 5:
            wins = session_perf.wins or 0
            f["session_symbol_win_rate"] = round(wins / session_perf.total * 100, 1)
            f["session_symbol_trades"] = session_perf.total
        else:
            f["session_symbol_win_rate"] = 50.0
            f["session_symbol_trades"] = 0

        # Hour-of-day win rate for this desk
        hour_perf = (
            db.query(func.count(MLTradeLog.id))
            .filter(
                MLTradeLog.desk_id == record.desk_id,
                MLTradeLog.hour_utc == hour,
                MLTradeLog.outcome == "WIN",
            )
            .scalar()
        )
        hour_total = (
            db.query(func.count(MLTradeLog.id))
            .filter(
                MLTradeLog.desk_id == record.desk_id,
                MLTradeLog.hour_utc == hour,
                MLTradeLog.outcome.isnot(None),
            )
            .scalar()
        )
        f["hour_win_rate"] = round(hour_perf / hour_total * 100, 1) if hour_total > 5 else 50.0

        return f

    # ─────────────────────────────────────────
    # E. CORRELATION & PORTFOLIO FEATURES
    # ─────────────────────────────────────────

    def _correlation_features(self, db: Session, record: MLTradeLog) -> Dict:
        """Features about current portfolio exposure."""
        f = {}

        # Total open positions across all desks
        total_open = (
            db.query(func.count(Trade.id))
            .filter(Trade.status.in_(["EXECUTED", "OPEN"]))
            .scalar()
        )
        f["total_open_positions"] = total_open
        f["portfolio_crowded"] = total_open >= 10

        # Same-symbol open positions
        same_sym_open = (
            db.query(func.count(Trade.id))
            .filter(
                Trade.status.in_(["EXECUTED", "OPEN"]),
                Trade.symbol == record.symbol,
            )
            .scalar()
        )
        f["same_symbol_open"] = same_sym_open
        f["symbol_already_open"] = same_sym_open > 0

        # Correlation group exposure
        for group_name, group in CORRELATION_GROUPS.items():
            if record.symbol in group.get("symbols", []):
                group_open = (
                    db.query(func.count(Trade.id))
                    .filter(
                        Trade.status.in_(["EXECUTED", "OPEN"]),
                        Trade.symbol.in_(group["symbols"]),
                    )
                    .scalar()
                )
                f["corr_group_open"] = group_open
                f["corr_group_name"] = group_name
                f["corr_group_crowded"] = group_open >= 2
                break
        else:
            f["corr_group_open"] = 0
            f["corr_group_name"] = None
            f["corr_group_crowded"] = False

        # Portfolio daily PnL state
        from app.models.desk_state import DeskState
        total_daily_pnl = (
            db.query(func.sum(DeskState.daily_pnl)).scalar() or 0
        )
        f["portfolio_daily_pnl"] = round(total_daily_pnl, 2)
        f["portfolio_in_profit"] = total_daily_pnl > 0
        f["portfolio_in_drawdown"] = total_daily_pnl < -1000

        return f

    # ─────────────────────────────────────────
    # F. DERIVED RATIOS & COMPOSITE SCORES
    # ─────────────────────────────────────────

    def _derived_ratios(self, record: MLTradeLog, features: Dict) -> Dict:
        """Composite scores combining multiple features."""
        f = {}

        # Signal quality score (0-100)
        quality = 50  # baseline

        # R:R bonus
        rr_tier = features.get("rr_tier", "FAIR")
        quality += {"EXCELLENT": 20, "GOOD": 10, "FAIR": 0, "POOR": -10, "BAD": -20}.get(rr_tier, 0)

        # Pipeline agreement bonus
        agreement = features.get("pipeline_agreement", 1)
        quality += (agreement - 1) * 15  # +15 for 2 agree, +30 for all 3

        # Volatility fit
        if features.get("vol_is_extreme"):
            quality -= 15
        elif features.get("vol_is_low"):
            quality -= 10

        # Session fit
        if features.get("is_overlap"):
            quality += 10  # London-NY overlap is best
        elif features.get("is_late_session"):
            quality -= 10

        # Historical desk performance
        desk_wr = features.get("desk_recent_win_rate", 50)
        if desk_wr > 60:
            quality += 10
        elif desk_wr < 40:
            quality -= 10

        # Streak penalty
        if features.get("streak_is_losing") and features.get("current_streak", 0) >= 3:
            quality -= 15

        f["signal_quality_score"] = max(0, min(100, quality))

        # Confidence composite (combine Claude + ML + consensus)
        claude_conf = record.claude_confidence or 0.5
        ml_score = record.ml_score or 0.5
        consensus = (record.consensus_score or 5) / 10
        f["composite_confidence"] = round(
            (claude_conf * 0.4 + ml_score * 0.3 + consensus * 0.3), 3
        )

        # Risk-adjusted expectancy estimate
        # E = (win_rate × avg_win) - (loss_rate × avg_loss)
        wr = features.get("desk_recent_win_rate", 50) / 100
        avg_win = features.get("desk_recent_avg_pnl", 20) if features.get("desk_recent_avg_pnl", 0) > 0 else 20
        avg_loss = abs(features.get("desk_recent_avg_pnl", -10)) if features.get("desk_recent_avg_pnl", 0) < 0 else 10
        f["expected_value_pips"] = round(wr * avg_win - (1 - wr) * avg_loss, 1)

        # Trade-or-skip recommendation (binary for ML target engineering)
        f["should_trade"] = (
            f["signal_quality_score"] >= 55
            and f["composite_confidence"] >= 0.55
            and not features.get("vol_is_extreme")
            and not features.get("portfolio_in_drawdown")
        )

        return f
