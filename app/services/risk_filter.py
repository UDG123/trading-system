"""
Hard Risk Filter
Independent safety layer that runs AFTER Claude's decision.
This mirrors the EA-side risk checks but runs server-side as a second line of defense.
Any trade that passes here will also be verified by the MT5 EA independently.
"""
import logging
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple, List

from sqlalchemy.orm import Session
from sqlalchemy import func

from app.config import (
    DESKS, CAPITAL_PER_ACCOUNT, MAX_DAILY_LOSS_PER_ACCOUNT,
    MAX_TOTAL_LOSS_PER_ACCOUNT, FIRM_WIDE_DAILY_DRAWDOWN_HALT,
    CONSECUTIVE_LOSS_RULES, SESSION_WINDOWS, CORRELATION_GROUPS,
    MAX_CORRELATED_POSITIONS,
)
from app.models.trade import Trade
from app.models.desk_state import DeskState

logger = logging.getLogger("TradingSystem.RiskFilter")


class HardRiskFilter:
    """
    Server-side risk filter. Independent of Claude's decision.
    Provides firm-wide risk state and final trade validation.
    """

    def get_desk_state(self, db: Session, desk_id: str) -> Dict:
        """
        Get current desk state for pipeline consumption.
        Creates desk state record if it doesn't exist.
        """
        state = db.query(DeskState).filter(DeskState.desk_id == desk_id).first()

        if not state:
            state = DeskState(
                desk_id=desk_id,
                is_active=True,
                is_paused=False,
                trades_today=0,
                daily_pnl=0.0,
                daily_loss=0.0,
                consecutive_losses=0,
                size_modifier=1.0,
                open_positions=0,
                last_reset_date=datetime.now(timezone.utc).strftime("%Y-%m-%d"),
            )
            db.add(state)
            db.commit()
            db.refresh(state)

        # Check if we need a daily reset
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if state.last_reset_date != today:
            state.trades_today = 0
            state.daily_pnl = 0.0
            state.daily_loss = 0.0
            state.consecutive_losses = 0
            state.size_modifier = 1.0
            state.is_paused = False
            state.is_active = True
            state.last_reset_date = today
            db.commit()

        # Check pause expiry
        if state.is_paused and state.pause_until:
            if datetime.now(timezone.utc) >= state.pause_until:
                state.is_paused = False
                state.pause_until = None
                db.commit()

        # Check max trades
        desk = DESKS.get(desk_id, {})
        max_trades = desk.get("max_trades_day", desk.get("max_simultaneous", 999))
        max_trades_hit = state.trades_today >= max_trades

        return {
            "is_active": state.is_active,
            "is_paused": state.is_paused,
            "trades_today": state.trades_today,
            "daily_pnl": state.daily_pnl,
            "daily_loss": state.daily_loss,
            "consecutive_losses": state.consecutive_losses,
            "size_modifier": state.size_modifier,
            "open_positions": state.open_positions,
            "max_trades_hit": max_trades_hit,
        }

    def get_firm_risk(self, db: Session) -> Dict:
        """
        Calculate firm-wide risk metrics across all desks.
        """
        total_daily_pnl = 0.0
        total_daily_loss = 0.0
        correlated_desks = []

        desk_states = db.query(DeskState).all()

        # Aggregate firm PnL
        for state in desk_states:
            total_daily_pnl += state.daily_pnl or 0
            total_daily_loss += abs(state.daily_loss or 0)

        # Check firm-wide drawdown
        firm_drawdown_exceeded = total_daily_loss >= FIRM_WIDE_DAILY_DRAWDOWN_HALT

        # Determine drawdown level
        if total_daily_loss >= FIRM_WIDE_DAILY_DRAWDOWN_HALT:
            drawdown_level = "CRITICAL"
        elif total_daily_loss >= FIRM_WIDE_DAILY_DRAWDOWN_HALT * 0.75:
            drawdown_level = "WARNING"
        elif total_daily_loss >= FIRM_WIDE_DAILY_DRAWDOWN_HALT * 0.5:
            drawdown_level = "ELEVATED"
        else:
            drawdown_level = "NORMAL"

        # TODO: Check correlated exposure across desks
        # (needs open positions data from MT5 — Phase 3)

        return {
            "total_daily_pnl": round(total_daily_pnl, 2),
            "total_daily_loss": round(total_daily_loss, 2),
            "firm_drawdown_exceeded": firm_drawdown_exceeded,
            "drawdown_level": drawdown_level,
            "correlated_desks": correlated_desks,
        }

    def validate_trade(
        self,
        decision: Dict,
        signal_data: Dict,
        desk_state: Dict,
        desk_id: str,
        db: Session = None,
    ) -> Tuple[bool, Optional[str], Dict]:
        """
        Final validation before sending to MT5.
        Returns (approved, rejection_reason, adjusted_params).
        """
        desk = DESKS.get(desk_id, {})

        # ── 1. Decision must be EXECUTE or REDUCE ──
        if decision.get("decision") == "SKIP":
            return False, "CTO decision: SKIP", {}

        # ── 2. Must have stop loss ──
        if signal_data.get("sl1") is None:
            return False, "No stop loss — trade rejected", {}

        # ── 3. Session enforcement ──
        session_ok, session_msg = self._check_session(desk_id, desk)
        if not session_ok:
            return False, session_msg, {}

        # ── 4. Correlation filter ──
        if db:
            corr_ok, corr_msg = self._check_correlation(
                db, signal_data.get("symbol"), signal_data.get("direction")
            )
            if not corr_ok:
                return False, corr_msg, {}

        # ── 5. Calculate position size ──
        risk_pct = desk.get("risk_pct", 1.0)
        size_mult = decision.get("size_multiplier", 1.0)
        desk_modifier = desk_state.get("size_modifier", 1.0)

        effective_risk_pct = risk_pct * size_mult * desk_modifier

        # Cap at desk's base risk
        effective_risk_pct = min(effective_risk_pct, risk_pct)

        risk_dollars = CAPITAL_PER_ACCOUNT * (effective_risk_pct / 100)

        # ── 6. Validate risk doesn't exceed daily limit ──
        remaining_daily_budget = MAX_DAILY_LOSS_PER_ACCOUNT - abs(
            desk_state.get("daily_loss", 0)
        )
        if risk_dollars > remaining_daily_budget:
            risk_dollars = remaining_daily_budget * 0.8  # leave 20% buffer
            if risk_dollars <= 0:
                return False, "Daily loss budget exhausted", {}

        # ── 7. Build trade parameters ──
        trade_params = {
            "desk_id": desk_id,
            "symbol": signal_data.get("symbol"),
            "direction": signal_data.get("direction"),
            "price": signal_data.get("price"),
            "timeframe": signal_data.get("timeframe"),
            "alert_type": signal_data.get("alert_type"),
            "trend": enrichment.get("trend", "UNKNOWN"),
            "rsi": enrichment.get("rsi"),
            "volatility_regime": enrichment.get("volatility_regime"),
            "risk_pct": round(effective_risk_pct, 4),
            "risk_dollars": round(risk_dollars, 2),
            "stop_loss": signal_data.get("sl1"),
            "take_profit_1": signal_data.get("tp1"),
            "take_profit_2": signal_data.get("tp2"),
            "trailing_stop_pips": desk.get("trailing_stop_pips"),
            "max_hold_hours": desk.get("max_hold_hours"),
            "size_multiplier": round(size_mult * desk_modifier, 4),
            "claude_decision": decision.get("decision"),
            "claude_reasoning": decision.get("reasoning"),
            "confidence": decision.get("confidence"),
        }

        logger.info(
            f"Trade APPROVED | {desk_id} | {signal_data.get('symbol')} "
            f"{signal_data.get('direction')} | Risk: ${risk_dollars:.2f} "
            f"({effective_risk_pct:.2f}%)"
        )

        return True, None, trade_params

    # ─────────────────────────────────────────
    # SESSION ENFORCEMENT
    # ─────────────────────────────────────────

    def _check_session(self, desk_id: str, desk: Dict) -> Tuple[bool, str]:
        """
        Hard-block trades outside allowed sessions.
        Returns (allowed, message).
        """
        sessions = desk.get("sessions", ["ALL"])
        if "ALL" in sessions:
            return True, ""

        now_utc = datetime.now(timezone.utc)
        current_hour = now_utc.hour

        for session_name in sessions:
            window = SESSION_WINDOWS.get(session_name)
            if not window:
                continue

            start = window["start"]
            end = window["end"]

            # Handle sessions that wrap midnight (e.g., Sydney 21-6)
            if start > end:
                if current_hour >= start or current_hour < end:
                    logger.debug(
                        f"Session OK: {session_name} ({start}:00-{end}:00 UTC), "
                        f"current: {current_hour}:00 UTC"
                    )
                    return True, ""
            else:
                if start <= current_hour < end:
                    logger.debug(
                        f"Session OK: {session_name} ({start}:00-{end}:00 UTC), "
                        f"current: {current_hour}:00 UTC"
                    )
                    return True, ""

        session_names = ", ".join(sessions)
        msg = (
            f"Outside trading session for {desk_id}. "
            f"Allowed: {session_names}. Current: {current_hour}:00 UTC"
        )
        logger.info(f"SESSION BLOCK | {msg}")
        return False, msg

    # ─────────────────────────────────────────
    # CORRELATION FILTER
    # ─────────────────────────────────────────

    def _check_correlation(
        self, db: Session, symbol: str, direction: str
    ) -> Tuple[bool, str]:
        """
        Check if taking this trade would create too much correlated exposure.
        Only allows MAX_CORRELATED_POSITIONS per correlation group per direction.
        """
        if not symbol or not direction:
            return True, ""

        # Find which correlation groups this symbol belongs to
        for group_name, group in CORRELATION_GROUPS.items():
            if symbol not in group.get("symbols", []):
                continue

            correlated_symbols = group["symbols"]

            # Count open trades in same direction for correlated symbols
            open_correlated = (
                db.query(Trade)
                .filter(
                    Trade.symbol.in_(correlated_symbols),
                    Trade.direction == direction,
                    Trade.status.in_(["OPEN", "EXECUTED"]),
                )
                .count()
            )

            if open_correlated >= MAX_CORRELATED_POSITIONS:
                msg = (
                    f"Correlation limit: {open_correlated} open {direction} "
                    f"positions in '{group_name}' group "
                    f"({', '.join(correlated_symbols)}). "
                    f"Max allowed: {MAX_CORRELATED_POSITIONS}"
                )
                logger.info(f"CORRELATION BLOCK | {msg}")
                return False, msg

        return True, ""

    def get_recent_signals(
        self, db: Session, symbol: str, hours: int = 24, limit: int = 20
    ):
        """Fetch recent signals for the same symbol (for consensus scoring)."""
        from app.models.signal import Signal

        cutoff = datetime.now(timezone.utc) - __import__("datetime").timedelta(hours=hours)

        signals = (
            db.query(Signal)
            .filter(
                Signal.symbol_normalized == symbol,
                Signal.received_at >= cutoff,
                Signal.is_valid == True,
            )
            .order_by(Signal.received_at.desc())
            .limit(limit)
            .all()
        )

        return [
            {
                "symbol": s.symbol_normalized,
                "timeframe": s.timeframe,
                "alert_type": s.alert_type,
                "direction": s.direction,
                "price": s.price,
                "received_at": s.received_at.isoformat() if s.received_at else None,
            }
            for s in signals
        ]
