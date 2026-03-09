"""
Server-Side Trade Simulator
Monitors approved trades using live price data.
No MT5 or EA required — runs entirely on Railway.

Picks up DECIDED + ONIAI_OPEN trades, fetches prices,
monitors SL/TP/trailing stops, reports results to Telegram.
"""
import asyncio
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

from sqlalchemy.orm import Session

from app.config import (
    DESKS, CAPITAL_PER_ACCOUNT,
    PORTFOLIO_CAPITAL_PER_DESK, get_pip_info,
)
from app.database import SessionLocal
from app.models.trade import Trade
from app.models.signal import Signal
from app.models.desk_state import DeskState
from app.services.telegram_bot import TelegramBot

logger = logging.getLogger("TradingSystem.Simulator")


class ServerSimulator:
    """
    Runs as a background task on Railway.
    Monitors all open sim/OniAI trades using live prices.
    """

    def __init__(self):
        from app.services.price_service import PriceService
        self.telegram = TelegramBot()
        self.price_service = PriceService()
        self.running = True

        # In-memory tracking for trailing stops and TP1 hits
        self.trade_state: Dict[int, Dict] = {}

    # ─────────────────────────────────────────
    # PRICE FETCHING (delegated to PriceService)
    # ─────────────────────────────────────────

    async def _get_price(self, symbol: str) -> Optional[float]:
        """Get current price via multi-provider service."""
        return await self.price_service.get_price(symbol)

    # ─────────────────────────────────────────
    # PIP CALCULATIONS
    # ─────────────────────────────────────────

    def _get_pip_info(self, symbol: str):
        """Get pip size and value for a symbol — delegates to config."""
        return get_pip_info(symbol)

    # ─────────────────────────────────────────
    # TRADE MONITORING
    # ─────────────────────────────────────────

    def _init_trade_state(self, trade: Trade) -> Dict:
        """Initialize in-memory state for a trade."""
        return {
            "current_sl": trade.stop_loss or 0,
            "high_water": trade.entry_price or 0,
            "low_water": trade.entry_price or 0,
            "tp1_hit": False,
            "partial_closed": False,
            "original_sl": trade.stop_loss or 0,
        }

    async def _check_trade(self, trade: Trade, price: float, db: Session):
        """Check a single trade against current price."""
        if not price or not trade.entry_price:
            return

        is_oniai = (trade.close_reason or "").startswith("ONIAI") or trade.status == "ONIAI_OPEN"

        # ── OniAI trades: RAW SL/TP only ──
        # No trailing, no breakeven, no partial close, no pyramiding.
        # Cleanest data to measure whether the filter was right to block.
        if is_oniai:
            await self._check_trade_raw(trade, price, db)
            return

        # ── Executed trades: full management ──
        # Get or init state
        if trade.id not in self.trade_state:
            self.trade_state[trade.id] = self._init_trade_state(trade)

        state = self.trade_state[trade.id]
        entry = trade.entry_price
        sl = state["current_sl"]
        tp1 = trade.take_profit_1 or 0
        tp2 = getattr(trade, "take_profit_2", 0) or 0
        is_long = (trade.direction or "").upper() in ["LONG", "BUY"]
        pip_size, pip_value = self._get_pip_info(trade.symbol)

        # Update water marks
        if is_long and price > state["high_water"]:
            state["high_water"] = price
        if not is_long and price < state["low_water"]:
            state["low_water"] = price

        # Check TP1 hit → partial close + move SL to breakeven
        if not state["tp1_hit"] and tp1 > 0:
            tp1_reached = (is_long and price >= tp1) or (not is_long and price <= tp1)
            if tp1_reached:
                state["tp1_hit"] = True
                buffer = 2 * pip_size
                state["current_sl"] = (entry + buffer) if is_long else (entry - buffer)
                sl = state["current_sl"]

                logger.info(
                    f"SIM #{trade.id} | TP1 HIT @ {tp1} | "
                    f"SL moved to breakeven: {sl}"
                )

        # Trailing stop after TP1
        if state["tp1_hit"]:
            trail_dist = abs(entry - state["original_sl"])
            if trail_dist > 0:
                if is_long:
                    new_sl = state["high_water"] - trail_dist
                    if new_sl > state["current_sl"]:
                        state["current_sl"] = new_sl
                        sl = new_sl
                else:
                    new_sl = state["low_water"] + trail_dist
                    if new_sl < state["current_sl"]:
                        state["current_sl"] = new_sl
                        sl = new_sl

        # Check SL hit
        hit_sl = False
        if sl > 0:
            if is_long and price <= sl:
                hit_sl = True
            if not is_long and price >= sl:
                hit_sl = True

        # Check TP2 hit
        hit_tp2 = False
        if state["tp1_hit"] and tp2 > 0:
            if is_long and price >= tp2:
                hit_tp2 = True
            if not is_long and price <= tp2:
                hit_tp2 = True

        # Check time exit
        time_expired = False
        desk = DESKS.get(trade.desk_id, {})
        max_hold = desk.get("max_hold_hours", 24)
        open_time = trade.opened_at or trade.closed_at  # fallback
        if open_time:
            elapsed = (datetime.now(timezone.utc) - open_time).total_seconds()
            if elapsed >= max_hold * 3600:
                time_expired = True

        # Close if triggered
        if hit_sl or hit_tp2 or time_expired:
            exit_price = price
            if hit_tp2:
                exit_price = tp2
            elif hit_sl:
                exit_price = sl

            pnl_pips = ((exit_price - entry) if is_long else (entry - exit_price)) / pip_size
            lot = trade.lot_size or 0.1
            pnl_dollars = pnl_pips * pip_value * lot

            reason = "SRV_"
            if hit_tp2:
                reason += "TP2_HIT"
            elif time_expired:
                reason += "TIME_EXIT"
            elif state["tp1_hit"]:
                reason += "TRAILING_SL"
            else:
                reason += "SL_HIT"

            trade.status = "SRV_CLOSED"

            trade.exit_price = exit_price
            trade.pnl_dollars = round(pnl_dollars, 2)
            trade.pnl_pips = round(pnl_pips, 1)
            trade.close_reason = reason
            trade.closed_at = datetime.now(timezone.utc)

            # Update desk state
            desk_state = db.query(DeskState).filter(
                DeskState.desk_id == trade.desk_id
            ).first()
            if desk_state:
                desk_state.open_positions = max(0, (desk_state.open_positions or 0) - 1)
                desk_state.daily_pnl = (desk_state.daily_pnl or 0) + pnl_dollars
                if pnl_dollars < 0:
                    desk_state.daily_loss = (desk_state.daily_loss or 0) + pnl_dollars
                    desk_state.consecutive_losses = (desk_state.consecutive_losses or 0) + 1
                else:
                    desk_state.consecutive_losses = 0

            db.commit()

            # Log outcome for ML training
            try:
                from app.services.ml_data_logger import MLDataLogger
                duration = 0
                if trade.opened_at:
                    duration = (datetime.now(timezone.utc) - trade.opened_at).total_seconds() / 60
                MLDataLogger.label_outcome(
                    db=db,
                    trade_id=trade.id,
                    signal_id=trade.signal_id,
                    exit_price=exit_price,
                    exit_reason=reason,
                    pnl_pips=pnl_pips,
                    pnl_dollars=pnl_dollars,
                    trade_duration_minutes=duration,
                    max_favorable_pips=abs(
                        (state["high_water"] - entry) if is_long
                        else (entry - state["low_water"])
                    ) / pip_size if pip_size else None,
                    max_adverse_pips=abs(
                        (entry - state["low_water"]) if is_long
                        else (state["high_water"] - entry)
                    ) / pip_size if pip_size else None,
                )
            except Exception as e:
                logger.debug(f"ML outcome log failed: {e}")

            # Clean up state
            if trade.id in self.trade_state:
                del self.trade_state[trade.id]

            logger.info(
                f"SIM CLOSED 🖥️ | #{trade.id} | {trade.symbol} "
                f"{trade.direction} | Entry: {entry} | Exit: {exit_price} | "
                f"PnL: ${pnl_dollars:+.2f} ({pnl_pips:+.1f}p) | {reason}"
            )

            # Log outcome for ML training
            try:
                from app.services.ml_data_logger import MLDataLogger
                hold_mins = None
                if trade.opened_at:
                    hold_mins = (trade.closed_at - trade.opened_at).total_seconds() / 60
                MLDataLogger().log_outcome(
                    db, signal_id=trade.signal_id, trade_id=trade.id,
                    pnl_pips=round(pnl_pips, 1), pnl_dollars=round(pnl_dollars, 2),
                    exit_price=exit_price, exit_reason=reason,
                    hold_time_minutes=hold_mins, profile="SRV_100",
                )
                db.commit()
            except Exception as e:
                logger.debug(f"ML outcome log failed: {e}")

            # Send Telegram
            try:
                await self.telegram.notify_trade_exit(
                    symbol=trade.symbol,
                    desk_id=trade.desk_id,
                    pnl=pnl_dollars,
                    reason=reason,
                )
            except Exception as e:
                logger.error(f"Telegram sim close failed: {e}")

    # ─────────────────────────────────────────
    # OniAI RAW TRADE CHECK
    # ─────────────────────────────────────────

    async def _check_trade_raw(self, trade: Trade, price: float, db: Session):
        """
        Check OniAI trade with RAW SL/TP only.
        No trailing stop, no breakeven, no partial close.
        Pure: did the original signal hit SL or TP1?
        """
        entry = trade.entry_price
        sl = trade.stop_loss or 0
        tp1 = trade.take_profit_1 or 0
        is_long = (trade.direction or "").upper() in ["LONG", "BUY"]
        pip_size, pip_value = self._get_pip_info(trade.symbol)

        # Check SL hit
        hit_sl = False
        if sl > 0:
            if is_long and price <= sl:
                hit_sl = True
            if not is_long and price >= sl:
                hit_sl = True

        # Check TP1 hit (full close — no partial)
        hit_tp = False
        if tp1 > 0:
            if is_long and price >= tp1:
                hit_tp = True
            if not is_long and price <= tp1:
                hit_tp = True

        # Check time exit
        time_expired = False
        desk = DESKS.get(trade.desk_id, {})
        max_hold = desk.get("max_hold_hours", 24)
        open_time = trade.opened_at
        if open_time:
            elapsed = (datetime.now(timezone.utc) - open_time).total_seconds()
            if elapsed >= max_hold * 3600:
                time_expired = True

        if not hit_sl and not hit_tp and not time_expired:
            return

        # Determine exit price
        if hit_tp:
            exit_price = tp1
        elif hit_sl:
            exit_price = sl
        else:
            exit_price = price  # time exit at market

        # Calculate P&L
        pnl_pips = ((exit_price - entry) if is_long else (entry - exit_price)) / pip_size
        lot = trade.lot_size or 0.1
        pnl_dollars = pnl_pips * pip_value * lot

        # Build reason
        if hit_tp:
            reason = "ONIAI_TP1_HIT"
        elif hit_sl:
            reason = "ONIAI_SL_HIT"
        else:
            reason = "ONIAI_TIME_EXIT"

        # Close the trade
        trade.exit_price = round(exit_price, 5)
        trade.pnl_dollars = round(pnl_dollars, 2)
        trade.pnl_pips = round(pnl_pips, 1)
        trade.close_reason = reason
        trade.status = "ONIAI_CLOSED"
        trade.closed_at = datetime.now(timezone.utc)
        db.commit()

        # Log outcome for ML training
        try:
            from app.services.ml_data_logger import MLDataLogger
            duration = 0
            if trade.opened_at:
                duration = (datetime.now(timezone.utc) - trade.opened_at).total_seconds() / 60
            MLDataLogger.label_outcome(
                db=db,
                trade_id=trade.id,
                signal_id=trade.signal_id,
                exit_price=exit_price,
                exit_reason=reason,
                pnl_pips=pnl_pips,
                pnl_dollars=pnl_dollars,
                trade_duration_minutes=duration,
            )
        except Exception as e:
            logger.debug(f"ML OniAI outcome log failed: {e}")

        logger.info(
            f"OniAI CLOSED 🤖 | #{trade.id} | {trade.symbol} "
            f"{trade.direction} | Entry: {entry} | Exit: {exit_price} | "
            f"PnL: ${pnl_dollars:+.2f} ({pnl_pips:+.1f}p) | {reason}"
        )

        # Log outcome for ML training
        try:
            from app.services.ml_data_logger import MLDataLogger
            duration = 0
            if trade.opened_at:
                duration = (datetime.now(timezone.utc) - trade.opened_at).total_seconds() / 60
            MLDataLogger().log_outcome(
                db=db, signal_id=trade.signal_id, trade_id=trade.id,
                exit_price=exit_price, exit_reason=reason,
                pnl_pips=round(pnl_pips, 1), pnl_dollars=round(pnl_dollars, 2),
                duration_minutes=duration,
            )
            db.commit()
        except Exception as e:
            logger.debug(f"ML OniAI outcome log failed: {e}")

        try:
            await self.telegram.notify_trade_exit(
                symbol=trade.symbol,
                desk_id=trade.desk_id,
                pnl=pnl_dollars,
                reason=reason,
            )
        except Exception as e:
            logger.error(f"Telegram OniAI close failed: {e}")

        # ML outcome logging
        try:
            from app.services.ml_data_logger import MLDataLogger
            hold_min = 0
            if trade.opened_at:
                hold_min = (datetime.now(timezone.utc) - trade.opened_at).total_seconds() / 60
            MLDataLogger.log_outcome(
                db=db, trade_id=trade.id, signal_id=trade.signal_id,
                pnl_pips=pnl_pips, pnl_dollars=pnl_dollars,
                exit_price=exit_price, exit_reason=reason,
                hold_minutes=hold_min, profile="ONIAI",
            )
        except Exception as e:
            logger.debug(f"ML outcome log failed: {e}")

    # ─────────────────────────────────────────
    # MAIN LOOP
    # ─────────────────────────────────────────

    async def run(self):
        """Main simulation loop. Checks open trades every 30 seconds."""
        logger.info("Server-side simulator started | Checking every 30s")

        while self.running:
            try:
                db = SessionLocal()
                try:
                    # Get all open sim/OniAI trades
                    open_trades = (
                        db.query(Trade)
                        .filter(
                            Trade.status.in_([
                                "EXECUTED", "OPEN",
                                "ONIAI_OPEN",
                            ])
                        )
                        .all()
                    )

                    if open_trades:
                        # Get unique symbols
                        symbols = set(t.symbol for t in open_trades if t.symbol)

                        # Fetch prices for all symbols
                        prices = {}
                        for sym in symbols:
                            price = await self._get_price(sym)
                            if price:
                                prices[sym] = price

                        # Check each trade
                        for trade in open_trades:
                            price = prices.get(trade.symbol)
                            if price:
                                await self._check_trade(trade, price, db)

                        if prices:
                            logger.debug(
                                f"Sim check: {len(open_trades)} trades, "
                                f"{len(prices)} prices fetched"
                            )
                finally:
                    db.close()

            except Exception as e:
                logger.error(f"Simulator error: {e}")

            await asyncio.sleep(30)

    async def stop(self):
        """Stop the simulator."""
        self.running = False
        await self.price_service.close()
        await self.telegram.close()
        logger.info("Server-side simulator stopped")
