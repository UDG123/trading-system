"""
Server-Side Trade Simulator — Batched 2-Provider JIT Architecture
Direct httpx calls to TwelveData (batched) + Finnhub (fallback).
No PriceService dependency. Mirrors institutional NovaQuant feed.

Rate Budget:
  - TwelveData free tier: 8 req/min, 800 req/day
  - Finnhub free tier: 60 req/min
  - Loop sleeps 15s → 4 cycles/min → 4 batched TD calls/min (under 8/min)
  - Finnhub only called for symbols TD misses (typically 0-2 per cycle)

Architecture:
  1. Query DB for open trades. If none → sleep 15s → zero API calls.
  2. Extract unique tickers from open trades only.
  3. BATCH all tickers into ONE TwelveData /price call (comma-separated).
  4. Any tickers TD missed → individual Finnhub /quote calls.
  5. Evaluate SL/TP/trailing/time exits against resolved prices.
  6. Sleep 15s → repeat.
"""
import os
import asyncio
import logging
from datetime import datetime, timezone
from typing import Dict, Optional, Set

import httpx
from sqlalchemy.orm import Session

from app.config import (
    DESKS, CAPITAL_PER_ACCOUNT,
    PORTFOLIO_CAPITAL_PER_DESK, get_pip_info,
)
from app.database import SessionLocal
from app.models.trade import Trade
from app.models.desk_state import DeskState
from app.services.telegram_bot import TelegramBot

logger = logging.getLogger("TradingSystem.Simulator")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# API KEYS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY", "")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY", "")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SYMBOL MAPPERS
# Internal ticker → provider-specific format
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# TwelveData: forex as "EUR/USD", commodities as ticker codes
TD_MAP = {
    # Forex majors
    "EURUSD": "EUR/USD", "USDJPY": "USD/JPY", "GBPUSD": "GBP/USD",
    "USDCHF": "USD/CHF", "AUDUSD": "AUD/USD", "USDCAD": "USD/CAD",
    "NZDUSD": "NZD/USD",
    # Forex crosses
    "EURJPY": "EUR/JPY", "GBPJPY": "GBP/JPY", "AUDJPY": "AUD/JPY",
    "EURGBP": "EUR/GBP", "EURAUD": "EUR/AUD", "GBPAUD": "GBP/AUD",
    "EURCHF": "EUR/CHF", "CADJPY": "CAD/JPY", "NZDJPY": "NZD/JPY",
    "GBPCAD": "GBP/CAD", "AUDCAD": "AUD/CAD", "AUDNZD": "AUD/NZD",
    "CHFJPY": "CHF/JPY", "EURNZD": "EUR/NZD", "GBPNZD": "GBP/NZD",
    # Commodities
    "XAUUSD": "XAU/USD", "XAGUSD": "XAG/USD",
    "WTIUSD": "CL", "XCUUSD": "HG",
    # Indices
    "US30": "DJI", "US100": "IXIC", "NAS100": "IXIC",
    # Crypto
    "BTCUSD": "BTC/USD", "ETHUSD": "ETH/USD",
    "SOLUSD": "SOL/USD", "XRPUSD": "XRP/USD", "LINKUSD": "LINK/USD",
    # US Equities
    "TSLA": "TSLA", "AAPL": "AAPL", "MSFT": "MSFT",
    "NVDA": "NVDA", "AMZN": "AMZN", "META": "META",
    "GOOGL": "GOOGL", "NFLX": "NFLX", "AMD": "AMD",
}

# Reverse mapper: TwelveData response key → internal ticker
# TD returns the symbol we sent as the key in multi-symbol responses
TD_REVERSE = {v: k for k, v in TD_MAP.items()}
# Handle duplicates (US100 and NAS100 both map to IXIC)
# IXIC reverse-maps to whichever was last — we'll handle this in parsing

# Finnhub FREE tier coverage:
#   ✅ US stocks (as-is)
#   ✅ Binance crypto (BINANCE:BTCUSDT format)
#   ❌ OANDA forex (403 Forbidden — requires premium)
#   ❌ Commodities (no free feed)
#   ❌ Index quotes (^DJI etc — limited on free)
FH_MAP = {
    # Crypto via Binance (free)
    "BTCUSD": "BINANCE:BTCUSDT", "ETHUSD": "BINANCE:ETHUSDT",
    "SOLUSD": "BINANCE:SOLUSDT", "XRPUSD": "BINANCE:XRPUSDT",
    "LINKUSD": "BINANCE:LINKUSDT",
    # US Equities (free)
    "TSLA": "TSLA", "AAPL": "AAPL", "MSFT": "MSFT",
    "NVDA": "NVDA", "AMZN": "AMZN", "META": "META",
    "GOOGL": "GOOGL", "NFLX": "NFLX", "AMD": "AMD",
}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONFIGURATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
OPEN_STATUSES = ["EXECUTED", "OPEN", "ONIAI_OPEN", "VIRTUAL_OPEN"]
POLL_INTERVAL = 15  # 15s → 4 cycles/min → under TD's 8/min limit
HTTP_TIMEOUT = 8.0  # seconds per request

# Provider stats (for /providers Telegram command)
_provider_stats = {
    "twelvedata": {"success": 0, "fail": 0, "batch_calls": 0},
    "finnhub": {"success": 0, "fail": 0, "individual_calls": 0},
}


def get_provider_stats() -> Dict:
    """Expose stats for diagnostics/monitoring."""
    return _provider_stats.copy()


class ServerSimulator:
    """
    2-provider JIT trade monitor.
    Batches TwelveData calls, falls back to Finnhub for misses.
    """

    def __init__(self):
        self.telegram = TelegramBot()
        self.client = httpx.AsyncClient(timeout=HTTP_TIMEOUT)
        self.running = True
        self.trade_state: Dict[int, Dict] = {}

        # Last known good prices (stale cache for when both providers fail)
        self._last_prices: Dict[str, float] = {}

        if not TWELVEDATA_API_KEY:
            logger.warning("TWELVEDATA_API_KEY not set — primary provider disabled")
        if not FINNHUB_API_KEY:
            logger.warning("FINNHUB_API_KEY not set — fallback provider disabled")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # PRICE FETCHING — BATCHED TWELVEDATA + FINNHUB FALLBACK
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def _fetch_prices_batch(self, tickers: Set[str]) -> Dict[str, float]:
        """
        Fetch prices for a set of tickers using:
          1. Single batched TwelveData /price call (all tickers at once)
          2. Individual Finnhub /quote calls for any TwelveData misses

        Returns {internal_ticker: price} dict.
        """
        prices: Dict[str, float] = {}
        remaining: Set[str] = set(tickers)

        # ── Step 1: Batched TwelveData ──
        if TWELVEDATA_API_KEY and remaining:
            td_prices = await self._fetch_twelvedata_batch(remaining)
            prices.update(td_prices)
            remaining -= set(td_prices.keys())

            if td_prices:
                logger.debug(
                    f"TD batch: {len(td_prices)}/{len(tickers)} resolved | "
                    f"Missing: {remaining or 'none'}"
                )

        # ── Step 2: Finnhub fallback for misses ──
        if FINNHUB_API_KEY and remaining:
            fh_prices = await self._fetch_finnhub_individual(remaining)
            prices.update(fh_prices)
            still_missing = remaining - set(fh_prices.keys())

            if fh_prices:
                logger.debug(
                    f"FH fallback: {len(fh_prices)}/{len(remaining)} resolved | "
                    f"Still missing: {still_missing or 'none'}"
                )

        # ── Step 3: Use stale cache for anything still missing ──
        final_missing = tickers - set(prices.keys())
        for sym in final_missing:
            stale = self._last_prices.get(sym)
            if stale:
                prices[sym] = stale
                logger.debug(f"Using stale price for {sym}: {stale}")

        # Update stale cache with fresh prices
        self._last_prices.update(prices)

        return prices

    async def _fetch_twelvedata_batch(self, tickers: Set[str]) -> Dict[str, float]:
        """
        Single batched call to TwelveData /price endpoint.
        Sends all symbols comma-separated, gets all prices in one response.

        TwelveData batch response format:
          Single symbol:  {"price": "1.08500"}
          Multi symbol:   {"EUR/USD": {"price": "1.08500"}, "XAU/USD": {"price": "2935.5"}}
        """
        # Map internal tickers to TD format
        td_symbols = []
        td_to_internal = {}  # reverse map for this batch

        for ticker in tickers:
            td_sym = TD_MAP.get(ticker)
            if td_sym:
                td_symbols.append(td_sym)
                td_to_internal[td_sym] = ticker

        if not td_symbols:
            return {}

        symbol_param = ",".join(td_symbols)

        try:
            resp = await self.client.get(
                "https://api.twelvedata.com/price",
                params={"symbol": symbol_param, "apikey": TWELVEDATA_API_KEY},
            )

            _provider_stats["twelvedata"]["batch_calls"] += 1

            if resp.status_code != 200:
                _provider_stats["twelvedata"]["fail"] += 1
                logger.warning(f"TD batch HTTP {resp.status_code}")
                return {}

            data = resp.json()

            # Check for API error response
            if isinstance(data, dict) and data.get("status") == "error":
                _provider_stats["twelvedata"]["fail"] += 1
                logger.warning(f"TD batch error: {data.get('message', 'unknown')}")
                return {}

            prices = {}

            if len(td_symbols) == 1:
                # Single symbol response: {"price": "1.08500"}
                td_sym = td_symbols[0]
                internal = td_to_internal.get(td_sym)
                price_str = data.get("price")
                if price_str and internal:
                    try:
                        price = float(price_str)
                        if price > 0:
                            prices[internal] = price
                            _provider_stats["twelvedata"]["success"] += 1
                    except (ValueError, TypeError):
                        pass
            else:
                # Multi symbol response: {"EUR/USD": {"price": "1.08500"}, ...}
                for td_sym, td_data in data.items():
                    internal = td_to_internal.get(td_sym)
                    if not internal:
                        # Try reverse map for edge cases
                        internal = TD_REVERSE.get(td_sym)
                    if not internal:
                        continue

                    if isinstance(td_data, dict):
                        # Check for per-symbol errors
                        if td_data.get("status") == "error":
                            continue
                        price_str = td_data.get("price")
                    else:
                        continue

                    if price_str:
                        try:
                            price = float(price_str)
                            if price > 0:
                                prices[internal] = price
                                _provider_stats["twelvedata"]["success"] += 1
                        except (ValueError, TypeError):
                            pass

            return prices

        except httpx.TimeoutException:
            _provider_stats["twelvedata"]["fail"] += 1
            logger.warning(f"TD batch timeout ({HTTP_TIMEOUT}s) for {len(td_symbols)} symbols")
            return {}
        except httpx.RequestError as e:
            _provider_stats["twelvedata"]["fail"] += 1
            logger.warning(f"TD batch network error: {e}")
            return {}
        except Exception as e:
            _provider_stats["twelvedata"]["fail"] += 1
            logger.error(f"TD batch unexpected error: {e}")
            return {}

    async def _fetch_finnhub_individual(self, tickers: Set[str]) -> Dict[str, float]:
        """
        Individual Finnhub /quote calls for tickers that TwelveData missed.
        Finnhub allows 60 req/min so individual calls are fine for fallback.
        """
        prices = {}

        for ticker in tickers:
            fh_sym = FH_MAP.get(ticker)
            if not fh_sym:
                continue

            try:
                resp = await self.client.get(
                    "https://finnhub.io/api/v1/quote",
                    params={"symbol": fh_sym, "token": FINNHUB_API_KEY},
                )

                _provider_stats["finnhub"]["individual_calls"] += 1

                if resp.status_code != 200:
                    _provider_stats["finnhub"]["fail"] += 1
                    continue

                data = resp.json()
                # Finnhub returns {"c": current, "h": high, "l": low, "o": open, ...}
                price = data.get("c", 0)
                if price and float(price) > 0:
                    prices[ticker] = float(price)
                    _provider_stats["finnhub"]["success"] += 1
                else:
                    _provider_stats["finnhub"]["fail"] += 1

            except httpx.TimeoutException:
                _provider_stats["finnhub"]["fail"] += 1
                logger.debug(f"FH timeout for {ticker}")
            except httpx.RequestError as e:
                _provider_stats["finnhub"]["fail"] += 1
                logger.debug(f"FH network error for {ticker}: {e}")
            except Exception as e:
                _provider_stats["finnhub"]["fail"] += 1
                logger.debug(f"FH error for {ticker}: {e}")

        return prices

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # TRADE STATE
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def _init_trade_state(self, trade: Trade) -> Dict:
        return {
            "current_sl": trade.stop_loss or 0,
            "high_water": trade.entry_price or 0,
            "low_water": trade.entry_price or 0,
            "tp1_hit": False,
            "partial_closed": False,
            "original_sl": trade.stop_loss or 0,
        }

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # EXECUTED TRADE CHECK (full management)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def _check_executed_trade(self, trade: Trade, price: float, db: Session):
        if not price or not trade.entry_price:
            return

        if trade.id not in self.trade_state:
            self.trade_state[trade.id] = self._init_trade_state(trade)

        state = self.trade_state[trade.id]
        entry = trade.entry_price
        sl = state["current_sl"]
        tp1 = trade.take_profit_1 or 0
        tp2 = getattr(trade, "take_profit_2", 0) or 0
        is_long = (trade.direction or "").upper() in ["LONG", "BUY"]
        pip_size, pip_value = get_pip_info(trade.symbol)

        # Water marks
        if is_long and price > state["high_water"]:
            state["high_water"] = price
        if not is_long and (price < state["low_water"] or state["low_water"] == 0):
            state["low_water"] = price

        # TP1 → partial close + breakeven
        if not state["tp1_hit"] and tp1 > 0:
            tp1_reached = (is_long and price >= tp1) or (not is_long and price <= tp1)
            if tp1_reached:
                state["tp1_hit"] = True
                buffer = 2 * pip_size
                state["current_sl"] = (entry + buffer) if is_long else (entry - buffer)
                sl = state["current_sl"]
                logger.info(f"SIM #{trade.id} | TP1 HIT @ {tp1} | SL → BE: {sl}")

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

        # Exit conditions
        hit_sl = sl > 0 and ((is_long and price <= sl) or (not is_long and price >= sl))
        hit_tp2 = state["tp1_hit"] and tp2 > 0 and (
            (is_long and price >= tp2) or (not is_long and price <= tp2)
        )
        time_expired = self._is_time_expired(trade)

        if not (hit_sl or hit_tp2 or time_expired):
            return

        # Determine exit
        if hit_tp2:
            exit_price, reason = tp2, "SRV_TP2_HIT"
        elif hit_sl:
            exit_price = sl
            reason = "SRV_TRAILING_SL" if state["tp1_hit"] else "SRV_SL_HIT"
        else:
            exit_price, reason = price, "SRV_TIME_EXIT"

        pnl_pips = ((exit_price - entry) if is_long else (entry - exit_price)) / pip_size
        pnl_dollars = pnl_pips * pip_value * (trade.lot_size or 0.1)

        await self._close_trade(
            trade, db,
            exit_price=exit_price, pnl_pips=pnl_pips, pnl_dollars=pnl_dollars,
            reason=reason, status="SRV_CLOSED",
            mfe_pips=abs(
                (state["high_water"] - entry) if is_long else (entry - state["low_water"])
            ) / pip_size if pip_size else None,
            mae_pips=abs(
                (entry - state["low_water"]) if is_long else (state["high_water"] - entry)
            ) / pip_size if pip_size else None,
        )

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # VIRTUAL / OniAI TRADE CHECK (raw SL/TP only)
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def _check_virtual_trade(self, trade: Trade, price: float, db: Session):
        if not price or not trade.entry_price:
            return

        entry = trade.entry_price
        sl = trade.stop_loss or 0
        tp1 = trade.take_profit_1 or 0
        is_long = (trade.direction or "").upper() in ["LONG", "BUY"]
        pip_size, pip_value = get_pip_info(trade.symbol)

        hit_sl = sl > 0 and ((is_long and price <= sl) or (not is_long and price >= sl))
        hit_tp = tp1 > 0 and ((is_long and price >= tp1) or (not is_long and price <= tp1))
        time_expired = self._is_time_expired(trade)

        if not (hit_sl or hit_tp or time_expired):
            return

        if hit_tp:
            exit_price, reason = tp1, "ONIAI_TP1_HIT"
        elif hit_sl:
            exit_price, reason = sl, "ONIAI_SL_HIT"
        else:
            exit_price, reason = price, "ONIAI_TIME_EXIT"

        pnl_pips = ((exit_price - entry) if is_long else (entry - exit_price)) / pip_size
        pnl_dollars = pnl_pips * pip_value * (trade.lot_size or 0.1)

        await self._close_trade(
            trade, db,
            exit_price=exit_price, pnl_pips=pnl_pips, pnl_dollars=pnl_dollars,
            reason=reason, status="ONIAI_CLOSED",
        )

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # UNIFIED TRADE CLOSE
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def _close_trade(
        self, trade: Trade, db: Session, *,
        exit_price: float, pnl_pips: float, pnl_dollars: float,
        reason: str, status: str,
        mfe_pips: float = None, mae_pips: float = None,
    ):
        trade.exit_price = round(exit_price, 5)
        trade.pnl_dollars = round(pnl_dollars, 2)
        trade.pnl_pips = round(pnl_pips, 1)
        trade.close_reason = reason
        trade.status = status
        trade.closed_at = datetime.now(timezone.utc)

        # Desk state
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

        # Clean state
        if trade.id in self.trade_state:
            del self.trade_state[trade.id]

        # Console log
        tag = "\U0001f916 OniAI" if "ONIAI" in reason else "\U0001f5a5\ufe0f SIM"
        hold_str = ""
        if trade.opened_at:
            hold_min = (trade.closed_at - trade.opened_at).total_seconds() / 60
            hold_str = f" | Held: {hold_min:.0f}m"

        logger.info(
            f"{tag} CLOSED | #{trade.id} | {trade.symbol} {trade.direction} | "
            f"Entry: {trade.entry_price} | Exit: {exit_price} | "
            f"PnL: ${pnl_dollars:+.2f} ({pnl_pips:+.1f}p) | {reason}{hold_str}"
        )

        # ML outcome log (single, deduped)
        try:
            from app.services.ml_data_logger import MLDataLogger
            duration = 0
            if trade.opened_at:
                duration = (trade.closed_at - trade.opened_at).total_seconds() / 60
            MLDataLogger.label_outcome(
                db=db,
                trade_id=trade.id,
                signal_id=trade.signal_id,
                exit_price=exit_price,
                exit_reason=reason,
                pnl_pips=round(pnl_pips, 1),
                pnl_dollars=round(pnl_dollars, 2),
                trade_duration_minutes=duration,
                max_favorable_pips=mfe_pips,
                max_adverse_pips=mae_pips,
            )
            db.commit()
        except Exception as e:
            logger.debug(f"ML outcome log failed: {e}")

        # Telegram
        try:
            await self.telegram.notify_trade_exit(
                symbol=trade.symbol,
                desk_id=trade.desk_id,
                pnl=pnl_dollars,
                reason=reason,
            )
        except Exception as e:
            logger.error(f"Telegram close failed: {e}")

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # TIME EXIT HELPERS
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    def _is_time_expired(self, trade: Trade) -> bool:
        desk = DESKS.get(trade.desk_id, {})
        max_hold = desk.get("max_hold_hours", 24)
        if not trade.opened_at:
            return False
        elapsed = (datetime.now(timezone.utc) - trade.opened_at).total_seconds()
        return elapsed >= max_hold * 3600

    async def _force_time_exit(self, trade: Trade, db: Session):
        exit_price = trade.entry_price or 0
        is_oniai = trade.status in ("ONIAI_OPEN", "VIRTUAL_OPEN")
        reason = "ONIAI_TIME_EXIT_NOPRICE" if is_oniai else "SRV_TIME_EXIT_NOPRICE"
        status = "ONIAI_CLOSED" if is_oniai else "SRV_CLOSED"

        desk = DESKS.get(trade.desk_id, {})
        elapsed = 0
        if trade.opened_at:
            elapsed = (datetime.now(timezone.utc) - trade.opened_at).total_seconds() / 3600

        logger.warning(
            f"TIME EXIT (no price) | #{trade.id} | {trade.symbol} "
            f"{trade.direction} | Held {elapsed:.1f}h > {desk.get('max_hold_hours', 24)}h"
        )

        await self._close_trade(
            trade, db,
            exit_price=exit_price, pnl_pips=0.0, pnl_dollars=0.0,
            reason=reason, status=status,
        )

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    # MAIN LOOP — BATCHED JIT POLLING
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    async def run(self):
        """
        Batched JIT polling loop.

        Cycle:
          1. Query DB for open trades → if none, sleep 15s, zero API calls
          2. Extract unique tickers
          3. ONE batched TwelveData call for all tickers
          4. Finnhub individual calls for TD misses only
          5. Evaluate each open trade against its price
          6. Sleep 15s → repeat (4 cycles/min, under 8/min TD limit)
        """
        td_status = "ON" if TWELVEDATA_API_KEY else "OFF"
        fh_status = "ON" if FINNHUB_API_KEY else "OFF"

        logger.info(
            f"Batched JIT Simulator started | {POLL_INTERVAL}s interval | "
            f"TD: {td_status} | FH: {fh_status} | "
            f"Monitoring: {OPEN_STATUSES}"
        )

        while self.running:
            try:
                db = SessionLocal()
                try:
                    # ── 1. Query open trades ──
                    open_trades = (
                        db.query(Trade)
                        .filter(Trade.status.in_(OPEN_STATUSES))
                        .all()
                    )

                    if not open_trades:
                        # Zero trades = zero API calls
                        await asyncio.sleep(POLL_INTERVAL)
                        continue

                    # ── 2. Extract unique tickers ──
                    tickers = set(t.symbol for t in open_trades if t.symbol)

                    # ── 3+4. Batched TD + Finnhub fallback ──
                    prices = await self._fetch_prices_batch(tickers)

                    # ── 5. Evaluate each trade ──
                    for trade in open_trades:
                        price = prices.get(trade.symbol)
                        is_virtual = trade.status in ("ONIAI_OPEN", "VIRTUAL_OPEN")

                        if price:
                            if is_virtual:
                                await self._check_virtual_trade(trade, price, db)
                            else:
                                await self._check_executed_trade(trade, price, db)
                        else:
                            # No price from either provider — time exit only
                            if self._is_time_expired(trade):
                                await self._force_time_exit(trade, db)

                    # ── 6. Cycle stats ──
                    logger.debug(
                        f"JIT cycle | {len(open_trades)} trades | "
                        f"{len(prices)}/{len(tickers)} prices | "
                        f"TD: {_provider_stats['twelvedata']['batch_calls']} batches, "
                        f"{_provider_stats['twelvedata']['success']} hits | "
                        f"FH: {_provider_stats['finnhub']['individual_calls']} calls, "
                        f"{_provider_stats['finnhub']['success']} hits"
                    )

                finally:
                    db.close()

            except Exception as e:
                logger.error(f"Simulator error: {e}", exc_info=True)

            # ── 7. Throttle: 15s sleep guarantees ≤4 TD calls/min ──
            await asyncio.sleep(POLL_INTERVAL)

    async def stop(self):
        self.running = False
        await self.client.aclose()
        await self.telegram.close()
        logger.info("Batched JIT Simulator stopped")
