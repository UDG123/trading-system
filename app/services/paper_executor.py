"""
Alpaca Paper Trading Executor — OniQuant v5.9
Routes orders through desk-specific logic:
  - Scalper desks → Limit IOC (zero slippage)
  - All others   → Market + native Alpaca Trailing Stop
"""
import os
import asyncio
import logging
import uuid
from typing import Dict, Optional

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import (
    MarketOrderRequest,
    LimitOrderRequest,
    TrailingStopOrderRequest,
)
from alpaca.trading.enums import OrderSide, TimeInForce

from app.config import DESKS, SRV_100, get_pip_info

logger = logging.getLogger("TradingSystem.PaperExecutor")

# Desks that use Limit IOC for zero-slippage scalping
_SCALPER_DESKS = {"DESK1_SCALPER"}
_SCALPER_SUB_STRATEGIES = {"gold_scalp"}


def _is_scalper(desk_id: str, sub_strategy: Optional[str] = None) -> bool:
    """Check if this desk/sub-strategy should use IOC limit orders."""
    if desk_id in _SCALPER_DESKS:
        return True
    if sub_strategy and sub_strategy in _SCALPER_SUB_STRATEGIES:
        return True
    return False


def _get_trailing_stop_price(desk_id: str, symbol: str, sub_strategy: Optional[str] = None) -> Optional[float]:
    """
    Convert trailing_stop_pips from config into a dollar trail_price
    for Alpaca's TrailingStopOrderRequest.

    trail_price = trailing_stop_pips × pip_size
    """
    desk = DESKS.get(desk_id, {})

    # Sub-strategy override (e.g. gold_scalp has its own trailing_stop_pips)
    if sub_strategy and "sub_strategies" in desk:
        sub = desk["sub_strategies"].get(sub_strategy, {})
        pips = sub.get("trailing_stop_pips")
        if pips is not None:
            pip_size, _ = get_pip_info(symbol)
            return round(float(pips) * pip_size, 4)

    # Desk-level trailing_stop_pips (may be int or dict)
    pips_cfg = desk.get("trailing_stop_pips")
    if pips_cfg is None:
        # Fallback from SRV_100 profile
        profile = SRV_100.get(desk_id, {})
        pips_cfg = profile.get("trailing_stop_pips")

    if pips_cfg is None:
        return None

    pip_size, _ = get_pip_info(symbol)

    # DESK5_ALTS stores a dict: {"indices": 15, "crypto": 50, "commodity": 30}
    if isinstance(pips_cfg, dict):
        sym_upper = symbol.upper()
        if any(c in sym_upper for c in ["BTC", "ETH", "SOL", "XRP", "LINK"]):
            pips = pips_cfg.get("crypto", 50)
        elif any(c in sym_upper for c in ["NAS", "US30", "US100"]):
            pips = pips_cfg.get("indices", 15)
        else:
            pips = pips_cfg.get("commodity", 30)
    else:
        pips = float(pips_cfg)

    return round(pips * pip_size, 4)


class PaperExecutor:
    """
    Alpaca Paper Trading executor with desk-specific order routing.

    All TradingClient methods are synchronous, so we wrap them with
    asyncio.to_thread() to keep the FastAPI event loop non-blocking.
    """

    def __init__(self):
        api_key = os.getenv("ALPACA_API_KEY", "")
        secret_key = os.getenv("ALPACA_SECRET_KEY", "")

        if not api_key or not secret_key:
            logger.warning("Alpaca API keys not set — executor will be in dry-run mode")
            self._client = None
        else:
            self._client = TradingClient(
                api_key=api_key,
                secret_key=secret_key,
                paper=True,
            )
            logger.info("Alpaca Paper Trading client initialized")

    @property
    def is_live(self) -> bool:
        return self._client is not None

    async def execute(
        self,
        symbol: str,
        direction: str,
        qty: float,
        desk_id: str,
        signal_id: int,
        entry_price: float,
        sub_strategy: Optional[str] = None,
    ) -> Dict:
        """
        Route and submit an order to Alpaca Paper Trading.

        Returns dict with order details or error info.
        """
        if not self.is_live:
            logger.info(f"DRY-RUN | {desk_id} {direction} {qty} {symbol} @ {entry_price}")
            return {
                "status": "dry_run",
                "desk_id": desk_id,
                "symbol": symbol,
                "direction": direction,
                "qty": qty,
                "entry_price": entry_price,
            }

        side = OrderSide.BUY if direction.upper() == "LONG" else OrderSide.SELL
        client_order_id = f"{desk_id}_{signal_id}_{uuid.uuid4().hex[:8]}"

        try:
            if _is_scalper(desk_id, sub_strategy):
                return await self._submit_limit_ioc(
                    symbol, qty, side, entry_price, client_order_id, desk_id
                )
            else:
                return await self._submit_market_with_trail(
                    symbol, qty, side, client_order_id, desk_id, sub_strategy
                )
        except Exception as e:
            logger.error(f"Order submission failed | {desk_id} {symbol}: {e}")
            return {"status": "error", "error": str(e), "desk_id": desk_id, "symbol": symbol}

    async def _submit_limit_ioc(
        self,
        symbol: str,
        qty: float,
        side: OrderSide,
        limit_price: float,
        client_order_id: str,
        desk_id: str,
    ) -> Dict:
        """
        Scalper path: Limit order with IOC (Immediate-or-Cancel).
        Guarantees zero slippage — fills at limit_price or not at all.
        """
        request = LimitOrderRequest(
            symbol=symbol,
            qty=qty,
            side=side,
            time_in_force=TimeInForce.IOC,
            limit_price=limit_price,
            client_order_id=client_order_id,
        )

        order = await asyncio.to_thread(self._client.submit_order, order_data=request)

        logger.info(
            f"LIMIT IOC | {desk_id} | {side.name} {qty} {symbol} @ {limit_price} | "
            f"order_id={order.id} status={order.status}"
        )

        return {
            "status": "submitted",
            "order_type": "limit_ioc",
            "order_id": str(order.id),
            "client_order_id": client_order_id,
            "desk_id": desk_id,
            "symbol": symbol,
            "side": side.name,
            "qty": qty,
            "limit_price": limit_price,
            "alpaca_status": str(order.status),
        }

    async def _submit_market_with_trail(
        self,
        symbol: str,
        qty: float,
        side: OrderSide,
        client_order_id: str,
        desk_id: str,
        sub_strategy: Optional[str] = None,
    ) -> Dict:
        """
        Non-scalper path: Market order for immediate fill, then a
        native Alpaca Trailing Stop on the opposite side to manage exit.
        """
        # 1. Submit market order
        market_request = MarketOrderRequest(
            symbol=symbol,
            qty=qty,
            side=side,
            time_in_force=TimeInForce.DAY,
            client_order_id=client_order_id,
        )

        market_order = await asyncio.to_thread(
            self._client.submit_order, order_data=market_request
        )

        logger.info(
            f"MARKET | {desk_id} | {side.name} {qty} {symbol} | "
            f"order_id={market_order.id} status={market_order.status}"
        )

        result = {
            "status": "submitted",
            "order_type": "market",
            "order_id": str(market_order.id),
            "client_order_id": client_order_id,
            "desk_id": desk_id,
            "symbol": symbol,
            "side": side.name,
            "qty": qty,
            "alpaca_status": str(market_order.status),
            "trailing_stop": None,
        }

        # 2. Submit trailing stop on the opposite side
        trail_price = _get_trailing_stop_price(desk_id, symbol, sub_strategy)
        if trail_price and trail_price > 0:
            exit_side = OrderSide.SELL if side == OrderSide.BUY else OrderSide.BUY
            trail_order_id = f"{client_order_id}_trail"

            trail_request = TrailingStopOrderRequest(
                symbol=symbol,
                qty=qty,
                side=exit_side,
                time_in_force=TimeInForce.GTC,
                trail_price=trail_price,
                client_order_id=trail_order_id,
            )

            trail_order = await asyncio.to_thread(
                self._client.submit_order, order_data=trail_request
            )

            logger.info(
                f"TRAIL STOP | {desk_id} | {exit_side.name} {qty} {symbol} "
                f"trail=${trail_price} | order_id={trail_order.id}"
            )

            result["trailing_stop"] = {
                "order_id": str(trail_order.id),
                "trail_price": trail_price,
                "side": exit_side.name,
                "alpaca_status": str(trail_order.status),
            }

        return result

    async def get_account(self) -> Dict:
        """Fetch Alpaca paper account info."""
        if not self.is_live:
            return {"status": "dry_run", "message": "No Alpaca keys configured"}

        account = await asyncio.to_thread(self._client.get_account)
        return {
            "equity": str(account.equity),
            "cash": str(account.cash),
            "buying_power": str(account.buying_power),
            "portfolio_value": str(account.portfolio_value),
            "status": str(account.status),
        }

    async def get_positions(self) -> list:
        """Fetch all open positions."""
        if not self.is_live:
            return []

        positions = await asyncio.to_thread(self._client.get_all_positions)
        return [
            {
                "symbol": p.symbol,
                "qty": str(p.qty),
                "side": str(p.side),
                "avg_entry_price": str(p.avg_entry_price),
                "current_price": str(p.current_price),
                "unrealized_pl": str(p.unrealized_pl),
            }
            for p in positions
        ]
