"""
MetaApi Cloud Executor — $1M Demo Account Bridge
Executes approved trades on a real broker demo via MetaApi REST API.
Runs parallel to the server simulator: simulator does math, MetaApi gets real fills.

Setup:
  1. Sign up at metaapi.cloud (free tier = 1 account)
  2. Add your broker demo account (IC Markets, etc.)
  3. Copy METAAPI_TOKEN + METAAPI_ACCOUNT_ID to Railway variables
  4. Trades auto-execute on your demo — no VPS, no EA, no MT5

REST API Reference:
  Base: https://mt-client-api-v1.{region}.agiliumtrade.ai
  Auth: auth-token header
  Trade: POST /users/current/accounts/{id}/trade
  Positions: GET /users/current/accounts/{id}/positions
  Account: GET /users/current/accounts/{id}/account-information
"""
import os
import logging
from datetime import datetime, timezone
from typing import Dict, Optional

import httpx

logger = logging.getLogger("TradingSystem.MetaApi")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONFIG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

METAAPI_TOKEN = os.getenv("METAAPI_TOKEN", "")
METAAPI_ACCOUNT_ID = os.getenv("METAAPI_ACCOUNT_ID", "")
METAAPI_REGION = os.getenv("METAAPI_REGION", "new-york")

# Base URL for MetaApi client REST API
METAAPI_BASE = f"https://mt-client-api-v1.{METAAPI_REGION}.agiliumtrade.ai"


def is_enabled() -> bool:
    """Check if MetaApi execution is configured."""
    return bool(METAAPI_TOKEN and METAAPI_ACCOUNT_ID)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SYMBOL MAPPING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# OniQuant internal → Broker symbol (IC Markets MT5 default)
# Most brokers use raw symbol names. If your broker appends
# suffixes (e.g., EURUSDm, EURUSD.raw), override with
# METAAPI_SYMBOL_SUFFIX env var.

SYMBOL_SUFFIX = os.getenv("METAAPI_SYMBOL_SUFFIX", "")

# Symbols that need special mapping (broker uses different name)
SYMBOL_OVERRIDE = {
    "US30": "DJ30",
    "US100": "USTEC",
    "NAS100": "USTEC",
    "WTIUSD": "XTIUSD",
    "XCUUSD": "COPPER",
    "BTCUSD": "BTCUSD",
    "ETHUSD": "ETHUSD",
    "SOLUSD": "SOLUSD",
    "XRPUSD": "XRPUSD",
    "LINKUSD": "LINKUSD",
}


def to_broker_symbol(internal: str) -> str:
    """Convert OniQuant symbol to broker symbol."""
    mapped = SYMBOL_OVERRIDE.get(internal, internal)
    return mapped + SYMBOL_SUFFIX


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# LOT SIZING FOR $1M ACCOUNT
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

METAAPI_CAPITAL = 1_000_000  # $1M demo account
METAAPI_RISK_PCT = 1.0       # 1% risk per trade (conservative for combined desks)


def scale_lot_size(original_lot: float, original_capital: float) -> float:
    """
    Scale lot size from $100K desk to $1M account proportionally.
    $100K @ 0.5 lots → $1M @ 5.0 lots (10x)
    """
    if not original_capital or original_capital <= 0:
        return original_lot
    ratio = METAAPI_CAPITAL / original_capital
    scaled = round(original_lot * ratio, 2)
    # Clamp to reasonable bounds
    return max(0.01, min(scaled, 100.0))


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# EXECUTOR
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class MetaApiExecutor:
    """
    Sends trade orders to MetaApi cloud for execution on a broker demo.
    Fire-and-forget: if MetaApi fails, the server simulator still works.
    """

    def __init__(self):
        self.client = httpx.AsyncClient(timeout=15.0)
        self._headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "auth-token": METAAPI_TOKEN,
        }
        self._trade_url = (
            f"{METAAPI_BASE}/users/current/accounts/{METAAPI_ACCOUNT_ID}/trade"
        )
        self._positions_url = (
            f"{METAAPI_BASE}/users/current/accounts/{METAAPI_ACCOUNT_ID}/positions"
        )
        self._account_url = (
            f"{METAAPI_BASE}/users/current/accounts/{METAAPI_ACCOUNT_ID}"
            f"/account-information"
        )
        # Map OniQuant trade_id → MetaApi positionId for close routing
        self._position_map: Dict[int, str] = {}

    async def close(self):
        """Cleanup."""
        await self.client.aclose()

    # ── OPEN TRADE ──────────────────────────────────────────────

    async def open_trade(
        self,
        trade_id: int,
        symbol: str,
        direction: str,
        lot_size: float,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
        desk_capital: float = 100_000,
        comment: str = "",
    ) -> Dict:
        """
        Execute a market order on MetaApi.
        Returns {"success": True, "orderId": "...", "positionId": "..."} or error.
        """
        if not is_enabled():
            return {"success": False, "error": "MetaApi not configured"}

        broker_symbol = to_broker_symbol(symbol)
        scaled_lot = scale_lot_size(lot_size, desk_capital)
        is_buy = direction.upper() in ("LONG", "BUY")

        body = {
            "actionType": "ORDER_TYPE_BUY" if is_buy else "ORDER_TYPE_SELL",
            "symbol": broker_symbol,
            "volume": scaled_lot,
            "comment": comment or f"OQ#{trade_id}",
        }
        if stop_loss and stop_loss > 0:
            body["stopLoss"] = round(stop_loss, 5)
        if take_profit and take_profit > 0:
            body["takeProfit"] = round(take_profit, 5)

        try:
            resp = await self.client.post(
                self._trade_url,
                json=body,
                headers=self._headers,
            )

            if resp.status_code == 200:
                data = resp.json()
                position_id = data.get("positionId", "")
                order_id = data.get("orderId", "")
                string_code = data.get("stringCode", "")

                if string_code == "TRADE_RETCODE_DONE":
                    # Track for close routing
                    if position_id:
                        self._position_map[trade_id] = position_id

                    logger.info(
                        f"MetaApi OPEN | #{trade_id} | {broker_symbol} "
                        f"{'BUY' if is_buy else 'SELL'} {scaled_lot} lots | "
                        f"Order: {order_id} | Position: {position_id}"
                    )
                    return {
                        "success": True,
                        "orderId": order_id,
                        "positionId": position_id,
                        "stringCode": string_code,
                        "volume": scaled_lot,
                        "broker_symbol": broker_symbol,
                    }
                else:
                    msg = data.get("message", string_code)
                    logger.warning(
                        f"MetaApi REJECT | #{trade_id} | {broker_symbol} | {msg}"
                    )
                    return {"success": False, "error": msg, "data": data}
            else:
                logger.warning(
                    f"MetaApi HTTP {resp.status_code} | #{trade_id} | "
                    f"{resp.text[:200]}"
                )
                return {"success": False, "error": f"HTTP {resp.status_code}"}

        except httpx.TimeoutException:
            logger.warning(f"MetaApi TIMEOUT | #{trade_id} | {broker_symbol}")
            return {"success": False, "error": "timeout"}
        except Exception as e:
            logger.error(f"MetaApi ERROR | #{trade_id} | {e}")
            return {"success": False, "error": str(e)}

    # ── CLOSE TRADE ─────────────────────────────────────────────

    async def close_trade(
        self,
        trade_id: int,
        symbol: str = "",
    ) -> Dict:
        """
        Close a position on MetaApi by trade_id (looks up positionId).
        Falls back to close-by-symbol if positionId unknown.
        """
        if not is_enabled():
            return {"success": False, "error": "MetaApi not configured"}

        position_id = self._position_map.pop(trade_id, None)

        if position_id:
            body = {
                "actionType": "POSITION_CLOSE_ID",
                "positionId": position_id,
            }
        elif symbol:
            # Fallback: close all positions for this symbol
            broker_symbol = to_broker_symbol(symbol)
            body = {
                "actionType": "POSITIONS_CLOSE_SYMBOL",
                "symbol": broker_symbol,
            }
        else:
            return {"success": False, "error": "No positionId or symbol"}

        try:
            resp = await self.client.post(
                self._trade_url,
                json=body,
                headers=self._headers,
            )

            if resp.status_code == 200:
                data = resp.json()
                string_code = data.get("stringCode", "")
                if string_code == "TRADE_RETCODE_DONE":
                    logger.info(
                        f"MetaApi CLOSE | #{trade_id} | "
                        f"Position: {position_id or 'by-symbol'}"
                    )
                    return {"success": True, "data": data}
                else:
                    msg = data.get("message", string_code)
                    logger.warning(f"MetaApi CLOSE REJECT | #{trade_id} | {msg}")
                    return {"success": False, "error": msg}
            else:
                return {"success": False, "error": f"HTTP {resp.status_code}"}

        except Exception as e:
            logger.error(f"MetaApi CLOSE ERROR | #{trade_id} | {e}")
            return {"success": False, "error": str(e)}

    # ── MODIFY SL/TP ────────────────────────────────────────────

    async def modify_position(
        self,
        trade_id: int,
        stop_loss: Optional[float] = None,
        take_profit: Optional[float] = None,
    ) -> Dict:
        """Update SL/TP on an open MetaApi position (e.g., move to breakeven)."""
        if not is_enabled():
            return {"success": False, "error": "MetaApi not configured"}

        position_id = self._position_map.get(trade_id)
        if not position_id:
            return {"success": False, "error": "No positionId tracked"}

        body = {
            "actionType": "POSITION_MODIFY",
            "positionId": position_id,
        }
        if stop_loss is not None:
            body["stopLoss"] = round(stop_loss, 5)
        if take_profit is not None:
            body["takeProfit"] = round(take_profit, 5)

        try:
            resp = await self.client.post(
                self._trade_url,
                json=body,
                headers=self._headers,
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get("stringCode") == "TRADE_RETCODE_DONE":
                    logger.debug(
                        f"MetaApi MODIFY | #{trade_id} | SL={stop_loss} TP={take_profit}"
                    )
                    return {"success": True, "data": data}
            return {"success": False, "error": f"HTTP {resp.status_code}"}
        except Exception as e:
            return {"success": False, "error": str(e)}

    # ── ACCOUNT INFO ────────────────────────────────────────────

    async def get_account_info(self) -> Dict:
        """Fetch MetaApi demo account balance/equity/margin."""
        if not is_enabled():
            return {"error": "MetaApi not configured"}

        try:
            resp = await self.client.get(
                self._account_url,
                headers=self._headers,
            )
            if resp.status_code == 200:
                return resp.json()
            return {"error": f"HTTP {resp.status_code}"}
        except Exception as e:
            return {"error": str(e)}

    async def get_positions(self) -> list:
        """Fetch all open positions on MetaApi demo."""
        if not is_enabled():
            return []

        try:
            resp = await self.client.get(
                self._positions_url,
                headers=self._headers,
            )
            if resp.status_code == 200:
                return resp.json()
            return []
        except Exception as e:
            logger.debug(f"MetaApi positions error: {e}")
            return []


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# SINGLETON
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_executor: Optional[MetaApiExecutor] = None


def get_executor() -> MetaApiExecutor:
    """Get or create the singleton MetaApi executor."""
    global _executor
    if _executor is None:
        _executor = MetaApiExecutor()
    return _executor
