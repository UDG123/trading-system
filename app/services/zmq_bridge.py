"""
ZeroMQ Bridge — Railway Side
Sends approved trade commands to the MT5 VPS over ZeroMQ.
Also receives execution confirmations back from the VPS.

The VPS runs a ZeroMQ PULL socket. Railway connects with a PUSH socket.
A separate REQ/REP pair handles status queries and kill switch commands.
"""
import json
import logging
import os
from datetime import datetime, timezone
from typing import Dict, Optional

import zmq
import zmq.asyncio

logger = logging.getLogger("TradingSystem.ZMQBridge")

VPS_HOST = os.getenv("VPS_HOST", "")  # e.g., "tcp://123.45.67.89:5555"
VPS_COMMAND_PORT = os.getenv("VPS_COMMAND_PORT", "5555")  # PUSH/PULL for trades
VPS_CONTROL_PORT = os.getenv("VPS_CONTROL_PORT", "5556")  # REQ/REP for control


class ZMQBridge:
    """
    Sends trade commands to MT5 VPS via ZeroMQ.
    Falls back to logging-only mode if VPS is not configured.
    """

    def __init__(self):
        self.context = None
        self.push_socket = None
        self.req_socket = None
        self.connected = False
        self.vps_host = VPS_HOST

        if self.vps_host:
            self._connect()
        else:
            logger.warning(
                "VPS_HOST not set — ZeroMQ bridge in LOG-ONLY mode. "
                "Trades will be logged but not sent to MT5."
            )

    def _connect(self):
        """Establish ZeroMQ connections to VPS."""
        try:
            self.context = zmq.asyncio.Context()

            # PUSH socket for sending trade commands
            self.push_socket = self.context.socket(zmq.PUSH)
            self.push_socket.setsockopt(zmq.SNDTIMEO, 5000)  # 5s timeout
            self.push_socket.setsockopt(zmq.LINGER, 1000)
            push_addr = f"tcp://{self.vps_host}:{VPS_COMMAND_PORT}"
            self.push_socket.connect(push_addr)

            # REQ socket for control commands (kill switch, status)
            self.req_socket = self.context.socket(zmq.REQ)
            self.req_socket.setsockopt(zmq.RCVTIMEO, 5000)
            self.req_socket.setsockopt(zmq.SNDTIMEO, 5000)
            self.req_socket.setsockopt(zmq.LINGER, 1000)
            ctrl_addr = f"tcp://{self.vps_host}:{VPS_CONTROL_PORT}"
            self.req_socket.connect(ctrl_addr)

            self.connected = True
            logger.info(
                f"ZeroMQ connected to VPS at {push_addr} (trades) "
                f"and {ctrl_addr} (control)"
            )
        except Exception as e:
            logger.error(f"ZeroMQ connection failed: {e}")
            self.connected = False

    async def send_trade(self, trade_params: Dict, signal_id: int) -> Dict:
        """
        Send an approved trade command to the MT5 VPS.
        Returns execution result or log-only confirmation.
        """
        command = {
            "type": "TRADE",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "signal_id": signal_id,
            "action": "OPEN",
            "symbol": trade_params.get("symbol"),
            "direction": trade_params.get("direction"),
            "risk_pct": trade_params.get("risk_pct"),
            "risk_dollars": trade_params.get("risk_dollars"),
            "stop_loss": trade_params.get("stop_loss"),
            "take_profit_1": trade_params.get("take_profit_1"),
            "take_profit_2": trade_params.get("take_profit_2"),
            "trailing_stop_pips": trade_params.get("trailing_stop_pips"),
            "size_multiplier": trade_params.get("size_multiplier"),
            "desk_id": trade_params.get("desk_id"),
            "claude_decision": trade_params.get("claude_decision"),
            "confidence": trade_params.get("confidence"),
        }

        if not self.connected or not self.push_socket:
            logger.info(
                f"[LOG-ONLY] Trade command for signal #{signal_id}: "
                f"{command['symbol']} {command['direction']} "
                f"Risk: ${command['risk_dollars']}"
            )
            return {
                "status": "logged",
                "message": "VPS not connected — trade logged only",
                "command": command,
            }

        try:
            await self.push_socket.send_json(command)
            logger.info(
                f"Trade SENT to VPS | Signal #{signal_id} | "
                f"{command['symbol']} {command['direction']} | "
                f"Risk: ${command['risk_dollars']}"
            )
            return {
                "status": "sent",
                "message": "Trade command sent to MT5 VPS",
                "command": command,
            }
        except zmq.error.Again:
            logger.error(f"ZMQ timeout sending trade for signal #{signal_id}")
            return {"status": "timeout", "message": "VPS did not respond"}
        except Exception as e:
            logger.error(f"ZMQ send error: {e}")
            return {"status": "error", "message": str(e)}

    async def send_close(
        self, symbol: str, desk_id: str, reason: str
    ) -> Dict:
        """Send a close/exit command for a position."""
        command = {
            "type": "TRADE",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "action": "CLOSE",
            "symbol": symbol,
            "desk_id": desk_id,
            "reason": reason,
        }

        if not self.connected:
            logger.info(f"[LOG-ONLY] Close command: {symbol} on {desk_id}")
            return {"status": "logged", "command": command}

        try:
            await self.push_socket.send_json(command)
            logger.info(f"Close SENT to VPS | {symbol} on {desk_id} | {reason}")
            return {"status": "sent", "command": command}
        except Exception as e:
            logger.error(f"ZMQ close send error: {e}")
            return {"status": "error", "message": str(e)}

    async def send_kill_switch(self, scope: str = "ALL") -> Dict:
        """
        Emergency kill switch. Closes all positions and stops trading.
        scope: "ALL" = entire firm, or a desk_id for single desk.
        """
        command = {
            "type": "KILL_SWITCH",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "scope": scope,
        }

        if not self.connected or not self.req_socket:
            logger.warning(f"[LOG-ONLY] Kill switch triggered: {scope}")
            return {"status": "logged", "message": "VPS not connected"}

        try:
            await self.req_socket.send_json(command)
            response = await self.req_socket.recv_json()
            logger.warning(
                f"KILL SWITCH EXECUTED | Scope: {scope} | "
                f"Response: {response}"
            )
            return {"status": "executed", "response": response}
        except Exception as e:
            logger.error(f"Kill switch ZMQ error: {e}")
            return {"status": "error", "message": str(e)}

    async def get_vps_status(self) -> Dict:
        """Query the VPS for current status (open positions, connection, etc.)."""
        command = {
            "type": "STATUS",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        if not self.connected or not self.req_socket:
            return {
                "status": "disconnected",
                "message": "VPS not configured or unreachable",
            }

        try:
            await self.req_socket.send_json(command)
            response = await self.req_socket.recv_json()
            return {"status": "connected", "vps_data": response}
        except zmq.error.Again:
            return {"status": "timeout", "message": "VPS did not respond"}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    async def toggle_desk(self, desk_id: str, active: bool) -> Dict:
        """Enable or disable a specific desk on the VPS."""
        command = {
            "type": "TOGGLE_DESK",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "desk_id": desk_id,
            "active": active,
        }

        if not self.connected or not self.req_socket:
            return {"status": "logged", "message": "VPS not connected"}

        try:
            await self.req_socket.send_json(command)
            response = await self.req_socket.recv_json()
            logger.info(f"Desk toggle: {desk_id} → {'ON' if active else 'OFF'}")
            return {"status": "executed", "response": response}
        except Exception as e:
            return {"status": "error", "message": str(e)}

    def close(self):
        """Clean up ZeroMQ sockets."""
        if self.push_socket:
            self.push_socket.close()
        if self.req_socket:
            self.req_socket.close()
        if self.context:
            self.context.term()
