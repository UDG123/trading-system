"""
OniQuant v5.9 — Redis Stream Consumer & MCP Verification Layer

Persistent background process that consumes from the `oniquant_alerts`
Redis Stream and runs each signal through three MCP verification gates
before executing virtual trades.

Gates:
  1. NumPy Gate    → numpy-mcp:calculate_hurst (reject Scalper if H < 0.55)
  2. altFINS Gate  → altfins-mcp:get_indicator_state (RSI/MACD/BB alignment > 70%)
  3. Paradox Gate  → paradox-mcp:get_sentiment (flag extreme inverse sentiment)

Run as: python -m app.worker
Deploy on Railway as a separate 'Service' tab process.
"""
import os
import sys
import asyncio
import logging
import signal as signal_mod
from datetime import datetime, timezone
from typing import Dict, Optional

import orjson
import redis.asyncio as aioredis

from app.config import REDIS_URL, DESKS

logger = logging.getLogger("TradingSystem.Worker")

# ─── Stream / Consumer Group config ───
STREAM_KEY = "oniquant_alerts"
CONSUMER_GROUP = "oniquant_workers"
CONSUMER_NAME = os.getenv("WORKER_ID", f"worker-{os.getpid()}")
BLOCK_MS = 5000          # Block 5s waiting for new messages
BATCH_SIZE = 10           # Read up to 10 messages per batch
HURST_SCALPER_MIN = 0.55  # Scalper desk Hurst floor
INDICATOR_ALIGNMENT_MIN = 0.70  # 70% indicator alignment required

# ─── Graceful shutdown ───
_shutdown = asyncio.Event()


def _handle_signal(sig, frame):
    logger.info(f"Received {sig}, initiating graceful shutdown...")
    _shutdown.set()


class MCPClient:
    """Thin wrapper for MCP tool calls via Redis pub/sub or HTTP."""

    def __init__(self, redis_client):
        self._redis = redis_client

    async def call_tool(self, server: str, tool: str, params: Dict) -> Dict:
        """
        Call an MCP tool and return the result.
        Uses Redis request/response pattern: XADD to mcp_{server}_requests,
        wait for response on mcp_{server}_responses:{request_id}.
        """
        import uuid
        request_id = str(uuid.uuid4())[:8]
        request_key = f"mcp_{server}_requests"
        response_key = f"mcp_{server}_responses:{request_id}"

        request_payload = orjson.dumps({
            "id": request_id,
            "tool": tool,
            "params": params,
        })

        await self._redis.xadd(request_key, {"payload": request_payload})

        # Wait for response with timeout
        for _ in range(30):  # 30 * 200ms = 6s timeout
            result = await self._redis.get(response_key)
            if result:
                await self._redis.delete(response_key)
                return orjson.loads(result)
            await asyncio.sleep(0.2)

        logger.warning(f"MCP timeout: {server}:{tool} (request {request_id})")
        return {"error": "timeout", "server": server, "tool": tool}


class VerificationWorker:
    """Consumes from Redis Stream and runs MCP verification gates."""

    def __init__(self):
        self._redis: Optional[aioredis.Redis] = None
        self._mcp: Optional[MCPClient] = None
        self._db_session_factory = None

    async def start(self):
        """Initialize connections and start consuming."""
        self._redis = aioredis.from_url(
            REDIS_URL,
            decode_responses=False,
            max_connections=10,
        )
        self._mcp = MCPClient(self._redis)

        # Lazy DB import — only needed for trade execution
        from app.database import SessionLocal
        self._db_session_factory = SessionLocal

        # Ensure consumer group exists
        try:
            await self._redis.xgroup_create(
                STREAM_KEY, CONSUMER_GROUP, id="0", mkstream=True
            )
            logger.info(f"Created consumer group '{CONSUMER_GROUP}'")
        except aioredis.ResponseError as e:
            if "BUSYGROUP" in str(e):
                logger.info(f"Consumer group '{CONSUMER_GROUP}' already exists")
            else:
                raise

        logger.info(
            f"Worker '{CONSUMER_NAME}' started | "
            f"Stream: {STREAM_KEY} | Group: {CONSUMER_GROUP} | "
            f"Batch: {BATCH_SIZE} | Block: {BLOCK_MS}ms"
        )

        await self._consume_loop()

    async def _consume_loop(self):
        """Main consumption loop — reads from stream, processes, ACKs."""
        while not _shutdown.is_set():
            try:
                messages = await self._redis.xreadgroup(
                    groupname=CONSUMER_GROUP,
                    consumername=CONSUMER_NAME,
                    streams={STREAM_KEY: ">"},
                    count=BATCH_SIZE,
                    block=BLOCK_MS,
                )

                if not messages:
                    continue

                for stream_name, entries in messages:
                    for msg_id, fields in entries:
                        try:
                            payload = orjson.loads(fields[b"payload"])
                            await self._process_signal(msg_id, payload)
                        except Exception as e:
                            logger.error(
                                f"Processing failed for {msg_id}: {e}",
                                exc_info=True,
                            )
                        finally:
                            # Always ACK to prevent redelivery loops
                            await self._redis.xack(
                                STREAM_KEY, CONSUMER_GROUP, msg_id
                            )

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Consumer loop error: {e}", exc_info=True)
                await asyncio.sleep(2)

        logger.info("Consumer loop exited")

    async def _process_signal(self, msg_id: bytes, payload: Dict):
        """Run the three MCP verification gates on a signal."""
        symbol = payload.get("symbol_normalized", payload.get("symbol", "?"))
        alert_type = payload.get("alert_type", "?")
        direction = payload.get("direction")
        desks = payload.get("desks_matched", [])

        logger.info(
            f"PROCESSING | {msg_id.decode()} | {symbol} {alert_type} | "
            f"Desks: {desks}"
        )

        # ── Gate 1: NumPy Hurst ──
        hurst_result = await self._gate_hurst(symbol, desks)
        if hurst_result.get("rejected"):
            logger.info(
                f"REJECTED (Hurst) | {symbol} | H={hurst_result.get('hurst', '?')} | "
                f"Desk requires H >= {HURST_SCALPER_MIN}"
            )
            return

        # ── Gate 2: altFINS Indicator Alignment ──
        alignment_result = await self._gate_indicator_alignment(
            symbol, direction, alert_type
        )
        if alignment_result.get("rejected"):
            logger.info(
                f"REJECTED (Alignment) | {symbol} | "
                f"Alignment={alignment_result.get('alignment_pct', 0):.0%} "
                f"< {INDICATOR_ALIGNMENT_MIN:.0%}"
            )
            return

        # ── Gate 3: Paradox Sentiment ──
        sentiment_result = await self._gate_sentiment(symbol, direction)
        risk_flag = sentiment_result.get("high_risk", False)
        if risk_flag:
            logger.warning(
                f"HIGH RISK (Sentiment) | {symbol} {direction} | "
                f"Sentiment strongly inverse — flagging trade"
            )

        # ── All gates passed → Execute virtual trade ──
        await self._execute_virtual_trade(
            payload=payload,
            hurst=hurst_result.get("hurst"),
            alignment_pct=alignment_result.get("alignment_pct", 0),
            sentiment_risk=risk_flag,
            indicators=alignment_result.get("indicators", {}),
        )

    async def _gate_hurst(self, symbol: str, desks: list) -> Dict:
        """
        Gate 1: NumPy Hurst exponent check.
        Reject Scalper signals where H < 0.55.
        """
        result = await self._mcp.call_tool(
            "numpy-mcp", "calculate_hurst", {"symbol": symbol}
        )

        if "error" in result:
            # MCP unavailable — pass through with warning
            logger.warning(f"Hurst MCP unavailable for {symbol}, passing through")
            return {"rejected": False, "hurst": None}

        hurst = result.get("hurst_exponent")
        if hurst is None:
            return {"rejected": False, "hurst": None}

        # Only enforce strict Hurst floor for Scalper desk
        is_scalper = any("SCALPER" in d for d in desks)
        if is_scalper and hurst < HURST_SCALPER_MIN:
            return {"rejected": True, "hurst": hurst}

        return {"rejected": False, "hurst": hurst}

    async def _gate_indicator_alignment(
        self, symbol: str, direction: Optional[str], alert_type: str
    ) -> Dict:
        """
        Gate 2: altFINS indicator state verification.
        RSI + MACD + Bollinger Bands must align > 70% with LuxAlgo direction.
        """
        result = await self._mcp.call_tool(
            "altfins-mcp", "get_indicator_state",
            {"symbol": symbol, "indicators": ["RSI", "MACD", "BollingerBands"]},
        )

        if "error" in result:
            logger.warning(f"altFINS MCP unavailable for {symbol}, passing through")
            return {"rejected": False, "alignment_pct": 1.0, "indicators": {}}

        indicators = result.get("indicators", {})
        if not indicators or not direction or direction == "EXIT":
            return {"rejected": False, "alignment_pct": 1.0, "indicators": indicators}

        # Score alignment: each indicator votes bullish/bearish
        aligned = 0
        total = 0

        rsi_state = indicators.get("RSI", {})
        if rsi_state:
            total += 1
            rsi_val = rsi_state.get("value", 50)
            if direction == "LONG" and rsi_val < 70:
                aligned += 1  # Not overbought → aligned for long
            elif direction == "SHORT" and rsi_val > 30:
                aligned += 1  # Not oversold → aligned for short

        macd_state = indicators.get("MACD", {})
        if macd_state:
            total += 1
            histogram = macd_state.get("histogram", 0)
            if direction == "LONG" and histogram > 0:
                aligned += 1
            elif direction == "SHORT" and histogram < 0:
                aligned += 1

        bb_state = indicators.get("BollingerBands", {})
        if bb_state:
            total += 1
            position = bb_state.get("position", "middle")  # upper/middle/lower
            if direction == "LONG" and position != "upper":
                aligned += 1  # Not at upper band → room to run
            elif direction == "SHORT" and position != "lower":
                aligned += 1

        alignment_pct = aligned / total if total > 0 else 1.0

        return {
            "rejected": alignment_pct < INDICATOR_ALIGNMENT_MIN,
            "alignment_pct": alignment_pct,
            "indicators": indicators,
        }

    async def _gate_sentiment(self, symbol: str, direction: Optional[str]) -> Dict:
        """
        Gate 3: Paradox sentiment check.
        Flag as 'High Risk' if social sentiment is strongly inverse to trade direction.
        """
        result = await self._mcp.call_tool(
            "paradox-mcp", "get_sentiment", {"symbol": symbol}
        )

        if "error" in result:
            logger.warning(f"Paradox MCP unavailable for {symbol}, passing through")
            return {"high_risk": False, "sentiment": None}

        sentiment = result.get("sentiment", "neutral")  # bullish/bearish/neutral
        strength = result.get("strength", "moderate")    # weak/moderate/strong/extreme

        if not direction or direction == "EXIT":
            return {"high_risk": False, "sentiment": sentiment}

        # Flag if sentiment is strongly inverse
        is_inverse = (
            (direction == "LONG" and sentiment == "bearish")
            or (direction == "SHORT" and sentiment == "bullish")
        )
        is_strong = strength in ("strong", "extreme")

        return {
            "high_risk": is_inverse and is_strong,
            "sentiment": sentiment,
            "strength": strength,
        }

    async def _execute_virtual_trade(
        self,
        payload: Dict,
        hurst: Optional[float],
        alignment_pct: float,
        sentiment_risk: bool,
        indicators: Dict,
    ):
        """
        Execute a virtual trade in Railway Postgres.
        Logs the OPEN position with full verification metadata.
        """
        from app.models.signal import Signal
        from app.models.trade import Trade
        from app.services.pipeline import process_signal

        db = self._db_session_factory()
        try:
            # Create Signal record from stream payload
            symbol = payload.get("symbol_normalized", payload.get("symbol", "UNKNOWN"))
            now = datetime.now(timezone.utc)

            signal_record = Signal(
                received_at=now,
                source="redis_stream",
                raw_payload=orjson.dumps(payload).decode("utf-8")[:5000],
                symbol=payload.get("symbol", "UNKNOWN")[:20],
                symbol_normalized=symbol[:20],
                timeframe=str(payload.get("timeframe", "60"))[:10],
                alert_type=str(payload.get("alert_type", "unknown"))[:50],
                direction=payload.get("direction"),
                price=float(payload.get("price", 0) or 0),
                tp1=payload.get("tp1"),
                tp2=payload.get("tp2"),
                sl1=payload.get("sl1"),
                sl2=payload.get("sl2"),
                smart_trail=payload.get("smart_trail"),
                desk_id=payload.get("desks_matched", [None])[0],
                desks_matched=payload.get("desks_matched", []),
                is_valid=True,
                status="VALIDATED",
                webhook_latency_ms=payload.get("webhook_latency_ms"),
            )
            db.add(signal_record)
            db.commit()
            db.refresh(signal_record)

            logger.info(
                f"SIGNAL #{signal_record.id} | {symbol} | "
                f"{payload.get('alert_type')} | "
                f"Hurst={hurst} | Alignment={alignment_pct:.0%} | "
                f"SentimentRisk={sentiment_risk}"
            )

            # Route to full pipeline for enrichment → ML → CTO → execution
            latency_ms = payload.get("webhook_latency_ms")
            result = await process_signal(
                signal_record.id, db, webhook_latency_ms=latency_ms
            )

            logger.info(
                f"PIPELINE #{signal_record.id}: "
                f"{orjson.dumps({k: v.get('decision', 'N/A') for k, v in result.get('results', {}).items()}).decode()}"
            )

        except Exception as e:
            logger.error(f"Virtual trade execution failed: {e}", exc_info=True)
            db.rollback()
        finally:
            db.close()

    async def stop(self):
        """Graceful shutdown."""
        if self._redis:
            await self._redis.aclose()
            logger.info("Redis connection closed")


async def main():
    """Entry point for the worker process."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Register signal handlers for graceful shutdown
    signal_mod.signal(signal_mod.SIGINT, _handle_signal)
    signal_mod.signal(signal_mod.SIGTERM, _handle_signal)

    logger.info("=" * 60)
    logger.info("OniQuant v5.9 — Redis Stream Consumer Worker")
    logger.info(f"Stream: {STREAM_KEY} | Group: {CONSUMER_GROUP}")
    logger.info(f"Consumer: {CONSUMER_NAME}")
    logger.info("=" * 60)

    worker = VerificationWorker()
    try:
        await worker.start()
    finally:
        await worker.stop()


if __name__ == "__main__":
    import uvloop
    uvloop.install()
    asyncio.run(main())
