"""
OniQuant v6.1 — Redis Stream Consumer & Signal Generator

Persistent background process that consumes from the `oniquant_alerts`
Redis Stream and runs each signal through the pipeline.

Flow:
  1. Shadow log the signal
  2. Create Signal record → run full pipeline (enrichment → ML → CTO)
  3. Shadow post-pipeline update
  4. VirtualBroker sim for all profiles

Run as: python -m app.worker
Deploy on Railway as a separate 'Service' tab process.
"""
import os
import asyncio
import logging
import signal as signal_mod
from datetime import datetime, timezone
from typing import Dict, Optional

import orjson
import redis.asyncio as aioredis

from app.config import REDIS_URL, DESKS

# ─── Shadow Sim Engine Mode ───
SHADOW_MODE = os.getenv("SHADOW_MODE", "COLLECT")  # COLLECT | ML_GATE | DISABLED
SHADOW_TRADE_ALL = os.getenv("SHADOW_TRADE_ALL", "true").lower() in ("true", "1")

logger = logging.getLogger("TradingSystem.Worker")

# ─── Stream / Consumer Group config ───
STREAM_KEY = "oniquant_alerts"
CONSUMER_GROUP = "oniquant_workers"
CONSUMER_NAME = os.getenv("WORKER_ID", f"worker-{os.getpid()}")
BLOCK_MS = 5000           # Block 5s waiting for new messages
BATCH_SIZE = 10            # Read up to 10 messages per batch

# ─── Hurst Chop Zone — per-asset-class thresholds (0.45-0.50) ───
# Gold/crypto use lower thresholds (0.45) due to inherent volatility.
# See config.get_hurst_thresholds() for per-symbol lookups.

# ─── Graceful shutdown ───
_shutdown = asyncio.Event()


def _handle_signal(sig, frame):
    logger.info(f"Received {sig}, initiating graceful shutdown...")
    _shutdown.set()


# ═════════════════════════════════════════════════════════════════
# Performance Digest — Daily PnL Report at 5 PM Toronto
# ═════════════════════════════════════════════════════════════════

async def generate_daily_digest(db_session_factory) -> str:
    """
    Query ml_trade_logs for today's completed trades across all 6 desks.
    Returns formatted Telegram HTML message.
    """
    from sqlalchemy import func, case
    from app.models.ml_trade_log import MLTradeLog

    db = db_session_factory()
    try:
        today = datetime.now(timezone.utc).date()

        # Per-desk stats
        desk_rows = (
            db.query(
                MLTradeLog.desk_id,
                func.count(MLTradeLog.id).label("total"),
                func.count(case((MLTradeLog.outcome == "WIN", 1))).label("wins"),
                func.count(case((MLTradeLog.outcome == "LOSS", 1))).label("losses"),
                func.coalesce(func.sum(MLTradeLog.pnl_pips), 0).label("pnl_pips"),
                func.coalesce(func.sum(MLTradeLog.pnl_dollars), 0).label("pnl_dollars"),
                func.coalesce(func.avg(MLTradeLog.hurst_exponent), 0).label("avg_hurst"),
            )
            .filter(func.date(MLTradeLog.created_at) == today)
            .filter(MLTradeLog.outcome.isnot(None))
            .group_by(MLTradeLog.desk_id)
            .all()
        )

        # Totals
        total_trades = sum(r.total for r in desk_rows)
        total_wins = sum(r.wins for r in desk_rows)
        total_losses = sum(r.losses for r in desk_rows)
        total_pnl_pips = sum(float(r.pnl_pips) for r in desk_rows)
        total_pnl_dollars = sum(float(r.pnl_dollars) for r in desk_rows)
        win_rate = (total_wins / total_trades * 100) if total_trades > 0 else 0

        # Chop filter stats — how many vetoed today
        vetoed = (
            db.query(func.count(MLTradeLog.id))
            .filter(func.date(MLTradeLog.created_at) == today)
            .filter(MLTradeLog.block_reason.like("%Hurst%chop%"))
            .scalar() or 0
        )

        # Build desk lines
        desk_lines = []
        desk_emoji = {
            "DESK1_SCALPER": "🟢", "DESK2_INTRADAY": "🟡",
            "DESK3_SWING": "🔵", "DESK4_GOLD": "🔴",
            "DESK5_ALTS": "⚫", "DESK6_EQUITIES": "⚪",
        }
        for r in sorted(desk_rows, key=lambda x: float(x.pnl_dollars), reverse=True):
            wr = (r.wins / r.total * 100) if r.total > 0 else 0
            emoji = desk_emoji.get(r.desk_id, "▪️")
            pnl_indicator = "✅" if float(r.pnl_dollars) >= 0 else "❌"
            desk_lines.append(
                f"  {emoji} {r.desk_id}\n"
                f"     {r.wins}W / {r.losses}L ({wr:.0f}%) | "
                f"{pnl_indicator} {float(r.pnl_pips):+.1f} pips | "
                f"${float(r.pnl_dollars):+,.0f} | "
                f"H̄={float(r.avg_hurst):.2f}"
            )

        # Win rate target indicator
        if win_rate >= 69.5:
            target_badge = "🎯 TARGET HIT"
        elif win_rate >= 60:
            target_badge = "📈 ON TRACK"
        else:
            target_badge = "⚠️ BELOW TARGET"

        pnl_emoji = "✅" if total_pnl_dollars >= 0 else "❌"

        msg = (
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 DAILY PnL REPORT\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"  📅 {today.strftime('%A, %B %d %Y')}\n"
            f"  🎯 Win Rate: {win_rate:.1f}% {target_badge}\n"
            f"  {pnl_emoji} Net P&L: ${total_pnl_dollars:+,.2f} ({total_pnl_pips:+.1f} pips)\n"
            f"  📈 Trades: {total_trades} ({total_wins}W / {total_losses}L)\n"
            f"  🌊 Chop Vetoes: {vetoed} signals filtered\n"
            f"\n"
            f"┌─ DESK BREAKDOWN\n"
        )
        for line in desk_lines:
            msg += f"│\n│{line}\n"
        if not desk_lines:
            msg += "│  No completed trades today\n"
        msg += (
            f"└─────────────────────\n"
            f"\n"
            f"  ⚙️ Per-instrument Hurst thresholds (0.45-0.50)\n"
            f"  🤖 Zero-Key Oracle v6.1.0\n"
            f"━━━━━━━━━━━━━━━━━━━━━"
        )

        return msg

    finally:
        db.close()


async def send_daily_digest(db_session_factory):
    """Generate and send the daily PnL digest to Portfolio channel."""
    from app.services.telegram_bot import TelegramBot
    try:
        msg = await generate_daily_digest(db_session_factory)
        bot = TelegramBot()
        await bot._send_to_portfolio(msg)
        logger.info("Daily PnL digest sent to Portfolio channel")
    except Exception as e:
        logger.error(f"Daily digest failed: {e}", exc_info=True)


# ═════════════════════════════════════════════════════════════════
# Verification Worker
# ═════════════════════════════════════════════════════════════════

class VerificationWorker:
    """Consumes from Redis Stream and runs signals through the pipeline."""

    def __init__(self):
        self._redis: Optional[aioredis.Redis] = None
        self._db_session_factory = None

    async def start(self):
        """Initialize connections and start consuming."""
        self._redis = aioredis.from_url(
            REDIS_URL,
            decode_responses=False,
            max_connections=10,
        )

        # Lazy DB import
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
            f"Batch: {BATCH_SIZE} | Block: {BLOCK_MS}ms | "
            f"Per-instrument Hurst thresholds (0.45-0.50)"
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
        """Shadow log → create Signal → pipeline → shadow update → virtual broker sim."""
        symbol = payload.get("symbol_normalized", payload.get("symbol", "?"))
        alert_type = payload.get("alert_type", "?")
        desks = payload.get("desks_matched", [])

        source = payload.get("source", "tradingview")
        logger.info(
            f"PROCESSING | {msg_id.decode()} | {symbol} {alert_type} | "
            f"Desks: {desks} | Source: {source}"
        )

        # ── Shadow Log — captures EVERYTHING before pipeline ──
        shadow_id = None
        if SHADOW_MODE != "DISABLED":
            try:
                from app.services.shadow_logger import ShadowLogger
                shadow_logger = ShadowLogger(self._db_session_factory)
                db = self._db_session_factory()
                try:
                    shadow_id = await shadow_logger.log_signal(
                        db=db, payload=payload,
                        desk_id=desks[0] if desks else None,
                    )
                    db.commit()
                finally:
                    db.close()
            except Exception as e:
                logger.debug(f"Shadow logging failed (non-blocking): {e}")

        # ── Execute: Signal → Pipeline → Shadow update → VirtualBroker ──
        await self._execute_and_broadcast(
            payload=payload,
            shadow_id=shadow_id,
        )

    async def _execute_and_broadcast(
        self,
        payload: Dict,
        shadow_id: int = None,
    ):
        """
        Create Signal record → Pipeline → Shadow update → VirtualBroker sim.
        """
        from app.models.signal import Signal
        from app.services.pipeline import process_signal

        db = self._db_session_factory()
        try:
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
                f"{payload.get('alert_type')}"
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

            # ── Shadow: update with post-pipeline data + sim trade ──
            if shadow_id and SHADOW_MODE != "DISABLED":
                try:
                    from app.services.shadow_logger import ShadowLogger
                    shadow_logger = ShadowLogger(self._db_session_factory)
                    # Determine if any desk was approved
                    any_approved = any(
                        r.get("approved") for r in result.get("results", {}).values()
                    )
                    # Get enrichment/scoring from first desk result
                    first_result = next(iter(result.get("results", {}).values()), {})
                    shadow_logger.update_post_pipeline(
                        db, shadow_id,
                        enrichment=first_result.get("enrichment_summary"),
                        ml_result={"ml_score": first_result.get("ml_score")},
                        consensus={"total_score": first_result.get("consensus_score"),
                                   "tier": first_result.get("consensus_tier")},
                        decision={"decision": first_result.get("decision"),
                                  "reasoning": first_result.get("reasoning"),
                                  "confidence": first_result.get("size_multiplier")},
                        live_approved=any_approved,
                    )
                    db.commit()
                except Exception as e:
                    logger.debug(f"Shadow post-pipeline update failed: {e}")

                # Virtual broker sim for ALL signals
                if shadow_id and SHADOW_TRADE_ALL:
                    try:
                        from app.services.virtual_broker import VirtualBroker
                        broker = VirtualBroker(self._db_session_factory)
                        await broker.execute_for_all_profiles(db, shadow_id)
                        db.commit()
                    except Exception as e:
                        logger.debug(f"Virtual broker sim failed: {e}")

        except Exception as e:
            logger.error(f"Execute & broadcast failed: {e}", exc_info=True)
            db.rollback()
        finally:
            db.close()

    async def stop(self):
        """Graceful shutdown."""
        if self._redis:
            await self._redis.aclose()
            logger.info("Redis connection closed")


# ═════════════════════════════════════════════════════════════════
# Health Server — minimal HTTP so Railway healthcheck passes
# ═════════════════════════════════════════════════════════════════

async def _run_health_server():
    """Minimal async HTTP health server so Railway healthcheck passes."""
    port = int(os.getenv("PORT", "8080"))
    async def handle_client(reader, writer):
        try:
            await reader.read(4096)
            body = b'{"status":"worker_alive"}'
            response = (
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: application/json\r\n"
                b"Content-Length: " + str(len(body)).encode() + b"\r\n"
                b"\r\n" + body
            )
            writer.write(response)
            await writer.drain()
        except Exception:
            pass
        finally:
            writer.close()
    server = await asyncio.start_server(handle_client, "0.0.0.0", port)
    logger.info(f"Worker health server listening on :{port}")


# ═════════════════════════════════════════════════════════════════
# Entry Point
# ═════════════════════════════════════════════════════════════════

async def main():
    """Entry point for the worker process."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    signal_mod.signal(signal_mod.SIGINT, _handle_signal)
    signal_mod.signal(signal_mod.SIGTERM, _handle_signal)

    logger.info("=" * 60)
    logger.info("OniQuant v6.1.0 — Signal Generator Worker")
    logger.info(f"Stream: {STREAM_KEY} | Group: {CONSUMER_GROUP}")
    logger.info(f"Consumer: {CONSUMER_NAME}")
    logger.info("Per-instrument Hurst thresholds (0.45-0.50)")
    logger.info(f"Broker: VirtualBroker (Shadow Sim)")
    logger.info("=" * 60)

    worker = VerificationWorker()
    try:
        await _run_health_server()
        await worker.start()
    finally:
        await worker.stop()


if __name__ == "__main__":
    import uvloop
    uvloop.install()
    asyncio.run(main())
