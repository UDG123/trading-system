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
    Query ml_trade_logs + sim_positions for today's metrics.
    All queries use COALESCE and defensive error handling so the digest
    never crashes even if v7 columns are missing.
    """
    from sqlalchemy import text as sa_text

    db = db_session_factory()
    try:
        today = datetime.now(timezone.utc).date()
        today_str = today.strftime("%Y-%m-%d")

        # ── Per-desk core stats (defensive — COALESCE for optional columns) ──
        desk_rows = []
        try:
            desk_rows = db.execute(sa_text("""
                SELECT
                    desk_id,
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE outcome = 'WIN') AS wins,
                    COUNT(*) FILTER (WHERE outcome = 'LOSS') AS losses,
                    COALESCE(SUM(pnl_pips), 0) AS pnl_pips,
                    COALESCE(SUM(pnl_dollars), 0) AS pnl_dollars,
                    COALESCE(AVG(hurst_exponent), 0) AS avg_hurst,
                    COALESCE(AVG(quality_score), 0) AS avg_quality,
                    COALESCE(
                        (SELECT regime_label FROM ml_trade_logs sub
                         WHERE sub.desk_id = ml_trade_logs.desk_id
                           AND DATE(sub.created_at) = :today
                           AND sub.regime_label IS NOT NULL
                         ORDER BY sub.created_at DESC LIMIT 1),
                        'N/A'
                    ) AS last_regime
                FROM ml_trade_logs
                WHERE DATE(created_at) = :today AND outcome IS NOT NULL
                GROUP BY desk_id
                ORDER BY COALESCE(SUM(pnl_dollars), 0) DESC
            """), {"today": today_str}).fetchall()
        except Exception as e:
            logger.debug(f"Desk stats query failed, trying fallback: {e}")
            try:
                desk_rows = db.execute(sa_text("""
                    SELECT desk_id,
                           COUNT(*) AS total,
                           COUNT(*) FILTER (WHERE outcome = 'WIN') AS wins,
                           COUNT(*) FILTER (WHERE outcome = 'LOSS') AS losses,
                           COALESCE(SUM(pnl_pips), 0) AS pnl_pips,
                           COALESCE(SUM(pnl_dollars), 0) AS pnl_dollars,
                           0 AS avg_hurst, 0 AS avg_quality, 'N/A' AS last_regime
                    FROM ml_trade_logs
                    WHERE DATE(created_at) = :today AND outcome IS NOT NULL
                    GROUP BY desk_id
                    ORDER BY COALESCE(SUM(pnl_dollars), 0) DESC
                """), {"today": today_str}).fetchall()
            except Exception:
                desk_rows = []

        # Totals
        total_trades = sum(int(r[1]) for r in desk_rows)
        total_wins = sum(int(r[2]) for r in desk_rows)
        total_losses = sum(int(r[3]) for r in desk_rows)
        total_pnl_pips = sum(float(r[4]) for r in desk_rows)
        total_pnl_dollars = sum(float(r[5]) for r in desk_rows)
        win_rate = (total_wins / total_trades * 100) if total_trades > 0 else 0

        # ── Regime distribution today ──
        regime_dist = {"TRENDING_UP": 0, "TRENDING_DOWN": 0, "RANGING": 0}
        try:
            regime_rows = db.execute(sa_text("""
                SELECT hmm_regime, COUNT(*) AS cnt
                FROM shadow_signals
                WHERE DATE(created_at) = :today
                  AND hmm_regime IS NOT NULL
                GROUP BY hmm_regime
            """), {"today": today_str}).fetchall()
            regime_total = sum(int(r[1]) for r in regime_rows) or 1
            for r in regime_rows:
                regime_dist[r[0]] = round(int(r[1]) / regime_total * 100, 1)
        except Exception:
            pass

        trending_pct = regime_dist.get("TRENDING_UP", 0) + regime_dist.get("TRENDING_DOWN", 0)
        ranging_pct = regime_dist.get("RANGING", 0)

        # ── Exit tier breakdown (from sim_positions) ──
        tier_counts = {0: 0, 1: 0, 2: 0, 3: 0}
        time_exits = 0
        try:
            tier_rows = db.execute(sa_text("""
                SELECT
                    COALESCE(exit_tier, 0) AS tier,
                    COUNT(*) AS cnt,
                    COUNT(*) FILTER (WHERE time_based_exit = TRUE) AS timed
                FROM sim_positions
                WHERE DATE(closed_at) = :today AND status = 'CLOSED'
                GROUP BY COALESCE(exit_tier, 0)
            """), {"today": today_str}).fetchall()
            for r in tier_rows:
                tier_counts[int(r[0])] = int(r[1])
                time_exits += int(r[2])
        except Exception:
            pass

        # ── Signal quality pass rate ──
        emitted = 0
        total_candidates = 0
        avg_quality_emitted = 0
        avg_quality_skipped = 0
        try:
            qual_row = db.execute(sa_text("""
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE approved = TRUE) AS emitted,
                    COALESCE(AVG(quality_score) FILTER (WHERE approved = TRUE), 0) AS avg_q_emit,
                    COALESCE(AVG(quality_score) FILTER (WHERE approved = FALSE), 0) AS avg_q_skip
                FROM ml_trade_logs
                WHERE DATE(created_at) = :today
            """), {"today": today_str}).fetchone()
            if qual_row:
                total_candidates = int(qual_row[0])
                emitted = int(qual_row[1])
                avg_quality_emitted = float(qual_row[2])
                avg_quality_skipped = float(qual_row[3])
        except Exception:
            pass

        pass_rate = (emitted / total_candidates * 100) if total_candidates > 0 else 0

        # ── Market hours filter stats (in-memory counters from signal engine) ──
        mh_filtered = 0
        try:
            from app.services.signal_engine.market_hours_filter import get_filter_stats
            mh_stats = get_filter_stats()
            mh_filtered = mh_stats.get("filtered", 0)
        except Exception:
            pass

        # ── Build desk lines ──
        desk_lines = []
        desk_emoji = {
            "DESK1_SCALPER": "🟢", "DESK2_INTRADAY": "🟡",
            "DESK3_SWING": "🔵", "DESK4_GOLD": "🔴",
            "DESK5_ALTS": "⚫", "DESK6_EQUITIES": "⚪",
        }
        for r in desk_rows:
            desk_id = r[0]
            total = int(r[1])
            wins = int(r[2])
            losses = int(r[3])
            pnl_pips = float(r[4])
            pnl_dollars = float(r[5])
            avg_quality = float(r[7])
            last_regime = r[8]
            wr = (wins / total * 100) if total > 0 else 0
            emoji = desk_emoji.get(desk_id, "▪️")
            pnl_icon = "✅" if pnl_dollars >= 0 else "❌"
            quality_str = f"Q={avg_quality:.0f}" if avg_quality > 0 else ""
            regime_str = last_regime if last_regime != "N/A" else ""
            desk_lines.append(
                f"  {emoji} {desk_id}\n"
                f"     {wins}W/{losses}L ({wr:.0f}%) | "
                f"{pnl_icon} {pnl_pips:+.1f}p ${pnl_dollars:+,.0f}"
                f"{' | ' + quality_str if quality_str else ''}"
                f"{' | ' + regime_str if regime_str else ''}"
            )

        # ── Win rate badge ──
        if win_rate >= 69.5:
            target_badge = "🎯 TARGET HIT"
        elif win_rate >= 60:
            target_badge = "📈 ON TRACK"
        else:
            target_badge = "⚠️ BELOW TARGET"

        pnl_emoji = "✅" if total_pnl_dollars >= 0 else "❌"

        # ── Build message ──
        msg = (
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"📊 Daily Digest — {today.strftime('%b %d %Y')}\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"🏢 Firm: {target_badge} | Trades: {total_trades} | "
            f"{pnl_emoji} ${total_pnl_dollars:+,.2f}\n"
            f"🎯 Win Rate: {win_rate:.1f}% ({total_wins}W / {total_losses}L)\n"
            f"\n"
            f"┌─ DESK BREAKDOWN\n"
        )
        for line in desk_lines:
            msg += f"│\n│{line}\n"
        if not desk_lines:
            msg += "│  No completed trades today\n"
        msg += f"└─────────────────────\n\n"

        # Signal quality section
        msg += (
            f"📈 Signal Quality: {emitted}/{total_candidates} "
            f"({pass_rate:.0f}% pass rate)"
        )
        if avg_quality_emitted > 0:
            msg += f"\n   Emitted avg: {avg_quality_emitted:.0f}/100"
        if avg_quality_skipped > 0:
            msg += f" | Skipped avg: {avg_quality_skipped:.0f}/100"
        msg += "\n"

        # Time filter
        if mh_filtered > 0:
            msg += f"⏰ Time-filtered: {mh_filtered} signals skipped\n"

        # Regime distribution
        if trending_pct > 0 or ranging_pct > 0:
            msg += (
                f"🔄 Regime: {trending_pct:.0f}% trending, "
                f"{ranging_pct:.0f}% ranging\n"
            )

        # Exit tier breakdown
        t1 = tier_counts.get(1, 0)
        t2 = tier_counts.get(2, 0)
        t3 = tier_counts.get(3, 0)
        t0 = tier_counts.get(0, 0)
        if t1 + t2 + t3 + time_exits > 0:
            msg += (
                f"🎯 Exits: {t1} Tier1, {t2} Tier2, {t3} Tier3"
                f"{f', {time_exits} timed out' if time_exits > 0 else ''}"
                f"{f', {t0} SL pre-tier' if t0 > 0 else ''}\n"
            )

        msg += (
            f"\n"
            f"  🤖 OniQuant v7.0\n"
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
