"""
OniQuant v5.9 — Redis Stream Consumer & MCP Verification Layer

Persistent background process that consumes from the `oniquant_alerts`
Redis Stream and runs each signal through the MCP verification gates
before executing virtual trades via LocalBroker.

Gates:
  1. NumPy Gate    → numpy-mcp:calculate_hurst (reject if H < 0.52 Chop Zone)
  2. altFINS Gate  → altfins-mcp:get_indicator_state (RSI/MACD/BB alignment > 70%)
  3. Paradox Gate  → paradox-mcp:get_sentiment (flag extreme inverse sentiment)

Execution:
  - LocalBroker (Virtual) — no external broker dependency (Canada-compliant)
  - TelegramService.broadcast_signal() with Master Mix OracleBridge layout

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

logger = logging.getLogger("TradingSystem.Worker")

# ─── Stream / Consumer Group config ───
STREAM_KEY = "oniquant_alerts"
CONSUMER_GROUP = "oniquant_workers"
CONSUMER_NAME = os.getenv("WORKER_ID", f"worker-{os.getpid()}")
BLOCK_MS = 5000           # Block 5s waiting for new messages
BATCH_SIZE = 10            # Read up to 10 messages per batch

# ─── Hurst Chop Zone — unified threshold for 69.5% win rate target ───
HURST_CHOP_THRESHOLD = 0.52   # H < 0.52 = Chop Zone → veto ALL desks
INDICATOR_ALIGNMENT_MIN = 0.70  # 70% indicator alignment required

# ─── Graceful shutdown ───
_shutdown = asyncio.Event()


def _handle_signal(sig, frame):
    logger.info(f"Received {sig}, initiating graceful shutdown...")
    _shutdown.set()


# ═════════════════════════════════════════════════════════════════
# LocalBroker — Virtual trade execution (no external broker)
# ═════════════════════════════════════════════════════════════════

class LocalBroker:
    """
    Virtual trade executor for Canadian compliance.
    Records trades in Railway Postgres with full metadata.
    No alpaca-py, no MetaApi, no external broker dependency.
    """

    def __init__(self, db_session_factory):
        self._db_factory = db_session_factory

    async def execute_virtual_trade(
        self,
        signal_id: int,
        symbol: str,
        direction: str,
        desk_id: str,
        entry_price: float,
        lot_size: float,
        risk_pct: float,
        risk_dollars: float,
        stop_loss: float = None,
        take_profit_1: float = None,
        take_profit_2: float = None,
        hurst: float = None,
        alignment_pct: float = None,
        sentiment_risk: bool = False,
        desk_idx: int = 0,
    ) -> Dict:
        """
        Create a virtual trade record in Postgres.
        Returns trade metadata dict.
        """
        from app.models.trade import Trade as TradeModel

        db = self._db_factory()
        try:
            trade_record = TradeModel(
                signal_id=signal_id,
                desk_id=desk_id,
                symbol=symbol,
                direction=direction,
                mt5_ticket=900000 + signal_id * 10 + desk_idx,
                entry_price=entry_price,
                lot_size=lot_size,
                risk_pct=risk_pct,
                risk_dollars=risk_dollars,
                stop_loss=stop_loss,
                take_profit_1=take_profit_1,
                take_profit_2=take_profit_2,
                status="VIRTUAL_OPEN",
                opened_at=datetime.now(timezone.utc),
                close_reason=f"H={hurst:.3f}" if hurst else None,
            )
            db.add(trade_record)
            db.commit()
            db.refresh(trade_record)

            logger.info(
                f"VIRTUAL TRADE #{trade_record.id} | {desk_id} | "
                f"{symbol} {direction} | Lot: {lot_size} | "
                f"Risk: ${risk_dollars:.2f} | H={hurst}"
            )

            return {
                "trade_id": trade_record.id,
                "status": "VIRTUAL_OPEN",
                "ticket": trade_record.mt5_ticket,
                "symbol": symbol,
                "direction": direction,
                "entry_price": entry_price,
                "lot_size": lot_size,
            }
        except Exception as e:
            db.rollback()
            logger.error(f"LocalBroker execute failed: {e}", exc_info=True)
            return {"trade_id": None, "status": "ERROR", "error": str(e)}
        finally:
            db.close()


# ═════════════════════════════════════════════════════════════════
# Telegram Broadcast — uses TelegramService + OracleBridge
# ═════════════════════════════════════════════════════════════════

class WorkerTelegramBroadcaster:
    """
    Wraps TelegramService (dual-routing) + OracleBridge (Master Mix layout).
    Portfolio channel sees everything; desk channels see only their signals.
    """

    def __init__(self):
        from app.services.telegram_notifications import TelegramService
        from app.services.oracle_bridge import OracleBridge
        self._tg = TelegramService()
        self._oracle = OracleBridge

    async def broadcast_signal(
        self,
        signal_data: Dict,
        trade_result: Dict,
        hurst: float = None,
        alignment_pct: float = None,
        sentiment_risk: bool = False,
        enrichment: Dict = None,
        ml_result: Dict = None,
    ):
        """
        Format via OracleBridge Master Mix layout, then dual-route
        via TelegramService to Portfolio + Desk channels.
        """
        enrichment = enrichment or {}
        ml_result = ml_result or {}
        symbol = signal_data.get("symbol_normalized", signal_data.get("symbol", "?"))
        desk_id = trade_result.get("desk_id", signal_data.get("desks_matched", ["?"])[0])

        # Build data dict for OracleBridge.format_strike_message
        strike_data = {
            "desk_id": desk_id,
            "ml_score": ml_result.get("ml_score", 0),
            "hurst": hurst or 0,
            "direction": signal_data.get("direction", "?"),
            "price": signal_data.get("price", 0),
            "rvol": enrichment.get("mse_rvol", 0),
            "vwap_z": enrichment.get("vwap_z_score", 0),
            "ml_conf": int((ml_result.get("ml_score", 0) / 5) * 100),
            "sl": signal_data.get("sl1", "N/A"),
            "tp1": signal_data.get("tp1", "N/A"),
            "tv_link": f"https://www.tradingview.com/chart/?symbol={symbol}",
        }

        # Format with Master Mix layout (Blue Bar + Alpha Metrics)
        message = self._oracle.format_strike_message(strike_data)

        # Dual-route: Portfolio + Desk channel
        await self._tg.broadcast_signal(desk_id, message)

        logger.info(
            f"BROADCAST | {symbol} {signal_data.get('direction')} | {desk_id} | "
            f"H={hurst} | Alignment={alignment_pct}"
        )


# ═════════════════════════════════════════════════════════════════
# MCP Client
# ═════════════════════════════════════════════════════════════════

class MCPClient:
    """Thin wrapper for MCP tool calls via Redis pub/sub."""

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
                f"  {emoji} <b>{r.desk_id}</b>\n"
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
            f"📊 <b>DAILY PnL REPORT</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━━\n"
            f"\n"
            f"  📅 {today.strftime('%A, %B %d %Y')}\n"
            f"  🎯 Win Rate: <b>{win_rate:.1f}%</b> {target_badge}\n"
            f"  {pnl_emoji} Net P&L: <b>${total_pnl_dollars:+,.2f}</b> ({total_pnl_pips:+.1f} pips)\n"
            f"  📈 Trades: {total_trades} ({total_wins}W / {total_losses}L)\n"
            f"  🌊 Chop Vetoes: {vetoed} signals filtered\n"
            f"\n"
            f"┌─ <b>DESK BREAKDOWN</b>\n"
        )
        for line in desk_lines:
            msg += f"│\n│{line}\n"
        if not desk_lines:
            msg += "│  No completed trades today\n"
        msg += (
            f"└─────────────────────\n"
            f"\n"
            f"  ⚙️ Hurst Chop Zone: H < {HURST_CHOP_THRESHOLD}\n"
            f"  🤖 Zero-Key Oracle v5.9\n"
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
    """Consumes from Redis Stream and runs MCP verification gates."""

    def __init__(self):
        self._redis: Optional[aioredis.Redis] = None
        self._mcp: Optional[MCPClient] = None
        self._db_session_factory = None
        self._local_broker: Optional[LocalBroker] = None
        self._telegram: Optional[WorkerTelegramBroadcaster] = None

    async def start(self):
        """Initialize connections and start consuming."""
        self._redis = aioredis.from_url(
            REDIS_URL,
            decode_responses=False,
            max_connections=10,
        )
        self._mcp = MCPClient(self._redis)

        # Lazy DB import
        from app.database import SessionLocal
        self._db_session_factory = SessionLocal

        # LocalBroker + Telegram (OracleBridge Master Mix layout)
        self._local_broker = LocalBroker(self._db_session_factory)
        self._telegram = WorkerTelegramBroadcaster()

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
            f"Hurst Chop Zone: H < {HURST_CHOP_THRESHOLD}"
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
        """Run the three MCP verification gates on a signal."""
        symbol = payload.get("symbol_normalized", payload.get("symbol", "?"))
        alert_type = payload.get("alert_type", "?")
        direction = payload.get("direction")
        desks = payload.get("desks_matched", [])

        logger.info(
            f"PROCESSING | {msg_id.decode()} | {symbol} {alert_type} | "
            f"Desks: {desks}"
        )

        # ── Gate 1: NumPy Hurst (Chop Zone veto — ALL desks) ──
        hurst_result = await self._gate_hurst(symbol)
        if hurst_result.get("rejected"):
            logger.info(
                f"VETOED (Chop Zone) | {symbol} | H={hurst_result.get('hurst', '?'):.3f} "
                f"< {HURST_CHOP_THRESHOLD} | signal discarded"
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

        # ── Gate 3: Paradox Sentiment (advisory flag, does not veto) ──
        sentiment_result = await self._gate_sentiment(symbol, direction)
        risk_flag = sentiment_result.get("high_risk", False)
        if risk_flag:
            logger.warning(
                f"HIGH RISK (Sentiment) | {symbol} {direction} | "
                f"Sentiment strongly inverse — flagging trade"
            )

        # ── All gates passed → DB record + LocalBroker + TG Broadcast ──
        await self._execute_and_broadcast(
            payload=payload,
            hurst=hurst_result.get("hurst"),
            alignment_pct=alignment_result.get("alignment_pct", 0),
            sentiment_risk=risk_flag,
            indicators=alignment_result.get("indicators", {}),
        )

    async def _gate_hurst(self, symbol: str) -> Dict:
        """
        Gate 1: Dynamic Hurst exponent check (time-aware regime filtering).
        Threshold adjusts by EDT session:
          - London/NY Overlap (8-12 EDT): H < 0.52 (trends reliable)
          - NY Lunch (12-14 EDT):         H < 0.58 (high chop risk)
          - Asian (19-03 EDT):            H < 0.55 (mean-reversion bias)
        This is the primary win-rate lever for 69.5% target.
        """
        result = await self._mcp.call_tool(
            "numpy-mcp", "calculate_hurst", {"symbol": symbol}
        )

        if "error" in result:
            logger.warning(f"Hurst MCP unavailable for {symbol}, passing through")
            return {"rejected": False, "hurst": None}

        hurst = result.get("hurst_exponent")
        if hurst is None:
            return {"rejected": False, "hurst": None}

        # Dynamic threshold based on current EDT session
        from app.services.twelvedata_enricher import TwelveDataEnricher
        dynamic_threshold = TwelveDataEnricher().get_dynamic_hurst_threshold()

        if hurst < dynamic_threshold:
            logger.info(
                f"CHOP ZONE | {symbol} | H={hurst:.3f} < {dynamic_threshold} "
                f"(session-adjusted threshold)"
            )
            return {"rejected": True, "hurst": hurst, "threshold": dynamic_threshold}

        return {"rejected": False, "hurst": hurst, "threshold": dynamic_threshold}

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

        aligned = 0
        total = 0

        rsi_state = indicators.get("RSI", {})
        if rsi_state:
            total += 1
            rsi_val = rsi_state.get("value", 50)
            if direction == "LONG" and rsi_val < 70:
                aligned += 1
            elif direction == "SHORT" and rsi_val > 30:
                aligned += 1

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
            position = bb_state.get("position", "middle")
            if direction == "LONG" and position != "upper":
                aligned += 1
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
        Flag as 'High Risk' if social sentiment is strongly inverse.
        """
        result = await self._mcp.call_tool(
            "paradox-mcp", "get_sentiment", {"symbol": symbol}
        )

        if "error" in result:
            logger.warning(f"Paradox MCP unavailable for {symbol}, passing through")
            return {"high_risk": False, "sentiment": None}

        sentiment = result.get("sentiment", "neutral")
        strength = result.get("strength", "moderate")

        if not direction or direction == "EXIT":
            return {"high_risk": False, "sentiment": sentiment}

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

    async def _execute_and_broadcast(
        self,
        payload: Dict,
        hurst: Optional[float],
        alignment_pct: float,
        sentiment_risk: bool,
        indicators: Dict,
    ):
        """
        Create Signal record → Pipeline → LocalBroker → TG Broadcast.
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
                f"{payload.get('alert_type')} | "
                f"Hurst={hurst} | Alignment={alignment_pct:.0%} | "
                f"SentimentRisk={sentiment_risk}"
            )

            # Route to full pipeline for enrichment → ML → CTO → execution
            latency_ms = payload.get("webhook_latency_ms")
            result = await process_signal(
                signal_record.id, db, webhook_latency_ms=latency_ms
            )

            # For each desk that was APPROVED, do LocalBroker + TG broadcast
            for desk_id, desk_result in result.get("results", {}).items():
                if not desk_result.get("approved"):
                    continue

                trade_params = desk_result.get("trade_params", {})

                # Execute via LocalBroker (virtual trade record)
                trade_result = await self._local_broker.execute_virtual_trade(
                    signal_id=signal_record.id,
                    symbol=symbol,
                    direction=payload.get("direction", "LONG"),
                    desk_id=desk_id,
                    entry_price=float(trade_params.get("price", 0)),
                    lot_size=float(trade_params.get("risk_pct", 0.5)),
                    risk_pct=float(trade_params.get("risk_pct", 0.5)),
                    risk_dollars=float(trade_params.get("risk_dollars", 0)),
                    stop_loss=trade_params.get("stop_loss"),
                    take_profit_1=trade_params.get("take_profit_1"),
                    take_profit_2=trade_params.get("take_profit_2"),
                    hurst=hurst,
                    alignment_pct=alignment_pct,
                    sentiment_risk=sentiment_risk,
                )
                trade_result["desk_id"] = desk_id

                # Broadcast via TelegramService (Master Mix layout)
                await self._telegram.broadcast_signal(
                    signal_data=payload,
                    trade_result=trade_result,
                    hurst=hurst,
                    alignment_pct=alignment_pct,
                    sentiment_risk=sentiment_risk,
                )

            logger.info(
                f"PIPELINE #{signal_record.id}: "
                f"{orjson.dumps({k: v.get('decision', 'N/A') for k, v in result.get('results', {}).items()}).decode()}"
            )

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
    logger.info("OniQuant v5.9 — Zero-Key Oracle Worker")
    logger.info(f"Stream: {STREAM_KEY} | Group: {CONSUMER_GROUP}")
    logger.info(f"Consumer: {CONSUMER_NAME}")
    logger.info(f"Hurst Chop Zone: H < {HURST_CHOP_THRESHOLD}")
    logger.info(f"Broker: LocalBroker (Virtual)")
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
