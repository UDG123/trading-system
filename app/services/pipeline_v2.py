"""
Streamlined Signal Pipeline v2 — 5-step processing replaces 17-step chain.

Steps:
  1. VALIDATE: dedup + cooldown + market hours + max positions + daily loss limit
  2. SCORE: weighted quality score (0-100) modulates SIZE not existence
  3. SIZE: quality score → position size (80+=full, 60-79=75%, 40-59=50%, <40=25%)
  4. ROUTE: Redis pub/sub channels per desk
  5. NOTIFY: Telegram alert + shadow log + ML training log

Claude review runs ASYNC (fire-and-forget) after signal routes.
Result logged to ML table only — never blocks signal flow.
"""
import asyncio
import json
import logging
import time
from datetime import datetime, timezone
from typing import Dict, Optional

from sqlalchemy.orm import Session

from app.models.signal import Signal
from app.config import (
    DESKS, CAPITAL_PER_ACCOUNT, PORTFOLIO_CAPITAL_PER_DESK,
    get_pip_info, calculate_lot_size, get_atr_settings,
    DEDUP_WINDOW_MINUTES, MAX_TOTAL_OPEN_POSITIONS,
)

logger = logging.getLogger("TradingSystem.PipelineV2")

# Redis pub/sub channel names
CHANNEL_RAW = "signals:raw"
CHANNEL_SCORED = "signals:scored"
CHANNEL_APPROVED = "signals:approved"
CHANNEL_DESK_PREFIX = "signals:desk:"


class PipelineV2:
    """Streamlined 5-step signal pipeline."""

    def __init__(self):
        self._enricher = None
        self._telegram = None
        self._price_service = None

    def _get_services(self):
        if self._enricher is None:
            from app.services.twelvedata_enricher import TwelveDataEnricher
            from app.services.telegram_bot import TelegramBot
            from app.services.price_service import PriceService
            self._enricher = TwelveDataEnricher()
            self._telegram = TelegramBot()
            self._price_service = PriceService()
        return self._enricher, self._telegram, self._price_service

    async def process_signal(
        self, signal_id: int, db: Session,
        webhook_latency_ms: int = None,
        redis=None,
    ) -> Dict:
        """
        5-step pipeline. Returns results dict per desk.
        """
        pipeline_start = time.time()
        enricher, telegram, price_service = self._get_services()

        # ── Load signal ──
        signal = db.query(Signal).filter(Signal.id == signal_id).first()
        if not signal:
            return {"status": "error", "message": "Signal not found"}
        if signal.status == "REJECTED":
            return {"status": "skipped", "message": "Already rejected"}

        signal_data = {
            "symbol": signal.symbol_normalized,
            "timeframe": signal.timeframe,
            "alert_type": signal.alert_type,
            "direction": signal.direction,
            "price": signal.price,
            "tp1": signal.tp1, "tp2": signal.tp2,
            "sl1": signal.sl1, "sl2": signal.sl2,
            "smart_trail": signal.smart_trail,
            "webhook_latency_ms": webhook_latency_ms,
        }

        desks = signal.desks_matched or []
        if not desks:
            signal.status = "REJECTED"
            db.commit()
            return {"status": "rejected", "message": "No desk match"}

        # Publish to raw channel
        if redis:
            try:
                import orjson
                await redis.publish(CHANNEL_RAW, orjson.dumps(signal_data))
            except Exception:
                pass

        results = {}

        for desk_id in desks:
            desk_start = time.time()

            try:
                # ═══ STEP 1: VALIDATE (hard gates only) ═══
                reject = self._step1_validate(db, signal, signal_data, desk_id)
                if reject:
                    results[desk_id] = reject
                    continue

                # ═══ STEP 2: SCORE (weighted quality 0-100) ═══
                enrichment = {}
                try:
                    enrichment = await enricher.enrich(
                        signal.symbol_normalized, signal.timeframe, signal.price,
                    )
                    signal.status = "ENRICHED"
                    db.commit()
                except Exception as e:
                    logger.debug(f"Enrichment failed: {e}")

                # Live price
                try:
                    live_price = await price_service.get_price(signal.symbol_normalized)
                    if live_price and live_price > 0:
                        signal_data["price"] = live_price
                except Exception:
                    pass

                # ATR SL/TP
                price = signal_data.get("price", 0)
                atr = enrichment.get("atr")
                if price > 0 and atr and atr > 0 and not signal_data.get("sl1"):
                    cfg = get_atr_settings(desk_id, signal.symbol_normalized, signal.timeframe)
                    direction = signal_data.get("direction", "LONG")
                    sl_mult = cfg.get("sl_mult", 2.0)
                    tp1_mult = cfg.get("tp1_mult", 4.0)
                    if direction == "LONG":
                        signal_data["sl1"] = round(price - atr * sl_mult, 5)
                        signal_data["tp1"] = round(price + atr * tp1_mult, 5)
                    else:
                        signal_data["sl1"] = round(price + atr * sl_mult, 5)
                        signal_data["tp1"] = round(price - atr * tp1_mult, 5)

                quality = self._step2_score(signal_data, enrichment, desk_id)

                # Publish scored
                if redis:
                    try:
                        import orjson
                        await redis.publish(CHANNEL_SCORED, orjson.dumps({
                            "symbol": signal.symbol_normalized,
                            "quality_score": quality["quality_score"],
                            "desk_id": desk_id,
                        }))
                    except Exception:
                        pass

                # ═══ STEP 3: SIZE (quality → position size) ═══
                desk = DESKS.get(desk_id, {})
                risk_pct = desk.get("risk_pct", 1.0)
                size_mult = quality["size_multiplier"]
                effective_risk = round(risk_pct * size_mult, 4)
                risk_dollars = CAPITAL_PER_ACCOUNT * (effective_risk / 100)

                # Lot size
                pip_size, pip_value = get_pip_info(signal_data.get("symbol", ""))
                sl_price = signal_data.get("sl1", 0)
                entry_price = signal_data.get("price", 0)
                sl_pips = abs(float(entry_price) - float(sl_price)) / pip_size if entry_price and sl_price and pip_size else 0
                desk_capital = PORTFOLIO_CAPITAL_PER_DESK.get(desk_id, CAPITAL_PER_ACCOUNT)
                lot_size = calculate_lot_size(
                    desk_id=desk_id, symbol=signal_data.get("symbol", ""),
                    risk_pct=effective_risk, sl_pips=sl_pips,
                    account_capital=desk_capital,
                )

                # ═══ STEP 4: ROUTE (create trade + publish) ═══
                from app.models.trade import Trade as TradeModel
                trade_record = TradeModel(
                    signal_id=signal.id,
                    desk_id=desk_id,
                    symbol=signal_data.get("symbol"),
                    direction=signal_data.get("direction"),
                    entry_price=entry_price,
                    lot_size=lot_size,
                    risk_pct=effective_risk,
                    risk_dollars=round(risk_dollars, 2),
                    stop_loss=signal_data.get("sl1"),
                    take_profit_1=signal_data.get("tp1"),
                    take_profit_2=signal_data.get("tp2"),
                    status="SIM_OPEN",
                    opened_at=datetime.now(timezone.utc),
                )
                db.add(trade_record)
                db.flush()

                signal.status = "SIM_OPEN"
                signal.desk_id = desk_id
                signal.position_size_pct = effective_risk

                # Publish to approved + desk channels
                if redis:
                    try:
                        import orjson
                        msg = orjson.dumps({
                            "trade_id": trade_record.id,
                            "symbol": signal_data.get("symbol"),
                            "direction": signal_data.get("direction"),
                            "desk_id": desk_id,
                            "quality_score": quality["quality_score"],
                            "size_mult": size_mult,
                        })
                        await redis.publish(CHANNEL_APPROVED, msg)
                        await redis.publish(f"{CHANNEL_DESK_PREFIX}{desk_id}", msg)
                    except Exception:
                        pass

                # ═══ STEP 5: NOTIFY (Telegram + ML log) ═══
                trade_params = {
                    "desk_id": desk_id,
                    "symbol": signal_data.get("symbol"),
                    "direction": signal_data.get("direction"),
                    "price": signal_data.get("price"),
                    "timeframe": signal_data.get("timeframe"),
                    "alert_type": signal_data.get("alert_type"),
                    "risk_pct": effective_risk,
                    "risk_dollars": round(risk_dollars, 2),
                    "stop_loss": signal_data.get("sl1"),
                    "take_profit_1": signal_data.get("tp1"),
                    "quality_score": quality["quality_score"],
                    "quality_tier": quality["tier"],
                    "size_multiplier": size_mult,
                    "regime": quality.get("regime", ""),
                }
                decision_stub = {
                    "decision": "EXECUTE",
                    "reasoning": f"Quality {quality['quality_score']}/100 ({quality['tier']})",
                    "size_multiplier": size_mult,
                    "confidence": quality["quality_score"] / 100,
                }
                await telegram.notify_trade_entry(trade_params, decision_stub)

                # Fire-and-forget Claude review (async, non-blocking)
                asyncio.create_task(
                    self._async_claude_review(
                        signal_data, enrichment, quality, desk_id, db, signal.id
                    )
                )

                db.commit()

                desk_time = int((time.time() - desk_start) * 1000)
                results[desk_id] = {
                    "decision": "EXECUTE",
                    "approved": True,
                    "quality_score": quality["quality_score"],
                    "quality_tier": quality["tier"],
                    "size_multiplier": size_mult,
                    "trade_id": trade_record.id,
                    "processing_time_ms": desk_time,
                }

                logger.info(
                    f"V2 TRADE #{trade_record.id} | {desk_id} | "
                    f"{signal_data.get('symbol')} {signal_data.get('direction')} | "
                    f"Quality: {quality['quality_score']}/100 | Size: {size_mult*100:.0f}% | "
                    f"{desk_time}ms"
                )

            except Exception as e:
                logger.error(f"Pipeline V2 error for {desk_id}: {e}", exc_info=True)
                try:
                    db.rollback()
                except Exception:
                    pass
                results[desk_id] = {"decision": "SKIP", "approved": False, "error": str(e)}

        total_ms = int((time.time() - pipeline_start) * 1000)
        signal.processing_time_ms = total_ms
        db.commit()

        return {
            "status": "processed",
            "signal_id": signal_id,
            "results": results,
            "total_processing_time_ms": total_ms,
        }

    # ═══════════════════════════════════════════════════════════
    # STEP 1: VALIDATE — only hard safety gates
    # ═══════════════════════════════════════════════════════════

    def _step1_validate(
        self, db: Session, signal: Signal, signal_data: Dict, desk_id: str,
    ) -> Optional[Dict]:
        """Returns rejection dict if signal fails hard gates, None if passes."""
        from sqlalchemy import text as sa_text

        # 1a. Dedup (15-min window)
        try:
            from datetime import timedelta
            cutoff = datetime.now(timezone.utc) - timedelta(minutes=DEDUP_WINDOW_MINUTES)
            dup = db.execute(
                sa_text("""
                    SELECT id FROM signals
                    WHERE symbol_normalized = :sym AND alert_type = :atype
                      AND desk_id = :desk AND direction = :dir
                      AND received_at > :cutoff
                      AND status NOT IN ('DUPLICATE', 'RECEIVED', 'VALIDATED')
                      AND id != :self_id
                    ORDER BY received_at DESC LIMIT 1
                """),
                {
                    "sym": signal.symbol_normalized, "atype": signal.alert_type,
                    "desk": desk_id, "dir": signal.direction or "",
                    "cutoff": cutoff, "self_id": signal.id,
                },
            ).fetchone()
            if dup:
                signal.status = "DUPLICATE"
                db.commit()
                return {"decision": "SKIP", "approved": False, "rejection_reason": "Duplicate"}
        except Exception:
            pass

        # 1b. Market hours
        try:
            from app.services.signal_engine.market_hours_filter import is_valid_trading_hour
            if not is_valid_trading_hour(signal.symbol_normalized, desk_id):
                return {"decision": "SKIP", "approved": False, "rejection_reason": "Market closed"}
        except Exception:
            pass

        # 1c. Max positions per desk
        try:
            from app.models.trade import Trade
            from sqlalchemy import func
            open_count = db.query(func.count(Trade.id)).filter(
                Trade.desk_id == desk_id,
                Trade.status.in_(["OPEN", "SIM_OPEN"]),
            ).scalar() or 0
            desk = DESKS.get(desk_id, {})
            max_open = desk.get("max_simultaneous", 5)
            if open_count >= max_open:
                return {"decision": "SKIP", "approved": False,
                        "rejection_reason": f"Max positions ({max_open})"}
        except Exception:
            pass

        # 1d. Daily loss limit
        try:
            from app.services.risk_filter import HardRiskFilter
            rf = HardRiskFilter()
            desk_state = rf.get_desk_state(db, desk_id)
            daily_loss = abs(desk_state.get("daily_loss", 0))
            if daily_loss >= 4500:
                return {"decision": "SKIP", "approved": False,
                        "rejection_reason": f"Daily loss ${daily_loss:.0f}"}
        except Exception:
            pass

        return None  # All gates passed

    # ═══════════════════════════════════════════════════════════
    # STEP 2: SCORE — weighted quality 0-100
    # ═══════════════════════════════════════════════════════════

    def _step2_score(
        self, signal_data: Dict, enrichment: Dict, desk_id: str,
    ) -> Dict:
        """
        Weighted quality score (0-100):
          MTF alignment:       25%
          Indicator confluence: 25%
          R:R ratio:           20%
          Regime context:      15%
          Volatility context:  15%

        Score modulates SIZE:
          80+  → 100% (full)
          60-79 → 75%
          40-59 → 50%
          <40   → 25%
        """
        score = 0.0
        breakdown = {}

        # 1. MTF Alignment (25 pts)
        ema_aligned = enrichment.get("ema_aligned_bull") or enrichment.get("ema_aligned_bear")
        ema200 = enrichment.get("ema200")
        price = signal_data.get("price", 0)
        direction = signal_data.get("direction", "")

        mtf = 10.0  # base
        if ema_aligned:
            mtf += 8.0
        if ema200 and price:
            if (direction == "LONG" and price > ema200) or (direction == "SHORT" and price < ema200):
                mtf += 7.0
        breakdown["mtf_alignment"] = min(25.0, mtf)
        score += breakdown["mtf_alignment"]

        # 2. Indicator Confluence (25 pts)
        rsi = enrichment.get("rsi", 50)
        adx = enrichment.get("adx", 20)
        macd_bull = enrichment.get("macd_hist_growing_bull", False)
        macd_bear = enrichment.get("macd_hist_growing_bear", False)

        conf = 5.0  # base
        if adx and adx > 25:
            conf += 5.0
        if (direction == "LONG" and 40 < rsi < 70) or (direction == "SHORT" and 30 < rsi < 60):
            conf += 5.0
        if (direction == "LONG" and macd_bull) or (direction == "SHORT" and macd_bear):
            conf += 5.0
        st_dir = enrichment.get("supertrend_direction", 0)
        if (direction == "LONG" and st_dir == 1) or (direction == "SHORT" and st_dir == -1):
            conf += 5.0
        breakdown["indicator_confluence"] = min(25.0, conf)
        score += breakdown["indicator_confluence"]

        # 3. R:R Ratio (20 pts)
        sl = signal_data.get("sl1")
        tp = signal_data.get("tp1")
        rr = 0.0
        if sl and tp and price:
            sl_dist = abs(price - float(sl))
            tp_dist = abs(float(tp) - price)
            rr = tp_dist / sl_dist if sl_dist > 0 else 0
        rr_score = min(20.0, rr * 5.0)  # 4:1 R:R = 20 pts
        breakdown["rr_ratio"] = round(rr_score, 1)
        score += rr_score

        # 4. Regime Context (15 pts)
        regime = enrichment.get("hmm_regime", "UNKNOWN")
        alert_type = signal_data.get("alert_type", "")
        is_momentum = "confirmation" in alert_type or "plus" in alert_type
        is_contrarian = "contrarian" in alert_type

        regime_score = 7.5  # neutral
        if regime in ("TRENDING_UP", "TRENDING_DOWN") and is_momentum:
            regime_score = 15.0
        elif regime == "RANGING" and is_contrarian:
            regime_score = 15.0
        elif regime in ("TRENDING_UP", "TRENDING_DOWN") and is_contrarian:
            regime_score = 3.0  # mismatch
        breakdown["regime_context"] = round(regime_score, 1)
        score += regime_score

        # 5. Volatility Context (15 pts)
        atr_pct = enrichment.get("atr_pct", 0)
        vol_regime = enrichment.get("volatility_regime", "NORMAL")

        vol_score = 7.5  # neutral
        if vol_regime in ("NORMAL", "LOW"):
            vol_score = 12.0
        elif vol_regime == "HIGH":
            vol_score = 5.0
        elif vol_regime in ("EXTREME", "HIGH_VOLATILITY"):
            vol_score = 2.0
        breakdown["volatility_context"] = round(vol_score, 1)
        score += vol_score

        total = round(min(100.0, max(0.0, score)), 1)

        # Size modulation
        if total >= 80:
            size_mult = 1.0
            tier = "FULL"
        elif total >= 60:
            size_mult = 0.75
            tier = "HIGH"
        elif total >= 40:
            size_mult = 0.50
            tier = "MEDIUM"
        else:
            size_mult = 0.25
            tier = "LOW"

        return {
            "quality_score": total,
            "tier": tier,
            "size_multiplier": size_mult,
            "breakdown": breakdown,
            "regime": regime,
        }

    # ═══════════════════════════════════════════════════════════
    # ASYNC CLAUDE REVIEW — fire and forget
    # ═══════════════════════════════════════════════════════════

    async def _async_claude_review(
        self, signal_data: Dict, enrichment: Dict,
        quality: Dict, desk_id: str, db: Session, signal_id: int,
    ) -> None:
        """
        Fire-and-forget Claude review. Uses Sonnet, 200 token limit, 30s timeout.
        Result logged to ML table only — never blocks signal flow.
        """
        try:
            import os
            import httpx

            api_key = os.getenv("ANTHROPIC_API_KEY", "")
            if not api_key:
                return

            prompt = (
                f"Signal: {signal_data.get('symbol')} {signal_data.get('direction')} "
                f"{signal_data.get('alert_type')} | Quality: {quality['quality_score']}/100 | "
                f"RSI: {enrichment.get('rsi', '?')} | ADX: {enrichment.get('adx', '?')} | "
                f"Regime: {quality.get('regime', '?')} | Desk: {desk_id}\n"
                f"Review in 1-2 sentences. Rate GOOD/NEUTRAL/BAD."
            )

            async with httpx.AsyncClient(timeout=30.0) as client:
                resp = await client.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={
                        "x-api-key": api_key,
                        "anthropic-version": "2023-06-01",
                        "content-type": "application/json",
                    },
                    json={
                        "model": "claude-sonnet-4-20250514",
                        "max_tokens": 200,
                        "messages": [{"role": "user", "content": prompt}],
                    },
                )

                if resp.status_code == 200:
                    data = resp.json()
                    review_text = ""
                    for block in data.get("content", []):
                        if block.get("type") == "text":
                            review_text += block.get("text", "")

                    # Log to ML table (best-effort)
                    try:
                        from sqlalchemy import text as sa_text
                        db.execute(
                            sa_text("""
                                UPDATE ml_trade_logs SET
                                    claude_reasoning = :review,
                                    claude_decision = 'ASYNC_REVIEW'
                                WHERE signal_id = :sid
                            """),
                            {"review": review_text[:500], "sid": signal_id},
                        )
                        db.commit()
                    except Exception:
                        pass

                    logger.debug(
                        f"Claude async review | {signal_data.get('symbol')} | {review_text[:80]}"
                    )

        except Exception as e:
            logger.debug(f"Claude async review failed: {e}")
