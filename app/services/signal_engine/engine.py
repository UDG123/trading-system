"""
OniQuant v7.0 — Python-Native Signal Engine
Main orchestrator. Fetches OHLCV data, computes indicators, generates signals,
and pushes them to the Redis Stream (oniquant_alerts) for the existing pipeline.
"""
import asyncio
import logging
import os
from datetime import datetime, timezone
from typing import Dict, List

import orjson

from app.config import DESKS, get_desk_for_symbol
from app.services.signal_engine.candle_manager import CandleManager
from app.services.signal_engine.indicator_calculator import IndicatorCalculator
from app.services.signal_engine.smc_analyzer import SMCAnalyzer
from app.services.signal_engine.confluence_scorer import ConfluenceScorer
from app.services.signal_engine.signal_generator import SignalGenerator
from app.services.signal_engine.dedup_filter import DedupFilter
from app.services.signal_engine.rate_limiter import RateLimiter
from app.services.signal_engine.market_hours_filter import (
    is_valid_trading_hour, get_filter_stats, reset_filter_stats,
)

logger = logging.getLogger("TradingSystem.SignalEngine")

STREAM_KEY = "oniquant_alerts"

# TwelveData budget: 500 credits/day for signal engine, 55/min shared
ENGINE_DAILY_CREDIT_BUDGET = int(os.getenv("ENGINE_DAILY_CREDITS", "500"))
ENGINE_PER_MINUTE_LIMIT = 50  # Leave 5/min headroom for enrichment

# Polling intervals per timeframe (seconds)
# Staggered to spread API load
POLLING_SCHEDULE = {
    "1M":  {"interval": 65,    "symbols": "DESK1_SCALPER"},
    "5M":  {"interval": 305,   "symbols": "DESK1_SCALPER,DESK4_GOLD"},
    "15M": {"interval": 905,   "symbols": "DESK2_INTRADAY,DESK4_GOLD"},
    "1H":  {"interval": 305,   "symbols": "ALL"},
    "4H":  {"interval": 905,   "symbols": "DESK3_SWING,DESK5_ALTS,DESK6_EQUITIES"},
    "D":   {"interval": 3600,  "symbols": "ALL"},
    "W":   {"interval": 14400, "symbols": "DESK3_SWING"},
}


def _resolve_symbols(desk_spec: str) -> List[str]:
    """Resolve desk spec like 'DESK1_SCALPER,DESK4_GOLD' or 'ALL' to symbol list."""
    if desk_spec == "ALL":
        return CandleManager.get_all_symbols()

    symbols = set()
    for desk_id in desk_spec.split(","):
        desk_id = desk_id.strip()
        desk = DESKS.get(desk_id, {})
        symbols.update(desk.get("symbols", []))
    return sorted(symbols)


class SignalEngine:
    """Main orchestrator for Python-native signal generation."""

    def __init__(self, redis_pool, db_session_factory):
        self.redis = redis_pool
        self._db_factory = db_session_factory

        self.rate_limiter = RateLimiter(
            daily_limit=ENGINE_DAILY_CREDIT_BUDGET,
            per_minute_limit=ENGINE_PER_MINUTE_LIMIT,
        )
        self.candle_manager = CandleManager(db_session_factory, self.rate_limiter)
        self.indicator_calc = IndicatorCalculator()
        self.smc_analyzer = SMCAnalyzer()
        self.confluence_scorer = ConfluenceScorer()
        self.signal_generator = SignalGenerator()
        self.dedup = DedupFilter(redis_pool)

        self._running = False
        self._signal_count = 0
        self._poll_tasks: List[asyncio.Task] = []

    async def run(self) -> None:
        """Main entry point. Runs continuously until cancelled."""
        self._running = True
        logger.info("Signal Engine starting...")
        reset_filter_stats()

        try:
            # Send Telegram notification
            await self._notify_start()

            # Phase 1: Backfill candle data from DB + API
            all_symbols = CandleManager.get_all_symbols()
            all_tfs = CandleManager.get_required_timeframes()
            logger.info(
                f"Backfilling {len(all_symbols)} symbols × {len(all_tfs)} timeframes..."
            )
            await self.candle_manager.initial_backfill(all_symbols, all_tfs)

            # Phase 2: Start polling loops per timeframe
            for tf, config in POLLING_SCHEDULE.items():
                symbols = _resolve_symbols(config["symbols"])
                interval = config["interval"]
                task = asyncio.create_task(
                    self._poll_loop(tf, interval, symbols),
                    name=f"poll_{tf}",
                )
                self._poll_tasks.append(task)

            logger.info(
                f"Signal Engine ONLINE | {len(all_symbols)} symbols | "
                f"{len(POLLING_SCHEDULE)} timeframe loops | "
                f"Budget: {ENGINE_DAILY_CREDIT_BUDGET} credits/day"
            )

            # Wait for all tasks (they run forever until cancelled)
            await asyncio.gather(*self._poll_tasks, return_exceptions=True)

        except asyncio.CancelledError:
            logger.info("Signal Engine shutting down...")
        except Exception as e:
            logger.error(f"Signal Engine fatal error: {e}", exc_info=True)
        finally:
            self._running = False
            for task in self._poll_tasks:
                if not task.done():
                    task.cancel()
            await self.candle_manager.close()
            await self._notify_stop()

    async def _poll_loop(
        self, timeframe: str, interval_seconds: int, symbols: List[str]
    ) -> None:
        """Continuously poll TwelveData for a timeframe and evaluate signals."""
        # Stagger initial start to avoid API burst
        tf_order = list(POLLING_SCHEDULE.keys())
        stagger = tf_order.index(timeframe) * 5 if timeframe in tf_order else 0
        await asyncio.sleep(stagger)

        logger.info(
            f"Poll loop started | {timeframe} | {len(symbols)} symbols | "
            f"Every {interval_seconds}s"
        )

        while self._running:
            cycle_start = asyncio.get_event_loop().time()

            for symbol in symbols:
                if not self._running:
                    return

                # Wait for rate limit slot
                while not self.rate_limiter.can_request():
                    wait = self.rate_limiter.seconds_until_minute_slot()
                    if wait > 0:
                        await asyncio.sleep(min(wait + 0.5, 10))
                    else:
                        await asyncio.sleep(1)

                try:
                    # Market hours pre-filter — skip symbols with NO open desk
                    # to avoid wasting a TwelveData credit on a fetch we won't use
                    from datetime import datetime, timezone as tz
                    from app.config import ENABLE_MARKET_HOURS_FILTER
                    now_utc = datetime.now(tz.utc)
                    desks = get_desk_for_symbol(symbol)
                    if ENABLE_MARKET_HOURS_FILTER:
                        active_desks = [
                            d for d in desks
                            if is_valid_trading_hour(symbol, d, now_utc)
                        ]
                        if not active_desks:
                            await asyncio.sleep(0.1)
                            continue
                    else:
                        active_desks = desks

                    # Fetch latest candles
                    new_bars = await self.candle_manager.fetch_latest(symbol, timeframe)
                    if not new_bars:
                        await asyncio.sleep(0.3)
                        continue

                    # Compute indicators
                    df = self.candle_manager.get_dataframe(symbol, timeframe)
                    if df is None or len(df) < 50:
                        await asyncio.sleep(0.3)
                        continue

                    # Get cached regime for adaptive indicators
                    regime_label = None
                    from app.config import ENABLE_ADAPTIVE_INDICATORS, ENABLE_HMM_REGIME
                    if ENABLE_ADAPTIVE_INDICATORS and ENABLE_HMM_REGIME:
                        try:
                            from app.services.signal_engine.regime_detector import HMMRegimeDetector
                            _det = HMMRegimeDetector(redis_pool=self.redis)
                            _regime = await _det.get_regime(symbol)
                            regime_label = _regime.get("regime", "UNKNOWN") if _regime else None
                        except Exception:
                            pass

                    indicators = self.indicator_calc.compute(df, symbol, timeframe, regime=regime_label)
                    if not indicators:
                        await asyncio.sleep(0.3)
                        continue

                    # SMC analysis
                    smc = self.smc_analyzer.analyze(df, symbol)

                    # Evaluate signals only for desks within market hours
                    for desk_id in active_desks:
                        signal = self.signal_generator.evaluate(
                            symbol=symbol,
                            timeframe=timeframe,
                            desk_id=desk_id,
                            indicators=indicators,
                            smc=smc,
                            candle_manager=self.candle_manager,
                            confluence_scorer=self.confluence_scorer,
                        )
                        if signal and not await self.dedup.is_duplicate(signal):
                            await self._emit_signal(signal)

                except Exception as e:
                    logger.debug(f"Poll error for {symbol} {timeframe}: {e}")

                # Inter-request delay for rate limiting
                await asyncio.sleep(1.2)

            # Wait for remaining interval time
            elapsed = asyncio.get_event_loop().time() - cycle_start
            remaining = max(0, interval_seconds - elapsed)
            if remaining > 0:
                await asyncio.sleep(remaining)

    async def _emit_signal(self, signal: Dict) -> None:
        """Push signal to Redis Stream in the format worker expects."""
        try:
            stream_payload = orjson.dumps(signal)
            await self.redis.xadd(STREAM_KEY, {"payload": stream_payload})
            self._signal_count += 1

            logger.info(
                f"SIGNAL EMITTED #{self._signal_count} | "
                f"{signal['symbol_normalized']} {signal['direction']} "
                f"{signal['alert_type']} | "
                f"Confluence: {signal.get('confluence_score', '?'):.1f} | "
                f"Strategy: {signal.get('strategy_id', '?')} | "
                f"Desks: {signal.get('desks_matched', [])}"
            )
        except Exception as e:
            logger.error(f"Failed to emit signal: {e}")

    async def _notify_start(self) -> None:
        """Send Telegram notification on engine start."""
        try:
            from app.services.telegram_bot import TelegramBot
            tg = TelegramBot()
            all_symbols = CandleManager.get_all_symbols()
            await tg._send_to_system(
                "⚡ SIGNAL ENGINE v7.0 ONLINE\n"
                "━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📡 Source: Python Native\n"
                f"📊 Symbols: {len(all_symbols)}\n"
                f"⏱️ Timeframes: {len(POLLING_SCHEDULE)}\n"
                f"🔌 Data: TwelveData Grow\n"
                f"💰 Budget: {ENGINE_DAILY_CREDIT_BUDGET} credits/day\n"
                f"━━━━━━━━━━━━━━━━━━━━━━━"
            )
        except Exception as e:
            logger.debug(f"Telegram start notification failed: {e}")

    async def _notify_stop(self) -> None:
        """Send Telegram notification on engine stop."""
        stats = get_filter_stats()
        logger.info(
            f"Market hours filter stats: {stats['filtered']} filtered, "
            f"{stats['passed']} passed ({stats['filter_rate']:.1%} filter rate)"
        )
        try:
            from app.services.telegram_bot import TelegramBot
            tg = TelegramBot()
            await tg._send_to_system(
                "🔴 SIGNAL ENGINE OFFLINE\n"
                f"Signals emitted this session: {self._signal_count}\n"
                f"Market hours filtered: {stats['filtered']}/{stats['total']}\n"
                f"Credits remaining: {self.rate_limiter.daily_remaining}"
            )
        except Exception:
            pass
