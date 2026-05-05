"""
OniQuant v7.0 — Python-Native Signal Engine
Main orchestrator. Fetches OHLCV data, computes indicators, generates signals,
and pushes them to the Redis Stream (oniquant_alerts) for the existing pipeline.
"""
import asyncio
import logging
import os
from datetime import datetime, date, timezone
from typing import Any, Dict, List

import orjson

from app.config import DESKS, get_desk_for_symbol
from app.services.signal_engine.candle_manager import CandleManager
from app.services.signal_engine.indicator_calculator import IndicatorCalculator
from app.services.signal_engine.smc_analyzer import SMCAnalyzer
from app.services.signal_engine.confluence_scorer import ConfluenceScorer
from app.services.signal_engine.signal_generator import SignalGenerator
from app.services.signal_engine.dedup_filter import DedupFilter
from app.services.signal_engine.signal_quality import SignalQualityEngine
from app.services.signal_engine.rate_limiter import RateLimiter
from app.services.signal_engine.market_hours_filter import (
    is_valid_trading_hour, get_filter_stats, reset_filter_stats,
)

logger = logging.getLogger("TradingSystem.SignalEngine")

STREAM_KEY = "oniquant_alerts"

ENGINE_DAILY_CREDIT_BUDGET = int(os.getenv("ENGINE_DAILY_CREDITS", "500"))
ENGINE_PER_MINUTE_LIMIT = int(os.getenv("ENGINE_PER_MINUTE_LIMIT", "7"))
TWELVEDATA_BACKFILL_DELAY_SECONDS = float(os.getenv("TWELVEDATA_BACKFILL_DELAY_SECONDS", "8.75"))
_raw_td_only = os.getenv("TD_ONLY_SYMBOLS", "WTIUSD")
TD_ONLY_SYMBOLS = {s.strip().upper() for s in _raw_td_only.split(",") if s.strip()}

POLLING_SCHEDULE = {
    "1M":  {"interval": 65,    "symbols": "TD_ONLY"},
    "5M":  {"interval": 305,   "symbols": "TD_ONLY"},
    "15M": {"interval": 905,   "symbols": "TD_ONLY"},
    "1H":  {"interval": 305,   "symbols": "TD_ONLY"},
    "4H":  {"interval": 905,   "symbols": "TD_ONLY"},
    "D":   {"interval": 3600,  "symbols": "TD_ONLY"},
    "W":   {"interval": 14400, "symbols": "TD_ONLY"},
}


def _resolve_symbols(desk_spec: str) -> List[str]:
    if desk_spec == "TD_ONLY":
        return sorted(TD_ONLY_SYMBOLS)
    if desk_spec == "ALL":
        return CandleManager.get_all_symbols()
    symbols = set()
    for desk_id in desk_spec.split(","):
        desk = DESKS.get(desk_id.strip(), {})
        symbols.update(desk.get("symbols", []))
    return sorted(symbols)


def _json_safe(value: Any) -> Any:
    """Convert numpy/pandas/scalar objects into Redis/orjson-safe primitives."""
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {str(k): _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    try:
        import numpy as np
        if isinstance(value, np.generic):
            return value.item()
        if isinstance(value, np.ndarray):
            return value.tolist()
    except Exception:
        pass
    try:
        import pandas as pd
        if isinstance(value, pd.Timestamp):
            return value.isoformat()
        if value is pd.NA:
            return None
    except Exception:
        pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return str(value)


class SignalEngine:
    def __init__(self, redis_pool, db_session_factory):
        self.redis = redis_pool
        self._db_factory = db_session_factory
        self.rate_limiter = RateLimiter(daily_limit=ENGINE_DAILY_CREDIT_BUDGET, per_minute_limit=ENGINE_PER_MINUTE_LIMIT)
        self.candle_manager = CandleManager(db_session_factory, self.rate_limiter)
        self.indicator_calc = IndicatorCalculator()
        self.smc_analyzer = SMCAnalyzer()
        self.confluence_scorer = ConfluenceScorer()
        self.signal_generator = SignalGenerator()
        self.dedup = DedupFilter(redis_pool)
        self.signal_quality_engine = SignalQualityEngine()
        self._running = False
        self._signal_count = 0
        self._poll_tasks: List[asyncio.Task] = []

    async def run(self) -> None:
        self._running = True
        logger.info("Signal Engine starting...")
        logger.info("TwelveData config | per_minute_limit=%s | backfill_delay=%ss | td_only=%s", ENGINE_PER_MINUTE_LIMIT, TWELVEDATA_BACKFILL_DELAY_SECONDS, sorted(TD_ONLY_SYMBOLS))
        reset_filter_stats()
        try:
            await self._notify_start()
            all_symbols = CandleManager.get_all_symbols()
            all_tfs = CandleManager.get_required_timeframes()
            logger.info(f"Backfilling {len(all_symbols)} symbols × {len(all_tfs)} timeframes...")
            await self.candle_manager.initial_backfill(all_symbols, all_tfs)

            try:
                from app.services.data_providers.kraken_ws import KrakenOHLCStream
                self._poll_tasks.append(asyncio.create_task(KrakenOHLCStream(self._db_factory).run(), name="kraken_ws"))
                logger.info("Kraken WS stream started (5 crypto pairs)")
            except Exception as e:
                logger.debug(f"Kraken WS start failed: {e}")

            if os.getenv("ENABLE_OANDA_STREAM", "false").lower() in {"1", "true", "yes", "on"}:
                try:
                    from app.services.data_providers.oanda_stream import OANDAStream
                    self._poll_tasks.append(asyncio.create_task(OANDAStream(self._db_factory).run(), name="oanda_stream"))
                    logger.info("OANDA stream started (16 FX+metals)")
                except Exception as e:
                    logger.debug(f"OANDA stream start failed: {e}")
            else:
                logger.info("OANDA stream skipped (ENABLE_OANDA_STREAM=false)")

            try:
                from app.services.data_providers.alpaca_ws import AlpacaBarStream
                self._poll_tasks.append(asyncio.create_task(AlpacaBarStream(self._db_factory).run(), name="alpaca_ws"))
                logger.info("Alpaca WS stream started (9 equities)")
            except Exception as e:
                logger.debug(f"Alpaca WS start failed: {e}")

            if os.getenv("ENABLE_MCP_MARKET_DATA", "false").lower() in {"1", "true", "yes", "on"}:
                try:
                    from app.services.data_providers.mcp_market_data import MCPMarketDataProvider
                    self._poll_tasks.append(asyncio.create_task(MCPMarketDataProvider(self.candle_manager).run(), name="mcp_market_data"))
                    logger.info("MCP market data provider started")
                except Exception as e:
                    logger.warning("MCP market data provider failed to start: %s", e)

            async def _resample_loop():
                while self._running:
                    try:
                        from app.services.data_providers.ohlcv_resampler import resample_all_symbols
                        db = self._db_factory()
                        try:
                            resample_all_symbols(db, all_symbols, hours_back=1)
                        finally:
                            db.close()
                    except Exception as e:
                        logger.debug(f"Resample error: {e}")
                    await asyncio.sleep(300)
            self._poll_tasks.append(asyncio.create_task(_resample_loop(), name="ohlcv_resampler"))

            for tf, config in POLLING_SCHEDULE.items():
                symbols = _resolve_symbols(config["symbols"])
                if not symbols:
                    logger.info("Poll loop skipped | %s | no TD_ONLY symbols", tf)
                    continue
                self._poll_tasks.append(asyncio.create_task(self._poll_loop(tf, config["interval"], symbols), name=f"poll_{tf}"))

            from app.services.signal_engine.desk_scanner import DeskScanner, SCAN_INTERVALS
            scanner = DeskScanner(self.candle_manager)
            for desk_id, interval_sec in SCAN_INTERVALS.items():
                self._poll_tasks.append(asyncio.create_task(self._desk_scan_loop(scanner, desk_id, interval_sec), name=f"scan_{desk_id}"))

            logger.info("Desk scanners started: " + ", ".join(f"{d}@{s}s" for d, s in SCAN_INTERVALS.items()))
            logger.info(f"Signal Engine ONLINE | {len(all_symbols)} symbols | Free streams: {len(all_symbols) - len(TD_ONLY_SYMBOLS)} | TwelveData: {len(TD_ONLY_SYMBOLS)} | Budget: {ENGINE_DAILY_CREDIT_BUDGET} credits/day")
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

    async def _poll_loop(self, timeframe: str, interval_seconds: int, symbols: List[str]) -> None:
        tf_order = list(POLLING_SCHEDULE.keys())
        stagger = tf_order.index(timeframe) * 5 if timeframe in tf_order else 0
        await asyncio.sleep(stagger)
        logger.info(f"Poll loop started | {timeframe} | {len(symbols)} symbols | Every {interval_seconds}s")
        while self._running:
            cycle_start = asyncio.get_event_loop().time()
            for symbol in symbols:
                if not self._running:
                    return
                while not self.rate_limiter.can_request():
                    wait = self.rate_limiter.seconds_until_minute_slot()
                    await asyncio.sleep(max(wait + 0.25, 1.0))
                try:
                    from app.config import ENABLE_MARKET_HOURS_FILTER
                    now_utc = datetime.now(timezone.utc)
                    desks = get_desk_for_symbol(symbol)
                    if ENABLE_MARKET_HOURS_FILTER:
                        active_desks = [d for d in desks if is_valid_trading_hour(symbol, d, now_utc)]
                        if not active_desks:
                            await asyncio.sleep(0.1)
                            continue
                    else:
                        active_desks = desks
                    new_bars = await self.candle_manager.fetch_latest(symbol, timeframe)
                    if not new_bars:
                        await asyncio.sleep(0.3)
                        continue
                    df = self.candle_manager.get_dataframe(symbol, timeframe)
                    if df is None or len(df) < 50:
                        await asyncio.sleep(0.3)
                        continue
                    regime_label = None
                    from app.config import ENABLE_ADAPTIVE_INDICATORS, ENABLE_HMM_REGIME
                    if ENABLE_ADAPTIVE_INDICATORS and ENABLE_HMM_REGIME:
                        try:
                            from app.services.signal_engine.regime_detector import HMMRegimeDetector
                            _regime = await HMMRegimeDetector(redis_pool=self.redis).get_regime(symbol)
                            regime_label = _regime.get("regime", "UNKNOWN") if _regime else None
                        except Exception:
                            pass
                    indicators = self.indicator_calc.compute(df, symbol, timeframe, regime=regime_label)
                    if not indicators:
                        await asyncio.sleep(0.3)
                        continue
                    smc = self.smc_analyzer.analyze(df, symbol)
                    for desk_id in active_desks:
                        signal = self.signal_generator.evaluate(symbol=symbol, timeframe=timeframe, desk_id=desk_id, indicators=indicators, smc=smc, candle_manager=self.candle_manager, confluence_scorer=self.confluence_scorer)
                        if signal and not await self.dedup.is_duplicate(signal):
                            await self._emit_signal(signal)
                except Exception as e:
                    logger.debug(f"Poll error for {symbol} {timeframe}: {e}")
                await asyncio.sleep(TWELVEDATA_BACKFILL_DELAY_SECONDS)
            remaining = max(0, interval_seconds - (asyncio.get_event_loop().time() - cycle_start))
            if remaining > 0:
                await asyncio.sleep(remaining)

    async def _desk_scan_loop(self, scanner, desk_id: str, interval_seconds: int) -> None:
        from app.services.signal_engine.desk_scanner import DeskScanner
        desk_order = list(DESKS.keys())
        await asyncio.sleep((desk_order.index(desk_id) * 3 if desk_id in desk_order else 0) + 10)
        logger.info(f"Desk scan loop started | {desk_id} | every {interval_seconds}s")
        while self._running:
            try:
                candidates = scanner.scan_desk(desk_id)
                logger.info("Desk %s produced %s candidates", desk_id, len(candidates))
                for candidate in candidates:
                    payload = DeskScanner.build_signal_payload(candidate)
                    if not await self.dedup.is_duplicate(payload):
                        await self._emit_signal(payload)
            except Exception as e:
                logger.debug(f"Desk scan error for {desk_id}: {e}")
            await asyncio.sleep(interval_seconds)

    async def _emit_signal(self, signal: Dict) -> None:
        try:
            signal = self.signal_quality_engine.enhance(signal)
            if signal.get("quality_blocked"):
                logger.info(
                    "SIGNAL QUALITY BLOCKED | %s %s | probability=%s | final_quality=%s | reasons=%s",
                    signal.get("symbol_normalized", signal.get("symbol", "?")),
                    signal.get("direction", "?"),
                    signal.get("signal_probability"),
                    signal.get("final_signal_quality"),
                    signal.get("quality_block_reasons"),
                )
                return
            safe_signal = _json_safe(signal)
            stream_payload = orjson.dumps(safe_signal)
            message_id = await self.redis.xadd(STREAM_KEY, {"payload": stream_payload})
            self._signal_count += 1
            logger.info(
                "SIGNAL EMITTED #%s | %s %s %s | Confluence: %s | Strategy: %s | Desks: %s | Redis=%s:%s | payload_bytes=%s",
                self._signal_count,
                safe_signal.get("symbol_normalized", safe_signal.get("symbol", "?")),
                safe_signal.get("direction", "?"),
                safe_signal.get("alert_type", "?"),
                safe_signal.get("confluence_score", "?"),
                safe_signal.get("strategy_id", "?"),
                safe_signal.get("desks_matched", []),
                STREAM_KEY,
                message_id,
                len(stream_payload),
            )
        except Exception as e:
            logger.error(f"Failed to emit signal: {e}")

    async def _notify_start(self) -> None:
        try:
            from app.services.telegram_bot import TelegramBot
            tg = TelegramBot()
            all_symbols = CandleManager.get_all_symbols()
            await tg._send_to_system(
                "⚡ SIGNAL ENGINE v7.0 ONLINE\n━━━━━━━━━━━━━━━━━━━━━━━\n\n"
                f"📡 Source: Python Native\n📊 Symbols: {len(all_symbols)}\n"
                f"⏱️ Timeframes: {len(POLLING_SCHEDULE)}\n🔌 Data: TwelveData/Free Streams\n"
                f"💰 Budget: {ENGINE_DAILY_CREDIT_BUDGET} credits/day\n━━━━━━━━━━━━━━━━━━━━━━━"
            )
        except Exception as e:
            logger.debug(f"Telegram start notification failed: {e}")

    async def _notify_stop(self) -> None:
        stats = get_filter_stats()
        logger.info(f"Market hours filter stats: {stats['filtered']} filtered, {stats['passed']} passed ({stats['filter_rate']:.1%} filter rate)")
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
