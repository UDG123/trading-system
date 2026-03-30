"""
Signal Generator
Combines indicators + SMC + confluence scoring to produce signal payloads.
Maps conditions to VALID_ALERT_TYPES and builds the Redis payload format
that the worker expects.
"""
import logging
import time
from datetime import datetime, timezone
from typing import Dict, List, Optional

from app.config import (
    DESKS, VALID_ALERT_TYPES, SESSION_WINDOWS,
    get_desk_for_symbol, get_atr_settings, MTF_SCORING,
)
from app.services.signal_engine.indicator_calculator import IndicatorCalculator
from app.services.signal_engine.smc_analyzer import SMCAnalyzer
from app.services.signal_engine.confluence_scorer import ConfluenceScorer, CONFLUENCE_THRESHOLD
from app.services.signal_engine.mtf_confluence import MTFConfluenceScorer
from app.services.signal_engine.quality_scorer import SignalQualityScorer
from app.services.signal_engine.candle_manager import CandleManager
from app.services.signal_engine.market_hours_filter import (
    is_valid_trading_hour, get_gold_confidence_boost,
)

logger = logging.getLogger("TradingSystem.SignalEngine.Generator")

# Cooldown: minimum bars between signals per symbol-desk-direction
COOLDOWN_BARS = {
    "DESK1_SCALPER": 5,
    "DESK2_INTRADAY": 3,
    "DESK3_SWING": 2,
    "DESK4_GOLD": 3,
    "DESK5_ALTS": 3,
    "DESK6_EQUITIES": 3,
}

# Minimum ADX for signal generation
MIN_ADX = 20


class SignalGenerator:
    """Evaluates conditions and generates signal payloads."""

    def __init__(self):
        self._cooldowns: Dict[str, int] = {}  # (symbol, desk, direction) → bar_count_at_last_signal

    def evaluate(
        self,
        symbol: str,
        timeframe: str,
        desk_id: str,
        indicators: Dict,
        smc: Optional[Dict],
        candle_manager: CandleManager,
        confluence_scorer: ConfluenceScorer,
    ) -> Optional[Dict]:
        """
        Evaluate if conditions warrant a signal for this symbol-desk pair.
        Returns a signal payload dict or None.
        """
        if not indicators:
            return None

        # Market hours filter (replaces old session + weekend checks)
        if not is_valid_trading_hour(symbol, desk_id):
            return None

        # ADX floor
        if indicators.get("adx", 0) < MIN_ADX:
            return None

        desk = DESKS.get(desk_id, {})
        desk_tfs = desk.get("timeframes", {})

        # Determine which TF role this timeframe plays for this desk
        tf_role = self._get_tf_role(timeframe, desk_tfs)
        if tf_role != "entry":
            # Only generate signals from entry timeframe
            return None

        # ── Scoring: WEIGHTED (new continuous) vs LEGACY (old binary) ──
        from app.config import ENABLE_MTF_SCORING
        if ENABLE_MTF_SCORING and MTF_SCORING == "WEIGHTED":
            return self._evaluate_weighted(
                symbol, timeframe, desk_id, indicators, smc,
                candle_manager, confluence_scorer,
            )
        else:
            return self._evaluate_legacy(
                symbol, timeframe, desk_id, desk, desk_tfs, indicators, smc,
                candle_manager, confluence_scorer,
            )

    def _evaluate_weighted(
        self, symbol, timeframe, desk_id, indicators, smc,
        candle_manager, confluence_scorer,
    ):
        """Weighted continuous MTF confluence scoring [-1, +1]."""
        mtf = MTFConfluenceScorer(candle_manager=candle_manager)
        confluence = mtf.score(symbol, candle_manager=candle_manager)

        if not confluence["passes_threshold"]:
            return None

        direction = confluence["direction"]

        # Gold mid-week confidence boost
        if desk_id == "DESK4_GOLD":
            gold_boost = get_gold_confidence_boost()
            if gold_boost != 1.0:
                raw = confluence["confluence_score"]
                boosted = raw * gold_boost
                confluence["confluence_score"] = round(max(-1.0, min(1.0, boosted)), 4)
                confluence["total_score"] = round(abs(boosted) * 10, 2)
                # Re-check threshold after boost
                from app.services.signal_engine.mtf_confluence import LONG_THRESHOLD, SHORT_THRESHOLD
                if boosted > LONG_THRESHOLD:
                    confluence["passes_threshold"] = True
                    confluence["direction"] = "LONG"
                    direction = "LONG"
                elif boosted < SHORT_THRESHOLD:
                    confluence["passes_threshold"] = True
                    confluence["direction"] = "SHORT"
                    direction = "SHORT"

        if not direction:
            return None

        # Cooldown
        if self._is_on_cooldown(symbol, desk_id, direction):
            return None

        # Direction viability from entry TF indicators
        if not self._direction_viable(direction, indicators, smc):
            return None

        return self._finalize_signal(
            symbol, timeframe, desk_id, direction, indicators, smc, confluence,
            candle_manager,
        )

    def _evaluate_legacy(
        self, symbol, timeframe, desk_id, desk, desk_tfs, indicators, smc,
        candle_manager, confluence_scorer,
    ):
        """Legacy binary consensus scoring (0-10 scale, threshold 6.5)."""
        confirm_tf = self._resolve_tf(desk_tfs.get("confirmation", ""))
        bias_tf = self._resolve_tf(desk_tfs.get("bias", ""))

        confirm_df = candle_manager.get_dataframe(symbol, confirm_tf) if confirm_tf else None
        bias_df = candle_manager.get_dataframe(symbol, bias_tf) if bias_tf else None

        calc = IndicatorCalculator()
        confirm_ind = calc.compute(confirm_df, symbol, confirm_tf) if confirm_df is not None else None
        bias_ind = calc.compute(bias_df, symbol, bias_tf) if bias_df is not None else None

        for direction in ["LONG", "SHORT"]:
            if not self._direction_viable(direction, indicators, smc):
                continue
            if self._is_on_cooldown(symbol, desk_id, direction):
                continue

            confluence = confluence_scorer.score(
                direction=direction,
                entry_indicators=indicators,
                confirm_indicators=confirm_ind,
                bias_indicators=bias_ind,
                smc_data=smc,
            )

            # Gold mid-week confidence boost
            if desk_id == "DESK4_GOLD":
                gold_boost = get_gold_confidence_boost()
                if gold_boost != 1.0:
                    confluence["total_score"] = round(
                        confluence["total_score"] * gold_boost, 2
                    )
                    confluence["passes_threshold"] = confluence["total_score"] >= CONFLUENCE_THRESHOLD

            if not confluence["passes_threshold"]:
                continue

            result = self._finalize_signal(
                symbol, timeframe, desk_id, direction, indicators, smc, confluence,
                candle_manager,
            )
            if result:
                return result

        return None

    def _finalize_signal(
        self, symbol, timeframe, desk_id, direction, indicators, smc, confluence,
        candle_manager,
    ):
        """Common logic for both scoring modes: alert type, SL/TP, R:R, payload."""
        desk = DESKS.get(desk_id, {})

        # ── Regime-aware filtering ──
        from app.config import ENABLE_HMM_REGIME
        regime_info = None
        if ENABLE_HMM_REGIME:
            regime_info = self._get_regime(symbol, candle_manager)
            if regime_info and regime_info.get("regime") != "UNKNOWN":
                regime = regime_info["regime"]
                favored = regime_info.get("favor_direction")

                # In trending regimes, skip signals against the trend
                if favored and direction != favored and regime_info.get("confidence", 0) > 0.6:
                    return None

                # Store regime in confluence for downstream logging
                confluence["regime"] = regime
                confluence["regime_size_mult"] = regime_info.get("size_multiplier", 1.0)

        # ── Ichimoku Cloud trend filter — prevents counter-trend trades ──
        from app.config import ENABLE_ICHIMOKU_FILTER
        if ENABLE_ICHIMOKU_FILTER:
            price_above = indicators.get("price_above_cloud")
            price_below = indicators.get("price_below_cloud")
            if price_above is not None:
                if direction == "SHORT" and price_above:
                    logger.info(
                        f"ICHIMOKU BLOCK | {symbol} SHORT blocked — price above cloud | "
                        f"Desk: {desk_id}"
                    )
                    return None
                if direction == "LONG" and price_below:
                    logger.info(
                        f"ICHIMOKU BLOCK | {symbol} LONG blocked — price below cloud | "
                        f"Desk: {desk_id}"
                    )
                    return None

        # ── Economic calendar blackout — block near high-impact events ──
        from app.config import ENABLE_ECON_CALENDAR
        if ENABLE_ECON_CALENDAR:
            try:
                from app.services.econ_calendar import is_near_high_impact_event
                blocked, event_name = is_near_high_impact_event(symbol, minutes_before=30, minutes_after=15)
                if blocked:
                    logger.info(f"ECON BLOCK | {symbol} blocked — {event_name} | Desk: {desk_id}")
                    return None
            except Exception:
                pass

        alert_type = self._classify_signal(direction, indicators, smc, confluence)
        if alert_type not in VALID_ALERT_TYPES:
            return None

        desk_alerts = desk.get("alerts", [])
        if desk_alerts and alert_type not in desk_alerts:
            return None

        if not self._passes_spread_filter(indicators, desk):
            return None

        price = indicators.get("price", 0)
        atr = indicators.get("atr", 0)

        # Use regime-specific SL ATR multiplier if in RANGING
        sl_atr_override = None
        if regime_info and regime_info.get("sl_atr_mult"):
            sl_atr_override = regime_info["sl_atr_mult"]

        sl, tp1, tp2 = self._compute_sl_tp(
            symbol, desk_id, timeframe, direction, price, atr,
            sl_mult_override=sl_atr_override,
        )

        if sl and tp1 and price:
            sl_dist = abs(price - sl)
            tp_dist = abs(tp1 - price)
            if sl_dist > 0 and tp_dist / sl_dist < 1.5:
                return None

        # ── Quality Score Gate ──
        from app.config import ENABLE_QUALITY_SCORER
        quality = None
        if ENABLE_QUALITY_SCORER:
            quality_scorer = SignalQualityScorer()

            # Try to get CatBoost probability for quality scoring
            catboost_proba = None
            try:
                from app.services.meta_labeler import MetaLabeler
                _meta = MetaLabeler()
                if _meta._model is not None:
                    from datetime import datetime as _dt, timezone as _tz
                    _now = _dt.now(_tz.utc)
                    meta_features = {
                        "consensus_score": confluence.get("total_score", 0),
                        "ml_score": 0.5,
                        "hurst": indicators.get("hurst", 0.5) if indicators else 0.5,
                        "rsi": indicators.get("rsi", 50),
                        "adx": indicators.get("adx", 20),
                        "atr_pct": indicators.get("atr_pct", 0),
                        "rvol": indicators.get("rvol", 1),
                        "hour_utc": _now.hour,
                        "day_of_week": _now.weekday(),
                        "vix_level": 20,
                    }
                    meta_result = _meta.predict(meta_features)
                    catboost_proba = meta_result.get("meta_probability")
            except Exception:
                pass

            # Add candle body ratio to indicators for quality scoring
            if candle_manager:
                df = candle_manager.get_dataframe(symbol, timeframe)
                if df is not None and len(df) >= 1:
                    last = df.iloc[-1]
                    bar_range = float(last["high"]) - float(last["low"])
                    if bar_range > 0:
                        indicators["candle_body_ratio"] = abs(
                            float(last["close"]) - float(last["open"])
                        ) / bar_range

            quality = quality_scorer.score(
                confluence=confluence,
                indicators=indicators,
                smc=smc,
                alert_type=alert_type,
                catboost_proba=catboost_proba,
            )

            if not quality["emit"]:
                logger.info(
                    f"QUALITY SKIP | {symbol} {direction} {alert_type} | "
                    f"Desk: {desk_id} | Score: {quality['quality_score']}/100 "
                    f"(threshold: {quality['threshold']}) | "
                    f"Breakdown: {quality['breakdown']}"
                )
                return None

        signal = self._build_payload(
            symbol=symbol,
            timeframe=timeframe,
            desk_id=desk_id,
            direction=direction,
            alert_type=alert_type,
            price=price,
            sl=sl, tp1=tp1, tp2=tp2,
            confluence=confluence,
            indicators=indicators,
            smc=smc,
        )

        # Attach quality score and size multiplier to payload
        if quality:
            signal["quality_score"] = quality["quality_score"]
            signal["quality_tier"] = quality["tier"]
            signal["quality_size_mult"] = quality["size_multiplier"]
            signal["quality_breakdown"] = quality["breakdown"]

        self._set_cooldown(symbol, desk_id, direction)

        scoring_mode = "WEIGHTED" if MTF_SCORING == "WEIGHTED" else "LEGACY"
        logger.info(
            f"SIGNAL | {symbol} {direction} {alert_type} | "
            f"Desk: {desk_id} | Quality: {quality['quality_score']}/100 ({quality['tier']}) | "
            f"Size: {quality['size_multiplier']}x | Mode: {scoring_mode} | "
            f"Breakdown: MTF={quality['breakdown'].get('mtf_confluence', 0):.0f} "
            f"Regime={quality['breakdown'].get('regime_suitability', 0):.0f} "
            f"S/R={quality['breakdown'].get('sr_proximity', 0):.0f} "
            f"Candle={quality['breakdown'].get('candle_quality', 0):.0f} "
            f"ML={quality['breakdown'].get('catboost', 0):.0f} "
            f"Vol={quality['breakdown'].get('volume', 0):.0f}"
        )

        return signal

    # ── Signal Classification ──

    def _classify_signal(
        self, direction: str, indicators: Dict,
        smc: Optional[Dict], confluence: Dict,
    ) -> str:
        """Map conditions to VALID_ALERT_TYPES."""
        is_long = direction == "LONG"
        score = confluence["total_score"]
        has_bos = smc and smc.get("bos_bull" if is_long else "bos_bear", False)
        has_choch = smc and smc.get("choch_bull" if is_long else "choch_bear", False)
        has_fvg = smc and smc.get("fvg_at_price") == ("bull" if is_long else "bear")
        has_ob = smc and smc.get("ob_at_price") == ("bull" if is_long else "bear")
        st_flip = indicators.get("supertrend_flip", False)

        # CHoCH + reversal = contrarian
        if has_choch:
            return f"contrarian_{'bullish' if is_long else 'bearish'}"

        # BOS + FVG/OB + high confluence = confirmation_plus
        if has_bos and (has_fvg or has_ob) and score >= 8.0:
            return f"{'bullish' if is_long else 'bearish'}_confirmation_plus"

        # SuperTrend flip + trend = plus
        if st_flip:
            return f"{'bullish' if is_long else 'bearish'}_plus"

        # BOS + EMA alignment = confirmation
        if has_bos:
            return f"{'bullish' if is_long else 'bearish'}_confirmation"

        # Default: standard confirmation
        return f"{'bullish' if is_long else 'bearish'}_confirmation"

    # ── Direction Viability ──

    def _direction_viable(self, direction: str, indicators: Dict, smc: Optional[Dict]) -> bool:
        """Quick check if direction has any basis from indicators/structure."""
        is_long = direction == "LONG"

        # Need at least EMA alignment OR BOS OR SuperTrend in direction
        ema_ok = indicators.get("ema_aligned_bull" if is_long else "ema_aligned_bear", False)
        st_ok = (indicators.get("supertrend_direction", 0) == 1) if is_long else (
            indicators.get("supertrend_direction", 0) == -1
        )
        bos_ok = smc and smc.get("bos_bull" if is_long else "bos_bear", False)
        choch_ok = smc and smc.get("choch_bull" if is_long else "choch_bear", False)

        return ema_ok or st_ok or bos_ok or choch_ok

    # ── SL/TP Computation ──

    def _compute_sl_tp(
        self, symbol: str, desk_id: str, timeframe: str,
        direction: str, price: float, atr: float,
        sl_mult_override: float = None,
    ) -> tuple:
        """Compute SL and TP levels from ATR settings."""
        if price <= 0 or atr <= 0:
            return None, None, None

        cfg = get_atr_settings(desk_id, symbol, timeframe)
        sl_mult = sl_mult_override or cfg.get("sl_mult", 2.0)
        tp1_mult = cfg.get("tp1_mult", 4.0)
        tp2_mult = cfg.get("tp2_mult", 6.0)

        if direction == "LONG":
            sl = round(price - atr * sl_mult, 5)
            tp1 = round(price + atr * tp1_mult, 5)
            tp2 = round(price + atr * tp2_mult, 5)
        else:
            sl = round(price + atr * sl_mult, 5)
            tp1 = round(price - atr * tp1_mult, 5)
            tp2 = round(price - atr * tp2_mult, 5)

        return sl, tp1, tp2

    # ── Payload Builder ──

    def _build_payload(
        self, symbol: str, timeframe: str, desk_id: str,
        direction: str, alert_type: str, price: float,
        sl: float, tp1: float, tp2: float,
        confluence: Dict, indicators: Dict, smc: Optional[Dict],
    ) -> Dict:
        """Build the Redis Stream payload matching worker expectations."""
        desks_matched = get_desk_for_symbol(symbol)
        # Ensure the triggering desk is included
        if desk_id not in desks_matched:
            desks_matched.append(desk_id)

        # Determine strategy ID from signal type
        strategy_id = "trend_continuation"
        if "contrarian" in alert_type:
            strategy_id = "smc_choch_reversal"
        elif "plus" in alert_type and smc and (smc.get("fvg_at_price") or smc.get("ob_at_price")):
            strategy_id = "smc_bos_trend"
        elif indicators.get("supertrend_flip"):
            strategy_id = "supertrend_flip"

        return {
            "symbol": symbol,
            "symbol_normalized": symbol,
            "exchange": "",
            "timeframe": timeframe,
            "alert_type": alert_type,
            "direction": direction,
            "price": price,
            "tp1": tp1,
            "tp2": tp2,
            "sl1": sl,
            "sl2": None,
            "smart_trail": indicators.get("supertrend_value"),
            "volume": None,
            "desks_matched": desks_matched,
            "webhook_latency_ms": 0,
            "time": str(int(time.time() * 1000)),
            "source": "python_engine",
            "confluence_score": confluence["total_score"],
            "strategy_id": strategy_id,
        }

    # ── Session / Weekend Filters ──

    @staticmethod
    def _is_in_session(desk_id: str) -> bool:
        """Check if desk is in active trading session."""
        desk = DESKS.get(desk_id, {})
        sessions = desk.get("sessions", ["ALL"])
        if "ALL" in sessions:
            return True

        now_utc = datetime.now(timezone.utc)
        hour = now_utc.hour

        for session_name in sessions:
            window = SESSION_WINDOWS.get(session_name)
            if not window:
                continue
            start, end = window["start"], window["end"]
            if start > end:  # wraps midnight
                if hour >= start or hour < end:
                    return True
            else:
                if start <= hour < end:
                    return True
        return False

    @staticmethod
    def _is_weekend_blocked(symbol: str) -> bool:
        """No signals Fri 21:00 UTC - Sun 22:00 UTC (except 24/7 crypto)."""
        from app.services.ohlcv_ingester import CRYPTO_SYMBOLS
        if symbol in CRYPTO_SYMBOLS:
            return False

        now = datetime.now(timezone.utc)
        weekday = now.weekday()  # Mon=0, Sun=6
        hour = now.hour

        if weekday == 4 and hour >= 21:  # Friday after 21:00
            return True
        if weekday == 5:  # Saturday
            return True
        if weekday == 6 and hour < 22:  # Sunday before 22:00
            return True
        return False

    # ── Cooldown Management ──

    def _is_on_cooldown(self, symbol: str, desk_id: str, direction: str) -> bool:
        key = f"{symbol}:{desk_id}:{direction}"
        last = self._cooldowns.get(key, 0)
        now = int(time.time())
        cooldown_bars = COOLDOWN_BARS.get(desk_id, 3)
        # Approximate bar duration for cooldown (entry TF)
        desk = DESKS.get(desk_id, {})
        entry_tf = desk.get("timeframes", {}).get("entry", "1H")
        bar_seconds = self._tf_to_seconds(entry_tf.split(",")[0].strip())
        return (now - last) < (cooldown_bars * bar_seconds)

    def _set_cooldown(self, symbol: str, desk_id: str, direction: str) -> None:
        key = f"{symbol}:{desk_id}:{direction}"
        self._cooldowns[key] = int(time.time())

    @staticmethod
    def _tf_to_seconds(tf: str) -> int:
        tf = tf.upper()
        mapping = {"1M": 60, "5M": 300, "15M": 900, "1H": 3600, "4H": 14400, "D": 86400, "W": 604800}
        return mapping.get(tf, 3600)

    # ── Timeframe Resolution ──

    @staticmethod
    def _get_tf_role(timeframe: str, desk_tfs: Dict) -> Optional[str]:
        """Determine what role a timeframe plays for a desk."""
        tf = timeframe.upper()
        for role, tf_str in desk_tfs.items():
            for t in tf_str.split(","):
                if t.strip().upper() == tf:
                    return role
        return None

    @staticmethod
    def _resolve_tf(tf_str: str) -> Optional[str]:
        """Resolve a timeframe string to the first valid TF."""
        if not tf_str:
            return None
        first = tf_str.split(",")[0].strip().upper()
        return first if first else None

    # ── Spread Filter ──

    @staticmethod
    def _passes_spread_filter(indicators: Dict, desk: Dict) -> bool:
        """Skip if spread > 30% of ATR (when spread data available)."""
        spread_limit = desk.get("spread_filter_pips")
        if not spread_limit:
            return True
        # Spread data would come from enrichment; not available at signal generation
        return True

    @staticmethod
    def _get_regime(symbol: str, candle_manager) -> Optional[Dict]:
        """Get HMM regime for a symbol. Returns None if unavailable."""
        try:
            from app.services.signal_engine.regime_detector import HMMRegimeDetector
            detector = HMMRegimeDetector()
            # Use candle_manager's DB factory for sync access
            if hasattr(candle_manager, '_db_factory') and candle_manager._db_factory:
                db = candle_manager._db_factory()
                try:
                    result = detector.get_regime_sync(symbol, db)
                    if result.get("regime") != "UNKNOWN":
                        return result
                finally:
                    db.close()
        except Exception:
            pass
        return None
