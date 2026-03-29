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
    get_desk_for_symbol, get_atr_settings,
)
from app.services.signal_engine.indicator_calculator import IndicatorCalculator
from app.services.signal_engine.smc_analyzer import SMCAnalyzer
from app.services.signal_engine.confluence_scorer import ConfluenceScorer, CONFLUENCE_THRESHOLD
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

        # Get indicators for confirmation and bias timeframes
        confirm_tf = self._resolve_tf(desk_tfs.get("confirmation", ""))
        bias_tf = self._resolve_tf(desk_tfs.get("bias", ""))

        confirm_df = candle_manager.get_dataframe(symbol, confirm_tf) if confirm_tf else None
        bias_df = candle_manager.get_dataframe(symbol, bias_tf) if bias_tf else None

        calc = IndicatorCalculator()
        confirm_ind = calc.compute(confirm_df, symbol, confirm_tf) if confirm_df is not None else None
        bias_ind = calc.compute(bias_df, symbol, bias_tf) if bias_df is not None else None

        # Evaluate both directions
        for direction in ["LONG", "SHORT"]:
            if not self._direction_viable(direction, indicators, smc):
                continue

            # Cooldown check
            if self._is_on_cooldown(symbol, desk_id, direction):
                continue

            # Multi-TF confluence score
            confluence = confluence_scorer.score(
                direction=direction,
                entry_indicators=indicators,
                confirm_indicators=confirm_ind,
                bias_indicators=bias_ind,
                smc_data=smc,
            )

            # Gold mid-week confidence boost (Tue-Thu)
            if desk_id == "DESK4_GOLD":
                gold_boost = get_gold_confidence_boost()
                if gold_boost != 1.0:
                    confluence["total_score"] = round(
                        confluence["total_score"] * gold_boost, 2
                    )
                    confluence["passes_threshold"] = confluence["total_score"] >= CONFLUENCE_THRESHOLD

            if not confluence["passes_threshold"]:
                continue

            # Determine alert type
            alert_type = self._classify_signal(direction, indicators, smc, confluence)
            if alert_type not in VALID_ALERT_TYPES:
                continue

            # Check desk accepts this alert type
            desk_alerts = desk.get("alerts", [])
            if desk_alerts and alert_type not in desk_alerts:
                continue

            # Spread filter
            if not self._passes_spread_filter(indicators, desk):
                continue

            # Compute SL/TP from ATR
            price = indicators.get("price", 0)
            atr = indicators.get("atr", 0)
            sl, tp1, tp2 = self._compute_sl_tp(symbol, desk_id, timeframe, direction, price, atr)

            # R:R check (minimum 1.5)
            if sl and tp1 and price:
                sl_dist = abs(price - sl)
                tp_dist = abs(tp1 - price)
                if sl_dist > 0 and tp_dist / sl_dist < 1.5:
                    continue

            # Build payload
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

            # Record cooldown
            self._set_cooldown(symbol, desk_id, direction)

            logger.info(
                f"SIGNAL | {symbol} {direction} {alert_type} | "
                f"Desk: {desk_id} | Confluence: {confluence['total_score']:.1f}/10 | "
                f"Entry: {confluence['entry_score']:.1f} Confirm: {confluence['confirm_score']:.1f} "
                f"Bias: {confluence['bias_score']:.1f} SMC: {confluence['structure_score']:.1f}"
            )

            return signal

        return None

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
    ) -> tuple:
        """Compute SL and TP levels from ATR settings."""
        if price <= 0 or atr <= 0:
            return None, None, None

        cfg = get_atr_settings(desk_id, symbol, timeframe)
        sl_mult = cfg.get("sl_mult", 2.0)
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
