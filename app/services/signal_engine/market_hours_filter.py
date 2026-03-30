"""
Market Hours Filter — enforces per-desk trading windows and day-of-week rules.
Runs BEFORE indicator computation to save TwelveData API credits.
"""
import logging
from datetime import datetime, timezone

from app.services.ohlcv_ingester import CRYPTO_SYMBOLS

logger = logging.getLogger("TradingSystem.SignalEngine.MarketHours")

# Per-desk UTC trading windows: (start_hour, start_min, end_hour, end_min)
# None means no restriction for that desk.
DESK_WINDOWS = {
    "DESK1_SCALPER":  (7, 0, 16, 0),    # London + NY overlap
    "DESK2_INTRADAY": (7, 0, 20, 0),    # London open through NY close
    "DESK3_SWING":    None,              # No restriction (overnight holds)
    "DESK4_GOLD":     (13, 0, 17, 0),   # London/NY overlap — gold's best window
    "DESK5_ALTS":     (7, 0, 20, 0),    # Non-crypto; crypto is unrestricted
    "DESK6_EQUITIES": (15, 0, 20, 0),   # US hours, skip first 30min (14:30-15:00)
}

# Pair-specific session restrictions for DESK1_SCALPER
# Block specific pairs during sessions where they historically lose
SCALPER_PAIR_BLOCKS = {
    "EURUSD": {"block_start": 22, "block_end": 7},   # Block Asian session
    "GBPUSD": {"block_start": 22, "block_end": 7},   # Block Asian session
    "USDCHF": {"block_start": 22, "block_end": 7},   # Block Asian session
    # USDJPY and AUDUSD are ALLOWED during Asian — they have liquidity
}

# Gold mid-week confidence boost days (Mon=0, Tue=1, Wed=2, Thu=3, Fri=4)
GOLD_BOOST_DAYS = {1, 2, 3}  # Tue, Wed, Thu
GOLD_BOOST_MULTIPLIER = 1.2

# Counters for logging (reset per engine lifetime)
_filtered_count = 0
_passed_count = 0
_pair_filtered_count = 0


def is_valid_trading_hour(
    symbol: str, desk_id: str, utc_now: datetime = None,
) -> bool:
    """
    Check if a symbol-desk pair should generate signals right now.

    Returns True if trading is allowed, False if outside market hours.
    """
    global _filtered_count, _passed_count, _pair_filtered_count

    if utc_now is None:
        utc_now = datetime.now(timezone.utc)

    weekday = utc_now.weekday()  # Mon=0, Sun=6
    hour = utc_now.hour
    minute = utc_now.minute

    # ── Day-of-week filters (apply to all desks) ──

    # Sunday before 22:00 UTC — thin liquidity at forex open
    if weekday == 6 and hour < 22:
        # Crypto is exempt
        if symbol.upper() not in CRYPTO_SYMBOLS:
            _filtered_count += 1
            return False

    # Friday after 20:00 UTC — weekend risk
    if weekday == 4 and hour >= 20:
        if symbol.upper() not in CRYPTO_SYMBOLS:
            _filtered_count += 1
            return False

    # Saturday — everything closed except crypto
    if weekday == 5:
        if symbol.upper() not in CRYPTO_SYMBOLS:
            _filtered_count += 1
            return False

    # ── Pair-specific session blocks (checked BEFORE desk window) ──

    if desk_id == "DESK1_SCALPER":
        block = SCALPER_PAIR_BLOCKS.get(symbol.upper())
        if block:
            bs = block["block_start"]
            be = block["block_end"]
            # Handle midnight wraparound (e.g. 22:00-07:00)
            if bs > be:
                blocked = hour >= bs or hour < be
            else:
                blocked = bs <= hour < be
            if blocked:
                _pair_filtered_count += 1
                logger.debug(f"PAIR BLOCK | {symbol} blocked on {desk_id} | hour={hour} UTC")
                return False

    # ── Per-desk time-of-day windows ──

    window = DESK_WINDOWS.get(desk_id)

    # DESK5_ALTS: crypto pairs bypass the window
    if desk_id == "DESK5_ALTS" and symbol.upper() in CRYPTO_SYMBOLS:
        _passed_count += 1
        return True

    # DESK3_SWING: no window restriction
    if window is None:
        _passed_count += 1
        return True

    start_h, start_m, end_h, end_m = window
    current_minutes = hour * 60 + minute
    start_minutes = start_h * 60 + start_m
    end_minutes = end_h * 60 + end_m

    if current_minutes < start_minutes or current_minutes >= end_minutes:
        _filtered_count += 1
        return False

    # DESK6_EQUITIES: skip first 30 minutes after open (14:30-15:00 UTC)
    # The window starts at 15:00 so this is already handled by the window.
    # But also block 14:30-15:00 explicitly if someone widens the window later.
    if desk_id == "DESK6_EQUITIES":
        if hour == 14 and minute >= 30:
            _filtered_count += 1
            return False

    _passed_count += 1
    return True


def get_gold_confidence_boost(utc_now: datetime = None) -> float:
    """
    Returns a confidence multiplier for gold signals.
    Gold performs best Tue-Thu, so apply 1.2x boost on those days.
    Returns 1.0 on other days (no penalty, just no boost).
    """
    if utc_now is None:
        utc_now = datetime.now(timezone.utc)

    if utc_now.weekday() in GOLD_BOOST_DAYS:
        return GOLD_BOOST_MULTIPLIER
    return 1.0


def get_filter_stats() -> dict:
    """Return cumulative filter statistics for logging."""
    total = _filtered_count + _passed_count + _pair_filtered_count
    return {
        "filtered": _filtered_count,
        "pair_filtered": _pair_filtered_count,
        "passed": _passed_count,
        "total": total,
        "filter_rate": round((_filtered_count + _pair_filtered_count) / total, 3) if total > 0 else 0.0,
    }


def reset_filter_stats() -> None:
    """Reset counters (called at engine start)."""
    global _filtered_count, _passed_count, _pair_filtered_count
    _filtered_count = 0
    _passed_count = 0
    _pair_filtered_count = 0
