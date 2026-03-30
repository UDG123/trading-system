"""
Economic Calendar — blocks trading around high-impact events.

MVP uses hardcoded recurring schedules for known events (NFP, FOMC, CPI, etc.).
Each event has a blackout window: [event_time - before, event_time + after].
"""
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Tuple
from calendar import monthrange

logger = logging.getLogger("TradingSystem.EconCalendar")

# All FX + gold pairs affected by USD events
USD_PAIRS = [
    "EURUSD", "GBPUSD", "USDJPY", "USDCHF", "USDCAD", "AUDUSD", "NZDUSD",
    "XAUUSD", "XAGUSD",
]
ALL_PAIRS = "ALL"

# ═══════════════════════════════════════════════════════════════
# HIGH-IMPACT RECURRING EVENTS (hardcoded schedule)
# ═══════════════════════════════════════════════════════════════

HIGH_IMPACT_EVENTS = {
    # ── US Events ──
    "NFP": {
        "description": "US Non-Farm Payrolls",
        "recurrence": "first_friday",
        "hour": 13, "minute": 30,
        "duration_minutes": 30,
        "pairs": USD_PAIRS,
    },
    "CPI_US": {
        "description": "US Consumer Price Index",
        "recurrence": "monthly",  # ~10th-14th, varies
        "typical_day_range": (10, 14),
        "hour": 13, "minute": 30,
        "duration_minutes": 30,
        "pairs": USD_PAIRS,
    },
    "FOMC_RATE": {
        "description": "FOMC Interest Rate Decision",
        "recurrence": "fomc",  # 8 times/year, ~6 weeks apart
        "hour": 19, "minute": 0,
        "duration_minutes": 60,
        "pairs": ALL_PAIRS,
    },
    "PPI_US": {
        "description": "US Producer Price Index",
        "recurrence": "monthly",
        "typical_day_range": (11, 15),
        "hour": 13, "minute": 30,
        "duration_minutes": 20,
        "pairs": USD_PAIRS,
    },
    "RETAIL_SALES": {
        "description": "US Retail Sales",
        "recurrence": "monthly",
        "typical_day_range": (14, 17),
        "hour": 13, "minute": 30,
        "duration_minutes": 20,
        "pairs": USD_PAIRS,
    },
    # ── ECB ──
    "ECB_RATE": {
        "description": "ECB Interest Rate Decision",
        "recurrence": "ecb",  # ~6 weeks apart
        "hour": 13, "minute": 15,
        "duration_minutes": 60,
        "pairs": ["EURUSD", "EURGBP", "EURJPY", "EURAUD", "XAUUSD"],
    },
    # ── BOE ──
    "BOE_RATE": {
        "description": "BOE Interest Rate Decision",
        "recurrence": "boe",
        "hour": 12, "minute": 0,
        "duration_minutes": 60,
        "pairs": ["GBPUSD", "EURGBP", "GBPJPY", "GBPCAD", "GBPAUD"],
    },
    # ── Weekly ──
    "JOBLESS_CLAIMS": {
        "description": "US Initial Jobless Claims",
        "recurrence": "weekly_thursday",
        "hour": 13, "minute": 30,
        "duration_minutes": 15,
        "pairs": USD_PAIRS,
    },
}


def is_near_high_impact_event(
    symbol: str,
    minutes_before: int = 30,
    minutes_after: int = 15,
    utc_now: datetime = None,
) -> Tuple[bool, str]:
    """
    Check if current time is within a blackout window around a high-impact event.

    Returns (True, "NFP in 15 min") or (False, "").
    """
    if utc_now is None:
        utc_now = datetime.now(timezone.utc)

    upcoming = _get_todays_events(utc_now)
    sym = symbol.upper()

    for event in upcoming:
        # Check if symbol is affected
        affected = event["pairs"]
        if affected != ALL_PAIRS and sym not in affected:
            continue

        event_time = event["event_time"]
        window_start = event_time - timedelta(minutes=minutes_before)
        window_end = event_time + timedelta(minutes=minutes_after + event.get("duration_minutes", 0))

        if window_start <= utc_now <= window_end:
            # How far from event?
            delta = event_time - utc_now
            if delta.total_seconds() > 0:
                desc = f"{event['name']} in {int(delta.total_seconds() / 60)}min"
            else:
                mins_since = int(-delta.total_seconds() / 60)
                desc = f"{event['name']} {mins_since}min ago (blackout)"
            return True, desc

    return False, ""


def get_upcoming_events(
    hours_ahead: int = 24,
    utc_now: datetime = None,
) -> List[Dict]:
    """Return events in the next N hours."""
    if utc_now is None:
        utc_now = datetime.now(timezone.utc)

    cutoff = utc_now + timedelta(hours=hours_ahead)
    results = []

    # Check today and tomorrow
    for day_offset in range(2):
        check_date = utc_now + timedelta(days=day_offset)
        events = _get_todays_events(check_date)
        for ev in events:
            if utc_now <= ev["event_time"] <= cutoff:
                results.append({
                    "name": ev["name"],
                    "description": ev.get("description", ""),
                    "time_utc": ev["event_time"].isoformat(),
                    "pairs": ev["pairs"] if ev["pairs"] != ALL_PAIRS else "ALL",
                    "minutes_until": int((ev["event_time"] - utc_now).total_seconds() / 60),
                })

    return sorted(results, key=lambda x: x["minutes_until"])


# ═══════════════════════════════════════════════════════════════
# SCHEDULE RESOLUTION
# ═══════════════════════════════════════════════════════════════

def _get_todays_events(utc_now: datetime) -> List[Dict]:
    """Resolve which events fall on this date."""
    events = []
    weekday = utc_now.weekday()  # Mon=0, Sun=6
    day = utc_now.day
    month = utc_now.month
    year = utc_now.year

    for name, cfg in HIGH_IMPACT_EVENTS.items():
        recurrence = cfg.get("recurrence", "")
        hour = cfg.get("hour", 0)
        minute = cfg.get("minute", 0)

        event_time = utc_now.replace(hour=hour, minute=minute, second=0, microsecond=0)
        if event_time.tzinfo is None:
            event_time = event_time.replace(tzinfo=timezone.utc)

        should_include = False

        if recurrence == "first_friday":
            # First Friday of the month
            if weekday == 4 and day <= 7:
                should_include = True

        elif recurrence == "weekly_thursday":
            if weekday == 3:  # Thursday
                should_include = True

        elif recurrence == "monthly":
            # Approximate: check if day falls in typical range
            day_range = cfg.get("typical_day_range", (10, 15))
            if day_range[0] <= day <= day_range[1] and weekday < 5:
                should_include = True

        elif recurrence == "fomc":
            # FOMC meets ~8 times/year. Approximate: 3rd Wednesday of
            # Jan, Mar, May, Jun, Jul, Sep, Nov, Dec
            fomc_months = {1, 3, 5, 6, 7, 9, 11, 12}
            if month in fomc_months and weekday == 2 and 15 <= day <= 21:
                should_include = True

        elif recurrence == "ecb":
            # ECB meets ~6 weeks apart. Approximate: 2nd Thursday of
            # Jan, Mar, Apr, Jun, Jul, Sep, Oct, Dec
            ecb_months = {1, 3, 4, 6, 7, 9, 10, 12}
            if month in ecb_months and weekday == 3 and 8 <= day <= 14:
                should_include = True

        elif recurrence == "boe":
            # BOE meets ~8 times/year. Approximate: 1st Thursday of
            # Feb, Mar, May, Jun, Aug, Sep, Nov, Dec
            boe_months = {2, 3, 5, 6, 8, 9, 11, 12}
            if month in boe_months and weekday == 3 and day <= 7:
                should_include = True

        if should_include:
            events.append({
                "name": name,
                "description": cfg.get("description", name),
                "event_time": event_time,
                "duration_minutes": cfg.get("duration_minutes", 30),
                "pairs": cfg.get("pairs", ALL_PAIRS),
            })

    return events
