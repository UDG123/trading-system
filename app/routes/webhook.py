"""
Webhook Route — Receives and processes TradingView/LuxAlgo alerts.
Entry point for the entire trading pipeline.

Latency tracking: Captures the delta between TradingView's alert timestamp
and our server arrival time. This feeds into the ML training data for
NovaQuant schema alignment.
"""
import json
import time
import asyncio
import logging
import re
import math
from datetime import datetime, timezone
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Request, BackgroundTasks
from sqlalchemy.orm import Session

from app.database import get_db, SessionLocal
from app.schemas import TradingViewAlert, SignalResponse
from app.services.signal_validator import SignalValidator
from app.config import WEBHOOK_SECRET, SYMBOL_ALIASES, get_desk_for_symbol
from app.models.signal import Signal
from app.models.trade import Trade

logger = logging.getLogger("TradingSystem.Webhook")
router = APIRouter()

# NaN/Infinity regex for sanitizing TradingView {{plot()}} output
_NAN_PATTERN = re.compile(r':\s*NaN\b', re.IGNORECASE)
_INF_PATTERN = re.compile(r':\s*-?Infinity\b', re.IGNORECASE)


def _sanitize_json(raw: str) -> str:
    """Replace NaN/Infinity tokens with null for valid JSON."""
    result = _NAN_PATTERN.sub(': null', raw)
    result = _INF_PATTERN.sub(': null', result)
    return result


def _clean_na_strings(payload: dict) -> dict:
    """Convert LuxAlgo 'na'/'NaN' string values to None for indicator fields."""
    na_fields = ("tp1", "tp2", "sl1", "sl2", "smart_trail")
    for key in na_fields:
        val = payload.get(key)
        if val is None:
            continue
        if isinstance(val, str):
            if val.lower() in ("nan", "na", "n/a", ""):
                payload[key] = None
            else:
                try:
                    payload[key] = float(val)
                except ValueError:
                    payload[key] = None
        elif isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
            payload[key] = None
    return payload


def _map_luxalgo_fields(payload: dict) -> dict:
    """Auto-map LuxAlgo native JSON field names to OniQuant schema."""
    # LuxAlgo v6+ sends "ticker" instead of "symbol"
    if "ticker" in payload and "symbol" not in payload:
        payload["symbol"] = payload.pop("ticker")
    # LuxAlgo v6.0.2+ sends "bartime" instead of "time"
    if "bartime" in payload and "time" not in payload:
        payload["time"] = payload.pop("bartime")
    # LuxAlgo sends "alert" or "signal" or "type" instead of "alert_type"
    if "alert_type" not in payload:
        for alt_key in ("alert", "signal", "type", "message", "condition"):
            if alt_key in payload:
                payload["alert_type"] = payload.pop(alt_key)
                break
    # LuxAlgo/TradingView may send "interval" or "tf" instead of "timeframe"
    if "timeframe" not in payload:
        for alt_key in ("interval", "tf"):
            if alt_key in payload:
                payload["timeframe"] = payload.pop(alt_key)
                break
    # LuxAlgo ohlcv nested object — flatten if present
    if "ohlcv" in payload and isinstance(payload["ohlcv"], dict):
        ohlcv = payload.pop("ohlcv")
        if "close" in ohlcv and "price" not in payload:
            payload["price"] = ohlcv["close"]
        if "volume" in ohlcv and "volume" not in payload:
            payload["volume"] = ohlcv["volume"]
    # LuxAlgo "close" field → "price"
    if "close" in payload and "price" not in payload:
        payload["price"] = payload.pop("close")
    # LuxAlgo v6.0.3+ sends "bar_color" (1=green, 0=purple, -1=red)
    if "bar_color" in payload and "mse" not in payload:
        payload.pop("bar_color", None)
    return payload


def _parse_webhook_body(raw_text: str) -> dict:
    """Parse webhook body — handles JSON, NaN-contaminated JSON, and plain text."""
    if not raw_text:
        return None

    # Try direct JSON parse
    try:
        return json.loads(raw_text)
    except (json.JSONDecodeError, ValueError):
        pass

    # Try with NaN/Infinity sanitization
    try:
        sanitized = _sanitize_json(raw_text)
        data = json.loads(sanitized)
        logger.info("Fixed NaN/Infinity in webhook payload")
        return data
    except (json.JSONDecodeError, ValueError):
        pass

    return None  # Not JSON — will be handled as plain text


def _calculate_webhook_latency(tv_time_str) -> Optional[int]:
    """
    Calculate milliseconds between TradingView alert fire and server arrival.

    TradingView sends `{time}` as a UTC Unix timestamp string in SECONDS
    (e.g. "1710432000" or "1710432000.123"). Some LuxAlgo versions send
    milliseconds directly. We handle both.

    Returns:
        Latency in milliseconds, or None if TV time is missing/unparseable.
    """
    if not tv_time_str:
        return None

    try:
        arrival_ms = int(time.time() * 1000)
        tv_val = float(str(tv_time_str).strip())

        # Detect if value is in seconds or milliseconds
        # Unix seconds are ~1.7 billion, milliseconds are ~1.7 trillion
        if tv_val > 1e12:
            # Already in milliseconds
            tv_ms = int(tv_val)
        else:
            # Convert seconds to milliseconds
            tv_ms = int(tv_val * 1000)

        latency = arrival_ms - tv_ms

        # Sanity check: latency should be 0-60,000ms (0-60 seconds)
        # If negative or huge, the TV time is garbage
        if latency < -5000 or latency > 300_000:
            logger.debug(
                f"Webhook latency out of range: {latency}ms "
                f"(tv_time={tv_time_str}, arrival={arrival_ms})"
            )
            return None

        return max(0, latency)

    except (ValueError, TypeError) as e:
        logger.debug(f"Could not parse TV time '{tv_time_str}': {e}")
        return None


@router.post("/webhook/{path_secret}", response_model=SignalResponse)
async def receive_webhook_with_secret(
    path_secret: str,
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Path-authenticated webhook for LuxAlgo S&O/PAC/OM alerts.
    LuxAlgo's alert() function overrides the Message field, so the secret
    CANNOT be in the JSON body. Put it in the URL instead:
    https://...railway.app/api/webhook/OniQuant_X9k7mP2w_2026

    This is the PRIMARY endpoint for all LuxAlgo alerts.
    """
    if path_secret != WEBHOOK_SECRET:
        logger.warning(f"Path auth failed from {request.client.host}")
        raise HTTPException(status_code=401, detail="Invalid webhook secret")

    return await _process_webhook(request, background_tasks, db, auth_verified=True)


@router.post("/webhook", response_model=SignalResponse)
async def receive_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    """
    Body-authenticated webhook for MSE alerts (secret inside JSON body).
    Also accepts ?secret= query parameter as fallback.
    """
    return await _process_webhook(request, background_tasks, db, auth_verified=False)


async def _process_webhook(
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session,
    auth_verified: bool = False,
):
    """
    Shared webhook processing logic.
    Pipeline: Receive → Authenticate → Validate → Log → Route → Pipeline

    Accepts:
      - MSE alerts: JSON body with secret inside
      - LuxAlgo native JSON: auto-maps field names (ticker→symbol, bartime→time, alert→alert_type)
      - LuxAlgo plain text: wraps "Bullish Confirmation Signal" etc into schema
      - NaN-contaminated JSON from {{plot()}} calls
    """
    # ── 0. Capture arrival time immediately ──
    arrival_time = time.time()

    # ── 1. Read raw body ──
    try:
        raw_body = await request.body()
        raw_text = raw_body.decode("utf-8").strip()
    except Exception as e:
        logger.error(f"Request read error: {e}")
        raise HTTP
