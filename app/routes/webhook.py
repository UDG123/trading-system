"""
Webhook Route — High-Speed Redis Ingestor (OniQuant v5.9)

Dumb ingestor pattern: validate schema → dedup → XADD → HTTP 202.
Target: < 50ms response. No DB writes, no pipeline calls, no broker code.
All heavy lifting deferred to the Redis stream consumer (app/worker.py).
"""
import hashlib
import math
import re
import time
import logging
from typing import Optional

import orjson
from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import ORJSONResponse

from app.config import WEBHOOK_SECRET, SYMBOL_ALIASES, get_desk_for_symbol

logger = logging.getLogger("TradingSystem.Webhook")
router = APIRouter()

# ─── Constants ───
STREAM_KEY = "oniquant_alerts"
DEDUP_PREFIX = "dedup:"
DEDUP_TTL_SECONDS = 60

_NAN_RE = re.compile(r':\s*NaN\b', re.IGNORECASE)
_INF_RE = re.compile(r':\s*-?Infinity\b', re.IGNORECASE)

# LuxAlgo {default} message → internal alert_type mapping (81 alert coverage)
_LUXALGO_MAP = {
    "bullish confirmation signal": "bullish_confirmation",
    "bearish confirmation signal": "bearish_confirmation",
    "bullish+ confirmation signal": "bullish_plus",
    "bearish+ confirmation signal": "bearish_plus",
    "strong bullish confirmation signal": "bullish_plus",
    "strong bearish confirmation signal": "bearish_plus",
    "bullish confirmation+ signal": "bullish_confirmation_plus",
    "bearish confirmation+ signal": "bearish_confirmation_plus",
    "bullish exit signal": "bullish_exit",
    "bearish exit signal": "bearish_exit",
    "bullish contrarian signal": "contrarian_bullish",
    "bearish contrarian signal": "contrarian_bearish",
    "confirmation turn bullish": "confirmation_turn_bullish",
    "confirmation turn bearish": "confirmation_turn_bearish",
    "confirmation turn plus": "confirmation_turn_plus",
    "bullish turn +": "confirmation_turn_bullish",
    "bearish turn +": "confirmation_turn_bearish",
    "bullish turn": "confirmation_turn_bullish",
    "bearish turn": "confirmation_turn_bearish",
    "bullish confirmation+": "bullish_confirmation_plus",
    "bearish confirmation+": "bearish_confirmation_plus",
    "take profit": "take_profit",
    "stop loss": "stop_loss",
    "smart trail cross": "smart_trail_cross",
    "smart trail crossed": "smart_trail_cross",
    "confirmation bullish exit": "bullish_exit",
    "confirmation bearish exit": "bearish_exit",
    "confirmation bullish exit signal": "bullish_exit",
    "confirmation bearish exit signal": "bearish_exit",
    "internal bullish bos formed": "smc_bullish_bos",
    "bearish bos formed": "smc_bearish_bos",
    "internal bullish choch formed": "smc_bullish_choch",
    "bearish choch formed": "smc_bearish_choch",
    "bullish fvg formed": "smc_bullish_fvg",
    "bearish fvg formed": "smc_bearish_fvg",
    "equal highs detected": "smc_equal_highs",
    "equal lows detected": "smc_equal_lows",
    "price broke bullish internal ob": "smc_bullish_ob_break",
    "price broke bearish internal ob": "smc_bearish_ob_break",
    "price broke bullish swing ob": "smc_bullish_ob_break",
    "price broke bearish swing ob": "smc_bearish_ob_break",
}

_TP_RE = re.compile(r'^tp\d?\s+[\d.,]+\s+reached$')
_SL_RE = re.compile(r'^sl\d?\s+[\d.,]+\s+reached$')


# ─── Redis handle (set during app lifespan) ───
_redis = None


def set_redis(redis_client):
    """Called from app lifespan to inject the shared Redis connection."""
    global _redis
    _redis = redis_client


def _get_redis():
    if _redis is None:
        raise HTTPException(status_code=503, detail="Redis not available")
    return _redis


# ─── Pure functions (no I/O) ───

def _parse_body(raw: bytes) -> Optional[dict]:
    """Parse JSON with orjson, fallback to NaN/Infinity sanitization."""
    if not raw:
        return None
    try:
        return orjson.loads(raw)
    except (orjson.JSONDecodeError, ValueError):
        pass
    # Fallback: sanitize NaN/Infinity in string form
    try:
        text = raw.decode("utf-8")
        cleaned = _NAN_RE.sub(': null', text)
        cleaned = _INF_RE.sub(': null', cleaned)
        return orjson.loads(cleaned.encode("utf-8"))
    except Exception:
        pass
    return None


def _map_fields(p: dict) -> dict:
    """Map LuxAlgo field names to OniQuant schema."""
    if "ticker" in p and "symbol" not in p:
        p["symbol"] = p.pop("ticker")
    if "bartime" in p and "time" not in p:
        p["time"] = str(p.pop("bartime"))
    elif "time" in p and not isinstance(p["time"], str):
        p["time"] = str(p["time"])
    if "alert_type" not in p:
        for k in ("alert", "signal", "type", "message", "condition"):
            if k in p:
                p["alert_type"] = p.pop(k)
                break
    if "timeframe" not in p:
        for k in ("interval", "tf"):
            if k in p:
                p["timeframe"] = p.pop(k)
                break
    if "ohlcv" in p and isinstance(p["ohlcv"], dict):
        ohlcv = p.pop("ohlcv")
        if "close" in ohlcv and "price" not in p:
            p["price"] = ohlcv["close"]
        if "volume" in ohlcv and "volume" not in p:
            p["volume"] = ohlcv["volume"]
    if "close" in p and "price" not in p:
        p["price"] = p.pop("close")
    p.pop("bar_color", None)
    return p


def _clean_na(p: dict) -> dict:
    """Convert 'na'/'NaN' strings to None for indicator fields."""
    for key in ("tp1", "tp2", "sl1", "sl2", "smart_trail"):
        val = p.get(key)
        if val is None:
            continue
        if isinstance(val, str):
            if val.lower() in ("nan", "na", "n/a", ""):
                p[key] = None
            else:
                try:
                    p[key] = float(val)
                except ValueError:
                    p[key] = None
        elif isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
            p[key] = None
    return p


def _normalize_alert_type(raw_type: str) -> str:
    """Normalize alert type — mirrors TradingViewAlert.normalize_alert_type."""
    cleaned = raw_type.strip().lower()
    if cleaned in _LUXALGO_MAP:
        return _LUXALGO_MAP[cleaned]
    if _TP_RE.match(cleaned):
        return "take_profit"
    if _SL_RE.match(cleaned):
        return "stop_loss"
    if cleaned.startswith("smart trail") and "reached" in cleaned:
        return "smart_trail_cross"
    result = cleaned.replace(" ", "_").replace("-", "_").replace("+", "_plus")
    return result[:50]


def _direction(alert_type: str) -> Optional[str]:
    """Derive trade direction from alert type."""
    at = alert_type.lower()
    if "bullish" in at:
        return "LONG"
    if "bearish" in at:
        return "SHORT"
    if "exit" in at or at in ("take_profit", "stop_loss", "smart_trail_cross"):
        return "EXIT"
    return None


def _calc_latency(tv_time_str) -> Optional[int]:
    """Calculate ms between TV alert fire and server arrival."""
    if not tv_time_str:
        return None
    try:
        arrival_ms = int(time.time() * 1000)
        tv_val = float(str(tv_time_str).strip())
        tv_ms = int(tv_val) if tv_val > 1e12 else int(tv_val * 1000)
        latency = arrival_ms - tv_ms
        if latency < -5000 or latency > 300_000:
            return None
        return max(0, latency)
    except (ValueError, TypeError):
        return None


def _compute_dedup_hash(payload: dict) -> str:
    """SHA-256 of (symbol, alert_type, timeframe, price) for 60s dedup."""
    key_parts = (
        str(payload.get("symbol", "")),
        str(payload.get("alert_type", "")),
        str(payload.get("timeframe", "")),
        str(payload.get("price", "")),
    )
    return hashlib.sha256("|".join(key_parts).encode()).hexdigest()[:16]


# ─── PATH-AUTHENTICATED ENDPOINT (for LuxAlgo S&O/PAC/OM) ───

@router.post("/webhook/{path_secret}", response_class=ORJSONResponse, status_code=202)
async def webhook_path_auth(path_secret: str, request: Request):
    """LuxAlgo alerts — secret in URL path since alert() overrides message body."""
    if path_secret != WEBHOOK_SECRET:
        logger.warning(f"Path auth failed from {request.client.host}")
        raise HTTPException(status_code=401, detail="Invalid webhook secret")
    return await _ingest(request, authed=True)


# ─── BODY-AUTHENTICATED ENDPOINT (for MSE) ───

@router.post("/webhook", response_class=ORJSONResponse, status_code=202)
async def webhook_body_auth(request: Request):
    """MSE alerts — secret inside JSON body."""
    return await _ingest(request, authed=False)


# ─── CORE INGESTOR ───

async def _ingest(request: Request, authed: bool = False) -> dict:
    """
    Validate → Dedup → XADD → 202 Accepted.
    No DB writes. No background tasks. No broker calls.
    """
    arrival_ns = time.monotonic_ns()
    redis = _get_redis()

    # ── Read raw body ──
    raw = await request.body()
    if not raw:
        raise HTTPException(status_code=400, detail="Empty body")

    # ── Parse JSON (orjson — zero-copy where possible) ──
    payload = _parse_body(raw)
    if payload and isinstance(payload, dict):
        payload = _map_fields(payload)
        payload = _clean_na(payload)
    else:
        # Plain-text alert — wrap it
        text = raw.decode("utf-8", errors="replace").strip()
        payload = {
            "symbol": request.query_params.get("symbol", "UNKNOWN"),
            "exchange": request.query_params.get("exchange", ""),
            "timeframe": request.query_params.get("timeframe", "60"),
            "alert_type": text,
            "price": 0,
        }

    # ── Auth (body/query secret for non-path-auth routes) ──
    if not authed:
        secret = payload.get("secret", "")
        q_secret = request.query_params.get("secret", "")
        if secret != WEBHOOK_SECRET and q_secret != WEBHOOK_SECRET:
            logger.warning(f"Auth failed from {request.client.host}")
            raise HTTPException(status_code=401, detail="Invalid webhook secret")

    # Strip secret before storing in Redis
    payload.pop("secret", None)

    # ── Normalize fields ──
    raw_alert = payload.get("alert_type", "unknown")
    alert_type = _normalize_alert_type(raw_alert)
    payload["alert_type"] = alert_type

    symbol_raw = (payload.get("symbol") or "UNKNOWN").strip().upper()
    exchange = payload.get("exchange", "")
    lookup_key = f"{exchange}:{symbol_raw}" if exchange else symbol_raw
    symbol_normalized = SYMBOL_ALIASES.get(lookup_key, symbol_raw.replace("/", ""))
    payload["symbol"] = symbol_raw
    payload["symbol_normalized"] = symbol_normalized

    if "timeframe" in payload:
        payload["timeframe"] = str(payload["timeframe"]).strip().upper()

    direction = _direction(alert_type)
    payload["direction"] = direction

    desks = get_desk_for_symbol(symbol_normalized)
    if payload.get("desk") and payload["desk"] != "auto":
        from app.config import DESKS
        if payload["desk"] in DESKS:
            desks = [payload["desk"]]
    payload["desks_matched"] = desks

    latency_ms = _calc_latency(payload.get("time"))
    payload["webhook_latency_ms"] = latency_ms

    # ── Idempotency: 60s dedup via Redis SET ──
    dedup_hash = _compute_dedup_hash(payload)
    dedup_key = f"{DEDUP_PREFIX}{dedup_hash}"

    is_new = await redis.set(dedup_key, b"1", ex=DEDUP_TTL_SECONDS, nx=True)
    if not is_new:
        elapsed_ms = (time.monotonic_ns() - arrival_ns) / 1_000_000
        logger.info(
            f"DEDUP | {symbol_normalized} {alert_type} | "
            f"hash={dedup_hash} | {elapsed_ms:.1f}ms"
        )
        return {
            "status": "duplicate",
            "message": "Signal already received within 60s window",
            "hash": dedup_hash,
        }

    # ── XADD to Redis Stream ──
    # orjson.dumps returns bytes — store as single field for consumer efficiency
    stream_payload = orjson.dumps(payload)
    await redis.xadd(
        STREAM_KEY,
        {"payload": stream_payload},
    )

    elapsed_ms = (time.monotonic_ns() - arrival_ns) / 1_000_000

    logger.info(
        f"INGESTED | {symbol_normalized} | {alert_type} | "
        f"{direction or 'N/A'} | Desks: {desks} | "
        f"Latency: {f'{latency_ms}ms' if latency_ms is not None else 'N/A'} | "
        f"Ingest: {elapsed_ms:.1f}ms"
    )

    return {
        "status": "accepted",
        "stream": STREAM_KEY,
        "symbol": symbol_normalized,
        "alert_type": alert_type,
        "desks_matched": desks,
        "hash": dedup_hash,
        "ingest_time_ms": round(elapsed_ms, 1),
    }


# ─── READ-ONLY SIGNAL STATUS (kept for backward compat) ───

@router.get("/signal/{signal_id}")
async def get_signal(signal_id: int):
    """Get signal status from DB. Lightweight read-only query."""
    from sqlalchemy.orm import Session
    from app.database import SessionLocal
    from app.models.signal import Signal

    db: Session = SessionLocal()
    try:
        signal = db.query(Signal).filter(Signal.id == signal_id).first()
        if not signal:
            raise HTTPException(status_code=404, detail="Signal not found")
        return {
            "id": signal.id,
            "symbol": signal.symbol_normalized,
            "alert_type": signal.alert_type,
            "direction": signal.direction,
            "price": signal.price,
            "status": signal.status,
            "desk_id": signal.desk_id,
            "desks_matched": signal.desks_matched,
            "ml_score": signal.ml_score,
            "consensus_score": signal.consensus_score,
            "claude_decision": signal.claude_decision,
            "claude_reasoning": signal.claude_reasoning,
            "position_size_pct": signal.position_size_pct,
            "processing_time_ms": signal.processing_time_ms,
            "webhook_latency_ms": signal.webhook_latency_ms,
            "received_at": signal.received_at.isoformat() if signal.received_at else None,
        }
    finally:
        db.close()
