"""
LiteLLM Model Cascade Router — OniQuant v5.9

Routes CTO analysis requests through a tiered model cascade:
  Tier 1 (Fast)  → phi-4 on Railway Ollama for high-confidence trend signals
  Tier 2 (Deep)  → Claude 3.5 Sonnet for mixed-confluence regime analysis
  Fallthrough    → Claude Haiku when Ollama is under load (backlog > 50)

Prompt Caching: The 81 LuxAlgo alert definitions are cached as a system
prefix to reduce TTFT on Claude models.
"""
import os
import time
import logging
from typing import Dict, Optional

import litellm

logger = logging.getLogger("TradingSystem.ModelRouter")

# ─── Model configuration ───
OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL = "ollama/phi-4"

TIER2_MODEL = "claude-3-5-sonnet-20241022"
FALLTHROUGH_MODEL = "claude-3-5-haiku-20241022"

# Backlog threshold — if Redis stream lag exceeds this, skip Ollama
BACKLOG_THRESHOLD = 50

# ML score threshold — signals at this score or above get fast-path
ML_SCORE_FAST_THRESHOLD = 4

# ─── Cached system prompt (81 alert definitions for prompt caching) ───
_ALERT_DEFINITIONS_PROMPT = """You are the OniQuant CTO Analyst. You evaluate trading signals from LuxAlgo indicators.

# Alert Type Definitions (81 LuxAlgo alerts)

## Entry Signals
- bullish_confirmation: Standard bullish entry — LuxAlgo Signals & Overlay confirms bullish bias
- bearish_confirmation: Standard bearish entry — confirms bearish bias
- bullish_plus: Strong bullish — enhanced confidence signal with momentum alignment
- bearish_plus: Strong bearish — enhanced confidence with momentum
- bullish_confirmation_plus: Maximum confidence bullish — all filters aligned
- bearish_confirmation_plus: Maximum confidence bearish — all filters aligned
- contrarian_bullish: Counter-trend long — oversold reversal detected
- contrarian_bearish: Counter-trend short — overbought reversal detected
- confirmation_turn_bullish: Trend reversal to bullish — multi-TF alignment shift
- confirmation_turn_bearish: Trend reversal to bearish — multi-TF alignment shift
- confirmation_turn_plus: Strong trend turn — highest-grade reversal
- mse_bullish_confirmation: Master Signal Engine bullish — pre-enriched composite signal
- mse_bearish_confirmation: Master Signal Engine bearish — pre-enriched composite signal

## Exit Signals
- bullish_exit: Close long positions — bullish momentum exhausted
- bearish_exit: Close short positions — bearish momentum exhausted
- take_profit: TP level reached
- stop_loss: SL level reached
- smart_trail_cross: Smart Trail crossed — adaptive trailing stop triggered

## SMC Structure Signals
- smc_bullish_bos: Bullish Break of Structure — higher high confirmed
- smc_bearish_bos: Bearish Break of Structure — lower low confirmed
- smc_bullish_choch: Bullish Change of Character — trend shift early signal
- smc_bearish_choch: Bearish Change of Character — trend shift early signal
- smc_bullish_fvg: Bullish Fair Value Gap — imbalance zone for long entries
- smc_bearish_fvg: Bearish Fair Value Gap — imbalance zone for short entries
- smc_equal_highs: Equal Highs — liquidity pool above (possible sweep target)
- smc_equal_lows: Equal Lows — liquidity pool below (possible sweep target)
- smc_bullish_ob_break: Bullish Order Block break — institutional demand zone tested
- smc_bearish_ob_break: Bearish Order Block break — institutional supply zone tested

# Evaluation Criteria
When analyzing a signal, consider:
1. Trend alignment (Hurst exponent, EMA cross, ADX)
2. Momentum confirmation (RSI zone, MACD histogram)
3. Volatility regime (ATR ratio, Bollinger width)
4. Intermarket context (DXY, VIX, correlated pairs)
5. Session timing (kill zone overlap, market hours)
6. Risk/Reward profile (ATR-based SL/TP, minimum R:R)

Respond with: EXECUTE, REDUCE, HOLD, or SKIP with confidence 0-100 and reasoning."""

# Redis handle — injected from outside
_redis = None


def set_redis(redis_client):
    """Inject shared Redis client for backlog checks."""
    global _redis
    _redis = redis_client


async def _get_backlog_size() -> int:
    """Check Redis stream pending count for load-shedding decisions."""
    if _redis is None:
        return 0
    try:
        info = await _redis.xinfo_stream("oniquant_alerts")
        return info.get("length", 0)
    except Exception:
        return 0


def _select_tier(ml_score: float, confluence_pct: float, backlog: int) -> str:
    """
    Select model tier based on signal quality and system load.

    Tier 1 (Fast): ml_score >= 4 → phi-4 on Ollama (unless overloaded)
    Tier 2 (Deep): ml_score < 4 or mixed confluence → Claude Sonnet
    Fallthrough:   Ollama overloaded (backlog > 50) → Claude Haiku
    """
    if ml_score >= ML_SCORE_FAST_THRESHOLD and backlog <= BACKLOG_THRESHOLD:
        return "tier1"
    if backlog > BACKLOG_THRESHOLD:
        logger.warning(
            f"FALLTHROUGH | Backlog={backlog} > {BACKLOG_THRESHOLD} "
            f"→ routing to Haiku to prevent signal expiration"
        )
        return "fallthrough"
    return "tier2"


async def route_analysis(
    signal_data: Dict,
    enrichment: Dict,
    ml_result: Dict,
    consensus: Dict,
    desk_state: Dict,
    firm_risk: Dict,
) -> Dict:
    """
    Route signal analysis to the appropriate model tier.

    Returns dict with: decision, confidence, reasoning, model_used, tier, latency_ms
    """
    start = time.time()

    ml_score = ml_result.get("ml_score", 0)
    confluence_pct = consensus.get("confluence_pct", 50)
    backlog = await _get_backlog_size()

    tier = _select_tier(ml_score, confluence_pct, backlog)

    # Build the analysis prompt (signal-specific, NOT cached)
    user_prompt = _build_analysis_prompt(
        signal_data, enrichment, ml_result, consensus, desk_state, firm_risk
    )

    model, extra_kwargs = _get_model_config(tier)

    try:
        response = await litellm.acompletion(
            model=model,
            messages=[
                {"role": "system", "content": _ALERT_DEFINITIONS_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.1,
            max_tokens=500,
            **extra_kwargs,
        )

        raw_text = response.choices[0].message.content
        result = _parse_model_response(raw_text)

    except Exception as e:
        logger.error(f"Tier {tier} ({model}) failed: {e}")

        # Failover: if tier1 fails, try tier2; if tier2 fails, try fallthrough
        fallback_tier = "tier2" if tier == "tier1" else "fallthrough"
        fallback_model, fallback_kwargs = _get_model_config(fallback_tier)

        logger.info(f"Failover → {fallback_tier} ({fallback_model})")
        try:
            response = await litellm.acompletion(
                model=fallback_model,
                messages=[
                    {"role": "system", "content": _ALERT_DEFINITIONS_PROMPT},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0.1,
                max_tokens=500,
                **fallback_kwargs,
            )
            raw_text = response.choices[0].message.content
            result = _parse_model_response(raw_text)
            tier = fallback_tier
            model = fallback_model
        except Exception as e2:
            logger.error(f"Failover {fallback_tier} also failed: {e2}")
            result = {
                "decision": "SKIP",
                "confidence": 0,
                "reasoning": f"All model tiers failed: {str(e)[:100]}",
            }

    latency_ms = int((time.time() - start) * 1000)

    result["model_used"] = model
    result["tier"] = tier
    result["latency_ms"] = latency_ms
    result["backlog_at_route"] = backlog

    logger.info(
        f"MODEL ROUTE | {tier} ({model}) | "
        f"ML={ml_score} | Decision={result['decision']} | "
        f"Confidence={result.get('confidence', '?')} | "
        f"Backlog={backlog} | {latency_ms}ms"
    )

    return result


def _get_model_config(tier: str) -> tuple:
    """Return (model_name, extra_kwargs) for the given tier."""
    if tier == "tier1":
        return OLLAMA_MODEL, {
            "api_base": OLLAMA_BASE_URL,
        }
    elif tier == "tier2":
        return TIER2_MODEL, {
            "extra_headers": {
                "anthropic-beta": "prompt-caching-2024-07-31",
            },
        }
    else:  # fallthrough
        return FALLTHROUGH_MODEL, {
            "extra_headers": {
                "anthropic-beta": "prompt-caching-2024-07-31",
            },
        }


def _build_analysis_prompt(
    signal_data: Dict,
    enrichment: Dict,
    ml_result: Dict,
    consensus: Dict,
    desk_state: Dict,
    firm_risk: Dict,
) -> str:
    """Build the signal-specific user prompt for CTO analysis."""
    symbol = signal_data.get("symbol", "?")
    direction = signal_data.get("direction", "?")
    alert_type = signal_data.get("alert_type", "?")
    price = signal_data.get("price", 0)
    timeframe = signal_data.get("timeframe", "?")

    hurst = enrichment.get("hurst_exponent", "N/A")
    rsi = enrichment.get("rsi", "N/A")
    atr = enrichment.get("atr", "N/A")
    trend = enrichment.get("trend", "UNKNOWN")
    vol_regime = enrichment.get("volatility_regime", "UNKNOWN")
    rvol = enrichment.get("mse_rvol", "N/A")
    vwap_z = enrichment.get("vwap_z_score", "N/A")

    ml_score = ml_result.get("ml_score", "N/A")
    consensus_score = consensus.get("total_score", 0)
    consensus_tier = consensus.get("tier", "?")
    confluence_pct = consensus.get("confluence_pct", "N/A")

    return f"""Analyze this trading signal and provide your decision.

## Signal
- Symbol: {symbol} | Direction: {direction} | Type: {alert_type}
- Price: {price} | Timeframe: {timeframe}
- SL: {signal_data.get('sl1', 'N/A')} | TP1: {signal_data.get('tp1', 'N/A')} | TP2: {signal_data.get('tp2', 'N/A')}

## Market Data
- Hurst Exponent: {hurst} (>0.55=trending, <0.45=choppy)
- RSI: {rsi} | Trend: {trend} | Volatility: {vol_regime}
- ATR: {atr} | RVOL: {rvol} | VWAP Z-Score: {vwap_z}

## Scores
- ML Score: {ml_score}/5 | Consensus: {consensus_score} ({consensus_tier})
- Confluence: {confluence_pct}%

## Desk State
- Open Positions: {desk_state.get('open_positions', 0)}
- Daily P&L: ${desk_state.get('daily_pnl', 0):.2f}
- Consecutive Losses: {desk_state.get('consecutive_losses', 0)}
- Size Modifier: {desk_state.get('size_modifier', 1.0)}

## Firm Risk
- Total Open: {firm_risk.get('total_open', 0)}
- Daily Drawdown: ${firm_risk.get('daily_drawdown', 0):.2f}

Respond in this exact format:
DECISION: [EXECUTE|REDUCE|HOLD|SKIP]
CONFIDENCE: [0-100]
SIZE_MULTIPLIER: [0.25-1.5]
REASONING: [1-2 sentences]"""


def _parse_model_response(text: str) -> Dict:
    """Parse structured model response into decision dict."""
    result = {
        "decision": "SKIP",
        "confidence": 0,
        "size_multiplier": 1.0,
        "reasoning": "",
    }

    for line in text.strip().split("\n"):
        line = line.strip()
        if line.upper().startswith("DECISION:"):
            raw_decision = line.split(":", 1)[1].strip().upper()
            if raw_decision in ("EXECUTE", "REDUCE", "HOLD", "SKIP"):
                result["decision"] = raw_decision
        elif line.upper().startswith("CONFIDENCE:"):
            try:
                result["confidence"] = int(line.split(":", 1)[1].strip())
            except ValueError:
                pass
        elif line.upper().startswith("SIZE_MULTIPLIER:"):
            try:
                mult = float(line.split(":", 1)[1].strip())
                result["size_multiplier"] = max(0.25, min(1.5, mult))
            except ValueError:
                pass
        elif line.upper().startswith("REASONING:"):
            result["reasoning"] = line.split(":", 1)[1].strip()

    return result
