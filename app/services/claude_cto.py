"""
Claude CTO Decision Engine
Claude operates as Chief Trading Officer of an institutional prop firm.
It receives the full signal context — enrichment, ML score, consensus score,
desk state, and firm-wide risk — and makes the final EXECUTE / SKIP / REDUCE
decision with institutional reasoning.
"""
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Optional

import httpx

from app.config import ANTHROPIC_API_KEY, DESKS, CAPITAL_PER_ACCOUNT

logger = logging.getLogger("TradingSystem.CTO")

CLAUDE_MODEL = "claude-sonnet-4-20250514"

# ─────────────────────────────────────────────────────────────
# SYSTEM PROMPT — CTO MODE
# ─────────────────────────────────────────────────────────────
CTO_SYSTEM_PROMPT = """You are the Chief Trading Officer (CTO) of OniQuant, a six-desk institutional prop firm managing $600,000 across six FTMO accounts.

CRITICAL CONTEXT: Every signal reaching you has ALREADY passed through:
1. LuxAlgo ML Classifier (level 3-4 trend-continuation only)
2. Smart Trail / Trend Catcher overlay filter
3. Hurst exponent chop filter (ranging markets already removed)
4. Consensus scoring (minimum score 3/10 to reach you)

These are PRE-FILTERED, high-quality signals. Your historical approval rate is ~14% — this is FAR too low. A missed profitable trade costs the same as a bad trade. TARGET APPROVAL RATE: 40-55%.

EVALUATION FRAMEWORK (score each factor 1-5, approve if total >= 18/35):

1. SIGNAL QUALITY (weight: high)
   - Is it a Strong/Plus signal? (+1 if yes)
   - R:R ratio above 1.5:1? (+1 if yes)
   - ML score above 0.55? (+1 if yes)

2. MARKET CONTEXT (weight: high)
   - Trend alignment (EMA50/200)? (+1 if with trend)
   - Volatility regime suitable? (+1 if not extreme)
   - ADX > 20 (trending)? (+1 if yes)

3. TIMING (weight: medium)
   - In kill zone or active session? (+1 if yes)
   - Not end of session? (+1 if yes)

4. RISK STATE (weight: medium)
   - Daily loss within budget? (+1 if < 60% used)
   - Consecutive losses < 3? (+1 if yes)
   - Correlation group not maxed? (+1 if yes)

5. INTERMARKET (weight: low)
   - DXY not opposing? (+1 if aligned or neutral)
   - VIX not extreme? (+1 if < 30)

DECISION RULES:
- Score >= 25: EXECUTE at 100% size
- Score 20-24: EXECUTE at 75% size
- Score 18-19: REDUCE at 50% size
- Score 15-17: REDUCE at 25% size
- Score < 15: SKIP (provide specific reason)

HARD SKIP RULES (override scoring):
- No stop loss → SKIP
- Desk paused/closed → SKIP
- Daily loss > $4,500 → SKIP
- VIX > 35 for indices/equities → SKIP

RESPONSE FORMAT (JSON only, no markdown):
{
    "decision": "EXECUTE" | "REDUCE" | "SKIP",
    "size_multiplier": 0.25-1.0,
    "score": 18-35,
    "reasoning": "2-3 sentences with specific factor references",
    "risk_flags": [],
    "confidence": 0.0-1.0,
    "notes_for_log": "Brief note"
}

DESK NOTES:
- DESK1_SCALPER: Speed matters. Auto-EXECUTE if score >= 20 AND in kill zone.
- DESK4_GOLD: Gold trends persist. DXY inverse correlation is your edge. Be MORE aggressive when DXY is falling and gold is bullish.
- DESK5_ALTS: VIX 25-30 = REDUCE, not SKIP. Only SKIP above 35.
- DESK3_SWING: Daily EMA alignment is the primary filter. If D1 trend matches, trust the signal.
"""


class ClaudeCTO:
    """
    Claude CTO Decision Engine.
    Makes final trade decisions with institutional reasoning.
    Falls back to rule-based decisions if API is unavailable.
    """

    def __init__(self):
        self.api_key = ANTHROPIC_API_KEY
        self.client = httpx.AsyncClient(timeout=30.0)

    async def decide(
        self,
        signal_data: Dict,
        enrichment: Dict,
        ml_result: Dict,
        consensus: Dict,
        desk_state: Dict,
        firm_risk: Dict,
    ) -> Dict:
        """
        Make the final execution decision.
        Returns decision dict with reasoning.
        """
        # ── Hard rules that override everything ──
        hard_skip = self._check_hard_rules(
            signal_data, consensus, desk_state, firm_risk
        )
        if hard_skip:
            return hard_skip

        # ── Build context for Claude ──
        if self.api_key:
            try:
                return await self._claude_decision(
                    signal_data, enrichment, ml_result,
                    consensus, desk_state, firm_risk,
                )
            except Exception as e:
                logger.error(f"Claude API error, using rule-based fallback: {e}")
                return self._rule_based_decision(
                    signal_data, enrichment, ml_result,
                    consensus, desk_state,
                )
        else:
            logger.info("No Anthropic API key — using rule-based decisions")
            return self._rule_based_decision(
                signal_data, enrichment, ml_result,
                consensus, desk_state,
            )

    def _check_hard_rules(
        self,
        signal_data: Dict,
        consensus: Dict,
        desk_state: Dict,
        firm_risk: Dict,
    ) -> Optional[Dict]:
        """
        Non-negotiable rules that always result in SKIP.
        These override Claude's judgment — safety first.
        """
        reasons = []

        # Consensus below minimum — only skip if score is 0 (no valid signal at all)
        if consensus.get("total_score", 0) < 1:
            reasons.append(
                f"Consensus score {consensus.get('total_score', 0)} — no valid signal detected"
            )

        # Desk paused or closed
        if desk_state.get("is_paused"):
            reasons.append("Desk is currently paused")
        if not desk_state.get("is_active", True):
            reasons.append("Desk is closed for the day")

        # Approaching daily loss limit
        daily_loss = abs(desk_state.get("daily_loss", 0))
        if daily_loss >= 4500:
            reasons.append(
                f"Daily loss ${daily_loss:.0f} approaching FTMO limit of $5,000"
            )

        # Consecutive losses
        consec = desk_state.get("consecutive_losses", 0)
        if consec >= 5:
            reasons.append(
                f"Desk has {consec} consecutive losses — closed for day"
            )
        elif consec >= 4:
            reasons.append(
                f"Desk has {consec} consecutive losses — paused 2 hours"
            )

        # No stop loss
        if signal_data.get("sl1") is None:
            reasons.append("No stop loss level — naked trades prohibited")

        # Firm-wide daily drawdown halt
        if firm_risk.get("firm_drawdown_exceeded"):
            reasons.append("Firm-wide daily drawdown exceeded $30,000")

        if reasons:
            return {
                "decision": "SKIP",
                "size_multiplier": 0.0,
                "reasoning": "; ".join(reasons),
                "risk_flags": reasons,
                "confidence": 1.0,
                "notes_for_log": f"Hard rule SKIP: {reasons[0]}",
            }

        return None

    async def _claude_decision(
        self,
        signal_data: Dict,
        enrichment: Dict,
        ml_result: Dict,
        consensus: Dict,
        desk_state: Dict,
        firm_risk: Dict,
    ) -> Dict:
        """Call Claude API for institutional decision."""

        # Build the briefing for Claude
        desk_id = consensus.get("desk_id", "UNKNOWN")
        desk = DESKS.get(desk_id, {})

        briefing = {
            "signal": {
                "symbol": signal_data.get("symbol"),
                "direction": signal_data.get("direction"),
                "alert_type": signal_data.get("alert_type"),
                "timeframe": signal_data.get("timeframe"),
                "price": signal_data.get("price"),
                "stop_loss": signal_data.get("sl1"),
                "take_profit_1": signal_data.get("tp1"),
                "take_profit_2": signal_data.get("tp2"),
                "smart_trail": signal_data.get("smart_trail"),
            },
            "desk": {
                "id": desk_id,
                "name": desk.get("name"),
                "style": desk.get("style"),
                "risk_pct": desk.get("risk_pct"),
                "max_trades_day": desk.get("max_trades_day"),
            },
            "consensus": {
                "total_score": consensus.get("total_score"),
                "tier": consensus.get("tier"),
                "size_multiplier": consensus.get("size_multiplier"),
                "breakdown": consensus.get("breakdown"),
            },
            "ml_model": {
                "score": ml_result.get("ml_score"),
                "method": ml_result.get("ml_method"),
            },
            "market_context": {
                "rsi": enrichment.get("rsi"),
                "rsi_zone": enrichment.get("rsi_zone"),
                "atr_pct": enrichment.get("atr_pct"),
                "volatility_regime": enrichment.get("volatility_regime"),
                "active_session": enrichment.get("active_session"),
                "kill_zone": enrichment.get("kill_zone_type"),
                "spread": enrichment.get("spread"),
            },
            "intermarket": enrichment.get("intermarket", {}),
            "desk_state": {
                "trades_today": desk_state.get("trades_today", 0),
                "daily_pnl": desk_state.get("daily_pnl", 0),
                "consecutive_losses": desk_state.get("consecutive_losses", 0),
                "size_modifier": desk_state.get("size_modifier", 1.0),
                "open_positions": desk_state.get("open_positions", 0),
            },
            "firm_risk": {
                "total_firm_daily_pnl": firm_risk.get("total_daily_pnl", 0),
                "desks_with_correlated_exposure": firm_risk.get("correlated_desks", []),
                "firm_drawdown_level": firm_risk.get("drawdown_level", "NORMAL"),
            },
        }

        user_message = (
            f"SIGNAL BRIEFING — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n\n"
            f"{json.dumps(briefing, indent=2)}\n\n"
            f"Make your decision. Respond with JSON only."
        )

        # Call Claude
        resp = await self.client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": self.api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": CLAUDE_MODEL,
                "max_tokens": 500,
                "system": CTO_SYSTEM_PROMPT,
                "messages": [{"role": "user", "content": user_message}],
            },
        )

        if resp.status_code != 200:
            logger.error(
                f"Claude API returned {resp.status_code}: {resp.text[:300]}"
            )
            raise Exception(f"Claude API error: {resp.status_code}")

        # Parse response
        response_data = resp.json()
        content = response_data.get("content", [])
        text = ""
        for block in content:
            if block.get("type") == "text":
                text += block.get("text", "")

        # Parse JSON from Claude's response
        text = text.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[-1].rsplit("```", 1)[0]

        try:
            decision = json.loads(text)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse Claude response: {text[:200]}")
            raise Exception(f"Claude JSON parse error: {e}")

        # Validate required fields
        if decision.get("decision") not in ("EXECUTE", "REDUCE", "SKIP"):
            raise Exception(
                f"Invalid decision: {decision.get('decision')}"
            )

        # Ensure size_multiplier is within bounds
        mult = decision.get("size_multiplier", 1.0)
        decision["size_multiplier"] = max(0.0, min(1.0, float(mult)))

        logger.info(
            f"CTO Decision: {decision['decision']} | "
            f"Size: {decision['size_multiplier']*100:.0f}% | "
            f"Confidence: {decision.get('confidence', 'N/A')} | "
            f"{decision.get('reasoning', '')[:100]}"
        )

        return decision

    def _rule_based_decision(
        self,
        signal_data: Dict,
        enrichment: Dict,
        ml_result: Dict,
        consensus: Dict,
        desk_state: Dict,
    ) -> Dict:
        """
        Rule-based fallback when Claude API is unavailable.
        Follows the consensus scoring tiers strictly.
        """
        tier = consensus.get("tier", "SKIP")
        score = consensus.get("total_score", 0)
        ml_score = ml_result.get("ml_score", 0.5)
        size_mult = consensus.get("size_multiplier", 0.0)

        # Start with consensus recommendation
        if tier == "SKIP":
            return {
                "decision": "SKIP",
                "size_multiplier": 0.0,
                "reasoning": f"Consensus score {score} below minimum. Tier: SKIP.",
                "risk_flags": [],
                "confidence": 0.8,
                "notes_for_log": f"Rule-based SKIP: score {score}",
            }

        # ML score adjusts size, NOT whether to trade
        if tier == "HIGH" and ml_score >= 0.50:
            decision = "EXECUTE"
            reasoning = (
                f"Strong alignment: Consensus {score} ({tier}), "
                f"ML probability {ml_score:.2f}. Full size approved."
            )
        elif tier == "HIGH":
            decision = "EXECUTE"
            size_mult *= 0.75
            reasoning = (
                f"High consensus {score} with moderate ML {ml_score:.2f}. "
                f"Executing at 75% size."
            )
        elif tier == "MEDIUM":
            decision = "EXECUTE"
            reasoning = (
                f"MEDIUM consensus {score}, ML {ml_score:.2f}. "
                f"Executing at {size_mult*100:.0f}% size."
            )
        elif tier == "LOW":
            decision = "REDUCE"
            size_mult *= 0.5
            reasoning = (
                f"LOW consensus {score}, ML {ml_score:.2f}. "
                f"Reducing to {size_mult*100:.0f}% size."
            )
        else:
            decision = "EXECUTE"
            reasoning = (
                f"Consensus {score} ({tier}), ML {ml_score:.2f}. "
                f"Executing at {size_mult*100:.0f}% size."
            )

        # Consecutive loss size adjustment
        consec = desk_state.get("consecutive_losses", 0)
        state_modifier = desk_state.get("size_modifier", 1.0)
        if state_modifier < 1.0:
            size_mult *= state_modifier
            reasoning += (
                f" Size reduced to {state_modifier*100:.0f}% "
                f"due to {consec} consecutive losses."
            )

        # Volatility regime caution
        regime = enrichment.get("volatility_regime", "NORMAL")
        if regime == "HIGH_VOLATILITY" and tier != "HIGH":
            size_mult *= 0.75
            reasoning += " Size reduced 25% for high volatility regime."

        risk_flags = []
        if regime == "HIGH_VOLATILITY":
            risk_flags.append("high_volatility")
        if consec >= 2:
            risk_flags.append(f"consecutive_losses_{consec}")
        if ml_score < 0.50:
            risk_flags.append("below_average_ml_score")

        return {
            "decision": decision,
            "size_multiplier": round(max(0.0, min(1.0, size_mult)), 2),
            "reasoning": reasoning,
            "risk_flags": risk_flags,
            "confidence": round(ml_score * 0.6 + (score / 12) * 0.4, 2),
            "notes_for_log": (
                f"Rule-based {decision}: score={score} ml={ml_score:.2f}"
            ),
        }

    async def close(self):
        """Close the HTTP client."""
        await self.client.aclose()
