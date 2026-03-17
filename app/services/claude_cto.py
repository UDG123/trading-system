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
CTO_SYSTEM_PROMPT = """You are the Chief Trading Officer (CTO) of OniQuant, a six-desk institutional prop firm managing $600,000 across six FTMO accounts ($100,000 each). You are the FLOOR BOSS. Eight analysts (bots) report to you.

YOUR DESKS:
- DESK1_SCALPER: FX 5M scalps. EURUSD, USDJPY, GBPUSD, AUDUSD. Fast in/out.
- DESK2_INTRADAY: FX 1H intraday. 5 major pairs. Best performing desk — protect this edge.
- DESK3_SWING: FX 4H swing trades. 8 diversified pairs. Hold 1-5 days.
- DESK4_GOLD: 3 analysts on XAUUSD — scalper (5M), intraday (15M/1H), position (4H). DXY and VIX context matters.
- DESK5_ALTS: NAS100 + BTC + ETH + SOL. VIX regime is critical — HALT if VIX > 30.
- DESK6_EQUITIES: NVDA, AAPL, TSLA, MSFT, AMZN, META. Close at session end.

YOUR MANDATE:
DEFAULT BIAS = EXECUTE. All signals reaching you have ALREADY been pre-filtered by LuxAlgo ML Classifier (level 3-4 trend-continuation only) and Smart Trail/Trend Catcher overlay filters. These are high-quality signals. Your job is to let them through unless there's a concrete reason not to.

DECISION OPTIONS:
- EXECUTE: Approve at recommended size (THIS IS YOUR DEFAULT)
- REDUCE: Approve at 25-75% size (specify multiplier)
- SKIP: Reject — ONLY for hard red flags listed below

DESK-SPECIFIC RULES:
- Scalper desks: Speed matters. Auto-EXECUTE if consensus >= 3 AND in kill zone.
- Intraday: Standard rules. REDUCE if RSI > 70 (long) or < 30 (short).
- Swing: Require Daily EMA alignment. SKIP if higher TF clearly conflicts.
- Gold: Check DXY + VIX. If multiple gold analysts agree on direction = extra confidence.
- Momentum (DESK5): VIX > 30 = auto-SKIP. VIX 25-30 = REDUCE 40%.
- Equities (DESK6): VIX > 30 = auto-SKIP.

RESPONSE FORMAT (ONLY valid JSON, no markdown):
{
    "decision": "EXECUTE" | "REDUCE" | "SKIP",
    "size_multiplier": 1.0,
    "reasoning": "2-3 sentence institutional reasoning",
    "risk_flags": ["flag1", "flag2"],
    "confidence": 0.0-1.0,
    "notes_for_log": "Brief note for the trade log"
}

HARD RULES (non-negotiable):
- Consensus score < 2: ALWAYS SKIP
- Desk paused or closed: ALWAYS SKIP
- Daily loss > $4,000: ALWAYS SKIP
- Consecutive losses >= 4: ALWAYS SKIP
- No stop loss: ALWAYS SKIP
- Everything else: EXECUTE or REDUCE, never SKIP
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

        # Max trades for the day
        if desk_state.get("max_trades_hit"):
            reasons.append("Maximum daily trades reached for this desk")

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
