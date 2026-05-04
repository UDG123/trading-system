"""Cross-desk bias alignment for FX desks.

DESK1/2/3 may trade the same FX symbols, but they are different engines:
- DESK3_SWING defines higher-timeframe structural bias.
- DESK2_INTRADAY should prefer or require alignment with swing bias.
- DESK1_SCALPER can scalp counter-bias moves, but with reduced confidence.

Environment controls:
- ENABLE_CROSS_DESK_BIAS=true  -> enable/disable all bias adjustments.
- ENABLE_HARD_BIAS_FILTER=false -> if true, DESK2 counter-bias signals are blocked.

This remains paper-signal logic only. It never places broker orders.
"""
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Dict, Optional


def _env_bool(name: str, default: str = "false") -> bool:
    return os.getenv(name, default).lower() in {"1", "true", "yes", "on"}


@dataclass(slots=True)
class BiasState:
    symbol: str
    direction: str  # LONG / SHORT / NEUTRAL
    source_desk: str
    confidence: float
    updated_at: float
    ttl_seconds: int = 6 * 60 * 60

    @property
    def expired(self) -> bool:
        return (time.time() - self.updated_at) > self.ttl_seconds


class CrossDeskBiasEngine:
    """Stores and applies higher-timeframe FX bias."""

    def __init__(self):
        self._bias: Dict[str, BiasState] = {}

    @property
    def enabled(self) -> bool:
        return _env_bool("ENABLE_CROSS_DESK_BIAS", "true")

    @property
    def hard_filter_enabled(self) -> bool:
        return _env_bool("ENABLE_HARD_BIAS_FILTER", "false")

    def update_from_candidate(self, candidate: Dict) -> None:
        if not self.enabled:
            return

        symbol = str(candidate.get("symbol", "")).upper()
        desk_id = candidate.get("desk_id")
        direction = candidate.get("direction")
        confidence = float(candidate.get("confidence", 0.0) or 0.0)

        if not symbol or desk_id != "DESK3_SWING" or direction not in {"LONG", "SHORT"}:
            return
        if confidence < 0.55:
            return

        self._bias[symbol] = BiasState(
            symbol=symbol,
            direction=direction,
            source_desk=desk_id,
            confidence=confidence,
            updated_at=time.time(),
        )

    def get_bias(self, symbol: str) -> Optional[BiasState]:
        state = self._bias.get(symbol.upper())
        if not state:
            return None
        if state.expired:
            self._bias.pop(symbol.upper(), None)
            return None
        return state

    def apply(self, candidate: Dict) -> Dict:
        if not self.enabled:
            candidate["bias_action"] = "DISABLED"
            return candidate

        symbol = str(candidate.get("symbol", "")).upper()
        desk_id = candidate.get("desk_id")
        direction = candidate.get("direction")
        if desk_id not in {"DESK1_SCALPER", "DESK2_INTRADAY"} or direction not in {"LONG", "SHORT"}:
            return candidate

        bias = self.get_bias(symbol)
        if not bias:
            candidate.setdefault("quality_hints", []).append("bias:neutral")
            candidate["cross_desk_bias"] = "NEUTRAL"
            candidate["bias_alignment"] = "NEUTRAL"
            candidate["bias_action"] = "NO_BIAS"
            return candidate

        candidate["cross_desk_bias"] = bias.direction
        candidate["bias_source_desk"] = bias.source_desk

        aligned = direction == bias.direction
        candidate["bias_alignment"] = "ALIGNED" if aligned else "COUNTER"
        candidate.setdefault("quality_hints", []).append("bias:aligned" if aligned else "bias:counter")

        base_conf = float(candidate.get("confidence", 0.5) or 0.5)

        if aligned:
            candidate["confidence"] = min(1.0, base_conf * 1.10)
            candidate["bias_action"] = "BOOSTED"
            candidate["bias_size_mult"] = 1.10
            candidate["mode_reason"] = f"{candidate.get('mode_reason', '')}; aligned with DESK3 {bias.direction} bias".strip("; ")
            return candidate

        # Hard filter: DESK2 should not fight DESK3 when explicitly enabled.
        if desk_id == "DESK2_INTRADAY" and self.hard_filter_enabled:
            candidate["blocked_by_bias"] = True
            candidate["bias_action"] = "BLOCKED"
            candidate["bias_size_mult"] = 0.0
            candidate["confidence"] = 0.0
            candidate["mode_reason"] = f"{candidate.get('mode_reason', '')}; blocked: DESK2 counter to DESK3 {bias.direction} bias".strip("; ")
            return candidate

        # Soft counter-bias handling.
        penalty = 0.70 if desk_id == "DESK2_INTRADAY" else 0.85
        candidate["confidence"] = max(0.10, base_conf * penalty)
        candidate["bias_action"] = "REDUCED"
        candidate["bias_size_mult"] = penalty
        candidate["mode_reason"] = f"{candidate.get('mode_reason', '')}; counter to DESK3 {bias.direction} bias".strip("; ")
        return candidate


GLOBAL_CROSS_DESK_BIAS = CrossDeskBiasEngine()
