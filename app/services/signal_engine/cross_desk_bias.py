"""Cross-desk bias alignment for FX desks.

DESK1/2/3 may trade the same FX symbols, but they are different engines:
- DESK3_SWING defines higher-timeframe structural bias.
- DESK2_INTRADAY should prefer signals aligned with swing bias.
- DESK1_SCALPER can still scalp, but aligned scalps receive a confidence boost
  while counter-bias scalps are reduced.

This module is intentionally lightweight and in-memory. It is safe for paper
signal generation and does not introduce broker execution.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Dict, Optional


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

    def update_from_candidate(self, candidate: Dict) -> None:
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
        symbol = str(candidate.get("symbol", "")).upper()
        desk_id = candidate.get("desk_id")
        direction = candidate.get("direction")
        if desk_id not in {"DESK1_SCALPER", "DESK2_INTRADAY"} or direction not in {"LONG", "SHORT"}:
            return candidate

        bias = self.get_bias(symbol)
        if not bias:
            candidate.setdefault("quality_hints", []).append("bias:neutral")
            candidate["cross_desk_bias"] = "NEUTRAL"
            return candidate

        candidate["cross_desk_bias"] = bias.direction
        candidate["bias_source_desk"] = bias.source_desk

        aligned = direction == bias.direction
        candidate.setdefault("quality_hints", []).append("bias:aligned" if aligned else "bias:counter")

        base_conf = float(candidate.get("confidence", 0.5) or 0.5)
        if aligned:
            candidate["confidence"] = min(1.0, base_conf * 1.10)
            candidate["mode_reason"] = f"{candidate.get('mode_reason', '')}; aligned with DESK3 {bias.direction} bias".strip("; ")
        else:
            # Intraday is stricter than scalper. Scalper can still take fast fades,
            # but confidence is reduced so pipeline sizing gets smaller.
            penalty = 0.70 if desk_id == "DESK2_INTRADAY" else 0.85
            candidate["confidence"] = max(0.10, base_conf * penalty)
            candidate["mode_reason"] = f"{candidate.get('mode_reason', '')}; counter to DESK3 {bias.direction} bias".strip("; ")

        return candidate


GLOBAL_CROSS_DESK_BIAS = CrossDeskBiasEngine()
