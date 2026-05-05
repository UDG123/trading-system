"""Cross-desk signal quality engine for OniQuant.

Adds four non-execution enhancements:
1. Cross-desk bias scoring
2. Trade clustering / signal stacking
3. Probability-weighted signal score
4. Lightweight ML-style enhancement layer

This module is deterministic and safe for paper-trading signal generation. It does
not execute trades and does not require trained model artifacts to work.
"""
from __future__ import annotations

import math
import os
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from typing import Any, Deque, Dict, List, Tuple


def _bool_env(name: str, default: bool = True) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except Exception:
        return default


def _int_env(name: str, default: int) -> int:
    try:
        return int(float(os.getenv(name, str(default))))
    except Exception:
        return default


@dataclass
class SignalQualityConfig:
    enable_cross_desk_bias: bool = field(default_factory=lambda: _bool_env("ENABLE_CROSS_DESK_BIAS", True))
    enable_hard_bias_filter: bool = field(default_factory=lambda: _bool_env("ENABLE_HARD_BIAS_FILTER", False))
    enable_signal_clustering: bool = field(default_factory=lambda: _bool_env("ENABLE_SIGNAL_CLUSTERING", True))
    enable_probability_weighting: bool = field(default_factory=lambda: _bool_env("ENABLE_PROBABILITY_WEIGHTING", True))
    enable_ml_enhancement: bool = field(default_factory=lambda: _bool_env("ENABLE_ML_ENHANCEMENT", True))
    cluster_window_seconds: int = field(default_factory=lambda: _int_env("SIGNAL_CLUSTER_WINDOW_SECONDS", 900))
    min_probability_to_emit: float = field(default_factory=lambda: _float_env("MIN_SIGNAL_PROBABILITY", 0.52))
    min_quality_to_emit: float = field(default_factory=lambda: _float_env("MIN_FINAL_SIGNAL_QUALITY", 50.0))
    signal_debug_mode: bool = field(default_factory=lambda: _bool_env("SIGNAL_DEBUG_MODE", False))


class SignalQualityEngine:
    """In-memory signal quality enhancer used before Redis emission."""

    def __init__(self, config: SignalQualityConfig | None = None):
        self.config = config or SignalQualityConfig()
        self._recent: Deque[Tuple[float, Dict[str, Any]]] = deque(maxlen=500)

    def enhance(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        now = time.time()
        self._prune(now)
        enriched = dict(signal)

        bias = self._cross_desk_bias(enriched)
        cluster = self._cluster_features(enriched)
        ml = self._ml_features(enriched, bias, cluster)
        probability = self._probability_score(enriched, bias, cluster, ml)
        final_quality = self._final_quality(enriched, bias, cluster, ml, probability)

        enriched.setdefault("metadata", {})
        enriched["cross_desk_bias"] = bias
        enriched["signal_cluster"] = cluster
        enriched["ml_enhancement"] = ml
        enriched["signal_probability"] = round(probability, 4)
        enriched["final_signal_quality"] = round(final_quality, 2)
        enriched["quality_layer_version"] = "v1"

        blocked = False
        block_reasons: List[str] = []
        soft_block_reasons: List[str] = []
        very_poor_quality = final_quality < max(20.0, self.config.min_quality_to_emit * 0.5)
        very_low_probability = probability < max(0.25, self.config.min_probability_to_emit * 0.6)

        if self.config.enable_probability_weighting and probability < self.config.min_probability_to_emit:
            if self.config.signal_debug_mode and not very_low_probability:
                soft_block_reasons.append("probability_below_min")
            else:
                blocked = True
                block_reasons.append("probability_below_min")
        if final_quality < self.config.min_quality_to_emit:
            if self.config.signal_debug_mode and not very_poor_quality:
                soft_block_reasons.append("final_quality_below_min")
            else:
                blocked = True
                block_reasons.append("final_quality_below_min")
        if self.config.enable_hard_bias_filter and bias.get("bias_direction") not in ("NEUTRAL", enriched.get("direction")):
            if self.config.signal_debug_mode:
                soft_block_reasons.append("hard_cross_desk_bias_conflict")
            else:
                blocked = True
                block_reasons.append("hard_cross_desk_bias_conflict")

        enriched["quality_blocked"] = blocked
        enriched["quality_block_reasons"] = block_reasons
        enriched["quality_would_block_reasons"] = soft_block_reasons

        self._recent.append((now, enriched))
        return enriched

    def _prune(self, now: float) -> None:
        cutoff = now - self.config.cluster_window_seconds
        while self._recent and self._recent[0][0] < cutoff:
            self._recent.popleft()

    def _cross_desk_bias(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        if not self.config.enable_cross_desk_bias:
            return {"enabled": False, "bias_direction": "NEUTRAL", "bias_score": 0.0}

        symbol = str(signal.get("symbol_normalized") or signal.get("symbol") or "")
        direction = str(signal.get("direction") or "").upper()
        related = []
        for _, prior in self._recent:
            psym = str(prior.get("symbol_normalized") or prior.get("symbol") or "")
            if not self._same_family(symbol, psym):
                continue
            related.append(prior)

        buy_weight = 0.0
        sell_weight = 0.0
        for prior in related:
            weight = float(prior.get("final_signal_quality") or prior.get("confluence_score") or prior.get("quality_score") or 50.0) / 100.0
            if str(prior.get("direction", "")).upper() == "BUY":
                buy_weight += weight
            elif str(prior.get("direction", "")).upper() == "SELL":
                sell_weight += weight

        if buy_weight > sell_weight + 0.35:
            bias_direction = "BUY"
        elif sell_weight > buy_weight + 0.35:
            bias_direction = "SELL"
        else:
            bias_direction = "NEUTRAL"

        alignment = 0.0
        if bias_direction == direction:
            alignment = 1.0
        elif bias_direction == "NEUTRAL":
            alignment = 0.0
        else:
            alignment = -1.0

        return {
            "enabled": True,
            "bias_direction": bias_direction,
            "bias_score": round(buy_weight - sell_weight, 4),
            "alignment": alignment,
            "related_signal_count": len(related),
        }

    @staticmethod
    def _same_family(a: str, b: str) -> bool:
        if not a or not b:
            return False
        if a == b:
            return True
        fx_roots = ["USD", "EUR", "GBP", "JPY", "AUD", "CAD", "CHF", "NZD"]
        if len(a) == 6 and len(b) == 6 and any(root in a and root in b for root in fx_roots):
            return True
        if a in {"XAUUSD", "XAGUSD"} and b in {"XAUUSD", "XAGUSD"}:
            return True
        crypto = {"BTCUSD", "ETHUSD", "SOLUSD", "XRPUSD", "LINKUSD"}
        if a in crypto and b in crypto:
            return True
        return False

    def _cluster_features(self, signal: Dict[str, Any]) -> Dict[str, Any]:
        if not self.config.enable_signal_clustering:
            return {"enabled": False, "stack_count": 0, "same_direction_count": 0, "opposite_direction_count": 0}

        symbol = str(signal.get("symbol_normalized") or signal.get("symbol") or "")
        direction = str(signal.get("direction") or "").upper()
        same = 0
        opposite = 0
        desks = set()
        strategies = set()
        for _, prior in self._recent:
            psym = str(prior.get("symbol_normalized") or prior.get("symbol") or "")
            if psym != symbol:
                continue
            pdir = str(prior.get("direction") or "").upper()
            if pdir == direction:
                same += 1
            elif pdir:
                opposite += 1
            if prior.get("desk_id"):
                desks.add(str(prior.get("desk_id")))
            if prior.get("strategy_id"):
                strategies.add(str(prior.get("strategy_id")))

        stack_count = same + 1
        return {
            "enabled": True,
            "stack_count": stack_count,
            "same_direction_count": same,
            "opposite_direction_count": opposite,
            "unique_desks": sorted(desks),
            "unique_strategies": sorted(strategies),
            "cluster_bonus": min(10.0, same * 3.0 + len(desks) * 1.5),
            "conflict_penalty": min(12.0, opposite * 4.0),
        }

    def _ml_features(self, signal: Dict[str, Any], bias: Dict[str, Any], cluster: Dict[str, Any]) -> Dict[str, Any]:
        if not self.config.enable_ml_enhancement:
            return {"enabled": False, "ml_score": 0.0, "model": "disabled"}

        confluence = float(signal.get("confluence_score") or signal.get("quality_score") or 50.0)
        regime = str(signal.get("regime") or signal.get("market_regime") or "UNKNOWN").upper()
        vol = str(signal.get("volatility_state") or "UNKNOWN").upper()
        rr = self._risk_reward(signal)

        score = 0.0
        score += (confluence - 50.0) * 0.45
        score += min(12.0, max(0.0, (rr - 1.0) * 8.0))
        score += float(cluster.get("cluster_bonus", 0.0))
        score -= float(cluster.get("conflict_penalty", 0.0))
        score += 6.0 * float(bias.get("alignment", 0.0))

        if regime in {"TREND_UP", "TREND_DOWN", "TRENDING"}:
            score += 4.0
        elif regime in {"MIXED", "UNKNOWN"}:
            score -= 3.0
        if vol == "HIGH_VOL":
            score -= 2.5
        elif vol == "NORMAL_VOL":
            score += 2.0

        return {
            "enabled": True,
            "model": "heuristic_v1_no_training_artifact",
            "ml_score": round(max(-25.0, min(25.0, score)), 4),
            "risk_reward": round(rr, 4),
            "features": {
                "confluence": confluence,
                "regime": regime,
                "volatility_state": vol,
                "bias_alignment": bias.get("alignment", 0.0),
                "stack_count": cluster.get("stack_count", 0),
            },
        }

    @staticmethod
    def _risk_reward(signal: Dict[str, Any]) -> float:
        try:
            entry = float(signal.get("entry") or signal.get("entry_price"))
            sl = float(signal.get("stop_loss") or signal.get("sl"))
            tp = float(signal.get("take_profit") or signal.get("tp"))
            risk = abs(entry - sl)
            reward = abs(tp - entry)
            return reward / risk if risk > 0 else 1.0
        except Exception:
            return 1.0

    @staticmethod
    def _probability_score(signal: Dict[str, Any], bias: Dict[str, Any], cluster: Dict[str, Any], ml: Dict[str, Any]) -> float:
        base_quality = float(signal.get("confluence_score") or signal.get("quality_score") or 50.0)
        x = -0.25 + ((base_quality - 50.0) / 18.0)
        x += float(bias.get("alignment", 0.0)) * 0.22
        x += float(cluster.get("cluster_bonus", 0.0)) / 45.0
        x -= float(cluster.get("conflict_penalty", 0.0)) / 35.0
        x += float(ml.get("ml_score", 0.0)) / 40.0
        return 1.0 / (1.0 + math.exp(-x))

    @staticmethod
    def _final_quality(signal: Dict[str, Any], bias: Dict[str, Any], cluster: Dict[str, Any], ml: Dict[str, Any], probability: float) -> float:
        base = float(signal.get("quality_score") or signal.get("confluence_score") or 50.0)
        base += float(cluster.get("cluster_bonus", 0.0))
        base -= float(cluster.get("conflict_penalty", 0.0))
        base += float(bias.get("alignment", 0.0)) * 6.0
        base += float(ml.get("ml_score", 0.0)) * 0.6
        base += (probability - 0.5) * 20.0
        return max(0.0, min(100.0, base))
