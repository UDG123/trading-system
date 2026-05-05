"""Signal debug/tuning configuration helpers for OniQuant."""
from __future__ import annotations

import os
from dataclasses import dataclass


def _bool_env(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def _float_env(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        return default


def _int_env(name: str, default: int) -> int:
    try:
        return int(float(os.getenv(name, str(default))))
    except (TypeError, ValueError):
        return default


@dataclass(frozen=True)
class SignalDebugConfig:
    signal_debug_mode: bool
    allow_weak_test_signals: bool
    quality_score_threshold: float
    min_confluence_score: float
    min_timeframe_bars: int

    @classmethod
    def from_env(cls) -> "SignalDebugConfig":
        debug = _bool_env("SIGNAL_DEBUG_MODE", False)
        return cls(
            signal_debug_mode=debug,
            allow_weak_test_signals=_bool_env("ALLOW_WEAK_TEST_SIGNALS", False),
            quality_score_threshold=_float_env("QUALITY_SCORE_THRESHOLD", 60.0 if debug else 65.0),
            min_confluence_score=_float_env("MIN_CONFLUENCE_SCORE", 55.0 if debug else 65.0),
            min_timeframe_bars=_int_env("MIN_TIMEFRAME_BARS", 50 if debug else 100),
        )


def get_signal_debug_config() -> SignalDebugConfig:
    return SignalDebugConfig.from_env()


def build_rejection(reason: str, **metadata):
    return {"accepted": False, "rejection_reason": reason, **metadata}
