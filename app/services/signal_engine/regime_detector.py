"""
3-State Hidden Markov Model Regime Detection

Classifies each symbol into TRENDING_UP, TRENDING_DOWN, or RANGING using
1H OHLCV data. Features: log returns, Garman-Klass volatility, 20-bar momentum.

Regimes adjust signal parameters:
  TRENDING_UP:   favor longs, 1.2x size, momentum signals
  TRENDING_DOWN: favor shorts, 1.2x size, momentum signals
  RANGING:       mean-reversion, 0.7x size, tighter stops (2.0x ATR)

Retrained every 4 hours. Cached in Redis per symbol with 4h TTL.
"""
import logging
import pickle
from datetime import datetime, timezone
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger("TradingSystem.SignalEngine.Regime")

REDIS_PREFIX = "regime:"
REDIS_TTL = 14400  # 4 hours
MIN_BARS = 252  # ~2 weeks of 1H data
N_STATES = 3

# Regime adjustment parameters
REGIME_PARAMS = {
    "TRENDING_UP": {
        "favor_direction": "LONG",
        "size_multiplier": 1.2,
        "sl_atr_mult": 2.5,     # standard
        "signal_type": "momentum",
    },
    "TRENDING_DOWN": {
        "favor_direction": "SHORT",
        "size_multiplier": 1.2,
        "sl_atr_mult": 2.5,
        "signal_type": "momentum",
    },
    "RANGING": {
        "favor_direction": None,
        "size_multiplier": 0.7,
        "sl_atr_mult": 2.0,     # tighter stops
        "signal_type": "mean_reversion",
    },
}


def garman_klass_vol(
    high: np.ndarray, low: np.ndarray,
    close: np.ndarray, open_: np.ndarray,
) -> np.ndarray:
    """
    Garman-Klass volatility estimator per bar.
    GK = 0.5 * ln(H/L)^2 - (2*ln(2) - 1) * ln(C/O)^2
    """
    log_hl = np.log(high / low)
    log_co = np.log(close / open_)
    return 0.5 * log_hl ** 2 - (2 * np.log(2) - 1) * log_co ** 2


class HMMRegimeDetector:
    """Per-symbol 3-state Gaussian HMM regime classifier."""

    def __init__(self, redis_pool=None):
        self.redis = redis_pool
        self._prev_regimes: Dict[str, str] = {}  # for transition logging

    async def get_regime(self, symbol: str, db: Session = None) -> Dict:
        """
        Get current regime for a symbol. Tries Redis cache first,
        falls back to fitting on the spot if cache miss.
        """
        # Check Redis cache
        if self.redis:
            try:
                cached = await self.redis.get(f"{REDIS_PREFIX}{symbol}")
                if cached:
                    return pickle.loads(cached)
            except Exception:
                pass

        # Fit on the spot from DB
        if db:
            result = self._fit_and_predict(symbol, db)
            if result["regime"] != "UNKNOWN":
                await self._cache_regime(symbol, result)
            return result

        return self._fallback()

    def get_regime_sync(self, symbol: str, db: Session) -> Dict:
        """Synchronous version for use in pipeline (non-async context)."""
        return self._fit_and_predict(symbol, db)

    def _fit_and_predict(self, symbol: str, db: Session) -> Dict:
        """Fetch 1H data, compute features, fit HMM, predict current state."""
        try:
            rows = db.execute(
                text("""
                    SELECT open, high, low, close FROM ohlcv_1h
                    WHERE symbol = :sym
                    ORDER BY time DESC
                    LIMIT :n
                """),
                {"sym": symbol, "n": MIN_BARS + 25},
            ).fetchall()

            if not rows or len(rows) < MIN_BARS:
                return self._fallback()

            # Reverse to chronological order
            open_ = np.array([float(r[0]) for r in reversed(rows)])
            high = np.array([float(r[1]) for r in reversed(rows)])
            low = np.array([float(r[2]) for r in reversed(rows)])
            close = np.array([float(r[3]) for r in reversed(rows)])

            # Guard against zero/negative values
            if np.any(open_ <= 0) or np.any(close <= 0) or np.any(high <= 0) or np.any(low <= 0):
                return self._fallback()

            # Feature 1: log returns
            log_returns = np.diff(np.log(close))

            # Feature 2: Garman-Klass volatility (per bar, skip first)
            gk_vol = garman_klass_vol(high[1:], low[1:], close[1:], open_[1:])

            # Feature 3: Rolling 20-bar momentum (mean of returns)
            n = len(log_returns)
            momentum = np.array([
                np.mean(log_returns[max(0, i - 19):i + 1])
                for i in range(n)
            ])

            # Stack features
            features = np.column_stack([log_returns, gk_vol, momentum])

            # Remove any NaN/inf rows
            valid_mask = np.all(np.isfinite(features), axis=1)
            features = features[valid_mask]

            if len(features) < MIN_BARS - 30:
                return self._fallback()

            # Fit HMM
            from hmmlearn.hmm import GaussianHMM

            model = GaussianHMM(
                n_components=N_STATES,
                covariance_type="full",
                n_iter=200,
                random_state=42,
                tol=0.01,
            )
            model.fit(features)

            # Predict states
            states = model.predict(features)
            current_state = int(states[-1])

            # Probabilities for current observation
            probs = model.predict_proba(features)[-1]

            # Map states by sorting on mean return (feature index 0)
            state_means = [model.means_[i][0] for i in range(N_STATES)]
            sorted_states = np.argsort(state_means)
            # sorted_states[0] = lowest mean return → TRENDING_DOWN
            # sorted_states[1] = middle → RANGING
            # sorted_states[2] = highest mean return → TRENDING_UP
            label_map = {
                int(sorted_states[0]): "TRENDING_DOWN",
                int(sorted_states[1]): "RANGING",
                int(sorted_states[2]): "TRENDING_UP",
            }

            regime = label_map.get(current_state, "RANGING")
            confidence = float(probs[current_state])

            # State probabilities in labeled form
            state_probs = {}
            for state_idx, label in label_map.items():
                state_probs[label] = round(float(probs[state_idx]), 4)

            # Transition risk: P(leaving current state)
            transition_risk = 1.0 - float(model.transmat_[current_state][current_state])

            # Log regime transitions
            prev = self._prev_regimes.get(symbol)
            if prev and prev != regime:
                logger.info(
                    f"REGIME CHANGE | {symbol} | {prev} → {regime} | "
                    f"conf={confidence:.2f} | trans_risk={transition_risk:.2f}"
                )
            self._prev_regimes[symbol] = regime

            # Get adjustment params
            params = REGIME_PARAMS.get(regime, REGIME_PARAMS["RANGING"])

            return {
                "regime": regime,
                "confidence": round(confidence, 4),
                "state_probabilities": state_probs,
                "transition_risk": round(transition_risk, 4),
                "favor_direction": params["favor_direction"],
                "size_multiplier": params["size_multiplier"],
                "sl_atr_mult": params["sl_atr_mult"],
                "signal_type": params["signal_type"],
                "n_bars_used": len(features),
            }

        except Exception as e:
            logger.debug(f"HMM fit failed for {symbol}: {e}")
            return self._fallback()

    async def train_all_symbols(self, db: Session) -> Dict:
        """Retrain HMM for all symbols and cache in Redis."""
        from app.config import DESKS
        all_symbols = set()
        for desk in DESKS.values():
            all_symbols.update(desk.get("symbols", []))

        trained = 0
        failed = 0
        for symbol in sorted(all_symbols):
            result = self._fit_and_predict(symbol, db)
            if result["regime"] != "UNKNOWN":
                trained += 1
                await self._cache_regime(symbol, result)
            else:
                failed += 1

        logger.info(
            f"HMM regime training: {trained} trained, {failed} failed "
            f"(out of {len(all_symbols)} symbols)"
        )
        return {"trained": trained, "failed": failed, "total": len(all_symbols)}

    async def _cache_regime(self, symbol: str, result: Dict) -> None:
        """Cache regime result in Redis."""
        if not self.redis:
            return
        try:
            await self.redis.set(
                f"{REDIS_PREFIX}{symbol}",
                pickle.dumps(result),
                ex=REDIS_TTL,
            )
        except Exception:
            pass

    @staticmethod
    def _fallback() -> Dict:
        return {
            "regime": "UNKNOWN",
            "confidence": 0.0,
            "state_probabilities": {
                "TRENDING_UP": 0.33, "TRENDING_DOWN": 0.33, "RANGING": 0.34,
            },
            "transition_risk": 0.5,
            "favor_direction": None,
            "size_multiplier": 1.0,
            "sl_atr_mult": 2.5,
            "signal_type": "momentum",
            "n_bars_used": 0,
        }
