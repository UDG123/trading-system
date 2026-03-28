"""
HMM Regime Detector — classifies market states into 3 regimes:
TRENDING, MEAN_REVERTING, and VOLATILE using a Gaussian Hidden Markov Model.
"""
import logging
import pickle
from typing import Dict, Optional

import numpy as np
from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger("TradingSystem.RegimeDetector")

REDIS_KEY_PREFIX = "hmm_model:"
REDIS_TTL = 86400  # 24 hours
REGIME_LABELS = {0: "MEAN_REVERTING", 1: "TRENDING", 2: "VOLATILE"}
MIN_OBSERVATIONS = 100


class RegimeDetector:
    """3-state Gaussian HMM for market regime detection."""

    def __init__(self, redis_pool=None):
        self.redis = redis_pool

    def fit(self, returns: np.ndarray, symbol: str = "ALL") -> Dict:
        """Train 3-state Gaussian HMM on feature matrix."""
        if len(returns) < MIN_OBSERVATIONS:
            return {"status": "insufficient_data", "n_obs": len(returns)}

        try:
            from hmmlearn.hmm import GaussianHMM

            # Feature vector: [log_return, realized_vol_20, abs_return]
            vol_20 = np.array([
                np.std(returns[max(0, i - 20):i + 1]) if i >= 1 else 0
                for i in range(len(returns))
            ])
            features = np.column_stack([returns, vol_20, np.abs(returns)])

            model = GaussianHMM(
                n_components=3,
                covariance_type="full",
                n_iter=200,
                random_state=42,
            )
            model.fit(features)

            # Label states: sort by variance
            state_variances = [model.covars_[i][0][0] for i in range(3)]
            sorted_indices = np.argsort(state_variances)
            # lowest variance = MEAN_REVERTING, mid = TRENDING, highest = VOLATILE
            label_map = {
                int(sorted_indices[0]): "MEAN_REVERTING",
                int(sorted_indices[1]): "TRENDING",
                int(sorted_indices[2]): "VOLATILE",
            }

            return {
                "status": "trained",
                "n_obs": len(returns),
                "model": model,
                "label_map": label_map,
                "features": features,
            }

        except Exception as e:
            logger.debug(f"HMM fit failed: {e}")
            return {"status": "error", "message": str(e)}

    def predict(self, model, features: np.ndarray, label_map: Dict) -> Dict:
        """Predict current regime with probabilities."""
        try:
            state_seq = model.predict(features)
            current_state = int(state_seq[-1])
            probs = model.predict_proba(features)[-1]

            regime = label_map.get(current_state, "UNKNOWN")
            prob_map = {}
            for state_idx, label in label_map.items():
                prob_map[label] = round(float(probs[state_idx]), 4)

            confidence = float(probs[current_state])

            # Transition risk: probability of switching state in next period
            trans = model.transmat_[current_state]
            transition_risk = 1.0 - float(trans[current_state])

            return {
                "regime": regime,
                "confidence": round(confidence, 4),
                "state_probabilities": prob_map,
                "transition_risk": round(transition_risk, 4),
            }

        except Exception as e:
            logger.debug(f"HMM predict failed: {e}")
            return self._fallback()

    def get_regime_for_symbol(self, symbol: str, db: Session) -> Dict:
        """Fetch OHLCV data, compute features, and return current regime."""
        try:
            rows = db.execute(
                text("""
                    SELECT close FROM ohlcv_1d
                    WHERE symbol = :sym
                    ORDER BY time DESC
                    LIMIT 600
                """),
                {"sym": symbol},
            ).fetchall()

            if not rows or len(rows) < MIN_OBSERVATIONS:
                return self._fallback()

            closes = np.array([float(r[0]) for r in reversed(rows)])
            returns = np.diff(np.log(closes))

            result = self.fit(returns, symbol)
            if result["status"] != "trained":
                return self._fallback()

            prediction = self.predict(result["model"], result["features"], result["label_map"])

            # Add Hurst for backwards compat
            prediction["hurst"] = self._compute_hurst(returns)

            return prediction

        except Exception as e:
            logger.debug(f"Regime detection failed for {symbol}: {e}")
            return self._fallback()

    async def train_all(self, db: Session) -> Dict:
        """Train HMM for all symbols. Cache models in Redis."""
        from app.config import DESKS
        all_symbols = set()
        for desk in DESKS.values():
            all_symbols.update(desk.get("symbols", []))

        trained = 0
        for symbol in sorted(all_symbols):
            try:
                regime = self.get_regime_for_symbol(symbol, db)
                if regime["regime"] != "UNKNOWN":
                    trained += 1
                    if self.redis:
                        cache_key = f"{REDIS_KEY_PREFIX}{symbol}"
                        await self.redis.set(
                            cache_key, pickle.dumps(regime), ex=REDIS_TTL
                        )
            except Exception:
                continue

        logger.info(f"HMM regime training complete: {trained}/{len(all_symbols)} symbols")
        return {"trained": trained, "total": len(all_symbols)}

    @staticmethod
    def _compute_hurst(returns: np.ndarray) -> Optional[float]:
        """Simplified Hurst exponent for backwards compatibility."""
        if len(returns) < 20:
            return None
        try:
            lags = range(2, min(20, len(returns) // 2))
            tau = []
            for lag in lags:
                tau.append(np.std(np.subtract(returns[lag:], returns[:-lag])))
            if not tau or any(t <= 0 for t in tau):
                return None
            log_lags = np.log(list(lags))
            log_tau = np.log(tau)
            poly = np.polyfit(log_lags, log_tau, 1)
            return round(float(poly[0]), 4)
        except Exception:
            return None

    @staticmethod
    def _fallback() -> Dict:
        return {
            "regime": "UNKNOWN",
            "confidence": 0.0,
            "state_probabilities": {"TRENDING": 0.33, "MEAN_REVERTING": 0.33, "VOLATILE": 0.34},
            "transition_risk": 0.5,
            "hurst": None,
        }
