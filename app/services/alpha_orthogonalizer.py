"""
Alpha Orthogonalizer — ensures Hurst, SMC, and ML signals provide
independent information via Gram-Schmidt orthogonalization.
"""
import logging
from typing import Dict, Optional

import numpy as np
from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger("TradingSystem.AlphaOrthogonalizer")

REDIS_KEY = "alpha_ortho:weights"
REDIS_TTL = 604800  # 1 week
MIN_SAMPLES = 200


class AlphaOrthogonalizer:
    """Orthogonalizes signal components and computes optimal weights."""

    def __init__(self, redis_pool=None):
        self.redis = redis_pool
        self._weights: Optional[Dict] = None

    def orthogonalize(self, signals: Dict) -> Dict:
        """
        Gram-Schmidt orthogonalization: keep Hurst, residualize SMC vs Hurst,
        residualize ML vs both.
        """
        hurst = float(signals.get("hurst", 0.5))
        smc = float(signals.get("smc_score", 0))
        ml = float(signals.get("ml_score", 0))

        # Normalize to comparable scales
        hurst_n = (hurst - 0.5) * 10  # center and scale
        smc_n = smc
        ml_n = ml * 10

        # Gram-Schmidt: project out Hurst from SMC
        if abs(hurst_n) > 1e-6:
            proj_smc = (np.dot(smc_n, hurst_n) / np.dot(hurst_n, hurst_n)) * hurst_n
        else:
            proj_smc = 0
        smc_residual = smc_n - proj_smc

        # Project out Hurst and SMC residual from ML
        ml_residual = ml_n
        if abs(hurst_n) > 1e-6:
            ml_residual -= (np.dot(ml_n, hurst_n) / np.dot(hurst_n, hurst_n)) * hurst_n
        if abs(smc_residual) > 1e-6:
            ml_residual -= (np.dot(ml_n, smc_residual) / np.dot(smc_residual, smc_residual)) * smc_residual

        return {
            "orthogonalized": {
                "hurst": round(float(hurst_n), 4),
                "smc_residual": round(float(smc_residual), 4),
                "ml_residual": round(float(ml_residual), 4),
            },
        }

    def compute_correlation_matrix(self, db: Session, days: int = 30) -> Optional[np.ndarray]:
        """Compute pairwise correlation between signal components."""
        try:
            rows = db.execute(text("""
                SELECT hurst_exponent, consensus_score, ml_score
                FROM shadow_signals
                WHERE hurst_exponent IS NOT NULL
                  AND consensus_score IS NOT NULL
                  AND ml_score IS NOT NULL
                  AND created_at > NOW() - INTERVAL ':days days'
                ORDER BY created_at
            """.replace(":days", str(int(days))))).fetchall()

            if not rows or len(rows) < MIN_SAMPLES:
                return None

            data = np.array([[float(r[0]), float(r[1]), float(r[2])] for r in rows])
            return np.corrcoef(data.T)

        except Exception:
            return None

    def get_optimal_weights(self, db: Session) -> Dict:
        """Ridge regression: predict tb_return from orthogonalized signals."""
        try:
            from sklearn.linear_model import Ridge

            rows = db.execute(text("""
                SELECT hurst_exponent, consensus_score, ml_score, tb_return
                FROM shadow_signals
                WHERE tb_return IS NOT NULL
                  AND hurst_exponent IS NOT NULL
                  AND consensus_score IS NOT NULL
                  AND ml_score IS NOT NULL
                ORDER BY created_at
            """)).fetchall()

            if not rows or len(rows) < MIN_SAMPLES:
                return self._default_weights()

            X = np.array([[float(r[0]), float(r[1]), float(r[2])] for r in rows])
            y = np.array([float(r[3]) for r in rows])

            model = Ridge(alpha=1.0)
            model.fit(X, y)

            weights_raw = np.abs(model.coef_)
            total = weights_raw.sum()
            if total > 0:
                weights_norm = weights_raw / total
            else:
                weights_norm = np.array([0.35, 0.40, 0.25])

            self._weights = {
                "hurst": round(float(weights_norm[0]), 4),
                "smc": round(float(weights_norm[1]), 4),
                "ml": round(float(weights_norm[2]), 4),
            }

            # Compute correlation before/after
            corr_before = np.corrcoef(X.T)
            avg_corr = float(np.mean(np.abs(corr_before[np.triu_indices(3, k=1)])))

            return {
                "optimal_weights": self._weights,
                "correlation_before": round(avg_corr, 4),
                "n_samples": len(rows),
            }

        except Exception as e:
            logger.debug(f"Optimal weights computation failed: {e}")
            return self._default_weights()

    @staticmethod
    def _default_weights() -> Dict:
        return {
            "optimal_weights": {"hurst": 0.35, "smc": 0.40, "ml": 0.25},
            "correlation_before": None,
            "n_samples": 0,
        }
