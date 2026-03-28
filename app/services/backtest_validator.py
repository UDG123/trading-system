"""
Backtest Validator — CSCV and Deflated Sharpe Ratio for overfitting detection.
"""
import logging
from typing import Dict, Optional

import numpy as np
from scipy import stats
from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger("TradingSystem.BacktestValidator")


class BacktestValidator:
    """Statistical validation that shadow sim results are not overfit."""

    def compute_deflated_sharpe(
        self,
        sharpe_observed: float,
        n_trials: int,
        T: int,
        skew: float = 0,
        kurtosis: float = 3,
    ) -> Dict:
        """
        Deflated Sharpe Ratio — corrects for selection bias.
        DSR p-value < 0.05 means the Sharpe is statistically significant
        even after accounting for multiple strategy tests.
        """
        if T < 2 or n_trials < 1:
            return {"dsr_p_value": 1.0, "is_significant": False}

        # Expected max Sharpe under null hypothesis
        euler_mascheroni = 0.5772156649
        sr_star = np.sqrt(2 * np.log(n_trials)) - (
            (np.log(np.pi) + euler_mascheroni) / (2 * np.sqrt(2 * np.log(n_trials)))
        ) if n_trials > 1 else 0

        # DSR statistic
        denominator = np.sqrt(
            1 - skew * sharpe_observed + (kurtosis - 1) / 4 * sharpe_observed ** 2
        )
        if denominator <= 0:
            denominator = 1.0

        dsr_stat = (sharpe_observed - sr_star) * np.sqrt(T - 1) / denominator
        dsr_p_value = 1 - stats.norm.cdf(dsr_stat)

        return {
            "sharpe_ratio": round(sharpe_observed, 4),
            "deflated_sharpe": round(float(dsr_stat), 4),
            "dsr_p_value": round(float(dsr_p_value), 4),
            "is_significant": dsr_p_value < 0.05,
            "sr_star": round(float(sr_star), 4),
            "n_trials_tested": n_trials,
            "sample_size": T,
        }

    def compute_cscv(self, returns_matrix: np.ndarray, n_splits: int = 16) -> Dict:
        """
        Combinatorially Symmetric Cross-Validation.
        PBO = fraction where in-sample best underperforms out-of-sample median.
        """
        n_obs, n_strategies = returns_matrix.shape
        if n_obs < n_splits * 2 or n_strategies < 2:
            return {"pbo_score": 0.5, "recommendation": "INSUFFICIENT_DATA"}

        split_size = n_obs // n_splits
        splits = [
            returns_matrix[i * split_size:(i + 1) * split_size]
            for i in range(n_splits)
        ]

        overfit_count = 0
        total_combos = 0

        # Simplified: use random combinations instead of full C(S, S/2)
        rng = np.random.RandomState(42)
        n_combos = min(100, 2 ** n_splits)  # cap at 100

        for _ in range(n_combos):
            # Random split into IS and OOS halves
            perm = rng.permutation(n_splits)
            half = n_splits // 2
            is_indices = perm[:half]
            oos_indices = perm[half:]

            is_data = np.vstack([splits[i] for i in is_indices])
            oos_data = np.vstack([splits[i] for i in oos_indices])

            # Best strategy in-sample
            is_sharpe = np.mean(is_data, axis=0) / (np.std(is_data, axis=0) + 1e-10)
            best_is = np.argmax(is_sharpe)

            # Check OOS performance
            oos_sharpe = np.mean(oos_data, axis=0) / (np.std(oos_data, axis=0) + 1e-10)
            oos_median = np.median(oos_sharpe)

            if oos_sharpe[best_is] < oos_median:
                overfit_count += 1
            total_combos += 1

        pbo = overfit_count / total_combos if total_combos > 0 else 0.5

        if pbo < 0.3:
            recommendation = "VALIDATED"
        elif pbo < 0.5:
            recommendation = "SUSPECT"
        else:
            recommendation = "OVERFIT"

        return {
            "pbo_score": round(pbo, 4),
            "n_combos_tested": total_combos,
            "recommendation": recommendation,
        }

    def validate_sim_profile(self, profile_name: str, db: Session) -> Dict:
        """Full validation: Sharpe + DSR on a sim profile's trades."""
        try:
            rows = db.execute(text("""
                SELECT pnl_pips FROM sim_positions
                WHERE profile = :profile AND status = 'CLOSED' AND pnl_pips IS NOT NULL
                ORDER BY closed_at
            """), {"profile": profile_name}).fetchall()

            if not rows or len(rows) < 30:
                return {"status": "insufficient_trades", "n_trades": len(rows) if rows else 0}

            returns = np.array([float(r[0]) for r in rows])
            sharpe = float(np.mean(returns) / np.std(returns) * np.sqrt(252)) if np.std(returns) > 0 else 0

            dsr = self.compute_deflated_sharpe(
                sharpe_observed=sharpe,
                n_trials=12,  # approximate number of parameter configs tested
                T=len(returns),
                skew=float(stats.skew(returns)),
                kurtosis=float(stats.kurtosis(returns, fisher=False)),
            )

            dsr["profile"] = profile_name
            dsr["n_trades"] = len(returns)
            dsr["recommendation"] = "VALIDATED" if dsr["is_significant"] else "SUSPECT"

            return dsr

        except Exception as e:
            logger.debug(f"Validation failed for {profile_name}: {e}")
            return {"status": "error", "message": str(e)}
