"""
Factor Exposure Monitor — monitors systematic risk concentration across all desks.
"""
import logging
from typing import Dict

import numpy as np
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.config import DESKS

logger = logging.getLogger("TradingSystem.FactorMonitor")

CONCENTRATION_THRESHOLD = 0.60  # Alert if >60% in one factor


class FactorMonitor:
    """Monitors systematic risk factor concentration across desks."""

    def compute_factor_exposures(self, db: Session) -> Dict:
        """Compute factor exposures per desk from open positions."""
        desk_exposures = {}

        for desk_id in DESKS:
            try:
                rows = db.execute(text("""
                    SELECT t.symbol, t.direction, t.risk_pct
                    FROM trades t
                    WHERE t.desk_id = :desk AND t.status IN ('OPEN', 'SIM_OPEN')
                """), {"desk": desk_id}).fetchall()

                if not rows:
                    desk_exposures[desk_id] = {
                        "momentum": 0, "carry": 0, "vol": 0, "n_positions": 0
                    }
                    continue

                momentum_score = 0
                vol_score = 0
                total_risk = 0

                for symbol, direction, risk_pct in rows:
                    risk = float(risk_pct or 0)
                    total_risk += risk

                    # Momentum: direction alignment
                    dir_sign = 1 if direction == "LONG" else -1
                    momentum_score += dir_sign * risk

                    # Vol exposure approximation from risk allocation
                    vol_score += risk

                if total_risk > 0:
                    desk_exposures[desk_id] = {
                        "momentum": round(momentum_score / total_risk, 3),
                        "carry": 0,  # Placeholder — needs interest rate data
                        "vol": round(vol_score, 3),
                        "n_positions": len(rows),
                    }
                else:
                    desk_exposures[desk_id] = {
                        "momentum": 0, "carry": 0, "vol": 0, "n_positions": 0
                    }

            except Exception:
                desk_exposures[desk_id] = {
                    "momentum": 0, "carry": 0, "vol": 0, "n_positions": 0
                }

        return desk_exposures

    def check_concentration(self, db: Session) -> Dict:
        """Check for factor concentration across all desks."""
        exposures = self.compute_factor_exposures(db)

        # Aggregate factor exposures
        total_momentum = 0
        total_vol = 0
        total_positions = 0

        for desk_id, exp in exposures.items():
            total_momentum += abs(exp.get("momentum", 0))
            total_vol += exp.get("vol", 0)
            total_positions += exp.get("n_positions", 0)

        if total_positions == 0:
            return {
                "factor_exposures": exposures,
                "concentration_warning": False,
                "dominant_factor": None,
                "cross_desk_correlation": 0,
                "recommendation": "DIVERSIFIED",
            }

        # Check if all desks are directionally aligned
        directions = []
        for exp in exposures.values():
            if exp["n_positions"] > 0:
                directions.append(np.sign(exp["momentum"]))

        all_same_direction = len(set(directions)) <= 1 and len(directions) > 2
        concentration = total_momentum / max(total_positions, 1)

        concentration_warning = all_same_direction or concentration > CONCENTRATION_THRESHOLD

        # Cross-desk correlation proxy
        cross_corr = concentration

        if concentration_warning:
            recommendation = "ALERT"
        elif concentration > 0.4:
            recommendation = "CONCENTRATED"
        else:
            recommendation = "DIVERSIFIED"

        return {
            "factor_exposures": exposures,
            "concentration_warning": concentration_warning,
            "dominant_factor": "momentum" if total_momentum > total_vol else "volatility",
            "cross_desk_correlation": round(cross_corr, 3),
            "recommendation": recommendation,
        }
