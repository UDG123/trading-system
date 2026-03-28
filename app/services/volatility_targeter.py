"""
Volatility Targeter — computes volatility-adjusted position size multipliers.
Output is a size_multiplier that adjusts risk_pct in trade_params.
"""
import logging
from typing import Dict, Optional

import numpy as np
from sqlalchemy import text
from sqlalchemy.orm import Session

from app.config import DESKS

logger = logging.getLogger("TradingSystem.VolTargeter")

# Per-desk target daily volatility
VOL_TARGETS = {
    "DESK1_SCALPER": 0.005,
    "DESK2_INTRADAY": 0.008,
    "DESK3_SWING": 0.012,
    "DESK4_GOLD": 0.015,
    "DESK5_ALTS": 0.020,
    "DESK6_EQUITIES": 0.010,
}

# Per-desk estimation method
VOL_METHODS = {
    "DESK1_SCALPER": "ewma",
    "DESK2_INTRADAY": "ewma",
    "DESK3_SWING": "garch",
    "DESK4_GOLD": "garch",
    "DESK5_ALTS": "rolling",
    "DESK6_EQUITIES": "gjr_garch",
}


class VolatilityTargeter:
    """Computes volatility-adjusted position size multipliers."""

    def compute_target_size(
        self, symbol: str, desk_id: str, base_risk_pct: float, db: Session
    ) -> Dict:
        """Compute vol-adjusted size multiplier."""
        target_vol = VOL_TARGETS.get(desk_id, 0.010)
        method = VOL_METHODS.get(desk_id, "rolling")

        # Fetch returns from DB
        returns = self._fetch_returns(symbol, db)
        if returns is None or len(returns) < 20:
            return {
                "vol_multiplier": 1.0,
                "realized_vol": None,
                "target_vol": target_vol,
                "estimation_method": "fallback",
                "adjusted_risk_pct": base_risk_pct,
            }

        # Estimate realized vol
        if method == "ewma":
            realized = self._ewma_vol(returns)
        elif method == "garch":
            realized = self._garch_vol(returns) or self._rolling_vol(returns)
        elif method == "gjr_garch":
            realized = self._gjr_garch_vol(returns) or self._rolling_vol(returns)
        else:
            realized = self._rolling_vol(returns)

        if realized <= 0:
            realized = target_vol  # fallback

        vol_multiplier = target_vol / realized
        vol_multiplier = max(0.25, min(2.0, vol_multiplier))
        adjusted_risk = round(base_risk_pct * vol_multiplier, 4)

        return {
            "vol_multiplier": round(vol_multiplier, 4),
            "realized_vol": round(realized, 6),
            "target_vol": target_vol,
            "estimation_method": method,
            "adjusted_risk_pct": adjusted_risk,
        }

    @staticmethod
    def _ewma_vol(returns: np.ndarray, lambda_: float = 0.94) -> float:
        """Exponentially weighted moving average volatility."""
        variance = returns[0] ** 2
        for r in returns[1:]:
            variance = lambda_ * variance + (1 - lambda_) * r ** 2
        return float(np.sqrt(variance))

    @staticmethod
    def _rolling_vol(returns: np.ndarray, window: int = 20) -> float:
        """Simple rolling standard deviation."""
        if len(returns) < window:
            return float(np.std(returns))
        return float(np.std(returns[-window:]))

    @staticmethod
    def _garch_vol(returns: np.ndarray) -> Optional[float]:
        """GARCH(1,1) volatility estimate."""
        try:
            from arch import arch_model
            scaled = returns * 100  # arch works better with percentage returns
            model = arch_model(scaled, vol="Garch", p=1, q=1, mean="Zero", rescale=False)
            res = model.fit(disp="off", show_warning=False)
            forecast = res.forecast(horizon=1)
            var = forecast.variance.iloc[-1, 0]
            return float(np.sqrt(var) / 100)  # back to decimal
        except Exception:
            return None

    @staticmethod
    def _gjr_garch_vol(returns: np.ndarray) -> Optional[float]:
        """GJR-GARCH volatility (asymmetric leverage effect)."""
        try:
            from arch import arch_model
            scaled = returns * 100
            model = arch_model(scaled, vol="GARCH", p=1, o=1, q=1, mean="Zero", rescale=False)
            res = model.fit(disp="off", show_warning=False)
            forecast = res.forecast(horizon=1)
            var = forecast.variance.iloc[-1, 0]
            return float(np.sqrt(var) / 100)
        except Exception:
            return None

    @staticmethod
    def _fetch_returns(symbol: str, db: Session, days: int = 60) -> Optional[np.ndarray]:
        """Fetch daily log returns from ohlcv_1d."""
        try:
            rows = db.execute(
                text("""
                    SELECT close FROM ohlcv_1d
                    WHERE symbol = :sym
                    ORDER BY time DESC
                    LIMIT :limit
                """),
                {"sym": symbol, "limit": days + 1},
            ).fetchall()

            if not rows or len(rows) < 21:
                return None

            closes = np.array([float(r[0]) for r in reversed(rows)])
            returns = np.diff(np.log(closes))
            return returns

        except Exception:
            return None
