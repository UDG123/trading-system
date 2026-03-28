"""
HAR-RV — Heterogeneous Autoregressive Realized Volatility model.
Forward-looking volatility estimation for SL/TP calculation.
"""
import logging
from typing import Dict, Optional

import numpy as np
from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger("TradingSystem.HARRV")

MIN_DAYS = 60
REDIS_PREFIX = "har_rv:"
REDIS_TTL = 86400  # 24h


class HARRV:
    """HAR-RV model for next-day volatility forecasting."""

    def __init__(self, redis_pool=None):
        self.redis = redis_pool

    def compute_realized_variance(self, symbol: str, db: Session) -> Optional[float]:
        """Sum of squared 1-minute log returns for the most recent trading day."""
        try:
            rows = db.execute(
                text("""
                    SELECT close FROM ohlcv_1m
                    WHERE symbol = :sym
                      AND time >= NOW() - INTERVAL '1 day'
                    ORDER BY time ASC
                """),
                {"sym": symbol},
            ).fetchall()

            if not rows or len(rows) < 30:
                return None

            closes = np.array([float(r[0]) for r in rows])
            log_returns = np.diff(np.log(closes))
            return float(np.sum(log_returns ** 2))

        except Exception:
            return None

    def fit_har(self, symbol: str, db: Session) -> Dict:
        """
        Fit HAR model: RV_{t+1} = β₀ + β_d*RV_t + β_w*RV_w + β_m*RV_m + ε
        """
        try:
            # Fetch daily closes to compute daily RV proxy
            rows = db.execute(
                text("""
                    SELECT close FROM ohlcv_1d
                    WHERE symbol = :sym
                    ORDER BY time DESC
                    LIMIT :lim
                """),
                {"sym": symbol, "lim": MIN_DAYS + 25},
            ).fetchall()

            if not rows or len(rows) < MIN_DAYS:
                return {"status": "insufficient_data"}

            closes = np.array([float(r[0]) for r in reversed(rows)])
            returns = np.diff(np.log(closes))
            rv_daily = returns ** 2  # squared return as RV proxy

            if len(rv_daily) < MIN_DAYS:
                return {"status": "insufficient_data"}

            # Build HAR features
            n = len(rv_daily)
            y = rv_daily[22:]  # target: next-day RV
            X_d = rv_daily[21:n - 1]  # daily RV
            X_w = np.array([np.mean(rv_daily[i - 4:i + 1]) for i in range(21, n - 1)])  # weekly avg
            X_m = np.array([np.mean(rv_daily[i - 21:i + 1]) for i in range(21, n - 1)])  # monthly avg

            # Trim to same length
            min_len = min(len(y), len(X_d), len(X_w), len(X_m))
            y = y[:min_len]
            X = np.column_stack([np.ones(min_len), X_d[:min_len], X_w[:min_len], X_m[:min_len]])

            # OLS
            betas, residuals, _, _ = np.linalg.lstsq(X, y, rcond=None)

            # R-squared
            ss_res = np.sum((y - X @ betas) ** 2)
            ss_tot = np.sum((y - np.mean(y)) ** 2)
            r_squared = 1 - ss_res / ss_tot if ss_tot > 0 else 0

            return {
                "status": "fitted",
                "betas": betas.tolist(),
                "r_squared": round(float(r_squared), 4),
                "n_obs": min_len,
                "rv_daily_latest": float(rv_daily[-1]),
                "rv_weekly_avg": float(np.mean(rv_daily[-5:])),
                "rv_monthly_avg": float(np.mean(rv_daily[-22:])),
            }

        except Exception as e:
            logger.debug(f"HAR-RV fit failed for {symbol}: {e}")
            return {"status": "error", "message": str(e)}

    def forecast(self, symbol: str, db: Session) -> Dict:
        """Return next-day volatility forecast."""
        fit = self.fit_har(symbol, db)
        if fit.get("status") != "fitted":
            return {"status": fit.get("status", "error")}

        betas = np.array(fit["betas"])
        rv_d = fit["rv_daily_latest"]
        rv_w = fit["rv_weekly_avg"]
        rv_m = fit["rv_monthly_avg"]

        forecast_rv = betas[0] + betas[1] * rv_d + betas[2] * rv_w + betas[3] * rv_m
        forecast_rv = max(forecast_rv, 1e-10)  # ensure positive
        forecast_vol = float(np.sqrt(forecast_rv))
        annualized = forecast_vol * np.sqrt(252)

        return {
            "status": "forecast",
            "rv_daily": rv_d,
            "rv_weekly_avg": rv_w,
            "rv_monthly_avg": rv_m,
            "forecast_next_day": round(forecast_rv, 8),
            "forecast_vol": round(forecast_vol, 6),
            "forecast_annualized": round(annualized, 4),
            "model_r_squared": fit["r_squared"],
        }
