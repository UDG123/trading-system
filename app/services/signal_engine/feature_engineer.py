"""
Feature Engineer — computes enriched features for CatBoost meta-labeling.

Adds volatility, microstructure, momentum, calendar, regime, and MTF
features to a DataFrame of OHLCV data for model training and inference.
"""
import logging
from datetime import datetime, timezone
from typing import Dict, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger("TradingSystem.SignalEngine.Features")


def compute_features(
    df: pd.DataFrame,
    symbol: str = "",
    regime_label: str = None,
    mtf_confluence_score: float = None,
) -> pd.DataFrame:
    """
    Add engineered features to an OHLCV DataFrame.

    Input: DataFrame with columns [time, open, high, low, close, volume].
    Returns: DataFrame with all original columns plus feature columns.
    Rows with NaN in critical features are forward-filled; rows with
    NaN in the last position are NOT dropped (caller decides).
    """
    if df is None or len(df) < 55:
        return df

    out = df.copy()
    o = out["open"].astype(float)
    h = out["high"].astype(float)
    l = out["low"].astype(float)  # noqa: E741
    c = out["close"].astype(float)
    v = out["volume"].astype(float) if "volume" in out.columns else pd.Series(0, index=out.index)

    hl_range = (h - l).replace(0, np.nan)  # avoid div-by-zero

    # ═══════════════════════════════════════════════════════
    # VOLATILITY FEATURES
    # ═══════════════════════════════════════════════════════

    # Garman-Klass per-bar, then 20-bar rolling mean
    log_hl = np.log(h / l.replace(0, np.nan))
    log_co = np.log(c / o.replace(0, np.nan))
    gk_per_bar = 0.5 * log_hl ** 2 - (2 * np.log(2) - 1) * log_co ** 2
    out["garman_klass_vol"] = gk_per_bar.rolling(20).mean()

    # Parkinson per-bar, then 20-bar rolling mean
    pk_per_bar = (1 / (4 * np.log(2))) * log_hl ** 2
    out["parkinson_vol"] = pk_per_bar.rolling(20).mean()

    # Vol regime: percentile rank of GK vol over 100 bars
    gk_rolling = out["garman_klass_vol"]
    out["vol_pctile"] = gk_rolling.rolling(100, min_periods=50).apply(
        lambda x: pd.Series(x).rank(pct=True).iloc[-1], raw=False,
    )
    out["vol_regime"] = pd.cut(
        out["vol_pctile"],
        bins=[-0.01, 0.25, 0.75, 1.01],
        labels=["LOW", "MEDIUM", "HIGH"],
    )

    # ATR ratio: ATR(14) / ATR(50)
    tr = pd.concat([
        h - l,
        (h - c.shift(1)).abs(),
        (l - c.shift(1)).abs(),
    ], axis=1).max(axis=1)
    atr14 = tr.rolling(14).mean()
    atr50 = tr.rolling(50).mean()
    out["atr_ratio"] = (atr14 / atr50.replace(0, np.nan)).fillna(1.0)

    # ═══════════════════════════════════════════════════════
    # MICROSTRUCTURE PROXIES
    # ═══════════════════════════════════════════════════════

    # Close location value: buying/selling pressure
    out["close_location_value"] = ((c - l) - (h - c)) / hl_range

    # Candle body ratio: conviction
    out["candle_body_ratio"] = (c - o).abs() / hl_range

    # Upper wick ratio
    max_oc = pd.concat([o, c], axis=1).max(axis=1)
    min_oc = pd.concat([o, c], axis=1).min(axis=1)
    out["upper_wick_ratio"] = (h - max_oc) / hl_range

    # Lower wick ratio
    out["lower_wick_ratio"] = (min_oc - l) / hl_range

    # ═══════════════════════════════════════════════════════
    # MOMENTUM / MEAN-REVERSION
    # ═══════════════════════════════════════════════════════

    # Z-scores
    sma20 = c.rolling(20).mean()
    std20 = c.rolling(20).std()
    out["zscore_20"] = (c - sma20) / std20.replace(0, np.nan)

    sma50 = c.rolling(50).mean()
    std50 = c.rolling(50).std()
    out["zscore_50"] = (c - sma50) / std50.replace(0, np.nan)

    # Rate of change
    out["roc_5"] = c.pct_change(5)
    out["roc_20"] = c.pct_change(20)

    # ═══════════════════════════════════════════════════════
    # CALENDAR FEATURES (CatBoost categoricals)
    # ═══════════════════════════════════════════════════════

    if "time" in out.columns:
        ts = pd.to_datetime(out["time"], utc=True)
        out["hour_of_day"] = ts.dt.hour
        out["day_of_week"] = ts.dt.dayofweek

        # Trading session
        out["trading_session"] = ts.dt.hour.map(_hour_to_session)

        # Month-end flag (last 3 trading days)
        out["is_month_end"] = ts.dt.is_month_end | (
            ts.dt.day >= (ts.dt.days_in_month - 2)
        )
    else:
        now = datetime.now(timezone.utc)
        out["hour_of_day"] = now.hour
        out["day_of_week"] = now.weekday()
        out["trading_session"] = _hour_to_session(now.hour)
        out["is_month_end"] = False

    # ═══════════════════════════════════════════════════════
    # REGIME (from HMM detector)
    # ═══════════════════════════════════════════════════════

    out["regime_label"] = regime_label or "UNKNOWN"

    # bars_since_regime_change: how many bars the regime has been stable
    # (approximated — exact tracking requires regime history)
    out["bars_since_regime_change"] = 0  # placeholder for inference

    # ═══════════════════════════════════════════════════════
    # MTF CONFLUENCE
    # ═══════════════════════════════════════════════════════

    out["mtf_confluence_score"] = mtf_confluence_score or 0.0

    # ═══════════════════════════════════════════════════════
    # CLEANUP
    # ═══════════════════════════════════════════════════════

    # Forward-fill rolling features (NaN at beginning of series)
    rolling_cols = [
        "garman_klass_vol", "parkinson_vol", "vol_pctile",
        "atr_ratio", "zscore_20", "zscore_50", "roc_5", "roc_20",
    ]
    for col in rolling_cols:
        if col in out.columns:
            out[col] = out[col].ffill()

    return out


def extract_feature_row(
    df: pd.DataFrame,
    symbol: str = "",
    regime_label: str = None,
    mtf_confluence_score: float = None,
) -> Optional[Dict]:
    """
    Compute features and extract only the latest row as a flat dict.
    Used for single-signal inference.
    """
    featured = compute_features(df, symbol, regime_label, mtf_confluence_score)
    if featured is None or len(featured) == 0:
        return None

    row = featured.iloc[-1]

    # Feature columns (exclude raw OHLCV and time)
    feature_cols = [
        "garman_klass_vol", "parkinson_vol", "vol_regime", "atr_ratio",
        "close_location_value", "candle_body_ratio",
        "upper_wick_ratio", "lower_wick_ratio",
        "zscore_20", "zscore_50", "roc_5", "roc_20",
        "hour_of_day", "day_of_week", "trading_session", "is_month_end",
        "regime_label", "bars_since_regime_change",
        "mtf_confluence_score",
    ]

    result = {}
    for col in feature_cols:
        if col in row.index:
            val = row[col]
            if pd.isna(val):
                result[col] = 0.0
            elif isinstance(val, (np.integer, np.floating)):
                result[col] = float(val)
            elif isinstance(val, bool) or isinstance(val, np.bool_):
                result[col] = 1.0 if val else 0.0
            else:
                result[col] = str(val)  # categorical features

    return result


# Numeric-only feature names for model training (excludes categoricals)
NUMERIC_FEATURES = [
    "garman_klass_vol", "parkinson_vol", "atr_ratio",
    "close_location_value", "candle_body_ratio",
    "upper_wick_ratio", "lower_wick_ratio",
    "zscore_20", "zscore_50", "roc_5", "roc_20",
    "hour_of_day", "day_of_week", "is_month_end",
    "bars_since_regime_change", "mtf_confluence_score",
]

# Categorical feature names (for CatBoost cat_features parameter)
CATEGORICAL_FEATURES = [
    "vol_regime", "trading_session", "regime_label",
]

# All feature names in order
ALL_FEATURES = NUMERIC_FEATURES + CATEGORICAL_FEATURES


def _hour_to_session(hour: int) -> str:
    """Map UTC hour to trading session name."""
    if 0 <= hour < 7:
        return "ASIAN"
    elif 7 <= hour < 12:
        return "LONDON"
    elif 12 <= hour < 16:
        return "OVERLAP"
    elif 16 <= hour < 21:
        return "NEW_YORK"
    else:
        return "ASIAN"
