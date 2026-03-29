"""
Weighted Continuous Multi-Timeframe Confluence Scorer

Replaces the binary consensus scoring with a continuous [-1, +1] score.
Each timeframe contributes a directional score based on EMA crossover,
RSI position, and price vs EMA_50, weighted by timeframe importance.

Signal thresholds:
  > +0.30 = LONG candidate
  < -0.30 = SHORT candidate
  between = no signal

Feature flag: MTF_SCORING=WEIGHTED (this module) vs LEGACY (old scorer)
"""
import logging
import os
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger("TradingSystem.SignalEngine.MTFConfluence")

# Timeframe weights — 1m excluded (too noisy)
TF_WEIGHTS = {
    "5M":  0.10,
    "15M": 0.15,
    "1H":  0.25,
    "4H":  0.25,
    "D":   0.20,
}

# Per-indicator weights within a timeframe
INDICATOR_WEIGHTS = {
    "ema_crossover": 0.40,  # EMA_8 vs EMA_21 direction
    "rsi_position":  0.30,  # RSI relative to 50
    "price_vs_ema":  0.30,  # Price vs EMA_50
}

# Signal thresholds
LONG_THRESHOLD = 0.30
SHORT_THRESHOLD = -0.30

# ADX trend strength minimum (checked on 1H)
ADX_MINIMUM = 20

# DB table mapping
TF_TO_TABLE = {
    "5M": "ohlcv_5m", "15M": "ohlcv_15m", "1H": "ohlcv_1h",
    "4H": "ohlcv_4h", "D": "ohlcv_1d",
}


class MTFConfluenceScorer:
    """Weighted continuous multi-timeframe confluence scoring."""

    def __init__(self, candle_manager=None, db_session_factory=None):
        self._candle_manager = candle_manager
        self._db_factory = db_session_factory

    def score(
        self,
        symbol: str,
        candle_manager=None,
        db: Session = None,
    ) -> Dict:
        """
        Compute the weighted confluence score across all timeframes.

        Returns dict with:
          - confluence_score: float in [-1, +1]
          - direction: "LONG" | "SHORT" | None
          - passes_threshold: bool
          - per_tf_scores: dict of timeframe → score
          - adx_filter_pass: bool
          - breakdown: per-TF indicator details
        """
        cm = candle_manager or self._candle_manager
        per_tf: Dict[str, float] = {}
        breakdown: Dict[str, Dict] = {}
        adx_1h = None

        for tf, weight in TF_WEIGHTS.items():
            df = cm.get_dataframe(symbol, tf) if cm else None

            # Fall back to DB if candle_manager doesn't have this TF
            if (df is None or len(df) < 55) and db:
                df = self._load_from_db(symbol, tf, db)

            if df is None or len(df) < 55:
                per_tf[tf] = 0.0
                breakdown[tf] = {"status": "insufficient_data"}
                continue

            tf_score, tf_breakdown = self._score_timeframe(df)
            per_tf[tf] = tf_score
            breakdown[tf] = tf_breakdown

            # Capture 1H ADX for trend filter
            if tf == "1H":
                adx_1h = tf_breakdown.get("adx")

        # Weighted sum
        confluence_score = sum(
            per_tf.get(tf, 0.0) * w for tf, w in TF_WEIGHTS.items()
        )
        confluence_score = round(max(-1.0, min(1.0, confluence_score)), 4)

        # ADX filter on 1H
        adx_pass = (adx_1h is not None and adx_1h >= ADX_MINIMUM) if adx_1h is not None else True

        # Direction and threshold
        if confluence_score > LONG_THRESHOLD and adx_pass:
            direction = "LONG"
            passes = True
        elif confluence_score < SHORT_THRESHOLD and adx_pass:
            direction = "SHORT"
            passes = True
        else:
            direction = None
            passes = False

        # Map to 0-10 scale for compatibility with existing pipeline
        # [-1,+1] → [0, 10]: score_10 = (abs(confluence_score)) * 10
        score_10 = round(abs(confluence_score) * 10, 2)

        return {
            "confluence_score": confluence_score,
            "total_score": score_10,
            "passes_threshold": passes,
            "direction": direction,
            "adx_filter_pass": adx_pass,
            "adx_1h": adx_1h,
            "per_tf_scores": per_tf,
            "breakdown": breakdown,
            # Compatibility fields for existing signal_generator
            "entry_score": per_tf.get("1H", 0.0),
            "confirm_score": per_tf.get("4H", 0.0),
            "bias_score": per_tf.get("D", 0.0),
            "structure_score": 0.0,
            "tier": "HIGH" if abs(confluence_score) > 0.6 else (
                "MEDIUM" if abs(confluence_score) > 0.4 else "LOW"
            ),
        }

    def _score_timeframe(self, df: pd.DataFrame) -> Tuple[float, Dict]:
        """
        Score a single timeframe. Returns (score_in_[-1,+1], breakdown_dict).
        """
        close = df["close"].astype(float)
        high = df["high"].astype(float)
        low = df["low"].astype(float)

        breakdown = {}

        # ── EMA crossover direction: EMA_8 vs EMA_21 ──
        ema8 = close.ewm(span=8, adjust=False).mean()
        ema21 = close.ewm(span=21, adjust=False).mean()
        ema8_val = float(ema8.iloc[-1])
        ema21_val = float(ema21.iloc[-1])

        if ema21_val > 0:
            # Normalized distance: positive = bullish, negative = bearish
            ema_cross = (ema8_val - ema21_val) / ema21_val * 100
            ema_score = max(-1.0, min(1.0, ema_cross / 0.5))  # scale: ±0.5% → ±1.0
        else:
            ema_score = 0.0

        breakdown["ema8"] = round(ema8_val, 5)
        breakdown["ema21"] = round(ema21_val, 5)
        breakdown["ema_score"] = round(ema_score, 4)

        # ── RSI position relative to 50 ──
        try:
            from ta.momentum import RSIIndicator
            rsi_series = RSIIndicator(close=close, window=14).rsi()
            rsi_val = float(rsi_series.iloc[-1]) if rsi_series is not None and not rsi_series.empty else 50.0
        except Exception:
            rsi_val = 50.0

        if pd.isna(rsi_val):
            rsi_val = 50.0

        # Map RSI to [-1, +1]: RSI 30→-1, RSI 50→0, RSI 70→+1
        rsi_score = max(-1.0, min(1.0, (rsi_val - 50) / 20))
        breakdown["rsi"] = round(rsi_val, 2)
        breakdown["rsi_score"] = round(rsi_score, 4)

        # ── Price position vs EMA_50 ──
        ema50 = close.ewm(span=50, adjust=False).mean()
        ema50_val = float(ema50.iloc[-1])
        price = float(close.iloc[-1])

        if ema50_val > 0:
            price_dist = (price - ema50_val) / ema50_val * 100
            price_score = max(-1.0, min(1.0, price_dist / 1.0))  # scale: ±1% → ±1.0
        else:
            price_score = 0.0

        breakdown["price"] = round(price, 5)
        breakdown["ema50"] = round(ema50_val, 5)
        breakdown["price_score"] = round(price_score, 4)

        # ── ADX (computed but not part of directional score — used as filter) ──
        try:
            from ta.trend import ADXIndicator
            adx_ind = ADXIndicator(high=high, low=low, close=close, window=14)
            adx_val = float(adx_ind.adx().iloc[-1])
            if pd.isna(adx_val):
                adx_val = 0.0
        except Exception:
            adx_val = 0.0
        breakdown["adx"] = round(adx_val, 2)

        # ── Weighted combination ──
        tf_score = (
            ema_score * INDICATOR_WEIGHTS["ema_crossover"]
            + rsi_score * INDICATOR_WEIGHTS["rsi_position"]
            + price_score * INDICATOR_WEIGHTS["price_vs_ema"]
        )
        tf_score = round(max(-1.0, min(1.0, tf_score)), 4)
        breakdown["tf_score"] = tf_score

        return tf_score, breakdown

    def _load_from_db(
        self, symbol: str, timeframe: str, db: Session, limit: int = 100,
    ) -> Optional[pd.DataFrame]:
        """Load OHLCV data from PostgreSQL higher-TF tables."""
        table = TF_TO_TABLE.get(timeframe)
        if not table:
            return None

        try:
            rows = db.execute(
                text(f"""
                    SELECT time, open, high, low, close, volume
                    FROM {table}
                    WHERE symbol = :sym
                    ORDER BY time DESC
                    LIMIT :lim
                """),
                {"sym": symbol, "lim": limit},
            ).fetchall()

            if not rows or len(rows) < 55:
                return None

            df = pd.DataFrame(
                rows, columns=["time", "open", "high", "low", "close", "volume"]
            )
            df["time"] = pd.to_datetime(df["time"], utc=True)
            for col in ["open", "high", "low", "close", "volume"]:
                df[col] = pd.to_numeric(df[col], errors="coerce")
            df = df.sort_values("time").reset_index(drop=True)
            return df

        except Exception:
            return None
