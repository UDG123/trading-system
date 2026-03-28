"""
Wickless Candle Detector — detects Marubozu/wickless candles
as a confluence confirmation signal.
"""
import logging
from typing import Dict, Optional

import pandas as pd

logger = logging.getLogger("TradingSystem.SignalEngine.Wickless")


def detect_wickless(df: pd.DataFrame, threshold: float = 0.05, lookback: int = 3) -> Dict:
    """
    Detect wickless (Marubozu) candles in the last N bars.

    A bullish wickless: (open - low) / (high - low) < threshold AND close > open
    A bearish wickless: (high - close) / (high - low) < threshold AND close < open

    Returns dict with detection results.
    """
    result = {
        "has_wickless": False,
        "wickless_type": None,
        "wickless_bar_index": None,
        "body_pct": 0.0,
    }

    if df is None or len(df) < lookback:
        return result

    for i in range(lookback):
        idx = -(i + 1)
        try:
            row = df.iloc[idx]
            high = float(row["high"])
            low = float(row["low"])
            open_ = float(row["open"])
            close = float(row["close"])

            bar_range = high - low
            if bar_range <= 0:
                continue

            body = abs(close - open_)
            body_pct = body / bar_range

            # Bullish wickless: minimal lower wick, close > open
            lower_wick_pct = (min(open_, close) - low) / bar_range
            if close > open_ and lower_wick_pct < threshold:
                result["has_wickless"] = True
                result["wickless_type"] = "bullish"
                result["wickless_bar_index"] = i
                result["body_pct"] = round(body_pct, 3)
                return result

            # Bearish wickless: minimal upper wick, close < open
            upper_wick_pct = (high - max(open_, close)) / bar_range
            if close < open_ and upper_wick_pct < threshold:
                result["has_wickless"] = True
                result["wickless_type"] = "bearish"
                result["wickless_bar_index"] = i
                result["body_pct"] = round(body_pct, 3)
                return result

        except (IndexError, KeyError, ValueError):
            continue

    return result
