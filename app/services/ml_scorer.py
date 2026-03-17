"""
ML Signal Scorer
Trains on historical trade data from PostgreSQL. Outputs a probability
score (0.0–1.0) for each incoming signal. Retrains weekly.

Before enough trade history exists, uses a rule-based fallback scorer.
"""
import os
import json
import pickle
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, Tuple

import numpy as np

from app.config import DESKS, SCORE_WEIGHTS

logger = logging.getLogger("TradingSystem.MLScorer")

MODEL_PATH = os.getenv("ML_MODEL_PATH", "/tmp/signal_model.pkl")
MIN_TRAINING_SAMPLES = 100  # need at least this many closed trades to train


class MLScorer:
    """
    Scores incoming signals with a probability of success.
    Uses scikit-learn RandomForest when trained, rule-based fallback otherwise.
    """

    def __init__(self):
        self.model = None
        self.feature_names = []
        self.is_trained = False
        self._load_model()

    def score(self, signal_data: Dict, enrichment: Dict, desk_id: str) -> Dict:
        """
        Score a signal. Returns dict with:
        - ml_score: float 0.0–1.0 (probability of profitable trade)
        - ml_features: dict of features used
        - ml_method: "model" or "rule_based"
        """
        features = self._extract_features(signal_data, enrichment, desk_id)

        if self.is_trained and self.model is not None:
            return self._model_score(features)
        else:
            return self._rule_based_score(features, signal_data, enrichment)

    def _extract_features(
        self, signal_data: Dict, enrichment: Dict, desk_id: str
    ) -> Dict:
        """Extract feature vector from signal + enrichment data."""
        desk = DESKS.get(desk_id, {})

        features = {
            # ── Signal features ──
            "is_bullish": 1.0 if signal_data.get("direction") == "LONG" else 0.0,
            "is_bearish": 1.0 if signal_data.get("direction") == "SHORT" else 0.0,
            "is_plus_signal": 1.0 if "plus" in signal_data.get("alert_type", "") else 0.0,
            "is_confirmation_turn": 1.0 if "confirmation_turn" in signal_data.get("alert_type", "") else 0.0,
            "is_contrarian": 1.0 if "contrarian" in signal_data.get("alert_type", "") else 0.0,

            # ── Risk/reward features ──
            "rr_ratio": self._calc_rr_ratio(signal_data),
            "sl_distance_pct": self._calc_sl_distance_pct(signal_data),

            # ── Market context features ──
            "rsi": enrichment.get("rsi", 50.0) or 50.0,
            "rsi_extreme": self._rsi_extreme_score(enrichment.get("rsi")),
            "adx": enrichment.get("adx", 20.0) or 20.0,
            "atr_pct": enrichment.get("atr_pct", 0.5) or 0.5,
            "spread": enrichment.get("spread", 0) or 0,
            "volume": enrichment.get("volume", 0) or 0,

            # ── Session features ──
            "is_kill_zone": 1.0 if enrichment.get("is_kill_zone") else 0.0,
            "is_overlap": 1.0 if enrichment.get("kill_zone_type") == "OVERLAP" else 0.0,
            "is_london": 1.0 if "LONDON" in enrichment.get("active_session", "") else 0.0,
            "is_ny": 1.0 if "NEW_YORK" in enrichment.get("active_session", "") else 0.0,

            # ── Volatility regime ──
            "regime_trending": 1.0 if enrichment.get("volatility_regime") == "TRENDING" else 0.0,
            "regime_high_vol": 1.0 if enrichment.get("volatility_regime") == "HIGH_VOLATILITY" else 0.0,
            "regime_low_vol": 1.0 if enrichment.get("volatility_regime") == "LOW_VOLATILITY" else 0.0,

            # ── Desk features ──
            "desk_risk_pct": desk.get("risk_pct", 1.0),

            # ── Intermarket features ──
            "dxy_change": self._get_intermarket_change(enrichment, "DXY"),
            "vix_level": self._get_intermarket_price(enrichment, "VIX"),
        }

        return features

    def _model_score(self, features: Dict) -> Dict:
        """Score using trained sklearn model."""
        try:
            # Build feature array in training order
            X = np.array(
                [[features.get(f, 0.0) for f in self.feature_names]]
            )
            proba = self.model.predict_proba(X)[0]
            # proba[1] = probability of class 1 (profitable trade)
            score = float(proba[1]) if len(proba) > 1 else float(proba[0])

            return {
                "ml_score": round(score, 4),
                "ml_features": features,
                "ml_method": "model",
            }
        except Exception as e:
            logger.error(f"Model scoring failed, falling back to rules: {e}")
            return self._rule_based_score(features, {}, {})

    def _rule_based_score(
        self, features: Dict, signal_data: Dict, enrichment: Dict
    ) -> Dict:
        """
        Rule-based scoring when model isn't trained yet.
        Outputs a probability-like score from 0.0 to 1.0.
        """
        score = 0.50  # baseline

        # Plus signals are stronger
        if features["is_plus_signal"]:
            score += 0.10
        if features["is_confirmation_turn"]:
            score += 0.08

        # Good risk/reward
        rr = features["rr_ratio"]
        if rr >= 3.0:
            score += 0.10
        elif rr >= 2.0:
            score += 0.05
        elif rr < 1.0:
            score -= 0.15

        # Kill zone bonus
        if features["is_overlap"]:
            score += 0.08
        elif features["is_kill_zone"]:
            score += 0.04

        # RSI alignment
        if features["is_bullish"] and features["rsi"] < 30:
            score += 0.07  # oversold + bullish = good
        elif features["is_bearish"] and features["rsi"] > 70:
            score += 0.07  # overbought + bearish = good
        elif features["is_bullish"] and features["rsi"] > 80:
            score -= 0.10  # overbought + bullish = risky
        elif features["is_bearish"] and features["rsi"] < 20:
            score -= 0.10  # oversold + bearish = risky

        # Volatility regime
        if features["regime_trending"]:
            score += 0.05  # trending markets favor trend signals
        if features["regime_high_vol"]:
            score -= 0.05  # high vol = wider stops, less predictable
        if features["regime_low_vol"]:
            score -= 0.03  # low vol = less opportunity

        # ADX trend strength
        adx = features.get("adx", 20.0)
        if adx >= 30:
            score += 0.06  # strong trend = higher probability
        elif adx >= 25:
            score += 0.03  # moderate trend
        elif adx < 18:
            score -= 0.04  # ranging market = lower probability

        # Contrarian signals are inherently riskier
        if features["is_contrarian"]:
            score -= 0.05

        # VIX elevated = general market fear
        vix = features.get("vix_level", 0)
        if vix > 30:
            score -= 0.05
        elif vix > 40:
            score -= 0.10

        # Clamp to 0.0–1.0
        score = max(0.0, min(1.0, score))

        return {
            "ml_score": round(score, 4),
            "ml_features": features,
            "ml_method": "rule_based",
        }

    # ────────────────────────────────────────
    # Training (called weekly by scheduler)
    # ────────────────────────────────────────

    def train(self, db_session) -> Dict:
        """
        Train model on historical trade data.
        Called by the weekly retraining scheduler.
        Returns training metrics.
        """
        from sklearn.ensemble import RandomForestClassifier
        from sklearn.model_selection import cross_val_score
        from app.models.trade import Trade
        from app.models.signal import Signal

        # Fetch closed trades with their signals
        trades = (
            db_session.query(Trade)
            .filter(Trade.status == "CLOSED")
            .filter(Trade.pnl_dollars.isnot(None))
            .all()
        )

        if len(trades) < MIN_TRAINING_SAMPLES:
            logger.info(
                f"Only {len(trades)} closed trades — need {MIN_TRAINING_SAMPLES} to train"
            )
            return {
                "status": "insufficient_data",
                "samples": len(trades),
                "required": MIN_TRAINING_SAMPLES,
            }

        # Build training data
        X_rows = []
        y_labels = []

        for trade in trades:
            signal = (
                db_session.query(Signal)
                .filter(Signal.id == trade.signal_id)
                .first()
            )
            if not signal:
                continue

            # Reconstruct features from stored signal data
            signal_data = {
                "direction": signal.direction,
                "alert_type": signal.alert_type,
                "price": signal.price,
                "tp1": signal.tp1,
                "sl1": signal.sl1,
            }

            # Use stored enrichment if available, otherwise defaults
            enrichment = {}
            if signal.raw_payload:
                try:
                    payload = json.loads(signal.raw_payload)
                    enrichment = payload.get("enrichment", {})
                except (json.JSONDecodeError, AttributeError):
                    pass

            features = self._extract_features(
                signal_data, enrichment, trade.desk_id
            )
            X_rows.append(features)

            # Label: 1 = profitable, 0 = loss
            y_labels.append(1 if trade.pnl_dollars > 0 else 0)

        if not X_rows:
            return {"status": "no_valid_samples"}

        # Build numpy arrays
        self.feature_names = sorted(X_rows[0].keys())
        X = np.array(
            [[row.get(f, 0.0) for f in self.feature_names] for row in X_rows]
        )
        y = np.array(y_labels)

        # Train Random Forest
        model = RandomForestClassifier(
            n_estimators=200,
            max_depth=10,
            min_samples_leaf=5,
            random_state=42,
            class_weight="balanced",
        )
        model.fit(X, y)

        # Cross-validation score
        cv_scores = cross_val_score(model, X, y, cv=5, scoring="accuracy")

        # Save model
        self.model = model
        self.is_trained = True
        self._save_model()

        # Feature importance
        importance = dict(
            zip(self.feature_names, model.feature_importances_)
        )
        top_features = sorted(
            importance.items(), key=lambda x: x[1], reverse=True
        )[:10]

        metrics = {
            "status": "trained",
            "samples": len(X),
            "positive_rate": round(sum(y) / len(y), 4),
            "cv_accuracy_mean": round(float(cv_scores.mean()), 4),
            "cv_accuracy_std": round(float(cv_scores.std()), 4),
            "top_features": top_features,
            "trained_at": datetime.now(timezone.utc).isoformat(),
        }

        logger.info(
            f"ML Model trained: {metrics['samples']} samples, "
            f"CV accuracy: {metrics['cv_accuracy_mean']:.1%} "
            f"± {metrics['cv_accuracy_std']:.1%}"
        )

        return metrics

    # ────────────────────────────────────────
    # Helper calculations
    # ────────────────────────────────────────

    def _calc_rr_ratio(self, signal: Dict) -> float:
        """Calculate risk/reward ratio from signal levels."""
        price = signal.get("price", 0)
        tp1 = signal.get("tp1")
        sl1 = signal.get("sl1")
        if not price or not tp1 or not sl1:
            return 1.0
        risk = abs(price - sl1)
        reward = abs(tp1 - price)
        if risk == 0:
            return 0.0
        return round(reward / risk, 2)

    def _calc_sl_distance_pct(self, signal: Dict) -> float:
        """SL distance as percentage of price."""
        price = signal.get("price", 0)
        sl1 = signal.get("sl1")
        if not price or not sl1 or price == 0:
            return 0.0
        return round(abs(price - sl1) / price * 100, 4)

    def _rsi_extreme_score(self, rsi: Optional[float]) -> float:
        """How extreme is RSI? 0 = neutral, 1 = very extreme."""
        if rsi is None:
            return 0.0
        return round(abs(rsi - 50) / 50, 2)

    def _get_intermarket_change(self, enrichment: Dict, key: str) -> float:
        """Get percentage change for an intermarket instrument."""
        im = enrichment.get("intermarket", {})
        if im and key in im and im[key]:
            return float(im[key].get("change_pct", 0))
        return 0.0

    def _get_intermarket_price(self, enrichment: Dict, key: str) -> float:
        """Get current price for an intermarket instrument."""
        im = enrichment.get("intermarket", {})
        if im and key in im and im[key]:
            return float(im[key].get("price", 0))
        return 0.0

    # ────────────────────────────────────────
    # Model persistence
    # ────────────────────────────────────────

    def _save_model(self):
        """Persist model to disk."""
        try:
            with open(MODEL_PATH, "wb") as f:
                pickle.dump(
                    {
                        "model": self.model,
                        "feature_names": self.feature_names,
                        "saved_at": datetime.now(timezone.utc).isoformat(),
                    },
                    f,
                )
            logger.info(f"Model saved to {MODEL_PATH}")
        except Exception as e:
            logger.error(f"Failed to save model: {e}")

    def _load_model(self):
        """Load model from disk if available."""
        try:
            if os.path.exists(MODEL_PATH):
                with open(MODEL_PATH, "rb") as f:
                    data = pickle.load(f)
                self.model = data["model"]
                self.feature_names = data["feature_names"]
                self.is_trained = True
                logger.info(
                    f"Loaded ML model from {MODEL_PATH} "
                    f"(saved: {data.get('saved_at', 'unknown')})"
                )
        except Exception as e:
            logger.warning(f"Could not load model: {e}")
            self.is_trained = False
