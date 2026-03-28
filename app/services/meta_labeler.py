"""
Meta-Labeler — secondary binary classifier that predicts P(correct | signal, features).
Learns WHEN to trade based on triple-barrier labels from shadow signals.
"""
import logging
import os
import pickle
from datetime import datetime, timezone
from typing import Dict, Optional, Tuple

import numpy as np
from sqlalchemy.orm import Session

logger = logging.getLogger("TradingSystem.MetaLabeler")

MODEL_PATH = "/tmp/oniquant_models/meta_labeler.pkl"
MIN_SAMPLES = 300


class MetaLabeler:
    """CatBoost-based secondary classifier for signal quality prediction."""

    def __init__(self):
        self._model = None
        self._model_version: Optional[str] = None
        self._load_model()

    def _load_model(self) -> None:
        """Load saved model from disk if available."""
        try:
            if os.path.exists(MODEL_PATH):
                with open(MODEL_PATH, "rb") as f:
                    saved = pickle.load(f)
                self._model = saved.get("model")
                self._model_version = saved.get("version")
                logger.info(f"Meta-labeler loaded (version: {self._model_version})")
        except Exception as e:
            logger.debug(f"Meta-labeler load failed: {e}")

    def train(self, db: Session) -> Dict:
        """Train meta-labeler on shadow_signals with triple-barrier labels."""
        try:
            from catboost import CatBoostClassifier
            from sqlalchemy import text

            # Query labeled shadow signals
            rows = db.execute(text("""
                SELECT direction, tb_label, tb_return,
                       consensus_score, ml_score, hurst_exponent,
                       rsi, adx, atr_pct, rvol_multiplier,
                       EXTRACT(HOUR FROM created_at) as hour_utc,
                       EXTRACT(DOW FROM created_at) as day_of_week,
                       volatility_regime, vix_level
                FROM shadow_signals
                WHERE tb_label IS NOT NULL
                  AND direction IS NOT NULL
                  AND consensus_score IS NOT NULL
                ORDER BY created_at
            """)).fetchall()

            if not rows or len(rows) < MIN_SAMPLES:
                return {"status": "insufficient_data", "n_samples": len(rows) if rows else 0}

            # Build feature matrix and meta-labels
            features = []
            labels = []
            for r in rows:
                direction = r[0]
                tb_label = r[1]
                tb_return = r[2] or 0

                # Meta-label construction
                if direction == "LONG":
                    meta = 1 if tb_label == 1 else (1 if tb_label == 0 and tb_return > 0 else 0)
                elif direction == "SHORT":
                    meta = 1 if tb_label == 1 else (1 if tb_label == 0 and tb_return > 0 else 0)
                else:
                    continue

                features.append([
                    float(r[3] or 0),   # consensus_score
                    float(r[4] or 0),   # ml_score
                    float(r[5] or 0.5), # hurst_exponent
                    float(r[6] or 50),  # rsi
                    float(r[7] or 20),  # adx
                    float(r[8] or 0),   # atr_pct
                    float(r[9] or 1),   # rvol_multiplier
                    float(r[10] or 12), # hour_utc
                    float(r[11] or 0),  # day_of_week
                    float(r[13] or 20), # vix_level
                ])
                labels.append(meta)

            X = np.array(features)
            y = np.array(labels)

            # Train CatBoost
            model = CatBoostClassifier(
                auto_class_weights="Balanced",
                eval_metric="F1",
                iterations=500,
                learning_rate=0.05,
                depth=5,
                verbose=0,
                random_seed=42,
            )
            model.fit(X, y)

            # Save
            os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
            version = datetime.now(timezone.utc).isoformat()
            with open(MODEL_PATH, "wb") as f:
                pickle.dump({"model": model, "version": version}, f)

            self._model = model
            self._model_version = version

            # Compute accuracy on full set
            predictions = model.predict(X)
            accuracy = float(np.mean(predictions == y))

            logger.info(f"Meta-labeler trained: {len(y)} samples, accuracy={accuracy:.3f}")
            return {
                "status": "trained",
                "n_samples": len(y),
                "accuracy": round(accuracy, 4),
                "positive_rate": round(float(np.mean(y)), 4),
                "version": version,
            }

        except Exception as e:
            logger.error(f"Meta-labeler training failed: {e}")
            return {"status": "error", "message": str(e)}

    def predict(self, features: Dict) -> Dict:
        """Predict probability of correct trade."""
        if self._model is None:
            return self._fallback()

        try:
            feature_vec = [
                float(features.get("consensus_score", 0)),
                float(features.get("ml_score", 0)),
                float(features.get("hurst", 0.5)),
                float(features.get("rsi", 50)),
                float(features.get("adx", 20)),
                float(features.get("atr_pct", 0)),
                float(features.get("rvol", 1)),
                float(features.get("hour_utc", 12)),
                float(features.get("day_of_week", 0)),
                float(features.get("vix_level", 20)),
            ]

            X = np.array([feature_vec])
            proba = self._model.predict_proba(X)[0]
            meta_prob = float(proba[1]) if len(proba) > 1 else float(proba[0])

            should_trade, bet_size = self.should_trade(meta_prob)

            return {
                "meta_probability": round(meta_prob, 4),
                "should_trade": should_trade,
                "sizing_method": "kelly",
                "bet_size": round(bet_size, 4),
                "model_version": self._model_version,
            }

        except Exception as e:
            logger.debug(f"Meta-labeler predict failed: {e}")
            return self._fallback()

    @staticmethod
    def should_trade(probability: float) -> Tuple[bool, float]:
        """Apply threshold and compute bet size."""
        if probability >= 0.75:
            return True, 1.0
        elif probability >= 0.65:
            return True, 0.75
        elif probability >= 0.55:
            return True, 0.50
        else:
            return False, 0.0

    @staticmethod
    def _fallback() -> Dict:
        return {
            "meta_probability": 0.5,
            "should_trade": True,
            "sizing_method": "fallback",
            "bet_size": 1.0,
            "model_version": None,
        }
