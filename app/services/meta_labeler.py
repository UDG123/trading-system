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

    # Enhanced feature column names matching the query order
    _FEATURE_NAMES = [
        "consensus_score", "ml_score", "hurst_exponent",
        "rsi", "adx", "atr_pct", "rvol_multiplier",
        "hour_utc", "day_of_week", "vix_level",
        # Enhanced features from feature_engineer (if available in feature_vector)
        "garman_klass_vol", "parkinson_vol", "atr_ratio",
        "close_location_value", "candle_body_ratio",
        "upper_wick_ratio", "lower_wick_ratio",
        "zscore_20", "zscore_50", "roc_5", "roc_20",
        "mtf_confluence_score",
    ]

    def train(self, db: Session) -> Dict:
        """Train meta-labeler on shadow_signals with triple-barrier labels and enhanced features."""
        try:
            from catboost import CatBoostClassifier
            from sklearn.model_selection import TimeSeriesSplit
            from sklearn.metrics import f1_score, accuracy_score
            from sqlalchemy import text

            # Query labeled shadow signals with feature_vector JSONB
            rows = db.execute(text("""
                SELECT direction, tb_label, tb_return,
                       consensus_score, ml_score, hurst_exponent,
                       rsi, adx, atr_pct, rvol_multiplier,
                       EXTRACT(HOUR FROM created_at) as hour_utc,
                       EXTRACT(DOW FROM created_at) as day_of_week,
                       volatility_regime, vix_level,
                       feature_vector
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

                # Meta-label: 1 if trade direction was correct
                if direction not in ("LONG", "SHORT"):
                    continue
                meta = 1 if tb_label == 1 else (1 if tb_label == 0 and tb_return > 0 else 0)

                # Core features (always available)
                row = [
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
                ]

                # Enhanced features from feature_vector JSONB (if available)
                fv = r[14] or {}
                row.extend([
                    float(fv.get("garman_klass_vol", 0) or 0),
                    float(fv.get("parkinson_vol", 0) or 0),
                    float(fv.get("atr_ratio", 1) or 1),
                    float(fv.get("close_location_value", 0) or 0),
                    float(fv.get("candle_body_ratio", 0.5) or 0.5),
                    float(fv.get("upper_wick_ratio", 0) or 0),
                    float(fv.get("lower_wick_ratio", 0) or 0),
                    float(fv.get("zscore_20", 0) or 0),
                    float(fv.get("zscore_50", 0) or 0),
                    float(fv.get("roc_5", 0) or 0),
                    float(fv.get("roc_20", 0) or 0),
                    float(fv.get("mtf_confluence_score", 0) or 0),
                ])

                features.append(row)
                labels.append(meta)

            X = np.array(features)
            y = np.array(labels)

            # Class imbalance ratio for scale_pos_weight
            n_pos = int(np.sum(y))
            n_neg = len(y) - n_pos
            scale_pos_weight = n_neg / n_pos if n_pos > 0 else 1.0

            # TimeSeriesSplit cross-validation
            tscv = TimeSeriesSplit(n_splits=5)
            cv_f1_scores = []
            for train_idx, test_idx in tscv.split(X):
                X_tr, X_te = X[train_idx], X[test_idx]
                y_tr, y_te = y[train_idx], y[test_idx]

                fold_model = CatBoostClassifier(
                    auto_class_weights="Balanced",
                    eval_metric="F1",
                    iterations=1000,
                    learning_rate=0.03,
                    depth=6,
                    early_stopping_rounds=50,
                    scale_pos_weight=scale_pos_weight,
                    verbose=0,
                    random_seed=42,
                )
                fold_model.fit(X_tr, y_tr, eval_set=(X_te, y_te))
                y_pred = fold_model.predict(X_te)
                cv_f1_scores.append(f1_score(y_te, y_pred, zero_division=0))

            mean_cv_f1 = float(np.mean(cv_f1_scores))

            # Train final model on all data
            model = CatBoostClassifier(
                auto_class_weights="Balanced",
                eval_metric="F1",
                iterations=1000,
                learning_rate=0.03,
                depth=6,
                scale_pos_weight=scale_pos_weight,
                verbose=0,
                random_seed=42,
            )
            model.fit(X, y)

            # Feature importance — log top 10
            importances = model.feature_importances_
            feature_importance = sorted(
                zip(self._FEATURE_NAMES[:len(importances)], importances),
                key=lambda x: x[1], reverse=True,
            )
            top_10 = feature_importance[:10]
            logger.info("Meta-labeler top 10 features:")
            for fname, fimp in top_10:
                logger.info(f"  {fname}: {fimp:.2f}")

            # Save
            os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
            version = datetime.now(timezone.utc).isoformat()
            with open(MODEL_PATH, "wb") as f:
                pickle.dump({
                    "model": model,
                    "version": version,
                    "feature_names": self._FEATURE_NAMES[:len(importances)],
                    "feature_importance": [(n, round(float(v), 4)) for n, v in top_10],
                }, f)

            self._model = model
            self._model_version = version

            predictions = model.predict(X)
            accuracy = float(np.mean(predictions == y))

            logger.info(
                f"Meta-labeler trained: {len(y)} samples | "
                f"accuracy={accuracy:.3f} | cv_f1={mean_cv_f1:.3f} | "
                f"scale_pos_weight={scale_pos_weight:.2f}"
            )
            return {
                "status": "trained",
                "n_samples": len(y),
                "n_features": len(self._FEATURE_NAMES),
                "accuracy": round(accuracy, 4),
                "cv_f1_mean": round(mean_cv_f1, 4),
                "positive_rate": round(float(np.mean(y)), 4),
                "scale_pos_weight": round(scale_pos_weight, 2),
                "top_features": [(n, round(float(v), 4)) for n, v in top_10],
                "version": version,
            }

        except Exception as e:
            logger.error(f"Meta-labeler training failed: {e}")
            return {"status": "error", "message": str(e)}

    def predict(self, features: Dict) -> Dict:
        """Predict probability of correct trade using enhanced feature set."""
        if self._model is None:
            return self._fallback()

        try:
            # Core features (always available)
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

            # Enhanced features (0 default if not available yet)
            feature_vec.extend([
                float(features.get("garman_klass_vol", 0)),
                float(features.get("parkinson_vol", 0)),
                float(features.get("atr_ratio", 1)),
                float(features.get("close_location_value", 0)),
                float(features.get("candle_body_ratio", 0.5)),
                float(features.get("upper_wick_ratio", 0)),
                float(features.get("lower_wick_ratio", 0)),
                float(features.get("zscore_20", 0)),
                float(features.get("zscore_50", 0)),
                float(features.get("roc_5", 0)),
                float(features.get("roc_20", 0)),
                float(features.get("mtf_confluence_score", 0)),
            ])

            # Trim to model's expected feature count (handles old models with fewer features)
            n_model_features = self._model.feature_count_ if hasattr(self._model, 'feature_count_') else len(feature_vec)
            feature_vec = feature_vec[:n_model_features]

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
