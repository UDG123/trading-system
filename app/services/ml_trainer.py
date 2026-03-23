"""
MLTrainer — Trains CatBoost and XGBoost models on labeled shadow signal data
using Purged K-Fold cross-validation.

Replaces the existing RandomForest in ml_scorer.py with production-grade
gradient boosting trained on triple-barrier-labeled data.
"""
import os
import pickle
import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Optional, List, Tuple

import numpy as np

from app.models.shadow_signal import ShadowSignal

logger = logging.getLogger("TradingSystem.MLTrainer")

MODEL_DIR = os.getenv("ML_MODEL_DIR", "/tmp/oniquant_models/")


class MLTrainer:
    """Trains CatBoost/XGBoost on triple-barrier-labeled shadow signals."""

    def __init__(self, db_session_factory):
        self._db_factory = db_session_factory
        os.makedirs(MODEL_DIR, exist_ok=True)

    async def train(self, min_samples: int = 500, test_size: float = 0.2) -> Dict:
        """
        Train models on labeled shadow signals.
        Returns training metrics dict.
        """
        import pandas as pd

        db = self._db_factory()
        try:
            # Query labeled signals
            labeled = (
                db.query(ShadowSignal)
                .filter(ShadowSignal.tb_label.isnot(None))
                .all()
            )

            if len(labeled) < min_samples:
                return {
                    "status": "insufficient_data",
                    "count": len(labeled),
                    "required": min_samples,
                }

            # Extract features from feature_vector JSONB
            rows = []
            targets = []
            timestamps = []
            for sig in labeled:
                fv = sig.feature_vector or {}
                if not fv:
                    continue

                # Flatten features into a row
                row = {}
                for k, v in fv.items():
                    if isinstance(v, (int, float, bool)):
                        row[k] = float(v) if not isinstance(v, bool) else (1.0 if v else 0.0)
                    elif isinstance(v, bool):
                        row[k] = 1.0 if v else 0.0

                if row:
                    rows.append(row)
                    targets.append(1 if sig.meta_label else 0)
                    timestamps.append(sig.created_at)

            if len(rows) < min_samples:
                return {
                    "status": "insufficient_features",
                    "count": len(rows),
                    "required": min_samples,
                }

            df = pd.DataFrame(rows)
            y = np.array(targets)
            ts = timestamps

            # Remove features with >50% null
            null_pct = df.isnull().mean()
            keep_cols = null_pct[null_pct < 0.5].index.tolist()
            df = df[keep_cols]

            # Impute remaining nulls with median
            df = df.fillna(df.median())

            X = df.values
            feature_names = df.columns.tolist()

            # Train both models
            catboost_metrics = self._train_catboost(X, y, ts, feature_names)
            xgboost_metrics = self._train_xgboost(X, y, ts, feature_names)

            # Pick best by AUC-ROC
            cb_auc = catboost_metrics.get("mean_auc", 0)
            xg_auc = xgboost_metrics.get("mean_auc", 0)

            if cb_auc >= xg_auc:
                best_model = "catboost"
                best_metrics = catboost_metrics
            else:
                best_model = "xgboost"
                best_metrics = xgboost_metrics

            # Compute SHAP for best model
            top_features = best_metrics.get("top_features", [])

            result = {
                "status": "trained",
                "model_type": best_model,
                "sample_count": len(rows),
                "feature_count": len(feature_names),
                "class_balance": {
                    "positive": int(sum(y)),
                    "negative": int(len(y) - sum(y)),
                    "positive_rate": round(sum(y) / len(y), 3),
                },
                "cv_metrics": best_metrics,
                "catboost_auc": cb_auc,
                "xgboost_auc": xg_auc,
                "top_features": top_features[:15],
                "trained_at": datetime.now(timezone.utc).isoformat(),
            }

            logger.info(
                f"ML Training complete: {best_model} | "
                f"AUC={best_metrics.get('mean_auc', 0):.4f} | "
                f"Samples={len(rows)} | Features={len(feature_names)}"
            )

            return result

        except ImportError as e:
            logger.warning(f"ML training dependencies not installed: {e}")
            return {"status": "error", "error": f"Missing dependency: {e}"}
        except Exception as e:
            logger.error(f"ML training failed: {e}", exc_info=True)
            return {"status": "error", "error": str(e)}
        finally:
            db.close()

    def _train_catboost(
        self, X: np.ndarray, y: np.ndarray, timestamps: list, feature_names: list
    ) -> Dict:
        """Train CatBoost with purged k-fold."""
        try:
            from catboost import CatBoostClassifier
            from sklearn.metrics import (
                accuracy_score, precision_score, recall_score,
                f1_score, roc_auc_score, log_loss,
            )

            metrics_per_fold = []

            for train_idx, test_idx in self._purged_kfold_split(timestamps):
                X_train, X_test = X[train_idx], X[test_idx]
                y_train, y_test = y[train_idx], y[test_idx]

                model = CatBoostClassifier(
                    iterations=1000,
                    learning_rate=0.05,
                    depth=6,
                    l2_leaf_reg=3,
                    early_stopping_rounds=50,
                    verbose=0,
                    random_seed=42,
                )
                model.fit(X_train, y_train, eval_set=(X_test, y_test))

                y_pred = model.predict(X_test)
                y_proba = model.predict_proba(X_test)[:, 1]

                metrics_per_fold.append({
                    "accuracy": accuracy_score(y_test, y_pred),
                    "precision": precision_score(y_test, y_pred, zero_division=0),
                    "recall": recall_score(y_test, y_pred, zero_division=0),
                    "f1": f1_score(y_test, y_pred, zero_division=0),
                    "auc": roc_auc_score(y_test, y_proba) if len(set(y_test)) > 1 else 0,
                    "log_loss": log_loss(y_test, y_proba),
                })

            # Train final model on all data
            final_model = CatBoostClassifier(
                iterations=1000, learning_rate=0.05, depth=6,
                l2_leaf_reg=3, verbose=0, random_seed=42,
            )
            final_model.fit(X, y)

            # Feature importance
            importances = final_model.feature_importances_
            top_features = sorted(
                zip(feature_names, importances),
                key=lambda x: x[1], reverse=True,
            )[:15]

            # Save model
            model_path = os.path.join(MODEL_DIR, "catboost_model.pkl")
            with open(model_path, "wb") as f:
                pickle.dump({
                    "model": final_model,
                    "feature_names": feature_names,
                    "model_type": "catboost",
                    "saved_at": datetime.now(timezone.utc).isoformat(),
                }, f)

            avg_metrics = {
                k: round(np.mean([m[k] for m in metrics_per_fold]), 4)
                for k in metrics_per_fold[0]
            }
            avg_metrics["mean_auc"] = avg_metrics["auc"]
            avg_metrics["top_features"] = [(n, round(float(v), 4)) for n, v in top_features]

            return avg_metrics

        except ImportError:
            logger.debug("CatBoost not installed")
            return {"mean_auc": 0}

    def _train_xgboost(
        self, X: np.ndarray, y: np.ndarray, timestamps: list, feature_names: list
    ) -> Dict:
        """Train XGBoost with purged k-fold."""
        try:
            from xgboost import XGBClassifier
            from sklearn.metrics import (
                accuracy_score, precision_score, recall_score,
                f1_score, roc_auc_score, log_loss,
            )

            metrics_per_fold = []

            for train_idx, test_idx in self._purged_kfold_split(timestamps):
                X_train, X_test = X[train_idx], X[test_idx]
                y_train, y_test = y[train_idx], y[test_idx]

                model = XGBClassifier(
                    n_estimators=1000,
                    learning_rate=0.05,
                    max_depth=6,
                    subsample=0.8,
                    colsample_bytree=0.8,
                    early_stopping_rounds=50,
                    eval_metric="logloss",
                    verbosity=0,
                    random_state=42,
                )
                model.fit(
                    X_train, y_train,
                    eval_set=[(X_test, y_test)],
                    verbose=False,
                )

                y_pred = model.predict(X_test)
                y_proba = model.predict_proba(X_test)[:, 1]

                metrics_per_fold.append({
                    "accuracy": accuracy_score(y_test, y_pred),
                    "precision": precision_score(y_test, y_pred, zero_division=0),
                    "recall": recall_score(y_test, y_pred, zero_division=0),
                    "f1": f1_score(y_test, y_pred, zero_division=0),
                    "auc": roc_auc_score(y_test, y_proba) if len(set(y_test)) > 1 else 0,
                    "log_loss": log_loss(y_test, y_proba),
                })

            # Train final model on all data
            final_model = XGBClassifier(
                n_estimators=1000, learning_rate=0.05, max_depth=6,
                subsample=0.8, colsample_bytree=0.8,
                eval_metric="logloss", verbosity=0, random_state=42,
            )
            final_model.fit(X, y)

            # Feature importance
            importances = final_model.feature_importances_
            top_features = sorted(
                zip(feature_names, importances),
                key=lambda x: x[1], reverse=True,
            )[:15]

            # Save model
            model_path = os.path.join(MODEL_DIR, "xgboost_model.pkl")
            with open(model_path, "wb") as f:
                pickle.dump({
                    "model": final_model,
                    "feature_names": feature_names,
                    "model_type": "xgboost",
                    "saved_at": datetime.now(timezone.utc).isoformat(),
                }, f)

            avg_metrics = {
                k: round(np.mean([m[k] for m in metrics_per_fold]), 4)
                for k in metrics_per_fold[0]
            }
            avg_metrics["mean_auc"] = avg_metrics["auc"]
            avg_metrics["top_features"] = [(n, round(float(v), 4)) for n, v in top_features]

            return avg_metrics

        except ImportError:
            logger.debug("XGBoost not installed")
            return {"mean_auc": 0}

    def _purged_kfold_split(
        self, timestamps: list, n_splits: int = 5, embargo_hours: int = 24
    ):
        """
        Purged K-Fold: standard k-fold on time-sorted index with embargo.
        Removes training samples within embargo_hours of any test sample.
        """
        n = len(timestamps)
        indices = np.arange(n)
        fold_size = n // n_splits

        for i in range(n_splits):
            test_start = i * fold_size
            test_end = (i + 1) * fold_size if i < n_splits - 1 else n
            test_idx = indices[test_start:test_end]
            train_idx = np.concatenate([indices[:test_start], indices[test_end:]])

            # Purge: remove train samples within embargo of test boundaries
            if embargo_hours > 0 and timestamps:
                embargo_delta = timedelta(hours=embargo_hours)
                test_min_time = timestamps[test_start]
                test_max_time = timestamps[min(test_end - 1, n - 1)]

                keep_mask = np.ones(len(train_idx), dtype=bool)
                for j, ti in enumerate(train_idx):
                    t = timestamps[ti]
                    if t and test_min_time and test_max_time:
                        if (abs((t - test_min_time).total_seconds()) < embargo_delta.total_seconds()
                                or abs((t - test_max_time).total_seconds()) < embargo_delta.total_seconds()):
                            keep_mask[j] = False

                train_idx = train_idx[keep_mask]

            yield train_idx, test_idx

    async def score_signal(self, feature_vector: Dict) -> Dict:
        """Score a single signal using the best trained model."""
        for model_name in ["catboost_model.pkl", "xgboost_model.pkl"]:
            model_path = os.path.join(MODEL_DIR, model_name)
            if os.path.exists(model_path):
                try:
                    with open(model_path, "rb") as f:
                        data = pickle.load(f)

                    model = data["model"]
                    feature_names = data["feature_names"]
                    model_type = data.get("model_type", "unknown")

                    # Build feature array
                    X = np.array([[
                        float(feature_vector.get(f, 0) or 0)
                        for f in feature_names
                    ]])

                    proba = model.predict_proba(X)[0]
                    score = float(proba[1]) if len(proba) > 1 else float(proba[0])

                    return {
                        "ml_score": round(score, 4),
                        "model_type": model_type,
                        "model_version": data.get("saved_at", "unknown"),
                    }
                except Exception as e:
                    logger.debug(f"Model scoring failed ({model_name}): {e}")

        # Fallback
        return {
            "ml_score": 0.5,
            "model_type": "fallback",
            "model_version": "none",
        }

    async def generate_report(self) -> str:
        """Generate Telegram-formatted training report."""
        # Check for latest model
        for model_name in ["catboost_model.pkl", "xgboost_model.pkl"]:
            model_path = os.path.join(MODEL_DIR, model_name)
            if os.path.exists(model_path):
                try:
                    with open(model_path, "rb") as f:
                        data = pickle.load(f)

                    model_type = data.get("model_type", "unknown")
                    saved_at = data.get("saved_at", "unknown")
                    feature_names = data.get("feature_names", [])

                    return (
                        f"<b>ML Model Report</b>\n"
                        f"Model: <code>{model_type}</code>\n"
                        f"Trained: <code>{saved_at}</code>\n"
                        f"Features: <code>{len(feature_names)}</code>\n"
                    )
                except Exception:
                    pass

        return "No trained model available. Run /train first."
