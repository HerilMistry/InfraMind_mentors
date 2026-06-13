
import logging
from pathlib import Path
from typing import Tuple, Dict, Any

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.metrics import f1_score, accuracy_score
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.pipeline import Pipeline

log = logging.getLogger(__name__)

FEATURE_COLS = [
    "cpu_usage_pct",
    "memory_pressure",
    "latency_ms",
    "error_rate",
    "request_rate",
    "pod_restarts",
]


def load_data(data_path: Path) -> Tuple[np.ndarray, np.ndarray]:
    
    df = pd.read_csv(data_path)

    # Fill any missing values
    df[FEATURE_COLS] = df[FEATURE_COLS].fillna(df[FEATURE_COLS].median())

    X = df[FEATURE_COLS].values
    # label column: 1 = anomaly, 0 = normal
    y = df["is_anomaly"].values if "is_anomaly" in df.columns else np.zeros(len(df))
    return X, y


def train_model(data_path: Path) -> Tuple[Pipeline, Dict[str, Any]]:
    
    X, y = load_data(data_path)
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y if y.sum() > 0 else None
    )

    pipeline = Pipeline([
        ("scaler", StandardScaler()),
        ("clf", IsolationForest(
            n_estimators=100,
            contamination=0.1,
            random_state=42,
            n_jobs=-1,
        )),
    ])

    pipeline.fit(X_train)

    # IsolationForest returns -1 for anomalies, 1 for normal → convert
    raw_preds = pipeline.predict(X_test)
    y_pred = np.where(raw_preds == -1, 1, 0)

    metrics = {
        "n_estimators":   100,
        "max_features":   "sqrt",
        "train_accuracy": float(accuracy_score(y_test, y_pred)),
        "train_f1":       float(f1_score(y_test, y_pred, zero_division=0)),
    }

    log.info("Trainer done — train_f1=%.3f", metrics["train_f1"])
    return pipeline, metrics
