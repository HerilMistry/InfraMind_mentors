"""
mlops/retraining/validator.py
Validates a trained model on a held-out portion of the dataset.
"""
import logging
from pathlib import Path
from typing import Dict, Any

import numpy as np
from sklearn.metrics import (
    accuracy_score, f1_score, precision_score, recall_score
)
from sklearn.model_selection import train_test_split

from mlops.retraining.trainer import load_data

log = logging.getLogger(__name__)


def validate_model(model, data_path: Path) -> Dict[str, Any]:
    """Run validation and return a metrics dict."""
    X, y = load_data(data_path)
    _, X_val, _, y_val = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y if y.sum() > 0 else None
    )

    raw_preds = model.predict(X_val)
    y_pred = np.where(raw_preds == -1, 1, 0)

    metrics = {
        "val_accuracy":  float(accuracy_score(y_val, y_pred)),
        "val_f1":        float(f1_score(y_val, y_pred, zero_division=0)),
        "val_precision": float(precision_score(y_val, y_pred, zero_division=0)),
        "val_recall":    float(recall_score(y_val, y_pred, zero_division=0)),
    }

    log.info("Validator done — val_f1=%.3f", metrics["val_f1"])
    return metrics
