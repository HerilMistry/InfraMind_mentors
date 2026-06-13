"""
mlops/versioning/model_registry.py
Utility helpers for querying the MLflow model registry.
Used by the inference service to load the latest Production model.
"""
import os
import logging
import mlflow
import mlflow.sklearn
from mlflow.tracking import MlflowClient

log = logging.getLogger(__name__)

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
MODEL_NAME          = os.getenv("MLFLOW_MODEL_NAME", "inframind-anomaly-detector")


def get_latest_model(stage: str = "Staging"):
    """Load and return the latest model in the given stage."""
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = MlflowClient()

    versions = client.get_latest_versions(MODEL_NAME, stages=[stage])
    if not versions:
        raise RuntimeError(f"No model versions found for '{MODEL_NAME}' in stage '{stage}'")

    latest = versions[0]
    log.info("Loading model: %s v%s from stage=%s", MODEL_NAME, latest.version, stage)
    model_uri = f"models:/{MODEL_NAME}/{stage}"
    return mlflow.sklearn.load_model(model_uri)


def promote_to_production(version: str) -> None:
    """Transition a specific version to Production."""
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    client = MlflowClient()
    client.transition_model_version_stage(
        name=MODEL_NAME,
        version=version,
        stage="Production",
    )
    log.info("Promoted model %s v%s to Production.", MODEL_NAME, version)
