
import os
import sys
import logging
import mlflow
import mlflow.sklearn
from pathlib import Path

# Allow sibling imports
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from mlops.retraining.trainer import train_model
from mlops.retraining.validator import validate_model

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger(__name__)

MLFLOW_TRACKING_URI = os.getenv("MLFLOW_TRACKING_URI", "http://localhost:5000")
EXPERIMENT_NAME     = os.getenv("MLFLOW_EXPERIMENT", "inframind-anomaly-detection")
MODEL_NAME          = os.getenv("MLFLOW_MODEL_NAME", "inframind-anomaly-detector")
DATA_PATH           = Path(__file__).resolve().parents[2] / "data" / "metrics_dataset.csv"


def run_pipeline() -> None:
    mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
    mlflow.set_experiment(EXPERIMENT_NAME)

    log.info("Starting retraining pipeline for experiment: %s", EXPERIMENT_NAME)

    with mlflow.start_run() as run:
        run_id = run.info.run_id
        log.info("MLflow run_id: %s", run_id)

        # ── 1. Train ──────────────────────────────────────────────────────
        log.info("Training model on dataset: %s", DATA_PATH)
        model, train_metrics = train_model(DATA_PATH)

        mlflow.log_params({
            "data_path": str(DATA_PATH),
            "n_estimators": train_metrics.get("n_estimators", 100),
            "max_features": train_metrics.get("max_features", "sqrt"),
        })
        mlflow.log_metrics({
            "train_accuracy": train_metrics["train_accuracy"],
            "train_f1":       train_metrics["train_f1"],
        })
        log.info("Train metrics: %s", train_metrics)

        # ── 2. Validate ───────────────────────────────────────────────────
        log.info("Validating model...")
        val_metrics = validate_model(model, DATA_PATH)

        mlflow.log_metrics({
            "val_accuracy":  val_metrics["val_accuracy"],
            "val_f1":        val_metrics["val_f1"],
            "val_precision": val_metrics["val_precision"],
            "val_recall":    val_metrics["val_recall"],
        })
        log.info("Validation metrics: %s", val_metrics)

        # ── 3. Quality gate ───────────────────────────────────────────────
        if val_metrics["val_f1"] < 0.70:
            log.error("Quality gate failed (F1=%.3f < 0.70). Model NOT registered.", val_metrics["val_f1"])
            sys.exit(1)

        # ── 4. Register ───────────────────────────────────────────────────
        log.info("Registering model as: %s", MODEL_NAME)
        model_uri = f"runs:/{run_id}/model"
        mlflow.sklearn.log_model(model, "model")
        mv = mlflow.register_model(model_uri, MODEL_NAME)
        log.info("Model registered → version %s", mv.version)

        # Transition to Staging automatically
        client = mlflow.MlflowClient()
        client.transition_model_version_stage(
            name=MODEL_NAME,
            version=mv.version,
            stage="Staging",
        )
        log.info("Model transitioned to Staging.")

    log.info("Pipeline complete.")


if __name__ == "__main__":
    run_pipeline()
