"""Data models for the Optimization Agent.

Provides strongly‑typed request and response schemas using Pydantic.
"""

from __future__ import annotations

from typing import Literal, Mapping

from pydantic import BaseModel, Field, PositiveInt, conint, confloat

# ---------------------------------------------------------------------------
# Enums / Literal Types
# ---------------------------------------------------------------------------

# Supported anomaly types
AnomalyType = Literal[
    "RESOURCE_PRESSURE",
    "LATENCY_ANOMALY",
    "FAILED_REQUEST_SPIKE",
    "DRIFT_DETECTED",
    "LOG_ERROR_SPIKE",
    "OOM_KILLED",
    "POD_CRASH_LOOP",
    "HIGH_CPU_UTILIZATION",
]

# Possible remediation actions
RemediationAction = Literal[
    "SCALE_UP_REPLICAS",
    "SCALE_DOWN_REPLICAS",
    "RESTART_PODS",
    "ROLLBACK_DEPLOYMENT",
    "DEPLOY_LIGHTWEIGHT_MODEL",
    "TRIGGER_RETRAINING",
    "INCREASE_RESOURCES",
    "NO_ACTION",
]

# ---------------------------------------------------------------------------
# Request model
# ---------------------------------------------------------------------------

class Metrics(BaseModel):
    """Metrics collected for a given inference deployment.

    Types are intentionally narrow to give clear validation errors.
    """

    cpu: conint(ge=0, le=100) = Field(..., description="CPU utilisation as a percentage")
    memory: conint(ge=0, le=100) = Field(..., description="Memory utilisation as a percentage")
    latency_p95: confloat(ge=0.0) = Field(..., description="95th percentile latency in seconds")
    error_rate: confloat(ge=0.0, le=1.0) = Field(..., description="Error rate as a fraction (0‑1)")


class DeploymentMetadata(BaseModel):
    """Optional deployment‑specific metadata.

    The model is open‑ended; any additional key/value pairs are accepted.
    """

    name: str = Field(..., description="Kubernetes deployment name")
    namespace: str = Field(default="default", description="K8s namespace")
    extra: Mapping[str, str] | None = Field(
        default=None,
        description="Arbitrary free‑form metadata that may be useful for future extensions.",
    )


class OptimizationRequest(BaseModel):
    """Input payload for the Optimization Agent."""

    anomaly_type: AnomalyType = Field(..., description="Type of detected anomaly")
    root_cause: str = Field(..., description="Human readable root cause description")
    metrics: Metrics = Field(..., description="Current infrastructure metrics")
    deployment_metadata: DeploymentMetadata = Field(..., description="Metadata about the deployment")


# ---------------------------------------------------------------------------
# Response model
# ---------------------------------------------------------------------------

class OptimizationResponse(BaseModel):
    """Agent output – a recommended remediation action.

    ``confidence`` is a value between 0 and 1 indicating how certain the
    rule‑based engine is about the recommendation.
    """

    recommended_action: RemediationAction = Field(..., description="Chosen remediation action")
    confidence: confloat(ge=0.0, le=1.0) = Field(..., description="Confidence score (0‑1)")
    reasoning: str = Field(..., description="Human‑readable explanation of the decision")

"""End of file."""
