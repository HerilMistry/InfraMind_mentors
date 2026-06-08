import pytest
from optimization_agent.models import OptimizationRequest, Metrics, DeploymentMetadata
from optimization_agent.remediation_engine import engine


def make_request(anomaly_type, cpu=50, memory=50, latency_p95=0.5, error_rate=0.0):
    return OptimizationRequest(
        anomaly_type=anomaly_type,
        root_cause="Test root cause",
        metrics=Metrics(
            cpu=cpu,
            memory=memory,
            latency_p95=latency_p95,
            error_rate=error_rate,
        ),
        deployment_metadata=DeploymentMetadata(
            name="test-deployment",
            namespace="default",
        ),
    )


@pytest.mark.parametrize(
    "anomaly,expected_action",
    [
        ("RESOURCE_PRESSURE", "INCREASE_RESOURCES"),
        ("LATENCY_ANOMALY", "SCALE_UP_REPLICAS"),
        ("FAILED_REQUEST_SPIKE", "RESTART_PODS"),
        ("DRIFT_DETECTED", "TRIGGER_RETRAINING"),
        ("LOG_ERROR_SPIKE", "RESTART_PODS"),
        ("OOM_KILLED", "INCREASE_RESOURCES"),
        ("POD_CRASH_LOOP", "RESTART_PODS"),
        ("HIGH_CPU_UTILIZATION", "SCALE_UP_REPLICAS"),
    ],
)
def test_remediation_engine_selects_correct_action(anomaly, expected_action):
    # Use values that trigger confidence boosts where applicable
    request = make_request(
        anomaly_type=anomaly,
        cpu=95 if anomaly == "HIGH_CPU_UTILIZATION" else 50,
        memory=95 if anomaly == "RESOURCE_PRESSURE" else 50,
        latency_p95=1.5 if anomaly == "LATENCY_ANOMALY" else 0.3,
        error_rate=0.1 if anomaly == "FAILED_REQUEST_SPIKE" else 0.0,
    )
    response = engine.decide(request)
    assert response.recommended_action == expected_action
    assert 0.0 <= response.confidence <= 1.0
    assert response.reasoning
