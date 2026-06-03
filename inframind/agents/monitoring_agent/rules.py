MEMORY_THRESHOLD = 0.1
DRIFT_THRESHOLD = 0.1


def evaluate_rules(metrics: dict):
    alerts = []

    memory = metrics.get("memory_pressure")
    drift = metrics.get("drift")

    if memory and memory > MEMORY_THRESHOLD:
        alerts.append("HIGH_MEMORY_PRESSURE")

    if drift and drift > DRIFT_THRESHOLD:
        alerts.append("MODEL_DRIFT_DETECTED")

    return alerts