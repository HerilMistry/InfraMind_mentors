from monitoring_agent.rules import evaluate_rules


class AnomalyDetector:
    def detect(self, metrics: dict):
        alerts = evaluate_rules(metrics)

        return {
            "anomaly_detected": len(alerts) > 0,
            "alerts": alerts,
            "metrics": metrics
        }