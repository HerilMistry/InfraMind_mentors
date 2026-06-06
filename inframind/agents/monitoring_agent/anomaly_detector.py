from monitoring_agent.rules import evaluate_rules


class AnomalyDetector:

    def detect(self, metrics):
        return evaluate_rules(metrics)