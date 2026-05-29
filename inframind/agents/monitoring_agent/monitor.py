import requests

PROMETHEUS_URL = "http://localhost:9090"


class PrometheusMonitor:
    def __init__(self):
        self.base_url = PROMETHEUS_URL

    def query_metric(self, metric_name: str):
        url = f"{self.base_url}/api/v1/query"

        response = requests.get(
            url,
            params={"query": metric_name}
        )

        data = response.json()

        try:
            value = float(data["data"]["result"][0]["value"][1])
            return value
        except Exception:
            return None

    def collect_metrics(self):
        return {
            "memory_pressure": self.query_metric("memory_pressure_score"),
            "prediction_drift": self.query_metric("prediction_drift_score"),
            "requests": self.query_metric("inference_requests_total")
        }