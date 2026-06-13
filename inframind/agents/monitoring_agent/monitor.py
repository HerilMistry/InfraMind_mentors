import os
import requests

# Use environment variable for Prometheus URL, useful for Kubernetes deployment
PROM_URL = os.environ.get("PROM_URL", "http://localhost:9090")

class Monitor:

    def query(self, metric):

        try:
            r = requests.get(
            f"{PROM_URL}/api/v1/query",
            params={"query": metric},
            timeout=5
            )

            r.raise_for_status()

            data = r.json()

            results = data["data"]["result"]

            if not results:
                return None

            return float(results[0]["value"][1])

        except Exception as e:
            print(f"Prometheus query failed: {e}")
            return None
        
    def collect_metrics(self):

        return {
            "latency":
                self.query(
                    "rate(inference_latency_seconds_sum[1m])"
                ),

            "failed_requests":
                self.query(
                    "inference_failed_requests_total"
                ),

            "memory_pressure":
                self.query(
                    "memory_pressure_score"
                ),

            "drift":
                self.query(
                    "prediction_drift_score"
                )
        }