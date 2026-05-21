from prometheus_client import Counter, Histogram, Gauge, generate_latest
from fastapi.responses import Response
import random

REQUEST_COUNT = Counter("inference_requests_total", "Total inference requests")
FAILED_REQUESTS = Counter("inference_failed_requests_total", "Failed inference requests")
LATENCY = Histogram("inference_latency_seconds", "Inference latency")
MEMORY_PRESSURE = Gauge("memory_pressure_score", "Simulated memory pressure")
DRIFT_SCORE = Gauge("prediction_drift_score", "Simulated prediction drift score")

def metrics_response():
    MEMORY_PRESSURE.set(random.uniform(0.2, 0.95))
    DRIFT_SCORE.set(random.uniform(0.0, 1.0))
    return Response(generate_latest(), media_type="text/plain")