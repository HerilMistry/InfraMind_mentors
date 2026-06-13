from shared.event_bus import Event
from shared.models import RootCause
from optimization_agent.remediation_engine import engine
from optimization_agent.models import OptimizationRequest, Metrics, DeploymentMetadata
from recovery_agent.models import Recommendation, RecommendedAction


class OptimizationAgent:

    def __init__(
        self,
        context_store,
        event_bus
    ):

        self.context = context_store
        self.event_bus = event_bus

        self.event_bus.subscribe(
            "ROOT_CAUSE_FOUND",
            self.optimize
        )

    def optimize(
        self,
        event
    ):

        print("Optimization started...")

        root_cause_obj = event.payload if isinstance(event.payload, RootCause) else None
        
        anomaly_type_raw = "LATENCY_ANOMALY"
        raw_metrics = {}
        root_cause_str = "Unknown"
        
        if root_cause_obj:
            root_cause_str = root_cause_obj.root_cause
            if root_cause_obj.anomaly:
                anomaly_type_raw = root_cause_obj.anomaly.anomaly_type
                raw_metrics = root_cause_obj.anomaly.metrics
        
        # Mapping simple alert types to AnomalyType
        anomaly_mapping = {
            "HIGH_MEMORY_PRESSURE": "RESOURCE_PRESSURE",
            "MODEL_DRIFT_DETECTED": "DRIFT_DETECTED"
        }
        anomaly_type = anomaly_mapping.get(anomaly_type_raw, "LATENCY_ANOMALY")
        
        raw_metrics = payload.get("metrics", {})
        
        metrics = Metrics(
            cpu=int(raw_metrics.get("cpu", 0)),
            memory=int((raw_metrics.get("memory_pressure", 0) or 0) * 100),
            latency_p95=float(raw_metrics.get("latency", 0) or 0),
            error_rate=float(raw_metrics.get("failed_requests", 0) or 0)
        )
        
        metadata = DeploymentMetadata(
            name="inference-deployment",
            namespace="default"
        )
        
        request = OptimizationRequest(
            anomaly_type=anomaly_type,
            root_cause=root_cause_str,
            metrics=metrics,
            deployment_metadata=metadata
        )

        response = engine.decide(request)
        
        recommendation = Recommendation(
            recommended_action=RecommendedAction(response.recommended_action),
            confidence=response.confidence,
            reasoning=response.reasoning
        )

        self.context.set_last_action(response.recommended_action)

        completed = Event(
            event_type="OPTIMIZATION_COMPLETED",
            payload=recommendation
        )

        self.context.add_event(completed)
        self.event_bus.publish(completed)

        print(f"Optimization completed: {response.recommended_action}")