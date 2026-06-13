from shared.event_bus import Event
from shared.models import Anomaly, RootCause

from root_cause_agent.log_parser import (
    LogParser
)

from root_cause_agent.diagnosis_engine import (
    DiagnosisEngine
)


class RootCauseAgent:

    def __init__(
        self,
        context_store,
        event_bus
    ):

        self.context = context_store

        self.event_bus = event_bus

        self.log_parser = LogParser()

        self.diagnosis_engine = (
            DiagnosisEngine()
        )

        self.event_bus.subscribe(
            "HIGH_MEMORY_PRESSURE",
            self.analyze
        )

        self.event_bus.subscribe(
            "MODEL_DRIFT_DETECTED",
            self.analyze
    def analyze(
        self,
        event
    ):

        print(
            f"Received event: "
            f"{event.event_type}"
        )
        
        anomaly = event.payload if isinstance(event.payload, Anomaly) else None
        metrics = anomaly.metrics if anomaly else {}

        logs = self.log_parser.get_logs(
            namespace="default",
        )

        root_cause_str = (
            self.diagnosis_engine.diagnose(
                event.event_type,
                logs
            )
        )

        self.context.set_root_cause(
            root_cause_str
        )
        
        root_cause_obj = RootCause(root_cause=root_cause_str, anomaly=anomaly) if anomaly else None

        root_event = Event(
            event_type="ROOT_CAUSE_FOUND",
            payload=root_cause_obj
        )

        self.context.add_event(
            root_event
        )

        self.event_bus.publish(
            root_event
        )

        print(
            f"Root cause identified: "
            f"{root_cause_str}"
        )