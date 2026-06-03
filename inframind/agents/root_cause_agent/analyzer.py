from shared.event_bus import Event

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
        )

    def analyze(
        self,
        event
    ):

        print(
            f"Received event: "
            f"{event.event_type}"
        )

        logs = self.log_parser.get_logs(
            namespace="default",
            pod_name="inference-pod"
        )

        root_cause = (
            self.diagnosis_engine.diagnose(
                event.event_type,
                logs
            )
        )

        self.context.set_root_cause(
            root_cause
        )

        root_event = Event(
            "ROOT_CAUSE_FOUND"
        )

        self.context.add_event(
            root_event
        )

        self.event_bus.publish(
            root_event
        )

        print(
            f"Root cause identified: "
            f"{root_cause}"
        )