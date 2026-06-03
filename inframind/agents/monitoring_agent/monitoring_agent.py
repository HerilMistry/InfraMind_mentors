from monitoring_agent.monitor import (
    Monitor
)

from monitoring_agent.anomaly_detector import (
    AnomalyDetector
)

from shared.event_bus import Event


class MonitoringAgent:

    def __init__(self,context_store,event_bus):

        self.context = context_store

        self.event_bus = event_bus

        self.monitor = Monitor()

        self.detector = (
            AnomalyDetector()
        )

    def run(self):

        metrics = (
            self.monitor.collect_metrics()
        )

        self.context.update_metrics(
            metrics
        )

        alerts = (
            self.detector.detect(
                metrics
            )
        )

        for alert in alerts:

            self.context.add_event(
                alert
            )

            self.event_bus.publish(
                Event(alert)
            )