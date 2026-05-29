class RootCauseAnalyzer:
    def analyze(self, anomaly_report: dict):
        alerts = anomaly_report.get("alerts", [])

        causes = []

        if "HIGH_MEMORY_PRESSURE" in alerts:
            causes.append("Possible memory overload or traffic spike")

        if "MODEL_DRIFT_DETECTED" in alerts:
            causes.append("Prediction distribution shift detected")

        return {
            "root_causes": causes
        }