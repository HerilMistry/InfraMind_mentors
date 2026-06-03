class DiagnosisEngine:

    def diagnose(
        self,
        event_type,
        logs
    ):

        if event_type == "HIGH_MEMORY_PRESSURE":

            return (
                "Pod memory consumption exceeded threshold"
            )

        if event_type == "MODEL_DRIFT_DETECTED":

            return (
                "Prediction drift score exceeded threshold"
            )

        return (
            "Unknown root cause"
        )