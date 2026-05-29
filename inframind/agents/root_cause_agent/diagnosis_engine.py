class DiagnosisEngine:
    def summarize(self, causes: dict):
        return {
            "summary": " | ".join(causes.get("root_causes", []))
        }