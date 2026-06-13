from pydantic import BaseModel, Field
from typing import Dict, Any

class Anomaly(BaseModel):
    anomaly_type: str = Field(..., description="Type of detected anomaly")
    metrics: Dict[str, Any] = Field(..., description="Metrics associated with the anomaly")

class RootCause(BaseModel):
    root_cause: str = Field(..., description="Diagnosed root cause")
    anomaly: Anomaly = Field(..., description="The anomaly that triggered this root cause")
