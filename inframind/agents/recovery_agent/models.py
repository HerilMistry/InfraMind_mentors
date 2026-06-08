from __future__ import annotations

import enum
from datetime import datetime
from typing import Literal, Optional

from pydantic import BaseModel, Field, validator


class RecommendedAction(str, enum.Enum):
    SCALE_UP_REPLICAS = "SCALE_UP_REPLICAS"
    SCALE_DOWN_REPLICAS = "SCALE_DOWN_REPLICAS"
    RESTART_PODS = "RESTART_PODS"
    ROLLBACK_DEPLOYMENT = "ROLLBACK_DEPLOYMENT"
    DEPLOY_LIGHTWEIGHT_MODEL = "DEPLOY_LIGHTWEIGHT_MODEL"
    TRIGGER_RETRAINING = "TRIGGER_RETRAINING"
    INCREASE_RESOURCES = "INCREASE_RESOURCES"
    NO_ACTION = "NO_ACTION"


class Recommendation(BaseModel):
    recommended_action: RecommendedAction
    confidence: float = Field(..., ge=0.0, le=1.0)
    reasoning: str

    @validator("confidence")
    def confidence_min(cls, v):
        if v < 0.0 or v > 1.0:
            raise ValueError("confidence must be between 0 and 1")
        return v


class ExecutionStatus(str, enum.Enum):
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    SKIPPED = "SKIPPED"


class ExecutionReport(BaseModel):
    status: ExecutionStatus
    action: RecommendedAction
    executed_at: datetime = Field(default_factory=datetime.utcnow)
    details: str
    audit_id: Optional[str] = None

    class Config:
        json_encoders = {datetime: lambda v: v.isoformat()}
