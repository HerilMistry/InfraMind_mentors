

from __future__ import annotations

import logging
from typing import Any

from ..models import Recommendation, ExecutionReport, RecommendedAction
from .base_executor import ActionExecutor

logger = logging.getLogger(__name__)


class SimulationExecutor(ActionExecutor):
    

    def execute(self, recommendation: Recommendation) -> ExecutionReport:
        action = recommendation.recommended_action
        logger.info("Simulating execution of %s", action)
        # Simple deterministic detail message – in a real system this could be
        # richer (e.g., echo back the reasoning).
        details = f"[SIMULATION] Action {action} would have been performed."
        return ExecutionReport(
            status="SUCCESS",
            action=action,
            details=details,
        )
