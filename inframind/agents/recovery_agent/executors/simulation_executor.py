"""Simulation executor used for dry‑run and unit‑test scenarios.
It does **not** talk to a real cluster; instead it returns a deterministic
:class:`ExecutionReport` that mirrors what a real executor *would* report.
"""

from __future__ import annotations

import logging
from typing import Any

from ..models import Recommendation, ExecutionReport, RecommendedAction
from .base_executor import ActionExecutor

logger = logging.getLogger(__name__)


class SimulationExecutor(ActionExecutor):
    """Fake executor that pretends to carry out the action.

    The purpose is to enable safe testing of the ``RecoveryManager`` without
    requiring a Kubernetes cluster or the ``kubernetes`` Python client.
    """

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
