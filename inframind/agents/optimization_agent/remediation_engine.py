"""Rule‑based remediation engine for the Optimization Agent.

The engine receives an :class:`OptimizationRequest` and returns an
:class:`OptimizationResponse`.  It is deliberately simple – a dictionary
maps each ``anomaly_type`` to a list of candidate actions ordered by
confidence.  The ``root_cause`` and ``metrics`` are inspected to adjust the
confidence score and to provide a human‑readable *reasoning* string.

The design is intentionally extensible: new anomaly types, actions, or a
switch to an LLM‑driven strategy can be added by extending the
``RULES`` dictionary or by subclassing :class:`BaseRemediationEngine`.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Dict, List, Tuple

from pydantic import ValidationError

from .models import (
    AnomalyType,
    OptimizationRequest,
    OptimizationResponse,
    RemediationAction,
)

# ---------------------------------------------------------------------------
# Logging configuration – production‑grade but lightweight
# ---------------------------------------------------------------------------

logger = logging.getLogger(__name__)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Rule definition
# ---------------------------------------------------------------------------

# Each entry: (action, base_confidence, reasoning_template)
# ``reasoning_template`` may reference ``metrics`` fields via ``{field}``
# and ``root_cause`` via ``{root_cause}``.

RULES: Dict[AnomalyType, List[Tuple[RemediationAction, float, str]]] = {
    "RESOURCE_PRESSURE": [
        (
            "INCREASE_RESOURCES",
            0.92,
            "High {resource} usage ({value}%) indicates resource pressure.",
        ),
        ("SCALE_UP_REPLICAS", 0.88, "Scaling up replicas to relieve pressure."),
    ],
    "LATENCY_ANOMALY": [
        (
            "SCALE_UP_REPLICAS",
            0.90,
            "Latency p95 is {latency_p95}s, suggesting capacity limits.",
        ),
        ("DEPLOY_LIGHTWEIGHT_MODEL", 0.80, "Consider deploying a lighter model.")
    ],
    "FAILED_REQUEST_SPIKE": [
        ("RESTART_PODS", 0.85, "Spike in failed requests ({error_rate*100:.1f}%)."),
        ("SCALE_UP_REPLICAS", 0.78, "Add capacity to handle load.")
    ],
    "DRIFT_DETECTED": [
        ("TRIGGER_RETRAINING", 0.95, "Data drift detected – retraining required."),
    ],
    "LOG_ERROR_SPIKE": [
        ("RESTART_PODS", 0.80, "Error log spike observed, restart may clear state."),
    ],
    "OOM_KILLED": [
        ("INCREASE_RESOURCES", 0.93, "Pod killed due to OOM – raise memory limits."),
    ],
    "POD_CRASH_LOOP": [
        ("RESTART_PODS", 0.88, "Crash‑loop back‑off detected – restart pods.")
    ],
    "HIGH_CPU_UTILIZATION": [
        ("SCALE_UP_REPLICAS", 0.89, "CPU utilisation at {cpu}% – add replicas."),
        ("INCREASE_RESOURCES", 0.75, "Consider raising CPU limits.")
    ],
}


@dataclass
class RuleMatch:
    action: RemediationAction
    confidence: float
    reasoning: str


class BaseRemediationEngine:
    """Base class for remediation engines.

    Sub‑class this if you need a more sophisticated strategy (e.g. LLM).
    """

    def decide(self, request: OptimizationRequest) -> OptimizationResponse:
        raise NotImplementedError


class RuleBasedRemediationEngine(BaseRemediationEngine):
    """Concrete implementation that uses the ``RULES`` mapping.

    The algorithm:
    1. Look‑up the rule list for ``request.anomaly_type``.
    2. For each candidate, optionally adjust confidence using metric
       thresholds (simple heuristics).
    3. Choose the candidate with the highest final confidence.
    4. Format a human readable reasoning string.
    """

    def __init__(self) -> None:
        self.rules = RULES
        logger.info("RuleBasedRemediationEngine initialised with %d anomaly types", len(self.rules))

    def _adjust_confidence(self, rule: Tuple[RemediationAction, float, str], req: OptimizationRequest) -> RuleMatch:
        action, base_conf, tmpl = rule
        # Simple heuristics – increase confidence when metrics are extreme
        confidence = base_conf
        # Example: if memory > 90% boost confidence for memory‑related actions
        if "memory" in tmpl and req.metrics.memory > 90:
            confidence = min(1.0, confidence + 0.05)
        if "cpu" in tmpl and req.metrics.cpu > 85:
            confidence = min(1.0, confidence + 0.04)
        # Populate template placeholders
        reasoning = tmpl.format(
            resource="memory" if "memory" in tmpl else "cpu",
            value=req.metrics.memory if "memory" in tmpl else req.metrics.cpu,
            latency_p95=req.metrics.latency_p95,
            error_rate=req.metrics.error_rate,
            root_cause=req.root_cause,
            cpu=req.metrics.cpu,
        )
        return RuleMatch(action=action, confidence=confidence, reasoning=reasoning)

    def decide(self, request: OptimizationRequest) -> OptimizationResponse:
        logger.debug("Received optimization request: %s", request.json())
        try:
            _ = request
        except ValidationError as exc:
            logger.error("Invalid request: %s", exc)
            raise

        candidates = self.rules.get(request.anomaly_type, [])
        if not candidates:
            logger.warning("No rules defined for anomaly %s – returning NO_ACTION", request.anomaly_type)
            return OptimizationResponse(
                recommended_action="NO_ACTION",
                confidence=0.0,
                reasoning=f"No remediation rule for anomaly type {request.anomaly_type}.",
            )

        matches = [self._adjust_confidence(rule, request) for rule in candidates]
        best = max(matches, key=lambda m: m.confidence)
        logger.info(
            "Selected action %s with confidence %.2f for anomaly %s",
            best.action,
            best.confidence,
            request.anomaly_type,
        )
        return OptimizationResponse(
            recommended_action=best.action,
            confidence=round(best.confidence, 2),
            reasoning=best.reasoning,
        )

# Export a ready‑to‑use engine instance for simple imports
engine = RuleBasedRemediationEngine()

__all__ = ["engine", "RuleBasedRemediationEngine", "BaseRemediationEngine"]
