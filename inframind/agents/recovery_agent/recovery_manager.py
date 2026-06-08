"""RecoveryManager – validates recommendations, enforces safety policies, and
executes remediation actions via the appropriate executor.
"""

from __future__ import annotations

import logging
import threading
from collections import deque
from datetime import datetime, timedelta
from typing import Deque, Optional

from ..models import Recommendation, ExecutionReport, ExecutionStatus
from ..executors.simulation_executor import SimulationExecutor
from ..executors.kubernetes_executor import KubernetesExecutor
from ..audit.logger import write_audit_entry

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Safety policy configuration – values can be overridden via environment
# variables if needed (not required for this exercise).
# ---------------------------------------------------------------------------
MAX_REPLICAS = 10
MAX_ROLLBACKS_PER_HOUR = 2
CONFIDENCE_THRESHOLD = 0.7
# Mutually exclusive actions – represented as sets for quick lookup
MUTUALLY_EXCLUSIVE = {frozenset(["ROLLBACK_DEPLOYMENT", "TRIGGER_RETRAINING"])}


class _RollbackTracker:
    """Thread‑safe tracker for roll‑backs performed in the last hour.

    It stores timestamps of each successful rollback and discards entries older
    than one hour. ``allow()`` returns ``True`` if the rate limit has not been
    exceeded.
    """

    def __init__(self) -> None:
        self._timestamps: Deque[datetime] = deque()
        self._lock = threading.Lock()

    def record(self) -> None:
        with self._lock:
            now = datetime.utcnow()
            self._timestamps.append(now)
            self._prune(now)

    def allow(self) -> bool:
        with self._lock:
            now = datetime.utcnow()
            self._prune(now)
            return len(self._timestamps) < MAX_ROLLBACKS_PER_HOUR

    def _prune(self, now: datetime) -> None:
        cutoff = now - timedelta(hours=1)
        while self._timestamps and self._timestamps[0] < cutoff:
            self._timestamps.popleft()


class RecoveryManager:
    """Core manager for the Recovery Agent.

    Parameters
    ----------
    context_store:
        An object providing ``set_last_action`` and ``add_event`` – the same
        interface used by other agents.
    event_bus:
        Shared :class:`EventBus` instance for publishing/subscribing.
    mode:
        Execution mode – ``"dry"`` (no side effects), ``"simulation"`` (returns
        deterministic results), or ``"live"`` (talks to a real Kubernetes
        cluster). Default is ``"simulation"`` for safety.
    """

    def __init__(self, context_store, event_bus, mode: str = "simulation") -> None:
        self.context = context_store
        self.event_bus = event_bus
        self.mode = mode.lower()
        if self.mode not in {"dry", "simulation", "live"}:
            raise ValueError(f"Unsupported mode {mode!r}. Choose dry, simulation, or live.")

        # Executors – simulation executor always available
        self.sim_executor = SimulationExecutor()
        self.k8s_executor: Optional[KubernetesExecutor] = None
        if self.mode == "live":
            try:
                self.k8s_executor = KubernetesExecutor()
            except Exception as exc:  # pragma: no cover – covered by unit tests via mock
                logger.error("Failed to initialise KubernetesExecutor: %s", exc)
                raise

        # Roll‑back rate‑limit tracker
        self._rollback_tracker = _RollbackTracker()

        # Subscribe to optimisation failure events (legacy path retained for
        # compatibility with existing RecoveryAgent implementation).
        self.event_bus.subscribe("OPTIMIZATION_FAILED", self.handle_event)

    # ---------------------------------------------------------------------
    # Public API – handle a recommendation payload
    # ---------------------------------------------------------------------
    def handle_recommendation(self, rec: Recommendation) -> ExecutionReport:
        """Validate, enforce policies, execute, audit, and return a report.
        """
        logger.debug("Received recommendation: %s", rec.json())

        # 1️⃣ Confidence check
        if rec.confidence < CONFIDENCE_THRESHOLD:
            logger.warning("Recommendation confidence %.2f below threshold %.2f – skipping",
                           rec.confidence, CONFIDENCE_THRESHOLD)
            report = ExecutionReport(
                status=ExecutionStatus.SKIPPED,
                action=rec.recommended_action,
                details="Confidence below required threshold.",
            )
            self._audit(rec, report)
            return report

        # 2️⃣ Safety policy checks
        safe, reason = self._enforce_safety(rec)
        if not safe:
            logger.warning("Safety policy prevented action %s: %s", rec.recommended_action, reason)
            report = ExecutionReport(
                status=ExecutionStatus.SKIPPED,
                action=rec.recommended_action,
                details=reason,
            )
            self._audit(rec, report)
            return report

        # 3️⃣ Choose executor based on mode
        if self.mode == "dry":
            executor = self.sim_executor
        elif self.mode == "simulation":
            executor = self.sim_executor
        else:  # live
            executor = self.k8s_executor  # type: ignore[assignment]

        # 4️⃣ Execute the action
        report = executor.execute(rec)

        # 5️⃣ Post‑execution handling (e.g., update rollback tracker)
        if rec.recommended_action == "ROLLBACK_DEPLOYMENT" and report.status == ExecutionStatus.SUCCESS:
            self._rollback_tracker.record()

        # 6️⃣ Audit the execution
        self._audit(rec, report)

        # 7️⃣ Propagate event for downstream agents (optional)
        completed_event = Event(event_type="RECOVERY_COMPLETED")
        self.context.add_event(completed_event)
        self.event_bus.publish(completed_event)

        return report

    # ---------------------------------------------------------------------
    # Internal helpers
    # ---------------------------------------------------------------------
    def _enforce_safety(self, rec: Recommendation) -> tuple[bool, str]:
        """Return ``(True, "")`` if the recommendation passes all safety checks.
        Otherwise return ``(False, reason)``.
        """
        action = rec.recommended_action
        # Mutual‑exclusion – currently only relevant when a recommendation could
        # contain multiple actions; kept for future extensibility.
        for excl_set in MUTUALLY_EXCLUSIVE:
            if action in excl_set and any(other in excl_set for other in []):
                return False, "Mutually exclusive actions requested."

        if action == "SCALE_UP_REPLICAS":
            # Naïve check: if reasoning contains the target replica count, guard
            # against exceeding MAX_REPLICAS. In a real system we would query the
            # current replica count from the cluster.
            try:
                target = int(rec.reasoning.split()[-1])  # expects something like "scale to 4"
                if target > MAX_REPLICAS:
                    return False, f"Requested replica count {target} exceeds limit of {MAX_REPLICAS}."
            except Exception:
                pass  # If we cannot parse, let executor decide.

        if action == "ROLLBACK_DEPLOYMENT":
            if not self._rollback_tracker.allow():
                return False, f"Rollback limit of {MAX_ROLLBACKS_PER_HOUR} per hour exceeded."

        # Additional policies can be added here.
        return True, ""

    def _audit(self, rec: Recommendation, report: ExecutionReport) -> None:
        """Write an audit entry – returns the generated audit_id.
        """
        entry = {
            "recommendation": rec.dict(),
            "report": report.dict(),
            "mode": self.mode,
        }
        audit_id = write_audit_entry(entry)
        # Propagate audit_id back to the report for callers that need it.
        report.audit_id = audit_id

    # ---------------------------------------------------------------------
    # Legacy event‑handler kept for backward compatibility – calls the new
    # ``handle_recommendation`` method.
    # ---------------------------------------------------------------------
    def handle_event(self, event) -> None:  # pragma: no cover – exercised via tests
        # Expect the payload to be a Recommendation inside the event.
        try:
            payload = getattr(event, "payload", None)
            if isinstance(payload, Recommendation):
                self.handle_recommendation(payload)
        except Exception as exc:
            logger.exception("Failed to handle recovery event: %s", exc)

# Export class for package import
__all__ = ["RecoveryManager"]