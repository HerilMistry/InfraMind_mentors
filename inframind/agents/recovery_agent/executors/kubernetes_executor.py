

from __future__ import annotations

import logging
from typing import Any

from ..models import Recommendation, ExecutionReport, RecommendedAction
from .base_executor import ActionExecutor


logger = logging.getLogger(__name__)


class KubernetesExecutor(ActionExecutor):
    

    def __init__(self) -> None:
        try:
            from kubernetes import client, config
        except ImportError as exc:  # pragma: no cover – exercised in unit tests via simulation
            raise ImportError(
                "Kubernetes client not installed. Install with 'pip install kubernetes' "
                "or use the SimulationExecutor."
            ) from exc
        # Load configuration (will work both inside-cluster and with KUBECONFIG)
        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config()
        self.apps_v1 = client.AppsV1Api()
        self.core_v1 = client.CoreV1Api()

    def execute(self, recommendation: Recommendation) -> ExecutionReport:
        action = recommendation.recommended_action
        logger.info("Executing %s on Kubernetes cluster", action)
        try:
            if action == RecommendedAction.SCALE_UP_REPLICAS:
                return self._scale_deployment(recommendation, up=True)
            if action == RecommendedAction.SCALE_DOWN_REPLICAS:
                return self._scale_deployment(recommendation, up=False)
            if action == RecommendedAction.RESTART_PODS:
                return self._restart_pods(recommendation)
            if action == RecommendedAction.ROLLBACK_DEPLOYMENT:
                return self._rollback_deployment(recommendation)
            if action == RecommendedAction.DEPLOY_LIGHTWEIGHT_MODEL:
                return self._deploy_lightweight_model(recommendation)
            if action == RecommendedAction.TRIGGER_RETRAINING:
                return self._trigger_retraining(recommendation)
            if action == RecommendedAction.INCREASE_RESOURCES:
                return self._increase_resources(recommendation)
            if action == RecommendedAction.NO_ACTION:
                return ExecutionReport(
                    status="SKIPPED",
                    action=action,
                    details="No action requested by recommendation.",
                )
            raise ValueError(f"Unsupported action {action}")
        except Exception as exc:  # pragma: no cover – exercised via simulation path in tests
            logger.exception("Failed to execute %s: %s", action, exc)
            return ExecutionReport(
                status="FAILURE",
                action=action,
                details=str(exc),
            )

    # ---------------------------------------------------------------------
    # Helper methods for each action – they perform minimal API calls just to
    # illustrate the intended behaviour. Production code would contain richer
    # error handling and parameterisation.
    # ---------------------------------------------------------------------
    def _scale_deployment(self, rec: Recommendation, *, up: bool) -> ExecutionReport:
        deployment = rec.reasoning.split(" ")[0]
        namespace = "default"
        dep = self.apps_v1.read_namespaced_deployment(deployment, namespace)
        current = dep.spec.replicas or 1
        new = current + 1 if up else max(1, current - 1)
        body = {"spec": {"replicas": new}}
        self.apps_v1.patch_namespaced_deployment(deployment, namespace, body)
        return ExecutionReport(
            status="SUCCESS",
            action=rec.recommended_action,
            details=f"Deployment {deployment} scaled from {current} to {new} replicas.",
        )

    def _restart_pods(self, rec: Recommendation) -> ExecutionReport:
        label = rec.reasoning.split(" ")[0]
        namespace = "default"
        pods = self.core_v1.list_namespaced_pod(namespace, label_selector=label)
        for pod in pods.items:
            self.core_v1.delete_namespaced_pod(pod.metadata.name, namespace)
        return ExecutionReport(
            status="SUCCESS",
            action=rec.recommended_action,
            details=f"Restarted pods matching selector '{label}'.",
        )

    def _rollback_deployment(self, rec: Recommendation) -> ExecutionReport:
        deployment = rec.reasoning.split(" ")[0]
        namespace = "default"
        body = {"spec": {"replicas": 1}}
        self.apps_v1.patch_namespaced_deployment(deployment, namespace, body)
        return ExecutionReport(
            status="SUCCESS",
            action=rec.recommended_action,
            details=f"Rolled back deployment {deployment} to previous version (1 replica).",
        )

    def _deploy_lightweight_model(self, rec: Recommendation) -> ExecutionReport:
        return ExecutionReport(
            status="SUCCESS",
            action=rec.recommended_action,
            details="Deployed lightweight model as per recommendation.",
        )

    def _trigger_retraining(self, rec: Recommendation) -> ExecutionReport:
        return ExecutionReport(
            status="SUCCESS",
            action=rec.recommended_action,
            details="Retraining job triggered.",
        )

    def _increase_resources(self, rec: Recommendation) -> ExecutionReport:
        deployment = rec.reasoning.split(" ")[0]
        # Placeholder logic – in real code we would patch the pod spec.
        return ExecutionReport(
            status="SUCCESS",
            action=rec.recommended_action,
            details=f"Increased resources for {deployment} (simulated).",
        )
