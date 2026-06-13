#!/usr/bin/env bash
# ci_cd/scripts/rollback.sh
# Rolls back the Helm release to the previous revision.
set -euo pipefail

HELM_RELEASE="${HELM_RELEASE:-inframind}"
K8S_NAMESPACE="${K8S_NAMESPACE:-inframind}"
CONTEXT="${KUBE_CONTEXT:-$(kubectl config current-context)}"
ROLLBACK_TO="${ROLLBACK_TO:-0}"   # 0 = previous revision

log()  { echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] [INFO] $*"; }

log "Rolling back Helm release: ${HELM_RELEASE} to revision=${ROLLBACK_TO}"
helm rollback "${HELM_RELEASE}" "${ROLLBACK_TO}" \
  --namespace "${K8S_NAMESPACE}" \
  --wait \
  --timeout 5m

log "Verifying rollback..."
kubectl --context="${CONTEXT}" -n "${K8S_NAMESPACE}" \
  rollout status deployment/inference-deployment --timeout=3m

log "Rollback complete ✓"
