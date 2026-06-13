#!/usr/bin/env bash
# ci_cd/scripts/deploy.sh
# Full deployment script: build → push → helm upgrade → verify
set -euo pipefail

# ─── Config ───────────────────────────────────────────────────────────────────
REGISTRY="${REGISTRY:-docker.io/inframind}"
IMAGE_NAME="${IMAGE_NAME:-inference-service}"
IMAGE_TAG="${IMAGE_TAG:-$(git rev-parse --short HEAD)}"
HELM_RELEASE="${HELM_RELEASE:-inframind}"
HELM_CHART_PATH="${HELM_CHART_PATH:-./helm/inframind}"
K8S_NAMESPACE="${K8S_NAMESPACE:-inframind}"
CONTEXT="${KUBE_CONTEXT:-$(kubectl config current-context)}"

log()  { echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] [INFO] $*"; }
err()  { echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] [ERROR] $*" >&2; exit 1; }

# ─── 1. Build Docker image ─────────────────────────────────────────────────
log "Building image: ${REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}"
docker build \
  -t "${REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}" \
  -t "${REGISTRY}/${IMAGE_NAME}:latest" \
  -f services/inference_service/Dockerfile \
  services/inference_service/

# ─── 2. Push image ─────────────────────────────────────────────────────────
log "Pushing image to registry..."
docker push "${REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}"
docker push "${REGISTRY}/${IMAGE_NAME}:latest"

# ─── 3. Ensure namespace exists ────────────────────────────────────────────
kubectl --context="${CONTEXT}" create namespace "${K8S_NAMESPACE}" --dry-run=client -o yaml \
  | kubectl --context="${CONTEXT}" apply -f -

# ─── 4. Helm upgrade / install ─────────────────────────────────────────────
log "Deploying Helm release: ${HELM_RELEASE} in namespace: ${K8S_NAMESPACE}"
helm upgrade --install "${HELM_RELEASE}" "${HELM_CHART_PATH}" \
  --namespace "${K8S_NAMESPACE}" \
  --set image.tag="${IMAGE_TAG}" \
  --set image.repository="${REGISTRY}/${IMAGE_NAME}" \
  --atomic \
  --timeout 5m \
  --wait

# ─── 5. Verify rollout ─────────────────────────────────────────────────────
log "Verifying rollout..."
kubectl --context="${CONTEXT}" -n "${K8S_NAMESPACE}" \
  rollout status deployment/inference-deployment --timeout=3m

log "Deployment complete ✓ → ${REGISTRY}/${IMAGE_NAME}:${IMAGE_TAG}"
