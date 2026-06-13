#!/usr/bin/env bash
# ci_cd/scripts/stress_test.sh
# Fire 500 concurrent requests at /predict and report p50/p95/p99 latency.
set -euo pipefail

TARGET_URL="${TARGET_URL:-http://localhost:8000/predict}"
CONCURRENCY="${CONCURRENCY:-50}"
TOTAL="${TOTAL:-500}"
PAYLOAD='{"input_text": "test load signal from stress harness"}'

log() { echo "[$(date -u '+%Y-%m-%dT%H:%M:%SZ')] [STRESS] $*"; }

# Prefer hey, fall back to ab
if command -v hey &>/dev/null; then
    log "Using 'hey' → ${TARGET_URL}  C=${CONCURRENCY}  N=${TOTAL}"
    hey -n "${TOTAL}" -c "${CONCURRENCY}" \
        -m POST \
        -H "Content-Type: application/json" \
        -d "${PAYLOAD}" \
        "${TARGET_URL}"
elif command -v ab &>/dev/null; then
    TMP=$(mktemp)
    echo -n "${PAYLOAD}" > "${TMP}"
    log "Using 'ab' → ${TARGET_URL}  C=${CONCURRENCY}  N=${TOTAL}"
    ab -n "${TOTAL}" -c "${CONCURRENCY}" \
       -T "application/json" \
       -p "${TMP}" \
       "${TARGET_URL}"
    rm -f "${TMP}"
else
    log "Neither 'hey' nor 'ab' found. Falling back to curl loop."
    for i in $(seq 1 "${TOTAL}"); do
        curl -s -o /dev/null -w "%{http_code} %{time_total}s\n" \
            -X POST "${TARGET_URL}" \
            -H "Content-Type: application/json" \
            -d "${PAYLOAD}" &
        if (( i % CONCURRENCY == 0 )); then wait; fi
    done
    wait
fi

log "Stress test complete."
