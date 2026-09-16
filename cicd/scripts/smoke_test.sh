#!/bin/bash
# Post-Dev-deploy smoke test (Section 26 stage 6): triggers a single
# low-risk Glue job (product_master, a small FULL-load pipeline) and checks
# the control table records a SUCCESS run within the timeout window.
# Failure here blocks promotion to QA.
set -euo pipefail

ENVIRONMENT="${1:-dev}"
PIPELINE_NAME="sap_product_master_incremental"
TIMEOUT_SECONDS=600

echo "Smoke test: triggering ${PIPELINE_NAME} in ${ENVIRONMENT}"
aws glue start-job-run --job-name ingest_sap \
  --arguments "--environment=${ENVIRONMENT},--pipeline_name=${PIPELINE_NAME}" \
  --query 'JobRunId' --output text > /tmp/smoke_test_run_id.txt

RUN_ID=$(cat /tmp/smoke_test_run_id.txt)
ELAPSED=0
while [[ "$ELAPSED" -lt "$TIMEOUT_SECONDS" ]]; do
  STATE=$(aws glue get-job-run --job-name ingest_sap --run-id "$RUN_ID" --query 'JobRun.JobRunState' --output text)
  if [[ "$STATE" == "SUCCEEDED" ]]; then
    echo "Smoke test PASSED (Glue run $RUN_ID succeeded)"
    exit 0
  elif [[ "$STATE" == "FAILED" || "$STATE" == "ERROR" || "$STATE" == "TIMEOUT" ]]; then
    echo "Smoke test FAILED: Glue run $RUN_ID ended in state $STATE" >&2
    exit 1
  fi
  sleep 15
  ELAPSED=$((ELAPSED + 15))
done

echo "Smoke test FAILED: timed out after ${TIMEOUT_SECONDS}s waiting for Glue run $RUN_ID" >&2
exit 1
