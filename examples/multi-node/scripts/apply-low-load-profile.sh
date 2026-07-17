#!/usr/bin/env bash
set -euo pipefail

: "${RAILWAY_PROJECT_ID:?set RAILWAY_PROJECT_ID}"
: "${RAILWAY_ENVIRONMENT_ID:?set RAILWAY_ENVIRONMENT_ID}"
: "${ORCHESTRATOR_SERVICE_ID:?set ORCHESTRATOR_SERVICE_ID}"

SESSION=hatchet-rust-sdk-comparison-20260715
railway_cli() {
  RAILWAY_CALLER=skill:use-railway@1.3.5 \
    RAILWAY_AGENT_SESSION="$SESSION" railway "$@"
}

railway_cli variable set --project "$RAILWAY_PROJECT_ID" \
  --environment "$RAILWAY_ENVIRONMENT_ID" --service "$ORCHESTRATOR_SERVICE_ID" \
  --skip-deploys --json \
  DATABASE_MAX_CONNS=10 DATABASE_MIN_CONNS=1 \
  DATABASE_MAX_QUEUE_CONNS=10 DATABASE_MIN_QUEUE_CONNS=1 \
  SCHEDULER_CONCURRENCY_POLLING_MIN_INTERVAL=5s \
  SCHEDULER_CONCURRENCY_POLLING_MAX_INTERVAL=10s \
  SCHEDULER_CHECK_ACTIVE_MIN_INTERVAL=60s \
  SCHEDULER_CHECK_ACTIVE_MAX_INTERVAL=120s \
  SERVER_OPERATIONS_POLL_INTERVAL=10 \
  SERVER_OPERATIONS_JITTER=1000

railway_cli redeploy --project "$RAILWAY_PROJECT_ID" \
  --environment "$RAILWAY_ENVIRONMENT_ID" --service "$ORCHESTRATOR_SERVICE_ID" \
  --yes --json

deadline=$((SECONDS + 300))
while ((SECONDS < deadline)); do
  status=$(railway_cli deployment list --project "$RAILWAY_PROJECT_ID" \
    --environment "$RAILWAY_ENVIRONMENT_ID" --service "$ORCHESTRATOR_SERVICE_ID" \
    --limit 1 --json | jq -er '.[0].status')
  case "$status" in
    SUCCESS) echo "low_load_profile=applied"; exit 0 ;;
    FAILED|CRASHED|REMOVED) echo "orchestrator_deploy_failure=$status" >&2; exit 1 ;;
  esac
  sleep 5
done
echo "orchestrator_deploy_timeout" >&2
exit 1
