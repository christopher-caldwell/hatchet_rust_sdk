#!/usr/bin/env bash
set -euo pipefail

: "${RAILWAY_PROJECT_ID:?set RAILWAY_PROJECT_ID}"
: "${RAILWAY_ENVIRONMENT_ID:?set RAILWAY_ENVIRONMENT_ID}"
: "${ORDERS_SERVICE_ID:?set ORDERS_SERVICE_ID}"
: "${NOTIFICATIONS_SERVICE_ID:?set NOTIFICATIONS_SERVICE_ID}"

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
OUT="$ROOT/examples/railway-multi-node/target/railway"
SESSION=${RAILWAY_AGENT_SESSION:-hatchet-rust-sdk-comparison-20260715}
mkdir -p "$OUT"

railway_cli() {
  RAILWAY_CALLER=skill:use-railway@1.3.5 \
    RAILWAY_AGENT_SESSION="$SESSION" railway "$@"
}

railway_ssh() {
  if [[ -n "${RAILWAY_SSH_IDENTITY_FILE:-}" ]]; then
    railway_cli ssh --identity-file "$RAILWAY_SSH_IDENTITY_FILE" "$@"
  else
    railway_cli ssh "$@"
  fi
}

deployment_status() {
  railway_cli deployment list \
    --project "$RAILWAY_PROJECT_ID" \
    --environment "$RAILWAY_ENVIRONMENT_ID" \
    --service "$1" --limit 1 --json | jq -er '.[0].status'
}

wait_for_success() {
  service_id=$1
  deadline=$((SECONDS + 300))
  while ((SECONDS < deadline)); do
    status=$(deployment_status "$service_id")
    case "$status" in
      SUCCESS) return 0 ;;
      FAILED|CRASHED|REMOVED)
        echo "deployment_terminal_failure service=$service_id status=$status" >&2
        return 1
        ;;
    esac
    sleep 5
  done
  echo "deployment_timeout service=$service_id" >&2
  return 1
}

trigger() {
  order_id=$1
  output_file=$2
  railway_ssh \
    --project "$RAILWAY_PROJECT_ID" \
    --environment "$RAILWAY_ENVIRONMENT_ID" \
    --service "$ORDERS_SERVICE_ID" \
    /app/trigger --wait --interval-seconds 3 --timeout-seconds 240 \
    --order-id "$order_id" --email buyer@example.test --total-cents 4299 \
    | tee "$output_file"
  grep -q '"event":"run_completed"' "$output_file"
}

worker_health() {
  local service_id=$1
  local port
  port=$(railway_ssh \
    --project "$RAILWAY_PROJECT_ID" \
    --environment "$RAILWAY_ENVIRONMENT_ID" \
    --service "$service_id" printenv PORT)
  [[ "$port" =~ ^[0-9]+$ ]]
  railway_ssh \
    --project "$RAILWAY_PROJECT_ID" \
    --environment "$RAILWAY_ENVIRONMENT_ID" \
    --service "$service_id" curl -fsS "http://127.0.0.1:$port/health"
}

capture_action_logs() {
  suffix=$1
  run_id=$2
  capture_action_log "$ORDERS_SERVICE_ID" process-order "$run_id" \
    "$OUT/orders-$suffix.jsonl"
  capture_action_log "$NOTIFICATIONS_SERVICE_ID" send-order-receipt "$run_id" \
    "$OUT/notifications-$suffix.jsonl"
}

capture_action_log() {
  local service_id=$1
  local action=$2
  local run_id=$3
  local output_file=$4
  local deadline=$((SECONDS + 90))
  while ((SECONDS < deadline)); do
    if railway_cli logs --project "$RAILWAY_PROJECT_ID" \
      --environment "$RAILWAY_ENVIRONMENT_ID" --service "$service_id" \
      --since 20m --lines 300 --json \
      | jq -R -c --arg action "$action" --arg run_id "$run_id" \
        'fromjson? | select(.action == $action and .run_id == $run_id)' \
        >"$output_file"; then
      [[ -s "$output_file" ]] && return 0
    fi
    sleep 3
  done
  echo "action_log_timeout service=$service_id action=$action" >&2
  return 1
}

wait_for_success "$ORDERS_SERVICE_ID"
wait_for_success "$NOTIFICATIONS_SERVICE_ID"

trigger "${FIRST_ORDER_ID:-ord_railway_001}" "$OUT/run-before-restart.jsonl"
first_run_id=$(jq -r 'select(.event == "run_completed") | .run_id' \
  "$OUT/run-before-restart.jsonl")
[[ -n "$first_run_id" ]]
capture_action_logs before-restart "$first_run_id"

restart_result=$(railway_cli restart --project "$RAILWAY_PROJECT_ID" \
  --environment "$RAILWAY_ENVIRONMENT_ID" \
  --service "$NOTIFICATIONS_SERVICE_ID" --yes --json)
printf '%s\n' "$restart_result" | jq -e . >/dev/null
wait_for_success "$NOTIFICATIONS_SERVICE_ID"

ready_deadline=$((SECONDS + 180))
while ((SECONDS < ready_deadline)); do
  if worker_health "$NOTIFICATIONS_SERVICE_ID" \
    | grep -q '"worker_name":"rust-notifications-worker"'; then
    break
  fi
  sleep 5
done
((SECONDS < ready_deadline))

trigger "${SECOND_ORDER_ID:-ord_railway_002}" "$OUT/run-after-restart.jsonl"
second_run_id=$(jq -r 'select(.event == "run_completed") | .run_id' \
  "$OUT/run-after-restart.jsonl")
[[ -n "$second_run_id" ]]
capture_action_logs after-restart "$second_run_id"

echo "railway_live_proof=pass evidence_dir=$OUT"
