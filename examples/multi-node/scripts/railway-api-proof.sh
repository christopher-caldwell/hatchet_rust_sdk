#!/usr/bin/env bash
set -euo pipefail

: "${SERVER_URL:?SERVER_URL must be injected from the orchestrator service}"
: "${ADMIN_EMAIL:?ADMIN_EMAIL must be injected from the orchestrator service}"
: "${ADMIN_PASSWORD:?ADMIN_PASSWORD must be injected from the orchestrator service}"
: "${DEFAULT_TENANT_ID:?DEFAULT_TENANT_ID must be injected from the orchestrator service}"
: "${FIRST_RUN_ID:?set FIRST_RUN_ID}"
: "${SECOND_RUN_ID:?set SECOND_RUN_ID}"

tmp=$(mktemp -d /tmp/hatchet-api-proof.XXXXXX)
trap 'rm -rf "$tmp"' EXIT INT TERM
base=${SERVER_URL%/}

jq -n --arg email "$ADMIN_EMAIL" --arg password "$ADMIN_PASSWORD" \
  '{email:$email,password:$password}' >"$tmp/login.json"
login_status=$(curl -sS -o "$tmp/login-response.json" -w '%{http_code}' \
  -c "$tmp/cookies.txt" -H 'Content-Type: application/json' \
  --data-binary @"$tmp/login.json" "$base/api/v1/users/login")
[[ "$login_status" == "200" ]]

fetch_json() {
  local label=$1
  local url=$2
  local output=$3
  local status
  status=$(curl -sS -o "$output" -w '%{http_code}' \
    -b "$tmp/cookies.txt" "$url")
  if [[ "$status" != "200" ]]; then
    echo "api_fetch_failure endpoint=$label status=$status" >&2
    return 1
  fi
}

fetch_json workflows \
  "$base/api/v1/tenants/$DEFAULT_TENANT_ID/workflows?limit=50" \
  "$tmp/workflows.json"
fetch_json workers \
  "$base/api/v1/tenants/$DEFAULT_TENANT_ID/worker?limit=50" \
  "$tmp/workers.json"
fetch_json runs \
  "$base/api/v1/stable/tenants/$DEFAULT_TENANT_ID/workflow-runs?since=2026-07-15T00%3A00%3A00Z&only_tasks=false&limit=50&include_payloads=false" \
  "$tmp/runs.json"
fetch_json first-run-details \
  "$base/api/v1/stable/workflow-runs/$FIRST_RUN_ID" \
  "$tmp/first-run-details.json"
fetch_json second-run-details \
  "$base/api/v1/stable/workflow-runs/$SECOND_RUN_ID" \
  "$tmp/second-run-details.json"

jq -n \
  --arg login_status "$login_status" \
  --arg first_run_id "$FIRST_RUN_ID" \
  --arg second_run_id "$SECOND_RUN_ID" \
  --slurpfile workflows "$tmp/workflows.json" \
  --slurpfile workers "$tmp/workers.json" \
  --slurpfile runs "$tmp/runs.json" \
  --slurpfile first_details "$tmp/first-run-details.json" \
  --slurpfile second_details "$tmp/second-run-details.json" \
  '{login_status:($login_status|tonumber),
    workflow_names:[$workflows[0].rows[]?.name],
    workers:[$workers[0].rows[]? | {name,actions,status}],
    target_runs:[$runs[0].rows[]? |
      select(.workflowRunExternalId == $first_run_id or
        .workflowRunExternalId == $second_run_id) |
      {id:.workflowRunExternalId,workflowName,status,startedAt,finishedAt,duration}],
    target_run_details:[$first_details[0],$second_details[0]] |
      map({id:.run.metadata.id,status:.run.status,
        shape:[.shape[] | {taskName,childrenStepIds}],
        task_sequence:[.tasks | sort_by(.startedAt)[] |
          {actionId,status,startedAt,finishedAt}]})}'
