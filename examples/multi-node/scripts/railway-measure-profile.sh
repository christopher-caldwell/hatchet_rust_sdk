#!/usr/bin/env bash
set -euo pipefail

: "${RAILWAY_PROJECT_ID:?set RAILWAY_PROJECT_ID}"
: "${RAILWAY_ENVIRONMENT_ID:?set RAILWAY_ENVIRONMENT_ID}"
: "${POSTGRES_SERVICE_ID:?set POSTGRES_SERVICE_ID}"

PROFILE=${1:?usage: railway-measure-profile.sh baseline|low-load}
case "$PROFILE" in baseline|low-load) ;; *) echo "invalid profile: $PROFILE" >&2; exit 2;; esac

WINDOW_SECONDS=${MEASUREMENT_SECONDS:-600}
if ((WINDOW_SECONDS < 60)); then
  echo "MEASUREMENT_SECONDS must be at least 60" >&2
  exit 2
fi

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
OUT="$ROOT/examples/multi-node/target/railway/metrics/$PROFILE"
SESSION=hatchet-rust-sdk-comparison-20260715
mkdir -p "$OUT"

railway_cli() {
  RAILWAY_CALLER=skill:use-railway@1.3.5 \
    RAILWAY_AGENT_SESSION="$SESSION" railway "$@"
}

snapshot_sql='SELECT json_build_object(
  '\''captured_at'\'', clock_timestamp(),
  '\''database'\'', datname,
  '\''connections'\'', (SELECT count(*) FROM pg_stat_activity WHERE datname = current_database()),
  '\''xact_commit'\'', xact_commit,
  '\''xact_rollback'\'', xact_rollback,
  '\''blks_read'\'', blks_read,
  '\''blks_hit'\'', blks_hit,
  '\''tup_returned'\'', tup_returned,
  '\''tup_fetched'\'', tup_fetched,
  '\''tup_inserted'\'', tup_inserted,
  '\''tup_updated'\'', tup_updated,
  '\''tup_deleted'\'', tup_deleted,
  '\''pg_stat_statements'\'', EXISTS (SELECT 1 FROM pg_extension WHERE extname = '\''pg_stat_statements'\'')
)::text FROM pg_stat_database WHERE datname = current_database();'

postgres_snapshot() {
  local sql64
  local ssh_args=()
  sql64=$(printf '%s' "$snapshot_sql" | base64 | tr -d '\n')
  if [[ -n "${RAILWAY_SSH_IDENTITY_FILE:-}" ]]; then
    ssh_args+=(--identity-file "$RAILWAY_SSH_IDENTITY_FILE")
  fi
  railway_cli ssh "${ssh_args[@]}" --project "$RAILWAY_PROJECT_ID" \
    --environment "$RAILWAY_ENVIRONMENT_ID" \
    --service "$POSTGRES_SERVICE_ID" \
    sh -lc "\"printf '%s' '$sql64' | base64 -d | psql \\\"\$DATABASE_URL\\\" -X -qAt\""
}

started_at=$(date -u +%Y-%m-%dT%H:%M:%SZ)
postgres_snapshot >"$OUT/postgres-start.json"
sleep "$WINDOW_SECONDS"
postgres_snapshot >"$OUT/postgres-end.json"

railway_cli metrics --all --project "$RAILWAY_PROJECT_ID" \
  --environment "$RAILWAY_ENVIRONMENT_ID" --since "${WINDOW_SECONDS}s" \
  --cpu --memory --json >"$OUT/railway-metrics.json"

jq -n \
  --arg profile "$PROFILE" \
  --arg started_at "$started_at" \
  --arg finished_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --argjson seconds "$WINDOW_SECONDS" \
  --slurpfile start "$OUT/postgres-start.json" \
  --slurpfile end "$OUT/postgres-end.json" \
  '{profile:$profile,started_at:$started_at,finished_at:$finished_at,window_seconds:$seconds,
    postgres_start:$start[0],postgres_end:$end[0],per_minute:{
      commits:(($end[0].xact_commit-$start[0].xact_commit)*60/$seconds),
      rollbacks:(($end[0].xact_rollback-$start[0].xact_rollback)*60/$seconds),
      blocks_read:(($end[0].blks_read-$start[0].blks_read)*60/$seconds),
      blocks_hit:(($end[0].blks_hit-$start[0].blks_hit)*60/$seconds),
      tuples_returned:(($end[0].tup_returned-$start[0].tup_returned)*60/$seconds),
      tuples_fetched:(($end[0].tup_fetched-$start[0].tup_fetched)*60/$seconds),
      tuples_inserted:(($end[0].tup_inserted-$start[0].tup_inserted)*60/$seconds),
      tuples_updated:(($end[0].tup_updated-$start[0].tup_updated)*60/$seconds),
      tuples_deleted:(($end[0].tup_deleted-$start[0].tup_deleted)*60/$seconds)
    }}' >"$OUT/database-summary.json"

echo "railway_measurement=complete profile=$PROFILE window_seconds=$WINDOW_SECONDS evidence_dir=$OUT"
