#!/usr/bin/env bash
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)
EXAMPLE="$ROOT/examples/multi-node"
RUN_KEY="hatchet-rust-smoke-$$"
NETWORK="$RUN_KEY"
POSTGRES="$RUN_KEY-postgres"
HATCHET="$RUN_KEY-hatchet"
VOLUME="$RUN_KEY-config"
TLS_DIR=""
ORDERS_PID=""
NOTIFICATIONS_PID=""
HATCHET_IMAGE="${SMOKE_HATCHET_IMAGE:-ghcr.io/hatchet-dev/hatchet/hatchet-lite:v0.94.14@sha256:b8d6b7e9b96d354f1a252590d1480d3bb70f01604cc6cba1fd97ff7aaa2cb07c}"
STARTED_AT=$(date -u +%Y-%m-%dT%H:%M:%SZ)
CURRENT_STAGE=initialization
CLEANED_UP=0
SDK_INTEGRATION_RAN=false
SDK_INTEGRATION_TESTS=0

perform_cleanup() {
  if [[ "$CLEANED_UP" == "1" ]]; then return; fi
  if [[ -n "${ORDERS_PID}" ]]; then kill "${ORDERS_PID}" 2>/dev/null || true; fi
  if [[ -n "${NOTIFICATIONS_PID}" ]]; then kill "${NOTIFICATIONS_PID}" 2>/dev/null || true; fi
  for pid in "${ORDERS_PID}" "${NOTIFICATIONS_PID}"; do
    [[ -n "$pid" ]] || continue
    for _ in $(seq 1 50); do
      kill -0 "$pid" 2>/dev/null || break
      sleep 0.1
    done
    if kill -0 "$pid" 2>/dev/null; then kill -KILL "$pid" 2>/dev/null || true; fi
  done
  wait "${ORDERS_PID}" 2>/dev/null || true
  wait "${NOTIFICATIONS_PID}" 2>/dev/null || true
  if [[ -n "${SMOKE_TEST_MARKER:-}" ]]; then rm -rf "${SMOKE_TEST_MARKER}"; fi
  docker rm -f "$HATCHET" "$POSTGRES" >/dev/null 2>&1 || true
  docker volume rm "$VOLUME" >/dev/null 2>&1 || true
  docker network rm "$NETWORK" >/dev/null 2>&1 || true
  if [[ -n "$TLS_DIR" ]]; then rm -rf "$TLS_DIR"; fi
  CLEANED_UP=1
}

cleanup() {
  status=$?
  perform_cleanup
  echo "cleanup_status=complete"
  exit "$status"
}
trap cleanup EXIT INT TERM
trap 'echo "smoke_error_stage=$CURRENT_STAGE line=$LINENO"' ERR

if [[ -n "${SMOKE_TEST_CLEANUP_ONLY:-}" ]]; then
  mkdir -p "${SMOKE_TEST_MARKER:?SMOKE_TEST_MARKER is required}"
  if [[ -n "${SMOKE_TEST_CHILD_PID_FILE:-}" ]]; then
    sleep 60 &
    ORDERS_PID=$!
    printf '%s\n' "$ORDERS_PID" >"$SMOKE_TEST_CHILD_PID_FILE"
  fi
  if [[ "$SMOKE_TEST_CLEANUP_ONLY" == "failure" ]]; then exit 23; fi
  exit 0
fi

mkdir -p "$EXAMPLE/target/smoke"
TLS_ARGS=()
GRPC_INSECURE=true
if [[ "${SMOKE_TLS:-0}" == "1" ]]; then
  TLS_DIR=$(mktemp -d "$EXAMPLE/target/tls-owned.XXXXXX")
  bash "$ROOT/scripts/generate-test-tls.sh" "$TLS_DIR"
  GRPC_INSECURE=false
  TLS_ARGS=(-v "$TLS_DIR:/tls:ro" -e SERVER_TLS_STRATEGY=tls
    -e SERVER_TLS_CERT_FILE=/tls/server.pem -e SERVER_TLS_KEY_FILE=/tls/server.key
    -e SERVER_TLS_ROOT_CA_FILE=/tls/ca.pem)
fi
CURRENT_STAGE=worker_build
cargo build --locked --manifest-path "$EXAMPLE/Cargo.toml" --bins
EXAMPLE_TARGET=$(cargo metadata --locked --manifest-path "$EXAMPLE/Cargo.toml" \
  --no-deps --format-version 1 | jq -r .target_directory)
CURRENT_STAGE=postgres_start
docker network create "$NETWORK" >/dev/null
docker volume create "$VOLUME" >/dev/null
docker run -d --name "$POSTGRES" --network "$NETWORK" \
  -e POSTGRES_USER=hatchet -e POSTGRES_PASSWORD=local-hatchet-password \
  -e POSTGRES_DB=hatchet --health-cmd='pg_isready -U hatchet' \
  --health-interval=2s --health-timeout=3s --health-retries=30 postgres:16-alpine >/dev/null

for _ in $(seq 1 60); do
  [[ "$(docker inspect -f '{{.State.Health.Status}}' "$POSTGRES")" == "healthy" ]] && break
  sleep 1
done
[[ "$(docker inspect -f '{{.State.Health.Status}}' "$POSTGRES")" == "healthy" ]]

CURRENT_STAGE=hatchet_start
STREAMS_ENTITLEMENT=false
if [[ "${SMOKE_RUN_MODERN_TESTS:-0}" == "1" ]]; then
  STREAMS_ENTITLEMENT=true
fi
docker run -d --name "$HATCHET" --network "$NETWORK" \
  -p 127.0.0.1:18888:8888 -p 127.0.0.1:17077:7077 -p 127.0.0.1:18733:8733 \
  -e SERVER_DEFAULT_TENANT_ENTITLEMENT_DURABLE_STREAMS="$STREAMS_ENTITLEMENT" \
  -v "$VOLUME:/config" \
  -e DATABASE_URL='postgresql://hatchet:local-hatchet-password@'"$POSTGRES"':5432/hatchet?sslmode=disable' \
  -e SERVER_MSGQUEUE_KIND=postgres -e ADMIN_EMAIL=local-demo@example.test \
  -e ADMIN_PASSWORD='LocalHatchet2026!' -e DEFAULT_TENANT_NAME='Rust SDK Demo' \
  -e DEFAULT_TENANT_SLUG=rust-sdk-demo -e DEFAULT_TENANT_ID=707d0855-80ab-4e1f-a156-f1c4546cbf52 \
  -e SERVER_GRPC_BIND_ADDRESS=0.0.0.0 -e SERVER_GRPC_PORT=7077 \
  -e SERVER_GRPC_INSECURE="$GRPC_INSECURE" -e SERVER_GRPC_BROADCAST_ADDRESS=localhost:7077 \
  -e SERVER_INTERNAL_CLIENT_INTERNAL_GRPC_BROADCAST_ADDRESS=localhost:7077 \
  -e SERVER_DEFAULT_ENGINE_VERSION=V1 \
  -e SERVER_OPTIMISTIC_SCHEDULING_ENABLED=false \
  -e SERVER_URL=http://localhost:18888 -e SERVER_AUTH_COOKIE_DOMAIN=localhost \
  -e SERVER_AUTH_COOKIE_INSECURE=true -e SERVER_AUTH_SET_EMAIL_VERIFIED=true \
  -e SERVER_ALLOW_SIGNUP=false -e SERVER_ALLOW_INVITES=false \
  -e LITE_RUNTIME_PORT=8888 -e SERVER_HEALTHCHECK_PORT=8733 \
  ${TLS_ARGS[@]+"${TLS_ARGS[@]}"} "$HATCHET_IMAGE" >/dev/null

CURRENT_STAGE=hatchet_readiness
for _ in $(seq 1 120); do
  curl -fsS http://127.0.0.1:18733/ready >/dev/null 2>&1 && break
  sleep 1
done
curl -fsS http://127.0.0.1:18733/ready >/dev/null

CURRENT_STAGE=token_bootstrap
TENANT_ID=$(docker exec "$POSTGRES" psql -U hatchet -d hatchet -tAc \
  'select "id" from "Tenant" where slug = '\''rust-sdk-demo'\'' limit 1')
[[ -n "$TENANT_ID" ]]
TOKEN_ERROR="$EXAMPLE/target/smoke/token-bootstrap.err"
if ! TOKEN=$(docker exec "$HATCHET" ./hatchet-admin token create --config /config --tenant-id "$TENANT_ID" --name local-rust-demo --expiresIn 24h 2>"$TOKEN_ERROR" | tail -n 1); then
  sed -E 's#postgres(ql)?://[^ ]+#postgres://[REDACTED]#g' "$TOKEN_ERROR" >&2
  exit 1
fi
[[ -n "$TOKEN" ]]
rm -f "$TOKEN_ERROR"

export HATCHET_CLIENT_TOKEN="$TOKEN"
export HATCHET_CLIENT_HOST_PORT=localhost:17077
export HATCHET_CLIENT_SERVER_URL=http://localhost:18888
export HATCHET_CLIENT_TLS_STRATEGY=none
if [[ "${SMOKE_TLS:-0}" == "1" ]]; then
  export HATCHET_CLIENT_TLS_STRATEGY=tls
  export HATCHET_CLIENT_TLS_ROOT_CA_FILE="$TLS_DIR/ca.pem"
  export HATCHET_TEST_TLS_CA_FILE="$TLS_DIR/ca.pem"
  export HATCHET_TEST_TLS_WRONG_CA_FILE="$TLS_DIR/wrong-ca.pem"
fi
export HATCHET_CLIENT_NAMESPACE=local-demo
export HATCHET_CLIENT_LOG_LEVEL=info

CURRENT_STAGE=worker_start
HATCHET_NODE=orders PORT=19081 "$EXAMPLE_TARGET/debug/hatchet-multi-node" \
  >"$EXAMPLE/target/smoke/orders.log" 2>&1 &
ORDERS_PID=$!
HATCHET_NODE=notifications PORT=19082 "$EXAMPLE_TARGET/debug/hatchet-multi-node" \
  >"$EXAMPLE/target/smoke/notifications.log" 2>&1 &
NOTIFICATIONS_PID=$!

for endpoint in http://127.0.0.1:19081/health http://127.0.0.1:19082/health; do
  for _ in $(seq 1 120); do
    kill -0 "$ORDERS_PID" 2>/dev/null && kill -0 "$NOTIFICATIONS_PID" 2>/dev/null
    curl -fsS "$endpoint" >/dev/null 2>&1 && break
    sleep 1
  done
  curl -fsS "$endpoint" >/dev/null
done
for log in "$EXAMPLE/target/smoke/orders.log" "$EXAMPLE/target/smoke/notifications.log"; do
  for _ in $(seq 1 60); do grep -q '"event":"worker_ready"' "$log" && break; sleep 1; done
  grep -q '"event":"worker_ready"' "$log"
done

CURRENT_STAGE=workflow_trigger
if [[ "${SMOKE_INJECT_FAILURE:-0}" == "1" ]]; then
  echo "injected_failure=before_trigger"
  exit 24
fi

TRIGGER_OUTPUT=$(cd "$EXAMPLE" && cargo run --locked --bin trigger -- \
  --wait --order-id ord_local_001 --email buyer@example.test --total-cents 4299 \
  --interval-seconds 3 --timeout-seconds 180)
printf '%s\n' "$TRIGGER_OUTPUT" >"$EXAMPLE/target/smoke/trigger.jsonl"
RUN_ID=$(printf '%s\n' "$TRIGGER_OUTPUT" | sed -n 's/.*"run_id":"\([^"]*\)".*/\1/p' | head -n 1)
grep -q '"event":"run_completed"' "$EXAMPLE/target/smoke/trigger.jsonl"
grep -q '"action":"process-order"' "$EXAMPLE/target/smoke/orders.log"
grep -q '"action":"send-order-receipt"' "$EXAMPLE/target/smoke/notifications.log"

if [[ "${SMOKE_RUN_SDK_INTEGRATION:-1}" != "0" ]]; then
  CURRENT_STAGE=sdk_integration_tests
  SDK_INTEGRATION_RAN=true
  (
    cd "$ROOT"
    SDK_TEST_TARGETS=(--test integration)
    if [[ "${SMOKE_TLS:-0}" == "1" ]]; then SDK_TEST_TARGETS+=(--test tls_engine); fi
    if [[ "${SMOKE_RUN_MODERN_TESTS:-0}" == "1" ]]; then SDK_TEST_TARGETS+=(--test modern_engine); fi
    if [[ "${SMOKE_RUN_RECOVERY_TESTS:-0}" == "1" ]]; then SDK_TEST_TARGETS+=(--test recovery_engine --test sticky_recovery_engine --test worker_crash_engine); fi
    export HATCHET_TEST_ENGINE_CONTAINER="$HATCHET"
    cargo test --locked -p another-hatchet-sdk --features integration \
      "${SDK_TEST_TARGETS[@]}" -- --ignored --nocapture --test-threads=1
    # Restart proofs disrupt worker identities/scheduling; keep topic restart in
    # its own late phase, after the ordinary routing and checkpoint tests.
    if [[ "${SMOKE_RUN_MODERN_TESTS:-0}" == "1" ]]; then
      cargo test --locked -p another-hatchet-sdk --features integration \
        --test topic_recovery_engine -- --ignored --nocapture --test-threads=1
    fi
  ) 2>&1 | tee "$EXAMPLE/target/smoke/sdk-integration.log"
  SDK_INTEGRATION_TESTS=$(sed -nE \
    's/^test result: ok\. ([0-9]+) passed; 0 failed; 0 ignored;.*/\1/p' \
    "$EXAMPLE/target/smoke/sdk-integration.log" | awk '{sum += $1} END {print sum}')
  [[ "$SDK_INTEGRATION_TESTS" =~ ^[0-9]+$ && "$SDK_INTEGRATION_TESTS" -gt 0 ]]
fi

CURRENT_STAGE=post_run_worker_recovery
for endpoint in http://127.0.0.1:19081/health http://127.0.0.1:19082/health; do
  for _ in $(seq 1 60); do curl -fsS "$endpoint" >/dev/null 2>&1 && break; sleep 1; done
  curl -fsS "$endpoint" >/dev/null
done

CURRENT_STAGE=cleanup
perform_cleanup

CURRENT_STAGE=evidence_write
jq -n \
  --arg started_at "$STARTED_AT" \
  --arg finished_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --arg hatchet_image "$HATCHET_IMAGE" \
  --arg run_id "$RUN_ID" \
  --argjson sdk_integration_ran "$SDK_INTEGRATION_RAN" \
  --argjson sdk_integration_tests "$SDK_INTEGRATION_TESTS" \
  --argjson elapsed_seconds "$SECONDS" \
  '{status:"pass",started_at:$started_at,finished_at:$finished_at,
    hatchet_image:$hatchet_image,workflow:"rust-order-processing",run_id:$run_id,
    elapsed_seconds:$elapsed_seconds,sdk_integration_ran:$sdk_integration_ran,
    sdk_integration_tests:$sdk_integration_tests,
    post_run_worker_health:"pass",ordered_steps:[
      {worker:"rust-orders-worker",action:"process-order"},
      {worker:"rust-notifications-worker",action:"send-order-receipt"}
    ],cleanup:"complete"}' >"$EXAMPLE/target/smoke/local-report.json"
echo "local_smoke=pass run_id=$RUN_ID"
