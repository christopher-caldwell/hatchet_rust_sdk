# Hatchet Rust: one orchestrator, two workers

This standalone consumer example runs one typed workflow across two independent
Rust processes backed by an official Hatchet Lite server and PostgreSQL:

```text
OrderInput
  -> rust-orders-worker / process-order
  -> rust-notifications-worker / send-order-receipt
  -> Receipt
```

The orders process alone registers the complete `rust-order-processing` DAG.
Each worker registers only its own action, advertises two normal and two durable
slots, binds its health server to `PORT`, and shuts down on SIGTERM or Ctrl-C.
All effects are deterministic; no email is sent.

## Local proof

Requirements are Docker, Rust 1.85 or newer, `curl`, and `psql` in the
PostgreSQL container. The SDK vendors both its protocol definitions and
`protoc`. Run the complete isolated proof from the repository root:

```sh
./examples/multi-node/scripts/local-smoke.sh
```

The script starts PostgreSQL 16, official Hatchet Lite
`v0.94.14@sha256:b8d6b7e9b96d354f1a252590d1480d3bb70f01604cc6cba1fd97ff7aaa2cb07c`,
both workers, and one trigger. Before teardown it also runs the SDK's
ignored live-engine integration tests serially against that same local server.
Those tests cover basic and DAG runs, child workflows, durable memo/sleep,
scoped durable-event lookback, event triggers, cancellation, REST feature
clients, and log retrieval.
It then requires both example workers to be healthy again, covering the
reconnect path when Hatchet closes and re-establishes an action stream.
Set `SMOKE_RUN_SDK_INTEGRATION=0` only when a faster example-only check is
needed. Its trap removes only its uniquely named containers, network, and
volume on success, failure, or interruption. Sanitized runtime evidence and the
SDK integration-test log are written below the ignored `target/smoke/`
directory.

Focused offline checks:

```sh
cargo test --locked --manifest-path examples/multi-node/Cargo.toml
cargo clippy --locked --manifest-path examples/multi-node/Cargo.toml \
  --all-targets --all-features -- -D warnings
```

To run a worker manually, copy `.env.example`, replace placeholders, export the
values, and choose one role:

```sh
HATCHET_NODE=orders PORT=19081 cargo run --locked \
  --manifest-path examples/multi-node/Cargo.toml
HATCHET_NODE=notifications PORT=19082 cargo run --locked \
  --manifest-path examples/multi-node/Cargo.toml
```

Submit without waiting, or observe at a three-second interval with a finite
timeout:

```sh
cargo run --locked --manifest-path examples/multi-node/Cargo.toml \
  --bin trigger -- --order-id ord_local_002 --email buyer@example.test \
  --total-cents 4299
cargo run --locked --manifest-path examples/multi-node/Cargo.toml \
  --bin trigger -- --wait --interval-seconds 3 --timeout-seconds 180 \
  --order-id ord_local_003 --email buyer@example.test --total-cents 4299
```

## Railway roles and variables

Both Rust services use the same image built from this repository and differ only
by `HATCHET_NODE`:

| Service | Role | Health |
| --- | --- | --- |
| `rust-orders-worker` | `HATCHET_NODE=orders` | `/health` on `$PORT` |
| `rust-notifications-worker` | `HATCHET_NODE=notifications` | `/health` on `$PORT` |

Both require `HATCHET_CLIENT_TOKEN`,
`HATCHET_CLIENT_HOST_PORT=hatchet-orchestrator.railway.internal:7077`,
`HATCHET_CLIENT_SERVER_URL=http://hatchet-orchestrator.railway.internal:8888`,
and `HATCHET_CLIENT_TLS_STRATEGY=none`. Plain gRPC is confined to Railway's
encrypted private network. Do not create worker domains or a public gRPC proxy.

Hatchet Lite uses the official pinned image, a `/config` volume, and Railway's
private PostgreSQL URL by reference. Its only public endpoint is the authenticated
dashboard on HTTP port 8888. This is a development/low-volume comparison, not a
high-availability production layout.

Prepare the auditable worker upload (about 1 MB) before deploying either role.
It includes the SDK's pinned protocol snapshot, uses normal ignore rules, and
excludes repository history, local environments, tests, documentation, and
build output. The Railway configuration and upload ignore rules live only in
this example directory; the preparation script copies them to the generated
context root that Railway receives:

```sh
./examples/multi-node/scripts/prepare-railway-context.sh
railway up examples/multi-node/target/railway/build-context \
  --path-as-root --service rust-orders-worker --environment production --detach
railway up examples/multi-node/target/railway/build-context \
  --path-as-root --service rust-notifications-worker --environment production --detach
```

Poll both detached deployments with `railway deployment list --json`; do not
treat an upload acknowledgment as a healthy deployment.

## Inspect the live project

The recorded comparison is running at
[hatchet-orchestrator-production.up.railway.app](https://hatchet-orchestrator-production.up.railway.app).
It presents Hatchet's login page; signup and invites are disabled.

Discover IDs instead of copying IDs from evidence:

```sh
railway login
railway list --json
railway link <PROJECT_ID>
railway environment link production
railway service list --json
railway deployment list --service hatchet-orchestrator --json
railway deployment list --service rust-orders-worker --json
railway deployment list --service rust-notifications-worker --json
```

Open the authenticated dashboard and inspect worker logs:

```sh
railway open
railway logs --service rust-orders-worker --lines 100
railway logs --service rust-notifications-worker --lines 100
```

Run the one-shot utility inside either worker without exposing gRPC:

```sh
railway ssh --service rust-orders-worker /app/trigger --wait \
  --interval-seconds 3 --timeout-seconds 180 --order-id ord_manual_001 \
  --email buyer@example.test --total-cents 4299
```

View resource metrics:

```sh
railway metrics --all --cpu --memory --since 1h
```

For a controlled idle comparison, leave both workers connected with no active
runs and capture the official defaults before applying the experimental profile:

```sh
RAILWAY_PROJECT_ID=<PROJECT_ID> RAILWAY_ENVIRONMENT_ID=<ENVIRONMENT_ID> \
POSTGRES_SERVICE_ID=<POSTGRES_SERVICE_ID> \
./examples/multi-node/scripts/railway-measure-profile.sh baseline

RAILWAY_PROJECT_ID=<PROJECT_ID> RAILWAY_ENVIRONMENT_ID=<ENVIRONMENT_ID> \
ORCHESTRATOR_SERVICE_ID=<ORCHESTRATOR_SERVICE_ID> \
./examples/multi-node/scripts/apply-low-load-profile.sh

RAILWAY_PROJECT_ID=<PROJECT_ID> RAILWAY_ENVIRONMENT_ID=<ENVIRONMENT_ID> \
POSTGRES_SERVICE_ID=<POSTGRES_SERVICE_ID> \
./examples/multi-node/scripts/railway-measure-profile.sh low-load
```

The final profile is deliberately conservative and source-version-specific:

| Variables | Value | Effect |
| --- | ---: | --- |
| `DATABASE_MAX_CONNS` / `DATABASE_MIN_CONNS` | `10` / `1` | Main database pool bounds |
| `DATABASE_MAX_QUEUE_CONNS` / `DATABASE_MIN_QUEUE_CONNS` | `10` / `1` | Postgres message-queue pool bounds |
| `SCHEDULER_CONCURRENCY_POLLING_MIN_INTERVAL` / `MAX_INTERVAL` | `5s` / `10s` | Concurrency-strategy polling backoff |
| `SCHEDULER_CHECK_ACTIVE_MIN_INTERVAL` / `MAX_INTERVAL` | `60s` / `120s` | Active-workflow controller checks |
| `SERVER_OPERATIONS_POLL_INTERVAL` / `SERVER_OPERATIONS_JITTER` | `10` / `1000` | Operations polling seconds and jitter milliseconds |

These are measurements for pinned Hatchet `v0.94.14`, not universal production
recommendations. Minimum and maximum ticker values must remain different.

To rotate the worker token, create a finite tenant token inside the orchestrator,
send it to both services with `railway variable set HATCHET_CLIENT_TOKEN --stdin`,
redeploy both workers, verify readiness, then revoke the old token in Hatchet.
Keep the token in a shell variable or pipe; never print it or put it in history.

To remove only this comparison project:

```sh
railway delete --project <PROJECT_ID> --yes
```

## Security and known floors

- Dashboard authentication, secure HTTPS cookies, restricted CORS, and closed
  signup/invites remain enabled.
- The tenant worker token is finite, shared only by the two private workers, and
  must never enter Git, build arguments, or evidence.
- The SDK result convenience path polls every 500 ms; this utility deliberately
  uses configurable three-second observation instead.
- Worker heartbeats remain at four seconds because the server's activity window
  is approximately five seconds.
- Official Hatchet retains hard-coded roughly one-second queue/replenishment
  loops and a five-second lease loop. No fictitious settings are used to hide
  that idle floor.
- Engine log ingestion can return 403 and fall back to an SDK-local buffer; that
  fallback is not accepted as workflow-execution evidence here.

See [AUDIT.md](AUDIT.md) for the checkout audit and final workspace verification.
[railway-evidence.json](railway-evidence.json) contains the sanitized
live topology, two completed run IDs, 10-minute measurements, and final settings.

### Versioned SDK parity proofs

`local-smoke.sh` defaults to the pinned v0.94.14 baseline. Set
`SMOKE_HATCHET_IMAGE` to an exact alternate image digest to test another release.
`SMOKE_RUN_MODERN_TESTS=1` additionally runs the v0.114.0 feature suite;
`SMOKE_RUN_RECOVERY_TESTS=1` runs the disposable engine-restart test. That test
requires the container name exported by the runner and never targets a remote
engine. Reports sum successful tests across the selected suites. See the root
[feature matrix](../../docs/parity/feature-support.md) for support floors and limits.
