# Hatchet Rust SDK: one orchestrator, two Rust workers, local and Railway proof

Execute this prompt from the Hatchet Rust SDK repository:

```text
/Users/christophercaldwell/Code/projects/rust/hatchet_rust_sdk
```

## Mission

Turn the existing SDK into a concrete, inspectable comparison with the Inngest multi-service demo:

```text
one official Hatchet orchestrator
              |
              +--> Rust orders worker: process-order
              |
              +--> Rust notifications worker: send-order-receipt
              |
              +--> PostgreSQL: Hatchet's durable state and Postgres-backed queue
```

Build and verify a standalone example, prove it locally against an official unmodified Hatchet server, deploy it to a new Railway project, and prove the cross-worker workflow while it is live. Leave the Railway deployment running so the user can inspect it.

This SDK is already substantially implemented. Begin with an evidence-based progress audit; do not rebuild the SDK from scratch or trust stale progress prose over current code and tests.

## Scope and ownership

- Work only in `/Users/christophercaldwell/Code/projects/rust/hatchet_rust_sdk` and in the new Railway project created for this Hatchet comparison.
- Do not modify `/Users/christophercaldwell/Code/projects/inngest_rust` or any existing Inngest Railway services.
- The Rust SDK in this repository is the implementation under test. Do not replace it with an official Hatchet Rust/Go/TypeScript/Python SDK.
- Use an official, unmodified Hatchet server image. The embedded official source under `sources/hatchet-main` is a reference and must not be edited.
- Do not patch or fork the Hatchet server to change its scheduler loops. If a desired low-load setting is unavailable, measure and document that limit.
- Preserve all pre-existing user changes. Inspect `git status` before editing, keep unrelated files untouched, and never use destructive Git commands.
- Do not publish a crate, create a release, push a branch, or open a pull request unless the user separately asks.

## Required operating approach

Use the `use-railway` skill for all Railway work. Read its complete instructions before any Railway action. Prefer Railway's dedicated tools when available; when using the CLI, prefix every invocation with:

```sh
RAILWAY_CALLER=skill:use-railway@1.3.5
RAILWAY_AGENT_SESSION=hatchet-rust-sdk-comparison-20260715
```

Keep the same `RAILWAY_AGENT_SESSION` value for the whole task. Use explicit project, environment, and service IDs after discovery. Use JSON output where the command supports it. For detached deployments, poll until terminal `SUCCESS`; treat `FAILED`, `CRASHED`, `REMOVED`, or timeout as failure. After every mutation, read the state back.

Do not expose, print, commit, or copy secrets into evidence. In particular, redact:

- `HATCHET_CLIENT_TOKEN`
- `ADMIN_PASSWORD`
- `SERVER_AUTH_COOKIE_SECRETS`
- database URLs and passwords
- generated encryption/JWT key material

## Facts already established from the local repository

Verify these against the current checkout rather than assuming they remain true:

- The checkout was at commit `05fa2d489d036d17f9c6476684f92aa4a3392298` when this prompt was prepared.
- The root README is stale in places. It presents `InMemoryTransport` as the quickstart and lists old example binary names, while several current examples already use `GrpcTransport`.
- `crates/hatchet-sdk/src/config.rs` supports real authenticated engine configuration through `HATCHET_CLIENT_TOKEN`, `HATCHET_CLIENT_HOST_PORT`, `HATCHET_CLIENT_SERVER_URL`, and TLS settings.
- `crates/hatchet-sdk/src/grpc_transport.rs` contains worker registration, streaming listeners, heartbeats, workflow registration, workflow triggering, and run-detail polling.
- `crates/hatchet-sdk/src/worker.rs` supports `.health_port(...)`; `crates/hatchet-sdk/src/health.rs` binds `0.0.0.0` and serves `GET /health` with worker name, health, slots, actions, and labels.
- Existing live integration tests in `crates/hatchet-sdk/tests/integration.rs` return successfully with a printed `SKIP` when no usable token/engine exists. A green exit code alone is therefore not live proof.
- `docs/todo/4/TODO.md` says P0-P5 and P6.1 were complete, but also says P6.2 remained. Current files include a parity matrix, so verify the executable state and update stale status documents only if the evidence justifies it.
- The documented logs integration has a known `403` fallback to an SDK-local buffer. Do not represent that fallback as proof of engine-backed logs.
- `protoc` is required by the current build.
- `examples/.env` is ignored and may contain a local secret. Never read it into logs, commit it, or reuse its token against a new server without confirming that it belongs to that exact Hatchet instance.

## Phase 1: audit actual SDK progress

Before adding the comparison example:

1. Inspect the workspace, existing examples, tests, current Git state, and recent history.
2. Produce a short audit table mapping each claimed capability needed by this exercise to code and tests:
   - environment configuration;
   - authenticated `GrpcTransport`;
   - workflow definition registration;
   - workflow triggering without waiting;
   - separate workers registering separate task actions;
   - worker readiness/health;
   - live engine tests;
   - result/status observation.
3. Run the existing offline verification suite. At minimum:

   ```sh
   cargo fmt --all --check
   cargo clippy --workspace --all-targets --all-features -- -D warnings -W clippy::pedantic
   cargo test --workspace --all-features
   ```

4. Record ignored/skipped tests separately. Do not call a live test successful unless a real official Hatchet engine and a valid token were present and the expected assertions executed.
5. If an existing SDK defect prevents the requested example, diagnose it narrowly and fix it with focused tests. Do not broaden this into a parity rewrite.
6. Do not ignore `Result` values in new demo code. Workflow registration, worker startup, readiness, trigger, and observation failures must all fail the command or smoke test.

## Phase 2: select and pin the official Hatchet server

Use primary sources only: current official Hatchet documentation, the official `hatchet-dev/hatchet` repository, GHCR metadata, and the embedded source snapshot. Record URLs, retrieval date, source commit/tag, image tag, and immutable image digest.

Requirements:

- Use `ghcr.io/hatchet-dev/hatchet/hatchet-lite` from Hatchet's official registry.
- Do not leave `:latest` as the reproducibility pin. Resolve and record an immutable digest. Prefer a stable version tag plus digest when available.
- Confirm the chosen image is protocol-compatible with this Rust SDK using a real local smoke before Railway deployment.
- Explicitly configure Postgres as the Hatchet message queue with `SERVER_MSGQUEUE_KIND=postgres`. Do not add RabbitMQ for this low-volume comparison.
- Use PostgreSQL. Do not attempt SQLite; Hatchet Lite's supported minimal infrastructure is Postgres.
- Preserve Hatchet Lite's `/config` directory with a persistent volume so generated encryption keys remain stable and issued worker tokens survive redeploys.
- Hatchet Lite is appropriate here because Hatchet documents it for development and low-volume use. Do not describe the result as a high-availability production topology.

Railway currently has marketplace entries with template codes `hatchet-lite` and `7oEFNd`, but neither returned as verified when this prompt was researched on 2026-07-15. Re-check this at execution time. Prefer recreating the small stack manually from the official image plus Railway Postgres so every setting is explicit. If a template is used, audit its image source, variables, volume, ports, and database service first; record the template creator and verification status. Never substitute a community-modified Hatchet server image.

## Phase 3: build a standalone multi-worker example

Create a self-contained example under:

```text
examples/railway-multi-node/
```

Mirror the useful packaging qualities of the Inngest comparison and the locally published Rust AI SDK examples:

- its own `Cargo.toml`, `Cargo.lock`, and `[workspace]` boundary;
- a path dependency on this repository's `another-hatchet-sdk` crate;
- a concise README with local commands, Railway roles, variables, security model, and teardown instructions;
- `.env.example` containing placeholders only;
- a multi-stage, non-root Dockerfile;
- Railway configuration and repeatable local/live smoke tooling;
- sanitized evidence output under an ignored `target/` directory and one small committed sanitized evidence summary if the live proof passes.

Do not replace the existing examples package. Add this as an intentionally standalone consumer example.

### Demonstration semantics

Implement one typed DAG named `rust-order-processing`:

```text
OrderInput
  { order_id, email, total_cents }
        |
        v
orders worker / process-order
  -> { order_id, email, total_cents, status: "inventory-reserved" }
        |
        v
notifications worker / send-order-receipt
  -> { order_id, recipient, subject }
```

Requirements:

- `rust-orders-worker` registers only the `process-order` handler.
- `rust-notifications-worker` registers only the `send-order-receipt` handler.
- The orders process owns registration of the complete workflow declaration. Do not let both services race to register different declarations.
- Both processes may share declarations, typed input/output models, and stable action names in a small library module.
- Use `step_after` or the SDK's equivalent to make the second task depend on the first.
- Give both workers distinct labels such as `service=orders` and `service=notifications`.
- Configure very small demo capacity, for example `.slots(2).durable_slots(2)`, rather than the SDK defaults of 100 and 1000.
- Read Railway's injected `PORT`, pass it to `.health_port(...)`, and fail clearly if it is invalid.
- Wait for worker readiness before reporting startup success.
- Handle SIGTERM/Ctrl-C and shut the worker down cleanly.
- Emit structured, secret-free logs carrying the same `order_id` and run ID so cross-worker execution is obvious.
- Keep side effects fake and deterministic. The notification step returns a receipt; it must not send real email.
- Package both worker binaries and a trigger/inspect utility in the same image. Select the long-lived role with `HATCHET_NODE=orders|notifications`.

### Trigger and observation behavior

The deployed workers must not synchronously wait for their own workflows. The current engine transport's `WorkflowRunRef::output()` path polls `GetRunDetails` every 500 ms for up to five minutes. Long-lived workers should use `run_no_wait` when they trigger anything.

Provide a one-shot trigger/inspect utility that:

1. submits a workflow with `run_no_wait`;
2. prints the run ID;
3. polls status only when explicitly asked to wait;
4. uses a configurable low-frequency interval, defaulting to 3 seconds rather than 500 ms;
5. has a finite timeout and reports failed/cancelled/unknown states correctly;
6. can be invoked inside a Railway worker container so public gRPC is unnecessary.

Do not change core SDK polling merely to make this example. If measurement proves the SDK's fixed 500 ms result poll is materially harmful, document a focused future API proposal; only implement it if the user explicitly expands scope.

### Required tests

Add focused tests for:

- payload preservation from order to receipt;
- stable task/workflow names;
- the DAG dependency edge;
- role selection and missing/invalid environment values;
- low-frequency wait configuration and timeout;
- health response/action identity;
- secret redaction;
- smoke cleanup on both success and injected failure.

Run at least:

```sh
cargo test --locked --manifest-path examples/railway-multi-node/Cargo.toml
cargo clippy --locked --manifest-path examples/railway-multi-node/Cargo.toml --all-targets --all-features -- -D warnings
```

## Phase 4: local full-stack proof

Create one command that starts and cleans up all four local processes/containers:

1. PostgreSQL;
2. the pinned official Hatchet Lite image;
3. orders worker;
4. notifications worker.

The runner must use isolated ports, wait for real readiness, obtain/use a token for that local instance, register the workflow, trigger one order, and prove both task actions ran. It must always clean up processes/containers on success, failure, and interruption. It must not delete unrelated containers or volumes.

The proof is invalid if:

- the workers used `InMemoryTransport`;
- the server was not the pinned official Hatchet image;
- either worker was absent;
- the workflow completed entirely in one worker;
- an integration test printed `SKIP`;
- only registration was observed but no live run completed;
- the run succeeded only through SDK-local fallback state.

Write a sanitized local report containing the official image digest, both worker names/actions, workflow/run ID, step order, result, timings, and cleanup status.

## Phase 5: low-load PostgreSQL research and configuration

The goal is lower idle database and compute load, not millisecond dispatch latency. Distinguish these three things in both implementation and reporting:

1. PostgreSQL query/transaction frequency;
2. Hatchet orchestrator memory/CPU;
3. each Rust worker's memory/CPU.

Do not claim that polling caused a 500 MB process merely because both were observed. Measure each service separately.

### Source-confirmed controls to evaluate

The official source snapshot available during prompt research showed these controls:

| Setting | Source default | Low-load candidate |
| --- | ---: | ---: |
| `DATABASE_MAX_CONNS` | 50 | 10 |
| `DATABASE_MIN_CONNS` | version-dependent docs/source disagreement; inspect image | 1 |
| `DATABASE_MAX_QUEUE_CONNS` | 50 | 10 |
| `DATABASE_MIN_QUEUE_CONNS` | 10 | 1 |
| `SCHEDULER_CONCURRENCY_POLLING_MIN_INTERVAL` | 500ms | 5s |
| `SCHEDULER_CONCURRENCY_POLLING_MAX_INTERVAL` | 5s | 10s |
| `SCHEDULER_CHECK_ACTIVE_MIN_INTERVAL` | 30s in the inspected source | 60s |
| `SCHEDULER_CHECK_ACTIVE_MAX_INTERVAL` | 60s in the inspected source | 120s |
| `SERVER_OPERATIONS_POLL_INTERVAL` | 2 seconds | 10 seconds |
| `SERVER_OPERATIONS_JITTER` | 0ms | 1000ms |

Rules:

- Verify every variable against the exact official image version/source before setting it. Current public docs omit the two `SCHEDULER_CHECK_ACTIVE_*` variables even though the inspected official source bound them; source/image verification is mandatory.
- Keep random ticker minimum strictly less than maximum. The inspected implementation calls `rand.Int63n(max-min)`, so equal values can panic.
- Apply low-load values as an experimental profile, not universal recommendations.
- Do not use concurrency/rate-limit features in the demo merely to exercise the concurrency polling loop.
- Do not add PgBouncer initially. It can improve connection reuse, but it is another always-on service and does not eliminate Hatchet's polling. Consider it only if measured connection churn or pool pressure justifies the extra compute.
- Do not enable `pg_stat_statements` or another PostgreSQL extension without user approval. If it is already enabled, use it read-only.
- Keep log ingestion available for the demo unless measurement shows it is the dominant unnecessary write source. Do not sacrifice the ability to prove both nodes ran merely to report a lower number.

### Known non-configurable floors

The inspected official source also contained hard-coded loops, including roughly 1-second queue/replenishment/rate-limit loops and a 5-second lease loop. This Rust SDK sends worker heartbeats every 4 seconds, and the inspected server queries treated workers as active only with a heartbeat in approximately the last 5 seconds. Therefore:

- do not lengthen the SDK heartbeat independently; it would make healthy workers appear dead;
- do not invent environment variables for hard-coded loops;
- do not patch the official server;
- quantify the remaining idle floor and report it as a Hatchet Lite limitation for this comparison.

### Measurement protocol

Measure the same live topology in two profiles:

1. baseline official defaults, two idle workers, no active runs;
2. final low-load profile, the same two idle workers, no active runs.

Use a meaningful stable window, ideally 10 minutes per profile after warm-up. Capture:

- Railway CPU and memory metrics for orchestrator, Postgres, orders, and notifications;
- Postgres connection count from `pg_stat_activity`;
- deltas from `pg_stat_database` for commits, rollbacks, blocks, and tuple operations, normalized per minute;
- top statements only if `pg_stat_statements` is already available;
- one completed workflow's end-to-end latency after tuning.

Then run one workflow and confirm the lower polling profile remains functionally correct. Record before/after values even if there is no improvement. Explain which settings affect pool size, periodic controller work, concurrency strategies, client result waiting, and worker liveness; do not collapse them into one generic "polling" claim.

## Phase 6: Railway topology and security

Create a new, dedicated Railway project in the same practical region as the Inngest comparison when possible. Do not alter the existing Inngest project. The final environment should contain exactly these application roles plus their database:

| Role | Suggested Railway service | Public? | Port/path |
| --- | --- | --- | --- |
| Official orchestrator/dashboard | `hatchet-orchestrator` | Yes, authenticated | HTTP `8888` |
| PostgreSQL | `Postgres` | No | private `5432` |
| Rust orders node | `rust-orders-worker` | No | private health on `$PORT` |
| Rust notifications node | `rust-notifications-worker` | No | private health on `$PORT` |

### Networking

- Put all services in the same Railway environment and use Railway private DNS.
- Give the orchestrator the stable private endpoint name `hatchet-orchestrator` if needed.
- Workers should use values equivalent to:

  ```text
  HATCHET_CLIENT_HOST_PORT=hatchet-orchestrator.railway.internal:7077
  HATCHET_CLIENT_SERVER_URL=http://hatchet-orchestrator.railway.internal:8888
  HATCHET_CLIENT_TLS_STRATEGY=none
  ```

  Plain gRPC is acceptable only on Railway's encrypted private network. Never expose it publicly in this baseline.
- Configure the orchestrator's gRPC bind/broadcast addresses explicitly for private access on `7077`, including its internal-client broadcast setting.
- Generate only one public Railway domain, targeting Hatchet Lite's HTTP runtime on port `8888`.
- Do not generate public worker domains, a public PostgreSQL proxy, or a public gRPC TCP proxy.
- If the user later wants to run a local client directly, document a temporary TCP-proxy option with removal steps, but do not enable it by default.

### Hatchet Lite port/health behavior

Verify this against the pinned image. The inspected official image serves:

- dashboard/API reverse proxy on `LITE_RUNTIME_PORT`, normally `8888`;
- gRPC on the configured `SERVER_GRPC_PORT`, use `7077`;
- engine readiness on `SERVER_HEALTHCHECK_PORT`, normally `8733`, at `/ready`.

Railway health checks normally target `$PORT`. A suitable explicit arrangement is:

```text
PORT=8733
SERVER_HEALTHCHECK_PORT=8733
LITE_RUNTIME_PORT=8888
```

with Railway's service health path set to `/ready` and the public domain explicitly targeting `8888`. Prove both `/ready` and the authenticated dashboard route work; do not assume automatic port detection chose the right listener.

Each Rust worker should bind its SDK health server to its own injected `$PORT` and use Railway health path `/health`.

### Authentication and secret bootstrap

- Keep Hatchet dashboard authentication enabled. Do not use `hatchet-lite-dev` or `--disable-auth` on Railway.
- Set a unique strong `ADMIN_PASSWORD`; never retain Hatchet's documented `Admin123!!` default.
- Set a real admin email supplied by the user or a clearly identified demo-only address they approve.
- Set secure cookie behavior for the HTTPS public domain:
  - `SERVER_AUTH_COOKIE_DOMAIN` is the bare Railway host;
  - `SERVER_AUTH_COOKIE_INSECURE=false`;
  - use strong stable `SERVER_AUTH_COOKIE_SECRETS` or the image's persistent generated config;
  - allow only the actual dashboard origin in CORS where supported.
- Disable open signup and invites after the seeded admin/tenant exist: `SERVER_ALLOW_SIGNUP=false`, `SERVER_ALLOW_INVITES=false`. Confirm the admin can still log in.
- Seed one stable demo tenant with an explicit UUID/name/slug, or verify Hatchet Lite's seeded default tenant and record its non-secret ID.
- Create a tenant-scoped worker API token with a finite expiration and a descriptive name such as `railway-rust-demo`.
- Prefer the Hatchet dashboard's Settings -> API Tokens flow if it avoids exposing the token to command transcripts. If using `hatchet-admin token create`, capture the value without echoing it into logs and immediately store it as a Railway secret/reference shared by both worker services.
- Never put the token in `railway.toml`, Docker build arguments, README commands, evidence, or Git.

### Railway variables

Use Railway reference variables rather than copying database secrets. At minimum configure and read back:

```text
# orchestrator
DATABASE_URL=${{Postgres.DATABASE_URL}}
SERVER_MSGQUEUE_KIND=postgres
SERVER_GRPC_BIND_ADDRESS=0.0.0.0
SERVER_GRPC_PORT=7077
SERVER_GRPC_INSECURE=true
SERVER_GRPC_BROADCAST_ADDRESS=hatchet-orchestrator.railway.internal:7077
SERVER_INTERNAL_CLIENT_INTERNAL_GRPC_BROADCAST_ADDRESS=hatchet-orchestrator.railway.internal:7077
LITE_RUNTIME_PORT=8888
PORT=8733
SERVER_HEALTHCHECK_PORT=8733
SERVER_URL=https://<hatchet-public-domain>
SERVER_AUTH_COOKIE_DOMAIN=<hatchet-public-domain-without-scheme>
SERVER_AUTH_COOKIE_INSECURE=false
SERVER_AUTH_SET_EMAIL_VERIFIED=true
SERVER_ALLOW_SIGNUP=false
SERVER_ALLOW_INVITES=false

# both workers
HATCHET_CLIENT_TOKEN=<secret or shared-variable reference>
HATCHET_CLIENT_HOST_PORT=hatchet-orchestrator.railway.internal:7077
HATCHET_CLIENT_SERVER_URL=http://hatchet-orchestrator.railway.internal:8888
HATCHET_CLIENT_TLS_STRATEGY=none
HATCHET_CLIENT_NAMESPACE=railway-demo
HATCHET_CLIENT_LOG_LEVEL=info

# per worker
HATCHET_NODE=orders
HATCHET_NODE=notifications
```

Add the verified final low-load variables to the orchestrator only after capturing the baseline.

### Deployment discipline

- Provision Postgres using Railway's official Postgres template/service and keep it private.
- Connect `hatchet-orchestrator` directly to the pinned official image, not to this Rust repository.
- Attach one volume at `/config` to `hatchet-orchestrator`.
- Deploy the same locally built Rust example image/source to both worker services with different `HATCHET_NODE` values.
- Set restart policy and health checks explicitly. Keep Railway sleeping/serverless disabled for all four always-on services; long-lived gRPC workers cannot sleep safely.
- Deploy in dependency order: Postgres, orchestrator, token bootstrap, workers.
- Inspect build and deploy logs for each service, but redact secrets before saving evidence.
- Read back service source, variables by key/presence, private networking, volume, domain target port, health configuration, deployment status, and replica count.

## Phase 7: live Railway proof

Do not stop at successful deployment. Prove the live topology:

1. PostgreSQL is healthy and private.
2. Hatchet `/ready` returns success and the public dashboard presents a login rather than an unauthenticated admin view.
3. Open signup is disabled.
4. Dashboard/API shows one workflow and both distinct worker names/actions.
5. Railway health checks pass for both workers.
6. From inside a worker container, run the one-shot utility with a unique order such as `ord_railway_001`.
7. Capture the run ID and wait at the configured 3-second observation interval.
8. Prove `process-order` ran on `rust-orders-worker`.
9. Prove `send-order-receipt` ran on `rust-notifications-worker` after it.
10. Confirm the final typed receipt and dashboard run state.
11. Restart one Rust worker, wait for readiness, and run a second order to prove registration reconnects cleanly.
12. Capture the baseline and low-load resource/database measurements described above.

The live proof must use the deployed official Hatchet orchestrator and the deployed Rust SDK workers. A local-only or in-memory success is insufficient.

## User-facing commands

The example README must give copy/paste commands for the user to:

- authenticate and select the dedicated Railway project/environment;
- list all four services and see their deployment status;
- open the authenticated Hatchet dashboard;
- view each worker's logs;
- invoke the trigger utility through Railway SSH without exposing gRPC;
- inspect the returned run in the dashboard;
- view current Railway CPU/memory metrics;
- rotate the Hatchet worker token;
- tear down only this dedicated comparison project when finished.

Use placeholders or discovery commands for IDs and secrets. Do not bake the agent's local IDs or tokens into executable source.

## Evidence and documentation

Update the root README only where needed to correct demonstrably stale example instructions and link the new standalone example. Keep broad SDK parity claims aligned with actual tests.

Add a sanitized `examples/railway-multi-node/railway-evidence.json` only after the live proof passes. It should include:

- schema version and timestamp;
- SDK Git commit;
- official Hatchet source/tag and image digest;
- Railway project/environment and service names/IDs;
- sanitized deployment IDs/statuses;
- public dashboard URL only;
- private topology described without credentials;
- workflow/run ID and ordered task/worker observations;
- local and Railway test results;
- baseline versus low-load metrics table;
- final low-load variable names and non-secret values;
- known hard-coded polling/heartbeat floors;
- overall pass/fail status.

Do not include the admin password, token, cookie secrets, database URL, database password, private key material, or raw environment dumps.

## Acceptance criteria

This task is complete only when all of the following are true:

- Existing SDK progress was audited from code/tests, not copied from status prose.
- Offline workspace checks pass or any unrelated pre-existing failure is clearly isolated with evidence.
- A standalone path-dependent example exists at `examples/railway-multi-node`.
- One official pinned Hatchet Lite plus Postgres and two separate Rust workers pass locally.
- No live test was counted if it skipped due to missing credentials.
- Railway has one dedicated Hatchet orchestrator, one private Postgres, and two independent Rust worker services.
- Only the authenticated Hatchet dashboard is public.
- The workflow crosses from the orders worker to the notifications worker and completes live.
- A second run succeeds after a worker restart.
- Baseline and tuned idle resource/database measurements are recorded.
- Supported low-load controls are applied and re-tested; unsupported hard-coded floors are stated plainly.
- The final README lets the user reproduce, inspect, rotate credentials, and tear down the setup.
- Evidence is sanitized and no secrets were committed or printed in the final response.
- The Inngest repository and its Railway deployment were not changed.

## Final report format

Lead with the outcome. Then provide:

1. a four-row service-role table;
2. the local and Railway proof results;
3. the baseline versus low-load measurement table;
4. the exact final polling/pool settings and what each affects;
5. the public authenticated dashboard link and the commands the user can run;
6. files changed with clickable absolute paths;
7. any remaining SDK or official Hatchet limitations, especially the 500 ms result poll, 4-second worker heartbeat, hard-coded server loops, and logs fallback;
8. confirmation that secrets were redacted and the Inngest setup was untouched.

Do not describe a deployment as tested until the live cross-worker run has actually completed.
