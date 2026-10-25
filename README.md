# another-hatchet-sdk

An unofficial, Rust-first SDK for [Hatchet](https://hatchet.run/) workflow
orchestration. It provides typed workflow declarations, workers, child runs,
durable task primitives, events, and selected management clients.

This project is community-maintained and is not affiliated with or endorsed by
Hatchet. Version 0.1 targets Hatchet `v0.94.14`; the live multi-node example in
this repository has been exercised against that version.

## Install

The crates have not been published to crates.io yet. Clone this repository and
use a path dependency while evaluating the SDK (adjust the path for your project):

```toml
[dependencies]
another-hatchet-sdk = { path = "../hatchet_rust_sdk/crates/hatchet-sdk" }
```

After the first registry release, the corresponding installation will be:

```toml
[dependencies]
another-hatchet-sdk = "0.1.0"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
```

The task, workflow, and durable-task procedural macros are re-exported by
`another-hatchet-sdk`; no separate macro dependency is needed. Protocol
definitions and `protoc` are vendored, so consumers do not need a system
Protocol Buffers installation.

The minimum supported Rust version is 1.85.

## Offline quickstart

`InMemoryTransport` is intended for fast tests and local exploration. It
executes declared DAG levels concurrently and makes named parent outputs
available to downstream tasks.

```rust
use std::sync::Arc;

use another_hatchet_sdk::{
    Client, ContextExt, InMemoryContext, InMemoryTransport, Result, RunOpts,
    TaskConfig, WorkflowDeclaration,
};

#[tokio::main]
async fn main() -> Result<()> {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(transport);

    let prepare = client.task::<serde_json::Value, serde_json::Value>("prepare");
    let finish = client.task::<serde_json::Value, String>("finish");

    let worker = client
        .worker("quickstart-worker")
        .register_task(&prepare, |input, _ctx: InMemoryContext<_>| async move {
            Ok(serde_json::json!({ "message": input["message"] }))
        })
        .register_task(&finish, |_input, ctx: InMemoryContext<_>| async move {
            let prepared: serde_json::Value = ctx.parent_output("prepare").await?;
            Ok(format!("done: {}", prepared["message"].as_str().unwrap_or("")))
        })
        .build()
        .start()?;

    client
        .register_workflow(
            WorkflowDeclaration::<serde_json::Value, String>::new("quickstart")
                .step(&prepare, TaskConfig::default())
                .step_after(&finish, &[&prepare], TaskConfig::default()),
        )
        .await?;

    let output = client
        .workflow::<serde_json::Value, String>("quickstart")
        .run(serde_json::json!({ "message": "hello" }), RunOpts::default())
        .await?;

    assert_eq!(output, "done: hello");
    worker.shutdown().await?;
    Ok(())
}
```

## Connect to Hatchet

Set the normal Hatchet client variables, then construct `GrpcTransport`:

```text
HATCHET_CLIENT_TOKEN=...
HATCHET_CLIENT_HOST_PORT=localhost:7077
HATCHET_CLIENT_SERVER_URL=http://localhost:8888
HATCHET_CLIENT_TLS_STRATEGY=none
```

```rust,no_run
use std::sync::Arc;
use another_hatchet_sdk::{Client, GrpcTransport, Result, config::HatchetConfig};

async fn live_client() -> Result<Client<GrpcTransport>> {
    let config = HatchetConfig::from_env()?;
    let transport = GrpcTransport::from_config(config).await?;
    Ok(Client::new(Arc::new(transport)))
}
```

Use TLS for production endpoints. A worker reports ready only after registration
and its required streams are connected; it becomes unhealthy while reconnecting.
`WorkerHandle::shutdown` is bounded by the configured cancellation grace
period.

Historical durable-event waits must use the same scope on the event and the
condition. The SDK rejects an unscoped lookback immediately instead of leaving
the task waiting for a future event:

```rust,ignore
use std::time::Duration;
use another_hatchet_sdk::{Condition, EventRecord};

let scope = "order:ord_123";
client.events().push(
    EventRecord::new("payment.received", serde_json::json!({ "paid": true }))
        .with_scope(scope),
).await?;

let condition = Condition::event("payment.received")
    .scope(scope)
    .lookback(Duration::from_secs(60))
    .build();
```

The focused SDK demonstrations are collected in
[examples/features](examples/features/README.md). The complete live topology is
in [examples/multi-node](examples/multi-node/README.md): an official
Hatchet Lite `v0.94.14` service, PostgreSQL, two independent Rust workers,
health checks, a trigger utility, and local/Railway smoke scripts.

## API notes

- `Client::register_workflow(...).await` is fallible and does not record a
  local declaration when the engine rejects registration.
- Parent output is selected by task name:
  `ctx.parent_output::<Output>("parent-task").await?`.
- DAG results select terminal task outputs even when the submitting client did
  not register the workflow. A single terminal task returns its output directly;
  multiple terminal tasks return a map keyed by task name.
- Hatchet v0.94.14 caches workflow versions for five seconds when triggering by
  name. Allow that cache to expire after deploying a changed workflow graph.
- An unused failure handler can leave a successful graph's engine status running
  on v0.94.14. The SDK can return its completed task outputs while status inspection
  continues to report the engine's state.
- Concurrent durable operations are serialized through their engine
  acknowledgments within each task invocation; their subsequent waits can overlap.
- Run status includes queued, running, completed, failed, cancelled, and unknown
  engine states. `delay()` schedules an absolute timestamp computed from its duration.
- Result metadata reads retry transient REST failures and replication lag using
  `GrpcTransport::with_retry_config`. Each request has a ten-second timeout;
  `max_elapsed` also bounds the full retry cycle when configured.
- `runs().get_task_external_id(run_id, task_name)` resolves engine state even
  for a fresh client; tasks that have not started return `None`.
- Durable recovery takes a **task external ID**, not a workflow run ID.
  `branch_durable_task(task_id, node_id, branch_id)` returns the new checkpoint;
  `restore_task(task_id)` requeues an evicted task without replacing its durable
  log. These operations require an engine-backed transport.
- `run.stream_events().await?` and `runs().subscribe_to_stream(run_id).await?`
  return an incremental `RunEventStream` implementing `futures_core::Stream`.
  Read it with `while let Some(value) = stream.next().await`; there is no idle
  cutoff. `run.stream()` remains a convenience that collects until completion.
  Dropping a stream closes the subscription, and transport errors are returned
  instead of a successful partial batch.
- Engine workflow streams are live-only: subscribe while a run is active.
  They do not replay history or automatically reconnect without a resume cursor.
  In-memory subscriptions replay their buffered values before following new ones.
- Recovery responses acknowledge an asynchronous engine operation. Use branch
  coordinates from the durable log, and observe the new execution before treating
  an earlier run snapshot as the recovered result.
- `client.engine_version().await?` fetches a semantic engine release. Capability
  checks cover slot configuration, durable eviction, observability and retry-after
  delays using the official SDK prerequisites. Worker startup checks its slot
  and durable prerequisites; unknown development versions return an error.
- `runs().list_page(RunListOptions { ..Default::default() })` supports offset,
  limit, time bounds, multiple workflow names and statuses, metadata, worker,
  parent-task and event filters. Workflow names are resolved to engine IDs.
  Pages retain pagination plus execution timestamps, output and metadata in
  `RunRecord::summary`; `get_details` exposes typed task details.
- `cancel_many` and `replay_many` take `RunSelection::ExternalIds` or `::Filter`.
  Omitted filter start times mean one hour ago. Results contain **task** external
  IDs. Replay preserves the existing workflow ID; it does not create a new run.
  Management writes are not automatically retried.
- Workflow deletion, filter lookup/update, CEL debugging, tenant lookup and
  schedule rescheduling/bulk operations use engine APIs. Bulk schedule responses
  retain successful IDs and per-item errors. Schedule updates change trigger time.
  CEL debugging is asynchronous and requires an engine; local transports do not
  pretend to evaluate expressions.
- Inbound event webhooks use `webhooks().create_inbound`, `get`, `update`,
  `list_inbound` and `delete_inbound`, with typed source/authentication options.
  The older `create(name, url)`, `list` and `delete` methods configure legacy
  webhook **workers**, a separate engine resource.
- `metrics()` exposes `get_task_status_metrics`, `get_task_queue_metrics`,
  `get_task_stats` and `scrape_prometheus_metrics`. Legacy workflow-run and tenant
  queue count methods return `UnsupportedOperation` on engine clients because
  the pinned engine deprecated those handlers. Local test counters remain available.
- Workflow pause/unpause uses the newer pause/queue-policy contract and verifies
  the returned state. The SDK requires v0.111.0 or later, a conservative support
  floor verified against that release rather than the earliest possible version.
  The pinned v0.94.14 engine is rejected before sending a mutation.
- Prometheus scraping requires the engine's Prometheus federation configuration
  and any tenant entitlement. Engine rejections are returned with their explanation.
- Hatchet engine schedules require JSON object inputs. Intermediate DAG outputs
  consumed by child tasks must also be objects; use structs or JSON object values.
- Namespaces configured in `HatchetConfig` are applied consistently to
  registration and run handles.
- `Error::NonRetryable` sets Hatchet's no-retry completion flag.
- Engine-backed management operations that are not implemented return
  `Error::UnsupportedOperation`; they never silently mutate process-local
  shadow state.
- The compatibility `grpc` Cargo feature is currently a no-op. Production
  transport support is part of the v0.1 base crate, so default and
  `--no-default-features` builds expose the same API.

## Official SDK feature support

The [feature support matrix](docs/parity/feature-support.md) compares the published
TS and Go SDKs, with immutable source references, Rust engine requirements and
named tests. [upstream.json](docs/parity/upstream.json) records npm/Go registry
versions separately from official `main` and GitHub's latest-release label.

- Worker `.middleware(layer)` wraps both normal and durable task execution.
  Implement `TaskMiddleware::handle(invocation, next)` and call
  `next.run(invocation).await` to continue. The first registered layer is outermost;
  layers may replace input/output, reject execution, or short-circuit it. `Next`
  is consumed once, and execution identity is immutable.
- The default `opentelemetry` feature emits submission/task spans and propagates
  W3C `traceparent`/`tracestate` through normal and durable children. Applications
  configure an OpenTelemetry **0.32** provider/exporter; the SDK does not install a
  global provider, send telemetry without one, or configure Hatchet collector
  credentials automatically. Async context is scoped per future poll. Disable
  default features to omit this instrumentation while keeping the transport API.
- `WorkflowDeclaration::idempotency(IdempotencyConfig { expression, ttl, method })`
  requests engine-owned TTL or STATUS deduplication, including one-task workflows.
  TTL is a positive whole-millisecond `Duration`; STATUS releases the key at a
  terminal outcome, with TTL as a fallback cap. The verified SDK floor is v0.111.0.
  Local transports reject this configuration rather than simulate an engine claim.
- Submission collisions return `Error::IdempotencyCollision` with the existing run
  ID; bulk collisions preserve successful IDs plus collision details in
  `Error::BulkIdempotencyCollision`. These errors are not retried. Reattach to
  successful bulk IDs with `client.run_ref(id)`; retrying the entire batch could
  duplicate work that succeeded. Submission uses the official gRPC contracts so
  collision details and routing information are retained for fresh clients too.
- `rate_limits().pause(key, until: SystemTime)` holds queued work behind a rendered
  key and returns `RateLimitPauseResult` with the actual engine pause time. Earlier
  requests do not shorten an active pause. This requires engine v0.114.0 or later.
- `RunOpts::sticky` supplies an explicit desired worker ID on both single and bulk
  normal dispatch, as well as durable children. Workflow `Soft`/`Hard` sticky
  policy is a separate registration setting.

Run `python3 scripts/parity.py check-upstream` to check newer official versions;
use `refresh`, review the changed implementations, then update `features.json`
and run `render`. `render --check` validates evidence and generated documentation
offline. CI supplies baseline/current engine jobs and a weekly/manual upstream
version-drift check; support claims are never promoted automatically on a release.

## Examples

From the repository root:

```sh
cargo run -p another-hatchet-sdk-examples --bin basic
cargo run -p another-hatchet-sdk-examples --bin dag
cargo run -p another-hatchet-sdk-examples --bin concurrency
cargo run -p another-hatchet-sdk-examples --bin durable_execution
cargo run -p another-hatchet-sdk-examples --bin conditions
cargo run -p another-hatchet-sdk-examples --bin macros
cargo run -p another-hatchet-sdk-examples --bin child_workflows
cargo run -p another-hatchet-sdk-examples --bin standalone_functions
```

## Validation

```sh
just lint
just test
just check-features
just check-examples
just doc
just package-check
```

Live integration tests are intentionally opt-in and require a reachable Hatchet
engine:

```sh
cargo test -p another-hatchet-sdk --features integration \
  --test integration -- --ignored --nocapture
```

For a credential-free, isolated local proof, Docker users can run:

```sh
./examples/multi-node/scripts/local-smoke.sh
```

That command starts the pinned official Hatchet Lite `v0.94.14` image and
PostgreSQL 16, runs the two-process example, executes the live SDK tests
serially—including a scoped, namespaced durable-event lookback—verifies both
long-lived workers recover to healthy, and removes only the containers, network,
and volume it created.

## Releasing

The workspace publishes in dependency order:
`another-hatchet-core`, `another-hatchet-macros`, then
`another-hatchet-sdk`.

From a clean branch that can be pushed to `origin`:

```sh
just release
```

The guarded release command prepares versions and the changelog with
`release-plz`, validates the exact package archives in an isolated temporary
consumer, commits and pushes the release metadata, publishes all three crates,
and finally pushes the version tag. Use `just package-check` for a dirty-tree
safe preflight without publishing. The release machine must have the declared
Rust 1.85 toolchain installed through `rustup`; the preflight compiles both the
workspace and standalone Railway consumer with that exact compiler before it
creates any package archive.
