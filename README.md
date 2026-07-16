# another-hatchet-sdk

An unofficial, Rust-first SDK for [Hatchet](https://hatchet.run/) workflow
orchestration. It provides typed workflow declarations, workers, child runs,
durable task primitives, events, and selected management clients.

This project is community-maintained and is not affiliated with or endorsed by
Hatchet. Version 0.1 targets Hatchet `v0.94.14`; the live multi-node example in
this repository has been exercised against that version.

## Install

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
            Ok(format!("done: {}", prepared["message"]))
        })
        .build()
        .start()?;

    client
        .register_workflow(
            WorkflowDeclaration::new("quickstart")
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

The complete live topology is in
[examples/railway-multi-node](examples/railway-multi-node/README.md): an official
Hatchet Lite `v0.94.14` service, PostgreSQL, two independent Rust workers,
health checks, a trigger utility, and local/Railway smoke scripts.

## API notes

- `Client::register_workflow(...).await` is fallible and does not record a
  local declaration when the engine rejects registration.
- Parent output is selected by task name:
  `ctx.parent_output::<Output>("parent-task").await?`.
- Namespaces configured in `HatchetConfig` are applied consistently to
  registration and run handles.
- `Error::NonRetryable` sets Hatchet's no-retry completion flag.
- Engine-backed management operations that are not implemented return
  `Error::UnsupportedOperation`; they never silently mutate process-local
  shadow state.
- The compatibility `grpc` Cargo feature is currently a no-op. Production
  transport support is part of the v0.1 base crate, so default and
  `--no-default-features` builds expose the same API.

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
./examples/railway-multi-node/scripts/local-smoke.sh
```

That command starts the pinned official Hatchet Lite `v0.94.14` image and
PostgreSQL 16, runs the two-process example, executes all nine live SDK tests
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
safe preflight without publishing.
