# Server-coalesced batch tasks (preview)

`BatchConfig` configures buffering in the real engine, separately from
`run_many_no_wait` (bulk submission). Requires engine v0.114.0 or newer;
registration rejects older engines and `InMemoryTransport`.

Pass the same `TaskConfig` to the task handle and workflow step. The handle
controls worker-side decoding/validation and broadcast behavior; the step controls
engine registration. Inputs are keyed by each member's external task-run UUID.
For mapped output, every input member must have exactly one output and no extra
IDs are allowed. An error fails the whole batch. `broadcast_output: true` lets
one typed output become the result of every member. Engine member retries are
forced to zero, matching the preview official SDK contract.

```rust,no_run
use another_hatchet_sdk::*;
use serde_json::{Value, json};
use std::{collections::BTreeMap, time::Duration};

async fn register(client: Client<GrpcTransport>) -> Result<()> {
    let config = TaskConfig::builder().batch(BatchConfig {
        max_size: 16,
        max_interval: Some(Duration::from_secs(2)),
        group_key: Some("input.account".into()),
        group_max_runs: Some(1),
        broadcast_output: false,
    }).build();
    let task = client.task::<BTreeMap<String, Value>, BTreeMap<String, Value>>("process-batch")
        .with_config(config.clone());
    client.register_workflow(
        WorkflowDeclaration::<Value, Value>::new("batch-workflow")
            .step(&task, config),
    ).await?;
    let worker = client.worker("batch-worker").register_task(
        &task,
        |items: BTreeMap<String, Value>, ctx: InMemoryContext<GrpcTransport>| async move {
            assert_eq!(items.len(), ctx.batch_member_ids().len());
            Ok(items.into_iter().map(|(id, input)| (id, json!({"input": input}))).collect())
        },
    ).build().start()?;
    worker.wait_until_ready(Duration::from_secs(15)).await?;
    // Keep the worker alive for the application's lifetime.
    worker.shutdown().await?;
    Ok(())
}
```

Size and interval must be positive signed 32-bit values; intervals use whole
milliseconds. CEL grouping and group concurrency run on the server. The optional
`input_schema` is applied to each member input. Durable tasks and failure-handler
batch configuration are rejected.

`ctx.batch_member_ids()` identifies the members. The context's task identity is
the batch ID; it has no single member workflow identity. Per-workflow context
operations such as child spawning, run logs and workflow streaming should use an
explicit member/run reference. `ctx.cancel()` cancels all batch members through
the engine. An external cancellation of one member leaves other members running;
cancelling all members signals the handler's cancellation token. Completion is
never sent for cancelled members.

Official references: [TS worker lifecycle](https://github.com/hatchet-dev/hatchet/blob/af128ec1672adaf8e3cc4afd339ce5c2c6f08c35/sdks/typescript/src/v1/client/worker/worker-internal.ts),
[Go batch registration](https://github.com/hatchet-dev/hatchet/blob/aef20b00755da5b1e7e69197e7189848309afb20/sdks/go/workflow.go).
