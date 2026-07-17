use another_hatchet_sdk::{
    Client, DurableContext, GrpcTransport, InMemoryDurableContext, Result, TaskConfig,
    WorkflowDeclaration, config::HatchetConfig,
};
use std::sync::Arc;
use std::time::Duration;

/// This example demonstrates durable capabilities, specifically sleeping
/// (which offloads the wait state to the Hatchet backend so instances don't block worker slots/memory)
/// and memoization (caching variables on the context to support yield-retry scenarios).
#[tokio::main]
async fn main() -> Result<()> {
    let config = HatchetConfig::from_env()?;
    let transport = Arc::new(GrpcTransport::from_config(config).await?);
    let client = Client::new(transport);

    // Note: We use `durable_task` instead of `task` to access DurableContext methods
    let durable_sleep_task = client.durable_task::<serde_json::Value, String>("sleep-task");

    let worker = client
        .worker("durable-worker")
        .register_durable_task(
            &durable_sleep_task,
            |_, ctx: InMemoryDurableContext<_>| async move {
                println!("Task started. Checking cache...");

                // memo: stores the value so that if the pod crashes and restarts, this call fetches the cached item instead of recomputing
                let initialized = ctx.memo("initialized", serde_json::json!(true)).await?;
                println!("Initialized state from durable storage: {:?}", initialized);

                println!("Going to sleep for 2 seconds (passively evicted from memory)");

                // This is not a thread sleep. It instructs Hatchet backend to wake the task up after 2s.
                ctx.sleep_for(Duration::from_secs(2)).await?;

                println!("Task woke up!");

                Ok("Awake".to_string())
            },
        )
        .build();
    let _handle = worker.start()?;

    // To add a durable task to a declaration, use the `.durable_step()` builder method.
    let decl: WorkflowDeclaration<serde_json::Value, String> =
        WorkflowDeclaration::new("durable-sleep-workflow")
            .durable_step(&durable_sleep_task, TaskConfig::default());

    client.register_workflow(decl).await?;

    println!("Starting durable sleep workflow...");
    let result = client
        .workflow::<serde_json::Value, String>("durable-sleep-workflow")
        .run(
            serde_json::json!({}),
            another_hatchet_sdk::RunOpts::default(),
        )
        .await?;

    println!("Result: {}", result);

    Ok(())
}
