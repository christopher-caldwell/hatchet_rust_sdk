use hatchet_sdk::{
    Client, Context, GrpcTransport, InMemoryContext, Result, TaskConfig, WorkflowDeclaration,
    config::HatchetConfig,
};
use std::sync::Arc;

/// A completely standalone task function defined outside of any worker registration block.
/// This makes it easier to test in isolation, reuse across different workers, or maintain
/// without deep nesting inside a `main` function.
async fn process_payment(
    input: serde_json::Value,
    ctx: InMemoryContext<GrpcTransport>,
) -> Result<String> {
    println!("[Task {}] Initialized standalone function", ctx.task_name());
    println!(
        "[Task {}] Processing payment for: {:?}",
        ctx.task_name(),
        input
    );

    // Business logic goes here
    let status = format!("Payment successful for payload: {:?}", input);

    // Context metadata is still fully available
    println!(
        "[Task {}] Retry count: {}",
        ctx.task_name(),
        ctx.retry_count()
    );

    Ok(status)
}

/// This example demonstrates how to register a standalone function by reference,
/// avoiding thick inline closures and callback pyramids.
#[tokio::main]
async fn main() -> Result<()> {
    let config = HatchetConfig::from_env()?;
    let client = Client::new(Arc::new(GrpcTransport::from_config(config).await?));

    // 1. Declare the Task handle
    let payment_task = client.task::<serde_json::Value, String>("payment-task");

    // 2. Create the Worker and register the task with the standalone function reference
    let worker = client
        .worker("standalone-worker")
        .register_task(&payment_task, process_payment)
        .build();

    // 3. Start the worker runtime
    let _handle = worker.start()?;

    // 4. Declare the Workflow and register it
    let workflow_decl: WorkflowDeclaration<serde_json::Value, String> =
        WorkflowDeclaration::new("payment-workflow").step(&payment_task, TaskConfig::default());

    client.register_workflow(workflow_decl).await;

    // 5. Trigger the workflow run
    println!("Triggering workflow run...");
    let result = client
        .workflow::<serde_json::Value, String>("payment-workflow")
        .run(
            serde_json::json!({ "amount": 100, "currency": "USD" }),
            hatchet_sdk::RunOpts::default(),
        )
        .await?;

    println!("Workflow completed with output: {}", result);

    Ok(())
}
