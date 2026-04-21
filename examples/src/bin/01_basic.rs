use hatchet_sdk::{
    Client, Context, InMemoryContext, InMemoryTransport, Result, TaskConfig, WorkflowDeclaration,
};
use std::sync::Arc;

/// This example demonstrates the most basic usage of the SDK without macros.
/// It creates a worker, registers a simple task, registers a workflow containing that task,
/// and then runs the workflow.
#[tokio::main]
async fn main() -> Result<()> {
    // 1. Initialize an InMemoryTransport (use GrpcTransport for real environments)
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(transport);

    // 2. Define a Task handle
    let my_task = client.task::<serde_json::Value, String>("my-echo-task");

    // 3. Create a Worker and register the task handler
    let worker = client
        .worker("example-worker")
        .register_task(&my_task, |input, ctx: InMemoryContext<_>| async move {
            // Task implementation: log something, output a string
            let msg = format!("Hello! Received input: {:?}", input);
            println!("[Task {}]: {}", ctx.task_name(), msg);
            Ok(msg)
        })
        .build();

    // 4. Start the worker runtime (starts processing queued tasks)
    let _worker_handle = worker.start()?;

    // 5. Declare a workflow incorporating this task
    let workflow_decl: WorkflowDeclaration<serde_json::Value, String> =
        WorkflowDeclaration::new("basic-workflow")
            .description("A single-step basic workflow")
            .step(&my_task, TaskConfig::default());

    // Register the workflow to the hatchet engine
    let _ = client.register_workflow(workflow_decl).await;

    // 6. trigger the workflow run
    println!("Triggering workflow run...");
    let run_handle = client
        .workflow::<serde_json::Value, String>("basic-workflow")
        .run_no_wait(
            serde_json::json!({"test": "data"}),
            hatchet_sdk::RunOpts::default(),
        )
        .await?;

    // Wait for output
    let output = run_handle.output().await?;
    println!("Workflow completed with output: {}", output);

    Ok(())
}
