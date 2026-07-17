use another_hatchet_sdk::{
    Client, ContextExt, GrpcTransport, InMemoryContext, Result, TaskConfig, WorkflowDeclaration,
    config::HatchetConfig,
};
use std::sync::Arc;

/// This example demonstrates a Directed Acyclic Graph (DAG) with both sequential
/// and concurrent execution paths.
#[tokio::main]
async fn main() -> Result<()> {
    let config = HatchetConfig::from_env()?;
    let transport = Arc::new(GrpcTransport::from_config(config).await?);
    let client = Client::new(transport);

    // 1. Declare Tasks
    let prepare_task = client.task::<serde_json::Value, String>("prepare");
    let fetch_a_task = client.task::<serde_json::Value, String>("fetch-a");
    let fetch_b_task = client.task::<serde_json::Value, String>("fetch-b");
    let combine_task = client.task::<serde_json::Value, String>("combine");

    // 2. Register Tasks to a Worker
    let worker = client
        .worker("dag-worker")
        .register_task(&prepare_task, |_, _ctx: InMemoryContext<_>| async move {
            println!("Preparing resources...");
            Ok("prepared".to_string())
        })
        .register_task(&fetch_a_task, |_, _ctx: InMemoryContext<_>| async move {
            println!("Fetching resource A...");
            Ok("data_a".to_string())
        })
        .register_task(&fetch_b_task, |_, _ctx: InMemoryContext<_>| async move {
            println!("Fetching resource B...");
            Ok("data_b".to_string())
        })
        // Parent outputs are keyed by the parent task's readable name.
        .register_task(&combine_task, |_, ctx: InMemoryContext<_>| async move {
            let res_a: String = ctx.parent_output("fetch-a").await?;
            let res_b: String = ctx.parent_output("fetch-b").await?;
            println!("Combining results...");
            Ok(format!("Combined result: {res_a} + {res_b}"))
        })
        .build();

    let _handle = worker.start()?;

    // 3. Declare the Workflow DAG Structure
    // Both fetch_a and fetch_b wait for 'prepare'
    // 'combine' waits for both fetch_a and fetch_b
    let decl: WorkflowDeclaration<serde_json::Value, String> =
        WorkflowDeclaration::new("dag-workflow")
            .step(&prepare_task, TaskConfig::default())
            // step_after links the task to its parent(s)
            .step_after(&fetch_a_task, &[&prepare_task], TaskConfig::default())
            .step_after(&fetch_b_task, &[&prepare_task], TaskConfig::default())
            // Combine runs only after both fetches succeed
            .step_after(
                &combine_task,
                &[&fetch_a_task, &fetch_b_task],
                TaskConfig::default(),
            );

    client.register_workflow(decl).await?;

    // 4. Run the Workflow
    println!("Starting DAG workflow...");
    let result = client
        .workflow::<serde_json::Value, String>("dag-workflow")
        .run(
            serde_json::json!({}),
            another_hatchet_sdk::RunOpts::default(),
        )
        .await?;

    println!("DAG Workflow Output: {}", result);

    Ok(())
}
