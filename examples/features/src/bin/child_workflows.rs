use another_hatchet_sdk::{
    Client, Context, GrpcTransport, InMemoryContext, Result, TaskConfig, WorkflowDeclaration,
    config::HatchetConfig,
};
use std::sync::Arc;

/// This example demonstrates how to spawn a child workflow from within a task.
#[tokio::main]
async fn main() -> Result<()> {
    let config = HatchetConfig::from_env()?;
    let client = Client::new(Arc::new(GrpcTransport::from_config(config).await?));

    // 1. Declare the Child Workflow Task
    let child_task = client.task::<serde_json::Value, String>("child-task");

    // We create a WorkflowHandle for the child workflow so we can spawn it from the parent
    let child_workflow_handle = client.workflow::<serde_json::Value, String>("child-workflow");

    // 2. Declare the Parent Workflow Task
    let parent_task = client.task::<serde_json::Value, String>("parent-task");

    // We clone the child handle so it can be moved into the worker closure
    let child_handle_for_closure = child_workflow_handle.clone();

    // 3. Register Both Tasks on a Worker
    let worker = client
        .worker("child-runtime-worker")
        .register_task(&child_task, |input, ctx: InMemoryContext<_>| async move {
            println!(
                "[Child Task {}] Running with input: {:?}",
                ctx.task_name(),
                input
            );
            Ok(format!(
                "Child completed dynamically with input: {:?}",
                input
            ))
        })
        .register_task(&parent_task, move |input, ctx: InMemoryContext<_>| {
            // Because the handler is called multiple times, we clone the handle for each async execution block
            let child_handle = child_handle_for_closure.clone();
            async move {
                println!(
                    "[Parent Task {}] Running with input: {:?}",
                    ctx.task_name(),
                    input
                );

                println!("[Parent Task] Spawning and waiting for child workflow...");
                // Spawn the child workflow natively
                let child_run = ctx
                    .run_child(
                        &child_handle,
                        serde_json::json!({ "prop": "value" }),
                        another_hatchet_sdk::RunOpts::default(),
                    )
                    .await?;
                let output = child_run.output().await?;

                Ok(format!("Parent got result from child: {}", output))
            }
        })
        .build();

    let _handle = worker.start()?;

    // 4. Register both workflow declarations to the engine
    let child_decl: WorkflowDeclaration<serde_json::Value, String> =
        WorkflowDeclaration::new("child-workflow").step(&child_task, TaskConfig::default());
    client.register_workflow(child_decl).await?;

    let parent_decl: WorkflowDeclaration<serde_json::Value, String> =
        WorkflowDeclaration::new("parent-workflow").step(&parent_task, TaskConfig::default());
    client.register_workflow(parent_decl).await?;

    // 5. Trigger the parent workflow
    println!("Triggering parent workflow...");
    let result = client
        .workflow::<serde_json::Value, String>("parent-workflow")
        .run(
            serde_json::json!({"start": true}),
            another_hatchet_sdk::RunOpts::default(),
        )
        .await?;

    println!("Parent Workflow Final Output: {}", result);

    Ok(())
}
