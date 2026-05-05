use hatchet_sdk::{
    Client, Context, GrpcTransport, InMemoryContext, Result, TaskConfig, WorkflowDeclaration,
    config::HatchetConfig,
};
use std::sync::Arc;

/// This example demonstrates how to spawn many child workflows from within a task.
#[tokio::main]
async fn main() -> Result<()> {
    let config = HatchetConfig::from_env()?;
    let client = Client::new(Arc::new(GrpcTransport::from_config(config).await?));

    // 1. Declare the child workflow task.
    let child_task = client.task::<serde_json::Value, String>("children-child-task");

    // We create a WorkflowHandle for the child workflow so the parent task can spawn it.
    let child_workflow_handle =
        client.workflow::<serde_json::Value, String>("children-child-workflow");

    // 2. Declare the parent workflow task.
    let parent_task = client.task::<serde_json::Value, String>("children-parent-task");

    // We clone the child handle so it can be moved into the worker closure.
    let child_handle_for_closure = child_workflow_handle.clone();

    // 3. Register both tasks on a worker.
    let worker = client
        .worker("children-runtime-worker")
        .register_task(&child_task, |input, ctx: InMemoryContext<_>| async move {
            println!(
                "[Child Task {}] Running with input: {:?}",
                ctx.task_name(),
                input
            );

            let child_index = input
                .pointer("/input/child_index")
                .or_else(|| input.get("child_index"))
                .and_then(serde_json::Value::as_u64)
                .unwrap_or_default();

            Ok(format!(
                "Child {child_index} completed with input: {input:?}"
            ))
        })
        .register_task(&parent_task, move |input, ctx: InMemoryContext<_>| {
            // Because the handler is called multiple times, clone the handle for each async execution block.
            let child_handle = child_handle_for_closure.clone();
            async move {
                println!(
                    "[Parent Task {}] Running with input: {:?}",
                    ctx.task_name(),
                    input
                );

                let child_count = input
                    .pointer("/input/child_count")
                    .or_else(|| input.get("child_count"))
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(5);

                let child_inputs = (0..child_count).map(|child_index| {
                    serde_json::json!({
                        "child_index": child_index,
                        "parent_input": input,
                    })
                });

                println!("[Parent Task] Spawning {child_count} child workflows...");
                let child_runs = ctx
                    .bulk_run_children(&child_handle, child_inputs, hatchet_sdk::RunOpts::default())
                    .await?;

                let mut outputs = Vec::with_capacity(child_runs.len());
                for child_run in child_runs {
                    outputs.push(child_run.output().await?);
                }

                Ok(format!(
                    "Parent collected {} child results: {}",
                    outputs.len(),
                    outputs.join(" | ")
                ))
            }
        })
        .build();

    let _handle = worker.start()?;

    // 4. Register both workflow declarations to the engine.
    let child_decl: WorkflowDeclaration<serde_json::Value, String> =
        WorkflowDeclaration::new("children-child-workflow")
            .step(&child_task, TaskConfig::default());
    client.register_workflow(child_decl).await;

    let parent_decl: WorkflowDeclaration<serde_json::Value, String> =
        WorkflowDeclaration::new("children-parent-workflow")
            .step(&parent_task, TaskConfig::default());
    client.register_workflow(parent_decl).await;

    // 5. Trigger the parent workflow.
    println!("Triggering parent workflow that spawns children...");
    let result = client
        .workflow::<serde_json::Value, String>("children-parent-workflow")
        .run(
            serde_json::json!({
                "start": true,
                "child_count": 5,
            }),
            hatchet_sdk::RunOpts::default(),
        )
        .await?;

    println!("Parent Workflow Final Output: {}", result);

    Ok(())
}
