use hatchet_sdk::{
    Client, ConcurrencyConfig, ConcurrencyLimitStrategy, GrpcTransport, InMemoryContext, Result,
    TaskConfig, WorkflowDeclaration, config::HatchetConfig,
};
use std::sync::Arc;

/// This example demonstrates how to configure concurrency at the workflow and task levels
/// to restrict how many instances of a workflow or task can run simultaneously.
#[tokio::main]
async fn main() -> Result<()> {
    let config = HatchetConfig::from_env()?;
    let transport = Arc::new(GrpcTransport::from_config(config).await?);
    let client = Client::new(transport);

    // 1. Setup a standard task
    let task = client.task::<serde_json::Value, String>("limited-task");

    let worker = client
        .worker("concurrency-worker")
        .slots(5) // limits total concurrent operations on this worker
        .register_task(&task, |input, _ctx: InMemoryContext<_>| async move {
            println!("Running limited task with input {:?}", input);
            Ok("done".to_string())
        })
        .build();

    let _handle = worker.start()?;

    // 2. Define concurrency configurations
    // The expression uses CEL (Common Expression Language) against the workflow/task payload
    // E.g., limit concurrency based on a "user_id" within the payload
    let wf_concurrency = ConcurrencyConfig::new("input.user_id")
        .max_runs(1) // Only allow 1 run at a time per user_id
        .limit_strategy(ConcurrencyLimitStrategy::CancelInProgress); // Cancel any running instance

    let step_concurrency = ConcurrencyConfig::new("input.group")
        .max_runs(2)
        .limit_strategy(ConcurrencyLimitStrategy::QueueNewRuns);

    // 3. Attach configurations to the workflow
    let task_config = TaskConfig::builder().concurrency(step_concurrency).build();

    let decl: WorkflowDeclaration<serde_json::Value, String> =
        WorkflowDeclaration::new("concurrency-workflow")
            .concurrency(wf_concurrency)
            .step(&task, task_config);

    let _ = client.register_workflow(decl).await;

    // 4. Trigger multiple runs and see limits apply (engine backed, so local InMemoryTransport mimics this)
    println!("Dispatching workflows...");
    let wf_client = client.workflow::<serde_json::Value, String>("concurrency-workflow");

    // User 1 requests run
    let run1 = wf_client
        .run_no_wait(
            serde_json::json!({"user_id": "U1", "group": "G1"}),
            hatchet_sdk::RunOpts::default(),
        )
        .await?;
    // Second request cancels the first due to CancelInProgress
    let _run2 = wf_client
        .run_no_wait(
            serde_json::json!({"user_id": "U1", "group": "G1"}),
            hatchet_sdk::RunOpts::default(),
        )
        .await?;

    let res = run1.output().await; // In a full engine, this might return cancelled.
    println!("Run 1 outcome: {:?}", res);

    Ok(())
}
