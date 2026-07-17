use another_hatchet_sdk::{
    Client, Condition, GrpcTransport, InMemoryContext, Result, StepConditions, TaskConfig,
    WorkflowDeclaration, config::HatchetConfig, or,
};
use std::sync::Arc;
use std::time::Duration;

/// This example demonstrates configuring a task so it sits in a queue and waits
/// for specific events or timeouts before deciding to trigger, cancel, or skip execution.
#[tokio::main]
async fn main() -> Result<()> {
    let config = HatchetConfig::from_env()?;
    let transport = Arc::new(GrpcTransport::from_config(config).await?);
    let client = Client::new(transport);

    let conditional_task = client.task::<serde_json::Value, String>("conditional-task");

    let worker = client
        .worker("conditions-worker")
        .register_task(
            &conditional_task,
            |_, _ctx: InMemoryContext<_>| async move {
                println!("Conditional task finally executed!");
                Ok("success".to_string())
            },
        )
        .build();

    let _handle = worker.start()?;

    // StepConditions let engineers pause a step from starting until criteria are met
    let conditions = StepConditions::new()
        // wait_for means queue the task, but wait to dispatch it until these occur
        .wait_for(Condition::event("approval:received").build())
        // cancel_if means immediately drop this workflow as a failure if met
        .cancel_if(or([
            Condition::event("user:deleted").build(),
            Condition::sleep_for(Duration::from_secs(300)), // timeout essentially
        ]))
        // skip_if means immediately bypass this task and treat it as 'skipped/successful'
        .skip_if(Condition::event("approval:not_needed").build());

    // step_with accepts a standard task, a task config, and the conditions wrapper
    let decl: WorkflowDeclaration<serde_json::Value, String> = WorkflowDeclaration::new(
        "conditions-workflow",
    )
    .step_with(&conditional_task, TaskConfig::default(), conditions);

    client.register_workflow(decl).await?;

    // Trigger the workflow
    // It will halt before executing 'conditional-task' until "approval:received" goes through the event stream
    let _run = client
        .workflow::<serde_json::Value, String>("conditions-workflow")
        .run_no_wait(
            serde_json::json!({}),
            another_hatchet_sdk::RunOpts::default(),
        )
        .await?;

    println!("Workflow is hanging, waiting for 'approval:received' event...");

    // Simulate some event triggering
    // (This normally goes through the `client.events()` proxy from your backend endpoint)
    // For this example `InMemoryTransport` simulates the payload if sent directly.
    // ...

    Ok(())
}
