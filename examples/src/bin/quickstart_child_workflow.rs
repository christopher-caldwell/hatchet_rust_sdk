use std::sync::Arc;

use hatchet_sdk::{Client, ContextIds, InMemoryTransport, RunOpts};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ParentInput {
    order_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChildInput {
    order_id: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(Arc::clone(&transport));

    let parent_workflow = client.workflow::<ParentInput, String>("orders.parent");
    let child_workflow = client.workflow::<ChildInput, String>("orders.child");
    let task = client.task::<ParentInput, String>("orders.run");

    let worker = client
        .worker("quickstart-worker")
        .register_task(&task, move |input, ctx| {
            let child_workflow = child_workflow.clone();
            async move {
                let child = ctx
                    .run_child(
                        &child_workflow,
                        ChildInput {
                            order_id: input.order_id.clone(),
                        },
                        RunOpts::default(),
                    )
                    .await?;
                Ok(format!("spawned child run {}", child.run_id().0))
            }
        })
        .build();

    let handle = worker.start()?;
    let parent_run = parent_workflow
        .run_no_wait(
            ParentInput {
                order_id: "ord-1001".to_owned(),
            },
            RunOpts::default(),
        )
        .await?;

    let output = handle
        .run_task(
            &task,
            ParentInput {
                order_id: "ord-1001".to_owned(),
            },
            ContextIds {
                workflow_name: "orders.parent".to_owned(),
                task_name: "orders.run".to_owned(),
                workflow_run_id: parent_run.run_id().clone(),
                task_run_id: "task-run-orders-run".to_owned(),
                workflow_id: None,
                workflow_version_id: None,
            },
        )
        .await?;
    handle.shutdown().await?;

    let submitted = transport.submitted_runs().await;
    let child_count = submitted
        .iter()
        .filter(|run| run.workflow_name == "orders.child")
        .count();
    println!(
        "quickstart_child_workflow parent_run={} child_runs={} output={}",
        parent_run.run_id().0,
        child_count,
        output
    );

    Ok(())
}
