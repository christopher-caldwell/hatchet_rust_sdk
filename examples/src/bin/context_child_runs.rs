use std::sync::Arc;

use hatchet_sdk::{Client, ContextIds, InMemoryContext, InMemoryTransport, RunOpts};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChildInput {
    customer_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ParentInput {
    customer_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ParentOutput {
    child_count: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(Arc::clone(&transport));

    let parent_workflow = client.workflow::<ParentInput, ParentOutput>("billing.monthly_batch");
    let dispatch_task = client.task::<ParentInput, ParentOutput>("billing.dispatch_children");
    let child_workflow = client.workflow::<ChildInput, String>("billing.charge_customer");

    let child_for_task = child_workflow.clone();
    let worker = client
        .worker("billing-dispatch-worker")
        .register_task(
            &dispatch_task,
            move |input: ParentInput, ctx: InMemoryContext<_>| {
                let child_workflow = child_for_task.clone();
                async move {
                    let child_runs = ctx
                        .bulk_run_children(
                            &child_workflow,
                            input
                                .customer_ids
                                .into_iter()
                                .map(|customer_id| ChildInput { customer_id }),
                            RunOpts::default(),
                        )
                        .await?;
                    Ok(ParentOutput {
                        child_count: child_runs.len(),
                    })
                }
            },
        )
        .build()
        .start()?;

    let parent_run = parent_workflow
        .run_no_wait(
            ParentInput {
                customer_ids: vec![
                    "cust-1".to_owned(),
                    "cust-2".to_owned(),
                    "cust-3".to_owned(),
                ],
            },
            RunOpts::default(),
        )
        .await?;

    let dispatch_result = worker
        .run_task(
            &dispatch_task,
            ParentInput {
                customer_ids: vec![
                    "cust-1".to_owned(),
                    "cust-2".to_owned(),
                    "cust-3".to_owned(),
                ],
            },
            ContextIds {
                workflow_name: "billing.monthly_batch".to_owned(),
                task_name: "billing.dispatch_children".to_owned(),
                workflow_run_id: parent_run.run_id().clone(),
                task_run_id: "dispatch-task-run-1".to_owned(),
                workflow_id: None,
                workflow_version_id: None,
            },
        )
        .await?;
    worker.shutdown().await?;

    let submitted = transport.submitted_runs().await;
    let linked_to_parent = submitted
        .iter()
        .filter(|run| {
            run.opts.parent_workflow_run_id.is_some() && run.opts.parent_task_run_id.is_some()
        })
        .count();

    println!(
        "parent_run={} dispatched_children={} linked_to_parent={} total_submitted={}",
        parent_run.run_id(),
        dispatch_result.child_count,
        linked_to_parent,
        submitted.len(),
    );

    Ok(())
}
