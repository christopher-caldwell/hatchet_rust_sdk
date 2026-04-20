use std::sync::Arc;

use hatchet_sdk::{
    Client, Context, ContextIds, InMemoryContext, InMemoryTransport, RunOpts, WorkflowRunId,
};
use serde::Serialize;

#[derive(Debug, Serialize)]
struct ChildInput {
    customer_id: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(Arc::clone(&transport));

    let child_workflow = client.workflow::<ChildInput, String>("billing.charge_customer");

    let context = InMemoryContext::new(
        ContextIds {
            workflow_name: "billing.parent_workflow".to_owned(),
            task_name: "dispatch_children".to_owned(),
            workflow_run_id: WorkflowRunId("parent-run-42".to_owned()),
            task_run_id: "parent-task-run-7".to_owned(),
        },
        Arc::clone(&transport),
    );

    let run_1 = context
        .run_child(
            &child_workflow,
            ChildInput {
                customer_id: "cust-1".to_owned(),
            },
            RunOpts::default(),
        )
        .await?;

    let run_2 = context
        .run_child_no_wait(
            &child_workflow,
            ChildInput {
                customer_id: "cust-2".to_owned(),
            },
            RunOpts::default(),
        )
        .await?;

    let run_many = context
        .bulk_run_children(
            &child_workflow,
            [
                ChildInput {
                    customer_id: "cust-3".to_owned(),
                },
                ChildInput {
                    customer_id: "cust-4".to_owned(),
                },
            ],
            RunOpts::default(),
        )
        .await?;

    let run_many_no_wait = context
        .bulk_run_children_no_wait(
            &child_workflow,
            [ChildInput {
                customer_id: "cust-5".to_owned(),
            }],
            RunOpts::default(),
        )
        .await?;

    let submitted = transport.submitted_runs().await;
    let linked_to_parent = submitted
        .iter()
        .filter(|run| {
            run.opts.metadata.contains_key("parent_workflow_run_id")
                && run.opts.metadata.contains_key("parent_task_run_id")
        })
        .count();

    println!(
        "workflow={} task={} run_1={} run_2={} bulk={} bulk_no_wait={} linked_to_parent={} total_submitted={}",
        context.workflow_name(),
        context.task_name(),
        run_1.run_id,
        run_2.run_id,
        run_many.len(),
        run_many_no_wait.len(),
        linked_to_parent,
        submitted.len()
    );

    Ok(())
}
