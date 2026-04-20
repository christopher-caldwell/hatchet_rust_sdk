use std::sync::Arc;
use std::time::Duration;

use hatchet_sdk::{
    Client, ContextIds, DurableContext, InMemoryContext, InMemoryDurableContext, InMemoryTransport,
    RunOpts, WorkflowRunId,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RenderEmailInput {
    recipient: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RenderEmailOutput {
    preview: String,
    child_run_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurableInput {
    invoice_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DurableOutput {
    status: String,
    invocation_count: u32,
    child_run_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ChildInput {
    key: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(Arc::clone(&transport));

    let render_task = client.task::<RenderEmailInput, RenderEmailOutput>("email.render");
    let durable_task = client.durable_task::<DurableInput, DurableOutput>("billing.await_payment");
    let child_workflow = client.workflow::<ChildInput, String>("internal.child_dispatch");

    let child_for_non_durable = child_workflow.clone();
    let child_for_durable = child_workflow.clone();

    let worker = client
        .worker("local-in-memory-worker")
        .slots(4)
        .durable_slots(4)
        .register_task(
            &render_task,
            move |input: RenderEmailInput, ctx: InMemoryContext<_>| {
                let child = child_for_non_durable.clone();
                async move {
                    let child_run = ctx
                        .run_child(
                            &child,
                            ChildInput {
                                key: format!("preview-{}", input.recipient),
                            },
                            RunOpts::default(),
                        )
                        .await?;
                    Ok(RenderEmailOutput {
                        preview: format!("hello {}", input.recipient),
                        child_run_id: child_run.run_id().0.clone(),
                    })
                }
            },
        )
        .register_durable_task(
            &durable_task,
            move |input: DurableInput, ctx: InMemoryDurableContext<_>| {
                let child = child_for_durable.clone();
                async move {
                    ctx.sleep_for(Duration::from_millis(10)).await;
                    let child_run = ctx
                        .spawn_child(
                            &child,
                            ChildInput {
                                key: format!("invoice-{}", input.invoice_id),
                            },
                            RunOpts::default(),
                        )
                        .await?;
                    Ok(DurableOutput {
                        status: "wait-complete".to_owned(),
                        invocation_count: ctx.invocation_count(),
                        child_run_id: child_run.run_id().0.clone(),
                    })
                }
            },
        )
        .build();

    let worker = worker.start()?;

    let render_output = worker
        .run_task(
            &render_task,
            RenderEmailInput {
                recipient: "dev@example.com".to_owned(),
            },
            ContextIds {
                workflow_name: "email.parent".to_owned(),
                task_name: "render".to_owned(),
                workflow_run_id: WorkflowRunId("worker-parent-run-1".to_owned()),
                task_run_id: "worker-task-run-1".to_owned(),
                workflow_id: None,
                workflow_version_id: None,
            },
        )
        .await?;

    let durable_output = worker
        .run_durable_task(
            &durable_task,
            DurableInput {
                invoice_id: "inv-900".to_owned(),
            },
            ContextIds {
                workflow_name: "billing.parent".to_owned(),
                task_name: "await_payment".to_owned(),
                workflow_run_id: WorkflowRunId("worker-parent-run-2".to_owned()),
                task_run_id: "worker-task-run-2".to_owned(),
                workflow_id: None,
                workflow_version_id: None,
            },
            3,
        )
        .await?;

    let submitted = transport.submitted_runs().await;

    println!(
        "render_preview={} durable_status={} durable_invocation_count={} child_submissions={}",
        render_output.preview,
        durable_output.status,
        durable_output.invocation_count,
        submitted.len()
    );

    worker.shutdown().await?;

    Ok(())
}
