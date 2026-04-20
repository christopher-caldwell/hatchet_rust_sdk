use std::sync::Arc;
use std::time::{Duration, SystemTime};

use hatchet_sdk::{
    Client, Context, ContextIds, DurableContext, InMemoryDurableContext, InMemoryTransport,
    RunOpts, WorkflowRunId,
};
use serde::Serialize;
use serde_json::json;

#[derive(Debug, Serialize)]
struct InvoiceChildInput {
    invoice_id: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(Arc::clone(&transport));
    let invoice_child = client.workflow::<InvoiceChildInput, String>("billing.process_invoice");

    let durable = Arc::new(InMemoryDurableContext::new(
        ContextIds {
            workflow_name: "billing.reconcile".to_owned(),
            task_name: "await_payment_event".to_owned(),
            workflow_run_id: WorkflowRunId("durable-parent-run-1".to_owned()),
            task_run_id: "durable-task-run-1".to_owned(),
        },
        Arc::clone(&transport),
        2,
    ));

    let now = durable.now().await;
    durable.sleep_for(Duration::from_millis(10)).await;
    durable
        .sleep_until(
            SystemTime::now()
                .checked_add(Duration::from_millis(10))
                .expect("future time should be representable"),
        )
        .await;

    let durable_for_signal = Arc::clone(&durable);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(15)).await;
        let _ = durable_for_signal.emit_event(
            "payment_received",
            json!({
                "invoice_id": "inv-42",
                "amount_cents": 1999
            }),
        );
    });

    let event = durable
        .wait_for_event("payment_received", Duration::from_millis(250))
        .await?;

    let spawned = durable
        .spawn_child(
            &invoice_child,
            InvoiceChildInput {
                invoice_id: "inv-42".to_owned(),
            },
            RunOpts::default(),
        )
        .await?;

    let spawned_many = durable
        .spawn_children(
            &invoice_child,
            [
                InvoiceChildInput {
                    invoice_id: "inv-43".to_owned(),
                },
                InvoiceChildInput {
                    invoice_id: "inv-44".to_owned(),
                },
            ],
            RunOpts::default(),
        )
        .await?;

    let submitted = transport.submitted_runs().await;

    println!(
        "workflow={} task={} invocation_count={} now_ok={} event_found={} spawned={} spawned_many={} total_submitted={}",
        durable.workflow_name(),
        durable.task_name(),
        durable.invocation_count(),
        now.duration_since(SystemTime::UNIX_EPOCH).is_ok(),
        event.is_some(),
        spawned.run_id,
        spawned_many.len(),
        submitted.len()
    );

    Ok(())
}
