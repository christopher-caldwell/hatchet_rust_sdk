use std::sync::Arc;
use std::time::Duration;

use hatchet_sdk::{
    Client, ContextIds, DurableContext, InMemoryContext, InMemoryDurableContext, InMemoryTransport,
    RunOpts, WorkerHandle, WorkflowRunId,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Clone, Serialize, Deserialize)]
struct OrderInput {
    order_id: String,
    subtotal_cents: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CalculateTotalOutput {
    total_cents: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ReconcileOutput {
    status: String,
}

fn checkout_context(task_name: &str, run_id: &WorkflowRunId, task_run_id: &str) -> ContextIds {
    ContextIds {
        workflow_name: "commerce.checkout".to_owned(),
        task_name: task_name.to_owned(),
        workflow_run_id: run_id.clone(),
        task_run_id: task_run_id.to_owned(),
        workflow_id: None,
        workflow_version_id: None,
    }
}

fn build_worker(client: &Client<InMemoryTransport>) -> hatchet_sdk::Result<WorkerHandle> {
    let calculate_total =
        client.task::<OrderInput, CalculateTotalOutput>("commerce.checkout.calculate_total");
    let reconcile =
        client.durable_task::<OrderInput, ReconcileOutput>("commerce.checkout.reconcile");

    client
        .worker("checkout-worker")
        .register_task(
            &calculate_total,
            |input: OrderInput, _ctx: InMemoryContext<_>| async move {
                Ok(CalculateTotalOutput {
                    total_cents: input.subtotal_cents + 799,
                })
            },
        )
        .register_durable_task(
            &reconcile,
            |input: OrderInput, ctx: InMemoryDurableContext<_>| async move {
                ctx.sleep_for(Duration::from_millis(10)).await;
                let _ = ctx
                    .spawn_child_json(
                        "commerce.ledger.record_transaction",
                        json!({ "order_id": input.order_id }),
                        RunOpts::default(),
                    )
                    .await?;
                Ok(ReconcileOutput {
                    status: "recorded".to_owned(),
                })
            },
        )
        .build()
        .start()
}

async fn wait_for_payment_signal(transport: Arc<InMemoryTransport>) -> hatchet_sdk::Result<bool> {
    let durable_ctx = Arc::new(InMemoryDurableContext::new(
        ContextIds {
            workflow_name: "commerce.checkout".to_owned(),
            task_name: "commerce.checkout.await_signal".to_owned(),
            workflow_run_id: WorkflowRunId("signal-run-1".to_owned()),
            task_run_id: "signal-task-1".to_owned(),
            workflow_id: None,
            workflow_version_id: None,
        },
        transport,
        1,
    ));
    let emit_ctx = Arc::clone(&durable_ctx);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let _ = emit_ctx.emit_event("payment.captured", json!({ "ok": true }));
    });

    durable_ctx
        .wait_for_event("payment.captured", Duration::from_millis(200))
        .await
        .map(|event| event.is_some())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(Arc::clone(&transport));

    let checkout_workflow = client.workflow::<OrderInput, String>("commerce.checkout");
    let calculate_total =
        client.task::<OrderInput, CalculateTotalOutput>("commerce.checkout.calculate_total");
    let reconcile =
        client.durable_task::<OrderInput, ReconcileOutput>("commerce.checkout.reconcile");

    let worker = build_worker(&client)?;

    let order = OrderInput {
        order_id: "order-2001".to_owned(),
        subtotal_cents: 12_000,
    };
    let checkout_run = checkout_workflow
        .run_no_wait(order.clone(), RunOpts::default())
        .await?;

    let total = worker
        .run_task(
            &calculate_total,
            order.clone(),
            checkout_context(
                "commerce.checkout.calculate_total",
                checkout_run.run_id(),
                "task-calc-total-1",
            ),
        )
        .await?;

    let reconciled = worker
        .run_durable_task(
            &reconcile,
            order,
            checkout_context(
                "commerce.checkout.reconcile",
                checkout_run.run_id(),
                "task-reconcile-1",
            ),
            2,
        )
        .await?;
    worker.shutdown().await?;

    let queue_metrics = client.metrics().get_queue_metrics().await;
    let run_status = client.runs().get_status(checkout_run.run_id()).await?;
    let submitted = transport.submitted_runs().await;
    let ledger_child_runs = submitted
        .iter()
        .filter(|run| run.workflow_name == "commerce.ledger.record_transaction")
        .count();

    println!(
        "checkout_run={} status={:?} total_cents={} reconcile_status={} ledger_child_runs={} queue_total={}",
        checkout_run.run_id(),
        run_status,
        total.total_cents,
        reconciled.status,
        ledger_child_runs,
        queue_metrics.total_runs
    );

    println!(
        "payment_signal_received={}",
        wait_for_payment_signal(Arc::clone(&transport)).await?
    );

    Ok(())
}
