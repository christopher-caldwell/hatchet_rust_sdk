use std::sync::Arc;

use hatchet_sdk::{Client, InMemoryTransport, Priority, RunOpts};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize)]
struct CheckoutWorkflowInput {
    order_id: String,
    customer_id: String,
    amount_cents: u64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(Arc::clone(&transport));

    let checkout_workflow =
        client.workflow::<CheckoutWorkflowInput, String>("commerce.checkout.process_order");
    let checkout_task =
        client.task::<CheckoutWorkflowInput, String>("commerce.checkout.process_order");
    let worker = client
        .worker("checkout-worker")
        .register_task(&checkout_task, |input, _ctx| async move {
            Ok(format!(
                "order {} for customer {}: {} cents",
                input.order_id, input.customer_id, input.amount_cents
            ))
        })
        .build()
        .start()?;
    let run_opts = RunOpts {
        additional_metadata: [("source".to_owned(), "web-checkout".to_owned())]
            .into_iter()
            .collect(),
        priority: Some(Priority::High),
        sticky: Some("customer-42".to_owned()),
        child_key: Some("order-1001".to_owned()),
        desired_worker_labels: [("region".to_owned(), "us-central".to_owned())]
            .into_iter()
            .collect(),
        ..RunOpts::default()
    };

    let run = checkout_workflow
        .run_no_wait(
            CheckoutWorkflowInput {
                order_id: "order-1001".to_owned(),
                customer_id: "customer-42".to_owned(),
                amount_cents: 12_900,
            },
            run_opts.clone(),
        )
        .await?;
    let output = run.output().await?;

    let initial_status = client.runs().get_status(run.run_id()).await?;
    client.runs().cancel(run.run_id()).await?;
    let cancelled_status = client.runs().get_status(run.run_id()).await?;
    let replay_id = client.runs().replay(run.run_id()).await?;
    let queue = client.metrics().get_queue_metrics().await;
    let submitted = transport.submitted_runs().await;

    println!(
        "workflow=commerce.checkout.process_order run={} status_before={:?} status_after={:?} replay={} output={} queue_total={} submitted={}",
        run.run_id(),
        initial_status,
        cancelled_status,
        replay_id,
        output,
        queue.total_runs,
        submitted.len(),
    );

    worker.shutdown().await?;

    Ok(())
}
