use std::sync::Arc;

use hatchet_sdk::{Client, InMemoryTransport, Priority, RunOpts};
use serde::Serialize;
use serde_json::json;

#[derive(Debug, Serialize)]
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
    let run_opts = RunOpts {
        metadata: [("source".to_owned(), json!("web-checkout"))]
            .into_iter()
            .collect(),
        priority: Some(Priority::High),
        sticky: Some("customer-42".to_owned()),
        key: Some("order-1001".to_owned()),
        desired_worker_labels: [("region".to_owned(), "us-central".to_owned())]
            .into_iter()
            .collect(),
    };

    let run = checkout_workflow
        .run(
            CheckoutWorkflowInput {
                order_id: "order-1001".to_owned(),
                customer_id: "customer-42".to_owned(),
                amount_cents: 12_900,
            },
            run_opts.clone(),
        )
        .await?;

    let run_no_wait = checkout_workflow
        .run_no_wait(
            CheckoutWorkflowInput {
                order_id: "order-1002".to_owned(),
                customer_id: "customer-77".to_owned(),
                amount_cents: 7_500,
            },
            run_opts.clone(),
        )
        .await?;

    let batched_runs = checkout_workflow
        .run_many(
            [
                CheckoutWorkflowInput {
                    order_id: "order-1003".to_owned(),
                    customer_id: "customer-91".to_owned(),
                    amount_cents: 1_999,
                },
                CheckoutWorkflowInput {
                    order_id: "order-1004".to_owned(),
                    customer_id: "customer-15".to_owned(),
                    amount_cents: 24_500,
                },
            ],
            run_opts,
        )
        .await?;

    let initial_status = client.runs().get_status(&run.run_id).await?;
    client.runs().cancel(&run.run_id).await?;
    let cancelled_status = client.runs().get_status(&run.run_id).await?;
    let replay_id = client.runs().replay(&run.run_id).await?;
    let queue = client.metrics().get_queue_metrics().await;
    let submitted = transport.submitted_runs().await;

    println!(
        "workflow=commerce.checkout.process_order run={} status_before={:?} status_after={:?} replay={} run_no_wait={} run_many={} queue_total={} submitted={}",
        run.run_id,
        initial_status,
        cancelled_status,
        replay_id,
        run_no_wait.run_id,
        batched_runs.len(),
        queue.total_runs,
        submitted.len(),
    );

    Ok(())
}
