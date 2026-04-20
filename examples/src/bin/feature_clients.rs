use std::sync::Arc;

use hatchet_sdk::{
    Client, Cron, EventRecord, FilterDefinition, InMemoryTransport, RunOpts, Schedule,
    WebhookDefinition,
};
use serde::Serialize;
use serde_json::json;

#[derive(Debug, Serialize)]
struct CustomerInput {
    customer_id: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(Arc::clone(&transport));

    let workflow = client.workflow::<CustomerInput, String>("billing.charge_customer");
    let run_ref = workflow
        .run_no_wait(
            CustomerInput {
                customer_id: "cust-44".to_owned(),
            },
            RunOpts::default(),
        )
        .await?;

    let _workflow_v1 = client
        .workflows()
        .put("billing.charge_customer", json!({"tasks":["charge"]}))
        .await;

    let _schedule = client
        .schedules()
        .create(Schedule {
            name: "daily-billing".to_owned(),
            workflow_name: "billing.charge_customer".to_owned(),
            expression: "0 0 * * *".to_owned(),
            input: json!({"batch":"daily"}),
        })
        .await;

    let _cron = client
        .crons()
        .create(Cron {
            name: "hourly-sync".to_owned(),
            workflow_name: "billing.charge_customer".to_owned(),
            expression: "0 * * * *".to_owned(),
        })
        .await;

    client
        .events()
        .push(EventRecord {
            key: "billing.customer.created".to_owned(),
            payload: json!({"customer_id":"cust-44"}),
        })
        .await;

    let _filter = client
        .filters()
        .create(FilterDefinition {
            name: "high-value".to_owned(),
            expression: "input.amount_cents > 10000".to_owned(),
        })
        .await;

    let _webhook = client
        .webhooks()
        .create(WebhookDefinition {
            name: "billing-hook".to_owned(),
            url: "https://example.test/hooks/billing".to_owned(),
            enabled: true,
        })
        .await;

    client
        .rate_limits()
        .put("billing.charge_customer", 50)
        .await;

    let run_status = client.runs().get_status(run_ref.run_id()).await?;
    let all_runs = client.runs().list().await;
    let workflows = client.workflows().list().await;
    let schedules = client.schedules().list().await;
    let crons = client.crons().list().await;
    let events = client.events().list().await;
    let filters = client.filters().list().await;
    let webhooks = client.webhooks().list().await;
    let rate_limits = client.rate_limits().list().await;
    let queue_metrics = client.metrics().get_queue_metrics().await;
    let cel_debug = client
        .cel()
        .debug("input.customer_id != ''", &json!({"customer_id":"cust-44"}));

    println!(
        "run_status={:?} runs={} workflows={} schedules={} crons={} events={} filters={} webhooks={} rate_limits={} queue_total={} cel_ok={}",
        run_status,
        all_runs.len(),
        workflows.len(),
        schedules.len(),
        crons.len(),
        events.len(),
        filters.len(),
        webhooks.len(),
        rate_limits.len(),
        queue_metrics.total_runs,
        cel_debug.accepted
    );

    Ok(())
}
