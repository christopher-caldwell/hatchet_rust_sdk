use std::sync::Arc;

use hatchet_sdk::{Client, InMemoryTransport, RunOpts};
use serde::Serialize;

#[derive(Debug, Serialize)]
struct EmailWorkflowInput {
    recipient: String,
    subject: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(Arc::clone(&transport));

    let email_workflow =
        client.workflow::<EmailWorkflowInput, String>("email.send_transactional_email");

    let run = email_workflow
        .run(
            EmailWorkflowInput {
                recipient: "dev@example.com".to_owned(),
                subject: "Welcome".to_owned(),
            },
            RunOpts::default(),
        )
        .await?;

    let run_no_wait = email_workflow
        .run_no_wait(
            EmailWorkflowInput {
                recipient: "ops@example.com".to_owned(),
                subject: "Provisioned".to_owned(),
            },
            RunOpts::default(),
        )
        .await?;

    let batched_runs = email_workflow
        .run_many(
            [
                EmailWorkflowInput {
                    recipient: "batch-1@example.com".to_owned(),
                    subject: "A".to_owned(),
                },
                EmailWorkflowInput {
                    recipient: "batch-2@example.com".to_owned(),
                    subject: "B".to_owned(),
                },
            ],
            RunOpts::default(),
        )
        .await?;

    let submitted = transport.submitted_runs().await;

    println!(
        "run={}, run_no_wait={}, run_many_count={}, submitted_count={}",
        run.run_id,
        run_no_wait.run_id,
        batched_runs.len(),
        submitted.len()
    );

    Ok(())
}
