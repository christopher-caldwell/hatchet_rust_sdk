use std::sync::Arc;
use std::time::Duration;

use hatchet_sdk::{
    Client, ContextIds, DurableContext, InMemoryDurableContext, InMemoryTransport, RunOpts,
    WorkflowRunId,
};
use serde::Serialize;
use serde_json::json;

#[derive(Debug, Clone, Serialize)]
struct Input {
    number: u32,
}

type Output = u32;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(Arc::clone(&transport));
    let workflow = client.workflow::<Input, Output>("validation.workflow");

    let run = workflow
        .run(Input { number: 5 }, RunOpts::default())
        .await?;
    let run_many = workflow
        .run_many(
            [
                Input { number: 6 },
                Input { number: 7 },
                Input { number: 8 },
            ],
            RunOpts::default(),
        )
        .await?;

    let ids = ContextIds {
        workflow_name: "validation.workflow".to_owned(),
        task_name: "validate".to_owned(),
        workflow_run_id: WorkflowRunId("validation-parent-run".to_owned()),
        task_run_id: "validation-task-run".to_owned(),
    };
    let durable_ctx = Arc::new(InMemoryDurableContext::new(ids, Arc::clone(&transport), 2));

    let emit_ctx = Arc::clone(&durable_ctx);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(20)).await;
        let _ = emit_ctx.emit_event("ready", json!({ "ok": true }));
    });

    let event = durable_ctx
        .wait_for_event("ready", Duration::from_millis(200))
        .await?;

    let task = client.task::<u32, u32>("sum");
    let durable_task = client.durable_task::<u32, u32>("mul");
    let worker = client
        .worker("validation-worker")
        .register_task(&task, |input, _ctx| async move { Ok(input + 1) })
        .register_durable_task(&durable_task, |input, ctx| async move {
            Ok(input * 2 + ctx.invocation_count())
        })
        .build();
    let handle = worker.start()?;
    let run_ids = ContextIds {
        workflow_name: "validation.workflow".to_owned(),
        task_name: "sum".to_owned(),
        workflow_run_id: WorkflowRunId("worker-parent-run".to_owned()),
        task_run_id: "worker-task-run".to_owned(),
    };
    let worker_normal = handle.run_task(&task, 10_u32, run_ids.clone()).await?;
    let worker_durable = handle
        .run_durable_task(&durable_task, 10_u32, run_ids, 3)
        .await?;
    handle.shutdown().await?;

    let queue_metrics = client.metrics().get_queue_metrics().await;

    let checks = [
        run.run_id.0.starts_with("run-"),
        run_many.len() == 3,
        event == Some(json!({ "ok": true })),
        worker_normal == 11,
        worker_durable == 23,
        queue_metrics.total_runs == 4,
    ];
    let passed = checks.into_iter().filter(|ok| *ok).count();

    println!(
        "feature_set_6_validation_suite passed={}/6 queued_runs={}",
        passed, queue_metrics.queued_runs
    );

    Ok(())
}
