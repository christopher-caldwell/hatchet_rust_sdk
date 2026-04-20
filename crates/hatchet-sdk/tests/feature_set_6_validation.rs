use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use hatchet_sdk::{
    Client, ContextIds, Cron, DurableContext, EventRecord, FilterDefinition, InMemoryContext,
    InMemoryDurableContext, InMemoryTransport, RunOpts, RunStatus, Schedule, WebhookDefinition,
    WorkerInfo, WorkflowRunId,
};
use serde::Serialize;
use serde_json::{Value, json};

#[derive(Debug, Clone, Serialize)]
struct WorkflowInput {
    value: u32,
}

type WorkflowOutput = u32;

fn base_ids() -> ContextIds {
    ContextIds {
        workflow_name: "parent-workflow".to_owned(),
        task_name: "parent-task".to_owned(),
        workflow_run_id: WorkflowRunId("parent-run-1".to_owned()),
        task_run_id: "task-run-1".to_owned(),
    }
}

#[tokio::test]
async fn validates_declaration_and_run_submission() {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(Arc::clone(&transport));

    let workflow = client.workflow::<WorkflowInput, WorkflowOutput>("email.workflow");
    let opts = RunOpts::default();
    let single = workflow.run(WorkflowInput { value: 7 }, opts.clone()).await;
    let many = workflow
        .run_many(
            [
                WorkflowInput { value: 8 },
                WorkflowInput { value: 9 },
                WorkflowInput { value: 10 },
            ],
            opts,
        )
        .await;

    assert!(single.is_ok());
    assert!(many.is_ok());

    let submitted = transport.submitted_runs().await;
    assert_eq!(submitted.len(), 4);
    assert_eq!(submitted[0].workflow_name, "email.workflow");
    assert_eq!(submitted[0].input, json!({ "value": 7 }));

    let run_records = client.runs().list().await;
    assert_eq!(run_records.len(), 4);
}

#[tokio::test]
async fn validates_child_workflow_metadata_propagation() {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(Arc::clone(&transport));
    let context = InMemoryContext::new(base_ids(), Arc::clone(&transport));
    let child = client.workflow::<WorkflowInput, WorkflowOutput>("child.workflow");

    let first = context
        .run_child(&child, WorkflowInput { value: 1 }, RunOpts::default())
        .await;
    let second = context
        .run_child_no_wait(&child, WorkflowInput { value: 2 }, RunOpts::default())
        .await;

    assert!(first.is_ok());
    assert!(second.is_ok());

    let submitted = transport.submitted_runs().await;
    assert_eq!(submitted.len(), 2);

    let first_meta = &submitted[0].opts.metadata;
    assert_eq!(
        first_meta.get("parent_workflow_run_id"),
        Some(&Value::String("parent-run-1".to_owned()))
    );
    assert_eq!(
        first_meta.get("parent_task_run_id"),
        Some(&Value::String("task-run-1".to_owned()))
    );
    assert_eq!(first_meta.get("child_index"), Some(&json!(0)));

    let second_meta = &submitted[1].opts.metadata;
    assert_eq!(second_meta.get("child_index"), Some(&json!(1)));
}

#[tokio::test]
async fn validates_durable_wait_and_spawn_children() {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(Arc::clone(&transport));
    let durable = Arc::new(InMemoryDurableContext::new(
        base_ids(),
        Arc::clone(&transport),
        3,
    ));
    let child = client.workflow::<WorkflowInput, WorkflowOutput>("durable.child");

    let emit_ctx = Arc::clone(&durable);
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(25)).await;
        let _ = emit_ctx.emit_event("approval", json!({ "approved": true }));
    });

    let received = durable
        .wait_for_event("approval", Duration::from_millis(250))
        .await;
    assert_eq!(durable.invocation_count(), 3);
    assert_eq!(received.ok().flatten(), Some(json!({ "approved": true })));

    let spawned = durable
        .spawn_children(
            &child,
            [WorkflowInput { value: 11 }, WorkflowInput { value: 12 }],
            RunOpts::default(),
        )
        .await;
    assert!(spawned.is_ok());
    assert_eq!(spawned.ok().map_or(0, |runs| runs.len()), 2);

    let submitted = transport.submitted_runs().await;
    assert_eq!(submitted.len(), 2);
}

#[tokio::test]
async fn validates_worker_dispatch_paths() {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(Arc::clone(&transport));

    let add_task = client.task::<u32, u32>("add-two");
    let durable_task = client.durable_task::<u32, u32>("multiply-three");

    let worker = client
        .worker("test-worker")
        .slots(2)
        .durable_slots(2)
        .register_task(&add_task, |input, _ctx| async move { Ok(input + 2) })
        .register_durable_task(&durable_task, |input, ctx| async move {
            Ok(input * 3 + ctx.invocation_count())
        })
        .build();

    let handle = worker.start().expect("worker should start");
    let ids = base_ids();

    let normal = handle.run_task(&add_task, 5_u32, ids.clone()).await;
    let durable = handle.run_durable_task(&durable_task, 4_u32, ids, 2).await;

    assert_eq!(normal.ok(), Some(7));
    assert_eq!(durable.ok(), Some(14));

    let stopped = handle.shutdown().await;
    assert!(stopped.is_ok());
}

#[tokio::test]
async fn validates_feature_client_operations() {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(Arc::clone(&transport));

    let workflow = client.workflow::<WorkflowInput, WorkflowOutput>("feature.workflow");
    let run = workflow
        .run(WorkflowInput { value: 42 }, RunOpts::default())
        .await
        .expect("run should succeed");
    let run_id = run.run_id;

    let workflow_def = client
        .workflows()
        .put("feature.workflow", json!({ "v": 1 }))
        .await;
    assert_eq!(workflow_def.version, 1);
    assert_eq!(
        client.workflows().version("feature.workflow").await.ok(),
        Some(1)
    );

    let schedule = Schedule {
        name: "daily".to_owned(),
        workflow_name: "feature.workflow".to_owned(),
        expression: "0 0 * * *".to_owned(),
        input: json!({ "kind": "daily" }),
    };
    let created_schedule = client.schedules().create(schedule.clone()).await;
    assert_eq!(created_schedule.name, "daily");
    assert_eq!(
        client.schedules().get("daily").await.ok().map(|s| s.name),
        Some("daily".to_owned())
    );

    let cron = Cron {
        name: "nightly".to_owned(),
        workflow_name: "feature.workflow".to_owned(),
        expression: "0 2 * * *".to_owned(),
    };
    let created_cron = client.crons().create(cron).await;
    assert_eq!(created_cron.name, "nightly");

    let worker_info = WorkerInfo {
        name: "worker-a".to_owned(),
        paused: false,
        labels: BTreeMap::from([("region".to_owned(), "us".to_owned())]),
    };
    let updated_worker = client.workers().update(worker_info).await;
    assert_eq!(updated_worker.name, "worker-a");
    assert!(client.workers().get("worker-a").await.is_ok());

    client.rate_limits().put("default", 10).await;
    assert_eq!(
        client.rate_limits().list().await.get("default").copied(),
        Some(10)
    );

    client
        .events()
        .push(EventRecord {
            key: "order.created".to_owned(),
            payload: json!({ "id": "ord-1" }),
        })
        .await;
    assert_eq!(client.events().list().await.len(), 1);

    let filter = FilterDefinition {
        name: "high-priority".to_owned(),
        expression: "payload.priority == 'high'".to_owned(),
    };
    let created_filter = client.filters().create(filter).await;
    assert_eq!(created_filter.name, "high-priority");

    let webhook = WebhookDefinition {
        name: "ops".to_owned(),
        url: "https://example.com/webhook".to_owned(),
        enabled: true,
    };
    let created_webhook = client.webhooks().create(webhook).await;
    assert_eq!(created_webhook.name, "ops");

    let status_before = client.runs().get_status(&run_id).await;
    assert_eq!(status_before.ok(), Some(RunStatus::Queued));
    assert!(client.runs().cancel(&run_id).await.is_ok());
    let status_after = client.runs().get_status(&run_id).await;
    assert_eq!(status_after.ok(), Some(RunStatus::Cancelled));

    let replay = client
        .runs()
        .replay(&run_id)
        .await
        .expect("replay should succeed");
    assert!(replay.0.starts_with("run-"));

    let queue_metrics = client.metrics().get_queue_metrics().await;
    assert_eq!(queue_metrics.total_runs, 2);
    assert_eq!(queue_metrics.queued_runs, 1);

    let cel = client
        .cel()
        .debug("payload.id != ''", &json!({ "id": "ord-1" }));
    assert!(cel.accepted);

    assert!(client.schedules().delete("daily").await.is_ok());
    assert!(client.crons().delete("nightly").await.is_ok());
    assert!(client.filters().delete("high-priority").await.is_ok());
    assert!(client.webhooks().delete("ops").await.is_ok());
}
