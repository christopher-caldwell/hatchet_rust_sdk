#[path = "support/grpc.rs"]
mod grpc;
use another_hatchet_sdk::*;
use grpc::*;
use prost::Message;
use serde_json::{Value, json};
use std::{sync::Arc, time::Duration};
fn config() -> BatchConfig {
    BatchConfig {
        max_size: 8,
        max_interval: Some(Duration::from_millis(1250)),
        group_key: Some("input.account".into()),
        group_max_runs: Some(2),
        broadcast_output: true,
    }
}
#[tokio::test]
async fn batch_registration_preserves_config_and_disables_member_retries() {
    let h = harness(
        "v0.114.0",
        vec![Reply::ok(
            proto::v1::CreateWorkflowVersionResponse::default(),
        )],
    )
    .await;
    let task = h.client.task::<Value, Value>("batch");
    let cfg = TaskConfig::builder().batch(config()).retries(9).build();
    h.client
        .register_workflow(
            WorkflowDeclaration::<Value, Value>::new("batch-workflow").step(&task, cfg),
        )
        .await
        .unwrap();
    let reqs = h.requests.lock().await;
    let (_, body) = reqs
        .iter()
        .find(|(p, _)| p.ends_with("/PutWorkflow"))
        .unwrap();
    let req = proto::v1::CreateWorkflowVersionRequest::decode(body.as_slice()).unwrap();
    let task = &req.tasks[0];
    assert_eq!(task.retries, 0);
    let b = task.batch.as_ref().unwrap();
    assert_eq!(b.batch_max_size, 8);
    assert_eq!(b.batch_max_interval_ms, Some(1250));
    assert_eq!(b.batch_group_key.as_deref(), Some("input.account"));
    assert_eq!(b.batch_group_max_runs, Some(2));
    assert_eq!(b.broadcast_output, Some(true));
}
#[tokio::test]
async fn invalid_durable_local_and_old_engine_batches_are_rejected() {
    let h = harness("v0.111.0", vec![]).await;
    let task = h.client.task::<Value, Value>("batch");
    for invalid in [
        BatchConfig {
            max_size: 0,
            ..config()
        },
        BatchConfig {
            max_interval: Some(Duration::from_nanos(1)),
            ..config()
        },
        BatchConfig {
            group_max_runs: Some(0),
            ..config()
        },
        BatchConfig {
            group_key: Some(" ".into()),
            ..config()
        },
    ] {
        assert!(matches!(
            h.client
                .register_workflow(
                    WorkflowDeclaration::<Value, Value>::new("invalid")
                        .step(&task, TaskConfig::builder().batch(invalid).build())
                )
                .await,
            Err(Error::InputValidation(_))
        ));
    }
    let durable = h.client.durable_task::<Value, Value>("durable");
    assert!(matches!(
        h.client
            .register_workflow(
                WorkflowDeclaration::<Value, Value>::new("durable")
                    .durable_step(&durable, TaskConfig::builder().batch(config()).build())
            )
            .await,
        Err(Error::InputValidation(_))
    ));
    assert!(matches!(
        h.client
            .register_workflow(
                WorkflowDeclaration::<Value, Value>::new("old")
                    .step(&task, TaskConfig::builder().batch(config()).build())
            )
            .await,
        Err(Error::UnsupportedOperation(_))
    ));
    assert!(
        h.requests
            .lock()
            .await
            .iter()
            .all(|(p, _)| p.ends_with("/GetVersion"))
    );
    let local = Client::new(Arc::new(InMemoryTransport::new()));
    let task = local.task::<Value, Value>("local");
    assert!(matches!(
        local
            .register_workflow(
                WorkflowDeclaration::<Value, Value>::new("local")
                    .step(&task, TaskConfig::builder().batch(config()).build())
            )
            .await,
        Err(Error::UnsupportedOperation(_))
    ));
}
fn ids() -> ContextIds {
    ContextIds {
        workflow_name: "wf".into(),
        task_name: "batch".into(),
        workflow_run_id: WorkflowRunId("run".into()),
        task_run_id: "batch-id".into(),
        workflow_id: None,
        workflow_version_id: None,
    }
}
#[tokio::test]
async fn batch_handlers_validate_members_expand_broadcast_and_reject_incomplete_output() {
    let client = Client::new(Arc::new(InMemoryTransport::new()));
    let b = config();
    let broadcast = client.task::<Value, Value>("broadcast").with_config(
        TaskConfig::builder()
            .batch(b.clone())
            .build()
            .with_input_schema_value(json!({"type":"integer"})),
    );
    let broken = client.task::<Value, Value>("broken").with_config(
        TaskConfig::builder()
            .batch(BatchConfig {
                broadcast_output: false,
                ..b
            })
            .build(),
    );
    let worker = client
        .worker("batch-contract")
        .register_task(
            &broadcast,
            |_, _: InMemoryContext<InMemoryTransport>| async { Ok(json!(42)) },
        )
        .register_task(&broken, |_, _: InMemoryContext<InMemoryTransport>| async {
            Ok(json!({"one":1}))
        })
        .build()
        .start()
        .unwrap();
    assert_eq!(
        worker
            .run_task_json("broadcast", json!({"one":1,"two":2}), ids())
            .await
            .unwrap(),
        json!({"one":42,"two":42})
    );
    assert!(matches!(
        worker
            .run_task_json("broadcast", json!({"one":"bad"}), ids())
            .await,
        Err(Error::InputValidation(_))
    ));
    assert!(matches!(
        worker
            .run_task_json("broken", json!({"one":1,"two":2}), ids())
            .await,
        Err(Error::Worker(_))
    ));
    worker.shutdown().await.unwrap();
}

#[tokio::test]
async fn batch_registration_preserves_engine_rejection() {
    let h = harness("v0.114.0", vec![Reply::error(3, vec![]); 4]).await;
    let task = h.client.task::<Value, Value>("batch");
    assert!(
        h.client
            .register_workflow(
                WorkflowDeclaration::<Value, Value>::new("rejected")
                    .step(&task, TaskConfig::builder().batch(config()).build())
            )
            .await
            .is_err()
    );
    assert_eq!(
        h.requests
            .lock()
            .await
            .iter()
            .filter(|(p, _)| p.ends_with("/PutWorkflow"))
            .count(),
        4
    );
}
