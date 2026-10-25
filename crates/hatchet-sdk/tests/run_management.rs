//! Engine-backed run management contracts, independent of local registration state.
mod support;
use another_hatchet_sdk::{WorkflowRunId, proto::v1::RunStatus};
use serde_json::json;
use support::*;

#[tokio::test]
async fn fresh_clients_resolve_only_started_task_ids_from_the_engine() {
    let mut reply = graph_reply(false);
    reply.body = json!({
        "run":{"metadata":{"id":"run-1"},"status":"RUNNING"},
        "shape":[
            {"taskName":"leaf","taskExternalId":"task-1","childrenStepIds":[]},
            {"taskName":"later","taskExternalId":"00000000-0000-0000-0000-000000000000","childrenStepIds":[]}
        ]
    }).to_string();
    let h = harness(RunStatus::Running, false, vec![reply], retry_config(0)).await;
    let run_id = WorkflowRunId("run-1".to_owned());
    assert_eq!(
        h.client
            .runs()
            .get_task_external_id(&run_id, "leaf")
            .await
            .unwrap()
            .as_deref(),
        Some("task-1")
    );
    assert_eq!(
        h.client
            .runs()
            .get_task_external_id(&run_id, "later")
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        h.client
            .runs()
            .get_task_external_id(&run_id, "unknown")
            .await
            .unwrap(),
        None
    );
    assert_eq!(h.requests.lock().await.len(), 3);
}

#[tokio::test]
async fn task_lookup_propagates_engine_errors() {
    let h = harness(
        RunStatus::Running,
        false,
        vec![error_reply(403)],
        retry_config(0),
    )
    .await;
    assert!(
        h.client
            .runs()
            .get_task_external_id(&WorkflowRunId("run-1".to_owned()), "leaf")
            .await
            .unwrap_err()
            .to_string()
            .contains("403")
    );
}

const TASK_ID: &str = "00000000-0000-4000-8000-000000000001";

fn reply(body: serde_json::Value) -> RestReply {
    RestReply {
        status: 200,
        body: body.to_string(),
        delay: std::time::Duration::ZERO,
    }
}

fn request_parts(raw: &[u8]) -> (&str, serde_json::Value) {
    let text = std::str::from_utf8(raw).unwrap();
    let (headers, body) = text.split_once("\r\n\r\n").unwrap();
    (
        headers.lines().next().unwrap(),
        serde_json::from_str(body).unwrap(),
    )
}

#[tokio::test]
async fn branching_uses_the_selected_checkpoint_and_returns_the_new_branch() {
    let h = harness(
        RunStatus::Completed,
        true,
        vec![reply(
            json!({"taskExternalId":TASK_ID,"nodeId":42,"branchId":4}),
        )],
        retry_config(3),
    )
    .await;
    let result = h
        .client
        .runs()
        .branch_durable_task(TASK_ID, 42, 3)
        .await
        .unwrap();
    assert_eq!(result.task_external_id, TASK_ID);
    assert_eq!(result.node_id, 42);
    assert_eq!(result.branch_id, 4);
    let requests = h.requests.lock().await;
    assert_eq!(requests.len(), 1);
    let (line, body) = request_parts(&requests[0]);
    assert_eq!(
        line,
        "POST /api/v1/stable/tenants/synthetic/durable-tasks/branch HTTP/1.1"
    );
    assert_eq!(
        body,
        json!({"taskExternalId":TASK_ID,"nodeId":42,"branchId":3})
    );
}

#[tokio::test]
async fn restore_requeues_the_task_without_calling_replay() {
    for requeued in [true, false] {
        let h = harness(
            RunStatus::Running,
            false,
            vec![reply(json!({"requeued":requeued}))],
            retry_config(3),
        )
        .await;
        assert_eq!(
            h.client
                .runs()
                .restore_task(TASK_ID)
                .await
                .unwrap()
                .requeued,
            requeued
        );
        let requests = h.requests.lock().await;
        assert_eq!(requests.len(), 1);
        let (line, body) = request_parts(&requests[0]);
        assert_eq!(
            line,
            format!("POST /api/v1/stable/tasks/{TASK_ID}/restore HTTP/1.1")
        );
        assert_eq!(body, json!({}));
    }
}

#[tokio::test]
async fn recovery_mutations_are_not_automatically_retried() {
    let h = harness(
        RunStatus::Running,
        false,
        vec![error_reply(503)],
        retry_config(3),
    )
    .await;
    assert!(
        h.client
            .runs()
            .branch_durable_task(TASK_ID, 2, 0)
            .await
            .is_err()
    );
    assert_eq!(h.requests.lock().await.len(), 1);
    assert!(h.client.runs().restore_task(TASK_ID).await.is_err());
    assert_eq!(h.requests.lock().await.len(), 2);
}

#[tokio::test]
async fn malformed_task_ids_do_not_send_mutations() {
    let h = harness(
        RunStatus::Running,
        false,
        vec![error_reply(503)],
        retry_config(3),
    )
    .await;
    assert!(matches!(
        h.client
            .runs()
            .branch_durable_task("not/a/task", 2, 0)
            .await,
        Err(another_hatchet_sdk::Error::InputValidation(_))
    ));
    assert!(matches!(
        h.client.runs().restore_task("not/a/task").await,
        Err(another_hatchet_sdk::Error::InputValidation(_))
    ));
    assert!(h.requests.lock().await.is_empty());
}

#[tokio::test]
async fn in_memory_recovery_is_explicitly_unsupported() {
    let client = another_hatchet_sdk::Client::new(std::sync::Arc::new(
        another_hatchet_sdk::InMemoryTransport::new(),
    ));
    assert!(matches!(
        client.runs().branch_durable_task(TASK_ID, 2, 0).await,
        Err(another_hatchet_sdk::Error::UnsupportedOperation(_))
    ));
    assert!(matches!(
        client.runs().restore_task(TASK_ID).await,
        Err(another_hatchet_sdk::Error::UnsupportedOperation(_))
    ));
}
