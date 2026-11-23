//! Task-log contracts exercised against the controlled gRPC wire peer.
#[path = "support/grpc.rs"]
// This file drives the transport directly and never reads `Harness.client`.
#[allow(dead_code)]
mod grpc;
use another_hatchet_sdk::*;
use grpc::*;
use prost::Message;
use std::sync::Arc;

const WORKFLOW_RUN: &str = "00000000-0000-4000-8000-0000000000a1";
const TASK_RUN: &str = "00000000-0000-4000-8000-0000000000b2";

// A task in a multi-task DAG: the workflow run ID is shared by every task in
// the run, so it differs from the task's own external run ID.
fn dag_task_ids() -> ContextIds {
    ContextIds {
        workflow_name: "dag".to_owned(),
        task_name: "second".to_owned(),
        workflow_run_id: WorkflowRunId(WORKFLOW_RUN.to_owned()),
        task_run_id: TASK_RUN.to_owned(),
        workflow_id: None,
        workflow_version_id: None,
    }
}

async fn sent_put_logs(h: &Harness) -> Vec<proto::events::PutLogRequest> {
    h.requests
        .lock()
        .await
        .iter()
        .filter(|(path, _)| path.ends_with("/PutLog"))
        .map(|(_, body)| proto::events::PutLogRequest::decode(body.as_slice()).unwrap())
        .collect()
}

#[tokio::test]
async fn ctx_log_sends_the_task_run_external_id() {
    let h = harness(
        "v0.94.14",
        vec![Reply::ok(proto::events::PutLogResponse {})],
    )
    .await;
    let ctx = InMemoryContext::new(dag_task_ids(), Arc::clone(&h.transport));
    ctx.log(LogLevel::Info, "hello").await.unwrap();
    let sent = sent_put_logs(&h).await;
    assert_eq!(sent.len(), 1);
    assert_eq!(sent[0].task_run_external_id, TASK_RUN);
    assert_eq!(sent[0].message, "hello");
    assert_eq!(sent[0].level.as_deref(), Some("INFO"));
}

// PERMISSION_DENIED (7): the transport reports the failure instead of
// discarding it, and ctx.log does not turn an undelivered line into a task
// failure.
#[tokio::test]
async fn rejected_put_log_is_reported_without_failing_ctx_log() {
    let h = harness(
        "v0.94.14",
        vec![Reply::error(7, vec![]), Reply::error(7, vec![])],
    )
    .await;
    let direct = h
        .transport
        .append_run_log(
            &WorkflowRunId(WORKFLOW_RUN.to_owned()),
            TASK_RUN,
            LogLevel::Warn,
            "direct".to_owned(),
        )
        .await;
    assert!(direct.is_err(), "append_run_log returned {direct:?}");

    let ctx = InMemoryContext::new(dag_task_ids(), Arc::clone(&h.transport));
    let result = ctx.log(LogLevel::Info, "hello").await;
    assert!(result.is_ok(), "ctx.log returned {result:?}");

    let sent = sent_put_logs(&h).await;
    assert_eq!(sent.len(), 2, "PutLog is not retried");
    assert!(sent.iter().all(|r| r.task_run_external_id == TASK_RUN));
}
