//! `GrpcTransport::append_run_log` forwards each line to `PutLog` and keeps no
//! process-local copy of it.
#[path = "support/grpc.rs"]
#[allow(dead_code)] // This test uses neither `Reply::error` nor `Harness.client`.
mod grpc;
use another_hatchet_sdk::*;
use grpc::*;
use prost::Message;

#[tokio::test]
async fn every_run_log_line_is_sent_to_put_log_without_a_local_buffer() {
    const LINES: usize = 100;
    let h = harness(
        "v0.114.0",
        (0..LINES)
            .map(|_| Reply::ok(proto::events::PutLogResponse::default()))
            .collect(),
    )
    .await;
    let run_id = WorkflowRunId("run-1".to_owned());
    for line in 0..LINES {
        let level = if line % 2 == 0 {
            LogLevel::Info
        } else {
            LogLevel::Warn
        };
        Transport::append_run_log(
            h.transport.as_ref(),
            &run_id,
            "task-run-1",
            level,
            format!("line {line}"),
        )
        .await
        .expect("the scripted PutLog reply is OK");
    }

    let reqs = h.requests.lock().await;
    let sent: Vec<_> = reqs
        .iter()
        .filter(|(path, _)| path.ends_with("/PutLog"))
        .map(|(_, body)| proto::events::PutLogRequest::decode(body.as_slice()).unwrap())
        .collect();
    assert_eq!(sent.len(), LINES, "each log line is one PutLog call");
    for (line, request) in sent.iter().enumerate() {
        assert_eq!(request.message, format!("line {line}"));
        assert_eq!(request.task_run_external_id, "task-run-1");
        let level = if line % 2 == 0 { "INFO" } else { "WARN" };
        assert_eq!(request.level.as_deref(), Some(level));
    }
}
