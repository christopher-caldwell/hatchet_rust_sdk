//! Exercise result retry policy through the public transport API.
mod support;
use another_hatchet_sdk::proto::v1::RunStatus;
use serde_json::{Value, json};
use std::time::Duration;
use support::*;

async fn output(harness: &Harness) -> another_hatchet_sdk::Result<Value> {
    tokio::time::timeout(
        Duration::from_secs(2),
        harness.client.run_ref::<Value>("run-1").output(),
    )
    .await
    .expect("result lookup must finish")
}

#[tokio::test]
async fn successful_run_recovers_from_read_model_lag_and_transient_errors() {
    let h = harness(
        RunStatus::Completed,
        true,
        vec![
            error_reply(404),
            error_reply(408),
            error_reply(429),
            error_reply(503),
            graph_reply(false),
        ],
        retry_config(4),
    )
    .await;
    assert_eq!(output(&h).await.unwrap(), json!(42));
    assert_eq!(h.requests.lock().await.len(), 5);
}

#[tokio::test]
async fn metadata_retries_stop_at_the_configured_attempt_limit() {
    for max_retries in [0, 2] {
        let h = harness(
            RunStatus::Completed,
            true,
            vec![error_reply(503)],
            retry_config(max_retries),
        )
        .await;
        assert!(output(&h).await.unwrap_err().to_string().contains("503"));
        assert_eq!(h.requests.lock().await.len(), max_retries as usize + 1);
    }
}

#[tokio::test]
async fn permanent_metadata_errors_are_not_retried() {
    let mut malformed = graph_reply(false);
    malformed.body = "invalid JSON".to_owned();
    for reply in [error_reply(401), error_reply(403), malformed] {
        let h = harness(RunStatus::Completed, true, vec![reply], retry_config(3)).await;
        assert!(output(&h).await.is_err());
        assert_eq!(h.requests.lock().await.len(), 1);
    }
}

#[tokio::test]
async fn terminal_engine_errors_do_not_depend_on_rest_metadata() {
    for (status, expected) in [
        (RunStatus::Failed, "engine failure"),
        (RunStatus::Cancelled, "workflow run was cancelled"),
    ] {
        let h = harness(status, true, vec![error_reply(503)], retry_config(3)).await;
        assert!(output(&h).await.unwrap_err().to_string().contains(expected));
        assert_eq!(h.requests.lock().await.len(), 0);
    }
}

#[tokio::test]
async fn unused_failure_handler_output_recovers_from_a_transient_rest_error() {
    let h = harness(
        RunStatus::Running,
        false,
        vec![graph_reply(true), error_reply(503), graph_reply(true)],
        retry_config(2),
    )
    .await;
    assert_eq!(output(&h).await.unwrap(), json!(42));
    assert_eq!(h.requests.lock().await.len(), 3);
}

#[tokio::test]
async fn metadata_deadline_also_bounds_a_request_waiting_for_headers() {
    let mut reply = graph_reply(false);
    reply.delay = Duration::from_secs(1);
    let mut config = retry_config(3);
    config.max_elapsed = Some(Duration::from_millis(20));
    let h = harness(RunStatus::Completed, true, vec![reply], config).await;
    assert!(
        output(&h)
            .await
            .unwrap_err()
            .to_string()
            .contains("retry deadline exceeded")
    );
    assert_eq!(h.requests.lock().await.len(), 1);
}
