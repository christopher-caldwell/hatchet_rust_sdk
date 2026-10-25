#[path = "support/grpc.rs"]
mod grpc;
use another_hatchet_sdk::*;
use grpc::*;
use prost::Message;

#[tokio::test]
async fn retry_after_failure_preserves_wire_delay_and_old_engine_falls_back_to_static_policy() {
    for (version, expected) in [
        ("v0.111.0", Some(1250)),
        ("v0.114.0", Some(1250)),
        ("v0.94.14", None),
        ("development", None),
    ] {
        let h = harness(
            version,
            vec![Reply::ok(proto::dispatcher::ActionEventResponse::default())],
        )
        .await;
        h.transport
            .send_step_action_event(proto::dispatcher::StepActionEvent {
                event_type: proto::dispatcher::StepActionEventType::StepEventTypeFailed as i32,
                event_payload: "busy".into(),
                should_not_retry: Some(false),
                retry_after_ms: Some(1250),
                ..Default::default()
            })
            .await
            .unwrap();
        let reqs = h.requests.lock().await;
        let (_, body) = reqs
            .iter()
            .find(|(path, _)| path.ends_with("/SendStepActionEvent"))
            .unwrap();
        let e = proto::dispatcher::StepActionEvent::decode(body.as_slice()).unwrap();
        assert_eq!(e.retry_after_ms, expected);
        assert_eq!(e.should_not_retry, Some(false));
        assert_eq!(e.event_payload, "busy");
    }
}
#[tokio::test]
async fn retry_after_never_reports_success_when_completion_is_rejected() {
    let h = harness("v0.114.0", vec![Reply::error(3, vec![]); 4]).await;
    assert!(
        h.transport
            .send_step_action_event(proto::dispatcher::StepActionEvent {
                event_type: proto::dispatcher::StepActionEventType::StepEventTypeFailed as i32,
                retry_after_ms: Some(0),
                should_not_retry: Some(false),
                ..Default::default()
            })
            .await
            .is_err()
    );
    let reqs = h.requests.lock().await;
    assert_eq!(
        reqs.iter()
            .filter(|(path, _)| path.ends_with("/SendStepActionEvent"))
            .count(),
        4
    );
    for (_, body) in reqs
        .iter()
        .filter(|(path, _)| path.ends_with("/SendStepActionEvent"))
    {
        assert_eq!(
            proto::dispatcher::StepActionEvent::decode(body.as_slice())
                .unwrap()
                .retry_after_ms,
            Some(0)
        );
    }
    drop(reqs);
    // Exercise the Client facade too: no unrelated RPC is emitted by construction.
    assert_eq!(
        h.client.engine_version().await.unwrap().to_string(),
        "0.114.0"
    );
}

#[tokio::test]
async fn stalled_version_probe_still_reports_retryable_failure_with_bounded_fallback() {
    use std::time::{Duration, Instant};
    let h = harness_with_version_delay(
        "v0.114.0",
        vec![Reply::ok(proto::dispatcher::ActionEventResponse::default())],
        Duration::from_secs(60),
    )
    .await;
    let start = Instant::now();
    tokio::time::timeout(
        Duration::from_secs(4),
        h.transport
            .send_step_action_event(proto::dispatcher::StepActionEvent {
                event_type: proto::dispatcher::StepActionEventType::StepEventTypeFailed as i32,
                event_payload: "busy".into(),
                should_not_retry: Some(false),
                retry_after_ms: Some(1250),
                ..Default::default()
            }),
    )
    .await
    .expect("version lookup blocked task failure reporting")
    .unwrap();
    assert!(start.elapsed() < Duration::from_secs(4));
    let reqs = h.requests.lock().await;
    assert_eq!(
        reqs.iter()
            .filter(|(p, _)| p.ends_with("/GetVersion"))
            .count(),
        1
    );
    let (_, body) = reqs
        .iter()
        .find(|(p, _)| p.ends_with("/SendStepActionEvent"))
        .unwrap();
    let event = proto::dispatcher::StepActionEvent::decode(body.as_slice()).unwrap();
    assert_eq!(event.retry_after_ms, None);
    assert_eq!(event.should_not_retry, Some(false));
    assert_eq!(event.event_payload, "busy");
}
