#[path = "support/grpc.rs"]
mod grpc;
use another_hatchet_sdk::*;
use grpc::*;
use prost::Message;
use serde_json::{Value, json};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
const FIRST: &str = "00000000-0000-4000-8000-000000000001";
const SECOND: &str = "00000000-0000-4000-8000-000000000002";
fn config(method: IdempotencyMethod) -> IdempotencyConfig {
    IdempotencyConfig {
        expression: "input.order_id".into(),
        ttl: Duration::from_secs(60),
        method,
    }
}

#[tokio::test]
async fn registration_sends_engine_owned_idempotency_with_exact_ttl_and_method() {
    let h = harness(
        "v0.114.0",
        vec![Reply::ok(
            proto::v1::CreateWorkflowVersionResponse::default(),
        )],
    )
    .await;
    let task = h.client.task::<Value, Value>("task");
    h.client
        .register_workflow(
            WorkflowDeclaration::<Value, Value>::new("orders")
                .idempotency(config(IdempotencyMethod::Status))
                .step(&task, TaskConfig::default()),
        )
        .await
        .unwrap();
    let requests = h.requests.lock().await;
    let (_, body) = requests
        .iter()
        .find(|(path, _)| path.ends_with("/PutWorkflow"))
        .unwrap();
    let request = proto::v1::CreateWorkflowVersionRequest::decode(body.as_slice()).unwrap();
    let policy = request.idempotency.unwrap();
    assert_eq!(policy.expression, "input.order_id");
    assert_eq!(policy.ttl_ms, 60_000);
    assert_eq!(
        policy.method,
        Some(proto::v1::IdempotencyMethod::Status as i32)
    );
}
#[tokio::test]
async fn invalid_local_and_older_engine_policies_do_not_silently_register() {
    let h = harness("v0.94.14", vec![]).await;
    let mut invalid = config(IdempotencyMethod::Ttl);
    invalid.ttl = Duration::ZERO;
    assert!(matches!(
        h.client
            .register_workflow(
                WorkflowDeclaration::<Value, Value>::new("invalid").idempotency(invalid)
            )
            .await,
        Err(Error::InputValidation(_))
    ));
    assert!(h.requests.lock().await.is_empty());
    assert!(matches!(
        h.client
            .register_workflow(
                WorkflowDeclaration::<Value, Value>::new("old")
                    .idempotency(config(IdempotencyMethod::Ttl))
            )
            .await,
        Err(Error::UnsupportedOperation(_))
    ));
    assert!(
        h.requests
            .lock()
            .await
            .iter()
            .all(|(path, _)| path.ends_with("/GetVersion"))
    );
    let local = Client::new(std::sync::Arc::new(InMemoryTransport::new()));
    assert!(matches!(
        local
            .register_workflow(
                WorkflowDeclaration::<Value, Value>::new("local")
                    .idempotency(config(IdempotencyMethod::Ttl))
            )
            .await,
        Err(Error::UnsupportedOperation(_))
    ));
}
#[derive(prost::Message)]
struct Envelope {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<prost_types::Any>,
}
fn collision_details(name: &str, body: impl Message) -> Vec<u8> {
    Envelope {
        code: 6,
        message: "collision".into(),
        details: vec![prost_types::Any {
            type_url: format!("type.googleapis.com/v1.{name}"),
            value: body.encode_to_vec(),
        }],
    }
    .encode_to_vec()
}
#[tokio::test]
async fn collision_errors_keep_existing_and_successful_ids_and_are_never_retried() {
    let single = proto::v1::IdempotencyCollisionError {
        existing_run_external_id: FIRST.into(),
        colliding_run_external_id: SECOND.into(),
    };
    let bulk = proto::v1::BulkTriggerIdempotencyCollisionError {
        successful_workflow_run_external_ids: vec![SECOND.into()],
        collisions: vec![single.clone()],
    };
    let h = harness(
        "v0.114.0",
        vec![
            Reply::error(6, collision_details("IdempotencyCollisionError", single)),
            Reply::error(
                6,
                collision_details("BulkTriggerIdempotencyCollisionError", bulk),
            ),
            Reply::error(6, vec![0xff]),
            Reply::error(
                6,
                collision_details(
                    "BulkTriggerIdempotencyCollisionError",
                    proto::v1::BulkTriggerIdempotencyCollisionError {
                        successful_workflow_run_external_ids: vec![SECOND.into()],
                        collisions: vec![proto::v1::IdempotencyCollisionError::default()],
                    },
                ),
            ),
        ],
    )
    .await;
    let workflow = h.client.workflow::<Value, Value>("orders");
    let error = workflow
        .run_no_wait(json!({}), RunOpts::default())
        .await
        .unwrap_err();
    assert!(!error.is_retryable());
    match error {
        Error::IdempotencyCollision(c) => {
            assert_eq!(c.existing_run_id.0, FIRST);
            assert_eq!(c.colliding_run_id.unwrap().0, SECOND);
        }
        e => panic!("{e:?}"),
    }
    let error = workflow
        .run_many_no_wait([json!({}), json!({})], RunOpts::default())
        .await
        .unwrap_err();
    assert!(!error.is_retryable());
    match error {
        Error::BulkIdempotencyCollision(c) => {
            assert_eq!(c.successful_run_ids[0].0, SECOND);
            assert_eq!(c.collisions[0].existing_run_id.0, FIRST);
        }
        e => panic!("{e:?}"),
    }
    assert!(matches!(
        workflow.run_no_wait(json!({}), RunOpts::default()).await,
        Err(Error::DedupeViolation(_))
    ));
    assert!(matches!(
        workflow
            .run_many_no_wait([json!({})], RunOpts::default())
            .await,
        Err(Error::DedupeViolation(_))
    ));
    assert_eq!(h.requests.lock().await.len(), 4);
}
#[tokio::test]
async fn normal_and_bulk_dispatch_preserve_explicit_sticky_worker_routing() {
    let h = harness(
        "v0.94.14",
        vec![
            Reply::ok(proto::workflows::TriggerWorkflowResponse {
                workflow_run_id: FIRST.into(),
            }),
            Reply::ok(proto::workflows::BulkTriggerWorkflowResponse {
                workflow_run_ids: vec![SECOND.into()],
            }),
        ],
    )
    .await;
    let workflow = h.client.workflow::<Value, Value>("routing");
    let opts = RunOpts {
        sticky: Some(FIRST.into()),
        ..Default::default()
    };
    workflow.run_no_wait(json!({}), opts.clone()).await.unwrap();
    workflow.run_many_no_wait([json!({})], opts).await.unwrap();
    let requests = h.requests.lock().await;
    let single = proto::v1::TriggerWorkflowRequest::decode(requests[0].1.as_slice()).unwrap();
    let bulk =
        proto::workflows::BulkTriggerWorkflowRequest::decode(requests[1].1.as_slice()).unwrap();
    assert_eq!(single.desired_worker_id.as_deref(), Some(FIRST));
    assert_eq!(bulk.workflows[0].desired_worker_id.as_deref(), Some(FIRST));
    assert!(
        requests
            .iter()
            .all(|(path, _)| path.contains("WorkflowService"))
    );
}
#[tokio::test]
async fn rate_pause_preserves_the_engine_time_and_never_shortens_or_fakes_the_ack() {
    let requested = UNIX_EPOCH + Duration::from_secs(2_000_000_000);
    let later = requested + Duration::from_secs(60);
    let h = harness(
        "v0.114.0",
        vec![
            Reply::ok(proto::workflows::PauseRateLimitResponse {
                paused_until: Some(prost_types::Timestamp {
                    seconds: 2_000_000_060,
                    nanos: 0,
                }),
            }),
            Reply::ok(proto::workflows::PauseRateLimitResponse { paused_until: None }),
            Reply::error(14, vec![]),
        ],
    )
    .await;
    let result = h
        .client
        .rate_limits()
        .pause("api/raw-key", requested)
        .await
        .unwrap();
    assert_eq!(result.paused_until, later);
    assert_eq!(result.key, "api/raw-key");
    assert!(
        h.client
            .rate_limits()
            .pause("api/raw-key", requested)
            .await
            .is_err()
    );
    assert!(
        h.client
            .rate_limits()
            .pause("api/raw-key", requested)
            .await
            .is_err()
    );
    let requests = h.requests.lock().await;
    let writes = requests
        .iter()
        .filter(|(path, _)| path.ends_with("/PauseRateLimit"))
        .collect::<Vec<_>>();
    assert_eq!(writes.len(), 3);
    let request = proto::workflows::PauseRateLimitRequest::decode(writes[0].1.as_slice()).unwrap();
    assert_eq!(request.key, "api/raw-key");
    assert_eq!(request.paused_until.unwrap().seconds, 2_000_000_000);
}
#[tokio::test]
async fn rate_pause_rejects_unsupported_engines_and_invalid_time_before_mutations() {
    let h = harness("v0.111.0", vec![]).await;
    assert!(matches!(
        h.client.rate_limits().pause("", SystemTime::now()).await,
        Err(Error::InputValidation(_))
    ));
    assert!(matches!(
        h.client.rate_limits().pause("api", SystemTime::now()).await,
        Err(Error::UnsupportedOperation(_))
    ));
    assert!(
        h.requests
            .lock()
            .await
            .iter()
            .all(|(path, _)| path.ends_with("/GetVersion"))
    );
}
