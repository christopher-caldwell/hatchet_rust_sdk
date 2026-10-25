//! Streaming contracts exercised through actual HTTP/2 and in-memory transports.
use std::sync::Arc;
use std::time::Duration;

use another_hatchet_sdk::proto::{
    dispatcher::{ResourceEventType, WorkflowEvent},
    events::{PutStreamEventRequest, PutStreamEventResponse},
};
use another_hatchet_sdk::{
    Client, Context, ContextIds, GrpcTransport, HatchetConfig, InMemoryContext, InMemoryTransport,
    TlsConfig, Transport, WorkflowRunId,
};
use prost::Message;
use serde_json::json;
use tokio::sync::{Mutex, Notify};
use tonic::codegen::http;

#[derive(Clone)]
struct Event {
    delay: Duration,
    value: WorkflowEvent,
}

fn chunk(value: serde_json::Value, delay: Duration) -> Event {
    Event {
        delay,
        value: WorkflowEvent {
            event_type: ResourceEventType::Stream as i32,
            event_payload: value.to_string(),
            ..Default::default()
        },
    }
}

fn terminal() -> Event {
    Event {
        delay: Duration::ZERO,
        value: WorkflowEvent {
            event_type: ResourceEventType::Completed as i32,
            hangup: true,
            ..Default::default()
        },
    }
}

fn frame(message: impl Message) -> Vec<u8> {
    let body = message.encode_to_vec();
    let mut framed = vec![0];
    framed.extend_from_slice(&u32::try_from(body.len()).unwrap().to_be_bytes());
    framed.extend_from_slice(&body);
    framed
}

struct Server {
    client: Client<GrpcTransport>,
    transport: Arc<GrpcTransport>,
    published: Arc<Mutex<Vec<PutStreamEventRequest>>>,
    cancelled: Arc<Notify>,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn server(events: Vec<Event>, status: &'static str, reject_publish: bool) -> Server {
    let listener = tokio::net::TcpListener::bind("[::1]:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let published = Arc::new(Mutex::new(Vec::new()));
    let cancelled = Arc::new(Notify::new());
    let task = tokio::spawn({
        let published = Arc::clone(&published);
        let cancelled = Arc::clone(&cancelled);
        async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let events = events.clone();
                let published = Arc::clone(&published);
                let cancelled = Arc::clone(&cancelled);
                tokio::spawn(async move {
                    let mut connection = h2::server::handshake(socket).await.unwrap();
                    while let Some(incoming) = connection.accept().await {
                        let (request, mut respond) = incoming.unwrap();
                        let publishing = request.uri().path().ends_with("/PutStreamEvent");
                        let events = events.clone();
                        let published = Arc::clone(&published);
                        let cancelled = Arc::clone(&cancelled);
                        // Polling the connection continues while this task sends
                        // delayed frames, so the first frame is flushed immediately.
                        tokio::spawn(async move {
                            let mut body = request.into_body();
                            let mut bytes = Vec::new();
                            while let Some(data) = body.data().await {
                                let data = data.unwrap();
                                bytes.extend_from_slice(&data);
                                body.flow_control().release_capacity(data.len()).unwrap();
                            }
                            if publishing {
                                published
                                    .lock()
                                    .await
                                    .push(PutStreamEventRequest::decode(&bytes[5..]).unwrap());
                            }
                            let headers = http::Response::builder()
                                .header("content-type", "application/grpc")
                                .body(())
                                .unwrap();
                            let mut stream = respond.send_response(headers, false).unwrap();
                            if publishing {
                                if !reject_publish {
                                    stream
                                        .send_data(frame(PutStreamEventResponse {}).into(), false)
                                        .unwrap();
                                }
                            } else {
                                for event in events {
                                    tokio::select! {
                                        _=tokio::time::sleep(event.delay)=>{}
                                        _=std::future::poll_fn(|cx|stream.poll_reset(cx))=>{
                                            cancelled.notify_one();return;
                                        }
                                    }
                                    if stream.send_data(frame(event.value).into(), false).is_err() {
                                        return;
                                    }
                                }
                            }
                            let mut trailers = http::HeaderMap::new();
                            trailers.insert(
                                "grpc-status",
                                (if publishing && reject_publish {
                                    "7"
                                } else {
                                    status
                                })
                                .parse()
                                .unwrap(),
                            );
                            trailers
                                .insert("grpc-message", "fixture-interruption".parse().unwrap());
                            let _ = stream.send_trailers(trailers);
                        });
                    }
                });
            }
        }
    });
    let config = HatchetConfig::builder()
        .host_port(addr.to_string())
        .server_url("http://[::1]:1")
        .tenant_id("synthetic")
        .token("synthetic-unused")
        .tls(TlsConfig::Insecure)
        .build()
        .unwrap();
    let transport = Arc::new(GrpcTransport::from_config(config).await.unwrap());
    Server {
        client: Client::new(Arc::clone(&transport)),
        transport,
        published,
        cancelled,
        task,
    }
}

#[tokio::test]
async fn grpc_stream_is_incremental_and_survives_idle_time_beyond_five_seconds() {
    let s = server(
        vec![
            chunk(json!("first"), Duration::ZERO),
            chunk(json!("late"), Duration::from_secs(6)),
            terminal(),
        ],
        "0",
        false,
    )
    .await;
    let mut stream = s
        .client
        .run_ref::<serde_json::Value>("run-1")
        .stream_events()
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        json!("first")
    );
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(15), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap(),
        json!("late")
    );
    assert!(stream.next().await.is_none());
}

#[tokio::test]
async fn only_published_values_are_yielded_and_hangup_ends_the_subscription() {
    let started = Event {
        delay: Duration::ZERO,
        value: WorkflowEvent {
            event_type: ResourceEventType::Started as i32,
            ..Default::default()
        },
    };
    let s = server(
        vec![
            started,
            chunk(json!({"chunk":1}), Duration::ZERO),
            terminal(),
            chunk(json!("after-hangup"), Duration::ZERO),
        ],
        "0",
        false,
    )
    .await;
    let values = s
        .client
        .runs()
        .subscribe_to_stream(&WorkflowRunId("run-1".to_owned()))
        .await
        .unwrap()
        .collect_all()
        .await
        .unwrap();
    assert_eq!(values, vec![json!({"chunk":1})]);
}

#[tokio::test]
async fn grpc_errors_are_delivered_instead_of_returning_a_successful_partial_batch() {
    let s = server(vec![chunk(json!(1), Duration::ZERO)], "14", false).await;
    let mut stream = s
        .client
        .run_ref::<serde_json::Value>("run-1")
        .stream_events()
        .await
        .unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap(), json!(1));
    assert!(
        stream
            .next()
            .await
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("fixture-interruption")
    );
    assert!(stream.next().await.is_none());
    assert!(
        s.client
            .run_ref::<serde_json::Value>("run-1")
            .stream()
            .await
            .is_err()
    );
}

#[tokio::test]
async fn dropping_a_consumer_closes_the_grpc_subscription() {
    let s = server(
        vec![
            chunk(json!(1), Duration::ZERO),
            chunk(json!(2), Duration::from_secs(60)),
        ],
        "0",
        false,
    )
    .await;
    let mut stream = s
        .client
        .run_ref::<serde_json::Value>("run-1")
        .stream_events()
        .await
        .unwrap();
    stream.next().await.unwrap().unwrap();
    drop(stream);
    tokio::time::timeout(Duration::from_secs(2), s.cancelled.notified())
        .await
        .expect("consumer drop must cancel HTTP/2 stream");
}

#[tokio::test]
async fn publishing_uses_task_identity_and_propagates_engine_rejection() {
    for rejected in [true, false] {
        let s = server(Vec::new(), "0", rejected).await;
        let ctx = InMemoryContext::new(
            ContextIds {
                workflow_name: "dag".to_owned(),
                task_name: "leaf".to_owned(),
                workflow_run_id: WorkflowRunId("workflow-id".to_owned()),
                task_run_id: "task-id".to_owned(),
                workflow_id: None,
                workflow_version_id: None,
            },
            Arc::clone(&s.transport),
        );
        assert_eq!(ctx.put_stream(json!({"chunk":1})).await.is_err(), rejected);
        let published = s.published.lock().await;
        assert_eq!(published[0].task_run_external_id, "task-id");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&published[0].message).unwrap(),
            json!({"chunk":1})
        );
    }
}

#[tokio::test]
async fn in_memory_subscriptions_replay_history_follow_new_values_and_close_on_completion() {
    let transport = Arc::new(InMemoryTransport::new());
    let client = Client::new(Arc::clone(&transport));
    let id = WorkflowRunId("run-1".to_owned());
    transport
        .append_run_stream(&id, "task-1", json!(1))
        .await
        .unwrap();
    let mut first = client
        .run_ref::<serde_json::Value>(id.0.clone())
        .stream_events()
        .await
        .unwrap();
    let mut second = client.runs().subscribe_to_stream(&id).await.unwrap();
    assert_eq!(first.next().await.unwrap().unwrap(), json!(1));
    assert_eq!(second.next().await.unwrap().unwrap(), json!(1));
    assert!(
        tokio::time::timeout(Duration::from_millis(20), first.next())
            .await
            .is_err()
    );
    transport
        .append_run_stream(&id, "task-2", json!(2))
        .await
        .unwrap();
    transport.resolve_result(&id, Ok(json!(null))).await;
    assert_eq!(first.collect_all().await.unwrap(), vec![json!(2)]);
    assert_eq!(second.collect_all().await.unwrap(), vec![json!(2)]);
    assert_eq!(
        client
            .run_ref::<serde_json::Value>(id.0)
            .stream()
            .await
            .unwrap(),
        vec![json!(1), json!(2)]
    );
}

#[tokio::test]
async fn cancellation_closes_an_idle_in_memory_subscription() {
    let transport = InMemoryTransport::new();
    let id = WorkflowRunId("cancelled".to_owned());
    let mut stream = transport.subscribe_run_stream(&id).await.unwrap();
    transport.cancel_workflow_run(&id).await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .is_none()
    );
}
