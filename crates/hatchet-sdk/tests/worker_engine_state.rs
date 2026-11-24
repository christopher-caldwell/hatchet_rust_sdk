//! Worker pause and label changes on an engine-backed worker must reach the
//! engine (REST worker pause, `UpsertWorkerLabels`) and must not make the
//! worker fail work the engine assigned to it.
//!
//! A scripted dispatcher speaks HTTP/2 gRPC like `tests/support/grpc.rs`: it
//! answers GetVersion, Register, Heartbeat, SendStepActionEvent and
//! UpsertWorkerLabels, streams test-pushed assignments down ListenV2 and
//! records every request. Register replies only while the test's register
//! gate is open. A REST peer records each HTTP request.
use another_hatchet_sdk::{
    Client, Context, DesiredLabelConfig, GrpcTransport, HatchetConfig, InMemoryContext, LabelValue,
    RetryConfig, TaskConfig, TlsConfig, proto,
};
use prost::Message;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU16, Ordering};
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::sync::{Mutex, mpsc, watch};
use tonic::codegen::http;

type Captured = Arc<Mutex<Vec<(String, Vec<u8>)>>>;
type Event = proto::dispatcher::StepActionEventType;

struct Engine {
    client: Client<GrpcTransport>,
    grpc_requests: Captured,
    rest_requests: Arc<Mutex<Vec<String>>>,
    rest_status: Arc<AtomicU16>,
    /// Register replies only while this is `true`.
    register_gate: watch::Sender<bool>,
    actions: mpsc::UnboundedSender<proto::dispatcher::AssignedAction>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Drop for Engine {
    fn drop(&mut self) {
        for task in &self.tasks {
            task.abort();
        }
    }
}

fn grpc_frame(payload: Vec<u8>) -> Vec<u8> {
    let mut frame = vec![0];
    frame.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
    frame.extend(payload);
    frame
}

async fn engine() -> Engine {
    let listener = tokio::net::TcpListener::bind("[::1]:0").await.unwrap();
    let grpc_addr = listener.local_addr().unwrap();
    let grpc_requests: Captured = Arc::default();
    let (register_gate, gate) = watch::channel(true);
    let (actions, rx) = mpsc::unbounded_channel::<proto::dispatcher::AssignedAction>();
    let rx = Arc::new(Mutex::new(rx));
    let grpc_task = tokio::spawn({
        let requests = Arc::clone(&grpc_requests);
        async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let (requests, rx, gate) = (Arc::clone(&requests), Arc::clone(&rx), gate.clone());
                tokio::spawn(async move {
                    let mut connection = h2::server::handshake(socket).await.unwrap();
                    while let Some(Ok((mut request, mut respond))) = connection.accept().await {
                        let (requests, rx, mut gate) =
                            (Arc::clone(&requests), Arc::clone(&rx), gate.clone());
                        tokio::spawn(async move {
                            let path = request.uri().path().to_owned();
                            let mut body = Vec::new();
                            while let Some(Ok(data)) = request.body_mut().data().await {
                                let _ = request
                                    .body_mut()
                                    .flow_control()
                                    .release_capacity(data.len());
                                body.extend_from_slice(&data);
                            }
                            let message = body.get(5..).unwrap_or_default().to_vec();
                            requests.lock().await.push((path.clone(), message));
                            let headers = http::Response::builder()
                                .header("content-type", "application/grpc")
                                .body(())
                                .unwrap();
                            let mut stream = respond.send_response(headers, false).unwrap();
                            let reply: Option<Vec<u8>> = if path.ends_with("/GetVersion") {
                                Some(
                                    proto::dispatcher::GetVersionResponse {
                                        version: "v0.114.0".into(),
                                    }
                                    .encode_to_vec(),
                                )
                            } else if path.ends_with("/Register") {
                                let _ = gate.wait_for(|open| *open).await;
                                Some(
                                    proto::dispatcher::WorkerRegisterResponse {
                                        tenant_id: "synthetic".into(),
                                        worker_id: "worker-1".into(),
                                        worker_name: "w".into(),
                                    }
                                    .encode_to_vec(),
                                )
                            } else if path.ends_with("/Heartbeat") {
                                Some(proto::dispatcher::HeartbeatResponse {}.encode_to_vec())
                            } else if path.ends_with("/ListenV2") {
                                let mut rx = rx.lock().await;
                                while let Some(action) = rx.recv().await {
                                    if stream
                                        .send_data(grpc_frame(action.encode_to_vec()).into(), false)
                                        .is_err()
                                    {
                                        return;
                                    }
                                }
                                return;
                            } else if path.ends_with("/SendStepActionEvent") {
                                Some(
                                    proto::dispatcher::ActionEventResponse::default()
                                        .encode_to_vec(),
                                )
                            } else if path.ends_with("/UpsertWorkerLabels") {
                                Some(
                                    proto::dispatcher::UpsertWorkerLabelsResponse {
                                        tenant_id: "synthetic".into(),
                                        worker_id: "worker-1".into(),
                                    }
                                    .encode_to_vec(),
                                )
                            } else {
                                None
                            };
                            let code = if let Some(payload) = reply {
                                let _ = stream.send_data(grpc_frame(payload).into(), false);
                                "0"
                            } else {
                                "12"
                            };
                            let mut trailers = http::HeaderMap::new();
                            trailers.insert("grpc-status", code.parse().unwrap());
                            let _ = stream.send_trailers(trailers);
                        });
                    }
                });
            }
        }
    });

    let listener = tokio::net::TcpListener::bind("[::1]:0").await.unwrap();
    let rest_addr = listener.local_addr().unwrap();
    let rest_requests = Arc::new(Mutex::new(Vec::new()));
    let rest_status = Arc::new(AtomicU16::new(200));
    let rest_task = tokio::spawn({
        let (received, status) = (Arc::clone(&rest_requests), Arc::clone(&rest_status));
        async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut bytes = Vec::new();
                let header_end = loop {
                    let mut chunk = [0; 1024];
                    let read = socket.read(&mut chunk).await.unwrap();
                    bytes.extend_from_slice(&chunk[..read]);
                    if let Some(end) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                        break end + 4;
                    }
                    assert!(read > 0, "REST request ended before its headers");
                };
                let content_len = String::from_utf8_lossy(&bytes[..header_end])
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap_or(0);
                while bytes.len() < header_end + content_len {
                    let mut chunk = [0; 1024];
                    let read = socket.read(&mut chunk).await.unwrap();
                    if read == 0 {
                        break;
                    }
                    bytes.extend_from_slice(&chunk[..read]);
                }
                received
                    .lock()
                    .await
                    .push(String::from_utf8_lossy(&bytes).into_owned());
                let response = format!(
                    "HTTP/1.1 {} Test\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{{}}",
                    status.load(Ordering::SeqCst)
                );
                let _ = socket.write_all(response.as_bytes()).await;
            }
        }
    });

    let transport = GrpcTransport::from_config(
        HatchetConfig::builder()
            .host_port(grpc_addr.to_string())
            .server_url(format!("http://{rest_addr}"))
            .tenant_id("synthetic")
            .token("synthetic")
            .tls(TlsConfig::Insecure)
            .build()
            .unwrap(),
    )
    .await
    .unwrap()
    .with_retry_config(RetryConfig {
        max_retries: 0,
        ..RetryConfig::default()
    });
    Engine {
        client: Client::new(Arc::new(transport)),
        grpc_requests,
        rest_requests,
        rest_status,
        register_gate,
        actions,
        tasks: vec![grpc_task, rest_task],
    }
}

fn action(task: &str, run: usize) -> proto::dispatcher::AssignedAction {
    proto::dispatcher::AssignedAction {
        tenant_id: "synthetic".into(),
        workflow_run_id: format!("workflow-run-{run}"),
        job_id: "job".into(),
        job_name: "wf".into(),
        job_run_id: format!("job-run-{run}"),
        task_id: "step".into(),
        task_run_external_id: format!("task-run-{run}"),
        action_id: format!("default:{task}"),
        action_type: proto::dispatcher::ActionType::StartStepRun as i32,
        action_payload: json!({ "input": {} }).to_string(),
        task_name: task.into(),
        ..Default::default()
    }
}

/// Waits (up to 5 s) for a COMPLETED or FAILED event for `task_run`.
async fn final_event(engine: &Engine, task_run: &str) -> proto::dispatcher::StepActionEvent {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        let event = engine
            .grpc_requests
            .lock()
            .await
            .iter()
            .filter(|(path, _)| path.ends_with("/SendStepActionEvent"))
            .map(|(_, body)| proto::dispatcher::StepActionEvent::decode(body.as_slice()).unwrap())
            .find(|event| {
                event.task_run_external_id == task_run
                    && (event.event_type == Event::StepEventTypeCompleted as i32
                        || event.event_type == Event::StepEventTypeFailed as i32)
            });
        if let Some(event) = event {
            return event;
        }
        assert!(Instant::now() < deadline, "no final event for {task_run}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

async fn label_upserts(engine: &Engine) -> Vec<proto::dispatcher::UpsertWorkerLabelsRequest> {
    engine
        .grpc_requests
        .lock()
        .await
        .iter()
        .filter(|(path, _)| path.ends_with("/UpsertWorkerLabels"))
        .map(|(_, body)| {
            proto::dispatcher::UpsertWorkerLabelsRequest::decode(body.as_slice()).unwrap()
        })
        .collect()
}

async fn worker_patches(engine: &Engine) -> Vec<String> {
    engine
        .rest_requests
        .lock()
        .await
        .iter()
        .filter(|request| request.starts_with("PATCH /api/v1/workers/worker-1 "))
        .cloned()
        .collect()
}

fn str_label(value: &str) -> proto::dispatcher::WorkerLabels {
    proto::dispatcher::WorkerLabels {
        str_value: Some(value.to_owned()),
        int_value: None,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engine_worker_pause_reaches_the_engine_and_does_not_fail_assigned_work() {
    let engine = engine().await;
    let task = engine.client.task::<Value, Value>("noop");
    let handle = engine
        .client
        .worker("w")
        .register_task(
            &task,
            |input: Value, _ctx: InMemoryContext<GrpcTransport>| async move { Ok(input) },
        )
        .build()
        .start()
        .unwrap();
    handle
        .wait_until_ready(Duration::from_secs(5))
        .await
        .unwrap();

    handle.pause().await.expect("engine accepted the pause");
    let patches = worker_patches(&engine).await;
    assert_eq!(patches.len(), 1, "pause must PATCH the registered worker");
    assert!(patches[0].contains(r#""isPaused":true"#), "{}", patches[0]);
    assert!(handle.is_paused().await.unwrap());

    // An assignment the engine sent before it applied the pause runs normally.
    engine.actions.send(action("noop", 1)).unwrap();
    let event = final_event(&engine, "task-run-1").await;
    assert_eq!(
        event.event_type,
        Event::StepEventTypeCompleted as i32,
        "engine-assigned work failed locally: {}",
        event.event_payload
    );

    handle.unpause().await.expect("engine accepted the unpause");
    let patches = worker_patches(&engine).await;
    assert_eq!(patches.len(), 2);
    assert!(patches[1].contains(r#""isPaused":false"#), "{}", patches[1]);
    assert!(!handle.is_paused().await.unwrap());

    // An engine rejection is returned and leaves the local state unchanged.
    engine.rest_status.store(500, Ordering::SeqCst);
    assert!(handle.pause().await.is_err());
    assert!(!handle.is_paused().await.unwrap());

    handle.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn engine_worker_label_upserts_reach_the_engine() {
    let engine = engine().await;
    // No pause is ever requested, so neither registration nor label upserts
    // may depend on the REST API: make every REST call fail.
    engine.rest_status.store(500, Ordering::SeqCst);
    let task = engine.client.task::<Value, Value>("regional").with_config(
        TaskConfig::builder()
            .desired_label("region", DesiredLabelConfig::new("us").required(true))
            .build(),
    );
    let (ctx_tx, mut ctx_rx) = mpsc::unbounded_channel();
    let handle = engine
        .client
        .worker("w")
        .labels(BTreeMap::from([("region".to_owned(), "us".to_owned())]))
        .register_task(
            &task,
            move |input: Value, ctx: InMemoryContext<GrpcTransport>| {
                let ctx_tx = ctx_tx.clone();
                async move {
                    if ctx.task_run_id() == "task-run-1" {
                        let result = ctx
                            .upsert_worker_labels(BTreeMap::from([
                                ("gpu".to_owned(), LabelValue::String("a100".to_owned())),
                                ("memory".to_owned(), LabelValue::Int(64)),
                            ]))
                            .await;
                        let _ = ctx_tx.send(result.map_err(|error| error.to_string()));
                    }
                    Ok(input)
                }
            },
        )
        .build()
        .start()
        .unwrap();
    handle
        .wait_until_ready(Duration::from_secs(5))
        .await
        .unwrap();

    // ctx.upsert_worker_labels from inside an engine-assigned task.
    engine.actions.send(action("regional", 1)).unwrap();
    let ctx_result = tokio::time::timeout(Duration::from_secs(5), ctx_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ctx_result, Ok(()));
    let upserts = label_upserts(&engine).await;
    assert_eq!(upserts.len(), 1, "ctx label upsert must reach the engine");
    assert_eq!(upserts[0].worker_id, "worker-1");
    assert_eq!(upserts[0].labels["gpu"], str_label("a100"));
    assert_eq!(
        upserts[0].labels["memory"],
        proto::dispatcher::WorkerLabels {
            str_value: None,
            int_value: Some(64),
        }
    );
    final_event(&engine, "task-run-1").await;

    // WorkerHandle::upsert_labels.
    handle
        .upsert_labels(BTreeMap::from([("region".to_owned(), "eu".to_owned())]))
        .await
        .expect("engine accepted the labels");
    let upserts = label_upserts(&engine).await;
    assert_eq!(
        upserts.len(),
        2,
        "handle label upsert must reach the engine"
    );
    assert_eq!(upserts[1].worker_id, "worker-1");
    assert_eq!(upserts[1].labels.len(), 1);
    assert_eq!(upserts[1].labels["region"], str_label("eu"));
    assert_eq!(handle.labels().await.unwrap()["region"], "eu");

    // The engine routes by its labels; the worker must not second-guess it.
    engine.actions.send(action("regional", 2)).unwrap();
    let event = final_event(&engine, "task-run-2").await;
    assert_eq!(
        event.event_type,
        Event::StepEventTypeCompleted as i32,
        "engine-routed work failed on a local label check: {}",
        event.event_payload
    );
    assert!(
        engine.rest_requests.lock().await.is_empty(),
        "no pause was requested, so the worker must not call the REST API"
    );

    handle.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn changes_made_before_registration_are_applied_when_the_worker_registers() {
    let engine = engine().await;
    // Hold Register until the changes below are made, so the worker has no
    // engine-assigned ID yet when they run.
    engine.register_gate.send_modify(|open| *open = false);
    let task = engine.client.task::<Value, Value>("noop");
    let handle = engine
        .client
        .worker("w")
        .labels(BTreeMap::from([("region".to_owned(), "us".to_owned())]))
        .register_task(
            &task,
            |input: Value, _ctx: InMemoryContext<GrpcTransport>| async move { Ok(input) },
        )
        .build()
        .start()
        .unwrap();

    // Not registered yet: there is no engine worker to update, so these only
    // record the wanted state.
    handle
        .upsert_labels(BTreeMap::from([("region".to_owned(), "eu".to_owned())]))
        .await
        .unwrap();
    handle.pause().await.unwrap();
    assert!(worker_patches(&engine).await.is_empty());
    assert!(label_upserts(&engine).await.is_empty());

    engine.register_gate.send_modify(|open| *open = true);
    handle
        .wait_until_ready(Duration::from_secs(5))
        .await
        .unwrap();

    // Register may or may not carry region=eu, depending on whether the
    // worker read its labels before or after the upsert. Either way the
    // registered worker must end up with it, and paused.
    let patches = worker_patches(&engine).await;
    assert_eq!(
        patches.len(),
        1,
        "a requested pause is applied on registration"
    );
    assert!(patches[0].contains(r#""isPaused":true"#), "{}", patches[0]);
    let requests = engine.grpc_requests.lock().await.clone();
    let register = requests
        .iter()
        .position(|(path, _)| path.ends_with("/Register"))
        .expect("worker registered");
    let upsert = requests
        .iter()
        .position(|(path, _)| path.ends_with("/UpsertWorkerLabels"))
        .expect("labels upserted before registration are sent on registration");
    assert!(
        upsert > register,
        "labels must be sent to the registered worker"
    );
    let upserts = label_upserts(&engine).await;
    assert_eq!(upserts.len(), 1);
    assert_eq!(upserts[0].worker_id, "worker-1");
    assert_eq!(upserts[0].labels.len(), 1);
    assert_eq!(upserts[0].labels["region"], str_label("eu"));

    handle.shutdown().await.unwrap();
}
