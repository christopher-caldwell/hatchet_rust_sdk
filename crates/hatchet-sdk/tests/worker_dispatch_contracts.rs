//! Worker dispatch contracts for the gRPC listen loop.
//!
//! A scripted HTTP/2 dispatcher answers GetVersion, Register and Heartbeat,
//! pushes assignments on ListenV2 and holds the stream open, and answers
//! SendStepActionEvent with a chosen gRPC status. Every SendStepActionEvent is
//! recorded in arrival order, before any reply; STARTED replies can be held
//! behind a gate the test opens.
use another_hatchet_sdk::proto::dispatcher::StepActionEventType as Ev;
use another_hatchet_sdk::{Client, GrpcTransport, HatchetConfig, TlsConfig, proto};
use prost::Message;
use serde_json::Value;
use std::collections::HashSet;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};
use tonic::codegen::http;

#[derive(Clone)]
struct Script {
    assignments: Vec<proto::dispatcher::AssignedAction>,
    /// When set, every STARTED reply waits until the test sends `true`.
    started_gate: Option<tokio::sync::watch::Receiver<bool>>,
    /// gRPC status for STARTED events; COMPLETED and FAILED are always OK.
    started_status: u16,
}

#[derive(Clone, Default)]
struct Peer {
    registrations: Arc<AtomicUsize>,
    step_events: Arc<std::sync::Mutex<Vec<(String, i32)>>>,
}

fn frame(body: Vec<u8>) -> Vec<u8> {
    let mut frame = vec![0];
    frame.extend_from_slice(&(body.len() as u32).to_be_bytes());
    frame.extend_from_slice(&body);
    frame
}

fn grpc_headers() -> http::Response<()> {
    http::Response::builder()
        .header("content-type", "application/grpc")
        .body(())
        .unwrap()
}

async fn dispatcher(script: Script) -> (String, Peer, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("[::1]:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let peer = Peer::default();
    let shared = peer.clone();
    let server = tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let (peer, script) = (shared.clone(), script.clone());
            tokio::spawn(async move {
                let mut connection = h2::server::handshake(socket).await.unwrap();
                while let Some(request) = connection.accept().await {
                    let (mut request, mut respond) = request.unwrap();
                    let (peer, script) = (peer.clone(), script.clone());
                    tokio::spawn(async move {
                        let path = request.uri().path().to_owned();
                        let mut body = Vec::new();
                        while let Some(data) = request.body_mut().data().await {
                            let data = data.unwrap();
                            let _ = request
                                .body_mut()
                                .flow_control()
                                .release_capacity(data.len());
                            body.extend_from_slice(&data);
                        }
                        let mut status = 0;
                        let reply = if path.ends_with("/GetVersion") {
                            proto::dispatcher::GetVersionResponse {
                                version: "v0.114.0".into(),
                            }
                            .encode_to_vec()
                        } else if path.ends_with("/Register") {
                            peer.registrations.fetch_add(1, Ordering::SeqCst);
                            proto::dispatcher::WorkerRegisterResponse {
                                worker_id: "worker-1".into(),
                                ..Default::default()
                            }
                            .encode_to_vec()
                        } else if path.ends_with("/Heartbeat") {
                            proto::dispatcher::HeartbeatResponse::default().encode_to_vec()
                        } else if path.ends_with("/ListenV2") {
                            let mut stream = respond.send_response(grpc_headers(), false).unwrap();
                            for action in &script.assignments {
                                stream
                                    .send_data(frame(action.encode_to_vec()).into(), false)
                                    .unwrap();
                            }
                            return std::future::pending::<()>().await;
                        } else if path.ends_with("/SendStepActionEvent") {
                            let event =
                                proto::dispatcher::StepActionEvent::decode(&body[5..]).unwrap();
                            peer.step_events
                                .lock()
                                .unwrap()
                                .push((event.task_run_external_id.clone(), event.event_type));
                            if event.event_type == Ev::StepEventTypeStarted as i32 {
                                if let Some(mut gate) = script.started_gate.clone() {
                                    let _ = gate.wait_for(|open| *open).await;
                                }
                                status = script.started_status;
                            }
                            proto::dispatcher::ActionEventResponse::default().encode_to_vec()
                        } else {
                            status = 12; // UNIMPLEMENTED
                            Vec::new()
                        };
                        let mut stream = respond.send_response(grpc_headers(), false).unwrap();
                        if status == 0 {
                            stream.send_data(frame(reply).into(), false).unwrap();
                        }
                        let mut trailers = http::HeaderMap::new();
                        trailers.insert("grpc-status", status.to_string().parse().unwrap());
                        stream.send_trailers(trailers).unwrap();
                    });
                }
            });
        }
    });
    (addr, peer, server)
}

async fn client(addr: String) -> Client<GrpcTransport> {
    let transport = GrpcTransport::from_config(
        HatchetConfig::builder()
            .host_port(addr)
            .server_url("http://[::1]:1")
            .tenant_id("synthetic")
            .token("synthetic")
            .tls(TlsConfig::Insecure)
            .build()
            .unwrap(),
    )
    .await
    .unwrap();
    Client::new(Arc::new(transport))
}

fn assignments(n: usize) -> Vec<proto::dispatcher::AssignedAction> {
    (0..n)
        .map(|i| proto::dispatcher::AssignedAction {
            tenant_id: "synthetic".into(),
            workflow_run_id: format!("run-{i}"),
            job_name: "dispatch".into(),
            task_run_external_id: format!("task-run-{i}"),
            action_id: "default:quick".into(),
            action_type: proto::dispatcher::ActionType::StartStepRun as i32,
            action_payload: r#"{"input":{}}"#.into(),
            task_name: "quick".into(),
            ..Default::default()
        })
        .collect()
}

async fn wait_for(deadline: Duration, mut done: impl FnMut() -> bool) {
    let deadline = Instant::now() + deadline;
    while !done() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn started_tasks(peer: &Peer) -> usize {
    peer.step_events
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, kind)| *kind == Ev::StepEventTypeStarted as i32)
        .map(|(task, _)| task.clone())
        .collect::<HashSet<_>>()
        .len()
}

fn completed_tasks(peer: &Peer) -> usize {
    peer.step_events
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, kind)| *kind == Ev::StepEventTypeCompleted as i32)
        .count()
}

// Twenty assignments arrive together and the engine holds every STARTED reply
// until the test opens the gate. With STARTED awaited inside the listen loop,
// only the first STARTED is ever sent while the gate is closed; sent off the
// loop, all twenty reach the engine first. Handlers must still wait for their
// own STARTED, and each task's STARTED must reach the engine before COMPLETED.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn started_acknowledgements_do_not_serialize_dispatch() {
    const N: usize = 20;
    let (release, gate) = tokio::sync::watch::channel(false);
    let (addr, peer, server) = dispatcher(Script {
        assignments: assignments(N),
        started_gate: Some(gate),
        started_status: 0,
    })
    .await;
    let client = client(addr).await;
    let task = client.task::<Value, Value>("quick");
    let runs = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&runs);
    let handle = client
        .worker("dispatch-worker")
        .slots(50)
        .register_task(&task, move |_input: Value, _ctx| {
            let counted = Arc::clone(&counted);
            async move {
                counted.fetch_add(1, Ordering::SeqCst);
                Ok(Value::Null)
            }
        })
        .build()
        .start()
        .unwrap();

    // The timeout is reached only on failure; the gate is opened either way.
    wait_for(Duration::from_secs(10), || started_tasks(&peer) >= N).await;
    let started_before_release = started_tasks(&peer);
    let runs_before_release = runs.load(Ordering::SeqCst);
    release.send_replace(true);
    wait_for(Duration::from_secs(10), || {
        runs.load(Ordering::SeqCst) >= N && completed_tasks(&peer) >= N
    })
    .await;
    let events = peer.step_events.lock().unwrap().clone();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle.shutdown()).await;
    server.abort();

    assert_eq!(
        started_before_release, N,
        "only {started_before_release} of {N} STARTED requests reached the engine before the \
         first STARTED reply: the listen loop waited on a STARTED round trip"
    );
    assert_eq!(
        runs_before_release, 0,
        "a handler ran before its STARTED was acknowledged"
    );
    assert_eq!(
        runs.load(Ordering::SeqCst),
        N,
        "every assignment ran its handler"
    );
    for i in 0..N {
        let id = format!("task-run-{i}");
        let kinds = events
            .iter()
            .filter(|(task, _)| *task == id)
            .map(|(_, kind)| *kind)
            .collect::<Vec<_>>();
        assert_eq!(
            kinds,
            [
                Ev::StepEventTypeStarted as i32,
                Ev::StepEventTypeCompleted as i32
            ],
            "{id} must report STARTED once, before COMPLETED"
        );
    }
}

// A STARTED that still fails after its retries must not run the handler, and it
// must still drop the connection so the worker re-registers, as it did when
// STARTED was sent inline.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_started_acknowledgement_skips_handler_and_reconnects() {
    let (addr, peer, server) = dispatcher(Script {
        assignments: assignments(1),
        started_gate: None,
        started_status: 14, // UNAVAILABLE
    })
    .await;
    let client = client(addr).await;
    let task = client.task::<Value, Value>("quick");
    let runs = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&runs);
    let handle = client
        .worker("failing-start-worker")
        .register_task(&task, move |_input: Value, _ctx| {
            let counted = Arc::clone(&counted);
            async move {
                counted.fetch_add(1, Ordering::SeqCst);
                Ok(Value::Null)
            }
        })
        .build()
        .start()
        .unwrap();

    wait_for(Duration::from_secs(10), || {
        peer.registrations.load(Ordering::SeqCst) >= 2
    })
    .await;
    let registrations = peer.registrations.load(Ordering::SeqCst);
    let events = peer.step_events.lock().unwrap().clone();
    let _ = tokio::time::timeout(Duration::from_secs(5), handle.shutdown()).await;
    server.abort();

    assert!(
        registrations >= 2,
        "a failed STARTED must drop the connection and re-register"
    );
    assert_eq!(
        runs.load(Ordering::SeqCst),
        0,
        "handler ran without STARTED"
    );
    assert!(
        events
            .iter()
            .all(|(_, kind)| *kind == Ev::StepEventTypeStarted as i32),
        "only STARTED may be reported for an unstarted task: {events:?}"
    );
}
