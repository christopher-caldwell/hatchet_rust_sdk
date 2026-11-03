//! Actual loopback HTTP/2 faults. No credentials, providers or remote engines.
use another_hatchet_sdk::proto::dispatcher::{HeartbeatResponse, WorkerRegisterResponse};
use another_hatchet_sdk::{GrpcTransport, HatchetConfig, RetryConfig, TlsConfig, Transport};
use another_hatchet_sdk::{WorkerConnectionState, WorkerStartOpts};
use prost::Message;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicU8, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Notify, mpsc, watch};
use tokio_util::sync::CancellationToken;
use tonic::codegen::http;

fn frame(value: impl Message) -> Vec<u8> {
    let body = value.encode_to_vec();
    let mut bytes = vec![0];
    bytes.extend_from_slice(&(body.len() as u32).to_be_bytes());
    bytes.extend(body);
    bytes
}

struct Peer {
    transport: Arc<GrpcTransport>,
    mode: Arc<AtomicU8>, // 1: no headers, 2: headers without body, 3: blackhole entire connection
    heartbeats: mpsc::UnboundedReceiver<()>,
    close_stream: Arc<Notify>,
    registrations: Arc<AtomicUsize>,
    connections: Arc<AtomicUsize>,
    stop: CancellationToken,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.stop.cancel();
        self.task.abort();
    }
}
async fn peer() -> Peer {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let mode = Arc::new(AtomicU8::new(0));
    let close_stream = Arc::new(Notify::new());
    let registrations = Arc::new(AtomicUsize::new(0));
    let connections = Arc::new(AtomicUsize::new(0));
    let stop = CancellationToken::new();
    let (hb_tx, heartbeats) = mpsc::unbounded_channel();
    let task = tokio::spawn({
        let mode = mode.clone();
        let close_stream = close_stream.clone();
        let registrations = registrations.clone();
        let connections = connections.clone();
        let stop = stop.clone();
        async move {
            let mut tasks = tokio::task::JoinSet::new();
            loop {
                let socket = tokio::select! {
                    _ = stop.cancelled() => break,
                    accepted = listener.accept() => accepted.unwrap().0,
                };
                let connection_index = connections.fetch_add(1, Ordering::SeqCst);
                let mode = mode.clone();
                let close_stream = close_stream.clone();
                let registrations = registrations.clone();
                let hb_tx = hb_tx.clone();
                let stop = stop.clone();
                tasks.spawn(async move {
                    let mut connection = h2::server::handshake(socket).await.unwrap();
                    let freeze = Arc::new(Notify::new());
                    let mut handlers = tokio::task::JoinSet::new();
                    loop {
                        let incoming = tokio::select! {
                            _ = stop.cancelled() => return,
                            _ = freeze.notified() => {
                                // Keep the TCP socket open but stop reading/writing all frames,
                                // including PING acknowledgements. New connections stay healthy.
                                stop.cancelled().await;
                                return;
                            }
                            incoming = connection.accept() => incoming,
                        };
                        let Some(Ok((request, mut respond))) = incoming else {
                            return;
                        };
                        let method = request.uri().path().rsplit('/').next().unwrap().to_owned();
                        assert_eq!(
                            request.headers().contains_key("grpc-timeout"),
                            method != "ListenV2",
                            "only unary calls may carry an RPC deadline"
                        );
                        let mode = mode.clone();
                        let close_stream = close_stream.clone();
                        let stop = stop.clone();
                        let registrations = registrations.clone();
                        let hb_tx = hb_tx.clone();
                        let freeze = freeze.clone();
                        handlers.spawn(async move {
                            let mut body = request.into_body();
                            while let Some(Ok(data)) = body.data().await {
                                body.flow_control().release_capacity(data.len()).unwrap();
                            }
                            let fault = if connection_index == 0 {
                                mode.load(Ordering::SeqCst)
                            } else {
                                0
                            };
                            if method == "Heartbeat" {
                                hb_tx.send(()).unwrap();
                                if fault == 3 {
                                    freeze.notify_one();
                                }
                                if fault == 1 || fault == 3 {
                                    stop.cancelled().await;
                                    return;
                                }
                            }
                            let headers = http::Response::builder()
                                .header("content-type", "application/grpc")
                                .body(())
                                .unwrap();
                            let mut stream = respond.send_response(headers, false).unwrap();
                            let payload = match method.as_str() {
                                "Register" => {
                                    let id = registrations.fetch_add(1, Ordering::SeqCst) + 1;
                                    if fault == 4 {
                                        stop.cancelled().await;
                                        return;
                                    }
                                    frame(WorkerRegisterResponse {
                                        worker_id: format!("fixture-{id}"),
                                        ..Default::default()
                                    })
                                }
                                "Heartbeat" => {
                                    if fault == 2 {
                                        stop.cancelled().await;
                                        return;
                                    }
                                    frame(HeartbeatResponse {})
                                }
                                "ListenV2" => {
                                    close_stream.notified().await;
                                    let mut trailers = http::HeaderMap::new();
                                    trailers.insert("grpc-status", "14".parse().unwrap());
                                    let _ = stream.send_trailers(trailers);
                                    return;
                                }
                                other => panic!("unexpected method {other}"),
                            };
                            if stream.send_data(payload.into(), false).is_err() {
                                return;
                            }
                            let mut trailers = http::HeaderMap::new();
                            trailers.insert("grpc-status", "0".parse().unwrap());
                            let _ = stream.send_trailers(trailers);
                        });
                    }
                });
            }
        }
    });
    let config = HatchetConfig::builder()
        .host_port(address.to_string())
        .server_url("http://127.0.0.1:1")
        .tenant_id("synthetic")
        .token("synthetic-unused")
        .tls(TlsConfig::Insecure)
        .build()
        .unwrap();
    let transport = Arc::new(
        GrpcTransport::from_config(config)
            .await
            .unwrap()
            .with_retry_config(RetryConfig {
                max_retries: 0,
                ..Default::default()
            }),
    );
    Peer {
        transport,
        mode,
        heartbeats,
        close_stream,
        registrations,
        connections,
        stop,
        task,
    }
}
fn start(
    peer: &Peer,
) -> (
    CancellationToken,
    watch::Receiver<WorkerConnectionState>,
    tokio::task::JoinHandle<another_hatchet_sdk::Result<()>>,
) {
    let cancel = CancellationToken::new();
    let (connection_tx, connection_rx) = watch::channel(WorkerConnectionState::Starting);
    let opts = WorkerStartOpts {
        worker_name: "fixture".into(),
        labels: HashMap::new(),
        slot_config: HashMap::new(),
        task_dispatchers: HashMap::new(),
        durable_task_dispatchers: HashMap::new(),
        cancel_token: cancel.clone(),
        connection_tx,
    };
    let transport = peer.transport.clone();
    (
        cancel,
        connection_rx,
        tokio::spawn(async move { transport.start_worker_loop(opts).await }),
    )
}
async fn state(rx: &mut watch::Receiver<WorkerConnectionState>, ready: bool) {
    tokio::time::timeout(Duration::from_secs(55), async {
        loop {
            if if ready {
                matches!(*rx.borrow(), WorkerConnectionState::Ready)
            } else {
                matches!(*rx.borrow(), WorkerConnectionState::Reconnecting(_))
            } {
                return;
            }
            rx.changed().await.unwrap();
        }
    })
    .await
    .expect("expected worker connection transition");
}
#[tokio::test]
async fn stalled_heartbeat_does_not_mask_stream_failure_or_cancellation() {
    let mut peer = peer().await;
    let (cancel, mut rx, task) = start(&peer);
    state(&mut rx, true).await;
    peer.heartbeats.recv().await.unwrap();
    peer.mode.store(1, Ordering::SeqCst);
    peer.heartbeats.recv().await.unwrap();
    peer.close_stream.notify_one();
    tokio::time::timeout(Duration::from_secs(1), state(&mut rx, false))
        .await
        .expect("heartbeat masked stream failure");
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn cancellation_interrupts_a_pending_startup_heartbeat() {
    let mut peer = peer().await;
    peer.mode.store(1, Ordering::SeqCst);
    let (cancel, _, task) = start(&peer);
    peer.heartbeats.recv().await.unwrap();
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("shutdown blocked by heartbeat")
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn unary_deadline_bounds_a_response_body_after_headers() {
    let mut peer = peer().await;
    peer.transport.set_worker_id("fixture".into()).await;
    peer.mode.store(2, Ordering::SeqCst);
    let call = tokio::spawn({
        let transport = peer.transport.clone();
        async move { transport.heartbeat().await }
    });
    peer.heartbeats.recv().await.unwrap();
    let error = tokio::time::timeout(Duration::from_secs(12), call)
        .await
        .expect("unary body has no deadline")
        .unwrap()
        .unwrap_err();
    assert!(error.to_string().contains("deadline"), "{error}");
}
#[tokio::test]
async fn half_open_connection_is_replaced_and_idle_worker_recovers() {
    let mut peer = peer().await;
    let (cancel, mut rx, task) = start(&peer);
    state(&mut rx, true).await;
    peer.heartbeats.recv().await.unwrap();
    peer.mode.store(3, Ordering::SeqCst);
    peer.heartbeats.recv().await.unwrap();
    state(&mut rx, false).await;
    state(&mut rx, true).await;
    assert!(
        peer.connections.load(Ordering::SeqCst) >= 2,
        "must replace the blackholed TCP connection"
    );
    assert!(peer.registrations.load(Ordering::SeqCst) >= 2);
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn healthy_idle_listener_has_no_whole_stream_deadline() {
    let mut peer = peer().await;
    let (cancel, mut rx, task) = start(&peer);
    state(&mut rx, true).await;
    // More than a unary attempt's budget, without a single task assignment.
    tokio::time::sleep(Duration::from_secs(12)).await;
    assert_eq!(*rx.borrow_and_update(), WorkerConnectionState::Ready);
    assert_eq!(peer.registrations.load(Ordering::SeqCst), 1);
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    // Retain the receiver until peer handlers have stopped sending.
    peer.heartbeats.close();
}

#[tokio::test]
async fn cancellation_interrupts_pending_registration() {
    let peer = peer().await;
    peer.mode.store(4, Ordering::SeqCst);
    let (cancel, _, task) = start(&peer);
    tokio::time::timeout(Duration::from_secs(2), async {
        while peer.registrations.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    cancel.cancel();
    tokio::time::timeout(Duration::from_secs(1), task)
        .await
        .expect("shutdown blocked by registration")
        .unwrap()
        .unwrap();
}
