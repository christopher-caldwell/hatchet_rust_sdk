//! DurableListenerClient — bidirectional gRPC stream client for durable task operations.
//!
//! Maintains a bidi stream (`V1Dispatcher::DurableTask`) with the engine for
//! `waitFor`, `memo`, `triggerRuns`, and `completeMemo` operations.
//!
//! Mirrors the TS `DurableListenerClient` from `durable-listener-client.ts`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, broadcast, mpsc, oneshot};
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;

use crate::config::HatchetConfig;
use crate::error::{Error, Result};
use crate::proto::v1;

/// Key for pending acknowledgment lookups.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct AckKey {
    task_external_id: String,
    invocation_count: i32,
}

/// Key for pending callback (entry completed) lookups.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct CallbackKey {
    task_external_id: String,
    invocation_count: i32,
    branch_id: i64,
    node_id: i64,
}

/// Response from a `waitFor` ack.
#[derive(Debug, Clone)]
pub struct WaitForAckResult {
    pub branch_id: i64,
    pub node_id: i64,
}

/// Response from a `triggerRuns` ack.
#[derive(Debug, Clone)]
pub struct TriggerRunsAckResult {
    pub run_entries: Vec<RunAckEntry>,
}

/// A single entry from a trigger runs ack.
#[derive(Debug, Clone)]
pub struct RunAckEntry {
    pub node_id: i64,
    pub branch_id: i64,
    pub workflow_run_external_id: String,
}

/// Response from a `memo` ack.
#[derive(Debug, Clone)]
pub struct MemoAckResult {
    pub already_existed: bool,
    pub cached_payload: Option<Vec<u8>>,
    pub branch_id: i64,
    pub node_id: i64,
}

/// Response from an entry completion callback.
#[derive(Debug, Clone)]
pub struct EntryCompletedResult {
    pub payload: Vec<u8>,
}

/// Internal message types for the ack resolution.
#[derive(Debug)]
enum AckResponse {
    WaitFor(WaitForAckResult),
    TriggerRuns(TriggerRunsAckResult),
    Memo(MemoAckResult),
}

type PendingAcks = Arc<Mutex<HashMap<AckKey, oneshot::Sender<Result<AckResponse>>>>>;
type PendingCallbacks =
    Arc<Mutex<HashMap<CallbackKey, oneshot::Sender<Result<EntryCompletedResult>>>>>;
type InflightRequests = Arc<Mutex<HashMap<AckKey, v1::DurableTaskRequest>>>;

/// Eviction notice: (task_external_id, invocation_count).
#[derive(Debug, Clone)]
pub struct EvictionNotice {
    pub task_external_id: String,
    pub invocation_count: i32,
    pub reason: String,
}

/// The durable listener client manages the bidi stream lifecycle with reconnection.
#[derive(Debug)]
pub struct DurableListenerClient {
    request_tx: Arc<Mutex<Option<mpsc::Sender<v1::DurableTaskRequest>>>>,
    pending_acks: PendingAcks,
    pending_callbacks: PendingCallbacks,
    inflight_requests: InflightRequests,
    evict_tx: broadcast::Sender<EvictionNotice>,
    _reconnect_task: tokio::task::JoinHandle<()>,
}

impl DurableListenerClient {
    /// Opens the durable task bidi stream and starts the reconnecting receive loop.
    ///
    /// # Errors
    ///
    /// Returns an error if the initial stream cannot be opened.
    pub async fn start(config: HatchetConfig, worker_id: String) -> Result<Self> {
        let pending_acks: PendingAcks = Arc::new(Mutex::new(HashMap::new()));
        let pending_callbacks: PendingCallbacks = Arc::new(Mutex::new(HashMap::new()));
        let inflight_requests: InflightRequests = Arc::new(Mutex::new(HashMap::new()));
        let request_tx: Arc<Mutex<Option<mpsc::Sender<v1::DurableTaskRequest>>>> =
            Arc::new(Mutex::new(None));
        let (evict_tx, _) = broadcast::channel::<EvictionNotice>(64);

        let (tx, inbound) = open_durable_stream(&config, &worker_id).await?;
        *request_tx.lock().await = Some(tx);

        let reconnect_task = tokio::spawn({
            let config = config.clone();
            let worker_id = worker_id.clone();
            let request_tx = Arc::clone(&request_tx);
            let pending_acks = Arc::clone(&pending_acks);
            let pending_callbacks = Arc::clone(&pending_callbacks);
            let inflight_requests = Arc::clone(&inflight_requests);
            let evict_tx = evict_tx.clone();
            async move {
                reconnect_loop(
                    config,
                    worker_id,
                    request_tx,
                    pending_acks,
                    pending_callbacks,
                    inflight_requests,
                    evict_tx,
                    inbound,
                )
                .await;
            }
        });

        Ok(Self {
            request_tx,
            pending_acks,
            pending_callbacks,
            inflight_requests,
            evict_tx,
            _reconnect_task: reconnect_task,
        })
    }

    /// Sends a `waitFor` request and returns the ack with branch/node IDs.
    ///
    /// # Errors
    ///
    /// Returns an error if the ack is not received or the stream fails.
    pub async fn send_wait_for(
        &self,
        task_id: &str,
        invocation_count: i32,
        conditions: v1::DurableEventListenerConditions,
    ) -> Result<WaitForAckResult> {
        let key = AckKey {
            task_external_id: task_id.to_owned(),
            invocation_count,
        };

        let request = v1::DurableTaskRequest {
            message: Some(v1::durable_task_request::Message::WaitFor(
                v1::DurableTaskWaitForRequest {
                    invocation_count,
                    durable_task_external_id: task_id.to_owned(),
                    wait_for_conditions: Some(conditions),
                },
            )),
        };

        let (tx, rx) = oneshot::channel();
        self.pending_acks.lock().await.insert(key.clone(), tx);
        self.inflight_requests
            .lock()
            .await
            .insert(key, request.clone());

        self.send_request(request).await?;

        let response = rx
            .await
            .map_err(|_| Error::Durable("ack channel closed".to_owned()))??;

        match response {
            AckResponse::WaitFor(result) => Ok(result),
            _ => Err(Error::Durable("unexpected ack type".to_owned())),
        }
    }

    /// Sends a `triggerRuns` request and returns the child run ack.
    ///
    /// # Errors
    ///
    /// Returns an error if the ack is not received or the stream fails.
    pub async fn send_trigger_runs(
        &self,
        task_id: &str,
        invocation_count: i32,
        trigger_opts: Vec<v1::TriggerWorkflowRequest>,
    ) -> Result<TriggerRunsAckResult> {
        let key = AckKey {
            task_external_id: task_id.to_owned(),
            invocation_count,
        };

        let request = v1::DurableTaskRequest {
            message: Some(v1::durable_task_request::Message::TriggerRuns(
                v1::DurableTaskTriggerRunsRequest {
                    invocation_count,
                    durable_task_external_id: task_id.to_owned(),
                    trigger_opts,
                },
            )),
        };

        let (tx, rx) = oneshot::channel();
        self.pending_acks.lock().await.insert(key.clone(), tx);
        self.inflight_requests
            .lock()
            .await
            .insert(key, request.clone());

        self.send_request(request).await?;

        let response = rx
            .await
            .map_err(|_| Error::Durable("ack channel closed".to_owned()))??;

        match response {
            AckResponse::TriggerRuns(result) => Ok(result),
            _ => Err(Error::Durable("unexpected ack type".to_owned())),
        }
    }

    /// Sends a `memo` request and returns the memo ack.
    ///
    /// # Errors
    ///
    /// Returns an error if the ack is not received or the stream fails.
    pub async fn send_memo(
        &self,
        task_id: &str,
        invocation_count: i32,
        key: Vec<u8>,
        payload: Option<Vec<u8>>,
    ) -> Result<MemoAckResult> {
        let ack_key = AckKey {
            task_external_id: task_id.to_owned(),
            invocation_count,
        };

        let request = v1::DurableTaskRequest {
            message: Some(v1::durable_task_request::Message::Memo(
                v1::DurableTaskMemoRequest {
                    invocation_count,
                    durable_task_external_id: task_id.to_owned(),
                    key,
                    payload,
                },
            )),
        };

        let (tx, rx) = oneshot::channel();
        self.pending_acks.lock().await.insert(ack_key.clone(), tx);
        self.inflight_requests
            .lock()
            .await
            .insert(ack_key, request.clone());

        self.send_request(request).await?;

        let response = rx
            .await
            .map_err(|_| Error::Durable("ack channel closed".to_owned()))??;

        match response {
            AckResponse::Memo(result) => Ok(result),
            _ => Err(Error::Durable("unexpected ack type".to_owned())),
        }
    }

    /// Sends a `completeMemo` request (no ack expected, fire-and-forget).
    ///
    /// # Errors
    ///
    /// Returns an error if the request cannot be sent.
    pub async fn send_complete_memo(
        &self,
        entry_ref: v1::DurableEventLogEntryRef,
        payload: Vec<u8>,
        memo_key: Vec<u8>,
    ) -> Result<()> {
        self.send_request(v1::DurableTaskRequest {
            message: Some(v1::durable_task_request::Message::CompleteMemo(
                v1::DurableTaskCompleteMemoRequest {
                    r#ref: Some(entry_ref),
                    payload,
                    memo_key,
                },
            )),
        })
        .await
    }

    /// Waits for an entry completion callback (sleep resolved, event matched, child completed).
    ///
    /// # Errors
    ///
    /// Returns an error if the callback channel is closed.
    pub async fn wait_for_callback(
        &self,
        task_id: &str,
        invocation_count: i32,
        branch_id: i64,
        node_id: i64,
    ) -> Result<EntryCompletedResult> {
        let key = CallbackKey {
            task_external_id: task_id.to_owned(),
            invocation_count,
            branch_id,
            node_id,
        };

        let (tx, rx) = oneshot::channel();
        self.pending_callbacks.lock().await.insert(key, tx);

        rx.await
            .map_err(|_| Error::Durable("callback channel closed".to_owned()))?
    }

    /// Sends a `evictInvocation` request.
    ///
    /// # Errors
    ///
    /// Returns an error if the request cannot be sent.
    pub async fn send_evict_invocation(
        &self,
        task_id: &str,
        invocation_count: i32,
        reason: Option<String>,
    ) -> Result<()> {
        self.send_request(v1::DurableTaskRequest {
            message: Some(v1::durable_task_request::Message::EvictInvocation(
                v1::DurableTaskEvictInvocationRequest {
                    invocation_count,
                    durable_task_external_id: task_id.to_owned(),
                    reason,
                },
            )),
        })
        .await
    }

    /// Subscribes to server eviction notices.
    pub fn subscribe_evictions(&self) -> broadcast::Receiver<EvictionNotice> {
        self.evict_tx.subscribe()
    }

    async fn send_request(&self, request: v1::DurableTaskRequest) -> Result<()> {
        let guard = self.request_tx.lock().await;
        match &*guard {
            Some(tx) => tx
                .send(request)
                .await
                .map_err(|error| Error::Durable(format!("request channel closed: {error}"))),
            None => Err(Error::Durable("durable stream not connected".to_owned())),
        }
    }
}

/// Opens a new durable task bidi stream and returns the sender and inbound stream.
async fn open_durable_stream(
    config: &HatchetConfig,
    worker_id: &str,
) -> Result<(
    mpsc::Sender<v1::DurableTaskRequest>,
    tonic::Streaming<v1::DurableTaskResponse>,
)> {
    let endpoint_url = config.endpoint_url();
    let channel = Channel::from_shared(endpoint_url.clone())
        .map_err(|error| Error::Transport(format!("invalid endpoint: {error}")))?
        .connect()
        .await
        .map_err(|error| Error::Transport(format!("connect failed: {error}")))?;

    let mut client = v1::v1_dispatcher_client::V1DispatcherClient::new(channel);
    let (tx, rx) = mpsc::channel::<v1::DurableTaskRequest>(256);

    let register_msg = v1::DurableTaskRequest {
        message: Some(v1::durable_task_request::Message::RegisterWorker(
            v1::DurableTaskRequestRegisterWorker {
                worker_id: worker_id.to_owned(),
            },
        )),
    };

    let token = config.token.clone();
    let outbound = async_stream::stream! {
        yield register_msg;
        let mut rx = rx;
        while let Some(msg) = rx.recv().await {
            yield msg;
        }
    };

    let mut request = tonic::Request::new(outbound);
    if let Ok(value) = format!("Bearer {token}").parse::<MetadataValue<tonic::metadata::Ascii>>() {
        request.metadata_mut().insert("authorization", value);
    }

    let response = client
        .durable_task(request)
        .await
        .map_err(|error| Error::Transport(format!("durable task stream failed: {error}")))?;

    let mut inbound = response.into_inner();

    // Wait for register ack
    match inbound.message().await {
        Ok(Some(msg)) => match msg.message {
            Some(v1::durable_task_response::Message::RegisterWorker(_)) => {}
            _ => {
                return Err(Error::Transport(
                    "expected register_worker ack as first response".to_owned(),
                ));
            }
        },
        Ok(None) => {
            return Err(Error::Transport(
                "durable stream closed before register ack".to_owned(),
            ));
        }
        Err(error) => {
            return Err(Error::Transport(format!(
                "durable stream register ack failed: {error}"
            )));
        }
    }

    Ok((tx, inbound))
}

/// Reconnecting receive loop — processes messages and reconnects on stream failure.
#[allow(clippy::too_many_arguments)]
async fn reconnect_loop(
    config: HatchetConfig,
    worker_id: String,
    request_tx: Arc<Mutex<Option<mpsc::Sender<v1::DurableTaskRequest>>>>,
    pending_acks: PendingAcks,
    pending_callbacks: PendingCallbacks,
    inflight_requests: InflightRequests,
    evict_tx: broadcast::Sender<EvictionNotice>,
    initial_inbound: tonic::Streaming<v1::DurableTaskResponse>,
) {
    receive_loop(
        initial_inbound,
        &pending_acks,
        &pending_callbacks,
        &inflight_requests,
        &evict_tx,
    )
    .await;

    *request_tx.lock().await = None;

    let mut backoff = Duration::from_secs(1);
    loop {
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(60));

        match open_durable_stream(&config, &worker_id).await {
            Ok((tx, inbound)) => {
                backoff = Duration::from_secs(1);
                *request_tx.lock().await = Some(tx.clone());

                // Re-send all in-flight requests on the new stream
                let pending = inflight_requests.lock().await.clone();
                for request in pending.into_values() {
                    if tx.send(request).await.is_err() {
                        break;
                    }
                }

                receive_loop(
                    inbound,
                    &pending_acks,
                    &pending_callbacks,
                    &inflight_requests,
                    &evict_tx,
                )
                .await;

                *request_tx.lock().await = None;
            }
            Err(error) => {
                eprintln!("durable stream reconnect failed: {error}");
            }
        }
    }
}

async fn receive_loop(
    mut stream: tonic::Streaming<v1::DurableTaskResponse>,
    pending_acks: &PendingAcks,
    pending_callbacks: &PendingCallbacks,
    inflight_requests: &InflightRequests,
    evict_tx: &broadcast::Sender<EvictionNotice>,
) {
    loop {
        match stream.message().await {
            Ok(Some(response)) => {
                dispatch_response(
                    response,
                    pending_acks,
                    pending_callbacks,
                    inflight_requests,
                    evict_tx,
                )
                .await;
            }
            Ok(None) => break,
            Err(error) => {
                eprintln!("durable stream error: {error}");
                break;
            }
        }
    }
}

async fn dispatch_response(
    response: v1::DurableTaskResponse,
    pending_acks: &PendingAcks,
    pending_callbacks: &PendingCallbacks,
    inflight_requests: &InflightRequests,
    evict_tx: &broadcast::Sender<EvictionNotice>,
) {
    let Some(message) = response.message else {
        return;
    };

    match message {
        v1::durable_task_response::Message::WaitForAck(ack) => {
            if let Some(entry_ref) = ack.r#ref {
                let key = AckKey {
                    task_external_id: entry_ref.durable_task_external_id.clone(),
                    invocation_count: entry_ref.invocation_count,
                };
                inflight_requests.lock().await.remove(&key);
                if let Some(tx) = pending_acks.lock().await.remove(&key) {
                    let _ = tx.send(Ok(AckResponse::WaitFor(WaitForAckResult {
                        branch_id: entry_ref.branch_id,
                        node_id: entry_ref.node_id,
                    })));
                }
            }
        }
        v1::durable_task_response::Message::TriggerRunsAck(ack) => {
            let key = AckKey {
                task_external_id: ack.durable_task_external_id.clone(),
                invocation_count: ack.invocation_count,
            };
            inflight_requests.lock().await.remove(&key);
            if let Some(tx) = pending_acks.lock().await.remove(&key) {
                let entries = ack
                    .run_entries
                    .into_iter()
                    .map(|e| RunAckEntry {
                        node_id: e.node_id,
                        branch_id: e.branch_id,
                        workflow_run_external_id: e.workflow_run_external_id,
                    })
                    .collect();
                let _ = tx.send(Ok(AckResponse::TriggerRuns(TriggerRunsAckResult {
                    run_entries: entries,
                })));
            }
        }
        v1::durable_task_response::Message::MemoAck(ack) => {
            if let Some(entry_ref) = ack.r#ref {
                let key = AckKey {
                    task_external_id: entry_ref.durable_task_external_id.clone(),
                    invocation_count: entry_ref.invocation_count,
                };
                inflight_requests.lock().await.remove(&key);
                if let Some(tx) = pending_acks.lock().await.remove(&key) {
                    let _ = tx.send(Ok(AckResponse::Memo(MemoAckResult {
                        already_existed: ack.memo_already_existed,
                        cached_payload: ack.memo_result_payload,
                        branch_id: entry_ref.branch_id,
                        node_id: entry_ref.node_id,
                    })));
                }
            }
        }
        v1::durable_task_response::Message::EntryCompleted(completed) => {
            if let Some(entry_ref) = completed.r#ref {
                let key = CallbackKey {
                    task_external_id: entry_ref.durable_task_external_id.clone(),
                    invocation_count: entry_ref.invocation_count,
                    branch_id: entry_ref.branch_id,
                    node_id: entry_ref.node_id,
                };
                if let Some(tx) = pending_callbacks.lock().await.remove(&key) {
                    let _ = tx.send(Ok(EntryCompletedResult {
                        payload: completed.payload,
                    }));
                }
            }
        }
        v1::durable_task_response::Message::Error(error) => {
            if let Some(entry_ref) = error.r#ref {
                let key = AckKey {
                    task_external_id: entry_ref.durable_task_external_id.clone(),
                    invocation_count: entry_ref.invocation_count,
                };
                inflight_requests.lock().await.remove(&key);
                if let Some(tx) = pending_acks.lock().await.remove(&key) {
                    let _ = tx.send(Err(Error::Durable(format!(
                        "durable error (type {:?}): {}",
                        error.error_type, error.error_message
                    ))));
                }
            }
        }
        v1::durable_task_response::Message::EvictionAck(_) => {
            // Eviction acks are handled by the eviction manager (P1.3)
        }
        v1::durable_task_response::Message::ServerEvict(notice) => {
            let _ = evict_tx.send(EvictionNotice {
                task_external_id: notice.durable_task_external_id,
                invocation_count: notice.invocation_count,
                reason: notice.reason,
            });
        }
        v1::durable_task_response::Message::RegisterWorker(_) => {
            // Unexpected — registration is handled during startup
        }
    }
}
