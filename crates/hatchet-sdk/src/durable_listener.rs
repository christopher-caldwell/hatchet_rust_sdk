//! DurableListenerClient — bidirectional gRPC stream client for durable task operations.
//!
//! Maintains a bidi stream (`V1Dispatcher::DurableTask`) with the engine for
//! `waitFor`, `memo`, `triggerRuns`, and `completeMemo` operations.
//!
//! Mirrors the TS `DurableListenerClient` from `durable-listener-client.ts`.

use std::collections::HashMap;
use std::sync::{Arc, Weak};
use std::time::Duration;

use tokio::sync::{Mutex, broadcast, mpsc, oneshot};
use tonic::metadata::MetadataValue;

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

#[derive(Debug)]
struct PendingAck {
    sender: oneshot::Sender<Result<AckResponse>>,
    // Keep the invocation serialized until the engine acknowledges the request,
    // even when its caller has stopped waiting for the response.
    _guard: tokio::sync::OwnedMutexGuard<()>,
}

type PendingAcks = Arc<Mutex<HashMap<AckKey, PendingAck>>>;
type AckLocks = std::sync::Mutex<HashMap<AckKey, Weak<Mutex<()>>>>;

#[derive(Debug, Default)]
struct CallbackRegistry {
    pending: HashMap<CallbackKey, oneshot::Sender<Result<EntryCompletedResult>>>,
    completed: HashMap<CallbackKey, EntryCompletedResult>,
}

type Callbacks = Arc<Mutex<CallbackRegistry>>;
type InflightRequests = Arc<Mutex<HashMap<AckKey, v1::DurableTaskRequest>>>;
const MAX_BUFFERED_CALLBACKS: usize = 1_024;

/// Eviction notice: (task_external_id, invocation_count).
#[derive(Debug, Clone)]
pub struct EvictionNotice {
    pub task_external_id: String,
    pub invocation_count: i32,
    pub reason: String,
    /// Server requests must be acknowledged; acknowledged local evictions must not.
    pub requires_acknowledgment: bool,
}

/// The durable listener client manages the bidi stream lifecycle with reconnection.
#[derive(Debug)]
pub struct DurableListenerClient {
    worker_id: String,
    request_tx: Arc<Mutex<Option<mpsc::Sender<v1::DurableTaskRequest>>>>,
    pending_acks: PendingAcks,
    ack_locks: AckLocks,
    callbacks: Callbacks,
    inflight_requests: InflightRequests,
    evict_tx: broadcast::Sender<EvictionNotice>,
    _reconnect_task: tokio::task::JoinHandle<()>,
    _status_task: tokio::task::JoinHandle<()>,
}

impl Drop for DurableListenerClient {
    fn drop(&mut self) {
        self._reconnect_task.abort();
        self._status_task.abort();
    }
}

impl DurableListenerClient {
    /// Opens the durable task bidi stream and starts the reconnecting receive loop.
    ///
    /// # Errors
    ///
    /// Returns an error if the initial stream cannot be opened.
    pub async fn start(config: HatchetConfig, worker_id: String) -> Result<Self> {
        let pending_acks: PendingAcks = Arc::new(Mutex::new(HashMap::new()));
        let callbacks: Callbacks = Arc::new(Mutex::new(CallbackRegistry::default()));
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
            let callbacks = Arc::clone(&callbacks);
            let inflight_requests = Arc::clone(&inflight_requests);
            let evict_tx = evict_tx.clone();
            async move {
                reconnect_loop(
                    config,
                    worker_id,
                    request_tx,
                    pending_acks,
                    callbacks,
                    inflight_requests,
                    evict_tx,
                    inbound,
                )
                .await;
            }
        });

        // The engine does not continuously push completed durable entries. Workers report the
        // exact entries they are awaiting so a completion can be delivered (and redelivered after
        // a transient disconnect). This mirrors the official TypeScript SDK's one-second status
        // poll and is required for sleeps, event waits, and durable child results to resume.
        let status_task = tokio::spawn({
            let request_tx = Arc::clone(&request_tx);
            let callbacks = Arc::clone(&callbacks);
            let worker_id = worker_id.clone();
            async move {
                let mut interval = tokio::time::interval(Duration::from_secs(1));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    interval.tick().await;
                    let _ = send_worker_status(&request_tx, &callbacks, &worker_id).await;
                }
            }
        });

        Ok(Self {
            worker_id,
            request_tx,
            pending_acks,
            ack_locks: AckLocks::default(),
            callbacks,
            inflight_requests,
            evict_tx,
            _reconnect_task: reconnect_task,
            _status_task: status_task,
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

        let response = self.send_ack_request(key, request).await?;

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

        let response = self.send_ack_request(key, request).await?;

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

        let response = self.send_ack_request(ack_key, request).await?;

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

        match register_callback(&self.callbacks, key).await {
            CallbackWaiter::Completed(result) => Ok(result),
            CallbackWaiter::Pending(receiver) => {
                // Report immediately instead of waiting for the next periodic status tick.
                // Periodic polling remains active so reconnects cannot strand this callback.
                let _ =
                    send_worker_status(&self.request_tx, &self.callbacks, &self.worker_id).await;
                receiver
                    .await
                    .map_err(|_| Error::Durable("callback channel closed".to_owned()))?
            }
        }
    }

    /// Sends an `evictInvocation` control request without waiting behind normal acks.
    /// Engine acknowledgments are delivered through `subscribe_evictions`.
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
        let request = v1::DurableTaskRequest {
            message: Some(v1::durable_task_request::Message::EvictInvocation(
                v1::DurableTaskEvictInvocationRequest {
                    invocation_count,
                    durable_task_external_id: task_id.to_owned(),
                    reason,
                },
            )),
        };
        self.send_request(request).await
    }

    /// Subscribes to server eviction notices.
    pub fn subscribe_evictions(&self) -> broadcast::Receiver<EvictionNotice> {
        self.evict_tx.subscribe()
    }

    async fn send_ack_request(
        &self,
        key: AckKey,
        request: v1::DurableTaskRequest,
    ) -> Result<AckResponse> {
        // The protocol identifies acks only by task ID and invocation count.
        // Different invocations can proceed independently, but requests within
        // one invocation must not replace each other's acknowledgment sender.
        let lock = {
            let mut locks = self
                .ack_locks
                .lock()
                .map_err(|_| Error::Durable("ack lock registry poisoned".to_owned()))?;
            locks.retain(|_, lock| lock.strong_count() > 0);
            match locks.get(&key).and_then(Weak::upgrade) {
                Some(lock) => lock,
                None => {
                    let lock = Arc::new(Mutex::new(()));
                    locks.insert(key.clone(), Arc::downgrade(&lock));
                    lock
                }
            }
        };
        let guard = lock.lock_owned().await;
        let (sender, receiver) = oneshot::channel();
        let pending_acks = Arc::clone(&self.pending_acks);
        let inflight_requests = Arc::clone(&self.inflight_requests);
        let request_tx = Arc::clone(&self.request_tx);
        // Registering and sending must finish even if the caller is cancelled
        // between these awaits. The pending entry owns the sequencing guard.
        tokio::spawn(async move {
            pending_acks.lock().await.insert(
                key.clone(),
                PendingAck {
                    sender,
                    _guard: guard,
                },
            );
            inflight_requests
                .lock()
                .await
                .insert(key.clone(), request.clone());
            if let Err(error) = send_request(&request_tx, request).await {
                inflight_requests.lock().await.remove(&key);
                if let Some(pending) = pending_acks.lock().await.remove(&key) {
                    let _ = pending.sender.send(Err(error));
                }
            }
        });
        receiver
            .await
            .map_err(|_| Error::Durable("ack channel closed".to_owned()))?
    }

    async fn send_request(&self, request: v1::DurableTaskRequest) -> Result<()> {
        send_request(&self.request_tx, request).await
    }
}

async fn send_request(
    request_tx: &Arc<Mutex<Option<mpsc::Sender<v1::DurableTaskRequest>>>>,
    request: v1::DurableTaskRequest,
) -> Result<()> {
    let sender = request_tx
        .lock()
        .await
        .clone()
        .ok_or_else(|| Error::Durable("durable stream not connected".to_owned()))?;
    sender
        .send(request)
        .await
        .map_err(|error| Error::Durable(format!("request channel closed: {error}")))
}

enum CallbackWaiter {
    Completed(EntryCompletedResult),
    Pending(oneshot::Receiver<Result<EntryCompletedResult>>),
}

async fn register_callback(callbacks: &Callbacks, key: CallbackKey) -> CallbackWaiter {
    let (tx, rx) = oneshot::channel();
    let mut callbacks = callbacks.lock().await;
    if let Some(completed) = callbacks.completed.remove(&key) {
        return CallbackWaiter::Completed(completed);
    }
    callbacks.pending.insert(key, tx);
    CallbackWaiter::Pending(rx)
}

#[cfg(test)]
async fn wait_for_registered_callback(
    callbacks: &Callbacks,
    key: CallbackKey,
) -> Result<EntryCompletedResult> {
    match register_callback(callbacks, key).await {
        CallbackWaiter::Completed(result) => Ok(result),
        CallbackWaiter::Pending(receiver) => receiver
            .await
            .map_err(|_| Error::Durable("callback channel closed".to_owned()))?,
    }
}

async fn send_worker_status(
    request_tx: &Arc<Mutex<Option<mpsc::Sender<v1::DurableTaskRequest>>>>,
    callbacks: &Callbacks,
    worker_id: &str,
) -> Result<()> {
    let waiting_entries = {
        let callbacks = callbacks.lock().await;
        callbacks
            .pending
            .keys()
            .map(|key| v1::DurableTaskAwaitedCompletedEntry {
                durable_task_external_id: key.task_external_id.clone(),
                branch_id: key.branch_id,
                node_id: key.node_id,
                invocation_count: key.invocation_count,
            })
            .collect::<Vec<_>>()
    };

    if waiting_entries.is_empty() {
        return Ok(());
    }

    let sender = request_tx.lock().await.clone().ok_or_else(|| {
        Error::Durable("cannot report durable worker status while disconnected".to_owned())
    })?;
    sender
        .send(v1::DurableTaskRequest {
            message: Some(v1::durable_task_request::Message::WorkerStatus(
                v1::DurableTaskWorkerStatusRequest {
                    worker_id: worker_id.to_owned(),
                    waiting_entries,
                },
            )),
        })
        .await
        .map_err(|error| Error::Durable(format!("worker status channel closed: {error}")))
}

/// Opens a new durable task bidi stream and returns the sender and inbound stream.
async fn open_durable_stream(
    config: &HatchetConfig,
    worker_id: &str,
) -> Result<(
    mpsc::Sender<v1::DurableTaskRequest>,
    tonic::Streaming<v1::DurableTaskResponse>,
)> {
    let channel = config.connect_channel().await?;

    let mut client = v1::v1_dispatcher_client::V1DispatcherClient::new(channel);
    let (tx, rx) = mpsc::channel::<v1::DurableTaskRequest>(256);

    let register_msg = v1::DurableTaskRequest {
        message: Some(v1::durable_task_request::Message::RegisterWorker(
            v1::DurableTaskRequestRegisterWorker {
                worker_id: worker_id.to_owned(),
            },
        )),
    };

    let token = config.token().to_owned();
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
    callbacks: Callbacks,
    inflight_requests: InflightRequests,
    evict_tx: broadcast::Sender<EvictionNotice>,
    initial_inbound: tonic::Streaming<v1::DurableTaskResponse>,
) {
    receive_loop(
        initial_inbound,
        &pending_acks,
        &callbacks,
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
                    &callbacks,
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
    callbacks: &Callbacks,
    inflight_requests: &InflightRequests,
    evict_tx: &broadcast::Sender<EvictionNotice>,
) {
    loop {
        match stream.message().await {
            Ok(Some(response)) => {
                dispatch_response(
                    response,
                    pending_acks,
                    callbacks,
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
    callbacks: &Callbacks,
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
                if let Some(pending) = pending_acks.lock().await.remove(&key) {
                    let _ = pending
                        .sender
                        .send(Ok(AckResponse::WaitFor(WaitForAckResult {
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
            if let Some(pending) = pending_acks.lock().await.remove(&key) {
                let entries = ack
                    .run_entries
                    .into_iter()
                    .map(|e| RunAckEntry {
                        node_id: e.node_id,
                        branch_id: e.branch_id,
                        workflow_run_external_id: e.workflow_run_external_id,
                    })
                    .collect();
                let _ = pending
                    .sender
                    .send(Ok(AckResponse::TriggerRuns(TriggerRunsAckResult {
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
                if let Some(pending) = pending_acks.lock().await.remove(&key) {
                    let _ = pending.sender.send(Ok(AckResponse::Memo(MemoAckResult {
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
                let result = EntryCompletedResult {
                    payload: completed.payload,
                };
                let mut callbacks = callbacks.lock().await;
                if let Some(tx) = callbacks.pending.remove(&key) {
                    let _ = tx.send(Ok(result));
                } else {
                    // The engine may deliver completion immediately after its ack,
                    // before the caller installs its waiter. Buffer it atomically.
                    let oldest = (callbacks.completed.len() >= MAX_BUFFERED_CALLBACKS)
                        .then(|| callbacks.completed.keys().next().cloned())
                        .flatten();
                    if let Some(oldest) = oldest {
                        callbacks.completed.remove(&oldest);
                    }
                    callbacks.completed.insert(key, result);
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
                if let Some(pending) = pending_acks.lock().await.remove(&key) {
                    let _ = pending.sender.send(Err(Error::Durable(format!(
                        "durable error (type {:?}): {}",
                        error.error_type, error.error_message
                    ))));
                }
            }
        }
        v1::durable_task_response::Message::EvictionAck(ack) => {
            let _ = evict_tx.send(EvictionNotice {
                task_external_id: ack.durable_task_external_id,
                invocation_count: ack.invocation_count,
                reason: "eviction acknowledged".to_owned(),
                requires_acknowledgment: false,
            });
        }
        v1::durable_task_response::Message::ServerEvict(notice) => {
            let _ = evict_tx.send(EvictionNotice {
                task_external_id: notice.durable_task_external_id,
                invocation_count: notice.invocation_count,
                reason: notice.reason,
                requires_acknowledgment: true,
            });
        }
        v1::durable_task_response::Message::RegisterWorker(_) => {
            // Unexpected — registration is handled during startup
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_listener() -> (
        Arc<DurableListenerClient>,
        mpsc::Receiver<v1::DurableTaskRequest>,
    ) {
        let (tx, rx) = mpsc::channel(8);
        let (evict_tx, _) = broadcast::channel(1);
        let listener = DurableListenerClient {
            worker_id: "worker-1".to_owned(),
            request_tx: Arc::new(Mutex::new(Some(tx))),
            pending_acks: Arc::new(Mutex::new(HashMap::new())),
            ack_locks: AckLocks::default(),
            callbacks: Arc::new(Mutex::new(CallbackRegistry::default())),
            inflight_requests: Arc::new(Mutex::new(HashMap::new())),
            evict_tx,
            _reconnect_task: tokio::spawn(std::future::pending()),
            _status_task: tokio::spawn(std::future::pending()),
        };
        (Arc::new(listener), rx)
    }

    #[tokio::test]
    async fn eviction_control_bypasses_pending_memo_and_does_not_steal_its_ack() {
        let (listener, mut requests) = test_listener();
        let mut notices = listener.subscribe_evictions();
        let memo = memo_request(&listener, 1);
        let memo_request = requests.recv().await.unwrap();
        tokio::time::timeout(
            Duration::from_secs(1),
            listener.send_evict_invocation("task-1", 1, None),
        )
        .await
        .unwrap()
        .unwrap();
        let eviction_request = requests.recv().await.unwrap();
        assert!(matches!(
            eviction_request.message,
            Some(v1::durable_task_request::Message::EvictInvocation(_))
        ));
        assert!(notices.try_recv().is_err());
        dispatch_response(
            v1::DurableTaskResponse {
                message: Some(v1::durable_task_response::Message::EvictionAck(
                    v1::DurableTaskEvictionAckResponse {
                        durable_task_external_id: "task-1".to_owned(),
                        invocation_count: 1,
                    },
                )),
            },
            &listener.pending_acks,
            &listener.callbacks,
            &listener.inflight_requests,
            &listener.evict_tx,
        )
        .await;
        let notice = notices.recv().await.unwrap();
        assert_eq!(notice.task_external_id, "task-1");
        assert_eq!(notice.invocation_count, 1);
        assert!(!notice.requires_acknowledgment);
        assert!(!memo.is_finished());
        acknowledge_memo(&listener, memo_request).await;
        assert_eq!(memo.await.unwrap().unwrap().cached_payload, Some(vec![1]));
        assert!(listener.pending_acks.lock().await.is_empty());
        assert!(listener.inflight_requests.lock().await.is_empty());
    }

    fn memo_request(
        listener: &Arc<DurableListenerClient>,
        value: u8,
    ) -> tokio::task::JoinHandle<Result<MemoAckResult>> {
        let listener = Arc::clone(listener);
        tokio::spawn(async move {
            listener
                .send_memo("task-1", 1, vec![value], Some(vec![value]))
                .await
        })
    }

    async fn acknowledge_memo(listener: &DurableListenerClient, request: v1::DurableTaskRequest) {
        let Some(v1::durable_task_request::Message::Memo(memo)) = request.message else {
            panic!("expected a memo request");
        };
        dispatch_response(
            v1::DurableTaskResponse {
                message: Some(v1::durable_task_response::Message::MemoAck(
                    v1::DurableTaskEventMemoAckResponse {
                        r#ref: Some(v1::DurableEventLogEntryRef {
                            durable_task_external_id: memo.durable_task_external_id,
                            invocation_count: memo.invocation_count,
                            branch_id: 0,
                            node_id: i64::from(memo.key[0]),
                        }),
                        memo_already_existed: true,
                        memo_result_payload: memo.payload,
                    },
                )),
            },
            &listener.pending_acks,
            &listener.callbacks,
            &listener.inflight_requests,
            &listener.evict_tx,
        )
        .await;
    }

    #[tokio::test]
    async fn overlapping_memos_keep_their_own_acknowledgments() {
        let (listener, mut requests) = test_listener();
        let first = memo_request(&listener, 1);
        let first_request = requests.recv().await.unwrap();
        let second = memo_request(&listener, 2);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), requests.recv())
                .await
                .is_err()
        );
        acknowledge_memo(&listener, first_request).await;
        assert_eq!(first.await.unwrap().unwrap().cached_payload, Some(vec![1]));
        let second_request = requests.recv().await.unwrap();
        acknowledge_memo(&listener, second_request).await;
        assert_eq!(second.await.unwrap().unwrap().cached_payload, Some(vec![2]));
        assert!(listener.pending_acks.lock().await.is_empty());
    }

    #[tokio::test]
    async fn cancelled_memo_waiters_do_not_steal_the_next_acknowledgment() {
        let (listener, mut requests) = test_listener();
        let first = memo_request(&listener, 1);
        let first_request = requests.recv().await.unwrap();
        first.abort();
        assert!(first.await.unwrap_err().is_cancelled());
        let second = memo_request(&listener, 2);
        assert!(
            tokio::time::timeout(Duration::from_millis(10), requests.recv())
                .await
                .is_err()
        );
        acknowledge_memo(&listener, first_request).await;
        let second_request = requests.recv().await.unwrap();
        acknowledge_memo(&listener, second_request).await;
        assert_eq!(second.await.unwrap().unwrap().cached_payload, Some(vec![2]));
        assert!(listener.pending_acks.lock().await.is_empty());
    }

    #[tokio::test]
    async fn completion_is_buffered_until_waiter_registration() {
        let callbacks: Callbacks = Arc::new(Mutex::new(CallbackRegistry::default()));
        let pending_acks: PendingAcks = Arc::new(Mutex::new(HashMap::new()));
        let inflight: InflightRequests = Arc::new(Mutex::new(HashMap::new()));
        let (evict_tx, _) = broadcast::channel(1);
        let entry_ref = v1::DurableEventLogEntryRef {
            durable_task_external_id: "task-1".to_owned(),
            invocation_count: 2,
            branch_id: 3,
            node_id: 4,
        };
        dispatch_response(
            v1::DurableTaskResponse {
                message: Some(v1::durable_task_response::Message::EntryCompleted(
                    v1::DurableTaskEventLogEntryCompletedResponse {
                        r#ref: Some(entry_ref.clone()),
                        payload: b"done".to_vec(),
                    },
                )),
            },
            &pending_acks,
            &callbacks,
            &inflight,
            &evict_tx,
        )
        .await;

        let result = tokio::time::timeout(
            Duration::from_millis(50),
            wait_for_registered_callback(
                &callbacks,
                CallbackKey {
                    task_external_id: entry_ref.durable_task_external_id,
                    invocation_count: entry_ref.invocation_count,
                    branch_id: entry_ref.branch_id,
                    node_id: entry_ref.node_id,
                },
            ),
        )
        .await
        .expect("buffered callback resolves without waiting")
        .expect("callback succeeds");

        assert_eq!(result.payload, b"done");
    }

    #[tokio::test]
    async fn worker_status_reports_every_pending_callback_key() {
        let callbacks: Callbacks = Arc::new(Mutex::new(CallbackRegistry::default()));
        let key = CallbackKey {
            task_external_id: "1ee6b921-8a37-4be1-833c-2987736eec9d".to_owned(),
            invocation_count: 4,
            branch_id: 5,
            node_id: 6,
        };
        let _waiter = register_callback(&callbacks, key.clone()).await;
        let (tx, mut rx) = mpsc::channel(1);
        let request_tx = Arc::new(Mutex::new(Some(tx)));

        send_worker_status(
            &request_tx,
            &callbacks,
            "0d53333a-c7a2-49c6-8942-fd9fc647979c",
        )
        .await
        .expect("worker status should send");

        let request = rx.recv().await.expect("worker status request");
        let Some(v1::durable_task_request::Message::WorkerStatus(status)) = request.message else {
            panic!("expected worker status request");
        };
        assert_eq!(status.worker_id, "0d53333a-c7a2-49c6-8942-fd9fc647979c");
        assert_eq!(status.waiting_entries.len(), 1);
        let waiting = &status.waiting_entries[0];
        assert_eq!(waiting.durable_task_external_id, key.task_external_id);
        assert_eq!(waiting.invocation_count, key.invocation_count);
        assert_eq!(waiting.branch_id, key.branch_id);
        assert_eq!(waiting.node_id, key.node_id);
    }
}
