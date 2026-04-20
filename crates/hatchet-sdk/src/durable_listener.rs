//! DurableListenerClient — bidirectional gRPC stream client for durable task operations.
//!
//! Maintains a bidi stream (`V1Dispatcher::DurableTask`) with the engine for
//! `waitFor`, `memo`, `triggerRuns`, and `completeMemo` operations.
//!
//! Mirrors the TS `DurableListenerClient` from `durable-listener-client.ts`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{Mutex, mpsc, oneshot};
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

/// The durable listener client manages the bidi stream lifecycle.
pub struct DurableListenerClient {
    config: HatchetConfig,
    request_tx: mpsc::Sender<v1::DurableTaskRequest>,
    pending_acks: Arc<Mutex<HashMap<AckKey, oneshot::Sender<Result<AckResponse>>>>>,
    pending_callbacks:
        Arc<Mutex<HashMap<CallbackKey, oneshot::Sender<Result<EntryCompletedResult>>>>>,
    _receive_task: tokio::task::JoinHandle<()>,
}

impl DurableListenerClient {
    /// Opens the durable task bidi stream and starts the receive loop.
    ///
    /// # Errors
    ///
    /// Returns an error if the stream cannot be opened.
    pub async fn start(config: HatchetConfig, worker_id: String) -> Result<Self> {
        let endpoint_url = config.endpoint_url();
        let channel = Channel::from_shared(endpoint_url.clone())
            .map_err(|error| Error::Transport(format!("invalid endpoint: {error}")))?
            .connect()
            .await
            .map_err(|error| Error::Transport(format!("connect failed: {error}")))?;

        let mut client = v1::v1_dispatcher_client::V1DispatcherClient::new(channel);

        let (request_tx, request_rx) = mpsc::channel::<v1::DurableTaskRequest>(256);
        let pending_acks: Arc<Mutex<HashMap<AckKey, oneshot::Sender<Result<AckResponse>>>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let pending_callbacks: Arc<
            Mutex<HashMap<CallbackKey, oneshot::Sender<Result<EntryCompletedResult>>>>,
        > = Arc::new(Mutex::new(HashMap::new()));

        // Send the register_worker message first
        let register_msg = v1::DurableTaskRequest {
            message: Some(v1::durable_task_request::Message::RegisterWorker(
                v1::DurableTaskRequestRegisterWorker {
                    worker_id: worker_id.clone(),
                },
            )),
        };

        // Create the request stream
        let token = config.token.clone();
        let outbound = async_stream::stream! {
            yield register_msg;
            let mut rx = request_rx;
            while let Some(msg) = rx.recv().await {
                yield msg;
            }
        };

        let mut request = tonic::Request::new(outbound);
        if let Ok(value) =
            format!("Bearer {token}").parse::<MetadataValue<tonic::metadata::Ascii>>()
        {
            request.metadata_mut().insert("authorization", value);
        }

        let response = client
            .durable_task(request)
            .await
            .map_err(|error| Error::Transport(format!("durable task stream failed: {error}")))?;

        let mut inbound = response.into_inner();

        // Wait for register ack
        if let Some(msg) = inbound.message().await.map_err(|error| {
            Error::Transport(format!("durable stream register ack failed: {error}"))
        })? {
            match msg.message {
                Some(v1::durable_task_response::Message::RegisterWorker(_)) => {
                    // Registration successful
                }
                _ => {
                    return Err(Error::Transport(
                        "expected register_worker ack as first response".to_owned(),
                    ));
                }
            }
        }

        // Spawn receive loop
        let acks = Arc::clone(&pending_acks);
        let callbacks = Arc::clone(&pending_callbacks);
        let receive_task = tokio::spawn(async move {
            Self::receive_loop(inbound, acks, callbacks).await;
        });

        Ok(Self {
            config,
            request_tx,
            pending_acks,
            pending_callbacks,
            _receive_task: receive_task,
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

        let (tx, rx) = oneshot::channel();
        self.pending_acks.lock().await.insert(key, tx);

        self.request_tx
            .send(v1::DurableTaskRequest {
                message: Some(v1::durable_task_request::Message::WaitFor(
                    v1::DurableTaskWaitForRequest {
                        invocation_count,
                        durable_task_external_id: task_id.to_owned(),
                        wait_for_conditions: Some(conditions),
                    },
                )),
            })
            .await
            .map_err(|error| Error::Durable(format!("request channel closed: {error}")))?;

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

        let (tx, rx) = oneshot::channel();
        self.pending_acks.lock().await.insert(key, tx);

        self.request_tx
            .send(v1::DurableTaskRequest {
                message: Some(v1::durable_task_request::Message::TriggerRuns(
                    v1::DurableTaskTriggerRunsRequest {
                        invocation_count,
                        durable_task_external_id: task_id.to_owned(),
                        trigger_opts,
                    },
                )),
            })
            .await
            .map_err(|error| Error::Durable(format!("request channel closed: {error}")))?;

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

        let (tx, rx) = oneshot::channel();
        self.pending_acks.lock().await.insert(ack_key, tx);

        self.request_tx
            .send(v1::DurableTaskRequest {
                message: Some(v1::durable_task_request::Message::Memo(
                    v1::DurableTaskMemoRequest {
                        invocation_count,
                        durable_task_external_id: task_id.to_owned(),
                        key,
                        payload,
                    },
                )),
            })
            .await
            .map_err(|error| Error::Durable(format!("request channel closed: {error}")))?;

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
        self.request_tx
            .send(v1::DurableTaskRequest {
                message: Some(v1::durable_task_request::Message::CompleteMemo(
                    v1::DurableTaskCompleteMemoRequest {
                        r#ref: Some(entry_ref),
                        payload,
                        memo_key,
                    },
                )),
            })
            .await
            .map_err(|error| Error::Durable(format!("request channel closed: {error}")))?;

        Ok(())
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
        self.request_tx
            .send(v1::DurableTaskRequest {
                message: Some(v1::durable_task_request::Message::EvictInvocation(
                    v1::DurableTaskEvictInvocationRequest {
                        invocation_count,
                        durable_task_external_id: task_id.to_owned(),
                        reason,
                    },
                )),
            })
            .await
            .map_err(|error| Error::Durable(format!("request channel closed: {error}")))?;

        Ok(())
    }

    /// The receive loop processes incoming messages from the bidi stream.
    async fn receive_loop(
        mut stream: tonic::Streaming<v1::DurableTaskResponse>,
        pending_acks: Arc<Mutex<HashMap<AckKey, oneshot::Sender<Result<AckResponse>>>>>,
        pending_callbacks: Arc<
            Mutex<HashMap<CallbackKey, oneshot::Sender<Result<EntryCompletedResult>>>>,
        >,
    ) {
        loop {
            match stream.message().await {
                Ok(Some(response)) => {
                    Self::dispatch_response(response, &pending_acks, &pending_callbacks).await;
                }
                Ok(None) => {
                    // Stream closed
                    break;
                }
                Err(error) => {
                    // TODO: implement reconnection with backoff
                    eprintln!("durable stream error: {error}");
                    break;
                }
            }
        }
    }

    async fn dispatch_response(
        response: v1::DurableTaskResponse,
        pending_acks: &Arc<Mutex<HashMap<AckKey, oneshot::Sender<Result<AckResponse>>>>>,
        pending_callbacks: &Arc<
            Mutex<HashMap<CallbackKey, oneshot::Sender<Result<EntryCompletedResult>>>>,
        >,
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
                    if let Some(tx) = pending_acks.lock().await.remove(&key) {
                        let _ = tx.send(Err(Error::Durable(format!(
                            "durable error (type {:?}): {}",
                            error.error_type, error.error_message
                        ))));
                    }
                }
            }
            v1::durable_task_response::Message::EvictionAck(_) => {
                // Eviction acks are handled by the eviction manager
            }
            v1::durable_task_response::Message::ServerEvict(notice) => {
                // TODO: Forward to eviction manager
                eprintln!(
                    "server eviction notice for task {}: {}",
                    notice.durable_task_external_id, notice.reason
                );
            }
            v1::durable_task_response::Message::RegisterWorker(_) => {
                // Unexpected — registration is handled during startup
            }
        }
    }
}
