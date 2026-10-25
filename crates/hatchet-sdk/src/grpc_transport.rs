//! gRPC transport implementation for connecting to a real Hatchet engine.
//!
//! Implements the `Transport` trait using actual gRPC calls to the engine's
//! `AdminService`, `Dispatcher`, `EventsService`, and `WorkflowService`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use another_hatchet_core::{RunOpts, WorkflowRunId};
use async_trait::async_trait;
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use serde_json::Value;
use tokio::sync::{Mutex, RwLock};
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::{Request, Status};

use crate::clients::{DurableBranchResult, LogRecord, RestoreTaskResult};
use crate::config::{
    HatchetConfig, apply_namespace, apply_workflow_namespace, normalize_namespace,
};
use crate::context::{LogLevel, TriggerData};
use crate::durable::RenderedConditions;
use crate::error::{Error, Result};
use crate::proto;
use crate::rest_client::RestApiClient;
use crate::retry::{RetryConfig, retry};
use crate::transport::{Transport, WorkerStartOpts, WorkflowRegistration};
use crate::worker::TaskContextData;
use crate::workflow::WorkflowDeclaration;

const MAX_COMPLETED_RUNS: usize = 1_024;

/// gRPC transport that connects to a real Hatchet engine.
///
/// Holds gRPC client stubs for all services and applies bearer-token auth
/// to every request.
#[derive(Clone)]
pub struct GrpcTransport {
    config: HatchetConfig,
    rest: reqwest::Client,
    admin: proto::v1::admin_service_client::AdminServiceClient<Channel>,
    legacy_workflows: proto::workflows::workflow_service_client::WorkflowServiceClient<Channel>,
    dispatcher: proto::dispatcher::dispatcher_client::DispatcherClient<Channel>,
    events: proto::events::events_service_client::EventsServiceClient<Channel>,
    v1_dispatcher: proto::v1::v1_dispatcher_client::V1DispatcherClient<Channel>,
    retry_config: RetryConfig,
    /// Worker ID assigned after registration (set by the worker startup flow).
    worker_id: Arc<RwLock<Option<String>>>,
    completed: Arc<Mutex<HashMap<WorkflowRunId, Result<Value>>>>,
    log_records: Arc<Mutex<Vec<LogRecord>>>,
    running_cancellations: Arc<Mutex<HashMap<String, Arc<tokio_util::sync::CancellationToken>>>>,
    durable_listener:
        Arc<std::sync::RwLock<Option<Arc<crate::durable_listener::DurableListenerClient>>>>,
    rest_api_client: Arc<RestApiClient>,
    rest_base: String,
}

impl std::fmt::Debug for GrpcTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GrpcTransport")
            .field("host_port", &self.config.host_port)
            .field("tenant_id", &self.config.tenant_id)
            .finish()
    }
}

impl GrpcTransport {
    /// Creates a new gRPC transport from the given configuration.
    ///
    /// Establishes a gRPC channel and creates client stubs for all services.
    ///
    /// # Errors
    ///
    /// Returns an error if the channel cannot be constructed (e.g. TLS misconfiguration).
    pub async fn from_config(mut config: HatchetConfig) -> Result<Self> {
        config.namespace = config.namespace.as_deref().and_then(normalize_namespace);
        let channel = config.connect_channel().await?;

        let rest_client = build_rest_client(&config)?;
        let rest_base = format!("{}/api/v1", config.server_url.trim_end_matches('/'));
        let rest_api_client = RestApiClient::new_with_namespace(
            rest_client.clone(),
            rest_base.clone(),
            config.tenant_id.clone(),
            config.namespace.clone(),
        );
        Ok(Self {
            rest: rest_client,
            admin: proto::v1::admin_service_client::AdminServiceClient::new(channel.clone()),
            legacy_workflows: proto::workflows::workflow_service_client::WorkflowServiceClient::new(
                channel.clone(),
            ),
            dispatcher: proto::dispatcher::dispatcher_client::DispatcherClient::new(
                channel.clone(),
            ),
            events: proto::events::events_service_client::EventsServiceClient::new(channel.clone()),
            v1_dispatcher: proto::v1::v1_dispatcher_client::V1DispatcherClient::new(channel),
            retry_config: RetryConfig::default(),
            worker_id: Arc::new(RwLock::new(None)),
            completed: Arc::new(Mutex::new(HashMap::new())),
            log_records: Arc::new(Mutex::new(Vec::new())),
            running_cancellations: Arc::new(Mutex::new(HashMap::new())),
            durable_listener: Arc::new(std::sync::RwLock::new(None)),
            rest_api_client,
            rest_base,
            config,
        })
    }

    /// Returns the configuration.
    #[must_use]
    pub fn config(&self) -> &HatchetConfig {
        &self.config
    }

    /// Returns a cloned transport with a custom retry policy for gRPC calls and
    /// REST run metadata reads. Metadata requests time out after ten seconds;
    /// `max_elapsed`, when set, also bounds each metadata read's full retry cycle.
    #[must_use]
    pub fn with_retry_config(mut self, retry_config: RetryConfig) -> Self {
        self.retry_config = retry_config;
        self
    }

    /// Sets the worker ID (called after `dispatcher.register()`).
    pub async fn set_worker_id(&self, worker_id: String) {
        *self.worker_id.write().await = Some(worker_id);
    }

    /// Returns the current worker ID, if registered.
    pub async fn worker_id(&self) -> Option<String> {
        self.worker_id.read().await.clone()
    }

    /// Returns a reference to the v1 dispatcher client (for durable task streaming).
    pub fn v1_dispatcher(&self) -> &proto::v1::v1_dispatcher_client::V1DispatcherClient<Channel> {
        &self.v1_dispatcher
    }

    /// Returns a mutable reference to the legacy dispatcher client (for worker registration).
    pub fn dispatcher_mut(
        &mut self,
    ) -> &mut proto::dispatcher::dispatcher_client::DispatcherClient<Channel> {
        &mut self.dispatcher
    }

    /// Creates a tonic `Request` with the bearer token attached.
    fn authed_request<T>(&self, payload: T) -> Request<T> {
        let mut request = Request::new(payload);
        if let Ok(value) = format!("Bearer {}", self.config.token())
            .parse::<MetadataValue<tonic::metadata::Ascii>>()
        {
            request.metadata_mut().insert("authorization", value);
        }
        request
    }

    /// Registers the worker with the engine's dispatcher service.
    ///
    /// # Errors
    ///
    /// Returns an error if the gRPC call fails.
    pub async fn register_worker(
        &self,
        worker_name: &str,
        actions: Vec<String>,
        labels: HashMap<String, String>,
        slot_config: HashMap<String, i32>,
    ) -> Result<proto::dispatcher::WorkerRegisterResponse> {
        if !slot_config.is_empty() {
            self.engine_version()
                .await?
                .require(crate::EngineFeature::SlotConfig)?;
        }
        let worker_labels: HashMap<String, proto::dispatcher::WorkerLabels> = labels
            .into_iter()
            .map(|(key, value)| {
                (
                    key,
                    proto::dispatcher::WorkerLabels {
                        str_value: Some(value),
                        int_value: None,
                    },
                )
            })
            .collect();

        let request = self.authed_request(proto::dispatcher::WorkerRegisterRequest {
            worker_name: worker_name.to_owned(),
            actions: actions.clone(),
            services: Vec::new(),
            slots: None,
            labels: worker_labels,
            webhook_id: None,
            runtime_info: Some(proto::dispatcher::RuntimeInfo {
                sdk_version: Some(env!("CARGO_PKG_VERSION").to_owned()),
                language: None,
                language_version: None,
                os: Some(std::env::consts::OS.to_owned()),
                extra: Some("rust".to_owned()),
            }),
            slot_config,
        });

        let dispatcher = self.dispatcher.clone();
        let response = retry(
            || {
                let req = request.get_ref().clone();
                let mut client = dispatcher.clone();
                let authed = self.authed_request(req);
                async move {
                    let res = client.register(authed).await;
                    res.map(|response| response.into_inner())
                        .map_err(grpc_error)
                }
            },
            &self.retry_config,
        )
        .await?;

        self.set_worker_id(response.worker_id.clone()).await;
        Ok(response)
    }

    /// Sends a heartbeat to keep the worker registration alive.
    ///
    /// # Errors
    ///
    /// Returns an error if the gRPC call fails.
    pub async fn heartbeat(&self) -> Result<()> {
        let worker_id = self
            .worker_id()
            .await
            .ok_or_else(|| Error::Transport("worker not registered".to_owned()))?;

        let dispatcher = self.dispatcher.clone();
        retry(
            || {
                let mut client = dispatcher.clone();
                let authed = self.authed_request(proto::dispatcher::HeartbeatRequest {
                    worker_id: worker_id.clone(),
                    heartbeat_at: Some(prost_now()),
                });
                async move {
                    client
                        .heartbeat(authed)
                        .await
                        .map(|_| ())
                        .map_err(grpc_error)
                }
            },
            &self.retry_config,
        )
        .await
    }

    /// Sends a step action event (started, completed, failed) to the engine.
    ///
    /// # Errors
    ///
    /// Returns an error if the gRPC call fails.
    pub async fn send_step_action_event(
        &self,
        mut event: proto::dispatcher::StepActionEvent,
    ) -> Result<()> {
        event.retry_after_ms = self.supported_retry_after(event.retry_after_ms).await;
        let dispatcher = self.dispatcher.clone();
        let request = self.authed_request(event);

        retry(
            || {
                let req = request.get_ref().clone();
                let mut client = dispatcher.clone();
                let authed = self.authed_request(req);
                async move {
                    client
                        .send_step_action_event(authed)
                        .await
                        .map(|_| ())
                        .map_err(grpc_error)
                }
            },
            &self.retry_config,
        )
        .await
    }

    /// Calls `RefreshTimeout` on the dispatcher.
    ///
    /// # Errors
    ///
    /// Returns an error if the gRPC call fails.
    pub async fn refresh_timeout(&self, task_run_external_id: &str, increment: &str) -> Result<()> {
        let dispatcher = self.dispatcher.clone();
        retry(
            || {
                let mut client = dispatcher.clone();
                let authed = self.authed_request(proto::dispatcher::RefreshTimeoutRequest {
                    task_run_external_id: task_run_external_id.to_owned(),
                    increment_timeout_by: increment.to_owned(),
                });
                async move {
                    client
                        .refresh_timeout(authed)
                        .await
                        .map(|_| ())
                        .map_err(grpc_error)
                }
            },
            &self.retry_config,
        )
        .await
    }

    /// Calls `ReleaseSlot` on the dispatcher.
    ///
    /// # Errors
    ///
    /// Returns an error if the gRPC call fails.
    pub async fn release_slot(&self, task_run_external_id: &str) -> Result<()> {
        let dispatcher = self.dispatcher.clone();
        retry(
            || {
                let mut client = dispatcher.clone();
                let authed = self.authed_request(proto::dispatcher::ReleaseSlotRequest {
                    task_run_external_id: task_run_external_id.to_owned(),
                });
                async move {
                    client
                        .release_slot(authed)
                        .await
                        .map(|_| ())
                        .map_err(grpc_error)
                }
            },
            &self.retry_config,
        )
        .await
    }

    /// Opens a server-streaming `Listen` call for receiving assigned actions.
    ///
    /// # Errors
    ///
    /// Returns an error if the gRPC call fails.
    pub async fn listen(
        &self,
        worker_id: &str,
    ) -> Result<tonic::Streaming<proto::dispatcher::AssignedAction>> {
        let mut dispatcher = self.dispatcher.clone();
        let request = self.authed_request(proto::dispatcher::WorkerListenRequest {
            worker_id: worker_id.to_owned(),
        });

        let response = dispatcher.listen_v2(request).await.map_err(grpc_error)?;
        Ok(response.into_inner())
    }

    /// Calls `GetRunDetails` on the v1 admin service to poll for run completion.
    ///
    /// # Errors
    ///
    /// Returns an error if the gRPC call fails.
    pub async fn get_run_details(&self, run_id: &str) -> Result<proto::v1::GetRunDetailsResponse> {
        let admin = self.admin.clone();
        retry(
            || {
                let mut client = admin.clone();
                let authed = self.authed_request(proto::v1::GetRunDetailsRequest {
                    external_id: run_id.to_owned(),
                });
                async move {
                    client
                        .get_run_details(authed)
                        .await
                        .map(|response| response.into_inner())
                        .map_err(grpc_error)
                }
            },
            &self.retry_config,
        )
        .await
    }
}

#[async_trait]
impl Transport for GrpcTransport {
    async fn run_details(
        &self,
        run_id: &WorkflowRunId,
    ) -> Result<proto::v1::GetRunDetailsResponse> {
        self.get_run_details(&run_id.0).await
    }

    async fn engine_version(&self) -> Result<crate::EngineVersion> {
        let version = retry(
            || async {
                self.dispatcher
                    .clone()
                    .get_version(self.authed_request(proto::dispatcher::GetVersionRequest {}))
                    .await
                    .map(|response| response.into_inner().version)
                    .map_err(grpc_error)
            },
            &self.retry_config,
        )
        .await?;
        crate::EngineVersion::parse(&version)
    }

    fn namespace(&self) -> Option<&str> {
        self.config.namespace.as_deref()
    }

    async fn run_workflow(
        &self,
        workflow_name: &str,
        input: Value,
        opts: RunOpts,
    ) -> Result<WorkflowRunId> {
        let workflow_name =
            apply_workflow_namespace(workflow_name, self.config.namespace.as_deref());
        let additional_metadata = if opts.additional_metadata.is_empty() {
            None
        } else {
            Some(
                serde_json::to_string(&opts.additional_metadata)
                    .unwrap_or_else(|_| "{}".to_owned()),
            )
        };

        let desired_worker_labels = opts
            .desired_worker_labels
            .iter()
            .map(|(key, value)| (key.clone(), run_opt_label_to_proto(value)))
            .collect();

        let trigger = proto::v1::TriggerWorkflowRequest {
            name: workflow_name.clone(),
            input: serde_json::to_string(&input).unwrap_or_default(),
            parent_id: opts.parent_workflow_run_id.clone(),
            parent_task_run_external_id: opts.parent_task_run_id.clone(),
            child_index: opts.child_index.map(|index| index as i32),
            child_key: opts.child_key.clone(),
            additional_metadata,
            desired_worker_id: opts.sticky.clone(),
            priority: opts.priority.map(priority_to_i32),
            desired_worker_labels,
        };

        let workflows = self.legacy_workflows.clone();

        let response = retry(
            || {
                let req = trigger.clone();
                let mut client = workflows.clone();
                let authed = self.authed_request(req);
                async move {
                    client
                        .trigger_workflow(authed)
                        .await
                        .map(|response| WorkflowRunId(response.into_inner().workflow_run_id))
                        .map_err(grpc_error)
                }
            },
            &self.retry_config,
        )
        .await?;

        Ok(response)
    }

    async fn run_workflows(
        &self,
        workflow_name: &str,
        inputs: Vec<Value>,
        opts: RunOpts,
    ) -> Result<Vec<WorkflowRunId>> {
        let workflow_name =
            apply_workflow_namespace(workflow_name, self.config.namespace.as_deref());
        if inputs.is_empty() {
            return Ok(Vec::new());
        }

        let additional_metadata = if opts.additional_metadata.is_empty() {
            None
        } else {
            Some(
                serde_json::to_string(&opts.additional_metadata)
                    .unwrap_or_else(|_| "{}".to_owned()),
            )
        };

        let desired_worker_labels: HashMap<String, proto::v1::DesiredWorkerLabels> = opts
            .desired_worker_labels
            .iter()
            .map(|(key, value)| (key.clone(), run_opt_label_to_proto(value)))
            .collect();

        let triggers: Vec<proto::v1::TriggerWorkflowRequest> = inputs
            .into_iter()
            .enumerate()
            .map(|(index, input)| {
                let child_index = if opts.parent_workflow_run_id.is_some() {
                    Some(
                        opts.child_index
                            .unwrap_or_default()
                            .saturating_add(index as u64) as i32,
                    )
                } else {
                    opts.child_index.map(|ci| ci as i32)
                };
                proto::v1::TriggerWorkflowRequest {
                    name: workflow_name.clone(),
                    input: serde_json::to_string(&input).unwrap_or_default(),
                    parent_id: opts.parent_workflow_run_id.clone(),
                    parent_task_run_external_id: opts.parent_task_run_id.clone(),
                    child_index,
                    child_key: opts.child_key.clone(),
                    additional_metadata: additional_metadata.clone(),
                    desired_worker_id: opts.sticky.clone(),
                    priority: opts.priority.map(priority_to_i32),
                    desired_worker_labels: desired_worker_labels.clone(),
                }
            })
            .collect();

        let bulk_request = proto::workflows::BulkTriggerWorkflowRequest {
            workflows: triggers.clone(),
        };

        let workflows = self.legacy_workflows.clone();
        let response = retry(
            || {
                let req = bulk_request.clone();
                let mut client = workflows.clone();
                let authed = self.authed_request(req);
                async move {
                    client
                        .bulk_trigger_workflow(authed)
                        .await
                        .map(|response| {
                            response
                                .into_inner()
                                .workflow_run_ids
                                .into_iter()
                                .map(WorkflowRunId)
                                .collect::<Vec<_>>()
                        })
                        .map_err(grpc_error)
                }
            },
            &self.retry_config,
        )
        .await?;

        Ok(response)
    }

    async fn register_result_channel(&self, _run_id: &WorkflowRunId) -> Result<()> {
        Ok(())
    }

    async fn await_result(&self, run_id: &WorkflowRunId) -> Result<Value> {
        // Check if already completed
        if let Some(result) = self.completed.lock().await.get(run_id).cloned() {
            return result;
        }

        // A durable branch can reuse a run ID, including when another client
        // initiates it. Do not permanently cache polled engine results.
        self.poll_run_result(run_id).await
    }

    async fn resolve_result(&self, run_id: &WorkflowRunId, result: Result<Value>) {
        let mut completed = self.completed.lock().await;
        let oldest = (completed.len() >= MAX_COMPLETED_RUNS && !completed.contains_key(run_id))
            .then(|| completed.keys().next().cloned())
            .flatten();
        if let Some(oldest) = oldest {
            completed.remove(&oldest);
        }
        completed.insert(run_id.clone(), result);
    }

    async fn cancel_task_runs(&self, ids: &[String]) -> Result<()> {
        self.rest_api_client
            .mutate_runs("cancel", &crate::RunSelection::ExternalIds(ids.to_vec()))
            .await?;
        Ok(())
    }

    async fn cancel_workflow_run(&self, run_id: &WorkflowRunId) -> Result<()> {
        self.rest_api_client.cancel_run(&run_id.0).await
    }

    async fn branch_durable_task(
        &self,
        task_external_id: &str,
        node_id: i64,
        branch_id: i64,
    ) -> Result<DurableBranchResult> {
        let result = self
            .rest_api_client
            .branch_durable_task(task_external_id, node_id, branch_id)
            .await?;
        self.completed.lock().await.clear();
        Ok(result)
    }

    async fn restore_task(&self, task_external_id: &str) -> Result<RestoreTaskResult> {
        let result = self.rest_api_client.restore_task(task_external_id).await?;
        self.completed.lock().await.clear();
        Ok(result)
    }

    async fn start_worker_loop(&self, opts: WorkerStartOpts) -> Result<()> {
        if !opts.durable_task_dispatchers.is_empty() {
            self.engine_version()
                .await?
                .require(crate::EngineFeature::DurableEviction)?;
        }
        let mut actions = Vec::new();
        for action in opts.task_dispatchers.keys() {
            actions.push(worker_action_id(action));
        }
        for action in opts.durable_task_dispatchers.keys() {
            actions.push(worker_action_id(action));
        }
        let mut connected_once = false;
        let mut reconnect_backoff = std::time::Duration::from_secs(1);

        loop {
            if opts.cancel_token.is_cancelled() {
                return Ok(());
            }

            let response = match self
                .register_worker(
                    &opts.worker_name,
                    actions.clone(),
                    opts.labels.clone(),
                    opts.slot_config.clone(),
                )
                .await
            {
                Ok(response) => response,
                Err(error) if !connected_once => return Err(error),
                Err(error) => {
                    let _ = opts.connection_tx.send(
                        crate::transport::WorkerConnectionState::Reconnecting(error.to_string()),
                    );
                    tokio::select! {
                        _ = opts.cancel_token.cancelled() => return Ok(()),
                        _ = tokio::time::sleep(reconnect_backoff) => {}
                    }
                    reconnect_backoff =
                        (reconnect_backoff * 2).min(std::time::Duration::from_secs(30));
                    continue;
                }
            };
            let worker_id = response.worker_id;

            let mut durable_listener = if opts.durable_task_dispatchers.is_empty() {
                None
            } else {
                match crate::durable_listener::DurableListenerClient::start(
                    self.config.clone(),
                    worker_id.clone(),
                )
                .await
                {
                    Ok(listener) => Some(Arc::new(listener)),
                    Err(error) if !connected_once => return Err(error),
                    Err(error) => {
                        let _ = opts.connection_tx.send(
                            crate::transport::WorkerConnectionState::Reconnecting(
                                error.to_string(),
                            ),
                        );
                        tokio::select! {
                            _ = opts.cancel_token.cancelled() => return Ok(()),
                            _ = tokio::time::sleep(reconnect_backoff) => {}
                        }
                        reconnect_backoff =
                            (reconnect_backoff * 2).min(std::time::Duration::from_secs(30));
                        continue;
                    }
                }
            };

            // Tonic waits for response headers before returning a server stream,
            // while Hatchet's ListenV2 endpoint may not flush those headers until
            // the first assignment. Start listening and heartbeating together,
            // and report readiness after the first successful heartbeat. This
            // mirrors the official TypeScript SDK, whose async iterable starts
            // ListenV2 without awaiting a response before starting heartbeats.
            let mut listen = Box::pin(retry(
                || async { self.listen(&worker_id).await },
                &self.retry_config,
            ));
            let mut heartbeat = tokio::time::interval(std::time::Duration::from_secs(4));
            let mut stream = None;
            let mut ready_sent = false;

            let stream_result = loop {
                tokio::select! {
                    _ = opts.cancel_token.cancelled() => return Ok(()),
                    result = &mut listen, if stream.is_none() => {
                        match result {
                            Ok(connected_stream) => {
                                stream = Some(connected_stream);
                                if ready_sent {
                                    break Ok(stream.expect("stream was just stored"));
                                }
                            }
                            Err(error) => break Err(error),
                        }
                    }
                    _ = heartbeat.tick() => {
                        if let Err(error) = self.heartbeat().await {
                            break Err(error);
                        }
                        if !ready_sent {
                            if let Ok(mut guard) = self.durable_listener.write() {
                                *guard = durable_listener.take();
                            }
                            connected_once = true;
                            reconnect_backoff = std::time::Duration::from_secs(1);
                            let _ = opts
                                .connection_tx
                                .send(crate::transport::WorkerConnectionState::Ready);
                            ready_sent = true;
                            if let Some(connected_stream) = stream.take() {
                                break Ok(connected_stream);
                            }
                        }
                    }
                }
            };

            let mut stream: tonic::Streaming<proto::dispatcher::AssignedAction> =
                match stream_result {
                    Ok(stream) => stream,
                    Err(error) if !connected_once => return Err(error),
                    Err(error) => {
                        if let Ok(mut guard) = self.durable_listener.write() {
                            *guard = None;
                        }
                        let _ = opts.connection_tx.send(
                            crate::transport::WorkerConnectionState::Reconnecting(
                                error.to_string(),
                            ),
                        );
                        tokio::select! {
                            _ = opts.cancel_token.cancelled() => return Ok(()),
                            _ = tokio::time::sleep(reconnect_backoff) => {}
                        }
                        reconnect_backoff =
                            (reconnect_backoff * 2).min(std::time::Duration::from_secs(30));
                        continue;
                    }
                };
            let (lifecycle_failure_tx, mut lifecycle_failure_rx) =
                tokio::sync::mpsc::unbounded_channel();
            let disconnect_reason = loop {
                tokio::select! {
                    _ = opts.cancel_token.cancelled() => return Ok(()),
                    failure = lifecycle_failure_rx.recv() => {
                        break failure.unwrap_or_else(|| {
                            "worker lifecycle failure channel closed".to_owned()
                        });
                    }
                    _ = heartbeat.tick() => {
                        if let Err(error) = self.heartbeat().await {
                            break format!("worker heartbeat failed: {error}");
                        }
                    }
                    message = stream.message() => {
                        match message {
                            Ok(Some(action)) => {
                                if let Err(error) = self.handle_assigned_action(
                                    action,
                                    &opts.worker_name,
                                    &worker_id,
                                    &opts,
                                    lifecycle_failure_tx.clone(),
                                )
                                .await
                                {
                                    break format!("worker action handling failed: {error}");
                                }
                            }
                            Ok(None) => {
                                break "worker action stream closed by the engine".to_owned();
                            }
                            Err(error) => break format!("worker action stream failed: {error}"),
                        }
                    }
                }
            };

            if let Ok(mut guard) = self.durable_listener.write() {
                *guard = None;
            }
            let _ = opts
                .connection_tx
                .send(crate::transport::WorkerConnectionState::Reconnecting(
                    disconnect_reason,
                ));
            tokio::select! {
                _ = opts.cancel_token.cancelled() => return Ok(()),
                _ = tokio::time::sleep(reconnect_backoff) => {}
            }
            reconnect_backoff = (reconnect_backoff * 2).min(std::time::Duration::from_secs(30));
        }
    }

    async fn register_workflow_definition(
        &self,
        mut definition: WorkflowDeclaration<Value, Value>,
    ) -> Result<()> {
        if let Some(config) = &definition.idempotency {
            config.ttl_ms()?;
            self.engine_version()
                .await?
                .require(crate::EngineFeature::Idempotency)?;
        }
        if definition.steps.iter().any(|s| s.config.batch.is_some()) {
            self.engine_version()
                .await?
                .require(crate::EngineFeature::BatchTasks)?;
        }
        definition.name =
            apply_workflow_namespace(&definition.name, self.config.namespace.as_deref());
        definition.on_events = definition
            .on_events
            .into_iter()
            .map(|event| apply_namespace(&event, self.config.namespace.as_deref()))
            .collect();
        let registration =
            WorkflowRegistration::from_declaration(&definition, self.config.namespace.as_deref());

        let tasks: Vec<proto::v1::CreateTaskOpts> = registration
            .tasks
            .iter()
            .map(|task| rendered_to_proto_task(task, &registration))
            .collect();
        let on_failure_task = definition.on_failure.as_ref().map(|failure_task_name| {
            let config = &definition.task_defaults;
            rendered_to_proto_task(
                &crate::transport::TaskRegistration {
                    readable_id: failure_task_name.clone(),
                    action: worker_action_id(failure_task_name),
                    timeout: config.execution_timeout,
                    schedule_timeout: config.schedule_timeout,
                    parents: Vec::new(),
                    retries: config.retries,
                    rate_limits: config.rate_limits.clone(),
                    worker_labels: config.desired_worker_labels.clone(),
                    backoff_factor: config.backoff.as_ref().map(|backoff| backoff.factor),
                    backoff_max_seconds: config.backoff.as_ref().map(|backoff| backoff.max_seconds),
                    concurrency: config.concurrency.clone(),
                    slot_requests: config.slot_requests.clone(),
                    conditions: RenderedConditions::default(),
                    is_durable: definition.on_failure_is_durable,
                    batch: None,
                },
                &registration,
            )
        });

        let idempotency = definition
            .idempotency
            .as_ref()
            .map(|config| -> Result<_> {
                Ok(proto::v1::IdempotencyConfig {
                    expression: config.expression.clone(),
                    ttl_ms: config.ttl_ms()?,
                    method: Some(match config.method {
                        crate::IdempotencyMethod::Ttl => proto::v1::IdempotencyMethod::Ttl as i32,
                        crate::IdempotencyMethod::Status => {
                            proto::v1::IdempotencyMethod::Status as i32
                        }
                    }),
                })
            })
            .transpose()?;
        let create_request = proto::v1::CreateWorkflowVersionRequest {
            name: registration.name.clone(),
            description: definition.description.unwrap_or_default(),
            version: definition.version.unwrap_or_default(),
            event_triggers: registration.event_triggers.clone(),
            cron_triggers: registration.cron_triggers.clone(),
            tasks,
            concurrency: None,
            cron_input: None,
            on_failure_task,
            sticky: definition.sticky.as_ref().map(|s| match s {
                crate::workflow::StickyStrategy::Soft => proto::v1::StickyStrategy::Soft.into(),
                crate::workflow::StickyStrategy::Hard => proto::v1::StickyStrategy::Hard.into(),
            }),
            default_priority: definition.default_priority.map(priority_to_i32),
            concurrency_arr: definition.concurrency.as_ref().map_or(Vec::new(), |c| {
                vec![proto::v1::Concurrency {
                    expression: c.expression.clone(),
                    max_runs: c.max_runs.map(|m| m as i32),
                    limit_strategy: c.limit_strategy.as_ref().map(|s| match s {
                        crate::task::ConcurrencyLimitStrategy::CancelInProgress => {
                            proto::v1::ConcurrencyLimitStrategy::CancelInProgress.into()
                        }
                        crate::task::ConcurrencyLimitStrategy::CancelNewest => {
                            proto::v1::ConcurrencyLimitStrategy::CancelNewest.into()
                        }
                        crate::task::ConcurrencyLimitStrategy::GroupRoundRobin => {
                            proto::v1::ConcurrencyLimitStrategy::GroupRoundRobin.into()
                        }
                    }),
                }]
            }),
            default_filters: definition
                .default_filters
                .iter()
                .map(|f| proto::v1::DefaultFilter {
                    expression: f.expression.clone(),
                    scope: f.scope.clone(),
                    payload: f
                        .payload
                        .as_ref()
                        .map(|p| serde_json::to_vec(p).unwrap_or_default()),
                })
                .collect(),
            input_json_schema: serialize_input_schema(definition.input_schema.as_ref()),
            idempotency,
        };

        let admin = self.admin.clone();
        let _request = self.authed_request(create_request.clone());

        retry(
            || {
                let req = create_request.clone();
                let mut client = admin.clone();
                let authed = self.authed_request(req);
                async move {
                    client
                        .put_workflow(authed)
                        .await
                        .map(|_| ())
                        .map_err(grpc_error)
                }
            },
            &self.retry_config,
        )
        .await?;

        Ok(())
    }

    async fn append_run_stream(
        &self,
        _run_id: &WorkflowRunId,
        task_run_id: &str,
        payload: Value,
    ) -> Result<()> {
        let message = serde_json::to_vec(&payload)
            .map_err(|error| Error::InputSerialization(format!("stream payload: {error}")))?;

        let mut events = self.events.clone();
        let request = self.authed_request(proto::events::PutStreamEventRequest {
            task_run_external_id: task_run_id.to_owned(),
            created_at: Some(prost_now()),
            message,
            metadata: "{}".to_owned(),
            event_index: None,
        });

        events.put_stream_event(request).await.map_err(grpc_error)?;
        Ok(())
    }

    async fn stream_run(&self, run_id: &WorkflowRunId) -> Result<Vec<Value>> {
        self.subscribe_run_stream(run_id).await?.collect_all().await
    }

    async fn subscribe_run_stream(&self, run_id: &WorkflowRunId) -> Result<crate::RunEventStream> {
        let mut dispatcher = self.dispatcher.clone();
        let request = self.authed_request(proto::dispatcher::SubscribeToWorkflowEventsRequest {
            workflow_run_id: Some(run_id.0.clone()),
            additional_meta_key: None,
            additional_meta_value: None,
        });

        let mut stream = dispatcher
            .subscribe_to_workflow_events(request)
            .await
            .map_err(grpc_error)?
            .into_inner();

        Ok(crate::RunEventStream::new(async_stream::try_stream! {
            while let Some(event) = stream.message().await.map_err(grpc_error)? {
                let complete = event.hangup;
                if event.event_type == proto::dispatcher::ResourceEventType::Stream as i32 {
                    yield workflow_event_payload_to_json(&event);
                }
                if complete { break; }
            }
        }))
    }

    async fn append_run_log(&self, run_id: &WorkflowRunId, level: LogLevel, message: String) {
        let task_run_id = run_id.0.clone();
        let level_str = match level {
            LogLevel::Debug => "DEBUG",
            LogLevel::Info => "INFO",
            LogLevel::Warn => "WARN",
            LogLevel::Error => "ERROR",
        };

        self.log_records.lock().await.push(LogRecord {
            level: level_str.to_owned(),
            message: message.clone(),
        });

        let mut events = self.events.clone();
        let request = self.authed_request(proto::events::PutLogRequest {
            task_run_external_id: task_run_id,
            created_at: Some(prost_now()),
            message,
            level: Some(level_str.to_owned()),
            metadata: "{}".to_owned(),
            task_retry_count: None,
        });

        let _ = events.put_log(request).await;
    }

    async fn refresh_timeout(&self, task_run_id: &str, increment: &str) -> Result<()> {
        GrpcTransport::refresh_timeout(self, task_run_id, increment).await
    }

    async fn release_slot(&self, task_run_id: &str) -> Result<()> {
        GrpcTransport::release_slot(self, task_run_id).await
    }

    async fn list_logs(&self, task_run_id: &str) -> Result<Vec<LogRecord>> {
        let candidate_urls = [
            format!(
                "{}/stable/tasks/{}/logs?limit=500",
                self.rest_base, task_run_id
            ),
            format!("{}/tasks/{}/logs?limit=500", self.rest_base, task_run_id),
        ];

        let mut last_error = None;
        for url in candidate_urls {
            let response = self
                .rest
                .get(&url)
                // Use the authenticated client's implicit headers
                .send()
                .await
                .map_err(|e| Error::Transport(format!("REST GET failed: {e}")))?;

            if !response.status().is_success() {
                last_error = Some(Error::Transport(format!(
                    "REST GET logs: HTTP {}",
                    response.status()
                )));
                continue;
            }

            let payload = response
                .json::<V1LogLineList>()
                .await
                .map_err(|e| Error::Transport(format!("REST decode failed: {e}")))?;

            return Ok(payload
                .rows
                .into_iter()
                .map(|row| LogRecord {
                    level: row.level.unwrap_or_else(|| "INFO".to_owned()),
                    message: row.message,
                })
                .collect());
        }

        Err(last_error.unwrap_or_else(|| Error::Transport("REST GET logs failed".to_owned())))
    }

    async fn push_event(&self, key: &str, payload: &Value) -> Result<()> {
        self.push_event_with_scope(key, payload, None).await
    }

    async fn push_event_with_scope(
        &self,
        key: &str,
        payload: &Value,
        scope: Option<&str>,
    ) -> Result<()> {
        let key = apply_namespace(key, self.config.namespace.as_deref());
        let request = proto::events::PushEventRequest {
            key: key.clone(),
            payload: serde_json::to_string(payload).map_err(|error| {
                Error::InputSerialization(format!("event '{key}' payload: {error}"))
            })?,
            event_timestamp: Some(prost_now()),
            additional_metadata: None,
            priority: None,
            scope: scope.map(str::to_owned),
        };
        let events = self.events.clone();
        retry(
            || {
                let mut client = events.clone();
                let request = self.authed_request(request.clone());
                async move { client.push(request).await.map(|_| ()).map_err(grpc_error) }
            },
            &self.retry_config,
        )
        .await
    }

    async fn bulk_push_events(&self, event_records: &[(String, Value)]) -> Result<()> {
        let scoped_records = event_records
            .iter()
            .map(|(key, payload)| (key.clone(), payload.clone(), None))
            .collect::<Vec<_>>();
        self.bulk_push_events_with_scope(&scoped_records).await
    }

    async fn bulk_push_events_with_scope(
        &self,
        event_records: &[(String, Value, Option<String>)],
    ) -> Result<()> {
        let events_payload = event_records
            .iter()
            .map(|(key, payload, scope)| {
                Ok(proto::events::PushEventRequest {
                    key: apply_namespace(key, self.config.namespace.as_deref()),
                    payload: serde_json::to_string(payload).map_err(|error| {
                        Error::InputSerialization(format!("event '{key}' payload: {error}"))
                    })?,
                    event_timestamp: Some(prost_now()),
                    additional_metadata: None,
                    priority: None,
                    scope: scope.clone(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let request = proto::events::BulkPushEventRequest {
            events: events_payload,
        };
        let events = self.events.clone();
        retry(
            || {
                let mut client = events.clone();
                let request = self.authed_request(request.clone());
                async move {
                    client
                        .bulk_push(request)
                        .await
                        .map(|_| ())
                        .map_err(grpc_error)
                }
            },
            &self.retry_config,
        )
        .await
    }

    async fn pause_rate_limit(
        &self,
        key: &str,
        until: std::time::SystemTime,
    ) -> Result<crate::RateLimitPauseResult> {
        if key.trim().is_empty() {
            return Err(Error::InputValidation(vec![
                "rate-limit key must not be empty".to_owned(),
            ]));
        }
        let paused_until = crate::rate_pause::timestamp(until)?;
        self.engine_version()
            .await?
            .require(crate::EngineFeature::RateLimitPause)?;
        let response = self
            .legacy_workflows
            .clone()
            .pause_rate_limit(
                self.authed_request(proto::workflows::PauseRateLimitRequest {
                    key: key.to_owned(),
                    paused_until: Some(paused_until),
                }),
            )
            .await
            .map_err(grpc_error)?
            .into_inner();
        let until = response
            .paused_until
            .ok_or_else(|| Error::Transport("engine omitted the pause timestamp".to_owned()))?;
        Ok(crate::RateLimitPauseResult {
            key: key.to_owned(),
            paused_until: crate::rate_pause::system_time(until)?,
        })
    }

    async fn upsert_rate_limit(
        &self,
        key: &str,
        limit: u32,
        duration: crate::task::RateLimitDuration,
    ) -> Result<String> {
        let key = key.to_owned();
        let limit = i32::try_from(limit)
            .map_err(|_| Error::Transport("rate limit exceeds i32::MAX".to_owned()))?;
        let request = proto::workflows::PutRateLimitRequest {
            key: key.clone(),
            limit,
            duration: rate_limit_duration_to_legacy_proto(duration) as i32,
        };
        let workflows = self.legacy_workflows.clone();

        retry(
            || {
                let req = request.clone();
                let returned_key = key.clone();
                let mut client = workflows.clone();
                let authed = self.authed_request(req);
                async move {
                    client
                        .put_rate_limit(authed)
                        .await
                        .map(|_| returned_key)
                        .map_err(grpc_error)
                }
            },
            &self.retry_config,
        )
        .await
    }

    fn durable_listener(&self) -> Option<Arc<crate::durable_listener::DurableListenerClient>> {
        self.durable_listener
            .read()
            .ok()
            .and_then(|guard| guard.clone())
    }

    fn rest_api(&self) -> Option<Arc<RestApiClient>> {
        Some(Arc::clone(&self.rest_api_client))
    }
}

impl GrpcTransport {
    async fn handle_assigned_action(
        &self,
        action: proto::dispatcher::AssignedAction,
        _worker_name: &str,
        worker_id: &str,
        opts: &crate::transport::WorkerStartOpts,
        lifecycle_failure_tx: tokio::sync::mpsc::UnboundedSender<String>,
    ) -> Result<()> {
        if action.action_type == proto::dispatcher::ActionType::CancelStepRun as i32 {
            if let Some(token) = self.running_cancellations.lock().await.remove(
                if action.task_run_external_id.is_empty() {
                    action.batch_id.as_deref().unwrap_or_default()
                } else {
                    &action.task_run_external_id
                },
            ) {
                token.cancel();
            }
            return Ok(());
        }
        if action.action_type != proto::dispatcher::ActionType::StartStepRun as i32
            && action.action_type != proto::dispatcher::ActionType::StartBatch as i32
        {
            return Ok(());
        }

        if action.action_type == proto::dispatcher::ActionType::StartBatch as i32 {
            return self
                .handle_assigned_batch(action, worker_id, opts, lifecycle_failure_tx)
                .await;
        }
        let task_name = resolve_action_task_name(&action, opts);

        let is_durable = opts.durable_task_dispatchers.contains_key(&task_name);
        let dispatcher = if is_durable {
            opts.durable_task_dispatchers.get(&task_name)
        } else {
            opts.task_dispatchers.get(&task_name)
        };

        if let Some(dispatcher) = dispatcher {
            let dispatcher = dispatcher.clone();
            let task_run_id = action.task_run_external_id.clone();
            let worker_id_clone = worker_id.to_owned();
            let transport = self.clone();
            let cancel_token = Arc::new(tokio_util::sync::CancellationToken::new());
            self.running_cancellations
                .lock()
                .await
                .insert(task_run_id.clone(), cancel_token.clone());

            // Send STARTED
            transport
                .send_step_action_event(proto::dispatcher::StepActionEvent {
                    worker_id: worker_id_clone.clone(),
                    job_id: action.job_id.clone(),
                    job_run_id: action.job_run_id.clone(),
                    task_id: action.task_id.clone(),
                    task_run_external_id: task_run_id.clone(),
                    action_id: action.action_id.clone(),
                    event_timestamp: Some(prost_now()),
                    event_type: proto::dispatcher::StepActionEventType::StepEventTypeStarted as i32,
                    event_payload: String::new(),
                    retry_count: None,
                    should_not_retry: None,
                    retry_after_ms: None,
                })
                .await?;

            let ids = crate::context::ContextIds {
                workflow_name: action.job_name.clone(),
                task_name: task_name.clone(),
                workflow_run_id: WorkflowRunId(action.workflow_run_id.clone()),
                task_run_id: task_run_id.clone(),
                workflow_id: action.workflow_id.clone(),
                workflow_version_id: action.workflow_version_id.clone(),
            };

            let mut decoded = decode_step_action_payload(&action.action_payload);
            let run_opts = RunOpts {
                additional_metadata: action
                    .additional_metadata
                    .as_deref()
                    .and_then(|metadata| serde_json::from_str(metadata).ok())
                    .unwrap_or_default(),
                priority: priority_from_i32(action.priority),
                child_key: action.child_workflow_key.clone(),
                parent_workflow_run_id: action.parent_workflow_run_id.clone(),
                child_index: action
                    .child_workflow_index
                    .and_then(|index| u64::try_from(index).ok()),
                ..RunOpts::default()
            };
            infer_trigger_data_from_metadata(
                &mut decoded.trigger_data,
                &run_opts.additional_metadata,
            );
            let input = decoded.input;
            let context = TaskContextData {
                parent_outputs: decoded.parent_outputs,
                cancel_token: Some(cancel_token.as_ref().clone()),
                run_opts,
                retry_count: u32::try_from(action.retry_count).unwrap_or_default(),
                filter_payload: decoded.filter_payload,
                worker_id: Some(worker_id_clone.clone()),
                trigger_data: decoded.trigger_data,
                errors: decoded.errors,
                batch_member_ids: Vec::new(),
            };

            // Spawn execution
            tokio::spawn(async move {
                let result = if is_durable {
                    dispatcher
                        .run_durable_task_json_with_context(
                            &task_name,
                            input,
                            ids,
                            action.durable_task_invocation_count.unwrap_or(1) as u32,
                            context,
                        )
                        .await
                } else {
                    dispatcher
                        .run_task_json_with_context(&task_name, input, ids, context)
                        .await
                };

                {
                    let mut running = transport.running_cancellations.lock().await;
                    if running
                        .get(&task_run_id)
                        .is_some_and(|token| Arc::ptr_eq(token, &cancel_token))
                    {
                        running.remove(&task_run_id);
                    }
                }

                let Some((event_type, payload, should_not_retry, retry_after_ms)) =
                    completion_event_from_result(result, cancel_token.is_cancelled())
                else {
                    // Hatchet already owns the cancellation outcome. Reporting
                    // FAILED here can overwrite its CANCELLED state.
                    return;
                };

                if let Err(error) = transport
                    .send_step_action_event(proto::dispatcher::StepActionEvent {
                        worker_id: worker_id_clone,
                        job_id: action.job_id,
                        job_run_id: action.job_run_id,
                        task_id: action.task_id,
                        task_run_external_id: task_run_id.clone(),
                        action_id: action.action_id,
                        event_timestamp: Some(prost_now()),
                        event_type: event_type as i32,
                        event_payload: payload,
                        retry_count: None,
                        should_not_retry,
                        retry_after_ms,
                    })
                    .await
                {
                    let _ = lifecycle_failure_tx.send(format!(
                        "failed to report completion for task run '{task_run_id}': {error}"
                    ));
                }
            });
            Ok(())
        } else {
            Err(Error::Worker(format!(
                "engine assigned unregistered task '{task_name}'"
            )))
        }
    }

    async fn supported_retry_after(&self, delay: Option<i64>) -> Option<i64> {
        let delay = delay?;
        // Version discovery must never prevent the failure from reaching the
        // engine. Bound the entire retrying probe, including a stalled RPC.
        match tokio::time::timeout(std::time::Duration::from_secs(2), self.engine_version()).await {
            Ok(Ok(version)) if version.supports(crate::EngineFeature::RetryAfter) => Some(delay),
            version => {
                eprintln!(
                    "RetryAfterError requires engine >= 0.111.0 (connected: {version:?}); using the configured retry policy instead of the requested delay"
                );
                None
            }
        }
    }

    async fn batch_event(
        &self,
        action: &proto::dispatcher::AssignedAction,
        worker_id: &str,
        event: proto::dispatcher::StepActionEventType,
        mut items: Vec<proto::dispatcher::BatchActionEventItem>,
    ) -> Result<()> {
        if let Some(delay) = items.iter().find_map(|item| item.retry_after_ms) {
            if self.supported_retry_after(Some(delay)).await.is_none() {
                for item in &mut items {
                    item.retry_after_ms = None;
                }
            }
        }
        self.dispatcher
            .clone()
            .send_batch_action_event(self.authed_request(proto::dispatcher::BatchActionEvent {
                worker_id: worker_id.to_owned(),
                job_id: action.job_id.clone(),
                action_id: action.action_id.clone(),
                batch_id: action.batch_id.clone(),
                event_timestamp: Some(prost_now()),
                event_type: event as i32,
                items,
            }))
            .await
            .map_err(grpc_error)?;
        Ok(())
    }

    async fn handle_assigned_batch(
        &self,
        action: proto::dispatcher::AssignedAction,
        worker_id: &str,
        opts: &WorkerStartOpts,
        failure_tx: tokio::sync::mpsc::UnboundedSender<String>,
    ) -> Result<()> {
        use proto::dispatcher::{BatchActionEventItem as Item, StepActionEventType as Event};
        let inputs = crate::batch::decode(&action.action_payload)?;
        let member_ids = inputs.keys().cloned().collect::<Vec<_>>();
        let name = resolve_action_task_name(&action, opts);
        let dispatcher = opts
            .task_dispatchers
            .get(&name)
            .cloned()
            .ok_or_else(|| Error::Worker(format!("unregistered batch action {name}")))?;
        let batch_id = action
            .batch_id
            .clone()
            .filter(|id| !id.is_empty())
            .ok_or_else(|| Error::Worker("batch assignment omitted batch ID".into()))?;
        self.batch_event(
            &action,
            worker_id,
            Event::StepEventTypeStarted,
            member_ids
                .iter()
                .map(|id| Item {
                    task_run_external_id: id.clone(),
                    ..Default::default()
                })
                .collect(),
        )
        .await?;
        let group = Arc::new(tokio_util::sync::CancellationToken::new());
        let tokens = member_ids
            .iter()
            .map(|id| {
                (
                    id.clone(),
                    Arc::new(tokio_util::sync::CancellationToken::new()),
                )
            })
            .collect::<Vec<_>>();
        {
            let mut running = self.running_cancellations.lock().await;
            running.insert(batch_id.clone(), Arc::clone(&group));
            for (id, token) in &tokens {
                running.insert(id.clone(), Arc::clone(token));
            }
        }
        let transport = self.clone();
        let worker_id = worker_id.to_owned();
        tokio::spawn(async move {
            let watcher = {
                let tokens = tokens.clone();
                let group = Arc::clone(&group);
                tokio::spawn(async move {
                    loop {
                        if tokens.iter().all(|(_, token)| token.is_cancelled()) {
                            group.cancel();
                            break;
                        }
                        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
                    }
                })
            };
            let ids = crate::ContextIds {
                workflow_name: action.job_name.clone(),
                task_name: name.clone(),
                workflow_run_id: WorkflowRunId(action.workflow_run_id.clone()),
                task_run_id: batch_id.clone(),
                workflow_id: action.workflow_id.clone(),
                workflow_version_id: action.workflow_version_id.clone(),
            };
            let context = TaskContextData {
                cancel_token: Some(group.as_ref().clone()),
                worker_id: Some(worker_id.clone()),
                batch_member_ids: member_ids.clone(),
                run_opts: RunOpts {
                    additional_metadata: action
                        .additional_metadata
                        .as_deref()
                        .and_then(|m| serde_json::from_str(m).ok())
                        .unwrap_or_default(),
                    ..Default::default()
                },
                ..Default::default()
            };
            let input = Value::Object(inputs.into_iter().collect());
            let result = dispatcher
                .run_task_json_with_context(&name, input, ids, context)
                .await
                .and_then(|value| crate::batch::outputs(&member_ids, value, false));
            watcher.abort();
            let mut completed = Vec::new();
            let mut failed = Vec::new();
            if !group.is_cancelled() {
                for (id, token) in &tokens {
                    if token.is_cancelled() {
                        continue;
                    }
                    match &result {
                        Ok(values) => completed.push(Item {
                            task_run_external_id: id.clone(),
                            event_payload: values[id].to_string(),
                            ..Default::default()
                        }),
                        Err(error) => failed.push(Item {
                            task_run_external_id: id.clone(),
                            event_payload: serde_json::json!({"message":error.to_string()})
                                .to_string(),
                            should_not_retry: Some(!error.is_retryable()),
                            retry_after_ms: match error {
                                Error::RetryAfter(e) => Some(e.milliseconds()),
                                _ => None,
                            },
                            ..Default::default()
                        }),
                    }
                }
            }
            for (event, items) in [
                (Event::StepEventTypeCompleted, completed),
                (Event::StepEventTypeFailed, failed),
            ] {
                if !items.is_empty() {
                    if let Err(error) = transport
                        .batch_event(&action, &worker_id, event, items)
                        .await
                    {
                        let _ =
                            failure_tx.send(format!("failed reporting batch completion: {error}"));
                    }
                }
            }
            let mut running = transport.running_cancellations.lock().await;
            for (id, token) in tokens.into_iter().chain(std::iter::once((batch_id, group))) {
                if running
                    .get(&id)
                    .is_some_and(|current| Arc::ptr_eq(current, &token))
                {
                    running.remove(&id);
                }
            }
        });
        Ok(())
    }

    /// Polls the engine for run completion by calling `GetRunDetails` repeatedly.
    async fn poll_run_result(&self, run_id: &WorkflowRunId) -> Result<Value> {
        let poll_interval = std::time::Duration::from_millis(500);
        let mut graph = None;

        loop {
            let details = self.get_run_details(&run_id.0).await?;

            // Failure and cancellation do not need graph metadata. Preserve the
            // engine outcome even when the REST read model is unavailable.
            if details.done {
                match proto::v1::RunStatus::try_from(details.status) {
                    Ok(proto::v1::RunStatus::Failed) => {
                        let message = details
                            .task_runs
                            .values()
                            .find_map(|task| task.error.clone())
                            .unwrap_or_else(|| "workflow run failed".to_owned());
                        return Err(Error::Transport(message));
                    }
                    Ok(proto::v1::RunStatus::Cancelled) => {
                        return Err(Error::Transport("workflow run was cancelled".to_owned()));
                    }
                    _ => {}
                }
            }

            if graph.is_none() {
                graph = Some(
                    self.rest_api_client
                        .run_graph(&run_id.0, &self.retry_config)
                        .await?,
                );
            }
            // v0.94.14 can leave an unused failure handler queued after the
            // normal graph completes. It must not keep a successful run waiting.
            if !details.done
                && graph
                    .as_ref()
                    .is_some_and(|graph| graph.on_failure_task.is_some())
            {
                if let Some(output) = self
                    .rest_api_client
                    .completed_main_output(&run_id.0, &self.retry_config)
                    .await?
                {
                    return Ok(output);
                }
            }
            if details.done && details.status == proto::v1::RunStatus::Completed as i32 {
                // Use the graph snapshot attached to this run, rather than
                // registration state that may be absent or belong to a newer version.
                return Ok(run_output_from_details(
                    &details.task_runs,
                    graph.as_ref().map(|graph| graph.output_tasks.as_slice()),
                ));
            }

            tokio::time::sleep(poll_interval).await;
        }
    }
}

fn resolve_action_task_name(
    action: &proto::dispatcher::AssignedAction,
    opts: &crate::transport::WorkerStartOpts,
) -> String {
    if opts.task_dispatchers.contains_key(&action.action_id)
        || opts
            .durable_task_dispatchers
            .contains_key(&action.action_id)
    {
        return action.action_id.clone();
    }
    // START_BATCH carries only the action ID; unlike START_STEP_RUN it omits
    // task_name. Resolve the same default integration prefix we advertised.
    if let Some(name) = action.action_id.strip_prefix("default:") {
        if opts.task_dispatchers.contains_key(name)
            || opts.durable_task_dispatchers.contains_key(name)
        {
            return name.to_owned();
        }
    }
    action.task_name.clone()
}

#[derive(Debug, Default)]
struct DecodedStepActionPayload {
    input: Value,
    parent_outputs: HashMap<String, Value>,
    trigger_data: TriggerData,
    filter_payload: Option<Value>,
    errors: HashMap<String, String>,
}

fn decode_step_action_payload(payload: &str) -> DecodedStepActionPayload {
    let decoded = serde_json::from_str::<Value>(payload).unwrap_or(Value::Null);
    let Some(object) = decoded.as_object() else {
        return DecodedStepActionPayload {
            input: decoded,
            ..DecodedStepActionPayload::default()
        };
    };
    let Some(input) = object.get("input") else {
        return DecodedStepActionPayload {
            input: decoded,
            ..DecodedStepActionPayload::default()
        };
    };
    let parent_outputs = object
        .get("parents")
        .and_then(Value::as_object)
        .map(|parents| {
            parents
                .iter()
                .map(|(name, output)| (name.clone(), output.clone()))
                .collect()
        })
        .unwrap_or_default();
    let triggered_by = object
        .get("triggered_by")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let triggers = object.get("triggers").and_then(Value::as_object);
    let filter_payload = triggers
        .and_then(|values| values.get("filter_payload"))
        .cloned();
    let errors = object
        .get("step_run_errors")
        .cloned()
        .and_then(|value| serde_json::from_value(value).ok())
        .unwrap_or_default();
    let trigger_data = TriggerData {
        triggered_by_cron: triggered_by == "cron",
        triggered_by_event: triggered_by == "event",
        cron_expression: nested_trigger_string(triggers, "cron", "expression"),
        event_key: nested_trigger_string(triggers, "event", "key"),
        scheduled_at: nested_trigger_string(triggers, "schedule", "trigger_at"),
    };

    DecodedStepActionPayload {
        input: input.clone(),
        parent_outputs,
        trigger_data,
        filter_payload,
        errors,
    }
}

fn nested_trigger_string(
    triggers: Option<&serde_json::Map<String, Value>>,
    trigger: &str,
    field: &str,
) -> Option<String> {
    triggers?
        .get(trigger)?
        .get(field)?
        .as_str()
        .map(ToOwned::to_owned)
}

fn infer_trigger_data_from_metadata(
    trigger_data: &mut TriggerData,
    metadata: &HashMap<String, String>,
) {
    // Hatchet v1 engines preserve top-level event/cron origin in internal run metadata, while
    // some versions serialize `triggered_by: "manual"` in the task action payload. Prefer the
    // richer payload when present, but recover the public context fields from the canonical
    // metadata keys used by the engine and official SDKs.
    if let Some(event_key) = metadata.get("hatchet__event_key") {
        trigger_data.triggered_by_event = true;
        trigger_data
            .event_key
            .get_or_insert_with(|| event_key.to_owned());
    }
    if let Some(cron_expression) = metadata.get("hatchet__cron_expression") {
        trigger_data.triggered_by_cron = true;
        trigger_data
            .cron_expression
            .get_or_insert_with(|| cron_expression.to_owned());
    }
}

fn worker_action_id(task_name: &str) -> String {
    format!("default:{task_name}")
}

fn run_output_from_details(
    task_runs: &HashMap<String, proto::v1::TaskRunDetail>,
    output_tasks: Option<&[String]>,
) -> Value {
    if let Some(output_tasks) = output_tasks {
        let single_output = (output_tasks.len() == 1)
            .then(|| find_task_output(task_runs, &output_tasks[0]))
            .flatten();
        if let Some(output) = single_output {
            return output;
        }

        let output_map = task_outputs_by_name(
            output_tasks
                .iter()
                .filter_map(|task_name| find_task_detail(task_runs, task_name)),
        );
        if !output_map.is_empty() {
            return Value::Object(output_map);
        }
    }

    let single_output = (task_runs.len() == 1)
        .then(|| task_runs.values().find_map(parse_task_output))
        .flatten();
    if let Some(output) = single_output {
        return output;
    }

    Value::Object(task_outputs_by_name(task_runs.values()))
}

fn find_task_output(
    task_runs: &HashMap<String, proto::v1::TaskRunDetail>,
    task_name: &str,
) -> Option<Value> {
    find_task_detail(task_runs, task_name).and_then(parse_task_output)
}

fn find_task_detail<'a>(
    task_runs: &'a HashMap<String, proto::v1::TaskRunDetail>,
    task_name: &str,
) -> Option<&'a proto::v1::TaskRunDetail> {
    task_runs
        .values()
        .find(|task_detail| task_detail.readable_id == task_name)
}

fn task_outputs_by_name<'a>(
    task_details: impl IntoIterator<Item = &'a proto::v1::TaskRunDetail>,
) -> serde_json::Map<String, Value> {
    let mut outputs: Vec<_> = task_details
        .into_iter()
        .filter_map(|task_detail| {
            parse_task_output(task_detail).map(|output| (task_detail.readable_id.clone(), output))
        })
        .collect();
    outputs.sort_by(|(left, _), (right, _)| left.cmp(right));
    outputs.into_iter().collect()
}

fn parse_task_output(task_detail: &proto::v1::TaskRunDetail) -> Option<Value> {
    task_detail
        .output
        .as_deref()
        .and_then(|output| serde_json::from_slice(output).ok())
}

fn completion_event_from_result(
    result: Result<Value>,
    cancelled: bool,
) -> Option<(
    proto::dispatcher::StepActionEventType,
    String,
    Option<bool>,
    Option<i64>,
)> {
    if cancelled {
        return None;
    }

    Some(match result {
        Ok(val) => (
            proto::dispatcher::StepActionEventType::StepEventTypeCompleted,
            serde_json::to_string(&val).unwrap_or_default(),
            None,
            None,
        ),
        Err(error) => {
            let should_not_retry = !error.is_retryable();
            (
                proto::dispatcher::StepActionEventType::StepEventTypeFailed,
                error.to_string(),
                Some(should_not_retry),
                match error {
                    Error::RetryAfter(e) => Some(e.milliseconds()),
                    _ => None,
                },
            )
        }
    })
}

pub(crate) fn run_opt_label_to_proto(
    label: &another_hatchet_core::DesiredWorkerLabelConfig,
) -> proto::v1::DesiredWorkerLabels {
    match label {
        another_hatchet_core::DesiredWorkerLabelConfig::Legacy(value) => {
            proto::v1::DesiredWorkerLabels {
                str_value: Some(value.clone()),
                int_value: None,
                required: None,
                comparator: None,
                weight: None,
            }
        }
        another_hatchet_core::DesiredWorkerLabelConfig::Rich(config) => {
            let comparator = config.comparator.map(|comparator| match comparator {
                another_hatchet_core::WorkerLabelComparator::Equal => {
                    proto::v1::WorkerLabelComparator::Equal
                }
                another_hatchet_core::WorkerLabelComparator::NotEqual => {
                    proto::v1::WorkerLabelComparator::NotEqual
                }
                another_hatchet_core::WorkerLabelComparator::GreaterThan => {
                    proto::v1::WorkerLabelComparator::GreaterThan
                }
                another_hatchet_core::WorkerLabelComparator::GreaterThanOrEqual => {
                    proto::v1::WorkerLabelComparator::GreaterThanOrEqual
                }
                another_hatchet_core::WorkerLabelComparator::LessThan => {
                    proto::v1::WorkerLabelComparator::LessThan
                }
                another_hatchet_core::WorkerLabelComparator::LessThanOrEqual => {
                    proto::v1::WorkerLabelComparator::LessThanOrEqual
                }
            });

            proto::v1::DesiredWorkerLabels {
                str_value: Some(config.value.clone()),
                int_value: None,
                required: Some(config.required),
                comparator: comparator.map(|value| value as i32),
                weight: config.weight.map(u32_to_i32_saturating),
            }
        }
    }
}

fn serialize_input_schema(schema: Option<&Value>) -> Option<Vec<u8>> {
    schema.and_then(|value| serde_json::to_vec(value).ok())
}

// --- Conversion helpers ---

#[derive(prost::Message)]
struct RpcStatusDetails {
    #[prost(int32, tag = "1")]
    code: i32,
    #[prost(string, tag = "2")]
    message: String,
    #[prost(message, repeated, tag = "3")]
    details: Vec<prost_types::Any>,
}
fn collision(detail: proto::v1::IdempotencyCollisionError) -> Option<crate::IdempotencyCollision> {
    (!detail.existing_run_external_id.is_empty()).then(|| crate::IdempotencyCollision {
        existing_run_id: WorkflowRunId(detail.existing_run_external_id),
        colliding_run_id: (!detail.colliding_run_external_id.is_empty())
            .then_some(WorkflowRunId(detail.colliding_run_external_id)),
    })
}
fn grpc_error(status: Status) -> Error {
    if status.code() == tonic::Code::AlreadyExists {
        use prost::Message;
        if let Ok(envelope) = RpcStatusDetails::decode(status.details()) {
            for detail in envelope.details {
                if detail
                    .type_url
                    .ends_with("/v1.BulkTriggerIdempotencyCollisionError")
                {
                    if let Ok(result) = proto::v1::BulkTriggerIdempotencyCollisionError::decode(
                        detail.value.as_slice(),
                    ) {
                        let Some(collisions) = result
                            .collisions
                            .into_iter()
                            .map(collision)
                            .collect::<Option<Vec<_>>>()
                        else {
                            continue;
                        };
                        if collisions.is_empty() {
                            continue;
                        }
                        return Error::BulkIdempotencyCollision(crate::BulkIdempotencyCollision {
                            successful_run_ids: result
                                .successful_workflow_run_external_ids
                                .into_iter()
                                .map(WorkflowRunId)
                                .collect(),
                            collisions,
                        });
                    }
                } else if detail.type_url.ends_with("/v1.IdempotencyCollisionError") {
                    if let Ok(result) =
                        proto::v1::IdempotencyCollisionError::decode(detail.value.as_slice())
                    {
                        if let Some(result) = collision(result) {
                            return Error::IdempotencyCollision(result);
                        }
                    }
                }
            }
        }
        return Error::DedupeViolation(status.message().to_owned());
    }
    Error::Transport(format!(
        "gRPC error: {} ({})",
        status.message(),
        status.code()
    ))
}

fn build_rest_client(config: &HatchetConfig) -> Result<reqwest::Client> {
    let mut headers = HeaderMap::new();
    let auth = format!("Bearer {}", config.token());
    let auth_value = HeaderValue::from_str(&auth)
        .map_err(|error| Error::Transport(format!("invalid auth header: {error}")))?;
    headers.insert(AUTHORIZATION, auth_value);

    reqwest::Client::builder()
        .default_headers(headers)
        .build()
        .map_err(|error| Error::Transport(format!("failed to build REST client: {error}")))
}

fn workflow_event_payload_to_json(event: &proto::dispatcher::WorkflowEvent) -> Value {
    serde_json::from_str::<Value>(&event.event_payload)
        .unwrap_or_else(|_| serde_json::json!({ "event_payload": event.event_payload }))
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct V1LogLine {
    message: String,
    #[serde(default)]
    level: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct V1LogLineList {
    #[serde(default)]
    rows: Vec<V1LogLine>,
}

fn prost_now() -> prost_types::Timestamp {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    prost_types::Timestamp {
        seconds: now.as_secs() as i64,
        nanos: now.subsec_nanos() as i32,
    }
}

pub(crate) fn priority_to_i32(priority: another_hatchet_core::Priority) -> i32 {
    match priority {
        another_hatchet_core::Priority::Low => 1,
        another_hatchet_core::Priority::Normal => 2,
        another_hatchet_core::Priority::High => 3,
    }
}

fn priority_from_i32(priority: i32) -> Option<another_hatchet_core::Priority> {
    match priority {
        1 => Some(another_hatchet_core::Priority::Low),
        2 => Some(another_hatchet_core::Priority::Normal),
        3 => Some(another_hatchet_core::Priority::High),
        _ => None,
    }
}

/// Converts a `TaskRegistration` into a protobuf `CreateTaskOpts`.
fn rendered_to_proto_task(
    task: &crate::transport::TaskRegistration,
    _registration: &WorkflowRegistration,
) -> proto::v1::CreateTaskOpts {
    let conditions = rendered_conditions_to_proto(&task.conditions);

    proto::v1::CreateTaskOpts {
        readable_id: task.readable_id.clone(),
        action: task.action.clone(),
        timeout: task.timeout.map_or(String::new(), |t| {
            duration_to_string(std::time::Duration::from_secs(t))
        }),
        inputs: String::new(),
        parents: task.parents.clone(),
        retries: if task.batch.is_some() {
            0
        } else {
            task.retries.map_or(0, u32_to_i32_saturating)
        },
        batch: task.batch.as_ref().map(|b| proto::v1::TaskBatchConfig {
            batch_max_size: b.max_size as i32,
            batch_max_interval_ms: b.max_interval.map(|d| d.as_millis() as i32),
            batch_group_key: b.group_key.clone(),
            batch_group_max_runs: b.group_max_runs.map(|n| n as i32),
            broadcast_output: Some(b.broadcast_output),
        }),
        rate_limits: task.rate_limits.iter().map(rate_limit_to_proto).collect(),
        worker_labels: task
            .worker_labels
            .iter()
            .map(|(key, label)| (key.clone(), desired_worker_label_to_proto(label)))
            .collect(),
        backoff_factor: task.backoff_factor.map(|factor| factor as f32),
        backoff_max_seconds: task.backoff_max_seconds.map(u64_to_i32_saturating),
        concurrency: task
            .concurrency
            .as_ref()
            .map_or_else(Vec::new, |concurrency| {
                vec![concurrency_to_proto(concurrency)]
            }),
        conditions: if conditions.parent_override_conditions.is_empty()
            && conditions.sleep_conditions.is_empty()
            && conditions.user_event_conditions.is_empty()
        {
            None
        } else {
            Some(conditions)
        },
        schedule_timeout: task
            .schedule_timeout
            .map(|t| duration_to_string(std::time::Duration::from_secs(t))),
        is_durable: task.is_durable,
        slot_requests: task
            .slot_requests
            .iter()
            .map(|(slot_type, units)| {
                (
                    slot_type.as_engine_key().to_owned(),
                    u32_to_i32_saturating(*units),
                )
            })
            .collect(),
    }
}

fn rate_limit_to_proto(rate_limit: &crate::task::RateLimit) -> proto::v1::CreateTaskRateLimit {
    let (key, key_expr) = match &rate_limit.key {
        crate::task::RateLimitKey::None => ("default".to_owned(), None),
        crate::task::RateLimitKey::Static(key) => (key.clone(), None),
        crate::task::RateLimitKey::Dynamic(expr) => ("dynamic".to_owned(), Some(expr.clone())),
    };

    let duration = rate_limit_duration_to_v1_proto(rate_limit.duration);

    proto::v1::CreateTaskRateLimit {
        key,
        units: Some(u32_to_i32_saturating(rate_limit.units)),
        key_expr,
        units_expr: None,
        limit_values_expr: Some(rate_limit.limit.to_string()),
        duration: Some(duration as i32),
    }
}

fn rate_limit_duration_to_v1_proto(
    duration: crate::task::RateLimitDuration,
) -> proto::v1::RateLimitDuration {
    match duration {
        crate::task::RateLimitDuration::Second => proto::v1::RateLimitDuration::Second,
        crate::task::RateLimitDuration::Minute => proto::v1::RateLimitDuration::Minute,
        crate::task::RateLimitDuration::Hour => proto::v1::RateLimitDuration::Hour,
        crate::task::RateLimitDuration::Day => proto::v1::RateLimitDuration::Day,
        crate::task::RateLimitDuration::Week => proto::v1::RateLimitDuration::Week,
        crate::task::RateLimitDuration::Month => proto::v1::RateLimitDuration::Month,
        crate::task::RateLimitDuration::Year => proto::v1::RateLimitDuration::Year,
    }
}

fn rate_limit_duration_to_legacy_proto(
    duration: crate::task::RateLimitDuration,
) -> proto::workflows::RateLimitDuration {
    match duration {
        crate::task::RateLimitDuration::Second => proto::workflows::RateLimitDuration::Second,
        crate::task::RateLimitDuration::Minute => proto::workflows::RateLimitDuration::Minute,
        crate::task::RateLimitDuration::Hour => proto::workflows::RateLimitDuration::Hour,
        crate::task::RateLimitDuration::Day => proto::workflows::RateLimitDuration::Day,
        crate::task::RateLimitDuration::Week => proto::workflows::RateLimitDuration::Week,
        crate::task::RateLimitDuration::Month => proto::workflows::RateLimitDuration::Month,
        crate::task::RateLimitDuration::Year => proto::workflows::RateLimitDuration::Year,
    }
}

fn desired_worker_label_to_proto(
    desired: &crate::task::DesiredLabelConfig,
) -> proto::v1::DesiredWorkerLabels {
    let (str_value, int_value) = match desired.value {
        crate::task::LabelValue::String(ref value) => (Some(value.clone()), None),
        crate::task::LabelValue::Int(value) => {
            if let Ok(int_value) = i32::try_from(value) {
                (None, Some(int_value))
            } else {
                (Some(value.to_string()), None)
            }
        }
        crate::task::LabelValue::Bool(value) => (Some(value.to_string()), None),
    };

    let comparator = match desired.comparator {
        crate::task::WorkerLabelComparator::Equal => proto::v1::WorkerLabelComparator::Equal,
        crate::task::WorkerLabelComparator::NotEqual => proto::v1::WorkerLabelComparator::NotEqual,
        crate::task::WorkerLabelComparator::GreaterThan => {
            proto::v1::WorkerLabelComparator::GreaterThan
        }
        crate::task::WorkerLabelComparator::LessThan => proto::v1::WorkerLabelComparator::LessThan,
        crate::task::WorkerLabelComparator::GreaterThanOrEqual => {
            proto::v1::WorkerLabelComparator::GreaterThanOrEqual
        }
        crate::task::WorkerLabelComparator::LessThanOrEqual => {
            proto::v1::WorkerLabelComparator::LessThanOrEqual
        }
    };

    proto::v1::DesiredWorkerLabels {
        str_value,
        int_value,
        required: Some(desired.required),
        comparator: Some(comparator as i32),
        weight: desired.weight.map(u32_to_i32_saturating),
    }
}

fn concurrency_to_proto(concurrency: &crate::task::ConcurrencyConfig) -> proto::v1::Concurrency {
    let limit_strategy = concurrency
        .limit_strategy
        .as_ref()
        .map(|strategy| match strategy {
            crate::task::ConcurrencyLimitStrategy::CancelInProgress => {
                proto::v1::ConcurrencyLimitStrategy::CancelInProgress
            }
            crate::task::ConcurrencyLimitStrategy::CancelNewest => {
                proto::v1::ConcurrencyLimitStrategy::CancelNewest
            }
            crate::task::ConcurrencyLimitStrategy::GroupRoundRobin => {
                proto::v1::ConcurrencyLimitStrategy::GroupRoundRobin
            }
        });

    proto::v1::Concurrency {
        expression: concurrency.expression.clone(),
        max_runs: concurrency.max_runs.map(u32_to_i32_saturating),
        limit_strategy: limit_strategy.map(|strategy| strategy as i32),
    }
}

fn u32_to_i32_saturating(value: u32) -> i32 {
    value.min(i32::MAX as u32) as i32
}

fn u64_to_i32_saturating(value: u64) -> i32 {
    value.min(i32::MAX as u64) as i32
}

/// Converts `RenderedConditions` into the protobuf `TaskConditions`.
fn rendered_conditions_to_proto(conditions: &RenderedConditions) -> proto::v1::TaskConditions {
    let sleep_conditions =
        conditions
            .sleep_conditions
            .iter()
            .map(|c| proto::v1::SleepMatchCondition {
                base: Some(base_to_proto(&c.base)),
                sleep_for: duration_to_string(c.duration),
            })
            .chain(conditions.sleep_until_conditions.iter().map(|c| {
                proto::v1::SleepMatchCondition {
                    base: Some(base_to_proto(&c.base)),
                    sleep_for: system_time_to_iso(&c.at),
                }
            }))
            .collect();

    let user_event_conditions = conditions
        .user_event_conditions
        .iter()
        .map(|c| {
            let consider_events_since = c.lookback_window.map(|window| {
                let since = SystemTime::now()
                    .checked_sub(window)
                    .unwrap_or(SystemTime::UNIX_EPOCH);
                let dur = since
                    .duration_since(SystemTime::UNIX_EPOCH)
                    .unwrap_or_default();
                prost_types::Timestamp {
                    seconds: dur.as_secs() as i64,
                    nanos: dur.subsec_nanos() as i32,
                }
            });

            proto::v1::UserEventMatchCondition {
                base: Some(base_to_proto(&c.base)),
                user_event_key: c.event_key.clone(),
                event_scope: c.scope.clone(),
                consider_events_since,
            }
        })
        .collect();

    let parent_override_conditions = conditions
        .parent_override_conditions
        .iter()
        .map(|c| proto::v1::ParentOverrideMatchCondition {
            base: Some(base_to_proto(&c.base)),
            parent_readable_id: c.parent_readable_id.clone(),
        })
        .collect();

    proto::v1::TaskConditions {
        sleep_conditions,
        user_event_conditions,
        parent_override_conditions,
    }
}

fn base_to_proto(base: &crate::durable::BaseMatchCondition) -> proto::v1::BaseMatchCondition {
    proto::v1::BaseMatchCondition {
        readable_data_key: base.readable_data_key.clone(),
        action: match base.action {
            crate::durable::ConditionAction::Create => proto::v1::Action::Create.into(),
            crate::durable::ConditionAction::Queue => proto::v1::Action::Queue.into(),
            crate::durable::ConditionAction::Cancel => proto::v1::Action::Cancel.into(),
            crate::durable::ConditionAction::Skip => proto::v1::Action::Skip.into(),
        },
        or_group_id: base.or_group_id.clone(),
        expression: base.expression.clone(),
    }
}

/// Converts a `Duration` to the engine's expected string format (e.g. `10s`, `5m`, `1h30m`).
pub fn duration_to_string(d: std::time::Duration) -> String {
    let total_secs = d.as_secs();
    if total_secs == 0 {
        return "0s".to_owned();
    }

    let hours = total_secs / 3600;
    let mins = (total_secs % 3600) / 60;
    let secs = total_secs % 60;

    let mut result = String::new();
    if hours > 0 {
        result.push_str(&format!("{hours}h"));
    }
    if mins > 0 {
        result.push_str(&format!("{mins}m"));
    }
    if secs > 0 || result.is_empty() {
        result.push_str(&format!("{secs}s"));
    }
    result
}

/// Converts a `SystemTime` to an ISO 8601 timestamp string.
fn system_time_to_iso(time: &SystemTime) -> String {
    let dur = time
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = dur.as_secs();
    let nanos = dur.subsec_nanos();

    // Simple ISO 8601 formatting without external deps
    let days_since_epoch = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;

    // Calculate date from days since epoch (simplified — correct for 1970-2099)
    let (year, month, day) = days_to_ymd(days_since_epoch);

    format!(
        "{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}.{millis:03}Z",
        millis = nanos / 1_000_000,
    )
}

/// Converts days since Unix epoch to (year, month, day).
fn days_to_ymd(days: u64) -> (u64, u64, u64) {
    // Algorithm from Howard Hinnant's `chrono`-compatible date calculation
    let z = days + 719468;
    let era = z / 146097;
    let doe = z - era * 146097;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    (y, m, d)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{
        V1LogLineList, WorkflowRegistration, completion_event_from_result,
        decode_step_action_payload, infer_trigger_data_from_metadata,
        rate_limit_duration_to_legacy_proto, rate_limit_duration_to_v1_proto,
        rendered_to_proto_task, resolve_action_task_name, run_opt_label_to_proto,
        run_output_from_details, serialize_input_schema, worker_action_id,
        workflow_event_payload_to_json,
    };
    use crate::task::{
        BackoffConfig, ConcurrencyConfig, ConcurrencyLimitStrategy, DesiredLabelConfig, LabelValue,
        RateLimit, RateLimitDuration, SlotType, TaskConfig, WorkerLabelComparator,
    };
    use crate::workflow::WorkflowDeclaration;
    use serde_json::Value;

    fn task_run_detail(readable_id: &str, output: Value) -> crate::proto::v1::TaskRunDetail {
        crate::proto::v1::TaskRunDetail {
            external_id: format!("{readable_id}-run"),
            status: crate::proto::v1::RunStatus::Completed as i32,
            error: None,
            output: Some(serde_json::to_vec(&output).expect("test output should serialize")),
            readable_id: readable_id.to_owned(),
            is_evicted: false,
        }
    }

    #[test]
    fn workflow_registration_from_declaration_carries_task_parity_fields() {
        let task_config = TaskConfig::builder()
            .retries(3)
            .backoff(BackoffConfig {
                factor: 2.25,
                max_seconds: 77,
            })
            .rate_limit(RateLimit::new(2, 30, RateLimitDuration::Minute).with_static_key("api"))
            .desired_label(
                "region",
                DesiredLabelConfig::new(LabelValue::String("us-east".to_owned()))
                    .required(true)
                    .weight(55)
                    .comparator(WorkerLabelComparator::Equal),
            )
            .concurrency(
                ConcurrencyConfig::new("input.customer_id")
                    .max_runs(8)
                    .limit_strategy(ConcurrencyLimitStrategy::GroupRoundRobin),
            )
            .slot_request(SlotType::Default, 2)
            .slot_request(SlotType::Durable, 5)
            .build();

        let declaration = WorkflowDeclaration::<Value, Value>::new("payments").step(
            &crate::Client::new(std::sync::Arc::new(crate::InMemoryTransport::new()))
                .task::<Value, Value>("charge"),
            task_config.clone(),
        );

        let registration = WorkflowRegistration::from_declaration(&declaration, None);
        assert_eq!(registration.tasks.len(), 1);
        let task = &registration.tasks[0];

        assert_eq!(task.rate_limits, task_config.rate_limits);
        assert_eq!(task.worker_labels, task_config.desired_worker_labels);
        assert_eq!(task.backoff_factor, Some(2.25));
        assert_eq!(task.backoff_max_seconds, Some(77));
        assert_eq!(task.concurrency, task_config.concurrency);
        assert_eq!(task.slot_requests, task_config.slot_requests);
    }

    #[test]
    fn rendered_to_proto_task_maps_rate_limits_labels_concurrency_backoff_and_slots() {
        let dynamic_limit =
            RateLimit::new(4, 90, RateLimitDuration::Hour).with_dynamic_key("input.account_id");
        let task_config = TaskConfig::builder()
            .retries(2)
            .backoff(BackoffConfig {
                factor: 1.5,
                max_seconds: 42,
            })
            .rate_limit(RateLimit::new(1, 20, RateLimitDuration::Second).with_static_key("burst"))
            .rate_limit(dynamic_limit.clone())
            .desired_label(
                "score",
                DesiredLabelConfig::new(LabelValue::Int(7))
                    .required(false)
                    .weight(12)
                    .comparator(WorkerLabelComparator::GreaterThanOrEqual),
            )
            .concurrency(
                ConcurrencyConfig::new("input.org")
                    .max_runs(3)
                    .limit_strategy(ConcurrencyLimitStrategy::CancelNewest),
            )
            .slot_request(SlotType::Default, 6)
            .slot_request(SlotType::Durable, 1)
            .build();

        let declaration = WorkflowDeclaration::<Value, Value>::new("billing").step(
            &crate::Client::new(std::sync::Arc::new(crate::InMemoryTransport::new()))
                .task::<Value, Value>("charge"),
            task_config,
        );

        let registration = WorkflowRegistration::from_declaration(&declaration, None);
        let task = &registration.tasks[0];
        let proto = rendered_to_proto_task(task, &registration);

        assert_eq!(proto.rate_limits.len(), 2);
        assert_eq!(proto.rate_limits[0].key, "burst");
        assert_eq!(proto.rate_limits[0].units, Some(1));
        assert_eq!(
            proto.rate_limits[0].limit_values_expr.as_deref(),
            Some("20")
        );
        assert_eq!(
            proto.rate_limits[0].duration,
            Some(crate::proto::v1::RateLimitDuration::Second as i32)
        );
        assert_eq!(proto.rate_limits[1].key, "dynamic");
        assert_eq!(
            proto.rate_limits[1].key_expr.as_deref(),
            Some("input.account_id")
        );
        assert_eq!(
            proto.rate_limits[1].duration,
            Some(crate::proto::v1::RateLimitDuration::Hour as i32)
        );

        let score_label = proto
            .worker_labels
            .get("score")
            .expect("score label should be present");
        assert_eq!(score_label.int_value, Some(7));
        assert_eq!(score_label.required, Some(false));
        assert_eq!(score_label.weight, Some(12));
        assert_eq!(
            score_label.comparator,
            Some(crate::proto::v1::WorkerLabelComparator::GreaterThanOrEqual as i32)
        );

        assert_eq!(proto.backoff_factor, Some(1.5));
        assert_eq!(proto.backoff_max_seconds, Some(42));
        assert_eq!(proto.concurrency.len(), 1);
        assert_eq!(proto.concurrency[0].expression, "input.org");
        assert_eq!(proto.concurrency[0].max_runs, Some(3));
        assert_eq!(
            proto.concurrency[0].limit_strategy,
            Some(crate::proto::v1::ConcurrencyLimitStrategy::CancelNewest as i32)
        );
        assert_eq!(proto.slot_requests.get("default").copied(), Some(6));
        assert_eq!(proto.slot_requests.get("durable").copied(), Some(1));
    }

    #[test]
    fn rate_limit_duration_mapping_includes_month_and_year() {
        assert_eq!(
            rate_limit_duration_to_v1_proto(RateLimitDuration::Month),
            crate::proto::v1::RateLimitDuration::Month
        );
        assert_eq!(
            rate_limit_duration_to_v1_proto(RateLimitDuration::Year),
            crate::proto::v1::RateLimitDuration::Year
        );
        assert_eq!(
            rate_limit_duration_to_legacy_proto(RateLimitDuration::Month),
            crate::proto::workflows::RateLimitDuration::Month
        );
        assert_eq!(
            rate_limit_duration_to_legacy_proto(RateLimitDuration::Year),
            crate::proto::workflows::RateLimitDuration::Year
        );
    }

    #[test]
    fn workflow_event_payload_parser_handles_json_and_plain_text() {
        let mut event = crate::proto::dispatcher::WorkflowEvent {
            workflow_run_id: "run-1".to_owned(),
            resource_type: 0,
            event_type: 0,
            resource_id: "resource-1".to_owned(),
            event_timestamp: None,
            event_payload: "{\"ok\":true}".to_owned(),
            hangup: false,
            task_retries: None,
            retry_count: None,
            event_index: None,
        };

        assert_eq!(
            workflow_event_payload_to_json(&event),
            serde_json::json!({"ok": true})
        );

        event.event_payload = "not-json".to_owned();
        assert_eq!(
            workflow_event_payload_to_json(&event),
            serde_json::json!({"event_payload":"not-json"})
        );
    }

    #[test]
    fn step_action_payload_exposes_typed_input_and_parent_output() {
        let payload = serde_json::json!({
            "input": {"order_id": "ord_1", "total_cents": 2500},
            "parents": {
                "process-order": {
                    "order_id": "ord_1",
                    "status": "inventory-reserved"
                },
                "calculate-tax": {
                    "tax_cents": 125
                }
            },
            "triggered_by": "event",
            "triggers": {
                "event": { "key": "order.created" },
                "filter_payload": { "region": "us-central" }
            },
            "step_run_errors": { "validate": "invalid address" }
        })
        .to_string();

        let decoded = decode_step_action_payload(&payload);

        assert_eq!(decoded.input["order_id"], "ord_1");
        assert_eq!(decoded.input["total_cents"], 2500);
        assert_eq!(
            decoded.parent_outputs["process-order"]["status"],
            "inventory-reserved"
        );
        assert_eq!(decoded.parent_outputs["calculate-tax"]["tax_cents"], 125);
        assert!(decoded.trigger_data.triggered_by_event);
        assert_eq!(
            decoded.trigger_data.event_key.as_deref(),
            Some("order.created")
        );
        assert_eq!(
            decoded.filter_payload.expect("filter")["region"],
            "us-central"
        );
        assert_eq!(decoded.errors["validate"], "invalid address");
    }

    #[test]
    fn step_action_payload_preserves_legacy_direct_payloads() {
        let payload = serde_json::json!({"order_id": "ord_legacy"}).to_string();
        let decoded = decode_step_action_payload(&payload);
        assert_eq!(decoded.input["order_id"], "ord_legacy");
        assert!(decoded.parent_outputs.is_empty());
    }

    #[test]
    fn task_trigger_context_recovers_event_and_cron_origin_from_engine_metadata() {
        let mut event = super::TriggerData::default();
        infer_trigger_data_from_metadata(
            &mut event,
            &HashMap::from([(
                "hatchet__event_key".to_owned(),
                "namespace_order.created".to_owned(),
            )]),
        );
        assert!(event.triggered_by_event);
        assert_eq!(event.event_key.as_deref(), Some("namespace_order.created"));

        let mut cron = super::TriggerData::default();
        infer_trigger_data_from_metadata(
            &mut cron,
            &HashMap::from([(
                "hatchet__cron_expression".to_owned(),
                "*/5 * * * *".to_owned(),
            )]),
        );
        assert!(cron.triggered_by_cron);
        assert_eq!(cron.cron_expression.as_deref(), Some("*/5 * * * *"));
    }

    #[test]
    fn run_output_from_details_prefers_registered_single_leaf() {
        let task_runs = HashMap::from([
            (
                "first-run".to_owned(),
                task_run_detail("first", serde_json::json!("first-output")),
            ),
            (
                "final-run".to_owned(),
                task_run_detail("final", serde_json::json!("final-output")),
            ),
        ]);

        let output = run_output_from_details(&task_runs, Some(&["final".to_owned()]));

        assert_eq!(output, serde_json::json!("final-output"));
    }

    #[test]
    fn run_output_from_details_returns_stable_map_for_multi_leaf_workflows() {
        let task_runs = HashMap::from([
            (
                "z-run".to_owned(),
                task_run_detail("z-task", serde_json::json!(2)),
            ),
            (
                "a-run".to_owned(),
                task_run_detail("a-task", serde_json::json!(1)),
            ),
        ]);

        let output = run_output_from_details(
            &task_runs,
            Some(&["z-task".to_owned(), "a-task".to_owned()]),
        );

        assert_eq!(output, serde_json::json!({"a-task": 1, "z-task": 2}));
    }

    #[test]
    fn parses_v1_log_list_payload_shape() {
        let payload = r#"{
            "rows":[
                {"createdAt":"2026-01-01T00:00:00Z","message":"hello","level":"INFO","metadata":{}},
                {"createdAt":"2026-01-01T00:00:01Z","message":"warn","level":"WARN","metadata":{}}
            ]
        }"#;
        let parsed: V1LogLineList = serde_json::from_str(payload).expect("log list parses");
        assert_eq!(parsed.rows.len(), 2);
        assert_eq!(parsed.rows[0].message, "hello");
        assert_eq!(parsed.rows[1].level.as_deref(), Some("WARN"));
    }

    #[test]
    fn completion_event_preserves_engine_owned_cancellation() {
        assert!(completion_event_from_result(Ok(serde_json::json!({"ok":true})), true).is_none());
        assert!(
            completion_event_from_result(Err(crate::Error::non_retryable("cancelled")), true)
                .is_none()
        );
    }

    #[test]
    fn completion_event_encodes_retry_after_only_on_retryable_failure() {
        let error =
            crate::RetryAfterError::new(std::time::Duration::from_millis(1250), "busy").unwrap();
        let (event, payload, should_not_retry, delay) =
            completion_event_from_result(Err(error.into()), false).unwrap();
        assert_eq!(
            event,
            crate::proto::dispatcher::StepActionEventType::StepEventTypeFailed
        );
        assert_eq!(payload, "busy");
        assert_eq!(should_not_retry, Some(false));
        assert_eq!(delay, Some(1250));
        let (_, _, flag, delay) =
            completion_event_from_result(Ok(serde_json::json!(true)), false).unwrap();
        assert_eq!((flag, delay), (None, None));
    }

    #[test]
    fn completion_event_preserves_non_retryable_errors() {
        let (_, _, should_not_retry, retry_after_ms) =
            completion_event_from_result(Err(crate::Error::non_retryable("invalid input")), false)
                .unwrap();
        assert_eq!(should_not_retry, Some(true));
        assert_eq!(retry_after_ms, None);
    }

    #[test]
    fn resolve_action_task_name_prefers_registered_action_id_and_falls_back_to_task_name() {
        let (tx, _rx) = tokio::sync::mpsc::channel(1);
        let dispatcher = crate::worker::WorkerDispatcher::new(tx);
        let mut opts = crate::transport::WorkerStartOpts {
            worker_name: "worker".to_owned(),
            labels: HashMap::new(),
            slot_config: HashMap::new(),
            task_dispatchers: HashMap::new(),
            durable_task_dispatchers: HashMap::new(),
            cancel_token: tokio_util::sync::CancellationToken::new(),
            connection_tx: tokio::sync::watch::channel(
                crate::transport::WorkerConnectionState::Starting,
            )
            .0,
        };
        opts.task_dispatchers
            .insert("wf:step".to_owned(), dispatcher.clone());

        let action = crate::proto::dispatcher::AssignedAction {
            tenant_id: String::new(),
            workflow_run_id: String::new(),
            get_group_key_run_id: String::new(),
            job_id: String::new(),
            job_name: String::new(),
            job_run_id: String::new(),
            task_id: String::new(),
            task_run_external_id: "task-run-1".to_owned(),
            action_id: "wf:step".to_owned(),
            action_type: crate::proto::dispatcher::ActionType::StartStepRun as i32,
            action_payload: "{}".to_owned(),
            task_name: "step".to_owned(),
            retry_count: 0,
            additional_metadata: None,
            child_workflow_index: None,
            child_workflow_key: None,
            parent_workflow_run_id: None,
            priority: 0,
            workflow_id: None,
            workflow_version_id: None,
            durable_task_invocation_count: None,
            ..Default::default()
        };
        assert_eq!(resolve_action_task_name(&action, &opts), "wf:step");

        let mut batch_opts = opts.clone();
        batch_opts.task_dispatchers.clear();
        batch_opts
            .task_dispatchers
            .insert("step".to_owned(), dispatcher.clone());
        let batch = crate::proto::dispatcher::AssignedAction {
            action_id: "default:step".to_owned(),
            action_type: crate::proto::dispatcher::ActionType::StartBatch as i32,
            ..Default::default()
        };
        assert_eq!(resolve_action_task_name(&batch, &batch_opts), "step");
        let mut opts_without_action = opts.clone();
        opts_without_action.task_dispatchers.clear();
        assert_eq!(
            resolve_action_task_name(&action, &opts_without_action),
            "step"
        );
    }

    #[test]
    fn worker_action_id_uses_default_integration_prefix() {
        assert_eq!(worker_action_id("echo"), "default:echo");
        assert_eq!(worker_action_id("hook.success"), "default:hook.success");
    }

    #[test]
    fn run_opt_label_mapping_supports_rich_config() {
        let rich = another_hatchet_core::DesiredWorkerLabelConfig::Rich(
            another_hatchet_core::DesiredWorkerLabel {
                value: "us-east".to_owned(),
                required: true,
                weight: Some(42),
                comparator: Some(another_hatchet_core::WorkerLabelComparator::Equal),
            },
        );
        let proto = run_opt_label_to_proto(&rich);
        assert_eq!(proto.str_value.as_deref(), Some("us-east"));
        assert_eq!(proto.required, Some(true));
        assert_eq!(proto.weight, Some(42));
        assert_eq!(
            proto.comparator,
            Some(crate::proto::v1::WorkerLabelComparator::Equal as i32)
        );
    }

    #[test]
    fn serialize_input_schema_encodes_json_bytes() {
        let schema = serde_json::json!({"type":"object","required":["id"]});
        let bytes = serialize_input_schema(Some(&schema)).expect("schema serializes");
        let parsed: serde_json::Value =
            serde_json::from_slice(&bytes).expect("serialized bytes are valid json");
        assert_eq!(parsed, schema);
    }
}
