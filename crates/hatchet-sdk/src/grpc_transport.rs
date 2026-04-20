//! gRPC transport implementation for connecting to a real Hatchet engine.
//!
//! Implements the `Transport` trait using actual gRPC calls to the engine's
//! `AdminService`, `Dispatcher`, `EventsService`, and `WorkflowService`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use hatchet_core::{RunOpts, WorkflowRunId};
use serde_json::Value;
use tokio::sync::{Mutex, RwLock, oneshot};
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::{Request, Status};

use crate::clients::LogRecord;
use crate::config::{HatchetConfig, TlsConfig};
use crate::context::LogLevel;
use crate::durable::RenderedConditions;
use crate::error::{Error, Result};
use crate::proto;
use crate::transport::{Transport, WorkerStartOpts, WorkflowRegistration};
use crate::workflow::WorkflowDeclaration;

/// gRPC transport that connects to a real Hatchet engine.
///
/// Holds gRPC client stubs for all services and applies bearer-token auth
/// to every request.
#[derive(Clone)]
pub struct GrpcTransport {
    config: HatchetConfig,
    admin: proto::v1::admin_service_client::AdminServiceClient<Channel>,
    legacy_workflows: proto::workflows::workflow_service_client::WorkflowServiceClient<Channel>,
    dispatcher: proto::dispatcher::dispatcher_client::DispatcherClient<Channel>,
    events: proto::events::events_service_client::EventsServiceClient<Channel>,
    v1_dispatcher: proto::v1::v1_dispatcher_client::V1DispatcherClient<Channel>,
    retry_config: RetryConfig,
    /// Worker ID assigned after registration (set by the worker startup flow).
    worker_id: Arc<RwLock<Option<String>>>,
    /// Result channels for awaiting run completion.
    pending: Arc<Mutex<HashMap<WorkflowRunId, oneshot::Sender<Result<Value>>>>>,
    receivers: Arc<Mutex<HashMap<WorkflowRunId, oneshot::Receiver<Result<Value>>>>>,
    completed: Arc<Mutex<HashMap<WorkflowRunId, Result<Value>>>>,
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
    pub async fn from_config(config: HatchetConfig) -> Result<Self> {
        let endpoint_url = config.endpoint_url();
        let mut endpoint = Channel::from_shared(endpoint_url.clone()).map_err(|error| {
            Error::Transport(format!("invalid endpoint URL '{endpoint_url}': {error}"))
        })?;

        match &config.tls {
            TlsConfig::Insecure => {
                // No TLS configuration needed
            }
            TlsConfig::System => {
                let tls = tonic::transport::ClientTlsConfig::new();
                endpoint = endpoint.tls_config(tls).map_err(|error| {
                    Error::Transport(format!("system TLS config failed: {error}"))
                })?;
            }
            TlsConfig::Custom {
                ca_cert,
                client_cert,
                client_key,
                server_name,
            } => {
                let ca_pem: Vec<u8> = tokio::fs::read(ca_cert).await.map_err(|error| {
                    Error::Transport(format!("failed to read CA cert: {error}"))
                })?;
                let ca = tonic::transport::Certificate::from_pem(ca_pem);
                let mut tls = tonic::transport::ClientTlsConfig::new().ca_certificate(ca);

                if let (Some(cert_path), Some(key_path)) = (client_cert, client_key) {
                    let cert_pem: Vec<u8> = tokio::fs::read(cert_path).await.map_err(|error| {
                        Error::Transport(format!("failed to read client cert: {error}"))
                    })?;
                    let key_pem: Vec<u8> = tokio::fs::read(key_path).await.map_err(|error| {
                        Error::Transport(format!("failed to read client key: {error}"))
                    })?;
                    let identity = tonic::transport::Identity::from_pem(cert_pem, key_pem);
                    tls = tls.identity(identity);
                }

                if let Some(name) = server_name {
                    tls = tls.domain_name(name.clone());
                }

                endpoint = endpoint.tls_config(tls).map_err(|error| {
                    Error::Transport(format!("custom TLS config failed: {error}"))
                })?;
            }
        }

        let channel = endpoint
            .connect()
            .await
            .map_err(|error| Error::Transport(format!("gRPC connect failed: {error}")))?;

        Ok(Self {
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
            pending: Arc::new(Mutex::new(HashMap::new())),
            receivers: Arc::new(Mutex::new(HashMap::new())),
            completed: Arc::new(Mutex::new(HashMap::new())),
            config,
        })
    }

    /// Returns the configuration.
    #[must_use]
    pub fn config(&self) -> &HatchetConfig {
        &self.config
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
        if let Ok(value) =
            format!("Bearer {}", self.config.token).parse::<MetadataValue<tonic::metadata::Ascii>>()
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
            actions,
            services: Vec::new(),
            slots: None,
            labels: worker_labels,
            webhook_id: None,
            runtime_info: Some(proto::dispatcher::RuntimeInfo {
                sdk_version: Some(env!("CARGO_PKG_VERSION").to_owned()),
                language: Some(proto::dispatcher::Sdks::Unknown.into()),
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
                    client
                        .register(authed)
                        .await
                        .map(|response| response.into_inner())
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

        let mut dispatcher = self.dispatcher.clone();
        let request = self.authed_request(proto::dispatcher::HeartbeatRequest {
            worker_id,
            heartbeat_at: Some(prost_now()),
        });

        dispatcher.heartbeat(request).await.map_err(grpc_error)?;

        Ok(())
    }

    /// Sends a step action event (started, completed, failed) to the engine.
    ///
    /// # Errors
    ///
    /// Returns an error if the gRPC call fails.
    pub async fn send_step_action_event(
        &self,
        event: proto::dispatcher::StepActionEvent,
    ) -> Result<()> {
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
        let mut dispatcher = self.dispatcher.clone();
        let request = self.authed_request(proto::dispatcher::RefreshTimeoutRequest {
            task_run_external_id: task_run_external_id.to_owned(),
            increment_timeout_by: increment.to_owned(),
        });

        dispatcher
            .refresh_timeout(request)
            .await
            .map_err(grpc_error)?;

        Ok(())
    }

    /// Calls `ReleaseSlot` on the dispatcher.
    ///
    /// # Errors
    ///
    /// Returns an error if the gRPC call fails.
    pub async fn release_slot(&self, task_run_external_id: &str) -> Result<()> {
        let mut dispatcher = self.dispatcher.clone();
        let request = self.authed_request(proto::dispatcher::ReleaseSlotRequest {
            task_run_external_id: task_run_external_id.to_owned(),
        });

        dispatcher.release_slot(request).await.map_err(grpc_error)?;

        Ok(())
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
        let mut admin = self.admin.clone();
        let request = self.authed_request(proto::v1::GetRunDetailsRequest {
            external_id: run_id.to_owned(),
        });

        let response = admin.get_run_details(request).await.map_err(grpc_error)?;

        Ok(response.into_inner())
    }
}

#[async_trait]
impl Transport for GrpcTransport {
    async fn run_workflow(
        &self,
        workflow_name: &str,
        input: Value,
        opts: RunOpts,
    ) -> Result<WorkflowRunId> {
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
            .map(|(key, value)| {
                (
                    key.clone(),
                    proto::v1::DesiredWorkerLabels {
                        str_value: Some(value.clone()),
                        int_value: None,
                        required: None,
                        comparator: None,
                        weight: None,
                    },
                )
            })
            .collect();

        let trigger = proto::v1::TriggerWorkflowRequest {
            name: workflow_name.to_owned(),
            input: serde_json::to_string(&input).unwrap_or_default(),
            parent_id: opts.parent_workflow_run_id.clone(),
            parent_task_run_external_id: opts.parent_task_run_id.clone(),
            child_index: opts.child_index.map(|index| index as i32),
            child_key: opts.child_key.clone(),
            additional_metadata,
            desired_worker_id: None,
            priority: opts.priority.map(|p| priority_to_i32(p)),
            desired_worker_labels,
        };

        let mut workflows = self.legacy_workflows.clone();
        let request = self.authed_request(trigger.clone());

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
            .map(|(key, value)| {
                (
                    key.clone(),
                    proto::v1::DesiredWorkerLabels {
                        str_value: Some(value.clone()),
                        int_value: None,
                        required: None,
                        comparator: None,
                        weight: None,
                    },
                )
            })
            .collect();

        let triggers: Vec<proto::v1::TriggerWorkflowRequest> = inputs
            .into_iter()
            .enumerate()
            .map(|(index, input)| {
                let mut child_index = opts.child_index.map(|ci| ci as i32);
                if child_index.is_none() && opts.parent_workflow_run_id.is_some() {
                    child_index = Some(index as i32);
                }
                proto::v1::TriggerWorkflowRequest {
                    name: workflow_name.to_owned(),
                    input: serde_json::to_string(&input).unwrap_or_default(),
                    parent_id: opts.parent_workflow_run_id.clone(),
                    parent_task_run_external_id: opts.parent_task_run_id.clone(),
                    child_index,
                    child_key: opts.child_key.clone(),
                    additional_metadata: additional_metadata.clone(),
                    desired_worker_id: None,
                    priority: opts.priority.map(|p| priority_to_i32(p)),
                    desired_worker_labels: desired_worker_labels.clone(),
                }
            })
            .collect();

        let bulk_request = proto::workflows::BulkTriggerWorkflowRequest {
            workflows: triggers.clone(),
        };

        let mut workflows = self.legacy_workflows.clone();
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
                                .collect()
                        })
                        .map_err(grpc_error)
                }
            },
            &self.retry_config,
        )
        .await?;

        Ok(response)
    }

    async fn register_result_channel(&self, run_id: &WorkflowRunId) -> Result<()> {
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(run_id.clone(), tx);
        self.receivers.lock().await.insert(run_id.clone(), rx);
        Ok(())
    }

    async fn await_result(&self, run_id: &WorkflowRunId) -> Result<Value> {
        // Check if already completed
        if let Some(result) = self.completed.lock().await.get(run_id).cloned() {
            return result;
        }

        // Try to receive from the oneshot channel
        if let Some(rx) = self.receivers.lock().await.remove(run_id) {
            let result = rx
                .await
                .map_err(|error| Error::Transport(format!("result channel closed: {error}")))?;
            self.completed
                .lock()
                .await
                .insert(run_id.clone(), result.clone());
            return result;
        }

        // Poll the engine for the result
        self.poll_run_result(run_id).await
    }

    async fn resolve_result(&self, run_id: &WorkflowRunId, result: Result<Value>) {
        if let Some(sender) = self.pending.lock().await.remove(run_id) {
            let _ = sender.send(result.clone());
        }
        self.completed.lock().await.insert(run_id.clone(), result);
    }

    async fn start_worker_loop(&self, opts: WorkerStartOpts) -> Result<()> {
        // Find all actions
        let mut actions = Vec::new();
        actions.extend(opts.task_dispatchers.keys().cloned());
        actions.extend(opts.durable_task_dispatchers.keys().cloned());

        let response = self
            .register_worker(
                &opts.worker_name,
                actions,
                opts.labels.clone(),
                opts.slot_config.clone(),
            )
            .await?;

        let worker_id = response.worker_id;

        // 1. Start heartbeat loop
        let heartbeat_transport = self.clone();
        let cancel_token = opts.cancel_token.clone();
        let heartbeat_handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(4));
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        let _ = heartbeat_transport.heartbeat().await;
                    }
                    _ = cancel_token.cancelled() => {
                        break;
                    }
                }
            }
        });

        // 2. Open action stream
        let mut stream = retry(
            || async { self.listen(&worker_id).await },
            &self.retry_config,
        )
        .await?;

        // 3. Process actions
        loop {
            tokio::select! {
                message = stream.message() => {
                    match message {
                        Ok(Some(action)) => {
                            self.handle_assigned_action(action, &opts.worker_name, &worker_id, &opts).await;
                        }
                        Ok(None) => {
                            // Stream closed by server or disconnect.
                            // In a full implementation, we should reconnect the listen stream.
                            break;
                        }
                        Err(error) => {
                            eprintln!("listen stream error: {error}");
                            break;
                        }
                    }
                }
                _ = opts.cancel_token.cancelled() => {
                    break;
                }
            }
        }

        heartbeat_handle.abort();
        Ok(())
    }

    async fn register_workflow_definition(
        &self,
        definition: WorkflowDeclaration<Value, Value>,
    ) -> Result<()> {
        let registration =
            WorkflowRegistration::from_declaration(&definition, self.config.namespace.as_deref());

        let tasks: Vec<proto::v1::CreateTaskOpts> = registration
            .tasks
            .iter()
            .map(|task| rendered_to_proto_task(task, &registration))
            .collect();

        let create_request = proto::v1::CreateWorkflowVersionRequest {
            name: registration.name.clone(),
            description: definition.description.unwrap_or_default(),
            version: definition.version.unwrap_or_default(),
            event_triggers: registration.event_triggers.clone(),
            cron_triggers: registration.cron_triggers.clone(),
            tasks,
            concurrency: None,
            cron_input: None,
            on_failure_task: None,
            sticky: definition.sticky.as_ref().map(|s| match s {
                crate::workflow::StickyStrategy::Soft => proto::v1::StickyStrategy::Soft.into(),
                crate::workflow::StickyStrategy::Hard => proto::v1::StickyStrategy::Hard.into(),
            }),
            default_priority: definition.default_priority.map(|p| priority_to_i32(p)),
            concurrency_arr: definition.concurrency.as_ref().map_or(Vec::new(), |c| {
                vec![proto::v1::Concurrency {
                    expression: c.expression.clone(),
                    max_runs: c.max_runs.map(|m| m as i32),
                    limit_strategy: c.limit_strategy.as_ref().map(|s| match s {
                        crate::task::ConcurrencyLimitStrategy::QueueNewRuns => {
                            proto::v1::ConcurrencyLimitStrategy::QueueNewest.into()
                        }
                        crate::task::ConcurrencyLimitStrategy::CancelInProgress => {
                            proto::v1::ConcurrencyLimitStrategy::CancelInProgress.into()
                        }
                        crate::task::ConcurrencyLimitStrategy::CancelOldest => {
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
            input_json_schema: None,
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
        .await
    }

    async fn append_run_stream(&self, run_id: &WorkflowRunId, payload: Value) {
        let task_run_id = run_id.0.clone();
        let message = serde_json::to_vec(&payload).unwrap_or_default();

        let mut events = self.events.clone();
        let request = self.authed_request(proto::events::PutStreamEventRequest {
            task_run_external_id: task_run_id,
            created_at: Some(prost_now()),
            message,
            metadata: "{}".to_owned(),
            event_index: None,
        });

        let _ = events.put_stream_event(request).await;
    }

    async fn stream_run(&self, _run_id: &WorkflowRunId) -> Result<Vec<Value>> {
        // Stream events are typically polled via REST. For now, return empty.
        // The real implementation would use the REST API.
        Ok(Vec::new())
    }

    async fn append_run_log(&self, run_id: &WorkflowRunId, level: LogLevel, message: String) {
        let task_run_id = run_id.0.clone();
        let level_str = match level {
            LogLevel::Debug => "DEBUG",
            LogLevel::Info => "INFO",
            LogLevel::Warn => "WARN",
            LogLevel::Error => "ERROR",
        };

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

    async fn list_logs(&self) -> Vec<LogRecord> {
        // Logs are typically retrieved via REST API, not gRPC.
        Vec::new()
    }
}

impl GrpcTransport {
    async fn handle_assigned_action(
        &self,
        action: proto::dispatcher::AssignedAction,
        _worker_name: &str,
        worker_id: &str,
        opts: &crate::transport::WorkerStartOpts,
    ) {
        if action.action_type != proto::dispatcher::ActionType::StartStepRun as i32 {
            // Ignore cancel for now, we will wire up full cancellation next.
            return;
        }

        let task_name = action.action_id.clone();

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

            // Send STARTED
            let _ = transport
                .send_step_action_event(proto::dispatcher::StepActionEvent {
                    worker_id: worker_id_clone.clone(),
                    job_id: action.workflow_id.clone().unwrap_or_default(),
                    job_run_id: action.workflow_version_id.clone().unwrap_or_default(),
                    task_id: action.task_name.clone(),
                    task_run_external_id: task_run_id.clone(),
                    action_id: action.action_id.clone(),
                    event_timestamp: Some(prost_now()),
                    event_type: proto::dispatcher::StepActionEventType::StepEventTypeStarted as i32,
                    event_payload: String::new(),
                    retry_count: None,
                    should_not_retry: None,
                })
                .await;

            let ids = crate::context::ContextIds {
                workflow_name: action.workflow_id.clone().unwrap_or_default(),
                task_name: action.task_name.clone(),
                workflow_run_id: WorkflowRunId(
                    action.parent_workflow_run_id.clone().unwrap_or_default(),
                ),
                task_run_id: task_run_id.clone(),
                workflow_id: None,
                workflow_version_id: None,
            };

            let input = serde_json::from_str(&action.action_payload).unwrap_or(Value::Null);

            // Spawn execution
            tokio::spawn(async move {
                let result = if is_durable {
                    dispatcher
                        .run_durable_task_json(
                            &task_name,
                            input,
                            ids,
                            action.durable_task_invocation_count.unwrap_or(1) as u32,
                        )
                        .await
                } else {
                    dispatcher.run_task_json(&task_name, input, ids).await
                };

                let (event_type, payload) = match result {
                    Ok(val) => (
                        proto::dispatcher::StepActionEventType::StepEventTypeCompleted,
                        serde_json::to_string(&val).unwrap_or_default(),
                    ),
                    Err(error) => (
                        proto::dispatcher::StepActionEventType::StepEventTypeFailed,
                        error.to_string(),
                    ),
                };

                let _ = transport
                    .send_step_action_event(proto::dispatcher::StepActionEvent {
                        worker_id: worker_id_clone,
                        job_id: action.workflow_id.unwrap_or_default(),
                        job_run_id: action.workflow_version_id.unwrap_or_default(),
                        task_id: action.task_name,
                        task_run_external_id: task_run_id,
                        action_id: action.action_id,
                        event_timestamp: Some(prost_now()),
                        event_type: event_type as i32,
                        event_payload: payload,
                        retry_count: None,
                        should_not_retry: None,
                    })
                    .await;
            });
        }
    }

    /// Polls the engine for run completion by calling `GetRunDetails` repeatedly.
    async fn poll_run_result(&self, run_id: &WorkflowRunId) -> Result<Value> {
        let poll_interval = std::time::Duration::from_millis(500);
        let max_polls = 600; // 5 minutes at 500ms

        for _ in 0..max_polls {
            let details = self.get_run_details(&run_id.0).await?;

            if details.done {
                match proto::v1::RunStatus::try_from(details.status) {
                    Ok(proto::v1::RunStatus::Completed) => {
                        // Collect task outputs — find the last task's output
                        let mut output = Value::Null;
                        for (_key, task_detail) in &details.task_runs {
                            if let Some(output_bytes) = &task_detail.output {
                                if let Ok(parsed) = serde_json::from_slice(output_bytes) {
                                    output = parsed;
                                }
                            }
                        }
                        return Ok(output);
                    }
                    Ok(proto::v1::RunStatus::Failed) => {
                        let error_msg = details
                            .task_runs
                            .values()
                            .find_map(|task| task.error.clone())
                            .unwrap_or_else(|| "workflow run failed".to_owned());
                        return Err(Error::Transport(error_msg));
                    }
                    Ok(proto::v1::RunStatus::Cancelled) => {
                        return Err(Error::Transport("workflow run was cancelled".to_owned()));
                    }
                    _ => {}
                }
            }

            tokio::time::sleep(poll_interval).await;
        }

        Err(Error::Transport(format!(
            "timed out waiting for run {run_id} to complete"
        )))
    }
}

// --- Conversion helpers ---

fn grpc_error(status: Status) -> Error {
    Error::Transport(format!(
        "gRPC error: {} ({})",
        status.message(),
        status.code()
    ))
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

fn priority_to_i32(priority: hatchet_core::Priority) -> i32 {
    match priority {
        hatchet_core::Priority::Low => 1,
        hatchet_core::Priority::Normal => 2,
        hatchet_core::Priority::High => 3,
        hatchet_core::Priority::Critical => 4,
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
        retries: task.retries.map_or(0, |r| r as i32),
        rate_limits: Vec::new(),       // TODO: wire rate limits
        worker_labels: HashMap::new(), // TODO: wire desired worker labels
        backoff_factor: None,
        backoff_max_seconds: None,
        concurrency: Vec::new(), // TODO: wire concurrency
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
        slot_requests: HashMap::new(),
    }
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
