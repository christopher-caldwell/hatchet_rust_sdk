//! gRPC transport implementation for connecting to a real Hatchet engine.
//!
//! Implements the `Transport` trait using actual gRPC calls to the engine's
//! `AdminService`, `Dispatcher`, `EventsService`, and `WorkflowService`.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

use async_trait::async_trait;
use hatchet_core::{RunOpts, WorkflowRunId};
use reqwest::header::{AUTHORIZATION, HeaderMap, HeaderValue};
use serde_json::Value;
use tokio::sync::{Mutex, RwLock};
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::{Request, Status};

use crate::clients::LogRecord;
use crate::config::{HatchetConfig, TlsConfig};
use crate::context::LogLevel;
use crate::durable::RenderedConditions;
use crate::error::{Error, Result};
use crate::proto;
use crate::rest_client::RestApiClient;
use crate::retry::{RetryConfig, retry};
use crate::transport::{Transport, WorkerStartOpts, WorkflowRegistration};
use crate::workflow::{WorkflowDeclaration, workflow_leaf_names};

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
    workflow_output_tasks: Arc<RwLock<HashMap<String, Vec<String>>>>,
    run_output_tasks: Arc<Mutex<HashMap<WorkflowRunId, Vec<String>>>>,
    completed: Arc<Mutex<HashMap<WorkflowRunId, Result<Value>>>>,
    log_records: Arc<Mutex<Vec<LogRecord>>>,
    running_cancellations: Arc<Mutex<HashMap<String, tokio_util::sync::CancellationToken>>>,
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

        let rest_client = build_rest_client(&config)?;
        let rest_base = format!("{}/api/v1", config.server_url.trim_end_matches('/'));
        let rest_api_client = RestApiClient::new(
            rest_client.clone(),
            rest_base.clone(),
            config.tenant_id.clone(),
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
            workflow_output_tasks: Arc::new(RwLock::new(HashMap::new())),
            run_output_tasks: Arc::new(Mutex::new(HashMap::new())),
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

    /// Returns a cloned transport with a custom retry policy.
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
            .map(|(key, value)| (key.clone(), run_opt_label_to_proto(value)))
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

        self.remember_run_output_tasks(workflow_name, &response)
            .await;

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
                    name: workflow_name.to_owned(),
                    input: serde_json::to_string(&input).unwrap_or_default(),
                    parent_id: opts.parent_workflow_run_id.clone(),
                    parent_task_run_external_id: opts.parent_task_run_id.clone(),
                    child_index,
                    child_key: opts.child_key.clone(),
                    additional_metadata: additional_metadata.clone(),
                    desired_worker_id: None,
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

        self.remember_bulk_run_output_tasks(workflow_name, &response)
            .await;

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

        // Engine-backed runs complete through Hatchet. This mirrors the
        // TypeScript SDK's WorkflowRunRef, which waits through the run listener
        // rather than an in-process channel.
        let result = self.poll_run_result(run_id).await;
        self.run_output_tasks.lock().await.remove(run_id);
        self.completed
            .lock()
            .await
            .insert(run_id.clone(), result.clone());
        result
    }

    async fn resolve_result(&self, run_id: &WorkflowRunId, result: Result<Value>) {
        self.run_output_tasks.lock().await.remove(run_id);
        self.completed.lock().await.insert(run_id.clone(), result);
    }

    async fn start_worker_loop(&self, opts: WorkerStartOpts) -> Result<()> {
        // Find all actions
        let mut actions = Vec::new();
        for action in opts.task_dispatchers.keys() {
            actions.push(worker_action_id(action));
        }
        for action in opts.durable_task_dispatchers.keys() {
            actions.push(worker_action_id(action));
        }

        let response = self
            .register_worker(
                &opts.worker_name,
                actions,
                opts.labels.clone(),
                opts.slot_config.clone(),
            )
            .await?;

        let worker_id = response.worker_id;

        // Start the durable listener stream (if durable tasks are registered)
        if !opts.durable_task_dispatchers.is_empty() {
            match crate::durable_listener::DurableListenerClient::start(
                self.config.clone(),
                worker_id.clone(),
            )
            .await
            {
                Ok(listener) => {
                    if let Ok(mut guard) = self.durable_listener.write() {
                        *guard = Some(Arc::new(listener));
                    }
                }
                Err(_error) => {}
            }
        }

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
        let mut stream: tonic::Streaming<proto::dispatcher::AssignedAction> = retry(
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
                        Err(_error) => break,
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
        let output_tasks = workflow_leaf_names(&definition.steps);
        let registration =
            WorkflowRegistration::from_declaration(&definition, self.config.namespace.as_deref());

        let tasks: Vec<proto::v1::CreateTaskOpts> = registration
            .tasks
            .iter()
            .map(|task| rendered_to_proto_task(task, &registration))
            .collect();
        let on_failure_task = definition
            .on_failure
            .as_ref()
            .and_then(|failure_task_name| {
                tasks
                    .iter()
                    .find(|task| task.readable_id == *failure_task_name)
                    .cloned()
            });

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
            input_json_schema: serialize_input_schema(definition.input_schema.as_ref()),
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

        self.workflow_output_tasks
            .write()
            .await
            .insert(registration.name, output_tasks);

        Ok(())
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

    async fn stream_run(&self, run_id: &WorkflowRunId) -> Result<Vec<Value>> {
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

        let mut events = Vec::new();
        let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(std::time::Duration::from_millis(500), stream.message())
                .await
            {
                Ok(Ok(Some(event))) => {
                    events.push(workflow_event_payload_to_json(&event));
                    if event.hangup {
                        break;
                    }
                }
                Ok(Ok(None)) => break,
                Ok(Err(error)) => return Err(grpc_error(error)),
                Err(_) => {
                    if !events.is_empty() {
                        break;
                    }
                }
            }
        }

        Ok(events)
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
    async fn remember_run_output_tasks(&self, workflow_name: &str, run_id: &WorkflowRunId) {
        let output_tasks = self
            .workflow_output_tasks
            .read()
            .await
            .get(workflow_name)
            .cloned();

        if let Some(output_tasks) = output_tasks {
            self.run_output_tasks
                .lock()
                .await
                .insert(run_id.clone(), output_tasks);
        }
    }

    async fn remember_bulk_run_output_tasks(&self, workflow_name: &str, run_ids: &[WorkflowRunId]) {
        let output_tasks = self
            .workflow_output_tasks
            .read()
            .await
            .get(workflow_name)
            .cloned();

        if let Some(output_tasks) = output_tasks {
            let mut run_output_tasks = self.run_output_tasks.lock().await;
            for run_id in run_ids {
                run_output_tasks.insert(run_id.clone(), output_tasks.clone());
            }
        }
    }

    async fn handle_assigned_action(
        &self,
        action: proto::dispatcher::AssignedAction,
        _worker_name: &str,
        worker_id: &str,
        opts: &crate::transport::WorkerStartOpts,
    ) {
        if action.action_type == proto::dispatcher::ActionType::CancelStepRun as i32 {
            if let Some(token) = self
                .running_cancellations
                .lock()
                .await
                .remove(&action.task_run_external_id)
            {
                token.cancel();
            }
            return;
        }
        if action.action_type != proto::dispatcher::ActionType::StartStepRun as i32 {
            return;
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
            let cancel_token = tokio_util::sync::CancellationToken::new();
            self.running_cancellations
                .lock()
                .await
                .insert(task_run_id.clone(), cancel_token.clone());

            // Send STARTED
            let _ = transport
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
                })
                .await;

            let ids = crate::context::ContextIds {
                workflow_name: action.job_name.clone(),
                task_name: task_name.clone(),
                workflow_run_id: WorkflowRunId(action.workflow_run_id.clone()),
                task_run_id: task_run_id.clone(),
                workflow_id: action.workflow_id.clone(),
                workflow_version_id: action.workflow_version_id.clone(),
            };

            let input = serde_json::from_str(&action.action_payload).unwrap_or(Value::Null);

            // Spawn execution
            tokio::spawn(async move {
                let result = if is_durable {
                    dispatcher
                        .run_durable_task_json_with_cancel(
                            &task_name,
                            input,
                            ids,
                            action.durable_task_invocation_count.unwrap_or(1) as u32,
                            Some(cancel_token.clone()),
                        )
                        .await
                } else {
                    dispatcher
                        .run_task_json_with_cancel(
                            &task_name,
                            input,
                            ids,
                            Some(cancel_token.clone()),
                        )
                        .await
                };

                let _ = transport
                    .running_cancellations
                    .lock()
                    .await
                    .remove(&task_run_id);

                let (event_type, payload) =
                    completion_event_from_result(result, cancel_token.is_cancelled());

                let _ = transport
                    .send_step_action_event(proto::dispatcher::StepActionEvent {
                        worker_id: worker_id_clone,
                        job_id: action.job_id,
                        job_run_id: action.job_run_id,
                        task_id: action.task_id,
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
                        let output_tasks = self.run_output_tasks.lock().await.get(run_id).cloned();
                        return Ok(run_output_from_details(
                            &details.task_runs,
                            output_tasks.as_deref(),
                        ));
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
    action.task_name.clone()
}

fn worker_action_id(task_name: &str) -> String {
    format!("default:{task_name}")
}

fn run_output_from_details(
    task_runs: &HashMap<String, proto::v1::TaskRunDetail>,
    output_tasks: Option<&[String]>,
) -> Value {
    if let Some(output_tasks) = output_tasks {
        if output_tasks.len() == 1
            && let Some(output) = find_task_output(task_runs, &output_tasks[0])
        {
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

    if task_runs.len() == 1
        && let Some(output) = task_runs.values().find_map(parse_task_output)
    {
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
) -> (proto::dispatcher::StepActionEventType, String) {
    if cancelled {
        return (
            proto::dispatcher::StepActionEventType::StepEventTypeFailed,
            "task cancelled".to_owned(),
        );
    }

    match result {
        Ok(val) => (
            proto::dispatcher::StepActionEventType::StepEventTypeCompleted,
            serde_json::to_string(&val).unwrap_or_default(),
        ),
        Err(error) => (
            proto::dispatcher::StepActionEventType::StepEventTypeFailed,
            error.to_string(),
        ),
    }
}

fn run_opt_label_to_proto(
    label: &hatchet_core::DesiredWorkerLabelConfig,
) -> proto::v1::DesiredWorkerLabels {
    match label {
        hatchet_core::DesiredWorkerLabelConfig::Legacy(value) => proto::v1::DesiredWorkerLabels {
            str_value: Some(value.clone()),
            int_value: None,
            required: None,
            comparator: None,
            weight: None,
        },
        hatchet_core::DesiredWorkerLabelConfig::Rich(config) => {
            let comparator = config.comparator.map(|comparator| match comparator {
                hatchet_core::WorkerLabelComparator::Equal => {
                    proto::v1::WorkerLabelComparator::Equal
                }
                hatchet_core::WorkerLabelComparator::NotEqual => {
                    proto::v1::WorkerLabelComparator::NotEqual
                }
                hatchet_core::WorkerLabelComparator::GreaterThan => {
                    proto::v1::WorkerLabelComparator::GreaterThan
                }
                hatchet_core::WorkerLabelComparator::GreaterThanOrEqual => {
                    proto::v1::WorkerLabelComparator::GreaterThanOrEqual
                }
                hatchet_core::WorkerLabelComparator::LessThan => {
                    proto::v1::WorkerLabelComparator::LessThan
                }
                hatchet_core::WorkerLabelComparator::LessThanOrEqual => {
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

fn grpc_error(status: Status) -> Error {
    Error::Transport(format!(
        "gRPC error: {} ({})",
        status.message(),
        status.code()
    ))
}

fn build_rest_client(config: &HatchetConfig) -> Result<reqwest::Client> {
    let mut headers = HeaderMap::new();
    let auth = format!("Bearer {}", config.token);
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
        retries: task.retries.map_or(0, u32_to_i32_saturating),
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
            crate::task::ConcurrencyLimitStrategy::QueueNewRuns => {
                proto::v1::ConcurrencyLimitStrategy::QueueNewest
            }
            crate::task::ConcurrencyLimitStrategy::CancelInProgress => {
                proto::v1::ConcurrencyLimitStrategy::CancelInProgress
            }
            crate::task::ConcurrencyLimitStrategy::CancelOldest => {
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
                    .limit_strategy(ConcurrencyLimitStrategy::CancelOldest),
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
    fn completion_event_marks_cancelled_runs_as_failed_even_on_success() {
        let (event_type, payload) =
            completion_event_from_result(Ok(serde_json::json!({"ok":true})), true);
        assert_eq!(
            event_type,
            crate::proto::dispatcher::StepActionEventType::StepEventTypeFailed
        );
        assert_eq!(payload, "task cancelled");
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
        };
        assert_eq!(resolve_action_task_name(&action, &opts), "wf:step");

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
        let rich = hatchet_core::DesiredWorkerLabelConfig::Rich(hatchet_core::DesiredWorkerLabel {
            value: "us-east".to_owned(),
            required: true,
            weight: Some(42),
            comparator: Some(hatchet_core::WorkerLabelComparator::Equal),
        });
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
