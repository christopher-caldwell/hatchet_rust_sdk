use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use another_hatchet_core::{RunOpts, WorkflowRunId};
use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;
use tokio::sync::{Mutex, RwLock, oneshot};

use crate::RunEventStream;
use crate::clients::{DurableBranchResult, LogRecord, RestoreTaskResult};
use crate::context::LogLevel;
use crate::durable::{RenderedConditions, task_conditions_to_pb};
use crate::durable_listener::DurableListenerClient;
use crate::error::{Error, Result};
use crate::rest_client::RestApiClient;
use crate::task::{ConcurrencyConfig, DesiredLabelConfig, RateLimit, RateLimitDuration, SlotType};
use crate::worker::{TaskContextData, WorkerDispatcher};
use crate::workflow::{WorkflowDeclaration, WorkflowStep};
use tokio_util::sync::CancellationToken;

const MAX_COMPLETED_RUNS: usize = 1_024;

/// The shape of a single task during workflow registration.
///
/// Separates the pure declaration (`WorkflowStep`) from the transport-facing
/// payload by including pre-rendered conditions.
#[derive(Debug, Clone, Serialize)]
pub struct TaskRegistration {
    pub readable_id: String,
    pub action: String,
    pub timeout: Option<u64>,
    pub schedule_timeout: Option<u64>,
    pub parents: Vec<String>,
    pub retries: Option<u32>,
    pub rate_limits: Vec<RateLimit>,
    pub worker_labels: Vec<(String, DesiredLabelConfig)>,
    pub backoff_factor: Option<f64>,
    pub backoff_max_seconds: Option<u64>,
    pub concurrency: Option<ConcurrencyConfig>,
    pub slot_requests: HashMap<SlotType, u32>,
    pub conditions: RenderedConditions,
    pub is_durable: bool,
}

/// The fully-resolved shape sent during `putWorkflow`.
#[derive(Debug, Clone, Serialize)]
pub struct WorkflowRegistration {
    pub name: String,
    pub event_triggers: Vec<String>,
    pub cron_triggers: Vec<String>,
    pub tasks: Vec<TaskRegistration>,
}

impl WorkflowRegistration {
    /// Builds a `WorkflowRegistration` from a `WorkflowDeclaration`, rendering
    /// all step-level conditions into transport-ready payloads.
    #[must_use]
    pub fn from_declaration(
        declaration: &WorkflowDeclaration<Value, Value>,
        namespace: Option<&str>,
    ) -> Self {
        let tasks = declaration
            .steps
            .iter()
            .map(|step| TaskRegistration {
                readable_id: step.task_name.clone(),
                action: format!("default:{}", step.task_name),
                timeout: step.config.execution_timeout,
                schedule_timeout: step.config.schedule_timeout,
                parents: step.parents.clone(),
                retries: step.config.retries,
                rate_limits: step.config.rate_limits.clone(),
                worker_labels: step.config.desired_worker_labels.clone(),
                backoff_factor: step.config.backoff.as_ref().map(|backoff| backoff.factor),
                backoff_max_seconds: step
                    .config
                    .backoff
                    .as_ref()
                    .map(|backoff| backoff.max_seconds),
                concurrency: step.config.concurrency.clone(),
                slot_requests: step.config.slot_requests.clone(),
                conditions: task_conditions_to_pb(step, namespace),
                is_durable: step.is_durable,
            })
            .collect();
        Self {
            name: declaration.name.clone(),
            event_triggers: declaration.on_events.clone(),
            cron_triggers: declaration.on_crons.clone(),
            tasks,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SubmittedRun {
    pub workflow_name: String,
    pub input: Value,
    pub opts: RunOpts,
}

#[derive(Clone)]
pub struct WorkerStartOpts {
    pub worker_name: String,
    pub labels: std::collections::HashMap<String, String>,
    pub slot_config: std::collections::HashMap<String, i32>,
    pub task_dispatchers: std::collections::HashMap<String, WorkerDispatcher>,
    pub durable_task_dispatchers: std::collections::HashMap<String, WorkerDispatcher>,
    pub cancel_token: CancellationToken,
    pub connection_tx: tokio::sync::watch::Sender<WorkerConnectionState>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WorkerConnectionState {
    Starting,
    Ready,
    Reconnecting(String),
}

#[async_trait]
pub trait Transport: Send + Sync {
    /// Returns the workflow/event namespace configured by this transport.
    /// Fetches the connected engine release, if this transport has an engine.
    async fn engine_version(&self) -> Result<crate::EngineVersion> {
        Err(Error::UnsupportedOperation(
            "engine version requires an engine-backed transport".to_owned(),
        ))
    }

    fn namespace(&self) -> Option<&str> {
        None
    }

    async fn run_workflow(
        &self,
        workflow_name: &str,
        input: Value,
        opts: RunOpts,
    ) -> Result<WorkflowRunId>;

    async fn run_workflows(
        &self,
        workflow_name: &str,
        inputs: Vec<Value>,
        opts: RunOpts,
    ) -> Result<Vec<WorkflowRunId>>;

    async fn run_details(
        &self,
        _run_id: &WorkflowRunId,
    ) -> Result<crate::proto::v1::GetRunDetailsResponse> {
        Err(Error::UnsupportedOperation(
            "run details require an engine-backed transport".to_owned(),
        ))
    }

    /// Submits a run with an OpenTelemetry producer span when enabled.
    async fn run_workflow_traced(
        &self,
        workflow_name: &str,
        input: Value,
        opts: RunOpts,
    ) -> Result<WorkflowRunId> {
        crate::telemetry::submission(workflow_name, opts, |opts| {
            self.run_workflow(workflow_name, input, opts)
        })
        .await
    }

    async fn run_workflows_traced(
        &self,
        workflow_name: &str,
        inputs: Vec<Value>,
        opts: RunOpts,
    ) -> Result<Vec<WorkflowRunId>> {
        crate::telemetry::submission(workflow_name, opts, |opts| {
            self.run_workflows(workflow_name, inputs, opts)
        })
        .await
    }

    async fn register_result_channel(&self, run_id: &WorkflowRunId) -> Result<()>;

    async fn await_result(&self, run_id: &WorkflowRunId) -> Result<Value>;

    async fn resolve_result(&self, run_id: &WorkflowRunId, result: Result<Value>);

    async fn branch_durable_task(
        &self,
        _task_external_id: &str,
        _node_id: i64,
        _branch_id: i64,
    ) -> Result<DurableBranchResult> {
        Err(Error::UnsupportedOperation(
            "durable branching requires an engine-backed transport".to_owned(),
        ))
    }

    async fn restore_task(&self, _task_external_id: &str) -> Result<RestoreTaskResult> {
        Err(Error::UnsupportedOperation(
            "task restoration requires an engine-backed transport".to_owned(),
        ))
    }

    async fn start_worker_loop(&self, opts: WorkerStartOpts) -> Result<()>;

    async fn register_workflow_definition(
        &self,
        definition: WorkflowDeclaration<Value, Value>,
    ) -> Result<()>;

    async fn append_run_stream(
        &self,
        run_id: &WorkflowRunId,
        task_run_id: &str,
        payload: Value,
    ) -> Result<()>;

    async fn stream_run(&self, run_id: &WorkflowRunId) -> Result<Vec<Value>>;

    /// Live transports override this; the default adapts a finite snapshot.
    async fn subscribe_run_stream(&self, run_id: &WorkflowRunId) -> Result<RunEventStream> {
        Ok(RunEventStream::from_values(self.stream_run(run_id).await?))
    }

    async fn append_run_log(&self, run_id: &WorkflowRunId, level: LogLevel, message: String);

    async fn refresh_timeout(&self, task_run_id: &str, increment: &str) -> Result<()>;

    async fn release_slot(&self, task_run_id: &str) -> Result<()>;

    async fn cancel_workflow_run(&self, _run_id: &WorkflowRunId) -> Result<()> {
        Err(Error::UnsupportedOperation(
            "workflow cancellation is not supported by this transport".to_owned(),
        ))
    }

    async fn list_logs(&self, task_run_id: &str) -> Result<Vec<LogRecord>>;

    async fn push_event(&self, _key: &str, _payload: &Value) -> Result<()> {
        Err(Error::UnsupportedOperation(
            "event push is not supported by this transport".to_owned(),
        ))
    }

    /// Pushes an event with an optional Hatchet scope.
    ///
    /// Custom transports that support scoped events should override this
    /// method. The default preserves compatibility for unscoped event pushes.
    async fn push_event_with_scope(
        &self,
        key: &str,
        payload: &Value,
        scope: Option<&str>,
    ) -> Result<()> {
        if scope.is_some() {
            return Err(Error::UnsupportedOperation(
                "scoped event push is not supported by this transport".to_owned(),
            ));
        }
        self.push_event(key, payload).await
    }

    async fn bulk_push_events(&self, events: &[(String, Value)]) -> Result<()> {
        for (key, payload) in events {
            self.push_event(key, payload).await?;
        }
        Ok(())
    }

    /// Pushes events that may each carry a Hatchet scope.
    ///
    /// The default implementation delegates one-by-one so existing custom
    /// transports continue to work for unscoped records.
    async fn bulk_push_events_with_scope(
        &self,
        events: &[(String, Value, Option<String>)],
    ) -> Result<()> {
        for (key, payload, scope) in events {
            self.push_event_with_scope(key, payload, scope.as_deref())
                .await?;
        }
        Ok(())
    }

    async fn pause_rate_limit(
        &self,
        _key: &str,
        _until: std::time::SystemTime,
    ) -> Result<crate::RateLimitPauseResult> {
        Err(Error::UnsupportedOperation(
            "rate-limit pause requires an engine-backed transport".to_owned(),
        ))
    }

    async fn upsert_rate_limit(
        &self,
        _key: &str,
        _limit: u32,
        _duration: RateLimitDuration,
    ) -> Result<String> {
        Err(Error::Transport(
            "rate-limit upsert is not supported by this transport".to_owned(),
        ))
    }

    async fn list_rate_limits(&self) -> Result<HashMap<String, u32>> {
        Err(Error::Transport(
            "rate-limit list is not supported by this transport".to_owned(),
        ))
    }

    async fn delete_rate_limit(&self, _key: &str) -> Result<()> {
        Err(Error::Transport(
            "rate-limit delete is not supported by this transport".to_owned(),
        ))
    }

    fn durable_listener(&self) -> Option<Arc<DurableListenerClient>> {
        None
    }

    fn rest_api(&self) -> Option<Arc<RestApiClient>> {
        None
    }
}

#[derive(Debug, Default)]
struct MemoryRunStream {
    values: Vec<Value>,
    complete: bool,
    notify: Arc<tokio::sync::Notify>,
}

#[derive(Debug, Clone, Default)]
pub struct InMemoryTransport {
    counter: Arc<AtomicU64>,
    runs: Arc<RwLock<Vec<SubmittedRun>>>,
    pending: Arc<Mutex<HashMap<WorkflowRunId, oneshot::Sender<Result<Value>>>>>,
    receivers: Arc<Mutex<HashMap<WorkflowRunId, oneshot::Receiver<Result<Value>>>>>,
    completed: Arc<Mutex<HashMap<WorkflowRunId, Result<Value>>>>,
    streams: Arc<Mutex<HashMap<WorkflowRunId, MemoryRunStream>>>,
    logs: Arc<Mutex<HashMap<WorkflowRunId, Vec<LogRecord>>>>,
    task_dispatchers: Arc<RwLock<HashMap<String, Vec<WorkerDispatcher>>>>,
    durable_task_dispatchers: Arc<RwLock<HashMap<String, Vec<WorkerDispatcher>>>>,
    workflow_definitions: Arc<RwLock<HashMap<String, WorkflowDeclaration<Value, Value>>>>,
    workflow_registrations: Arc<RwLock<HashMap<String, WorkflowRegistration>>>,
    rate_limits: Arc<RwLock<HashMap<String, u32>>>,
}

impl InMemoryTransport {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn submitted_runs(&self) -> Vec<SubmittedRun> {
        self.runs.read().await.clone()
    }

    /// Returns all workflow registrations (with rendered conditions).
    pub async fn workflow_registrations(&self) -> Vec<WorkflowRegistration> {
        self.workflow_registrations
            .read()
            .await
            .values()
            .cloned()
            .collect()
    }

    fn next_run_id(&self) -> WorkflowRunId {
        let next = self.counter.fetch_add(1, Ordering::Relaxed) + 1;
        WorkflowRunId(format!("run-{next}"))
    }

    async fn execute_workflow_run(
        &self,
        run_id: WorkflowRunId,
        workflow_name: String,
        input: Value,
    ) {
        let result = match self
            .workflow_definitions
            .read()
            .await
            .get(&workflow_name)
            .cloned()
        {
            Some(definition) if !definition.steps.is_empty() => {
                self.execute_workflow_definition(definition, input, run_id.clone())
                    .await
            }
            _ => {
                self.execute_direct_workflow(workflow_name, input, run_id.clone())
                    .await
            }
        };
        self.resolve_result(&run_id, result).await;
    }

    async fn execute_direct_workflow(
        &self,
        workflow_name: String,
        input: Value,
        run_id: WorkflowRunId,
    ) -> Result<Value> {
        if let Some(dispatcher) = self
            .task_dispatchers
            .read()
            .await
            .get(&workflow_name)
            .and_then(|dispatchers| dispatchers.first())
            .cloned()
        {
            return dispatcher
                .run_task_json(
                    &workflow_name,
                    input,
                    crate::context::ContextIds {
                        workflow_name: workflow_name.clone(),
                        task_name: workflow_name.clone(),
                        workflow_run_id: run_id,
                        task_run_id: format!("{workflow_name}-task"),
                        workflow_id: None,
                        workflow_version_id: None,
                    },
                )
                .await;
        }

        if let Some(dispatcher) = self
            .durable_task_dispatchers
            .read()
            .await
            .get(&workflow_name)
            .and_then(|dispatchers| dispatchers.first())
            .cloned()
        {
            return dispatcher
                .run_durable_task_json(
                    &workflow_name,
                    input,
                    crate::context::ContextIds {
                        workflow_name: workflow_name.clone(),
                        task_name: workflow_name.clone(),
                        workflow_run_id: run_id,
                        task_run_id: format!("{workflow_name}-task"),
                        workflow_id: None,
                        workflow_version_id: None,
                    },
                    1,
                )
                .await;
        }

        Err(Error::Worker(format!(
            "no worker registered for workflow '{workflow_name}'"
        )))
    }

    async fn execute_workflow_definition(
        &self,
        definition: WorkflowDeclaration<Value, Value>,
        input: Value,
        run_id: WorkflowRunId,
    ) -> Result<Value> {
        let leaf_names = workflow_leaf_names(&definition.steps);
        let mut remaining = definition.steps;
        let mut outputs = HashMap::<String, Value>::new();

        while !remaining.is_empty() {
            let mut ready = Vec::new();
            let mut waiting = Vec::new();
            for step in remaining {
                if step
                    .parents
                    .iter()
                    .all(|parent| outputs.contains_key(parent))
                {
                    ready.push(step);
                } else {
                    waiting.push(step);
                }
            }

            if ready.is_empty() {
                return Err(Error::Worker(
                    "workflow declaration contains unresolved or cyclic step dependencies"
                        .to_owned(),
                ));
            }

            let mut join_set = tokio::task::JoinSet::new();
            for step in ready {
                let transport = self.clone();
                let workflow_name = definition.name.clone();
                let workflow_input = input.clone();
                let workflow_run_id = run_id.clone();
                let parent_outputs = step
                    .parents
                    .iter()
                    .filter_map(|parent| {
                        outputs
                            .get(parent)
                            .cloned()
                            .map(|output| (parent.clone(), output))
                    })
                    .collect();
                join_set.spawn(async move {
                    let task_name = step.task_name.clone();
                    let output = transport
                        .execute_step(
                            &workflow_name,
                            &step,
                            workflow_input,
                            parent_outputs,
                            workflow_run_id,
                        )
                        .await?;
                    Ok::<_, Error>((task_name, output))
                });
            }

            while let Some(joined) = join_set.join_next().await {
                let (task_name, output) = joined.map_err(|error| {
                    Error::Worker(format!("workflow task join failed: {error}"))
                })??;
                outputs.insert(task_name, output);
            }
            remaining = waiting;
        }

        if leaf_names.len() == 1 {
            return outputs.remove(&leaf_names[0]).ok_or_else(|| {
                Error::Worker(format!(
                    "workflow leaf '{}' did not produce output",
                    leaf_names[0]
                ))
            });
        }

        let mut leaf_outputs = serde_json::Map::new();
        for leaf_name in leaf_names {
            if let Some(output) = outputs.remove(&leaf_name) {
                leaf_outputs.insert(leaf_name, output);
            }
        }
        Ok(Value::Object(leaf_outputs))
    }

    async fn execute_step(
        &self,
        workflow_name: &str,
        step: &WorkflowStep,
        input: Value,
        parent_outputs: HashMap<String, Value>,
        run_id: WorkflowRunId,
    ) -> Result<Value> {
        let ids = crate::context::ContextIds {
            workflow_name: workflow_name.to_owned(),
            task_name: step.task_name.clone(),
            workflow_run_id: run_id,
            task_run_id: format!("{workflow_name}-{}", step.task_name),
            workflow_id: None,
            workflow_version_id: None,
        };

        if step.is_durable {
            if let Some(dispatcher) = self
                .durable_task_dispatchers
                .read()
                .await
                .get(&step.task_name)
                .and_then(|dispatchers| dispatchers.first())
                .cloned()
            {
                return dispatcher
                    .run_durable_task_json_with_context(
                        &step.task_name,
                        input,
                        ids,
                        1,
                        TaskContextData {
                            parent_outputs,
                            ..TaskContextData::default()
                        },
                    )
                    .await;
            }
        } else if let Some(dispatcher) = self
            .task_dispatchers
            .read()
            .await
            .get(&step.task_name)
            .and_then(|dispatchers| dispatchers.first())
            .cloned()
        {
            return dispatcher
                .run_task_json_with_context(
                    &step.task_name,
                    input,
                    ids,
                    TaskContextData {
                        parent_outputs,
                        ..TaskContextData::default()
                    },
                )
                .await;
        }

        Err(Error::Worker(format!(
            "no {}worker registered for workflow step '{}'",
            if step.is_durable { "durable " } else { "" },
            step.task_name
        )))
    }
}

#[async_trait]
impl Transport for InMemoryTransport {
    async fn run_workflow(
        &self,
        workflow_name: &str,
        input: Value,
        opts: RunOpts,
    ) -> Result<WorkflowRunId> {
        let run_id = self.next_run_id();
        let input_for_run = input.clone();
        let run = SubmittedRun {
            workflow_name: workflow_name.to_owned(),
            input,
            opts,
        };
        self.runs.write().await.push(run);
        let transport = self.clone();
        let workflow_name = workflow_name.to_owned();
        let run_id_for_task = run_id.clone();
        tokio::spawn(async move {
            transport
                .execute_workflow_run(run_id_for_task, workflow_name, input_for_run)
                .await;
        });
        Ok(run_id)
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

        let mut run_ids = Vec::with_capacity(inputs.len());
        let mut runs = self.runs.write().await;

        for (index, input) in inputs.into_iter().enumerate() {
            let run_id = self.next_run_id();
            let input_for_run = input.clone();
            run_ids.push(run_id);
            let mut run_opts = opts.clone();
            if run_opts.parent_workflow_run_id.is_some() {
                run_opts.child_index = Some(
                    run_opts
                        .child_index
                        .unwrap_or_default()
                        .saturating_add(index as u64),
                );
            }
            runs.push(SubmittedRun {
                workflow_name: workflow_name.to_owned(),
                input,
                opts: run_opts,
            });
            let transport = self.clone();
            let workflow_name = workflow_name.to_owned();
            let run_id_for_task = run_ids.last().cloned().expect("run id just pushed");
            tokio::spawn(async move {
                transport
                    .execute_workflow_run(run_id_for_task, workflow_name, input_for_run)
                    .await;
            });
        }

        Ok(run_ids)
    }

    async fn register_result_channel(&self, run_id: &WorkflowRunId) -> Result<()> {
        // Use the same lock order as `resolve_result` so completion cannot land
        // between the completed check and channel registration.
        let mut pending = self.pending.lock().await;
        let mut receivers = self.receivers.lock().await;
        if self.completed.lock().await.contains_key(run_id) {
            return Ok(());
        }
        let (tx, rx) = oneshot::channel();
        pending.insert(run_id.clone(), tx);
        receivers.insert(run_id.clone(), rx);
        Ok(())
    }

    async fn await_result(&self, run_id: &WorkflowRunId) -> Result<Value> {
        if let Some(result) = self.completed.lock().await.get(run_id).cloned() {
            return result;
        }

        let receiver =
            self.receivers.lock().await.remove(run_id).ok_or_else(|| {
                Error::Transport(format!("missing result channel for run {run_id}"))
            })?;

        let result = receiver
            .await
            .map_err(|error| Error::Transport(format!("result channel closed: {error}")))?;
        self.completed
            .lock()
            .await
            .insert(run_id.clone(), result.clone());
        result
    }

    async fn resolve_result(&self, run_id: &WorkflowRunId, result: Result<Value>) {
        if let Some(sender) = self.pending.lock().await.remove(run_id) {
            let _ = sender.send(result.clone());
        }
        let mut completed = self.completed.lock().await;
        let oldest = (completed.len() >= MAX_COMPLETED_RUNS && !completed.contains_key(run_id))
            .then(|| completed.keys().next().cloned())
            .flatten();
        if let Some(oldest) = oldest {
            completed.remove(&oldest);
        }
        completed.insert(run_id.clone(), result);
        drop(completed);
        let mut streams = self.streams.lock().await;
        let stream = streams.entry(run_id.clone()).or_default();
        stream.complete = true;
        stream.notify.notify_waiters();
    }

    async fn cancel_workflow_run(&self, run_id: &WorkflowRunId) -> Result<()> {
        self.resolve_result(
            run_id,
            Err(Error::Worker("workflow run was cancelled".to_owned())),
        )
        .await;
        Ok(())
    }

    async fn start_worker_loop(&self, opts: WorkerStartOpts) -> Result<()> {
        let task_dispatchers = opts.task_dispatchers;
        let durable_task_dispatchers = opts.durable_task_dispatchers;
        let mut t_map = self.task_dispatchers.write().await;
        for (task_name, dispatcher) in &task_dispatchers {
            t_map
                .entry(task_name.clone())
                .or_default()
                .push(dispatcher.clone());
        }
        let mut d_map = self.durable_task_dispatchers.write().await;
        for (task_name, dispatcher) in &durable_task_dispatchers {
            d_map
                .entry(task_name.clone())
                .or_default()
                .push(dispatcher.clone());
        }
        drop(t_map);
        drop(d_map);
        let _ = opts.connection_tx.send(WorkerConnectionState::Ready);

        // For in-memory, we just block here until cancelled, as workflow runs
        // fetch the dispatchers directly.
        opts.cancel_token.cancelled().await;

        let mut t_map = self.task_dispatchers.write().await;
        for (task_name, dispatcher) in &task_dispatchers {
            if let Some(registered) = t_map.get_mut(task_name) {
                registered.retain(|candidate| !candidate.same_channel(dispatcher));
                if registered.is_empty() {
                    t_map.remove(task_name);
                }
            }
        }
        drop(t_map);
        let mut d_map = self.durable_task_dispatchers.write().await;
        for (task_name, dispatcher) in &durable_task_dispatchers {
            if let Some(registered) = d_map.get_mut(task_name) {
                registered.retain(|candidate| !candidate.same_channel(dispatcher));
                if registered.is_empty() {
                    d_map.remove(task_name);
                }
            }
        }
        Ok(())
    }

    async fn register_workflow_definition(
        &self,
        definition: WorkflowDeclaration<Value, Value>,
    ) -> Result<()> {
        if definition.idempotency.is_some() {
            return Err(Error::UnsupportedOperation(
                "idempotency requires an engine-backed transport".to_owned(),
            ));
        }
        let registration = WorkflowRegistration::from_declaration(&definition, None);
        self.workflow_registrations
            .write()
            .await
            .insert(definition.name.clone(), registration);
        self.workflow_definitions
            .write()
            .await
            .insert(definition.name.clone(), definition);
        Ok(())
    }

    async fn append_run_stream(
        &self,
        run_id: &WorkflowRunId,
        _task_run_id: &str,
        payload: Value,
    ) -> Result<()> {
        let mut streams = self.streams.lock().await;
        let stream = streams.entry(run_id.clone()).or_default();
        stream.values.push(payload);
        stream.notify.notify_waiters();
        Ok(())
    }

    async fn stream_run(&self, run_id: &WorkflowRunId) -> Result<Vec<Value>> {
        self.subscribe_run_stream(run_id).await?.collect_all().await
    }

    async fn subscribe_run_stream(&self, run_id: &WorkflowRunId) -> Result<RunEventStream> {
        let notify = self
            .streams
            .lock()
            .await
            .entry(run_id.clone())
            .or_default()
            .notify
            .clone();
        let streams = Arc::clone(&self.streams);
        let run_id = run_id.clone();
        Ok(RunEventStream::new(async_stream::stream! {
            let mut cursor = 0;
            loop {
                // Register before inspecting the state so a publication or
                // completion between inspection and await cannot be lost.
                let notified = notify.clone().notified_owned();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let (next, complete) = {
                    let streams = streams.lock().await;
                    let state = &streams[&run_id];
                    (state.values.get(cursor).cloned(), state.complete)
                };
                if let Some(value) = next {
                    cursor += 1;
                    yield Ok(value);
                } else if complete {
                    break;
                } else {
                    notified.await;
                }
            }
        }))
    }

    async fn append_run_log(&self, run_id: &WorkflowRunId, level: LogLevel, message: String) {
        self.logs
            .lock()
            .await
            .entry(run_id.clone())
            .or_default()
            .push(LogRecord {
                level: format!("{level:?}"),
                message,
            });
    }

    async fn refresh_timeout(&self, _task_run_id: &str, _increment: &str) -> Result<()> {
        Ok(())
    }

    async fn release_slot(&self, _task_run_id: &str) -> Result<()> {
        Ok(())
    }

    async fn list_logs(&self, _task_run_id: &str) -> Result<Vec<LogRecord>> {
        Ok(self
            .logs
            .lock()
            .await
            .values()
            .flat_map(|records| records.clone())
            .collect())
    }

    async fn upsert_rate_limit(
        &self,
        key: &str,
        limit: u32,
        _duration: RateLimitDuration,
    ) -> Result<String> {
        self.rate_limits.write().await.insert(key.to_owned(), limit);
        Ok(key.to_owned())
    }

    async fn list_rate_limits(&self) -> Result<HashMap<String, u32>> {
        Ok(self.rate_limits.read().await.clone())
    }

    async fn delete_rate_limit(&self, key: &str) -> Result<()> {
        self.rate_limits.write().await.remove(key);
        Ok(())
    }
}

impl From<tokio::sync::AcquireError> for Error {
    fn from(error: tokio::sync::AcquireError) -> Self {
        Self::Transport(error.to_string())
    }
}

fn workflow_leaf_names(steps: &[WorkflowStep]) -> Vec<String> {
    let parents: HashSet<_> = steps
        .iter()
        .flat_map(|step| step.parents.iter().cloned())
        .collect();
    let mut leaves: Vec<_> = steps
        .iter()
        .filter(|step| !parents.contains(&step.task_name))
        .map(|step| step.task_name.clone())
        .collect();
    leaves.sort();
    leaves
}
