use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use hatchet_core::{RunOpts, WorkflowRunId};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::{Mutex, RwLock, oneshot};

use crate::clients::LogRecord;
use crate::context::LogLevel;
use crate::durable::{RenderedConditions, task_conditions_to_pb};
use crate::durable_listener::DurableListenerClient;
use crate::error::{Error, Result};
use crate::rest_client::RestApiClient;
use crate::task::{ConcurrencyConfig, DesiredLabelConfig, RateLimit, RateLimitDuration, SlotType};
use crate::worker::WorkerDispatcher;
use crate::workflow::{WorkflowDeclaration, WorkflowStep};
use tokio_util::sync::CancellationToken;

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
}

#[async_trait]
pub trait Transport: Send + Sync {
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

    async fn register_result_channel(&self, run_id: &WorkflowRunId) -> Result<()>;

    async fn await_result(&self, run_id: &WorkflowRunId) -> Result<Value>;

    async fn resolve_result(&self, run_id: &WorkflowRunId, result: Result<Value>);

    async fn start_worker_loop(&self, opts: WorkerStartOpts) -> Result<()>;

    async fn register_workflow_definition(
        &self,
        definition: WorkflowDeclaration<Value, Value>,
    ) -> Result<()>;

    async fn append_run_stream(&self, run_id: &WorkflowRunId, payload: Value);

    async fn stream_run(&self, run_id: &WorkflowRunId) -> Result<Vec<Value>>;

    async fn append_run_log(&self, run_id: &WorkflowRunId, level: LogLevel, message: String);

    async fn refresh_timeout(&self, task_run_id: &str, increment: &str) -> Result<()>;

    async fn release_slot(&self, task_run_id: &str) -> Result<()>;

    async fn list_logs(&self, task_run_id: &str) -> Result<Vec<LogRecord>>;

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

#[derive(Debug, Clone, Default)]
pub struct InMemoryTransport {
    counter: Arc<AtomicU64>,
    runs: Arc<RwLock<Vec<SubmittedRun>>>,
    pending: Arc<Mutex<HashMap<WorkflowRunId, oneshot::Sender<Result<Value>>>>>,
    receivers: Arc<Mutex<HashMap<WorkflowRunId, oneshot::Receiver<Result<Value>>>>>,
    completed: Arc<Mutex<HashMap<WorkflowRunId, Result<Value>>>>,
    streams: Arc<Mutex<HashMap<WorkflowRunId, Vec<Value>>>>,
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
        let ordered = workflow_steps_in_execution_order(&definition.steps)?;
        let mut last_output = input.clone();
        for step in ordered {
            last_output = self
                .execute_step(&definition.name, &step, input.clone(), run_id.clone())
                .await?;
        }
        Ok(last_output)
    }

    async fn execute_step(
        &self,
        workflow_name: &str,
        step: &WorkflowStep,
        input: Value,
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
                    .run_durable_task_json(&step.task_name, input, ids, 1)
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
            return dispatcher.run_task_json(&step.task_name, input, ids).await;
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
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.insert(run_id.clone(), tx);
        self.receivers.lock().await.insert(run_id.clone(), rx);
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
        self.completed.lock().await.insert(run_id.clone(), result);
    }

    async fn start_worker_loop(&self, opts: WorkerStartOpts) -> Result<()> {
        let mut t_map = self.task_dispatchers.write().await;
        for (task_name, dispatcher) in opts.task_dispatchers {
            t_map.entry(task_name).or_default().push(dispatcher);
        }
        let mut d_map = self.durable_task_dispatchers.write().await;
        for (task_name, dispatcher) in opts.durable_task_dispatchers {
            d_map.entry(task_name).or_default().push(dispatcher);
        }
        drop(t_map);
        drop(d_map);

        // For in-memory, we just block here until cancelled, as workflow runs
        // fetch the dispatchers directly.
        opts.cancel_token.cancelled().await;
        Ok(())
    }

    async fn register_workflow_definition(
        &self,
        definition: WorkflowDeclaration<Value, Value>,
    ) -> Result<()> {
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

    async fn append_run_stream(&self, run_id: &WorkflowRunId, payload: Value) {
        self.streams
            .lock()
            .await
            .entry(run_id.clone())
            .or_default()
            .push(payload);
    }

    async fn stream_run(&self, run_id: &WorkflowRunId) -> Result<Vec<Value>> {
        Ok(self
            .streams
            .lock()
            .await
            .get(run_id)
            .cloned()
            .unwrap_or_default())
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

fn workflow_steps_in_execution_order(steps: &[WorkflowStep]) -> Result<Vec<WorkflowStep>> {
    let mut remaining = steps.to_vec();
    let mut completed = Vec::<String>::new();
    let mut ordered = Vec::with_capacity(remaining.len());

    while !remaining.is_empty() {
        let mut progressed = false;
        let mut index = 0;
        while index < remaining.len() {
            let step = &remaining[index];
            if step.parents.iter().all(|parent| completed.contains(parent)) {
                completed.push(step.task_name.clone());
                ordered.push(remaining.remove(index));
                progressed = true;
            } else {
                index += 1;
            }
        }

        if !progressed {
            return Err(Error::Worker(
                "workflow declaration contains unresolved or cyclic step dependencies".to_owned(),
            ));
        }
    }

    Ok(ordered)
}
