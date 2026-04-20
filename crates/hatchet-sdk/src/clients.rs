#![allow(clippy::missing_errors_doc)]

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use hatchet_core::{RunOpts, WorkflowRunId, WorkflowRunRef};
use serde_json::Value;
use tokio::sync::RwLock;

use crate::error::{Error, Result};
use crate::transport::Transport;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunStatus {
    Queued,
    Cancelled,
    Completed,
}

#[derive(Debug, Clone)]
pub struct RunRecord {
    pub run_id: WorkflowRunId,
    pub workflow_name: String,
    pub input: Value,
    pub opts: RunOpts,
    pub status: RunStatus,
    pub external_task_ids: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct WorkflowDefinition {
    pub name: String,
    pub version: u32,
    pub definition: Value,
}

#[derive(Debug, Clone)]
pub struct Schedule {
    pub name: String,
    pub workflow_name: String,
    pub expression: String,
    pub input: Value,
}

#[derive(Debug, Clone)]
pub struct Cron {
    pub name: String,
    pub workflow_name: String,
    pub expression: String,
}

#[derive(Debug, Clone)]
pub struct WorkerInfo {
    pub name: String,
    pub paused: bool,
    pub labels: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct WorkflowMetrics {
    pub workflow_name: String,
    pub total_runs: usize,
    pub queued_runs: usize,
    pub cancelled_runs: usize,
}

#[derive(Debug, Clone)]
pub struct QueueMetrics {
    pub total_runs: usize,
    pub queued_runs: usize,
}

#[derive(Debug, Clone)]
pub struct EventRecord {
    pub key: String,
    pub payload: Value,
}

#[derive(Debug, Clone)]
pub struct LogRecord {
    pub level: String,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct FilterDefinition {
    pub name: String,
    pub expression: String,
}

#[derive(Debug, Clone)]
pub struct WebhookDefinition {
    pub name: String,
    pub url: String,
    pub enabled: bool,
}

#[derive(Debug, Clone)]
pub struct CelDebugResult {
    pub expression: String,
    pub accepted: bool,
    pub details: String,
}

#[derive(Debug, Default)]
pub(crate) struct FeatureState {
    runs: HashMap<String, RunRecord>,
    workflows: HashMap<String, WorkflowDefinition>,
    schedules: HashMap<String, Schedule>,
    crons: HashMap<String, Cron>,
    workers: HashMap<String, WorkerInfo>,
    rate_limits: HashMap<String, u32>,
    events: Vec<EventRecord>,
    logs: Vec<LogRecord>,
    filters: HashMap<String, FilterDefinition>,
    webhooks: HashMap<String, WebhookDefinition>,
}

pub(crate) type SharedFeatureState = Arc<RwLock<FeatureState>>;

#[derive(Debug, Clone)]
pub struct RunsClient<T: Transport> {
    _transport: Arc<T>,
    state: SharedFeatureState,
}

impl<T: Transport> RunsClient<T> {
    pub(crate) fn new(transport: Arc<T>, state: SharedFeatureState) -> Self {
        Self {
            _transport: transport,
            state,
        }
    }

    pub async fn get(&self, run_id: &WorkflowRunId) -> Result<RunRecord> {
        let state = self.state.read().await;
        state
            .runs
            .get(&run_id.0)
            .cloned()
            .ok_or_else(|| Error::Worker(format!("run not found: {run_id}")))
    }

    pub async fn get_status(&self, run_id: &WorkflowRunId) -> Result<RunStatus> {
        Ok(self.get(run_id).await?.status)
    }

    pub async fn list(&self) -> Vec<RunRecord> {
        let state = self.state.read().await;
        state.runs.values().cloned().collect()
    }

    pub async fn cancel(&self, run_id: &WorkflowRunId) -> Result<()> {
        let mut state = self.state.write().await;
        let run = state
            .runs
            .get_mut(&run_id.0)
            .ok_or_else(|| Error::Worker(format!("run not found: {run_id}")))?;
        run.status = RunStatus::Cancelled;
        Ok(())
    }

    pub async fn replay(&self, run_id: &WorkflowRunId) -> Result<WorkflowRunId> {
        let mut state = self.state.write().await;
        let original = state
            .runs
            .get(&run_id.0)
            .cloned()
            .ok_or_else(|| Error::Worker(format!("run not found: {run_id}")))?;
        let replay_id = WorkflowRunId(format!(
            "{}-replay-{}",
            original.run_id.0,
            timestamp_nanos()
        ));
        state.runs.insert(
            replay_id.0.clone(),
            RunRecord {
                run_id: replay_id.clone(),
                workflow_name: original.workflow_name,
                input: original.input,
                opts: original.opts,
                status: RunStatus::Queued,
                external_task_ids: original.external_task_ids,
            },
        );
        Ok(replay_id)
    }

    #[must_use]
    pub fn run_ref<O>(&self, run_id: WorkflowRunId) -> WorkflowRunRef<O> {
        WorkflowRunRef::new(run_id)
    }

    pub async fn subscribe_to_stream(&self, _run_id: &WorkflowRunId) -> Result<Vec<Value>> {
        let _state = self.state.read().await;
        Ok(Vec::new())
    }

    pub async fn branch_durable_task(
        &self,
        run_id: &WorkflowRunId,
        _task_name: &str,
    ) -> Result<WorkflowRunId> {
        self.replay(run_id).await
    }

    pub async fn restore_task(&self, run_id: &WorkflowRunId, _task_name: &str) -> Result<()> {
        let mut state = self.state.write().await;
        let run = state
            .runs
            .get_mut(&run_id.0)
            .ok_or_else(|| Error::Worker(format!("run not found: {run_id}")))?;
        run.status = RunStatus::Queued;
        Ok(())
    }

    pub async fn get_task_external_id(
        &self,
        run_id: &WorkflowRunId,
        task_name: &str,
    ) -> Result<Option<String>> {
        let state = self.state.read().await;
        let run = state
            .runs
            .get(&run_id.0)
            .ok_or_else(|| Error::Worker(format!("run not found: {run_id}")))?;
        Ok(run.external_task_ids.get(task_name).cloned())
    }
}

#[derive(Debug, Clone)]
pub struct WorkflowsClient<T: Transport> {
    _transport: Arc<T>,
    state: SharedFeatureState,
}

impl<T: Transport> WorkflowsClient<T> {
    pub(crate) fn new(transport: Arc<T>, state: SharedFeatureState) -> Self {
        Self {
            _transport: transport,
            state,
        }
    }

    pub async fn get(&self, name: &str) -> Result<WorkflowDefinition> {
        let state = self.state.read().await;
        state
            .workflows
            .get(name)
            .cloned()
            .ok_or_else(|| Error::Worker(format!("workflow not found: {name}")))
    }

    pub async fn list(&self) -> Vec<WorkflowDefinition> {
        let state = self.state.read().await;
        state.workflows.values().cloned().collect()
    }

    pub async fn delete(&self, name: &str) -> Result<()> {
        let mut state = self.state.write().await;
        if state.workflows.remove(name).is_none() {
            return Err(Error::Worker(format!("workflow not found: {name}")));
        }
        Ok(())
    }

    pub async fn put(&self, name: impl Into<String>, definition: Value) -> WorkflowDefinition {
        let name = name.into();
        let mut state = self.state.write().await;
        let version = state.workflows.get(&name).map_or(1, |wf| wf.version + 1);
        let workflow = WorkflowDefinition {
            name: name.clone(),
            version,
            definition,
        };
        state.workflows.insert(name, workflow.clone());
        workflow
    }

    pub async fn version(&self, name: &str) -> Result<u32> {
        Ok(self.get(name).await?.version)
    }
}

#[derive(Debug, Clone)]
pub struct SchedulesClient<T: Transport> {
    _transport: Arc<T>,
    state: SharedFeatureState,
}

impl<T: Transport> SchedulesClient<T> {
    pub(crate) fn new(transport: Arc<T>, state: SharedFeatureState) -> Self {
        Self {
            _transport: transport,
            state,
        }
    }

    pub async fn create(&self, schedule: Schedule) -> Schedule {
        let mut state = self.state.write().await;
        state
            .schedules
            .insert(schedule.name.clone(), schedule.clone());
        schedule
    }

    pub async fn get(&self, name: &str) -> Result<Schedule> {
        let state = self.state.read().await;
        state
            .schedules
            .get(name)
            .cloned()
            .ok_or_else(|| Error::Worker(format!("schedule not found: {name}")))
    }

    pub async fn list(&self) -> Vec<Schedule> {
        let state = self.state.read().await;
        state.schedules.values().cloned().collect()
    }

    pub async fn update(&self, schedule: Schedule) -> Result<Schedule> {
        let mut state = self.state.write().await;
        if !state.schedules.contains_key(&schedule.name) {
            return Err(Error::Worker(format!(
                "schedule not found: {}",
                schedule.name
            )));
        }
        state
            .schedules
            .insert(schedule.name.clone(), schedule.clone());
        Ok(schedule)
    }

    pub async fn delete(&self, name: &str) -> Result<()> {
        let mut state = self.state.write().await;
        if state.schedules.remove(name).is_none() {
            return Err(Error::Worker(format!("schedule not found: {name}")));
        }
        Ok(())
    }

    pub async fn bulk_update(&self, schedules: Vec<Schedule>) -> Result<Vec<Schedule>> {
        let mut updated = Vec::with_capacity(schedules.len());
        for schedule in schedules {
            updated.push(self.update(schedule).await?);
        }
        Ok(updated)
    }

    pub async fn bulk_delete(&self, names: Vec<String>) {
        let mut state = self.state.write().await;
        for name in names {
            state.schedules.remove(&name);
        }
    }
}

#[derive(Debug, Clone)]
pub struct CronsClient<T: Transport> {
    _transport: Arc<T>,
    state: SharedFeatureState,
}

impl<T: Transport> CronsClient<T> {
    pub(crate) fn new(transport: Arc<T>, state: SharedFeatureState) -> Self {
        Self {
            _transport: transport,
            state,
        }
    }

    pub async fn create(&self, cron: Cron) -> Cron {
        let mut state = self.state.write().await;
        state.crons.insert(cron.name.clone(), cron.clone());
        cron
    }

    pub async fn get(&self, name: &str) -> Result<Cron> {
        let state = self.state.read().await;
        state
            .crons
            .get(name)
            .cloned()
            .ok_or_else(|| Error::Worker(format!("cron not found: {name}")))
    }

    pub async fn list(&self) -> Vec<Cron> {
        let state = self.state.read().await;
        state.crons.values().cloned().collect()
    }

    pub async fn delete(&self, name: &str) -> Result<()> {
        let mut state = self.state.write().await;
        if state.crons.remove(name).is_none() {
            return Err(Error::Worker(format!("cron not found: {name}")));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct WorkersClient<T: Transport> {
    _transport: Arc<T>,
    state: SharedFeatureState,
}

impl<T: Transport> WorkersClient<T> {
    pub(crate) fn new(transport: Arc<T>, state: SharedFeatureState) -> Self {
        Self {
            _transport: transport,
            state,
        }
    }

    pub async fn list(&self) -> Vec<WorkerInfo> {
        let state = self.state.read().await;
        state.workers.values().cloned().collect()
    }

    pub async fn get(&self, name: &str) -> Result<WorkerInfo> {
        let state = self.state.read().await;
        state
            .workers
            .get(name)
            .cloned()
            .ok_or_else(|| Error::Worker(format!("worker not found: {name}")))
    }

    pub async fn update(&self, worker: WorkerInfo) -> WorkerInfo {
        let mut state = self.state.write().await;
        state.workers.insert(worker.name.clone(), worker.clone());
        worker
    }
}

#[derive(Debug, Clone)]
pub struct MetricsClient<T: Transport> {
    _transport: Arc<T>,
    state: SharedFeatureState,
}

impl<T: Transport> MetricsClient<T> {
    pub(crate) fn new(transport: Arc<T>, state: SharedFeatureState) -> Self {
        Self {
            _transport: transport,
            state,
        }
    }

    pub async fn get_workflow_metrics(&self, workflow_name: &str) -> WorkflowMetrics {
        let state = self.state.read().await;
        let runs = state
            .runs
            .values()
            .filter(|run| run.workflow_name == workflow_name);
        let mut total_runs = 0;
        let mut queued_runs = 0;
        let mut cancelled_runs = 0;
        for run in runs {
            total_runs += 1;
            if run.status == RunStatus::Queued {
                queued_runs += 1;
            }
            if run.status == RunStatus::Cancelled {
                cancelled_runs += 1;
            }
        }
        WorkflowMetrics {
            workflow_name: workflow_name.to_owned(),
            total_runs,
            queued_runs,
            cancelled_runs,
        }
    }

    pub async fn get_queue_metrics(&self) -> QueueMetrics {
        let state = self.state.read().await;
        let total_runs = state.runs.len();
        let queued_runs = state
            .runs
            .values()
            .filter(|run| run.status == RunStatus::Queued)
            .count();
        QueueMetrics {
            total_runs,
            queued_runs,
        }
    }
}

#[derive(Debug, Clone)]
pub struct RateLimitsClient<T: Transport> {
    _transport: Arc<T>,
    state: SharedFeatureState,
}

impl<T: Transport> RateLimitsClient<T> {
    pub(crate) fn new(transport: Arc<T>, state: SharedFeatureState) -> Self {
        Self {
            _transport: transport,
            state,
        }
    }

    pub async fn list(&self) -> HashMap<String, u32> {
        let state = self.state.read().await;
        state.rate_limits.clone()
    }

    pub async fn put(&self, key: impl Into<String>, limit: u32) {
        let mut state = self.state.write().await;
        state.rate_limits.insert(key.into(), limit);
    }

    pub async fn delete(&self, key: &str) {
        let mut state = self.state.write().await;
        state.rate_limits.remove(key);
    }
}

#[derive(Debug, Clone)]
pub struct EventsClient<T: Transport> {
    _transport: Arc<T>,
    state: SharedFeatureState,
}

impl<T: Transport> EventsClient<T> {
    pub(crate) fn new(transport: Arc<T>, state: SharedFeatureState) -> Self {
        Self {
            _transport: transport,
            state,
        }
    }

    pub async fn push(&self, event: EventRecord) {
        let mut state = self.state.write().await;
        state.events.push(event);
    }

    pub async fn bulk_push(&self, events: Vec<EventRecord>) {
        let mut state = self.state.write().await;
        state.events.extend(events);
    }

    pub async fn list(&self) -> Vec<EventRecord> {
        let state = self.state.read().await;
        state.events.clone()
    }
}

#[derive(Debug, Clone)]
pub struct LogsClient<T: Transport> {
    _transport: Arc<T>,
    state: SharedFeatureState,
}

impl<T: Transport> LogsClient<T> {
    pub(crate) fn new(transport: Arc<T>, state: SharedFeatureState) -> Self {
        Self {
            _transport: transport,
            state,
        }
    }

    pub async fn list(&self) -> Vec<LogRecord> {
        let state = self.state.read().await;
        state.logs.clone()
    }
}

#[derive(Debug, Clone)]
pub struct FiltersClient<T: Transport> {
    _transport: Arc<T>,
    state: SharedFeatureState,
}

impl<T: Transport> FiltersClient<T> {
    pub(crate) fn new(transport: Arc<T>, state: SharedFeatureState) -> Self {
        Self {
            _transport: transport,
            state,
        }
    }

    pub async fn create(&self, filter: FilterDefinition) -> FilterDefinition {
        let mut state = self.state.write().await;
        state.filters.insert(filter.name.clone(), filter.clone());
        filter
    }

    pub async fn get(&self, name: &str) -> Result<FilterDefinition> {
        let state = self.state.read().await;
        state
            .filters
            .get(name)
            .cloned()
            .ok_or_else(|| Error::Worker(format!("filter not found: {name}")))
    }

    pub async fn list(&self) -> Vec<FilterDefinition> {
        let state = self.state.read().await;
        state.filters.values().cloned().collect()
    }

    pub async fn update(&self, filter: FilterDefinition) -> Result<FilterDefinition> {
        let mut state = self.state.write().await;
        if !state.filters.contains_key(&filter.name) {
            return Err(Error::Worker(format!("filter not found: {}", filter.name)));
        }
        state.filters.insert(filter.name.clone(), filter.clone());
        Ok(filter)
    }

    pub async fn delete(&self, name: &str) -> Result<()> {
        let mut state = self.state.write().await;
        if state.filters.remove(name).is_none() {
            return Err(Error::Worker(format!("filter not found: {name}")));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct WebhooksClient<T: Transport> {
    _transport: Arc<T>,
    state: SharedFeatureState,
}

impl<T: Transport> WebhooksClient<T> {
    pub(crate) fn new(transport: Arc<T>, state: SharedFeatureState) -> Self {
        Self {
            _transport: transport,
            state,
        }
    }

    pub async fn create(&self, webhook: WebhookDefinition) -> WebhookDefinition {
        let mut state = self.state.write().await;
        state.webhooks.insert(webhook.name.clone(), webhook.clone());
        webhook
    }

    pub async fn list(&self) -> Vec<WebhookDefinition> {
        let state = self.state.read().await;
        state.webhooks.values().cloned().collect()
    }

    pub async fn delete(&self, name: &str) -> Result<()> {
        let mut state = self.state.write().await;
        if state.webhooks.remove(name).is_none() {
            return Err(Error::Worker(format!("webhook not found: {name}")));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct CelClient<T: Transport> {
    _transport: Arc<T>,
}

impl<T: Transport> CelClient<T> {
    pub(crate) fn new(transport: Arc<T>) -> Self {
        Self {
            _transport: transport,
        }
    }

    pub fn debug(&self, expression: impl Into<String>, payload: &Value) -> CelDebugResult {
        let expression = expression.into();
        let accepted = !expression.trim().is_empty() && !payload.is_null();
        let details = if accepted {
            "expression accepted by local debug evaluator".to_owned()
        } else {
            "expression rejected by local debug evaluator".to_owned()
        };
        CelDebugResult {
            expression,
            accepted,
            details,
        }
    }
}

pub(crate) async fn record_run(
    state: &SharedFeatureState,
    run_id: WorkflowRunId,
    workflow_name: String,
    input: Value,
    opts: RunOpts,
) {
    let mut state = state.write().await;
    state.runs.insert(
        run_id.0.clone(),
        RunRecord {
            run_id,
            workflow_name,
            input,
            opts,
            status: RunStatus::Queued,
            external_task_ids: HashMap::new(),
        },
    );
}

fn timestamp_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos())
}
