#![allow(clippy::missing_errors_doc)]

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use another_hatchet_core::{RunOpts, WorkflowRunId};
use serde_json::Value;
use tokio::sync::RwLock;

use crate::error::{Error, Result};
use crate::run_ref::{RunControl, WorkflowRunRef};
use crate::task::RateLimitDuration;
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

/// An event to publish to Hatchet.
///
/// Use [`EventRecord::with_scope`] when the event should satisfy a scoped
/// condition. Hatchet requires scoped events for durable lookback waits.
#[derive(Debug, Clone, PartialEq)]
pub struct EventRecord {
    pub key: String,
    pub payload: Value,
    pub scope: Option<String>,
}

impl EventRecord {
    #[must_use]
    pub fn new(key: impl Into<String>, payload: Value) -> Self {
        Self {
            key: key.into(),
            payload,
            scope: None,
        }
    }

    #[must_use]
    pub fn with_scope(mut self, scope: impl Into<String>) -> Self {
        self.scope = Some(scope.into());
        self
    }
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
    pub(crate) workers: HashMap<String, WorkerInfo>,
    rate_limits: HashMap<String, u32>,
    events: Vec<EventRecord>,
    filters: HashMap<String, FilterDefinition>,
    webhooks: HashMap<String, WebhookDefinition>,
}

pub(crate) type SharedFeatureState = Arc<RwLock<FeatureState>>;

/// Filter parameters for run listing.
#[derive(Debug, Clone, Default)]
pub struct RunFilter {
    pub workflow_name: Option<String>,
    pub status: Option<RunStatus>,
}

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
        if let Some(api) = self._transport.rest_api() {
            return api.get_run(&run_id.0).await;
        }
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

    pub async fn list(&self) -> Result<Vec<RunRecord>> {
        if let Some(api) = self._transport.rest_api() {
            return api.list_runs().await;
        }
        let state = self.state.read().await;
        Ok(state.runs.values().cloned().collect())
    }

    pub async fn list_filtered(&self, filter: RunFilter) -> Result<Vec<RunRecord>> {
        if let Some(api) = self._transport.rest_api() {
            let status_str = filter.status.map(|s| match s {
                RunStatus::Queued => "PENDING",
                RunStatus::Cancelled => "CANCELLED",
                RunStatus::Completed => "COMPLETED",
            });
            return api
                .list_runs_filtered(filter.workflow_name.as_deref(), status_str)
                .await;
        }
        let state = self.state.read().await;
        Ok(state
            .runs
            .values()
            .filter(|r| {
                filter
                    .workflow_name
                    .as_deref()
                    .is_none_or(|n| r.workflow_name == n)
                    && filter.status.is_none_or(|s| r.status == s)
            })
            .cloned()
            .collect())
    }

    pub async fn cancel(&self, run_id: &WorkflowRunId) -> Result<()> {
        if let Some(api) = self._transport.rest_api() {
            return api.cancel_run(&run_id.0).await;
        }
        let mut state = self.state.write().await;
        let run = state
            .runs
            .get_mut(&run_id.0)
            .ok_or_else(|| Error::Worker(format!("run not found: {run_id}")))?;
        run.status = RunStatus::Cancelled;
        Ok(())
    }

    pub async fn replay(&self, run_id: &WorkflowRunId) -> Result<WorkflowRunId> {
        if let Some(api) = self._transport.rest_api() {
            let new_id = api.replay_run(&run_id.0).await?;
            return Ok(WorkflowRunId(new_id));
        }
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
    pub fn run_ref<O>(
        &self,
        run_id: WorkflowRunId,
        client: Arc<dyn RunControl>,
    ) -> WorkflowRunRef<O> {
        WorkflowRunRef::new(run_id, client)
    }

    pub async fn subscribe_to_stream(&self, run_id: &WorkflowRunId) -> Result<Vec<Value>> {
        let _state = self.state.read().await;
        self._transport.stream_run(run_id).await
    }

    pub async fn branch_durable_task(
        &self,
        run_id: &WorkflowRunId,
        _task_name: &str,
    ) -> Result<WorkflowRunId> {
        self.replay(run_id).await
    }

    pub async fn restore_task(&self, run_id: &WorkflowRunId, task_name: &str) -> Result<()> {
        if let Some(api) = self._transport.rest_api() {
            api.replay_run(&run_id.0).await?;
            return Ok(());
        }
        let mut state = self.state.write().await;
        let _task_name = task_name;
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
        if let Some(api) = self._transport.rest_api() {
            return api.get_workflow(name).await;
        }
        let state = self.state.read().await;
        state
            .workflows
            .get(name)
            .cloned()
            .ok_or_else(|| Error::Worker(format!("workflow not found: {name}")))
    }

    pub async fn list(&self) -> Result<Vec<WorkflowDefinition>> {
        if let Some(api) = self._transport.rest_api() {
            return api.list_workflows().await;
        }
        let state = self.state.read().await;
        Ok(state.workflows.values().cloned().collect())
    }

    pub async fn delete(&self, name: &str) -> Result<()> {
        if self._transport.rest_api().is_some() {
            return Err(Error::UnsupportedOperation(
                "workflow deletion is not implemented for engine-backed clients".to_owned(),
            ));
        }
        let mut state = self.state.write().await;
        if state.workflows.remove(name).is_none() {
            return Err(Error::Worker(format!("workflow not found: {name}")));
        }
        Ok(())
    }

    pub async fn put(
        &self,
        name: impl Into<String>,
        definition: Value,
    ) -> Result<WorkflowDefinition> {
        if self._transport.rest_api().is_some() {
            return Err(Error::UnsupportedOperation(
                "raw workflow writes are not supported for engine-backed clients; use Client::register_workflow"
                    .to_owned(),
            ));
        }
        Ok(self.record_put(name, definition).await)
    }

    pub(crate) async fn record_put(
        &self,
        name: impl Into<String>,
        definition: Value,
    ) -> WorkflowDefinition {
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

    pub async fn create(
        &self,
        workflow_name: &str,
        schedule_name: &str,
        trigger_at: &str,
        input: serde_json::Value,
    ) -> Result<Schedule> {
        if let Some(api) = self._transport.rest_api() {
            return api
                .create_schedule(workflow_name, schedule_name, trigger_at, &input)
                .await;
        }
        let schedule = Schedule {
            name: schedule_name.to_owned(),
            workflow_name: workflow_name.to_owned(),
            expression: trigger_at.to_owned(),
            input,
        };
        let mut state = self.state.write().await;
        state
            .schedules
            .insert(schedule.name.clone(), schedule.clone());
        Ok(schedule)
    }

    pub async fn get(&self, name: &str) -> Result<Schedule> {
        if let Some(api) = self._transport.rest_api() {
            return api.get_schedule(name).await;
        }
        let state = self.state.read().await;
        state
            .schedules
            .get(name)
            .cloned()
            .ok_or_else(|| Error::Worker(format!("schedule not found: {name}")))
    }

    pub async fn list(&self) -> Result<Vec<Schedule>> {
        if let Some(api) = self._transport.rest_api() {
            return api.list_schedules().await;
        }
        let state = self.state.read().await;
        Ok(state.schedules.values().cloned().collect())
    }

    pub async fn update(&self, schedule: Schedule) -> Result<Schedule> {
        if self._transport.rest_api().is_some() {
            return Err(Error::UnsupportedOperation(
                "schedule updates are not implemented for engine-backed clients".to_owned(),
            ));
        }
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
        if let Some(api) = self._transport.rest_api() {
            return api.delete_schedule(name).await;
        }
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

    pub async fn bulk_delete(&self, names: Vec<String>) -> Result<()> {
        for name in names {
            self.delete(&name).await?;
        }
        Ok(())
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

    pub async fn create(
        &self,
        workflow_name: &str,
        cron_name: &str,
        expression: &str,
        input: serde_json::Value,
    ) -> Result<Cron> {
        if let Some(api) = self._transport.rest_api() {
            return api
                .create_cron(workflow_name, cron_name, expression, &input)
                .await;
        }
        let cron = Cron {
            name: cron_name.to_owned(),
            workflow_name: workflow_name.to_owned(),
            expression: expression.to_owned(),
        };
        let mut state = self.state.write().await;
        state.crons.insert(cron.name.clone(), cron.clone());
        Ok(cron)
    }

    pub async fn get(&self, name: &str) -> Result<Cron> {
        if let Some(api) = self._transport.rest_api() {
            return api.get_cron(name).await;
        }
        let state = self.state.read().await;
        state
            .crons
            .get(name)
            .cloned()
            .ok_or_else(|| Error::Worker(format!("cron not found: {name}")))
    }

    pub async fn list(&self) -> Result<Vec<Cron>> {
        if let Some(api) = self._transport.rest_api() {
            return api.list_crons().await;
        }
        let state = self.state.read().await;
        Ok(state.crons.values().cloned().collect())
    }

    pub async fn delete(&self, name: &str) -> Result<()> {
        if let Some(api) = self._transport.rest_api() {
            return api.delete_cron(name).await;
        }
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

    pub async fn list(&self) -> Result<Vec<WorkerInfo>> {
        if let Some(api) = self._transport.rest_api() {
            return api.list_workers().await;
        }
        let state = self.state.read().await;
        Ok(state.workers.values().cloned().collect())
    }

    pub async fn get(&self, name: &str) -> Result<WorkerInfo> {
        if let Some(api) = self._transport.rest_api() {
            return api.get_worker(name).await;
        }
        let state = self.state.read().await;
        state
            .workers
            .get(name)
            .cloned()
            .ok_or_else(|| Error::Worker(format!("worker not found: {name}")))
    }

    pub async fn update(&self, worker: WorkerInfo) -> Result<WorkerInfo> {
        if self._transport.rest_api().is_some() {
            return Err(Error::UnsupportedOperation(
                "arbitrary worker updates are not implemented for engine-backed clients".to_owned(),
            ));
        }
        let mut state = self.state.write().await;
        state.workers.insert(worker.name.clone(), worker.clone());
        Ok(worker)
    }

    pub async fn is_paused(&self, name: &str) -> Result<bool> {
        Ok(self.get(name).await?.paused)
    }

    pub async fn pause(&self, name: &str) -> Result<WorkerInfo> {
        if let Some(api) = self._transport.rest_api() {
            api.pause_worker(name).await?;
            return api.get_worker(name).await;
        }
        let mut worker = self.get(name).await?;
        worker.paused = true;
        self.update(worker).await
    }

    pub async fn unpause(&self, name: &str) -> Result<WorkerInfo> {
        if let Some(api) = self._transport.rest_api() {
            api.unpause_worker(name).await?;
            return api.get_worker(name).await;
        }
        let mut worker = self.get(name).await?;
        worker.paused = false;
        self.update(worker).await
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

    pub async fn get_workflow_metrics(&self, workflow_name: &str) -> Result<WorkflowMetrics> {
        if let Some(api) = self._transport.rest_api() {
            let m = api.get_workflow_metrics(workflow_name).await?;
            return Ok(WorkflowMetrics {
                workflow_name: workflow_name.to_owned(),
                total_runs: m.total_runs.unwrap_or(0) as usize,
                queued_runs: m.queued_runs.unwrap_or(0) as usize,
                cancelled_runs: 0,
            });
        }
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
        Ok(WorkflowMetrics {
            workflow_name: workflow_name.to_owned(),
            total_runs,
            queued_runs,
            cancelled_runs,
        })
    }

    pub async fn get_queue_metrics(&self) -> Result<QueueMetrics> {
        if let Some(api) = self._transport.rest_api() {
            let m = api.get_queue_metrics().await?;
            return Ok(QueueMetrics {
                total_runs: m.total_runs.unwrap_or(0) as usize,
                queued_runs: m.queued_runs.unwrap_or(0) as usize,
            });
        }
        let state = self.state.read().await;
        let total_runs = state.runs.len();
        let queued_runs = state
            .runs
            .values()
            .filter(|run| run.status == RunStatus::Queued)
            .count();
        Ok(QueueMetrics {
            total_runs,
            queued_runs,
        })
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

    pub async fn list(&self) -> Result<HashMap<String, u32>> {
        if let Some(api) = self._transport.rest_api() {
            return api.list_rate_limits().await;
        }

        if let Ok(rate_limits) = self._transport.list_rate_limits().await {
            return Ok(rate_limits);
        }

        let state = self.state.read().await;
        Ok(state.rate_limits.clone())
    }

    pub async fn upsert(
        &self,
        key: impl Into<String>,
        limit: u32,
        duration: RateLimitDuration,
    ) -> Result<String> {
        let key = key.into();
        let returned_key = self
            ._transport
            .upsert_rate_limit(&key, limit, duration)
            .await?;

        let mut state = self.state.write().await;
        state.rate_limits.insert(returned_key.clone(), limit);
        Ok(returned_key)
    }

    pub async fn put(&self, key: impl Into<String>, limit: u32) -> Result<String> {
        self.upsert(key, limit, RateLimitDuration::Second).await
    }

    pub async fn delete(&self, key: &str) -> Result<()> {
        self.delete_result(key).await
    }

    pub async fn delete_result(&self, key: &str) -> Result<()> {
        if let Some(api) = self._transport.rest_api() {
            api.delete_rate_limit(key).await?;
        } else {
            self._transport.delete_rate_limit(key).await?;
        }

        let mut state = self.state.write().await;
        state.rate_limits.remove(key);
        Ok(())
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

    pub async fn push(&self, event: EventRecord) -> Result<()> {
        if self._transport.rest_api().is_some() {
            self._transport
                .push_event_with_scope(&event.key, &event.payload, event.scope.as_deref())
                .await?;
            return Ok(());
        }
        let mut state = self.state.write().await;
        state.events.push(event);
        Ok(())
    }

    pub async fn bulk_push(&self, events: Vec<EventRecord>) -> Result<()> {
        if self._transport.rest_api().is_some() {
            let records = events
                .iter()
                .map(|event| {
                    (
                        event.key.clone(),
                        event.payload.clone(),
                        event.scope.clone(),
                    )
                })
                .collect::<Vec<_>>();
            self._transport
                .bulk_push_events_with_scope(&records)
                .await?;
            return Ok(());
        }
        let mut state = self.state.write().await;
        state.events.extend(events);
        Ok(())
    }

    pub async fn list(&self) -> Result<Vec<EventRecord>> {
        if self._transport.rest_api().is_some() {
            return Err(Error::UnsupportedOperation(
                "event listing is not implemented for engine-backed clients".to_owned(),
            ));
        }
        let state = self.state.read().await;
        Ok(state.events.clone())
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

    pub async fn list(&self, task_run_id: &str) -> Result<Vec<LogRecord>> {
        let _state = self.state.read().await;
        self._transport.list_logs(task_run_id).await
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

    pub async fn create(
        &self,
        workflow_name: &str,
        filter_name: &str,
        expression: &str,
    ) -> Result<FilterDefinition> {
        if let Some(api) = self._transport.rest_api() {
            return api
                .create_filter(workflow_name, filter_name, expression)
                .await;
        }
        let filter = FilterDefinition {
            name: filter_name.to_owned(),
            expression: expression.to_owned(),
        };
        let mut state = self.state.write().await;
        state.filters.insert(filter.name.clone(), filter.clone());
        Ok(filter)
    }

    pub async fn get(&self, name: &str) -> Result<FilterDefinition> {
        if self._transport.rest_api().is_some() {
            return Err(Error::UnsupportedOperation(
                "filter lookup is not implemented for engine-backed clients".to_owned(),
            ));
        }
        let state = self.state.read().await;
        state
            .filters
            .get(name)
            .cloned()
            .ok_or_else(|| Error::Worker(format!("filter not found: {name}")))
    }

    pub async fn list(&self) -> Result<Vec<FilterDefinition>> {
        if let Some(api) = self._transport.rest_api() {
            return api.list_filters().await;
        }
        let state = self.state.read().await;
        Ok(state.filters.values().cloned().collect())
    }

    pub async fn update(&self, filter: FilterDefinition) -> Result<FilterDefinition> {
        if self._transport.rest_api().is_some() {
            return Err(Error::UnsupportedOperation(
                "filter updates are not implemented for engine-backed clients".to_owned(),
            ));
        }
        let mut state = self.state.write().await;
        if !state.filters.contains_key(&filter.name) {
            return Err(Error::Worker(format!("filter not found: {}", filter.name)));
        }
        state.filters.insert(filter.name.clone(), filter.clone());
        Ok(filter)
    }

    pub async fn delete(&self, name: &str) -> Result<()> {
        if let Some(api) = self._transport.rest_api() {
            return api.delete_filter(name).await;
        }
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

    pub async fn create(&self, name: &str, url: &str) -> Result<WebhookDefinition> {
        if let Some(api) = self._transport.rest_api() {
            return api.create_webhook(name, url).await;
        }
        let webhook = WebhookDefinition {
            name: name.to_owned(),
            url: url.to_owned(),
            enabled: true,
        };
        let mut state = self.state.write().await;
        state.webhooks.insert(webhook.name.clone(), webhook.clone());
        Ok(webhook)
    }

    pub async fn list(&self) -> Result<Vec<WebhookDefinition>> {
        if let Some(api) = self._transport.rest_api() {
            return api.list_webhooks().await;
        }
        let state = self.state.read().await;
        Ok(state.webhooks.values().cloned().collect())
    }

    pub async fn delete(&self, name: &str) -> Result<()> {
        if let Some(api) = self._transport.rest_api() {
            return api.delete_webhook(name).await;
        }
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

    pub fn debug(&self, expression: impl Into<String>, payload: &Value) -> Result<CelDebugResult> {
        if self._transport.rest_api().is_some() {
            return Err(Error::UnsupportedOperation(
                "CEL debugging is not implemented for engine-backed clients".to_owned(),
            ));
        }
        let expression = expression.into();
        let accepted = !expression.trim().is_empty() && !payload.is_null();
        let details = if accepted {
            "expression accepted by local debug evaluator".to_owned()
        } else {
            "expression rejected by local debug evaluator".to_owned()
        };
        Ok(CelDebugResult {
            expression,
            accepted,
            details,
        })
    }
}

/// Client for tenant-level operations.
///
/// Placeholder for transport-backed tenant operations (API key management,
/// tenant settings, etc.) that exist in the TS SDK's `features/tenant.ts`.
#[derive(Debug, Clone)]
pub struct TenantClient<T: Transport> {
    _transport: Arc<T>,
}

impl<T: Transport> TenantClient<T> {
    pub(crate) fn new(transport: Arc<T>) -> Self {
        Self {
            _transport: transport,
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
