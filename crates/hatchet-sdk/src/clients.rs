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
    Running,
    Failed,
    Cancelled,
    Completed,
    Unknown,
}

/// The checkpoint created by branching an engine-backed durable task.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DurableBranchResult {
    pub task_external_id: String,
    pub node_id: i64,
    pub branch_id: i64,
}

/// Whether Hatchet requeued an evicted durable task.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub struct RestoreTaskResult {
    pub requeued: bool,
}

#[derive(Debug, Clone)]
pub struct RunRecord {
    pub run_id: WorkflowRunId,
    pub workflow_name: String,
    pub input: Value,
    pub opts: RunOpts,
    pub status: RunStatus,
    pub external_task_ids: HashMap<String, String>,
    pub summary: Option<crate::RunSummary>,
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
    /// Engine filter UUID; local transports retain their caller-supplied key.
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

/// Engine-backed run search, with explicit pagination and RFC3339 time bounds.
#[derive(Debug, Clone, Default)]
pub struct RunListOptions {
    pub filter: RunQuery,
    pub offset: u64,
    pub limit: Option<u64>,
    pub worker_id: Option<String>,
    pub only_tasks: bool,
    pub parent_task_external_id: Option<String>,
    pub triggering_event_external_id: Option<String>,
    pub include_payloads: Option<bool>,
}

/// A shared selection for search, cancel and replay. Omitted `since` means one hour ago.
#[derive(Debug, Clone, Default)]
pub struct RunQuery {
    pub since: Option<String>,
    pub until: Option<String>,
    pub statuses: Vec<RunStatus>,
    pub workflow_names: Vec<String>,
    pub additional_metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Default, serde::Deserialize)]
pub struct Pagination {
    pub current_page: Option<u64>,
    pub next_page: Option<u64>,
    pub num_pages: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct RunPage {
    pub rows: Vec<RunRecord>,
    pub pagination: Option<Pagination>,
}

/// Exactly one selection is sent to the engine; empty ID selections are rejected.
#[derive(Debug, Clone)]
pub enum RunSelection {
    ExternalIds(Vec<String>),
    Filter(RunQuery),
}

/// Task external IDs affected by cancel or replay. These are not workflow run IDs.
#[derive(Debug, Clone, serde::Deserialize)]
pub struct TaskMutationResult {
    pub ids: Vec<String>,
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
            return Ok(api
                .list_run_page(&RunListOptions {
                    filter: RunQuery {
                        workflow_names: filter.workflow_name.into_iter().collect(),
                        statuses: filter.status.into_iter().collect(),
                        ..RunQuery::default()
                    },
                    ..RunListOptions::default()
                })
                .await?
                .rows);
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

    /// Returns an engine page, preserving pagination metadata.
    pub async fn list_page(&self, options: RunListOptions) -> Result<RunPage> {
        let api = self._transport.rest_api().ok_or_else(|| {
            Error::UnsupportedOperation(
                "advanced run search requires an engine-backed transport".to_owned(),
            )
        })?;
        api.list_run_page(&options).await
    }

    pub async fn get_details(
        &self,
        run_id: &WorkflowRunId,
    ) -> Result<crate::proto::v1::GetRunDetailsResponse> {
        self._transport.run_details(run_id).await
    }

    pub async fn cancel_many(&self, selection: RunSelection) -> Result<TaskMutationResult> {
        let api = self._transport.rest_api().ok_or_else(|| {
            Error::UnsupportedOperation(
                "bulk cancellation requires an engine-backed transport".to_owned(),
            )
        })?;
        api.mutate_runs("cancel", &selection).await
    }

    pub async fn replay_many(&self, selection: RunSelection) -> Result<TaskMutationResult> {
        let api = self._transport.rest_api().ok_or_else(|| {
            Error::UnsupportedOperation(
                "bulk replay requires an engine-backed transport".to_owned(),
            )
        })?;
        api.mutate_runs("replay", &selection).await
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
                summary: None,
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

    pub async fn subscribe_to_stream(
        &self,
        run_id: &WorkflowRunId,
    ) -> Result<crate::RunEventStream> {
        self._transport.subscribe_run_stream(run_id).await
    }

    /// Branches a durable task from a node on an existing branch. The engine
    /// returns the new checkpoint; this does not create a new workflow run ID.
    pub async fn branch_durable_task(
        &self,
        task_external_id: &str,
        node_id: i64,
        branch_id: i64,
    ) -> Result<DurableBranchResult> {
        self._transport
            .branch_durable_task(task_external_id, node_id, branch_id)
            .await
    }

    /// Requeues an evicted task by task external ID, preserving its durable log.
    pub async fn restore_task(&self, task_external_id: &str) -> Result<RestoreTaskResult> {
        self._transport.restore_task(task_external_id).await
    }

    pub async fn get_task_external_id(
        &self,
        run_id: &WorkflowRunId,
        task_name: &str,
    ) -> Result<Option<String>> {
        if let Some(api) = self._transport.rest_api() {
            let run = api.get_run(&run_id.0).await?;
            return Ok(run.external_task_ids.get(task_name).cloned());
        }
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
        if let Some(api) = self._transport.rest_api() {
            return api.delete_workflow(name).await;
        }
        let mut state = self.state.write().await;
        if state.workflows.remove(name).is_none() {
            return Err(Error::Worker(format!("workflow not found: {name}")));
        }
        Ok(())
    }

    /// Requires the engine's newer pause contract; the pinned v0.94.14 handler is deprecated.
    pub async fn pause(&self, name: &str, options: crate::WorkflowPauseOptions) -> Result<()> {
        self._transport
            .engine_version()
            .await?
            .require(crate::EngineFeature::WorkflowPause)?;
        engine_api(&self._transport)?
            .pause_workflow(name, Some(&options))
            .await
    }

    pub async fn unpause(&self, name: &str) -> Result<()> {
        self._transport
            .engine_version()
            .await?
            .require(crate::EngineFeature::WorkflowPause)?;
        engine_api(&self._transport)?
            .pause_workflow(name, None)
            .await
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

    /// On engine clients, updates the trigger time; input and workflow are immutable.
    pub async fn update(&self, schedule: Schedule) -> Result<Schedule> {
        if let Some(api) = self._transport.rest_api() {
            return api.reschedule(&schedule.name, &schedule.expression).await;
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

    /// Updates trigger times only, matching the engine contract. All per-item failures are returned.
    pub async fn bulk_update(
        &self,
        updates: Vec<crate::ScheduleUpdate>,
    ) -> Result<crate::ScheduleBulkUpdateResult> {
        engine_api(&self._transport)?
            .bulk_reschedule(&updates)
            .await
    }

    pub async fn bulk_delete(&self, ids: Vec<String>) -> Result<crate::ScheduleBulkDeleteResult> {
        engine_api(&self._transport)?
            .bulk_delete_schedules(&ids)
            .await
    }

    pub async fn reschedule(&self, id: &str, trigger_at: &str) -> Result<Schedule> {
        engine_api(&self._transport)?
            .reschedule(id, trigger_at)
            .await
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

    /// Engine task counts grouped by status, including cancelled tasks.
    pub async fn get_task_status_metrics(
        &self,
        options: crate::RunListOptions,
    ) -> Result<Vec<crate::TaskStatusMetric>> {
        engine_api(&self._transport)?
            .task_status_metrics(&options)
            .await
    }
    pub async fn get_task_queue_metrics(&self) -> Result<crate::TaskQueueMetrics> {
        engine_api(&self._transport)?.task_queue_metrics().await
    }
    pub async fn get_task_stats(&self) -> Result<HashMap<String, crate::TaskStat>> {
        engine_api(&self._transport)?.task_stats().await
    }
    pub async fn scrape_prometheus_metrics(&self) -> Result<String> {
        engine_api(&self._transport)?.scrape_prometheus().await
    }

    pub async fn get_workflow_metrics(&self, workflow_name: &str) -> Result<WorkflowMetrics> {
        if self._transport.rest_api().is_some() {
            return Err(Error::UnsupportedOperation("legacy workflow run metrics are deprecated by the engine; use get_task_status_metrics with a workflow filter".to_owned()));
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
        if self._transport.rest_api().is_some() {
            return Err(Error::UnsupportedOperation("legacy tenant queue metrics are deprecated by the engine; use get_task_queue_metrics".to_owned()));
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
        if let Some(api) = self._transport.rest_api() {
            let record = api.get_filter(name).await?;
            return Ok(FilterDefinition {
                name: record.metadata.id,
                expression: record.expression,
            });
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
        if let Some(api) = self._transport.rest_api() {
            let record = api
                .update_filter(
                    &filter.name,
                    &crate::FilterUpdate {
                        expression: Some(filter.expression),
                        ..Default::default()
                    },
                )
                .await?;
            return Ok(FilterDefinition {
                name: record.metadata.id,
                expression: record.expression,
            });
        }
        let mut state = self.state.write().await;
        if !state.filters.contains_key(&filter.name) {
            return Err(Error::Worker(format!("filter not found: {}", filter.name)));
        }
        state.filters.insert(filter.name.clone(), filter.clone());
        Ok(filter)
    }

    pub async fn get_record(&self, id: &str) -> Result<crate::FilterRecord> {
        engine_api(&self._transport)?.get_filter(id).await
    }
    pub async fn update_options(
        &self,
        id: &str,
        update: crate::FilterUpdate,
    ) -> Result<crate::FilterRecord> {
        engine_api(&self._transport)?
            .update_filter(id, &update)
            .await
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

    /// Creates an inbound event webhook. `create(name, url)` configures a legacy webhook worker.
    pub async fn create_inbound(
        &self,
        options: crate::WebhookCreate,
    ) -> Result<crate::InboundWebhook> {
        engine_api(&self._transport)?
            .create_inbound_webhook(&options)
            .await
    }
    pub async fn get(&self, name: &str) -> Result<crate::InboundWebhook> {
        engine_api(&self._transport)?
            .get_inbound_webhook(name)
            .await
    }
    pub async fn update(
        &self,
        name: &str,
        update: crate::WebhookUpdate,
    ) -> Result<crate::InboundWebhook> {
        engine_api(&self._transport)?
            .update_inbound_webhook(name, &update)
            .await
    }
    pub async fn list_inbound(&self) -> Result<Vec<crate::InboundWebhook>> {
        engine_api(&self._transport)?.list_inbound_webhooks().await
    }
    pub async fn delete_inbound(&self, name: &str) -> Result<()> {
        engine_api(&self._transport)?
            .delete_inbound_webhook(name)
            .await
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

    pub async fn debug(
        &self,
        expression: impl Into<String>,
        payload: &Value,
    ) -> Result<crate::CelEvaluation> {
        let input = payload.as_object().cloned().ok_or_else(|| {
            Error::InputValidation(vec!["CEL input must be a JSON object".to_owned()])
        })?;
        self.debug_with_options(crate::CelDebugOptions {
            expression: expression.into(),
            input,
            ..Default::default()
        })
        .await
    }

    pub async fn debug_with_options(
        &self,
        options: crate::CelDebugOptions,
    ) -> Result<crate::CelEvaluation> {
        engine_api(&self._transport)?.debug_cel(&options).await
    }
}

/// Client for tenant-level operations.
///
/// Fetches details of the authenticated tenant from the engine.
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
    pub async fn get(&self) -> Result<crate::TenantDetails> {
        engine_api(&self._transport)?.get_tenant().await
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
            summary: None,
        },
    );
}

fn timestamp_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos())
}

fn engine_api<T: Transport>(transport: &Arc<T>) -> Result<Arc<crate::rest_client::RestApiClient>> {
    transport.rest_api().ok_or_else(|| {
        Error::UnsupportedOperation(
            "this management operation requires an engine-backed transport".to_owned(),
        )
    })
}
