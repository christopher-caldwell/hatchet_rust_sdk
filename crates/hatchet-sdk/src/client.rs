use std::marker::PhantomData;
use std::sync::Arc;

use async_trait::async_trait;
use hatchet_core::{RunOpts, WorkflowRunId};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::sync::RwLock;

use crate::clients::{
    CelClient, CronsClient, EventsClient, FeatureState, FiltersClient, LogsClient, MetricsClient,
    RateLimitsClient, RunsClient, SchedulesClient, TenantClient, WebhooksClient, WorkersClient,
    WorkflowsClient, record_run,
};
use crate::error::{Error, Result};
use crate::run_ref::{RunControl, WorkflowRunRef};
use crate::task::TaskConfig;
use crate::transport::Transport;
use crate::worker::WorkerBuilder;
use crate::workflow::WorkflowDeclaration;

#[derive(Debug)]
pub struct Client<T: Transport> {
    transport: Arc<T>,
    state: Arc<RwLock<FeatureState>>,
    namespace: Option<String>,
    runs: RunsClient<T>,
    workflows: WorkflowsClient<T>,
    schedules: SchedulesClient<T>,
    crons: CronsClient<T>,
    workers: WorkersClient<T>,
    metrics: MetricsClient<T>,
    rate_limits: RateLimitsClient<T>,
    events: EventsClient<T>,
    logs: LogsClient<T>,
    filters: FiltersClient<T>,
    webhooks: WebhooksClient<T>,
    cel: CelClient<T>,
    tenant: TenantClient<T>,
}

impl<T: Transport> Clone for Client<T> {
    fn clone(&self) -> Self {
        Self {
            transport: Arc::clone(&self.transport),
            state: Arc::clone(&self.state),
            namespace: self.namespace.clone(),
            runs: RunsClient::new(Arc::clone(&self.transport), Arc::clone(&self.state)),
            workflows: WorkflowsClient::new(Arc::clone(&self.transport), Arc::clone(&self.state)),
            schedules: SchedulesClient::new(Arc::clone(&self.transport), Arc::clone(&self.state)),
            crons: CronsClient::new(Arc::clone(&self.transport), Arc::clone(&self.state)),
            workers: WorkersClient::new(Arc::clone(&self.transport), Arc::clone(&self.state)),
            metrics: MetricsClient::new(Arc::clone(&self.transport), Arc::clone(&self.state)),
            rate_limits: RateLimitsClient::new(
                Arc::clone(&self.transport),
                Arc::clone(&self.state),
            ),
            events: EventsClient::new(Arc::clone(&self.transport), Arc::clone(&self.state)),
            logs: LogsClient::new(Arc::clone(&self.transport), Arc::clone(&self.state)),
            filters: FiltersClient::new(Arc::clone(&self.transport), Arc::clone(&self.state)),
            webhooks: WebhooksClient::new(Arc::clone(&self.transport), Arc::clone(&self.state)),
            cel: CelClient::new(Arc::clone(&self.transport)),
            tenant: TenantClient::new(Arc::clone(&self.transport)),
        }
    }
}

impl<T: Transport + 'static> Client<T> {
    #[must_use]
    pub fn new(transport: Arc<T>) -> Self {
        let state = Arc::new(RwLock::new(FeatureState::default()));
        Self {
            runs: RunsClient::new(Arc::clone(&transport), Arc::clone(&state)),
            workflows: WorkflowsClient::new(Arc::clone(&transport), Arc::clone(&state)),
            schedules: SchedulesClient::new(Arc::clone(&transport), Arc::clone(&state)),
            crons: CronsClient::new(Arc::clone(&transport), Arc::clone(&state)),
            workers: WorkersClient::new(Arc::clone(&transport), Arc::clone(&state)),
            metrics: MetricsClient::new(Arc::clone(&transport), Arc::clone(&state)),
            rate_limits: RateLimitsClient::new(Arc::clone(&transport), Arc::clone(&state)),
            events: EventsClient::new(Arc::clone(&transport), Arc::clone(&state)),
            logs: LogsClient::new(Arc::clone(&transport), Arc::clone(&state)),
            filters: FiltersClient::new(Arc::clone(&transport), Arc::clone(&state)),
            webhooks: WebhooksClient::new(Arc::clone(&transport), Arc::clone(&state)),
            cel: CelClient::new(Arc::clone(&transport)),
            tenant: TenantClient::new(Arc::clone(&transport)),
            namespace: None,
            transport,
            state,
        }
    }

    /// Sets a namespace for this client. Namespaces are applied to workflow
    /// names and event triggers during registration and to workflow names
    /// during run dispatch.
    #[must_use]
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    /// Returns the namespace, if set.
    #[must_use]
    pub fn namespace(&self) -> Option<&str> {
        self.namespace.as_deref()
    }

    #[must_use]
    pub fn workflow<I, O>(&self, workflow_name: impl Into<String>) -> WorkflowHandle<T, I, O> {
        WorkflowHandle {
            workflow_name: workflow_name.into(),
            transport: Arc::clone(&self.transport),
            state: Arc::clone(&self.state),
            run_control: Arc::new((*self).clone()),
            input: PhantomData,
            output: PhantomData,
        }
    }

    #[must_use]
    pub fn task<I, O>(&self, task_name: impl Into<String>) -> TaskHandle<I, O> {
        TaskHandle {
            task_name: task_name.into(),
            config: TaskConfig::default(),
            input: PhantomData,
            output: PhantomData,
        }
    }

    #[must_use]
    pub fn durable_task<I, O>(&self, task_name: impl Into<String>) -> DurableTaskHandle<I, O> {
        DurableTaskHandle {
            task_name: task_name.into(),
            config: TaskConfig::default(),
            input: PhantomData,
            output: PhantomData,
        }
    }

    #[must_use]
    pub fn worker(&self, name: impl Into<String>) -> WorkerBuilder<T>
    where
        T: 'static,
    {
        WorkerBuilder::new(name.into(), Arc::clone(&self.transport))
    }

    #[must_use]
    pub fn runs(&self) -> &RunsClient<T> {
        &self.runs
    }

    #[must_use]
    pub fn workflows(&self) -> &WorkflowsClient<T> {
        &self.workflows
    }

    #[must_use]
    pub fn schedules(&self) -> &SchedulesClient<T> {
        &self.schedules
    }

    #[must_use]
    pub fn crons(&self) -> &CronsClient<T> {
        &self.crons
    }

    #[must_use]
    pub fn workers(&self) -> &WorkersClient<T> {
        &self.workers
    }

    #[must_use]
    pub fn metrics(&self) -> &MetricsClient<T> {
        &self.metrics
    }

    #[must_use]
    pub fn rate_limits(&self) -> &RateLimitsClient<T> {
        &self.rate_limits
    }

    #[must_use]
    pub fn events(&self) -> &EventsClient<T> {
        &self.events
    }

    #[must_use]
    pub fn logs(&self) -> &LogsClient<T> {
        &self.logs
    }

    #[must_use]
    pub fn filters(&self) -> &FiltersClient<T> {
        &self.filters
    }

    #[must_use]
    pub fn webhooks(&self) -> &WebhooksClient<T> {
        &self.webhooks
    }

    #[must_use]
    pub fn cel(&self) -> &CelClient<T> {
        &self.cel
    }

    #[must_use]
    pub fn tenant(&self) -> &TenantClient<T> {
        &self.tenant
    }

    pub async fn register_workflow<I, O>(
        &self,
        mut declaration: WorkflowDeclaration<I, O>,
    ) -> WorkflowDeclaration<I, O> {
        // Apply namespace to workflow name and triggers
        if let Some(ns) = &self.namespace {
            declaration.name = format!("{ns}/{}", declaration.name).to_lowercase();
            declaration.on_events = declaration
                .on_events
                .into_iter()
                .map(|event| format!("{ns}/{event}"))
                .collect();
        }
        let serialized = serde_json::to_value(&declaration).unwrap_or(Value::Null);
        let _ = self
            .workflows
            .put(&declaration.name, serialized.clone())
            .await;
        if let Ok(runtime_definition) =
            serde_json::from_value::<WorkflowDeclaration<Value, Value>>(serialized)
        {
            let _ = self
                .transport
                .register_workflow_definition(runtime_definition)
                .await;
        }
        declaration
    }

    /// Returns a typed `WorkflowRunRef` for an existing run ID.
    ///
    /// This provides access to the run's output, cancellation, and replay
    /// without having started the run from this client instance.
    #[must_use]
    pub fn run_ref<O>(&self, run_id: impl Into<String>) -> WorkflowRunRef<O>
    where
        O: serde::de::DeserializeOwned + Send + 'static,
    {
        WorkflowRunRef::new(WorkflowRunId(run_id.into()), Arc::new((*self).clone()))
    }
}

#[async_trait]
impl<T: Transport> RunControl for Client<T> {
    async fn output_run(&self, run_id: &WorkflowRunId) -> Result<Value> {
        self.transport.await_result(run_id).await
    }

    async fn cancel_run(&self, run_id: &WorkflowRunId) -> Result<()> {
        self.runs.cancel(run_id).await
    }

    async fn replay_run(&self, run_id: &WorkflowRunId) -> Result<WorkflowRunId> {
        let replay_id = self.runs.replay(run_id).await?;
        self.transport.register_result_channel(&replay_id).await?;
        Ok(replay_id)
    }

    async fn stream_run(&self, run_id: &WorkflowRunId) -> Result<Vec<Value>> {
        self.transport.stream_run(run_id).await
    }
}

pub struct WorkflowHandle<T: Transport, I, O> {
    workflow_name: String,
    transport: Arc<T>,
    state: Arc<RwLock<FeatureState>>,
    run_control: Arc<dyn RunControl>,
    input: PhantomData<fn(I)>,
    output: PhantomData<fn() -> O>,
}

impl<T: Transport, I, O> std::fmt::Debug for WorkflowHandle<T, I, O> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkflowHandle")
            .field("workflow_name", &self.workflow_name)
            .finish()
    }
}

impl<T: Transport, I, O> Clone for WorkflowHandle<T, I, O> {
    fn clone(&self) -> Self {
        Self {
            workflow_name: self.workflow_name.clone(),
            transport: Arc::clone(&self.transport),
            state: Arc::clone(&self.state),
            run_control: Arc::clone(&self.run_control),
            input: PhantomData,
            output: PhantomData,
        }
    }
}

impl<T: Transport, I, O> WorkflowHandle<T, I, O> {
    pub(crate) fn workflow_name_owned(&self) -> String {
        self.workflow_name.clone()
    }

    pub(crate) fn run_control(&self) -> Arc<dyn RunControl> {
        Arc::clone(&self.run_control)
    }
}

impl<T: Transport, I, O> WorkflowHandle<T, I, O>
where
    I: Serialize,
    O: DeserializeOwned + Send + 'static,
{
    #[must_use]
    pub fn workflow_name(&self) -> &str {
        &self.workflow_name
    }

    /// Submits one workflow run and waits for the typed output.
    ///
    /// # Errors
    ///
    /// Returns an error when input serialization fails or when the transport
    /// rejects the run request.
    pub async fn run(&self, input: I, opts: RunOpts) -> Result<O> {
        self.run_no_wait(input, opts).await?.output().await
    }

    /// Submits one workflow run without waiting for completion.
    ///
    /// # Errors
    ///
    /// Returns an error when input serialization fails or when the transport
    /// rejects the run request.
    pub async fn run_no_wait(&self, input: I, opts: RunOpts) -> Result<WorkflowRunRef<O>> {
        let input_value = serde_json::to_value(input).map_err(|error| {
            Error::InputSerialization(format!("workflow {}: {error}", self.workflow_name))
        })?;
        let run_id = self
            .transport
            .run_workflow(&self.workflow_name, input_value.clone(), opts.clone())
            .await?;
        record_run(
            &self.state,
            run_id.clone(),
            self.workflow_name.clone(),
            input_value,
            opts,
        )
        .await;
        self.transport.register_result_channel(&run_id).await?;
        Ok(WorkflowRunRef::new(run_id, Arc::clone(&self.run_control)))
    }

    /// Submits many workflow runs in one transport call without waiting for completion.
    ///
    /// # Errors
    ///
    /// Returns an error when any input serialization fails or when the
    /// transport rejects the bulk request.
    pub async fn run_many_no_wait(
        &self,
        inputs: impl IntoIterator<Item = I>,
        opts: RunOpts,
    ) -> Result<Vec<WorkflowRunRef<O>>> {
        let values = inputs
            .into_iter()
            .map(|input| {
                serde_json::to_value(input).map_err(|error| {
                    Error::InputSerialization(format!("workflow {}: {error}", self.workflow_name))
                })
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let run_ids = self
            .transport
            .run_workflows(&self.workflow_name, values.clone(), opts.clone())
            .await?;

        for (run_id, input_value) in run_ids.iter().cloned().zip(values.into_iter()) {
            record_run(
                &self.state,
                run_id.clone(),
                self.workflow_name.clone(),
                input_value,
                opts.clone(),
            )
            .await;
            self.transport.register_result_channel(&run_id).await?;
        }

        Ok(run_ids
            .into_iter()
            .map(|run_id| WorkflowRunRef::new(run_id, Arc::clone(&self.run_control)))
            .collect())
    }
}

#[derive(Debug, Clone)]
pub struct TaskHandle<I, O> {
    task_name: String,
    config: TaskConfig,
    input: PhantomData<fn(I)>,
    output: PhantomData<fn() -> O>,
}

impl<I, O> TaskHandle<I, O> {
    #[must_use]
    pub fn task_name(&self) -> &str {
        &self.task_name
    }

    #[must_use]
    pub fn config(&self) -> &TaskConfig {
        &self.config
    }

    #[must_use]
    pub fn with_config(mut self, config: TaskConfig) -> Self {
        self.config = config;
        self
    }
}

#[derive(Debug, Clone)]
pub struct DurableTaskHandle<I, O> {
    task_name: String,
    config: TaskConfig,
    input: PhantomData<fn(I)>,
    output: PhantomData<fn() -> O>,
}

impl<I, O> DurableTaskHandle<I, O> {
    #[must_use]
    pub fn task_name(&self) -> &str {
        &self.task_name
    }

    #[must_use]
    pub fn config(&self) -> &TaskConfig {
        &self.config
    }

    #[must_use]
    pub fn with_config(mut self, config: TaskConfig) -> Self {
        self.config = config;
        self
    }
}
