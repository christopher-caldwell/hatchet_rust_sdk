use std::marker::PhantomData;
use std::sync::Arc;

use hatchet_core::{RunOpts, WorkflowRunRef};
use serde::Serialize;
use tokio::sync::RwLock;

use crate::clients::{
    CelClient, CronsClient, EventsClient, FeatureState, FiltersClient, LogsClient, MetricsClient,
    RateLimitsClient, RunsClient, SchedulesClient, WebhooksClient, WorkersClient, WorkflowsClient,
    record_run,
};
use crate::error::{Error, Result};
use crate::transport::Transport;
use crate::worker::WorkerBuilder;

#[derive(Debug, Clone)]
pub struct Client<T: Transport> {
    transport: Arc<T>,
    state: Arc<RwLock<FeatureState>>,
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
}

impl<T: Transport> Client<T> {
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
            transport,
            state,
        }
    }

    #[must_use]
    pub fn workflow<I, O>(&self, workflow_name: impl Into<String>) -> WorkflowHandle<T, I, O> {
        WorkflowHandle {
            workflow_name: workflow_name.into(),
            transport: Arc::clone(&self.transport),
            state: Arc::clone(&self.state),
            input: PhantomData,
            output: PhantomData,
        }
    }

    #[must_use]
    pub fn task<I, O>(&self, task_name: impl Into<String>) -> TaskHandle<I, O> {
        TaskHandle {
            task_name: task_name.into(),
            input: PhantomData,
            output: PhantomData,
        }
    }

    #[must_use]
    pub fn durable_task<I, O>(&self, task_name: impl Into<String>) -> DurableTaskHandle<I, O> {
        DurableTaskHandle {
            task_name: task_name.into(),
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
}

#[derive(Debug)]
pub struct WorkflowHandle<T: Transport, I, O> {
    workflow_name: String,
    transport: Arc<T>,
    state: Arc<RwLock<FeatureState>>,
    input: PhantomData<fn(I)>,
    output: PhantomData<fn() -> O>,
}

impl<T: Transport, I, O> Clone for WorkflowHandle<T, I, O> {
    fn clone(&self) -> Self {
        Self {
            workflow_name: self.workflow_name.clone(),
            transport: Arc::clone(&self.transport),
            state: Arc::clone(&self.state),
            input: PhantomData,
            output: PhantomData,
        }
    }
}

impl<T: Transport, I, O> WorkflowHandle<T, I, O>
where
    I: Serialize,
{
    #[must_use]
    pub fn workflow_name(&self) -> &str {
        &self.workflow_name
    }

    pub(crate) fn workflow_name_owned(&self) -> String {
        self.workflow_name.clone()
    }

    /// Submits one workflow run and returns a typed run reference.
    ///
    /// # Errors
    ///
    /// Returns an error when input serialization fails or when the transport
    /// rejects the run request.
    pub async fn run(&self, input: I, opts: RunOpts) -> Result<WorkflowRunRef<O>> {
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
        Ok(WorkflowRunRef::new(run_id))
    }

    /// Submits one workflow run without waiting for completion.
    ///
    /// # Errors
    ///
    /// Returns an error when input serialization fails or when the transport
    /// rejects the run request.
    pub async fn run_no_wait(&self, input: I, opts: RunOpts) -> Result<WorkflowRunRef<O>> {
        self.run(input, opts).await
    }

    /// Submits many workflow runs in one transport call.
    ///
    /// # Errors
    ///
    /// Returns an error when any input serialization fails or when the
    /// transport rejects the bulk request.
    pub async fn run_many(
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
                run_id,
                self.workflow_name.clone(),
                input_value,
                opts.clone(),
            )
            .await;
        }

        Ok(run_ids.into_iter().map(WorkflowRunRef::new).collect())
    }
}

#[derive(Debug, Clone)]
pub struct TaskHandle<I, O> {
    task_name: String,
    input: PhantomData<fn(I)>,
    output: PhantomData<fn() -> O>,
}

impl<I, O> TaskHandle<I, O> {
    #[must_use]
    pub fn task_name(&self) -> &str {
        &self.task_name
    }
}

#[derive(Debug, Clone)]
pub struct DurableTaskHandle<I, O> {
    task_name: String,
    input: PhantomData<fn(I)>,
    output: PhantomData<fn() -> O>,
}

impl<I, O> DurableTaskHandle<I, O> {
    #[must_use]
    pub fn task_name(&self) -> &str {
        &self.task_name
    }
}
