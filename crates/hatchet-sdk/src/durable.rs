use std::sync::Arc;
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use hatchet_core::{RunOpts, WorkflowRunId, WorkflowRunRef};
use serde::Serialize;
use serde_json::Value;
use tokio::sync::broadcast;
use tokio::time::{sleep, timeout};

use crate::client::WorkflowHandle;
use crate::context::{Context, ContextIds, InMemoryContext};
use crate::error::{Error, Result};
use crate::transport::Transport;

#[derive(Debug, Clone)]
pub struct DurableEvent {
    pub key: String,
    pub payload: Value,
}

#[async_trait]
pub trait DurableContext: Context {
    fn invocation_count(&self) -> u32;

    async fn now(&self) -> SystemTime;

    async fn sleep_for(&self, duration: Duration);

    async fn sleep_until(&self, at: SystemTime);

    async fn wait_for_event(
        &self,
        event_key: &str,
        timeout_after: Duration,
    ) -> Result<Option<Value>>;

    async fn spawn_child_json(
        &self,
        workflow_name: &str,
        input: Value,
        opts: RunOpts,
    ) -> Result<WorkflowRunId>;

    async fn spawn_children_json(
        &self,
        workflow_name: &str,
        inputs: Vec<Value>,
        opts: RunOpts,
    ) -> Result<Vec<WorkflowRunId>>;
}

#[derive(Debug)]
pub struct InMemoryDurableContext<T: Transport> {
    base: InMemoryContext<T>,
    invocation_count: u32,
    event_tx: broadcast::Sender<DurableEvent>,
}

impl<T: Transport> InMemoryDurableContext<T> {
    #[must_use]
    pub fn new(ids: ContextIds, transport: Arc<T>, invocation_count: u32) -> Self {
        let (event_tx, _) = broadcast::channel(128);
        Self {
            base: InMemoryContext::new(ids, transport),
            invocation_count,
            event_tx,
        }
    }

    /// Emits a test event into this in-memory durable context.
    ///
    /// # Errors
    ///
    /// Returns an error if the event channel has no active receiver.
    pub fn emit_event(&self, key: impl Into<String>, payload: Value) -> Result<()> {
        self.event_tx
            .send(DurableEvent {
                key: key.into(),
                payload,
            })
            .map(|_| ())
            .map_err(|error| Error::Durable(error.to_string()))
    }

    /// Spawns one child workflow from durable context.
    ///
    /// # Errors
    ///
    /// Returns an error when input serialization fails or when the transport
    /// rejects the spawn request.
    pub async fn spawn_child<I, O>(
        &self,
        workflow: &WorkflowHandle<T, I, O>,
        input: I,
        opts: RunOpts,
    ) -> Result<WorkflowRunRef<O>>
    where
        I: Serialize,
    {
        self.base.run_child(workflow, input, opts).await
    }

    /// Spawns many child workflows from durable context.
    ///
    /// # Errors
    ///
    /// Returns an error when any input serialization fails or when the
    /// transport rejects the bulk spawn request.
    pub async fn spawn_children<I, O>(
        &self,
        workflow: &WorkflowHandle<T, I, O>,
        inputs: impl IntoIterator<Item = I>,
        opts: RunOpts,
    ) -> Result<Vec<WorkflowRunRef<O>>>
    where
        I: Serialize,
    {
        self.base.bulk_run_children(workflow, inputs, opts).await
    }
}

#[async_trait]
impl<T: Transport> Context for InMemoryDurableContext<T> {
    fn workflow_name(&self) -> &str {
        self.base.workflow_name()
    }

    fn task_name(&self) -> &str {
        self.base.task_name()
    }

    fn workflow_run_id(&self) -> &WorkflowRunId {
        self.base.workflow_run_id()
    }

    fn task_run_id(&self) -> &str {
        self.base.task_run_id()
    }

    async fn run_child_json(
        &self,
        workflow_name: &str,
        input: Value,
        opts: RunOpts,
    ) -> Result<WorkflowRunId> {
        self.base.run_child_json(workflow_name, input, opts).await
    }

    async fn run_child_no_wait_json(
        &self,
        workflow_name: &str,
        input: Value,
        opts: RunOpts,
    ) -> Result<WorkflowRunId> {
        self.base
            .run_child_no_wait_json(workflow_name, input, opts)
            .await
    }

    async fn bulk_run_children_json(
        &self,
        workflow_name: &str,
        inputs: Vec<Value>,
        opts: RunOpts,
    ) -> Result<Vec<WorkflowRunId>> {
        self.base
            .bulk_run_children_json(workflow_name, inputs, opts)
            .await
    }

    async fn bulk_run_children_no_wait_json(
        &self,
        workflow_name: &str,
        inputs: Vec<Value>,
        opts: RunOpts,
    ) -> Result<Vec<WorkflowRunId>> {
        self.base
            .bulk_run_children_no_wait_json(workflow_name, inputs, opts)
            .await
    }
}

#[async_trait]
impl<T: Transport> DurableContext for InMemoryDurableContext<T> {
    fn invocation_count(&self) -> u32 {
        self.invocation_count
    }

    async fn now(&self) -> SystemTime {
        SystemTime::now()
    }

    async fn sleep_for(&self, duration: Duration) {
        sleep(duration).await;
    }

    async fn sleep_until(&self, at: SystemTime) {
        if let Ok(duration) = at.duration_since(SystemTime::now()) {
            sleep(duration).await;
        }
    }

    async fn wait_for_event(
        &self,
        event_key: &str,
        timeout_after: Duration,
    ) -> Result<Option<Value>> {
        let mut receiver = self.event_tx.subscribe();
        let expected_key = event_key.to_owned();

        let wait_result = timeout(timeout_after, async move {
            loop {
                match receiver.recv().await {
                    Ok(event) if event.key == expected_key => {
                        return Ok::<Value, Error>(event.payload);
                    }
                    Ok(_) | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        return Err(Error::Durable("event channel closed".to_owned()));
                    }
                }
            }
        })
        .await;

        match wait_result {
            Ok(event) => event.map(Some),
            Err(_) => Ok(None),
        }
    }

    async fn spawn_child_json(
        &self,
        workflow_name: &str,
        input: Value,
        opts: RunOpts,
    ) -> Result<WorkflowRunId> {
        self.base.run_child_json(workflow_name, input, opts).await
    }

    async fn spawn_children_json(
        &self,
        workflow_name: &str,
        inputs: Vec<Value>,
        opts: RunOpts,
    ) -> Result<Vec<WorkflowRunId>> {
        self.base
            .bulk_run_children_json(workflow_name, inputs, opts)
            .await
    }
}
