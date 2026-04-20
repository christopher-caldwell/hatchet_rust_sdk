use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use hatchet_core::{RunOpts, WorkflowRunId, WorkflowRunRef};
use serde::Serialize;
use serde_json::Value;

use crate::client::WorkflowHandle;
use crate::error::{Error, Result};
use crate::transport::Transport;

#[derive(Debug, Clone)]
pub struct ContextIds {
    pub workflow_name: String,
    pub task_name: String,
    pub workflow_run_id: WorkflowRunId,
    pub task_run_id: String,
}

#[async_trait]
pub trait Context {
    fn workflow_name(&self) -> &str;
    fn task_name(&self) -> &str;
    fn workflow_run_id(&self) -> &WorkflowRunId;
    fn task_run_id(&self) -> &str;

    async fn run_child_json(
        &self,
        workflow_name: &str,
        input: Value,
        opts: RunOpts,
    ) -> Result<WorkflowRunId>;

    async fn run_child_no_wait_json(
        &self,
        workflow_name: &str,
        input: Value,
        opts: RunOpts,
    ) -> Result<WorkflowRunId>;

    async fn bulk_run_children_json(
        &self,
        workflow_name: &str,
        inputs: Vec<Value>,
        opts: RunOpts,
    ) -> Result<Vec<WorkflowRunId>>;

    async fn bulk_run_children_no_wait_json(
        &self,
        workflow_name: &str,
        inputs: Vec<Value>,
        opts: RunOpts,
    ) -> Result<Vec<WorkflowRunId>>;
}

#[derive(Debug)]
pub struct InMemoryContext<T: Transport> {
    ids: ContextIds,
    transport: Arc<T>,
    child_index: AtomicU64,
}

impl<T: Transport> InMemoryContext<T> {
    #[must_use]
    pub fn new(ids: ContextIds, transport: Arc<T>) -> Self {
        Self {
            ids,
            transport,
            child_index: AtomicU64::new(0),
        }
    }

    /// Submits a child workflow run and returns a typed run reference.
    ///
    /// # Errors
    ///
    /// Returns an error when input serialization fails or when the transport
    /// rejects the run request.
    pub async fn run_child<I, O>(
        &self,
        workflow: &WorkflowHandle<T, I, O>,
        input: I,
        opts: RunOpts,
    ) -> Result<WorkflowRunRef<O>>
    where
        I: Serialize,
    {
        let workflow_name = workflow.workflow_name_owned();
        let run_id = self
            .run_child_json(
                &workflow_name,
                serde_json::to_value(input).map_err(|error| {
                    Error::InputSerialization(format!("workflow {workflow_name}: {error}"))
                })?,
                opts,
            )
            .await?;
        Ok(WorkflowRunRef::new(run_id))
    }

    /// Submits a child workflow run without waiting for completion.
    ///
    /// # Errors
    ///
    /// Returns an error when input serialization fails or when the transport
    /// rejects the run request.
    pub async fn run_child_no_wait<I, O>(
        &self,
        workflow: &WorkflowHandle<T, I, O>,
        input: I,
        opts: RunOpts,
    ) -> Result<WorkflowRunRef<O>>
    where
        I: Serialize,
    {
        let workflow_name = workflow.workflow_name_owned();
        let run_id = self
            .run_child_no_wait_json(
                &workflow_name,
                serde_json::to_value(input).map_err(|error| {
                    Error::InputSerialization(format!("workflow {workflow_name}: {error}"))
                })?,
                opts,
            )
            .await?;
        Ok(WorkflowRunRef::new(run_id))
    }

    /// Submits many child workflow runs in one transport call.
    ///
    /// # Errors
    ///
    /// Returns an error when any input serialization fails or when the
    /// transport rejects the bulk request.
    pub async fn bulk_run_children<I, O>(
        &self,
        workflow: &WorkflowHandle<T, I, O>,
        inputs: impl IntoIterator<Item = I>,
        opts: RunOpts,
    ) -> Result<Vec<WorkflowRunRef<O>>>
    where
        I: Serialize,
    {
        let workflow_name = workflow.workflow_name_owned();
        let values = inputs
            .into_iter()
            .map(|input| {
                serde_json::to_value(input).map_err(|error| {
                    Error::InputSerialization(format!("workflow {workflow_name}: {error}"))
                })
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let run_ids = self
            .bulk_run_children_json(&workflow_name, values, opts)
            .await?;

        Ok(run_ids.into_iter().map(WorkflowRunRef::new).collect())
    }

    /// Submits many child workflow runs without waiting for completion.
    ///
    /// # Errors
    ///
    /// Returns an error when any input serialization fails or when the
    /// transport rejects the bulk request.
    pub async fn bulk_run_children_no_wait<I, O>(
        &self,
        workflow: &WorkflowHandle<T, I, O>,
        inputs: impl IntoIterator<Item = I>,
        opts: RunOpts,
    ) -> Result<Vec<WorkflowRunRef<O>>>
    where
        I: Serialize,
    {
        let workflow_name = workflow.workflow_name_owned();
        let values = inputs
            .into_iter()
            .map(|input| {
                serde_json::to_value(input).map_err(|error| {
                    Error::InputSerialization(format!("workflow {workflow_name}: {error}"))
                })
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let run_ids = self
            .bulk_run_children_no_wait_json(&workflow_name, values, opts)
            .await?;

        Ok(run_ids.into_iter().map(WorkflowRunRef::new).collect())
    }

    fn child_run_opts(&self, mut opts: RunOpts) -> RunOpts {
        let child_index = self.child_index.fetch_add(1, Ordering::Relaxed);
        opts.metadata.insert(
            "parent_workflow_run_id".to_owned(),
            Value::String(self.ids.workflow_run_id.0.clone()),
        );
        opts.metadata.insert(
            "parent_task_run_id".to_owned(),
            Value::String(self.ids.task_run_id.clone()),
        );
        opts.metadata
            .insert("child_index".to_owned(), Value::Number(child_index.into()));
        opts
    }
}

#[async_trait]
impl<T: Transport> Context for InMemoryContext<T> {
    fn workflow_name(&self) -> &str {
        &self.ids.workflow_name
    }

    fn task_name(&self) -> &str {
        &self.ids.task_name
    }

    fn workflow_run_id(&self) -> &WorkflowRunId {
        &self.ids.workflow_run_id
    }

    fn task_run_id(&self) -> &str {
        &self.ids.task_run_id
    }

    async fn run_child_json(
        &self,
        workflow_name: &str,
        input: Value,
        opts: RunOpts,
    ) -> Result<WorkflowRunId> {
        self.transport
            .run_workflow(workflow_name, input, self.child_run_opts(opts))
            .await
    }

    async fn run_child_no_wait_json(
        &self,
        workflow_name: &str,
        input: Value,
        opts: RunOpts,
    ) -> Result<WorkflowRunId> {
        self.run_child_json(workflow_name, input, opts).await
    }

    async fn bulk_run_children_json(
        &self,
        workflow_name: &str,
        inputs: Vec<Value>,
        opts: RunOpts,
    ) -> Result<Vec<WorkflowRunId>> {
        self.transport
            .run_workflows(workflow_name, inputs, self.child_run_opts(opts))
            .await
    }

    async fn bulk_run_children_no_wait_json(
        &self,
        workflow_name: &str,
        inputs: Vec<Value>,
        opts: RunOpts,
    ) -> Result<Vec<WorkflowRunId>> {
        self.bulk_run_children_json(workflow_name, inputs, opts)
            .await
    }
}
