use std::marker::PhantomData;
use std::sync::Arc;

use hatchet_core::{RunOpts, WorkflowRunRef};
use serde::Serialize;

use crate::error::{Error, Result};
use crate::transport::Transport;

#[derive(Debug, Clone)]
pub struct Client<T: Transport> {
    transport: Arc<T>,
}

impl<T: Transport> Client<T> {
    #[must_use]
    pub fn new(transport: Arc<T>) -> Self {
        Self { transport }
    }

    #[must_use]
    pub fn workflow<I, O>(&self, workflow_name: impl Into<String>) -> WorkflowHandle<T, I, O> {
        WorkflowHandle {
            workflow_name: workflow_name.into(),
            transport: Arc::clone(&self.transport),
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
}

#[derive(Debug, Clone)]
pub struct WorkflowHandle<T: Transport, I, O> {
    workflow_name: String,
    transport: Arc<T>,
    input: PhantomData<fn(I)>,
    output: PhantomData<fn() -> O>,
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
        let run_id = self
            .transport
            .run_workflow(
                &self.workflow_name,
                serde_json::to_value(input).map_err(|error| {
                    Error::InputSerialization(format!("workflow {}: {error}", self.workflow_name))
                })?,
                opts,
            )
            .await?;
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
            .run_workflows(&self.workflow_name, values, opts)
            .await?;

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
