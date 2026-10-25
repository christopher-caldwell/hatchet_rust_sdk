use std::future::{Future, IntoFuture};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;

use another_hatchet_core::WorkflowRunId;
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::RunEventStream;
use crate::error::{Error, Result};

pub type BoxRunFuture<O> = Pin<Box<dyn Future<Output = Result<O>> + Send + 'static>>;

#[async_trait]
pub trait RunControl: Send + Sync {
    async fn output_run(&self, run_id: &WorkflowRunId) -> Result<Value>;
    async fn cancel_run(&self, run_id: &WorkflowRunId) -> Result<()>;
    async fn replay_run(&self, run_id: &WorkflowRunId) -> Result<WorkflowRunId>;
    async fn stream_run(&self, run_id: &WorkflowRunId) -> Result<Vec<Value>>;

    /// Custom controls may override this to provide an incremental subscription.
    async fn subscribe_run_stream(&self, run_id: &WorkflowRunId) -> Result<RunEventStream> {
        Ok(RunEventStream::from_values(self.stream_run(run_id).await?))
    }
}

#[derive(Clone)]
pub struct WorkflowRunRef<O> {
    run_id: WorkflowRunId,
    client: Arc<dyn RunControl>,
    output: PhantomData<fn() -> O>,
}

impl<O> std::fmt::Debug for WorkflowRunRef<O> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkflowRunRef")
            .field("run_id", &self.run_id)
            .finish()
    }
}

impl<O> WorkflowRunRef<O> {
    #[must_use]
    pub fn new(run_id: WorkflowRunId, client: Arc<dyn RunControl>) -> Self {
        Self {
            run_id,
            client,
            output: PhantomData,
        }
    }

    #[must_use]
    pub fn run_id(&self) -> &WorkflowRunId {
        &self.run_id
    }
}

impl<O: DeserializeOwned + Send + 'static> WorkflowRunRef<O> {
    pub async fn output(&self) -> Result<O> {
        let value = self.client.output_run(&self.run_id).await?;
        serde_json::from_value(value)
            .map_err(|error| Error::OutputDeserialization(error.to_string()))
    }

    pub async fn cancel(&self) -> Result<()> {
        self.client.cancel_run(&self.run_id).await
    }

    pub async fn replay(&self) -> Result<Self> {
        let run_id = self.client.replay_run(&self.run_id).await?;
        Ok(Self::new(run_id, Arc::clone(&self.client)))
    }

    pub async fn stream(&self) -> Result<Vec<Value>> {
        self.client.stream_run(&self.run_id).await
    }

    /// Opens an incremental stream; values can be read before the run completes.
    pub async fn stream_events(&self) -> Result<RunEventStream> {
        self.client.subscribe_run_stream(&self.run_id).await
    }
}

impl<O: DeserializeOwned + Send + 'static> IntoFuture for WorkflowRunRef<O> {
    type Output = Result<O>;
    type IntoFuture = BoxRunFuture<O>;

    fn into_future(self) -> Self::IntoFuture {
        Box::pin(async move { self.output().await })
    }
}
