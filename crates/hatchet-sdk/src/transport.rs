use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use hatchet_core::{RunOpts, WorkflowRunId};
use serde_json::Value;
use tokio::sync::RwLock;

use crate::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct SubmittedRun {
    pub workflow_name: String,
    pub input: Value,
    pub opts: RunOpts,
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
}

#[derive(Debug, Default)]
pub struct InMemoryTransport {
    counter: AtomicU64,
    runs: Arc<RwLock<Vec<SubmittedRun>>>,
}

impl InMemoryTransport {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub async fn submitted_runs(&self) -> Vec<SubmittedRun> {
        self.runs.read().await.clone()
    }

    fn next_run_id(&self) -> WorkflowRunId {
        let next = self.counter.fetch_add(1, Ordering::Relaxed) + 1;
        WorkflowRunId(format!("run-{next}"))
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
        let run = SubmittedRun {
            workflow_name: workflow_name.to_owned(),
            input,
            opts,
        };
        self.runs.write().await.push(run);
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

        for input in inputs {
            let run_id = self.next_run_id();
            run_ids.push(run_id);
            runs.push(SubmittedRun {
                workflow_name: workflow_name.to_owned(),
                input,
                opts: opts.clone(),
            });
        }

        Ok(run_ids)
    }
}

impl From<tokio::sync::AcquireError> for Error {
    fn from(error: tokio::sync::AcquireError) -> Self {
        Self::Transport(error.to_string())
    }
}
