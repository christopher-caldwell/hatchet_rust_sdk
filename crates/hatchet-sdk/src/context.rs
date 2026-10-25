use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use another_hatchet_core::{Priority, RunOpts, WorkflowRunId};
use async_trait::async_trait;
use serde::Serialize;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

use crate::client::WorkflowHandle;
use crate::error::{Error, Result};
use crate::run_ref::WorkflowRunRef;
use crate::task::LabelValue;
use crate::transport::Transport;

#[derive(Debug, Clone)]
pub struct ContextIds {
    pub workflow_name: String,
    pub task_name: String,
    pub workflow_run_id: WorkflowRunId,
    pub task_run_id: String,
    pub workflow_id: Option<String>,
    pub workflow_version_id: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, Default)]
pub struct TriggerData {
    pub triggered_by_cron: bool,
    pub triggered_by_event: bool,
    pub cron_expression: Option<String>,
    pub event_key: Option<String>,
    pub scheduled_at: Option<String>,
}

#[async_trait]
pub trait Context {
    fn workflow_name(&self) -> &str;
    fn task_name(&self) -> &str;
    fn workflow_run_id(&self) -> &WorkflowRunId;
    fn task_run_id(&self) -> &str;
    fn retry_count(&self) -> u32;
    fn child_index(&self) -> Option<u64>;
    fn child_key(&self) -> Option<&str>;
    fn parent_workflow_run_id(&self) -> Option<&str>;
    fn additional_metadata(&self) -> &std::collections::HashMap<String, String>;
    fn priority(&self) -> Option<Priority>;
    fn triggered_by_event(&self) -> bool;
    fn filter_payload(&self) -> Option<&Value>;
    fn worker_id(&self) -> Option<&str>;
    fn worker_labels(&self) -> std::collections::BTreeMap<String, LabelValue>;
    fn workflow_id(&self) -> Option<&str>;
    fn workflow_version_id(&self) -> Option<&str>;
    fn triggers(&self) -> &TriggerData;
    fn errors(&self) -> &std::collections::HashMap<String, String>;

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

    async fn parent_output_json(&self, parent_task_name: &str) -> Result<Value>;

    async fn log(&self, level: LogLevel, message: impl Into<String> + Send) -> Result<()>;

    async fn refresh_timeout(&self) -> Result<()>;

    async fn release_slot(&self) -> Result<()>;

    async fn put_stream(&self, payload: Value) -> Result<()>;

    async fn upsert_worker_labels(
        &self,
        labels: std::collections::BTreeMap<String, LabelValue>,
    ) -> Result<()>;

    /// Member task-run UUIDs for a server-coalesced batch; empty for normal tasks.
    /// Batch context identity names the batch, not an individual workflow run.
    fn batch_member_ids(&self) -> &[String] {
        &[]
    }
    /// Cancels the current workflow run, or all members of the current batch.
    async fn cancel(&self) -> Result<()>;
}

#[derive(Debug)]
pub struct InMemoryContext<T: Transport> {
    ids: ContextIds,
    transport: Arc<T>,
    child_index: AtomicU64,
    additional_metadata: std::collections::HashMap<String, String>,
    priority: Option<Priority>,
    child_key: Option<String>,
    parent_workflow_run_id: Option<String>,
    current_child_index: Option<u64>,
    retry_count: u32,
    filter_payload: Option<Value>,
    parent_outputs: std::collections::HashMap<String, Value>,
    worker_labels: std::sync::RwLock<std::collections::BTreeMap<String, LabelValue>>,
    worker_id: Option<String>,
    trigger_data: TriggerData,
    errors: std::collections::HashMap<String, String>,
    cancel_token: CancellationToken,
    batch_member_ids: Vec<String>,
    batch_cancel_token: Option<CancellationToken>,
    logs: tokio::sync::Mutex<Vec<(LogLevel, String)>>,
    stream: tokio::sync::Mutex<Vec<Value>>,
}

impl<T: Transport> InMemoryContext<T> {
    #[must_use]
    pub fn new(ids: ContextIds, transport: Arc<T>) -> Self {
        Self {
            ids,
            transport,
            child_index: AtomicU64::new(0),
            additional_metadata: std::collections::HashMap::new(),
            priority: None,
            child_key: None,
            parent_workflow_run_id: None,
            current_child_index: None,
            retry_count: 0,
            filter_payload: None,
            parent_outputs: std::collections::HashMap::new(),
            worker_labels: std::sync::RwLock::new(std::collections::BTreeMap::new()),
            worker_id: None,
            trigger_data: TriggerData::default(),
            errors: std::collections::HashMap::new(),
            cancel_token: CancellationToken::new(),
            batch_member_ids: Vec::new(),
            batch_cancel_token: None,
            logs: tokio::sync::Mutex::new(Vec::new()),
            stream: tokio::sync::Mutex::new(Vec::new()),
        }
    }

    pub(crate) fn with_batch_member_ids(
        mut self,
        ids: Vec<String>,
        cancel_token: Option<CancellationToken>,
    ) -> Self {
        self.batch_member_ids = ids;
        self.batch_cancel_token = cancel_token;
        self
    }

    pub(crate) fn transport_namespace(&self) -> Option<&str> {
        self.transport.namespace()
    }

    #[must_use]
    pub fn with_run_opts(mut self, opts: &RunOpts) -> Self {
        self.additional_metadata = opts.additional_metadata.clone();
        self.priority = opts.priority;
        self.child_key = opts.child_key.clone();
        self.parent_workflow_run_id = opts.parent_workflow_run_id.clone();
        self.current_child_index = opts.child_index;
        self
    }

    #[must_use]
    pub fn with_retry_count(mut self, retry_count: u32) -> Self {
        self.retry_count = retry_count;
        self
    }

    #[must_use]
    pub fn with_parent_output(
        mut self,
        parent_task_name: impl Into<String>,
        parent_output: Value,
    ) -> Self {
        self.parent_outputs
            .insert(parent_task_name.into(), parent_output);
        self
    }

    #[must_use]
    pub fn with_parent_outputs(
        mut self,
        parent_outputs: std::collections::HashMap<String, Value>,
    ) -> Self {
        self.parent_outputs = parent_outputs;
        self
    }

    #[must_use]
    pub fn with_filter_payload(mut self, filter_payload: Value) -> Self {
        self.filter_payload = Some(filter_payload);
        self
    }

    #[must_use]
    pub fn with_worker_id(mut self, worker_id: impl Into<String>) -> Self {
        self.worker_id = Some(worker_id.into());
        self
    }

    #[must_use]
    pub fn with_worker_label(self, key: impl Into<String>, value: impl Into<LabelValue>) -> Self {
        self.worker_labels
            .write()
            .expect("worker labels lock poisoned")
            .insert(key.into(), value.into());
        self
    }

    #[must_use]
    pub fn with_worker_labels(self, labels: std::collections::BTreeMap<String, String>) -> Self {
        let mut current = self
            .worker_labels
            .write()
            .expect("worker labels lock poisoned");
        for (key, value) in labels {
            current.insert(key, LabelValue::String(value));
        }
        drop(current);
        self
    }

    #[must_use]
    pub fn with_trigger_data(mut self, trigger_data: TriggerData) -> Self {
        self.trigger_data = trigger_data;
        self
    }

    #[must_use]
    pub fn with_errors(mut self, errors: std::collections::HashMap<String, String>) -> Self {
        self.errors = errors;
        self
    }

    /// Provides an external cancellation token.
    ///
    /// When the engine sends a cancel signal via the dispatcher stream,
    /// this token is triggered to propagate cancellation to the running task.
    #[must_use]
    pub fn with_cancel_token(mut self, token: CancellationToken) -> Self {
        self.cancel_token = token;
        self
    }

    /// Returns `true` if this context's cancellation token has been triggered.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancel_token.is_cancelled()
    }

    /// Returns a future that completes when the cancellation token is triggered.
    ///
    /// Useful for `tokio::select!` patterns:
    /// ```ignore
    /// tokio::select! {
    ///     _ = ctx.cancelled() => { /* handle cancel */ }
    ///     result = do_work() => { /* handle result */ }
    /// }
    /// ```
    pub fn cancelled(&self) -> tokio_util::sync::WaitForCancellationFuture<'_> {
        self.cancel_token.cancelled()
    }

    /// Returns a clone of the cancellation token.
    #[must_use]
    pub fn cancel_token(&self) -> CancellationToken {
        self.cancel_token.clone()
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
        Ok(WorkflowRunRef::new(run_id, workflow.run_control()))
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
        Ok(WorkflowRunRef::new(run_id, workflow.run_control()))
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
        inputs: impl IntoIterator<Item = I> + Send,
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

        Ok(run_ids
            .into_iter()
            .map(|run_id| WorkflowRunRef::new(run_id, workflow.run_control()))
            .collect())
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
        inputs: impl IntoIterator<Item = I> + Send,
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

        Ok(run_ids
            .into_iter()
            .map(|run_id| WorkflowRunRef::new(run_id, workflow.run_control()))
            .collect())
    }

    pub(crate) fn child_run_opts(&self, mut opts: RunOpts) -> RunOpts {
        crate::telemetry::inject_current(&mut opts.additional_metadata);
        let child_index = self.child_index.fetch_add(1, Ordering::Relaxed);
        opts.parent_workflow_run_id = Some(self.ids.workflow_run_id.0.clone());
        opts.parent_task_run_id = Some(self.ids.task_run_id.clone());
        opts.child_index = Some(child_index);
        opts
    }

    pub(crate) fn bulk_child_run_opts(&self, mut opts: RunOpts, child_count: usize) -> RunOpts {
        crate::telemetry::inject_current(&mut opts.additional_metadata);
        let first_child_index = self
            .child_index
            .fetch_add(child_count as u64, Ordering::Relaxed);
        opts.parent_workflow_run_id = Some(self.ids.workflow_run_id.0.clone());
        opts.parent_task_run_id = Some(self.ids.task_run_id.clone());
        opts.child_index = Some(first_child_index);
        opts
    }

    pub(crate) async fn cache_run_result(&self, run_id: &WorkflowRunId, result: Result<Value>) {
        self.transport.resolve_result(run_id, result).await;
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

    fn retry_count(&self) -> u32 {
        self.retry_count
    }

    fn child_index(&self) -> Option<u64> {
        self.current_child_index
    }

    fn child_key(&self) -> Option<&str> {
        self.child_key.as_deref()
    }

    fn parent_workflow_run_id(&self) -> Option<&str> {
        self.parent_workflow_run_id.as_deref()
    }

    fn additional_metadata(&self) -> &std::collections::HashMap<String, String> {
        &self.additional_metadata
    }

    fn priority(&self) -> Option<Priority> {
        self.priority
    }

    fn triggered_by_event(&self) -> bool {
        self.trigger_data.triggered_by_event
    }

    fn filter_payload(&self) -> Option<&Value> {
        self.filter_payload.as_ref()
    }

    fn worker_id(&self) -> Option<&str> {
        self.worker_id.as_deref()
    }

    fn worker_labels(&self) -> std::collections::BTreeMap<String, LabelValue> {
        self.worker_labels
            .read()
            .expect("worker labels lock poisoned")
            .clone()
    }

    fn workflow_id(&self) -> Option<&str> {
        self.ids.workflow_id.as_deref()
    }

    fn workflow_version_id(&self) -> Option<&str> {
        self.ids.workflow_version_id.as_deref()
    }

    fn triggers(&self) -> &TriggerData {
        &self.trigger_data
    }

    fn errors(&self) -> &std::collections::HashMap<String, String> {
        &self.errors
    }

    async fn run_child_json(
        &self,
        workflow_name: &str,
        input: Value,
        opts: RunOpts,
    ) -> Result<WorkflowRunId> {
        let run_id = self
            .transport
            .run_workflow_traced(workflow_name, input, self.child_run_opts(opts))
            .await?;
        self.transport.register_result_channel(&run_id).await?;
        Ok(run_id)
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
        let child_count = inputs.len();
        let run_ids = self
            .transport
            .run_workflows_traced(
                workflow_name,
                inputs,
                self.bulk_child_run_opts(opts, child_count),
            )
            .await?;
        for run_id in &run_ids {
            self.transport.register_result_channel(run_id).await?;
        }
        Ok(run_ids)
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

    async fn parent_output_json(&self, parent_task_name: &str) -> Result<Value> {
        self.parent_outputs
            .get(parent_task_name)
            .cloned()
            .ok_or_else(|| {
                Error::Worker(format!(
                    "output for parent task '{parent_task_name}' was not found"
                ))
            })
    }

    async fn log(&self, level: LogLevel, message: impl Into<String> + Send) -> Result<()> {
        let message = message.into();
        self.transport
            .append_run_log(&self.ids.workflow_run_id, level, message.clone())
            .await;
        self.logs.lock().await.push((level, message));
        Ok(())
    }

    async fn refresh_timeout(&self) -> Result<()> {
        self.transport
            .refresh_timeout(self.task_run_id(), "60s")
            .await
    }

    async fn release_slot(&self) -> Result<()> {
        self.transport.release_slot(self.task_run_id()).await
    }

    async fn put_stream(&self, payload: Value) -> Result<()> {
        self.transport
            .append_run_stream(
                &self.ids.workflow_run_id,
                &self.ids.task_run_id,
                payload.clone(),
            )
            .await?;
        self.stream.lock().await.push(payload);
        Ok(())
    }

    async fn upsert_worker_labels(
        &self,
        labels: std::collections::BTreeMap<String, LabelValue>,
    ) -> Result<()> {
        let mut current = self
            .worker_labels
            .write()
            .map_err(|_| Error::Worker("worker labels lock poisoned".to_owned()))?;
        current.extend(labels);
        Ok(())
    }

    fn batch_member_ids(&self) -> &[String] {
        &self.batch_member_ids
    }
    async fn cancel(&self) -> Result<()> {
        if !self.batch_member_ids.is_empty() {
            self.transport
                .cancel_task_runs(&self.batch_member_ids)
                .await?;
            // Publish cancellation synchronously to the transport completion guard.
            // WorkerRuntime's combined token only propagates cancellation inward.
            if let Some(token) = &self.batch_cancel_token {
                token.cancel();
            }
            self.cancel_token.cancel();
            return Ok(());
        }
        self.cancel_token.cancel();
        self.transport
            .cancel_workflow_run(&self.ids.workflow_run_id)
            .await
    }
}

#[async_trait]
pub trait ContextExt: Context {
    async fn run_child<I, O, T>(
        &self,
        workflow: &WorkflowHandle<T, I, O>,
        input: I,
        opts: RunOpts,
    ) -> Result<O>
    where
        T: Transport,
        I: Serialize + Send,
        O: serde::de::DeserializeOwned + Send + 'static,
    {
        self.run_child_no_wait(workflow, input, opts)
            .await?
            .output()
            .await
    }

    async fn run_child_no_wait<I, O, T>(
        &self,
        workflow: &WorkflowHandle<T, I, O>,
        input: I,
        opts: RunOpts,
    ) -> Result<WorkflowRunRef<O>>
    where
        T: Transport,
        I: Serialize + Send,
        O: serde::de::DeserializeOwned + Send + 'static,
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
        Ok(WorkflowRunRef::new(run_id, workflow.run_control()))
    }

    async fn bulk_run_children<I, O, T>(
        &self,
        workflow: &WorkflowHandle<T, I, O>,
        inputs: impl IntoIterator<Item = I> + Send,
        opts: RunOpts,
    ) -> Result<Vec<O>>
    where
        T: Transport,
        I: Serialize + Send,
        O: serde::de::DeserializeOwned + Send + 'static,
    {
        let refs = self
            .bulk_run_children_no_wait(workflow, inputs, opts)
            .await?;
        let mut outputs = Vec::with_capacity(refs.len());
        for run_ref in refs {
            outputs.push(run_ref.output().await?);
        }
        Ok(outputs)
    }

    async fn bulk_run_children_no_wait<I, O, T>(
        &self,
        workflow: &WorkflowHandle<T, I, O>,
        inputs: impl IntoIterator<Item = I> + Send,
        opts: RunOpts,
    ) -> Result<Vec<WorkflowRunRef<O>>>
    where
        T: Transport,
        I: Serialize + Send,
        O: serde::de::DeserializeOwned + Send + 'static,
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

        Ok(run_ids
            .into_iter()
            .map(|run_id| WorkflowRunRef::new(run_id, workflow.run_control()))
            .collect())
    }

    async fn parent_output<O: serde::de::DeserializeOwned>(
        &self,
        parent_task_name: &str,
    ) -> Result<O> {
        serde_json::from_value(self.parent_output_json(parent_task_name).await?)
            .map_err(|error| Error::OutputDeserialization(error.to_string()))
    }
}

impl<T: Context + ?Sized> ContextExt for T {}

#[cfg(test)]
mod batch_cancellation_tests {
    use super::*;
    struct CancelTransport {
        fail: bool,
    }
    #[async_trait]
    impl Transport for CancelTransport {
        async fn cancel_task_runs(&self, ids: &[String]) -> Result<()> {
            assert_eq!(ids, &["member-1", "member-2"]);
            if self.fail {
                Err(Error::Transport("cancellation rejected".into()))
            } else {
                Ok(())
            }
        }
        async fn run_workflow(&self, _: &str, _: Value, _: RunOpts) -> Result<WorkflowRunId> {
            unreachable!()
        }
        async fn run_workflows(
            &self,
            _: &str,
            _: Vec<Value>,
            _: RunOpts,
        ) -> Result<Vec<WorkflowRunId>> {
            unreachable!()
        }
        async fn register_result_channel(&self, _: &WorkflowRunId) -> Result<()> {
            unreachable!()
        }
        async fn await_result(&self, _: &WorkflowRunId) -> Result<Value> {
            unreachable!()
        }
        async fn resolve_result(&self, _: &WorkflowRunId, _: Result<Value>) {
            unreachable!()
        }
        async fn start_worker_loop(&self, _: crate::transport::WorkerStartOpts) -> Result<()> {
            unreachable!()
        }
        async fn register_workflow_definition(
            &self,
            _: crate::WorkflowDeclaration<Value, Value>,
        ) -> Result<()> {
            unreachable!()
        }
        async fn append_run_stream(&self, _: &WorkflowRunId, _: &str, _: Value) -> Result<()> {
            unreachable!()
        }
        async fn stream_run(&self, _: &WorkflowRunId) -> Result<Vec<Value>> {
            unreachable!()
        }
        async fn append_run_log(&self, _: &WorkflowRunId, _: LogLevel, _: String) {
            unreachable!()
        }
        async fn refresh_timeout(&self, _: &str, _: &str) -> Result<()> {
            unreachable!()
        }
        async fn release_slot(&self, _: &str) -> Result<()> {
            unreachable!()
        }
        async fn list_logs(&self, _: &str) -> Result<Vec<crate::clients::LogRecord>> {
            unreachable!()
        }
    }
    #[tokio::test]
    async fn acknowledged_batch_cancel_synchronously_cancels_transport_guard_but_rejection_does_not()
     {
        for fail in [false, true] {
            let original = CancellationToken::new();
            let combined = CancellationToken::new();
            let ctx = InMemoryContext::new(
                ContextIds {
                    workflow_name: String::new(),
                    task_name: "batch".into(),
                    workflow_run_id: WorkflowRunId(String::new()),
                    task_run_id: "batch-id".into(),
                    workflow_id: None,
                    workflow_version_id: None,
                },
                Arc::new(CancelTransport { fail }),
            )
            .with_cancel_token(combined.clone())
            .with_batch_member_ids(
                vec!["member-1".into(), "member-2".into()],
                Some(original.clone()),
            );
            assert_eq!(ctx.cancel().await.is_err(), fail);
            // No yield: a late engine CANCEL action cannot satisfy these assertions.
            assert_eq!(original.is_cancelled(), !fail);
            assert_eq!(combined.is_cancelled(), !fail);
        }
    }
}
