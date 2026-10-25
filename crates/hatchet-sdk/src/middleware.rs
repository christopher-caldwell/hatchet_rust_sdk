//! Worker middleware shared by normal and durable task execution.
use crate::{ContextIds, Result};
use async_trait::async_trait;
use serde_json::Value;
use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc};

pub type TaskFuture = Pin<Box<dyn Future<Output = Result<Value>> + Send + 'static>>;

/// The execution identity is immutable; middleware may replace input or output,
/// short-circuit execution, or return an error. No payload is logged automatically.
#[derive(Debug, Clone)]
pub struct TaskInvocation {
    task_name: String,
    ids: ContextIds,
    input: Value,
    metadata: HashMap<String, String>,
    retry_count: u32,
    durable: bool,
}
impl TaskInvocation {
    pub(crate) fn new(
        task_name: String,
        ids: ContextIds,
        input: Value,
        metadata: HashMap<String, String>,
        retry_count: u32,
        durable: bool,
    ) -> Self {
        Self {
            task_name,
            ids,
            input,
            metadata,
            retry_count,
            durable,
        }
    }
    #[must_use]
    pub fn task_name(&self) -> &str {
        &self.task_name
    }
    #[must_use]
    pub fn ids(&self) -> &ContextIds {
        &self.ids
    }
    #[must_use]
    pub fn input(&self) -> &Value {
        &self.input
    }
    #[must_use]
    pub fn metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }
    #[must_use]
    pub fn retry_count(&self) -> u32 {
        self.retry_count
    }
    #[must_use]
    pub fn is_durable(&self) -> bool {
        self.durable
    }
    #[must_use]
    pub fn with_input(mut self, input: Value) -> Self {
        self.input = input;
        self
    }
    #[must_use]
    pub fn into_input(self) -> Value {
        self.input
    }
}

/// Registered in order: the first middleware is the outermost wrapper.
#[async_trait]
pub trait TaskMiddleware: Send + Sync + std::fmt::Debug {
    async fn handle(&self, invocation: TaskInvocation, next: Next) -> Result<Value>;
}

/// Consuming `run` ensures each layer can invoke the rest of the pipeline once.
pub struct Next {
    middleware: Arc<[Arc<dyn TaskMiddleware>]>,
    index: usize,
    handler: Box<dyn FnOnce(TaskInvocation) -> TaskFuture + Send>,
}
impl Next {
    pub(crate) fn new(
        middleware: Arc<[Arc<dyn TaskMiddleware>]>,
        handler: impl FnOnce(TaskInvocation) -> TaskFuture + Send + 'static,
    ) -> Self {
        Self {
            middleware,
            index: 0,
            handler: Box::new(handler),
        }
    }
    pub fn run(self, invocation: TaskInvocation) -> TaskFuture {
        Box::pin(async move {
            if let Some(layer) = self.middleware.get(self.index).cloned() {
                let next = Self {
                    index: self.index + 1,
                    ..self
                };
                layer.handle(invocation, next).await
            } else {
                (self.handler)(invocation).await
            }
        })
    }
}
