use std::collections::BTreeMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::sync::{Semaphore, mpsc, oneshot};
use tokio::task::{JoinHandle, JoinSet};

use crate::client::{DurableTaskHandle, TaskHandle};
use crate::context::{ContextIds, InMemoryContext};
use crate::durable::InMemoryDurableContext;
use crate::error::{Error, Result};
use crate::transport::Transport;

type BoxFutureResult = Pin<Box<dyn Future<Output = Result<Value>> + Send + 'static>>;

trait ErasedTaskFn<T: Transport>: Send + Sync {
    fn call(&self, input: Value, ctx: InMemoryContext<T>) -> BoxFutureResult;
}

trait ErasedDurableTaskFn<T: Transport>: Send + Sync {
    fn call(&self, input: Value, ctx: InMemoryDurableContext<T>) -> BoxFutureResult;
}

struct TypedTaskHandler<T: Transport, I, O, F> {
    func: Arc<F>,
    io: std::marker::PhantomData<fn(I) -> O>,
    transport: std::marker::PhantomData<fn() -> T>,
}

impl<T, I, O, F, Fut> ErasedTaskFn<T> for TypedTaskHandler<T, I, O, F>
where
    T: Transport + 'static,
    I: DeserializeOwned + Send + 'static,
    O: Serialize + Send + 'static,
    F: Fn(I, InMemoryContext<T>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<O>> + Send + 'static,
{
    fn call(&self, input: Value, ctx: InMemoryContext<T>) -> BoxFutureResult {
        let func = Arc::clone(&self.func);
        Box::pin(async move {
            let decoded = serde_json::from_value::<I>(input)
                .map_err(|error| Error::Worker(format!("task input decode failed: {error}")))?;
            let output = (func)(decoded, ctx).await?;
            serde_json::to_value(output)
                .map_err(|error| Error::Worker(format!("task output encode failed: {error}")))
        })
    }
}

struct TypedDurableTaskHandler<T: Transport, I, O, F> {
    func: Arc<F>,
    io: std::marker::PhantomData<fn(I) -> O>,
    transport: std::marker::PhantomData<fn() -> T>,
}

impl<T, I, O, F, Fut> ErasedDurableTaskFn<T> for TypedDurableTaskHandler<T, I, O, F>
where
    T: Transport + 'static,
    I: DeserializeOwned + Send + 'static,
    O: Serialize + Send + 'static,
    F: Fn(I, InMemoryDurableContext<T>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<O>> + Send + 'static,
{
    fn call(&self, input: Value, ctx: InMemoryDurableContext<T>) -> BoxFutureResult {
        let func = Arc::clone(&self.func);
        Box::pin(async move {
            let decoded = serde_json::from_value::<I>(input)
                .map_err(|error| Error::Worker(format!("durable input decode failed: {error}")))?;
            let output = (func)(decoded, ctx).await?;
            serde_json::to_value(output)
                .map_err(|error| Error::Worker(format!("durable output encode failed: {error}")))
        })
    }
}

pub struct WorkerBuilder<T: Transport> {
    name: String,
    slots: usize,
    durable_slots: usize,
    labels: BTreeMap<String, String>,
    transport: Arc<T>,
    non_durable_registry: BTreeMap<String, Arc<dyn ErasedTaskFn<T>>>,
    durable_registry: BTreeMap<String, Arc<dyn ErasedDurableTaskFn<T>>>,
}

impl<T: Transport + 'static> WorkerBuilder<T> {
    pub(crate) fn new(name: String, transport: Arc<T>) -> Self {
        Self {
            name,
            slots: 100,
            durable_slots: 1000,
            labels: BTreeMap::new(),
            transport,
            non_durable_registry: BTreeMap::new(),
            durable_registry: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn slots(mut self, slots: usize) -> Self {
        self.slots = slots.max(1);
        self
    }

    #[must_use]
    pub fn durable_slots(mut self, durable_slots: usize) -> Self {
        self.durable_slots = durable_slots.max(1);
        self
    }

    #[must_use]
    pub fn labels(mut self, labels: BTreeMap<String, String>) -> Self {
        self.labels = labels;
        self
    }

    #[must_use]
    pub fn register_task<I, O, F, Fut>(mut self, task: &TaskHandle<I, O>, handler: F) -> Self
    where
        I: DeserializeOwned + Send + 'static,
        O: Serialize + Send + 'static,
        F: Fn(I, InMemoryContext<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O>> + Send + 'static,
    {
        self.non_durable_registry.insert(
            task.task_name().to_owned(),
            Arc::new(TypedTaskHandler::<T, I, O, F> {
                func: Arc::new(handler),
                io: std::marker::PhantomData,
                transport: std::marker::PhantomData,
            }),
        );
        self
    }

    #[must_use]
    pub fn register_durable_task<I, O, F, Fut>(
        mut self,
        task: &DurableTaskHandle<I, O>,
        handler: F,
    ) -> Self
    where
        I: DeserializeOwned + Send + 'static,
        O: Serialize + Send + 'static,
        F: Fn(I, InMemoryDurableContext<T>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<O>> + Send + 'static,
    {
        self.durable_registry.insert(
            task.task_name().to_owned(),
            Arc::new(TypedDurableTaskHandler::<T, I, O, F> {
                func: Arc::new(handler),
                io: std::marker::PhantomData,
                transport: std::marker::PhantomData,
            }),
        );
        self
    }

    #[must_use]
    pub fn build(self) -> Worker<T> {
        Worker {
            name: self.name,
            slots: self.slots,
            durable_slots: self.durable_slots,
            labels: self.labels,
            non_durable_registry: Arc::new(self.non_durable_registry),
            durable_registry: Arc::new(self.durable_registry),
            transport: self.transport,
        }
    }
}

pub struct Worker<T: Transport> {
    name: String,
    slots: usize,
    durable_slots: usize,
    labels: BTreeMap<String, String>,
    non_durable_registry: Arc<BTreeMap<String, Arc<dyn ErasedTaskFn<T>>>>,
    durable_registry: Arc<BTreeMap<String, Arc<dyn ErasedDurableTaskFn<T>>>>,
    transport: Arc<T>,
}

impl<T: Transport + 'static> Worker<T> {
    /// Starts the worker runtime loop and returns a handle used to invoke or stop it.
    ///
    /// # Errors
    ///
    /// Returns an error if the runtime task cannot be started.
    pub fn start(self) -> Result<WorkerHandle> {
        let (tx, rx) = mpsc::channel(256);
        let runtime = Arc::new(WorkerRuntime {
            name: self.name,
            _labels: self.labels,
            transport: self.transport,
            non_durable_registry: self.non_durable_registry,
            durable_registry: self.durable_registry,
            non_durable_semaphore: Arc::new(Semaphore::new(self.slots)),
            durable_semaphore: Arc::new(Semaphore::new(self.durable_slots)),
        });

        let join = tokio::spawn(async move {
            runtime.run_loop(rx).await;
        });

        Ok(WorkerHandle {
            tx,
            join: Some(join),
        })
    }

    /// Starts the worker and blocks until shutdown is requested.
    ///
    /// # Errors
    ///
    /// Returns an error if the runtime fails while waiting.
    pub async fn start_blocking(self) -> Result<()> {
        self.start()?.wait().await
    }
}

#[derive(Debug)]
pub struct WorkerHandle {
    tx: mpsc::Sender<WorkItem>,
    join: Option<JoinHandle<()>>,
}

impl WorkerHandle {
    /// Executes one registered non-durable task and returns serialized output.
    ///
    /// # Errors
    ///
    /// Returns an error if dispatch fails, the task is unregistered, or execution fails.
    pub async fn run_task_json(
        &self,
        task_name: &str,
        input: Value,
        ids: ContextIds,
    ) -> Result<Value> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(WorkItem::RunTask {
                task_name: task_name.to_owned(),
                input,
                ids,
                reply: reply_tx,
            })
            .await
            .map_err(|error| Error::Worker(format!("worker channel closed: {error}")))?;
        reply_rx
            .await
            .map_err(|error| Error::Worker(format!("task reply channel closed: {error}")))?
    }

    /// Executes one registered durable task and returns serialized output.
    ///
    /// # Errors
    ///
    /// Returns an error if dispatch fails, the task is unregistered, or execution fails.
    pub async fn run_durable_task_json(
        &self,
        task_name: &str,
        input: Value,
        ids: ContextIds,
        invocation_count: u32,
    ) -> Result<Value> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(WorkItem::RunDurableTask {
                task_name: task_name.to_owned(),
                input,
                ids,
                invocation_count,
                reply: reply_tx,
            })
            .await
            .map_err(|error| Error::Worker(format!("worker channel closed: {error}")))?;
        reply_rx
            .await
            .map_err(|error| Error::Worker(format!("durable reply channel closed: {error}")))?
    }

    /// Executes one registered non-durable task with typed input and output.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization, dispatch, or deserialization fails.
    pub async fn run_task<I, O>(
        &self,
        task: &TaskHandle<I, O>,
        input: I,
        ids: ContextIds,
    ) -> Result<O>
    where
        I: Serialize,
        O: DeserializeOwned,
    {
        let output = self
            .run_task_json(
                task.task_name(),
                serde_json::to_value(input).map_err(|error| {
                    Error::Worker(format!(
                        "task input encode failed for {}: {error}",
                        task.task_name()
                    ))
                })?,
                ids,
            )
            .await?;
        serde_json::from_value(output).map_err(|error| {
            Error::Worker(format!(
                "task output decode failed for {}: {error}",
                task.task_name()
            ))
        })
    }

    /// Executes one registered durable task with typed input and output.
    ///
    /// # Errors
    ///
    /// Returns an error if serialization, dispatch, or deserialization fails.
    pub async fn run_durable_task<I, O>(
        &self,
        task: &DurableTaskHandle<I, O>,
        input: I,
        ids: ContextIds,
        invocation_count: u32,
    ) -> Result<O>
    where
        I: Serialize,
        O: DeserializeOwned,
    {
        let output = self
            .run_durable_task_json(
                task.task_name(),
                serde_json::to_value(input).map_err(|error| {
                    Error::Worker(format!(
                        "durable input encode failed for {}: {error}",
                        task.task_name()
                    ))
                })?,
                ids,
                invocation_count,
            )
            .await?;
        serde_json::from_value(output).map_err(|error| {
            Error::Worker(format!(
                "durable output decode failed for {}: {error}",
                task.task_name()
            ))
        })
    }

    /// Requests worker shutdown and waits for the runtime loop to finish.
    ///
    /// # Errors
    ///
    /// Returns an error if shutdown signaling or join fails.
    pub async fn shutdown(self) -> Result<()> {
        let _ = self.tx.send(WorkItem::Shutdown).await;
        self.wait().await
    }

    /// Waits for the worker runtime loop to exit.
    ///
    /// # Errors
    ///
    /// Returns an error if the runtime task panics.
    pub async fn wait(mut self) -> Result<()> {
        if let Some(join) = self.join.take() {
            join.await
                .map_err(|error| Error::Worker(format!("worker join failed: {error}")))?;
        }
        Ok(())
    }
}

enum WorkItem {
    RunTask {
        task_name: String,
        input: Value,
        ids: ContextIds,
        reply: oneshot::Sender<Result<Value>>,
    },
    RunDurableTask {
        task_name: String,
        input: Value,
        ids: ContextIds,
        invocation_count: u32,
        reply: oneshot::Sender<Result<Value>>,
    },
    Shutdown,
}

struct WorkerRuntime<T: Transport> {
    name: String,
    _labels: BTreeMap<String, String>,
    transport: Arc<T>,
    non_durable_registry: Arc<BTreeMap<String, Arc<dyn ErasedTaskFn<T>>>>,
    durable_registry: Arc<BTreeMap<String, Arc<dyn ErasedDurableTaskFn<T>>>>,
    non_durable_semaphore: Arc<Semaphore>,
    durable_semaphore: Arc<Semaphore>,
}

impl<T: Transport + 'static> WorkerRuntime<T> {
    async fn run_loop(self: Arc<Self>, mut rx: mpsc::Receiver<WorkItem>) {
        let mut join_set = JoinSet::new();

        while let Some(item) = rx.recv().await {
            match item {
                WorkItem::RunTask {
                    task_name,
                    input,
                    ids,
                    reply,
                } => {
                    let runtime = Arc::clone(&self);
                    join_set.spawn(async move {
                        runtime
                            .execute_non_durable(task_name, input, ids, reply)
                            .await;
                    });
                }
                WorkItem::RunDurableTask {
                    task_name,
                    input,
                    ids,
                    invocation_count,
                    reply,
                } => {
                    let runtime = Arc::clone(&self);
                    join_set.spawn(async move {
                        runtime
                            .execute_durable(task_name, input, ids, invocation_count, reply)
                            .await;
                    });
                }
                WorkItem::Shutdown => break,
            }

            while join_set.try_join_next().is_some() {}
        }

        while join_set.join_next().await.is_some() {}
    }

    async fn execute_non_durable(
        self: Arc<Self>,
        task_name: String,
        input: Value,
        ids: ContextIds,
        reply: oneshot::Sender<Result<Value>>,
    ) {
        let result = match self.non_durable_semaphore.clone().acquire_owned().await {
            Ok(_permit) => {
                let Some(handler) = self.non_durable_registry.get(&task_name).cloned() else {
                    let _ = reply.send(Err(Error::Worker(format!(
                        "worker '{}' missing non-durable task '{}'",
                        self.name, task_name
                    ))));
                    return;
                };
                let ctx = InMemoryContext::new(ids, Arc::clone(&self.transport));
                handler.call(input, ctx).await
            }
            Err(error) => Err(Error::from(error)),
        };

        let _ = reply.send(result);
    }

    async fn execute_durable(
        self: Arc<Self>,
        task_name: String,
        input: Value,
        ids: ContextIds,
        invocation_count: u32,
        reply: oneshot::Sender<Result<Value>>,
    ) {
        let result = match self.durable_semaphore.clone().acquire_owned().await {
            Ok(_permit) => {
                let Some(handler) = self.durable_registry.get(&task_name).cloned() else {
                    let _ = reply.send(Err(Error::Worker(format!(
                        "worker '{}' missing durable task '{}'",
                        self.name, task_name
                    ))));
                    return;
                };
                let ctx =
                    InMemoryDurableContext::new(ids, Arc::clone(&self.transport), invocation_count);
                handler.call(input, ctx).await
            }
            Err(error) => Err(Error::from(error)),
        };

        let _ = reply.send(result);
    }
}
