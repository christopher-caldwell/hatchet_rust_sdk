use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use another_hatchet_core::RunOpts;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use tokio::sync::{RwLock, Semaphore, mpsc, oneshot, watch};
use tokio::task::{JoinHandle, JoinSet};

use crate::client::{DurableTaskHandle, TaskHandle};
use crate::clients::{SharedFeatureState, WorkerInfo};
use crate::context::{ContextIds, InMemoryContext, TriggerData};
use crate::durable::InMemoryDurableContext;
use crate::error::{Error, Result};
use crate::health::{HealthServer, HealthState};
use crate::task::{DesiredLabelConfig, LabelValue, TaskConfig, WorkerLabelComparator};
use crate::transport::{Transport, WorkerConnectionState, WorkerControl, WorkerStartOpts};
use tokio_util::sync::CancellationToken;

type BoxFutureResult = Pin<Box<dyn Future<Output = Result<Value>> + Send + 'static>>;
const DEFAULT_CANCELLATION_GRACE_PERIOD: Duration = Duration::from_secs(5);

trait ErasedTaskFn<T: Transport>: Send + Sync {
    fn validate_config(&self) -> Result<()>;
    fn concurrency_limit(&self) -> Option<Arc<KeyedConcurrencyLimit>>;
    fn matches_worker_labels(&self, worker_labels: &BTreeMap<String, String>) -> bool;
    fn call(&self, input: Value, ctx: InMemoryContext<T>) -> BoxFutureResult;
}

trait ErasedDurableTaskFn<T: Transport>: Send + Sync {
    fn validate_config(&self) -> Result<()>;
    fn concurrency_limit(&self) -> Option<Arc<KeyedConcurrencyLimit>>;
    fn matches_worker_labels(&self, worker_labels: &BTreeMap<String, String>) -> bool;
    fn call(&self, input: Value, ctx: InMemoryDurableContext<T>) -> BoxFutureResult;
}

struct TypedTaskHandler<T: Transport, I, O, F> {
    func: Arc<F>,
    config: TaskConfig,
    validator: Result<Option<crate::schema::InputValidator>>,
    concurrency: Option<Arc<KeyedConcurrencyLimit>>,
    io: std::marker::PhantomData<fn(I) -> O>,
    transport: std::marker::PhantomData<fn() -> T>,
}

struct TypedDurableTaskHandler<T: Transport, I, O, F> {
    func: Arc<F>,
    config: TaskConfig,
    validator: Result<Option<crate::schema::InputValidator>>,
    concurrency: Option<Arc<KeyedConcurrencyLimit>>,
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
    fn validate_config(&self) -> Result<()> {
        self.validator.as_ref().map_err(Clone::clone)?;
        if let Some(batch) = &self.config.batch {
            batch.validate()?;
        }
        Ok(())
    }
    fn concurrency_limit(&self) -> Option<Arc<KeyedConcurrencyLimit>> {
        self.concurrency.as_ref().map(Arc::clone)
    }

    fn matches_worker_labels(&self, worker_labels: &BTreeMap<String, String>) -> bool {
        task_matches_worker_labels(&self.config, worker_labels)
    }

    fn call(&self, input: Value, ctx: InMemoryContext<T>) -> BoxFutureResult {
        let func = Arc::clone(&self.func);
        let validator = self.validator.clone();
        let batch = self.config.batch.clone();
        Box::pin(async move {
            let validator = validator?;
            let validation_errors = if let Some(batch) = &batch {
                batch.validate()?;
                let members = input
                    .as_object()
                    .filter(|members| !members.is_empty())
                    .ok_or_else(|| {
                        Error::Worker("batch input must be a nonempty member map".into())
                    })?;
                members
                    .iter()
                    .flat_map(|(id, value)| {
                        crate::schema::errors(validator.as_ref(), value)
                            .into_iter()
                            .map(move |error| {
                                crate::schema::diagnostic(format_args!(
                                    "batch member {id}: {error}"
                                ))
                            })
                    })
                    .take(crate::schema::MAX_ERRORS)
                    .collect::<Vec<_>>()
            } else {
                crate::schema::errors(validator.as_ref(), &input)
            };
            if !validation_errors.is_empty() {
                return Err(Error::InputValidation(validation_errors));
            }
            let member_ids = input
                .as_object()
                .map(|m| m.keys().cloned().collect::<Vec<_>>())
                .unwrap_or_default();
            let decoded = serde_json::from_value::<I>(input).map_err(|_| {
                Error::InputValidation(vec![
                    "task input cannot be decoded as its declared type".into(),
                ])
            })?;
            let output = (func)(decoded, ctx).await?;
            let value = serde_json::to_value(output)
                .map_err(|error| Error::Worker(format!("task output encode failed: {error}")))?;
            if let Some(batch) = batch {
                crate::batch::outputs(&member_ids, value, batch.broadcast_output)
            } else {
                Ok(value)
            }
        })
    }
}

impl<T, I, O, F, Fut> ErasedDurableTaskFn<T> for TypedDurableTaskHandler<T, I, O, F>
where
    T: Transport + 'static,
    I: DeserializeOwned + Send + 'static,
    O: Serialize + Send + 'static,
    F: Fn(I, InMemoryDurableContext<T>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = Result<O>> + Send + 'static,
{
    fn validate_config(&self) -> Result<()> {
        self.validator.as_ref().map_err(Clone::clone)?;
        if self.config.batch.is_some() {
            return Err(Error::InputValidation(vec![
                "batch tasks cannot be durable".into(),
            ]));
        }
        Ok(())
    }
    fn concurrency_limit(&self) -> Option<Arc<KeyedConcurrencyLimit>> {
        self.concurrency.as_ref().map(Arc::clone)
    }

    fn matches_worker_labels(&self, worker_labels: &BTreeMap<String, String>) -> bool {
        task_matches_worker_labels(&self.config, worker_labels)
    }

    fn call(&self, input: Value, ctx: InMemoryDurableContext<T>) -> BoxFutureResult {
        let func = Arc::clone(&self.func);
        let validator = self.validator.clone();
        let is_batch = self.config.batch.is_some();
        Box::pin(async move {
            if is_batch {
                return Err(Error::InputValidation(vec![
                    "batch tasks cannot be durable".into(),
                ]));
            }
            let validator = validator?;
            let validation_errors = crate::schema::errors(validator.as_ref(), &input);
            if !validation_errors.is_empty() {
                return Err(Error::InputValidation(validation_errors));
            }
            let decoded = serde_json::from_value::<I>(input).map_err(|_| {
                Error::InputValidation(vec![
                    "durable input cannot be decoded as its declared type".into(),
                ])
            })?;
            let output = (func)(decoded, ctx).await?;
            serde_json::to_value(output)
                .map_err(|error| Error::Worker(format!("durable output encode failed: {error}")))
        })
    }
}

/// Live semaphores by concurrency key; an entry exists only while it is in use.
type KeySemaphores = std::sync::Mutex<HashMap<String, std::sync::Weak<Semaphore>>>;

/// Local per-key emulation of a handle's `ConcurrencyConfig`.
///
/// Each distinct concurrency key gets its own `max_runs` semaphore, so runs
/// for one key never wait behind runs for another. A key's semaphore exists
/// only while some run holds or awaits it. Only `input` and `input.<field>...`
/// expressions are evaluated locally. Any other CEL expression keeps the
/// worker-wide limit it always had: every run of the task shares one key, so
/// the worker never runs more than `max_runs` of them at once.
struct KeyedConcurrencyLimit {
    /// Input path the key is read from; `None` puts every run under one shared key.
    path: Option<Vec<String>>,
    max_runs: usize,
    semaphores: Arc<KeySemaphores>,
}

/// Holds one run's place under its concurrency key and releases it on drop.
struct KeyedConcurrencyPermit {
    key: String,
    semaphore: Arc<Semaphore>,
    acquired: bool,
    semaphores: Arc<KeySemaphores>,
}

impl KeyedConcurrencyLimit {
    fn from_config(config: &TaskConfig) -> Option<Arc<Self>> {
        let concurrency = config.concurrency.as_ref()?;
        let max_runs = concurrency.max_runs?;
        let path = parse_input_key_path(&concurrency.expression);
        Some(Arc::new(Self {
            path,
            max_runs: max_runs as usize,
            semaphores: Arc::default(),
        }))
    }

    /// A missing field maps to the empty string, which no JSON value encodes to.
    /// Without an input path every run gets that same empty-string key.
    fn key(&self, input: &Value) -> String {
        let Some(path) = &self.path else {
            return String::new();
        };
        path.iter()
            .try_fold(input, |value, segment| value.get(segment.as_str()))
            .map_or_else(String::new, Value::to_string)
    }

    async fn acquire(&self, input: &Value) -> Option<KeyedConcurrencyPermit> {
        let key = self.key(input);
        let semaphore = {
            let mut semaphores = self
                .semaphores
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            match semaphores.get(&key).and_then(std::sync::Weak::upgrade) {
                Some(semaphore) => semaphore,
                None => {
                    let semaphore = Arc::new(Semaphore::new(self.max_runs));
                    semaphores.insert(key.clone(), Arc::downgrade(&semaphore));
                    semaphore
                }
            }
        };
        // Build the guard before waiting so a cancelled wait still cleans up.
        let mut permit = KeyedConcurrencyPermit {
            key,
            semaphore,
            acquired: false,
            semaphores: Arc::clone(&self.semaphores),
        };
        permit.semaphore.acquire().await.ok()?.forget();
        permit.acquired = true;
        Some(permit)
    }
}

impl Drop for KeyedConcurrencyPermit {
    fn drop(&mut self) {
        if self.acquired {
            self.semaphore.add_permits(1);
        }
        let mut semaphores = self
            .semaphores
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // New holders are only created under this lock, so a count of one
        // means no other run holds or awaits this key.
        if Arc::strong_count(&self.semaphore) == 1 {
            let is_ours = semaphores
                .get(&self.key)
                .is_some_and(|weak| std::ptr::eq(weak.as_ptr(), Arc::as_ptr(&self.semaphore)));
            if is_ours {
                semaphores.remove(&self.key);
            }
        }
    }
}

/// Accepts `input` or `input.a.b` with identifier segments only.
fn parse_input_key_path(expression: &str) -> Option<Vec<String>> {
    let mut segments = expression.trim().split('.');
    if segments.next()? != "input" {
        return None;
    }
    segments
        .map(|segment| {
            let valid = !segment.is_empty()
                && segment
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_');
            valid.then(|| segment.to_owned())
        })
        .collect()
}

async fn acquire_task_permit(
    limit: Option<Arc<KeyedConcurrencyLimit>>,
    input: &Value,
) -> Option<KeyedConcurrencyPermit> {
    match limit {
        Some(limit) => limit.acquire(input).await,
        None => None,
    }
}

/// Engine operations a non-generic [`WorkerHandle`] needs from its transport.
#[async_trait::async_trait]
trait WorkerEngine: Send + Sync {
    async fn set_paused(&self, worker_id: &str, paused: bool) -> Result<()>;
    async fn upsert_labels(
        &self,
        worker_id: &str,
        labels: &BTreeMap<String, LabelValue>,
    ) -> Result<()>;
}

struct TransportWorkerEngine<T>(Arc<T>);

#[async_trait::async_trait]
impl<T: Transport + 'static> WorkerEngine for TransportWorkerEngine<T> {
    async fn set_paused(&self, worker_id: &str, paused: bool) -> Result<()> {
        self.0.set_worker_paused(worker_id, paused).await
    }

    async fn upsert_labels(
        &self,
        worker_id: &str,
        labels: &BTreeMap<String, LabelValue>,
    ) -> Result<()> {
        self.0.upsert_worker_labels(worker_id, labels).await
    }
}

fn task_matches_worker_labels(
    config: &TaskConfig,
    worker_labels: &BTreeMap<String, String>,
) -> bool {
    config
        .desired_worker_labels
        .iter()
        .all(|(key, desired)| match worker_labels.get(key) {
            Some(actual) => label_matches(actual, desired),
            None => !desired.required,
        })
}

fn label_matches(actual: &str, desired: &DesiredLabelConfig) -> bool {
    let desired_value = desired.value.as_string();
    let numeric_actual = actual.parse::<f64>().ok();
    let numeric_desired = desired.value.as_number();

    match desired.comparator {
        WorkerLabelComparator::Equal => actual == desired_value,
        WorkerLabelComparator::NotEqual => actual != desired_value,
        WorkerLabelComparator::GreaterThan => compare_label_values(
            numeric_actual,
            numeric_desired,
            actual,
            &desired_value,
            |ordering| ordering.is_gt(),
            |a, b| a > b,
        ),
        WorkerLabelComparator::LessThan => compare_label_values(
            numeric_actual,
            numeric_desired,
            actual,
            &desired_value,
            |ordering| ordering.is_lt(),
            |a, b| a < b,
        ),
        WorkerLabelComparator::GreaterThanOrEqual => compare_label_values(
            numeric_actual,
            numeric_desired,
            actual,
            &desired_value,
            |ordering| !ordering.is_lt(),
            |a, b| a >= b,
        ),
        WorkerLabelComparator::LessThanOrEqual => compare_label_values(
            numeric_actual,
            numeric_desired,
            actual,
            &desired_value,
            |ordering| !ordering.is_gt(),
            |a, b| a <= b,
        ),
    }
}

fn compare_label_values(
    actual_numeric: Option<f64>,
    desired_numeric: Option<f64>,
    actual_text: &str,
    desired_text: &str,
    lexical: fn(std::cmp::Ordering) -> bool,
    numeric: fn(f64, f64) -> bool,
) -> bool {
    match (actual_numeric, desired_numeric) {
        (Some(actual), Some(desired)) => numeric(actual, desired),
        _ => lexical(actual_text.cmp(desired_text)),
    }
}

pub struct WorkerBuilder<T: Transport> {
    name: String,
    slots: usize,
    durable_slots: usize,
    middleware: Arc<[Arc<dyn crate::TaskMiddleware>]>,
    cancellation_grace_period: Duration,
    health_port: Option<u16>,
    labels: BTreeMap<String, String>,
    transport: Arc<T>,
    state: SharedFeatureState,
    non_durable_registry: BTreeMap<String, Arc<dyn ErasedTaskFn<T>>>,
    durable_registry: BTreeMap<String, Arc<dyn ErasedDurableTaskFn<T>>>,
}

#[derive(Debug, Clone)]
enum WorkerLifecycleState {
    Starting,
    Ready,
    Reconnecting,
    Failed(String),
    Stopped,
}

#[derive(Debug, Clone)]
pub struct WorkerDispatcher {
    tx: mpsc::Sender<WorkItem>,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct TaskContextData {
    pub parent_outputs: HashMap<String, Value>,
    pub cancel_token: Option<CancellationToken>,
    pub run_opts: RunOpts,
    pub retry_count: u32,
    pub filter_payload: Option<Value>,
    pub worker_id: Option<String>,
    pub trigger_data: TriggerData,
    pub errors: HashMap<String, String>,
    pub batch_member_ids: Vec<String>,
}

impl WorkerDispatcher {
    pub(crate) fn new(tx: mpsc::Sender<WorkItem>) -> Self {
        Self { tx }
    }

    pub(crate) fn same_channel(&self, other: &Self) -> bool {
        self.tx.same_channel(&other.tx)
    }

    pub(crate) async fn run_task_json(
        &self,
        task_name: &str,
        input: Value,
        ids: ContextIds,
    ) -> Result<Value> {
        self.run_task_json_with_cancel(task_name, input, ids, None)
            .await
    }

    pub(crate) async fn run_task_json_with_cancel(
        &self,
        task_name: &str,
        input: Value,
        ids: ContextIds,
        cancel_token: Option<CancellationToken>,
    ) -> Result<Value> {
        self.run_task_json_with_context(
            task_name,
            input,
            ids,
            TaskContextData {
                cancel_token,
                ..TaskContextData::default()
            },
        )
        .await
    }

    pub(crate) async fn run_task_json_with_context(
        &self,
        task_name: &str,
        input: Value,
        ids: ContextIds,
        context: TaskContextData,
    ) -> Result<Value> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(WorkItem::RunTask {
                task_name: task_name.to_owned(),
                input,
                ids,
                context,
                reply: reply_tx,
            })
            .await
            .map_err(|error| Error::Worker(format!("worker channel closed: {error}")))?;
        reply_rx
            .await
            .map_err(|error| Error::Worker(format!("task reply channel closed: {error}")))?
    }

    pub(crate) async fn run_durable_task_json(
        &self,
        task_name: &str,
        input: Value,
        ids: ContextIds,
        invocation_count: u32,
    ) -> Result<Value> {
        self.run_durable_task_json_with_cancel(task_name, input, ids, invocation_count, None)
            .await
    }

    pub(crate) async fn run_durable_task_json_with_cancel(
        &self,
        task_name: &str,
        input: Value,
        ids: ContextIds,
        invocation_count: u32,
        cancel_token: Option<CancellationToken>,
    ) -> Result<Value> {
        self.run_durable_task_json_with_context(
            task_name,
            input,
            ids,
            invocation_count,
            TaskContextData {
                cancel_token,
                ..TaskContextData::default()
            },
        )
        .await
    }

    pub(crate) async fn run_durable_task_json_with_context(
        &self,
        task_name: &str,
        input: Value,
        ids: ContextIds,
        invocation_count: u32,
        context: TaskContextData,
    ) -> Result<Value> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(WorkItem::RunDurableTask {
                task_name: task_name.to_owned(),
                input,
                ids,
                invocation_count,
                context,
                reply: reply_tx,
            })
            .await
            .map_err(|error| Error::Worker(format!("worker channel closed: {error}")))?;
        reply_rx
            .await
            .map_err(|error| Error::Worker(format!("durable reply channel closed: {error}")))?
    }
}

impl<T: Transport + 'static> WorkerBuilder<T> {
    pub(crate) fn new(name: String, transport: Arc<T>, state: SharedFeatureState) -> Self {
        Self {
            name,
            slots: 100,
            durable_slots: 1000,
            middleware: Arc::from([]),
            cancellation_grace_period: DEFAULT_CANCELLATION_GRACE_PERIOD,
            health_port: None,
            labels: BTreeMap::new(),
            transport,
            state,
            non_durable_registry: BTreeMap::new(),
            durable_registry: BTreeMap::new(),
        }
    }

    /// Adds an outer-to-inner wrapper around both normal and durable task handlers.
    #[must_use]
    pub fn middleware(mut self, layer: impl crate::TaskMiddleware + 'static) -> Self {
        let mut layers = self.middleware.to_vec();
        layers.push(Arc::new(layer));
        self.middleware = layers.into();
        self
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
    pub fn cancellation_grace_period(mut self, grace_period: Duration) -> Self {
        self.cancellation_grace_period = grace_period;
        self
    }

    #[must_use]
    pub fn labels(mut self, labels: BTreeMap<String, String>) -> Self {
        self.labels = labels;
        self
    }

    #[must_use]
    pub fn health_port(mut self, port: u16) -> Self {
        self.health_port = Some(port);
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
        let config = task.config().clone();
        self.non_durable_registry.insert(
            task.task_name().to_owned(),
            Arc::new(TypedTaskHandler::<T, I, O, F> {
                func: Arc::new(handler),
                validator: crate::schema::InputValidator::compile(config.input_schema.as_ref()),
                concurrency: KeyedConcurrencyLimit::from_config(&config),
                config,
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
        let config = task.config().clone();
        self.durable_registry.insert(
            task.task_name().to_owned(),
            Arc::new(TypedDurableTaskHandler::<T, I, O, F> {
                func: Arc::new(handler),
                validator: crate::schema::InputValidator::compile(config.input_schema.as_ref()),
                concurrency: KeyedConcurrencyLimit::from_config(&config),
                config,
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
            middleware: self.middleware,
            cancellation_grace_period: self.cancellation_grace_period,
            health_port: self.health_port,
            labels: self.labels,
            non_durable_registry: Arc::new(self.non_durable_registry),
            durable_registry: Arc::new(self.durable_registry),
            transport: self.transport,
            state: self.state,
        }
    }
}

pub struct Worker<T: Transport> {
    name: String,
    slots: usize,
    durable_slots: usize,
    middleware: Arc<[Arc<dyn crate::TaskMiddleware>]>,
    cancellation_grace_period: Duration,
    health_port: Option<u16>,
    labels: BTreeMap<String, String>,
    non_durable_registry: Arc<BTreeMap<String, Arc<dyn ErasedTaskFn<T>>>>,
    durable_registry: Arc<BTreeMap<String, Arc<dyn ErasedDurableTaskFn<T>>>>,
    transport: Arc<T>,
    state: SharedFeatureState,
}

impl<T: Transport + 'static> Worker<T> {
    /// Starts the worker runtime loop and returns a handle used to invoke or stop it.
    ///
    /// # Errors
    ///
    /// Returns an error if the runtime task cannot be started.
    pub fn start(self) -> Result<WorkerHandle> {
        for handler in self.non_durable_registry.values() {
            handler.validate_config()?;
        }
        for handler in self.durable_registry.values() {
            handler.validate_config()?;
        }
        let (tx, rx) = mpsc::channel(256);
        let dispatcher = WorkerDispatcher::new(tx.clone());
        let paused = Arc::new(RwLock::new(false));
        let initial_labels = self.labels.clone();
        let labels = Arc::new(RwLock::new(initial_labels.clone()));
        let control = WorkerControl::new(Arc::clone(&paused), Arc::clone(&labels));
        // The engine owns routing for engine-backed workers, so pause and label
        // changes must reach it; in-memory workers keep them process-local.
        let engine = self.transport.rest_api().map(|_| {
            Arc::new(TransportWorkerEngine(Arc::clone(&self.transport))) as Arc<dyn WorkerEngine>
        });
        let transport = Arc::clone(&self.transport);
        let state = Arc::clone(&self.state);
        let worker_name = self.name.clone();
        let (ready_tx, ready_rx) = watch::channel(WorkerLifecycleState::Starting);
        let (connection_tx, mut connection_rx) = watch::channel(WorkerConnectionState::Starting);

        let cancel_token = CancellationToken::new();

        // Build the option maps
        let mut task_dispatchers = std::collections::HashMap::new();
        for key in self.non_durable_registry.keys() {
            task_dispatchers.insert(key.clone(), dispatcher.clone());
        }

        let mut durable_task_dispatchers = std::collections::HashMap::new();
        for key in self.durable_registry.keys() {
            durable_task_dispatchers.insert(key.clone(), dispatcher.clone());
        }

        let mut slot_config = std::collections::HashMap::new();
        slot_config.insert("default".to_owned(), self.slots as i32);
        slot_config.insert("durable".to_owned(), self.durable_slots as i32);

        let opts = WorkerStartOpts {
            worker_name: self.name.clone(),
            labels: self.labels.into_iter().collect(),
            slot_config,
            task_dispatchers,
            durable_task_dispatchers,
            cancel_token: cancel_token.clone(),
            connection_tx,
            control: control.clone(),
        };

        let runtime = Arc::new(WorkerRuntime {
            name: self.name,
            paused: Arc::clone(&paused),
            labels: Arc::clone(&labels),
            control: control.clone(),
            transport: self.transport,
            non_durable_registry: self.non_durable_registry,
            durable_registry: self.durable_registry,
            non_durable_semaphore: Arc::new(Semaphore::new(self.slots)),
            durable_semaphore: Arc::new(Semaphore::new(self.durable_slots)),
            cancellation_grace_period: self.cancellation_grace_period,
            shutdown_token: cancel_token.clone(),
            middleware: self.middleware,
        });

        let health_state = self.health_port.map(|_| {
            let actions = runtime
                .non_durable_registry
                .keys()
                .chain(runtime.durable_registry.keys())
                .map(|task_name| format!("default:{task_name}"))
                .collect();
            Arc::new(RwLock::new(HealthState {
                worker_name: worker_name.clone(),
                slots: self.slots,
                durable_slots: self.durable_slots,
                actions,
                labels: initial_labels,
                healthy: false,
            }))
        });

        let health_server = self
            .health_port
            .zip(health_state.as_ref())
            .map(|(port, state)| HealthServer::start_detached(port, Arc::clone(state)));

        let mut health_server = match health_server {
            Some(Ok(server)) => Some(server),
            Some(Err(error)) => {
                return Err(Error::Worker(format!(
                    "health server start failed: {error}"
                )));
            }
            None => None,
        };

        let worker_name_for_start = worker_name.clone();
        let runtime_tx = tx.clone();
        let health_state_for_transport = health_state.clone();
        let transport_join = tokio::spawn(async move {
            {
                let mut features = state.write().await;
                features.workers.insert(
                    worker_name_for_start.clone(),
                    WorkerInfo {
                        name: worker_name_for_start.clone(),
                        paused: false,
                        labels: opts
                            .labels
                            .iter()
                            .map(|(key, value)| (key.clone(), value.clone()))
                            .collect(),
                    },
                );
            }

            let transport_loop = transport.start_worker_loop(opts);
            tokio::pin!(transport_loop);
            let result = loop {
                tokio::select! {
                    result = &mut transport_loop => break result,
                    changed = connection_rx.changed() => {
                        if changed.is_err() {
                            break Err(Error::Worker(
                                "worker connection state channel closed".to_owned(),
                            ));
                        }
                        let connection_state = connection_rx.borrow().clone();
                        match connection_state {
                            WorkerConnectionState::Starting => {
                                if let Some(health_state) = &health_state_for_transport {
                                    health_state.write().await.healthy = false;
                                }
                            }
                            WorkerConnectionState::Ready => {
                                if let Some(health_state) = &health_state_for_transport {
                                    health_state.write().await.healthy = true;
                                }
                                let _ = ready_tx.send(WorkerLifecycleState::Ready);
                            }
                            WorkerConnectionState::Reconnecting(message) => {
                                eprintln!(
                                    "worker '{worker_name_for_start}' reconnecting: {message}"
                                );
                                if let Some(health_state) = &health_state_for_transport {
                                    health_state.write().await.healthy = false;
                                }
                                let _ = ready_tx.send(WorkerLifecycleState::Reconnecting);
                            }
                        }
                    }
                }
            };

            if let Some(health_state) = &health_state_for_transport {
                health_state.write().await.healthy = false;
            }
            let _ = ready_tx.send(match &result {
                Ok(()) => WorkerLifecycleState::Stopped,
                Err(error) => WorkerLifecycleState::Failed(error.to_string()),
            });
            let _ = runtime_tx.send(WorkItem::Shutdown).await;
            result
        });

        let join = tokio::spawn(async move {
            runtime.run_loop(rx).await;
        });

        Ok(WorkerHandle {
            tx,
            join: Some(join),
            transport_join: Some(transport_join),
            paused,
            labels,
            control,
            engine,
            cancel_token,
            ready_rx: tokio::sync::Mutex::new(ready_rx),
            state: self.state,
            worker_name,
            health_server: health_server.take(),
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

pub struct WorkerHandle {
    tx: mpsc::Sender<WorkItem>,
    join: Option<JoinHandle<()>>,
    transport_join: Option<JoinHandle<Result<()>>>,
    paused: Arc<tokio::sync::RwLock<bool>>,
    labels: Arc<tokio::sync::RwLock<BTreeMap<String, String>>>,
    control: WorkerControl,
    engine: Option<Arc<dyn WorkerEngine>>,
    cancel_token: CancellationToken,
    ready_rx: tokio::sync::Mutex<watch::Receiver<WorkerLifecycleState>>,
    state: SharedFeatureState,
    worker_name: String,
    health_server: Option<HealthServer>,
}

impl WorkerHandle {
    async fn sync_worker_state(&self) {
        let paused = *self.paused.read().await;
        let labels = self.labels.read().await.clone();
        self.state.write().await.workers.insert(
            self.worker_name.clone(),
            WorkerInfo {
                name: self.worker_name.clone(),
                paused,
                labels,
            },
        );
        if let Some(health_server) = &self.health_server {
            health_server
                .set_labels(self.labels.read().await.clone())
                .await;
        }
    }

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
                context: TaskContextData::default(),
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
                context: TaskContextData::default(),
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
        let mut this = self;
        this.cancel_token.cancel();
        let _ = this.tx.send(WorkItem::Shutdown).await;
        this.state.write().await.workers.remove(&this.worker_name);
        if let Some(server) = &this.health_server {
            server.set_healthy(false).await;
        }
        if let Some(mut server) = this.health_server.take() {
            server.stop().await;
        }
        this.wait().await
    }

    /// Pauses the worker. On an engine-backed worker the engine is told first,
    /// so it stops assigning work; a worker that has not registered yet is
    /// paused as soon as it registers, and the pause is re-applied after a
    /// reconnect.
    ///
    /// # Errors
    ///
    /// Returns the engine error; local state is then left unchanged.
    pub async fn pause(&self) -> Result<()> {
        self.set_paused(true).await
    }

    /// Unpauses the worker, telling the engine first on an engine-backed worker.
    ///
    /// # Errors
    ///
    /// Returns the engine error; local state is then left unchanged.
    pub async fn unpause(&self) -> Result<()> {
        self.set_paused(false).await
    }

    async fn set_paused(&self, paused: bool) -> Result<()> {
        {
            // Held until the local change is made; see `WorkerControl`.
            let engine_state = self.control.engine.lock().await;
            if let (Some(engine), Some(worker_id)) =
                (&self.engine, engine_state.worker_id.as_deref())
            {
                engine.set_paused(worker_id, paused).await?;
            }
            *self.paused.write().await = paused;
        }
        self.sync_worker_state().await;
        Ok(())
    }

    pub async fn is_paused(&self) -> Result<bool> {
        Ok(*self.paused.read().await)
    }

    /// Adds or replaces worker labels. On an engine-backed worker the engine is
    /// told first, so routing uses the new labels; a worker that has not
    /// registered yet gets them when it registers, and they are re-applied
    /// after a reconnect.
    ///
    /// # Errors
    ///
    /// Returns the engine error; local state is then left unchanged.
    pub async fn upsert_labels(&self, labels: BTreeMap<String, String>) -> Result<()> {
        {
            // Held until the local change is made; see `WorkerControl`.
            let mut engine_state = self.control.engine.lock().await;
            if let Some(engine) = &self.engine {
                let engine_labels: BTreeMap<String, LabelValue> = labels
                    .iter()
                    .map(|(key, value)| (key.clone(), LabelValue::String(value.clone())))
                    .collect();
                if let Some(worker_id) = engine_state.worker_id.as_deref() {
                    engine.upsert_labels(worker_id, &engine_labels).await?;
                }
                // Re-sent after every (re)registration.
                engine_state.upserted_labels.extend(engine_labels);
            }
            self.labels.write().await.extend(labels);
        }
        self.sync_worker_state().await;
        Ok(())
    }

    pub async fn labels(&self) -> Result<BTreeMap<String, String>> {
        Ok(self.labels.read().await.clone())
    }

    /// Waits for the worker runtime loop to exit.
    ///
    /// # Errors
    ///
    /// Returns an error if the runtime task panics.
    pub async fn wait(mut self) -> Result<()> {
        match (self.join.take(), self.transport_join.take()) {
            (Some(mut runtime_join), Some(mut transport_join)) => {
                tokio::select! {
                    runtime_result = &mut runtime_join => {
                        self.cancel_token.cancel();
                        runtime_result.map_err(|error| {
                            Error::Worker(format!("worker runtime join failed: {error}"))
                        })?;
                        transport_join.await.map_err(|error| {
                            Error::Worker(format!("worker transport join failed: {error}"))
                        })??;
                    }
                    transport_result = &mut transport_join => {
                        self.cancel_token.cancel();
                        let _ = self.tx.send(WorkItem::Shutdown).await;
                        let transport_result = transport_result.map_err(|error| {
                            Error::Worker(format!("worker transport join failed: {error}"))
                        })?;
                        runtime_join.await.map_err(|error| {
                            Error::Worker(format!("worker runtime join failed: {error}"))
                        })?;
                        transport_result?;
                    }
                }
            }
            (Some(runtime_join), None) => {
                runtime_join.await.map_err(|error| {
                    Error::Worker(format!("worker runtime join failed: {error}"))
                })?;
            }
            (None, Some(transport_join)) => {
                transport_join.await.map_err(|error| {
                    Error::Worker(format!("worker transport join failed: {error}"))
                })??;
            }
            (None, None) => {}
        }
        Ok(())
    }

    /// Waits until the worker start sequence has reached the locally-ready state.
    ///
    /// # Errors
    ///
    /// Returns an error on timeout or if the startup task reports failure.
    pub async fn wait_until_ready(&self, timeout: Duration) -> Result<()> {
        let mut ready_rx = self.ready_rx.lock().await;
        if matches!(*ready_rx.borrow(), WorkerLifecycleState::Ready) {
            return Ok(());
        }

        tokio::time::timeout(timeout, async {
            loop {
                match ready_rx.borrow().clone() {
                    WorkerLifecycleState::Ready => return Ok(()),
                    WorkerLifecycleState::Failed(message) => {
                        return Err(Error::Worker(format!("worker failed to start: {message}")));
                    }
                    WorkerLifecycleState::Stopped => {
                        return Err(Error::Worker(
                            "worker stopped before becoming ready".to_owned(),
                        ));
                    }
                    WorkerLifecycleState::Starting | WorkerLifecycleState::Reconnecting => {}
                }

                ready_rx
                    .changed()
                    .await
                    .map_err(|_| Error::Worker("worker readiness channel closed".to_owned()))?;
            }
        })
        .await
        .map_err(|_| Error::Worker("timed out waiting for worker readiness".to_owned()))?
    }
}

pub(crate) enum WorkItem {
    RunTask {
        task_name: String,
        input: Value,
        ids: ContextIds,
        context: TaskContextData,
        reply: oneshot::Sender<Result<Value>>,
    },
    RunDurableTask {
        task_name: String,
        input: Value,
        ids: ContextIds,
        invocation_count: u32,
        context: TaskContextData,
        reply: oneshot::Sender<Result<Value>>,
    },
    Shutdown,
}

struct DurableInvocation {
    task_name: String,
    input: Value,
    ids: ContextIds,
    invocation_count: u32,
    context: TaskContextData,
    reply: oneshot::Sender<Result<Value>>,
}

struct WorkerRuntime<T: Transport> {
    name: String,
    paused: Arc<RwLock<bool>>,
    labels: Arc<RwLock<BTreeMap<String, String>>>,
    control: WorkerControl,
    transport: Arc<T>,
    non_durable_registry: Arc<BTreeMap<String, Arc<dyn ErasedTaskFn<T>>>>,
    durable_registry: Arc<BTreeMap<String, Arc<dyn ErasedDurableTaskFn<T>>>>,
    non_durable_semaphore: Arc<Semaphore>,
    durable_semaphore: Arc<Semaphore>,
    cancellation_grace_period: Duration,
    shutdown_token: CancellationToken,
    middleware: Arc<[Arc<dyn crate::TaskMiddleware>]>,
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
                    context,
                    reply,
                } => {
                    let runtime = Arc::clone(&self);
                    join_set.spawn(async move {
                        runtime
                            .execute_non_durable(task_name, input, ids, context, reply)
                            .await;
                    });
                }
                WorkItem::RunDurableTask {
                    task_name,
                    input,
                    ids,
                    invocation_count,
                    context,
                    reply,
                } => {
                    let runtime = Arc::clone(&self);
                    join_set.spawn(async move {
                        runtime
                            .execute_durable(DurableInvocation {
                                task_name,
                                input,
                                ids,
                                invocation_count,
                                context,
                                reply,
                            })
                            .await;
                    });
                }
                WorkItem::Shutdown => break,
            }

            while join_set.try_join_next().is_some() {}
        }

        let drain = async { while join_set.join_next().await.is_some() {} };
        if tokio::time::timeout(self.cancellation_grace_period, drain)
            .await
            .is_err()
        {
            join_set.abort_all();
            while join_set.join_next().await.is_some() {}
        }
    }

    fn spawn_cancellation_monitor(
        &self,
        task_name: &str,
        ids: &ContextIds,
        cancel_token: Option<CancellationToken>,
        completed: Arc<AtomicBool>,
    ) -> (Option<JoinHandle<()>>, Arc<AtomicBool>) {
        let warned = Arc::new(AtomicBool::new(false));
        let Some(cancel_token) = cancel_token else {
            return (None, warned);
        };

        let transport = Arc::clone(&self.transport);
        let workflow_run_id = ids.workflow_run_id.clone();
        let task_run_id = ids.task_run_id.clone();
        let worker_name = self.name.clone();
        let task_name = task_name.to_owned();
        let grace_period = self.cancellation_grace_period;
        let warned_flag = Arc::clone(&warned);

        let handle = tokio::spawn(async move {
            cancel_token.cancelled().await;
            tokio::time::sleep(grace_period).await;
            if completed.load(Ordering::Relaxed) {
                return;
            }

            warned_flag.store(true, Ordering::Relaxed);
            if let Err(error) = transport
                .append_run_log(
                    &workflow_run_id,
                    &task_run_id,
                    crate::context::LogLevel::Warn,
                    format!(
                        "worker '{worker_name}' task '{task_name}' (task_run_id '{task_run_id}') is still running {}ms after cancellation",
                        grace_period.as_millis()
                    ),
                )
                .await
            {
                eprintln!("failed to send log line for task run '{task_run_id}': {error}");
            }
        });

        (Some(handle), warned)
    }

    async fn execute_non_durable(
        self: Arc<Self>,
        task_name: String,
        input: Value,
        ids: ContextIds,
        context: TaskContextData,
        reply: oneshot::Sender<Result<Value>>,
    ) {
        let TaskContextData {
            parent_outputs,
            cancel_token,
            run_opts,
            retry_count,
            filter_payload,
            worker_id,
            trigger_data,
            errors,
            batch_member_ids,
        } = context;
        let batch_cancel_token = if batch_member_ids.is_empty() {
            None
        } else {
            cancel_token.clone()
        };
        let (cancel_token, cancel_watcher) =
            combine_cancellation_tokens(cancel_token, self.shutdown_token.clone());
        let workflow_run_id = ids.workflow_run_id.clone();
        let task_run_id = ids.task_run_id.clone();
        let resolve_run = ids.workflow_name == ids.task_name;
        let completed = Arc::new(AtomicBool::new(false));
        let (monitor, warned) = self.spawn_cancellation_monitor(
            &task_name,
            &ids,
            Some(cancel_token.clone()),
            Arc::clone(&completed),
        );
        let result = match self.non_durable_semaphore.clone().acquire_owned().await {
            Ok(_permit) => {
                let Some(handler) = self.non_durable_registry.get(&task_name).cloned() else {
                    if let Some(handle) = monitor {
                        handle.abort();
                    }
                    let _ = reply.send(Err(Error::Worker(format!(
                        "worker '{}' missing non-durable task '{}'",
                        self.name, task_name
                    ))));
                    return;
                };
                // The engine routes work to engine-backed workers by their
                // engine-side pause state and labels; failing its assignments
                // here would only burn retries.
                let local_gates = self.transport.rest_api().is_none();
                if local_gates && *self.paused.read().await {
                    Err(Error::Worker(format!("worker '{}' is paused", self.name)))
                } else {
                    let worker_labels = self.labels.read().await.clone();
                    if local_gates && !handler.matches_worker_labels(&worker_labels) {
                        Err(Error::Worker(format!(
                            "worker '{}' does not satisfy labels for task '{}'",
                            self.name, task_name
                        )))
                    } else {
                        let _task_permit =
                            acquire_task_permit(handler.concurrency_limit(), &input).await;
                        let invocation = crate::TaskInvocation::new(
                            task_name.clone(),
                            ids.clone(),
                            input,
                            run_opts.additional_metadata.clone(),
                            retry_count,
                            false,
                        );
                        let mut ctx = InMemoryContext::new(ids, Arc::clone(&self.transport))
                            .with_run_opts(&run_opts)
                            .with_retry_count(retry_count)
                            .with_worker_labels(worker_labels)
                            .with_worker_control(self.control.clone())
                            .with_cancel_token(cancel_token)
                            .with_parent_outputs(parent_outputs)
                            .with_trigger_data(trigger_data)
                            .with_errors(errors)
                            .with_batch_member_ids(batch_member_ids, batch_cancel_token);
                        if let Some(payload) = filter_payload {
                            ctx = ctx.with_filter_payload(payload);
                        }
                        if let Some(worker_id) = worker_id {
                            ctx = ctx.with_worker_id(worker_id);
                        }
                        crate::telemetry::task(
                            invocation,
                            crate::Next::new(Arc::clone(&self.middleware), move |invocation| {
                                handler.call(invocation.into_input(), ctx)
                            }),
                        )
                        .await
                    }
                }
            }
            Err(error) => Err(Error::from(error)),
        };

        completed.store(true, Ordering::Relaxed);
        drop(cancel_watcher);
        if let Some(handle) = monitor {
            handle.abort();
        }
        if warned.load(Ordering::Relaxed) {
            let level = if result.is_ok() {
                crate::context::LogLevel::Error
            } else {
                crate::context::LogLevel::Warn
            };
            let outcome = if result.is_ok() {
                "completed successfully after cancellation was requested"
            } else {
                "finished only after extended cancellation delay"
            };
            if let Err(error) = self
                .transport
                .append_run_log(
                    &workflow_run_id,
                    &task_run_id,
                    level,
                    format!("worker '{}' task '{}' {}", self.name, task_name, outcome),
                )
                .await
            {
                eprintln!("failed to send log line for task run '{task_run_id}': {error}");
            }
        }

        // Engine-backed runs are resolved by Hatchet after completion reporting;
        // a local handler return must not override cancellation or graph status.
        if resolve_run && self.transport.rest_api().is_none() {
            self.transport
                .resolve_result(&workflow_run_id, result.clone())
                .await;
        }
        let _ = reply.send(result);
    }

    async fn execute_durable(self: Arc<Self>, invocation: DurableInvocation) {
        let DurableInvocation {
            task_name,
            input,
            ids,
            invocation_count,
            context,
            reply,
        } = invocation;
        let TaskContextData {
            parent_outputs,
            cancel_token,
            run_opts,
            retry_count,
            filter_payload,
            worker_id,
            trigger_data,
            errors,
            batch_member_ids: _,
        } = context;
        let transport_cancel_token = cancel_token.clone();
        let (mut cancel_token, shutdown_cancel_watcher) =
            combine_cancellation_tokens(cancel_token, self.shutdown_token.clone());
        let workflow_run_id = ids.workflow_run_id.clone();
        let task_run_id = ids.task_run_id.clone();
        let resolve_run = ids.workflow_name == ids.task_name;
        let completed = Arc::new(AtomicBool::new(false));

        let mut eviction_watch = None;
        let mut evict_cancel_watcher = None;
        // Merge eviction with the transport token too, so a late handler return
        // cannot overwrite an engine-owned EVICTED outcome with FAILED.
        if let Some(listener) = self.transport.durable_listener() {
            let evict_token = CancellationToken::new();
            eviction_watch = Some(spawn_durable_eviction_watch(
                listener,
                ids.task_run_id.clone(),
                invocation_count as i32,
                transport_cancel_token,
                evict_token.clone(),
            ));
            let (combined, watcher) = combine_cancellation_tokens(Some(cancel_token), evict_token);
            cancel_token = combined;
            evict_cancel_watcher = watcher;
        }

        let (monitor, warned) = self.spawn_cancellation_monitor(
            &task_name,
            &ids,
            Some(cancel_token.clone()),
            Arc::clone(&completed),
        );
        let result = match self.durable_semaphore.clone().acquire_owned().await {
            Ok(_permit) => {
                let Some(handler) = self.durable_registry.get(&task_name).cloned() else {
                    if let Some(watch) = eviction_watch.take() {
                        watch.abort();
                    }
                    if let Some(handle) = monitor {
                        handle.abort();
                    }
                    let _ = reply.send(Err(Error::Worker(format!(
                        "worker '{}' missing durable task '{}'",
                        self.name, task_name
                    ))));
                    return;
                };
                // The engine routes work to engine-backed workers by their
                // engine-side pause state and labels; failing its assignments
                // here would only burn retries.
                let local_gates = self.transport.rest_api().is_none();
                if local_gates && *self.paused.read().await {
                    Err(Error::Worker(format!("worker '{}' is paused", self.name)))
                } else {
                    let worker_labels = self.labels.read().await.clone();
                    if local_gates && !handler.matches_worker_labels(&worker_labels) {
                        Err(Error::Worker(format!(
                            "worker '{}' does not satisfy labels for task '{}'",
                            self.name, task_name
                        )))
                    } else {
                        let _task_permit =
                            acquire_task_permit(handler.concurrency_limit(), &input).await;
                        let invocation = crate::TaskInvocation::new(
                            task_name.clone(),
                            ids.clone(),
                            input,
                            run_opts.additional_metadata.clone(),
                            retry_count,
                            true,
                        );
                        let mut ctx = InMemoryDurableContext::new(
                            ids,
                            Arc::clone(&self.transport),
                            invocation_count,
                        )
                        .with_run_opts(&run_opts)
                        .with_retry_count(retry_count)
                        .with_worker_labels(worker_labels)
                        .with_worker_control(self.control.clone())
                        .with_cancel_token(cancel_token)
                        .with_parent_outputs(parent_outputs)
                        .with_trigger_data(trigger_data)
                        .with_errors(errors);
                        if let Some(payload) = filter_payload {
                            ctx = ctx.with_filter_payload(payload);
                        }
                        if let Some(worker_id) = worker_id {
                            ctx = ctx.with_worker_id(worker_id);
                        }
                        if let Some(listener) = self.transport.durable_listener() {
                            ctx = ctx.with_durable_listener(listener);
                        }
                        crate::telemetry::task(
                            invocation,
                            crate::Next::new(Arc::clone(&self.middleware), move |invocation| {
                                handler.call(invocation.into_input(), ctx)
                            }),
                        )
                        .await
                    }
                }
            }
            Err(error) => Err(Error::from(error)),
        };

        completed.store(true, Ordering::Relaxed);
        if let Some(watch) = eviction_watch {
            watch.abort();
        }
        drop(evict_cancel_watcher);
        drop(shutdown_cancel_watcher);
        if let Some(handle) = monitor {
            handle.abort();
        }
        if warned.load(Ordering::Relaxed) {
            let level = if result.is_ok() {
                crate::context::LogLevel::Error
            } else {
                crate::context::LogLevel::Warn
            };
            let outcome = if result.is_ok() {
                "completed successfully after cancellation was requested"
            } else {
                "finished only after extended cancellation delay"
            };
            if let Err(error) = self
                .transport
                .append_run_log(
                    &workflow_run_id,
                    &task_run_id,
                    level,
                    format!(
                        "worker '{}' durable task '{}' {}",
                        self.name, task_name, outcome
                    ),
                )
                .await
            {
                eprintln!("failed to send log line for task run '{task_run_id}': {error}");
            }
        }

        if resolve_run && self.transport.rest_api().is_none() {
            self.transport
                .resolve_result(&workflow_run_id, result.clone())
                .await;
        }
        let _ = reply.send(result);
    }
}

// Subscribe before spawning so an eviction cannot arrive between setup and polling.
pub(crate) fn spawn_durable_eviction_watch(
    listener: Arc<crate::durable_listener::DurableListenerClient>,
    task_id: String,
    inv: i32,
    transport_cancel_token: Option<CancellationToken>,
    evict_token: CancellationToken,
) -> JoinHandle<()> {
    let mut evict_rx = listener.subscribe_evictions();
    tokio::spawn(async move {
        loop {
            match evict_rx.recv().await {
                Ok(notice)
                    if notice.task_external_id == task_id && notice.invocation_count == inv =>
                {
                    // The engine has already evicted this invocation. Publish
                    // cancellation before the bounded outbound queue can block,
                    // even if handler completion later aborts this watcher.
                    if let Some(token) = transport_cancel_token {
                        token.cancel();
                    }
                    evict_token.cancel();
                    if notice.requires_acknowledgment {
                        if let Err(error) = listener
                            .send_evict_invocation(&task_id, inv, Some(notice.reason))
                            .await
                        {
                            eprintln!("failed to acknowledge durable eviction: {error}");
                        }
                    }
                    break;
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }
    })
}

/// Aborts the wrapped task when dropped, so a per-execution watcher cannot
/// outlive the execution that spawned it (including early returns and aborts).
struct AbortOnDrop(JoinHandle<()>);

impl Drop for AbortOnDrop {
    fn drop(&mut self) {
        self.0.abort();
    }
}

// The returned guard owns the forwarding watcher. Transport tokens are not
// cancelled when a task finishes normally, so callers must drop the guard once
// the execution ends or the watcher stays parked until worker shutdown.
fn combine_cancellation_tokens(
    existing: Option<CancellationToken>,
    shutdown: CancellationToken,
) -> (CancellationToken, Option<AbortOnDrop>) {
    let Some(existing) = existing else {
        return (shutdown, None);
    };
    let combined = CancellationToken::new();
    let combined_for_task = combined.clone();
    let watcher = tokio::spawn(async move {
        tokio::select! {
            _ = existing.cancelled() => combined_for_task.cancel(),
            _ = shutdown.cancelled() => combined_for_task.cancel(),
        }
    });
    (combined, Some(AbortOnDrop(watcher)))
}

#[cfg(test)]
mod tests {
    use super::*;

    use serde_json::json;

    use crate::client::Client;
    use crate::transport::InMemoryTransport;
    use another_hatchet_core::WorkflowRunId;

    fn test_ids(task_name: &str) -> ContextIds {
        ContextIds {
            workflow_name: "test.workflow".to_owned(),
            task_name: task_name.to_owned(),
            workflow_run_id: WorkflowRunId("run-1".to_owned()),
            task_run_id: "task-run-1".to_owned(),
            workflow_id: None,
            workflow_version_id: None,
        }
    }

    #[tokio::test]
    async fn cancellation_monitor_logs_when_task_ignores_cancel() {
        let transport = Arc::new(InMemoryTransport::new());
        let client = Client::new(Arc::clone(&transport));
        let worker = client
            .worker("cancel-worker")
            .cancellation_grace_period(Duration::from_millis(10))
            .register_task(
                &client.task::<(), ()>("ignore-cancel"),
                |_input, _ctx| async move {
                    tokio::time::sleep(Duration::from_millis(40)).await;
                    Ok(())
                },
            )
            .build()
            .start()
            .expect("worker should start");

        let cancel_token = CancellationToken::new();
        let (reply_tx, reply_rx) = oneshot::channel();
        worker
            .tx
            .send(WorkItem::RunTask {
                task_name: "ignore-cancel".to_owned(),
                input: json!(null),
                ids: test_ids("ignore-cancel"),
                context: TaskContextData {
                    cancel_token: Some(cancel_token.clone()),
                    ..TaskContextData::default()
                },
                reply: reply_tx,
            })
            .await
            .expect("dispatch should succeed");

        tokio::time::sleep(Duration::from_millis(5)).await;
        cancel_token.cancel();
        reply_rx.await.expect("task reply should arrive").unwrap();
        tokio::time::sleep(Duration::from_millis(5)).await;

        let logs = transport
            .list_logs("task-run-1")
            .await
            .expect("logs should load");
        assert!(logs.iter().any(|entry| {
            entry.level == "Warn"
                && entry
                    .message
                    .contains("still running 10ms after cancellation")
        }));
        assert!(logs.iter().any(|entry| {
            entry.level == "Error"
                && entry
                    .message
                    .contains("completed successfully after cancellation was requested")
        }));

        worker.shutdown().await.expect("worker should shut down");
    }

    #[tokio::test]
    async fn cancellation_monitor_stays_quiet_for_cooperative_task() {
        let transport = Arc::new(InMemoryTransport::new());
        let client = Client::new(Arc::clone(&transport));
        let worker = client
            .worker("cooperative-worker")
                .cancellation_grace_period(Duration::from_millis(20))
                .register_task(&client.task::<(), ()>("cooperative"), |_input, ctx| async move {
                    tokio::select! {
                        _ = ctx.cancelled() => Err(Error::Worker("cooperatively cancelled".to_owned())),
                        _ = tokio::time::sleep(Duration::from_millis(100)) => Ok(()),
                    }
                })
                .build()
                .start()
                .expect("worker should start");

        let cancel_token = CancellationToken::new();
        let (reply_tx, reply_rx) = oneshot::channel();
        worker
            .tx
            .send(WorkItem::RunTask {
                task_name: "cooperative".to_owned(),
                input: json!(null),
                ids: test_ids("cooperative"),
                context: TaskContextData {
                    cancel_token: Some(cancel_token.clone()),
                    ..TaskContextData::default()
                },
                reply: reply_tx,
            })
            .await
            .expect("dispatch should succeed");

        tokio::time::sleep(Duration::from_millis(5)).await;
        cancel_token.cancel();
        let result = reply_rx.await.expect("task reply should arrive");
        assert!(result.is_err());
        tokio::time::sleep(Duration::from_millis(25)).await;

        assert!(
            transport
                .list_logs("task-run-1")
                .await
                .expect("logs should load")
                .is_empty()
        );

        worker.shutdown().await.expect("worker should shut down");
    }

    /// Starts a 4-slot worker whose `keyed` task allows one run per value of `expression`.
    fn start_keyed_worker<F, Fut>(expression: &str, handler: F) -> WorkerHandle
    where
        F: Fn(Value, InMemoryContext<InMemoryTransport>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<()>> + Send + 'static,
    {
        let client = Client::new(Arc::new(InMemoryTransport::new()));
        let task = client.task::<Value, ()>("keyed").with_config(
            TaskConfig::builder()
                .concurrency(crate::ConcurrencyConfig::new(expression).max_runs(1))
                .build(),
        );
        client
            .worker("keyed-concurrency-worker")
            .slots(4)
            .register_task(&task, handler)
            .build()
            .start()
            .expect("worker should start")
    }

    #[tokio::test]
    async fn handle_concurrency_limit_does_not_serialize_different_keys() {
        // Each run can finish only while the other run is inside the handler too.
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let worker = start_keyed_worker("input.tenant", move |_input, _ctx| {
            let barrier = Arc::clone(&barrier);
            async move {
                if tokio::time::timeout(Duration::from_secs(5), barrier.wait())
                    .await
                    .is_err()
                {
                    return Err(Error::Worker(
                        "runs for different keys did not run together".to_owned(),
                    ));
                }
                Ok(())
            }
        });

        let (first, second) = tokio::join!(
            worker.run_task_json("keyed", json!({ "tenant": "a" }), test_ids("keyed")),
            worker.run_task_json("keyed", json!({ "tenant": "b" }), test_ids("keyed")),
        );
        first.expect("tenant a should run alongside tenant b");
        second.expect("tenant b should run alongside tenant a");
        worker.shutdown().await.expect("worker should shut down");
    }

    /// Runs two `keyed` tasks with the given inputs while holding the first one
    /// inside the handler, and checks that the second never enters alongside it.
    async fn assert_runs_are_capped_at_one(
        expression: &str,
        first_input: Value,
        second_input: Value,
    ) {
        let active = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let peak = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(Semaphore::new(0));
        let worker = {
            let (active, peak) = (Arc::clone(&active), Arc::clone(&peak));
            let (entered, release) = (Arc::clone(&entered), Arc::clone(&release));
            start_keyed_worker(expression, move |_input, _ctx| {
                let (active, peak) = (Arc::clone(&active), Arc::clone(&peak));
                let (entered, release) = (Arc::clone(&entered), Arc::clone(&release));
                async move {
                    let now = active.fetch_add(1, Ordering::SeqCst) + 1;
                    peak.fetch_max(now, Ordering::SeqCst);
                    entered.notify_one();
                    // Held until the test releases it; the timeout only bounds a failing test.
                    let _ = tokio::time::timeout(Duration::from_secs(5), release.acquire()).await;
                    active.fetch_sub(1, Ordering::SeqCst);
                    Ok(())
                }
            })
        };

        let control = async {
            tokio::time::timeout(Duration::from_secs(5), entered.notified())
                .await
                .expect("one run should enter the handler");
            // Both runs are dispatched; let a wrongly admitted second run get in.
            for _ in 0..32 {
                tokio::task::yield_now().await;
            }
            assert_eq!(active.load(Ordering::SeqCst), 1);
            release.add_permits(1);
        };
        let (first, second, ()) = tokio::join!(
            worker.run_task_json("keyed", first_input, test_ids("keyed")),
            worker.run_task_json("keyed", second_input, test_ids("keyed")),
            control,
        );
        first.expect("first run should succeed");
        second.expect("second run should succeed");
        assert_eq!(peak.load(Ordering::SeqCst), 1);
        worker.shutdown().await.expect("worker should shut down");
    }

    #[tokio::test]
    async fn handle_concurrency_limit_still_caps_runs_sharing_a_key() {
        assert_runs_are_capped_at_one(
            "input.tenant",
            json!({ "tenant": "a" }),
            json!({ "tenant": "a" }),
        )
        .await;
    }

    #[tokio::test]
    async fn unevaluable_handle_concurrency_expression_keeps_the_worker_wide_limit() {
        // The worker cannot evaluate this expression, so different inputs
        // still share one `max_runs` limit, as before keyed limits existed.
        assert_runs_are_capped_at_one(
            "additional_metadata.tenant",
            json!({ "tenant": "a" }),
            json!({ "tenant": "b" }),
        )
        .await;
    }

    #[tokio::test]
    async fn keyed_concurrency_limit_removes_idle_keys() {
        let limit = KeyedConcurrencyLimit::from_config(
            &TaskConfig::builder()
                .concurrency(crate::ConcurrencyConfig::new("input.tenant").max_runs(1))
                .build(),
        )
        .expect("input path expression should build a local limit");
        let held = limit.acquire(&json!({ "tenant": "a" })).await.unwrap();
        let other = limit.acquire(&json!({ "tenant": "b" })).await.unwrap();
        assert_eq!(limit.semaphores.lock().unwrap().len(), 2);

        // A cancelled waiter must not leave its key behind or leak a permit.
        let waiting = tokio::time::timeout(
            Duration::from_millis(10),
            limit.acquire(&json!({ "tenant": "a" })),
        )
        .await;
        assert!(waiting.is_err(), "same key should wait while held");

        drop(held);
        drop(other);
        assert!(limit.semaphores.lock().unwrap().is_empty());
        let again = limit.acquire(&json!({ "tenant": "a" })).await.unwrap();
        drop(again);
        assert!(limit.semaphores.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn unevaluable_expression_shares_one_key_that_is_removed_when_idle() {
        let limit = KeyedConcurrencyLimit::from_config(
            &TaskConfig::builder()
                .concurrency(
                    crate::ConcurrencyConfig::new("additional_metadata.tenant").max_runs(1),
                )
                .build(),
        )
        .expect("an unevaluable expression should keep a local limit");
        let held = limit.acquire(&json!({ "tenant": "a" })).await.unwrap();
        assert_eq!(limit.semaphores.lock().unwrap().len(), 1);

        // A different input still waits on the shared key; cancelling the wait
        // must not leak a permit or an entry.
        let waiting = tokio::time::timeout(
            Duration::from_millis(10),
            limit.acquire(&json!({ "tenant": "b" })),
        )
        .await;
        assert!(waiting.is_err(), "different inputs should share one limit");
        assert_eq!(limit.semaphores.lock().unwrap().len(), 1);

        drop(held);
        assert!(limit.semaphores.lock().unwrap().is_empty());
        let again = limit.acquire(&json!({ "tenant": "b" })).await.unwrap();
        drop(again);
        assert!(limit.semaphores.lock().unwrap().is_empty());
    }

    #[test]
    fn only_input_path_expressions_are_evaluated_locally() {
        assert_eq!(
            parse_input_key_path(" input.user.id "),
            Some(vec!["user".to_owned(), "id".to_owned()])
        );
        assert_eq!(parse_input_key_path("input"), Some(Vec::new()));
        assert_eq!(parse_input_key_path("input."), None);
        assert_eq!(parse_input_key_path("additional_metadata.tenant"), None);
        assert_eq!(parse_input_key_path("input.a + input.b"), None);
        assert_eq!(parse_input_key_path("inputs.a"), None);
    }

    #[tokio::test]
    async fn shutdown_aborts_a_handler_after_the_cancellation_grace_period() {
        let transport = Arc::new(InMemoryTransport::new());
        let client = Client::new(Arc::clone(&transport));
        let started = Arc::new(tokio::sync::Notify::new());
        let started_for_handler = Arc::clone(&started);
        let worker = client
            .worker("bounded-shutdown-worker")
            .cancellation_grace_period(Duration::from_millis(10))
            .register_task(
                &client.task::<(), ()>("never-finishes"),
                move |_input, _ctx| {
                    let started = Arc::clone(&started_for_handler);
                    async move {
                        started.notify_one();
                        std::future::pending::<()>().await;
                        Ok(())
                    }
                },
            )
            .build()
            .start()
            .expect("worker should start");

        let (reply_tx, reply_rx) = oneshot::channel();
        worker
            .tx
            .send(WorkItem::RunTask {
                task_name: "never-finishes".to_owned(),
                input: json!(null),
                ids: test_ids("never-finishes"),
                context: TaskContextData::default(),
                reply: reply_tx,
            })
            .await
            .expect("dispatch should succeed");
        tokio::time::timeout(Duration::from_millis(100), started.notified())
            .await
            .expect("handler should start");

        tokio::time::timeout(Duration::from_millis(250), worker.shutdown())
            .await
            .expect("shutdown should be bounded")
            .expect("worker should shut down cleanly");
        assert!(
            reply_rx.await.is_err(),
            "aborted task should drop its reply"
        );
    }

    const WATCHER_LEAK_RUNS: usize = 50;
    // Only reached when watchers leak; a passing run stops polling as soon as the
    // runtime has reaped the aborted watchers.
    const WATCHER_SETTLE_DEADLINE: Duration = Duration::from_secs(5);

    type TokenStash = Arc<std::sync::Mutex<Vec<CancellationToken>>>;

    // GrpcTransport gives every assignment a fresh token and never cancels it when
    // the task finishes normally. Dispatch that shape without cancelling, then check
    // that no per-task watcher is still alive and still forwarding cancellation.
    async fn assert_finished_runs_leave_no_cancellation_watcher(
        worker: &WorkerHandle,
        stash: &TokenStash,
        make_item: impl Fn(CancellationToken, oneshot::Sender<Result<Value>>) -> WorkItem,
    ) {
        // Startup tasks spawned after this point only raise the count by a few,
        // which the WATCHER_LEAK_RUNS / 2 slack below absorbs.
        let metrics = tokio::runtime::Handle::current().metrics();
        let baseline = metrics.num_alive_tasks();
        let leak_threshold = baseline + WATCHER_LEAK_RUNS / 2;

        let mut transport_tokens = Vec::with_capacity(WATCHER_LEAK_RUNS);
        for _ in 0..WATCHER_LEAK_RUNS {
            let token = CancellationToken::new();
            transport_tokens.push(token.clone());
            let (reply_tx, reply_rx) = oneshot::channel();
            worker
                .tx
                .send(make_item(token, reply_tx))
                .await
                .expect("dispatch should succeed");
            reply_rx
                .await
                .expect("task reply should arrive")
                .expect("task should succeed");
        }
        // Aborted watchers and monitors leave the runtime once it next polls them.
        // Leaked watchers stay parked on tokens nobody cancels, so the count never
        // falls below the threshold and only that failure waits for the deadline.
        let deadline = tokio::time::Instant::now() + WATCHER_SETTLE_DEADLINE;
        let mut alive = metrics.num_alive_tasks();
        while alive >= leak_threshold && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
            alive = metrics.num_alive_tasks();
        }
        assert!(
            alive < leak_threshold,
            "{alive} live tasks after {WATCHER_LEAK_RUNS} finished runs (baseline \
             {baseline}); per-run cancellation watchers are still parked"
        );

        // Only a still-running watcher can carry a late transport cancel into the
        // token a finished handler saw. An aborted task is never polled again, so
        // this does not depend on timing when the guards were dropped.
        let handler_tokens = stash.lock().unwrap().clone();
        assert_eq!(handler_tokens.len(), WATCHER_LEAK_RUNS);
        assert!(
            handler_tokens.iter().all(|token| !token.is_cancelled()),
            "precondition: no handler token is cancelled at completion"
        );
        for token in &transport_tokens {
            token.cancel();
        }
        // On the old code the first watcher forwards this immediately; the timeout
        // only bounds how long a passing run waits to see nothing arrive.
        let _ =
            tokio::time::timeout(Duration::from_millis(100), handler_tokens[0].cancelled()).await;
        let propagated = handler_tokens
            .iter()
            .filter(|token| token.is_cancelled())
            .count();
        assert_eq!(
            propagated, 0,
            "a late transport cancel reached {propagated} finished runs' tokens"
        );
    }

    #[tokio::test]
    async fn finished_tasks_leave_no_cancellation_watcher_running() {
        let transport = Arc::new(InMemoryTransport::new());
        let client = Client::new(Arc::clone(&transport));
        let stash = TokenStash::default();
        let stash_for_handler = Arc::clone(&stash);
        let worker = client
            .worker("watcher-leak-worker")
            .register_task(&client.task::<(), ()>("noop"), move |_input, ctx| {
                let stash = Arc::clone(&stash_for_handler);
                async move {
                    stash.lock().unwrap().push(ctx.cancel_token());
                    Ok(())
                }
            })
            .build()
            .start()
            .expect("worker should start");

        assert_finished_runs_leave_no_cancellation_watcher(&worker, &stash, |token, reply| {
            WorkItem::RunTask {
                task_name: "noop".to_owned(),
                input: json!(null),
                ids: test_ids("noop"),
                context: TaskContextData {
                    cancel_token: Some(token),
                    ..TaskContextData::default()
                },
                reply,
            }
        })
        .await;

        worker.shutdown().await.expect("worker should shut down");
    }

    #[tokio::test]
    async fn finished_durable_tasks_leave_no_cancellation_watcher_running() {
        let transport = Arc::new(InMemoryTransport::new());
        let client = Client::new(Arc::clone(&transport));
        let stash = TokenStash::default();
        let stash_for_handler = Arc::clone(&stash);
        let worker = client
            .worker("durable-watcher-leak-worker")
            .register_durable_task(
                &client.durable_task::<(), ()>("durable-noop"),
                move |_input, ctx| {
                    let stash = Arc::clone(&stash_for_handler);
                    async move {
                        stash.lock().unwrap().push(ctx.cancel_token());
                        Ok(())
                    }
                },
            )
            .build()
            .start()
            .expect("worker should start");

        assert_finished_runs_leave_no_cancellation_watcher(&worker, &stash, |token, reply| {
            WorkItem::RunDurableTask {
                task_name: "durable-noop".to_owned(),
                input: json!(null),
                ids: test_ids("durable-noop"),
                invocation_count: 1,
                context: TaskContextData {
                    cancel_token: Some(token),
                    ..TaskContextData::default()
                },
                reply,
            }
        })
        .await;

        worker.shutdown().await.expect("worker should shut down");
    }
}
