use std::fmt;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, SystemTime};

use another_hatchet_core::{RunOpts, WorkflowRunId};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{Mutex, broadcast};
use tokio::time::{sleep, timeout};

use crate::client::WorkflowHandle;
use crate::config::apply_namespace;
use crate::context::{Context, ContextIds, InMemoryContext, TriggerData};
use crate::durable_listener::DurableListenerClient;
use crate::error::{Error, Result};
use crate::proto::v1;
use crate::run_ref::WorkflowRunRef;
use crate::transport::Transport;
use crate::workflow::WorkflowStep;

mod serde_duration_millis {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::Duration;

    pub fn serialize<S: Serializer>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error> {
        duration.as_millis().serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Duration, D::Error> {
        let millis = u64::deserialize(deserializer)?;
        Ok(Duration::from_millis(millis))
    }
}

mod serde_duration_millis_option {
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::Duration;

    pub fn serialize<S: Serializer>(
        duration: &Option<Duration>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        match duration {
            Some(d) => serializer.serialize_some(&d.as_millis()),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Option<Duration>, D::Error> {
        let millis: Option<u64> = Option::deserialize(deserializer)?;
        Ok(millis.map(Duration::from_millis))
    }
}

mod serde_system_time_millis {
    use serde::{Deserialize, Deserializer, Serialize, Serializer};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    pub fn serialize<S: Serializer>(time: &SystemTime, serializer: S) -> Result<S::Ok, S::Error> {
        let millis = time
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis();
        millis.serialize(serializer)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(deserializer: D) -> Result<SystemTime, D::Error> {
        let millis = u64::deserialize(deserializer)?;
        Ok(UNIX_EPOCH + Duration::from_millis(millis))
    }
}

#[derive(Debug, Clone)]
pub struct DurableEvent {
    pub key: String,
    pub payload: Value,
}

#[derive(Debug, Clone)]
struct ObservedDurableEvent {
    key: String,
    payload: Value,
    scope: Option<String>,
    emitted_at: SystemTime,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Condition {
    SleepFor(SleepCondition),
    SleepUntil(SleepUntilCondition),
    Event(EventCondition),
    Parent(ParentCondition),
}

impl Condition {
    #[must_use]
    pub fn sleep_for(duration: Duration) -> Self {
        Self::SleepFor(SleepCondition {
            duration,
            metadata: ConditionMetadata::default(),
        })
    }

    #[must_use]
    pub fn sleep_until(at: SystemTime) -> Self {
        Self::SleepUntil(SleepUntilCondition {
            at,
            metadata: ConditionMetadata::default(),
        })
    }

    #[must_use]
    pub fn event(key: impl Into<String>) -> EventConditionBuilder {
        EventConditionBuilder {
            key: key.into(),
            expression: None,
            scope: None,
            lookback_window: None,
            readable_data_key: None,
            action: None,
            or_group_id: None,
        }
    }

    #[must_use]
    pub fn parent(parent_task_name: impl Into<String>) -> ParentConditionBuilder {
        ParentConditionBuilder {
            parent_task_name: parent_task_name.into(),
            expression: None,
            readable_data_key: None,
            action: None,
            or_group_id: None,
        }
    }

    #[must_use]
    pub fn readable_data_key(mut self, key: impl Into<String>) -> Self {
        self.metadata_mut().readable_data_key = Some(key.into());
        self
    }

    #[must_use]
    pub fn action(mut self, action: ConditionAction) -> Self {
        self.metadata_mut().action = Some(action);
        self
    }

    #[must_use]
    pub fn or_group_id(mut self, or_group_id: impl Into<String>) -> Self {
        self.metadata_mut().or_group_id = Some(or_group_id.into());
        self
    }

    #[must_use]
    pub fn metadata(&self) -> &ConditionMetadata {
        match self {
            Self::SleepFor(condition) => &condition.metadata,
            Self::SleepUntil(condition) => &condition.metadata,
            Self::Event(condition) => &condition.metadata,
            Self::Parent(condition) => &condition.metadata,
        }
    }

    fn metadata_mut(&mut self) -> &mut ConditionMetadata {
        match self {
            Self::SleepFor(condition) => &mut condition.metadata,
            Self::SleepUntil(condition) => &mut condition.metadata,
            Self::Event(condition) => &mut condition.metadata,
            Self::Parent(condition) => &mut condition.metadata,
        }
    }

    fn with_default_action(mut self, action: ConditionAction) -> Self {
        self.metadata_mut().action.get_or_insert(action);
        self
    }

    fn with_or_group_id_if_missing(mut self, or_group_id: &str) -> Self {
        self.metadata_mut()
            .or_group_id
            .get_or_insert_with(|| or_group_id.to_owned());
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ConditionAction {
    Create,
    Queue,
    Cancel,
    Skip,
}

impl fmt::Display for ConditionAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Create => write!(f, "Create"),
            Self::Queue => write!(f, "Queue"),
            Self::Cancel => write!(f, "Cancel"),
            Self::Skip => write!(f, "Skip"),
        }
    }
}

impl fmt::Display for Condition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SleepFor(sleep) => write!(f, "SleepFor({}ms)", sleep.duration.as_millis()),
            Self::SleepUntil(_) => write!(f, "SleepUntil(...)"),
            Self::Event(event) => write!(f, "Event({})", event.event_key),
            Self::Parent(parent) => write!(f, "Parent({})", parent.parent_task_name),
        }
    }
}

impl fmt::Display for Conditions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Single(condition) => write!(f, "{condition}"),
            Self::Or(conditions) => {
                write!(f, "Or(")?;
                for (i, c) in conditions.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{c}")?;
                }
                write!(f, ")")
            }
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConditionMetadata {
    pub readable_data_key: Option<String>,
    pub action: Option<ConditionAction>,
    pub or_group_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SleepCondition {
    #[serde(with = "serde_duration_millis")]
    pub duration: Duration,
    pub metadata: ConditionMetadata,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SleepUntilCondition {
    #[serde(with = "serde_system_time_millis")]
    pub at: SystemTime,
    pub metadata: ConditionMetadata,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventCondition {
    pub event_key: String,
    pub expression: Option<String>,
    pub scope: Option<String>,
    #[serde(default, with = "serde_duration_millis_option")]
    pub lookback_window: Option<Duration>,
    pub metadata: ConditionMetadata,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ParentCondition {
    pub parent_task_name: String,
    pub expression: Option<String>,
    pub metadata: ConditionMetadata,
}

#[derive(Debug, Clone)]
pub struct ParentConditionBuilder {
    parent_task_name: String,
    expression: Option<String>,
    readable_data_key: Option<String>,
    action: Option<ConditionAction>,
    or_group_id: Option<String>,
}

impl ParentConditionBuilder {
    #[must_use]
    pub fn expression(mut self, expression: impl Into<String>) -> Self {
        self.expression = Some(expression.into());
        self
    }

    #[must_use]
    pub fn readable_data_key(mut self, key: impl Into<String>) -> Self {
        self.readable_data_key = Some(key.into());
        self
    }

    #[must_use]
    pub fn action(mut self, action: ConditionAction) -> Self {
        self.action = Some(action);
        self
    }

    #[must_use]
    pub fn or_group_id(mut self, or_group_id: impl Into<String>) -> Self {
        self.or_group_id = Some(or_group_id.into());
        self
    }

    #[must_use]
    pub fn build(self) -> Condition {
        Condition::Parent(ParentCondition {
            parent_task_name: self.parent_task_name.clone(),
            expression: self.expression,
            metadata: ConditionMetadata {
                readable_data_key: self
                    .readable_data_key
                    .or_else(|| Some(format!("parent-{}", self.parent_task_name))),
                action: self.action,
                or_group_id: self.or_group_id,
            },
        })
    }
}

#[derive(Debug, Clone)]
pub struct EventConditionBuilder {
    key: String,
    expression: Option<String>,
    scope: Option<String>,
    lookback_window: Option<Duration>,
    readable_data_key: Option<String>,
    action: Option<ConditionAction>,
    or_group_id: Option<String>,
}

impl EventConditionBuilder {
    #[must_use]
    pub fn expression(mut self, expression: impl Into<String>) -> Self {
        self.expression = Some(expression.into());
        self
    }

    #[must_use]
    pub fn scope(mut self, scope: impl Into<String>) -> Self {
        self.scope = Some(scope.into());
        self
    }

    /// Includes matching events published before the wait was established.
    ///
    /// Hatchet requires a matching [`Self::scope`] when lookback is enabled.
    /// Engine-backed workflow registration and runtime waits reject an
    /// unscoped lookback instead of silently waiting only for future events.
    #[must_use]
    pub fn lookback(mut self, lookback_window: Duration) -> Self {
        self.lookback_window = Some(lookback_window);
        self
    }

    #[must_use]
    pub fn readable_data_key(mut self, key: impl Into<String>) -> Self {
        self.readable_data_key = Some(key.into());
        self
    }

    #[must_use]
    pub fn action(mut self, action: ConditionAction) -> Self {
        self.action = Some(action);
        self
    }

    #[must_use]
    pub fn or_group_id(mut self, or_group_id: impl Into<String>) -> Self {
        self.or_group_id = Some(or_group_id.into());
        self
    }

    #[must_use]
    pub fn build(self) -> Condition {
        Condition::Event(EventCondition {
            event_key: self.key,
            expression: self.expression,
            scope: self.scope,
            lookback_window: self.lookback_window,
            metadata: ConditionMetadata {
                readable_data_key: self.readable_data_key,
                action: self.action,
                or_group_id: self.or_group_id,
            },
        })
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Conditions {
    Single(Condition),
    Or(Vec<Condition>),
}

pub(crate) fn condition_validation_errors(conditions: &Conditions) -> Vec<String> {
    let conditions = match conditions {
        Conditions::Single(condition) => std::slice::from_ref(condition),
        Conditions::Or(conditions) => conditions.as_slice(),
    };

    let mut errors = Vec::new();
    if conditions.is_empty() {
        errors.push("durable wait requires at least one condition".to_owned());
    }
    errors.extend(
        conditions
        .iter()
        .filter_map(|condition| match condition {
            Condition::Event(event)
                if event.lookback_window.is_some() && event.scope.is_none() =>
            {
                Some(format!(
                    "event condition '{}' uses lookback without a scope; call .scope(...) when using .lookback(...)",
                    event.event_key
                ))
            }
            _ => None,
        })
    );
    errors
}

impl From<Condition> for Conditions {
    fn from(value: Condition) -> Self {
        Self::Single(value)
    }
}

#[must_use]
pub fn or(conditions: impl IntoIterator<Item = Condition>) -> Conditions {
    let or_group_id = next_or_group_id();
    Conditions::Or(
        conditions
            .into_iter()
            .map(|condition| condition.with_or_group_id_if_missing(&or_group_id))
            .collect(),
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct BaseMatchCondition {
    pub readable_data_key: String,
    pub action: ConditionAction,
    pub or_group_id: String,
    pub expression: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SleepMatchCondition {
    pub base: BaseMatchCondition,
    pub duration: Duration,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct SleepUntilMatchCondition {
    pub base: BaseMatchCondition,
    pub at: SystemTime,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct UserEventMatchCondition {
    pub base: BaseMatchCondition,
    pub event_key: String,
    pub scope: Option<String>,
    pub lookback_window: Option<Duration>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ParentOverrideMatchCondition {
    pub base: BaseMatchCondition,
    pub parent_readable_id: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct RenderedConditions {
    pub sleep_conditions: Vec<SleepMatchCondition>,
    pub sleep_until_conditions: Vec<SleepUntilMatchCondition>,
    pub user_event_conditions: Vec<UserEventMatchCondition>,
    pub parent_override_conditions: Vec<ParentOverrideMatchCondition>,
}

impl RenderedConditions {
    /// Returns `true` if all condition vectors are empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.sleep_conditions.is_empty()
            && self.sleep_until_conditions.is_empty()
            && self.user_event_conditions.is_empty()
            && self.parent_override_conditions.is_empty()
    }

    /// Merges another `RenderedConditions` into this one.
    pub fn extend(&mut self, other: RenderedConditions) {
        self.sleep_conditions.extend(other.sleep_conditions);
        self.sleep_until_conditions
            .extend(other.sleep_until_conditions);
        self.user_event_conditions
            .extend(other.user_event_conditions);
        self.parent_override_conditions
            .extend(other.parent_override_conditions);
    }
}

#[must_use]
pub fn render_conditions(
    action: ConditionAction,
    conditions: impl IntoIterator<Item = Conditions>,
) -> Vec<Condition> {
    conditions
        .into_iter()
        .flat_map(|conditions| match conditions {
            Conditions::Single(condition) => vec![condition.with_default_action(action)],
            Conditions::Or(conditions) => conditions
                .into_iter()
                .map(|condition| condition.with_default_action(action))
                .collect(),
        })
        .collect()
}

#[must_use]
pub fn conditions_to_rendered(conditions: &[Condition]) -> RenderedConditions {
    let mut rendered = RenderedConditions::default();
    for condition in conditions {
        match condition {
            Condition::SleepFor(sleep) => rendered.sleep_conditions.push(SleepMatchCondition {
                base: base_match_condition(condition),
                duration: sleep.duration,
            }),
            Condition::SleepUntil(sleep) => {
                rendered
                    .sleep_until_conditions
                    .push(SleepUntilMatchCondition {
                        base: base_match_condition(condition),
                        at: sleep.at,
                    });
            }
            Condition::Event(event) => {
                rendered
                    .user_event_conditions
                    .push(UserEventMatchCondition {
                        base: base_match_condition(condition),
                        event_key: event.event_key.clone(),
                        scope: event.scope.clone(),
                        lookback_window: event.lookback_window,
                    })
            }
            Condition::Parent(parent) => {
                rendered
                    .parent_override_conditions
                    .push(ParentOverrideMatchCondition {
                        base: base_match_condition(condition),
                        parent_readable_id: parent.parent_task_name.clone(),
                    })
            }
        }
    }
    rendered
}

/// Converts step-level condition declarations into a single `RenderedConditions`.
///
/// This is the Rust equivalent of the TS `taskConditionsToPb(task, namespace)` function
/// called during workflow registration. It merges `wait_for` (Queue), `cancel_if` (Cancel),
/// and `skip_if` (Skip) declarations and optionally applies namespace to event keys.
#[must_use]
pub fn task_conditions_to_pb(step: &WorkflowStep, namespace: Option<&str>) -> RenderedConditions {
    let wait_for = render_conditions(ConditionAction::Queue, step.wait_for.iter().cloned());
    let cancel_if = render_conditions(ConditionAction::Cancel, step.cancel_if.iter().cloned());
    let skip_if = render_conditions(ConditionAction::Skip, step.skip_if.iter().cloned());
    let mut all = Vec::new();
    all.extend(wait_for);
    all.extend(cancel_if);
    all.extend(skip_if);
    let mut rendered = conditions_to_rendered(&all);
    if let Some(ns) = namespace {
        apply_namespace_to_conditions(&mut rendered, ns);
    }
    rendered
}

/// Applies a namespace prefix to all event keys in rendered conditions.
fn apply_namespace_to_conditions(rendered: &mut RenderedConditions, namespace: &str) {
    for event in &mut rendered.user_event_conditions {
        event.event_key = apply_namespace(&event.event_key, Some(namespace));
    }
}

#[derive(Debug, Clone)]
pub struct ConditionOutput {
    pub matched_condition_index: usize,
    pub payload: Option<Value>,
    pub matched_readable_data_key: Option<String>,
}

#[derive(Debug, Clone)]
pub struct SleepResult {
    pub woke_at: SystemTime,
}

#[async_trait]
pub trait DurableContext: Context {
    fn invocation_count(&self) -> u32;
    fn supports_eviction(&self) -> bool;

    async fn now(&self) -> Result<SystemTime>;

    async fn sleep_for(&self, duration: Duration) -> Result<()>;

    async fn sleep_until(&self, at: SystemTime) -> Result<()>;

    async fn wait_for_event(
        &self,
        event_key: &str,
        timeout_after: Duration,
    ) -> Result<Option<Value>>;

    async fn wait_for(&self, conditions: Conditions) -> Result<ConditionOutput>;

    async fn memo(&self, key: &str, value: Value) -> Result<Value>;

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
    event_tx: broadcast::Sender<ObservedDurableEvent>,
    event_log: StdMutex<Vec<ObservedDurableEvent>>,
    memo_store: Mutex<std::collections::HashMap<String, Value>>,
    memoized_now: Mutex<Option<SystemTime>>,
    durable_listener: Option<Arc<DurableListenerClient>>,
}

impl<T: Transport> InMemoryDurableContext<T> {
    #[must_use]
    pub fn new(ids: ContextIds, transport: Arc<T>, invocation_count: u32) -> Self {
        let (event_tx, _) = broadcast::channel(128);
        Self {
            base: InMemoryContext::new(ids, transport),
            invocation_count,
            event_tx,
            event_log: StdMutex::new(Vec::new()),
            memo_store: Mutex::new(std::collections::HashMap::new()),
            memoized_now: Mutex::new(None),
            durable_listener: None,
        }
    }

    #[must_use]
    pub fn with_durable_listener(mut self, listener: Arc<DurableListenerClient>) -> Self {
        self.durable_listener = Some(listener);
        self
    }

    #[must_use]
    pub fn with_worker_labels(
        mut self,
        labels: std::collections::BTreeMap<String, String>,
    ) -> Self {
        self.base = self.base.with_worker_labels(labels);
        self
    }

    #[must_use]
    pub fn with_run_opts(mut self, opts: &RunOpts) -> Self {
        self.base = self.base.with_run_opts(opts);
        self
    }

    #[must_use]
    pub fn with_retry_count(mut self, retry_count: u32) -> Self {
        self.base = self.base.with_retry_count(retry_count);
        self
    }

    #[must_use]
    pub fn with_filter_payload(mut self, filter_payload: Value) -> Self {
        self.base = self.base.with_filter_payload(filter_payload);
        self
    }

    #[must_use]
    pub fn with_worker_id(mut self, worker_id: impl Into<String>) -> Self {
        self.base = self.base.with_worker_id(worker_id);
        self
    }

    #[must_use]
    pub fn with_trigger_data(mut self, trigger_data: TriggerData) -> Self {
        self.base = self.base.with_trigger_data(trigger_data);
        self
    }

    #[must_use]
    pub fn with_errors(mut self, errors: std::collections::HashMap<String, String>) -> Self {
        self.base = self.base.with_errors(errors);
        self
    }

    #[must_use]
    pub fn with_parent_output(
        mut self,
        parent_task_name: impl Into<String>,
        parent_output: Value,
    ) -> Self {
        self.base = self
            .base
            .with_parent_output(parent_task_name, parent_output);
        self
    }

    #[must_use]
    pub fn with_parent_outputs(
        mut self,
        parent_outputs: std::collections::HashMap<String, Value>,
    ) -> Self {
        self.base = self.base.with_parent_outputs(parent_outputs);
        self
    }

    /// Installs the engine/worker cancellation token in the durable context.
    #[must_use]
    pub fn with_cancel_token(mut self, cancel_token: tokio_util::sync::CancellationToken) -> Self {
        self.base = self.base.with_cancel_token(cancel_token);
        self
    }

    /// Emits a test event into this in-memory durable context.
    ///
    /// # Errors
    ///
    /// Records an event for future lookback checks and broadcasts it to active waiters.
    pub fn emit_event(&self, key: impl Into<String>, payload: Value) -> Result<()> {
        self.emit_observed_event(key.into(), None, payload)
    }

    /// Emits a test event with a scope into this in-memory durable context.
    pub fn emit_scoped_event(
        &self,
        key: impl Into<String>,
        scope: impl Into<String>,
        payload: Value,
    ) -> Result<()> {
        self.emit_observed_event(key.into(), Some(scope.into()), payload)
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
        let workflow_name = workflow.workflow_name_owned();
        let run_id = DurableContext::spawn_child_json(
            self,
            &workflow_name,
            serde_json::to_value(input).map_err(|error| {
                Error::InputSerialization(format!("workflow {workflow_name}: {error}"))
            })?,
            opts,
        )
        .await?;
        Ok(WorkflowRunRef::new(run_id, workflow.run_control()))
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
        let run_ids =
            DurableContext::spawn_children_json(self, &workflow_name, values, opts).await?;
        Ok(run_ids
            .into_iter()
            .map(|run_id| WorkflowRunRef::new(run_id, workflow.run_control()))
            .collect())
    }

    fn emit_observed_event(
        &self,
        key: String,
        scope: Option<String>,
        payload: Value,
    ) -> Result<()> {
        let observed = ObservedDurableEvent {
            key,
            payload,
            scope,
            emitted_at: SystemTime::now(),
        };
        self.event_log
            .lock()
            .expect("durable event log lock poisoned")
            .push(observed.clone());
        let _ = self.event_tx.send(observed);
        Ok(())
    }

    async fn wait_for_condition(&self, condition: &Condition) -> Result<Option<Value>> {
        match condition {
            Condition::SleepFor(sleep) => {
                self.sleep_for(sleep.duration).await?;
                Ok(None)
            }
            Condition::SleepUntil(sleep) => {
                self.sleep_until(sleep.at).await?;
                Ok(None)
            }
            Condition::Event(event) => Ok(Some(self.wait_for_event_condition(event).await?)),
            Condition::Parent(_) => Err(Error::Durable(
                "parent conditions require engine-backed resolution".to_owned(),
            )),
        }
    }

    async fn wait_for_event_condition(&self, event: &EventCondition) -> Result<Value> {
        let mut receiver = self.event_tx.subscribe();

        if let Some(payload) = self.find_recorded_event(event, event.lookback_window) {
            return Ok(payload);
        }

        loop {
            match receiver.recv().await {
                Ok(observed) => {
                    if let Some(payload) = resolve_event_payload(event, &observed) {
                        return Ok(payload);
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    return Err(Error::Durable("event channel closed".to_owned()));
                }
            }
        }
    }

    fn find_recorded_event(
        &self,
        event: &EventCondition,
        lookback_window: Option<Duration>,
    ) -> Option<Value> {
        let cutoff = lookback_window.and_then(|window| SystemTime::now().checked_sub(window));
        let log = self
            .event_log
            .lock()
            .expect("durable event log lock poisoned");
        log.iter()
            .rev()
            .filter(|observed| {
                cutoff.is_none_or(|min_time| observed.emitted_at.duration_since(min_time).is_ok())
            })
            .find_map(|observed| resolve_event_payload(event, observed))
    }

    async fn wait_for_any(&self, conditions: Vec<Condition>) -> Result<ConditionOutput> {
        if conditions.is_empty() {
            return Ok(ConditionOutput {
                matched_condition_index: 0,
                payload: None,
                matched_readable_data_key: None,
            });
        }

        let started_at = SystemTime::now();
        let pending = conditions
            .iter()
            .enumerate()
            .map(|(index, condition)| PendingCondition {
                index,
                condition,
                sleep_deadline: sleep_deadline(condition, started_at),
            })
            .collect::<Vec<_>>();

        let mut receiver = self.event_tx.subscribe();

        if let Some(output) = self.resolve_ready_pending(&pending, started_at) {
            return Ok(output);
        }

        loop {
            if let Some((index, _)) = first_expired_sleep(&pending, SystemTime::now()) {
                return Ok(condition_output(index, pending[index].condition, None));
            }

            let next_sleep = pending
                .iter()
                .filter_map(|pending| {
                    pending
                        .sleep_deadline
                        .map(|deadline| (pending.index, deadline))
                })
                .filter_map(|(index, deadline)| {
                    deadline
                        .duration_since(SystemTime::now())
                        .ok()
                        .map(|duration| (index, duration))
                })
                .min_by_key(|(_, duration)| *duration);

            match next_sleep {
                Some((_index, duration)) => match timeout(duration, receiver.recv()).await {
                    Ok(Ok(observed)) => {
                        if let Some(output) =
                            pending.iter().find_map(|pending| match pending.condition {
                                Condition::Event(event) => resolve_event_payload(event, &observed)
                                    .map(|payload| {
                                        condition_output(
                                            pending.index,
                                            pending.condition,
                                            Some(payload),
                                        )
                                    }),
                                Condition::SleepFor(_)
                                | Condition::SleepUntil(_)
                                | Condition::Parent(_) => None,
                            })
                        {
                            return Ok(output);
                        }
                    }
                    Ok(Err(tokio::sync::broadcast::error::RecvError::Lagged(_))) => {}
                    Ok(Err(tokio::sync::broadcast::error::RecvError::Closed)) => {
                        return Err(Error::Durable("event channel closed".to_owned()));
                    }
                    Err(_) => {
                        if let Some((index, _)) = first_expired_sleep(&pending, SystemTime::now()) {
                            return Ok(condition_output(index, pending[index].condition, None));
                        }
                    }
                },
                None => match receiver.recv().await {
                    Ok(observed) => {
                        if let Some(output) =
                            pending.iter().find_map(|pending| match pending.condition {
                                Condition::Event(event) => resolve_event_payload(event, &observed)
                                    .map(|payload| {
                                        condition_output(
                                            pending.index,
                                            pending.condition,
                                            Some(payload),
                                        )
                                    }),
                                Condition::SleepFor(_)
                                | Condition::SleepUntil(_)
                                | Condition::Parent(_) => None,
                            })
                        {
                            return Ok(output);
                        }
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {}
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                        return Err(Error::Durable("event channel closed".to_owned()));
                    }
                },
            }
        }
    }

    fn resolve_ready_pending(
        &self,
        pending: &[PendingCondition<'_>],
        started_at: SystemTime,
    ) -> Option<ConditionOutput> {
        pending.iter().find_map(|pending| match pending.condition {
            Condition::SleepFor(_) | Condition::SleepUntil(_) => pending
                .sleep_deadline
                .filter(|deadline| is_due(*deadline, started_at))
                .map(|_| condition_output(pending.index, pending.condition, None)),
            Condition::Event(event) => self
                .find_recorded_event(event, event.lookback_window)
                .map(|payload| condition_output(pending.index, pending.condition, Some(payload))),
            Condition::Parent(_) => None,
        })
    }

    async fn wait_for_engine(
        &self,
        listener: &DurableListenerClient,
        conditions: Conditions,
    ) -> Result<ConditionOutput> {
        let (raw_conditions, is_or) = match &conditions {
            Conditions::Single(c) => (vec![c.clone()], false),
            Conditions::Or(cs) => (cs.clone(), true),
        };

        let or_group_id = if is_or {
            next_or_group_id()
        } else {
            String::new()
        };

        let mut sleep_conds = Vec::new();
        let mut event_conds = Vec::new();
        let mut has_parent = false;

        for condition in raw_conditions.iter() {
            match condition {
                Condition::SleepFor(sleep) => {
                    let mut base = base_match_condition(condition);
                    if is_or {
                        base.or_group_id = or_group_id.clone();
                    }
                    sleep_conds.push(v1::SleepMatchCondition {
                        base: Some(condition_base_to_proto(&base)),
                        sleep_for: duration_to_engine_string(sleep.duration),
                    });
                }
                Condition::SleepUntil(sleep) => {
                    let mut base = base_match_condition(condition);
                    if is_or {
                        base.or_group_id = or_group_id.clone();
                    }
                    sleep_conds.push(v1::SleepMatchCondition {
                        base: Some(condition_base_to_proto(&base)),
                        sleep_for: system_time_to_iso_string(&sleep.at),
                    });
                }
                Condition::Event(event) => {
                    let mut base = base_match_condition(condition);
                    if is_or {
                        base.or_group_id = or_group_id.clone();
                    }
                    let consider_events_since = event.lookback_window.map(|window| {
                        let since = SystemTime::now()
                            .checked_sub(window)
                            .unwrap_or(SystemTime::UNIX_EPOCH);
                        let dur = since
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap_or_default();
                        prost_types::Timestamp {
                            seconds: dur.as_secs() as i64,
                            nanos: dur.subsec_nanos() as i32,
                        }
                    });
                    event_conds.push(v1::UserEventMatchCondition {
                        base: Some(condition_base_to_proto(&base)),
                        user_event_key: apply_namespace(
                            &event.event_key,
                            self.base.transport_namespace(),
                        ),
                        event_scope: event.scope.clone(),
                        consider_events_since,
                    });
                }
                Condition::Parent(_) => {
                    has_parent = true;
                }
            }
        }

        // Parent conditions are handled at scheduling time by the engine (via task registration
        // conditions). By the time user code calls wait_for(parent(...)), the parent has already
        // completed. Immediately return the first parent condition as satisfied.
        if has_parent && sleep_conds.is_empty() && event_conds.is_empty() {
            let (idx, condition) = raw_conditions
                .iter()
                .enumerate()
                .find(|(_, c)| matches!(c, Condition::Parent(_)))
                .unwrap_or((0, &raw_conditions[0]));
            return Ok(condition_output(idx, condition, None));
        }

        let proto_conditions = v1::DurableEventListenerConditions {
            sleep_conditions: sleep_conds,
            user_event_conditions: event_conds,
        };

        let task_id = self.base.task_run_id();
        let inv = self.invocation_count as i32;
        let ack = listener
            .send_wait_for(task_id, inv, proto_conditions)
            .await?;
        let result = listener
            .wait_for_callback(task_id, inv, ack.branch_id, ack.node_id)
            .await?;

        condition_output_from_engine(&raw_conditions, &result.payload)
    }
}

#[derive(Debug, Clone, Copy)]
struct PendingCondition<'a> {
    index: usize,
    condition: &'a Condition,
    sleep_deadline: Option<SystemTime>,
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

    fn retry_count(&self) -> u32 {
        self.base.retry_count()
    }

    fn child_index(&self) -> Option<u64> {
        self.base.child_index()
    }

    fn child_key(&self) -> Option<&str> {
        self.base.child_key()
    }

    fn parent_workflow_run_id(&self) -> Option<&str> {
        self.base.parent_workflow_run_id()
    }

    fn additional_metadata(&self) -> &std::collections::HashMap<String, String> {
        self.base.additional_metadata()
    }

    fn priority(&self) -> Option<another_hatchet_core::Priority> {
        self.base.priority()
    }

    fn triggered_by_event(&self) -> bool {
        self.base.triggered_by_event()
    }

    fn filter_payload(&self) -> Option<&Value> {
        self.base.filter_payload()
    }

    fn worker_id(&self) -> Option<&str> {
        self.base.worker_id()
    }

    fn worker_labels(&self) -> std::collections::BTreeMap<String, crate::task::LabelValue> {
        self.base.worker_labels()
    }

    fn workflow_id(&self) -> Option<&str> {
        self.base.workflow_id()
    }

    fn workflow_version_id(&self) -> Option<&str> {
        self.base.workflow_version_id()
    }

    fn triggers(&self) -> &crate::context::TriggerData {
        self.base.triggers()
    }

    fn errors(&self) -> &std::collections::HashMap<String, String> {
        self.base.errors()
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

    async fn parent_output_json(&self, parent_task_name: &str) -> Result<Value> {
        self.base.parent_output_json(parent_task_name).await
    }

    async fn log(
        &self,
        level: crate::context::LogLevel,
        message: impl Into<String> + Send,
    ) -> Result<()> {
        self.base.log(level, message).await
    }

    async fn refresh_timeout(&self) -> Result<()> {
        self.base.refresh_timeout().await
    }

    async fn release_slot(&self) -> Result<()> {
        self.base.release_slot().await
    }

    async fn put_stream(&self, payload: Value) -> Result<()> {
        self.base.put_stream(payload).await
    }

    async fn upsert_worker_labels(
        &self,
        labels: std::collections::BTreeMap<String, crate::task::LabelValue>,
    ) -> Result<()> {
        self.base.upsert_worker_labels(labels).await
    }

    async fn cancel(&self) -> Result<()> {
        self.base.cancel().await
    }
}

#[async_trait]
impl<T: Transport> DurableContext for InMemoryDurableContext<T> {
    fn invocation_count(&self) -> u32 {
        self.invocation_count
    }

    fn supports_eviction(&self) -> bool {
        self.durable_listener.is_some()
    }

    async fn now(&self) -> Result<SystemTime> {
        if let Some(existing) = *self.memoized_now.lock().await {
            return Ok(existing);
        }

        let millis = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .map_err(|error| Error::Durable(format!("system clock before Unix epoch: {error}")))?
            .as_millis();
        let memoized = self
            .memo(
                "__another_hatchet_sdk_now",
                serde_json::json!({ "unix_millis": millis.to_string() }),
            )
            .await?;
        let millis = memoized
            .get("unix_millis")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::Durable("memoized clock payload is invalid".to_owned()))?
            .parse::<u64>()
            .map_err(|error| Error::Durable(format!("memoized clock decode: {error}")))?;
        let current = SystemTime::UNIX_EPOCH
            .checked_add(Duration::from_millis(millis))
            .ok_or_else(|| Error::Durable("memoized clock is out of range".to_owned()))?;
        *self.memoized_now.lock().await = Some(current);
        Ok(current)
    }

    async fn sleep_for(&self, duration: Duration) -> Result<()> {
        if let Some(listener) = &self.durable_listener {
            let task_id = self.base.task_run_id();
            let inv = self.invocation_count as i32;
            let conditions = condition_to_durable_listener_proto(
                &Condition::SleepFor(SleepCondition {
                    duration,
                    metadata: ConditionMetadata::default(),
                }),
                self.base.transport_namespace(),
            );
            let ack = listener.send_wait_for(task_id, inv, conditions).await?;
            listener
                .wait_for_callback(task_id, inv, ack.branch_id, ack.node_id)
                .await?;
            return Ok(());
        }
        sleep(duration).await;
        Ok(())
    }

    async fn sleep_until(&self, at: SystemTime) -> Result<()> {
        if let Some(listener) = &self.durable_listener {
            let task_id = self.base.task_run_id();
            let inv = self.invocation_count as i32;
            let conditions = condition_to_durable_listener_proto(
                &Condition::SleepUntil(SleepUntilCondition {
                    at,
                    metadata: ConditionMetadata::default(),
                }),
                self.base.transport_namespace(),
            );
            let ack = listener.send_wait_for(task_id, inv, conditions).await?;
            listener
                .wait_for_callback(task_id, inv, ack.branch_id, ack.node_id)
                .await?;
            return Ok(());
        }
        if let Ok(duration) = at.duration_since(SystemTime::now()) {
            sleep(duration).await;
        }
        Ok(())
    }

    async fn wait_for_event(
        &self,
        event_key: &str,
        timeout_after: Duration,
    ) -> Result<Option<Value>> {
        if let Some(listener) = &self.durable_listener {
            let task_id = self.base.task_run_id();
            let inv = self.invocation_count as i32;
            let conditions = condition_to_durable_listener_proto(
                &Condition::Event(EventCondition {
                    event_key: event_key.to_owned(),
                    expression: None,
                    scope: None,
                    lookback_window: None,
                    metadata: ConditionMetadata::default(),
                }),
                self.base.transport_namespace(),
            );
            let ack = listener.send_wait_for(task_id, inv, conditions).await?;
            match tokio::time::timeout(
                timeout_after,
                listener.wait_for_callback(task_id, inv, ack.branch_id, ack.node_id),
            )
            .await
            {
                Ok(Ok(result)) => {
                    let output = condition_output_from_engine(
                        &[Condition::Event(EventCondition {
                            event_key: event_key.to_owned(),
                            expression: None,
                            scope: None,
                            lookback_window: None,
                            metadata: ConditionMetadata::default(),
                        })],
                        &result.payload,
                    )?;
                    return Ok(output.payload);
                }
                Ok(Err(error)) => {
                    return Err(error);
                }
                Err(_) => return Ok(None),
            }
        }

        let mut receiver = self.event_tx.subscribe();
        let expected_key = event_key.to_owned();

        if let Some(payload) = self
            .event_log
            .lock()
            .expect("durable event log lock poisoned")
            .iter()
            .rev()
            .find(|event| event.key == expected_key)
            .map(|event| event.payload.clone())
        {
            return Ok(Some(payload));
        }

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

    async fn wait_for(&self, conditions: Conditions) -> Result<ConditionOutput> {
        let validation_errors = condition_validation_errors(&conditions);
        if !validation_errors.is_empty() {
            return Err(Error::InputValidation(validation_errors));
        }
        if let Some(listener) = &self.durable_listener {
            return self.wait_for_engine(listener, conditions).await;
        }
        match conditions {
            Conditions::Single(condition) => Ok(condition_output(
                0,
                &condition,
                self.wait_for_condition(&condition).await?,
            )),
            Conditions::Or(conditions) => self.wait_for_any(conditions).await,
        }
    }

    async fn memo(&self, key: &str, value: Value) -> Result<Value> {
        if let Some(listener) = &self.durable_listener {
            let task_id = self.base.task_run_id();
            let inv = self.invocation_count as i32;
            let key_bytes = key.as_bytes().to_vec();
            let payload_bytes = serde_json::to_vec(&value)
                .map_err(|e| Error::Durable(format!("memo serialize: {e}")))?;
            let ack = listener
                .send_memo(task_id, inv, key_bytes.clone(), Some(payload_bytes.clone()))
                .await?;
            if ack.already_existed {
                if let Some(cached) = ack.cached_payload {
                    return serde_json::from_slice::<Value>(&cached)
                        .map_err(|e| Error::Durable(format!("memo cached decode: {e}")));
                }
                return Err(Error::Durable(
                    "memo entry exists but its cached payload is missing".to_owned(),
                ));
            }
            let entry_ref = v1::DurableEventLogEntryRef {
                durable_task_external_id: task_id.to_owned(),
                invocation_count: inv,
                branch_id: ack.branch_id,
                node_id: ack.node_id,
            };
            listener
                .send_complete_memo(entry_ref, payload_bytes, key_bytes)
                .await?;
            return Ok(value);
        }
        let mut memo_store = self.memo_store.lock().await;
        Ok(memo_store.entry(key.to_owned()).or_insert(value).clone())
    }

    async fn spawn_child_json(
        &self,
        workflow_name: &str,
        input: Value,
        opts: RunOpts,
    ) -> Result<WorkflowRunId> {
        if let Some(listener) = &self.durable_listener {
            let opts = self.base.child_run_opts(opts);
            let ack = listener
                .send_trigger_runs(
                    self.base.task_run_id(),
                    self.invocation_count as i32,
                    vec![durable_child_trigger(workflow_name, input, &opts)?],
                )
                .await?;
            let mut entries = ack.run_entries;
            if entries.len() != 1 {
                return Err(Error::Durable(format!(
                    "durable child trigger returned {} run entries; expected 1",
                    entries.len()
                )));
            }
            let entry = entries.remove(0);
            let completion = listener
                .wait_for_callback(
                    self.base.task_run_id(),
                    self.invocation_count as i32,
                    entry.branch_id,
                    entry.node_id,
                )
                .await?;
            let output = durable_callback_payload(&completion.payload)?;
            let run_id = WorkflowRunId(entry.workflow_run_external_id);
            self.base.cache_run_result(&run_id, Ok(output)).await;
            return Ok(run_id);
        }
        self.base.run_child_json(workflow_name, input, opts).await
    }

    async fn spawn_children_json(
        &self,
        workflow_name: &str,
        inputs: Vec<Value>,
        opts: RunOpts,
    ) -> Result<Vec<WorkflowRunId>> {
        if let Some(listener) = &self.durable_listener {
            if inputs.is_empty() {
                return Ok(Vec::new());
            }
            let opts = self.base.bulk_child_run_opts(opts, inputs.len());
            let first_child_index = opts.child_index;
            let triggers = inputs
                .into_iter()
                .enumerate()
                .map(|(index, input)| {
                    let mut child_opts = opts.clone();
                    child_opts.child_index =
                        first_child_index.map(|first| first.saturating_add(index as u64));
                    durable_child_trigger(workflow_name, input, &child_opts)
                })
                .collect::<Result<Vec<_>>>()?;
            let ack = listener
                .send_trigger_runs(
                    self.base.task_run_id(),
                    self.invocation_count as i32,
                    triggers,
                )
                .await?;
            let mut run_ids = Vec::with_capacity(ack.run_entries.len());
            for entry in ack.run_entries {
                let completion = listener
                    .wait_for_callback(
                        self.base.task_run_id(),
                        self.invocation_count as i32,
                        entry.branch_id,
                        entry.node_id,
                    )
                    .await?;
                let output = durable_callback_payload(&completion.payload)?;
                let run_id = WorkflowRunId(entry.workflow_run_external_id);
                self.base.cache_run_result(&run_id, Ok(output)).await;
                run_ids.push(run_id);
            }
            return Ok(run_ids);
        }
        self.base
            .bulk_run_children_json(workflow_name, inputs, opts)
            .await
    }
}

fn durable_child_trigger(
    workflow_name: &str,
    input: Value,
    opts: &RunOpts,
) -> Result<v1::TriggerWorkflowRequest> {
    let additional_metadata = if opts.additional_metadata.is_empty() {
        None
    } else {
        Some(
            serde_json::to_string(&opts.additional_metadata).map_err(|error| {
                Error::InputSerialization(format!("child workflow metadata: {error}"))
            })?,
        )
    };
    let desired_worker_labels = opts
        .desired_worker_labels
        .iter()
        .map(|(key, value)| {
            (
                key.clone(),
                crate::grpc_transport::run_opt_label_to_proto(value),
            )
        })
        .collect();

    Ok(v1::TriggerWorkflowRequest {
        name: workflow_name.to_owned(),
        input: serde_json::to_string(&input).map_err(|error| {
            Error::InputSerialization(format!("child workflow {workflow_name}: {error}"))
        })?,
        parent_id: opts.parent_workflow_run_id.clone(),
        parent_task_run_external_id: opts.parent_task_run_id.clone(),
        child_index: opts
            .child_index
            .map(|index| index.min(i32::MAX as u64) as i32),
        child_key: opts.child_key.clone(),
        additional_metadata,
        desired_worker_id: opts.sticky.clone(),
        priority: opts.priority.map(crate::grpc_transport::priority_to_i32),
        desired_worker_labels,
    })
}

fn durable_callback_payload(payload: &[u8]) -> Result<Value> {
    if payload.is_empty() {
        return Ok(Value::Null);
    }
    serde_json::from_slice(payload)
        .map_err(|error| Error::Durable(format!("child output decode: {error}")))
}

fn resolve_event_payload(event: &EventCondition, observed: &ObservedDurableEvent) -> Option<Value> {
    if observed.key != event.event_key {
        return None;
    }

    if event
        .scope
        .as_deref()
        .is_some_and(|scope| observed.scope.as_deref() != Some(scope))
    {
        return None;
    }

    if let Some(expression) = &event.expression {
        return evaluate_simple_expression(expression, &observed.payload);
    }

    Some(observed.payload.clone())
}

fn evaluate_simple_expression(expression: &str, payload: &Value) -> Option<Value> {
    let expression = expression.trim();
    let (left, right, negated) = if let Some((left, right)) = expression.split_once("==") {
        (left.trim(), right.trim(), false)
    } else if let Some((left, right)) = expression.split_once("!=") {
        (left.trim(), right.trim(), true)
    } else {
        return None;
    };

    let path = left
        .strip_prefix("payload.")
        .or_else(|| left.strip_prefix("event."))
        .or_else(|| left.strip_prefix("data."))
        .unwrap_or(left);
    let actual = lookup_path(payload, path)?;
    let expected = parse_literal(right)?;
    let matches = actual == &expected;
    if matches ^ negated {
        Some(payload.clone())
    } else {
        None
    }
}

fn lookup_path<'a>(payload: &'a Value, path: &str) -> Option<&'a Value> {
    path.split('.')
        .try_fold(payload, |value, segment| value.get(segment))
}

fn parse_literal(value: &str) -> Option<Value> {
    let value = value.trim();
    if let Some(stripped) = value.strip_prefix('\'').and_then(|s| s.strip_suffix('\'')) {
        return Some(Value::String(stripped.to_owned()));
    }
    if let Some(stripped) = value.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
        return Some(Value::String(stripped.to_owned()));
    }
    if value.eq_ignore_ascii_case("true") {
        return Some(Value::Bool(true));
    }
    if value.eq_ignore_ascii_case("false") {
        return Some(Value::Bool(false));
    }
    if value.eq_ignore_ascii_case("null") {
        return Some(Value::Null);
    }
    if let Ok(number) = value.parse::<i64>() {
        return Some(Value::Number(number.into()));
    }
    if let Ok(number) = value.parse::<f64>() {
        return serde_json::Number::from_f64(number).map(Value::Number);
    }
    None
}

fn base_match_condition(condition: &Condition) -> BaseMatchCondition {
    let metadata = condition.metadata();
    BaseMatchCondition {
        readable_data_key: proto_readable_data_key(condition),
        action: metadata.action.unwrap_or(ConditionAction::Create),
        or_group_id: metadata
            .or_group_id
            .clone()
            .unwrap_or_else(next_or_group_id),
        expression: match condition {
            Condition::Event(event) => event.expression.clone().unwrap_or_default(),
            Condition::Parent(parent) => parent.expression.clone().unwrap_or_default(),
            Condition::SleepFor(_) | Condition::SleepUntil(_) => String::new(),
        },
    }
}

fn proto_readable_data_key(condition: &Condition) -> String {
    match condition {
        Condition::SleepFor(sleep) => sleep
            .metadata
            .readable_data_key
            .clone()
            .unwrap_or_else(|| format!("sleep-{}", format_duration_key(sleep.duration))),
        Condition::SleepUntil(sleep) => sleep
            .metadata
            .readable_data_key
            .clone()
            .unwrap_or_else(|| "sleep-until".to_owned()),
        Condition::Event(event) => event
            .metadata
            .readable_data_key
            .clone()
            .unwrap_or_else(|| event.event_key.clone()),
        Condition::Parent(parent) => parent
            .metadata
            .readable_data_key
            .clone()
            .unwrap_or_else(|| format!("parent-{}", parent.parent_task_name)),
    }
}

fn format_duration_key(duration: Duration) -> String {
    if duration.subsec_nanos() == 0 {
        if duration.as_secs() > 0 {
            return format!("{}s", duration.as_secs());
        }
        return "0s".to_owned();
    }

    let millis = duration.as_millis();
    if millis > 0 {
        return format!("{millis}ms");
    }

    format!("{}ns", duration.as_nanos())
}

fn next_or_group_id() -> String {
    uuid::Uuid::new_v4().to_string()
}

fn sleep_deadline(condition: &Condition, started_at: SystemTime) -> Option<SystemTime> {
    match condition {
        Condition::SleepFor(sleep) => started_at.checked_add(sleep.duration),
        Condition::SleepUntil(sleep) => Some(sleep.at),
        Condition::Event(_) | Condition::Parent(_) => None,
    }
}

fn first_expired_sleep(
    pending: &[PendingCondition<'_>],
    now: SystemTime,
) -> Option<(usize, SystemTime)> {
    pending
        .iter()
        .filter_map(|pending| {
            pending
                .sleep_deadline
                .map(|deadline| (pending.index, deadline))
        })
        .filter(|(_, deadline)| is_due(*deadline, now))
        .min_by_key(|(index, _)| *index)
}

fn is_due(deadline: SystemTime, now: SystemTime) -> bool {
    deadline
        .duration_since(now)
        .map_or(true, |duration| duration.is_zero())
}

fn condition_output(
    index: usize,
    condition: &Condition,
    payload: Option<Value>,
) -> ConditionOutput {
    ConditionOutput {
        matched_condition_index: index,
        payload,
        matched_readable_data_key: condition_readable_data_key(condition),
    }
}

fn condition_output_from_engine(
    conditions: &[Condition],
    payload: &[u8],
) -> Result<ConditionOutput> {
    let first_condition = conditions.first().ok_or_else(|| {
        Error::InputValidation(vec![
            "durable wait requires at least one condition".to_owned(),
        ])
    })?;

    if payload.is_empty() {
        return Ok(condition_output(0, first_condition, None));
    }

    let decoded = serde_json::from_slice::<Value>(payload)
        .map_err(|error| Error::Durable(format!("wait callback payload decode: {error}")))?;

    // Hatchet wraps match data as
    // {"CREATE":{"readable-key":[<event payload>]}}. Resolve the key back to
    // its condition so OR waits report the actual branch and engine-backed
    // event waits match the bare-payload behavior of the in-memory runtime.
    for (index, condition) in conditions.iter().enumerate() {
        let action = condition_action_key(
            condition
                .metadata()
                .action
                .unwrap_or(ConditionAction::Create),
        );
        let readable_key = proto_readable_data_key(condition);
        let Some(first_match) = decoded
            .get(action)
            .and_then(Value::as_object)
            .and_then(|matches| matches.get(&readable_key))
            .and_then(Value::as_array)
            .and_then(|matches| matches.first())
        else {
            continue;
        };

        let event_payload = matches!(condition, Condition::Event(_)).then(|| first_match.clone());
        return Ok(condition_output(index, condition, event_payload));
    }

    // Older engines may return a bare payload for a single event condition.
    // Preserve that shape while keeping sleep callbacks payload-free.
    let event_payload = matches!(first_condition, Condition::Event(_)).then_some(decoded);
    Ok(condition_output(0, first_condition, event_payload))
}

fn condition_action_key(action: ConditionAction) -> &'static str {
    match action {
        ConditionAction::Create => "CREATE",
        ConditionAction::Queue => "QUEUE",
        ConditionAction::Cancel => "CANCEL",
        ConditionAction::Skip => "SKIP",
    }
}

fn condition_readable_data_key(condition: &Condition) -> Option<String> {
    match condition {
        Condition::Event(event) => Some(
            event
                .metadata
                .readable_data_key
                .clone()
                .unwrap_or_else(|| event.event_key.clone()),
        ),
        Condition::Parent(parent) => Some(
            parent
                .metadata
                .readable_data_key
                .clone()
                .unwrap_or_else(|| format!("parent-{}", parent.parent_task_name)),
        ),
        Condition::SleepFor(sleep) => sleep.metadata.readable_data_key.clone(),
        Condition::SleepUntil(sleep) => sleep.metadata.readable_data_key.clone(),
    }
}

fn condition_to_durable_listener_proto(
    condition: &Condition,
    namespace: Option<&str>,
) -> v1::DurableEventListenerConditions {
    let base = base_match_condition(condition);
    let proto_base = condition_base_to_proto(&base);
    match condition {
        Condition::SleepFor(sleep) => v1::DurableEventListenerConditions {
            sleep_conditions: vec![v1::SleepMatchCondition {
                base: Some(proto_base),
                sleep_for: duration_to_engine_string(sleep.duration),
            }],
            user_event_conditions: vec![],
        },
        Condition::SleepUntil(sleep) => v1::DurableEventListenerConditions {
            sleep_conditions: vec![v1::SleepMatchCondition {
                base: Some(proto_base),
                sleep_for: system_time_to_iso_string(&sleep.at),
            }],
            user_event_conditions: vec![],
        },
        Condition::Event(event) => v1::DurableEventListenerConditions {
            sleep_conditions: vec![],
            user_event_conditions: vec![v1::UserEventMatchCondition {
                base: Some(proto_base),
                user_event_key: apply_namespace(&event.event_key, namespace),
                event_scope: event.scope.clone(),
                consider_events_since: event.lookback_window.map(|window| {
                    let since = SystemTime::now()
                        .checked_sub(window)
                        .unwrap_or(SystemTime::UNIX_EPOCH);
                    let dur = since
                        .duration_since(SystemTime::UNIX_EPOCH)
                        .unwrap_or_default();
                    prost_types::Timestamp {
                        seconds: dur.as_secs() as i64,
                        nanos: dur.subsec_nanos() as i32,
                    }
                }),
            }],
        },
        Condition::Parent(_) => v1::DurableEventListenerConditions {
            sleep_conditions: vec![],
            user_event_conditions: vec![],
        },
    }
}

fn condition_base_to_proto(base: &BaseMatchCondition) -> v1::BaseMatchCondition {
    v1::BaseMatchCondition {
        readable_data_key: base.readable_data_key.clone(),
        action: match base.action {
            ConditionAction::Create => v1::Action::Create as i32,
            ConditionAction::Queue => v1::Action::Queue as i32,
            ConditionAction::Cancel => v1::Action::Cancel as i32,
            ConditionAction::Skip => v1::Action::Skip as i32,
        },
        or_group_id: base.or_group_id.clone(),
        expression: base.expression.clone(),
    }
}

fn duration_to_engine_string(d: Duration) -> String {
    let total_secs = d.as_secs();
    if total_secs == 0 {
        return "0s".to_owned();
    }
    let hours = total_secs / 3600;
    let mins = (total_secs % 3600) / 60;
    let secs = total_secs % 60;
    let mut result = String::new();
    if hours > 0 {
        result.push_str(&format!("{hours}h"));
    }
    if mins > 0 {
        result.push_str(&format!("{mins}m"));
    }
    if secs > 0 || result.is_empty() {
        result.push_str(&format!("{secs}s"));
    }
    result
}

fn system_time_to_iso_string(time: &SystemTime) -> String {
    let dur = time
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = dur.as_secs();
    let nanos = dur.subsec_nanos();
    let days_since_epoch = secs / 86400;
    let time_of_day = secs % 86400;
    let hours = time_of_day / 3600;
    let minutes = (time_of_day % 3600) / 60;
    let seconds = time_of_day % 60;
    let (year, month, day) = days_to_ymd(days_since_epoch);
    format!(
        "{year:04}-{month:02}-{day:02}T{hours:02}:{minutes:02}:{seconds:02}.{millis:03}Z",
        millis = nanos / 1_000_000,
    )
}

fn days_to_ymd(days: u64) -> (u64, u64, u64) {
    let mut remaining = days;
    let mut year = 1970u64;
    loop {
        let days_in_year = if is_leap_year(year) { 366 } else { 365 };
        if remaining < days_in_year {
            break;
        }
        remaining -= days_in_year;
        year += 1;
    }
    let leap = is_leap_year(year);
    let month_days: [u64; 12] = [
        31,
        if leap { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    let mut month = 0usize;
    for &md in &month_days {
        if remaining < md {
            break;
        }
        remaining -= md;
        month += 1;
    }
    (year, (month + 1) as u64, remaining + 1)
}

fn is_leap_year(year: u64) -> bool {
    (year % 4 == 0 && year % 100 != 0) || year % 400 == 0
}

#[async_trait]
pub trait DurableContextExt: DurableContext {
    async fn spawn_child<I, O, T>(
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
        let workflow_name = workflow.workflow_name_owned();
        let run_id = self
            .spawn_child_json(
                &workflow_name,
                serde_json::to_value(input).map_err(|error| {
                    Error::InputSerialization(format!("workflow {workflow_name}: {error}"))
                })?,
                opts,
            )
            .await?;
        WorkflowRunRef::new(run_id, workflow.run_control())
            .output()
            .await
    }

    async fn spawn_children<I, O, T>(
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
            .spawn_children_json(&workflow_name, values, opts)
            .await?;
        let mut outputs = Vec::with_capacity(run_ids.len());
        for run_id in run_ids {
            outputs.push(
                WorkflowRunRef::new(run_id, workflow.run_control())
                    .output()
                    .await?,
            );
        }
        Ok(outputs)
    }
}

impl<T: DurableContext + ?Sized> DurableContextExt for T {}

#[cfg(test)]
mod tests {
    use another_hatchet_core::{DesiredWorkerLabel, Priority};

    use super::*;

    #[test]
    fn generated_condition_group_ids_are_unique_uuids() {
        let first = next_or_group_id();
        let second = next_or_group_id();

        uuid::Uuid::parse_str(&first).expect("Hatchet requires condition group UUIDs");
        uuid::Uuid::parse_str(&second).expect("Hatchet requires condition group UUIDs");
        assert_ne!(first, second);
    }

    #[test]
    fn durable_event_listener_conditions_apply_the_transport_namespace() {
        let condition = Condition::event("approval.received").build();
        let proto = condition_to_durable_listener_proto(&condition, Some("Acme"));

        assert_eq!(
            proto.user_event_conditions[0].user_event_key,
            "acme_approval.received"
        );
    }

    #[test]
    fn durable_event_lookback_requires_scope() {
        let invalid = Condition::event("approval.received")
            .lookback(Duration::from_secs(30))
            .build()
            .into();
        assert_eq!(condition_validation_errors(&invalid).len(), 1);

        let valid = Condition::event("approval.received")
            .scope("tenant-a")
            .lookback(Duration::from_secs(30))
            .build()
            .into();
        assert!(condition_validation_errors(&valid).is_empty());
        assert_eq!(condition_validation_errors(&or([])).len(), 1);
    }

    #[test]
    fn durable_event_callback_unwraps_match_data_and_identifies_or_branch() {
        let conditions = vec![
            Condition::sleep_for(Duration::from_secs(30)),
            Condition::event("approval.received")
                .readable_data_key("approval-hit")
                .build(),
        ];
        let output = condition_output_from_engine(
            &conditions,
            br#"{"CREATE":{"approval-hit":[{"message":"approved"}]}}"#,
        )
        .expect("callback payload should decode");

        assert_eq!(output.matched_condition_index, 1);
        assert_eq!(
            output.matched_readable_data_key.as_deref(),
            Some("approval-hit")
        );
        assert_eq!(
            output.payload,
            Some(serde_json::json!({"message": "approved"}))
        );
    }

    #[test]
    fn durable_event_callback_accepts_legacy_bare_payload() {
        let conditions = vec![Condition::event("approval.received").build()];
        let output = condition_output_from_engine(&conditions, br#"{"message":"approved"}"#)
            .expect("callback payload should decode");

        assert_eq!(output.matched_condition_index, 0);
        assert_eq!(
            output.payload,
            Some(serde_json::json!({"message": "approved"}))
        );
    }

    #[test]
    fn durable_child_trigger_preserves_lineage_and_run_options() {
        let mut opts = RunOpts {
            parent_workflow_run_id: Some("parent-run".to_owned()),
            parent_task_run_id: Some("parent-task".to_owned()),
            child_index: Some(7),
            child_key: Some("invoice-42".to_owned()),
            priority: Some(Priority::High),
            ..RunOpts::default()
        };
        opts.additional_metadata
            .insert("source".to_owned(), "durable".to_owned());
        opts.desired_worker_labels.insert(
            "region".to_owned(),
            DesiredWorkerLabel::new("us-central").into(),
        );

        let trigger = durable_child_trigger("billing/charge", serde_json::json!({"id": 42}), &opts)
            .expect("trigger should serialize");

        assert_eq!(trigger.name, "billing/charge");
        assert_eq!(trigger.parent_id.as_deref(), Some("parent-run"));
        assert_eq!(
            trigger.parent_task_run_external_id.as_deref(),
            Some("parent-task")
        );
        assert_eq!(trigger.child_index, Some(7));
        assert_eq!(trigger.child_key.as_deref(), Some("invoice-42"));
        assert_eq!(trigger.priority, Some(3));
        assert_eq!(
            trigger
                .desired_worker_labels
                .get("region")
                .and_then(|label| label.str_value.as_deref()),
            Some("us-central")
        );
    }
}
