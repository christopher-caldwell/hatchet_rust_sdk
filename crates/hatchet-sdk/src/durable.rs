use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, SystemTime};

use async_trait::async_trait;
use hatchet_core::{RunOpts, WorkflowRunId};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{Mutex, broadcast};
use tokio::time::{sleep, timeout};

use crate::client::WorkflowHandle;
use crate::context::{Context, ContextExt, ContextIds, InMemoryContext};
use crate::error::{Error, Result};
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
        event.event_key = format!("{namespace}/{}", event.event_key);
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

    async fn now(&self) -> SystemTime;

    async fn sleep_for(&self, duration: Duration);

    async fn sleep_until(&self, at: SystemTime);

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
        }
    }

    #[must_use]
    pub fn with_worker_labels(
        mut self,
        labels: std::collections::BTreeMap<String, String>,
    ) -> Self {
        self.base = self.base.with_worker_labels(labels);
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
        inputs: impl IntoIterator<Item = I> + Send,
        opts: RunOpts,
    ) -> Result<Vec<WorkflowRunRef<O>>>
    where
        I: Serialize,
    {
        self.base.bulk_run_children(workflow, inputs, opts).await
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
                self.sleep_for(sleep.duration).await;
                Ok(None)
            }
            Condition::SleepUntil(sleep) => {
                self.sleep_until(sleep.at).await;
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

    fn priority(&self) -> Option<hatchet_core::Priority> {
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

    async fn parent_output_json(&self) -> Result<Option<Value>> {
        self.base.parent_output_json().await
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
        false
    }

    async fn now(&self) -> SystemTime {
        let mut now = self.memoized_now.lock().await;
        if let Some(existing) = *now {
            return existing;
        }
        let current = SystemTime::now();
        *now = Some(current);
        current
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
        let mut memo_store = self.memo_store.lock().await;
        Ok(memo_store.entry(key.to_owned()).or_insert(value).clone())
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

fn resolve_event_payload(event: &EventCondition, observed: &ObservedDurableEvent) -> Option<Value> {
    if observed.key != event.event_key {
        return None;
    }

    if let Some(scope) = &event.scope
        && observed.scope.as_deref() != Some(scope.as_str())
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
    static NEXT_OR_GROUP_ID: AtomicU64 = AtomicU64::new(1);
    format!("or-{}", NEXT_OR_GROUP_ID.fetch_add(1, Ordering::Relaxed))
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
        ContextExt::run_child(self, workflow, input, opts).await
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
        ContextExt::bulk_run_children(self, workflow, inputs, opts).await
    }
}

impl<T: DurableContext + ?Sized> DurableContextExt for T {}
