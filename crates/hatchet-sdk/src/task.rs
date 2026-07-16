use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct TaskConfig {
    pub execution_timeout: Option<u64>,
    pub schedule_timeout: Option<u64>,
    pub retries: Option<u32>,
    pub backoff: Option<BackoffConfig>,
    pub rate_limits: Vec<RateLimit>,
    pub desired_worker_labels: Vec<(String, DesiredLabelConfig)>,
    pub concurrency: Option<ConcurrencyConfig>,
    pub input_schema: Option<Value>,
    /// Slot requests — allows a task to request multiple worker slots.
    /// Key is the slot type (`Default` or `Durable`), value is the number of
    /// slots consumed. Mirrors the TS `slotRequests: Record<SlotType, number>`.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub slot_requests: HashMap<SlotType, u32>,
}

/// Type of worker slot a task can request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum SlotType {
    /// Default (non-durable) worker slot.
    Default,
    /// Durable worker slot.
    Durable,
}

impl SlotType {
    #[must_use]
    pub(crate) fn as_engine_key(self) -> &'static str {
        match self {
            Self::Default => "default",
            Self::Durable => "durable",
        }
    }
}

impl TaskConfig {
    #[must_use]
    pub fn builder() -> TaskConfigBuilder {
        TaskConfigBuilder::default()
    }

    #[must_use]
    pub fn with_input_schema_value(mut self, schema: Value) -> Self {
        self.input_schema = Some(schema);
        self
    }

    #[must_use]
    pub fn validate_input_value(&self, input: &Value) -> Vec<String> {
        let mut errors = Vec::new();
        if let Some(schema) = &self.input_schema {
            validate_schema(schema, input, "$", &mut errors);
        }
        errors
    }
}

#[derive(Debug, Clone, Default)]
pub struct TaskConfigBuilder {
    inner: TaskConfig,
}

impl TaskConfigBuilder {
    #[must_use]
    pub fn execution_timeout(mut self, timeout_secs: u64) -> Self {
        self.inner.execution_timeout = Some(timeout_secs);
        self
    }

    #[must_use]
    pub fn schedule_timeout(mut self, timeout_secs: u64) -> Self {
        self.inner.schedule_timeout = Some(timeout_secs);
        self
    }

    #[must_use]
    pub fn retries(mut self, retries: u32) -> Self {
        self.inner.retries = Some(retries);
        self
    }

    #[must_use]
    pub fn backoff(mut self, backoff: BackoffConfig) -> Self {
        self.inner.backoff = Some(backoff);
        self
    }

    #[must_use]
    pub fn rate_limit(mut self, rate_limit: RateLimit) -> Self {
        self.inner.rate_limits.push(rate_limit);
        self
    }

    #[must_use]
    pub fn desired_label(mut self, key: impl Into<String>, config: DesiredLabelConfig) -> Self {
        self.inner.desired_worker_labels.push((key.into(), config));
        self
    }

    #[must_use]
    pub fn concurrency(mut self, concurrency: ConcurrencyConfig) -> Self {
        self.inner.concurrency = Some(concurrency);
        self
    }

    #[must_use]
    pub fn input_schema(mut self, schema: Value) -> Self {
        self.inner.input_schema = Some(schema);
        self
    }

    #[must_use]
    pub fn slot_request(mut self, slot_type: SlotType, units: u32) -> Self {
        self.inner.slot_requests.insert(slot_type, units);
        self
    }

    #[must_use]
    pub fn build(self) -> TaskConfig {
        self.inner
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct BackoffConfig {
    pub factor: f64,
    pub max_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RateLimit {
    pub units: u32,
    pub limit: u32,
    pub duration: RateLimitDuration,
    pub key: RateLimitKey,
}

impl RateLimit {
    #[must_use]
    pub fn new(units: u32, limit: u32, duration: RateLimitDuration) -> Self {
        Self {
            units,
            limit,
            duration,
            key: RateLimitKey::None,
        }
    }

    #[must_use]
    pub fn with_static_key(mut self, key: impl Into<String>) -> Self {
        self.key = RateLimitKey::Static(key.into());
        self
    }

    #[must_use]
    pub fn with_dynamic_key(mut self, key: impl Into<String>) -> Self {
        self.key = RateLimitKey::Dynamic(key.into());
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum RateLimitKey {
    None,
    Static(String),
    Dynamic(String),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum RateLimitDuration {
    Second,
    Minute,
    Hour,
    Day,
    Week,
    Month,
    Year,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DesiredLabelConfig {
    pub value: LabelValue,
    pub required: bool,
    pub weight: Option<u32>,
    pub comparator: WorkerLabelComparator,
}

impl DesiredLabelConfig {
    #[must_use]
    pub fn new(value: impl Into<LabelValue>) -> Self {
        Self {
            value: value.into(),
            required: false,
            weight: None,
            comparator: WorkerLabelComparator::Equal,
        }
    }

    #[must_use]
    pub fn required(mut self, required: bool) -> Self {
        self.required = required;
        self
    }

    #[must_use]
    pub fn weight(mut self, weight: u32) -> Self {
        self.weight = Some(weight);
        self
    }

    #[must_use]
    pub fn comparator(mut self, comparator: WorkerLabelComparator) -> Self {
        self.comparator = comparator;
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum LabelValue {
    String(String),
    Int(i64),
    Bool(bool),
}

impl From<&str> for LabelValue {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<String> for LabelValue {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<i64> for LabelValue {
    fn from(value: i64) -> Self {
        Self::Int(value)
    }
}

impl From<bool> for LabelValue {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl LabelValue {
    #[must_use]
    pub fn as_string(&self) -> String {
        match self {
            Self::String(value) => value.clone(),
            Self::Int(value) => value.to_string(),
            Self::Bool(value) => value.to_string(),
        }
    }

    #[must_use]
    pub fn as_number(&self) -> Option<f64> {
        match self {
            Self::String(value) => value.parse::<f64>().ok(),
            Self::Int(value) => Some(*value as f64),
            Self::Bool(_) => None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WorkerLabelComparator {
    Equal,
    NotEqual,
    GreaterThan,
    LessThan,
    GreaterThanOrEqual,
    LessThanOrEqual,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ConcurrencyConfig {
    pub expression: String,
    pub max_runs: Option<u32>,
    pub limit_strategy: Option<ConcurrencyLimitStrategy>,
}

impl ConcurrencyConfig {
    #[must_use]
    pub fn new(expression: impl Into<String>) -> Self {
        Self {
            expression: expression.into(),
            max_runs: None,
            limit_strategy: None,
        }
    }

    #[must_use]
    pub fn max_runs(mut self, max_runs: u32) -> Self {
        self.max_runs = Some(max_runs);
        self
    }

    #[must_use]
    pub fn limit_strategy(mut self, limit_strategy: ConcurrencyLimitStrategy) -> Self {
        self.limit_strategy = Some(limit_strategy);
        self
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ConcurrencyLimitStrategy {
    CancelInProgress,
    CancelNewest,
    GroupRoundRobin,
}

fn validate_schema(schema: &Value, input: &Value, path: &str, errors: &mut Vec<String>) {
    if let Some(expected_type) = schema.get("type").and_then(Value::as_str) {
        let matches = match expected_type {
            "object" => input.is_object(),
            "array" => input.is_array(),
            "string" => input.is_string(),
            "integer" => input.is_i64() || input.is_u64(),
            "number" => input.is_number(),
            "boolean" => input.is_boolean(),
            "null" => input.is_null(),
            _ => true,
        };

        if !matches {
            errors.push(format!("{path}: expected {expected_type}"));
            return;
        }
    }

    if let Some(enum_values) = schema.get("enum").and_then(Value::as_array)
        && !enum_values.iter().any(|candidate| candidate == input)
    {
        errors.push(format!("{path}: value is not in enum"));
    }

    if let Some(object) = input.as_object() {
        if let Some(required) = schema.get("required").and_then(Value::as_array) {
            for key in required.iter().filter_map(Value::as_str) {
                if !object.contains_key(key) {
                    errors.push(format!("{path}.{key}: required field missing"));
                }
            }
        }

        if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
            for (key, property_schema) in properties {
                if let Some(value) = object.get(key) {
                    validate_schema(property_schema, value, &format!("{path}.{key}"), errors);
                }
            }
        }
    }

    if let Some(items_schema) = schema.get("items")
        && let Some(items) = input.as_array()
    {
        for (index, item) in items.iter().enumerate() {
            validate_schema(items_schema, item, &format!("{path}[{index}]"), errors);
        }
    }
}
