use std::collections::HashMap;
use std::fmt::{Display, Formatter};

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Priority {
    Low,
    Normal,
    High,
    Critical,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WorkerLabelComparator {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanOrEqual,
    LessThan,
    LessThanOrEqual,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DesiredWorkerLabel {
    pub value: String,
    #[serde(default)]
    pub required: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub weight: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub comparator: Option<WorkerLabelComparator>,
}

impl DesiredWorkerLabel {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            required: false,
            weight: None,
            comparator: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DesiredWorkerLabelConfig {
    Legacy(String),
    Rich(DesiredWorkerLabel),
}

impl From<String> for DesiredWorkerLabelConfig {
    fn from(value: String) -> Self {
        Self::Legacy(value)
    }
}

impl From<&str> for DesiredWorkerLabelConfig {
    fn from(value: &str) -> Self {
        Self::Legacy(value.to_owned())
    }
}

impl From<DesiredWorkerLabel> for DesiredWorkerLabelConfig {
    fn from(value: DesiredWorkerLabel) -> Self {
        Self::Rich(value)
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct RunOpts {
    pub additional_metadata: HashMap<String, String>,
    pub priority: Option<Priority>,
    pub sticky: Option<String>,
    pub child_key: Option<String>,
    pub desired_worker_labels: HashMap<String, DesiredWorkerLabelConfig>,
    pub parent_workflow_run_id: Option<String>,
    pub parent_task_run_id: Option<String>,
    pub child_index: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkflowRunId(pub String);

impl Display for WorkflowRunId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::{DesiredWorkerLabelConfig, RunOpts};
    use std::collections::HashMap;

    #[test]
    fn run_opts_deserializes_legacy_label_map_shape() {
        let value = serde_json::json!({
            "desiredWorkerLabels": {
                "region": "us-east"
            }
        });
        let parsed: RunOpts = serde_json::from_value(value).expect("legacy shape parses");
        assert_eq!(
            parsed.desired_worker_labels.get("region"),
            Some(&DesiredWorkerLabelConfig::Legacy("us-east".to_owned()))
        );
    }

    #[test]
    fn run_opts_serializes_legacy_label_values_as_strings() {
        let mut opts = RunOpts::default();
        let mut labels = HashMap::new();
        labels.insert(
            "zone".to_owned(),
            DesiredWorkerLabelConfig::Legacy("1a".to_owned()),
        );
        opts.desired_worker_labels = labels;

        let value = serde_json::to_value(opts).expect("run opts serialize");
        assert_eq!(
            value["desiredWorkerLabels"]["zone"],
            serde_json::json!("1a")
        );
    }
}
