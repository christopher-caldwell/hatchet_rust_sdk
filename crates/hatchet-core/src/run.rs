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

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RunOpts {
    pub additional_metadata: HashMap<String, String>,
    pub priority: Option<Priority>,
    pub sticky: Option<String>,
    pub child_key: Option<String>,
    pub desired_worker_labels: HashMap<String, String>,
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
