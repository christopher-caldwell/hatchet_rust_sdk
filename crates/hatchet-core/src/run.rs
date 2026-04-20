use std::collections::HashMap;
use std::fmt::{Display, Formatter};
use std::marker::PhantomData;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Priority {
    Low,
    Normal,
    High,
    Critical,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RunOpts {
    pub metadata: HashMap<String, Value>,
    pub priority: Option<Priority>,
    pub sticky: Option<String>,
    pub key: Option<String>,
    pub desired_worker_labels: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkflowRunId(pub String);

impl Display for WorkflowRunId {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

#[derive(Debug, Clone)]
pub struct WorkflowRunRef<O> {
    pub run_id: WorkflowRunId,
    output: PhantomData<fn() -> O>,
}

impl<O> WorkflowRunRef<O> {
    #[must_use]
    pub fn new(run_id: WorkflowRunId) -> Self {
        Self {
            run_id,
            output: PhantomData,
        }
    }
}
