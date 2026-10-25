//! Engine-owned idempotency configuration and collision results.
use crate::{Error, Result, WorkflowRunId};
use serde::{Deserialize, Serialize};
use std::time::Duration;

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum IdempotencyMethod {
    #[default]
    Ttl,
    Status,
}

/// A workflow policy, also applicable to a workflow containing one standalone task.
/// STATUS retains the key until a terminal outcome, with TTL as its fallback cap.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IdempotencyConfig {
    pub expression: String,
    pub ttl: Duration,
    #[serde(default)]
    pub method: IdempotencyMethod,
}
impl IdempotencyConfig {
    pub(crate) fn ttl_ms(&self) -> Result<i64> {
        let millis = i64::try_from(self.ttl.as_millis()).map_err(|_| {
            Error::InputValidation(vec![
                "idempotency TTL exceeds signed 64-bit milliseconds".into(),
            ])
        })?;
        if self.expression.trim().is_empty()
            || millis <= 0
            || self.ttl.subsec_nanos() % 1_000_000 != 0
        {
            return Err(Error::InputValidation(vec!["idempotency requires a nonempty CEL expression and a positive whole-millisecond TTL".into()]));
        }
        Ok(millis)
    }
}
#[derive(Debug, Clone, thiserror::Error)]
#[error("idempotency collision: existing run {existing_run_id}")]
pub struct IdempotencyCollision {
    pub existing_run_id: WorkflowRunId,
    pub colliding_run_id: Option<WorkflowRunId>,
}
/// A bulk failure can include successful submissions. Retrying the original batch
/// can duplicate those submissions; reattach using the returned successful IDs.
#[derive(Debug, Clone, thiserror::Error)]
#[error("bulk idempotency collision: {collisions:?}; successful runs: {successful_run_ids:?}")]
pub struct BulkIdempotencyCollision {
    pub successful_run_ids: Vec<WorkflowRunId>,
    pub collisions: Vec<IdempotencyCollision>,
}
