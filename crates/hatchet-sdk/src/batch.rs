//! Preview server-coalesced task batches; distinct from bulk workflow submission.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::BTreeMap, time::Duration};
/// Preview server-side coalescing for a normal task. Pass the same task config
/// to the handle and workflow step. A handler receives a map keyed by member
/// task-run UUID, and returns exactly that key set unless `broadcast_output` is set.
/// Requires engine v0.114.0 or later; durable tasks and in-memory registration
/// are unsupported. The engine forces member retries to zero.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BatchConfig {
    /// Flush when this many items are available (positive signed 32-bit range).
    pub max_size: u32,
    /// Optional maximum buffering interval; positive whole milliseconds.
    pub max_interval: Option<Duration>,
    /// Optional CEL expression, such as `input.account`, separating batches.
    pub group_key: Option<String>,
    /// Optional maximum concurrent batches per group.
    pub group_max_runs: Option<u32>,
    #[serde(default)]
    /// Replicate one handler output to every member rather than requiring a map.
    pub broadcast_output: bool,
}
impl BatchConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.max_size == 0
            || self.max_size > i32::MAX as u32
            || self
                .group_max_runs
                .is_some_and(|n| n == 0 || n > i32::MAX as u32)
        {
            return Err(Error::InputValidation(vec![
                "batch size and group concurrency must be positive signed 32-bit integers".into(),
            ]));
        }
        if self.max_interval.is_some_and(|d| {
            d.as_millis() == 0
                || d.as_millis() > i32::MAX as u128
                || d.subsec_nanos() % 1_000_000 != 0
        }) {
            return Err(Error::InputValidation(vec![
                "batch interval must be a positive whole-millisecond signed 32-bit duration".into(),
            ]));
        }
        if self
            .group_key
            .as_ref()
            .is_some_and(|key| key.trim().is_empty())
        {
            return Err(Error::InputValidation(vec![
                "batch group expression must not be empty".into(),
            ]));
        }
        Ok(())
    }
}
pub(crate) fn decode(payload: &str) -> Result<BTreeMap<String, Value>> {
    let raw: BTreeMap<String, Value> = serde_json::from_str(payload)
        .map_err(|e| Error::Worker(format!("invalid batch payload: {e}")))?;
    if raw.is_empty() {
        return Err(Error::Worker("engine assigned an empty batch".into()));
    }
    raw.into_iter()
        .map(|(id, value)| {
            uuid::Uuid::parse_str(&id)
                .map_err(|_| Error::Worker("batch member ID must be an external UUID".into()))?;
            value
                .pointer("/payload/input")
                .cloned()
                .map(|input| (id, input))
                .ok_or_else(|| Error::Worker("batch member omitted payload.input".into()))
        })
        .collect()
}
pub(crate) fn outputs(ids: &[String], value: Value, broadcast: bool) -> Result<Value> {
    if broadcast {
        return Ok(Value::Object(
            ids.iter().map(|id| (id.clone(), value.clone())).collect(),
        ));
    }
    let result = value.as_object().ok_or_else(|| {
        Error::Worker("batch handler must return a map keyed by every member ID".into())
    })?;
    if result.len() != ids.len() || ids.iter().any(|id| !result.contains_key(id)) {
        return Err(Error::Worker(
            "batch output IDs must exactly match input member IDs".into(),
        ));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    #[test]
    fn assignment_decode_keeps_member_ids_and_rejects_malformed_members() {
        let id = "00000000-0000-4000-8000-000000000001";
        assert_eq!(
            decode(&json!({id:{"payload":{"input":{"x":1}}}}).to_string()).unwrap()[id],
            json!({"x":1})
        );
        for value in [
            json!({}),
            json!({"bad":{"payload":{"input":1}}}),
            json!({id:{"payload":{}}}),
            json!([]),
        ] {
            assert!(decode(&value.to_string()).is_err());
        }
        let ids = vec![id.to_owned()];
        assert!(outputs(&ids, json!({id:1,"extra":2}), false).is_err());
        assert!(outputs(&ids, json!(1), false).is_err());
    }
}
