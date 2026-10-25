//! Per-failure engine retry delays, independent of the configured retry count.
use crate::{Error, Result};
use std::time::Duration;

/// A retryable task failure requesting a delay before the next attempt.
///
/// The engine still controls retry limits and may cap the delay (v0.114.0 defaults
/// to a 30-day maximum). Engine >= v0.111.0 is required for this override. Older
/// or unknown engines receive a normal retryable failure and an SDK warning.
#[derive(Debug, Clone, thiserror::Error)]
#[error("{message}")]
pub struct RetryAfterError {
    message: String,
    after_ms: i64,
}
impl RetryAfterError {
    /// Zero requests an immediate retry. Durations must be exact whole
    /// milliseconds within the signed 64-bit wire range.
    /// # Errors
    /// Returns input validation errors for a fractional or out-of-range delay.
    pub fn new(after: Duration, message: impl Into<String>) -> Result<Self> {
        if after.subsec_nanos() % 1_000_000 != 0 || after.as_millis() > i64::MAX as u128 {
            return Err(Error::InputValidation(vec![
                "retry delay must use whole milliseconds within the signed 64-bit wire range"
                    .into(),
            ]));
        }
        Ok(Self {
            message: message.into(),
            after_ms: after.as_millis() as i64,
        })
    }
    #[must_use]
    pub fn after(&self) -> Duration {
        Duration::from_millis(self.after_ms as u64)
    }
    pub(crate) fn milliseconds(&self) -> i64 {
        self.after_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn retry_after_is_retryable_preserves_message_and_checks_wire_boundaries() {
        for ms in [0, 1, 1250, i64::MAX as u64] {
            let e = RetryAfterError::new(Duration::from_millis(ms), "busy").unwrap();
            assert_eq!(e.after(), Duration::from_millis(ms));
            assert_eq!(e.to_string(), "busy");
            assert!(Error::from(e).is_retryable());
        }
        assert!(RetryAfterError::new(Duration::from_nanos(1), "busy").is_err());
        assert!(RetryAfterError::new(Duration::from_millis(i64::MAX as u64 + 1), "busy").is_err());
        assert!(RetryAfterError::new(Duration::MAX, "busy").is_err());
    }
}
