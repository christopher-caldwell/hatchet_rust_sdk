//! Retry wrapper with exponential backoff and jitter.
//!
//! Applied to all gRPC calls in `GrpcTransport` to handle transient failures.
//! Mirrors the TS SDK's `retrier()` utility.

use std::future::Future;
use std::time::Duration;

use crate::error::{Error, Result};

/// Default maximum number of retries for gRPC calls.
pub const DEFAULT_MAX_RETRIES: u32 = 3;

/// Default initial backoff duration.
pub const DEFAULT_INITIAL_BACKOFF: Duration = Duration::from_millis(100);

/// Default maximum backoff duration.
pub const DEFAULT_MAX_BACKOFF: Duration = Duration::from_secs(10);

/// Default backoff multiplier.
pub const DEFAULT_BACKOFF_MULTIPLIER: f64 = 2.0;

/// Retry configuration.
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts (0 = no retries, just the initial attempt).
    pub max_retries: u32,
    /// Initial backoff duration before the first retry.
    pub initial_backoff: Duration,
    /// Maximum backoff duration (caps exponential growth).
    pub max_backoff: Duration,
    /// Multiplier applied to backoff on each retry.
    pub backoff_multiplier: f64,
    /// Lower bound for jitter factor applied to each sleep.
    pub jitter_min_factor: f64,
    /// Upper bound for jitter factor applied to each sleep.
    pub jitter_max_factor: f64,
    /// Maximum total wall-clock time spent retrying.
    pub max_elapsed: Option<Duration>,
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: DEFAULT_MAX_RETRIES,
            initial_backoff: DEFAULT_INITIAL_BACKOFF,
            max_backoff: DEFAULT_MAX_BACKOFF,
            backoff_multiplier: DEFAULT_BACKOFF_MULTIPLIER,
            jitter_min_factor: 0.75,
            jitter_max_factor: 1.25,
            max_elapsed: None,
        }
    }
}

/// Retries the given async function with exponential backoff and jitter.
///
/// The function `f` is called repeatedly until it either succeeds or the
/// maximum number of retries is exhausted. Only retryable errors (as
/// determined by [`Error::is_retryable`]) trigger a retry; non-retryable
/// errors are returned immediately.
///
/// # Errors
///
/// Returns the last error if all retry attempts fail.
pub async fn retry<F, Fut, T>(f: F, config: &RetryConfig) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    let mut last_error = None;
    let mut backoff = config.initial_backoff;
    let started_at = tokio::time::Instant::now();

    for attempt in 0..=config.max_retries {
        match f().await {
            Ok(value) => return Ok(value),
            Err(error) => {
                if !error.is_retryable() || attempt == config.max_retries {
                    return Err(error);
                }

                last_error = Some(error);

                let jitter_span = (config.jitter_max_factor - config.jitter_min_factor).max(0.0);
                let jitter_factor = config.jitter_min_factor + (pseudo_random_f64() * jitter_span);
                let jittered = Duration::from_secs_f64(backoff.as_secs_f64() * jitter_factor);
                let capped = jittered.min(config.max_backoff);

                if config
                    .max_elapsed
                    .is_some_and(|max_elapsed| started_at.elapsed() + capped > max_elapsed)
                {
                    return Err(last_error.unwrap_or_else(|| {
                        Error::Transport("retry max elapsed exceeded".to_owned())
                    }));
                }

                tokio::time::sleep(capped).await;

                // Exponential growth
                backoff = Duration::from_secs_f64(
                    (backoff.as_secs_f64() * config.backoff_multiplier)
                        .min(config.max_backoff.as_secs_f64()),
                );
            }
        }
    }

    Err(last_error.unwrap_or_else(|| Error::Transport("retry exhausted with no error".to_owned())))
}

/// Retries with default configuration.
///
/// # Errors
///
/// Returns the last error if all retry attempts fail.
pub async fn retry_default<F, Fut, T>(f: F) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    retry(f, &RetryConfig::default()).await
}

/// Cheap pseudo-random float in [0, 1) using system time as seed.
/// Not cryptographically secure — only used for jitter.
fn pseudo_random_f64() -> f64 {
    use std::time::SystemTime;
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    // Mix the bits a bit for better distribution
    let mixed = nanos.wrapping_mul(2654435761); // Knuth's multiplicative hash
    f64::from(mixed) / f64::from(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    #[tokio::test]
    async fn succeeds_on_first_attempt() {
        let result = retry(|| async { Ok::<_, Error>(42) }, &RetryConfig::default()).await;
        assert_eq!(result.unwrap(), 42);
    }

    #[tokio::test]
    async fn retries_on_transient_failure() {
        let call_count = AtomicU32::new(0);
        let config = RetryConfig {
            max_retries: 3,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(10),
            backoff_multiplier: 2.0,
            jitter_min_factor: 1.0,
            jitter_max_factor: 1.0,
            max_elapsed: None,
        };

        let result = retry(
            || {
                let count = call_count.fetch_add(1, Ordering::Relaxed);
                async move {
                    if count < 2 {
                        Err(Error::Transport("transient".to_owned()))
                    } else {
                        Ok(99)
                    }
                }
            },
            &config,
        )
        .await;

        assert_eq!(result.unwrap(), 99);
        assert_eq!(call_count.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn does_not_retry_non_retryable() {
        let call_count = AtomicU32::new(0);
        let config = RetryConfig {
            max_retries: 5,
            initial_backoff: Duration::from_millis(1),
            ..RetryConfig::default()
        };

        let result: Result<i32> = retry(
            || {
                call_count.fetch_add(1, Ordering::Relaxed);
                async { Err(Error::NonRetryable("permanent".to_owned())) }
            },
            &config,
        )
        .await;

        assert!(result.is_err());
        assert_eq!(call_count.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn exhausts_retries() {
        let call_count = AtomicU32::new(0);
        let config = RetryConfig {
            max_retries: 2,
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(5),
            backoff_multiplier: 2.0,
            jitter_min_factor: 1.0,
            jitter_max_factor: 1.0,
            max_elapsed: None,
        };

        let result: Result<i32> = retry(
            || {
                call_count.fetch_add(1, Ordering::Relaxed);
                async { Err(Error::Transport("still failing".to_owned())) }
            },
            &config,
        )
        .await;

        assert!(result.is_err());
        // 1 initial + 2 retries = 3 total
        assert_eq!(call_count.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn stops_when_max_elapsed_is_exceeded() {
        let call_count = AtomicU32::new(0);
        let config = RetryConfig {
            max_retries: 10,
            initial_backoff: Duration::from_millis(10),
            max_backoff: Duration::from_millis(10),
            backoff_multiplier: 1.0,
            jitter_min_factor: 1.0,
            jitter_max_factor: 1.0,
            max_elapsed: Some(Duration::from_millis(15)),
        };

        let result: Result<i32> = retry(
            || {
                call_count.fetch_add(1, Ordering::Relaxed);
                async { Err(Error::Transport("still failing".to_owned())) }
            },
            &config,
        )
        .await;

        assert!(result.is_err());
        assert!(call_count.load(Ordering::Relaxed) <= 2);
    }
}
