use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Clone, Error)]
pub enum Error {
    #[error("input serialization failed: {0}")]
    InputSerialization(String),
    #[error("output deserialization failed: {0}")]
    OutputDeserialization(String),
    #[error("transport error: {0}")]
    Transport(String),
    #[error("durable runtime error: {0}")]
    Durable(String),
    #[error("worker runtime error: {0}")]
    Worker(String),
    #[error("durable callback timed out: {0}")]
    DurableCallbackTimeout(String),
    #[error("durable listener disconnected: {0}")]
    DurableListenerDisconnected(String),
    #[error("unsupported operation in current mode: {0}")]
    UnsupportedOperation(String),
    #[error("input validation failed: {0:?}")]
    InputValidation(Vec<String>),
    #[error("non-retryable error: {0}")]
    NonRetryable(String),
    #[error(transparent)]
    RetryAfter(#[from] crate::RetryAfterError),
    #[error(transparent)]
    IdempotencyCollision(crate::IdempotencyCollision),
    #[error(transparent)]
    BulkIdempotencyCollision(crate::BulkIdempotencyCollision),
    #[error("dedupe violation: {0}")]
    DedupeViolation(String),
    #[error("gRPC error: {0}")]
    Grpc(String),
    #[error("configuration error: {0}")]
    Config(String),
}

impl Error {
    #[must_use]
    pub fn non_retryable(message: impl Into<String>) -> Self {
        Self::NonRetryable(message.into())
    }

    #[must_use]
    pub fn is_retryable(&self) -> bool {
        !matches!(
            self,
            Self::NonRetryable(_)
                | Self::InputValidation(_)
                | Self::DedupeViolation(_)
                | Self::IdempotencyCollision(_)
                | Self::BulkIdempotencyCollision(_)
                | Self::Config(_)
                | Self::UnsupportedOperation(_)
        )
    }
}
