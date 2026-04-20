use thiserror::Error;

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug, Error)]
pub enum Error {
    #[error("input serialization failed: {0}")]
    InputSerialization(String),
    #[error("transport error: {0}")]
    Transport(String),
    #[error("durable runtime error: {0}")]
    Durable(String),
}
