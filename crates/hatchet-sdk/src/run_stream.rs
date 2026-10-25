//! Incremental consumption of task-published workflow stream values.
use std::future::poll_fn;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_core::Stream;
use serde_json::Value;

use crate::Result;

/// A stream of JSON values published by tasks in a workflow run.
///
/// Values arrive incrementally, without an idle timeout. The stream finishes
/// when the engine closes the subscription or reports workflow completion.
/// Transport errors are yielded to the consumer. Dropping the stream closes
/// its underlying subscription. This also implements [`Stream`] for use with
/// standard Rust stream combinators.
pub struct RunEventStream {
    inner: Pin<Box<dyn Stream<Item = Result<Value>> + Send + 'static>>,
}

impl std::fmt::Debug for RunEventStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunEventStream").finish_non_exhaustive()
    }
}

impl RunEventStream {
    /// Wraps a provider's incremental stream without a forwarding task or queue.
    pub fn new(stream: impl Stream<Item = Result<Value>> + Send + 'static) -> Self {
        Self {
            inner: Box::pin(stream),
        }
    }

    /// Adapts a finite set of values, primarily for custom snapshot transports.
    pub fn from_values(values: Vec<Value>) -> Self {
        Self::new(async_stream::stream! {
            for value in values { yield Ok(value); }
        })
    }

    /// Waits for the next published value, transport error, or end of stream.
    pub async fn next(&mut self) -> Option<Result<Value>> {
        poll_fn(|cx| Pin::new(&mut *self).poll_next(cx)).await
    }

    /// Collects values until the subscription ends.
    ///
    /// # Errors
    /// Returns the first transport error instead of silently returning a partial batch.
    pub async fn collect_all(mut self) -> Result<Vec<Value>> {
        let mut values = Vec::new();
        while let Some(value) = self.next().await {
            values.push(value?);
        }
        Ok(values)
    }
}

impl Stream for RunEventStream {
    type Item = Result<Value>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().inner.as_mut().poll_next(cx)
    }
}
