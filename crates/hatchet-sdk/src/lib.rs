//! Minimal Rust-first Hatchet SDK surface for personal use.
//!
//! This crate is intentionally scoped around:
//! - typed workflow declaration and run APIs
//! - child workflow execution from task context
//! - durable task context primitives
//! - in-process worker runtime and registration
//! - feature-oriented clients (`runs`, `workflows`, `events`, etc.)
//! - optional proc-macro sugar (`#[workflow]`, `#[task]`, `#[durable_task]`)
//!
//! For runnable end-to-end usage, see the examples workspace binaries.

mod client;
mod clients;
mod context;
mod durable;
mod error;
mod transport;
mod worker;

pub use client::{Client, DurableTaskHandle, TaskHandle, WorkflowHandle};
pub use clients::{
    CelClient, CelDebugResult, Cron, CronsClient, EventRecord, EventsClient, FilterDefinition,
    FiltersClient, LogRecord, LogsClient, MetricsClient, QueueMetrics, RateLimitsClient, RunRecord,
    RunStatus, RunsClient, Schedule, SchedulesClient, WebhookDefinition, WebhooksClient,
    WorkerInfo, WorkersClient, WorkflowDefinition, WorkflowMetrics, WorkflowsClient,
};
pub use context::{Context, ContextIds, InMemoryContext};
pub use durable::{DurableContext, DurableEvent, InMemoryDurableContext};
pub use error::{Error, Result};
pub use hatchet_core::{Priority, RunOpts, WorkflowRunId, WorkflowRunRef};
pub use hatchet_macros::{durable_task, task, workflow};
pub use transport::{InMemoryTransport, SubmittedRun, Transport};
pub use worker::{Worker, WorkerBuilder, WorkerHandle};
