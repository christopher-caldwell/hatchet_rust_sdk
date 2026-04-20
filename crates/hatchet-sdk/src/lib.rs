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
mod run_ref;
mod task;
mod transport;
mod worker;
mod workflow;

// New modules for production-ready infrastructure
#[cfg(feature = "grpc")]
pub mod config;
#[cfg(feature = "grpc")]
pub mod durable_listener;
pub mod eviction;
#[cfg(feature = "grpc")]
pub mod grpc_transport;
pub mod health;
#[cfg(feature = "grpc")]
pub mod proto;
pub mod retry;

pub use client::{Client, DurableTaskHandle, TaskHandle, WorkflowHandle};
pub use clients::{
    CelClient, CelDebugResult, Cron, CronsClient, EventRecord, EventsClient, FilterDefinition,
    FiltersClient, LogRecord, LogsClient, MetricsClient, QueueMetrics, RateLimitsClient, RunRecord,
    RunStatus, RunsClient, Schedule, SchedulesClient, TenantClient, WebhookDefinition,
    WebhooksClient, WorkerInfo, WorkersClient, WorkflowDefinition, WorkflowMetrics,
    WorkflowsClient,
};
pub use context::{Context, ContextExt, ContextIds, InMemoryContext, LogLevel, TriggerData};
pub use durable::{
    BaseMatchCondition, Condition, ConditionAction, ConditionMetadata, ConditionOutput, Conditions,
    DurableContext, DurableContextExt, DurableEvent, EventCondition, EventConditionBuilder,
    InMemoryDurableContext, ParentCondition, ParentConditionBuilder, ParentOverrideMatchCondition,
    RenderedConditions, SleepCondition, SleepMatchCondition, SleepResult, SleepUntilCondition,
    SleepUntilMatchCondition, UserEventMatchCondition, conditions_to_rendered, or,
    render_conditions, task_conditions_to_pb,
};
pub use error::{Error, Result};
pub use hatchet_core::{Priority, RunOpts, WorkflowRunId};
pub use hatchet_macros::{durable_task, task, workflow};
pub use run_ref::{RunControl, WorkflowRunRef};
pub use task::{
    BackoffConfig, ConcurrencyConfig, ConcurrencyLimitStrategy, DesiredLabelConfig, LabelValue,
    RateLimit, RateLimitDuration, RateLimitKey, SlotType, TaskConfig, TaskConfigBuilder,
    WorkerLabelComparator,
};
pub use transport::{
    InMemoryTransport, SubmittedRun, TaskRegistration, Transport, WorkflowRegistration,
};
pub use worker::{Worker, WorkerBuilder, WorkerHandle};
pub use workflow::{
    DefaultFilterExpression, StepConditions, StickyStrategy, WorkflowDeclaration, WorkflowStep,
};

// Conditional gRPC exports
#[cfg(feature = "grpc")]
pub use config::{HatchetConfig, HatchetConfigBuilder, SdkLogLevel, TlsConfig};
#[cfg(feature = "grpc")]
pub use durable_listener::DurableListenerClient;
pub use eviction::{ActionKey, EvictionConfig, EvictionManager, TaskState};
#[cfg(feature = "grpc")]
pub use grpc_transport::GrpcTransport;
pub use health::{HealthServer, HealthState};
pub use retry::RetryConfig;
