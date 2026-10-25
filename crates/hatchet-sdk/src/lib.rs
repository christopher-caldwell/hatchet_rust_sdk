//! An unofficial Rust-first Hatchet SDK.
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

mod batch;
mod client;
mod clients;
mod context;
mod durable;
mod engine_version;
mod error;
mod idempotency;
mod management;
mod middleware;
mod rate_pause;
mod rest_client;
mod retry_after;
mod run_ref;
mod run_stream;
mod streams;
mod task;
mod telemetry;
mod transport;
mod worker;
mod workflow;

pub mod config;
pub mod durable_listener;
pub mod eviction;
pub mod grpc_transport;
pub mod health;
pub mod proto;
pub mod retry;

pub use another_hatchet_core::{
    DesiredWorkerLabel, DesiredWorkerLabelConfig, Priority, RunOpts,
    WorkerLabelComparator as RunWorkerLabelComparator, WorkflowRunId,
};
pub use another_hatchet_macros::{durable_task, task, workflow};
pub use batch::BatchConfig;
pub use client::{Client, DurableTaskHandle, TaskHandle, WorkflowHandle};
pub use clients::{
    CelClient, CelDebugResult, Cron, CronsClient, DurableBranchResult, EventRecord, EventsClient,
    FilterDefinition, FiltersClient, LogRecord, LogsClient, MetricsClient, QueueMetrics,
    RateLimitsClient, RestoreTaskResult, RunFilter, RunRecord, RunStatus, RunsClient, Schedule,
    SchedulesClient, TenantClient, WebhookDefinition, WebhooksClient, WorkerInfo, WorkersClient,
    WorkflowDefinition, WorkflowMetrics, WorkflowsClient,
};
pub use clients::{
    Pagination, RunListOptions, RunPage, RunQuery, RunSelection, TaskMutationResult,
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
pub use engine_version::{EngineFeature, EngineVersion};
pub use error::{Error, Result};
pub use idempotency::{
    BulkIdempotencyCollision, IdempotencyCollision, IdempotencyConfig, IdempotencyMethod,
};
pub use management::*;
pub use middleware::{Next, TaskFuture, TaskInvocation, TaskMiddleware};
pub use rate_pause::RateLimitPauseResult;
pub use retry_after::RetryAfterError;
pub use run_ref::{RunControl, WorkflowRunRef};
pub use run_stream::RunEventStream;
pub use streams::{
    StreamEvent, StreamOptions, StreamTopicMetadata, StreamsClient, TopicEventStream,
};
pub use task::{
    BackoffConfig, ConcurrencyConfig, ConcurrencyLimitStrategy, DesiredLabelConfig, LabelValue,
    RateLimit, RateLimitDuration, RateLimitKey, SlotType, TaskConfig, TaskConfigBuilder,
    WorkerLabelComparator,
};
pub use transport::{
    InMemoryTransport, SubmittedRun, TaskRegistration, Transport, WorkerConnectionState,
    WorkerStartOpts, WorkflowRegistration,
};
pub use worker::{Worker, WorkerBuilder, WorkerHandle};
pub use workflow::{
    DefaultFilterExpression, StepConditions, StickyStrategy, WorkflowDeclaration, WorkflowStep,
};

pub use config::{HatchetConfig, HatchetConfigBuilder, SdkLogLevel, TlsConfig};
pub use durable_listener::DurableListenerClient;
pub use eviction::{ActionKey, EvictionConfig, EvictionManager, TaskState};
pub use grpc_transport::GrpcTransport;
pub use health::{HealthServer, HealthState};
pub use retry::RetryConfig;
