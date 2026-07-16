//! Re-exports of protobuf-generated types from the Hatchet API contracts.
//!
//! These types are generated at build time by `tonic-build` from the vendored
//! `.proto` files under `crates/hatchet-sdk/proto/`.

/// v1 API types — the primary API surface for the Rust SDK.
///
/// Includes `AdminService` (workflow registration), `V1Dispatcher` (durable task
/// bidi streaming), and shared types for conditions, triggers, and worker labels.
pub mod v1 {
    tonic::include_proto!("v1");
}

/// Legacy types (packageless protos) — dispatcher, events, and workflows services.
///
/// Includes dispatcher (`WorkerRegisterRequest`, `AssignedAction`, `StepActionEvent`),
/// events (`PushEventRequest`, `PutLogRequest`), and legacy workflow admin service
/// (`TriggerWorkflowResponse`, `BulkTriggerWorkflowRequest`).
///
/// These proto files have no `package` declaration, so tonic-build merges them
/// into a single output file named `_.rs`.
pub mod legacy {
    // Bring v1 into scope here so that references like `v1::TriggerWorkflowRequest` in _.rs resolve
    pub use super::v1;
    tonic::include_proto!("_");
}

// Re-export commonly used types under convenience aliases
pub mod dispatcher {
    pub use super::legacy::*;
}

pub mod events {
    pub use super::legacy::*;
}

pub mod workflows {
    pub use super::legacy::*;
}
