# Hatchet Rust SDK — Remaining Work to Production-Ready

## Current State

The Rust SDK has **full model-layer and API-surface parity** with the TypeScript v1 SDK. All workflow declarations, condition types, context methods, feature clients, and worker builder patterns are implemented and tested (43 tests, zero warnings).

**What does NOT work:** The SDK cannot connect to a real Hatchet engine. It only works against `InMemoryTransport` for local testing. There is no gRPC transport, no `DurableListenerClient`, no real workflow registration, and no real run dispatch.

This document lists **everything** that must be done to make the SDK production-ready. Items are ordered by dependency (do earlier items first).

---

## Reference Material

- **TS SDK source:** `sources/hatchet-main/sdks/typescript/src/v1/`
- **Proto contracts:** `sources/hatchet-main/api-contracts/`  
  - `v1/dispatcher.proto` — Worker registration, action dispatch, durable task streaming
  - `v1/workflows.proto` — `putWorkflow`, `triggerWorkflow`, `bulkTriggerWorkflow`, `putRateLimit`
  - `v1/shared/condition.proto` — Condition protobuf shapes
  - `v1/shared/trigger.proto` — `TriggerWorkflowRequest`, `DesiredWorkerLabels`
  - `dispatcher/dispatcher.proto` — Legacy dispatcher (heartbeat, refreshTimeout, releaseSlot, sendActionEvent)
  - `events/events.proto` — Event push, put log, put stream
  - `workflows/workflows.proto` — Legacy workflow trigger

---

## 1. Proto Codegen Setup

**Goal:** Generate Rust types from the Hatchet proto contracts.

### Steps

1. Add `tonic-build` to `[build-dependencies]` and `tonic` + `prost` to `[dependencies]` of `hatchet-sdk/Cargo.toml`.
2. Create `hatchet-sdk/build.rs` that compiles all `.proto` files from `sources/hatchet-main/api-contracts/`:
   ```rust
   // build.rs
   fn main() -> Result<(), Box<dyn std::error::Error>> {
       tonic_build::configure()
           .build_server(false)
           .compile_protos(
               &[
                   "../../../sources/hatchet-main/api-contracts/v1/dispatcher.proto",
                   "../../../sources/hatchet-main/api-contracts/v1/workflows.proto",
                   "../../../sources/hatchet-main/api-contracts/v1/shared/condition.proto",
                   "../../../sources/hatchet-main/api-contracts/v1/shared/trigger.proto",
                   "../../../sources/hatchet-main/api-contracts/dispatcher/dispatcher.proto",
                   "../../../sources/hatchet-main/api-contracts/events/events.proto",
                   "../../../sources/hatchet-main/api-contracts/workflows/workflows.proto",
               ],
               &["../../../sources/hatchet-main/api-contracts/"],
           )?;
       Ok(())
   }
   ```
3. Create `hatchet-sdk/src/proto.rs` that `include!`s the generated code and re-exports it.
4. Verify `cargo build` generates valid Rust types for all services and messages.

### Dependencies

- `tonic = "0.12"` (or latest)
- `prost = "0.13"`
- `tonic-build = "0.12"` (build-dep)
- `prost-types = "0.13"` (for `google.protobuf.Timestamp`)

---

## 2. Configuration + TLS

**Goal:** Load connection config from env vars / config file, matching TS `ConfigLoader`.

### What TS Does (file: `src/clients/hatchet-client/client-config.ts`)

- Reads `HATCHET_CLIENT_TOKEN` (JWT), `HATCHET_CLIENT_HOST_PORT`, `HATCHET_CLIENT_TLS_*` env vars
- Parses the JWT to extract `server_url`, `grpc_broadcast_address`
- Supports TLS with client certs, CA cert, or insecure mode
- Validates config with Zod schema

### Rust Implementation

1. Create `hatchet-sdk/src/config.rs`:
   ```rust
   pub struct HatchetConfig {
       pub host_port: String,        // e.g. "localhost:7077"
       pub token: String,            // JWT bearer token
       pub tls: TlsConfig,
       pub namespace: Option<String>,
       pub tenant_id: String,        // extracted from JWT
       pub log_level: LogLevel,
   }

   pub enum TlsConfig {
       Insecure,
       System,
       Custom {
           ca_cert: PathBuf,
           client_cert: Option<PathBuf>,
           client_key: Option<PathBuf>,
           server_name: Option<String>,
       },
   }

   impl HatchetConfig {
       pub fn from_env() -> Result<Self> { /* ... */ }
   }
   ```
2. Parse the JWT to extract `server_url` and `tenant_id` (use `jsonwebtoken` or manual base64 decode — the TS SDK just does base64 decode without signature verification).
3. Build `tonic::transport::Channel` with appropriate TLS credentials.

---

## 3. gRPC Transport Implementation

**Goal:** Implement a real `GrpcTransport` that satisfies the existing `Transport` trait by making actual gRPC calls.

### Architecture

```
GrpcTransport
├── AdminServiceClient (v1/workflows.proto)
│   ├── putWorkflow()        → register_workflow_definition()
│   └── putRateLimit()       → (new method on Transport)
├── WorkflowServiceClient (workflows/workflows.proto)
│   ├── triggerWorkflow()    → submit_run()
│   └── bulkTriggerWorkflow() → (used by bulk child runs)
├── DispatcherClient (dispatcher/dispatcher.proto)
│   ├── register()           → worker registration
│   ├── listen()             → action stream (bidirectional)
│   ├── sendStepActionEvent() → resolve_result()
│   ├── refreshTimeout()     → (used by ctx.refresh_timeout())
│   └── releaseSlot()        → (used by ctx.release_slot())
├── EventClient (events/events.proto)
│   ├── push()               → event push
│   ├── putLog()             → log()
│   └── putStream()          → put_stream()
└── V1DispatcherClient (v1/dispatcher.proto)
    └── durableTask()        → Durable bidi stream
```

### Steps

1. Create `hatchet-sdk/src/grpc_transport.rs` implementing `Transport`:

   ```rust
   pub struct GrpcTransport {
       config: HatchetConfig,
       admin: AdminServiceClient<Channel>,
       workflows: WorkflowServiceClient<Channel>,
       dispatcher: DispatcherServiceClient<Channel>,
       events: EventServiceClient<Channel>,
       v1_dispatcher: V1DispatcherServiceClient<Channel>,
   }
   ```

2. Implement every method on the `Transport` trait:

   | `Transport` method | gRPC call |
   |---|---|
   | `register_workflow_definition()` | `admin.putWorkflow()` — must convert `WorkflowRegistration` → `CreateWorkflowVersionRequest` proto |
   | `submit_run()` | `workflows.triggerWorkflow()` → returns `WorkflowRunId` |
   | `resolve_result()` | `dispatcher.sendStepActionEvent()` with `STEP_EVENT_TYPE_COMPLETED` or `STEP_EVENT_TYPE_FAILED` |
   | `log()` | `events.putLog()` |
   | `append_run_stream()` | `events.putStream()` |
   | `stream_run()` | Subscribe via `RunListenerClient` |
   | `list_logs()` | REST API call (not gRPC) |
   | `register_task_dispatcher()` | No-op for gRPC — actions come via the dispatcher listen stream |

3. **Critical conversion:** The `WorkflowRegistration` struct must map to `CreateWorkflowVersionRequest` proto. Study the TS `admin.putWorkflow()` path:
   - File: `sources/hatchet-main/sdks/typescript/src/v1/client/worker/worker-internal.ts` (search for `putWorkflow`)
   - The TS builds the proto request with task definitions, conditions, concurrency keys, rate limits, etc.

4. Add `from_config()` constructor:
   ```rust
   impl GrpcTransport {
       pub async fn from_config(config: HatchetConfig) -> Result<Self> {
           let channel = Channel::from_shared(format!("https://{}", config.host_port))?
               .tls_config(/* from config.tls */)
               .connect()
               .await?;
           // Create all gRPC clients from the channel
       }
   }
   ```

5. Add token interceptor (bearer auth on every gRPC call):
   ```rust
   // Add `Authorization: Bearer {token}` to all requests
   ```

---

## 4. Worker Registration + Action Listener

**Goal:** When `Worker::start()` is called with a `GrpcTransport`, it must register with the engine and listen for incoming actions (task assignments).

### What TS Does (file: `worker-internal.ts`)

1. For each workflow, calls `admin.putWorkflow()` to register the workflow definition
2. Calls `dispatcher.register()` to register the worker (gets back a `workerId`)
3. Opens `dispatcher.listen()` — a server-streaming RPC that sends `AssignedAction` messages
4. For each action received, dispatches to the correct task handler, runs it, and sends the result back via `dispatcher.sendStepActionEvent()`

### Rust Implementation

1. Modify `Worker::start()` for `GrpcTransport`:
   - Register all workflows via `transport.register_workflow_definition()`
   - Call `dispatcher.register()` with worker name, actions list, labels
   - Spawn a tokio task that calls `dispatcher.listen()` and processes the stream
   
2. For each `AssignedAction` from the stream:
   - Extract `taskRunExternalId`, `actionId`, `actionPayload`, `retryCount`, etc.
   - Parse `actionPayload` JSON to get input + context data (`triggers`, `parents`, `step_run_errors`, etc.)
   - Build `ContextIds` with real IDs from the action
   - Dispatch to the correct task handler
   - Send result back via `dispatcher.sendStepActionEvent()`

3. Handle heartbeat: The TS worker sends periodic heartbeats via `dispatcher.sendStepActionEvent()` with `STEP_EVENT_TYPE_STARTED`. The Rust worker needs to do the same.

4. Handle graceful shutdown: When `Worker::shutdown()` is called, stop accepting new actions, wait for in-flight tasks, then close the stream.

### Reference Files
- `sources/hatchet-main/sdks/typescript/src/v1/client/worker/worker-internal.ts` (1200+ lines, the core worker loop)
- `sources/hatchet-main/api-contracts/dispatcher/dispatcher.proto` (the `register`, `listen`, `sendStepActionEvent` RPCs)

---

## 5. DurableListenerClient

**Goal:** Implement the real durable task streaming client that backs `DurableContext.sleepFor()`, `waitFor()`, `memo()`, `now()`, and `spawnChild()`.

### What TS Does (file: `durable-listener-client.ts`, 967 lines)

The `DurableListenerClient` maintains a **bidirectional gRPC stream** (`v1/dispatcher.proto:durableTask`) with the engine:

1. **Sends requests** when durable operations are called:
   - `waitFor` → sends `DurableTaskWaitForRequest` with sleep/event conditions
   - `runChildren` → sends `DurableTaskTriggerRunsRequest` with workflow trigger opts
   - `memo` → sends `DurableTaskMemoRequest` with key + optional payload
   - `completeMemo` → sends `DurableTaskCompleteMemoRequest` with result
   - `evictInvocation` → sends eviction request

2. **Receives responses** from the engine:
   - `triggerRunsAck` → acknowledges child run dispatch
   - `waitForAck` → acknowledges wait condition registration (`branchId`, `nodeId`)
   - `memoAck` → acknowledges memo (may return cached value)
   - `entryCompleted` → the actual resolution: condition was met, child completed, etc.
   - `error` → error response (including `NonDeterminismError`)
   - `serverEvict` → engine requests eviction of a durable invocation

3. **Deferred pattern:** Each `sendEvent()` creates a `Deferred` promise keyed by `taskExternalId:invocationCount`. When the matching ack arrives, the promise resolves. Then `waitForCallback()` creates another deferred keyed by `taskExternalId:invocationCount:branchId:nodeId` that resolves when `entryCompleted` arrives.

### Rust Implementation

1. Create `hatchet-sdk/src/durable_listener.rs`:
   ```rust
   pub struct DurableListenerClient {
       config: HatchetConfig,
       v1_dispatcher: V1DispatcherServiceClient<Channel>,
       request_tx: mpsc::Sender<DurableTaskRequest>,
       pending_acks: Arc<Mutex<HashMap<AckKey, oneshot::Sender<DurableTaskEventAck>>>>,
       pending_callbacks: Arc<Mutex<HashMap<CallbackKey, oneshot::Sender<EntryResult>>>>,
   }
   ```

2. Implement the bidirectional stream loop:
   ```rust
   impl DurableListenerClient {
       pub async fn start(&self, worker_id: String) -> Result<()> {
           // Open bidi stream: v1_dispatcher.durable_task()
           // Send RegisterWorker as first message
           // Spawn receive loop that dispatches responses to pending_acks/pending_callbacks
       }

       pub async fn send_wait_for(
           &self, task_id: &str, invocation_count: u32,
           conditions: DurableEventListenerConditions
       ) -> Result<WaitForAck> { /* ... */ }

       pub async fn send_trigger_runs(
           &self, task_id: &str, invocation_count: u32,
           trigger_opts: Vec<TriggerWorkflowRequest>
       ) -> Result<RunAck> { /* ... */ }

       pub async fn send_memo(
           &self, task_id: &str, invocation_count: u32,
           key: Vec<u8>
       ) -> Result<MemoAck> { /* ... */ }

       pub async fn wait_for_callback(
           &self, task_id: &str, invocation_count: u32,
           branch_id: i32, node_id: i32
       ) -> Result<EntryResult> { /* ... */ }
   }
   ```

3. **Wire into DurableContext:** Replace `InMemoryDurableContext` usage in the real worker with a `GrpcDurableContext` that uses `DurableListenerClient`:
   - `sleepFor()` → renders conditions → `send_wait_for()` → `wait_for_callback()`
   - `waitFor()` → same pattern
   - `now()` → `send_memo()` → if cached, return; else compute + `send_memo_completed()`
   - `memo()` → same pattern as `now()`
   - `spawnChild()` → `send_trigger_runs()` → `wait_for_callback()` per child

4. Handle reconnection: The TS client auto-reconnects on stream disconnect with a 3-second backoff.

5. Handle status polling: Periodically send `workerStatus` requests listing all pending callbacks so the server can re-deliver missed completions.

### Reference Files
- `sources/hatchet-main/sdks/typescript/src/clients/listeners/durable-listener/durable-listener-client.ts`
- `sources/hatchet-main/api-contracts/v1/dispatcher.proto` (the `DurableTask` bidi stream RPC)

---

## 6. RunListenerClient (Run Output Subscription)

**Goal:** Allow `WorkflowRunRef.output()` to block until a workflow run completes and return the result.

### What TS Does

The `RunListenerClient` uses the REST API (not gRPC) to poll or subscribe to workflow run results:
- `client.runs.get(runId)` → REST `GET /api/v1/tenants/{tenant}/workflow-runs/{id}`
- The `WorkflowRunRef.output` getter polls until the run completes

### Rust Implementation

1. Add `reqwest` to dependencies for REST API calls.
2. Implement `RunControl` trait methods with real REST API calls:
   ```rust
   impl RunControl for GrpcTransport {
       async fn output_run(&self, run_id: &WorkflowRunId) -> Result<Value> {
           // Poll GET /api/v1/tenants/{tenant}/workflow-runs/{id}
           // until status is SUCCEEDED or FAILED
       }
       async fn cancel_run(&self, run_id: &WorkflowRunId) -> Result<()> {
           // POST cancel endpoint
       }
   }
   ```

---

## 7. Feature Client Backends

**Goal:** Feature clients (`runs`, `workflows`, `schedules`, `crons`, `events`, etc.) currently operate on in-memory state. They need REST API backends.

### What TS Does

All feature clients call `this.client.api.{endpoint}` (Axios REST calls). They **do not** use gRPC.

### Rust Implementation

1. Each feature client needs a `mode` enum: `InMemory` or `Rest(api_url, token)`.
2. The `Client::new()` constructor accepts a `HatchetConfig` and creates REST-backed feature clients.
3. Use `reqwest` for HTTP calls to the Hatchet REST API.
4. The REST API is documented at `sources/hatchet-main/sdks/typescript/src/clients/rest/generated/Api.ts`.

**Note:** This is lower priority than items 1-6. The feature clients are "nice to have" management tools. The core SDK functionality (declaring workflows, running tasks, receiving actions) works without them.

---

## 8. Wire Format Fixes

### 8a. `considerEventsSince` → ISO timestamp

**Current:** Rust uses `lookback_window: Option<Duration>` on `EventCondition`.
**Required:** The wire format is an ISO 8601 timestamp string (e.g. `"2026-04-20T12:00:00Z"`). The TS computes it as `new Date(now - lookbackMs).toISOString()`.

**Fix:** During `task_conditions_to_pb()`, convert `lookback_window` to an ISO timestamp string relative to the current time.

### 8b. Duration wire format

**Current:** Rust serializes `Duration` as millis (via `serde_duration_millis`).
**Required:** The engine expects duration strings like `"10s"`, `"5m"`, `"1h"` (see `sources/hatchet-main/sdks/typescript/src/v1/client/duration.ts`).

**Fix:** Add a `duration_to_string(d: Duration) -> String` helper that formats as `{hours}h{mins}m{secs}s` and use it in the proto conversion layer.

### 8c. RenderedConditions → Proto Conditions

**Current:** `RenderedConditions` serializes to JSON. 
**Required:** Must convert to protobuf `SleepMatchCondition`, `UserEventMatchCondition` types from `v1/shared/condition.proto`.

**Fix:** Add `impl From<RenderedConditions> for proto::DurableEventListenerConditions` (or similar converter).

---

## 9. Missing TaskConfig Fields

### 9a. `slotRequests`

**TS type:** `Record<SlotType, number>` where `SlotType = 'default' | 'durable'`

**Purpose:** Allows a task to request multiple worker slots (e.g., a task that does heavy work can request 2 slots so fewer tasks run concurrently on the same worker).

**Fix:** Add to `TaskConfig`:
```rust
pub slot_requests: Option<HashMap<SlotType, u32>>,
```

### 9b. `defaultFilters` on WorkflowDeclaration

**TS type:** `{ expression: string }[]`

**Purpose:** Default filter expressions applied to all event triggers.

**Fix:** Add to `WorkflowDeclaration`:
```rust
pub default_filters: Vec<FilterExpression>,
```

---

## 10. Health Server

**Goal:** Worker exposes an HTTP health endpoint for Kubernetes liveness/readiness probes.

### What TS Does (file: `health-server.ts`)

- HTTP server on configurable port (default 8080)
- `GET /health` → returns JSON with worker status, name, slots, actions, labels
- `GET /metrics` → optional Prometheus metrics (requires `prom-client` dep)

### Rust Implementation

1. Create `hatchet-sdk/src/health.rs`:
   ```rust
   pub struct HealthServer {
       port: u16,
       worker_name: String,
       // ... closures to get current status
   }
   ```
2. Use `axum` or `hyper` for the HTTP server (or just raw `tokio::net::TcpListener` for zero extra deps).
3. Start automatically when `Worker::start()` is called (configurable via `WorkerBuilder::health_port()`).
4. Support `GET /health` returning JSON. Prometheus metrics are optional/future.

---

## 11. Eviction Manager

**Goal:** Implement durable task eviction with grace periods.

### What TS Does (files: `eviction/eviction-manager.ts`, `eviction/eviction-cache.ts`)

- Tracks which durable tasks are actively computing vs. passively waiting (sleep, waitFor, child run)
- When the engine requests eviction (`serverEvict` on durable stream), applies a grace period:
  - If the task is passively waiting → immediate eviction via cancellation
  - If actively computing → waits for a configurable grace period before aborting
- Reports eviction status back to the engine

### Rust Implementation

1. Create `hatchet-sdk/src/eviction.rs`:
   ```rust
   pub struct EvictionManager {
       states: Arc<RwLock<HashMap<ActionKey, TaskState>>>,
   }

   enum TaskState {
       Active,
       Waiting { kind: String, resource_id: String },
   }
   ```
2. Wire into `DurableContext.withEvictionWait()` pattern — mark waiting before blocking calls, mark active after.
3. Handle `serverEvict` responses from `DurableListenerClient`.

**Note:** This is lower priority. The SDK is functional without eviction — it's an optimization for long-running durable tasks.

---

## 12. Cancel Supervision

**Goal:** Propagate cancellation from the engine to running task handlers.

### What TS Does

- The `Context` has an `AbortController` that is aborted when the engine sends a cancel signal
- `ctx.cancelled` → checks if the abort signal was triggered
- `ctx.throwIfCancelled()` → throws if cancelled
- Child workflows inherit the parent's abort signal

### Rust Implementation

1. Add `CancellationToken` from `tokio_util::sync` (or use `tokio::sync::watch`):
   ```rust
   pub struct InMemoryContext<T: Transport> {
       // existing fields...
       cancel_token: CancellationToken,
   }
   ```
2. When the engine sends a cancel action via the dispatcher stream, trigger the token.
3. Add `ctx.is_cancelled() -> bool` and `ctx.cancelled() -> WaitForCancellation` methods.
4. Child workflow runs should check cancellation before starting.

---

## 13. Retry Logic + Retrier

**Goal:** gRPC calls should retry on transient failures.

### What TS Does

Uses a `retrier()` utility that wraps gRPC calls with exponential backoff and retry logic.

### Rust Implementation

Add a generic retry wrapper:
```rust
pub async fn retry<F, Fut, T>(f: F, max_retries: u32) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: Future<Output = Result<T>>,
{
    // Exponential backoff with jitter
}
```

Apply to all gRPC calls in `GrpcTransport`.

---

## 14. Integration Test Suite

**Goal:** End-to-end tests against a real Hatchet engine.

### Steps

1. Add a `docker-compose.yml` that starts a Hatchet engine (use the official `ghcr.io/hatchet-dev/hatchet/hatchet-engine` image).
2. Create integration tests behind a `#[cfg(feature = "integration")]` flag.
3. Test:
   - Workflow registration → trigger → wait for result
   - Child workflow spawning
   - Durable sleep + wake
   - Durable event wait + event push
   - Worker label matching
   - Cancellation
   - Retry behavior

---

## Dependency Summary

Add these to `hatchet-sdk/Cargo.toml`:

```toml
[dependencies]
# Existing deps stay...
tonic = { version = "0.12", features = ["tls", "tls-roots"] }
prost = "0.13"
prost-types = "0.13"
reqwest = { version = "0.12", features = ["json"], optional = true }
base64 = "0.22"       # JWT parsing
sha2 = "0.10"          # Memo key computation
tokio-util = "0.7"     # CancellationToken

[build-dependencies]
tonic-build = "0.12"

[features]
default = ["grpc"]
grpc = ["dep:reqwest"]
integration = []
```

---

## Implementation Priority Order

| Priority | Item | Estimate | Depends On |
|---|---|---|---|
| **P0** | 1. Proto codegen | Small | — |
| **P0** | 2. Config + TLS | Small | 1 |
| **P0** | 3. gRPC Transport | Large | 1, 2 |
| **P0** | 4. Worker registration + action listener | Large | 3 |
| **P0** | 5. DurableListenerClient | Large | 3, 4 |
| **P0** | 6. RunListenerClient | Medium | 3 |
| **P1** | 8. Wire format fixes | Small | 1 |
| **P1** | 12. Cancel supervision | Medium | 4 |
| **P1** | 13. Retry logic | Small | 3 |
| **P2** | 7. Feature client backends | Medium | 2 |
| **P2** | 9. Missing TaskConfig fields | Small | — |
| **P2** | 10. Health server | Small | 4 |
| **P2** | 11. Eviction manager | Medium | 5 |
| **P3** | 14. Integration tests | Medium | All above |

---

## File Structure After Completion

```
crates/hatchet-sdk/
├── build.rs                    # NEW — proto codegen
├── Cargo.toml                  # MODIFIED — new deps
└── src/
    ├── lib.rs                  # MODIFIED — new module declarations
    ├── config.rs               # NEW — HatchetConfig, env parsing, TLS
    ├── proto.rs                # NEW — generated proto re-exports
    ├── grpc_transport.rs       # NEW — GrpcTransport impl Transport
    ├── durable_listener.rs     # NEW — DurableListenerClient (bidi stream)
    ├── health.rs               # NEW — HTTP health server
    ├── eviction.rs             # NEW — DurableEvictionManager
    ├── retry.rs                # NEW — retry/backoff utility
    ├── client.rs               # MODIFIED — Client::from_env() constructor
    ├── transport.rs            # MODIFIED — InMemoryTransport stays, add from_config
    ├── context.rs              # MODIFIED — cancel token integration
    ├── durable.rs              # MODIFIED — wire format conversions
    ├── worker.rs               # MODIFIED — gRPC action stream handler
    ├── workflow.rs             # MODIFIED — slotRequests, defaultFilters
    └── task.rs                 # MODIFIED — slotRequests field
```

---

## What NOT To Do

1. **Do NOT rewrite the existing in-memory infrastructure.** It works perfectly for testing and must continue to work. The `GrpcTransport` is an alternative `Transport` impl, not a replacement for `InMemoryTransport`.

2. **Do NOT add middleware/plugin systems.** The TS SDK has `withMiddleware()` for global pre/post hooks — this is a TypeScript-specific pattern that doesn't map well to Rust. Skip it.

3. **Do NOT implement legacy v0 compatibility.** The TS SDK has extensive backward-compat code for v0 engines. The Rust SDK targets v1 only.

4. **Do NOT implement `PooledDurableListenerClient`.** The TS has a pooled version for shared durable listeners. Start with a single client per worker.
