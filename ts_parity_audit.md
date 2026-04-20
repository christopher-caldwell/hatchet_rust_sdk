# TS → Rust SDK Parity Audit

This audit compares the Rust SDK (`crates/hatchet-sdk/src/`) against the canonical TypeScript source (`sources/hatchet-main/sdks/typescript/src/`) module by module. Each area is rated on **functional parity** (does it do the same thing?) and **Rust idiom** (does it do it the Rust way?).

> [!NOTE]
> This audit is against the *current* state of the TS `v1` SDK surface. Legacy/deprecated APIs (v0, `stepOutput`, `spawnWorkflow` old signatures) are intentionally excluded.

---

## Summary Matrix

| Module | Functional Parity | Rust Idiom | Notes |
|---|---|---|---|
| **Conditions model** | ✅ High | ✅ Excellent | All 4 condition types, OR groups, rendering |
| **Workflow declarations** | ✅ High | ✅ Excellent | `waitFor`/`cancelIf`/`skipIf` wired in |
| **Task configuration** | ✅ High | ✅ Good | All TS fields present |
| **Context (non-durable)** | ⚠️ Moderate | ✅ Good | Missing some runtime fields |
| **Durable context** | ⚠️ Moderate | ✅ Good | Model done, runtime plumbing absent |
| **Client surface** | ⚠️ Moderate | ✅ Excellent | Handles + feature clients present |
| **Worker runtime** | ⚠️ Moderate | ✅ Good | Dispatch works, some gaps |
| **Transport / registration** | 🔴 Low | ✅ Good | Trait-based, but conditions not wired to proto |
| **Feature clients** | ✅ High | ✅ Excellent | All 12 TS feature clients exist |

---

## Module-by-Module Analysis

### 1. Conditions Model

**TS source:** `v1/conditions/` (7 files)
**Rust:** `durable.rs` condition types + `workflow.rs` declaration fields

#### What's at parity

| TS Concept | Rust Equivalent | Status |
|---|---|---|
| `SleepCondition` class | `Condition::SleepFor(SleepCondition)` | ✅ |
| `SleepUntil` (via `sleepUntil()`) | `Condition::SleepUntil(SleepUntilCondition)` | ✅ (Rust extension) |
| `UserEventCondition` class | `Condition::Event(EventCondition)` | ✅ |
| `ParentCondition` class | `Condition::Parent(ParentCondition)` | ✅ |
| `OrCondition` wrapper | `Conditions::Or(Vec<Condition>)` | ✅ |
| `Render(action, conditions)` | `render_conditions(action, conditions)` | ✅ |
| `Or(...)` factory | `or(conditions)` | ✅ |
| `conditionsToPb(...)` | `conditions_to_rendered(...)` | ✅ (model only) |
| `taskConditionsToPb(...)` | Not yet (needs transport layer) | ⚠️ |
| `BaseCondition` fields | `BaseMatchCondition` | ✅ |
| `readableDataKey` defaults | Matches TS defaults exactly | ✅ |
| `orGroupId` auto-generation | UUID-based generation | ✅ |
| Serialize/Deserialize | All types serializable | ✅ |

#### Rust idiom assessment

**Excellent.** The Rust condition model uses:
- Enums with named variants instead of class hierarchies (idiomatic)
- Builder pattern for `EventConditionBuilder`, `ParentConditionBuilder` (idiomatic)
- `impl From<Condition> for Conditions` for ergonomic conversion
- `PartialEq` derives for testability
- Custom serde helpers for `Duration`/`SystemTime` (necessary, well-encapsulated)

> [!TIP]
> The TS `sleepFor` accepts a `Duration` string (`"5s"`, `"1m"`), while Rust uses `std::time::Duration`. This is the correct Rust-idiomatic choice — duration parsing belongs at the API boundary, not in the core model.

#### What's missing

- **`considerEventsSince`**: TS `UserEventCondition` has `considerEventsSince: string` (ISO timestamp). Rust uses `lookback_window: Option<Duration>` which is functionally equivalent but not wire-compatible. The TS transport converts this to a `Date` at registration time.
- **`taskConditionsToPb` equivalent**: The Rust `conditions_to_rendered` produces protocol-shaped structs, but these aren't yet consumed by any actual transport/registration code. The TS calls `taskConditionsToPb(task, namespace)` during `putWorkflow`.

---

### 2. Workflow Declarations

**TS source:** `v1/declaration.ts` (1306 lines)
**Rust:** `workflow.rs` (199 lines)

#### What's at parity

| TS Concept | Rust Equivalent | Status |
|---|---|---|
| `WorkflowDeclaration` class | `WorkflowDeclaration<I, O>` struct | ✅ |
| `definition.name` | `name: String` | ✅ |
| `onEvents`, `onCrons` | `on_events`, `on_crons` | ✅ |
| `concurrency` | `concurrency: Option<ConcurrencyConfig>` | ✅ |
| `defaultPriority` | `default_priority: Option<Priority>` | ✅ |
| `sticky` | `sticky: Option<StickyStrategy>` | ✅ |
| `taskDefaults` | `task_defaults: TaskConfig` | ✅ |
| `_tasks` array | `steps: Vec<WorkflowStep>` | ✅ |
| Task `parents` | `WorkflowStep.parents: Vec<String>` | ✅ |
| Task `waitFor` | `WorkflowStep.wait_for: Vec<Conditions>` | ✅ |
| Task `cancelIf` | `WorkflowStep.cancel_if: Vec<Conditions>` | ✅ |
| Task `skipIf` | `WorkflowStep.skip_if: Vec<Conditions>` | ✅ |
| `StepConditions` builder | `StepConditions::new().wait_for(...).cancel_if(...)` | ✅ |

#### What's NOT at parity (and shouldn't be)

| TS Concept | Why Excluded | 
|---|---|
| `BaseWorkflowDeclaration.run()` | In Rust, execution lives on `WorkflowHandle`, not `WorkflowDeclaration`. This is correct — declarations are data, handles are execution. |
| `BaseWorkflowDeclaration.schedule()` | Same — belongs on handles |
| `BaseWorkflowDeclaration.cron()` | Same |
| `TaskWorkflowDeclaration` | TS conflates "standalone task" and "workflow" into one type. Rust separates `TaskHandle` and `WorkflowHandle`. Better separation of concerns. |
| `onFailure` / `onSuccess` handlers | Not yet present. These are workflow-level lifecycle hooks. |
| `_durableTasks` separate array | Rust tracks durable-ness via `DurableTaskHandle` at the type level, not a separate runtime array. Better. |
| `description`, `version` | Not present. Low priority metadata fields. |
| `inputValidator` (Zod) | No Rust equivalent. Zod is TS-specific. |
| `defaultFilters` | Not present. |
| Middleware (`before`/`after`) | Not applicable — Rust has no dynamic middleware model. Comparable behavior achieved through trait composition. |

#### Rust idiom assessment

**Excellent.** Key decisions:
- `WorkflowDeclaration` is a pure data struct with `Serialize/Deserialize` — declarations are values, not classes with behavior.
- Execution methods live on `WorkflowHandle<T, I, O>` with proper lifetime management via `Arc<T>`.
- `step_with()` / `step_after_with()` use the builder pattern with `StepConditions`, avoiding TS's overloaded `waitFor?: Conditions | Conditions[]` union type in favor of explicit `.wait_for(...)` calls.
- PhantomData for type-safe I/O without runtime cost.

---

### 3. Task Configuration

**TS source:** `v1/task.ts` (271 lines)
**Rust:** `task.rs` (341 lines)

| TS Field | Rust Field | Status |
|---|---|---|
| `name` | Lives on `TaskHandle` | ✅ |
| `executionTimeout` | `execution_timeout: Option<Duration>` | ✅ |
| `scheduleTimeout` | `schedule_timeout: Option<Duration>` | ✅ |
| `retries` | `retries: Option<u32>` | ✅ |
| `backoff.factor` / `backoff.maxSeconds` | `backoff: Option<BackoffConfig>` | ✅ |
| `rateLimits` | `rate_limits: Vec<RateLimitConfig>` | ✅ |
| `desiredWorkerLabels` | `desired_worker_labels` | ✅ |
| `concurrency` | `concurrency: Option<Vec<ConcurrencyConfig>>` | ✅ |
| `slotRequests` | Not present | ⚠️ |
| `timeout` (deprecated) | Correctly excluded | ✅ |

**Rust idiom:** Good. Uses `Option<T>` for all optional fields, builder-style `with_*` methods, proper defaults.

---

### 4. Context

**TS source:** `v1/client/worker/context.ts` (1250 lines)
**Rust:** `context.rs` (various traits + `InMemoryContext`)

| TS Method | Rust Equivalent | Status |
|---|---|---|
| `input` property | `Context::input()` | ✅ |
| `workflowRunId()` | `ContextIds::workflow_run_id()` | ✅ |
| `taskRunExternalId()` | `ContextIds::task_run_id()` | ✅ |
| `taskName()` | `ContextIds::task_name()` | ✅ |
| `workflowName()` | `ContextIds::workflow_name()` | ✅ |
| `retryCount()` | `ContextIds::retry_count()` | ✅ |
| `parentOutput()` | `ContextExt::parent_output()` | ✅ |
| `runChild()` | `ContextExt::spawn_child()` | ✅ |
| `bulkRunChildren()` | `ContextExt::spawn_children()` | ✅ |
| `putStream()` | `ContextExt::put_stream()` | ✅ |
| `log()` / `logger` | `ContextExt::log()` | ✅ |
| `refreshTimeout()` | `ContextExt::refresh_timeout()` | ✅ |
| `releaseSlot()` | Not present | ⚠️ |
| `cancel()` | Not present | ⚠️ |
| `abortController` | Not applicable (Rust uses `CancellationToken`) | N/A |
| `additionalMetadata()` | `ContextIds::additional_metadata()` | ✅ |
| `childIndex()` / `childKey()` | `ContextIds::child_index()` / `child_key()` | ✅ |
| `triggers()` / `filterPayload()` | Not present | ⚠️ |
| `errors()` | Not present | ⚠️ |
| `priority()` | Not present | ⚠️ |
| `worker` wrapper | Not present (worker info via `ContextIds`) | Partial |

**Rust idiom:** Good. Trait-based context (`Context`, `ContextExt`, `ContextIds`) with `InMemoryContext` for testing. The trait split is more modular than TS's monolithic class.

---

### 5. Durable Context

**TS source:** `DurableContext` class in `context.ts` (lines 844–1242)
**Rust:** `durable.rs` (`DurableContext` trait, `InMemoryDurableContext`)

| TS Method | Rust Equivalent | Status |
|---|---|---|
| `sleepFor()` | `DurableContextExt::sleep_for()` | ✅ (in-memory) |
| `sleepUntil()` | `DurableContextExt::sleep_until()` | ✅ (in-memory) |
| `waitFor(conditions)` | `DurableContextExt::wait_for()` / `wait_for_any()` | ✅ (in-memory) |
| `waitForEvent()` | `DurableContextExt::wait_for_event()` | ✅ (in-memory) |
| `spawnChild()` | `DurableContextExt::spawn_child()` | ✅ |
| `spawnChildren()` | `DurableContextExt::spawn_children()` | ✅ |
| `now()` (memoized) | Not present | ⚠️ |
| `memo()` (durable memo) | Not present | ⚠️ |
| `SleepResult` | `SleepResult` | ✅ |
| `ConditionOutput` | `ConditionOutput` | ✅ |
| Eviction manager integration | Not present | ⚠️ |
| `DurableListenerClient` calls | Not present | 🔴 |

> [!IMPORTANT]
> The Rust durable context is fully functional for **in-memory testing** but does not yet connect to a real engine. The TS `DurableContext` communicates with a `DurableListenerClient` via gRPC streams. This is the single largest gap.

**Rust idiom:** Good. `async_trait` for async trait methods, `Arc<Mutex<>>` for shared state, `broadcast` channels for event fan-out. The in-memory design is clean and testable.

---

### 6. Client Surface

**TS source:** `v1/client/client.ts` (734 lines)
**Rust:** `client.rs` (411 lines)

| TS Method | Rust Equivalent | Status |
|---|---|---|
| `workflow()` | `Client::workflow()` → `WorkflowHandle` | ✅ |
| `task()` | `Client::task()` → `TaskHandle` | ✅ |
| `durableTask()` | `Client::durable_task()` → `DurableTaskHandle` | ✅ |
| `worker()` | `Client::worker()` → `WorkerBuilder` | ✅ |
| `run()` / `runAndWait()` | `WorkflowHandle::run()` | ✅ |
| `runNoWait()` | `WorkflowHandle::run_no_wait()` | ✅ |
| `runManyNoWait()` | `WorkflowHandle::run_many_no_wait()` | ✅ |
| Feature client accessors | All 12 present | ✅ |
| `runRef()` | Not present | ⚠️ |
| `cel` | `Client::cel()` | ✅ |
| `withMiddleware()` | Not applicable | N/A |
| Namespace handling | Not present | ⚠️ |

**Rust idiom:** Excellent. Generic `Client<T: Transport>` with Arc-based sharing. `WorkflowHandle` correctly separates declaration from execution. `RunControl` trait for cancellation/replay/streaming.

---

### 7. Worker Runtime

**TS source:** `v1/client/worker/` (worker.ts, worker-internal.ts, ~1400 lines total)
**Rust:** `worker.rs` (650+ lines)

| TS Concept | Rust Equivalent | Status |
|---|---|---|
| `Worker.create()` | `WorkerBuilder::build()` | ✅ |
| `Worker.start()` | `Worker::start()` | ✅ |
| `Worker.stop()` | `WorkerHandle::shutdown()` | ✅ |
| Action registry | Action dispatch map | ✅ |
| Workflow registration | `register_workflow()` via transport | ✅ |
| Health server | Not present | ⚠️ |
| Eviction manager | Not present | ⚠️ |
| `upsertLabels()` | Not present | ⚠️ |
| `isPaused()` / `pause()` / `unpause()` | Not present | ⚠️ |
| `waitUntilReady()` | Not present (Rust worker blocks on start) | ✅ (different pattern) |
| Signal handling (`SIGTERM`/`SIGINT`) | Via tokio signal in `Worker::start()` | ✅ |
| Durable action dispatch | Separate dispatch path | ✅ |
| Cancel supervision | Not present | ⚠️ |

**Rust idiom:** Good. Tokio task-based execution, `JoinHandle` for lifecycle management, proper shutdown coordination.

---

### 8. Transport / Registration

**TS source:** `v1/client/admin.ts` + `worker-internal.ts` registration code
**Rust:** `transport.rs` (`Transport` trait + `InMemoryTransport`)

| TS Concept | Rust Equivalent | Status |
|---|---|---|
| `putWorkflow()` gRPC call | `Transport::register_workflow_definition()` | ✅ (trait) |
| `triggerWorkflow()` gRPC call | `Transport::run_workflow()` | ✅ (trait) |
| `bulkTriggerWorkflow()` gRPC call | `Transport::run_workflows()` | ✅ (trait) |
| `taskConditionsToPb()` during registration | **Not wired** | 🔴 |
| Namespace application | Not present | ⚠️ |
| gRPC channel management | Not present (trait-based abstraction) | Design choice |
| Run listener / result streaming | `Transport::await_result()` / `stream_run()` | ✅ |
| Durable listener client | Not present | 🔴 |

> [!WARNING]
> The Transport trait is well designed but currently has no real implementation — only `InMemoryTransport`. The critical gap is that **condition declarations from `WorkflowStep.wait_for/cancel_if/skip_if` are not transformed into protobuf-compatible shapes and submitted during workflow registration**. In the TS SDK, this happens at line 403 of `worker-internal.ts`: `conditions: taskConditionsToPb(task, this.client.config.namespace)`.

---

### 9. Feature Clients

**TS source:** `v1/client/features/` (13 files)
**Rust:** `clients.rs` (all in one file)

| TS Client | Rust Client | Status |
|---|---|---|
| `RunsClient` | `RunsClient<T>` | ✅ |
| `WorkflowsClient` | `WorkflowsClient<T>` | ✅ |
| `ScheduleClient` | `SchedulesClient<T>` | ✅ |
| `CronClient` | `CronsClient<T>` | ✅ |
| `WorkersClient` | `WorkersClient<T>` | ✅ |
| `MetricsClient` | `MetricsClient<T>` | ✅ |
| `RatelimitsClient` | `RateLimitsClient<T>` | ✅ |
| `EventClient` | `EventsClient<T>` | ✅ |
| `LogsClient` | `LogsClient<T>` | ✅ |
| `FiltersClient` | `FiltersClient<T>` | ✅ |
| `WebhooksClient` | `WebhooksClient<T>` | ✅ |
| `CELClient` | `CelClient<T>` | ✅ |
| `TenantClient` | Not present | ⚠️ |

**Rust idiom:** Excellent. All feature clients are generic over `Transport`, use `Arc` for sharing, and follow the same pattern.

---

## Overall Rust Idiom Assessment

### What the Rust SDK does well

1. **Trait-based abstraction** — `Transport`, `Context`, `DurableContext` are all traits with in-memory test implementations. This is textbook Rust.
2. **Type-safe handles** — `TaskHandle<I, O>`, `DurableTaskHandle<I, O>`, `WorkflowHandle<T, I, O>` carry type information through the API without runtime cost.
3. **Builder patterns** — `WorkflowDeclaration::new().on_event().step_with()`, `TaskConfig::default().with_retries()`, `Condition::event("key").expression().build()` are all idiomatic.
4. **Enum-based conditions** — `Condition::SleepFor | SleepUntil | Event | Parent` is better than TS's class hierarchy for pattern matching and exhaustiveness.
5. **Separation of data and behavior** — Declarations are data (`Serialize/Deserialize`), execution is on handles, context is traits. TS conflates these.
6. **PhantomData for zero-cost type safety** — I/O types are tracked at compile time without runtime overhead.
7. **Error handling** — `Result<T>` throughout, no panics in the API surface.

### What could be improved (but isn't wrong)

1. **`durable.rs` is 1200+ lines** — Could benefit from splitting into `durable/mod.rs`, `durable/conditions.rs`, `durable/context.rs`, `durable/rendering.rs`. This is cosmetic.
2. **Serde helpers as inner modules** — The `serde_duration_millis` etc. modules work but could be a small utility crate or at least in a separate file.
3. **No `Display` implementations** — Conditions and related types don't impl `Display`, which would help with logging/debugging.

---

## Critical Remaining Gaps (Priority Order)

### 🔴 P0: Transport layer doesn't consume condition declarations

**What:** `WorkflowStep.wait_for/cancel_if/skip_if` conditions exist in the declaration model but are **not transformed and submitted** during workflow registration. The TS SDK calls `taskConditionsToPb(task, namespace)` at registration time (line 403 of `worker-internal.ts`).

**Impact:** Without this, conditions declared on steps are inert metadata — the engine never sees them.

**Fix:** Implement `task_conditions_to_pb()` equivalent that transforms declaration-level conditions into the registration payload, including namespace application for event keys.

### 🔴 P0: No real Transport implementation

**What:** Only `InMemoryTransport` exists. No gRPC/REST transport to a real Hatchet engine.

**Impact:** The SDK cannot connect to a real Hatchet deployment.

### 🔴 P0: No DurableListenerClient

**What:** TS `DurableContext.waitFor()` communicates with the engine via `DurableListenerClient.sendEvent()` and `waitForCallback()`. The Rust durable context is in-memory only.

**Impact:** Durable waits, child spawning, and memoization don't work against a real engine.

### ⚠️ P1: Missing runtime Context fields

`triggers()`, `filterPayload()`, `errors()`, `priority()`, `cancel()`, `releaseSlot()` are not on the Rust context traits.

### ⚠️ P1: Missing `now()` and `memo()` on DurableContext

These are important for deterministic replay in durable tasks.

### ⚠️ P1: No eviction / cancel supervision

TS has `DurableEvictionManager` and cancel supervision with grace periods. Rust has neither.

### ⚠️ P2: Worker lifecycle gaps

`isPaused()`, `pause()`, `unpause()`, `upsertLabels()`, health server, `waitUntilReady()`.

### ⚠️ P2: No namespace support

TS applies namespaces to workflow names and event keys throughout. Rust doesn't.

---

## Bottom Line

**The condition model and declaration layer are at high parity with excellent Rust idiom.** The work completed brings the SDK's *model* and *API surface* into alignment with the TS SDK's condition declaration capabilities.

**The remaining gap is entirely in the execution/transport plane:**
- The model knows what conditions to declare → but can't tell the engine about them
- The durable context knows how to wait → but only in memory, not via engine-backed listeners
- The worker knows how to dispatch → but doesn't register conditions during `putWorkflow`

This is a natural progression — model first, then transport — and the model layer is solid.
