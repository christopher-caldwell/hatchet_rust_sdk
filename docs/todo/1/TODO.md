# Hatchet Rust SDK Parity TODO (Execution-Ready)

This is the authoritative backlog to bring this Rust SDK to functional parity with Hatchet TS v1 while staying Rust-native.

Use this file as a handoff contract: an agent should be able to implement this top-to-bottom and close parity gaps without additional discovery.

---

## Target Outcome

After completion, the Rust SDK must allow users to do the same things as TS v1 in production:

1. Declare workflows/tasks/durable tasks with equivalent semantics.
2. Run/schedule/cron/bulk-run workflows with equivalent run options.
3. Run workers against real Hatchet engine, including cancellation and durable flows.
4. Use context and durable context methods with real backend behavior.
5. Use feature clients (`runs`, `workflows`, `workers`, etc.) against real API.
6. Keep Rust-first design: typed APIs, `Result` errors, trait-based transport, no JS-only runtime patterns.

---

## Ground Rules For Implementation

1. Keep `InMemoryTransport` and current tests working.
2. Add parity through additive changes and gRPC/REST-backed paths.
3. Preserve Rust API ergonomics even when adding TS-equivalent capability.
4. Do not remove existing public items unless replaced with compatible aliases.

---

## Current Gaps (Confirmed)

These are real, present deficiencies in current code:

1. Workflow registration over gRPC drops task fields: rate limits, worker labels, concurrency, slot requests.
2. `stream_run()` and `list_logs()` in gRPC transport are stubbed (empty).
3. Worker action loop ignores cancel actions.
4. `Context::refresh_timeout()` and `Context::release_slot()` are no-ops.
5. Durable listener exists but is not integrated into durable runtime path.
6. Durable eviction path is not integrated end-to-end.
7. Feature clients are mostly in-memory state, not real REST-backed implementations.
8. Workflow handle lacks TS-equivalent run helpers (`run_many`, schedule/delay/cron convenience).
9. Run options lack full TS desired worker label fidelity.
10. Health server module exists but is not integrated with worker lifecycle.

---

## TS to Rust Mapping Index

Use this index when implementing each task.

### Client Surface

- TS: `src/v1/client/client.ts`
  - `runNoWait`, `run`, `runManyNoWait`, `runRef`
  - feature getters (`runs`, `workflows`, `workers`, etc.)
  - `worker(name, options)`
- Rust:
  - `crates/hatchet-sdk/src/client.rs`
  - `crates/hatchet-sdk/src/clients.rs`
  - `crates/hatchet-sdk/src/worker.rs`

### Workflow Declaration + Task Options

- TS: `src/v1/declaration.ts`, `src/v1/task.ts`
  - `waitFor`, `cancelIf`, `skipIf`, `slotRequests`, concurrency arrays
  - `onFailure`, `onSuccess`, `inputValidator`, `defaultFilters`
- Rust:
  - `crates/hatchet-sdk/src/workflow.rs`
  - `crates/hatchet-sdk/src/task.rs`
  - `crates/hatchet-sdk/src/transport.rs`
  - `crates/hatchet-sdk/src/grpc_transport.rs`

### Context + Durable Context

- TS: `src/v1/client/worker/context.ts`
  - `cancel`, `refreshTimeout`, `releaseSlot`, `putStream`
  - `waitFor`, `waitForEvent`, `sleepFor`, `sleepUntil`, `now`, durable `memo`, `spawnChild(ren)`
- Rust:
  - `crates/hatchet-sdk/src/context.rs`
  - `crates/hatchet-sdk/src/durable.rs`
  - `crates/hatchet-sdk/src/durable_listener.rs`
  - `crates/hatchet-sdk/src/eviction.rs`

### Worker Runtime

- TS: `src/v1/client/worker/worker.ts`, `worker-internal.ts`
  - register/listen loop, cancel supervision, health server, eviction integration
- Rust:
  - `crates/hatchet-sdk/src/worker.rs`
  - `crates/hatchet-sdk/src/grpc_transport.rs`
  - `crates/hatchet-sdk/src/health.rs`

### Feature Clients

- TS: `src/v1/client/features/*.ts`
- Rust:
  - `crates/hatchet-sdk/src/clients.rs`

---

## Execution Plan (Do In Order)

## P0 - Transport + Worker Correctness (Blocking)

### [x] P0.1 - Complete gRPC workflow registration payload parity

TS mapping:
- `worker-internal.ts` registration payload build (`putWorkflow` call).
- Includes per-task `rateLimits`, `workerLabels`, `backoff`, `concurrency`, `slotRequests`, `conditions`, `isDurable`.

Rust files:
- `crates/hatchet-sdk/src/grpc_transport.rs`
- `crates/hatchet-sdk/src/transport.rs`
- `crates/hatchet-sdk/src/task.rs`
- `crates/hatchet-sdk/src/workflow.rs`

Tasks:
1. Fill `rendered_to_proto_task()` missing mappings:
   - `rate_limits`
   - `worker_labels`
   - `concurrency`
   - `slot_requests`
   - `backoff_factor` / `backoff_max_seconds`
2. Ensure `WorkflowRegistration::from_declaration()` carries enough data for conversion.
3. Add tests that assert proto payload includes these fields, not defaults.

Done when:
1. No TODO placeholders remain in task proto conversion.
2. New tests fail on regression if any field is dropped.

---

### [x] P0.2 - Add real run stream and log retrieval paths

TS mapping:
- `util/workflow-run-ref.ts` uses listener-backed result/stream.
- `features/logs.ts` uses API log retrieval.

Rust files:
- `crates/hatchet-sdk/src/grpc_transport.rs`
- `crates/hatchet-sdk/src/clients.rs`

Tasks:
1. Implement `GrpcTransport::stream_run()` with real source (listener or REST).
2. Implement `GrpcTransport::list_logs()` with real backend call.
3. Ensure `LogsClient::list()` returns real records in grpc mode.

Done when:
1. `WorkflowRunRef::stream()` returns non-empty events for streamed tasks.
2. `logs().list()` returns engine logs for a real run.

---

### [x] P0.3 - Handle cancel actions in worker action loop

TS mapping:
- `worker-internal.ts::handleCancelStepRun()`

Rust files:
- `crates/hatchet-sdk/src/grpc_transport.rs`
- `crates/hatchet-sdk/src/worker.rs`
- `crates/hatchet-sdk/src/context.rs`

Tasks:
1. In `handle_assigned_action`, add handling for cancel action type.
2. Track running task contexts by task-run id and trigger cancellation token on cancel.
3. Ensure task completion after cancellation is reported as cancelled/failed appropriately.

Done when:
1. Engine-issued cancel stops long-running task code path.
2. Cancellation test proves no completion event is emitted after cancel unless expected.

---

### [x] P0.4 - Implement `refresh_timeout` and `release_slot` end-to-end

TS mapping:
- `context.ts::refreshTimeout()`, `releaseSlot()`

Rust files:
- `crates/hatchet-sdk/src/transport.rs`
- `crates/hatchet-sdk/src/context.rs`
- `crates/hatchet-sdk/src/grpc_transport.rs`

Tasks:
1. Extend `Transport` trait with:
   - `refresh_timeout(task_run_id, increment)`
   - `release_slot(task_run_id)`
2. Call these from context methods (remove no-op behavior).
3. Implement no-op fallback in `InMemoryTransport` only.
4. Implement gRPC dispatcher RPC calls in `GrpcTransport`.

Done when:
1. Context methods affect real worker scheduling behavior.
2. In-memory tests remain green.

---

### [x] P0.5 - Correct worker action identity mapping

TS mapping:
- `worker-internal.ts` uses action/task identifiers consistently from dispatch payload.

Rust files:
- `crates/hatchet-sdk/src/grpc_transport.rs`
- `crates/hatchet-sdk/src/worker.rs`

Tasks:
1. Validate current `action_id`, `task_name`, and registry key usage.
2. Fix dispatch lookup to always use the registered action key expected by engine.
3. Add a regression test for action-id mismatch.

Done when:
1. Worker can run both simple and namespaced task actions without key mismatch failures.

---

## P1 - Durable Runtime Parity (Blocking for durable production)

### [x] P1.1 - Integrate `DurableListenerClient` into runtime durable context

TS mapping:
- `context.ts::DurableContext.waitFor/sleepFor/waitForEvent/spawnChildren/memo/now`
- `durable-listener-client.ts`

Rust files:
- `crates/hatchet-sdk/src/durable.rs`
- `crates/hatchet-sdk/src/durable_listener.rs`
- `crates/hatchet-sdk/src/worker.rs`
- `crates/hatchet-sdk/src/grpc_transport.rs`

Tasks:
1. Introduce engine-backed durable context type (separate from in-memory durable context).
2. Route durable worker execution to engine-backed context when transport is gRPC.
3. Implement durable operations via durable stream request/ack/callback flow.

Done when:
1. Durable task `wait_for` and `spawn_children` survive worker restart semantics via engine.

---

### [x] P1.2 - Durable listener reconnection + callback durability

TS mapping:
- `durable-listener-client.ts` reconnect and deferred callback behavior.

Rust files:
- `crates/hatchet-sdk/src/durable_listener.rs`

Tasks:
1. Implement reconnect with backoff when stream closes/errors.
2. Rehydrate pending ack/callback waiters across reconnect boundaries.
3. Add timeout/error semantics equivalent to TS behavior.

Done when:
1. Stream interruption does not permanently orphan in-flight durable waits.

---

### [x] P1.3 - Wire `serverEvict` into eviction manager

TS mapping:
- `worker-internal.ts` + eviction manager integration.

Rust files:
- `crates/hatchet-sdk/src/durable_listener.rs`
- `crates/hatchet-sdk/src/eviction.rs`
- `crates/hatchet-sdk/src/worker.rs`
- `crates/hatchet-sdk/src/durable.rs`

Tasks:
1. Forward server eviction notices from durable listener to eviction manager.
2. Track active vs waiting durable states and apply correct eviction behavior.
3. Emit local cancellation and engine ack/report as needed.

Done when:
1. Eviction paths are deterministic and covered by tests.

---

### [x] P1.4 - Parent condition engine-backed durable resolution

TS mapping:
- durable `waitFor` supports parent-based orchestration via engine condition handling.

Rust files:
- `crates/hatchet-sdk/src/durable.rs`

Tasks:
1. Replace current "parent conditions unsupported" runtime behavior with engine-backed wait flow.
2. Ensure parent condition expressions and readable-data-key behavior match task condition renderer.

Done when:
1. Durable parent conditions do not error in gRPC mode.

---

## P2 - API Parity (User-facing capability parity)

### [x] P2.1 - Expand `RunOpts` worker-label fidelity

TS mapping:
- `declaration.ts::RunOpts.desiredWorkerLabels`
- `admin.ts::convertDesiredWorkerLabels`

Rust files:
- `crates/hatchet-core/src/run.rs`
- `crates/hatchet-sdk/src/grpc_transport.rs`

Tasks:
1. Replace flat `HashMap<String, String>` desired worker labels in `RunOpts` with rich config:
   - value
   - required
   - weight
   - comparator
2. Preserve backward compatibility if possible (deserialize old format).
3. Map fields through trigger RPC conversion.

Done when:
1. Run trigger can route with full desired label semantics.

---

### [x] P2.2 - Add workflow handle parity helpers

TS mapping:
- `BaseWorkflowDeclaration.runManyNoWait`, `runMany`, `schedule`, `delay`, `cron`

Rust files:
- `crates/hatchet-sdk/src/client.rs`
- `crates/hatchet-sdk/src/clients.rs`

Tasks:
1. Add `WorkflowHandle::run_many(...)` (wait for all outputs).
2. Add convenience APIs for schedule/delay/cron at workflow handle level.
3. Keep existing feature-client methods; this is ergonomic parity.

Done when:
1. Users can execute common TS workflow patterns from the workflow handle itself.

---

### [x] P2.3 - Support durable steps in workflow declaration

TS mapping:
- `WorkflowDeclaration.durableTask(...)` and mixed `_tasks/_durableTasks`.

Rust files:
- `crates/hatchet-sdk/src/workflow.rs`
- `crates/hatchet-sdk/src/transport.rs`
- `crates/hatchet-sdk/src/client.rs`

Tasks:
1. Add APIs to place `DurableTaskHandle` steps in workflow DAG.
2. Carry durable marker into registration and execution.
3. Ensure `is_durable` registration is true for durable steps.

Done when:
1. Mixed durable/non-durable workflow DAG is declarable and executes correctly.

---

### [x] P2.4 - Workflow-level input schema parity

TS mapping:
- `CreateBaseWorkflowOpts.inputValidator` converted to JSON schema in registration.

Rust files:
- `crates/hatchet-sdk/src/workflow.rs`
- `crates/hatchet-sdk/src/grpc_transport.rs`

Tasks:
1. Add optional workflow input schema field on declaration.
2. Serialize to `input_json_schema` on registration.

Done when:
1. Dashboard/backend receives workflow input schema in gRPC mode.

---

### [x] P2.5 - Lifecycle hook registration parity (`on_failure`, `on_success`)

TS mapping:
- `worker-internal.ts` creates `onFailureTask` and appends synthetic on-success task with leaf parents.

Rust files:
- `crates/hatchet-sdk/src/workflow.rs`
- `crates/hatchet-sdk/src/grpc_transport.rs`
- `crates/hatchet-sdk/src/transport.rs`

Tasks:
1. Map `on_failure` into `CreateWorkflowVersionRequest.on_failure_task`.
2. Define Rust strategy for `on_success` equivalent:
   - synthetic terminal step with all DAG leaves as parents.
3. Ensure serialization and registration parity with task config defaults.

Done when:
1. Failure and success handlers are actually executed by engine, not just stored locally.

---

## P3 - Feature Clients Parity (Management API parity)

### [x] P3.1 - Introduce real REST-backed feature client mode

TS mapping:
- `src/v1/client/features/*.ts` all use REST API client.

Rust files:
- `crates/hatchet-sdk/src/clients.rs`
- `crates/hatchet-sdk/src/client.rs`
- new REST client module(s)

Tasks:
1. Add backend enum per feature client: `InMemory` vs `Rest`.
2. Build shared authenticated REST client.
3. Keep in-memory mode for tests.

Done when:
1. Feature client operations reflect live engine state in grpc/rest mode.

---

### [x] P3.2 - Runs client parity improvements

TS mapping:
- `features/runs.ts`
  - filter-based `list/cancel/replay`
  - `restoreTask`, `branchDurableTask`, `getTaskExternalId`
  - streaming subscription utility

Rust files:
- `crates/hatchet-sdk/src/clients.rs`

Tasks:
1. Add filter-based run operations in addition to direct-id methods.
2. Align durable run helpers with TS semantics.
3. Align task external id lookup behavior.

Done when:
1. Runs client supports equivalent operational workflows as TS.

---

### [x] P3.3 - Workers feature parity (`isPaused/pause/unpause`)

TS mapping:
- `features/workers.ts`

Rust files:
- `crates/hatchet-sdk/src/clients.rs`
- `crates/hatchet-sdk/src/worker.rs`

Tasks:
1. Add workers feature methods for paused status and state changes against real API.
2. Keep local pause/unpause on worker handle, but also expose remote control parity.

Done when:
1. `client.workers()` supports the same pause lifecycle operations as TS.

---

### [x] P3.4 - Logs, schedules, crons, metrics, filters, webhooks parity audit and close

TS mapping:
- `features/logs.ts`, `schedules.ts`, `crons.ts`, `metrics.ts`, `filters.ts`, `webhooks.ts`

Rust files:
- `crates/hatchet-sdk/src/clients.rs`

Tasks:
1. Ensure each Rust feature client method corresponds to TS operation semantics and arguments.
2. Fill any missing methods or behavioral mismatches.
3. Add parity tests for each feature client.

Done when:
1. Method-level parity matrix is complete with tests.

---

## P4 - Worker Runtime Quality

### P4.1 - Integrate health server into worker lifecycle

TS mapping:
- `worker-internal.ts` health server start/stop and state reporting.

Rust files:
- `crates/hatchet-sdk/src/worker.rs`
- `crates/hatchet-sdk/src/health.rs`

Tasks:
1. Add optional health server startup from `WorkerBuilder`.
2. Update health status on worker state transitions.
3. Stop health server on shutdown.

Done when:
1. `/health` reflects live worker state for running worker.

---

### P4.2 - Add `wait_until_ready` style worker readiness helper

TS mapping:
- `worker.ts::waitUntilReady()`

Rust files:
- `crates/hatchet-sdk/src/worker.rs`

Tasks:
1. Add readiness API that waits for successful registration/listen loop startup.
2. Use timeout and return typed error.

Done when:
1. Caller can reliably block until worker is ready without sleeps.

---

### P4.3 - Cancellation supervision and diagnostics

TS mapping:
- `worker-internal.ts` cancellation warning/grace diagnostics.

Rust files:
- `crates/hatchet-sdk/src/worker.rs`

Tasks:
1. Add cancellation grace monitoring for non-cooperative task code.
2. Emit structured warnings when tasks ignore cancellation.

Done when:
1. Cancellation failure modes are observable and test-covered.

---

## P5 - Reliability + Error Semantics

### [x] P5.1 - Retry policy coverage for all transient gRPC calls

TS mapping:
- `util/retrier.ts` usage throughout admin/worker/listeners.

Rust files:
- `crates/hatchet-sdk/src/retry.rs`
- `crates/hatchet-sdk/src/grpc_transport.rs`
- `crates/hatchet-sdk/src/durable_listener.rs`

Tasks:
1. Ensure all network calls use retry policy where safe.
2. Define explicit non-retryable statuses.
3. Add jitter and max elapsed policy controls.

Done when:
1. Retry behavior is consistent and configurable.

---

### [x] P5.2 - Standardize error mapping and context

TS mapping:
- TS throws domain errors (`HatchetError`, cancellation/dedupe errors).

Rust files:
- `crates/hatchet-sdk/src/error.rs`
- all callsites

Tasks:
1. Normalize transport/domain errors with context-rich messages.
2. Add error variants for durable callback timeout, listener disconnect, unsupported operation mode.

Done when:
1. Errors are actionable and non-ambiguous in production logs.

---

## P6 - Integration Tests + CI Gates

### P6.1 - Real engine integration suite

Tasks:
1. Add dockerized test harness for Hatchet engine.
2. Add integration tests for:
   - workflow register + run + output
   - child runs
   - cancellation
   - durable wait/memo/spawn
   - stream/log retrieval
   - feature clients REST operations

Done when:
1. `cargo test --workspace --features integration` passes locally and in CI.

---

### P6.2 - Parity contract tests (TS-equivalent behavior)

Tasks:
1. Add a parity matrix test file keyed by TS function names.
2. Each parity row has at least one test case.
3. Fail CI if any row is unimplemented or `#[ignore]` without issue reference.

Done when:
1. Parity status is machine-checkable.

---

## API and Data Model Changes Required

These changes are expected and should be implemented intentionally:

1. `RunOpts.desired_worker_labels` type upgrade in `hatchet-core`.
2. `Transport` trait expansion for timeout/slot control (and any required lifecycle hooks).
3. Workflow declaration support for durable DAG steps.
4. Optional workflow input schema field.
5. Worker readiness and health APIs.

---

## Suggested Milestones

### Milestone A (Production non-durable baseline)

Includes:
1. P0.1, P0.2, P0.3, P0.4, P0.5
2. P2.1
3. P4.1
4. P6.1 non-durable subset

Exit criteria:
1. Non-durable workflows run reliably on real engine.
2. Cancellation and stream/log behavior are real, not stubs.

### Milestone B (Durable production baseline)

Includes:
1. P1.1, P1.2, P1.3, P1.4
2. P4.3
3. P6.1 durable subset

Exit criteria:
1. Durable operations are engine-backed and restart-safe.

### Milestone C (Full feature/client parity)

Includes:
1. P2.2, P2.3, P2.4, P2.5
2. P3.1, P3.2, P3.3, P3.4
3. P5.1, P5.2
4. P6.2

Exit criteria:
1. SDK capabilities are functionally equivalent to TS v1 for supported surfaces.

---

## Quick "Go" Prompt For Implementation Agent

Use this prompt to execute this file directly:

1. Implement tasks in order from P0 through P6.
2. For each completed task:
   - update this file with `[x]` marker beside the task id,
   - add tests proving behavior,
   - run full test suite.
3. Do not skip failing tests; fix root causes.
4. Preserve in-memory mode.
5. Keep changes Rust-idiomatic and type-safe.
