# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added

- Async task middleware for normal/durable handlers, and optional/default-enabled
  OpenTelemetry 0.32 spans with W3C context propagation across child submissions.
- Workflow TTL/STATUS idempotency with engine capability guards, typed single/bulk
  collision errors and retained successful bulk IDs.
- Rate-limit pause-until requests and actual engine acknowledgment times.
- A versioned TS/Go feature matrix, registry/source snapshot, guarded refresh and
  drift checks, and pinned baseline/current engine CI definitions. Live coverage
  now includes 128-child durable fanout and a pending wait across engine restart.

- Engine release lookup and capability prerequisites; worker startup checks slot
  configuration and durable support before registering.
- Paginated run search with time, workflow, status, metadata, worker, parent and
  event filters; retained payload/timestamp summaries and typed task details.
- Bulk/filtered run cancellation and replay with explicit affected task IDs.
- Workflow deletion and guarded pause/unpause, filter lookup and partial updates,
  real CEL evaluation, tenant details, typed inbound event webhooks, schedule
  rescheduling/bulk operations, task metrics/statistics and Prometheus scraping.

### Changed

- CEL debugging is now asynchronous and returns the engine evaluation, including
  false results and expression errors. In-memory CEL evaluation is unsupported.
- Bulk schedule methods now return affected IDs and per-item errors; updates
  take `ScheduleUpdate` values and change trigger times only.
- `RunRecord` retains an optional `RunSummary`. Legacy metrics handlers are
  explicitly unsupported on engine clients; use the supported task metrics APIs.

- `RunsClient::subscribe_to_stream` now returns `RunEventStream` rather than a
  finite vector. `WorkflowRunRef::stream_events` provides the same incremental API;
  `stream` collects until completion. Custom transports receive both workflow and
  task identity in `append_run_stream` and must return publication errors.
- Correct durable recovery APIs: `branch_durable_task` now accepts a task external
  ID, node ID and branch ID and returns `DurableBranchResult`; `restore_task`
  accepts a task external ID and returns `RestoreTaskResult`. The previous
  workflow replay aliases were removed. In-memory recovery is explicitly unsupported.

### Fixed

- Preserve explicit desired worker identity on normal single and bulk submissions.
- Use official gRPC submission paths so idempotency details are not lost through
  the REST proxy, and classify collisions as non-retryable.

- Resolve workflow filters to IDs, use exploded query arrays, and decode actual
  replay task acknowledgments while preserving the existing workflow ID.
- Create filters with workflow IDs and scope; distinguish inbound event webhooks
  from the existing webhook-worker operations.

- Remove workflow stream idle cutoffs, close subscriptions on consumer drop,
  publish DAG stream values under task identity, and propagate publishing failures.
- Resolve task IDs from the engine for fresh clients, avoid caching polled run
  outputs across external durable branches, and process eviction acknowledgments
  so cooperative handlers preserve the engine's eviction outcome.
- Retry transient REST run-metadata failures within the configured retry budget,
  bound metadata request time, and preserve terminal failures and cancellations
  without requiring the REST graph to be available.
- Enable platform trust roots for the default TLS transport and update both
  lockfiles to patched network and certificate-validation dependencies.
- Preserve concurrent durable acknowledgments, including when a caller stops
  waiting, without serializing unrelated task invocations.
- Recover terminal DAG outputs from each run's engine graph for fresh clients
  and resumed runs, including after a workflow version changes. Handle completed
  normal tasks when an unused failure handler delays engine finalization.
- Decode nested run details and distinguish running, failed, and unknown statuses.
- Preserve engine-owned cancellation outcomes instead of reporting a late handler
  return as a failure or resolving the run from local worker state.
- Schedule delays using absolute timestamps and reject clock-range overflow.
- Correct the README quickstart and run it against packaged crates during release
  validation. Document local installation until registry publication.
- Avoid health-test port conflicts on hosts with exhausted source-port ranges.
- Track the actual smoke-worker processes and bound their shutdown so failed
  local proofs cannot leave workers running against a later test engine.

## [0.1.0] - 2026-07-16

First public release of the unofficial Rust Hatchet SDK.

### Added

- Typed workflow, task, durable-task, child-run, and run-reference APIs.
- In-memory workflow transport with concurrent DAG scheduling and named parent outputs.
- Authenticated gRPC/REST transport, supervised worker reconnects, health/readiness,
  cancellation, events, rate limits, logs, and selected management clients.
- Durable waits, event callbacks, memoization, engine-backed child runs, and
  eviction-aware cancellation.
- Scoped event publication and scoped historical-event lookback.
- Re-exported `#[workflow]`, `#[task]`, and `#[durable_task]` macros.
- Vendored Hatchet `v0.94.14` protocol definitions and vendored `protoc`.
- Standalone examples, including the two-worker Hatchet Lite/PostgreSQL Railway proof.
- MSRV, feature-matrix, strict lint, rustdoc, security-advisory, and clean-package CI.
- Guarded multi-crate publishing in dependency order.

### Fixed

- Propagate workflow registration and worker lifecycle failures instead of
  reporting false success.
- Preserve namespaces, multi-parent task outputs, live context metadata,
  non-retryable completion flags, and supported run options.
- Buffer early durable completions and propagate durable listener failures.
- Report awaited durable callbacks to the engine, use UUID condition groups,
  and resume engine-backed memo/sleep operations reliably.
- Recover event/cron trigger context from canonical engine metadata on Hatchet
  versions whose v1 action envelope reports a manual trigger.
- Normalize workflow/event namespaces to Hatchet's underscore wire convention
  and avoid worker-readiness deadlocks while opening action streams.
- Reject invalid unscoped durable lookback before it can hang, and normalize
  Hatchet callback match envelopes to bare event payloads and the matched branch.
- Register workflow failure handlers as distinct engine tasks.
- Align concurrency strategy names with Hatchet wire semantics.
- Bound result caches and worker shutdown; remove stale in-memory dispatchers.
- Redact client tokens from debug output.
- Return explicit unsupported-operation errors for unimplemented live management calls.
- Correct workflow-specific cron and scheduled-run REST routes.
- Restore actual Rust 1.85 compatibility after late hardening changes and make
  the guarded publish preflight enforce the declared compiler version.
- Keep the Railway proof portable to macOS Bash 3.2 and resilient to non-JSON
  CLI notices while matching exact run/action log records.

[Unreleased]: https://github.com/christopher-caldwell/hatchet_rust_sdk/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/christopher-caldwell/hatchet_rust_sdk/releases/tag/v0.1.0
