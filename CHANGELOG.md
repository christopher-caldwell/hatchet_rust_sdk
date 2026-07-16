# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project uses [Semantic Versioning](https://semver.org/).

## [Unreleased]

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
