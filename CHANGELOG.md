# Changelog

## [0.1.0] - 2026-04-20

Initial SDK foundation release.

### Added

- Workspace crates:
  - `hatchet-core`
  - `hatchet-sdk`
  - `hatchet-macros`
- Typed client declaration and run APIs:
  - `Client::workflow`, `Client::task`, `Client::durable_task`
  - `WorkflowHandle::run`, `run_no_wait`, `run_many`
- Child workflow execution support in context:
  - `InMemoryContext::run_child`, `run_child_no_wait`
  - `bulk_run_children`, `bulk_run_children_no_wait`
- Durable context primitives:
  - invocation counting, event waiting, sleep/time helpers, child spawning
- Worker runtime:
  - task/durable task registration
  - typed dispatch APIs and lifecycle (`start`, `shutdown`, `wait`)
- Feature clients:
  - runs, workflows, schedules, crons, workers, metrics, rate limits, events, logs, filters, webhooks, cel
- Proc macro sugar:
  - `#[workflow(name = "...")]`, `#[task]`, `#[durable_task]`
  - generated task handles and module-level `register_all`
- Validation and integration coverage:
  - Phase 6 validation test suite
  - Phase 7 proc-macro integration test
  - runnable checkpoint examples in `examples/src/bin`

### Notes

- Current transport/runtime is in-memory oriented for API iteration speed.
- Engine wire-protocol parity remains future work.
