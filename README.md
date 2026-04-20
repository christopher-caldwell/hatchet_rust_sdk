# hatchet_rust_sdk

Personal Rust SDK implementation for Hatchet, built to control feature scope and prioritize child workflow execution support.

## Scope

This repository currently includes:

- `hatchet-core`: shared run primitives (`RunOpts`, `WorkflowRunId`, `Priority`)
- `hatchet-sdk`: client API, context APIs, durable context, worker runtime, feature clients
- `hatchet-macros`: optional proc-macro sugar (`#[workflow]`, `#[task]`, `#[durable_task]`)
- `examples`: runnable usage scenarios

The implementation is intentionally in-memory first so API ergonomics can be iterated quickly before transport parity hardening.

## Quickstart

Run the examples:

```bash
cargo run -p example --bin declare_and_run
cargo run -p example --bin context_child_runs
cargo run -p example --bin durable_context
cargo run -p example --bin worker_runtime
cargo run -p example --bin feature_clients
cargo run -p example --bin validation_suite
cargo run -p example --bin proc_macros
cargo run -p example --bin quickstart_child_workflow
```

## Development

```bash
just fmt
just lint
cargo test --workspace
```

Build docs:

```bash
cargo doc --no-deps --all-features
```

## Parity Notes

- Current surface is API-first and test-backed for declaration, child execution, durable flows, worker dispatch, and macro sugar.
- Transport/wire parity with official Hatchet SDKs is intentionally deferred while stabilizing local API behavior.
