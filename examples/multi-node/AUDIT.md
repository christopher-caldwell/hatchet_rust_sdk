# SDK audit for the Railway multi-node proof

Audit date: 2026-07-15/16. Checkout baseline:
`05fa2d489d036d17f9c6476684f92aa4a3392298`.

| Required capability | Current implementation evidence | Test/evidence status |
| --- | --- | --- |
| Environment configuration | `config.rs` parses token, gRPC host, server URL, namespace, log level, and TLS strategy. | Existing config unit tests plus standalone role/port tests pass. |
| Authenticated engine transport | `GrpcTransport::from_config` builds authenticated gRPC and REST clients. | Real local official-engine smoke passes; not inferred from in-memory tests. |
| Workflow registration | `GrpcTransport::register_workflow` performs engine registration. `Client::register_workflow` propagates registration failures. | Orders worker readiness fails if registration fails; local smoke proves the workflow appears and runs. |
| Trigger without waiting | `Workflow::run_no_wait` calls transport trigger and returns a run reference. | Standalone trigger prints the live run ID before optional observation. |
| Separate actions on separate workers | Worker registration sends only each builder's tasks. | Local health and live logs prove orders has `process-order` and notifications has `send-order-receipt`. |
| Worker readiness and health | Worker startup now acknowledges readiness only after engine registration; `/health` reports name, actions, labels, and slots. | Focused unit/integration tests and local smoke pass. |
| Live engine tests | The root SDK exposes nine explicitly ignored credentialed tests. | The local harness bootstraps a real token and runs all nine: basic, child, DAG, durable memo/sleep, scoped durable-event lookback, event triggering, REST clients, logs, and cancellation. |
| Result/status observation | `get_run_details` observes engine status/output; trigger utility polls only with `--wait`. | Local run completed with typed receipt at a three-second configured interval. |

## Final verification

The original baseline failures were remediated as part of the publish-readiness
pass. Formatting, strict workspace and standalone Clippy, all-target/all-feature
tests, no-default tests, strict rustdoc, Rust 1.85 checks, RustSec audits, and
exact packaged-archive tests pass. The final local run used the pinned official
Hatchet Lite image and PostgreSQL 16, completed the cross-process workflow and
all nine live SDK tests, rechecked both worker health endpoints after stream
recovery, and removed its isolated Docker resources.

The final candidate sources were also uploaded to both Railway workers. The
first build correctly exposed late Rust syntax that violated the declared 1.85
minimum; that release blocker was fixed and the publish preflight now repeats
both MSRV checks. Corrected deployments `b87b4fff-c3c4-4c18-ae3d-d87b12f51eb0`
and `5ab09f1e-df88-4a4b-965c-00da0813df67` reached `SUCCESS`. Runs
`c82fa3c2-3fd8-4b4d-8d47-d8ace3606a2a` and
`20627f5c-6765-4a6a-b2fb-272e6df56877` completed on the intended separate
workers before and after a notifications-worker restart. Both private health
endpoints were healthy, all four services had one running replica and zero
crashes, and post-ready worker error logs were empty.

## Narrow SDK defects fixed for the proof

- Engine workflow-registration errors were swallowed, allowing false readiness.
- Worker readiness was announced before gRPC worker registration completed.
- Official Hatchet action envelopes were passed directly to typed handlers,
  losing the declared input and parent output needed by dependent steps.
- Top-level triggering used a legacy path that is not reliable with the current
  v1 engine; it now uses Hatchet's official stable REST trigger with an ID-only
  response while run observation remains engine-backed.
- Namespaces now use Hatchet's normalized underscore wire form for workflows,
  children, registration, and events.
- Durable listener clients now report awaited callbacks every second, condition
  groups use server-valid UUIDs, and event origin is recovered from canonical
  engine metadata when the action envelope says `manual`.
- Event publication carries optional scopes, durable lookback rejects a missing
  scope before waiting, and callback match envelopes are normalized to the bare
  event payload while preserving the matched OR branch.

Focused tests cover action-envelope decoding and the worker startup contract;
the supervised live suite covers the resulting engine behavior. The legacy
registration method remains compatible; the example uses the checked variant
so every required `Result` is handled.
