# Worker connection recovery evidence

The production incident exposed a worker defect separate from the engine outage: a half-open
HTTP/2 connection could hold a heartbeat indefinitely. The worker awaited that heartbeat inside
its select branch, so it also stopped observing task-stream errors and cancellation. Connection
readiness could stay true even though the worker could no longer accept scheduled work.

`GrpcTransport` now applies a ten-second per-attempt unary RPC budget to both response headers
and bodies and sends the corresponding gRPC deadline metadata. Channel establishment has a
ten-second connection budget. HTTP/2 keepalive uses a thirty-second interval and ten-second
acknowledgement budget, including idle connections. No `Endpoint::timeout` is set: `ListenV2`
can wait for its first assignment before sending response headers. Run subscriptions and durable
streams likewise have no whole-stream deadline.

Heartbeat completion remains in the select alongside stream failure and cancellation. Only one
heartbeat is in flight. Registration, version discovery and durable listener startup can also be
cancelled. A disconnected session drops its old heartbeat and task stream before reconnecting.
Existing retry counts remain unchanged. A deadline does not prove a mutation was rejected by the
engine; this change supplies no concurrent or external exactly-once guarantee.

## Executed checks on November 3, 2026

- Baseline `377dbf1`, with the new protocol test copied into a detached checkout: all three
  selected regressions failed. A stalled heartbeat masked cancellation and a stream failure;
  a unary response body could outlive the test's twelve-second bound. The baseline checkout
  was subsequently removed; failure logs are retained.
- Patched real loopback HTTP/2 peer: all six contracts passed across the five-test run and the
  additional registration-cancellation test. These cover startup heartbeat cancellation,
  registration cancellation, stream failure during a pending heartbeat, a stalled unary body,
  healthy idle listening beyond the unary budget, and replacement of a blackholed TCP
  connection. The replacement test used zero per-call retries and recovered in 41 seconds.
- Disposable local Hatchet Lite v0.114.0, image digest
  `sha256:47bfdd1ee569d760012dfb37ba5e551c3cdb5f2f6018b4d9d11ba7febb52f731`,
  with local PostgreSQL 16 and the SDK's default retry policy: both
  `durable_wait_and_checkpoint_survive_engine_freeze` and
  `durable_wait_and_checkpoint_survive_engine_restart` passed together in 44.42 seconds.
  The freeze pauses the engine without closing sockets until readiness fails, then resumes it.
  The restart replaces its process. Both assert readiness becomes 503 while liveness stays 200,
  readiness returns to 200, the durable event wait completes, the memo remains the original
  value, and the task external ID is preserved. Freeze cleanup unpauses the owned engine even
  when an assertion fails. Each invocation uses unique action names to avoid old fixture tasks.
- `cargo test --workspace --all-targets --all-features`: 201 passed, 37 ignored before the
  additional registration regression and freeze test were added. The new registration test
  passed separately and both real-engine tests were explicitly executed. Ordinary ignored
  integration tests were not silently counted as passing.
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` and formatting passed;
  the final check result is in the retained verification logs.
- The Lead Generator application's `just verify` passed: 281 test executions, 28 live tests
  ignored. Its initial attempt lacked a cached dependency; after `cargo fetch`, the gate
  completed successfully. A second `just verify` also passed with the local patched SDK
  supplied through a temporary Cargo source override. The override and original lockfile
  were restored afterwards; no application manifest or lockfile change remains. This
  establishes local application compatibility, not a released or deployed SDK upgrade.

Evidence logs live under `~/Backups/lead-generator/incident-20261031/`, with prefixes
`sdk-baseline-regressions`, `sdk-recovery-final`, `sdk-registration-cancel`,
`sdk-workspace-final`, `sdk-clippy-final`, and `sdk-local-engine-recovery`.

The first local fixture attempts did not exercise engine restart recovery. Docker Desktop did
not publish ports on the internal-only network. A later attempt also failed to seed a regular
user/tenant because the generated admin password was rejected; the image entrypoint continued
with only its internal tenant, which the scheduler intentionally excludes. The corrected
fixture uses an explicitly valid password, selects the regular fixture tenant, and verifies its
partition bindings before running the test. Security checks and outbound analytics were
explicitly disabled. No production endpoint or business provider was called by these tests.

## Release boundary and remaining incident

These changes are local and unreleased. Publishing an SDK, updating the worker dependency,
pushing watched application paths, deploying, or changing production scheduling each require
the owner's applicable approval. Keep the existing single production cron path until a
separately authorized cutover; do not create duplicate schedules.

This establishes recovery from the tested connection failures. It does not diagnose or prevent
the original engine VM's extreme load/steal episode. The local engine restart is graceful at
the socket level; the separate freeze and HTTP/2 blackhole tests exercise silent failures.
The local timings are test observations, not a production availability guarantee.

| Playbook rule/source | Scope and evidence | Classification |
| --- | --- | --- |
| IN1 / CONDITIONAL / infrastructure-adapters | The existing SDK transport remains the concrete remote adapter; no application/domain dependency or package change. | COMPLIANT WITH RULE |
| IN3 / MUST / infrastructure-adapters | Channel and RPC construction remain in the existing SDK adapter; no service locator or inward graph construction added. | COMPLIANT WITH RULE |
| TEST2 / CONDITIONAL / testing-evidence | No persistence mapping or transaction change. The disposable engine uses real PostgreSQL; no application database guarantee is claimed. | NOT APPLICABLE |
| Testing/evidence guidance: local behavior | Baseline failures, actual HTTP/2 faults, and actual local engine restart/freeze assertions were executed separately; environment and limits are recorded above. | COMPLIANT WITH RULE |
| Testing/evidence guidance: production release and recovery | No release, worker deployment or production recovery test was performed. | NOT EXERCISED |
