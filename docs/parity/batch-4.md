# Preview execution and developer tooling parity

Work proceeds through the remaining triage entries in order. Each implemented
entry receives a separate read-only code-review subagent before the next starts.
The [feature matrix](feature-support.md) records feature-specific evidence and
limits; [triage.json](triage.json) retains outstanding entries.

## N2: server-coalesced batch tasks

Implemented the official v0.114.0 additive registration and dispatcher contracts:
size/interval flushing, CEL grouping and group concurrency, exact member-result
maps, broadcast output, whole-batch failure and cancellation. Member retries are
forced to zero. Durable tasks, failure-handler batch configuration and in-memory
registration are rejected. See [batch-tasks.md](batch-tasks.md) for the public API
and the distinction between batch context identity and individual run references.

References were rechecked against official TS 1.43.0, Go v0.114.0
(`aef20b00755da5b1e7e69197e7189848309afb20`) and main
`af128ec1672adaf8e3cc4afd339ce5c2c6f08c35`; the recorded upstream versions remain
current. Additive protobuf fields keep upstream field numbers.

The first live run exposed omitted `task_name` on batch assignments. Dispatch now
resolves the registered default integration action ID. Four batch-specific live
cases pass against v0.114.0: size/group/interval coalescing, mapped/broadcast
outputs and whole failure, partial/all member cancellation, and handler-initiated
cancellation followed immediately by a return. The full live run passes **28**
executions (18 baseline contracts, nine modern contracts, one engine-restart
case), plus the two-worker example and cleanup.

Independent code review found one P2 cancellation race: the worker's combined
cancellation token did not update the transport completion guard synchronously.
The context now retains the original batch token and cancels it after the engine
acknowledges cancellation. The reviewer independently ran the focused regression
that checks both original/combined tokens without yielding, including rejection
preserving both tokens. Review cleared with no remaining actionable findings.
Four registration/handler wire tests and malformed-assignment/output unit tests
also pass. Detailed local evidence is under `target/parity-batch4-20261025/`.

Guidance: installed Rust Playbook v0.3.0, revision
`14c3ee5cfdb84b7431258c6822205f91d61da8be`. Infrastructure adapters retain the
preview server contract; TEST2 is exercised with the actual engine. Batch worker
process-crash recovery and production capacity are NOT EXERCISED. The official
TS/Go suites are not run by this repository. Crates remain unpublished.

The completed N2 workspace suite passes **149** tests; no-default SDK tests pass
**146**, and the standalone example passes **7**. Format, strict Clippy/rustdoc,
doctests and all six matrix guard tests pass. Engine requirements are checked
against the runtime feature gates by the matrix renderer.
Rust **1.85.1** checks pass for both workspace and standalone targets.

## N3: per-error retry delay

`RetryAfterError` preserves the message and exact whole-millisecond delay, and
remains retryable. Step and batch-member failure events retain the official
`retry_after_ms` field numbers. Success/non-retryable failures do not acquire an
override. Engine retry counts and delay caps remain authoritative; preview batch
retries stay zero. See [retry-after.md](retry-after.md).

Three protocol tests cover current/floor releases, older/unrecognized releases,
zero delay, rejected failure reporting and a stalled capability lookup. Boundary
unit tests reject fractional/overflowing durations. The live case proves normal
and durable requested delays replace a configured 60-second backoff, retry count
advances once, and a task with no retries left runs only once. It passes on both
**v0.111.0** and **v0.114.0**. The current-engine full suite passes **29** live
executions; a separate support-floor run passes the focused retry test, alongside
the two-worker example and complete cleanup. Batch failure with `RetryAfterError`
still fails all members without granting additional attempts.

Independent review found one P2 reliability issue: a hung `GetVersion` could
block reporting the task failure indefinitely. The complete capability probe,
including its retries, is now bounded to two seconds. A controlled HTTP/2 peer
holds version discovery for 60 seconds but accepts failure events; the regression
proves a normal retryable failure is sent within the bound, with no override.
The reviewer independently ran this regression and cleared the change with no
remaining actionable findings. The live support-floor run also passes after this
fix. No production load or engine delay-cap exhaustion test is claimed.

Final N3 checks pass **154** workspace tests, **151** SDK tests without default
features and **7** standalone tests. All ten quality/compatibility checks pass,
including strict Clippy/rustdoc and Rust 1.85.1; six matrix guard tests also pass.

## Newly observed routing/restart limitation (S1)

Follow-up: [batch-5.md](batch-5.md) now proves eventual recovery after a longer
observed restart window. The original failures below remain historical evidence.

Two full-suite attempts placed the new topic restart proof before ordinary hard
sticky routing. Topic reader/cursor recovery passed both times. The routing test
then left a task queued until its 45-second deadline. Read-only database
inspection confirmed the task's `HARD` strategy, expected action and requested
worker ID matched an active worker with current heartbeat/listener and configured
slots. The second run also passed ordinary topic reset/history/large-payload
operations, isolating the ordering assertion from this separate failure.

S1 is now **partial**. Ordinary single/bulk explicit routing retains its existing
wire and fresh-engine live evidence; hard sticky recovery immediately after
engine restart is not verified and has an observed failure. The evidence is
consistent with a scheduling/restart interaction, but does not conclusively
attribute the cause to the engine without an equivalent official-client or
independent raw-wire reproduction. No schema mutation, worker substitution or
weakened `HARD` routing policy was used to bypass it.

Independent review agreed that the disruptive topic restart proof should run in
an explicit late test target, after ordinary routing/checkpoint contracts. Ordinary
topic operations remain before sticky routing, so the isolation experiment checks
the trigger rather than removing stream operations from that scenario. Failed
runs and database state are retained under `target/parity-batch4-20261025/n5/`.

## N5: durable topic streams

Implemented the current TS preview contract as `client.streams()?`: independently
namespaced topics, retained-message metadata, cursor reads and reset, serialized
producer sequences, automatic count/byte bounded batches, single-publish fallback
with partial acknowledgments, and large-payload upload/fetch. Readers reconnect
from the last delivered cursor; failed payload fetches do not skip buffered
messages. Dropping/cancelling a reader closes its actual HTTP/2 request. See
[durable-topics.md](durable-topics.md) for the API and error/lifetime contract.

Nine controlled HTTP/2/HTTP tests cover count/byte limits, namespace isolation,
ambiguous-write sequence rotation, producer-gap recovery, older RPC fallback,
partial failure, upload/reference authentication, transient/permanent payload
fetches, cursor reconnect, hangup, cancellation and actual request closure.
Independent source review found no product defects. Its P2 test-order finding was
fixed with explicit biased polling of concurrent publish futures; the reviewer
independently reran all nine corrected tests successfully.

The fresh v0.114.0 run passes **31** live checks: 18 baseline contracts, 11 modern
contracts, durable checkpoint restart, then an explicit late topic restart phase.
It includes ordinary topic reset/resume/history/namespace semantics, 4 MiB
upload/download and active reader plus cursor persistence across real engine
restart. Ordinary stream operations and hard sticky routing pass together before
the disruptive phases. The existing restart/routing limitation is retained above
and in S1, rather than erased by test isolation. The local test tenant enables
the official `SERVER_DEFAULT_TENANT_ENTITLEMENT_DURABLE_STREAMS` configuration;
production tenant enablement is an engine/account decision.

The real older-publish fallback, elapsed retention/cursor expiration, elapsed
producer watermark retention, 30-minute idle expiry and 64 MiB production capacity
are NOT EXERCISED against an engine. Their request/error paths are protocol-tested;
the actual live payload is 4 MiB. Uploads within a batch are processed in order,
so overlapping large-upload throughput is not claimed. The official TS/Go suites
are not run. New multipart dependencies pass both lockfile RustSec audits against
the recorded advisory database with zero vulnerabilities/warnings.

N5 final checks pass **163** workspace tests, **160** SDK tests without default
features, **7** standalone tests and six matrix guard tests. Strict Clippy/rustdoc,
format and Rust **1.85.1** workspace/standalone checks pass. The unchanged baseline
engine v0.94.14 passes **19** live contracts, checkpoint restart and the two-worker
example; both engine runs finish with complete cleanup.

## N4: owned embedded real engine

`embedded::EmbeddedClient` and `EmbeddedEngine` manage the official sidecar with
private validated readiness, persisted bundled Postgres, explicit connection
configuration and awaited, cancellation-safe shutdown. Eight executable-peer
lifecycle tests and three HTTP release/cache tests cover the failure boundaries.
The real published v0.110.5 sidecar is tracked independently of SDK v0.114.0;
[embedded.md](embedded.md) records exact source/artifact pins and supported modes.

Independent review found a P2 checksum race in a shared same-tag cache filename:
a different digest policy could replace a pinned caller's verified path before
spawn. Cache executables now live in expected-digest directories; pinned calls do
not rewrite the release-policy offline reference. The reviewer independently ran
the differing-policy interleaving regression and cleared all source findings.

Post-fix native tests pass twice: one real download/install run and one warm-cache
run with public HTTP(S) proxies deliberately unreachable (loopback bypassed).
These are **four live executions** of two cases on darwin-arm64. They run/restore
workflow history and definitions across bundled-Postgres restart, authenticate a
protected gRPC workflow-registration RPC without REST, and assert sidecar and
Postgres processes exit. There is no Docker or supplied API token in these runs.
A separately pinned Linux-amd64 CI job is supplied, not remotely executed yet.

Final N4 workspace tests pass **174**, SDK tests without default features pass
**171**, and standalone tests pass **7**. Strict Clippy/rustdoc, format, Rust
**1.85.1** workspace/standalone and both RustSec lockfile audits pass; vulnerabilities
and warnings are zero. Nine Python support-claim guards pass, including embedded
release/source/artifact drift and native CI pin checks. Upstream versions/main
were queried again and are current. Native parent SIGKILL during initialization,
external DB/RabbitMQ fleets and forced-kill database cleanup are NOT EXERCISED.

## O2: runtime input schemas and generation

Replaced the handwritten subset with reusable offline `jsonschema` 0.60.1
validators and Serde-deserialization schema generation through Schemars 1.2.3.
Both preserve Rust 1.85 compatibility. Normal, durable and individual batch inputs
are validated after middleware and before typed decode/handler execution; invalid
schema configuration fails before worker startup or workflow registration mutation.
Workflow schemas remain explicit metadata. See [input-schemas.md](input-schemas.md).

Seven focused contracts cover constraints/composition/conditionals/recursive local
references, generated Serde names/defaults/enums, exact registration metadata,
pre-mutation errors, blocked external retrieval and handler/decode boundaries. A
separate SDK-only consumer builds and runs the re-exported derive API. A live
test observes all attempts through middleware: three valid and three rejected
inputs produce six attempts, only three handler calls and no retries despite a
configured three-retry policy. Both baseline v0.94.14 and current v0.114.0 pass.
The full runs pass **20** baseline and **32** current live contracts with complete
cleanup, preserving the separately documented S1 restart/routing limitation.

Independent review found a nested property-name value leak through the validator's
masked renderer, then a batch member prefix that bypassed message truncation.
Diagnostics now render only schema/instance locations and cap the complete message
(including member identity) at 512 bytes plus an ellipsis, with UTF-8-safe truncation
and at most 16 messages. Both regressions pass; the reviewer independently reran
all seven contracts and cleared the source. No official SDK or exhaustive JSON
Schema conformance suite is claimed; external resources and Zod callbacks remain
outside the documented offline schema contract.

The workspace suite passes **181** tests; strict Clippy/rustdoc, Rust **1.85.1**
workspace/standalone checks and both lockfile audits pass. Final batch-wide checks
follow O4. Detailed evidence is retained under `target/parity-batch4-20261025/o2/`.

## O4: distinct liveness and readiness

Implemented the latest TS `/livez` and `/readyz` operational contract while
preserving Rust's legacy `/health` JSON and 200/503 behavior. Liveness does not
acquire worker state or depend on its connection; readiness follows the existing
transport listener state. HEAD/routing, invalid/oversized HTTP, fragmented
requests and bounded connection cleanup are covered in [health-probes.md](health-probes.md).

Four real TCP tests and the 30 existing parity tests pass. Testing exposed a
platform TCP-reset issue when rejecting oversized headers with unread request
bytes; the server now sends its write-side FIN then drains under the existing
two-second deadline. Independent review checked that handling, lifecycle ownership
and the status contracts and independently reran all 34 tests; no actionable
findings remained.

The disposable checkpoint restart proof now additionally observes readiness
200 → 503 → 200, and liveness 200 during the real listener outage. It passes on
both **v0.94.14** and **v0.114.0**. Full runs pass **20** baseline and **32** current
live contracts with complete cleanup, including the ordinary hard sticky test
before the explicitly isolated late restart phases. The unresolved S1 recovery
limitation remains partial. Latest official package/source/artifact checks are
current; no newer official revision was silently substituted.

All six batch entries now have independent review clearance. The 39-row matrix
contains **30 supported**, **9 partial** and **0 missing** entries. Partial entries
retain their operational or evidence limits, including exporter setup, scheduling
coverage, real TLS/federation proofs, remote CI and hard sticky recovery. This is
not an assertion of exhaustive SDK parity or production capacity.

## Final verification and packaging

The final source passes **185** workspace tests, **182** SDK tests without default
features, **7** standalone example tests and **9** Python support-claim guards.
Format, strict workspace/standalone Clippy, strict rustdoc, doctests and Rust
**1.85.1** workspace/standalone checks pass. Both lockfile RustSec audits report
zero vulnerabilities and zero warnings against the recorded advisory database.
The final live runs pass **20** baseline and **32** current engine contracts; the
separate N4 native cold/offline-warm proof remains four post-fix live executions.

Release preflight's quality stages passed, then its package-content guard exposed
a stale expected protobuf count: durable streams adds the eighth build input.
The guard now enforces eight files. The streams reviewer independently checked
the exact package list against all eight `build.rs` inputs and cleared the change.
The unchanged packaging, exact-archive test/doc, README consumer and registry
dry-run functions were resumed after that correction and all passed. Core/macros
registry dry runs pass; the SDK registry dry run remains deferred until those
unpublished sibling versions exist in the registry. Nothing was published.

Official versions/main and embedded release/source/assets were rechecked and
remain current. Origin main was fetched successfully through the scoped VPN
workaround. No VPN/system settings were changed. Final local check logs and
archive evidence are under `target/parity-batch4-20261025/final/`; engine reports
are under `o4/`. Remote GitHub Actions, official TS/Go suites, real TLS/federation
coverage, production load and the other matrix partials are not asserted by
these results.
