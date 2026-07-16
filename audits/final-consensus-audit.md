# Final Consensus Audit: `another-hatchet-sdk` v0.1.0

## Summary

Five independent reviewers were requested to assess the current working tree for
first-version publish readiness. Four valid reports completed; the original
`reviewer-03` attempt and its replacement failed before producing a report.
Following the audit skill's failure policy, synthesis continued with the four
complete reports and a quorum denominator of four. At the configured `0.60`
threshold, three reviewers were required for quorum.

The reviewers unanimously found that the original candidate was not ready to
publish. The root remediation pass addressed every quorum-backed issue and every
concrete high-severity minority issue, plus several lower-severity risks. The
resulting candidate is approved for a v0.1.0 publish under the crate name
`another-hatchet-sdk` and Rust import name `another_hatchet_sdk`.

No crate, commit, tag, or release was published by this audit.

## Configuration Used

```yaml
audit_target: "Current working tree at base commit 05fa2d489d036d17f9c6476684f92aa4a3392298"
audit_type: "code_review"
reviewer_count_requested: 5
reviewer_count_completed: 4
reviewer_mode: "identical_reviewers"
quorum_threshold: 0.60
quorum_support_required: 3
include_minority_findings: true
minority_severity_floor: "high"
compatibility_target: "Hatchet v0.94.14"
```

## Publish-Readiness Verdict

**Approved for v0.1.0 publish.** All deterministic release gates are green, the
exact package archives build outside the repository, and the release workflow
handles the first-publication dependency ordering for:

1. `another-hatchet-core`
2. `another-hatchet-macros`
3. `another-hatchet-sdk`

The remaining limitations are explicitly scoped rather than silent: unsupported
engine-management operations return `UnsupportedOperation`, and compatibility
is currently declared only for Hatchet `v0.94.14`. All nine opt-in integration
tests were run successfully against that exact official engine image during the
final supervised local validation.

## Final Local Engine Proof

The final worktree was exercised with
`./examples/railway-multi-node/scripts/local-smoke.sh` on 2026-07-16. The
harness used PostgreSQL 16 and the exact official image
`ghcr.io/hatchet-dev/hatchet/hatchet-lite:v0.94.14@sha256:b8d6b7e9b96d354f1a252590d1480d3bb70f01604cc6cba1fd97ff7aaa2cb07c`.

- The separate orders and notifications processes completed workflow run
  `9f75decc-0b40-451e-9384-f302222a286d` in the expected cross-worker order.
- All nine live SDK tests passed: basic execution, child workflow/output,
  dependent DAG, durable memo/sleep/resume, scoped and namespaced durable-event
  lookback, event triggering/context, REST feature clients, log retrieval, and
  cancellation.
- Hatchet naturally closed both example action streams during the SDK test
  phase; both reconnect supervisors recovered and both `/health` endpoints were
  required to pass afterward.
- The harness reported complete teardown of its two containers, Docker network,
  and configuration volume. It left no SDK worker process running.

## Final Railway Artifact Checkpoint

The candidate crate sources and locked standalone worker artifact were then
uploaded from the prepared local build context to the existing Railway
production project on 2026-07-16. This checkpoint found one additional genuine
release blocker: late remediation had introduced Rust let-chain syntax that the
declared Rust 1.85 compiler rejects. Both initial Railway builds failed before
serving traffic. The SDK was rewritten with Rust-1.85-compatible control flow,
and the guarded publish preflight now runs the workspace and standalone MSRV
checks itself so a late regression cannot bypass the release command.

The corrected deployments both reached `SUCCESS`:

- `rust-orders-worker`: `b87b4fff-c3c4-4c18-ae3d-d87b12f51eb0`
- `rust-notifications-worker`: `5ab09f1e-df88-4a4b-965c-00da0813df67`

The full Railway proof then completed run
`c82fa3c2-3fd8-4b4d-8d47-d8ace3606a2a`, restarted the notifications service,
waited for its health endpoint to recover, and completed run
`20627f5c-6765-4a6a-b2fb-272e6df56877`. Per-service logs place
`process-order` on the orders worker and `send-order-receipt` on the
notifications worker for both runs. Both private health endpoints report the
expected single action, labels, slot counts, and `healthy: true`. The
orchestrator, PostgreSQL, and both workers read back `SUCCESS` with one running
replica and zero crashes; neither worker emitted an error after its latest ready
event. The temporary Railway SSH public key used for private-container proof
commands was removed and the empty key list was verified.

The complete IDs and runtime records are retained in
[`railway-evidence.json`](../examples/railway-multi-node/railway-evidence.json).

## Consensus Findings

### Finding 1: The original crates were neither publishable nor self-contained

**Severity:** critical  
**Confidence:** 10/10  
**Support:** 4/4 reviewers  
**Quorum Status:** unanimous  
**Category:** release engineering  
**Root Verification:** verified

**Original issue:** Publication was disabled, internal path dependencies lacked
registry versions, package metadata was incomplete, and the SDK build depended
on ignored protobuf files outside the package.

**Remediation:** The public crates now use the `another-hatchet-*` names, carry
complete crates.io metadata and versioned path dependencies, include MIT license
text, and vendor all seven Hatchet protocol files plus a vendored `protoc`.
Publishing is ordered by the dependency graph.

**Verification:** `scripts/publish-crates.sh --check-only --allow-dirty`
successfully created all three `.crate` files, verified their contents, tested
the extracted SDK against the extracted core and macros archives, built strict
rustdoc, and built a minimal downstream consumer. Core and macros also passed
real crates.io upload dry-runs. The SDK upload dry-run is intentionally repeated
by the release script after those two first-release dependencies are indexed.

**Reviewers Supporting:** `reviewer-01`, `reviewer-02`, `reviewer-04`,
`reviewer-05`

### Finding 2: The repository's release-quality gates were red

**Severity:** high  
**Confidence:** 10/10  
**Support:** 4/4 reviewers  
**Quorum Status:** unanimous  
**Category:** testing  
**Root Verification:** verified

**Original issue:** Formatting, strict Clippy, and the all-feature test build
failed. The committed CI workflow could not provide a release signal.

**Remediation:** Source and tests were formatted, integration compilation was
repaired, generated-code lint handling was made practical, and CI now checks
formatting, warning-free Clippy, all targets/features, no-default features,
strict rustdoc, MSRV, standalone Railway tests, exact packages, and RustSec.

**Verification:** The workspace completed 94 automated tests with no failures;
the nine credentialed live tests remain explicit ignores in ordinary offline
runs and all nine passed when invoked against the isolated local engine. The
standalone Railway crate completed seven more tests. Strict Clippy and strict
rustdoc passed for the supported workspace.

**Reviewers Supporting:** `reviewer-01`, `reviewer-02`, `reviewer-04`,
`reviewer-05`

### Finding 3: The declared no-default-features configuration did not compile

**Severity:** high  
**Confidence:** 10/10  
**Support:** 4/4 reviewers  
**Quorum Status:** unanimous  
**Category:** design  
**Root Verification:** verified

**Original issue:** Optional transport dependencies were consumed
unconditionally, making Cargo's advertised feature graph false.

**Remediation:** Production gRPC/REST transport is now an honest part of the
v0.1 base crate. `grpc` remains as a documented no-op compatibility feature,
and its dependencies are mandatory.

**Verification:** `cargo check -p another-hatchet-sdk --no-default-features
--all-targets --locked` and the stronger no-default test run in the release
preflight both passed.

**Reviewers Supporting:** `reviewer-01`, `reviewer-02`, `reviewer-04`,
`reviewer-05`

### Finding 4: Worker stream loss could leave a healthy but idle process

**Severity:** high  
**Confidence:** 9/10  
**Support:** 4/4 reviewers  
**Quorum Status:** unanimous  
**Category:** correctness and availability  
**Root Verification:** verified

**Original issue:** EOF, heartbeat failure, or action-stream error terminated a
detached listener without reconnecting or changing readiness.

**Remediation:** Worker registration, action listening, heartbeat, and initial
durable-listener readiness now run under a cancellation-aware reconnect
supervisor with bounded exponential backoff. Health begins false, becomes true
only after required streams are active, and returns false while the main worker
stream reconnects. The handle owns the transport lifecycle.

**Verification:** Code paths, connection-state tests, shutdown tests, local
worker tests, and the live Railway workers were verified. During the final local
run Hatchet closed both example action streams, both supervisors entered their
reconnect path, and the harness required both health endpoints to recover before
success. A deliberately injected network partition remains useful follow-up
coverage rather than a publish blocker.

**Reviewers Supporting:** `reviewer-01`, `reviewer-02`, `reviewer-04`,
`reviewer-05`

### Finding 5: Workflow registration reported false success

**Severity:** high  
**Confidence:** 10/10  
**Support:** 3/4 reviewers  
**Quorum Status:** meets quorum (75%)  
**Category:** error handling  
**Root Verification:** verified

**Original issue:** `Client::register_workflow` discarded serialization and
transport failures while recording a local declaration.

**Remediation:** The primary API is now fallible, propagates engine errors, and
records locally only after remote registration succeeds. The previous checked
alias remains deprecated. README and examples use the fallible API.

**Reviewers Supporting:** `reviewer-01`, `reviewer-02`, `reviewer-05`

### Finding 6: Fast durable completions could be dropped

**Severity:** high  
**Confidence:** 10/10  
**Support:** 3/4 reviewers  
**Quorum Status:** meets quorum (75%)  
**Category:** concurrency and durable execution  
**Root Verification:** verified

**Original issue:** An engine completion arriving between acknowledgement and
waiter registration was discarded, potentially hanging a durable task forever.

**Remediation:** Callback registration and early completions now share an atomic
registry. Unmatched completions are buffered with a 1,024-entry bound and
consumed when the waiter registers. Failed sends clean pending and in-flight
state. The listener immediately reports awaited callback references and polls
them every second across reconnects, while all generated condition group IDs
are server-valid UUIDs.

**Verification:** Completion-before-waiter, callback-status-envelope, and UUID
regression tests pass in both the source tree and the extracted package archive.
A live durable task memoized state, slept passively for one second, resumed from
the engine callback, and returned the expected output.

**Reviewers Supporting:** `reviewer-01`, `reviewer-02`, `reviewer-04`

### Finding 7: Engine contexts lost named parents and action metadata

**Severity:** high  
**Confidence:** 9/10  
**Support:** 3/4 reviewers  
**Quorum Status:** meets quorum (75%)  
**Category:** correctness  
**Root Verification:** verified

**Original issue:** The live decoder retained one arbitrary parent and defaulted
retry, trigger, filter, error, metadata, priority, child, and worker fields.

**Remediation:** A typed task-context payload now hydrates the complete named
parent map and relevant engine/action metadata. `parent_output(parent_name)`
selects a specific parent. The in-memory scheduler also supplies named parents
and executes ready DAG waves concurrently.

**Verification:** Multi-parent, metadata-rich decoder fixtures and in-memory DAG
tests pass. Live child-workflow and event-trigger runs also passed; the event
context recovered `local-demo_live.event` from canonical engine metadata. The
Railway proof remains a single-parent two-node DAG, so a live multi-parent run
is useful post-release coverage.

**Reviewers Supporting:** `reviewer-01`, `reviewer-02`, `reviewer-04`

### Finding 8: The waiting child helper created top-level runs

**Severity:** high  
**Confidence:** 10/10  
**Support:** 3/4 reviewers  
**Quorum Status:** meets quorum (75%)  
**Category:** correctness  
**Root Verification:** verified

**Original issue:** `ContextExt::run_child` bypassed the context path, losing
parent workflow/task IDs and child indexing.

**Remediation:** The waiting helper now uses the child no-wait path and waits on
the resulting reference. Durable child operations use Hatchet's durable event
log, preserve lineage/options, and wait for the engine callback.

**Reviewers Supporting:** `reviewer-01`, `reviewer-02`, `reviewer-04`

### Finding 9: Engine-backed feature clients could silently mutate local state

**Severity:** high  
**Confidence:** 9/10  
**Support:** 3/4 reviewers  
**Quorum Status:** meets quorum (75%)  
**Category:** public API correctness  
**Root Verification:** verified

**Original issue:** Several exported management/event operations reported
success while changing only process-local shadow state under `GrpcTransport`.

**Remediation:** Events and supported rate-limit/log/resource operations use
the engine. Operations without a verified Hatchet `v0.94.14` endpoint return an
explicit `UnsupportedOperation` for engine-backed clients; local behavior is
retained only for the in-memory transport.

**Reviewers Supporting:** `reviewer-01`, `reviewer-02`, `reviewer-04`

### Finding 10: Validation overstated production parity

**Severity:** high  
**Confidence:** 8/10  
**Support:** 3/4 reviewers  
**Quorum Status:** meets quorum (75%)  
**Category:** testing  
**Root Verification:** verified

**Original issue:** Empty parity-row tests, token-gated early-success live tests,
and an unrealistic serial in-memory DAG could produce confidence without
testing the claimed behavior.

**Remediation:** The empty parity matrix was removed. Credentialed tests are
explicitly ignored with clear invocation instructions, not reported as passes.
The in-memory DAG now models concurrency and parent data flow, and the
standalone Railway example has deterministic offline contract tests plus a real
two-worker proof.

**Reviewers Supporting:** `reviewer-02`, `reviewer-04`, `reviewer-05`

### Finding 11: Debug output could expose the bearer token

**Severity:** medium  
**Confidence:** 10/10  
**Support:** 3/4 reviewers  
**Quorum Status:** meets quorum (75%)  
**Category:** security  
**Root Verification:** verified

**Original issue:** Derived `Debug` output included raw tokens in both config
and builder values.

**Remediation:** Token fields are private, access is controlled internally, and
custom `Debug` implementations always redact secrets. Regression tests assert
that a known token never appears.

**Reviewers Supporting:** `reviewer-01`, `reviewer-02`, `reviewer-04`

## Strong Minority Findings

The following findings did not reach the three-reviewer quorum but met the
configured high-severity floor, had concrete evidence, and were root-verified.
All were remediated.

### Minority Finding 1: Durable APIs silently fell back to non-durable behavior

**Severity:** high  
**Confidence:** 10/10  
**Support:** 2/4 reviewers  
**Root Verification:** verified

Durable listener/send/callback failures previously fell back to local sleeps or
events, durable children bypassed the event log, and `now()` was only
process-memoized. The SDK now propagates listener failures, uses engine-backed
child triggers and memoization, and makes `now()` replay-stable.

**Reviewers Supporting:** `reviewer-02`, `reviewer-04`

### Minority Finding 2: Top-level labels and bulk triggering regressed

**Severity:** high  
**Confidence:** 9/10  
**Support:** 2/4 reviewers  
**Root Verification:** verified

The sequential REST bulk path dropped label support and could partially submit a
batch. Ordinary single triggers retain the stable REST endpoint; desired-label
and bulk requests now use the Hatchet bulk gRPC contract so options and batch
semantics are preserved.

**Reviewers Supporting:** `reviewer-02`, `reviewer-05`

### Minority Finding 3: Namespace normalization differed between registration and dispatch

**Severity:** high  
**Confidence:** 10/10  
**Support:** 2/4 reviewers  
**Root Verification:** verified

Namespace ownership is now inherited from the configured transport, normalized
to Hatchet's lowercase trailing-underscore convention, and applied idempotently
to registration, workflow handles, child/top-level dispatch, and events.
Namespace regression tests and all corresponding live paths pass.

**Reviewers Supporting:** `reviewer-01`, `reviewer-04`

### Minority Finding 4: Result polling cached a hard-coded five-minute timeout

**Severity:** high  
**Confidence:** 10/10  
**Support:** 2/4 reviewers  
**Root Verification:** verified

Engine result polling is now open-ended and cancellation/error aware. Only
successful terminal results are cached, and the cache is bounded to 1,024
entries.

**Reviewers Supporting:** `reviewer-01`, `reviewer-04`

### Minority Finding 5: Concurrency strategy names reversed wire behavior

**Severity:** high  
**Confidence:** 10/10  
**Support:** 2/4 reviewers  
**Root Verification:** verified

The public enum and wire conversion now expose Hatchet's supported semantics as
`CancelInProgress`, `CancelNewest`, and `GroupRoundRobin`; the misleading
`CancelOldest`/deprecated queue mapping was removed.

**Reviewers Supporting:** `reviewer-04`, `reviewer-05`

### Minority Finding 6: Durable cancellation and eviction did not reach handlers

**Severity:** high  
**Confidence:** 9/10  
**Support:** 1/4 reviewers  
**Root Verification:** verified

The combined engine/eviction token is now installed in durable task contexts,
waits observe it, and context cancellation calls the transport's workflow-run
cancellation operation.

**Reviewer Supporting:** `reviewer-01`

### Minority Finding 7: Shutdown could wait forever on a user future

**Severity:** high  
**Confidence:** 9/10  
**Support:** 1/4 reviewers  
**Root Verification:** verified

Shutdown now waits only for the configured cancellation grace period and then
aborts remaining handler tasks. A never-completing-handler regression test
proves the bound.

**Reviewer Supporting:** `reviewer-05`

### Minority Finding 8: `NonRetryable` was not serialized to Hatchet

**Severity:** high  
**Confidence:** 10/10  
**Support:** 1/4 reviewers  
**Root Verification:** verified

Completion event construction now sets Hatchet's `should_not_retry` field for
non-retryable errors and for cancelled runs. Wire-shape tests pass.

**Reviewer Supporting:** `reviewer-01`

### Minority Finding 9: Declared failure handlers were omitted from registration

**Severity:** high  
**Confidence:** 10/10  
**Support:** 1/4 reviewers  
**Root Verification:** verified

Failure handlers are now materialized as distinct engine tasks with the
workflow defaults and durable marker instead of being looked up among ordinary
steps. Registration-shape tests pass.

**Reviewer Supporting:** `reviewer-04`

### Minority Finding 10: Final action-lifecycle delivery failures were discarded

**Severity:** high  
**Confidence:** 9/10  
**Support:** 1/4 reviewers  
**Root Verification:** verified

STARTED delivery errors now fail action handling immediately. Exhausted
completion delivery errors signal the worker supervisor, transition it to
reconnecting/unhealthy state, and preserve an actionable reason.

**Reviewer Supporting:** `reviewer-04`

### Minority Finding 11: Workflow, cron, and scheduled-run REST routes were incorrect

**Severity:** high  
**Confidence:** 9/10  
**Support:** 1/4 reviewers  
**Root Verification:** verified

Workflow lookup now uses the supported list-and-select contract, cron and
scheduled-run creation include the workflow path component, request keys match
the pinned contract, and percent encoding is UTF-8 correct. Route tests pass.

**Reviewer Supporting:** `reviewer-01`

## Additional Remediated Risks Below the Minority Severity Floor

- Removed the server-invalid `Priority::Critical` variant.
- Bounded completed-result and durable-callback state to 1,024 entries.
- Made in-memory result registration race-safe and unregister worker dispatchers
  on shutdown.
- Added `**/target/` to repository ignores; no nested Railway build artifacts are
  exposed to `git add`.
- Removed stale upstream-source and system-`protoc` requirements from the
  Railway build context and pinned its builder to Rust 1.85.
- Corrected the standalone examples README and launcher to use the real package
  and binary names.
- Added an isolated Hatchet Lite/PostgreSQL harness that runs two independent
  workers plus nine live SDK integration paths and proves cleanup on every exit.
- Avoided the Tonic response-header readiness deadlock by starting ListenV2 and
  heartbeats concurrently, matching the official TypeScript SDK lifecycle.
- Added scoped event publication, fail-fast validation for lookback without a
  scope, transport namespace handling for durable event conditions, and
  callback-envelope normalization with correct OR-branch reporting.
- Removed post-Rust-1.85 let-chain syntax exposed by the final Railway build and
  made the guarded publish preflight execute both MSRV checks after all other
  source changes.
- Made the Railway proof harness portable to macOS Bash 3.2 and tolerant of
  occasional non-JSON Railway CLI notices without weakening log matching.

## Split or Disputed Findings

No reviewer contradicted a consensus finding. Reviewers did present different
acceptable fixes for three design questions:

- **Feature boundary:** fully gate gRPC versus make it mandatory. The remediation
  chose mandatory v0.1 transport dependencies and retained a no-op compatibility
  feature, because production transport is part of the intended base SDK.
- **Unsupported management methods:** implement every TypeScript surface versus
  remove/fail explicitly. The remediation implements verified Hatchet v0.94.14
  operations and returns `UnsupportedOperation` for the rest.
- **Worker disconnect policy:** terminate versus reconnect. The remediation
  reconnects with bounded backoff and exposes unhealthy/not-ready state during
  main-stream recovery.

## Likely False Positives and Non-Issues

- The `default:{task}` action prefix looked suspicious to one reviewer, but the
  live Hatchet v0.94.14 Railway evidence shows those exact action IDs completing
  on the intended workers.
- The changed rate-limit routes and response fields match the pinned Hatchet
  v0.94.14 contract.
- The stable REST endpoint for a normal single top-level trigger is valid; the
  accepted defect was its former use for labels and bulk requests.
- No committed bearer token or private key was found. Test-only secret strings
  are synthetic and validate redaction.

## Verification Matrix After Remediation

| Gate | Result |
| --- | --- |
| `cargo fmt --all -- --check` | pass |
| Workspace all-target/all-feature check | pass |
| Workspace all-target/all-feature tests | 94 passed, 9 explicit live ignores |
| Warning-free workspace Clippy | pass |
| No-default-features check and tests | pass |
| Strict workspace rustdoc | pass |
| Rust 1.85 workspace check | pass |
| Standalone Railway tests | 7 passed |
| Standalone Railway warning-free Clippy | pass |
| Rust 1.85 standalone Railway check | pass |
| Root and standalone RustSec audits | no vulnerabilities |
| Exact core/macros crates.io dry-runs | pass |
| Exact SDK archive tests and strict docs | 92 passed, 9 live ignores; docs pass |
| Minimal downstream extracted-archive consumer | pass |
| Official local Hatchet `v0.94.14` SDK suite | 9/9 passed |
| Local two-process workflow and recovery | pass; both post-run health checks passed |
| Local Docker cleanup | complete; no harness containers/network/volume remain |
| `release-plz` configuration in a clean temporary Git repository | pass |
| Shell syntax for release/Railway scripts | pass |
| Final Railway candidate deployments | both corrected worker deployments `SUCCESS` |
| Railway restart workflow proof | runs `c82fa3c2…` and `20627f5c…` completed with matching per-worker action logs |
| Railway health and post-ready logs | both workers healthy; all four services running with zero crashes; zero post-ready worker errors |
| Temporary Railway SSH access | public key removed; zero registered keys after cleanup |

## Final Recommendation

The v0.1.0 candidate is ready to publish. The recommended release sequence is:

1. Review and commit the current worktree.
2. Require the updated CI workflow to pass on that exact commit.
3. From the clean, pushable branch, run `just release`.
4. Confirm the crates.io uploads when prompted; the script publishes core,
   macros, and SDK in dependency order, waits for indexing, then creates the
   single `v0.1.0` tag.

Post-v0.1 priorities are deliberate fault injection for durable-stream
reconnect/eviction, a live multi-parent DAG, and expanding explicitly
unsupported management methods only when their v0.94.14 contracts are verified.

## Caveats

- Four of five requested reviewers completed. Both attempts assigned to the
  missing reviewer slot failed before a valid report; support ratios therefore
  use four as required by the audit procedure.
- All nine credentialed SDK integration tests ran against the final worktree
  and the pinned local Hatchet image. They remain explicit opt-in tests so normal
  offline `cargo test` runs never report credential skips as success.
- The final candidate SDK sources were deployed to both Railway workers and
  exercised before and after a notifications-worker restart. The existing
  official Hatchet orchestrator and PostgreSQL services were reused rather than
  replaced; both read back healthy during the proof.
- Natural action-stream closure exercised and recovered both main worker
  reconnect loops. Forced durable-stream disconnect, eviction, and
  completion-delivery faults were not injected; deterministic tests cover those
  paths.
- A redundant local `linux/amd64` Docker rebuild was cancelled after prolonged
  Apple-Silicon QEMU emulation and is not counted as a pass. Railway completed
  the same Rust-1.85 Dockerfile natively on amd64, while the separate local
  Hatchet Docker suite, real Rust 1.85 checks, and standalone tests all passed.
- The SDK intentionally does not claim full TypeScript management-client parity
  in v0.1; unsupported live operations fail explicitly.
- The actual SDK crates.io upload dry-run cannot resolve its internal crates on
  a first release until core and macros are indexed. The exact SDK archive was
  built and tested locally, and the guarded release script runs the real SDK
  dry-run immediately after indexing those dependencies.

## Audit Files Reviewed

- `./audits/reviewer-01.md`
- `./audits/reviewer-02.md`
- `./audits/reviewer-04.md`
- `./audits/reviewer-05.md`

The individual reports were removed after this synthesis, as required by the
audit workflow. `audit-config.yaml` is retained with this final report.
