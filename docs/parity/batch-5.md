# Recovery and operational verification follow-up

The current official SDK/source/artifact snapshot was checked again and remains
current. Implementation and verification follow the installed Rust Playbook. Each
cycle receives an independent read-only review before the next begins.

## Sticky routing recovery and exact workflow lookup

A new disposable-engine regression verifies HARD routing before restart, then
reconnected workers A/B, a fresh worker C, a new workflow/action after restart, and
bulk dispatch to C. It refreshes routing targets after re-registration rather than
reusing dead worker IDs. Baseline v0.94.14 and current v0.114.0 pass, including
cleanup and the existing two-worker example. The observed post-reconnect routing
window was approximately **61–63 seconds**, within the explicit 90-second test
deadline. No production latency SLA follows from that bound.

A separate pinned published **TS 1.43.0** comparison submitted four equivalent
runs alongside Rust to the same Rust workers. Both submission paths completed
after approximately 60 seconds and reached the exact intended worker IDs. This
establishes shared observed delay and eventual delivery in that scenario; it
does not independently isolate Rust workers from the engine, or prove a specific
internal lease mechanism. Original 45-second failures remain preserved. S1 stays
partial for prompt recovery/attribution; no HARD semantics were weakened and no
SDK sleep/retry was added to conceal the delay. Full official SDK suites were not
run. Local comparison evidence: `target/parity-batch5-20261025/sticky/`.

The comparison's cleanup exposed an actual SDK defect: the engine's workflow name
filter is a substring search, but lookup requested only one row. A newer sibling
name could hide the exact workflow and break get/delete/pause/run-filter lookup.
Lookup now pages 100 results at a time and accepts only an exact normalized name.
Absent matches and repeated pages stop without a mutation. Two new protocol tests
cover a second-page exact match, prefix-only absence, exact mutation ID and ignored
pagination. The real base-plus-`-cold` sibling scenario now cleans up successfully.

An independent reviewer checked the engine's actual limit/offset contract, routing
assertions, smoke wiring and diagnostic interpretation, independently ran all
**13** management contracts, and cleared the changes. Strict Clippy passes. The
new ignored integration regression is wired into the existing disposable recovery
suite on both CI engine targets. Remote CI still awaits a push.

## Worker process crash and replacement

A new ignored disposable-engine test launches actual subprocesses of the exact
SDK test executable. The original worker starts one normal and one durable task,
and records acknowledgement only after its durable memo RPC returns. The parent
sends **SIGKILL**, checks signal 9 on Unix and reaps the child, then starts a
replacement worker process. No synthetic transport or graceful shutdown substitutes
for the crash. Both tasks complete on **retry 1**, retain their original task IDs,
and report the replacement PID; the durable memo still contains the killed PID.

The scenario passes on **v0.94.14** and **v0.114.0**, each in approximately 11
seconds, with complete smoke cleanup. It exercises recovery through explicitly
configured **10-second execution timeout/retries=1**. Immediate death detection,
automatic recovery without a retry policy, and exactly-once external side effects
are not claimed. The replacement worker shuts down cleanly and is awaited;
kill-on-drop protects both owned children on test failure. Private child stdout
logs keep helper test reports out of the smoke execution count.

Independent review checked subprocess ownership, crash/commit ordering, memo/task
identity, retry scope, isolation and CI wiring. The reviewer independently ran
the ordinary no-op helper and strict Clippy; no actionable findings remained.
The parent's Rust **1.85.1** check also passes. The actual crash parent is included
in the existing baseline/current disposable recovery targets; no external runtime
is needed. V1 remains partial for remote CI and its other explicit limits.
Evidence: `target/parity-batch5-20261025/crash/`.

## Native engine TLS and configuration fixes

The runner now supports opt-in native engine **gRPC TLS**, with an owned
short-lived CA/leaf fixture, server TLS configuration and SDK trust configuration.
There is no TLS terminator. Both **v0.94.14** and **v0.114.0** pass **20** live
checks each: 19 ordinary SDK contracts plus the TLS case. The latter checks
trusted custom CA, rejected wrong CA and wrong hostname, an authenticated workflow
round trip, default native-store rejection of the fresh CA and successful native
root loading through a child-process-only `SSL_CERT_FILE`. Host trust stores,
user environment and VPN settings are never changed; owned fixture keys are
removed by cleanup. REST remains local HTTP.

The first real run exposed tonic 0.14.2 passing a bracketed IPv6 URI host to
rustls as the default verification name. The SDK now supplies the unbracketed IP
in both Custom and System paths, preserving IP SAN checks and explicit hostname
overrides. A second defect silently omitted a requested client identity when
only its certificate or key was supplied. Builder validation and channel creation
now reject incomplete pairs before file/network access, including configurations
mutated through the public TLS field.

Independent review found a test gap: canonical CI connects to `localhost`, so its
live case alone would miss regression of the IPv6 fix. A new network-level unit
case requires both default TLS modes to open an IPv6 listener connection and send
a TLS ClientHello, then aborts/joins the deliberately incomplete handshake. The
old bracket failure occurs before connecting and fails that regression. Both live
CA-verification and wire tests complement each other; the latter does not claim
a completed verified handshake by itself. The reviewer independently reran the
config/wire/helper checks and strict Clippy, inspected both live profiles and
cleared all findings.

A pinned current-engine TLS CI profile is supplied alongside the existing plain
profiles; remote CI remains unexecuted. C13 now records the exercised gRPC TLS
contract as supported, with explicit limits for HTTPS REST, OS/platform fleets,
trust-store installation, full mTLS lifecycle and rotation. Those operations were
not exercised by this fixture. Evidence: `target/parity-batch5-20261025/tls/`.

## Final combined-validation corrections

The first plaintext run caught a macOS Bash 3.2 `set -u` incompatibility: expanding
an empty TLS argument array aborted the default non-TLS fixture. The Docker call
now uses a conditional array expansion that emits zero arguments for an empty
array and retains quoted word boundaries otherwise. The TLS reviewer independently
verified macOS Bash 3.2 cases for empty/unset arrays, spaces, empty string elements
and literal glob characters. This is a runner correction, not a TLS-verification
change.

A subsequent full run exposed a pre-existing batch self-cancellation test race:
the gRPC terminal read returned cancellation while the independent REST OLAP
projection still reported Running. The test previously accepted any output error
and immediately asserted REST Cancelled. It now requires the exact authoritative
cancellation error and waits up to 15 seconds for REST to converge, failing on a
successful output, any other terminal status or deadline. An independent reviewer
checked the separate read paths, confirmed this strengthens the outcome assertion
and independently ran strict Clippy. Product cancellation behavior is unchanged.
Original failed-run evidence is retained under the final local evidence directory.

## Stopping point and next priorities

This pass fixed three SDK defects (exact workflow lookup, incomplete TLS identity,
and default IPv6 TLS names), added actual crash/TLS/restart evidence and corrected
the runner/projection test issues exposed by combined verification. All independent
review findings are closed. The remaining local work has lower immediate return
than these correctness/recovery checks:

1. Isolate prompt sticky restart behavior using fully official workers and engine
   instrumentation; eventual delivery is proven, the delay's origin/SLA is not.
2. Design and exercise automatic Hatchet collector/exporter setup against a real
   collector, including authentication, trace parenting, attribute propagation and
   provider ownership. Existing application-owned OpenTelemetry remains supported.
3. Broaden scheduling, retry/backoff, cron/time-zone, label/slot and Prometheus
   federation fixtures; exhaustive interleavings and production capacity are not
   asserted by more local unit tests.
4. Push and run the supplied remote CI/platform profiles, then handle publication
   in dependency order. No push or publication is performed by this pass.

The matrix retains eight partial rows instead of treating additional test volume
as exhaustive parity. Further collector/platform/load integration deserves its
own focused work rather than extending this local verification loop indefinitely.

## Final verification

Final workspace tests pass **191**, SDK tests without default features pass
**186**, standalone tests pass **7**, and Python support-claim guards pass **9**.
Format, strict Clippy/rustdoc, doctests and Rust **1.85.1** workspace/standalone
checks pass; the final cancellation-test change also passes its targeted strict
Clippy and MSRV check. Exact packaged SDK tests/docs, its README consumer and
core/macros registry dry runs pass. SDK registry dry run remains deferred until
those unpublished sibling versions exist. Dependency locks are unchanged, so
the prior recorded RustSec zero-vulnerability/warning audits still describe the
same locked dependencies. No crate was published.

The final ordinary/recovery suites pass **34** current-engine and **22** baseline
checks. Separate native TLS profiles pass **20** per engine. These total **96 live
test executions across four profiles**, with overlap between profiles, plus each
profile's two-worker example and complete cleanup. There are no new ignored tests
counted as live executions. The earlier N4 native sidecar proof is retained as
historical evidence and was not rerun in this pass.

The 39-row support matrix is **31 supported / 8 partial / 0 missing**. Official
package/source/artifact versions were queried again and are current; the snapshot
check timestamp was refreshed without changing reviewed versions. All independent
review findings are closed. Full logs, reports and packaged-consumer evidence are
under `target/parity-batch5-20261025/final/`; profile reports are retained beside
their corresponding crash/TLS/routing evidence.
