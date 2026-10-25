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
