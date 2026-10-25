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
