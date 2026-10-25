# Operational parity and current reference tracking

This batch adds shared asynchronous task middleware, native OpenTelemetry spans
and W3C propagation, engine-owned workflow idempotency, rate-limit pause and
explicit worker routing on normal/bulk submission. The
[39-row support matrix](feature-support.md) is the maintained comparison; its
machine-readable source is [features.json](features.json).

## Official references

The npm registry reports TypeScript **1.43.0**. Its published tarball was verified
against its SHA-512 integrity value, and its published task declarations,
administration/rate-limit implementation and instrumentation were inspected.
The package has no `gitHead`; source references therefore identify the inspected
main revision explicitly, rather than pretending the registry supplied one.

The Go proxy reports module `github.com/hatchet-dev/hatchet` **v0.114.0**, revision
`aef20b00755da5b1e7e69197e7189848309afb20`. Its `sdks/go` and `api-contracts` trees
match official main `af128ec1672adaf8e3cc4afd339ce5c2c6f08c35`, checked again for
this batch. The official main TS manifest is also 1.43.0. GitHub's latest-release
endpoint separately reports v0.110.5; it is not used to infer npm/Go publication.
[upstream.json](upstream.json) records the endpoints, exact revisions and artifact
integrity. Future refreshes cannot pass matrix validation without updating the
explicit implementation-review versions.

## Behavior and executed evidence

| Contract | Supplied implementation | Evidence |
| --- | --- | --- |
| Shared middleware | WorkerBuilder middleware wraps both task kinds in registration order. Layers can transform input/output, reject or short-circuit. Execution identity remains immutable; Next is consumed once. | Normal/durable ordering and rejection tests. |
| Native tracing | Submission producer/task consumer spans; per-poll async context; W3C traceparent/tracestate on normal and durable children; error and aborted-future completion. | Captured real OTel spans prove parent/child linkage, concurrent isolation, false-success prevention and completion on abort. Both live engines exercise middleware/traces across a child workflow. |
| Idempotency | Workflow CEL key, whole-millisecond TTL and TTL/STATUS policy. Named single/bulk errors preserve existing IDs and partial successes; collisions are not retried. | Exact gRPC requests/error trailers; live TTL expiration, STATUS release after completion and bulk partial success on v0.111.0 and v0.114.0. In-memory registration rejects this engine-owned guarantee. |
| Rate pause | Exact rendered key and requested timestamp; actual engine acknowledgment; no implicit mutation retry. | Timestamp boundary/protocol tests and live v0.114.0 work held queued until pause expiry. Earlier pause requests do not shorten the hold. |
| Explicit routing | Single and bulk gRPC dispatch retain desired worker ID. Workflow hard/soft policy is separate. | Wire tests plus two live v0.114.0 workers selected by identity for single and bulk runs. |
| Recovery/fanout | Supplied transport, worker and durable listener adapters; no reviewer-added SDK adapter or engine schema changes. | 128 durable children preserve every result. A pending durable wait resumes after engine restart with the original checkpoint and task ID, on both baseline/current engines. |
| Support claims | Versioned source snapshot, named existing tests, runtime support-floor matching and deterministic Markdown. | Six negative guard tests; offline render validation; actual upstream drift check. |

Final workspace validation passes **143** tests; the SDK without default features
passes **140**; the standalone example passes **7**. Six Python support-claim guard
tests also pass. Formatting, strict Clippy/rustdoc and Rust **1.85.1** checks pass.
Both lockfiles have zero RustSec vulnerabilities or warnings after refreshing the
advisory database.

Live execution is separate from ordinary offline tests, which intentionally
ignore infrastructure tests:

| Engine | Live tests executed | Additional proof |
| --- | --- | --- |
| v0.94.14 | 19 | Two-worker example, durable engine restart, post-run worker health and cleanup. |
| v0.111.0 | 3 | Conservative pause/idempotency support-floor probes, two-worker example and cleanup. |
| v0.114.0 | 24 | Full baseline suite plus modern features, durable engine restart, two-worker example, health and cleanup. |

These are **46 live executions**, including cases repeated across versions, not
46 distinct feature guarantees. Both CI target images use verified multiarch
manifest digests; local runs use their verified arm64 layers. The ExpressVPN
source-port workaround affects test networking only; no VPN/system settings or
engine schemas were changed. Detailed logs/reports and source fingerprints are
retained locally under `target/parity-batch3-20261025/`.

## CI and remaining limits

The new parity workflow validates matrix evidence on changes, supplies exact-image
baseline/current live jobs, and checks upstream version drift weekly or manually.
Drift fails the job and uploads observed versions; it does not edit support claims
or post messages. Remote GitHub Actions has not run for this local commit. The
same supplied live suites have passed locally.

The matrix deliberately retains nine partial rows and five missing rows. The
partial rows include broader semantics/evidence limits as well as application
exporter configuration; counts are not a full-parity percentage. Automatic
Hatchet collector authentication/reparenting and automatic Hatchet attributes on
all application spans are not installed by the Rust SDK. Applications own their
OpenTelemetry 0.32 provider/exporter.

The six remaining triaged feature areas are server-coalesced batch tasks,
per-error retry delay, durable topic streams, embedded real-engine lifecycle,
broader runtime schema validation and distinct liveness/readiness semantics.
This batch does not execute the official TS/Go SDK suites, cover worker-process
crashes, exhaust every scheduling interleaving or establish production capacity.
Crates remain unpublished; exact archive tests/docs and the packaged README
consumer are checked by the release preflight, while the SDK registry dry run
remains deferred until its sibling crates are published.

## Playbook evidence ledger

Guidance: installed Rust Playbook v0.3.0, source revision
`14c3ee5cfdb84b7431258c6822205f91d61da8be`.

| Active rule/guide | Applicability and evidence | Classification | Remaining gap |
| --- | --- | --- | --- |
| AS4, use-case outputs | RateLimitPauseResult returns the engine time; typed collisions retain existing and successful run IDs. Protocol and real-engine tests distinguish partial success from failure. | COMPLIANT WITH RULE | Malformed/unknown collision details fail without retry rather than fabricating IDs. |
| Infrastructure adapters | Additive official protobuf contracts feed existing authenticated adapters. Engine-owned idempotency/pause are guarded; in-memory implementations reject unsupported guarantees. | COMPLIANT WITH RULE | SDK floors are conservative verified release floors, not earliest-release claims. |
| TEST2, actual infrastructure | Exact baseline/current engine images and PostgreSQL 16 execute the supplied adapters, restart, checkpoint, fanout and queued-work scenarios. | COMPLIANT WITH RULE | No worker-process crash/load guarantee. |
| Testing-evidence guide | Official source review, protocol fixtures, captured native OTel spans, live infrastructure runs and remote CI availability are explicitly distinguished. | COMPLIANT WITH RULE | TS/Go runtime equivalence and production collector/federation behavior are NOT EXERCISED. |
| Review-ledger guide | Matrix rows link to named code/tests, record support floors and retain partial/missing statuses. Drift checks require explicit source review. | COMPLIANT WITH RULE | Batch 4 remains pending. |
