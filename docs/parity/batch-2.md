# Priority parity batch 2

Batch 2 completes the nine operational/discovery items in [triage.json](triage.json).
The comparison uses official SDK source revision
`af128ec1672adaf8e3cc4afd339ce5c2c6f08c35`; baseline engine contracts come from
v0.94.14 (`301391d9915036ab8e2a5387525572baac980ba8`). Workflow pause additionally
uses the verified v0.111.0 contract (`fd1ba5f4b931b270fcbe8e2ddf29f1646c2d5552`).

| Item | Delivered behavior | Evidence |
| --- | --- | --- |
| O3 | Engine release retrieval, semantic capability checks and worker slot/durable guards. Unknown versions fail explicitly. | Version boundary tests, authenticated gRPC fixture, rejected startup on unknown/old engines, both live engines. |
| R4 | Paginated run search, time/status/workflow/metadata/worker/parent/event filters, output and timestamp summaries, typed task details. Names resolve to IDs with the configured namespace. | Exact query/response tests; live namespaced search and task details. |
| R5 | Bulk or filtered cancellation/replay; named affected task IDs. Single-run replay preserves the existing workflow ID. Empty ID requests are rejected and writes are not retried. | Wire/failure tests; live two-run cancellation, cancelled counts, filtered replay and subsequent execution. |
| M1 | Workflow deletion; version-gated workflow pause/unpause with confirmed returned state. | Baseline live deletion/rejection; modern live queued run held during pause and executed once after unpause. |
| M2 | Filter lookup/update, including optional scope/payload patches. Creation resolves the workflow ID and sends scope. | Exact CRUD requests; live create/read/update/delete. |
| M3 | Real asynchronous CEL evaluation with input/metadata/filter payload; tenant details with retained settings. | False/error response tests; live CEL evaluation and authenticated tenant lookup. |
| M4 | Typed inbound event webhook source/authentication and create/get/update/list/delete. Authentication Debug output is redacted. | Exact wire requests and escaped resource names; live CRUD. Legacy webhook-worker methods remain separately documented. |
| M5 | Trigger-time rescheduling; one bulk request per operation, with successful IDs and per-item errors returned. | Wire tests with partial failures; live reschedule/bulk update/bulk delete. |
| M7 | Task status aggregation, queue details, task statistics and Prometheus text retrieval. | Wire tests preserving cancelled counts, nested queue data and text; live status/queue/stats and disabled-Prometheus rejection. |

The standard suite contains 133 passing workspace tests, 131 passing SDK tests
without default features, and seven standalone tests. Seventeen live tests run
separately: 16 on v0.94.14 and one pause/resume test on v0.111.0. Both live runs
also execute the two-worker example, verify worker health and clean up their
containers and worker processes. Standard offline runs intentionally ignore live
tests; their separate successful runs establish the live claims above.

Validation also covers formatting, strict Clippy/rustdoc, Rust 1.85.1, both
lockfile advisory scans and the release package/README consumer preflight.
Detailed command outputs and source fingerprints are retained locally in
`target/parity-batch2-20261024/`.

## Support limits

- Workflow pause has a conservative SDK floor of v0.111.0, verified against that
  release. This does not identify the first engine release implementing pause.
- The baseline engine's legacy workflow/tenant metrics handlers are deprecated.
  Existing high-level legacy methods return `UnsupportedOperation` for engine
  transports, with supported task-metric replacements in the error. Local test
  counters remain available.
- A real Prometheus federation server is not provisioned here. Successful text
  retrieval is verified against an HTTP fixture, and the actual baseline engine
  verifies the disabled-configuration error. Production scraping also depends on
  engine configuration and any tenant entitlement.
- Advanced management operations require an engine. In-memory CEL evaluation
  was a placeholder and now returns `UnsupportedOperation`.
- New bulk schedule result types preserve partial failure information. Schedule
  updates change trigger times; inputs and workflow identity are immutable.
- Crates remain unpublished. Batches 3 and 4 remain pending; no full TS/Go parity
  or general compatibility with every newer engine release is claimed.

## Playbook evidence ledger

Guidance: installed Rust Playbook v0.3.0, source revision
`14c3ee5cfdb84b7431258c6822205f91d61da8be`.

| Active rule/guide | Applicability and evidence | Classification | Remaining gap |
| --- | --- | --- | --- |
| AS4, use-case outputs | Branch/restore/bulk operations expose named results with actual task/checkpoint IDs. Bulk schedule errors are retained; CEL false results remain successful evaluations. `management_contracts` exercises these distinctions. | COMPLIANT WITH RULE | None identified for this batch. |
| Infrastructure adapters | Management clients delegate to the authenticated REST/gRPC transport. Protocol tests inspect requests, failures and responses; live tests exercise supplied adapters against both named engine releases. | COMPLIANT WITH RULE | Newer features outside this batch remain unimplemented. |
| TEST2, actual infrastructure | Disposable PostgreSQL 16 and exact engine images exercise baseline management and modern pause/resume. The source-port/IPv6 workaround changes test networking, not SDK behavior or engine schemas. | COMPLIANT WITH RULE | No production deployment, load or crash-restart guarantee is established. |
| Testing-evidence guide, Prometheus | Actual engine rejection and fixture text response are distinguished explicitly. | NOT EXERCISED | Production Prometheus federation/data generation. |
| Review-ledger guide | This table links claims to executed environments and separates known limits. Final source fingerprints, command outcomes and package checks are recorded with the local completion report. | COMPLIANT WITH RULE | Batches 3 and 4 are follow-up work, not evidence of completed parity. |
