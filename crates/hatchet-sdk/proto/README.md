# Vendored Hatchet protocol definitions

These protocol definitions are vendored from Hatchet's `api-contracts`
directory so crates.io consumers can build the SDK without a source checkout
or a system `protoc` installation.

Source: https://github.com/hatchet-dev/hatchet

The files remain available under Hatchet's MIT license. See
[`UPSTREAM-LICENSE`](UPSTREAM-LICENSE).

The baseline protocol remains v0.94.14. The following additive wire definitions
are copied from verified official releases, retaining existing field numbers:

| Contract | Source |
| --- | --- |
| Workflow idempotency config, TTL/STATUS policy, single/bulk collision details | `api-contracts/v1/workflows.proto`, v0.111.0 (`fd1ba5f4b931b270fcbe8e2ddf29f1646c2d5552`) |
| Batch task registration, START_BATCH assignment and SendBatchActionEvent lifecycle RPC | `api-contracts/v1/workflows.proto` and `api-contracts/dispatcher/dispatcher.proto`, v0.114.0 (`aef20b00755da5b1e7e69197e7189848309afb20`) |
| PauseRateLimit RPC and request/response messages | `api-contracts/workflows/workflows.proto`, v0.114.0 (`aef20b00755da5b1e7e69197e7189848309afb20`) |

These are guarded by `EngineFeature` support floors and exercised against real
engine releases. The vendored definitions are not a claim that every newer
protocol feature is implemented.
