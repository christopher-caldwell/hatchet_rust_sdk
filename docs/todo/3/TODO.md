# Hatchet Rust SDK Parity — Session 3 Handoff

## Status Summary

**P0-P5: Complete ✓**  
**P6: In Progress (blocked on worker registration validation)**

REST API connectivity is fully restored and verified. `integration_feature_clients_rest` and `integration_run_cancel` are passing. Remaining tests are timing out because the worker cannot register with the engine due to strict Action ID validation.

---

## Current Blocker: Action ID Validation

The engine rejects worker registration with:
`Status { code: InvalidArgument, message: "Invalid action ID. Action IDs must be in the format <integrationId>:<verb>" }`

**Findings**:

1.  **Format**: Prepending `default:` to the task name (e.g., `default:echo`) is necessary but not sufficient.
2.  **Characters**: The engine likely forbids dashes in the `<verb>` part of the ID. Current test IDs like `integration-echo-task` are likely causing the rejection.

**To Fix**:

1.  Rename tasks in `tests/integration.rs` to use simple alphanumeric names (e.g., `echo`, `stepa`).
2.  Verify registration success and task pickup.

---

## Session 3 Completed Work

### REST API Resolution

- **Path Correction**: Identified that Hatchet uses a mix of `/api/v1` and `/api/v1/stable`.
    - `v1/stable`: `workflow-runs`, `tasks/cancel`, `tasks/replay`, `filters`.
    - `v1`: `worker`, `workflows`, `crons`, `scheduled`.
- **Parameter Fixes**: Added required `since` and `only_tasks` parameters to run listing.
- **Method Fixes**: `cancel` and `replay` now correctly use the bulk POST body (`externalIds`) rather than path parameters.

### SDK Reliability

- **Result Propagation**: Refactored `clients.rs` to return `Result<Vec<T>>` for all list methods. This prevents silent failures (e.g., tests passing with 0 results when the API actually returned 403).
- **Tracing**: Added internal `eprintln!` tracing to `GrpcTransport` to track gRPC lifecycle (connection, registration, heartbeat, action pickup).

### Integration Test Hardening

- **Timeouts**: Wrapped integration workflow runs in `tokio::time::timeout(30s)` to prevent local test hangs.
- **Cleanup**: Ensured worker shutdown is called even on test failure/timeout.

---

## Files Changed

- **`rest_client.rs`**: Complete overhaul of path construction and method signatures. Added `since` timestamp generation.
- **`clients.rs`**: Updated `RunsClient`, `WorkflowsClient`, `WorkersClient`, etc., to return `Result`. Changed status mapping to `COMPLETED` (engine default).
- **`grpc_transport.rs`**: Added tracing logs and fixed `rest_base` initialization.
- **`transport.rs`**: Updated `Transport` trait and `InMemoryTransport` for `Result` compatibility. Prepended `default:` to action IDs.
- **`integration.rs`**: Updated to handle `Result` types and added timeout protection.

---

## Next Steps

1.  **Fix integration test names**: Strip dashes and simplify (e.g., `integration-echo-task` -> `echo`).
2.  **Validate worker loop**: Once registration passes, confirm `listen` stream receives actions and heartbeat stays alive.
3.  **Cleanup**: Remove `eprintln!` tracing from `grpc_transport.rs`.
4.  **Contract Tests**: Move to P6.2 contract validation.

---

## Run Integration Tests

```bash
cargo test -p hatchet-sdk --features integration --test integration -- --nocapture
```

Currently: 2 PASS, 3 TIMEOUT (due to registration failure).
