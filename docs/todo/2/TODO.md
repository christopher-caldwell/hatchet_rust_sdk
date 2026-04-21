# Hatchet Rust SDK Parity — Session 2 Handoff

## Status Summary

**P0-P5: Complete ✓**  
**P6: In Progress (blocked on REST auth)**

All core SDK features implemented and unit-tested. Integration test framework ready. Validation against real engine blocked by REST API 403 on `/workers`, `/workflows` endpoints.

---

## Current Blocker: REST API 403

REST endpoints return 403 Forbidden even with valid Bearer token:
```
curl -H "Authorization: Bearer $TOKEN" \
  http://192.168.1.162:8888/api/v1/stable/tenants/{id}/workers
# → HTTP 403 Forbidden

curl -H "Authorization: Bearer $TOKEN" \
  http://192.168.1.162:8888/api/v1/stable/tenants/{id}/workflow-runs?limit=5&only_tasks=false
# → HTTP 200 OK (empty)
```

**Hypothesis**: Token missing `/workers`, `/workflows` permissions OR endpoint path mismatch.

**To Fix**:
1. Verify token permissions in Hatchet admin UI
2. Check actual API schema for `/workers` path
3. Update rest_client.rs if paths differ

---

## Session 2 Completed Work

### P3.2: Runs Client Parity
- RunFilter struct with optional workflow_name, status filters
- list_filtered() method with REST backing
- REST-backed replay() and restore_task()

### P3.4: Feature Client REST Parity
- CronsClient.create(), SchedulesClient.create(), FiltersClient.create(), WebhooksClient.create()
- MetricsClient.get_workflow_metrics(), get_queue_metrics()

### Config & REST URL Fix
- Added HatchetConfig.server_url (separate from gRPC host_port)
- Extracted from JWT or HATCHET_CLIENT_SERVER_URL env var
- GrpcTransport now uses correct REST endpoint (8888 not 7077)

### P6.1: Integration Test Framework
- 5 tests: basic_register_and_run, dag_workflow, run_cancel, feature_clients_rest, log_retrieval
- Multi-threaded tokio harness (critical for worker+polling concurrency)
- load_env() and real_client() helpers

---

## Files Changed

- config.rs: server_url extraction
- grpc_transport.rs: REST URL uses server_url
- clients.rs: RunFilter, list_filtered(), REST-backed create() methods
- rest_client.rs: 7 new endpoints
- lib.rs: Export RunFilter
- tests/integration.rs: 5 integration tests
- TODO.parity.json: P3.2, P3.3, P3.4, P5.1, P5.2 marked done

---

## Next Steps

1. Debug REST 403: verify token perms, check API schema
2. Run integration test suite once auth fixed
3. Add parity contract tests (P6.2)
4. Mark P6.1, P6.2 done in TODO.parity.json

---

## Run Integration Tests

```bash
cargo test -p hatchet-sdk --features integration --test integration -- --nocapture
```

Blocked by REST auth 403 — fix that first.
