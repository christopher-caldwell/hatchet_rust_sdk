# Worker health probes

A worker configured with `.health_port(port)` serves three endpoints on
`0.0.0.0:port`. A directly created `HealthServer` also accepts port zero and
reports the actual assigned port through `port()`.

| Endpoint | HTTP contract | Use |
| --- | --- | --- |
| `/livez` | 200 whenever the server responds, independent of Hatchet connectivity | Process liveness |
| `/readyz` | 200 while the worker is registered with its required listeners; 503 during startup, disconnect/reconnect or after transport termination | Readiness |
| `/health` | Existing Rust JSON and connection-dependent 200/503 status | Detailed worker diagnostics and existing integrations |

`/livez` does not acquire the shared worker-state lock. A temporary engine outage
therefore removes the worker from readiness without making its liveness probe
fail. It does not prove that every application task is progressing. Worker
shutdown stops the health server; a stopped server is unreachable.

The legacy `/health` JSON retains `healthy`, `worker_name`, `slots`,
`durable_slots`, `actions` and `labels`. The latest TS server returns 200 for its
legacy `/health`; Rust keeps its pre-existing status contract for compatibility.
Both SDKs use the same distinct liveness/readiness behavior. Metrics are a
separate capability, not an alias for a successful health response.

All three routes accept `GET` and `HEAD`. `HEAD` preserves the corresponding
status, headers and GET content length with no response body. Unknown paths
(including query-suffixed paths) return 404; other methods on known routes return
405 with `Allow: GET, HEAD`. Malformed HTTP syntax returns 400 and oversized header sets
return 431. The lightweight server accepts fragmented requests, caps headers at
8 KiB/64 fields, bounds request/connection lifetime to two seconds, and closes
connections after a single response. Incomplete requests that reach the deadline
are closed. Explicit stop aborts and joins outstanding connection tasks; dropping
the server also signals cleanup.

Evidence includes real TCP request/response tests for status transitions, JSON,
HEAD/method/path contracts, fragmentation, malformed/oversized/incomplete
requests, contended state and lifecycle cleanup. The disposable engine restart
proof additionally observes `/readyz` changing 200 → 503 → 200 while `/livez`
remains 200 during the real listener outage. This is operational behavior testing,
not a production traffic-capacity or general-purpose HTTP-server claim.

Reference: [official TS health server](https://github.com/hatchet-dev/hatchet/blob/af128ec1672adaf8e3cc4afd339ce5c2c6f08c35/sdks/typescript/src/v1/client/worker/health-server.ts).
