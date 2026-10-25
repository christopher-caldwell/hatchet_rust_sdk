# Per-error retry delays

Return `RetryAfterError` from a normal or durable task to request a delay before
the engine's next retry. The error converts into the SDK's `Error`:

```rust
use another_hatchet_sdk::{Result, RetryAfterError};
use std::time::Duration;

fn busy() -> Result<()> {
    Err(RetryAfterError::new(Duration::from_millis(1250), "service busy")?.into())
}
```

Durations must use exact whole milliseconds in the signed 64-bit wire range.
Zero requests an immediate retry; fractional or overflowing durations are
rejected. The error remains retryable and preserves the supplied message.
Success and non-retryable failures do not carry a retry-delay override.

The engine owns retry counts, eligibility and maximum delay. This error does not
add attempts to a task with no retries left. Engine v0.114.0 caps retry-after at
30 days by default. Static retry/backoff configuration remains the fallback;
this feature does not implement a separate client retry loop.

Requires engine >= v0.111.0. Before sending an override, the transport checks the
engine release, bounding the entire capability probe to two seconds. On an older, unrecognized or unreachable version endpoint, it
omits the override and writes an SDK warning that the configured retry policy
applies. The failure is still reported. This follows the official clients'
fallback behavior without claiming the requested delay was honored.

Failure events carry `retry_after_ms` on normal/durable actions and batch member
failures. Preview batch tasks still have retries forced to zero, so their
retry-delay field does not grant extra attempts. `InMemoryTransport` does not
simulate engine retry scheduling.

References: [TS RetryAfterError](https://github.com/hatchet-dev/hatchet/blob/af128ec1672adaf8e3cc4afd339ce5c2c6f08c35/sdks/typescript/src/v1/task.ts),
[TS fallback warning](https://github.com/hatchet-dev/hatchet/blob/af128ec1672adaf8e3cc4afd339ce5c2c6f08c35/sdks/typescript/src/v1/client/worker/worker-internal.ts),
[Go public error](https://github.com/hatchet-dev/hatchet/blob/aef20b00755da5b1e7e69197e7189848309afb20/sdks/go/errors.go),
[engine delay cap](https://github.com/hatchet-dev/hatchet/blob/aef20b00755da5b1e7e69197e7189848309afb20/internal/services/controllers/task/controller.go).
