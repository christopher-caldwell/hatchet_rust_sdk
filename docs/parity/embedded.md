# Embedded real-engine mode

`embedded::EmbeddedClient::start` explicitly starts a managed local Hatchet
sidecar and returns a normal client guarded by its lifetime. No engine starts
when importing the SDK or constructing ordinary clients. The sidecar generates
a tenant/token and, by default, runs bundled Postgres with persistent data.

```rust,no_run
use another_hatchet_sdk::{embedded::*, *};
use serde_json::Value;
use std::{path::PathBuf, time::Duration};

async fn example() -> Result<()> {
    let mut local = EmbeddedClient::start(EmbeddedOptions {
        version: Some("v0.110.5".into()),
        postgres_data_dir: Some(PathBuf::from("./.hatchet-pg")),
        ..Default::default()
    }).await?;
    let task = local.task::<Value, Value>("echo");
    local.register_workflow(
        WorkflowDeclaration::<Value, Value>::new("echo-workflow")
            .step(&task, TaskConfig::default())
    ).await?;
    let worker = local.worker("local-worker").register_task(
        &task, |input, _: InMemoryContext<GrpcTransport>| async { Ok(input) }
    ).build().start()?;
    worker.wait_until_ready(Duration::from_secs(15)).await?;
    let output = local.workflow::<Value, Value>("echo-workflow")
        .run(serde_json::json!({"local": true}), RunOpts::default()).await?;
    assert_eq!(output, serde_json::json!({"local": true}));
    worker.shutdown().await?;
    local.stop().await?;
    Ok(())
}
```

Keep the guard alive when cloning clients. Explicit `stop()` waits for exit and
reaps the sidecar, including its own bundled-Postgres cleanup. Repeated stops
retain the result. A cancelled stop preserves the original shutdown deadline.
Drop requests shutdown and reaps while a runtime is active; the inherited stdin
pipe also closes when the parent exits. A readiness failure or cancelled startup
requests process cleanup. Forced shutdown reports an error; it does not promise
graceful database/profile cleanup. Use explicit awaited stop for that guarantee.

`EmbeddedOptions` forwards database URL, persistent Postgres directory, gRPC/API
ports, API/migration controls, RabbitMQ and log level. `working_dir` affects only
the child. Multiple bundled engines must use distinct data directories; default
data-directory behavior belongs to the upstream sidecar. Existing-database and
RabbitMQ fleets are configurable but are not exercised by this repository.

Binary selection follows explicit `binary_path`, then
`HATCHET_CLIENT_EMBEDDED_BINARY_PATH`, otherwise a release download. Version
selection follows explicit `version`, `HATCHET_CLIENT_EMBEDDED_VERSION`, then
latest. Downloaded releases are cached under `~/.hatchet/embedded/<version>` by
default; `cache_dir` overrides this. Cached bytes are verified on every start.
A supplied binary skips downloading and is verified when `checksum` is provided.

Release downloads use HTTPS, the official platform asset and `checksums.txt`;
partial downloads install atomically only after SHA-256 verification. Executables
live in digest-specific subdirectories, so a concurrent same-tag release update
cannot replace a pinned caller's verified executable before it is spawned. A verified
pinned cache can fall back to its saved digest when checksum lookup is offline.
An explicit SHA-256 `checksum` replaces the release checksum as the trust anchor;
pin it together with the version. Latest resolution still needs the network.
Release downloads support macOS/Linux on arm64/amd64. Custom native binaries can
be supplied; other host-platform behavior is not exercised.

The readiness handshake lives in a private temporary directory, is bounded and
validated, and is removed after readiness. Token and database/broker credentials
are redacted from SDK debug/error output. Endpoint validation restricts handshake
addresses to loopback HTTP/gRPC. Handshake configuration directly supplies the
client; ambient API token, remote server and TLS settings are not substituted.
No environment variables are overwritten by the Rust wrapper.

`EmbeddedEngine::start` exposes a lower-level guard and connection details.
`start_api: false` boots a gRPC-only engine, but its full Rust client construction
is explicitly unsupported because run graphs and management need REST. The
convenience `EmbeddedClient` rejects that combination before spawning anything.
Callers can use the raw authenticated connection details for gRPC-only work.

The official sidecar registers its temporary `embedded` Hatchet CLI profile and
removes its token-matched registration during graceful shutdown. That behavior
is upstream-owned; other profiles/default settings are preserved by its store.

## References and evidence

At inspection, TS is **1.43.0** and Go is **v0.114.0**, while the latest published
sidecar is **v0.110.5** (release source
`3dc26f939dfc5c1072c35dedc65bf51313d665a7`). Sidecar main is independently pinned
at `f0b16756371f27279f3114fa72c83f9b56176811`. SDK feature floors still apply:
using the default published sidecar does not enable newer v0.111/v0.114 APIs.
The drift checker tracks both the sidecar release/assets and source main, and
requires an explicit implementation review after changes.

Eight controlled executable lifecycle tests cover argument forwarding, private
handshake/configuration, redaction, early exit, bad readiness, cancellation,
timeout/forced stop, API-less controls and cancelled-stop deadlines. Three HTTP
release tests exercise latest resolution, digest mismatch, cache tampering,
offline/pinned reuse and concurrent atomic installation, including different same-tag digest policies.

Actual official darwin-arm64 sidecar tests run workflows without Docker or a
supplied API token, stop/restart bundled Postgres while retaining run history and
registered workflows, authenticate gRPC-only mode and verify process/database
exit. CI supplies a separately pinned Linux-amd64 native job; remote execution
awaits pushing. External DB/RabbitMQ fleets, other OS/architectures, parent-process
SIGKILL during initialization and forced-kill database cleanup are NOT EXERCISED.

Sources: [TS sidecar implementation](https://github.com/hatchet-dev/hatchet/blob/af128ec1672adaf8e3cc4afd339ce5c2c6f08c35/sdks/typescript/src/v1/embedded.ts),
[Go entry point](https://github.com/hatchet-dev/hatchet/blob/aef20b00755da5b1e7e69197e7189848309afb20/sdks/go/embedded.go),
[sidecar release](https://github.com/hatchet-dev/hatchet-embedded/releases/tag/v0.110.5),
[sidecar source](https://github.com/hatchet-dev/hatchet-embedded/tree/3dc26f939dfc5c1072c35dedc65bf51313d665a7),
[official embedded guide](https://docs.hatchet.run/v1/embedded).
