//! Explicit local-engine sidecar lifecycle, following the official TS embedded API.
//! No engine starts until requested. Normal clients and environment credentials
//! are unchanged. Keep the engine guard alive while its clients/workers run.
mod download;
mod process;
use crate::{Client, Error, GrpcTransport, HatchetConfig, Result, TlsConfig};
use std::{path::PathBuf, time::Duration};

/// Options for the real Hatchet embedded sidecar. The default downloads the
/// latest official sidecar and uses its bundled, persistent Postgres database.
#[derive(Clone)]
pub struct EmbeddedOptions {
    /// Release tag, or latest. Falls back to HATCHET_CLIENT_EMBEDDED_VERSION.
    pub version: Option<String>,
    /// Existing executable; takes precedence over downloading. Falls back to
    /// HATCHET_CLIENT_EMBEDDED_BINARY_PATH.
    pub binary_path: Option<PathBuf>,
    /// Optional SHA-256 hex trust anchor, including for a supplied executable.
    pub checksum: Option<String>,
    /// Defaults to ~/.hatchet/embedded; each release/platform has its own file.
    pub cache_dir: Option<PathBuf>,
    /// Optional existing Postgres; omitted by default for an entirely local run.
    pub database_url: Option<String>,
    pub postgres_data_dir: Option<PathBuf>,
    pub grpc_port: Option<u16>,
    pub api_port: Option<u16>,
    pub start_api: bool,
    pub run_migrations: bool,
    pub rabbitmq_url: Option<String>,
    pub log_level: Option<String>,
    /// Sidecar working directory; the default uses the application's directory.
    pub working_dir: Option<PathBuf>,
    pub ready_timeout: Duration,
    pub shutdown_timeout: Duration,
}
impl Default for EmbeddedOptions {
    fn default() -> Self {
        Self {
            version: None,
            binary_path: None,
            checksum: None,
            cache_dir: None,
            database_url: None,
            postgres_data_dir: None,
            grpc_port: None,
            api_port: None,
            start_api: true,
            run_migrations: true,
            rabbitmq_url: None,
            log_level: None,
            working_dir: None,
            ready_timeout: Duration::from_secs(300),
            shutdown_timeout: Duration::from_secs(30),
        }
    }
}
impl std::fmt::Debug for EmbeddedOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbeddedOptions")
            .field("version", &self.version)
            .field("binary_path", &self.binary_path)
            .field(
                "database_url",
                &self.database_url.as_ref().map(|_| "[REDACTED]"),
            )
            .field(
                "rabbitmq_url",
                &self.rabbitmq_url.as_ref().map(|_| "[REDACTED]"),
            )
            .field("postgres_data_dir", &self.postgres_data_dir)
            .field("start_api", &self.start_api)
            .finish_non_exhaustive()
    }
}
impl EmbeddedOptions {
    fn validate(&self) -> Result<()> {
        for (name, duration) in [
            ("ready_timeout", self.ready_timeout),
            ("shutdown_timeout", self.shutdown_timeout),
        ] {
            if duration.is_zero() || tokio::time::Instant::now().checked_add(duration).is_none() {
                return Err(Error::Config(format!(
                    "embedded {name} must be positive and fit the clock range"
                )));
            }
        }
        for (name, value) in [
            ("database_url", &self.database_url),
            ("rabbitmq_url", &self.rabbitmq_url),
        ] {
            if value.as_ref().is_some_and(|value| value.trim().is_empty()) {
                return Err(Error::Config(format!("embedded {name} must not be empty")));
            }
        }
        Ok(())
    }
}
/// Connection details from the sidecar's private readiness handshake.
#[derive(Clone, serde::Deserialize)]
pub struct EmbeddedConnection {
    token: String,
    tenant_id: String,
    grpc_address: String,
    #[serde(default)]
    api_url: String,
}
impl std::fmt::Debug for EmbeddedConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbeddedConnection")
            .field("token", &"[REDACTED]")
            .field("tenant_id", &self.tenant_id)
            .field("grpc_address", &self.grpc_address)
            .field("api_url", &self.api_url)
            .finish()
    }
}
impl EmbeddedConnection {
    pub fn token(&self) -> &str {
        &self.token
    }
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }
    pub fn grpc_address(&self) -> &str {
        &self.grpc_address
    }
    pub fn api_url(&self) -> &str {
        &self.api_url
    }
    fn validate(&self) -> Result<()> {
        if self.token.is_empty()
            || self
                .token
                .parse::<tonic::metadata::MetadataValue<tonic::metadata::Ascii>>()
                .is_err()
        {
            return Err(Error::Config(
                "embedded handshake has no valid token".into(),
            ));
        }
        uuid::Uuid::parse_str(&self.tenant_id)
            .map_err(|_| Error::Config("embedded handshake has no valid tenant UUID".into()))?;
        if self
            .grpc_address
            .parse::<tonic::codegen::http::uri::Authority>()
            .ok()
            .and_then(|a| a.port_u16())
            .is_none_or(|port| port == 0)
        {
            return Err(Error::Config(
                "embedded handshake gRPC address requires an explicit port".into(),
            ));
        }
        validate_local_url(&format!("http://{}", self.grpc_address))?;
        if !self.api_url.is_empty() {
            validate_local_url(&self.api_url)?;
        }
        Ok(())
    }
    /// Builds a normal SDK configuration from the handshake, ignoring ambient
    /// HATCHET_CLIENT_TOKEN/TLS/server settings.
    /// # Errors
    /// API-less sidecars expose raw connection details but cannot construct this
    /// SDK's full client, which needs REST for result graphs/management.
    pub fn config(&self) -> Result<HatchetConfig> {
        if self.api_url.is_empty() {
            return Err(Error::UnsupportedOperation("the full Rust client requires embedded REST; API-less mode exposes raw connection details only".into()));
        }
        HatchetConfig::builder()
            .token(self.token.clone())
            .tenant_id(self.tenant_id.clone())
            .host_port(self.grpc_address.clone())
            .server_url(self.api_url.clone())
            .tls(TlsConfig::Insecure)
            .build()
    }
}
fn validate_local_url(value: &str) -> Result<()> {
    let url = reqwest::Url::parse(value)
        .map_err(|_| Error::Config("embedded handshake has an invalid local endpoint".into()))?;
    let hostname = url.host_str().unwrap_or_default();
    let local = hostname.eq_ignore_ascii_case("localhost")
        || hostname
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback());
    if url.scheme() != "http"
        || !local
        || url.port_or_known_default().is_none_or(|port| port == 0)
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(Error::Config("embedded handshake endpoints must be local HTTP/loopback addresses with explicit ports".into()));
    }
    Ok(())
}
/// Owns a real engine process. Explicit stop waits for engine and bundled
/// Postgres cleanup. Drop requests shutdown and reaps while a runtime is active;
/// the sidecar also shuts down when the parent's stdin pipe closes.
pub struct EmbeddedEngine {
    process: process::Process,
    connection: EmbeddedConnection,
}
impl std::fmt::Debug for EmbeddedEngine {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbeddedEngine")
            .field("pid", &self.process.id())
            .field("connection", &self.connection)
            .finish()
    }
}
impl EmbeddedEngine {
    /// Resolves/verifies the binary, starts it and waits for its private handshake.
    /// # Errors
    /// Returns binary/checksum/platform/startup errors, stopping a spawned process
    /// on timeout, malformed readiness or cancellation.
    pub async fn start(options: EmbeddedOptions) -> Result<Self> {
        options.validate()?;
        let binary = download::resolve(&options).await?;
        let (process, connection) = process::start(&binary, &options).await?;
        Ok(Self {
            process,
            connection,
        })
    }
    pub fn connection(&self) -> &EmbeddedConnection {
        &self.connection
    }
    pub fn process_id(&self) -> Option<u32> {
        self.process.id()
    }
    /// # Errors
    /// Returns an I/O error if the process state cannot be read.
    pub fn is_running(&mut self) -> Result<bool> {
        self.process.is_running()
    }
    /// # Errors
    /// Returns a connection/configuration error. The guard continues to own the
    /// engine; dropping it still requests shutdown.
    pub async fn client(&self) -> Result<Client<GrpcTransport>> {
        let config = self.connection.config()?;
        let transport =
            tokio::time::timeout(Duration::from_secs(30), GrpcTransport::from_config(config))
                .await
                .map_err(|_| Error::Transport("embedded client connection timed out".into()))??;
        Ok(Client::new(std::sync::Arc::new(transport)))
    }
    /// Gracefully stops and reaps the process. Idempotent, including its result.
    /// # Errors
    /// Returns an error for abnormal exit or a forced kill after the shutdown
    /// deadline, rather than claiming graceful cleanup succeeded.
    pub async fn stop(&mut self) -> Result<()> {
        self.process.stop().await
    }
}
/// A regular SDK client together with its engine guard. Stop workers before
/// stopping the embedded engine, and keep this value alive when cloning clients.
pub struct EmbeddedClient {
    client: Client<GrpcTransport>,
    engine: EmbeddedEngine,
}
impl std::fmt::Debug for EmbeddedClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EmbeddedClient")
            .field("engine", &self.engine)
            .finish()
    }
}
impl std::ops::Deref for EmbeddedClient {
    type Target = Client<GrpcTransport>;
    fn deref(&self) -> &Self::Target {
        &self.client
    }
}
impl EmbeddedClient {
    /// Starts a sidecar and connects its full SDK client.
    /// # Errors
    /// Returns startup or connection errors; failure still requests engine cleanup.
    pub async fn start(options: EmbeddedOptions) -> Result<Self> {
        if !options.start_api {
            return Err(Error::UnsupportedOperation(
                "the full Rust embedded client requires REST; use EmbeddedEngine for API-less mode"
                    .into(),
            ));
        }
        let engine = EmbeddedEngine::start(options).await?;
        let client = engine.client().await?;
        Ok(Self { client, engine })
    }
    pub fn engine(&self) -> &EmbeddedEngine {
        &self.engine
    }
    /// # Errors
    /// Returns the engine's shutdown result.
    pub async fn stop(&mut self) -> Result<()> {
        self.engine.stop().await
    }
}
