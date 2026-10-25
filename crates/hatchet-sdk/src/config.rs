//! HatchetConfig — connection configuration loaded from environment variables.
//!
//! Mirrors the TypeScript `ConfigLoader` from `clients/hatchet-client/client-config.ts`.
//! Reads `HATCHET_CLIENT_TOKEN`, `HATCHET_CLIENT_HOST_PORT`, `HATCHET_CLIENT_TLS_*`
//! environment variables and parses the JWT to extract `server_url` and `tenant_id`.

use std::fmt;
use std::path::PathBuf;

use crate::error::{Error, Result};

/// TLS configuration for the gRPC connection.
#[derive(Debug, Clone)]
pub enum TlsConfig {
    /// No TLS — plain-text gRPC (for local development).
    Insecure,
    /// System-default TLS using the platform root CA store.
    System,
    /// Custom TLS with explicit certificates.
    Custom {
        ca_cert: PathBuf,
        client_cert: Option<PathBuf>,
        client_key: Option<PathBuf>,
        server_name: Option<String>,
    },
}

impl TlsConfig {
    fn validate(&self) -> Result<()> {
        if let Self::Custom {
            client_cert,
            client_key,
            ..
        } = self
        {
            if client_cert.is_some() != client_key.is_some() {
                return Err(Error::Config(
                    "client certificate and client key must be supplied together".into(),
                ));
            }
        }
        Ok(())
    }
}

/// Log level for the SDK's internal logging.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SdkLogLevel {
    Off,
    Error,
    Warn,
    #[default]
    Info,
    Debug,
    Trace,
}

/// Connection configuration for the Hatchet engine.
///
/// Constructed from environment variables via [`HatchetConfig::from_env`] or
/// directly via [`HatchetConfigBuilder`].
#[derive(Clone)]
pub struct HatchetConfig {
    /// The gRPC server address, e.g. `localhost:7077`.
    pub host_port: String,
    /// The HTTP server URL for REST API calls, e.g. `http://localhost:8888`.
    /// Defaults to the same address as host_port if not set from JWT.
    pub server_url: String,
    /// JWT bearer token for authentication.
    token: String,
    /// TLS configuration.
    pub tls: TlsConfig,
    /// Optional namespace prefix for workflow names and event triggers.
    ///
    /// The SDK normalizes a non-empty value to lowercase with a trailing
    /// underscore, matching the official TypeScript SDK (for example,
    /// `Acme` becomes `acme_`).
    pub namespace: Option<String>,
    /// Tenant ID extracted from the JWT payload.
    pub tenant_id: String,
    /// SDK log level.
    pub log_level: SdkLogLevel,
}

impl HatchetConfig {
    /// Loads configuration from environment variables.
    ///
    /// # Required Environment Variables
    ///
    /// - `HATCHET_CLIENT_TOKEN` — JWT bearer token (used for auth and to extract `server_url`, `tenant_id`)
    ///
    /// # Optional Environment Variables
    ///
    /// - `HATCHET_CLIENT_HOST_PORT` — gRPC host:port (overrides JWT-derived `grpc_broadcast_address`)
    /// - `HATCHET_CLIENT_TLS_STRATEGY` — `none`, `tls`, or `mtls`
    /// - `HATCHET_CLIENT_TLS_ROOT_CA_FILE` — path to CA certificate file
    /// - `HATCHET_CLIENT_TLS_CERT_FILE` — path to client certificate file (for mTLS)
    /// - `HATCHET_CLIENT_TLS_KEY_FILE` — path to client key file (for mTLS)
    /// - `HATCHET_CLIENT_TLS_SERVER_NAME` — TLS server name override
    /// - `HATCHET_CLIENT_NAMESPACE` — namespace prefix
    /// - `HATCHET_CLIENT_LOG_LEVEL` — log level (`off`, `error`, `warn`, `info`, `debug`, `trace`)
    ///
    /// # Errors
    ///
    /// Returns an error if `HATCHET_CLIENT_TOKEN` is missing or the JWT cannot be
    /// decoded to extract `tenant_id`.
    pub fn from_env() -> Result<Self> {
        let token = env_var("HATCHET_CLIENT_TOKEN")
            .map_err(|_| Error::Transport("HATCHET_CLIENT_TOKEN is required".to_owned()))?;

        let jwt_claims = decode_jwt_claims(&token)?;

        let server_url = env_var("HATCHET_CLIENT_SERVER_URL").unwrap_or_else(|_| {
            jwt_claims
                .server_url
                .clone()
                .unwrap_or_else(|| "http://localhost:8888".to_owned())
        });

        let host_port = env_var("HATCHET_CLIENT_HOST_PORT").unwrap_or_else(|_| {
            jwt_claims
                .grpc_broadcast_address
                .clone()
                .unwrap_or_else(|| {
                    jwt_claims
                        .server_url
                        .as_deref()
                        .and_then(|url| {
                            url.strip_prefix("https://").or(url.strip_prefix("http://"))
                        })
                        .unwrap_or("localhost:7077")
                        .to_owned()
                })
        });

        let tenant_id = jwt_claims
            .sub
            .ok_or_else(|| Error::Transport("JWT missing 'sub' (tenant_id) claim".to_owned()))?;

        let tls = parse_tls_config()?;

        let namespace = env_var("HATCHET_CLIENT_NAMESPACE")
            .ok()
            .and_then(|namespace| normalize_namespace(&namespace));

        let log_level = env_var("HATCHET_CLIENT_LOG_LEVEL")
            .ok()
            .map(|level| parse_log_level(&level))
            .unwrap_or_default();

        Ok(Self {
            host_port,
            server_url,
            token,
            tls,
            namespace,
            tenant_id,
            log_level,
        })
    }

    /// Returns a builder for constructing configuration programmatically.
    #[must_use]
    pub fn builder() -> HatchetConfigBuilder {
        HatchetConfigBuilder::default()
    }

    /// Returns the gRPC endpoint URL (with scheme).
    #[must_use]
    pub fn endpoint_url(&self) -> String {
        match &self.tls {
            TlsConfig::Insecure => format!("http://{}", self.host_port),
            TlsConfig::System | TlsConfig::Custom { .. } => {
                format!("https://{}", self.host_port)
            }
        }
    }

    pub(crate) fn token(&self) -> &str {
        &self.token
    }

    pub(crate) async fn connect_channel(&self) -> Result<tonic::transport::Channel> {
        self.tls.validate()?;
        let endpoint_url = self.endpoint_url();
        let mut endpoint =
            tonic::transport::Channel::from_shared(endpoint_url.clone()).map_err(|error| {
                Error::Transport(format!("invalid endpoint URL '{endpoint_url}': {error}"))
            })?;

        // http::Uri retains IPv6 brackets; rustls ServerName requires the raw
        // IP address. Preserve certificate verification for IP SANs, including
        // the System path, rather than handing tonic an invalid default name.
        let tls_domain = endpoint
            .uri()
            .host()
            .unwrap_or_default()
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_owned();
        match &self.tls {
            TlsConfig::Insecure => {}
            TlsConfig::System => {
                endpoint = endpoint
                    .tls_config(
                        tonic::transport::ClientTlsConfig::new()
                            .domain_name(tls_domain.clone())
                            .with_native_roots(),
                    )
                    .map_err(|error| {
                        Error::Transport(format!("system TLS config failed: {error}"))
                    })?;
            }
            TlsConfig::Custom {
                ca_cert,
                client_cert,
                client_key,
                server_name,
            } => {
                let ca_pem = tokio::fs::read(ca_cert).await.map_err(|error| {
                    Error::Transport(format!("failed to read CA cert: {error}"))
                })?;
                let ca = tonic::transport::Certificate::from_pem(ca_pem);
                let mut tls = tonic::transport::ClientTlsConfig::new()
                    .domain_name(tls_domain)
                    .ca_certificate(ca);

                if let (Some(cert_path), Some(key_path)) = (client_cert, client_key) {
                    let cert_pem = tokio::fs::read(cert_path).await.map_err(|error| {
                        Error::Transport(format!("failed to read client cert: {error}"))
                    })?;
                    let key_pem = tokio::fs::read(key_path).await.map_err(|error| {
                        Error::Transport(format!("failed to read client key: {error}"))
                    })?;
                    tls = tls.identity(tonic::transport::Identity::from_pem(cert_pem, key_pem));
                }

                if let Some(name) = server_name {
                    tls = tls.domain_name(name.clone());
                }

                endpoint = endpoint.tls_config(tls).map_err(|error| {
                    Error::Transport(format!("custom TLS config failed: {error}"))
                })?;
            }
        }

        endpoint
            .connect()
            .await
            .map_err(|error| Error::Transport(format!("gRPC connect failed: {error}")))
    }
}

impl fmt::Debug for HatchetConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HatchetConfig")
            .field("host_port", &self.host_port)
            .field("server_url", &self.server_url)
            .field("token", &"[REDACTED]")
            .field("tls", &self.tls)
            .field("namespace", &self.namespace)
            .field("tenant_id", &self.tenant_id)
            .field("log_level", &self.log_level)
            .finish()
    }
}

/// Builder for constructing `HatchetConfig` programmatically.
#[derive(Clone, Default)]
pub struct HatchetConfigBuilder {
    host_port: Option<String>,
    server_url: Option<String>,
    token: Option<String>,
    tls: Option<TlsConfig>,
    namespace: Option<String>,
    tenant_id: Option<String>,
    log_level: Option<SdkLogLevel>,
}

impl fmt::Debug for HatchetConfigBuilder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HatchetConfigBuilder")
            .field("host_port", &self.host_port)
            .field("server_url", &self.server_url)
            .field("token", &self.token.as_ref().map(|_| "[REDACTED]"))
            .field("tls", &self.tls)
            .field("namespace", &self.namespace)
            .field("tenant_id", &self.tenant_id)
            .field("log_level", &self.log_level)
            .finish()
    }
}

impl HatchetConfigBuilder {
    #[must_use]
    pub fn host_port(mut self, host_port: impl Into<String>) -> Self {
        self.host_port = Some(host_port.into());
        self
    }

    #[must_use]
    pub fn server_url(mut self, server_url: impl Into<String>) -> Self {
        self.server_url = Some(server_url.into());
        self
    }

    #[must_use]
    pub fn token(mut self, token: impl Into<String>) -> Self {
        self.token = Some(token.into());
        self
    }

    #[must_use]
    pub fn tls(mut self, tls: TlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    #[must_use]
    pub fn namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = Some(namespace.into());
        self
    }

    #[must_use]
    pub fn tenant_id(mut self, tenant_id: impl Into<String>) -> Self {
        self.tenant_id = Some(tenant_id.into());
        self
    }

    #[must_use]
    pub fn log_level(mut self, log_level: SdkLogLevel) -> Self {
        self.log_level = Some(log_level);
        self
    }

    /// Builds the configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if required fields (`token`, `host_port`, `tenant_id`)
    /// are not set.
    pub fn build(self) -> Result<HatchetConfig> {
        let token = self
            .token
            .ok_or_else(|| Error::Transport("token is required".to_owned()))?;

        // Try to extract tenant_id from JWT if not explicitly provided
        let tenant_id = match self.tenant_id {
            Some(id) => id,
            None => {
                let claims = decode_jwt_claims(&token)?;
                claims
                    .sub
                    .ok_or_else(|| Error::Transport("tenant_id is required".to_owned()))?
            }
        };

        let host_port = self
            .host_port
            .ok_or_else(|| Error::Transport("host_port is required".to_owned()))?;

        let tls = self.tls.unwrap_or(TlsConfig::System);
        tls.validate()?;
        let scheme = match &tls {
            TlsConfig::Insecure => "http",
            _ => "https",
        };
        let server_url = self
            .server_url
            .unwrap_or_else(|| format!("{scheme}://{host_port}"));

        Ok(HatchetConfig {
            host_port,
            server_url,
            token,
            tls,
            namespace: self.namespace.as_deref().and_then(normalize_namespace),
            tenant_id,
            log_level: self.log_level.unwrap_or_default(),
        })
    }
}

pub(crate) fn normalize_namespace(namespace: &str) -> Option<String> {
    let namespace = namespace.trim();
    if namespace.is_empty() {
        return None;
    }

    let mut normalized = namespace.to_lowercase();
    if !normalized.ends_with('_') {
        normalized.push('_');
    }
    Some(normalized)
}

pub(crate) fn apply_namespace(name: &str, namespace: Option<&str>) -> String {
    let Some(namespace) = namespace.and_then(normalize_namespace) else {
        return name.to_owned();
    };

    if name.starts_with(&namespace) {
        name.to_owned()
    } else {
        format!("{namespace}{name}")
    }
}

pub(crate) fn apply_workflow_namespace(name: &str, namespace: Option<&str>) -> String {
    apply_namespace(name, namespace).to_lowercase()
}

// --- JWT decoding (no signature verification, same approach as TS SDK) ---

#[derive(Debug, Clone, Default, serde::Deserialize)]
struct JwtClaims {
    sub: Option<String>,
    server_url: Option<String>,
    grpc_broadcast_address: Option<String>,
}

fn decode_jwt_claims(token: &str) -> Result<JwtClaims> {
    let parts: Vec<&str> = token.split('.').collect();
    if parts.len() < 2 {
        return Err(Error::Transport(
            "invalid JWT: expected at least 2 parts".to_owned(),
        ));
    }

    let payload_b64 = parts[1];

    // JWT uses base64url encoding (no padding)
    let decoded = base64_url_decode(payload_b64)
        .map_err(|error| Error::Transport(format!("JWT base64 decode failed: {error}")))?;

    serde_json::from_slice::<JwtClaims>(&decoded)
        .map_err(|error| Error::Transport(format!("JWT payload parse failed: {error}")))
}

fn base64_url_decode(input: &str) -> std::result::Result<Vec<u8>, base64::DecodeError> {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.decode(input)
}

// --- TLS config parsing ---

fn parse_tls_config() -> Result<TlsConfig> {
    let strategy = env_var("HATCHET_CLIENT_TLS_STRATEGY")
        .unwrap_or_else(|_| "tls".to_owned())
        .to_lowercase();

    match strategy.as_str() {
        "none" => Ok(TlsConfig::Insecure),
        "tls" => match env_var("HATCHET_CLIENT_TLS_ROOT_CA_FILE") {
            Ok(ca_cert) => Ok(TlsConfig::Custom {
                ca_cert: PathBuf::from(ca_cert),
                client_cert: None,
                client_key: None,
                server_name: env_var("HATCHET_CLIENT_TLS_SERVER_NAME").ok(),
            }),
            Err(_) => Ok(TlsConfig::System),
        },
        "mtls" => {
            let ca_cert = env_var("HATCHET_CLIENT_TLS_ROOT_CA_FILE").map_err(|_| {
                Error::Transport("HATCHET_CLIENT_TLS_ROOT_CA_FILE required for mTLS".to_owned())
            })?;
            let client_cert = env_var("HATCHET_CLIENT_TLS_CERT_FILE").map_err(|_| {
                Error::Transport("HATCHET_CLIENT_TLS_CERT_FILE required for mTLS".to_owned())
            })?;
            let client_key = env_var("HATCHET_CLIENT_TLS_KEY_FILE").map_err(|_| {
                Error::Transport("HATCHET_CLIENT_TLS_KEY_FILE required for mTLS".to_owned())
            })?;

            Ok(TlsConfig::Custom {
                ca_cert: PathBuf::from(ca_cert),
                client_cert: Some(PathBuf::from(client_cert)),
                client_key: Some(PathBuf::from(client_key)),
                server_name: env_var("HATCHET_CLIENT_TLS_SERVER_NAME").ok(),
            })
        }
        other => Err(Error::Transport(format!(
            "unknown TLS strategy: '{other}' (expected 'none', 'tls', or 'mtls')"
        ))),
    }
}

fn parse_log_level(level: &str) -> SdkLogLevel {
    match level.to_lowercase().as_str() {
        "off" => SdkLogLevel::Off,
        "error" => SdkLogLevel::Error,
        "warn" | "warning" => SdkLogLevel::Warn,
        "info" => SdkLogLevel::Info,
        "debug" => SdkLogLevel::Debug,
        "trace" => SdkLogLevel::Trace,
        _ => SdkLogLevel::Info,
    }
}

fn env_var(name: &str) -> std::result::Result<String, std::env::VarError> {
    std::env::var(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jwt_decode_extracts_claims() {
        // Build a minimal JWT with base64url-encoded payload
        use base64::Engine;
        let payload = serde_json::json!({
            "sub": "tenant-123",
            "server_url": "https://app.hatchet.run",
            "grpc_broadcast_address": "engine.hatchet.run:443"
        });
        let payload_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&payload).unwrap());
        let token = format!("header.{payload_b64}.signature");

        let claims = decode_jwt_claims(&token).unwrap();
        assert_eq!(claims.sub.as_deref(), Some("tenant-123"));
        assert_eq!(
            claims.server_url.as_deref(),
            Some("https://app.hatchet.run")
        );
        assert_eq!(
            claims.grpc_broadcast_address.as_deref(),
            Some("engine.hatchet.run:443")
        );
    }

    #[test]
    fn jwt_decode_handles_missing_optional_fields() {
        use base64::Engine;
        let payload = serde_json::json!({ "sub": "t1" });
        let payload_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&payload).unwrap());
        let token = format!("h.{payload_b64}.s");

        let claims = decode_jwt_claims(&token).unwrap();
        assert_eq!(claims.sub.as_deref(), Some("t1"));
        assert!(claims.server_url.is_none());
        assert!(claims.grpc_broadcast_address.is_none());
    }

    #[test]
    fn jwt_decode_rejects_malformed_token() {
        assert!(decode_jwt_claims("not-a-jwt").is_err());
    }

    #[test]
    fn builder_validates_required_fields() {
        let result = HatchetConfig::builder().build();
        assert!(result.is_err());
    }

    #[test]
    fn builder_constructs_config() {
        use base64::Engine;
        let payload = serde_json::json!({ "sub": "tenant-1" });
        let payload_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&payload).unwrap());
        let token = format!("h.{payload_b64}.s");

        let config = HatchetConfig::builder()
            .host_port("localhost:7077")
            .token(token)
            .tls(TlsConfig::Insecure)
            .build()
            .unwrap();

        assert_eq!(config.host_port, "localhost:7077");
        assert_eq!(config.tenant_id, "tenant-1");
        assert_eq!(config.endpoint_url(), "http://localhost:7077");
    }

    #[test]
    fn builder_normalizes_namespace_like_typescript_sdk() {
        let config = HatchetConfig::builder()
            .host_port("localhost:7077")
            .tenant_id("tenant-1")
            .token("synthetic-token")
            .tls(TlsConfig::Insecure)
            .namespace("Acme")
            .build()
            .expect("valid config");

        assert_eq!(config.namespace.as_deref(), Some("acme_"));
        assert_eq!(
            apply_namespace("User:Created", config.namespace.as_deref()),
            "acme_User:Created"
        );
        assert_eq!(
            apply_workflow_namespace("Order-Flow", config.namespace.as_deref()),
            "acme_order-flow"
        );
        assert_eq!(
            apply_workflow_namespace("acme_order-flow", config.namespace.as_deref()),
            "acme_order-flow"
        );
        assert_eq!(normalize_namespace("  "), None);
    }

    #[test]
    fn endpoint_url_uses_https_for_tls() {
        use base64::Engine;
        let payload = serde_json::json!({ "sub": "t" });
        let payload_b64 = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(serde_json::to_vec(&payload).unwrap());
        let token = format!("h.{payload_b64}.s");

        let config = HatchetConfig::builder()
            .host_port("engine:443")
            .token(token)
            .tls(TlsConfig::System)
            .build()
            .unwrap();

        assert_eq!(config.endpoint_url(), "https://engine:443");
    }

    #[test]
    fn debug_output_redacts_tokens() {
        let secret = "super-secret-token";
        let builder = HatchetConfig::builder()
            .host_port("localhost:7077")
            .tenant_id("tenant-1")
            .token(secret)
            .tls(TlsConfig::Insecure);

        assert!(!format!("{builder:?}").contains(secret));
        let config = builder.build().expect("valid config");
        let debug = format!("{config:?}");
        assert!(!debug.contains(secret));
        assert!(debug.contains("[REDACTED]"));
    }
    #[tokio::test]
    async fn incomplete_client_identity_is_rejected_before_file_or_network_access() {
        for (client_cert, client_key) in [
            (Some(PathBuf::from("missing-cert")), None),
            (None, Some(PathBuf::from("missing-key"))),
        ] {
            let tls = TlsConfig::Custom {
                ca_cert: PathBuf::from("missing-ca"),
                client_cert,
                client_key,
                server_name: None,
            };
            assert!(matches!(
                HatchetConfig::builder()
                    .host_port("127.0.0.1:9")
                    .tenant_id("t")
                    .token("synthetic")
                    .tls(tls.clone())
                    .build(),
                Err(Error::Config(_))
            ));
            let mut config = HatchetConfig::builder()
                .host_port("127.0.0.1:9")
                .tenant_id("t")
                .token("synthetic")
                .tls(TlsConfig::Insecure)
                .build()
                .unwrap();
            config.tls = tls;
            assert!(matches!(
                config.connect_channel().await,
                Err(Error::Config(_))
            ));
        }
    }
    #[tokio::test]
    async fn ipv6_tls_defaults_reach_a_real_socket_and_send_a_client_hello() {
        use tokio::io::AsyncReadExt;
        let empty_ca = tempfile::NamedTempFile::new().unwrap();
        for tls in [
            TlsConfig::System,
            TlsConfig::Custom {
                ca_cert: empty_ca.path().to_owned(),
                client_cert: None,
                client_key: None,
                server_name: None,
            },
        ] {
            let listener = tokio::net::TcpListener::bind("[::1]:0").await.unwrap();
            let config = HatchetConfig::builder()
                .host_port(listener.local_addr().unwrap().to_string())
                .tenant_id("t")
                .token("synthetic")
                .tls(tls)
                .build()
                .unwrap();
            let mut connection = tokio::spawn(async move { config.connect_channel().await });
            let (mut socket,_)=tokio::time::timeout(std::time::Duration::from_secs(5),async {
                tokio::select! {
                    accepted=listener.accept()=>accepted.unwrap(),
                    result=&mut connection=>panic!("TLS failed before opening IPv6 socket: {result:?}"),
                }
            }).await.unwrap();
            let mut byte = [0];
            tokio::time::timeout(
                std::time::Duration::from_secs(5),
                socket.read_exact(&mut byte),
            )
            .await
            .unwrap()
            .unwrap();
            assert_eq!(
                byte[0], 0x16,
                "expected TLS handshake, never plaintext fallback"
            );
            connection.abort();
            assert!(connection.await.unwrap_err().is_cancelled());
        }
    }
}
