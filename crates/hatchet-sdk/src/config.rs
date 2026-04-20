//! HatchetConfig — connection configuration loaded from environment variables.
//!
//! Mirrors the TypeScript `ConfigLoader` from `clients/hatchet-client/client-config.ts`.
//! Reads `HATCHET_CLIENT_TOKEN`, `HATCHET_CLIENT_HOST_PORT`, `HATCHET_CLIENT_TLS_*`
//! environment variables and parses the JWT to extract `server_url` and `tenant_id`.

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

/// Log level for the SDK's internal logging.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SdkLogLevel {
    Off,
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl Default for SdkLogLevel {
    fn default() -> Self {
        Self::Info
    }
}

/// Connection configuration for the Hatchet engine.
///
/// Constructed from environment variables via [`HatchetConfig::from_env`] or
/// directly via [`HatchetConfigBuilder`].
#[derive(Debug, Clone)]
pub struct HatchetConfig {
    /// The gRPC server address, e.g. `localhost:7077`.
    pub host_port: String,
    /// JWT bearer token for authentication.
    pub token: String,
    /// TLS configuration.
    pub tls: TlsConfig,
    /// Optional namespace prefix for workflow names and event triggers.
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

        let namespace = env_var("HATCHET_CLIENT_NAMESPACE").ok();

        let log_level = env_var("HATCHET_CLIENT_LOG_LEVEL")
            .ok()
            .map(|level| parse_log_level(&level))
            .unwrap_or_default();

        Ok(Self {
            host_port,
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
}

/// Builder for constructing `HatchetConfig` programmatically.
#[derive(Debug, Clone, Default)]
pub struct HatchetConfigBuilder {
    host_port: Option<String>,
    token: Option<String>,
    tls: Option<TlsConfig>,
    namespace: Option<String>,
    tenant_id: Option<String>,
    log_level: Option<SdkLogLevel>,
}

impl HatchetConfigBuilder {
    #[must_use]
    pub fn host_port(mut self, host_port: impl Into<String>) -> Self {
        self.host_port = Some(host_port.into());
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

        Ok(HatchetConfig {
            host_port,
            token,
            tls: self.tls.unwrap_or(TlsConfig::System),
            namespace: self.namespace,
            tenant_id,
            log_level: self.log_level.unwrap_or_default(),
        })
    }
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
}
