//! HTTP health server for Kubernetes liveness/readiness probes.
//!
//! Mirrors the TS SDK's `health-server.ts`. Exposes `GET /health` returning
//! JSON with worker status, name, and slot information.

use std::collections::BTreeMap;
use std::sync::Arc;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

/// Health server state, shared between the worker and the HTTP handler.
#[derive(Debug, Clone)]
pub struct HealthState {
    pub worker_name: String,
    pub slots: usize,
    pub durable_slots: usize,
    pub actions: Vec<String>,
    pub labels: BTreeMap<String, String>,
    pub healthy: bool,
}

/// A lightweight HTTP health server.
pub struct HealthServer {
    port: u16,
    state: Arc<RwLock<HealthState>>,
    _join: Option<JoinHandle<()>>,
}

impl HealthServer {
    /// Creates and starts the health server on the given port.
    ///
    /// # Errors
    ///
    /// Returns an error if the TCP listener cannot bind.
    pub async fn start(port: u16, state: Arc<RwLock<HealthState>>) -> std::io::Result<Self> {
        let listener = TcpListener::bind(format!("0.0.0.0:{port}")).await?;
        let state_clone = Arc::clone(&state);

        let join = tokio::spawn(async move {
            Self::serve_loop(listener, state_clone).await;
        });

        Ok(Self {
            port,
            state,
            _join: Some(join),
        })
    }

    /// Returns the port the server is listening on.
    #[must_use]
    pub fn port(&self) -> u16 {
        self.port
    }

    /// Updates the health state.
    pub async fn set_healthy(&self, healthy: bool) {
        self.state.write().await.healthy = healthy;
    }

    async fn serve_loop(listener: TcpListener, state: Arc<RwLock<HealthState>>) {
        loop {
            let Ok((mut stream, _addr)) = listener.accept().await else {
                continue;
            };

            let state = Arc::clone(&state);
            tokio::spawn(async move {
                let mut buf = vec![0u8; 4096];
                let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut buf).await;

                let state = state.read().await;
                let body = serde_json::json!({
                    "healthy": state.healthy,
                    "worker_name": state.worker_name,
                    "slots": state.slots,
                    "durable_slots": state.durable_slots,
                    "actions": state.actions,
                    "labels": state.labels,
                });

                let body_str = body.to_string();
                let status = if state.healthy {
                    "200 OK"
                } else {
                    "503 Service Unavailable"
                };
                let response = format!(
                    "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body_str}",
                    body_str.len(),
                );

                let _ = stream.write_all(response.as_bytes()).await;
            });
        }
    }
}
