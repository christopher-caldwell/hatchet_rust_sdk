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
use tokio_util::sync::CancellationToken;

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
    stop_token: CancellationToken,
    join: Option<JoinHandle<()>>,
}

impl HealthServer {
    /// Creates and starts the health server on the given port without
    /// requiring an async constructor at the callsite.
    ///
    /// # Errors
    ///
    /// Returns an error if the TCP listener cannot bind.
    pub fn start_detached(port: u16, state: Arc<RwLock<HealthState>>) -> std::io::Result<Self> {
        let listener = std::net::TcpListener::bind(format!("0.0.0.0:{port}"))?;
        listener.set_nonblocking(true)?;
        let listener = TcpListener::from_std(listener)?;
        let state_clone = Arc::clone(&state);
        let stop_token = CancellationToken::new();
        let stop_clone = stop_token.clone();

        let join = tokio::spawn(async move {
            Self::serve_loop(listener, state_clone, stop_clone).await;
        });

        Ok(Self {
            port,
            state,
            stop_token,
            join: Some(join),
        })
    }

    /// Creates and starts the health server on the given port.
    ///
    /// # Errors
    ///
    /// Returns an error if the TCP listener cannot bind.
    pub async fn start(port: u16, state: Arc<RwLock<HealthState>>) -> std::io::Result<Self> {
        Self::start_detached(port, state)
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

    /// Updates the labels exposed by the endpoint.
    pub async fn set_labels(&self, labels: BTreeMap<String, String>) {
        self.state.write().await.labels = labels;
    }

    /// Stops the server and waits for the accept loop to exit.
    pub async fn stop(&mut self) {
        self.stop_token.cancel();
        if let Some(join) = self.join.take() {
            let _ = join.await;
        }
    }

    async fn serve_loop(
        listener: TcpListener,
        state: Arc<RwLock<HealthState>>,
        stop_token: CancellationToken,
    ) {
        loop {
            let accepted = tokio::select! {
                result = listener.accept() => result,
                _ = stop_token.cancelled() => break,
            };
            let Ok((mut stream, _addr)) = accepted else {
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
