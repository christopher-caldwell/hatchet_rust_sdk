//! HTTP health server for Kubernetes liveness/readiness probes.
//!
//! `/livez` returns 200 while the server can respond. `/readyz` returns
//! 200 only while connected, and 503 otherwise. `/health` preserves the
//! connection-dependent JSON response with worker name and slot information.

use std::collections::BTreeMap;
use std::sync::Arc;

use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tokio::task::{JoinHandle, JoinSet};

const MAX_HEADERS: usize = 8192;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(2);
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
        let port = listener.local_addr()?.port();
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
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                biased;
                _ = stop_token.cancelled() => break,
                Some(_) = connections.join_next(), if !connections.is_empty() => {},
                result = listener.accept() => {
                    if let Ok((stream, _)) = result {
                        connections.spawn(Self::serve_connection(stream, Arc::clone(&state)));
                    }
                }
            }
        }
        connections.abort_all();
        while connections.join_next().await.is_some() {}
    }

    async fn serve_connection(mut stream: TcpStream, state: Arc<RwLock<HealthState>>) {
        // A probe can arrive in several TCP packets. Bound both the accumulated
        // header bytes and the full request/write lifetime; never wait for a body.
        let _ = tokio::time::timeout(REQUEST_TIMEOUT, async {
            let request = Self::read_request(&mut stream).await;
            let (status, content_type, body, head, allow) = match request {
                Ok((method, path)) => {
                    let head = method == "HEAD";
                    if !matches!(path.as_str(), "/health" | "/livez" | "/readyz") {
                        ("404 Not Found", "text/plain", "Not Found".into(), head, false)
                    } else if !matches!(method.as_str(), "GET" | "HEAD") {
                        ("405 Method Not Allowed", "text/plain", "Method Not Allowed".into(), false, true)
                    } else if path == "/livez" {
                        ("200 OK", "text/plain", "ok".into(), head, false)
                    } else {
                        let state = state.read().await;
                        let status = if state.healthy {"200 OK"} else {"503 Service Unavailable"};
                        let body = if path == "/readyz" {
                            if state.healthy {"ok"} else {"not ready"}.into()
                        } else {
                            serde_json::json!({
                                "healthy": state.healthy,
                                "worker_name": state.worker_name,
                                "slots": state.slots,
                                "durable_slots": state.durable_slots,
                                "actions": state.actions,
                                "labels": state.labels,
                            }).to_string()
                        };
                        (status, if path == "/health" {"application/json"} else {"text/plain"}, body, head, false)
                    }
                }
                Err(status) => (status, "text/plain", "Invalid probe request".into(), false, false),
            };
            let response = format!(
                "HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n{}\r\n{}",
                body.len(),
                if allow {"Allow: GET, HEAD\r\n"} else {""},
                if head {""} else {&body},
            );
            let _ = stream.write_all(response.as_bytes()).await;
            let _ = stream.shutdown().await;
            // Drain after the write-side FIN so unread oversized headers/body
            // bytes cannot reset the socket and discard the response on close.
            // The enclosing deadline also bounds peers that keep sending.
            let mut discard = [0; 1024];
            while matches!(stream.read(&mut discard).await, Ok(length) if length > 0) {}
        }).await;
    }

    async fn read_request(stream: &mut TcpStream) -> Result<(String, String), &'static str> {
        let mut buffer = [0; MAX_HEADERS];
        let mut used = 0;
        loop {
            let length = stream
                .read(&mut buffer[used..])
                .await
                .map_err(|_| "400 Bad Request")?;
            if length == 0 {
                return Err("400 Bad Request");
            }
            used += length;
            let mut headers = [httparse::EMPTY_HEADER; 64];
            let mut request = httparse::Request::new(&mut headers);
            match request.parse(&buffer[..used]) {
                Ok(httparse::Status::Complete(_)) => {
                    let method = request.method.ok_or("400 Bad Request")?;
                    let path = request
                        .path
                        .filter(|path| path.starts_with('/'))
                        .ok_or("400 Bad Request")?;
                    return Ok((method.into(), path.into()));
                }
                Ok(httparse::Status::Partial) if used < MAX_HEADERS => {}
                Ok(httparse::Status::Partial) | Err(httparse::Error::TooManyHeaders) => {
                    return Err("431 Request Header Fields Too Large");
                }
                Err(_) => return Err("400 Bad Request"),
            }
        }
    }
}

impl Drop for HealthServer {
    fn drop(&mut self) {
        self.stop_token.cancel();
    }
}
