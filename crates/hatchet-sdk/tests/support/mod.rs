//! Controlled HTTP/2 and REST fixtures for public transport contract tests.
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use std::time::Duration;

use another_hatchet_sdk::proto::v1::{GetRunDetailsResponse, RunStatus, TaskRunDetail};
use another_hatchet_sdk::retry::RetryConfig;
use another_hatchet_sdk::{Client, GrpcTransport, HatchetConfig, TlsConfig};
use prost::Message;
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tonic::codegen::http;

pub struct Harness {
    pub client: Client<GrpcTransport>,
    pub requests: Arc<tokio::sync::Mutex<Vec<Vec<u8>>>>,
    grpc_task: tokio::task::JoinHandle<()>,
    rest_task: tokio::task::JoinHandle<()>,
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.grpc_task.abort();
        self.rest_task.abort();
    }
}

#[derive(Clone)]
pub struct RestReply {
    pub status: u16,
    pub body: String,
    pub delay: Duration,
}

pub fn graph_reply(failure_handler: bool) -> RestReply {
    let mut shape = vec![json!({
        "taskName":"leaf", "taskExternalId":"task-1", "childrenStepIds":[]
    })];
    if failure_handler {
        shape.push(json!({
            "taskName":"failure", "taskExternalId":"00000000-0000-0000-0000-000000000000",
            "childrenStepIds":[]
        }));
    }
    RestReply {
        status: 200,
        body: json!({
            "run":{"metadata":{"id":"run-1"}, "status":"RUNNING"},
            "shape":shape,
            "workflowConfig":if failure_handler {json!({"onFailureJob":{"ReadableId":"failure"}})} else {json!({})},
            "tasks":[{"taskExternalId":"task-1", "status":"COMPLETED"}],
            "taskEvents":[{"taskId":"task-1", "eventType":"FINISHED", "retryCount":0, "output":"42"}]
        }).to_string(),
        delay: Duration::ZERO,
    }
}

pub fn error_reply(status: u16) -> RestReply {
    RestReply {
        status,
        body: "{}".to_owned(),
        delay: Duration::ZERO,
    }
}

pub fn retry_config(max_retries: u32) -> RetryConfig {
    RetryConfig {
        max_retries,
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(1),
        jitter_min_factor: 1.0,
        jitter_max_factor: 1.0,
        max_elapsed: Some(Duration::from_secs(1)),
        ..RetryConfig::default()
    }
}

pub async fn harness(
    status: RunStatus,
    done: bool,
    replies: Vec<RestReply>,
    retry: RetryConfig,
) -> Harness {
    harness_with_version(status, done, replies, retry, "v0.94.14").await
}

pub async fn harness_with_version(
    status: RunStatus,
    done: bool,
    replies: Vec<RestReply>,
    retry: RetryConfig,
    version: &str,
) -> Harness {
    let version = version.to_owned();
    let listener = tokio::net::TcpListener::bind("[::1]:0").await.unwrap();
    let grpc_addr = listener.local_addr().unwrap();
    let grpc_task = tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let version = version.clone();
            tokio::spawn(async move {
                let mut connection = h2::server::handshake(socket).await.unwrap();
                while let Some(request) = connection.accept().await {
                    let (request, mut respond) = request.unwrap();
                    let details = GetRunDetailsResponse {
                        done,
                        status: status as i32,
                        task_runs: HashMap::from([(
                            "leaf".to_owned(),
                            TaskRunDetail {
                                readable_id: "leaf".to_owned(),
                                external_id: "task-1".to_owned(),
                                status: status as i32,
                                output: Some(b"42".to_vec()),
                                error: (status == RunStatus::Failed)
                                    .then(|| "engine failure".to_owned()),
                                ..Default::default()
                            },
                        )]),
                        ..Default::default()
                    };
                    let payload = if request.uri().path().ends_with("/GetVersion") {
                        another_hatchet_sdk::proto::dispatcher::GetVersionResponse {
                            version: version.clone(),
                        }
                        .encode_to_vec()
                    } else {
                        details.encode_to_vec()
                    };
                    let mut frame = vec![0];
                    frame.extend_from_slice(&u32::try_from(payload.len()).unwrap().to_be_bytes());
                    frame.extend_from_slice(&payload);
                    let headers = http::Response::builder()
                        .header("content-type", "application/grpc")
                        .body(())
                        .unwrap();
                    let mut stream = respond.send_response(headers, false).unwrap();
                    stream.send_data(frame.into(), false).unwrap();
                    let mut trailers = http::HeaderMap::new();
                    trailers.insert("grpc-status", "0".parse().unwrap());
                    stream.send_trailers(trailers).unwrap();
                }
            });
        }
    });
    let listener = tokio::net::TcpListener::bind("[::1]:0").await.unwrap();
    let rest_addr = listener.local_addr().unwrap();
    let requests = Arc::new(tokio::sync::Mutex::new(Vec::new()));
    let received = Arc::clone(&requests);
    let mut replies: VecDeque<_> = replies.into();
    assert!(!replies.is_empty());
    let rest_task = tokio::spawn(async move {
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut headers = Vec::new();
            loop {
                let mut bytes = [0; 1024];
                let read = socket.read(&mut bytes).await.unwrap();
                if read == 0 {
                    break;
                }
                headers.extend_from_slice(&bytes[..read]);
                if headers.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }
            let header_end = headers
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .unwrap()
                + 4;
            let header_text = String::from_utf8_lossy(&headers[..header_end]);
            let content_len = header_text
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then(|| value.trim().parse::<usize>().unwrap())
                })
                .unwrap_or(0);
            while headers.len() < header_end + content_len {
                let mut bytes = [0; 1024];
                let read = socket.read(&mut bytes).await.unwrap();
                if read == 0 {
                    break;
                }
                headers.extend_from_slice(&bytes[..read]);
            }
            received.lock().await.push(headers);
            let reply = if replies.len() > 1 {
                replies.pop_front().unwrap()
            } else {
                replies[0].clone()
            };
            tokio::time::sleep(reply.delay).await;
            let response = format!(
                "HTTP/1.1 {} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                reply.status,
                reply.body.len(),
                reply.body
            );
            let _ = socket.write_all(response.as_bytes()).await;
        }
    });
    let config = HatchetConfig::builder()
        .host_port(grpc_addr.to_string())
        .server_url(format!("http://{rest_addr}"))
        .tenant_id("synthetic")
        .token("synthetic-unused")
        .tls(TlsConfig::Insecure)
        .build()
        .unwrap();
    let transport = GrpcTransport::from_config(config)
        .await
        .unwrap()
        .with_retry_config(retry);
    Harness {
        client: Client::new(Arc::new(transport)),
        requests,
        grpc_task,
        rest_task,
    }
}
