//! Controlled gRPC wire peer; all requests and mutation attempts are recorded.
use another_hatchet_sdk::{Client, GrpcTransport, HatchetConfig, RetryConfig, TlsConfig, proto};
use base64::Engine;
use prost::Message;
use std::{collections::VecDeque, sync::Arc, time::Duration};
use tonic::codegen::http;

#[derive(Clone)]
pub struct Reply {
    pub body: Vec<u8>,
    pub code: u16,
    pub details: Vec<u8>,
}
impl Reply {
    pub fn ok(value: impl Message) -> Self {
        Self {
            body: value.encode_to_vec(),
            code: 0,
            details: vec![],
        }
    }
    pub fn error(code: u16, details: Vec<u8>) -> Self {
        Self {
            body: vec![],
            code,
            details,
        }
    }
}
type CapturedRequests = Arc<tokio::sync::Mutex<Vec<(String, Vec<u8>)>>>;
pub struct Harness {
    pub client: Client<GrpcTransport>,
    pub requests: CapturedRequests,
    server: tokio::task::JoinHandle<()>,
}
impl Drop for Harness {
    fn drop(&mut self) {
        self.server.abort();
    }
}
pub async fn harness(version: &str, replies: Vec<Reply>) -> Harness {
    let listener = tokio::net::TcpListener::bind("[::1]:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let requests = Arc::new(tokio::sync::Mutex::new(vec![]));
    let captured = Arc::clone(&requests);
    let replies = Arc::new(tokio::sync::Mutex::new(VecDeque::from(replies)));
    let version = version.to_owned();
    let server = tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let captured = Arc::clone(&captured);
            let replies = Arc::clone(&replies);
            let version = version.clone();
            tokio::spawn(async move {
                let mut connection = h2::server::handshake(socket).await.unwrap();
                while let Some(request) = connection.accept().await {
                    let (mut request, mut respond) = request.unwrap();
                    let captured = Arc::clone(&captured);
                    let replies = Arc::clone(&replies);
                    let version = version.clone();
                    tokio::spawn(async move {
                        assert_eq!(request.headers()["authorization"], "Bearer synthetic");
                        let path = request.uri().path().to_owned();
                        let mut body = vec![];
                        while let Some(data) = request.body_mut().data().await {
                            let data = data.unwrap();
                            request
                                .body_mut()
                                .flow_control()
                                .release_capacity(data.len())
                                .unwrap();
                            body.extend_from_slice(&data);
                        }
                        assert_eq!(body[0], 0);
                        let size = u32::from_be_bytes(body[1..5].try_into().unwrap()) as usize;
                        assert_eq!(size, body.len() - 5);
                        captured
                            .lock()
                            .await
                            .push((path.clone(), body[5..].to_vec()));
                        let reply = if path.ends_with("/GetVersion") {
                            Reply::ok(proto::dispatcher::GetVersionResponse { version })
                        } else {
                            replies
                                .lock()
                                .await
                                .pop_front()
                                .expect("unexpected mutation or retry")
                        };
                        let headers = http::Response::builder()
                            .header("content-type", "application/grpc")
                            .body(())
                            .unwrap();
                        let mut stream = respond.send_response(headers, false).unwrap();
                        if reply.code == 0 {
                            let mut frame = vec![0];
                            frame.extend_from_slice(&(reply.body.len() as u32).to_be_bytes());
                            frame.extend_from_slice(&reply.body);
                            stream.send_data(frame.into(), false).unwrap();
                        }
                        let mut trailers = http::HeaderMap::new();
                        trailers.insert("grpc-status", reply.code.to_string().parse().unwrap());
                        if !reply.details.is_empty() {
                            trailers.insert(
                                "grpc-status-details-bin",
                                base64::engine::general_purpose::STANDARD_NO_PAD
                                    .encode(reply.details)
                                    .parse()
                                    .unwrap(),
                            );
                        }
                        stream.send_trailers(trailers).unwrap();
                    });
                }
            });
        }
    });
    let transport = GrpcTransport::from_config(
        HatchetConfig::builder()
            .host_port(addr.to_string())
            .server_url("http://[::1]:1")
            .tenant_id("synthetic")
            .token("synthetic")
            .tls(TlsConfig::Insecure)
            .build()
            .unwrap(),
    )
    .await
    .unwrap()
    .with_retry_config(RetryConfig {
        max_retries: 3,
        initial_backoff: Duration::from_millis(1),
        max_backoff: Duration::from_millis(1),
        ..Default::default()
    });
    Harness {
        client: Client::new(Arc::new(transport)),
        requests,
        server,
    }
}
