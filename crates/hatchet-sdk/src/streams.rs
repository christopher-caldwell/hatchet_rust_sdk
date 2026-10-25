//! Preview durable topics, independent of workflow/run output streams.
mod publish;
use crate::{Error, Result, config::HatchetConfig, proto::v1};
use futures_core::Stream;
use std::{
    collections::HashMap,
    future::poll_fn,
    pin::Pin,
    sync::{Arc, atomic::AtomicBool},
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;
use tonic::{Code, Request, Status, transport::Channel};

pub use v1::StreamTopicMetadata;
const MAX_INLINE: usize = 4 * 1024 * 1024 - 5 * 1024;
const MAX_UPLOADED: usize = 64 * 1024 * 1024;
const RPC_TIMEOUT: Duration = Duration::from_secs(30);

/// Options shared by durable-topic operations. Topic namespaces default to the
/// empty namespace, independently of the workflow/event namespace in config.
#[derive(Debug, Clone, Default)]
pub struct StreamOptions {
    pub namespace: String,
    /// Resume immediately after this opaque cursor (events only).
    pub cursor: Option<String>,
    /// Maximum batching wait before publish dispatch; zero sends immediately.
    pub batch_timeout: Duration,
    /// Ends an events subscription. Dropping the stream also closes it.
    pub cancel: CancellationToken,
}
#[derive(Debug, Clone, PartialEq)]
pub struct StreamEvent {
    pub payload: Vec<u8>,
    pub cursor: String,
    pub created_at: Option<prost_types::Timestamp>,
}
/// Incremental durable-topic events. Recoverable disconnects resubscribe after
/// the last yielded cursor. Errors that cannot recover are yielded once, then end.
/// Dropping this value closes its current request and reconnect timer.
pub struct TopicEventStream {
    inner: Pin<Box<dyn Stream<Item = Result<StreamEvent>> + Send>>,
}
impl std::fmt::Debug for TopicEventStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TopicEventStream").finish_non_exhaustive()
    }
}
impl TopicEventStream {
    pub async fn next(&mut self) -> Option<Result<StreamEvent>> {
        poll_fn(|cx| Pin::new(&mut *self).poll_next(cx)).await
    }
}
impl Stream for TopicEventStream {
    type Item = Result<StreamEvent>;
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.get_mut().inner.as_mut().poll_next(cx)
    }
}
/// Cloneable client for preview durable topics on an engine exposing V1Streams.
/// Publishes on the same (namespace, topic) are serialized and share batches;
/// clones share producer state. Acknowledgment means the message was stored.
#[derive(Clone)]
pub struct StreamsClient {
    inner: Arc<Inner>,
}
impl std::fmt::Debug for StreamsClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamsClient").finish_non_exhaustive()
    }
}
struct Inner {
    config: HatchetConfig,
    auth: tonic::metadata::MetadataValue<tonic::metadata::Ascii>,
    grpc: v1::v1_streams_client::V1StreamsClient<Channel>,
    http: reqwest::Client,
    topics: Mutex<HashMap<(String, String), Arc<publish::Topic>>>,
    single_only: AtomicBool,
}
impl Inner {
    fn request<T>(&self, payload: T) -> Request<T> {
        let mut request = Request::new(payload);
        request
            .metadata_mut()
            .insert("authorization", self.auth.clone());
        request
    }
    fn payload_url(&self) -> Result<reqwest::Url> {
        let mut url = reqwest::Url::parse(&format!(
            "{}/api/v1/stable/tenants",
            self.config.server_url.trim_end_matches('/')
        ))
        .map_err(|e| Error::Config(e.to_string()))?;
        url.path_segments_mut()
            .map_err(|()| Error::Config("stream payload URL cannot be a base".into()))?
            .push(&self.config.tenant_id)
            .push("streams")
            .push("payloads");
        Ok(url)
    }
}
fn grpc_error(status: Status) -> Error {
    if status.code() == Code::Unimplemented {
        Error::UnsupportedOperation(format!("engine durable streams RPC unavailable: {status}"))
    } else {
        Error::Grpc(status.to_string())
    }
}
fn validate_address(topic: &str, namespace: &str) -> Result<()> {
    if topic.is_empty()
        || topic.chars().count() > 255
        || namespace.chars().count() > 255
        || topic.contains('\0')
        || namespace.contains('\0')
    {
        Err(Error::InputValidation(vec![
            "stream topic must be nonempty; topic/namespace must contain at most 255 characters and no NUL".into(),
        ]))
    } else {
        Ok(())
    }
}
fn permanent(code: Code, attempt: u32) -> bool {
    matches!(
        code,
        Code::InvalidArgument
            | Code::PermissionDenied
            | Code::Unauthenticated
            | Code::OutOfRange
            | Code::FailedPrecondition
            | Code::NotFound
    ) || (code == Code::Unimplemented && attempt == 0)
}
fn backoff(failures: u32) -> Duration {
    let base = 250_u64
        .saturating_mul(2_u64.saturating_pow(failures.min(6)))
        .min(10_000);
    Duration::from_millis(
        base / 2 + (uuid::Uuid::new_v4().as_u128() % (u128::from(base - base / 2) + 1)) as u64,
    )
}
impl StreamsClient {
    pub(crate) fn new(
        config: HatchetConfig,
        channel: Channel,
        http: reqwest::Client,
    ) -> Result<Self> {
        let auth = format!("Bearer {}", config.token())
            .parse()
            .map_err(|_| Error::Config("invalid gRPC authorization token".into()))?;
        Ok(Self {
            inner: Arc::new(Inner {
                auth,
                config,
                grpc: v1::v1_streams_client::V1StreamsClient::new(channel),
                http,
                topics: Mutex::new(HashMap::new()),
                single_only: AtomicBool::new(false),
            }),
        })
    }
    /// Counts and latest cursor describe retained messages only.
    /// # Errors
    /// Returns the actual engine error, including missing topic/entitlement.
    pub async fn topic_metadata(
        &self,
        topic: &str,
        options: StreamOptions,
    ) -> Result<StreamTopicMetadata> {
        validate_address(topic, &options.namespace)?;
        tokio::time::timeout(
            RPC_TIMEOUT,
            self.inner
                .grpc
                .clone()
                .get_topic_metadata(self.inner.request(v1::GetStreamTopicMetadataRequest {
                    namespace: options.namespace,
                    topic: topic.into(),
                })),
        )
        .await
        .map_err(|_| Error::Grpc("stream metadata timed out".into()))?
        .map(|v| v.into_inner())
        .map_err(grpc_error)
    }
    /// Future cursorless subscriptions begin after the latest message. Existing
    /// readers and explicit cursors are unaffected; retained history is preserved.
    /// # Errors
    /// Returns the engine error; this mutation is not retried after ambiguity.
    pub async fn reset(&self, topic: &str, options: StreamOptions) -> Result<()> {
        validate_address(topic, &options.namespace)?;
        tokio::time::timeout(
            RPC_TIMEOUT,
            self.inner
                .grpc
                .clone()
                .reset(self.inner.request(v1::ResetStreamTopicRequest {
                    namespace: options.namespace,
                    topic: topic.into(),
                })),
        )
        .await
        .map_err(|_| Error::Grpc("stream reset timed out".into()))?
        .map(|_| ())
        .map_err(grpc_error)
    }
    /// Reads from the oldest retained message or immediately after an explicit
    /// cursor. After reset, cursorless readers start after the reset boundary.
    /// # Errors
    /// Rejects an empty topic. RPC/cursor/payload errors arrive through the stream.
    pub fn events(&self, topic: &str, options: StreamOptions) -> Result<TopicEventStream> {
        validate_address(topic, &options.namespace)?;
        let mut reader = Reader {
            client: self.clone(),
            topic: topic.to_owned(),
            cursor: options.cursor.clone(),
            options,
            wire: None,
            pending: std::collections::VecDeque::new(),
            failures: 0,
            attempt: 0,
            retry_wait: false,
            ended: false,
        };
        Ok(TopicEventStream {
            inner: Box::pin(async_stream::stream! {
                while let Some(event)=reader.next_event().await {yield event;}
            }),
        })
    }
}
struct Reader {
    client: StreamsClient,
    topic: String,
    options: StreamOptions,
    cursor: Option<String>,
    wire: Option<tonic::Streaming<v1::StreamMessage>>,
    pending: std::collections::VecDeque<v1::StreamEntry>,
    failures: u32,
    attempt: u32,
    retry_wait: bool,
    ended: bool,
}
impl Reader {
    fn reconnect(&mut self) {
        self.wire = None;
        self.pending.clear();
        self.retry_wait = true;
    }
    fn failure(&mut self, status: Status) -> Option<Result<StreamEvent>> {
        if permanent(status.code(), self.attempt) {
            self.ended = true;
            Some(Err(grpc_error(status)))
        } else {
            self.reconnect();
            None
        }
    }
    async fn next_event(&mut self) -> Option<Result<StreamEvent>> {
        loop {
            if self.ended || self.options.cancel.is_cancelled() {
                return None;
            }
            if self.retry_wait {
                tokio::select! {_=self.options.cancel.cancelled()=>return None,_=tokio::time::sleep(backoff(self.failures))=>{}}
                self.failures = self.failures.saturating_add(1);
                self.attempt = self.attempt.saturating_add(1);
                self.retry_wait = false;
            }
            if let Some(entry) = self.pending.pop_front() {
                if entry.cursor.is_empty() {
                    self.ended = true;
                    return Some(Err(Error::Grpc(
                        "stream entry omitted resume cursor".into(),
                    )));
                }
                let payload = if entry.payload_ref.is_empty() {
                    entry.payload
                } else {
                    let result = tokio::select! {_=self.options.cancel.cancelled()=>return None,result=self.client.fetch_payload(&entry.payload_ref)=>result};
                    match result {
                        Ok(bytes) => bytes,
                        Err(failure) if !failure.temporary => {
                            self.ended = true;
                            return Some(Err(failure.error));
                        }
                        Err(_) => {
                            self.reconnect();
                            continue;
                        }
                    }
                };
                // Advance only after retrieval, immediately before delivery.
                self.cursor = Some(entry.cursor.clone());
                self.failures = 0;
                return Some(Ok(StreamEvent {
                    payload,
                    cursor: entry.cursor,
                    created_at: entry.created_at,
                }));
            }
            if let Some(wire) = &mut self.wire {
                let result = tokio::select! {_=self.options.cancel.cancelled()=>return None,result=wire.message()=>result};
                match result {
                    Ok(Some(message)) if message.hangup => {
                        self.ended = true;
                        return None;
                    }
                    Ok(Some(message)) => self.pending.extend(message.entries),
                    Ok(None) => self.reconnect(),
                    Err(error) => {
                        if let Some(error) = self.failure(error) {
                            return Some(error);
                        }
                    }
                }
            } else {
                let mut grpc = self.client.inner.grpc.clone();
                let request = self.client.inner.request(v1::SubscribeStreamRequest {
                    namespace: self.options.namespace.clone(),
                    topic: self.topic.clone(),
                    cursor: self.cursor.clone(),
                });
                let result = tokio::select! {_=self.options.cancel.cancelled()=>return None,result=grpc.subscribe(request)=>result};
                match result {
                    Ok(response) => self.wire = Some(response.into_inner()),
                    Err(error) => {
                        if let Some(error) = self.failure(error) {
                            return Some(error);
                        }
                    }
                }
            }
        }
    }
}
struct HttpFailure {
    error: Error,
    temporary: bool,
}
impl HttpFailure {
    fn request(error: reqwest::Error) -> Self {
        Self {
            temporary: true,
            error: Error::Transport(format!("stream payload request failed: {error}")),
        }
    }
    fn permanent(message: impl Into<String>) -> Self {
        Self {
            temporary: false,
            error: Error::Transport(message.into()),
        }
    }
}
async fn body_limited(
    mut response: reqwest::Response,
    limit: usize,
) -> std::result::Result<Vec<u8>, HttpFailure> {
    let mut bytes = Vec::new();
    while let Some(chunk) = response.chunk().await.map_err(HttpFailure::request)? {
        if bytes.len().saturating_add(chunk.len()) > limit {
            return Err(HttpFailure::permanent(
                "stream payload response exceeds maximum size",
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}
impl StreamsClient {
    async fn payload_request(
        &self,
        payload: Option<&[u8]>,
        reference: &str,
    ) -> std::result::Result<Vec<u8>, HttpFailure> {
        let url = self.inner.payload_url().map_err(|error| HttpFailure {
            error,
            temporary: false,
        })?;
        for attempt in 0..3 {
            let request = if let Some(payload) = payload {
                self.inner
                    .http
                    .post(url.clone())
                    .multipart(reqwest::multipart::Form::new().part(
                        "payload",
                        reqwest::multipart::Part::bytes(payload.to_vec()).file_name("payload.bin"),
                    ))
            } else {
                self.inner
                    .http
                    .get(url.clone())
                    .query(&[("ref", reference)])
            };
            let result = match request.timeout(Duration::from_secs(10)).send().await {
                Err(error) => Err(HttpFailure::request(error)),
                Ok(response) if !response.status().is_success() => {
                    let status = response.status();
                    Err(HttpFailure {
                        temporary: status.is_server_error()
                            || status == reqwest::StatusCode::TOO_MANY_REQUESTS,
                        error: Error::Transport(format!("stream payload HTTP {status}")),
                    })
                }
                Ok(response) => {
                    body_limited(
                        response,
                        if payload.is_some() {
                            64 * 1024
                        } else {
                            MAX_UPLOADED
                        },
                    )
                    .await
                }
            };
            match result {
                Err(failure) if failure.temporary && attempt < 2 => {
                    tokio::time::sleep(Duration::from_millis(50 * (1 << attempt))).await
                }
                result => return result,
            }
        }
        unreachable!("bounded request loop always returns")
    }
    async fn upload_payload(&self, payload: &[u8]) -> Result<String> {
        let bytes = self
            .payload_request(Some(payload), "")
            .await
            .map_err(|e| e.error)?;
        let value: serde_json::Value = serde_json::from_slice(&bytes)
            .map_err(|e| Error::Transport(format!("invalid stream upload response: {e}")))?;
        value["ref"]
            .as_str()
            .filter(|s| !s.is_empty())
            .map(str::to_owned)
            .ok_or_else(|| Error::Transport("stream upload omitted payload reference".into()))
    }
    async fn fetch_payload(&self, reference: &str) -> std::result::Result<Vec<u8>, HttpFailure> {
        self.payload_request(None, reference).await
    }
}
