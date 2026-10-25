//! Durable topic contracts through actual HTTP/2 and HTTP peers.
use another_hatchet_sdk::{proto::v1, *};
use prost::Message;
use std::{collections::VecDeque, sync::Arc, time::Duration};
use tokio::sync::{Mutex, Notify};
use tonic::codegen::http;
struct Reply {
    path: &'static str,
    messages: Vec<Vec<u8>>,
    code: u16,
    hold: bool,
}
impl Reply {
    fn unary(path: &'static str, code: u16) -> Self {
        Self {
            path,
            messages: vec![vec![]],
            code,
            hold: false,
        }
    }
    fn subscription(entries: Vec<v1::StreamEntry>, code: u16, hangup: bool) -> Self {
        let mut messages = vec![
            v1::StreamMessage {
                entries,
                hangup: false,
                cursor: String::new(),
            }
            .encode_to_vec(),
        ];
        if hangup {
            messages.push(
                v1::StreamMessage {
                    hangup: true,
                    ..Default::default()
                }
                .encode_to_vec(),
            );
        }
        Self {
            path: "Subscribe",
            messages,
            code,
            hold: false,
        }
    }
}
fn entry(text: &str, cursor: &str) -> v1::StreamEntry {
    v1::StreamEntry {
        payload: text.as_bytes().to_vec(),
        cursor: cursor.into(),
        ..Default::default()
    }
}
type Requests = Arc<Mutex<Vec<(String, Vec<u8>)>>>;
struct Peer {
    client: Client<GrpcTransport>,
    requests: Requests,
    reset: Arc<Notify>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn peer(replies: Vec<Reply>, rest: Option<&str>) -> Peer {
    let listener = tokio::net::TcpListener::bind("[::1]:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let requests = Arc::new(Mutex::new(vec![]));
    let reset = Arc::new(Notify::new());
    let task = tokio::spawn({
        let captured = Arc::clone(&requests);
        let reset = Arc::clone(&reset);
        let replies = Arc::new(Mutex::new(VecDeque::from(replies)));
        async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let captured = Arc::clone(&captured);
                let reset = Arc::clone(&reset);
                let replies = Arc::clone(&replies);
                tokio::spawn(async move {
                    let mut connection = h2::server::handshake(socket).await.unwrap();
                    while let Some(incoming) = connection.accept().await {
                        let (mut request, mut respond) = incoming.unwrap();
                        let captured = Arc::clone(&captured);
                        let reset = Arc::clone(&reset);
                        let replies = Arc::clone(&replies);
                        tokio::spawn(async move {
                            assert_eq!(request.headers()["authorization"], "Bearer synthetic");
                            let path = request.uri().path().to_owned();
                            let mut body = Vec::new();
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
                            captured
                                .lock()
                                .await
                                .push((path.clone(), body[5..].to_vec()));
                            let reply = replies
                                .lock()
                                .await
                                .pop_front()
                                .expect("unexpected RPC/retry");
                            assert!(path.ends_with(reply.path), "{path} expected {}", reply.path);
                            let mut stream = respond
                                .send_response(
                                    http::Response::builder()
                                        .header("content-type", "application/grpc")
                                        .body(())
                                        .unwrap(),
                                    false,
                                )
                                .unwrap();
                            if reply.code == 0 || reply.path == "Subscribe" {
                                for message in reply.messages {
                                    let mut frame = vec![0];
                                    frame.extend_from_slice(&(message.len() as u32).to_be_bytes());
                                    frame.extend(message);
                                    if stream.send_data(frame.into(), false).is_err() {
                                        return;
                                    }
                                }
                            }
                            if reply.hold {
                                std::future::poll_fn(|cx| stream.poll_reset(cx))
                                    .await
                                    .unwrap();
                                reset.notify_one();
                                return;
                            }
                            let mut trailers = http::HeaderMap::new();
                            trailers.insert("grpc-status", reply.code.to_string().parse().unwrap());
                            let _ = stream.send_trailers(trailers);
                        });
                    }
                });
            }
        }
    });
    let transport = GrpcTransport::from_config(
        HatchetConfig::builder()
            .host_port(addr.to_string())
            .server_url(rest.unwrap_or("http://[::1]:1"))
            .tenant_id("synthetic")
            .token("synthetic")
            .namespace("workflow-prefix")
            .tls(TlsConfig::Insecure)
            .build()
            .unwrap(),
    )
    .await
    .unwrap();
    Peer {
        client: Client::new(Arc::new(transport)),
        requests,
        reset,
        task,
    }
}
fn batching() -> StreamOptions {
    StreamOptions {
        batch_timeout: Duration::from_millis(50),
        ..Default::default()
    }
}
#[tokio::test]
async fn topic_publish_rotates_after_ambiguous_error_and_retries_producer_gap_once() {
    let p = peer(
        vec![
            Reply::unary("PublishBatch", 14),
            Reply::unary("PublishBatch", 0),
            Reply::unary("PublishBatch", 9),
            Reply::unary("PublishBatch", 0),
        ],
        None,
    )
    .await;
    let streams = p.client.streams().unwrap();
    assert!(
        streams
            .publish("topic", b"ambiguous".to_vec(), StreamOptions::default())
            .await
            .is_err()
    );
    streams
        .publish("topic", b"next".to_vec(), StreamOptions::default())
        .await
        .unwrap();
    streams
        .publish("topic", b"gap".to_vec(), StreamOptions::default())
        .await
        .unwrap();
    let reqs = p.requests.lock().await;
    assert_eq!(reqs.len(), 4);
    let requests = reqs
        .iter()
        .map(|(_, b)| v1::PublishStreamMessagesRequest::decode(b.as_slice()).unwrap())
        .collect::<Vec<_>>();
    assert_ne!(requests[0].producer_id, requests[1].producer_id);
    assert_eq!(requests[1].first_producer_seq, 0);
    assert_eq!(requests[1].producer_id, requests[2].producer_id);
    assert_eq!(requests[2].first_producer_seq, 1);
    assert_ne!(requests[2].producer_id, requests[3].producer_id);
    assert_eq!(requests[3].first_producer_seq, 0);
    assert_eq!(requests[2].entries, requests[3].entries);
    assert_eq!(requests[1].namespace, "");
}
#[tokio::test]
async fn topic_publish_batch_fallback_preserves_partial_success_and_rejected_sequence() {
    let p = peer(
        vec![
            Reply::unary("PublishBatch", 12),
            Reply::unary("Publish", 0),
            Reply::unary("Publish", 8),
            Reply::unary("Publish", 0),
        ],
        None,
    )
    .await;
    let streams = p.client.streams().unwrap();
    let (first, second) = tokio::join!(biased;
        streams.publish("topic",b"first".to_vec(),batching()),
        streams.publish("topic",b"second".to_vec(),batching()),
    );
    assert!(first.is_ok());
    assert!(second.is_err());
    streams
        .publish("topic", b"third".to_vec(), StreamOptions::default())
        .await
        .unwrap();
    let reqs = p.requests.lock().await;
    assert_eq!(reqs.len(), 4);
    let batch = v1::PublishStreamMessagesRequest::decode(reqs[0].1.as_slice()).unwrap();
    assert_eq!(batch.entries.len(), 2);
    let singles = reqs[1..]
        .iter()
        .map(|(_, b)| v1::PublishStreamMessageRequest::decode(b.as_slice()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        singles.iter().map(|r| r.producer_seq).collect::<Vec<_>>(),
        vec![0, 1, 1]
    );
    assert!(singles.iter().all(|r| r.producer_id == batch.producer_id));
    assert_eq!(singles[2].payload, b"third");
}
#[tokio::test]
async fn topic_automatic_batches_respect_entry_and_byte_limits_and_namespaces_are_independent() {
    let p = peer(
        vec![
            Reply::unary("PublishBatch", 0),
            Reply::unary("PublishBatch", 0),
            Reply::unary("PublishBatch", 0),
        ],
        None,
    )
    .await;
    let streams = p.client.streams().unwrap();
    let mut tasks = Vec::new();
    for n in 0..1001_u32 {
        let s = streams.clone();
        tasks.push(tokio::spawn(async move {
            s.publish(
                "topic",
                n.to_be_bytes().to_vec(),
                StreamOptions {
                    batch_timeout: Duration::from_secs(2),
                    ..Default::default()
                },
            )
            .await
        }));
    }
    for task in tasks {
        task.await.unwrap().unwrap();
    }
    let reqs = p.requests.lock().await;
    assert_eq!(reqs.len(), 3);
    let requests = reqs
        .iter()
        .map(|(_, b)| v1::PublishStreamMessagesRequest::decode(b.as_slice()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        requests.iter().map(|r| r.entries.len()).collect::<Vec<_>>(),
        vec![500, 500, 1]
    );
    assert_eq!(
        requests
            .iter()
            .map(|r| r.first_producer_seq)
            .collect::<Vec<_>>(),
        vec![0, 500, 1000]
    );
    let values = requests
        .iter()
        .flat_map(|r| {
            r.entries
                .iter()
                .map(|e| u32::from_be_bytes(e.payload.as_slice().try_into().unwrap()))
        })
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(values.len(), 1001);
    drop(reqs);
    let p = peer(
        vec![
            Reply::unary("PublishBatch", 0),
            Reply::unary("PublishBatch", 0),
        ],
        None,
    )
    .await;
    let streams = p.client.streams().unwrap();
    let (first, second) = tokio::join!(biased;
        streams.publish("topic",vec![1;3*1024*1024],batching()),
        streams.publish("topic",vec![2;3*1024*1024],batching()),
    );
    first.unwrap();
    second.unwrap();
    assert_eq!(p.requests.lock().await.len(), 2);
    let p = peer(
        vec![
            Reply::unary("PublishBatch", 0),
            Reply::unary("PublishBatch", 0),
            Reply::unary("PublishBatch", 0),
        ],
        None,
    )
    .await;
    let streams = p.client.streams().unwrap();
    for namespace in ["a", "b", "a"] {
        streams
            .publish(
                "same-topic",
                b"x".to_vec(),
                StreamOptions {
                    namespace: namespace.into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }
    let reqs = p.requests.lock().await;
    let reqs = reqs
        .iter()
        .map(|(_, b)| v1::PublishStreamMessagesRequest::decode(b.as_slice()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(reqs[0].producer_id, reqs[2].producer_id);
    assert_ne!(reqs[0].producer_id, reqs[1].producer_id);
    assert_eq!(reqs[2].first_producer_seq, 1);
}
#[tokio::test]
async fn topic_reconnect_resumes_after_last_delivered_cursor_and_permanent_errors_stop() {
    let p = peer(
        vec![
            Reply::subscription(vec![entry("a", "c1"), entry("b", "c2")], 14, false),
            Reply::subscription(vec![entry("c", "c3")], 0, true),
        ],
        None,
    )
    .await;
    let mut events = p
        .client
        .streams()
        .unwrap()
        .events(
            "topic",
            StreamOptions {
                cursor: Some("c0".into()),
                ..Default::default()
            },
        )
        .unwrap();
    for (payload, cursor) in [(b"a", "c1"), (b"b", "c2"), (b"c", "c3")] {
        let e = tokio::time::timeout(Duration::from_secs(3), events.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(e.payload, payload);
        assert_eq!(e.cursor, cursor);
    }
    assert!(events.next().await.is_none());
    let reqs = p.requests.lock().await;
    assert_eq!(reqs.len(), 2);
    assert_eq!(
        v1::SubscribeStreamRequest::decode(reqs[0].1.as_slice())
            .unwrap()
            .cursor
            .as_deref(),
        Some("c0")
    );
    assert_eq!(
        v1::SubscribeStreamRequest::decode(reqs[1].1.as_slice())
            .unwrap()
            .cursor
            .as_deref(),
        Some("c2")
    );
    drop(reqs);
    for code in [7, 11, 12] {
        let p = peer(vec![Reply::subscription(vec![], code, false)], None).await;
        let mut events = p
            .client
            .streams()
            .unwrap()
            .events("topic", StreamOptions::default())
            .unwrap();
        assert!(events.next().await.unwrap().is_err());
        assert!(events.next().await.is_none());
        assert_eq!(p.requests.lock().await.len(), 1);
    }
}
#[tokio::test]
async fn topic_drop_and_cancellation_close_the_actual_subscription() {
    for explicit in [false, true] {
        let mut reply = Reply::subscription(vec![entry("a", "c1")], 0, false);
        reply.hold = true;
        let p = peer(vec![reply], None).await;
        let options = StreamOptions::default();
        let cancel = options.cancel.clone();
        let mut events = p
            .client
            .streams()
            .unwrap()
            .events("topic", options)
            .unwrap();
        assert_eq!(events.next().await.unwrap().unwrap().payload, b"a");
        if explicit {
            cancel.cancel();
            assert!(events.next().await.is_none());
        }
        drop(events);
        tokio::time::timeout(Duration::from_secs(2), p.reset.notified())
            .await
            .unwrap();
    }
}
#[tokio::test]
async fn topic_invalid_inputs_and_local_transport_are_explicitly_rejected() {
    assert!(matches!(
        Client::new(Arc::new(InMemoryTransport::new())).streams(),
        Err(Error::UnsupportedOperation(_))
    ));
    let p = peer(vec![], None).await;
    let streams = p.client.streams().unwrap();
    assert!(
        streams
            .publish("topic", Vec::new(), StreamOptions::default())
            .await
            .is_err()
    );
    assert!(
        streams
            .publish(
                "topic",
                vec![0; 64 * 1024 * 1024 + 1],
                StreamOptions::default()
            )
            .await
            .is_err()
    );
    assert!(
        streams
            .publish(
                "topic",
                b"a".to_vec(),
                StreamOptions {
                    batch_timeout: Duration::MAX,
                    ..Default::default()
                }
            )
            .await
            .is_err()
    );
    for name in ["".to_owned(), "x".repeat(256), "a\0b".to_owned()] {
        assert!(streams.events(&name, StreamOptions::default()).is_err());
    }
    assert!(
        streams
            .events(
                "topic",
                StreamOptions {
                    namespace: "x".repeat(256),
                    ..Default::default()
                }
            )
            .is_err()
    );
    assert!(p.requests.lock().await.is_empty());
}

struct HttpPeer {
    url: String,
    requests: Arc<Mutex<Vec<Vec<u8>>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for HttpPeer {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn http_peer(replies: Vec<(u16, Vec<u8>)>) -> HttpPeer {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let listener = tokio::net::TcpListener::bind("[::1]:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let requests = Arc::new(Mutex::new(Vec::new()));
    let task = tokio::spawn({
        let captured = Arc::clone(&requests);
        async move {
            for (status, body) in replies {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut request = Vec::new();
                let mut end = None;
                let mut length = 0;
                loop {
                    let mut bytes = [0; 32768];
                    let n = socket.read(&mut bytes).await.unwrap();
                    assert!(n > 0);
                    request.extend_from_slice(&bytes[..n]);
                    if end.is_none() {
                        if let Some(pos) = request.windows(4).position(|s| s == b"\r\n\r\n") {
                            let headers = String::from_utf8_lossy(&request[..pos]).to_lowercase();
                            length = headers
                                .lines()
                                .find_map(|l| {
                                    l.strip_prefix("content-length: ")
                                        .and_then(|s| s.parse::<usize>().ok())
                                })
                                .unwrap_or(0);
                            end = Some(pos + 4);
                        }
                    }
                    if end.is_some_and(|end| request.len() >= end + length) {
                        break;
                    }
                }
                captured.lock().await.push(request);
                socket.write_all(format!("HTTP/1.1 {status} fixture\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len()).as_bytes()).await.unwrap();
                socket.write_all(&body).await.unwrap();
            }
        }
    });
    HttpPeer {
        url,
        requests,
        task,
    }
}
#[tokio::test]
async fn topic_large_payloads_use_authenticated_upload_references_and_failed_uploads_do_not_poison_batch()
 {
    let http = http_peer(vec![(200, br#"{"ref":"uploaded-object"}"#.to_vec())]).await;
    let p = peer(vec![Reply::unary("PublishBatch", 0)], Some(&http.url)).await;
    let streams = p.client.streams().unwrap();
    let payload = vec![42; 4 * 1024 * 1024];
    streams
        .publish("large", payload.clone(), StreamOptions::default())
        .await
        .unwrap();
    let requests = p.requests.lock().await;
    let publish = v1::PublishStreamMessagesRequest::decode(requests[0].1.as_slice()).unwrap();
    assert!(publish.entries[0].payload.is_empty());
    assert_eq!(publish.entries[0].payload_ref, "uploaded-object");
    drop(requests);
    let requests = http.requests.lock().await;
    let bytes = &requests[0];
    let header_end = bytes.windows(4).position(|b| b == b"\r\n\r\n").unwrap();
    let header = String::from_utf8_lossy(&bytes[..header_end]).to_lowercase();
    assert!(header.starts_with("post /api/v1/stable/tenants/synthetic/streams/payloads "));
    assert!(header.contains("authorization: bearer synthetic"));
    assert!(header.contains("multipart/form-data; boundary="));
    assert!(bytes.windows(payload.len()).any(|b| b == payload));
    drop(requests);
    let http = http_peer(vec![(413, b"too big".to_vec())]).await;
    let p = peer(vec![Reply::unary("PublishBatch", 0)], Some(&http.url)).await;
    let streams = p.client.streams().unwrap();
    let (large, small) = tokio::join!(biased;
        streams.publish("topic",vec![3;4*1024*1024],batching()),
        streams.publish("topic",b"small".to_vec(),batching()),
    );
    small.unwrap();
    assert!(large.is_err());
    let reqs = p.requests.lock().await;
    let publish = v1::PublishStreamMessagesRequest::decode(reqs[0].1.as_slice()).unwrap();
    assert_eq!(publish.entries.len(), 1);
    assert_eq!(publish.entries[0].payload, b"small");
    assert_eq!(publish.first_producer_seq, 0);
}
#[tokio::test]
async fn topic_payload_retry_reconnect_keeps_undelivered_cursor_and_forbidden_fetch_is_terminal() {
    let http = http_peer(vec![
        (500, vec![]),
        (500, vec![]),
        (500, vec![]),
        (200, b"downloaded".to_vec()),
    ])
    .await;
    let reference = v1::StreamEntry {
        cursor: "c1".into(),
        payload_ref: "object".into(),
        ..Default::default()
    };
    let p = peer(
        vec![
            Reply::subscription(vec![reference.clone(), entry("b", "c2")], 0, true),
            Reply::subscription(vec![reference, entry("b", "c2")], 0, true),
        ],
        Some(&http.url),
    )
    .await;
    let mut stream = p
        .client
        .streams()
        .unwrap()
        .events(
            "topic",
            StreamOptions {
                cursor: Some("before".into()),
                ..Default::default()
            },
        )
        .unwrap();
    let first = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first.payload, b"downloaded");
    assert_eq!(first.cursor, "c1");
    assert_eq!(stream.next().await.unwrap().unwrap().payload, b"b");
    assert!(stream.next().await.is_none());
    let reqs = p.requests.lock().await;
    assert_eq!(reqs.len(), 2);
    for (_, body) in reqs.iter() {
        assert_eq!(
            v1::SubscribeStreamRequest::decode(body.as_slice())
                .unwrap()
                .cursor
                .as_deref(),
            Some("before")
        );
    }
    drop(reqs);
    assert_eq!(http.requests.lock().await.len(), 4);
    let http = http_peer(vec![(403, vec![])]).await;
    let p = peer(
        vec![Reply::subscription(
            vec![v1::StreamEntry {
                cursor: "c1".into(),
                payload_ref: "forbidden".into(),
                ..Default::default()
            }],
            0,
            false,
        )],
        Some(&http.url),
    )
    .await;
    let mut stream = p
        .client
        .streams()
        .unwrap()
        .events("topic", StreamOptions::default())
        .unwrap();
    assert!(
        stream
            .next()
            .await
            .unwrap()
            .unwrap_err()
            .to_string()
            .contains("403")
    );
    assert!(stream.next().await.is_none());
    assert_eq!(p.requests.lock().await.len(), 1);
    assert_eq!(http.requests.lock().await.len(), 1);
}
#[tokio::test]
async fn topic_metadata_and_reset_use_exact_namespaces_and_return_engine_values() {
    let metadata = v1::StreamTopicMetadata {
        namespace: "literal namespace".into(),
        topic: "topic".into(),
        tenant_id: "tenant".into(),
        message_count: 7,
        latest_cursor: Some("cursor".into()),
        last_published_at: Some(prost_types::Timestamp {
            seconds: 123,
            nanos: 4,
        }),
    };
    let p = peer(
        vec![
            Reply {
                path: "GetTopicMetadata",
                messages: vec![metadata.encode_to_vec()],
                code: 0,
                hold: false,
            },
            Reply::unary("Reset", 0),
        ],
        None,
    )
    .await;
    let client = p.client.streams().unwrap();
    let options = StreamOptions {
        namespace: "literal namespace".into(),
        ..Default::default()
    };
    assert_eq!(
        client
            .topic_metadata("topic", options.clone())
            .await
            .unwrap(),
        metadata
    );
    client.reset("topic", options).await.unwrap();
    let requests = p.requests.lock().await;
    let reset = v1::ResetStreamTopicRequest::decode(requests[1].1.as_slice()).unwrap();
    assert_eq!(reset.namespace, "literal namespace");
    assert_eq!(reset.topic, "topic");
}
