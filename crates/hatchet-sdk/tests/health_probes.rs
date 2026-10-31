use another_hatchet_sdk::health::{HealthServer, HealthState};
use serde_json::{Value, json};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::RwLock,
};
#[path = "support/health.rs"]
mod probes;

fn state() -> Arc<RwLock<HealthState>> {
    Arc::new(RwLock::new(HealthState {
        worker_name: "probe-worker".into(),
        slots: 3,
        durable_slots: 2,
        actions: vec!["default:task".into()],
        labels: BTreeMap::from([("region".into(), "test".into())]),
        healthy: false,
    }))
}
async fn get(port: u16, path: &str) -> String {
    probes::request(
        port,
        format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n\r\n").as_bytes(),
    )
    .await
}
/// A platform's private network can be IPv6-only, so the probes must answer there as well.
#[tokio::test]
async fn probes_answer_over_ipv6_and_ipv4() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    if std::net::TcpListener::bind("[::1]:0").is_err() {
        return; // This host has no IPv6.
    }
    let mut server = HealthServer::start(0, state()).await.unwrap();
    for host in ["[::1]", "127.0.0.1"] {
        let mut stream = tokio::net::TcpStream::connect(format!("{host}:{}", server.port()))
            .await
            .unwrap();
        stream
            .write_all(b"GET /livez HTTP/1.1\r\nHost: localhost\r\n\r\n")
            .await
            .unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 200"), "{host}: {response}");
    }
    server.stop().await;
}
#[tokio::test]
async fn liveness_is_independent_of_readiness_and_legacy_health_preserves_json() {
    let mut server = HealthServer::start(0, state()).await.unwrap();
    assert_ne!(server.port(), 0);
    for healthy in [false, true, false, true, false] {
        server.set_healthy(healthy).await;
        let live = get(server.port(), "/livez").await;
        assert!(live.starts_with("HTTP/1.1 200 OK"));
        assert!(live.ends_with("\r\n\r\nok"));
        let expected = if healthy {
            "200 OK"
        } else {
            "503 Service Unavailable"
        };
        assert!(
            get(server.port(), "/readyz")
                .await
                .starts_with(&format!("HTTP/1.1 {expected}"))
        );
        let response = get(server.port(), "/health").await;
        assert!(response.starts_with(&format!("HTTP/1.1 {expected}")));
        let body: Value = serde_json::from_str(response.split_once("\r\n\r\n").unwrap().1).unwrap();
        assert_eq!(
            body,
            json!({"healthy":healthy,"worker_name":"probe-worker","slots":3,"durable_slots":2,"actions":["default:task"],"labels":{"region":"test"}})
        );
    }
    // Liveness must not wait behind a contended worker-state write lock.
    let shared = state();
    let mut independent = HealthServer::start(0, Arc::clone(&shared)).await.unwrap();
    let _held = shared.write().await;
    assert!(
        get(independent.port(), "/livez")
            .await
            .starts_with("HTTP/1.1 200 OK")
    );
    independent.stop().await;
    server.stop().await;
}
#[tokio::test]
async fn probe_http_routes_methods_and_head_follow_their_status_contracts() {
    let mut server = HealthServer::start(0, state()).await.unwrap();
    for path in ["/unknown", "/livez-extra", "/readyz?query=1"] {
        assert!(
            get(server.port(), path)
                .await
                .starts_with("HTTP/1.1 404 Not Found")
        );
    }
    let response = probes::request(
        server.port(),
        b"POST /livez HTTP/1.1\r\nHost: local\r\n\r\n",
    )
    .await;
    assert!(response.starts_with("HTTP/1.1 405 Method Not Allowed"));
    assert!(response.contains("\r\nAllow: GET, HEAD\r\n"));
    for path in ["/health", "/livez", "/readyz", "/unknown"] {
        let ordinary = get(server.port(), path).await;
        let head = probes::request(
            server.port(),
            format!("HEAD {path} HTTP/1.1\r\nHost: local\r\n\r\n").as_bytes(),
        )
        .await;
        assert_eq!(
            head.split_once("\r\n\r\n").unwrap(),
            (ordinary.split_once("\r\n\r\n").unwrap().0, "")
        );
    }
    assert!(
        probes::request(server.port(), b"GET /livez HTTP/bogus\r\n\r\n")
            .await
            .starts_with("HTTP/1.1 400 Bad Request")
    );
    let huge = format!("GET /livez HTTP/1.1\r\nX-Large: {}", "x".repeat(9000));
    assert!(
        probes::request(server.port(), huge.as_bytes())
            .await
            .starts_with("HTTP/1.1 431 Request Header Fields Too Large")
    );
    server.stop().await;
}
#[tokio::test]
async fn fragmented_probe_requests_complete_and_shutdown_closes_pending_connections() {
    let mut server = HealthServer::start(0, state()).await.unwrap();
    let mut connection = probes::connect(server.port()).await;
    connection.write_all(b"GET /rea").await.unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    connection
        .write_all(b"dyz HTTP/1.1\r\nHost: local\r\n\r\n")
        .await
        .unwrap();
    let mut response = String::new();
    connection.read_to_string(&mut response).await.unwrap();
    assert!(response.starts_with("HTTP/1.1 503 Service Unavailable"));
    let mut idle = probes::connect(server.port()).await;
    idle.write_all(b"GET /").await.unwrap();
    server.stop().await;
    let mut bytes = Vec::new();
    let read = tokio::time::timeout(Duration::from_secs(1), idle.read_to_end(&mut bytes))
        .await
        .unwrap();
    assert!(read.is_err() || bytes.is_empty());
    // Dropping an owner also releases its listener and active request tasks.
    let server = HealthServer::start(0, state()).await.unwrap();
    let port = server.port();
    drop(server);
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let Ok(listener) = std::net::TcpListener::bind(("0.0.0.0", port)) {
                drop(listener);
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn incomplete_probe_requests_have_a_bounded_lifetime() {
    let mut server = HealthServer::start(0, state()).await.unwrap();
    let mut idle = probes::connect(server.port()).await;
    idle.write_all(b"GET /livez HTTP/1.1\r\n").await.unwrap();
    let mut bytes = Vec::new();
    tokio::time::timeout(Duration::from_secs(3), idle.read_to_end(&mut bytes))
        .await
        .unwrap()
        .unwrap();
    assert!(bytes.is_empty());
    server.stop().await;
}
