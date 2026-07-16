use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use hatchet_railway_multi_node::{
    ORDERS_WORKER, OrderInput, PROCESS_ORDER, ProcessedOrder, labels,
};
use another_hatchet_sdk::{Client, InMemoryContext, InMemoryTransport};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[tokio::test]
async fn health_response_identifies_only_the_orders_action() {
    let probe = std::net::TcpListener::bind("127.0.0.1:0").expect("reserve probe port");
    let port = probe.local_addr().expect("probe address").port();
    drop(probe);

    let client = Client::new(Arc::new(InMemoryTransport::new()));
    let task = client.task::<OrderInput, ProcessedOrder>(PROCESS_ORDER);
    let handle = client
        .worker(ORDERS_WORKER)
        .slots(2)
        .durable_slots(2)
        .labels(labels("orders"))
        .health_port(port)
        .register_task(&task, |input, _ctx: InMemoryContext<_>| async move {
            Ok(hatchet_railway_multi_node::process_order(input))
        })
        .build()
        .start()
        .expect("worker starts");
    handle
        .wait_until_ready(Duration::from_secs(2))
        .await
        .expect("worker ready");

    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("connect health server");
    stream
        .write_all(b"GET /health HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .await
        .expect("write request");
    let mut response = Vec::new();
    stream
        .read_to_end(&mut response)
        .await
        .expect("read response");
    let response = String::from_utf8(response).expect("utf8 response");
    let body = response.split("\r\n\r\n").nth(1).expect("response body");
    let json: serde_json::Value = serde_json::from_str(body).expect("health json");
    assert_eq!(json["worker_name"], ORDERS_WORKER);
    assert_eq!(json["slots"], 2);
    assert_eq!(
        json["actions"],
        serde_json::json!(["default:process-order"])
    );
    assert_eq!(json["labels"]["service"], "orders");
    handle.shutdown().await.expect("clean shutdown");
}

#[test]
fn smoke_cleanup_runs_on_success_and_injected_failure() {
    let script = format!("{}/scripts/local-smoke.sh", env!("CARGO_MANIFEST_DIR"));
    for (mode, expected_success) in [("success", true), ("failure", false)] {
        let temp = tempfile::tempdir().expect("tempdir");
        let marker = temp.path().join("marker");
        let status = Command::new("bash")
            .arg(&script)
            .env("SMOKE_TEST_CLEANUP_ONLY", mode)
            .env("SMOKE_TEST_MARKER", &marker)
            .status()
            .expect("run cleanup contract");
        assert_eq!(status.success(), expected_success);
        assert!(!marker.exists(), "cleanup did not remove marker for {mode}");
    }
}
