//! Destructive restart proof; run as an explicit late phase after routing tests.
#![cfg(feature = "integration")]
use another_hatchet_sdk::*;
use std::{sync::Arc, time::Duration};
async fn real_client() -> Client<GrpcTransport> {
    Client::new(Arc::new(
        GrpcTransport::from_config(HatchetConfig::from_env().unwrap())
            .await
            .unwrap(),
    ))
}
async fn topic_next(stream: &mut TopicEventStream) -> StreamEvent {
    tokio::time::timeout(Duration::from_secs(15), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable engine v0.114.0 with durable streams enabled"]
async fn durable_topic_cursor_and_active_reader_survive_engine_restart() {
    let engine =
        std::env::var("HATCHET_TEST_ENGINE_CONTAINER").expect("use disposable smoke runner");
    assert!(engine.starts_with("hatchet-rust-") && engine.ends_with("-hatchet"));
    let client = real_client().await;
    let streams = client.streams().unwrap();
    let topic = format!("restart-{}", uuid::Uuid::new_v4());
    streams
        .publish(&topic, b"before".to_vec(), StreamOptions::default())
        .await
        .unwrap();
    let mut reader = streams.events(&topic, StreamOptions::default()).unwrap();
    let before = topic_next(&mut reader).await;
    assert_eq!(before.payload, b"before");
    let pending = tokio::spawn(async move {
        tokio::time::timeout(Duration::from_secs(60), reader.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
    });
    tokio::time::sleep(Duration::from_millis(200)).await;
    let restarted = tokio::task::spawn_blocking(move || {
        std::process::Command::new("docker")
            .args(["restart", "--", &engine])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(
        restarted.status.success(),
        "{}",
        String::from_utf8_lossy(&restarted.stderr)
    );
    tokio::time::timeout(Duration::from_secs(45), async {
        loop {
            if tokio::time::timeout(Duration::from_secs(2), client.engine_version())
                .await
                .is_ok_and(|r| r.is_ok())
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .unwrap();
    streams
        .publish(&topic, b"after".to_vec(), StreamOptions::default())
        .await
        .unwrap();
    assert_eq!(pending.await.unwrap().payload, b"after");
    let fresh = real_client().await;
    let mut resumed = fresh
        .streams()
        .unwrap()
        .events(
            &topic,
            StreamOptions {
                cursor: Some(before.cursor),
                ..Default::default()
            },
        )
        .unwrap();
    assert_eq!(topic_next(&mut resumed).await.payload, b"after");
    assert_eq!(
        streams
            .topic_metadata(&topic, StreamOptions::default())
            .await
            .unwrap()
            .message_count,
        2
    );
}
