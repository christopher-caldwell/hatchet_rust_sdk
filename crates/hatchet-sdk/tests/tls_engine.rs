//! Real native engine gRPC TLS; no global trust-store or environment changes.
#![cfg(feature = "integration")]
use another_hatchet_sdk::*;
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc, time::Duration};

fn owned_engine() {
    let name =
        std::env::var("HATCHET_TEST_ENGINE_CONTAINER").expect("use disposable TLS smoke runner");
    assert!(name.starts_with("hatchet-rust-") && name.ends_with("-hatchet"));
}
async fn transport(config: HatchetConfig) -> Result<GrpcTransport> {
    tokio::time::timeout(Duration::from_secs(8), GrpcTransport::from_config(config))
        .await
        .expect("TLS connection deadline")
}
// Process isolation is required: rustls-native-certs reads these environment
// variables, and mutation inside a multithreaded Rust process would be unsafe.
#[tokio::test]
async fn native_tls_roots_process_helper() {
    let Ok(expected) = std::env::var("HATCHET_TEST_NATIVE_TLS_EXPECT") else {
        return;
    };
    owned_engine();
    let mut config = HatchetConfig::from_env().unwrap();
    config.tls = TlsConfig::System;
    let result = transport(config).await;
    if expected == "trusted" {
        let transport = result.expect("process-scoped native CA must be trusted");
        let client = Client::new(Arc::new(transport));
        assert!(
            tokio::time::timeout(Duration::from_secs(8), client.engine_version())
                .await
                .unwrap()
                .is_ok()
        );
    } else {
        assert!(
            result.is_err(),
            "default native roots must reject disposable CA"
        );
    }
}
async fn native_roots(ca: Option<PathBuf>) {
    let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", "native_tls_roots_process_helper", "--nocapture"])
        .env(
            "HATCHET_TEST_NATIVE_TLS_EXPECT",
            if ca.is_some() { "trusted" } else { "untrusted" },
        )
        .env_remove("SSL_CERT_FILE")
        .env_remove("SSL_CERT_DIR")
        .kill_on_drop(true);
    if let Some(ca) = ca {
        command.env("SSL_CERT_FILE", ca);
    }
    let output = tokio::time::timeout(Duration::from_secs(20), command.output())
        .await
        .expect("native root child deadline")
        .unwrap();
    assert!(
        output.status.success(),
        "native TLS helper failed: {} {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable TLS engine and generated fixture paths"]
async fn real_engine_tls_verifies_custom_and_native_roots_and_rejects_wrong_ca_and_hostname() {
    owned_engine();
    let ca = PathBuf::from(std::env::var("HATCHET_TEST_TLS_CA_FILE").unwrap());
    let wrong_ca = PathBuf::from(std::env::var("HATCHET_TEST_TLS_WRONG_CA_FILE").unwrap());
    let mut config = HatchetConfig::from_env().unwrap();
    config.tls = TlsConfig::Custom {
        ca_cert: ca.clone(),
        client_cert: None,
        client_key: None,
        server_name: Some("localhost".into()),
    };
    let good = transport(config.clone())
        .await
        .expect("trusted TLS engine connection");
    let mut wrong = config.clone();
    wrong.tls = TlsConfig::Custom {
        ca_cert: wrong_ca,
        client_cert: None,
        client_key: None,
        server_name: Some("localhost".into()),
    };
    assert!(transport(wrong).await.is_err(), "untrusted CA accepted");
    let mut wrong = config.clone();
    wrong.tls = TlsConfig::Custom {
        ca_cert: ca.clone(),
        client_cert: None,
        client_key: None,
        server_name: Some("wrong-host.invalid".into()),
    };
    assert!(
        transport(wrong).await.is_err(),
        "wrong server name accepted"
    );
    native_roots(None).await;
    native_roots(Some(ca)).await;
    let client = Client::new(Arc::new(good));
    let name = format!("tls-proof-{}", uuid::Uuid::new_v4());
    let task = client.task::<Value, Value>(&name);
    client
        .register_workflow(
            WorkflowDeclaration::<Value, Value>::new(&name).step(&task, TaskConfig::default()),
        )
        .await
        .unwrap();
    let worker = client
        .worker("tls-proof-worker")
        .register_task(
            &task,
            |v: Value, _: InMemoryContext<GrpcTransport>| async move { Ok(v) },
        )
        .build()
        .start()
        .unwrap();
    worker
        .wait_until_ready(Duration::from_secs(20))
        .await
        .unwrap();
    let run = client
        .workflow::<Value, Value>(&name)
        .run_no_wait(json!({"encrypted":true}), RunOpts::default())
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(30), run.output())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result, json!({"encrypted":true}));
    worker.shutdown().await.unwrap();
    client.workflows().delete(&name).await.unwrap();
}
