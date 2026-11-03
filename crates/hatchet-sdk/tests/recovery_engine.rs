//! Explicit disposable-container restart proof; never targets a remote production engine.
#![cfg(feature = "integration")]
use another_hatchet_sdk::*;
#[path = "support/health.rs"]
mod probes;
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a disposable local engine and HATCHET_TEST_ENGINE_CONTAINER"]
async fn durable_wait_and_checkpoint_survive_engine_restart() {
    engine_recovery(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a disposable local engine and HATCHET_TEST_ENGINE_CONTAINER"]
async fn durable_wait_and_checkpoint_survive_engine_freeze() {
    engine_recovery(true).await;
}

// A failed freeze assertion must not leave the owned fixture paused.
struct UnpauseOnDrop(Option<String>);
impl Drop for UnpauseOnDrop {
    fn drop(&mut self) {
        if let Some(engine) = self.0.take() {
            let _ = std::process::Command::new("docker")
                .args(["unpause", "--", &engine])
                .output();
        }
    }
}

async fn engine_recovery(freeze: bool) {
    let engine =
        std::env::var("HATCHET_TEST_ENGINE_CONTAINER").expect("use the disposable smoke runner");
    assert!(engine.starts_with("hatchet-rust-") && engine.ends_with("-hatchet"));
    let client = Client::new(Arc::new(
        GrpcTransport::from_config(HatchetConfig::from_env().unwrap())
            .await
            .unwrap(),
    ));
    // Action names also need isolation: a namespace scopes workflow names, but
    // an unfinished task from another run may still target the same action.
    let suffix = uuid::Uuid::new_v4().simple().to_string();
    let task_name = format!("restart-checkpoint-task-{suffix}");
    let workflow_name = format!("restart-checkpoint-workflow-{suffix}");
    let worker_name = format!("restart-checkpoint-worker-{suffix}");
    let task = client.durable_task::<Value, Value>(&task_name);
    let started = Arc::new(tokio::sync::Notify::new());
    let invocations = Arc::new(AtomicUsize::new(0));
    let task_id = Arc::new(tokio::sync::Mutex::new(None));
    let reservation = std::net::TcpListener::bind("0.0.0.0:0")
        .or_else(|error| {
            (20000..49000)
                .find_map(|port| std::net::TcpListener::bind(("0.0.0.0", port)).ok())
                .ok_or(error)
        })
        .unwrap();
    let health_port = reservation.local_addr().unwrap().port();
    drop(reservation);
    let worker = client
        .worker(&worker_name)
        .health_port(health_port)
        .register_durable_task(&task, {
            let started = Arc::clone(&started);
            let invocations = Arc::clone(&invocations);
            let task_id = Arc::clone(&task_id);
            move |input, ctx: InMemoryDurableContext<GrpcTransport>| {
                let started = Arc::clone(&started);
                let invocations = Arc::clone(&invocations);
                let task_id = Arc::clone(&task_id);
                async move {
                    let invocation = invocations.fetch_add(1, Ordering::SeqCst) + 1;
                    let seed = ctx.memo("restart-seed", json!(invocation)).await?;
                    task_id
                        .lock()
                        .await
                        .get_or_insert_with(|| ctx.task_run_id().to_owned());
                    started.notify_one();
                    let matched = ctx
                        .wait_for(
                            Condition::event("parity.restart.resume")
                                .scope(input["scope"].as_str().unwrap())
                                .lookback(Duration::from_secs(120))
                                .build()
                                .into(),
                        )
                        .await?;
                    Ok(json!({"seed":seed,"payload":matched.payload,"task_id":ctx.task_run_id()}))
                }
            }
        })
        .build()
        .start()
        .unwrap();
    worker
        .wait_until_ready(Duration::from_secs(15))
        .await
        .unwrap();
    assert!(
        probes::request(health_port, b"GET /readyz HTTP/1.1\r\nHost: local\r\n\r\n")
            .await
            .starts_with("HTTP/1.1 200 OK")
    );
    client
        .register_workflow(
            WorkflowDeclaration::<Value, Value>::new(&workflow_name)
                .durable_step(&task, TaskConfig::default()),
        )
        .await
        .unwrap();
    let scope = uuid::Uuid::new_v4().to_string();
    let run = client
        .workflow::<Value, Value>(&workflow_name)
        .run_no_wait(json!({"scope":scope}), RunOpts::default())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(30), started.notified())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut unpause = UnpauseOnDrop(if freeze { Some(engine.clone()) } else { None });
    let target = engine.clone();
    let restart = tokio::task::spawn_blocking(move || {
        std::process::Command::new("docker")
            .args([if freeze { "pause" } else { "restart" }, "--", &target])
            .output()
            .unwrap()
    });
    tokio::time::timeout(Duration::from_secs(if freeze { 65 } else { 20 }), async {
        loop {
            if probes::request(health_port, b"GET /readyz HTTP/1.1\r\nHost: local\r\n\r\n")
                .await
                .starts_with("HTTP/1.1 503 Service Unavailable")
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("readiness must fail while the engine listener is disconnected");
    assert!(
        probes::request(health_port, b"GET /livez HTTP/1.1\r\nHost: local\r\n\r\n")
            .await
            .starts_with("HTTP/1.1 200 OK")
    );
    let status = restart.await.unwrap();
    if freeze {
        let target = engine.clone();
        let resumed = tokio::task::spawn_blocking(move || {
            std::process::Command::new("docker")
                .args(["unpause", "--", &target])
                .output()
                .unwrap()
        })
        .await
        .unwrap();
        assert!(
            resumed.status.success(),
            "could not resume the local engine"
        );
        unpause.0 = None;
    }
    assert!(
        status.status.success(),
        "{}",
        String::from_utf8_lossy(&status.stderr)
    );
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            if client.engine_version().await.is_ok() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    })
    .await
    .unwrap();
    worker
        .wait_until_ready(Duration::from_secs(60))
        .await
        .unwrap();
    assert!(
        probes::request(health_port, b"GET /readyz HTTP/1.1\r\nHost: local\r\n\r\n")
            .await
            .starts_with("HTTP/1.1 200 OK")
    );
    client
        .events()
        .push(EventRecord::new("parity.restart.resume", json!({"resumed":true})).with_scope(&scope))
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(90), run.output())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result["seed"], 1);
    assert_eq!(result["payload"]["resumed"], true);
    assert_eq!(result["task_id"], task_id.lock().await.as_deref().unwrap());
    worker.shutdown().await.unwrap();
}
