//! Integration tests against a real Hatchet engine.
//!
//! Run with:
//!   cargo test -p hatchet-sdk --features integration --test integration -- --nocapture
//!
//! Requires environment variables (or examples/.env):
//!   HATCHET_CLIENT_TOKEN=<jwt>
//!   HATCHET_CLIENT_TLS_STRATEGY=none   (for local dev)

#![cfg(feature = "integration")]

use std::sync::Arc;
use std::time::Duration;

use hatchet_sdk::config::HatchetConfig;
use hatchet_sdk::grpc_transport::GrpcTransport;
use hatchet_sdk::*;

type Ctx = InMemoryContext<GrpcTransport>;

const TEST_TIMEOUT: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// Load `examples/.env` into the process environment if `HATCHET_CLIENT_TOKEN`
/// is not already set.
fn load_env() {
    if std::env::var("HATCHET_CLIENT_TOKEN").is_ok() {
        return;
    }
    let manifest = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_default();
    let env_path = std::path::Path::new(&manifest)
        .parent()
        .and_then(|p| p.parent())
        .map(|root| root.join("examples/.env"))
        .unwrap_or_default();

    if let Ok(content) = std::fs::read_to_string(&env_path) {
        for line in content.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            if let Some((key, val)) = line.split_once('=') {
                let val = val.trim_matches('"');
                // SAFETY: single-threaded test setup, no concurrent env reads
                unsafe { std::env::set_var(key.trim(), val) };
            }
        }
    }
}

/// Build a real `Client` backed by `GrpcTransport`.
async fn real_client() -> Option<Client<GrpcTransport>> {
    load_env();
    let config = HatchetConfig::from_env().ok()?;
    let transport = GrpcTransport::from_config(config).await.ok()?;
    Some(Client::new(Arc::new(transport)))
}

// ---------------------------------------------------------------------------
// P6.1 — Basic workflow: register + trigger + output
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_basic_register_and_run() {
    let client = match real_client().await {
        Some(c) => c,
        None => {
            eprintln!("SKIP: no Hatchet token available");
            return;
        }
    };

    let echo_task = client.task::<serde_json::Value, String>("echo");

    let declaration =
        WorkflowDeclaration::<serde_json::Value, String>::new("basic")
            .step(&echo_task, TaskConfig::default());

    client.register_workflow(declaration).await;

    let _worker_handle = client
        .worker("integration-test-worker")
        .register_task(&echo_task, |input, _ctx: Ctx| async move {
            let msg = format!("echo: {input}");
            Ok(msg)
        })
        .build()
        .start()
        .expect("worker should start");

    _worker_handle
        .wait_until_ready(Duration::from_secs(10))
        .await
        .expect("worker should become ready");

    let workflow = client.workflow::<serde_json::Value, String>("basic");
    let run_fut = workflow.run(serde_json::json!({"hello": "world"}), RunOpts::default());

    let res = tokio::time::timeout(TEST_TIMEOUT, run_fut).await;

    match res {
        Ok(Ok(output)) => {
            assert!(output.contains("echo"), "output should contain echo prefix");
            eprintln!("integration_basic_register_and_run output: {output}");
        }
        Ok(Err(e)) => panic!("workflow run failed: {e:?}"),
        Err(_) => panic!("workflow run timed out after {TEST_TIMEOUT:?}"),
    }

    _worker_handle.shutdown().await.unwrap();
}

// ---------------------------------------------------------------------------
// P6.1 — DAG workflow: two-step dependency
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_dag_workflow() {
    let client = match real_client().await {
        Some(c) => c,
        None => {
            eprintln!("SKIP: no Hatchet token available");
            return;
        }
    };

    let step_a = client.task::<serde_json::Value, serde_json::Value>("stepa");
    let step_b = client.task::<serde_json::Value, serde_json::Value>("stepb");

    let _worker = client
        .worker("integration-dag-worker")
        .register_task(&step_a, |_input, _ctx: Ctx| async move {
            Ok(serde_json::json!({"from": "step-a"}))
        })
        .register_task(&step_b, |_input, _ctx: Ctx| async move {
            Ok(serde_json::json!({"from": "step-b"}))
        })
        .build()
        .start()
        .expect("dag worker start");

    _worker
        .wait_until_ready(Duration::from_secs(10))
        .await
        .expect("dag worker ready");

    let declaration =
        WorkflowDeclaration::<serde_json::Value, serde_json::Value>::new("dag")
            .step(&step_a, TaskConfig::default())
            .step_after(&step_b, &[&step_a], TaskConfig::default());

    client.register_workflow(declaration).await;

    let workflow = client.workflow::<serde_json::Value, serde_json::Value>("dag");
    let run_fut = workflow.run(serde_json::json!({}), RunOpts::default());

    let res = tokio::time::timeout(TEST_TIMEOUT, run_fut).await;

    match res {
        Ok(Ok(output)) => {
            eprintln!("integration_dag_workflow output: {output}");
        }
        Ok(Err(e)) => panic!("dag workflow failed: {e:?}"),
        Err(_) => panic!("dag workflow timed out after {TEST_TIMEOUT:?}"),
    }

    _worker.shutdown().await.unwrap();
}

// ---------------------------------------------------------------------------
// P6.1 — Run cancel
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_run_cancel() {
    let client = match real_client().await {
        Some(c) => c,
        None => {
            eprintln!("SKIP: no Hatchet token available");
            return;
        }
    };

    let slow_task = client.task::<serde_json::Value, String>("slow");

    let _worker = client
        .worker("integration-cancel-worker")
        .register_task(&slow_task, |_input, ctx: Ctx| async move {
            // Sleep long enough to be cancelled
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_secs(60)) => {}
                _ = ctx.cancelled() => {}
            }
            Ok("done".to_owned())
        })
        .build()
        .start()
        .expect("cancel worker start");

    _worker
        .wait_until_ready(Duration::from_secs(10))
        .await
        .expect("cancel worker ready");

    let declaration =
        WorkflowDeclaration::<serde_json::Value, String>::new("cancel")
            .step(&slow_task, TaskConfig::default());

    client.register_workflow(declaration).await;

    let workflow = client.workflow::<serde_json::Value, String>("cancel");
    let run_ref = workflow
        .run_no_wait(serde_json::json!({}), RunOpts::default())
        .await
        .expect("run should trigger");

    // Give the run a moment to start, then cancel
    tokio::time::sleep(Duration::from_secs(2)).await;

    let cancel_fut = client
        .runs()
        .cancel(run_ref.run_id());

    let res = tokio::time::timeout(TEST_TIMEOUT, cancel_fut).await;

    match res {
        Ok(Ok(())) => {
            eprintln!("integration_run_cancel: run {} cancelled", run_ref.run_id());
        }
        Ok(Err(e)) => panic!("cancel failed: {e:?}"),
        Err(_) => panic!("cancel timed out after {TEST_TIMEOUT:?}"),
    }

    _worker.shutdown().await.unwrap();
}

// ---------------------------------------------------------------------------
// P6.1 — Feature client: list runs and workers via REST
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_feature_clients_rest() {
    let client = match real_client().await {
        Some(c) => c,
        None => {
            eprintln!("SKIP: no Hatchet token available");
            return;
        }
    };

    // List runs (may be empty, but should not error)
    let runs = client.runs().list().await.expect("list runs should succeed");
    eprintln!("integration_feature_clients_rest: {} runs found", runs.len());

    // List workers
    let workers = client.workers().list().await.expect("list workers should succeed");
    eprintln!(
        "integration_feature_clients_rest: {} workers found",
        workers.len()
    );

    // List workflows
    let workflows = client.workflows().list().await.expect("list workflows should succeed");
    eprintln!(
        "integration_feature_clients_rest: {} workflows found",
        workflows.len()
    );

    // RunFilter - filtered list
    let filtered = client
        .runs()
        .list_filtered(RunFilter {
            status: Some(RunStatus::Completed),
            ..Default::default()
        })
        .await
        .expect("list filtered runs should succeed");
    eprintln!(
        "integration_feature_clients_rest: {} completed runs",
        filtered.len()
    );
}

// ---------------------------------------------------------------------------
// P6.1 — Log retrieval
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn integration_log_retrieval() {
    let client = match real_client().await {
        Some(c) => c,
        None => {
            eprintln!("SKIP: no Hatchet token available");
            return;
        }
    };

    let log_task = client.task::<serde_json::Value, String>("log");

    let (tx, rx) = tokio::sync::oneshot::channel::<String>();
    let tx = Arc::new(tokio::sync::Mutex::new(Some(tx)));

    let _worker = client
        .worker("integration-log-worker")
        .register_task(&log_task, move |_input, ctx: Ctx| {
            let tx = Arc::clone(&tx);
            async move {
                let task_run_id = ctx.ids().task_run_id.clone();
                if let Some(sender) = tx.lock().await.take() {
                    let _ = sender.send(task_run_id);
                }
                ctx.log(LogLevel::Info, "integration test log line")
                    .await
                    .ok();
                Ok("logged".to_owned())
            }
        })
        .build()
        .start()
        .expect("log worker start");

    _worker
        .wait_until_ready(Duration::from_secs(10))
        .await
        .expect("log worker ready");

    let declaration =
        WorkflowDeclaration::<serde_json::Value, String>::new("logworkflow")
            .step(&log_task, TaskConfig::default());

    client.register_workflow(declaration).await;

    let workflow = client.workflow::<serde_json::Value, String>("logworkflow");
    let run_fut = workflow.run(serde_json::json!({}), RunOpts::default());

    let res = tokio::time::timeout(TEST_TIMEOUT, run_fut).await;

    match res {
        Ok(Ok(_)) => {
            eprintln!("log workflow run succeeded");
        }
        Ok(Err(e)) => panic!("log workflow run failed: {e:?}"),
        Err(_) => panic!("log workflow run timed out after {TEST_TIMEOUT:?}"),
    }

    let task_run_id = rx.await.expect("worker should have executed and sent task run id");

    let logs = client.logs().list(&task_run_id).await.expect("list logs should succeed");
    eprintln!(
        "integration_log_retrieval: {} log entries from transport",
        logs.len()
    );

    _worker.shutdown().await.unwrap();
}
