//! Integration tests against a real Hatchet engine.
//!
//! Run with:
//!   cargo test -p another-hatchet-sdk --features integration --test integration -- --nocapture
//!
//! Requires environment variables:
//!   HATCHET_CLIENT_TOKEN=<jwt>
//!   HATCHET_CLIENT_TLS_STRATEGY=none   (for local dev)

#![cfg(feature = "integration")]

use std::sync::Arc;
use std::time::Duration;

use another_hatchet_sdk::config::HatchetConfig;
use another_hatchet_sdk::grpc_transport::GrpcTransport;
use another_hatchet_sdk::*;

type Ctx = InMemoryContext<GrpcTransport>;
type DurableCtx = InMemoryDurableContext<GrpcTransport>;

const TEST_TIMEOUT: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

/// Build a real `Client` backed by `GrpcTransport`.
async fn real_client() -> Client<GrpcTransport> {
    let config = HatchetConfig::from_env()
        .expect("live integration tests require a valid HATCHET_CLIENT_TOKEN");
    let transport = GrpcTransport::from_config(config)
        .await
        .expect("live integration tests require a reachable Hatchet engine");
    Client::new(Arc::new(transport))
}

// ---------------------------------------------------------------------------
// P6.1 — Basic workflow: register + trigger + output
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live Hatchet engine credentials; run explicitly with --ignored"]
async fn integration_basic_register_and_run() {
    let client = real_client().await;

    let echo_task = client.task::<serde_json::Value, String>("echo");

    let declaration = WorkflowDeclaration::<serde_json::Value, String>::new("basic")
        .step(&echo_task, TaskConfig::default());

    client
        .register_workflow(declaration)
        .await
        .expect("workflow registration succeeds");

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
// Child workflow: parent task spawns a child and awaits its typed output
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live Hatchet engine credentials; run explicitly with --ignored"]
async fn integration_child_workflow() {
    let client = real_client().await;

    let child_task = client.task::<serde_json::Value, String>("live-child-task");
    let parent_task = client.task::<serde_json::Value, String>("live-parent-task");
    let child_workflow = client.workflow::<serde_json::Value, String>("live-child-workflow");
    let child_for_handler = child_workflow.clone();

    let worker = client
        .worker("integration-child-worker")
        .register_task(&child_task, |input, _ctx: Ctx| async move {
            Ok(format!("child:{input}"))
        })
        .register_task(&parent_task, move |_input, ctx: Ctx| {
            let child = child_for_handler.clone();
            async move {
                let child_run = ctx
                    .run_child(
                        &child,
                        serde_json::json!({"message": "from-parent"}),
                        RunOpts::default(),
                    )
                    .await?;
                let child_output = child_run.output().await?;
                Ok(format!("parent:{child_output}"))
            }
        })
        .build()
        .start()
        .expect("child workflow worker should start");

    worker
        .wait_until_ready(Duration::from_secs(10))
        .await
        .expect("child workflow worker should become ready");

    client
        .register_workflow(
            WorkflowDeclaration::<serde_json::Value, String>::new("live-child-workflow")
                .step(&child_task, TaskConfig::default()),
        )
        .await
        .expect("child workflow registration succeeds");
    client
        .register_workflow(
            WorkflowDeclaration::<serde_json::Value, String>::new("live-parent-workflow")
                .step(&parent_task, TaskConfig::default()),
        )
        .await
        .expect("parent workflow registration succeeds");

    let workflow = client.workflow::<serde_json::Value, String>("live-parent-workflow");
    let run = workflow.run(serde_json::json!({"start": true}), RunOpts::default());
    let output = tokio::time::timeout(TEST_TIMEOUT, run)
        .await
        .expect("parent workflow should not time out")
        .expect("parent workflow should succeed");

    assert!(
        output.starts_with("parent:child:"),
        "parent should return its child workflow output: {output}"
    );
    eprintln!("integration_child_workflow output: {output}");

    worker.shutdown().await.unwrap();
}

// ---------------------------------------------------------------------------
// P6.1 — DAG workflow: two-step dependency
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live Hatchet engine credentials; run explicitly with --ignored"]
async fn integration_dag_workflow() {
    let client = real_client().await;

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

    let declaration = WorkflowDeclaration::<serde_json::Value, serde_json::Value>::new("dag")
        .step(&step_a, TaskConfig::default())
        .step_after(&step_b, &[&step_a], TaskConfig::default());

    client
        .register_workflow(declaration)
        .await
        .expect("workflow registration succeeds");

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
// Durable task: engine-backed memoization and passive sleep
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live Hatchet engine credentials; run explicitly with --ignored"]
async fn integration_durable_memo_and_sleep() {
    let client = real_client().await;

    let durable_task = client.durable_task::<serde_json::Value, String>("live-durable-task");
    let worker = client
        .worker("integration-durable-worker")
        .register_durable_task(&durable_task, |_input, ctx: DurableCtx| async move {
            assert!(
                ctx.supports_eviction(),
                "live durable tasks should use the engine listener"
            );
            let memoized = ctx
                .memo("live-integration-memo", serde_json::json!({"stored": true}))
                .await?;
            ctx.sleep_for(Duration::from_secs(1)).await?;
            Ok(format!("awake:{memoized}"))
        })
        .build()
        .start()
        .expect("durable worker should start");

    worker
        .wait_until_ready(Duration::from_secs(10))
        .await
        .expect("durable worker should become ready");

    client
        .register_workflow(
            WorkflowDeclaration::<serde_json::Value, String>::new("live-durable-workflow")
                .durable_step(&durable_task, TaskConfig::default()),
        )
        .await
        .expect("durable workflow registration succeeds");

    let workflow = client.workflow::<serde_json::Value, String>("live-durable-workflow");
    let run = workflow.run(serde_json::json!({}), RunOpts::default());
    let output = tokio::time::timeout(Duration::from_secs(60), run)
        .await
        .expect("durable workflow should not time out")
        .expect("durable workflow should succeed");

    assert_eq!(output, "awake:{\"stored\":true}");
    eprintln!("integration_durable_memo_and_sleep output: {output}");

    worker.shutdown().await.unwrap();
}

// ---------------------------------------------------------------------------
// Durable event wait: namespaced event lookback resolves through engine callback
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live Hatchet engine credentials; run explicitly with --ignored"]
async fn integration_durable_event_wait() {
    let client = real_client().await;

    let durable_task =
        client.durable_task::<serde_json::Value, serde_json::Value>("live-durable-event-task");
    let worker = client
        .worker("integration-durable-event-worker")
        .register_durable_task(&durable_task, |input, ctx: DurableCtx| async move {
            let scope = input
                .get("scope")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| Error::InputValidation(vec!["scope is required".to_owned()]))?;
            let matched = ctx
                .wait_for(
                    Condition::event("durable.wait.event")
                        .scope(scope)
                        .lookback(Duration::from_secs(30))
                        .build()
                        .into(),
                )
                .await?;
            matched
                .payload
                .ok_or_else(|| Error::Durable("durable event callback had no payload".to_owned()))
        })
        .build()
        .start()
        .expect("durable event worker should start");

    worker
        .wait_until_ready(Duration::from_secs(10))
        .await
        .expect("durable event worker should become ready");

    client
        .register_workflow(
            WorkflowDeclaration::<serde_json::Value, serde_json::Value>::new(
                "live-durable-event-workflow",
            )
            .durable_step(&durable_task, TaskConfig::default()),
        )
        .await
        .expect("durable event workflow registration succeeds");

    let scope = format!("local-smoke-{}", uuid::Uuid::new_v4());
    client
        .events()
        .push(
            EventRecord::new(
                "durable.wait.event",
                serde_json::json!({"message": "durable-event-resolved"}),
            )
            .with_scope(&scope),
        )
        .await
        .expect("durable event push succeeds");

    let workflow =
        client.workflow::<serde_json::Value, serde_json::Value>("live-durable-event-workflow");
    let run = workflow.run(serde_json::json!({"scope": scope}), RunOpts::default());
    let output = tokio::time::timeout(Duration::from_secs(60), run)
        .await
        .expect("durable event workflow should not time out")
        .expect("durable event workflow should succeed");

    assert_eq!(output["message"], "durable-event-resolved");
    eprintln!("integration_durable_event_wait output: {output}");

    worker.shutdown().await.unwrap();
}

// ---------------------------------------------------------------------------
// Event trigger: register an event workflow, push an event, observe context
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live Hatchet engine credentials; run explicitly with --ignored"]
async fn integration_event_trigger() {
    let client = real_client().await;

    let event_task = client.task::<serde_json::Value, String>("live-event-task");
    let (tx, rx) = tokio::sync::oneshot::channel::<(bool, Option<String>)>();
    let tx = Arc::new(tokio::sync::Mutex::new(Some(tx)));

    let worker = client
        .worker("integration-event-worker")
        .register_task(&event_task, move |_input, ctx: Ctx| {
            let tx = Arc::clone(&tx);
            async move {
                if let Some(sender) = tx.lock().await.take() {
                    let _ =
                        sender.send((ctx.triggered_by_event(), ctx.triggers().event_key.clone()));
                }
                Ok("event-handled".to_owned())
            }
        })
        .build()
        .start()
        .expect("event worker should start");

    worker
        .wait_until_ready(Duration::from_secs(10))
        .await
        .expect("event worker should become ready");

    client
        .register_workflow(
            WorkflowDeclaration::<serde_json::Value, String>::new("live-event-workflow")
                .on_event("live.event")
                .step(&event_task, TaskConfig::default()),
        )
        .await
        .expect("event workflow registration succeeds");

    client
        .events()
        .push(EventRecord::new(
            "live.event",
            serde_json::json!({"message": "hello-from-event"}),
        ))
        .await
        .expect("event push succeeds");

    let (triggered_by_event, event_key) = tokio::time::timeout(TEST_TIMEOUT, rx)
        .await
        .expect("event-triggered task should not time out")
        .expect("event-triggered task should execute");
    assert!(
        triggered_by_event,
        "task context should identify an event trigger"
    );
    assert!(
        event_key
            .as_deref()
            .is_some_and(|key| key.ends_with("live.event")),
        "task context should retain the event key: {event_key:?}"
    );
    eprintln!("integration_event_trigger event key: {event_key:?}");

    worker.shutdown().await.unwrap();
}

// ---------------------------------------------------------------------------
// P6.1 — Run cancel
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live Hatchet engine credentials; run explicitly with --ignored"]
async fn integration_run_cancel() {
    let client = real_client().await;

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

    let declaration = WorkflowDeclaration::<serde_json::Value, String>::new("cancel")
        .step(&slow_task, TaskConfig::default());

    client
        .register_workflow(declaration)
        .await
        .expect("workflow registration succeeds");

    let workflow = client.workflow::<serde_json::Value, String>("cancel");
    let run_ref = workflow
        .run_no_wait(serde_json::json!({}), RunOpts::default())
        .await
        .expect("run should trigger");

    // Give the run a moment to start, then cancel
    tokio::time::sleep(Duration::from_secs(2)).await;

    let cancel_fut = client.runs().cancel(run_ref.run_id());

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
#[ignore = "requires live Hatchet engine credentials; run explicitly with --ignored"]
async fn integration_feature_clients_rest() {
    let client = real_client().await;

    // List runs (may be empty, but should not error)
    let runs = client
        .runs()
        .list()
        .await
        .expect("list runs should succeed");
    eprintln!(
        "integration_feature_clients_rest: {} runs found",
        runs.len()
    );

    // List workers
    let workers = client
        .workers()
        .list()
        .await
        .expect("list workers should succeed");
    eprintln!(
        "integration_feature_clients_rest: {} workers found",
        workers.len()
    );

    // List workflows
    let workflows = client
        .workflows()
        .list()
        .await
        .expect("list workflows should succeed");
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
#[ignore = "requires live Hatchet engine credentials; run explicitly with --ignored"]
async fn integration_log_retrieval() {
    let client = real_client().await;

    let log_task = client.task::<serde_json::Value, String>("log");

    let (tx, rx) = tokio::sync::oneshot::channel::<String>();
    let tx = Arc::new(tokio::sync::Mutex::new(Some(tx)));

    let _worker = client
        .worker("integration-log-worker")
        .register_task(&log_task, move |_input, ctx: Ctx| {
            let tx = Arc::clone(&tx);
            async move {
                let task_run_id = ctx.task_run_id().to_owned();
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

    let declaration = WorkflowDeclaration::<serde_json::Value, String>::new("logworkflow")
        .step(&log_task, TaskConfig::default());

    client
        .register_workflow(declaration)
        .await
        .expect("workflow registration succeeds");

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

    let task_run_id = rx
        .await
        .expect("worker should have executed and sent task run id");

    let logs = client
        .logs()
        .list(&task_run_id)
        .await
        .expect("list logs should succeed");
    eprintln!(
        "integration_log_retrieval: {} log entries from transport",
        logs.len()
    );

    _worker.shutdown().await.unwrap();
}
