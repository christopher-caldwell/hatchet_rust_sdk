//! Integration tests against a real Hatchet engine.
//!
//! Run with:
//!   cargo test -p another-hatchet-sdk --features integration --test integration -- --ignored --nocapture --test-threads=1
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

async fn wait_for_status(
    client: &Client<GrpcTransport>,
    run_id: &WorkflowRunId,
    expected: RunStatus,
) {
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let status = client
                .runs()
                .get_status(run_id)
                .await
                .expect("read engine status");
            if status == expected {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("engine should report the expected run status");
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
            let (memoized, other) = tokio::try_join!(
                ctx.memo("live-integration-memo", serde_json::json!({"stored": true})),
                ctx.memo("live-parallel-memo", serde_json::json!({"other": true})),
            )?;
            assert_eq!(memoized, serde_json::json!({"stored": true}));
            assert_eq!(other, serde_json::json!({"other": true}));
            tokio::try_join!(
                ctx.sleep_for(Duration::from_secs(1)),
                ctx.sleep_for(Duration::from_millis(100)),
            )?;
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

    wait_for_status(&client, run_ref.run_id(), RunStatus::Running).await;

    let cancel_fut = client.runs().cancel(run_ref.run_id());

    let res = tokio::time::timeout(TEST_TIMEOUT, cancel_fut).await;

    match res {
        Ok(Ok(())) => {
            eprintln!("integration_run_cancel: run {} cancelled", run_ref.run_id());
        }
        Ok(Err(e)) => panic!("cancel failed: {e:?}"),
        Err(_) => panic!("cancel timed out after {TEST_TIMEOUT:?}"),
    }

    wait_for_status(&client, run_ref.run_id(), RunStatus::Cancelled).await;
    assert!(
        tokio::time::timeout(TEST_TIMEOUT, run_ref.output())
            .await
            .unwrap()
            .is_err()
    );
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live Hatchet engine credentials; run explicitly with --ignored"]
async fn integration_fresh_client_dag_outputs_and_reattachment() {
    let owner = real_client().await;
    let first = owner.task::<serde_json::Value, serde_json::Value>("fresh-first");
    let last = owner.task::<serde_json::Value, String>("fresh-last");
    let side = owner.task::<serde_json::Value, String>("fresh-side");
    let failure = owner.task::<serde_json::Value, String>("fresh-on-failure");
    let worker = owner
        .worker("fresh-output-worker")
        .register_task(&first, |_, _: Ctx| async {
            Ok(serde_json::json!({"intermediate":true}))
        })
        .register_task(&last, |_, _: Ctx| async { Ok("finished".to_owned()) })
        .register_task(&side, |_, _: Ctx| async { Ok("side".to_owned()) })
        .register_task(&failure, |_, _: Ctx| async {
            Ok("failure-handler".to_owned())
        })
        .build()
        .start()
        .unwrap();
    worker
        .wait_until_ready(Duration::from_secs(10))
        .await
        .unwrap();
    owner
        .register_workflow(
            WorkflowDeclaration::<serde_json::Value, String>::new("fresh-output")
                .step(&first, TaskConfig::default())
                .step_after(&last, &[&first], TaskConfig::default())
                .on_failure_task(&failure),
        )
        .await
        .unwrap();

    let producer = real_client().await;
    let run = producer
        .workflow::<serde_json::Value, String>("fresh-output")
        .run_no_wait(serde_json::json!({}), RunOpts::default())
        .await
        .unwrap();
    let old_run_id = run.run_id().0.clone();
    let output = tokio::time::timeout(TEST_TIMEOUT, run.output())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output, "finished");

    // An older run must retain its own graph after a new version adds another leaf.
    owner
        .register_workflow(
            WorkflowDeclaration::<serde_json::Value, serde_json::Value>::new("fresh-output")
                .step(&first, TaskConfig::default())
                .step_after(&last, &[&first], TaskConfig::default())
                .step_after(&side, &[&first], TaskConfig::default())
                .on_failure_task(&failure),
        )
        .await
        .unwrap();
    let resumed = real_client().await;
    let old = tokio::time::timeout(TEST_TIMEOUT, resumed.run_ref::<String>(old_run_id).output())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(old, "finished");
    // Hatchet v0.94.14 caches workflow-name -> version for five seconds
    // (pkg/repository/shared.go). Let that engine cache expire before checking
    // the new version; the old run above must still use its original graph.
    tokio::time::sleep(Duration::from_secs(6)).await;
    let branched = tokio::time::timeout(
        TEST_TIMEOUT,
        resumed
            .workflow::<serde_json::Value, serde_json::Value>("fresh-output")
            .run(serde_json::json!({}), RunOpts::default()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        branched,
        serde_json::json!({"fresh-last":"finished", "fresh-side":"side"})
    );
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live Hatchet engine credentials; run explicitly with --ignored"]
async fn integration_failed_run_status() {
    let client = real_client().await;
    let task = client.task::<serde_json::Value, String>("failed-status-task");
    let worker = client
        .worker("failed-status-worker")
        .register_task(&task, |_, _: Ctx| async {
            Err(Error::non_retryable("intentional status regression test"))
        })
        .build()
        .start()
        .unwrap();
    worker
        .wait_until_ready(Duration::from_secs(10))
        .await
        .unwrap();
    client
        .register_workflow(
            WorkflowDeclaration::<serde_json::Value, String>::new("failed-status")
                .step(&task, TaskConfig::default()),
        )
        .await
        .unwrap();
    let run = client
        .workflow::<serde_json::Value, String>("failed-status")
        .run_no_wait(serde_json::json!({}), RunOpts::default())
        .await
        .unwrap();
    let result = tokio::time::timeout(TEST_TIMEOUT, run.output())
        .await
        .unwrap();
    assert!(result.is_err());
    wait_for_status(&client, run.run_id(), RunStatus::Failed).await;
    let failed = client
        .runs()
        .list_filtered(RunFilter {
            status: Some(RunStatus::Failed),
            ..RunFilter::default()
        })
        .await
        .unwrap();
    assert!(failed.iter().any(|record| &record.run_id == run.run_id()));
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live Hatchet engine credentials; run explicitly with --ignored"]
async fn integration_delayed_workflow_executes() {
    let client = real_client().await;
    let task = client.task::<serde_json::Value, u32>("delayed-task");
    let (tx, rx) = tokio::sync::oneshot::channel();
    let tx = Arc::new(tokio::sync::Mutex::new(Some(tx)));
    let worker = client
        .worker("delayed-worker")
        .register_task(&task, move |input, _: Ctx| {
            let tx = Arc::clone(&tx);
            async move {
                let input = input["value"].as_u64().unwrap() as u32;
                if let Some(sender) = tx.lock().await.take() {
                    let _ = sender.send(input);
                }
                Ok(input * 2)
            }
        })
        .build()
        .start()
        .unwrap();
    worker
        .wait_until_ready(Duration::from_secs(10))
        .await
        .unwrap();
    client
        .register_workflow(
            WorkflowDeclaration::<serde_json::Value, u32>::new("delayed-workflow")
                .step(&task, TaskConfig::default()),
        )
        .await
        .unwrap();
    let started = tokio::time::Instant::now();
    client
        .workflow::<serde_json::Value, u32>("delayed-workflow")
        .delay(
            "delayed-regression",
            Duration::from_secs(2),
            serde_json::json!({"value":21}),
        )
        .await
        .unwrap();
    let input = tokio::time::timeout(TEST_TIMEOUT, rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(input, 21);
    assert!(
        started.elapsed() >= Duration::from_secs(1),
        "schedule must not execute immediately"
    );
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live Hatchet engine credentials; run explicitly with --ignored"]
async fn integration_durable_recovery_branch_preserves_prior_memos() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let owner = real_client().await;
    let task = owner.durable_task::<serde_json::Value, serde_json::Value>("recovery-branch-task");
    let calls = Arc::new(AtomicUsize::new(0));
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let worker = owner
        .worker("recovery-branch-worker")
        .register_durable_task(&task, move |_, ctx: DurableCtx| {
            let calls = Arc::clone(&calls);
            let tx = tx.clone();
            async move {
                let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
                let before = ctx.memo("before-branch", serde_json::json!(call)).await?;
                let after = ctx.memo("after-branch", serde_json::json!(call)).await?;
                tx.send((call, ctx.task_run_id().to_owned())).unwrap();
                Ok(serde_json::json!({"before":before,"after":after}))
            }
        })
        .build()
        .start()
        .unwrap();
    worker
        .wait_until_ready(Duration::from_secs(10))
        .await
        .unwrap();
    owner
        .register_workflow(
            WorkflowDeclaration::<serde_json::Value, serde_json::Value>::new("recovery-branch")
                .durable_step(&task, TaskConfig::default()),
        )
        .await
        .unwrap();
    let run = owner
        .workflow::<serde_json::Value, serde_json::Value>("recovery-branch")
        .run_no_wait(serde_json::json!({}), RunOpts::default())
        .await
        .unwrap();
    let first = tokio::time::timeout(TEST_TIMEOUT, run.output())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first, serde_json::json!({"before":1,"after":1}));
    let (_, task_id) = rx.recv().await.unwrap();
    let operator = real_client().await;
    let resolved = operator
        .runs()
        .get_task_external_id(run.run_id(), "recovery-branch-task")
        .await
        .unwrap();
    assert_eq!(resolved.as_deref(), Some(task_id.as_str()));
    assert_eq!(
        operator
            .runs()
            .get_task_external_id(run.run_id(), "missing")
            .await
            .unwrap(),
        None
    );
    // The pinned engine initializes durable logs on branch 1. Fork its second
    // node so the first memo is inherited and the second memo is recomputed.
    let branch = operator
        .runs()
        .branch_durable_task(&task_id, 2, 1)
        .await
        .unwrap();
    assert_eq!(branch.task_external_id, task_id);
    assert_eq!(branch.node_id, 2);
    assert!(branch.branch_id > 1);
    let (call, rerun_task) = tokio::time::timeout(TEST_TIMEOUT, rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(call, 2);
    assert_eq!(rerun_task, task_id);
    // The original client's prior output must not remain cached after a branch
    // initiated by a separate client.
    let second = tokio::time::timeout(TEST_TIMEOUT, run.output())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(second, serde_json::json!({"before":1,"after":2}));
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live Hatchet engine credentials; run explicitly with --ignored"]
async fn integration_durable_recovery_restore_preserves_task_and_memo() {
    let config = HatchetConfig::from_env().unwrap();
    let transport = Arc::new(GrpcTransport::from_config(config).await.unwrap());
    let owner = Client::new(Arc::clone(&transport));
    let task = owner.durable_task::<serde_json::Value, serde_json::Value>("recovery-restore-task");
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let (stopped_tx, mut stopped_rx) = tokio::sync::mpsc::unbounded_channel();
    let worker=owner.worker("recovery-restore-worker")
        .register_durable_task(&task,move |_,ctx:DurableCtx| {
            let tx=tx.clone();let stopped=stopped_tx.clone();
            async move {
                let invocation=ctx.invocation_count();
                let seed=ctx.memo("restore-seed",serde_json::json!(invocation)).await?;
                tx.send((ctx.task_run_id().to_owned(),invocation)).unwrap();
                if invocation==1 {
                    ctx.cancelled().await;
                    stopped.send(()).unwrap();
                    return Err(Error::Worker("evicted invocation stopped".to_owned()));
                }
                Ok(serde_json::json!({"seed":seed,"invocation":invocation,"task":ctx.task_run_id()}))
            }
        }).build().start().unwrap();
    worker
        .wait_until_ready(Duration::from_secs(10))
        .await
        .unwrap();
    owner
        .register_workflow(
            WorkflowDeclaration::<serde_json::Value, serde_json::Value>::new("recovery-restore")
                .durable_step(&task, TaskConfig::default()),
        )
        .await
        .unwrap();
    let run = owner
        .workflow::<serde_json::Value, serde_json::Value>("recovery-restore")
        .run_no_wait(serde_json::json!({}), RunOpts::default())
        .await
        .unwrap();
    let (task_id, invocation) = tokio::time::timeout(TEST_TIMEOUT, rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(invocation, 1);
    transport
        .durable_listener()
        .unwrap()
        .send_evict_invocation(
            &task_id,
            invocation as i32,
            Some("restore parity test".to_owned()),
        )
        .await
        .unwrap();
    tokio::time::timeout(TEST_TIMEOUT, stopped_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let operator = real_client().await;
    assert!(
        operator
            .runs()
            .restore_task(&task_id)
            .await
            .unwrap()
            .requeued
    );
    let (restored_id, restored_invocation) = tokio::time::timeout(TEST_TIMEOUT, rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(restored_id, task_id);
    assert!(restored_invocation > invocation);
    let result = tokio::time::timeout(TEST_TIMEOUT, run.output())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result["seed"], 1);
    assert_eq!(result["invocation"], restored_invocation);
    assert_eq!(result["task"], task_id);
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live Hatchet engine credentials; run explicitly with --ignored"]
async fn integration_streaming_is_incremental_across_a_long_idle_gap() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let owner = real_client().await;
    let publishing = owner.task::<serde_json::Value, serde_json::Value>("live-stream-publisher");
    let finish = owner.task::<serde_json::Value, serde_json::Value>("live-stream-finish");
    let begin = Arc::new(tokio::sync::Notify::new());
    let finish_gate = Arc::new(tokio::sync::Notify::new());
    let completed = Arc::new(AtomicBool::new(false));
    let worker = owner
        .worker("live-stream-worker")
        .register_task(&publishing, {
            let begin = Arc::clone(&begin);
            let finish_gate = Arc::clone(&finish_gate);
            move |_, ctx: Ctx| {
                let begin = Arc::clone(&begin);
                let finish_gate = Arc::clone(&finish_gate);
                async move {
                    assert_ne!(
                        ctx.task_run_id(),
                        ctx.workflow_run_id().0,
                        "DAG publication must use task identity"
                    );
                    // The pinned engine sends subscription headers with its first
                    // event. Publish readiness values until the consumer confirms it
                    // has actually received one, then start the ordered test data.
                    loop {
                        ctx.put_stream(serde_json::json!("ready")).await?;
                        tokio::select! {
                            _=begin.notified()=>break,
                            _=tokio::time::sleep(Duration::from_millis(100))=>{}
                        }
                    }
                    ctx.put_stream(serde_json::json!({"chunk":1})).await?;
                    tokio::time::sleep(Duration::from_secs(6)).await;
                    ctx.put_stream(serde_json::json!({"chunk":2})).await?;
                    finish_gate.notified().await;
                    Ok(serde_json::json!({"published":2}))
                }
            }
        })
        .register_task(&finish, {
            let completed = Arc::clone(&completed);
            move |_, _: Ctx| {
                let completed = Arc::clone(&completed);
                async move {
                    completed.store(true, Ordering::SeqCst);
                    Ok(serde_json::json!({"done":true}))
                }
            }
        })
        .build()
        .start()
        .unwrap();
    worker
        .wait_until_ready(Duration::from_secs(10))
        .await
        .unwrap();
    owner
        .register_workflow(
            WorkflowDeclaration::<serde_json::Value, serde_json::Value>::new("live-stream-dag")
                .step(&publishing, TaskConfig::default())
                .step_after(&finish, &[&publishing], TaskConfig::default()),
        )
        .await
        .unwrap();
    let run = owner
        .workflow::<serde_json::Value, serde_json::Value>("live-stream-dag")
        .run_no_wait(serde_json::json!({}), RunOpts::default())
        .await
        .unwrap();
    let consumer = real_client().await;
    let mut stream = tokio::time::timeout(
        TEST_TIMEOUT,
        consumer.runs().subscribe_to_stream(run.run_id()),
    )
    .await
    .unwrap()
    .unwrap();
    let ready = tokio::time::timeout(TEST_TIMEOUT, stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(ready, serde_json::json!("ready"));
    begin.notify_one();
    let first = tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let value = stream.next().await.unwrap().unwrap();
            if value != serde_json::json!("ready") {
                break value;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(first, serde_json::json!({"chunk":1}));
    assert!(!completed.load(Ordering::SeqCst));
    let second = tokio::time::timeout(TEST_TIMEOUT, stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(second, serde_json::json!({"chunk":2}));
    assert!(!completed.load(Ordering::SeqCst));
    finish_gate.notify_one();
    assert!(
        tokio::time::timeout(TEST_TIMEOUT, stream.next())
            .await
            .unwrap()
            .is_none()
    );
    let output = tokio::time::timeout(TEST_TIMEOUT, run.output())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output, serde_json::json!({"done":true}));
    worker.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live Hatchet engine credentials; run explicitly with --ignored"]
async fn integration_management_search_bulk_controls_and_resources() {
    use serde_json::json;
    let client = real_client().await;
    assert!(
        client
            .engine_version()
            .await
            .unwrap()
            .supports(EngineFeature::DurableEviction)
    );
    assert!(!client.tenant().get().await.unwrap().slug.is_empty());
    let task = client.task::<serde_json::Value, serde_json::Value>("management-batch2-task");
    let name = "management-batch2";
    client
        .register_workflow(
            WorkflowDeclaration::<serde_json::Value, serde_json::Value>::new(name)
                .step(&task, TaskConfig::default()),
        )
        .await
        .unwrap();
    let filter = client
        .filters()
        .create(name, "batch2", "input.ok == true")
        .await
        .unwrap();
    let record = client.filters().get_record(&filter.name).await.unwrap();
    assert_eq!(record.scope, "batch2");
    let updated = client
        .filters()
        .update_options(
            &filter.name,
            FilterUpdate {
                scope: Some("batch2-updated".into()),
                payload: Some(json!({"limit":5}).as_object().unwrap().clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(updated.scope, "batch2-updated");
    assert_eq!(updated.payload["limit"], 5);
    client.filters().delete(&filter.name).await.unwrap();
    let cel = client
        .cel()
        .debug("input.ok == true", &json!({"ok":true}))
        .await
        .unwrap();
    assert_eq!(cel.status, "SUCCESS");
    assert_eq!(cel.output, Some(true));
    let webhook_name = "management-batch2-webhook";
    let webhook = client
        .webhooks()
        .create_inbound(WebhookCreate {
            name: webhook_name.into(),
            source_name: WebhookSource::Generic,
            auth: WebhookAuth::ApiKey {
                header_name: "X-Test-Key".into(),
                api_key: "disposable-local-key".into(),
            },
            options: WebhookUpdate {
                event_key_expression: Some("'management-test'".into()),
                ..Default::default()
            },
        })
        .await
        .unwrap();
    assert_eq!(webhook.name, webhook_name);
    assert_eq!(
        client.webhooks().get(webhook_name).await.unwrap().auth_type,
        "API_KEY"
    );
    assert_eq!(
        client
            .webhooks()
            .update(
                webhook_name,
                WebhookUpdate {
                    static_payload: Some(json!({"fixed":true}).as_object().unwrap().clone()),
                    ..Default::default()
                }
            )
            .await
            .unwrap()
            .configuration
            .static_payload
            .unwrap()["fixed"],
        true
    );
    assert!(
        client
            .webhooks()
            .list_inbound()
            .await
            .unwrap()
            .iter()
            .any(|w| w.name == webhook_name)
    );
    client
        .webhooks()
        .delete_inbound(webhook_name)
        .await
        .unwrap();
    let schedule = client
        .schedules()
        .create(
            name,
            "management-schedule",
            "2099-01-01T00:00:00Z",
            json!({}),
        )
        .await
        .unwrap();
    let schedule = client
        .schedules()
        .reschedule(&schedule.name, "2099-02-01T00:00:00Z")
        .await
        .unwrap();
    assert!(schedule.expression.starts_with("2099-02-01"));
    let update = client
        .schedules()
        .bulk_update(vec![ScheduleUpdate {
            id: schedule.name.clone(),
            trigger_at: "2099-03-01T00:00:00Z".into(),
        }])
        .await
        .unwrap();
    assert!(update.errors.is_empty(), "{:?}", update.errors);
    assert_eq!(update.updated_ids, vec![schedule.name.clone()]);
    let deleted = client
        .schedules()
        .bulk_delete(vec![schedule.name.clone()])
        .await
        .unwrap();
    assert!(deleted.errors.is_empty());
    assert_eq!(deleted.deleted_ids, vec![schedule.name]);
    let options = WorkflowPauseOptions {
        paused_workflow_queue_ttl: "1h".into(),
        paused_workflow_cron_run_queue_behavior: PausedRunBehavior::Queue,
        paused_workflow_scheduled_run_queue_behavior: PausedRunBehavior::Queue,
    };
    if client
        .engine_version()
        .await
        .unwrap()
        .supports(EngineFeature::WorkflowPause)
    {
        client.workflows().pause(name, options).await.unwrap();
        client.workflows().unpause(name).await.unwrap();
    } else {
        assert!(matches!(
            client.workflows().pause(name, options).await,
            Err(Error::UnsupportedOperation(_))
        ));
    }
    let opts = RunOpts {
        additional_metadata: std::collections::HashMap::from([(
            "batch".into(),
            "management-2".into(),
        )]),
        ..Default::default()
    };
    let workflow = client.workflow::<serde_json::Value, serde_json::Value>(name);
    let first = workflow
        .run_no_wait(json!({"ok":true}), opts.clone())
        .await
        .unwrap();
    let second = workflow
        .run_no_wait(json!({"ok":true}), opts)
        .await
        .unwrap();
    let query = RunQuery {
        workflow_names: vec![name.into()],
        additional_metadata: std::collections::BTreeMap::from([(
            "batch".into(),
            "management-2".into(),
        )]),
        ..Default::default()
    };
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let page = client
                .runs()
                .list_page(RunListOptions {
                    filter: query.clone(),
                    limit: Some(1),
                    ..Default::default()
                })
                .await
                .unwrap();
            if !page.rows.is_empty() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    let cancelled = client
        .runs()
        .cancel_many(RunSelection::ExternalIds(vec![
            first.run_id().0.clone(),
            second.run_id().0.clone(),
        ]))
        .await
        .unwrap();
    assert_eq!(cancelled.ids.len(), 2);
    wait_for_status(&client, first.run_id(), RunStatus::Cancelled).await;
    wait_for_status(&client, second.run_id(), RunStatus::Cancelled).await;
    tokio::time::timeout(TEST_TIMEOUT, async {
        loop {
            let metrics = client
                .metrics()
                .get_task_status_metrics(RunListOptions {
                    filter: query.clone(),
                    ..Default::default()
                })
                .await
                .unwrap();
            if metrics
                .iter()
                .any(|m| m.status == "CANCELLED" && m.count >= 2)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    let mut replay_query = query;
    replay_query.statuses = vec![RunStatus::Cancelled];
    let replayed = client
        .runs()
        .replay_many(RunSelection::Filter(replay_query))
        .await
        .unwrap();
    assert_eq!(replayed.ids.len(), 2);
    let worker = client
        .worker("management-batch2-worker")
        .register_task(&task, |input, _: Ctx| async move { Ok(input) })
        .build()
        .start()
        .unwrap();
    worker
        .wait_until_ready(Duration::from_secs(10))
        .await
        .unwrap();
    for run in [&first, &second] {
        assert_eq!(
            tokio::time::timeout(TEST_TIMEOUT, run.output())
                .await
                .unwrap()
                .unwrap()["ok"],
            true
        );
    }
    assert!(
        client
            .runs()
            .get_details(first.run_id())
            .await
            .unwrap()
            .done
    );
    client.metrics().get_task_queue_metrics().await.unwrap();
    client.metrics().get_task_stats().await.unwrap();
    // This baseline fixture deliberately has no Prometheus federation server.
    let error = client
        .metrics()
        .scrape_prometheus_metrics()
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("Prometheus metrics are not enabled"),
        "{error}"
    );
    worker.shutdown().await.unwrap();
    client.workflows().delete(name).await.unwrap();
    assert!(client.workflows().get(name).await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires live Hatchet engine credentials; run explicitly with --ignored"]
async fn integration_durable_bulk_fanout_preserves_all_128_child_results() {
    use serde_json::{Value, json};
    let client = real_client().await;
    let child = client.task::<Value, Value>("parity-fanout-child");
    let parent = client.durable_task::<Value, Value>("parity-fanout-parent");
    let children = client.workflow::<Value, Value>("parity-fanout-child-workflow");
    let worker = client
        .worker("parity-fanout-worker")
        .slots(64)
        .register_task(&child, |input, _: Ctx| async { Ok(input) })
        .register_durable_task(&parent, move |_, ctx: DurableCtx| {
            let children = children.clone();
            async move {
                let runs = ctx
                    .spawn_children(
                        &children,
                        (0..128).map(|i| json!({"index":i})).collect::<Vec<_>>(),
                        RunOpts::default(),
                    )
                    .await?;
                let mut values = vec![];
                for run in runs {
                    values.push(run.output().await?["index"].as_u64().unwrap());
                }
                values.sort_unstable();
                Ok(json!({"indices":values}))
            }
        })
        .build()
        .start()
        .unwrap();
    worker
        .wait_until_ready(Duration::from_secs(15))
        .await
        .unwrap();
    client
        .register_workflow(
            WorkflowDeclaration::<Value, Value>::new("parity-fanout-child-workflow")
                .step(&child, TaskConfig::default()),
        )
        .await
        .unwrap();
    client
        .register_workflow(
            WorkflowDeclaration::<Value, Value>::new("parity-fanout-parent-workflow")
                .durable_step(&parent, TaskConfig::default()),
        )
        .await
        .unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(90),
        client
            .workflow::<Value, Value>("parity-fanout-parent-workflow")
            .run(json!({}), RunOpts::default()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result["indices"], json!((0..128).collect::<Vec<_>>()));
    worker.shutdown().await.unwrap();
}

#[cfg(feature = "opentelemetry")]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires live Hatchet engine credentials; run explicitly with --ignored"]
async fn integration_native_tracing_and_middleware_propagate_across_child_runs() {
    use opentelemetry::{global, trace::TraceContextExt};
    use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
    use serde_json::{Value, json};
    #[derive(Debug)]
    struct Observe(Arc<std::sync::atomic::AtomicUsize>);
    #[async_trait::async_trait]
    impl TaskMiddleware for Observe {
        async fn handle(&self, invocation: TaskInvocation, next: Next) -> Result<Value> {
            assert!(invocation.metadata().contains_key("traceparent"));
            self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            next.run(invocation).await
        }
    }
    let exporter = InMemorySpanExporter::default();
    let provider = SdkTracerProvider::builder()
        .with_simple_exporter(exporter.clone())
        .build();
    global::set_tracer_provider(provider.clone());
    let client = real_client().await;
    let child = client.task::<Value, Value>("otel-live-child");
    let parent = client.task::<Value, Value>("otel-live-parent");
    let child_workflow = client.workflow::<Value, Value>("otel-live-child-workflow");
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let worker=client.worker("otel-live-worker").middleware(Observe(Arc::clone(&calls))).register_task(&child,|_,_:Ctx|async{Ok(json!({"trace_id":opentelemetry::Context::current().span().span_context().trace_id().to_string()}))}).register_task(&parent,move |_,ctx:Ctx|{let child=child_workflow.clone();async move{let run=ctx.run_child(&child,json!({}),RunOpts::default()).await?;run.output().await}}).build().start().unwrap();
    worker
        .wait_until_ready(Duration::from_secs(15))
        .await
        .unwrap();
    client
        .register_workflow(
            WorkflowDeclaration::<Value, Value>::new("otel-live-child-workflow")
                .step(&child, TaskConfig::default()),
        )
        .await
        .unwrap();
    client
        .register_workflow(
            WorkflowDeclaration::<Value, Value>::new("otel-live-parent-workflow")
                .step(&parent, TaskConfig::default()),
        )
        .await
        .unwrap();
    let result = tokio::time::timeout(
        TEST_TIMEOUT,
        client
            .workflow::<Value, Value>("otel-live-parent-workflow")
            .run(json!({}), RunOpts::default()),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 2);
    provider.force_flush().unwrap();
    let spans = exporter.get_finished_spans().unwrap();
    let traced = spans
        .iter()
        .filter(|s| s.span_context.trace_id().to_string() == result["trace_id"].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(traced.len(), 4);
    assert_eq!(
        traced
            .iter()
            .filter(|s| s.name == "hatchet.start_step_run")
            .count(),
        2
    );
    assert_eq!(
        traced
            .iter()
            .filter(|s| s.name == "hatchet.run_workflow")
            .count(),
        2
    );
    worker.shutdown().await.unwrap();
    provider.shutdown().unwrap();
    global::set_tracer_provider(opentelemetry::trace::noop::NoopTracerProvider::new());
}
