//! Operations whose engine prerequisites exceed the pinned baseline.
#![cfg(feature = "integration")]
use another_hatchet_sdk::*;
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live Hatchet engine v0.111.0 or later"]
async fn workflow_pause_holds_new_runs_until_unpause() {
    let transport = GrpcTransport::from_config(HatchetConfig::from_env().unwrap())
        .await
        .unwrap();
    let client = Client::new(Arc::new(transport));
    client
        .engine_version()
        .await
        .unwrap()
        .require(EngineFeature::WorkflowPause)
        .unwrap();
    let task = client.task::<Value, Value>("modern-pause-task");
    let name = "modern-pause-workflow";
    client
        .register_workflow(
            WorkflowDeclaration::<Value, Value>::new(name).step(&task, TaskConfig::default()),
        )
        .await
        .unwrap();
    let executions = Arc::new(AtomicUsize::new(0));
    let worker = client
        .worker("modern-pause-worker")
        .register_task(&task, {
            let executions = Arc::clone(&executions);
            move |input, _: InMemoryContext<GrpcTransport>| {
                executions.fetch_add(1, Ordering::SeqCst);
                async move { Ok(input) }
            }
        })
        .build()
        .start()
        .unwrap();
    worker
        .wait_until_ready(Duration::from_secs(15))
        .await
        .unwrap();
    // A newly registered, unpaused workflow has no stored pause-policy fields.
    client
        .workflows()
        .pause(
            name,
            WorkflowPauseOptions {
                paused_workflow_queue_ttl: "1h".into(),
                paused_workflow_cron_run_queue_behavior: PausedRunBehavior::Drop,
                paused_workflow_scheduled_run_queue_behavior: PausedRunBehavior::Queue,
            },
        )
        .await
        .unwrap();
    let run = client
        .workflow::<Value, Value>(name)
        .run_no_wait(json!({"resumed":true}), RunOpts::default())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(3)).await;
    assert_eq!(executions.load(Ordering::SeqCst), 0);
    assert_eq!(
        client.runs().get_status(run.run_id()).await.unwrap(),
        RunStatus::Queued
    );
    client.workflows().unpause(name).await.unwrap();
    let output = tokio::time::timeout(Duration::from_secs(30), run.output())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(output, json!({"resumed":true}));
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    worker.shutdown().await.unwrap();
    client.workflows().delete(name).await.unwrap();
}
