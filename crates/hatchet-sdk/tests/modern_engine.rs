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

async fn real_client() -> Client<GrpcTransport> {
    Client::new(Arc::new(
        GrpcTransport::from_config(HatchetConfig::from_env().unwrap())
            .await
            .unwrap(),
    ))
}
async fn output(run: &WorkflowRunRef<Value>) -> Value {
    tokio::time::timeout(Duration::from_secs(45), run.output())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live Hatchet engine v0.111.0 or later"]
async fn idempotency_ttl_and_bulk_collisions_preserve_partial_success() {
    let client = real_client().await;
    let name = "modern-idempotency-ttl";
    let task = client.task::<Value, Value>("modern-idempotency-ttl-task");
    client
        .register_workflow(
            WorkflowDeclaration::<Value, Value>::new(name)
                .idempotency(IdempotencyConfig {
                    expression: "input.key".into(),
                    ttl: Duration::from_secs(3),
                    method: IdempotencyMethod::Ttl,
                })
                .step(&task, TaskConfig::default()),
        )
        .await
        .unwrap();
    let workflow = client.workflow::<Value, Value>(name);
    let first = workflow
        .run_no_wait(json!({"key":"one"}), RunOpts::default())
        .await
        .unwrap();
    match workflow
        .run_no_wait(json!({"key":"one"}), RunOpts::default())
        .await
        .unwrap_err()
    {
        Error::IdempotencyCollision(c) => assert_eq!(&c.existing_run_id, first.run_id()),
        e => panic!("{e:?}"),
    }
    let partial = match workflow
        .run_many_no_wait(
            [json!({"key":"one"}), json!({"key":"two"})],
            RunOpts::default(),
        )
        .await
        .unwrap_err()
    {
        Error::BulkIdempotencyCollision(result) => result,
        e => panic!("{e:?}"),
    };
    assert_eq!(partial.successful_run_ids.len(), 1);
    assert_eq!(partial.collisions.len(), 1);
    assert_eq!(&partial.collisions[0].existing_run_id, first.run_id());
    tokio::time::sleep(Duration::from_secs(4)).await;
    let after_ttl = workflow
        .run_no_wait(json!({"key":"one"}), RunOpts::default())
        .await
        .unwrap();
    assert_ne!(after_ttl.run_id(), first.run_id());
    let calls = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&calls);
    let worker = client
        .worker("modern-idempotency-ttl-worker")
        .register_task(&task, move |input, _: InMemoryContext<GrpcTransport>| {
            count.fetch_add(1, Ordering::SeqCst);
            async { Ok(input) }
        })
        .build()
        .start()
        .unwrap();
    worker
        .wait_until_ready(Duration::from_secs(15))
        .await
        .unwrap();
    assert_eq!(output(&first).await["key"], "one");
    assert_eq!(output(&after_ttl).await["key"], "one");
    let successful = client.run_ref::<Value>(partial.successful_run_ids[0].0.clone());
    assert_eq!(output(&successful).await["key"], "two");
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    worker.shutdown().await.unwrap();
    client.workflows().delete(name).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live Hatchet engine v0.111.0 or later"]
async fn status_idempotency_releases_the_key_after_terminal_completion() {
    let client = real_client().await;
    let name = "modern-idempotency-status";
    let task = client.task::<Value, Value>("modern-idempotency-status-task");
    client
        .register_workflow(
            WorkflowDeclaration::<Value, Value>::new(name)
                .idempotency(IdempotencyConfig {
                    expression: "input.key".into(),
                    ttl: Duration::from_secs(3600),
                    method: IdempotencyMethod::Status,
                })
                .step(&task, TaskConfig::default()),
        )
        .await
        .unwrap();
    let workflow = client.workflow::<Value, Value>(name);
    let first = workflow
        .run_no_wait(json!({"key":"same"}), RunOpts::default())
        .await
        .unwrap();
    assert!(matches!(
        workflow
            .run_no_wait(json!({"key":"same"}), RunOpts::default())
            .await,
        Err(Error::IdempotencyCollision(_))
    ));
    let worker = client
        .worker("modern-idempotency-status-worker")
        .register_task(&task, |input, _: InMemoryContext<GrpcTransport>| async {
            Ok(input)
        })
        .build()
        .start()
        .unwrap();
    worker
        .wait_until_ready(Duration::from_secs(15))
        .await
        .unwrap();
    output(&first).await;
    let second = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            match workflow
                .run_no_wait(json!({"key":"same"}), RunOpts::default())
                .await
            {
                Ok(run) => break run,
                Err(Error::IdempotencyCollision(_)) => {
                    tokio::time::sleep(Duration::from_millis(100)).await
                }
                Err(e) => panic!("{e}"),
            }
        }
    })
    .await
    .unwrap();
    assert_ne!(first.run_id(), second.run_id());
    assert_eq!(output(&second).await["key"], "same");
    worker.shutdown().await.unwrap();
    client.workflows().delete(name).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live Hatchet engine v0.114.0 or later"]
async fn rate_limit_pause_holds_queued_work_and_earlier_pauses_do_not_shorten_it() {
    use std::time::SystemTime;
    let client = real_client().await;
    let key = "modern-pause-rate-key";
    let name = "modern-pause-rate";
    client
        .rate_limits()
        .upsert(key, 100, RateLimitDuration::Second)
        .await
        .unwrap();
    let config = TaskConfig::builder()
        .rate_limit(RateLimit::new(1, 100, RateLimitDuration::Second).with_static_key(key))
        .build();
    let task = client.task::<Value, Value>("modern-rate-task");
    client
        .register_workflow(WorkflowDeclaration::<Value, Value>::new(name).step(&task, config))
        .await
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&calls);
    let worker = client
        .worker("modern-rate-worker")
        .register_task(&task, move |input, _: InMemoryContext<GrpcTransport>| {
            count.fetch_add(1, Ordering::SeqCst);
            async { Ok(input) }
        })
        .build()
        .start()
        .unwrap();
    worker
        .wait_until_ready(Duration::from_secs(15))
        .await
        .unwrap();
    let paused = client
        .rate_limits()
        .pause(key, SystemTime::now() + Duration::from_secs(5))
        .await
        .unwrap();
    let earlier = client
        .rate_limits()
        .pause(key, SystemTime::now() + Duration::from_secs(1))
        .await
        .unwrap();
    assert!(earlier.paused_until >= paused.paused_until);
    let run = client
        .workflow::<Value, Value>(name)
        .run_no_wait(json!({"done":true}), RunOpts::default())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_secs(2)).await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        client.runs().get_status(run.run_id()).await.unwrap(),
        RunStatus::Queued
    );
    assert_eq!(output(&run).await["done"], true);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    worker.shutdown().await.unwrap();
    client.workflows().delete(name).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live Hatchet engine"]
async fn explicit_sticky_worker_routing_reaches_the_selected_worker_for_single_and_bulk_runs() {
    let a = Arc::new(
        GrpcTransport::from_config(HatchetConfig::from_env().unwrap())
            .await
            .unwrap(),
    );
    let b = Arc::new(
        GrpcTransport::from_config(HatchetConfig::from_env().unwrap())
            .await
            .unwrap(),
    );
    let client = Client::new(Arc::clone(&a));
    let other = Client::new(Arc::clone(&b));
    let name = "modern-sticky";
    let task = client.task::<Value, Value>("modern-sticky-task");
    client
        .register_workflow(
            WorkflowDeclaration::<Value, Value>::new(name)
                .sticky(StickyStrategy::Hard)
                .step(&task, TaskConfig::default()),
        )
        .await
        .unwrap();
    let handler = |_: Value, ctx: InMemoryContext<GrpcTransport>| async move {
        Ok(json!({"worker":ctx.worker_id()}))
    };
    let first = client
        .worker("modern-sticky-a")
        .register_task(&task, handler)
        .build()
        .start()
        .unwrap();
    let second = other
        .worker("modern-sticky-b")
        .register_task(&task, handler)
        .build()
        .start()
        .unwrap();
    first
        .wait_until_ready(Duration::from_secs(15))
        .await
        .unwrap();
    second
        .wait_until_ready(Duration::from_secs(15))
        .await
        .unwrap();
    let a_id = a.worker_id().await.unwrap();
    let b_id = b.worker_id().await.unwrap();
    assert_ne!(a_id, b_id);
    let workflow = client.workflow::<Value, Value>(name);
    for target in [&a_id, &b_id] {
        let run = workflow
            .run_no_wait(
                json!({}),
                RunOpts {
                    sticky: Some(target.clone()),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(output(&run).await["worker"], target.as_str());
    }
    let runs = workflow
        .run_many_no_wait(
            [json!({}), json!({})],
            RunOpts {
                sticky: Some(b_id.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    for run in runs {
        assert_eq!(output(&run).await["worker"], b_id);
    }
    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
    client.workflows().delete(name).await.unwrap();
}
