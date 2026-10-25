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

fn batch_task_config(broadcast_output: bool) -> TaskConfig {
    TaskConfig::builder()
        .batch(BatchConfig {
            max_size: 3,
            max_interval: Some(Duration::from_secs(2)),
            group_key: Some("input.group".into()),
            group_max_runs: Some(1),
            broadcast_output,
        })
        .retries(5)
        .build()
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live Hatchet engine v0.114.0 or later"]
async fn server_batches_coalesce_by_size_and_group_and_flush_on_interval() {
    use std::collections::BTreeMap;
    let client = real_client().await;
    let config = batch_task_config(false);
    let name = "modern-batches";
    let task = client
        .task::<BTreeMap<String, Value>, BTreeMap<String, Value>>("modern-batches-task")
        .with_config(config.clone());
    client
        .register_workflow(WorkflowDeclaration::<Value, Value>::new(name).step(&task, config))
        .await
        .unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let worker = client
        .worker("modern-batches-worker")
        .register_task(
            &task,
            move |input: BTreeMap<String, Value>, ctx: InMemoryContext<GrpcTransport>| {
                assert_eq!(ctx.batch_member_ids().len(), input.len());
                let group = input.values().next().unwrap()["group"].clone();
                assert!(input.values().all(|v| v["group"] == group));
                tx.send((input.len(), group)).unwrap();
                async move {
                    Ok(input
                        .into_iter()
                        .map(|(id, v)| (id, json!({"n":v["n"]})))
                        .collect())
                }
            },
        )
        .build()
        .start()
        .unwrap();
    worker
        .wait_until_ready(Duration::from_secs(15))
        .await
        .unwrap();
    let wf = client.workflow::<Value, Value>(name);
    let runs = wf
        .run_many_no_wait(
            (0..6).map(|n| json!({"n":n,"group":if n%2==0 {"a"} else {"b"}})),
            RunOpts::default(),
        )
        .await
        .unwrap();
    for (n, run) in runs.iter().enumerate() {
        assert_eq!(output(run).await, json!({"n":n}));
    }
    let mut sizes = Vec::new();
    for _ in 0..2 {
        sizes.push(
            tokio::time::timeout(Duration::from_secs(5), rx.recv())
                .await
                .unwrap()
                .unwrap()
                .0,
        );
    }
    assert_eq!(sizes, vec![3, 3]);
    let start = std::time::Instant::now();
    let lone = wf
        .run_no_wait(json!({"n":9,"group":"alone"}), RunOpts::default())
        .await
        .unwrap();
    assert_eq!(output(&lone).await, json!({"n":9}));
    assert!(start.elapsed() >= Duration::from_secs(1));
    assert_eq!(rx.recv().await.unwrap().0, 1);
    worker.shutdown().await.unwrap();
    client.workflows().delete(name).await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live Hatchet engine v0.114.0 or later"]
async fn server_batch_broadcast_and_whole_batch_failure_preserve_member_outcomes() {
    use std::collections::BTreeMap;
    let client = real_client().await;
    let name = "modern-batch-broadcast";
    let config = batch_task_config(true);
    let task = client
        .task::<BTreeMap<String, Value>, Value>("modern-batch-broadcast-task")
        .with_config(config.clone());
    client
        .register_workflow(WorkflowDeclaration::<Value, Value>::new(name).step(&task, config))
        .await
        .unwrap();
    let calls = Arc::new(AtomicUsize::new(0));
    let count = Arc::clone(&calls);
    let worker = client
        .worker("modern-batch-broadcast-worker")
        .register_task(
            &task,
            move |input: BTreeMap<String, Value>, _: InMemoryContext<GrpcTransport>| {
                count.fetch_add(1, Ordering::SeqCst);
                async move {
                    if input.values().any(|v| v["group"] == "failure") {
                        Err(Error::Worker("whole batch failed".into()))
                    } else {
                        Ok(json!({"shared":true}))
                    }
                }
            },
        )
        .build()
        .start()
        .unwrap();
    worker
        .wait_until_ready(Duration::from_secs(15))
        .await
        .unwrap();
    let wf = client.workflow::<Value, Value>(name);
    let good = wf
        .run_many_no_wait((0..3).map(|_| json!({"group":"ok"})), RunOpts::default())
        .await
        .unwrap();
    for run in good {
        assert_eq!(output(&run).await, json!({"shared":true}));
    }
    let failed = wf
        .run_many_no_wait(
            (0..3).map(|_| json!({"group":"failure"})),
            RunOpts::default(),
        )
        .await
        .unwrap();
    for run in failed {
        assert!(
            tokio::time::timeout(Duration::from_secs(30), run.output())
                .await
                .unwrap()
                .unwrap_err()
                .to_string()
                .contains("whole batch failed")
        );
    }
    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "batch retries must be disabled"
    );
    worker.shutdown().await.unwrap();
    client.workflows().delete(name).await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live Hatchet engine v0.114.0 or later"]
async fn server_batch_member_cancellation_preserves_other_members_and_all_cancel_stops_handler() {
    use std::collections::BTreeMap;
    let client = real_client().await;
    let name = "modern-batch-cancel";
    let config = batch_task_config(false);
    let task = client
        .task::<BTreeMap<String, Value>, BTreeMap<String, Value>>("modern-batch-cancel-task")
        .with_config(config.clone());
    client
        .register_workflow(WorkflowDeclaration::<Value, Value>::new(name).step(&task, config))
        .await
        .unwrap();
    let release = Arc::new(tokio::sync::Notify::new());
    let released = Arc::clone(&release);
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let cancelled = Arc::new(AtomicUsize::new(0));
    let observed = Arc::clone(&cancelled);
    let worker=client.worker("modern-batch-cancel-worker").register_task(&task,move |input:BTreeMap<String,Value>,ctx:InMemoryContext<GrpcTransport>| {
        let release=Arc::clone(&released);let tx=tx.clone();let observed=Arc::clone(&observed);
        async move { tx.send(()).unwrap();tokio::select! { _=release.notified()=>Ok(input), _=ctx.cancelled()=>{ observed.fetch_add(1,Ordering::SeqCst);Err(Error::non_retryable("cancelled")) } } }
    }).build().start().unwrap();
    worker
        .wait_until_ready(Duration::from_secs(15))
        .await
        .unwrap();
    let wf = client.workflow::<Value, Value>(name);
    let runs = wf
        .run_many_no_wait(
            (0..3).map(|n| json!({"n":n,"group":"partial"})),
            RunOpts::default(),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(15), rx.recv())
        .await
        .unwrap()
        .unwrap();
    runs[0].cancel().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if client.runs().get_status(runs[0].run_id()).await.unwrap() == RunStatus::Cancelled {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    release.notify_one();
    for (n, run) in runs.iter().enumerate().skip(1) {
        assert_eq!(output(run).await, json!({"n":n,"group":"partial"}));
    }
    assert!(runs[0].output().await.is_err());
    assert_eq!(cancelled.load(Ordering::SeqCst), 0);
    let runs = wf
        .run_many_no_wait(
            (0..3).map(|n| json!({"n":n,"group":"all"})),
            RunOpts::default(),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(15), rx.recv())
        .await
        .unwrap()
        .unwrap();
    for run in &runs {
        run.cancel().await.unwrap();
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        while cancelled.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    worker.shutdown().await.unwrap();
    client.workflows().delete(name).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live Hatchet engine v0.114.0 or later"]
async fn server_batch_handler_cancel_then_return_keeps_every_member_cancelled() {
    use std::collections::BTreeMap;
    let client = real_client().await;
    let name = "modern-batch-self-cancel";
    let config = batch_task_config(false);
    let task = client
        .task::<BTreeMap<String, Value>, BTreeMap<String, Value>>("modern-batch-self-cancel-task")
        .with_config(config.clone());
    client
        .register_workflow(WorkflowDeclaration::<Value, Value>::new(name).step(&task, config))
        .await
        .unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let worker = client
        .worker("modern-batch-self-cancel-worker")
        .register_task(
            &task,
            move |input: BTreeMap<String, Value>, ctx: InMemoryContext<GrpcTransport>| {
                let tx = tx.clone();
                async move {
                    ctx.cancel().await?;
                    assert!(ctx.is_cancelled());
                    tx.send(()).unwrap();
                    Ok(input)
                }
            },
        )
        .build()
        .start()
        .unwrap();
    worker
        .wait_until_ready(Duration::from_secs(15))
        .await
        .unwrap();
    let runs = client
        .workflow::<Value, Value>(name)
        .run_many_no_wait(
            (0..3).map(|n| json!({"n":n,"group":"self"})),
            RunOpts::default(),
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(15), rx.recv())
        .await
        .unwrap()
        .unwrap();
    for run in runs {
        assert!(
            tokio::time::timeout(Duration::from_secs(15), run.output())
                .await
                .unwrap()
                .is_err()
        );
        assert_eq!(
            client.runs().get_status(run.run_id()).await.unwrap(),
            RunStatus::Cancelled
        );
    }
    worker.shutdown().await.unwrap();
    client.workflows().delete(name).await.unwrap();
}
