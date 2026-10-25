//! Disposable-engine routing recovery checks, separate from ordinary scheduling.
#![cfg(feature = "integration")]
use another_hatchet_sdk::*;
use serde_json::{Value, json};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

async fn transport() -> Arc<GrpcTransport> {
    Arc::new(
        GrpcTransport::from_config(HatchetConfig::from_env().unwrap())
            .await
            .unwrap(),
    )
}
async fn check(client: &Client<GrpcTransport>, name: &str, target: &str) -> Result<Value> {
    let run = client
        .workflow::<Value, Value>(name)
        .run_no_wait(
            json!({}),
            RunOpts {
                sticky: Some(target.into()),
                ..Default::default()
            },
        )
        .await?;
    let result = tokio::time::timeout(Duration::from_secs(90), run.output())
        .await
        .map_err(|_| Error::Worker(format!("sticky output deadline for {}", run.run_id().0)))??;
    assert_eq!(result["worker"], target);
    Ok(result)
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable Hatchet engine and HATCHET_TEST_ENGINE_CONTAINER"]
async fn hard_sticky_routing_recovers_for_reconnected_and_new_workers() {
    let engine = std::env::var("HATCHET_TEST_ENGINE_CONTAINER").unwrap();
    assert!(engine.starts_with("hatchet-rust-") && engine.ends_with("-hatchet"));
    let a = transport().await;
    let b = transport().await;
    let client = Client::new(Arc::clone(&a));
    let other = Client::new(Arc::clone(&b));
    let name = format!("sticky-recovery-{}", uuid::Uuid::new_v4());
    let task = client.task::<Value, Value>(format!("{name}-task"));
    client
        .register_workflow(
            WorkflowDeclaration::<Value, Value>::new(&name)
                .sticky(StickyStrategy::Hard)
                .step(&task, TaskConfig::default()),
        )
        .await
        .unwrap();
    let handler = |_: Value, ctx: InMemoryContext<GrpcTransport>| async move {
        Ok(json!({"worker":ctx.worker_id()}))
    };
    let first = client
        .worker("sticky-recovery-a")
        .register_task(&task, handler)
        .build()
        .start()
        .unwrap();
    let second = other
        .worker("sticky-recovery-b")
        .register_task(&task, handler)
        .build()
        .start()
        .unwrap();
    first
        .wait_until_ready(Duration::from_secs(20))
        .await
        .unwrap();
    second
        .wait_until_ready(Duration::from_secs(20))
        .await
        .unwrap();
    let before = a.worker_id().await.unwrap();
    check(&client, &name, &before).await.unwrap();
    check(&client, &name, &b.worker_id().await.unwrap())
        .await
        .unwrap();
    let restart = tokio::task::spawn_blocking(move || {
        std::process::Command::new("docker")
            .args(["restart", "--", &engine])
            .output()
            .unwrap()
    })
    .await
    .unwrap();
    assert!(restart.status.success());
    first
        .wait_until_ready(Duration::from_secs(60))
        .await
        .unwrap();
    second
        .wait_until_ready(Duration::from_secs(60))
        .await
        .unwrap();
    let after = a.worker_id().await.unwrap();
    assert_ne!(
        before, after,
        "re-registration must refresh the routing target"
    );
    let c = transport().await;
    let fresh_client = Client::new(Arc::clone(&c));
    let cold_name = format!("{name}-cold");
    let cold_task = fresh_client.task::<Value, Value>(format!("{cold_name}-task"));
    fresh_client
        .register_workflow(
            WorkflowDeclaration::<Value, Value>::new(&cold_name)
                .sticky(StickyStrategy::Hard)
                .step(&cold_task, TaskConfig::default()),
        )
        .await
        .unwrap();
    let fresh = fresh_client
        .worker("sticky-recovery-new")
        .register_task(&task, handler)
        .register_task(&cold_task, handler)
        .build()
        .start()
        .unwrap();
    fresh
        .wait_until_ready(Duration::from_secs(20))
        .await
        .unwrap();
    let targets = [
        after,
        b.worker_id().await.unwrap(),
        c.worker_id().await.unwrap(),
    ];
    let start = Instant::now();
    let results = tokio::join!(
        check(&client, &name, &targets[0]),
        check(&client, &name, &targets[1]),
        check(&client, &name, &targets[2]),
        check(&client, &cold_name, &targets[2])
    );
    eprintln!(
        "sticky_recovery_elapsed_ms={} outcomes={:?}",
        start.elapsed().as_millis(),
        results
    );
    let bulk = client
        .workflow::<Value, Value>(&name)
        .run_many_no_wait(
            [json!({}), json!({})],
            RunOpts {
                sticky: Some(targets[2].clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    for run in bulk {
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(15), run.output())
                .await
                .unwrap()
                .unwrap()["worker"],
            targets[2]
        );
    }
    first.shutdown().await.unwrap();
    second.shutdown().await.unwrap();
    fresh.shutdown().await.unwrap();
    assert!(
        results.0.is_ok() && results.1.is_ok() && results.2.is_ok() && results.3.is_ok(),
        "sticky recovery failed: {results:?}"
    );
    client.workflows().delete(&name).await.unwrap();
    client.workflows().delete(&cold_name).await.unwrap();
}
