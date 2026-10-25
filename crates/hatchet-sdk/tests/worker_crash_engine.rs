//! Actual worker-process death; confined to the disposable smoke engine.
#![cfg(feature = "integration")]
use another_hatchet_sdk::*;
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio::process::{Child, Command};

fn owned_engine() {
    let engine =
        std::env::var("HATCHET_TEST_ENGINE_CONTAINER").expect("use disposable smoke runner");
    assert!(engine.starts_with("hatchet-rust-") && engine.ends_with("-hatchet"));
}
async fn client() -> Client<GrpcTransport> {
    Client::new(Arc::new(
        GrpcTransport::from_config(HatchetConfig::from_env().unwrap())
            .await
            .unwrap(),
    ))
}
fn config() -> TaskConfig {
    TaskConfig::builder()
        .execution_timeout(10)
        .retries(1)
        .build()
}
async fn record(root: &Path, name: &str, value: Value) {
    let temporary = root.join(format!("{name}.partial"));
    tokio::fs::write(&temporary, serde_json::to_vec(&value).unwrap())
        .await
        .unwrap();
    tokio::fs::rename(temporary, root.join(format!("{name}.json")))
        .await
        .unwrap();
}
async fn read(root: &Path, name: &str) -> Value {
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Ok(bytes) = tokio::fs::read(root.join(format!("{name}.json"))).await {
                break serde_json::from_slice(&bytes).unwrap();
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("child checkpoint deadline")
}
fn spawn(root: &Path, generation: &str, normal: &str, durable: &str) -> Child {
    let log = std::fs::File::create(root.join(format!("{generation}.log"))).unwrap();
    Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "worker_crash_process_helper", "--nocapture"])
        .env("HATCHET_TEST_CRASH_CHILD_DIR", root)
        .env("HATCHET_TEST_CRASH_GENERATION", generation)
        .env("HATCHET_TEST_CRASH_NORMAL", normal)
        .env("HATCHET_TEST_CRASH_DURABLE", durable)
        .stdout(log.try_clone().unwrap())
        .stderr(log)
        .kill_on_drop(true)
        .spawn()
        .unwrap()
}
// An ordinary no-op test outside the explicitly opted-in child process. Keeping
// the helper in this executable tests the exact SDK build without shipping a bin.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn worker_crash_process_helper() {
    let Ok(root) = std::env::var("HATCHET_TEST_CRASH_CHILD_DIR") else {
        return;
    };
    owned_engine();
    let root = PathBuf::from(root);
    let generation = std::env::var("HATCHET_TEST_CRASH_GENERATION").unwrap();
    let original = generation == "original";
    let client = client().await;
    let normal = client
        .task::<Value, Value>(std::env::var("HATCHET_TEST_CRASH_NORMAL").unwrap())
        .with_config(config());
    let durable = client
        .durable_task::<Value, Value>(std::env::var("HATCHET_TEST_CRASH_DURABLE").unwrap())
        .with_config(config());
    let normal_root = root.clone();
    let durable_root = root.clone();
    let worker=client.worker(format!("crash-{generation}"))
        .register_task(&normal,move |_:Value,ctx:InMemoryContext<GrpcTransport>|{
            let root=normal_root.clone();async move {
                let result=json!({"pid":std::process::id(),"retry":ctx.retry_count(),"task_id":ctx.task_run_id()});
                if original {record(&root,"normal-started",result).await;std::future::pending().await} else {Ok(result)}
            }
        })
        .register_durable_task(&durable,move |_:Value,ctx:InMemoryDurableContext<GrpcTransport>|{
            let root=durable_root.clone();async move {
                let seed=ctx.memo("crash-seed",json!({"pid":std::process::id()})).await?;
                let result=json!({"pid":std::process::id(),"seed":seed,"retry":ctx.retry_count(),"task_id":ctx.task_run_id()});
                if original {record(&root,"durable-started",result).await;std::future::pending().await} else {Ok(result)}
            }
        }).build().start().unwrap();
    worker
        .wait_until_ready(Duration::from_secs(20))
        .await
        .unwrap();
    record(&root, &format!("{generation}-ready"), json!(true)).await;
    while !root.join("stop").exists() {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    worker.shutdown().await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires disposable engine and HATCHET_TEST_ENGINE_CONTAINER"]
async fn killed_worker_reassigns_inflight_normal_and_durable_tasks_and_replays_memo() {
    owned_engine();
    let root = tempfile::tempdir().unwrap();
    let client = client().await;
    let id = uuid::Uuid::new_v4();
    let normal_name = format!("crash-normal-{id}");
    let durable_name = format!("crash-durable-{id}");
    let normal = client.task::<Value, Value>(&normal_name);
    let durable = client.durable_task::<Value, Value>(&durable_name);
    client
        .register_workflow(
            WorkflowDeclaration::<Value, Value>::new(&normal_name).step(&normal, config()),
        )
        .await
        .unwrap();
    client
        .register_workflow(
            WorkflowDeclaration::<Value, Value>::new(&durable_name)
                .durable_step(&durable, config()),
        )
        .await
        .unwrap();
    let mut original = spawn(root.path(), "original", &normal_name, &durable_name);
    read(root.path(), "original-ready").await;
    let normal_run = client
        .workflow::<Value, Value>(&normal_name)
        .run_no_wait(json!({}), RunOpts::default())
        .await
        .unwrap();
    let durable_run = client
        .workflow::<Value, Value>(&durable_name)
        .run_no_wait(json!({}), RunOpts::default())
        .await
        .unwrap();
    let normal_before = read(root.path(), "normal-started").await;
    let durable_before = read(root.path(), "durable-started").await;
    let killed_pid = original.id().unwrap();
    assert_eq!(durable_before["seed"]["pid"], killed_pid);
    original.kill().await.unwrap();
    let killed = original.wait().await.unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(killed.signal(), Some(9));
    }
    #[cfg(not(unix))]
    assert!(!killed.success());
    let mut replacement = spawn(root.path(), "replacement", &normal_name, &durable_name);
    read(root.path(), "replacement-ready").await;
    let results = tokio::time::timeout(Duration::from_secs(150), async {
        tokio::join!(normal_run.output(), durable_run.output())
    })
    .await;
    tokio::fs::write(root.path().join("stop"), b"stop")
        .await
        .unwrap();
    let stopped = tokio::time::timeout(Duration::from_secs(15), replacement.wait())
        .await
        .unwrap()
        .unwrap();
    assert!(stopped.success());
    let (normal_after, durable_after) = results.expect("worker crash recovery deadline");
    let normal_after = normal_after.unwrap();
    let durable_after = durable_after.unwrap();
    assert_ne!(normal_after["pid"], killed_pid);
    assert_ne!(durable_after["pid"], killed_pid);
    assert_eq!(normal_after["task_id"], normal_before["task_id"]);
    assert_eq!(durable_after["task_id"], durable_before["task_id"]);
    assert_eq!(durable_after["seed"], durable_before["seed"]);
    assert!(normal_after["retry"].as_u64().unwrap() <= 1);
    assert!(durable_after["retry"].as_u64().unwrap() <= 1);
    eprintln!("worker_crash_recovered normal={normal_after} durable={durable_after}");
    client.workflows().delete(&normal_name).await.unwrap();
    client.workflows().delete(&durable_name).await.unwrap();
}
