//! Actual official sidecar and bundled Postgres, without Docker or an API token.
#![cfg(all(feature = "integration", unix))]
use another_hatchet_sdk::{embedded::*, *};
use serde_json::{Value, json};
use std::{path::PathBuf, sync::Arc, time::Duration};
fn options(name: &str) -> EmbeddedOptions {
    let root = std::env::var_os("HATCHET_TEST_EMBEDDED_ROOT")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target/embedded-native")
        });
    let root = root.join(name);
    std::fs::create_dir_all(&root).unwrap();
    let root = root.canonicalize().unwrap();
    EmbeddedOptions {
        version: Some(
            std::env::var("HATCHET_TEST_EMBEDDED_VERSION")
                .expect("pin the official sidecar release for this test"),
        ),
        checksum: Some(
            std::env::var("HATCHET_TEST_EMBEDDED_CHECKSUM")
                .expect("pin the official platform asset SHA-256"),
        ),
        cache_dir: Some(root.parent().unwrap().join("cache")),
        postgres_data_dir: Some(root.join("postgres")),
        working_dir: Some(root),
        log_level: Some("error".into()),
        ..Default::default()
    }
}
async fn echo(
    client: &Client<GrpcTransport>,
    label: &str,
    input: Value,
    register: bool,
) -> WorkflowRunId {
    let task = client.task::<Value, Value>("native-embedded-task");
    let name = "native-embedded-workflow";
    if register {
        client
            .register_workflow(
                WorkflowDeclaration::<Value, Value>::new(name).step(&task, TaskConfig::default()),
            )
            .await
            .unwrap();
    }
    let worker = client
        .worker(label)
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
    let run = client
        .workflow::<Value, Value>(name)
        .run_no_wait(input.clone(), RunOpts::default())
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(30), run.output())
            .await
            .unwrap()
            .unwrap(),
        input
    );
    let id = run.run_id().clone();
    worker.shutdown().await.unwrap();
    id
}
fn postgres_pid(options: &EmbeddedOptions) -> i32 {
    std::fs::read_to_string(
        options
            .postgres_data_dir
            .as_ref()
            .unwrap()
            .join("data/postmaster.pid"),
    )
    .unwrap()
    .lines()
    .next()
    .unwrap()
    .parse()
    .unwrap()
}
async fn gone(pid: i32) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if rustix::process::test_kill_process(rustix::process::Pid::from_raw(pid).unwrap())
                .is_err()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires official sidecar download and bundled Postgres; pin version/checksum via HATCHET_TEST_EMBEDDED_*"]
async fn embedded_real_engine_runs_workflows_and_preserves_history_across_postgres_restart() {
    let opts = options("persistent");
    let mut embedded = EmbeddedClient::start(opts.clone()).await.unwrap();
    let pg = postgres_pid(&opts);
    let pid = embedded.engine().process_id().unwrap();
    let tenant = embedded.engine().connection().tenant_id().to_owned();
    assert!(!embedded.engine().connection().token().is_empty());
    let version = embedded.engine_version().await.unwrap();
    assert_eq!(
        version.to_string(),
        std::env::var("HATCHET_TEST_EMBEDDED_VERSION")
            .unwrap()
            .trim_start_matches('v')
    );
    let run = echo(
        &embedded,
        "native-embedded-before",
        json!({"persisted":true}),
        true,
    )
    .await;
    embedded.stop().await.unwrap();
    gone(pid as i32).await;
    gone(pg).await;
    let mut restarted = EmbeddedClient::start(opts.clone()).await.unwrap();
    assert_eq!(restarted.engine().connection().tenant_id(), tenant);
    let prior: WorkflowRunRef<Value> = restarted.run_ref(run.0);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(30), prior.output())
            .await
            .unwrap()
            .unwrap(),
        json!({"persisted":true})
    );
    echo(
        &restarted,
        "native-embedded-after",
        json!({"after_restart":true}),
        false,
    )
    .await;
    let pg = postgres_pid(&opts);
    restarted.stop().await.unwrap();
    gone(pg).await;
    assert!(
        !opts
            .postgres_data_dir
            .as_ref()
            .unwrap()
            .join("data/postmaster.pid")
            .exists()
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires official sidecar and bundled Postgres"]
async fn embedded_real_api_less_mode_exposes_authenticated_grpc_and_cleans_up() {
    let mut opts = options("api-less");
    opts.start_api = false;
    let mut engine = EmbeddedEngine::start(opts.clone()).await.unwrap();
    assert_eq!(engine.connection().api_url(), "");
    assert!(matches!(
        engine.client().await,
        Err(Error::UnsupportedOperation(_))
    ));
    let c = engine.connection();
    let transport = GrpcTransport::from_config(
        HatchetConfig::builder()
            .token(c.token())
            .tenant_id(c.tenant_id())
            .host_port(c.grpc_address())
            .server_url("http://localhost:1")
            .tls(TlsConfig::Insecure)
            .build()
            .unwrap(),
    )
    .await
    .unwrap();
    let raw = Client::new(Arc::new(transport));
    assert!(raw.engine_version().await.is_ok());
    let task = raw.task::<Value, Value>("native-api-less-task");
    raw.register_workflow(
        WorkflowDeclaration::<Value, Value>::new("native-api-less-workflow")
            .step(&task, TaskConfig::default()),
    )
    .await
    .unwrap();
    let pg = postgres_pid(&opts);
    engine.stop().await.unwrap();
    gone(pg).await;
}
