//! Process ownership/handshake contracts using a controlled native sidecar peer.
#![cfg(unix)]
use another_hatchet_sdk::{Error, embedded::*};
use std::os::unix::fs::PermissionsExt;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
fn fixture(mode: &str) -> (tempfile::TempDir, EmbeddedOptions) {
    let dir = tempfile::Builder::new()
        .prefix("embedded-fixture-")
        .tempdir()
        .unwrap();
    let binary = dir.path().join("sidecar.py");
    std::fs::write(&binary,r#"#!/usr/bin/env python3
import os,sys,json,pathlib,signal,time,select
args=sys.argv[1:]
def value(flag,default=''):
 return args[args.index(flag)+1] if flag in args else default
mode=value('-log-level','ready')
def stop(*unused):
 pathlib.Path('stopped').write_text('graceful')
 sys.exit(0)
signal.signal(signal.SIGTERM,signal.SIG_IGN if mode=='ignore' else stop)
pathlib.Path('pid.json').write_text(json.dumps({'pid':os.getpid(),'group':os.getpgid(0),'args':args}))
if mode=='exit':sys.exit(7)
if mode not in ['timeout']:
 handshake=pathlib.Path(value('-handshake-file'))
 if mode=='malformed':data='{not json'
 elif mode=='oversized':data='x'*65537
 else:data=json.dumps({'token':'private-handshake-token','tenant_id':'00000000-0000-4000-8000-000000000001','grpc_address':'remote.example:7077' if mode=='remote' else 'localhost:7077','api_url':'' if '-no-api' in args else 'http://localhost:28243'})
 with os.fdopen(os.open(handshake,os.O_WRONLY|os.O_CREAT,0o600),'w') as f:f.write(data)
while True:
 if mode!='ignore':
  ready,_,_=select.select([sys.stdin],[],[],0.05)
  if ready and not os.read(sys.stdin.fileno(),1):stop()
 else:time.sleep(0.05)
"#).unwrap();
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o755)).unwrap();
    let options = EmbeddedOptions {
        binary_path: Some(binary),
        working_dir: Some(dir.path().to_owned()),
        log_level: Some(mode.into()),
        ready_timeout: Duration::from_secs(3),
        shutdown_timeout: Duration::from_millis(500),
        ..Default::default()
    };
    (dir, options)
}
fn pid(dir: &Path) -> Option<i32> {
    std::fs::read(dir.join("pid.json"))
        .ok()
        .and_then(|b| serde_json::from_slice::<serde_json::Value>(&b).ok())
        .and_then(|v| v["pid"].as_i64())
        .and_then(|p| p.try_into().ok())
}
async fn wait_pid(dir: &Path) -> i32 {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let Some(pid) = pid(dir) {
                break pid;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}
async fn gone(pid: i32) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if rustix::process::test_kill_process(rustix::process::Pid::from_raw(pid).unwrap())
                .is_err()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn embedded_handshake_wires_config_redacts_secrets_and_stop_reaps_idempotently() {
    let (dir, mut options) = fixture("ready");
    options.database_url = Some("postgres://private-db-secret".into());
    options.rabbitmq_url = Some("amqp://private-mq-secret".into());
    options.postgres_data_dir = Some(PathBuf::from("persisted"));
    options.grpc_port = Some(7077);
    options.api_port = Some(28243);
    options.run_migrations = false;
    let debug = format!("{options:?}");
    assert!(!debug.contains("private-db-secret") && !debug.contains("private-mq-secret"));
    let mut engine = EmbeddedEngine::start(options).await.unwrap();
    let child = engine.process_id().unwrap();
    assert!(engine.is_running().unwrap());
    let config = engine.connection().config().unwrap();
    assert_eq!(config.host_port, "localhost:7077");
    assert_eq!(config.server_url, "http://localhost:28243");
    assert_eq!(config.tenant_id, "00000000-0000-4000-8000-000000000001");
    for debug in [
        format!("{engine:?}"),
        format!("{:?}", engine.connection()),
        format!("{config:?}"),
    ] {
        assert!(!debug.contains("private-handshake-token"));
    }
    let recorded: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("pid.json")).unwrap()).unwrap();
    assert_eq!(recorded["pid"], child);
    assert_eq!(recorded["group"], child);
    let args = recorded["args"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect::<Vec<_>>();
    for flag in [
        "-database-url",
        "-rabbitmq-url",
        "-postgres-data-dir",
        "-grpc-port",
        "-api-port",
        "-no-migrations",
    ] {
        assert!(args.contains(&flag));
    }
    let handshake =
        PathBuf::from(args[args.iter().position(|a| *a == "-handshake-file").unwrap() + 1]);
    assert!(!handshake.parent().unwrap().exists());
    engine.stop().await.unwrap();
    engine.stop().await.unwrap();
    assert!(!engine.is_running().unwrap());
    gone(child as i32).await;
    assert!(dir.path().join("stopped").exists());
}
#[tokio::test]
async fn embedded_api_less_mode_has_raw_connection_but_rejects_full_client() {
    let (_dir, mut options) = fixture("ready");
    options.start_api = false;
    let mut engine = EmbeddedEngine::start(options).await.unwrap();
    assert_eq!(engine.connection().api_url(), "");
    assert!(matches!(
        engine.client().await,
        Err(Error::UnsupportedOperation(_))
    ));
    engine.stop().await.unwrap();
}
#[tokio::test]
async fn embedded_invalid_handshakes_exit_and_timeout_stop_the_process() {
    for mode in ["malformed", "oversized", "remote", "exit", "timeout"] {
        let (dir, mut options) = fixture(mode);
        if mode == "timeout" {
            options.ready_timeout = Duration::from_secs(1);
        }
        let error = EmbeddedEngine::start(options).await.unwrap_err();
        assert!(!error.to_string().contains("private-handshake-token"));
        if let Some(pid) = pid(dir.path()) {
            gone(pid).await;
        }
    }
}
#[tokio::test]
async fn embedded_cancelled_start_and_dropped_guard_request_cleanup() {
    let (dir, options) = fixture("timeout");
    let starting = tokio::spawn(EmbeddedEngine::start(options));
    let pid = wait_pid(dir.path()).await;
    starting.abort();
    assert!(starting.await.unwrap_err().is_cancelled());
    gone(pid).await;
    assert!(dir.path().join("stopped").exists());
    let (dir, options) = fixture("ready");
    let engine = EmbeddedEngine::start(options).await.unwrap();
    let pid = engine.process_id().unwrap();
    drop(engine);
    gone(pid as i32).await;
    assert!(dir.path().join("stopped").exists());
}
#[tokio::test]
async fn embedded_unresponsive_shutdown_is_forced_and_reported_as_an_error() {
    let (_dir, mut options) = fixture("ignore");
    options.shutdown_timeout = Duration::from_millis(50);
    let mut engine = EmbeddedEngine::start(options).await.unwrap();
    let pid = engine.process_id().unwrap();
    let error = engine.stop().await.unwrap_err();
    assert!(error.to_string().contains("force-stopped"));
    gone(pid as i32).await;
    assert_eq!(
        engine.stop().await.unwrap_err().to_string(),
        error.to_string()
    );
}
#[tokio::test]
async fn embedded_invalid_timeouts_and_supplied_binary_checksums_never_spawn() {
    let (dir, mut options) = fixture("ready");
    options.ready_timeout = Duration::ZERO;
    assert!(EmbeddedEngine::start(options).await.is_err());
    assert!(!dir.path().join("pid.json").exists());
    let (dir, mut options) = fixture("ready");
    options.checksum = Some("00".repeat(32));
    assert!(
        EmbeddedEngine::start(options)
            .await
            .unwrap_err()
            .to_string()
            .contains("checksum mismatch")
    );
    assert!(!dir.path().join("pid.json").exists());
}

#[tokio::test]
async fn embedded_api_less_full_client_rejects_before_spawning() {
    let (dir, mut options) = fixture("ready");
    options.start_api = false;
    assert!(matches!(
        EmbeddedClient::start(options).await,
        Err(Error::UnsupportedOperation(_))
    ));
    assert!(!dir.path().join("pid.json").exists());
}
#[tokio::test]
async fn embedded_cancelled_stop_preserves_the_original_shutdown_deadline() {
    let (_dir, mut options) = fixture("ignore");
    options.shutdown_timeout = Duration::from_millis(500);
    let mut engine = EmbeddedEngine::start(options).await.unwrap();
    let pid = engine.process_id().unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), engine.stop())
            .await
            .is_err()
    );
    tokio::time::sleep(Duration::from_millis(550)).await;
    let error = tokio::time::timeout(Duration::from_millis(150), engine.stop())
        .await
        .expect("a cancelled stop reset the shutdown deadline")
        .unwrap_err();
    assert!(error.to_string().contains("force-stopped"));
    gone(pid as i32).await;
}
