use super::*;
use std::{path::Path, process::Stdio};
use tokio::io::AsyncReadExt;
use tokio::process::{Child, Command};

pub(super) struct Process {
    child: Option<Child>,
    timeout: Duration,
    stopped: Option<Result<()>>,
    shutdown_deadline: Option<tokio::time::Instant>,
}
fn error(message: impl Into<String>) -> Error {
    Error::Worker(message.into())
}
fn request_stop(child: &mut Child) {
    drop(child.stdin.take());
    #[cfg(unix)]
    if let Some(pid) = child
        .id()
        .and_then(|id| i32::try_from(id).ok())
        .and_then(rustix::process::Pid::from_raw)
    {
        let _ = rustix::process::kill_process(pid, rustix::process::Signal::TERM);
    }
}
async fn reap(child: &mut Child, timeout: Duration) -> Result<()> {
    match tokio::time::timeout(timeout, child.wait()).await {
        Ok(Ok(status)) if status.success() => Ok(()),
        Ok(Ok(status)) => Err(error(format!(
            "embedded sidecar exited abnormally: {status}"
        ))),
        Ok(Err(e)) => Err(error(format!("cannot reap embedded sidecar: {e}"))),
        Err(_) => {
            #[cfg(unix)]
            if let Some(pid) = child
                .id()
                .and_then(|id| i32::try_from(id).ok())
                .filter(|id| *id > 1)
                .and_then(rustix::process::Pid::from_raw)
            {
                let _ = rustix::process::kill_process_group(pid, rustix::process::Signal::KILL);
            }
            let _ = child.start_kill();
            tokio::time::timeout(Duration::from_secs(5), child.wait())
                .await
                .map_err(|_| error("embedded sidecar could not be reaped after force-stop"))?
                .map_err(|e| error(format!("cannot reap force-stopped embedded sidecar: {e}")))?;
            Err(error(
                "embedded shutdown timed out; sidecar was force-stopped",
            ))
        }
    }
}
impl Process {
    pub(super) fn id(&self) -> Option<u32> {
        self.child.as_ref().and_then(Child::id)
    }
    pub(super) fn is_running(&mut self) -> Result<bool> {
        self.child
            .as_mut()
            .map(|child| {
                child
                    .try_wait()
                    .map(|status| status.is_none())
                    .map_err(|e| error(e.to_string()))
            })
            .unwrap_or(Ok(false))
    }
    pub(super) async fn stop(&mut self) -> Result<()> {
        if let Some(result) = &self.stopped {
            return result.clone();
        }
        let result = if let Some(child) = &mut self.child {
            let deadline = *self
                .shutdown_deadline
                .get_or_insert_with(|| tokio::time::Instant::now() + self.timeout);
            request_stop(child);
            reap(
                child,
                deadline.saturating_duration_since(tokio::time::Instant::now()),
            )
            .await
        } else {
            Ok(())
        };
        self.child.take();
        self.stopped = Some(result.clone());
        result
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            request_stop(&mut child);
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                let timeout = self
                    .shutdown_deadline
                    .map(|deadline| deadline.saturating_duration_since(tokio::time::Instant::now()))
                    .unwrap_or(self.timeout);
                runtime.spawn(async move {
                    let _ = reap(&mut child, timeout).await;
                });
            }
            // If the runtime is gone, SIGTERM/stdin EOF still reaches the
            // sidecar; its own shutdown owns bundled Postgres cleanup.
        }
    }
}
pub(super) async fn start(
    binary: &Path,
    options: &EmbeddedOptions,
) -> Result<(Process, EmbeddedConnection)> {
    let temporary = tempfile::Builder::new()
        .prefix("hatchet-embedded-")
        .tempdir()
        .map_err(|e| {
            error(format!(
                "cannot create private embedded handshake directory: {e}"
            ))
        })?;
    let handshake = temporary
        .path()
        .canonicalize()
        .map_err(|e| error(e.to_string()))?
        .join("handshake.json");
    let mut command = Command::new(binary);
    command.arg("-handshake-file").arg(&handshake);
    if let Some(url) = &options.database_url {
        command.arg("-database-url").arg(url);
    }
    if let Some(url) = &options.rabbitmq_url {
        command.arg("-rabbitmq-url").arg(url);
    }
    if let Some(path) = &options.postgres_data_dir {
        command.arg("-postgres-data-dir").arg(path);
    }
    if let Some(port) = options.grpc_port {
        command.arg("-grpc-port").arg(port.to_string());
    }
    if let Some(port) = options.api_port {
        command.arg("-api-port").arg(port.to_string());
    }
    if !options.start_api {
        command.arg("-no-api");
    }
    if !options.run_migrations {
        command.arg("-no-migrations");
    }
    if let Some(level) = &options.log_level {
        command.arg("-log-level").arg(level);
    }
    if let Some(dir) = &options.working_dir {
        command.current_dir(dir);
    }
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::null())
        .stderr(Stdio::inherit());
    #[cfg(unix)]
    command.process_group(0);
    let child = command
        .spawn()
        .map_err(|e| error(format!("cannot start embedded sidecar: {e}")))?;
    let mut process = Process {
        child: Some(child),
        timeout: options.shutdown_timeout,
        stopped: None,
        shutdown_deadline: None,
    };
    let wait = async {
        let mut heartbeat = tokio::time::Instant::now() + Duration::from_secs(30);
        loop {
            if let Some(status) = process
                .child
                .as_mut()
                .expect("owned sidecar")
                .try_wait()
                .map_err(|e| error(e.to_string()))?
            {
                return Err(error(format!(
                    "embedded sidecar exited before readiness: {status}"
                )));
            }
            match tokio::fs::File::open(&handshake).await {
                Ok(file) => {
                    let mut bytes = Vec::new();
                    file.take(64 * 1024 + 1)
                        .read_to_end(&mut bytes)
                        .await
                        .map_err(|e| error(format!("cannot read embedded readiness: {e}")))?;
                    if bytes.len() > 64 * 1024 {
                        return Err(error("embedded handshake exceeds size limit"));
                    }
                    let connection: EmbeddedConnection = serde_json::from_slice(&bytes)
                        .map_err(|_| error("embedded sidecar wrote a malformed handshake"))?;
                    connection.validate()?;
                    return Ok(connection);
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(error(format!("cannot read embedded readiness: {e}"))),
            }
            if tokio::time::Instant::now() >= heartbeat {
                eprintln!("hatchet embedded: still waiting for engine readiness");
                heartbeat += Duration::from_secs(30);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    };
    let connection = match tokio::time::timeout(options.ready_timeout, wait).await {
        Ok(Ok(connection)) => connection,
        Ok(Err(error)) => {
            let _ = process.stop().await;
            return Err(error);
        }
        Err(_) => {
            let _ = process.stop().await;
            return Err(error("embedded engine readiness timed out"));
        }
    };
    drop(temporary);
    Ok((process, connection))
}
