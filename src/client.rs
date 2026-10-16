use crate::daemon::socket_path::resolve_runtime_socket;
use crate::protocol::{PROTOCOL_VERSION, RpcRequest, RpcRequestEnvelope, RpcResponse};
use anyhow::{Context, Result, bail};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use ulid::Ulid;

#[derive(Debug)]
pub struct RuntimeError(pub crate::protocol::RpcError);

impl std::fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.0.code, self.0.message)
    }
}
impl std::error::Error for RuntimeError {}

const MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

#[derive(Clone, Debug)]
pub struct RuntimeClient {
    runtime_root: PathBuf,
}

impl RuntimeClient {
    pub fn new(runtime_root: impl Into<PathBuf>) -> Self {
        Self {
            runtime_root: runtime_root.into(),
        }
    }

    pub fn runtime_root(&self) -> &Path {
        &self.runtime_root
    }

    pub async fn request(&self, request: RpcRequest) -> Result<serde_json::Value> {
        self.ensure_daemon().await?;
        let socket = resolve_runtime_socket(&self.runtime_root)?.socket_path;
        let mut stream = UnixStream::connect(&socket)
            .await
            .context("connect to terminal runtime daemon")?;
        let envelope = RpcRequestEnvelope {
            version: PROTOCOL_VERSION,
            request_id: Ulid::generate().to_string(),
            request,
        };
        let bytes = serde_json::to_vec(&envelope)?;
        stream.write_all(&bytes).await?;
        stream.write_all(b"\n").await?;
        stream.flush().await?;

        let reader = BufReader::new(stream);
        let mut line = Vec::new();
        let count = reader
            .take((MAX_RESPONSE_BYTES + 1) as u64)
            .read_until(b'\n', &mut line)
            .await?;
        if count == 0 {
            bail!("terminal runtime closed the IPC connection without a response")
        }
        if line.len() > MAX_RESPONSE_BYTES {
            bail!("terminal runtime response exceeds the 64 MiB IPC limit")
        }
        let response: RpcResponse = serde_json::from_slice(&line)?;
        if !response.ok {
            let error = response.error.context("runtime returned an empty error")?;
            return Err(RuntimeError(error).into());
        }
        response.result.context("runtime returned an empty result")
    }

    async fn ensure_daemon(&self) -> Result<()> {
        let paths = resolve_runtime_socket(&self.runtime_root)?;
        let socket = paths.socket_path;
        if UnixStream::connect(&socket).await.is_ok() {
            return Ok(());
        }
        let executable = std::env::current_exe().context("locate runtime executable")?;
        let log_path = paths.runtime_root.join("daemon.log");
        if std::fs::metadata(&log_path).is_ok_and(|m| m.len() > 2 * 1024 * 1024) {
            let _ = std::fs::rename(&log_path, self.runtime_root.join("daemon.log.1"));
        }
        use std::os::unix::fs::OpenOptionsExt;
        let log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .custom_flags(libc::O_NOFOLLOW)
            .open(&log_path)
            .context("open daemon diagnostics")?;
        let log_start_offset = log.metadata()?.len();
        let mut command = tokio::process::Command::new(executable);
        command
            .arg("daemon")
            .arg("--runtime-dir")
            .arg(&paths.runtime_root)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::from(log))
            // Shell/agent runners may tear down the CLI invocation's process
            // group after it returns. The daemon must outlive that client.
            .process_group(0);
        command.spawn().context("start terminal runtime daemon")?;

        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            if UnixStream::connect(&socket).await.is_ok() {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                if let Some(error) = daemon_start_error(&log_path, log_start_offset) {
                    bail!("terminal runtime daemon failed to start: {error}")
                }
                bail!("terminal runtime daemon did not create its socket within five seconds")
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

fn daemon_start_error(log_path: &Path, start_offset: u64) -> Option<String> {
    let log = std::fs::read_to_string(log_path).ok()?;
    let new_log = log.get(start_offset as usize..)?;
    let line = new_log
        .lines()
        .rev()
        .find(|line| line.starts_with("Error: "))?;
    let message = line.trim_start_matches("Error: ").trim();
    if let Some(bind) = message.find("bind daemon socket") {
        let details = message[bind + "bind daemon socket".len()..].trim();
        let (_, error) = details.rsplit_once(": ")?;
        return (!error.is_empty()).then(|| format!("socket bind failed: {error}"));
    }
    (!message.is_empty()).then(|| message.to_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bind_diagnostic_keeps_os_error_and_omits_socket_path() {
        let temp = tempfile::tempdir().unwrap();
        let log = temp.path().join("daemon.log");
        std::fs::write(
            &log,
            "Error: bind daemon socket: Address already in use (os error 98)\n",
        )
        .unwrap();
        let diagnostic = daemon_start_error(&log, 0).unwrap();
        assert!(diagnostic.contains("Address already in use"));
        assert!(!diagnostic.contains(temp.path().to_str().unwrap()));
    }

    #[test]
    fn daemon_start_diagnostic_ignores_errors_from_prior_attempts() {
        let temp = tempfile::tempdir().unwrap();
        let log = temp.path().join("daemon.log");
        std::fs::write(&log, "Error: stale startup error\n").unwrap();
        let start_offset = std::fs::metadata(&log).unwrap().len();
        use std::io::Write;
        std::fs::OpenOptions::new()
            .append(true)
            .open(&log)
            .unwrap()
            .write_all(b"Error: private runtime socket directory is not user-owned\n")
            .unwrap();

        let diagnostic = daemon_start_error(&log, start_offset).unwrap();
        assert!(diagnostic.contains("not user-owned"));
        assert!(!diagnostic.contains("stale startup error"));
    }
}
