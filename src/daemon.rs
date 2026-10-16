use crate::protocol::{PROTOCOL_VERSION, RpcRequestEnvelope, RpcResponse};
use crate::service::SessionService;
use crate::session::create_private_dir;
use crate::viewer::ViewerServer;
use anyhow::{Context, Result, bail};
use std::fs;
use std::os::unix::fs::{FileTypeExt, MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::watch;

const MAX_REQUEST_BYTES: usize = 4 * 1024 * 1024;
const MAX_RESPONSE_BYTES: usize = 64 * 1024 * 1024;

#[path = "socket_path.rs"]
pub(crate) mod socket_path;

use socket_path::resolve_runtime_socket;

pub fn socket_path(runtime_root: &Path) -> Result<PathBuf> {
    Ok(resolve_runtime_socket(runtime_root)?.socket_path)
}

pub async fn run(runtime_root: PathBuf) -> Result<()> {
    let paths = resolve_runtime_socket(&runtime_root)?;
    create_private_dir(&paths.runtime_root)?;
    let runtime_root = paths.runtime_root;
    let socket = paths.socket_path;
    match fs::symlink_metadata(&socket) {
        Ok(metadata) => {
            if !metadata.file_type().is_socket() {
                bail!(
                    "refusing to replace non-socket daemon path {}",
                    socket.display()
                )
            }
            if metadata.uid() != unsafe { libc::geteuid() } {
                bail!("refusing to replace foreign-owner daemon socket")
            }
            match UnixStream::connect(&socket).await {
                Ok(_) => bail!("a terminal runtime daemon is already listening"),
                Err(error) => {
                    if error.kind() != std::io::ErrorKind::ConnectionRefused
                        && error.kind() != std::io::ErrorKind::NotFound
                    {
                        return Err(error).context("probe existing daemon socket");
                    }
                    fs::remove_file(&socket)?;
                }
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(error) => return Err(error.into()),
    }

    let listener = UnixListener::bind(&socket).context("bind daemon socket")?;
    fs::set_permissions(&socket, fs::Permissions::from_mode(0o600))?;
    let service = Arc::new(SessionService::new(runtime_root.clone())?);
    let viewer = Arc::new(ViewerServer::new(Arc::clone(&service)));
    let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
    eprintln!("terminal runtime listening");

    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, _) = accepted?;
                let service = Arc::clone(&service);
                let viewer = Arc::clone(&viewer);
                let shutdown_tx = shutdown_tx.clone();
                tokio::spawn(async move {
                    if let Err(error) = handle_connection(stream, service, viewer, shutdown_tx).await {
                        eprintln!("terminal runtime request failed: {error:#}");
                    }
                });
            }
            changed = shutdown_rx.changed() => {
                if changed.is_err() || *shutdown_rx.borrow() {
                    break;
                }
            }
            signal = tokio::signal::ctrl_c() => {
                signal.context("wait for SIGINT")?;
                break;
            }
        }
    }

    viewer.shutdown().await;
    service.shutdown()?;
    drop(listener);
    if let Ok(metadata) = fs::symlink_metadata(&socket)
        && metadata.file_type().is_socket()
        && metadata.uid() == unsafe { libc::geteuid() }
    {
        fs::remove_file(&socket)?;
    }
    Ok(())
}

async fn handle_connection(
    stream: UnixStream,
    service: Arc<SessionService>,
    viewer: Arc<ViewerServer>,
    shutdown_tx: watch::Sender<bool>,
) -> Result<()> {
    let (read, mut write) = stream.into_split();
    let reader = BufReader::new(read);
    let mut line = Vec::new();
    let read_count = reader
        .take((MAX_REQUEST_BYTES + 1) as u64)
        .read_until(b'\n', &mut line)
        .await?;
    if read_count == 0 {
        return Ok(());
    }
    if line.len() > MAX_REQUEST_BYTES {
        let response = RpcResponse::failure(
            "oversize".into(),
            "request_too_large",
            "request exceeds the 4 MiB limit",
        );
        write_response(&mut write, &response).await?;
        return Ok(());
    }
    let request: RpcRequestEnvelope = match serde_json::from_slice(&line) {
        Ok(request) => request,
        Err(error) => {
            let response =
                RpcResponse::failure("invalid".into(), "invalid_request", error.to_string());
            write_response(&mut write, &response).await?;
            return Ok(());
        }
    };
    if request.version != PROTOCOL_VERSION {
        let response = RpcResponse::failure(
            request.request_id,
            "unsupported_protocol_version",
            format!(
                "protocol version {} is unsupported; server uses {PROTOCOL_VERSION}",
                request.version
            ),
        );
        write_response(&mut write, &response).await?;
        return Ok(());
    }
    let id = request.request_id;
    let response = match request.request {
        request @ (crate::protocol::RpcRequest::ViewStart { .. }
        | crate::protocol::RpcRequest::ViewStatus { .. }
        | crate::protocol::RpcRequest::ViewStop { .. }) => {
            match viewer.handle_request(request).await {
                Ok(result) => Ok((result, false)),
                Err(error) => Err(error),
            }
        }
        request => {
            let service_for_call = Arc::clone(&service);
            tokio::task::spawn_blocking(move || service_for_call.handle(request)).await?
        }
    };
    let response = match response {
        Ok((result, shutdown)) => {
            if shutdown {
                let _ = shutdown_tx.send(true);
            }
            RpcResponse::success(id, result)
        }
        Err(error) => {
            let code = classify_error(&error);
            let mut response = RpcResponse::failure(id, &code, error.to_string());
            if let Some(failure) = response.error.as_mut() {
                failure.details = Some(
                    serde_json::json!({"retryable": matches!(code.as_str(), "control_held_by_browser" | "control_held_by_actor" | "timeout")}),
                );
                if let Some(allocation) = error.downcast_ref::<crate::session::LaunchFailure>()
                    && let Some(details) = failure.details.as_mut()
                {
                    details["allocation_id"] = serde_json::json!(allocation.session_id);
                }
            }
            response
        }
    };
    write_response(&mut write, &response).await
}

async fn write_response(
    writer: &mut tokio::net::unix::OwnedWriteHalf,
    response: &RpcResponse,
) -> Result<()> {
    let bytes = serde_json::to_vec(response)?;
    if bytes.len() > MAX_RESPONSE_BYTES {
        bail!("response exceeds the 64 MiB IPC limit")
    }
    writer.write_all(&bytes).await?;
    writer.write_all(b"\n").await?;
    writer.flush().await?;
    Ok(())
}

fn classify_error(error: &anyhow::Error) -> String {
    let message = error.to_string();
    if let Some((code, _)) = message.split_once(':')
        && !code.is_empty()
        && code.len() < 64
        && code.chars().all(|c| c.is_ascii_lowercase() || c == '_')
    {
        return code.to_string();
    }
    if message.contains("unsupported") || message.contains("unavailable") {
        "unsupported_operation".into()
    } else if message.contains("timeout") {
        "timeout".into()
    } else if message.contains("unknown") || message.contains("inactive") {
        "not_found".into()
    } else if message.contains("exited") {
        "process_exited".into()
    } else {
        "runtime_error".into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::net::UnixListener as StdUnixListener;

    #[tokio::test]
    async fn startup_keeps_non_socket_endpoint_intact() {
        let temp = tempfile::tempdir().unwrap();
        let paths = resolve_runtime_socket(temp.path()).unwrap();
        fs::write(&paths.socket_path, b"keep me").unwrap();
        let error = run(temp.path().to_path_buf()).await.unwrap_err();
        assert!(error.to_string().contains("non-socket"));
        assert_eq!(fs::read(paths.socket_path).unwrap(), b"keep me");
    }

    #[tokio::test]
    async fn startup_does_not_follow_or_remove_endpoint_symlink() {
        let temp = tempfile::tempdir().unwrap();
        let paths = resolve_runtime_socket(temp.path()).unwrap();
        let target = temp.path().join("target-file");
        fs::write(&target, b"keep me").unwrap();
        std::os::unix::fs::symlink(&target, &paths.socket_path).unwrap();
        let error = run(temp.path().to_path_buf()).await.unwrap_err();
        assert!(error.to_string().contains("non-socket"));
        assert!(
            fs::symlink_metadata(&paths.socket_path)
                .unwrap()
                .file_type()
                .is_symlink()
        );
        assert_eq!(fs::read(target).unwrap(), b"keep me");
    }

    #[tokio::test]
    async fn startup_keeps_foreign_owner_socket_intact() {
        let temp = tempfile::tempdir().unwrap();
        let paths = resolve_runtime_socket(temp.path()).unwrap();
        let listener = StdUnixListener::bind(&paths.socket_path).unwrap();
        let path =
            std::ffi::CString::new(paths.socket_path.as_os_str().as_encoded_bytes()).unwrap();
        // Non-root test users may not be allowed to change ownership. In that
        // case the foreign-owner scenario cannot be represented in this test.
        if unsafe { libc::chown(path.as_ptr(), 65_534, 65_534) } != 0 {
            drop(listener);
            return;
        }
        let error = run(temp.path().to_path_buf()).await.unwrap_err();
        assert!(error.to_string().contains("foreign-owner"));
        assert!(
            fs::symlink_metadata(paths.socket_path)
                .unwrap()
                .file_type()
                .is_socket()
        );
        drop(listener);
    }
}
