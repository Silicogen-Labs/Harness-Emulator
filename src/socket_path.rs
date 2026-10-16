//! Shared Unix socket path selection for the runtime client and daemon.

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};
use std::fs;
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::path::{Path, PathBuf};

const UNIX_SOCKET_PATH_MAX: usize = 107;

#[derive(Clone, Debug)]
pub struct RuntimeSocketPaths {
    pub runtime_root: PathBuf,
    pub socket_path: PathBuf,
}

/// Resolve the runtime root and its daemon socket in one place so independently
/// launched clients and daemons always choose the same endpoint.
pub fn resolve_runtime_socket(runtime_root: &Path) -> Result<RuntimeSocketPaths> {
    fs::create_dir_all(runtime_root)
        .with_context(|| format!("create runtime directory {}", runtime_root.display()))?;
    let runtime_root = fs::canonicalize(runtime_root)
        .with_context(|| format!("canonicalize runtime directory {}", runtime_root.display()))?;
    let rooted_socket = runtime_root.join("daemon.sock");
    let socket_path = if rooted_socket.as_os_str().as_encoded_bytes().len() <= UNIX_SOCKET_PATH_MAX
    {
        rooted_socket
    } else {
        short_socket_path(&runtime_root)?
    };
    Ok(RuntimeSocketPaths {
        runtime_root,
        socket_path,
    })
}

fn short_socket_path(runtime_root: &Path) -> Result<PathBuf> {
    let uid = unsafe { libc::geteuid() };
    let hash = Sha256::digest(runtime_root.as_os_str().as_encoded_bytes());
    let filename = format!("s-{}", hex_digest(&hash[..16]));

    if let Some(xdg) = std::env::var_os("XDG_RUNTIME_DIR") {
        let candidate = PathBuf::from(xdg);
        if let Ok(directory) = validated_private_dir(&candidate, uid) {
            let socket = directory.join(&filename);
            if socket.as_os_str().as_encoded_bytes().len() <= UNIX_SOCKET_PATH_MAX {
                return Ok(socket);
            }
        }
    }

    let fallback = PathBuf::from(format!("/tmp/silicogen-runtime-{uid}"));
    ensure_private_dir(&fallback, uid)?;
    let socket = fallback.join(filename);
    if socket.as_os_str().as_encoded_bytes().len() > UNIX_SOCKET_PATH_MAX {
        bail!("private runtime directory produces an overlong daemon socket path")
    }
    Ok(socket)
}

fn validated_private_dir(path: &Path, uid: u32) -> Result<PathBuf> {
    let canonical = fs::canonicalize(path)?;
    let metadata = fs::symlink_metadata(&canonical)?;
    if !metadata.is_dir()
        || metadata.uid() != uid
        || metadata.mode() & 0o077 != 0
        || metadata.mode() & 0o300 != 0o300
    {
        bail!("runtime directory is not a user-owned private directory")
    }
    Ok(canonical)
}

fn ensure_private_dir(path: &Path, uid: u32) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.is_dir()
                || metadata.uid() != uid
                || metadata.mode() & 0o077 != 0
                || metadata.mode() & 0o700 != 0o700
            {
                bail!(
                    "refusing unsafe private runtime directory {}",
                    path.display()
                )
            }
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir(path).with_context(|| {
                format!("create private runtime socket directory {}", path.display())
            })?;
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
            let metadata = fs::symlink_metadata(path)?;
            if !metadata.is_dir()
                || metadata.uid() != uid
                || metadata.mode() & 0o077 != 0
                || metadata.mode() & 0o700 != 0o700
            {
                bail!("private runtime socket directory failed ownership or mode validation")
            }
        }
        Err(error) => return Err(error.into()),
    }
    Ok(())
}

fn hex_digest(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut result = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        result.push(HEX[(byte >> 4) as usize] as char);
        result.push(HEX[(byte & 0x0f) as usize] as char);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::os::unix::fs::MetadataExt;

    #[test]
    fn short_runtime_uses_existing_rooted_socket_and_canonicalizes_relative_roots() {
        let temp = tempfile::tempdir().unwrap();
        let requested_root = temp.path().join("new-runtime");
        let paths = resolve_runtime_socket(&requested_root).unwrap();
        assert_eq!(paths.socket_path, paths.runtime_root.join("daemon.sock"));
        assert!(paths.runtime_root.is_absolute());
        assert_eq!(
            paths.runtime_root,
            fs::canonicalize(requested_root).unwrap()
        );
    }

    #[test]
    fn long_roots_resolve_to_short_stable_paths_unique_per_canonical_root() {
        let temp = tempfile::tempdir().unwrap();
        let root_a = temp.path().join("a".repeat(100));
        let root_b = temp.path().join("b".repeat(100));
        let a1 = resolve_runtime_socket(&root_a).unwrap();
        let a2 = resolve_runtime_socket(&root_a).unwrap();
        let b = resolve_runtime_socket(&root_b).unwrap();
        assert_eq!(a1.socket_path, a2.socket_path);
        assert_ne!(a1.socket_path, b.socket_path);
        assert!(a1.socket_path.as_os_str().as_encoded_bytes().len() <= UNIX_SOCKET_PATH_MAX);
        assert_ne!(a1.socket_path, a1.runtime_root.join("daemon.sock"));
    }

    #[test]
    fn fallback_directory_is_private_and_owned_by_current_user() {
        let uid = unsafe { libc::geteuid() };
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("private");
        ensure_private_dir(&path, uid).unwrap();
        let metadata = fs::symlink_metadata(path).unwrap();
        assert_eq!(metadata.uid(), uid);
        assert_eq!(metadata.mode() & 0o777, 0o700);
    }

    #[test]
    fn unsafe_existing_private_directory_is_rejected() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("shared");
        fs::create_dir(&path).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o755)).unwrap();
        assert!(ensure_private_dir(&path, unsafe { libc::geteuid() }).is_err());
    }
}
