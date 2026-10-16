//! Disposable snapshots of source trees used by terminal sessions.
//!
//! Snapshots are always independent copies (optionally filesystem reflinks),
//! never hard links. Changes made under the returned root are never merged
//! back into the source tree.

use anyhow::{Context, Result, bail, ensure};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum WorkspaceMode {
    #[default]
    Copy,
    Reflink,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct WorkspaceSpec {
    pub mode: WorkspaceMode,
    pub source: String,
    pub exclude: Vec<String>,
}

impl Default for WorkspaceSpec {
    fn default() -> Self {
        Self {
            mode: WorkspaceMode::Copy,
            source: String::new(),
            exclude: default_excludes(),
        }
    }
}

fn default_excludes() -> Vec<String> {
    [".git", "target", "node_modules", ".runtime"]
        .into_iter()
        .map(str::to_owned)
        .collect()
}

/// A sanitized description of a prepared snapshot, suitable for launch results.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct WorkspaceManifest {
    pub root: PathBuf,
    pub source: PathBuf,
    pub digest: String,
    pub method: String,
    pub copied_bytes: u64,
}

/// Copy the source working tree into a new session-owned directory.
pub fn prepare_workspace(
    runtime_root: &Path,
    session_id: &str,
    spec: &WorkspaceSpec,
) -> Result<WorkspaceManifest> {
    prepare_workspace_with_limit(runtime_root, session_id, spec, None)
}

/// Estimate regular-file bytes included by the same filters as the copier.
/// Special files and escaping symlinks are rejected before capacity is claimed.
pub fn estimate_workspace_bytes(spec: &WorkspaceSpec) -> Result<u64> {
    ensure!(
        !spec.source.is_empty(),
        "workspace source must not be empty"
    );
    let source = fs::canonicalize(&spec.source)
        .with_context(|| format!("canonicalizing workspace source {}", spec.source))?;
    ensure!(source.is_dir(), "workspace source is not a directory");
    estimate_tree(&source, &source, &spec.exclude)
}

fn estimate_tree(root: &Path, directory: &Path, excludes: &[String]) -> Result<u64> {
    let mut bytes = 0_u64;
    let mut entries = fs::read_dir(directory)?.collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let from = entry.path();
        let relative = from.strip_prefix(root)?;
        if excluded(relative, excludes) {
            continue;
        }
        let metadata = fs::symlink_metadata(&from)?;
        if metadata.file_type().is_symlink() {
            let link = fs::read_link(&from)?;
            validate_symlink(root, &from, &link)?;
            bytes = bytes.saturating_add(link.as_os_str().len() as u64);
        } else if metadata.is_dir() {
            bytes = bytes.saturating_add(estimate_tree(root, &from, excludes)?);
        } else if metadata.is_file() {
            bytes = bytes.saturating_add(metadata.len());
        } else {
            bail!("unsupported special file in workspace: {}", from.display());
        }
    }
    Ok(bytes)
}

pub fn prepare_workspace_with_limit(
    runtime_root: &Path,
    session_id: &str,
    spec: &WorkspaceSpec,
    max_bytes: Option<u64>,
) -> Result<WorkspaceManifest> {
    validate_session_id(session_id)?;
    ensure!(
        !spec.source.is_empty(),
        "workspace source must not be empty"
    );
    let source = fs::canonicalize(&spec.source)
        .with_context(|| format!("canonicalizing workspace source {}", spec.source))?;
    ensure!(
        source.is_dir(),
        "workspace source is not a directory: {}",
        source.display()
    );
    let runtime_root = absolute_or_create(runtime_root)?;
    let parent_requested = runtime_root.join("workspaces");
    fs::create_dir_all(&parent_requested).context("creating workspace storage")?;
    let parent = fs::canonicalize(&parent_requested).context("canonicalizing workspace storage")?;
    ensure!(
        parent.starts_with(&runtime_root),
        "workspace storage resolves outside runtime root"
    );
    let root = parent.join(session_id);
    ensure!(
        !root.exists(),
        "workspace destination already exists: {}",
        root.display()
    );

    let reflink_available = spec.mode == WorkspaceMode::Reflink && probe_reflink(&parent)?;
    fs::create_dir(&root).with_context(|| format!("creating workspace {}", root.display()))?;
    let mut hasher = Sha256::new();
    let mut copied_bytes = 0_u64;
    let copy_result = copy_tree(
        &source,
        &source,
        &root,
        &root,
        &spec.exclude,
        reflink_available,
        &mut hasher,
        &mut copied_bytes,
        max_bytes,
    );
    if let Err(error) = copy_result {
        // The target was created by this invocation and is disposable. Avoid
        // leaving a half snapshot that could be mistaken for a complete one.
        let _ = fs::remove_dir_all(&root);
        return Err(error);
    }
    let method = if reflink_available { "reflink" } else { "copy" }.to_owned();
    Ok(WorkspaceManifest {
        root,
        source,
        digest: format!("{:x}", hasher.finalize()),
        method,
        copied_bytes,
    })
}

fn absolute_or_create(path: &Path) -> Result<PathBuf> {
    if path.exists() {
        return fs::canonicalize(path).context("canonicalizing runtime root");
    }
    fs::create_dir_all(path).context("creating runtime root")?;
    fs::canonicalize(path).context("canonicalizing runtime root")
}

fn validate_session_id(id: &str) -> Result<()> {
    ensure!(
        !id.is_empty() && id != "." && id != "..",
        "invalid session id"
    );
    ensure!(
        id.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'),
        "session id contains unsafe characters"
    );
    Ok(())
}

fn excluded(relative: &Path, patterns: &[String]) -> bool {
    let path = relative.to_string_lossy().replace('\\', "/");
    patterns.iter().any(|pattern| {
        let pattern = pattern.trim_matches('/');
        if pattern.is_empty() {
            return false;
        }
        path == pattern
            || path
                .strip_prefix(pattern)
                .is_some_and(|tail| tail.starts_with('/'))
            || (pattern.contains('*') && wildcard_match(pattern, &path))
    })
}

fn wildcard_match(pattern: &str, value: &str) -> bool {
    let mut regex = String::from("^");
    let mut chars = pattern.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '*' if chars.peek() == Some(&'*') => {
                chars.next();
                regex.push_str(".*");
            }
            '*' => regex.push_str("[^/]*"),
            '?' => regex.push_str("[^/]"),
            other => regex.push_str(&regex::escape(&other.to_string())),
        }
    }
    regex.push('$');
    regex::Regex::new(&regex).is_ok_and(|r| r.is_match(value))
}

#[allow(clippy::too_many_arguments)] // Keep source/target roots, digest and byte accounting explicit through recursion.
fn copy_tree(
    source_root: &Path,
    source_dir: &Path,
    target_dir: &Path,
    target_root: &Path,
    excludes: &[String],
    reflink: bool,
    digest: &mut Sha256,
    copied_bytes: &mut u64,
    max_bytes: Option<u64>,
) -> Result<()> {
    let mut entries = fs::read_dir(source_dir)
        .with_context(|| format!("reading source directory {}", source_dir.display()))?
        .collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let from = entry.path();
        let relative = from.strip_prefix(source_root)?;
        if excluded(relative, excludes) {
            continue;
        }
        let to = target_dir.join(entry.file_name());
        let metadata = fs::symlink_metadata(&from)?;
        hash_field(digest, relative.to_string_lossy().as_bytes());
        if metadata.file_type().is_symlink() {
            let link = fs::read_link(&from)?;
            validate_symlink(source_root, &from, &link)?;
            hash_field(digest, b"symlink");
            hash_field(digest, link.to_string_lossy().as_bytes());
            let copied_link = if link.is_absolute() {
                let normalized = normalize_path(&link)?;
                let cloned_target = target_root.join(normalized.strip_prefix(source_root)?);
                relative_path(to.parent().unwrap_or(target_root), &cloned_target)?
            } else {
                link
            };
            let copied_link_bytes = copied_link.as_os_str().len() as u64;
            if max_bytes.is_some_and(|limit| copied_bytes.saturating_add(copied_link_bytes) > limit)
            {
                bail!(
                    "retention_quota_exceeded: copied workspace exceeded its reserved byte limit"
                );
            }
            create_symlink(&copied_link, &to)?;
            *copied_bytes = (*copied_bytes).saturating_add(copied_link_bytes);
        } else if metadata.is_dir() {
            hash_field(digest, b"directory");
            hash_field(
                digest,
                &permission_mode(&metadata.permissions()).to_le_bytes(),
            );
            fs::create_dir(&to)?;
            copy_tree(
                source_root,
                &from,
                &to,
                target_root,
                excludes,
                reflink,
                digest,
                copied_bytes,
                max_bytes,
            )?;
            set_copied_permissions(&to, metadata.permissions())?;
        } else if metadata.is_file() {
            hash_field(digest, b"file");
            hash_field(
                digest,
                &permission_mode(&metadata.permissions()).to_le_bytes(),
            );
            if max_bytes.is_some_and(|limit| copied_bytes.saturating_add(metadata.len()) > limit) {
                bail!(
                    "retention_quota_exceeded: workspace snapshot exceeded its reserved byte capacity"
                );
            }
            let copied_file_bytes = copy_file(&from, &to, reflink, copied_bytes, max_bytes)?;
            hash_field(digest, &copied_file_bytes.to_le_bytes());
            set_copied_permissions(&to, metadata.permissions())?;
            let mut file = File::open(&to)?;
            let mut buffer = [0_u8; 64 * 1024];
            loop {
                let count = file.read(&mut buffer)?;
                if count == 0 {
                    break;
                }
                digest.update(&buffer[..count]);
            }
        } else {
            bail!("unsupported special file in workspace: {}", from.display());
        }
    }
    Ok(())
}

#[cfg(unix)]
fn permission_mode(permissions: &fs::Permissions) -> u32 {
    use std::os::unix::fs::PermissionsExt;
    permissions.mode() & 0o777
}

#[cfg(not(unix))]
fn permission_mode(permissions: &fs::Permissions) -> u32 {
    u32::from(permissions.readonly())
}

fn relative_path(from: &Path, to: &Path) -> Result<PathBuf> {
    let from = from.components().collect::<Vec<_>>();
    let to = to.components().collect::<Vec<_>>();
    let shared = from.iter().zip(&to).take_while(|(a, b)| a == b).count();
    ensure!(shared > 0, "cannot rewrite symlink across filesystem roots");
    let mut result = PathBuf::new();
    for component in &from[shared..] {
        if matches!(component, Component::Normal(_)) {
            result.push("..");
        }
    }
    for component in &to[shared..] {
        result.push(component.as_os_str());
    }
    if result.as_os_str().is_empty() {
        result.push(".");
    }
    Ok(result)
}

fn hash_field(hasher: &mut Sha256, value: &[u8]) {
    hasher.update((value.len() as u64).to_le_bytes());
    hasher.update(value);
}

fn validate_symlink(root: &Path, link_path: &Path, target: &Path) -> Result<()> {
    let joined = if target.is_absolute() {
        target.to_path_buf()
    } else {
        link_path.parent().unwrap_or(root).join(target)
    };
    let normalized = normalize_path(&joined)?;
    ensure!(
        normalized.starts_with(root),
        "workspace contains symlink escaping source root: {} -> {}",
        link_path.display(),
        target.display()
    );
    let resolved = fs::canonicalize(&normalized)
        .with_context(|| format!("resolving source symlink {}", link_path.display()))?;
    ensure!(
        resolved.starts_with(root),
        "workspace contains symlink escaping source root: {} -> {}",
        link_path.display(),
        target.display()
    );
    Ok(())
}

#[cfg(unix)]
fn set_copied_permissions(path: &Path, permissions: fs::Permissions) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    fs::set_permissions(
        path,
        fs::Permissions::from_mode(permissions.mode() & !0o6000),
    )
    .with_context(|| format!("setting permissions on {}", path.display()))
}

#[cfg(not(unix))]
fn set_copied_permissions(path: &Path, permissions: fs::Permissions) -> Result<()> {
    fs::set_permissions(path, permissions)
        .with_context(|| format!("setting permissions on {}", path.display()))
}

fn normalize_path(path: &Path) -> Result<PathBuf> {
    let mut result = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(prefix) => result.push(prefix.as_os_str()),
            Component::RootDir => result.push(component.as_os_str()),
            Component::CurDir => {}
            Component::ParentDir => {
                ensure!(result.pop(), "path escapes filesystem root");
            }
            Component::Normal(part) => result.push(part),
        }
    }
    Ok(result)
}

#[cfg(unix)]
fn create_symlink(target: &Path, link: &Path) -> Result<()> {
    std::os::unix::fs::symlink(target, link)
        .with_context(|| format!("creating symlink {}", link.display()))
}

#[cfg(not(unix))]
fn create_symlink(_target: &Path, link: &Path) -> Result<()> {
    bail!(
        "source symlinks are not supported on this platform: {}",
        link.display()
    )
}

fn copy_file(
    from: &Path,
    to: &Path,
    reflink: bool,
    copied_bytes: &mut u64,
    max_bytes: Option<u64>,
) -> Result<u64> {
    let mut output = OpenOptions::new().write(true).create_new(true).open(to)?;
    if reflink && try_reflink(from, &output)? {
        let length = output.metadata()?.len();
        let next = (*copied_bytes).saturating_add(length);
        if max_bytes.is_some_and(|limit| next > limit) {
            bail!("retention_quota_exceeded: copied workspace exceeded its reserved byte limit");
        }
        *copied_bytes = next;
        return Ok(length);
    }
    let mut input = File::open(from)?;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = input.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        let next = (*copied_bytes).saturating_add(count as u64);
        if max_bytes.is_some_and(|limit| next > limit) {
            bail!("retention_quota_exceeded: copied workspace exceeded its reserved byte limit");
        }
        output.write_all(&buffer[..count])?;
        *copied_bytes = next;
    }
    output.flush()?;
    Ok(output.metadata()?.len())
}

#[cfg(target_os = "linux")]
fn try_reflink(from: &Path, to: &File) -> Result<bool> {
    use std::os::fd::AsRawFd;
    const FICLONE: libc::c_ulong = 0x4004_9409;
    let input = File::open(from)?;
    // ioctl returns EXDEV/EOPNOTSUPP for filesystems without clone support.
    let result = unsafe { libc::ioctl(to.as_raw_fd(), FICLONE, input.as_raw_fd()) };
    if result == 0 {
        return Ok(true);
    }
    let error = std::io::Error::last_os_error();
    if matches!(
        error.raw_os_error(),
        Some(libc::EOPNOTSUPP | libc::EXDEV | libc::EINVAL | libc::ENOTTY)
    ) {
        Ok(false)
    } else {
        Err(error).context("attempting reflink clone")
    }
}

#[cfg(not(target_os = "linux"))]
fn try_reflink(_from: &Path, _to: &File) -> Result<bool> {
    Ok(false)
}

fn probe_reflink(parent: &Path) -> Result<bool> {
    let probe = parent.join(format!(".reflink-probe-{}", ulid::Ulid::generate()));
    fs::create_dir(&probe).context("creating reflink probe")?;
    let result = (|| {
        let source = probe.join("source");
        let target = probe.join("target");
        fs::write(&source, b"reflink probe")?;
        let output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&target)?;
        try_reflink(&source, &output)
    })();
    fs::remove_dir_all(&probe).context("removing reflink probe")?;
    result
}
