//! Session-scoped artifact registration and race-resistant bounded reads.
//!
//! Callers register only roots they own or explicitly configured log roots. All
//! subsequent file opens are relative to retained directory descriptors.

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::ffi::CString;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::os::unix::fs::MetadataExt;
use std::path::{Component, Path, PathBuf};
use std::sync::RwLock;
use ulid::Ulid;

pub const MAX_ARTIFACT_READ: usize = 1024 * 1024;
const MAX_LIST_PAGE: usize = 500;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArtifactSourceKind {
    Screenshot,
    History,
    Frame,
    ApplicationLog,
    Other,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactMetadata {
    pub artifact_id: String,
    pub session_id: String,
    pub run_id: Option<String>,
    pub source_kind: ArtifactSourceKind,
    pub display_name: String,
    pub content_type: String,
    pub size_bytes: u64,
    pub modified_ms: Option<u128>,
    pub retention: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ArtifactRead {
    pub artifact: ArtifactMetadata,
    pub offset: u64,
    pub next_offset: u64,
    pub eof: bool,
    pub bytes: Vec<u8>,
}

/// One member of an atomic batch of caller-identified evidence artifacts.
pub struct EvidenceFileRegistration<'a> {
    pub relative_path: &'a Path,
    pub artifact_id: &'a str,
    pub source_kind: ArtifactSourceKind,
    pub content_type: &'a str,
    pub run_id: Option<String>,
    pub retention: Option<String>,
}

struct RegisteredRoot {
    session_id: String,
    label: String,
    kind: ArtifactSourceKind,
    dir: OwnedFd,
}

struct RegisteredFile {
    metadata: ArtifactMetadata,
    root_id: String,
    relative_path: PathBuf,
}

/// A registry of explicitly authorized roots and files.
pub struct ArtifactStore {
    runtime_root: PathBuf,
    runtime_dir: OwnedFd,
    roots: RwLock<BTreeMap<String, RegisteredRoot>>,
    files: RwLock<BTreeMap<String, RegisteredFile>>,
}

impl ArtifactStore {
    pub fn new(runtime_root: PathBuf) -> Result<Self> {
        let runtime_dir = open_absolute_dir(&runtime_root)
            .with_context(|| format!("open runtime root {}", runtime_root.display()))?;
        Ok(Self {
            runtime_root,
            runtime_dir,
            roots: RwLock::new(BTreeMap::new()),
            files: RwLock::new(BTreeMap::new()),
        })
    }

    pub fn runtime_root(&self) -> &Path {
        &self.runtime_root
    }

    /// Registers an app-owned evidence directory for one session.
    pub fn register_session_evidence(&self, session_id: &str, path: &Path) -> Result<String> {
        validate_identity(session_id, "session id")?;
        let relative = path
            .strip_prefix(&self.runtime_root)
            .context("session evidence root must be beneath the runtime root")?;
        validate_relative(relative)?;
        let mut components = relative.components();
        let category = components.next().and_then(|c| c.as_os_str().to_str());
        let owner = components.next().and_then(|c| c.as_os_str().to_str());
        if !matches!(category, Some("sessions" | "artifacts")) || owner != Some(session_id) {
            bail!(
                "session evidence root must be inside this session's sessions/ or artifacts/ directory"
            )
        }
        let dir = open_relative_dir(self.runtime_dir.as_raw_fd(), relative)?;
        self.register_open_root(
            session_id,
            "session_evidence",
            ArtifactSourceKind::Other,
            dir,
        )
    }

    /// Registers a configured application-log directory for one session.
    pub fn register_log_root(&self, session_id: &str, label: &str, path: &Path) -> Result<String> {
        if label.trim().is_empty() || label.len() > 80 {
            bail!("log root label must contain 1 to 80 bytes")
        }
        self.register_root(session_id, label, ArtifactSourceKind::ApplicationLog, path)
    }

    /// Registers only the daemon's single shared log file. The runtime root is
    /// retained internally and is never exposed as a listable artifact root.
    pub fn register_daemon_log(&self, session_id: &str) -> Result<ArtifactMetadata> {
        validate_identity(session_id, "session id")?;
        let duplicate =
            unsafe { libc::fcntl(self.runtime_dir.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 3) };
        if duplicate < 0 {
            bail!(
                "duplicate runtime directory handle: {}",
                std::io::Error::last_os_error()
            )
        }
        let root_id = self.register_open_root(
            session_id,
            "daemon",
            ArtifactSourceKind::ApplicationLog,
            unsafe { OwnedFd::from_raw_fd(duplicate) },
        )?;
        self.register_file(
            session_id,
            &root_id,
            Path::new("daemon.log"),
            ArtifactSourceKind::ApplicationLog,
            "text/plain",
            None,
            Some("runtime".to_owned()),
        )
    }

    fn register_root(
        &self,
        session_id: &str,
        label: &str,
        kind: ArtifactSourceKind,
        path: &Path,
    ) -> Result<String> {
        validate_identity(session_id, "session id")?;
        let dir = open_absolute_dir(path)
            .with_context(|| format!("securely open registered root {}", path.display()))?;
        self.register_open_root(session_id, label, kind, dir)
    }

    fn register_open_root(
        &self,
        session_id: &str,
        label: &str,
        kind: ArtifactSourceKind,
        dir: OwnedFd,
    ) -> Result<String> {
        let mut roots = self
            .roots
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((root_id, _)) = roots.iter().find(|(_, root)| {
            root.session_id == session_id
                && root.label == label
                && root.kind == kind
                && same_directory(root.dir.as_raw_fd(), dir.as_raw_fd())
        }) {
            return Ok(root_id.clone());
        }
        let root_id = format!("root-{}", Ulid::generate());
        roots.insert(
            root_id.clone(),
            RegisteredRoot {
                session_id: session_id.to_owned(),
                label: label.to_owned(),
                kind,
                dir,
            },
        );
        Ok(root_id)
    }

    /// Registers one file below a previously registered root. Relative paths
    /// cannot traverse, and sensitive credential-like names are never exposed.
    #[allow(clippy::too_many_arguments)] // Explicit evidence identity and retention fields at the registry boundary.
    pub fn register_file(
        &self,
        session_id: &str,
        root_id: &str,
        relative_path: &Path,
        source_kind: ArtifactSourceKind,
        content_type: impl Into<String>,
        run_id: Option<String>,
        retention: Option<String>,
    ) -> Result<ArtifactMetadata> {
        self.register_file_with_id(
            session_id,
            root_id,
            relative_path,
            None,
            source_kind,
            content_type.into(),
            run_id,
            retention,
        )
    }

    /// Registers an evidence file under an identity chosen by its producer.
    /// The identity is validated and collisions are rejected. Re-registering
    /// the same file with the same metadata is idempotent and returns the
    /// original metadata.
    #[allow(clippy::too_many_arguments)] // Explicit caller identity and evidence metadata at the registry boundary.
    pub fn register_evidence_file_with_id(
        &self,
        session_id: &str,
        root_id: &str,
        relative_path: &Path,
        artifact_id: &str,
        source_kind: ArtifactSourceKind,
        content_type: impl Into<String>,
        run_id: Option<String>,
        retention: Option<String>,
    ) -> Result<ArtifactMetadata> {
        if source_kind == ArtifactSourceKind::ApplicationLog {
            bail!("application logs must be registered under a log root")
        }
        validate_identity(artifact_id, "artifact id")?;
        self.register_file_with_id(
            session_id,
            root_id,
            relative_path,
            Some(artifact_id),
            source_kind,
            content_type.into(),
            run_id,
            retention,
        )
    }

    /// Register a related set of evidence files atomically. Every source file,
    /// root, identity, and collision is checked before any registry entry is
    /// published, so failed paired evidence cannot leave a discoverable half.
    pub fn register_evidence_files_with_ids(
        &self,
        session_id: &str,
        root_id: &str,
        registrations: &[EvidenceFileRegistration<'_>],
    ) -> Result<Vec<ArtifactMetadata>> {
        validate_identity(session_id, "session id")?;
        if registrations.is_empty() {
            bail!("at least one evidence file is required")
        }
        let roots = self
            .roots
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = roots.get(root_id).context("unknown artifact root")?;
        if root.session_id != session_id || root.kind == ArtifactSourceKind::ApplicationLog {
            bail!("artifact root is not a session evidence root")
        }

        let mut prepared = Vec::with_capacity(registrations.len());
        let mut ids = std::collections::BTreeSet::new();
        for registration in registrations {
            validate_identity(registration.artifact_id, "artifact id")?;
            validate_relative(registration.relative_path)?;
            deny_sensitive_path(registration.relative_path)?;
            if registration.source_kind == ArtifactSourceKind::ApplicationLog {
                bail!("application logs must be registered under a log root")
            }
            if !ids.insert(registration.artifact_id) {
                bail!("artifact id is duplicated in evidence batch")
            }
            let file = open_relative_file(root.dir.as_raw_fd(), registration.relative_path)?;
            let file_metadata = file.metadata()?;
            if !file_metadata.is_file() {
                bail!("artifact source is not a regular file")
            }
            let file_name = registration
                .relative_path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("artifact");
            let metadata = ArtifactMetadata {
                artifact_id: registration.artifact_id.to_owned(),
                session_id: session_id.to_owned(),
                run_id: registration.run_id.clone(),
                source_kind: registration.source_kind.clone(),
                display_name: file_name.to_owned(),
                content_type: registration.content_type.to_owned(),
                size_bytes: file_metadata.len(),
                modified_ms: file_metadata.modified().ok().and_then(system_time_ms),
                retention: registration.retention.clone(),
            };
            prepared.push((metadata, registration.relative_path.to_path_buf()));
        }

        let mut files = self
            .files
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if prepared
            .iter()
            .any(|(metadata, _)| files.contains_key(&metadata.artifact_id))
        {
            bail!("artifact id is already registered")
        }
        let metadata = prepared
            .into_iter()
            .map(|(metadata, relative_path)| {
                files.insert(
                    metadata.artifact_id.clone(),
                    RegisteredFile {
                        metadata: metadata.clone(),
                        root_id: root_id.to_owned(),
                        relative_path,
                    },
                );
                metadata
            })
            .collect();
        Ok(metadata)
    }

    #[allow(clippy::too_many_arguments)] // Keep the artifact identity and provenance explicit at the storage boundary.
    fn register_file_with_id(
        &self,
        session_id: &str,
        root_id: &str,
        relative_path: &Path,
        requested_id: Option<&str>,
        source_kind: ArtifactSourceKind,
        content_type: String,
        run_id: Option<String>,
        retention: Option<String>,
    ) -> Result<ArtifactMetadata> {
        validate_identity(session_id, "session id")?;
        validate_relative(relative_path)?;
        deny_sensitive_path(relative_path)?;
        let roots = self
            .roots
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = roots.get(root_id).context("unknown artifact root")?;
        if root.session_id != session_id {
            bail!("artifact root belongs to a different session")
        }
        if (source_kind == ArtifactSourceKind::ApplicationLog)
            != (root.kind == ArtifactSourceKind::ApplicationLog)
        {
            bail!("artifact source kind does not match its registered root")
        }
        let file = open_relative_file(root.dir.as_raw_fd(), relative_path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            bail!("artifact source is not a regular file")
        }
        let file_name = relative_path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("artifact");
        let display_name = if source_kind == ArtifactSourceKind::ApplicationLog {
            format!("{}:{file_name}", root.label)
        } else {
            file_name.to_owned()
        };
        let id = requested_id
            .map(str::to_owned)
            .unwrap_or_else(|| format!("art-{}", Ulid::generate()));
        let artifact = ArtifactMetadata {
            artifact_id: id.clone(),
            session_id: session_id.to_owned(),
            run_id: run_id.clone(),
            source_kind,
            display_name,
            content_type,
            size_bytes: metadata.len(),
            modified_ms: metadata.modified().ok().and_then(system_time_ms),
            retention: retention.clone(),
        };
        let mut files = self
            .files
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(existing) = files.get(&id) {
            if existing.root_id == root_id
                && existing.relative_path == relative_path
                && existing.metadata.session_id == session_id
                && existing.metadata.run_id == run_id
                && existing.metadata.source_kind == artifact.source_kind
                && existing.metadata.display_name == artifact.display_name
                && existing.metadata.content_type == artifact.content_type
                && existing.metadata.size_bytes == artifact.size_bytes
                && existing.metadata.modified_ms == artifact.modified_ms
                && existing.metadata.retention == retention
                && requested_id.is_some()
            {
                return Ok(existing.metadata.clone());
            }
            bail!("artifact id is already registered")
        }
        files.insert(
            id,
            RegisteredFile {
                metadata: artifact.clone(),
                root_id: root_id.to_owned(),
                relative_path: relative_path.to_path_buf(),
            },
        );
        Ok(artifact)
    }

    /// Registers a generated file in a session evidence root.
    #[allow(clippy::too_many_arguments)] // Mirrors register_file without an implicit identity/default.
    pub fn register_evidence_file(
        &self,
        session_id: &str,
        root_id: &str,
        relative_path: &Path,
        source_kind: ArtifactSourceKind,
        content_type: impl Into<String>,
        run_id: Option<String>,
        retention: Option<String>,
    ) -> Result<ArtifactMetadata> {
        if source_kind == ArtifactSourceKind::ApplicationLog {
            bail!("application logs must be registered under a log root")
        }
        self.register_file(
            session_id,
            root_id,
            relative_path,
            source_kind,
            content_type,
            run_id,
            retention,
        )
    }

    pub fn list(
        &self,
        session_id: &str,
        offset: usize,
        limit: usize,
    ) -> Result<Vec<ArtifactMetadata>> {
        validate_identity(session_id, "session id")?;
        if limit == 0 || limit > MAX_LIST_PAGE {
            bail!("artifact page limit must be between 1 and {MAX_LIST_PAGE}")
        }
        let files = self
            .files
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        Ok(files
            .values()
            .filter(|f| f.metadata.session_id == session_id)
            .skip(offset)
            .take(limit)
            .map(|f| f.metadata.clone())
            .collect())
    }

    pub fn metadata(&self, session_id: &str, artifact_id: &str) -> Result<ArtifactMetadata> {
        let files = self
            .files
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let file = files.get(artifact_id).context("unknown artifact id")?;
        if file.metadata.session_id != session_id {
            bail!("artifact belongs to a different session")
        }
        Ok(file.metadata.clone())
    }

    pub fn read(
        &self,
        session_id: &str,
        artifact_id: &str,
        offset: u64,
        length: usize,
    ) -> Result<ArtifactRead> {
        if length > MAX_ARTIFACT_READ {
            bail!("artifact reads are limited to {MAX_ARTIFACT_READ} bytes")
        }
        let files = self
            .files
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let registered = files.get(artifact_id).context("unknown artifact id")?;
        if registered.metadata.session_id != session_id {
            bail!("artifact belongs to a different session")
        }
        let roots = self
            .roots
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = roots
            .get(&registered.root_id)
            .context("artifact root expired")?;
        let mut file = open_relative_file(root.dir.as_raw_fd(), &registered.relative_path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() {
            bail!("registered artifact stopped being a regular file")
        }
        let size = metadata.len();
        let start = offset.min(size);
        let count = (size - start).min(length as u64) as usize;
        file.seek(SeekFrom::Start(start))?;
        let mut bytes = vec![0; count];
        let mut filled = 0;
        while filled < count {
            match file.read(&mut bytes[filled..])? {
                0 => break,
                n => filled += n,
            }
        }
        bytes.truncate(filled);
        let mut artifact = registered.metadata.clone();
        artifact.size_bytes = size;
        artifact.modified_ms = metadata.modified().ok().and_then(system_time_ms);
        Ok(ArtifactRead {
            artifact,
            offset: start,
            next_offset: start + filled as u64,
            eof: start + filled as u64 >= size,
            bytes,
        })
    }

    pub(crate) fn registered_file_path(
        &self,
        session_id: &str,
        artifact_id: &str,
    ) -> Result<(i32, PathBuf)> {
        let files = self
            .files
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let registered = files.get(artifact_id).context("unknown artifact id")?;
        if registered.metadata.session_id != session_id {
            bail!("artifact belongs to a different session")
        }
        let roots = self
            .roots
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let root = roots
            .get(&registered.root_id)
            .context("artifact root expired")?;
        Ok((root.dir.as_raw_fd(), registered.relative_path.clone()))
    }
}

pub(crate) fn open_relative_file(root_fd: i32, path: &Path) -> Result<File> {
    validate_relative(path)?;
    let duplicate = unsafe { libc::fcntl(root_fd, libc::F_DUPFD_CLOEXEC, 3) };
    if duplicate < 0 {
        bail!(
            "duplicate registered directory handle: {}",
            std::io::Error::last_os_error()
        )
    }
    let mut current = unsafe { OwnedFd::from_raw_fd(duplicate) };
    let components = path
        .components()
        .filter_map(|c| match c {
            Component::Normal(v) => Some(v),
            _ => None,
        })
        .collect::<Vec<_>>();
    for component in &components[..components.len() - 1] {
        let name = CString::new(component.as_encoded_bytes()).context("invalid path component")?;
        let next = unsafe {
            libc::openat(
                current.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            )
        };
        if next < 0 {
            bail!(
                "open artifact directory component: {}",
                std::io::Error::last_os_error()
            )
        }
        current = unsafe { OwnedFd::from_raw_fd(next) };
    }
    let name = CString::new(components.last().unwrap().as_encoded_bytes())
        .context("invalid path component")?;
    let fd = unsafe {
        libc::openat(
            current.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        )
    };
    if fd < 0 {
        bail!(
            "open registered artifact: {}",
            std::io::Error::last_os_error()
        )
    }
    let file = unsafe { File::from_raw_fd(fd) };
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        bail!("artifact source is not a regular file")
    }
    if metadata.nlink() > 1 {
        bail!("hard-linked artifact sources are denied")
    }
    Ok(file)
}

fn open_relative_dir(root_fd: i32, path: &Path) -> Result<OwnedFd> {
    validate_relative(path)?;
    let duplicate = unsafe { libc::fcntl(root_fd, libc::F_DUPFD_CLOEXEC, 3) };
    if duplicate < 0 {
        bail!(
            "duplicate registered directory handle: {}",
            std::io::Error::last_os_error()
        )
    }
    let mut current = unsafe { OwnedFd::from_raw_fd(duplicate) };
    for component in path.components() {
        let Component::Normal(component) = component else {
            bail!("artifact path traversal is forbidden")
        };
        let name = CString::new(component.as_encoded_bytes()).context("invalid path component")?;
        let next = unsafe {
            libc::openat(
                current.as_raw_fd(),
                name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            )
        };
        if next < 0 {
            bail!(
                "open session evidence directory: {}",
                std::io::Error::last_os_error()
            )
        }
        current = unsafe { OwnedFd::from_raw_fd(next) };
    }
    Ok(current)
}

pub(crate) fn validate_relative(path: &Path) -> Result<()> {
    if path.as_os_str().is_empty() || path.is_absolute() {
        bail!("artifact path must be a nonempty relative path")
    }
    for component in path.components() {
        if !matches!(component, Component::Normal(_)) {
            bail!("artifact path traversal is forbidden")
        }
    }
    Ok(())
}

fn open_absolute_dir(path: &Path) -> Result<OwnedFd> {
    if !path.is_absolute() {
        bail!("registered directories must use absolute paths")
    }
    let root = CString::new("/").unwrap();
    let fd = unsafe {
        libc::open(
            root.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        bail!("open filesystem root: {}", std::io::Error::last_os_error())
    }
    let mut current = unsafe { OwnedFd::from_raw_fd(fd) };
    for component in path.components() {
        match component {
            Component::RootDir => {}
            Component::Normal(name) => {
                let name =
                    CString::new(name.as_encoded_bytes()).context("invalid directory component")?;
                let next = unsafe {
                    libc::openat(
                        current.as_raw_fd(),
                        name.as_ptr(),
                        libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                    )
                };
                if next < 0 {
                    bail!(
                        "open registered directory component: {}",
                        std::io::Error::last_os_error()
                    )
                }
                current = unsafe { OwnedFd::from_raw_fd(next) };
            }
            _ => bail!("registered directory path may not contain traversal components"),
        }
    }
    Ok(current)
}

fn same_directory(left_fd: libc::c_int, right_fd: libc::c_int) -> bool {
    let mut left = std::mem::MaybeUninit::<libc::stat>::uninit();
    let mut right = std::mem::MaybeUninit::<libc::stat>::uninit();
    if unsafe { libc::fstat(left_fd, left.as_mut_ptr()) } != 0
        || unsafe { libc::fstat(right_fd, right.as_mut_ptr()) } != 0
    {
        return false;
    }
    let left = unsafe { left.assume_init() };
    let right = unsafe { right.assume_init() };
    left.st_dev == right.st_dev && left.st_ino == right.st_ino
}

fn deny_sensitive_path(path: &Path) -> Result<()> {
    for component in path.components() {
        let value = component.as_os_str().to_string_lossy().to_ascii_lowercase();
        let base = value.rsplit('/').next().unwrap_or(&value);
        let words = base
            .trim_start_matches('.')
            .split(|ch: char| !ch.is_ascii_alphanumeric())
            .filter(|word| !word.is_empty());
        if base == ".env"
            || base.starts_with(".env.")
            || base == "env"
            || base.starts_with("env.")
            || words.into_iter().any(|word| {
                matches!(
                    word,
                    "auth"
                        | "credential"
                        | "credentials"
                        | "secret"
                        | "secrets"
                        | "token"
                        | "key"
                        | "private"
                        | "privatekey"
                        | "apikey"
                        | "rsa"
                        | "ed25519"
                )
            })
        {
            bail!("credential-like artifact sources are denied")
        }
    }
    Ok(())
}

pub(crate) fn validate_identity(value: &str, label: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        bail!("invalid {label}")
    }
    Ok(())
}

fn system_time_ms(time: std::time::SystemTime) -> Option<u128> {
    time.duration_since(std::time::UNIX_EPOCH)
        .ok()
        .map(|d| d.as_millis())
}
