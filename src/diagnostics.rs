//! Standalone core for bounded diagnostic sidecars and commands.
//!
//! Integration code must construct `EffectiveContext` from trusted, already
//! validated session metadata. `LaunchSpec` received over MCP is not sufficient
//! authority to choose cwd, environment, workspace mounts, or containment.

use crate::protocol::LaunchSpec;
use anyhow::{Context, Result, bail, ensure};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::ffi::{CString, OsStr, OsString};
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::path::{Component, Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const SEED_FILE_MAX_BYTES: usize = 1024 * 1024;
const DEFAULT_POLL: Duration = Duration::from_millis(10);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct OwnerKey {
    pub session_id: String,
    pub run_id: String,
}

impl OwnerKey {
    pub fn new(session_id: impl Into<String>, run_id: impl Into<String>) -> Result<Self> {
        let key = Self {
            session_id: session_id.into(),
            run_id: run_id.into(),
        };
        validate_owner_component(&key.session_id)?;
        validate_owner_component(&key.run_id)?;
        Ok(key)
    }
}

#[derive(Debug, Clone)]
pub enum ContainmentPolicy {
    /// Own the direct child and its verified process group while the group
    /// leader identity is still valid. Detached descendants are not contained.
    Host,
    /// Run under a Bubblewrap PID namespace. Mount roots and executable are
    /// supplied by the trusted integration layer after isolation validation.
    BubblewrapPidNamespace {
        executable: PathBuf,
        writable_roots: Vec<PathBuf>,
        unshare_network: bool,
    },
}

#[derive(Debug, Clone)]
pub struct EffectiveContext {
    cwd: PathBuf,
    environment: BTreeMap<String, String>,
    containment: ContainmentPolicy,
}

#[derive(Debug, Clone)]
pub struct SelectedWorkspace {
    root: PathBuf,
}

impl SelectedWorkspace {
    /// Construct only from the copied workspace identity in trusted session
    /// metadata. Never accept this root directly from a tool request.
    pub(crate) fn from_validated(root: PathBuf) -> Self {
        Self { root }
    }
}

impl EffectiveContext {
    /// Only crate-internal integration code should create this from validated
    /// profile/workspace/session policy. Never deserialize it from MCP input.
    pub(crate) fn from_validated(
        cwd: PathBuf,
        environment: BTreeMap<String, String>,
        containment: ContainmentPolicy,
    ) -> Self {
        Self {
            cwd,
            environment,
            containment,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Limits {
    pub max_processes: usize,
    pub max_total_processes: usize,
    pub max_total_output_bytes: usize,
    pub max_output_bytes_per_process: usize,
    pub max_retained_bytes_per_stream: usize,
    pub max_runtime: Duration,
    pub output_drain_timeout: Duration,
    pub termination_grace: Duration,
    pub max_argv_count: usize,
    pub max_argv_bytes: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_processes: 8,
            max_total_processes: 64,
            max_total_output_bytes: 4 * 1024 * 1024,
            max_output_bytes_per_process: 1024 * 1024,
            max_retained_bytes_per_stream: 256 * 1024,
            max_runtime: Duration::from_secs(60),
            output_drain_timeout: Duration::from_millis(250),
            termination_grace: Duration::from_millis(300),
            max_argv_count: 128,
            max_argv_bytes: 32 * 1024,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Lifecycle {
    Running,
    Exited,
    TimedOut,
    Cancelled,
    OutputLimitExceeded,
}

#[derive(Debug, Clone)]
pub struct StartReceipt {
    pub id: String,
    pub label: String,
    pub backend: &'static str,
    pub argv_count: usize,
    pub detached_children_contained: bool,
}

#[derive(Debug, Clone)]
pub struct Status {
    pub id: String,
    pub lifecycle: Lifecycle,
    pub exit_code: Option<i32>,
    pub elapsed_ms: u128,
    pub output_bytes: usize,
    pub capture_complete: Option<bool>,
    pub capture_gaps: Vec<OutputStream>,
    pub capture_gap_reason: Option<CaptureGapReason>,
    pub backend: &'static str,
    pub detached_children_contained: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputStream {
    Stdout,
    Stderr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CaptureGapReason {
    OutputLimit,
    PipeStillOpenAtStop,
    DrainDeadline,
}

#[derive(Debug, Clone)]
pub struct OutputChunk {
    pub text: String,
    pub cursor: usize,
    pub next_cursor: usize,
    pub truncated_before_cursor: bool,
    pub total_bytes_seen: usize,
}

#[derive(Debug, Clone)]
pub struct SeedReceipt {
    pub bytes_written: usize,
    pub sha256: String,
    pub overwritten: bool,
}

struct ManagerInner {
    limits: Limits,
    state: Mutex<ManagerState>,
    state_cv: Condvar,
    total_output: AtomicUsize,
    output_overflow: AtomicBool,
}

struct ManagerState {
    entries: HashMap<String, Arc<ProcessEntry>>,
    active_count: usize,
    total_started: usize,
    starting_by_owner: HashMap<OwnerKey, usize>,
    stopped_owners: HashSet<OwnerKey>,
}

struct Admission {
    inner: Arc<ManagerInner>,
    owner: OwnerKey,
    committed: bool,
}

impl Admission {
    fn commit(&mut self, entry: Arc<ProcessEntry>) {
        let mut state = lock(&self.inner.state);
        state.entries.insert(entry.id.clone(), entry);
        state
            .starting_by_owner
            .entry(self.owner.clone())
            .and_modify(|count| *count -= 1);
        if state.starting_by_owner.get(&self.owner) == Some(&0) {
            state.starting_by_owner.remove(&self.owner);
        }
        self.committed = true;
        self.inner.state_cv.notify_all();
    }
}

impl Drop for Admission {
    fn drop(&mut self) {
        if self.committed {
            return;
        }
        let mut state = lock(&self.inner.state);
        state.active_count = state.active_count.saturating_sub(1);
        state.total_started = state.total_started.saturating_sub(1);
        if let Some(count) = state.starting_by_owner.get_mut(&self.owner) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                state.starting_by_owner.remove(&self.owner);
            }
        }
        self.inner.state_cv.notify_all();
    }
}

struct ProcessEntry {
    id: String,
    owner: OwnerKey,
    backend: &'static str,
    detached_children_contained: bool,
    started: Instant,
    identity: Option<ProcessIdentity>,
    process_output: AtomicUsize,
    reader_stop: AtomicBool,
    drain_deadline: Mutex<Option<Instant>>,
    stdout_eof: AtomicBool,
    stderr_eof: AtomicBool,
    capture_gap_reason: Mutex<Option<CaptureGapReason>>,
    stdout: Mutex<BoundedBytes>,
    stderr: Mutex<BoundedBytes>,
    cancel: AtomicBool,
    finished: Mutex<Option<Finished>>,
    finished_cv: Condvar,
    monitor: Mutex<Option<JoinHandle<()>>>,
}

#[derive(Debug, Clone)]
struct Finished {
    lifecycle: Lifecycle,
    exit_code: Option<i32>,
}

struct BoundedBytes {
    data: VecDeque<u8>,
    total_seen: usize,
    capacity: usize,
}

impl BoundedBytes {
    fn new(capacity: usize) -> Self {
        Self {
            data: VecDeque::with_capacity(capacity.min(8192)),
            total_seen: 0,
            capacity,
        }
    }
    fn push(&mut self, bytes: &[u8]) {
        self.total_seen = self.total_seen.saturating_add(bytes.len());
        for byte in bytes {
            if self.capacity == 0 {
                continue;
            }
            if self.data.len() == self.capacity {
                self.data.pop_front();
            }
            self.data.push_back(*byte);
        }
    }
    fn read(&self, cursor: usize, max_bytes: usize) -> OutputChunk {
        let retained_start = self.total_seen.saturating_sub(self.data.len());
        let start = cursor.max(retained_start).min(self.total_seen);
        let skip = start.saturating_sub(retained_start);
        let bytes: Vec<u8> = self
            .data
            .iter()
            .skip(skip)
            .take(max_bytes)
            .copied()
            .collect();
        let next_cursor = start.saturating_add(bytes.len());
        OutputChunk {
            text: String::from_utf8_lossy(&bytes).into_owned(),
            cursor,
            next_cursor,
            truncated_before_cursor: cursor < retained_start,
            total_bytes_seen: self.total_seen,
        }
    }
}

#[derive(Clone, Copy)]
struct ProcessIdentity {
    pid: u32,
    pgid: i32,
    start_time: u64,
}

pub struct DiagnosticsManager {
    inner: Arc<ManagerInner>,
}

impl DiagnosticsManager {
    pub fn new(limits: Limits) -> Result<Self> {
        ensure!(limits.max_processes > 0, "max_processes must be positive");
        ensure!(
            limits.max_total_processes > 0,
            "max_total_processes must be positive"
        );
        ensure!(
            limits.max_total_output_bytes > 0,
            "max_total_output_bytes must be positive"
        );
        ensure!(
            limits.max_output_bytes_per_process > 0,
            "max_output_bytes_per_process must be positive"
        );
        ensure!(
            limits.max_retained_bytes_per_stream > 0,
            "max_retained_bytes_per_stream must be positive"
        );
        ensure!(
            !limits.max_runtime.is_zero(),
            "max_runtime must be positive"
        );
        ensure!(
            limits.max_argv_count > 0 && limits.max_argv_bytes > 0,
            "argv limits must be positive"
        );
        Ok(Self {
            inner: Arc::new(ManagerInner {
                limits,
                state: Mutex::new(ManagerState {
                    entries: HashMap::new(),
                    active_count: 0,
                    total_started: 0,
                    starting_by_owner: HashMap::new(),
                    stopped_owners: HashSet::new(),
                }),
                state_cv: Condvar::new(),
                total_output: AtomicUsize::new(0),
                output_overflow: AtomicBool::new(false),
            }),
        })
    }

    pub fn start(
        &self,
        owner: OwnerKey,
        spec: &LaunchSpec,
        context: EffectiveContext,
    ) -> Result<StartReceipt> {
        let label = safe_label(spec.name.as_deref().unwrap_or("diagnostic"));
        validate_argv(spec, &self.inner.limits)?;
        let cwd = fs::canonicalize(&context.cwd).context("resolving validated diagnostic cwd")?;
        ensure!(cwd.is_dir(), "effective diagnostic cwd must be a directory");
        validate_environment(&context.environment)?;
        let executable = resolve_executable(&spec.program, &cwd, &context.environment)?;
        let (mut command, backend, detached_children_contained) =
            build_command(&executable, &spec.args, &cwd, &context)?;
        command.current_dir(&cwd).env_clear();
        for (name, value) in &context.environment {
            command.env(name, value);
        }
        command
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped());
        #[cfg(unix)]
        command.process_group(0);

        let mut admission = self.reserve_admission(owner.clone())?;
        ensure!(
            !self.inner.output_overflow.load(Ordering::Acquire),
            "diagnostics_output_limit: manager output budget is exhausted"
        );
        let child = command
            .spawn()
            .context("starting owned diagnostic process")?;
        let identity = process_identity(child.id());
        let child = Arc::new(Mutex::new(Some(child)));
        let pipes = {
            let mut child_guard = lock(&child);
            let child_ref = child_guard
                .as_mut()
                .context("owned diagnostic child disappeared during start")?;
            child_ref.stdout.take().zip(child_ref.stderr.take())
        };
        let (stdout, stderr) = match pipes {
            Some(pipes) => pipes,
            None => {
                cleanup_unmonitored_child(&child, identity, self.inner.limits.termination_grace);
                bail!("capturing diagnostic output pipes failed");
            }
        };
        let id = ulid::Ulid::generate().to_string();
        let entry = Arc::new(ProcessEntry {
            id: id.clone(),
            owner,
            backend,
            detached_children_contained,
            started: Instant::now(),
            identity,
            process_output: AtomicUsize::new(0),
            reader_stop: AtomicBool::new(false),
            drain_deadline: Mutex::new(None),
            stdout_eof: AtomicBool::new(false),
            stderr_eof: AtomicBool::new(false),
            capture_gap_reason: Mutex::new(None),
            stdout: Mutex::new(BoundedBytes::new(
                self.inner.limits.max_retained_bytes_per_stream,
            )),
            stderr: Mutex::new(BoundedBytes::new(
                self.inner.limits.max_retained_bytes_per_stream,
            )),
            cancel: AtomicBool::new(false),
            finished: Mutex::new(None),
            finished_cv: Condvar::new(),
            monitor: Mutex::new(None),
        });
        let stdout = match nonblocking(stdout) {
            Ok(reader) => reader,
            Err(error) => {
                cleanup_unmonitored_child(&child, identity, self.inner.limits.termination_grace);
                return Err(error).context("preparing diagnostic stdout reader");
            }
        };
        let stderr = match nonblocking(stderr) {
            Ok(reader) => reader,
            Err(error) => {
                cleanup_unmonitored_child(&child, identity, self.inner.limits.termination_grace);
                return Err(error).context("preparing diagnostic stderr reader");
            }
        };

        let stdout_thread = match spawn_reader(
            OutputStream::Stdout,
            stdout,
            Arc::clone(&entry),
            Arc::clone(&self.inner),
        ) {
            Ok(thread) => thread,
            Err(error) => {
                entry.reader_stop.store(true, Ordering::Release);
                cleanup_unmonitored_child(&child, identity, self.inner.limits.termination_grace);
                return Err(error);
            }
        };
        let stderr_thread = match spawn_reader(
            OutputStream::Stderr,
            stderr,
            Arc::clone(&entry),
            Arc::clone(&self.inner),
        ) {
            Ok(thread) => thread,
            Err(error) => {
                entry.reader_stop.store(true, Ordering::Release);
                cleanup_unmonitored_child(&child, identity, self.inner.limits.termination_grace);
                let _ = stdout_thread.join();
                return Err(error);
            }
        };
        let readers = Arc::new(Mutex::new(vec![stdout_thread, stderr_thread]));
        let monitor_entry = Arc::clone(&entry);
        let monitor_inner = Arc::clone(&self.inner);
        let monitor_child = Arc::clone(&child);
        let monitor_readers = Arc::clone(&readers);
        let grace = self.inner.limits.termination_grace;
        let timeout = self.inner.limits.max_runtime;
        let drain_timeout = self.inner.limits.output_drain_timeout;
        let monitor = match thread::Builder::new()
            .name("diagnostics-owner".into())
            .spawn(move || {
                if let Some(child) = lock(&monitor_child).take() {
                    monitor_process(
                        child,
                        monitor_entry,
                        monitor_inner,
                        monitor_readers,
                        timeout,
                        grace,
                        drain_timeout,
                    )
                }
            }) {
            Ok(thread) => thread,
            Err(error) => {
                entry.cancel.store(true, Ordering::Release);
                entry.reader_stop.store(true, Ordering::Release);
                cleanup_unmonitored_child(&child, identity, grace);
                join_readers(&readers);
                bail!("starting diagnostic monitor failed: {error}");
            }
        };
        *lock(&entry.monitor) = Some(monitor);
        admission.commit(Arc::clone(&entry));
        Ok(StartReceipt {
            id,
            label,
            backend,
            argv_count: 1 + spec.args.len(),
            detached_children_contained,
        })
    }

    fn reserve_admission(&self, owner: OwnerKey) -> Result<Admission> {
        let mut state = lock(&self.inner.state);
        ensure!(
            !state.stopped_owners.contains(&owner),
            "diagnostics_owner_stopped: this session/run has already been stopped"
        );
        ensure!(
            state.active_count < self.inner.limits.max_processes,
            "diagnostics_process_limit: owned process limit reached"
        );
        ensure!(
            state.total_started < self.inner.limits.max_total_processes,
            "diagnostics_total_process_limit: manager launch count limit reached"
        );
        state.active_count += 1;
        state.total_started += 1;
        *state.starting_by_owner.entry(owner.clone()).or_default() += 1;
        Ok(Admission {
            inner: Arc::clone(&self.inner),
            owner,
            committed: false,
        })
    }

    pub fn status(&self, owner: &OwnerKey, id: &str) -> Result<Status> {
        let entry = self.owned_entry(owner, id)?;
        let finished = lock(&entry.finished).clone();
        let is_finished = finished.is_some();
        let stdout_eof = entry.stdout_eof.load(Ordering::Acquire);
        let stderr_eof = entry.stderr_eof.load(Ordering::Acquire);
        let capture_gap_reason = *lock(&entry.capture_gap_reason);
        Ok(Status {
            id: entry.id.clone(),
            lifecycle: finished
                .as_ref()
                .map_or(Lifecycle::Running, |state| state.lifecycle),
            exit_code: finished.and_then(|state| state.exit_code),
            elapsed_ms: entry.started.elapsed().as_millis(),
            output_bytes: entry.process_output.load(Ordering::Acquire),
            capture_complete: is_finished
                .then_some(stdout_eof && stderr_eof && capture_gap_reason.is_none()),
            capture_gaps: if is_finished {
                [
                    (!stdout_eof).then_some(OutputStream::Stdout),
                    (!stderr_eof).then_some(OutputStream::Stderr),
                ]
                .into_iter()
                .flatten()
                .collect()
            } else {
                Vec::new()
            },
            capture_gap_reason,
            backend: entry.backend,
            detached_children_contained: entry.detached_children_contained,
        })
    }

    pub fn read_output(
        &self,
        owner: &OwnerKey,
        id: &str,
        stream: OutputStream,
        cursor: usize,
        max_bytes: usize,
    ) -> Result<OutputChunk> {
        ensure!(
            max_bytes > 0 && max_bytes <= self.inner.limits.max_retained_bytes_per_stream,
            "output read size is outside the configured bound"
        );
        let entry = self.owned_entry(owner, id)?;
        let buffer = match stream {
            OutputStream::Stdout => &entry.stdout,
            OutputStream::Stderr => &entry.stderr,
        };
        Ok(lock(buffer).read(cursor, max_bytes))
    }

    pub fn cancel(&self, owner: &OwnerKey, id: &str) -> Result<Status> {
        let entry = self.owned_entry(owner, id)?;
        entry.cancel.store(true, Ordering::Release);
        self.wait_for_entry(&entry)?;
        self.status(owner, id)
    }

    /// Cancel one owned process and remove its retained entry after returning
    /// the final bounded status. The caller may not target another owner.
    pub fn stop(&self, owner: &OwnerKey, id: &str) -> Result<Status> {
        let status = self.cancel(owner, id)?;
        lock(&self.inner.state).entries.remove(id);
        Ok(status)
    }

    pub fn stop_owner(&self, owner: &OwnerKey) -> Result<usize> {
        let mut state = lock(&self.inner.state);
        state.stopped_owners.insert(owner.clone());
        while state.starting_by_owner.get(owner).copied().unwrap_or(0) != 0 {
            state = self
                .inner
                .state_cv
                .wait(state)
                .unwrap_or_else(std::sync::PoisonError::into_inner);
        }
        let entries: Vec<_> = state
            .entries
            .values()
            .filter(|entry| &entry.owner == owner)
            .cloned()
            .collect();
        drop(state);
        for entry in &entries {
            entry.cancel.store(true, Ordering::Release);
        }
        for entry in &entries {
            self.wait_for_entry(entry)?;
        }
        let mut state = lock(&self.inner.state);
        for entry in &entries {
            state.entries.remove(&entry.id);
        }
        Ok(entries.len())
    }

    fn owned_entry(&self, owner: &OwnerKey, id: &str) -> Result<Arc<ProcessEntry>> {
        lock(&self.inner.state)
            .entries
            .get(id)
            .filter(|entry| &entry.owner == owner)
            .cloned()
            .context("diagnostic process not found for this session/run")
    }

    fn wait_for_entry(&self, entry: &ProcessEntry) -> Result<()> {
        let deadline = Instant::now()
            + self.inner.limits.termination_grace
            + self.inner.limits.output_drain_timeout
            + Duration::from_secs(2);
        let mut finished = lock(&entry.finished);
        while finished.is_none() {
            let now = Instant::now();
            if now >= deadline {
                bail!(
                    "diagnostics_cleanup_timeout: owned process did not stop within the cleanup bound"
                );
            }
            let (next, _) = entry
                .finished_cv
                .wait_timeout(finished, deadline.saturating_duration_since(now))
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            finished = next;
        }
        drop(finished);
        if let Some(monitor) = lock(&entry.monitor).take() {
            let _ = monitor.join();
        }
        Ok(())
    }

    pub fn seed_file(
        &self,
        workspace: &SelectedWorkspace,
        relative_path: &Path,
        content: &[u8],
        expected_sha256: Option<&str>,
    ) -> Result<SeedReceipt> {
        seed_workspace_file(&workspace.root, relative_path, content, expected_sha256)
    }
}

impl Drop for DiagnosticsManager {
    fn drop(&mut self) {
        let entries: Vec<_> = lock(&self.inner.state).entries.values().cloned().collect();
        for entry in &entries {
            entry.cancel.store(true, Ordering::Release);
        }
        for entry in &entries {
            let _ = self.wait_for_entry(entry);
        }
        lock(&self.inner.state).entries.clear();
    }
}

fn monitor_process(
    mut child: Child,
    entry: Arc<ProcessEntry>,
    inner: Arc<ManagerInner>,
    readers: Arc<Mutex<Vec<JoinHandle<()>>>>,
    timeout: Duration,
    grace: Duration,
    drain_timeout: Duration,
) {
    let mut terminal = None;
    let mut observed_members = Vec::new();
    let exit_status = loop {
        if let Some(leader) = entry.identity.filter(|identity| verify_identity(*identity)) {
            extend_group_members(&mut observed_members, process_group_members(leader.pgid));
        }
        let reason = if entry.cancel.load(Ordering::Acquire) {
            Some(Lifecycle::Cancelled)
        } else if inner.output_overflow.load(Ordering::Acquire) {
            Some(Lifecycle::OutputLimitExceeded)
        } else if entry.started.elapsed() >= timeout {
            Some(Lifecycle::TimedOut)
        } else {
            None
        };
        if let Some(reason) = reason {
            terminal = Some(reason);
            terminate_owned(&mut child, entry.identity, &observed_members, grace);
            break child.wait().ok();
        }
        if let Some(status) = child.try_wait().ok().flatten() {
            if observed_members.iter().any(|member| {
                entry.identity.is_none_or(|leader| member.pid != leader.pid)
                    && verify_member_identity(*member)
            }) {
                terminate_group_members(&observed_members, grace);
            }
            break Some(status);
        }
        thread::sleep(DEFAULT_POLL);
    };
    let exit_code = exit_status.and_then(|status| status.code());
    *lock(&entry.drain_deadline) = Some(Instant::now() + drain_timeout);
    entry.reader_stop.store(true, Ordering::Release);
    join_readers(&readers);
    let lifecycle = terminal.unwrap_or_else(|| {
        if inner.output_overflow.load(Ordering::Acquire) {
            Lifecycle::OutputLimitExceeded
        } else if entry.cancel.load(Ordering::Acquire) {
            Lifecycle::Cancelled
        } else {
            Lifecycle::Exited
        }
    });
    {
        let mut state = lock(&inner.state);
        state.active_count = state.active_count.saturating_sub(1);
        inner.state_cv.notify_all();
    }
    *lock(&entry.finished) = Some(Finished {
        lifecycle,
        exit_code,
    });
    entry.finished_cv.notify_all();
}

fn join_readers(readers: &Arc<Mutex<Vec<JoinHandle<()>>>>) {
    let handles = std::mem::take(&mut *lock(readers));
    for handle in handles {
        let _ = handle.join();
    }
}

fn cleanup_unmonitored_child(
    child: &Arc<Mutex<Option<Child>>>,
    identity: Option<ProcessIdentity>,
    grace: Duration,
) {
    if let Some(mut child) = lock(child).take() {
        let members =
            identity.map_or_else(Vec::new, |identity| process_group_members(identity.pgid));
        terminate_owned(&mut child, identity, &members, grace);
        let _ = child.wait();
    }
}

fn terminate_owned(
    child: &mut Child,
    identity: Option<ProcessIdentity>,
    observed: &[ProcessIdentity],
    grace: Duration,
) {
    let mut original_members = observed.to_vec();
    if let Some(leader) = identity.filter(|identity| verify_identity(*identity)) {
        extend_group_members(&mut original_members, process_group_members(leader.pgid));
    }
    if original_members.is_empty() {
        let _ = child.kill();
    } else {
        terminate_group_members(&original_members, grace);
    }
    let _ = child.kill();
}

fn terminate_group_members(observed: &[ProcessIdentity], grace: Duration) {
    let mut original_members = observed.to_vec();
    for member in &original_members {
        signal_identity(*member, libc::SIGTERM);
    }
    let deadline = Instant::now() + grace;
    while Instant::now() < deadline {
        if !original_members
            .iter()
            .any(|member| verify_member_identity(*member))
        {
            break;
        }
        thread::sleep(DEFAULT_POLL);
    }
    if let Some(pgid) = original_members.first().map(|anchor| anchor.pgid)
        && original_members
            .iter()
            .any(|member| verify_member_identity(*member))
    {
        extend_group_members(&mut original_members, process_group_members(pgid));
        for member in &original_members {
            signal_identity(*member, libc::SIGKILL);
        }
    }
}

fn extend_group_members(known: &mut Vec<ProcessIdentity>, new: Vec<ProcessIdentity>) {
    for member in new {
        if !known.iter().any(|candidate| {
            candidate.pid == member.pid
                && candidate.start_time == member.start_time
                && candidate.pgid == member.pgid
        }) {
            known.push(member);
        }
    }
}

fn spawn_reader<R: Read + Send + 'static>(
    stream: OutputStream,
    mut reader: R,
    entry: Arc<ProcessEntry>,
    inner: Arc<ManagerInner>,
) -> Result<JoinHandle<()>> {
    let name = match stream {
        OutputStream::Stdout => "diagnostics-stdout",
        OutputStream::Stderr => "diagnostics-stderr",
    };
    thread::Builder::new()
        .name(name.to_owned())
        .spawn(move || {
            let mut chunk = [0_u8; 8192];
            loop {
                if entry
                    .drain_deadline
                    .lock()
                    .map(|deadline| deadline.is_some_and(|deadline| Instant::now() >= deadline))
                    .unwrap_or(true)
                {
                    set_capture_gap(&entry, CaptureGapReason::DrainDeadline);
                    break;
                }
                match reader.read(&mut chunk) {
                    Ok(0) => {
                        match stream {
                            OutputStream::Stdout => entry.stdout_eof.store(true, Ordering::Release),
                            OutputStream::Stderr => entry.stderr_eof.store(true, Ordering::Release),
                        }
                        break;
                    }
                    Ok(n) => {
                        let global =
                            reserve(&inner.total_output, inner.limits.max_total_output_bytes, n);
                        let process = reserve(
                            &entry.process_output,
                            inner.limits.max_output_bytes_per_process,
                            global,
                        );
                        let accepted = process;
                        let buffer = match stream {
                            OutputStream::Stdout => &entry.stdout,
                            OutputStream::Stderr => &entry.stderr,
                        };
                        lock(buffer).push(&chunk[..accepted]);
                        if accepted < n {
                            inner.output_overflow.store(true, Ordering::Release);
                            set_capture_gap(&entry, CaptureGapReason::OutputLimit);
                            break;
                        }
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        if entry.reader_stop.load(Ordering::Acquire) {
                            if !match stream {
                                OutputStream::Stdout => entry.stdout_eof.load(Ordering::Acquire),
                                OutputStream::Stderr => entry.stderr_eof.load(Ordering::Acquire),
                            } {
                                set_capture_gap(&entry, CaptureGapReason::PipeStillOpenAtStop);
                            }
                            break;
                        }
                        thread::sleep(DEFAULT_POLL)
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(_) => break,
                }
            }
        })
        .context("starting bounded diagnostic output reader")
}

fn set_capture_gap(entry: &ProcessEntry, reason: CaptureGapReason) {
    let mut current = lock(&entry.capture_gap_reason);
    if current.is_none() {
        *current = Some(reason);
    }
}

fn reserve(counter: &AtomicUsize, limit: usize, requested: usize) -> usize {
    let mut current = counter.load(Ordering::Acquire);
    loop {
        let available = limit.saturating_sub(current);
        let accepted = requested.min(available);
        match counter.compare_exchange_weak(
            current,
            current.saturating_add(accepted),
            Ordering::AcqRel,
            Ordering::Acquire,
        ) {
            Ok(_) => return accepted,
            Err(actual) => current = actual,
        }
    }
}

#[cfg(unix)]
fn nonblocking<T: std::os::fd::AsRawFd>(reader: T) -> Result<T> {
    let flags = unsafe { libc::fcntl(reader.as_raw_fd(), libc::F_GETFL) };
    ensure!(flags >= 0, "reading diagnostic pipe flags failed");
    ensure!(
        unsafe { libc::fcntl(reader.as_raw_fd(), libc::F_SETFL, flags | libc::O_NONBLOCK) } >= 0,
        "setting diagnostic pipe nonblocking mode failed"
    );
    Ok(reader)
}

#[cfg(not(unix))]
fn nonblocking<T>(reader: T) -> Result<T> {
    Ok(reader)
}

fn build_command(
    executable: &Path,
    args: &[String],
    cwd: &Path,
    context: &EffectiveContext,
) -> Result<(Command, &'static str, bool)> {
    match &context.containment {
        ContainmentPolicy::Host => {
            let mut command = Command::new(executable);
            command.args(args);
            Ok((command, "host", false))
        }
        ContainmentPolicy::BubblewrapPidNamespace {
            executable: bwrap,
            writable_roots,
            unshare_network,
        } => {
            ensure!(
                cfg!(target_os = "linux"),
                "Bubblewrap PID namespace is available only on Linux"
            );
            ensure!(
                bwrap.is_absolute() && bwrap.is_file(),
                "validated Bubblewrap executable path is unavailable"
            );
            let mut command = Command::new(bwrap);
            command.args([
                OsStr::new("--die-with-parent"),
                OsStr::new("--unshare-user"),
                OsStr::new("--unshare-pid"),
                OsStr::new("--ro-bind"),
                OsStr::new("/"),
                OsStr::new("/"),
                OsStr::new("--dev"),
                OsStr::new("/dev"),
                OsStr::new("--proc"),
                OsStr::new("/proc"),
                OsStr::new("--tmpfs"),
                OsStr::new("/tmp"),
            ]);
            if *unshare_network {
                command.arg("--unshare-net");
            }
            for root in writable_roots {
                let canonical = fs::canonicalize(root)
                    .context("resolving validated Bubblewrap writable root")?;
                ensure!(
                    canonical == *root && canonical.is_dir(),
                    "Bubblewrap writable roots must be canonical directories"
                );
                command.args([
                    OsStr::new("--bind"),
                    canonical.as_os_str(),
                    canonical.as_os_str(),
                ]);
            }
            command
                .arg("--chdir")
                .arg(cwd)
                .arg("--")
                .arg(executable)
                .args(args);
            Ok((command, "bubblewrap_pid_namespace", true))
        }
    }
}

fn validate_argv(spec: &LaunchSpec, limits: &Limits) -> Result<()> {
    ensure!(
        !spec.program.is_empty() && !spec.program.contains('\0'),
        "diagnostic executable is invalid"
    );
    ensure!(
        spec.args.len() < limits.max_argv_count,
        "diagnostics_argv_limit: too many argv entries"
    );
    let bytes = spec
        .program
        .len()
        .saturating_add(spec.args.iter().map(String::len).sum::<usize>());
    ensure!(
        bytes <= limits.max_argv_bytes,
        "diagnostics_argv_limit: argv byte limit exceeded"
    );
    ensure!(
        spec.args.iter().all(|arg| !arg.contains('\0')),
        "diagnostic argv contains NUL"
    );
    Ok(())
}

fn validate_environment(environment: &BTreeMap<String, String>) -> Result<()> {
    for (name, value) in environment {
        let mut bytes = name.bytes();
        ensure!(
            bytes
                .next()
                .is_some_and(|byte| byte.is_ascii_alphabetic() || byte == b'_')
                && bytes.all(|byte| byte.is_ascii_alphanumeric() || byte == b'_'),
            "invalid effective environment variable name"
        );
        ensure!(
            !value.contains('\0'),
            "effective environment value contains NUL"
        );
    }
    Ok(())
}

fn resolve_executable(
    program: &str,
    cwd: &Path,
    environment: &BTreeMap<String, String>,
) -> Result<PathBuf> {
    let candidate = Path::new(program);
    if candidate.components().count() > 1 || candidate.is_absolute() {
        let path = if candidate.is_absolute() {
            candidate.to_path_buf()
        } else {
            cwd.join(candidate)
        };
        return validate_executable(&path);
    }
    let search = environment.get("PATH").map(String::as_str).unwrap_or("");
    for directory in std::env::split_paths(OsStr::new(search)) {
        let path = directory.join(program);
        if path.is_file()
            && let Ok(resolved) = validate_executable(&path)
        {
            return Ok(resolved);
        }
    }
    bail!("diagnostic executable was not found in the effective PATH")
}

fn validate_executable(path: &Path) -> Result<PathBuf> {
    let resolved = fs::canonicalize(path).context("resolving diagnostic executable")?;
    let metadata = fs::metadata(&resolved)?;
    ensure!(
        metadata.is_file(),
        "diagnostic executable must be a regular file"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        ensure!(
            metadata.permissions().mode() & 0o111 != 0,
            "diagnostic executable is not executable"
        );
    }
    Ok(resolved)
}

fn safe_label(label: &str) -> String {
    if !label.is_empty()
        && label.len() <= 48
        && label
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
    {
        label.to_owned()
    } else {
        "diagnostic".to_owned()
    }
}

fn validate_owner_component(value: &str) -> Result<()> {
    ensure!(
        !value.is_empty()
            && value.len() <= 128
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.')),
        "invalid internal diagnostic owner identifier"
    );
    Ok(())
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(target_os = "linux")]
fn process_identity(pid: u32) -> Option<ProcessIdentity> {
    let text = fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let (_, rest) = text.rsplit_once(')')?;
    let fields: Vec<_> = rest.split_whitespace().collect();
    Some(ProcessIdentity {
        pid,
        pgid: fields.get(2)?.parse().ok()?,
        start_time: fields.get(19)?.parse().ok()?,
    })
}

#[cfg(not(target_os = "linux"))]
fn process_identity(_pid: u32) -> Option<ProcessIdentity> {
    None
}

fn verify_identity(identity: ProcessIdentity) -> bool {
    verify_member_identity(identity) && identity.pgid == identity.pid as i32
}

fn verify_member_identity(identity: ProcessIdentity) -> bool {
    process_identity(identity.pid).is_some_and(|current| {
        current.pgid == identity.pgid && current.start_time == identity.start_time
    })
}

#[cfg(target_os = "linux")]
fn process_group_members(pgid: i32) -> Vec<ProcessIdentity> {
    let Ok(entries) = fs::read_dir("/proc") else {
        return Vec::new();
    };
    entries
        .flatten()
        .filter_map(|entry| {
            let pid = entry.file_name().to_string_lossy().parse::<u32>().ok()?;
            process_identity(pid).filter(|identity| identity.pgid == pgid)
        })
        .collect()
}

#[cfg(not(target_os = "linux"))]
fn process_group_members(_pgid: i32) -> Vec<ProcessIdentity> {
    Vec::new()
}

#[cfg(target_os = "linux")]
fn signal_identity(identity: ProcessIdentity, signal: i32) {
    use std::os::fd::RawFd;
    let pidfd =
        unsafe { libc::syscall(libc::SYS_pidfd_open, identity.pid as libc::pid_t, 0) as RawFd };
    if pidfd < 0 {
        return;
    }
    if verify_member_identity(identity) {
        unsafe {
            libc::syscall(
                libc::SYS_pidfd_send_signal,
                pidfd,
                signal,
                std::ptr::null::<libc::siginfo_t>(),
                0,
            );
        }
    }
    unsafe {
        libc::close(pidfd);
    }
}

#[cfg(not(target_os = "linux"))]
fn signal_identity(identity: ProcessIdentity, signal: i32) {
    if verify_identity(identity) {
        unsafe {
            libc::kill(identity.pid as libc::pid_t, signal);
        }
    }
}

fn seed_workspace_file(
    workspace_root: &Path,
    relative_path: &Path,
    content: &[u8],
    expected_sha256: Option<&str>,
) -> Result<SeedReceipt> {
    ensure!(
        content.len() <= SEED_FILE_MAX_BYTES,
        "seed_file_limit: content exceeds 1 MiB"
    );
    ensure!(
        !relative_path.as_os_str().is_empty() && !relative_path.is_absolute(),
        "seed path must be non-empty and relative to the selected workspace"
    );
    let parts: Vec<_> = relative_path
        .components()
        .map(|component| match component {
            Component::Normal(part) => Ok(part.to_owned()),
            _ => bail!("seed path may contain only normal relative components"),
        })
        .collect::<Result<_>>()?;
    ensure!(!parts.is_empty(), "seed path must include a filename");
    #[cfg(unix)]
    {
        seed_workspace_file_unix(workspace_root, &parts, content, expected_sha256)
    }
    #[cfg(not(unix))]
    {
        let _ = (workspace_root, parts, content, expected_sha256);
        bail!("safe workspace seeding is not supported on this platform")
    }
}

#[cfg(unix)]
fn seed_workspace_file_unix(
    workspace_root: &Path,
    parts: &[OsString],
    content: &[u8],
    expected_sha256: Option<&str>,
) -> Result<SeedReceipt> {
    use std::os::fd::{AsRawFd, FromRawFd};
    let root = fs::canonicalize(workspace_root).context("resolving selected copied workspace")?;
    ensure!(root.is_dir(), "selected workspace is not a directory");
    let root_name = CString::new(root.as_os_str().as_encoded_bytes())?;
    let root_fd = unsafe {
        libc::open(
            root_name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    ensure!(root_fd >= 0, "opening selected workspace root failed");
    let mut parent = unsafe { File::from_raw_fd(root_fd) };
    for component in &parts[..parts.len() - 1] {
        let fd = open_or_create_directory(parent.as_raw_fd(), component)?;
        parent = unsafe { File::from_raw_fd(fd) };
    }
    let filename = CString::new(parts.last().unwrap().as_encoded_bytes())?;
    if let Some(expected) = expected_sha256 {
        ensure!(
            valid_digest(expected),
            "expected_sha256 must be 64 lowercase hexadecimal characters"
        );
        let fd = unsafe {
            libc::openat(
                parent.as_raw_fd(),
                filename.as_ptr(),
                libc::O_RDWR | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
        ensure!(fd >= 0, "CAS overwrite requires an existing regular file");
        let mut file = unsafe { File::from_raw_fd(fd) };
        ensure!(
            unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX) } == 0,
            "locking seed file for compare-and-swap failed"
        );
        let metadata = file.metadata()?;
        use std::os::unix::fs::MetadataExt;
        ensure!(
            metadata.is_file() && metadata.nlink() == 1,
            "CAS target must be a singly linked regular file"
        );
        let mut existing =
            Vec::with_capacity(metadata.len().min(SEED_FILE_MAX_BYTES as u64) as usize);
        (&mut file)
            .take((SEED_FILE_MAX_BYTES + 1) as u64)
            .read_to_end(&mut existing)?;
        ensure!(
            existing.len() <= SEED_FILE_MAX_BYTES,
            "CAS target grew beyond 1 MiB while being checked"
        );
        let actual = digest(&existing);
        ensure!(
            actual == expected,
            "seed_file_conflict: existing file hash does not match expected_sha256"
        );
        let metadata = file.metadata()?;
        ensure!(
            metadata.is_file() && metadata.nlink() == 1,
            "CAS target gained a hard link while being checked"
        );
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        file.write_all(content)?;
        file.sync_all()?;
        return Ok(SeedReceipt {
            bytes_written: content.len(),
            sha256: digest(content),
            overwritten: true,
        });
    }
    let fd = unsafe {
        libc::openat(
            parent.as_raw_fd(),
            filename.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            0o600,
        )
    };
    ensure!(
        fd >= 0,
        "seed create failed; target exists or path is unsafe"
    );
    let mut file = unsafe { File::from_raw_fd(fd) };
    if let Err(error) = file.write_all(content).and_then(|()| file.sync_all()) {
        drop(file);
        unsafe {
            libc::unlinkat(parent.as_raw_fd(), filename.as_ptr(), 0);
        }
        return Err(error).context("writing new workspace seed");
    }
    Ok(SeedReceipt {
        bytes_written: content.len(),
        sha256: digest(content),
        overwritten: false,
    })
}

#[cfg(unix)]
fn open_or_create_directory(parent_fd: i32, component: &OsStr) -> Result<i32> {
    use std::os::unix::ffi::OsStrExt;
    let name = CString::new(component.as_bytes())?;
    let mut fd = unsafe {
        libc::openat(
            parent_fd,
            name.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
        )
    };
    if fd < 0 && std::io::Error::last_os_error().kind() == std::io::ErrorKind::NotFound {
        let created = unsafe { libc::mkdirat(parent_fd, name.as_ptr(), 0o700) };
        if created != 0
            && std::io::Error::last_os_error().kind() != std::io::ErrorKind::AlreadyExists
        {
            bail!("creating workspace seed directory failed");
        }
        fd = unsafe {
            libc::openat(
                parent_fd,
                name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
            )
        };
    }
    ensure!(
        fd >= 0,
        "seed path contains a symlink or non-directory component"
    );
    Ok(fd)
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}
fn digest(content: &[u8]) -> String {
    format!("{:x}", Sha256::digest(content))
}
