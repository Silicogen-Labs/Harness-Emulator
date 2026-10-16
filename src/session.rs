use crate::control::{
    BrowserAuthority, BrowserLease, ControlState, ControlStatus, ReleasedMouseButton,
};
use crate::engine::{EngineState, now_ms};
use crate::eventlog::BoundedEventLog;
use crate::frame_stream::StreamFrame;
use crate::protocol::{
    CellRegionSnapshot, FrameSnapshot, HistorySnapshot, LaunchSpec, MouseInput, ScreenshotArtifact,
    SessionSummary,
};
use crate::screenshot;
use anyhow::{Context, Result, bail};
use portable_pty::{Child, CommandBuilder, MasterPty, PtySize, native_pty_system};
use std::collections::{BTreeMap, VecDeque};
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use std::time::Instant;
use tokio::sync::watch;
use ulid::Ulid;

struct RevisionSignal {
    value: Mutex<u64>,
    sender: watch::Sender<u64>,
}

impl RevisionSignal {
    fn new() -> Arc<Self> {
        let (sender, _) = watch::channel(0);
        Arc::new(Self {
            value: Mutex::new(0),
            sender,
        })
    }

    fn publish(&self) -> u64 {
        let mut value = self
            .value
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *value = value.saturating_add(1);
        self.sender.send_replace(*value);
        *value
    }

    fn current(&self) -> u64 {
        *self
            .value
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn subscribe(&self) -> watch::Receiver<u64> {
        self.sender.subscribe()
    }
}

#[derive(Debug, Clone, Default)]
struct ProcessState {
    exit_code: Option<i32>,
    exit_signal: Option<String>,
}

pub enum BrowserInput {
    Key(String),
    Type(String),
    Paste(String),
    Mouse(MouseInput),
    Resize { cols: u16, rows: u16 },
}

#[derive(Debug, Clone)]
pub struct BrowserActionContext {
    pub action_id: String,
    pub frame_id: String,
    pub view_revision: u64,
    pub viewport_revision: u64,
}

#[derive(Debug, serde::Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum BrowserInputResult {
    Accepted,
    Pasted { bracketed: bool },
    Resized { cols: u16, rows: u16 },
}

fn browser_input_kind(input: &BrowserInput) -> &'static str {
    match input {
        BrowserInput::Key(_) => "key",
        BrowserInput::Type(_) => "type",
        BrowserInput::Paste(_) => "paste",
        BrowserInput::Mouse(_) => "mouse",
        BrowserInput::Resize { .. } => "resize",
    }
}

impl ProcessState {
    fn is_running(&self) -> bool {
        self.exit_code.is_none() && self.exit_signal.is_none()
    }

    fn label(&self) -> String {
        if self.is_running() {
            "running".to_string()
        } else if let Some(signal) = &self.exit_signal {
            format!("signaled:{signal}")
        } else {
            format!("exited:{}", self.exit_code.unwrap_or_default())
        }
    }
}

struct RunResources {
    run_id: String,
    process_group: i32,
    master: Arc<Mutex<Box<dyn MasterPty + Send>>>,
    writer: Arc<Mutex<Box<dyn Write + Send>>>,
    child: Arc<Mutex<Box<dyn Child + Send + Sync>>>,
    engine: Arc<Mutex<EngineState>>,
    process_state: Arc<Mutex<ProcessState>>,
    history: Arc<Mutex<BoundedEventLog>>,
    stopping: Arc<AtomicBool>,
    reader_thread: Option<JoinHandle<()>>,
    monitor_thread: Option<JoinHandle<()>>,
}

impl RunResources {
    fn start(
        session_id: &str,
        spec: &LaunchSpec,
        runtime_root: &Path,
        run_id: String,
        revision: Arc<RevisionSignal>,
        environment: &BTreeMap<String, String>,
    ) -> Result<Self> {
        let cwd = PathBuf::from(&spec.cwd);
        if !cwd.is_dir() {
            bail!("working directory is not a directory: {}", cwd.display())
        }
        if spec.program.trim().is_empty() {
            bail!("program path must not be empty")
        }
        if spec.cols == 0 || spec.rows == 0 {
            bail!("terminal size must be greater than zero")
        }
        if spec.cols > 300 || spec.rows > 120 {
            bail!("unsupported terminal size: maximum is 300 columns by 120 rows")
        }
        validate_environment(&spec.env)?;

        let state_root = runtime_root.join("sessions").join(session_id).join("state");
        let home = state_root.join("home");
        let data = state_root.join("data");
        let config = state_root.join("config");
        let cache = state_root.join("cache");
        let state = state_root.join("state");
        let tmp = state_root.join("tmp");
        for path in [&home, &data, &config, &cache, &state, &tmp] {
            create_private_dir(path)?;
        }

        let pty = native_pty_system();
        let pair = pty.openpty(PtySize {
            rows: spec.rows,
            cols: spec.cols,
            pixel_width: 0,
            pixel_height: 0,
        })?;
        let mut command = CommandBuilder::new(&spec.program);
        command.args(&spec.args);
        command.cwd(&cwd);
        command.env_clear();
        for (key, value) in environment {
            command.env(key, value);
        }

        let child = pair.slave.spawn_command(command)?;
        let process_id = child
            .process_id()
            .context("PTY implementation did not expose the child process ID")?;
        let process_group = pair
            .master
            .process_group_leader()
            .unwrap_or(process_id as i32);
        let reader = pair.master.try_clone_reader()?;
        let writer = pair.master.take_writer()?;
        let master = Arc::new(Mutex::new(pair.master));
        let writer = Arc::new(Mutex::new(writer));
        let child: Arc<Mutex<Box<dyn Child + Send + Sync>>> = Arc::new(Mutex::new(child));
        let engine = Arc::new(Mutex::new(EngineState::new(spec.cols, spec.rows)?));
        let process_state = Arc::new(Mutex::new(ProcessState::default()));
        let history = Arc::new(Mutex::new(BoundedEventLog::new(
            session_id,
            run_id.clone(),
            spec.record_output,
        )));
        let stopping = Arc::new(AtomicBool::new(false));

        let reader_engine = Arc::clone(&engine);
        let reader_writer = Arc::clone(&writer);
        let reader_history = Arc::clone(&history);
        let reader_stopping = Arc::clone(&stopping);
        let reader_revision = Arc::clone(&revision);
        let reader_thread = thread::Builder::new()
            .name(format!("pty-read-{session_id}"))
            .spawn(move || {
                let mut reader = reader;
                let mut buffer = vec![0_u8; 16 * 1024];
                loop {
                    match reader.read(&mut buffer) {
                        Ok(0) => break,
                        Ok(length) => {
                            let bytes = &buffer[..length];
                            reader_history
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .push_output(bytes);
                            let replies = {
                                let mut engine = reader_engine
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                                let replies = engine.process_output(bytes);
                                reader_revision.publish();
                                replies
                            };
                            if !replies.is_empty() {
                                let write_result = reader_writer
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                                    .write_all(&replies);
                                if write_result.is_err() {
                                    break;
                                }
                                let _ = reader_writer
                                    .lock()
                                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                                    .flush();
                            }
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                        Err(error)
                            if error.kind() == std::io::ErrorKind::WouldBlock
                                || error.kind() == std::io::ErrorKind::TimedOut =>
                        {
                            if reader_stopping.load(Ordering::Acquire) {
                                break;
                            }
                            continue;
                        }
                        Err(_) => break,
                    }
                }
            })?;

        let monitor_child = Arc::clone(&child);
        let monitor_state = Arc::clone(&process_state);
        let monitor_history = Arc::clone(&history);
        let monitor_stopping = Arc::clone(&stopping);
        let monitor_revision = Arc::clone(&revision);
        let monitor_thread = thread::Builder::new()
            .name(format!("pty-wait-{session_id}"))
            .spawn(move || {
                loop {
                    if monitor_stopping.load(Ordering::Acquire) {
                        break;
                    }
                    let status = monitor_child
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .try_wait();
                    match status {
                        Ok(Some(exit)) => {
                            let mut state = monitor_state
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            state.exit_code = Some(exit.exit_code() as i32);
                            state.exit_signal = exit.signal().map(str::to_owned);
                            monitor_history
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .push_process_exit(state.exit_code, state.exit_signal.as_deref());
                            monitor_revision.publish();
                            break;
                        }
                        Ok(None) => thread::sleep(Duration::from_millis(30)),
                        Err(error) => {
                            let mut state = monitor_state
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            state.exit_code = Some(255);
                            state.exit_signal = Some(format!("wait-error:{error}"));
                            monitor_revision.publish();
                            break;
                        }
                    }
                }
            })?;

        Ok(Self {
            run_id,
            process_group,
            master,
            writer,
            child,
            engine,
            process_state,
            history,
            stopping,
            reader_thread: Some(reader_thread),
            monitor_thread: Some(monitor_thread),
        })
    }

    fn terminate(&mut self) {
        if self.stopping.swap(true, Ordering::AcqRel) {
            return;
        }
        signal_process_group(self.process_group, libc::SIGTERM);
        thread::sleep(Duration::from_millis(250));
        signal_process_group(self.process_group, libc::SIGKILL);
        if let Ok(mut child) = self.child.lock() {
            match child.wait() {
                Ok(exit) => {
                    let mut process = self
                        .process_state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    process.exit_code = Some(exit.exit_code() as i32);
                    process.exit_signal = exit.signal().map(str::to_owned);
                }
                Err(error) => {
                    let mut process = self
                        .process_state
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    process.exit_code = Some(255);
                    process.exit_signal = Some(format!("close-wait-error:{error}"));
                }
            }
        }
        if let Some(reader) = self.reader_thread.take() {
            let _ = reader.join();
        }
        if let Some(monitor) = self.monitor_thread.take() {
            let _ = monitor.join();
        }
        self.writer
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .flush()
            .ok();
    }
}

impl Drop for RunResources {
    fn drop(&mut self) {
        self.terminate()
    }
}

pub struct Session {
    pub id: String,
    runtime_root: PathBuf,
    initial_cols: u16,
    initial_rows: u16,
    spec: Mutex<LaunchSpec>,
    summary: Mutex<SessionSummary>,
    run: Mutex<RunResources>,
    control: Mutex<ControlState>,
    revision: Arc<RevisionSignal>,
    viewport_revision: Arc<AtomicU64>,
    launch_environment: BTreeMap<String, String>,
    diagnostics: serde_json::Value,
    actor: Mutex<ActorState>,
    lifecycle: SessionLifecycle,
    evidence_lock: Mutex<()>,
}

#[derive(Default)]
struct SessionLifecycleState {
    leases: usize,
    retiring: bool,
    retired: bool,
}

#[derive(Clone, Default)]
struct SessionLifecycle(Arc<Mutex<SessionLifecycleState>>);

pub struct EvidenceLease(Arc<Mutex<SessionLifecycleState>>);
impl Drop for EvidenceLease {
    fn drop(&mut self) {
        let mut state = self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.leases = state.leases.saturating_sub(1);
    }
}

pub(crate) struct ReclamationLease {
    lifecycle: Arc<Mutex<SessionLifecycleState>>,
    committed: bool,
}

impl ReclamationLease {
    pub(crate) fn commit(mut self) {
        let mut state = self
            .lifecycle
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state.retired = true;
        state.retiring = false;
        self.committed = true;
    }
}

impl Drop for ReclamationLease {
    fn drop(&mut self) {
        if !self.committed {
            self.lifecycle
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .retiring = false;
        }
    }
}

#[derive(Debug)]
pub struct LaunchFailure {
    pub session_id: String,
    message: String,
}
impl std::fmt::Display for LaunchFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "launch_failed: {} (allocation {})",
            self.message, self.session_id
        )
    }
}
impl std::error::Error for LaunchFailure {}

#[derive(Default)]
struct ActorState {
    owner: Option<String>,
    deadline: Option<Instant>,
    generation: u64,
    results: VecDeque<(String, String, serde_json::Value)>,
}

impl Session {
    pub fn launch(runtime_root: &Path, spec: LaunchSpec) -> Result<Arc<Self>> {
        Self::launch_configured(runtime_root, spec, crate::config::LaunchOptions::default())
    }

    pub fn launch_configured(
        runtime_root: &Path,
        spec: LaunchSpec,
        options: crate::config::LaunchOptions,
    ) -> Result<Arc<Self>> {
        Self::launch_configured_with_id(
            runtime_root,
            spec,
            options,
            Ulid::generate().to_string(),
            None,
            None,
        )
    }

    pub(crate) fn launch_configured_reserved(
        runtime_root: &Path,
        spec: LaunchSpec,
        options: crate::config::LaunchOptions,
        session_id: String,
        max_workspace_bytes: Option<u64>,
        max_seed_bytes: u64,
    ) -> Result<Arc<Self>> {
        Self::launch_configured_with_id(
            runtime_root,
            spec,
            options,
            session_id,
            max_workspace_bytes,
            Some(max_seed_bytes),
        )
    }

    pub(crate) fn discard_rejected_allocation(&self) -> Result<()> {
        let session_root = self.runtime_root.join("sessions").join(&self.id);
        for path in [
            session_root.join("state"),
            self.runtime_root.join("workspaces").join(&self.id),
            self.runtime_root.join("artifacts").join(&self.id),
        ] {
            if path.exists() {
                fs::remove_dir_all(&path).with_context(|| {
                    format!("remove rejected launch allocation {}", path.display())
                })?;
            }
        }
        atomic_json_write(
            &session_root.join("failure.json"),
            &serde_json::json!({
                "code":"launch_allocation_rejected",
                "private_state_removed":true
            }),
        )
    }

    fn launch_configured_with_id(
        runtime_root: &Path,
        spec: LaunchSpec,
        options: crate::config::LaunchOptions,
        session_id: String,
        max_workspace_bytes: Option<u64>,
        max_seed_bytes: Option<u64>,
    ) -> Result<Arc<Self>> {
        let result = Self::launch_allocated(
            runtime_root,
            spec,
            options,
            session_id.clone(),
            max_workspace_bytes,
            max_seed_bytes,
        );
        result.map_err(|error| {
            // Only this fresh allocation is ours. Do not retain imported credentials
            // after a rejected launch; retain its sanitized manifest/failure evidence.
            let session_root = runtime_root.join("sessions").join(&session_id);
            let state = session_root.join("state");
            let workspace = runtime_root.join("workspaces").join(&session_id);
            let state_cleanup = if state.exists() {
                fs::remove_dir_all(&state)
            } else {
                Ok(())
            };
            let workspace_cleanup = if workspace.exists() {
                fs::remove_dir_all(&workspace)
            } else {
                Ok(())
            };
            let cleanup_ok = state_cleanup.is_ok() && workspace_cleanup.is_ok();
            let message = if !cleanup_ok {
                "launch rejected; private allocation cleanup failed".to_owned()
            } else {
                error.to_string()
            };
            let _ = create_private_dir(&session_root);
            let _ = atomic_json_write(
                &session_root.join("failure.json"),
                &serde_json::json!({
                    "code":"launch_failed",
                    "state_cleanup_complete":state_cleanup.is_ok(),
                    "workspace_cleanup_complete":workspace_cleanup.is_ok()
                }),
            );
            LaunchFailure {
                session_id,
                message,
            }
            .into()
        })
    }

    fn launch_allocated(
        runtime_root: &Path,
        mut spec: LaunchSpec,
        mut options: crate::config::LaunchOptions,
        session_id: String,
        max_workspace_bytes: Option<u64>,
        max_seed_bytes: Option<u64>,
    ) -> Result<Arc<Self>> {
        let initial_cols = spec.cols;
        let initial_rows = spec.rows;
        let run_id = Ulid::generate().to_string();
        let session_dir = runtime_root.join("sessions").join(&session_id);
        create_private_dir(&session_dir)?;
        create_private_dir(&session_dir.join("runs"))?;
        if options
            .sensitive_args
            .iter()
            .any(|index| *index >= spec.args.len())
        {
            bail!("invalid_params: sensitive argument index is outside argv")
        }
        if !spec.cwd.contains("${workspace}") {
            spec.cwd = fs::canonicalize(&spec.cwd)
                .context("working directory is unavailable")?
                .to_string_lossy()
                .into_owned();
        }
        if let Some(workspace) = options.workspace.as_mut() {
            workspace.source = fs::canonicalize(&workspace.source)
                .context("workspace source is unavailable")?
                .to_string_lossy()
                .into_owned();
        }
        let requested_spec = spec.clone();
        let workspace = if let Some(workspace) = options.workspace.as_ref() {
            Some(crate::workspace::prepare_workspace_with_limit(
                runtime_root,
                &session_id,
                workspace,
                max_workspace_bytes,
            )?)
        } else {
            None
        };
        if let Some(workspace) = workspace.as_ref() {
            let path = workspace.root.to_string_lossy();
            spec.cwd = spec.cwd.replace("${workspace}", &path);
            for arg in &mut spec.args {
                *arg = arg.replace("${workspace}", &path);
            }
            spec.env
                .insert("EMULATION_WORKSPACE".into(), path.into_owned());
        }
        let prepared = crate::launch::prepare_launch_with_seed_limit(
            runtime_root,
            &session_id,
            &spec,
            &options,
            max_seed_bytes,
        )?;
        let mut diagnostics = prepared.diagnostics;
        let mut log_root = options.metadata.get("app_log_root").cloned();
        if log_root.is_none()
            && let Some(name) = options.profile.as_ref()
        {
            log_root = crate::profiles::load_profile(runtime_root, name)?
                .metadata
                .get("app_log_root")
                .cloned();
        }
        let log_root = log_root.unwrap_or_else(|| "data/silicogen/log".into());
        crate::artifacts::validate_relative(Path::new(&log_root))?;
        diagnostics["app_log_root"] = serde_json::json!(log_root);
        diagnostics["artifact_limits"] = serde_json::json!({"max_bytes":options.metadata.get("artifact_max_bytes").and_then(|v|v.parse::<u64>().ok()).unwrap_or(256*1024*1024),"max_count":options.metadata.get("artifact_max_count").and_then(|v|v.parse::<usize>().ok()).unwrap_or(1000)});
        diagnostics["workspace"] = serde_json::to_value(&workspace)?;
        diagnostics["isolation"] = crate::isolation::capabilities(&options.isolation);
        let visible_spec = prepared.spec;
        validate_spec(&visible_spec)?;
        let effective_spec = crate::isolation::wrap_command(
            &visible_spec,
            &prepared.state_root,
            workspace.as_ref(),
            &options.isolation,
        )?;
        let mut sanitized_spec = requested_spec;
        let required_env_names = sanitized_spec.env.keys().cloned().collect::<Vec<_>>();
        sanitized_spec.env.clear();
        for index in &options.sensitive_args {
            sanitized_spec.args[*index] = "<redacted>".into();
        }
        let mut safe_options = serde_json::to_value(&options)?;
        let shell_script_required = options.shell.is_some();
        if shell_script_required {
            safe_options["shell"]["script"] = serde_json::json!("");
        }
        let manifest = serde_json::json!({"version":1,"spec":sanitized_spec,"options":safe_options,"required_env_names":required_env_names,"shell_script_required":shell_script_required,"argv_values_required":!options.sensitive_args.is_empty()});
        atomic_json_write(&session_dir.join("launch.json"), &manifest)?;
        atomic_json_write(&session_dir.join("launch-diagnostics.json"), &diagnostics)?;
        let revision = RevisionSignal::new();
        let viewport_revision = Arc::new(AtomicU64::new(0));
        let run = RunResources::start(
            &session_id,
            &effective_spec,
            runtime_root,
            run_id.clone(),
            Arc::clone(&revision),
            &prepared.environment,
        )?;
        let summary = SessionSummary {
            session_id: session_id.clone(),
            name: visible_spec.name.clone(),
            run_id,
            state: "running".into(),
            program: visible_spec.program.clone(),
            args: visible_spec
                .args
                .iter()
                .enumerate()
                .map(|(i, arg)| {
                    if options.shell.is_some() || options.sensitive_args.contains(&i) {
                        "<redacted>".into()
                    } else {
                        arg.clone()
                    }
                })
                .collect(),
            cwd: visible_spec.cwd.clone(),
            cols: visible_spec.cols,
            rows: visible_spec.rows,
            started_at_ms: now_ms(),
            exit_code: None,
            process_group: Some(run.process_group),
        };
        let session = Arc::new(Self {
            id: session_id,
            runtime_root: runtime_root.to_path_buf(),
            initial_cols,
            initial_rows,
            spec: Mutex::new(effective_spec),
            summary: Mutex::new(summary),
            run: Mutex::new(run),
            control: Mutex::new(ControlState::default()),
            revision,
            viewport_revision,
            launch_environment: prepared.environment,
            diagnostics,
            actor: Mutex::new(ActorState::default()),
            lifecycle: SessionLifecycle::default(),
            evidence_lock: Mutex::new(()),
        });
        session.persist_metadata()?;
        Ok(session)
    }

    pub fn summary(&self) -> SessionSummary {
        let run = self
            .run
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let process = run
            .process_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut summary = self
            .summary
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if summary.state == "running" && !process.is_running() {
            summary.state = "exited".into();
            summary.exit_code = process.exit_code;
        }
        let (cols, rows) = run
            .engine
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .dimensions();
        summary.cols = cols;
        summary.rows = rows;
        summary.clone()
    }

    pub fn initial_dimensions(&self) -> (u16, u16) {
        (self.initial_cols, self.initial_rows)
    }

    pub fn capabilities(&self) -> serde_json::Value {
        let frame = self.observe();
        serde_json::json!({"session":self.summary(),"launch":self.diagnostics,"viewport_revision":self.viewport_revision.load(Ordering::Acquire),"mouse_tracking":frame.mouse_tracking,"mouse_encoding":frame.mouse_encoding,"keyboard_mode":frame.keyboard_mode,"coordinates":"zero-based row,column","input_preconditions":{"expected_run_id":true,"expected_viewport_revision":true},"controller":self.actor_status()})
    }

    pub fn retain_evidence(&self) -> Result<EvidenceLease> {
        let mut state = self
            .lifecycle
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if state.retiring || state.retired {
            bail!("session_retiring: new access is blocked while cleanup owns this session");
        }
        state.leases += 1;
        Ok(EvidenceLease(Arc::clone(&self.lifecycle.0)))
    }
    pub fn evidence_retained(&self) -> bool {
        self.lifecycle
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .leases
            > 0
    }

    pub(crate) fn begin_reclamation(&self) -> Result<ReclamationLease> {
        let running = self.summary().state == "running";
        let mut state = self
            .lifecycle
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if running || state.retiring || state.retired || state.leases > 0 {
            bail!("session_active: session became active or is leased after the cleanup preview");
        }
        state.retiring = true;
        Ok(ReclamationLease {
            lifecycle: Arc::clone(&self.lifecycle.0),
            committed: false,
        })
    }

    pub(crate) fn is_retired(&self) -> bool {
        self.lifecycle
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retired
    }

    pub fn actor_status(&self) -> serde_json::Value {
        let actor = self
            .actor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let live = actor.deadline.is_some_and(|d| d > Instant::now());
        serde_json::json!({"actor_id":if live {actor.owner.clone()} else {None},"generation":actor.generation,"expires_in_ms":actor.deadline.map(|d|d.saturating_duration_since(Instant::now()).as_millis())})
    }

    pub fn claim_actor(&self, actor_id: &str, release: bool) -> Result<serde_json::Value> {
        if actor_id.is_empty() || actor_id.len() > 128 {
            bail!("invalid_actor: actor identity must contain 1..128 bytes")
        }
        let _control = self.automation_guard_for(Some(actor_id))?;
        let mut actor = self
            .actor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if release {
            actor.owner = None;
            actor.deadline = None;
            actor.generation += 1;
        } else {
            if actor.owner.as_deref() != Some(actor_id)
                || actor.deadline.is_none_or(|d| d <= Instant::now())
            {
                actor.generation += 1;
            }
            actor.owner = Some(actor_id.into());
            actor.deadline = Some(Instant::now() + Duration::from_secs(30));
        }
        Ok(
            serde_json::json!({"actor_id":actor.owner,"generation":actor.generation,"lease_ms":if release{0}else{30000}}),
        )
    }

    pub fn agent_action(
        &self,
        context: &crate::protocol::ActionContext,
        action: &crate::protocol::AgentAction,
    ) -> Result<serde_json::Value> {
        use crate::protocol::AgentAction;
        if context.actor_id.is_empty()
            || context.actor_id.len() > 128
            || context.action_id.is_empty()
            || context.action_id.len() > 128
        {
            bail!("invalid_action: actor/action identifiers must contain 1..128 bytes")
        }
        let mut control = self.automation_guard_for(Some(&context.actor_id))?;
        let current_run = self.summary().run_id;
        if context.expected_run_id != current_run {
            bail!(
                "stale_run: expected {}, current {}",
                context.expected_run_id,
                current_run
            )
        }
        let mut actor = self
            .actor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(generation) = context.controller_generation
            && generation != actor.generation
        {
            bail!("stale_actor: controller generation changed")
        }
        let key = format!("{}:{}", context.actor_id, context.action_id);
        let digest = serde_json::to_string(&(context, action))?;
        if let Some((_, previous, result)) = actor.results.iter().find(|(id, _, _)| id == &key) {
            if previous != &digest {
                bail!("action_conflict: action id was reused with different parameters")
            }
            return Ok(result.clone());
        }
        if matches!(
            action,
            AgentAction::Mouse { .. } | AgentAction::Resize { .. }
        ) {
            let revision = self.viewport_revision.load(Ordering::Acquire);
            if context.expected_viewport_revision != Some(revision) {
                bail!(
                    "stale_viewport: expected {:?}, current {}",
                    context.expected_viewport_revision,
                    revision
                )
            }
        }
        match action {
            AgentAction::Key { key } => self.key_unchecked(key)?,
            AgentAction::Type { text } => self.type_unchecked(text)?,
            AgentAction::Paste { text } => {
                self.paste_unchecked(text)?;
            }
            AgentAction::Mouse { input } => {
                self.mouse_unchecked(input)?;
                control.record_browser_mouse(input);
            }
            AgentAction::Resize { cols, rows } => self.resize_unchecked(*cols, *rows)?,
        }
        let observed = self.observe();
        let result = serde_json::json!({"accepted":true,"actor_id":context.actor_id,"action_id":context.action_id,"run_id":observed.run_id,"frame_id":observed.frame_id,"viewport_revision":self.viewport_revision.load(Ordering::Acquire)});
        actor.results.push_back((key, digest, result.clone()));
        while actor.results.len() > 256 {
            actor.results.pop_front();
        }
        self.record_event("agent_action",serde_json::json!({"actor_id":context.actor_id,"action_id":context.action_id,"run_id":current_run,"accepted":true}));
        Ok(result)
    }

    pub fn observe(&self) -> FrameSnapshot {
        let run = self
            .run
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let state = run
            .process_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .label();
        run.engine
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .snapshot(&self.id, &run.run_id, &state)
    }

    pub fn subscribe_changes(&self) -> watch::Receiver<u64> {
        self.revision.subscribe()
    }

    pub fn stream_snapshot(&self) -> Result<StreamFrame> {
        for _ in 0..3 {
            let control_before = self.control_status()?;
            let (frame, view_revision, viewport_revision, session_state) = {
                let run = self
                    .run
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let engine = run
                    .engine
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let process = run
                    .process_state
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let state = process.label();
                let mut summary = self
                    .summary
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if summary.state == "running" && !process.is_running() {
                    summary.state = "exited".into();
                    summary.exit_code = process.exit_code;
                }
                let session_state = summary.state.clone();
                let frame = engine.snapshot(&self.id, &run.run_id, &state);
                let view_revision = self.revision.current();
                let viewport_revision = self.viewport_revision.load(Ordering::Acquire);
                (frame, view_revision, viewport_revision, session_state)
            };
            let control_after = self.control_status()?;
            if control_before.generation != control_after.generation {
                continue;
            }
            return StreamFrame::from_frame(
                frame,
                view_revision,
                viewport_revision,
                control_after,
                session_state,
            )
            .ok_or_else(|| {
                anyhow::anyhow!("terminal frame contains an unsupported cell or style count")
            });
        }
        bail!("terminal state changed during each atomic snapshot attempt")
    }

    pub fn control_status(&self) -> Result<ControlStatus> {
        let mut control = self
            .control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.reconcile_expired_control(&mut control)?;
        Ok(control.status())
    }

    pub fn take_browser_control(
        &self,
        view_id: &str,
        connection_id: &str,
        permitted: bool,
    ) -> Result<BrowserLease> {
        let mut control = self
            .control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.reconcile_expired_control(&mut control)?;
        control.authorize_automation()?;
        self.release_mouse_events(control.release_pressed_mouse())?;
        let run_id = {
            let run = self
                .run
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            ensure_running(&run)?;
            run.run_id.clone()
        };
        let previous = control.status();
        let lease = control.take_browser_control(view_id, connection_id, &run_id, permitted)?;
        {
            let mut actor = self
                .actor
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            actor.owner = None;
            actor.deadline = None;
            actor.generation += 1;
        }
        let next = control.status();
        if next.generation != previous.generation {
            self.revision.publish();
            self.record_control_transition(&previous, &next);
        }
        Ok(lease)
    }

    pub fn renew_browser_control(&self, authority: &BrowserAuthority) -> Result<ControlStatus> {
        let mut control = self.browser_guard(authority)?;
        control.renew_browser_control(authority)?;
        Ok(control.status())
    }

    pub fn release_browser_control(&self, authority: &BrowserAuthority) -> Result<ControlStatus> {
        let mut control = self.browser_guard(authority)?;
        let previous = control.status();
        let (_, releases) = control.release_browser_control(authority)?;
        let next = control.status();
        if next.generation != previous.generation {
            self.revision.publish();
            self.record_control_transition(&previous, &next);
        }
        self.release_mouse_events(releases)?;
        Ok(control.status())
    }

    pub fn release_browser_control_from_admin(
        &self,
        expected_generation: u64,
    ) -> Result<ControlStatus> {
        let mut control = self
            .control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.reconcile_expired_control(&mut control)?;
        if control.generation() != expected_generation {
            bail!(
                "control_generation_changed: current ownership differs from the observed generation"
            )
        }
        let previous = control.status();
        let releases = control.release_for_admin();
        let next = control.status();
        if next.generation != previous.generation {
            self.revision.publish();
            self.record_control_transition(&previous, &next);
        }
        self.release_mouse_events(releases)?;
        Ok(control.status())
    }

    pub fn browser_input(
        &self,
        authority: &BrowserAuthority,
        context: &BrowserActionContext,
        input: BrowserInput,
    ) -> Result<BrowserInputResult> {
        let mut control = self.browser_guard(authority)?;
        if context.action_id.is_empty() || context.action_id.len() > 128 {
            bail!("invalid_action_id")
        }
        let (current_run_id, current_frame_sequence) = {
            let run = self
                .run
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let sequence = run
                .engine
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .frame_sequence();
            (run.run_id.clone(), sequence)
        };
        if authority.expected_run_id != current_run_id {
            bail!("stale_run: browser action targets a different application run")
        }
        if context.view_revision > self.revision.current() {
            bail!("stale_frame: browser action references a future frame")
        }
        let frame_hex = context
            .frame_id
            .strip_prefix("fr-")
            .ok_or_else(|| anyhow::anyhow!("invalid_browser_frame_id"))?;
        let frame_sequence = u64::from_str_radix(frame_hex, 16)
            .map_err(|_| anyhow::anyhow!("invalid_browser_frame_id"))?;
        if frame_sequence > current_frame_sequence {
            bail!("stale_frame: browser action references a future terminal frame")
        }
        let input_kind = browser_input_kind(&input);
        let pointer_action = matches!(&input, BrowserInput::Mouse(_) | BrowserInput::Resize { .. });
        let current_viewport = self.viewport_revision.load(Ordering::Acquire);
        if pointer_action && current_viewport != context.viewport_revision {
            self.record_browser_action(
                authority,
                context,
                input_kind,
                false,
                Some("stale_viewport"),
            );
            bail!("stale_viewport: terminal geometry changed since the browser pointer action")
        }

        self.record_browser_action(authority, context, input_kind, false, Some("in_progress"));
        let result = match input {
            BrowserInput::Key(key) => self
                .key_unchecked(&key)
                .map(|_| BrowserInputResult::Accepted),
            BrowserInput::Type(text) => self
                .type_unchecked(&text)
                .map(|_| BrowserInputResult::Accepted),
            BrowserInput::Paste(text) => self
                .paste_unchecked(&text)
                .map(|bracketed| BrowserInputResult::Pasted { bracketed }),
            BrowserInput::Mouse(input) => self.mouse_unchecked(&input).map(|_| {
                control.record_browser_mouse(&input);
                BrowserInputResult::Accepted
            }),
            BrowserInput::Resize { cols, rows } => self
                .resize_unchecked(cols, rows)
                .map(|_| BrowserInputResult::Resized { cols, rows }),
        };
        match result {
            Ok(result) => {
                self.record_browser_action(authority, context, input_kind, true, None);
                Ok(result)
            }
            Err(error) => {
                self.record_browser_action(
                    authority,
                    context,
                    input_kind,
                    false,
                    Some(&error.to_string()),
                );
                Err(error)
            }
        }
    }

    pub fn cells(&self, row: u16, col: u16, rows: u16, cols: u16) -> Result<CellRegionSnapshot> {
        if rows == 0 || cols == 0 || rows > 40 || cols > 120 {
            bail!("cell region dimensions must be 1..=120 columns by 1..=40 rows")
        }
        let frame = self.observe();
        if row.saturating_add(rows) > frame.rows || col.saturating_add(cols) > frame.cols {
            bail!("requested cell region extends beyond the current frame")
        }
        let cells = frame.cells[row as usize..(row + rows) as usize]
            .iter()
            .map(|line| line[col as usize..(col + cols) as usize].to_vec())
            .collect();
        Ok(CellRegionSnapshot {
            session_id: frame.session_id,
            run_id: frame.run_id,
            frame_id: frame.frame_id,
            row,
            col,
            rows,
            cols,
            palette: frame.palette,
            cells,
        })
    }

    pub fn history(&self) -> HistorySnapshot {
        let run = self
            .run
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        run.history
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .snapshot()
    }

    fn automation_guard(&self) -> Result<MutexGuard<'_, ControlState>> {
        self.automation_guard_for(None)
    }

    fn automation_guard_for(&self, actor_id: Option<&str>) -> Result<MutexGuard<'_, ControlState>> {
        let mut control = self
            .control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.reconcile_expired_control(&mut control)?;
        control.authorize_automation()?;
        let actor = self
            .actor
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if actor.deadline.is_some_and(|d| d > Instant::now()) && actor.owner.as_deref() != actor_id
        {
            bail!(
                "control_held_by_actor: actor {} owns terminal input",
                actor.owner.as_deref().unwrap_or("unknown")
            );
        }
        Ok(control)
    }

    fn browser_guard(&self, authority: &BrowserAuthority) -> Result<MutexGuard<'_, ControlState>> {
        let mut control = self
            .control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        self.reconcile_expired_control(&mut control)?;
        let run_id = self
            .run
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .run_id
            .clone();
        control.authorize_browser(authority, &run_id)?;
        Ok(control)
    }

    fn reconcile_expired_control(&self, control: &mut ControlState) -> Result<()> {
        let previous = control.status();
        let mut releases = control.release_expired_control().unwrap_or_default();
        let process_exited = {
            let run = self
                .run
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            !run.process_state
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .is_running()
        };
        if process_exited {
            releases.extend(control.release_for_admin());
        }
        let next = control.status();
        if next.generation != previous.generation {
            self.revision.publish();
            self.record_control_transition(&previous, &next);
        }
        self.release_mouse_events(releases)?;
        Ok(())
    }

    fn record_control_transition(&self, previous: &ControlStatus, next: &ControlStatus) {
        let detail = serde_json::json!({
            "source":"control_plane",
            "from_owner":previous.owner,
            "to_owner":next.owner,
            "generation":next.generation,
            "view_id":next.view_id,
            "connection_id":next.connection_id,
        });
        self.record_event("control_changed", detail);
    }

    fn record_browser_action(
        &self,
        authority: &BrowserAuthority,
        context: &BrowserActionContext,
        input_kind: &str,
        accepted: bool,
        error: Option<&str>,
    ) {
        let detail = serde_json::json!({
            "source":"browser",
            "action_id":context.action_id,
            "run_id":authority.expected_run_id,
            "displayed_frame_id":context.frame_id,
            "displayed_view_revision":context.view_revision,
            "viewport_revision":context.viewport_revision,
            "control_generation":authority.generation,
            "input_kind":input_kind,
            "accepted":accepted,
            "error":error.map(|value| value.split(':').next().unwrap_or(value)),
        });
        self.record_event("browser_action", detail);
    }

    fn record_event(&self, kind: &str, detail: serde_json::Value) {
        let Ok(detail) = serde_json::to_string(&detail) else {
            return;
        };
        let run = self
            .run
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        run.history
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push_control_event(kind, &detail);
    }

    fn release_mouse_events(&self, releases: Vec<ReleasedMouseButton>) -> Result<()> {
        if releases.is_empty() {
            return Ok(());
        }
        let run = self
            .run
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !run
            .process_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_running()
        {
            return Ok(());
        }
        let engine = run
            .engine
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (_, row, col) in releases {
            let bytes = engine.encode_mouse(&MouseInput::Up { row, col })?;
            write_input(&run, "mouse_release", &bytes)?;
        }
        Ok(())
    }

    fn key_unchecked(&self, key: &str) -> Result<()> {
        let run = self
            .run
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ensure_running(&run)?;
        let bytes = run
            .engine
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .encode_key(key)?;
        write_input(&run, "key", &bytes)
    }

    pub fn key(&self, key: &str) -> Result<()> {
        let _control = self.automation_guard()?;
        self.key_unchecked(key)
    }

    pub fn type_text(&self, text: &str) -> Result<()> {
        let _control = self.automation_guard()?;
        self.type_unchecked(text)
    }

    fn type_unchecked(&self, text: &str) -> Result<()> {
        if text.len() > 1024 * 1024 {
            bail!("typed text exceeds the 1 MiB request limit")
        }
        let run = self
            .run
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ensure_running(&run)?;
        write_input(&run, "type", text.as_bytes())
    }

    pub fn paste(&self, text: &str) -> Result<bool> {
        let _control = self.automation_guard()?;
        self.paste_unchecked(text)
    }

    fn paste_unchecked(&self, text: &str) -> Result<bool> {
        if text.len() > 1024 * 1024 {
            bail!("paste text exceeds the 1 MiB request limit")
        }
        let run = self
            .run
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ensure_running(&run)?;
        let engine = run
            .engine
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let bracketed = engine.mode(tui_test::terminal::emu::TerminalMode::BracketedPaste);
        let bytes = engine.encode_paste(text);
        drop(engine);
        write_input(
            &run,
            if bracketed {
                "paste_bracketed"
            } else {
                "paste_plain"
            },
            &bytes,
        )?;
        Ok(bracketed)
    }

    pub fn mouse(&self, input: &MouseInput) -> Result<()> {
        let mut control = self.automation_guard()?;
        self.mouse_unchecked(input)?;
        control.record_browser_mouse(input);
        Ok(())
    }

    fn mouse_unchecked(&self, input: &MouseInput) -> Result<()> {
        let run = self
            .run
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ensure_running(&run)?;
        let bytes = run
            .engine
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .encode_mouse(input)?;
        write_input(&run, "mouse", &bytes)
    }

    pub fn resize(&self, cols: u16, rows: u16) -> Result<()> {
        let _control = self.automation_guard()?;
        self.resize_unchecked(cols, rows)
    }

    fn resize_unchecked(&self, cols: u16, rows: u16) -> Result<()> {
        if cols == 0 || rows == 0 {
            bail!("terminal size must be greater than zero")
        }
        if cols > 300 || rows > 120 {
            bail!("unsupported terminal size: maximum is 300 columns by 120 rows")
        }
        let run = self
            .run
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ensure_running(&run)?;
        run.master
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .resize(PtySize {
                rows,
                cols,
                pixel_width: 0,
                pixel_height: 0,
            })?;
        {
            let mut engine = run
                .engine
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            engine.resize(cols, rows)?;
            self.viewport_revision.fetch_add(1, Ordering::AcqRel);
            self.revision.publish();
        }
        run.history
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push_resize(cols, rows);
        let mut spec = self
            .spec
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        spec.cols = cols;
        spec.rows = rows;
        drop(spec);
        let mut summary = self
            .summary
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        summary.cols = cols;
        summary.rows = rows;
        drop(summary);
        drop(run);
        self.persist_metadata()
    }

    pub fn screenshot(&self) -> Result<ScreenshotArtifact> {
        let frame = self.observe();
        self.screenshot_snapshot(&frame)
    }

    pub fn screenshot_snapshot(&self, frame: &FrameSnapshot) -> Result<ScreenshotArtifact> {
        let _lifecycle = self.retain_evidence()?;
        let _lock = self
            .evidence_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if frame.session_id != self.id {
            bail!("snapshot belongs to a different session")
        }
        if frame.cols == 0 || frame.rows == 0 || frame.cols > 300 || frame.rows > 120 {
            bail!("snapshot dimensions are outside the supported terminal bounds")
        }
        let (bytes, count, max_bytes, max_count) = self.evidence_usage()?;
        if count >= max_count || bytes >= max_bytes {
            bail!(
                "artifact_quota_exceeded: retained artifacts require cleanup or larger configured limits"
            )
        }
        let artifact_id = Ulid::generate().to_string();
        let path = self
            .runtime_root
            .join("artifacts")
            .join(&self.id)
            .join(&frame.run_id)
            .join(format!("{artifact_id}-{}.png", frame.frame_id));
        if let Some(parent) = path.parent() {
            create_private_dir(parent)?;
        }
        let artifact = screenshot::render_png(frame, &path, artifact_id)?;
        if bytes.saturating_add(fs::metadata(&path)?.len()) > max_bytes {
            let _ = fs::remove_file(&path);
            bail!("artifact_quota_exceeded: new capture would exceed the session artifact budget")
        }
        Ok(artifact)
    }

    pub(crate) fn evidence_budget_guard(&self, expected_bytes: u64) -> Result<MutexGuard<'_, ()>> {
        let guard = self
            .evidence_lock
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let (bytes, count, max_bytes, max_count) = self.evidence_usage()?;
        if count >= max_count || bytes.saturating_add(expected_bytes) > max_bytes {
            bail!(
                "artifact_quota_exceeded: frame evidence would exceed the session artifact budget"
            )
        }
        Ok(guard)
    }

    fn evidence_usage(&self) -> Result<(u64, usize, u64, usize)> {
        let artifact_root = self.runtime_root.join("artifacts").join(&self.id);
        let mut bytes = 0u64;
        let mut count = 0usize;
        if artifact_root.exists() {
            for directory in fs::read_dir(&artifact_root)? {
                let directory = directory?;
                if directory.file_type()?.is_symlink() {
                    bail!("artifact_path_invalid: symlinked artifact directory")
                };
                if directory.file_type()?.is_dir() {
                    for file in fs::read_dir(directory.path())? {
                        let file = file?;
                        let metadata = file.metadata()?;
                        if file.file_type()?.is_symlink() {
                            bail!("artifact_path_invalid: symlinked artifact file")
                        };
                        if metadata.is_file() {
                            bytes = bytes.saturating_add(metadata.len());
                            count += 1;
                        }
                    }
                }
            }
        }
        let max_bytes = self.diagnostics["artifact_limits"]["max_bytes"]
            .as_u64()
            .unwrap_or(256 * 1024 * 1024);
        let max_count = self.diagnostics["artifact_limits"]["max_count"]
            .as_u64()
            .unwrap_or(1000) as usize;
        Ok((bytes, count, max_bytes, max_count))
    }

    pub fn restart(&self) -> Result<SessionSummary> {
        self.restart_for(None)
    }

    pub fn agent_restart(&self, actor_id: &str) -> Result<SessionSummary> {
        self.restart_for(Some(actor_id))
    }

    fn restart_for(&self, actor_id: Option<&str>) -> Result<SessionSummary> {
        let _lifecycle = self.retain_evidence()?;
        let mut control = self.automation_guard_for(actor_id)?;
        {
            let mut actor = self
                .actor
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            actor.results.clear();
            actor.owner = None;
            actor.deadline = None;
            actor.generation += 1;
        }
        control.release_pressed_mouse();
        let mut run = self
            .run
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let spec = self
            .spec
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        persist_run_history(&self.runtime_root, &self.id, &run)?;
        run.terminate();
        let run_id = Ulid::generate().to_string();
        *run = RunResources::start(
            &self.id,
            &spec,
            &self.runtime_root,
            run_id.clone(),
            Arc::clone(&self.revision),
            &self.launch_environment,
        )?;
        self.revision.publish();
        self.viewport_revision.store(0, Ordering::Release);
        let mut summary = self
            .summary
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        summary.run_id = run_id;
        summary.state = "running".into();
        summary.started_at_ms = now_ms();
        summary.exit_code = None;
        summary.process_group = Some(run.process_group);
        summary.cols = spec.cols;
        summary.rows = spec.rows;
        let result = summary.clone();
        drop(summary);
        drop(run);
        self.persist_metadata()?;
        Ok(result)
    }

    pub fn close(&self) -> Result<SessionSummary> {
        let mut control = self.automation_guard()?;
        self.close_with_control(&mut control)
    }

    pub fn agent_close(&self, actor_id: &str) -> Result<SessionSummary> {
        let mut control = self.automation_guard_for(Some(actor_id))?;
        self.close_with_control(&mut control)
    }

    pub fn close_admin(&self) -> Result<SessionSummary> {
        let mut control = self
            .control
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let previous = control.status();
        control.release_for_admin();
        let next = control.status();
        if next.generation != previous.generation {
            self.revision.publish();
            self.record_control_transition(&previous, &next);
        }
        self.close_with_control(&mut control)
    }

    fn close_with_control(&self, _control: &mut ControlState) -> Result<SessionSummary> {
        let _lifecycle = self.retain_evidence()?;
        _control.release_pressed_mouse();
        let mut run = self
            .run
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        run.terminate();
        self.revision.publish();
        let mut summary = self
            .summary
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        summary.state = "closed".into();
        summary.exit_code = run
            .process_state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .exit_code;
        let result = summary.clone();
        drop(summary);
        drop(run);
        self.persist_metadata()?;
        self.persist_history()?;
        Ok(result)
    }

    pub fn persist_metadata(&self) -> Result<()> {
        let summary = self.summary();
        let path = self
            .runtime_root
            .join("sessions")
            .join(&self.id)
            .join("session.json");
        atomic_json_write(&path, &summary)
    }

    pub fn persist_history(&self) -> Result<()> {
        let run = self
            .run
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        persist_run_history(&self.runtime_root, &self.id, &run)
    }
}

fn persist_run_history(runtime_root: &Path, session_id: &str, run: &RunResources) -> Result<()> {
    let history = run
        .history
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .snapshot();
    let path = runtime_root
        .join("sessions")
        .join(session_id)
        .join("runs")
        .join(format!("{}-history.json", run.run_id));
    atomic_json_write(&path, &history)
}

fn ensure_running(run: &RunResources) -> Result<()> {
    if run
        .process_state
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .is_running()
    {
        Ok(())
    } else {
        bail!("application process has exited")
    }
}

fn write_input(run: &RunResources, operation: &str, bytes: &[u8]) -> Result<()> {
    run.writer
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .write_all(bytes)
        .with_context(|| format!("write {operation} input to PTY"))?;
    run.writer
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .flush()
        .with_context(|| format!("flush {operation} input to PTY"))?;
    run.history
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .push_input(operation, bytes);
    Ok(())
}

fn validate_spec(spec: &LaunchSpec) -> Result<()> {
    let program = Path::new(&spec.program);
    if spec.program.trim().is_empty() {
        bail!("program path must not be empty")
    }
    if spec.cwd.trim().is_empty() || !Path::new(&spec.cwd).is_dir() {
        bail!("cwd must name an existing directory")
    }
    if spec.cols == 0 || spec.rows == 0 {
        bail!("terminal size must be greater than zero")
    }
    if spec.cols > 300 || spec.rows > 120 {
        bail!("unsupported terminal size: maximum is 300 columns by 120 rows")
    }
    for value in std::iter::once(spec.program.as_str()).chain(spec.args.iter().map(String::as_str))
    {
        if value.contains('\0') {
            bail!("program and arguments must not contain NUL bytes")
        }
    }
    if program.components().count() == 0 {
        bail!("program path is invalid")
    }
    validate_environment(&spec.env)
}

fn validate_environment(env: &std::collections::BTreeMap<String, String>) -> Result<()> {
    for (key, value) in env {
        if key.is_empty() || key.contains('=') || key.contains('\0') || value.contains('\0') {
            bail!("invalid environment entry for key {key:?}")
        }
    }
    Ok(())
}

pub(crate) fn create_private_dir(path: &Path) -> Result<()> {
    fs::create_dir_all(path).with_context(|| format!("create directory {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

pub(crate) fn atomic_json_write(path: &Path, value: &impl serde::Serialize) -> Result<()> {
    let parent = path.parent().context("metadata path has no parent")?;
    create_private_dir(parent)?;
    let temp_path = path.with_extension(format!("{}.tmp", Ulid::generate()));
    let bytes = serde_json::to_vec_pretty(value)?;
    {
        use std::io::Write as _;
        let mut file = fs::File::create(&temp_path)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&temp_path, fs::Permissions::from_mode(0o600))?;
    }
    fs::rename(&temp_path, path)?;
    Ok(())
}

fn signal_process_group(group: i32, signal: i32) {
    if group > 1 {
        // portable-pty starts the child in a fresh session; its leader PID is
        // also the process-group ID. Negative PID targets the complete group.
        unsafe {
            libc::kill(-group, signal);
        }
    }
}
