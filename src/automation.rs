//! Shared agent operations. Every transport calls the same typed service boundary.
use crate::{
    artifacts::{ArtifactSourceKind, ArtifactStore},
    assertions::FrameAssertion,
    frame_store::{CellMask, FrameStore},
    logs::{LogCursor, LogStore},
    predicates::{PredicateEvaluator, PredicateOutcome, PredicateSpec},
    protocol::{ActionContext, AgentAction, LaunchSpec, RpcRequest},
    retention::{
        CleanupPlan, ManagedEntry, OwnerKind, QuotaReservation, ReservationTarget, RetentionClass,
        RetentionRegistry,
    },
    scenario::{Scenario, ScenarioBackend, ScenarioControl, ScenarioManager},
    service::SessionService,
    session::Session,
};
use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

const RENDER_PROFILE: &str = "alacritty-cells-v1";

pub struct AutomationService {
    root: PathBuf,
    artifacts: ArtifactStore,
    frames: FrameStore,
    retention: Mutex<RetentionRegistry>,
    scenarios: ScenarioManager,
    registrations: Mutex<HashMap<String, String>>,
    waits: Mutex<HashMap<String, Arc<Mutex<WaitJob>>>>,
}
struct WaitJob {
    session_id: String,
    run_id: String,
    predicate: Value,
    evaluator: PredicateEvaluator,
    deadline: Instant,
    trace_stage: Option<String>,
    terminal: Option<Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionArgs {
    session_id: String,
}
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ProfileArgs {
    action: String,
    name: String,
    description: String,
    profile: Option<crate::profiles::Profile>,
}
impl Default for ProfileArgs {
    fn default() -> Self {
        Self {
            action: "list".into(),
            name: String::new(),
            description: String::new(),
            profile: None,
        }
    }
}
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct ArtifactArgs {
    session_id: String,
    artifact_id: String,
    offset: u64,
    length: usize,
    limit: usize,
}
impl Default for ArtifactArgs {
    fn default() -> Self {
        Self {
            session_id: String::new(),
            artifact_id: String::new(),
            offset: 0,
            length: 65536,
            limit: 100,
        }
    }
}
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct LogsArgs {
    session_id: String,
    source: String,
    path: String,
    cursor: Option<LogCursor>,
    max_bytes: usize,
}
impl Default for LogsArgs {
    fn default() -> Self {
        Self {
            session_id: String::new(),
            source: "app".into(),
            path: "dev.log".into(),
            cursor: None,
            max_bytes: 65536,
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SaveArgs {
    session_id: String,
    name: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AssertArgs {
    session_id: String,
    assertion: FrameAssertion,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DiffArgs {
    session_id: String,
    name: String,
    #[serde(default)]
    masks: Vec<CellMask>,
}
#[derive(Deserialize)]
#[serde(default, deny_unknown_fields)]
struct WaitArgs {
    session_id: String,
    predicate: Option<PredicateSpec>,
    wait_id: Option<String>,
    timeout_ms: u64,
    poll_ms: u64,
    trace_stage: Option<String>,
    cancel: bool,
}
impl Default for WaitArgs {
    fn default() -> Self {
        Self {
            session_id: String::new(),
            predicate: None,
            wait_id: None,
            timeout_ms: 30000,
            poll_ms: 200,
            trace_stage: None,
            cancel: false,
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BatchArgs {
    session_id: String,
    context: ActionContext,
    actions: Vec<AgentAction>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ActorArgs {
    session_id: String,
    actor_id: String,
    #[serde(default)]
    release: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ScenarioArgs {
    scenario: Scenario,
    #[serde(default)]
    workspace: Option<PathBuf>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JobArgs {
    job_id: String,
}
#[derive(Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
struct CleanupArgs {
    apply: bool,
    plan: Option<CleanupPlan>,
    include_developer_cache: bool,
    policy: Option<crate::retention::RetentionPolicy>,
    #[serde(default)]
    register: Vec<ManagedEntry>,
}

fn parse<T: serde::de::DeserializeOwned>(params: Value) -> Result<T> {
    serde_json::from_value(params).context("invalid_params: operation parameters failed validation")
}
fn encode<T: serde::Serialize>(value: T) -> Result<Value> {
    Ok(serde_json::to_value(value)?)
}

impl AutomationService {
    pub fn new(root: PathBuf) -> Result<Self> {
        let policy_path = root.join("retention-policy.json");
        let policy = if policy_path.exists() {
            serde_json::from_slice(&std::fs::read(&policy_path)?)?
        } else {
            crate::retention::RetentionPolicy::default()
        };
        let service = Self {
            artifacts: ArtifactStore::new(root.clone())?,
            frames: FrameStore::new(root.clone())?,
            retention: Mutex::new(RetentionRegistry::open_with_policy(&root, policy)?),
            scenarios: ScenarioManager::new(root.join("scenarios"))?,
            root,
            registrations: Mutex::new(HashMap::new()),
            waits: Mutex::new(HashMap::new()),
        };
        for report in service.scenarios.list() {
            service.register_scenario_report(&report)?;
        }
        Ok(service)
    }

    fn register_scenario_report(&self, report: &crate::scenario::ScenarioReport) -> Result<()> {
        let active = matches!(
            report.status,
            crate::scenario::ScenarioStatus::Queued
                | crate::scenario::ScenarioStatus::Running
                | crate::scenario::ScenarioStatus::PausedForControl
        );
        let (retention, pinned) = if active {
            (RetentionClass::Interactive, true)
        } else if report.status == crate::scenario::ScenarioStatus::Passed {
            (RetentionClass::Successful, false)
        } else {
            (RetentionClass::Failed, false)
        };
        let created_at_ms = report.finished_at_ms.unwrap_or(report.started_at_ms);
        let path = PathBuf::from("scenarios").join(format!("{}.json", report.scenario_id));
        let mut registry = self
            .retention
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some((entry_id, current_retention, current_pinned, current_created_at)) = registry
            .entries()
            .iter()
            .find(|entry| entry.relative_path == path)
            .map(|entry| {
                (
                    entry.id.clone(),
                    entry.retention,
                    entry.pinned,
                    entry.created_at_ms,
                )
            })
        {
            if current_retention != retention
                || current_pinned != pinned
                || current_created_at != created_at_ms
            {
                registry.set_retention_at(&entry_id, retention, pinned, created_at_ms)?;
            }
        } else {
            registry.register(ManagedEntry {
                id: ulid::Ulid::generate().to_string(),
                relative_path: path,
                kind: OwnerKind::Run,
                retention,
                created_at_ms,
                pinned,
                description: "durable scenario report".into(),
            })?;
        }
        Ok(())
    }

    pub fn register_session(&self, session: &Session, disposable: bool) -> Result<()> {
        let id = &session.id;
        let class = if disposable {
            RetentionClass::Successful
        } else {
            RetentionClass::Interactive
        };
        let mut retention = self
            .retention
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        retention.register(ManagedEntry::new(
            PathBuf::from("sessions").join(id),
            OwnerKind::Session,
            class,
            crate::engine::now_ms(),
            "session state and history",
        ))?;
        let evidence = self.root.join("artifacts").join(id);
        crate::session::create_private_dir(&evidence)?;
        retention.register(ManagedEntry::new(
            PathBuf::from("artifacts").join(id),
            OwnerKind::Artifact,
            class,
            crate::engine::now_ms(),
            "session screenshots",
        ))?;
        let workspace = self.root.join("workspaces").join(id);
        if workspace.is_dir() {
            retention.register(ManagedEntry::new(
                PathBuf::from("workspaces").join(id),
                OwnerKind::TestWorkspace,
                class,
                crate::engine::now_ms(),
                "disposable project snapshot",
            ))?;
        }
        Ok(())
    }

    pub(crate) fn reserve_session_allocation(
        &self,
        session_id: &str,
        reserved_bytes: u64,
        has_workspace: bool,
    ) -> Result<QuotaReservation> {
        let mut targets = vec![
            ReservationTarget::new(
                PathBuf::from("sessions").join(session_id),
                OwnerKind::Session,
                "session state and history",
            ),
            ReservationTarget::new(
                PathBuf::from("artifacts").join(session_id),
                OwnerKind::Artifact,
                "session screenshots",
            ),
        ];
        if has_workspace {
            targets.push(ReservationTarget::new(
                PathBuf::from("workspaces").join(session_id),
                OwnerKind::TestWorkspace,
                "disposable project snapshot",
            ));
        }
        self.retention
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .reserve(reserved_bytes, targets)
    }

    pub(crate) fn commit_session_allocation(
        &self,
        reservation_id: &str,
        session_id: &str,
        disposable: bool,
    ) -> Result<()> {
        let evidence = self.root.join("artifacts").join(session_id);
        crate::session::create_private_dir(&evidence)?;
        let retention = if disposable {
            RetentionClass::Successful
        } else {
            RetentionClass::Interactive
        };
        self.retention
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .commit_reservation(reservation_id, retention, crate::engine::now_ms())?;
        Ok(())
    }

    pub(crate) fn fail_session_allocation(&self, reservation_id: &str) -> Result<()> {
        self.retention
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .fail_reservation(reservation_id, crate::engine::now_ms())?;
        Ok(())
    }

    pub fn register_failed_launch(&self, id: &str) -> Result<()> {
        let mut registry = self
            .retention
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for (prefix, kind) in [
            ("sessions", OwnerKind::Session),
            ("workspaces", OwnerKind::TestWorkspace),
        ] {
            let relative = PathBuf::from(prefix).join(id);
            if self.root.join(&relative).exists() {
                registry.register(ManagedEntry::new(
                    relative,
                    kind,
                    RetentionClass::Failed,
                    crate::engine::now_ms(),
                    "rejected launch evidence (imported state removed)",
                ))?;
            }
        }
        Ok(())
    }

    pub fn check_capacity(
        &self,
        service: &SessionService,
        bytes: u64,
        entries: usize,
    ) -> Result<()> {
        self.retention
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .ensure_budget(
                bytes,
                entries,
                crate::engine::now_ms(),
                &service.protected_paths(),
            )?;
        Ok(())
    }

    pub fn protected_jobs(&self) -> Vec<PathBuf> {
        let mut paths = Vec::new();
        for report in self.scenarios.list() {
            if matches!(
                report.status,
                crate::scenario::ScenarioStatus::Queued
                    | crate::scenario::ScenarioStatus::Running
                    | crate::scenario::ScenarioStatus::PausedForControl
            ) {
                paths.push(
                    self.root
                        .join("scenarios")
                        .join(format!("{}.json", report.scenario_id)),
                );
                for step in report.steps {
                    if let Some(result) = step.result
                        && let Some(id) = result["session_id"].as_str()
                    {
                        paths.push(self.root.join("sessions").join(id));
                        paths.push(self.root.join("artifacts").join(id));
                        paths.push(self.root.join("workspaces").join(id));
                    }
                }
            }
        }
        paths
    }

    pub(crate) fn register_capture(
        &self,
        capture: &crate::protocol::ScreenshotArtifact,
    ) -> Result<String> {
        let path = Path::new(&capture.path);
        if !path.is_absolute() || path.file_name().is_none() {
            bail!("invalid_capture_path: screenshot path must be absolute")
        }
        let key = format!("{}:{}", capture.session_id, path.display());
        let mut cached = self
            .registrations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(id) = cached.get(&key) {
            return Ok(id.clone());
        }
        let root_id = self.artifacts.register_session_evidence(
            &capture.session_id,
            path.parent().context("capture has no parent")?,
        )?;
        let file_name = path.file_name().context("capture has no filename")?;
        let metadata = self.artifacts.register_evidence_file_with_id(
            &capture.session_id,
            &root_id,
            Path::new(file_name),
            &capture.artifact_id,
            ArtifactSourceKind::Screenshot,
            "image/png",
            Some(capture.run_id.clone()),
            Some("session retention".into()),
        )?;
        cached.insert(key, metadata.artifact_id.clone());
        Ok(metadata.artifact_id)
    }

    fn register_file(
        &self,
        session_id: &str,
        path: &Path,
        kind: ArtifactSourceKind,
        run_id: Option<String>,
    ) -> Result<String> {
        let key = format!("{}:{}", session_id, path.display());
        let mut cached = self
            .registrations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(id) = cached.get(&key) {
            return Ok(id.clone());
        }
        let parent = path.parent().context("artifact file has no parent")?;
        let file = path.file_name().context("artifact file has no filename")?;
        let root_id = if kind == ArtifactSourceKind::ApplicationLog {
            self.artifacts
                .register_log_root(session_id, "application", parent)?
        } else {
            self.artifacts
                .register_session_evidence(session_id, parent)?
        };
        let content_type = if path.extension().is_some_and(|e| e == "png") {
            "image/png"
        } else if path.extension().is_some_and(|e| e == "json") {
            "application/json"
        } else {
            "text/plain"
        };
        let metadata = self.artifacts.register_file(
            session_id,
            &root_id,
            Path::new(file),
            kind,
            content_type,
            run_id,
            Some("session retention".into()),
        )?;
        cached.insert(key, metadata.artifact_id.clone());
        Ok(metadata.artifact_id)
    }

    fn discover_artifacts(&self, service: &SessionService, session_id: &str) -> Result<()> {
        let (_, _) = service.handle(RpcRequest::Status {
            session_id: session_id.into(),
        })?;
        let _lifecycle = match service.active_session(session_id) {
            Ok(session) => Some(session.retain_evidence()?),
            Err(_) => None,
        };
        let directory = self.root.join("artifacts").join(session_id);
        if directory.is_dir() {
            for run in std::fs::read_dir(&directory)?.take(100) {
                let run = run?;
                if !run.file_type()?.is_dir() {
                    continue;
                }
                for file in std::fs::read_dir(run.path())?.take(500) {
                    let file = file?;
                    if file.file_type()?.is_file() {
                        let kind = match file.path().extension().and_then(|e| e.to_str()) {
                            Some("png") => {
                                let path = file.path();
                                let name = path
                                    .file_name()
                                    .and_then(|n| n.to_str())
                                    .context("invalid screenshot filename")?;
                                let (capture_id, frame_id) = name
                                    .split_once('-')
                                    .context("screenshot filename lacks capture identity")?;
                                let capture = crate::protocol::ScreenshotArtifact {
                                    artifact_id: capture_id.to_owned(),
                                    session_id: session_id.to_owned(),
                                    run_id: run.file_name().to_string_lossy().into_owned(),
                                    frame_id: frame_id.trim_end_matches(".png").to_owned(),
                                    path: path.to_string_lossy().into_owned(),
                                    sha256: String::new(),
                                    cols: 0,
                                    rows: 0,
                                    captured_at_ms: 0,
                                };
                                self.register_capture(&capture)?;
                                continue;
                            }
                            Some("json") => ArtifactSourceKind::Frame,
                            _ => continue,
                        };
                        self.register_file(
                            session_id,
                            &file.path(),
                            kind,
                            Some(run.file_name().to_string_lossy().into_owned()),
                        )?;
                    }
                }
            }
        }
        Ok(())
    }

    fn logs(&self, service: &SessionService, args: LogsArgs) -> Result<Value> {
        let summary = service
            .handle(RpcRequest::Status {
                session_id: args.session_id.clone(),
            })?
            .0;
        let state = self
            .root
            .join("sessions")
            .join(&args.session_id)
            .join("state");
        let diagnostics = service
            .active_session(&args.session_id)
            .ok()
            .map(|s| s.capabilities()["launch"].clone())
            .or_else(|| {
                std::fs::read(
                    self.root
                        .join("sessions")
                        .join(&args.session_id)
                        .join("launch-diagnostics.json"),
                )
                .ok()
                .and_then(|bytes| serde_json::from_slice::<Value>(&bytes).ok())
            });
        let root = diagnostics
            .as_ref()
            .and_then(|v| v["app_log_root"].as_str())
            .unwrap_or("data/silicogen/log");
        crate::artifacts::validate_relative(Path::new(root))?;
        let file = match args.source.as_str() {
            "app" => {
                crate::artifacts::validate_relative(Path::new(&args.path))?;
                state.join(root).join(&args.path)
            }
            "startup" => state.join(root).join("startup-trace.log"),
            "daemon" => self.root.join("daemon.log"),
            _ => bail!("invalid_params: log source must be app, startup, or daemon"),
        };
        if !file.exists() {
            return Ok(
                json!({"session_id":args.session_id,"run_id":summary["run_id"],"pending":true,"source":args.source,"path":file,"reason":"log_not_created"}),
            );
        }
        let id = if args.source == "daemon" {
            let key = format!("{}:daemon", args.session_id);
            let mut cached = self
                .registrations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(id) = cached.get(&key) {
                id.clone()
            } else {
                let id = self
                    .artifacts
                    .register_daemon_log(&args.session_id)?
                    .artifact_id;
                cached.insert(key, id.clone());
                id
            }
        } else {
            self.register_file(
                &args.session_id,
                &file,
                ArtifactSourceKind::ApplicationLog,
                summary["run_id"].as_str().map(str::to_owned),
            )?
        };
        let expired = args.cursor.as_ref().is_some_and(|c| c.artifact_id != id);
        let poll = LogStore::new(&self.artifacts).poll(
            &args.session_id,
            &id,
            if expired { None } else { args.cursor.as_ref() },
            args.max_bytes,
        )?;
        let mut result = json!({"session_id":args.session_id,"run_id":summary["run_id"],"artifact":poll.artifact,"cursor":poll.cursor,"gap":poll.gap,"text":String::from_utf8_lossy(&poll.bytes),"eof":poll.eof,"received_at_ms":crate::engine::now_ms(),"correlation":"observed app log; not a render completion boundary"});
        if expired {
            result["gap"] = json!("cursor_expired");
        }
        Ok(result)
    }

    fn wait(&self, service: &SessionService, args: WaitArgs) -> Result<Value> {
        if args.poll_ms > 5000 || args.timeout_ms == 0 || args.timeout_ms > 3600000 {
            bail!("invalid_params: wait poll <=5000ms, overall timeout 1..3600000ms")
        }
        let mut jobs = self
            .waits
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let existing = args.wait_id.as_ref().and_then(|id| jobs.get(id).cloned());
        let (id, job) = if let Some(job) = existing {
            (args.wait_id.clone().unwrap(), job)
        } else {
            let session = service.active_session(&args.session_id)?;
            let predicate = args
                .predicate
                .clone()
                .context("wait_not_found: a new wait requires a predicate")?;
            let id = args
                .wait_id
                .clone()
                .unwrap_or_else(|| ulid::Ulid::generate().to_string());
            if id.is_empty()
                || id.len() > 128
                || !id
                    .bytes()
                    .all(|c| c.is_ascii_alphanumeric() || b"-_.:".contains(&c))
            {
                bail!("invalid_params: invalid wait id")
            }
            let job = Arc::new(Mutex::new(WaitJob {
                session_id: args.session_id.clone(),
                run_id: session.summary().run_id,
                predicate: serde_json::to_value(&predicate)?,
                evaluator: PredicateEvaluator::new(predicate)?,
                deadline: Instant::now() + Duration::from_millis(args.timeout_ms),
                trace_stage: args.trace_stage.clone(),
                terminal: None,
            }));
            if jobs.len() >= 256 {
                jobs.retain(|_, j| {
                    j.lock()
                        .is_ok_and(|j| j.terminal.is_none() && j.deadline > Instant::now())
                });
                if jobs.len() >= 256 {
                    bail!("wait_limit: too many active waits")
                }
            }
            jobs.insert(id.clone(), Arc::clone(&job));
            (id, job)
        };
        drop(jobs);
        let mut job = job
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !args.session_id.is_empty() && args.session_id != job.session_id {
            bail!("wait_session_mismatch: handle belongs to another session")
        }
        if args.predicate.as_ref().is_some_and(|predicate| {
            serde_json::to_value(predicate).ok().as_ref() != Some(&job.predicate)
        }) || args
            .trace_stage
            .as_ref()
            .is_some_and(|stage| job.trace_stage.as_ref() != Some(stage))
        {
            bail!("wait_conflict: wait id was reused with different conditions")
        }
        if let Some(result) = &job.terminal {
            return Ok(result.clone());
        }
        if args.cancel {
            let result = json!({"wait_id":id,"status":"cancelled","matched":false});
            job.terminal = Some(result.clone());
            return Ok(result);
        }
        let polling_deadline = Instant::now() + Duration::from_millis(args.poll_ms);
        loop {
            let session = service.active_session(&job.session_id)?;
            let frame = session.observe();
            if frame.run_id != job.run_id {
                bail!("stale_run: wait targets previous application run")
            }
            let evaluation = job.evaluator.observe(&frame, Instant::now());
            let trace_ready = if let Some(stage) = &job.trace_stage {
                let log = self.logs(
                    service,
                    LogsArgs {
                        session_id: job.session_id.clone(),
                        source: "startup".into(),
                        max_bytes: 1024 * 1024,
                        ..Default::default()
                    },
                )?;
                let started = session.summary().started_at_ms;
                log["text"].as_str().is_some_and(|text| {
                    text.lines().any(|line| {
                        let fields = line.split_whitespace().collect::<Vec<_>>();
                        fields.len() >= 4
                            && fields[1]
                                .parse::<u128>()
                                .is_ok_and(|timestamp| timestamp >= started)
                            && fields[3] == stage
                    })
                })
            } else {
                true
            };
            let done = (evaluation.matched && trace_ready)
                || matches!(
                    evaluation.outcome,
                    PredicateOutcome::AppCrash | PredicateOutcome::Exited
                );
            let timed_out = Instant::now() >= job.deadline;
            let mut result = encode(&evaluation)?;
            result["matched"] = json!(evaluation.matched && trace_ready);
            result["wait_id"] = json!(id);
            result["status"] = json!(if done {
                "completed"
            } else if timed_out {
                "timed_out"
            } else {
                "pending"
            });
            result["timed_out"] = json!(timed_out && !done);
            result["trace_ready"] = json!(trace_ready);
            result["snapshot"] = encode(frame)?;
            if done || timed_out {
                job.terminal = Some(result.clone());
                return Ok(result);
            }
            if Instant::now() >= polling_deadline {
                return Ok(result);
            }
            std::thread::sleep(Duration::from_millis(25));
        }
    }

    pub fn handle(
        &self,
        service: &SessionService,
        operation: &str,
        params: Value,
    ) -> Result<Value> {
        match operation.trim_start_matches("terminal_") {
            "capabilities" => {
                let args: SessionArgs = parse(params)?;
                Ok(service.active_session(&args.session_id)?.capabilities())
            }
            "profiles" => {
                let args: ProfileArgs = parse(params)?;
                match args.action.as_str() {
                    "list" => encode(crate::profiles::list_profiles(&self.root)?),
                    "create" => encode(
                        crate::profiles::create_profile(&self.root, &args.name, &args.description)
                            .map(|p| crate::profiles::profile_metadata(&p))?,
                    ),
                    "store" => encode(crate::profiles::store_profile(
                        &self.root,
                        &args
                            .profile
                            .context("invalid_params: profile is required")?,
                    )?),
                    "template" => encode(crate::profiles::silicogen_test_profile_template()),
                    _ => bail!("invalid_params: unknown profile action"),
                }
            }
            "artifacts" => {
                let args: ArtifactArgs = parse(params)?;
                self.discover_artifacts(service, &args.session_id)?;
                encode(
                    self.artifacts
                        .list(&args.session_id, args.offset as usize, args.limit)?,
                )
            }
            "read_artifact" => {
                let args: ArtifactArgs = parse(params)?;
                let _lifecycle = match service.active_session(&args.session_id) {
                    Ok(session) => Some(session.retain_evidence()?),
                    Err(_) => None,
                };
                let data = self.artifacts.read(
                    &args.session_id,
                    &args.artifact_id,
                    args.offset,
                    args.length,
                )?;
                use base64::Engine;
                Ok(
                    json!({"artifact":data.artifact,"offset":data.offset,"next_offset":data.next_offset,"eof":data.eof,"encoding":"base64","data":base64::engine::general_purpose::STANDARD.encode(&data.bytes),"text":std::str::from_utf8(&data.bytes).ok()}),
                )
            }
            "logs" => {
                let args: LogsArgs = parse(params)?;
                let _lifecycle = match service.active_session(&args.session_id) {
                    Ok(session) => Some(session.retain_evidence()?),
                    Err(_) => None,
                };
                self.logs(service, args)
            }
            "save_frame" => {
                let args: SaveArgs = parse(params)?;
                let session = service.active_session(&args.session_id)?;
                let _lifecycle = session.retain_evidence()?;
                let frame = session.observe();
                let expected_bytes = serde_json::to_vec_pretty(
                    &json!({"baseline":{"frame":frame},"cells":frame.cells}),
                )?
                .len() as u64
                    + 4096;
                self.check_capacity(service, expected_bytes.saturating_mul(2), 1)?;
                let _capture_budget = session.evidence_budget_guard(expected_bytes)?;
                let saved = self.frames.save_named(&args.name, &frame, RENDER_PROFILE)?;
                let path = self
                    .frames
                    .directory()
                    .join(format!("{}--{}.json", args.session_id, args.name));
                let evidence_path = self
                    .root
                    .join("artifacts")
                    .join(&args.session_id)
                    .join(&frame.run_id)
                    .join(format!("{}.json", saved.baseline_id));
                crate::session::create_private_dir(evidence_path.parent().unwrap())?;
                crate::session::atomic_json_write(
                    &evidence_path,
                    &json!({"baseline":saved,"cells":frame.cells}),
                )?;
                let artifact = self.register_file(
                    &args.session_id,
                    &evidence_path,
                    ArtifactSourceKind::Frame,
                    Some(frame.run_id.clone()),
                )?;
                self.retention
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .register(ManagedEntry::new(
                        path.strip_prefix(&self.root)?.to_path_buf(),
                        OwnerKind::Artifact,
                        RetentionClass::Interactive,
                        crate::engine::now_ms(),
                        "immutable frame baseline",
                    ))?;
                let mut result = encode(saved)?;
                result["artifact_id"] = json!(artifact);
                Ok(result)
            }
            "assert" => {
                let args: AssertArgs = parse(params)?;
                let session = service.active_session(&args.session_id)?;
                let _lifecycle = session.retain_evidence()?;
                let frame = session.observe();
                let assertion = crate::assertions::evaluate(&frame, &args.assertion)?;
                let mut result = encode(&assertion)?;
                if !assertion.passed {
                    let evidence = session.screenshot_snapshot(&frame)?;
                    let artifact = self.register_capture(&evidence)?;
                    let mut evidence_value = encode(evidence)?;
                    evidence_value["artifact_id"] = json!(artifact);
                    result["evidence"] = evidence_value;
                }
                Ok(result)
            }
            "diff" => {
                let args: DiffArgs = parse(params)?;
                let saved = self.frames.load_named(&args.session_id, &args.name)?;
                let frame = service.active_session(&args.session_id)?.observe();
                let diff =
                    crate::frame_store::diff_frames(&saved, &frame, RENDER_PROFILE, &args.masks)?;
                let passed = diff.changed_cell_count == 0;
                let mut value = encode(diff)?;
                value["passed"] = json!(passed);
                Ok(value)
            }
            "wait" => self.wait(service, parse(params)?),
            "actor" => {
                let args: ActorArgs = parse(params)?;
                service
                    .active_session(&args.session_id)?
                    .claim_actor(&args.actor_id, args.release)
            }
            "restart" => {
                let args: ActorArgs = parse(params)?;
                encode(
                    service
                        .active_session(&args.session_id)?
                        .agent_restart(&args.actor_id)?,
                )
            }
            "close" => {
                let args: ActorArgs = parse(params)?;
                let session = service.active_session(&args.session_id)?;
                session.claim_actor(&args.actor_id, true)?;
                Ok(service
                    .handle(RpcRequest::Close {
                        session_id: args.session_id,
                    })?
                    .0)
            }
            "batch" => {
                let args: BatchArgs = parse(params)?;
                if args.actions.is_empty() || args.actions.len() > 64 {
                    bail!("invalid_params: batch requires 1..64 actions")
                };
                let session = service.active_session(&args.session_id)?;
                if session.summary().run_id != args.context.expected_run_id {
                    bail!("stale_run: batch targets a previous application run")
                }
                session.claim_actor(&args.context.actor_id, false)?;
                let mut results = Vec::new();
                for (i, action) in args.actions.iter().enumerate() {
                    let mut context = args.context.clone();
                    context.action_id = format!("{}:{i}", context.action_id);
                    results.push(session.agent_action(&context, action)?);
                }
                Ok(json!({"accepted":true,"results":results}))
            }
            "scenario_run" => {
                let args: ScenarioArgs = parse(params)?;
                let bound = args
                    .scenario
                    .steps
                    .iter()
                    .filter_map(|step| step.params["session_id"].as_str())
                    .filter(|id| !id.contains("${"))
                    .map(str::to_owned)
                    .collect();
                let backend = Arc::new(ServiceBackend {
                    service: service.clone(),
                    sessions: Mutex::new(Vec::new()),
                    referenced: Mutex::new(bound),
                    actor_id: format!("scenario-{}", ulid::Ulid::generate()),
                });
                let agent = Arc::downgrade(&service.agent);
                let on_created: crate::scenario::ScenarioLifecycleHook = Arc::new(move |report| {
                    let agent = agent
                        .upgrade()
                        .context("automation service stopped before scenario registration")?;
                    agent.register_scenario_report(report)
                });
                let on_finished = Arc::clone(&on_created);
                let job_id = self.scenarios.start_with_hooks(
                    args.scenario,
                    backend,
                    args.workspace,
                    Some(on_created),
                    Some(on_finished),
                )?;
                Ok(json!({"job_id":job_id,"status":"queued"}))
            }
            "scenario_status" => {
                let args: JobArgs = parse(params)?;
                let report = self.scenarios.status(&args.job_id)?;
                self.register_scenario_report(&report)?;
                encode(report)
            }
            "scenario_cancel" => {
                let args: JobArgs = parse(params)?;
                encode(self.scenarios.cancel(&args.job_id)?)
            }
            "cleanup" => {
                let args: CleanupArgs = parse(params)?;
                let plan = args.plan;
                let mut reclaiming = HashMap::new();
                if args.apply {
                    let plan = plan
                        .as_ref()
                        .context("invalid_params: applying cleanup requires its preview plan")?;
                    let mut session_ids = std::collections::BTreeSet::new();
                    for candidate in &plan.candidates {
                        let Ok(relative) = candidate.path.strip_prefix(&self.root) else {
                            continue;
                        };
                        let mut components = relative.components();
                        let owner = components.next().and_then(|c| c.as_os_str().to_str());
                        let id = components.next().and_then(|c| c.as_os_str().to_str());
                        if matches!(owner, Some("sessions" | "artifacts" | "workspaces"))
                            && let Some(id) = id
                        {
                            session_ids.insert(id.to_owned());
                        }
                    }
                    for id in session_ids {
                        if let Some(lease) = service.begin_reclamation(&id)? {
                            reclaiming.insert(id, lease);
                        }
                    }
                }
                let active = service.protected_paths();
                let mut registry = self
                    .retention
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(policy) = args.policy {
                    crate::session::atomic_json_write(
                        &self.root.join("retention-policy.json"),
                        &policy,
                    )?;
                    *registry = RetentionRegistry::open_with_policy(&self.root, policy)?;
                }
                for entry in args.register {
                    registry.register(entry)?;
                }
                if args.apply {
                    let applied = registry.apply(
                        &plan.context(
                            "invalid_params: applying cleanup requires its preview plan",
                        )?,
                        &active,
                    )?;
                    drop(registry);
                    for (id, lease) in reclaiming {
                        let session_path = PathBuf::from("sessions").join(&id);
                        let removed_session = applied.removed.iter().any(|entry| {
                            entry
                                .path
                                .strip_prefix(&self.root)
                                .is_ok_and(|relative| relative == session_path)
                        });
                        service.finish_reclamation(&id, lease, removed_session);
                    }
                    encode(applied)
                } else {
                    let mut result = encode(registry.preview(
                        crate::engine::now_ms(),
                        &active,
                        args.include_developer_cache,
                    )?)?;
                    result["unowned_legacy"] = encode(registry.inventory_unowned_legacy()?)?;
                    Ok(result)
                }
            }
            "relaunch" => {
                let args: SessionArgs = parse(params)?;
                service.handle(RpcRequest::Status {
                    session_id: args.session_id.clone(),
                })?;
                let path = self
                    .root
                    .join("sessions")
                    .join(&args.session_id)
                    .join("launch.json");
                let manifest: Value =
                    serde_json::from_slice(&std::fs::read(path).context(
                        "launch_manifest_missing: legacy session cannot be reconstructed",
                    )?)?;
                let spec: LaunchSpec = serde_json::from_value(manifest["spec"].clone())?;
                let missing = manifest["required_env_names"]
                    .as_array()
                    .is_some_and(|names| !names.is_empty())
                    || manifest["shell_script_required"] == true
                    || manifest["argv_values_required"] == true;
                if missing {
                    bail!(
                        "credentials_required: launch used inline environment values or shell text; provide a new launch or profile"
                    )
                };
                let options = serde_json::from_value(manifest["options"].clone())?;
                let mut result = service
                    .handle(RpcRequest::LaunchConfigured { spec, options })?
                    .0;
                result["previous_session_id"] = json!(args.session_id);
                Ok(result)
            }
            _ => bail!("unsupported_operation: unknown agent operation {operation}"),
        }
    }
}

struct ServiceBackend {
    service: SessionService,
    sessions: Mutex<Vec<String>>,
    referenced: Mutex<Vec<String>>,
    actor_id: String,
}
impl ScenarioBackend for ServiceBackend {
    fn execute(&self, operation: &str, mut params: Value) -> Result<Value> {
        let operation = operation.trim_start_matches("terminal_");
        let id = params
            .get("session_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        if let Some(id) = &id {
            let mut referenced = self
                .referenced
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if !referenced.contains(id) {
                referenced.push(id.clone());
            }
        }
        match operation {
            "launch" => {
                let spec: LaunchSpec = serde_json::from_value(
                    params
                        .get("spec")
                        .cloned()
                        .unwrap_or_else(|| params.clone()),
                )?;
                let options = serde_json::from_value(
                    params
                        .get("options")
                        .cloned()
                        .unwrap_or_else(|| json!({"disposable":true})),
                )?;
                let result = self
                    .service
                    .handle(RpcRequest::LaunchConfigured { spec, options })?
                    .0;
                self.sessions
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(
                        result["session_id"]
                            .as_str()
                            .context("launch response lacks session id")?
                            .to_string(),
                    );
                Ok(result)
            }
            "observe" | "screenshot" | "status" => {
                let id = id.context("invalid_params: session_id required")?;
                let request = match operation {
                    "observe" => RpcRequest::Observe { session_id: id },
                    "screenshot" => RpcRequest::Screenshot { session_id: id },
                    _ => RpcRequest::Status { session_id: id },
                };
                Ok(self.service.handle(request)?.0)
            }
            "key" | "type" | "paste" | "mouse" | "resize" => {
                let pre = params.get("_preconditions").cloned().unwrap_or(json!({}));
                let pre: crate::scenario::ScenarioPreconditions = serde_json::from_value(pre)
                    .context("invalid_preconditions: run/viewport values must use their declared string/integer types")?;
                let id = id.context("invalid_params: session_id required")?;
                params
                    .as_object_mut()
                    .context("invalid_params: step params must be object")?
                    .remove("session_id");
                params.as_object_mut().unwrap().remove("_preconditions");
                params["kind"] = json!(operation);
                let action: AgentAction = serde_json::from_value(params)?;
                let session = self.service.active_session(&id)?;
                session.claim_actor(&self.actor_id, false)?;
                let capabilities = session.capabilities();
                let context = ActionContext {
                    actor_id: self.actor_id.clone(),
                    action_id: ulid::Ulid::generate().to_string(),
                    expected_run_id: pre
                        .expected_run_id
                        .unwrap_or(capabilities["session"]["run_id"].as_str().unwrap().into()),
                    expected_viewport_revision: Some(
                        pre.expected_viewport_revision
                            .unwrap_or(capabilities["viewport_revision"].as_u64().unwrap()),
                    ),
                    controller_generation: None,
                };
                session.agent_action(&context, &action)
            }
            "restart" => {
                let session = self
                    .service
                    .active_session(&id.context("session_id required")?)?;
                encode(session.agent_restart(&self.actor_id)?)
            }
            "close" => {
                let id = id.context("session_id required")?;
                let session = self.service.active_session(&id)?;
                session.claim_actor(&self.actor_id, true)?;
                Ok(self.service.handle(RpcRequest::Close { session_id: id })?.0)
            }
            _ => {
                if operation == "wait" {
                    params["poll_ms"] = json!(100);
                }
                self.service.agent.handle(&self.service, operation, params)
            }
        }
    }
    fn control_state(&self) -> Result<ScenarioControl> {
        let mut ids = self
            .sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        ids.extend(
            self.referenced
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone(),
        );
        for id in &ids {
            if let Ok(session) = self.service.active_session(id)
                && serde_json::to_value(session.control_status()?)?["owner"] == "browser"
            {
                return Ok(ScenarioControl::BrowserHoldsControl);
            }
        }
        Ok(ScenarioControl::Available)
    }
    fn close_owned_sessions(&self) -> Result<()> {
        for id in self
            .sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            if let Ok(session) = self.service.active_session(id) {
                session.close_admin()?;
            }
        }
        Ok(())
    }
}
