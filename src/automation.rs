//! Shared agent operations. Every transport calls the same typed service boundary.
use crate::{
    app_scroll::{
        AppScrollBackend, ScrollDirection, ScrollObservation, ScrollRequest, VerifiedScrollBinding,
    },
    artifacts::{ArtifactSourceKind, ArtifactStore},
    capture_jobs::{ArmedCapture, CaptureBackend, CaptureCheckpoint, CaptureFrameRef},
    frame_store::FrameStore,
    locators::{ObservationIdentity, locate_text},
    logs::LogStore,
    predicates::{PredicateEvaluator, PredicateOutcome},
    protocol::{ActionContext, AgentAction, LaunchSpec, RpcRequest},
    retention::{
        ManagedEntry, OwnerKind, QuotaReservation, ReservationTarget, RetentionClass,
        RetentionRegistry,
    },
    scenario::{ScenarioBackend, ScenarioControl, ScenarioManager},
    sequences::{
        BarrierCondition, BarrierResult, DelayResult, DispatchResult, SequenceActionIdentity,
        SequenceAuthorityContext, SequenceBackend, SequenceCancellation, SequenceIdentity,
        SequenceManager, SequenceStatus, SequenceViewport,
    },
    service::{SessionService, WaitStep, WaitStop, execute_session_wait},
    session::Session,
    wait_engine::WaitEvaluator,
};
use anyhow::{Context, Result, bail, ensure};
use serde_json::{Value, json};
use sha2::Digest;
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::atomic::{AtomicBool, Ordering},
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

const RENDER_PROFILE: &str = "alacritty-cells-v1";
const MAX_FRESH_WAIT_JOBS: usize = 32;
const MAX_FRESH_WAIT_BASELINE_BYTES: usize = 64 * 1024 * 1024;
const MAX_FRESH_WAIT_TOMBSTONES: usize = 256;
const FRESH_WAIT_TOMBSTONE_TTL: Duration = Duration::from_secs(60);
const MAX_SEQUENCE_BASELINES_BYTES: usize = 64 * 1024 * 1024;
const MAX_SEQUENCE_BASELINE_BYTES: usize = 8 * 1024 * 1024;

type CaptureManagers =
    HashMap<String, Arc<Mutex<crate::capture_jobs::CaptureManager<RuntimeCaptureBackend>>>>;

fn write_private_atomic(path: &Path, bytes: &[u8]) -> Result<()> {
    use std::io::Write as _;
    let parent = path.parent().context("evidence file has no parent")?;
    crate::session::create_private_dir(parent)?;
    let temporary = parent.join(format!(".crop-{}.tmp", ulid::Ulid::generate()));
    let mut options = std::fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt as _;
        options.mode(0o600);
    }
    let result = (|| -> Result<()> {
        let mut file = options.open(&temporary)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        std::fs::rename(&temporary, path)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

fn read_registered_artifact(
    artifacts: &ArtifactStore,
    session_id: &str,
    artifact_id: &str,
    max_bytes: usize,
) -> Result<Vec<u8>> {
    let metadata = artifacts.metadata(session_id, artifact_id)?;
    ensure!(
        metadata.size_bytes <= max_bytes as u64,
        "crop_artifact_invalid: registered artifact exceeds its byte limit"
    );
    let mut bytes = Vec::with_capacity(metadata.size_bytes as usize);
    let mut offset = 0u64;
    loop {
        let read = artifacts.read(
            session_id,
            artifact_id,
            offset,
            crate::artifacts::MAX_ARTIFACT_READ.min(max_bytes),
        )?;
        if read.bytes.is_empty() {
            ensure!(
                read.eof,
                "crop_artifact_incomplete: artifact read made no progress"
            );
            break;
        }
        offset = read.next_offset;
        bytes.extend_from_slice(&read.bytes);
        ensure!(
            bytes.len() <= max_bytes,
            "crop_artifact_invalid: registered artifact exceeds its byte limit"
        );
        if read.eof {
            break;
        }
    }
    ensure!(
        bytes.len() as u64 == metadata.size_bytes,
        "crop_artifact_incomplete: registered artifact size changed during read"
    );
    Ok(bytes)
}

struct RecordingOwner;
impl crate::manual_recording::RecordingOwner for RecordingOwner {
    type Lease = ();
    fn start(&mut self, _: &crate::manual_recording::RecordingOptions) -> Result<Self::Lease> {
        Ok(())
    }
}

struct SessionRecordingRedactor(crate::finalization::Redactor);
impl crate::manual_recording::RecordingRedactor for SessionRecordingRedactor {
    fn redact(&self, value: &str) -> crate::manual_recording::RedactedText {
        let safe = self.0.text(value);
        if safe == value {
            crate::manual_recording::RedactedText::Safe(safe)
        } else {
            crate::manual_recording::RedactedText::ExternalReferenceRequired
        }
    }
}

struct ActiveManualRecording {
    session_id: String,
    run_id: String,
    recording_id: String,
    path: PathBuf,
    recorder: crate::manual_recording::ManualRecorder<RecordingOwner, SessionRecordingRedactor>,
    _session_lease: crate::session::EvidenceLease,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedManualRecording {
    version: u16,
    session_id: String,
    run_id: String,
    trace: crate::manual_recording::RecordingTrace,
}

#[derive(serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct PersistedCropEvidence {
    version: u16,
    artifact_id: String,
    metadata_artifact_id: String,
    metadata: crate::crop_evidence::CropMetadata,
}

#[path = "finalization_service.rs"]
mod finalization_service;

pub struct AutomationService {
    root: PathBuf,
    artifacts: Arc<ArtifactStore>,
    frames: FrameStore,
    retention: Arc<Mutex<RetentionRegistry>>,
    scenarios: ScenarioManager,
    registrations: Mutex<HashMap<String, String>>,
    waits: Mutex<HashMap<String, Arc<Mutex<WaitJob>>>>,
    wait_cancellations: Mutex<HashMap<String, Arc<AtomicBool>>>,
    fresh_waits: Mutex<HashMap<String, Arc<Mutex<FreshWaitJob>>>>,
    fresh_wait_cancellations: Mutex<HashMap<String, Arc<AtomicBool>>>,
    sequences: Arc<Mutex<SequenceManager>>,
    sequence_cancellations: Arc<Mutex<HashMap<String, SequenceCancellation>>>,
    sequence_baselines: Arc<Mutex<HashMap<String, SequenceBaselineEntry>>>,
    captures: Arc<Mutex<CaptureManagers>>,
    capture_quota: Arc<Mutex<CaptureQuota>>,
    diagnostics: crate::diagnostics::DiagnosticsManager,
    finalizations: Mutex<()>,
    app_adapters: Mutex<
        HashMap<String, Arc<Mutex<crate::app_adapter::SilicogenAdapter<RuntimeBindingVerifier>>>>,
    >,
    recordings: Mutex<HashMap<String, ActiveManualRecording>>,
}

#[derive(Default)]
struct CaptureQuota {
    jobs: usize,
    bytes: u64,
    frames: u64,
}

struct CaptureJobLease {
    _session: crate::session::EvidenceLease,
    _quota: CaptureQuotaLease,
}

struct CaptureQuotaLease {
    quota: Arc<Mutex<CaptureQuota>>,
    bytes: u64,
    frames: u64,
}

impl Drop for CaptureQuotaLease {
    fn drop(&mut self) {
        let mut quota = self
            .quota
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        quota.jobs = quota.jobs.saturating_sub(1);
        quota.bytes = quota.bytes.saturating_sub(self.bytes);
        quota.frames = quota.frames.saturating_sub(self.frames);
    }
}

fn reserve_capture_quota(
    quota: Arc<Mutex<CaptureQuota>>,
    bytes: u64,
    frames: u64,
) -> Result<CaptureQuotaLease> {
    {
        let mut current = quota
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        ensure!(
            current.jobs < crate::capture_jobs::MAX_CAPTURE_JOBS,
            "capture_quota_exhausted"
        );
        ensure!(
            current.bytes.saturating_add(bytes) <= 128 * 1024 * 1024,
            "capture_quota_exhausted"
        );
        ensure!(
            current.frames.saturating_add(frames) <= 512,
            "capture_quota_exhausted"
        );
        current.jobs += 1;
        current.bytes += bytes;
        current.frames += frames;
    }
    Ok(CaptureQuotaLease {
        quota,
        bytes,
        frames,
    })
}

fn with_capture_quota_lease<T>(
    lease: CaptureQuotaLease,
    admission: impl FnOnce() -> Result<T>,
) -> Result<(T, CaptureQuotaLease)> {
    let value = admission()?;
    Ok((value, lease))
}

struct RuntimeCaptureBackend {
    root: PathBuf,
    session: Arc<Session>,
    artifacts: Arc<ArtifactStore>,
    retention: Arc<Mutex<RetentionRegistry>>,
    quota: Arc<Mutex<CaptureQuota>>,
    retention_id: Option<String>,
    created_at_ms: u128,
}

struct CaptureFrameLease(crate::session::EvidenceLease);

impl CaptureBackend for RuntimeCaptureBackend {
    type JobLease = CaptureJobLease;
    type FrameLease = CaptureFrameLease;

    fn admit(
        &mut self,
        request: &crate::capture_jobs::CaptureRequest,
        _: &ObservationIdentity,
    ) -> Result<Self::JobLease> {
        ensure!(
            request.authority.session_id == self.session.id,
            "capture session mismatch"
        );
        let session_lease = self.session.retain_evidence()?;
        let reserved_bytes = request.budget.max_bytes.saturating_add(2 * 1024 * 1024);
        let reserved_frames = u64::from(request.budget.max_frames);
        let quota_lease =
            reserve_capture_quota(self.quota.clone(), reserved_bytes, reserved_frames)?;
        let directory = capture_directory(&self.root, &request.handle);
        let directory_was_present = directory.exists();
        let admission = with_capture_quota_lease(quota_lease, || -> Result<String> {
            crate::session::create_private_dir(&directory)?;
            let checkpoint_path = directory.join("checkpoint.json");
            if !checkpoint_path.exists() {
                crate::session::atomic_json_write(
                    &checkpoint_path,
                    &serde_json::json!({"state":"admitting"}),
                )?;
            }
            let relative = checkpoint_path.strip_prefix(&self.root)?;
            let mut retention = self
                .retention
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(entry) = retention
                .entries()
                .iter()
                .find(|entry| entry.relative_path == relative)
            {
                Ok(entry.id.clone())
            } else {
                let entry = crate::retention::ManagedEntry::new(
                    relative.to_path_buf(),
                    OwnerKind::Run,
                    RetentionClass::Interactive,
                    self.created_at_ms,
                    "active transient terminal capture job",
                );
                let id = entry.id.clone();
                retention.register(entry)?;
                Ok(id)
            }
        });
        let (retention_id, quota_lease) = match admission {
            Ok(result) => result,
            Err(error) => {
                if !directory_was_present {
                    let _ = std::fs::remove_dir_all(&directory);
                }
                return Err(error);
            }
        };
        self.retention_id = Some(retention_id);
        Ok(CaptureJobLease {
            _session: session_lease,
            _quota: quota_lease,
        })
    }

    fn retain(&mut self, _: &str, _: &CaptureFrameRef) -> Result<Self::FrameLease> {
        Ok(CaptureFrameLease(self.session.retain_evidence()?))
    }

    fn checkpoint(&mut self, checkpoint: &CaptureCheckpoint) -> Result<()> {
        let path =
            capture_directory(&self.root, &checkpoint.request.handle).join("checkpoint.json");
        crate::session::atomic_json_write(&path, checkpoint)?;
        if checkpoint.status.state.is_terminal()
            && let Some(id) = &self.retention_id
        {
            let (class, pinned) = match checkpoint.status.state {
                crate::capture_jobs::CaptureState::Completed => (RetentionClass::Successful, false),
                crate::capture_jobs::CaptureState::Cancelled => {
                    (RetentionClass::Interactive, false)
                }
                _ => (RetentionClass::Failed, false),
            };
            self.retention
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .set_retention_at(id, class, pinned, checkpoint.status.updated_at_ms as u128)?;
        }
        Ok(())
    }

    fn materialize(
        &mut self,
        _: &str,
        frame: &CaptureFrameRef,
        snapshot: &crate::protocol::FrameSnapshot,
    ) -> Result<crate::capture_jobs::MaterializedCapture> {
        let artifact = self.session.screenshot_snapshot(snapshot)?;
        let path = Path::new(&artifact.path);
        let parent = path.parent().context("capture screenshot has no parent")?;
        let root_id = self
            .artifacts
            .register_session_evidence(&self.session.id, parent)?;
        let filename = path
            .file_name()
            .context("capture screenshot has no file name")?;
        self.artifacts.register_evidence_file_with_id(
            &self.session.id,
            &root_id,
            Path::new(filename),
            &artifact.artifact_id,
            ArtifactSourceKind::Screenshot,
            "image/png",
            Some(frame.observation.run_id.clone()),
            Some("capture job frame".into()),
        )?;
        Ok(crate::capture_jobs::MaterializedCapture {
            reference: artifact.artifact_id,
            observation: frame.observation.clone(),
        })
    }
}

impl Drop for CaptureFrameLease {
    fn drop(&mut self) {
        let _ = &self.0;
    }
}

fn capture_directory(root: &Path, handle: &str) -> PathBuf {
    let digest = sha2::Sha256::digest(handle.as_bytes());
    root.join("captures").join(format!("{:x}", digest))
}

fn capture_clock_ms() -> u64 {
    static EPOCH: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    EPOCH
        .get_or_init(Instant::now)
        .elapsed()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

fn runtime_capture_observation(session: &Session) -> crate::capture_jobs::CaptureObservation {
    let (frame, revision, viewport_revision) = session.observation_snapshot();
    crate::capture_jobs::CaptureObservation {
        identity: ObservationIdentity {
            session_id: frame.session_id.clone(),
            run_id: frame.run_id.clone(),
            frame_id: frame.frame_id.clone(),
            observation_revision: revision,
            viewport_revision,
            cols: frame.cols,
            rows: frame.rows,
        },
        producer: crate::capture_jobs::ProducerProgress {
            // The terminal revision is the strongest live producer signal we
            // currently expose. A skipped revision is reported as a gap.
            event_sequence: revision,
            dropped_events: 0,
            coalesced_events: 0,
        },
        frame: Arc::new(frame),
    }
}

fn spawn_capture_observer(
    session: Arc<Session>,
    handle: String,
    manager: Arc<Mutex<crate::capture_jobs::CaptureManager<RuntimeCaptureBackend>>>,
) {
    std::thread::Builder::new()
        .name("terminal-capture-observer".into())
        .spawn(move || {
            loop {
                let now = capture_clock_ms();
                let status = {
                    let mut manager = manager
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    match refresh_capture(&session, &mut manager, &handle, now) {
                        Ok(status) => status,
                        Err(error) => {
                            let _ = manager.terminate(
                                &handle,
                                crate::capture_jobs::CaptureState::Failed,
                                now,
                                format!("capture observer stopped: {error:#}"),
                            );
                            break;
                        }
                    }
                };
                if status.state.is_terminal() {
                    break;
                }
                let observation = runtime_capture_observation(&session);
                let wake_at = manager
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .next_wakeup_ms(&handle)
                    .ok()
                    .flatten();
                let until_sample = wake_at
                    .map(|at| at.saturating_sub(now))
                    .unwrap_or(20)
                    .clamp(1, 20);
                session.wait_for_change(
                    observation.identity.observation_revision,
                    Duration::from_millis(until_sample),
                );
            }
        })
        .expect("spawn bounded capture observer");
}

fn refresh_capture(
    session: &Session,
    manager: &mut crate::capture_jobs::CaptureManager<RuntimeCaptureBackend>,
    handle: &str,
    now_ms: u64,
) -> Result<crate::capture_jobs::CaptureStatus> {
    match session.summary().state.as_str() {
        "closed" => {
            return manager.terminate(
                handle,
                crate::capture_jobs::CaptureState::SessionClosed,
                now_ms,
                "owning terminal session closed before capture completed",
            );
        }
        "exited" => {
            return manager.terminate(
                handle,
                crate::capture_jobs::CaptureState::ProcessExited,
                now_ms,
                "owned application process exited before capture completed",
            );
        }
        _ => {}
    }
    manager.advance(handle, now_ms, Some(runtime_capture_observation(session)))
}

fn capture_status_projection(
    status: crate::capture_jobs::CaptureStatus,
    options: Option<crate::projection::ResponseOptions>,
    operation: &str,
) -> Result<Value> {
    let options = options.unwrap_or_default();
    let mode = options.validate(crate::projection::ResponseMode::Summary)?;
    let limit = options
        .max_bytes
        .unwrap_or(if mode == crate::projection::ResponseMode::Full {
            64 * 1024
        } else {
            2048
        });
    let frame_count = status.frames.len();
    let mut value = if mode == crate::projection::ResponseMode::Summary {
        let latch = |latch: &Option<crate::capture_jobs::CaptureLatch>| {
            latch.as_ref().map(|value| {
                json!({
                    "frame_id":value.observation.frame_id,
                    "observation_revision":value.observation.observation_revision,
                    "at_ms":value.at_ms,
                    "snippet":value.matched.as_ref().map(|matched| &matched.snippet),
                })
            })
        };
        json!({
            "handle":status.handle,"state":status.state,"capture_status":status.state,
            "armed_baseline":status.armed_baseline,"armed_at_ms":status.armed_at_ms,
            "deadline_ms":status.deadline_ms,"updated_at_ms":status.updated_at_ms,
            "appearance":latch(&status.appearance),"transition":latch(&status.transition),
            "frame_count":frame_count,"frames_omitted":frame_count,
            "retained_bytes":status.retained_bytes,"burst_finished":status.burst_finished,
            "coverage":status.coverage,"failure":status.failure,"operation":operation,
            "sampling_guarantee":"best_effort_not_every_transition"
        })
    } else {
        let mut value = serde_json::to_value(&status)?;
        value["capture_status"] = json!(status.state);
        value["frame_count"] = json!(frame_count);
        value["operation"] = json!(operation);
        value["sampling_guarantee"] = json!("best_effort_not_every_transition");
        value
    };
    value["response"] = json!({"contract_revision":2,"mode":mode,"max_bytes":limit,"truncated":mode == crate::projection::ResponseMode::Summary && frame_count > 0});
    if serde_json::to_vec(&value)?.len() > limit {
        bail!("capture_response_too_large: response exceeds response.max_bytes")
    }
    Ok(value)
}
struct WaitJob {
    session_id: String,
    run_id: String,
    predicate: Value,
    evaluator: PredicateEvaluator,
    deadline: Instant,
    trace_stage: Option<String>,
    terminal: Option<Value>,
    cancelled: Arc<AtomicBool>,
}
struct FreshWaitJob {
    session_id: String,
    run_id: String,
    spec: Value,
    timeout_ms: u64,
    deadline: Instant,
    baseline: ObservationIdentity,
    baseline_bytes: usize,
    evaluator: Option<WaitEvaluator>,
    _evidence_lease: Option<crate::session::EvidenceLease>,
    terminal: Option<Value>,
    terminal_at: Option<Instant>,
    cancelled: Arc<AtomicBool>,
    deadline_signal: Arc<(Mutex<bool>, Condvar)>,
}

struct SequenceBaselineEntry {
    baseline: Option<crate::wait_engine::WaitBaseline>,
    retained_bytes: usize,
    _lease: crate::session::EvidenceLease,
}

impl FreshWaitJob {
    fn finish(&mut self, result: Value) -> Value {
        self.evaluator = None;
        self._evidence_lease = None;
        self.baseline_bytes = 0;
        self.terminal_at = Some(Instant::now());
        self.terminal = Some(result.clone());
        let (finished, wake) = &*self.deadline_signal;
        *finished
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
        wake.notify_one();
        result
    }
}

use crate::contracts::*;

fn parse<T: serde::de::DeserializeOwned>(params: Value) -> Result<T> {
    Ok(crate::validation::decode(params)?)
}
fn encode<T: serde::Serialize>(value: T) -> Result<Value> {
    Ok(serde_json::to_value(value)?)
}

fn project_sequence_report(
    report: crate::sequences::SequenceReport,
    options: Option<crate::projection::ResponseOptions>,
    baseline_retained_bytes: Option<usize>,
) -> Result<Value> {
    let options = options.unwrap_or_default();
    let mode = options.validate(crate::projection::ResponseMode::Summary)?;
    let budget = options
        .max_bytes
        .unwrap_or(if mode == crate::projection::ResponseMode::Full {
            crate::projection::MAX_RESPONSE_BYTES
        } else {
            1024
        });
    if mode == crate::projection::ResponseMode::Full {
        let mut value = encode(report)?;
        value["baseline_retention"] = json!({"retained":baseline_retained_bytes.is_some(),"bytes":baseline_retained_bytes.unwrap_or(0)});
        value["response"] =
            json!({"contract_revision":2,"mode":"full","max_bytes":budget,"truncated":false});
        if serde_json::to_vec(&value)?.len() > budget {
            bail!("sequence_response_too_large: full report exceeds response.max_bytes")
        }
        return Ok(value);
    }
    let all_steps = report.steps.len();
    let baseline_retention = json!({"retained":baseline_retained_bytes.is_some(),"bytes":baseline_retained_bytes.unwrap_or(0)});
    let mut steps = report.steps.iter().map(|step| json!({
        "step_id":step.step_id,"status":step.status,
        "pending_action_ids":step.pending_action_ids,
        "receipt_count":step.receipts.len(),
        "receipts":step.receipts.iter().take(1).map(|receipt| json!({
            "action_id":receipt.identity.action_id,"pre_dispatch":receipt.pre_dispatch,
            "post_dispatch_viewport":receipt.post_dispatch_viewport,"written_to_pty":receipt.written_to_pty,
            "input_disposition":receipt.input_disposition
        })).collect::<Vec<_>>(),
        "failure":step.failure.as_deref().map(|error| crate::projection::truncate(error, 160))
    })).collect::<Vec<_>>();
    let make = |steps: Vec<Value>, truncated: bool| {
        json!({
            "identity":report.identity,"status":report.status,"deadline_unix_ms":report.deadline_unix_ms,
            "completed_step_ids":report.completed_step_ids,"failing_step_id":report.failing_step_id,
            "pending_step_ids":report.pending_step_ids,"step_count":all_steps,"steps":steps,
            "baseline_retention":baseline_retention,
            "failure":report.failure.as_deref().map(|error| crate::projection::truncate(error, 160)),
            "response":{"contract_revision":2,"mode":"summary","max_bytes":budget,"truncated":truncated}
        })
    };
    let mut value = make(steps.clone(), steps.len() < all_steps);
    while steps.len() > 1 && serde_json::to_vec(&value)?.len() > budget {
        steps.pop();
        value = make(steps.clone(), true);
    }
    if serde_json::to_vec(&value)?.len() > budget {
        bail!("sequence_response_too_large: summary exceeds response.max_bytes")
    }
    Ok(value)
}

fn read_json(path: &Path) -> Result<Value> {
    crate::profiles::ensure_no_symlink_components(path)?;
    Ok(serde_json::from_slice(&std::fs::read(path)?)?)
}

fn remove_historical_credentials(session_dir: &Path, _manifest: &Value) -> Result<()> {
    // Frozen at launch: a later profile edit must not redirect credential deletion.
    let diagnostics = read_json(&session_dir.join("launch-diagnostics.json"))?;
    let destinations: Vec<PathBuf> = serde_json::from_value(
        diagnostics
            .get("credential_destinations")
            .cloned()
            .unwrap_or(json!([])),
    )?;
    for destination in destinations {
        crate::artifacts::validate_relative(&destination)?;
        let path = session_dir.join("state").join(destination);
        crate::profiles::ensure_no_symlink_components(&path)?;
        if path.is_file() {
            std::fs::remove_file(path)?;
        } else if path.exists() {
            bail!(
                "credential_cleanup_failed: broad legacy credential seed requires explicit cleanup"
            );
        }
    }
    Ok(())
}

impl AutomationService {
    pub fn new(root: PathBuf) -> Result<Self> {
        let sequence_path = root.join("sequences/sequence-journal.json");
        let policy_path = root.join("retention-policy.json");
        let policy = if policy_path.exists() {
            serde_json::from_slice(&std::fs::read(&policy_path)?)?
        } else {
            crate::retention::RetentionPolicy::default()
        };
        let service = Self {
            artifacts: Arc::new(ArtifactStore::new(root.clone())?),
            frames: FrameStore::new(root.clone())?,
            retention: Arc::new(Mutex::new(RetentionRegistry::open_with_policy(
                &root, policy,
            )?)),
            scenarios: ScenarioManager::new(root.join("scenarios"))?,
            root,
            registrations: Mutex::new(HashMap::new()),
            waits: Mutex::new(HashMap::new()),
            wait_cancellations: Mutex::new(HashMap::new()),
            fresh_waits: Mutex::new(HashMap::new()),
            fresh_wait_cancellations: Mutex::new(HashMap::new()),
            sequences: Arc::new(Mutex::new(SequenceManager::open(sequence_path)?)),
            sequence_cancellations: Arc::new(Mutex::new(HashMap::new())),
            sequence_baselines: Arc::new(Mutex::new(HashMap::new())),
            captures: Arc::new(Mutex::new(HashMap::new())),
            capture_quota: Arc::new(Mutex::new(CaptureQuota::default())),
            diagnostics: crate::diagnostics::DiagnosticsManager::new(
                crate::diagnostics::Limits::default(),
            )?,
            finalizations: Mutex::new(()),
            app_adapters: Mutex::new(HashMap::new()),
            recordings: Mutex::new(HashMap::new()),
        };
        for report in service.scenarios.list() {
            service.register_scenario_report(&report)?;
        }
        // Older versions marked disposable launches successful before any test
        // outcome existed. Do not carry that inferred pass into new retention.
        let mut registry = service
            .retention
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let legacy: Vec<_> = registry
            .entries()
            .iter()
            .filter(|entry| {
                let parts: Vec<_> = entry.relative_path.components().collect();
                entry.retention == RetentionClass::Successful
                    && parts.len() == 2
                    && matches!(
                        parts[0].as_os_str().to_str(),
                        Some("sessions" | "artifacts" | "workspaces")
                    )
                    && !service
                        .root
                        .join("sessions")
                        .join(parts[1])
                        .join("finalization.json")
                        .exists()
            })
            .map(|entry| entry.id.clone())
            .collect();
        for id in legacy {
            registry.set_retention_at(
                &id,
                RetentionClass::Failed,
                false,
                crate::engine::now_ms(),
            )?;
        }
        drop(registry);
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

    pub fn register_session(&self, session: &Session, _disposable: bool) -> Result<()> {
        let id = &session.id;
        let class = RetentionClass::Interactive;
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
        _disposable: bool,
    ) -> Result<()> {
        let evidence = self.root.join("artifacts").join(session_id);
        crate::session::create_private_dir(&evidence)?;
        let retention = RetentionClass::Interactive;
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
        for (handle, manager) in self
            .captures
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
        {
            let manager = manager
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if manager
                .status(handle)
                .is_ok_and(|status| !status.state.is_terminal())
            {
                paths.push(capture_directory(&self.root, handle));
            }
        }
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

    fn crop(&self, service: &SessionService, args: CropArgs) -> Result<Value> {
        let session = service.active_session(&args.session_id)?;
        let _lifecycle = session.retain_evidence()?;
        let (frame, observation_revision, viewport_revision) = session.observation_snapshot();
        let source = ObservationIdentity {
            session_id: frame.session_id.clone(),
            run_id: frame.run_id.clone(),
            frame_id: frame.frame_id.clone(),
            observation_revision,
            viewport_revision,
            cols: frame.cols,
            rows: frame.rows,
        };
        if let Some(expected) = &args.source
            && expected != &source
        {
            bail!("stale_observation: crop source identity does not match the current atomic frame")
        }
        let crop = crate::crop_evidence::render_crop(
            &frame,
            &source,
            &crate::crop_evidence::CropRequest {
                region: args.region,
                scale: args.scale,
            },
        )?;
        let artifact_id = ulid::Ulid::generate().to_string();
        let metadata_artifact_id = format!("cropmeta-{artifact_id}");
        let persisted = PersistedCropEvidence {
            version: 1,
            artifact_id: artifact_id.clone(),
            metadata_artifact_id: metadata_artifact_id.clone(),
            metadata: crop.metadata.clone(),
        };
        let metadata_bytes = serde_json::to_vec_pretty(&persisted)?;
        ensure!(
            metadata_bytes.len() <= 64 * 1024,
            "crop_metadata_limit: crop metadata exceeds 64 KiB"
        );
        let reserved_bytes = (crop.png.len() as u64).saturating_add(metadata_bytes.len() as u64);
        let _quota = session.evidence_budget_guard_for(reserved_bytes, 2)?;
        let directory = self
            .root
            .join("artifacts")
            .join(&args.session_id)
            .join(&source.run_id);
        crate::session::create_private_dir(&directory)?;
        let png_name = format!("{artifact_id}-{}.png", source.frame_id);
        let metadata_name = format!("cropmeta-{artifact_id}.json");
        let png_path = directory.join(&png_name);
        let metadata_path = directory.join(&metadata_name);
        write_private_atomic(&png_path, &crop.png)?;
        if let Err(error) = write_private_atomic(&metadata_path, &metadata_bytes) {
            let _ = std::fs::remove_file(&png_path);
            return Err(error);
        }
        let root_id = match self
            .artifacts
            .register_session_evidence(&args.session_id, &directory)
        {
            Ok(root_id) => root_id,
            Err(error) => {
                let _ = std::fs::remove_file(&png_path);
                let _ = std::fs::remove_file(&metadata_path);
                return Err(error);
            }
        };
        if let Err(error) = self.artifacts.register_evidence_files_with_ids(
            &args.session_id,
            &root_id,
            &[
                crate::artifacts::EvidenceFileRegistration {
                    relative_path: Path::new(&png_name),
                    artifact_id: &artifact_id,
                    source_kind: ArtifactSourceKind::Screenshot,
                    content_type: "image/png",
                    run_id: Some(source.run_id.clone()),
                    retention: Some("session crop evidence".into()),
                },
                crate::artifacts::EvidenceFileRegistration {
                    relative_path: Path::new(&metadata_name),
                    artifact_id: &metadata_artifact_id,
                    source_kind: ArtifactSourceKind::Frame,
                    content_type: "application/json",
                    run_id: Some(source.run_id.clone()),
                    retention: Some("session crop metadata".into()),
                },
            ],
        ) {
            let _ = std::fs::remove_file(&png_path);
            let _ = std::fs::remove_file(&metadata_path);
            return Err(error);
        }
        let mut result = serde_json::to_value(&crop.metadata)?;
        result["artifact_id"] = json!(artifact_id);
        result["metadata_artifact_id"] = json!(metadata_artifact_id);
        result["content_type"] = json!("image/png");
        result["png_size_bytes"] = json!(crop.png.len());
        result["artifact_storage"] = json!("terminal_read_artifact");
        Ok(result)
    }

    fn crop_compare(&self, service: &SessionService, args: CropCompareArgs) -> Result<Value> {
        crate::artifacts::validate_identity(&args.session_id, "session id")?;
        let _lifecycle = match service.active_session(&args.session_id) {
            Ok(session) => Some(session.retain_evidence()?),
            Err(_) => None,
        };
        let left = self.read_persisted_crop(&args.session_id, &args.left_artifact_id)?;
        let right = self.read_persisted_crop(&args.session_id, &args.right_artifact_id)?;
        if left.metadata.source.run_id != right.metadata.source.run_id {
            bail!("pixel_comparison_incompatible: source runs differ")
        }
        let comparison =
            crate::crop_evidence::compare_crop_pixels(&left, &right, args.tolerance)
                .map_err(|error| anyhow::anyhow!("pixel_comparison_incompatible: {error}"))?;
        Ok(json!({
            "session_id": args.session_id,
            "left_artifact_id": args.left_artifact_id,
            "right_artifact_id": args.right_artifact_id,
            "left_source": left.metadata.source,
            "right_source": right.metadata.source,
            "tolerance": args.tolerance,
            "comparison": comparison,
            "supplemental_to": "terminal_diff"
        }))
    }

    fn read_persisted_crop(
        &self,
        session_id: &str,
        artifact_id: &str,
    ) -> Result<crate::crop_evidence::CroppedEvidence> {
        crate::artifacts::validate_identity(artifact_id, "crop artifact id")?;
        let png_artifact = self.artifacts.metadata(session_id, artifact_id)?;
        ensure!(
            png_artifact.source_kind == ArtifactSourceKind::Screenshot
                && png_artifact.content_type == "image/png",
            "crop_artifact_invalid: artifact is not a registered crop PNG"
        );
        let metadata_artifact_id = format!("cropmeta-{artifact_id}");
        let metadata_artifact = self
            .artifacts
            .metadata(session_id, &metadata_artifact_id)
            .context("crop_metadata_missing: registered crop metadata is unavailable")?;
        ensure!(
            metadata_artifact.source_kind == ArtifactSourceKind::Frame
                && metadata_artifact.content_type == "application/json"
                && metadata_artifact.run_id == png_artifact.run_id,
            "crop_metadata_invalid: metadata artifact identity does not match PNG"
        );
        let metadata_bytes = read_registered_artifact(
            &self.artifacts,
            session_id,
            &metadata_artifact_id,
            64 * 1024,
        )?;
        let record: PersistedCropEvidence = serde_json::from_slice(&metadata_bytes)
            .context("crop_metadata_invalid: persisted metadata cannot be decoded")?;
        ensure!(
            record.version == 1
                && record.artifact_id == artifact_id
                && record.metadata_artifact_id == metadata_artifact_id
                && record.metadata.source.session_id == session_id
                && Some(record.metadata.source.run_id.as_str()) == png_artifact.run_id.as_deref()
                && record.metadata.png_sha256.len() == 64,
            "crop_metadata_invalid: persisted source/hash identity does not match artifacts"
        );
        let png = read_registered_artifact(
            &self.artifacts,
            session_id,
            artifact_id,
            crate::crop_evidence::MAX_CROP_PNG_BYTES,
        )?;
        Ok(crate::crop_evidence::CroppedEvidence {
            png,
            metadata: record.metadata,
        })
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
                        if let Some(crop_id) = file
                            .file_name()
                            .to_str()
                            .and_then(|name| name.strip_prefix("cropmeta-"))
                            .and_then(|name| name.strip_suffix(".json"))
                        {
                            crate::artifacts::validate_identity(crop_id, "crop artifact id")?;
                            let path = file.path();
                            let root_id = self.artifacts.register_session_evidence(
                                session_id,
                                path.parent().context("crop metadata has no parent")?,
                            )?;
                            self.artifacts.register_evidence_file_with_id(
                                session_id,
                                &root_id,
                                Path::new(
                                    path.file_name().context("crop metadata has no filename")?,
                                ),
                                &format!("cropmeta-{crop_id}"),
                                ArtifactSourceKind::Frame,
                                "application/json",
                                Some(run.file_name().to_string_lossy().into_owned()),
                                Some("session crop metadata".into()),
                            )?;
                            continue;
                        }
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
                                let crop_metadata = path
                                    .parent()
                                    .context("screenshot has no parent")?
                                    .join(format!("cropmeta-{capture_id}.json"));
                                if crop_metadata.is_file() {
                                    crate::artifacts::validate_identity(
                                        capture_id,
                                        "crop artifact id",
                                    )?;
                                    let root_id = self.artifacts.register_session_evidence(
                                        session_id,
                                        path.parent().context("crop screenshot has no parent")?,
                                    )?;
                                    self.artifacts.register_evidence_file_with_id(
                                        session_id,
                                        &root_id,
                                        Path::new(name),
                                        capture_id,
                                        ArtifactSourceKind::Screenshot,
                                        "image/png",
                                        Some(run.file_name().to_string_lossy().into_owned()),
                                        Some("session crop evidence".into()),
                                    )?;
                                    continue;
                                }
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

    pub(crate) fn compare_named(
        &self,
        service: &SessionService,
        args: DiffArgs,
        strict: bool,
    ) -> Result<Value> {
        use crate::validation::RequestError;
        let saved = self
            .frames
            .load_named(&args.session_id, &args.name)
            .map_err(|error| {
                if strict
                    && error
                        .downcast_ref::<std::io::Error>()
                        .is_some_and(|error| error.kind() == std::io::ErrorKind::NotFound)
                {
                    RequestError::new("name", "retained named frame baseline")
                        .code("baseline_unavailable")
                        .into()
                } else {
                    error
                }
            })?;
        let frame = service.active_session(&args.session_id)?.observe();
        if strict
            && (saved.run_id != frame.run_id
                || saved.frame.cols != frame.cols
                || saved.frame.rows != frame.rows
                || saved.render_profile != RENDER_PROFILE)
        {
            return Err(RequestError::new(
                "name",
                "baseline from the same application run, terminal dimensions and render profile",
            )
            .code("baseline_incompatible")
            .into());
        }
        let diff = crate::frame_store::diff_frames(&saved, &frame, RENDER_PROFILE, &args.masks)?;
        let passed = diff.changed_cell_count == 0;
        let mut value = encode(diff)?;
        value["passed"] = json!(passed);
        Ok(value)
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
        if args.cancel
            && let Some(wait_id) = args.wait_id.as_ref()
            && let Some(token) = self
                .wait_cancellations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(wait_id)
                .cloned()
        {
            token.store(true, Ordering::Release);
        }
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
            let cancelled = Arc::new(AtomicBool::new(false));
            self.wait_cancellations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(id.clone(), Arc::clone(&cancelled));
            let job = Arc::new(Mutex::new(WaitJob {
                session_id: args.session_id.clone(),
                run_id: session.summary().run_id,
                predicate: serde_json::to_value(&predicate)?,
                evaluator: PredicateEvaluator::new(predicate)?,
                deadline: Instant::now() + Duration::from_millis(args.timeout_ms),
                trace_stage: args.trace_stage.clone(),
                terminal: None,
                cancelled,
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
        let (session_id, run_id, cancelled, deadline) = {
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
            (
                job.session_id.clone(),
                job.run_id.clone(),
                Arc::clone(&job.cancelled),
                job.deadline,
            )
        };
        let session = service.active_session(&session_id)?;
        let poll_deadline = Instant::now() + Duration::from_millis(args.poll_ms);
        let (mut result, stop) = execute_session_wait(
            &session,
            deadline,
            Some(poll_deadline),
            || cancelled.load(Ordering::Acquire),
            |frame, revision, viewport_revision, now| {
                let (evaluation, trace_stage, next_wakeup) = {
                    let mut current = job
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if let Some(result) = &current.terminal {
                        return Ok(WaitStep {
                            value: result.clone(),
                            terminal: true,
                            next_wakeup: None,
                        });
                    }
                    if frame.run_id != run_id {
                        bail!("stale_run: wait targets previous application run")
                    }
                    let evaluation = current.evaluator.observe(frame, now);
                    let next_wakeup = current.evaluator.next_deadline();
                    (evaluation, current.trace_stage.clone(), next_wakeup)
                };
                let trace_ready = if let Some(stage) = trace_stage {
                    let log = self.logs(
                        service,
                        LogsArgs {
                            session_id: session_id.clone(),
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
                let mut value = encode(&evaluation)?;
                value["matched"] = json!(evaluation.matched && trace_ready);
                value["wait_id"] = json!(id);
                value["status"] = json!(if done { "completed" } else { "pending" });
                value["timed_out"] = json!(false);
                value["trace_ready"] = json!(trace_ready);
                value["snapshot"] = encode(frame)?;
                value["observation_revision"] = json!(revision);
                value["viewport_revision"] = json!(viewport_revision);
                let mut current = job
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if done {
                    current.terminal = Some(value.clone());
                }
                Ok(WaitStep {
                    value,
                    terminal: done,
                    next_wakeup,
                })
            },
        )?;
        match stop {
            WaitStop::Predicate => {}
            WaitStop::Cancelled => {
                result = json!({"wait_id":id,"status":"cancelled","matched":false});
                job.lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .terminal = Some(result.clone());
            }
            WaitStop::Deadline => {
                result["status"] = json!("timed_out");
                result["timed_out"] = json!(true);
                job.lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .terminal = Some(result.clone());
            }
            WaitStop::PollComplete => {}
        }
        Ok(result)
    }

    fn locate(&self, service: &SessionService, args: LocatorArgs) -> Result<Value> {
        let session = service.active_session(&args.session_id)?;
        let (frame, observation_revision, viewport_revision) = session.observation_snapshot();
        let result = locate_text(
            &frame,
            &args.query,
            args.region,
            observation_revision,
            viewport_revision,
        )?;
        encode(result)
    }

    fn wait_fresh(&self, service: &SessionService, args: FreshWaitArgs) -> Result<Value> {
        use crate::validation::RequestError;
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
            return Err(RequestError::new(
                "wait_id",
                "non-empty wait ID up to 128 safe characters",
            )
            .into());
        }
        let mut map = self
            .fresh_waits
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = Instant::now();
        map.retain(|wait_id, job| {
            let mut job = job
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if job.terminal.is_none() && now >= job.deadline {
                job.finish(json!({"wait_id":wait_id,"status":"timed_out","matched":false}));
            }
            job.terminal_at.is_none_or(|terminal_at| {
                now.saturating_duration_since(terminal_at) < FRESH_WAIT_TOMBSTONE_TTL
            })
        });
        let retained_ids = map
            .keys()
            .cloned()
            .collect::<std::collections::HashSet<_>>();
        self.fresh_wait_cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|wait_id, _| retained_ids.contains(wait_id));
        let existing = map.get(&id).cloned();
        if args.cancel {
            if existing.is_none() {
                return Err(RequestError::new(
                    "wait_id",
                    "existing fresh-wait handle when cancelling",
                )
                .code("baseline_unavailable")
                .into());
            }
            if let Some(token) = self
                .fresh_wait_cancellations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&id)
                .cloned()
            {
                token.store(true, Ordering::Release);
            }
        }
        let job = if let Some(job) = existing {
            job
        } else {
            let session_id = args.session_id.clone().ok_or_else(|| {
                RequestError::new("session_id", "required when creating a fresh wait")
            })?;
            let spec = args
                .spec
                .clone()
                .ok_or_else(|| RequestError::new("spec", "required when creating a fresh wait"))?;
            let timeout_ms = args.timeout_ms.ok_or_else(|| {
                RequestError::new("timeout_ms", "required when creating a fresh wait")
            })?;
            let identity = args.prior_observation.clone().ok_or_else(|| {
                RequestError::new(
                    "prior_observation",
                    "identity from a retained prior observe or action receipt",
                )
                .code("baseline_unavailable")
            })?;
            let session = service.active_session(&session_id)?;
            let (baseline, baseline_bytes) =
                service.resolve_wait_baseline(&session_id, &identity)?;
            let current = session.summary();
            if current.run_id != identity.run_id {
                return Err(RequestError::new(
                    "prior_observation",
                    "same active application run and viewport",
                )
                .code("baseline_incompatible")
                .into());
            }
            let active_jobs = map
                .values()
                .filter(|job| job.lock().is_ok_and(|job| job.terminal.is_none()))
                .count();
            if active_jobs >= MAX_FRESH_WAIT_JOBS
                || map.len() >= MAX_FRESH_WAIT_TOMBSTONES
                || map
                    .values()
                    .map(|j| j.lock().map(|j| j.baseline_bytes).unwrap_or(0))
                    .fold(0usize, usize::saturating_add)
                    .saturating_add(baseline_bytes)
                    > MAX_FRESH_WAIT_BASELINE_BYTES
            {
                return Err(RequestError::new(
                    "wait_id",
                    "at most 32 active fresh waits and 64 MiB retained baselines",
                )
                .code("wait_capacity")
                .into());
            }
            let deadline = Instant::now() + Duration::from_millis(timeout_ms);
            let evaluator = WaitEvaluator::new(spec.clone(), baseline, deadline)?;
            let cancelled = Arc::new(AtomicBool::new(false));
            let deadline_signal = Arc::new((Mutex::new(false), Condvar::new()));
            self.fresh_wait_cancellations
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(id.clone(), Arc::clone(&cancelled));
            let job = Arc::new(Mutex::new(FreshWaitJob {
                session_id: session_id.clone(),
                run_id: identity.run_id.clone(),
                spec: serde_json::to_value(&spec)?,
                timeout_ms,
                deadline,
                baseline: identity,
                baseline_bytes,
                evaluator: Some(evaluator),
                _evidence_lease: Some(session.retain_evidence()?),
                terminal: None,
                terminal_at: None,
                cancelled,
                deadline_signal: Arc::clone(&deadline_signal),
            }));
            let timeout_job = Arc::downgrade(&job);
            let timeout_id = id.clone();
            std::thread::Builder::new()
                .name("fresh-wait-deadline".into())
                .spawn(move || {
                    let (finished, wake) = &*deadline_signal;
                    let guard = finished
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let (guard, _) = wake
                        .wait_timeout_while(
                            guard,
                            deadline.saturating_duration_since(Instant::now()),
                            |done| !*done,
                        )
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let already_finished = *guard;
                    drop(guard);
                    if already_finished {
                        return;
                    }
                    if let Some(job) = timeout_job.upgrade() {
                        let mut job = job
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        if job.terminal.is_none() {
                            job.finish(json!({
                                "wait_id": timeout_id,
                                "status": "timed_out",
                                "matched": false
                            }));
                        }
                    }
                })?;
            map.insert(id.clone(), Arc::clone(&job));
            job
        };
        drop(map);
        let (session_id, run_id, deadline, cancelled) = {
            let job = job
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if args
                .session_id
                .as_ref()
                .is_some_and(|s| s != &job.session_id)
                || args
                    .spec
                    .as_ref()
                    .is_some_and(|s| serde_json::to_value(s).ok().as_ref() != Some(&job.spec))
                || args.timeout_ms.is_some_and(|t| t != job.timeout_ms)
                || args
                    .prior_observation
                    .as_ref()
                    .is_some_and(|i| i != &job.baseline)
            {
                return Err(RequestError::new(
                    "wait_id",
                    "same session, specification, timeout and baseline when resuming",
                )
                .code("wait_conflict")
                .into());
            }
            if let Some(done) = &job.terminal {
                return Ok(done.clone());
            }
            (
                job.session_id.clone(),
                job.run_id.clone(),
                job.deadline,
                Arc::clone(&job.cancelled),
            )
        };
        let session = service.active_session(&session_id)?;
        let poll_deadline = Instant::now() + Duration::from_millis(args.poll_ms.min(5000));
        let (mut result, stop) = execute_session_wait(
            &session,
            deadline,
            Some(poll_deadline),
            || cancelled.load(Ordering::Acquire),
            |frame, revision, viewport_revision, now| {
                let mut job = job
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if let Some(done) = &job.terminal {
                    return Ok(WaitStep {
                        value: done.clone(),
                        terminal: true,
                        next_wakeup: None,
                    });
                }
                use crate::wait_engine::WaitOutcome;
                let outcome = if cancelled.load(Ordering::Acquire) {
                    job.evaluator
                        .as_mut()
                        .expect("active wait has evaluator")
                        .cancel()
                } else {
                    job.evaluator
                        .as_mut()
                        .expect("active wait has evaluator")
                        .observe(frame, revision, viewport_revision, now)
                };
                let (value, terminal) = match outcome {
                    WaitOutcome::Matched(matches) => (
                        json!({"wait_id":id,"status":"matched","matched":true,"baseline":job.baseline,"matches":matches,"observation_revision":revision,"viewport_revision":viewport_revision}),
                        true,
                    ),
                    WaitOutcome::Cancelled => (
                        json!({"wait_id":id,"status":"cancelled","matched":false}),
                        true,
                    ),
                    WaitOutcome::DeadlineExceeded => (
                        json!({"wait_id":id,"status":"timed_out","matched":false}),
                        true,
                    ),
                    WaitOutcome::StaleBaseline => (
                        json!({"wait_id":id,"status":"stale_run","matched":false,"baseline":job.baseline}),
                        true,
                    ),
                    WaitOutcome::IncompatibleViewport => (
                        json!({"wait_id":id,"status":"baseline_incompatible","matched":false,"baseline":job.baseline}),
                        true,
                    ),
                    WaitOutcome::Pending if frame.run_id != run_id => (
                        json!({"wait_id":id,"status":"stale_run","matched":false,"baseline":job.baseline}),
                        true,
                    ),
                    WaitOutcome::Pending => (
                        json!({"wait_id":id,"status":"pending","matched":false,"baseline":job.baseline}),
                        false,
                    ),
                };
                if terminal {
                    job.finish(value.clone());
                }
                Ok(WaitStep {
                    value,
                    terminal,
                    next_wakeup: None,
                })
            },
        )?;
        if matches!(stop, WaitStop::Cancelled | WaitStop::Deadline) {
            let mut job = job
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some(done) = &job.terminal {
                return Ok(done.clone());
            }
            result = if matches!(stop, WaitStop::Cancelled) {
                json!({"wait_id":id,"status":"cancelled","matched":false})
            } else {
                json!({"wait_id":id,"status":"timed_out","matched":false})
            };
            job.finish(result.clone());
        }
        Ok(result)
    }

    fn sequence(&self, service: &SessionService, args: SequenceArgs) -> Result<Value> {
        let journal_path = self.root.join("sequences/sequence-journal.json");
        let response = args.response.clone();
        if args.action == "resume" {
            let sequence_id = args
                .sequence_id
                .context("invalid_params: sequence_id is required")?;
            let request = SequenceManager::read_request(&journal_path, &sequence_id)?;
            if args.actor_id.as_deref() != Some(request.identity.actor_id.as_str()) {
                return Err(crate::validation::RequestError::new(
                    "actor_id",
                    "owner of this sequence",
                )
                .code("sequence_forbidden")
                .into());
            }
            return self.sequence(
                service,
                SequenceArgs {
                    action: "start".into(),
                    request: Some(request),
                    sequence_id: Some(sequence_id),
                    actor_id: args.actor_id,
                    response,
                },
            );
        }
        match args.action.as_str() {
            "start" => {
                let request = args
                    .request
                    .context("invalid_params: request is required for sequence start")?;
                crate::sequences::validate_request(&request)?;
                if args.actor_id.as_deref() != Some(request.identity.actor_id.as_str()) {
                    return Err(crate::validation::RequestError::new(
                        "actor_id",
                        "match request.identity.actor_id",
                    )
                    .code("sequence_forbidden")
                    .into());
                }
                let session = service.active_session(&request.identity.session_id)?;
                let capabilities = session.capabilities();
                let summary = session.summary();
                if summary.run_id != request.identity.run_id {
                    bail!("stale_run: sequence targets a previous application run")
                }
                let viewport_revision = capabilities["viewport_revision"].as_u64().unwrap_or(0);
                if viewport_revision != request.authority.expected_viewport_revision
                    || summary.cols != request.authority.expected_cols
                    || summary.rows != request.authority.expected_rows
                {
                    bail!("stale_viewport: sequence authority does not match the current viewport")
                }
                let controller_generation = capabilities["controller"]["generation"]
                    .as_u64()
                    .unwrap_or(0);
                if controller_generation != request.authority.controller_generation {
                    bail!("stale_actor: sequence controller generation is stale")
                }
                if capabilities["controller"]["actor_id"].as_str()
                    != Some(request.identity.actor_id.as_str())
                {
                    return Err(crate::validation::RequestError::new(
                        "actor_id",
                        "current terminal controller; claim control before starting",
                    )
                    .code("sequence_forbidden")
                    .into());
                }
                let sequence_id = request.identity.sequence_id.clone();
                let cancellation = SequenceCancellation::default();
                self.sequences
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .admit(&request)?;
                let mut cancellations = self
                    .sequence_cancellations
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if cancellations.contains_key(&sequence_id) {
                    return Ok(
                        json!({"sequence_id":sequence_id,"status":"queued","duplicate":true}),
                    );
                }
                let mut baselines = self
                    .sequence_baselines
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if !baselines.contains_key(&sequence_id) {
                    let reserved = baselines
                        .values()
                        .map(|entry| entry.retained_bytes)
                        .fold(0usize, usize::saturating_add);
                    if baselines.len() >= crate::sequences::MAX_SEQUENCE_JOBS
                        || reserved.saturating_add(MAX_SEQUENCE_BASELINE_BYTES)
                            > MAX_SEQUENCE_BASELINES_BYTES
                    {
                        return Err(crate::validation::RequestError::new(
                            "sequence_id",
                            "at most eight 8 MiB sequence baseline reservations (64 MiB total)",
                        )
                        .code("sequence_capacity")
                        .into());
                    }
                    baselines.insert(
                        sequence_id.clone(),
                        SequenceBaselineEntry {
                            baseline: None,
                            retained_bytes: MAX_SEQUENCE_BASELINE_BYTES,
                            _lease: session.retain_evidence()?,
                        },
                    );
                }
                drop(baselines);
                cancellations.insert(sequence_id.clone(), cancellation.clone());
                drop(cancellations);
                let manager = Arc::clone(&self.sequences);
                let cancellations = Arc::clone(&self.sequence_cancellations);
                let baselines = Arc::clone(&self.sequence_baselines);
                let service = service.clone();
                let worker_sequence_id = sequence_id.clone();
                let spawn_result = std::thread::Builder::new()
                    .name(format!("sequence-{sequence_id}"))
                    .spawn(move || {
                        let mut backend = RuntimeSequenceBackend::new(service, baselines.clone());
                        let report = {
                            let mut manager = manager
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner);
                            manager.execute(request, &mut backend, &cancellation)
                        };
                        cancellations
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .remove(&worker_sequence_id);
                        if report.is_err()
                            || report.as_ref().is_ok_and(|r| {
                                !matches!(
                                    r.status,
                                    SequenceStatus::Pending | SequenceStatus::Running
                                )
                            })
                        {
                            baselines
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .remove(&worker_sequence_id);
                        }
                    });
                if let Err(error) = spawn_result {
                    self.sequence_cancellations
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .remove(&sequence_id);
                    self.sequence_baselines
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .remove(&sequence_id);
                    return Err(error.into());
                }
                let mut result = json!({"sequence_id":sequence_id,"status":"queued"});
                let reserved_bytes = self
                    .sequence_baselines
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get(&sequence_id)
                    .map(|entry| entry.retained_bytes)
                    .unwrap_or(0);
                result["baseline_retention"] =
                    json!({"retained":reserved_bytes > 0,"bytes":reserved_bytes});
                result["response"] = json!({"contract_revision":2,"mode":response.as_ref().and_then(|r| r.mode).unwrap_or(crate::projection::ResponseMode::Summary),"truncated":false});
                Ok(result)
            }
            "status" | "cancel" => {
                let sequence_id = args
                    .sequence_id
                    .context("invalid_params: sequence_id is required")?;
                let report = SequenceManager::read_report(&journal_path, &sequence_id)?;
                if args.actor_id.as_deref() != Some(report.identity.actor_id.as_str()) {
                    return Err(crate::validation::RequestError::new(
                        "actor_id",
                        "owner of this sequence",
                    )
                    .code("sequence_forbidden")
                    .into());
                }
                if args.action == "cancel" {
                    if let Some(cancellation) = self
                        .sequence_cancellations
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .get(&sequence_id)
                        .cloned()
                    {
                        cancellation.cancel();
                        let retained_bytes = self
                            .sequence_baselines
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .get(&sequence_id)
                            .map(|entry| entry.retained_bytes)
                            .unwrap_or(0);
                        return Ok(
                            json!({"sequence_id":sequence_id,"status":"cancellation_requested","baseline_retention":{"retained":retained_bytes > 0,"bytes":retained_bytes},"response":{"contract_revision":2,"mode":"summary","truncated":false}}),
                        );
                    }
                    let report = self
                        .sequences
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .cancel(&sequence_id)?;
                    if !matches!(
                        report.status,
                        SequenceStatus::Pending | SequenceStatus::Running
                    ) {
                        self.sequence_baselines
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .remove(&sequence_id);
                    }
                    return project_sequence_report(report, response, None);
                }
                let retained_bytes = self
                    .sequence_baselines
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get(&sequence_id)
                    .map(|entry| entry.retained_bytes);
                project_sequence_report(report, response, retained_bytes)
            }
            _ => bail!("invalid_params: action must be start, status, or cancel"),
        }
    }

    fn capture_job(&self, service: &SessionService, args: CaptureJobArgs) -> Result<Value> {
        use crate::capture_jobs::{CaptureLimits, CaptureState};
        match args.action.as_str() {
            "start" => {
                let request = args.request.context("request required for capture start")?;
                let session = service.active_session(&request.authority.session_id)?;
                let mut jobs = self
                    .captures
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if jobs.contains_key(&request.handle) {
                    let manager = jobs.get(&request.handle).unwrap().clone();
                    let armed = manager
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .start(
                            request.clone(),
                            runtime_capture_observation(&session),
                            capture_clock_ms(),
                        )?;
                    return Ok(
                        json!({"status":"armed","capture":armed,"armed_before_return":true,"sampling_guarantee":"best_effort_not_every_transition"}),
                    );
                }
                ensure!(
                    jobs.len() < crate::capture_jobs::MAX_CAPTURE_JOBS,
                    "capture_quota_exhausted"
                );
                let backend = RuntimeCaptureBackend {
                    root: self.root.clone(),
                    session: session.clone(),
                    artifacts: self.artifacts.clone(),
                    retention: self.retention.clone(),
                    quota: self.capture_quota.clone(),
                    retention_id: None,
                    created_at_ms: crate::engine::now_ms(),
                };
                let manager = crate::capture_jobs::CaptureManager::new(
                    backend,
                    CaptureLimits {
                        max_jobs: 1,
                        ..CaptureLimits::default()
                    },
                )?;
                let manager = Arc::new(Mutex::new(manager));
                let baseline = runtime_capture_observation(&session);
                let armed: ArmedCapture = manager
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .start(request.clone(), baseline, capture_clock_ms())?;
                jobs.insert(request.handle.clone(), manager.clone());
                drop(jobs);
                spawn_capture_observer(session, request.handle.clone(), manager);
                Ok(
                    json!({"status":"armed","capture":armed,"armed_before_return":true,"sampling_guarantee":"best_effort_not_every_transition"}),
                )
            }
            "status" | "cancel" | "materialize" | "release" => {
                let handle = args.handle.context("handle required")?;
                let manager = self
                    .captures
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .get(&handle)
                    .cloned();
                if let Some(manager) = manager {
                    let mut manager = manager
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if args.action == "cancel" {
                        return capture_status_projection(
                            manager.cancel(&handle)?,
                            args.response,
                            "cancelled",
                        );
                    }
                    if args.action == "materialize" {
                        let result = manager.materialize(
                            &handle,
                            args.frame_index.context("frame_index required")?,
                        )?;
                        let artifact_id = result.reference.clone();
                        return Ok(
                            json!({"status":"materialized","capture":result,"artifact_id":artifact_id}),
                        );
                    }
                    if args.action == "release" {
                        let mut status = manager.status(&handle)?;
                        manager.release(&handle)?;
                        status.coverage.restored_frames_unavailable =
                            status.coverage.restored_frames_unavailable.saturating_add(
                                status.frames.iter().filter(|frame| frame.available).count() as u64,
                            );
                        status.retained_bytes = 0;
                        for frame in &mut status.frames {
                            frame.available = false;
                        }
                        drop(manager);
                        self.captures
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .remove(&handle);
                        return capture_status_projection(status, args.response, "released");
                    }
                    if args.action == "status" {
                        let current = manager.status(&handle)?;
                        match service.active_session(&current.armed_baseline.session_id) {
                            Ok(session) => {
                                if let Err(error) = refresh_capture(
                                    &session,
                                    &mut manager,
                                    &handle,
                                    capture_clock_ms(),
                                ) {
                                    let _ = manager.terminate(
                                        &handle,
                                        CaptureState::Failed,
                                        capture_clock_ms(),
                                        format!("capture observer refresh failed: {error:#}"),
                                    );
                                }
                            }
                            Err(_) => {
                                manager.terminate(
                                    &handle,
                                    CaptureState::SessionClosed,
                                    capture_clock_ms(),
                                    "owning terminal session is no longer available",
                                )?;
                            }
                        }
                    }
                    return capture_status_projection(
                        manager.status(&handle)?,
                        args.response,
                        "status",
                    );
                }
                let path = capture_directory(&self.root, &handle).join("checkpoint.json");
                ensure!(path.is_file(), "capture_not_found");
                let mut checkpoint: CaptureCheckpoint = serde_json::from_value(read_json(&path)?)?;
                if !checkpoint.status.state.is_terminal() {
                    checkpoint.status.state = CaptureState::Interrupted;
                    checkpoint.status.updated_at_ms = capture_clock_ms();
                    checkpoint.status.failure =
                        Some("runtime restarted; active capture was not resumed".into());
                    checkpoint.status.coverage.restored_frames_unavailable =
                        checkpoint.status.frames.len() as u64;
                    checkpoint.status.retained_bytes = 0;
                    for frame in &mut checkpoint.status.frames {
                        frame.available = false;
                    }
                    crate::session::atomic_json_write(&path, &checkpoint)?;
                    let relative = path.strip_prefix(&self.root).unwrap_or(Path::new(""));
                    let entry_id = self
                        .retention
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .entries()
                        .iter()
                        .find(|entry| entry.relative_path == relative)
                        .map(|entry| entry.id.clone());
                    if let Some(entry_id) = entry_id {
                        self.retention
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .set_retention_at(
                                &entry_id,
                                RetentionClass::Failed,
                                false,
                                crate::engine::now_ms(),
                            )?;
                    }
                }
                if args.action == "cancel" || args.action == "release" {
                    return capture_status_projection(
                        checkpoint.status,
                        args.response,
                        "already_terminal",
                    );
                }
                if args.action == "materialize" {
                    bail!(
                        "capture_frame_unavailable: capture frames are unavailable after runtime restart"
                    )
                }
                capture_status_projection(checkpoint.status, args.response, "status")
            }
            _ => bail!("invalid_params: action must be start, status, cancel, or materialize"),
        }
    }

    pub(crate) fn stop_diagnostics_owner(&self, session_id: &str, run_id: &str) -> Result<usize> {
        let owner = crate::diagnostics::OwnerKey::new(session_id, run_id)?;
        self.diagnostics.stop_owner(&owner)
    }

    fn diagnostics(
        &self,
        service: &SessionService,
        args: crate::contracts::DiagnosticsArgs,
    ) -> Result<Value> {
        use crate::{contracts::DiagnosticAction, diagnostics::OutputStream};

        let session_id = args.session_id.clone();
        let run_id = args.run_id.clone();
        let session = service.active_session(&session_id)?;
        session.with_diagnostic_context(&run_id, |owner, context, workspace, redactor| {
            match args.action {
                DiagnosticAction::Start => {
                    let spec = LaunchSpec {
                        name: args.label,
                        program: args.program.context("invalid_params: program is required for start")?,
                        args: args.args,
                        // EffectiveContext supplies the trusted canonical cwd;
                        // the request has no cwd/environment/isolation fields.
                        cwd: String::new(),
                        env: Default::default(),
                        cols: 80,
                        rows: 24,
                        record_output: false,
                    };
                    let receipt = self.diagnostics.start(owner, &spec, context)?;
                    Ok(json!({
                        "process_id": receipt.id,
                        "label": receipt.label,
                        "backend": receipt.backend,
                        "argv_count": receipt.argv_count,
                        "detached_children_contained": receipt.detached_children_contained,
                        "limitation": if receipt.detached_children_contained { Value::Null } else { json!("host mode owns the verified direct child/process group; a process that detaches or escapes that group is not contained") }
                    }))
                }
                DiagnosticAction::Status => {
                    let process_id = args.process_id.context("invalid_params: process_id is required for status")?;
                    let status = self.diagnostics.status(&owner, &process_id)?;
                    Ok(diagnostic_status_value(status))
                }
                DiagnosticAction::Read => {
                    let process_id = args.process_id.context("invalid_params: process_id is required for read")?;
                    let stream = args.stream.context("invalid_params: stream is required for read")?;
                    let stream = match stream {
                        crate::contracts::DiagnosticStream::Stdout => OutputStream::Stdout,
                        crate::contracts::DiagnosticStream::Stderr => OutputStream::Stderr,
                    };
                    let mut chunk = self.diagnostics.read_output(
                        &owner,
                        &process_id,
                        stream,
                        args.cursor,
                        args.max_bytes,
                    )?;
                    chunk.text = redactor.text(&chunk.text);
                    Ok(json!({
                        "process_id": process_id,
                        "stream": match stream { OutputStream::Stdout => "stdout", OutputStream::Stderr => "stderr" },
                        "text": chunk.text,
                        "cursor": chunk.cursor,
                        "next_cursor": chunk.next_cursor,
                        "truncated_before_cursor": chunk.truncated_before_cursor,
                        "total_bytes_seen": chunk.total_bytes_seen,
                        "redaction": "known session environment and seeded credentials; not a general secret detector"
                    }))
                }
                DiagnosticAction::Cancel => {
                    let process_id = args.process_id.context("invalid_params: process_id is required for cancel")?;
                    let status = self.diagnostics.cancel(&owner, &process_id)?;
                    Ok(diagnostic_status_value(status))
                }
                DiagnosticAction::Stop => {
                    let process_id = args.process_id.context("invalid_params: process_id is required for stop")?;
                    let status = self.diagnostics.stop(&owner, &process_id)?;
                    Ok(diagnostic_status_value(status))
                }
                DiagnosticAction::SeedFile => {
                    let relative_path = args.relative_path.context("invalid_params: relative_path is required for seed_file")?;
                    let content = args.content.context("invalid_params: content is required for seed_file")?;
                    let workspace = workspace.context(
                        "diagnostics_seed_unsupported: this session has no selected copied workspace",
                    )?;
                    let receipt = self.diagnostics.seed_file(
                        &workspace,
                        &relative_path,
                        content.as_bytes(),
                        args.expected_sha256.as_deref(),
                    )?;
                    Ok(json!({
                        "session_id": session_id,
                        "run_id": run_id,
                        "relative_path": relative_path,
                        "bytes_written": receipt.bytes_written,
                        "sha256": receipt.sha256,
                        "overwritten": receipt.overwritten,
                        "workspace_only": true
                    }))
                }
            }
        })
    }

    pub fn handle(
        &self,
        service: &SessionService,
        operation: &str,
        params: Value,
    ) -> Result<Value> {
        let operation_name = operation.trim_start_matches("terminal_").to_owned();
        if operation_name == "recording" {
            return self.recording(service, parse(params)?);
        }
        let session_id = params
            .get("session_id")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let session_before = session_id
            .as_deref()
            .and_then(|id| service.active_session(id).ok());
        let previous_run_id = session_before
            .as_ref()
            .map(|session| session.summary().run_id);
        let before = session_before
            .as_ref()
            .map(|session| recording_identity(session));
        let mut result = self.handle_inner(service, &operation_name, params.clone())?;
        if let (Some(session_id), Some(baseline)) = (session_id.as_deref(), before) {
            self.record_confirmed_operation(
                service,
                session_id,
                &operation_name,
                &params,
                &result,
                baseline,
            )?;
        }
        if matches!(operation_name.as_str(), "restart" | "close")
            && let (Some(session_id), Some(run_id)) =
                (session_id.as_deref(), previous_run_id.as_deref())
        {
            self.finish_recording(session_id, run_id)?;
        }
        if let Some(object) = result.as_object_mut() {
            object.remove("_recording_pre_dispatch");
        }
        Ok(result)
    }

    fn handle_inner(
        &self,
        service: &SessionService,
        operation: &str,
        params: Value,
    ) -> Result<Value> {
        crate::validation::operation(operation, &params)?;
        match operation.trim_start_matches("terminal_") {
            "diagnostics" => self.diagnostics(service, parse(params)?),
            "crop" => self.crop(service, parse(params)?),
            "crop_compare" => self.crop_compare(service, parse(params)?),
            "capture_job" => self.capture_job(service, parse(params)?),
            "scroll_until" => self.scroll_until(service, parse(params)?),
            "app_adapter" => self.app_adapter(service, parse(params)?),
            "sequence" => self.sequence(service, parse(params)?),
            "recording" => {
                unreachable!("recording is handled before the shared operation dispatcher")
            }
            "finalize" => self.finalize(service, parse(params)?),
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
                crate::artifacts::validate_identity(&args.session_id, "session id")?;
                crate::artifacts::validate_identity(&args.artifact_id, "artifact id")?;
                let _lifecycle = match service.active_session(&args.session_id) {
                    Ok(session) => Some(session.retain_evidence()?),
                    Err(_) => None,
                };
                let record_path = self
                    .root
                    .join("sessions")
                    .join(&args.session_id)
                    .join("finalization.json");
                if record_path.is_file() {
                    let record: crate::finalization::FinalizationRecord =
                        serde_json::from_value(read_json(&record_path)?)?;
                    self.restore_final_evidence(&record)?;
                }
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
                let redactor = session.final_evidence_redactor()?;
                let path = self
                    .root
                    .join("sessions")
                    .join(&args.session_id)
                    .join("assertions.json");
                let (mut assertions, assertions_complete) =
                    crate::finalization::load_sanitized_assertions(&path, &redactor);
                if !assertions_complete {
                    bail!(
                        "assertion_store_invalid: existing assertion evidence is unreadable or invalid; refusing to overwrite it"
                    )
                }
                let items = assertions
                    .as_array_mut()
                    .context("assertion_store_invalid: stored report is not an array")?;
                if items.len() >= 64 {
                    bail!(
                        "assertion_quota_exceeded: this run already retains the maximum 64 complete assertion reports"
                    )
                }
                let frame = session.observe();
                let assertion = crate::assertions::evaluate(&frame, &args.assertion)?;
                let safe_assertion = redactor.value(&serde_json::to_value(&args.assertion)?);
                let safe_result = redactor.value(&serde_json::to_value(&assertion)?);
                items.push(json!({"assertion":safe_assertion,"result":safe_result,"frame_id":frame.frame_id,"run_id":frame.run_id}));
                crate::session::atomic_json_write(&path, &assertions)?;
                let mut result = encode(&assertion)?;
                if !assertion.passed {
                    let mut sanitized_frame = frame.clone();
                    redactor.frame(&mut sanitized_frame);
                    let evidence = session.screenshot_snapshot(&sanitized_frame)?;
                    let artifact = self.register_capture(&evidence)?;
                    let mut evidence_value = encode(evidence)?;
                    evidence_value["artifact_id"] = json!(artifact);
                    result["evidence"] = evidence_value;
                }
                Ok(result)
            }
            "diff" => {
                let args: DiffArgs = parse(params)?;
                self.compare_named(service, args, false)
            }
            "wait" => self.wait(service, parse(params)?),
            "locate" => self.locate(service, parse(params)?),
            "wait_fresh" => self.wait_fresh(service, parse(params)?),
            "actor" => {
                let args: ActorArgs = parse(params)?;
                service
                    .active_session(&args.session_id)?
                    .claim_actor(&args.actor_id, args.release)
            }
            "restart" => {
                let args: ActorArgs = parse(params)?;
                let session = service.active_session(&args.session_id)?;
                let previous_run = session.summary().run_id;
                let summary = session.agent_restart(&args.actor_id)?;
                self.stop_diagnostics_owner(&args.session_id, &previous_run)?;
                encode(summary)
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
                let mut pre_dispatch = Vec::new();
                let recording_active = self
                    .recordings
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .contains_key(&recording_key(
                        &args.session_id,
                        &args.context.expected_run_id,
                    ));
                for (i, action) in args.actions.iter().enumerate() {
                    let mut context = args.context.clone();
                    context.action_id = format!("{}:{i}", context.action_id);
                    if recording_active {
                        let mut receipt_context = context.clone();
                        if receipt_context.expected_viewport_revision.is_none() {
                            receipt_context.expected_viewport_revision = Some(
                                session.capabilities()["viewport_revision"]
                                    .as_u64()
                                    .context("current viewport revision missing for atomic recording receipt")?,
                            );
                        }
                        let (mut result, _, _, _) =
                            session.agent_action_with_receipt(&receipt_context, action)?;
                        let receipt = result
                            .get("receipt")
                            .cloned()
                            .context("atomic action receipt missing")?;
                        pre_dispatch.push(receipt["pre_dispatch"].clone());
                        if let Some(object) = result.as_object_mut() {
                            object.remove("receipt");
                        }
                        results.push(result);
                    } else {
                        results.push(session.agent_action(&context, action)?);
                    }
                }
                let mut response = json!({"accepted":true,"results":results});
                if recording_active {
                    response["_recording_pre_dispatch"] = json!(pre_dispatch);
                }
                Ok(response)
            }
            "scenario_run" => {
                let args: ScenarioArgs = parse(params)?;
                crate::contracts::validate_scenario(&args.scenario)?;
                let bound: Vec<String> = args
                    .scenario
                    .steps
                    .iter()
                    .filter_map(|step| step.params["session_id"].as_str())
                    .filter(|id| !id.contains("${"))
                    .map(str::to_owned)
                    .collect();
                let mut evidence_leases = Vec::new();
                for id in &bound {
                    if let Ok(session) = service.active_session(id) {
                        evidence_leases.push(session.retain_evidence()?);
                    }
                }
                let backend = Arc::new(ServiceBackend {
                    service: service.clone(),
                    sessions: Mutex::new(Vec::new()),
                    referenced: Mutex::new(bound),
                    actor_id: format!("scenario-{}", ulid::Ulid::generate()),
                    evidence_leases: Mutex::new(evidence_leases),
                    active_sequence: Mutex::new(None),
                    cancellation_requested: AtomicBool::new(false),
                });
                let agent = Arc::downgrade(&service.agent);
                let final_agent = agent.clone();
                let on_created: crate::scenario::ScenarioLifecycleHook = Arc::new(move |report| {
                    let agent = agent
                        .upgrade()
                        .context("automation service stopped before scenario registration")?;
                    agent.register_scenario_report(report)
                });
                let final_backend = Arc::downgrade(&backend);
                let on_finished: crate::scenario::ScenarioLifecycleHook = Arc::new(move |report| {
                    let agent = final_agent
                        .upgrade()
                        .context("automation service stopped before outcome finalization")?;
                    agent.register_scenario_report(report)?;
                    let backend = final_backend
                        .upgrade()
                        .context("scenario backend stopped before outcome finalization")?;
                    backend
                        .evidence_leases
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .clear();
                    backend.finalize_test_sessions(report)
                });
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

    fn recording(
        &self,
        service: &SessionService,
        args: crate::contracts::RecordingArgs,
    ) -> Result<Value> {
        use crate::manual_recording::{
            ManualRecorder, RecordedOperation, RecordedServiceEvent, RecordingOptions,
            RecordingSource,
        };
        crate::artifacts::validate_identity(&args.session_id, "session id")?;
        if args.recording_id.is_empty()
            || args.recording_id.len() > 64
            || !args
                .recording_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_".contains(&b))
        {
            bail!(
                "invalid_params: recording_id must contain 1..=64 alphanumeric, dash or underscore characters"
            )
        }
        match args.action.as_str() {
            "start" => {
                let session = service.active_session(&args.session_id)?;
                let summary = session.summary();
                if summary.run_id != args.run_id {
                    bail!("stale_run: recording must bind to the current session run")
                }
                if summary.state != "running" {
                    bail!("recording requires a running application session")
                }
                let key = recording_key(&args.session_id, &args.run_id);
                let mut recordings = self
                    .recordings
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                if recordings.contains_key(&key) {
                    bail!("recording_conflict: a recording is already active for this session run")
                }
                let mut redactor = None;
                session.with_diagnostic_context(&args.run_id, |_, _, _, value| {
                    redactor = Some(SessionRecordingRedactor(value));
                    Ok(())
                })?;
                let caps = session.capabilities();
                let workspace = if caps["launch"]["workspace"]["root"].as_str().is_some() {
                    crate::manual_recording::RecordedWorkspace::SelectedIndependentCopy
                } else {
                    crate::manual_recording::RecordedWorkspace::ExternalProfile
                };
                let launch = crate::manual_recording::LaunchMetadata {
                    program: summary.program.clone(),
                    args: summary.args.clone(),
                    cwd: None,
                    cols: summary.cols,
                    rows: summary.rows,
                    workspace,
                    external_environment_required: caps["launch"]["environment_sources"]
                        .as_object()
                        .is_some_and(|sources| {
                            sources.values().any(|source| {
                                source == "daemon_ambient" || source == "profile_secret_reference"
                            })
                        }),
                    external_auth_required: caps["launch"]["credential_directory_seed_count"]
                        .as_u64()
                        .unwrap_or(0)
                        > 0,
                };
                let mut recorder = ManualRecorder::start(
                    RecordingOptions {
                        recording_id: args.recording_id.clone(),
                        opted_in: args.opted_in,
                        record_text: args.record_text,
                        max_events: args.max_events,
                        max_bytes: args.max_bytes,
                    },
                    RecordingOwner,
                    redactor.context("session redactor missing")?,
                )?;
                let baseline = recording_identity(&session);
                recorder.record(RecordedServiceEvent {
                    order: 1,
                    timestamp_ms: crate::engine::now_ms() as u64,
                    source: RecordingSource::Mcp,
                    result_identity: baseline,
                    operation: RecordedOperation::Launch { launch },
                })?;
                let path = self
                    .root
                    .join("recordings")
                    .join(&args.session_id)
                    .join(format!("{}.json", args.recording_id));
                if path.exists() {
                    bail!("recording_id_conflict: an existing trace will not be overwritten")
                }
                crate::profiles::ensure_no_symlink_components(&path)?;
                crate::session::create_private_dir(
                    path.parent().context("recording path has no parent")?,
                )?;
                persist_recording(&path, &args.session_id, &args.run_id, &recorder.trace())?;
                self.retention
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .register(ManagedEntry::new(
                        path.strip_prefix(&self.root)?.to_path_buf(),
                        OwnerKind::Run,
                        RetentionClass::Interactive,
                        crate::engine::now_ms(),
                        "opt-in bounded sanitized manual recording",
                    ))?;
                recordings.insert(
                    key,
                    ActiveManualRecording {
                        session_id: args.session_id,
                        run_id: args.run_id,
                        recording_id: args.recording_id.clone(),
                        path,
                        recorder,
                        _session_lease: session.retain_evidence()?,
                    },
                );
                Ok(
                    json!({"recording_id":args.recording_id,"session_id":summary.session_id,"run_id":summary.run_id,"status":"recording","capture_surface":["MCP automation service: actor-guarded batch input, wait_ready and assert"],"unsupported_surfaces":["terminal_observe","browser input","legacy RPC input","sequences and other automation operations"]}),
                )
            }
            "status" | "stop" | "export" => {
                let key = recording_key(&args.session_id, &args.run_id);
                if args.action == "stop" {
                    {
                        let recordings = self
                            .recordings
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        if recordings
                            .get(&key)
                            .is_some_and(|active| active.recording_id != args.recording_id)
                        {
                            bail!(
                                "recording_identity_mismatch: recording id does not own this session run"
                            )
                        }
                    }
                    self.finish_recording(&args.session_id, &args.run_id)?;
                } else if args.action == "status" {
                    let recordings = self
                        .recordings
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if let Some(active) = recordings.get(&key) {
                        if active.recording_id != args.recording_id {
                            bail!(
                                "recording_identity_mismatch: recording id does not own this session run"
                            )
                        }
                        return encode(active.recorder.trace());
                    }
                }
                let path = self
                    .root
                    .join("recordings")
                    .join(&args.session_id)
                    .join(format!("{}.json", args.recording_id));
                crate::profiles::ensure_no_symlink_components(&path)?;
                let persisted: PersistedManualRecording =
                    serde_json::from_value(read_json(&path)?)?;
                if persisted.version != 1
                    || persisted.session_id != args.session_id
                    || persisted.run_id != args.run_id
                {
                    bail!(
                        "recording_identity_mismatch: trace owner does not match requested session/run"
                    )
                }
                let trace = persisted.trace;
                if trace.recording_id != args.recording_id {
                    bail!("recording_identity_mismatch: trace does not match requested recording")
                }
                if args.action == "export" {
                    encode(crate::manual_recording::export_trace(&trace)?)
                } else {
                    encode(trace)
                }
            }
            _ => bail!("invalid_params: recording action must be start, status, stop or export"),
        }
    }

    pub(crate) fn finish_recording(&self, session_id: &str, run_id: &str) -> Result<()> {
        let key = recording_key(session_id, run_id);
        if let Some(active) = self
            .recordings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&key)
        {
            if active.session_id != session_id || active.run_id != run_id {
                bail!("recording_identity_mismatch: active trace owner changed")
            }
            persist_recording(
                &active.path,
                &active.session_id,
                &active.run_id,
                &active.recorder.trace(),
            )?;
        }
        Ok(())
    }

    fn record_confirmed_operation(
        &self,
        service: &SessionService,
        session_id: &str,
        operation: &str,
        params: &Value,
        result: &Value,
        baseline: crate::locators::ObservationIdentity,
    ) -> Result<()> {
        use crate::manual_recording::{
            RecordedInput, RecordedOperation, RecordedSequenceStep, RecordedServiceEvent,
            RecordingSource,
        };
        let run_id = baseline.run_id.as_str();
        let key = recording_key(session_id, run_id);
        let mut recordings = self
            .recordings
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(active) = recordings.get_mut(&key) else {
            return Ok(());
        };
        if result.get("accepted") == Some(&Value::Bool(false)) {
            return Ok(());
        }
        if result.get("passed") == Some(&Value::Bool(false)) {
            active.recorder.mark_unsupported();
            return persist_recording(
                &active.path,
                &active.session_id,
                &active.run_id,
                &active.recorder.trace(),
            );
        }
        let op = match operation {
            "batch" => {
                let Some(actions) = params["actions"].as_array() else {
                    active.recorder.mark_unsupported();
                    return persist_recording(
                        &active.path,
                        &active.session_id,
                        &active.run_id,
                        &active.recorder.trace(),
                    );
                };
                let Some(context) =
                    serde_json::from_value::<ActionContext>(params["context"].clone()).ok()
                else {
                    active.recorder.mark_unsupported();
                    return persist_recording(
                        &active.path,
                        &active.session_id,
                        &active.run_id,
                        &active.recorder.trace(),
                    );
                };
                if result["results"].as_array().map_or(0, Vec::len) != actions.len() {
                    active.recorder.mark_unsupported();
                    return persist_recording(
                        &active.path,
                        &active.session_id,
                        &active.run_id,
                        &active.recorder.trace(),
                    );
                }
                let mut steps = Vec::new();
                for (index, action) in actions.iter().enumerate() {
                    let Ok(action) = serde_json::from_value::<AgentAction>(action.clone()) else {
                        active.recorder.mark_unsupported();
                        return persist_recording(
                            &active.path,
                            &active.session_id,
                            &active.run_id,
                            &active.recorder.trace(),
                        );
                    };
                    let mut action_context = context.clone();
                    action_context.action_id = format!("{}:{index}", context.action_id);
                    let Some(pre_dispatch) = result["_recording_pre_dispatch"]
                        .as_array()
                        .and_then(|identities| identities.get(index))
                        .cloned()
                    else {
                        active.recorder.mark_unsupported();
                        return persist_recording(
                            &active.path,
                            &active.session_id,
                            &active.run_id,
                            &active.recorder.trace(),
                        );
                    };
                    let Ok(pre_dispatch) = serde_json::from_value(pre_dispatch) else {
                        active.recorder.mark_unsupported();
                        return persist_recording(
                            &active.path,
                            &active.session_id,
                            &active.run_id,
                            &active.recorder.trace(),
                        );
                    };
                    let written_to_pty = !matches!(action, AgentAction::Resize { .. });
                    steps.push(RecordedSequenceStep::Input {
                        input: RecordedInput {
                            action,
                            context: action_context,
                            baseline: pre_dispatch,
                            written_to_pty,
                        },
                    });
                }
                RecordedOperation::Sequence { steps }
            }
            "observe" => RecordedOperation::Observation {
                identity: baseline.clone(),
                text: None,
            },
            "wait" => {
                if result["matched"] != true || result["timed_out"] == true {
                    active.recorder.mark_unsupported();
                    persist_recording(
                        &active.path,
                        &active.session_id,
                        &active.run_id,
                        &active.recorder.trace(),
                    )?;
                    return Ok(());
                }
                let Some(predicate) = serde_json::from_value(params["predicate"].clone()).ok()
                else {
                    active.recorder.mark_unsupported();
                    return persist_recording(
                        &active.path,
                        &active.session_id,
                        &active.run_id,
                        &active.recorder.trace(),
                    );
                };
                RecordedOperation::Wait {
                    predicate,
                    baseline: baseline.clone(),
                }
            }
            "assert" => {
                let Some(assertion) = serde_json::from_value(params["assertion"].clone()).ok()
                else {
                    active.recorder.mark_unsupported();
                    return persist_recording(
                        &active.path,
                        &active.session_id,
                        &active.run_id,
                        &active.recorder.trace(),
                    );
                };
                RecordedOperation::Assert {
                    assertion,
                    baseline: baseline.clone(),
                }
            }
            "wait_fresh" => {
                active.recorder.mark_unsupported();
                persist_recording(
                    &active.path,
                    &active.session_id,
                    &active.run_id,
                    &active.recorder.trace(),
                )?;
                return Ok(());
            }
            "sequence" | "scroll_until" | "capture_job" | "diagnostics" | "app_adapter" => {
                active.recorder.mark_unsupported();
                persist_recording(
                    &active.path,
                    &active.session_id,
                    &active.run_id,
                    &active.recorder.trace(),
                )?;
                return Ok(());
            }
            _ => return Ok(()),
        };
        let session = service.active_session(session_id)?;
        let identity = recording_identity(&session);
        let order = active
            .recorder
            .trace()
            .records
            .last()
            .map_or(1, |last| last.order.saturating_add(1));
        let timestamp_ms = active
            .recorder
            .trace()
            .records
            .last()
            .map_or(crate::engine::now_ms() as u64, |last| {
                (crate::engine::now_ms() as u64).max(last.timestamp_ms.saturating_add(1))
            });
        active.recorder.record(RecordedServiceEvent {
            order,
            timestamp_ms,
            source: RecordingSource::Mcp,
            result_identity: identity,
            operation: op,
        })?;
        persist_recording(
            &active.path,
            &active.session_id,
            &active.run_id,
            &active.recorder.trace(),
        )
    }

    fn scroll_until(
        &self,
        service: &SessionService,
        args: crate::contracts::ScrollUntilArgs,
    ) -> Result<Value> {
        use crate::validation::RequestError;
        let direction = match args.direction.as_str() {
            "up" => ScrollDirection::Up,
            "down" => ScrollDirection::Down,
            _ => return Err(RequestError::new("direction", "up or down").into()),
        };
        if args.timeout_ms == 0 || args.timeout_ms > 60_000 {
            return Err(RequestError::new("timeout_ms", "integer in 1..=60000 ms").into());
        }
        let session = service.active_session(&args.session_id)?;
        let caps = session.capabilities();
        let summary = session.summary();
        if summary.run_id != args.context.expected_run_id {
            return Err(
                RequestError::new("context.expected_run_id", "current run id")
                    .code("stale_run")
                    .into(),
            );
        }
        let viewport = caps["viewport_revision"].as_u64().unwrap_or(0);
        if args.context.expected_viewport_revision != Some(viewport) {
            return Err(RequestError::new(
                "context.expected_viewport_revision",
                "current viewport revision",
            )
            .code("stale_viewport")
            .into());
        }
        let generation = caps["controller"]["generation"].as_u64().unwrap_or(0);
        if args.context.controller_generation != Some(generation)
            || caps["controller"]["actor_id"].as_str() != Some(args.context.actor_id.as_str())
        {
            return Err(
                RequestError::new("context", "current claimed controller and generation")
                    .code("sequence_forbidden")
                    .into(),
            );
        }
        let (frame, revision, current_viewport) = session.observation_snapshot();
        let initial = crate::locators::ObservationIdentity {
            session_id: frame.session_id.clone(),
            run_id: frame.run_id.clone(),
            frame_id: frame.frame_id.clone(),
            observation_revision: revision,
            viewport_revision: current_viewport,
            cols: frame.cols,
            rows: frame.rows,
        };
        let binding =
            VerifiedScrollBinding::from_verified(direction, args.bindings, args.verified_at)?;
        let identity = SequenceIdentity {
            actor_id: args.context.actor_id.clone(),
            session_id: args.session_id.clone(),
            run_id: summary.run_id,
            sequence_id: args.context.action_id.clone(),
        };
        let request = ScrollRequest {
            identity,
            authority: SequenceAuthorityContext {
                expected_viewport_revision: viewport,
                expected_cols: summary.cols,
                expected_rows: summary.rows,
                controller_generation: generation,
            },
            direction,
            region: args.region,
            target: args.target,
            max_steps: args.max_steps,
            max_no_progress: args.max_no_progress,
            post_dispatch_observe_timeout: Duration::from_millis(250),
            deadline: Instant::now() + Duration::from_millis(args.timeout_ms),
        };
        let cancellation = SequenceCancellation::default();
        let mut backend = RuntimeAppScrollBackend {
            service: service.clone(),
        };
        let report =
            crate::app_scroll::scroll_until(&mut backend, request, &binding, &cancellation)?;
        let mut value = serde_json::to_value(report)?;
        value["initial_observation"] = serde_json::to_value(initial)?;
        value["scroll_space"] = json!(
            "application_viewport; distinct from terminal scrollback and browser page scroll"
        );
        Ok(value)
    }

    pub(crate) fn forget_adapters_for_session(&self, session_id: &str) {
        self.app_adapters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|_, adapter| {
                !adapter
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .references_terminal_session(session_id)
            });
    }

    fn app_adapter(
        &self,
        service: &SessionService,
        args: crate::contracts::AppAdapterArgs,
    ) -> Result<Value> {
        use crate::app_adapter::{BackendBinding, OwnedBackendProof, TuiAttachProof};
        if args.action == "bind" {
            {
                let mut adapters = self
                    .app_adapters
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                adapters.retain(|_, adapter| {
                    adapter
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner)
                        .binding_is_current()
                });
                if adapters.len() >= 64 {
                    bail!("adapter_capacity: at most 64 live app adapter bindings")
                }
            }
            let backend_id = args
                .backend_session_id
                .as_deref()
                .context("invalid_params: backend_session_id is required for bind")?;
            let tui_id = args
                .tui_session_id
                .as_deref()
                .context("invalid_params: tui_session_id is required for bind")?;
            if args.allowed_application_sessions.is_empty()
                || args.allowed_application_sessions.len() > 64
            {
                bail!("invalid_params: allowed_application_sessions must contain 1..64 IDs")
            }
            let backend = service.active_session(backend_id)?;
            let tui = service.active_session(tui_id)?;
            let backend_summary = backend.summary();
            let tui_summary = tui.summary();
            ensure!(
                backend_summary.state == "running" && tui_summary.state == "running",
                "adapter binding requires live backend and TUI sessions"
            );
            ensure!(
                backend_summary.cwd == tui_summary.cwd,
                "adapter binding rejected: backend and TUI workspaces differ"
            );
            let port = launch_port(&backend_summary.args)
                .context("adapter binding requires a backend launched with an explicit --port")?;
            let session_arg = tui_summary
                .args
                .windows(2)
                .find(|pair| pair[0] == "--session")
                .map(|pair| pair[1].as_str());
            ensure!(
                session_arg.is_some_and(|id| args
                    .allowed_application_sessions
                    .iter()
                    .any(|allowed| allowed == id)),
                "adapter binding requires the attached --session to be in the explicit application-session allowlist"
            );
            let attach = format!("http://127.0.0.1:{port}");
            ensure!(
                tui_summary.args.iter().any(|arg| arg.contains(&attach)),
                "adapter binding rejected: TUI launch does not name this exact loopback backend"
            );
            let backend_pid = backend_summary
                .process_group
                .context("backend process identity unavailable")?;
            ensure!(
                process_group_owns_loopback_listener(backend_pid, port)?,
                "adapter binding rejected: selected loopback port is not owned by the daemon-managed backend process group"
            );
            let selected_directory = std::fs::canonicalize(&backend_summary.cwd)?;
            let owned = OwnedBackendProof {
                owner_session_id: backend_id.to_owned(),
                owner_run_id: backend_summary.run_id.clone(),
                pid: u32::try_from(backend_pid).context("invalid backend PID")?,
                process_start_identity: process_start_identity(backend_pid)?,
                loopback_port: port,
                selected_directory,
            };
            let tui_proof = TuiAttachProof {
                terminal_session_id: tui_id.to_owned(),
                terminal_run_id: tui_summary.run_id,
                attached_backend: owned.clone(),
            };
            let binding = BackendBinding::from_owned(
                owned,
                tui_proof,
                args.allowed_application_sessions.into_iter().collect(),
            )?;
            let adapter_id = format!("app-{}", ulid::Ulid::generate());
            let verifier = Arc::new(RuntimeBindingVerifier {
                service: service.clone(),
            });
            let adapter = crate::app_adapter::SilicogenAdapter::new(
                binding,
                verifier,
                crate::app_adapter::AdapterLimits::default(),
            )?;
            self.app_adapters
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(adapter_id.clone(), Arc::new(Mutex::new(adapter)));
            return Ok(
                json!({"adapter_id":adapter_id,"status":"bound","evidence_scope":"allowlisted application sessions on the exact daemon-owned backend attached by this TUI run","ui_state":"unavailable_until_producer_connected"}),
            );
        }
        let id = args
            .adapter_id
            .as_deref()
            .context("invalid_params: adapter_id is required")?;
        let adapter = self
            .app_adapters
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(id)
            .cloned()
            .context("adapter_not_found: bind a live backend/TUI pair first")?;
        let app_session = args.application_session_id.clone();
        let producer_instance = args.producer_instance.clone();
        let baseline_handle = args.baseline_handle.clone();
        let predicate = args.predicate.clone();
        let action = args.action.clone();
        let timeout = args.timeout_ms.unwrap_or(5000);
        if timeout == 0 || timeout > 60_000 {
            bail!("invalid_params: timeout_ms must be 1..=60000")
        }
        let result = std::thread::Builder::new()
            .name("app-adapter-read".into())
            .spawn(move || -> Result<Value> {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                let cancellation = SequenceCancellation::default();
                let deadline = Instant::now() + Duration::from_millis(timeout);
                let mut adapter = adapter
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                runtime.block_on(async move {
                    match action.as_str() {
                        "messages" | "transcript" => Ok(serde_json::to_value(
                            adapter
                                .transcript(
                                    app_session
                                        .as_deref()
                                        .context("application_session_id is required")?,
                                    deadline,
                                    &cancellation,
                                )
                                .await?,
                        )?),
                        "session" => Ok(serde_json::to_value(
                            adapter
                                .session(
                                    app_session
                                        .as_deref()
                                        .context("application_session_id is required")?,
                                    deadline,
                                    &cancellation,
                                )
                                .await?,
                        )?),
                        "status" => Ok(serde_json::to_value(
                            adapter
                                .status(
                                    app_session
                                        .as_deref()
                                        .context("application_session_id is required")?,
                                    deadline,
                                    &cancellation,
                                )
                                .await?,
                        )?),
                        "events" => Ok(serde_json::to_value(
                            adapter.read_events(deadline, &cancellation).await?,
                        )?),
                        "ui_state" => Ok(serde_json::to_value(
                            adapter
                                .ui_state(producer_instance.as_deref(), deadline, &cancellation)
                                .await?,
                        )?),
                        "message_baseline" => {
                            let session = app_session
                                .as_deref()
                                .context("application_session_id is required")?;
                            let (handle, evidence_sequence) = adapter
                                .create_message_baseline(session, deadline, &cancellation)
                                .await?;
                            Ok(json!({"baseline_handle":handle,"application_session_id":session,"binding_generation":adapter.binding_generation(),"evidence_sequence":evidence_sequence,"expires_in_ms":300000}))
                        }
                        "semantic_wait" => {
                            let handle = baseline_handle
                                .as_deref()
                                .context("baseline_handle is required")?;
                            let predicate = predicate
                                .as_ref()
                                .context("predicate is required")?;
                            let predicate = match predicate {
                                crate::contracts::SemanticPredicateArgs::TurnCompleted { user_message_id } => crate::app_adapter::SemanticPredicate::TurnCompleted { user_message_id: user_message_id.clone() },
                                crate::contracts::SemanticPredicateArgs::ToolCompleted { user_message_id, call_id, tool } => crate::app_adapter::SemanticPredicate::ToolCompleted { user_message_id: user_message_id.clone(), call_id: call_id.clone(), tool: tool.clone() },
                            };
                            Ok(serde_json::to_value(adapter.wait_semantic(handle, &predicate, deadline, &cancellation).await?)?)
                        }
                        _ => bail!("invalid_params: unsupported app adapter action"),
                    }
                })
            })
            .context("could not start bounded app adapter read")?
            .join()
            .map_err(|_| anyhow::anyhow!("app adapter worker panicked"))??;
        Ok(result)
    }
}

fn recording_key(session_id: &str, run_id: &str) -> String {
    format!("{session_id}:{run_id}")
}

fn recording_identity(session: &Session) -> ObservationIdentity {
    let (frame, observation_revision, viewport_revision) = session.observation_snapshot();
    ObservationIdentity {
        session_id: frame.session_id,
        run_id: frame.run_id,
        frame_id: frame.frame_id,
        observation_revision,
        viewport_revision,
        cols: frame.cols,
        rows: frame.rows,
    }
}

fn persist_recording(
    path: &Path,
    session_id: &str,
    run_id: &str,
    trace: &crate::manual_recording::RecordingTrace,
) -> Result<()> {
    crate::session::atomic_json_write(
        path,
        &PersistedManualRecording {
            version: 1,
            session_id: session_id.to_owned(),
            run_id: run_id.to_owned(),
            trace: trace.clone(),
        },
    )
}

fn diagnostic_status_value(status: crate::diagnostics::Status) -> Value {
    use crate::diagnostics::{CaptureGapReason, Lifecycle, OutputStream};
    json!({
        "process_id": status.id,
        "lifecycle": match status.lifecycle {
            Lifecycle::Running => "running",
            Lifecycle::Exited => "exited",
            Lifecycle::TimedOut => "timed_out",
            Lifecycle::Cancelled => "cancelled",
            Lifecycle::OutputLimitExceeded => "output_limit_exceeded",
        },
        "exit_code": status.exit_code,
        "elapsed_ms": status.elapsed_ms,
        "output_bytes": status.output_bytes,
        "capture_complete": status.capture_complete,
        "capture_gaps": status.capture_gaps.into_iter().map(|stream| match stream {
            OutputStream::Stdout => "stdout",
            OutputStream::Stderr => "stderr",
        }).collect::<Vec<_>>(),
        "capture_gap_reason": status.capture_gap_reason.map(|reason| match reason {
            CaptureGapReason::OutputLimit => "output_limit",
            CaptureGapReason::PipeStillOpenAtStop => "pipe_still_open_at_stop",
            CaptureGapReason::DrainDeadline => "drain_deadline",
        }),
        "backend": status.backend,
        "detached_children_contained": status.detached_children_contained,
        "limitation": if status.detached_children_contained { Value::Null } else { json!("host mode owns the verified direct child/process group; detached descendants are not contained") }
    })
}

fn launch_port(args: &[String]) -> Option<u16> {
    let values = args
        .iter()
        .enumerate()
        .filter_map(|(i, arg)| {
            arg.strip_prefix("--port=").or_else(|| {
                (arg == "--port")
                    .then(|| args.get(i + 1).map(String::as_str))
                    .flatten()
            })
        })
        .collect::<Vec<_>>();
    if values.len() != 1 {
        return None;
    }
    values[0].parse::<u16>().ok().filter(|port| *port != 0)
}

fn process_start_identity(pid: i32) -> Result<u64> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .context("backend process is no longer live")?;
    let value = stat
        .rsplit_once(')')
        .context("invalid Linux process stat")?
        .1
        .split_whitespace()
        .nth(19)
        .context("missing process start identity")?;
    Ok(value.parse()?)
}

fn process_group_owns_loopback_listener(group_id: i32, port: u16) -> Result<bool> {
    let mut listener_inodes = std::collections::BTreeSet::new();
    for line in std::fs::read_to_string("/proc/net/tcp")?.lines().skip(1) {
        let fields = line.split_whitespace().collect::<Vec<_>>();
        if fields.len() < 10 || fields[3] != "0A" {
            continue;
        }
        let Some((address, port_hex)) = fields[1].split_once(':') else {
            continue;
        };
        if address == "0100007F"
            && u16::from_str_radix(port_hex, 16).ok() == Some(port)
            && let Ok(inode) = fields[9].parse::<u64>()
        {
            listener_inodes.insert(inode);
        }
    }
    if listener_inodes.is_empty() {
        return Ok(false);
    }
    for entry in std::fs::read_dir("/proc")?.flatten() {
        let Some(_pid) = entry
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<i32>().ok())
        else {
            continue;
        };
        let Ok(stat) = std::fs::read_to_string(entry.path().join("stat")) else {
            continue;
        };
        let Some(group) = stat
            .rsplit_once(')')
            .and_then(|(_, rest)| rest.split_whitespace().nth(2))
            .and_then(|value| value.parse::<i32>().ok())
        else {
            continue;
        };
        if group != group_id {
            continue;
        }
        let Ok(descriptors) = std::fs::read_dir(entry.path().join("fd")) else {
            continue;
        };
        for descriptor in descriptors.flatten() {
            let Ok(target) = std::fs::read_link(descriptor.path()) else {
                continue;
            };
            let value = target.to_string_lossy();
            if let Some(inode) = value
                .strip_prefix("socket:[")
                .and_then(|value| value.strip_suffix(']'))
                .and_then(|value| value.parse::<u64>().ok())
                && listener_inodes.contains(&inode)
            {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

struct RuntimeBindingVerifier {
    service: SessionService,
}
impl crate::app_adapter::BindingVerifier for RuntimeBindingVerifier {
    fn verify_current(&self, binding: &crate::app_adapter::BackendBinding) -> Result<()> {
        let owner = binding.owned_backend();
        let backend = self.service.active_session(&owner.owner_session_id)?;
        let tui = self
            .service
            .active_session(&binding.tui_attachment().terminal_session_id)?;
        let backend_summary = backend.summary();
        let tui_summary = tui.summary();
        ensure!(
            backend_summary.state == "running" && backend_summary.run_id == owner.owner_run_id,
            "owned backend run is no longer current"
        );
        ensure!(
            backend_summary.process_group == Some(owner.pid as i32)
                && process_start_identity(owner.pid as i32)? == owner.process_start_identity,
            "owned backend process identity changed"
        );
        ensure!(
            launch_port(&backend_summary.args) == Some(owner.loopback_port),
            "owned backend port changed"
        );
        ensure!(
            process_group_owns_loopback_listener(owner.pid as i32, owner.loopback_port)?,
            "owned backend process group no longer owns the selected loopback listener"
        );
        ensure!(
            std::fs::canonicalize(&backend_summary.cwd)? == owner.selected_directory
                && tui_summary.cwd == backend_summary.cwd,
            "owned backend workspace changed"
        );
        ensure!(
            tui_summary.state == "running"
                && tui_summary.run_id == binding.tui_attachment().terminal_run_id,
            "attached TUI run is no longer current"
        );
        let attach = format!("http://127.0.0.1:{}", owner.loopback_port);
        ensure!(
            tui_summary.args.iter().any(|arg| arg.contains(&attach)),
            "TUI is no longer attached to the owned backend"
        );
        Ok(())
    }
}

struct ServiceBackend {
    service: SessionService,
    sessions: Mutex<Vec<String>>,
    referenced: Mutex<Vec<String>>,
    actor_id: String,
    evidence_leases: Mutex<Vec<crate::session::EvidenceLease>>,
    active_sequence: Mutex<Option<String>>,
    cancellation_requested: AtomicBool,
}
impl ServiceBackend {
    fn finalize_test_sessions(&self, report: &crate::scenario::ScenarioReport) -> Result<()> {
        use crate::{finalization::Outcome, scenario::ScenarioStatus};
        let outcome = match report.status {
            ScenarioStatus::Passed if !report.backend_work_may_continue => Outcome::Passed,
            ScenarioStatus::Cancelled => Outcome::Cancelled,
            ScenarioStatus::Interrupted => Outcome::Interrupted,
            ScenarioStatus::Failed | ScenarioStatus::TimedOut => Outcome::Failed,
            _ => Outcome::Unknown,
        };
        let sessions = self
            .sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        for id in sessions {
            let diagnostics = read_json(
                &self
                    .service
                    .runtime_root()
                    .join("sessions")
                    .join(&id)
                    .join("launch-diagnostics.json"),
            )?;
            if diagnostics["test_policy"].is_object() {
                let summary = self
                    .service
                    .handle(RpcRequest::Status {
                        session_id: id.clone(),
                    })?
                    .0;
                self.service.agent.finalize(
                    &self.service,
                    crate::finalization::FinalizeArgs {
                        session_id: id,
                        expected_run_id: summary["run_id"]
                            .as_str()
                            .context("run id missing")?
                            .into(),
                        actor_id: Some(self.actor_id.clone()),
                        outcome,
                    },
                )?;
            }
        }
        Ok(())
    }
}
struct RuntimeSequenceBackend {
    service: SessionService,
    baselines: Arc<Mutex<HashMap<String, SequenceBaselineEntry>>>,
}

struct RuntimeAppScrollBackend {
    service: SessionService,
}

impl SequenceBackend for RuntimeAppScrollBackend {
    fn dispatch(
        &mut self,
        identity: &SequenceActionIdentity,
        action: &AgentAction,
        _deadline: Instant,
        _cancellation: &SequenceCancellation,
    ) -> DispatchResult {
        let session = match self.service.active_session(&identity.session_id) {
            Ok(session) => session,
            Err(error) => {
                return DispatchResult::Rejected {
                    reason: error.to_string(),
                };
            }
        };
        let context = ActionContext {
            actor_id: identity.actor_id.clone(),
            action_id: identity.action_id.clone(),
            expected_run_id: identity.run_id.clone(),
            expected_viewport_revision: Some(identity.expected_viewport.revision),
            controller_generation: Some(identity.controller_generation),
        };
        match session.agent_action_with_receipt(&context, action) {
            Ok((value, Some(_frame), _, _)) => {
                match serde_json::from_value(value["receipt"]["pre_dispatch"].clone()) {
                    Ok(pre_dispatch) => match serde_json::from_value(
                        value["receipt"]["post_dispatch_viewport"].clone(),
                    ) {
                        Ok(post_dispatch_viewport) => DispatchResult::Written {
                            pre_dispatch,
                            post_dispatch_viewport,
                        },
                        Err(error) => DispatchResult::Unknown {
                            reason: format!("invalid viewport receipt: {error}"),
                        },
                    },
                    Err(error) => DispatchResult::Unknown {
                        reason: format!("invalid dispatch receipt: {error}"),
                    },
                }
            }
            Ok((_, None, _, _)) => DispatchResult::Unknown {
                reason: "scroll action receipt baseline is no longer available".into(),
            },
            Err(error) => {
                let reason = error.to_string();
                if [
                    "stale_run:",
                    "stale_viewport:",
                    "stale_actor:",
                    "control_held_by_actor:",
                    "application process has exited",
                ]
                .iter()
                .any(|prefix| reason.contains(prefix))
                {
                    DispatchResult::Rejected { reason }
                } else {
                    DispatchResult::Unknown { reason }
                }
            }
        }
    }
    fn barrier(
        &mut self,
        _: &SequenceIdentity,
        _: &ObservationIdentity,
        _: &BarrierCondition,
        _: Instant,
        _: &SequenceCancellation,
    ) -> BarrierResult {
        BarrierResult::Failed {
            reason: "application scroll uses bounded frame observations, not sequence barriers"
                .into(),
        }
    }
    fn delay(&mut self, _: Duration, _: Instant, _: &SequenceCancellation) -> DelayResult {
        DelayResult::Elapsed
    }
}

impl AppScrollBackend for RuntimeAppScrollBackend {
    fn observe_application(
        &mut self,
        identity: &SequenceIdentity,
        after: Option<&ObservationIdentity>,
        deadline: Instant,
        cancellation: &SequenceCancellation,
    ) -> Result<ScrollObservation> {
        let session = self.service.active_session(&identity.session_id)?;
        if session.summary().run_id != identity.run_id {
            anyhow::bail!("stale_run: application scroll targets another run")
        }
        let (mut frame, mut revision, mut viewport_revision) = session.observation_snapshot();
        if let Some(after) = after {
            while revision <= after.observation_revision
                && Instant::now() < deadline
                && !cancellation.is_cancelled()
            {
                session.wait_for_change(
                    revision,
                    deadline
                        .saturating_duration_since(Instant::now())
                        .min(Duration::from_millis(25)),
                );
                (frame, revision, viewport_revision) = session.observation_snapshot();
            }
        }
        Ok(ScrollObservation {
            frame,
            observation_revision: revision,
            viewport_revision,
        })
    }
}

impl RuntimeSequenceBackend {
    fn new(
        service: SessionService,
        baselines: Arc<Mutex<HashMap<String, SequenceBaselineEntry>>>,
    ) -> Self {
        Self { service, baselines }
    }
}

impl SequenceBackend for RuntimeSequenceBackend {
    fn dispatch(
        &mut self,
        identity: &SequenceActionIdentity,
        action: &AgentAction,
        _deadline: Instant,
        _cancellation: &SequenceCancellation,
    ) -> DispatchResult {
        let session = match self.service.active_session(&identity.session_id) {
            Ok(session) => session,
            Err(error) => {
                return DispatchResult::Rejected {
                    reason: error.to_string(),
                };
            }
        };
        let context = ActionContext {
            actor_id: identity.actor_id.clone(),
            action_id: identity.action_id.clone(),
            expected_run_id: identity.run_id.clone(),
            expected_viewport_revision: Some(identity.expected_viewport.revision),
            controller_generation: Some(identity.controller_generation),
        };
        match session.agent_action_with_receipt(&context, action) {
            Ok((value, Some(frame), observation_revision, viewport_revision)) => {
                let pre_dispatch: ObservationIdentity =
                    match serde_json::from_value(value["receipt"]["pre_dispatch"].clone()) {
                        Ok(identity) => identity,
                        Err(error) => {
                            return DispatchResult::Unknown {
                                reason: format!("invalid atomic dispatch receipt: {error}"),
                            };
                        }
                    };
                match crate::wait_engine::WaitBaseline::capture(
                    &frame,
                    observation_revision,
                    viewport_revision,
                ) {
                    Ok(baseline) => {
                        let mut slots = self
                            .baselines
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner);
                        let Some(slot) = slots.get_mut(&identity.sequence_id) else {
                            return DispatchResult::Unknown { reason: "sequence baseline reservation disappeared after PTY input was accepted".into() };
                        };
                        let retained = baseline.retained_bytes();
                        if retained > MAX_SEQUENCE_BASELINE_BYTES {
                            return DispatchResult::Unknown { reason: "baseline exceeds reserved sequence memory after PTY input was accepted".into() };
                        }
                        slot.baseline = Some(baseline);
                        slot.retained_bytes = retained;
                    }
                    Err(error) => {
                        return DispatchResult::Unknown {
                            reason: format!(
                                "dispatch accepted but baseline retention failed: {error}"
                            ),
                        };
                    }
                }
                let post: SequenceViewport = match serde_json::from_value(
                    value["receipt"]["post_dispatch_viewport"].clone(),
                ) {
                    Ok(viewport) => viewport,
                    Err(error) => {
                        return DispatchResult::Unknown {
                            reason: format!("invalid post-dispatch viewport receipt: {error}"),
                        };
                    }
                };
                DispatchResult::Written {
                    pre_dispatch,
                    post_dispatch_viewport: post,
                }
            }
            Ok((_, None, _, _)) => DispatchResult::Unknown {
                reason:
                    "idempotent action result exists but its atomic baseline is no longer available"
                        .into(),
            },
            Err(error) => {
                let message = error.to_string();
                let definitely_rejected = [
                    "stale_run:",
                    "stale_viewport:",
                    "stale_actor:",
                    "control_held_by_actor:",
                    "invalid_action:",
                    "session_finalized:",
                    "application process has exited",
                ]
                .iter()
                .any(|prefix| message.contains(prefix));
                if definitely_rejected {
                    DispatchResult::Rejected { reason: message }
                } else {
                    DispatchResult::Unknown { reason: message }
                }
            }
        }
    }

    fn barrier(
        &mut self,
        identity: &SequenceIdentity,
        baseline_identity: &ObservationIdentity,
        condition: &BarrierCondition,
        deadline: Instant,
        cancellation: &SequenceCancellation,
    ) -> BarrierResult {
        let session = match self.service.active_session(&identity.session_id) {
            Ok(session) => session,
            Err(_) => return BarrierResult::StaleRun,
        };
        if session.summary().run_id != identity.run_id {
            return BarrierResult::StaleRun;
        }
        let baseline =
            if let Some(baseline) = self
                .baselines
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .get(&identity.sequence_id)
                .and_then(|entry| entry.baseline.as_ref())
                .filter(|baseline| baseline.identity == *baseline_identity)
            {
                baseline.clone()
            } else {
                match self
                    .service
                    .resolve_wait_baseline(&identity.session_id, baseline_identity)
                {
                    Ok((baseline, _)) => baseline,
                    Err(_) => return BarrierResult::Failed {
                        reason:
                            "barrier baseline unavailable; provide a retained observation identity"
                                .into(),
                    },
                }
            };
        enum Evaluator {
            Fresh(WaitEvaluator),
            Predicate(PredicateEvaluator),
        }
        let mut evaluator = match condition {
            BarrierCondition::Fresh(spec) => {
                match WaitEvaluator::new(spec.clone(), baseline, deadline) {
                    Ok(evaluator) => Evaluator::Fresh(evaluator),
                    Err(error) => {
                        return BarrierResult::Failed {
                            reason: error.to_string(),
                        };
                    }
                }
            }
            BarrierCondition::Predicate(spec) => match PredicateEvaluator::new(spec.clone()) {
                Ok(evaluator) => Evaluator::Predicate(evaluator),
                Err(error) => {
                    return BarrierResult::Failed {
                        reason: error.to_string(),
                    };
                }
            },
        };
        let poll_deadline = Instant::now() + Duration::from_millis(250);
        let result = execute_session_wait(
            &session,
            deadline,
            Some(poll_deadline),
            || cancellation.is_cancelled(),
            |frame, revision, viewport_revision, now| {
                use crate::wait_engine::WaitOutcome;
                let outcome = match &mut evaluator {
                    Evaluator::Fresh(evaluator) => {
                        match evaluator.observe(frame, revision, viewport_revision, now) {
                            WaitOutcome::Matched(_) => Some(BarrierResult::Matched {
                                observation: ObservationIdentity {
                                    session_id: frame.session_id.clone(),
                                    run_id: frame.run_id.clone(),
                                    frame_id: frame.frame_id.clone(),
                                    observation_revision: revision,
                                    viewport_revision,
                                    cols: frame.cols,
                                    rows: frame.rows,
                                },
                            }),
                            WaitOutcome::StaleBaseline => Some(BarrierResult::StaleRun),
                            WaitOutcome::IncompatibleViewport => {
                                Some(BarrierResult::IncompatibleViewport)
                            }
                            WaitOutcome::Cancelled => Some(BarrierResult::Cancelled),
                            WaitOutcome::DeadlineExceeded => Some(BarrierResult::DeadlineExceeded),
                            WaitOutcome::Pending => None,
                        }
                    }
                    Evaluator::Predicate(evaluator) => {
                        let result = evaluator.observe(frame, now);
                        if result.matched {
                            Some(BarrierResult::Matched {
                                observation: ObservationIdentity {
                                    session_id: frame.session_id.clone(),
                                    run_id: frame.run_id.clone(),
                                    frame_id: frame.frame_id.clone(),
                                    observation_revision: revision,
                                    viewport_revision,
                                    cols: frame.cols,
                                    rows: frame.rows,
                                },
                            })
                        } else if frame.run_id != identity.run_id {
                            Some(BarrierResult::StaleRun)
                        } else if viewport_revision != baseline_identity.viewport_revision {
                            Some(BarrierResult::IncompatibleViewport)
                        } else if now >= deadline {
                            Some(BarrierResult::DeadlineExceeded)
                        } else {
                            None
                        }
                    }
                };
                Ok(WaitStep {
                    value: outcome.clone().unwrap_or(BarrierResult::Pending),
                    terminal: outcome.is_some(),
                    next_wakeup: None,
                })
            },
        );
        match result {
            Ok((result, WaitStop::Cancelled)) => {
                if cancellation.is_cancelled() {
                    BarrierResult::Cancelled
                } else {
                    result
                }
            }
            Ok((result, _)) => result,
            Err(error) => BarrierResult::Failed {
                reason: error.to_string(),
            },
        }
    }

    fn delay(
        &mut self,
        duration: Duration,
        deadline: Instant,
        cancellation: &SequenceCancellation,
    ) -> DelayResult {
        let until = Instant::now() + duration;
        loop {
            if cancellation.is_cancelled() {
                return DelayResult::Cancelled;
            }
            let now = Instant::now();
            if now >= deadline {
                return DelayResult::DeadlineExceeded;
            }
            if now >= until {
                return DelayResult::Elapsed;
            }
            std::thread::sleep(
                (until.min(deadline).saturating_duration_since(now)).min(Duration::from_millis(10)),
            );
        }
    }
}

impl ScenarioBackend for ServiceBackend {
    fn execute(&self, operation: &str, mut params: Value) -> Result<Value> {
        let operation = operation.trim_start_matches("terminal_");
        crate::contracts::validate_scenario_params(operation, params.clone())?;
        let response = params
            .as_object_mut()
            .and_then(|object| object.remove("response"))
            .filter(|value| !value.is_null())
            .map(crate::validation::decode::<crate::projection::ResponseOptions>)
            .transpose()?;
        if response.is_some()
            && !["observe", "cells", "wait", "diff", "capture_job"].contains(&operation)
        {
            return Err(crate::validation::RequestError::new(
                "response",
                "projection-supported observation operation",
            )
            .code("unsupported_response")
            .into());
        }
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
            "scroll_until" => {
                let id = id.context("session_id required")?;
                let session = self.service.active_session(&id)?;
                let claimed = session.claim_actor(&self.actor_id, false)?;
                let caps = session.capabilities();
                params["context"]["actor_id"] = json!(self.actor_id);
                params["context"]["expected_run_id"] = caps["session"]["run_id"].clone();
                params["context"]["expected_viewport_revision"] = caps["viewport_revision"].clone();
                params["context"]["controller_generation"] = claimed["generation"].clone();
                self.service
                    .agent
                    .scroll_until(&self.service, parse(params)?)
            }
            "finalize" => {
                params["actor_id"] = json!(self.actor_id);
                self.service.agent.finalize(&self.service, parse(params)?)
            }
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
                self.evidence_leases
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .push(
                        self.service
                            .active_session(
                                result["session_id"]
                                    .as_str()
                                    .context("launch response lacks session id")?,
                            )?
                            .retain_evidence()?,
                    );
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
            "observe" | "screenshot" | "status" | "cells" => {
                let id = id.context("invalid_params: session_id required")?;
                let request = match operation {
                    "observe" => RpcRequest::Observe { session_id: id },
                    "screenshot" => RpcRequest::Screenshot { session_id: id },
                    "cells" => {
                        let args: crate::contracts::CellsArgs = crate::validation::decode(params)?;
                        RpcRequest::Cells {
                            session_id: id,
                            row: args.row,
                            col: args.col,
                            rows: args.rows,
                            cols: args.cols,
                        }
                    }
                    _ => RpcRequest::Status { session_id: id },
                };
                let request = if response.is_some() {
                    RpcRequest::Contract {
                        revision: 2,
                        request: Box::new(request),
                        response,
                    }
                } else {
                    request
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
            "sequence" => {
                let mut args: SequenceArgs = crate::validation::decode(params)?;
                if args.action != "start" {
                    bail!("scenario sequence step requires action=start")
                }
                let mut request = args.request.take().context("sequence request required")?;
                if args.actor_id.as_deref() != Some(request.identity.actor_id.as_str()) {
                    bail!(
                        "sequence_forbidden: supplied scenario actor_id must match request identity"
                    )
                }
                let session = self.service.active_session(&request.identity.session_id)?;
                let claimed = session.claim_actor(&self.actor_id, false)?;
                request.identity.actor_id = self.actor_id.clone();
                request.authority.controller_generation =
                    claimed["generation"].as_u64().unwrap_or(0);
                let start = self.service.agent.handle(
                    &self.service,
                    "sequence",
                    json!({"action":"start","actor_id":self.actor_id,"request":request}),
                )?;
                let sequence_id = start["sequence_id"]
                    .as_str()
                    .context("sequence start omitted id")?
                    .to_owned();
                self.cancellation_requested.store(false, Ordering::Release);
                *self
                    .active_sequence
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(sequence_id.clone());
                loop {
                    if self.cancellation_requested.load(Ordering::Acquire) {
                        let _ = self.service.agent.handle(&self.service, "sequence", json!({"action":"cancel","sequence_id":sequence_id,"actor_id":self.actor_id}));
                        let deadline = Instant::now() + Duration::from_secs(2);
                        while Instant::now() < deadline {
                            if let Ok(report) = crate::sequences::SequenceManager::read_report(
                                self.service
                                    .runtime_root()
                                    .join("sequences/sequence-journal.json"),
                                &sequence_id,
                            ) && !matches!(
                                report.status,
                                SequenceStatus::Pending | SequenceStatus::Running
                            ) {
                                break;
                            }
                            std::thread::sleep(Duration::from_millis(20));
                        }
                        *self
                            .active_sequence
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
                        bail!("scenario sequence cancelled")
                    }
                    let report = crate::sequences::SequenceManager::read_report(
                        self.service
                            .runtime_root()
                            .join("sequences/sequence-journal.json"),
                        &sequence_id,
                    )?;
                    match report.status {
                        crate::sequences::SequenceStatus::Pending => {
                            self.service.agent.handle(&self.service, "sequence", json!({"action":"resume","sequence_id":sequence_id,"actor_id":self.actor_id}))?;
                            std::thread::sleep(Duration::from_millis(20));
                        }
                        crate::sequences::SequenceStatus::Running => {
                            std::thread::sleep(Duration::from_millis(20))
                        }
                        _ => {
                            *self
                                .active_sequence
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner) = None;
                            let retained_bytes = self
                                .service
                                .agent
                                .sequence_baselines
                                .lock()
                                .unwrap_or_else(std::sync::PoisonError::into_inner)
                                .get(&sequence_id)
                                .map(|entry| entry.retained_bytes);
                            return project_sequence_report(report, args.response, retained_bytes);
                        }
                    }
                }
            }
            "capture_job" => {
                let args: CaptureJobArgs = crate::validation::decode(params.clone())?;
                if let Some(session_id) = args
                    .request
                    .as_ref()
                    .map(|request| request.authority.session_id.clone())
                {
                    let mut referenced = self
                        .referenced
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    if !referenced.contains(&session_id) {
                        referenced.push(session_id);
                    }
                }
                if let Some(response) = response {
                    params["response"] = serde_json::to_value(response)?;
                }
                self.service
                    .agent
                    .handle(&self.service, "capture_job", params)
            }
            "restart" => {
                let session = self
                    .service
                    .active_session(&id.context("session_id required")?)?;
                let previous_run = session.summary().run_id;
                let summary = session.agent_restart(&self.actor_id)?;
                self.service
                    .agent
                    .stop_diagnostics_owner(&session.id, &previous_run)?;
                encode(summary)
            }
            "close" => {
                let id = id.context("session_id required")?;
                let session = self.service.active_session(&id)?;
                session.claim_actor(&self.actor_id, true)?;
                Ok(self.service.handle(RpcRequest::Close { session_id: id })?.0)
            }
            _ => {
                if operation == "wait" && params.get("poll_ms").is_none() {
                    params["poll_ms"] = json!(100);
                }
                if response.is_some() {
                    return Ok(self
                        .service
                        .handle(RpcRequest::Contract {
                            revision: 2,
                            request: Box::new(RpcRequest::Agent {
                                operation: operation.into(),
                                params,
                            }),
                            response,
                        })?
                        .0);
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

    fn cancel_pending(&self) -> Result<()> {
        self.cancellation_requested.store(true, Ordering::Release);
        let sequence_id = self
            .active_sequence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let Some(sequence_id) = sequence_id else {
            return Ok(());
        };
        let _ = self.service.agent.handle(
            &self.service,
            "sequence",
            json!({
                "action":"cancel","sequence_id":sequence_id,"actor_id":self.actor_id
            }),
        );
        let deadline = Instant::now() + Duration::from_millis(900);
        while Instant::now() < deadline {
            if let Ok(report) = crate::sequences::SequenceManager::read_report(
                self.service
                    .runtime_root()
                    .join("sequences/sequence-journal.json"),
                &sequence_id,
            ) && !matches!(
                report.status,
                SequenceStatus::Pending | SequenceStatus::Running
            ) {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        bail!("sequence cancellation did not reach a terminal journal state within 900ms")
    }
}

#[cfg(test)]
mod capture_admission_tests {
    use super::*;

    #[test]
    fn failed_post_reservation_admission_releases_all_reserved_quota() {
        let quota = Arc::new(Mutex::new(CaptureQuota::default()));
        let reservation = reserve_capture_quota(quota.clone(), 4096, 3).unwrap();
        assert_eq!(quota.lock().unwrap().jobs, 1);
        assert_eq!(quota.lock().unwrap().bytes, 4096);
        assert_eq!(quota.lock().unwrap().frames, 3);

        let result = with_capture_quota_lease(reservation, || -> Result<()> {
            anyhow::bail!("injected checkpoint-directory admission failure")
        });
        assert!(result.is_err());
        let quota = quota.lock().unwrap();
        assert_eq!(quota.jobs, 0);
        assert_eq!(quota.bytes, 0);
        assert_eq!(quota.frames, 0);
    }
}
