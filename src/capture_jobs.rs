//! Immutable provider-frame capture, independent of browser delivery and parsing.
use crate::locators::{CellRect, LocatorQuery, LocatorResults, ObservationIdentity};
use crate::protocol::{CellSnapshot, CellStyle, FrameSnapshot};
use anyhow::{Result, bail};
use rmcp::schemars;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::mem::size_of;
use std::sync::Arc;

pub const MAX_CAPTURE_JOBS: usize = 32;
pub const MAX_CAPTURE_FRAMES: u32 = 128;
pub const MAX_CAPTURE_BYTES: u64 = 32 * 1024 * 1024;
pub const MAX_CAPTURE_FRAME_BYTES: u64 = 16 * 1024 * 1024;
pub const MAX_CAPTURE_TIMEOUT_MS: u64 = 3_600_000;
// Includes bounded regex programs/caches and job/request/latch metadata.
const JOB_METADATA_RESERVE: u64 = 2 * 1024 * 1024;
const FRAME_METADATA_RESERVE: u64 = 4096;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CaptureAuthority {
    pub session_id: String,
    pub run_id: String,
    pub viewport_revision: u64,
    pub cols: u16,
    pub rows: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CaptureQuery {
    pub query: LocatorQuery,
    pub region: Option<CellRect>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind", content = "condition", rename_all = "snake_case")]
pub enum CaptureCompletion {
    Disappeared,
    /// A later observation must lack the appearance query and contain this query.
    Replaced(CaptureQuery),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CaptureBudget {
    pub max_frames: u32,
    /// Snapshot allocations plus conservative retained-record overhead.
    pub max_bytes: u64,
    pub max_frame_bytes: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SampleBurst {
    /// Samples begin at arm time. Slots at or after duration_ms are excluded.
    pub interval_ms: u64,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CaptureRequest {
    pub handle: String,
    pub authority: CaptureAuthority,
    pub appearance: CaptureQuery,
    pub completion: Option<CaptureCompletion>,
    pub burst: Option<SampleBurst>,
    pub budget: CaptureBudget,
    pub timeout_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ProducerProgress {
    pub event_sequence: u64,
    /// Counts attached to this notification, not cumulative counters.
    pub dropped_events: u64,
    pub coalesced_events: u64,
}

#[derive(Debug, Clone)]
pub struct CaptureObservation {
    pub frame: Arc<FrameSnapshot>,
    pub identity: ObservationIdentity,
    pub producer: ProducerProgress,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CaptureState {
    Armed,
    Appeared,
    Completed,
    Cancelled,
    TimedOut,
    StaleRun,
    SessionClosed,
    ProcessExited,
    IncompatibleViewport,
    Interrupted,
    Failed,
}

impl CaptureState {
    pub fn is_terminal(self) -> bool {
        !matches!(self, Self::Armed | Self::Appeared)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CaptureCoverage {
    /// Always false: one provider notification/PTY chunk can contain transitions
    /// that no immutable snapshot exposes, even with consecutive revisions.
    pub all_transitions_observed: bool,
    pub producer_may_skip_intermediate_states: bool,
    pub sampling_is_partial: bool,
    /// Missing revision/event numbers, not a claim about the number of UI states.
    pub revision_gap_units: u64,
    pub event_gap_units: u64,
    pub producer_dropped_events: u64,
    pub producer_coalesced_events: u64,
    pub out_of_order_notifications: u64,
    pub missed_sample_slots: u64,
    pub dropped_frames: u64,
    pub dropped_bytes: u64,
    pub frame_limit_reached: bool,
    pub byte_limit_reached: bool,
    pub oversized_frames: u64,
    pub lease_failures: u64,
    pub source_capture_incomplete: bool,
    pub checkpoint_failed: bool,
    pub restored_frames_unavailable: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CaptureMatch {
    pub rect: Option<CellRect>,
    pub cell_rects: Vec<CellRect>,
    pub snippet: String,
    pub snippet_truncated: bool,
    pub locator_truncated: bool,
    pub omitted_matches: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CaptureLatch {
    pub observation: ObservationIdentity,
    pub at_ms: u64,
    pub frame_index: Option<u32>,
    pub matched: Option<CaptureMatch>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CaptureFrameRef {
    pub index: u32,
    pub observation: ObservationIdentity,
    pub at_ms: u64,
    pub charged_bytes: u64,
    pub sample: bool,
    pub appearance: bool,
    pub transition: bool,
    pub available: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CaptureStatus {
    pub handle: String,
    pub state: CaptureState,
    pub armed_baseline: ObservationIdentity,
    pub armed_at_ms: u64,
    pub deadline_ms: u64,
    pub updated_at_ms: u64,
    pub appearance: Option<CaptureLatch>,
    pub transition: Option<CaptureLatch>,
    pub frames: Vec<CaptureFrameRef>,
    pub retained_bytes: u64,
    pub burst_finished: bool,
    pub coverage: CaptureCoverage,
    pub failure: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ArmedCapture {
    pub handle: String,
    pub baseline: ObservationIdentity,
    pub armed_at_ms: u64,
    pub deadline_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CaptureCheckpoint {
    pub request: CaptureRequest,
    /// Metadata only. Raw FrameSnapshot cells are deliberately not serialized.
    pub status: CaptureStatus,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MaterializedCapture {
    pub reference: String,
    pub observation: ObservationIdentity,
}

/// Lease values are owned by the core and released through Drop. The adapter
/// must bound any heap/external resources hidden inside those lease values.
/// `admit` reserves ownership/quota before any client/stream publication.
/// `retain` registers a snapshot lease before that snapshot becomes visible.
/// `checkpoint` supplies persistence; no artifact registry or files are written
/// by this module. `materialize` is the integration's screenshot/evidence path.
/// Frames are RAW operational truth, not sanitized evidence. Before saving or
/// exposing materialized evidence, the backend must apply Session's redactor
/// and profile secret annotations. Raw evidence requires explicit opt-in.
pub trait CaptureBackend {
    type JobLease;
    type FrameLease;
    fn admit(
        &mut self,
        request: &CaptureRequest,
        baseline: &ObservationIdentity,
    ) -> Result<Self::JobLease>;
    fn retain(&mut self, handle: &str, frame: &CaptureFrameRef) -> Result<Self::FrameLease>;
    fn checkpoint(&mut self, checkpoint: &CaptureCheckpoint) -> Result<()>;
    fn materialize(
        &mut self,
        handle: &str,
        frame: &CaptureFrameRef,
        snapshot: &FrameSnapshot,
    ) -> Result<MaterializedCapture>;
}

#[derive(Debug, Clone, Copy)]
pub struct CaptureLimits {
    pub max_jobs: usize,
    pub max_reserved_bytes: u64,
    pub max_reserved_frames: u64,
}

impl Default for CaptureLimits {
    fn default() -> Self {
        Self {
            max_jobs: 32,
            max_reserved_bytes: 128 * 1024 * 1024,
            max_reserved_frames: 512,
        }
    }
}

struct RetainedFrame<L> {
    snapshot: Arc<FrameSnapshot>,
    _lease: L,
}

struct CaptureJob<B: CaptureBackend> {
    checkpoint: CaptureCheckpoint,
    _lease: B::JobLease,
    retained: Vec<RetainedFrame<B::FrameLease>>,
    appearance_regex: regex::Regex,
    replacement_regex: Option<regex::Regex>,
    absence_observed: bool,
    last_identity: ObservationIdentity,
    last_producer_sequence: u64,
    next_sample_ms: Option<u64>,
    burst_end_ms: Option<u64>,
}

pub struct CaptureManager<B: CaptureBackend> {
    backend: B,
    limits: CaptureLimits,
    jobs: BTreeMap<String, CaptureJob<B>>,
    reserved_bytes: u64,
    reserved_frames: u64,
}

impl<B: CaptureBackend> CaptureManager<B> {
    pub fn new(backend: B, limits: CaptureLimits) -> Result<Self> {
        if limits.max_jobs == 0
            || limits.max_jobs > MAX_CAPTURE_JOBS
            || limits.max_reserved_bytes == 0
            || limits.max_reserved_bytes > 256 * 1024 * 1024
            || limits.max_reserved_frames == 0
            || limits.max_reserved_frames > 4096
        {
            bail!("invalid capture manager limits")
        }
        Ok(Self {
            backend,
            limits,
            jobs: BTreeMap::new(),
            reserved_bytes: 0,
            reserved_frames: 0,
        })
    }

    /// Arm before triggering input. The integration must subscribe to the
    /// authoritative producer before resolving this baseline, closing that
    /// subscription/baseline race outside browser coalescing.
    pub fn start(
        &mut self,
        request: CaptureRequest,
        baseline: CaptureObservation,
        now_ms: u64,
    ) -> Result<ArmedCapture> {
        validate_request(&request)?;
        if let Some(job) = self.jobs.get(&request.handle) {
            if job.checkpoint.request != request {
                bail!("capture_conflict: handle reused with different request")
            }
            return Ok(armed(&job.checkpoint.status));
        }
        validate_observation(&baseline)?;
        if !compatible(&request.authority, &baseline.identity) {
            bail!("capture_baseline_incompatible")
        }
        self.check_admission(&request)?;
        let appearance_regex = compile_capture_query(&request.appearance.query)?;
        let replacement_regex = replacement_regex(&request)?;
        let initial = locate(&request.appearance, &appearance_regex, &baseline)?;
        let lease = self.backend.admit(&request, &baseline.identity)?;
        let deadline_ms = now_ms
            .checked_add(request.timeout_ms)
            .ok_or_else(|| anyhow::anyhow!("capture deadline overflow"))?;
        let burst_end_ms = request
            .burst
            .map(|burst| now_ms.saturating_add(burst.duration_ms));
        let mut job = CaptureJob {
            checkpoint: CaptureCheckpoint {
                request: request.clone(),
                status: CaptureStatus {
                    handle: request.handle.clone(),
                    state: CaptureState::Armed,
                    armed_baseline: baseline.identity.clone(),
                    armed_at_ms: now_ms,
                    deadline_ms,
                    updated_at_ms: now_ms,
                    appearance: None,
                    transition: None,
                    frames: Vec::new(),
                    retained_bytes: 0,
                    burst_finished: request.burst.is_none(),
                    coverage: CaptureCoverage {
                        producer_may_skip_intermediate_states: true,
                        sampling_is_partial: request.burst.is_some(),
                        source_capture_incomplete: !baseline.frame.capture_complete,
                        ..CaptureCoverage::default()
                    },
                    failure: None,
                },
            },
            _lease: lease,
            retained: Vec::new(),
            appearance_regex,
            replacement_regex,
            absence_observed: !initial.any_match,
            last_identity: baseline.identity.clone(),
            last_producer_sequence: baseline.producer.event_sequence,
            next_sample_ms: request
                .burst
                .map(|burst| now_ms.saturating_add(burst.interval_ms)),
            burst_end_ms,
        };
        if request.burst.is_some() {
            retain_frame(
                &mut self.backend,
                &mut job,
                &baseline,
                now_ms,
                FrameRole::Sample,
            )?;
        }
        self.backend.checkpoint(&job.checkpoint)?;
        let result = armed(&job.checkpoint.status);
        self.reserved_bytes += reservation::<B>(&request);
        self.reserved_frames += u64::from(request.budget.max_frames);
        self.jobs.insert(request.handle.clone(), job);
        Ok(result)
    }

    /// Called for producer notifications and timer ticks. `None` means no
    /// authoritative snapshot was available for this tick; due samples are
    /// explicitly counted as missed. now_ms is one monotonic service clock.
    pub fn advance(
        &mut self,
        handle: &str,
        now_ms: u64,
        observation: Option<CaptureObservation>,
    ) -> Result<CaptureStatus> {
        let job = self
            .jobs
            .get_mut(handle)
            .ok_or_else(|| anyhow::anyhow!("capture_not_found"))?;
        if job.checkpoint.status.state.is_terminal() {
            return Ok(job.checkpoint.status.clone());
        }
        if now_ms < job.checkpoint.status.updated_at_ms {
            bail!("capture_clock_regressed")
        }
        job.checkpoint.status.updated_at_ms = now_ms;
        let update = (|| -> Result<()> {
            if now_ms >= job.checkpoint.status.deadline_ms {
                job.checkpoint.status.state = CaptureState::TimedOut;
                job.checkpoint.status.failure = Some("overall capture deadline expired".into());
                sample_due(&mut self.backend, job, now_ms, None)?;
            } else if let Some(observation) = observation {
                if observation.identity.session_id != job.checkpoint.request.authority.session_id
                    || observation.identity.run_id != job.checkpoint.request.authority.run_id
                {
                    job.checkpoint.status.state = CaptureState::StaleRun;
                } else if !compatible(&job.checkpoint.request.authority, &observation.identity) {
                    job.checkpoint.status.state = CaptureState::IncompatibleViewport;
                } else if observation.frame.process_state != "running" {
                    job.checkpoint.status.state = CaptureState::ProcessExited;
                    job.checkpoint.status.failure = Some(format!(
                        "owned application process ended ({})",
                        observation.frame.process_state
                    ));
                } else {
                    validate_observation(&observation)?;
                    let is_new = observe_progress(job, &observation);
                    if is_new {
                        evaluate_latches(&mut self.backend, job, &observation, now_ms)?;
                    }
                    let sample = if is_new || observation.identity == job.last_identity {
                        Some(&observation)
                    } else {
                        None
                    };
                    sample_due(&mut self.backend, job, now_ms, sample)?;
                }
            } else {
                sample_due(&mut self.backend, job, now_ms, None)?;
            }
            Ok(())
        })();
        if update.is_err() {
            job.checkpoint.status.state = CaptureState::Failed;
            job.checkpoint.status.failure =
                Some("capture observation could not be evaluated".into());
        }
        maybe_complete(job);
        if let Err(error) = self.backend.checkpoint(&job.checkpoint) {
            job.checkpoint.status.coverage.checkpoint_failed = true;
            return Err(error);
        }
        update?;
        Ok(job.checkpoint.status.clone())
    }

    /// Earliest required timer tick, independent of producer output.
    pub fn next_wakeup_ms(&self, handle: &str) -> Result<Option<u64>> {
        let job = self
            .jobs
            .get(handle)
            .ok_or_else(|| anyhow::anyhow!("capture_not_found"))?;
        if job.checkpoint.status.state.is_terminal() {
            return Ok(None);
        }
        let mut next = job.checkpoint.status.deadline_ms;
        if !job.checkpoint.status.burst_finished {
            if let Some(end) = job.burst_end_ms {
                next = next.min(end);
            }
            if let Some(slot) = job.next_sample_ms {
                next = next.min(slot);
            }
        }
        Ok(Some(next))
    }

    pub fn status(&self, handle: &str) -> Result<CaptureStatus> {
        Ok(self
            .jobs
            .get(handle)
            .ok_or_else(|| anyhow::anyhow!("capture_not_found"))?
            .checkpoint
            .status
            .clone())
    }

    pub fn cancel(&mut self, handle: &str) -> Result<CaptureStatus> {
        let job = self
            .jobs
            .get_mut(handle)
            .ok_or_else(|| anyhow::anyhow!("capture_not_found"))?;
        if !job.checkpoint.status.state.is_terminal() {
            job.checkpoint.status.state = CaptureState::Cancelled;
            if let Err(error) = self.backend.checkpoint(&job.checkpoint) {
                job.checkpoint.status.coverage.checkpoint_failed = true;
                job.checkpoint.status.failure =
                    Some("capture cancellation checkpoint was not persisted".into());
                return Err(error);
            }
        }
        Ok(job.checkpoint.status.clone())
    }

    /// End a capture when its owning session closes or the observer is no
    /// longer able to run. This is a terminal, checkpointed lifecycle result.
    pub fn terminate(
        &mut self,
        handle: &str,
        state: CaptureState,
        now_ms: u64,
        reason: impl Into<String>,
    ) -> Result<CaptureStatus> {
        if !matches!(
            state,
            CaptureState::SessionClosed | CaptureState::ProcessExited | CaptureState::Failed
        ) {
            bail!("invalid_capture_termination_state")
        }
        let job = self
            .jobs
            .get_mut(handle)
            .ok_or_else(|| anyhow::anyhow!("capture_not_found"))?;
        if !job.checkpoint.status.state.is_terminal() {
            job.checkpoint.status.state = state;
            job.checkpoint.status.updated_at_ms = now_ms.max(job.checkpoint.status.updated_at_ms);
            job.checkpoint.status.failure = Some(reason.into());
            self.backend.checkpoint(&job.checkpoint)?;
        }
        Ok(job.checkpoint.status.clone())
    }

    pub fn materialize(&mut self, handle: &str, frame_index: u32) -> Result<MaterializedCapture> {
        let job = self
            .jobs
            .get(handle)
            .ok_or_else(|| anyhow::anyhow!("capture_not_found"))?;
        let reference = job
            .checkpoint
            .status
            .frames
            .get(frame_index as usize)
            .filter(|frame| frame.available)
            .ok_or_else(|| anyhow::anyhow!("capture_frame_unavailable"))?;
        let retained = job
            .retained
            .get(frame_index as usize)
            .ok_or_else(|| anyhow::anyhow!("capture_frame_unavailable"))?;
        let result = self
            .backend
            .materialize(handle, reference, &retained.snapshot)?;
        if result.observation != reference.observation
            || result.reference.is_empty()
            || result.reference.len() > 1024
        {
            bail!("capture_materialization_identity_mismatch")
        }
        Ok(result)
    }

    pub fn checkpoints(&self) -> Vec<CaptureCheckpoint> {
        self.jobs
            .values()
            .map(|job| job.checkpoint.clone())
            .collect()
    }

    /// Checkpoints contain metadata, never serialized FrameSnapshot cells.
    /// Reload is intentionally not a resume: nonterminal jobs are interrupted,
    /// retained-frame references are marked unavailable, and leases are readmitted.
    pub fn restore(
        backend: B,
        limits: CaptureLimits,
        checkpoints: Vec<CaptureCheckpoint>,
    ) -> Result<Self> {
        let mut manager = Self::new(backend, limits)?;
        for mut checkpoint in checkpoints {
            validate_request(&checkpoint.request)?;
            validate_checkpoint(&checkpoint)?;
            if manager.jobs.contains_key(&checkpoint.request.handle) {
                bail!("duplicate capture checkpoint handle")
            }
            manager.check_admission(&checkpoint.request)?;
            let lease = manager
                .backend
                .admit(&checkpoint.request, &checkpoint.status.armed_baseline)?;
            if !checkpoint.status.state.is_terminal() {
                checkpoint.status.state = CaptureState::Interrupted;
            }
            checkpoint.status.coverage.all_transitions_observed = false;
            checkpoint
                .status
                .coverage
                .producer_may_skip_intermediate_states = true;
            checkpoint.status.coverage.restored_frames_unavailable =
                checkpoint.status.frames.len() as u64;
            for frame in &mut checkpoint.status.frames {
                frame.available = false;
            }
            checkpoint.status.retained_bytes = 0;
            let appearance_regex = compile_capture_query(&checkpoint.request.appearance.query)?;
            let replacement_regex = replacement_regex(&checkpoint.request)?;
            manager.backend.checkpoint(&checkpoint)?;
            let job = CaptureJob {
                last_identity: checkpoint.status.armed_baseline.clone(),
                checkpoint: checkpoint.clone(),
                _lease: lease,
                retained: Vec::new(),
                appearance_regex,
                replacement_regex,
                absence_observed: false,
                last_producer_sequence: 0,
                next_sample_ms: None,
                burst_end_ms: None,
            };
            manager.reserved_bytes += reservation::<B>(&checkpoint.request);
            manager.reserved_frames += u64::from(checkpoint.request.budget.max_frames);
            manager.jobs.insert(checkpoint.request.handle.clone(), job);
        }
        Ok(manager)
    }

    /// Explicit retention release only; quota pressure never silently evicts a
    /// latch a client has not yet polled.
    pub fn release(&mut self, handle: &str) -> Result<()> {
        let job = self
            .jobs
            .get_mut(handle)
            .ok_or_else(|| anyhow::anyhow!("capture_not_found"))?;
        if !job.checkpoint.status.state.is_terminal() {
            bail!("capture_still_active")
        }
        let unavailable = job
            .checkpoint
            .status
            .frames
            .iter()
            .filter(|frame| frame.available)
            .count() as u64;
        for frame in &mut job.checkpoint.status.frames {
            frame.available = false;
        }
        job.checkpoint.status.coverage.restored_frames_unavailable = job
            .checkpoint
            .status
            .coverage
            .restored_frames_unavailable
            .saturating_add(unavailable);
        job.checkpoint.status.retained_bytes = 0;
        self.backend.checkpoint(&job.checkpoint)?;
        let job = self.jobs.remove(handle).unwrap();
        self.reserved_bytes -= reservation::<B>(&job.checkpoint.request);
        self.reserved_frames -= u64::from(job.checkpoint.request.budget.max_frames);
        drop(job);
        Ok(())
    }

    fn check_admission(&self, request: &CaptureRequest) -> Result<()> {
        if self.jobs.len() >= self.limits.max_jobs
            || self
                .reserved_bytes
                .saturating_add(reservation::<B>(request))
                > self.limits.max_reserved_bytes
            || self
                .reserved_frames
                .saturating_add(u64::from(request.budget.max_frames))
                > self.limits.max_reserved_frames
        {
            bail!("capture_quota_exhausted")
        }
        Ok(())
    }
}

impl<B: CaptureBackend> Drop for CaptureManager<B> {
    fn drop(&mut self) {
        for job in self.jobs.values_mut() {
            if !job.checkpoint.status.state.is_terminal() {
                job.checkpoint.status.state = CaptureState::Interrupted;
                job.checkpoint.status.failure =
                    Some("capture manager dropped; producer observation stopped".into());
                let _ = self.backend.checkpoint(&job.checkpoint);
            }
        }
    }
}

#[derive(Clone, Copy)]
enum FrameRole {
    Sample,
    Appearance,
    Transition,
}

fn reservation<B: CaptureBackend>(request: &CaptureRequest) -> u64 {
    request
        .budget
        .max_bytes
        .saturating_add(JOB_METADATA_RESERVE)
        .saturating_add(size_of::<B::JobLease>() as u64)
        .saturating_add(
            u64::from(request.budget.max_frames).saturating_mul(size_of::<B::FrameLease>() as u64),
        )
}

fn armed(status: &CaptureStatus) -> ArmedCapture {
    ArmedCapture {
        handle: status.handle.clone(),
        baseline: status.armed_baseline.clone(),
        armed_at_ms: status.armed_at_ms,
        deadline_ms: status.deadline_ms,
    }
}

fn compatible(authority: &CaptureAuthority, identity: &ObservationIdentity) -> bool {
    authority.session_id == identity.session_id
        && authority.run_id == identity.run_id
        && authority.viewport_revision == identity.viewport_revision
        && authority.cols == identity.cols
        && authority.rows == identity.rows
}

fn validate_observation(observation: &CaptureObservation) -> Result<()> {
    let frame = &observation.frame;
    let identity = &observation.identity;
    for value in [&identity.session_id, &identity.run_id, &identity.frame_id] {
        if value.is_empty() || value.len() > 128 {
            bail!("invalid capture observation identity")
        }
    }
    if frame.session_id != identity.session_id
        || frame.run_id != identity.run_id
        || frame.frame_id != identity.frame_id
        || frame.cols != identity.cols
        || frame.rows != identity.rows
    {
        bail!("capture frame/observation identity mismatch")
    }
    crate::locators::validate_frame(frame)
}

pub fn validate_capture_request(request: &CaptureRequest) -> Result<()> {
    if request.handle.is_empty()
        || request.handle.len() > 64
        || !request
            .handle
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.:".contains(&byte))
    {
        bail!("invalid capture handle")
    }
    for value in [&request.authority.session_id, &request.authority.run_id] {
        if value.is_empty() || value.len() > 128 {
            bail!("invalid capture authority identity")
        }
    }
    if request.authority.cols == 0
        || request.authority.cols > 300
        || request.authority.rows == 0
        || request.authority.rows > 120
    {
        bail!("invalid capture viewport")
    }
    if request.timeout_ms == 0 || request.timeout_ms > MAX_CAPTURE_TIMEOUT_MS {
        bail!("capture timeout must be 1..=3600000ms")
    }
    let budget = request.budget;
    if budget.max_frames == 0
        || budget.max_frames > MAX_CAPTURE_FRAMES
        || budget.max_frame_bytes == 0
        || budget.max_frame_bytes > MAX_CAPTURE_FRAME_BYTES
        || budget.max_frame_bytes > budget.max_bytes
        || budget.max_bytes > MAX_CAPTURE_BYTES
    {
        bail!("invalid capture frame/byte budget")
    }
    if let Some(burst) = request.burst
        && (burst.interval_ms == 0
            || burst.interval_ms > 60_000
            || burst.duration_ms == 0
            || burst.duration_ms > 60_000
            || burst.duration_ms > request.timeout_ms)
    {
        bail!("invalid capture sample burst")
    }
    validate_query(&request.appearance, &request.authority)?;
    if let Some(CaptureCompletion::Replaced(query)) = &request.completion {
        validate_query(query, &request.authority)?;
    }
    if serde_json::to_vec(request)?.len() > 16 * 1024 {
        bail!("capture request exceeds 16 KiB")
    }
    Ok(())
}

fn validate_request(request: &CaptureRequest) -> Result<()> {
    validate_capture_request(request)
}

fn validate_query(query: &CaptureQuery, authority: &CaptureAuthority) -> Result<()> {
    compile_capture_query(&query.query)?;
    if let Some(region) = query.region
        && (region.row_start >= region.row_end
            || region.col_start >= region.col_end
            || region.row_end > authority.rows
            || region.col_end > authority.cols)
    {
        bail!("capture query region is outside pinned viewport")
    }
    Ok(())
}

fn replacement_regex(request: &CaptureRequest) -> Result<Option<regex::Regex>> {
    match &request.completion {
        Some(CaptureCompletion::Replaced(query)) => Ok(Some(compile_capture_query(&query.query)?)),
        _ => Ok(None),
    }
}

fn compile_capture_query(query: &LocatorQuery) -> Result<regex::Regex> {
    let raw = match query {
        LocatorQuery::Text(text) | LocatorQuery::Regex(text) => text,
    };
    if raw.len() > crate::locators::MAX_QUERY_BYTES || raw.is_empty() {
        bail!("invalid capture query byte length")
    }
    let pattern = match query {
        LocatorQuery::Text(text) => regex::escape(text),
        LocatorQuery::Regex(pattern) => pattern.clone(),
    };
    Ok(regex::RegexBuilder::new(&pattern)
        .size_limit(128 * 1024)
        .dfa_size_limit(128 * 1024)
        .build()?)
}

fn locate(
    query: &CaptureQuery,
    regex: &regex::Regex,
    observation: &CaptureObservation,
) -> Result<LocatorResults> {
    let region = query.region.unwrap_or(CellRect {
        row_start: 0,
        col_start: 0,
        row_end: observation.identity.rows,
        col_end: observation.identity.cols,
    });
    crate::locators::locate_cells(
        &observation.frame,
        &observation.frame.cells,
        regex,
        region,
        observation.identity.observation_revision,
        observation.identity.viewport_revision,
    )
}

fn observe_progress<B: CaptureBackend>(
    job: &mut CaptureJob<B>,
    observation: &CaptureObservation,
) -> bool {
    let identity = &observation.identity;
    let producer = observation.producer;
    if *identity == job.last_identity && producer.event_sequence == job.last_producer_sequence {
        return false;
    }
    if identity.observation_revision <= job.last_identity.observation_revision
        || producer.event_sequence <= job.last_producer_sequence
    {
        job.checkpoint.status.coverage.out_of_order_notifications += 1;
        return false;
    }
    let coverage = &mut job.checkpoint.status.coverage;
    coverage.revision_gap_units = coverage
        .revision_gap_units
        .saturating_add(identity.observation_revision - job.last_identity.observation_revision - 1);
    coverage.event_gap_units = coverage
        .event_gap_units
        .saturating_add(producer.event_sequence - job.last_producer_sequence - 1);
    coverage.producer_dropped_events = coverage
        .producer_dropped_events
        .saturating_add(producer.dropped_events);
    coverage.producer_coalesced_events = coverage
        .producer_coalesced_events
        .saturating_add(producer.coalesced_events);
    coverage.source_capture_incomplete |= !observation.frame.capture_complete;
    job.last_identity = identity.clone();
    job.last_producer_sequence = producer.event_sequence;
    true
}

fn evaluate_latches<B: CaptureBackend>(
    backend: &mut B,
    job: &mut CaptureJob<B>,
    observation: &CaptureObservation,
    now_ms: u64,
) -> Result<()> {
    let appearance = locate(
        &job.checkpoint.request.appearance,
        &job.appearance_regex,
        observation,
    )?;
    if job.checkpoint.status.appearance.is_none() {
        if !appearance.any_match {
            job.absence_observed = true;
        }
        if appearance.any_match && job.absence_observed {
            // Mark latch before retaining so the reserved appearance capacity
            // becomes usable and later samples can never overwrite it.
            job.checkpoint.status.appearance = Some(CaptureLatch {
                observation: observation.identity.clone(),
                at_ms: now_ms,
                frame_index: None,
                matched: Some(match_summary(&appearance)),
            });
            let index = retain_frame(backend, job, observation, now_ms, FrameRole::Appearance)?;
            job.checkpoint
                .status
                .appearance
                .as_mut()
                .unwrap()
                .frame_index = index;
            job.checkpoint.status.state = CaptureState::Appeared;
        }
        return Ok(());
    }
    if job.checkpoint.status.transition.is_some() {
        return Ok(());
    }
    let matches_transition = match &job.checkpoint.request.completion {
        Some(CaptureCompletion::Disappeared) => !appearance.any_match,
        Some(CaptureCompletion::Replaced(query)) => {
            !appearance.any_match
                && locate(query, job.replacement_regex.as_ref().unwrap(), observation)?.any_match
        }
        None => false,
    };
    if matches_transition {
        let matched = match &job.checkpoint.request.completion {
            Some(CaptureCompletion::Replaced(query)) => Some(match_summary(&locate(
                query,
                job.replacement_regex.as_ref().unwrap(),
                observation,
            )?)),
            _ => None,
        };
        let index = retain_frame(backend, job, observation, now_ms, FrameRole::Transition)?;
        job.checkpoint.status.transition = Some(CaptureLatch {
            observation: observation.identity.clone(),
            at_ms: now_ms,
            frame_index: index,
            matched,
        });
    }
    Ok(())
}

fn match_summary(matches: &LocatorResults) -> CaptureMatch {
    let first = matches.matches.first();
    let snippet = first.map_or("", |found| found.snippet.as_str());
    let mut end = snippet.len().min(512);
    while !snippet.is_char_boundary(end) {
        end -= 1;
    }
    CaptureMatch {
        rect: first.map(|found| found.rect),
        cell_rects: first.map_or_else(Vec::new, |found| found.cell_rects.clone()),
        snippet: snippet[..end].to_owned(),
        snippet_truncated: end < snippet.len() || first.is_none(),
        locator_truncated: matches.truncated,
        omitted_matches: matches.omitted_matches as u64,
    }
}

fn retain_frame<B: CaptureBackend>(
    backend: &mut B,
    job: &mut CaptureJob<B>,
    observation: &CaptureObservation,
    now_ms: u64,
    role: FrameRole,
) -> Result<Option<u32>> {
    if let Some(existing) = job
        .checkpoint
        .status
        .frames
        .iter_mut()
        .find(|frame| frame.observation == observation.identity)
    {
        match role {
            FrameRole::Sample => existing.sample = true,
            FrameRole::Appearance => existing.appearance = true,
            FrameRole::Transition => existing.transition = true,
        }
        return Ok(Some(existing.index));
    }
    let charge = snapshot_retention_bytes(&observation.frame)?;
    let budget = job.checkpoint.request.budget;
    let before_appearance = job.checkpoint.status.appearance.is_none();
    let reserve_frames = u32::from(before_appearance);
    let reserve_bytes = if before_appearance {
        budget.max_frame_bytes
    } else {
        0
    };
    let frame_overflow =
        job.checkpoint.status.frames.len() as u32 + 1 + reserve_frames > budget.max_frames;
    let byte_overflow = job
        .checkpoint
        .status
        .retained_bytes
        .saturating_add(charge)
        .saturating_add(reserve_bytes)
        > budget.max_bytes;
    let oversized = charge > budget.max_frame_bytes;
    if frame_overflow || byte_overflow || oversized {
        let coverage = &mut job.checkpoint.status.coverage;
        coverage.dropped_frames = coverage.dropped_frames.saturating_add(1);
        coverage.dropped_bytes = coverage.dropped_bytes.saturating_add(charge);
        coverage.frame_limit_reached |= frame_overflow;
        coverage.byte_limit_reached |= byte_overflow || oversized;
        coverage.oversized_frames = coverage
            .oversized_frames
            .saturating_add(u64::from(oversized));
        return Ok(None);
    }
    let reference = CaptureFrameRef {
        index: job.checkpoint.status.frames.len() as u32,
        observation: observation.identity.clone(),
        at_ms: now_ms,
        charged_bytes: charge,
        sample: matches!(role, FrameRole::Sample),
        appearance: matches!(role, FrameRole::Appearance),
        transition: matches!(role, FrameRole::Transition),
        available: true,
    };
    let lease = match backend.retain(&job.checkpoint.request.handle, &reference) {
        Ok(lease) => lease,
        Err(_) => {
            let coverage = &mut job.checkpoint.status.coverage;
            coverage.lease_failures = coverage.lease_failures.saturating_add(1);
            coverage.dropped_frames = coverage.dropped_frames.saturating_add(1);
            coverage.dropped_bytes = coverage.dropped_bytes.saturating_add(charge);
            return Ok(None);
        }
    };
    job.retained.push(RetainedFrame {
        snapshot: Arc::clone(&observation.frame),
        _lease: lease,
    });
    job.checkpoint.status.retained_bytes += charge;
    job.checkpoint.status.frames.push(reference);
    Ok(Some(job.checkpoint.status.frames.len() as u32 - 1))
}

fn sample_due<B: CaptureBackend>(
    backend: &mut B,
    job: &mut CaptureJob<B>,
    now_ms: u64,
    observation: Option<&CaptureObservation>,
) -> Result<()> {
    let (Some(next), Some(end), Some(burst)) = (
        job.next_sample_ms,
        job.burst_end_ms,
        job.checkpoint.request.burst,
    ) else {
        return Ok(());
    };
    let limit = now_ms.min(end.saturating_sub(1));
    if next <= limit {
        let due = (limit - next) / burst.interval_ms + 1;
        let can_sample = observation.is_some() && now_ms < end;
        job.checkpoint.status.coverage.missed_sample_slots = job
            .checkpoint
            .status
            .coverage
            .missed_sample_slots
            .saturating_add(due - u64::from(can_sample));
        if let Some(observation) = observation.filter(|_| can_sample) {
            retain_frame(backend, job, observation, now_ms, FrameRole::Sample)?;
        }
        job.next_sample_ms = Some(next.saturating_add(due.saturating_mul(burst.interval_ms)));
    }
    if now_ms >= end {
        job.checkpoint.status.burst_finished = true;
    }
    Ok(())
}

fn maybe_complete<B: CaptureBackend>(job: &mut CaptureJob<B>) {
    let status = &mut job.checkpoint.status;
    if !status.state.is_terminal()
        && status.appearance.is_some()
        && status.burst_finished
        && (job.checkpoint.request.completion.is_none() || status.transition.is_some())
    {
        status.state = CaptureState::Completed;
    }
}

fn validate_checkpoint(checkpoint: &CaptureCheckpoint) -> Result<()> {
    let status = &checkpoint.status;
    if status.handle != checkpoint.request.handle
        || !compatible(&checkpoint.request.authority, &status.armed_baseline)
        || status.deadline_ms
            != status
                .armed_at_ms
                .checked_add(checkpoint.request.timeout_ms)
                .unwrap_or(0)
        || status.frames.len() > checkpoint.request.budget.max_frames as usize
        || serde_json::to_vec(checkpoint)?.len() > 512 * 1024
    {
        bail!("invalid capture checkpoint")
    }
    for (index, frame) in status.frames.iter().enumerate() {
        if frame.index != index as u32
            || !compatible(&checkpoint.request.authority, &frame.observation)
        {
            bail!("invalid capture checkpoint frame identity")
        }
    }
    Ok(())
}

/// Conservative retained allocation charge, including every cell and its heap
/// allocations. FrameSnapshot's serde projection skips cells and is not used.
/// Vector/String capacities (not lengths) prevent hidden spare allocations from
/// bypassing quotas. BTreeMap nodes are conservatively charged at 12 slots per
/// entry, including pointer/header allowance. Shared Arcs are charged in full.
pub fn snapshot_retention_bytes(frame: &FrameSnapshot) -> Result<u64> {
    crate::locators::validate_frame(frame)?;
    let mut bytes = size_of::<FrameSnapshot>() + 2 * size_of::<usize>();
    let mut add = |amount: usize| {
        bytes = bytes.saturating_add(amount);
    };
    for string in [
        &frame.session_id,
        &frame.run_id,
        &frame.frame_id,
        &frame.text,
        &frame.cursor.shape,
        &frame.cursor.color,
        &frame.mouse_tracking,
        &frame.mouse_encoding,
        &frame.default_foreground,
        &frame.default_background,
        &frame.cursor_color,
        &frame.process_state,
    ] {
        add(string.capacity());
    }
    add(style_heap(&frame.base_style));
    add(frame
        .styled_runs
        .capacity()
        .saturating_mul(size_of::<crate::protocol::StyledRun>()));
    for run in &frame.styled_runs {
        add(run.text.capacity());
        add(style_heap(&run.style));
    }
    add(frame
        .cells
        .capacity()
        .saturating_mul(size_of::<Vec<CellSnapshot>>()));
    for row in &frame.cells {
        add(row.capacity().saturating_mul(size_of::<CellSnapshot>()));
        for cell in row {
            for string in [
                &cell.text,
                &cell.resolved_foreground,
                &cell.resolved_background,
                &cell.underline,
            ] {
                add(string.capacity());
            }
            if let Some(link) = &cell.hyperlink {
                add(link.capacity());
            }
        }
    }
    add(map_nodes::<String, bool>(frame.modes.len()));
    for key in frame.modes.keys() {
        add(key.capacity());
    }
    add(frame
        .keyboard_mode
        .capacity()
        .saturating_mul(size_of::<String>()));
    for string in &frame.keyboard_mode {
        add(string.capacity());
    }
    add(map_nodes::<u8, String>(frame.palette.len()));
    for color in frame.palette.values() {
        add(color.capacity());
    }
    for string in [&frame.title, &frame.capture_warning].into_iter().flatten() {
        add(string.capacity());
    }
    // Additional headroom covers allocator size classes/headers and spare
    // retained-record vector slots; each frame is charged independently even
    // when its Arc is shared with another job or the producer.
    Ok((bytes as u64)
        .saturating_mul(2)
        .saturating_add(FRAME_METADATA_RESERVE))
}

fn style_heap(style: &CellStyle) -> usize {
    style
        .foreground
        .capacity()
        .saturating_add(style.background.capacity())
        .saturating_add(style.underline.capacity())
        .saturating_add(style.hyperlink.as_ref().map_or(0, String::capacity))
}

fn map_nodes<K, V>(length: usize) -> usize {
    if length == 0 {
        return 0;
    }
    length.saturating_add(1).saturating_mul(12).saturating_mul(
        size_of::<K>()
            .saturating_add(size_of::<V>())
            .saturating_add(size_of::<[usize; 8]>()),
    )
}
