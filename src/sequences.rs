//! Durable, service-independent sequencing over input receipts and barriers.
//!
//! The backend owns the session integration. It must check input authority on
//! every dispatch, capture the pre-dispatch identity atomically with that
//! dispatch, and run barriers without holding input/control/writer locks.

use crate::locators::ObservationIdentity;
use crate::predicates::PredicateSpec;
use crate::protocol::AgentAction;
use crate::wait_engine::FreshWaitSpec;
use anyhow::{Context, Result, bail};
use rmcp::schemars;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

pub const MAX_SEQUENCE_STEPS: usize = 64;
pub const MAX_SEQUENCE_JOBS: usize = 32;
pub const MAX_SEQUENCE_JOURNAL_BYTES: usize = 4 * 1024 * 1024;
pub const MAX_SEQUENCE_REQUEST_BYTES: usize = 1024 * 1024;
pub const MAX_SEQUENCE_TIMEOUT_MS: u64 = 60 * 60 * 1000;
pub const MAX_LEADER_KEYS: usize = 16;
pub const MAX_KEY_DELAY_MS: u64 = 1000;

const JOURNAL_VERSION: u32 = 1;
static TEMP_SEQUENCE_ID: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SequenceIdentity {
    pub actor_id: String,
    pub session_id: String,
    pub run_id: String,
    pub sequence_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SequenceRequest {
    pub identity: SequenceIdentity,
    /// Pinned from the caller's observed capabilities at admission. The core
    /// advances this only after a confirmed explicit resize action.
    pub authority: SequenceAuthorityContext,
    /// One deadline for all inputs, delays and barriers in this sequence.
    pub timeout_ms: u64,
    pub steps: Vec<SequenceStep>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SequenceAuthorityContext {
    pub expected_viewport_revision: u64,
    pub expected_cols: u16,
    pub expected_rows: u16,
    pub controller_generation: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SequenceViewport {
    pub revision: u64,
    pub cols: u16,
    pub rows: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SequenceStep {
    Input {
        id: String,
        action: AgentAction,
    },
    /// Each key is dispatched as its own AgentAction::Key in array order.
    /// delay_ms is applied between adjacent key writes, not before the first.
    LeaderKeys {
        id: String,
        keys: Vec<String>,
        delay_ms: u64,
    },
    Barrier {
        id: String,
        baseline: BarrierBaseline,
        condition: BarrierCondition,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum BarrierBaseline {
    PreviousInput,
    Explicit(ObservationIdentity),
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind", content = "predicate", rename_all = "snake_case")]
pub enum BarrierCondition {
    Predicate(PredicateSpec),
    Fresh(FreshWaitSpec),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SequenceActionIdentity {
    pub actor_id: String,
    pub session_id: String,
    pub run_id: String,
    pub sequence_id: String,
    pub action_id: String,
    pub expected_viewport: SequenceViewport,
    pub controller_generation: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ActionReceipt {
    pub identity: SequenceActionIdentity,
    pub pre_dispatch: ObservationIdentity,
    pub post_dispatch_viewport: SequenceViewport,
    /// Distinguishes PTY bytes from a PTY resize ioctl; neither implies the
    /// application consumed, rendered, or acted on input.
    pub input_disposition: InputDisposition,
    pub written_to_pty: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum InputDisposition {
    PtyBytesAccepted,
    PtyResized,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum SequenceStatus {
    Pending,
    Running,
    Completed,
    PartialFailure,
    Cancelled,
    TimedOut,
    InterruptedUnknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    Pending,
    Dispatching,
    Waiting,
    Written,
    Matched,
    Failed,
    Cancelled,
    TimedOut,
    InterruptedUnknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SequenceStepReport {
    pub step_id: String,
    pub status: StepStatus,
    /// Stable IDs assigned to every input action in this step, including work
    /// that has not yet been dispatched.
    pub action_ids: Vec<String>,
    pub completed_action_ids: Vec<String>,
    pub pending_action_ids: Vec<String>,
    pub dispatching_action_ids: Vec<String>,
    pub unknown_action_ids: Vec<String>,
    pub receipts: Vec<ActionReceipt>,
    pub failure: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SequenceReport {
    pub identity: SequenceIdentity,
    pub status: SequenceStatus,
    pub deadline_unix_ms: u64,
    pub completed_step_ids: Vec<String>,
    pub failing_step_id: Option<String>,
    pub pending_step_ids: Vec<String>,
    pub steps: Vec<SequenceStepReport>,
    pub failure: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DispatchResult {
    /// The backend confirmed a PTY write and provides its atomically captured
    /// pre-dispatch baseline.
    Written {
        pre_dispatch: ObservationIdentity,
        post_dispatch_viewport: SequenceViewport,
    },
    /// The backend guarantees it rejected the action before writing any bytes.
    Rejected { reason: String },
    /// The write may or may not have reached the PTY. This action must never be
    /// automatically replayed after this result or after journal recovery.
    Unknown { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BarrierResult {
    Matched { observation: ObservationIdentity },
    Pending,
    Failed { reason: String },
    StaleRun,
    IncompatibleViewport,
    Cancelled,
    DeadlineExceeded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelayResult {
    Elapsed,
    Cancelled,
    DeadlineExceeded,
}

/// Adapter boundary. `dispatch` must call the authorized per-action path
/// (normally Session::agent_action); its baseline capture and write must share
/// the runtime's run/input synchronization boundary. `barrier` and `delay` must
/// not acquire input ownership or retain a PTY writer/control lock while they
/// wait. The core never treats output advance as application acknowledgement.
pub trait SequenceBackend {
    fn dispatch(
        &mut self,
        identity: &SequenceActionIdentity,
        action: &AgentAction,
        deadline: Instant,
        cancellation: &SequenceCancellation,
    ) -> DispatchResult;

    fn barrier(
        &mut self,
        identity: &SequenceIdentity,
        baseline: &ObservationIdentity,
        condition: &BarrierCondition,
        deadline: Instant,
        cancellation: &SequenceCancellation,
    ) -> BarrierResult;

    fn delay(
        &mut self,
        duration: Duration,
        deadline: Instant,
        cancellation: &SequenceCancellation,
    ) -> DelayResult;
}

#[derive(Debug, Clone, Default)]
pub struct SequenceCancellation(Arc<AtomicBool>);

impl SequenceCancellation {
    pub fn cancel(&self) {
        self.0.store(true, Ordering::Release);
    }

    pub fn is_cancelled(&self) -> bool {
        self.0.load(Ordering::Acquire)
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct SequenceJournal {
    version: u32,
    records: BTreeMap<String, SequenceRecord>,
}

#[derive(Debug, Serialize, Deserialize)]
struct SequenceRecord {
    request: SequenceRequest,
    request_digest: String,
    deadline_unix_ms: u64,
    expected_viewport: SequenceViewport,
    status: SequenceStatus,
    steps: Vec<StepRecord>,
    failure: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct StepRecord {
    id: String,
    status: StepStatus,
    receipts: Vec<ActionReceipt>,
    dispatching_action_id: Option<String>,
    unknown_action_id: Option<String>,
    non_retryable: bool,
    failure: Option<String>,
}

pub struct SequenceManager {
    path: PathBuf,
    journal: SequenceJournal,
}

impl SequenceManager {
    /// Durably reserve a sequence ID and its validated request before a worker
    /// is started. Repeated admission is idempotent only for identical input.
    pub fn admit(&mut self, request: &SequenceRequest) -> Result<()> {
        validate_request(request)?;
        let request_digest = digest_request(request)?;
        let sequence_id = request.identity.sequence_id.clone();
        if let Some(record) = self.journal.records.get(&sequence_id) {
            if record.request_digest != request_digest {
                bail!("sequence_conflict: sequence id was reused with a different request")
            }
            return Ok(());
        }
        if self.journal.records.len() >= MAX_SEQUENCE_JOBS {
            return Err(crate::validation::RequestError::new(
                "request",
                format!("fewer than {MAX_SEQUENCE_JOBS} retained sequence jobs"),
            )
            .code("sequence_limit")
            .into());
        }
        let deadline_unix_ms = now_unix_ms()?.saturating_add(request.timeout_ms);
        let steps = request
            .steps
            .iter()
            .map(|step| StepRecord {
                id: step.id().to_owned(),
                status: StepStatus::Pending,
                receipts: Vec::new(),
                dispatching_action_id: None,
                unknown_action_id: None,
                non_retryable: false,
                failure: None,
            })
            .collect();
        self.journal.records.insert(
            sequence_id,
            SequenceRecord {
                request: request.clone(),
                request_digest,
                deadline_unix_ms,
                expected_viewport: SequenceViewport {
                    revision: request.authority.expected_viewport_revision,
                    cols: request.authority.expected_cols,
                    rows: request.authority.expected_rows,
                },
                status: SequenceStatus::Pending,
                steps,
                failure: None,
            },
        );
        self.persist()
    }

    pub fn read_report(path: impl AsRef<Path>, sequence_id: &str) -> Result<SequenceReport> {
        let bytes = fs::read(path)?;
        if bytes.len() > MAX_SEQUENCE_JOURNAL_BYTES {
            bail!("sequence journal exceeds the 4 MiB limit")
        }
        let journal: SequenceJournal =
            serde_json::from_slice(&bytes).context("invalid sequence journal")?;
        if journal.version != JOURNAL_VERSION {
            bail!("unsupported sequence journal version")
        }
        validate_loaded_journal(&journal)?;
        Self {
            path: PathBuf::new(),
            journal,
        }
        .report(sequence_id)
    }

    pub fn read_request(path: impl AsRef<Path>, sequence_id: &str) -> Result<SequenceRequest> {
        let bytes = fs::read(path)?;
        if bytes.len() > MAX_SEQUENCE_JOURNAL_BYTES {
            bail!("sequence journal exceeds the 4 MiB limit")
        }
        let journal: SequenceJournal =
            serde_json::from_slice(&bytes).context("invalid sequence journal")?;
        if journal.version != JOURNAL_VERSION {
            bail!("unsupported sequence journal version")
        }
        validate_loaded_journal(&journal)?;
        journal
            .records
            .get(sequence_id)
            .map(|r| r.request.clone())
            .ok_or_else(|| anyhow::anyhow!("sequence_not_found"))
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let journal = match fs::read(&path) {
            Ok(bytes) => {
                if bytes.len() > MAX_SEQUENCE_JOURNAL_BYTES {
                    bail!("sequence journal exceeds the 4 MiB limit")
                }
                let journal: SequenceJournal =
                    serde_json::from_slice(&bytes).context("invalid sequence journal")?;
                if journal.version != JOURNAL_VERSION {
                    bail!("unsupported sequence journal version")
                }
                if journal.records.len() > MAX_SEQUENCE_JOBS {
                    bail!("sequence journal exceeds the 32-job limit")
                }
                validate_loaded_journal(&journal)?;
                journal
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => SequenceJournal {
                version: JOURNAL_VERSION,
                records: BTreeMap::new(),
            },
            Err(error) => return Err(error.into()),
        };
        let mut manager = Self { path, journal };
        if manager.recover_after_restart()? {
            manager.persist()?;
        }
        Ok(manager)
    }

    /// Validate all steps and durably admit the request before dispatching any
    /// input. A retry with the same sequence ID and body continues only steps
    /// that are definitely unwritten; completed action receipts are retained.
    pub fn execute<B: SequenceBackend>(
        &mut self,
        request: SequenceRequest,
        backend: &mut B,
        cancellation: &SequenceCancellation,
    ) -> Result<SequenceReport> {
        self.admit(&request)?;
        let sequence_id = request.identity.sequence_id.clone();

        let status = self.journal.records[&sequence_id].status;
        if matches!(
            status,
            SequenceStatus::Completed
                | SequenceStatus::Cancelled
                | SequenceStatus::TimedOut
                | SequenceStatus::InterruptedUnknown
        ) {
            return self.report(&sequence_id);
        }
        if self.journal.records[&sequence_id]
            .steps
            .iter()
            .any(|step| step.non_retryable)
        {
            return self.report(&sequence_id);
        }
        if cancellation.is_cancelled() {
            self.cancel_record(&sequence_id, "cancelled before execution")?;
            return self.report(&sequence_id);
        }
        if self.journal.records[&sequence_id].deadline_unix_ms <= now_unix_ms()? {
            self.set_terminal(
                &sequence_id,
                SequenceStatus::TimedOut,
                "overall deadline expired",
            )?;
            return self.report(&sequence_id);
        }

        self.journal.records.get_mut(&sequence_id).unwrap().status = SequenceStatus::Running;
        self.journal.records.get_mut(&sequence_id).unwrap().failure = None;
        self.persist()?;

        for step_index in 0..request.steps.len() {
            if cancellation.is_cancelled() {
                self.cancel_record(&sequence_id, "sequence cancelled")?;
                return self.report(&sequence_id);
            }
            if self.remaining_deadline(&sequence_id)?.is_none() {
                self.set_step_status(&sequence_id, step_index, StepStatus::TimedOut, None)?;
                self.set_terminal(
                    &sequence_id,
                    SequenceStatus::TimedOut,
                    "overall deadline expired",
                )?;
                return self.report(&sequence_id);
            }
            let step = request.steps[step_index].clone();
            let step_status = self.journal.records[&sequence_id].steps[step_index].status;
            if matches!(step_status, StepStatus::Written | StepStatus::Matched) {
                continue;
            }
            if step_status == StepStatus::InterruptedUnknown {
                self.set_terminal(
                    &sequence_id,
                    SequenceStatus::InterruptedUnknown,
                    "input outcome is unknown and will not be replayed",
                )?;
                return self.report(&sequence_id);
            }

            match step {
                SequenceStep::Input { id, action } => {
                    let action_id = deterministic_action_id(&request.identity.sequence_id, &id, 0);
                    match self.dispatch_one(
                        &sequence_id,
                        step_index,
                        action_id,
                        &action,
                        backend,
                        cancellation,
                    )? {
                        DispatchAttempt::Written => {
                            self.set_step_status(
                                &sequence_id,
                                step_index,
                                StepStatus::Written,
                                None,
                            )?;
                        }
                        DispatchAttempt::Rejected(reason) => {
                            self.fail_step(&sequence_id, step_index, reason)?;
                            return self.report(&sequence_id);
                        }
                        DispatchAttempt::Unknown(reason) => {
                            self.mark_unknown(&sequence_id, step_index, reason)?;
                            return self.report(&sequence_id);
                        }
                        DispatchAttempt::Cancelled => {
                            self.cancel_record(&sequence_id, "sequence cancelled")?;
                            return self.report(&sequence_id);
                        }
                        DispatchAttempt::TimedOut => {
                            self.set_step_status(
                                &sequence_id,
                                step_index,
                                StepStatus::TimedOut,
                                None,
                            )?;
                            self.set_terminal(
                                &sequence_id,
                                SequenceStatus::TimedOut,
                                "overall deadline expired",
                            )?;
                            return self.report(&sequence_id);
                        }
                    }
                }
                SequenceStep::LeaderKeys { id, keys, delay_ms } => {
                    for (key_index, key) in keys.iter().enumerate() {
                        let action_id =
                            deterministic_action_id(&request.identity.sequence_id, &id, key_index);
                        match self.dispatch_one(
                            &sequence_id,
                            step_index,
                            action_id,
                            &AgentAction::Key { key: key.clone() },
                            backend,
                            cancellation,
                        )? {
                            DispatchAttempt::Written => {}
                            DispatchAttempt::Rejected(reason) => {
                                self.fail_step(&sequence_id, step_index, reason)?;
                                return self.report(&sequence_id);
                            }
                            DispatchAttempt::Unknown(reason) => {
                                self.mark_unknown(&sequence_id, step_index, reason)?;
                                return self.report(&sequence_id);
                            }
                            DispatchAttempt::Cancelled => {
                                self.cancel_record(&sequence_id, "sequence cancelled")?;
                                return self.report(&sequence_id);
                            }
                            DispatchAttempt::TimedOut => {
                                self.set_step_status(
                                    &sequence_id,
                                    step_index,
                                    StepStatus::TimedOut,
                                    None,
                                )?;
                                self.set_terminal(
                                    &sequence_id,
                                    SequenceStatus::TimedOut,
                                    "overall deadline expired",
                                )?;
                                return self.report(&sequence_id);
                            }
                        }
                        if key_index + 1 < keys.len() && delay_ms > 0 {
                            let deadline = self.deadline_instant(&sequence_id)?;
                            match backend.delay(
                                Duration::from_millis(delay_ms),
                                deadline,
                                cancellation,
                            ) {
                                DelayResult::Elapsed => {}
                                DelayResult::Cancelled => {
                                    self.cancel_record(
                                        &sequence_id,
                                        "sequence cancelled during key delay",
                                    )?;
                                    return self.report(&sequence_id);
                                }
                                DelayResult::DeadlineExceeded => {
                                    self.set_step_status(
                                        &sequence_id,
                                        step_index,
                                        StepStatus::TimedOut,
                                        None,
                                    )?;
                                    self.set_terminal(
                                        &sequence_id,
                                        SequenceStatus::TimedOut,
                                        "overall deadline expired",
                                    )?;
                                    return self.report(&sequence_id);
                                }
                            }
                        }
                    }
                    self.set_step_status(&sequence_id, step_index, StepStatus::Written, None)?;
                }
                SequenceStep::Barrier {
                    baseline,
                    condition,
                    ..
                } => {
                    let Some(baseline) =
                        self.resolve_baseline(&sequence_id, step_index, baseline)?
                    else {
                        self.fail_step(
                            &sequence_id,
                            step_index,
                            "barrier baseline is unavailable".into(),
                        )?;
                        return self.report(&sequence_id);
                    };
                    self.set_step_status(&sequence_id, step_index, StepStatus::Waiting, None)?;
                    let deadline = self.deadline_instant(&sequence_id)?;
                    let result = backend.barrier(
                        &request.identity,
                        &baseline,
                        &condition,
                        deadline,
                        cancellation,
                    );
                    match result {
                        BarrierResult::Matched { observation } => {
                            if observation.session_id != request.identity.session_id
                                || observation.run_id != request.identity.run_id
                                || observation.viewport_revision != baseline.viewport_revision
                                || observation.cols != baseline.cols
                                || observation.rows != baseline.rows
                            {
                                self.fail_step(
                                    &sequence_id,
                                    step_index,
                                    "barrier returned an incompatible observation identity".into(),
                                )?;
                                return self.report(&sequence_id);
                            }
                            self.set_step_status(
                                &sequence_id,
                                step_index,
                                StepStatus::Matched,
                                None,
                            )?;
                        }
                        BarrierResult::Pending => {
                            self.journal.records.get_mut(&sequence_id).unwrap().status =
                                SequenceStatus::Pending;
                            self.persist()?;
                            return self.report(&sequence_id);
                        }
                        BarrierResult::Failed { reason } => {
                            self.fail_step(&sequence_id, step_index, reason)?;
                            return self.report(&sequence_id);
                        }
                        BarrierResult::StaleRun => {
                            self.fail_step(&sequence_id, step_index, "stale run".into())?;
                            return self.report(&sequence_id);
                        }
                        BarrierResult::IncompatibleViewport => {
                            self.fail_step(
                                &sequence_id,
                                step_index,
                                "incompatible viewport".into(),
                            )?;
                            return self.report(&sequence_id);
                        }
                        BarrierResult::Cancelled => {
                            self.cancel_record(&sequence_id, "barrier cancelled")?;
                            return self.report(&sequence_id);
                        }
                        BarrierResult::DeadlineExceeded => {
                            self.set_step_status(
                                &sequence_id,
                                step_index,
                                StepStatus::TimedOut,
                                None,
                            )?;
                            self.set_terminal(
                                &sequence_id,
                                SequenceStatus::TimedOut,
                                "overall deadline expired",
                            )?;
                            return self.report(&sequence_id);
                        }
                    }
                }
            }
        }

        self.journal.records.get_mut(&sequence_id).unwrap().status = SequenceStatus::Completed;
        self.journal.records.get_mut(&sequence_id).unwrap().failure = None;
        self.persist()?;
        self.report(&sequence_id)
    }

    pub fn cancel(&mut self, sequence_id: &str) -> Result<SequenceReport> {
        if !self.journal.records.contains_key(sequence_id) {
            bail!("sequence_not_found")
        }
        let dispatching = self.journal.records[sequence_id]
            .steps
            .iter()
            .position(|step| step.dispatching_action_id.is_some());
        if let Some(step_index) = dispatching {
            self.mark_unknown(
                sequence_id,
                step_index,
                "cancelled while input dispatch outcome was uncertain".into(),
            )?;
        } else {
            self.cancel_record(sequence_id, "sequence cancelled")?;
        }
        self.report(sequence_id)
    }

    pub fn report(&self, sequence_id: &str) -> Result<SequenceReport> {
        let record = self
            .journal
            .records
            .get(sequence_id)
            .ok_or_else(|| anyhow::anyhow!("sequence_not_found"))?;
        let steps = record
            .steps
            .iter()
            .zip(&record.request.steps)
            .map(|(step, request_step)| {
                let action_ids =
                    planned_action_ids(&record.request.identity.sequence_id, request_step);
                let completed_action_ids = step
                    .receipts
                    .iter()
                    .map(|receipt| receipt.identity.action_id.clone())
                    .collect::<Vec<_>>();
                let unknown_action_ids = step.unknown_action_id.iter().cloned().collect::<Vec<_>>();
                let dispatching_action_ids = step
                    .dispatching_action_id
                    .iter()
                    .cloned()
                    .collect::<Vec<_>>();
                let pending_action_ids = action_ids
                    .iter()
                    .filter(|action_id| {
                        !completed_action_ids.contains(action_id)
                            && !unknown_action_ids.contains(action_id)
                            && !dispatching_action_ids.contains(action_id)
                    })
                    .cloned()
                    .collect();
                SequenceStepReport {
                    step_id: step.id.clone(),
                    status: step.status,
                    action_ids,
                    completed_action_ids,
                    pending_action_ids,
                    dispatching_action_ids,
                    unknown_action_ids,
                    receipts: step.receipts.clone(),
                    failure: step.failure.clone(),
                }
            })
            .collect::<Vec<_>>();
        let completed_step_ids = steps
            .iter()
            .filter(|step| matches!(step.status, StepStatus::Written | StepStatus::Matched))
            .map(|step| step.step_id.clone())
            .collect();
        let failing_step_id = steps
            .iter()
            .find(|step| {
                matches!(
                    step.status,
                    StepStatus::Failed | StepStatus::InterruptedUnknown | StepStatus::TimedOut
                )
            })
            .map(|step| step.step_id.clone());
        let pending_step_ids = steps
            .iter()
            .filter(|step| {
                matches!(
                    step.status,
                    StepStatus::Pending | StepStatus::Dispatching | StepStatus::Waiting
                )
            })
            .map(|step| step.step_id.clone())
            .collect();
        Ok(SequenceReport {
            identity: record.request.identity.clone(),
            status: record.status,
            deadline_unix_ms: record.deadline_unix_ms,
            completed_step_ids,
            failing_step_id,
            pending_step_ids,
            steps,
            failure: record.failure.clone(),
        })
    }

    fn dispatch_one<B: SequenceBackend>(
        &mut self,
        sequence_id: &str,
        step_index: usize,
        action_id: String,
        action: &AgentAction,
        backend: &mut B,
        cancellation: &SequenceCancellation,
    ) -> Result<DispatchAttempt> {
        if self.journal.records[sequence_id].steps[step_index]
            .receipts
            .iter()
            .any(|receipt| receipt.identity.action_id == action_id)
        {
            return Ok(DispatchAttempt::Written);
        }
        if cancellation.is_cancelled() {
            return Ok(DispatchAttempt::Cancelled);
        }
        let Some(deadline) = self.remaining_deadline(sequence_id)? else {
            return Ok(DispatchAttempt::TimedOut);
        };
        let action_identity = {
            let identity = &self.journal.records[sequence_id].request.identity;
            SequenceActionIdentity {
                actor_id: identity.actor_id.clone(),
                session_id: identity.session_id.clone(),
                run_id: identity.run_id.clone(),
                sequence_id: identity.sequence_id.clone(),
                action_id: action_id.clone(),
                expected_viewport: self.journal.records[sequence_id].expected_viewport,
                controller_generation: self.journal.records[sequence_id]
                    .request
                    .authority
                    .controller_generation,
            }
        };
        {
            let step = &mut self.journal.records.get_mut(sequence_id).unwrap().steps[step_index];
            step.status = StepStatus::Dispatching;
            step.dispatching_action_id = Some(action_id.clone());
            step.failure = None;
        }
        self.persist()?;

        let outcome = backend.dispatch(&action_identity, action, deadline, cancellation);
        match outcome {
            DispatchResult::Written {
                pre_dispatch,
                post_dispatch_viewport,
            } => {
                let receipt = ActionReceipt {
                    identity: action_identity,
                    pre_dispatch: pre_dispatch.clone(),
                    post_dispatch_viewport,
                    input_disposition: if matches!(action, AgentAction::Resize { .. }) {
                        InputDisposition::PtyResized
                    } else {
                        InputDisposition::PtyBytesAccepted
                    },
                    written_to_pty: !matches!(action, AgentAction::Resize { .. }),
                };
                let identity = &self.journal.records[sequence_id].request.identity;
                let baseline_matches = pre_dispatch.session_id == identity.session_id
                    && pre_dispatch.run_id == identity.run_id
                    && pre_dispatch.viewport_revision
                        == self.journal.records[sequence_id].expected_viewport.revision
                    && pre_dispatch.cols
                        == self.journal.records[sequence_id].expected_viewport.cols
                    && pre_dispatch.rows
                        == self.journal.records[sequence_id].expected_viewport.rows;
                let viewport_transition_valid = match action {
                    AgentAction::Resize { cols, rows } => {
                        post_dispatch_viewport.revision > pre_dispatch.viewport_revision
                            && post_dispatch_viewport.cols == *cols
                            && post_dispatch_viewport.rows == *rows
                    }
                    _ => {
                        post_dispatch_viewport
                            == self.journal.records[sequence_id].expected_viewport
                    }
                };
                let identity_error = {
                    let record = self.journal.records.get_mut(sequence_id).unwrap();
                    let step = &mut record.steps[step_index];
                    step.dispatching_action_id = None;
                    step.receipts.push(receipt.clone());
                    if !baseline_matches || !viewport_transition_valid {
                        let reason = "backend returned an incompatible pre/post-dispatch identity after writing".to_owned();
                        step.status = StepStatus::Failed;
                        step.non_retryable = true;
                        step.failure = Some(reason.clone());
                        record.status = SequenceStatus::PartialFailure;
                        record.failure = Some(reason.clone());
                        Some(reason)
                    } else {
                        if matches!(action, AgentAction::Resize { .. }) {
                            record.expected_viewport = post_dispatch_viewport;
                        }
                        None
                    }
                };
                if let Some(reason) = identity_error {
                    self.persist()?;
                    return Ok(DispatchAttempt::Rejected(reason));
                }
                self.persist()?;
                Ok(DispatchAttempt::Written)
            }
            DispatchResult::Rejected { reason } => {
                let step =
                    &mut self.journal.records.get_mut(sequence_id).unwrap().steps[step_index];
                step.dispatching_action_id = None;
                step.status = StepStatus::Failed;
                step.failure = Some(reason.clone());
                self.journal.records.get_mut(sequence_id).unwrap().status =
                    SequenceStatus::PartialFailure;
                self.journal.records.get_mut(sequence_id).unwrap().failure = Some(reason.clone());
                self.persist()?;
                Ok(DispatchAttempt::Rejected(reason))
            }
            DispatchResult::Unknown { reason } => {
                self.mark_unknown(sequence_id, step_index, reason.clone())?;
                Ok(DispatchAttempt::Unknown(reason))
            }
        }
    }

    fn resolve_baseline(
        &self,
        sequence_id: &str,
        before_step: usize,
        baseline: BarrierBaseline,
    ) -> Result<Option<ObservationIdentity>> {
        match baseline {
            BarrierBaseline::Explicit(identity) => Ok(Some(identity)),
            BarrierBaseline::PreviousInput => Ok(self.journal.records[sequence_id].steps
                [..before_step]
                .iter()
                .rev()
                .flat_map(|step| step.receipts.iter().rev())
                .next()
                .map(|receipt| receipt.pre_dispatch.clone())),
        }
    }

    fn fail_step(&mut self, sequence_id: &str, index: usize, reason: String) -> Result<()> {
        let step = &mut self.journal.records.get_mut(sequence_id).unwrap().steps[index];
        step.status = StepStatus::Failed;
        step.dispatching_action_id = None;
        step.failure = Some(reason.clone());
        let record = self.journal.records.get_mut(sequence_id).unwrap();
        record.status = SequenceStatus::PartialFailure;
        record.failure = Some(reason);
        self.persist()
    }

    fn mark_unknown(&mut self, sequence_id: &str, index: usize, reason: String) -> Result<()> {
        let step = &mut self.journal.records.get_mut(sequence_id).unwrap().steps[index];
        step.status = StepStatus::InterruptedUnknown;
        step.unknown_action_id = step.dispatching_action_id.take();
        step.failure = Some(reason.clone());
        let record = self.journal.records.get_mut(sequence_id).unwrap();
        record.status = SequenceStatus::InterruptedUnknown;
        record.failure = Some(reason);
        self.persist()
    }

    fn cancel_record(&mut self, sequence_id: &str, reason: &str) -> Result<()> {
        let record = self.journal.records.get_mut(sequence_id).unwrap();
        if let Some(step) = record
            .steps
            .iter_mut()
            .find(|step| step.dispatching_action_id.is_some())
        {
            step.status = StepStatus::InterruptedUnknown;
            step.unknown_action_id = step.dispatching_action_id.take();
            step.failure = Some("cancelled while input dispatch outcome was uncertain".into());
            record.status = SequenceStatus::InterruptedUnknown;
            record.failure = Some("input outcome is unknown; action will not be replayed".into());
            return self.persist();
        }
        for step in &mut record.steps {
            if matches!(
                step.status,
                StepStatus::Pending | StepStatus::Waiting | StepStatus::Dispatching
            ) {
                step.status = StepStatus::Cancelled;
            }
        }
        record.status = SequenceStatus::Cancelled;
        record.failure = Some(reason.into());
        self.persist()
    }

    fn set_step_status(
        &mut self,
        sequence_id: &str,
        index: usize,
        status: StepStatus,
        failure: Option<String>,
    ) -> Result<()> {
        let step = &mut self.journal.records.get_mut(sequence_id).unwrap().steps[index];
        step.status = status;
        step.failure = failure;
        step.dispatching_action_id = None;
        self.persist()
    }

    fn set_terminal(
        &mut self,
        sequence_id: &str,
        status: SequenceStatus,
        reason: &str,
    ) -> Result<()> {
        let record = self.journal.records.get_mut(sequence_id).unwrap();
        record.status = status;
        record.failure = Some(reason.into());
        self.persist()
    }

    fn remaining_deadline(&self, sequence_id: &str) -> Result<Option<Instant>> {
        let deadline_ms = self.journal.records[sequence_id].deadline_unix_ms;
        let remaining_ms = deadline_ms.saturating_sub(now_unix_ms()?);
        if remaining_ms == 0 {
            return Ok(None);
        }
        Ok(Instant::now().checked_add(Duration::from_millis(remaining_ms)))
    }

    fn deadline_instant(&self, sequence_id: &str) -> Result<Instant> {
        self.remaining_deadline(sequence_id)?
            .ok_or_else(|| anyhow::anyhow!("sequence deadline expired"))
    }

    fn recover_after_restart(&mut self) -> Result<bool> {
        let mut changed = false;
        for record in self.journal.records.values_mut() {
            if matches!(
                record.status,
                SequenceStatus::Completed
                    | SequenceStatus::Cancelled
                    | SequenceStatus::TimedOut
                    | SequenceStatus::InterruptedUnknown
            ) {
                continue;
            }
            if let Some(step) = record
                .steps
                .iter_mut()
                .find(|step| step.dispatching_action_id.is_some())
            {
                step.status = StepStatus::InterruptedUnknown;
                step.unknown_action_id = step.dispatching_action_id.take();
                step.failure = Some("manager reloaded while dispatch was in progress".into());
                record.status = SequenceStatus::InterruptedUnknown;
                record.failure =
                    Some("input outcome is unknown; action will not be replayed".into());
            } else {
                for step in &mut record.steps {
                    if matches!(
                        step.status,
                        StepStatus::Pending | StepStatus::Waiting | StepStatus::Dispatching
                    ) {
                        step.status = StepStatus::Cancelled;
                    }
                }
                record.status = SequenceStatus::Cancelled;
                record.failure = Some("nonterminal sequence cancelled during reload".into());
            }
            changed = true;
        }
        Ok(changed)
    }

    fn persist(&self) -> Result<()> {
        let bytes = serde_json::to_vec(&self.journal)?;
        if bytes.len() > MAX_SEQUENCE_JOURNAL_BYTES {
            bail!("sequence journal would exceed the 4 MiB limit")
        }
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        fs::create_dir_all(parent)?;
        let file_name = self
            .path
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("sequence-journal");
        let mut temp = None;
        for _ in 0..16 {
            let nonce = TEMP_SEQUENCE_ID.fetch_add(1, Ordering::Relaxed);
            let candidate =
                parent.join(format!(".{file_name}.{}.{}.tmp", std::process::id(), nonce));
            match OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&candidate)
            {
                Ok(file) => {
                    temp = Some((candidate, file));
                    break;
                }
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error.into()),
            }
        }
        let (temp_path, mut file) =
            temp.ok_or_else(|| anyhow::anyhow!("could not allocate sequence journal temp file"))?;
        let write_result = (|| -> Result<()> {
            file.write_all(&bytes)?;
            file.sync_all()?;
            drop(file);
            fs::rename(&temp_path, &self.path)?;
            File::open(parent)?.sync_all()?;
            Ok(())
        })();
        if write_result.is_err() {
            let _ = fs::remove_file(&temp_path);
        }
        write_result
    }
}

impl Drop for SequenceManager {
    fn drop(&mut self) {
        let mut changed = false;
        for record in self.journal.records.values_mut() {
            if matches!(
                record.status,
                SequenceStatus::Completed
                    | SequenceStatus::Cancelled
                    | SequenceStatus::TimedOut
                    | SequenceStatus::InterruptedUnknown
            ) {
                continue;
            }
            if let Some(step) = record
                .steps
                .iter_mut()
                .find(|step| step.dispatching_action_id.is_some())
            {
                step.status = StepStatus::InterruptedUnknown;
                step.unknown_action_id = step.dispatching_action_id.take();
                step.failure = Some("manager dropped while dispatch was in progress".into());
                record.status = SequenceStatus::InterruptedUnknown;
                record.failure =
                    Some("input outcome is unknown; action will not be replayed".into());
            } else {
                for step in &mut record.steps {
                    if matches!(
                        step.status,
                        StepStatus::Pending | StepStatus::Waiting | StepStatus::Dispatching
                    ) {
                        step.status = StepStatus::Cancelled;
                    }
                }
                record.status = SequenceStatus::Cancelled;
                record.failure = Some("nonterminal sequence cancelled when manager dropped".into());
            }
            changed = true;
        }
        if changed {
            let _ = self.persist();
        }
    }
}

#[derive(Debug)]
enum DispatchAttempt {
    Written,
    Rejected(String),
    Unknown(String),
    Cancelled,
    TimedOut,
}

pub fn validate_request(request: &SequenceRequest) -> Result<()> {
    validate_identifier("actor_id", &request.identity.actor_id, 128)?;
    validate_identifier("session_id", &request.identity.session_id, 128)?;
    validate_identifier("run_id", &request.identity.run_id, 128)?;
    validate_identifier("sequence_id", &request.identity.sequence_id, 32)?;
    if request.authority.expected_cols == 0
        || request.authority.expected_cols > 300
        || request.authority.expected_rows == 0
        || request.authority.expected_rows > 120
    {
        bail!("invalid_params: initial viewport geometry must be within 1..=300 by 1..=120")
    }
    if request.timeout_ms == 0 || request.timeout_ms > MAX_SEQUENCE_TIMEOUT_MS {
        bail!("invalid_params: sequence timeout must be 1..=3600000ms")
    }
    if request.steps.is_empty() || request.steps.len() > MAX_SEQUENCE_STEPS {
        bail!("invalid_params: sequence must contain 1..=64 steps")
    }
    let encoded = serde_json::to_vec(request)?;
    if encoded.len() > MAX_SEQUENCE_REQUEST_BYTES {
        bail!("invalid_params: serialized sequence request exceeds 1 MiB")
    }

    let mut step_ids = std::collections::BTreeSet::new();
    let mut previous_input_exists = false;
    let mut validation_cols = request.authority.expected_cols;
    let mut validation_rows = request.authority.expected_rows;
    for (index, step) in request.steps.iter().enumerate() {
        validate_identifier("step_id", step.id(), 32)?;
        if !step_ids.insert(step.id()) {
            bail!("invalid_params: duplicate sequence step id at steps[{index}]")
        }
        match step {
            SequenceStep::Input { action, .. } => {
                crate::validation::action(
                    &serde_json::to_value(action)?,
                    &format!("steps[{index}].action"),
                )?;
                validate_action_id(&request.identity.sequence_id, step.id(), 0)?;
                previous_input_exists = true;
                if let AgentAction::Resize { cols, rows } = action {
                    validation_cols = *cols;
                    validation_rows = *rows;
                }
            }
            SequenceStep::LeaderKeys { keys, delay_ms, .. } => {
                if keys.is_empty() || keys.len() > MAX_LEADER_KEYS {
                    bail!(
                        "invalid_params: leader keys must contain 1..=16 entries at steps[{index}]"
                    )
                }
                if *delay_ms > MAX_KEY_DELAY_MS {
                    bail!("invalid_params: per-key delay must be 0..=1000ms at steps[{index}]")
                }
                for (key_index, key) in keys.iter().enumerate() {
                    crate::validation::action(
                        &serde_json::to_value(AgentAction::Key { key: key.clone() })?,
                        &format!("steps[{index}].keys[{key_index}]"),
                    )?;
                    validate_action_id(&request.identity.sequence_id, step.id(), key_index)?;
                }
                previous_input_exists = true;
            }
            SequenceStep::Barrier {
                baseline,
                condition,
                ..
            } => {
                match baseline {
                    BarrierBaseline::PreviousInput if !previous_input_exists => {
                        bail!(
                            "invalid_params: previous_input baseline requires an earlier input step at steps[{index}]"
                        )
                    }
                    BarrierBaseline::Explicit(identity) => {
                        if identity.session_id != request.identity.session_id
                            || identity.run_id != request.identity.run_id
                        {
                            bail!(
                                "invalid_params: explicit barrier baseline belongs to another session or run at steps[{index}]"
                            )
                        }
                        if identity.cols == 0 || identity.rows == 0 {
                            bail!(
                                "invalid_params: explicit barrier baseline geometry must be nonzero at steps[{index}]"
                            )
                        }
                        if identity.cols > 300 || identity.rows > 120 {
                            bail!(
                                "invalid_params: explicit barrier baseline geometry exceeds supported viewport at steps[{index}]"
                            )
                        }
                        if let BarrierCondition::Fresh(fresh) = condition
                            && let Some(region) = fresh.region
                            && (region.row_end > identity.rows || region.col_end > identity.cols)
                        {
                            bail!(
                                "invalid_params: fresh barrier region exceeds explicit baseline viewport at steps[{index}]"
                            )
                        }
                    }
                    _ => {}
                }
                let (barrier_cols, barrier_rows) = match baseline {
                    BarrierBaseline::Explicit(identity) => (identity.cols, identity.rows),
                    BarrierBaseline::PreviousInput => (validation_cols, validation_rows),
                };
                match condition {
                    BarrierCondition::Predicate(predicate) => {
                        crate::validation::predicate(
                            &serde_json::to_value(predicate)?,
                            &format!("steps[{index}].condition"),
                        )?;
                        validate_predicate_viewport(predicate, barrier_cols, barrier_rows, index)?;
                    }
                    BarrierCondition::Fresh(fresh) => {
                        crate::locators::compile_query(&fresh.query)?;
                        if let Some(region) = fresh.region
                            && (region.row_start >= region.row_end
                                || region.col_start >= region.col_end)
                        {
                            bail!(
                                "invalid_params: fresh barrier region must be non-empty at steps[{index}]"
                            )
                        }
                        if let Some(region) = fresh.region
                            && (region.row_end > barrier_rows || region.col_end > barrier_cols)
                        {
                            bail!(
                                "invalid_params: fresh barrier region exceeds its viewport baseline at steps[{index}]"
                            )
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn validate_identifier(field: &str, value: &str, max_bytes: usize) -> Result<()> {
    if value.is_empty()
        || value.len() > max_bytes
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"-_.:".contains(&byte))
    {
        bail!("invalid_params: {field} must be 1..={max_bytes} ASCII identifier bytes")
    }
    Ok(())
}

fn validate_predicate_viewport(
    predicate: &PredicateSpec,
    cols: u16,
    rows: u16,
    step_index: usize,
) -> Result<()> {
    let region = match predicate {
        PredicateSpec::StableRegion {
            row,
            col,
            rows,
            cols,
            ..
        } => Some((*row, *col, *rows, *cols)),
        PredicateSpec::Ready {
            stable_region: Some(region),
            ..
        } => Some((region.row, region.col, region.rows, region.cols)),
        _ => None,
    };
    if region.is_some_and(|(row, col, region_rows, region_cols)| {
        row.saturating_add(region_rows) > rows || col.saturating_add(region_cols) > cols
    }) {
        bail!(
            "invalid_params: predicate region exceeds its viewport baseline at steps[{step_index}]"
        )
    }
    Ok(())
}

fn validate_action_id(sequence_id: &str, step_id: &str, index: usize) -> Result<()> {
    let action_id = deterministic_action_id(sequence_id, step_id, index);
    if action_id.len() > 128 {
        bail!("invalid_params: generated action id exceeds the session 128-byte limit")
    }
    Ok(())
}

fn deterministic_action_id(sequence_id: &str, step_id: &str, index: usize) -> String {
    format!("seq:{sequence_id}:{step_id}:{index}")
}

fn planned_action_ids(sequence_id: &str, step: &SequenceStep) -> Vec<String> {
    match step {
        SequenceStep::Input { id, .. } => {
            vec![deterministic_action_id(sequence_id, id, 0)]
        }
        SequenceStep::LeaderKeys { id, keys, .. } => (0..keys.len())
            .map(|index| deterministic_action_id(sequence_id, id, index))
            .collect(),
        SequenceStep::Barrier { .. } => Vec::new(),
    }
}

fn digest_request(request: &SequenceRequest) -> Result<String> {
    let bytes = serde_json::to_vec(request)?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

fn validate_loaded_journal(journal: &SequenceJournal) -> Result<()> {
    if journal.version != JOURNAL_VERSION || journal.records.len() > MAX_SEQUENCE_JOBS {
        bail!("invalid sequence journal header or job count")
    }
    for (key, record) in &journal.records {
        validate_request(&record.request).context("invalid request in sequence journal")?;
        if key != &record.request.identity.sequence_id
            || digest_request(&record.request)? != record.request_digest
            || record.steps.len() != record.request.steps.len()
            || record.deadline_unix_ms == 0
            || record.expected_viewport.cols == 0
            || record.expected_viewport.cols > 300
            || record.expected_viewport.rows == 0
            || record.expected_viewport.rows > 120
        {
            bail!("inconsistent sequence journal record")
        }
        for (request_step, stored_step) in record.request.steps.iter().zip(&record.steps) {
            if request_step.id() != stored_step.id {
                bail!("sequence journal step identity mismatch")
            }
            let expected_action_ids = match request_step {
                SequenceStep::Input { id, .. } => {
                    vec![deterministic_action_id(
                        &record.request.identity.sequence_id,
                        id,
                        0,
                    )]
                }
                SequenceStep::LeaderKeys { id, keys, .. } => (0..keys.len())
                    .map(|index| {
                        deterministic_action_id(&record.request.identity.sequence_id, id, index)
                    })
                    .collect(),
                SequenceStep::Barrier { .. } => Vec::new(),
            };
            for receipt in &stored_step.receipts {
                let expected_written_to_pty = !matches!(
                    request_step,
                    SequenceStep::Input {
                        action: AgentAction::Resize { .. },
                        ..
                    }
                );
                if !expected_action_ids.contains(&receipt.identity.action_id)
                    || receipt.identity.actor_id != record.request.identity.actor_id
                    || receipt.identity.session_id != record.request.identity.session_id
                    || receipt.identity.run_id != record.request.identity.run_id
                    || receipt.identity.sequence_id != record.request.identity.sequence_id
                    || receipt.identity.controller_generation
                        != record.request.authority.controller_generation
                    || receipt.written_to_pty != expected_written_to_pty
                    || receipt.input_disposition
                        != if expected_written_to_pty {
                            InputDisposition::PtyBytesAccepted
                        } else {
                            InputDisposition::PtyResized
                        }
                {
                    bail!("invalid action receipt in sequence journal")
                }
            }
            for action_id in [
                stored_step.dispatching_action_id.as_ref(),
                stored_step.unknown_action_id.as_ref(),
            ]
            .into_iter()
            .flatten()
            {
                if !expected_action_ids.contains(action_id) {
                    bail!("invalid in-flight action identity in sequence journal")
                }
            }
        }
    }
    Ok(())
}

fn now_unix_ms() -> Result<u64> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock predates Unix epoch")?
        .as_millis();
    Ok(u64::try_from(millis).unwrap_or(u64::MAX))
}

impl SequenceStep {
    fn id(&self) -> &str {
        match self {
            Self::Input { id, .. } | Self::LeaderKeys { id, .. } | Self::Barrier { id, .. } => id,
        }
    }
}
