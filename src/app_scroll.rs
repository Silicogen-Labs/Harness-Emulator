//! Application-viewport scrolling over the existing authorized input boundary.
use crate::locators::{CellRect, LocatorResults, ObservationIdentity};
use crate::protocol::{AgentAction, FrameSnapshot, MouseInput};
use crate::sequences::{
    DispatchResult, SequenceActionIdentity, SequenceAuthorityContext, SequenceBackend,
    SequenceCancellation, SequenceIdentity, SequenceViewport,
};
use crate::wait_engine::{FreshWaitSpec, WaitBaseline, WaitEvaluator, WaitOutcome};
use anyhow::{Result, ensure};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScrollDirection {
    Up,
    Down,
}

/// No default keys exist. Integration supplies bindings verified in the target
/// application, with the observation at which their meaning was established.
pub struct VerifiedScrollBinding {
    direction: ScrollDirection,
    actions: Vec<AgentAction>,
    verified_at: ObservationIdentity,
}

impl VerifiedScrollBinding {
    pub(crate) fn from_verified(
        direction: ScrollDirection,
        actions: Vec<AgentAction>,
        verified_at: ObservationIdentity,
    ) -> Result<Self> {
        ensure!(
            !actions.is_empty() && actions.len() <= 8,
            "scroll binding requires 1..8 verified input events"
        );
        for action in &actions {
            match action {
                AgentAction::Key { key } => {
                    ensure!(!key.is_empty() && key.len() <= 128, "invalid verified key")
                }
                AgentAction::Mouse {
                    input:
                        MouseInput::Wheel {
                            steps,
                            direction: wheel,
                            ..
                        },
                } => {
                    ensure!((1..=20).contains(steps), "wheel steps outside input limit");
                    ensure!(
                        matches!(
                            (direction, wheel),
                            (ScrollDirection::Up, crate::protocol::WheelDirection::Up)
                                | (ScrollDirection::Down, crate::protocol::WheelDirection::Down)
                        ),
                        "binding wheel direction differs from requested application direction"
                    );
                }
                _ => anyhow::bail!(
                    "application scroll bindings accept verified keys or wheel events only"
                ),
            }
        }
        Ok(Self {
            direction,
            actions,
            verified_at,
        })
    }
}

pub struct ScrollRequest {
    pub identity: SequenceIdentity,
    pub authority: SequenceAuthorityContext,
    pub direction: ScrollDirection,
    pub region: CellRect,
    pub target: FreshWaitSpec,
    pub max_steps: usize,
    pub max_no_progress: usize,
    pub post_dispatch_observe_timeout: Duration,
    pub deadline: Instant,
}

pub struct ScrollObservation {
    pub frame: FrameSnapshot,
    pub observation_revision: u64,
    pub viewport_revision: u64,
}

pub trait AppScrollBackend: SequenceBackend {
    /// Observe until the supplied bounded slice expires or output advances;
    /// return the current authoritative frame even when no output advances. This
    /// call must release writer/control locks while waiting and honor deadline
    /// and cancellation. It never acquires input control on the caller's behalf.
    fn observe_application(
        &mut self,
        identity: &SequenceIdentity,
        after: Option<&ObservationIdentity>,
        deadline: Instant,
        cancellation: &SequenceCancellation,
    ) -> Result<ScrollObservation>;
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ScrollOutcome {
    Found,
    NotFoundWithinBudget,
    NoProgress,
    DeadlineExceeded,
    Cancelled,
    StaleRun,
    IncompatibleViewport,
    StaleObservation,
    InputRejected,
    DispatchUnknown,
}

#[derive(Debug, Clone, Serialize)]
pub struct ScrollReport {
    pub space: &'static str,
    pub outcome: ScrollOutcome,
    pub steps: usize,
    pub written_events: usize,
    pub no_progress_steps: usize,
    pub final_observation: ObservationIdentity,
    pub matches: Option<LocatorResults>,
    pub failure: Option<String>,
    pub transcript_complete: bool,
}

pub fn scroll_until<B: AppScrollBackend>(
    backend: &mut B,
    mut request: ScrollRequest,
    binding: &VerifiedScrollBinding,
    cancellation: &SequenceCancellation,
) -> Result<ScrollReport> {
    ensure!(
        (1..=256).contains(&request.max_steps),
        "application scrolling requires a step budget of 1..256"
    );
    ensure!(
        (1..=32).contains(&request.max_no_progress),
        "no-progress threshold must be 1..32"
    );
    ensure!(
        !request.post_dispatch_observe_timeout.is_zero()
            && request.post_dispatch_observe_timeout <= Duration::from_secs(1),
        "post-dispatch observation slice must be positive and at most one second"
    );
    ensure!(
        binding.direction == request.direction,
        "verified binding direction mismatch"
    );
    ensure!(
        request
            .target
            .region
            .is_none_or(|region| region == request.region),
        "target region must equal application scroll region"
    );
    request.target.region = Some(request.region);
    let mut observation =
        backend.observe_application(&request.identity, None, request.deadline, cancellation)?;
    let baseline = WaitBaseline::capture(
        &observation.frame,
        observation.observation_revision,
        observation.viewport_revision,
    )?;
    let mut identity = baseline.identity.clone();
    ensure!(
        identity.session_id == request.identity.session_id
            && identity.run_id == request.identity.run_id,
        "initial application observation belongs to another run"
    );
    ensure!(
        binding.verified_at.session_id == identity.session_id
            && binding.verified_at.run_id == identity.run_id
            && binding.verified_at.viewport_revision == identity.viewport_revision
            && binding.verified_at.cols == identity.cols
            && binding.verified_at.rows == identity.rows,
        "verified scroll binding is stale or has incompatible geometry"
    );
    ensure!(
        identity.viewport_revision == request.authority.expected_viewport_revision
            && identity.cols == request.authority.expected_cols
            && identity.rows == request.authority.expected_rows,
        "scroll authority viewport does not match the initial observation"
    );
    crate::locators::validate_region(&observation.frame, request.region)?;
    for action in &binding.actions {
        if let AgentAction::Mouse {
            input: MouseInput::Wheel { row, col, .. },
        } = action
        {
            ensure!(
                *row >= request.region.row_start
                    && *row < request.region.row_end
                    && *col >= request.region.col_start
                    && *col < request.region.col_end,
                "verified wheel target lies outside application region"
            );
        }
    }
    let mut evaluator = WaitEvaluator::new(request.target, baseline, request.deadline)?;
    let mut report = ScrollReport {
        space: "application_viewport",
        outcome: ScrollOutcome::NotFoundWithinBudget,
        steps: 0,
        written_events: 0,
        no_progress_steps: 0,
        final_observation: identity.clone(),
        matches: None,
        failure: None,
        transcript_complete: false,
    };
    let mut seen = HashSet::new();
    seen.insert(region_fingerprint(&observation.frame, request.region)?);
    loop {
        if cancellation.is_cancelled() {
            report.outcome = ScrollOutcome::Cancelled;
            return Ok(report);
        }
        match evaluator.observe(
            &observation.frame,
            observation.observation_revision,
            observation.viewport_revision,
            Instant::now(),
        ) {
            WaitOutcome::Matched(matches) => {
                report.outcome = ScrollOutcome::Found;
                report.matches = Some(matches);
                return Ok(report);
            }
            WaitOutcome::DeadlineExceeded => {
                report.outcome = ScrollOutcome::DeadlineExceeded;
                return Ok(report);
            }
            WaitOutcome::StaleBaseline => {
                report.outcome = ScrollOutcome::StaleRun;
                return Ok(report);
            }
            WaitOutcome::IncompatibleViewport => {
                report.outcome = ScrollOutcome::IncompatibleViewport;
                return Ok(report);
            }
            WaitOutcome::Cancelled => {
                report.outcome = ScrollOutcome::Cancelled;
                return Ok(report);
            }
            WaitOutcome::Pending => {}
        }
        if report.steps >= request.max_steps {
            return Ok(report);
        }
        if report.no_progress_steps >= request.max_no_progress {
            report.outcome = ScrollOutcome::NoProgress;
            return Ok(report);
        }
        let previous_revision = observation.observation_revision;
        let previous_fingerprint = region_fingerprint(&observation.frame, request.region)?;
        for (event, action) in binding.actions.iter().enumerate() {
            if cancellation.is_cancelled() {
                report.outcome = ScrollOutcome::Cancelled;
                return Ok(report);
            }
            if Instant::now() >= request.deadline {
                report.outcome = ScrollOutcome::DeadlineExceeded;
                return Ok(report);
            }
            let action_identity = SequenceActionIdentity {
                actor_id: request.identity.actor_id.clone(),
                session_id: request.identity.session_id.clone(),
                run_id: request.identity.run_id.clone(),
                sequence_id: request.identity.sequence_id.clone(),
                action_id: format!(
                    "{}:scroll:{}:{}",
                    request.identity.sequence_id, report.steps, event
                ),
                expected_viewport: SequenceViewport {
                    revision: request.authority.expected_viewport_revision,
                    cols: request.authority.expected_cols,
                    rows: request.authority.expected_rows,
                },
                controller_generation: request.authority.controller_generation,
            };
            match backend.dispatch(&action_identity, action, request.deadline, cancellation) {
                DispatchResult::Written {
                    pre_dispatch,
                    post_dispatch_viewport,
                } => {
                    if pre_dispatch.session_id != identity.session_id
                        || pre_dispatch.run_id != identity.run_id
                    {
                        report.outcome = ScrollOutcome::StaleRun;
                        return Ok(report);
                    }
                    if pre_dispatch.viewport_revision != identity.viewport_revision
                        || pre_dispatch.cols != identity.cols
                        || pre_dispatch.rows != identity.rows
                        || post_dispatch_viewport != action_identity.expected_viewport
                    {
                        report.outcome = ScrollOutcome::IncompatibleViewport;
                        return Ok(report);
                    }
                    if pre_dispatch.observation_revision < identity.observation_revision {
                        report.outcome = ScrollOutcome::StaleObservation;
                        return Ok(report);
                    }
                    identity = pre_dispatch;
                    report.written_events += 1;
                }
                DispatchResult::Rejected { reason } => {
                    report.outcome = ScrollOutcome::InputRejected;
                    report.failure = Some(reason.chars().take(256).collect());
                    return Ok(report);
                }
                DispatchResult::Unknown { reason } => {
                    report.outcome = ScrollOutcome::DispatchUnknown;
                    report.failure = Some(reason.chars().take(256).collect());
                    return Ok(report);
                }
            }
        }
        report.steps += 1;
        let observe_deadline = request
            .deadline
            .min(Instant::now() + request.post_dispatch_observe_timeout);
        observation = backend.observe_application(
            &request.identity,
            Some(&identity),
            observe_deadline,
            cancellation,
        )?;
        let fresh = WaitBaseline::capture(
            &observation.frame,
            observation.observation_revision,
            observation.viewport_revision,
        )?
        .identity;
        if fresh.session_id != identity.session_id || fresh.run_id != identity.run_id {
            report.outcome = ScrollOutcome::StaleRun;
            return Ok(report);
        }
        if fresh.viewport_revision != identity.viewport_revision
            || fresh.cols != identity.cols
            || fresh.rows != identity.rows
        {
            report.outcome = ScrollOutcome::IncompatibleViewport;
            return Ok(report);
        }
        if fresh.observation_revision < identity.observation_revision {
            report.outcome = ScrollOutcome::StaleObservation;
            return Ok(report);
        }
        report.final_observation = fresh.clone();
        identity = fresh;
        // Content and style within the requested application region only.
        let fingerprint = region_fingerprint(&observation.frame, request.region)?;
        if observation.observation_revision == previous_revision
            && fingerprint != previous_fingerprint
        {
            report.outcome = ScrollOutcome::StaleObservation;
            return Ok(report);
        }
        if seen.insert(fingerprint) {
            report.no_progress_steps = 0;
        } else {
            report.no_progress_steps += 1;
        }
    }
}

fn region_fingerprint(frame: &FrameSnapshot, region: CellRect) -> Result<[u8; 32]> {
    crate::locators::validate_frame(frame)?;
    crate::locators::validate_region(frame, region)?;
    let mut hash = Sha256::new();
    for row in region.row_start..region.row_end {
        for col in region.col_start..region.col_end {
            let cell = serde_json::to_vec(&frame.cells[usize::from(row)][usize::from(col)])?;
            hash.update(cell.len().to_le_bytes());
            hash.update(cell);
        }
    }
    Ok(hash.finalize().into())
}
