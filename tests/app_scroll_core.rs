mod protocol {
    pub use silicogen_terminal_runtime::protocol::*;
}
mod predicates {
    pub use silicogen_terminal_runtime::predicates::*;
}
mod validation {
    pub use silicogen_terminal_runtime::validation::*;
}
#[path = "../src/app_scroll.rs"]
#[allow(dead_code)]
mod app_scroll;
#[path = "../src/locators.rs"]
#[allow(dead_code)]
mod locators;
#[path = "../src/sequences.rs"]
#[allow(dead_code)]
mod sequences;
#[path = "../src/wait_engine.rs"]
#[allow(dead_code)]
mod wait_engine;

use app_scroll::*;
use locators::{CellRect, LocatorQuery, ObservationIdentity};
use protocol::{AgentAction, FrameSnapshot, MouseInput, WheelDirection};
use sequences::*;
use silicogen_terminal_runtime::engine::EngineState;
use std::time::{Duration, Instant};
use wait_engine::{FreshTransition, FreshWaitSpec};

/// Only the current virtual page is ever painted. No terminal scrollback or
/// frame history contains the other logical pages before scrolling to them.
struct VirtualizedTranscript {
    pages: Vec<String>,
    position: usize,
    engine: EngineState,
    revision: u64,
    generation: u64,
    attempts: usize,
    painted: Vec<String>,
    slice_lengths: Vec<Duration>,
    lose_authority_after: Option<usize>,
    wrong_run: bool,
    wrong_viewport: bool,
    regress: bool,
    unknown: bool,
    cancellation_on_observe: bool,
}
impl VirtualizedTranscript {
    fn new(pages: &[&str]) -> Self {
        let mut fixture = Self {
            pages: pages.iter().map(|s| s.to_string()).collect(),
            position: 0,
            engine: EngineState::new(40, 3).unwrap(),
            revision: 1,
            generation: 7,
            attempts: 0,
            painted: vec![],
            slice_lengths: vec![],
            lose_authority_after: None,
            wrong_run: false,
            wrong_viewport: false,
            regress: false,
            unknown: false,
            cancellation_on_observe: false,
        };
        fixture.paint();
        fixture
    }
    fn paint(&mut self) {
        let content = self.pages[self.position].clone();
        self.engine
            .process_output(format!("\x1b[2J\x1b[H{content}").as_bytes());
        self.painted.push(content);
    }
    fn frame(&self) -> FrameSnapshot {
        self.engine.snapshot("terminal", "run", "running")
    }
    fn identity(&self) -> ObservationIdentity {
        ObservationIdentity {
            session_id: "terminal".into(),
            run_id: "run".into(),
            frame_id: format!("frame-{}", self.revision),
            observation_revision: self.revision,
            viewport_revision: 1,
            cols: 40,
            rows: 3,
        }
    }
}
impl SequenceBackend for VirtualizedTranscript {
    fn dispatch(
        &mut self,
        identity: &SequenceActionIdentity,
        action: &AgentAction,
        deadline: Instant,
        cancellation: &SequenceCancellation,
    ) -> DispatchResult {
        self.attempts += 1;
        if self
            .lose_authority_after
            .is_some_and(|count| self.attempts > count)
        {
            self.generation += 1;
        }
        if identity.controller_generation != self.generation
            || cancellation.is_cancelled()
            || Instant::now() >= deadline
        {
            return DispatchResult::Rejected {
                reason: "controller authority revoked before write".into(),
            };
        }
        if self.unknown {
            return DispatchResult::Unknown {
                reason: "uncertain PTY write".into(),
            };
        }
        let pre_dispatch = self.identity();
        let old = self.position;
        match action {
            AgentAction::Mouse {
                input:
                    MouseInput::Wheel {
                        direction: WheelDirection::Down,
                        ..
                    },
            } => self.position = (self.position + 1).min(self.pages.len() - 1),
            AgentAction::Mouse {
                input:
                    MouseInput::Wheel {
                        direction: WheelDirection::Up,
                        ..
                    },
            } => self.position = self.position.saturating_sub(1),
            _ => {}
        }
        if self.position != old {
            self.revision += 1;
            self.paint();
        }
        DispatchResult::Written {
            pre_dispatch,
            post_dispatch_viewport: identity.expected_viewport,
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
        panic!("scroll must use bounded observations")
    }
    fn delay(&mut self, _: Duration, _: Instant, _: &SequenceCancellation) -> DelayResult {
        panic!("no blind delay")
    }
}
impl AppScrollBackend for VirtualizedTranscript {
    fn observe_application(
        &mut self,
        _: &SequenceIdentity,
        after: Option<&ObservationIdentity>,
        deadline: Instant,
        cancellation: &SequenceCancellation,
    ) -> anyhow::Result<ScrollObservation> {
        if after.is_some() {
            self.slice_lengths
                .push(deadline.saturating_duration_since(Instant::now()));
            if self.cancellation_on_observe {
                cancellation.cancel();
            }
            if after.is_some_and(|identity| identity.observation_revision == self.revision) {
                std::thread::sleep(deadline.saturating_duration_since(Instant::now()));
            }
        }
        let mut frame = self.frame();
        if after.is_some() && self.wrong_run {
            frame.run_id = "other-run".into();
        }
        Ok(ScrollObservation {
            frame,
            observation_revision: if after.is_some() && self.regress {
                0
            } else {
                self.revision
            },
            viewport_revision: if after.is_some() && self.wrong_viewport {
                2
            } else {
                1
            },
        })
    }
}
fn request(target: &str, steps: usize) -> ScrollRequest {
    ScrollRequest {
        identity: SequenceIdentity {
            actor_id: "controller".into(),
            session_id: "terminal".into(),
            run_id: "run".into(),
            sequence_id: "scroll-job".into(),
        },
        authority: SequenceAuthorityContext {
            expected_viewport_revision: 1,
            expected_cols: 40,
            expected_rows: 3,
            controller_generation: 7,
        },
        direction: ScrollDirection::Down,
        region: CellRect {
            row_start: 0,
            row_end: 2,
            col_start: 0,
            col_end: 40,
        },
        target: FreshWaitSpec {
            query: LocatorQuery::Text(target.into()),
            region: None,
            transition: FreshTransition::CurrentPresence,
        },
        max_steps: steps,
        max_no_progress: 2,
        post_dispatch_observe_timeout: Duration::from_millis(5),
        deadline: Instant::now() + Duration::from_secs(2),
    }
}
fn binding(fixture: &VirtualizedTranscript) -> VerifiedScrollBinding {
    VerifiedScrollBinding::from_verified(
        ScrollDirection::Down,
        vec![AgentAction::Mouse {
            input: MouseInput::Wheel {
                row: 1,
                col: 10,
                direction: WheelDirection::Down,
                steps: 1,
            },
        }],
        fixture.identity(),
    )
    .unwrap()
}

#[test]
fn finds_virtualized_target_never_painted_in_terminal_history() {
    let mut fixture =
        VirtualizedTranscript::new(&["newest page", "middle page", "hidden catch-up"]);
    assert!(!fixture.frame().text.contains("hidden catch-up"));
    assert!(
        !fixture
            .painted
            .iter()
            .any(|page| page.contains("hidden catch-up"))
    );
    let bindings = binding(&fixture);
    let report = scroll_until(
        &mut fixture,
        request("hidden catch-up", 4),
        &bindings,
        &SequenceCancellation::default(),
    )
    .unwrap();
    assert_eq!(report.outcome, ScrollOutcome::Found);
    assert_eq!(report.steps, 2);
    assert_eq!(report.written_events, 2);
    assert_eq!(report.space, "application_viewport");
    assert!(!report.transcript_complete);
    assert!(report.matches.unwrap().any_match);
}

#[test]
fn no_output_at_boundary_uses_equal_revision_and_no_progress_before_deadline() {
    let mut fixture = VirtualizedTranscript::new(&["only page"]);
    let bindings = binding(&fixture);
    let start = Instant::now();
    let report = scroll_until(
        &mut fixture,
        request("absent", 10),
        &bindings,
        &SequenceCancellation::default(),
    )
    .unwrap();
    assert_eq!(report.outcome, ScrollOutcome::NoProgress);
    assert_eq!(report.no_progress_steps, 2);
    assert_eq!(report.steps, 2);
    assert_eq!(report.final_observation.observation_revision, 1);
    assert_eq!(
        fixture.revision, 1,
        "boundary must not manufacture revision advance"
    );
    assert!(start.elapsed() < Duration::from_millis(250));
    assert!(
        fixture
            .slice_lengths
            .iter()
            .all(|slice| *slice <= Duration::from_millis(5))
    );
}

#[test]
fn not_found_is_bounded_and_does_not_claim_full_transcript() {
    let mut fixture = VirtualizedTranscript::new(&["page0", "page1", "page2", "target"]);
    let bindings = binding(&fixture);
    let report = scroll_until(
        &mut fixture,
        request("target", 1),
        &bindings,
        &SequenceCancellation::default(),
    )
    .unwrap();
    assert_eq!(report.outcome, ScrollOutcome::NotFoundWithinBudget);
    assert_eq!(report.steps, 1);
    assert!(!report.transcript_complete);
}

#[test]
fn input_authority_is_checked_for_every_event_without_taking_control() {
    let mut fixture = VirtualizedTranscript::new(&["first", "second", "target"]);
    fixture.lose_authority_after = Some(1);
    let bindings = binding(&fixture);
    let report = scroll_until(
        &mut fixture,
        request("target", 5),
        &bindings,
        &SequenceCancellation::default(),
    )
    .unwrap();
    assert_eq!(report.outcome, ScrollOutcome::InputRejected);
    assert_eq!(fixture.attempts, 2);
    assert_eq!(report.written_events, 1);
}

#[test]
fn run_viewport_and_decreasing_revision_are_rejected() {
    for (run, viewport, regress, outcome) in [
        (true, false, false, ScrollOutcome::StaleRun),
        (false, true, false, ScrollOutcome::IncompatibleViewport),
        (false, false, true, ScrollOutcome::StaleObservation),
    ] {
        let mut fixture = VirtualizedTranscript::new(&["first", "second"]);
        fixture.wrong_run = run;
        fixture.wrong_viewport = viewport;
        fixture.regress = regress;
        let bindings = binding(&fixture);
        let report = scroll_until(
            &mut fixture,
            request("missing", 5),
            &bindings,
            &SequenceCancellation::default(),
        )
        .unwrap();
        assert_eq!(report.outcome, outcome);
        assert_eq!(fixture.attempts, 1);
    }
}

#[test]
fn deadline_cancel_unknown_dispatch_and_explicit_binding_bounds() {
    let mut fixture = VirtualizedTranscript::new(&["one", "two"]);
    let bindings = binding(&fixture);
    let mut expired = request("missing", 4);
    expired.deadline = Instant::now();
    assert_eq!(
        scroll_until(
            &mut fixture,
            expired,
            &bindings,
            &SequenceCancellation::default()
        )
        .unwrap()
        .outcome,
        ScrollOutcome::DeadlineExceeded
    );
    assert_eq!(fixture.attempts, 0);
    let cancel = SequenceCancellation::default();
    cancel.cancel();
    assert_eq!(
        scroll_until(&mut fixture, request("missing", 4), &bindings, &cancel)
            .unwrap()
            .outcome,
        ScrollOutcome::Cancelled
    );
    fixture.unknown = true;
    assert_eq!(
        scroll_until(
            &mut fixture,
            request("missing", 4),
            &bindings,
            &SequenceCancellation::default()
        )
        .unwrap()
        .outcome,
        ScrollOutcome::DispatchUnknown
    );
    let mut invalid = request("missing", 0);
    assert!(
        scroll_until(
            &mut fixture,
            invalid,
            &bindings,
            &SequenceCancellation::default()
        )
        .is_err()
    );
    invalid = request("missing", 2);
    invalid.region.row_end = 99;
    assert!(
        scroll_until(
            &mut fixture,
            invalid,
            &bindings,
            &SequenceCancellation::default()
        )
        .is_err()
    );
    assert!(
        VerifiedScrollBinding::from_verified(
            ScrollDirection::Down,
            vec![AgentAction::Paste {
                text: "not scrolling".into()
            }],
            fixture.identity()
        )
        .is_err()
    );
}
