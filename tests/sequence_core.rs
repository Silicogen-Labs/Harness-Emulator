use silicogen_terminal_runtime::protocol::{AgentAction, MouseButton, MouseInput};
use std::collections::VecDeque;
use std::fs;
use std::path::PathBuf;
use std::time::{Duration, Instant};
use tempfile::TempDir;

mod protocol {
    pub use silicogen_terminal_runtime::protocol::*;
}
mod predicates {
    pub use silicogen_terminal_runtime::predicates::*;
}
#[path = "../src/locators.rs"]
mod locators;
#[path = "../src/wait_engine.rs"]
#[allow(dead_code)]
mod wait_engine;
mod validation {
    pub use silicogen_terminal_runtime::validation::*;
}

#[path = "../src/sequences.rs"]
#[allow(dead_code)]
mod sequences;

use locators::{CellRect, LocatorQuery, ObservationIdentity};
use sequences::{
    ActionReceipt, BarrierBaseline, BarrierCondition, BarrierResult, DelayResult, DispatchResult,
    MAX_KEY_DELAY_MS, MAX_SEQUENCE_JOBS, MAX_SEQUENCE_JOURNAL_BYTES, MAX_SEQUENCE_STEPS,
    SequenceActionIdentity, SequenceAuthorityContext, SequenceBackend, SequenceCancellation,
    SequenceIdentity, SequenceManager, SequenceRequest, SequenceStatus, SequenceStep,
    SequenceViewport, StepStatus,
};
use wait_engine::{FreshTransition, FreshWaitSpec};

fn temp_journal() -> (TempDir, PathBuf) {
    fs::create_dir_all(".runtime").unwrap();
    let dir = TempDir::new_in(".runtime").unwrap();
    let path = dir.path().join("sequence.json");
    (dir, path)
}

fn make_request(sequence_id: &str, steps: Vec<SequenceStep>) -> SequenceRequest {
    SequenceRequest {
        identity: SequenceIdentity {
            actor_id: "agent-a".into(),
            session_id: "session-a".into(),
            run_id: "run-a".into(),
            sequence_id: sequence_id.into(),
        },
        authority: SequenceAuthorityContext {
            expected_viewport_revision: 1,
            expected_cols: 80,
            expected_rows: 24,
            controller_generation: 9,
        },
        timeout_ms: 30_000,
        steps,
    }
}

fn fresh_barrier(text: &str) -> BarrierCondition {
    BarrierCondition::Fresh(FreshWaitSpec {
        query: LocatorQuery::Text(text.into()),
        region: Some(CellRect {
            row_start: 0,
            col_start: 0,
            row_end: 8,
            col_end: 80,
        }),
        transition: FreshTransition::BecamePresent,
    })
}

#[derive(Default)]
struct FakeBackend {
    journal_path: Option<PathBuf>,
    attempts: Vec<(SequenceActionIdentity, AgentAction)>,
    writes: Vec<(SequenceActionIdentity, AgentAction)>,
    barriers: Vec<(ObservationIdentity, BarrierCondition)>,
    barrier_results: VecDeque<BarrierResult>,
    delays: Vec<Duration>,
    current_viewport: Option<SequenceViewport>,
    authorized: bool,
    controller_generation: u64,
    reject_once: Option<String>,
    panic_after_write: bool,
    resize_after_barrier: bool,
    invalid_baseline: bool,
    deadline_on_delay: bool,
}

impl FakeBackend {
    fn with_viewport() -> Self {
        Self {
            current_viewport: Some(SequenceViewport {
                revision: 1,
                cols: 80,
                rows: 24,
            }),
            authorized: true,
            controller_generation: 9,
            ..Self::default()
        }
    }

    fn baseline(&self, frame_id: impl Into<String>) -> ObservationIdentity {
        let viewport = self.current_viewport.unwrap();
        ObservationIdentity {
            session_id: "session-a".into(),
            run_id: "run-a".into(),
            frame_id: frame_id.into(),
            observation_revision: self.writes.len() as u64,
            viewport_revision: viewport.revision,
            cols: viewport.cols,
            rows: viewport.rows,
        }
    }
}

impl SequenceBackend for FakeBackend {
    fn dispatch(
        &mut self,
        identity: &SequenceActionIdentity,
        action: &AgentAction,
        _deadline: Instant,
        cancellation: &SequenceCancellation,
    ) -> DispatchResult {
        self.attempts.push((identity.clone(), action.clone()));
        if let Some(path) = &self.journal_path {
            let journal: serde_json::Value =
                serde_json::from_slice(&fs::read(path).unwrap()).unwrap();
            let step_id = identity.action_id.split(':').nth(2).unwrap();
            let steps = journal["records"][&identity.sequence_id]["steps"]
                .as_array()
                .unwrap();
            let step = steps.iter().find(|step| step["id"] == step_id).unwrap();
            assert_eq!(step["status"], "dispatching");
            assert_eq!(step["dispatching_action_id"], identity.action_id);
        }
        let current = self.current_viewport.unwrap();
        if !self.authorized
            || identity.expected_viewport != current
            || identity.controller_generation != self.controller_generation
            || cancellation.is_cancelled()
        {
            return DispatchResult::Rejected {
                reason: "authority or pinned viewport rejected before write".into(),
            };
        }
        if self.reject_once.as_deref() == Some(&identity.action_id) {
            self.reject_once = None;
            return DispatchResult::Rejected {
                reason: "synthetic pre-write rejection".into(),
            };
        }

        let mut pre_dispatch = self.baseline(format!("before-{}", self.writes.len()));
        if self.invalid_baseline {
            pre_dispatch.run_id = "replacement-run".into();
        }
        self.writes.push((identity.clone(), action.clone()));
        if self.panic_after_write {
            panic!("simulated runner crash after possible PTY write")
        }
        let post_dispatch_viewport = if let AgentAction::Resize { cols, rows } = action {
            let next = SequenceViewport {
                revision: current.revision + 1,
                cols: *cols,
                rows: *rows,
            };
            self.current_viewport = Some(next);
            next
        } else {
            current
        };
        DispatchResult::Written {
            pre_dispatch,
            post_dispatch_viewport,
        }
    }

    fn barrier(
        &mut self,
        _identity: &SequenceIdentity,
        baseline: &ObservationIdentity,
        condition: &BarrierCondition,
        _deadline: Instant,
        cancellation: &SequenceCancellation,
    ) -> BarrierResult {
        self.barriers.push((baseline.clone(), condition.clone()));
        if cancellation.is_cancelled() {
            return BarrierResult::Cancelled;
        }
        let result = self
            .barrier_results
            .pop_front()
            .unwrap_or_else(|| BarrierResult::Matched {
                observation: ObservationIdentity {
                    frame_id: "barrier-result".into(),
                    observation_revision: baseline.observation_revision + 1,
                    ..baseline.clone()
                },
            });
        if self.resize_after_barrier {
            self.resize_after_barrier = false;
            self.current_viewport = Some(SequenceViewport {
                revision: baseline.viewport_revision + 2,
                cols: baseline.cols,
                rows: baseline.rows,
            });
        }
        result
    }

    fn delay(
        &mut self,
        duration: Duration,
        _deadline: Instant,
        cancellation: &SequenceCancellation,
    ) -> DelayResult {
        self.delays.push(duration);
        if cancellation.is_cancelled() {
            DelayResult::Cancelled
        } else if self.deadline_on_delay {
            DelayResult::DeadlineExceeded
        } else {
            DelayResult::Elapsed
        }
    }
}

#[test]
fn delayed_palette_and_query_barriers_resume_without_replaying_inputs_or_fixed_sleep() {
    let (_dir, journal_path) = temp_journal();
    let mut manager = SequenceManager::open(&journal_path).unwrap();
    let mut backend = FakeBackend::with_viewport();
    backend.journal_path = Some(journal_path);
    backend.barrier_results = VecDeque::from([
        BarrierResult::Pending,
        BarrierResult::Matched {
            observation: ObservationIdentity {
                session_id: "session-a".into(),
                run_id: "run-a".into(),
                frame_id: "palette-visible".into(),
                observation_revision: 2,
                viewport_revision: 1,
                cols: 80,
                rows: 24,
            },
        },
        BarrierResult::Pending,
        BarrierResult::Matched {
            observation: ObservationIdentity {
                session_id: "session-a".into(),
                run_id: "run-a".into(),
                frame_id: "query-visible".into(),
                observation_revision: 4,
                viewport_revision: 1,
                cols: 80,
                rows: 24,
            },
        },
    ]);
    let request = make_request(
        "navigation",
        vec![
            SequenceStep::Input {
                id: "open".into(),
                action: AgentAction::Key {
                    key: "Ctrl+P".into(),
                },
            },
            SequenceStep::Barrier {
                id: "palette".into(),
                baseline: BarrierBaseline::PreviousInput,
                condition: fresh_barrier("Palette"),
            },
            SequenceStep::Input {
                id: "query".into(),
                action: AgentAction::Type {
                    text: "file".into(),
                },
            },
            SequenceStep::Barrier {
                id: "query_echo".into(),
                baseline: BarrierBaseline::PreviousInput,
                condition: fresh_barrier("file"),
            },
            SequenceStep::Input {
                id: "select".into(),
                action: AgentAction::Key {
                    key: "Enter".into(),
                },
            },
        ],
    );
    let cancellation = SequenceCancellation::default();

    let first = manager
        .execute(request.clone(), &mut backend, &cancellation)
        .unwrap();
    assert_eq!(first.status, SequenceStatus::Pending);
    assert_eq!(first.completed_step_ids, ["open"]);
    let second = manager
        .execute(request.clone(), &mut backend, &cancellation)
        .unwrap();
    assert_eq!(second.status, SequenceStatus::Pending);
    assert_eq!(first.deadline_unix_ms, second.deadline_unix_ms);
    assert_eq!(second.completed_step_ids, ["open", "palette", "query"]);
    let third = manager
        .execute(request, &mut backend, &cancellation)
        .unwrap();
    assert_eq!(third.status, SequenceStatus::Completed);
    assert_eq!(second.deadline_unix_ms, third.deadline_unix_ms);
    assert_eq!(backend.writes.len(), 3);
    assert!(backend.delays.is_empty());
    assert_eq!(
        backend
            .writes
            .iter()
            .map(|(id, _)| id.action_id.as_str())
            .collect::<Vec<_>>(),
        [
            "seq:navigation:open:0",
            "seq:navigation:query:0",
            "seq:navigation:select:0"
        ]
    );
    assert!(backend.barriers[0].0.frame_id.starts_with("before-"));
}

#[test]
fn leader_keys_are_sequential_and_use_bounded_inter_key_delay() {
    let (_dir, path) = temp_journal();
    let mut manager = SequenceManager::open(path).unwrap();
    let mut backend = FakeBackend::with_viewport();
    let request = make_request(
        "leader",
        vec![SequenceStep::LeaderKeys {
            id: "binding".into(),
            keys: vec!["Ctrl+X".into(), "L".into()],
            delay_ms: 17,
        }],
    );
    let result = manager
        .execute(request, &mut backend, &SequenceCancellation::default())
        .unwrap();
    assert_eq!(result.status, SequenceStatus::Completed);
    assert_eq!(backend.writes.len(), 2);
    assert_eq!(backend.writes[0].0.action_id, "seq:leader:binding:0");
    assert_eq!(backend.writes[1].0.action_id, "seq:leader:binding:1");
    assert!(matches!(&backend.writes[0].1, AgentAction::Key { key } if key == "Ctrl+X"));
    assert!(matches!(&backend.writes[1].1, AgentAction::Key { key } if key == "L"));
    assert_eq!(backend.delays, [Duration::from_millis(17)]);
}

#[test]
fn excessive_leader_key_delay_is_rejected_before_any_write() {
    let (_dir, path) = temp_journal();
    let mut manager = SequenceManager::open(path).unwrap();
    let mut backend = FakeBackend::with_viewport();
    let request = make_request(
        "long-delay",
        vec![SequenceStep::LeaderKeys {
            id: "binding".into(),
            keys: vec!["Ctrl+X".into(), "L".into()],
            delay_ms: MAX_KEY_DELAY_MS + 1,
        }],
    );
    assert!(
        manager
            .execute(request, &mut backend, &SequenceCancellation::default())
            .is_err()
    );
    assert!(backend.writes.is_empty());

    let invalid_region = make_request(
        "invalid-region",
        vec![
            SequenceStep::Input {
                id: "would-write".into(),
                action: AgentAction::Key {
                    key: "Enter".into(),
                },
            },
            SequenceStep::Barrier {
                id: "out-of-bounds".into(),
                baseline: BarrierBaseline::PreviousInput,
                condition: BarrierCondition::Fresh(FreshWaitSpec {
                    query: LocatorQuery::Text("READY".into()),
                    region: Some(CellRect {
                        row_start: 0,
                        col_start: 0,
                        row_end: 25,
                        col_end: 80,
                    }),
                    transition: FreshTransition::BecamePresent,
                }),
            },
        ],
    );
    assert!(
        manager
            .execute(
                invalid_region,
                &mut backend,
                &SequenceCancellation::default()
            )
            .is_err()
    );
    assert!(backend.writes.is_empty());
}

#[test]
fn step_and_job_limits_reject_before_the_next_write() {
    let (_dir, path) = temp_journal();
    let mut manager = SequenceManager::open(path).unwrap();
    let mut backend = FakeBackend::with_viewport();
    let too_many_steps = make_request(
        "too-many-steps",
        (0..=MAX_SEQUENCE_STEPS)
            .map(|index| SequenceStep::Input {
                id: format!("step-{index}"),
                action: AgentAction::Key { key: "A".into() },
            })
            .collect(),
    );
    assert!(
        manager
            .execute(
                too_many_steps,
                &mut backend,
                &SequenceCancellation::default()
            )
            .is_err()
    );
    assert!(backend.writes.is_empty());

    for index in 0..MAX_SEQUENCE_JOBS {
        let request = make_request(
            &format!("job-{index}"),
            vec![SequenceStep::Input {
                id: "write".into(),
                action: AgentAction::Key { key: "A".into() },
            }],
        );
        assert_eq!(
            manager
                .execute(request, &mut backend, &SequenceCancellation::default())
                .unwrap()
                .status,
            SequenceStatus::Completed
        );
    }
    let overflow = make_request(
        "job-overflow",
        vec![SequenceStep::Input {
            id: "write".into(),
            action: AgentAction::Key { key: "A".into() },
        }],
    );
    let error = manager.admit(&overflow).unwrap_err();
    let error = error
        .downcast_ref::<validation::RequestError>()
        .expect("journal saturation is a typed request error");
    assert_eq!(error.code, "sequence_limit");
    assert_eq!(error.field, "request");
    assert_eq!(backend.writes.len(), MAX_SEQUENCE_JOBS);
}

#[test]
fn pending_leader_keys_report_all_planned_and_only_unwritten_action_ids() {
    let (_dir, path) = temp_journal();
    let mut manager = SequenceManager::open(path).unwrap();
    let mut backend = FakeBackend::with_viewport();
    backend.deadline_on_delay = true;
    let request = make_request(
        "partial-leader",
        vec![SequenceStep::LeaderKeys {
            id: "palette-keys".into(),
            keys: vec!["Ctrl+P".into(), "s".into(), "Enter".into()],
            delay_ms: 1,
        }],
    );

    let report = manager
        .execute(request, &mut backend, &SequenceCancellation::default())
        .unwrap();

    assert_eq!(report.status, SequenceStatus::TimedOut);
    let step = &report.steps[0];
    assert_eq!(step.action_ids.len(), 3);
    assert_eq!(step.completed_action_ids, vec![step.action_ids[0].clone()]);
    assert_eq!(step.pending_action_ids, step.action_ids[1..]);
    assert!(step.dispatching_action_ids.is_empty());
    assert!(step.unknown_action_ids.is_empty());
    assert_eq!(step.receipts.len(), 1);
}

#[test]
fn journal_byte_limit_rejects_admission_before_dispatch() {
    let (_dir, path) = temp_journal();
    let mut manager = SequenceManager::open(path).unwrap();
    let mut backend = FakeBackend::with_viewport();
    let large_text = "x".repeat(MAX_SEQUENCE_JOURNAL_BYTES / 5);
    for index in 0..4 {
        let request = make_request(
            &format!("large-{index}"),
            vec![SequenceStep::Input {
                id: "type".into(),
                action: AgentAction::Type {
                    text: large_text.clone(),
                },
            }],
        );
        manager
            .execute(request, &mut backend, &SequenceCancellation::default())
            .unwrap();
    }
    let last = make_request(
        "large-overflow",
        vec![SequenceStep::Input {
            id: "type".into(),
            action: AgentAction::Type { text: large_text },
        }],
    );
    assert!(
        manager
            .execute(last, &mut backend, &SequenceCancellation::default())
            .unwrap_err()
            .to_string()
            .contains("journal would exceed")
    );
    assert_eq!(backend.writes.len(), 4);
}

#[test]
fn malformed_later_step_is_rejected_before_any_input_write() {
    let (_dir, path) = temp_journal();
    let mut manager = SequenceManager::open(path).unwrap();
    let mut backend = FakeBackend::with_viewport();
    let request = make_request(
        "malformed",
        vec![
            SequenceStep::Input {
                id: "valid".into(),
                action: AgentAction::Key {
                    key: "Enter".into(),
                },
            },
            SequenceStep::Input {
                id: "invalid".into(),
                action: AgentAction::Key {
                    key: "Ctrl+NotAKey".into(),
                },
            },
        ],
    );
    assert!(
        manager
            .execute(request, &mut backend, &SequenceCancellation::default())
            .is_err()
    );
    assert!(backend.writes.is_empty());
}

#[test]
fn definite_partial_failure_retries_only_unwritten_steps_and_conflicts_on_body_change() {
    let (_dir, path) = temp_journal();
    let mut manager = SequenceManager::open(path).unwrap();
    let mut backend = FakeBackend::with_viewport();
    backend.reject_once = Some("seq:retry:second:0".into());
    let request = make_request(
        "retry",
        vec![
            SequenceStep::Input {
                id: "first".into(),
                action: AgentAction::Key {
                    key: "Ctrl+P".into(),
                },
            },
            SequenceStep::Input {
                id: "second".into(),
                action: AgentAction::Type {
                    text: "filter".into(),
                },
            },
        ],
    );
    let partial = manager
        .execute(
            request.clone(),
            &mut backend,
            &SequenceCancellation::default(),
        )
        .unwrap();
    assert_eq!(partial.status, SequenceStatus::PartialFailure);
    assert_eq!(partial.completed_step_ids, ["first"]);
    assert_eq!(partial.failing_step_id.as_deref(), Some("second"));

    let completed = manager
        .execute(
            request.clone(),
            &mut backend,
            &SequenceCancellation::default(),
        )
        .unwrap();
    assert_eq!(completed.status, SequenceStatus::Completed);
    assert_eq!(backend.writes.len(), 2);
    assert_eq!(backend.writes[0].0.action_id, "seq:retry:first:0");
    assert_eq!(backend.writes[1].0.action_id, "seq:retry:second:0");
    assert_eq!(
        backend
            .attempts
            .iter()
            .filter(|(identity, _)| identity.action_id == "seq:retry:first:0")
            .count(),
        1
    );

    let changed = make_request(
        "retry",
        vec![SequenceStep::Input {
            id: "first".into(),
            action: AgentAction::Type {
                text: "different".into(),
            },
        }],
    );
    assert!(
        manager
            .execute(changed, &mut backend, &SequenceCancellation::default())
            .unwrap_err()
            .to_string()
            .contains("sequence_conflict")
    );
}

#[test]
fn explicit_resize_advances_only_the_pinned_viewport_for_later_actions() {
    let (_dir, path) = temp_journal();
    let mut manager = SequenceManager::open(path).unwrap();
    let mut backend = FakeBackend::with_viewport();
    let request = make_request(
        "resize-sequence",
        vec![
            SequenceStep::Input {
                id: "resize".into(),
                action: AgentAction::Resize {
                    cols: 100,
                    rows: 30,
                },
            },
            SequenceStep::Input {
                id: "click".into(),
                action: AgentAction::Mouse {
                    input: MouseInput::Click {
                        row: 2,
                        col: 3,
                        button: MouseButton::Left,
                    },
                },
            },
        ],
    );
    let result = manager
        .execute(request, &mut backend, &SequenceCancellation::default())
        .unwrap();
    assert_eq!(result.status, SequenceStatus::Completed);
    assert_eq!(backend.writes[0].0.expected_viewport.revision, 1);
    assert_eq!(backend.writes[1].0.expected_viewport.revision, 2);
    assert_eq!(backend.writes[1].0.expected_viewport.cols, 100);
    assert_eq!(backend.writes[1].0.controller_generation, 9);
    assert!(!result.steps[0].receipts[0].written_to_pty);
    assert_eq!(
        result.steps[0].receipts[0].input_disposition,
        sequences::InputDisposition::PtyResized
    );
}

#[test]
fn resize_away_and_back_and_control_handoff_are_rejected_without_rebinding() {
    let (_dir, path) = temp_journal();
    let mut manager = SequenceManager::open(path).unwrap();
    let mut backend = FakeBackend::with_viewport();
    backend.resize_after_barrier = true;
    backend.barrier_results.push_back(BarrierResult::Matched {
        observation: ObservationIdentity {
            session_id: "session-a".into(),
            run_id: "run-a".into(),
            frame_id: "before-external-resize".into(),
            observation_revision: 2,
            viewport_revision: 1,
            cols: 80,
            rows: 24,
        },
    });
    let request = make_request(
        "stale-viewport",
        vec![
            SequenceStep::Input {
                id: "open".into(),
                action: AgentAction::Key {
                    key: "Ctrl+P".into(),
                },
            },
            SequenceStep::Barrier {
                id: "ready".into(),
                baseline: BarrierBaseline::PreviousInput,
                condition: fresh_barrier("Palette"),
            },
            SequenceStep::Input {
                id: "click".into(),
                action: AgentAction::Mouse {
                    input: MouseInput::Click {
                        row: 1,
                        col: 1,
                        button: MouseButton::Left,
                    },
                },
            },
        ],
    );
    let result = manager
        .execute(request, &mut backend, &SequenceCancellation::default())
        .unwrap();
    assert_eq!(result.status, SequenceStatus::PartialFailure);
    assert_eq!(backend.writes.len(), 1);
    let rejected_identity = &backend.attempts.last().unwrap().0;
    assert_eq!(rejected_identity.expected_viewport.revision, 1);
    assert_eq!(rejected_identity.expected_viewport.cols, 80);

    let (_dir2, path2) = temp_journal();
    let mut handoff_manager = SequenceManager::open(path2).unwrap();
    let mut handoff_backend = FakeBackend::with_viewport();
    handoff_backend
        .barrier_results
        .push_back(BarrierResult::Pending);
    handoff_backend
        .barrier_results
        .push_back(BarrierResult::Matched {
            observation: ObservationIdentity {
                session_id: "session-a".into(),
                run_id: "run-a".into(),
                frame_id: "palette".into(),
                observation_revision: 2,
                viewport_revision: 1,
                cols: 80,
                rows: 24,
            },
        });
    let handoff_request = make_request(
        "handoff",
        vec![
            SequenceStep::Input {
                id: "open".into(),
                action: AgentAction::Key {
                    key: "Ctrl+P".into(),
                },
            },
            SequenceStep::Barrier {
                id: "ready".into(),
                baseline: BarrierBaseline::PreviousInput,
                condition: fresh_barrier("Palette"),
            },
            SequenceStep::Input {
                id: "type".into(),
                action: AgentAction::Type {
                    text: "query".into(),
                },
            },
        ],
    );
    assert_eq!(
        handoff_manager
            .execute(
                handoff_request.clone(),
                &mut handoff_backend,
                &SequenceCancellation::default()
            )
            .unwrap()
            .status,
        SequenceStatus::Pending
    );
    handoff_backend.controller_generation = 10;
    let result = handoff_manager
        .execute(
            handoff_request,
            &mut handoff_backend,
            &SequenceCancellation::default(),
        )
        .unwrap();
    assert_eq!(result.status, SequenceStatus::PartialFailure);
    assert_eq!(handoff_backend.writes.len(), 1);
    assert_eq!(
        handoff_backend
            .attempts
            .last()
            .unwrap()
            .0
            .controller_generation,
        9
    );
}

#[test]
fn cancellation_and_uncertain_restart_never_replay_an_input() {
    let (_dir, path) = temp_journal();
    let mut manager = SequenceManager::open(&path).unwrap();
    let mut backend = FakeBackend::with_viewport();
    let request = make_request(
        "cancelled",
        vec![SequenceStep::Input {
            id: "enter".into(),
            action: AgentAction::Key {
                key: "Enter".into(),
            },
        }],
    );
    let cancellation = SequenceCancellation::default();
    cancellation.cancel();
    assert_eq!(
        manager
            .execute(request, &mut backend, &cancellation)
            .unwrap()
            .status,
        SequenceStatus::Cancelled
    );
    assert!(backend.writes.is_empty());

    let panic_request = make_request(
        "crash-window",
        vec![SequenceStep::Input {
            id: "possibly-written".into(),
            action: AgentAction::Type {
                text: "once".into(),
            },
        }],
    );
    let (_crash_dir, crash_path) = temp_journal();
    let mut crashing_manager = SequenceManager::open(&crash_path).unwrap();
    let mut crashing_backend = FakeBackend::with_viewport();
    crashing_backend.panic_after_write = true;
    let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let _ = crashing_manager.execute(
            panic_request.clone(),
            &mut crashing_backend,
            &SequenceCancellation::default(),
        );
    }));
    assert!(panic.is_err());
    assert_eq!(crashing_backend.writes.len(), 1);
    drop(crashing_manager);

    let mut recovered = SequenceManager::open(&crash_path).unwrap();
    let mut retry_backend = FakeBackend::with_viewport();
    let report = recovered
        .execute(
            panic_request,
            &mut retry_backend,
            &SequenceCancellation::default(),
        )
        .unwrap();
    assert_eq!(report.status, SequenceStatus::InterruptedUnknown);
    assert_eq!(report.steps[0].status, StepStatus::InterruptedUnknown);
    assert!(retry_backend.writes.is_empty());
}

#[test]
fn explicit_cancel_marks_waiting_steps_and_keeps_completed_receipts() {
    let (_dir, path) = temp_journal();
    let mut manager = SequenceManager::open(path).unwrap();
    let mut backend = FakeBackend::with_viewport();
    backend.barrier_results.push_back(BarrierResult::Pending);
    let request = make_request(
        "cancel-wait",
        vec![
            SequenceStep::Input {
                id: "open".into(),
                action: AgentAction::Key {
                    key: "Ctrl+P".into(),
                },
            },
            SequenceStep::Barrier {
                id: "visible".into(),
                baseline: BarrierBaseline::PreviousInput,
                condition: fresh_barrier("Palette"),
            },
            SequenceStep::Input {
                id: "select".into(),
                action: AgentAction::Key {
                    key: "Enter".into(),
                },
            },
        ],
    );
    let pending = manager
        .execute(request, &mut backend, &SequenceCancellation::default())
        .unwrap();
    assert_eq!(pending.status, SequenceStatus::Pending);
    let cancelled = manager.cancel("cancel-wait").unwrap();
    assert_eq!(cancelled.status, SequenceStatus::Cancelled);
    assert_eq!(cancelled.completed_step_ids, ["open"]);
    assert_eq!(cancelled.steps[1].status, StepStatus::Cancelled);
    assert_eq!(cancelled.steps[2].status, StepStatus::Cancelled);
    assert_eq!(backend.writes.len(), 1);
}

#[test]
fn dropping_and_reloading_cancels_waiting_work_without_replaying_completed_input() {
    let (_dir, path) = temp_journal();
    let request = make_request(
        "reload-cancel",
        vec![
            SequenceStep::Input {
                id: "open".into(),
                action: AgentAction::Key {
                    key: "Ctrl+P".into(),
                },
            },
            SequenceStep::Barrier {
                id: "visible".into(),
                baseline: BarrierBaseline::PreviousInput,
                condition: fresh_barrier("Palette"),
            },
            SequenceStep::Input {
                id: "select".into(),
                action: AgentAction::Key {
                    key: "Enter".into(),
                },
            },
        ],
    );
    let mut manager = SequenceManager::open(&path).unwrap();
    let mut first_backend = FakeBackend::with_viewport();
    first_backend
        .barrier_results
        .push_back(BarrierResult::Pending);
    assert_eq!(
        manager
            .execute(
                request.clone(),
                &mut first_backend,
                &SequenceCancellation::default(),
            )
            .unwrap()
            .status,
        SequenceStatus::Pending
    );
    assert_eq!(first_backend.writes.len(), 1);
    drop(manager);

    let mut recovered = SequenceManager::open(&path).unwrap();
    let mut retry_backend = FakeBackend::with_viewport();
    let report = recovered
        .execute(
            request,
            &mut retry_backend,
            &SequenceCancellation::default(),
        )
        .unwrap();
    assert_eq!(report.status, SequenceStatus::Cancelled);
    assert_eq!(report.completed_step_ids, ["open"]);
    assert_eq!(report.steps[1].status, StepStatus::Cancelled);
    assert_eq!(report.steps[2].status, StepStatus::Cancelled);
    assert!(retry_backend.writes.is_empty());
}

#[test]
fn written_receipts_keep_pre_dispatch_identity_and_reject_wrong_run_baselines() {
    let (_dir, path) = temp_journal();
    let mut manager = SequenceManager::open(path).unwrap();
    let mut backend = FakeBackend::with_viewport();
    backend.invalid_baseline = true;
    let request = make_request(
        "bad-baseline",
        vec![SequenceStep::Input {
            id: "write".into(),
            action: AgentAction::Type { text: "one".into() },
        }],
    );
    let report = manager
        .execute(request, &mut backend, &SequenceCancellation::default())
        .unwrap();
    assert_eq!(report.status, SequenceStatus::PartialFailure);
    let receipt: &ActionReceipt = &report.steps[0].receipts[0];
    assert!(receipt.written_to_pty);
    assert_eq!(receipt.identity.action_id, "seq:bad-baseline:write:0");
    assert_eq!(receipt.pre_dispatch.run_id, "replacement-run");
    let attempts_before_retry = backend.attempts.len();
    let retry = manager
        .execute(
            make_request(
                "bad-baseline",
                vec![SequenceStep::Input {
                    id: "write".into(),
                    action: AgentAction::Type { text: "one".into() },
                }],
            ),
            &mut backend,
            &SequenceCancellation::default(),
        )
        .unwrap();
    assert_eq!(retry.status, SequenceStatus::PartialFailure);
    assert_eq!(backend.attempts.len(), attempts_before_retry);
}
