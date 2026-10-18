use anyhow::{Result, bail};
use silicogen_terminal_runtime::protocol::{
    CellSnapshot, CellStyle, CursorSnapshot, FrameSnapshot,
};
use std::sync::{Arc, Mutex};

mod protocol {
    pub use silicogen_terminal_runtime::protocol::*;
}
#[path = "../src/capture_jobs.rs"]
mod capture_jobs;
#[path = "../src/locators.rs"]
mod locators;

use capture_jobs::*;
use locators::{LocatorQuery, ObservationIdentity};

#[derive(Default)]
struct Hooks {
    events: Vec<String>,
    checkpoints: Vec<CaptureCheckpoint>,
    materialized_texts: Vec<String>,
    jobs: usize,
    frames: usize,
    reject_admit: bool,
    reject_frame: bool,
    reject_checkpoint: bool,
}

struct Lease {
    hooks: Arc<Mutex<Hooks>>,
    job: bool,
}
impl Drop for Lease {
    fn drop(&mut self) {
        let mut hooks = self.hooks.lock().unwrap();
        if self.job {
            hooks.jobs -= 1;
        } else {
            hooks.frames -= 1;
        }
    }
}
struct Backend(Arc<Mutex<Hooks>>);
impl CaptureBackend for Backend {
    type JobLease = Lease;
    type FrameLease = Lease;
    fn admit(&mut self, request: &CaptureRequest, _: &ObservationIdentity) -> Result<Lease> {
        let mut hooks = self.0.lock().unwrap();
        if hooks.reject_admit {
            bail!("synthetic admission failure")
        }
        hooks.events.push(format!("admit:{}", request.handle));
        hooks.jobs += 1;
        Ok(Lease {
            hooks: self.0.clone(),
            job: true,
        })
    }
    fn retain(&mut self, handle: &str, frame: &CaptureFrameRef) -> Result<Lease> {
        let mut hooks = self.0.lock().unwrap();
        if hooks.reject_frame {
            bail!("synthetic frame lease failure")
        }
        assert!(
            hooks.jobs > 0,
            "job must be admitted before retaining any frame"
        );
        hooks
            .events
            .push(format!("retain:{handle}:{}", frame.index));
        hooks.frames += 1;
        Ok(Lease {
            hooks: self.0.clone(),
            job: false,
        })
    }
    fn checkpoint(&mut self, checkpoint: &CaptureCheckpoint) -> Result<()> {
        let mut hooks = self.0.lock().unwrap();
        if hooks.reject_checkpoint {
            bail!("synthetic checkpoint failure")
        }
        hooks
            .events
            .push(format!("checkpoint:{}", checkpoint.request.handle));
        hooks.checkpoints.push(checkpoint.clone());
        Ok(())
    }
    fn materialize(
        &mut self,
        handle: &str,
        frame: &CaptureFrameRef,
        snapshot: &FrameSnapshot,
    ) -> Result<MaterializedCapture> {
        assert_eq!(snapshot.frame_id, frame.observation.frame_id);
        self.0
            .lock()
            .unwrap()
            .materialized_texts
            .push(snapshot.text.clone());
        Ok(MaterializedCapture {
            reference: format!("synthetic:{handle}:{}", frame.index),
            observation: frame.observation.clone(),
        })
    }
}

fn style() -> CellStyle {
    CellStyle {
        foreground: "#ffffff".into(),
        background: "#000000".into(),
        bold: false,
        dim: false,
        italic: false,
        inverse: false,
        invisible: false,
        strike: false,
        blink: false,
        underline: "none".into(),
        underline_color: None,
        hyperlink: None,
    }
}

fn cell(text: &str) -> CellSnapshot {
    CellSnapshot {
        text: text.into(),
        width: 1,
        foreground: None,
        background: None,
        resolved_foreground: "#ffffff".into(),
        resolved_background: "#000000".into(),
        bold: false,
        dim: false,
        italic: false,
        inverse: false,
        invisible: false,
        strike: false,
        blink: false,
        underline: "none".into(),
        underline_color: None,
        hyperlink: None,
    }
}

fn observation(text: &str, revision: u64) -> CaptureObservation {
    let mut row: Vec<_> = text.chars().map(|ch| cell(&ch.to_string())).collect();
    row.resize_with(20, || cell(" "));
    let frame = FrameSnapshot {
        session_id: "s".into(),
        run_id: "r".into(),
        frame_id: format!("f{revision}"),
        captured_at_ms: u128::from(revision),
        output_sequence: revision,
        cols: 20,
        rows: 1,
        text: text.into(),
        base_style: style(),
        styled_runs: Vec::new(),
        cells: vec![row],
        cursor: CursorSnapshot {
            row: 0,
            col: 0,
            visible: true,
            shape: "block".into(),
            color: "#ffffff".into(),
        },
        modes: Default::default(),
        keyboard_mode: Vec::new(),
        mouse_tracking: "none".into(),
        mouse_encoding: "none".into(),
        default_foreground: "#ffffff".into(),
        default_background: "#000000".into(),
        cursor_color: "#ffffff".into(),
        palette: Default::default(),
        title: None,
        process_state: "running".into(),
        capture_complete: true,
        capture_warning: None,
    };
    CaptureObservation {
        frame: Arc::new(frame),
        identity: ObservationIdentity {
            session_id: "s".into(),
            run_id: "r".into(),
            frame_id: format!("f{revision}"),
            observation_revision: revision,
            viewport_revision: 7,
            cols: 20,
            rows: 1,
        },
        producer: ProducerProgress {
            event_sequence: revision,
            dropped_events: 0,
            coalesced_events: 0,
        },
    }
}

fn query(text: &str) -> CaptureQuery {
    CaptureQuery {
        query: LocatorQuery::Text(text.into()),
        region: None,
    }
}
fn request(handle: &str) -> CaptureRequest {
    CaptureRequest {
        handle: handle.into(),
        authority: CaptureAuthority {
            session_id: "s".into(),
            run_id: "r".into(),
            viewport_revision: 7,
            cols: 20,
            rows: 1,
        },
        appearance: query("TOAST"),
        completion: None,
        burst: None,
        budget: CaptureBudget {
            max_frames: 8,
            max_bytes: 2 * 1024 * 1024,
            max_frame_bytes: 256 * 1024,
        },
        timeout_ms: 1000,
    }
}
fn manager() -> (CaptureManager<Backend>, Arc<Mutex<Hooks>>) {
    let hooks = Arc::new(Mutex::new(Hooks::default()));
    (
        CaptureManager::new(Backend(hooks.clone()), CaptureLimits::default()).unwrap(),
        hooks,
    )
}

#[test]
fn short_toast_appearance_and_replacement_are_latched_before_any_status_poll() {
    let (mut manager, hooks) = manager();
    let mut req = request("toast");
    req.completion = Some(CaptureCompletion::Replaced(query("DONE")));
    let armed = manager.start(req, observation("idle", 1), 100).unwrap();
    assert_eq!(armed.baseline.frame_id, "f1");
    assert_eq!(manager.status("toast").unwrap().state, CaptureState::Armed);
    assert_eq!(
        hooks.lock().unwrap().events,
        ["admit:toast", "checkpoint:toast"]
    );
    // This represents the triggered input's producer frames, not browser frames.
    manager
        .advance("toast", 101, Some(observation("TOAST", 2)))
        .unwrap();
    manager
        .advance("toast", 102, Some(observation("DONE", 3)))
        .unwrap();
    let later_poll = manager.status("toast").unwrap();
    assert_eq!(later_poll.state, CaptureState::Completed);
    assert_eq!(
        later_poll.appearance.as_ref().unwrap().observation.frame_id,
        "f2"
    );
    assert_eq!(
        later_poll.transition.as_ref().unwrap().observation.frame_id,
        "f3"
    );
    assert_eq!(later_poll.frames.len(), 2);
    manager
        .materialize("toast", later_poll.appearance.unwrap().frame_index.unwrap())
        .unwrap();
    assert_eq!(hooks.lock().unwrap().materialized_texts, ["TOAST"]);
    assert!(!later_poll.coverage.all_transitions_observed);
    assert!(later_poll.coverage.producer_may_skip_intermediate_states);
}

#[test]
fn disappearance_requires_appearance_after_arm_and_old_presence_needs_absence() {
    let (mut manager, _) = manager();
    let mut req = request("disappear");
    req.completion = Some(CaptureCompletion::Disappeared);
    manager.start(req, observation("TOAST", 1), 0).unwrap();
    assert!(
        manager
            .advance("disappear", 1, Some(observation("TOAST", 2)))
            .unwrap()
            .appearance
            .is_none()
    );
    manager
        .advance("disappear", 2, Some(observation("", 3)))
        .unwrap();
    manager
        .advance("disappear", 3, Some(observation("TOAST", 4)))
        .unwrap();
    let final_status = manager
        .advance("disappear", 4, Some(observation("", 5)))
        .unwrap();
    assert_eq!(final_status.state, CaptureState::Completed);
    assert_eq!(final_status.appearance.unwrap().observation.frame_id, "f4");
    assert_eq!(final_status.transition.unwrap().observation.frame_id, "f5");
}

#[test]
fn timer_ticks_finish_burst_and_deadline_without_output() {
    let (mut manager, _) = manager();
    let mut req = request("timer");
    req.burst = Some(SampleBurst {
        interval_ms: 10,
        duration_ms: 40,
    });
    manager.start(req, observation("", 1), 100).unwrap();
    assert_eq!(manager.next_wakeup_ms("timer").unwrap(), Some(110));
    manager
        .advance("timer", 101, Some(observation("TOAST", 2)))
        .unwrap();
    let done = manager.advance("timer", 140, None).unwrap();
    assert_eq!(done.state, CaptureState::Completed);
    assert_eq!(done.coverage.missed_sample_slots, 3);
    assert!(done.burst_finished);
    assert_eq!(manager.next_wakeup_ms("timer").unwrap(), None);
    manager
        .start(request("timeout"), observation("", 1), 100)
        .unwrap();
    assert_eq!(
        manager.advance("timeout", 1100, None).unwrap().state,
        CaptureState::TimedOut
    );
    assert_eq!(
        manager
            .advance("timeout", 1101, Some(observation("TOAST", 2)))
            .unwrap()
            .state,
        CaptureState::TimedOut
    );
}

#[test]
fn gaps_coalescing_source_fault_and_late_sampling_are_reported_honestly() {
    let (mut manager, _) = manager();
    let mut req = request("gaps");
    req.burst = Some(SampleBurst {
        interval_ms: 10,
        duration_ms: 50,
    });
    manager.start(req, observation("", 1), 0).unwrap();
    let mut toast = observation("TOAST", 5);
    toast.producer.event_sequence = 7;
    toast.producer.dropped_events = 2;
    toast.producer.coalesced_events = 3;
    Arc::get_mut(&mut toast.frame).unwrap().capture_complete = false;
    let status = manager.advance("gaps", 35, Some(toast)).unwrap();
    assert_eq!(status.coverage.revision_gap_units, 3);
    assert_eq!(status.coverage.event_gap_units, 5);
    assert_eq!(status.coverage.producer_dropped_events, 2);
    assert_eq!(status.coverage.producer_coalesced_events, 3);
    assert_eq!(status.coverage.missed_sample_slots, 2);
    assert!(status.coverage.source_capture_incomplete);
    assert!(status.coverage.sampling_is_partial);
    let status = manager
        .advance("gaps", 40, Some(observation("old", 2)))
        .unwrap();
    assert_eq!(status.coverage.out_of_order_notifications, 1);
    assert_eq!(status.coverage.missed_sample_slots, 3);
}

#[test]
fn frame_and_byte_overflow_preserve_the_reserved_appearance() {
    let (mut manager, _) = manager();
    let mut req = request("frames");
    req.budget.max_frames = 1;
    req.burst = Some(SampleBurst {
        interval_ms: 10,
        duration_ms: 50,
    });
    manager.start(req, observation("", 1), 0).unwrap();
    let status = manager
        .advance("frames", 10, Some(observation("TOAST", 2)))
        .unwrap();
    assert_eq!(status.frames.len(), 1);
    assert!(status.frames[0].appearance);
    assert!(status.coverage.frame_limit_reached);
    assert!(status.appearance.unwrap().frame_index.is_some());

    let toast = observation("TOAST", 2);
    let charge = snapshot_retention_bytes(&toast.frame).unwrap();
    let mut req = request("bytes");
    req.completion = Some(CaptureCompletion::Disappeared);
    req.budget.max_bytes = charge;
    req.budget.max_frame_bytes = charge;
    manager.start(req, observation("", 1), 0).unwrap();
    manager.advance("bytes", 1, Some(toast)).unwrap();
    let status = manager
        .advance("bytes", 2, Some(observation("", 3)))
        .unwrap();
    assert_eq!(status.state, CaptureState::Completed);
    assert_eq!(status.frames.len(), 1);
    assert!(status.coverage.byte_limit_reached);
    assert_eq!(status.coverage.dropped_frames, 1);
    assert!(status.transition.unwrap().frame_index.is_none());
}

#[test]
fn full_cells_styles_graphemes_and_spare_capacity_are_charged() {
    let mut dense = observation("", 2);
    let frame = Arc::get_mut(&mut dense.frame).unwrap();
    frame.cols = 300;
    frame.rows = 120;
    frame.cells = (0..120)
        .map(|row| {
            (0..300)
                .map(|col| {
                    let mut glyph = cell(&"e\u{301}".repeat(8));
                    glyph.bold = col % 2 == 0;
                    glyph.resolved_foreground = format!("#{:06x}", row * 300 + col);
                    glyph.hyperlink = Some(format!("https://synthetic.invalid/{row}/{col}"));
                    glyph
                })
                .collect()
        })
        .collect();
    dense.identity.cols = 300;
    dense.identity.rows = 120;
    let view_bytes = serde_json::to_vec(&dense.frame).unwrap().len() as u64;
    let charge = snapshot_retention_bytes(&dense.frame).unwrap();
    assert!(charge > 10 * 1024 * 1024);
    assert!(charge > view_bytes * 100);
    let mut spare = observation("", 1);
    let before = snapshot_retention_bytes(&spare.frame).unwrap();
    Arc::get_mut(&mut spare.frame).unwrap().cells[0][0]
        .text
        .reserve(64 * 1024);
    assert!(snapshot_retention_bytes(&spare.frame).unwrap() >= before + 128 * 1024);

    let (mut manager, hooks) = manager();
    let mut req = request("dense");
    req.authority.cols = 300;
    req.authority.rows = 120;
    req.appearance = query("e");
    req.budget.max_frame_bytes = 1024 * 1024;
    req.budget.max_bytes = 2 * 1024 * 1024;
    let mut baseline = dense.clone();
    let baseline_frame = Arc::make_mut(&mut baseline.frame);
    baseline_frame.frame_id = "f1".into();
    for row in &mut baseline_frame.cells {
        for cell in row {
            cell.text = " ".into();
        }
    }
    baseline.identity.frame_id = "f1".into();
    baseline.identity.observation_revision = 1;
    baseline.producer.event_sequence = 1;
    manager.start(req, baseline, 0).unwrap();
    let status = manager.advance("dense", 1, Some(dense)).unwrap();
    assert!(status.appearance.is_some());
    assert_eq!(status.retained_bytes, 0);
    assert_eq!(status.coverage.oversized_frames, 1);
    assert_eq!(hooks.lock().unwrap().frames, 0);
}

#[test]
fn cancellation_restart_and_resize_away_back_are_terminal() {
    let (mut manager, _) = manager();
    manager
        .start(request("cancel"), observation("", 1), 0)
        .unwrap();
    assert_eq!(
        manager.cancel("cancel").unwrap().state,
        CaptureState::Cancelled
    );
    assert_eq!(
        manager
            .advance("cancel", 1, Some(observation("TOAST", 2)))
            .unwrap()
            .state,
        CaptureState::Cancelled
    );
    manager
        .start(request("restart"), observation("", 1), 0)
        .unwrap();
    let mut restarted = observation("TOAST", 2);
    restarted.identity.run_id = "other-run".into();
    Arc::get_mut(&mut restarted.frame).unwrap().run_id = "other-run".into();
    assert_eq!(
        manager
            .advance("restart", 1, Some(restarted))
            .unwrap()
            .state,
        CaptureState::StaleRun
    );
    manager
        .start(request("resize"), observation("", 1), 0)
        .unwrap();
    let mut resized = observation("TOAST", 2);
    resized.identity.viewport_revision = 9;
    assert_eq!(
        manager.advance("resize", 1, Some(resized)).unwrap().state,
        CaptureState::IncompatibleViewport
    );
}

#[test]
fn count_and_aggregate_reservations_happen_at_creation_and_are_leased() {
    let hooks = Arc::new(Mutex::new(Hooks::default()));
    let mut manager = CaptureManager::new(
        Backend(hooks.clone()),
        CaptureLimits {
            max_jobs: 2,
            max_reserved_bytes: 64 * 1024 * 1024,
            max_reserved_frames: 16,
        },
    )
    .unwrap();
    manager
        .start(request("one"), observation("", 1), 0)
        .unwrap();
    manager
        .start(request("two"), observation("", 1), 0)
        .unwrap();
    assert_eq!(hooks.lock().unwrap().jobs, 2);
    assert!(
        manager
            .start(request("three"), observation("", 1), 0)
            .is_err()
    );
    assert_eq!(hooks.lock().unwrap().jobs, 2);
    manager
        .start(request("one"), observation("", 1), 999)
        .unwrap();
    assert_eq!(hooks.lock().unwrap().jobs, 2);
    assert_eq!(manager.status("one").unwrap().deadline_ms, 1000);
    assert!(manager.release("one").is_err());
    manager.cancel("one").unwrap();
    manager.release("one").unwrap();
    assert_eq!(hooks.lock().unwrap().jobs, 1);
    manager
        .start(request("three"), observation("", 1), 0)
        .unwrap();
    drop(manager);
    assert_eq!(hooks.lock().unwrap().jobs, 0);

    let mut tiny = CaptureManager::new(
        Backend(hooks.clone()),
        CaptureLimits {
            max_jobs: 32,
            max_reserved_bytes: 1,
            max_reserved_frames: 512,
        },
    )
    .unwrap();
    assert!(
        tiny.start(request("no-bytes"), observation("", 1), 0)
            .is_err()
    );
    assert_eq!(hooks.lock().unwrap().jobs, 0);
}

#[test]
fn drop_and_reload_interrupt_active_jobs_and_mark_frames_unavailable() {
    let (mut manager, hooks) = manager();
    let mut req = request("reload");
    req.completion = Some(CaptureCompletion::Disappeared);
    manager.start(req, observation("", 1), 0).unwrap();
    manager
        .advance("reload", 1, Some(observation("TOAST", 2)))
        .unwrap();
    let checkpoints = manager.checkpoints();
    drop(manager);
    assert_eq!(hooks.lock().unwrap().frames, 0);
    assert_eq!(
        hooks
            .lock()
            .unwrap()
            .checkpoints
            .last()
            .unwrap()
            .status
            .state,
        CaptureState::Interrupted
    );
    let mut restored = CaptureManager::restore(
        Backend(hooks.clone()),
        CaptureLimits::default(),
        checkpoints,
    )
    .unwrap();
    let status = restored.status("reload").unwrap();
    assert_eq!(status.state, CaptureState::Interrupted);
    assert!(status.appearance.is_some());
    assert_eq!(status.coverage.restored_frames_unavailable, 1);
    assert!(!status.frames[0].available);
    assert!(restored.materialize("reload", 0).is_err());
    assert_eq!(
        restored
            .advance("reload", 2, Some(observation("", 3)))
            .unwrap()
            .state,
        CaptureState::Interrupted
    );
}

#[test]
fn failed_hooks_release_admission_and_report_dropped_snapshot() {
    let (mut manager, hooks) = manager();
    hooks.lock().unwrap().reject_checkpoint = true;
    assert!(
        manager
            .start(request("not-armed"), observation("", 1), 0)
            .is_err()
    );
    assert_eq!(hooks.lock().unwrap().jobs, 0);
    hooks.lock().unwrap().reject_checkpoint = false;
    manager
        .start(request("lease"), observation("", 1), 0)
        .unwrap();
    hooks.lock().unwrap().reject_frame = true;
    let status = manager
        .advance("lease", 1, Some(observation("TOAST", 2)))
        .unwrap();
    assert!(status.appearance.is_some());
    assert_eq!(status.coverage.lease_failures, 1);
    assert_eq!(status.coverage.dropped_frames, 1);
    assert!(status.frames.is_empty());
}

#[test]
fn termination_rejects_invalid_state_and_checkpoints_terminal_state_once() {
    let (mut manager, hooks) = manager();
    manager
        .start(request("terminate"), observation("", 1), 100)
        .unwrap();
    let initial_checkpoints = hooks.lock().unwrap().checkpoints.len();

    assert!(
        manager
            .terminate("terminate", CaptureState::Cancelled, 150, "invalid")
            .is_err()
    );
    assert_eq!(
        manager.status("terminate").unwrap().state,
        CaptureState::Armed
    );
    assert_eq!(hooks.lock().unwrap().checkpoints.len(), initial_checkpoints);

    let terminated = manager
        .terminate(
            "terminate",
            CaptureState::ProcessExited,
            125,
            "application exited",
        )
        .unwrap();
    assert_eq!(terminated.state, CaptureState::ProcessExited);
    assert_eq!(terminated.updated_at_ms, 125);
    assert_eq!(terminated.failure.as_deref(), Some("application exited"));
    let checkpoint = hooks.lock().unwrap().checkpoints.last().unwrap().clone();
    assert_eq!(checkpoint.status.state, CaptureState::ProcessExited);
    assert_eq!(checkpoint.status.updated_at_ms, 125);
    assert_eq!(
        checkpoint.status.failure.as_deref(),
        Some("application exited")
    );

    let checkpoint_count = hooks.lock().unwrap().checkpoints.len();
    let repeated = manager
        .terminate(
            "terminate",
            CaptureState::SessionClosed,
            200,
            "closed later",
        )
        .unwrap();
    assert_eq!(repeated, terminated);
    assert_eq!(hooks.lock().unwrap().checkpoints.len(), checkpoint_count);
}

#[test]
fn cancellation_and_deadline_checkpoint_failures_remain_explicit_and_terminal() {
    let (mut manager, hooks) = manager();
    manager
        .start(request("cancel-save"), observation("", 1), 0)
        .unwrap();
    manager
        .start(request("deadline-save"), observation("", 1), 0)
        .unwrap();
    hooks.lock().unwrap().reject_checkpoint = true;
    assert!(manager.cancel("cancel-save").is_err());
    let cancelled = manager.status("cancel-save").unwrap();
    assert_eq!(cancelled.state, CaptureState::Cancelled);
    assert!(cancelled.coverage.checkpoint_failed);
    assert!(manager.advance("deadline-save", 1000, None).is_err());
    let timeout = manager.status("deadline-save").unwrap();
    assert_eq!(timeout.state, CaptureState::TimedOut);
    assert!(timeout.coverage.checkpoint_failed);
    assert!(timeout.failure.is_some());
    hooks.lock().unwrap().reject_checkpoint = false;
    assert_eq!(
        manager
            .advance("deadline-save", 1001, Some(observation("TOAST", 2)))
            .unwrap()
            .state,
        CaptureState::TimedOut
    );
}

#[test]
fn all_32_job_leases_are_reserved_before_status_polling_and_no_auto_eviction() {
    let hooks = Arc::new(Mutex::new(Hooks::default()));
    let mut manager = CaptureManager::new(
        Backend(hooks.clone()),
        CaptureLimits {
            max_reserved_bytes: 256 * 1024 * 1024,
            ..CaptureLimits::default()
        },
    )
    .unwrap();
    for index in 0..MAX_CAPTURE_JOBS {
        manager
            .start(request(&format!("job-{index}")), observation("", 1), 0)
            .unwrap();
    }
    assert_eq!(hooks.lock().unwrap().jobs, 32);
    assert!(
        manager
            .start(request("overflow"), observation("", 1), 0)
            .is_err()
    );
    manager.cancel("job-0").unwrap();
    assert!(
        manager
            .start(request("overflow"), observation("", 1), 0)
            .is_err()
    );
    assert_eq!(
        manager.status("job-0").unwrap().state,
        CaptureState::Cancelled
    );
    manager.release("job-0").unwrap();
    manager
        .start(request("overflow"), observation("", 1), 0)
        .unwrap();
    drop(manager);
    assert_eq!(hooks.lock().unwrap().jobs, 0);
}

#[test]
fn malformed_and_identity_mismatched_requests_do_not_admit_jobs() {
    let (mut manager, hooks) = manager();
    let mut req = request("bad-region");
    req.appearance.region = Some(locators::CellRect {
        row_start: 0,
        col_start: 0,
        row_end: 2,
        col_end: 20,
    });
    assert!(manager.start(req, observation("", 1), 0).is_err());
    let mut baseline = observation("", 1);
    baseline.identity.frame_id = "wrong".into();
    assert!(manager.start(request("bad-frame"), baseline, 0).is_err());
    assert_eq!(hooks.lock().unwrap().jobs, 0);
    manager
        .start(request("conflict"), observation("", 1), 0)
        .unwrap();
    let mut changed = request("conflict");
    changed.appearance = query("other");
    assert!(
        manager
            .start(changed, observation("", 1), 0)
            .unwrap_err()
            .to_string()
            .contains("capture_conflict")
    );
    assert_eq!(hooks.lock().unwrap().jobs, 1);
}
