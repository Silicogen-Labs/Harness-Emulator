use silicogen_terminal_runtime::protocol::{
    CellSnapshot, CellStyle, CursorSnapshot, FrameSnapshot,
};
use std::time::{Duration, Instant};

mod protocol {
    pub use silicogen_terminal_runtime::protocol::*;
}
mod predicates {
    pub use silicogen_terminal_runtime::predicates::*;
}
#[path = "../src/locators.rs"]
mod locators;
#[path = "../src/wait_engine.rs"]
mod wait_engine;

use locators::{CellRect, LocatorQuery};
use predicates::{PredicateEvaluator, PredicateOutcome, PredicateSpec};
use wait_engine::{FreshTransition, FreshWaitSpec, WaitBaseline, WaitEvaluator, WaitOutcome};

fn cell(ch: char) -> CellSnapshot {
    CellSnapshot {
        text: ch.to_string(),
        width: 1,
        foreground: None,
        background: None,
        resolved_foreground: "white".into(),
        resolved_background: "black".into(),
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

fn frame(line0: &str, line1: &str, frame_id: &str, sequence: u64) -> FrameSnapshot {
    let make_row = |line: &str| {
        let mut chars = line.chars().take(12).collect::<Vec<_>>();
        chars.resize(12, ' ');
        chars.into_iter().map(cell).collect::<Vec<_>>()
    };
    FrameSnapshot {
        session_id: "session-a".into(),
        run_id: "run-a".into(),
        frame_id: frame_id.into(),
        captured_at_ms: sequence as u128,
        output_sequence: sequence,
        cols: 12,
        rows: 2,
        text: format!("{line0}\n{line1}"),
        base_style: CellStyle {
            foreground: "white".into(),
            background: "black".into(),
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
        },
        styled_runs: Vec::new(),
        cells: vec![make_row(line0), make_row(line1)],
        cursor: CursorSnapshot {
            row: 0,
            col: 0,
            visible: false,
            shape: "block".into(),
            color: "white".into(),
        },
        modes: Default::default(),
        keyboard_mode: Vec::new(),
        mouse_tracking: "none".into(),
        mouse_encoding: "none".into(),
        default_foreground: "white".into(),
        default_background: "black".into(),
        cursor_color: "white".into(),
        palette: Default::default(),
        title: None,
        process_state: "running".into(),
        capture_complete: true,
        capture_warning: None,
    }
}

fn spec(query: &str, transition: FreshTransition) -> FreshWaitSpec {
    FreshWaitSpec {
        query: LocatorQuery::Text(query.into()),
        region: None,
        transition,
    }
}

fn regex_spec(pattern: &str, transition: FreshTransition) -> FreshWaitSpec {
    FreshWaitSpec {
        query: LocatorQuery::Regex(pattern.into()),
        region: None,
        transition,
    }
}

#[test]
fn recap_plus_spinner_does_not_satisfy_became_present_until_absence_then_reappearance() {
    let t0 = Instant::now();
    let baseline_frame = frame("OLD RECAP", "spinner .", "f0", 1);
    let baseline = WaitBaseline::capture(&baseline_frame, 10, 1).unwrap();
    let mut wait = WaitEvaluator::new(
        spec("OLD RECAP", FreshTransition::BecamePresent),
        baseline,
        t0 + Duration::from_secs(30),
    )
    .unwrap();
    assert_eq!(wait.deadline(), t0 + Duration::from_secs(30));

    let spinner_only = frame("OLD RECAP", "spinner *", "f1", 2);
    assert_eq!(wait.observe(&spinner_only, 11, 1, t0), WaitOutcome::Pending);
    let absent = frame("", "spinner *", "f2", 3);
    assert_eq!(wait.observe(&absent, 12, 1, t0), WaitOutcome::Pending);
    let reappeared = frame("OLD RECAP", "spinner *", "f3", 4);
    assert!(matches!(
        wait.observe(&reappeared, 13, 1, t0),
        WaitOutcome::Matched(matches) if matches.matches.len() == 1
    ));
}

#[test]
fn absent_to_present_matches_but_output_sequence_without_match_does_not() {
    let t0 = Instant::now();
    let baseline_frame = frame("", "spinner .", "f0", 1);
    let baseline = WaitBaseline::capture(&baseline_frame, 4, 1).unwrap();
    let mut wait = WaitEvaluator::new(
        spec("WRITE", FreshTransition::BecamePresent),
        baseline,
        t0 + Duration::from_secs(5),
    )
    .unwrap();
    assert_eq!(
        wait.observe(&frame("", "spinner *", "f1", 2), 5, 1, t0),
        WaitOutcome::Pending
    );
    assert!(matches!(
        wait.observe(&frame("WRITE COMPLETE", "spinner *", "f2", 3), 6, 1, t0),
        WaitOutcome::Matched(_)
    ));
}

#[test]
fn current_presence_remains_level_triggered_at_the_baseline_revision() {
    let t0 = Instant::now();
    let baseline_frame = frame("READY", "", "f0", 1);
    let baseline = WaitBaseline::capture(&baseline_frame, 7, 3).unwrap();
    let mut wait = WaitEvaluator::new(
        spec("READY", FreshTransition::CurrentPresence),
        baseline,
        t0 + Duration::from_secs(1),
    )
    .unwrap();
    assert!(matches!(
        wait.observe(&baseline_frame, 7, 3, t0),
        WaitOutcome::Matched(_)
    ));
}

#[test]
fn changed_match_requires_a_changed_cell_inside_the_match() {
    let t0 = Instant::now();
    let baseline_frame = frame("READY", "spinner .", "f0", 1);
    let baseline = WaitBaseline::capture(&baseline_frame, 20, 1).unwrap();
    let mut wait = WaitEvaluator::new(
        spec("READY", FreshTransition::MatchIntersectsChangedCells),
        baseline,
        t0 + Duration::from_secs(5),
    )
    .unwrap();
    let spinner_changed = frame("READY", "spinner *", "f1", 2);
    assert_eq!(
        wait.observe(&spinner_changed, 21, 1, t0),
        WaitOutcome::Pending
    );

    let mut style_changed = frame("READY", "spinner *", "f2", 3);
    style_changed.cells[0][0].bold = true;
    assert!(matches!(
        wait.observe(&style_changed, 22, 1, t0),
        WaitOutcome::Matched(_)
    ));
}

#[test]
fn changed_match_is_checked_beyond_the_bounded_returned_match_page() {
    let t0 = Instant::now();
    let mut baseline_frame = frame("", "", "f0", 1);
    baseline_frame.cols = 160;
    baseline_frame.cells = vec![vec![cell('R'); 160]; 2];
    let baseline = WaitBaseline::capture(&baseline_frame, 1, 1).unwrap();
    let mut wait = WaitEvaluator::new(
        spec("R", FreshTransition::MatchIntersectsChangedCells),
        baseline,
        t0 + Duration::from_secs(5),
    )
    .unwrap();
    let mut current = baseline_frame.clone();
    current.frame_id = "f1".into();
    current.output_sequence = 2;
    current.cells[1][159].bold = true;

    let WaitOutcome::Matched(results) = wait.observe(&current, 2, 1, t0) else {
        panic!("changed matching text after the result page must still satisfy the wait")
    };
    assert!(results.truncated);
    assert_eq!(results.matches.len(), locators::MAX_MATCHES);
    assert_eq!(results.omitted_matches, 64);
}

#[test]
fn oversized_match_remains_present_for_all_fresh_transition_modes() {
    let t0 = Instant::now();
    let long_text = "x".repeat(locators::MAX_SNIPPET_BYTES + 1);
    let mut present_frame = frame("", "", "f0", 1);
    present_frame.cells[0][0].text = long_text;

    let baseline = WaitBaseline::capture(&present_frame, 10, 3).unwrap();
    let mut current_wait = WaitEvaluator::new(
        regex_spec("x+", FreshTransition::CurrentPresence),
        baseline,
        t0 + Duration::from_secs(5),
    )
    .unwrap();
    let WaitOutcome::Matched(current_result) = current_wait.observe(&present_frame, 10, 3, t0)
    else {
        panic!("omitted snippets must still count as a current match")
    };
    assert!(current_result.any_match);
    assert!(current_result.matches.is_empty());
    assert_eq!(current_result.omitted_matches, 1);

    let absent = frame("", "", "f1", 2);
    let baseline = WaitBaseline::capture(&absent, 20, 4).unwrap();
    let mut became_wait = WaitEvaluator::new(
        regex_spec("x+", FreshTransition::BecamePresent),
        baseline,
        t0 + Duration::from_secs(5),
    )
    .unwrap();
    present_frame.frame_id = "f2".into();
    let WaitOutcome::Matched(became_result) = became_wait.observe(&present_frame, 21, 4, t0) else {
        panic!("an oversized absent-to-present match must satisfy the wait")
    };
    assert!(became_result.any_match);
    assert!(became_result.matches.is_empty());
    assert!(became_result.truncated);
    assert_eq!(became_result.omitted_matches, 1);

    let baseline = WaitBaseline::capture(&present_frame, 30, 5).unwrap();
    let mut changed_wait = WaitEvaluator::new(
        regex_spec("x+", FreshTransition::MatchIntersectsChangedCells),
        baseline,
        t0 + Duration::from_secs(5),
    )
    .unwrap();
    let mut changed = present_frame.clone();
    changed.frame_id = "f3".into();
    changed.cells[0][0].bold = true;
    let WaitOutcome::Matched(changed_result) = changed_wait.observe(&changed, 31, 5, t0) else {
        panic!("an oversized match intersecting a changed cell must satisfy the wait")
    };
    assert!(changed_result.any_match);
    assert!(changed_result.matches.is_empty());
    assert!(changed_result.truncated);
    assert_eq!(changed_result.omitted_matches, 1);
}

#[test]
fn run_restart_resize_deadline_and_cancellation_are_explicit() {
    let t0 = Instant::now();
    let baseline_frame = frame("OLD", "", "f0", 1);
    let baseline = WaitBaseline::capture(&baseline_frame, 5, 1).unwrap();
    let mut wait = WaitEvaluator::new(
        spec("NEW", FreshTransition::BecamePresent),
        baseline,
        t0 + Duration::from_millis(10),
    )
    .unwrap();
    let mut restarted = frame("NEW", "", "f1", 2);
    restarted.run_id = "run-b".into();
    assert_eq!(
        wait.observe(&restarted, 6, 1, t0),
        WaitOutcome::StaleBaseline
    );

    let baseline = WaitBaseline::capture(&baseline_frame, 5, 1).unwrap();
    let mut resized_wait = WaitEvaluator::new(
        spec("NEW", FreshTransition::BecamePresent),
        baseline,
        t0 + Duration::from_millis(10),
    )
    .unwrap();
    let mut resized = frame("NEW", "", "f2", 3);
    resized.cols = 11;
    resized.cells.iter_mut().for_each(|row| {
        row.pop();
    });
    assert_eq!(
        resized_wait.observe(&resized, 6, 1, t0),
        WaitOutcome::IncompatibleViewport
    );

    let baseline = WaitBaseline::capture(&baseline_frame, 5, 1).unwrap();
    let mut resize_back = WaitEvaluator::new(
        spec("NEW", FreshTransition::BecamePresent),
        baseline,
        t0 + Duration::from_millis(10),
    )
    .unwrap();
    assert_eq!(
        resize_back.observe(&frame("NEW", "", "f3", 4), 6, 2, t0),
        WaitOutcome::IncompatibleViewport
    );

    let baseline = WaitBaseline::capture(&baseline_frame, 5, 1).unwrap();
    let mut timed = WaitEvaluator::new(
        spec("NEW", FreshTransition::BecamePresent),
        baseline,
        t0 + Duration::from_millis(10),
    )
    .unwrap();
    assert_eq!(
        timed.tick(t0 + Duration::from_millis(10)),
        WaitOutcome::DeadlineExceeded
    );
    assert_eq!(
        timed.cancel(),
        WaitOutcome::DeadlineExceeded,
        "terminal outcomes remain stable after completion"
    );

    let baseline = WaitBaseline::capture(&baseline_frame, 5, 1).unwrap();
    let mut cancelled = WaitEvaluator::new(
        spec("NEW", FreshTransition::BecamePresent),
        baseline,
        t0 + Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(cancelled.cancel(), WaitOutcome::Cancelled);
}

#[test]
fn stability_timer_expires_on_a_tick_without_new_output() {
    let t0 = Instant::now();
    let snapshot = frame("READY", "", "f0", 1);
    let mut evaluator = PredicateEvaluator::new(PredicateSpec::StableRegion {
        row: 0,
        col: 0,
        rows: 1,
        cols: 5,
        duration_ms: 100,
    })
    .unwrap();
    assert!(!evaluator.observe(&snapshot, t0).matched);
    assert_eq!(
        evaluator.next_deadline(),
        Some(t0 + Duration::from_millis(100))
    );
    let expired = evaluator.observe(&snapshot, t0 + Duration::from_millis(100));
    assert!(expired.matched);
    assert_eq!(expired.outcome, PredicateOutcome::Matched);
}

#[test]
fn region_scoped_fresh_wait_ignores_matching_text_outside_its_region() {
    let t0 = Instant::now();
    let baseline_frame = frame("", "", "f0", 1);
    let baseline = WaitBaseline::capture(&baseline_frame, 1, 1).unwrap();
    let mut scoped = spec("READY", FreshTransition::BecamePresent);
    scoped.region = Some(CellRect {
        row_start: 1,
        col_start: 0,
        row_end: 2,
        col_end: 8,
    });
    let mut wait = WaitEvaluator::new(scoped, baseline, t0 + Duration::from_secs(2)).unwrap();
    assert_eq!(
        wait.observe(&frame("READY", "", "f1", 2), 2, 1, t0),
        WaitOutcome::Pending
    );
}
