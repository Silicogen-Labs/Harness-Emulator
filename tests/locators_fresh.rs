use silicogen_terminal_runtime::protocol::{
    CellSnapshot, CellStyle, CursorSnapshot, FrameSnapshot,
};

mod protocol {
    pub use silicogen_terminal_runtime::protocol::*;
}

#[path = "../src/locators.rs"]
mod locators;

use locators::{CellRect, LocatorQuery, locate_text};

fn cell(text: &str, width: usize) -> CellSnapshot {
    CellSnapshot {
        text: text.into(),
        width,
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

fn frame(grid: Vec<Vec<CellSnapshot>>) -> FrameSnapshot {
    let rows = grid.len() as u16;
    let cols = grid.first().map_or(0, |row| row.len()) as u16;
    FrameSnapshot {
        session_id: "s".into(),
        run_id: "r".into(),
        frame_id: "f1".into(),
        captured_at_ms: 1,
        output_sequence: 1,
        cols,
        rows,
        text: String::new(),
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
        cells: grid,
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

#[test]
fn locator_finds_repeated_unicode_and_wide_glyph_cells() {
    let observed = frame(vec![vec![
        cell("界", 2),
        cell("", 0),
        cell(" ", 1),
        cell("界", 2),
        cell("", 0),
        cell(" ", 1),
    ]]);
    let result = locate_text(&observed, &LocatorQuery::Text("界".into()), None, 7, 2).unwrap();
    assert_eq!(result.matches.len(), 2);
    assert!(result.any_match);
    assert_eq!(result.matches[0].snippet, "界");
    assert_eq!(
        result.matches[0].rect,
        CellRect {
            row_start: 0,
            col_start: 0,
            row_end: 1,
            col_end: 2
        }
    );
    assert_eq!(
        result.matches[1].rect,
        CellRect {
            row_start: 0,
            col_start: 3,
            row_end: 1,
            col_end: 5
        }
    );
    assert_eq!(result.matches[1].observation.observation_revision, 7);
    assert_eq!(result.matches[1].observation.viewport_revision, 2);
    assert!(!result.truncated);
}

#[test]
fn combining_codepoint_keeps_the_base_glyph_cell_rectangle() {
    let observed = frame(vec![vec![cell("e", 1), cell("\u{301}", 0), cell(" ", 1)]]);
    let result = locate_text(
        &observed,
        &LocatorQuery::Text("e\u{301}".into()),
        None,
        2,
        1,
    )
    .unwrap();
    assert_eq!(result.matches.len(), 1);
    assert_eq!(result.matches[0].snippet, "e\u{301}");
    assert_eq!(
        result.matches[0].rect,
        CellRect {
            row_start: 0,
            col_start: 0,
            row_end: 1,
            col_end: 1
        }
    );
}

#[test]
fn regex_uses_unicode_byte_offsets_and_region_bounds() {
    let observed = frame(vec![vec![
        cell("a", 1),
        cell("é", 1),
        cell("b", 1),
        cell(" ", 1),
        cell("a", 1),
        cell("é", 1),
        cell("b", 1),
        cell(" ", 1),
    ]]);
    let result = locate_text(
        &observed,
        &LocatorQuery::Regex("éb".into()),
        Some(CellRect {
            row_start: 0,
            col_start: 1,
            row_end: 1,
            col_end: 4,
        }),
        9,
        4,
    )
    .unwrap();
    assert_eq!(result.matches.len(), 1);
    assert_eq!(result.matches[0].snippet, "éb");
    assert_eq!(
        result.matches[0].rect,
        CellRect {
            row_start: 0,
            col_start: 1,
            row_end: 1,
            col_end: 3
        }
    );
}

#[test]
fn explicit_rendered_row_break_can_be_matched_without_claiming_soft_wrap_metadata() {
    let observed = frame(vec![
        vec![cell("a", 1), cell("b", 1), cell("c", 1)],
        vec![cell("d", 1), cell("e", 1), cell("f", 1)],
    ]);
    let result = locate_text(&observed, &LocatorQuery::Text("bc\nde".into()), None, 11, 0).unwrap();
    assert_eq!(result.matches.len(), 1);
    assert_eq!(result.matches[0].snippet, "bc\nde");
    assert_eq!(result.matches[0].cell_rects.len(), 2);
    assert_eq!(
        result.matches[0].cell_rects[0],
        CellRect {
            row_start: 0,
            col_start: 1,
            row_end: 1,
            col_end: 3
        }
    );
    assert_eq!(
        result.matches[0].cell_rects[1],
        CellRect {
            row_start: 1,
            col_start: 0,
            row_end: 2,
            col_end: 2
        }
    );
}

#[test]
fn default_region_supports_representative_160_by_45_viewport_and_reports_truncation() {
    let observed = frame(vec![vec![cell("x", 1); 160]; 45]);
    let result = locate_text(&observed, &LocatorQuery::Text("x".into()), None, 1, 3).unwrap();
    assert_eq!(result.matches.len(), locators::MAX_MATCHES);
    assert!(result.any_match);
    assert!(result.truncated);
    assert_eq!(result.omitted_matches, 160 * 45 - locators::MAX_MATCHES);
}

#[test]
fn dense_300_by_120_query_has_accurate_bounded_results_and_runtime() {
    let observed = frame(vec![vec![cell("x", 1); 300]; 120]);
    let started = std::time::Instant::now();
    let result = locate_text(&observed, &LocatorQuery::Regex(".".into()), None, 8, 5).unwrap();
    let elapsed = started.elapsed();

    assert_eq!(result.matches.len(), locators::MAX_MATCHES);
    assert_eq!(result.omitted_matches, 300 * 120 - locators::MAX_MATCHES);
    assert!(result.truncated);
    assert_eq!(result.matches[0].rect.col_start, 0);
    assert_eq!(result.matches[0].rect.row_start, 0);
    assert_eq!(result.matches[locators::MAX_MATCHES - 1].rect.row_start, 0);
    assert_eq!(
        result.matches[locators::MAX_MATCHES - 1].rect.col_start,
        255
    );
    assert!(
        elapsed < std::time::Duration::from_secs(5),
        "dense query took {elapsed:?}"
    );
}

#[test]
fn literal_query_limit_applies_to_raw_utf8_bytes_before_escaping() {
    let observed = frame(vec![vec![cell("x", 1)]]);
    let accepted_raw = "\\".repeat(locators::MAX_QUERY_BYTES);
    assert!(locate_text(&observed, &LocatorQuery::Text(accepted_raw), None, 1, 0).is_ok());

    let rejected_raw = "x".repeat(locators::MAX_QUERY_BYTES + 1);
    let error = locate_text(&observed, &LocatorQuery::Text(rejected_raw), None, 1, 0).unwrap_err();
    assert!(error.to_string().contains("exceeds 4096 bytes"));
}

#[test]
fn oversized_match_snippets_are_omitted_and_truncation_is_reported() {
    let huge = "x".repeat(locators::MAX_SNIPPET_BYTES + 1);
    let observed = frame(vec![vec![cell(&huge, 1)]]);
    let result = locate_text(&observed, &LocatorQuery::Regex("x+".into()), None, 1, 0).unwrap();
    assert!(result.matches.is_empty());
    assert!(result.any_match);
    assert!(result.truncated);
    assert_eq!(result.omitted_matches, 1);
}
