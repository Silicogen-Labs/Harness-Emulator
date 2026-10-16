//! Deterministic text, regex, absence, cell, and style assertions for frames.

use crate::protocol::{CellStyle, FrameSnapshot};
use anyhow::{Result, bail};
use regex::Regex;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum FrameAssertion {
    TextContains {
        text: String,
    },
    TextAbsent {
        text: String,
    },
    Regex {
        pattern: String,
    },
    CellText {
        row: u16,
        col: u16,
        text: String,
    },
    CellStyle {
        row: u16,
        col: u16,
        style: CellStyle,
    },
    All {
        assertions: Vec<FrameAssertion>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssertionFailure {
    pub path: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AssertionReport {
    pub session_id: String,
    pub run_id: String,
    pub frame_id: String,
    pub passed: bool,
    pub checked: usize,
    pub failures: Vec<AssertionFailure>,
}

pub fn evaluate(frame: &FrameSnapshot, assertion: &FrameAssertion) -> Result<AssertionReport> {
    if frame.cols == 0 || frame.rows == 0 || frame.cols > 300 || frame.rows > 120 {
        bail!("frame dimensions are outside supported bounds")
    }
    let mut failures = Vec::new();
    let mut checked = 0;
    evaluate_one(frame, assertion, "$", &mut checked, &mut failures)?;
    Ok(AssertionReport {
        session_id: frame.session_id.clone(),
        run_id: frame.run_id.clone(),
        frame_id: frame.frame_id.clone(),
        passed: failures.is_empty(),
        checked,
        failures,
    })
}

fn evaluate_one(
    frame: &FrameSnapshot,
    assertion: &FrameAssertion,
    path: &str,
    checked: &mut usize,
    failures: &mut Vec<AssertionFailure>,
) -> Result<()> {
    match assertion {
        FrameAssertion::All { assertions } => {
            if assertions.is_empty() {
                bail!("compound assertion must contain at least one requirement")
            }
            for (index, child) in assertions.iter().enumerate() {
                evaluate_one(
                    frame,
                    child,
                    &format!("{path}.all[{index}]"),
                    checked,
                    failures,
                )?;
            }
        }
        FrameAssertion::TextContains { text } => {
            *checked += 1;
            if !frame.text.contains(text) {
                fail(failures, path, format!("text did not contain {text:?}"));
            }
        }
        FrameAssertion::TextAbsent { text } => {
            *checked += 1;
            if frame.text.contains(text) {
                fail(
                    failures,
                    path,
                    format!("text unexpectedly contained {text:?}"),
                );
            }
        }
        FrameAssertion::Regex { pattern } => {
            *checked += 1;
            let regex =
                Regex::new(pattern).map_err(|e| anyhow::anyhow!("invalid assertion regex: {e}"))?;
            if !regex.is_match(&frame.text) {
                fail(
                    failures,
                    path,
                    format!("text did not match regex {pattern:?}"),
                );
            }
        }
        FrameAssertion::CellText { row, col, text } => {
            *checked += 1;
            let Some(cell) = cell(frame, *row, *col) else {
                fail(
                    failures,
                    path,
                    format!("cell ({row},{col}) is outside captured cell data"),
                );
                return Ok(());
            };
            if &cell.text != text {
                fail(
                    failures,
                    path,
                    format!(
                        "cell ({row},{col}) text was {:?}, expected {:?}",
                        cell.text, text
                    ),
                );
            }
        }
        FrameAssertion::CellStyle { row, col, style } => {
            *checked += 1;
            let Some(cell) = cell(frame, *row, *col) else {
                fail(
                    failures,
                    path,
                    format!("cell ({row},{col}) is outside captured cell data"),
                );
                return Ok(());
            };
            let actual = CellStyle {
                foreground: cell.resolved_foreground.clone(),
                background: cell.resolved_background.clone(),
                bold: cell.bold,
                dim: cell.dim,
                italic: cell.italic,
                inverse: cell.inverse,
                invisible: cell.invisible,
                strike: cell.strike,
                blink: cell.blink,
                underline: cell.underline.clone(),
                underline_color: cell.underline_color.clone(),
                hyperlink: cell.hyperlink.clone(),
            };
            if &actual != style {
                fail(
                    failures,
                    path,
                    format!("cell ({row},{col}) style did not match expected style"),
                );
            }
        }
    }
    Ok(())
}

fn cell(frame: &FrameSnapshot, row: u16, col: u16) -> Option<&crate::protocol::CellSnapshot> {
    if row >= frame.rows || col >= frame.cols {
        return None;
    }
    frame.cells.get(row as usize)?.get(col as usize)
}

fn fail(failures: &mut Vec<AssertionFailure>, path: &str, message: String) {
    failures.push(AssertionFailure {
        path: path.to_owned(),
        message,
    });
}
