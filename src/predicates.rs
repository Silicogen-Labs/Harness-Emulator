//! Stateful, frame-based readiness predicates.
//!
//! The evaluator deliberately does not infer generic spinner state. Applications
//! can opt into busy and crash markers as part of a `Ready` predicate.

use crate::protocol::{CellSnapshot, FrameSnapshot};
use anyhow::{Result, bail};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum PredicateSpec {
    TextContains {
        text: String,
    },
    TextAbsent {
        text: String,
    },
    Regex {
        pattern: String,
    },
    /// The PTY output sequence must not advance for this duration.
    OutputQuiet {
        duration_ms: u64,
    },
    /// Visible text must remain equal for this duration. PTY output elsewhere
    /// does not reset this timer.
    StableText {
        duration_ms: u64,
    },
    /// Cell content and style in the specified zero-based region must remain
    /// equal for this duration.
    StableRegion {
        row: u16,
        col: u16,
        rows: u16,
        cols: u16,
        duration_ms: u64,
    },
    /// Required markers must all be present; configured busy and crash markers
    /// must be absent. Optional region stability can further gate readiness.
    Ready {
        #[serde(default)]
        required: Vec<String>,
        #[serde(default)]
        busy: Vec<String>,
        #[serde(default)]
        crash: Vec<String>,
        #[serde(default)]
        stable_region: Option<RegionStability>,
    },
    ProcessExited,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegionStability {
    pub row: u16,
    pub col: u16,
    pub rows: u16,
    pub cols: u16,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PredicateOutcome {
    Pending,
    Matched,
    Quiet,
    Ready,
    Exited,
    AppCrash,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PredicateResult {
    pub matched: bool,
    pub outcome: PredicateOutcome,
    pub elapsed_ms: u64,
    pub session_id: String,
    pub run_id: String,
    pub frame_id: String,
    pub output_sequence: u64,
    pub matched_markers: Vec<String>,
    pub missing_markers: Vec<String>,
    pub busy_markers: Vec<String>,
    pub crash_markers: Vec<String>,
}

/// Stateful evaluator. Call `observe` for each fresh frame until it matches or
/// an external deadline expires. A new run should use a new evaluator.
pub struct PredicateEvaluator {
    spec: PredicateSpec,
    started_at: Instant,
    previous_sequence: Option<u64>,
    quiet_since: Option<Instant>,
    previous_text: Option<String>,
    text_stable_since: Option<Instant>,
    previous_region: Option<Vec<Vec<CellFingerprint>>>,
    region_stable_since: Option<Instant>,
    regex: Option<Regex>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CellFingerprint {
    text: String,
    width: usize,
    foreground: Option<crate::protocol::ColorValue>,
    background: Option<crate::protocol::ColorValue>,
    resolved_foreground: String,
    resolved_background: String,
    bold: bool,
    dim: bool,
    italic: bool,
    inverse: bool,
    invisible: bool,
    strike: bool,
    blink: bool,
    underline: String,
    underline_color: Option<crate::protocol::ColorValue>,
    hyperlink: Option<String>,
}

impl From<&CellSnapshot> for CellFingerprint {
    fn from(cell: &CellSnapshot) -> Self {
        Self {
            text: cell.text.clone(),
            width: cell.width,
            foreground: cell.foreground.clone(),
            background: cell.background.clone(),
            resolved_foreground: cell.resolved_foreground.clone(),
            resolved_background: cell.resolved_background.clone(),
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
        }
    }
}

impl PredicateEvaluator {
    pub fn new(spec: PredicateSpec) -> Result<Self> {
        let regex = match &spec {
            PredicateSpec::Regex { pattern } => Some(Regex::new(pattern)?),
            _ => None,
        };
        validate_spec(&spec)?;
        Ok(Self {
            spec,
            started_at: Instant::now(),
            previous_sequence: None,
            quiet_since: None,
            previous_text: None,
            text_stable_since: None,
            previous_region: None,
            region_stable_since: None,
            regex,
        })
    }

    pub fn observe(&mut self, frame: &FrameSnapshot, now: Instant) -> PredicateResult {
        let process_exited = frame.process_state != "running";
        if self.previous_sequence != Some(frame.output_sequence) {
            self.previous_sequence = Some(frame.output_sequence);
            self.quiet_since = Some(now);
        }
        if self.previous_text.as_deref() != Some(frame.text.as_str()) {
            self.previous_text = Some(frame.text.clone());
            self.text_stable_since = Some(now);
        }

        let region = match &self.spec {
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
        let invalid_region =
            region.is_some_and(|(row, col, rows, cols)| !region_valid(frame, row, col, rows, cols));
        if let Some((row, col, rows, cols)) = region.filter(|_| !invalid_region) {
            let fingerprint = region_fingerprint(frame, row, col, rows, cols);
            if self.previous_region.as_ref() != Some(&fingerprint) {
                self.previous_region = Some(fingerprint);
                self.region_stable_since = Some(now);
            }
        }

        let mut outcome = PredicateOutcome::Pending;
        let mut matched = false;
        let mut matched_markers = Vec::new();
        let mut missing_markers = Vec::new();
        let mut busy_markers = Vec::new();
        let mut crash_markers = Vec::new();

        match &self.spec {
            PredicateSpec::TextContains { text } => matched = frame.text.contains(text),
            PredicateSpec::TextAbsent { text } => matched = !frame.text.contains(text),
            PredicateSpec::Regex { .. } => {
                matched = self
                    .regex
                    .as_ref()
                    .is_some_and(|regex| regex.is_match(&frame.text));
            }
            PredicateSpec::OutputQuiet { duration_ms } => {
                matched = elapsed_at_least(self.quiet_since, *duration_ms, now);
                if matched {
                    outcome = PredicateOutcome::Quiet;
                }
            }
            PredicateSpec::StableText { duration_ms } => {
                matched = elapsed_at_least(self.text_stable_since, *duration_ms, now);
            }
            PredicateSpec::StableRegion {
                row,
                col,
                rows,
                cols,
                duration_ms,
            } => {
                matched = !invalid_region
                    && elapsed_at_least(self.region_stable_since, *duration_ms, now);
                if invalid_region {
                    missing_markers.push("region_out_of_bounds".into());
                }
                let _ = (row, col, rows, cols);
            }
            PredicateSpec::Ready {
                required,
                busy,
                crash,
                stable_region,
            } => {
                matched_markers = required
                    .iter()
                    .filter(|marker| frame.text.contains(marker.as_str()))
                    .cloned()
                    .collect();
                missing_markers = required
                    .iter()
                    .filter(|marker| !frame.text.contains(marker.as_str()))
                    .cloned()
                    .collect();
                busy_markers = busy
                    .iter()
                    .filter(|marker| frame.text.contains(marker.as_str()))
                    .cloned()
                    .collect();
                crash_markers = crash
                    .iter()
                    .filter(|marker| frame.text.contains(marker.as_str()))
                    .cloned()
                    .collect();
                if !crash_markers.is_empty() {
                    outcome = PredicateOutcome::AppCrash;
                    matched = true;
                } else if process_exited {
                    outcome = PredicateOutcome::Exited;
                    matched = true;
                } else if missing_markers.is_empty() && busy_markers.is_empty() {
                    let stable = stable_region.as_ref().is_none_or(|spec| {
                        !invalid_region
                            && elapsed_at_least(self.region_stable_since, spec.duration_ms, now)
                    });
                    if stable {
                        outcome = PredicateOutcome::Ready;
                        matched = true;
                    }
                }
            }
            PredicateSpec::ProcessExited => {
                matched = process_exited;
                if matched {
                    outcome = PredicateOutcome::Exited;
                }
            }
        }
        if outcome == PredicateOutcome::Pending && matched {
            outcome = PredicateOutcome::Matched;
        }
        PredicateResult {
            matched,
            outcome,
            elapsed_ms: now.saturating_duration_since(self.started_at).as_millis() as u64,
            session_id: frame.session_id.clone(),
            run_id: frame.run_id.clone(),
            frame_id: frame.frame_id.clone(),
            output_sequence: frame.output_sequence,
            matched_markers,
            missing_markers,
            busy_markers,
            crash_markers,
        }
    }
}

/// One-shot evaluator for predicates that do not require history. For temporal
/// predicates, use `PredicateEvaluator` and feed every observation.
pub fn evaluate(frame: &FrameSnapshot, predicate: &PredicateSpec) -> Result<PredicateResult> {
    let mut evaluator = PredicateEvaluator::new(predicate.clone())?;
    Ok(evaluator.observe(frame, Instant::now()))
}

fn validate_spec(spec: &PredicateSpec) -> Result<()> {
    let region = match spec {
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
    if region.is_some_and(|(_, _, rows, cols)| rows == 0 || cols == 0) {
        bail!("stable region rows and columns must be nonzero")
    }
    Ok(())
}

fn region_valid(frame: &FrameSnapshot, row: u16, col: u16, rows: u16, cols: u16) -> bool {
    let Some(end_row) = row.checked_add(rows) else {
        return false;
    };
    let Some(end_col) = col.checked_add(cols) else {
        return false;
    };
    rows > 0
        && cols > 0
        && end_row <= frame.rows
        && end_col <= frame.cols
        && frame.cells.len() >= usize::from(end_row)
        && frame
            .cells
            .iter()
            .take(usize::from(end_row))
            .skip(usize::from(row))
            .all(|line| line.len() >= usize::from(end_col))
}

fn region_fingerprint(
    frame: &FrameSnapshot,
    row: u16,
    col: u16,
    rows: u16,
    cols: u16,
) -> Vec<Vec<CellFingerprint>> {
    frame.cells[usize::from(row)..usize::from(row + rows)]
        .iter()
        .map(|line| {
            line[usize::from(col)..usize::from(col + cols)]
                .iter()
                .map(CellFingerprint::from)
                .collect()
        })
        .collect()
}

fn elapsed_at_least(since: Option<Instant>, duration_ms: u64, now: Instant) -> bool {
    since.is_some_and(|since| {
        now.saturating_duration_since(since) >= Duration::from_millis(duration_ms)
    })
}
