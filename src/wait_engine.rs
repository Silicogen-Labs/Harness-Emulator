//! Service-independent fresh-wait state machine.
//!
//! A baseline is retained by the evaluator for its complete lifetime. Callers
//! should keep one evaluator per wait id rather than looking the baseline up in
//! a short-lived observation cache on every resume.

use crate::locators::{
    CellRect, LocatorCell, LocatorQuery, LocatorResults, ObservationIdentity, compile_query,
    locate_cells, validate_frame, validate_region,
};
use crate::protocol::{CellSnapshot, FrameSnapshot};
use anyhow::{Result, bail};
use rmcp::schemars;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::Instant;

const MAX_BASELINE_BYTES: usize = 8 * 1024 * 1024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FreshTransition {
    CurrentPresence,
    BecamePresent,
    MatchIntersectsChangedCells,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FreshWaitSpec {
    pub query: LocatorQuery,
    #[serde(default)]
    pub region: Option<CellRect>,
    pub transition: FreshTransition,
}

#[derive(Debug, Clone)]
pub struct WaitBaseline {
    pub identity: ObservationIdentity,
    cells: Vec<Vec<BaselineCell>>,
}

#[derive(Debug, Clone)]
struct BaselineCell {
    text: String,
    width: usize,
    fingerprint: [u8; 32],
}

impl LocatorCell for BaselineCell {
    fn text(&self) -> &str {
        &self.text
    }

    fn width(&self) -> usize {
        self.width
    }
}

impl WaitBaseline {
    #[allow(dead_code)] // Production sequence integration uses this; isolated wait tests compile the module separately.
    pub fn retained_bytes(&self) -> usize {
        self.cells
            .iter()
            .flatten()
            .map(|cell| {
                cell.text
                    .capacity()
                    .saturating_add(std::mem::size_of::<BaselineCell>())
            })
            .fold(std::mem::size_of::<Self>(), usize::saturating_add)
    }

    /// Capture text, width and a compact style/content fingerprint per cell.
    /// This retains enough state for matching and exact change detection while
    /// keeping the baseline bounded to 8 MiB.
    pub fn capture(
        frame: &FrameSnapshot,
        observation_revision: u64,
        viewport_revision: u64,
    ) -> Result<Self> {
        validate_frame(frame)?;
        let mut retained_bytes = 0usize;
        let mut cells = Vec::with_capacity(frame.cells.len());
        for row in &frame.cells {
            let mut baseline_row = Vec::with_capacity(row.len());
            for cell in row {
                retained_bytes = retained_bytes
                    .saturating_add(cell.text.len())
                    .saturating_add(std::mem::size_of::<usize>() + 32);
                if retained_bytes > MAX_BASELINE_BYTES {
                    bail!("wait baseline exceeds the 8 MiB retained-state limit")
                }
                baseline_row.push(BaselineCell {
                    text: cell.text.clone(),
                    width: cell.width,
                    fingerprint: cell_fingerprint(cell)?,
                });
            }
            cells.push(baseline_row);
        }
        Ok(Self {
            identity: ObservationIdentity {
                session_id: frame.session_id.clone(),
                run_id: frame.run_id.clone(),
                frame_id: frame.frame_id.clone(),
                observation_revision,
                viewport_revision,
                cols: frame.cols,
                rows: frame.rows,
            },
            cells,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WaitOutcome {
    Pending,
    Matched(LocatorResults),
    StaleBaseline,
    IncompatibleViewport,
    Cancelled,
    DeadlineExceeded,
}

pub struct WaitEvaluator {
    spec: FreshWaitSpec,
    baseline: WaitBaseline,
    deadline: Instant,
    regex: regex::Regex,
    baseline_match_present: bool,
    absence_observed: bool,
    terminal_outcome: Option<WaitOutcome>,
}

impl WaitEvaluator {
    pub fn new(spec: FreshWaitSpec, baseline: WaitBaseline, deadline: Instant) -> Result<Self> {
        let regex = compile_query(&spec.query)?;
        let region = spec.region.unwrap_or(CellRect {
            row_start: 0,
            col_start: 0,
            row_end: baseline.identity.rows,
            col_end: baseline.identity.cols,
        });
        let baseline_frame = FrameSnapshot {
            session_id: baseline.identity.session_id.clone(),
            run_id: baseline.identity.run_id.clone(),
            frame_id: baseline.identity.frame_id.clone(),
            captured_at_ms: 0,
            output_sequence: 0,
            cols: baseline.identity.cols,
            rows: baseline.identity.rows,
            text: String::new(),
            base_style: empty_style(),
            styled_runs: Vec::new(),
            cells: Vec::new(),
            cursor: crate::protocol::CursorSnapshot {
                row: 0,
                col: 0,
                visible: false,
                shape: String::new(),
                color: String::new(),
            },
            modes: Default::default(),
            keyboard_mode: Vec::new(),
            mouse_tracking: String::new(),
            mouse_encoding: String::new(),
            default_foreground: String::new(),
            default_background: String::new(),
            cursor_color: String::new(),
            palette: Default::default(),
            title: None,
            process_state: "running".into(),
            capture_complete: true,
            capture_warning: None,
        };
        validate_region(&baseline_frame, region)?;
        let baseline_match_present = locate_cells(
            &baseline_frame,
            &baseline.cells,
            &regex,
            region,
            baseline.identity.observation_revision,
            baseline.identity.viewport_revision,
        )?
        .any_match;
        Ok(Self {
            spec,
            baseline,
            deadline,
            regex,
            baseline_match_present,
            absence_observed: false,
            terminal_outcome: None,
        })
    }

    pub fn observe(
        &mut self,
        frame: &FrameSnapshot,
        revision: u64,
        viewport_revision: u64,
        now: Instant,
    ) -> WaitOutcome {
        if let Some(outcome) = &self.terminal_outcome {
            return outcome.clone();
        }
        if now >= self.deadline {
            return self.finish(WaitOutcome::DeadlineExceeded);
        }
        if frame.session_id != self.baseline.identity.session_id
            || frame.run_id != self.baseline.identity.run_id
            || revision < self.baseline.identity.observation_revision
        {
            return self.finish(WaitOutcome::StaleBaseline);
        }
        if frame.cols != self.baseline.identity.cols || frame.rows != self.baseline.identity.rows {
            return self.finish(WaitOutcome::IncompatibleViewport);
        }
        if viewport_revision != self.baseline.identity.viewport_revision {
            return self.finish(WaitOutcome::IncompatibleViewport);
        }
        if validate_frame(frame).is_err() {
            return self.finish(WaitOutcome::IncompatibleViewport);
        }
        let region = self.spec.region.unwrap_or(CellRect {
            row_start: 0,
            col_start: 0,
            row_end: frame.rows,
            col_end: frame.cols,
        });
        let (located, changed_match) = match crate::locators::locate_cells_with_predicate(
            frame,
            &frame.cells,
            &self.regex,
            region,
            revision,
            viewport_revision,
            |rects| {
                rects
                    .iter()
                    .any(|rect| changed_cells_intersect(&self.baseline.cells, &frame.cells, *rect))
            },
        ) {
            Ok(matches) => matches,
            Err(_) => return self.finish(WaitOutcome::IncompatibleViewport),
        };
        let is_after_baseline = revision > self.baseline.identity.observation_revision;

        let matched = match self.spec.transition {
            FreshTransition::CurrentPresence => located.any_match,
            FreshTransition::BecamePresent => {
                if self.baseline_match_present && is_after_baseline && !located.any_match {
                    self.absence_observed = true;
                }
                if is_after_baseline {
                    if !self.baseline_match_present {
                        located.any_match
                    } else {
                        self.absence_observed && located.any_match
                    }
                } else {
                    false
                }
            }
            FreshTransition::MatchIntersectsChangedCells => is_after_baseline && changed_match,
        };
        if matched {
            self.finish(WaitOutcome::Matched(located))
        } else {
            WaitOutcome::Pending
        }
    }

    /// Advance timer-driven state without requiring another output event.
    pub fn tick(&mut self, now: Instant) -> WaitOutcome {
        if let Some(outcome) = &self.terminal_outcome {
            return outcome.clone();
        }
        if now >= self.deadline {
            self.finish(WaitOutcome::DeadlineExceeded)
        } else {
            WaitOutcome::Pending
        }
    }

    pub fn cancel(&mut self) -> WaitOutcome {
        if let Some(outcome) = &self.terminal_outcome {
            return outcome.clone();
        }
        self.finish(WaitOutcome::Cancelled)
    }

    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    fn finish(&mut self, outcome: WaitOutcome) -> WaitOutcome {
        self.terminal_outcome = Some(outcome.clone());
        outcome
    }
}

fn changed_cells_intersect(
    baseline: &[Vec<BaselineCell>],
    current: &[Vec<CellSnapshot>],
    rect: CellRect,
) -> bool {
    for row in rect.row_start..rect.row_end {
        for col in rect.col_start..rect.col_end {
            let Some(old) = baseline
                .get(usize::from(row))
                .and_then(|cells| cells.get(usize::from(col)))
            else {
                continue;
            };
            let Some(new) = current
                .get(usize::from(row))
                .and_then(|cells| cells.get(usize::from(col)))
            else {
                continue;
            };
            if old.fingerprint != cell_fingerprint(new).unwrap_or([0; 32]) {
                return true;
            }
        }
    }
    false
}

fn cell_fingerprint(cell: &CellSnapshot) -> Result<[u8; 32]> {
    let encoded = serde_json::to_vec(cell)?;
    Ok(Sha256::digest(encoded).into())
}

fn empty_style() -> crate::protocol::CellStyle {
    crate::protocol::CellStyle {
        foreground: String::new(),
        background: String::new(),
        bold: false,
        dim: false,
        italic: false,
        inverse: false,
        invisible: false,
        strike: false,
        blink: false,
        underline: String::new(),
        underline_color: None,
        hyperlink: None,
    }
}
