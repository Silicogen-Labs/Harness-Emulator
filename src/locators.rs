//! Bounded text and regex location over rendered terminal rows.
//!
//! Rows are searched as rendered. A literal newline or regex `\n` can span a
//! row boundary; soft wraps cannot be distinguished from explicit line breaks
//! because the frame snapshot does not retain that distinction.

use crate::protocol::{CellSnapshot, FrameSnapshot};
use anyhow::{Result, bail};
use regex::Regex;
use rmcp::schemars;
use serde::{Deserialize, Serialize};

pub const MAX_QUERY_BYTES: usize = 4096;
pub const MAX_MATCHES: usize = 256;
pub const MAX_SNIPPET_BYTES: usize = 64 * 1024;
const MAX_FRAME_CELLS: usize = 300 * 120;
const MAX_RENDERED_BYTES: usize = 4 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum LocatorQuery {
    Text(String),
    Regex(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CellRect {
    pub row_start: u16,
    pub col_start: u16,
    /// Exclusive end coordinate.
    pub row_end: u16,
    /// Exclusive end coordinate.
    pub col_end: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ObservationIdentity {
    pub session_id: String,
    pub run_id: String,
    pub frame_id: String,
    pub observation_revision: u64,
    pub viewport_revision: u64,
    pub cols: u16,
    pub rows: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct LocatedMatch {
    pub rect: CellRect,
    /// Tight per-rendered-row rectangles, used for precise cell intersection.
    pub cell_rects: Vec<CellRect>,
    pub snippet: String,
    pub observation: ObservationIdentity,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct LocatorResults {
    pub matches: Vec<LocatedMatch>,
    /// True when at least one non-empty cell-backed match was found, even if
    /// every snippet was omitted by the bounded result limits.
    pub any_match: bool,
    pub truncated: bool,
    pub omitted_matches: usize,
}

/// Locate non-empty literal or regex matches in a bounded cell region.
///
/// Cell rectangles are zero-based and half-open. Wide glyphs cover every cell
/// in their declared width; zero-width combining cells are attached to the
/// previous rendered cell. Matches spanning rows have one enclosing rectangle.
#[allow(dead_code)] // Also compiled as an isolated module by algorithm tests.
pub fn locate_text(
    frame: &FrameSnapshot,
    query: &LocatorQuery,
    region: Option<CellRect>,
    observation_revision: u64,
    viewport_revision: u64,
) -> Result<LocatorResults> {
    validate_frame(frame)?;
    let region = region.unwrap_or(CellRect {
        row_start: 0,
        col_start: 0,
        row_end: frame.rows,
        col_end: frame.cols,
    });
    validate_region(frame, region)?;
    let regex = compile_query(query)?;
    locate_cells(
        frame,
        &frame.cells,
        &regex,
        region,
        observation_revision,
        viewport_revision,
    )
}

pub(crate) fn compile_query(query: &LocatorQuery) -> Result<Regex> {
    let raw_bytes = match query {
        LocatorQuery::Text(text) => {
            if text.is_empty() {
                bail!("locator text must not be empty")
            }
            text.len()
        }
        LocatorQuery::Regex(pattern) => pattern.len(),
    };
    if raw_bytes > MAX_QUERY_BYTES {
        bail!("locator query exceeds {MAX_QUERY_BYTES} bytes")
    }
    let pattern = match query {
        LocatorQuery::Text(text) => regex::escape(text),
        LocatorQuery::Regex(pattern) => pattern.clone(),
    };
    Ok(Regex::new(&pattern)?)
}

pub(crate) fn validate_frame(frame: &FrameSnapshot) -> Result<()> {
    let cell_count = usize::from(frame.cols).saturating_mul(usize::from(frame.rows));
    if frame.cols == 0
        || frame.rows == 0
        || frame.cols > 300
        || frame.rows > 120
        || cell_count > MAX_FRAME_CELLS
        || frame.cells.len() != usize::from(frame.rows)
        || frame
            .cells
            .iter()
            .any(|row| row.len() != usize::from(frame.cols))
    {
        bail!("frame cell grid does not match supported viewport geometry")
    }
    Ok(())
}

pub(crate) fn validate_region(frame: &FrameSnapshot, region: CellRect) -> Result<()> {
    if region.row_start >= region.row_end
        || region.col_start >= region.col_end
        || region.row_end > frame.rows
        || region.col_end > frame.cols
    {
        bail!("locator region must be a non-empty rectangle within the viewport")
    }
    Ok(())
}

pub(crate) trait LocatorCell {
    fn text(&self) -> &str;
    fn width(&self) -> usize;
}

impl LocatorCell for CellSnapshot {
    fn text(&self) -> &str {
        &self.text
    }

    fn width(&self) -> usize {
        self.width
    }
}

pub(crate) fn locate_cells<C: LocatorCell>(
    frame: &FrameSnapshot,
    cells: &[Vec<C>],
    regex: &Regex,
    region: CellRect,
    observation_revision: u64,
    viewport_revision: u64,
) -> Result<LocatorResults> {
    locate_cells_with_predicate(
        frame,
        cells,
        regex,
        region,
        observation_revision,
        viewport_revision,
        |_| false,
    )
    .map(|(results, _)| results)
}

pub(crate) fn locate_cells_with_predicate<C, F>(
    frame: &FrameSnapshot,
    cells: &[Vec<C>],
    regex: &Regex,
    region: CellRect,
    observation_revision: u64,
    viewport_revision: u64,
    mut is_relevant: F,
) -> Result<(LocatorResults, bool)>
where
    C: LocatorCell,
    F: FnMut(&[CellRect]) -> bool,
{
    validate_region(frame, region)?;
    let mut rendered = String::new();
    let mut byte_cells: Vec<(std::ops::Range<usize>, u16, u16, u16)> = Vec::new();

    for row in region.row_start..region.row_end {
        if row > region.row_start {
            rendered.push('\n');
        }
        for col in region.col_start..region.col_end {
            let cell = cells
                .get(usize::from(row))
                .and_then(|line| line.get(usize::from(col)))
                .ok_or_else(|| anyhow::anyhow!("frame cell grid is incomplete"))?;
            if cell.text().is_empty() {
                continue;
            }
            if cell.width() == 0 && col > region.col_start {
                // A zero-width codepoint extends the prior glyph. Map it to
                // that glyph's cell span so a combining-mark match remains
                // locatable without inventing a display column.
                let start = rendered.len();
                rendered.push_str(cell.text());
                if rendered.len() > MAX_RENDERED_BYTES {
                    bail!("rendered locator text exceeds the 4 MiB scan limit")
                }
                if let Some(previous) = byte_cells.last() {
                    byte_cells.push((start..rendered.len(), previous.1, previous.2, previous.3));
                } else {
                    byte_cells.push((start..rendered.len(), row, col, col.saturating_add(1)));
                }
                continue;
            }
            let start = rendered.len();
            rendered.push_str(cell.text());
            if rendered.len() > MAX_RENDERED_BYTES {
                bail!("rendered locator text exceeds the 4 MiB scan limit")
            }
            let width = u16::try_from(cell.width()).unwrap_or(u16::MAX).max(1);
            let end_col = col.saturating_add(width).min(region.col_end);
            byte_cells.push((start..rendered.len(), row, col, end_col));
        }
    }

    let identity = ObservationIdentity {
        session_id: frame.session_id.clone(),
        run_id: frame.run_id.clone(),
        frame_id: frame.frame_id.clone(),
        observation_revision,
        viewport_revision,
        cols: frame.cols,
        rows: frame.rows,
    };
    let mut found = Vec::new();
    let mut snippet_bytes = 0usize;
    let mut omitted_matches = 0usize;
    let mut relevant_match_found = false;
    let mut any_match = false;
    for hit in regex.find_iter(&rendered) {
        if hit.start() == hit.end() {
            continue;
        }
        let first_span = byte_cells.partition_point(|(bytes, _, _, _)| bytes.end <= hit.start());
        let end_span = byte_cells.partition_point(|(bytes, _, _, _)| bytes.start < hit.end());
        let spans = byte_cells.get(first_span..end_span).unwrap_or_default();
        let Some(first) = spans.first() else { continue };
        any_match = true;
        let rect = CellRect {
            row_start: spans.iter().map(|span| span.1).min().unwrap_or(first.1),
            col_start: spans.iter().map(|span| span.2).min().unwrap_or(first.2),
            row_end: spans
                .iter()
                .map(|span| span.1.saturating_add(1))
                .max()
                .unwrap_or(first.1.saturating_add(1)),
            col_end: spans.iter().map(|span| span.3).max().unwrap_or(first.3),
        };
        let mut row_rects: Vec<CellRect> = Vec::new();
        for (_, row, col_start, col_end) in spans {
            if let Some(last) = row_rects.last_mut().filter(|rect| rect.row_start == *row) {
                last.col_start = last.col_start.min(*col_start);
                last.col_end = last.col_end.max(*col_end);
            } else {
                row_rects.push(CellRect {
                    row_start: *row,
                    col_start: *col_start,
                    row_end: row.saturating_add(1),
                    col_end: *col_end,
                });
            }
        }
        relevant_match_found |= is_relevant(&row_rects);
        let snippet_len = hit.end() - hit.start();
        if found.len() >= MAX_MATCHES
            || snippet_bytes.saturating_add(snippet_len) > MAX_SNIPPET_BYTES
        {
            omitted_matches = omitted_matches.saturating_add(1);
            continue;
        }
        found.push(LocatedMatch {
            rect,
            cell_rects: row_rects,
            snippet: rendered[hit.start()..hit.end()].to_string(),
            observation: identity.clone(),
        });
        snippet_bytes += snippet_len;
    }
    Ok((
        LocatorResults {
            matches: found,
            any_match,
            truncated: omitted_matches > 0,
            omitted_matches,
        },
        relevant_match_found,
    ))
}
