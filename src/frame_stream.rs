use crate::control::ControlStatus;
use crate::protocol::{CellStyle, FrameSnapshot};
use serde::Serialize;
use std::collections::BTreeMap;

pub const STREAM_VERSION: u16 = 1;

#[derive(Debug, Clone, Serialize)]
pub struct StreamFrame {
    pub stream_version: u16,
    pub view_revision: u64,
    pub viewport_revision: u64,
    pub coalesced_revisions: u64,
    pub control: ControlStatus,
    pub session_state: String,
    pub frame: FrameSnapshot,
    pub styles: Vec<CellStyle>,
    pub cells: Vec<Vec<StreamCell>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StreamCell(pub String, pub u8, pub u16, pub bool, pub Option<String>);

impl StreamFrame {
    pub fn from_frame(
        frame: FrameSnapshot,
        view_revision: u64,
        viewport_revision: u64,
        control: ControlStatus,
        session_state: String,
    ) -> Option<Self> {
        let (styles, cells) = compact_cells(&frame.cells)?;

        Some(Self {
            stream_version: STREAM_VERSION,
            view_revision,
            viewport_revision,
            coalesced_revisions: 0,
            control,
            session_state,
            frame,
            styles,
            cells,
        })
    }
}

/// Shared browser/MCP cell encoding; palette colors are resolved once per style.
pub fn compact_cells(
    grid: &[Vec<crate::protocol::CellSnapshot>],
) -> Option<(Vec<CellStyle>, Vec<Vec<StreamCell>>)> {
    let mut style_ids = BTreeMap::<CellStyle, u16>::new();
    let mut styles = Vec::new();
    let mut cells = Vec::with_capacity(grid.len());

    for row in grid {
        let mut compact_row = Vec::with_capacity(row.len());
        for cell in row {
            let style = CellStyle {
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
                hyperlink: None,
            };
            let style_id = if let Some(id) = style_ids.get(&style) {
                *id
            } else {
                let id = u16::try_from(styles.len()).ok()?;
                styles.push(style.clone());
                style_ids.insert(style, id);
                id
            };
            compact_row.push(StreamCell(
                cell.text.clone(),
                u8::try_from(cell.width).ok()?,
                style_id,
                cell.text.is_empty(),
                cell.hyperlink.clone(),
            ));
        }
        cells.push(compact_row);
    }

    Some((styles, cells))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::control::ControlOwner;
    use crate::protocol::{CellSnapshot, CursorSnapshot};
    use std::collections::BTreeMap;

    fn frame(cells: Vec<Vec<CellSnapshot>>) -> FrameSnapshot {
        FrameSnapshot {
            session_id: "session".into(),
            run_id: "run".into(),
            frame_id: "frame".into(),
            captured_at_ms: 1,
            output_sequence: 2,
            cols: 3,
            rows: 1,
            text: "界x".into(),
            base_style: CellStyle {
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
            },
            styled_runs: Vec::new(),
            cells,
            cursor: CursorSnapshot {
                row: 0,
                col: 2,
                visible: true,
                shape: "block".into(),
                color: "#ffffff".into(),
            },
            modes: BTreeMap::new(),
            keyboard_mode: Vec::new(),
            mouse_tracking: "none".into(),
            mouse_encoding: "legacy".into(),
            default_foreground: "#ffffff".into(),
            default_background: "#000000".into(),
            cursor_color: "#ffffff".into(),
            palette: BTreeMap::new(),
            title: None,
            process_state: "running".into(),
            capture_complete: true,
            capture_warning: None,
        }
    }

    fn cell(text: &str, width: usize, background: &str) -> CellSnapshot {
        CellSnapshot {
            text: text.into(),
            width,
            foreground: None,
            background: None,
            resolved_foreground: "#ffffff".into(),
            resolved_background: background.into(),
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

    #[test]
    fn stream_compacts_styles_and_preserves_wide_continuation_and_blank_backgrounds() {
        let frame = frame(vec![vec![
            cell("界", 2, "#112233"),
            cell("", 0, "#112233"),
            cell("x", 1, "#000000"),
        ]]);
        let stream = StreamFrame::from_frame(
            frame,
            7,
            3,
            ControlStatus {
                owner: ControlOwner::Automation,
                generation: 0,
                view_id: None,
                connection_id: None,
                expires_in_ms: None,
            },
            "running".into(),
        )
        .unwrap();
        assert_eq!(stream.view_revision, 7);
        assert_eq!(stream.viewport_revision, 3);
        assert_eq!(stream.styles.len(), 2);
        assert_eq!(stream.cells[0][0].0, "界");
        assert_eq!(stream.cells[0][0].1, 2);
        assert!(stream.cells[0][1].3);
        assert_ne!(stream.cells[0][0].2, stream.cells[0][2].2);

        let serialized = serde_json::to_value(stream).unwrap();
        assert!(serialized["cells"][0][1][3].as_bool().unwrap());
        assert_eq!(serialized["frame"]["frame_id"], "frame");
    }
}
