use crate::protocol::{FrameSnapshot, ScreenshotArtifact};
use anyhow::{Context, Result};
use resvg::tiny_skia::{Pixmap, Transform};
use resvg::usvg::{Options, Tree};
use sha2::{Digest, Sha256};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

const CELL_WIDTH: u16 = 10;
const CELL_HEIGHT: u16 = 20;
const PANEL_TOP: u16 = 34;
const PAD: u16 = 16;

pub fn render_png(
    frame: &FrameSnapshot,
    path: &Path,
    artifact_id: String,
) -> Result<ScreenshotArtifact> {
    let svg = frame_svg(frame);
    let mut options = Options::default();
    options.fontdb_mut().load_system_fonts();
    let tree = Tree::from_str(&svg, &options).context("parse terminal screenshot SVG")?;
    let size = tree.size().to_int_size();
    let mut pixmap =
        Pixmap::new(size.width(), size.height()).context("allocate terminal screenshot image")?;
    resvg::render(&tree, Transform::default(), &mut pixmap.as_mut());
    let png = pixmap
        .encode_png()
        .context("encode terminal screenshot PNG")?;
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| format!("create screenshot {}", path.display()))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(std::fs::Permissions::from_mode(0o600))?;
    }
    file.write_all(&png)?;
    file.sync_all()?;
    let digest = Sha256::digest(&png);
    let sha256 = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    Ok(ScreenshotArtifact {
        artifact_id,
        session_id: frame.session_id.clone(),
        run_id: frame.run_id.clone(),
        frame_id: frame.frame_id.clone(),
        path: path.display().to_string(),
        sha256,
        cols: frame.cols,
        rows: frame.rows,
        captured_at_ms: frame.captured_at_ms,
    })
}

pub fn frame_svg(frame: &FrameSnapshot) -> String {
    let width = u32::from(frame.cols) * u32::from(CELL_WIDTH) + u32::from(PAD) * 2;
    let height =
        u32::from(frame.rows) * u32::from(CELL_HEIGHT) + u32::from(PANEL_TOP) + u32::from(PAD) * 2;
    let mut svg = String::with_capacity((frame.cols as usize * frame.rows as usize * 180) + 1024);
    svg.push_str(&format!(
        "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"{width}\" height=\"{height}\" viewBox=\"0 0 {width} {height}\">"
    ));
    svg.push_str("<rect width=\"100%\" height=\"100%\" fill=\"#6867aa\"/>");
    svg.push_str(&format!(
        "<rect x=\"8\" y=\"8\" width=\"{}\" height=\"{}\" rx=\"8\" fill=\"#000000\"/>",
        width - 16,
        height - 16
    ));
    svg.push_str("<rect x=\"8\" y=\"8\" width=\"");
    svg.push_str(&(width - 16).to_string());
    svg.push_str("\" height=\"26\" rx=\"8\" fill=\"#d9d9e8\"/>");
    svg.push_str(&format!(
        "<text x=\"{}\" y=\"26\" font-family=\"DejaVu Sans Mono,monospace\" font-size=\"12\" fill=\"#33333b\">{} · {}×{}</text>",
        PAD + 3,
        xml_escape(&frame.session_id),
        frame.cols,
        frame.rows
    ));
    let left = u32::from(PAD);
    let top = u32::from(PANEL_TOP);
    svg.push_str(&format!(
        "<rect x=\"{left}\" y=\"{top}\" width=\"{}\" height=\"{}\" fill=\"{}\"/>",
        u32::from(frame.cols) * u32::from(CELL_WIDTH),
        u32::from(frame.rows) * u32::from(CELL_HEIGHT),
        frame.default_background
    ));

    for (row_index, row) in frame.cells.iter().enumerate().take(frame.rows as usize) {
        for (col_index, cell) in row.iter().enumerate().take(frame.cols as usize) {
            let x = left + col_index as u32 * u32::from(CELL_WIDTH);
            let y = top + row_index as u32 * u32::from(CELL_HEIGHT);
            if cell.resolved_background != frame.default_background {
                svg.push_str(&format!(
                    "<rect x=\"{x}\" y=\"{y}\" width=\"{}\" height=\"{}\" fill=\"{}\"/>",
                    u32::from(CELL_WIDTH) * cell.width.max(1) as u32,
                    CELL_HEIGHT,
                    cell.resolved_background
                ));
            }
            if cell.text.is_empty() || cell.width == 0 {
                continue;
            }
            let weight = if cell.bold { "bold" } else { "normal" };
            let style = if cell.italic { "italic" } else { "normal" };
            let decoration = match (cell.underline != "none", cell.strike) {
                (true, true) => "underline line-through",
                (true, false) => "underline",
                (false, true) => "line-through",
                (false, false) => "none",
            };
            let opacity = if cell.dim { "0.65" } else { "1" };
            let span_width = u32::from(CELL_WIDTH) * cell.width as u32;
            svg.push_str(&format!(
                "<text x=\"{x}\" y=\"{}\" font-family=\"DejaVu Sans Mono,monospace\" font-size=\"16\" font-weight=\"{weight}\" font-style=\"{style}\" text-decoration=\"{decoration}\" fill=\"{}\" opacity=\"{opacity}\" textLength=\"{span_width}\" lengthAdjust=\"spacingAndGlyphs\">{}</text>",
                y + 15,
                cell.resolved_foreground,
                xml_escape(&cell.text)
            ));
        }
    }

    if frame.cursor.visible && frame.cursor.row < frame.rows && frame.cursor.col < frame.cols {
        let x = left + u32::from(frame.cursor.col) * u32::from(CELL_WIDTH);
        let y = top + u32::from(frame.cursor.row) * u32::from(CELL_HEIGHT);
        match frame.cursor.shape.as_str() {
            "underline" => svg.push_str(&format!(
                "<rect x=\"{x}\" y=\"{}\" width=\"{}\" height=\"2\" fill=\"{}\"/>",
                y + u32::from(CELL_HEIGHT) - 2,
                CELL_WIDTH,
                frame.cursor.color
            )),
            "bar" => svg.push_str(&format!(
                "<rect x=\"{x}\" y=\"{y}\" width=\"2\" height=\"{}\" fill=\"{}\"/>",
                CELL_HEIGHT,
                frame.cursor.color
            )),
            _ => svg.push_str(&format!(
                "<rect x=\"{x}\" y=\"{y}\" width=\"{}\" height=\"{}\" fill=\"none\" stroke=\"{}\" stroke-width=\"1\"/>",
                CELL_WIDTH,
                CELL_HEIGHT,
                frame.cursor.color
            )),
        }
    }

    svg.push_str("</svg>");
    svg
}

fn xml_escape(text: &str) -> String {
    let mut output = String::with_capacity(text.len());
    for character in text.chars() {
        match character {
            '&' => output.push_str("&amp;"),
            '<' => output.push_str("&lt;"),
            '>' => output.push_str("&gt;"),
            '"' => output.push_str("&quot;"),
            '\'' => output.push_str("&apos;"),
            character if character.is_control() => output.push(' '),
            character => output.push(character),
        }
    }
    output
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::CellSnapshot;

    #[test]
    fn svg_escapes_application_text_and_includes_frame_content() {
        let mut frame: FrameSnapshot = serde_json::from_value(serde_json::json!({
            "session_id":"s1", "run_id":"r1", "frame_id":"f1", "captured_at_ms":1,
            "output_sequence":1, "cols":1, "rows":1, "text":"<unsafe>",
            "base_style":{"foreground":"#ffffff","background":"#000000","bold":false,"dim":false,
                "italic":false,"inverse":false,"invisible":false,"strike":false,"blink":false,
                "underline":"none","underline_color":null,"hyperlink":null},
            "styled_runs":[],
            "cursor":{"row":0,"col":0,"visible":false,"shape":"block","color":"#ffffff"},
            "modes":{},"keyboard_mode":[],"mouse_tracking":"none","mouse_encoding":"legacy",
            "default_foreground":"#ffffff","default_background":"#000000","cursor_color":"#ffffff",
            "palette":{},"title":null,"process_state":"running","capture_complete":true
        }))
        .unwrap();
        frame.text = "<unsafe>".into();
        frame.cells = vec![vec![CellSnapshot {
            text: "<".into(),
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
        }]];
        let svg = frame_svg(&frame);
        assert!(svg.contains("&lt;"));
        assert!(!svg.contains("><unsafe><"));
    }
}
