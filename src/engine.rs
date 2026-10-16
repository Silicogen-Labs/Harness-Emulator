use crate::protocol::{
    CellSnapshot, CellStyle, ColorValue, CursorSnapshot, FrameSnapshot, MouseButton, MouseInput,
    StyledRun,
};
use anyhow::{Result, bail};
use std::collections::{BTreeMap, BTreeSet};
use std::time::{SystemTime, UNIX_EPOCH};
use tui_test::Backend;
use tui_test::api::KeyAction;
use tui_test::input::keys::{InputModes, token_to_seq_for_action_with_mode};
use tui_test::profile::{ColorSlot, Profile, Rgb};
use tui_test::terminal::cell::{Attrs, Color, EmuCell, UnderlineStyle, rows_to_strings};
use tui_test::terminal::emu::{Emulator, KeyboardMode, TerminalMode};
use vte::{Params, Parser, Perform};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MouseTracking {
    None,
    Click,
    Drag,
    Motion,
}

impl MouseTracking {
    fn name(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Click => "click",
            Self::Drag => "drag",
            Self::Motion => "motion",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MouseEncoding {
    Legacy,
    Utf8,
    Sgr,
    Urxvt,
    SgrPixels,
}

impl MouseEncoding {
    fn name(self) -> &'static str {
        match self {
            Self::Legacy => "legacy",
            Self::Utf8 => "utf8",
            Self::Sgr => "sgr",
            Self::Urxvt => "urxvt",
            Self::SgrPixels => "sgr_pixels",
        }
    }
}

#[derive(Debug)]
struct MouseModeState {
    tracking: MouseTracking,
    encoding: MouseEncoding,
}

impl Default for MouseModeState {
    fn default() -> Self {
        Self {
            tracking: MouseTracking::None,
            encoding: MouseEncoding::Legacy,
        }
    }
}

impl Perform for MouseModeState {
    fn csi_dispatch(&mut self, params: &Params, intermediates: &[u8], ignore: bool, action: char) {
        if ignore || intermediates != b"?" || !matches!(action, 'h' | 'l') {
            return;
        }
        let enabled = action == 'h';
        for mode in params.iter().filter_map(|param| param.first()).copied() {
            match mode {
                9 | 1000 if enabled => self.tracking = MouseTracking::Click,
                1002 if enabled => self.tracking = MouseTracking::Drag,
                1003 if enabled => self.tracking = MouseTracking::Motion,
                9 | 1000 if self.tracking == MouseTracking::Click => {
                    self.tracking = MouseTracking::None
                }
                1002 if self.tracking == MouseTracking::Drag => self.tracking = MouseTracking::None,
                1003 if self.tracking == MouseTracking::Motion => {
                    self.tracking = MouseTracking::None
                }
                1005 if enabled => self.encoding = MouseEncoding::Utf8,
                1006 if enabled => self.encoding = MouseEncoding::Sgr,
                1015 if enabled => self.encoding = MouseEncoding::Urxvt,
                1016 if enabled => self.encoding = MouseEncoding::SgrPixels,
                1005 if self.encoding == MouseEncoding::Utf8 => {
                    self.encoding = MouseEncoding::Legacy
                }
                1006 if self.encoding == MouseEncoding::Sgr => {
                    self.encoding = MouseEncoding::Legacy
                }
                1015 if self.encoding == MouseEncoding::Urxvt => {
                    self.encoding = MouseEncoding::Legacy
                }
                1016 if self.encoding == MouseEncoding::SgrPixels => {
                    self.encoding = MouseEncoding::Legacy
                }
                _ => {}
            }
        }
    }

    fn esc_dispatch(&mut self, intermediates: &[u8], ignore: bool, byte: u8) {
        if !ignore && intermediates.is_empty() && byte == b'c' {
            *self = Self::default()
        }
    }
}

pub struct MouseModeTracker {
    parser: Parser,
    state: MouseModeState,
}

impl Default for MouseModeTracker {
    fn default() -> Self {
        Self {
            parser: Parser::new(),
            state: MouseModeState::default(),
        }
    }
}

impl MouseModeTracker {
    pub fn process(&mut self, bytes: &[u8]) {
        self.parser.advance(&mut self.state, bytes)
    }

    pub fn tracking(&self) -> MouseTracking {
        self.state.tracking
    }

    pub fn encoding(&self) -> MouseEncoding {
        self.state.encoding
    }
}

pub struct EngineState {
    emulator: Box<dyn Emulator>,
    mouse: MouseModeTracker,
    frame_sequence: u64,
    output_sequence: u64,
}

impl EngineState {
    pub fn new(cols: u16, rows: u16) -> Result<Self> {
        if cols == 0 || rows == 0 {
            bail!("terminal size must be greater than zero")
        }
        let profile = Profile::default();
        let emulator = Backend::Alacritty.build(cols, rows, &profile)?;
        Ok(Self {
            emulator,
            mouse: MouseModeTracker::default(),
            frame_sequence: 0,
            output_sequence: 0,
        })
    }

    pub fn process_output(&mut self, bytes: &[u8]) -> Vec<u8> {
        self.mouse.process(bytes);
        self.emulator.process(bytes);
        self.output_sequence = self.output_sequence.saturating_add(bytes.len() as u64);
        self.frame_sequence = self.frame_sequence.saturating_add(1);
        self.emulator.take_pending_writes()
    }

    pub fn resize(&mut self, cols: u16, rows: u16) -> Result<()> {
        if cols == 0 || rows == 0 {
            bail!("terminal size must be greater than zero")
        }
        if cols > 300 || rows > 120 {
            bail!("unsupported terminal size: maximum is 300 columns by 120 rows")
        }
        self.emulator.resize(cols, rows);
        self.frame_sequence = self.frame_sequence.saturating_add(1);
        Ok(())
    }

    pub fn dimensions(&self) -> (u16, u16) {
        self.emulator.size()
    }

    pub fn output_sequence(&self) -> u64 {
        self.output_sequence
    }

    pub fn frame_sequence(&self) -> u64 {
        self.frame_sequence
    }

    pub fn mode(&self, mode: TerminalMode) -> bool {
        self.emulator.mode(mode)
    }

    pub fn mouse_tracking(&self) -> MouseTracking {
        self.mouse.tracking()
    }

    pub fn mouse_encoding(&self) -> MouseEncoding {
        self.mouse.encoding()
    }

    pub fn encode_key(&self, key: &str) -> Result<Vec<u8>> {
        let input_modes = InputModes {
            keyboard: self.emulator.keyboard_mode(),
            cursor_key_application: self.emulator.cursor_key_application(),
        };
        Ok(token_to_seq_for_action_with_mode(key, KeyAction::Press, input_modes)?.into_bytes())
    }

    pub fn encode_paste(&self, text: &str) -> Vec<u8> {
        if self.emulator.mode(TerminalMode::BracketedPaste) {
            let mut bytes = Vec::with_capacity(text.len() + 12);
            bytes.extend_from_slice(b"\x1b[200~");
            bytes.extend_from_slice(text.as_bytes());
            bytes.extend_from_slice(b"\x1b[201~");
            bytes
        } else {
            text.as_bytes().to_vec()
        }
    }

    pub fn encode_mouse(&self, input: &MouseInput) -> Result<Vec<u8>> {
        if self.mouse.encoding() != MouseEncoding::Sgr {
            bail!(
                "unsupported mouse operation: the application has not negotiated SGR mouse encoding"
            )
        }
        let tracking = self.mouse.tracking();
        if tracking == MouseTracking::None {
            bail!("unsupported mouse operation: the application has not enabled mouse tracking")
        }
        let (cols, rows) = self.emulator.size();
        let (row, col) = match input {
            MouseInput::Move { row, col }
            | MouseInput::Click { row, col, .. }
            | MouseInput::Down { row, col, .. }
            | MouseInput::Up { row, col }
            | MouseInput::Wheel { row, col, .. } => (*row, *col),
        };
        if row >= rows || col >= cols {
            bail!("mouse coordinate row={row}, col={col} is outside the {cols}x{rows} screen")
        }
        let mut out = String::new();
        match input {
            MouseInput::Move { row, col } => {
                if tracking != MouseTracking::Motion {
                    bail!(
                        "unsupported mouse operation: hover requires any-motion tracking (DECSET 1003)"
                    )
                }
                out.push_str(&sgr_mouse(35, *row, *col, true));
            }
            MouseInput::Click { row, col, button } => {
                let code = button_code(*button);
                out.push_str(&sgr_mouse(code, *row, *col, true));
                if tracking != MouseTracking::Click {
                    out.push_str(&sgr_mouse(3, *row, *col, false));
                }
            }
            MouseInput::Down { row, col, button } => {
                out.push_str(&sgr_mouse(button_code(*button), *row, *col, true));
            }
            MouseInput::Up { row, col } => {
                if tracking == MouseTracking::Click {
                    bail!(
                        "unsupported mouse operation: release events require VT200-or-higher tracking"
                    )
                }
                out.push_str(&sgr_mouse(3, *row, *col, false));
            }
            MouseInput::Wheel {
                row,
                col,
                direction,
                steps,
            } => {
                if *steps == 0 || *steps > 20 {
                    bail!("mouse wheel steps must be between 1 and 20")
                }
                let code = match direction {
                    crate::protocol::WheelDirection::Up => 64,
                    crate::protocol::WheelDirection::Down => 65,
                };
                for _ in 0..(*steps).max(1) {
                    out.push_str(&sgr_mouse(code, *row, *col, true));
                }
            }
        }
        Ok(out.into_bytes())
    }

    pub fn snapshot(&self, session_id: &str, run_id: &str, process_state: &str) -> FrameSnapshot {
        let (cols, rows) = self.emulator.size();
        let grid = self.emulator.viewable_rows();
        let text = rows_to_strings(&grid)
            .into_iter()
            .map(|line| line.trim_end().to_string())
            .collect::<Vec<_>>()
            .join("\n");
        let (base_style, styled_runs) = styled_runs(&grid, &*self.emulator);
        let cells = grid
            .iter()
            .map(|row| {
                row.iter()
                    .map(|cell| cell_snapshot(cell, &*self.emulator))
                    .collect()
            })
            .collect();
        let (cursor_col, cursor_row) = self.emulator.cursor();
        let colors = self.emulator.colors();
        let modes = TerminalMode::ALL
            .into_iter()
            .map(|mode| (mode.name().to_string(), self.emulator.mode(mode)))
            .collect::<BTreeMap<_, _>>();
        let cursor_color = self.emulator.color(ColorSlot::Cursor).to_hex();
        let cursor = CursorSnapshot {
            row: cursor_row,
            col: cursor_col,
            visible: self.emulator.cursor_visible(),
            shape: self.emulator.cursor_shape().name().to_string(),
            color: cursor_color.clone(),
        };
        let keyboard_mode = keyboard_mode_names(self.emulator.keyboard_mode());
        let palette = referenced_palette(&grid, &colors.palette);
        let fault = self.emulator.fault();
        FrameSnapshot {
            session_id: session_id.to_owned(),
            run_id: run_id.to_owned(),
            frame_id: format!("fr-{:08x}", self.frame_sequence),
            captured_at_ms: now_ms(),
            output_sequence: self.output_sequence,
            cols,
            rows,
            text,
            base_style,
            styled_runs,
            cells,
            cursor,
            modes,
            keyboard_mode,
            mouse_tracking: self.mouse.tracking().name().to_string(),
            mouse_encoding: self.mouse.encoding().name().to_string(),
            default_foreground: colors.foreground.to_hex(),
            default_background: colors.background.to_hex(),
            cursor_color,
            palette,
            title: self.emulator.title(),
            process_state: process_state.to_string(),
            capture_complete: fault.is_none(),
            capture_warning: fault,
        }
    }

    pub fn screenshot_cells(&self) -> (u16, u16, Vec<Vec<CellSnapshot>>, Rgb, Rgb, Rgb, String) {
        let (cols, rows) = self.emulator.size();
        let grid = self.emulator.viewable_rows();
        let cells = grid
            .iter()
            .map(|row| {
                row.iter()
                    .map(|cell| cell_snapshot(cell, &*self.emulator))
                    .collect()
            })
            .collect();
        let colors = self.emulator.colors();
        (
            cols,
            rows,
            cells,
            colors.foreground,
            colors.background,
            colors.cursor,
            self.emulator.cursor_shape().name().to_string(),
        )
    }
}

fn cell_snapshot(cell: &EmuCell, emulator: &dyn Emulator) -> CellSnapshot {
    let (resolved_foreground, resolved_background) = effective_colors(cell, emulator);
    CellSnapshot {
        text: cell.ch.to_string(),
        width: tui_test::terminal::cell::display_width(cell.ch.as_str()),
        foreground: cell.fg.map(color_value),
        background: cell.bg.map(color_value),
        resolved_foreground: resolved_foreground.to_hex(),
        resolved_background: resolved_background.to_hex(),
        bold: cell.has(Attrs::BOLD),
        dim: cell.has(Attrs::DIM),
        italic: cell.has(Attrs::ITALIC),
        inverse: cell.has(Attrs::INVERSE),
        invisible: cell.has(Attrs::INVISIBLE),
        strike: cell.has(Attrs::STRIKE),
        blink: cell.has(Attrs::BLINK),
        underline: underline_name(cell.underline).to_string(),
        underline_color: cell.underline_color.map(color_value),
        hyperlink: cell.uri().map(str::to_owned),
    }
}

fn style_of_cell(cell: &EmuCell, emulator: &dyn Emulator) -> CellStyle {
    let (foreground, background) = effective_colors(cell, emulator);
    CellStyle {
        foreground: foreground.to_hex(),
        background: background.to_hex(),
        bold: cell.has(Attrs::BOLD),
        dim: cell.has(Attrs::DIM),
        italic: cell.has(Attrs::ITALIC),
        inverse: cell.has(Attrs::INVERSE),
        invisible: cell.has(Attrs::INVISIBLE),
        strike: cell.has(Attrs::STRIKE),
        blink: cell.has(Attrs::BLINK),
        underline: underline_name(cell.underline).to_string(),
        underline_color: cell.underline_color.map(color_value),
        hyperlink: cell.uri().map(str::to_owned),
    }
}

fn styled_runs(grid: &[Vec<EmuCell>], emulator: &dyn Emulator) -> (CellStyle, Vec<StyledRun>) {
    let styles = grid
        .iter()
        .map(|row| {
            row.iter()
                .map(|cell| style_of_cell(cell, emulator))
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    let mut frequencies = BTreeMap::<CellStyle, usize>::new();
    for style in styles.iter().flatten() {
        *frequencies.entry(style.clone()).or_default() += 1;
    }
    let base_style = frequencies
        .into_iter()
        .max_by_key(|(_, count)| *count)
        .map(|(style, _)| style)
        .unwrap_or_else(|| CellStyle {
            foreground: emulator.color(ColorSlot::Foreground).to_hex(),
            background: emulator.color(ColorSlot::Background).to_hex(),
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
        });
    let mut output = Vec::new();
    for (row_index, row) in grid.iter().enumerate() {
        let row_styles = &styles[row_index];
        let mut col = 0;
        while col < row.len() {
            let style = &row_styles[col];
            if style == &base_style {
                col += 1;
                continue;
            }
            let start = col;
            let mut text = String::new();
            let mut width = 0usize;
            while col < row.len() && row_styles[col] == *style {
                text.push_str(row[col].ch.as_str());
                width += tui_test::terminal::cell::display_width(row[col].ch.as_str());
                col += 1;
            }
            output.push(StyledRun {
                row: row_index as u16,
                start_col: start as u16,
                width: width as u16,
                text,
                style: style.clone(),
            });
        }
    }
    (base_style, output)
}

fn effective_colors(cell: &EmuCell, emulator: &dyn Emulator) -> (Rgb, Rgb) {
    let mut foreground = emulator.resolve(cell.fg, true);
    let mut background = emulator.resolve(cell.bg, false);
    if cell.has(Attrs::INVERSE) {
        std::mem::swap(&mut foreground, &mut background);
    }
    if cell.has(Attrs::DIM) {
        foreground = Rgb::new(
            ((u16::from(foreground.r) + u16::from(background.r)) / 2) as u8,
            ((u16::from(foreground.g) + u16::from(background.g)) / 2) as u8,
            ((u16::from(foreground.b) + u16::from(background.b)) / 2) as u8,
        );
    }
    if cell.has(Attrs::INVISIBLE) {
        foreground = background;
    }
    (foreground, background)
}

fn referenced_palette(grid: &[Vec<EmuCell>], palette: &[Rgb; 256]) -> BTreeMap<u8, String> {
    let mut indexes = BTreeSet::new();
    for cell in grid.iter().flatten() {
        for color in [cell.fg, cell.bg, cell.underline_color]
            .into_iter()
            .flatten()
        {
            match color {
                Color::Named(color) => {
                    indexes.insert(color.index());
                }
                Color::Idx(index) => {
                    indexes.insert(index);
                }
                Color::Rgb(_, _, _) => {}
            }
        }
    }
    indexes
        .into_iter()
        .map(|index| (index, palette[index as usize].to_hex()))
        .collect()
}

fn color_value(color: Color) -> ColorValue {
    match color {
        Color::Named(color) => ColorValue::Named(color.index()),
        Color::Idx(index) => ColorValue::Indexed(index),
        Color::Rgb(r, g, b) => ColorValue::Rgb(r, g, b),
    }
}

fn underline_name(style: UnderlineStyle) -> &'static str {
    match style {
        UnderlineStyle::None => "none",
        UnderlineStyle::Single => "single",
        UnderlineStyle::Double => "double",
        UnderlineStyle::Curly => "curly",
        UnderlineStyle::Dotted => "dotted",
        UnderlineStyle::Dashed => "dashed",
    }
}

fn keyboard_mode_names(mode: KeyboardMode) -> Vec<String> {
    [
        (
            KeyboardMode::DISAMBIGUATE_ESC_CODES,
            "disambiguate_escape_codes",
        ),
        (KeyboardMode::REPORT_EVENT_TYPES, "report_event_types"),
        (KeyboardMode::REPORT_ALTERNATE_KEYS, "report_alternate_keys"),
        (
            KeyboardMode::REPORT_ALL_KEYS_AS_ESC,
            "report_all_keys_as_escape",
        ),
        (
            KeyboardMode::REPORT_ASSOCIATED_TEXT,
            "report_associated_text",
        ),
    ]
    .into_iter()
    .filter(|(flag, _)| mode.contains(*flag))
    .map(|(_, name)| name.to_string())
    .collect()
}

fn button_code(button: MouseButton) -> u8 {
    match button {
        MouseButton::Left => 0,
        MouseButton::Middle => 1,
        MouseButton::Right => 2,
    }
}

fn sgr_mouse(code: u8, row: u16, col: u16, pressed: bool) -> String {
    format!(
        "\x1b[<{code};{};{}{}",
        u32::from(col) + 1,
        u32::from(row) + 1,
        if pressed { 'M' } else { 'm' }
    )
}

pub fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn mouse_tracker_handles_split_sequences_and_resets() {
        let mut tracker = MouseModeTracker::default();
        tracker.process(b"\x1b[?1003h\x1b[?10");
        tracker.process(b"06h");
        assert_eq!(tracker.tracking(), MouseTracking::Motion);
        assert_eq!(tracker.encoding(), MouseEncoding::Sgr);
        tracker.process(b"\x1b[?1003l\x1bc");
        assert_eq!(tracker.tracking(), MouseTracking::None);
        assert_eq!(tracker.encoding(), MouseEncoding::Legacy);
    }

    #[test]
    fn mouse_input_fails_closed_without_sgr_tracking() {
        let engine = EngineState::new(80, 24).unwrap();
        let error = engine
            .encode_mouse(&MouseInput::Click {
                row: 1,
                col: 2,
                button: MouseButton::Left,
            })
            .unwrap_err();
        assert!(error.to_string().contains("SGR mouse encoding"));
    }

    #[test]
    fn paste_encoding_follows_the_negotiated_bracketed_mode() {
        let mut engine = EngineState::new(80, 24).unwrap();
        assert_eq!(engine.encode_paste("hello"), b"hello");

        engine.process_output(b"\x1b[?2004h");
        assert_eq!(engine.encode_paste("hello"), b"\x1b[200~hello\x1b[201~");
    }
}
