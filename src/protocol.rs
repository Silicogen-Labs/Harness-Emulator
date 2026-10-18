use rmcp::schemars;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const PROTOCOL_VERSION: u16 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct LaunchSpec {
    pub name: Option<String>,
    pub program: String,
    #[serde(default)]
    pub args: Vec<String>,
    pub cwd: String,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[schemars(range(min = 1, max = 300))]
    pub cols: u16,
    #[schemars(range(min = 1, max = 120))]
    pub rows: u16,
    #[serde(default)]
    pub record_output: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "action", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)] // Keep the public request construction API flat; requests are not buffered in bulk.
pub enum RpcRequest {
    Ping,
    /// Explicit revision negotiation; v1 requests retain their legacy payloads.
    Contract {
        revision: u16,
        request: Box<RpcRequest>,
        response: Option<crate::projection::ResponseOptions>,
    },
    Launch {
        spec: LaunchSpec,
    },
    LaunchConfigured {
        spec: LaunchSpec,
        #[serde(default)]
        options: crate::config::LaunchOptions,
    },
    /// Versioned extensions validate operation-specific parameters in the service.
    Agent {
        operation: String,
        #[serde(default)]
        params: serde_json::Value,
    },
    List,
    Status {
        session_id: String,
    },
    History {
        session_id: String,
    },
    Observe {
        session_id: String,
    },
    Cells {
        session_id: String,
        row: u16,
        col: u16,
        rows: u16,
        cols: u16,
    },
    Screenshot {
        session_id: String,
    },
    Key {
        session_id: String,
        key: String,
    },
    Type {
        session_id: String,
        text: String,
    },
    Paste {
        session_id: String,
        text: String,
    },
    Mouse {
        session_id: String,
        input: MouseInput,
    },
    Resize {
        session_id: String,
        cols: u16,
        rows: u16,
    },
    Wait {
        session_id: String,
        predicate: WaitPredicate,
        timeout_ms: u64,
    },
    Restart {
        session_id: String,
    },
    Close {
        session_id: String,
    },
    ControlStatus {
        session_id: String,
    },
    ControlRelease {
        session_id: String,
        expected_generation: u64,
    },
    ViewStart {
        session_id: String,
        #[serde(default)]
        allow_browser_control: bool,
        #[serde(default)]
        idempotency_key: Option<String>,
    },
    ViewStatus {
        view_id: String,
    },
    ViewStop {
        view_id: String,
    },
    Shutdown,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum MouseInput {
    Move {
        row: u16,
        col: u16,
    },
    Click {
        row: u16,
        col: u16,
        button: MouseButton,
    },
    Down {
        row: u16,
        col: u16,
        button: MouseButton,
    },
    Up {
        row: u16,
        col: u16,
    },
    Wheel {
        row: u16,
        col: u16,
        direction: WheelDirection,
        #[schemars(range(min = 1, max = 20))]
        steps: u16,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum AgentAction {
    Key {
        key: String,
    },
    Type {
        text: String,
    },
    Paste {
        text: String,
    },
    Mouse {
        input: MouseInput,
    },
    Resize {
        #[schemars(range(min = 1, max = 300))]
        cols: u16,
        #[schemars(range(min = 1, max = 120))]
        rows: u16,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ActionContext {
    pub actor_id: String,
    pub action_id: String,
    pub expected_run_id: String,
    #[serde(default)]
    pub expected_viewport_revision: Option<u64>,
    #[serde(default)]
    pub controller_generation: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum MouseButton {
    Left,
    Middle,
    Right,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum WheelDirection {
    Up,
    Down,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum WaitPredicate {
    TextContains { text: String },
    TextAbsent { text: String },
    Regex { pattern: String },
    Stable { duration_ms: u64 },
    ProcessExited,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct RpcRequestEnvelope {
    pub version: u16,
    pub request_id: String,
    pub request: RpcRequest,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct RpcResponse {
    pub version: u16,
    pub request_id: String,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<RpcError>,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct RpcError {
    pub code: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<serde_json::Value>,
}

impl RpcResponse {
    pub fn success(request_id: String, result: serde_json::Value) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            request_id,
            ok: true,
            result: Some(result),
            error: None,
        }
    }

    pub fn failure(
        request_id: String,
        code: impl Into<String>,
        message: impl Into<String>,
    ) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            request_id,
            ok: false,
            result: None,
            error: Some(RpcError {
                code: code.into(),
                message: message.into(),
                details: None,
            }),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SessionSummary {
    pub session_id: String,
    pub name: Option<String>,
    pub run_id: String,
    pub state: String,
    pub program: String,
    pub args: Vec<String>,
    pub cwd: String,
    pub cols: u16,
    pub rows: u16,
    pub started_at_ms: u128,
    pub exit_code: Option<i32>,
    pub process_group: Option<i32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CellSnapshot {
    pub text: String,
    pub width: usize,
    pub foreground: Option<ColorValue>,
    pub background: Option<ColorValue>,
    pub resolved_foreground: String,
    pub resolved_background: String,
    pub bold: bool,
    pub dim: bool,
    pub italic: bool,
    pub inverse: bool,
    pub invisible: bool,
    pub strike: bool,
    pub blink: bool,
    pub underline: String,
    pub underline_color: Option<ColorValue>,
    pub hyperlink: Option<String>,
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, schemars::JsonSchema,
)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ColorValue {
    Named(u8),
    Indexed(u8),
    Rgb(u8, u8, u8),
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct FrameSnapshot {
    pub session_id: String,
    pub run_id: String,
    pub frame_id: String,
    pub captured_at_ms: u128,
    pub output_sequence: u64,
    pub cols: u16,
    pub rows: u16,
    pub text: String,
    pub base_style: CellStyle,
    pub styled_runs: Vec<StyledRun>,
    #[serde(skip, default)]
    pub cells: Vec<Vec<CellSnapshot>>,
    pub cursor: CursorSnapshot,
    pub modes: BTreeMap<String, bool>,
    pub keyboard_mode: Vec<String>,
    pub mouse_tracking: String,
    pub mouse_encoding: String,
    pub default_foreground: String,
    pub default_background: String,
    pub cursor_color: String,
    pub palette: BTreeMap<u8, String>,
    pub title: Option<String>,
    pub process_state: String,
    pub capture_complete: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub capture_warning: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct StyledRun {
    pub row: u16,
    pub start_col: u16,
    pub width: u16,
    pub text: String,
    pub style: CellStyle,
}

#[derive(
    Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, schemars::JsonSchema,
)]
pub struct CellStyle {
    pub foreground: String,
    pub background: String,
    pub bold: bool,
    pub dim: bool,
    pub italic: bool,
    pub inverse: bool,
    pub invisible: bool,
    pub strike: bool,
    pub blink: bool,
    pub underline: String,
    pub underline_color: Option<ColorValue>,
    pub hyperlink: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CellRegionSnapshot {
    pub session_id: String,
    pub run_id: String,
    pub frame_id: String,
    pub row: u16,
    pub col: u16,
    pub rows: u16,
    pub cols: u16,
    pub palette: BTreeMap<u8, String>,
    pub cells: Vec<Vec<CellSnapshot>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CursorSnapshot {
    pub row: u16,
    pub col: u16,
    pub visible: bool,
    pub shape: String,
    pub color: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ScreenshotArtifact {
    pub artifact_id: String,
    pub session_id: String,
    pub run_id: String,
    pub frame_id: String,
    pub path: String,
    pub sha256: String,
    pub cols: u16,
    pub rows: u16,
    pub captured_at_ms: u128,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct HistorySnapshot {
    pub session_id: String,
    pub run_id: String,
    pub events: Vec<RecordedEvent>,
    pub event_count: usize,
    pub retained_bytes: usize,
    pub max_bytes: usize,
    pub raw_recording_enabled: bool,
    pub truncated: bool,
    pub truncated_before_sequence: Option<u64>,
    pub replayable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct RecordedEvent {
    pub sequence: u64,
    pub at_ms: u128,
    pub kind: String,
    pub byte_count: usize,
    pub sha256: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub raw_base64: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct WaitResult {
    pub matched: bool,
    pub timed_out: bool,
    pub elapsed_ms: u64,
    pub snapshot: Option<FrameSnapshot>,
}
