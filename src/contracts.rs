//! Shared requests for MCP, daemon validation and scripted scenarios.
use crate::{
    assertions::FrameAssertion,
    frame_store::CellMask,
    logs::LogCursor,
    predicates::PredicateSpec,
    protocol::{ActionContext, AgentAction},
    retention::{CleanupPlan, ManagedEntry},
    scenario::Scenario,
    validation::range,
};
use rmcp::schemars;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LaunchArgs {
    /// Executable path or program name. Arguments are passed as an array, never through a shell.
    pub program: Option<String>,
    /// Arguments passed directly to the executable.
    #[serde(default)]
    pub args: Vec<String>,
    /// Existing working directory for the application.
    pub cwd: Option<String>,
    /// Extra environment variables passed as a key/value object.
    #[serde(default)]
    pub env: std::collections::BTreeMap<String, String>,
    /// Optional human-readable session name.
    pub name: Option<String>,
    /// Terminal columns.
    #[serde(default = "default_cols")]
    #[schemars(range(min = 1, max = 300))]
    pub cols: u16,
    /// Terminal rows.
    #[serde(default = "default_rows")]
    #[schemars(range(min = 1, max = 120))]
    pub rows: u16,
    /// Retain a bounded raw PTY stream. May include sensitive visible output.
    #[serde(default)]
    pub record_output: bool,
    /// Configured state/profile/environment/workspace/isolation options.
    pub options: Option<crate::config::LaunchOptions>,
    pub profile: Option<String>,
    pub state_mode: Option<String>,
    pub env_inherit: Option<Vec<String>>,
    #[serde(default)]
    pub env_unset: Vec<String>,
    pub workspace: Option<crate::workspace::WorkspaceSpec>,
    pub isolation: Option<crate::isolation::IsolationSpec>,
    pub shell: Option<crate::config::ShellSpec>,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct McpSessionArgs {
    pub session_id: String,
    pub actor_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ViewStartArgs {
    pub session_id: String,
    /// Allow this browser grant to request exclusive terminal input control.
    #[serde(default)]
    pub allow_browser_control: bool,
    /// Optional stable key; retries return the same unredeemed viewing URL.
    pub idempotency_key: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ViewArgs {
    pub view_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ControlReleaseArgs {
    pub session_id: String,
    /// Generation returned by terminal_control_status.
    pub expected_generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CellsArgs {
    pub response: Option<crate::projection::ResponseOptions>,
    pub session_id: String,
    /// Zero-based top row.
    pub row: u16,
    /// Zero-based left column.
    pub col: u16,
    /// Number of rows, at most 40.
    #[schemars(range(min = 1, max = 40))]
    pub rows: u16,
    /// Number of columns, at most 120.
    #[schemars(range(min = 1, max = 120))]
    pub cols: u16,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct KeyArgs {
    pub session_id: String,
    /// Logical key token, for example Down, Enter, Escape, or Ctrl+C.
    pub key: String,
    pub context: Option<ActionContext>,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct TextArgs {
    pub session_id: String,
    pub text: String,
    pub context: Option<ActionContext>,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ResizeArgs {
    pub session_id: String,
    #[schemars(range(min = 1, max = 300))]
    pub cols: u16,
    #[schemars(range(min = 1, max = 120))]
    pub rows: u16,
    pub context: Option<ActionContext>,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct MouseArgs {
    pub session_id: String,
    /// move, click, down, up, wheel_up, or wheel_down
    #[schemars(extend("enum"=["move","click","down","up","wheel_up","wheel_down"]))]
    pub action: String,
    /// Zero-based row coordinate.
    pub row: u16,
    /// Zero-based column coordinate.
    pub col: u16,
    /// Mouse button for click/down; defaults to left.
    #[schemars(extend("enum"=["left","middle","right",null]))]
    pub button: Option<String>,
    /// Wheel steps, defaults to one.
    #[schemars(range(min = 1, max = 20))]
    pub steps: Option<u16>,
    pub context: Option<ActionContext>,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct McpWaitArgs {
    pub response: Option<crate::projection::ResponseOptions>,
    pub session_id: String,
    /// contains, absent, regex, stable, or process_exited
    #[serde(default)]
    #[schemars(extend("enum"=["","contains","absent","regex","stable","process_exited","output_quiet","ready"]))]
    pub kind: String,
    /// Text or regex pattern where applicable.
    pub value: Option<String>,
    /// Stability duration or overall timeout, in milliseconds.
    pub duration_ms: Option<u64>,
    /// Bounded overall timeout, in milliseconds.
    #[schemars(range(min = 1, max = 3600000))]
    pub timeout_ms: Option<u64>,
    pub predicate: Option<PredicateSpec>,
    pub wait_id: Option<String>,
    #[schemars(range(min = 0, max = 5000))]
    pub poll_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ObserveArgs {
    pub session_id: String,
    pub response: Option<crate::projection::ResponseOptions>,
}
fn default_cols() -> u16 {
    120
}
fn default_rows() -> u16 {
    40
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SessionArgs {
    pub session_id: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ProfileArgs {
    #[schemars(extend("enum"=["list","create","store","template"]))]
    pub action: String,
    pub name: String,
    pub description: String,
    pub profile: Option<crate::profiles::Profile>,
}
impl Default for ProfileArgs {
    fn default() -> Self {
        Self {
            action: "list".into(),
            name: String::new(),
            description: String::new(),
            profile: None,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct ArtifactArgs {
    #[schemars(required)]
    pub session_id: String,
    pub artifact_id: String,
    pub offset: u64,
    #[schemars(range(min = 0, max = 1048576))]
    pub length: usize,
    #[schemars(range(min = 1, max = 500))]
    pub limit: usize,
}
impl Default for ArtifactArgs {
    fn default() -> Self {
        Self {
            session_id: String::new(),
            artifact_id: String::new(),
            offset: 0,
            length: 65536,
            limit: 100,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct LogsArgs {
    #[schemars(required)]
    pub session_id: String,
    #[schemars(extend("enum"=["app","startup","daemon"]))]
    pub source: String,
    pub path: String,
    pub cursor: Option<LogCursor>,
    pub max_bytes: usize,
}
impl Default for LogsArgs {
    fn default() -> Self {
        Self {
            session_id: String::new(),
            source: "app".into(),
            path: "dev.log".into(),
            cursor: None,
            max_bytes: 65536,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SaveArgs {
    pub session_id: String,
    pub name: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AssertArgs {
    pub session_id: String,
    pub assertion: FrameAssertion,
}
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DiffArgs {
    pub response: Option<crate::projection::ResponseOptions>,
    pub session_id: String,
    pub name: String,
    #[serde(default)]
    pub masks: Vec<CellMask>,
}

/// Crop the current atomic observation, optionally requiring an exact identity
/// returned by `terminal_observe`.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CropArgs {
    pub session_id: String,
    /// If supplied, every identity/revision field must match the current frame.
    pub source: Option<crate::locators::ObservationIdentity>,
    /// Zero-based half-open cell region. Maximum viewport extent is 300x120.
    pub region: crate::locators::CellRect,
    #[serde(default = "default_crop_scale")]
    #[schemars(range(min = 1, max = 4))]
    pub scale: u8,
}

fn default_crop_scale() -> u8 {
    1
}

/// Compare two retained crop PNGs belonging to this session.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CropCompareArgs {
    pub session_id: String,
    pub left_artifact_id: String,
    pub right_artifact_id: String,
    /// Explicit pixel acceptance threshold; different pixels are those whose
    /// maximum RGBA channel delta exceeds `max_channel_delta`.
    pub tolerance: crate::crop_evidence::PixelTolerance,
}
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct WaitArgs {
    pub response: Option<crate::projection::ResponseOptions>,
    pub session_id: String,
    pub predicate: Option<PredicateSpec>,
    pub wait_id: Option<String>,
    #[schemars(range(min = 1, max = 3600000))]
    pub timeout_ms: u64,
    #[schemars(range(min = 0, max = 5000))]
    pub poll_ms: u64,
    pub trace_stage: Option<String>,
    pub cancel: bool,
}
impl Default for WaitArgs {
    fn default() -> Self {
        Self {
            session_id: String::new(),
            predicate: None,
            wait_id: None,
            timeout_ms: 30000,
            poll_ms: 200,
            trace_stage: None,
            cancel: false,
            response: None,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LocatorArgs {
    pub session_id: String,
    pub query: crate::locators::LocatorQuery,
    /// Zero-based half-open cell rectangle; omitted searches the current viewport.
    pub region: Option<crate::locators::CellRect>,
    pub response: Option<crate::projection::ResponseOptions>,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(extend("anyOf"=[
    {"required":["wait_id"]},
    {"required":["session_id","prior_observation","spec","timeout_ms"]}
]))]
pub struct FreshWaitArgs {
    /// Required with prior_observation, spec and timeout_ms when creating a wait.
    pub session_id: Option<String>,
    /// Stable handle returned by the first poll. Reuse the same handle to resume; may be supplied
    /// on creation as an idempotency key.
    pub wait_id: Option<String>,
    pub spec: Option<crate::wait_engine::FreshWaitSpec>,
    /// Identity returned by terminal_observe or a prior action receipt.
    pub prior_observation: Option<crate::locators::ObservationIdentity>,
    #[schemars(range(min = 1, max = 3600000))]
    pub timeout_ms: Option<u64>,
    #[serde(default = "default_fresh_poll_ms")]
    #[schemars(range(min = 0, max = 5000))]
    pub poll_ms: u64,
    #[serde(default)]
    pub cancel: bool,
    pub response: Option<crate::projection::ResponseOptions>,
}

fn default_fresh_poll_ms() -> u64 {
    200
}
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
#[schemars(extend("examples"=[{"session_id":"session-id","context":{"actor_id":"agent","action_id":"palette-open","expected_run_id":"run-id"},"actions":[{"kind":"key","key":"Ctrl+P"},{"kind":"type","text":"panel"}]}]))]
pub struct BatchArgs {
    pub session_id: String,
    pub context: ActionContext,
    #[schemars(length(min = 1, max = 64))]
    pub actions: Vec<AgentAction>,
}
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SequenceArgs {
    /// start executes the validated sequence; status/cancel require sequence_id.
    #[schemars(extend("enum"=["start","status","resume","cancel"]))]
    pub action: String,
    #[serde(default)]
    pub request: Option<crate::sequences::SequenceRequest>,
    pub sequence_id: Option<String>,
    pub actor_id: Option<String>,
    /// Contract-v2 projection. Summary is default; full explicitly opts into the report body.
    pub response: Option<crate::projection::ResponseOptions>,
}

/// Bounded, application-widget navigation. `bindings` are explicit actions
/// already verified against `verified_at`; this is not terminal scrollback.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScrollUntilArgs {
    pub session_id: String,
    pub context: crate::protocol::ActionContext,
    #[schemars(extend("enum"=["up", "down"]))]
    pub direction: String,
    pub region: crate::locators::CellRect,
    pub target: crate::wait_engine::FreshWaitSpec,
    pub verified_at: crate::locators::ObservationIdentity,
    #[schemars(length(min = 1, max = 8))]
    pub bindings: Vec<crate::protocol::AgentAction>,
    #[schemars(range(min = 1, max = 256))]
    pub max_steps: usize,
    #[serde(default = "default_scroll_no_progress")]
    #[schemars(range(min = 1, max = 32))]
    pub max_no_progress: usize,
    #[schemars(range(min = 1, max = 60000))]
    pub timeout_ms: u64,
}

fn default_scroll_no_progress() -> usize {
    2
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AppAdapterArgs {
    /// bind, messages, transcript, session, status, events, ui_state, message_baseline, or semantic_wait.
    #[schemars(extend("enum"=["bind", "messages", "transcript", "session", "status", "events", "ui_state", "message_baseline", "semantic_wait"]))]
    pub action: String,
    pub adapter_id: Option<String>,
    /// Bind only daemon-owned backend/TUI sessions; neither value is a URL.
    pub backend_session_id: Option<String>,
    pub tui_session_id: Option<String>,
    /// Explicit application-session allowlist established at bind time.
    #[serde(default)]
    #[schemars(length(max = 64))]
    pub allowed_application_sessions: Vec<String>,
    pub application_session_id: Option<String>,
    /// Optional expected producer identity for action=ui_state. Supplying it opts into the fixed producer route; it is never a URL or authority.
    pub producer_instance: Option<String>,
    /// Opaque handle returned by action=message_baseline for this adapter generation.
    pub baseline_handle: Option<String>,
    /// Application evidence predicate for action=semantic_wait.
    pub predicate: Option<SemanticPredicateArgs>,
    #[schemars(range(min = 1, max = 60000))]
    pub timeout_ms: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SemanticPredicateArgs {
    TurnCompleted {
        user_message_id: String,
    },
    ToolCompleted {
        user_message_id: String,
        call_id: String,
        tool: String,
    },
}

/// Arm, inspect, cancel, or materialize a bounded transient-UI capture job.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CaptureJobArgs {
    #[schemars(extend("enum"=["start", "status", "cancel", "materialize", "release"]))]
    pub action: String,
    /// Required for start. Capture is armed before the call returns.
    pub request: Option<crate::capture_jobs::CaptureRequest>,
    /// Required for status/cancel/materialize.
    pub handle: Option<String>,
    /// Required only for materialize; index from the status frame list.
    #[schemars(range(min = 0, max = 127))]
    pub frame_index: Option<u32>,
    /// Summary is compact by default; full includes bounded frame references.
    pub response: Option<crate::projection::ResponseOptions>,
}

/// Start and control a bounded diagnostic process owned by one exact session run.
/// Process context (cwd, environment and containment) is resolved by the daemon.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct DiagnosticsArgs {
    pub action: DiagnosticAction,
    pub session_id: String,
    pub run_id: String,
    /// Executable passed directly for `start`; no shell is inserted by the runtime.
    pub program: Option<String>,
    /// Direct argv for `start`; never interpreted by a shell.
    #[serde(default)]
    #[schemars(length(max = 127))]
    pub args: Vec<String>,
    pub label: Option<String>,
    pub process_id: Option<String>,
    pub stream: Option<DiagnosticStream>,
    #[serde(default)]
    pub cursor: usize,
    #[serde(default = "default_diagnostic_read_bytes")]
    #[schemars(range(min = 1, max = 65536))]
    pub max_bytes: usize,
    /// Relative to the selected copied/reflink workspace for `seed_file`.
    pub relative_path: Option<PathBuf>,
    /// UTF-8 file content for `seed_file`, limited to 1 MiB.
    #[schemars(length(max = 1048576))]
    pub content: Option<String>,
    /// Required to replace an existing file; hash of its current bytes.
    pub expected_sha256: Option<String>,
}

/// Start, stop, inspect or export an explicitly opted-in logical action trace.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RecordingArgs {
    #[schemars(extend("enum"=["start","status","stop","export"]))]
    pub action: String,
    pub session_id: String,
    pub run_id: String,
    #[schemars(length(min = 1, max = 64))]
    pub recording_id: String,
    #[serde(default)]
    pub opted_in: bool,
    #[serde(default)]
    pub record_text: bool,
    #[serde(default = "default_recording_events")]
    #[schemars(range(min = 1, max = 512))]
    pub max_events: usize,
    #[serde(default = "default_recording_bytes")]
    #[schemars(range(min = 1024, max = 2097152))]
    pub max_bytes: usize,
}

fn default_recording_events() -> usize {
    256
}
fn default_recording_bytes() -> usize {
    512 * 1024
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticAction {
    Start,
    Status,
    Read,
    Cancel,
    Stop,
    SeedFile,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum DiagnosticStream {
    Stdout,
    Stderr,
}

fn default_diagnostic_read_bytes() -> usize {
    16 * 1024
}
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ActorArgs {
    pub session_id: String,
    pub actor_id: String,
    #[serde(default)]
    pub release: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScenarioArgs {
    pub scenario: Scenario,
    #[serde(default)]
    pub workspace: Option<PathBuf>,
}
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct JobArgs {
    pub job_id: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, Default, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct CleanupArgs {
    pub apply: bool,
    pub plan: Option<CleanupPlan>,
    pub include_developer_cache: bool,
    pub policy: Option<crate::retention::RetentionPolicy>,
    #[serde(default)]
    pub register: Vec<ManagedEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScenarioLaunchArgs {
    pub spec: crate::protocol::LaunchSpec,
    pub options: Option<crate::config::LaunchOptions>,
}
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScenarioKeyArgs {
    pub session_id: String,
    pub key: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScenarioTextArgs {
    pub session_id: String,
    pub text: String,
}
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScenarioMouseArgs {
    pub session_id: String,
    pub input: crate::protocol::MouseInput,
}
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ScenarioResizeArgs {
    pub session_id: String,
    #[schemars(range(min = 1, max = 300))]
    pub cols: u16,
    #[schemars(range(min = 1, max = 120))]
    pub rows: u16,
}

pub fn scenario_params_schema(
    operation: &str,
    generator: &mut schemars::SchemaGenerator,
) -> serde_json::Value {
    use serde_json::json;
    match operation {
        "launch" => {
            let spec = generator.subschema_for::<crate::protocol::LaunchSpec>();
            let options = generator.subschema_for::<crate::config::LaunchOptions>();
            json!({"anyOf":[generator.subschema_for::<ScenarioLaunchArgs>(),{"allOf":[spec],"properties":{"options":options}}]})
        }
        "observe" => json!(generator.subschema_for::<ObserveArgs>()),
        "cells" => json!(generator.subschema_for::<CellsArgs>()),
        "locate" => json!(generator.subschema_for::<LocatorArgs>()),
        "key" => json!(generator.subschema_for::<ScenarioKeyArgs>()),
        "type" | "paste" => json!(generator.subschema_for::<ScenarioTextArgs>()),
        "mouse" => json!(generator.subschema_for::<ScenarioMouseArgs>()),
        "resize" => json!(generator.subschema_for::<ScenarioResizeArgs>()),
        "wait" => json!(generator.subschema_for::<WaitArgs>()),
        "wait_fresh" => json!(generator.subschema_for::<FreshWaitArgs>()),
        "sequence" => json!(generator.subschema_for::<SequenceArgs>()),
        "scroll_until" => json!(generator.subschema_for::<ScrollUntilArgs>()),
        "app_adapter" => json!(generator.subschema_for::<AppAdapterArgs>()),
        "capture_job" => json!(generator.subschema_for::<CaptureJobArgs>()),
        "logs" => json!(generator.subschema_for::<LogsArgs>()),
        "save_frame" => json!(generator.subschema_for::<SaveArgs>()),
        "assert" => json!(generator.subschema_for::<AssertArgs>()),
        "diff" => json!(generator.subschema_for::<DiffArgs>()),
        "finalize" => json!(generator.subschema_for::<crate::finalization::FinalizeArgs>()),
        _ => json!(generator.subschema_for::<SessionArgs>()),
    }
}

pub fn scenario_step_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
    use serde_json::json;
    let preconditions = generator.subschema_for::<crate::scenario::ScenarioPreconditions>();
    let expect = generator.subschema_for::<crate::scenario::ScenarioExpectation>();
    let variants:Vec<_>=crate::scenario::SUPPORTED_OPERATIONS.iter().map(|operation|json!({
        "type":"object","additionalProperties":false,"required":["id","operation","params"],
        "properties":{
            "id":{"type":"string","minLength":1},"operation":{"const":operation},"params":scenario_params_schema(operation,generator),
            "preconditions":preconditions,"timeout_ms":{"type":"integer","minimum":1,"maximum":600000,"default":30000},"expect":expect
        }
    })).collect();
    json!({"oneOf":variants})
        .try_into()
        .expect("valid scenario step schema")
}

/// Validate every concrete step before starting a runtime-backed scenario.
/// Variable interpolation changes values, never the operation's field types.
pub fn validate_scenario(scenario: &Scenario) -> anyhow::Result<()> {
    for (index, step) in scenario.steps.iter().enumerate() {
        validate_scenario_params(
            step.operation.trim_start_matches("terminal_"),
            step.params.clone(),
        )
        .map_err(|error| error.prefix(&format!("scenario.steps[{index}].params")))?;
    }
    Ok(())
}

pub fn validate_scenario_params(
    operation: &str,
    mut params: serde_json::Value,
) -> Result<(), crate::validation::RequestError> {
    use crate::validation::{RequestError, decode};
    if let Some(object) = params.as_object_mut() {
        object.remove("_preconditions");
    }
    let response = params
        .as_object_mut()
        .and_then(|object| object.remove("response"))
        .filter(|value| !value.is_null());
    if let Some(response) = response {
        if ![
            "observe",
            "cells",
            "locate",
            "wait",
            "wait_fresh",
            "diff",
            "capture_job",
        ]
        .contains(&operation)
        {
            return Err(RequestError::new(
                "response",
                "projection-supported observation operation",
            )
            .code("unsupported_response"));
        }
        let options: crate::projection::ResponseOptions =
            decode(response).map_err(|error| error.prefix("response"))?;
        if operation == "diff" {
            crate::projection::validate_named_diff(&options).map_err(as_request_error)?;
        }
        let mode = options
            .validate(
                if ["wait", "wait_fresh", "locate", "diff", "capture_job"].contains(&operation) {
                    crate::projection::ResponseMode::Summary
                } else {
                    crate::projection::ResponseMode::Text
                },
            )
            .map_err(as_request_error)?;
        if operation == "cells" && mode == crate::projection::ResponseMode::Diff {
            return Err(RequestError::new(
                "response.mode",
                "observation diff, not cell-region diff",
            )
            .code("unsupported_response"));
        }
    }
    macro_rules! check {
        ($kind:ty) => {{
            let _: $kind = decode(params)?;
        }};
    }
    match operation {
        "launch" => {
            let spec = params
                .get("spec")
                .cloned()
                .unwrap_or_else(|| params.clone());
            let spec: crate::protocol::LaunchSpec = decode(spec)?;
            crate::validation::request(&crate::protocol::RpcRequest::Launch { spec })
                .map_err(as_request_error)?;
            if params.get("spec").is_some() {
                check!(ScenarioLaunchArgs);
            } else if let Some(options) = params.get("options") {
                let _: crate::config::LaunchOptions =
                    decode(options.clone()).map_err(|e| e.prefix("options"))?;
            }
        }
        "key" | "type" | "paste" | "mouse" | "resize" => {
            let mut action = params.clone();
            action
                .as_object_mut()
                .ok_or_else(|| RequestError::new("params", "object"))?
                .remove("session_id");
            action["kind"] = serde_json::json!(operation);
            crate::validation::action(&action, "")?;
            match operation {
                "key" => check!(ScenarioKeyArgs),
                "type" | "paste" => check!(ScenarioTextArgs),
                "mouse" => check!(ScenarioMouseArgs),
                _ => check!(ScenarioResizeArgs),
            }
        }
        "observe" => check!(ObserveArgs),
        "locate" => {
            crate::validation::operation("locate", &params).map_err(as_request_error)?;
            check!(LocatorArgs);
        }
        "cells" => {
            let args: CellsArgs = decode(params)?;
            crate::validation::request(&crate::protocol::RpcRequest::Cells {
                session_id: args.session_id,
                row: args.row,
                col: args.col,
                rows: args.rows,
                cols: args.cols,
            })
            .map_err(as_request_error)?;
        }
        "wait" => {
            crate::validation::operation("wait", &params).map_err(as_request_error)?;
            check!(WaitArgs);
        }
        "wait_fresh" => {
            crate::validation::operation("wait_fresh", &params).map_err(as_request_error)?;
            check!(FreshWaitArgs);
        }
        "sequence" => {
            let args: SequenceArgs = decode(params)?;
            if let Some(response) = args.response {
                let mode = response
                    .validate(crate::projection::ResponseMode::Summary)
                    .map_err(as_request_error)?;
                if !matches!(
                    mode,
                    crate::projection::ResponseMode::Summary
                        | crate::projection::ResponseMode::Full
                ) {
                    return Err(RequestError::new(
                        "response.mode",
                        "summary or full for sequence reports",
                    )
                    .code("unsupported_response"));
                }
            }
            if args.action == "start" {
                let request = args
                    .request
                    .ok_or_else(|| RequestError::new("request", "required when action=start"))?;
                crate::sequences::validate_request(&request)
                    .map_err(|_| RequestError::new("request", "valid bounded sequence request"))?;
                if args.actor_id.as_deref() != Some(request.identity.actor_id.as_str()) {
                    return Err(RequestError::new(
                        "actor_id",
                        "must match request.identity.actor_id",
                    ));
                }
            } else if !["status", "resume", "cancel"].contains(&args.action.as_str()) {
                return Err(RequestError::new(
                    "action",
                    "start, status, resume, or cancel",
                ));
            }
            if args.action != "start" && args.sequence_id.as_deref().is_none_or(str::is_empty) {
                return Err(RequestError::new(
                    "sequence_id",
                    "required for status/resume/cancel",
                ));
            }
            if args.action != "start" && args.actor_id.as_deref().is_none_or(str::is_empty) {
                return Err(RequestError::new(
                    "actor_id",
                    "required for status/resume/cancel",
                ));
            }
        }
        "scroll_until" => {
            let args: ScrollUntilArgs = decode(params)?;
            if !matches!(args.direction.as_str(), "up" | "down") {
                return Err(RequestError::new("direction", "up or down"));
            }
            crate::validation::range("max_steps", args.max_steps as u64, 1, 256)?;
            crate::validation::range("max_no_progress", args.max_no_progress as u64, 1, 32)?;
            crate::validation::range("timeout_ms", args.timeout_ms, 1, 60_000)?;
            if args.region.row_start >= args.region.row_end
                || args.region.col_start >= args.region.col_end
            {
                return Err(RequestError::new(
                    "region",
                    "non-empty half-open cell rectangle",
                ));
            }
            if args.bindings.is_empty() || args.bindings.len() > 8 {
                return Err(RequestError::new(
                    "bindings",
                    "1..=8 verified key/wheel actions",
                ));
            }
            for (index, action) in args.bindings.iter().enumerate() {
                let value = serde_json::to_value(action).map_err(|_| {
                    RequestError::new(format!("bindings[{index}]"), "valid terminal action")
                })?;
                crate::validation::action(&value, &format!("bindings[{index}]"))?;
            }
        }
        "app_adapter" => {
            let args: AppAdapterArgs = decode(params)?;
            if ![
                "bind",
                "messages",
                "transcript",
                "session",
                "status",
                "events",
                "ui_state",
                "message_baseline",
                "semantic_wait",
            ]
            .contains(&args.action.as_str())
            {
                return Err(RequestError::new(
                    "action",
                    "bind, messages, transcript, session, status, events, ui_state, message_baseline, or semantic_wait",
                ));
            }
            match args.action.as_str() {
                "bind" => {
                    if args.adapter_id.is_some() {
                        return Err(RequestError::new("adapter_id", "omit for action=bind"));
                    }
                }
                "messages" | "transcript" | "session" | "status" | "message_baseline" => {
                    if args
                        .application_session_id
                        .as_deref()
                        .is_none_or(str::is_empty)
                    {
                        return Err(RequestError::new(
                            "application_session_id",
                            "required for this adapter action",
                        ));
                    }
                }
                "semantic_wait" => {
                    if args.baseline_handle.as_deref().is_none_or(str::is_empty) {
                        return Err(RequestError::new(
                            "baseline_handle",
                            "required opaque handle from message_baseline",
                        ));
                    }
                    if args.predicate.is_none() {
                        return Err(RequestError::new(
                            "predicate",
                            "required typed turn_completed or tool_completed predicate",
                        ));
                    }
                }
                _ => {}
            }
            if args.producer_instance.is_some() && args.action != "ui_state" {
                return Err(RequestError::new(
                    "producer_instance",
                    "allowed only with action=ui_state",
                ));
            }
            if let Some(timeout) = args.timeout_ms {
                range("timeout_ms", timeout, 1, 60_000)?;
            }
        }
        "capture_job" => {
            let args: CaptureJobArgs = decode(params)?;
            match args.action.as_str() {
                "start" => {
                    let request = args.request.as_ref().ok_or_else(|| {
                        RequestError::new("request", "required when action=start")
                    })?;
                    crate::capture_jobs::validate_capture_request(request).map_err(|_| {
                        RequestError::new("request", "valid bounded capture request")
                    })?;
                    if args.handle.is_some() || args.frame_index.is_some() {
                        return Err(RequestError::new(
                            "handle/frame_index",
                            "omit for action=start",
                        ));
                    }
                }
                "status" | "cancel" | "release" => {
                    if args.handle.as_deref().is_none_or(str::is_empty) {
                        return Err(RequestError::new(
                            "handle",
                            "required for status/cancel/release",
                        ));
                    }
                    if args.request.is_some() || args.frame_index.is_some() {
                        return Err(RequestError::new(
                            "request/frame_index",
                            "omit for status/cancel/release",
                        ));
                    }
                }
                "materialize" => {
                    if args.handle.as_deref().is_none_or(str::is_empty) {
                        return Err(RequestError::new("handle", "required for materialize"));
                    }
                    if args.frame_index.is_none() {
                        return Err(RequestError::new("frame_index", "required for materialize"));
                    }
                    if args.request.is_some() {
                        return Err(RequestError::new("request", "omit for materialize"));
                    }
                }
                _ => {
                    return Err(RequestError::new(
                        "action",
                        "start, status, cancel, materialize, or release",
                    ));
                }
            }
            if let Some(response) = args.response {
                let mode = response
                    .validate(crate::projection::ResponseMode::Summary)
                    .map_err(as_request_error)?;
                if !matches!(
                    mode,
                    crate::projection::ResponseMode::Summary
                        | crate::projection::ResponseMode::Full
                ) {
                    return Err(RequestError::new(
                        "response.mode",
                        "summary or full for capture jobs",
                    )
                    .code("unsupported_response"));
                }
            }
        }
        "logs" => check!(LogsArgs),
        "save_frame" => check!(SaveArgs),
        "assert" => check!(AssertArgs),
        "diff" => check!(DiffArgs),
        "finalize" => check!(crate::finalization::FinalizeArgs),
        _ => check!(SessionArgs),
    }
    Ok(())
}
fn as_request_error(error: anyhow::Error) -> crate::validation::RequestError {
    error
        .downcast::<crate::validation::RequestError>()
        .unwrap_or_else(|_| {
            crate::validation::RequestError::new("params", "published operation contract")
        })
}
