//! Safe, field-addressed errors shared by daemon, MCP and scenarios.
use crate::protocol::{ActionContext, RpcRequest};
use anyhow::Result;
use serde::{
    Deserialize, Serialize,
    de::{DeserializeOwned, IntoDeserializer},
};
use serde_json::{Value, json};

pub const LEGACY_WAIT_MAX_MS: u64 = 60_000;
pub const RESUMABLE_WAIT_MAX_MS: u64 = 3_600_000;
pub const MAX_WHEEL_STEPS: u64 = 20;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestError {
    pub code: String,
    pub field: String,
    pub expected: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub minimum: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub maximum: Option<u64>,
}
impl RequestError {
    pub fn new(field: impl Into<String>, expected: impl Into<String>) -> Self {
        Self {
            code: "invalid_params".into(),
            field: field.into(),
            expected: expected.into(),
            minimum: None,
            maximum: None,
        }
    }
    pub fn code(mut self, code: &str) -> Self {
        self.code = code.into();
        self
    }
    pub fn prefix(mut self, prefix: &str) -> Self {
        self.field = if self.field.is_empty() || self.field == "." {
            prefix.into()
        } else {
            format!("{prefix}.{}", self.field)
        };
        self
    }
}
impl std::fmt::Display for RequestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {}: expected {}",
            self.code, self.field, self.expected
        )
    }
}
impl std::error::Error for RequestError {}

pub fn decode<T: DeserializeOwned>(value: Value) -> std::result::Result<T, RequestError> {
    serde_path_to_error::deserialize(value.clone().into_deserializer()).map_err(|error| {
        let mut field = error.path().to_string();
        if field == "." {
            field.clear();
        }
        let message = error.inner().to_string();
        let expected = if let Some(name) = message
            .strip_prefix("missing field `")
            .and_then(|s| s.split('`').next())
        {
            field = if field.is_empty() {
                name.into()
            } else {
                format!("{field}.{name}")
            };
            "required field".to_owned()
        } else if let Some(name) = message
            .strip_prefix("unknown field `")
            .and_then(|s| s.split('`').next())
        {
            let safe: String = name
                .chars()
                .filter(|c| c.is_ascii_alphanumeric() || *c == '_')
                .take(64)
                .collect();
            field = if field.is_empty() {
                safe
            } else {
                format!("{field}.{safe}")
            };
            "published request field (unknown fields are rejected)".into()
        } else if let Some((_, expected)) = message.split_once(", expected ") {
            expected.to_owned()
        } else {
            "value matching the published request schema".to_owned()
        };
        // Never forward Serde's actual value / unknown variant text: it may be a credential.
        RequestError::new(field, expected)
    })
}
pub fn range(field: &str, value: u64, min: u64, max: u64) -> std::result::Result<(), RequestError> {
    if value < min || value > max {
        return Err(RequestError {
            minimum: Some(min),
            maximum: Some(max),
            ..RequestError::new(field, format!("integer in {min}..={max}"))
        });
    }
    Ok(())
}
fn required<'a>(
    value: &'a Value,
    key: &str,
    prefix: &str,
) -> std::result::Result<&'a Value, RequestError> {
    value
        .get(key)
        .ok_or_else(|| RequestError::new(join(prefix, key), "required field"))
}
fn join(prefix: &str, field: &str) -> String {
    if prefix.is_empty() {
        field.into()
    } else {
        format!("{prefix}.{field}")
    }
}
fn number(
    value: &Value,
    key: &str,
    prefix: &str,
    min: u64,
    max: u64,
) -> std::result::Result<(), RequestError> {
    let field = join(prefix, key);
    let number: u64 =
        decode(required(value, key, prefix)?.clone()).map_err(|e| e.prefix(&field))?;
    range(&field, number, min, max)
}
fn string(value: &Value, key: &str, prefix: &str) -> std::result::Result<(), RequestError> {
    let field = join(prefix, key);
    let _: String = decode(required(value, key, prefix)?.clone()).map_err(|e| e.prefix(&field))?;
    Ok(())
}
pub fn action(value: &Value, prefix: &str) -> std::result::Result<(), RequestError> {
    let tag = value.get("kind").and_then(Value::as_str).unwrap_or("");
    match tag {
        "key" => {
            string(value, "key", prefix)?;
            if tui_test::input::keys::token_to_seq(value["key"].as_str().unwrap()).is_err() {
                return Err(RequestError::new(
                    join(prefix, "key"),
                    "supported logical key token (e.g. Enter, Down, Ctrl+C)",
                ));
            }
        }
        "type" | "paste" => {
            string(value, "text", prefix)?;
            range(
                &join(prefix, "text"),
                value["text"].as_str().unwrap().len() as u64,
                0,
                1048576,
            )?;
        }
        "resize" => {
            number(value, "cols", prefix, 1, 300)?;
            number(value, "rows", prefix, 1, 120)?;
        }
        "mouse" => mouse(required(value, "input", prefix)?, &join(prefix, "input"))?,
        _ => {
            return Err(RequestError::new(
                join(prefix, "kind"),
                "key, type, paste, mouse, or resize",
            ));
        }
    }
    Ok(())
}
pub fn mouse(value: &Value, prefix: &str) -> std::result::Result<(), RequestError> {
    let tag = value.get("kind").and_then(Value::as_str).unwrap_or("");
    if !["move", "click", "down", "up", "wheel"].contains(&tag) {
        return Err(RequestError::new(
            join(prefix, "kind"),
            "move, click, down, up, or wheel",
        ));
    }
    number(value, "row", prefix, 0, 119)?;
    number(value, "col", prefix, 0, 299)?;
    if ["click", "down"].contains(&tag)
        && ![Some("left"), Some("middle"), Some("right")].contains(&value["button"].as_str())
    {
        return Err(RequestError::new(
            join(prefix, "button"),
            "left, middle, or right",
        ));
    }
    if tag == "wheel" {
        number(value, "steps", prefix, 1, MAX_WHEEL_STEPS)?;
        if ![Some("up"), Some("down")].contains(&value["direction"].as_str()) {
            return Err(RequestError::new(join(prefix, "direction"), "up or down"));
        }
    }
    Ok(())
}
pub fn predicate(value: &Value, prefix: &str) -> std::result::Result<(), RequestError> {
    match value.get("kind").and_then(Value::as_str).unwrap_or("") {
        "text_contains" | "text_absent" => string(value, "text", prefix)?,
        "regex" => {
            string(value, "pattern", prefix)?;
            if regex::Regex::new(value["pattern"].as_str().unwrap()).is_err() {
                return Err(RequestError::new(
                    join(prefix, "pattern"),
                    "valid regular expression",
                ));
            }
        }
        "output_quiet" | "stable_text" => number(value, "duration_ms", prefix, 0, u64::MAX)?,
        "stable_region" => {
            region(value, prefix)?;
            number(value, "duration_ms", prefix, 0, u64::MAX)?;
        }
        "ready" => {
            if let Some(region_value) = value.get("stable_region").filter(|v| !v.is_null()) {
                region(region_value, &join(prefix, "stable_region"))?;
                number(
                    region_value,
                    "duration_ms",
                    &join(prefix, "stable_region"),
                    0,
                    u64::MAX,
                )?;
            }
        }
        "process_exited" => {}
        _ => {
            return Err(RequestError::new(
                join(prefix, "kind"),
                "text_contains, text_absent, regex, output_quiet, stable_text, stable_region, ready, or process_exited",
            ));
        }
    }
    Ok(())
}
fn region(value: &Value, prefix: &str) -> std::result::Result<(), RequestError> {
    number(value, "row", prefix, 0, 119)?;
    number(value, "col", prefix, 0, 299)?;
    number(value, "rows", prefix, 1, 120)?;
    number(value, "cols", prefix, 1, 300)
}
pub fn operation(operation: &str, value: &Value) -> Result<()> {
    match operation.trim_start_matches("terminal_") {
        "recording" => {
            let args: crate::contracts::RecordingArgs = decode(value.clone())?;
            if !["start", "status", "stop", "export"].contains(&args.action.as_str()) {
                return Err(RequestError::new("action", "start, status, stop or export").into());
            }
            string(value, "session_id", "")?;
            string(value, "run_id", "")?;
            string(value, "recording_id", "")?;
            range(
                "max_events",
                args.max_events as u64,
                1,
                crate::manual_recording::MAX_RECORDING_EVENTS as u64,
            )?;
            range(
                "max_bytes",
                args.max_bytes as u64,
                1024,
                crate::manual_recording::MAX_RECORDING_BYTES as u64,
            )?;
            if args.action == "start" && !args.opted_in {
                return Err(RequestError::new(
                    "opted_in",
                    "true to begin retaining a manual trace",
                )
                .into());
            }
        }
        "crop" => {
            let args: crate::contracts::CropArgs = decode(value.clone())?;
            string(value, "session_id", "")?;
            range("scale", u64::from(args.scale), 1, 4)?;
            if args.region.row_start >= args.region.row_end
                || args.region.col_start >= args.region.col_end
                || args.region.row_end > 120
                || args.region.col_end > 300
            {
                return Err(RequestError::new(
                    "region",
                    "non-empty zero-based half-open rectangle within 300x120 viewport bounds",
                )
                .into());
            }
            if let Some(source) = args.source {
                for (field, identity) in [
                    ("source.session_id", source.session_id.as_str()),
                    ("source.run_id", source.run_id.as_str()),
                    ("source.frame_id", source.frame_id.as_str()),
                ] {
                    if identity.is_empty() || identity.len() > 128 {
                        return Err(
                            RequestError::new(field, "non-empty identity up to 128 bytes").into(),
                        );
                    }
                }
            }
        }
        "crop_compare" => {
            let args: crate::contracts::CropCompareArgs = decode(value.clone())?;
            string(value, "session_id", "")?;
            for (field, identity) in [
                ("left_artifact_id", args.left_artifact_id.as_str()),
                ("right_artifact_id", args.right_artifact_id.as_str()),
            ] {
                if identity.is_empty() || identity.len() > 128 {
                    return Err(
                        RequestError::new(field, "non-empty artifact id up to 128 bytes").into(),
                    );
                }
            }
            range(
                "tolerance.max_different_pixels",
                args.tolerance.max_different_pixels,
                0,
                crate::crop_evidence::MAX_CROP_PIXELS,
            )?;
        }
        "locate" => {
            let args: crate::contracts::LocatorArgs = decode(value.clone())?;
            crate::locators::compile_query(&args.query)
                .map_err(|_| RequestError::new("query", "non-empty text or valid bounded regex"))?;
            if let Some(region) = args.region
                && (region.row_start >= region.row_end
                    || region.col_start >= region.col_end
                    || region.row_end > 120
                    || region.col_end > 300)
            {
                return Err(RequestError::new(
                    "region",
                    "non-empty zero-based half-open rectangle within 300x120 viewport bounds",
                )
                .into());
            }
            if let Some(response) = args.response {
                let mode = response.validate(crate::projection::ResponseMode::Summary)?;
                if mode == crate::projection::ResponseMode::Diff {
                    return Err(RequestError::new(
                        "response.mode",
                        "summary, text, or full for locator results",
                    )
                    .code("unsupported_response")
                    .into());
                }
                if response.styles == Some(true) {
                    return Err(RequestError::new(
                        "response.styles",
                        "styles are not returned by text locators",
                    )
                    .code("unsupported_response")
                    .into());
                }
            }
        }
        "wait_fresh" => {
            let args: crate::contracts::FreshWaitArgs = decode(value.clone())?;
            if let Some(timeout_ms) = args.timeout_ms {
                range("timeout_ms", timeout_ms, 1, RESUMABLE_WAIT_MAX_MS)?;
            }
            range("poll_ms", args.poll_ms, 0, 5000)?;
            if args.wait_id.is_none()
                && (args.session_id.is_none() || args.spec.is_none() || args.timeout_ms.is_none())
            {
                return Err(RequestError::new(
                    "session_id/spec/timeout_ms",
                    "required to create a fresh wait",
                )
                .into());
            }
            if let Some(spec) = &args.spec {
                crate::locators::compile_query(&spec.query).map_err(|_| {
                    RequestError::new("spec.query", "non-empty text or valid bounded regex")
                })?;
            }
            if let Some(region) = args.spec.as_ref().and_then(|spec| spec.region)
                && (region.row_start >= region.row_end
                    || region.col_start >= region.col_end
                    || region.row_end > 120
                    || region.col_end > 300)
            {
                return Err(RequestError::new(
                    "spec.region",
                    "non-empty zero-based half-open rectangle within 300x120 viewport bounds",
                )
                .into());
            }
            if let Some(response) = args.response {
                let mode = response.validate(crate::projection::ResponseMode::Summary)?;
                if mode == crate::projection::ResponseMode::Diff {
                    return Err(RequestError::new(
                        "response.mode",
                        "summary, text, or full for fresh waits",
                    )
                    .code("unsupported_response")
                    .into());
                }
                if response.styles == Some(true) {
                    return Err(RequestError::new(
                        "response.styles",
                        "fresh-wait results do not return styles",
                    )
                    .code("unsupported_response")
                    .into());
                }
            }
        }
        "scroll_until" => {
            let args: crate::contracts::ScrollUntilArgs = decode(value.clone())?;
            if !matches!(args.direction.as_str(), "up" | "down") {
                return Err(RequestError::new("direction", "up or down").into());
            }
            range("max_steps", args.max_steps as u64, 1, 256)?;
            range("max_no_progress", args.max_no_progress as u64, 1, 32)?;
            range("timeout_ms", args.timeout_ms, 1, 60_000)?;
            if args.region.row_start >= args.region.row_end
                || args.region.col_start >= args.region.col_end
                || args.region.row_end > 120
                || args.region.col_end > 300
            {
                return Err(RequestError::new(
                    "region",
                    "non-empty half-open rectangle inside viewport bounds",
                )
                .into());
            }
            if args.bindings.is_empty() || args.bindings.len() > 8 {
                return Err(
                    RequestError::new("bindings", "1..=8 verified key/wheel actions").into(),
                );
            }
            for (index, action_value) in args.bindings.iter().enumerate() {
                let encoded = serde_json::to_value(action_value)
                    .map_err(|_| RequestError::new(format!("bindings[{index}]"), "valid action"))?;
                action(&encoded, &format!("bindings[{index}]"))?;
            }
            crate::locators::compile_query(&args.target.query).map_err(|_| {
                RequestError::new("target.query", "non-empty text or valid bounded regex")
            })?;
        }
        "capture_job" => {
            crate::contracts::validate_scenario_params("capture_job", value.clone())
                .map_err(anyhow::Error::from)?;
        }
        "diagnostics" => {
            use crate::contracts::{DiagnosticAction, DiagnosticsArgs};
            let args: DiagnosticsArgs = decode(value.clone())?;
            if args.session_id.is_empty() {
                return Err(RequestError::new("session_id", "non-empty live session id").into());
            }
            if args.run_id.is_empty() {
                return Err(RequestError::new("run_id", "non-empty current run id").into());
            }
            match args.action {
                DiagnosticAction::Start => {
                    let program = args.program.as_deref().unwrap_or("");
                    if program.is_empty() || program.contains('\0') {
                        return Err(RequestError::new(
                            "program",
                            "required non-empty executable without NUL",
                        )
                        .into());
                    }
                    if args.args.iter().any(|arg| arg.contains('\0')) {
                        return Err(RequestError::new("args", "argv strings without NUL").into());
                    }
                    range("args", args.args.len() as u64, 0, 127)?;
                    let bytes = program
                        .len()
                        .saturating_add(args.args.iter().map(String::len).sum::<usize>());
                    range("args", bytes as u64, 1, 32 * 1024)?;
                }
                DiagnosticAction::Status | DiagnosticAction::Cancel | DiagnosticAction::Stop => {
                    if args.process_id.as_deref().is_none_or(str::is_empty) {
                        return Err(RequestError::new(
                            "process_id",
                            "required for status/cancel/stop",
                        )
                        .into());
                    }
                }
                DiagnosticAction::Read => {
                    if args.process_id.as_deref().is_none_or(str::is_empty) {
                        return Err(RequestError::new("process_id", "required for read").into());
                    }
                    if args.stream.is_none() {
                        return Err(RequestError::new("stream", "stdout or stderr for read").into());
                    }
                    range("max_bytes", args.max_bytes as u64, 1, 65_536)?;
                }
                DiagnosticAction::SeedFile => {
                    if args.relative_path.is_none() {
                        return Err(
                            RequestError::new("relative_path", "required for seed_file").into()
                        );
                    }
                    let content = args.content.as_deref().unwrap_or("");
                    range("content", content.len() as u64, 0, 1024 * 1024)?;
                    if let Some(hash) = args.expected_sha256
                        && (hash.len() != 64 || !hash.bytes().all(|b| b.is_ascii_hexdigit()))
                    {
                        return Err(RequestError::new(
                            "expected_sha256",
                            "64-character SHA-256 hex digest",
                        )
                        .into());
                    }
                }
            }
        }
        "app_adapter" => {
            let args: crate::contracts::AppAdapterArgs = decode(value.clone())?;
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
                )
                .into());
            }
            if args.action == "bind" {
                string(value, "backend_session_id", "")?;
                string(value, "tui_session_id", "")?;
            } else if args.adapter_id.as_deref().is_none_or(str::is_empty) {
                return Err(
                    RequestError::new("adapter_id", "required for bound adapter reads").into(),
                );
            }
            match args.action.as_str() {
                "message_baseline" => string(value, "application_session_id", "")?,
                "semantic_wait" => {
                    string(value, "baseline_handle", "")?;
                    if args.predicate.is_none() {
                        return Err(RequestError::new(
                            "predicate",
                            "required typed turn_completed or tool_completed predicate",
                        )
                        .into());
                    }
                }
                _ => {}
            }
            if args
                .producer_instance
                .as_ref()
                .is_some_and(|value| value.is_empty())
            {
                return Err(RequestError::new(
                    "producer_instance",
                    "non-empty expected producer ID",
                )
                .into());
            }
            if args.producer_instance.is_some() {
                if args.action != "ui_state" {
                    return Err(RequestError::new(
                        "producer_instance",
                        "allowed only with action=ui_state",
                    )
                    .into());
                }
                string(value, "producer_instance", "")?;
            }
            if let Some(timeout) = args.timeout_ms {
                range("timeout_ms", timeout, 1, 60_000)?;
            }
        }
        "logs" | "artifacts" | "read_artifact" => {
            string(value, "session_id", "")?;
            if operation.trim_start_matches("terminal_") == "read_artifact" {
                string(value, "artifact_id", "")?;
            }
            if let Some(length) = value.get("length") {
                let n: u64 = decode(length.clone()).map_err(|e| e.prefix("length"))?;
                range("length", n, 0, 1048576)?;
            }
            if let Some(limit) = value.get("limit") {
                let n: u64 = decode(limit.clone()).map_err(|e| e.prefix("limit"))?;
                range("limit", n, 1, 500)?;
            }
        }
        "batch" => {
            let _: ActionContext =
                decode(required(value, "context", "")?.clone()).map_err(|e| e.prefix("context"))?;
            let actions: Vec<Value> =
                decode(required(value, "actions", "")?.clone()).map_err(|e| e.prefix("actions"))?;
            range("actions", actions.len() as u64, 1, 64)?;
            for (i, item) in actions.iter().enumerate() {
                action(item, &format!("actions[{i}]"))?;
            }
        }
        "mouse" => {
            if !["move", "click", "down", "up", "wheel_up", "wheel_down"]
                .contains(&value["action"].as_str().unwrap_or(""))
            {
                return Err(RequestError::new(
                    "action",
                    "move, click, down, up, wheel_up, or wheel_down",
                )
                .into());
            }
            if let Some(button) = value.get("button").filter(|v| !v.is_null())
                && ![Some("left"), Some("middle"), Some("right")].contains(&button.as_str())
            {
                return Err(RequestError::new("button", "left, middle, or right").into());
            }
        }
        "mcp_wait" | "wait" | "wait_ready" => {
            if operation.trim_start_matches("terminal_") == "mcp_wait"
                && value.get("predicate").is_none_or(Value::is_null)
                && value.get("wait_id").is_none_or(Value::is_null)
                && ![
                    "contains",
                    "absent",
                    "regex",
                    "stable",
                    "process_exited",
                    "output_quiet",
                    "ready",
                ]
                .contains(&value["kind"].as_str().unwrap_or(""))
            {
                return Err(RequestError::new(
                    "kind",
                    "supported wait kind or structured predicate",
                )
                .into());
            }
            if let Some(p) = value.get("predicate").filter(|v| !v.is_null()) {
                predicate(p, "predicate")?;
            }
            if let Some(timeout) = value.get("timeout_ms") {
                let n: u64 = decode(timeout.clone()).map_err(|e| e.prefix("timeout_ms"))?;
                range("timeout_ms", n, 1, RESUMABLE_WAIT_MAX_MS)?;
            }
            if let Some(poll) = value.get("poll_ms") {
                let n: u64 = decode(poll.clone()).map_err(|e| e.prefix("poll_ms"))?;
                range("poll_ms", n, 0, 5000)?;
            }
        }
        _ => {}
    }
    Ok(())
}
pub fn request(request: &RpcRequest) -> Result<()> {
    match request {
        RpcRequest::Agent {
            operation: op,
            params,
        } => {
            if params.get("response").is_some_and(|value| !value.is_null()) {
                return Err(RequestError::new(
                    "response",
                    "contract revision 2; legacy RPC does not project responses",
                )
                .code("contract_required")
                .into());
            }
            operation(op, params)?;
        }
        RpcRequest::Mouse { input, .. } => mouse(&serde_json::to_value(input)?, "input")?,
        RpcRequest::Resize { cols, rows, .. } => {
            range("cols", *cols as u64, 1, 300)?;
            range("rows", *rows as u64, 1, 120)?;
        }
        RpcRequest::Launch { spec } | RpcRequest::LaunchConfigured { spec, .. } => {
            range("cols", spec.cols as u64, 1, 300)?;
            range("rows", spec.rows as u64, 1, 120)?;
        }
        RpcRequest::Wait {
            timeout_ms,
            predicate,
            ..
        } => {
            range("timeout_ms", *timeout_ms, 1, LEGACY_WAIT_MAX_MS)?;
            if let crate::protocol::WaitPredicate::Regex { pattern } = predicate
                && regex::Regex::new(pattern).is_err()
            {
                return Err(
                    RequestError::new("predicate.pattern", "valid regular expression").into(),
                );
            }
        }
        RpcRequest::Cells {
            row,
            col,
            rows,
            cols,
            ..
        } => {
            region(&json!({"row":row,"col":col,"rows":rows,"cols":cols}), "")?;
            range("rows", *rows as u64, 1, 40)?;
            range("cols", *cols as u64, 1, 120)?;
        }
        _ => {}
    }
    Ok(())
}
