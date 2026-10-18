//! Opt-in bounded logical recording and honest scenario-v1 export templates.
use crate::assertions::FrameAssertion;
use crate::locators::ObservationIdentity;
use crate::predicates::PredicateSpec;
use crate::protocol::{ActionContext, AgentAction};
use crate::scenario::{Scenario, ScenarioExpectation, ScenarioPreconditions, ScenarioStep};
use anyhow::{Result, bail};
use rmcp::schemars;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

pub const MAX_RECORDING_EVENTS: usize = 512;
pub const MAX_RECORDING_BYTES: usize = 2 * 1024 * 1024;
const MAX_EVENT_BYTES: usize = 64 * 1024;

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RecordingOptions {
    pub recording_id: String,
    #[serde(default)]
    pub opted_in: bool,
    #[serde(default)]
    pub record_text: bool,
    pub max_events: usize,
    pub max_bytes: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RecordingSource {
    Cli,
    Mcp,
    Browser,
}

/// Deliberately has no environment values, view grants, URLs or auth-file paths.
/// Arguments/program/cwd still pass through the supplied declared-value redactor.
#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LaunchMetadata {
    pub program: String,
    pub args: Vec<String>,
    pub cwd: Option<String>,
    pub cols: u16,
    pub rows: u16,
    /// Trusted integration declaration, not inferred from an arbitrary cwd.
    pub workspace: RecordedWorkspace,
    pub external_environment_required: bool,
    pub external_auth_required: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RecordedWorkspace {
    SelectedIndependentCopy,
    ExternalProfile,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RecordedInput {
    pub action: AgentAction,
    pub context: ActionContext,
    pub baseline: ObservationIdentity,
    pub written_to_pty: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RecordedSequenceStep {
    Input {
        input: RecordedInput,
    },
    Wait {
        predicate: PredicateSpec,
        baseline: ObservationIdentity,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum RecordedOperation {
    Launch {
        launch: LaunchMetadata,
    },
    Input {
        input: RecordedInput,
    },
    Sequence {
        steps: Vec<RecordedSequenceStep>,
    },
    Wait {
        predicate: PredicateSpec,
        baseline: ObservationIdentity,
    },
    Assert {
        assertion: FrameAssertion,
        baseline: ObservationIdentity,
    },
    Observation {
        identity: ObservationIdentity,
        text: Option<String>,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct RecordedServiceEvent {
    /// Service-assigned sequence number; strictly increasing across all clients.
    pub order: u64,
    pub timestamp_ms: u64,
    pub source: RecordingSource,
    pub result_identity: ObservationIdentity,
    pub operation: RecordedOperation,
}

pub enum RedactedText {
    Safe(String),
    ExternalReferenceRequired,
}

/// Receives every string value AND object key before anything is retained.
/// Integration supplies explicit declared profile secrets, view tokens,
/// environment VALUES and auth paths. Never infer secrecy from env key names.
pub trait RecordingRedactor {
    fn redact(&self, value: &str) -> RedactedText;
}

/// Small explicit-value implementation for integrations/tests. Any occurrence
/// removes the entire field; secret values are never stored as trace metadata.
pub struct DeclaredSecretRedactor {
    values: Vec<String>,
}
impl DeclaredSecretRedactor {
    pub fn new(values: Vec<String>) -> Result<Self> {
        if values.len() > 256
            || values
                .iter()
                .any(|value| value.is_empty() || value.len() > 64 * 1024)
        {
            bail!("invalid declared secret set")
        }
        Ok(Self { values })
    }
}
impl RecordingRedactor for DeclaredSecretRedactor {
    fn redact(&self, value: &str) -> RedactedText {
        if self.values.iter().any(|secret| value.contains(secret)) {
            RedactedText::ExternalReferenceRequired
        } else {
            RedactedText::Safe(value.into())
        }
    }
}

pub trait RecordingOwner {
    type Lease;
    /// Called before the recording handle or first logical event is exposed.
    fn start(&mut self, options: &RecordingOptions) -> Result<Self::Lease>;
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct UnresolvedBinding {
    pub reference: String,
    pub record_order: u64,
    pub field: String,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct SafeRecord {
    pub order: u64,
    pub timestamp_ms: u64,
    pub source: RecordingSource,
    /// Sanitized typed event projection. Replaced scalar values are explicit
    /// external references, so this is intentionally a JSON trace representation.
    pub event: Value,
    pub unresolved: Vec<UnresolvedBinding>,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct RecordingTrace {
    pub recording_id: String,
    pub records: Vec<SafeRecord>,
    pub bytes_retained: usize,
    pub dropped_events: u64,
    pub missing_order_units: u64,
    #[serde(default)]
    pub unsupported_actions: u64,
    pub stopped_at_limit: bool,
    pub text_opted_in: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordDisposition {
    Recorded,
    DroppedAtLimit,
}

pub struct ManualRecorder<O: RecordingOwner, R: RecordingRedactor> {
    options: RecordingOptions,
    _lease: O::Lease,
    redactor: R,
    records: Vec<SafeRecord>,
    bytes: usize,
    last_order: Option<u64>,
    last_time: Option<u64>,
    dropped: u64,
    gaps: u64,
    unsupported: u64,
    stopped: bool,
}

impl<O: RecordingOwner, R: RecordingRedactor> ManualRecorder<O, R> {
    pub fn start(options: RecordingOptions, mut owner: O, redactor: R) -> Result<Self> {
        if !options.opted_in {
            bail!("manual recording requires explicit opt-in")
        }
        if options.recording_id.is_empty()
            || options.recording_id.len() > 64
            || !options
                .recording_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-_".contains(&byte))
            || options.max_events == 0
            || options.max_events > MAX_RECORDING_EVENTS
            || options.max_bytes < 1024
            || options.max_bytes > MAX_RECORDING_BYTES
        {
            bail!("invalid recording limits/id")
        }
        match redactor.redact(&options.recording_id) {
            RedactedText::Safe(value) if value == options.recording_id => (),
            _ => bail!("recording identifier contains a declared sensitive value"),
        }
        let lease = owner.start(&options)?;
        Ok(Self {
            options,
            _lease: lease,
            redactor,
            records: Vec::new(),
            bytes: 0,
            last_order: None,
            last_time: None,
            dropped: 0,
            gaps: 0,
            unsupported: 0,
            stopped: false,
        })
    }

    pub fn record(&mut self, event: RecordedServiceEvent) -> Result<RecordDisposition> {
        match &event.operation {
            RecordedOperation::Input { input } => validate_input(input)?,
            RecordedOperation::Sequence { steps } => {
                for step in steps {
                    if let RecordedSequenceStep::Input { input } = step {
                        validate_input(input)?;
                    }
                }
            }
            _ => (),
        }
        if self.last_order.is_some_and(|order| event.order <= order)
            || self.last_time.is_some_and(|time| event.timestamp_ms < time)
        {
            bail!("recording event order/time regressed")
        }
        if let Some(order) = self.last_order {
            self.gaps = self.gaps.saturating_add(event.order - order - 1);
        }
        self.last_order = Some(event.order);
        self.last_time = Some(event.timestamp_ms);
        if self.stopped {
            self.dropped = self.dropped.saturating_add(1);
            return Ok(RecordDisposition::DroppedAtLimit);
        }
        if let RecordedOperation::Sequence { steps } = &event.operation
            && (steps.is_empty() || steps.len() > 64)
        {
            bail!("recorded sequence must contain 1..=64 steps")
        }
        let value = serde_json::to_value(&event)?;
        if serde_json::to_vec(&value)?.len() > MAX_EVENT_BYTES {
            return Ok(self.drop_at_limit());
        }
        let mut unresolved = Vec::new();
        let mut next_ref = 0;
        let safe = sanitize(
            value,
            "",
            event.order,
            self.options.record_text,
            &self.redactor,
            &mut unresolved,
            &mut next_ref,
        )?;
        let record = SafeRecord {
            order: event.order,
            timestamp_ms: event.timestamp_ms,
            source: event.source,
            event: safe,
            unresolved,
        };
        // Serialized safe bytes are a content bound; add conservative owned JSON
        // node/String/Vec overhead and reject before retaining the record.
        let charge = owned_json_charge(&record.event)
            .saturating_add(serde_json::to_vec(&record.unresolved)?.len() * 4)
            .saturating_add(1024);
        if self.records.len() >= self.options.max_events
            || self.bytes.saturating_add(charge) > self.options.max_bytes
        {
            return Ok(self.drop_at_limit());
        }
        self.bytes += charge;
        self.records.push(record);
        Ok(RecordDisposition::Recorded)
    }

    pub fn trace(&self) -> RecordingTrace {
        RecordingTrace {
            recording_id: self.options.recording_id.clone(),
            records: self.records.clone(),
            bytes_retained: self.bytes,
            dropped_events: self.dropped,
            missing_order_units: self.gaps,
            unsupported_actions: self.unsupported,
            stopped_at_limit: self.stopped,
            text_opted_in: self.options.record_text,
        }
    }

    /// Records a service-observed operation that this exporter cannot encode.
    /// It is never accepted as a client-supplied proof of dispatch.
    pub fn mark_unsupported(&mut self) {
        self.unsupported = self.unsupported.saturating_add(1);
    }

    pub fn export(&self) -> Result<ScenarioExport> {
        export_trace(&self.trace())
    }

    fn drop_at_limit(&mut self) -> RecordDisposition {
        self.stopped = true;
        self.dropped = self.dropped.saturating_add(1);
        RecordDisposition::DroppedAtLimit
    }
}

fn sanitize<R: RecordingRedactor>(
    value: Value,
    path: &str,
    order: u64,
    record_text: bool,
    redactor: &R,
    unresolved: &mut Vec<UnresolvedBinding>,
    next_ref: &mut u64,
) -> Result<Value> {
    match value {
        Value::String(text) => {
            let omit = !record_text && (path.ends_with("/text") || path.ends_with("/pattern"));
            match redactor.redact(&text) {
                RedactedText::Safe(safe) if !omit && !safe.contains("${") => {
                    Ok(Value::String(safe))
                }
                _ => {
                    *next_ref += 1;
                    let reference = format!("external_record_{order}_{}", *next_ref);
                    unresolved.push(UnresolvedBinding {
                        reference: reference.clone(),
                        record_order: order,
                        field: path.into(),
                        reason: if omit {
                            "text recording not opted in"
                        } else {
                            "declared sensitive value or interpolation needs external binding"
                        }
                        .into(),
                    });
                    Ok(json!(format!("<external:{reference}>")))
                }
            }
        }
        Value::Array(values) => Ok(Value::Array(
            values
                .into_iter()
                .enumerate()
                .map(|(i, value)| {
                    sanitize(
                        value,
                        &format!("{path}/{i}"),
                        order,
                        record_text,
                        redactor,
                        unresolved,
                        next_ref,
                    )
                })
                .collect::<Result<_>>()?,
        )),
        Value::Object(values) => {
            let mut safe = serde_json::Map::new();
            for (key, value) in values {
                let key = match redactor.redact(&key) {
                    RedactedText::Safe(key) => key,
                    _ => bail!("typed recording field name was declared sensitive"),
                };
                safe.insert(
                    key.clone(),
                    sanitize(
                        value,
                        &format!("{path}/{key}"),
                        order,
                        record_text,
                        redactor,
                        unresolved,
                        next_ref,
                    )?,
                );
            }
            Ok(Value::Object(safe))
        }
        scalar => Ok(scalar),
    }
}

fn owned_json_charge(value: &Value) -> usize {
    let node = std::mem::size_of::<Value>() + 64;
    match value {
        Value::String(text) => node.saturating_add(text.capacity() * 2),
        Value::Array(values) => node
            .saturating_add(values.capacity() * std::mem::size_of::<Value>())
            .saturating_add(values.iter().map(owned_json_charge).sum::<usize>()),
        Value::Object(values) => node.saturating_add(
            values
                .iter()
                .map(|(key, value)| key.capacity() * 2 + 512 + owned_json_charge(value))
                .sum::<usize>(),
        ),
        _ => node,
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ResultBinding {
    pub step_id: String,
    pub field: String,
    pub variable: String,
    pub value_type: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ScenarioExport {
    #[serde(serialize_with = "serialize_export_scenario")]
    pub scenario: Scenario,
    pub unresolved_bindings: Vec<UnresolvedBinding>,
    pub result_bindings: Vec<ResultBinding>,
    pub replay_verified: bool,
    pub dropped_events: u64,
    pub missing_order_units: u64,
    pub unsupported_actions: u64,
}

fn serialize_export_scenario<S: serde::Serializer>(
    scenario: &Scenario,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    let mut value = serde_json::to_value(scenario).map_err(serde::ser::Error::custom)?;
    if let Some(steps) = value["steps"].as_array_mut() {
        for step in steps {
            if let Some(preconditions) =
                step.get_mut("preconditions").and_then(Value::as_object_mut)
            {
                // Omit absent optional guards; never change a supplied guard or
                // accept an incoming null as a valid string/integer binding.
                preconditions.retain(|_, value| !value.is_null());
            }
        }
    }
    value.serialize(serializer)
}

pub fn export_trace(trace: &RecordingTrace) -> Result<ScenarioExport> {
    let mut result = ScenarioExport {
        scenario: Scenario {
            version: 1,
            name: format!("recording-{}", trace.recording_id),
            template_only: false,
            timeout_ms: 600_000,
            close_on_failure: false,
            close_on_cancel: false,
            steps: Vec::new(),
        },
        unresolved_bindings: Vec::new(),
        result_bindings: Vec::new(),
        replay_verified: false,
        dropped_events: trace.dropped_events,
        missing_order_units: trace.missing_order_units,
        unsupported_actions: trace.unsupported_actions,
    };
    let mut launched = false;
    for record in &trace.records {
        result.unresolved_bindings.extend(record.unresolved.clone());
        if matches!(record.source, RecordingSource::Browser)
            && matches!(
                record.event["operation"]["kind"].as_str(),
                Some("input" | "sequence")
            )
        {
            require(
                &mut result,
                record.order,
                "browser.frame_and_view_revision",
                "browser input needs typed new-run frame/view-revision bindings before replay",
            );
        }
        let operation = &record.event["operation"];
        if operation["kind"] == "sequence" {
            for (index, step) in operation["steps"]
                .as_array()
                .into_iter()
                .flatten()
                .enumerate()
            {
                export_operation(
                    &mut result,
                    record.order,
                    &format!("event_{}_{}", record.order, index),
                    step,
                    &mut launched,
                )?;
            }
        } else {
            export_operation(
                &mut result,
                record.order,
                &format!("event_{}", record.order),
                operation,
                &mut launched,
            )?;
        }
    }
    if trace.dropped_events > 0 || trace.missing_order_units > 0 || trace.unsupported_actions > 0 {
        require(
            &mut result,
            0,
            "trace",
            "recording contains dropped, missing or unsupported logical events",
        );
    }
    if !launched {
        require(
            &mut result,
            0,
            "launch",
            "launch metadata is required before replay",
        );
    }
    // Rebind only known identity values. Any occurrence in a replay literal is
    // an external dependency, not permission to reuse an old runtime/session.
    for record in &trace.records {
        for field in ["session_id", "run_id", "frame_id"] {
            if let Some(identity) = record.event["result_identity"][field].as_str() {
                if identity.starts_with("<external:") {
                    continue;
                }
                let mut found = false;
                for step in &mut result.scenario.steps {
                    scrub_identity(&mut step.params, identity, &mut found);
                }
                if found {
                    require(
                        &mut result,
                        record.order,
                        field,
                        "recorded runtime identity embedded in replay literal needs result binding",
                    );
                }
            }
        }
    }
    if result.scenario.steps.is_empty() {
        bail!("recording contains no exportable logical events")
    }
    result.scenario.template_only = !result.unresolved_bindings.is_empty();
    // Launches always require a newly selected replay workspace; the recorded
    // copy can have been finalized/disposed before this export is used.
    result.scenario.validate()?;
    if !result.scenario.template_only {
        crate::contracts::validate_scenario(&result.scenario)?;
    }
    Ok(result)
}

fn export_operation(
    result: &mut ScenarioExport,
    order: u64,
    step_id: &str,
    operation: &Value,
    launched: &mut bool,
) -> Result<()> {
    let mut preconditions = None;
    let mut expect = None;
    let (name, params) = match operation["kind"].as_str() {
        Some("launch") => {
            if *launched {
                require(
                    result,
                    order,
                    "launch",
                    "multiple launches need explicit session bindings",
                );
            }
            *launched = true;
            let launch = &operation["launch"];
            if launch["workspace"] != "selected_independent_copy" {
                require(
                    result,
                    order,
                    "workspace",
                    "external configured profile/independent workspace binding required",
                );
            }
            for flag in ["external_environment_required", "external_auth_required"] {
                if launch[flag] == true {
                    require(
                        result,
                        order,
                        flag,
                        "external launch configuration required",
                    );
                }
            }
            let program = launch["program"].as_str().unwrap_or("");
            let program = if program.contains('/') || program.contains('\\') {
                require(
                    result,
                    order,
                    "program",
                    "absolute or path-based executable needs a new replay executable reference",
                );
                "<external:replay_executable>"
            } else {
                program
            };
            let mut args = launch["args"].as_array().cloned().unwrap_or_default();
            for arg in &mut args {
                if let Some(value) = arg.as_str() {
                    if value == "<redacted>" || value.starts_with("<external:") {
                        require(
                            result,
                            order,
                            "args",
                            "one or more launch arguments were redacted and require explicit replay binding",
                        );
                        *arg = json!("<external:replay_argument>");
                        continue;
                    }
                    let old_cwd = launch["cwd"].as_str().filter(|cwd| !cwd.is_empty());
                    let rebound = old_cwd
                        .and_then(|cwd| value.strip_prefix(cwd))
                        .filter(|tail| tail.is_empty() || tail.starts_with('/'));
                    if let Some(tail) = rebound {
                        *arg = json!(format!("${{workspace}}{tail}"));
                    } else if value.starts_with('/')
                        || value.contains(":\\")
                        || value.contains("../")
                    {
                        require(
                            result,
                            order,
                            "args",
                            "absolute/runtime/auth/data path needs external replay binding",
                        );
                        *arg = json!("<external:replay_argument>");
                    }
                }
            }
            (
                "launch".to_owned(),
                json!({"program":program,"args":args,"cwd":"${workspace}","cols":launch["cols"],"rows":launch["rows"],"env":{},"record_output":false}),
            )
        }
        Some("input") => {
            let input = &operation["input"];
            if input["written_to_pty"] != true {
                require(
                    result,
                    order,
                    "written_to_pty",
                    "input was not confirmed written; replay requires review",
                );
            }
            let action = input["action"]
                .as_object()
                .ok_or_else(|| anyhow::anyhow!("recorded action missing"))?;
            let kind = action["kind"].as_str().unwrap_or("");
            if !["key", "type", "paste", "mouse", "resize"].contains(&kind) {
                bail!("unsupported recorded input kind")
            }
            let mut params = action.clone();
            params.remove("kind");
            params.insert("session_id".into(), json!("${session}"));
            if kind == "mouse" {
                params = export_mouse(&input["action"]["input"])?;
            }
            let context = &input["context"];
            let needs_viewport = context["expected_viewport_revision"].is_u64()
                || ["mouse", "resize"].contains(&kind);
            preconditions = Some(ScenarioPreconditions {
                expected_run_id: Some("${run_id}".into()),
                // A deliberately impossible guard keeps this template fail-closed
                // until integration installs the typed numeric result binding.
                expected_viewport_revision: needs_viewport.then_some(u64::MAX),
            });
            if needs_viewport {
                require(
                    result,
                    order,
                    "preconditions.expected_viewport_revision",
                    "typed viewport_revision result binding unsupported by current scenario v1; replace fail-closed sentinel before replay",
                );
                result.result_bindings.push(ResultBinding {
                    step_id: step_id.into(),
                    field: "preconditions.expected_viewport_revision".into(),
                    variable: "viewport_revision".into(),
                    value_type: "u64".into(),
                });
            }
            if !context["controller_generation"].is_null() {
                require(
                    result,
                    order,
                    "controller_generation",
                    "typed controller authority binding required; current scenario adapter cannot preserve it",
                );
                result.result_bindings.push(ResultBinding {
                    step_id: step_id.into(),
                    field: "input_context.controller_generation".into(),
                    variable: "controller_generation".into(),
                    value_type: "u64".into(),
                });
            }
            (kind.to_owned(), Value::Object(params))
        }
        Some("wait") => (
            "wait".into(),
            json!({"session_id":"${session}","predicate":operation["predicate"],"timeout_ms":30000}),
        ),
        Some("assert") => {
            expect = Some(ScenarioExpectation {
                pointer: Some("/passed".into()),
                equals: Some(json!(true)),
                truthy: None,
            });
            (
                "assert".into(),
                json!({"session_id":"${session}","assertion":operation["assertion"]}),
            )
        }
        Some("observation") => {
            result.result_bindings.push(ResultBinding {
                step_id: step_id.into(),
                field: "/frame_id".into(),
                variable: "frame_id".into(),
                value_type: "string".into(),
            });
            result.result_bindings.push(ResultBinding {
                step_id: step_id.into(),
                field: "/viewport_revision".into(),
                variable: "viewport_revision".into(),
                value_type: "u64".into(),
            });
            ("observe".into(), json!({"session_id":"${session}"}))
        }
        _ => bail!("unsupported recorded logical event"),
    };
    if name != "launch" && !*launched {
        require(
            result,
            order,
            "session",
            "input session has no preceding recorded launch",
        );
    }
    result.scenario.steps.push(ScenarioStep {
        id: step_id.into(),
        operation: name,
        params,
        preconditions,
        timeout_ms: 30000,
        expect,
    });
    Ok(())
}

fn export_mouse(input: &Value) -> Result<serde_json::Map<String, Value>> {
    Ok(json!({"session_id":"${session}","input":input})
        .as_object()
        .unwrap()
        .clone())
}

fn require(result: &mut ScenarioExport, order: u64, field: &str, reason: &str) {
    result.unresolved_bindings.push(UnresolvedBinding {
        reference: format!(
            "external_record_{order}_binding_{}",
            result.unresolved_bindings.len()
        ),
        record_order: order,
        field: field.into(),
        reason: reason.into(),
    });
}

fn validate_input(input: &RecordedInput) -> Result<()> {
    if input.context.expected_run_id != input.baseline.run_id {
        bail!("recorded input run and baseline mismatch")
    }
    if input
        .context
        .expected_viewport_revision
        .is_some_and(|revision| revision != input.baseline.viewport_revision)
    {
        bail!("recorded input viewport and baseline mismatch")
    }
    crate::validation::action(
        &serde_json::to_value(&input.action)?,
        "recorded_input.action",
    )?;
    Ok(())
}

fn scrub_identity(value: &mut Value, identity: &str, found: &mut bool) {
    match value {
        Value::String(text)
            if !text.contains("${") && !identity.is_empty() && text.contains(identity) =>
        {
            *text = "<external:replay_identity>".into();
            *found = true;
        }
        Value::Array(values) => {
            for value in values {
                scrub_identity(value, identity, found);
            }
        }
        Value::Object(values) => {
            for value in values.values_mut() {
                scrub_identity(value, identity, found);
            }
        }
        _ => (),
    }
}
