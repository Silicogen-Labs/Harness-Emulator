//! Durable, versioned scripted scenarios for the terminal runtime.
//!
//! The module deliberately depends on a small backend trait rather than on the
//! service implementation. The service can adapt each operation to its existing
//! launch, observation, input, artifact, and lifecycle APIs.

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use ulid::Ulid;

pub const SCENARIO_VERSION: u16 = 1;
const DEFAULT_OVERALL_TIMEOUT_MS: u64 = 10 * 60 * 1000;
const DEFAULT_STEP_TIMEOUT_MS: u64 = 30 * 1000;
const MAX_SCENARIO_TIMEOUT_MS: u64 = 24 * 60 * 60 * 1000;
const MAX_STEP_TIMEOUT_MS: u64 = 10 * 60 * 1000;
const BACKEND_WAIT_POLL_MS: u64 = 150;
const BACKEND_WAIT_CALL_LIMIT: Duration = Duration::from_millis(190);
const BACKEND_HOOK_LIMIT: Duration = Duration::from_secs(1);
const CONTROL_POLL_INTERVAL: Duration = Duration::from_millis(100);

/// A versioned scenario document. Validation is performed before a job is
/// created or any backend operation is executed.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Scenario {
    pub version: u16,
    pub name: String,
    #[serde(default)]
    pub template_only: bool,
    #[serde(default = "default_overall_timeout")]
    pub timeout_ms: u64,
    #[serde(default)]
    pub close_on_failure: bool,
    #[serde(default)]
    pub close_on_cancel: bool,
    pub steps: Vec<ScenarioStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScenarioStep {
    pub id: String,
    pub operation: String,
    #[serde(default)]
    pub params: Value,
    /// Backend-enforced input preconditions, passed through under `_preconditions`
    /// after literal variable interpolation.
    #[serde(default, deserialize_with = "deserialize_preconditions")]
    pub preconditions: Option<ScenarioPreconditions>,
    #[serde(default = "default_step_timeout")]
    pub timeout_ms: u64,
    #[serde(default)]
    pub expect: Option<ScenarioExpectation>,
}

/// Expected state used by a scripted terminal input. Fields are optional for
/// compatibility; each supplied value is validated before the job can run.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default, deny_unknown_fields)]
pub struct ScenarioPreconditions {
    #[serde(deserialize_with = "deserialize_present_string")]
    pub expected_run_id: Option<String>,
    #[serde(deserialize_with = "deserialize_present_u64")]
    pub expected_viewport_revision: Option<u64>,
}

fn deserialize_preconditions<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<ScenarioPreconditions>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    ScenarioPreconditions::deserialize(deserializer).map(Some)
}

fn deserialize_present_string<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    String::deserialize(deserializer).map(Some)
}

fn deserialize_present_u64<'de, D>(deserializer: D) -> std::result::Result<Option<u64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    u64::deserialize(deserializer).map(Some)
}

/// Optional machine-checkable result requirement. `pointer` uses JSON Pointer.
/// Exactly one of `equals` or `truthy` may be supplied; with neither, a
/// successful backend call is the required result.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ScenarioExpectation {
    #[serde(default)]
    pub pointer: Option<String>,
    #[serde(default)]
    pub equals: Option<Value>,
    #[serde(default)]
    pub truthy: Option<bool>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScenarioStatus {
    Queued,
    Running,
    PausedForControl,
    Passed,
    Failed,
    Cancelled,
    TimedOut,
    Interrupted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioStepOutcome {
    pub step_id: String,
    pub operation: String,
    pub status: ScenarioStatus,
    pub started_at_ms: u128,
    pub finished_at_ms: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub action_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub frame_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default)]
    pub backend_work_may_continue: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioReport {
    pub schema_version: u16,
    #[serde(alias = "run_id")]
    pub scenario_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub app_run_id: Option<String>,
    #[serde(default)]
    pub backend_work_may_continue: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backend_operation_may_continue: Option<String>,
    pub scenario_name: String,
    pub status: ScenarioStatus,
    pub started_at_ms: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub finished_at_ms: Option<u128>,
    pub deadline_at_ms: u128,
    pub variables: BTreeMap<String, String>,
    pub steps: Vec<ScenarioStepOutcome>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub failure_reason: Option<String>,
}

/// Shared execution adapter. The manager bounds how long it waits for each
/// call. Since a synchronous call cannot be force-killed safely, backends
/// should honor supplied timeouts and implement `cancel_pending` when possible.
pub trait ScenarioBackend: Send + Sync + 'static {
    fn execute(&self, operation: &str, params: Value) -> Result<Value>;

    /// Called between steps. The runner never reclaims a browser-held lease.
    fn control_state(&self) -> Result<ScenarioControl> {
        Ok(ScenarioControl::Available)
    }

    /// Optional best-effort cleanup for sessions created by this scenario.
    fn close_owned_sessions(&self) -> Result<()> {
        Ok(())
    }

    /// Best-effort cancellation for a backend operation that outlived its
    /// deadline. The report still marks that work as potentially active unless
    /// the backend call itself returned.
    fn cancel_pending(&self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScenarioControl {
    Available,
    BrowserHoldsControl,
}

#[derive(Clone)]
pub struct ScenarioManager {
    inner: Arc<ManagerInner>,
}

pub type ScenarioLifecycleHook = Arc<dyn Fn(&ScenarioReport) -> Result<()> + Send + Sync>;

struct ManagerInner {
    root: PathBuf,
    jobs: Mutex<BTreeMap<String, Arc<Job>>>,
}

struct Job {
    report: Mutex<ScenarioReport>,
    cancel: AtomicBool,
    report_path: PathBuf,
    on_finished: Option<ScenarioLifecycleHook>,
}

impl ScenarioManager {
    /// Creates a manager and marks jobs left in a nonterminal state by a prior
    /// process as interrupted. Reports remain available from disk.
    pub fn new(root: impl Into<PathBuf>) -> Result<Self> {
        let root = root.into();
        fs::create_dir_all(&root)
            .with_context(|| format!("create scenario root {}", root.display()))?;
        let mut jobs = BTreeMap::new();
        for entry in
            fs::read_dir(&root).with_context(|| format!("read scenario root {}", root.display()))?
        {
            let entry = entry?;
            if !entry.file_type()?.is_file()
                || entry.path().extension().is_none_or(|ext| ext != "json")
            {
                continue;
            }
            let bytes = fs::read(entry.path())?;
            let Ok(mut report) = serde_json::from_slice::<ScenarioReport>(&bytes) else {
                continue;
            };
            if matches!(
                report.status,
                ScenarioStatus::Queued | ScenarioStatus::Running | ScenarioStatus::PausedForControl
            ) {
                report.status = ScenarioStatus::Interrupted;
                report.finished_at_ms = Some(now_ms());
                report.failure_reason =
                    Some("scenario manager restarted before completion".to_owned());
                persist_report(&entry.path(), &report)?;
            }
            let path = entry.path();
            let scenario_id = report.scenario_id.clone();
            jobs.insert(
                scenario_id,
                Arc::new(Job {
                    report: Mutex::new(report),
                    cancel: AtomicBool::new(false),
                    report_path: path,
                    on_finished: None,
                }),
            );
        }
        Ok(Self {
            inner: Arc::new(ManagerInner {
                root,
                jobs: Mutex::new(jobs),
            }),
        })
    }

    pub fn root(&self) -> &Path {
        &self.inner.root
    }

    /// Parse and validate a JSON document without starting a job.
    pub fn validate_json(input: &str) -> Result<Scenario> {
        let scenario: Scenario = serde_json::from_str(input).context("parse scenario JSON")?;
        scenario.validate()?;
        Ok(scenario)
    }

    /// Starts a durable background job. `workspace` is optional and is exposed
    /// as the literal `${workspace}` variable. `${scenario_id}` and `${job_id}`
    /// identify this durable job; `${session}` and `${run_id}` are populated
    /// only from the backend's launch/restart result.
    pub fn start(
        &self,
        scenario: Scenario,
        backend: Arc<dyn ScenarioBackend>,
        workspace: Option<PathBuf>,
    ) -> Result<String> {
        self.start_with_hooks(scenario, backend, workspace, None, None)
    }

    pub fn start_with_hooks(
        &self,
        scenario: Scenario,
        backend: Arc<dyn ScenarioBackend>,
        workspace: Option<PathBuf>,
        on_created: Option<ScenarioLifecycleHook>,
        on_finished: Option<ScenarioLifecycleHook>,
    ) -> Result<String> {
        scenario.validate()?;
        if scenario.template_only {
            bail!("template-only scenarios must be reviewed and verified before execution");
        }
        scenario.validate_launch_variables(workspace.is_some())?;
        let scenario_id = Ulid::generate().to_string();
        let started = now_ms();
        let mut variables = BTreeMap::new();
        variables.insert("scenario_id".to_owned(), scenario_id.clone());
        variables.insert("job_id".to_owned(), scenario_id.clone());
        if let Some(path) = workspace {
            variables.insert("workspace".to_owned(), path.to_string_lossy().into_owned());
        }
        let report = ScenarioReport {
            schema_version: SCENARIO_VERSION,
            scenario_id: scenario_id.clone(),
            session_id: None,
            app_run_id: None,
            backend_work_may_continue: false,
            backend_operation_may_continue: None,
            scenario_name: scenario.name.clone(),
            status: ScenarioStatus::Queued,
            started_at_ms: started,
            finished_at_ms: None,
            deadline_at_ms: started.saturating_add(scenario.timeout_ms as u128),
            variables,
            steps: Vec::new(),
            failure_reason: None,
        };
        let report_path = self.inner.root.join(format!("{scenario_id}.json"));
        persist_report(&report_path, &report)?;
        if let Some(hook) = &on_created
            && let Err(error) = hook(&report)
        {
            let _ = fs::remove_file(&report_path);
            return Err(error).context("register scenario report before execution");
        }
        let job = Arc::new(Job {
            report: Mutex::new(report),
            cancel: AtomicBool::new(false),
            report_path,
            on_finished,
        });
        self.inner
            .jobs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(scenario_id.clone(), Arc::clone(&job));
        let worker_job = Arc::clone(&job);
        let worker = thread::Builder::new()
            .name(format!("scenario-{scenario_id}"))
            .spawn(move || {
                run_scenario(scenario, backend, worker_job);
            });
        if let Err(error) = worker {
            finish(
                &job,
                ScenarioStatus::Failed,
                Some(format!("could not start scenario worker: {error}")),
            );
            return Err(error).context("spawn scenario worker");
        }
        Ok(scenario_id)
    }

    pub fn status(&self, scenario_id: &str) -> Result<ScenarioReport> {
        let jobs = self
            .inner
            .jobs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let job = jobs
            .get(scenario_id)
            .ok_or_else(|| anyhow!("unknown scenario job: {scenario_id}"))?;
        Ok(job
            .report
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone())
    }

    pub fn cancel(&self, scenario_id: &str) -> Result<ScenarioReport> {
        let jobs = self
            .inner
            .jobs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let job = jobs
            .get(scenario_id)
            .ok_or_else(|| anyhow!("unknown scenario job: {scenario_id}"))?;
        let report = job
            .report
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !is_terminal(report.status) {
            job.cancel.store(true, Ordering::Release);
        }
        Ok(report.clone())
    }

    pub fn list(&self) -> Vec<ScenarioReport> {
        let jobs = self
            .inner
            .jobs
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        jobs.values()
            .map(|job| {
                job.report
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .clone()
            })
            .collect()
    }
}

impl Scenario {
    pub fn validate(&self) -> Result<()> {
        if self.version != SCENARIO_VERSION {
            bail!(
                "unsupported scenario version {}; expected {}",
                self.version,
                SCENARIO_VERSION
            );
        }
        if self.name.trim().is_empty() {
            bail!("scenario name must not be empty");
        }
        if self.timeout_ms == 0 || self.timeout_ms > MAX_SCENARIO_TIMEOUT_MS {
            bail!("scenario timeout_ms must be between 1 and {MAX_SCENARIO_TIMEOUT_MS}");
        }
        if self.steps.is_empty() {
            bail!("scenario must contain at least one step");
        }
        let mut ids = std::collections::BTreeSet::new();
        for step in &self.steps {
            if step.id.trim().is_empty() || !ids.insert(step.id.clone()) {
                bail!("step IDs must be nonempty and unique: {:?}", step.id);
            }
            if !SUPPORTED_OPERATIONS.contains(&step.operation.as_str()) {
                bail!("unsupported scenario operation {:?}", step.operation);
            }
            if step.timeout_ms == 0 || step.timeout_ms > MAX_STEP_TIMEOUT_MS {
                bail!(
                    "step {} timeout_ms must be between 1 and {MAX_STEP_TIMEOUT_MS}",
                    step.id
                );
            }
            if !step.params.is_object() {
                bail!("step {} params must be a JSON object", step.id);
            }
            if let Some(expect) = &step.expect {
                if expect.equals.is_some() && expect.truthy.is_some() {
                    bail!(
                        "step {} expectation must use either equals or truthy",
                        step.id
                    );
                }
                if expect
                    .pointer
                    .as_deref()
                    .is_some_and(|pointer| !pointer.is_empty() && !pointer.starts_with('/'))
                {
                    bail!(
                        "step {} expectation pointer must be a JSON Pointer",
                        step.id
                    );
                }
            }
            let preconditions = step
                .preconditions
                .as_ref()
                .map(serde_json::to_value)
                .transpose()?;
            for text in strings_in(&step.params)
                .into_iter()
                .chain(preconditions.as_ref().into_iter().flat_map(strings_in))
            {
                for variable in variable_names(&text)? {
                    if !["session", "workspace", "run_id", "scenario_id", "job_id"]
                        .contains(&variable.as_str())
                    {
                        bail!(
                            "step {} references unknown scenario variable ${{{variable}}}",
                            step.id
                        );
                    }
                }
            }
        }
        Ok(())
    }

    fn validate_launch_variables(&self, has_workspace: bool) -> Result<()> {
        let mut launched = false;
        let mut workspace_available = has_workspace;
        for step in &self.steps {
            let params_strings = strings_in(&step.params);
            let preconditions = step
                .preconditions
                .as_ref()
                .map(serde_json::to_value)
                .transpose()?;
            for text in params_strings
                .iter()
                .cloned()
                .chain(preconditions.as_ref().into_iter().flat_map(strings_in))
            {
                for variable in variable_names(&text)? {
                    match variable.as_str() {
                        "workspace" if !workspace_available => {
                            let defer_for_snapshot = step.operation == "launch"
                                && has_launch_workspace_option(&step.params)
                                && has_workspace_target_reference(&step.params)
                                && !has_workspace_reference_outside_target(&step.params)
                                && !preconditions
                                    .as_ref()
                                    .is_some_and(|value| contains_variable(value, "workspace"));
                            if defer_for_snapshot {
                                continue;
                            }
                            bail!(
                                "step {} references ${{workspace}} before a workspace is available; supply a workspace or configure launch options.workspace with target cwd/args placeholders",
                                step.id
                            )
                        }
                        "session" | "run_id" if !launched => {
                            bail!(
                                "step {} references ${{session}} or ${{run_id}} before a launch step",
                                step.id
                            )
                        }
                        _ => {}
                    }
                }
            }
            if step.operation == "launch" {
                launched = true;
                if has_launch_workspace_option(&step.params) {
                    workspace_available = true;
                }
            }
        }
        Ok(())
    }
}

fn strings_in(value: &Value) -> Vec<String> {
    match value {
        Value::String(text) => vec![text.clone()],
        Value::Array(values) => values.iter().flat_map(strings_in).collect(),
        Value::Object(values) => values.values().flat_map(strings_in).collect(),
        _ => Vec::new(),
    }
}

fn has_launch_workspace_option(params: &Value) -> bool {
    params
        .get("options")
        .and_then(|options| options.get("workspace"))
        .is_some_and(Value::is_object)
}

fn has_workspace_target_reference(value: &Value) -> bool {
    fn visit(value: &Value, in_target: bool) -> bool {
        match value {
            Value::String(text) => {
                in_target
                    && variable_names(text)
                        .is_ok_and(|names| names.iter().any(|name| name == "workspace"))
            }
            Value::Array(values) => values.iter().any(|item| visit(item, in_target)),
            Value::Object(values) => values.iter().any(|(key, item)| {
                let target = in_target || matches!(key.as_str(), "cwd" | "args" | "argv");
                visit(item, target)
            }),
            _ => false,
        }
    }
    visit(value, false)
}

fn has_workspace_reference_outside_target(value: &Value) -> bool {
    fn visit(value: &Value, in_target: bool) -> bool {
        match value {
            Value::String(text) => {
                !in_target
                    && variable_names(text)
                        .is_ok_and(|names| names.iter().any(|name| name == "workspace"))
            }
            Value::Array(values) => values.iter().any(|item| visit(item, in_target)),
            Value::Object(values) => values.iter().any(|(key, item)| {
                let target = in_target || matches!(key.as_str(), "cwd" | "args" | "argv");
                visit(item, target)
            }),
            _ => false,
        }
    }
    visit(value, false)
}

fn contains_variable(value: &Value, variable: &str) -> bool {
    strings_in(value).iter().any(|text| {
        variable_names(text).is_ok_and(|names| names.iter().any(|name| name == variable))
    })
}

fn variable_names(text: &str) -> Result<Vec<String>> {
    let mut found = Vec::new();
    let mut rest = text;
    while let Some(start) = rest.find("${") {
        let after = &rest[start + 2..];
        let end = after
            .find('}')
            .ok_or_else(|| anyhow!("unterminated variable expression"))?;
        let name = &after[..end];
        if name.is_empty()
            || !name
                .chars()
                .all(|character| character.is_ascii_alphanumeric() || character == '_')
        {
            bail!("invalid variable name {name:?}");
        }
        found.push(name.to_owned());
        rest = &after[end + 1..];
    }
    Ok(found)
}

const SUPPORTED_OPERATIONS: &[&str] = &[
    "launch",
    "observe",
    "key",
    "type",
    "paste",
    "mouse",
    "resize",
    "wait",
    "logs",
    "screenshot",
    "save_frame",
    "assert",
    "diff",
    "restart",
    "close",
];

fn run_scenario(scenario: Scenario, backend: Arc<dyn ScenarioBackend>, job: Arc<Job>) {
    set_status(&job, ScenarioStatus::Running, None);
    let started = Instant::now();
    let deadline = started + Duration::from_millis(scenario.timeout_ms);
    let mut variables = job
        .report
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .variables
        .clone();
    let mut terminal = ScenarioStatus::Passed;
    let mut failure: Option<String> = None;

    'steps: for step in &scenario.steps {
        if let Err(status) = wait_until_available(&backend, &job, deadline) {
            terminal = status;
            failure = Some(status_message(status));
            break;
        }
        if job.cancel.load(Ordering::Acquire) {
            terminal = ScenarioStatus::Cancelled;
            failure = Some("cancelled by client".to_owned());
            break;
        }
        let elapsed = started.elapsed();
        if elapsed >= Duration::from_millis(scenario.timeout_ms) {
            terminal = ScenarioStatus::TimedOut;
            failure = Some("overall scenario deadline exceeded".to_owned());
            break;
        }
        let step_started_at = now_ms();
        let interpolated =
            if step.operation == "launch" && has_launch_workspace_option(&step.params) {
                interpolate_launch_params(&step.params, &variables)
            } else {
                interpolate_value(&step.params, &variables)
            };
        let mut params = match interpolated {
            Ok(value) => value,
            Err(err) => {
                terminal = ScenarioStatus::Failed;
                failure = Some(format!("step {} interpolation: {err:#}", step.id));
                record_step_failure(
                    &job,
                    &variables,
                    step,
                    ScenarioStatus::Failed,
                    step_started_at,
                    failure.as_deref().unwrap_or("interpolation failed"),
                );
                break;
            }
        };
        if let Some(preconditions) = &step.preconditions {
            let interpolated = serde_json::to_value(preconditions)
                .map_err(anyhow::Error::from)
                .and_then(|encoded| interpolate_value(&encoded, &variables));
            match interpolated {
                Ok(value) => {
                    params
                        .as_object_mut()
                        .expect("validated params object")
                        .insert("_preconditions".to_owned(), value);
                }
                Err(err) => {
                    terminal = ScenarioStatus::Failed;
                    failure = Some(format!(
                        "step {} precondition interpolation: {err:#}",
                        step.id
                    ));
                    record_step_failure(
                        &job,
                        &variables,
                        step,
                        ScenarioStatus::Failed,
                        step_started_at,
                        failure
                            .as_deref()
                            .unwrap_or("precondition interpolation failed"),
                    );
                    break;
                }
            }
        }
        let step_deadline = (Instant::now() + Duration::from_millis(step.timeout_ms)).min(deadline);
        if step_deadline
            .saturating_duration_since(Instant::now())
            .is_zero()
        {
            terminal = ScenarioStatus::TimedOut;
            failure = Some(format!(
                "step {} deadline exceeded before execution",
                step.id
            ));
            record_step_failure(
                &job,
                &variables,
                step,
                ScenarioStatus::TimedOut,
                step_started_at,
                failure.as_deref().unwrap_or("step deadline exceeded"),
            );
            break;
        }
        if step.operation == "wait" {
            params
                .as_object_mut()
                .expect("validated params object")
                .insert(
                    "wait_id".to_owned(),
                    Value::String(format!("{}:{}", variables["scenario_id"], step.id)),
                );
        }
        let mut backend_work_may_continue = false;
        let result = execute_step(
            &backend,
            step,
            params,
            &job,
            step_deadline,
            &mut backend_work_may_continue,
            scenario.close_on_cancel,
            scenario.close_on_failure,
        );
        let (step_status, result_value, error) = match result {
            Ok(value) if step.operation == "wait" && value["status"] == "timed_out" => (
                ScenarioStatus::TimedOut,
                Some(value),
                Some("wait deadline exceeded".into()),
            ),
            Ok(value) => match validate_step_result(step, &value) {
                Ok(()) => (ScenarioStatus::Passed, Some(value), None),
                Err(err) => (
                    ScenarioStatus::Failed,
                    Some(value),
                    Some(format!("{err:#}")),
                ),
            },
            Err(err) if job.cancel.load(Ordering::Acquire) => {
                (ScenarioStatus::Cancelled, None, Some(format!("{err:#}")))
            }
            Err(err) if Instant::now() >= step_deadline || Instant::now() >= deadline => {
                (ScenarioStatus::TimedOut, None, Some(format!("{err:#}")))
            }
            Err(err) => (ScenarioStatus::Failed, None, Some(format!("{err:#}"))),
        };
        let mut outcome = ScenarioStepOutcome {
            step_id: step.id.clone(),
            operation: step.operation.clone(),
            status: step_status,
            started_at_ms: step_started_at,
            finished_at_ms: now_ms(),
            result: result_value.clone(),
            action_id: result_value
                .as_ref()
                .and_then(|value| find_string_key(value, &["action_id", "actionId"])),
            frame_id: result_value
                .as_ref()
                .and_then(|value| find_string_key(value, &["frame_id", "frameId"])),
            error: error.clone(),
            backend_work_may_continue,
        };
        if let Some(value) = &result_value {
            if matches!(step.operation.as_str(), "launch" | "restart") {
                if let Some(session) = find_string_key(value, &["session_id", "sessionId"]) {
                    variables.insert("session".to_owned(), session);
                }
                if let Some(run_id) = find_string_key(value, &["run_id", "runId"]) {
                    variables.insert("run_id".to_owned(), run_id);
                }
            }
            if step.operation == "launch"
                && let Some(effective_workspace) = effective_workspace_root(value)
            {
                variables.insert("workspace".to_owned(), effective_workspace.to_owned());
            }
        }
        update_variables_and_step(&job, variables.clone(), outcome.clone());
        if step_status != ScenarioStatus::Passed {
            terminal = if step_status == ScenarioStatus::Cancelled {
                ScenarioStatus::Cancelled
            } else if step_status == ScenarioStatus::TimedOut {
                ScenarioStatus::TimedOut
            } else {
                ScenarioStatus::Failed
            };
            failure = error.or_else(|| Some(format!("step {} failed", step.id)));
            if backend_work_may_continue {
                let uncertain = format!(
                    "backend operation for step {} may still be running after cancellation/deadline; cancellation was requested",
                    step.id
                );
                if let Err(cancel_error) =
                    run_backend_hook(Arc::clone(&backend), BackendHook::CancelPending)
                {
                    mark_backend_uncertain(&job, "cancel_pending");
                    failure = Some(format!(
                        "{}; backend cancellation hook failed: {cancel_error:#}",
                        failure.unwrap_or_default()
                    ));
                }
                failure = Some(
                    failure.map_or(uncertain.clone(), |reason| format!("{reason}; {uncertain}")),
                );
            }
            break 'steps;
        }
        outcome.result = result_value;
    }

    if job.cancel.load(Ordering::Acquire) && terminal == ScenarioStatus::Passed {
        terminal = ScenarioStatus::Cancelled;
        failure = Some("cancelled by client".to_owned());
    }
    if terminal == ScenarioStatus::TimedOut && failure.is_none() {
        failure = Some("overall scenario deadline exceeded".to_owned());
    }
    if matches!(
        terminal,
        ScenarioStatus::Failed | ScenarioStatus::Cancelled | ScenarioStatus::TimedOut
    ) {
        let should_close = if terminal == ScenarioStatus::Cancelled {
            scenario.close_on_cancel
        } else {
            scenario.close_on_failure
        };
        if should_close
            && let Err(err) =
                run_backend_hook(Arc::clone(&backend), BackendHook::CloseOwnedSessions)
        {
            mark_backend_uncertain(&job, "close_owned_sessions");
            let suffix = format!("cleanup failed: {err:#}");
            failure = Some(failure.map_or(suffix.clone(), |f| format!("{f}; {suffix}")));
        }
    }
    finish(&job, terminal, failure);
}

fn wait_until_available(
    backend: &Arc<dyn ScenarioBackend>,
    job: &Arc<Job>,
    deadline: Instant,
) -> std::result::Result<(), ScenarioStatus> {
    loop {
        if job.cancel.load(Ordering::Acquire) {
            return Err(ScenarioStatus::Cancelled);
        }
        if Instant::now() >= deadline {
            return Err(ScenarioStatus::TimedOut);
        }
        match backend.control_state() {
            Ok(ScenarioControl::Available) => {
                set_status(job, ScenarioStatus::Running, None);
                return Ok(());
            }
            Ok(ScenarioControl::BrowserHoldsControl) => {
                set_status(job, ScenarioStatus::PausedForControl, None)
            }
            Err(err) => {
                set_status(
                    job,
                    ScenarioStatus::Failed,
                    Some(format!("control status failed: {err:#}")),
                );
                return Err(ScenarioStatus::Failed);
            }
        }
        thread::sleep(
            CONTROL_POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now())),
        );
    }
}

#[allow(clippy::too_many_arguments)] // Cancellation, deadline and cleanup policies remain explicit at dispatch.
fn execute_step(
    backend: &Arc<dyn ScenarioBackend>,
    step: &ScenarioStep,
    params: Value,
    job: &Arc<Job>,
    deadline: Instant,
    backend_work_may_continue: &mut bool,
    close_on_cancel: bool,
    close_on_timeout: bool,
) -> Result<Value> {
    loop {
        if job.cancel.load(Ordering::Acquire) {
            bail!("cancelled by client");
        }
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            bail!("step deadline exceeded");
        }
        let poll_ms = if step.operation == "wait" {
            remaining
                .as_millis()
                .min(BACKEND_WAIT_POLL_MS as u128)
                .max(1) as u64
        } else {
            remaining.as_millis().min(u64::MAX as u128).max(1) as u64
        };
        let mut poll_params = params.clone();
        if step.operation == "wait" {
            poll_params
                .as_object_mut()
                .expect("validated params object")
                .insert("poll_ms".to_owned(), Value::from(poll_ms));
            // Poll duration is not the wait's lifetime. The first service call
            // creates one durable handle with the remaining bounded deadline.
            let lifetime = params
                .get("timeout_ms")
                .and_then(Value::as_u64)
                .unwrap_or(u64::MAX)
                .min(remaining.as_millis().min(u64::MAX as u128) as u64)
                .max(1);
            poll_params["timeout_ms"] = Value::from(lifetime);
        }
        let (tx, rx) = mpsc::sync_channel(1);
        let call_still_running = Arc::new(AtomicBool::new(true));
        let worker_running = Arc::clone(&call_still_running);
        let late_cleanup_requested = Arc::new(AtomicBool::new(false));
        let worker_late_cleanup = Arc::clone(&late_cleanup_requested);
        let call_backend = Arc::clone(backend);
        let late_cleanup_backend = Arc::clone(backend);
        let operation = step.operation.clone();
        if let Err(err) = thread::Builder::new()
            .name(format!("scenario-action-{}", step.id))
            .spawn(move || {
                let result = call_backend.execute(&operation, poll_params);
                worker_running.store(false, Ordering::Release);
                if worker_late_cleanup.load(Ordering::Acquire) {
                    let _ = late_cleanup_backend.close_owned_sessions();
                }
                let _ = tx.send(result);
            })
        {
            call_still_running.store(false, Ordering::Release);
            bail!("could not spawn backend action: {err}");
        }
        let call_deadline = if step.operation == "wait" {
            (Instant::now() + BACKEND_WAIT_CALL_LIMIT).min(deadline)
        } else {
            deadline
        };
        let result = loop {
            if job.cancel.load(Ordering::Acquire) {
                *backend_work_may_continue |= call_still_running.load(Ordering::Acquire);
                if close_on_cancel {
                    late_cleanup_requested.store(true, Ordering::Release);
                }
                bail!("cancelled by client");
            }
            let remaining = call_deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                *backend_work_may_continue |= call_still_running.load(Ordering::Acquire);
                if close_on_timeout {
                    late_cleanup_requested.store(true, Ordering::Release);
                }
                if step.operation == "wait" && Instant::now() < deadline {
                    bail!("backend wait poll exceeded the 190ms call limit");
                }
                bail!("step deadline exceeded");
            }
            match rx.recv_timeout(CONTROL_POLL_INTERVAL.min(remaining)) {
                Ok(result) => break result?,
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    *backend_work_may_continue |= call_still_running.load(Ordering::Acquire);
                    bail!("backend action ended without a result");
                }
                Err(mpsc::RecvTimeoutError::Timeout) => continue,
            }
        };
        if step.operation != "wait"
            || result.get("matched").and_then(Value::as_bool) == Some(true)
            || matches!(
                result.get("status").and_then(Value::as_str),
                Some("completed" | "timed_out" | "cancelled")
            )
        {
            return Ok(result);
        }
        // Poll misses use short backend waits against the same step and overall
        // deadlines; the timeout is never reset by an MCP/client poll.
        thread::sleep(
            CONTROL_POLL_INTERVAL.min(deadline.saturating_duration_since(Instant::now())),
        );
    }
}

fn validate_step_result(step: &ScenarioStep, value: &Value) -> Result<()> {
    if step.operation == "launch" {
        if find_string_key(value, &["session_id", "sessionId"]).is_none() {
            bail!("launch result did not include a session_id");
        }
        if find_string_key(value, &["run_id", "runId"]).is_none() {
            bail!("launch result did not include an app run_id");
        }
        if has_launch_workspace_option(&step.params)
            && has_workspace_target_reference(&step.params)
            && effective_workspace_root(value).is_none()
        {
            bail!("launch result did not include capabilities.launch.workspace.root");
        }
    }
    if step.operation == "restart" && find_string_key(value, &["run_id", "runId"]).is_none() {
        bail!("restart result did not include the replacement app run_id");
    }
    if matches!(step.operation.as_str(), "assert" | "wait")
        && value.get("matched").and_then(Value::as_bool) == Some(false)
    {
        bail!("{} returned matched:false", step.operation);
    }
    if let Some(expect) = &step.expect {
        let actual = match &expect.pointer {
            Some(pointer) => value
                .pointer(pointer)
                .ok_or_else(|| anyhow!("expected result pointer {pointer:?} was absent"))?,
            None => value,
        };
        if let Some(expected) = &expect.equals
            && actual != expected
        {
            bail!("result expectation failed: expected {expected}, got {actual}");
        }
        if let Some(expected) = expect.truthy {
            let actual_truth = match actual {
                Value::Bool(b) => *b,
                Value::Null => false,
                Value::Number(n) => n.as_f64().is_some_and(|n| n != 0.0),
                Value::String(s) => !s.is_empty(),
                Value::Array(a) => !a.is_empty(),
                Value::Object(o) => !o.is_empty(),
            };
            if actual_truth != expected {
                bail!(
                    "result truthiness expectation failed: expected {expected}, got {actual_truth}"
                );
            }
        }
    }
    Ok(())
}

fn interpolate_value(value: &Value, variables: &BTreeMap<String, String>) -> Result<Value> {
    interpolate_value_at(value, variables, false, false)
}

fn interpolate_launch_params(value: &Value, variables: &BTreeMap<String, String>) -> Result<Value> {
    interpolate_value_at(value, variables, false, true)
}

fn interpolate_value_at(
    value: &Value,
    variables: &BTreeMap<String, String>,
    in_launch_target: bool,
    defer_workspace: bool,
) -> Result<Value> {
    match value {
        Value::String(s) => {
            let mut output = String::with_capacity(s.len());
            let mut rest = s.as_str();
            while let Some(start) = rest.find("${") {
                output.push_str(&rest[..start]);
                let after = &rest[start + 2..];
                let end = after
                    .find('}')
                    .ok_or_else(|| anyhow!("unterminated variable expression"))?;
                let key = &after[..end];
                if key.is_empty() || !key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                    bail!("invalid variable name {key:?}");
                }
                if key == "workspace" && in_launch_target && defer_workspace {
                    output.push_str("${workspace}");
                    rest = &after[end + 1..];
                    continue;
                }
                let resolved = variables
                    .get(key)
                    .ok_or_else(|| anyhow!("undefined scenario variable ${{{key}}}"))?;
                output.push_str(resolved);
                rest = &after[end + 1..];
            }
            output.push_str(rest);
            Ok(Value::String(output))
        }
        Value::Array(items) => Ok(Value::Array(
            items
                .iter()
                .map(|item| {
                    interpolate_value_at(item, variables, in_launch_target, defer_workspace)
                })
                .collect::<Result<Vec<_>>>()?,
        )),
        Value::Object(items) => {
            let mut result = Map::new();
            for (key, item) in items {
                let target = in_launch_target || matches!(key.as_str(), "cwd" | "args" | "argv");
                result.insert(
                    key.clone(),
                    interpolate_value_at(item, variables, target, defer_workspace)?,
                );
            }
            Ok(Value::Object(result))
        }
        _ => Ok(value.clone()),
    }
}

fn update_variables_and_step(
    job: &Job,
    variables: BTreeMap<String, String>,
    outcome: ScenarioStepOutcome,
) {
    let mut report = job
        .report
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    report.variables = variables;
    report.session_id = report.variables.get("session").cloned();
    report.app_run_id = report.variables.get("run_id").cloned();
    report.backend_work_may_continue |= outcome.backend_work_may_continue;
    if outcome.backend_work_may_continue {
        report.backend_operation_may_continue =
            Some(format!("{}:{}", outcome.operation, outcome.step_id));
    }
    report.steps.push(outcome);
    let _ = persist_report(&job.report_path, &report);
}

fn record_step_failure(
    job: &Job,
    variables: &BTreeMap<String, String>,
    step: &ScenarioStep,
    status: ScenarioStatus,
    started_at_ms: u128,
    error: &str,
) {
    update_variables_and_step(
        job,
        variables.clone(),
        ScenarioStepOutcome {
            step_id: step.id.clone(),
            operation: step.operation.clone(),
            status,
            started_at_ms,
            finished_at_ms: now_ms(),
            result: None,
            action_id: None,
            frame_id: None,
            error: Some(error.to_owned()),
            backend_work_may_continue: false,
        },
    );
}

fn set_status(job: &Job, status: ScenarioStatus, failure: Option<String>) {
    let mut report = job
        .report
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    report.status = status;
    if failure.is_some() {
        report.failure_reason = failure;
    }
    let _ = persist_report(&job.report_path, &report);
}

fn finish(job: &Job, status: ScenarioStatus, failure: Option<String>) {
    let mut report = job
        .report
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    report.status = status;
    report.finished_at_ms = Some(now_ms());
    report.failure_reason = failure;
    let _ = persist_report(&job.report_path, &report);
    let completed = report.clone();
    drop(report);
    if let Some(hook) = &job.on_finished
        && let Err(error) = hook(&completed)
    {
        let mut report = job
            .report
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let detail = format!("retention update failed: {error:#}");
        report.failure_reason = Some(match report.failure_reason.take() {
            Some(existing) => format!("{existing}; {detail}"),
            None => detail,
        });
        let _ = persist_report(&job.report_path, &report);
    }
}

#[derive(Clone, Copy)]
enum BackendHook {
    CancelPending,
    CloseOwnedSessions,
}

fn run_backend_hook(backend: Arc<dyn ScenarioBackend>, hook: BackendHook) -> Result<()> {
    let name = match hook {
        BackendHook::CancelPending => "cancel_pending",
        BackendHook::CloseOwnedSessions => "close_owned_sessions",
    };
    let (tx, rx) = mpsc::sync_channel(1);
    thread::Builder::new()
        .name(format!("scenario-hook-{name}"))
        .spawn(move || {
            let result = match hook {
                BackendHook::CancelPending => backend.cancel_pending(),
                BackendHook::CloseOwnedSessions => backend.close_owned_sessions(),
            };
            let _ = tx.send(result);
        })
        .with_context(|| format!("spawn backend hook {name}"))?;
    match rx.recv_timeout(BACKEND_HOOK_LIMIT) {
        Ok(result) => result.with_context(|| format!("backend hook {name}")),
        Err(mpsc::RecvTimeoutError::Timeout) => {
            bail!("backend hook {name} exceeded the one second cleanup deadline; it may continue")
        }
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            bail!("backend hook {name} ended without a result")
        }
    }
}

fn mark_backend_uncertain(job: &Job, operation: &str) {
    let mut report = job
        .report
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    report.backend_work_may_continue = true;
    if report.backend_operation_may_continue.is_none() {
        report.backend_operation_may_continue = Some(operation.to_owned());
    }
    let _ = persist_report(&job.report_path, &report);
}

fn persist_report(path: &Path, report: &ScenarioReport) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("scenario report has no parent"))?;
    fs::create_dir_all(parent)?;
    let temporary = path.with_extension(format!("json.{}.tmp", Ulid::generate()));
    let bytes = serde_json::to_vec_pretty(report)?;
    let mut file = File::create(&temporary)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, path)
        .with_context(|| format!("replace scenario report {}", path.display()))?;
    Ok(())
}

fn find_string_key(value: &Value, keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| value.get(*key).and_then(Value::as_str).map(str::to_owned))
}

fn effective_workspace_root(value: &Value) -> Option<&str> {
    value
        .pointer("/capabilities/launch/workspace/root")
        .and_then(Value::as_str)
        .filter(|root| !root.is_empty())
        .or_else(|| {
            value
                .get("workspace")
                .and_then(Value::as_str)
                .filter(|root| !root.is_empty())
        })
}

fn is_terminal(status: ScenarioStatus) -> bool {
    matches!(
        status,
        ScenarioStatus::Passed
            | ScenarioStatus::Failed
            | ScenarioStatus::Cancelled
            | ScenarioStatus::TimedOut
            | ScenarioStatus::Interrupted
    )
}

fn status_message(status: ScenarioStatus) -> String {
    match status {
        ScenarioStatus::Cancelled => "cancelled by client",
        ScenarioStatus::TimedOut => "overall scenario deadline exceeded",
        _ => "scenario could not continue",
    }
    .to_owned()
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
fn default_overall_timeout() -> u64 {
    DEFAULT_OVERALL_TIMEOUT_MS
}
fn default_step_timeout() -> u64 {
    DEFAULT_STEP_TIMEOUT_MS
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn interpolation_is_literal_and_nested() {
        let vars = BTreeMap::from([("workspace".to_owned(), "/tmp/a; touch nope".to_owned())]);
        let input = json!({"args":["${workspace}", "prefix-${workspace}"]});
        let got = interpolate_value(&input, &vars).unwrap();
        assert_eq!(got["args"][0], "/tmp/a; touch nope");
        assert_eq!(got["args"][1], "prefix-/tmp/a; touch nope");
    }

    #[test]
    fn assertion_and_wait_false_never_pass() {
        let step = ScenarioStep {
            id: "s".into(),
            operation: "assert".into(),
            params: json!({}),
            preconditions: None,
            timeout_ms: 100,
            expect: None,
        };
        assert!(validate_step_result(&step, &json!({"matched":false})).is_err());
    }
}
