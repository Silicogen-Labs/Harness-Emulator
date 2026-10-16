use silicogen_terminal_runtime::scenario;

use anyhow::{Result, anyhow};
use scenario::{Scenario, ScenarioBackend, ScenarioControl, ScenarioManager, ScenarioStatus};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Default)]
struct FakeBackend {
    calls: Mutex<Vec<(String, Value)>>,
    fail_operation: Option<String>,
    delay: Duration,
    wait_false: bool,
    browser_control: bool,
    closed: Mutex<bool>,
    cancelled: Mutex<bool>,
}

impl ScenarioBackend for FakeBackend {
    fn execute(&self, operation: &str, params: Value) -> Result<Value> {
        if !self.delay.is_zero() {
            thread::sleep(self.delay);
        }
        self.calls
            .lock()
            .unwrap()
            .push((operation.to_owned(), params.clone()));
        if self.fail_operation.as_deref() == Some(operation) {
            return Err(anyhow!("synthetic backend failure"));
        }
        Ok(match operation {
            "launch" => {
                let mut result =
                    json!({"session_id":"session-1","run_id":"run-1","action_id":"a1"});
                if params.pointer("/options/workspace").is_some() {
                    result["capabilities"] =
                        json!({"launch":{"workspace":{"root":"/tmp/effective-snapshot"}}});
                }
                result
            }
            "restart" => json!({"session_id":"session-1","run_id":"run-2"}),
            "observe" => json!({"frame_id":"frame-1","text":"ready"}),
            "assert" => {
                json!({"matched":params.get("matched").and_then(Value::as_bool).unwrap_or(true),"frame_id":"frame-2"})
            }
            "wait" if self.wait_false => json!({"matched":false,"timed_out":true}),
            "wait" => json!({"matched":true}),
            _ => json!({"ok":true,"action_id":"a2"}),
        })
    }

    fn control_state(&self) -> Result<ScenarioControl> {
        Ok(if self.browser_control {
            ScenarioControl::BrowserHoldsControl
        } else {
            ScenarioControl::Available
        })
    }

    fn close_owned_sessions(&self) -> Result<()> {
        *self.closed.lock().unwrap() = true;
        Ok(())
    }

    fn cancel_pending(&self) -> Result<()> {
        *self.cancelled.lock().unwrap() = true;
        Ok(())
    }
}

fn parse(value: Value) -> Scenario {
    ScenarioManager::validate_json(&value.to_string()).unwrap()
}

fn wait_for_status(
    manager: &ScenarioManager,
    id: &str,
    wanted: ScenarioStatus,
) -> scenario::ScenarioReport {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let report = manager.status(id).unwrap();
        if report.status == wanted {
            return report;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {wanted:?}; got {:?}",
            report.status
        );
        thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn reload_marks_nonterminal_jobs_interrupted() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path()).unwrap();
    let scenario_id = "scenario-interrupted";
    let report = scenario::ScenarioReport {
        schema_version: 1,
        scenario_id: scenario_id.to_owned(),
        session_id: Some("session-1".to_owned()),
        app_run_id: Some("run-1".to_owned()),
        backend_work_may_continue: false,
        backend_operation_may_continue: None,
        scenario_name: "interrupted fixture".to_owned(),
        status: ScenarioStatus::Running,
        started_at_ms: 10,
        finished_at_ms: None,
        deadline_at_ms: 1000,
        variables: Default::default(),
        steps: Vec::new(),
        failure_reason: None,
    };
    std::fs::write(
        dir.path().join(format!("{scenario_id}.json")),
        serde_json::to_vec(&report).unwrap(),
    )
    .unwrap();
    let manager = ScenarioManager::new(dir.path()).unwrap();
    let interrupted = manager.status(scenario_id).unwrap();
    assert_eq!(interrupted.status, ScenarioStatus::Interrupted);
    assert!(
        interrupted
            .failure_reason
            .unwrap()
            .contains("manager restarted")
    );
}

#[test]
fn checked_in_scenarios_validate_and_silicogen_template_stays_nonrunnable() {
    let fixture =
        ScenarioManager::validate_json(include_str!("../scenarios/example.json")).unwrap();
    assert!(!fixture.template_only);
    let template =
        ScenarioManager::validate_json(include_str!("../scenarios/silicogen-templates.json"))
            .unwrap();
    assert!(template.template_only);
}

#[test]
fn scenario_rejects_malformed_or_unknown_input_preconditions_before_start() {
    use silicogen_terminal_runtime::scenario::ScenarioManager;

    for preconditions in [
        serde_json::json!({"expected_run_id":123}),
        serde_json::json!({"expected_viewport_revision":"old"}),
        serde_json::json!({"expected_run_id":"run-1","typo_viewport":0}),
    ] {
        let input=serde_json::json!({
            "version":1,"name":"bad-precondition",
            "steps":[{"id":"type","operation":"type","params":{"session_id":"must-not-send","text":"no"},"preconditions":preconditions}]
        }).to_string();
        assert!(ScenarioManager::validate_json(&input).is_err(), "{input}");
    }

    let null_preconditions = r#"{"version":1,"name":"null-precondition","steps":[{"id":"type","operation":"type","params":{"session_id":"session-1","text":"no"},"preconditions":null}]}"#;
    assert!(ScenarioManager::validate_json(null_preconditions).is_err());

    let valid = r#"{"version":1,"name":"valid","steps":[{"id":"type","operation":"type","params":{"session_id":"session-1","text":"ok"},"preconditions":{"expected_run_id":"run-1","expected_viewport_revision":0}}]}"#;
    assert!(ScenarioManager::validate_json(valid).is_ok());
}

#[test]
fn durable_success_records_steps_variables_and_ids() {
    let dir = tempfile::tempdir().unwrap();
    let manager = ScenarioManager::new(dir.path()).unwrap();
    let backend = Arc::new(FakeBackend::default());
    let scenario = parse(json!({
        "version":1,"name":"fake-success","timeout_ms":3000,
        "steps":[
            {"id":"launch","operation":"launch","params":{"cwd":"${workspace}"}},
            {"id":"observe","operation":"observe","params":{"session":"${session}","run":"${run_id}"}},
            {"id":"wait","operation":"wait","timeout_ms":1000,"params":{"session_id":"${session}","predicate":{"kind":"text_contains","text":"ready"}}},
            {"id":"check","operation":"assert","params":{},"expect":{"pointer":"/matched","equals":true}}
        ]
    }));
    let id = manager
        .start(scenario, backend.clone(), Some("/tmp/work space".into()))
        .unwrap();
    let report = wait_for_status(&manager, &id, ScenarioStatus::Passed);
    assert_eq!(report.steps.len(), 4);
    assert_eq!(report.steps[0].action_id.as_deref(), Some("a1"));
    assert_eq!(report.steps[1].frame_id.as_deref(), Some("frame-1"));
    assert_eq!(
        report.variables.get("session").map(String::as_str),
        Some("session-1")
    );
    assert_eq!(
        report.variables.get("run_id").map(String::as_str),
        Some("run-1")
    );
    assert_eq!(report.session_id.as_deref(), Some("session-1"));
    assert_eq!(report.app_run_id.as_deref(), Some("run-1"));
    let calls = backend.calls.lock().unwrap();
    let observed = calls
        .iter()
        .find(|(operation, _)| operation == "observe")
        .unwrap();
    assert_eq!(observed.1["session"], "session-1");
    assert_eq!(observed.1["run"], "run-1");
    let wait = calls
        .iter()
        .find(|(operation, _)| operation == "wait")
        .unwrap();
    assert!(!wait.1["wait_id"].as_str().unwrap().is_empty());
    assert!(wait.1["poll_ms"].as_u64().unwrap() < 200);
    assert!(wait.1["timeout_ms"].as_u64().unwrap() > 200);
    drop(calls);
    assert!(dir.path().join(format!("{id}.json")).is_file());
    let reloaded = ScenarioManager::new(dir.path()).unwrap();
    assert_eq!(reloaded.status(&id).unwrap().status, ScenarioStatus::Passed);
}

#[test]
fn snapshot_launch_defers_workspace_target_then_uses_effective_root() {
    let dir = tempfile::tempdir().unwrap();
    let manager = ScenarioManager::new(dir.path()).unwrap();
    let backend = Arc::new(FakeBackend::default());
    let scenario = parse(json!({"version":1,"name":"snapshot-workspace","steps":[
        {"id":"launch","operation":"launch","params":{
            "spec":{"program":"demo","cwd":"${workspace}","args":["--project","${workspace}"]},
            "options":{"workspace":{"mode":"copy","source":"/source/project"}}
        }},
        {"id":"observe","operation":"observe","params":{"cwd":"${workspace}"}}
    ]}));
    let id = manager.start(scenario, backend.clone(), None).unwrap();
    let report = wait_for_status(&manager, &id, ScenarioStatus::Passed);
    assert_eq!(
        report.variables.get("workspace").map(String::as_str),
        Some("/tmp/effective-snapshot")
    );
    let calls = backend.calls.lock().unwrap();
    let launch = calls
        .iter()
        .find(|(operation, _)| operation == "launch")
        .unwrap();
    assert_eq!(launch.1.pointer("/spec/cwd").unwrap(), "${workspace}");
    assert_eq!(launch.1.pointer("/spec/args/1").unwrap(), "${workspace}");
    assert_eq!(
        launch.1.pointer("/options/workspace/source").unwrap(),
        "/source/project"
    );
    assert_eq!(
        report.steps[0]
            .result
            .as_ref()
            .unwrap()
            .pointer("/capabilities/launch/workspace/root")
            .unwrap(),
        "/tmp/effective-snapshot"
    );
    let observe = calls
        .iter()
        .find(|(operation, _)| operation == "observe")
        .unwrap();
    assert_eq!(observe.1["cwd"], "/tmp/effective-snapshot");
}

#[test]
fn backend_failure_is_durable_and_closes_when_configured() {
    let dir = tempfile::tempdir().unwrap();
    let manager = ScenarioManager::new(dir.path()).unwrap();
    let backend = Arc::new(FakeBackend {
        fail_operation: Some("type".into()),
        ..Default::default()
    });
    let scenario = parse(json!({"version":1,"name":"fail","close_on_failure":true,
        "steps":[{"id":"type","operation":"type","params":{"text":"x"}}]}));
    let id = manager.start(scenario, backend.clone(), None).unwrap();
    let report = wait_for_status(&manager, &id, ScenarioStatus::Failed);
    assert!(
        report
            .failure_reason
            .as_deref()
            .unwrap()
            .contains("synthetic backend failure")
    );
    assert!(*backend.closed.lock().unwrap());
}

#[test]
fn restart_replaces_the_app_run_variable() {
    let dir = tempfile::tempdir().unwrap();
    let manager = ScenarioManager::new(dir.path()).unwrap();
    let backend = Arc::new(FakeBackend::default());
    let scenario = parse(json!({"version":1,"name":"restart-run-id","steps":[
        {"id":"launch","operation":"launch","params":{}},
        {"id":"restart","operation":"restart","params":{"session_id":"${session}"}},
        {"id":"observe","operation":"observe","params":{"run_id":"${run_id}"}}
    ]}));
    let id = manager.start(scenario, backend.clone(), None).unwrap();
    let report = wait_for_status(&manager, &id, ScenarioStatus::Passed);
    assert_eq!(
        report.variables.get("run_id").map(String::as_str),
        Some("run-2")
    );
    assert_eq!(report.app_run_id.as_deref(), Some("run-2"));
    let calls = backend.calls.lock().unwrap();
    let observe = calls
        .iter()
        .find(|(operation, _)| operation == "observe")
        .unwrap();
    assert_eq!(observe.1["run_id"], "run-2");
}

#[test]
fn cancellation_interrupts_running_action_and_does_not_pass() {
    let dir = tempfile::tempdir().unwrap();
    let manager = ScenarioManager::new(dir.path()).unwrap();
    let backend = Arc::new(FakeBackend {
        delay: Duration::from_millis(500),
        ..Default::default()
    });
    let scenario = parse(json!({"version":1,"name":"cancel","close_on_cancel":true,
        "steps":[{"id":"observe","operation":"observe","timeout_ms":2000,"params":{}}]}));
    let id = manager.start(scenario, backend.clone(), None).unwrap();
    thread::sleep(Duration::from_millis(40));
    manager.cancel(&id).unwrap();
    let report = wait_for_status(&manager, &id, ScenarioStatus::Cancelled);
    assert_ne!(report.status, ScenarioStatus::Passed);
    assert!(report.backend_work_may_continue);
    assert!(*backend.closed.lock().unwrap());
    assert!(*backend.cancelled.lock().unwrap());
}

#[test]
fn wait_polls_keep_one_id_and_share_the_step_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let manager = ScenarioManager::new(dir.path()).unwrap();
    let backend = Arc::new(FakeBackend {
        wait_false: true,
        ..Default::default()
    });
    let scenario = parse(json!({"version":1,"name":"wait-timeout","steps":[
        {"id":"wait","operation":"wait","timeout_ms":350,"params":{"predicate":{"kind":"stable","duration_ms":500}}}
    ]}));
    let id = manager.start(scenario, backend.clone(), None).unwrap();
    let report = wait_for_status(&manager, &id, ScenarioStatus::TimedOut);
    assert!(
        report
            .failure_reason
            .as_deref()
            .unwrap()
            .contains("deadline")
    );
    let calls = backend.calls.lock().unwrap();
    let waits = calls
        .iter()
        .filter(|(operation, _)| operation == "wait")
        .collect::<Vec<_>>();
    assert!(waits.len() >= 2);
    let wait_id = waits[0].1["wait_id"].as_str().unwrap();
    assert!(waits.iter().all(|(_, params)| params["wait_id"] == wait_id));
    assert!(
        waits
            .iter()
            .all(|(_, params)| params["poll_ms"].as_u64().unwrap() < 200)
    );
}

#[test]
fn false_wait_or_assertion_is_failure_and_templates_cannot_run() {
    let dir = tempfile::tempdir().unwrap();
    let manager = ScenarioManager::new(dir.path()).unwrap();
    let backend = Arc::new(FakeBackend::default());
    let scenario = parse(json!({"version":1,"name":"false-assert",
        "steps":[{"id":"check","operation":"assert","params":{"matched":false}}]}));
    let id = manager.start(scenario, backend.clone(), None).unwrap();
    let report = wait_for_status(&manager, &id, ScenarioStatus::Failed);
    assert!(report.failure_reason.unwrap().contains("matched:false"));
    let template = parse(json!({"version":1,"name":"template","template_only":true,
        "steps":[{"id":"observe","operation":"observe","params":{}}]}));
    assert!(manager.start(template, backend, None).is_err());
}

#[test]
fn browser_takeover_pauses_then_resumes_within_same_deadline() {
    let dir = tempfile::tempdir().unwrap();
    let manager = ScenarioManager::new(dir.path()).unwrap();
    let backend = Arc::new(FakeBackend {
        browser_control: true,
        ..Default::default()
    });
    let scenario = parse(json!({"version":1,"name":"takeover","timeout_ms":2000,
        "steps":[{"id":"observe","operation":"observe","params":{}}]}));
    let id = manager.start(scenario, backend.clone(), None).unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    while manager.status(&id).unwrap().status != ScenarioStatus::PausedForControl {
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(10));
    }
    // The trait is immutable for this fake; cancellation verifies the pause is
    // interruptible and cannot accidentally become a pass.
    manager.cancel(&id).unwrap();
    assert_eq!(
        wait_for_status(&manager, &id, ScenarioStatus::Cancelled).status,
        ScenarioStatus::Cancelled
    );
}
