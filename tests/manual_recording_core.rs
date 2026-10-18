use anyhow::{Result, bail};
use serde_json::{Value, json};
use silicogen_terminal_runtime::{
    assertions, contracts, engine::EngineState, predicates, protocol, scenario,
};
use std::{
    sync::{Arc, Mutex, mpsc},
    time::Duration,
};
#[path = "../src/locators.rs"]
mod locators;
#[path = "../src/manual_recording.rs"]
mod manual_recording;
use locators::ObservationIdentity;
use manual_recording::*;
use protocol::{ActionContext, AgentAction};
mod validation {
    pub use silicogen_terminal_runtime::validation::*;
}

#[derive(Clone)]
struct Owner(Arc<Mutex<usize>>);
struct Lease(Arc<Mutex<usize>>);
impl Drop for Lease {
    fn drop(&mut self) {
        *self.0.lock().unwrap() -= 1;
    }
}
impl RecordingOwner for Owner {
    type Lease = Lease;
    fn start(&mut self, _: &RecordingOptions) -> Result<Lease> {
        *self.0.lock().unwrap() += 1;
        Ok(Lease(self.0.clone()))
    }
}
fn options(text: bool) -> RecordingOptions {
    RecordingOptions {
        recording_id: "synthetic".into(),
        opted_in: true,
        record_text: text,
        max_events: 64,
        max_bytes: MAX_RECORDING_BYTES,
    }
}
fn recorder(text: bool, secrets: Vec<String>) -> ManualRecorder<Owner, DeclaredSecretRedactor> {
    ManualRecorder::start(
        options(text),
        Owner(Arc::new(Mutex::new(0))),
        DeclaredSecretRedactor::new(secrets).unwrap(),
    )
    .unwrap()
}
fn identity() -> ObservationIdentity {
    ObservationIdentity {
        session_id: "old-session-123".into(),
        run_id: "old-run-123".into(),
        frame_id: "old-frame-123".into(),
        observation_revision: 9,
        viewport_revision: 7,
        cols: 30,
        rows: 3,
    }
}
fn event(order: u64, operation: RecordedOperation) -> RecordedServiceEvent {
    RecordedServiceEvent {
        order,
        timestamp_ms: 50 + order,
        source: RecordingSource::Cli,
        result_identity: identity(),
        operation,
    }
}
fn launch(cwd: &str) -> RecordedOperation {
    RecordedOperation::Launch {
        launch: LaunchMetadata {
            program: "terminal-fixture".into(),
            args: Vec::new(),
            cwd: Some(cwd.into()),
            cols: 30,
            rows: 3,
            workspace: RecordedWorkspace::SelectedIndependentCopy,
            external_environment_required: false,
            external_auth_required: false,
        },
    }
}
fn input(action: AgentAction, viewport: Option<u64>, controller: Option<u64>) -> RecordedInput {
    RecordedInput {
        action,
        context: ActionContext {
            actor_id: "old-actor".into(),
            action_id: "old-action".into(),
            expected_run_id: "old-run-123".into(),
            expected_viewport_revision: viewport,
            controller_generation: controller,
        },
        baseline: identity(),
        written_to_pty: true,
    }
}

#[test]
fn recorder_requires_opt_in_and_owns_lease_before_any_event() {
    let count = Arc::new(Mutex::new(0));
    let mut opts = options(false);
    opts.opted_in = false;
    assert!(
        ManualRecorder::start(
            opts,
            Owner(count.clone()),
            DeclaredSecretRedactor::new(Vec::new()).unwrap()
        )
        .is_err()
    );
    assert_eq!(*count.lock().unwrap(), 0);
    let recorder = ManualRecorder::start(
        options(false),
        Owner(count.clone()),
        DeclaredSecretRedactor::new(Vec::new()).unwrap(),
    )
    .unwrap();
    assert_eq!(*count.lock().unwrap(), 1);
    drop(recorder);
    assert_eq!(*count.lock().unwrap(), 0);
}

#[test]
fn declared_values_are_absent_from_trace_and_export_without_environment_key_heuristics() {
    let values = [
        "synthetic-view-token-123",
        "innocently-named-env-value",
        "/private/auth/synthetic.json",
    ];
    let mut rec = recorder(true, values.iter().map(|s| s.to_string()).collect());
    let mut launch = launch("/old/disposed-copy");
    if let RecordedOperation::Launch { launch } = &mut launch {
        launch.args = values.iter().map(|s| s.to_string()).collect();
        launch.external_auth_required = true;
    }
    rec.record(event(1, launch)).unwrap();
    rec.record(event(
        2,
        RecordedOperation::Input {
            input: input(
                AgentAction::Type {
                    text: format!("Bearer {}", values[0]),
                },
                None,
                None,
            ),
        },
    ))
    .unwrap();
    let exported = rec.export().unwrap();
    let trace = serde_json::to_string(&rec.trace()).unwrap();
    let export = serde_json::to_string(&exported).unwrap();
    for value in values {
        assert!(!trace.contains(value));
        assert!(!export.contains(value));
    }
    assert!(exported.scenario.template_only);
    assert!(!exported.unresolved_bindings.is_empty());
    assert!(!exported.replay_verified);
}

#[test]
fn default_text_policy_and_overflow_are_explicit_and_cannot_export_as_runnable() {
    let mut rec = recorder(false, Vec::new());
    rec.record(event(1, launch("/old/copy"))).unwrap();
    rec.record(event(
        2,
        RecordedOperation::Input {
            input: input(
                AgentAction::Type {
                    text: "private-input".into(),
                },
                None,
                None,
            ),
        },
    ))
    .unwrap();
    assert!(
        !serde_json::to_string(&rec.trace())
            .unwrap()
            .contains("private-input")
    );
    assert!(rec.export().unwrap().scenario.template_only);

    let mut opts = options(true);
    opts.max_events = 1;
    let mut bounded = ManualRecorder::start(
        opts,
        Owner(Arc::new(Mutex::new(0))),
        DeclaredSecretRedactor::new(Vec::new()).unwrap(),
    )
    .unwrap();
    bounded.record(event(1, launch("/copy"))).unwrap();
    assert_eq!(
        bounded
            .record(event(
                3,
                RecordedOperation::Observation {
                    identity: identity(),
                    text: Some("later".into())
                }
            ))
            .unwrap(),
        RecordDisposition::DroppedAtLimit
    );
    let trace = bounded.trace();
    assert_eq!(trace.records.len(), 1);
    assert_eq!(trace.dropped_events, 1);
    assert_eq!(trace.missing_order_units, 1);
    assert!(bounded.export().unwrap().scenario.template_only);
}

#[test]
fn unsupported_action_is_visible_and_keeps_export_template_only() {
    let mut rec = recorder(true, Vec::new());
    rec.record(event(1, launch("/copy"))).unwrap();
    rec.mark_unsupported();
    let exported = rec.export().unwrap();
    assert_eq!(exported.unsupported_actions, 1);
    assert!(exported.scenario.template_only);
    assert!(
        exported
            .unresolved_bindings
            .iter()
            .any(|binding| binding.field == "trace")
    );
}

#[test]
fn byte_budget_stops_retention_and_reordered_events_are_rejected() {
    let mut opts = options(true);
    opts.max_bytes = 1024;
    let mut rec = ManualRecorder::start(
        opts,
        Owner(Arc::new(Mutex::new(0))),
        DeclaredSecretRedactor::new(Vec::new()).unwrap(),
    )
    .unwrap();
    assert_eq!(
        rec.record(event(1, launch("/copy"))).unwrap(),
        RecordDisposition::DroppedAtLimit
    );
    assert!(rec.trace().records.is_empty());
    assert!(rec.trace().bytes_retained <= 1024);
    assert!(rec.record(event(1, launch("/copy"))).is_err());
    assert_eq!(rec.trace().dropped_events, 1);
}

#[test]
fn sequence_order_across_clients_and_unsafe_launch_arguments_stay_explicit() {
    let mut rec = recorder(true, Vec::new());
    let mut started = event(1, launch("/old/copy"));
    started.source = RecordingSource::Mcp;
    if let RecordedOperation::Launch { launch } = &mut started.operation {
        launch.args = vec![
            "/old/copy/input.txt".into(),
            "/unowned/runtime/auth.db".into(),
            "old-session-123".into(),
        ];
    }
    rec.record(started).unwrap();
    let mut sequence = event(
        2,
        RecordedOperation::Sequence {
            steps: vec![
                RecordedSequenceStep::Input {
                    input: input(
                        AgentAction::Key {
                            key: "Ctrl+X".into(),
                        },
                        None,
                        None,
                    ),
                },
                RecordedSequenceStep::Wait {
                    predicate: predicates::PredicateSpec::TextContains {
                        text: "Palette".into(),
                    },
                    baseline: identity(),
                },
                RecordedSequenceStep::Input {
                    input: input(AgentAction::Key { key: "L".into() }, None, None),
                },
            ],
        },
    );
    sequence.source = RecordingSource::Browser;
    rec.record(sequence).unwrap();
    let exported = rec.export().unwrap();
    assert!(exported.scenario.template_only);
    assert_eq!(
        exported
            .scenario
            .steps
            .iter()
            .map(|step| step.operation.as_str())
            .collect::<Vec<_>>(),
        ["launch", "key", "wait", "key"]
    );
    assert_eq!(
        exported.scenario.steps[0].params["args"][0],
        "${workspace}/input.txt"
    );
    let json = serde_json::to_string(&exported).unwrap();
    assert!(!json.contains("/unowned/runtime/auth.db"));
    assert!(!json.contains("old-session-123"));
    contracts::validate_scenario(&exported.scenario).unwrap();
}

#[test]
fn guarded_mouse_resize_and_controller_context_remain_fail_closed_templates() {
    let mut rec = recorder(true, Vec::new());
    rec.record(event(1, launch("/old/copy"))).unwrap();
    rec.record(event(
        2,
        RecordedOperation::Input {
            input: input(
                AgentAction::Mouse {
                    input: protocol::MouseInput::Wheel {
                        row: 1,
                        col: 2,
                        direction: protocol::WheelDirection::Up,
                        steps: 2,
                    },
                },
                Some(7),
                Some(9),
            ),
        },
    ))
    .unwrap();
    rec.record(event(
        3,
        RecordedOperation::Input {
            input: input(AgentAction::Resize { cols: 40, rows: 5 }, Some(7), Some(9)),
        },
    ))
    .unwrap();
    let exported = rec.export().unwrap();
    assert!(exported.scenario.template_only);
    let mouse = &exported.scenario.steps[1];
    assert_eq!(mouse.params["input"]["kind"], "wheel");
    assert_eq!(mouse.params["input"]["direction"], "up");
    assert_eq!(
        mouse
            .preconditions
            .as_ref()
            .unwrap()
            .expected_viewport_revision,
        Some(u64::MAX)
    );
    assert!(
        exported
            .unresolved_bindings
            .iter()
            .any(|binding| binding.field == "controller_generation")
    );
    contracts::validate_scenario(&exported.scenario).unwrap();
    assert_eq!(
        exported.scenario.steps[2]
            .preconditions
            .as_ref()
            .unwrap()
            .expected_viewport_revision,
        Some(u64::MAX)
    );
}

struct FakeBackend {
    engine: Mutex<EngineState>,
    workspace: std::path::PathBuf,
    calls: Mutex<Vec<(String, Value)>>,
}
impl scenario::ScenarioBackend for FakeBackend {
    fn execute(&self, operation: &str, params: Value) -> Result<Value> {
        contracts::validate_scenario_params(operation, params.clone())?;
        self.calls
            .lock()
            .unwrap()
            .push((operation.into(), params.clone()));
        let mut engine = self.engine.lock().unwrap();
        if operation == "launch" {
            assert_eq!(params["cwd"], self.workspace.to_string_lossy().as_ref());
            engine.process_output(b"READY");
            return Ok(json!({"session_id":"new-session", "run_id":"new-run"}));
        }
        assert_eq!(params["session_id"], "new-session");
        match operation {
            "key" | "type" => {
                assert_eq!(params["_preconditions"]["expected_run_id"], "new-run");
                if operation == "key" {
                    engine.process_output(b"\r\nPALETTE");
                } else {
                    let text = params["text"].as_str().unwrap();
                    engine.process_output(text.as_bytes());
                    std::fs::write(self.workspace.join("fixture-result.txt"), text)?;
                }
                Ok(json!({"accepted":true,"frame_id":"new-input-frame"}))
            }
            "wait" => {
                let predicate = serde_json::from_value(params["predicate"].clone())?;
                let result = predicates::evaluate(
                    &engine.snapshot("new-session", "new-run", "running"),
                    &predicate,
                )?;
                Ok(serde_json::to_value(result)?)
            }
            "assert" => {
                let assertion = serde_json::from_value(params["assertion"].clone())?;
                Ok(serde_json::to_value(assertions::evaluate(
                    &engine.snapshot("new-session", "new-run", "running"),
                    &assertion,
                )?)?)
            }
            "observe" => Ok(serde_json::to_value(engine.snapshot(
                "new-session",
                "new-run",
                "running",
            ))?),
            _ => bail!("unexpected fake operation"),
        }
    }
}

#[test]
fn record_export_replays_with_scenario_manager_after_old_copy_disposal_in_new_workspace() {
    std::fs::create_dir_all(".runtime").unwrap();
    let root = tempfile::TempDir::new_in(".runtime").unwrap();
    let source = root.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("sentinel.txt"), "source-unchanged").unwrap();
    let old = tempfile::TempDir::new_in(root.path()).unwrap();
    std::fs::copy(source.join("sentinel.txt"), old.path().join("sentinel.txt")).unwrap();
    let mut rec = recorder(true, Vec::new());
    rec.record(event(1, launch(old.path().to_str().unwrap())))
        .unwrap();
    rec.record(event(
        2,
        RecordedOperation::Input {
            input: input(
                AgentAction::Key {
                    key: "Ctrl+P".into(),
                },
                None,
                None,
            ),
        },
    ))
    .unwrap();
    rec.record(event(
        3,
        RecordedOperation::Wait {
            predicate: predicates::PredicateSpec::TextContains {
                text: "PALETTE".into(),
            },
            baseline: identity(),
        },
    ))
    .unwrap();
    rec.record(event(
        4,
        RecordedOperation::Input {
            input: input(
                AgentAction::Type {
                    text: "query".into(),
                },
                None,
                None,
            ),
        },
    ))
    .unwrap();
    rec.record(event(
        5,
        RecordedOperation::Assert {
            assertion: assertions::FrameAssertion::TextContains {
                text: "query".into(),
            },
            baseline: identity(),
        },
    ))
    .unwrap();
    rec.record(event(
        6,
        RecordedOperation::Observation {
            identity: identity(),
            text: None,
        },
    ))
    .unwrap();
    // Simulated finalization/disposal of this core test's independently owned copy.
    // Actual service finalize hooks remain integration acceptance.
    let old_path = old.path().to_owned();
    drop(old);
    assert!(!old_path.exists());
    let exported = rec.export().unwrap();
    assert!(!exported.scenario.template_only);
    contracts::validate_scenario(&exported.scenario).unwrap();
    let export_artifact = serde_json::to_value(&exported).unwrap();
    let encoded = serde_json::to_string(&export_artifact["scenario"]).unwrap();
    let replay_scenario = scenario::ScenarioManager::validate_json(&encoded).unwrap();
    assert!(!encoded.contains(old_path.to_str().unwrap()));
    for value in ["old-session-123", "old-run-123", "old-frame-123"] {
        assert!(!encoded.contains(value));
    }
    let selected = root.path().join("new-copy");
    std::fs::create_dir(&selected).unwrap();
    std::fs::copy(source.join("sentinel.txt"), selected.join("sentinel.txt")).unwrap();
    let runner = scenario::ScenarioManager::new(root.path().join("reports")).unwrap();
    let backend = Arc::new(FakeBackend {
        engine: Mutex::new(EngineState::new(30, 3).unwrap()),
        workspace: selected.clone(),
        calls: Mutex::new(Vec::new()),
    });
    let (tx, rx) = mpsc::sync_channel(1);
    let hook: scenario::ScenarioLifecycleHook = Arc::new(move |report| {
        tx.send(report.clone()).unwrap();
        Ok(())
    });
    runner
        .start_with_hooks(
            replay_scenario,
            backend.clone(),
            Some(selected.clone()),
            None,
            Some(hook),
        )
        .unwrap();
    let report = rx.recv_timeout(Duration::from_secs(3)).unwrap();
    assert_eq!(
        report.status,
        scenario::ScenarioStatus::Passed,
        "{:?}",
        report.failure_reason
    );
    assert_eq!(
        std::fs::read_to_string(selected.join("fixture-result.txt")).unwrap(),
        "query"
    );
    assert!(!source.join("fixture-result.txt").exists());
    assert_eq!(
        std::fs::read_to_string(source.join("sentinel.txt")).unwrap(),
        "source-unchanged"
    );
}
