use base64::Engine;
use serde_json::{Value, json};
use silicogen_terminal_runtime::config::{LaunchOptions, StateMode, StateSeed};
use silicogen_terminal_runtime::protocol::{
    ActionContext, AgentAction, FrameSnapshot, LaunchSpec, RpcRequest,
};
use silicogen_terminal_runtime::service::SessionService;
use std::collections::BTreeMap;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

struct ServiceCleanup(SessionService);

impl Drop for ServiceCleanup {
    fn drop(&mut self) {
        let _ = self.0.shutdown();
    }
}

fn setup() -> (TempDir, SessionService, ServiceCleanup) {
    let temp = TempDir::new().unwrap();
    let service = SessionService::new(temp.path().join("runtime")).unwrap();
    let cleanup = ServiceCleanup(service.clone());
    (temp, service, cleanup)
}

fn fixture_spec(cwd: &Path) -> LaunchSpec {
    LaunchSpec {
        name: Some("agent-control-e2e".into()),
        program: env!("CARGO_BIN_EXE_terminal-fixture").into(),
        args: Vec::new(),
        cwd: cwd.to_string_lossy().into_owned(),
        env: BTreeMap::new(),
        cols: 80,
        rows: 24,
        record_output: true,
    }
}

fn launch(service: &SessionService, spec: LaunchSpec, options: LaunchOptions) -> (String, Value) {
    let value = service
        .handle(RpcRequest::LaunchConfigured { spec, options })
        .unwrap()
        .0;
    let session_id = value["session_id"].as_str().unwrap().to_owned();
    (session_id, value)
}

fn agent(service: &SessionService, operation: &str, params: Value) -> anyhow::Result<Value> {
    Ok(service
        .handle(RpcRequest::Agent {
            operation: operation.into(),
            params,
        })?
        .0)
}

fn observe(service: &SessionService, session_id: &str) -> FrameSnapshot {
    serde_json::from_value(
        service
            .handle(RpcRequest::Observe {
                session_id: session_id.into(),
            })
            .unwrap()
            .0,
    )
    .unwrap()
}

fn wait_text(service: &SessionService, session_id: &str, text: &str) -> FrameSnapshot {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let frame = observe(service, session_id);
        if frame.text.contains(text) {
            return frame;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {text:?}; last frame was {:?}",
            frame.text
        );
        thread::sleep(Duration::from_millis(20));
    }
}

fn action_context(
    service: &SessionService,
    session_id: &str,
    actor_id: &str,
    action_id: &str,
) -> ActionContext {
    let capabilities = agent(service, "capabilities", json!({"session_id":session_id})).unwrap();
    ActionContext {
        actor_id: actor_id.into(),
        action_id: action_id.into(),
        expected_run_id: capabilities["session"]["run_id"].as_str().unwrap().into(),
        expected_viewport_revision: Some(capabilities["viewport_revision"].as_u64().unwrap()),
        controller_generation: None,
    }
}

fn batch(
    service: &SessionService,
    session_id: &str,
    context: &ActionContext,
    actions: &[AgentAction],
) -> anyhow::Result<Value> {
    agent(
        service,
        "batch",
        json!({
            "session_id": session_id,
            "context": context,
            "actions": actions,
        }),
    )
}

#[test]
fn launch_configured_copies_synthetic_user_state_and_agents_can_read_logs_and_frames() {
    let (temp, service, _cleanup) = setup();
    let cwd = temp.path().join("work");
    fs::create_dir_all(&cwd).unwrap();
    let synthetic_auth = temp.path().join("synthetic-auth.json");
    fs::write(
        &synthetic_auth,
        r#"{"token":"synthetic-only-not-a-real-credential"}"#,
    )
    .unwrap();
    let options = LaunchOptions {
        state_mode: StateMode::CopyUser,
        seeds: vec![StateSeed {
            source: synthetic_auth.clone(),
            destination: PathBuf::from("data/silicogen/auth.json"),
            include_credentials: true,
        }],
        disposable: true,
        ..LaunchOptions::default()
    };
    let (session_id, launched) = launch(&service, fixture_spec(&cwd), options);
    assert_eq!(
        launched["capabilities"]["launch"]["state_mode"],
        "copy_user"
    );
    assert_eq!(
        launched["capabilities"]["launch"]["credential_seed_count"],
        1
    );
    wait_text(&service, &session_id, "READY");

    let session_state = temp
        .path()
        .join("runtime/sessions")
        .join(&session_id)
        .join("state");
    let copied_auth = session_state.join("data/silicogen/auth.json");
    assert_eq!(
        fs::read_to_string(&copied_auth).unwrap(),
        fs::read_to_string(&synthetic_auth).unwrap()
    );
    fs::write(&copied_auth, "changed only inside disposable session").unwrap();
    assert!(
        fs::read_to_string(&synthetic_auth)
            .unwrap()
            .contains("synthetic-only-not-a-real-credential")
    );

    let log_dir = session_state.join("data/silicogen/log");
    fs::create_dir_all(&log_dir).unwrap();
    fs::write(
        log_dir.join("dev.log"),
        "synthetic startup log line\nsecond line\n",
    )
    .unwrap();
    let log = agent(
        &service,
        "logs",
        json!({"session_id":session_id,"source":"app","path":"dev.log","max_bytes":1024}),
    )
    .unwrap();
    assert!(
        log["text"]
            .as_str()
            .unwrap()
            .contains("synthetic startup log line")
    );
    assert_eq!(log["gap"], Value::Null);
    let artifact_id = log["artifact"]["artifact_id"].as_str().unwrap();
    let read = agent(
        &service,
        "read_artifact",
        json!({"session_id":session_id,"artifact_id":artifact_id,"offset":0,"length":128}),
    )
    .unwrap();
    assert!(read["text"].as_str().unwrap().contains("second line"));
    assert_eq!(read["eof"], true);
    fs::OpenOptions::new()
        .append(true)
        .open(log_dir.join("dev.log"))
        .unwrap()
        .write_all(b"appended after cursor\n")
        .unwrap();
    let follow = agent(
        &service,
        "logs",
        json!({
            "session_id":session_id,
            "source":"app",
            "path":"dev.log",
            "cursor":log["cursor"],
            "max_bytes":1024
        }),
    )
    .unwrap();
    assert_eq!(follow["text"], "appended after cursor\n");

    let screenshot = service
        .handle(RpcRequest::Screenshot {
            session_id: session_id.clone(),
        })
        .unwrap()
        .0;
    let screenshot_id = screenshot["artifact_id"].as_str().unwrap();
    let direct_read = agent(
        &service,
        "read_artifact",
        json!({"session_id":session_id,"artifact_id":screenshot_id,"length":16}),
    )
    .unwrap();
    assert_eq!(direct_read["encoding"], "base64");
    assert!(
        base64::engine::general_purpose::STANDARD
            .decode(direct_read["data"].as_str().unwrap())
            .unwrap()
            .starts_with(b"\x89PNG")
    );
    let artifacts = agent(&service, "artifacts", json!({"session_id":session_id})).unwrap();
    let listed = artifacts.as_array().expect("artifact list is an array");
    assert!(
        listed
            .iter()
            .any(|item| item["source_kind"] == "screenshot" && item["artifact_id"] == screenshot_id)
    );
    assert!(
        listed
            .iter()
            .any(|item| item["source_kind"] == "application_log")
    );

    let baseline = agent(
        &service,
        "save_frame",
        json!({"session_id":session_id,"name":"initial-ready"}),
    )
    .unwrap();
    assert!(baseline["baseline_id"].as_str().is_some());
    let assertion = agent(
        &service,
        "assert",
        json!({"session_id":session_id,"assertion":{"kind":"text_contains","text":"READY"}}),
    )
    .unwrap();
    assert_eq!(assertion["passed"], true);
    let unchanged = agent(
        &service,
        "diff",
        json!({"session_id":session_id,"name":"initial-ready"}),
    )
    .unwrap();
    assert_eq!(unchanged["passed"], true);
    assert_eq!(unchanged["changed_cell_count"], 0);

    let input_context = action_context(&service, &session_id, "frame-agent", "select-beta");
    batch(
        &service,
        &session_id,
        &input_context,
        &[AgentAction::Key { key: "Down".into() }],
    )
    .unwrap();
    wait_text(&service, &session_id, "Selection: Beta");
    let selected = agent(
        &service,
        "assert",
        json!({"session_id":session_id,"assertion":{"kind":"text_contains","text":"Selection: Beta"}}),
    )
    .unwrap();
    assert_eq!(selected["passed"], true);
    let changed = agent(
        &service,
        "diff",
        json!({"session_id":session_id,"name":"initial-ready"}),
    )
    .unwrap();
    assert_eq!(changed["passed"], false);
    assert!(changed["changed_cell_count"].as_u64().unwrap() > 0);
}

#[test]
fn agent_wait_resumes_one_deadline_and_distinguishes_output_quiet_from_stable_text() {
    let (temp, service, _cleanup) = setup();
    let cwd = temp.path().join("work");
    fs::create_dir_all(&cwd).unwrap();
    let spec = LaunchSpec {
        program: "/bin/sh".into(),
        args: vec![
            "-c".into(),
            "printf 'working'; i=0; while [ \"$i\" -lt 20 ]; do printf '\\rworking'; sleep 0.05; i=$((i+1)); done; printf '\\rREADY\\n'; exec sleep 3".into(),
        ],
        ..fixture_spec(&cwd)
    };
    let (session_id, _) = launch(
        &service,
        spec,
        LaunchOptions {
            disposable: true,
            ..LaunchOptions::default()
        },
    );

    let stable = agent(
        &service,
        "wait",
        json!({
            "session_id":session_id,
            "predicate":{"kind":"stable_text","duration_ms":100},
            "timeout_ms":2500,
            "poll_ms":180
        }),
    )
    .unwrap();
    assert_eq!(stable["status"], "completed");
    assert_eq!(stable["matched"], true);

    let quiet_first = agent(
        &service,
        "wait",
        json!({
            "session_id":session_id,
            "predicate":{"kind":"output_quiet","duration_ms":160},
            "timeout_ms":2500,
            "poll_ms":150
        }),
    )
    .unwrap();
    assert_eq!(quiet_first["status"], "pending");
    assert_eq!(quiet_first["matched"], false);
    let wait_id = quiet_first["wait_id"].as_str().unwrap();
    let conflict = agent(&service, "wait", json!({"wait_id":wait_id,"predicate":{"kind":"text_contains","text":"different-condition"}})).unwrap_err();
    assert!(conflict.to_string().contains("wait_conflict"));
    let deadline = Instant::now() + Duration::from_secs(3);
    let completed = loop {
        let result = agent(&service, "wait", json!({"wait_id":wait_id,"poll_ms":150})).unwrap();
        if result["status"] == "completed" {
            break result;
        }
        assert_eq!(result["status"], "pending");
        assert!(
            Instant::now() < deadline,
            "resumable wait did not complete: {result}"
        );
    };
    assert_eq!(completed["outcome"], "quiet");
    assert!(completed["elapsed_ms"].as_u64().unwrap() >= 160);
}

#[test]
fn agent_batch_deduplicates_actions_and_rejects_stale_viewport_and_run() {
    let (temp, service, _cleanup) = setup();
    let cwd = temp.path().join("work");
    fs::create_dir_all(&cwd).unwrap();
    let (session_id, _) = launch(
        &service,
        fixture_spec(&cwd),
        LaunchOptions {
            disposable: true,
            ..LaunchOptions::default()
        },
    );
    wait_text(&service, &session_id, "READY");

    let context = action_context(&service, &session_id, "test-agent", "type-once");
    let actions = [AgentAction::Type {
        text: "once".into(),
    }];
    let first = batch(&service, &session_id, &context, &actions).unwrap();
    let replay = batch(&service, &session_id, &context, &actions).unwrap();
    assert_eq!(first["results"], replay["results"]);
    let typed = wait_text(&service, &session_id, "Typed: once");
    assert!(!typed.text.contains("onceonce"));

    let old_context = action_context(&service, &session_id, "test-agent", "resize-first");
    batch(
        &service,
        &session_id,
        &old_context,
        &[AgentAction::Resize {
            cols: 100,
            rows: 30,
        }],
    )
    .unwrap();
    let stale_viewport = ActionContext {
        action_id: "resize-stale-viewport".into(),
        ..old_context.clone()
    };
    let error = batch(
        &service,
        &session_id,
        &stale_viewport,
        &[AgentAction::Resize { cols: 90, rows: 28 }],
    )
    .unwrap_err();
    assert!(error.to_string().contains("stale_viewport"), "{error:#}");

    let anonymous_input = service
        .handle(RpcRequest::Key {
            session_id: session_id.clone(),
            key: "Down".into(),
        })
        .unwrap_err();
    assert!(
        anonymous_input
            .to_string()
            .contains("control_held_by_actor")
    );
    let anonymous_restart = service
        .handle(RpcRequest::Restart {
            session_id: session_id.clone(),
        })
        .unwrap_err();
    assert!(
        anonymous_restart
            .to_string()
            .contains("control_held_by_actor")
    );
    let restarted = agent(
        &service,
        "restart",
        json!({"session_id":session_id,"actor_id":"test-agent"}),
    )
    .unwrap();
    assert_ne!(restarted["run_id"], old_context.expected_run_id);
    let stale_run = ActionContext {
        action_id: "type-stale-run".into(),
        ..old_context
    };
    let error = batch(
        &service,
        &session_id,
        &stale_run,
        &[AgentAction::Type {
            text: "must-not-send".into(),
        }],
    )
    .unwrap_err();
    assert!(error.to_string().contains("stale_run"), "{error:#}");
}

#[test]
fn scenario_can_start_report_status_and_cancel_through_shared_service() {
    let (temp, service, _cleanup) = setup();
    let cwd = temp.path().join("work");
    fs::create_dir_all(&cwd).unwrap();
    let (session_id, _) = launch(
        &service,
        fixture_spec(&cwd),
        LaunchOptions {
            disposable: true,
            ..LaunchOptions::default()
        },
    );
    wait_text(&service, &session_id, "READY");

    let scenario = json!({
        "version":1,
        "name":"cancel-resumable-wait",
        "timeout_ms":10000,
        "close_on_cancel":false,
        "steps":[
            {"id":"wait","operation":"wait","timeout_ms":9000,"params":{
                "session_id":session_id,
                "timeout_ms":8000,
                "predicate":{"kind":"ready","required":["MARKER_THAT_NEVER_APPEARS"],"busy":[],"crash":[]}
            }}
        ]
    });
    let started = agent(&service, "scenario_run", json!({"scenario":scenario})).unwrap();
    assert_eq!(started["status"], "queued");
    let job_id = started["job_id"].as_str().unwrap().to_owned();

    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let status = agent(&service, "scenario_status", json!({"job_id":job_id})).unwrap();
        if status["status"] == "running" {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "scenario failed to begin: {status}"
        );
        thread::sleep(Duration::from_millis(20));
    }
    thread::sleep(Duration::from_millis(250));
    let in_flight = agent(&service, "scenario_status", json!({"job_id":job_id})).unwrap();
    assert_eq!(in_flight["status"], "running", "{in_flight}");
    let cancellation = agent(&service, "scenario_cancel", json!({"job_id":job_id})).unwrap();
    assert!(matches!(
        cancellation["status"].as_str(),
        Some("running" | "queued")
    ));
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let status = agent(&service, "scenario_status", json!({"job_id":job_id})).unwrap();
        if status["status"] == "cancelled" {
            assert!(status["finished_at_ms"].as_u64().is_some());
            assert_eq!(status["steps"][0]["operation"], "wait");
            assert_eq!(status["steps"][0]["status"], "cancelled");
            break;
        }
        assert!(
            Instant::now() < deadline,
            "scenario did not cancel: {status}"
        );
        thread::sleep(Duration::from_millis(25));
    }
}

#[test]
fn scenario_report_is_retained_at_creation_and_updated_without_status_polling() {
    let (temp, service, _cleanup) = setup();
    let cwd = temp.path().join("work");
    fs::create_dir_all(&cwd).unwrap();
    let (session_id, _) = launch(&service, fixture_spec(&cwd), LaunchOptions::default());
    wait_text(&service, &session_id, "READY");

    let started = agent(
        &service,
        "scenario_run",
        json!({
            "scenario": {
                "version": 1,
                "name": "unpolled-report",
                "steps": [{
                    "id": "observe-ready",
                    "operation": "wait",
                    "params": {
                        "session_id": session_id,
                        "predicate": {"kind":"text_contains","text":"READY"}
                    }
                }]
            }
        }),
    )
    .unwrap();
    let job_id = started["job_id"].as_str().unwrap();
    let report_path = temp
        .path()
        .join("runtime/scenarios")
        .join(format!("{job_id}.json"));
    assert!(report_path.is_file());

    let mut registry =
        silicogen_terminal_runtime::retention::RetentionRegistry::open(temp.path().join("runtime"))
            .unwrap();
    let relative_report = PathBuf::from("scenarios").join(format!("{job_id}.json"));
    let initial_owner = registry
        .entries()
        .iter()
        .find(|entry| entry.relative_path == relative_report)
        .expect("scenario report must be registered before execution starts");
    assert_eq!(
        initial_owner.retention,
        silicogen_terminal_runtime::retention::RetentionClass::Interactive
    );
    assert!(initial_owner.pinned);

    let deadline = Instant::now() + Duration::from_secs(3);
    let report: Value = loop {
        let report: Value = serde_json::from_slice(&fs::read(&report_path).unwrap()).unwrap();
        if matches!(
            report["status"].as_str(),
            Some("passed" | "failed" | "cancelled" | "timed_out")
        ) {
            break report;
        }
        assert!(
            Instant::now() < deadline,
            "scenario did not finish: {report}"
        );
        thread::sleep(Duration::from_millis(20));
    };
    let owner_id = registry
        .entries()
        .iter()
        .find(|entry| entry.relative_path == relative_report)
        .unwrap()
        .id
        .clone();
    while registry
        .entries()
        .iter()
        .find(|entry| entry.id == owner_id)
        .is_some_and(|entry| {
            entry.retention == silicogen_terminal_runtime::retention::RetentionClass::Interactive
        })
    {
        assert!(
            Instant::now() < deadline,
            "scenario retention did not transition"
        );
        thread::sleep(Duration::from_millis(20));
        registry = silicogen_terminal_runtime::retention::RetentionRegistry::open(
            temp.path().join("runtime"),
        )
        .unwrap();
    }
    let owner = registry
        .entries()
        .iter()
        .find(|entry| entry.id == owner_id)
        .unwrap();
    let expected_class = if report["status"] == "passed" {
        silicogen_terminal_runtime::retention::RetentionClass::Successful
    } else {
        silicogen_terminal_runtime::retention::RetentionClass::Failed
    };
    assert_eq!(owner.retention, expected_class);
    assert_eq!(
        owner.created_at_ms,
        report["finished_at_ms"].as_u64().unwrap() as u128
    );
}

#[test]
fn service_startup_reconciles_unowned_completed_and_interrupted_scenario_reports() {
    let temp = TempDir::new().unwrap();
    let runtime = temp.path().join("runtime");
    let scenario_root = runtime.join("scenarios");
    fs::create_dir_all(&scenario_root).unwrap();
    for (id, status, finished_at_ms) in [
        (
            "previously-unpolled",
            silicogen_terminal_runtime::scenario::ScenarioStatus::Passed,
            Some(200_u128),
        ),
        (
            "interrupted-before-restart",
            silicogen_terminal_runtime::scenario::ScenarioStatus::Running,
            None,
        ),
    ] {
        let report = silicogen_terminal_runtime::scenario::ScenarioReport {
            schema_version: 1,
            scenario_id: id.into(),
            session_id: None,
            app_run_id: None,
            backend_work_may_continue: false,
            backend_operation_may_continue: None,
            scenario_name: id.into(),
            status,
            started_at_ms: 100,
            finished_at_ms,
            deadline_at_ms: 10_000,
            variables: BTreeMap::new(),
            steps: Vec::new(),
            failure_reason: None,
        };
        fs::write(
            scenario_root.join(format!("{id}.json")),
            serde_json::to_vec(&report).unwrap(),
        )
        .unwrap();
    }

    let service = SessionService::new(&runtime).unwrap();
    let registry =
        silicogen_terminal_runtime::retention::RetentionRegistry::open(&runtime).unwrap();
    let passed = registry
        .entries()
        .iter()
        .find(|entry| {
            entry.relative_path.as_path() == Path::new("scenarios/previously-unpolled.json")
        })
        .expect("completed unpolled report must be managed after startup");
    assert_eq!(
        passed.retention,
        silicogen_terminal_runtime::retention::RetentionClass::Successful
    );
    assert_eq!(passed.created_at_ms, 200);
    let interrupted = registry
        .entries()
        .iter()
        .find(|entry| {
            entry.relative_path.as_path() == Path::new("scenarios/interrupted-before-restart.json")
        })
        .expect("interrupted report must be managed after startup");
    assert_eq!(
        interrupted.retention,
        silicogen_terminal_runtime::retention::RetentionClass::Failed
    );
    let persisted: Value = serde_json::from_slice(
        &fs::read(scenario_root.join("interrupted-before-restart.json")).unwrap(),
    )
    .unwrap();
    assert_eq!(persisted["status"], "interrupted");
    service.shutdown().unwrap();
}

#[tokio::test]
async fn cleanup_and_view_start_have_one_atomic_evidence_lease_boundary() {
    let temp = TempDir::new().unwrap();
    let runtime = temp.path().join("runtime");
    fs::create_dir_all(&runtime).unwrap();
    fs::write(
        runtime.join("retention-policy.json"),
        serde_json::to_vec(&silicogen_terminal_runtime::retention::RetentionPolicy {
            successful_ms: 0,
            failed_ms: 0,
            max_bytes: Some(16 * 1024 * 1024),
            max_entries: Some(100),
        })
        .unwrap(),
    )
    .unwrap();
    let service = SessionService::new(&runtime).unwrap();
    let cleanup_guard = ServiceCleanup(service.clone());
    let cwd = temp.path().join("work");
    fs::create_dir_all(&cwd).unwrap();
    let mut spec = fixture_spec(&cwd);
    spec.program = "/bin/sh".into();
    spec.args = vec!["-c".into(), "printf CLEANUP_VIEW_READY".into()];
    let launched = service
        .handle(RpcRequest::LaunchConfigured {
            spec,
            options: LaunchOptions {
                disposable: true,
                ..Default::default()
            },
        })
        .unwrap()
        .0;
    let session_id = launched["session_id"].as_str().unwrap().to_owned();
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let summary = service
            .handle(RpcRequest::Status {
                session_id: session_id.clone(),
            })
            .unwrap()
            .0;
        if summary["state"] != "running" {
            break;
        }
        assert!(Instant::now() < deadline, "fixture did not exit: {summary}");
        thread::sleep(Duration::from_millis(10));
    }
    let screenshot = service
        .handle(RpcRequest::Screenshot {
            session_id: session_id.clone(),
        })
        .unwrap()
        .0;
    let screenshot_path = PathBuf::from(screenshot["path"].as_str().unwrap());
    let preview = agent(&service, "cleanup", json!({})).unwrap();
    let plan = preview;
    assert!(
        plan["candidates"]
            .as_array()
            .unwrap()
            .iter()
            .any(|candidate| {
                candidate["path"]
                    .as_str()
                    .is_some_and(|path| path.ends_with(&format!("sessions/{session_id}")))
            })
    );

    let viewer = silicogen_terminal_runtime::viewer::ViewerServer::new(Arc::new(service.clone()));
    let barrier = Arc::new(Barrier::new(2));
    let cleanup_service = service.clone();
    let cleanup_plan = plan.clone();
    let cleanup_barrier = Arc::clone(&barrier);
    let cleanup = thread::spawn(move || {
        cleanup_barrier.wait();
        agent(
            &cleanup_service,
            "cleanup",
            json!({"apply":true,"plan":cleanup_plan}),
        )
    });
    barrier.wait();
    let viewing = viewer
        .handle_request(RpcRequest::ViewStart {
            session_id: session_id.clone(),
            allow_browser_control: false,
            idempotency_key: None,
        })
        .await;
    let cleaned = cleanup.join().unwrap();

    assert!(
        !(viewing.is_ok() && cleaned.is_ok()),
        "cleanup deleted evidence after granting a viewer"
    );
    if viewing.is_ok() {
        assert!(runtime.join("sessions").join(&session_id).exists());
        assert!(screenshot_path.exists());
    } else if cleaned.is_ok() {
        assert!(!runtime.join("sessions").join(&session_id).exists());
        assert!(!screenshot_path.exists());
    } else {
        assert!(runtime.join("sessions").join(&session_id).exists());
        assert!(screenshot_path.exists());
    }
    drop(viewer);
    drop(cleanup_guard);
    service.shutdown().unwrap();
}

#[test]
fn failed_launch_removes_imported_state_and_retains_only_owned_failure_evidence() {
    let (temp, service, _cleanup) = setup();
    let auth = temp.path().join("auth.json");
    fs::write(&auth, "synthetic-failure-key").unwrap();
    let mut spec = fixture_spec(temp.path());
    spec.program = "/nonexistent-emulation-fixture".into();
    let error = service
        .handle(RpcRequest::LaunchConfigured {
            spec,
            options: LaunchOptions {
                state_mode: StateMode::CopyUser,
                seeds: vec![StateSeed {
                    source: auth.clone(),
                    destination: "data/silicogen/auth.json".into(),
                    include_credentials: true,
                }],
                ..Default::default()
            },
        })
        .unwrap_err();
    let allocation = error
        .downcast_ref::<silicogen_terminal_runtime::session::LaunchFailure>()
        .unwrap();
    let directory = temp
        .path()
        .join("runtime/sessions")
        .join(&allocation.session_id);
    assert!(!directory.join("state").exists());
    assert!(directory.join("failure.json").exists());
    assert_eq!(fs::read_to_string(auth).unwrap(), "synthetic-failure-key");
    let registry =
        silicogen_terminal_runtime::retention::RetentionRegistry::open(temp.path().join("runtime"))
            .unwrap();
    assert!(
        registry
            .entries()
            .iter()
            .any(|e| e.relative_path.ends_with(&allocation.session_id))
    );
    let status = registry.ensure_budget(0, 0, u128::MAX, &[]).unwrap();
    assert_eq!(status.projected_entries, 1);
}

#[test]
fn marked_secret_arguments_are_not_persisted_and_cannot_be_relaunched_without_values() {
    let (temp, service, _cleanup) = setup();
    let mut spec = fixture_spec(temp.path());
    spec.args = vec![
        "--synthetic-secret".into(),
        "do-not-persist-this-value".into(),
    ];
    let (id, launched) = launch(
        &service,
        spec,
        LaunchOptions {
            sensitive_args: vec![1],
            ..Default::default()
        },
    );
    wait_text(&service, &id, "READY");
    assert_eq!(launched["args"][1], "<redacted>");
    let directory = temp.path().join("runtime/sessions").join(&id);
    for filename in ["launch.json", "session.json"] {
        assert!(
            !fs::read_to_string(directory.join(filename))
                .unwrap()
                .contains("do-not-persist-this-value")
        );
    }
    assert!(
        agent(&service, "relaunch", json!({"session_id":id}))
            .unwrap_err()
            .to_string()
            .contains("credentials_required")
    );
}

#[test]
fn scenario_wait_poll_duration_does_not_expire_a_slow_startup_handle() {
    let (temp, service, _cleanup) = setup();
    let spec = LaunchSpec {
        program: "/bin/sh".into(),
        args: vec!["-c".into(), "sleep 0.4; printf SLOW_READY; sleep 2".into()],
        ..fixture_spec(temp.path())
    };
    let (id, _) = launch(&service, spec, LaunchOptions::default());
    let started=agent(&service,"scenario_run",json!({"scenario":{"version":1,"name":"slow-startup","timeout_ms":4000,"steps":[{"id":"ready","operation":"wait","timeout_ms":3000,"params":{"session_id":id,"predicate":{"kind":"text_contains","text":"SLOW_READY"}}}]}})).unwrap();
    let deadline = Instant::now() + Duration::from_secs(4);
    loop {
        let result = agent(
            &service,
            "scenario_status",
            json!({"job_id":started["job_id"]}),
        )
        .unwrap();
        if result["status"] == "passed" {
            assert!(result["steps"][0]["result"]["elapsed_ms"].as_u64().unwrap() > 200);
            break;
        }
        assert!(
            result["status"] == "queued" || result["status"] == "running",
            "{result}"
        );
        assert!(Instant::now() < deadline);
        thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn screenshots_and_saved_frames_share_the_session_capture_quota() {
    let (temp, service, _cleanup) = setup();
    let options = LaunchOptions {
        metadata: BTreeMap::from([("artifact_max_count".into(), "1".into())]),
        ..Default::default()
    };
    let (id, _) = launch(&service, fixture_spec(temp.path()), options);
    wait_text(&service, &id, "READY");
    agent(
        &service,
        "save_frame",
        json!({"session_id":id,"name":"only-frame"}),
    )
    .unwrap();
    assert!(
        agent(
            &service,
            "save_frame",
            json!({"session_id":id,"name":"no-second-frame"})
        )
        .unwrap_err()
        .to_string()
        .contains("artifact_quota_exceeded")
    );
    assert!(
        service
            .handle(RpcRequest::Screenshot { session_id: id })
            .unwrap_err()
            .to_string()
            .contains("artifact_quota_exceeded")
    );
}
