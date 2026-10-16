use rmcp::ServiceExt;
use rmcp::model::CallToolRequestParams;
use rmcp::transport::TokioChildProcess;
use serde_json::Value;
use silicogen_terminal_runtime::client::RuntimeClient;
use silicogen_terminal_runtime::control::BrowserAuthority;
use silicogen_terminal_runtime::protocol::{
    CellRegionSnapshot, FrameSnapshot, LaunchSpec, MouseButton, MouseInput, RpcRequest,
    ScreenshotArtifact, WaitPredicate, WaitResult, WheelDirection,
};
use silicogen_terminal_runtime::service::SessionService;
use silicogen_terminal_runtime::session::{BrowserActionContext, BrowserInput};
use std::collections::BTreeMap;
use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::os::unix::fs::{FileTypeExt, MetadataExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

fn service() -> (TempDir, SessionService) {
    let temp = tempfile::tempdir().unwrap();
    let runtime = temp.path().join("runtime");
    fs::create_dir_all(&runtime).unwrap();
    let service = SessionService::new(&runtime).unwrap();
    (temp, service)
}

fn launch(service: &SessionService, cwd: &Path, args: &[&str], record: bool) -> String {
    let response = service
        .handle(RpcRequest::Launch {
            spec: LaunchSpec {
                name: Some("fixture".into()),
                program: env!("CARGO_BIN_EXE_terminal-fixture").into(),
                args: args.iter().map(|arg| (*arg).into()).collect(),
                cwd: cwd.display().to_string(),
                env: BTreeMap::new(),
                cols: 80,
                rows: 24,
                record_output: record,
            },
        })
        .unwrap()
        .0;
    response["session_id"].as_str().unwrap().to_owned()
}

fn wait_contains(service: &SessionService, session_id: &str, text: &str) -> FrameSnapshot {
    let value = service
        .handle(RpcRequest::Wait {
            session_id: session_id.to_owned(),
            predicate: WaitPredicate::TextContains {
                text: text.to_owned(),
            },
            timeout_ms: 5_000,
        })
        .unwrap()
        .0;
    let result: WaitResult = serde_json::from_value(value).unwrap();
    assert!(
        result.matched,
        "timed out waiting for {text:?}; current frame: {:?}",
        result.snapshot.as_ref().map(|snapshot| (
            &snapshot.text,
            &snapshot.keyboard_mode,
            &snapshot.process_state
        ))
    );
    result.snapshot.unwrap()
}

#[test]
fn fixture_exercises_observation_input_resize_screenshot_restart_and_close() {
    let (temp, service) = service();
    let cwd = temp.path().join("work");
    fs::create_dir_all(&cwd).unwrap();
    let session_id = launch(&service, &cwd, &[], false);

    let initial = wait_contains(&service, &session_id, "READY");
    assert_eq!((initial.cols, initial.rows), (80, 24));
    assert_eq!(initial.mouse_tracking, "motion");
    assert_eq!(initial.mouse_encoding, "sgr");
    assert_eq!(initial.modes.get("bracketed_paste"), Some(&true));
    assert!(!initial.styled_runs.is_empty());
    let region: CellRegionSnapshot = serde_json::from_value(
        service
            .handle(RpcRequest::Cells {
                session_id: session_id.clone(),
                row: 0,
                col: 0,
                rows: 8,
                cols: 80,
            })
            .unwrap()
            .0,
    )
    .unwrap();
    assert_eq!(region.frame_id, initial.frame_id);
    assert!(region.cells[0].iter().any(|cell| cell.background.is_some()));

    service
        .handle(RpcRequest::Key {
            session_id: session_id.clone(),
            key: "Down".into(),
        })
        .unwrap();
    let selected = wait_contains(&service, &session_id, "Selection: Beta");
    assert!(selected.text.contains("Selection: Beta"));

    service
        .handle(RpcRequest::Type {
            session_id: session_id.clone(),
            text: "typed text".into(),
        })
        .unwrap();
    assert!(
        wait_contains(&service, &session_id, "Typed: typed text")
            .text
            .contains("typed text")
    );

    let paste = service
        .handle(RpcRequest::Paste {
            session_id: session_id.clone(),
            text: "pasted text".into(),
        })
        .unwrap()
        .0;
    assert_eq!(paste["bracketed"], true);
    assert!(
        wait_contains(&service, &session_id, "Paste: pasted text")
            .text
            .contains("pasted text")
    );

    service
        .handle(RpcRequest::Mouse {
            session_id: session_id.clone(),
            input: MouseInput::Move { row: 2, col: 7 },
        })
        .unwrap();
    assert!(
        wait_contains(&service, &session_id, "Mouse: code=35 row=2 col=7")
            .text
            .contains("code=35")
    );

    service
        .handle(RpcRequest::Mouse {
            session_id: session_id.clone(),
            input: MouseInput::Click {
                row: 3,
                col: 8,
                button: MouseButton::Left,
            },
        })
        .unwrap();
    assert!(
        wait_contains(&service, &session_id, "Mouse: code=3 row=3 col=8")
            .text
            .contains("code=3")
    );

    service
        .handle(RpcRequest::Mouse {
            session_id: session_id.clone(),
            input: MouseInput::Wheel {
                row: 4,
                col: 9,
                direction: WheelDirection::Down,
                steps: 1,
            },
        })
        .unwrap();
    assert!(
        wait_contains(&service, &session_id, "Mouse: code=65 row=4 col=9")
            .text
            .contains("code=65")
    );

    service
        .handle(RpcRequest::Resize {
            session_id: session_id.clone(),
            cols: 92,
            rows: 28,
        })
        .unwrap();
    let resized = wait_contains(&service, &session_id, "Size: 92x28");
    assert_eq!((resized.cols, resized.rows), (92, 28));

    let screenshot: ScreenshotArtifact = serde_json::from_value(
        service
            .handle(RpcRequest::Screenshot {
                session_id: session_id.clone(),
            })
            .unwrap()
            .0,
    )
    .unwrap();
    let png = fs::read(&screenshot.path).unwrap();
    assert!(png.starts_with(b"\x89PNG\r\n\x1a\n"));
    assert_eq!(screenshot.session_id, resized.session_id);
    assert_eq!(screenshot.run_id, resized.run_id);
    assert_eq!(screenshot.frame_id, resized.frame_id);

    let old_run = resized.run_id;
    let restarted: Value = service
        .handle(RpcRequest::Restart {
            session_id: session_id.clone(),
        })
        .unwrap()
        .0;
    assert_ne!(restarted["run_id"].as_str().unwrap(), old_run);
    let after_restart = wait_contains(&service, &session_id, "Selection: Alpha");
    assert!(!after_restart.text.contains("typed text"));
    assert_eq!((after_restart.cols, after_restart.rows), (92, 28));

    let closed: Value = service
        .handle(RpcRequest::Close {
            session_id: session_id.clone(),
        })
        .unwrap()
        .0;
    assert_eq!(closed["state"], "closed");
    assert!(service.handle(RpcRequest::Status { session_id }).unwrap().0["state"] == "closed");
}

#[test]
fn browser_takeover_blocks_automation_and_stale_runs_before_pty_write() {
    let (temp, service) = service();
    let cwd = temp.path().join("control-work");
    fs::create_dir_all(&cwd).unwrap();
    let session_id = launch(&service, &cwd, &[], false);
    let initial = wait_contains(&service, &session_id, "READY");
    let lease = service
        .viewer_take_control(&session_id, "view-a", "connection-a", true)
        .unwrap();
    let authority = BrowserAuthority {
        view_id: lease.view_id.clone(),
        connection_id: lease.connection_id.clone(),
        lease_id: lease.lease_id.clone(),
        generation: lease.generation,
        expected_run_id: lease.run_id.clone(),
    };
    let current_frame = service
        .viewer_session(&session_id)
        .unwrap()
        .stream_snapshot()
        .unwrap();
    let browser_action = BrowserActionContext {
        action_id: "browser-input-1".into(),
        frame_id: current_frame.frame.frame_id.clone(),
        view_revision: current_frame.view_revision,
        viewport_revision: current_frame.viewport_revision,
    };

    let denied = service
        .handle(RpcRequest::Type {
            session_id: session_id.clone(),
            text: "must not reach the PTY".into(),
        })
        .unwrap_err();
    assert!(denied.to_string().contains("control_held_by_browser"));
    let still_initial = service
        .handle(RpcRequest::Observe {
            session_id: session_id.clone(),
        })
        .unwrap()
        .0;
    assert_eq!(still_initial["text"], initial.text);

    let browser_result = service
        .viewer_input(
            &session_id,
            &authority,
            &browser_action,
            BrowserInput::Type("from browser".into()),
        )
        .unwrap();
    assert!(matches!(
        browser_result,
        silicogen_terminal_runtime::session::BrowserInputResult::Accepted
    ));
    assert!(
        wait_contains(&service, &session_id, "Typed: from browser")
            .text
            .contains("from browser")
    );
    let history = service
        .handle(RpcRequest::History {
            session_id: session_id.clone(),
        })
        .unwrap()
        .0;
    let browser_event = history["events"]
        .as_array()
        .unwrap()
        .iter()
        .find(|event| {
            event["kind"] == "browser_action"
                && event["detail"].as_str().is_some_and(|detail| {
                    detail.contains("browser-input-1") && detail.contains("\"accepted\":true")
                })
        })
        .expect("browser input should have bounded provenance in the session history");
    let detail: Value = serde_json::from_str(browser_event["detail"].as_str().unwrap()).unwrap();
    assert_eq!(detail["source"], "browser");
    assert_eq!(detail["displayed_frame_id"], current_frame.frame.frame_id);
    assert_eq!(detail["control_generation"], lease.generation);
    assert!(
        !browser_event["detail"]
            .as_str()
            .unwrap()
            .contains("from browser")
    );

    let stale_release = service.handle(RpcRequest::ControlRelease {
        session_id: session_id.clone(),
        expected_generation: lease.generation.saturating_sub(1),
    });
    assert!(
        stale_release
            .unwrap_err()
            .to_string()
            .contains("control_generation_changed")
    );

    service
        .viewer_release_control(&session_id, &authority)
        .unwrap();
    service
        .handle(RpcRequest::Type {
            session_id: session_id.clone(),
            text: "automation back".into(),
        })
        .unwrap();
    assert!(
        wait_contains(&service, &session_id, "automation back")
            .text
            .contains("automation back")
    );

    let restarted = service
        .handle(RpcRequest::Restart {
            session_id: session_id.clone(),
        })
        .unwrap()
        .0;
    assert_ne!(restarted["run_id"], initial.run_id);
    let next_lease = service
        .viewer_take_control(&session_id, "view-b", "connection-b", true)
        .unwrap();
    let stale_run_authority = BrowserAuthority {
        expected_run_id: initial.run_id.clone(),
        view_id: next_lease.view_id,
        connection_id: next_lease.connection_id,
        lease_id: next_lease.lease_id,
        generation: next_lease.generation,
    };
    let stale_action = BrowserActionContext {
        action_id: "stale-run-input".into(),
        frame_id: current_frame.frame.frame_id,
        view_revision: current_frame.view_revision,
        viewport_revision: current_frame.viewport_revision,
    };
    let stale_run = service.viewer_input(
        &session_id,
        &stale_run_authority,
        &stale_action,
        BrowserInput::Type("stale run".into()),
    );
    assert!(stale_run.unwrap_err().to_string().contains("stale_run"));
    service.shutdown().unwrap();
}

#[test]
fn concurrent_browser_takeover_and_automation_write_have_one_serial_order() {
    let (temp, service) = service();
    let cwd = temp.path().join("control-race-work");
    fs::create_dir_all(&cwd).unwrap();
    let session_id = launch(&service, &cwd, &[], false);
    wait_contains(&service, &session_id, "READY");

    let service = Arc::new(service);
    let barrier = Arc::new(Barrier::new(3));
    let takeover_service = Arc::clone(&service);
    let takeover_barrier = Arc::clone(&barrier);
    let takeover_session = session_id.clone();
    let takeover = thread::spawn(move || {
        takeover_barrier.wait();
        takeover_service.viewer_take_control(
            &takeover_session,
            "race-view",
            "race-connection",
            true,
        )
    });

    let input_service = Arc::clone(&service);
    let input_barrier = Arc::clone(&barrier);
    let input_session = session_id.clone();
    let input = thread::spawn(move || {
        input_barrier.wait();
        input_service.handle(RpcRequest::Type {
            session_id: input_session,
            text: "race-marker".into(),
        })
    });

    barrier.wait();
    let lease = takeover.join().unwrap().unwrap();
    let input_result = input.join().unwrap();
    if let Err(error) = &input_result {
        assert!(error.to_string().contains("control_held_by_browser"));
    }

    let snapshot = wait_contains(&service, &session_id, "READY");
    assert_eq!(snapshot.text.contains("race-marker"), input_result.is_ok());
    let control = service
        .handle(RpcRequest::ControlStatus {
            session_id: session_id.clone(),
        })
        .unwrap()
        .0;
    assert_eq!(control["owner"], "browser");

    let authority = BrowserAuthority {
        view_id: lease.view_id,
        connection_id: lease.connection_id,
        lease_id: lease.lease_id,
        generation: lease.generation,
        expected_run_id: lease.run_id,
    };
    service
        .viewer_release_control(&session_id, &authority)
        .unwrap();
    service.shutdown().unwrap();
}

#[test]
fn frame_stream_uses_full_grid_and_revisions_after_live_input() {
    let (temp, service) = service();
    let cwd = temp.path().join("stream-work");
    fs::create_dir_all(&cwd).unwrap();
    let session_id = launch(&service, &cwd, &[], false);
    wait_contains(&service, &session_id, "READY");
    let session = service.viewer_session(&session_id).unwrap();
    let mut changed = session.subscribe_changes();
    let before = session.stream_snapshot().unwrap();
    assert_eq!(before.frame.cols, 80);
    assert_eq!(before.cells.len(), usize::from(before.frame.rows));
    assert_eq!(before.cells[0].len(), usize::from(before.frame.cols));
    let before_revision = before.view_revision;
    drop(changed.borrow_and_update());

    service
        .handle(RpcRequest::Type {
            session_id: session_id.clone(),
            text: "stream update".into(),
        })
        .unwrap();
    wait_contains(&service, &session_id, "stream update");
    assert!(changed.has_changed().unwrap());
    let after = session.stream_snapshot().unwrap();
    assert!(after.view_revision > before_revision);
    assert!(after.frame.text.contains("stream update"));
    service.shutdown().unwrap();
}

#[test]
fn browser_lease_is_released_when_the_application_exits() {
    let (temp, service) = service();
    let cwd = temp.path().join("browser-exit-work");
    fs::create_dir_all(&cwd).unwrap();
    let session_id = launch(&service, &cwd, &[], false);
    let initial = wait_contains(&service, &session_id, "READY");
    service
        .viewer_take_control(&session_id, "view-exit", "connection-exit", true)
        .unwrap();
    assert_eq!(
        service
            .handle(RpcRequest::ControlStatus {
                session_id: session_id.clone(),
            })
            .unwrap()
            .0["owner"],
        "browser"
    );
    let process_group = service
        .handle(RpcRequest::Status {
            session_id: session_id.clone(),
        })
        .unwrap()
        .0["process_group"]
        .as_i64()
        .unwrap() as i32;
    assert_eq!(unsafe { libc::kill(-process_group, libc::SIGTERM) }, 0);
    let exited = service
        .handle(RpcRequest::Wait {
            session_id: session_id.clone(),
            predicate: WaitPredicate::ProcessExited,
            timeout_ms: 5_000,
        })
        .unwrap()
        .0;
    assert!(exited["matched"].as_bool().unwrap());
    let status = service
        .handle(RpcRequest::ControlStatus {
            session_id: session_id.clone(),
        })
        .unwrap()
        .0;
    assert_eq!(status["owner"], "automation");
    let restarted = service
        .handle(RpcRequest::Restart {
            session_id: session_id.clone(),
        })
        .unwrap()
        .0;
    assert_ne!(restarted["run_id"], initial.run_id);
    service.shutdown().unwrap();
}

#[test]
fn predicate_waits_report_timeout_and_process_exit() {
    let (temp, service) = service();
    let cwd = temp.path().join("work");
    fs::create_dir_all(&cwd).unwrap();
    let session_id = launch(&service, &cwd, &[], false);
    wait_contains(&service, &session_id, "READY");

    let timeout: WaitResult = serde_json::from_value(
        service
            .handle(RpcRequest::Wait {
                session_id: session_id.clone(),
                predicate: WaitPredicate::TextContains {
                    text: "not present".into(),
                },
                timeout_ms: 100,
            })
            .unwrap()
            .0,
    )
    .unwrap();
    assert!(!timeout.matched);
    assert!(timeout.timed_out);
    assert!((100..1_000).contains(&timeout.elapsed_ms));
    assert!(timeout.snapshot.is_some());

    service.handle(RpcRequest::Close { session_id }).unwrap();

    let exit_session = launch(&service, &cwd, &["--exit-after-ready"], false);
    let exited: WaitResult = serde_json::from_value(
        service
            .handle(RpcRequest::Wait {
                session_id: exit_session.clone(),
                predicate: WaitPredicate::ProcessExited,
                timeout_ms: 2_000,
            })
            .unwrap()
            .0,
    )
    .unwrap();
    assert!(exited.matched);
    assert!(!exited.timed_out);
    assert_ne!(exited.snapshot.unwrap().process_state, "running");
    service
        .handle(RpcRequest::Close {
            session_id: exit_session,
        })
        .unwrap();
}

#[test]
fn closing_session_kills_descendants_in_the_pty_process_group() {
    let (temp, service) = service();
    let cwd = temp.path().join("work");
    fs::create_dir_all(&cwd).unwrap();
    let session_id = launch(&service, &cwd, &["--spawn-child"], false);
    wait_contains(&service, &session_id, "READY");
    let child_pid: i32 = fs::read_to_string(cwd.join("fixture-child.pid"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();

    service.handle(RpcRequest::Close { session_id }).unwrap();
    let mut gone = false;
    for _ in 0..50 {
        if !process_is_alive(child_pid) {
            gone = true;
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(gone, "descendant process {child_pid} survived close");
}

#[test]
fn opt_in_recording_is_bounded_and_reports_truncation() {
    let (temp, service) = service();
    let cwd = temp.path().join("work");
    fs::create_dir_all(&cwd).unwrap();
    let session_id = launch(&service, &cwd, &[], true);
    wait_contains(&service, &session_id, "READY");
    for _ in 0..600 {
        service
            .handle(RpcRequest::Type {
                session_id: session_id.clone(),
                text: "x".into(),
            })
            .unwrap();
    }
    let history: Value = service
        .handle(RpcRequest::History {
            session_id: session_id.clone(),
        })
        .unwrap()
        .0;
    assert_eq!(history["raw_recording_enabled"], true);
    assert_eq!(history["truncated"], true);
    assert_eq!(history["replayable"], false);
    assert!(history["retained_bytes"].as_u64().unwrap() <= history["max_bytes"].as_u64().unwrap());
    service.handle(RpcRequest::Close { session_id }).unwrap();
}

#[test]
fn predicate_waits_match_absence_regex_and_stable_screen() {
    let (temp, service) = service();
    let cwd = temp.path().join("work");
    fs::create_dir_all(&cwd).unwrap();
    let session_id = launch(&service, &cwd, &[], false);
    wait_contains(&service, &session_id, "READY");

    let absent: WaitResult = serde_json::from_value(
        service
            .handle(RpcRequest::Wait {
                session_id: session_id.clone(),
                predicate: WaitPredicate::TextAbsent {
                    text: "not present".into(),
                },
                timeout_ms: 1_000,
            })
            .unwrap()
            .0,
    )
    .unwrap();
    assert!(absent.matched);
    assert!(!absent.timed_out);

    let regex: WaitResult = serde_json::from_value(
        service
            .handle(RpcRequest::Wait {
                session_id: session_id.clone(),
                predicate: WaitPredicate::Regex {
                    pattern: r"Selection: Alpha".into(),
                },
                timeout_ms: 1_000,
            })
            .unwrap()
            .0,
    )
    .unwrap();
    assert!(regex.matched);
    assert!(!regex.timed_out);

    let stable: WaitResult = serde_json::from_value(
        service
            .handle(RpcRequest::Wait {
                session_id: session_id.clone(),
                predicate: WaitPredicate::Stable { duration_ms: 100 },
                timeout_ms: 1_000,
            })
            .unwrap()
            .0,
    )
    .unwrap();
    assert!(stable.matched);
    assert!(!stable.timed_out);
    assert!(stable.elapsed_ms >= 100);
    service.handle(RpcRequest::Close { session_id }).unwrap();
}

#[test]
fn cli_invocations_share_a_daemon_owned_session() {
    let temp = tempfile::tempdir().unwrap();
    let runtime = temp.path().join("runtime");
    let cwd = temp.path().join("work");
    fs::create_dir_all(&cwd).unwrap();
    let binary = env!("CARGO_BIN_EXE_silicogen-emulation");
    let _cleanup = DaemonCleanup {
        binary: binary.to_string(),
        runtime: runtime.display().to_string(),
    };

    // The first CLI call starts the daemon; later invocations reconnect to it.
    let mut launch_command = std::process::Command::new(binary);
    launch_command
        .process_group(0)
        .arg("--runtime-dir")
        .arg(&runtime)
        .args([
            "launch",
            "--cwd",
            cwd.to_str().unwrap(),
            "--",
            env!("CARGO_BIN_EXE_terminal-fixture"),
        ])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let launch_child = launch_command.spawn().unwrap();
    let launch_process_group = launch_child.id() as i32;
    let launch = launch_child.wait_with_output().unwrap();
    assert!(
        launch.status.success(),
        "{}",
        String::from_utf8_lossy(&launch.stderr)
    );
    let launched: Value = serde_json::from_slice(&launch.stdout).unwrap();
    let session_id = launched["session_id"].as_str().unwrap().to_owned();

    // Model a command runner that tears down the launch process group once the
    // CLI call returns. The daemon must have detached so the next invocation
    // can still observe the original process and session.
    unsafe {
        libc::kill(-launch_process_group, libc::SIGTERM);
    }
    thread::sleep(Duration::from_millis(100));

    let wait = cli(
        binary,
        &runtime,
        [
            "wait",
            &session_id,
            "--contains",
            "READY",
            "--timeout-ms",
            "5000",
        ],
    );
    assert!(
        wait.status.success(),
        "{}",
        String::from_utf8_lossy(&wait.stderr)
    );
    let observed = cli(binary, &runtime, ["observe", &session_id]);
    assert!(
        observed.status.success(),
        "{}",
        String::from_utf8_lossy(&observed.stderr)
    );
    let frame: Value = serde_json::from_slice(&observed.stdout).unwrap();
    assert!(frame["text"].as_str().unwrap().contains("READY"));

    let typed = cli(
        binary,
        &runtime,
        ["type", &session_id, "from-second-process"],
    );
    assert!(
        typed.status.success(),
        "{}",
        String::from_utf8_lossy(&typed.stderr)
    );
    let result = cli(
        binary,
        &runtime,
        [
            "wait",
            &session_id,
            "--contains",
            "from-second-process",
            "--timeout-ms",
            "5000",
        ],
    );
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );

    let closed = cli(binary, &runtime, ["close", &session_id]);
    assert!(
        closed.status.success(),
        "{}",
        String::from_utf8_lossy(&closed.stderr)
    );
    let shutdown = cli(binary, &runtime, ["shutdown"]);
    assert!(
        shutdown.status.success(),
        "{}",
        String::from_utf8_lossy(&shutdown.stderr)
    );
    assert!(!runtime.join("daemon.sock").exists());
}

#[test]
fn cli_canonicalizes_initially_absent_relative_runtime_directory() {
    let temp = tempfile::tempdir().unwrap();
    let binary = env!("CARGO_BIN_EXE_silicogen-emulation");
    let runtime = temp.path().join("relative-runtime");

    for args in [vec!["list"], vec!["shutdown"]] {
        let output = std::process::Command::new(binary)
            .current_dir(temp.path())
            .arg("--runtime-dir")
            .arg("relative-runtime")
            .args(args)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    assert!(!runtime.join("daemon.sock").exists());
}

#[tokio::test]
async fn client_uses_private_short_socket_for_long_runtime_root() {
    let temp = tempfile::tempdir().unwrap();
    let runtime = temp.path().join("long-runtime").join("r".repeat(110));
    let binary = env!("CARGO_BIN_EXE_silicogen-emulation");
    let _cleanup = DaemonCleanup {
        binary: binary.to_owned(),
        runtime: runtime.display().to_string(),
    };
    let client = RuntimeClient::new(&runtime);

    // A CLI process starts the real runtime binary; subsequent RPCs exercise
    // RuntimeClient against that daemon.
    let initial_list = cli(binary, &runtime, ["list"]);
    assert!(
        initial_list.status.success(),
        "{}",
        String::from_utf8_lossy(&initial_list.stderr)
    );
    let ping = client.request(RpcRequest::Ping).await.unwrap();
    assert_eq!(ping["status"], "ok");

    let socket = silicogen_terminal_runtime::daemon::socket_path(&runtime).unwrap();
    let paths_root = fs::canonicalize(&runtime).unwrap();
    assert!(socket.as_os_str().as_encoded_bytes().len() <= 107);
    assert_ne!(socket, paths_root.join("daemon.sock"));
    let metadata = fs::symlink_metadata(&socket).unwrap();
    assert!(metadata.file_type().is_socket());
    assert_eq!(metadata.uid(), unsafe { libc::geteuid() });
    assert_eq!(metadata.mode() & 0o777, 0o600);
    let parent = fs::symlink_metadata(socket.parent().unwrap()).unwrap();
    assert_eq!(parent.uid(), unsafe { libc::geteuid() });
    assert_eq!(parent.mode() & 0o077, 0);

    let list = client.request(RpcRequest::List).await.unwrap();
    assert!(list.as_array().unwrap().is_empty());

    client.request(RpcRequest::Shutdown).await.unwrap();
}

#[tokio::test]
async fn mcp_tools_use_the_same_persistent_session_service() {
    let temp = tempfile::tempdir().unwrap();
    let runtime = temp.path().join("runtime");
    let cwd = temp.path().join("work");
    fs::create_dir_all(&cwd).unwrap();
    let binary = env!("CARGO_BIN_EXE_silicogen-emulation");
    let mut command = tokio::process::Command::new(binary);
    command.arg("--runtime-dir").arg(&runtime).arg("mcp");
    let transport = TokioChildProcess::new(command).unwrap();
    let client = ().serve(transport).await.unwrap();

    let tools = client.peer().list_all_tools().await.unwrap();
    assert!(tools.iter().any(|tool| tool.name == "terminal_launch"));
    assert!(tools.iter().any(|tool| tool.name == "terminal_screenshot"));

    let launch_args = serde_json::json!({
        "program": env!("CARGO_BIN_EXE_terminal-fixture"),
        "args": [],
        "cwd": cwd.display().to_string(),
        "cols": 80,
        "rows": 24,
        "record_output": false
    });
    let launch = client
        .peer()
        .call_tool(
            CallToolRequestParams::new("terminal_launch")
                .with_arguments(launch_args.as_object().unwrap().clone()),
        )
        .await
        .unwrap();
    assert_ne!(launch.is_error, Some(true));
    let session_id = launch
        .structured_content
        .as_ref()
        .and_then(|value| value.get("session_id"))
        .and_then(Value::as_str)
        .unwrap()
        .to_owned();

    let wait_args = serde_json::json!({
        "session_id": session_id,
        "kind": "contains",
        "value": "READY",
        "timeout_ms": 5000
    });
    let wait = client
        .peer()
        .call_tool(
            CallToolRequestParams::new("terminal_wait")
                .with_arguments(wait_args.as_object().unwrap().clone()),
        )
        .await
        .unwrap();
    assert_ne!(wait.is_error, Some(true));

    let screenshot = client
        .peer()
        .call_tool(
            CallToolRequestParams::new("terminal_screenshot").with_arguments(
                serde_json::json!({ "session_id": session_id })
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .unwrap();
    assert_ne!(screenshot.is_error, Some(true));
    assert!(screenshot.content.iter().any(|block| {
        block
            .as_image()
            .is_some_and(|image| image.mime_type == "image/png" && !image.data.is_empty())
    }));
    let metadata = screenshot
        .content
        .iter()
        .filter_map(|block| block.as_text())
        .find_map(|block| serde_json::from_str::<Value>(&block.text).ok())
        .expect("screenshot tool should return artifact metadata alongside the image");
    assert!(metadata["frame_id"].as_str().is_some());

    let view = client
        .peer()
        .call_tool(
            CallToolRequestParams::new("terminal_view_start").with_arguments(
                serde_json::json!({
                    "session_id": session_id,
                    "allow_browser_control": false,
                    "idempotency_key": "mcp-client-exit-view"
                })
                .as_object()
                .unwrap()
                .clone(),
            ),
        )
        .await
        .unwrap();
    assert_ne!(view.is_error, Some(true));
    let view_data = view.structured_content.as_ref().unwrap();
    let view_id = view_data["view_id"].as_str().unwrap().to_owned();
    let port = view_data["port"].as_u64().unwrap() as u16;

    drop(client);

    let status = cli(binary, &runtime, ["status", &session_id]);
    assert!(
        status.status.success(),
        "{}",
        String::from_utf8_lossy(&status.stderr)
    );
    let status: Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(status["state"], "running");

    let view_status = cli(binary, &runtime, ["view-status", &view_id]);
    assert!(
        view_status.status.success(),
        "{}",
        String::from_utf8_lossy(&view_status.stderr)
    );
    let view_status: Value = serde_json::from_slice(&view_status.stdout).unwrap();
    assert_eq!(view_status["state"], "active");

    let mut page = TcpStream::connect(("127.0.0.1", port)).unwrap();
    write!(
        page,
        "GET /view/{view_id} HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    let mut response = String::new();
    page.read_to_string(&mut response).unwrap();
    assert!(response.starts_with("HTTP/1.1 200"));
    assert!(response.contains("Per-session XDG state"));

    let shutdown = cli(binary, &runtime, ["shutdown"]);
    assert!(
        shutdown.status.success(),
        "{}",
        String::from_utf8_lossy(&shutdown.stderr)
    );
}

fn cli<const N: usize>(binary: &str, runtime: &Path, args: [&str; N]) -> std::process::Output {
    std::process::Command::new(binary)
        .arg("--runtime-dir")
        .arg(runtime)
        .args(args)
        .output()
        .unwrap()
}

struct DaemonCleanup {
    binary: String,
    runtime: String,
}

impl Drop for DaemonCleanup {
    fn drop(&mut self) {
        let _ = std::process::Command::new(&self.binary)
            .arg("--runtime-dir")
            .arg(&self.runtime)
            .arg("shutdown")
            .output();
    }
}

fn process_is_alive(pid: i32) -> bool {
    let path = PathBuf::from(format!("/proc/{pid}/stat"));
    let Ok(stat) = fs::read_to_string(path) else {
        return false;
    };
    let Some(after_comm) = stat.rsplit_once(") ").map(|(_, tail)| tail) else {
        return true;
    };
    after_comm
        .chars()
        .next()
        .is_some_and(|state| state != 'Z' && state != 'X')
}
