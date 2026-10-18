use rmcp::{
    RoleClient, ServiceExt,
    model::{CallToolRequestParams, CallToolResult},
    service::{RunningService, ServiceError},
    transport::TokioChildProcess,
};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};

struct Harness {
    client: RunningService<RoleClient, ()>,
    runtime: PathBuf,
    temp: tempfile::TempDir,
}
impl Harness {
    async fn new() -> Self {
        let base = Path::new(env!("CARGO_MANIFEST_DIR")).join(".runtime/mcp-manual-recording");
        std::fs::create_dir_all(&base).unwrap();
        let temp = tempfile::tempdir_in(base).unwrap();
        let runtime = temp.path().join("runtime");
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_silicogen-emulation"));
        command
            .args(["--runtime-dir", runtime.to_str().unwrap(), "mcp"])
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("LANG", "C.UTF-8")
            .env("HOME", temp.path());
        let client = ().serve(TokioChildProcess::new(command).unwrap()).await.unwrap();
        Self {
            client,
            runtime,
            temp,
        }
    }
    async fn raw(&self, name: &str, args: Value) -> Result<CallToolResult, ServiceError> {
        tokio::time::timeout(
            Duration::from_secs(8),
            self.client.peer().call_tool(
                CallToolRequestParams::new(name.to_owned())
                    .with_arguments(args.as_object().unwrap().clone()),
            ),
        )
        .await
        .expect("MCP request timed out")
    }
    async fn call(&self, name: &str, args: Value) -> Value {
        let result = self.raw(name, args).await.unwrap();
        assert_ne!(result.is_error, Some(true), "{result:?}");
        result.structured_content.unwrap()
    }
    async fn fixture(&self) -> Value {
        self.fixture_with_env(json!({})).await
    }
    async fn fixture_with_env(&self, env: Value) -> Value {
        let source = self.temp.path().join("project-source");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::write(source.join("fixture-sentinel.txt"), "isolated source").unwrap();
        self.call(
            "terminal_launch",
            json!({
                "program":env!("CARGO_BIN_EXE_terminal-fixture"), "cwd":"${workspace}",
                "cols":80,"rows":24,"env_inherit":[],"env":env,
                "workspace":{"source":source,"mode":"copy","exclude":[]}
            }),
        )
        .await
    }
}
impl Drop for Harness {
    fn drop(&mut self) {
        let mut child = match Command::new(env!("CARGO_BIN_EXE_silicogen-emulation"))
            .arg("--runtime-dir")
            .arg(&self.runtime)
            .arg("shutdown")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
        {
            Ok(child) => child,
            Err(_) => return,
        };
        if wait_child(&mut child, Duration::from_secs(2)) {
            return;
        }
        let _ = child.kill();
        let _ = child.wait();
        if let Some(pid) = owned_daemon_pid(&self.runtime) {
            unsafe { libc::kill(pid as i32, libc::SIGTERM) };
        }
    }
}
fn wait_child(child: &mut Child, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if child.try_wait().ok().flatten().is_some() {
            return true;
        }
        thread::sleep(Duration::from_millis(10));
    }
    false
}
fn owned_daemon_pid(runtime: &Path) -> Option<u32> {
    let runtime = std::fs::canonicalize(runtime).ok()?;
    for entry in std::fs::read_dir("/proc").ok()?.flatten() {
        let pid = entry.file_name().to_string_lossy().parse::<u32>().ok()?;
        let args = std::fs::read(entry.path().join("cmdline")).ok()?;
        let args = args.split(|byte| *byte == 0).collect::<Vec<_>>();
        if args.windows(2).any(|pair| {
            pair[0] == b"--runtime-dir"
                && std::str::from_utf8(pair[1]).is_ok_and(|path| Path::new(path) == runtime)
        }) && args.contains(&b"daemon".as_slice())
        {
            return Some(pid);
        }
    }
    None
}

fn recording_args(action: &str, session: &str, run: &str, id: &str) -> Value {
    json!({"action":action,"session_id":session,"run_id":run,"recording_id":id,"opted_in":true,"record_text":true,"max_events":32,"max_bytes":524288})
}

#[tokio::test]
async fn public_mcp_recording_exports_and_replays_confirmed_fixture_interaction() {
    let h = Harness::new().await;
    let launched = h.fixture().await;
    let session = launched["session_id"].as_str().unwrap();
    let run = launched["run_id"].as_str().unwrap();
    let copied_workspace = launched["capabilities"]["launch"]["workspace"]["root"]
        .as_str()
        .expect("fixture must use copied workspace");
    let actor = "manual-recording-agent";
    h.call(
        "terminal_actor",
        json!({"session_id":session,"actor_id":actor}),
    )
    .await;
    let started_response = h
        .call(
            "terminal_recording",
            recording_args("start", session, run, "mcp-replay"),
        )
        .await;
    assert!(
        !started_response["capture_surface"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item == "observe")
    );
    let started = h
        .call(
            "terminal_recording",
            recording_args("status", session, run, "mcp-replay"),
        )
        .await;
    assert_eq!(
        started["records"][0]["event"]["operation"]["launch"]["args"],
        json!([])
    );
    assert!(
        h.raw(
            "terminal_recording",
            recording_args("start", session, run, "mcp-replay")
        )
        .await
        .is_err(),
        "a second active recording must be rejected"
    );

    let before_input = h
        .call(
            "terminal_observe",
            json!({"session_id":session,"response":{"mode":"full","max_bytes":32768}}),
        )
        .await;
    let context = json!({"actor_id":actor,"action_id":"type-probe","expected_run_id":run});
    h.call(
        "terminal_type",
        json!({"session_id":session,"text":"manual-recording-probe","context":context}),
    )
    .await;
    let wait = h.call("terminal_wait_ready", json!({"session_id":session,"predicate":{"kind":"text_contains","text":"manual-recording-probe"},"timeout_ms":5000})).await;
    assert_eq!(wait["matched"], true, "wait must be an observed match");
    let assertion = h.call("terminal_assert", json!({"session_id":session,"assertion":{"kind":"text_contains","text":"manual-recording-probe"}})).await;
    assert_eq!(assertion["passed"], true);

    h.call(
        "terminal_recording",
        recording_args("stop", session, run, "mcp-replay"),
    )
    .await;
    assert!(
        h.raw(
            "terminal_recording",
            recording_args("start", session, run, "mcp-replay")
        )
        .await
        .is_err(),
        "a finalized recording ID must remain immutable"
    );
    let trace = h
        .call(
            "terminal_recording",
            recording_args("status", session, run, "mcp-replay"),
        )
        .await;
    let recorded_pre = &trace["records"][1]["event"]["operation"]["steps"][0]["input"]["baseline"];
    assert_eq!(
        recorded_pre["frame_id"], before_input["frame_id"],
        "recorder must use the atomic pre-dispatch receipt identity"
    );
    assert_eq!(
        recorded_pre["observation_revision"],
        before_input["observation_revision"]
    );
    let exported = h
        .call(
            "terminal_recording",
            recording_args("export", session, run, "mcp-replay"),
        )
        .await;
    assert_eq!(exported["replay_verified"], false);
    assert_eq!(
        exported["scenario"]["template_only"], true,
        "path-based executable needs explicit replay rebinding"
    );
    assert!(
        exported["unresolved_bindings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|binding| binding["field"] == "program")
    );
    assert_eq!(exported["scenario"]["steps"][0]["operation"], "launch");
    assert_eq!(exported["scenario"]["steps"][1]["operation"], "type");
    assert_eq!(exported["scenario"]["steps"][2]["operation"], "wait");
    assert_eq!(
        exported["scenario"]["steps"][3]["operation"], "assert",
        "export contents: {exported}"
    );
    let serialized = exported.to_string();
    assert!(!serialized.contains(session));
    assert!(!serialized.contains(run));
    assert!(!serialized.contains("view_token"));

    let replay_workspace = h.temp.path().join("fresh-replay-copy");
    std::fs::create_dir_all(&replay_workspace).unwrap();
    std::fs::copy(
        h.temp.path().join("project-source/fixture-sentinel.txt"),
        replay_workspace.join("fixture-sentinel.txt"),
    )
    .unwrap();
    assert_ne!(copied_workspace, replay_workspace.to_string_lossy());
    let mut resolved_scenario = exported["scenario"].clone();
    resolved_scenario["template_only"] = json!(false);
    resolved_scenario["steps"][0]["params"]["program"] =
        json!(env!("CARGO_BIN_EXE_terminal-fixture"));
    assert!(
        serde_json::from_value::<silicogen_terminal_runtime::scenario::Scenario>(
            resolved_scenario.clone()
        )
        .is_ok(),
        "exported preconditions must deserialize: {resolved_scenario}"
    );
    let replay = h
        .call(
            "terminal_scenario_run",
            json!({"workspace":replay_workspace,"scenario":resolved_scenario}),
        )
        .await;
    let job = replay["job_id"].as_str().unwrap();
    let mut report = Value::Null;
    for _ in 0..300 {
        report = h
            .call("terminal_scenario_status", json!({"job_id":job}))
            .await;
        if matches!(
            report["status"].as_str(),
            Some("passed" | "failed" | "timed_out" | "cancelled")
        ) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        report["status"], "passed",
        "exported scenario did not replay successfully: {report}"
    );
}

#[tokio::test]
async fn restart_finalizes_recording_and_a_timed_out_wait_marks_export_incomplete() {
    let h = Harness::new().await;
    let launched = h.fixture().await;
    let session = launched["session_id"].as_str().unwrap();
    let run = launched["run_id"].as_str().unwrap();
    h.call(
        "terminal_recording",
        recording_args("start", session, run, "restart-trace"),
    )
    .await;
    let result = h.call("terminal_wait_ready", json!({"session_id":session,"predicate":{"kind":"text_contains","text":"NEVER_APPEARS"},"timeout_ms":100})).await;
    assert_ne!(result["matched"], true);
    h.call("terminal_restart", json!({"session_id":session}))
        .await;
    let exported = h
        .call(
            "terminal_recording",
            recording_args("export", session, run, "restart-trace"),
        )
        .await;
    assert_eq!(exported["unsupported_actions"], 1);
    assert_eq!(exported["scenario"]["template_only"], true);
    let status = h
        .call("terminal_status", json!({"session_id":session}))
        .await;
    assert_ne!(
        status["run_id"], run,
        "restart must not keep appending to the old run recording"
    );
}

#[tokio::test]
async fn mcp_recording_redacts_declared_launch_secret_and_exports_external_binding() {
    let h = Harness::new().await;
    let secret = "synthetic-manual-recording-secret-4f26";
    let launched = h.fixture_with_env(json!({"SYNTHETIC_SECRET":secret})).await;
    let session = launched["session_id"].as_str().unwrap();
    let run = launched["run_id"].as_str().unwrap();
    let actor = "secret-recording-agent";
    h.call(
        "terminal_actor",
        json!({"session_id":session,"actor_id":actor}),
    )
    .await;
    let caps = h
        .call("terminal_capabilities", json!({"session_id":session}))
        .await;
    h.call(
        "terminal_recording",
        recording_args("start", session, run, "secret-trace"),
    )
    .await;
    h.call("terminal_type", json!({
        "session_id":session,
        "text":secret,
        "context":{"actor_id":actor,"action_id":"type-secret","expected_run_id":run,"expected_viewport_revision":caps["viewport_revision"]}
    })).await;
    h.call(
        "terminal_recording",
        recording_args("stop", session, run, "secret-trace"),
    )
    .await;
    let trace = h
        .call(
            "terminal_recording",
            recording_args("status", session, run, "secret-trace"),
        )
        .await;
    let exported = h
        .call(
            "terminal_recording",
            recording_args("export", session, run, "secret-trace"),
        )
        .await;
    let trace_json = trace.to_string();
    let export_json = exported.to_string();
    assert!(!trace_json.contains(secret));
    assert!(!export_json.contains(secret));
    assert!(
        trace_json.contains("<external:"),
        "redacted input must remain an explicit external reference"
    );
    assert!(
        exported["unresolved_bindings"]
            .as_array()
            .unwrap()
            .iter()
            .any(|binding| binding["field"].as_str().unwrap_or("").contains("text"))
    );
    assert_eq!(exported["scenario"]["template_only"], true);
    assert_eq!(exported["replay_verified"], false);
}
