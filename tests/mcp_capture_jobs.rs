use rmcp::{
    RoleClient, ServiceExt,
    model::{CallToolRequestParams, CallToolResult},
    service::{RunningService, ServiceError},
    transport::TokioChildProcess,
};
use serde_json::{Value, json};
use std::{
    path::{Path, PathBuf},
    process::Command,
    time::Duration,
};

struct Harness {
    client: RunningService<RoleClient, ()>,
    runtime: PathBuf,
    temp: tempfile::TempDir,
}

impl Harness {
    async fn new() -> Self {
        let base = Path::new(env!("CARGO_MANIFEST_DIR")).join(".runtime/mcp-capture-jobs");
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
            Duration::from_secs(5),
            self.client.peer().call_tool(
                CallToolRequestParams::new(name.to_owned())
                    .with_arguments(args.as_object().unwrap().clone()),
            ),
        )
        .await
        .expect("capture MCP call timed out")
    }

    async fn call(&self, name: &str, args: Value) -> Value {
        let result = self.raw(name, args).await.unwrap();
        assert_ne!(result.is_error, Some(true), "{result:?}");
        result.structured_content.unwrap()
    }

    async fn fixture(&self) -> Value {
        let launch = self
            .call(
                "terminal_launch",
                json!({
                    "program": env!("CARGO_BIN_EXE_terminal-fixture"),
                    "args": ["--transient-toast"],
                    "cwd": self.temp.path(), "cols": 80, "rows": 24, "env_inherit": []
                }),
            )
            .await;
        let ready = self.call("terminal_wait", json!({
            "session_id": launch["session_id"], "kind": "contains", "value": "READY", "timeout_ms": 5000
        })).await;
        assert_eq!(ready["matched"], true);
        launch
    }

    async fn capture_request(
        &self,
        launch: &Value,
        handle: &str,
        appearance: &str,
        completion: Value,
        timeout_ms: u64,
        burst: Option<Value>,
    ) -> Value {
        let id = launch["session_id"].as_str().unwrap();
        let caps = self
            .call("terminal_capabilities", json!({"session_id": id}))
            .await;
        json!({
            "handle": handle,
            "authority": {
                "session_id": id,
                "run_id": caps["session"]["run_id"],
                "viewport_revision": caps["viewport_revision"],
                "cols": 80, "rows": 24
            },
            "appearance": {"query":{"kind":"text","value":appearance}},
            "completion": completion,
            "burst": burst,
            "budget":{"max_frames":24,"max_bytes":33554432,"max_frame_bytes":16777216},
            "timeout_ms": timeout_ms
        })
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = Command::new(env!("CARGO_BIN_EXE_silicogen-emulation"))
            .args(["--runtime-dir", self.runtime.to_str().unwrap(), "shutdown"])
            .output();
    }
}

async fn wait_capture(h: &Harness, handle: &str, expected: &str) -> Value {
    for _ in 0..100 {
        let status = h
            .call(
                "terminal_capture",
                json!({
                    "action":"status","handle":handle,"response":{"mode":"full","max_bytes":4194304}
                }),
            )
            .await;
        if status["capture_status"] == expected {
            return status;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("capture {handle} did not reach {expected}");
}

fn kill_owned_daemon(runtime: &Path) {
    let runtime = std::fs::canonicalize(runtime).unwrap();
    let pid = std::fs::read_dir("/proc")
        .unwrap()
        .flatten()
        .find_map(|entry| {
            let pid = entry.file_name().to_string_lossy().parse::<i32>().ok()?;
            let args = std::fs::read(entry.path().join("cmdline")).ok()?;
            let args = args
                .split(|byte| *byte == 0)
                .filter(|part| !part.is_empty())
                .collect::<Vec<_>>();
            let is_daemon = args.iter().any(|arg| *arg == b"daemon");
            let owns_runtime = args.windows(2).any(|pair| {
                pair[0] == b"--runtime-dir"
                    && Path::new(std::str::from_utf8(pair[1]).ok().unwrap_or("")) == runtime
            });
            (is_daemon && owns_runtime).then_some(pid)
        })
        .expect("locate only this test's runtime daemon");
    assert_eq!(unsafe { libc::kill(pid, libc::SIGKILL) }, 0);
    for _ in 0..100 {
        if !Path::new(&format!("/proc/{pid}")).exists() {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    panic!("test-owned daemon {pid} did not exit after SIGKILL");
}

#[tokio::test]
async fn mcp_capture_is_armed_before_input_and_latches_short_toast_replacement_with_burst_frames() {
    let h = Harness::new().await;
    let tools = h.client.peer().list_all_tools().await.unwrap();
    let schema = tools
        .iter()
        .find(|tool| tool.name == "terminal_capture")
        .expect("terminal_capture tool discovered");
    let schema = serde_json::to_value(&schema.input_schema).unwrap();
    assert!(schema.to_string().contains("materialize"));
    assert!(schema.to_string().contains("max_frames"));

    let launch = h.fixture().await;
    let request = h.capture_request(
        &launch,
        "short-toast-replacement",
        "SHORT TOAST",
        json!({"kind":"replaced","condition":{"query":{"kind":"text","value":"TOAST REPLACED"}}}),
        3000,
        Some(json!({"interval_ms":20,"duration_ms":600})),
    ).await;
    let armed = h
        .call(
            "terminal_capture",
            json!({"action":"start","request":request}),
        )
        .await;
    assert_eq!(armed["status"], "armed");
    assert_eq!(armed["armed_before_return"], true);
    assert_eq!(
        armed["sampling_guarantee"],
        "best_effort_not_every_transition"
    );

    h.call(
        "terminal_type",
        json!({"session_id":launch["session_id"],"text":"!"}),
    )
    .await;
    let completed = wait_capture(&h, "short-toast-replacement", "completed").await;
    assert_eq!(completed["appearance"]["matched"]["snippet"], "SHORT TOAST");
    assert_eq!(
        completed["transition"]["matched"]["snippet"],
        "TOAST REPLACED"
    );
    assert!(
        completed["frames"].as_array().unwrap().len() >= 3,
        "{completed}"
    );
    assert_eq!(completed["coverage"]["all_transitions_observed"], false);
    assert_eq!(completed["coverage"]["sampling_is_partial"], true);
    assert_eq!(
        completed["coverage"]["producer_may_skip_intermediate_states"],
        true
    );

    let appearance_index = completed["appearance"]["frame_index"].as_u64().unwrap();
    let materialized = h.call("terminal_capture", json!({
        "action":"materialize","handle":"short-toast-replacement","frame_index":appearance_index
    })).await;
    assert_eq!(materialized["status"], "materialized");
    let artifact = h.call("terminal_read_artifact", json!({
        "session_id":launch["session_id"],"artifact_id":materialized["artifact_id"],"offset":0,"length":16
    })).await;
    assert_eq!(artifact["encoding"], "base64");

    let released = h
        .call(
            "terminal_capture",
            json!({"action":"release","handle":"short-toast-replacement"}),
        )
        .await;
    assert_eq!(released["operation"], "released");
    assert_eq!(released["retained_bytes"], 0);
    assert_eq!(
        released["frames_omitted"].as_u64().unwrap(),
        completed["frame_count"].as_u64().unwrap()
    );
    assert!(h.raw("terminal_capture", json!({
        "action":"materialize","handle":"short-toast-replacement","frame_index":appearance_index
    })).await.is_err());
}

#[tokio::test]
async fn mcp_capture_cancel_deadline_stale_run_and_restart_are_explicit() {
    let h = Harness::new().await;
    let launch = h.fixture().await;
    let completion = Value::Null;
    let never = h
        .capture_request(
            &launch,
            "cancel-capture",
            "NEVER VISIBLE",
            completion.clone(),
            2000,
            None,
        )
        .await;
    h.call(
        "terminal_capture",
        json!({"action":"start","request":never}),
    )
    .await;
    let cancelled = h
        .call(
            "terminal_capture",
            json!({"action":"cancel","handle":"cancel-capture"}),
        )
        .await;
    assert_eq!(cancelled["capture_status"], "cancelled");
    h.call(
        "terminal_capture",
        json!({"action":"release","handle":"cancel-capture"}),
    )
    .await;

    let timeout = h
        .capture_request(
            &launch,
            "deadline-capture",
            "NEVER VISIBLE",
            completion.clone(),
            80,
            None,
        )
        .await;
    h.call(
        "terminal_capture",
        json!({"action":"start","request":timeout}),
    )
    .await;
    let timed_out = wait_capture(&h, "deadline-capture", "timed_out").await;
    assert_eq!(timed_out["failure"], "overall capture deadline expired");
    h.call(
        "terminal_capture",
        json!({"action":"release","handle":"deadline-capture"}),
    )
    .await;

    let stale = h
        .capture_request(
            &launch,
            "stale-run-capture",
            "NEVER VISIBLE",
            completion,
            2000,
            None,
        )
        .await;
    h.call(
        "terminal_capture",
        json!({"action":"start","request":stale}),
    )
    .await;
    h.call(
        "terminal_restart",
        json!({"session_id":launch["session_id"]}),
    )
    .await;
    let stale_status = wait_capture(&h, "stale-run-capture", "stale_run").await;
    assert_eq!(
        stale_status["armed_baseline"]["run_id"],
        stale["authority"]["run_id"]
    );
    h.call(
        "terminal_capture",
        json!({"action":"release","handle":"stale-run-capture"}),
    )
    .await;

    // A separate active capture is durably checkpointed before daemon shutdown.
    let after_restart_launch = h.fixture().await;
    let restart_request = h
        .capture_request(
            &after_restart_launch,
            "daemon-restart-capture",
            "NEVER VISIBLE",
            Value::Null,
            5000,
            None,
        )
        .await;
    h.call(
        "terminal_capture",
        json!({"action":"start","request":restart_request}),
    )
    .await;
    kill_owned_daemon(&h.runtime);
    tokio::time::sleep(Duration::from_millis(80)).await;
    let interrupted = h
        .call(
            "terminal_capture",
            json!({"action":"status","handle":"daemon-restart-capture"}),
        )
        .await;
    assert_eq!(interrupted["capture_status"], "interrupted");
    assert_eq!(interrupted["coverage"]["restored_frames_unavailable"], 0);
}

#[tokio::test]
async fn mcp_capture_rejects_invalid_budgets_before_any_session_input() {
    let h = Harness::new().await;
    let launch = h.fixture().await;
    let before = h
        .call(
            "terminal_observe",
            json!({"session_id":launch["session_id"],"response":{"mode":"text"}}),
        )
        .await;
    let mut request = h
        .capture_request(
            &launch,
            "invalid-budget",
            "SHORT TOAST",
            Value::Null,
            1000,
            None,
        )
        .await;
    request["budget"]["max_frames"] = json!(129);
    let result = h
        .raw(
            "terminal_capture",
            json!({"action":"start","request":request}),
        )
        .await
        .unwrap_err();
    let ServiceError::McpError(error) = result else {
        panic!("expected MCP validation failure: {result:?}")
    };
    assert_eq!(serde_json::to_value(error.code).unwrap(), -32602);
    let after = h
        .call(
            "terminal_observe",
            json!({"session_id":launch["session_id"],"response":{"mode":"text"}}),
        )
        .await;
    assert_eq!(before["text"], after["text"]);
}

#[tokio::test]
async fn mcp_scenario_executor_arms_capture_before_input_and_asserts_transition() {
    let h = Harness::new().await;
    let scenario = h
        .call(
            "terminal_scenario_run",
            json!({"scenario":{
                "version":1,"name":"scenario-owned transient capture","timeout_ms":15000,
                "steps":[
                    {"id":"launch-fixture","operation":"launch","timeout_ms":5000,"params":{
                        "program":env!("CARGO_BIN_EXE_terminal-fixture"),
                        "args":["--transient-toast"],"cwd":h.temp.path(),"cols":80,"rows":24
                    }},
                    {"id":"arm-before-input","operation":"capture_job","timeout_ms":2000,"params":{
                        "action":"start","request":{
                            "handle":"scenario-toast-capture",
                            "authority":{"session_id":"${session}","run_id":"${run_id}","viewport_revision":0,"cols":80,"rows":24},
                            "appearance":{"query":{"kind":"text","value":"SHORT TOAST"}},
                            "completion":{"kind":"replaced","condition":{"query":{"kind":"text","value":"TOAST REPLACED"}}},
                            "budget":{"max_frames":12,"max_bytes":33554432,"max_frame_bytes":16777216},
                            "timeout_ms":8000
                        }
                    }},
                    {"id":"trigger-toast","operation":"type","timeout_ms":2000,"params":{"session_id":"${session}","text":"!"}},
                    {"id":"wait-replacement","operation":"wait","timeout_ms":5000,"params":{"session_id":"${session}","predicate":{"kind":"text_contains","text":"TOAST REPLACED"},"timeout_ms":5000}},
                    {"id":"assert-capture-transition","operation":"capture_job","timeout_ms":2000,"params":{"action":"status","handle":"scenario-toast-capture"},"expect":{"pointer":"/transition/frame_id","truthy":true}}
                ]
            }})
        )
        .await;
    let job_id = scenario["job_id"].as_str().unwrap();
    let mut report = Value::Null;
    for _ in 0..300 {
        report = h
            .call("terminal_scenario_status", json!({"job_id":job_id}))
            .await;
        if matches!(
            report["status"].as_str(),
            Some("passed" | "failed" | "timed_out")
        ) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(report["status"], "passed", "{report}");
    let steps = report["steps"].as_array().unwrap();
    assert_eq!(steps.len(), 5);
    let capture = steps
        .iter()
        .find(|step| step["step_id"] == "assert-capture-transition")
        .unwrap();
    assert_eq!(capture["status"], "passed");
    assert_eq!(capture["result"]["capture_status"], "completed");
    assert!(
        capture["result"]["transition"]["frame_id"]
            .as_str()
            .is_some()
    );
}

#[tokio::test]
async fn mcp_capture_terminates_explicitly_when_session_closes_or_process_exits() {
    let h = Harness::new().await;
    let closed_session = h.fixture().await;
    let request = h
        .capture_request(
            &closed_session,
            "close-owned-capture",
            "NEVER VISIBLE",
            Value::Null,
            10000,
            None,
        )
        .await;
    h.call(
        "terminal_capture",
        json!({"action":"start","request":request}),
    )
    .await;
    h.call(
        "terminal_close",
        json!({"session_id":closed_session["session_id"]}),
    )
    .await;
    let closed = wait_capture(&h, "close-owned-capture", "session_closed").await;
    assert_eq!(
        closed["failure"],
        "owning terminal session closed before capture completed"
    );

    let exited = h
        .call(
            "terminal_launch",
            json!({
                "program":env!("CARGO_BIN_EXE_terminal-fixture"),"args":["--exit-on-bang"],
                "cwd":h.temp.path(),"cols":80,"rows":24,"env_inherit":[]
            }),
        )
        .await;
    h.call(
        "terminal_wait",
        json!({
            "session_id":exited["session_id"],"kind":"contains","value":"READY","timeout_ms":5000
        }),
    )
    .await;
    let request = h
        .capture_request(
            &exited,
            "process-exit-capture",
            "NEVER VISIBLE",
            Value::Null,
            10000,
            None,
        )
        .await;
    h.call(
        "terminal_capture",
        json!({"action":"start","request":request}),
    )
    .await;
    h.call(
        "terminal_type",
        json!({"session_id":exited["session_id"],"text":"!"}),
    )
    .await;
    let ended = wait_capture(&h, "process-exit-capture", "process_exited").await;
    assert!(
        ended["failure"]
            .as_str()
            .unwrap()
            .contains("owned application process exited")
    );
}
