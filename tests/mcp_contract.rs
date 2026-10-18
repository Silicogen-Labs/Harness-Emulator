use rmcp::{
    RoleClient, ServiceExt,
    model::{CallToolRequestParams, CallToolResult},
    service::{RunningService, ServiceError},
    transport::TokioChildProcess,
};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

struct Harness {
    client: RunningService<RoleClient, ()>,
    runtime: PathBuf,
    _temp: tempfile::TempDir,
}
impl Harness {
    async fn new() -> Self {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(".runtime/issue-2-tests");
        fs::create_dir_all(&root).unwrap();
        let temp = tempfile::tempdir_in(root).unwrap();
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
            _temp: temp,
        }
    }
    async fn raw(&self, name: &str, args: Value) -> Result<CallToolResult, ServiceError> {
        self.client
            .peer()
            .call_tool(
                CallToolRequestParams::new(name.to_owned())
                    .with_arguments(args.as_object().unwrap().clone()),
            )
            .await
    }
    async fn call(&self, name: &str, args: Value) -> Value {
        let result = self.raw(name, args).await.unwrap();
        assert_ne!(result.is_error, Some(true), "{result:?}");
        result.structured_content.unwrap()
    }
    async fn fixture(&self) -> Value {
        let launch=self.call("terminal_launch",json!({"program":env!("CARGO_BIN_EXE_terminal-fixture"),"cwd":self._temp.path(),"cols":160,"rows":45,"env_inherit":[]})).await;
        let wait=self.call("terminal_wait",json!({"session_id":launch["session_id"],"kind":"contains","value":"READY","timeout_ms":5000})).await;
        assert_eq!(wait["matched"], true);
        launch
    }
    async fn invalid(&self, name: &str, args: Value, field: &str, max: Option<u64>) {
        let error = self.raw(name, args).await.unwrap_err();
        let ServiceError::McpError(error) = error else {
            panic!("expected MCP error: {error:?}")
        };
        assert_eq!(serde_json::to_value(error.code).unwrap(), -32602);
        let data = error.data.unwrap();
        assert_eq!(data["field"], field, "{data}");
        assert!(!error.message.contains("DO_NOT_LEAK"));
        if let Some(max) = max {
            assert_eq!(data["maximum"], max, "{data}");
        }
    }
    async fn error_code(&self, name: &str, args: Value, code: &str) {
        let ServiceError::McpError(error) = self.raw(name, args).await.unwrap_err() else {
            panic!("expected MCP error")
        };
        assert_eq!(error.data.unwrap()["code"], code);
    }
}
impl Drop for Harness {
    fn drop(&mut self) {
        let _ = Command::new(env!("CARGO_BIN_EXE_silicogen-emulation"))
            .arg("--runtime-dir")
            .arg(&self.runtime)
            .arg("shutdown")
            .output();
    }
}

#[tokio::test]
async fn tools_list_exposes_nested_contracts_and_actual_bounds() {
    let h = Harness::new().await;
    let tools = h.client.peer().list_all_tools().await.unwrap();
    let schema = |name: &str| {
        serde_json::to_value(
            &tools
                .iter()
                .find(|tool| tool.name == name)
                .unwrap()
                .input_schema,
        )
        .unwrap()
    };
    let batch = schema("terminal_batch");
    assert!(
        batch["required"]
            .as_array()
            .unwrap()
            .contains(&json!("context"))
    );
    let all = batch.to_string();
    assert!(all.contains("expected_run_id") && all.contains("actions") && all.contains("paste"));
    assert!(
        schema("terminal_wait_ready")
            .to_string()
            .contains("stable_region")
    );
    let fresh_wait = schema("terminal_wait_fresh");
    assert!(fresh_wait.to_string().contains("prior_observation"));
    assert!(fresh_wait.to_string().contains("became_present"));
    assert!(fresh_wait.to_string().contains("3600000"));
    assert!(fresh_wait.to_string().contains("wait_id"));
    assert!(
        schema("terminal_mouse")
            .to_string()
            .contains("\"maximum\":20")
    );
    assert!(schema("terminal_wait").to_string().contains("60000"));
    assert!(
        schema("terminal_profiles")
            .to_string()
            .contains("test_policy")
    );
}

#[tokio::test]
async fn validation_errors_keep_field_paths_and_bounds_without_sending_input() {
    let h = Harness::new().await;
    let launch = h.fixture().await;
    let id = &launch["session_id"];
    h.invalid(
        "terminal_batch",
        json!({"session_id":id,"actions":[{"kind":"type","text":"DO_NOT_LEAK"}]}),
        "context",
        None,
    )
    .await;
    let context = json!({"actor_id":"contract-test","action_id":"invalid-batch","expected_run_id":launch["run_id"]});
    h.invalid("terminal_batch",json!({"session_id":id,"context":context,"actions":[{"kind":"type","text":"BAD_INPUT"},{"kind":"mouse","input":{"kind":"wheel","row":0,"col":0,"direction":"down","steps":25}}]}),"actions[1].input.steps",Some(20)).await;
    h.invalid(
        "terminal_batch",
        json!({"session_id":id,"context":context,"actions":[{"kind":"key","key":123}]}),
        "actions[0].key",
        None,
    )
    .await;
    h.invalid(
        "terminal_wait_ready",
        json!({"session_id":id,"predicate":{"kind":"DO_NOT_LEAK"}}),
        "predicate.kind",
        None,
    )
    .await;
    h.invalid(
        "terminal_wait",
        json!({"session_id":id,"kind":"contains","value":"READY","timeout_ms":120000}),
        "timeout_ms",
        Some(60000),
    )
    .await;
    h.invalid(
        "terminal_mouse",
        json!({"session_id":id,"action":"wheel_down","row":2,"col":2,"steps":25}),
        "steps",
        Some(20),
    )
    .await;
    let frame = h
        .call(
            "terminal_observe",
            json!({"session_id":id,"response":{"mode":"full"}}),
        )
        .await;
    assert!(!frame["text"].as_str().unwrap().contains("BAD_INPUT"));
}

#[tokio::test]
async fn wait_summaries_and_compact_fifty_cell_reads_fit_declared_budgets() {
    let h = Harness::new().await;
    let launch = h.fixture().await;
    let id = &launch["session_id"];
    let matched = h
        .raw(
            "terminal_wait",
            json!({"session_id":id,"kind":"contains","value":"READY","timeout_ms":5000}),
        )
        .await
        .unwrap();
    assert!(serde_json::to_vec(&matched).unwrap().len() <= 2048);
    assert!(
        matched
            .structured_content
            .as_ref()
            .unwrap()
            .get("snapshot")
            .is_none()
    );
    let pending=h.raw("terminal_wait_ready",json!({"session_id":id,"predicate":{"kind":"text_contains","text":"never present"},"timeout_ms":10000,"poll_ms":0})).await.unwrap();
    assert!(serde_json::to_vec(&pending).unwrap().len() <= 2048);
    let cells = h
        .call(
            "terminal_cells",
            json!({"session_id":id,"row":2,"col":0,"rows":5,"cols":10,"response":{"styles":true}}),
        )
        .await;
    assert!(serde_json::to_vec(&cells).unwrap().len() <= 4096);
    assert!(cells["cells"][0][0].is_array());
    assert!(cells["styles"].is_array());
    assert!(cells.get("palette").is_none());
    let full_wait=h.call("terminal_wait",json!({"session_id":id,"kind":"contains","value":"READY","timeout_ms":5000,"response":{"mode":"full"}})).await;
    assert!(full_wait["snapshot"]["styled_runs"].is_array());
    let timeout = h
        .call(
            "terminal_wait",
            json!({"session_id":id,"kind":"contains","value":"missing marker","timeout_ms":1}),
        )
        .await;
    assert_eq!(timeout["matched"], false);
    assert_eq!(timeout["timed_out"], true);
    assert!(timeout.get("snapshot").is_none());
    eprintln!(
        "MCP wire bytes: matched={}, pending={}, fifty_cells={}",
        serde_json::to_vec(&matched).unwrap().len(),
        serde_json::to_vec(&pending).unwrap().len(),
        serde_json::to_vec(&cells).unwrap().len()
    );
}

#[tokio::test]
async fn observation_modes_are_bounded_and_diff_failures_never_fall_back() {
    let h = Harness::new().await;
    let launch = h.fixture().await;
    let id = &launch["session_id"];
    let first = h.call("terminal_observe", json!({"session_id":id})).await;
    assert!(first.get("text").is_some());
    assert!(first.get("styled_runs").is_none());
    let baseline = json!({"session_id":id,"run_id":first["run_id"],"frame_id":first["frame_id"]});
    h.error_code("terminal_observe",json!({"session_id":id,"response":{"mode":"diff","baseline":{"session_id":"another-session","run_id":first["run_id"],"frame_id":first["frame_id"]}}}),"baseline_incompatible").await;
    h.call(
        "terminal_save_frame",
        json!({"session_id":id,"name":"before-resize"}),
    )
    .await;
    h.call(
        "terminal_type",
        json!({"session_id":id,"text":"diff-check"}),
    )
    .await;
    h.call(
        "terminal_wait",
        json!({"session_id":id,"kind":"contains","value":"Typed: diff-check","timeout_ms":5000}),
    )
    .await;
    let diff = h
        .call(
            "terminal_observe",
            json!({"session_id":id,"response":{"mode":"diff","baseline":baseline}}),
        )
        .await;
    assert!(diff["changed_rows"].as_u64().unwrap() > 0);
    assert!(diff.get("text").is_none());
    let budget = serde_json::to_vec(&first).unwrap().len() - 50;
    let small = h
        .call(
            "terminal_observe",
            json!({"session_id":id,"response":{"mode":"text","max_bytes":budget}}),
        )
        .await;
    assert!(serde_json::to_vec(&small).unwrap().len() <= budget);
    assert_eq!(small["response"]["truncated"], true);
    let full = h
        .call(
            "terminal_observe",
            json!({"session_id":id,"response":{"mode":"full"}}),
        )
        .await;
    assert!(full["styled_runs"].is_array());
    assert!(full["modes"].is_object());
    h.error_code(
        "terminal_observe",
        json!({"session_id":id,"response":{"mode":"full","max_bytes":256}}),
        "response_budget_exceeded",
    )
    .await;
    for (run, frame, code) in [
        (
            first["run_id"].clone(),
            json!("expired-frame"),
            "baseline_unavailable",
        ),
        (
            json!("another-run"),
            first["frame_id"].clone(),
            "baseline_incompatible",
        ),
    ] {
        h.error_code("terminal_observe",json!({"session_id":id,"response":{"mode":"diff","baseline":{"session_id":id,"run_id":run,"frame_id":frame}}}),code).await;
    }
    h.call(
        "terminal_resize",
        json!({"session_id":id,"cols":120,"rows":40}),
    )
    .await;
    h.error_code(
        "terminal_observe",
        json!({"session_id":id,"response":{"mode":"diff","baseline":baseline}}),
        "baseline_incompatible",
    )
    .await;
    h.error_code(
        "terminal_diff",
        json!({"session_id":id,"name":"absent-baseline"}),
        "baseline_unavailable",
    )
    .await;
}

#[tokio::test]
async fn response_options_on_input_are_rejected_before_dispatch() {
    use silicogen_terminal_runtime::{
        client::{RuntimeClient, RuntimeError},
        projection::ResponseOptions,
        protocol::RpcRequest,
    };
    let h = Harness::new().await;
    let launch = h.fixture().await;
    let id = launch["session_id"].as_str().unwrap();
    let client = RuntimeClient::new(&h.runtime);
    let error = client
        .request_v2(RpcRequest::Contract {
            revision: 2,
            request: Box::new(RpcRequest::Type {
                session_id: id.into(),
                text: "BAD_OPTION_INPUT".into(),
            }),
            response: Some(ResponseOptions::default()),
        })
        .await
        .unwrap_err();
    assert_eq!(
        error.downcast_ref::<RuntimeError>().unwrap().0.code,
        "unsupported_response"
    );
    let frame = h.call("terminal_observe", json!({"session_id":id})).await;
    assert!(!frame["text"].as_str().unwrap().contains("BAD_OPTION_INPUT"));
}

#[tokio::test]
async fn response_budget_rejections_are_reported_as_invalid_parameters() {
    let h = Harness::new().await;
    let launch = h.fixture().await;
    let error = h
        .raw(
            "terminal_observe",
            json!({"session_id":launch["session_id"],"response":{"mode":"full","max_bytes":256}}),
        )
        .await
        .unwrap_err();
    let ServiceError::McpError(error) = error else {
        panic!("expected MCP invalid-params: {error:?}")
    };
    assert_eq!(serde_json::to_value(error.code).unwrap(), -32602);
    assert_eq!(error.data.unwrap()["code"], "response_budget_exceeded");
}

#[tokio::test]
async fn compact_styles_match_legacy_cells_and_summary_has_no_grid() {
    use silicogen_terminal_runtime::{client::RuntimeClient, protocol::RpcRequest};
    let h = Harness::new().await;
    let launch = h.fixture().await;
    let id = launch["session_id"].as_str().unwrap();
    let legacy = RuntimeClient::new(&h.runtime)
        .request(RpcRequest::Cells {
            session_id: id.into(),
            row: 0,
            col: 0,
            rows: 3,
            cols: 10,
        })
        .await
        .unwrap();
    let compact = h
        .call(
            "terminal_cells",
            json!({"session_id":id,"row":0,"col":0,"rows":3,"cols":10}),
        )
        .await;
    for (row, original) in legacy["cells"].as_array().unwrap().iter().enumerate() {
        for (col, cell) in original.as_array().unwrap().iter().enumerate() {
            let encoded = &compact["cells"][row][col];
            let style = &compact["styles"][encoded[2].as_u64().unwrap() as usize];
            assert_eq!(encoded[0], cell["text"]);
            assert_eq!(encoded[1], cell["width"]);
            assert_eq!(style["foreground"], cell["resolved_foreground"]);
            assert_eq!(style["background"], cell["resolved_background"]);
            assert_eq!(style["bold"], cell["bold"]);
        }
    }
    let summary=h.call("terminal_cells",json!({"session_id":id,"row":0,"col":0,"rows":3,"cols":10,"response":{"mode":"summary"}})).await;
    assert!(summary.get("cells").is_none());
    assert!(summary.get("styles").is_none());
}

#[tokio::test]
async fn scenarios_validate_all_steps_before_dispatch_and_support_projections() {
    let h = Harness::new().await;
    let launch = h.fixture().await;
    let id = &launch["session_id"];
    let invalid = json!({"scenario":{"version":1,"name":"reject-before-input","steps":[
        {"id":"input","operation":"type","params":{"session_id":id,"text":"BAD_SCENARIO_INPUT"}},
        {"id":"bad","operation":"mouse","params":{"session_id":id,"input":{"kind":"wheel","row":0,"col":0,"direction":"down","steps":25}}}
    ]}});
    h.invalid(
        "terminal_scenario_run",
        invalid,
        "scenario.steps[1].params.input.steps",
        Some(20),
    )
    .await;
    let frame = h.call("terminal_observe", json!({"session_id":id})).await;
    assert!(
        !frame["text"]
            .as_str()
            .unwrap()
            .contains("BAD_SCENARIO_INPUT")
    );
    let started=h.call("terminal_scenario_run",json!({"scenario":{"version":1,"name":"compact-scenario","steps":[{"id":"screen","operation":"observe","params":{"session_id":id,"response":{"mode":"summary"}},"expect":{"pointer":"/response/mode","equals":"summary"}}]}})).await;
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let status = h
            .call(
                "terminal_scenario_status",
                json!({"job_id":started["job_id"]}),
            )
            .await;
        if status["status"] == "passed" {
            assert!(status["steps"][0]["result"].get("text").is_none());
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "{status}");
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    }
}

#[tokio::test]
async fn cli_preserves_legacy_full_and_negotiates_explicit_projection() {
    let h = Harness::new().await;
    let launch = h.fixture().await;
    let id = launch["session_id"].as_str().unwrap();
    let cli = |extra: &[&str]| {
        let output = Command::new(env!("CARGO_BIN_EXE_silicogen-emulation"))
            .arg("--runtime-dir")
            .arg(&h.runtime)
            .args(["observe", id])
            .args(extra)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice::<Value>(&output.stdout).unwrap()
    };
    let legacy = cli(&[]);
    assert!(legacy["styled_runs"].is_array());
    assert!(legacy.get("response").is_none());
    let compact = cli(&["--response-json", r#"{"mode":"summary"}"#]);
    assert!(compact.get("text").is_none());
    assert_eq!(compact["response"]["contract_revision"], 2);
    let output = Command::new(env!("CARGO_BIN_EXE_silicogen-emulation"))
        .arg("schema")
        .output()
        .unwrap();
    assert!(output.status.success());
    let schema: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(schema.to_string().contains("ScenarioStep"));
    assert_eq!(
        schema,
        serde_json::from_str::<Value>(
            &fs::read_to_string(
                Path::new(env!("CARGO_MANIFEST_DIR")).join("scenarios/scenario.schema.json")
            )
            .unwrap()
        )
        .unwrap()
    );
}

#[tokio::test]
async fn old_daemon_is_probed_without_sending_an_action_or_restarting_it() {
    use silicogen_terminal_runtime::{
        client::RuntimeClient,
        daemon,
        protocol::{RpcRequest, RpcResponse},
    };
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(".runtime/issue-2-tests");
    fs::create_dir_all(&root).unwrap();
    let temp = tempfile::tempdir_in(root).unwrap();
    let socket = daemon::socket_path(temp.path()).unwrap();
    let listener = tokio::net::UnixListener::bind(socket).unwrap();
    let server = tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let mut reader = BufReader::new(stream);
            let mut line = String::new();
            if reader.read_line(&mut line).await.unwrap() == 0 {
                continue;
            } // ensure_daemon's connect probe
            let value: Value = serde_json::from_str(&line).unwrap();
            assert_eq!(value["request"]["action"], "ping");
            let response = RpcResponse::success(
                value["request_id"].as_str().unwrap().into(),
                json!({"status":"ok","protocol_version":1}),
            );
            reader
                .get_mut()
                .write_all(&serde_json::to_vec(&response).unwrap())
                .await
                .unwrap();
            reader.get_mut().write_all(b"\n").await.unwrap();
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(150), listener.accept())
                    .await
                    .is_err(),
                "old daemon received a post-negotiation connection"
            );
            break;
        }
    });
    let error = RuntimeClient::new(temp.path())
        .request_v2(RpcRequest::Type {
            session_id: "never-dispatched".into(),
            text: "BAD_INPUT".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(
        error
            .downcast_ref::<silicogen_terminal_runtime::validation::RequestError>()
            .unwrap()
            .code,
        "contract_mismatch"
    );
    server.await.unwrap();
}
