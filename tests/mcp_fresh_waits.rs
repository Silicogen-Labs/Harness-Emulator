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
};

struct Harness {
    client: RunningService<RoleClient, ()>,
    runtime: PathBuf,
    _temp: tempfile::TempDir,
}
impl Harness {
    async fn new() -> Self {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(".runtime/mcp3");
        std::fs::create_dir_all(&root).unwrap();
        let temp = tempfile::tempdir_in(root).unwrap();
        let runtime = temp.path().join("runtime");
        let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_silicogen-emulation"));
        command
            .args(["--runtime-dir", runtime.to_str().unwrap(), "mcp"])
            .env_clear()
            .env("PATH", "/usr/bin:/bin")
            .env("LANG", "C.UTF-8")
            .env("CODEX_NETWORK_ALLOW_LOCAL_BINDING", "1")
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
    async fn session(&self) -> Value {
        let launch=self.call("terminal_launch",json!({"program":env!("CARGO_BIN_EXE_terminal-fixture"),"cwd":self._temp.path(),"cols":80,"rows":24,"env_inherit":[]})).await;
        let wait=self.call("terminal_wait",json!({"session_id":launch["session_id"],"kind":"contains","value":"READY","timeout_ms":5000})).await;
        assert_eq!(wait["matched"], true);
        launch
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
async fn mcp_locate_and_fresh_wait_bind_to_observed_identity_and_reject_resize() {
    let h = Harness::new().await;
    let launch = h.session().await;
    let id = launch["session_id"].as_str().unwrap();
    let baseline = h
        .call(
            "terminal_observe",
            json!({"session_id":id,"response":{"mode":"summary"}}),
        )
        .await;
    let identity = json!({"session_id":id,"run_id":baseline["run_id"],"frame_id":baseline["frame_id"],"observation_revision":baseline["observation_revision"],"viewport_revision":baseline["viewport_revision"],"cols":baseline["cols"],"rows":baseline["rows"]});
    assert!(identity["observation_revision"].as_u64().is_some());
    let locate_result = h
        .raw(
            "terminal_locate",
            json!({"session_id":id,"query":{"kind":"text","value":"READY"},"response":{"mode":"summary"}}),
        )
        .await
        .unwrap();
    let located = locate_result.structured_content.unwrap();
    assert_eq!(located["any_match"], true);
    assert_eq!(located["match_count"], 1);
    assert!(located["matches"][0]["rect"]["row_start"].is_number());
    let text_summary = locate_result
        .content
        .iter()
        .filter_map(|block| block.as_text())
        .find_map(|block| serde_json::from_str::<Value>(&block.text).ok())
        .unwrap();
    assert_eq!(text_summary["match_count"], 1);
    assert!(text_summary["match"]["rect"]["row_start"].is_number());
    let first=h.call("terminal_wait_fresh",json!({"session_id":id,"prior_observation":identity,"spec":{"query":{"kind":"text","value":"READY"},"transition":"became_present"},"timeout_ms":5000,"poll_ms":0})).await;
    assert_eq!(first["status"], "pending");
    assert!(first["wait_id"].as_str().is_some());
    let resumed = h
        .call(
            "terminal_wait_fresh",
            json!({"wait_id":first["wait_id"],"poll_ms":0}),
        )
        .await;
    assert_eq!(resumed["baseline"], identity);
    h.call(
        "terminal_resize",
        json!({"session_id":id,"cols":81,"rows":24}),
    )
    .await;
    let invalidated = h
        .call(
            "terminal_wait_fresh",
            json!({"wait_id":first["wait_id"],"poll_ms":0}),
        )
        .await;
    assert_eq!(invalidated["status"], "baseline_incompatible");
}

#[tokio::test]
async fn mcp_fresh_wait_requires_a_retained_prior_observation() {
    let h = Harness::new().await;
    let launch = h.session().await;
    let error=h.raw("terminal_wait_fresh",json!({"session_id":launch["session_id"],"spec":{"query":{"kind":"text","value":"READY"},"transition":"current_presence"},"timeout_ms":1000,"poll_ms":0})).await.unwrap_err();
    let ServiceError::McpError(error) = error else {
        panic!("expected MCP invalid-params: {error:?}")
    };
    assert_eq!(serde_json::to_value(error.code).unwrap(), -32602);
    assert_eq!(error.data.unwrap()["code"], "baseline_unavailable");
}

#[tokio::test]
async fn fresh_wait_deadline_is_terminal_even_when_no_client_resumes_it() {
    let h = Harness::new().await;
    let launch = h.session().await;
    let id = launch["session_id"].as_str().unwrap();
    let baseline = h
        .call(
            "terminal_observe",
            json!({"session_id":id,"response":{"mode":"summary"}}),
        )
        .await;
    let identity = json!({"session_id":id,"run_id":baseline["run_id"],"frame_id":baseline["frame_id"],"observation_revision":baseline["observation_revision"],"viewport_revision":baseline["viewport_revision"],"cols":baseline["cols"],"rows":baseline["rows"]});
    let started = h
        .call(
            "terminal_wait_fresh",
            json!({"session_id":id,"prior_observation":identity,"spec":{"query":{"kind":"text","value":"NEVER_PRESENT"},"transition":"became_present"},"timeout_ms":25,"poll_ms":0}),
        )
        .await;
    assert_eq!(started["status"], "pending");
    tokio::time::sleep(std::time::Duration::from_millis(60)).await;
    let expired = h
        .call(
            "terminal_wait_fresh",
            json!({"wait_id":started["wait_id"],"poll_ms":0}),
        )
        .await;
    assert_eq!(expired["status"], "timed_out");
}

#[tokio::test]
async fn legacy_tool_output_quiet_completes_on_timer_without_more_output() {
    let h = Harness::new().await;
    let launch = h.session().await;
    let started = std::time::Instant::now();
    let result = h
        .call(
            "terminal_wait",
            json!({
                "session_id":launch["session_id"],
                "kind":"output_quiet",
                "duration_ms":100,
                "timeout_ms":1500,
                "poll_ms":700
            }),
        )
        .await;
    assert_eq!(result["status"], "completed");
    assert_eq!(result["matched"], true);
    assert!(started.elapsed() >= std::time::Duration::from_millis(80));
    assert!(started.elapsed() < std::time::Duration::from_millis(700));
}

#[tokio::test]
async fn fresh_wait_reaches_deadline_while_output_is_quiet() {
    let h = Harness::new().await;
    let launch = h.session().await;
    let id = launch["session_id"].as_str().unwrap();
    let baseline = h
        .call(
            "terminal_observe",
            json!({"session_id":id,"response":{"mode":"summary"}}),
        )
        .await;
    let identity = json!({"session_id":id,"run_id":baseline["run_id"],"frame_id":baseline["frame_id"],"observation_revision":baseline["observation_revision"],"viewport_revision":baseline["viewport_revision"],"cols":baseline["cols"],"rows":baseline["rows"]});
    let started = std::time::Instant::now();
    let result = h
        .call(
            "terminal_wait_fresh",
            json!({
                "session_id":id,
                "prior_observation":identity,
                "spec":{"query":{"kind":"text","value":"NEVER_APPEARS"},"transition":"became_present"},
                "timeout_ms":120,
                "poll_ms":1000
            }),
        )
        .await;
    assert_eq!(result["status"], "timed_out");
    assert!(started.elapsed() >= std::time::Duration::from_millis(100));
    assert!(started.elapsed() < std::time::Duration::from_millis(800));
}

#[tokio::test]
async fn cancelling_a_wait_does_not_wait_for_the_poll_to_finish() {
    let h = Harness::new().await;
    let launch = h.session().await;
    let id = launch["session_id"].as_str().unwrap();
    let started = h
        .call(
            "terminal_wait_ready",
            json!({
                "session_id":id,
                "predicate":{"kind":"text_contains","text":"NEVER_APPEARS"},
                "timeout_ms":5000,
                "poll_ms":0
            }),
        )
        .await;
    assert_eq!(started["status"], "pending");
    let wait_id = started["wait_id"].as_str().unwrap().to_owned();
    let poll = h.raw(
        "terminal_wait_ready",
        json!({"session_id":id,"wait_id":wait_id,"timeout_ms":5000,"poll_ms":4000}),
    );
    let cancel = async {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let now = std::time::Instant::now();
        let result = h
            .call(
                "terminal_wait_ready",
                json!({"session_id":id,"wait_id":wait_id,"timeout_ms":5000,"poll_ms":0,"cancel":true}),
            )
            .await;
        (result, now.elapsed())
    };
    let (polled, (cancelled, cancel_elapsed)) = tokio::join!(poll, cancel);
    let polled = polled.unwrap().structured_content.unwrap();
    assert_eq!(cancelled["status"], "cancelled");
    assert_eq!(polled["status"], "cancelled");
    assert!(cancel_elapsed < std::time::Duration::from_millis(500));
}

#[tokio::test]
async fn output_event_wakes_a_wait_before_its_poll_timer() {
    let h = Harness::new().await;
    let launch = h.session().await;
    let id = launch["session_id"].as_str().unwrap();
    let pending = h
        .call(
            "terminal_wait_ready",
            json!({
                "session_id":id,
                "predicate":{"kind":"text_contains","text":"Selection: Beta"},
                "timeout_ms":3000,
                "poll_ms":0
            }),
        )
        .await;
    let wait_id = pending["wait_id"].as_str().unwrap().to_owned();
    let started = std::time::Instant::now();
    let poll = h.raw(
        "terminal_wait_ready",
        json!({"session_id":id,"wait_id":wait_id,"timeout_ms":3000,"poll_ms":2500}),
    );
    let input = async {
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        h.call("terminal_key", json!({"session_id":id,"key":"Down"}))
            .await
    };
    let (polled, _) = tokio::join!(poll, input);
    let polled = polled.unwrap().structured_content.unwrap();
    assert_eq!(polled["status"], "completed");
    assert!(started.elapsed() < std::time::Duration::from_millis(1500));
}

#[tokio::test]
async fn fresh_wait_validation_errors_are_mcp_invalid_params() {
    let h = Harness::new().await;
    let launch = h.session().await;
    let id = launch["session_id"].as_str().unwrap();
    let error = h
        .raw(
            "terminal_wait_fresh",
            json!({"session_id":id,"prior_observation":{},"spec":{"query":{"kind":"text","value":"READY"},"transition":"current_presence"},"timeout_ms":1000,"poll_ms":0}),
        )
        .await
        .unwrap_err();
    let ServiceError::McpError(error) = error else {
        panic!("expected typed invalid-params: {error:?}")
    };
    assert_eq!(serde_json::to_value(error.code).unwrap(), -32602);
    assert_eq!(error.data.unwrap()["field"], "prior_observation.session_id");
}

#[tokio::test]
async fn generic_cli_agent_uses_revision_two_for_locators() {
    let h = Harness::new().await;
    let launch = h.session().await;
    let id = launch["session_id"].as_str().unwrap();
    let params = json!({"session_id":id,"query":{"kind":"text","value":"READY"}});
    let output = Command::new(env!("CARGO_BIN_EXE_silicogen-emulation"))
        .arg("--runtime-dir")
        .arg(&h.runtime)
        .args(["agent", "locate", "--json", &params.to_string()])
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let result: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(result["response"]["contract_revision"], 2);
    assert_eq!(result["match_count"], 1);
    assert!(result["matches"][0]["rect"]["row_start"].as_u64().unwrap() < 24);
}
