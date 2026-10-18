use rmcp::{
    RoleClient, ServiceExt,
    model::{CallToolRequestParams, CallToolResult},
    service::{RunningService, ServiceError},
    transport::TokioChildProcess,
};
use serde_json::{Value, json};
use std::{
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::Command,
    time::{Duration, Instant},
};

struct Harness {
    client: RunningService<RoleClient, ()>,
    runtime: PathBuf,
    temp: tempfile::TempDir,
}

impl Harness {
    async fn new() -> Self {
        let base = Path::new(env!("CARGO_MANIFEST_DIR")).join(".runtime/issue5-mcp");
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

    async fn raw(&self, tool: &str, args: Value) -> Result<CallToolResult, ServiceError> {
        tokio::time::timeout(
            Duration::from_secs(8),
            self.client.peer().call_tool(
                CallToolRequestParams::new(tool.to_owned())
                    .with_arguments(args.as_object().unwrap().clone()),
            ),
        )
        .await
        .expect("MCP request exceeded test bound")
    }

    async fn call(&self, tool: &str, args: Value) -> Value {
        let result = self.raw(tool, args).await.unwrap();
        assert_ne!(result.is_error, Some(true), "{tool}: {result:?}");
        result.structured_content.unwrap()
    }

    async fn launch(&self, cwd: &Path, args: &[String]) -> Value {
        self.call(
            "terminal_launch",
            json!({
                "program": env!("CARGO_BIN_EXE_terminal-fixture"),
                "args": args,
                "cwd": cwd,
                "cols": 80,
                "rows": 24,
                "env_inherit": []
            }),
        )
        .await
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

fn reserve_port() -> u16 {
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    listener.local_addr().unwrap().port()
}

async fn wait_for_port(port: u16) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline {
        if TcpStream::connect(("127.0.0.1", port)).is_ok() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("owned fixture backend did not listen on {port}");
}

async fn launch_api(h: &Harness, cwd: &Path) -> (Value, u16) {
    let port = reserve_port();
    let backend = h
        .launch(
            cwd,
            &["--adapter-http".into(), "--port".into(), port.to_string()],
        )
        .await;
    wait_for_port(port).await;
    (backend, port)
}

fn app_bind(backend: &Value, tui: &Value, allowed: &[&str]) -> Value {
    json!({
        "action":"bind",
        "backend_session_id":backend["session_id"],
        "tui_session_id":tui["session_id"],
        "allowed_application_sessions":allowed
    })
}

fn app_ui_state(backend: &Value, tui: &Value, frame: &Value, sequence: u64) -> Value {
    json!({
        "version": 1,
        "producer_instance": "fixture-ui-v1",
        "backend_session_id": backend["session_id"],
        "backend_run_id": backend["run_id"],
        "observation": {
            "session_id": tui["session_id"],
            "run_id": frame["run_id"],
            "frame_id": frame["frame_id"],
            "observation_revision": frame["observation_revision"],
            "viewport_revision": frame["viewport_revision"],
            "cols": frame["cols"],
            "rows": frame["rows"]
        },
        "sequence": sequence,
        "active_application_session_id": "fixture-session",
        "route": "home",
        "dialog": "subagents",
        "filter": "compile",
        "focus": "filter"
    })
}

fn completed_tool_message(session: &str, message: &str, parent: &str, call: &str) -> Value {
    json!({
        "info": {"id":message,"sessionID":session,"role":"assistant","parentID":parent,"time":{"created":1,"completed":2},"finish":"stop"},
        "parts": [{"type":"tool","sessionID":session,"messageID":message,"callID":call,"tool":"write","state":{"status":"completed","output":"written"}}]
    })
}

#[tokio::test]
async fn tools_list_and_scroll_dispatch_preserve_actor_run_viewport_and_report_progress() {
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
    let scroll_schema = schema("terminal_scroll_until");
    for field in ["context", "verified_at", "bindings", "target", "max_steps"] {
        assert!(
            scroll_schema.to_string().contains(field),
            "missing {field} in {scroll_schema}"
        );
    }
    assert!(
        schema("terminal_app_adapter")
            .to_string()
            .contains("allowed_application_sessions")
    );

    let launched = h
        .launch(h.temp.path(), &["--virtual-transcript".into()])
        .await;
    let session_id = launched["session_id"].as_str().unwrap();
    let frame = h
        .call(
            "terminal_observe",
            json!({"session_id":session_id,"response":{"mode":"full","max_bytes":4000000}}),
        )
        .await;
    assert!(frame["text"].as_str().unwrap().contains("newest messages"));
    let caps = h
        .call("terminal_capabilities", json!({"session_id":session_id}))
        .await;
    let unowned = json!({
        "session_id":session_id,
        "context":{"actor_id":"unclaimed-agent","action_id":"unowned-scroll","expected_run_id":caps["session"]["run_id"],"expected_viewport_revision":caps["viewport_revision"],"controller_generation":caps["controller"]["generation"]},
        "direction":"down","region":{"row_start":0,"col_start":0,"row_end":24,"col_end":80},
        "target":{"query":{"kind":"text","value":"older catch-up recap"},"transition":"current_presence"},
        "verified_at":{"session_id":session_id,"run_id":frame["run_id"],"frame_id":frame["frame_id"],"observation_revision":frame["observation_revision"],"viewport_revision":frame["viewport_revision"],"cols":frame["cols"],"rows":frame["rows"]},
        "bindings":[{"kind":"mouse","input":{"kind":"wheel","row":2,"col":4,"direction":"down","steps":1}}],
        "max_steps":3,"max_no_progress":1,"timeout_ms":1000
    });
    assert!(h.raw("terminal_scroll_until", unowned).await.is_err());
    let still_unowned = h
        .call("terminal_capabilities", json!({"session_id":session_id}))
        .await;
    assert!(
        still_unowned["controller"]["actor_id"].is_null(),
        "scroll must not claim control: {still_unowned}"
    );
    let still_top = h
        .call(
            "terminal_observe",
            json!({"session_id":session_id,"response":{"mode":"full","max_bytes":4000000}}),
        )
        .await;
    assert!(
        still_top["text"]
            .as_str()
            .unwrap()
            .contains("newest messages")
    );
    let claimed = h
        .call(
            "terminal_actor",
            json!({"session_id":session_id,"actor_id":"scroll-test-agent"}),
        )
        .await;
    let verified_at = json!({
        "session_id":session_id,
        "run_id":frame["run_id"],
        "frame_id":frame["frame_id"],
        "observation_revision":frame["observation_revision"],
        "viewport_revision":frame["viewport_revision"],
        "cols":frame["cols"],
        "rows":frame["rows"]
    });
    let args = |action_id: &str,
                direction: &str,
                target: &str,
                max_no_progress: usize,
                timeout_ms: u64| {
        json!({
            "session_id":session_id,
            "context":{"actor_id":"scroll-test-agent","action_id":action_id,"expected_run_id":caps["session"]["run_id"],"expected_viewport_revision":caps["viewport_revision"],"controller_generation":claimed["generation"]},
            "direction":direction,
            "region":{"row_start":0,"col_start":0,"row_end":24,"col_end":80},
            "target":{"query":{"kind":"text","value":target},"transition":"current_presence"},
            "verified_at":verified_at,
            "bindings":[{"kind":"mouse","input":{"kind":"wheel","row":2,"col":4,"direction":if direction == "down" {"down"} else {"up"},"steps":1}}],
            "max_steps":5,"max_no_progress":max_no_progress,"timeout_ms":timeout_ms
        })
    };

    // Without an existing owner, scroll must fail without acquiring control.
    let fresh = h
        .call("terminal_capabilities", json!({"session_id":session_id}))
        .await;
    assert_eq!(fresh["controller"]["actor_id"], "scroll-test-agent");
    let found = h
        .call(
            "terminal_scroll_until",
            args("scroll-found", "down", "older catch-up recap", 2, 3000),
        )
        .await;
    assert_eq!(found["outcome"], "found", "{found}");
    assert_eq!(found["space"], "application_viewport");
    assert_eq!(found["steps"], 2);
    assert_eq!(found["matches"]["any_match"], true);

    let no_progress = h
        .call(
            "terminal_scroll_until",
            args("scroll-no-progress", "up", "never rendered target", 1, 3000),
        )
        .await;
    assert_eq!(no_progress["outcome"], "no_progress", "{no_progress}");

    let mut stale_context = args("scroll-stale-actor", "down", "not present", 2, 1000);
    stale_context["context"]["controller_generation"] =
        json!(claimed["generation"].as_u64().unwrap() + 1);
    assert!(h.raw("terminal_scroll_until", stale_context).await.is_err());

    let timed = h
        .call(
            "terminal_scroll_until",
            args("scroll-deadline", "down", "not present", 5, 1),
        )
        .await;
    assert_eq!(timed["outcome"], "deadline_exceeded", "{timed}");
}

#[tokio::test]
async fn app_adapter_binds_owned_backend_and_tui_and_rejects_unscoped_or_stale_reads() {
    let h = Harness::new().await;
    let tools = h.client.peer().list_all_tools().await.unwrap();
    assert!(tools.iter().any(|tool| tool.name == "terminal_app_adapter"));
    let adapter_schema = &tools
        .iter()
        .find(|tool| tool.name == "terminal_app_adapter")
        .unwrap()
        .input_schema;
    let adapter_schema = serde_json::to_string(adapter_schema).unwrap();
    assert!(
        !adapter_schema.contains("limit"),
        "fixed bounded message limit must not be advertised as configurable"
    );
    let workspace = h.temp.path().join("workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let (backend, port) = launch_api(&h, &workspace).await;
    let tui = h
        .launch(
            &workspace,
            &[
                "attach".into(),
                format!("http://127.0.0.1:{port}"),
                "--session".into(),
                "fixture-session".into(),
            ],
        )
        .await;
    let bound = h
        .call(
            "terminal_app_adapter",
            app_bind(&backend, &tui, &["fixture-session"]),
        )
        .await;
    assert_eq!(bound["status"], "bound");
    let adapter_id = bound["adapter_id"].as_str().unwrap();

    let messages = h
        .call(
            "terminal_app_adapter",
            json!({"action":"messages","adapter_id":adapter_id,"application_session_id":"fixture-session"}),
        )
        .await;
    assert_eq!(messages["messages"].as_array().unwrap().len(), 0);
    let session = h
        .call(
            "terminal_app_adapter",
            json!({"action":"session","adapter_id":adapter_id,"application_session_id":"fixture-session"}),
        )
        .await;
    assert_eq!(session["id"], "fixture-session");
    let status = h
        .call(
            "terminal_app_adapter",
            json!({"action":"status","adapter_id":adapter_id,"application_session_id":"fixture-session"}),
        )
        .await;
    assert_eq!(status["status"]["type"], "idle");
    let events = h
        .call(
            "terminal_app_adapter",
            json!({"action":"events","adapter_id":adapter_id}),
        )
        .await;
    assert_eq!(events["events"].as_array().unwrap().len(), 1);
    assert_eq!(events["filtered_events"], 1);
    let unavailable = h
        .call(
            "terminal_app_adapter",
            json!({"action":"ui_state","adapter_id":adapter_id}),
        )
        .await;
    assert!(
        unavailable
            .to_string()
            .contains("no UI-state producer connected")
    );
    let expected_but_missing = h
        .call(
            "terminal_app_adapter",
            json!({"action":"ui_state","adapter_id":adapter_id,"producer_instance":"fixture-ui-v1"}),
        )
        .await;
    assert!(
        expected_but_missing
            .to_string()
            .contains("route unavailable")
    );
    let frame = h
        .call(
            "terminal_observe",
            json!({"session_id":tui["session_id"],"response":{"mode":"summary"}}),
        )
        .await;
    let ui_file = workspace.join("emulation-ui-state.json");
    let mut invalid_state = app_ui_state(&backend, &tui, &frame, 1);
    invalid_state["backend_run_id"] = json!("stale-backend-run");
    std::fs::write(&ui_file, serde_json::to_vec(&invalid_state).unwrap()).unwrap();
    assert!(h
        .raw(
            "terminal_app_adapter",
            json!({"action":"ui_state","adapter_id":adapter_id,"producer_instance":"fixture-ui-v1"}),
        )
        .await
        .is_err(), "invalid producer identity must not pin the adapter");
    let mut ui_state = app_ui_state(&backend, &tui, &frame, 1);
    std::fs::write(&ui_file, serde_json::to_vec(&ui_state).unwrap()).unwrap();
    let connected = h
        .call(
            "terminal_app_adapter",
            json!({"action":"ui_state","adapter_id":adapter_id,"producer_instance":"fixture-ui-v1"}),
        )
        .await;
    assert_eq!(connected["Available"]["route"], "home");
    assert_eq!(connected["Available"]["dialog"], "subagents");
    assert_eq!(connected["Available"]["filter"], "compile");
    assert_eq!(connected["Available"]["focus"], "filter");
    ui_state["sequence"] = json!(2);
    ui_state["route"] = json!("session");
    ui_state["filter"] = json!("verification");
    ui_state["focus"] = json!("transcript");
    std::fs::write(&ui_file, serde_json::to_vec(&ui_state).unwrap()).unwrap();
    let updated = h
        .call(
            "terminal_app_adapter",
            json!({"action":"ui_state","adapter_id":adapter_id}),
        )
        .await;
    assert_eq!(updated["Available"]["route"], "session");
    assert_eq!(updated["Available"]["filter"], "verification");
    assert_eq!(updated["Available"]["focus"], "transcript");
    assert!(h.raw("terminal_app_adapter", json!({"action":"ui_state","adapter_id":adapter_id,"producer_instance":"different-producer"})).await.is_err(), "a pinned adapter cannot switch producer identity");

    assert!(h.raw("terminal_app_adapter", json!({"action":"messages","adapter_id":adapter_id,"application_session_id":"other-session"})).await.is_err());
    assert!(h.raw("terminal_app_adapter", json!({"action":"messages","adapter_id":adapter_id,"application_session_id":"fixture-session","limit":1})).await.is_err(), "unsupported limit must not be silently accepted");

    let other_workspace = h.temp.path().join("other-workspace");
    std::fs::create_dir_all(&other_workspace).unwrap();
    let mismatch_tui = h
        .launch(
            &other_workspace,
            &[
                "attach".into(),
                format!("http://127.0.0.1:{port}"),
                "--session".into(),
                "fixture-session".into(),
            ],
        )
        .await;
    assert!(
        h.raw(
            "terminal_app_adapter",
            app_bind(&backend, &mismatch_tui, &["fixture-session"])
        )
        .await
        .is_err()
    );

    let (shared_data_backend, _) = launch_api(&h, &workspace).await;
    assert!(
        h.raw(
            "terminal_app_adapter",
            app_bind(&shared_data_backend, &tui, &["fixture-session"])
        )
        .await
        .is_err()
    );
    assert!(
        h.raw(
            "terminal_app_adapter",
            app_bind(
                &json!({"session_id":"not-owned"}),
                &tui,
                &["fixture-session"]
            )
        )
        .await
        .is_err()
    );

    // Restarting either side invalidates and removes the binding rather than
    // retaining a stale adapter entry that could target a later run.
    h.call("terminal_restart", json!({"session_id":tui["session_id"]}))
        .await;
    assert!(h.raw("terminal_app_adapter", json!({"action":"session","adapter_id":adapter_id,"application_session_id":"fixture-session"})).await.is_err());
    let rebound = h
        .call(
            "terminal_app_adapter",
            app_bind(&backend, &tui, &["fixture-session"]),
        )
        .await;
    let rebound_tui = json!({"session_id":tui["session_id"]});
    let stale_frame = h
        .call(
            "terminal_observe",
            json!({"session_id":tui["session_id"],"response":{"mode":"summary"}}),
        )
        .await;
    assert_ne!(stale_frame["run_id"], frame["run_id"]);
    assert!(h
        .raw(
            "terminal_app_adapter",
            json!({"action":"ui_state","adapter_id":rebound["adapter_id"],"producer_instance":"fixture-ui-v1"}),
        )
        .await
        .is_err(), "old TUI run state must be rejected after rebinding");
    let fresh_ui = app_ui_state(&backend, &rebound_tui, &stale_frame, 1);
    std::fs::write(&ui_file, serde_json::to_vec(&fresh_ui).unwrap()).unwrap();
    let fresh = h
        .call(
            "terminal_app_adapter",
            json!({"action":"ui_state","adapter_id":rebound["adapter_id"],"producer_instance":"fixture-ui-v1"}),
        )
        .await;
    assert_eq!(
        fresh["Available"]["observation"]["run_id"],
        stale_frame["run_id"]
    );
    h.call(
        "terminal_restart",
        json!({"session_id":backend["session_id"]}),
    )
    .await;
    assert!(h.raw("terminal_app_adapter", json!({"action":"session","adapter_id":rebound["adapter_id"],"application_session_id":"fixture-session"})).await.is_err(), "restarted backend run must invalidate adapter binding");
}

#[tokio::test]
async fn app_adapter_semantic_wait_requires_a_postbaseline_completion() {
    let h = Harness::new().await;
    let workspace = h.temp.path().join("semantic-workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let (backend, port) = launch_api(&h, &workspace).await;
    let tui = h
        .launch(
            &workspace,
            &[
                "attach".into(),
                format!("http://127.0.0.1:{port}"),
                "--session".into(),
                "fixture-session".into(),
            ],
        )
        .await;
    let bound = h
        .call(
            "terminal_app_adapter",
            app_bind(&backend, &tui, &["fixture-session"]),
        )
        .await;
    let adapter_id = bound["adapter_id"].as_str().unwrap();
    let messages_file = workspace.join("emulation-messages.json");
    std::fs::write(
        &messages_file,
        serde_json::to_vec(&json!([completed_tool_message(
            "fixture-session",
            "assistant-old",
            "user-old",
            "call-old"
        )]))
        .unwrap(),
    )
    .unwrap();

    let baseline = h
        .call(
            "terminal_app_adapter",
            json!({"action":"message_baseline","adapter_id":adapter_id,"application_session_id":"fixture-session"}),
        )
        .await;
    let baseline_handle = baseline["baseline_handle"].as_str().unwrap();
    let stale = h
        .call(
            "terminal_app_adapter",
            json!({
                "action":"semantic_wait",
                "adapter_id":adapter_id,
                "baseline_handle":baseline_handle,
                "timeout_ms":250,
                "predicate":{"kind":"tool_completed","user_message_id":"user-old","call_id":"call-old","tool":"write"}
            }),
        )
        .await;
    assert_eq!(
        stale["matched"], false,
        "pre-baseline completion cannot satisfy the wait: {stale}"
    );
    assert_eq!(stale["outcome"], "timed_out");

    std::fs::write(
        &messages_file,
        serde_json::to_vec(&json!([
            completed_tool_message("fixture-session", "assistant-old", "user-old", "call-old"),
            completed_tool_message("fixture-session", "assistant-new", "user-new", "call-new")
        ]))
        .unwrap(),
    )
    .unwrap();
    let fresh = h
        .call(
            "terminal_app_adapter",
            json!({
                "action":"semantic_wait",
                "adapter_id":adapter_id,
                "baseline_handle":baseline_handle,
                "timeout_ms":2000,
                "predicate":{"kind":"tool_completed","user_message_id":"user-new","call_id":"call-new","tool":"write"}
            }),
        )
        .await;
    assert_eq!(
        fresh["matched"], true,
        "post-baseline tool completion should satisfy the wait: {fresh}"
    );
    assert_eq!(fresh["outcome"], "matched");
}

#[tokio::test]
async fn app_adapter_registry_is_bounded_and_close_prunes_owned_bindings() {
    let h = Harness::new().await;
    let workspace = h.temp.path().join("bounded-workspace");
    std::fs::create_dir_all(&workspace).unwrap();
    let (backend, port) = launch_api(&h, &workspace).await;
    let tui = h
        .launch(
            &workspace,
            &[
                "attach".into(),
                format!("http://127.0.0.1:{port}"),
                "--session".into(),
                "fixture-session".into(),
            ],
        )
        .await;
    let mut first_adapter = String::new();
    for index in 0..64 {
        let bound = h
            .call(
                "terminal_app_adapter",
                app_bind(&backend, &tui, &["fixture-session"]),
            )
            .await;
        if index == 0 {
            first_adapter = bound["adapter_id"].as_str().unwrap().to_owned();
        }
    }
    assert!(
        h.raw(
            "terminal_app_adapter",
            app_bind(&backend, &tui, &["fixture-session"]),
        )
        .await
        .is_err(),
        "the adapter registry must reject its 65th live binding"
    );

    h.call("terminal_close", json!({"session_id":tui["session_id"]}))
        .await;
    assert!(h
        .raw(
            "terminal_app_adapter",
            json!({"action":"session","adapter_id":first_adapter,"application_session_id":"fixture-session"}),
        )
        .await
        .is_err(), "closing either owner must remove all associated adapter bindings");
}
