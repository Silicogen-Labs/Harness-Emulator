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
        let base = Path::new(env!("CARGO_MANIFEST_DIR")).join(".runtime/mcp-sequences");
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
        .expect("MCP call exceeded 5s; possible service deadlock")
    }

    async fn call(&self, name: &str, args: Value) -> Value {
        let result = self.raw(name, args).await.unwrap();
        assert_ne!(result.is_error, Some(true), "{result:?}");
        result.structured_content.unwrap()
    }

    async fn fixture(&self) -> Value {
        self.call(
            "terminal_launch",
            json!({
                "program": env!("CARGO_BIN_EXE_terminal-fixture"),
                "cwd": self.temp.path(), "cols": 80, "rows": 24, "env_inherit": []
            }),
        )
        .await
    }

    async fn delayed_palette_fixture(&self) -> Value {
        self.call(
            "terminal_launch",
            json!({
                "program": env!("CARGO_BIN_EXE_terminal-fixture"),
                "args": ["--delayed-palette"],
                "cwd": self.temp.path(), "cols": 80, "rows": 24, "env_inherit": []
            }),
        )
        .await
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let mut shutdown = match Command::new(env!("CARGO_BIN_EXE_silicogen-emulation"))
            .arg("--runtime-dir")
            .arg(&self.runtime)
            .arg("shutdown")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
        {
            Ok(child) => child,
            Err(error) => {
                eprintln!(
                    "sequence test cleanup could not spawn shutdown for {}: {error}",
                    self.runtime.display()
                );
                return;
            }
        };
        if wait_child(&mut shutdown, Duration::from_secs(2)) {
            return;
        }
        let _ = shutdown.kill();
        let _ = shutdown.wait();
        let Some(daemon_pid) = find_owned_daemon(&self.runtime) else {
            eprintln!(
                "sequence test cleanup timed out; no daemon with exact runtime {} remained",
                self.runtime.display()
            );
            return;
        };
        unsafe {
            libc::kill(daemon_pid as i32, libc::SIGTERM);
        }
        if wait_pid_gone(daemon_pid, Duration::from_millis(500)) {
            return;
        }
        unsafe {
            libc::kill(daemon_pid as i32, libc::SIGKILL);
        }
        if !wait_pid_gone(daemon_pid, Duration::from_millis(500)) {
            eprintln!(
                "sequence test cleanup failed to reap daemon pid {daemon_pid} for exact runtime {}",
                self.runtime.display()
            );
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

fn find_owned_daemon(runtime: &Path) -> Option<u32> {
    let runtime = std::fs::canonicalize(runtime).ok()?;
    for entry in std::fs::read_dir("/proc").ok()?.flatten() {
        let Ok(pid) = entry.file_name().to_string_lossy().parse::<u32>() else {
            continue;
        };
        let Ok(args) = std::fs::read(entry.path().join("cmdline")) else {
            continue;
        };
        let args = args
            .split(|byte| *byte == 0)
            .filter(|part| !part.is_empty())
            .collect::<Vec<_>>();
        let exact_runtime = args.windows(2).any(|pair| {
            pair[0] == b"--runtime-dir"
                && Path::new(std::str::from_utf8(pair[1]).ok().unwrap_or("")) == runtime
        });
        if exact_runtime && args.iter().any(|arg| *arg == b"daemon") {
            return Some(pid);
        }
    }
    None
}

fn wait_pid_gone(pid: u32, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if !Path::new(&format!("/proc/{pid}")).exists() {
            return true;
        }
        thread::sleep(Duration::from_millis(10));
    }
    !Path::new(&format!("/proc/{pid}")).exists()
}

#[tokio::test]
async fn mcp_sequence_admission_status_duplicate_cancel_and_resume_are_durable() {
    let harness = Harness::new().await;
    let tools = harness.client.peer().list_all_tools().await.unwrap();
    assert!(tools.iter().any(|tool| tool.name == "terminal_sequence"));
    let launched = harness.fixture().await;
    let session_id = launched["session_id"].as_str().unwrap();
    let actor_id = "sequence-mcp-agent";
    let actor = harness
        .call(
            "terminal_actor",
            json!({"session_id":session_id,"actor_id":actor_id}),
        )
        .await;
    let capabilities = harness
        .call("terminal_capabilities", json!({"session_id":session_id}))
        .await;
    let request = json!({
        "identity":{"actor_id":actor_id,"session_id":session_id,"run_id":capabilities["session"]["run_id"],"sequence_id":"mcp-sequence-01"},
        "authority":{"expected_viewport_revision":capabilities["viewport_revision"],"expected_cols":80,"expected_rows":24,"controller_generation":actor["generation"]},
        "timeout_ms":10000,
        "steps":[{"kind":"leader_keys","id":"leader","keys":["Ctrl+X","L"],"delay_ms":1000}]
    });
    let start_args = json!({"action":"start","actor_id":actor_id,"request":request});
    let started = harness.call("terminal_sequence", start_args.clone()).await;
    assert_eq!(started["sequence_id"], "mcp-sequence-01");
    assert_eq!(started["status"], "queued");

    // Admission is durably journaled before start returns, so an immediate
    // status request cannot race a missing journal file.
    let immediate_result = harness.raw("terminal_sequence", json!({"action":"status","sequence_id":"mcp-sequence-01","actor_id":actor_id,"response":{"mode":"summary","max_bytes":1024}})).await.unwrap();
    let immediate = immediate_result.structured_content.clone().unwrap();
    assert!(
        serde_json::to_vec(&immediate).unwrap().len() <= 1024,
        "{}",
        immediate
    );
    assert!(matches!(
        immediate["status"].as_str(),
        Some("pending" | "running")
    ));
    let cli_status = tokio::task::spawn_blocking({
        let runtime = harness.runtime.clone();
        move || Command::new(env!("CARGO_BIN_EXE_silicogen-emulation"))
            .arg("--runtime-dir").arg(runtime).args(["agent", "sequence", "--json"])
            .arg(r#"{"action":"status","sequence_id":"mcp-sequence-01","actor_id":"sequence-mcp-agent"}"#)
            .output().unwrap()
    }).await.unwrap();
    assert!(
        cli_status.status.success(),
        "{}",
        String::from_utf8_lossy(&cli_status.stderr)
    );
    let cli_status: Value = serde_json::from_slice(&cli_status.stdout).unwrap();
    assert!(matches!(
        cli_status["status"].as_str(),
        Some("pending" | "running")
    ));

    let duplicate = harness.call("terminal_sequence", start_args).await;
    assert_eq!(duplicate["sequence_id"], "mcp-sequence-01");

    let cancelled = harness
        .call(
            "terminal_sequence",
            json!({"action":"cancel","sequence_id":"mcp-sequence-01","actor_id":actor_id}),
        )
        .await;
    assert!(matches!(
        cancelled["status"].as_str(),
        Some("cancellation_requested" | "cancelled")
    ));
    let mut report = Value::Null;
    for _ in 0..100 {
        report = harness
            .call(
                "terminal_sequence",
                json!({"action":"status","sequence_id":"mcp-sequence-01","actor_id":actor_id}),
            )
            .await;
        if report["status"] == "cancelled" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(report["status"], "cancelled", "{report}");

    let resumed = harness
        .call(
            "terminal_sequence",
            json!({"action":"resume","sequence_id":"mcp-sequence-01","actor_id":actor_id}),
        )
        .await;
    assert_eq!(resumed["sequence_id"], "mcp-sequence-01");
    tokio::time::sleep(Duration::from_millis(50)).await;
    let final_report = harness
        .call(
            "terminal_sequence",
            json!({"action":"status","sequence_id":"mcp-sequence-01","actor_id":actor_id}),
        )
        .await;
    assert_eq!(final_report["status"], "cancelled");
}

#[tokio::test]
async fn mcp_sequence_rejects_owner_mismatch_before_admission() {
    let harness = Harness::new().await;
    let launched = harness.fixture().await;
    let session_id = launched["session_id"].as_str().unwrap();
    let capabilities = harness
        .call("terminal_capabilities", json!({"session_id":session_id}))
        .await;
    let request = json!({
        "identity":{"actor_id":"real-owner","session_id":session_id,"run_id":capabilities["session"]["run_id"],"sequence_id":"mcp-sequence-owner"},
        "authority":{"expected_viewport_revision":capabilities["viewport_revision"],"expected_cols":80,"expected_rows":24,"controller_generation":capabilities["controller"]["generation"]},
        "timeout_ms":1000,"steps":[{"kind":"input","id":"one","action":{"kind":"key","key":"Enter"}}]
    });
    let error = harness
        .raw(
            "terminal_sequence",
            json!({"action":"start","actor_id":"different-owner","request":request}),
        )
        .await
        .unwrap_err();
    let ServiceError::McpError(error) = error else {
        panic!("expected MCP invalid params: {error:?}")
    };
    assert_eq!(serde_json::to_value(error.code).unwrap(), -32602);
    assert_eq!(error.data.unwrap()["field"], "actor_id");
    assert!(
        !harness
            .runtime
            .join("sequences/sequence-journal.json")
            .exists()
    );
}

#[tokio::test]
async fn mcp_sequence_journal_limit_is_invalid_params_at_the_actual_boundary() {
    let harness = Harness::new().await;
    let launched = harness.fixture().await;
    let session_id = launched["session_id"].as_str().unwrap();
    let actor_id = "sequence-capacity-agent";
    let actor = harness
        .call(
            "terminal_actor",
            json!({"session_id":session_id,"actor_id":actor_id}),
        )
        .await;
    let caps = harness
        .call("terminal_capabilities", json!({"session_id":session_id}))
        .await;

    for index in 0..silicogen_terminal_runtime::sequences::MAX_SEQUENCE_JOBS {
        let sequence_id = format!("capacity-{index:02}");
        let request = json!({
            "identity":{"actor_id":actor_id,"session_id":session_id,"run_id":caps["session"]["run_id"],"sequence_id":sequence_id},
            "authority":{"expected_viewport_revision":caps["viewport_revision"],"expected_cols":80,"expected_rows":24,"controller_generation":actor["generation"]},
            "timeout_ms":5000,
            "steps":[{"kind":"input","id":"one","action":{"kind":"key","key":"Enter"}}]
        });
        harness
            .call(
                "terminal_sequence",
                json!({"action":"start","actor_id":actor_id,"request":request}),
            )
            .await;
        harness
            .call("terminal_sequence", json!({"action":"cancel","sequence_id":sequence_id,"actor_id":actor_id,"response":{"mode":"summary","max_bytes":4096}}))
            .await;
        for _ in 0..100 {
            let status = harness
                .call("terminal_sequence", json!({"action":"status","sequence_id":sequence_id,"actor_id":actor_id,"response":{"mode":"summary","max_bytes":4096}}))
                .await;
            if matches!(status["status"].as_str(), Some("cancelled" | "completed")) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    let overflow = json!({
        "identity":{"actor_id":actor_id,"session_id":session_id,"run_id":caps["session"]["run_id"],"sequence_id":"capacity-overflow"},
        "authority":{"expected_viewport_revision":caps["viewport_revision"],"expected_cols":80,"expected_rows":24,"controller_generation":actor["generation"]},
        "timeout_ms":1000,
        "steps":[{"kind":"input","id":"one","action":{"kind":"key","key":"Enter"}}]
    });
    let error = harness
        .raw(
            "terminal_sequence",
            json!({"action":"start","actor_id":actor_id,"request":overflow}),
        )
        .await
        .unwrap_err();
    let ServiceError::McpError(error) = error else {
        panic!("expected MCP invalid-params at sequence journal capacity: {error:?}")
    };
    assert_eq!(serde_json::to_value(error.code).unwrap(), -32602);
    let details = error.data.expect("typed validation details");
    assert_eq!(details["code"], "sequence_limit");
    assert_eq!(details["field"], "request");
    assert!(
        harness
            .runtime
            .join("sequences/sequence-journal.json")
            .exists()
    );
}

#[tokio::test]
async fn mcp_pending_leader_keys_include_planned_and_completed_action_ids() {
    let harness = Harness::new().await;
    let launched = harness.fixture().await;
    let session_id = launched["session_id"].as_str().unwrap();
    let actor_id = "sequence-pending-actions-agent";
    let actor = harness
        .call(
            "terminal_actor",
            json!({"session_id":session_id,"actor_id":actor_id}),
        )
        .await;
    let caps = harness
        .call("terminal_capabilities", json!({"session_id":session_id}))
        .await;
    let request = json!({
        "identity":{"actor_id":actor_id,"session_id":session_id,"run_id":caps["session"]["run_id"],"sequence_id":"pending-leader-keys"},
        "authority":{"expected_viewport_revision":caps["viewport_revision"],"expected_cols":80,"expected_rows":24,"controller_generation":actor["generation"]},
        "timeout_ms":10000,
        "steps":[{"kind":"leader_keys","id":"palette","keys":["Ctrl+P","s","Enter"],"delay_ms":1000}]
    });
    harness
        .call(
            "terminal_sequence",
            json!({"action":"start","actor_id":actor_id,"request":request}),
        )
        .await;

    let mut report = Value::Null;
    for _ in 0..100 {
        report = harness
            .call("terminal_sequence", json!({"action":"status","sequence_id":"pending-leader-keys","actor_id":actor_id,"response":{"mode":"full","max_bytes":8192}}))
            .await;
        if report["steps"][0]["completed_action_ids"]
            .as_array()
            .is_some_and(|ids| !ids.is_empty())
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    let step = &report["steps"][0];
    assert_eq!(step["action_ids"].as_array().unwrap().len(), 3, "{report}");
    assert_eq!(
        step["completed_action_ids"].as_array().unwrap().len(),
        1,
        "{report}"
    );
    assert_eq!(
        step["pending_action_ids"].as_array().unwrap().len(),
        2,
        "{report}"
    );
    assert_eq!(step["receipts"].as_array().unwrap().len(), 1, "{report}");

    let summary = harness
        .call("terminal_sequence", json!({"action":"status","sequence_id":"pending-leader-keys","actor_id":actor_id,"response":{"mode":"summary","max_bytes":2048}}))
        .await;
    assert_eq!(
        summary["steps"][0]["pending_action_ids"],
        step["pending_action_ids"]
    );
    assert_eq!(summary["steps"][0]["receipt_count"], 1);
    assert_eq!(
        summary["steps"][0]["receipts"][0]["action_id"],
        step["completed_action_ids"][0]
    );
    harness
        .call("terminal_sequence", json!({"action":"cancel","sequence_id":"pending-leader-keys","actor_id":actor_id,"response":{"mode":"summary","max_bytes":4096}}))
        .await;
}

#[tokio::test]
async fn mcp_sequence_waits_for_delayed_palette_and_query_without_fixed_sleep() {
    let harness = Harness::new().await;
    let launched = harness.delayed_palette_fixture().await;
    let session_id = launched["session_id"].as_str().unwrap();
    let actor_id = "delayed-palette-agent";
    let actor = harness
        .call(
            "terminal_actor",
            json!({"session_id":session_id,"actor_id":actor_id}),
        )
        .await;
    let caps = harness
        .call("terminal_capabilities", json!({"session_id":session_id}))
        .await;
    let fresh_text = |id: &str, value: &str| {
        json!({
            "kind":"barrier",
            "id":id,
            "baseline":{"kind":"previous_input"},
            "condition":{"kind":"fresh","predicate":{
                "query":{"kind":"text","value":value},
                "region":{"row_start":0,"col_start":0,"row_end":24,"col_end":80},
                "transition":"became_present"
            }}
        })
    };
    let request = json!({
        "identity":{"actor_id":actor_id,"session_id":session_id,"run_id":caps["session"]["run_id"],"sequence_id":"delayed-palette-navigation"},
        "authority":{"expected_viewport_revision":caps["viewport_revision"],"expected_cols":80,"expected_rows":24,"controller_generation":actor["generation"]},
        "timeout_ms":10000,
        "steps":[
            {"kind":"input","id":"open-palette","action":{"kind":"key","key":"Ctrl+P"}},
            fresh_text("palette-visible", "PALETTE SEARCH READY"),
            {"kind":"input","id":"type-filter","action":{"kind":"type","text":"sidebar"}},
            fresh_text("result-highlighted", "HIGHLIGHTED: SIDEBAR PANEL"),
            {"kind":"input","id":"select-result","action":{"kind":"key","key":"Enter"}},
            fresh_text("selection-confirmed", "CONFIRMED: SIDEBAR PANEL")
        ]
    });
    let started = harness
        .call(
            "terminal_sequence",
            json!({"action":"start","actor_id":actor_id,"request":request}),
        )
        .await;
    assert_eq!(started["status"], "queued");

    let mut report = Value::Null;
    for _ in 0..500 {
        report = harness
            .call("terminal_sequence", json!({"action":"status","sequence_id":"delayed-palette-navigation","actor_id":actor_id,"response":{"mode":"full","max_bytes":32768}}))
            .await;
        if matches!(
            report["status"].as_str(),
            Some("completed" | "partial_failure" | "timed_out")
        ) {
            break;
        }
        if report["status"] == "pending" {
            harness
                .call("terminal_sequence", json!({"action":"resume","sequence_id":"delayed-palette-navigation","actor_id":actor_id,"response":{"mode":"summary","max_bytes":4096}}))
                .await;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(
        report["status"], "completed",
        "delayed-render sequence failed: {report}"
    );
    let inputs = ["open-palette", "type-filter", "select-result"];
    let input_reports = inputs
        .iter()
        .map(|id| {
            report["steps"]
                .as_array()
                .unwrap()
                .iter()
                .find(|step| step["step_id"] == *id)
                .unwrap()
        })
        .collect::<Vec<_>>();
    for step in &input_reports {
        assert_eq!(
            step["completed_action_ids"].as_array().unwrap().len(),
            1,
            "{step}"
        );
        assert_eq!(step["receipts"].as_array().unwrap().len(), 1, "{step}");
        assert!(
            step["pending_action_ids"].as_array().unwrap().is_empty(),
            "{step}"
        );
    }
    let action_ids = input_reports
        .iter()
        .map(|step| step["action_ids"][0].as_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        action_ids
            .iter()
            .collect::<std::collections::HashSet<_>>()
            .len(),
        3
    );

    let frame = harness
        .call(
            "terminal_observe",
            json!({"session_id":session_id,"response":{"mode":"full","max_bytes":65536}}),
        )
        .await;
    let screen = frame["text"].as_str().unwrap_or_default();
    assert!(screen.contains("PALETTE SEARCH READY"), "{screen}");
    assert!(screen.contains("FILTER: sidebar"), "{screen}");
    assert!(screen.contains("HIGHLIGHTED: SIDEBAR PANEL"), "{screen}");
    assert!(screen.contains("CONFIRMED: SIDEBAR PANEL"), "{screen}");
    assert!(
        screen.contains("ACTIONS: CTRL_P=1 TYPE=1 ENTER=1"),
        "{screen}"
    );
    assert!(screen.contains("ORDER: CTRL_P,TYPE,ENTER"), "{screen}");
}

#[tokio::test]
async fn mcp_sequence_resume_rechecks_barrier_without_replaying_input_and_cancel_releases_slot() {
    let harness = Harness::new().await;
    let launched = harness.fixture().await;
    let session_id = launched["session_id"].as_str().unwrap();
    let actor_id = "sequence-resume-agent";
    let actor = harness
        .call(
            "terminal_actor",
            json!({"session_id":session_id,"actor_id":actor_id}),
        )
        .await;
    let caps = harness
        .call("terminal_capabilities", json!({"session_id":session_id}))
        .await;
    let request = json!({
        "identity":{"actor_id":actor_id,"session_id":session_id,"run_id":caps["session"]["run_id"],"sequence_id":"mcp-sequence-resume"},
        "authority":{"expected_viewport_revision":caps["viewport_revision"],"expected_cols":80,"expected_rows":24,"controller_generation":actor["generation"]},
        "timeout_ms":10000,
        "steps":[
            {"kind":"input","id":"type-once","action":{"kind":"type","text":"sent-once"}},
            {"kind":"barrier","id":"wait-for-absent-transition","baseline":{"kind":"previous_input"},"condition":{"kind":"fresh","predicate":{"query":{"kind":"text","value":"NEVER_APPEARS"},"transition":"became_present"}}}
        ]
    });
    harness
        .call(
            "terminal_sequence",
            json!({"action":"start","actor_id":actor_id,"request":request}),
        )
        .await;

    let mut pending = Value::Null;
    for _ in 0..100 {
        pending = harness
            .call(
                "terminal_sequence",
                json!({"action":"status","sequence_id":"mcp-sequence-resume","actor_id":actor_id}),
            )
            .await;
        if pending["status"] == "pending" && pending["steps"][0]["status"] == "written" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(pending["status"], "pending", "{pending}");
    assert_eq!(pending["steps"][0]["receipts"].as_array().unwrap().len(), 1);

    let resume = harness
        .call(
            "terminal_sequence",
            json!({"action":"resume","sequence_id":"mcp-sequence-resume","actor_id":actor_id}),
        )
        .await;
    assert_eq!(resume["status"], "queued");
    tokio::time::sleep(Duration::from_millis(350)).await;
    let after_resume = harness
        .call(
            "terminal_sequence",
            json!({"action":"status","sequence_id":"mcp-sequence-resume","actor_id":actor_id}),
        )
        .await;
    assert_eq!(after_resume["status"], "pending");
    assert_eq!(
        after_resume["steps"][0]["receipts"]
            .as_array()
            .unwrap()
            .len(),
        1,
        "resume replayed a confirmed input: {after_resume}"
    );

    let cancelled = harness
        .call(
            "terminal_sequence",
            json!({"action":"cancel","sequence_id":"mcp-sequence-resume","actor_id":actor_id}),
        )
        .await;
    assert_eq!(cancelled["status"], "cancelled");
    let final_report = harness
        .call(
            "terminal_sequence",
            json!({"action":"status","sequence_id":"mcp-sequence-resume","actor_id":actor_id}),
        )
        .await;
    assert_eq!(final_report["status"], "cancelled");
}

#[tokio::test]
async fn mcp_sequence_pending_barrier_resumes_after_observed_change_without_replaying() {
    let harness = Harness::new().await;
    let launched = harness.fixture().await;
    let session_id = launched["session_id"].as_str().unwrap();
    let actor_id = "sequence-barrier-agent";
    let actor = harness
        .call(
            "terminal_actor",
            json!({"session_id":session_id,"actor_id":actor_id}),
        )
        .await;
    let caps = harness
        .call("terminal_capabilities", json!({"session_id":session_id}))
        .await;
    let request = json!({
        "identity":{"actor_id":actor_id,"session_id":session_id,"run_id":caps["session"]["run_id"],"sequence_id":"mcp-sequence-barrier"},
        "authority":{"expected_viewport_revision":caps["viewport_revision"],"expected_cols":80,"expected_rows":24,"controller_generation":actor["generation"]},
        "timeout_ms":10000,
        "steps":[
            {"kind":"input","id":"seed-once","action":{"kind":"type","text":"seed"}},
            {"kind":"barrier","id":"wait-unlock","baseline":{"kind":"previous_input"},"condition":{"kind":"fresh","predicate":{"query":{"kind":"text","value":"UNLOCK"},"transition":"became_present"}}}
        ]
    });
    harness
        .call(
            "terminal_sequence",
            json!({"action":"start","actor_id":actor_id,"request":request}),
        )
        .await;
    let mut report = Value::Null;
    for _ in 0..100 {
        report = harness
            .call(
                "terminal_sequence",
                json!({"action":"status","sequence_id":"mcp-sequence-barrier","actor_id":actor_id,"response":{"mode":"full","max_bytes":4096}}),
            )
            .await;
        if report["status"] == "pending" && report["steps"][0]["status"] == "written" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        report["status"], "pending",
        "first fresh barrier should wait: {report}"
    );
    let first_action_ids = report["steps"][0]["action_ids"].clone();
    let first_receipts = report["steps"][0]["receipts"].clone();
    assert_eq!(first_action_ids.as_array().unwrap().len(), 1);
    assert_eq!(first_receipts.as_array().unwrap().len(), 1);
    assert_eq!(first_receipts[0]["written_to_pty"], true);

    harness
        .call(
            "terminal_sequence",
            json!({"action":"resume","sequence_id":"mcp-sequence-barrier","actor_id":actor_id}),
        )
        .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let still_pending = harness
        .call(
            "terminal_sequence",
            json!({"action":"status","sequence_id":"mcp-sequence-barrier","actor_id":actor_id,"response":{"mode":"full","max_bytes":4096}}),
        )
        .await;
    assert_eq!(still_pending["status"], "pending");
    assert_eq!(still_pending["steps"][0]["action_ids"], first_action_ids);
    assert_eq!(still_pending["steps"][0]["receipts"], first_receipts);

    let action_context = json!({
        "actor_id":actor_id,"action_id":"parent-unlock","expected_run_id":caps["session"]["run_id"],
        "expected_viewport_revision":caps["viewport_revision"],"controller_generation":actor["generation"]
    });
    harness
        .call(
            "terminal_type",
            json!({"session_id":session_id,"text":"UNLOCK","context":action_context}),
        )
        .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    harness
        .call(
            "terminal_sequence",
            json!({"action":"resume","sequence_id":"mcp-sequence-barrier","actor_id":actor_id}),
        )
        .await;
    let mut complete = Value::Null;
    for _ in 0..100 {
        complete = harness
            .call(
                "terminal_sequence",
                json!({"action":"status","sequence_id":"mcp-sequence-barrier","actor_id":actor_id,"response":{"mode":"full","max_bytes":4096}}),
            )
            .await;
        if complete["status"] == "completed" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(complete["status"], "completed", "{complete}");
    assert_eq!(complete["steps"][0]["action_ids"], first_action_ids);
    assert_eq!(complete["steps"][0]["receipts"], first_receipts);
    assert_eq!(complete["steps"][1]["status"], "matched");
    assert_eq!(
        complete["completed_step_ids"],
        json!(["seed-once", "wait-unlock"])
    );
}

#[tokio::test]
async fn mcp_sequence_resumes_previous_input_barrier_to_completion_after_new_text() {
    let harness = Harness::new().await;
    let launched = harness.fixture().await;
    let session_id = launched["session_id"].as_str().unwrap();
    let actor_id = "sequence-match-agent";
    let actor = harness
        .call(
            "terminal_actor",
            json!({"session_id":session_id,"actor_id":actor_id}),
        )
        .await;
    let caps = harness
        .call("terminal_capabilities", json!({"session_id":session_id}))
        .await;
    let request = json!({
        "identity":{"actor_id":actor_id,"session_id":session_id,"run_id":caps["session"]["run_id"],"sequence_id":"mcp-sequence-match"},
        "authority":{"expected_viewport_revision":caps["viewport_revision"],"expected_cols":80,"expected_rows":24,"controller_generation":actor["generation"]},
        "timeout_ms":10000,
        "steps":[
            {"kind":"input","id":"seed-once","action":{"kind":"type","text":"seed"}},
            {"kind":"barrier","id":"wait-unlock","baseline":{"kind":"previous_input"},"condition":{"kind":"fresh","predicate":{"query":{"kind":"text","value":"UNLOCK"},"transition":"became_present"}}}
        ]
    });
    harness
        .call(
            "terminal_sequence",
            json!({"action":"start","actor_id":actor_id,"request":request}),
        )
        .await;
    let mut pending = Value::Null;
    for _ in 0..100 {
        pending = harness
            .call(
                "terminal_sequence",
                json!({"action":"status","sequence_id":"mcp-sequence-match","actor_id":actor_id,"response":{"mode":"full","max_bytes":4096}}),
            )
            .await;
        if pending["status"] == "pending" && pending["steps"][0]["status"] == "written" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(pending["status"], "pending", "{pending}");
    let original_action_ids = pending["steps"][0]["action_ids"].clone();
    let original_receipts = pending["steps"][0]["receipts"].clone();
    assert_eq!(original_action_ids.as_array().unwrap().len(), 1);
    assert_eq!(original_receipts.as_array().unwrap().len(), 1);
    harness
        .call(
            "terminal_sequence",
            json!({"action":"resume","sequence_id":"mcp-sequence-match","actor_id":actor_id}),
        )
        .await;

    let unlock_context = json!({
        "actor_id":actor_id,"action_id":"external-unlock","expected_run_id":caps["session"]["run_id"],
        "expected_viewport_revision":caps["viewport_revision"],"controller_generation":actor["generation"]
    });
    harness
        .call(
            "terminal_type",
            json!({"session_id":session_id,"text":"UNLOCK","context":unlock_context}),
        )
        .await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    harness
        .call(
            "terminal_sequence",
            json!({"action":"resume","sequence_id":"mcp-sequence-match","actor_id":actor_id}),
        )
        .await;

    let mut completed = Value::Null;
    for _ in 0..100 {
        completed = harness
            .call(
                "terminal_sequence",
                json!({"action":"status","sequence_id":"mcp-sequence-match","actor_id":actor_id,"response":{"mode":"full","max_bytes":4096}}),
            )
            .await;
        if completed["status"] == "completed" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(completed["status"], "completed", "{completed}");
    assert_eq!(completed["steps"][0]["action_ids"], original_action_ids);
    assert_eq!(completed["steps"][0]["receipts"], original_receipts);
    assert_eq!(completed["steps"][1]["status"], "matched");
    let history = harness
        .call("terminal_history", json!({"session_id":session_id}))
        .await;
    let writes = history["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|event| event["kind"] == "input" && event["detail"] == "type")
        .count();
    assert_eq!(
        writes, 2,
        "one sequence seed plus one parent unlock; history={history}"
    );
}

#[tokio::test]
async fn scenario_rejects_malformed_later_sequence_before_dispatching_earlier_input() {
    let harness = Harness::new().await;
    let launched = harness.fixture().await;
    let session_id = launched["session_id"].as_str().unwrap();
    let before = harness
        .call("terminal_history", json!({"session_id":session_id}))
        .await;
    let sequence = json!({
        "action":"start","actor_id":"scenario-placeholder",
        "request":{
            "identity":{"actor_id":"scenario-placeholder","session_id":session_id,"run_id":"run-placeholder","sequence_id":"scenario-invalid-sequence"},
            "authority":{"expected_viewport_revision":0,"expected_cols":80,"expected_rows":24,"controller_generation":0},
            "timeout_ms":1000,
            "steps":[{"kind":"input","id":"bad-later-step","action":{"kind":"not-a-terminal-action"}}]
        }
    });
    let error = harness.raw("terminal_scenario_run", json!({"scenario":{
        "version":1,"name":"invalid sequence atomic validation","steps":[
            {"id":"would-type","operation":"type","params":{"session_id":session_id,"text":"MUST_NOT_BE_SENT"}},
            {"id":"invalid-sequence","operation":"sequence","params":sequence}
        ]
    }})).await.unwrap_err();
    let ServiceError::McpError(error) = error else {
        panic!("expected MCP validation error: {error:?}")
    };
    assert_eq!(serde_json::to_value(error.code).unwrap(), -32602);
    let after = harness
        .call("terminal_history", json!({"session_id":session_id}))
        .await;
    let before_inputs = before["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|event| event["kind"] == "input")
        .count();
    let after_inputs = after["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|event| event["kind"] == "input")
        .count();
    assert_eq!(
        after_inputs, before_inputs,
        "scenario dispatched an earlier step before validating its malformed sequence"
    );
}

#[tokio::test]
async fn scenario_cancel_cancels_its_owned_pending_sequence() {
    let harness = Harness::new().await;
    let launched = harness.fixture().await;
    let session_id = launched["session_id"].as_str().unwrap();
    let caps = harness
        .call("terminal_capabilities", json!({"session_id":session_id}))
        .await;
    let request = json!({
        "identity":{"actor_id":"scenario-supplied-owner","session_id":session_id,"run_id":caps["session"]["run_id"],"sequence_id":"scenario-sequence-cancel"},
        "authority":{"expected_viewport_revision":caps["viewport_revision"],"expected_cols":80,"expected_rows":24,"controller_generation":caps["controller"]["generation"]},
        "timeout_ms":60000,
        "steps":[
            {"kind":"input","id":"seed","action":{"kind":"type","text":"seed"}},
            {"kind":"barrier","id":"wait-never","baseline":{"kind":"previous_input"},"condition":{"kind":"fresh","predicate":{"query":{"kind":"text","value":"NEVER"},"transition":"became_present"}}}
        ]
    });
    let scenario = harness.call("terminal_scenario_run", json!({"scenario":{
        "version":1,"name":"cancel sequence from scenario","timeout_ms":30000,"steps":[
            {"id":"sequence","operation":"sequence","timeout_ms":20000,"params":{"action":"start","actor_id":"scenario-supplied-owner","request":request}}
        ]
    }})).await;
    let job_id = scenario["job_id"].as_str().unwrap();
    let journal_path = harness.runtime.join("sequences/sequence-journal.json");
    for _ in 0..100 {
        if !journal_path.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
            continue;
        }
        let journal: Value =
            serde_json::from_slice(&std::fs::read(&journal_path).unwrap()).unwrap();
        if journal["records"]["scenario-sequence-cancel"]["steps"][0]["status"] == "written" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    harness
        .call("terminal_scenario_cancel", json!({"job_id":job_id}))
        .await;
    let mut status = Value::Null;
    for _ in 0..100 {
        status = harness
            .call("terminal_scenario_status", json!({"job_id":job_id}))
            .await;
        if status["status"] == "cancelled" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(status["status"], "cancelled", "{status}");
    let journal: Value = serde_json::from_slice(&std::fs::read(journal_path).unwrap()).unwrap();
    assert_eq!(
        journal["records"]["scenario-sequence-cancel"]["status"],
        "cancelled"
    );
}

#[tokio::test]
async fn cancelling_a_pending_sequence_releases_its_retained_baseline_and_lease() {
    let harness = Harness::new().await;
    let launched = harness.fixture().await;
    let session_id = launched["session_id"].as_str().unwrap();
    let actor_id = "sequence-quota-agent";
    let actor = harness
        .call(
            "terminal_actor",
            json!({"session_id":session_id,"actor_id":actor_id}),
        )
        .await;
    let caps = harness
        .call("terminal_capabilities", json!({"session_id":session_id}))
        .await;
    let sequence_id = "baseline-lease-release";
    let request = json!({
        "identity":{"actor_id":actor_id,"session_id":session_id,"run_id":caps["session"]["run_id"],"sequence_id":sequence_id},
        "authority":{"expected_viewport_revision":caps["viewport_revision"],"expected_cols":80,"expected_rows":24,"controller_generation":actor["generation"]},
        "timeout_ms":30000,
        "steps":[
            {"kind":"input","id":"seed","action":{"kind":"type","text":"s"}},
            {"kind":"barrier","id":"hold","baseline":{"kind":"previous_input"},"condition":{"kind":"fresh","predicate":{"query":{"kind":"text","value":"NEVER_QUOTA"},"transition":"became_present"}}}
        ]
    });
    harness
        .call(
            "terminal_sequence",
            json!({"action":"start","actor_id":actor_id,"request":request}),
        )
        .await;
    let mut pending = Value::Null;
    for _ in 0..100 {
        pending = harness
            .call(
                "terminal_sequence",
                json!({"action":"status","sequence_id":sequence_id,"actor_id":actor_id}),
            )
            .await;
        if pending["status"] == "pending" && pending["steps"][0]["status"] == "written" {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert_eq!(
        pending["status"], "pending",
        "first sequence baseline was not retained: {pending}"
    );
    assert_eq!(pending["baseline_retention"]["retained"], true);
    let retained_bytes = pending["baseline_retention"]["bytes"].as_u64().unwrap();
    assert!(
        retained_bytes > 0 && retained_bytes < 8 * 1024 * 1024,
        "retention must report actual bytes, not the admission reservation: {pending}"
    );

    // The first worker has returned Pending, so cancel takes the inactive-job
    // path and must release its retained baseline and evidence lease itself.
    let cancelled = harness
        .call(
            "terminal_sequence",
            json!({"action":"cancel","sequence_id":sequence_id,"actor_id":actor_id,"response":{"mode":"full","max_bytes":4096}}),
        )
        .await;
    assert_eq!(cancelled["status"], "cancelled");
    assert_eq!(cancelled["baseline_retention"]["retained"], false);
    assert_eq!(cancelled["baseline_retention"]["bytes"], 0);
}
