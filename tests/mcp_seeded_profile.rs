use rmcp::{ServiceExt, model::CallToolRequestParams, transport::TokioChildProcess};
use serde_json::{Value, json};
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
};

fn temp() -> tempfile::TempDir {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(".runtime/issue-1-mcp-tests");
    fs::create_dir_all(&root).unwrap();
    tempfile::tempdir_in(root).unwrap()
}

struct StopDaemon(PathBuf);
impl Drop for StopDaemon {
    fn drop(&mut self) {
        let _ = Command::new(env!("CARGO_BIN_EXE_silicogen-emulation"))
            .arg("--runtime-dir")
            .arg(&self.0)
            .arg("shutdown")
            .output();
    }
}

#[tokio::test]
async fn mcp_seeded_sandbox_viewer_finalize_and_separate_cli_share_the_contract() {
    let tmp = temp();
    let runtime = tmp.path().join("runtime");
    let source = tmp.path().join("project");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("rtl.v"), "original").unwrap();
    let auth = tmp.path().join("auth.json");
    fs::write(&auth, r#"{"token":"synthetic-login-only"}"#).unwrap();
    let _cleanup = StopDaemon(runtime.clone());
    let binary = env!("CARGO_BIN_EXE_silicogen-emulation");
    let mut command = tokio::process::Command::new(binary);
    command
        .arg("--runtime-dir")
        .arg(&runtime)
        .arg("mcp")
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("LANG", "C.UTF-8")
        .env("DAEMON_UNRELATED_SECRET", "synthetic-must-not-inherit")
        .env("SSH_AUTH_SOCK", "synthetic-agent-socket");
    let client = ().serve(TokioChildProcess::new(command).unwrap()).await.unwrap();
    macro_rules! call {
        ($name:expr, $args:expr) => {{
            let result = client
                .peer()
                .call_tool(
                    CallToolRequestParams::new($name)
                        .with_arguments($args.as_object().unwrap().clone()),
                )
                .await
                .unwrap();
            assert_ne!(result.is_error, Some(true), "{result:?}");
            result.structured_content.unwrap()
        }};
    }
    let tools = client.peer().list_all_tools().await.unwrap();
    let finalize = tools
        .iter()
        .find(|tool| tool.name == "terminal_finalize")
        .unwrap();
    let schema = serde_json::to_string(&finalize.input_schema).unwrap();
    assert!(schema.contains("expected_run_id") && schema.contains("interrupted"));
    let mut profile = call!("terminal_profiles", json!({"action":"template"}));
    profile["seeds"] = json!([{"source":auth,"destination":"data/silicogen/auth.json","include_credentials":true}]);
    call!(
        "terminal_profiles",
        json!({"action":"store","profile":profile})
    );
    let launch = call!(
        "terminal_launch",
        json!({
            "program":"/bin/sh", "cwd":source, "profile":"silicogen-test",
            "workspace":{"source":source,"mode":"reflink"},
            "args":["-c","test -z \"${DAEMON_UNRELATED_SECRET+x}${SSH_AUTH_SOCK+x}\" || exit 9; test -f \"$XDG_DATA_HOME/silicogen/auth.json\" || exit 10; printf edited > rtl.v; printf MCP_SAFE_READY; sleep 60"]
        })
    );
    assert_eq!(
        launch["capabilities"]["launch"]["isolation"]["effective_backend"],
        "bubblewrap"
    );
    let names = launch["capabilities"]["launch"]["environment_names"]
        .as_array()
        .unwrap();
    assert!(
        !names.contains(&json!("DAEMON_UNRELATED_SECRET"))
            && !names.contains(&json!("SSH_AUTH_SOCK"))
    );
    assert!(!launch.to_string().contains("synthetic-must-not-inherit"));
    let id = &launch["session_id"];
    let wait = call!(
        "terminal_wait",
        json!({"session_id":id,"kind":"contains","value":"MCP_SAFE_READY","timeout_ms":5000})
    );
    assert_eq!(wait["matched"], true);
    let view = call!("terminal_view_start", json!({"session_id":id}));
    let args = json!({"session_id":id,"expected_run_id":launch["run_id"],"outcome":"passed"});
    let pending = call!("terminal_finalize", args.clone());
    assert_eq!(pending["disposal"], "pending");
    call!("terminal_view_stop", json!({"view_id":view["view_id"]}));
    let done = call!("terminal_finalize", args.clone());
    assert_eq!(done["disposal"], "disposed");
    assert_eq!(call!("terminal_finalize", args), done);
    let read = call!(
        "terminal_read_artifact",
        json!({"session_id":id,"artifact_id":done["evidence"]["artifact_id"]})
    );
    assert!(read["text"].as_str().unwrap().contains("MCP_SAFE_READY"));
    call!(
        "terminal_read_artifact",
        json!({"session_id":id,"artifact_id":done["evidence"]["screenshot"]["artifact_id"]})
    );
    let status = Command::new(binary)
        .arg("--runtime-dir")
        .arg(&runtime)
        .arg("status")
        .arg(id.as_str().unwrap())
        .output()
        .unwrap();
    assert!(status.status.success());
    let status: Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(status["finalization"]["disposal"], "disposed");
    assert_eq!(
        fs::read_to_string(source.join("rtl.v")).unwrap(),
        "original"
    );
    assert_eq!(
        fs::read_to_string(auth).unwrap(),
        r#"{"token":"synthetic-login-only"}"#
    );
    assert!(!Path::new(launch["workspace"].as_str().unwrap()).exists());
    drop(client);
}

#[tokio::test]
async fn missing_required_backend_is_an_mcp_error_never_a_host_fallback() {
    let tmp = temp();
    let runtime = tmp.path().join("runtime");
    let source = tmp.path().join("project");
    fs::create_dir(&source).unwrap();
    let _cleanup = StopDaemon(runtime.clone());
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_silicogen-emulation"));
    command
        .arg("--runtime-dir")
        .arg(&runtime)
        .arg("mcp")
        .env_clear()
        .env("PATH", "");
    let client = ().serve(TokioChildProcess::new(command).unwrap()).await.unwrap();
    let profile = silicogen_terminal_runtime::profiles::silicogen_test_profile_template();
    let result = client
        .peer()
        .call_tool(
            CallToolRequestParams::new("terminal_profiles").with_arguments(
                json!({"action":"store","profile":profile})
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .unwrap();
    assert_ne!(result.is_error, Some(true));
    let result = client
        .peer()
        .call_tool(
            CallToolRequestParams::new("terminal_launch").with_arguments(
                json!({
                    "program":"/bin/sh","args":["-c","printf UNSAFE"],"cwd":source,
                    "profile":"silicogen-test","workspace":{"source":source}
                })
                .as_object()
                .unwrap()
                .clone(),
            ),
        )
        .await;
    // Existing MCP transport reports service failures as JSON-RPC errors.
    let error = result.unwrap_err().to_string();
    assert!(error.contains("unavailable"), "{error}");
    let list = client
        .peer()
        .call_tool(CallToolRequestParams::new("terminal_list"))
        .await
        .unwrap();
    assert!(
        !serde_json::to_string(&list)
            .unwrap()
            .contains("\"running\"")
    );
    drop(client);
}
