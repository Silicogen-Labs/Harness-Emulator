use rmcp::{
    RoleClient, ServiceExt,
    model::{CallToolRequestParams, CallToolResult},
    service::ServiceError,
    transport::TokioChildProcess,
};
use serde_json::{Value, json};
use std::{
    fs,
    io::{BufRead, BufReader, Read, Write},
    net::{TcpListener, TcpStream},
    path::{Path, PathBuf},
    process::{Command, Stdio},
    time::{Duration, Instant},
};

struct Harness {
    client: rmcp::service::RunningService<RoleClient, ()>,
    runtime: PathBuf,
    temp: tempfile::TempDir,
}

impl Harness {
    async fn new() -> Self {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(".runtime/mcp-managed-diagnostics");
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
        .expect("MCP call exceeded test timeout")
    }

    async fn call(&self, name: &str, args: Value) -> Value {
        let result = self.raw(name, args).await.unwrap();
        assert_ne!(result.is_error, Some(true), "{name}: {result:?}");
        result.structured_content.unwrap()
    }

    async fn launch(&self, cwd: &Path, env: Value, workspace: Option<Value>) -> Value {
        let mut args = json!({
            "program":"/bin/sleep",
            "args":["60"],
            "cwd":cwd,
            "env":env,
            "env_inherit":[],
            "cols":80,
            "rows":24
        });
        if let Some(workspace) = workspace {
            args["workspace"] = workspace;
            args["cwd"] = json!("${workspace}");
        }
        self.call("terminal_launch", args).await
    }

    async fn diagnostic(&self, args: Value) -> Value {
        self.call("terminal_diagnostics", args).await
    }

    async fn close(&self, session_id: &str) {
        self.call("terminal_close", json!({"session_id":session_id}))
            .await;
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

fn owner(launch: &Value) -> Value {
    json!({"session_id":launch["session_id"],"run_id":launch["run_id"]})
}

fn external_synthetic_server_helper() {
    if std::env::var_os("MCP_DIAGNOSTICS_EXTERNAL_SERVER").as_deref()
        != Some(std::ffi::OsStr::new("1"))
    {
        return;
    }
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    println!("{}", listener.local_addr().unwrap());
    std::io::stdout().flush().unwrap();
    for mut stream in listener.incoming().flatten() {
        let mut request = [0; 1024];
        let _ = stream.read(&mut request);
        let _ = stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok");
    }
}

#[test]
fn external_synthetic_server_helper_test() {
    external_synthetic_server_helper();
}

#[tokio::test]
async fn mcp_exposes_typed_owned_diagnostics_and_redacts_known_session_secrets() {
    let h = Harness::new().await;
    let tools = h.client.peer().list_all_tools().await.unwrap();
    let schema = serde_json::to_value(
        &tools
            .iter()
            .find(|tool| tool.name == "terminal_diagnostics")
            .unwrap()
            .input_schema,
    )
    .unwrap();
    let schema_text = schema.to_string();
    for field in [
        "session_id",
        "run_id",
        "process_id",
        "program",
        "args",
        "cursor",
        "max_bytes",
        "relative_path",
        "expected_sha256",
    ] {
        assert!(
            schema_text.contains(field),
            "schema omits {field}: {schema_text}"
        );
    }
    assert!(
        !schema_text.contains("\"cwd\""),
        "diagnostics must not accept cwd"
    );
    assert!(
        !schema_text.contains("\"env\""),
        "diagnostics must not accept environment"
    );

    let cwd = h.temp.path().join("session-cwd");
    fs::create_dir(&cwd).unwrap();
    let secret = "SYNTHETIC_DIAGNOSTIC_SECRET_98a7";
    let launch = h.launch(&cwd, json!({"DIAG_SECRET":secret}), None).await;
    let mut start = owner(&launch);
    start["action"] = json!("start");
    start["program"] = json!("/usr/bin/printenv");
    start["args"] = json!(["DIAG_SECRET"]);
    let started = h.diagnostic(start).await;
    assert_eq!(started["backend"], "host");
    assert_eq!(started["detached_children_contained"], false);
    assert!(started["limitation"].as_str().unwrap().contains("detaches"));

    let mut read = owner(&launch);
    read["action"] = json!("read");
    read["process_id"] = started["process_id"].clone();
    read["stream"] = json!("stdout");
    read["cursor"] = json!(0);
    read["max_bytes"] = json!(128);
    let read_deadline = Instant::now() + Duration::from_secs(2);
    let output = loop {
        let output = h.diagnostic(read.clone()).await;
        if output["total_bytes_seen"].as_u64().unwrap_or(0) > 0 {
            break output;
        }
        assert!(
            Instant::now() < read_deadline,
            "diagnostic output did not arrive"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    };
    assert_eq!(output["text"], "<redacted>\n");
    assert!(!output.to_string().contains(secret));

    let mut printf = owner(&launch);
    printf["action"] = json!("start");
    printf["program"] = json!("/usr/bin/printf");
    printf["args"] = json!(["fixture-output"]);
    let printf_started = h.diagnostic(printf).await;
    let mut printf_read = owner(&launch);
    printf_read["action"] = json!("read");
    printf_read["process_id"] = printf_started["process_id"].clone();
    printf_read["stream"] = json!("stdout");
    printf_read["max_bytes"] = json!(32);
    let printf_deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let output = h.diagnostic(printf_read.clone()).await;
        if output["total_bytes_seen"].as_u64().unwrap_or(0) > 0 {
            assert_eq!(output["text"], "fixture-output");
            assert_eq!(output["next_cursor"], 14);
            break;
        }
        assert!(Instant::now() < printf_deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let mut status = owner(&launch);
    status["action"] = json!("status");
    status["process_id"] = started["process_id"].clone();
    let deadline = Instant::now() + Duration::from_secs(2);
    loop {
        let result = h.diagnostic(status.clone()).await;
        if result["lifecycle"] != "running" {
            assert_eq!(result["lifecycle"], "exited");
            break;
        }
        assert!(Instant::now() < deadline);
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let mut invalid = read;
    invalid["cwd"] = json!("/tmp");
    let ServiceError::McpError(error) = h.raw("terminal_diagnostics", invalid).await.unwrap_err()
    else {
        panic!("expected structured invalid-params error")
    };
    assert_eq!(serde_json::to_value(error.code).unwrap(), -32602);
    assert!(
        error.data.unwrap()["field"]
            .as_str()
            .unwrap()
            .contains("cwd")
    );
    let mut invalid = owner(&launch);
    invalid["action"] = json!("start");
    invalid["program"] = json!("/usr/bin/true");
    invalid["env"] = json!({"UNTRUSTED":"value"});
    let ServiceError::McpError(error) = h.raw("terminal_diagnostics", invalid).await.unwrap_err()
    else {
        panic!("expected structured invalid-params error")
    };
    assert_eq!(serde_json::to_value(error.code).unwrap(), -32602);
    assert!(
        error.data.unwrap()["field"]
            .as_str()
            .unwrap()
            .contains("env")
    );
    h.close(launch["session_id"].as_str().unwrap()).await;
}

#[tokio::test]
async fn mcp_diagnostics_enforces_exact_run_owner_and_bounds_cancel_and_stop() {
    let h = Harness::new().await;
    let cwd = h.temp.path().join("owner-cwd");
    fs::create_dir(&cwd).unwrap();
    let first = h.launch(&cwd, json!({}), None).await;
    let second = h.launch(&cwd, json!({}), None).await;
    let mut start = owner(&first);
    start["action"] = json!("start");
    start["program"] = json!("/bin/sleep");
    start["args"] = json!(["60"]);
    let started = h.diagnostic(start).await;

    let mut wrong_run = owner(&first);
    wrong_run["run_id"] = json!("00000000000000000000000000");
    wrong_run["action"] = json!("status");
    wrong_run["process_id"] = started["process_id"].clone();
    assert!(h.raw("terminal_diagnostics", wrong_run).await.is_err());

    let mut wrong_owner = owner(&second);
    wrong_owner["action"] = json!("status");
    wrong_owner["process_id"] = started["process_id"].clone();
    assert!(h.raw("terminal_diagnostics", wrong_owner).await.is_err());

    let mut invalid_read = owner(&first);
    invalid_read["action"] = json!("read");
    invalid_read["process_id"] = started["process_id"].clone();
    invalid_read["stream"] = json!("stdout");
    invalid_read["max_bytes"] = json!(65537);
    let ServiceError::McpError(error) = h
        .raw("terminal_diagnostics", invalid_read)
        .await
        .unwrap_err()
    else {
        panic!("expected read bound error")
    };
    assert_eq!(error.data.unwrap()["maximum"], 65536);

    let mut cancel = owner(&first);
    cancel["action"] = json!("cancel");
    cancel["process_id"] = started["process_id"].clone();
    assert_eq!(h.diagnostic(cancel).await["lifecycle"], "cancelled");
    let mut stop = owner(&first);
    stop["action"] = json!("stop");
    stop["process_id"] = started["process_id"].clone();
    h.diagnostic(stop).await;
    let mut after_stop = owner(&first);
    after_stop["action"] = json!("status");
    after_stop["process_id"] = started["process_id"].clone();
    assert!(h.raw("terminal_diagnostics", after_stop).await.is_err());
    h.close(first["session_id"].as_str().unwrap()).await;
    h.close(second["session_id"].as_str().unwrap()).await;
}

#[tokio::test]
async fn close_and_restart_reap_only_owned_diagnostics_and_workspace_seed_never_touches_origin() {
    let h = Harness::new().await;
    let source = h.temp.path().join("project-source");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("sentinel.txt"), "origin").unwrap();
    let launch = h
        .launch(
            h.temp.path(),
            json!({}),
            Some(json!({"source":source,"mode":"copy","exclude":[".git"]})),
        )
        .await;
    let workspace = launch["workspace"]
        .as_str()
        .expect("launch reports selected workspace");
    let mut seed = owner(&launch);
    seed["action"] = json!("seed_file");
    seed["relative_path"] = json!("fixtures/generated.txt");
    seed["content"] = json!("synthetic fixture");
    let seeded = h.diagnostic(seed).await;
    assert_eq!(seeded["workspace_only"], true);
    assert_eq!(
        fs::read_to_string(Path::new(workspace).join("fixtures/generated.txt")).unwrap(),
        "synthetic fixture"
    );
    assert_eq!(
        fs::read_to_string(source.join("sentinel.txt")).unwrap(),
        "origin"
    );

    let marker = h.temp.path().join("must-not-be-written-on-restart");
    let script = format!("/bin/sleep 1; /usr/bin/touch {}", marker.display());
    let mut start = owner(&launch);
    start["action"] = json!("start");
    start["program"] = json!("/bin/sh");
    start["args"] = json!(["-c", script]);
    let sidecar = h.diagnostic(start).await;
    let restarted = h
        .call(
            "terminal_restart",
            json!({"session_id":launch["session_id"]}),
        )
        .await;
    assert_ne!(restarted["run_id"], launch["run_id"]);
    tokio::time::sleep(Duration::from_millis(1150)).await;
    assert!(
        !marker.exists(),
        "restart must terminate manager-owned sidecar group"
    );
    let mut stale = owner(&launch);
    stale["action"] = json!("status");
    stale["process_id"] = sidecar["process_id"].clone();
    assert!(h.raw("terminal_diagnostics", stale).await.is_err());

    let marker_on_close = h.temp.path().join("must-not-be-written-on-close");
    let script = format!("/bin/sleep 1; /usr/bin/touch {}", marker_on_close.display());
    let mut start = owner(&restarted);
    start["action"] = json!("start");
    start["program"] = json!("/bin/sh");
    start["args"] = json!(["-c", script]);
    h.diagnostic(start).await;

    let mut external = Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "external_synthetic_server_helper_test",
            "--nocapture",
        ])
        .env("MCP_DIAGNOSTICS_EXTERNAL_SERVER", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut external_output = BufReader::new(external.stdout.take().unwrap());
    let address: std::net::SocketAddr = loop {
        let mut line = String::new();
        assert_ne!(external_output.read_line(&mut line).unwrap(), 0);
        if let Ok(address) = line.trim().parse() {
            break address;
        }
    };

    h.close(launch["session_id"].as_str().unwrap()).await;
    tokio::time::sleep(Duration::from_millis(1150)).await;
    assert!(
        !marker_on_close.exists(),
        "close must terminate manager-owned sidecar group"
    );
    let mut response = TcpStream::connect(address).unwrap();
    response
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .unwrap();
    let mut body = String::new();
    response.read_to_string(&mut body).unwrap();
    assert!(body.contains("200 OK"));
    assert!(
        external.try_wait().unwrap().is_none(),
        "external server must not be killed"
    );
    external.kill().unwrap();
    external.wait().unwrap();
}
