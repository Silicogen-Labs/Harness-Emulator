use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, ContentBlock};
use rmcp::{ErrorData, ServiceExt, schemars, tool, tool_router};
use serde::Deserialize;
use serde_json::{Value, json};
use silicogen_terminal_runtime::client::RuntimeClient;
use silicogen_terminal_runtime::protocol::{
    LaunchSpec, MouseButton, MouseInput, RpcRequest, WaitPredicate, WheelDirection,
};
use std::path::PathBuf;

#[derive(Clone)]
struct EmulationMcp {
    client: RuntimeClient,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct LaunchArgs {
    /// Executable path or program name. Arguments are passed as an array, never through a shell.
    program: Option<String>,
    /// Arguments passed directly to the executable.
    #[serde(default)]
    args: Vec<String>,
    /// Existing working directory for the application.
    cwd: Option<String>,
    /// Extra environment variables passed as a key/value object.
    #[serde(default)]
    env: std::collections::BTreeMap<String, String>,
    /// Optional human-readable session name.
    name: Option<String>,
    /// Terminal columns.
    #[serde(default = "default_cols")]
    cols: u16,
    /// Terminal rows.
    #[serde(default = "default_rows")]
    rows: u16,
    /// Retain a bounded raw PTY stream. May include sensitive visible output.
    #[serde(default)]
    record_output: bool,
    /// Configured state/profile/environment/workspace/isolation options.
    #[schemars(with = "Option<std::collections::BTreeMap<String, Value>>")]
    options: Option<Value>,
    profile: Option<String>,
    state_mode: Option<String>,
    env_inherit: Option<Vec<String>>,
    #[serde(default)]
    env_unset: Vec<String>,
    #[schemars(with = "Option<std::collections::BTreeMap<String, Value>>")]
    workspace: Option<Value>,
    #[schemars(with = "Option<std::collections::BTreeMap<String, Value>>")]
    isolation: Option<Value>,
    #[schemars(with = "Option<std::collections::BTreeMap<String, Value>>")]
    shell: Option<Value>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct OperationArgs {
    #[serde(flatten)]
    params: std::collections::BTreeMap<String, Value>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct SessionArgs {
    session_id: String,
    actor_id: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ViewStartArgs {
    session_id: String,
    /// Allow this browser grant to request exclusive terminal input control.
    #[serde(default)]
    allow_browser_control: bool,
    /// Optional stable key; retries return the same unredeemed viewing URL.
    idempotency_key: Option<String>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ViewArgs {
    view_id: String,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ControlReleaseArgs {
    session_id: String,
    /// Generation returned by terminal_control_status.
    expected_generation: u64,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct CellsArgs {
    session_id: String,
    /// Zero-based top row.
    row: u16,
    /// Zero-based left column.
    col: u16,
    /// Number of rows, at most 40.
    rows: u16,
    /// Number of columns, at most 120.
    cols: u16,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct KeyArgs {
    session_id: String,
    /// Logical key token, for example Down, Enter, Escape, or Ctrl+C.
    key: String,
    #[schemars(with = "Option<std::collections::BTreeMap<String, Value>>")]
    context: Option<Value>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct TextArgs {
    session_id: String,
    text: String,
    #[schemars(with = "Option<std::collections::BTreeMap<String, Value>>")]
    context: Option<Value>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct ResizeArgs {
    session_id: String,
    cols: u16,
    rows: u16,
    #[schemars(with = "Option<std::collections::BTreeMap<String, Value>>")]
    context: Option<Value>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct MouseArgs {
    session_id: String,
    /// move, click, down, up, wheel_up, or wheel_down
    action: String,
    /// Zero-based row coordinate.
    row: u16,
    /// Zero-based column coordinate.
    col: u16,
    /// Mouse button for click/down; defaults to left.
    button: Option<String>,
    /// Wheel steps, defaults to one.
    steps: Option<u16>,
    #[schemars(with = "Option<std::collections::BTreeMap<String, Value>>")]
    context: Option<Value>,
}

#[derive(Debug, Deserialize, schemars::JsonSchema)]
struct WaitArgs {
    session_id: String,
    /// contains, absent, regex, stable, or process_exited
    #[serde(default)]
    kind: String,
    /// Text or regex pattern where applicable.
    value: Option<String>,
    /// Stability duration or overall timeout, in milliseconds.
    duration_ms: Option<u64>,
    /// Bounded overall timeout, in milliseconds.
    timeout_ms: Option<u64>,
    #[schemars(with = "Option<std::collections::BTreeMap<String, Value>>")]
    predicate: Option<Value>,
    wait_id: Option<String>,
    poll_ms: Option<u64>,
}

#[tool_router(server_handler)]
impl EmulationMcp {
    #[tool(
        name = "terminal_launch",
        description = "Launch a TUI under a persistent PTY. Executable and args are passed directly; no implicit shell is used."
    )]
    async fn launch(
        &self,
        Parameters(args): Parameters<LaunchArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let cwd = args.cwd.unwrap_or_else(|| {
            std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .display()
                .to_string()
        });
        let mut options = args.options.unwrap_or_else(|| json!({}));
        if let Some(profile) = args.profile {
            options["profile"] = json!(profile);
            options["state_mode"] = json!("profile");
        }
        if let Some(state_mode) = args.state_mode {
            options["state_mode"] = json!(state_mode);
        }
        if let Some(names) = args.env_inherit {
            options["env_inherit"] = json!(names);
        }
        if !args.env_unset.is_empty() {
            options["env_unset"] = json!(args.env_unset);
        }
        if let Some(workspace) = args.workspace {
            options["workspace"] = workspace;
        }
        if let Some(isolation) = args.isolation {
            options["isolation"] = isolation;
        }
        if let Some(shell) = args.shell {
            options["shell"] = shell;
        }
        let options = serde_json::from_value(options).map_err(tool_error)?;
        let result = self
            .client
            .request(RpcRequest::LaunchConfigured {
                spec: LaunchSpec {
                    name: args.name,
                    program: args.program.unwrap_or_default(),
                    args: args.args,
                    cwd,
                    env: args.env,
                    cols: args.cols,
                    rows: args.rows,
                    record_output: args.record_output,
                },
                options,
            })
            .await
            .map_err(tool_error)?;
        Ok(tool_result(result))
    }

    #[tool(
        name = "terminal_list",
        description = "List active and historical terminal sessions."
    )]
    async fn list(&self) -> Result<CallToolResult, ErrorData> {
        self.call(RpcRequest::List).await
    }

    #[tool(
        name = "terminal_status",
        description = "Read a terminal session's lifecycle state."
    )]
    async fn status(
        &self,
        Parameters(args): Parameters<SessionArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call(RpcRequest::Status {
            session_id: args.session_id,
        })
        .await
    }

    #[tool(
        name = "terminal_view_start",
        description = "Create a short-lived, localhost-only browser URL for an existing terminal session. The URL grants access to that session's visible terminal content."
    )]
    async fn view_start(
        &self,
        Parameters(args): Parameters<ViewStartArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call(RpcRequest::ViewStart {
            session_id: args.session_id,
            allow_browser_control: args.allow_browser_control,
            idempotency_key: args.idempotency_key,
        })
        .await
    }

    #[tool(
        name = "terminal_view_status",
        description = "Read a view grant's state and connection count without returning its secret URL."
    )]
    async fn view_status(
        &self,
        Parameters(args): Parameters<ViewArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call(RpcRequest::ViewStatus {
            view_id: args.view_id,
        })
        .await
    }

    #[tool(
        name = "terminal_view_stop",
        description = "Revoke one browser viewing grant and disconnect its viewers while leaving the application running."
    )]
    async fn view_stop(
        &self,
        Parameters(args): Parameters<ViewArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call(RpcRequest::ViewStop {
            view_id: args.view_id,
        })
        .await
    }

    #[tool(
        name = "terminal_control_status",
        description = "Read whether automation or a browser currently owns terminal input, including its generation."
    )]
    async fn control_status(
        &self,
        Parameters(args): Parameters<SessionArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call(RpcRequest::ControlStatus {
            session_id: args.session_id,
        })
        .await
    }

    #[tool(
        name = "terminal_control_release",
        description = "Return terminal input to automation only if expected_generation still matches the observed owner."
    )]
    async fn control_release(
        &self,
        Parameters(args): Parameters<ControlReleaseArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call(RpcRequest::ControlRelease {
            session_id: args.session_id,
            expected_generation: args.expected_generation,
        })
        .await
    }

    #[tool(
        name = "terminal_observe",
        description = "Observe one atomic terminal frame: text, styled cells, dimensions, cursor, modes, and frame identity."
    )]
    async fn observe(
        &self,
        Parameters(args): Parameters<SessionArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call(RpcRequest::Observe {
            session_id: args.session_id,
        })
        .await
    }

    #[tool(
        name = "terminal_cells",
        description = "Read a bounded cell rectangle with styles from a single frame."
    )]
    async fn cells(
        &self,
        Parameters(args): Parameters<CellsArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call(RpcRequest::Cells {
            session_id: args.session_id,
            row: args.row,
            col: args.col,
            rows: args.rows,
            cols: args.cols,
        })
        .await
    }

    #[tool(
        name = "terminal_screenshot",
        description = "Capture the current terminal frame as a PNG image and return its run/frame identity."
    )]
    async fn screenshot(
        &self,
        Parameters(args): Parameters<SessionArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let result = self
            .client
            .request(RpcRequest::Screenshot {
                session_id: args.session_id,
            })
            .await
            .map_err(tool_error)?;
        let path = result
            .get("path")
            .and_then(Value::as_str)
            .ok_or_else(|| ErrorData::internal_error("screenshot response omitted path", None))?;
        let bytes = tokio::fs::read(path).await.map_err(|error| {
            ErrorData::internal_error(
                "cannot read screenshot artifact",
                Some(json!({"message":error.to_string()})),
            )
        })?;
        let encoded = STANDARD.encode(bytes);
        Ok(CallToolResult::success(vec![
            ContentBlock::image(encoded, "image/png"),
            ContentBlock::text(serde_json::to_string(&result).map_err(tool_error)?),
        ]))
    }

    #[tool(
        name = "terminal_key",
        description = "Send a logical key encoded for the application's negotiated keyboard mode."
    )]
    async fn key(
        &self,
        Parameters(args): Parameters<KeyArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let action = json!({"kind":"key","key":args.key});
        self.guarded(
            args.session_id.clone(),
            args.context,
            action,
            RpcRequest::Key {
                session_id: args.session_id,
                key: args.key,
            },
        )
        .await
    }

    #[tool(
        name = "terminal_type",
        description = "Type literal UTF-8 text into a terminal session."
    )]
    async fn type_text(
        &self,
        Parameters(args): Parameters<TextArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let action = json!({"kind":"type","text":args.text});
        self.guarded(
            args.session_id.clone(),
            args.context,
            action,
            RpcRequest::Type {
                session_id: args.session_id,
                text: args.text,
            },
        )
        .await
    }

    #[tool(
        name = "terminal_paste",
        description = "Paste literal text, using bracketed-paste markers only when the application enabled that mode."
    )]
    async fn paste(
        &self,
        Parameters(args): Parameters<TextArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let action = json!({"kind":"paste","text":args.text});
        self.guarded(
            args.session_id.clone(),
            args.context,
            action,
            RpcRequest::Paste {
                session_id: args.session_id,
                text: args.text,
            },
        )
        .await
    }

    #[tool(
        name = "terminal_mouse",
        description = "Move/click/wheel at zero-based terminal-cell row/column coordinates. Fails if the app did not negotiate supported SGR mouse modes."
    )]
    async fn mouse(
        &self,
        Parameters(args): Parameters<MouseArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let button = match args.button.as_deref().unwrap_or("left") {
            "left" => MouseButton::Left,
            "middle" => MouseButton::Middle,
            "right" => MouseButton::Right,
            value => {
                return Err(ErrorData::invalid_params(
                    format!("unknown mouse button {value}"),
                    None,
                ));
            }
        };
        let input = match args.action.as_str() {
            "move" => MouseInput::Move {
                row: args.row,
                col: args.col,
            },
            "click" => MouseInput::Click {
                row: args.row,
                col: args.col,
                button,
            },
            "down" => MouseInput::Down {
                row: args.row,
                col: args.col,
                button,
            },
            "up" => MouseInput::Up {
                row: args.row,
                col: args.col,
            },
            "wheel_up" => MouseInput::Wheel {
                row: args.row,
                col: args.col,
                direction: WheelDirection::Up,
                steps: args.steps.unwrap_or(1),
            },
            "wheel_down" => MouseInput::Wheel {
                row: args.row,
                col: args.col,
                direction: WheelDirection::Down,
                steps: args.steps.unwrap_or(1),
            },
            value => {
                return Err(ErrorData::invalid_params(
                    format!("unknown mouse action {value}"),
                    None,
                ));
            }
        };
        let action = json!({"kind":"mouse","input":input});
        self.guarded(
            args.session_id.clone(),
            args.context,
            action,
            RpcRequest::Mouse {
                session_id: args.session_id,
                input,
            },
        )
        .await
    }

    #[tool(
        name = "terminal_resize",
        description = "Resize both PTY and terminal state."
    )]
    async fn resize(
        &self,
        Parameters(args): Parameters<ResizeArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        let action = json!({"kind":"resize","cols":args.cols,"rows":args.rows});
        self.guarded(
            args.session_id.clone(),
            args.context,
            action,
            RpcRequest::Resize {
                session_id: args.session_id,
                cols: args.cols,
                rows: args.rows,
            },
        )
        .await
    }

    #[tool(
        name = "terminal_wait",
        description = "Wait for a bounded visible-text, regex, stable-screen, or process-exit condition."
    )]
    async fn wait(
        &self,
        Parameters(args): Parameters<WaitArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        if args.predicate.is_some()
            || args.wait_id.is_some()
            || args.kind == "output_quiet"
            || args.kind == "ready"
        {
            let mut params = json!({"session_id":args.session_id,"timeout_ms":args.timeout_ms.unwrap_or(30000),"poll_ms":args.poll_ms.unwrap_or(200)});
            if let Some(id) = args.wait_id {
                params["wait_id"] = json!(id);
            }
            if let Some(predicate) = args.predicate {
                params["predicate"] = predicate;
            } else if args.kind == "output_quiet" {
                params["predicate"] =
                    json!({"kind":"output_quiet","duration_ms":args.duration_ms.unwrap_or(500)});
            }
            return self
                .call(RpcRequest::Agent {
                    operation: "wait".into(),
                    params,
                })
                .await;
        }
        let predicate = match args.kind.as_str() {
            "contains" => WaitPredicate::TextContains {
                text: required(args.value, "value")?,
            },
            "absent" => WaitPredicate::TextAbsent {
                text: required(args.value, "value")?,
            },
            "regex" => WaitPredicate::Regex {
                pattern: required(args.value, "value")?,
            },
            "stable" => WaitPredicate::Stable {
                duration_ms: args.duration_ms.unwrap_or(500),
            },
            "process_exited" => WaitPredicate::ProcessExited,
            value => {
                return Err(ErrorData::invalid_params(
                    format!("unknown wait kind {value}"),
                    None,
                ));
            }
        };
        self.call(RpcRequest::Wait {
            session_id: args.session_id,
            predicate,
            timeout_ms: args.timeout_ms.unwrap_or(30_000),
        })
        .await
    }

    #[tool(
        name = "terminal_history",
        description = "Read the bounded event history and any opt-in raw recording metadata."
    )]
    async fn history(
        &self,
        Parameters(args): Parameters<SessionArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.call(RpcRequest::History {
            session_id: args.session_id,
        })
        .await
    }

    #[tool(
        name = "terminal_restart",
        description = "Restart a session's application from its original argv and launch configuration."
    )]
    async fn restart(
        &self,
        Parameters(args): Parameters<SessionArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        if let Some(actor_id) = args.actor_id {
            return self
                .call(RpcRequest::Agent {
                    operation: "restart".into(),
                    params: json!({"session_id":args.session_id,"actor_id":actor_id}),
                })
                .await;
        }
        self.call(RpcRequest::Restart {
            session_id: args.session_id,
        })
        .await
    }

    #[tool(
        name = "terminal_close",
        description = "Terminate the application's process group and release the PTY."
    )]
    async fn close(
        &self,
        Parameters(args): Parameters<SessionArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        if let Some(actor_id) = args.actor_id {
            return self
                .call(RpcRequest::Agent {
                    operation: "close".into(),
                    params: json!({"session_id":args.session_id,"actor_id":actor_id}),
                })
                .await;
        }
        self.call(RpcRequest::Close {
            session_id: args.session_id,
        })
        .await
    }

    #[tool(
        name = "terminal_profiles",
        description = "List/create/store named state profiles. Parameters: action=list|create|store|template; name, description, profile as applicable. Metadata omits credentials."
    )]
    async fn profiles(
        &self,
        Parameters(args): Parameters<OperationArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.extension("profiles", args).await
    }
    #[tool(
        name = "terminal_capabilities",
        description = "Effective launch/profile/isolation and input capabilities. Requires session_id."
    )]
    async fn capabilities(
        &self,
        Parameters(args): Parameters<OperationArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.extension("capabilities", args).await
    }
    #[tool(
        name = "terminal_artifacts",
        description = "List registered session artifacts. session_id, optional offset/limit."
    )]
    async fn artifacts(
        &self,
        Parameters(args): Parameters<OperationArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.extension("artifacts", args).await
    }
    #[tool(
        name = "terminal_read_artifact",
        description = "Bounded registered file read. session_id, artifact_id, optional offset,length. No arbitrary host paths."
    )]
    async fn read_artifact(
        &self,
        Parameters(args): Parameters<OperationArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.extension("read_artifact", args).await
    }
    #[tool(
        name = "terminal_logs",
        description = "Incremental logs with rotation/truncation cursor. session_id, source=app|startup|daemon, optional path,cursor,max_bytes."
    )]
    async fn logs(
        &self,
        Parameters(args): Parameters<OperationArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.extension("logs", args).await
    }
    #[tool(
        name = "terminal_save_frame",
        description = "Save immutable terminal cell baseline. session_id,name."
    )]
    async fn save_frame(
        &self,
        Parameters(args): Parameters<OperationArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.extension("save_frame", args).await
    }
    #[tool(
        name = "terminal_assert",
        description = "Assert visible text/regex/cell/style. session_id,assertion tagged kind=text_contains|text_absent|regex|cell_text|cell_style|all."
    )]
    async fn assert_frame(
        &self,
        Parameters(args): Parameters<OperationArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.extension("assert", args).await
    }
    #[tool(
        name = "terminal_diff",
        description = "Compare cells/styles with saved baseline. session_id,name, optional explicit masks."
    )]
    async fn diff(
        &self,
        Parameters(args): Parameters<OperationArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.extension("diff", args).await
    }
    #[tool(
        name = "terminal_wait_ready",
        description = "Resumable readiness/output quiet/region stability wait. session_id,predicate,timeout_ms,poll_ms; resume with wait_id. Quiet does not imply ready."
    )]
    async fn wait_ready(
        &self,
        Parameters(args): Parameters<OperationArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.extension("wait", args).await
    }
    #[tool(
        name = "terminal_batch",
        description = "Ordered bounded actions with actor and stale-run/viewport checks. session_id,context={actor_id,action_id,expected_run_id,expected_viewport_revision},actions=[{kind,...}]."
    )]
    async fn batch(
        &self,
        Parameters(args): Parameters<OperationArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.extension("batch", args).await
    }
    #[tool(
        name = "terminal_actor",
        description = "Claim/release an automation controller lease. session_id,actor_id,release. Browser takeover remains authoritative."
    )]
    async fn actor(
        &self,
        Parameters(args): Parameters<OperationArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.extension("actor", args).await
    }
    #[tool(
        name = "terminal_scenario_run",
        description = "Start validated durable JSON scenario. scenario={version,name,steps}, optional workspace. Returns job_id; poll status independently."
    )]
    async fn scenario_run(
        &self,
        Parameters(args): Parameters<OperationArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.extension("scenario_run", args).await
    }
    #[tool(
        name = "terminal_scenario_status",
        description = "Read durable scenario step outcomes/evidence. Requires job_id."
    )]
    async fn scenario_status(
        &self,
        Parameters(args): Parameters<OperationArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.extension("scenario_status", args).await
    }
    #[tool(
        name = "terminal_scenario_cancel",
        description = "Cancel scenario and perform configured cleanup. Requires job_id."
    )]
    async fn scenario_cancel(
        &self,
        Parameters(args): Parameters<OperationArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.extension("scenario_cancel", args).await
    }
    #[tool(
        name = "terminal_cleanup",
        description = "Preview managed age/count/byte retention; apply requires exact preview plan. Protects live sessions, viewers and profiles; unknown legacy folders are inventory only."
    )]
    async fn cleanup(
        &self,
        Parameters(args): Parameters<OperationArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.extension("cleanup", args).await
    }
    #[tool(
        name = "terminal_relaunch",
        description = "Explicitly relaunch interrupted historical session from sanitized manifest; inline secrets must be provided through new launch/profile. session_id."
    )]
    async fn relaunch(
        &self,
        Parameters(args): Parameters<OperationArgs>,
    ) -> Result<CallToolResult, ErrorData> {
        self.extension("relaunch", args).await
    }
}

impl EmulationMcp {
    async fn guarded(
        &self,
        session_id: String,
        context: Option<Value>,
        action: Value,
        legacy: RpcRequest,
    ) -> Result<CallToolResult, ErrorData> {
        if let Some(context) = context {
            self.call(RpcRequest::Agent {
                operation: "batch".into(),
                params: json!({"session_id":session_id,"context":context,"actions":[action]}),
            })
            .await
        } else {
            self.call(legacy).await
        }
    }
    async fn extension(
        &self,
        operation: &str,
        args: OperationArgs,
    ) -> Result<CallToolResult, ErrorData> {
        self.call(RpcRequest::Agent {
            operation: operation.into(),
            params: serde_json::to_value(args.params).map_err(tool_error)?,
        })
        .await
    }
    async fn call(&self, request: RpcRequest) -> Result<CallToolResult, ErrorData> {
        self.client
            .request(request)
            .await
            .map(tool_result)
            .map_err(tool_error)
    }
}

pub async fn run(runtime_dir: PathBuf) -> anyhow::Result<()> {
    let server = EmulationMcp {
        client: RuntimeClient::new(runtime_dir),
    };
    let service = server.serve(rmcp::transport::stdio()).await?;
    service.waiting().await?;
    Ok(())
}

// MCP structuredContent must be an object. Preserve array results in the JSON
// text block rather than advertising an invalid unconstrained output schema.
fn tool_result(value: Value) -> CallToolResult {
    let mut result = CallToolResult::success(vec![ContentBlock::text(value.to_string())]);
    if value.is_object() {
        result.structured_content = Some(value);
    }
    result
}

fn required(value: Option<String>, field: &str) -> Result<String, ErrorData> {
    value.ok_or_else(|| ErrorData::invalid_params(format!("{field} is required"), None))
}

fn tool_error(error: impl Into<anyhow::Error>) -> ErrorData {
    let error = error.into();
    if let Some(runtime) = error.downcast_ref::<silicogen_terminal_runtime::client::RuntimeError>()
    {
        return ErrorData::internal_error(
            runtime.0.message.clone(),
            Some(json!({"code":runtime.0.code,"details":runtime.0.details})),
        );
    }
    ErrorData::internal_error(error.to_string(), None)
}

fn default_cols() -> u16 {
    120
}
fn default_rows() -> u16 {
    40
}
