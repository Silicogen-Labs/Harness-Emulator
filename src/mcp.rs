use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use rmcp::handler::server::wrapper::Parameters;
use rmcp::model::{CallToolResult, ContentBlock};
use rmcp::{ErrorData, ServiceExt, tool, tool_router};
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

use silicogen_terminal_runtime::contracts::*;
use silicogen_terminal_runtime::validation;

#[tool_router]
impl EmulationMcp {
    #[tool(
        name = "terminal_launch",
        input_schema = rmcp::handler::server::common::schema_for_input::<LaunchArgs>().expect("valid shared input schema"),
        description = "Launch a TUI under a persistent PTY. Executable and args are passed directly; no implicit shell is used."
    )]
    async fn launch(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("launch", &raw).map_err(tool_error)?;
        let args: LaunchArgs = validation::decode(raw).map_err(tool_error)?;
        let cwd = args.cwd.unwrap_or_else(|| {
            std::env::current_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .display()
                .to_string()
        });
        let mut options =
            serde_json::to_value(args.options.unwrap_or_default()).map_err(tool_error)?;
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
            options["workspace"] = serde_json::to_value(workspace).map_err(tool_error)?;
        }
        if let Some(isolation) = args.isolation {
            options["isolation"] = serde_json::to_value(isolation).map_err(tool_error)?;
        }
        if let Some(shell) = args.shell {
            options["shell"] = serde_json::to_value(shell).map_err(tool_error)?;
        }
        let options = serde_json::from_value(options).map_err(tool_error)?;
        let result = self
            .client
            .request_v2(RpcRequest::LaunchConfigured {
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
        input_schema = rmcp::handler::server::common::schema_for_input::<McpSessionArgs>().expect("valid shared input schema"),
        description = "Read a terminal session's lifecycle state."
    )]
    async fn status(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("status", &raw).map_err(tool_error)?;
        let args: McpSessionArgs = validation::decode(raw).map_err(tool_error)?;
        self.call(RpcRequest::Status {
            session_id: args.session_id,
        })
        .await
    }

    #[tool(
        name = "terminal_view_start",
        input_schema = rmcp::handler::server::common::schema_for_input::<ViewStartArgs>().expect("valid shared input schema"),
        description = "Create a short-lived, localhost-only browser URL for an existing terminal session. The URL grants access to that session's visible terminal content."
    )]
    async fn view_start(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("view_start", &raw).map_err(tool_error)?;
        let args: ViewStartArgs = validation::decode(raw).map_err(tool_error)?;
        self.call(RpcRequest::ViewStart {
            session_id: args.session_id,
            allow_browser_control: args.allow_browser_control,
            idempotency_key: args.idempotency_key,
        })
        .await
    }

    #[tool(
        name = "terminal_view_status",
        input_schema = rmcp::handler::server::common::schema_for_input::<ViewArgs>().expect("valid shared input schema"),
        description = "Read a view grant's state and connection count without returning its secret URL."
    )]
    async fn view_status(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("view_status", &raw).map_err(tool_error)?;
        let args: ViewArgs = validation::decode(raw).map_err(tool_error)?;
        self.call(RpcRequest::ViewStatus {
            view_id: args.view_id,
        })
        .await
    }

    #[tool(
        name = "terminal_view_stop",
        input_schema = rmcp::handler::server::common::schema_for_input::<ViewArgs>().expect("valid shared input schema"),
        description = "Revoke one browser viewing grant and disconnect its viewers while leaving the application running."
    )]
    async fn view_stop(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("view_stop", &raw).map_err(tool_error)?;
        let args: ViewArgs = validation::decode(raw).map_err(tool_error)?;
        self.call(RpcRequest::ViewStop {
            view_id: args.view_id,
        })
        .await
    }

    #[tool(
        name = "terminal_control_status",
        input_schema = rmcp::handler::server::common::schema_for_input::<McpSessionArgs>().expect("valid shared input schema"),
        description = "Read whether automation or a browser currently owns terminal input, including its generation."
    )]
    async fn control_status(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("control_status", &raw).map_err(tool_error)?;
        let args: McpSessionArgs = validation::decode(raw).map_err(tool_error)?;
        self.call(RpcRequest::ControlStatus {
            session_id: args.session_id,
        })
        .await
    }

    #[tool(
        name = "terminal_control_release",
        input_schema = rmcp::handler::server::common::schema_for_input::<ControlReleaseArgs>().expect("valid shared input schema"),
        description = "Return terminal input to automation only if expected_generation still matches the observed owner."
    )]
    async fn control_release(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("control_release", &raw).map_err(tool_error)?;
        let args: ControlReleaseArgs = validation::decode(raw).map_err(tool_error)?;
        self.call(RpcRequest::ControlRelease {
            session_id: args.session_id,
            expected_generation: args.expected_generation,
        })
        .await
    }

    #[tool(
        name = "terminal_observe",
        input_schema = rmcp::handler::server::common::schema_for_input::<ObserveArgs>().expect("valid shared input schema"),
        description = "Contract v2: observe an atomic frame. Defaults to bounded text without styles; response.mode=summary|text|diff|full. Full snapshots require explicit mode=full. Diff requires the baseline session/run/frame identity."
    )]
    async fn observe(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("observe", &raw).map_err(tool_error)?;
        let args: ObserveArgs = validation::decode(raw).map_err(tool_error)?;
        self.call(RpcRequest::Contract {
            revision: 2,
            request: Box::new(RpcRequest::Observe {
                session_id: args.session_id,
            }),
            response: args.response,
        })
        .await
    }

    #[tool(
        name = "terminal_cells",
        input_schema = rmcp::handler::server::common::schema_for_input::<CellsArgs>().expect("valid shared input schema"),
        description = "Contract v2: read a bounded rectangle as compact [text,width,style_id,empty,hyperlink] cells with a shared style table, no full palette. Set response.styles=false for [text,width]; full opts into a larger region budget."
    )]
    async fn cells(&self, Parameters(raw): Parameters<Value>) -> Result<CallToolResult, ErrorData> {
        validation::operation("cells", &raw).map_err(tool_error)?;
        let args: CellsArgs = validation::decode(raw).map_err(tool_error)?;
        self.call(RpcRequest::Contract {
            revision: 2,
            request: Box::new(RpcRequest::Cells {
                session_id: args.session_id,
                row: args.row,
                col: args.col,
                rows: args.rows,
                cols: args.cols,
            }),
            response: args.response,
        })
        .await
    }

    #[tool(
        name = "terminal_locate",
        input_schema = rmcp::handler::server::common::schema_for_input::<silicogen_terminal_runtime::contracts::LocatorArgs>().expect("valid locator schema"),
        description = "Locate typed text or regex in the current viewport or a zero-based half-open cell region. Returns bounded match rectangles with observation and viewport identity."
    )]
    async fn locate(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("locate", &raw).map_err(tool_error)?;
        let args: silicogen_terminal_runtime::contracts::LocatorArgs =
            validation::decode(raw).map_err(tool_error)?;
        self.extension("locate", args).await
    }

    #[tool(
        name = "terminal_screenshot",
        input_schema = rmcp::handler::server::common::schema_for_input::<McpSessionArgs>().expect("valid shared input schema"),
        description = "Capture the current terminal frame as a PNG image and return its run/frame identity."
    )]
    async fn screenshot(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("screenshot", &raw).map_err(tool_error)?;
        let args: McpSessionArgs = validation::decode(raw).map_err(tool_error)?;
        let result = self
            .client
            .request_v2(RpcRequest::Screenshot {
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
        name = "terminal_crop",
        input_schema = rmcp::handler::server::common::schema_for_input::<CropArgs>().expect("valid crop schema"),
        description = "Save a bounded PNG crop of the current atomic terminal frame. Region coordinates are zero-based, half-open terminal cells; scale is 1..=4. Optionally supply the exact terminal_observe identity to reject stale frames. Returns registered PNG and metadata artifact IDs; retrieve image bytes with terminal_read_artifact."
    )]
    async fn crop(&self, Parameters(raw): Parameters<Value>) -> Result<CallToolResult, ErrorData> {
        validation::operation("crop", &raw).map_err(tool_error)?;
        let args: CropArgs = validation::decode(raw).map_err(tool_error)?;
        self.extension("crop", args).await
    }

    #[tool(
        name = "terminal_crop_compare",
        input_schema = rmcp::handler::server::common::schema_for_input::<CropCompareArgs>().expect("valid crop comparison schema"),
        description = "Compare two retained crops from this session using explicit per-channel and different-pixel tolerances. Requires compatible run, region, viewport geometry, renderer profile and bundled font. Pixel comparison is supplemental to terminal_diff."
    )]
    async fn crop_compare(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("crop_compare", &raw).map_err(tool_error)?;
        let args: CropCompareArgs = validation::decode(raw).map_err(tool_error)?;
        self.extension("crop_compare", args).await
    }

    #[tool(
        name = "terminal_key",
        input_schema = rmcp::handler::server::common::schema_for_input::<KeyArgs>().expect("valid shared input schema"),
        description = "Send a logical key encoded for the application's negotiated keyboard mode."
    )]
    async fn key(&self, Parameters(raw): Parameters<Value>) -> Result<CallToolResult, ErrorData> {
        validation::operation("key", &raw).map_err(tool_error)?;
        let args: KeyArgs = validation::decode(raw).map_err(tool_error)?;
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
        input_schema = rmcp::handler::server::common::schema_for_input::<TextArgs>().expect("valid shared input schema"),
        description = "Type literal UTF-8 text into a terminal session."
    )]
    async fn type_text(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("type_text", &raw).map_err(tool_error)?;
        let args: TextArgs = validation::decode(raw).map_err(tool_error)?;
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
        input_schema = rmcp::handler::server::common::schema_for_input::<TextArgs>().expect("valid shared input schema"),
        description = "Paste literal text, using bracketed-paste markers only when the application enabled that mode."
    )]
    async fn paste(&self, Parameters(raw): Parameters<Value>) -> Result<CallToolResult, ErrorData> {
        validation::operation("paste", &raw).map_err(tool_error)?;
        let args: TextArgs = validation::decode(raw).map_err(tool_error)?;
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
        input_schema = rmcp::handler::server::common::schema_for_input::<MouseArgs>().expect("valid shared input schema"),
        description = "Move/click/wheel at zero-based terminal-cell row/column coordinates. Fails if the app did not negotiate supported SGR mouse modes."
    )]
    async fn mouse(&self, Parameters(raw): Parameters<Value>) -> Result<CallToolResult, ErrorData> {
        validation::operation("mouse", &raw).map_err(tool_error)?;
        let args: MouseArgs = validation::decode(raw).map_err(tool_error)?;
        if let Some(steps) = args.steps {
            validation::range("steps", steps as u64, 1, 20).map_err(tool_error)?;
        }
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
        input_schema = rmcp::handler::server::common::schema_for_input::<ResizeArgs>().expect("valid shared input schema"),
        description = "Resize both PTY and terminal state."
    )]
    async fn resize(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("resize", &raw).map_err(tool_error)?;
        let args: ResizeArgs = validation::decode(raw).map_err(tool_error)?;
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
        input_schema = wait_input_schema(),
        description = "Revision 2, summary by default (1024 bytes). Legacy kind=contains|absent|regex|stable|process_exited: timeout_ms 1..60000. For resumable waits up to 3600000ms use predicate, e.g. {kind:text_contains,text:READY}, poll_ms 0..5000, then resume with wait_id. Quiet is not app readiness. Use response.mode=full for snapshots."
    )]
    async fn wait(&self, Parameters(raw): Parameters<Value>) -> Result<CallToolResult, ErrorData> {
        validation::operation("mcp_wait", &raw).map_err(tool_error)?;
        let args: McpWaitArgs = validation::decode(raw).map_err(tool_error)?;
        let response = args.response.clone();
        if args.predicate.is_some()
            || args.wait_id.is_some()
            || args.kind == "output_quiet"
            || args.kind == "ready"
        {
            let mut params = json!({"session_id":args.session_id,"timeout_ms":args.timeout_ms.unwrap_or(30000),"poll_ms":args.poll_ms.unwrap_or(200)});
            params["response"] = serde_json::to_value(response).map_err(tool_error)?;
            if let Some(id) = args.wait_id {
                params["wait_id"] = json!(id);
            }
            if let Some(predicate) = args.predicate {
                params["predicate"] = serde_json::to_value(predicate).map_err(tool_error)?;
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
            _ => {
                return Err(tool_error(validation::RequestError::new(
                    "kind",
                    "contains, absent, regex, stable, process_exited, output_quiet or ready",
                )));
            }
        };
        self.call(RpcRequest::Contract {
            revision: 2,
            request: Box::new(RpcRequest::Wait {
                session_id: args.session_id,
                predicate,
                timeout_ms: args.timeout_ms.unwrap_or(30_000),
            }),
            response,
        })
        .await
    }

    #[tool(
        name = "terminal_history",
        input_schema = rmcp::handler::server::common::schema_for_input::<McpSessionArgs>().expect("valid shared input schema"),
        description = "Read the bounded event history and any opt-in raw recording metadata."
    )]
    async fn history(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("history", &raw).map_err(tool_error)?;
        let args: McpSessionArgs = validation::decode(raw).map_err(tool_error)?;
        self.call(RpcRequest::History {
            session_id: args.session_id,
        })
        .await
    }

    #[tool(
        name = "terminal_restart",
        input_schema = rmcp::handler::server::common::schema_for_input::<McpSessionArgs>().expect("valid shared input schema"),
        description = "Restart a session's application from its original argv and launch configuration."
    )]
    async fn restart(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("restart", &raw).map_err(tool_error)?;
        let args: McpSessionArgs = validation::decode(raw).map_err(tool_error)?;
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
        input_schema = rmcp::handler::server::common::schema_for_input::<McpSessionArgs>().expect("valid shared input schema"),
        description = "Terminate the application's process group and release the PTY."
    )]
    async fn close(&self, Parameters(raw): Parameters<Value>) -> Result<CallToolResult, ErrorData> {
        validation::operation("close", &raw).map_err(tool_error)?;
        let args: McpSessionArgs = validation::decode(raw).map_err(tool_error)?;
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
        name = "terminal_finalize",
        input_schema = rmcp::handler::server::common::schema_for_input::<silicogen_terminal_runtime::finalization::FinalizeArgs>().expect("valid shared input schema"),
        description = "Explicitly finalize a test outcome for the observed run: passed, failed, cancelled, interrupted, or unknown. Terminates owned work; enforced test profiles dispose passed copies, retain bounded failure evidence, and remove imported credentials. Never infers success from exit/close. Pending disposal requires retry after viewer/job leases release. Idempotent for identical run/outcome; cannot change an outcome."
    )]
    async fn finalize(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("finalize", &raw).map_err(tool_error)?;
        let args: silicogen_terminal_runtime::finalization::FinalizeArgs =
            validation::decode(raw).map_err(tool_error)?;
        self.call(RpcRequest::Agent {
            operation: "finalize".into(),
            params: serde_json::to_value(args)
                .map_err(|error| ErrorData::internal_error(error.to_string(), None))?,
        })
        .await
    }

    #[tool(
        name = "terminal_profiles",
        input_schema = rmcp::handler::server::common::schema_for_input::<ProfileArgs>().expect("valid shared input schema"),
        description = "List/create/store named state profiles. action=list|create|store|template; name, description, profile as applicable. template returns silicogen-test with enforced Bubblewrap/network-none, required independent workspace and selected-file config/auth seeds. Store it with explicit seed files, then launch with profile and workspace. Metadata omits credentials. Legacy profiles without test_policy keep unrestricted host behavior."
    )]
    async fn profiles(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("profiles", &raw).map_err(tool_error)?;
        let args: ProfileArgs = validation::decode(raw).map_err(tool_error)?;
        self.extension("profiles", args).await
    }
    #[tool(
        name = "terminal_capabilities",
        input_schema = rmcp::handler::server::common::schema_for_input::<SessionArgs>().expect("valid shared input schema"),
        description = "Effective launch/profile/isolation and input capabilities. Requires session_id."
    )]
    async fn capabilities(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("capabilities", &raw).map_err(tool_error)?;
        let args: SessionArgs = validation::decode(raw).map_err(tool_error)?;
        self.extension("capabilities", args).await
    }
    #[tool(
        name = "terminal_artifacts",
        input_schema = rmcp::handler::server::common::schema_for_input::<ArtifactArgs>().expect("valid shared input schema"),
        description = "List registered session artifacts. session_id, optional offset/limit."
    )]
    async fn artifacts(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("artifacts", &raw).map_err(tool_error)?;
        let args: ArtifactArgs = validation::decode(raw).map_err(tool_error)?;
        self.extension("artifacts", args).await
    }
    #[tool(
        name = "terminal_read_artifact",
        input_schema = rmcp::handler::server::common::schema_for_input::<ArtifactArgs>().expect("valid shared input schema"),
        description = "Bounded registered file read. session_id, artifact_id, optional offset,length. No arbitrary host paths."
    )]
    async fn read_artifact(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("read_artifact", &raw).map_err(tool_error)?;
        let args: ArtifactArgs = validation::decode(raw).map_err(tool_error)?;
        self.extension("read_artifact", args).await
    }
    #[tool(
        name = "terminal_logs",
        input_schema = rmcp::handler::server::common::schema_for_input::<LogsArgs>().expect("valid shared input schema"),
        description = "Incremental logs with rotation/truncation cursor. session_id, source=app|startup|daemon, optional path,cursor,max_bytes."
    )]
    async fn logs(&self, Parameters(raw): Parameters<Value>) -> Result<CallToolResult, ErrorData> {
        validation::operation("logs", &raw).map_err(tool_error)?;
        let args: LogsArgs = validation::decode(raw).map_err(tool_error)?;
        self.extension("logs", args).await
    }
    #[tool(
        name = "terminal_save_frame",
        input_schema = rmcp::handler::server::common::schema_for_input::<SaveArgs>().expect("valid shared input schema"),
        description = "Save immutable terminal cell baseline. session_id,name."
    )]
    async fn save_frame(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("save_frame", &raw).map_err(tool_error)?;
        let args: SaveArgs = validation::decode(raw).map_err(tool_error)?;
        self.extension("save_frame", args).await
    }
    #[tool(
        name = "terminal_assert",
        input_schema = rmcp::handler::server::common::schema_for_input::<AssertArgs>().expect("valid shared input schema"),
        description = "Assert visible text/regex/cell/style. session_id,assertion tagged kind=text_contains|text_absent|regex|cell_text|cell_style|all."
    )]
    async fn assert_frame(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("assert", &raw).map_err(tool_error)?;
        let args: AssertArgs = validation::decode(raw).map_err(tool_error)?;
        self.extension("assert", args).await
    }
    #[tool(
        name = "terminal_diff",
        input_schema = rmcp::handler::server::common::schema_for_input::<DiffArgs>().expect("valid shared input schema"),
        description = "Compare cells/styles with saved baseline. session_id,name, optional explicit masks."
    )]
    async fn diff(&self, Parameters(raw): Parameters<Value>) -> Result<CallToolResult, ErrorData> {
        validation::operation("diff", &raw).map_err(tool_error)?;
        let args: DiffArgs = validation::decode(raw).map_err(tool_error)?;
        self.extension("diff", args).await
    }
    #[tool(
        name = "terminal_wait_ready",
        input_schema = rmcp::handler::server::common::schema_for_input::<WaitArgs>().expect("valid shared input schema"),
        description = "Resumable readiness/output quiet/region stability wait. session_id,predicate,timeout_ms,poll_ms; resume with wait_id. Quiet does not imply ready."
    )]
    async fn wait_ready(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("wait", &raw).map_err(tool_error)?;
        let args: WaitArgs = validation::decode(raw).map_err(tool_error)?;
        self.extension("wait", args).await
    }

    #[tool(
        name = "terminal_wait_fresh",
        input_schema = rmcp::handler::server::common::schema_for_input::<silicogen_terminal_runtime::contracts::FreshWaitArgs>().expect("valid fresh wait schema"),
        description = "Wait for current presence, newly present text, or a match intersecting changed cells since prior_observation. Create with session_id, prior_observation, spec and timeout_ms; resume with wait_id."
    )]
    async fn wait_fresh(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("wait_fresh", &raw).map_err(tool_error)?;
        let args: silicogen_terminal_runtime::contracts::FreshWaitArgs =
            validation::decode(raw).map_err(tool_error)?;
        self.extension("wait_fresh", args).await
    }
    #[tool(
        name = "terminal_sequence",
        input_schema = rmcp::handler::server::common::schema_for_input::<SequenceArgs>().expect("valid sequence schema"),
        description = "Start, inspect, resume, or cancel a durable observation-aware input sequence. Start requires a claimed actor, exact run/viewport/controller generation, timeout_ms and typed steps. PTY acceptance is not application acknowledgement."
    )]
    async fn sequence(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("sequence", &raw).map_err(tool_error)?;
        let args: SequenceArgs = validation::decode(raw).map_err(tool_error)?;
        self.extension("sequence", args).await
    }
    #[tool(
        name = "terminal_capture",
        input_schema = rmcp::handler::server::common::schema_for_input::<silicogen_terminal_runtime::contracts::CaptureJobArgs>().expect("valid capture job schema"),
        description = "Arm before triggering an action, then inspect/cancel a bounded transient-UI capture or materialize a retained frame. Sampling is best-effort and never guarantees every transition."
    )]
    async fn capture(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("capture_job", &raw).map_err(tool_error)?;
        let args: silicogen_terminal_runtime::contracts::CaptureJobArgs =
            validation::decode(raw).map_err(tool_error)?;
        self.extension("capture_job", args).await
    }
    #[tool(
        name = "terminal_diagnostics",
        input_schema = rmcp::handler::server::common::schema_for_input::<DiagnosticsArgs>().expect("valid diagnostics schema"),
        description = "Start/read/status/cancel/stop a bounded argv diagnostic owned by the exact live session+run, or seed a relative file in that session's copied workspace. The daemon derives cwd, environment and containment from trusted launch metadata; requests cannot choose them. Host process-group mode does not contain descendants that detach or escape the verified group."
    )]
    async fn diagnostics(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("diagnostics", &raw).map_err(tool_error)?;
        let args: DiagnosticsArgs = validation::decode(raw).map_err(tool_error)?;
        self.extension("diagnostics", args).await
    }
    #[tool(
        name = "terminal_recording",
        input_schema = rmcp::handler::server::common::schema_for_input::<RecordingArgs>().expect("valid recording schema"),
        description = "Explicitly opt in to a bounded sanitized logical recording for one exact session/run. Start records trusted session-derived launch metadata. Captured only after success: actor-guarded MCP input routed through the shared batch service, terminal_wait_ready and terminal_assert. stop persists; export emits scenario v1 plus unresolved/result bindings. terminal_observe, browser input, legacy RPC input, sequences and other automation operations are not captured. Never records raw PTY bytes."
    )]
    async fn recording(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("recording", &raw).map_err(tool_error)?;
        let args: RecordingArgs = validation::decode(raw).map_err(tool_error)?;
        self.extension("recording", args).await
    }
    #[tool(
        name = "terminal_scroll_until",
        input_schema = rmcp::handler::server::common::schema_for_input::<silicogen_terminal_runtime::contracts::ScrollUntilArgs>().expect("valid scroll schema"),
        description = "Scroll an application-owned viewport (not terminal scrollback) with a finite step/deadline budget and verified key/wheel bindings. Requires the current actor, run, viewport, and controller generation; never claims control. Returns found, bounded not-found, or no-progress status."
    )]
    async fn scroll_until(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("scroll_until", &raw).map_err(tool_error)?;
        let args: silicogen_terminal_runtime::contracts::ScrollUntilArgs =
            validation::decode(raw).map_err(tool_error)?;
        self.extension("scroll_until", args).await
    }
    #[tool(
        name = "terminal_app_adapter",
        input_schema = rmcp::handler::server::common::schema_for_input::<silicogen_terminal_runtime::contracts::AppAdapterArgs>().expect("valid app adapter schema"),
        description = "Bind read-only Silicogen evidence to a daemon-owned backend and a live TUI explicitly attached to its exact loopback port/workspace. Then read only allowlisted session/messages/status/SSE; UI state stays unavailable without a connected producer. Never accepts arbitrary URLs or reads SQLite."
    )]
    async fn app_adapter(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("app_adapter", &raw).map_err(tool_error)?;
        let args: silicogen_terminal_runtime::contracts::AppAdapterArgs =
            validation::decode(raw).map_err(tool_error)?;
        self.extension("app_adapter", args).await
    }
    #[tool(
        name = "terminal_batch",
        input_schema = rmcp::handler::server::common::schema_for_input::<BatchArgs>().expect("valid shared input schema"),
        description = "Ordered bounded actions with actor and stale-run/viewport checks. session_id,context={actor_id,action_id,expected_run_id,expected_viewport_revision},actions=[{kind,...}]."
    )]
    async fn batch(&self, Parameters(raw): Parameters<Value>) -> Result<CallToolResult, ErrorData> {
        validation::operation("batch", &raw).map_err(tool_error)?;
        let args: BatchArgs = validation::decode(raw).map_err(tool_error)?;
        self.extension("batch", args).await
    }
    #[tool(
        name = "terminal_actor",
        input_schema = rmcp::handler::server::common::schema_for_input::<ActorArgs>().expect("valid shared input schema"),
        description = "Claim/release an automation controller lease. session_id,actor_id,release. Browser takeover remains authoritative."
    )]
    async fn actor(&self, Parameters(raw): Parameters<Value>) -> Result<CallToolResult, ErrorData> {
        validation::operation("actor", &raw).map_err(tool_error)?;
        let args: ActorArgs = validation::decode(raw).map_err(tool_error)?;
        self.extension("actor", args).await
    }
    #[tool(
        name = "terminal_scenario_run",
        input_schema = rmcp::handler::server::common::schema_for_input::<ScenarioArgs>().expect("valid shared input schema"),
        description = "Start validated durable JSON scenario. scenario={version,name,steps}, optional workspace. Returns job_id; poll status independently."
    )]
    async fn scenario_run(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("scenario_run", &raw).map_err(tool_error)?;
        let args: ScenarioArgs = validation::decode(raw).map_err(tool_error)?;
        self.extension("scenario_run", args).await
    }
    #[tool(
        name = "terminal_scenario_status",
        input_schema = rmcp::handler::server::common::schema_for_input::<JobArgs>().expect("valid shared input schema"),
        description = "Read durable scenario step outcomes/evidence. Requires job_id."
    )]
    async fn scenario_status(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("scenario_status", &raw).map_err(tool_error)?;
        let args: JobArgs = validation::decode(raw).map_err(tool_error)?;
        self.extension("scenario_status", args).await
    }
    #[tool(
        name = "terminal_scenario_cancel",
        input_schema = rmcp::handler::server::common::schema_for_input::<JobArgs>().expect("valid shared input schema"),
        description = "Cancel scenario and perform configured cleanup. Requires job_id."
    )]
    async fn scenario_cancel(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("scenario_cancel", &raw).map_err(tool_error)?;
        let args: JobArgs = validation::decode(raw).map_err(tool_error)?;
        self.extension("scenario_cancel", args).await
    }
    #[tool(
        name = "terminal_cleanup",
        input_schema = rmcp::handler::server::common::schema_for_input::<CleanupArgs>().expect("valid shared input schema"),
        description = "Preview managed age/count/byte retention; apply requires exact preview plan. Protects live sessions, viewers and profiles; unknown legacy folders are inventory only."
    )]
    async fn cleanup(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("cleanup", &raw).map_err(tool_error)?;
        let args: CleanupArgs = validation::decode(raw).map_err(tool_error)?;
        self.extension("cleanup", args).await
    }
    #[tool(
        name = "terminal_relaunch",
        input_schema = rmcp::handler::server::common::schema_for_input::<SessionArgs>().expect("valid shared input schema"),
        description = "Explicitly relaunch interrupted historical session from sanitized manifest; inline secrets must be provided through new launch/profile. session_id."
    )]
    async fn relaunch(
        &self,
        Parameters(raw): Parameters<Value>,
    ) -> Result<CallToolResult, ErrorData> {
        validation::operation("relaunch", &raw).map_err(tool_error)?;
        let args: SessionArgs = validation::decode(raw).map_err(tool_error)?;
        self.extension("relaunch", args).await
    }
}

impl EmulationMcp {
    async fn guarded(
        &self,
        session_id: String,
        context: Option<silicogen_terminal_runtime::protocol::ActionContext>,
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
    async fn extension<T: serde::Serialize>(
        &self,
        operation: &str,
        args: T,
    ) -> Result<CallToolResult, ErrorData> {
        self.call(RpcRequest::Agent {
            operation: operation.into(),
            params: serde_json::to_value(args).map_err(tool_error)?,
        })
        .await
    }
    async fn call(&self, request: RpcRequest) -> Result<CallToolResult, ErrorData> {
        self.client
            .request_v2(request)
            .await
            .map(tool_result)
            .map_err(tool_error)
    }
}

#[rmcp::tool_handler(router=Self::tool_router())]
impl rmcp::ServerHandler for EmulationMcp {
    fn get_info(&self) -> rmcp::model::ServerConfig {
        let mut info = rmcp::model::ServerConfig::default();
        info.capabilities = rmcp::model::ServerCapabilities::builder()
            .enable_tools()
            .build();
        info.instructions=Some("Terminal API contract revision 2. Requires a revision-2 daemon, never silently upgrades/restarts a live old daemon. Observe defaults to text (16384 JSON bytes), waits to summary (1024), cells to compact styles (4096). response.mode=full opts into up to 4 MiB. StructuredContent is authoritative for projected object payloads; the text block is a bounded presentation summary, not a duplicate full grid. MCP envelope/presentation overhead is at most 512 additional bytes. Coordinates are zero-based row,column. Input acceptance is not rendering acknowledgement.".into());
        info
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
    let presentation = if value["response"]["contract_revision"] == 2 {
        let mut summary = json!({"contract_revision":2,"mode":value["response"]["mode"]});
        for key in [
            "status",
            "matched",
            "frame_id",
            "passed",
            "changed_cell_count",
        ] {
            if !value[key].is_null() {
                summary[key] = value[key].clone();
            }
        }
        if !value["match_count"].is_null() {
            summary["match_count"] = value["match_count"].clone();
        }
        if !value["any_match"].is_null() {
            summary["any_match"] = value["any_match"].clone();
        }
        let matches = value["matches"]
            .as_array()
            .or_else(|| value["matches"]["matches"].as_array());
        if let Some(first) = matches.and_then(|matches| matches.first()) {
            summary["match"] = first.clone();
        }
        if value["matches"]["any_match"].is_boolean() {
            summary["any_match"] = value["matches"]["any_match"].clone();
            summary["match_count"] = value["matches"]["match_count"].clone();
            if let Some(first) = value["matches"]["matches"]
                .as_array()
                .and_then(|m| m.first())
            {
                summary["match"] = first.clone();
            }
        }
        summary.to_string()
    } else {
        value.to_string()
    };
    let mut result = CallToolResult::success(vec![ContentBlock::text(presentation)]);
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
    if let Some(validation) = error.downcast_ref::<validation::RequestError>() {
        let details = serde_json::to_value(validation).ok();
        if matches!(
            validation.code.as_str(),
            "invalid_params"
                | "baseline_unavailable"
                | "baseline_incompatible"
                | "wait_conflict"
                | "unsupported_response"
                | "response_budget_exceeded"
                | "sequence_forbidden"
                | "sequence_capacity"
                | "sequence_limit"
        ) {
            return ErrorData::invalid_params(validation.to_string(), details);
        }
        return ErrorData::internal_error(validation.to_string(), details);
    }
    if let Some(runtime) = error.downcast_ref::<silicogen_terminal_runtime::client::RuntimeError>()
    {
        if matches!(
            runtime.0.code.as_str(),
            "invalid_params"
                | "baseline_unavailable"
                | "baseline_incompatible"
                | "wait_conflict"
                | "unsupported_response"
                | "response_budget_exceeded"
                | "sequence_forbidden"
                | "sequence_capacity"
                | "sequence_limit"
        ) {
            return ErrorData::invalid_params(runtime.0.message.clone(), runtime.0.details.clone());
        }
        return ErrorData::internal_error(
            runtime.0.message.clone(),
            Some(json!({"code":runtime.0.code,"details":runtime.0.details})),
        );
    }
    ErrorData::internal_error(error.to_string(), None)
}

fn wait_input_schema() -> std::sync::Arc<serde_json::Map<String, Value>> {
    let mut schema = (*rmcp::handler::server::common::schema_for_input::<McpWaitArgs>()
        .expect("valid wait schema"))
    .clone();
    schema.insert("allOf".into(),json!([{
        "if":{"anyOf":[{"required":["predicate"],"properties":{"predicate":{"type":"object"}}},{"required":["wait_id"],"properties":{"wait_id":{"type":"string"}}},{"required":["kind"],"properties":{"kind":{"enum":["output_quiet","ready"]}}}]},
        "then":{"properties":{"timeout_ms":{"minimum":1,"maximum":3600000}}},
        "else":{"properties":{"timeout_ms":{"minimum":1,"maximum":60000}}}
    }]));
    std::sync::Arc::new(schema)
}
