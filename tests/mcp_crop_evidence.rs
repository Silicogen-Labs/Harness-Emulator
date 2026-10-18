use rmcp::{RoleClient, ServiceExt, model::CallToolRequestParams, transport::TokioChildProcess};
use serde_json::{Value, json};
use silicogen_terminal_runtime::artifacts::{
    ArtifactSourceKind, ArtifactStore, EvidenceFileRegistration,
};
use std::{path::Path, process::Command};

struct Harness {
    client: rmcp::service::RunningService<RoleClient, ()>,
    runtime: std::path::PathBuf,
    _temp: tempfile::TempDir,
}

impl Harness {
    async fn new() -> Self {
        let base = Path::new(env!("CARGO_MANIFEST_DIR")).join(".runtime/mcp-crop-evidence");
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
            _temp: temp,
        }
    }

    async fn call(&self, name: &str, args: Value) -> Value {
        let result = self
            .client
            .peer()
            .call_tool(
                CallToolRequestParams::new(name.to_owned())
                    .with_arguments(args.as_object().unwrap().clone()),
            )
            .await
            .unwrap_or_else(|error| panic!("MCP tool {name} failed: {error}"));
        assert_ne!(result.is_error, Some(true), "{result:?}");
        result.structured_content.unwrap_or_else(|| {
            result
                .content
                .iter()
                .find_map(|block| block.as_text())
                .and_then(|block| serde_json::from_str(&block.text).ok())
                .expect("tool returned neither structured content nor JSON text")
        })
    }

    async fn error(&self, name: &str, args: Value) -> rmcp::ErrorData {
        let error = self
            .client
            .peer()
            .call_tool(
                CallToolRequestParams::new(name.to_owned())
                    .with_arguments(args.as_object().unwrap().clone()),
            )
            .await
            .unwrap_err();
        let rmcp::service::ServiceError::McpError(error) = error else {
            panic!("expected MCP error, received {error:?}");
        };
        error
    }

    async fn fixture(&self) -> Value {
        let launch = self
            .call(
                "terminal_launch",
                json!({
                    "program": env!("CARGO_BIN_EXE_terminal-fixture"),
                    "cwd": self._temp.path(), "cols": 80, "rows": 24, "env_inherit": []
                }),
            )
            .await;
        let ready = self
            .call(
                "terminal_wait",
                json!({
                    "session_id": launch["session_id"], "kind": "contains",
                    "value": "TERMINAL AUTOMATION FIXTURE", "timeout_ms": 5000
                }),
            )
            .await;
        assert_eq!(ready["matched"], true);
        launch
    }
}

impl Drop for Harness {
    fn drop(&mut self) {
        let _ = Command::new(env!("CARGO_BIN_EXE_silicogen-emulation"))
            .args(["--runtime-dir", self.runtime.to_str().unwrap(), "shutdown"])
            .output();
    }
}

fn kill_test_daemon(runtime: &Path) {
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
        .expect("locate only this crop test's runtime daemon");
    assert_eq!(unsafe { libc::kill(pid, libc::SIGKILL) }, 0);
    for _ in 0..100 {
        if !Path::new(&format!("/proc/{pid}")).exists() {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    panic!("test-owned daemon {pid} did not exit after SIGKILL");
}

#[test]
fn paired_artifact_registration_does_not_publish_first_file_when_second_is_missing() {
    let temp = tempfile::tempdir().unwrap();
    let runtime = temp.path().join("runtime");
    let evidence = runtime.join("artifacts/session-crop-atomic/run-1");
    std::fs::create_dir_all(&evidence).unwrap();
    std::fs::write(evidence.join("crop.png"), b"png").unwrap();
    let store = ArtifactStore::new(runtime.clone()).unwrap();
    let root_id = store
        .register_session_evidence("session-crop-atomic", &evidence)
        .unwrap();

    let result = store.register_evidence_files_with_ids(
        "session-crop-atomic",
        &root_id,
        &[
            EvidenceFileRegistration {
                relative_path: Path::new("crop.png"),
                artifact_id: "crop-artifact-1",
                source_kind: ArtifactSourceKind::Screenshot,
                content_type: "image/png",
                run_id: Some("run-1".into()),
                retention: Some("session crop evidence".into()),
            },
            EvidenceFileRegistration {
                relative_path: Path::new("cropmeta.json"),
                artifact_id: "crop-metadata-1",
                source_kind: ArtifactSourceKind::Frame,
                content_type: "application/json",
                run_id: Some("run-1".into()),
                retention: Some("session crop metadata".into()),
            },
        ],
    );

    assert!(result.is_err(), "missing second file must reject the batch");
    assert!(store.list("session-crop-atomic", 0, 10).unwrap().is_empty());
    assert!(
        store
            .metadata("session-crop-atomic", "crop-artifact-1")
            .is_err()
    );
}

#[tokio::test]
async fn crop_tools_discover_and_crop_png_and_metadata_are_readable_artifacts() {
    let harness = Harness::new().await;
    let tools = harness.client.peer().list_all_tools().await.unwrap();
    for name in [
        "terminal_crop",
        "terminal_crop_compare",
        "terminal_read_artifact",
    ] {
        assert!(tools.iter().any(|tool| tool.name == name), "missing {name}");
    }

    let launch = harness.fixture().await;
    let session_id = launch["session_id"].as_str().unwrap();
    let observed = harness
        .call(
            "terminal_observe",
            json!({"session_id": session_id, "response":{"mode":"full"}}),
        )
        .await;
    let source = json!({
        "session_id": session_id,
        "run_id": observed["run_id"],
        "frame_id": observed["frame_id"],
        "observation_revision": observed["observation_revision"],
        "viewport_revision": observed["viewport_revision"],
        "cols": observed["cols"],
        "rows": observed["rows"]
    });
    let artifacts_before = harness
        .call(
            "terminal_artifacts",
            json!({"session_id":session_id,"limit":100}),
        )
        .await;
    let count_before = artifacts_before.as_array().unwrap().len();
    let bytes_before: u64 = artifacts_before
        .as_array()
        .unwrap()
        .iter()
        .map(|artifact| artifact["size_bytes"].as_u64().unwrap())
        .sum();
    let crop = harness
        .call(
            "terminal_crop",
            json!({
                "session_id": session_id,
                "source": source,
                "region":{"row_start":0,"col_start":0,"row_end":2,"col_end":4},
                "scale":2
            }),
        )
        .await;
    assert_eq!(crop["source"]["session_id"], session_id);
    assert_eq!(crop["source"]["run_id"], observed["run_id"]);
    assert_eq!(crop["source"]["frame_id"], observed["frame_id"]);
    assert_eq!(crop["width_px"], 80);
    assert_eq!(crop["height_px"], 80);
    assert_eq!(crop["scale"], 2);
    assert!(crop["artifact_id"].as_str().is_some());
    assert!(crop["metadata_artifact_id"].as_str().is_some());

    let observed_after_crop = harness
        .call(
            "terminal_observe",
            json!({"session_id":session_id,"response":{"mode":"full"}}),
        )
        .await;
    let mut normalized_observation = observed_after_crop;
    // Each observe call gets a fresh capture timestamp even when the terminal
    // frame is byte-for-byte unchanged; compare the complete frame contract.
    normalized_observation["captured_at_ms"] = observed["captured_at_ms"].clone();
    assert_eq!(
        normalized_observation, observed,
        "crop generation must not mutate any part of its source observation"
    );

    let artifacts_after = harness
        .call(
            "terminal_artifacts",
            json!({"session_id":session_id,"limit":100}),
        )
        .await;
    let listed_after = artifacts_after.as_array().unwrap();
    assert_eq!(listed_after.len(), count_before + 2);
    let bytes_after: u64 = listed_after
        .iter()
        .map(|artifact| artifact["size_bytes"].as_u64().unwrap())
        .sum();
    let registered_png = listed_after
        .iter()
        .find(|artifact| artifact["artifact_id"] == crop["artifact_id"])
        .unwrap();
    let registered_metadata = listed_after
        .iter()
        .find(|artifact| artifact["artifact_id"] == crop["metadata_artifact_id"])
        .unwrap();
    assert_eq!(registered_png["size_bytes"], crop["png_size_bytes"]);
    assert_eq!(
        bytes_after - bytes_before,
        crop["png_size_bytes"].as_u64().unwrap()
            + registered_metadata["size_bytes"].as_u64().unwrap()
    );

    let png = harness
        .call(
            "terminal_read_artifact",
            json!({"session_id":session_id,"artifact_id":crop["artifact_id"],"length":4096}),
        )
        .await;
    use base64::Engine as _;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(png["data"].as_str().unwrap())
        .unwrap();
    assert_eq!(&bytes[..8], b"\x89PNG\r\n\x1a\n");
    assert_eq!(u32::from_be_bytes(bytes[16..20].try_into().unwrap()), 80);
    assert_eq!(u32::from_be_bytes(bytes[20..24].try_into().unwrap()), 80);

    let metadata = harness
        .call(
            "terminal_read_artifact",
            json!({"session_id":session_id,"artifact_id":crop["metadata_artifact_id"],"length":65536}),
        )
        .await;
    let metadata_bytes = base64::engine::general_purpose::STANDARD
        .decode(metadata["data"].as_str().unwrap())
        .unwrap();
    let persisted: Value = serde_json::from_slice(&metadata_bytes).unwrap();
    assert_eq!(persisted["artifact_id"], crop["artifact_id"]);
    assert_eq!(
        persisted["metadata_artifact_id"],
        crop["metadata_artifact_id"]
    );
    assert_eq!(persisted["metadata"]["source"], source);

    let matching_crop = harness
        .call(
            "terminal_crop",
            json!({
                "session_id":session_id,
                "region":{"row_start":0,"col_start":0,"row_end":2,"col_end":4},
                "scale":2
            }),
        )
        .await;
    let comparison = harness
        .call(
            "terminal_crop_compare",
            json!({
                "session_id":session_id,
                "left_artifact_id":crop["artifact_id"],
                "right_artifact_id":matching_crop["artifact_id"],
                "tolerance":{"max_channel_delta":0,"max_different_pixels":0}
            }),
        )
        .await;
    assert_eq!(comparison["comparison"]["matched"], true);
    assert_eq!(comparison["supplemental_to"], "terminal_diff");

    let different_region = harness
        .call(
            "terminal_crop",
            json!({
                "session_id":session_id,
                "region":{"row_start":0,"col_start":1,"row_end":2,"col_end":5},
                "scale":2
            }),
        )
        .await;
    let incompatible = harness
        .error(
            "terminal_crop_compare",
            json!({
                "session_id":session_id,
                "left_artifact_id":crop["artifact_id"],
                "right_artifact_id":different_region["artifact_id"],
                "tolerance":{"max_channel_delta":0,"max_different_pixels":0}
            }),
        )
        .await;
    assert!(incompatible.message.contains("incompatible"));

    let other = harness.fixture().await;
    let other_crop = harness
        .call(
            "terminal_crop",
            json!({
                "session_id":other["session_id"],
                "region":{"row_start":0,"col_start":0,"row_end":2,"col_end":4},
                "scale":2
            }),
        )
        .await;
    let cross_session = harness
        .error(
            "terminal_crop_compare",
            json!({
                "session_id":other["session_id"],
                "left_artifact_id":crop["artifact_id"],
                "right_artifact_id":other_crop["artifact_id"],
                "tolerance":{"max_channel_delta":0,"max_different_pixels":0}
            }),
        )
        .await;
    assert!(cross_session.message.contains("different session"));
    let cross_session_read = harness
        .error(
            "terminal_read_artifact",
            json!({"session_id":other["session_id"],"artifact_id":crop["artifact_id"]}),
        )
        .await;
    assert!(cross_session_read.message.contains("different session"));

    kill_test_daemon(&harness.runtime);
    let listed = harness
        .call(
            "terminal_artifacts",
            json!({"session_id":session_id,"limit":100}),
        )
        .await;
    let list = listed.as_array().unwrap();
    assert!(
        list.iter()
            .any(|item| item["artifact_id"] == crop["artifact_id"])
    );
    assert!(
        list.iter()
            .any(|item| item["artifact_id"] == crop["metadata_artifact_id"])
    );
    let recovered_png = harness
        .call(
            "terminal_read_artifact",
            json!({"session_id":session_id,"artifact_id":crop["artifact_id"],"length":4096}),
        )
        .await;
    let recovered_bytes = base64::engine::general_purpose::STANDARD
        .decode(recovered_png["data"].as_str().unwrap())
        .unwrap();
    assert_eq!(&recovered_bytes[..8], b"\x89PNG\r\n\x1a\n");
}

#[tokio::test]
async fn crop_quota_rejection_leaves_no_registered_or_partial_artifacts() {
    let harness = Harness::new().await;
    let launch = harness
        .call(
            "terminal_launch",
            json!({
                "program": env!("CARGO_BIN_EXE_terminal-fixture"),
                "cwd": harness._temp.path(), "cols": 80, "rows": 24, "env_inherit": [],
                "options": {"metadata":{"artifact_max_count":"1"}}
            }),
        )
        .await;
    let session_id = launch["session_id"].as_str().unwrap();
    let ready = harness
        .call(
            "terminal_wait",
            json!({
                "session_id":session_id,"kind":"contains",
                "value":"TERMINAL AUTOMATION FIXTURE","timeout_ms":5000
            }),
        )
        .await;
    assert_eq!(ready["matched"], true);

    let error = harness
        .error(
            "terminal_crop",
            json!({
                "session_id":session_id,
                "region":{"row_start":0,"col_start":0,"row_end":2,"col_end":4},
                "scale":2
            }),
        )
        .await;
    assert!(error.message.contains("artifact_quota_exceeded"));
    let artifacts = harness
        .call(
            "terminal_artifacts",
            json!({"session_id":session_id,"limit":100}),
        )
        .await;
    assert!(artifacts.as_array().unwrap().is_empty());
    let session_artifacts = harness.runtime.join("artifacts").join(session_id);
    assert!(
        !session_artifacts.exists()
            || std::fs::read_dir(&session_artifacts)
                .unwrap()
                .next()
                .is_none(),
        "quota rejection left files under the session artifact root"
    );
}

#[tokio::test]
async fn crop_comparison_rejects_artifacts_from_different_runs_after_restart() {
    let harness = Harness::new().await;
    let launch = harness.fixture().await;
    let session_id = launch["session_id"].as_str().unwrap();
    let region = json!({"row_start":0,"col_start":0,"row_end":2,"col_end":4});
    let first = harness
        .call(
            "terminal_crop",
            json!({"session_id":session_id,"region":region,"scale":1}),
        )
        .await;
    let restarted = harness
        .call("terminal_restart", json!({"session_id":session_id}))
        .await;
    assert_ne!(restarted["run_id"], first["source"]["run_id"]);
    let ready = harness
        .call(
            "terminal_wait",
            json!({
                "session_id":session_id,"kind":"contains",
                "value":"TERMINAL AUTOMATION FIXTURE","timeout_ms":5000
            }),
        )
        .await;
    assert_eq!(ready["matched"], true);
    let second = harness
        .call(
            "terminal_crop",
            json!({"session_id":session_id,"region":region,"scale":1}),
        )
        .await;
    assert_eq!(second["source"]["run_id"], restarted["run_id"]);
    let error = harness
        .error(
            "terminal_crop_compare",
            json!({
                "session_id":session_id,
                "left_artifact_id":first["artifact_id"],
                "right_artifact_id":second["artifact_id"],
                "tolerance":{"max_channel_delta":0,"max_different_pixels":0}
            }),
        )
        .await;
    assert!(error.message.contains("runs differ"), "{}", error.message);
}

#[tokio::test]
async fn crop_rejects_stale_observations_and_invalid_or_oversized_requests() {
    let harness = Harness::new().await;
    let launch = harness.fixture().await;
    let session_id = launch["session_id"].as_str().unwrap();
    let observed = harness
        .call(
            "terminal_observe",
            json!({"session_id":session_id,"response":{"mode":"full"}}),
        )
        .await;
    let source = json!({
        "session_id": session_id,
        "run_id": observed["run_id"],
        "frame_id": observed["frame_id"],
        "observation_revision": observed["observation_revision"],
        "viewport_revision": observed["viewport_revision"],
        "cols": observed["cols"], "rows": observed["rows"]
    });
    harness
        .call(
            "terminal_resize",
            json!({
                "session_id":session_id,
                "cols":observed["cols"].as_u64().unwrap() + 1,
                "rows":observed["rows"]
            }),
        )
        .await;
    let stale = harness
        .error(
            "terminal_crop",
            json!({
                "session_id":session_id,"source":source,
                "region":{"row_start":0,"col_start":0,"row_end":1,"col_end":2},"scale":1
            }),
        )
        .await;
    assert!(
        stale.message.contains("stale_observation"),
        "{}",
        stale.message
    );

    let invalid_scale = harness
        .error(
            "terminal_crop",
            json!({"session_id":session_id,"region":{"row_start":0,"col_start":0,"row_end":1,"col_end":1},"scale":5}),
        )
        .await;
    assert_eq!(serde_json::to_value(invalid_scale.code).unwrap(), -32602);
    assert_eq!(invalid_scale.data.unwrap()["field"], "scale");

    let outside_viewport = harness
        .error(
            "terminal_crop",
            json!({
                "session_id":session_id,
                "region":{
                    "row_start":0,
                "col_start":observed["cols"].as_u64().unwrap() + 1,
                    "row_end":1,
                "col_end":observed["cols"].as_u64().unwrap() + 2
                },
                "scale":1
            }),
        )
        .await;
    assert!(outside_viewport.message.contains("region"));
}
