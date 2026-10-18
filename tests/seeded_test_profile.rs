use serde_json::{Value, json};
use silicogen_terminal_runtime::{
    config::LaunchOptions,
    profiles::{self, ProfileEnvValue},
    protocol::{LaunchSpec, RpcRequest, WaitPredicate},
    retention::{ManagedEntry, OwnerKind, RetentionClass, RetentionPolicy, RetentionRegistry},
    service::SessionService,
};
use std::{
    collections::BTreeMap,
    fs,
    path::{Path, PathBuf},
    sync::Mutex,
};

static TEST_ENVIRONMENT_MUTEX: Mutex<()> = Mutex::new(());

struct RestoreEnvironment {
    name: &'static str,
    previous: Option<std::ffi::OsString>,
}

impl RestoreEnvironment {
    fn set(name: &'static str, value: &str) -> Self {
        let previous = std::env::var_os(name);
        // This unique synthetic variable is read only by this integration test.
        unsafe { std::env::set_var(name, value) };
        Self { name, previous }
    }
}

impl Drop for RestoreEnvironment {
    fn drop(&mut self) {
        // Restore the daemon test process environment even when an assertion fails.
        unsafe {
            if let Some(previous) = self.previous.take() {
                std::env::set_var(self.name, previous);
            } else {
                std::env::remove_var(self.name);
            }
        }
    }
}

fn temp() -> tempfile::TempDir {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join(".runtime/issue-1-tests");
    fs::create_dir_all(&root).unwrap();
    tempfile::tempdir_in(root).unwrap()
}

fn launch_spec(cwd: &Path, script: &str) -> LaunchSpec {
    LaunchSpec {
        name: Some("synthetic-safe-launch".into()),
        program: "/bin/sh".into(),
        args: vec!["-c".into(), script.into()],
        cwd: cwd.display().to_string(),
        env: BTreeMap::new(),
        cols: 100,
        rows: 24,
        record_output: false,
    }
}

fn options(source: &Path) -> LaunchOptions {
    serde_json::from_value(json!({
        "state_mode":"profile", "profile":"silicogen-test",
        "workspace":{"source":source,"mode":"reflink"}
    }))
    .unwrap()
}

fn store_template(runtime: &Path) {
    profiles::store_profile(runtime, &profiles::silicogen_test_profile_template()).unwrap();
}

fn agent(service: &SessionService, operation: &str, params: Value) -> anyhow::Result<Value> {
    Ok(service
        .handle(RpcRequest::Agent {
            operation: operation.into(),
            params,
        })?
        .0)
}

#[test]
fn successful_finalization_waits_for_evidence_lease_then_disposes_and_is_idempotent() {
    let tmp = temp();
    let source = tmp.path().join("project");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("rtl.v"), "original").unwrap();
    let root = tmp.path().join("runtime");
    store_template(&root);
    let service = SessionService::new(&root).unwrap();
    let launched = service
        .handle(RpcRequest::LaunchConfigured {
            spec: launch_spec(&source, "printf READY; sleep 60"),
            options: options(&source),
        })
        .unwrap()
        .0;
    let id = launched["session_id"].as_str().unwrap();
    let session = service.viewer_session(id).unwrap();
    let lease = session.retain_evidence().unwrap();
    let params = json!({"session_id":id,"expected_run_id":launched["run_id"],"outcome":"passed"});
    let first = agent(&service, "finalize", params.clone()).unwrap();
    assert_eq!(first["disposal"], "pending");
    assert!(root.join("sessions").join(id).join("state").exists());
    assert!(Path::new(launched["workspace"].as_str().unwrap()).exists());
    assert!(
        service
            .handle(RpcRequest::Restart {
                session_id: id.into()
            })
            .is_err()
    );
    drop(lease);
    let done = agent(&service, "finalize", params.clone()).unwrap();
    assert_eq!(done["disposal"], "disposed");
    assert!(!root.join("sessions").join(id).join("state").exists());
    assert!(!Path::new(launched["workspace"].as_str().unwrap()).exists());
    let repeated = agent(&service, "finalize", params.clone()).unwrap();
    assert_eq!(repeated, done);
    service.shutdown().unwrap();
    drop(service);
    let reloaded = SessionService::new(&root).unwrap();
    assert_eq!(agent(&reloaded, "finalize", params).unwrap(), done);
    assert_eq!(
        fs::read_to_string(source.join("rtl.v")).unwrap(),
        "original"
    );
}

#[test]
fn close_never_infers_pass_and_unknown_finalization_retains_state() {
    let tmp = temp();
    let root = tmp.path().join("runtime");
    let service = SessionService::new(&root).unwrap();
    let launched = service
        .handle(RpcRequest::LaunchConfigured {
            spec: launch_spec(tmp.path(), "exit 0"),
            options: LaunchOptions {
                disposable: true,
                ..Default::default()
            },
        })
        .unwrap()
        .0;
    let id = launched["session_id"].as_str().unwrap();
    service
        .handle(RpcRequest::Close {
            session_id: id.into(),
        })
        .unwrap();
    let registry = silicogen_terminal_runtime::retention::RetentionRegistry::open(&root).unwrap();
    assert!(registry.entries().iter().all(|entry| entry.retention
        != silicogen_terminal_runtime::retention::RetentionClass::Successful));
    let result = agent(
        &service,
        "finalize",
        json!({"session_id":id,"expected_run_id":launched["run_id"],"outcome":"unknown"}),
    )
    .unwrap();
    assert_eq!(result["disposal"], "retained");
    assert!(root.join("sessions").join(id).join("state").is_dir());
}

#[test]
fn finalization_rejects_stale_run_and_conflicting_outcome() {
    let tmp = temp();
    let root = tmp.path().join("runtime");
    let service = SessionService::new(&root).unwrap();
    let launched = service
        .handle(RpcRequest::LaunchConfigured {
            spec: launch_spec(tmp.path(), "sleep 60"),
            options: LaunchOptions::default(),
        })
        .unwrap()
        .0;
    let id = launched["session_id"].as_str().unwrap();
    assert!(
        agent(
            &service,
            "finalize",
            json!({"session_id":id,"expected_run_id":"wrong-run","outcome":"passed"})
        )
        .is_err()
    );
    assert_eq!(
        service
            .handle(RpcRequest::Status {
                session_id: id.into()
            })
            .unwrap()
            .0["state"],
        "running"
    );
    let session = service.viewer_session(id).unwrap();
    session.claim_actor("authorized-owner", false).unwrap();
    assert!(agent(
        &service,
        "finalize",
        json!({"session_id":id,"expected_run_id":launched["run_id"],"outcome":"failed","actor_id":"unauthorized-actor"})
    )
    .is_err());
    assert_eq!(session.summary().state, "running");
    session.claim_actor("authorized-owner", true).unwrap();
    let params = json!({"session_id":id,"expected_run_id":launched["run_id"],"outcome":"failed"});
    let first = agent(&service, "finalize", params.clone()).unwrap();
    assert_eq!(first["outcome"], "failed");
    assert_eq!(agent(&service, "finalize", params).unwrap(), first);
    assert!(
        agent(
            &service,
            "finalize",
            json!({"session_id":id,"expected_run_id":launched["run_id"],"outcome":"passed"})
        )
        .is_err()
    );
}

#[test]
fn template_declares_required_isolation_and_outcome_disposal() {
    let template = serde_json::to_value(profiles::silicogen_test_profile_template()).unwrap();
    assert_eq!(
        template["test_policy"]["isolation"]["backend"],
        "bubblewrap"
    );
    assert_eq!(template["test_policy"]["isolation"]["network"], "none");
    assert_eq!(template["test_policy"]["dispose_on_pass"], true);
}

#[test]
fn failed_cancelled_and_interrupted_runs_retain_copies_with_bounded_sanitized_evidence() {
    for outcome in ["failed", "cancelled", "interrupted"] {
        let tmp = temp();
        let root = tmp.path().join("runtime");
        let source = tmp.path().join("project");
        let auth = tmp.path().join("auth.json");
        fs::create_dir(&source).unwrap();
        fs::write(source.join("rtl.v"), "original").unwrap();
        fs::write(&auth, r#"{"token":"synthetic-do-not-record"}"#).unwrap();
        let mut profile = profiles::silicogen_test_profile_template();
        profile
            .seeds
            .push(silicogen_terminal_runtime::config::StateSeed {
                source: auth.clone(),
                destination: "data/silicogen/auth.json".into(),
                include_credentials: true,
            });
        profiles::store_profile(&root, &profile).unwrap();
        let service = SessionService::new(&root).unwrap();
        let launched = service.handle(RpcRequest::LaunchConfigured {
            spec:launch_spec(&source, "printf changed > rtl.v; mkdir -p \"$XDG_DATA_HOME/silicogen/log\"; printf 'token=synthetic-do-not-record' > \"$XDG_DATA_HOME/silicogen/log/dev.log\"; printf 'READY synthetic-do-not-record'; sleep 60"),
            options:options(&source),
        }).unwrap().0;
        let id = launched["session_id"].as_str().unwrap();
        let waited = service
            .handle(RpcRequest::Wait {
                session_id: id.into(),
                predicate: WaitPredicate::TextContains {
                    text: "READY".into(),
                },
                timeout_ms: 5000,
            })
            .unwrap()
            .0;
        assert_eq!(waited["matched"], true);
        let mut expected_frame = service.viewer_session(id).unwrap().observe();
        let mut redactor = silicogen_terminal_runtime::finalization::Redactor::default();
        redactor.add("synthetic-do-not-record");
        redactor.frame(&mut expected_frame);
        let expected_png = silicogen_terminal_runtime::screenshot::render_png(
            &expected_frame,
            &tmp.path().join("expected-sanitized.png"),
            "expected".into(),
        )
        .unwrap();
        agent(
            &service,
            "assert",
            json!({"session_id":id,"assertion":{"kind":"text_contains","text":"READY"}}),
        )
        .unwrap();
        let result = agent(
            &service,
            "finalize",
            json!({"session_id":id,"expected_run_id":launched["run_id"],"outcome":outcome}),
        )
        .unwrap();
        assert_eq!(result["disposal"], "retained", "{result}");
        let state = root.join("sessions").join(id).join("state");
        assert!(state.is_dir());
        assert!(!state.join("data/silicogen/auth.json").exists());
        assert!(Path::new(launched["workspace"].as_str().unwrap()).exists());
        let evidence = agent(
            &service,
            "read_artifact",
            json!({"session_id":id,"artifact_id":result["evidence"]["artifact_id"]}),
        )
        .unwrap();
        let text = evidence["text"].as_str().unwrap();
        assert!(
            !text.contains("synthetic-do-not-record"),
            "known credentials leaked"
        );
        let bundle: Value = serde_json::from_str(text).unwrap();
        assert_eq!(bundle["raw_recording_enabled"], false);
        assert_eq!(bundle["raw_bytes_in_bundle"], false);
        assert_eq!(bundle["assertions"][0]["result"]["passed"], true);
        assert_eq!(
            bundle["assertions"][0]["result"]["frame_id"],
            bundle["frame_id"]
        );
        assert!(bundle["changed_files"]["files"]["rtl.v"].is_string());
        assert_eq!(bundle["log_tail_available"], true);
        assert!(text.len() < 1024 * 1024);
        assert!(Path::new(result["evidence"]["screenshot"]["path"].as_str().unwrap()).is_file());
        assert_eq!(
            result["evidence"]["screenshot"]["sha256"], expected_png.sha256,
            "screenshot must render the masked frame, not the raw credential row"
        );
        assert_eq!(
            fs::read_to_string(source.join("rtl.v")).unwrap(),
            "original"
        );
        assert_eq!(
            fs::read_to_string(auth).unwrap(),
            r#"{"token":"synthetic-do-not-record"}"#
        );
        service.shutdown().unwrap();
        drop(service);
        let reloaded = SessionService::new(&root).unwrap();
        assert!(
            agent(
                &reloaded,
                "read_artifact",
                json!({"session_id":id,"artifact_id":result["evidence"]["artifact_id"]})
            )
            .is_ok()
        );
    }
}

#[test]
fn scenario_outcome_finalizes_only_the_sessions_it_launched() {
    let tmp = temp();
    let source = tmp.path().join("project");
    fs::create_dir(&source).unwrap();
    let root = tmp.path().join("runtime");
    store_template(&root);
    let service = SessionService::new(&root).unwrap();
    let scenario = json!({"version":1,"name":"safe-copy-pass","steps":[
        {"id":"launch","operation":"launch","params":{"spec":launch_spec(&source,"printf READY; sleep 60"),"options":options(&source)}},
        {"id":"ready","operation":"wait","params":{"session_id":"${session}","predicate":{"kind":"text_contains","text":"READY"}}}
    ]});
    let job = agent(&service, "scenario_run", json!({"scenario":scenario})).unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let report = agent(&service, "scenario_status", json!({"job_id":job["job_id"]})).unwrap();
        if report["status"] == "passed" {
            let id = report["session_id"].as_str().unwrap();
            let status = service
                .handle(RpcRequest::Status {
                    session_id: id.into(),
                })
                .unwrap()
                .0;
            if status["finalization"]["disposal"] == "disposed" {
                break;
            }
        }
        assert!(
            std::time::Instant::now() < deadline,
            "scenario finalization not completed: {report}"
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    service.shutdown().unwrap();
}

#[test]
fn supported_profile_requires_workspace_before_any_process_is_started() {
    let tmp = temp();
    let root = tmp.path().join("runtime");
    store_template(&root);
    let service = SessionService::new(&root).unwrap();
    let result = service.handle(RpcRequest::LaunchConfigured {
        spec: launch_spec(tmp.path(), "printf UNSAFE"),
        options: serde_json::from_value(json!({"state_mode":"profile","profile":"silicogen-test"}))
            .unwrap(),
    });
    service.shutdown().unwrap();
    assert!(result.is_err(), "required snapshot must not be optional");
    assert!(result.unwrap_err().to_string().contains("workspace"));
}

#[test]
fn test_profile_rejects_extra_ambient_inheritance_and_directory_seeds() {
    let tmp = temp();
    let root = tmp.path().join("runtime");
    store_template(&root);
    let service = SessionService::new(&root).unwrap();
    let project = tmp.path().join("project");
    fs::create_dir(&project).unwrap();
    let mut request = options(&project);
    request.env_inherit = Some(vec!["SSH_AUTH_SOCK".into()]);
    let result = service.handle(RpcRequest::LaunchConfigured {
        spec: launch_spec(&project, "printf UNSAFE"),
        options: request,
    });
    service.shutdown().unwrap();
    assert!(result.is_err(), "test profile must enforce its allowlist");

    let mut template = serde_json::to_value(profiles::silicogen_test_profile_template()).unwrap();
    template["seeds"] =
        json!([{"source":tmp.path(),"destination":"data/silicogen","include_credentials":true}]);
    let profile = serde_json::from_value(template).unwrap();
    assert!(
        profiles::store_profile(&root, &profile).is_err(),
        "test seeds must be selected files, not broad directory imports"
    );
}

#[test]
fn enforced_profile_rejects_database_log_cache_seeds_and_host_backend() {
    let tmp = temp();
    let root = tmp.path().join("runtime");
    for name in [
        "history.db",
        "history.sqlite-wal",
        "history.sqlite-shm",
        "dev.log",
    ] {
        let file = tmp.path().join(name);
        fs::write(&file, "do not import").unwrap();
        let mut profile = profiles::silicogen_test_profile_template();
        profile
            .seeds
            .push(silicogen_terminal_runtime::config::StateSeed {
                source: file,
                destination: Path::new("data/silicogen").join(name),
                include_credentials: false,
            });
        assert!(profiles::store_profile(&root, &profile).is_err());
    }
    let settings = tmp.path().join("settings.json");
    fs::write(&settings, "{}").unwrap();
    let mut profile = profiles::silicogen_test_profile_template();
    profile
        .seeds
        .push(silicogen_terminal_runtime::config::StateSeed {
            source: settings,
            destination: "cache/settings.json".into(),
            include_credentials: false,
        });
    assert!(profiles::store_profile(&root, &profile).is_err());
    profile.seeds.clear();
    profile.test_policy.as_mut().unwrap().isolation.backend =
        silicogen_terminal_runtime::isolation::IsolationBackend::Host;
    assert!(profiles::store_profile(&root, &profile).is_err());
}

#[test]
fn startup_reclassifies_legacy_inferred_success_as_unknown_not_passed() {
    use silicogen_terminal_runtime::retention::{
        ManagedEntry, OwnerKind, RetentionClass, RetentionRegistry,
    };
    let tmp = temp();
    let root = tmp.path().join("runtime");
    fs::create_dir_all(root.join("sessions/legacy-session/state")).unwrap();
    let mut registry = RetentionRegistry::open(&root).unwrap();
    registry
        .register(ManagedEntry::new(
            "sessions/legacy-session",
            OwnerKind::Session,
            RetentionClass::Successful,
            1,
            "legacy inferred success",
        ))
        .unwrap();
    drop(registry);
    let service = SessionService::new(&root).unwrap();
    let registry = RetentionRegistry::open(&root).unwrap();
    assert_eq!(registry.entries()[0].retention, RetentionClass::Failed);
    assert!(root.join("sessions/legacy-session/state").is_dir());
    assert!(
        !root
            .join("sessions/legacy-session/finalization.json")
            .exists()
    );
    service.shutdown().unwrap();
}

#[test]
fn failed_capture_reports_failed_disposal_without_removing_copies() {
    let tmp = temp();
    let source = tmp.path().join("project");
    fs::create_dir(&source).unwrap();
    let root = tmp.path().join("runtime");
    store_template(&root);
    let service = SessionService::new(&root).unwrap();
    let mut config = options(&source);
    config
        .metadata
        .insert("artifact_max_count".into(), "0".into());
    let launched = service
        .handle(RpcRequest::LaunchConfigured {
            spec: launch_spec(&source, "sleep 60"),
            options: config,
        })
        .unwrap()
        .0;
    let id = launched["session_id"].as_str().unwrap();
    let result = agent(
        &service,
        "finalize",
        json!({"session_id":id,"expected_run_id":launched["run_id"],"outcome":"passed"}),
    )
    .unwrap();
    assert_eq!(result["disposal"], "failed");
    assert!(Path::new(launched["workspace"].as_str().unwrap()).is_dir());
    assert!(root.join("sessions").join(id).join("state").is_dir());
    service.shutdown().unwrap();
}

#[test]
fn enforced_profile_rejects_sandbox_launcher_loader_injection_and_escaping_cwd() {
    let tmp = temp();
    let source = tmp.path().join("project");
    fs::create_dir(&source).unwrap();
    let root = tmp.path().join("runtime");
    store_template(&root);
    let service = SessionService::new(&root).unwrap();
    let mut spec = launch_spec(&source, "printf UNSAFE");
    spec.env
        .insert("LD_PRELOAD".into(), "synthetic-untrusted-library.so".into());
    let result = service.handle(RpcRequest::LaunchConfigured {
        spec,
        options: options(&source),
    });
    service.shutdown().unwrap();
    assert!(
        result.is_err(),
        "loader variables execute before the sandbox is established"
    );
    let mut spec = launch_spec(&source, "printf UNSAFE");
    spec.cwd = "${workspace}/../..".into();
    let result = service.handle(RpcRequest::LaunchConfigured {
        spec,
        options: options(&source),
    });
    service.shutdown().unwrap();
    assert!(result.is_err(), "testing cwd must stay inside its copy");
}

#[test]
fn profile_selection_is_frozen_before_admission_and_not_serialized() {
    let tmp = temp();
    let source = tmp.path().join("project");
    fs::create_dir(&source).unwrap();
    let root = tmp.path().join("runtime");
    store_template(&root);
    let mut spec = launch_spec(&source, "printf FROZEN_READY; sleep 60");
    let mut config = options(&source);
    silicogen_terminal_runtime::launch::resolve_options(&root, &mut spec, &mut config).unwrap();
    assert!(config.resolved_profile.is_some());
    assert!(
        serde_json::to_value(&config)
            .unwrap()
            .get("resolved_profile")
            .is_none()
    );
    fs::remove_file(profiles::profile_path(&root, "silicogen-test").unwrap()).unwrap();
    let session =
        silicogen_terminal_runtime::session::Session::launch_configured(&root, spec, config)
            .unwrap();
    assert_eq!(
        session.capabilities()["launch"]["isolation"]["effective_backend"],
        "bubblewrap"
    );
    assert_eq!(
        session.capabilities()["launch"]["test_policy"]["dispose_on_pass"],
        true
    );
    session.close_admin().unwrap();
}

#[test]
fn explicit_secret_profile_reference_and_full_assertion_report_are_sanitized() {
    const ENV_NAME: &str = "SILICOGEN_SYNTHETIC_FINAL_EVIDENCE_SECRET";
    const SECRET: &str = "synthetic-final-evidence-secret-7f31";
    let _environment_lock = TEST_ENVIRONMENT_MUTEX.lock().unwrap();
    let _restore = RestoreEnvironment::set(ENV_NAME, SECRET);
    let tmp = temp();
    let source = tmp.path().join("project");
    fs::create_dir(&source).unwrap();
    let root = tmp.path().join("runtime");
    let mut profile = profiles::silicogen_test_profile_template();
    profile.env.insert(
        "SESSION_AUTH".into(),
        ProfileEnvValue::HostReference {
            variable: ENV_NAME.into(),
            secret: true,
        },
    );
    profiles::store_profile(&root, &profile).unwrap();
    let service = SessionService::new(&root).unwrap();
    let launched = service
        .handle(RpcRequest::LaunchConfigured {
            spec: launch_spec(&source, "printf 'READY:%s' \"$SESSION_AUTH\"; sleep 60"),
            options: options(&source),
        })
        .unwrap()
        .0;
    let session_id = launched["session_id"].as_str().unwrap();
    let wait = service
        .handle(RpcRequest::Wait {
            session_id: session_id.into(),
            predicate: WaitPredicate::TextContains {
                text: "READY".into(),
            },
            timeout_ms: 5_000,
        })
        .unwrap()
        .0;
    assert_eq!(wait["matched"], true);
    let assertion = agent(
        &service,
        "assert",
        json!({"session_id":session_id,"assertion":{"kind":"text_absent","text":SECRET}}),
    )
    .unwrap();
    assert_eq!(assertion["passed"], false);
    service
        .handle(RpcRequest::Close {
            session_id: session_id.into(),
        })
        .unwrap();
    let closed_evidence: Value = serde_json::from_slice(
        &fs::read(
            root.join("sessions")
                .join(session_id)
                .join("closed-evidence.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert!(!closed_evidence.to_string().contains(SECRET));
    service.shutdown().unwrap();
    drop(service);
    let reloaded = SessionService::new(&root).unwrap();
    let finalized = agent(
        &reloaded,
        "finalize",
        json!({"session_id":session_id,"expected_run_id":launched["run_id"],"outcome":"failed"}),
    )
    .unwrap();
    let artifact = agent(
        &reloaded,
        "read_artifact",
        json!({"session_id":session_id,"artifact_id":finalized["evidence"]["artifact_id"]}),
    )
    .unwrap();
    let text = artifact["text"].as_str().unwrap();
    assert!(
        !text.contains(SECRET),
        "declared secret leaked into final evidence"
    );
    let evidence: Value = serde_json::from_str(text).unwrap();
    let stored = &evidence["assertions"][0];
    assert_eq!(stored["assertion"]["kind"], "text_absent");
    assert_eq!(stored["assertion"]["text"], "<redacted>");
    assert_eq!(stored["result"]["passed"], false);
    assert_eq!(stored["result"]["failures"][0]["path"], "$");
    assert!(
        !stored["result"]["failures"][0]["message"]
            .as_str()
            .unwrap()
            .contains(SECRET)
    );
    assert_eq!(stored["frame_id"], evidence["frame_id"]);
    assert_eq!(stored["run_id"], evidence["run_id"]);
    reloaded.shutdown().unwrap();
}

#[test]
fn corrupt_assertion_store_rejects_append_and_makes_final_evidence_incomplete() {
    let tmp = temp();
    let source = tmp.path().join("project");
    fs::create_dir(&source).unwrap();
    let root = tmp.path().join("runtime");
    store_template(&root);
    let service = SessionService::new(&root).unwrap();
    let launched = service
        .handle(RpcRequest::LaunchConfigured {
            spec: launch_spec(&source, "printf READY; sleep 60"),
            options: options(&source),
        })
        .unwrap()
        .0;
    let session_id = launched["session_id"].as_str().unwrap();
    let assertion_path = root
        .join("sessions")
        .join(session_id)
        .join("assertions.json");
    let corrupt_bytes = b"{broken synthetic assertion report";
    fs::write(&assertion_path, corrupt_bytes).unwrap();
    let append = agent(
        &service,
        "assert",
        json!({"session_id":session_id,"assertion":{"kind":"text_contains","text":"READY"}}),
    );
    assert!(
        append
            .unwrap_err()
            .to_string()
            .contains("assertion_store_invalid")
    );
    assert_eq!(fs::read(&assertion_path).unwrap(), corrupt_bytes);

    let finalized = agent(
        &service,
        "finalize",
        json!({"session_id":session_id,"expected_run_id":launched["run_id"],"outcome":"passed"}),
    )
    .unwrap();
    assert_eq!(finalized["disposal"], "failed");
    assert_eq!(finalized["evidence"]["complete"], false);
    assert!(
        finalized["reason"]
            .as_str()
            .unwrap()
            .contains("evidence is incomplete")
    );
    assert!(
        root.join("sessions")
            .join(session_id)
            .join("state")
            .is_dir()
    );
    assert!(Path::new(launched["workspace"].as_str().unwrap()).is_dir());
    let artifact = agent(
        &service,
        "read_artifact",
        json!({"session_id":session_id,"artifact_id":finalized["evidence"]["artifact_id"]}),
    )
    .unwrap();
    let bundle: Value = serde_json::from_str(artifact["text"].as_str().unwrap()).unwrap();
    assert_eq!(bundle["assertions"], json!([]));
    assert_eq!(bundle["assertions_complete"], false);
    assert_eq!(bundle["complete"], false);
    assert!(
        !bundle
            .to_string()
            .contains("broken synthetic assertion report")
    );
    service.shutdown().unwrap();
}

#[test]
fn assertion_quota_rejects_append_without_silently_dropping_complete_history() {
    let tmp = temp();
    let source = tmp.path().join("project");
    fs::create_dir(&source).unwrap();
    let root = tmp.path().join("runtime");
    store_template(&root);
    let service = SessionService::new(&root).unwrap();
    let launched = service
        .handle(RpcRequest::LaunchConfigured {
            spec: launch_spec(&source, "printf READY; sleep 60"),
            options: options(&source),
        })
        .unwrap()
        .0;
    let session_id = launched["session_id"].as_str().unwrap();
    let run_id = launched["run_id"].as_str().unwrap();
    let stored = (0..64)
        .map(|index| {
            json!({
                "assertion":{"kind":"text_contains","text":"READY"},
                "result":{
                    "session_id":session_id,
                    "run_id":run_id,
                    "frame_id":format!("stored-frame-{index}"),
                    "passed":true,
                    "checked":1,
                    "failures":[]
                },
                "frame_id":format!("stored-frame-{index}"),
                "run_id":run_id
            })
        })
        .collect::<Vec<_>>();
    let path = root
        .join("sessions")
        .join(session_id)
        .join("assertions.json");
    let initial_bytes = serde_json::to_vec(&stored).unwrap();
    fs::write(&path, &initial_bytes).unwrap();

    let error = agent(
        &service,
        "assert",
        json!({"session_id":session_id,"assertion":{"kind":"text_contains","text":"READY"}}),
    )
    .unwrap_err();
    assert!(error.to_string().contains("assertion_quota_exceeded"));
    assert_eq!(fs::read(&path).unwrap(), initial_bytes);

    let finalization = agent(
        &service,
        "finalize",
        json!({"session_id":session_id,"expected_run_id":run_id,"outcome":"failed"}),
    )
    .unwrap();
    assert_eq!(finalization["disposal"], "retained");
    let artifact = agent(
        &service,
        "read_artifact",
        json!({"session_id":session_id,"artifact_id":finalization["evidence"]["artifact_id"]}),
    )
    .unwrap();
    let bundle: Value = serde_json::from_str(artifact["text"].as_str().unwrap()).unwrap();
    assert_eq!(bundle["assertions"].as_array().unwrap().len(), 64);
    assert_eq!(bundle["assertions_complete"], true);
    assert_eq!(bundle["complete"], true);
    service.shutdown().unwrap();
}

#[test]
fn corrupt_finalization_journal_stops_authorized_run_without_overwriting_verdict_bytes() {
    let tmp = temp();
    let source = tmp.path().join("project");
    fs::create_dir(&source).unwrap();
    let root = tmp.path().join("runtime");
    store_template(&root);
    let service = SessionService::new(&root).unwrap();
    let launched = service
        .handle(RpcRequest::LaunchConfigured {
            spec: launch_spec(&source, "printf READY; sleep 60"),
            options: options(&source),
        })
        .unwrap()
        .0;
    let session_id = launched["session_id"].as_str().unwrap();
    let journal = root
        .join("sessions")
        .join(session_id)
        .join("finalization.json");
    let corrupt_bytes = b"{corrupt but must not be replaced";
    fs::write(&journal, corrupt_bytes).unwrap();

    let error = agent(
        &service,
        "finalize",
        json!({"session_id":session_id,"expected_run_id":launched["run_id"],"outcome":"passed"}),
    )
    .unwrap_err();
    assert!(error.to_string().contains("journal is unreadable"));
    assert_eq!(fs::read(&journal).unwrap(), corrupt_bytes);
    let session = service.viewer_session(session_id).unwrap();
    assert_eq!(session.summary().state, "closed");
    assert!(
        root.join("sessions")
            .join(session_id)
            .join("state")
            .is_dir()
    );
    assert!(Path::new(launched["workspace"].as_str().unwrap()).is_dir());
    assert!(
        root.join("sessions")
            .join(session_id)
            .join("closed-evidence.json")
            .is_file()
    );
    let registry = RetentionRegistry::open(&root).unwrap();
    for category in ["sessions", "workspaces"] {
        let path = PathBuf::from(category).join(session_id);
        assert_eq!(
            registry
                .entries()
                .iter()
                .find(|entry| entry.relative_path == path)
                .unwrap()
                .retention,
            RetentionClass::Failed
        );
    }
    service.shutdown().unwrap();
}

#[test]
fn corrupt_launch_metadata_is_read_only_after_stop_and_cannot_trigger_disposal() {
    let tmp = temp();
    let source = tmp.path().join("project");
    fs::create_dir(&source).unwrap();
    let root = tmp.path().join("runtime");
    store_template(&root);
    let service = SessionService::new(&root).unwrap();
    let launched = service
        .handle(RpcRequest::LaunchConfigured {
            spec: launch_spec(&source, "printf READY; sleep 60"),
            options: options(&source),
        })
        .unwrap()
        .0;
    let session_id = launched["session_id"].as_str().unwrap();
    let manifest_path = root.join("sessions").join(session_id).join("launch.json");
    let corrupt_manifest = b"{corrupt launch metadata";
    fs::write(&manifest_path, corrupt_manifest).unwrap();

    let result = agent(
        &service,
        "finalize",
        json!({"session_id":session_id,"expected_run_id":launched["run_id"],"outcome":"passed"}),
    )
    .unwrap();
    assert_eq!(result["disposal"], "failed");
    assert_eq!(result["evidence"]["sanitized"], false);
    assert_eq!(result["evidence"]["complete"], false);
    assert_eq!(fs::read(&manifest_path).unwrap(), corrupt_manifest);
    assert_eq!(
        service.viewer_session(session_id).unwrap().summary().state,
        "closed"
    );
    assert!(
        root.join("sessions")
            .join(session_id)
            .join("state")
            .is_dir()
    );
    assert!(Path::new(launched["workspace"].as_str().unwrap()).is_dir());
    service.shutdown().unwrap();
}

#[test]
fn corrupt_launch_manifest_stops_workload_before_metadata_failure_and_retains_copy() {
    let tmp = temp();
    let source = tmp.path().join("project");
    fs::create_dir(&source).unwrap();
    let root = tmp.path().join("runtime");
    store_template(&root);
    let service = SessionService::new(&root).unwrap();
    let launched = service
        .handle(RpcRequest::LaunchConfigured {
            spec: launch_spec(&source, "printf READY; sleep 60"),
            options: options(&source),
        })
        .unwrap()
        .0;
    let session_id = launched["session_id"].as_str().unwrap();
    fs::write(
        root.join("sessions").join(session_id).join("launch.json"),
        "{corrupt launch metadata",
    )
    .unwrap();

    let result = agent(
        &service,
        "finalize",
        json!({"session_id":session_id,"expected_run_id":launched["run_id"],"outcome":"passed"}),
    )
    .unwrap();
    assert_eq!(result["disposal"], "failed");
    assert_eq!(result["evidence"]["sanitized"], false);
    assert_eq!(result["evidence"]["complete"], false);
    assert_eq!(
        service.viewer_session(session_id).unwrap().summary().state,
        "closed"
    );
    assert!(
        root.join("sessions")
            .join(session_id)
            .join("state")
            .is_dir()
    );
    assert!(Path::new(launched["workspace"].as_str().unwrap()).is_dir());
    service.shutdown().unwrap();
}

#[test]
fn protected_registered_workspace_descendant_blocks_all_disposal_before_unlink() {
    let tmp = temp();
    let source = tmp.path().join("project");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("rtl.v"), "original").unwrap();
    let root = tmp.path().join("runtime");
    store_template(&root);
    let service = SessionService::new(&root).unwrap();
    let launched = service
        .handle(RpcRequest::LaunchConfigured {
            spec: launch_spec(&source, "printf READY; sleep 60"),
            options: options(&source),
        })
        .unwrap()
        .0;
    let session_id = launched["session_id"].as_str().unwrap();
    let workspace = PathBuf::from(launched["workspace"].as_str().unwrap());
    let protected_file = workspace.join("registered-evidence/pinned.txt");
    fs::create_dir_all(protected_file.parent().unwrap()).unwrap();
    fs::write(&protected_file, "synthetic pinned evidence").unwrap();
    let relative = PathBuf::from("workspaces")
        .join(session_id)
        .join("registered-evidence/pinned.txt");
    let mut registry = RetentionRegistry::open(&root).unwrap();
    let mut pinned = ManagedEntry::new(
        &relative,
        OwnerKind::Artifact,
        RetentionClass::Interactive,
        1,
        "synthetic pinned evidence descendant",
    );
    pinned.pinned = true;
    registry.register(pinned).unwrap();
    let preview = registry.preview(2, &[], false).unwrap();
    assert!(
        preview
            .protected
            .iter()
            .any(|entry| entry.path == root.join(&relative))
    );
    drop(registry);

    let result = agent(
        &service,
        "finalize",
        json!({"session_id":session_id,"expected_run_id":launched["run_id"],"outcome":"passed"}),
    )
    .unwrap();
    assert_ne!(
        result["disposal"], "disposed",
        "protected descendant was unlinked"
    );
    assert_eq!(
        fs::read_to_string(protected_file).unwrap(),
        "synthetic pinned evidence"
    );
    assert!(
        root.join("sessions")
            .join(session_id)
            .join("state")
            .is_dir()
    );
    assert!(workspace.is_dir());
    service.shutdown().unwrap();
}

#[test]
fn outcome_retention_preserves_explicit_pins_on_each_owner_root() {
    for category in ["sessions", "artifacts", "workspaces"] {
        let tmp = temp();
        let source = tmp.path().join("project");
        fs::create_dir(&source).unwrap();
        let root = tmp.path().join("runtime");
        store_template(&root);
        let service = SessionService::new(&root).unwrap();
        let launched = service
            .handle(RpcRequest::LaunchConfigured {
                spec: launch_spec(&source, "printf READY; sleep 60"),
                options: options(&source),
            })
            .unwrap()
            .0;
        let session_id = launched["session_id"].as_str().unwrap();
        let owner_path = PathBuf::from(category).join(session_id);
        let mut registry = RetentionRegistry::open(&root).unwrap();
        let owner_id = registry
            .entries()
            .iter()
            .find(|entry| entry.relative_path == owner_path)
            .unwrap()
            .id
            .clone();
        registry
            .set_retention_at(&owner_id, RetentionClass::Interactive, true, 1)
            .unwrap();

        let result = agent(
            &service,
            "finalize",
            json!({"session_id":session_id,"expected_run_id":launched["run_id"],"outcome":"passed"}),
        )
        .unwrap();
        let registry = RetentionRegistry::open(&root).unwrap();
        let owner = registry
            .entries()
            .iter()
            .find(|entry| entry.relative_path == owner_path)
            .unwrap();
        assert!(owner.pinned, "outcome assignment cleared {category} pin");
        if category == "artifacts" {
            assert_eq!(result["disposal"], "disposed");
            assert_eq!(owner.retention, RetentionClass::Successful);
            assert!(
                !root
                    .join("sessions")
                    .join(session_id)
                    .join("state")
                    .exists()
            );
            assert!(!Path::new(launched["workspace"].as_str().unwrap()).exists());
        } else {
            assert_eq!(result["disposal"], "failed");
            assert_eq!(owner.retention, RetentionClass::Failed);
            assert!(
                root.join("sessions")
                    .join(session_id)
                    .join("state")
                    .is_dir()
            );
            assert!(Path::new(launched["workspace"].as_str().unwrap()).is_dir());
        }
        service.shutdown().unwrap();
    }
}

#[test]
fn one_mib_retention_quota_cannot_prevent_process_stop_and_outcome_journal() {
    let tmp = temp();
    let source = tmp.path().join("project");
    fs::create_dir(&source).unwrap();
    let root = tmp.path().join("runtime");
    fs::create_dir_all(&root).unwrap();
    let policy = RetentionPolicy {
        successful_ms: 0,
        failed_ms: 0,
        max_bytes: Some(1024 * 1024),
        max_entries: Some(100),
    };
    fs::write(
        root.join("retention-policy.json"),
        serde_json::to_vec(&policy).unwrap(),
    )
    .unwrap();
    store_template(&root);
    let service = SessionService::new(&root).unwrap();
    let launched = service
        .handle(RpcRequest::LaunchConfigured {
            spec: launch_spec(&source, "printf READY; sleep 60"),
            options: options(&source),
        })
        .unwrap()
        .0;
    let session_id = launched["session_id"].as_str().unwrap();
    let result = agent(
        &service,
        "finalize",
        json!({"session_id":session_id,"expected_run_id":launched["run_id"],"outcome":"failed"}),
    )
    .expect("quota pressure must return a durable finalization result");
    assert_eq!(result["disposal"], "failed");
    assert_eq!(result["evidence"]["complete"], false);
    let status = service
        .handle(RpcRequest::Status {
            session_id: session_id.into(),
        })
        .unwrap()
        .0;
    assert_eq!(status["state"], "closed", "owned process must be stopped");
    let journal: Value = serde_json::from_slice(
        &fs::read(
            root.join("sessions")
                .join(session_id)
                .join("finalization.json"),
        )
        .unwrap(),
    )
    .unwrap();
    assert_eq!(journal["outcome"], "failed");
    assert_eq!(journal["disposal"], "failed");
    service.shutdown().unwrap();
}

#[test]
fn failed_capture_reclassifies_session_workspace_and_artifacts_for_failure_retention() {
    let tmp = temp();
    let source = tmp.path().join("project");
    fs::create_dir(&source).unwrap();
    let root = tmp.path().join("runtime");
    fs::create_dir_all(&root).unwrap();
    let policy = RetentionPolicy {
        successful_ms: 0,
        failed_ms: 0,
        ..RetentionPolicy::default()
    };
    fs::write(
        root.join("retention-policy.json"),
        serde_json::to_vec(&policy).unwrap(),
    )
    .unwrap();
    store_template(&root);
    let service = SessionService::new(&root).unwrap();
    let mut config = options(&source);
    config
        .metadata
        .insert("artifact_max_count".into(), "0".into());
    let launched = service
        .handle(RpcRequest::LaunchConfigured {
            spec: launch_spec(&source, "sleep 60"),
            options: config,
        })
        .unwrap()
        .0;
    let session_id = launched["session_id"].as_str().unwrap();
    let result = agent(
        &service,
        "finalize",
        json!({"session_id":session_id,"expected_run_id":launched["run_id"],"outcome":"failed"}),
    )
    .unwrap();
    assert_eq!(result["disposal"], "failed");
    let mut registry = RetentionRegistry::open_with_policy(&root, policy.clone()).unwrap();
    for category in ["sessions", "artifacts", "workspaces"] {
        let path = PathBuf::from(category).join(session_id);
        let entry = registry
            .entries()
            .iter()
            .find(|entry| entry.relative_path == path)
            .unwrap_or_else(|| panic!("missing managed owner {path:?}"));
        assert_eq!(entry.retention, RetentionClass::Failed, "{path:?}");
    }
    let plan = registry.preview(u128::MAX / 2, &[], false).unwrap();
    for category in ["sessions", "workspaces"] {
        let path = root.join(category).join(session_id);
        assert!(
            plan.candidates
                .iter()
                .any(|candidate| candidate.path == path)
        );
    }
    service.shutdown().unwrap();
}

#[test]
fn clean_close_then_finalization_preserves_complete_final_capture() {
    let tmp = temp();
    let source = tmp.path().join("project");
    fs::create_dir(&source).unwrap();
    fs::write(source.join("rtl.v"), "original").unwrap();
    let root = tmp.path().join("runtime");
    store_template(&root);
    let service = SessionService::new(&root).unwrap();
    let launched = service
        .handle(RpcRequest::LaunchConfigured {
            spec: launch_spec(
                &source,
                "printf changed > rtl.v; mkdir -p \"$XDG_DATA_HOME/silicogen/log\"; printf 'synthetic close log entry' > \"$XDG_DATA_HOME/silicogen/log/dev.log\"; printf 'READY-CLOSE-ME'; sleep 60",
            ),
            options: options(&source),
        })
        .unwrap()
        .0;
    let session_id = launched["session_id"].as_str().unwrap();
    let wait = service
        .handle(RpcRequest::Wait {
            session_id: session_id.into(),
            predicate: WaitPredicate::TextContains {
                text: "READY-CLOSE-ME".into(),
            },
            timeout_ms: 5_000,
        })
        .unwrap()
        .0;
    assert_eq!(wait["matched"], true);
    let closed = service
        .handle(RpcRequest::Close {
            session_id: session_id.into(),
        })
        .unwrap()
        .0;
    assert_eq!(closed["state"], "closed");
    let closed_evidence_path = root
        .join("sessions")
        .join(session_id)
        .join("closed-evidence.json");
    assert!(closed_evidence_path.is_file());
    assert!(
        !root
            .join("sessions")
            .join(session_id)
            .join("finalization.json")
            .exists(),
        "close must not declare a verdict"
    );
    service.shutdown().unwrap();
    drop(service);
    let reloaded = SessionService::new(&root).unwrap();
    let finalized = agent(
        &reloaded,
        "finalize",
        json!({"session_id":session_id,"expected_run_id":launched["run_id"],"outcome":"passed"}),
    )
    .unwrap();
    assert_eq!(finalized["disposal"], "disposed");
    assert!(finalized["evidence"]["screenshot"]["path"].is_string());
    let artifact = agent(
        &reloaded,
        "read_artifact",
        json!({"session_id":session_id,"artifact_id":finalized["evidence"]["artifact_id"]}),
    )
    .unwrap();
    let evidence_text = artifact["text"].as_str().unwrap();
    let evidence: Value = serde_json::from_str(evidence_text).unwrap();
    assert_eq!(evidence["complete"], true);
    assert!(
        evidence["text"]
            .as_str()
            .unwrap()
            .contains("READY-CLOSE-ME")
    );
    assert!(evidence["changed_files"]["files"]["rtl.v"].is_string());
    assert!(
        evidence["log_tail"]
            .as_str()
            .unwrap()
            .contains("synthetic close log entry")
    );
    assert!(!Path::new(launched["workspace"].as_str().unwrap()).exists());
    reloaded.shutdown().unwrap();
}

#[test]
fn missing_bounded_close_capture_retains_copies_and_reports_failure() {
    let tmp = temp();
    let source = tmp.path().join("project");
    fs::create_dir(&source).unwrap();
    let root = tmp.path().join("runtime");
    store_template(&root);
    let service = SessionService::new(&root).unwrap();
    let launched = service
        .handle(RpcRequest::LaunchConfigured {
            spec: launch_spec(&source, "printf READY; sleep 60"),
            options: options(&source),
        })
        .unwrap()
        .0;
    let session_id = launched["session_id"].as_str().unwrap();
    service
        .handle(RpcRequest::Close {
            session_id: session_id.into(),
        })
        .unwrap();
    fs::remove_file(
        root.join("sessions")
            .join(session_id)
            .join("closed-evidence.json"),
    )
    .unwrap();
    service.shutdown().unwrap();
    drop(service);

    let reloaded = SessionService::new(&root).unwrap();
    let finalized = agent(
        &reloaded,
        "finalize",
        json!({"session_id":session_id,"expected_run_id":launched["run_id"],"outcome":"passed"}),
    )
    .unwrap();
    assert_eq!(finalized["disposal"], "failed");
    assert_eq!(finalized["evidence"]["complete"], false);
    assert!(Path::new(launched["workspace"].as_str().unwrap()).is_dir());
    assert!(
        root.join("sessions")
            .join(session_id)
            .join("state")
            .is_dir()
    );
    assert!(
        finalized["reason"]
            .as_str()
            .unwrap()
            .contains("evidence is unavailable")
    );
    reloaded.shutdown().unwrap();
}

#[test]
fn active_scenario_holds_evidence_until_terminal_without_owning_the_referenced_session() {
    let tmp = temp();
    let source = tmp.path().join("project");
    fs::create_dir(&source).unwrap();
    let root = tmp.path().join("runtime");
    store_template(&root);
    let service = SessionService::new(&root).unwrap();
    let launch = service
        .handle(RpcRequest::LaunchConfigured {
            spec: launch_spec(&source, "printf READY; sleep 60"),
            options: options(&source),
        })
        .unwrap()
        .0;
    let id = launch["session_id"].as_str().unwrap();
    let other = service
        .handle(RpcRequest::LaunchConfigured {
            spec: launch_spec(&source, "sleep 60"),
            options: LaunchOptions::default(),
        })
        .unwrap()
        .0;
    let job = agent(&service,"scenario_run",json!({"scenario":{"version":1,"name":"reference-reader","steps":[
        {"id":"read","operation":"observe","params":{"session_id":id}},
        {"id":"hold","operation":"wait","timeout_ms":10000,"params":{"session_id":other["session_id"],"timeout_ms":10000,"predicate":{"kind":"text_contains","text":"never appears"}}}
    ]}})).unwrap();
    let args = json!({"session_id":id,"expected_run_id":launch["run_id"],"outcome":"passed"});
    let result = agent(&service, "finalize", args.clone()).unwrap();
    assert_eq!(result["disposal"], "pending");
    assert!(Path::new(launch["workspace"].as_str().unwrap()).exists());
    agent(&service, "scenario_cancel", json!({"job_id":job["job_id"]})).unwrap();
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let result = agent(&service, "finalize", args.clone()).unwrap();
        if result["disposal"] == "disposed" {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "scenario evidence lease not released: {result}"
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    }
    service.shutdown().unwrap();
}

#[test]
fn concurrent_conflicting_finalizers_commit_exactly_one_outcome() {
    let tmp = temp();
    let root = tmp.path().join("runtime");
    let service = SessionService::new(&root).unwrap();
    let launch = service
        .handle(RpcRequest::LaunchConfigured {
            spec: launch_spec(tmp.path(), "sleep 60"),
            options: LaunchOptions::default(),
        })
        .unwrap()
        .0;
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let workers: Vec<_> = ["passed","failed"].into_iter().map(|outcome| {
        let service = service.clone(); let barrier = barrier.clone(); let launch = launch.clone();
        std::thread::spawn(move || { barrier.wait(); agent(&service,"finalize",json!({"session_id":launch["session_id"],"expected_run_id":launch["run_id"],"outcome":outcome})) })
    }).collect();
    let results: Vec<_> = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(
        results
            .iter()
            .filter(|result| result
                .as_ref()
                .err()
                .is_some_and(|error| error.to_string().contains("outcome_conflict")))
            .count(),
        1
    );
    service.shutdown().unwrap();
}

#[test]
fn required_namespace_restart_and_finalize_reap_owned_detached_descendants() {
    fn descendants(pid: i32, output: &mut Vec<i32>) {
        if output.len() >= 64 {
            return;
        }
        output.push(pid);
        if let Ok(text) = fs::read_to_string(format!("/proc/{pid}/task/{pid}/children")) {
            for child in text
                .split_whitespace()
                .filter_map(|value| value.parse::<i32>().ok())
            {
                descendants(child, output);
            }
        }
    }
    fn stopped(pid: i32) -> bool {
        fs::read_to_string(format!("/proc/{pid}/stat")).map_or(true, |stat| {
            stat.rsplit_once(')')
                .is_some_and(|(_, tail)| tail.trim_start().starts_with('Z'))
        })
    }
    let tmp = temp();
    let source = tmp.path().join("project");
    fs::create_dir(&source).unwrap();
    let root = tmp.path().join("runtime");
    store_template(&root);
    let service = SessionService::new(&root).unwrap();
    let mut launch = service
        .handle(RpcRequest::LaunchConfigured {
            spec: launch_spec(&source, "setsid /bin/sleep 60 & printf OWNED_READY; wait"),
            options: options(&source),
        })
        .unwrap()
        .0;
    let id = launch["session_id"].as_str().unwrap().to_owned();
    for cycle in 0..2 {
        assert_eq!(
            service
                .handle(RpcRequest::Wait {
                    session_id: id.clone(),
                    predicate: WaitPredicate::TextContains {
                        text: "OWNED_READY".into()
                    },
                    timeout_ms: 5000
                })
                .unwrap()
                .0["matched"],
            true
        );
        let mut owned = Vec::new();
        descendants(launch["process_group"].as_i64().unwrap() as i32, &mut owned);
        assert!(owned.len() >= 2, "expected real sandbox descendants");
        if cycle == 0 {
            launch = service
                .handle(RpcRequest::Restart {
                    session_id: id.clone(),
                })
                .unwrap()
                .0;
        } else {
            assert_eq!(
                agent(
                    &service,
                    "finalize",
                    json!({"session_id":id,"expected_run_id":launch["run_id"],"outcome":"passed"})
                )
                .unwrap()["disposal"],
                "disposed"
            );
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(3);
        while owned.iter().any(|pid| !stopped(*pid)) {
            assert!(
                std::time::Instant::now() < deadline,
                "owned descendants survived shutdown: {owned:?}"
            );
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
    }
    service.shutdown().unwrap();
}

#[test]
fn pending_disposal_resumes_after_service_reload_without_recapturing_evidence() {
    let tmp = temp();
    let source = tmp.path().join("project");
    fs::create_dir(&source).unwrap();
    let root = tmp.path().join("runtime");
    store_template(&root);
    let service = SessionService::new(&root).unwrap();
    let launch = service
        .handle(RpcRequest::LaunchConfigured {
            spec: launch_spec(&source, "printf READY; sleep 60"),
            options: options(&source),
        })
        .unwrap()
        .0;
    let id = launch["session_id"].as_str().unwrap();
    let session = service.viewer_session(id).unwrap();
    let lease = session.retain_evidence().unwrap();
    let args = json!({"session_id":id,"expected_run_id":launch["run_id"],"outcome":"passed"});
    let pending = agent(&service, "finalize", args.clone()).unwrap();
    assert_eq!(pending["disposal"], "pending");
    drop(lease);
    drop(session);
    service.shutdown().unwrap();
    drop(service);
    let reloaded = SessionService::new(&root).unwrap();
    let done = agent(&reloaded, "finalize", args).unwrap();
    assert_eq!(done["disposal"], "disposed");
    assert_eq!(pending["evidence"], done["evidence"]);
    assert!(
        agent(
            &reloaded,
            "read_artifact",
            json!({"session_id":id,"artifact_id":done["evidence"]["artifact_id"]})
        )
        .is_ok()
    );
    assert!(!Path::new(launch["workspace"].as_str().unwrap()).exists());
}

#[test]
fn legacy_broad_credentials_are_not_misrepresented_as_sanitized_evidence() {
    let tmp = temp();
    let root = tmp.path().join("runtime");
    let seed = tmp.path().join("seed");
    fs::create_dir(&seed).unwrap();
    fs::write(seed.join("auth.json"), "synthetic-secret").unwrap();
    let service = SessionService::new(&root).unwrap();
    let launch = service
        .handle(RpcRequest::LaunchConfigured {
            spec: launch_spec(tmp.path(), "printf synthetic-secret; sleep 60"),
            options: LaunchOptions {
                state_mode: silicogen_terminal_runtime::config::StateMode::CopyUser,
                seeds: vec![silicogen_terminal_runtime::config::StateSeed {
                    source: seed,
                    destination: "data/application".into(),
                    include_credentials: true,
                }],
                ..Default::default()
            },
        })
        .unwrap()
        .0;
    let result = agent(&service,"finalize",json!({"session_id":launch["session_id"],"expected_run_id":launch["run_id"],"outcome":"failed"})).unwrap();
    assert_eq!(result["disposal"], "failed");
    assert_eq!(result["evidence"]["sanitized"], false);
    assert_eq!(result["evidence"]["sanitization"], "not_certified");
    assert_eq!(result["evidence"]["complete"], false);
    assert_eq!(result["evidence"]["capture_failed"], true);
    assert!(!result["evidence"].to_string().contains("synthetic-secret"));
    assert!(
        root.join("sessions")
            .join(launch["session_id"].as_str().unwrap())
            .join("state")
            .is_dir()
    );
    service.shutdown().unwrap();
}

#[test]
fn seeded_login_works_in_required_sandbox_and_host_sentinels_survive_restart() {
    let tmp = temp();
    let host = tmp.path().join("host");
    let source = tmp.path().join("project");
    fs::create_dir_all(&host).unwrap();
    fs::create_dir_all(&source).unwrap();
    for (path, value) in [
        (host.join("auth.json"), "synthetic-login"),
        (host.join("settings.json"), "synthetic-config"),
        (host.join("history.db"), "synthetic-database"),
        (source.join("rtl.v"), "original-rtl"),
    ] {
        fs::write(path, value).unwrap();
    }
    let root = tmp.path().join("runtime");
    let mut template = serde_json::to_value(profiles::silicogen_test_profile_template()).unwrap();
    template["seeds"] = json!([
        {"source":host.join("auth.json"),"destination":"data/silicogen/auth.json","include_credentials":true},
        {"source":host.join("settings.json"),"destination":"config/silicogen/settings.json"}
    ]);
    let profile = serde_json::from_value(template).unwrap();
    profiles::store_profile(&root, &profile).unwrap();
    let service = SessionService::new(&root).unwrap();
    let script = format!(
        "test \"$(cat \"$XDG_DATA_HOME/silicogen/auth.json\")\" = synthetic-login || exit 10; \
         test -f \"$XDG_CONFIG_HOME/silicogen/settings.json\" || exit 11; \
         test ! -e \"$XDG_DATA_HOME/silicogen/history.db\" || exit 12; \
         test -z \"${{SSH_AUTH_SOCK+x}}\" || exit 13; \
         printf edited > rtl.v; printf local-db > \"$XDG_DATA_HOME/silicogen/new.db\"; \
         export HOME='{}'; if printf breach > \"$HOME/auth.json\" 2>/dev/null; then printf BREACH; exit 14; fi; \
         printf SAFE_READY; sleep 60",
        host.display()
    );
    let launched = service
        .handle(RpcRequest::LaunchConfigured {
            spec: launch_spec(&source, &script),
            options: options(&source),
        })
        .unwrap()
        .0;
    let id = launched["session_id"].as_str().unwrap();
    for attempt in 0..2 {
        if attempt == 1 {
            service
                .handle(RpcRequest::Restart {
                    session_id: id.into(),
                })
                .unwrap();
        }
        let result: Value = service
            .handle(RpcRequest::Wait {
                session_id: id.into(),
                predicate: WaitPredicate::TextContains {
                    text: "SAFE_READY".into(),
                },
                timeout_ms: 5000,
            })
            .unwrap()
            .0;
        assert_eq!(result["matched"], true, "{result}");
        assert!(
            !result["snapshot"]["text"]
                .as_str()
                .unwrap()
                .contains("BREACH")
        );
    }
    service.shutdown().unwrap();
    assert_eq!(
        launched["capabilities"]["launch"]["isolation"]["effective_backend"],
        "bubblewrap"
    );
    let snapshot = Path::new(launched["workspace"].as_str().unwrap());
    assert_eq!(
        fs::read_to_string(snapshot.join("rtl.v")).unwrap(),
        "edited"
    );
    assert_eq!(
        fs::read_to_string(source.join("rtl.v")).unwrap(),
        "original-rtl"
    );
    assert_eq!(
        fs::read_to_string(host.join("auth.json")).unwrap(),
        "synthetic-login"
    );
    assert_eq!(
        fs::read_to_string(host.join("history.db")).unwrap(),
        "synthetic-database"
    );
}
