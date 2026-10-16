use silicogen_terminal_runtime::config::LaunchOptions;
use silicogen_terminal_runtime::protocol::{LaunchSpec, RpcRequest};
use silicogen_terminal_runtime::retention::{RetentionPolicy, RetentionRegistry};
use silicogen_terminal_runtime::service::SessionService;
use silicogen_terminal_runtime::workspace::{WorkspaceMode, WorkspaceSpec};
use std::collections::BTreeMap;
use std::fs;
use std::sync::{Arc, Barrier};
use std::thread;
use tempfile::TempDir;

fn service_with_policy(temp: &TempDir, policy: RetentionPolicy) -> SessionService {
    let runtime = temp.path().join("runtime");
    fs::create_dir_all(&runtime).unwrap();
    fs::write(
        runtime.join("retention-policy.json"),
        serde_json::to_vec(&policy).unwrap(),
    )
    .unwrap();
    SessionService::new(runtime).unwrap()
}

fn spec(cwd: &std::path::Path) -> LaunchSpec {
    LaunchSpec {
        name: Some("quota-admission-test".into()),
        program: "/bin/sleep".into(),
        args: vec!["60".into()],
        cwd: cwd.to_string_lossy().into_owned(),
        env: BTreeMap::new(),
        cols: 80,
        rows: 24,
        record_output: false,
    }
}

#[test]
fn workspace_larger_than_remaining_byte_budget_is_rejected_before_allocation() {
    let temp = TempDir::new().unwrap();
    let runtime = temp.path().join("runtime");
    let service = service_with_policy(
        &temp,
        RetentionPolicy {
            max_bytes: Some(1024 * 1024),
            max_entries: Some(10),
            ..RetentionPolicy::default()
        },
    );
    let source = temp.path().join("large-workspace");
    fs::create_dir_all(&source).unwrap();
    let file = fs::File::create(source.join("large.bin")).unwrap();
    file.set_len(2 * 1024 * 1024).unwrap();

    let mut launch = spec(temp.path());
    launch.cwd = "${workspace}".into();
    let error = service
        .handle(RpcRequest::LaunchConfigured {
            spec: launch,
            options: LaunchOptions {
                workspace: Some(WorkspaceSpec {
                    mode: WorkspaceMode::Copy,
                    source: source.to_string_lossy().into_owned(),
                    ..Default::default()
                }),
                ..Default::default()
            },
        })
        .unwrap_err();
    assert!(
        error.to_string().contains("retention_quota_exceeded"),
        "{error:#}"
    );
    assert_eq!(fs::read_dir(runtime.join("sessions")).unwrap().count(), 0);
    assert!(!runtime.join("workspaces").exists());

    let registry = RetentionRegistry::open(runtime).unwrap();
    assert!(registry.entries().is_empty());
}

#[test]
fn concurrent_launches_cannot_exceed_a_two_entry_quota() {
    let temp = TempDir::new().unwrap();
    let service = service_with_policy(
        &temp,
        RetentionPolicy {
            max_bytes: Some(4 * 1024 * 1024),
            max_entries: Some(2),
            ..RetentionPolicy::default()
        },
    );
    let barrier = Arc::new(Barrier::new(2));
    let mut launches = Vec::new();
    for use_legacy_launch in [false, true] {
        let service = service.clone();
        let barrier = Arc::clone(&barrier);
        let cwd = temp.path().to_path_buf();
        launches.push(thread::spawn(move || {
            barrier.wait();
            if use_legacy_launch {
                service.handle(RpcRequest::Launch { spec: spec(&cwd) })
            } else {
                service.handle(RpcRequest::LaunchConfigured {
                    spec: spec(&cwd),
                    options: LaunchOptions::default(),
                })
            }
        }));
    }
    let results = launches
        .into_iter()
        .map(|launch| launch.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);

    let registry = RetentionRegistry::open(temp.path().join("runtime")).unwrap();
    assert_eq!(registry.entries().len(), 2);
    assert!(
        registry
            .entries()
            .iter()
            .any(|entry| entry.kind == silicogen_terminal_runtime::retention::OwnerKind::Session)
    );
    assert!(
        registry
            .entries()
            .iter()
            .any(|entry| entry.kind == silicogen_terminal_runtime::retention::OwnerKind::Artifact)
    );
    service.shutdown().unwrap();
}
