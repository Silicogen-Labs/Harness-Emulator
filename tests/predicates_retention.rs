use silicogen_terminal_runtime::{predicates, protocol, retention};

use predicates::{PredicateEvaluator, PredicateOutcome, PredicateSpec, RegionStability};
use protocol::{CellSnapshot, CellStyle, CursorSnapshot, FrameSnapshot};
use retention::{
    ManagedEntry, OwnerKind, ReservationTarget, RetentionClass, RetentionPolicy, RetentionRegistry,
};
use std::fs;
use std::time::{Duration, Instant};
use tempfile::TempDir;

fn cell(text: &str, bold: bool) -> CellSnapshot {
    CellSnapshot {
        text: text.into(),
        width: 1,
        foreground: None,
        background: None,
        resolved_foreground: "white".into(),
        resolved_background: "black".into(),
        bold,
        dim: false,
        italic: false,
        inverse: false,
        invisible: false,
        strike: false,
        blink: false,
        underline: "none".into(),
        underline_color: None,
        hyperlink: None,
    }
}

fn frame(text: &str, sequence: u64, state: &str, cells: Vec<Vec<CellSnapshot>>) -> FrameSnapshot {
    FrameSnapshot {
        session_id: "session-a".into(),
        run_id: "run-a".into(),
        frame_id: format!("frame-{sequence}"),
        captured_at_ms: sequence as u128,
        output_sequence: sequence,
        cols: 4,
        rows: 2,
        text: text.into(),
        base_style: CellStyle {
            foreground: "white".into(),
            background: "black".into(),
            bold: false,
            dim: false,
            italic: false,
            inverse: false,
            invisible: false,
            strike: false,
            blink: false,
            underline: "none".into(),
            underline_color: None,
            hyperlink: None,
        },
        styled_runs: Vec::new(),
        cells,
        cursor: CursorSnapshot {
            row: 0,
            col: 0,
            visible: false,
            shape: "block".into(),
            color: "white".into(),
        },
        modes: Default::default(),
        keyboard_mode: Vec::new(),
        mouse_tracking: "none".into(),
        mouse_encoding: "none".into(),
        default_foreground: "white".into(),
        default_background: "black".into(),
        cursor_color: "white".into(),
        palette: Default::default(),
        title: None,
        process_state: state.into(),
        capture_complete: true,
        capture_warning: None,
    }
}

#[test]
fn identical_text_repaint_does_not_satisfy_output_quiet() {
    let t0 = Instant::now();
    let mut evaluator =
        PredicateEvaluator::new(PredicateSpec::OutputQuiet { duration_ms: 500 }).unwrap();
    let repaint = frame("working", 10, "running", Vec::new());
    assert!(!evaluator.observe(&repaint, t0).matched);
    assert!(
        !evaluator
            .observe(
                &frame("working", 11, "running", Vec::new()),
                t0 + Duration::from_secs(1)
            )
            .matched
    );
    let result = evaluator.observe(
        &frame("working", 11, "running", Vec::new()),
        t0 + Duration::from_secs(2),
    );
    assert!(result.matched);
    assert_eq!(result.outcome, PredicateOutcome::Quiet);
}

#[test]
fn ready_requires_markers_and_reports_crash_and_exit_separately() {
    let spec = PredicateSpec::Ready {
        required: vec!["READY".into()],
        busy: vec!["LOADING".into()],
        crash: vec!["FATAL".into()],
        stable_region: None,
    };
    let mut evaluator = PredicateEvaluator::new(spec.clone()).unwrap();
    assert!(
        !evaluator
            .observe(
                &frame("quiet blank", 0, "running", Vec::new()),
                Instant::now()
            )
            .matched
    );
    let crash = evaluator.observe(
        &frame("FATAL error", 1, "running", Vec::new()),
        Instant::now(),
    );
    assert!(crash.matched);
    assert_eq!(crash.outcome, PredicateOutcome::AppCrash);

    let mut exited = PredicateEvaluator::new(spec).unwrap();
    let result = exited.observe(&frame("READY", 4, "exited:0", Vec::new()), Instant::now());
    assert!(result.matched);
    assert_eq!(result.outcome, PredicateOutcome::Exited);
}

#[test]
fn ready_can_require_stable_cell_styles_in_a_region() {
    let spec = PredicateSpec::Ready {
        required: vec!["READY".into()],
        busy: Vec::new(),
        crash: Vec::new(),
        stable_region: Some(RegionStability {
            row: 0,
            col: 0,
            rows: 1,
            cols: 1,
            duration_ms: 100,
        }),
    };
    let t0 = Instant::now();
    let mut evaluator = PredicateEvaluator::new(spec).unwrap();
    let first = frame(
        "READY",
        1,
        "running",
        vec![
            vec![
                cell("R", false),
                cell(" ", false),
                cell(" ", false),
                cell(" ", false),
            ],
            vec![cell(" ", false); 4],
        ],
    );
    assert!(!evaluator.observe(&first, t0).matched);
    let style_changed = frame(
        "READY",
        2,
        "running",
        vec![
            vec![
                cell("R", true),
                cell(" ", false),
                cell(" ", false),
                cell(" ", false),
            ],
            vec![cell(" ", false); 4],
        ],
    );
    assert!(
        !evaluator
            .observe(&style_changed, t0 + Duration::from_millis(200))
            .matched
    );
    let stable = evaluator.observe(
        &frame("READY", 2, "running", style_changed.cells.clone()),
        t0 + Duration::from_millis(301),
    );
    assert!(stable.matched);
    assert_eq!(stable.outcome, PredicateOutcome::Ready);
}

#[test]
fn retention_preview_protects_active_profiles_and_reports_legacy_without_owning_them() {
    let temp = TempDir::new().unwrap();
    let root = temp.path();
    fs::create_dir(root.join("expired")).unwrap();
    fs::write(root.join("expired/evidence.txt"), "test evidence").unwrap();
    fs::create_dir(root.join("profile")).unwrap();
    fs::write(root.join("profile/config.json"), "private test profile").unwrap();
    fs::create_dir(root.join("sd.12345")).unwrap();
    fs::write(root.join("sd.12345/keep"), "unowned").unwrap();
    let mut registry = RetentionRegistry::open_with_policy(
        root,
        RetentionPolicy {
            successful_ms: 10,
            failed_ms: 100,
            ..RetentionPolicy::default()
        },
    )
    .unwrap();
    registry
        .register(ManagedEntry::new(
            "expired",
            OwnerKind::Artifact,
            RetentionClass::Successful,
            1,
            "old success",
        ))
        .unwrap();
    registry
        .register(ManagedEntry::new(
            "profile",
            OwnerKind::Profile,
            RetentionClass::Successful,
            1,
            "named profile",
        ))
        .unwrap();

    let plan = registry
        .preview(100, &[root.join("expired")], false)
        .unwrap();
    assert!(plan.candidates.is_empty());
    assert_eq!(plan.protected.len(), 2);
    assert_eq!(plan.unowned_legacy_paths, vec![root.join("sd.12345")]);
    assert!(root.join("sd.12345/keep").exists());
    let quota = registry.ensure_capacity(100, Some(1)).unwrap_err();
    assert!(quota.to_string().contains("retention_quota_exceeded"));
    assert!(quota.to_string().contains("pinned="));
}

#[test]
fn retention_apply_rechecks_generation_and_only_removes_registered_expired_data() {
    let temp = TempDir::new().unwrap();
    let root = temp.path();
    fs::create_dir(root.join("managed")).unwrap();
    fs::write(root.join("managed/output.log"), "discardable").unwrap();
    fs::create_dir(root.join("unregistered")).unwrap();
    fs::write(root.join("unregistered/keep"), "keep").unwrap();
    let mut registry = RetentionRegistry::open_with_policy(
        root,
        RetentionPolicy {
            successful_ms: 10,
            failed_ms: 100,
            ..RetentionPolicy::default()
        },
    )
    .unwrap();
    registry
        .register(ManagedEntry::new(
            "managed",
            OwnerKind::Artifact,
            RetentionClass::Successful,
            1,
            "old success",
        ))
        .unwrap();
    let plan = registry.preview(100, &[], false).unwrap();
    assert_eq!(plan.candidates.len(), 1);
    let applied = registry.apply(&plan, &[]).unwrap();
    assert_eq!(applied.removed.len(), 1);
    assert!(!root.join("managed").exists());
    assert!(root.join("unregistered/keep").exists());

    let stale = registry.preview(100, &[], false).unwrap();
    registry
        .register(ManagedEntry::new(
            "unregistered",
            OwnerKind::Artifact,
            RetentionClass::Interactive,
            1,
            "new entry",
        ))
        .unwrap();
    assert!(registry.apply(&stale, &[]).is_err());
}

#[test]
fn budgets_account_for_expired_entries_but_quota_on_active_and_pinned_usage() {
    let temp = TempDir::new().unwrap();
    let root = temp.path();
    for (path, contents) in [
        ("expired", "1234567890"),
        ("active", "abcdefgh"),
        ("profile", "PROFILE!!"),
    ] {
        fs::create_dir(root.join(path)).unwrap();
        fs::write(root.join(path).join("data"), contents).unwrap();
    }
    let mut registry = RetentionRegistry::open_with_policy(
        root,
        RetentionPolicy {
            successful_ms: 10,
            failed_ms: 100,
            max_bytes: Some(16),
            max_entries: Some(2),
        },
    )
    .unwrap();
    for (path, kind, retention) in [
        ("expired", OwnerKind::Artifact, RetentionClass::Successful),
        ("active", OwnerKind::Session, RetentionClass::Successful),
        ("profile", OwnerKind::Profile, RetentionClass::Successful),
    ] {
        registry
            .register(ManagedEntry::new(
                path,
                kind,
                retention,
                1,
                "budget fixture",
            ))
            .unwrap();
    }

    let error = registry
        .ensure_budget(0, 0, 100, &[root.join("active")])
        .unwrap_err();
    let message = error.to_string();
    assert!(message.contains("retention_quota_exceeded"));
    assert!(message.contains("pinned_bytes=9"));
    assert!(message.contains("active_bytes=8"));
    assert!(root.join("expired/data").exists());

    let sufficient = RetentionRegistry::open_with_policy(
        root,
        RetentionPolicy {
            successful_ms: 10,
            failed_ms: 100,
            max_bytes: Some(27),
            max_entries: Some(3),
        },
    )
    .unwrap();
    let status = sufficient
        .ensure_budget(0, 0, 100, &[root.join("active")])
        .unwrap();
    assert_eq!(status.projected_bytes, 27);
    assert_eq!(status.projected_entries, 3);
    assert_eq!(status.reclaimable_entries, 1);
}

#[test]
fn retention_policy_defaults_are_finite_but_null_remains_unlimited() {
    let policy = RetentionPolicy::default();
    assert_eq!(policy.max_bytes, Some(5 * 1024 * 1024 * 1024));
    assert_eq!(policy.max_entries, Some(10_000));

    let explicit_null: RetentionPolicy = serde_json::from_str(
        r#"{"successful_ms":1,"failed_ms":2,"max_bytes":null,"max_entries":null}"#,
    )
    .unwrap();
    assert_eq!(explicit_null.max_bytes, None);
    assert_eq!(explicit_null.max_entries, None);

    let legacy_missing: RetentionPolicy =
        serde_json::from_str(r#"{"successful_ms":1,"failed_ms":2}"#).unwrap();
    assert_eq!(legacy_missing.max_bytes, Some(5 * 1024 * 1024 * 1024));
    assert_eq!(legacy_missing.max_entries, Some(10_000));
}

#[test]
fn one_workspace_reservation_claims_three_entries_and_release_rolls_back() {
    let temp = TempDir::new().unwrap();
    let mut registry = RetentionRegistry::open_with_policy(
        temp.path(),
        RetentionPolicy {
            max_bytes: Some(100),
            max_entries: Some(3),
            ..RetentionPolicy::default()
        },
    )
    .unwrap();
    let targets = vec![
        ReservationTarget::new("workspaces/ws", OwnerKind::TestWorkspace, "workspace"),
        ReservationTarget::new("runs/run-1", OwnerKind::Run, "run outputs"),
        ReservationTarget::new("artifacts/run-1", OwnerKind::Artifact, "artifacts"),
    ];
    let reservation = registry.reserve(75, targets).unwrap();
    assert_eq!(reservation.targets.len(), 3);

    let status = registry.ensure_budget(0, 0, u128::MAX, &[]).unwrap();
    assert_eq!(status.projected_bytes, 75);
    assert_eq!(status.projected_entries, 3);

    assert!(
        registry
            .reserve(
                1,
                vec![ReservationTarget::new(
                    "other",
                    OwnerKind::Artifact,
                    "over count",
                )],
            )
            .unwrap_err()
            .to_string()
            .contains("retention_quota_exceeded")
    );

    registry.release_reservation(&reservation.id).unwrap();
    let status = registry.ensure_budget(0, 0, u128::MAX, &[]).unwrap();
    assert_eq!(status.projected_bytes, 0);
    assert_eq!(status.projected_entries, 0);
}

#[test]
fn concurrent_reservations_cannot_overclaim_bytes() {
    let temp = TempDir::new().unwrap();
    let root = temp.path().to_path_buf();
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
    let mut workers = Vec::new();
    for name in ["one", "two"] {
        let root = root.clone();
        let barrier = barrier.clone();
        workers.push(std::thread::spawn(move || {
            let mut registry = RetentionRegistry::open_with_policy(
                root,
                RetentionPolicy {
                    max_bytes: Some(10),
                    max_entries: Some(2),
                    ..RetentionPolicy::default()
                },
            )
            .unwrap();
            barrier.wait();
            registry.reserve(
                6,
                vec![ReservationTarget::new(
                    name,
                    OwnerKind::Artifact,
                    "concurrent claim",
                )],
            )
        }));
    }
    let results = workers
        .into_iter()
        .map(|worker| worker.join().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
    assert_eq!(results.iter().filter(|result| result.is_err()).count(), 1);
}

#[test]
fn reservation_commit_reconciles_actual_bytes() {
    let temp = TempDir::new().unwrap();
    let mut registry = RetentionRegistry::open_with_policy(
        temp.path(),
        RetentionPolicy {
            max_bytes: Some(100),
            max_entries: Some(2),
            ..RetentionPolicy::default()
        },
    )
    .unwrap();
    let reservation = registry
        .reserve(
            80,
            vec![ReservationTarget::new(
                "workspace",
                OwnerKind::TestWorkspace,
                "allocated workspace",
            )],
        )
        .unwrap();
    fs::create_dir(temp.path().join("workspace")).unwrap();
    fs::write(temp.path().join("workspace/data"), "actual bytes").unwrap();

    let entries = registry
        .commit_reservation(&reservation.id, RetentionClass::Successful, 42)
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].relative_path,
        std::path::PathBuf::from("workspace")
    );
    assert_eq!(entries[0].created_at_ms, 42);
    let status = registry.ensure_budget(0, 0, 43, &[]).unwrap();
    assert_eq!(status.used_bytes, 12);
    assert_eq!(status.projected_bytes, 12);
    assert_eq!(status.projected_entries, 1);
}

#[test]
fn reservation_commit_refuses_allocations_larger_than_the_claim() {
    let temp = TempDir::new().unwrap();
    let mut registry = RetentionRegistry::open_with_policy(
        temp.path(),
        RetentionPolicy {
            max_bytes: Some(100),
            max_entries: Some(2),
            ..RetentionPolicy::default()
        },
    )
    .unwrap();
    let reservation = registry
        .reserve(
            5,
            vec![ReservationTarget::new(
                "workspace",
                OwnerKind::TestWorkspace,
                "oversized allocation",
            )],
        )
        .unwrap();
    fs::create_dir(temp.path().join("workspace")).unwrap();
    fs::write(temp.path().join("workspace/data"), b"sixteen-byte-file").unwrap();

    let error = registry
        .commit_reservation(&reservation.id, RetentionClass::Successful, 42)
        .unwrap_err();
    assert!(error.to_string().contains("retention_quota_exceeded"));
    assert!(registry.entries().is_empty());
    assert_eq!(
        registry
            .ensure_budget(0, 0, 43, &[])
            .unwrap()
            .projected_bytes,
        5
    );

    let failed = registry.fail_reservation(&reservation.id, 44).unwrap();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].retention, RetentionClass::Failed);
}

#[test]
fn restart_reconciles_existing_orphan_allocations_as_failed_entries() {
    let temp = TempDir::new().unwrap();
    let root = temp.path();
    let reservation_id;
    {
        let mut registry = RetentionRegistry::open(root).unwrap();
        let reservation = registry
            .reserve(
                100,
                vec![
                    ReservationTarget::new(
                        "snapshot/incomplete",
                        OwnerKind::TestWorkspace,
                        "partial snapshot",
                    ),
                    ReservationTarget::new("never-created", OwnerKind::Artifact, "unused output"),
                ],
            )
            .unwrap();
        reservation_id = reservation.id;
        fs::create_dir_all(root.join("snapshot/incomplete")).unwrap();
        fs::write(root.join("snapshot/incomplete/part"), "partial").unwrap();
    }

    let reopened = RetentionRegistry::open(root).unwrap();
    assert_eq!(reopened.entries().len(), 1);
    let recovered = &reopened.entries()[0];
    assert_eq!(
        recovered.relative_path,
        std::path::PathBuf::from("snapshot/incomplete")
    );
    assert_eq!(recovered.retention, RetentionClass::Failed);
    assert!(recovered.description.contains("partial snapshot"));

    let manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(root.join("retention/owners.json")).unwrap()).unwrap();
    assert!(manifest["reservations"].as_array().unwrap().is_empty());
    assert!(!manifest.to_string().contains(&reservation_id));
    assert!(root.join("snapshot/incomplete/part").exists());
}

#[test]
fn profile_protection_survives_an_entry_kind_change() {
    let temp = TempDir::new().unwrap();
    let root = temp.path();
    fs::create_dir(root.join("profiles")).unwrap();
    fs::write(root.join("profiles/config.json"), "profile data").unwrap();
    let mut registry = RetentionRegistry::open(root).unwrap();
    registry
        .register(ManagedEntry::new(
            "profiles",
            OwnerKind::Profile,
            RetentionClass::Successful,
            0,
            "named profile",
        ))
        .unwrap();
    let manifest_path = root.join("retention/owners.json");
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&fs::read(&manifest_path).unwrap()).unwrap();
    manifest["entries"][0]["kind"] = serde_json::Value::String("artifact".into());
    fs::write(&manifest_path, serde_json::to_vec(&manifest).unwrap()).unwrap();

    let mut reopened = RetentionRegistry::open_with_policy(
        root,
        RetentionPolicy {
            successful_ms: 0,
            ..RetentionPolicy::default()
        },
    )
    .unwrap();
    let plan = reopened.preview(100, &[], false).unwrap();
    assert!(plan.candidates.is_empty());
    assert!(
        plan.protected
            .iter()
            .any(|entry| entry.reason.contains("profiles"))
    );
}

#[cfg(unix)]
#[test]
fn retention_refuses_symlinked_managed_trees() {
    use std::os::unix::fs::symlink;
    let temp = TempDir::new().unwrap();
    let root = temp.path();
    fs::create_dir(root.join("target")).unwrap();
    fs::write(root.join("target/keep"), "safe").unwrap();
    symlink(root.join("target"), root.join("linked")).unwrap();
    let mut registry = RetentionRegistry::open(root).unwrap();
    assert!(
        registry
            .register(ManagedEntry::new(
                "linked",
                OwnerKind::Artifact,
                RetentionClass::Successful,
                0,
                "unsafe"
            ))
            .is_err()
    );
    assert!(root.join("target/keep").exists());
}

#[cfg(unix)]
#[test]
fn retention_budgets_special_nodes_as_zero_and_keeps_active_tree_protected() {
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::net::UnixListener;

    let temp = TempDir::new().unwrap();
    let root = temp.path();
    let session = root.join("sessions/session-synthetic");
    fs::create_dir_all(&session).unwrap();
    fs::write(session.join("log.txt"), "abc").unwrap();

    // Linux AF_UNIX paths are short; keep the actual socket in the owned tree
    // while binding through its open directory descriptor.
    use std::os::fd::AsRawFd;
    let session_dir = fs::File::open(&session).unwrap();
    let _socket = UnixListener::bind(format!(
        "/proc/self/fd/{}/control.sock",
        session_dir.as_raw_fd()
    ))
    .unwrap();
    let fifo_path = session.join("pipe");
    let fifo_c = std::ffi::CString::new(fifo_path.as_os_str().as_bytes()).unwrap();
    assert_eq!(unsafe { libc::mkfifo(fifo_c.as_ptr(), 0o600) }, 0);

    let mut registry = RetentionRegistry::open(root).unwrap();
    registry
        .register(ManagedEntry::new(
            "sessions/session-synthetic",
            OwnerKind::Session,
            RetentionClass::Successful,
            1,
            "active session with PTY control nodes",
        ))
        .unwrap();

    let status = registry
        .ensure_budget(0, 0, 2, std::slice::from_ref(&session))
        .unwrap();
    assert_eq!(status.used_bytes, 3);
    assert_eq!(status.active_bytes, 3);

    let plan = registry
        .preview(100_000_000, std::slice::from_ref(&session), false)
        .unwrap();
    assert!(plan.candidates.is_empty());
    assert!(
        plan.protected
            .iter()
            .any(|entry| entry.reason.contains("active"))
    );
    assert!(session.join("control.sock").exists());
    assert!(session.join("pipe").exists());
}
