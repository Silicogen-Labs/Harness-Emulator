use silicogen_terminal_runtime::{artifacts, assertions, frame_store, logs, protocol};

use artifacts::{ArtifactSourceKind, ArtifactStore};
use assertions::{FrameAssertion, evaluate};
use frame_store::{CellMask, FrameStore, diff_frames};
use logs::{LogGap, LogStore};
use protocol::{CellSnapshot, CellStyle, ColorValue, FrameSnapshot};
use std::fs;
use std::path::Path;
use tempfile::TempDir;

fn cell(text: &str, foreground: &str, bold: bool) -> CellSnapshot {
    CellSnapshot {
        text: text.to_owned(),
        width: 1,
        foreground: Some(ColorValue::Rgb(255, 255, 255)),
        background: None,
        resolved_foreground: foreground.to_owned(),
        resolved_background: "#000000".into(),
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

fn frame() -> FrameSnapshot {
    serde_json::from_value(serde_json::json!({
        "session_id":"session-1", "run_id":"run-1", "frame_id":"frame-1", "captured_at_ms":10,
        "output_sequence":1, "cols":2, "rows":1, "text":"OK", "base_style":{
          "foreground":"#ffffff", "background":"#000000", "bold":false, "dim":false, "italic":false,
          "inverse":false, "invisible":false, "strike":false, "blink":false, "underline":"none",
          "underline_color":null, "hyperlink":null
        }, "styled_runs":[], "cursor":{"row":0,"col":0,"visible":false,"shape":"block","color":"#ffffff"},
        "modes":{}, "keyboard_mode":[], "mouse_tracking":"none", "mouse_encoding":"legacy",
        "default_foreground":"#ffffff", "default_background":"#000000", "cursor_color":"#ffffff",
        "palette":{}, "title":null, "process_state":"running", "capture_complete":true, "capture_warning":null
    })).unwrap()
}

fn populated_frame() -> FrameSnapshot {
    let mut value = frame();
    value.cells = vec![vec![
        cell("O", "#ffffff", false),
        cell("K", "#00ff00", true),
    ]];
    value
}

fn roots() -> (TempDir, std::path::PathBuf) {
    let temp = tempfile::tempdir().unwrap();
    let runtime = temp.path().join("runtime");
    fs::create_dir(&runtime).unwrap();
    (temp, runtime)
}

#[test]
fn saved_frame_is_immutable_and_diff_uses_explicit_masks_and_profile() {
    let (_temp, runtime) = roots();
    let store = FrameStore::new(runtime).unwrap();
    let mut observed = populated_frame();
    let saved = store
        .save_named("startup", &observed, "tui-profile-v1")
        .unwrap();
    assert_eq!(saved.name, "startup");
    observed.cells[0][0] = cell("X", "#ff0000", true);
    observed.frame_id = "frame-2".into();

    let loaded = store.load_named("session-1", "startup").unwrap();
    assert_eq!(loaded.frame.cells[0][0].text, "O");
    assert!(diff_frames(&loaded, &observed, "different-profile", &[]).is_err());
    let diff = diff_frames(&loaded, &observed, "tui-profile-v1", &[]).unwrap();
    assert_eq!(diff.changed_cell_count, 1);
    assert!(!diff.pixel_comparison);

    let masked = diff_frames(
        &loaded,
        &observed,
        "tui-profile-v1",
        &[CellMask {
            row: 0,
            col: 0,
            rows: 1,
            cols: 1,
            reason: "known cursor cell".into(),
        }],
    )
    .unwrap();
    assert_eq!(masked.changed_cell_count, 0);
    assert!(
        diff_frames(
            &loaded,
            &observed,
            "tui-profile-v1",
            &[CellMask {
                row: 1,
                col: 0,
                rows: 1,
                cols: 1,
                reason: "invalid".into(),
            }]
        )
        .is_err()
    );
}

#[test]
fn assertions_report_wrong_text_and_wrong_scoped_style() {
    let observed = populated_frame();
    let expected_style = CellStyle {
        foreground: "#ffffff".into(),
        background: "#000000".into(),
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
    };
    let report = evaluate(
        &observed,
        &FrameAssertion::All {
            assertions: vec![
                FrameAssertion::TextContains { text: "OK".into() },
                FrameAssertion::TextAbsent {
                    text: "ERROR".into(),
                },
                FrameAssertion::Regex {
                    pattern: "^OK$".into(),
                },
                FrameAssertion::CellText {
                    row: 0,
                    col: 0,
                    text: "O".into(),
                },
                FrameAssertion::CellStyle {
                    row: 0,
                    col: 1,
                    style: expected_style,
                },
            ],
        },
    )
    .unwrap();
    assert!(!report.passed);
    assert_eq!(report.checked, 5);
    assert_eq!(report.failures.len(), 1);
    assert!(report.failures[0].message.contains("style"));
    let wrong_text = evaluate(
        &observed,
        &FrameAssertion::TextContains {
            text: "MISSING".into(),
        },
    )
    .unwrap();
    assert!(!wrong_text.passed);
}

#[test]
fn log_cursor_reports_truncation_and_rotation() {
    let (temp, runtime) = roots();
    let log_dir = temp.path().join("app-logs");
    fs::create_dir(&log_dir).unwrap();
    fs::write(log_dir.join("app.log"), b"one\ntwo\n").unwrap();
    let artifacts = ArtifactStore::new(runtime).unwrap();
    let root = artifacts
        .register_log_root("session-1", "app", &log_dir)
        .unwrap();
    let logs = LogStore::new(&artifacts);
    let artifact = logs
        .register_log(
            "session-1",
            &root,
            Path::new("app.log"),
            "text/plain",
            Some("run-1".into()),
        )
        .unwrap();
    let first = logs
        .poll("session-1", &artifact.artifact_id, None, 1024)
        .unwrap();
    assert_eq!(first.bytes, b"one\ntwo\n");

    fs::write(log_dir.join("app.log"), b"x\n").unwrap();
    let truncated = logs
        .poll(
            "session-1",
            &artifact.artifact_id,
            Some(&first.cursor),
            1024,
        )
        .unwrap();
    assert_eq!(truncated.gap, Some(LogGap::Truncated));
    assert_eq!(truncated.bytes, b"x\n");

    fs::rename(log_dir.join("app.log"), log_dir.join("app.log.1")).unwrap();
    fs::write(log_dir.join("app.log"), b"new generation\n").unwrap();
    let rotated = logs
        .poll(
            "session-1",
            &artifact.artifact_id,
            Some(&truncated.cursor),
            1024,
        )
        .unwrap();
    assert_eq!(rotated.gap, Some(LogGap::Rotated));
    assert_eq!(rotated.bytes, b"new generation\n");
}

#[test]
fn daemon_log_registration_exposes_only_the_exact_regular_runtime_log() {
    let (temp, runtime) = roots();
    fs::write(runtime.join("daemon.log"), b"daemon diagnostic line\n").unwrap();
    fs::write(runtime.join("auth.json"), b"synthetic secret\n").unwrap();
    let store = ArtifactStore::new(runtime.clone()).unwrap();
    let artifact = store.register_daemon_log("session-1").unwrap();
    assert_eq!(artifact.display_name, "daemon:daemon.log");
    assert_eq!(artifact.source_kind, ArtifactSourceKind::ApplicationLog);
    assert_eq!(
        store
            .read("session-1", &artifact.artifact_id, 0, 1024)
            .unwrap()
            .bytes,
        b"daemon diagnostic line\n"
    );
    assert!(
        store
            .read("session-2", &artifact.artifact_id, 0, 10)
            .is_err()
    );

    #[cfg(unix)]
    {
        fs::remove_file(runtime.join("daemon.log")).unwrap();
        std::os::unix::fs::symlink(temp.path().join("outside.txt"), runtime.join("daemon.log"))
            .unwrap();
        assert!(store.register_daemon_log("session-1").is_err());
    }
}

#[test]
fn artifact_reader_denies_traversal_symlinks_secrets_and_cross_session_reads() {
    let (temp, runtime) = roots();
    let session_root = runtime.join("sessions").join("session-1");
    fs::create_dir_all(&session_root).unwrap();
    let evidence = session_root.join("evidence");
    fs::create_dir(&evidence).unwrap();
    fs::write(evidence.join("screen.txt"), b"safe evidence").unwrap();
    fs::write(evidence.join("auth.json"), b"synthetic secret").unwrap();
    fs::write(temp.path().join("outside.txt"), b"outside").unwrap();
    fs::hard_link(temp.path().join("outside.txt"), evidence.join("linked.txt")).unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(temp.path().join("outside.txt"), evidence.join("escape.txt"))
        .unwrap();

    let store = ArtifactStore::new(runtime).unwrap();
    let root = store
        .register_session_evidence("session-1", &evidence)
        .unwrap();
    let registered = store
        .register_evidence_file(
            "session-1",
            &root,
            Path::new("screen.txt"),
            ArtifactSourceKind::Other,
            "text/plain",
            None,
            None,
        )
        .unwrap();
    assert!(registered.artifact_id.starts_with("art-"));
    assert_eq!(
        store
            .read("session-1", &registered.artifact_id, 0, 32)
            .unwrap()
            .bytes,
        b"safe evidence"
    );
    let page = store
        .read("session-1", &registered.artifact_id, 5, 4)
        .unwrap();
    assert_eq!(page.offset, 5);
    assert_eq!(page.next_offset, 9);
    assert_eq!(page.bytes, b"evid");
    assert_eq!(store.list("session-1", 0, 10).unwrap().len(), 1);
    assert!(
        store
            .read("session-2", &registered.artifact_id, 0, 5)
            .is_err()
    );
    assert!(
        store
            .read(
                "session-1",
                &registered.artifact_id,
                0,
                artifacts::MAX_ARTIFACT_READ + 1
            )
            .is_err()
    );
    assert!(
        store
            .register_evidence_file(
                "session-1",
                &root,
                Path::new("../outside.txt"),
                ArtifactSourceKind::Other,
                "text/plain",
                None,
                None
            )
            .is_err()
    );
    assert!(
        store
            .register_session_evidence("session-1", temp.path())
            .is_err()
    );
    assert!(
        store
            .register_evidence_file(
                "session-1",
                &root,
                Path::new("linked.txt"),
                ArtifactSourceKind::Other,
                "text/plain",
                None,
                None
            )
            .is_err()
    );
    assert!(
        store
            .register_evidence_file(
                "session-1",
                &root,
                Path::new("auth.json"),
                ArtifactSourceKind::Other,
                "application/json",
                None,
                None
            )
            .is_err()
    );
    #[cfg(unix)]
    assert!(
        store
            .register_evidence_file(
                "session-1",
                &root,
                Path::new("escape.txt"),
                ArtifactSourceKind::Other,
                "text/plain",
                None,
                None
            )
            .is_err()
    );
    assert!(
        store
            .register_evidence_file(
                "session-1",
                &root,
                Path::new("api_key.txt"),
                ArtifactSourceKind::Other,
                "text/plain",
                None,
                None
            )
            .is_err()
    );
    fs::rename(evidence.join("screen.txt"), evidence.join("screen.saved")).unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(temp.path().join("outside.txt"), evidence.join("screen.txt"))
        .unwrap();
    assert!(
        store
            .read("session-1", &registered.artifact_id, 0, 32)
            .is_err()
    );
}

#[test]
fn caller_selected_evidence_identity_is_readable_scoped_and_collision_safe() {
    let (temp, runtime) = roots();
    let evidence_one = runtime.join("sessions/session-1/evidence");
    let evidence_two = runtime.join("sessions/session-2/evidence");
    fs::create_dir_all(&evidence_one).unwrap();
    fs::create_dir_all(&evidence_two).unwrap();
    fs::write(evidence_one.join("capture.png"), b"screenshot bytes").unwrap();
    fs::write(evidence_one.join("other.png"), b"different bytes").unwrap();
    fs::write(evidence_one.join("credentials.txt"), b"synthetic secret").unwrap();
    fs::write(temp.path().join("outside.png"), b"outside").unwrap();
    #[cfg(unix)]
    std::os::unix::fs::symlink(
        temp.path().join("outside.png"),
        evidence_one.join("link.png"),
    )
    .unwrap();

    let store = ArtifactStore::new(runtime).unwrap();
    let root_one = store
        .register_session_evidence("session-1", &evidence_one)
        .unwrap();
    let root_two = store
        .register_session_evidence("session-2", &evidence_two)
        .unwrap();
    let capture_id = "01J8CAPTUREIDEXISTING123456";
    let registered = store
        .register_evidence_file_with_id(
            "session-1",
            &root_one,
            Path::new("capture.png"),
            capture_id,
            ArtifactSourceKind::Screenshot,
            "image/png",
            Some("run-42".into()),
            Some("session".into()),
        )
        .unwrap();
    assert_eq!(registered.artifact_id, capture_id);
    assert_eq!(registered.source_kind, ArtifactSourceKind::Screenshot);
    assert_eq!(registered.content_type, "image/png");
    assert_eq!(registered.run_id.as_deref(), Some("run-42"));
    assert_eq!(
        store.read("session-1", capture_id, 0, 1024).unwrap().bytes,
        b"screenshot bytes"
    );
    assert_eq!(
        store
            .register_evidence_file_with_id(
                "session-1",
                &root_one,
                Path::new("capture.png"),
                capture_id,
                ArtifactSourceKind::Screenshot,
                "image/png",
                Some("run-42".into()),
                Some("session".into()),
            )
            .unwrap()
            .artifact_id,
        capture_id
    );

    assert!(
        store
            .register_evidence_file_with_id(
                "session-1",
                &root_one,
                Path::new("other.png"),
                capture_id,
                ArtifactSourceKind::Screenshot,
                "image/png",
                Some("run-42".into()),
                Some("session".into()),
            )
            .is_err()
    );
    assert!(
        store
            .register_evidence_file_with_id(
                "session-1",
                &root_two,
                Path::new("capture.png"),
                "01J8ANOTHERIDEXISTING12345",
                ArtifactSourceKind::Screenshot,
                "image/png",
                None,
                None,
            )
            .is_err()
    );
    assert!(
        store
            .register_evidence_file_with_id(
                "session-1",
                &root_one,
                Path::new("../outside.png"),
                "01J8TRAVERSALIDEXISTING12345",
                ArtifactSourceKind::Screenshot,
                "image/png",
                None,
                None,
            )
            .is_err()
    );
    assert!(
        store
            .register_evidence_file_with_id(
                "session-1",
                &root_one,
                Path::new("credentials.txt"),
                "01J8CREDENTIALIDEXISTING1234",
                ArtifactSourceKind::Screenshot,
                "text/plain",
                None,
                None,
            )
            .is_err()
    );
    #[cfg(unix)]
    assert!(
        store
            .register_evidence_file_with_id(
                "session-1",
                &root_one,
                Path::new("link.png"),
                "01J8SYMLINKIDEXISTING123456",
                ArtifactSourceKind::Screenshot,
                "image/png",
                None,
                None,
            )
            .is_err()
    );
}
