use silicogen_terminal_runtime::{isolation, protocol, workspace};

use isolation::{IsolationBackend, IsolationSpec, NetworkPolicy};
use protocol::LaunchSpec;
use std::fs;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::process::Command;
use workspace::{WorkspaceMode, WorkspaceSpec, prepare_workspace};

fn spec(source: &Path) -> WorkspaceSpec {
    WorkspaceSpec {
        mode: WorkspaceMode::Copy,
        source: source.display().to_string(),
        exclude: vec![
            ".git".into(),
            "target".into(),
            "node_modules".into(),
            ".runtime".into(),
        ],
    }
}

#[test]
fn snapshots_dirty_and_untracked_files_without_linking_or_copying_excluded_roots() {
    let temp = tempfile::tempdir().unwrap();
    let origin = temp.path().join("origin");
    fs::create_dir_all(origin.join(".git")).unwrap();
    fs::create_dir_all(origin.join("target")).unwrap();
    fs::create_dir_all(origin.join(".runtime")).unwrap();
    fs::write(origin.join("tracked.txt"), "dirty contents").unwrap();
    fs::write(origin.join("untracked.txt"), "new file").unwrap();
    fs::write(origin.join(".git/config"), "private git metadata").unwrap();
    fs::write(origin.join("target/build"), "ignored build").unwrap();
    fs::write(origin.join(".runtime/auth.json"), "synthetic secret").unwrap();

    let snapshot = prepare_workspace(temp.path(), "session-1", &spec(&origin)).unwrap();
    assert_eq!(
        fs::read_to_string(snapshot.root.join("tracked.txt")).unwrap(),
        "dirty contents"
    );
    assert_eq!(
        fs::read_to_string(snapshot.root.join("untracked.txt")).unwrap(),
        "new file"
    );
    assert!(!snapshot.root.join(".git").exists());
    assert!(!snapshot.root.join("target").exists());
    assert!(!snapshot.root.join(".runtime").exists());
    assert_ne!(
        fs::metadata(origin.join("tracked.txt")).unwrap().ino(),
        fs::metadata(snapshot.root.join("tracked.txt"))
            .unwrap()
            .ino()
    );
    assert_eq!(snapshot.source, fs::canonicalize(origin).unwrap());
    assert_eq!(snapshot.digest.len(), 64);
    fs::write(snapshot.root.join("tracked.txt"), "workspace edit").unwrap();
    assert_eq!(
        fs::read_to_string(snapshot.source.join("tracked.txt")).unwrap(),
        "dirty contents"
    );
}

#[test]
fn workspace_copy_enforces_its_reserved_byte_limit_and_removes_partial_tree() {
    let temp = tempfile::tempdir().unwrap();
    let origin = temp.path().join("origin");
    fs::create_dir_all(&origin).unwrap();
    fs::write(origin.join("payload.bin"), vec![b'x'; 32]).unwrap();

    let error = workspace::prepare_workspace_with_limit(
        temp.path(),
        "limited-workspace",
        &spec(&origin),
        Some(16),
    )
    .unwrap_err();
    assert!(error.to_string().contains("retention_quota_exceeded"));
    assert!(!temp.path().join("workspaces/limited-workspace").exists());
}

#[test]
fn refuses_symlink_escape_and_rebases_internal_absolute_symlinks() {
    let temp = tempfile::tempdir().unwrap();
    let origin = temp.path().join("origin");
    fs::create_dir_all(&origin).unwrap();
    fs::write(origin.join("file.txt"), "origin").unwrap();
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(origin.join("file.txt"), origin.join("absolute-link")).unwrap();
        let snapshot = prepare_workspace(temp.path(), "internal", &spec(&origin)).unwrap();
        fs::write(snapshot.root.join("absolute-link"), "snapshot edit").unwrap();
        assert_eq!(
            fs::read_to_string(origin.join("file.txt")).unwrap(),
            "origin"
        );
        assert_eq!(
            fs::read_to_string(snapshot.root.join("file.txt")).unwrap(),
            "snapshot edit"
        );

        let outside = temp.path().join("outside.txt");
        fs::write(&outside, "outside").unwrap();
        std::os::unix::fs::symlink(&outside, origin.join("escape")).unwrap();
        let error = prepare_workspace(temp.path(), "escape", &spec(&origin)).unwrap_err();
        assert!(error.to_string().contains("symlink escaping source root"));
        assert!(!temp.path().join("workspaces/escape").exists());
    }
}

#[test]
fn reflink_mode_falls_back_to_physical_copy_when_unavailable() {
    let temp = tempfile::tempdir().unwrap();
    let origin = temp.path().join("origin");
    fs::create_dir_all(&origin).unwrap();
    fs::write(origin.join("file"), "contents").unwrap();
    let mut request = spec(&origin);
    request.mode = WorkspaceMode::Reflink;
    let snapshot = prepare_workspace(temp.path(), "reflink", &request).unwrap();
    assert!(snapshot.method == "copy" || snapshot.method == "reflink");
    assert_ne!(
        fs::metadata(origin.join("file")).unwrap().ino(),
        fs::metadata(snapshot.root.join("file")).unwrap().ino()
    );
}

#[test]
fn requested_no_network_fails_closed_on_host_backend() {
    let request = IsolationSpec {
        backend: IsolationBackend::Host,
        network: NetworkPolicy::None,
    };
    assert!(
        isolation::wrap_command(
            &launch_spec("/bin/true", vec![]),
            Path::new("."),
            None,
            &request
        )
        .is_err()
    );
    let caps = isolation::capabilities(&request);
    assert_eq!(caps["available"], false);
    assert_eq!(caps["effective_backend"], "unavailable");
}

#[test]
fn strict_mode_blocks_absolute_origin_writes_when_bubblewrap_is_available() {
    if !Path::new("/var/tmp").is_dir() {
        eprintln!(
            "STRICT ISOLATION UNAVAILABLE FOR THIS TEST: /var/tmp is not present for a host read-only probe"
        );
        return;
    }
    let temp = tempfile::tempdir_in("/var/tmp").unwrap();
    let origin = temp.path().join("origin");
    let state = temp.path().join("state");
    fs::create_dir_all(&origin).unwrap();
    fs::create_dir_all(&state).unwrap();
    let protected = origin.join("synthetic-auth.json");
    fs::write(&protected, "original").unwrap();
    let manifest = prepare_workspace(temp.path(), "strict-workspace", &spec(&origin)).unwrap();
    let isolation = IsolationSpec {
        backend: IsolationBackend::Bubblewrap,
        network: NetworkPolicy::None,
    };
    let caps = isolation::capabilities(&isolation);
    if caps["available"] != true {
        let reason = caps["reason"].as_str().unwrap_or("no diagnostic");
        eprintln!("STRICT ISOLATION UNAVAILABLE: {reason}");
        assert!(!reason.is_empty());
        return;
    }

    let script = format!(
        "if printf changed > '{}'; then exit 41; fi; printf workspace > '{}/result';",
        protected.display(),
        manifest.root.display()
    );
    let original = launch_spec("/bin/sh", vec!["-c".into(), script]);
    let wrapped = isolation::wrap_command(&original, &state, Some(&manifest), &isolation).unwrap();
    let status = Command::new(wrapped.program)
        .args(wrapped.args)
        .current_dir(wrapped.cwd)
        .status()
        .unwrap();
    assert!(
        status.success(),
        "workload should fail the protected write and continue in the workspace"
    );
    assert_eq!(fs::read_to_string(protected).unwrap(), "original");
    assert_eq!(
        fs::read_to_string(manifest.root.join("result")).unwrap(),
        "workspace"
    );
}

fn launch_spec(program: &str, args: Vec<String>) -> LaunchSpec {
    LaunchSpec {
        name: None,
        program: program.into(),
        args,
        cwd: "/".into(),
        env: Default::default(),
        cols: 80,
        rows: 24,
        record_output: false,
    }
}
