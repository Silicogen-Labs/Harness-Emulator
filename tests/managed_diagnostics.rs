mod protocol {
    pub use silicogen_terminal_runtime::protocol::LaunchSpec;
}

#[path = "../src/diagnostics.rs"]
#[allow(dead_code)]
mod diagnostics;

use diagnostics::{
    ContainmentPolicy, DiagnosticsManager, EffectiveContext, Lifecycle, Limits, OutputStream,
    OwnerKey, SelectedWorkspace,
};
use silicogen_terminal_runtime::protocol::LaunchSpec;
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

fn owner(session: &str, run: &str) -> OwnerKey {
    OwnerKey::new(session, run).unwrap()
}

fn test_tempdir() -> tempfile::TempDir {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".runtime/managed-diagnostics-tests");
    std::fs::create_dir_all(&root).unwrap();
    tempfile::Builder::new()
        .prefix("case-")
        .tempdir_in(root)
        .unwrap()
}

fn spec(program: &str, args: &[&str], label: &str) -> LaunchSpec {
    LaunchSpec {
        name: Some(label.into()),
        program: program.into(),
        args: args.iter().map(|value| (*value).into()).collect(),
        cwd: "/".into(),
        env: BTreeMap::from([("MCP_ENV_SECRET".into(), "must-not-be-used".into())]),
        cols: 80,
        rows: 24,
        record_output: false,
    }
}

fn context(cwd: &Path, environment: BTreeMap<String, String>) -> EffectiveContext {
    EffectiveContext::from_validated(cwd.to_path_buf(), environment, ContainmentPolicy::Host)
}

fn manager_with(mut limits: Limits) -> DiagnosticsManager {
    limits.termination_grace = Duration::from_millis(80);
    DiagnosticsManager::new(limits).unwrap()
}

fn wait_finished(manager: &DiagnosticsManager, owner: &OwnerKey, id: &str) -> diagnostics::Status {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let status = manager.status(owner, id).unwrap();
        if status.lifecycle != Lifecycle::Running {
            return status;
        }
        assert!(
            Instant::now() < deadline,
            "diagnostic process did not finish within test deadline"
        );
        thread::sleep(Duration::from_millis(10));
    }
}

fn helper_synthetic_server() {
    if std::env::var_os("MANAGED_DIAGNOSTICS_SERVER_HELPER").as_deref()
        != Some(std::ffi::OsStr::new("1"))
    {
        return;
    }
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    println!("{}", listener.local_addr().unwrap());
    std::io::stdout().flush().unwrap();
    for mut stream in listener.incoming().flatten() {
        let mut request = [0_u8; 1024];
        let _ = stream.read(&mut request);
        let _ = stream
            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok");
    }
}

#[test]
fn external_synthetic_server_helper() {
    helper_synthetic_server();
}

#[test]
fn stopping_owner_stops_owned_child_but_leaves_separately_started_server_untouched() {
    let dir = test_tempdir();
    let mut external = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "external_synthetic_server_helper", "--nocapture"])
        .env("MANAGED_DIAGNOSTICS_SERVER_HELPER", "1")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut external_out = BufReader::new(external.stdout.take().unwrap());
    let address = loop {
        let mut line = String::new();
        assert_ne!(
            external_out.read_line(&mut line).unwrap(),
            0,
            "synthetic server helper exited before binding"
        );
        if line.trim().parse::<std::net::SocketAddr>().is_ok() {
            break line;
        }
    };
    let address = address.trim();

    let manager = DiagnosticsManager::new(Limits::default()).unwrap();
    let owner = owner("session_owned", "run_one");
    let started = manager
        .start(
            owner.clone(),
            &spec("/bin/sleep", &["60"], "sleep-owned"),
            context(dir.path(), BTreeMap::new()),
        )
        .unwrap();
    assert_eq!(started.backend, "host");
    assert!(!started.detached_children_contained);
    assert_eq!(manager.stop_owner(&owner).unwrap(), 1);
    assert!(
        manager.status(&owner, &started.id).is_err(),
        "stop_owner removes the stopped owner's entries"
    );

    let mut response = TcpStream::connect(address).unwrap();
    response
        .write_all(b"GET / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .unwrap();
    let mut result = String::new();
    response.read_to_string(&mut result).unwrap();
    assert!(result.contains("200 OK"));
    assert!(
        external.try_wait().unwrap().is_none(),
        "manager affected an unrelated server child"
    );
    external.kill().unwrap();
    external.wait().unwrap();
}

#[test]
fn dropping_manager_stops_its_remaining_owned_processes() {
    let dir = test_tempdir();
    let owner = owner("drop_session", "drop_run");
    let marker = dir.path().join("late-output-marker");
    let script = format!(
        "printf ready; /bin/sleep 0.3; printf late > '{}'",
        marker.display()
    );
    let manager = DiagnosticsManager::new(Limits::default()).unwrap();
    let started = manager
        .start(
            owner.clone(),
            &spec("/bin/sh", &["-c", script.as_str()], "drop-cleanup"),
            context(dir.path(), BTreeMap::new()),
        )
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(1);
    loop {
        let output = manager
            .read_output(&owner, &started.id, OutputStream::Stdout, 0, 128)
            .unwrap();
        if output.text.contains("ready") {
            break;
        }
        assert!(Instant::now() < deadline, "owned process did not start");
        thread::sleep(Duration::from_millis(5));
    }
    drop(manager);
    thread::sleep(Duration::from_millis(350));
    assert!(!marker.exists(), "owned descendant survived manager drop");
}

#[test]
fn timeouts_and_explicit_cancellation_stop_only_the_selected_owned_run() {
    let dir = test_tempdir();
    let limits = Limits {
        max_runtime: Duration::from_millis(120),
        ..Limits::default()
    };
    let manager = manager_with(limits);
    let a = owner("session_a", "run_a");
    let b = owner("session_a", "run_b");
    let timed = manager
        .start(
            a.clone(),
            &spec("/bin/sleep", &["60"], "timeout"),
            context(dir.path(), BTreeMap::new()),
        )
        .unwrap();
    assert_eq!(
        wait_finished(&manager, &a, &timed.id).lifecycle,
        Lifecycle::TimedOut
    );
    let cancellable = manager
        .start(
            b.clone(),
            &spec("/bin/sleep", &["60"], "cancel"),
            context(dir.path(), BTreeMap::new()),
        )
        .unwrap();
    assert_eq!(
        manager.cancel(&b, &cancellable.id).unwrap().lifecycle,
        Lifecycle::Cancelled
    );
}

#[test]
fn run_boundary_rejects_reads_and_stops_from_another_run() {
    let dir = test_tempdir();
    let manager = DiagnosticsManager::new(Limits::default()).unwrap();
    let first = owner("same_session", "run_first");
    let second = owner("same_session", "run_second");
    let started = manager
        .start(
            first.clone(),
            &spec("/bin/sleep", &["60"], "boundary"),
            context(dir.path(), BTreeMap::new()),
        )
        .unwrap();
    assert!(manager.status(&second, &started.id).is_err());
    assert!(
        manager
            .read_output(&second, &started.id, OutputStream::Stdout, 0, 20)
            .is_err()
    );
    assert_eq!(manager.stop_owner(&second).unwrap(), 0);
    assert_eq!(
        manager.status(&first, &started.id).unwrap().lifecycle,
        Lifecycle::Running
    );
    assert_eq!(manager.stop_owner(&first).unwrap(), 1);
}

#[test]
fn concurrent_starts_reserve_the_single_process_slot_before_spawning() {
    let dir = test_tempdir();
    let limits = Limits {
        max_processes: 1,
        ..Limits::default()
    };
    let manager = std::sync::Arc::new(DiagnosticsManager::new(limits).unwrap());
    let barrier = std::sync::Arc::new(std::sync::Barrier::new(8));
    let workers: Vec<_> = (0..8)
        .map(|n| {
            let manager = manager.clone();
            let barrier = barrier.clone();
            let cwd = dir.path().to_path_buf();
            thread::spawn(move || {
                barrier.wait();
                manager.start(
                    owner("racing_session", "racing_run"),
                    &spec("/bin/sleep", &["60"], &format!("race-{n}")),
                    context(&cwd, BTreeMap::new()),
                )
            })
        })
        .collect();
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
                .is_some_and(|error| error.to_string().contains("diagnostics_process_limit")))
            .count(),
        7
    );
    assert_eq!(
        manager
            .stop_owner(&owner("racing_session", "racing_run"))
            .unwrap(),
        1
    );
}

#[test]
fn cumulative_process_count_is_bounded_after_children_exit() {
    let dir = test_tempdir();
    let limits = Limits {
        max_total_processes: 2,
        ..Limits::default()
    };
    let manager = DiagnosticsManager::new(limits).unwrap();
    let owner = owner("total_count_session", "total_count_run");
    for _ in 0..2 {
        let started = manager
            .start(
                owner.clone(),
                &spec("/bin/true", &[], "bounded-count"),
                context(dir.path(), BTreeMap::new()),
            )
            .unwrap();
        assert_eq!(
            wait_finished(&manager, &owner, &started.id).lifecycle,
            Lifecycle::Exited
        );
    }
    let error = manager
        .start(
            owner,
            &spec("/bin/true", &[], "over-limit"),
            context(dir.path(), BTreeMap::new()),
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("diagnostics_total_process_limit")
    );
}

#[test]
fn host_backend_preserves_the_validated_argument_vector() {
    let dir = test_tempdir();
    let manager = DiagnosticsManager::new(Limits::default()).unwrap();
    let owner = owner("argv_session", "argv_run");
    let started = manager
        .start(
            owner.clone(),
            &spec("/bin/echo", &["arg-one", "arg-two"], "argv"),
            context(dir.path(), BTreeMap::new()),
        )
        .unwrap();
    assert_eq!(started.argv_count, 3);
    assert_eq!(
        wait_finished(&manager, &owner, &started.id).lifecycle,
        Lifecycle::Exited
    );
    let output = manager
        .read_output(&owner, &started.id, OutputStream::Stdout, 0, 128)
        .unwrap();
    assert_eq!(output.text, "arg-one arg-two\n");
}

#[test]
fn fast_exit_drains_exact_stdout_and_stderr_repeatedly() {
    let dir = test_tempdir();
    let manager = DiagnosticsManager::new(Limits::default()).unwrap();
    let owner = owner("fast_io_session", "fast_io_run");
    for n in 0..24 {
        let started = manager
            .start(
                owner.clone(),
                &spec(
                    "/bin/sh",
                    &["-c", "printf out-marker; printf err-marker >&2"],
                    "fast-io",
                ),
                context(dir.path(), BTreeMap::new()),
            )
            .unwrap();
        let status = wait_finished(&manager, &owner, &started.id);
        assert_eq!(status.lifecycle, Lifecycle::Exited, "iteration {n}");
        assert_eq!(
            status.capture_complete,
            Some(true),
            "iteration {n}: {status:?}"
        );
        assert!(status.capture_gaps.is_empty());
        assert_eq!(
            manager
                .read_output(&owner, &started.id, OutputStream::Stdout, 0, 128)
                .unwrap()
                .text,
            "out-marker"
        );
        assert_eq!(
            manager
                .read_output(&owner, &started.id, OutputStream::Stderr, 0, 128)
                .unwrap()
                .text,
            "err-marker"
        );
    }
    manager.stop_owner(&owner).unwrap();
}

#[test]
fn detached_pipe_holder_gets_bounded_drain_and_explicit_capture_gap() {
    let dir = test_tempdir();
    let limits = Limits {
        output_drain_timeout: Duration::from_millis(40),
        ..Limits::default()
    };
    let manager = manager_with(limits);
    let owner = owner("drain_gap_session", "drain_gap_run");
    let script = "/usr/bin/setsid /bin/sleep 0.5 >/dev/stdout 2>/dev/stderr & printf fast-out; printf fast-err >&2";
    let started = manager
        .start(
            owner.clone(),
            &spec("/bin/sh", &["-c", script], "pipe-holder"),
            context(dir.path(), BTreeMap::new()),
        )
        .unwrap();
    let before = Instant::now();
    let status = wait_finished(&manager, &owner, &started.id);
    assert!(
        before.elapsed() < Duration::from_secs(1),
        "reader drain exceeded its configured bound"
    );
    assert_eq!(status.lifecycle, Lifecycle::Exited);
    assert_eq!(status.capture_complete, Some(false));
    assert!(!status.capture_gaps.is_empty());
    assert!(matches!(
        status.capture_gap_reason,
        Some(
            diagnostics::CaptureGapReason::PipeStillOpenAtStop
                | diagnostics::CaptureGapReason::DrainDeadline
        )
    ));
    assert_eq!(
        manager
            .read_output(&owner, &started.id, OutputStream::Stdout, 0, 128)
            .unwrap()
            .text,
        "fast-out"
    );
    assert_eq!(
        manager
            .read_output(&owner, &started.id, OutputStream::Stderr, 0, 128)
            .unwrap()
            .text,
        "fast-err"
    );
}

#[test]
fn output_is_bounded_and_overflow_fails_closed() {
    let dir = test_tempdir();
    let limits = Limits {
        max_total_output_bytes: 4096,
        max_output_bytes_per_process: 4096,
        max_retained_bytes_per_stream: 128,
        ..Limits::default()
    };
    let manager = manager_with(limits);
    let owner = owner("output_session", "output_run");
    let started = manager
        .start(
            owner.clone(),
            &spec("/usr/bin/yes", &["bounded"], "output"),
            context(dir.path(), BTreeMap::new()),
        )
        .unwrap();
    let status = wait_finished(&manager, &owner, &started.id);
    assert_eq!(status.lifecycle, Lifecycle::OutputLimitExceeded);
    assert!(status.output_bytes <= 4096);
    let chunk = manager
        .read_output(&owner, &started.id, OutputStream::Stdout, 0, 128)
        .unwrap();
    assert!(chunk.truncated_before_cursor);
    assert!(chunk.text.contains("bounded"));
    assert!(chunk.total_bytes_seen <= 4096);
}

#[test]
fn environment_is_cleared_and_only_effective_values_reach_the_child_or_receipt() {
    let dir = test_tempdir();
    unsafe {
        std::env::set_var("DIAGNOSTICS_AMBIENT_SENTINEL", "ambient-must-not-leak");
    }
    let manager = DiagnosticsManager::new(Limits::default()).unwrap();
    let owner = owner("env_session", "env_run");
    let mut effective = BTreeMap::new();
    effective.insert("ONLY_EFFECTIVE".into(), "fixture-value".into());
    let started = manager
        .start(
            owner.clone(),
            &spec("/usr/bin/env", &[], "environment"),
            context(dir.path(), effective),
        )
        .unwrap();
    assert!(!format!("{started:?}").contains("must-not-be-used"));
    assert_eq!(
        wait_finished(&manager, &owner, &started.id).lifecycle,
        Lifecycle::Exited
    );
    let output = manager
        .read_output(&owner, &started.id, OutputStream::Stdout, 0, 4096)
        .unwrap();
    assert_eq!(output.text.trim(), "ONLY_EFFECTIVE=fixture-value");
    unsafe {
        std::env::remove_var("DIAGNOSTICS_AMBIENT_SENTINEL");
    }
}

#[test]
fn host_backend_explicitly_reports_detached_children_are_not_contained() {
    let dir = test_tempdir();
    let manager = DiagnosticsManager::new(Limits::default()).unwrap();
    let owner = owner("host_scope", "host_run");
    let started = manager
        .start(
            owner,
            &spec("/bin/true", &[], "host-capability"),
            context(dir.path(), BTreeMap::new()),
        )
        .unwrap();
    assert_eq!(started.backend, "host");
    assert!(!started.detached_children_contained);
}

#[test]
fn bubblewrap_pid_namespace_is_owned_and_cleaned_when_supported() {
    let bwrap = match find_bwrap() {
        Some(path) => path,
        None => {
            eprintln!("SKIP: bwrap executable unavailable");
            return;
        }
    };
    if !Path::new("/usr/bin/setsid").is_file() {
        eprintln!("SKIP: setsid helper unavailable");
        return;
    }
    let probe = Command::new(&bwrap)
        .args([
            "--die-with-parent",
            "--unshare-user",
            "--unshare-pid",
            "--ro-bind",
            "/",
            "/",
            "--dev",
            "/dev",
            "--proc",
            "/proc",
            "--tmpfs",
            "/tmp",
            "--",
            "/bin/true",
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    if !probe.is_ok_and(|status| status.success()) {
        eprintln!("SKIP: required Bubblewrap PID namespace unavailable");
        return;
    }
    let dir = test_tempdir();
    let limits = Limits {
        max_runtime: Duration::from_secs(5),
        ..Limits::default()
    };
    let manager = manager_with(limits);
    let owner = owner("namespace_session", "namespace_run");
    let marker = dir.path().join("namespace-marker");
    let marker_name = marker.to_string_lossy();
    let script = format!(
        "(/usr/bin/setsid /bin/sh -c 'printf started > {marker_name}; /bin/sleep 0.5; printf late >> {marker_name}') & while [ ! -f {marker_name} ]; do /bin/sleep 0.01; done"
    );
    let strict_context = EffectiveContext::from_validated(
        dir.path().to_path_buf(),
        BTreeMap::new(),
        ContainmentPolicy::BubblewrapPidNamespace {
            executable: bwrap,
            writable_roots: vec![dir.path().to_path_buf()],
            unshare_network: false,
        },
    );
    let started = manager
        .start(
            owner.clone(),
            &spec("/bin/sh", &["-c", script.as_str()], "strict-namespace"),
            strict_context,
        )
        .unwrap();
    assert!(started.detached_children_contained);
    assert_eq!(
        wait_finished(&manager, &owner, &started.id).lifecycle,
        Lifecycle::Exited
    );
    assert_eq!(std::fs::read_to_string(&marker).unwrap(), "started");
    thread::sleep(Duration::from_millis(600));
    assert_eq!(
        std::fs::read_to_string(&marker).unwrap(),
        "started",
        "detached namespace child survived leader exit"
    );
    assert_eq!(manager.stop_owner(&owner).unwrap(), 1);
    assert!(manager.status(&owner, &started.id).is_err());
}

#[test]
fn workspace_seed_is_relative_bounded_create_new_and_hash_cas() {
    let dir = test_tempdir();
    let workspace = dir.path().join("copied-workspace");
    std::fs::create_dir(&workspace).unwrap();
    let selected = SelectedWorkspace::from_validated(workspace.clone());
    let manager = DiagnosticsManager::new(Limits::default()).unwrap();
    let created = manager
        .seed_file(&selected, Path::new("nested/note.txt"), b"before\n", None)
        .unwrap();
    assert!(!created.overwritten);
    assert!(
        manager
            .seed_file(&selected, Path::new("nested/note.txt"), b"other", None)
            .is_err()
    );
    assert!(
        manager
            .seed_file(
                &selected,
                Path::new("nested/note.txt"),
                b"wrong",
                Some(&"0".repeat(64))
            )
            .is_err()
    );
    assert_eq!(
        std::fs::read(workspace.join("nested/note.txt")).unwrap(),
        b"before\n"
    );
    let hardlink_sentinel = dir.path().join("hardlink-sentinel");
    std::fs::hard_link(workspace.join("nested/note.txt"), &hardlink_sentinel).unwrap();
    assert!(
        manager
            .seed_file(
                &selected,
                Path::new("nested/note.txt"),
                b"must-not-change-host-link",
                Some(&created.sha256)
            )
            .is_err()
    );
    assert_eq!(std::fs::read(&hardlink_sentinel).unwrap(), b"before\n");
    std::fs::remove_file(&hardlink_sentinel).unwrap();
    let replaced = manager
        .seed_file(
            &selected,
            Path::new("nested/note.txt"),
            b"after\n",
            Some(&created.sha256),
        )
        .unwrap();
    assert!(replaced.overwritten);
    assert_eq!(
        std::fs::read(workspace.join("nested/note.txt")).unwrap(),
        b"after\n"
    );
    assert!(
        manager
            .seed_file(&selected, Path::new("../../escape"), b"bad", None)
            .is_err()
    );
    assert!(
        manager
            .seed_file(
                &selected,
                Path::new("large"),
                &vec![b'x'; 1024 * 1024 + 1],
                None
            )
            .is_err()
    );

    let outside = dir.path().join("outside");
    std::fs::write(&outside, b"sentinel").unwrap();
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(&outside, workspace.join("escape-link")).unwrap();
        assert!(
            manager
                .seed_file(&selected, Path::new("escape-link"), b"overwrite", None)
                .is_err()
        );
        assert_eq!(std::fs::read(&outside).unwrap(), b"sentinel");
    }
}

fn find_bwrap() -> Option<PathBuf> {
    std::env::split_paths(&std::env::var_os("PATH")?)
        .map(|path| path.join("bwrap"))
        .find(|path| path.is_file())
        .and_then(|path| std::fs::canonicalize(path).ok())
}
