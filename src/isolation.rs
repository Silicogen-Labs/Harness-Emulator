//! Process and filesystem confinement for launched workloads.

use crate::protocol::LaunchSpec;
use anyhow::{Context, Result, bail, ensure};
use rmcp::schemars;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum IsolationBackend {
    #[default]
    Host,
    Bubblewrap,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
#[serde(rename_all = "lowercase")]
pub enum NetworkPolicy {
    #[default]
    Host,
    None,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct IsolationSpec {
    pub backend: IsolationBackend,
    pub network: NetworkPolicy,
}

/// Wrap an argv launch in bubblewrap when requested. `state_root` must be the
/// private writable root allocated to this session, not the daemon's shared
/// runtime directory.
pub fn wrap_command(
    spec: &LaunchSpec,
    state_root: &Path,
    workspace: Option<&crate::workspace::WorkspaceManifest>,
    isolation: &IsolationSpec,
) -> Result<LaunchSpec> {
    if isolation.backend == IsolationBackend::Host {
        ensure!(
            isolation.network == NetworkPolicy::Host,
            "network=none requires the bubblewrap isolation backend; refusing unrestricted host launch"
        );
        return Ok(spec.clone());
    }
    // The dynamic loader starts the broker before Bubblewrap establishes its
    // namespaces. These variables must not inject code into that host process.
    for name in spec.env.keys() {
        crate::profiles::validate_test_environment_name(name)?;
    }
    ensure!(
        cfg!(target_os = "linux"),
        "bubblewrap isolation is supported only on Linux"
    );
    let bwrap = find_bwrap().context("bubblewrap isolation requested but bwrap is unavailable")?;
    probe_bwrap(&bwrap, isolation.network == NetworkPolicy::None).context(
        "bubblewrap, requested namespaces, or unprivileged user namespaces are unavailable",
    )?;

    let state_root = fs::canonicalize(state_root)
        .with_context(|| format!("canonicalizing session state root {}", state_root.display()))?;
    ensure!(state_root.is_dir(), "session state root is not a directory");
    ensure!(
        Path::new(&spec.cwd).is_absolute(),
        "bubblewrap launch cwd must be absolute"
    );
    let mut args: Vec<String> = vec![
        "--die-with-parent".into(),
        "--new-session".into(),
        "--unshare-user".into(),
        "--unshare-pid".into(),
    ];
    if isolation.network == NetworkPolicy::None {
        args.push("--unshare-net".into());
    }
    args.extend(["--ro-bind".into(), "/".into(), "/".into()]);
    args.extend(["--dev".into(), "/dev".into()]);
    args.extend(["--proc".into(), "/proc".into()]);
    args.extend(["--tmpfs".into(), "/tmp".into()]);
    add_tmp_mountpoint_dirs(&mut args, &state_root);
    args.extend([
        "--bind".into(),
        state_root.display().to_string(),
        state_root.display().to_string(),
    ]);
    if let Some(workspace) = workspace {
        let workspace_root = fs::canonicalize(&workspace.root)
            .with_context(|| format!("canonicalizing workspace {}", workspace.root.display()))?;
        ensure!(workspace_root.is_dir(), "workspace root is not a directory");
        ensure!(
            !workspace_root.starts_with(&state_root) && !state_root.starts_with(&workspace_root),
            "session state and workspace roots must not overlap"
        );
        add_tmp_mountpoint_dirs(&mut args, &workspace_root);
        args.extend([
            "--bind".into(),
            workspace_root.display().to_string(),
            workspace_root.display().to_string(),
        ]);
    }
    args.extend([
        "--chdir".into(),
        spec.cwd.clone(),
        "--".into(),
        spec.program.clone(),
    ]);
    args.extend(spec.args.clone());

    Ok(LaunchSpec {
        name: spec.name.clone(),
        program: bwrap.display().to_string(),
        args,
        cwd: "/".to_owned(),
        env: spec.env.clone(),
        cols: spec.cols,
        rows: spec.rows,
        record_output: spec.record_output,
    })
}

fn add_tmp_mountpoint_dirs(args: &mut Vec<String>, path: &Path) {
    let tmp = Path::new("/tmp");
    let Ok(relative) = path.strip_prefix(tmp) else {
        return;
    };
    let mut current = tmp.to_path_buf();
    for component in relative.components() {
        current.push(component.as_os_str());
        args.extend(["--dir".into(), current.display().to_string()]);
    }
}

/// Report the requested/effective backend and the result of a non-mutating
/// capability probe. A failed probe never changes the launch to host mode.
pub fn capabilities(isolation: &IsolationSpec) -> Value {
    let host_network = isolation.network == NetworkPolicy::Host;
    if isolation.backend == IsolationBackend::Host {
        return json!({
            "backend": "host",
            "effective_backend": if host_network { "host" } else { "unavailable" },
            "filesystem_confined": false,
            "network": if host_network { "host" } else { "unavailable" },
            "network_confined": false,
            "process_namespace": false,
            "available": host_network,
            "reason": if host_network { Value::Null } else { json!("network=none cannot be enforced by the host backend") },
        });
    }
    let probe = find_bwrap().and_then(|path| probe_bwrap(&path, !host_network).map(|_| path));
    match probe {
        Ok(path) => json!({
            "backend": "bubblewrap",
            "effective_backend": "bubblewrap",
            "filesystem_confined": true,
            "network": if host_network { "host" } else { "none" },
            "network_confined": !host_network,
            "process_namespace": true,
            "available": true,
            "executable": path,
            "reason": Value::Null,
        }),
        Err(error) => json!({
            "backend": "bubblewrap",
            "effective_backend": "unavailable",
            "filesystem_confined": false,
            "network": if host_network { "host" } else { "none" },
            "network_confined": false,
            "process_namespace": false,
            "available": false,
            "reason": error.to_string(),
        }),
    }
}

pub(crate) fn find_bwrap() -> Result<PathBuf> {
    if let Some(path) = std::env::var_os("PATH") {
        for directory in std::env::split_paths(&path) {
            let candidate = directory.join("bwrap");
            if candidate.is_file() {
                return fs::canonicalize(candidate).context("resolving bwrap executable");
            }
        }
    }
    bail!("bwrap executable was not found in PATH")
}

fn probe_bwrap(path: &Path, isolate_network: bool) -> Result<()> {
    let mut command = Command::new(path);
    command.args([
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
    ]);
    if isolate_network {
        command.arg("--unshare-net");
    }
    let output = command
        .args(["--", "/bin/true"])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .output()
        .context("running bubblewrap user-namespace probe")?;
    ensure!(
        output.status.success(),
        "bubblewrap probe failed: {}",
        String::from_utf8_lossy(&output.stderr).trim()
    );
    Ok(())
}
