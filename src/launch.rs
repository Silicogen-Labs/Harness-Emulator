use anyhow::{Context, Result, bail};
use serde_json::{Value, json};
use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};

use crate::config::{LaunchOptions, ShellSpec, StateMode, StateSeed};
use crate::profiles::{
    Profile, ProfileEnvValue, ensure_no_symlink_components, ensure_private_dir, load_profile,
    validate_env_name,
};
use crate::protocol::LaunchSpec;

const MANAGED_STATE_VARS: &[&str] = &[
    "HOME",
    "XDG_DATA_HOME",
    "XDG_CONFIG_HOME",
    "XDG_CACHE_HOME",
    "XDG_STATE_HOME",
    "TMPDIR",
    "TMP",
    "TEMP",
];
const MANAGED_TERMINAL_VARS: &[(&str, &str)] = &[
    ("TERM", "xterm-256color"),
    ("COLORTERM", "truecolor"),
    ("TERM_PROGRAM", "silicogen-emulation"),
];

#[derive(Debug, Clone)]
pub struct PreparedLaunch {
    /// Command and other launch properties after profile/shell resolution.
    pub spec: LaunchSpec,
    /// Complete child environment. Integrators should clear the inherited
    /// environment and install exactly this map.
    pub environment: BTreeMap<String, String>,
    /// Session-owned HOME/XDG/TMP root, containing independent seed copies.
    pub state_root: PathBuf,
    /// Non-secret effective settings; environment values are never included.
    pub diagnostics: Value,
}

/// Size of the selected independent state seed copies. The scan follows the
/// same SQLite/WAL and credential rules as the copy operation.
pub fn estimate_state_seed_bytes(runtime_root: &Path, options: &LaunchOptions) -> Result<u64> {
    let profile = match (&options.state_mode, options.profile.as_deref()) {
        (StateMode::CopyUser, _) => None,
        (StateMode::Profile, Some(name)) => Some(load_profile(runtime_root, name)?),
        (StateMode::Profile, None) => bail!("state_mode=profile requires a named profile"),
        (_, Some(_)) => bail!("a named profile requires state_mode=profile"),
        (_, None) => None,
    };
    let seeds = match (&options.state_mode, profile.as_ref()) {
        (StateMode::CopyUser, _) => &options.seeds,
        (StateMode::Profile, Some(profile)) => &profile.seeds,
        _ => return Ok(0),
    };
    seeds.iter().try_fold(0_u64, |total, seed| {
        validate_seed_destination(&seed.destination)?;
        Ok(total.saturating_add(estimate_seed(&seed.source, seed.include_credentials)?))
    })
}

fn estimate_seed(path: &Path, include_credentials: bool) -> Result<u64> {
    if !path.is_absolute() {
        bail!(
            "state seed source must be an explicit absolute path: {}",
            path.display()
        )
    }
    ensure_no_symlink_components(path)?;
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        bail!("state seed source may not be a symlink: {}", path.display())
    }
    if path_looks_like_credential(path) && !include_credentials {
        bail!(
            "state seed {} looks like credential data; set include_credentials=true to opt in",
            path.display()
        )
    }
    if metadata.is_file() {
        return if is_sqlite_or_journal(path) || has_sqlite_header(path)? {
            Ok(0)
        } else {
            Ok(metadata.len())
        };
    }
    if !metadata.is_dir() {
        bail!(
            "state seed source must be a regular file or directory: {}",
            path.display()
        )
    }
    let mut total = 0_u64;
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let child = entry.path();
        let kind = entry.file_type()?;
        if kind.is_symlink() {
            bail!("refusing symlink in state seed: {}", child.display())
        }
        if is_sqlite_or_journal(&child) || (kind.is_file() && has_sqlite_header(&child)?) {
            continue;
        }
        total = total.saturating_add(estimate_seed(&child, include_credentials)?);
    }
    Ok(total)
}

pub fn prepare_launch(
    runtime_root: &Path,
    session_id: &str,
    spec: &LaunchSpec,
    options: &LaunchOptions,
) -> Result<PreparedLaunch> {
    prepare_launch_with_seed_limit(runtime_root, session_id, spec, options, None)
}

pub fn prepare_launch_with_seed_limit(
    runtime_root: &Path,
    session_id: &str,
    spec: &LaunchSpec,
    options: &LaunchOptions,
    max_seed_bytes: Option<u64>,
) -> Result<PreparedLaunch> {
    validate_session_id(session_id)?;
    validate_launch_options(spec, options)?;

    let profile = match (&options.state_mode, options.profile.as_deref()) {
        (StateMode::Profile, Some(name)) => Some(load_profile(runtime_root, name)?),
        (StateMode::Profile, None) => bail!("state_mode=profile requires a named profile"),
        (_, Some(_)) => bail!("a named profile requires state_mode=profile"),
        (_, None) => None,
    };
    if let Some(profile) = &profile
        && profile.name != options.profile.as_deref().unwrap_or_default()
    {
        bail!("profile name does not match its profile directory; reload or repair the profile");
    }

    let session_root = runtime_root.join("sessions").join(session_id);
    ensure_private_dir(&runtime_root.join("sessions"))?;
    ensure_no_symlink_components(&session_root)?;
    let state_root = session_root.join("state");
    ensure_no_symlink_components(&state_root)?;
    if state_root.exists() {
        bail!(
            "session state already exists for {session_id}; use a new session id to avoid reusing prior state"
        );
    }
    ensure_private_dir(&session_root)?;
    ensure_private_dir(&state_root)?;
    let roots = create_state_roots(&state_root)?;

    let seeds: &[StateSeed] = match (&options.state_mode, &profile) {
        (StateMode::CopyUser, _) => &options.seeds,
        (StateMode::Profile, Some(profile)) => &profile.seeds,
        _ => &[],
    };
    let mut copied_seed_bytes = 0_u64;
    for seed in seeds {
        copy_seed(seed, &state_root, &mut copied_seed_bytes, max_seed_bytes)?;
    }

    let (mut environment, mut sources) = resolve_base_environment(profile.as_ref(), options)?;
    if let Some(profile) = &profile {
        apply_profile_environment(profile, &mut environment, &mut sources)?;
    }
    for (key, value) in &spec.env {
        if MANAGED_STATE_VARS.contains(&key.as_str()) {
            bail!(
                "{key} is managed by the selected state mode; remove the explicit override and use a profile seed instead"
            );
        }
        environment.insert(key.clone(), value.clone());
        sources.insert(key.clone(), "explicit".to_owned());
    }
    for key in &options.env_unset {
        environment.remove(key);
        sources.insert(key.clone(), "explicitly_unset".to_owned());
    }
    for (name, path) in &roots {
        environment.insert(name.clone(), path.to_string_lossy().into_owned());
        sources.insert(name.clone(), "managed_state".to_owned());
    }
    for (name, value) in MANAGED_TERMINAL_VARS {
        environment.insert((*name).to_owned(), (*value).to_owned());
        sources.insert((*name).to_owned(), "managed_terminal".to_owned());
    }

    let mut resolved_spec = spec.clone();
    if let Some(shell) = &options.shell {
        resolve_shell(&mut resolved_spec, shell)?;
    }
    resolved_spec.env = environment.clone();
    let state_mode = match &options.state_mode {
        StateMode::Isolated => "isolated",
        StateMode::CopyUser => "copy_user",
        StateMode::Profile => "profile",
    };
    let diagnostics = json!({
        "state_mode": state_mode,
        "state_root": state_root,
        "profile": profile.as_ref().map(|p| p.name.clone()),
        "environment_sources": sources,
        "environment_names": environment.keys().cloned().collect::<Vec<_>>(),
        "seed_count": seeds.len(),
        "credential_seed_count": seeds.iter().filter(|seed| seed.include_credentials).count(),
        "shell": options.shell.as_ref().map(|shell| json!({ "executable": shell.executable, "login": shell.login })),
    });
    Ok(PreparedLaunch {
        spec: resolved_spec,
        environment,
        state_root,
        diagnostics,
    })
}

fn validate_launch_options(spec: &LaunchSpec, options: &LaunchOptions) -> Result<()> {
    for name in spec.env.keys() {
        validate_env_name(name)?;
    }
    let mut unset = BTreeSet::new();
    for name in &options.env_unset {
        validate_env_name(name)?;
        if MANAGED_STATE_VARS.contains(&name.as_str()) {
            bail!("{name} is a managed state variable and cannot be removed");
        }
        if !unset.insert(name) {
            bail!("environment variable {name} appears more than once in env_unset");
        }
    }
    if let Some(names) = &options.env_inherit {
        for name in names {
            validate_env_name(name)?;
        }
    }
    if options.shell.is_some() && !spec.args.is_empty() {
        bail!("shell and executable argv are mutually exclusive; clear args when specifying shell");
    }
    if matches!(&options.state_mode, StateMode::Isolated) && !options.seeds.is_empty() {
        bail!(
            "state seeds require state_mode=copy_user; isolated mode intentionally starts with empty app state"
        );
    }
    if let Some(shell) = &options.shell {
        if shell.executable.trim().is_empty() {
            bail!("shell executable must not be empty");
        }
        if shell.script.is_empty() {
            bail!("shell script must not be empty");
        }
    } else if spec.program.trim().is_empty() {
        bail!("program must not be empty unless an explicit shell is configured");
    }
    Ok(())
}

fn resolve_base_environment(
    profile: Option<&Profile>,
    options: &LaunchOptions,
) -> Result<(BTreeMap<String, String>, BTreeMap<String, String>)> {
    let mut env = BTreeMap::new();
    let mut sources = BTreeMap::new();
    if profile.is_none() && options.env_inherit.is_none() {
        for (key, value) in std::env::vars() {
            env.insert(key.clone(), value);
            sources.insert(key, "daemon_ambient".into());
        }
    } else {
        let mut names = BTreeSet::new();
        if let Some(profile) = profile {
            for name in &profile.env_allowlist {
                names.insert(name.clone());
            }
        }
        if let Some(inherit) = &options.env_inherit {
            for name in inherit {
                names.insert(name.clone());
            }
        }
        // Retain the minimum needed to locate programs and communicate with a
        // Unix terminal even when an explicit allowlist is supplied.
        names.extend(["PATH".to_owned(), "LANG".to_owned(), "LC_ALL".to_owned()]);
        for name in names {
            if let Ok(value) = std::env::var(&name) {
                env.insert(name.clone(), value);
                sources.insert(name, "daemon_allowlist".into());
            }
        }
    }
    Ok((env, sources))
}

fn apply_profile_environment(
    profile: &Profile,
    env: &mut BTreeMap<String, String>,
    sources: &mut BTreeMap<String, String>,
) -> Result<()> {
    for (name, value) in &profile.env {
        if MANAGED_STATE_VARS.contains(&name.as_str()) {
            bail!(
                "profile variable {name} conflicts with a managed state path; remove it from profile {}",
                profile.name
            );
        }
        match value {
            ProfileEnvValue::Literal { value } => {
                env.insert(name.clone(), value.clone());
                sources.insert(name.clone(), "profile".into());
            }
            ProfileEnvValue::HostReference { variable, secret } => {
                let value = std::env::var(variable).with_context(|| format!("profile {} requires daemon environment variable {variable}; set it in the daemon environment and restart the daemon", profile.name))?;
                env.insert(name.clone(), value);
                sources.insert(
                    name.clone(),
                    if *secret {
                        "profile_secret_reference"
                    } else {
                        "profile_host_reference"
                    }
                    .into(),
                );
            }
        }
    }
    Ok(())
}

fn resolve_shell(spec: &mut LaunchSpec, shell: &ShellSpec) -> Result<()> {
    spec.program = shell.executable.clone();
    spec.args = if shell.login {
        vec!["--login".into(), "-c".into(), shell.script.clone()]
    } else {
        vec!["-c".into(), shell.script.clone()]
    };
    Ok(())
}

fn create_state_roots(state_root: &Path) -> Result<Vec<(String, PathBuf)>> {
    let roots = ["home", "data", "config", "cache", "state", "tmp"];
    let mut env_roots = Vec::new();
    for root in roots {
        let path = state_root.join(root);
        ensure_private_dir(&path)?;
        let env_name = match root {
            "home" => "HOME",
            "data" => "XDG_DATA_HOME",
            "config" => "XDG_CONFIG_HOME",
            "cache" => "XDG_CACHE_HOME",
            "state" => "XDG_STATE_HOME",
            "tmp" => "TMPDIR",
            _ => unreachable!(),
        };
        env_roots.push((env_name.to_owned(), path));
    }
    let tmp = state_root.join("tmp");
    env_roots.push(("TMP".into(), tmp.clone()));
    env_roots.push(("TEMP".into(), tmp));
    Ok(env_roots)
}

fn copy_seed(
    seed: &StateSeed,
    state_root: &Path,
    copied_bytes: &mut u64,
    max_bytes: Option<u64>,
) -> Result<()> {
    validate_seed_destination(&seed.destination)?;
    let source = &seed.source;
    if !source.is_absolute() {
        bail!(
            "state seed source must be an explicit absolute path: {}",
            source.display()
        );
    }
    ensure_no_symlink_components(source)?;
    let meta = fs::symlink_metadata(source)
        .with_context(|| format!("reading seed source {}", source.display()))?;
    if meta.file_type().is_symlink() {
        bail!(
            "state seed source may not be a symlink: {}",
            source.display()
        );
    }
    if path_looks_like_credential(source) && !seed.include_credentials {
        bail!(
            "state seed {} looks like credential data; set include_credentials=true to opt in",
            source.display()
        );
    }
    let destination = state_root.join(&seed.destination);
    ensure_no_symlink_components(&destination)?;
    if meta.is_dir() {
        copy_directory(
            source,
            &destination,
            seed.include_credentials,
            copied_bytes,
            max_bytes,
        )?;
    } else if meta.is_file() {
        if !is_sqlite_or_journal(source) && !has_sqlite_header(source)? {
            copy_regular_file(source, &destination, copied_bytes, max_bytes)?;
        }
    } else {
        bail!(
            "state seed source must be a regular file or directory: {}",
            source.display()
        );
    }
    Ok(())
}

fn copy_directory(
    source: &Path,
    destination: &Path,
    include_credentials: bool,
    copied_bytes: &mut u64,
    max_bytes: Option<u64>,
) -> Result<()> {
    let meta = fs::symlink_metadata(source)?;
    if meta.file_type().is_symlink() || !meta.is_dir() {
        bail!(
            "refusing non-directory or symlink in state seed: {}",
            source.display()
        );
    }
    ensure_private_dir(destination)?;
    for entry in
        fs::read_dir(source).with_context(|| format!("reading state seed {}", source.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let kind = entry.file_type()?;
        if kind.is_symlink() {
            bail!("refusing symlink in state seed: {}", path.display());
        }
        if is_sqlite_or_journal(&path) || (kind.is_file() && has_sqlite_header(&path)?) {
            continue;
        }
        if path_looks_like_credential(&path) && !include_credentials {
            bail!(
                "state seed contains credential-like path {}; set include_credentials=true to opt in",
                path.display()
            );
        }
        let target = destination.join(entry.file_name());
        if kind.is_dir() {
            copy_directory(&path, &target, include_credentials, copied_bytes, max_bytes)?;
        } else if kind.is_file() {
            copy_regular_file(&path, &target, copied_bytes, max_bytes)?;
        } else {
            bail!("refusing special file in state seed: {}", path.display());
        }
    }
    Ok(())
}

fn copy_regular_file(
    source: &Path,
    destination: &Path,
    copied_bytes: &mut u64,
    max_bytes: Option<u64>,
) -> Result<()> {
    let meta = fs::symlink_metadata(source)?;
    if !meta.is_file() || meta.file_type().is_symlink() {
        bail!("refusing non-regular state seed file: {}", source.display());
    }
    if let Some(parent) = destination.parent() {
        ensure_private_dir(parent)?;
    }
    ensure_no_symlink_components(destination)?;
    if destination.exists() {
        bail!(
            "two state seeds target the same path: {}",
            destination.display()
        );
    }
    let mut input = fs::File::open(source)
        .with_context(|| format!("opening state seed {}", source.display()))?;
    let mut output = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)
        .with_context(|| format!("creating state seed copy {}", destination.display()))?;
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let count = input.read(&mut buffer)?;
        if count == 0 {
            break;
        }
        let next = (*copied_bytes).saturating_add(count as u64);
        if max_bytes.is_some_and(|limit| next > limit) {
            bail!(
                "retention_quota_exceeded: copied state seed exceeded its reserved byte capacity"
            );
        }
        output.write_all(&buffer[..count])?;
        *copied_bytes = next;
    }
    output.sync_all()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(destination, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

fn validate_seed_destination(path: &Path) -> Result<()> {
    if path.is_absolute()
        || path.components().any(|c| {
            matches!(
                c,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        bail!(
            "state seed destination must be a relative path inside the private state root: {}",
            path.display()
        );
    }
    let first = path
        .components()
        .next()
        .and_then(|c| c.as_os_str().to_str())
        .unwrap_or_default();
    if !["home", "data", "config", "cache", "state", "tmp"].contains(&first) {
        bail!(
            "state seed destination must begin with home, data, config, cache, state, or tmp: {}",
            path.display()
        );
    }
    if path.as_os_str().is_empty() {
        bail!("state seed destination must not be empty");
    }
    Ok(())
}

fn is_sqlite_or_journal(path: &Path) -> bool {
    let name = path
        .file_name()
        .and_then(|s| s.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    name.ends_with("-wal")
        || name.ends_with("-shm")
        || name.ends_with("-journal")
        || ["db", "sqlite", "sqlite3"].iter().any(|ext| {
            path.extension()
                .and_then(|s| s.to_str())
                .is_some_and(|value| value.eq_ignore_ascii_case(ext))
        })
}

fn has_sqlite_header(path: &Path) -> Result<bool> {
    use std::io::Read;
    let mut file = fs::File::open(path)?;
    let mut signature = [0_u8; 16];
    let read = file.read(&mut signature)?;
    Ok(read == signature.len() && &signature == b"SQLite format 3\0")
}

fn path_looks_like_credential(path: &Path) -> bool {
    path.components()
        .filter_map(|c| c.as_os_str().to_str())
        .any(|part| {
            let p = part.to_ascii_lowercase();
            p == "auth.json" || p.contains("credential") || p == "secrets" || p == ".env"
        })
}

fn validate_session_id(session_id: &str) -> Result<()> {
    if session_id.is_empty()
        || !session_id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "-_".contains(c))
    {
        bail!("invalid session id; use ASCII letters, digits, '-' or '_'");
    }
    Ok(())
}
