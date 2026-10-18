use anyhow::{Context, Result, bail};
use rmcp::schemars;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::{self, OpenOptions};
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Component, Path, PathBuf};

use crate::config::StateSeed;

pub const PROFILE_VERSION: u32 = 1;

/// Values stored in a profile are either non-secret literals or references to
/// the daemon's environment. Secret values themselves are never persisted.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ProfileEnvValue {
    Literal { value: String },
    HostReference { variable: String, secret: bool },
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Profile {
    #[serde(default = "profile_version")]
    pub version: u32,
    pub name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default)]
    pub owner: Option<String>,
    /// Ambient daemon variables that this profile is allowed to inherit.
    #[serde(default)]
    pub env_allowlist: Vec<String>,
    /// Explicit profile values. Secret provider keys should use HostReference.
    #[serde(default)]
    pub env: BTreeMap<String, ProfileEnvValue>,
    /// Selected state copied into each session snapshot.
    #[serde(default)]
    pub seeds: Vec<StateSeed>,
    /// Enforced launch requirements, not descriptive metadata. Absent for legacy profiles.
    #[serde(default)]
    pub test_policy: Option<TestProfilePolicy>,
    /// Non-secret descriptive fields, such as readiness hints.
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct TestProfilePolicy {
    pub isolation: crate::isolation::IsolationSpec,
    pub dispose_on_pass: bool,
}

impl Default for TestProfilePolicy {
    fn default() -> Self {
        Self {
            isolation: crate::isolation::IsolationSpec {
                backend: crate::isolation::IsolationBackend::Bubblewrap,
                network: crate::isolation::NetworkPolicy::None,
            },
            dispose_on_pass: true,
        }
    }
}

fn profile_version() -> u32 {
    PROFILE_VERSION
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
pub struct ProfileMetadata {
    pub version: u32,
    pub name: String,
    pub description: String,
    pub owner: Option<String>,
    pub environment_names: Vec<String>,
    pub credential_reference_names: Vec<String>,
    pub seed_count: usize,
    /// Names only: profile metadata values are deliberately not exported.
    pub metadata_keys: Vec<String>,
    pub test_policy: Option<TestProfilePolicy>,
}

pub fn profiles_root(runtime_root: &Path) -> PathBuf {
    runtime_root.join("profiles")
}

pub fn profile_path(runtime_root: &Path, name: &str) -> Result<PathBuf> {
    validate_profile_name(name)?;
    Ok(profiles_root(runtime_root).join(name).join("profile.json"))
}

pub fn load_profile(runtime_root: &Path, name: &str) -> Result<Profile> {
    let path = profile_path(runtime_root, name)?;
    ensure_no_symlink_components(&path)?;
    let bytes = fs::read(&path).with_context(|| format!("reading profile {}", path.display()))?;
    let profile: Profile = serde_json::from_slice(&bytes)
        .with_context(|| format!("parsing profile {}", path.display()))?;
    validate_profile(&profile)?;
    Ok(profile)
}

/// Create and persist an empty named profile. Existing profiles are not
/// overwritten; callers can edit the returned value and call store_profile.
pub fn create_profile(runtime_root: &Path, name: &str, description: &str) -> Result<Profile> {
    let profile = Profile {
        version: PROFILE_VERSION,
        name: name.to_owned(),
        description: description.to_owned(),
        owner: None,
        env_allowlist: Vec::new(),
        env: BTreeMap::new(),
        seeds: Vec::new(),
        test_policy: None,
        metadata: BTreeMap::new(),
    };
    store_profile_new(runtime_root, &profile)?;
    Ok(profile)
}

/// Persist a profile atomically with owner-only permissions.
pub fn store_profile(runtime_root: &Path, profile: &Profile) -> Result<ProfileMetadata> {
    validate_profile(profile)?;
    let path = profile_path(runtime_root, &profile.name)?;
    ensure_private_dir(path.parent().context("profile path has no parent")?)?;
    let temp = path.with_extension(format!("json.tmp-{}", std::process::id()));
    ensure_no_symlink_components(&temp)?;
    let bytes = serde_json::to_vec_pretty(profile)?;
    let mut opts = OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    opts.mode(0o600);
    let mut file = opts
        .open(&temp)
        .with_context(|| format!("creating {}", temp.display()))?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    drop(file);
    fs::rename(&temp, &path).with_context(|| format!("storing profile {}", path.display()))?;
    set_private_file(&path)?;
    Ok(metadata(profile))
}

fn store_profile_new(runtime_root: &Path, profile: &Profile) -> Result<()> {
    validate_profile(profile)?;
    let path = profile_path(runtime_root, &profile.name)?;
    ensure_private_dir(path.parent().context("profile path has no parent")?)?;
    let bytes = serde_json::to_vec_pretty(profile)?;
    let mut opts = OpenOptions::new();
    opts.write(true).create_new(true);
    #[cfg(unix)]
    opts.mode(0o600);
    opts.open(&path)
        .with_context(|| format!("creating profile {} (it may already exist)", profile.name))?
        .write_all(&bytes)?;
    Ok(())
}

pub fn list_profiles(runtime_root: &Path) -> Result<Vec<ProfileMetadata>> {
    let root = profiles_root(runtime_root);
    if !root.exists() {
        return Ok(Vec::new());
    }
    ensure_no_symlink_components(&root)?;
    let mut out = Vec::new();
    for entry in fs::read_dir(&root).with_context(|| format!("listing {}", root.display()))? {
        let entry = entry?;
        let kind = entry.file_type()?;
        if !kind.is_dir() || kind.is_symlink() {
            continue;
        }
        let profile_file = entry.path().join("profile.json");
        if !profile_file.exists() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        out.push(metadata(&load_profile(runtime_root, &name)?));
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

/// A safe starter profile: it uses an isolated, fake state root, has no
/// provider credentials or model selection, and makes live usage opt-in.
pub fn silicogen_test_profile_template() -> Profile {
    let mut metadata = BTreeMap::new();
    metadata.insert("state_template".into(), "dedicated-fake-state".into());
    metadata.insert(
        "provider_setup".into(),
        "configure a deterministic test provider; live credentials are not included".into(),
    );
    Profile {
        version: PROFILE_VERSION,
        name: "silicogen-test".into(),
        description: "Enforced Bubblewrap test profile: selected auth/config files, fresh app state, required project copy; no credentials or model configured"
            .into(),
        owner: Some("test".into()),
        env_allowlist: vec!["PATH".into(), "LANG".into(), "LC_ALL".into()],
        env: BTreeMap::from([(
            "SILICOGEN_STARTUP_TRACE".into(),
            ProfileEnvValue::Literal { value: "1".into() },
        )]),
        seeds: Vec::new(),
        test_policy: Some(TestProfilePolicy::default()),
        metadata,
    }
}

pub fn profile_metadata(profile: &Profile) -> ProfileMetadata {
    metadata(profile)
}

fn metadata(profile: &Profile) -> ProfileMetadata {
    let mut environment_names: Vec<_> = profile.env_allowlist.to_vec();
    environment_names.extend(profile.env.keys().cloned());
    environment_names.sort();
    environment_names.dedup();
    let credential_reference_names = profile
        .env
        .iter()
        .filter_map(|(name, value)| match value {
            ProfileEnvValue::HostReference { secret: true, .. } => Some(name.clone()),
            _ => None,
        })
        .collect();
    ProfileMetadata {
        version: profile.version,
        name: profile.name.clone(),
        description: profile.description.clone(),
        owner: profile.owner.clone(),
        environment_names,
        credential_reference_names,
        seed_count: profile.seeds.len(),
        metadata_keys: profile.metadata.keys().cloned().collect(),
        test_policy: profile.test_policy.clone(),
    }
}

fn validate_profile(profile: &Profile) -> Result<()> {
    validate_profile_name(&profile.name)?;
    if profile.version != PROFILE_VERSION {
        bail!(
            "unsupported profile version {}; supported version is {}",
            profile.version,
            PROFILE_VERSION
        );
    }
    for name in profile.env_allowlist.iter().chain(profile.env.keys()) {
        validate_env_name(name)?;
    }
    for (key, value) in &profile.env {
        if let ProfileEnvValue::HostReference { variable, .. } = value {
            validate_env_name(variable)?;
        }
        if let ProfileEnvValue::Literal { .. } = value
            && looks_secret_name(key)
        {
            bail!(
                "profile variable {key} looks secret; store a host environment reference instead of a secret value"
            );
        }
    }
    for seed in &profile.seeds {
        if seed.include_credentials && !seed.source.is_absolute() {
            bail!("credential seed sources must be explicit absolute paths");
        }
        if profile.test_policy.is_some() {
            validate_test_seed(seed)?;
        }
    }
    if let Some(policy) = &profile.test_policy {
        if policy.isolation.backend != crate::isolation::IsolationBackend::Bubblewrap {
            bail!(
                "test_profile_policy: a testing profile requires bubblewrap; select a legacy host profile explicitly for unrestricted execution"
            );
        }
        for name in &profile.env_allowlist {
            if looks_secret_name(name) || name.ends_with("_SOCK") {
                bail!(
                    "test_profile_policy: {name} may not be ambiently inherited; use an explicit selected credential seed or secret reference"
                );
            }
        }
        for name in profile.env_allowlist.iter().chain(profile.env.keys()) {
            validate_test_environment_name(name)?;
        }
    }
    Ok(())
}

pub(crate) fn validate_test_environment_name(name: &str) -> Result<()> {
    if name.starts_with("LD_") || ["GLIBC_TUNABLES", "GCONV_PATH", "LOCPATH"].contains(&name) {
        bail!(
            "test_profile_policy: {name} could alter the sandbox launcher before confinement and is not allowed"
        );
    }
    Ok(())
}

fn validate_test_seed(seed: &StateSeed) -> Result<()> {
    ensure_no_symlink_components(&seed.source)?;
    if !seed.source.is_absolute() || !seed.source.is_file() {
        bail!(
            "test_profile_policy: seeds must name selected configuration/credential files, not directories"
        );
    }
    let components: Vec<_> = seed.destination.components().collect();
    if components.len() < 2
        || !matches!(
            components[0].as_os_str().to_str(),
            Some("home" | "config" | "data")
        )
        || components
            .iter()
            .any(|part| !matches!(part, Component::Normal(_)))
    {
        bail!(
            "test_profile_policy: seed destination must be a selected file under home, config, or data"
        );
    }
    for path in [&seed.source, &seed.destination] {
        if path.components().any(|part| {
            let name = part.as_os_str().to_string_lossy().to_ascii_lowercase();
            matches!(name.as_str(), "log" | "logs" | "cache" | "storage")
                || name.ends_with(".log")
                || name.ends_with(".db")
                || name.contains("sqlite")
                || name.ends_with("-wal")
                || name.ends_with("-shm")
        }) {
            bail!("test_profile_policy: database, log, and cache seeds are not permitted");
        }
    }
    Ok(())
}

pub(crate) fn validate_profile_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 64
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "-_".contains(c))
    {
        bail!("invalid profile name {name:?}; use 1-64 ASCII letters, digits, '-' or '_'");
    }
    Ok(())
}

pub(crate) fn validate_env_name(name: &str) -> Result<()> {
    let mut chars = name.chars();
    if !chars
        .next()
        .is_some_and(|c| c == '_' || c.is_ascii_alphabetic())
        || !chars.all(|c| c == '_' || c.is_ascii_alphanumeric())
    {
        bail!("invalid environment variable name {name:?}");
    }
    Ok(())
}

pub(crate) fn ensure_private_dir(path: &Path) -> Result<()> {
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(_) | Component::RootDir => current.push(component.as_os_str()),
            Component::CurDir => continue,
            Component::ParentDir => bail!(
                "parent traversal is not allowed in private path {}",
                path.display()
            ),
            Component::Normal(part) => {
                current.push(part);
                match fs::symlink_metadata(&current) {
                    Ok(meta) if meta.file_type().is_symlink() || !meta.is_dir() => bail!(
                        "private directory component is not a real directory: {}",
                        current.display()
                    ),
                    Ok(_) => {}
                    Err(e) if e.kind() == std::io::ErrorKind::NotFound => fs::create_dir(&current)
                        .with_context(|| format!("creating {}", current.display()))?,
                    Err(e) => return Err(e.into()),
                }
            }
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

pub(crate) fn ensure_no_symlink_components(path: &Path) -> Result<()> {
    let mut current = PathBuf::new();
    for component in path.components() {
        current.push(component.as_os_str());
        if let Ok(meta) = fs::symlink_metadata(&current)
            && meta.file_type().is_symlink()
        {
            bail!(
                "refusing symlink in managed profile path: {}",
                current.display()
            );
        }
    }
    Ok(())
}

fn set_private_file(path: &Path) -> Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

fn looks_secret_name(name: &str) -> bool {
    let name = name.to_ascii_uppercase();
    ["KEY", "TOKEN", "SECRET", "PASSWORD", "CREDENTIAL"]
        .iter()
        .any(|needle| name.contains(needle))
}
