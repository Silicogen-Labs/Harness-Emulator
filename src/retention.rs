//! Ownership-based cleanup for emulator-managed paths.
//!
//! A path is eligible only after explicit registration. Names and directory
//! prefixes are never sufficient ownership evidence.

use anyhow::{Context, Result, bail};
use rmcp::schemars;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Component, Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use ulid::Ulid;

const MANIFEST_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OwnerKind {
    Session,
    Run,
    Artifact,
    TestWorkspace,
    DeveloperCache,
    Profile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RetentionClass {
    Successful,
    Failed,
    Interactive,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ManagedEntry {
    pub id: String,
    pub relative_path: PathBuf,
    pub kind: OwnerKind,
    pub retention: RetentionClass,
    pub created_at_ms: u128,
    #[serde(default)]
    pub pinned: bool,
    #[serde(default)]
    pub description: String,
}

impl ManagedEntry {
    pub fn new(
        relative_path: impl Into<PathBuf>,
        kind: OwnerKind,
        retention: RetentionClass,
        created_at_ms: u128,
        description: impl Into<String>,
    ) -> Self {
        Self {
            id: Ulid::generate().to_string(),
            relative_path: relative_path.into(),
            kind,
            retention,
            created_at_ms,
            pinned: false,
            description: description.into(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(default, deny_unknown_fields)]
pub struct RetentionPolicy {
    pub successful_ms: u128,
    pub failed_ms: u128,
    #[serde(default = "default_max_bytes")]
    pub max_bytes: Option<u64>,
    #[serde(default = "default_max_entries")]
    pub max_entries: Option<usize>,
}

fn default_max_bytes() -> Option<u64> {
    Some(5 * 1024 * 1024 * 1024)
}

fn default_max_entries() -> Option<usize> {
    Some(10_000)
}

impl Default for RetentionPolicy {
    fn default() -> Self {
        Self {
            successful_ms: Duration::from_secs(24 * 60 * 60).as_millis(),
            failed_ms: Duration::from_secs(7 * 24 * 60 * 60).as_millis(),
            max_bytes: Some(5 * 1024 * 1024 * 1024),
            max_entries: Some(10_000),
        }
    }
}

/// One target path claimed by a durable quota reservation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ReservationTarget {
    pub relative_path: PathBuf,
    pub kind: OwnerKind,
    pub description: String,
}

impl ReservationTarget {
    pub fn new(
        relative_path: impl Into<PathBuf>,
        kind: OwnerKind,
        description: impl Into<String>,
    ) -> Self {
        Self {
            relative_path: relative_path.into(),
            kind,
            description: description.into(),
        }
    }
}

/// Handle returned after capacity has been durably claimed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct QuotaReservation {
    pub id: String,
    pub reserved_bytes: u64,
    pub targets: Vec<ReservationTarget>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CleanupCandidate {
    pub id: String,
    pub path: PathBuf,
    pub reason: String,
    pub bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct ProtectedEntry {
    pub id: String,
    pub path: PathBuf,
    pub reason: String,
    pub bytes: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct CleanupPlan {
    pub token: String,
    pub generation: u64,
    pub generated_at_ms: u128,
    pub candidates: Vec<CleanupCandidate>,
    pub protected: Vec<ProtectedEntry>,
    pub total_bytes: u64,
    pub unowned_legacy_paths: Vec<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct AppliedCleanup {
    pub removed: Vec<CleanupCandidate>,
    pub total_bytes: u64,
    pub generation: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
pub struct BudgetStatus {
    pub used_bytes: u64,
    pub requested_bytes: u64,
    pub max_bytes: Option<u64>,
    pub pinned_bytes: u64,
    pub active_bytes: u64,
    pub reclaimable_bytes: u64,
    pub reclaimable_entries: usize,
    pub projected_bytes: u64,
    pub projected_entries: usize,
    pub requested_entries: usize,
    pub max_entries: Option<usize>,
    pub quota_exceeded: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
struct OwnerManifest {
    version: u16,
    generation: u64,
    entries: Vec<ManagedEntry>,
    /// Durable protection independent of an entry's mutable `kind` field.
    #[serde(default)]
    protected_profile_roots: Vec<PathBuf>,
    #[serde(default)]
    reservations: Vec<QuotaReservation>,
}

impl Default for OwnerManifest {
    fn default() -> Self {
        Self {
            version: MANIFEST_VERSION,
            generation: 0,
            entries: Vec::new(),
            protected_profile_roots: Vec::new(),
            reservations: Vec::new(),
        }
    }
}

/// Registry rooted at one private runtime directory. Mutation persists the
/// ownership manifest before returning.
pub struct RetentionRegistry {
    root: PathBuf,
    manifest_path: PathBuf,
    manifest: OwnerManifest,
    policy: RetentionPolicy,
    pending_plans: HashMap<String, (u64, Vec<CleanupCandidate>)>,
}

impl RetentionRegistry {
    pub fn open(root: impl AsRef<Path>) -> Result<Self> {
        Self::open_with_policy(root, RetentionPolicy::default())
    }

    pub fn open_with_policy(root: impl AsRef<Path>, policy: RetentionPolicy) -> Result<Self> {
        fs::create_dir_all(root.as_ref())
            .with_context(|| format!("create retention root {}", root.as_ref().display()))?;
        let root = fs::canonicalize(root.as_ref()).context("canonicalize retention root")?;
        let metadata = fs::symlink_metadata(&root)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            bail!("retention root must be a real directory")
        }
        let directory = root.join("retention");
        fs::create_dir_all(&directory)?;
        let directory_metadata = fs::symlink_metadata(&directory)?;
        if !directory_metadata.is_dir() || directory_metadata.file_type().is_symlink() {
            bail!("retention manifest directory must be a real directory")
        }
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&directory, fs::Permissions::from_mode(0o700))?;
        }
        let manifest_path = directory.join("owners.json");
        let _lock = ManifestLock::acquire(&directory)?;
        let mut manifest = match fs::symlink_metadata(&manifest_path) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() || !metadata.is_file() {
                    bail!("retention manifest must be a regular file")
                }
                let parsed: OwnerManifest = serde_json::from_slice(&fs::read(&manifest_path)?)?;
                if parsed.version != MANIFEST_VERSION {
                    bail!("unsupported retention manifest version {}", parsed.version)
                }
                parsed
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => OwnerManifest::default(),
            Err(error) => return Err(error.into()),
        };
        for entry in &manifest.entries {
            if entry.kind == OwnerKind::Profile
                && !manifest
                    .protected_profile_roots
                    .contains(&entry.relative_path)
            {
                manifest
                    .protected_profile_roots
                    .push(entry.relative_path.clone());
            }
        }
        reconcile_reservations(&root, &manifest_path, &mut manifest)?;
        if !manifest_path.exists() {
            persist_manifest(&manifest_path, &manifest)?;
        }
        Ok(Self {
            root,
            manifest_path,
            manifest,
            policy,
            pending_plans: HashMap::new(),
        })
    }

    pub fn generation(&self) -> u64 {
        self.manifest.generation
    }

    pub fn entries(&self) -> &[ManagedEntry] {
        &self.manifest.entries
    }

    /// Drop accounting only after an owned workspace was actually disposed.
    pub fn forget_disposed_workspace(&mut self, relative: &Path) -> Result<()> {
        validate_relative_path(relative)?;
        let _lock = ManifestLock::acquire(self.manifest_path.parent().unwrap())?;
        self.reload_manifest()?;
        match fs::symlink_metadata(self.root.join(relative)) {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            _ => bail!(
                "workspace_not_disposed: refusing to remove live or unreadable ownership accounting"
            ),
        }
        if let Some(entry) = self
            .manifest
            .entries
            .iter()
            .find(|entry| entry.relative_path == relative)
        {
            if entry.kind != OwnerKind::TestWorkspace || entry.pinned {
                bail!("workspace_not_disposable: entry is protected");
            }
            self.manifest
                .entries
                .retain(|entry| entry.relative_path != relative);
            self.bump_and_persist()?;
        }
        Ok(())
    }

    /// Validate ownership and protected descendants, then remove the state and
    /// optional test workspace while holding the manifest lock across both
    /// checks and unlink operations. A separately registered descendant makes
    /// recursive workspace disposal unsafe, regardless of its retention age.
    pub fn dispose_test_allocation(
        &mut self,
        state_relative: &Path,
        workspace_relative: Option<&Path>,
    ) -> Result<()> {
        validate_relative_path(state_relative)?;
        if state_relative.file_name().and_then(|name| name.to_str()) != Some("state") {
            bail!("invalid_disposal_target: expected a session state directory")
        }
        if let Some(workspace) = workspace_relative {
            validate_relative_path(workspace)?;
            if workspace
                .components()
                .next()
                .and_then(|part| part.as_os_str().to_str())
                != Some("workspaces")
            {
                bail!("invalid_disposal_target: expected a managed workspace directory")
            }
        }

        let _lock = ManifestLock::acquire(self.manifest_path.parent().unwrap())?;
        self.reload_manifest()?;

        let session_owner_path = state_relative
            .parent()
            .context("state path has no session owner")?;
        let session_owner = self
            .manifest
            .entries
            .iter()
            .find(|entry| {
                entry.relative_path == session_owner_path && entry.kind == OwnerKind::Session
            })
            .context("session state has no registered session owner")?;
        if session_owner.pinned || self.is_profile_protected(state_relative) {
            bail!("session_state_protected: refusing to dispose protected session state")
        }
        if self.manifest.entries.iter().any(|entry| {
            entry.relative_path != state_relative && entry.relative_path.starts_with(state_relative)
        }) {
            bail!("session_state_has_registered_descendant: refusing recursive disposal")
        }

        if let Some(workspace) = workspace_relative {
            let owner = self
                .manifest
                .entries
                .iter()
                .find(|entry| {
                    entry.relative_path == workspace && entry.kind == OwnerKind::TestWorkspace
                })
                .context("test workspace has no registered owner")?;
            if owner.pinned || self.is_profile_protected(workspace) {
                bail!("workspace_protected: refusing to dispose protected test workspace")
            }
            if self.has_registered_descendant(workspace, &owner.id) {
                bail!("workspace_has_registered_descendant: refusing recursive disposal")
            }
        }

        for relative in std::iter::once(state_relative).chain(workspace_relative) {
            match fs::symlink_metadata(self.root.join(relative)) {
                Ok(_) => validate_tree_path(&self.root, &self.root.join(relative))?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(error.into()),
            }
        }

        for relative in std::iter::once(state_relative).chain(workspace_relative) {
            let path = self.root.join(relative);
            if path.exists() {
                remove_owned_path(&path)?;
            }
        }
        if let Some(workspace) = workspace_relative {
            self.manifest
                .entries
                .retain(|entry| entry.relative_path != workspace);
        }
        self.bump_and_persist()?;
        Ok(())
    }

    /// Report managed storage usage and fail with a stable quota marker when a
    /// configured budget would be exceeded. This method never evicts data.
    pub fn ensure_capacity(
        &self,
        requested_bytes: u64,
        max_bytes: Option<u64>,
    ) -> Result<BudgetStatus> {
        let mut used_bytes = 0_u64;
        let mut pinned_bytes = 0_u64;
        for entry in &self.manifest.entries {
            let path = self.root.join(&entry.relative_path);
            validate_tree_path(&self.root, &path)?;
            let bytes = self.entry_bytes(entry)?;
            used_bytes = used_bytes.saturating_add(bytes);
            if self.is_profile_protected(&entry.relative_path)
                || entry.kind == OwnerKind::Profile
                || entry.pinned
                || entry.retention == RetentionClass::Interactive
            {
                pinned_bytes = pinned_bytes.saturating_add(bytes);
            }
        }
        let reserved_bytes = self
            .manifest
            .reservations
            .iter()
            .fold(0_u64, |total, reservation| {
                total.saturating_add(reservation.reserved_bytes)
            });
        used_bytes = used_bytes.saturating_add(reserved_bytes);
        let quota_exceeded =
            max_bytes.is_some_and(|limit| used_bytes.saturating_add(requested_bytes) > limit);
        let status = BudgetStatus {
            used_bytes,
            requested_bytes,
            max_bytes,
            pinned_bytes,
            active_bytes: 0,
            reclaimable_bytes: 0,
            reclaimable_entries: 0,
            projected_bytes: used_bytes.saturating_add(requested_bytes),
            projected_entries: self.manifest.entries.len()
                + self
                    .manifest
                    .reservations
                    .iter()
                    .map(|reservation| reservation.targets.len())
                    .sum::<usize>(),
            requested_entries: 0,
            max_entries: None,
            quota_exceeded,
        };
        if quota_exceeded {
            bail!(
                "retention_quota_exceeded: used={} requested={} limit={} pinned={}",
                used_bytes,
                requested_bytes,
                max_bytes.unwrap_or_default(),
                pinned_bytes
            )
        }
        Ok(status)
    }

    /// Evaluate optional byte and entry budgets using only explicitly managed
    /// entries plus every outstanding reservation. Expired successful/failed
    /// entries are reported as reclaimable when safe, but remain in the quota
    /// projection until cleanup is actually applied.
    pub fn ensure_budget(
        &self,
        requested_bytes: u64,
        requested_entries: usize,
        now_ms: u128,
        active_paths: &[PathBuf],
    ) -> Result<BudgetStatus> {
        let active = canonical_active_paths(&self.root, active_paths);
        let mut used_bytes = 0_u64;
        let mut pinned_bytes = 0_u64;
        let mut active_bytes = 0_u64;
        let mut reclaimable_bytes = 0_u64;
        let mut reclaimable_entries = 0_usize;

        for entry in &self.manifest.entries {
            let path = self.root.join(&entry.relative_path);
            validate_tree_path(&self.root, &path)?;
            let bytes = self.entry_bytes(entry)?;
            used_bytes = used_bytes.saturating_add(bytes);
            let active_entry = active
                .iter()
                .any(|active_path| overlaps(active_path, &path));
            let profile_entry = self.is_profile_protected(&entry.relative_path);
            if profile_entry || entry.pinned || entry.retention == RetentionClass::Interactive {
                pinned_bytes = pinned_bytes.saturating_add(bytes);
            }
            if active_entry {
                active_bytes = active_bytes.saturating_add(bytes);
            }

            let age = now_ms.saturating_sub(entry.created_at_ms);
            let max_age = match entry.retention {
                RetentionClass::Successful => self.policy.successful_ms,
                RetentionClass::Failed => self.policy.failed_ms,
                RetentionClass::Interactive => u128::MAX,
            };
            let expired = age >= max_age;
            let protected = profile_entry
                || entry.kind == OwnerKind::Profile
                || self.has_registered_descendant(&entry.relative_path, &entry.id)
                || entry.pinned
                || entry.retention == RetentionClass::Interactive
                || entry.kind == OwnerKind::DeveloperCache
                || active_entry;
            if expired && !protected {
                reclaimable_bytes = reclaimable_bytes.saturating_add(bytes);
                reclaimable_entries = reclaimable_entries.saturating_add(1);
            }
        }

        let reserved_bytes = self
            .manifest
            .reservations
            .iter()
            .fold(0_u64, |total, reservation| {
                total.saturating_add(reservation.reserved_bytes)
            });
        let reserved_entries = self
            .manifest
            .reservations
            .iter()
            .map(|reservation| reservation.targets.len())
            .sum::<usize>();
        let projected_bytes = used_bytes
            .saturating_add(reserved_bytes)
            .saturating_add(requested_bytes);
        let projected_entries = self
            .manifest
            .entries
            .len()
            .saturating_add(reserved_entries)
            .saturating_add(requested_entries);
        let bytes_over = self
            .policy
            .max_bytes
            .is_some_and(|limit| projected_bytes > limit);
        let entries_over = self
            .policy
            .max_entries
            .is_some_and(|limit| projected_entries > limit);
        let status = BudgetStatus {
            used_bytes,
            requested_bytes,
            max_bytes: self.policy.max_bytes,
            pinned_bytes,
            active_bytes,
            reclaimable_bytes,
            reclaimable_entries,
            projected_bytes,
            projected_entries,
            requested_entries,
            max_entries: self.policy.max_entries,
            quota_exceeded: bytes_over || entries_over,
        };
        if status.quota_exceeded {
            bail!(
                "retention_quota_exceeded: used_bytes={} requested_bytes={} projected_bytes={} max_bytes={:?} projected_entries={} max_entries={:?} pinned_bytes={} active_bytes={}",
                status.used_bytes,
                status.requested_bytes,
                status.projected_bytes,
                status.max_bytes,
                status.projected_entries,
                status.max_entries,
                status.pinned_bytes,
                status.active_bytes
            )
        }
        Ok(status)
    }

    /// Register an existing path. Its lexical path must be relative and every
    /// component must resolve inside the registry root without a symlink.
    pub fn register(&mut self, entry: ManagedEntry) -> Result<()> {
        validate_relative_path(&entry.relative_path)?;
        let _lock = ManifestLock::acquire(self.manifest_path.parent().unwrap())?;
        self.reload_manifest()?;
        let full_path = self.root.join(&entry.relative_path);
        validate_tree_path(&self.root, &full_path)?;
        if self
            .manifest
            .entries
            .iter()
            .any(|known| known.relative_path == entry.relative_path || known.id == entry.id)
        {
            bail!("managed path or entry ID is already registered")
        }
        if self.manifest.reservations.iter().any(|reservation| {
            reservation
                .targets
                .iter()
                .any(|target| paths_overlap(&entry.relative_path, &target.relative_path))
        }) {
            bail!("managed path is currently held by a quota reservation")
        }
        if entry.kind == OwnerKind::Profile
            && !self
                .manifest
                .protected_profile_roots
                .contains(&entry.relative_path)
        {
            self.manifest
                .protected_profile_roots
                .push(entry.relative_path.clone());
        }
        self.manifest.entries.push(entry);
        self.bump_and_persist()
    }

    /// Durably claim bytes and managed-entry slots before creating any target
    /// paths. Every reservation in the manifest participates in quota checks.
    pub fn reserve(
        &mut self,
        reserved_bytes: u64,
        targets: Vec<ReservationTarget>,
    ) -> Result<QuotaReservation> {
        if targets.is_empty() {
            bail!("a quota reservation must include at least one target")
        }
        let _lock = ManifestLock::acquire(self.manifest_path.parent().unwrap())?;
        self.reload_manifest()?;
        let mut seen = BTreeSet::new();
        for target in &targets {
            validate_relative_path(&target.relative_path)?;
            if !seen.insert(target.relative_path.clone()) {
                bail!("reservation contains a duplicate target path")
            }
            if self
                .manifest
                .entries
                .iter()
                .any(|entry| paths_overlap(&entry.relative_path, &target.relative_path))
                || self.manifest.reservations.iter().any(|reservation| {
                    reservation
                        .targets
                        .iter()
                        .any(|known| paths_overlap(&known.relative_path, &target.relative_path))
                })
            {
                bail!("reservation target overlaps an already managed or reserved path")
            }
            let path = self.root.join(&target.relative_path);
            validate_reservation_path(&self.root, &path)?;
            if path.exists() {
                bail!("reservation target already exists: {}", path.display())
            }
        }
        for (index, target) in targets.iter().enumerate() {
            if targets[index + 1..]
                .iter()
                .any(|other| paths_overlap(&target.relative_path, &other.relative_path))
            {
                bail!("reservation target paths overlap")
            }
        }
        let reserved_total = self
            .manifest
            .reservations
            .iter()
            .fold(0_u64, |total, reservation| {
                total.saturating_add(reservation.reserved_bytes)
            });
        let reserved_entries = self
            .manifest
            .reservations
            .iter()
            .map(|reservation| reservation.targets.len())
            .sum::<usize>();
        let actual_bytes = self.actual_managed_bytes()?;
        let projected_bytes = actual_bytes
            .saturating_add(reserved_total)
            .saturating_add(reserved_bytes);
        let projected_entries = self
            .manifest
            .entries
            .len()
            .saturating_add(reserved_entries)
            .saturating_add(targets.len());
        if self
            .policy
            .max_bytes
            .is_some_and(|limit| projected_bytes > limit)
            || self
                .policy
                .max_entries
                .is_some_and(|limit| projected_entries > limit)
        {
            bail!(
                "retention_quota_exceeded: projected_bytes={} max_bytes={:?} projected_entries={} max_entries={:?}",
                projected_bytes,
                self.policy.max_bytes,
                projected_entries,
                self.policy.max_entries
            )
        }
        let reservation = QuotaReservation {
            id: Ulid::generate().to_string(),
            reserved_bytes,
            targets,
        };
        self.manifest.reservations.push(reservation.clone());
        self.bump_and_persist()?;
        Ok(reservation)
    }

    /// Convert all targets of a claim into managed entries after allocations
    /// finish. Recorded bytes are measured from the resulting trees.
    pub fn commit_reservation(
        &mut self,
        reservation_id: &str,
        retention: RetentionClass,
        created_at_ms: u128,
    ) -> Result<Vec<ManagedEntry>> {
        let _lock = ManifestLock::acquire(self.manifest_path.parent().unwrap())?;
        self.reload_manifest()?;
        let index = self
            .manifest
            .reservations
            .iter()
            .position(|reservation| reservation.id == reservation_id)
            .context("unknown quota reservation")?;
        let reservation = self.manifest.reservations[index].clone();
        let mut entries = Vec::with_capacity(reservation.targets.len());
        let mut actual_target_bytes = 0_u64;
        for target in reservation.targets {
            let path = self.root.join(&target.relative_path);
            validate_tree_path(&self.root, &path)?;
            if !path.exists() {
                bail!("reservation target is missing: {}", path.display())
            }
            actual_target_bytes = actual_target_bytes.saturating_add(tree_size(&path)?);
            entries.push(ManagedEntry::new(
                target.relative_path,
                target.kind,
                retention,
                created_at_ms,
                target.description,
            ));
        }
        if actual_target_bytes > reservation.reserved_bytes {
            bail!(
                "retention_quota_exceeded: allocation used {} bytes but reserved {}",
                actual_target_bytes,
                reservation.reserved_bytes
            )
        }
        let other_reserved_bytes = self
            .manifest
            .reservations
            .iter()
            .enumerate()
            .filter(|(other_index, _)| *other_index != index)
            .fold(0_u64, |total, (_, other)| {
                total.saturating_add(other.reserved_bytes)
            });
        let projected_bytes = self
            .actual_managed_bytes()?
            .saturating_add(actual_target_bytes)
            .saturating_add(other_reserved_bytes);
        let other_reserved_entries = self
            .manifest
            .reservations
            .iter()
            .enumerate()
            .filter(|(other_index, _)| *other_index != index)
            .map(|(_, other)| other.targets.len())
            .sum::<usize>();
        let projected_entries = self
            .manifest
            .entries
            .len()
            .saturating_add(entries.len())
            .saturating_add(other_reserved_entries);
        if self
            .policy
            .max_bytes
            .is_some_and(|limit| projected_bytes > limit)
            || self
                .policy
                .max_entries
                .is_some_and(|limit| projected_entries > limit)
        {
            bail!(
                "retention_quota_exceeded: completed allocation projects {} bytes and {} entries",
                projected_bytes,
                projected_entries
            )
        }
        self.manifest.reservations.remove(index);
        for entry in &entries {
            if entry.kind == OwnerKind::Profile
                && !self
                    .manifest
                    .protected_profile_roots
                    .contains(&entry.relative_path)
            {
                self.manifest
                    .protected_profile_roots
                    .push(entry.relative_path.clone());
            }
        }
        self.manifest.entries.extend(entries.clone());
        self.bump_and_persist()?;
        Ok(entries)
    }

    /// Cancel an unused claim. Missing reservations are treated as an error so
    /// callers can detect double release or stale handles.
    pub fn release_reservation(&mut self, reservation_id: &str) -> Result<()> {
        let _lock = ManifestLock::acquire(self.manifest_path.parent().unwrap())?;
        self.reload_manifest()?;
        let Some(index) = self
            .manifest
            .reservations
            .iter()
            .position(|reservation| reservation.id == reservation_id)
        else {
            bail!("unknown quota reservation")
        };
        self.manifest.reservations.remove(index);
        self.bump_and_persist()
    }

    /// Convert any paths created under a failed reservation into failed
    /// evidence, while releasing unused targets and the reserved capacity.
    pub fn fail_reservation(
        &mut self,
        reservation_id: &str,
        created_at_ms: u128,
    ) -> Result<Vec<ManagedEntry>> {
        let _lock = ManifestLock::acquire(self.manifest_path.parent().unwrap())?;
        self.reload_manifest()?;
        let index = self
            .manifest
            .reservations
            .iter()
            .position(|reservation| reservation.id == reservation_id)
            .context("unknown quota reservation")?;
        let reservation = self.manifest.reservations[index].clone();
        let mut entries = Vec::new();
        for target in &reservation.targets {
            let path = self.root.join(&target.relative_path);
            if !path.exists() {
                continue;
            }
            validate_tree_path(&self.root, &path)?;
            entries.push(ManagedEntry::new(
                target.relative_path.clone(),
                target.kind,
                RetentionClass::Failed,
                created_at_ms,
                format!("{} (failed allocation)", target.description),
            ));
        }
        self.manifest.reservations.remove(index);
        self.manifest.entries.extend(entries.clone());
        self.bump_and_persist()?;
        Ok(entries)
    }

    /// Update success/failure state after a run completes. Interactive entries
    /// remain pinned until explicitly changed by their owner.
    pub fn set_retention(
        &mut self,
        id: &str,
        retention: RetentionClass,
        pinned: bool,
    ) -> Result<()> {
        self.set_retention_inner(id, retention, pinned, None)
    }

    /// Update retention and reset the age origin, used when an asynchronous
    /// scenario transitions from an interactive report to a terminal result.
    pub fn set_retention_at(
        &mut self,
        id: &str,
        retention: RetentionClass,
        pinned: bool,
        created_at_ms: u128,
    ) -> Result<()> {
        self.set_retention_inner(id, retention, pinned, Some(created_at_ms))
    }

    fn set_retention_inner(
        &mut self,
        id: &str,
        retention: RetentionClass,
        pinned: bool,
        created_at_ms: Option<u128>,
    ) -> Result<()> {
        let _lock = ManifestLock::acquire(self.manifest_path.parent().unwrap())?;
        self.reload_manifest()?;
        let Some(entry) = self
            .manifest
            .entries
            .iter_mut()
            .find(|entry| entry.id == id)
        else {
            bail!("unknown managed entry: {id}")
        };
        entry.retention = retention;
        entry.pinned = pinned;
        if let Some(created_at_ms) = created_at_ms {
            entry.created_at_ms = created_at_ms;
        }
        self.bump_and_persist()
    }

    /// Preview cleanup. Active paths are paths known to be live by the caller;
    /// matching either an entry or one of its descendants protects that entry.
    pub fn preview(
        &mut self,
        now_ms: u128,
        active_paths: &[PathBuf],
        include_developer_cache: bool,
    ) -> Result<CleanupPlan> {
        let active = canonical_active_paths(&self.root, active_paths);
        let mut candidates = Vec::new();
        let mut protected = Vec::new();
        for entry in &self.manifest.entries {
            let path = self.root.join(&entry.relative_path);
            let reason = if self.is_profile_protected(&entry.relative_path)
                || entry.kind == OwnerKind::Profile
            {
                Some("profiles are protected by default".to_string())
            } else if self.has_registered_descendant(&entry.relative_path, &entry.id) {
                Some("contains a separately registered managed path".to_string())
            } else if entry.pinned || entry.retention == RetentionClass::Interactive {
                Some("entry is pinned or interactive".to_string())
            } else if entry.kind == OwnerKind::DeveloperCache && !include_developer_cache {
                Some("developer cache cleanup was not selected".to_string())
            } else if active
                .iter()
                .any(|active_path| overlaps(active_path, &path))
            {
                Some("path is active or contains an active path".to_string())
            } else {
                let age = now_ms.saturating_sub(entry.created_at_ms);
                let max_age = match entry.retention {
                    RetentionClass::Successful => self.policy.successful_ms,
                    RetentionClass::Failed => self.policy.failed_ms,
                    RetentionClass::Interactive => u128::MAX,
                };
                if age < max_age {
                    Some(format!(
                        "retained for {} ms more",
                        max_age.saturating_sub(age)
                    ))
                } else {
                    None
                }
            };

            match validate_tree_path(&self.root, &path) {
                Ok(()) => match self.entry_bytes(entry) {
                    Ok(bytes) => {
                        if let Some(reason) = reason {
                            protected.push(ProtectedEntry {
                                id: entry.id.clone(),
                                path,
                                reason,
                                bytes: Some(bytes),
                            });
                        } else {
                            candidates.push(CleanupCandidate {
                                id: entry.id.clone(),
                                path,
                                reason: format!("{:?} retention expired", entry.retention)
                                    .to_lowercase(),
                                bytes,
                            });
                        }
                    }
                    Err(error) => protected.push(ProtectedEntry {
                        id: entry.id.clone(),
                        path,
                        reason: format!("unsafe or unreadable tree: {error:#}"),
                        bytes: None,
                    }),
                },
                Err(error) => protected.push(ProtectedEntry {
                    id: entry.id.clone(),
                    path,
                    reason: format!("unsafe path: {error:#}"),
                    bytes: None,
                }),
            }
        }
        let total_bytes = candidates.iter().map(|candidate| candidate.bytes).sum();
        let token = Ulid::generate().to_string();
        self.pending_plans.insert(
            token.clone(),
            (self.manifest.generation, candidates.clone()),
        );
        Ok(CleanupPlan {
            token,
            generation: self.manifest.generation,
            generated_at_ms: now_ms,
            candidates,
            protected,
            total_bytes,
            unowned_legacy_paths: self.inventory_unowned_legacy()?,
        })
    }

    /// Apply exactly a previously previewed plan. Any manifest generation
    /// change, new active path, missing candidate, symlink, or special file
    /// invalidates the operation before that candidate is removed.
    pub fn apply(
        &mut self,
        plan: &CleanupPlan,
        active_paths: &[PathBuf],
    ) -> Result<AppliedCleanup> {
        if plan.generation != self.manifest.generation {
            bail!(
                "cleanup preview is stale: expected generation {}, current {}",
                plan.generation,
                self.manifest.generation
            )
        }
        if plan.token.is_empty() {
            bail!("cleanup preview token is missing")
        }
        let Some((preview_generation, preview_candidates)) = self.pending_plans.get(&plan.token)
        else {
            bail!("cleanup preview token is unknown, expired, or already applied")
        };
        if *preview_generation != plan.generation || preview_candidates != &plan.candidates {
            bail!("cleanup plan does not match its registered preview")
        }
        let active = canonical_active_paths(&self.root, active_paths);
        let mut validated = Vec::with_capacity(plan.candidates.len());
        for candidate in &plan.candidates {
            let relative = candidate
                .path
                .strip_prefix(&self.root)
                .context("preview candidate escaped registry root")?;
            validate_relative_path(relative)?;
            let entry = self
                .manifest
                .entries
                .iter()
                .find(|entry| entry.id == candidate.id && entry.relative_path == relative)
                .context("preview candidate is no longer registered")?;
            if self.is_profile_protected(&entry.relative_path)
                || entry.kind == OwnerKind::Profile
                || self.has_registered_descendant(&entry.relative_path, &entry.id)
                || entry.pinned
                || entry.retention == RetentionClass::Interactive
            {
                bail!(
                    "preview candidate became protected: {}",
                    candidate.path.display()
                )
            }
            if active
                .iter()
                .any(|active_path| overlaps(active_path, &candidate.path))
            {
                bail!(
                    "preview candidate is now active: {}",
                    candidate.path.display()
                )
            }
            validate_tree_path(&self.root, &candidate.path)?;
            let actual_size = tree_size(&candidate.path)?;
            if actual_size != candidate.bytes {
                bail!(
                    "preview candidate changed size: {}",
                    candidate.path.display()
                )
            }
            validated.push((candidate, relative.to_path_buf()));
        }
        let mut removed = Vec::new();
        for (candidate, relative) in validated {
            remove_owned_path(&candidate.path)?;
            self.manifest
                .entries
                .retain(|known| known.id != candidate.id || known.relative_path != relative);
            self.bump_and_persist()?;
            removed.push(candidate.clone());
        }
        self.pending_plans.remove(&plan.token);
        Ok(AppliedCleanup {
            total_bytes: removed.iter().map(|candidate| candidate.bytes).sum(),
            removed,
            generation: self.manifest.generation,
        })
    }

    /// Convenience JSON representation for CLI/MCP callers.
    pub fn preview_json(
        &mut self,
        now_ms: u128,
        active_paths: &[PathBuf],
        include_developer_cache: bool,
    ) -> Result<serde_json::Value> {
        Ok(serde_json::to_value(self.preview(
            now_ms,
            active_paths,
            include_developer_cache,
        )?)?)
    }

    /// Inventory legacy-looking directories for reconciliation only. This
    /// method never registers, follows, or deletes them.
    pub fn inventory_unowned_legacy(&self) -> Result<Vec<PathBuf>> {
        let registered: BTreeSet<PathBuf> = self
            .manifest
            .entries
            .iter()
            .map(|entry| entry.relative_path.clone())
            .collect();
        let mut paths = Vec::new();
        for child in fs::read_dir(&self.root)? {
            let child = child?;
            let name = child.file_name().to_string_lossy().into_owned();
            if !is_legacy_name(&name) {
                continue;
            }
            let metadata = fs::symlink_metadata(child.path())?;
            if metadata.file_type().is_dir() && !metadata.file_type().is_symlink() {
                let rel = PathBuf::from(name);
                if !registered.contains(&rel) {
                    paths.push(child.path());
                }
            }
        }
        paths.sort();
        Ok(paths)
    }

    fn bump_and_persist(&mut self) -> Result<()> {
        self.manifest.generation = self.manifest.generation.saturating_add(1);
        persist_manifest(&self.manifest_path, &self.manifest)
    }

    fn is_profile_protected(&self, relative_path: &Path) -> bool {
        self.manifest
            .protected_profile_roots
            .iter()
            .any(|profile_root| {
                relative_path.starts_with(profile_root) || profile_root.starts_with(relative_path)
            })
    }

    fn has_registered_descendant(&self, relative_path: &Path, entry_id: &str) -> bool {
        self.manifest.entries.iter().any(|entry| {
            entry.id != entry_id
                && entry.relative_path != relative_path
                && entry.relative_path.starts_with(relative_path)
        })
    }

    fn entry_bytes(&self, entry: &ManagedEntry) -> Result<u64> {
        let path = self.root.join(&entry.relative_path);
        let descendants = self
            .manifest
            .entries
            .iter()
            .filter(|other| {
                other.id != entry.id && other.relative_path.starts_with(&entry.relative_path)
            })
            .map(|other| self.root.join(&other.relative_path))
            .collect::<Vec<_>>();
        tree_size_excluding(&path, &descendants)
    }

    fn actual_managed_bytes(&self) -> Result<u64> {
        self.manifest
            .entries
            .iter()
            .try_fold(0_u64, |total, entry| {
                Ok(total.saturating_add(self.entry_bytes(entry)?))
            })
    }

    fn reload_manifest(&mut self) -> Result<()> {
        let parsed: OwnerManifest = serde_json::from_slice(&fs::read(&self.manifest_path)?)?;
        if parsed.version != MANIFEST_VERSION {
            bail!("unsupported retention manifest version {}", parsed.version)
        }
        self.manifest = parsed;
        for entry in &self.manifest.entries {
            if entry.kind == OwnerKind::Profile
                && !self
                    .manifest
                    .protected_profile_roots
                    .contains(&entry.relative_path)
            {
                self.manifest
                    .protected_profile_roots
                    .push(entry.relative_path.clone());
            }
        }
        Ok(())
    }
}

fn reconcile_reservations(
    root: &Path,
    manifest_path: &Path,
    manifest: &mut OwnerManifest,
) -> Result<()> {
    if manifest.reservations.is_empty() {
        return Ok(());
    }
    let pending = std::mem::take(&mut manifest.reservations);
    for reservation in pending {
        for target in reservation.targets {
            validate_relative_path(&target.relative_path)?;
            let path = root.join(&target.relative_path);
            validate_reservation_path(root, &path)?;
            if !path.exists() {
                continue;
            }
            validate_tree_path(root, &path)?;
            if manifest
                .entries
                .iter()
                .any(|entry| entry.relative_path == target.relative_path)
            {
                continue;
            }
            let entry = ManagedEntry::new(
                target.relative_path,
                target.kind,
                RetentionClass::Failed,
                now_ms(),
                format!(
                    "{} (recovered from interrupted allocation)",
                    target.description
                ),
            );
            if entry.kind == OwnerKind::Profile
                && !manifest
                    .protected_profile_roots
                    .contains(&entry.relative_path)
            {
                manifest
                    .protected_profile_roots
                    .push(entry.relative_path.clone());
            }
            manifest.entries.push(entry);
        }
    }
    manifest.generation = manifest.generation.saturating_add(1);
    persist_manifest(manifest_path, manifest)
}

fn persist_manifest(manifest_path: &Path, manifest: &OwnerManifest) -> Result<()> {
    let bytes = serde_json::to_vec_pretty(manifest)?;
    let temp = manifest_path.with_extension(format!("json.{}.tmp", Ulid::generate()));
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temp)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&temp, fs::Permissions::from_mode(0o600))?;
    }
    fs::rename(temp, manifest_path).context("persist retention ownership manifest")?;
    #[cfg(unix)]
    File::open(
        manifest_path
            .parent()
            .context("retention manifest has no parent directory")?,
    )?
    .sync_all()?;
    Ok(())
}

fn now_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn paths_overlap(left: &Path, right: &Path) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

struct ManifestLock(File);

impl ManifestLock {
    fn acquire(directory: &Path) -> Result<Self> {
        let path = directory.join("owners.lock");
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path)?;
        #[cfg(unix)]
        if unsafe { libc::flock(std::os::fd::AsRawFd::as_raw_fd(&file), libc::LOCK_EX) } != 0 {
            return Err(std::io::Error::last_os_error().into());
        }
        Ok(Self(file))
    }
}

impl Drop for ManifestLock {
    fn drop(&mut self) {
        #[cfg(unix)]
        unsafe {
            libc::flock(std::os::fd::AsRawFd::as_raw_fd(&self.0), libc::LOCK_UN);
        }
        #[cfg(not(unix))]
        let _ = &self.0;
    }
}

fn validate_relative_path(path: &Path) -> Result<()> {
    if path.as_os_str().is_empty() || path.is_absolute() {
        bail!("managed path must be a nonempty relative path")
    }
    for component in path.components() {
        match component {
            Component::Normal(_) => {}
            _ => bail!("managed path must not contain traversal or root components"),
        }
    }
    Ok(())
}

fn validate_tree_path(root: &Path, path: &Path) -> Result<()> {
    let relative = path
        .strip_prefix(root)
        .context("managed path is outside retention root")?;
    validate_relative_path(relative)?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        let Component::Normal(part) = component else {
            bail!("invalid path component")
        };
        current.push(part);
        let metadata = fs::symlink_metadata(&current)
            .with_context(|| format!("inspect managed path {}", current.display()))?;
        if metadata.file_type().is_symlink() {
            bail!("symlinks are not eligible for retention")
        }
        if !metadata.is_dir() && current != path {
            bail!("a managed path parent is not a directory")
        }
        if current == path && !(metadata.is_dir() || metadata.is_file()) {
            bail!("special files are not eligible for retention")
        }
    }
    let canonical = fs::canonicalize(path)?;
    if !canonical.starts_with(root) || canonical != path {
        bail!("managed path canonicalizes outside or differs from its registered path")
    }
    Ok(())
}

fn validate_reservation_path(root: &Path, path: &Path) -> Result<()> {
    let relative = path
        .strip_prefix(root)
        .context("reservation target is outside retention root")?;
    validate_relative_path(relative)?;
    let mut current = root.to_path_buf();
    for component in relative.components() {
        let Component::Normal(part) = component else {
            bail!("invalid reservation path component")
        };
        current.push(part);
        match fs::symlink_metadata(&current) {
            Ok(metadata) => {
                if metadata.file_type().is_symlink() {
                    bail!("reservation target has a symlink component")
                }
                if !metadata.is_dir() {
                    bail!("reservation target parent is not a directory")
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn tree_size(path: &Path) -> Result<u64> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        bail!("symlink encountered during size scan")
    }
    if metadata.is_file() {
        return Ok(metadata.len());
    }
    if !metadata.is_dir() {
        // symlink_metadata above identifies these without opening or following
        // them. Sockets, FIFOs, devices, and other special nodes occupy no
        // managed file-content budget.
        return Ok(0);
    }
    let mut total = 0_u64;
    for child in fs::read_dir(path)? {
        total = total.saturating_add(tree_size(&child?.path())?);
    }
    Ok(total)
}

fn tree_size_excluding(path: &Path, excluded: &[PathBuf]) -> Result<u64> {
    if excluded
        .iter()
        .any(|excluded_path| path.starts_with(excluded_path))
    {
        return Ok(0);
    }
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        bail!("symlink encountered during size scan")
    }
    if metadata.is_file() {
        return Ok(metadata.len());
    }
    if !metadata.is_dir() {
        // Keep the accounting walk metadata-only for non-regular nodes.
        return Ok(0);
    }
    let mut total = 0_u64;
    for child in fs::read_dir(path)? {
        total = total.saturating_add(tree_size_excluding(&child?.path(), excluded)?);
    }
    Ok(total)
}

fn remove_owned_path(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path)?;
    if metadata.file_type().is_symlink() {
        bail!("refusing to remove symlink")
    }
    if metadata.is_dir() {
        fs::remove_dir_all(path)?;
    } else if metadata.is_file() {
        fs::remove_file(path)?;
    } else {
        bail!("refusing to remove special file")
    }
    Ok(())
}

fn canonical_active_paths(root: &Path, paths: &[PathBuf]) -> Vec<PathBuf> {
    paths
        .iter()
        .filter_map(|path| {
            let absolute = if path.is_absolute() {
                path.clone()
            } else {
                root.join(path)
            };
            fs::canonicalize(&absolute)
                .ok()
                .filter(|canonical| canonical.starts_with(root))
        })
        .collect()
}

fn overlaps(left: &Path, right: &Path) -> bool {
    left.starts_with(right) || right.starts_with(left)
}

fn is_legacy_name(name: &str) -> bool {
    let suffix = name
        .strip_prefix("work-pw")
        .or_else(|| name.strip_prefix("pw"))
        .or_else(|| name.strip_prefix("sd."));
    suffix.is_some_and(|suffix| {
        !suffix.is_empty()
            && (suffix.chars().all(|ch| ch.is_ascii_digit()) || name.starts_with("sd."))
    })
}

pub fn unix_time_ms(time: SystemTime) -> u128 {
    time.duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
