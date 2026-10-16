use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StateMode {
    #[default]
    Isolated,
    CopyUser,
    Profile,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StateSeed {
    pub source: PathBuf,
    /// Relative to the private state directory (home/config/data/cache/state/tmp).
    pub destination: PathBuf,
    #[serde(default)]
    pub include_credentials: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ShellSpec {
    pub executable: String,
    pub script: String,
    #[serde(default)]
    pub login: bool,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LaunchOptions {
    pub state_mode: StateMode,
    pub profile: Option<String>,
    pub seeds: Vec<StateSeed>,
    pub env_inherit: Option<Vec<String>>,
    pub env_unset: Vec<String>,
    pub shell: Option<ShellSpec>,
    pub workspace: Option<crate::workspace::WorkspaceSpec>,
    pub isolation: crate::isolation::IsolationSpec,
    pub disposable: bool,
    /// Argument positions whose values must not be persisted or returned in metadata.
    pub sensitive_args: Vec<usize>,
    pub metadata: BTreeMap<String, String>,
}
