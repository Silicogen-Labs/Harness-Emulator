//! Explicit, run-scoped outcomes. Exit/close/disposable are never test verdicts.
use anyhow::{Context, Result, bail};
use rmcp::schemars::{self, JsonSchema};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, fs, io::Read, path::Path};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Passed,
    Failed,
    Cancelled,
    Interrupted,
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FinalizeArgs {
    pub session_id: String,
    /// The observed application run, not a scenario/job ID.
    pub expected_run_id: String,
    pub outcome: Outcome,
    /// Required while another automation actor owns input. Never steals browser control.
    pub actor_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FinalizationRecord {
    pub version: u32,
    pub session_id: String,
    pub run_id: String,
    pub outcome: Outcome,
    /// capturing, pending, retained, disposed, or failed. Retry pending/failed with identical args.
    pub disposal: String,
    pub finalized_at_ms: u128,
    pub evidence: Option<Value>,
    pub reason: Option<String>,
}

impl FinalizationRecord {
    pub fn new(args: &FinalizeArgs) -> Self {
        Self {
            version: 1,
            session_id: args.session_id.clone(),
            run_id: args.expected_run_id.clone(),
            outcome: args.outcome,
            disposal: "capturing".into(),
            finalized_at_ms: crate::engine::now_ms(),
            evidence: None,
            reason: None,
        }
    }
    pub fn check(&self, args: &FinalizeArgs) -> Result<()> {
        if self.version != 1 {
            bail!("unsupported_finalization_version: cannot interpret this outcome journal");
        }
        if self.session_id != args.session_id || self.run_id != args.expected_run_id {
            bail!("stale_run: finalization belongs to a different application run");
        }
        if self.outcome != args.outcome {
            bail!("outcome_conflict: a finalized outcome cannot be changed; launch a new session");
        }
        Ok(())
    }
}

/// Bounded hashes of the independent snapshot, never source-file contents.
/// Truncation is explicit; this is diagnostic evidence, not a complete VCS diff.
pub fn workspace_manifest(root: &Path) -> Result<Value> {
    let mut hashes = BTreeMap::new();
    let mut budget = 64 * 1024 * 1024_u64;
    let mut complete = true;
    let mut visited = 0;
    manifest_tree(
        root,
        root,
        &mut hashes,
        &mut budget,
        &mut complete,
        &mut visited,
    )?;
    Ok(
        json!({"files":hashes,"complete":complete,"limits":{"max_files":128,"max_hash_bytes":64*1024*1024}}),
    )
}

fn manifest_tree(
    root: &Path,
    path: &Path,
    hashes: &mut BTreeMap<String, String>,
    budget: &mut u64,
    complete: &mut bool,
    visited: &mut usize,
) -> Result<()> {
    let mut entries = fs::read_dir(path)?
        .take(1025)
        .collect::<std::io::Result<Vec<_>>>()?;
    if entries.len() > 1024 {
        *complete = false;
        entries.truncate(1024);
    }
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        *visited += 1;
        if *visited > 4096 {
            *complete = false;
            break;
        }
        if hashes.len() >= 128 {
            *complete = false;
            break;
        }
        let path = entry.path();
        let relative = path.strip_prefix(root)?.to_string_lossy().into_owned();
        if relative.len() > 512 {
            *complete = false;
            continue;
        }
        let meta = fs::symlink_metadata(&path)?;
        if meta.file_type().is_symlink() {
            *complete = false;
            continue;
        }
        if meta.is_dir() {
            manifest_tree(root, &path, hashes, budget, complete, visited)?;
        } else if meta.is_file() && meta.len() <= *budget {
            use std::os::unix::fs::OpenOptionsExt;
            let mut file = fs::OpenOptions::new()
                .read(true)
                .custom_flags(libc::O_NOFOLLOW)
                .open(&path)?;
            let mut digest = Sha256::new();
            let mut chunk = [0_u8; 65536];
            loop {
                let read = file.read(&mut chunk)?;
                if read == 0 {
                    break;
                }
                if read as u64 > *budget {
                    *complete = false;
                    break;
                }
                *budget -= read as u64;
                digest.update(&chunk[..read]);
            }
            hashes.insert(relative, format!("{:x}", digest.finalize()));
        } else {
            *complete = false;
        }
    }
    Ok(())
}

pub fn changed_files(before: &Value, after: &Value) -> Value {
    let empty = serde_json::Map::new();
    let old = before["files"].as_object().unwrap_or(&empty);
    let new = after["files"].as_object().unwrap_or(&empty);
    let mut changed = BTreeMap::new();
    for (path, hash) in new {
        if old.get(path) != Some(hash) {
            changed.insert(path, hash.clone());
        }
    }
    for path in old.keys() {
        if !new.contains_key(path) {
            changed.insert(
                path,
                if after["complete"] == true {
                    Value::Null
                } else {
                    json!({"status":"unobserved_in_truncated_scan"})
                },
            );
        }
    }
    json!({"files":changed,"complete":before["complete"] == true && after["complete"] == true})
}

/// Redacts known imported credentials and environment values. Not a general
/// secret detector: fixtures should use synthetic credentials and avoid logging secrets.
#[derive(Default, Clone)]
pub struct Redactor {
    values: Vec<String>,
}
impl Redactor {
    pub fn add(&mut self, value: &str) {
        if value.len() >= 3 && !self.values.iter().any(|old| old == value) {
            self.values.push(value.into());
            self.values
                .sort_by_key(|value| std::cmp::Reverse(value.len()));
        }
    }
    pub fn credential_file(&mut self, path: &Path) -> Result<()> {
        use std::os::unix::fs::OpenOptionsExt;
        let file = fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NOFOLLOW)
            .open(path)?;
        if file.metadata()?.len() > 1024 * 1024 {
            bail!("credential seed too large for bounded evidence redaction");
        }
        let mut text = String::new();
        file.take(1024 * 1024 + 1)
            .read_to_string(&mut text)
            .context("credential seed must be UTF-8 for evidence redaction")?;
        self.add(text.trim());
        if let Ok(value) = serde_json::from_str::<Value>(&text) {
            self.add_json(&value);
        }
        Ok(())
    }
    fn add_json(&mut self, value: &Value) {
        match value {
            Value::String(text) => self.add(text),
            Value::Array(values) => {
                for value in values {
                    self.add_json(value);
                }
            }
            Value::Object(values) => {
                for value in values.values() {
                    self.add_json(value);
                }
            }
            _ => {}
        }
    }
    pub fn text(&self, text: &str) -> String {
        let mut text = text.to_owned();
        for secret in &self.values {
            text = text.replace(secret, "<redacted>");
        }
        text
    }
    pub fn value(&self, value: &Value) -> Value {
        match value {
            Value::String(text) => Value::String(self.text(text)),
            Value::Array(items) => {
                Value::Array(items.iter().map(|item| self.value(item)).collect())
            }
            Value::Object(fields) => Value::Object(
                fields
                    .iter()
                    .map(|(key, value)| (key.clone(), self.value(value)))
                    .collect(),
            ),
            other => other.clone(),
        }
    }
    pub fn frame(&self, frame: &mut crate::protocol::FrameSnapshot) {
        // Mask entire affected rows, preserving cell geometry. Concatenation
        // also catches known credentials wrapped across terminal lines.
        let all: String = frame
            .cells
            .iter()
            .flatten()
            .map(|cell| cell.text.as_str())
            .collect();
        let ranges: Vec<_> = self
            .values
            .iter()
            .flat_map(|secret| {
                all.match_indices(secret)
                    .map(|(start, _)| start..start + secret.len())
            })
            .collect();
        let mut offset = 0;
        for row in &mut frame.cells {
            let end = offset + row.iter().map(|cell| cell.text.len()).sum::<usize>();
            if ranges
                .iter()
                .any(|range| range.start < end && range.end > offset)
            {
                for cell in row.iter_mut() {
                    cell.text = "*".into();
                    cell.hyperlink = None;
                }
            }
            offset = end;
        }
        frame.text = frame
            .cells
            .iter()
            .map(|row| {
                row.iter()
                    .map(|cell| cell.text.as_str())
                    .collect::<String>()
            })
            .collect::<Vec<_>>()
            .join("\n");
        frame.styled_runs.clear();
        frame.title = frame.title.as_ref().map(|title| self.text(title));
    }
}

/// Read a bounded assertion report without treating a broken existing store as
/// an empty successful history. A missing store is valid when no assertions ran.
pub fn load_sanitized_assertions(path: &Path, redactor: &Redactor) -> (Value, bool) {
    const MAX_BYTES: u64 = 256 * 1024;
    const MAX_ITEMS: usize = 64;
    let empty = || json!([]);
    let Some(parent) = path.parent() else {
        return (empty(), false);
    };
    if crate::profiles::ensure_no_symlink_components(parent).is_err() {
        return (empty(), false);
    }
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return (empty(), true),
        Err(_) => return (empty(), false),
    };
    if metadata.file_type().is_symlink() || !metadata.is_file() || metadata.len() > MAX_BYTES {
        return (empty(), false);
    }

    use std::os::unix::fs::OpenOptionsExt;
    let mut file = match fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)
    {
        Ok(file) => file,
        Err(_) => return (empty(), false),
    };
    let mut bytes = Vec::new();
    if file
        .by_ref()
        .take(MAX_BYTES + 1)
        .read_to_end(&mut bytes)
        .is_err()
        || bytes.len() as u64 > MAX_BYTES
    {
        return (empty(), false);
    }
    let Ok(value) = serde_json::from_slice::<Value>(&bytes) else {
        return (empty(), false);
    };
    let Some(items) = value.as_array() else {
        return (empty(), false);
    };
    if items.len() > MAX_ITEMS
        || items.iter().any(|item| {
            !item["assertion"].is_object()
                || !item["result"].is_object()
                || !item["frame_id"].is_string()
                || !item["run_id"].is_string()
                || !item["result"]["passed"].is_boolean()
                || !item["result"]["checked"].is_number()
                || !item["result"]["failures"].is_array()
        })
    {
        return (empty(), false);
    }
    (redactor.value(&value), true)
}
