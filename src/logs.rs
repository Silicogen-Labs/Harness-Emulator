//! Incremental reads for explicitly registered application log files.

use crate::artifacts::{
    ArtifactMetadata, ArtifactSourceKind, ArtifactStore, MAX_ARTIFACT_READ, open_relative_file,
};
use anyhow::{Context, Result, bail};
use rmcp::schemars;
use serde::{Deserialize, Serialize};
use std::io::{Read, Seek, SeekFrom};
use std::os::unix::fs::MetadataExt;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct LogCursor {
    pub artifact_id: String,
    pub device: u64,
    pub inode: u64,
    pub offset: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum LogGap {
    Rotated,
    Truncated,
}

#[derive(Debug, Clone)]
pub struct LogPoll {
    pub artifact: ArtifactMetadata,
    pub cursor: LogCursor,
    pub gap: Option<LogGap>,
    pub bytes: Vec<u8>,
    pub eof: bool,
}

pub struct LogStore<'a> {
    artifacts: &'a ArtifactStore,
}

impl<'a> LogStore<'a> {
    pub fn new(artifacts: &'a ArtifactStore) -> Self {
        Self { artifacts }
    }

    pub fn register_log(
        &self,
        session_id: &str,
        root_id: &str,
        relative_path: &Path,
        content_type: impl Into<String>,
        run_id: Option<String>,
    ) -> Result<ArtifactMetadata> {
        self.artifacts.register_file(
            session_id,
            root_id,
            relative_path,
            ArtifactSourceKind::ApplicationLog,
            content_type,
            run_id,
            Some("session".to_owned()),
        )
    }

    /// Polls at most 1 MiB. A changed device/inode reports rotation; a smaller
    /// size on the same inode reports truncation. Either gap restarts at byte 0.
    pub fn poll(
        &self,
        session_id: &str,
        artifact_id: &str,
        cursor: Option<&LogCursor>,
        max_bytes: usize,
    ) -> Result<LogPoll> {
        if max_bytes == 0 || max_bytes > MAX_ARTIFACT_READ {
            bail!("log poll size must be between 1 and {MAX_ARTIFACT_READ} bytes")
        }
        let artifact = self.artifacts.metadata(session_id, artifact_id)?;
        if artifact.source_kind != ArtifactSourceKind::ApplicationLog {
            bail!("artifact is not a registered application log")
        }
        if let Some(cursor) = cursor
            && cursor.artifact_id != artifact_id
        {
            bail!("log cursor belongs to a different artifact")
        }
        let (root_fd, relative_path) = self
            .artifacts
            .registered_file_path(session_id, artifact_id)?;
        let mut file = open_relative_file(root_fd, &relative_path)?;
        let metadata = file.metadata().context("stat registered log")?;
        let (device, inode, size) = (metadata.dev(), metadata.ino(), metadata.len());
        let mut gap = None;
        let mut offset = cursor.map_or(0, |c| c.offset);
        if let Some(old) = cursor {
            if old.device != device || old.inode != inode {
                offset = 0;
                gap = Some(LogGap::Rotated);
            } else if offset > size {
                offset = 0;
                gap = Some(LogGap::Truncated);
            }
        }
        let count = (size.saturating_sub(offset)).min(max_bytes as u64) as usize;
        file.seek(SeekFrom::Start(offset))?;
        let mut bytes = vec![0; count];
        let mut filled = 0;
        while filled < count {
            match file.read(&mut bytes[filled..])? {
                0 => break,
                n => filled += n,
            }
        }
        bytes.truncate(filled);
        let next = offset + filled as u64;
        let cursor = LogCursor {
            artifact_id: artifact_id.to_owned(),
            device,
            inode,
            offset: next,
        };
        Ok(LogPoll {
            artifact,
            cursor,
            gap,
            bytes,
            eof: next >= size,
        })
    }
}
