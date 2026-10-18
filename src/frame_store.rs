//! Immutable named terminal frame baselines. Pixel comparison is deliberately
//! absent: the runtime's renderer currently depends on host fonts and DPR.

use crate::artifacts::validate_relative;
use crate::protocol::{CellSnapshot, FrameSnapshot};
use anyhow::{Context, Result, bail};
use rmcp::schemars;
use serde::{Deserialize, Serialize};
use std::ffi::CString;
use std::fs::File;
use std::io::{Read, Write};
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
use std::path::{Path, PathBuf};
use ulid::Ulid;

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SavedFrame {
    pub baseline_id: String,
    pub name: String,
    pub session_id: String,
    pub run_id: String,
    pub frame_id: String,
    pub saved_at_ms: u128,
    pub render_profile: String,
    pub frame: FrameSnapshot,
}

#[derive(Serialize, Deserialize, schemars::JsonSchema)]
struct StoredFrame {
    baseline_id: String,
    name: String,
    session_id: String,
    run_id: String,
    frame_id: String,
    saved_at_ms: u128,
    render_profile: String,
    frame: FrameSnapshot,
    cells: Vec<Vec<CellSnapshot>>,
}

pub struct FrameStore {
    root: OwnedFd,
    display_path: PathBuf,
}

impl FrameStore {
    pub fn new(runtime_root: PathBuf) -> Result<Self> {
        let runtime = open_absolute_dir(&runtime_root)?;
        let dir_name = CString::new("frame-baselines").unwrap();
        let mkdir_result = unsafe { libc::mkdirat(runtime.as_raw_fd(), dir_name.as_ptr(), 0o700) };
        if mkdir_result < 0 {
            let error = std::io::Error::last_os_error();
            if error.kind() != std::io::ErrorKind::AlreadyExists {
                return Err(error).context("create frame baseline directory");
            }
        }
        let fd = unsafe {
            libc::openat(
                runtime.as_raw_fd(),
                dir_name.as_ptr(),
                libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            )
        };
        if fd < 0 {
            bail!(
                "open frame baseline directory: {}",
                std::io::Error::last_os_error()
            )
        }
        Ok(Self {
            root: unsafe { OwnedFd::from_raw_fd(fd) },
            display_path: runtime_root.join("frame-baselines"),
        })
    }

    pub fn save_named(
        &self,
        name: &str,
        frame: &FrameSnapshot,
        render_profile: &str,
    ) -> Result<SavedFrame> {
        validate_name(name)?;
        validate_session_frame(frame)?;
        if render_profile.trim().is_empty() || render_profile.len() > 128 {
            bail!("render profile must contain 1 to 128 bytes")
        }
        let baseline_id = format!("base-{}", Ulid::generate());
        let stored = StoredFrame {
            baseline_id: baseline_id.clone(),
            name: name.to_owned(),
            session_id: frame.session_id.clone(),
            run_id: frame.run_id.clone(),
            frame_id: frame.frame_id.clone(),
            saved_at_ms: now_ms(),
            render_profile: render_profile.to_owned(),
            frame: frame.clone(),
            cells: frame.cells.clone(),
        };
        let encoded = serde_json::to_vec(&stored)?;
        let filename = baseline_filename(&frame.session_id, name)?;
        let file = create_immutable_file(self.root.as_raw_fd(), &filename)?;
        let mut file = file;
        file.write_all(&encoded)?;
        file.sync_all()?;
        Ok(to_saved(stored))
    }

    /// Optional capture path for callers whose observation may be unavailable.
    pub fn save_optional(
        &self,
        name: &str,
        frame: Option<&FrameSnapshot>,
        render_profile: &str,
    ) -> Result<Option<SavedFrame>> {
        match frame {
            Some(frame) => self.save_named(name, frame, render_profile).map(Some),
            None => Ok(None),
        }
    }

    pub fn load_named(&self, session_id: &str, name: &str) -> Result<SavedFrame> {
        validate_identity(session_id, "session id")?;
        validate_name(name)?;
        let filename = baseline_filename(session_id, name)?;
        let file = open_file(self.root.as_raw_fd(), &filename)?;
        let mut bytes = Vec::new();
        file.take(32 * 1024 * 1024).read_to_end(&mut bytes)?;
        let mut stored: StoredFrame =
            serde_json::from_slice(&bytes).context("decode frame baseline")?;
        stored.frame.cells = stored.cells.clone();
        Ok(to_saved(stored))
    }

    pub fn directory(&self) -> &Path {
        &self.display_path
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CellMask {
    pub row: u16,
    pub col: u16,
    pub rows: u16,
    pub cols: u16,
    pub reason: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct CellDelta {
    pub row: u16,
    pub col: u16,
    pub expected: CellSnapshot,
    pub actual: CellSnapshot,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct FrameDiff {
    pub expected_baseline_id: String,
    pub actual_session_id: String,
    pub actual_run_id: String,
    pub actual_frame_id: String,
    pub cols: u16,
    pub rows: u16,
    pub changed_cell_count: usize,
    pub changed_regions: Vec<ChangedRegion>,
    pub changed_cells: Vec<CellDelta>,
    pub masks_applied: Vec<CellMask>,
    pub pixel_comparison: bool,
    pub note: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ChangedRegion {
    pub row: u16,
    pub col: u16,
    pub rows: u16,
    pub cols: u16,
    pub changed_cells: usize,
}

pub fn diff_frames(
    expected: &SavedFrame,
    actual: &FrameSnapshot,
    actual_render_profile: &str,
    masks: &[CellMask],
) -> Result<FrameDiff> {
    validate_session_frame(actual)?;
    if expected.session_id != actual.session_id {
        bail!("frame diff session mismatch")
    }
    if expected.frame.cols != actual.cols || expected.frame.rows != actual.rows {
        bail!(
            "frame geometry mismatch: expected {}x{}, actual {}x{}",
            expected.frame.cols,
            expected.frame.rows,
            actual.cols,
            actual.rows
        )
    }
    if expected.render_profile.trim().is_empty() || actual_render_profile.trim().is_empty() {
        bail!("frame diff requires both render profiles")
    }
    if expected.render_profile != actual_render_profile {
        bail!(
            "render profile mismatch: expected {:?}, actual {:?}",
            expected.render_profile,
            actual_render_profile
        )
    }
    validate_session_frame(&expected.frame)?;
    for mask in masks {
        if mask.rows == 0
            || mask.cols == 0
            || mask.row.saturating_add(mask.rows) > actual.rows
            || mask.col.saturating_add(mask.cols) > actual.cols
        {
            bail!("frame mask is empty or outside frame geometry")
        }
        if mask.reason.trim().is_empty() {
            bail!("every frame mask needs an explicit reason")
        }
    }
    let mut changed_cells = Vec::new();
    for row in 0..actual.rows as usize {
        for col in 0..actual.cols as usize {
            if masks.iter().any(|m| {
                row >= m.row as usize
                    && row < (m.row + m.rows) as usize
                    && col >= m.col as usize
                    && col < (m.col + m.cols) as usize
            }) {
                continue;
            }
            let left = expected.frame.cells.get(row).and_then(|r| r.get(col));
            let right = actual.cells.get(row).and_then(|r| r.get(col));
            if !matches!((left, right), (Some(left), Some(right)) if cells_equal(left, right))
                && let (Some(left), Some(right)) = (left, right)
            {
                changed_cells.push(CellDelta {
                    row: row as u16,
                    col: col as u16,
                    expected: left.clone(),
                    actual: right.clone(),
                });
            }
        }
    }
    let changed_cell_count = changed_cells.len();
    let mut changed_regions = Vec::new();
    let mut start = 0;
    while start < changed_cells.len() {
        let row = changed_cells[start].row;
        let first_col = changed_cells[start].col;
        let mut end = start + 1;
        let mut last_col = first_col;
        while end < changed_cells.len()
            && changed_cells[end].row == row
            && changed_cells[end].col == last_col.saturating_add(1)
        {
            last_col = changed_cells[end].col;
            end += 1;
        }
        changed_regions.push(ChangedRegion {
            row,
            col: first_col,
            rows: 1,
            cols: last_col - first_col + 1,
            changed_cells: end - start,
        });
        start = end;
    }
    Ok(FrameDiff {
        expected_baseline_id: expected.baseline_id.clone(),
        actual_session_id: actual.session_id.clone(),
        actual_run_id: actual.run_id.clone(),
        actual_frame_id: actual.frame_id.clone(),
        cols: actual.cols,
        rows: actual.rows,
        changed_cell_count,
        changed_regions,
        changed_cells,
        masks_applied: masks.to_vec(),
        pixel_comparison: false,
        note:
            "Cell and style comparison only; pixels depend on an unpinned host renderer and fonts."
                .to_owned(),
    })
}

fn create_immutable_file(root_fd: i32, name: &str) -> Result<File> {
    let path = Path::new(name);
    validate_relative(path)?;
    let c_name = CString::new(name).context("invalid baseline filename")?;
    let fd = unsafe {
        libc::openat(
            root_fd,
            c_name.as_ptr(),
            libc::O_WRONLY | libc::O_CREAT | libc::O_EXCL | libc::O_CLOEXEC | libc::O_NOFOLLOW,
            0o600,
        )
    };
    if fd < 0 {
        bail!(
            "create immutable baseline: {}",
            std::io::Error::last_os_error()
        )
    }
    Ok(unsafe { File::from_raw_fd(fd) })
}

fn open_file(root_fd: i32, name: &str) -> Result<File> {
    let c_name = CString::new(name).context("invalid baseline filename")?;
    let fd = unsafe {
        libc::openat(
            root_fd,
            c_name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC | libc::O_NOFOLLOW | libc::O_NONBLOCK,
        )
    };
    if fd < 0 {
        return Err(std::io::Error::last_os_error()).context("open named baseline");
    }
    let file = unsafe { File::from_raw_fd(fd) };
    let metadata = file.metadata()?;
    if !metadata.is_file() {
        bail!("baseline is not a regular file")
    }
    use std::os::unix::fs::MetadataExt;
    if metadata.nlink() > 1 {
        bail!("hard-linked baselines are denied")
    }
    Ok(file)
}

fn open_absolute_dir(path: &Path) -> Result<OwnedFd> {
    if !path.is_absolute() {
        bail!("runtime root must be absolute")
    }
    let root = CString::new("/").unwrap();
    let fd = unsafe {
        libc::open(
            root.as_ptr(),
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        bail!("open filesystem root: {}", std::io::Error::last_os_error())
    }
    let mut current = unsafe { OwnedFd::from_raw_fd(fd) };
    for component in path.components() {
        match component {
            std::path::Component::RootDir => {}
            std::path::Component::Normal(name) => {
                let name =
                    CString::new(name.as_encoded_bytes()).context("invalid path component")?;
                let next = unsafe {
                    libc::openat(
                        current.as_raw_fd(),
                        name.as_ptr(),
                        libc::O_RDONLY | libc::O_DIRECTORY | libc::O_CLOEXEC | libc::O_NOFOLLOW,
                    )
                };
                if next < 0 {
                    bail!(
                        "open runtime directory component: {}",
                        std::io::Error::last_os_error()
                    )
                }
                current = unsafe { OwnedFd::from_raw_fd(next) };
            }
            _ => bail!("runtime root cannot contain traversal components"),
        }
    }
    Ok(current)
}

fn baseline_filename(session_id: &str, name: &str) -> Result<String> {
    validate_identity(session_id, "session id")?;
    validate_name(name)?;
    Ok(format!("{session_id}--{name}.json"))
}

fn validate_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 80
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
        || name == "."
        || name == ".."
    {
        bail!("baseline name must be 1 to 80 ASCII letters, digits, dot, underscore, or dash")
    }
    Ok(())
}

fn validate_identity(value: &str, label: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        bail!("invalid {label}")
    }
    Ok(())
}

fn validate_session_frame(frame: &FrameSnapshot) -> Result<()> {
    validate_identity(&frame.session_id, "session id")?;
    if frame.cols == 0 || frame.rows == 0 || frame.cols > 300 || frame.rows > 120 {
        bail!("frame dimensions are outside supported bounds")
    }
    if frame.cells.len() < frame.rows as usize
        || frame
            .cells
            .iter()
            .take(frame.rows as usize)
            .any(|r| r.len() < frame.cols as usize)
    {
        bail!("frame does not contain complete cell data")
    }
    Ok(())
}

fn to_saved(stored: StoredFrame) -> SavedFrame {
    SavedFrame {
        baseline_id: stored.baseline_id,
        name: stored.name,
        session_id: stored.session_id,
        run_id: stored.run_id,
        frame_id: stored.frame_id,
        saved_at_ms: stored.saved_at_ms,
        render_profile: stored.render_profile,
        frame: stored.frame,
    }
}

fn cells_equal(a: &CellSnapshot, b: &CellSnapshot) -> bool {
    a.text == b.text
        && a.width == b.width
        && a.foreground == b.foreground
        && a.background == b.background
        && a.resolved_foreground == b.resolved_foreground
        && a.resolved_background == b.resolved_background
        && a.bold == b.bold
        && a.dim == b.dim
        && a.italic == b.italic
        && a.inverse == b.inverse
        && a.invisible == b.invisible
        && a.strike == b.strike
        && a.blink == b.blink
        && a.underline == b.underline
        && a.underline_color == b.underline_color
        && a.hyperlink == b.hyperlink
}

fn now_ms() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}
