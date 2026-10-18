use crate::control::{BrowserAuthority, BrowserLease, ControlStatus};
use crate::protocol::{HistorySnapshot, RpcRequest, SessionSummary, WaitPredicate, WaitResult};
use crate::session::{
    BrowserActionContext, BrowserInput, BrowserInputResult, Session, atomic_json_write,
    create_private_dir,
};
use anyhow::{Context, Result, bail};
use regex::Regex;
use serde_json::{Value, json};
use std::collections::{HashMap, VecDeque};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Result of one shared session-wait execution. Both legacy waits and
/// resumable contract-v2 waits use this driver so they share output wakeups,
/// timer ticks, cancellation checks, and deadline semantics.
pub(crate) enum WaitStop {
    Predicate,
    Cancelled,
    Deadline,
    PollComplete,
}

pub(crate) struct WaitStep<T> {
    pub value: T,
    pub terminal: bool,
    pub next_wakeup: Option<Instant>,
}

pub(crate) fn execute_session_wait<T>(
    session: &Session,
    deadline: Instant,
    poll_deadline: Option<Instant>,
    cancelled: impl Fn() -> bool,
    mut evaluate: impl FnMut(&crate::protocol::FrameSnapshot, u64, u64, Instant) -> Result<WaitStep<T>>,
) -> Result<(T, WaitStop)> {
    const TIMER_TICK: Duration = Duration::from_millis(25);
    loop {
        let (frame, revision, viewport_revision) = session.observation_snapshot();
        let now = Instant::now();
        let step = evaluate(&frame, revision, viewport_revision, now)?;
        if step.terminal {
            return Ok((step.value, WaitStop::Predicate));
        }
        if cancelled() {
            return Ok((step.value, WaitStop::Cancelled));
        }
        if now >= deadline {
            return Ok((step.value, WaitStop::Deadline));
        }
        if poll_deadline.is_some_and(|poll| now >= poll) {
            return Ok((step.value, WaitStop::PollComplete));
        }
        let mut wake_at = now + TIMER_TICK;
        wake_at = wake_at.min(deadline);
        if let Some(poll) = poll_deadline {
            wake_at = wake_at.min(poll);
        }
        if let Some(timer) = step.next_wakeup {
            wake_at = wake_at.min(timer);
        }
        session.wait_for_change(revision, wake_at.saturating_duration_since(Instant::now()));
    }
}

const OBSERVATION_BASELINE_TTL: Duration = Duration::from_secs(60);
const OBSERVATION_BASELINES_PER_SESSION: usize = 8;
const OBSERVATION_BASELINES_TOTAL: usize = 32;
const OBSERVATION_BASELINES_BYTES: usize = 16 * 1024 * 1024;

struct CachedWaitBaseline {
    baseline: crate::wait_engine::WaitBaseline,
    retained_bytes: usize,
    captured_at: Instant,
}

#[derive(Default)]
struct ObservationBaselineCache {
    entries: VecDeque<CachedWaitBaseline>,
    bytes: usize,
}

impl ObservationBaselineCache {
    fn expire(&mut self, now: Instant) {
        self.entries.retain(|entry| {
            now.saturating_duration_since(entry.captured_at) < OBSERVATION_BASELINE_TTL
        });
        self.bytes = self.entries.iter().map(|entry| entry.retained_bytes).sum();
    }

    fn remember(
        &mut self,
        baseline: crate::wait_engine::WaitBaseline,
        retained_bytes: usize,
        now: Instant,
    ) -> bool {
        if retained_bytes > 8 * 1024 * 1024 {
            return false;
        }
        self.expire(now);
        let identity = &baseline.identity;
        self.entries.retain(|entry| {
            let old = &entry.baseline.identity;
            !(old.session_id == identity.session_id
                && old.run_id == identity.run_id
                && old.frame_id == identity.frame_id
                && old.observation_revision == identity.observation_revision
                && old.viewport_revision == identity.viewport_revision)
        });
        self.bytes = self.entries.iter().map(|entry| entry.retained_bytes).sum();
        loop {
            let session_count = self
                .entries
                .iter()
                .filter(|entry| entry.baseline.identity.session_id == identity.session_id)
                .count();
            if self.entries.len() < OBSERVATION_BASELINES_TOTAL
                && session_count < OBSERVATION_BASELINES_PER_SESSION
                && self.bytes.saturating_add(retained_bytes) <= OBSERVATION_BASELINES_BYTES
            {
                break;
            }
            let Some(old) = self.entries.pop_front() else {
                return false;
            };
            self.bytes = self.bytes.saturating_sub(old.retained_bytes);
        }
        self.bytes = self.bytes.saturating_add(retained_bytes);
        self.entries.push_back(CachedWaitBaseline {
            baseline,
            retained_bytes,
            captured_at: now,
        });
        true
    }

    fn resolve(
        &mut self,
        session_id: &str,
        identity: &crate::locators::ObservationIdentity,
        now: Instant,
    ) -> anyhow::Result<(crate::wait_engine::WaitBaseline, usize)> {
        use crate::validation::RequestError;
        self.expire(now);
        if identity.session_id != session_id {
            return Err(
                RequestError::new("baseline", "observation from this session")
                    .code("baseline_incompatible")
                    .into(),
            );
        }
        if let Some(found) = self
            .entries
            .iter()
            .find(|entry| entry.baseline.identity == *identity)
        {
            return Ok((found.baseline.clone(), found.retained_bytes));
        }
        if self.entries.iter().any(|entry| {
            let old = &entry.baseline.identity;
            old.session_id == identity.session_id
                && old.run_id == identity.run_id
                && old.frame_id == identity.frame_id
                && old.observation_revision == identity.observation_revision
        }) {
            return Err(RequestError::new(
                "baseline",
                "same run, frame, observation and viewport identity",
            )
            .code("baseline_incompatible")
            .into());
        }
        Err(RequestError::new(
            "baseline",
            "a retained observation baseline (last 8 per session, 60 second admission TTL)",
        )
        .code("baseline_unavailable")
        .into())
    }
}

fn conservative_baseline_bytes(frame: &crate::protocol::FrameSnapshot) -> usize {
    // Baseline clones each cell's text plus a smaller cell record. Charge the
    // source capacities and full CellSnapshot size as a conservative upper bound.
    let grid = frame.cells.iter().fold(
        frame
            .cells
            .capacity()
            .saturating_mul(std::mem::size_of::<Vec<crate::protocol::CellSnapshot>>()),
        |sum, row| {
            sum.saturating_add(
                row.capacity()
                    .saturating_mul(std::mem::size_of::<crate::protocol::CellSnapshot>()),
            )
            .saturating_add(row.iter().fold(0usize, |bytes, cell| {
                bytes.saturating_add(cell.text.capacity())
            }))
        },
    );
    grid.saturating_add(frame.session_id.capacity())
        .saturating_add(frame.run_id.capacity())
        .saturating_add(frame.frame_id.capacity())
        .saturating_add(std::mem::size_of::<crate::locators::ObservationIdentity>())
}

#[derive(Clone)]
pub struct SessionService {
    runtime_root: PathBuf,
    sessions: Arc<Mutex<HashMap<String, Arc<Session>>>>,
    historical: Arc<Mutex<HashMap<String, SessionSummary>>>,
    pub(crate) agent: Arc<crate::automation::AutomationService>,
    lease_sessions: Arc<Mutex<HashMap<String, std::sync::Weak<Session>>>>,
    observations: Arc<Mutex<crate::projection::ObservationCache>>,
    observation_baselines: Arc<Mutex<ObservationBaselineCache>>,
}

impl SessionService {
    pub fn new(runtime_root: impl Into<PathBuf>) -> Result<Self> {
        let runtime_root = runtime_root.into();
        create_private_dir(&runtime_root)?;
        let runtime_root =
            fs::canonicalize(&runtime_root).context("canonicalize runtime directory")?;
        create_private_dir(&runtime_root.join("sessions"))?;
        create_private_dir(&runtime_root.join("artifacts"))?;
        let historical = load_historical_sessions(&runtime_root)?;
        Ok(Self {
            runtime_root: runtime_root.clone(),
            sessions: Arc::new(Mutex::new(HashMap::new())),
            historical: Arc::new(Mutex::new(historical)),
            agent: Arc::new(crate::automation::AutomationService::new(
                runtime_root.clone(),
            )?),
            lease_sessions: Arc::new(Mutex::new(HashMap::new())),
            observations: Arc::new(Mutex::new(crate::projection::ObservationCache::default())),
            observation_baselines: Arc::new(Mutex::new(ObservationBaselineCache::default())),
        })
    }

    pub fn runtime_root(&self) -> &Path {
        &self.runtime_root
    }

    pub fn viewer_take_control(
        &self,
        session_id: &str,
        view_id: &str,
        connection_id: &str,
        permitted: bool,
    ) -> Result<BrowserLease> {
        self.active_session(session_id)?
            .take_browser_control(view_id, connection_id, permitted)
    }

    pub fn viewer_renew_control(
        &self,
        session_id: &str,
        authority: &BrowserAuthority,
    ) -> Result<ControlStatus> {
        self.active_session(session_id)?
            .renew_browser_control(authority)
    }

    pub fn viewer_release_control(
        &self,
        session_id: &str,
        authority: &BrowserAuthority,
    ) -> Result<ControlStatus> {
        self.active_session(session_id)?
            .release_browser_control(authority)
    }

    pub fn viewer_input(
        &self,
        session_id: &str,
        authority: &BrowserAuthority,
        context: &BrowserActionContext,
        input: BrowserInput,
    ) -> Result<BrowserInputResult> {
        self.active_session(session_id)?
            .browser_input(authority, context, input)
    }

    pub fn viewer_session(&self, session_id: &str) -> Result<Arc<Session>> {
        self.active_session(session_id)
    }

    fn launch_registered(
        &self,
        mut spec: crate::protocol::LaunchSpec,
        mut options: crate::config::LaunchOptions,
    ) -> Result<Arc<Session>> {
        const LAUNCH_METADATA_HEADROOM: u64 = 256 * 1024;

        crate::launch::resolve_options(&self.runtime_root, &mut spec, &mut options)?;

        let session_id = ulid::Ulid::generate().to_string();
        let seed_bytes = crate::launch::estimate_state_seed_bytes(&self.runtime_root, &options)?;
        let workspace_bytes = options
            .workspace
            .as_ref()
            .map(crate::workspace::estimate_workspace_bytes)
            .transpose()?;
        let reserved_bytes = seed_bytes
            .checked_add(workspace_bytes.unwrap_or_default())
            .and_then(|bytes| bytes.checked_add(LAUNCH_METADATA_HEADROOM))
            .context("retention_quota_exceeded: launch allocation size overflow")?;
        let has_workspace = workspace_bytes.is_some();
        let disposable = options.disposable;
        let reservation =
            self.agent
                .reserve_session_allocation(&session_id, reserved_bytes, has_workspace)?;
        let session = match Session::launch_configured_reserved(
            &self.runtime_root,
            spec,
            options,
            session_id.clone(),
            workspace_bytes,
            seed_bytes,
        ) {
            Ok(session) => session,
            Err(error) => {
                self.agent.fail_session_allocation(&reservation.id)?;
                return Err(error);
            }
        };
        if let Err(error) =
            self.agent
                .commit_session_allocation(&reservation.id, &session_id, disposable)
        {
            let _ = session.close_admin();
            session.discard_rejected_allocation()?;
            self.agent.fail_session_allocation(&reservation.id)?;
            return Err(error).context("committing launch allocation reservation");
        }
        Ok(session)
    }

    fn track_session(&self, session: Arc<Session>) {
        let summary = session.summary();
        self.lease_sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(session.id.clone(), Arc::downgrade(&session));
        self.historical
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .remove(&summary.session_id);
        self.sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(summary.session_id, session);
    }

    pub fn handle(&self, request: RpcRequest) -> Result<(Value, bool)> {
        crate::validation::request(&request)?;
        match request {
            RpcRequest::Contract {
                revision,
                request,
                response,
            } => self.handle_contract(revision, *request, response),
            RpcRequest::Agent { operation, params } => {
                Ok((self.agent.handle(self, &operation, params)?, false))
            }
            RpcRequest::LaunchConfigured { spec, options } => {
                let session = self.launch_registered(spec, options)?;
                let summary = session.summary();
                let capabilities = session.capabilities();
                self.track_session(session);
                let mut value = serde_json::to_value(summary)?;
                value["workspace"] = capabilities["launch"]["workspace"]["root"].clone();
                value["capabilities"] = capabilities;
                Ok((value, false))
            }
            RpcRequest::Ping => Ok((
                json!({"status":"ok","protocol_version":1,"contract_revisions":[2],"mcp_contract_revision":2,"response_defaults":{"wait":"summary","observe":"text","cells":"compact","summary_max_bytes":1024,"frame_cache_per_session":8,"frame_cache_ttl_ms":60000}}),
                false,
            )),
            RpcRequest::Launch { spec } => {
                let session =
                    self.launch_registered(spec, crate::config::LaunchOptions::default())?;
                let summary = session.summary();
                self.track_session(session);
                Ok((serde_json::to_value(summary)?, false))
            }
            RpcRequest::List => {
                let active = self
                    .sessions
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let mut list = active
                    .values()
                    .map(|session| session.summary())
                    .collect::<Vec<_>>();
                drop(active);
                let historical = self
                    .historical
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                list.extend(historical.values().cloned());
                list.sort_by_key(|summary| summary.started_at_ms);
                Ok((serde_json::to_value(list)?, false))
            }
            RpcRequest::Status { session_id } => {
                let summary = if let Some(session) = self.optional_active_session(&session_id) {
                    session.summary()
                } else {
                    self.historical_summary(&session_id)?
                };
                let mut value = serde_json::to_value(summary)?;
                let path = self
                    .runtime_root
                    .join("sessions")
                    .join(&session_id)
                    .join("finalization.json");
                if path.exists() {
                    crate::profiles::ensure_no_symlink_components(&path)?;
                    value["finalization"] = serde_json::from_slice(&fs::read(path)?)?;
                }
                Ok((value, false))
            }
            RpcRequest::Observe { session_id } => {
                let session = self.active_session(&session_id)?;
                let (frame, observation_revision, viewport_revision) =
                    session.observation_snapshot();
                let baseline = crate::wait_engine::WaitBaseline::capture(
                    &frame,
                    observation_revision,
                    viewport_revision,
                )?;
                let retained_bytes = conservative_baseline_bytes(&frame);
                self.observation_baselines
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remember(baseline, retained_bytes, Instant::now());
                let mut value = serde_json::to_value(frame)?;
                value["observation_revision"] = json!(observation_revision);
                value["viewport_revision"] = json!(viewport_revision);
                Ok((value, false))
            }
            RpcRequest::Cells {
                session_id,
                row,
                col,
                rows,
                cols,
            } => {
                let cells = self
                    .active_session(&session_id)?
                    .cells(row, col, rows, cols)?;
                Ok((serde_json::to_value(cells)?, false))
            }
            RpcRequest::Screenshot { session_id } => {
                self.agent.check_capacity(self, 1024 * 1024, 0)?;
                let session = self.active_session(&session_id)?;
                let _lease = session.retain_evidence()?;
                let artifact = session.screenshot()?;
                self.agent.register_capture(&artifact)?;
                Ok((serde_json::to_value(artifact)?, false))
            }
            RpcRequest::History { session_id } => {
                let history = if let Some(session) = self.optional_active_session(&session_id) {
                    let _lease = session.retain_evidence()?;
                    let history = session.history();
                    session.persist_history()?;
                    history
                } else {
                    self.historical_history(&session_id)?
                };
                Ok((serde_json::to_value(history)?, false))
            }
            RpcRequest::Key { session_id, key } => {
                self.active_session(&session_id)?.key(&key)?;
                Ok((json!({"accepted":true,"key":key}), false))
            }
            RpcRequest::Type { session_id, text } => {
                self.active_session(&session_id)?.type_text(&text)?;
                Ok((
                    json!({"accepted":true,"characters":text.chars().count()}),
                    false,
                ))
            }
            RpcRequest::Paste { session_id, text } => {
                let bracketed = self.active_session(&session_id)?.paste(&text)?;
                Ok((json!({"accepted":true,"bracketed":bracketed}), false))
            }
            RpcRequest::Mouse { session_id, input } => {
                self.active_session(&session_id)?.mouse(&input)?;
                Ok((
                    json!({"accepted":true,"coordinates":"zero-based row,column","input":input}),
                    false,
                ))
            }
            RpcRequest::Resize {
                session_id,
                cols,
                rows,
            } => {
                self.active_session(&session_id)?.resize(cols, rows)?;
                Ok((json!({"resized":true,"cols":cols,"rows":rows}), false))
            }
            RpcRequest::Wait {
                session_id,
                predicate,
                timeout_ms,
            } => {
                let result = self.wait_for(&session_id, predicate, timeout_ms)?;
                Ok((serde_json::to_value(result)?, false))
            }
            RpcRequest::Restart { session_id } => {
                self.agent.forget_adapters_for_session(&session_id);
                let session = self.active_session(&session_id)?;
                let previous_run = session.summary().run_id;
                let summary = session.restart()?;
                self.agent.finish_recording(&session_id, &previous_run)?;
                self.agent
                    .stop_diagnostics_owner(&session_id, &previous_run)?;
                Ok((serde_json::to_value(summary)?, false))
            }
            RpcRequest::Close { session_id } => {
                self.agent.forget_adapters_for_session(&session_id);
                let session = self.active_session(&session_id)?;
                let run_id = session.summary().run_id;
                let summary = session.close()?;
                self.agent.finish_recording(&session_id, &run_id)?;
                self.agent.stop_diagnostics_owner(&session_id, &run_id)?;
                self.historical
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .insert(session_id.clone(), summary.clone());
                self.sessions
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner)
                    .remove(&session_id);
                Ok((serde_json::to_value(summary)?, false))
            }
            RpcRequest::ControlStatus { session_id } => {
                let status = self.active_session(&session_id)?.control_status()?;
                Ok((serde_json::to_value(status)?, false))
            }
            RpcRequest::ControlRelease {
                session_id,
                expected_generation,
            } => {
                let status = self
                    .active_session(&session_id)?
                    .release_browser_control_from_admin(expected_generation)?;
                Ok((serde_json::to_value(status)?, false))
            }
            RpcRequest::ViewStart { .. }
            | RpcRequest::ViewStatus { .. }
            | RpcRequest::ViewStop { .. } => {
                bail!("viewer requests must be handled by the daemon viewer service")
            }
            RpcRequest::Shutdown => {
                self.shutdown()?;
                Ok((json!({"shutting_down":true}), true))
            }
        }
    }

    pub(crate) fn resolve_wait_baseline(
        &self,
        session_id: &str,
        identity: &crate::locators::ObservationIdentity,
    ) -> Result<(crate::wait_engine::WaitBaseline, usize)> {
        self.observation_baselines
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .resolve(session_id, identity, Instant::now())
    }

    pub fn shutdown(&self) -> Result<()> {
        let sessions = self
            .sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .values()
            .cloned()
            .collect::<Vec<_>>();
        for session in sessions {
            let summary = session.close_admin()?;
            self.agent
                .stop_diagnostics_owner(&summary.session_id, &summary.run_id)?;
            self.historical
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(summary.session_id.clone(), summary);
        }
        Ok(())
    }

    fn handle_contract(
        &self,
        revision: u16,
        mut request: RpcRequest,
        mut response: Option<crate::projection::ResponseOptions>,
    ) -> Result<(Value, bool)> {
        use crate::{
            projection::{CONTRACT_REVISION, ResponseMode},
            validation::RequestError,
        };
        if revision != CONTRACT_REVISION {
            return Err(
                RequestError::new("revision", "supported contract revision 2")
                    .code("contract_mismatch")
                    .into(),
            );
        }
        if matches!(request, RpcRequest::Contract { .. }) {
            return Err(RequestError::new("request", "non-nested contract request").into());
        }
        if let RpcRequest::Agent { params, .. } = &mut request
            && let Some(option) = params
                .as_object_mut()
                .and_then(|object| object.remove("response"))
                .filter(|value| !value.is_null())
        {
            if response.is_some() {
                return Err(RequestError::new("response", "one response options object").into());
            }
            response = Some(crate::validation::decode(option).map_err(|e| e.prefix("response"))?);
        }
        let is_wait = matches!(request, RpcRequest::Wait { .. })
            || matches!(&request,RpcRequest::Agent{operation,..} if matches!(operation.trim_start_matches("terminal_"),"wait"|"wait_fresh"));
        let is_locate = matches!(&request,RpcRequest::Agent{operation,..} if operation.trim_start_matches("terminal_")=="locate");
        let is_fresh_wait = matches!(&request,RpcRequest::Agent{operation,..} if operation.trim_start_matches("terminal_")=="wait_fresh");
        let is_sequence = matches!(&request,RpcRequest::Agent{operation,..} if operation.trim_start_matches("terminal_")=="sequence");
        let is_capture = matches!(&request,RpcRequest::Agent{operation,..} if operation.trim_start_matches("terminal_")=="capture_job");
        let is_cells = matches!(request, RpcRequest::Cells { .. });
        let is_observe = matches!(request, RpcRequest::Observe { .. });
        let is_diff = matches!(&request,RpcRequest::Agent{operation,..} if operation.trim_start_matches("terminal_")=="diff");
        if (is_sequence || is_capture)
            && let Some(options) = response
        {
            let RpcRequest::Agent {
                operation,
                mut params,
            } = request
            else {
                unreachable!()
            };
            params["response"] = serde_json::to_value(options)?;
            return Ok((self.agent.handle(self, &operation, params)?, false));
        }
        if !is_wait && !is_locate && !is_cells && !is_observe && !is_diff {
            if response.is_some() {
                return Err(RequestError::new("response","response projections are only supported by observations, cells, waits and diffs").code("unsupported_response").into());
            }
            return self.handle(request);
        }
        let options = response.unwrap_or_default();
        if is_diff {
            crate::projection::validate_named_diff(&options)?;
        }
        let mode = options.validate(if is_wait || is_diff {
            ResponseMode::Summary
        } else {
            ResponseMode::Text
        })?;
        if is_cells && mode == ResponseMode::Diff {
            return Err(RequestError::new(
                "response.mode",
                "use an observation diff, not a cell-region diff",
            )
            .code("unsupported_response")
            .into());
        }
        if let RpcRequest::Cells {
            session_id,
            row,
            col,
            rows,
            cols,
        } = &request
        {
            crate::validation::request(&request)?;
            let cells = self
                .active_session(session_id)?
                .cells(*row, *col, *rows, *cols)?;
            return Ok((crate::projection::cells(cells, options)?, false));
        }
        let query = match &request {
            RpcRequest::Wait {
                predicate: WaitPredicate::TextContains { text },
                ..
            } => Some((text.clone(), false)),
            RpcRequest::Wait {
                predicate: WaitPredicate::Regex { pattern },
                ..
            } => Some((pattern.clone(), true)),
            RpcRequest::Agent { params, .. } => match params["predicate"]["kind"].as_str() {
                Some("text_contains") => params["predicate"]["text"]
                    .as_str()
                    .map(|s| (s.into(), false)),
                Some("regex") => params["predicate"]["pattern"]
                    .as_str()
                    .map(|s| (s.into(), true)),
                _ => None,
            },
            _ => None,
        };
        if is_diff {
            let RpcRequest::Agent { params, .. } = request else {
                unreachable!()
            };
            let value = self
                .agent
                .compare_named(self, crate::validation::decode(params)?, true)?;
            return Ok((crate::projection::named_diff(value, options)?, false));
        }
        let (value, shutdown) = self.handle(request)?;
        if is_locate || is_fresh_wait {
            return Ok((
                crate::projection::bounded_matches(value, options, is_fresh_wait)?,
                shutdown,
            ));
        }
        let projected = self
            .observations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .project(
                value,
                options,
                is_wait,
                query.as_ref().map(|(s, r)| (s.as_str(), *r)),
                Instant::now(),
            )?;
        Ok((projected, shutdown))
    }

    pub(crate) fn protected_paths(&self) -> Vec<PathBuf> {
        let sessions = self
            .lease_sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let mut paths = Vec::new();
        for (id, weak) in sessions.iter() {
            if let Some(session) = weak.upgrade()
                && (session.summary().state == "running" || session.evidence_retained())
            {
                paths.push(self.runtime_root.join("sessions").join(id));
                paths.push(self.runtime_root.join("artifacts").join(id));
                paths.push(self.runtime_root.join("workspaces").join(id));
            }
        }
        paths.extend(self.agent.protected_jobs());
        paths
    }

    pub(crate) fn begin_reclamation(
        &self,
        session_id: &str,
    ) -> Result<Option<crate::session::ReclamationLease>> {
        let session = self
            .lease_sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(session_id)
            .and_then(std::sync::Weak::upgrade);
        session
            .map(|session| session.begin_reclamation())
            .transpose()
    }

    pub(crate) fn finish_reclamation(
        &self,
        session_id: &str,
        lease: crate::session::ReclamationLease,
        session_directory_removed: bool,
    ) {
        if !session_directory_removed {
            drop(lease);
            return;
        }
        lease.commit();
        let mut sessions = self
            .sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(session) = sessions.remove(session_id) {
            let summary = session.summary();
            self.historical
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(session_id.to_owned(), summary);
        }
    }

    pub(crate) fn active_session(&self, id: &str) -> Result<Arc<Session>> {
        let session = self
            .sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("unknown or inactive session: {id}"))?;
        if session.is_retired() {
            anyhow::bail!(
                "session_reclaimed: session evidence has been removed by retention cleanup"
            )
        }
        Ok(session)
    }

    fn optional_active_session(&self, id: &str) -> Option<Arc<Session>> {
        self.sessions
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(id)
            .cloned()
    }

    fn historical_summary(&self, id: &str) -> Result<SessionSummary> {
        self.historical
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(id)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("unknown session: {id}"))
    }

    fn historical_history(&self, id: &str) -> Result<HistorySnapshot> {
        let summary = self.historical_summary(id)?;
        let path = self
            .runtime_root
            .join("sessions")
            .join(id)
            .join("runs")
            .join(format!("{}-history.json", summary.run_id));
        let bytes = fs::read(&path).with_context(|| format!("read history {}", path.display()))?;
        Ok(serde_json::from_slice(&bytes)?)
    }

    fn wait_for(
        &self,
        session_id: &str,
        predicate: WaitPredicate,
        timeout_ms: u64,
    ) -> Result<WaitResult> {
        if timeout_ms == 0 || timeout_ms > 60_000 {
            bail!("wait timeout must be between 1 and 60000 milliseconds")
        }
        let session = self.active_session(session_id)?;
        let start = Instant::now();
        let deadline = start + Duration::from_millis(timeout_ms);
        let regex = match &predicate {
            WaitPredicate::Regex { pattern } => {
                Some(Regex::new(pattern).context("invalid wait regex")?)
            }
            _ => None,
        };
        let mut last_text = String::new();
        let mut stable_since = Instant::now();
        let (mut result, stop) = execute_session_wait(
            &session,
            deadline,
            None,
            || false,
            |snapshot, _, _, now| {
                let matched = match &predicate {
                    WaitPredicate::TextContains { text } => snapshot.text.contains(text),
                    WaitPredicate::TextAbsent { text } => !snapshot.text.contains(text),
                    WaitPredicate::Regex { .. } => regex
                        .as_ref()
                        .is_some_and(|pattern| pattern.is_match(&snapshot.text)),
                    WaitPredicate::Stable { duration_ms } => {
                        if snapshot.text != last_text {
                            last_text.clone_from(&snapshot.text);
                            stable_since = now;
                        }
                        now.saturating_duration_since(stable_since)
                            >= Duration::from_millis(*duration_ms)
                    }
                    WaitPredicate::ProcessExited => snapshot.process_state != "running",
                };
                Ok(WaitStep {
                    value: WaitResult {
                        matched,
                        timed_out: false,
                        elapsed_ms: now.saturating_duration_since(start).as_millis() as u64,
                        snapshot: Some(snapshot.clone()),
                    },
                    terminal: matched,
                    next_wakeup: match predicate {
                        WaitPredicate::Stable { duration_ms } => {
                            Some(stable_since + Duration::from_millis(duration_ms))
                        }
                        _ => None,
                    },
                })
            },
        )?;
        result.timed_out = matches!(stop, WaitStop::Deadline);
        Ok(result)
    }
}

fn load_historical_sessions(runtime_root: &Path) -> Result<HashMap<String, SessionSummary>> {
    let mut output = HashMap::new();
    let sessions_dir = runtime_root.join("sessions");
    if !sessions_dir.exists() {
        return Ok(output);
    }
    for entry in fs::read_dir(&sessions_dir)? {
        let entry = entry?;
        let path = entry.path().join("session.json");
        if !path.is_file() {
            continue;
        }
        let bytes = match fs::read(&path) {
            Ok(bytes) => bytes,
            Err(_) => continue,
        };
        let Ok(mut summary) = serde_json::from_slice::<SessionSummary>(&bytes) else {
            continue;
        };
        if summary.state == "running" {
            summary.state = "interrupted".into();
            atomic_json_write(&path, &summary)?;
        }
        output.insert(summary.session_id.clone(), summary);
    }
    Ok(output)
}
