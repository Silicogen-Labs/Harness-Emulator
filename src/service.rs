use crate::control::{BrowserAuthority, BrowserLease, ControlStatus};
use crate::protocol::{HistorySnapshot, RpcRequest, SessionSummary, WaitPredicate, WaitResult};
use crate::session::{
    BrowserActionContext, BrowserInput, BrowserInputResult, Session, atomic_json_write,
    create_private_dir,
};
use anyhow::{Context, Result, bail};
use regex::Regex;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone)]
pub struct SessionService {
    runtime_root: PathBuf,
    sessions: Arc<Mutex<HashMap<String, Arc<Session>>>>,
    historical: Arc<Mutex<HashMap<String, SessionSummary>>>,
    pub(crate) agent: Arc<crate::automation::AutomationService>,
    lease_sessions: Arc<Mutex<HashMap<String, std::sync::Weak<Session>>>>,
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
        spec: crate::protocol::LaunchSpec,
        options: crate::config::LaunchOptions,
    ) -> Result<Arc<Session>> {
        const LAUNCH_METADATA_HEADROOM: u64 = 256 * 1024;

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
        match request {
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
            RpcRequest::Ping => Ok((json!({"status":"ok","protocol_version":1}), false)),
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
                Ok((serde_json::to_value(summary)?, false))
            }
            RpcRequest::Observe { session_id } => {
                let frame = self.active_session(&session_id)?.observe();
                Ok((serde_json::to_value(frame)?, false))
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
                let session = self.active_session(&session_id)?;
                let summary = session.restart()?;
                Ok((serde_json::to_value(summary)?, false))
            }
            RpcRequest::Close { session_id } => {
                let session = self.active_session(&session_id)?;
                let summary = session.close()?;
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
            self.historical
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .insert(summary.session_id.clone(), summary);
        }
        Ok(())
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
        loop {
            let snapshot = session.observe();
            let matched = match &predicate {
                WaitPredicate::TextContains { text } => snapshot.text.contains(text),
                WaitPredicate::TextAbsent { text } => !snapshot.text.contains(text),
                WaitPredicate::Regex { .. } => regex
                    .as_ref()
                    .is_some_and(|pattern| pattern.is_match(&snapshot.text)),
                WaitPredicate::Stable { duration_ms } => {
                    if snapshot.text != last_text {
                        last_text.clone_from(&snapshot.text);
                        stable_since = Instant::now();
                    }
                    stable_since.elapsed() >= Duration::from_millis(*duration_ms)
                }
                WaitPredicate::ProcessExited => snapshot.process_state != "running",
            };
            if matched {
                return Ok(WaitResult {
                    matched: true,
                    timed_out: false,
                    elapsed_ms: start.elapsed().as_millis() as u64,
                    snapshot: Some(snapshot),
                });
            }
            if Instant::now() >= deadline {
                return Ok(WaitResult {
                    matched: false,
                    timed_out: true,
                    elapsed_ms: start.elapsed().as_millis() as u64,
                    snapshot: Some(snapshot),
                });
            }
            thread::sleep(Duration::from_millis(25));
        }
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
