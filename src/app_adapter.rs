//! Read-only same-instance Silicogen adapter. No caller-supplied URL or database.
use crate::locators::ObservationIdentity;
use crate::sequences::SequenceCancellation;
use anyhow::{Context, Result, bail, ensure};
use futures_util::StreamExt;
use reqwest::{Client, Response, Url};
use serde::de::{DeserializeSeed, IgnoredAny, MapAccess, Visitor};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::io::{Cursor, Read};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

const UI_ROUTE: &str = "/__emulation/ui-state";
const MAX_MESSAGE_BASELINES: usize = 32;
const MAX_MESSAGE_BASELINE_BYTES: usize = 1024 * 1024;
const MESSAGE_BASELINE_TTL: Duration = Duration::from_secs(300);

/// Constructed by the session integration from an actually owned backend.
/// These fields are deliberately not deserializable from a tool request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OwnedBackendProof {
    pub owner_session_id: String,
    pub owner_run_id: String,
    pub pid: u32,
    pub process_start_identity: u64,
    pub loopback_port: u16,
    pub selected_directory: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TuiAttachProof {
    pub terminal_session_id: String,
    pub terminal_run_id: String,
    pub attached_backend: OwnedBackendProof,
}

pub struct BackendBinding {
    owned: OwnedBackendProof,
    tui: TuiAttachProof,
    allowed_sessions: BTreeSet<String>,
    generation: String,
}

impl BackendBinding {
    pub(crate) fn from_owned(
        owned: OwnedBackendProof,
        tui: TuiAttachProof,
        allowed_sessions: BTreeSet<String>,
    ) -> Result<Self> {
        ensure!(
            owned == tui.attached_backend,
            "TUI attachment does not name this owned backend process/run/port/directory"
        );
        ensure!(
            owned.pid > 0 && owned.process_start_identity > 0 && owned.loopback_port > 0,
            "missing owned backend process identity"
        );
        for id in [
            &owned.owner_session_id,
            &owned.owner_run_id,
            &tui.terminal_session_id,
            &tui.terminal_run_id,
        ] {
            validate_id(id)?;
        }
        ensure!(
            owned.selected_directory.is_absolute() && owned.selected_directory.is_dir(),
            "selected backend directory must be an existing absolute directory"
        );
        ensure!(
            std::fs::canonicalize(&owned.selected_directory)? == owned.selected_directory,
            "backend directory must already be canonicalized by session policy"
        );
        ensure!(
            !allowed_sessions.is_empty() && allowed_sessions.len() <= 64,
            "application session allowlist must contain 1..64 IDs"
        );
        for id in &allowed_sessions {
            validate_id(id)?;
        }
        let generation = format!(
            "{:x}",
            Sha256::digest(format!("{owned:?}:{tui:?}").as_bytes())
        );
        Ok(Self {
            owned,
            tui,
            allowed_sessions,
            generation,
        })
    }

    pub(crate) fn owned_backend(&self) -> &OwnedBackendProof {
        &self.owned
    }
    pub(crate) fn tui_attachment(&self) -> &TuiAttachProof {
        &self.tui
    }

    pub(crate) fn references_terminal_session(&self, session_id: &str) -> bool {
        self.owned.owner_session_id == session_id || self.tui.terminal_session_id == session_id
    }

    fn authorize_session(&self, session: &str) -> Result<()> {
        validate_id(session)?;
        ensure!(
            self.allowed_sessions.contains(session),
            "application session is outside the attached TUI allowlist"
        );
        Ok(())
    }

    pub(crate) fn validate_destination(&self, url: &Url, allow_ui: bool) -> Result<()> {
        ensure!(
            url.scheme() == "http"
                && url.host_str() == Some("127.0.0.1")
                && url.port_or_known_default() == Some(self.owned.loopback_port),
            "adapter destination must be the exact owned IPv4 loopback socket"
        );
        ensure!(
            url.username().is_empty() && url.password().is_none() && url.fragment().is_none(),
            "adapter URL may not contain credentials or fragments"
        );
        let mut directory_seen = false;
        let mut limit_seen = false;
        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "directory" => {
                    ensure!(
                        !directory_seen
                            && std::path::Path::new(value.as_ref())
                                == self.owned.selected_directory,
                        "adapter directory query differs from selected workspace"
                    );
                    directory_seen = true;
                }
                "limit" => {
                    ensure!(
                        !limit_seen
                            && url.path().ends_with("/message")
                            && value
                                .parse::<usize>()
                                .is_ok_and(|limit| (1..=1025).contains(&limit)),
                        "invalid fixed message limit query"
                    );
                    limit_seen = true;
                }
                _ => bail!("query parameter outside the adapter allowlist"),
            }
        }
        ensure!(
            directory_seen,
            "adapter requires the owned backend directory context"
        );
        let path = url.path();
        if path == "/event" || path == "/session/status" || (allow_ui && path == UI_ROUTE) {
            return Ok(());
        }
        for id in &self.allowed_sessions {
            if path == format!("/session/{id}") || path == format!("/session/{id}/message") {
                return Ok(());
            }
        }
        bail!("adapter route is outside its fixed read-only allowlist")
    }
}

/// The integration must check its current ownership registry, run, live PID
/// start identity, bound port and TUI attachment on every call. Health/version
/// and shared storage are never accepted as process or attachment proof.
pub trait BindingVerifier: Send + Sync {
    fn verify_current(&self, binding: &BackendBinding) -> Result<()>;
}

#[derive(Debug, Clone)]
pub struct AdapterLimits {
    pub max_response_bytes: usize,
    pub max_messages: usize,
    pub max_event_bytes: usize,
    pub max_stream_bytes: usize,
    pub max_events: usize,
    pub request_timeout: Duration,
}

impl Default for AdapterLimits {
    fn default() -> Self {
        Self {
            max_response_bytes: 1024 * 1024,
            max_messages: 256,
            max_event_bytes: 64 * 1024,
            max_stream_bytes: 1024 * 1024,
            max_events: 128,
            request_timeout: Duration::from_secs(5),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct DataProvenance {
    pub assertion_plane: &'static str,
    pub binding_generation: String,
    pub backend_session_id: String,
    pub backend_run_id: String,
    pub backend_pid: u32,
    pub process_start_identity: u64,
    pub loopback_port: u16,
    pub terminal_session_id: String,
    pub terminal_run_id: String,
    pub application_session_id: String,
    pub evidence_sequence: u64,
}

#[derive(Debug, Clone, Serialize)]
pub struct ToolResult {
    pub call_id: String,
    pub tool: String,
    pub status: String,
    pub output: Option<String>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RevertState {
    #[serde(rename = "messageID")]
    pub message_id: String,
    #[serde(rename = "partID")]
    pub part_id: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RecapState {
    pub status: Option<String>,
    pub summary: Option<String>,
    pub next_action: Option<String>,
    #[serde(rename = "throughMessageID")]
    pub through_message_id: Option<String>,
    #[serde(rename = "sourceAssistantID")]
    pub source_assistant_id: Option<String>,
    pub revision: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct LogicalMessage {
    pub id: String,
    pub role: String,
    pub parent_id: Option<String>,
    pub completed: bool,
    pub finish: Option<String>,
    pub text: Vec<String>,
    pub tools: Vec<ToolResult>,
    pub recap: Option<RecapState>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MessagesEvidence {
    pub provenance: DataProvenance,
    pub messages: Vec<LogicalMessage>,
    pub truncated: bool,
    pub exact_visual_order_or_position: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionEvidence {
    pub provenance: DataProvenance,
    pub id: String,
    pub title: Option<String>,
    pub revert: Option<RevertState>,
    pub recap: Option<RecapState>,
}

#[derive(Debug, Clone, Serialize)]
pub struct StatusEvidence {
    pub provenance: DataProvenance,
    pub status: Option<LiveStatus>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LiveStatus {
    Idle,
    Busy,
    Retry {
        attempt: u64,
        message: String,
        next: u64,
    },
    Compacting {
        phase: String,
        attempt: u64,
        #[serde(rename = "maxAttempts")]
        max_attempts: u64,
        #[serde(rename = "startedAt")]
        started_at: u64,
        #[serde(rename = "nextRetryAt")]
        next_retry_at: Option<u64>,
    },
    CompactionFailed {
        message: String,
        attempt: u64,
        #[serde(rename = "maxAttempts")]
        max_attempts: u64,
        retryable: bool,
    },
    RetryFailed {
        message: String,
        attempt: u64,
        #[serde(rename = "maxAttempts")]
        max_attempts: u64,
        retryable: bool,
    },
}

#[derive(Debug, Clone)]
pub struct MessageBaseline {
    generation: String,
    app_session: String,
    sequence: u64,
    terminal_states: BTreeSet<String>,
}

struct RetainedMessageBaseline {
    baseline: MessageBaseline,
    expires_at: Instant,
    retained_bytes: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct SemanticWaitResult {
    pub outcome: &'static str,
    pub matched: bool,
    pub baseline_handle: String,
    pub evidence_sequence: u64,
    pub elapsed_ms: u64,
}

impl MessagesEvidence {
    pub fn baseline(&self) -> Result<MessageBaseline> {
        ensure!(
            !self.truncated,
            "cannot establish a complete message baseline from a bounded/truncated message response"
        );
        Ok(MessageBaseline {
            generation: self.provenance.binding_generation.clone(),
            app_session: self.provenance.application_session_id.clone(),
            sequence: self.provenance.evidence_sequence,
            terminal_states: semantic_states(&self.messages),
        })
    }
}

#[derive(Debug, Clone)]
pub enum SemanticPredicate {
    TurnCompleted {
        user_message_id: String,
    },
    ToolCompleted {
        user_message_id: String,
        call_id: String,
        tool: String,
    },
}

pub fn evaluate_semantic(
    baseline: &MessageBaseline,
    evidence: &MessagesEvidence,
    predicate: &SemanticPredicate,
) -> Result<bool> {
    ensure!(
        baseline.generation == evidence.provenance.binding_generation
            && baseline.app_session == evidence.provenance.application_session_id,
        "message baseline belongs to another attached backend/run/session"
    );
    ensure!(
        evidence.provenance.evidence_sequence > baseline.sequence,
        "semantic predicates require evidence after the message baseline"
    );
    ensure!(
        !evidence.truncated,
        "semantic result is unavailable for a truncated logical message window"
    );
    Ok(evidence
        .messages
        .iter()
        .filter(|message| message.role == "assistant")
        .any(|message| match predicate {
            SemanticPredicate::TurnCompleted { user_message_id } => {
                message.parent_id.as_ref() == Some(user_message_id)
                    && message.completed
                    && message.finish.as_deref() == Some("stop")
                    && !baseline.terminal_states.contains(&turn_key(message))
            }
            SemanticPredicate::ToolCompleted {
                user_message_id,
                call_id,
                tool,
            } => {
                message.parent_id.as_ref() == Some(user_message_id)
                    && message.tools.iter().any(|result| {
                        result.call_id == *call_id
                            && result.tool == *tool
                            && result.status == "completed"
                            && !baseline
                                .terminal_states
                                .contains(&tool_key(message, result))
                    })
            }
        }))
}

fn turn_key(message: &LogicalMessage) -> String {
    format!(
        "turn:{}:{}",
        message.parent_id.as_deref().unwrap_or(""),
        message.id
    )
}
fn tool_key(message: &LogicalMessage, tool: &ToolResult) -> String {
    format!(
        "tool:{}:{}:{}:{}",
        message.parent_id.as_deref().unwrap_or(""),
        message.id,
        tool.call_id,
        tool.tool
    )
}
fn semantic_states(messages: &[LogicalMessage]) -> BTreeSet<String> {
    let mut result = BTreeSet::new();
    for message in messages
        .iter()
        .filter(|message| message.role == "assistant")
    {
        if message.completed && message.finish.as_deref() == Some("stop") {
            result.insert(turn_key(message));
        }
        for tool in &message.tools {
            if tool.status == "completed" {
                result.insert(tool_key(message, tool));
            }
        }
    }
    result
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PresenceDiagnosis {
    DataPresentAndRendered,
    DataPresentViewMissing,
    DataAbsent,
    RenderedWithoutData,
}

pub fn diagnose_render_presence(
    evidence: &MessagesEvidence,
    message_id: &str,
    observation: &ObservationIdentity,
    rendered: bool,
) -> Result<PresenceDiagnosis> {
    ensure!(
        evidence.provenance.terminal_session_id == observation.session_id
            && evidence.provenance.terminal_run_id == observation.run_id,
        "render evidence belongs to another terminal run"
    );
    let present = evidence
        .messages
        .iter()
        .any(|message| message.id == message_id);
    ensure!(
        present || !evidence.truncated,
        "data absence is unknown because the logical message response was truncated"
    );
    Ok(match (present, rendered) {
        (true, true) => PresenceDiagnosis::DataPresentAndRendered,
        (true, false) => PresenceDiagnosis::DataPresentViewMissing,
        (false, false) => PresenceDiagnosis::DataAbsent,
        (false, true) => PresenceDiagnosis::RenderedWithoutData,
    })
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct UiProducerState {
    pub version: u32,
    pub producer_instance: String,
    pub backend_session_id: String,
    pub backend_run_id: String,
    pub observation: ObservationIdentity,
    pub sequence: u64,
    pub active_application_session_id: String,
    pub route: String,
    pub dialog: Option<String>,
    pub filter: Option<String>,
    pub focus: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub enum UiAvailability {
    Unavailable { reason: &'static str },
    Available(Box<UiProducerState>),
}

#[derive(Debug, Clone, Serialize)]
pub struct FilteredEvent {
    pub application_session_id: String,
    pub event_type: String,
    pub payload: EventPayload,
    pub provenance: DataProvenance,
}

#[derive(Debug, Clone, Serialize)]
pub enum EventPayload {
    Status(LiveStatus),
    Idle,
    MessageUpdated {
        message_id: String,
        role: String,
        parent_id: Option<String>,
        completed: bool,
        finish: Option<String>,
    },
    MessageRemoved {
        message_id: String,
    },
    PartUpdated {
        message_id: String,
        part_id: String,
        part_type: String,
        tool: Option<ToolResult>,
    },
}

#[derive(Debug, Clone, Serialize)]
pub struct EventBatch {
    pub events: Vec<FilteredEvent>,
    pub filtered_events: usize,
    pub wire_bytes: usize,
    pub stop_reason: &'static str,
    pub complete_stream: bool,
}

pub struct SilicogenAdapter<V: BindingVerifier> {
    binding: BackendBinding,
    verifier: Arc<V>,
    client: Client,
    limits: AdapterLimits,
    sequence: u64,
    producer: Option<String>,
    last_ui_sequence: Option<u64>,
    last_ui_observation: Option<ObservationIdentity>,
    last_ui_digest: Option<[u8; 32]>,
    message_baselines: BTreeMap<String, RetainedMessageBaseline>,
    message_baseline_bytes: usize,
    statuses: BTreeMap<String, StatusEvidence>,
    #[cfg(test)]
    status_decoders: Arc<std::sync::atomic::AtomicUsize>,
}

impl<V: BindingVerifier> SilicogenAdapter<V> {
    pub fn new(binding: BackendBinding, verifier: Arc<V>, limits: AdapterLimits) -> Result<Self> {
        ensure!(
            (1..=4 * 1024 * 1024).contains(&limits.max_response_bytes)
                && (1..=1024).contains(&limits.max_messages),
            "adapter JSON bounds invalid"
        );
        ensure!(
            (1..=256 * 1024).contains(&limits.max_event_bytes)
                && limits.max_stream_bytes <= 4 * 1024 * 1024
                && limits.max_stream_bytes >= limits.max_event_bytes
                && (1..=256).contains(&limits.max_events),
            "adapter SSE bounds invalid"
        );
        ensure!(
            !limits.request_timeout.is_zero() && limits.request_timeout <= Duration::from_secs(60),
            "adapter timeout must be positive and at most 60 seconds"
        );
        verifier.verify_current(&binding)?;
        let client = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .connect_timeout(limits.request_timeout)
            .read_timeout(limits.request_timeout)
            .timeout(limits.request_timeout)
            .build()?;
        Ok(Self {
            binding,
            verifier,
            client,
            limits,
            sequence: 0,
            producer: None,
            last_ui_sequence: None,
            last_ui_observation: None,
            last_ui_digest: None,
            message_baselines: BTreeMap::new(),
            message_baseline_bytes: 0,
            statuses: BTreeMap::new(),
            #[cfg(test)]
            status_decoders: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        })
    }

    pub(crate) fn references_terminal_session(&self, session_id: &str) -> bool {
        self.binding.references_terminal_session(session_id)
    }

    pub(crate) fn binding_is_current(&self) -> bool {
        self.verifier.verify_current(&self.binding).is_ok()
    }

    pub(crate) fn binding_generation(&self) -> &str {
        &self.binding.generation
    }

    pub async fn create_message_baseline(
        &mut self,
        app_session: &str,
        deadline: Instant,
        cancellation: &SequenceCancellation,
    ) -> Result<(String, u64)> {
        self.prune_message_baselines();
        let evidence = self.messages(app_session, deadline, cancellation).await?;
        let baseline = evidence.baseline()?;
        let retained_bytes = baseline
            .terminal_states
            .iter()
            .map(String::len)
            .sum::<usize>()
            .saturating_add(baseline.generation.len())
            .saturating_add(baseline.app_session.len())
            .saturating_add(64);
        ensure!(
            self.message_baselines.len() < MAX_MESSAGE_BASELINES,
            "message_baseline_capacity: at most 32 live baselines per adapter"
        );
        ensure!(
            self.message_baseline_bytes.saturating_add(retained_bytes)
                <= MAX_MESSAGE_BASELINE_BYTES,
            "message_baseline_capacity: retained baseline evidence exceeds 1 MiB"
        );
        let handle = format!("mb-{}", ulid::Ulid::generate());
        let sequence = baseline.sequence;
        self.message_baseline_bytes += retained_bytes;
        self.message_baselines.insert(
            handle.clone(),
            RetainedMessageBaseline {
                baseline,
                expires_at: Instant::now() + MESSAGE_BASELINE_TTL,
                retained_bytes,
            },
        );
        Ok((handle, sequence))
    }

    pub async fn wait_semantic(
        &mut self,
        baseline_handle: &str,
        predicate: &SemanticPredicate,
        deadline: Instant,
        cancellation: &SequenceCancellation,
    ) -> Result<SemanticWaitResult> {
        self.prune_message_baselines();
        let (baseline, app_session) = self
            .message_baselines
            .get(baseline_handle)
            .filter(|record| record.expires_at > Instant::now())
            .map(|record| (record.baseline.clone(), record.baseline.app_session.clone()))
            .context("message_baseline_expired_or_unknown: create a fresh baseline")?;
        self.binding.authorize_session(&app_session)?;
        ensure!(
            baseline.generation == self.binding.generation && baseline.app_session == app_session,
            "message_baseline_scope_mismatch: handle belongs to another adapter generation or application session"
        );
        let started = Instant::now();
        loop {
            if cancellation.is_cancelled() {
                bail!("semantic_wait_cancelled")
            }
            if Instant::now() >= deadline {
                return Ok(SemanticWaitResult {
                    outcome: "timed_out",
                    matched: false,
                    baseline_handle: baseline_handle.to_owned(),
                    evidence_sequence: self.sequence,
                    elapsed_ms: started.elapsed().as_millis().min(u64::MAX as u128) as u64,
                });
            }
            match self
                .tool_completion(&app_session, &baseline, predicate, deadline, cancellation)
                .await
            {
                Ok(true) => {
                    return Ok(SemanticWaitResult {
                        outcome: "matched",
                        matched: true,
                        baseline_handle: baseline_handle.to_owned(),
                        evidence_sequence: self.sequence,
                        elapsed_ms: started.elapsed().as_millis().min(u64::MAX as u128) as u64,
                    });
                }
                Ok(false) if Instant::now() >= deadline => {
                    return Ok(SemanticWaitResult {
                        outcome: "timed_out",
                        matched: false,
                        baseline_handle: baseline_handle.to_owned(),
                        evidence_sequence: self.sequence,
                        elapsed_ms: started.elapsed().as_millis().min(u64::MAX as u128) as u64,
                    });
                }
                Ok(false) => {}
                Err(_error) if Instant::now() >= deadline => {
                    return Ok(SemanticWaitResult {
                        outcome: "timed_out",
                        matched: false,
                        baseline_handle: baseline_handle.to_owned(),
                        evidence_sequence: self.sequence,
                        elapsed_ms: started.elapsed().as_millis().min(u64::MAX as u128) as u64,
                    });
                }
                Err(error) => return Err(error),
            }
            let sleep_for =
                Duration::from_millis(100).min(deadline.saturating_duration_since(Instant::now()));
            tokio::time::sleep(sleep_for).await;
        }
    }

    fn prune_message_baselines(&mut self) {
        let now = Instant::now();
        let expired = self
            .message_baselines
            .iter()
            .filter(|(_, baseline)| baseline.expires_at <= now)
            .map(|(handle, baseline)| (handle.clone(), baseline.retained_bytes))
            .collect::<Vec<_>>();
        for (handle, retained_bytes) in expired {
            self.message_baselines.remove(&handle);
            self.message_baseline_bytes =
                self.message_baseline_bytes.saturating_sub(retained_bytes);
        }
    }

    #[cfg(test)]
    #[allow(dead_code)] // Used by integration tests that compile this module as a fixture.
    pub(crate) fn active_status_decoders(&self) -> usize {
        self.status_decoders
            .load(std::sync::atomic::Ordering::Acquire)
    }

    fn provenance(&mut self, app_session: &str) -> DataProvenance {
        self.sequence += 1;
        DataProvenance {
            assertion_plane: "application_data",
            binding_generation: self.binding.generation.clone(),
            backend_session_id: self.binding.owned.owner_session_id.clone(),
            backend_run_id: self.binding.owned.owner_run_id.clone(),
            backend_pid: self.binding.owned.pid,
            process_start_identity: self.binding.owned.process_start_identity,
            loopback_port: self.binding.owned.loopback_port,
            terminal_session_id: self.binding.tui.terminal_session_id.clone(),
            terminal_run_id: self.binding.tui.terminal_run_id.clone(),
            application_session_id: app_session.into(),
            evidence_sequence: self.sequence,
        }
    }

    fn url(&self, path: &str, limit: Option<usize>, allow_ui_state: bool) -> Result<Url> {
        let mut url = Url::parse(&format!(
            "http://127.0.0.1:{}{path}",
            self.binding.owned.loopback_port
        ))?;
        url.query_pairs_mut().append_pair(
            "directory",
            self.binding
                .owned
                .selected_directory
                .to_str()
                .context("selected directory is not UTF-8")?,
        );
        if let Some(limit) = limit {
            url.query_pairs_mut()
                .append_pair("limit", &limit.to_string());
        }
        self.binding.validate_destination(&url, allow_ui_state)?;
        Ok(url)
    }

    async fn get(
        &self,
        url: Url,
        allow_ui_state: bool,
        deadline: Instant,
        cancellation: &SequenceCancellation,
    ) -> Result<Response> {
        self.verifier.verify_current(&self.binding)?;
        self.binding
            .validate_destination(&url, allow_ui_state || self.producer.is_some())?;
        let response =
            await_io(self.client.get(url.clone()).send(), deadline, cancellation).await??;
        ensure!(
            response.url() == &url,
            "adapter response changed destination"
        );
        ensure!(
            response.remote_addr()
                == Some(SocketAddr::new(
                    IpAddr::V4(Ipv4Addr::LOCALHOST),
                    self.binding.owned.loopback_port
                )),
            "adapter response peer differs from the exact owned loopback socket"
        );
        ensure!(
            !response.status().is_redirection(),
            "adapter redirects are forbidden"
        );
        self.verifier.verify_current(&self.binding)?;
        Ok(response)
    }

    async fn bounded_json_bytes(
        &self,
        url: Url,
        deadline: Instant,
        cancellation: &SequenceCancellation,
        allow_missing: bool,
        allow_ui_state: bool,
    ) -> Result<Option<Vec<u8>>> {
        let response = self
            .get(url, allow_ui_state, deadline, cancellation)
            .await?;
        if allow_missing && response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        ensure!(
            response.status().is_success(),
            "application read failed with HTTP {}",
            response.status().as_u16()
        );
        ensure!(
            response
                .content_length()
                .is_none_or(|length| length <= self.limits.max_response_bytes as u64),
            "application JSON response exceeds byte limit"
        );
        let mut stream = response.bytes_stream();
        let mut bytes = Vec::new();
        while let Some(chunk) = await_io(stream.next(), deadline, cancellation).await? {
            let chunk = chunk?;
            ensure!(
                bytes.len().saturating_add(chunk.len()) <= self.limits.max_response_bytes,
                "application JSON response exceeds byte limit"
            );
            bytes.extend_from_slice(&chunk);
        }
        self.verifier.verify_current(&self.binding)?;
        Ok(Some(bytes))
    }

    pub async fn messages(
        &mut self,
        app_session: &str,
        deadline: Instant,
        cancellation: &SequenceCancellation,
    ) -> Result<MessagesEvidence> {
        self.binding.authorize_session(app_session)?;
        let bytes = self
            .bounded_json_bytes(
                self.url(
                    &format!("/session/{app_session}/message"),
                    Some(self.limits.max_messages + 1),
                    false,
                )?,
                deadline,
                cancellation,
                false,
                false,
            )
            .await?
            .unwrap();
        let values: Vec<Value> = serde_json::from_slice(&bytes)?;
        ensure!(
            values.len() <= self.limits.max_messages + 1,
            "application ignored logical message count limit"
        );
        let truncated = values.len() > self.limits.max_messages;
        let mut messages = Vec::new();
        let mut ids = BTreeSet::new();
        for value in values {
            let info = &value["info"];
            ensure!(
                info["sessionID"].as_str() == Some(app_session),
                "application returned a message from an unrelated session"
            );
            let id = info["id"]
                .as_str()
                .context("message ID missing")?
                .to_owned();
            validate_id(&id)?;
            ensure!(ids.insert(id.clone()), "duplicate logical message ID");
            let role = info["role"].as_str().context("message role missing")?;
            ensure!(
                matches!(role, "assistant" | "user"),
                "unsupported logical message role"
            );
            if role == "assistant" {
                validate_id(
                    info["parentID"]
                        .as_str()
                        .context("assistant parent message ID missing")?,
                )?;
            }
            let mut text = Vec::new();
            let mut tools = Vec::new();
            for part in value["parts"].as_array().context("message parts missing")? {
                ensure!(
                    part["sessionID"].as_str() == Some(app_session)
                        && part["messageID"].as_str() == Some(id.as_str()),
                    "application returned a part from an unrelated session or message"
                );
                match part["type"].as_str() {
                    Some("text") => {
                        if let Some(value) = part["text"].as_str() {
                            text.push(value.to_owned());
                        }
                    }
                    Some("tool") => {
                        ensure!(
                            part["state"]["status"]
                                .as_str()
                                .is_some_and(|status| matches!(
                                    status,
                                    "pending" | "running" | "completed" | "error"
                                )),
                            "invalid logical tool state"
                        );
                        tools.push(ToolResult {
                            call_id: part["callID"]
                                .as_str()
                                .context("tool call ID missing")?
                                .into(),
                            tool: part["tool"].as_str().context("tool name missing")?.into(),
                            status: part["state"]["status"]
                                .as_str()
                                .context("tool state missing")?
                                .into(),
                            output: part["state"]["output"].as_str().map(str::to_owned),
                            error: part["state"]["error"].as_str().map(str::to_owned),
                        });
                    }
                    _ => {}
                }
            }
            if messages.len() < self.limits.max_messages {
                messages.push(LogicalMessage {
                    id,
                    role: role.into(),
                    parent_id: info["parentID"].as_str().map(str::to_owned),
                    completed: info["time"]["completed"].is_number(),
                    finish: info["finish"].as_str().map(str::to_owned),
                    text,
                    tools,
                    recap: decode_optional(info["summary"].get("recap"))?,
                });
            }
        }
        Ok(MessagesEvidence {
            provenance: self.provenance(app_session),
            messages,
            truncated,
            exact_visual_order_or_position: false,
        })
    }

    pub async fn transcript(
        &mut self,
        app_session: &str,
        deadline: Instant,
        cancellation: &SequenceCancellation,
    ) -> Result<MessagesEvidence> {
        self.messages(app_session, deadline, cancellation).await
    }

    pub async fn session(
        &mut self,
        app_session: &str,
        deadline: Instant,
        cancellation: &SequenceCancellation,
    ) -> Result<SessionEvidence> {
        self.binding.authorize_session(app_session)?;
        let bytes = self
            .bounded_json_bytes(
                self.url(&format!("/session/{app_session}"), None, false)?,
                deadline,
                cancellation,
                false,
                false,
            )
            .await?
            .unwrap();
        let value: Value = serde_json::from_slice(&bytes)?;
        ensure!(
            value["id"].as_str() == Some(app_session),
            "application returned an unrelated session"
        );
        if let Some(directory) = value["directory"].as_str() {
            ensure!(
                std::path::Path::new(directory) == self.binding.owned.selected_directory,
                "application session directory differs from owned workspace"
            );
        }
        Ok(SessionEvidence {
            provenance: self.provenance(app_session),
            id: app_session.into(),
            title: value["title"].as_str().map(str::to_owned),
            revert: decode_optional(value.get("revert"))?,
            recap: decode_optional(value.get("recap"))?,
        })
    }

    pub async fn status(
        &mut self,
        app_session: &str,
        deadline: Instant,
        cancellation: &SequenceCancellation,
    ) -> Result<StatusEvidence> {
        self.binding.authorize_session(app_session)?;
        let response = self
            .get(
                self.url("/session/status", None, false)?,
                false,
                deadline,
                cancellation,
            )
            .await?;
        ensure!(
            response.status().is_success(),
            "application status read failed"
        );
        ensure!(
            response
                .content_length()
                .is_none_or(|length| length <= self.limits.max_response_bytes as u64),
            "application status response exceeds byte limit"
        );
        // Decode a bounded stream with one bounded transport chunk in flight.
        // The serde visitor never builds or retains the all-session status map.
        let (sender, receiver) = tokio::sync::mpsc::channel(1);
        let target = app_session.to_owned();
        #[cfg(test)]
        let decoder_guard = DecoderGuard::new(Arc::clone(&self.status_decoders));
        let task = tokio::task::spawn_blocking(move || -> Result<Option<LiveStatus>> {
            #[cfg(test)]
            let _guard = decoder_guard;
            let reader = StatusReader {
                receiver,
                chunk: Cursor::new(Vec::new()),
            };
            let mut decoder = serde_json::Deserializer::from_reader(reader);
            let status = TargetStatus(&target).deserialize(&mut decoder)?;
            decoder.end()?;
            Ok(status)
        });
        let mut stream = response.bytes_stream();
        let mut total = 0usize;
        while let Some(chunk) = await_io(stream.next(), deadline, cancellation).await? {
            self.verifier.verify_current(&self.binding)?;
            let chunk = chunk?;
            total = total.saturating_add(chunk.len());
            ensure!(
                total <= self.limits.max_response_bytes,
                "application status response exceeds byte limit"
            );
            // Fixed 4 KiB pieces, never an aggregate all-session buffer.
            for piece in chunk.chunks(4096) {
                if await_io(sender.send(piece.to_vec()), deadline, cancellation)
                    .await?
                    .is_err()
                {
                    break;
                }
            }
        }
        drop(sender);
        let status = await_io(task, deadline, cancellation).await???;
        self.verifier.verify_current(&self.binding)?;
        let result = StatusEvidence {
            provenance: self.provenance(app_session),
            status,
        };
        self.statuses.insert(app_session.into(), result.clone());
        Ok(result)
    }

    pub async fn tool_completion(
        &mut self,
        app_session: &str,
        baseline: &MessageBaseline,
        predicate: &SemanticPredicate,
        deadline: Instant,
        cancellation: &SequenceCancellation,
    ) -> Result<bool> {
        let evidence = self.messages(app_session, deadline, cancellation).await?;
        evaluate_semantic(baseline, &evidence, predicate)
    }

    pub async fn ui_state(
        &mut self,
        expected_producer: Option<&str>,
        deadline: Instant,
        cancellation: &SequenceCancellation,
    ) -> Result<UiAvailability> {
        self.verifier.verify_current(&self.binding)?;
        if let Some(pinned) = self.producer.as_deref() {
            ensure!(
                expected_producer.is_none_or(|expected| expected == pinned),
                "UI-state producer differs from the producer pinned for this adapter generation"
            );
        }
        let Some(producer) = self
            .producer
            .as_deref()
            .or(expected_producer)
            .map(str::to_owned)
        else {
            return Ok(UiAvailability::Unavailable {
                reason: "no UI-state producer connected",
            });
        };
        validate_id(&producer)?;
        let Some(bytes) = self
            .bounded_json_bytes(
                self.url(UI_ROUTE, None, true)?,
                deadline,
                cancellation,
                true,
                true,
            )
            .await?
        else {
            return Ok(UiAvailability::Unavailable {
                reason: "opt-in UI-state producer route unavailable",
            });
        };
        let state: UiProducerState = serde_json::from_slice(&bytes)?;
        ensure!(
            state.version == 1
                && state.producer_instance == producer
                && state.backend_session_id == self.binding.owned.owner_session_id
                && state.backend_run_id == self.binding.owned.owner_run_id,
            "UI producer belongs to another backend generation"
        );
        ensure!(
            state.observation.session_id == self.binding.tui.terminal_session_id
                && state.observation.run_id == self.binding.tui.terminal_run_id
                && !state.observation.frame_id.is_empty(),
            "UI producer observation belongs to another attached terminal run"
        );
        ensure!(
            state.sequence > 0
                && (1..=300).contains(&state.observation.cols)
                && (1..=120).contains(&state.observation.rows),
            "UI producer observation geometry or sequence is invalid"
        );
        self.binding
            .authorize_session(&state.active_application_session_id)?;
        ensure!(
            self.last_ui_sequence
                .is_none_or(|sequence| state.sequence >= sequence),
            "UI producer sequence regressed"
        );
        for value in [
            Some(&state.route),
            state.dialog.as_ref(),
            state.filter.as_ref(),
            state.focus.as_ref(),
        ]
        .into_iter()
        .flatten()
        {
            ensure!(value.len() <= 512, "UI producer field exceeds bound");
        }
        let digest: [u8; 32] = Sha256::digest(serde_json::to_vec(&state)?).into();
        if self.last_ui_sequence == Some(state.sequence) {
            ensure!(
                self.last_ui_digest == Some(digest),
                "UI producer changed state without advancing its sequence"
            );
        }
        if let Some(previous) = &self.last_ui_observation {
            ensure!(
                state.observation.observation_revision >= previous.observation_revision
                    && state.observation.viewport_revision >= previous.viewport_revision,
                "UI producer observation regressed"
            );
            if state.observation.viewport_revision == previous.viewport_revision {
                ensure!(
                    state.observation.cols == previous.cols
                        && state.observation.rows == previous.rows,
                    "UI producer geometry changed without viewport revision"
                );
            }
        }
        self.last_ui_sequence = Some(state.sequence);
        self.last_ui_observation = Some(state.observation.clone());
        self.last_ui_digest = Some(digest);
        if self.producer.is_none() {
            self.producer = Some(producer);
        }
        Ok(UiAvailability::Available(Box::new(state)))
    }

    pub async fn read_events(
        &mut self,
        deadline: Instant,
        cancellation: &SequenceCancellation,
    ) -> Result<EventBatch> {
        let response = self
            .get(
                self.url("/event", None, false)?,
                false,
                deadline,
                cancellation,
            )
            .await?;
        ensure!(
            response.status().is_success(),
            "application SSE read failed"
        );
        ensure!(
            response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .is_some_and(|value| value.starts_with("text/event-stream")),
            "application event response is not SSE"
        );
        let mut stream = response.bytes_stream();
        let mut decoder = SseDecoder::new(self.limits.max_event_bytes);
        let mut batch = EventBatch {
            events: Vec::new(),
            filtered_events: 0,
            wire_bytes: 0,
            stop_reason: "end_of_stream",
            complete_stream: false,
        };
        loop {
            let next = match await_io(stream.next(), deadline, cancellation).await {
                Ok(next) => next,
                Err(_error) if !cancellation.is_cancelled() && Instant::now() >= deadline => {
                    batch.stop_reason = "deadline";
                    break;
                }
                Err(error) => return Err(error),
            };
            let Some(chunk) = next else {
                break;
            };
            let chunk = chunk?;
            ensure!(
                batch.wire_bytes.saturating_add(chunk.len()) <= self.limits.max_stream_bytes,
                "application SSE wire byte budget exceeded"
            );
            batch.wire_bytes += chunk.len();
            decoder.feed(&chunk, |data| {
                self.verifier.verify_current(&self.binding)?;
                let event: Value = serde_json::from_slice(data)?;
                let kind = event["type"].as_str().unwrap_or("");
                let session = match kind {
                    "session.status" | "session.idle" | "message.removed" => {
                        event["properties"]["sessionID"].as_str()
                    }
                    "message.updated" => event["properties"]["info"]["sessionID"].as_str(),
                    "message.part.updated" => event["properties"]["part"]["sessionID"].as_str(),
                    _ => None,
                };
                let Some(session) = session
                    .filter(|id| self.binding.allowed_sessions.contains(*id))
                    .map(str::to_owned)
                else {
                    batch.filtered_events += 1;
                    return Ok(());
                };
                if batch.events.len() >= self.limits.max_events {
                    batch.stop_reason = "event_budget";
                    return Ok(());
                }
                let payload = project_event(kind, &event["properties"])?;
                let provenance = self.provenance(&session);
                if let EventPayload::Status(status) = &payload {
                    self.statuses.insert(
                        session.clone(),
                        StatusEvidence {
                            provenance: provenance.clone(),
                            status: Some(status.clone()),
                        },
                    );
                }
                batch.events.push(FilteredEvent {
                    application_session_id: session,
                    event_type: kind.into(),
                    payload,
                    provenance,
                });
                Ok(())
            })?;
            if batch.events.len() >= self.limits.max_events {
                batch.stop_reason = "event_budget";
                break;
            }
        }
        if decoder.incomplete() && batch.stop_reason == "end_of_stream" {
            batch.stop_reason = "incomplete_event";
        }
        self.verifier.verify_current(&self.binding)?;
        Ok(batch)
    }
}

fn decode_optional<T: serde::de::DeserializeOwned>(value: Option<&Value>) -> Result<Option<T>> {
    value
        .filter(|value| !value.is_null())
        .map(|value| serde_json::from_value(value.clone()).map_err(anyhow::Error::from))
        .transpose()
}

fn project_event(kind: &str, properties: &Value) -> Result<EventPayload> {
    let required = |value: &Value, name: &str| -> Result<String> {
        let id = value
            .as_str()
            .with_context(|| format!("event {name} missing"))?;
        validate_id(id)?;
        Ok(id.into())
    };
    Ok(match kind {
        "session.status" => {
            EventPayload::Status(serde_json::from_value(properties["status"].clone())?)
        }
        "session.idle" => EventPayload::Idle,
        "message.removed" => EventPayload::MessageRemoved {
            message_id: required(&properties["messageID"], "message ID")?,
        },
        "message.updated" => {
            let info = &properties["info"];
            let role = info["role"]
                .as_str()
                .context("event message role missing")?;
            ensure!(
                matches!(role, "user" | "assistant"),
                "unsupported event message role"
            );
            EventPayload::MessageUpdated {
                message_id: required(&info["id"], "message ID")?,
                role: role.into(),
                parent_id: info["parentID"].as_str().map(str::to_owned),
                completed: info["time"]["completed"].is_number(),
                finish: info["finish"].as_str().map(str::to_owned),
            }
        }
        "message.part.updated" => {
            let part = &properties["part"];
            let part_type = part["type"].as_str().context("event part type missing")?;
            ensure!(part_type.len() <= 64, "event part type exceeds bound");
            let tool = if part_type == "tool" {
                let status = part["state"]["status"]
                    .as_str()
                    .context("event tool status missing")?;
                ensure!(
                    matches!(status, "pending" | "running" | "completed" | "error"),
                    "invalid event tool state"
                );
                Some(ToolResult {
                    call_id: required(&part["callID"], "tool call ID")?,
                    tool: required(&part["tool"], "tool name")?,
                    status: status.into(),
                    output: part["state"]["output"].as_str().map(str::to_owned),
                    error: part["state"]["error"].as_str().map(str::to_owned),
                })
            } else {
                None
            };
            EventPayload::PartUpdated {
                message_id: required(&part["messageID"], "message ID")?,
                part_id: required(&part["id"], "part ID")?,
                part_type: part_type.into(),
                tool,
            }
        }
        _ => bail!("event is outside the read-only event allowlist"),
    })
}

struct TargetStatus<'a>(&'a str);
impl<'de> DeserializeSeed<'de> for TargetStatus<'_> {
    type Value = Option<LiveStatus>;
    fn deserialize<D: serde::Deserializer<'de>>(
        self,
        decoder: D,
    ) -> std::result::Result<Self::Value, D::Error> {
        struct StatusVisitor<'a>(&'a str);
        impl<'de> Visitor<'de> for StatusVisitor<'_> {
            type Value = Option<LiveStatus>;
            fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
                formatter.write_str("a session status map")
            }
            fn visit_map<M: MapAccess<'de>>(
                self,
                mut map: M,
            ) -> std::result::Result<Self::Value, M::Error> {
                let mut selected = None;
                while let Some(key) = map.next_key::<String>()? {
                    if key == self.0 {
                        if selected.is_some() {
                            return Err(serde::de::Error::custom("duplicate target status"));
                        }
                        selected = Some(map.next_value::<LiveStatus>()?);
                    } else {
                        map.next_value::<IgnoredAny>()?;
                    }
                }
                Ok(selected)
            }
        }
        decoder.deserialize_map(StatusVisitor(self.0))
    }
}

struct StatusReader {
    receiver: tokio::sync::mpsc::Receiver<Vec<u8>>,
    chunk: Cursor<Vec<u8>>,
}

#[cfg(test)]
struct DecoderGuard(Arc<std::sync::atomic::AtomicUsize>);
#[cfg(test)]
impl DecoderGuard {
    fn new(count: Arc<std::sync::atomic::AtomicUsize>) -> Self {
        count.fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        Self(count)
    }
}
#[cfg(test)]
impl Drop for DecoderGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, std::sync::atomic::Ordering::AcqRel);
    }
}
impl Read for StatusReader {
    fn read(&mut self, output: &mut [u8]) -> std::io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        loop {
            let read = self.chunk.read(output)?;
            if read > 0 {
                return Ok(read);
            }
            match self.receiver.blocking_recv() {
                Some(chunk) => self.chunk = Cursor::new(chunk),
                None => return Ok(0),
            }
        }
    }
}

async fn await_io<F: Future>(
    future: F,
    deadline: Instant,
    cancellation: &SequenceCancellation,
) -> Result<F::Output> {
    ensure!(
        Instant::now() < deadline && !cancellation.is_cancelled(),
        "adapter read cancelled or deadline exceeded"
    );
    tokio::select! {
        output = future => Ok(output),
        _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => bail!("adapter deadline exceeded"),
        _ = async { while !cancellation.is_cancelled() { tokio::time::sleep(Duration::from_millis(10)).await; } } => bail!("adapter read cancelled"),
    }
}

fn validate_id(id: &str) -> Result<()> {
    ensure!(
        !id.is_empty()
            && id.len() <= 128
            && id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.')),
        "invalid scoped application identity"
    );
    Ok(())
}

/// SSE framing only; HTTP parsing and transfer framing belong to reqwest.
/// At most one event is assembled before filtering into the retained batch.
struct SseDecoder {
    line: Vec<u8>,
    data: Vec<u8>,
    max: usize,
}
impl SseDecoder {
    fn new(max: usize) -> Self {
        Self {
            line: Vec::new(),
            data: Vec::new(),
            max,
        }
    }
    fn feed(&mut self, bytes: &[u8], mut event: impl FnMut(&[u8]) -> Result<()>) -> Result<()> {
        for byte in bytes {
            if *byte == b'\n' {
                if self.line.last() == Some(&b'\r') {
                    self.line.pop();
                }
                if self.line.is_empty() {
                    if !self.data.is_empty() {
                        self.data.pop();
                        event(&self.data)?;
                        self.data.clear();
                    }
                } else if self.line.starts_with(b"data:") {
                    let payload = self.line.strip_prefix(b"data: ").unwrap_or(&self.line[5..]);
                    ensure!(
                        self.data.len() + payload.len() < self.max,
                        "SSE event exceeds bounded framing limit"
                    );
                    self.data.extend_from_slice(payload);
                    self.data.push(b'\n');
                }
                self.line.clear();
            } else {
                ensure!(
                    self.line.len() < self.max,
                    "SSE line exceeds bounded framing limit"
                );
                self.line.push(*byte);
            }
        }
        Ok(())
    }
    fn incomplete(&self) -> bool {
        !self.line.is_empty() || !self.data.is_empty()
    }
}
