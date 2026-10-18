mod protocol {
    pub use silicogen_terminal_runtime::protocol::*;
}
mod predicates {
    pub use silicogen_terminal_runtime::predicates::*;
}
mod validation {
    pub use silicogen_terminal_runtime::validation::*;
}
#[path = "../src/app_adapter.rs"]
#[allow(dead_code)]
mod app_adapter;
#[path = "../src/locators.rs"]
#[allow(dead_code)]
mod locators;
#[path = "../src/sequences.rs"]
#[allow(dead_code)]
mod sequences;
#[path = "../src/wait_engine.rs"]
#[allow(dead_code)]
mod wait_engine;

use app_adapter::*;
use axum::{
    Router,
    body::Body,
    extract::{OriginalUri, State},
    http::{Method, StatusCode},
    response::{IntoResponse, Response},
};
use sequences::SequenceCancellation;
use serde_json::{Value, json};
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
use std::time::{Duration, Instant};

#[derive(Clone)]
struct FixtureState {
    directory: String,
    messages: Arc<Mutex<Value>>,
    session: Arc<Mutex<Value>>,
    statuses: Arc<Mutex<String>>,
    events: Arc<Mutex<String>>,
    ui: Arc<Mutex<Option<Value>>>,
    redirect: Arc<AtomicBool>,
    slow: Arc<AtomicBool>,
    large: Arc<AtomicBool>,
    requests: Arc<Mutex<Vec<(String, String)>>>,
}
async fn handler(
    State(state): State<FixtureState>,
    method: Method,
    OriginalUri(uri): OriginalUri,
) -> Response {
    state
        .requests
        .lock()
        .unwrap()
        .push((method.to_string(), uri.to_string()));
    if method != Method::GET {
        return StatusCode::METHOD_NOT_ALLOWED.into_response();
    }
    let url = reqwest::Url::parse(&format!("http://127.0.0.1{uri}")).unwrap();
    if !url
        .query_pairs()
        .any(|(k, v)| k == "directory" && v == state.directory)
    {
        return StatusCode::BAD_REQUEST.into_response();
    }
    if state.redirect.load(Ordering::Acquire) {
        return (
            StatusCode::TEMPORARY_REDIRECT,
            [("location", "http://127.0.0.1:9/session/ses_other")],
        )
            .into_response();
    }
    match uri.path() {
        "/session/ses_a/message" => {
            if state.large.load(Ordering::Acquire) {
                let pieces = vec![
                    Ok::<_, std::io::Error>(vec![b' '; 2048]),
                    Ok(vec![b' '; 2048]),
                ];
                return Response::builder()
                    .header("content-type", "application/json")
                    .body(Body::from_stream(futures_util::stream::iter(pieces)))
                    .unwrap();
            }
            axum::Json(state.messages.lock().unwrap().clone()).into_response()
        }
        "/session/ses_a" => axum::Json(state.session.lock().unwrap().clone()).into_response(),
        "/session/status" => {
            if state.slow.load(Ordering::Acquire) {
                let stream = futures_util::stream::unfold(0u8, |step| async move {
                    match step {
                        0 => Some((Ok::<_, std::io::Error>(b"{\"ses_a\":".to_vec()), 1)),
                        1 => {
                            tokio::time::sleep(Duration::from_secs(2)).await;
                            Some((Ok(b"{\"type\":\"busy\"}}".to_vec()), 2))
                        }
                        _ => None,
                    }
                });
                return Response::builder()
                    .header("content-type", "application/json")
                    .body(Body::from_stream(stream))
                    .unwrap();
            }
            let status = state.statuses.lock().unwrap().clone();
            Response::builder()
                .header("content-type", "application/json")
                .body(Body::from(status))
                .unwrap()
        }
        "/event" => {
            let data = state.events.lock().unwrap().clone();
            let pieces: Vec<_> = data
                .as_bytes()
                .chunks(7)
                .map(|piece| Ok::<_, std::io::Error>(piece.to_vec()))
                .collect();
            Response::builder()
                .header("content-type", "text/event-stream")
                .body(Body::from_stream(futures_util::stream::iter(pieces)))
                .unwrap()
        }
        "/__emulation/ui-state" => match state.ui.lock().unwrap().as_ref() {
            Some(ui) => axum::Json(ui.clone()).into_response(),
            None => StatusCode::NOT_FOUND.into_response(),
        },
        _ => StatusCode::NOT_FOUND.into_response(),
    }
}

struct Fixture {
    state: FixtureState,
    port: u16,
    directory: PathBuf,
    task: tokio::task::JoinHandle<()>,
    _temp: tempfile::TempDir,
}
impl Fixture {
    async fn start() -> Self {
        Self::start_shared(None).await
    }
    async fn start_shared(shared: Option<&Self>) -> Self {
        let runtime = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(".runtime/app-adapter-core");
        std::fs::create_dir_all(&runtime).unwrap();
        let temp = tempfile::tempdir_in(&runtime).unwrap();
        let directory = shared.map_or_else(
            || std::fs::canonicalize(temp.path()).unwrap(),
            |other| other.directory.clone(),
        );
        let state=FixtureState { directory:directory.to_string_lossy().into_owned(), messages:Arc::new(Mutex::new(json!([]))),session:Arc::new(Mutex::new(json!({"id":"ses_a","directory":directory,"title":"Synthetic session","revert":{"messageID":"user1"},"recap":{"summary":"Synthetic catch-up","throughMessageID":"user1"}}))),statuses:Arc::new(Mutex::new(json!({"ses_other":{"type":"busy","synthetic_secret":"unrelated-status-sentinel"},"ses_a":{"type":"idle"}}).to_string())),events:Arc::new(Mutex::new(String::new())),ui:Arc::new(Mutex::new(None)), redirect:Arc::new(AtomicBool::new(false)), slow:Arc::new(AtomicBool::new(false)),large:Arc::new(AtomicBool::new(false)),requests:Arc::new(Mutex::new(vec![])) };
        let mut state = state;
        if let Some(other) = shared {
            state.messages = other.state.messages.clone();
            state.session = other.state.session.clone();
        }
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let router = Router::new().fallback(handler).with_state(state.clone());
        let task = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        Self {
            state,
            port,
            directory,
            task,
            _temp: temp,
        }
    }
    fn proof(&self, run: &str) -> OwnedBackendProof {
        OwnedBackendProof {
            owner_session_id: "owned-fixture".into(),
            owner_run_id: run.into(),
            pid: std::process::id(),
            process_start_identity: process_start(),
            loopback_port: self.port,
            selected_directory: self.directory.clone(),
        }
    }
    fn binding(&self, run: &str) -> BackendBinding {
        let proof = self.proof(run);
        BackendBinding::from_owned(
            proof.clone(),
            TuiAttachProof {
                terminal_session_id: "terminal".into(),
                terminal_run_id: format!("tui-{run}"),
                attached_backend: proof,
            },
            BTreeSet::from(["ses_a".into()]),
        )
        .unwrap()
    }
    fn adapter(&self) -> (SilicogenAdapter<Verifier>, Arc<Verifier>) {
        self.adapter_with("run1", AdapterLimits::default())
    }
    fn adapter_with(
        &self,
        run: &str,
        limits: AdapterLimits,
    ) -> (SilicogenAdapter<Verifier>, Arc<Verifier>) {
        let verifier = Arc::new(Verifier {
            expected: self.proof(run),
            valid: AtomicBool::new(true),
            calls: AtomicUsize::new(0),
        });
        (
            SilicogenAdapter::new(self.binding(run), verifier.clone(), limits).unwrap(),
            verifier,
        )
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct Verifier {
    expected: OwnedBackendProof,
    valid: AtomicBool,
    calls: AtomicUsize,
}
impl BindingVerifier for Verifier {
    fn verify_current(&self, binding: &BackendBinding) -> anyhow::Result<()> {
        self.calls.fetch_add(1, Ordering::AcqRel);
        anyhow::ensure!(
            self.valid.load(Ordering::Acquire),
            "owned run/process attachment is no longer current"
        );
        anyhow::ensure!(
            binding.owned_backend() == &self.expected
                && binding.tui_attachment().attached_backend == self.expected,
            "owned backend differs from attached process/run/port/directory"
        );
        anyhow::ensure!(
            self.expected.pid == std::process::id()
                && self.expected.process_start_identity == process_start(),
            "fixture process identity changed"
        );
        Ok(())
    }
}
fn process_start() -> u64 {
    let stat = std::fs::read_to_string(format!("/proc/{}/stat", std::process::id())).unwrap();
    stat.rsplit_once(')')
        .unwrap()
        .1
        .split_whitespace()
        .nth(19)
        .unwrap()
        .parse()
        .unwrap()
}
fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(1)
}
fn observation(run: &str) -> locators::ObservationIdentity {
    locators::ObservationIdentity {
        session_id: "terminal".into(),
        run_id: format!("tui-{run}"),
        frame_id: "frame1".into(),
        observation_revision: 10,
        viewport_revision: 1,
        cols: 80,
        rows: 24,
    }
}
fn message(completed: bool, tool_status: &str) -> Value {
    json!({"info":{"id":"assistant1","sessionID":"ses_a","role":"assistant","parentID":"user1","time":{"created":1,"completed":if completed {Some(2)} else {None}},"finish":if completed {Some("stop")} else {None}},"parts":[{"type":"text","text":"prompt text is not a semantic predicate","sessionID":"ses_a","messageID":"assistant1"},{"type":"tool","sessionID":"ses_a","messageID":"assistant1","callID":"call1","tool":"write","state":{"status":tool_status,"output":"fixture file written"}}]})
}
async fn decoders_stopped(adapter: &SilicogenAdapter<Verifier>) {
    tokio::time::timeout(Duration::from_millis(500), async {
        while adapter.active_status_decoders() != 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("orphaned blocking status decoder");
}

#[tokio::test]
async fn actual_http_reads_scoped_messages_session_and_filtered_status() {
    let fixture = Fixture::start().await;
    let (mut adapter, verifier) = fixture.adapter();
    *fixture.state.messages.lock().unwrap() = json!([message(true, "completed")]);
    let cancel = SequenceCancellation::default();
    let evidence = adapter
        .transcript("ses_a", deadline(), &cancel)
        .await
        .unwrap();
    assert_eq!(evidence.messages.len(), 1);
    assert!(!evidence.exact_visual_order_or_position);
    assert_eq!(evidence.provenance.assertion_plane, "application_data");
    let session = adapter.session("ses_a", deadline(), &cancel).await.unwrap();
    assert_eq!(session.revert.unwrap().message_id, "user1");
    assert_eq!(
        session.recap.unwrap().summary.as_deref(),
        Some("Synthetic catch-up")
    );
    let status = adapter.status("ses_a", deadline(), &cancel).await.unwrap();
    assert_eq!(status.status, Some(LiveStatus::Idle));
    assert!(
        !serde_json::to_string(&status)
            .unwrap()
            .contains("unrelated-status-sentinel")
    );
    assert!(verifier.calls.load(Ordering::Acquire) >= 10);
    for (method, uri) in fixture.state.requests.lock().unwrap().iter() {
        assert_eq!(method, "GET");
        assert!(uri.contains("directory="));
        assert!(!uri.contains("/global/health"));
    }
}

#[tokio::test]
async fn separate_backend_sharing_data_is_not_tui_busy_proof() {
    let a = Fixture::start().await;
    let b = Fixture::start_shared(Some(&a)).await;
    assert!(Arc::ptr_eq(&a.state.messages, &b.state.messages));
    assert_eq!(a.directory, b.directory);
    *b.state.statuses.lock().unwrap() = json!({"ses_a":{"type":"busy"}}).to_string();
    let proof_a = a.proof("run1");
    let proof_b = b.proof("run1");
    assert!(
        BackendBinding::from_owned(
            proof_b.clone(),
            TuiAttachProof {
                terminal_session_id: "terminal".into(),
                terminal_run_id: "tui-run1".into(),
                attached_backend: proof_a.clone()
            },
            BTreeSet::from(["ses_a".into()])
        )
        .is_err()
    );
    let wrong_verifier = Arc::new(Verifier {
        expected: proof_a,
        valid: AtomicBool::new(true),
        calls: AtomicUsize::new(0),
    });
    assert!(
        SilicogenAdapter::new(b.binding("run1"), wrong_verifier, AdapterLimits::default()).is_err()
    );
    assert!(b.state.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn schemes_urls_other_sessions_queries_and_redirects_are_denied() {
    let fixture = Fixture::start().await;
    let binding = fixture.binding("run1");
    for raw in [
        format!("https://127.0.0.1:{}/session/ses_a", fixture.port),
        format!("http://localhost:{}/session/ses_a", fixture.port),
        format!("http://127.0.0.1:{}/session/ses_other", fixture.port),
        format!("http://127.0.0.1:{}/global/health", fixture.port),
        format!(
            "http://127.0.0.1:{}/session/ses_a?directory=/etc",
            fixture.port
        ),
        "file:///etc/passwd".to_string(),
    ] {
        assert!(
            binding
                .validate_destination(&reqwest::Url::parse(&raw).unwrap(), false)
                .is_err(),
            "accepted {raw}"
        );
    }
    let (mut adapter, _) = fixture.adapter();
    let cancel = SequenceCancellation::default();
    assert!(
        adapter
            .messages("ses_other", deadline(), &cancel)
            .await
            .is_err()
    );
    assert!(
        adapter
            .messages("ses_a/../../event", deadline(), &cancel)
            .await
            .is_err()
    );
    assert!(fixture.state.requests.lock().unwrap().is_empty());
    fixture.state.redirect.store(true, Ordering::Release);
    assert!(
        adapter
            .messages("ses_a", deadline(), &cancel)
            .await
            .unwrap_err()
            .to_string()
            .contains("redirect")
    );
    assert_eq!(fixture.state.requests.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn requests_and_sse_reconnects_recheck_live_ownership() {
    let fixture = Fixture::start().await;
    let (mut adapter, verifier) = fixture.adapter();
    let cancel = SequenceCancellation::default();
    adapter.read_events(deadline(), &cancel).await.unwrap();
    let requests = fixture.state.requests.lock().unwrap().len();
    verifier.valid.store(false, Ordering::Release);
    assert!(adapter.read_events(deadline(), &cancel).await.is_err());
    assert!(
        adapter
            .messages("ses_a", deadline(), &cancel)
            .await
            .is_err()
    );
    assert_eq!(fixture.state.requests.lock().unwrap().len(), requests);
}

#[tokio::test]
async fn sse_filters_unrelated_and_unknown_events_before_retained_batch() {
    let fixture = Fixture::start().await;
    let (mut adapter, _) = fixture.adapter();
    let events = [
        json!({"type":"session.status","properties":{"sessionID":"ses_other","status":{"type":"busy"},"sentinel":"unrelated-event-sentinel"}}),
        json!({"type":"unknown","properties":{"sessionID":"ses_a","sentinel":"unknown-event-sentinel"}}),
        json!({"type":"session.status","properties":{"sessionID":"ses_a","status":{"type":"busy","ignored":"typed-extra-sentinel"},"ignored":"typed-extra-sentinel"}}),
    ];
    *fixture.state.events.lock().unwrap() = events
        .iter()
        .map(|event| format!("data: {event}\r\n\r\n"))
        .collect::<String>()
        + "data: {\"partial\":";
    let batch = adapter
        .read_events(deadline(), &SequenceCancellation::default())
        .await
        .unwrap();
    assert_eq!(batch.events.len(), 1);
    assert_eq!(batch.filtered_events, 2);
    assert_eq!(batch.events[0].application_session_id, "ses_a");
    assert!(!format!("{batch:?}").contains("unrelated-event-sentinel"));
    assert!(!format!("{batch:?}").contains("unknown-event-sentinel"));
    assert!(!format!("{batch:?}").contains("typed-extra-sentinel"));
    assert_eq!(batch.stop_reason, "incomplete_event");
    assert!(!batch.complete_stream);
}

#[tokio::test]
async fn ui_producer_absent_connected_dialog_and_restart_correlation() {
    let fixture = Fixture::start().await;
    let (mut adapter, _) = fixture.adapter();
    let cancel = SequenceCancellation::default();
    assert!(matches!(
        adapter.ui_state(None, deadline(), &cancel).await.unwrap(),
        UiAvailability::Unavailable { .. }
    ));
    assert!(fixture.state.requests.lock().unwrap().is_empty());
    assert!(matches!(
        adapter
            .ui_state(Some("producer1"), deadline(), &cancel)
            .await
            .unwrap(),
        UiAvailability::Unavailable { .. }
    ));
    let mut state = json!({"version":1,"producer_instance":"producer1","backend_session_id":"owned-fixture","backend_run_id":"run1","observation":observation("run1"),"sequence":5,"active_application_session_id":"ses_a","route":"session","dialog":"palette","filter":"subagents","focus":"filter"});
    *fixture.state.ui.lock().unwrap() = Some(state.clone());
    let UiAvailability::Available(ui) = adapter
        .ui_state(Some("producer1"), deadline(), &cancel)
        .await
        .unwrap()
    else {
        panic!("fixture producer missing")
    };
    assert_eq!(ui.dialog.as_deref(), Some("palette"));
    assert_eq!(ui.observation.observation_revision, 10);
    state["sequence"] = json!(6);
    state["dialog"] = Value::Null;
    state["filter"] = Value::Null;
    state["focus"] = json!("transcript");
    *fixture.state.ui.lock().unwrap() = Some(state.clone());
    let UiAvailability::Available(ui) = adapter.ui_state(None, deadline(), &cancel).await.unwrap()
    else {
        panic!("fixture producer missing")
    };
    assert!(ui.dialog.is_none());
    let (mut restarted, _) = fixture.adapter_with("run2", AdapterLimits::default());
    assert!(
        restarted
            .ui_state(Some("producer2"), deadline(), &cancel)
            .await
            .is_err(),
        "stale state crossed backend restart"
    );
    state["backend_run_id"] = json!("run2");
    state["producer_instance"] = json!("producer2");
    state["sequence"] = json!(1);
    state["observation"] = serde_json::to_value(observation("run2")).unwrap();
    *fixture.state.ui.lock().unwrap() = Some(state);
    assert!(matches!(
        restarted
            .ui_state(Some("producer2"), deadline(), &cancel)
            .await
            .unwrap(),
        UiAvailability::Available(_)
    ));
}

#[tokio::test]
async fn semantic_completion_requires_postbaseline_roles_parent_and_tool_evidence() {
    let fixture = Fixture::start().await;
    let (mut adapter, _) = fixture.adapter();
    let cancel = SequenceCancellation::default();
    *fixture.state.messages.lock().unwrap() = json!([message(false, "running")]);
    let before = adapter
        .messages("ses_a", deadline(), &cancel)
        .await
        .unwrap();
    let baseline = before.baseline().unwrap();
    let turn = SemanticPredicate::TurnCompleted {
        user_message_id: "user1".into(),
    };
    let tool = SemanticPredicate::ToolCompleted {
        user_message_id: "user1".into(),
        call_id: "call1".into(),
        tool: "write".into(),
    };
    assert!(evaluate_semantic(&baseline, &before, &turn).is_err());
    assert!(
        !adapter
            .tool_completion("ses_a", &baseline, &tool, deadline(), &cancel)
            .await
            .unwrap()
    );
    *fixture.state.messages.lock().unwrap() = json!([message(true, "completed")]);
    let after = adapter
        .messages("ses_a", deadline(), &cancel)
        .await
        .unwrap();
    assert!(evaluate_semantic(&baseline, &after, &turn).unwrap());
    assert!(evaluate_semantic(&baseline, &after, &tool).unwrap());
    let completed_baseline = after.baseline().unwrap();
    let repeated = adapter
        .messages("ses_a", deadline(), &cancel)
        .await
        .unwrap();
    assert!(!evaluate_semantic(&completed_baseline, &repeated, &tool).unwrap());
    let wrong = SemanticPredicate::TurnCompleted {
        user_message_id: "other_user".into(),
    };
    assert!(!evaluate_semantic(&baseline, &after, &wrong).unwrap());
}

#[tokio::test]
async fn data_present_view_missing_differs_from_data_absent() {
    let fixture = Fixture::start().await;
    let (mut adapter, _) = fixture.adapter();
    let cancel = SequenceCancellation::default();
    *fixture.state.messages.lock().unwrap() = json!([message(true, "completed")]);
    let present = adapter
        .messages("ses_a", deadline(), &cancel)
        .await
        .unwrap();
    assert_eq!(
        diagnose_render_presence(&present, "assistant1", &observation("run1"), false).unwrap(),
        PresenceDiagnosis::DataPresentViewMissing
    );
    assert_eq!(
        diagnose_render_presence(&present, "assistant1", &observation("run1"), true).unwrap(),
        PresenceDiagnosis::DataPresentAndRendered
    );
    *fixture.state.messages.lock().unwrap() = json!([]);
    let absent = adapter
        .messages("ses_a", deadline(), &cancel)
        .await
        .unwrap();
    assert_eq!(
        diagnose_render_presence(&absent, "assistant1", &observation("run1"), false).unwrap(),
        PresenceDiagnosis::DataAbsent
    );
    assert!(diagnose_render_presence(&absent, "assistant1", &observation("run2"), false).is_err());
}

#[tokio::test]
async fn status_pipeline_cancel_deadline_truncated_and_overbudget_leave_no_decoder() {
    let fixture = Fixture::start().await;
    let (mut adapter, _) = fixture.adapter();
    fixture.state.slow.store(true, Ordering::Release);
    let cancel = SequenceCancellation::default();
    let trigger = cancel.clone();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(30)).await;
        trigger.cancel();
    });
    assert!(adapter.status("ses_a", deadline(), &cancel).await.is_err());
    decoders_stopped(&adapter).await;
    assert!(
        adapter
            .status(
                "ses_a",
                Instant::now() + Duration::from_millis(30),
                &SequenceCancellation::default()
            )
            .await
            .is_err()
    );
    decoders_stopped(&adapter).await;
    fixture.state.slow.store(false, Ordering::Release);
    *fixture.state.statuses.lock().unwrap() = "{\"ses_a\":".into();
    assert!(
        adapter
            .status("ses_a", deadline(), &SequenceCancellation::default())
            .await
            .is_err()
    );
    decoders_stopped(&adapter).await;
    *fixture.state.statuses.lock().unwrap() = format!(
        "{{\"ses_a\":{{\"type\":\"busy\"}},\"other\":\"{}\"}}",
        "x".repeat(4096)
    );
    let limits = AdapterLimits {
        max_response_bytes: 1024,
        ..Default::default()
    };
    let (mut bounded, _) = fixture.adapter_with("run1", limits);
    assert!(
        bounded
            .status("ses_a", deadline(), &SequenceCancellation::default())
            .await
            .is_err()
    );
    decoders_stopped(&bounded).await;
}

#[tokio::test]
async fn message_body_and_sse_framing_are_bounded_before_json_deserialization() {
    let fixture = Fixture::start().await;
    let limits = AdapterLimits {
        max_response_bytes: 1024,
        max_event_bytes: 128,
        max_stream_bytes: 1024,
        ..Default::default()
    };
    let (mut adapter, _) = fixture.adapter_with("run1", limits);
    fixture.state.large.store(true, Ordering::Release);
    assert!(
        adapter
            .messages("ses_a", deadline(), &SequenceCancellation::default())
            .await
            .is_err()
    );
    *fixture.state.events.lock().unwrap() = format!("data: {}\n\n", "x".repeat(512));
    assert!(
        adapter
            .read_events(deadline(), &SequenceCancellation::default())
            .await
            .is_err()
    );
    fixture.state.large.store(false, Ordering::Release);
    let mut unrelated = message(true, "completed");
    unrelated["info"]["sessionID"] = json!("ses_other");
    *fixture.state.messages.lock().unwrap() = json!([unrelated]);
    assert!(
        adapter
            .messages("ses_a", deadline(), &SequenceCancellation::default())
            .await
            .is_err()
    );
}
