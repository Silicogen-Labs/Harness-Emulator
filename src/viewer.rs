use crate::control::{BrowserAuthority, BrowserLease};
use crate::frame_stream::StreamFrame;
use crate::protocol::{MouseInput, RpcRequest, SessionSummary};
use crate::service::SessionService;
use crate::session::{BrowserActionContext, BrowserInput, Session};
use anyhow::{Context, Result, bail};
use axum::Router;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Path, Request, State};
use axum::http::header::{CACHE_CONTROL, CONTENT_TYPE, HOST, ORIGIN};
use axum::http::{HeaderMap, HeaderValue, StatusCode};
use axum::middleware::{self, Next};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use base64::Engine as _;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{HashMap, VecDeque};
use std::fs::File;
use std::io::Read;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use tokio::net::TcpListener;
use tokio::sync::{Mutex as AsyncMutex, watch};
use ulid::Ulid;

const BOOTSTRAP_TTL: Duration = Duration::from_secs(5 * 60);
const RECONNECT_TTL: Duration = Duration::from_secs(8 * 60 * 60);
const AUTH_TIMEOUT: Duration = Duration::from_secs(5);
const SEND_TIMEOUT: Duration = Duration::from_secs(2);
const FRAME_INTERVAL: Duration = Duration::from_millis(100);
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);
const VIEW_IDLE_GRACE: Duration = Duration::from_secs(30);
const MAX_INPUT_BYTES: usize = 128 * 1024;
const MAX_FRAME_BYTES: usize = 4 * 1024 * 1024;
const MAX_SCREENSHOT_BYTES: usize = 8 * 1024 * 1024;
const MAX_CONNECTIONS_PER_VIEW: usize = 1;
const MAX_CONNECTIONS_PER_SESSION: usize = 4;
const MAX_CONNECTIONS_PER_DAEMON: usize = 16;
const MAX_LIVE_GRANTS: usize = 64;
const MAX_FRAME_COUNT_PER_SESSION: usize = 8;
const MAX_FRAME_BYTES_PER_SESSION: usize = 16 * 1024 * 1024;
const MAX_FRAME_BYTES_PER_DAEMON: usize = 64 * 1024 * 1024;
const MAX_ACTION_RESULTS: usize = 256;
const MAX_ACTION_CACHE_AGE: Duration = Duration::from_secs(120);

#[derive(Clone)]
pub struct ViewerServer {
    inner: Arc<ViewerInner>,
}

struct ViewerInner {
    sessions: Arc<SessionService>,
    grants: Mutex<GrantStore>,
    listener: AsyncMutex<Option<RunningListener>>,
    streams: Mutex<HashMap<String, SessionStream>>,
    frame_cache: Mutex<HashMap<String, VecDeque<Arc<SharedFrame>>>>,
    total_connections: Arc<AtomicUsize>,
}

struct RunningListener {
    address: SocketAddr,
    stop: watch::Sender<bool>,
    task: tokio::task::JoinHandle<()>,
}

#[derive(Default)]
struct GrantStore {
    grants: HashMap<String, ViewGrant>,
    idempotency: HashMap<(String, String), String>,
}

struct ViewGrant {
    id: String,
    session_id: String,
    session: Arc<Session>,
    _evidence: crate::session::EvidenceLease,
    allow_browser_control: bool,
    bootstrap_hash: Option<[u8; 32]>,
    bootstrap_secret: Option<String>,
    bootstrap_expires: Instant,
    reconnect_hash: Option<[u8; 32]>,
    reconnect_expires: Option<Instant>,
    expires: Instant,
    revoked: bool,
    connections: Arc<AtomicUsize>,
    revoke: watch::Sender<bool>,
}

#[derive(Debug, Clone, Serialize)]
pub struct ViewStartResult {
    pub view_id: String,
    pub session_id: String,
    pub run_id: String,
    pub url: String,
    pub bind_address: String,
    pub port: u16,
    pub bootstrap_expires_at_ms: u128,
    pub allow_browser_control: bool,
    pub execution_mode: &'static str,
}

#[derive(Debug, Clone, Serialize)]
pub struct ViewStatus {
    pub view_id: String,
    pub session_id: String,
    pub state: &'static str,
    pub active_connections: usize,
    pub allow_browser_control: bool,
    pub expires_in_ms: u64,
}

struct AuthenticatedView {
    view_id: String,
    session_id: String,
    session: Arc<Session>,
    connection_id: String,
    allow_browser_control: bool,
    reconnect_secret: Option<String>,
    expires: Instant,
    revoked: watch::Receiver<bool>,
    _connection: ConnectionLease,
}

struct SocketClientState {
    authenticated: AuthenticatedView,
    session: Arc<Session>,
    authority: Option<BrowserAuthority>,
    actions: HashMap<String, CachedAction>,
    action_order: VecDeque<String>,
}

struct ConnectionLease {
    per_view: Arc<AtomicUsize>,
    total: Arc<AtomicUsize>,
}

impl Drop for ConnectionLease {
    fn drop(&mut self) {
        self.per_view.fetch_sub(1, Ordering::AcqRel);
        self.total.fetch_sub(1, Ordering::AcqRel);
    }
}

struct SessionStream {
    sender: watch::Sender<StreamUpdate>,
    snapshot_lock: Arc<Mutex<()>>,
    stop: watch::Sender<bool>,
}

struct SharedFrame {
    frame: Arc<StreamFrame>,
    json: Arc<str>,
    bytes: usize,
    captured_at: Instant,
}

#[derive(Clone)]
enum StreamUpdate {
    Frame(Arc<SharedFrame>),
    Error { code: &'static str, message: String },
}

#[derive(Clone)]
struct HttpState {
    inner: Arc<ViewerInner>,
    expected_host: String,
    expected_origin: String,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ClientMessage {
    Logs {
        #[serde(default = "default_log_source")]
        source: String,
        #[serde(default)]
        cursor: Option<Value>,
    },
    Authenticate {
        token: String,
    },
    TakeControl,
    ReleaseControl {
        lease_id: String,
        generation: u64,
    },
    HeartbeatAck {
        lease_id: Option<String>,
        generation: Option<u64>,
    },
    Input {
        action_id: String,
        run_id: String,
        frame_id: String,
        view_revision: u64,
        viewport_revision: u64,
        lease_id: String,
        generation: u64,
        input: BrowserInputMessage,
    },
    Snapshot {
        run_id: String,
        frame_id: String,
        view_revision: u64,
    },
}

fn default_log_source() -> String {
    "app".into()
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum BrowserInputMessage {
    Key { key: String },
    Type { text: String },
    Paste { text: String },
    Mouse { input: MouseInput },
    Resize { cols: u16, rows: u16 },
}

#[derive(Clone)]
struct CachedAction {
    digest: [u8; 32],
    result: Value,
    stored_at: Instant,
}

impl ViewerServer {
    pub fn new(sessions: Arc<SessionService>) -> Self {
        Self {
            inner: Arc::new(ViewerInner {
                sessions,
                grants: Mutex::new(GrantStore::default()),
                listener: AsyncMutex::new(None),
                streams: Mutex::new(HashMap::new()),
                frame_cache: Mutex::new(HashMap::new()),
                total_connections: Arc::new(AtomicUsize::new(0)),
            }),
        }
    }

    pub async fn handle_request(&self, request: RpcRequest) -> Result<Value> {
        match request {
            RpcRequest::ViewStart {
                session_id,
                allow_browser_control,
                idempotency_key,
            } => Ok(serde_json::to_value(
                self.start_view(&session_id, allow_browser_control, idempotency_key)
                    .await?,
            )?),
            RpcRequest::ViewStatus { view_id } => {
                Ok(serde_json::to_value(self.view_status(&view_id)?)?)
            }
            RpcRequest::ViewStop { view_id } => {
                self.stop_view(&view_id)?;
                Ok(json!({"view_id":view_id,"revoked":true}))
            }
            _ => bail!("unsupported viewer operation"),
        }
    }

    async fn start_view(
        &self,
        session_id: &str,
        allow_browser_control: bool,
        idempotency_key: Option<String>,
    ) -> Result<ViewStartResult> {
        let session = self.inner.sessions.viewer_session(session_id)?;
        let summary = session.summary();
        let idempotency_key = idempotency_key
            .map(|value| validate_idempotency_key(&value).map(|_| value))
            .transpose()?;
        let (id, secret, created) = {
            let mut store = self
                .inner
                .grants
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            prune_grants(&mut store);
            if let Some(key) = &idempotency_key
                && let Some(existing_id) =
                    store.idempotency.get(&(session_id.to_owned(), key.clone()))
            {
                let existing = store.grants.get(existing_id).ok_or_else(|| {
                    anyhow::anyhow!("view_grant_expired: use a new idempotency key")
                })?;
                if existing.revoked || existing.expires <= Instant::now() {
                    bail!("view_grant_expired: use a new idempotency key to create another view")
                }
                let secret = existing.bootstrap_secret.clone().ok_or_else(|| {
                    anyhow::anyhow!("view_link_already_redeemed: create a new view grant to open another browser")
                })?;
                (existing.id.clone(), secret, false)
            } else {
                if store.grants.len() >= MAX_LIVE_GRANTS {
                    bail!(
                        "view_grant_limit: close or revoke an existing view before creating another"
                    )
                }
                let id = Ulid::generate().to_string();
                let secret = random_secret()?;
                let (revoke, _) = watch::channel(false);
                let grant = ViewGrant {
                    id: id.clone(),
                    session_id: session_id.into(),
                    session: Arc::clone(&session),
                    _evidence: session.retain_evidence()?,
                    allow_browser_control,
                    bootstrap_hash: Some(token_hash(&secret)),
                    bootstrap_secret: Some(secret.clone()),
                    bootstrap_expires: Instant::now() + BOOTSTRAP_TTL,
                    reconnect_hash: None,
                    reconnect_expires: None,
                    expires: Instant::now() + BOOTSTRAP_TTL,
                    revoked: false,
                    connections: Arc::new(AtomicUsize::new(0)),
                    revoke,
                };
                store.grants.insert(id.clone(), grant);
                if let Some(key) = idempotency_key {
                    store
                        .idempotency
                        .insert((session_id.into(), key), id.clone());
                }
                (id, secret, true)
            }
        };

        let address = match self.ensure_listener().await {
            Ok(address) => address,
            Err(error) => {
                if created {
                    self.stop_view(&id)?;
                }
                return Err(error);
            }
        };
        let url_host = format!("127.0.0.1:{}", address.port());
        let store = self
            .inner
            .grants
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let grant = store
            .grants
            .get(&id)
            .ok_or_else(|| anyhow::anyhow!("view_grant_expired"))?;
        Ok(view_start_result(
            grant,
            &secret,
            &url_host,
            address.port(),
            &summary,
        ))
    }

    fn view_status(&self, view_id: &str) -> Result<ViewStatus> {
        let mut store = self
            .inner
            .grants
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        prune_grants(&mut store);
        let grant = store
            .grants
            .get(view_id)
            .ok_or_else(|| anyhow::anyhow!("unknown or expired view grant"))?;
        Ok(ViewStatus {
            view_id: grant.id.clone(),
            session_id: grant.session_id.clone(),
            state: if grant.revoked { "revoked" } else { "active" },
            active_connections: grant.connections.load(Ordering::Acquire),
            allow_browser_control: grant.allow_browser_control,
            expires_in_ms: grant
                .expires
                .saturating_duration_since(Instant::now())
                .as_millis() as u64,
        })
    }

    fn stop_view(&self, view_id: &str) -> Result<()> {
        let mut store = self
            .inner
            .grants
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let Some(grant) = store.grants.get_mut(view_id) else {
            return Ok(());
        };
        grant.revoked = true;
        grant.bootstrap_secret = None;
        grant.bootstrap_hash = None;
        grant.reconnect_hash = None;
        grant.expires = Instant::now();
        grant.revoke.send_replace(true);
        Ok(())
    }

    async fn ensure_listener(&self) -> Result<SocketAddr> {
        let mut current = self.inner.listener.lock().await;
        if let Some(listener) = current.as_ref() {
            return Ok(listener.address);
        }
        let listener = TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
            .await
            .context("bind local browser viewer")?;
        let address = listener.local_addr()?;
        let expected_host = format!("127.0.0.1:{}", address.port());
        let expected_origin = format!("http://{expected_host}");
        let http_state = HttpState {
            inner: Arc::clone(&self.inner),
            expected_host: expected_host.clone(),
            expected_origin,
        };
        let app = Router::new()
            .route("/view/{view_id}", get(view_page))
            .route("/favicon.ico", get(favicon))
            .route("/assets/viewer.js", get(viewer_js))
            .route("/assets/styles.css", get(viewer_css))
            .route("/assets/fonts/DejaVuSansMono.ttf", get(viewer_font))
            .route("/api/views/{view_id}/ws", get(view_socket))
            .with_state(http_state)
            .layer(middleware::from_fn_with_state(expected_host, host_guard));
        let (stop, mut stop_rx) = watch::channel(false);
        let task = tokio::spawn(async move {
            let result = axum::serve(listener, app)
                .with_graceful_shutdown(async move {
                    let _ = stop_rx.changed().await;
                })
                .await;
            if let Err(error) = result {
                eprintln!("terminal viewer server stopped: {error}");
            }
        });
        *current = Some(RunningListener {
            address,
            stop,
            task,
        });
        let monitor = self.clone();
        tokio::spawn(async move {
            monitor.idle_reaper().await;
        });
        Ok(address)
    }

    async fn idle_reaper(&self) {
        loop {
            tokio::time::sleep(VIEW_IDLE_GRACE).await;
            let mut listener = self.inner.listener.lock().await;
            if listener.is_none() {
                return;
            }
            let still_used = {
                let mut grants = self
                    .inner
                    .grants
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                prune_grants(&mut grants);
                grants.grants.values().any(|grant| {
                    (!grant.revoked && grant.expires > Instant::now())
                        || grant.connections.load(Ordering::Acquire) > 0
                })
            };
            if still_used {
                continue;
            }
            let Some(running) = listener.take() else {
                return;
            };
            running.stop.send_replace(true);
            drop(listener);
            let _ = tokio::time::timeout(Duration::from_secs(2), running.task).await;
            let mut streams = self
                .inner
                .streams
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for stream in streams.values() {
                stream.stop.send_replace(true);
            }
            streams.clear();
            self.inner
                .frame_cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clear();
            return;
        }
    }

    pub async fn shutdown(&self) {
        {
            let mut store = self
                .inner
                .grants
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            for grant in store.grants.values_mut() {
                grant.revoked = true;
                grant.revoke.send_replace(true);
            }
        }
        if let Some(listener) = self.inner.listener.lock().await.take() {
            listener.stop.send_replace(true);
            let _ = tokio::time::timeout(Duration::from_secs(2), listener.task).await;
        }
        let mut streams = self
            .inner
            .streams
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        for stream in streams.values() {
            stream.stop.send_replace(true);
        }
        streams.clear();
        self.inner
            .frame_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }
}

fn view_start_result(
    grant: &ViewGrant,
    secret: &str,
    host: &str,
    port: u16,
    summary: &SessionSummary,
) -> ViewStartResult {
    ViewStartResult {
        view_id: grant.id.clone(),
        session_id: grant.session_id.clone(),
        run_id: summary.run_id.clone(),
        url: format!("http://{host}/view/{}#token={secret}", grant.id),
        bind_address: "127.0.0.1".into(),
        port,
        bootstrap_expires_at_ms: unix_ms().saturating_add(
            grant
                .bootstrap_expires
                .saturating_duration_since(Instant::now())
                .as_millis(),
        ),
        allow_browser_control: grant.allow_browser_control,
        execution_mode: "local_process",
    }
}

fn unix_ms() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
}

fn random_secret() -> Result<String> {
    let mut bytes = [0_u8; 32];
    File::open("/dev/urandom")
        .context("open operating-system random source")?
        .read_exact(&mut bytes)
        .context("read operating-system random source")?;
    Ok(URL_SAFE_NO_PAD.encode(bytes))
}

fn token_hash(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

fn constant_time_equal(left: &[u8; 32], right: &[u8; 32]) -> bool {
    use subtle::ConstantTimeEq;
    bool::from(left.ct_eq(right))
}

fn validate_idempotency_key(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        bail!("idempotency_key must be 1..=128 ASCII letters, digits, '-' or '_'")
    }
    Ok(())
}

fn prune_grants(store: &mut GrantStore) {
    let now = Instant::now();
    store.grants.retain(|_, grant| {
        !(grant.revoked || grant.expires <= now) || grant.connections.load(Ordering::Acquire) > 0
    });
    store
        .idempotency
        .retain(|_, view_id| store.grants.contains_key(view_id));
}

async fn host_guard(State(expected_host): State<String>, request: Request, next: Next) -> Response {
    let actual_host = request
        .headers()
        .get(HOST)
        .and_then(|value| value.to_str().ok());
    if actual_host != Some(expected_host.as_str()) {
        return StatusCode::FORBIDDEN.into_response();
    }
    next.run(request).await
}

async fn view_page(State(state): State<HttpState>, Path(view_id): Path<String>) -> Response {
    if !view_grant_is_live(&state.inner, &view_id) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let csp = format!(
        "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self' ws://{}; img-src 'self' blob:; font-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'",
        state.expected_host
    );
    let mut response = Response::new(axum::body::Body::from(include_str!("../web/index.html")));
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/html; charset=utf-8"),
    );
    apply_security_headers(response.headers_mut(), &csp);
    response
}

async fn viewer_js(State(state): State<HttpState>) -> Response {
    let mut response = Response::new(axum::body::Body::from(include_str!("../web/viewer.js")));
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/javascript; charset=utf-8"),
    );
    apply_security_headers(
        response.headers_mut(),
        &format!(
            "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self' ws://{}; img-src 'self' blob:; font-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'none'",
            state.expected_host
        ),
    );
    response
}

async fn viewer_css(State(state): State<HttpState>) -> Response {
    let mut response = Response::new(axum::body::Body::from(include_str!("../web/styles.css")));
    response.headers_mut().insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/css; charset=utf-8"),
    );
    apply_security_headers(
        response.headers_mut(),
        "default-src 'none'; style-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'",
    );
    let _ = state;
    response
}

async fn viewer_font() -> Response {
    let mut response = Response::new(axum::body::Body::from(include_bytes!(
        "../web/fonts/DejaVuSansMono.ttf"
    ) as &'static [u8]));
    response
        .headers_mut()
        .insert(CONTENT_TYPE, HeaderValue::from_static("font/ttf"));
    apply_security_headers(
        response.headers_mut(),
        "default-src 'none'; font-src 'self'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'",
    );
    response
}

fn apply_security_headers(headers: &mut HeaderMap, csp: &str) {
    if let Ok(value) = HeaderValue::from_str(csp) {
        headers.insert("content-security-policy", value);
    }
    headers.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    headers.insert("referrer-policy", HeaderValue::from_static("no-referrer"));
    headers.insert(
        "x-content-type-options",
        HeaderValue::from_static("nosniff"),
    );
    headers.insert("x-frame-options", HeaderValue::from_static("DENY"));
}

fn view_grant_is_live(inner: &ViewerInner, view_id: &str) -> bool {
    let mut store = inner
        .grants
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    prune_grants(&mut store);
    store
        .grants
        .get(view_id)
        .is_some_and(|grant| !grant.revoked && grant.expires > Instant::now())
}

async fn view_socket(
    State(state): State<HttpState>,
    Path(view_id): Path<String>,
    headers: HeaderMap,
    websocket: WebSocketUpgrade,
) -> Response {
    let origin = headers.get(ORIGIN).and_then(|value| value.to_str().ok());
    if origin != Some(state.expected_origin.as_str()) {
        return StatusCode::FORBIDDEN.into_response();
    }
    if !view_grant_is_live(&state.inner, &view_id) {
        return StatusCode::NOT_FOUND.into_response();
    }
    websocket
        .max_message_size(MAX_INPUT_BYTES)
        .max_frame_size(MAX_INPUT_BYTES)
        .on_upgrade(move |socket| serve_socket(socket, state.inner, view_id))
}

impl ViewerInner {
    fn authenticate(&self, view_id: &str, token: &str) -> Result<AuthenticatedView> {
        if token.len() > 128 {
            bail!("view_authentication_failed")
        }
        let now = Instant::now();
        let mut store = self
            .grants
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        prune_grants(&mut store);
        let session_id = store
            .grants
            .get(view_id)
            .ok_or_else(|| anyhow::anyhow!("view_authentication_failed"))?
            .session_id
            .clone();
        let per_session = store
            .grants
            .values()
            .filter(|other| other.session_id == session_id)
            .map(|other| other.connections.load(Ordering::Acquire))
            .sum::<usize>();
        let grant = store
            .grants
            .get_mut(view_id)
            .ok_or_else(|| anyhow::anyhow!("view_authentication_failed"))?;
        if grant.revoked || grant.expires <= now {
            bail!("view_grant_expired")
        }

        let provided_hash = token_hash(token);
        let mut new_reconnect_secret = None;
        let valid = if !grant.revoked
            && grant.bootstrap_secret.is_some()
            && grant.bootstrap_expires > now
            && !grant.bootstrap_hash.is_none()
            && grant
                .bootstrap_hash
                .as_ref()
                .is_some_and(|expected| constant_time_equal(expected, &provided_hash))
        {
            let reconnect_secret = random_secret()?;
            grant.bootstrap_secret = None;
            grant.bootstrap_hash = None;
            grant.reconnect_hash = Some(token_hash(&reconnect_secret));
            grant.reconnect_expires = Some(now + RECONNECT_TTL);
            grant.expires = now + RECONNECT_TTL;
            new_reconnect_secret = Some(reconnect_secret);
            true
        } else {
            grant.reconnect_expires.is_some_and(|expires| expires > now)
                && grant
                    .reconnect_hash
                    .as_ref()
                    .is_some_and(|expected| constant_time_equal(expected, &provided_hash))
        };
        if !valid {
            bail!("view_authentication_failed")
        }
        let per_view = grant.connections.load(Ordering::Acquire);
        let total = self.total_connections.load(Ordering::Acquire);
        if per_view >= MAX_CONNECTIONS_PER_VIEW
            || per_session >= MAX_CONNECTIONS_PER_SESSION
            || total >= MAX_CONNECTIONS_PER_DAEMON
        {
            bail!("viewer_connection_limit")
        }
        grant.connections.fetch_add(1, Ordering::AcqRel);
        self.total_connections.fetch_add(1, Ordering::AcqRel);
        let connection_id = Ulid::generate().to_string();
        Ok(AuthenticatedView {
            view_id: grant.id.clone(),
            session_id: grant.session_id.clone(),
            session: Arc::clone(&grant.session),
            connection_id,
            allow_browser_control: grant.allow_browser_control,
            reconnect_secret: new_reconnect_secret,
            expires: grant.expires,
            revoked: grant.revoke.subscribe(),
            _connection: ConnectionLease {
                per_view: Arc::clone(&grant.connections),
                total: Arc::clone(&self.total_connections),
            },
        })
    }

    fn subscribe_session(
        self: &Arc<Self>,
        session_id: &str,
        session: Arc<Session>,
    ) -> Result<(Arc<Session>, watch::Receiver<StreamUpdate>)> {
        if session.id != session_id {
            bail!("view_session_mismatch")
        }
        let mut streams = self
            .streams
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(stream) = streams.get(session_id) {
            let _snapshot = stream
                .snapshot_lock
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let current = session.stream_snapshot();
            let previous = match &*stream.sender.borrow() {
                StreamUpdate::Frame(frame) => frame.frame.view_revision,
                StreamUpdate::Error { .. } => 0,
            };
            match current {
                Ok(current) if current.view_revision > previous => match shared_frame(current) {
                    Ok(frame) => match self.remember_frame(session_id, Arc::clone(&frame)) {
                        Ok(()) => {
                            stream.sender.send_replace(StreamUpdate::Frame(frame));
                        }
                        Err(error) => {
                            stream.sender.send_replace(stream_error(&error));
                        }
                    },
                    Err(error) => {
                        stream.sender.send_replace(stream_error(&error));
                    }
                },
                Err(error) => {
                    stream.sender.send_replace(stream_error(&error));
                }
                Ok(_) => {}
            }
            return Ok((Arc::clone(&session), stream.sender.subscribe()));
        }

        let mut changes = session.subscribe_changes();
        let first = match session.stream_snapshot().and_then(shared_frame) {
            Ok(frame) => match self.remember_frame(session_id, Arc::clone(&frame)) {
                Ok(()) => StreamUpdate::Frame(frame),
                Err(error) => stream_error(&error),
            },
            Err(error) => stream_error(&error),
        };
        let first_revision = match &first {
            StreamUpdate::Frame(frame) => frame.frame.view_revision,
            StreamUpdate::Error { .. } => 0,
        };
        let (sender, receiver) = watch::channel(first);
        let (stop, mut stop_receiver) = watch::channel(false);
        let snapshot_lock = Arc::new(Mutex::new(()));
        streams.insert(
            session_id.into(),
            SessionStream {
                sender: sender.clone(),
                snapshot_lock: Arc::clone(&snapshot_lock),
                stop: stop.clone(),
            },
        );
        let inner = Arc::clone(self);
        let session_for_worker = Arc::clone(&session);
        let session_key = session_id.to_string();
        tokio::spawn(async move {
            let mut last_revision = first_revision;
            let mut last_sent = Instant::now();
            loop {
                let changed = tokio::select! {
                    result = changes.changed() => result.is_ok(),
                    stopped = stop_receiver.changed() => stopped.is_ok() && *stop_receiver.borrow(),
                };
                if !changed {
                    break;
                }
                if *stop_receiver.borrow() {
                    break;
                }
                if sender.receiver_count() == 0 {
                    continue;
                }
                let remaining = FRAME_INTERVAL.saturating_sub(last_sent.elapsed());
                if !remaining.is_zero() {
                    tokio::time::sleep(remaining).await;
                }
                if sender.receiver_count() == 0 {
                    continue;
                }
                let _snapshot = snapshot_lock
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                let mut frame = match session_for_worker.stream_snapshot() {
                    Ok(frame) => frame,
                    Err(error) => {
                        sender.send_replace(stream_error(&error));
                        continue;
                    }
                };
                frame.coalesced_revisions = frame
                    .view_revision
                    .saturating_sub(last_revision.saturating_add(1));
                last_revision = frame.view_revision;
                let shared = match shared_frame(frame) {
                    Ok(frame) => frame,
                    Err(error) => {
                        sender.send_replace(stream_error(&error));
                        continue;
                    }
                };
                if let Err(error) = inner.remember_frame(&session_key, Arc::clone(&shared)) {
                    sender.send_replace(stream_error(&error));
                    continue;
                }
                sender.send_replace(StreamUpdate::Frame(shared));
                last_sent = Instant::now();
            }
        });
        Ok((session, receiver))
    }

    fn remember_frame(&self, session_id: &str, frame: Arc<SharedFrame>) -> Result<()> {
        if frame.bytes > MAX_FRAME_BYTES {
            bail!("terminal frame exceeds the 4 MiB stream limit")
        }
        if frame.bytes > MAX_FRAME_BYTES_PER_SESSION {
            bail!("terminal frame exceeds the per-session cache limit")
        }
        let mut cache = self
            .frame_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = Instant::now();
        let entries = cache.entry(session_id.into()).or_default();
        while entries.front().is_some_and(|old| {
            now.saturating_duration_since(old.captured_at) > Duration::from_secs(30)
        }) {
            entries.pop_front();
        }
        while entries.len() >= MAX_FRAME_COUNT_PER_SESSION
            || entries.iter().map(|item| item.bytes).sum::<usize>() + frame.bytes
                > MAX_FRAME_BYTES_PER_SESSION
        {
            entries.pop_front();
        }
        entries.push_back(frame);

        while cache
            .values()
            .flat_map(|items| items.iter())
            .map(|item| item.bytes)
            .sum::<usize>()
            > MAX_FRAME_BYTES_PER_DAEMON
        {
            let oldest_key = cache
                .iter()
                .filter_map(|(key, items)| {
                    items.front().map(|frame| (key.clone(), frame.captured_at))
                })
                .min_by_key(|(_, captured_at)| *captured_at)
                .map(|(key, _)| key);
            let Some(oldest_key) = oldest_key else {
                break;
            };
            if let Some(items) = cache.get_mut(&oldest_key) {
                items.pop_front();
                if items.is_empty() {
                    cache.remove(&oldest_key);
                }
            }
        }
        Ok(())
    }

    fn cached_frame(
        &self,
        session_id: &str,
        run_id: &str,
        frame_id: &str,
        view_revision: u64,
    ) -> Option<Arc<SharedFrame>> {
        let current = self
            .streams
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(session_id)
            .map(|stream| stream.sender.borrow().clone());
        if let Some(StreamUpdate::Frame(current)) = current
            && current.frame.frame.run_id == run_id
            && current.frame.frame.frame_id == frame_id
            && current.frame.view_revision == view_revision
        {
            return Some(current);
        }
        let mut cache = self
            .frame_cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let now = Instant::now();
        let entries = cache.get_mut(session_id)?;
        entries.retain(|entry| {
            now.saturating_duration_since(entry.captured_at) <= Duration::from_secs(30)
        });
        entries
            .iter()
            .find(|entry| {
                entry.frame.frame.run_id == run_id
                    && entry.frame.frame.frame_id == frame_id
                    && entry.frame.view_revision == view_revision
            })
            .cloned()
    }
}

fn shared_frame(frame: StreamFrame) -> Result<Arc<SharedFrame>> {
    let json = serde_json::to_string(&frame)?;
    if json.len() > MAX_FRAME_BYTES {
        bail!("terminal frame exceeds the 4 MiB stream limit")
    }
    Ok(Arc::new(SharedFrame {
        bytes: json.len(),
        json: Arc::from(json),
        frame: Arc::new(frame),
        captured_at: Instant::now(),
    }))
}

fn stream_error(error: &anyhow::Error) -> StreamUpdate {
    let message = error.to_string();
    let code = if message.contains("4 MiB") || message.contains("cache limit") {
        "frame_too_large"
    } else {
        "frame_capture_failed"
    };
    StreamUpdate::Error { code, message }
}

async fn serve_socket(mut socket: WebSocket, inner: Arc<ViewerInner>, view_id: String) {
    let first = match tokio::time::timeout(AUTH_TIMEOUT, socket.recv()).await {
        Ok(Some(Ok(Message::Text(text)))) if text.len() <= MAX_INPUT_BYTES => text,
        _ => {
            let _ = send_json(
                &mut socket,
                json!({"type":"error","code":"authentication_required"}),
            )
            .await;
            let _ = socket.send(Message::Close(None)).await;
            return;
        }
    };
    let ClientMessage::Authenticate { token } =
        serde_json::from_str::<ClientMessage>(first.as_str()).unwrap_or_else(|_| {
            ClientMessage::Authenticate {
                token: String::new(),
            }
        })
    else {
        let _ = send_json(
            &mut socket,
            json!({"type":"error","code":"authentication_required"}),
        )
        .await;
        let _ = socket.send(Message::Close(None)).await;
        return;
    };
    let authenticated = match inner.authenticate(&view_id, &token) {
        Ok(view) => view,
        Err(error) => {
            let _ = send_json(
                &mut socket,
                json!({"type":"error","code":"view_authentication_failed","message":error.to_string()}),
            )
            .await;
            let _ = socket.send(Message::Close(None)).await;
            return;
        }
    };
    let (session, mut frames) = match inner.subscribe_session(
        &authenticated.session_id,
        Arc::clone(&authenticated.session),
    ) {
        Ok(value) => value,
        Err(error) => {
            let _ = send_json(
                &mut socket,
                json!({"type":"error","code":"session_unavailable","message":error.to_string()}),
            )
            .await;
            return;
        }
    };

    if send_json(
        &mut socket,
        json!({
            "type":"authenticated",
            "view_id":authenticated.view_id,
            "session_id":authenticated.session_id,
            "connection_id":authenticated.connection_id,
            "permissions":{"observe":true,"request_control":authenticated.allow_browser_control},
            "reconnect_secret":authenticated.reconnect_secret,
            "reconnect_expires_in_ms":RECONNECT_TTL.as_millis(),
        }),
    )
    .await
    .is_err()
    {
        return;
    }

    let summary = session.summary();
    let (initial_cols, initial_rows) = session.initial_dimensions();
    if send_json(
        &mut socket,
        json!({
            "type":"hello",
            "session":summary,
            "initial_size":{"cols":initial_cols,"rows":initial_rows},
            "execution_mode":"local_process"
        }),
    )
    .await
    .is_err()
    {
        return;
    }
    let initial_frame = frames.borrow().clone();
    if send_stream_update(&mut socket, &initial_frame)
        .await
        .is_err()
    {
        return;
    }

    let mut client = SocketClientState {
        authenticated,
        session,
        authority: None,
        actions: HashMap::new(),
        action_order: VecDeque::new(),
    };
    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    heartbeat.tick().await;

    loop {
        tokio::select! {
            incoming = socket.recv() => {
                let Some(Ok(message)) = incoming else { break };
                match message {
                    Message::Text(text) => {
                        if text.len() > MAX_INPUT_BYTES {
                            let _ = send_json(&mut socket, json!({"type":"error","code":"message_too_large"})).await;
                            break;
                        }
                        let parsed = serde_json::from_str::<ClientMessage>(text.as_str());
                        let Ok(message) = parsed else {
                            let _ = send_json(&mut socket, json!({"type":"error","code":"invalid_message"})).await;
                            continue;
                        };
                        if handle_client_message(&mut socket, &inner, &mut client, message).await.is_err() {
                            break;
                        }
                    }
                    Message::Binary(_) => {
                        let _ = send_json(&mut socket, json!({"type":"error","code":"binary_input_not_supported"})).await;
                    }
                    Message::Ping(payload) => {
                        if socket.send(Message::Pong(payload)).await.is_err() {
                            break;
                        }
                    }
                    Message::Pong(_) => {}
                    Message::Close(_) => break,
                }
            }
            changed = frames.changed() => {
                if changed.is_err() {
                    break;
                }
                let update = frames.borrow_and_update().clone();
                if send_stream_update(&mut socket, &update).await.is_err() {
                    break;
                }
            }
            revoked = client.authenticated.revoked.changed() => {
                if revoked.is_err() || *client.authenticated.revoked.borrow() {
                    let _ = send_json(&mut socket, json!({"type":"grant_revoked"})).await;
                    break;
                }
            }
            _ = tokio::time::sleep_until(tokio::time::Instant::from_std(client.authenticated.expires)) => {
                let _ = send_json(&mut socket, json!({"type":"grant_expired"})).await;
                break;
            }
            _ = heartbeat.tick() => {
                if send_json(&mut socket, json!({"type":"heartbeat"})).await.is_err() {
                    break;
                }
            }
        }
    }

    if let Some(authority) = client.authority {
        let _ = client.session.release_browser_control(&authority);
    }
}

async fn handle_client_message(
    socket: &mut WebSocket,
    inner: &Arc<ViewerInner>,
    client: &mut SocketClientState,
    message: ClientMessage,
) -> Result<()> {
    match message {
        ClientMessage::Logs { source, cursor } => {
            if !matches!(source.as_str(), "app" | "startup") {
                send_json(socket,json!({"type":"logs_error","message":"Browser views may read only this session's app/startup logs"})).await?;
                return Ok(());
            }
            let service = Arc::clone(&inner.sessions);
            let params = json!({"session_id":client.session.id,"source":source,"cursor":cursor,"max_bytes":65536});
            let result = tokio::task::spawn_blocking(move || {
                service.handle(crate::protocol::RpcRequest::Agent {
                    operation: "logs".into(),
                    params,
                })
            })
            .await?;
            match result {
                Ok((data, _)) => send_json(socket, json!({"type":"logs","data":data})).await?,
                Err(error) => {
                    send_json(
                        socket,
                        json!({"type":"logs_error","message":error.to_string()}),
                    )
                    .await?
                }
            }
        }
        ClientMessage::Authenticate { .. } => {
            send_json(
                socket,
                json!({"type":"error","code":"already_authenticated"}),
            )
            .await?;
        }
        ClientMessage::TakeControl => {
            let lease = match client.session.take_browser_control(
                &client.authenticated.view_id,
                &client.authenticated.connection_id,
                client.authenticated.allow_browser_control,
            ) {
                Ok(lease) => lease,
                Err(error) => {
                    send_json(
                        socket,
                        json!({"type":"control_error","code":typed_error_code(&error.to_string()),"message":error.to_string()}),
                    )
                    .await?;
                    return Ok(());
                }
            };
            let current = authority_from_lease(&lease);
            send_json(
                socket,
                json!({"type":"control_changed","owner":"browser","lease":lease}),
            )
            .await?;
            client.authority = Some(current);
        }
        ClientMessage::ReleaseControl {
            lease_id,
            generation,
        } => {
            let Some(current) = client.authority.as_ref() else {
                send_json(
                    socket,
                    json!({"type":"error","code":"control_lease_expired"}),
                )
                .await?;
                return Ok(());
            };
            if current.lease_id != lease_id || current.generation != generation {
                send_json(
                    socket,
                    json!({"type":"error","code":"control_generation_changed"}),
                )
                .await?;
                return Ok(());
            }
            let status = match client.session.release_browser_control(current) {
                Ok(status) => status,
                Err(error) => {
                    send_json(
                        socket,
                        json!({"type":"control_error","code":typed_error_code(&error.to_string()),"message":error.to_string()}),
                    )
                    .await?;
                    return Ok(());
                }
            };
            client.authority = None;
            send_json(socket, json!({"type":"control_changed","status":status})).await?;
        }
        ClientMessage::HeartbeatAck {
            lease_id,
            generation,
        } => {
            if let Some(current) = client.authority.as_ref() {
                if lease_id.as_deref() != Some(current.lease_id.as_str())
                    || generation != Some(current.generation)
                {
                    client.authority = None;
                    send_json(socket, json!({"type":"control_changed","owner":"automation","reason":"generation_changed"})).await?;
                    return Ok(());
                }
                match client.session.renew_browser_control(current) {
                    Ok(status) => {
                        send_json(socket, json!({"type":"control_renewed","status":status}))
                            .await?;
                    }
                    Err(error) => {
                        client.authority = None;
                        send_json(socket, json!({"type":"control_changed","owner":"automation","reason":error.to_string()})).await?;
                    }
                }
            }
        }
        ClientMessage::Input {
            action_id,
            run_id,
            frame_id,
            view_revision,
            viewport_revision,
            lease_id,
            generation,
            input,
        } => {
            if action_id.is_empty() || action_id.len() > 128 {
                send_json(socket, json!({"type":"error","code":"invalid_action_id"})).await?;
                return Ok(());
            }
            let digest = action_digest(
                &run_id,
                &frame_id,
                view_revision,
                viewport_revision,
                &lease_id,
                generation,
                &input,
            )?;
            prune_action_cache(&mut client.actions, &mut client.action_order);
            if let Some(previous) = client.actions.get(&action_id) {
                if previous.digest == digest {
                    send_json(socket, previous.result.clone()).await?;
                } else {
                    send_json(socket, json!({"type":"action_result","action_id":action_id,"accepted":false,"error":"action_id_reused_with_different_payload"})).await?;
                }
                return Ok(());
            }
            let Some(current) = client.authority.as_ref() else {
                let result = json!({"type":"action_result","action_id":action_id,"accepted":false,"error":"control_lease_expired"});
                cache_action(
                    &mut client.actions,
                    &mut client.action_order,
                    action_id,
                    digest,
                    result.clone(),
                );
                send_json(socket, result).await?;
                return Ok(());
            };
            if current.lease_id != lease_id || current.generation != generation {
                let result = json!({"type":"action_result","action_id":action_id,"accepted":false,"error":"control_generation_changed"});
                cache_action(
                    &mut client.actions,
                    &mut client.action_order,
                    action_id,
                    digest,
                    result.clone(),
                );
                send_json(socket, result).await?;
                return Ok(());
            }
            if inner
                .cached_frame(
                    &client.authenticated.session_id,
                    &run_id,
                    &frame_id,
                    view_revision,
                )
                .is_none()
            {
                let result = json!({"type":"action_result","action_id":action_id,"accepted":false,"error":"frame_not_retained","run_id":run_id});
                cache_action(
                    &mut client.actions,
                    &mut client.action_order,
                    action_id,
                    digest,
                    result.clone(),
                );
                send_json(socket, result).await?;
                return Ok(());
            }
            let authority = BrowserAuthority {
                expected_run_id: run_id.clone(),
                ..current.clone()
            };
            let input = match input.into_runtime() {
                Ok(input) => input,
                Err(error) => {
                    let result = json!({"type":"action_result","action_id":action_id,"accepted":false,"error":error.to_string()});
                    cache_action(
                        &mut client.actions,
                        &mut client.action_order,
                        action_id,
                        digest,
                        result.clone(),
                    );
                    send_json(socket, result).await?;
                    return Ok(());
                }
            };
            let result = match client.session.browser_input(
                &authority,
                &BrowserActionContext {
                    action_id: action_id.clone(),
                    frame_id,
                    view_revision,
                    viewport_revision,
                },
                input,
            ) {
                Ok(result) => {
                    json!({"type":"action_result","action_id":action_id,"accepted":true,"result":result,"run_id":run_id})
                }
                Err(error) => {
                    json!({"type":"action_result","action_id":action_id,"accepted":false,"error":error.to_string(),"run_id":client.session.summary().run_id})
                }
            };
            cache_action(
                &mut client.actions,
                &mut client.action_order,
                action_id,
                digest,
                result.clone(),
            );
            send_json(socket, result).await?;
        }
        ClientMessage::Snapshot {
            run_id,
            frame_id,
            view_revision,
        } => {
            let Some(frame) = inner.cached_frame(
                &client.authenticated.session_id,
                &run_id,
                &frame_id,
                view_revision,
            ) else {
                send_json(socket, json!({"type":"snapshot_error","code":"frame_not_retained","run_id":run_id,"frame_id":frame_id,"view_revision":view_revision})).await?;
                return Ok(());
            };
            let artifact = match client.session.screenshot_snapshot(&frame.frame.frame) {
                Ok(artifact) => artifact,
                Err(error) => {
                    send_json(socket, json!({"type":"snapshot_error","code":"screenshot_failed","message":error.to_string()})).await?;
                    return Ok(());
                }
            };
            let bytes = match tokio::fs::read(&artifact.path).await {
                Ok(bytes) => bytes,
                Err(error) => {
                    send_json(socket, json!({"type":"snapshot_error","code":"screenshot_failed","message":error.to_string()})).await?;
                    return Ok(());
                }
            };
            if bytes.len() > MAX_SCREENSHOT_BYTES {
                send_json(
                    socket,
                    json!({"type":"snapshot_error","code":"screenshot_too_large"}),
                )
                .await?;
                return Ok(());
            }
            send_json(
                socket,
                json!({
                    "type":"snapshot_ready",
                    "artifact_id":artifact.artifact_id,
                    "session_id":artifact.session_id,
                    "run_id":artifact.run_id,
                    "frame_id":artifact.frame_id,
                    "view_revision":frame.frame.view_revision,
                    "sha256":artifact.sha256,
                    "cols":artifact.cols,
                    "rows":artifact.rows,
                    "mime_type":"image/png"
                }),
            )
            .await?;
            send_binary(socket, bytes).await?;
        }
    }
    Ok(())
}

async fn favicon() -> StatusCode {
    StatusCode::NO_CONTENT
}

fn typed_error_code(message: &str) -> &str {
    message.split(':').next().unwrap_or("runtime_error")
}

impl BrowserInputMessage {
    fn into_runtime(self) -> Result<BrowserInput> {
        match self {
            Self::Key { key } if !key.is_empty() && key.len() <= 32 => Ok(BrowserInput::Key(key)),
            Self::Key { .. } => bail!("key input must be 1..=32 bytes"),
            Self::Type { text } if text.len() <= 64 * 1024 => Ok(BrowserInput::Type(text)),
            Self::Type { .. } => bail!("type input exceeds the 64 KiB browser limit"),
            Self::Paste { text } if text.len() <= 64 * 1024 => Ok(BrowserInput::Paste(text)),
            Self::Paste { .. } => bail!("paste input exceeds the 64 KiB browser limit"),
            Self::Mouse { input } => Ok(BrowserInput::Mouse(input)),
            Self::Resize { cols, rows }
                if (1..=300).contains(&cols) && (1..=120).contains(&rows) =>
            {
                Ok(BrowserInput::Resize { cols, rows })
            }
            Self::Resize { .. } => bail!("resize must be between 1x1 and 300x120 cells"),
        }
    }
}

fn authority_from_lease(lease: &BrowserLease) -> BrowserAuthority {
    BrowserAuthority {
        view_id: lease.view_id.clone(),
        connection_id: lease.connection_id.clone(),
        lease_id: lease.lease_id.clone(),
        generation: lease.generation,
        expected_run_id: lease.run_id.clone(),
    }
}

fn action_digest(
    run_id: &str,
    frame_id: &str,
    view_revision: u64,
    viewport_revision: u64,
    lease_id: &str,
    generation: u64,
    input: &BrowserInputMessage,
) -> Result<[u8; 32]> {
    let payload = serde_json::to_vec(&(
        run_id,
        frame_id,
        view_revision,
        viewport_revision,
        lease_id,
        generation,
        input,
    ))?;
    Ok(Sha256::digest(payload).into())
}

fn prune_action_cache(cache: &mut HashMap<String, CachedAction>, order: &mut VecDeque<String>) {
    let now = Instant::now();
    order.retain(|id| {
        cache.get(id).is_some_and(|entry| {
            now.saturating_duration_since(entry.stored_at) <= MAX_ACTION_CACHE_AGE
        })
    });
    cache.retain(|_, entry| now.saturating_duration_since(entry.stored_at) <= MAX_ACTION_CACHE_AGE);
}

fn cache_action(
    cache: &mut HashMap<String, CachedAction>,
    order: &mut VecDeque<String>,
    id: String,
    digest: [u8; 32],
    result: Value,
) {
    while cache.len() >= MAX_ACTION_RESULTS {
        if let Some(oldest) = order.pop_front() {
            cache.remove(&oldest);
        } else {
            break;
        }
    }
    order.push_back(id.clone());
    cache.insert(
        id,
        CachedAction {
            digest,
            result,
            stored_at: Instant::now(),
        },
    );
}

async fn send_json(socket: &mut WebSocket, value: Value) -> Result<()> {
    let text = serde_json::to_string(&value)?;
    tokio::time::timeout(SEND_TIMEOUT, socket.send(Message::Text(text.into())))
        .await
        .context("timed out sending a viewer message")??;
    Ok(())
}

async fn send_binary(socket: &mut WebSocket, bytes: Vec<u8>) -> Result<()> {
    tokio::time::timeout(SEND_TIMEOUT, socket.send(Message::Binary(bytes.into())))
        .await
        .context("timed out sending a viewer screenshot")??;
    Ok(())
}

async fn send_stream_frame(socket: &mut WebSocket, frame: &SharedFrame) -> Result<()> {
    let message = format!("{{\"type\":\"frame\",\"data\":{}}}", frame.json);
    tokio::time::timeout(SEND_TIMEOUT, socket.send(Message::Text(message.into())))
        .await
        .context("timed out streaming a terminal frame")??;
    Ok(())
}

async fn send_stream_update(socket: &mut WebSocket, update: &StreamUpdate) -> Result<()> {
    match update {
        StreamUpdate::Frame(frame) => send_stream_frame(socket, frame).await,
        StreamUpdate::Error { code, message } => {
            send_json(
                socket,
                json!({"type":"stream_error","code":code,"message":message}),
            )
            .await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{LaunchSpec, RpcRequest};
    use std::fs;

    #[tokio::test]
    async fn expired_bootstrap_and_reconnect_credentials_are_rejected() {
        let temp = tempfile::tempdir().unwrap();
        let runtime = temp.path().join("runtime");
        let cwd = temp.path().join("work");
        fs::create_dir_all(&cwd).unwrap();
        let sessions = Arc::new(SessionService::new(&runtime).unwrap());
        let launched = sessions
            .handle(RpcRequest::Launch {
                spec: LaunchSpec {
                    name: Some("viewer-expiry-test".into()),
                    program: "/bin/sleep".into(),
                    args: vec!["60".into()],
                    cwd: cwd.display().to_string(),
                    env: Default::default(),
                    cols: 80,
                    rows: 24,
                    record_output: false,
                },
            })
            .unwrap()
            .0;
        let session_id = launched["session_id"].as_str().unwrap().to_owned();
        let viewer = ViewerServer::new(Arc::clone(&sessions));

        let bootstrap = viewer
            .handle_request(RpcRequest::ViewStart {
                session_id: session_id.clone(),
                allow_browser_control: false,
                idempotency_key: None,
            })
            .await
            .unwrap();
        let bootstrap_id = bootstrap["view_id"].as_str().unwrap().to_owned();
        let bootstrap_token = bootstrap["url"]
            .as_str()
            .unwrap()
            .split_once("#token=")
            .unwrap()
            .1
            .to_owned();
        {
            let mut store = viewer
                .inner
                .grants
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            store
                .grants
                .get_mut(&bootstrap_id)
                .unwrap()
                .bootstrap_expires = Instant::now() - Duration::from_secs(1);
        }
        let bootstrap_error = viewer
            .inner
            .authenticate(&bootstrap_id, &bootstrap_token)
            .err()
            .expect("an expired bootstrap credential must be rejected");
        assert!(
            bootstrap_error
                .to_string()
                .contains("view_authentication_failed")
        );

        let reconnect = viewer
            .handle_request(RpcRequest::ViewStart {
                session_id: session_id.clone(),
                allow_browser_control: false,
                idempotency_key: None,
            })
            .await
            .unwrap();
        let reconnect_id = reconnect["view_id"].as_str().unwrap().to_owned();
        let reconnect_token = reconnect["url"]
            .as_str()
            .unwrap()
            .split_once("#token=")
            .unwrap()
            .1
            .to_owned();
        let authenticated = viewer
            .inner
            .authenticate(&reconnect_id, &reconnect_token)
            .unwrap();
        let reconnect_secret = authenticated.reconnect_secret.clone().unwrap();
        drop(authenticated);
        {
            let mut store = viewer
                .inner
                .grants
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            store
                .grants
                .get_mut(&reconnect_id)
                .unwrap()
                .reconnect_expires = Some(Instant::now() - Duration::from_secs(1));
        }
        let reconnect_error = viewer
            .inner
            .authenticate(&reconnect_id, &reconnect_secret)
            .err()
            .expect("an expired reconnect credential must be rejected");
        assert!(
            reconnect_error
                .to_string()
                .contains("view_authentication_failed")
        );

        viewer.shutdown().await;
        sessions.handle(RpcRequest::Close { session_id }).unwrap();
        sessions.shutdown().unwrap();
    }

    #[tokio::test]
    async fn viewer_connection_limits_are_enforced_per_view_and_per_session() {
        let temp = tempfile::tempdir().unwrap();
        let runtime = temp.path().join("runtime");
        let cwd = temp.path().join("work");
        fs::create_dir_all(&cwd).unwrap();
        let sessions = Arc::new(SessionService::new(&runtime).unwrap());
        let launched = sessions
            .handle(RpcRequest::Launch {
                spec: LaunchSpec {
                    name: Some("viewer-connection-limit-test".into()),
                    program: "/bin/sleep".into(),
                    args: vec!["60".into()],
                    cwd: cwd.display().to_string(),
                    env: Default::default(),
                    cols: 80,
                    rows: 24,
                    record_output: false,
                },
            })
            .unwrap()
            .0;
        let session_id = launched["session_id"].as_str().unwrap().to_owned();
        let viewer = ViewerServer::new(Arc::clone(&sessions));
        let mut active = Vec::new();
        let mut first_view = None;
        let mut first_reconnect = None;

        for index in 0..=MAX_CONNECTIONS_PER_SESSION {
            let grant = viewer
                .handle_request(RpcRequest::ViewStart {
                    session_id: session_id.clone(),
                    allow_browser_control: false,
                    idempotency_key: Some(format!("connection-limit-{index}")),
                })
                .await
                .unwrap();
            let view_id = grant["view_id"].as_str().unwrap().to_owned();
            let token = grant["url"]
                .as_str()
                .unwrap()
                .split_once("#token=")
                .unwrap()
                .1
                .to_owned();

            if index < MAX_CONNECTIONS_PER_SESSION {
                let authenticated = viewer.inner.authenticate(&view_id, &token).unwrap();
                if index == 0 {
                    first_view = Some(view_id.clone());
                    first_reconnect = authenticated.reconnect_secret.clone();
                }
                active.push(authenticated);
            } else {
                let error = viewer
                    .inner
                    .authenticate(&view_id, &token)
                    .err()
                    .expect("a viewer over the per-session connection limit must be rejected");
                assert!(error.to_string().contains("viewer_connection_limit"));
            }
        }

        let same_view_error = viewer
            .inner
            .authenticate(
                first_view.as_deref().unwrap(),
                first_reconnect.as_deref().unwrap(),
            )
            .err()
            .expect("a second connection to one view grant must be rejected");
        assert!(
            same_view_error
                .to_string()
                .contains("viewer_connection_limit")
        );

        drop(active);
        viewer.shutdown().await;
        sessions.handle(RpcRequest::Close { session_id }).unwrap();
        sessions.shutdown().unwrap();
    }
}
