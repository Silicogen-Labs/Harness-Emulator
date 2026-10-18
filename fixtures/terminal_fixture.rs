use std::fs;
use std::io::{Read, Write};
use std::process::{Child, Command};
use std::time::{Duration, Instant};

#[derive(Default)]
struct UiState {
    selected: usize,
    typed: String,
    pasted: String,
    mouse: String,
    virtual_page: usize,
    virtual_scroll: bool,
    transient_toast: bool,
    exit_on_bang: bool,
    toast: Option<String>,
    toast_due: Option<Instant>,
    toast_replaced: bool,
}

struct RawMode {
    fd: libc::c_int,
    old: libc::termios,
}

impl RawMode {
    fn enable(fd: libc::c_int) -> std::io::Result<Self> {
        let mut old: libc::termios = unsafe { std::mem::zeroed() };
        if unsafe { libc::tcgetattr(fd, &mut old) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        let mut raw = old;
        unsafe { libc::cfmakeraw(&mut raw) };
        if unsafe { libc::tcsetattr(fd, libc::TCSANOW, &raw) } != 0 {
            return Err(std::io::Error::last_os_error());
        }
        Ok(Self { fd, old })
    }
}

impl Drop for RawMode {
    fn drop(&mut self) {
        unsafe {
            libc::tcsetattr(self.fd, libc::TCSANOW, &self.old);
        }
    }
}

fn main() -> std::io::Result<()> {
    if std::env::args().any(|arg| arg == "--adapter-http") {
        return run_adapter_http_fixture();
    }
    let _raw = RawMode::enable(libc::STDIN_FILENO)?;
    if std::env::args().any(|arg| arg == "--delayed-palette") {
        return run_delayed_palette_fixture();
    }
    let mut state = UiState {
        virtual_scroll: std::env::args().any(|arg| arg == "--virtual-transcript"),
        transient_toast: std::env::args().any(|arg| arg == "--transient-toast"),
        exit_on_bang: std::env::args().any(|arg| arg == "--exit-on-bang"),
        ..UiState::default()
    };
    let mut input = Vec::new();
    let mut children = Vec::<Child>::new();
    if std::env::args().any(|arg| arg == "--spawn-child")
        && let Ok(child) = Command::new("sleep").arg("600").spawn()
    {
        let _ = fs::write("fixture-child.pid", child.id().to_string());
        children.push(child);
    }
    let no_mouse = std::env::args().any(|arg| arg == "--no-mouse");
    let initial_modes: &[u8] = if no_mouse {
        b"\x1b[?2004h\x1b[?1004h\x1b[>1u\x1b[?25h"
    } else {
        b"\x1b[?1003h\x1b[?1006h\x1b[?2004h\x1b[?1004h\x1b[>1u\x1b[?25h"
    };
    write_all(initial_modes)?;
    render(&state)?;
    if std::env::args().any(|arg| arg == "--exit-after-ready") {
        write_all(b"\r\nEXITING\r\n")?;
        return Ok(());
    }

    let mut reader = std::io::stdin();
    let mut chunk = [0_u8; 4096];
    let mut last_size = terminal_size();
    loop {
        if !stdin_ready(20)? {
            if advance_transient_toast(&mut state) {
                render(&state)?;
            }
            let current_size = terminal_size();
            if current_size != last_size {
                last_size = current_size;
                render(&state)?;
            }
            continue;
        }
        let count = match reader.read(&mut chunk) {
            Ok(0) => break,
            Ok(count) => count,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        input.extend_from_slice(&chunk[..count]);
        if consume_input(&mut input, &mut state)? {
            write_all(b"\x1b[?1003l\x1b[?1006l\x1b[?2004l\r\nBYE\r\n")?;
            for child in &mut children {
                let _ = child.kill();
            }
            return Ok(());
        }
        render(&state)?;
    }
    Ok(())
}

fn run_adapter_http_fixture() -> std::io::Result<()> {
    let port = std::env::args()
        .collect::<Vec<_>>()
        .windows(2)
        .find(|pair| pair[0] == "--port")
        .and_then(|pair| pair[1].parse::<u16>().ok())
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "--port required"))?;
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        use axum::{Json, Router, extract::{Path, Query}, routing::get};
        use axum::http::StatusCode;
        use axum::response::IntoResponse;
        use std::collections::BTreeMap;
        async fn session(Path(id): Path<String>, Query(query): Query<BTreeMap<String, String>>) -> Json<serde_json::Value> {
            Json(serde_json::json!({"id":id,"directory":query.get("directory"),"title":"Owned adapter fixture","revert":null,"recap":null}))
        }
        async fn messages(Path(_id): Path<String>, Query(_query): Query<BTreeMap<String, String>>) -> axum::response::Response {
            match read_workspace_json("emulation-messages.json", 64 * 1024) {
                Ok(Some(value)) if value.is_array() => Json(value).into_response(),
                Ok(Some(_)) => StatusCode::BAD_REQUEST.into_response(),
                Ok(None) => Json(Vec::<serde_json::Value>::new()).into_response(),
                Err(_) => StatusCode::BAD_REQUEST.into_response(),
            }
        }
        async fn ui_state(Query(_query): Query<BTreeMap<String, String>>) -> axum::response::Response {
            match read_workspace_json("emulation-ui-state.json", 64 * 1024) {
                Ok(Some(value)) if value.is_object() => Json(value).into_response(),
                Ok(Some(_)) => StatusCode::BAD_REQUEST.into_response(),
                Ok(None) => StatusCode::NOT_FOUND.into_response(),
                Err(_) => StatusCode::BAD_REQUEST.into_response(),
            }
        }
        async fn status() -> Json<serde_json::Value> { Json(serde_json::json!({"fixture-session":{"type":"idle"}})) }
        async fn events() -> axum::response::Response {
            let body = concat!(
                "data: {\"type\":\"session.status\",\"properties\":{\"sessionID\":\"fixture-session\",\"status\":{\"type\":\"busy\"}}}\n\n",
                "data: {\"type\":\"session.status\",\"properties\":{\"sessionID\":\"unlisted-session\",\"status\":{\"type\":\"busy\"}}}\n\n"
            );
            axum::response::Response::builder().header("content-type", "text/event-stream").body(axum::body::Body::from(body)).unwrap()
        }
        let app = Router::new()
            .route("/session/{id}", get(session))
            .route("/session/{id}/message", get(messages))
            .route("/session/status", get(status))
            .route("/event", get(events))
            .route("/__emulation/ui-state", get(ui_state));
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", port)).await?;
        axum::serve(listener, app).await
    })
}

fn read_workspace_json(name: &str, max_bytes: usize) -> std::io::Result<Option<serde_json::Value>> {
    let path = std::env::current_dir()?.join(name);
    let file = match fs::File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error),
    };
    if file.metadata()?.len() > max_bytes as u64 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "fixture JSON exceeds its byte limit",
        ));
    }
    let mut bytes = Vec::new();
    Read::take(file, max_bytes as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > max_bytes {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "fixture JSON exceeds its byte limit",
        ));
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
}

#[derive(Default)]
struct DelayedPaletteState {
    palette_visible: bool,
    query_visible: bool,
    confirmed: bool,
    filter: String,
    ctrl_p_count: usize,
    type_action_count: usize,
    enter_count: usize,
    order: Vec<&'static str>,
    palette_due: Option<Instant>,
    query_due: Option<Instant>,
}

fn run_delayed_palette_fixture() -> std::io::Result<()> {
    let mut state = DelayedPaletteState::default();
    let mut reader = std::io::stdin();
    let mut chunk = [0_u8; 256];
    render_delayed_palette(&state)?;
    loop {
        let changed_by_timer = advance_delayed_palette(&mut state);
        if changed_by_timer {
            render_delayed_palette(&state)?;
        }
        if !stdin_ready(10)? {
            continue;
        }
        let count = match reader.read(&mut chunk) {
            Ok(0) => break,
            Ok(count) => count,
            Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(error) => return Err(error),
        };
        let mut changed = false;
        for byte in &chunk[..count] {
            match *byte {
                0x10 if state.ctrl_p_count == 0 => {
                    state.ctrl_p_count += 1;
                    state.order.push("CTRL_P");
                    state.palette_due = Some(Instant::now() + Duration::from_millis(140));
                    changed = true;
                }
                b'\r' | b'\n' => {
                    state.enter_count += 1;
                    state.order.push("ENTER");
                    state.confirmed = state.query_visible && state.filter == "sidebar";
                    changed = true;
                }
                byte if state.palette_visible && byte.is_ascii_graphic() => {
                    if state.filter.is_empty() {
                        state.type_action_count += 1;
                        state.order.push("TYPE");
                    }
                    state.filter.push(byte as char);
                    state.query_visible = false;
                    state.query_due = Some(Instant::now() + Duration::from_millis(160));
                    changed = true;
                }
                _ => {}
            }
        }
        if changed {
            render_delayed_palette(&state)?;
        }
    }
    Ok(())
}

fn advance_delayed_palette(state: &mut DelayedPaletteState) -> bool {
    let now = Instant::now();
    let mut changed = false;
    if state.palette_due.is_some_and(|due| now >= due) {
        state.palette_due = None;
        state.palette_visible = true;
        changed = true;
    }
    if state.query_due.is_some_and(|due| now >= due) {
        state.query_due = None;
        state.query_visible = true;
        changed = true;
    }
    changed
}

fn render_delayed_palette(state: &DelayedPaletteState) -> std::io::Result<()> {
    let mut text = String::from("\x1b[?25h\x1b[2J\x1b[HDELAYED PALETTE FIXTURE\r\n");
    text.push_str("FIXTURE READY\r\n");
    if state.palette_visible {
        text.push_str("PALETTE SEARCH READY\r\n");
        text.push_str(&format!("FILTER: {}\r\n", state.filter));
    }
    if state.query_visible {
        text.push_str("RESULT: SIDEBAR PANEL\r\nHIGHLIGHTED: SIDEBAR PANEL\r\n");
    }
    if state.confirmed {
        text.push_str("CONFIRMED: SIDEBAR PANEL\r\n");
    }
    text.push_str(&format!(
        "ACTIONS: CTRL_P={} TYPE={} ENTER={}\r\nORDER: {}\r\n",
        state.ctrl_p_count,
        state.type_action_count,
        state.enter_count,
        state.order.join(",")
    ));
    write_all(text.as_bytes())
}

fn consume_input(input: &mut Vec<u8>, state: &mut UiState) -> std::io::Result<bool> {
    loop {
        if input.is_empty() {
            break;
        }
        if input.starts_with(b"\x1b[200~") {
            let Some(end) = find_subslice(input, b"\x1b[201~", 6) else {
                break;
            };
            state.pasted = String::from_utf8_lossy(&input[6..end]).to_string();
            input.drain(..end + 6);
            continue;
        }
        if input.starts_with(b"\x1b[<") {
            let Some(end) = input
                .iter()
                .enumerate()
                .skip(3)
                .find(|(_, byte)| **byte == b'M' || **byte == b'm')
                .map(|(index, _)| index)
            else {
                break;
            };
            let raw = String::from_utf8_lossy(&input[..=end]);
            state.mouse = parse_mouse(&raw);
            if state.virtual_scroll {
                if raw.starts_with("\x1b[<64;") {
                    state.virtual_page = state.virtual_page.saturating_sub(1);
                }
                if raw.starts_with("\x1b[<65;") {
                    state.virtual_page = (state.virtual_page + 1).min(2);
                }
            }
            input.drain(..=end);
            continue;
        }
        if input.first() == Some(&0x03) {
            input.drain(..1);
            return Ok(true);
        }
        if input.first() == Some(&0x1b) {
            if input.len() < 2 {
                break;
            }
            if input[1] == b']' {
                let end = input.iter().position(|byte| *byte == 0x07);
                if let Some(end) = end {
                    input.drain(..=end);
                    continue;
                }
                break;
            }
            if input[1] == b'[' || input[1] == b'O' {
                let Some((end, final_byte)) = csi_final(input) else {
                    break;
                };
                match final_byte {
                    b'A' => state.selected = state.selected.saturating_sub(1),
                    b'B' => state.selected = (state.selected + 1).min(1),
                    _ => {}
                }
                input.drain(..=end);
                continue;
            }
            input.drain(..2);
            continue;
        }

        let next_escape = input
            .iter()
            .position(|byte| *byte == 0x1b)
            .unwrap_or(input.len());
        let text_end = if next_escape == 0 { 1 } else { next_escape };
        let text = String::from_utf8_lossy(&input[..text_end]);
        if state.exit_on_bang && text.contains('!') {
            return Ok(true);
        }
        state.typed.push_str(
            &text
                .chars()
                .filter(|ch| !ch.is_control())
                .collect::<String>(),
        );
        if state.transient_toast && text.contains('!') {
            state.toast = Some("SHORT TOAST".into());
            state.toast_due = Some(Instant::now() + Duration::from_millis(180));
            state.toast_replaced = false;
        }
        input.drain(..text_end);
    }
    Ok(false)
}

fn advance_transient_toast(state: &mut UiState) -> bool {
    if !state.toast_due.is_some_and(|due| Instant::now() >= due) {
        return false;
    }
    if state.toast_replaced {
        state.toast = None;
        state.toast_due = None;
    } else {
        state.toast = Some("TOAST REPLACED".into());
        state.toast_due = Some(Instant::now() + Duration::from_millis(180));
        state.toast_replaced = true;
    }
    true
}

fn stdin_ready(timeout_ms: i32) -> std::io::Result<bool> {
    let mut descriptor = libc::pollfd {
        fd: libc::STDIN_FILENO,
        events: libc::POLLIN,
        revents: 0,
    };
    let result = unsafe { libc::poll(&mut descriptor, 1, timeout_ms) };
    if result < 0 {
        let error = std::io::Error::last_os_error();
        if error.kind() == std::io::ErrorKind::Interrupted {
            return Ok(false);
        }
        return Err(error);
    }
    Ok(result > 0)
}

fn csi_final(input: &[u8]) -> Option<(usize, u8)> {
    if input.len() < 3 {
        return None;
    }
    input
        .iter()
        .enumerate()
        .skip(2)
        .find(|(_, byte)| (0x40..=0x7e).contains(*byte))
        .map(|(index, byte)| (index, *byte))
}

fn find_subslice(haystack: &[u8], needle: &[u8], from: usize) -> Option<usize> {
    haystack
        .get(from..)?
        .windows(needle.len())
        .position(|window| window == needle)
        .map(|offset| offset + from)
}

fn parse_mouse(sequence: &str) -> String {
    let Some(body) = sequence.strip_prefix("\x1b[<") else {
        return sequence.to_string();
    };
    let body = body.trim_end_matches(['M', 'm']);
    let parts = body.split(';').collect::<Vec<_>>();
    if parts.len() != 3 {
        return sequence.to_string();
    }
    let code = parts[0].parse::<u16>().unwrap_or_default();
    let col = parts[1].parse::<u16>().unwrap_or(1).saturating_sub(1);
    let row = parts[2].parse::<u16>().unwrap_or(1).saturating_sub(1);
    format!("code={code} row={row} col={col}")
}

fn render(state: &UiState) -> std::io::Result<()> {
    let size = terminal_size();
    let mut text = String::new();
    text.push_str("\x1b[?25h\x1b[2J\x1b[H");
    text.push_str("\x1b[44;97m TERMINAL AUTOMATION FIXTURE \x1b[0m\r\n");
    if state.virtual_scroll {
        let page = match state.virtual_page {
            0 => "newest messages",
            1 => "middle messages",
            _ => "older catch-up recap",
        };
        text.push_str(&format!(
            "Application transcript page {}: {page}\r\n",
            state.virtual_page
        ));
        return write_all(text.as_bytes());
    }
    text.push_str("Use arrows to change selection; text and paste are echoed.\r\n");
    text.push_str(if state.selected == 0 {
        "\x1b[42;30m  Alpha option  \x1b[0m\r\n"
    } else {
        "  Alpha option  \r\n"
    });
    text.push_str(if state.selected == 1 {
        "\x1b[42;30m  Beta option   \x1b[0m\r\n"
    } else {
        "  Beta option   \r\n"
    });
    text.push_str(&format!(
        "\r\nSelection: {}\r\n",
        if state.selected == 0 { "Alpha" } else { "Beta" }
    ));
    text.push_str(&format!("Typed: {}\r\n", state.typed));
    text.push_str(&format!("Paste: {}\r\n", state.pasted));
    text.push_str(&format!("Mouse: {}\r\n", state.mouse));
    if let Some(toast) = &state.toast {
        text.push_str(&format!("NOTICE: {toast}\r\n"));
    }
    text.push_str(&format!("Size: {}x{}\r\n", size.0, size.1));
    text.push_str("READY\r\n");
    write_all(text.as_bytes())
}

fn terminal_size() -> (u16, u16) {
    let mut size: libc::winsize = unsafe { std::mem::zeroed() };
    if unsafe { libc::ioctl(libc::STDOUT_FILENO, libc::TIOCGWINSZ, &mut size) } == 0 {
        (size.ws_col.max(1), size.ws_row.max(1))
    } else {
        (80, 24)
    }
}

fn write_all(bytes: &[u8]) -> std::io::Result<()> {
    let mut output = std::io::stdout().lock();
    output.write_all(bytes)?;
    output.flush()
}

#[cfg(test)]
mod delayed_palette_tests {
    use super::*;

    #[test]
    fn palette_and_query_visibility_are_delayed_until_their_observable_barriers() {
        let mut state = DelayedPaletteState {
            palette_due: Some(Instant::now() + Duration::from_millis(20)),
            ..DelayedPaletteState::default()
        };
        assert!(!advance_delayed_palette(&mut state));
        std::thread::sleep(Duration::from_millis(25));
        assert!(advance_delayed_palette(&mut state));
        assert!(state.palette_visible);
        state.filter = "sidebar".into();
        state.query_due = Some(Instant::now() + Duration::from_millis(20));
        assert!(!advance_delayed_palette(&mut state));
        std::thread::sleep(Duration::from_millis(25));
        assert!(advance_delayed_palette(&mut state));
        assert!(state.query_visible);
    }
}
