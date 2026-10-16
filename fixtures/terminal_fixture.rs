use std::fs;
use std::io::{Read, Write};
use std::process::{Child, Command};

#[derive(Default)]
struct UiState {
    selected: usize,
    typed: String,
    pasted: String,
    mouse: String,
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
    let _raw = RawMode::enable(libc::STDIN_FILENO)?;
    let mut state = UiState::default();
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
        if !stdin_ready(100)? {
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
        state.typed.push_str(
            &text
                .chars()
                .filter(|ch| !ch.is_control())
                .collect::<String>(),
        );
        input.drain(..text_end);
    }
    Ok(false)
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
