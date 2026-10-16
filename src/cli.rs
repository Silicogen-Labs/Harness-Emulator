use anyhow::{Context, Result, bail};
use clap::{Args, Parser, Subcommand, ValueEnum};
use silicogen_terminal_runtime::client::RuntimeClient;
use silicogen_terminal_runtime::daemon;
use silicogen_terminal_runtime::protocol::{
    LaunchSpec, MouseButton, MouseInput, RpcRequest, WaitPredicate, WheelDirection,
};
use std::ffi::OsString;
use std::path::PathBuf;

#[derive(Debug, Parser)]
#[command(
    name = "silicogen-emulation",
    version,
    about = "Linux PTY automation runtime"
)]
pub struct Cli {
    #[arg(long, global = true)]
    runtime_dir: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Agent operation with validated JSON arguments (profiles/logs/wait/scenarios/cleanup).
    Agent {
        operation: String,
        #[arg(long, default_value = "{}")]
        json: String,
    },
    /// Run the persistent local session daemon in the foreground.
    Daemon,
    /// Launch an executable with an explicit argument vector.
    Launch(LaunchArgs),
    /// List sessions.
    List,
    /// Read one session's lifecycle state.
    Status { session_id: String },
    /// Capture text, styled cells, terminal modes, and frame identity.
    Observe { session_id: String },
    /// Read a bounded styled-cell rectangle from one frame.
    Cells {
        session_id: String,
        row: u16,
        col: u16,
        rows: u16,
        cols: u16,
    },
    /// Capture a PNG for the current frame.
    Screenshot { session_id: String },
    /// Send one logical key token, e.g. Down, Enter, Ctrl+C.
    Key { session_id: String, key: String },
    /// Type literal text.
    Type { session_id: String, text: String },
    /// Paste literal text, using bracketed paste when negotiated.
    Paste { session_id: String, text: String },
    /// Send a mouse event in zero-based terminal-cell coordinates.
    Mouse(MouseArgs),
    /// Resize the PTY and terminal engine together.
    Resize {
        session_id: String,
        cols: u16,
        rows: u16,
    },
    /// Wait for an observable condition with a required timeout.
    Wait(WaitArgs),
    /// Return bounded session events/recording metadata.
    History { session_id: String },
    /// Restart the application from its original launch specification.
    Restart { session_id: String },
    /// Close the application process group and release its PTY.
    Close { session_id: String },
    /// Create a local browser view of the same terminal session.
    ViewStart {
        session_id: String,
        #[arg(long)]
        allow_browser_control: bool,
        #[arg(long)]
        idempotency_key: Option<String>,
    },
    /// Inspect a browser view without returning its capability URL.
    ViewStatus { view_id: String },
    /// Revoke a browser view while leaving its application session running.
    ViewStop { view_id: String },
    /// Read the current automation/browser input owner.
    ControlStatus { session_id: String },
    /// Release a browser owner only if the observed generation still matches.
    ControlRelease {
        session_id: String,
        expected_generation: u64,
    },
    /// Stop the daemon and close its sessions.
    Shutdown,
    /// Run an MCP stdio server backed by this daemon.
    Mcp,
}

#[derive(Debug, Args)]
struct LaunchArgs {
    #[arg(long)]
    name: Option<String>,
    #[arg(long)]
    cwd: Option<PathBuf>,
    /// Set an environment variable for the launched program. May be repeated as KEY=VALUE.
    #[arg(long = "env", value_name = "KEY=VALUE")]
    env: Vec<String>,
    #[arg(long, default_value_t = 120)]
    cols: u16,
    #[arg(long, default_value_t = 40)]
    rows: u16,
    /// Retain a bounded raw terminal event stream. Screen text may contain secrets.
    #[arg(long)]
    record_output: bool,
    #[arg(long)]
    profile: Option<String>,
    #[arg(long)]
    state_mode: Option<String>,
    #[arg(long, value_delimiter = ',')]
    env_inherit: Option<Vec<String>>,
    #[arg(long)]
    env_unset: Vec<String>,
    #[arg(long, default_value = "{}")]
    options_json: String,
    #[arg(long)]
    workspace: Option<PathBuf>,
    #[arg(long)]
    isolation: Option<String>,
    #[arg(last = true, num_args = 1..)]
    command: Vec<OsString>,
}

#[derive(Debug, Args)]
struct MouseArgs {
    session_id: String,
    #[command(subcommand)]
    action: MouseCommand,
}

#[derive(Debug, Subcommand)]
enum MouseCommand {
    Move {
        row: u16,
        col: u16,
    },
    Click {
        row: u16,
        col: u16,
        #[arg(long, value_enum, default_value_t = MouseButtonArg::Left)]
        button: MouseButtonArg,
    },
    Down {
        row: u16,
        col: u16,
        #[arg(long, value_enum, default_value_t = MouseButtonArg::Left)]
        button: MouseButtonArg,
    },
    Up {
        row: u16,
        col: u16,
    },
    Wheel {
        row: u16,
        col: u16,
        #[arg(value_enum)]
        direction: WheelDirectionArg,
        #[arg(long, default_value_t = 1)]
        steps: u16,
    },
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum MouseButtonArg {
    Left,
    Middle,
    Right,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum WheelDirectionArg {
    Up,
    Down,
}

#[derive(Debug, Args)]
struct WaitArgs {
    session_id: String,
    #[arg(long)]
    contains: Option<String>,
    #[arg(long)]
    absent: Option<String>,
    #[arg(long)]
    regex: Option<String>,
    #[arg(long)]
    stable_ms: Option<u64>,
    #[arg(long)]
    process_exited: bool,
    #[arg(long, default_value_t = 30_000)]
    timeout_ms: u64,
}

pub async fn run() -> Result<()> {
    let cli = Cli::parse();
    let runtime_dir = cli.runtime_dir.unwrap_or_else(default_runtime_dir);
    match cli.command {
        Command::Daemon => daemon::run(runtime_dir).await,
        Command::Mcp => crate::mcp::run(runtime_dir).await,
        command => {
            let client = RuntimeClient::new(runtime_dir);
            let request = match command {
                Command::Agent { operation, json } => RpcRequest::Agent {
                    operation,
                    params: serde_json::from_str(&json).context("invalid agent parameters JSON")?,
                },
                Command::Launch(launch) => {
                    let cwd = launch
                        .cwd
                        .map(|p| p.to_string_lossy().into_owned())
                        .unwrap_or(std::env::current_dir()?.to_string_lossy().into_owned());
                    let mut options: serde_json::Value =
                        serde_json::from_str(&launch.options_json)?;
                    if let Some(profile) = launch.profile {
                        options["profile"] = profile.into();
                        options["state_mode"] = "profile".into();
                    }
                    if let Some(mode) = launch.state_mode {
                        options["state_mode"] = mode.into();
                    }
                    if let Some(names) = launch.env_inherit {
                        options["env_inherit"] = serde_json::json!(names);
                    }
                    if !launch.env_unset.is_empty() {
                        options["env_unset"] = serde_json::json!(launch.env_unset);
                    }
                    if let Some(workspace) = launch.workspace {
                        options["workspace"] =
                            serde_json::json!({"source":workspace,"mode":"copy"});
                    }
                    if let Some(backend) = launch.isolation {
                        options["isolation"] = serde_json::json!({"backend":backend});
                    }
                    let options = serde_json::from_value(options)?;
                    let mut command = launch.command.into_iter();
                    let program = command
                        .next()
                        .map(|s| into_utf8(s, "program"))
                        .transpose()?
                        .unwrap_or_default();
                    let args = command
                        .map(|arg| into_utf8(arg, "argument"))
                        .collect::<Result<Vec<_>>>()?;
                    let env = parse_env(launch.env)?;
                    RpcRequest::LaunchConfigured {
                        spec: LaunchSpec {
                            name: launch.name,
                            program,
                            args,
                            cwd,
                            env,
                            cols: launch.cols,
                            rows: launch.rows,
                            record_output: launch.record_output,
                        },
                        options,
                    }
                }
                Command::List => RpcRequest::List,
                Command::Status { session_id } => RpcRequest::Status { session_id },
                Command::Observe { session_id } => RpcRequest::Observe { session_id },
                Command::Cells {
                    session_id,
                    row,
                    col,
                    rows,
                    cols,
                } => RpcRequest::Cells {
                    session_id,
                    row,
                    col,
                    rows,
                    cols,
                },
                Command::Screenshot { session_id } => RpcRequest::Screenshot { session_id },
                Command::Key { session_id, key } => RpcRequest::Key { session_id, key },
                Command::Type { session_id, text } => RpcRequest::Type { session_id, text },
                Command::Paste { session_id, text } => RpcRequest::Paste { session_id, text },
                Command::Mouse(args) => {
                    let input = match args.action {
                        MouseCommand::Move { row, col } => MouseInput::Move { row, col },
                        MouseCommand::Click { row, col, button } => MouseInput::Click {
                            row,
                            col,
                            button: button.into(),
                        },
                        MouseCommand::Down { row, col, button } => MouseInput::Down {
                            row,
                            col,
                            button: button.into(),
                        },
                        MouseCommand::Up { row, col } => MouseInput::Up { row, col },
                        MouseCommand::Wheel {
                            row,
                            col,
                            direction,
                            steps,
                        } => MouseInput::Wheel {
                            row,
                            col,
                            direction: direction.into(),
                            steps,
                        },
                    };
                    RpcRequest::Mouse {
                        session_id: args.session_id,
                        input,
                    }
                }
                Command::Resize {
                    session_id,
                    cols,
                    rows,
                } => RpcRequest::Resize {
                    session_id,
                    cols,
                    rows,
                },
                Command::Wait(args) => RpcRequest::Wait {
                    session_id: args.session_id.clone(),
                    predicate: wait_predicate(&args)?,
                    timeout_ms: args.timeout_ms,
                },
                Command::History { session_id } => RpcRequest::History { session_id },
                Command::Restart { session_id } => RpcRequest::Restart { session_id },
                Command::Close { session_id } => RpcRequest::Close { session_id },
                Command::ViewStart {
                    session_id,
                    allow_browser_control,
                    idempotency_key,
                } => RpcRequest::ViewStart {
                    session_id,
                    allow_browser_control,
                    idempotency_key,
                },
                Command::ViewStatus { view_id } => RpcRequest::ViewStatus { view_id },
                Command::ViewStop { view_id } => RpcRequest::ViewStop { view_id },
                Command::ControlStatus { session_id } => RpcRequest::ControlStatus { session_id },
                Command::ControlRelease {
                    session_id,
                    expected_generation,
                } => RpcRequest::ControlRelease {
                    session_id,
                    expected_generation,
                },
                Command::Shutdown => RpcRequest::Shutdown,
                Command::Daemon | Command::Mcp => unreachable!(),
            };
            let result = client.request(request).await?;
            println!("{}", serde_json::to_string_pretty(&result)?);
            Ok(())
        }
    }
}

fn wait_predicate(args: &WaitArgs) -> Result<WaitPredicate> {
    let count = usize::from(args.contains.is_some())
        + usize::from(args.absent.is_some())
        + usize::from(args.regex.is_some())
        + usize::from(args.stable_ms.is_some())
        + usize::from(args.process_exited);
    if count != 1 {
        bail!(
            "choose exactly one wait condition: --contains, --absent, --regex, --stable-ms, --process-exited"
        )
    }
    if let Some(text) = &args.contains {
        Ok(WaitPredicate::TextContains { text: text.clone() })
    } else if let Some(text) = &args.absent {
        Ok(WaitPredicate::TextAbsent { text: text.clone() })
    } else if let Some(pattern) = &args.regex {
        Ok(WaitPredicate::Regex {
            pattern: pattern.clone(),
        })
    } else if let Some(duration_ms) = args.stable_ms {
        Ok(WaitPredicate::Stable { duration_ms })
    } else {
        Ok(WaitPredicate::ProcessExited)
    }
}

fn into_utf8(value: OsString, label: &str) -> Result<String> {
    value
        .into_string()
        .map_err(|_| anyhow::anyhow!("{label} must be valid UTF-8"))
}

fn parse_env(entries: Vec<String>) -> Result<std::collections::BTreeMap<String, String>> {
    let mut env = std::collections::BTreeMap::new();
    for entry in entries {
        let (key, value) = entry
            .split_once('=')
            .ok_or_else(|| anyhow::anyhow!("environment entry must be KEY=VALUE: {entry:?}"))?;
        if key.is_empty() || env.insert(key.to_owned(), value.to_owned()).is_some() {
            bail!("environment keys must be nonempty and unique: {key:?}")
        }
    }
    Ok(env)
}

pub fn default_runtime_dir() -> PathBuf {
    if let Some(path) = std::env::var_os("SILICOGEN_EMULATION_RUNTIME_DIR") {
        return PathBuf::from(path);
    }
    std::env::var_os("XDG_STATE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|p| PathBuf::from(p).join(".local/state")))
        .unwrap_or_else(std::env::temp_dir)
        .join("silicogen-emulation")
}

impl From<MouseButtonArg> for MouseButton {
    fn from(value: MouseButtonArg) -> Self {
        match value {
            MouseButtonArg::Left => MouseButton::Left,
            MouseButtonArg::Middle => MouseButton::Middle,
            MouseButtonArg::Right => MouseButton::Right,
        }
    }
}

impl From<WheelDirectionArg> for WheelDirection {
    fn from(value: WheelDirectionArg) -> Self {
        match value {
            WheelDirectionArg::Up => WheelDirection::Up,
            WheelDirectionArg::Down => WheelDirection::Down,
        }
    }
}
