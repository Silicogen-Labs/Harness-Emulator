#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
EMULATION_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
SILICOGEN_SOURCE_DIR="${SILICOGEN_SOURCE_DIR:-$EMULATION_ROOT/../Silicogen-AI}"
SILICOGEN_SOURCE_DIR="$(cd -- "$SILICOGEN_SOURCE_DIR" && pwd)"
SILICOGEN_PACKAGE="$SILICOGEN_SOURCE_DIR/packages/silicogen"
export SILICOGEN_SOURCE_DIR
RUNTIME="$EMULATION_ROOT/target/release/silicogen-emulation"
BUN="${BUN:-$(command -v bun || true)}"
if [[ -n "${SILICOGEN_FIXTURE_PORT:-}" ]]; then
  PORT="$SILICOGEN_FIXTURE_PORT"
else
  PORT="$(python3 -c 'import socket; s=socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1]); s.close()')"
fi
RUNTIME_DIR=""
RUN_ROOT=""
DAEMON_HOME=""
DAEMON_TMP=""
SERVER_SESSION=""
TUI_SESSION=""

runtime() {
  env -i \
    PATH="$PATH" \
    HOME="$DAEMON_HOME" \
    TMPDIR="$DAEMON_TMP" \
    USER="${USER:-fixture}" \
    LOGNAME="${LOGNAME:-fixture}" \
    TERM="${TERM:-xterm-256color}" \
    LANG="${LANG:-C.UTF-8}" \
    SILICOGEN_SOURCE_DIR="$SILICOGEN_SOURCE_DIR" \
    "$RUNTIME" --runtime-dir "$RUNTIME_DIR" "$@"
}

cleanup_failed_run() {
  local status=$?
  trap - EXIT
  if (( status != 0 )) && [[ -n "$RUNTIME_DIR" && -d "$RUNTIME_DIR" ]]; then
    printf '\nFixture failed; closing its sessions and daemon. Evidence is retained at %s\n' "$RUN_ROOT" >&2
    local sessions
    sessions="$(runtime list 2>/dev/null || true)"
    while IFS= read -r session_id; do
      [[ -z "$session_id" ]] && continue
      runtime close "$session_id" >/dev/null 2>&1 || true
    done < <(jq -r '.[].session_id // empty' <<<"$sessions" 2>/dev/null || true)
    runtime shutdown >/dev/null 2>&1 || true
  fi
  exit "$status"
}
trap cleanup_failed_run EXIT

wait_for() {
  local session_id="$1"
  shift
  local response
  response="$("$RUNTIME" --runtime-dir "$RUNTIME_DIR" wait "$session_id" "$@")"
  if [[ "$(jq -r '.matched' <<<"$response")" != "true" ]]; then
    printf 'Timed out waiting for the TUI fixture condition. Current screen:\n%s\n' \
      "$(jq -r '.snapshot.text // "<no snapshot>"' <<<"$response")" >&2
    return 1
  fi
}

if [[ ! -x "$RUNTIME" ]]; then
  printf 'Build the runtime first: cargo build --release --bin silicogen-emulation\n' >&2
  exit 1
fi
if [[ ! -f "$SILICOGEN_PACKAGE/src/index.ts" ]]; then
  printf 'Silicogen source checkout not found at %s\n' "$SILICOGEN_PACKAGE" >&2
  exit 1
fi
if [[ -z "$BUN" ]]; then
  printf 'Bun was not found; set BUN to its executable path.\n' >&2
  exit 1
fi
if ! python3 - "$PORT" <<'PY'
import socket
import sys
s = socket.socket()
try:
    s.bind(("127.0.0.1", int(sys.argv[1])))
except OSError as error:
    print(f"Fixture port {sys.argv[1]} is already in use: {error}", file=sys.stderr)
    raise SystemExit(1)
finally:
    s.close()
PY
then
  exit 1
fi

mkdir -p "$EMULATION_ROOT/.runtime"
RUN_ROOT="$(mktemp -d "$EMULATION_ROOT/.runtime/sd.XXXXXX")"
RUNTIME_DIR="$RUN_ROOT"
DAEMON_HOME="$RUN_ROOT/daemon-home"
DAEMON_TMP="$RUN_ROOT/daemon-tmp"
mkdir -p "$DAEMON_HOME" "$DAEMON_TMP"
PROJECT_DIR="$RUN_ROOT/project"
mkdir -p "$PROJECT_DIR"
git init --quiet "$PROJECT_DIR"
cp "$SCRIPT_DIR/silicogen-demo-config.json" "$PROJECT_DIR/silicogen.json"
python3 - "$PROJECT_DIR/silicogen.json" "$BUN" "$SCRIPT_DIR/local-mcp-fixture.mjs" <<'PY'
import json, sys
with open(sys.argv[1]) as file:
    config = json.load(file)
config["mcp"] = {"fixture-toggle": {"type": "local", "command": [sys.argv[2], sys.argv[3]], "enabled": False, "timeout": 5000}}
with open(sys.argv[1], "w") as file:
    json.dump(config, file, indent=2)
PY
runtime agent profiles --json '{"action":"store","profile":{"name":"silicogen-fixture","description":"Synthetic fixture; no live provider credentials","env_allowlist":["PATH","LANG","USER","LOGNAME"],"env":{},"seeds":[],"metadata":{"purpose":"deterministic TUI acceptance"}}}' >/dev/null
jq --arg bun "$BUN" \
  --arg package "$SILICOGEN_PACKAGE" \
  --arg server "$SILICOGEN_PACKAGE/test/fixture/mcp-server.ts" \
  '.mcp = {"fixture-toggle": {"type":"local", "command":[$bun,"run","--cwd",$package,"--conditions=browser",$server,"serve"], "enabled":false, "timeout":5000}}' \
  "$PROJECT_DIR/silicogen.json" > "$PROJECT_DIR/silicogen.json.tmp"
mv "$PROJECT_DIR/silicogen.json.tmp" "$PROJECT_DIR/silicogen.json"

SERVER_JSON="$(runtime launch \
  --profile silicogen-fixture \
  --name subagents-fixture-server \
  --cwd "$PROJECT_DIR" \
  --env EMULATION_FIXTURE_API_KEY=emulation-placeholder-never-used \
  --env "SILICOGEN_SOURCE_DIR=$SILICOGEN_SOURCE_DIR" \
  --env SILICOGEN_STARTUP_TRACE=1 \
  --env SILICOGEN_DISABLE_MODELS_FETCH=1 \
  -- "$BUN" run --cwd "$SILICOGEN_PACKAGE" --conditions=browser \
    "$EMULATION_ROOT/fixtures/silicogen-runtime-wrapper.mjs" \
    "$PROJECT_DIR" "$SILICOGEN_PACKAGE/src/index.ts" \
    serve --port "$PORT" --hostname 127.0.0.1)"
SERVER_SESSION="$(jq -r '.session_id' <<<"$SERVER_JSON")"
if [[ -z "$SERVER_SESSION" || "$SERVER_SESSION" == "null" ]]; then
  printf 'Server launch did not return a session id:\n%s\n' "$SERVER_JSON" >&2
  exit 1
fi

# The isolated process gets its own XDG cache. Seed an empty catalog so the
# provider fixture in silicogen.json is authoritative and startup makes no
# models.dev network request.
MODEL_CACHE="$RUNTIME_DIR/sessions/$SERVER_SESSION/state/cache/silicogen"
mkdir -p "$MODEL_CACHE"
cp "$SCRIPT_DIR/empty-model-catalog.json" "$MODEL_CACHE/models.json"

wait_for "$SERVER_SESSION" --contains "silicogen server listening" --timeout-ms 10000

SEED_JSON="$(cd "$SILICOGEN_PACKAGE" && env -i \
  PATH="$PATH" HOME="$DAEMON_HOME" TMPDIR="$DAEMON_TMP" \
  USER="${USER:-fixture}" LOGNAME="${LOGNAME:-fixture}" \
  TERM="${TERM:-xterm-256color}" LANG="${LANG:-C.UTF-8}" \
  SILICOGEN_SOURCE_DIR="$SILICOGEN_SOURCE_DIR" \
  "$BUN" --conditions=browser \
  "$EMULATION_ROOT/fixtures/seed-silicogen-subagents.ts" \
  "$RUNTIME_DIR" "$SERVER_SESSION" "$PROJECT_DIR")"
PARENT_SESSION="$(jq -r '.sessionId' <<<"$SEED_JSON")"
GROUP_ID="$(jq -r '.groupId' <<<"$SEED_JSON")"
if [[ -z "$PARENT_SESSION" || "$PARENT_SESSION" == "null" ]]; then
  printf 'Fixture seeding did not return a parent session:\n%s\n' "$SEED_JSON" >&2
  exit 1
fi

TUI_JSON="$(runtime launch \
  --profile silicogen-fixture \
  --name subagents-fixture-tui \
  --cwd "$PROJECT_DIR" \
  --env EMULATION_FIXTURE_API_KEY=emulation-placeholder-never-used \
  --env "SILICOGEN_SOURCE_DIR=$SILICOGEN_SOURCE_DIR" \
  --env SILICOGEN_STARTUP_TRACE=1 \
  --env SILICOGEN_DISABLE_MODELS_FETCH=1 \
  --env SILICOGEN_DISABLE_REMOTE_SKILLS=1 \
  -- "$BUN" run --cwd "$SILICOGEN_PACKAGE" --conditions=browser \
    "$EMULATION_ROOT/fixtures/silicogen-runtime-wrapper.mjs" \
    "$PROJECT_DIR" --fixture-auth "$SILICOGEN_PACKAGE/src/index.ts" \
    attach "http://127.0.0.1:$PORT" --dir "$PROJECT_DIR" --session "$PARENT_SESSION")"
TUI_SESSION="$(jq -r '.session_id' <<<"$TUI_JSON")"

wait_for "$TUI_SESSION" --contains "Message..." --timeout-ms 15000
# Use the session's default leader binding to open the view. This bypasses
# provider/model prompt gating and exercises the same registered TUI command.
runtime key "$TUI_SESSION" Ctrl+X >/dev/null
wait_for "$TUI_SESSION" --stable-ms 200 --timeout-ms 1000
runtime key "$TUI_SESSION" w >/dev/null
wait_for "$TUI_SESSION" --contains "Filter subagents" --timeout-ms 10000
wait_for "$TUI_SESSION" --contains "clarification_waiter" --timeout-ms 10000
wait_for "$TUI_SESSION" --contains "1 active · 2 review" --timeout-ms 10000
wait_for "$TUI_SESSION" --contains "rtl_width_audit" --timeout-ms 10000
wait_for "$TUI_SESSION" --contains "questa_flow_audit" --timeout-ms 10000
SCREENSHOT_JSON="$(runtime screenshot "$TUI_SESSION")"
SCREENSHOT_PATH="$(jq -r '.path' <<<"$SCREENSHOT_JSON")"
SCREENSHOT_FRAME="$(jq -r '.frame_id' <<<"$SCREENSHOT_JSON")"
OBSERVE_JSON="$(runtime observe "$TUI_SESSION")"
SCREEN_TEXT="$(jq -r '.text // .snapshot.text // "<screen text unavailable>"' <<<"$OBSERVE_JSON")"

cat <<EOF
Static Silicogen Subagents fixture is open and ready. No model calls were made.

Parent session: $PARENT_SESSION
Task group:     $GROUP_ID
TUI session:    $TUI_SESSION
Runtime data:   $RUN_ROOT
Screenshot:     $SCREENSHOT_PATH
Frame:          $SCREENSHOT_FRAME
HTTP port:      $PORT
Fixture MCP:    fixture-toggle (local stdio fixture, initially disabled)

Observed screen:
$SCREEN_TEXT

The TUI is already on /subagents. Navigate/select with arrows, click rows with
mouse, and press Enter to open a fixture worker child session.

Observe or interact from other shell calls:
  $RUNTIME --runtime-dir '$RUNTIME_DIR' observe '$TUI_SESSION'
  $RUNTIME --runtime-dir '$RUNTIME_DIR' screenshot '$TUI_SESSION'
  $RUNTIME --runtime-dir '$RUNTIME_DIR' close '$TUI_SESSION'
  $RUNTIME --runtime-dir '$RUNTIME_DIR' close '$SERVER_SESSION'
  $RUNTIME --runtime-dir '$RUNTIME_DIR' shutdown

Artifacts and the copied project remain under $RUN_ROOT for inspection.
EOF
