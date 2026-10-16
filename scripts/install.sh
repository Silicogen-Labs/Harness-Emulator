#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
EMULATION_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
BIN_DIR="$EMULATION_ROOT/.local/bin"
INSTALL_PATH="$BIN_DIR/silicogen-emulation"
RUNTIME_DIR="$EMULATION_ROOT/.runtime/installed-mcp"

command -v cargo >/dev/null || { printf 'cargo is required to build silicogen-emulation.\n' >&2; exit 1; }

TARGET_DIR="$(cargo metadata --locked --no-deps --format-version 1 \
  --manifest-path "$EMULATION_ROOT/Cargo.toml" \
  | python3 -c 'import json, sys; print(json.load(sys.stdin)["target_directory"])')"
export CARGO_TARGET_DIR="$TARGET_DIR"

printf 'Building silicogen-emulation once with the locked dependency graph...\n'
cargo build --release --locked --manifest-path "$EMULATION_ROOT/Cargo.toml" --bin silicogen-emulation

mkdir -p "$BIN_DIR"
install -m 0755 "$TARGET_DIR/release/silicogen-emulation" "$INSTALL_PATH"

printf 'Installed binary: %s\n' "$INSTALL_PATH"
printf 'MCP configuration (copy manually; this script does not edit client configuration):\n'
python3 - "$INSTALL_PATH" "$RUNTIME_DIR" <<'PY'
import json
import sys
print(json.dumps({"command": sys.argv[1], "args": ["--runtime-dir", sys.argv[2], "mcp"]}, indent=2))
PY
