#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
install_dir="${SNOW_RELEASE_INSTALL_DIR:-$HOME/.cargo/bin}"
snow_bin="$install_dir/snow"
bridge_bin="$install_dir/snow_mcp_bridge"

run() {
  printf '+'
  printf ' %q' "$@"
  printf '\n'
  "$@"
}

cd "$repo_root"

run mkdir -p "$install_dir"
run cargo build --release -p snow_cli -p snow_daemon -p snow_mcp

run install -m 755 target/release/snow "$snow_bin"
run install -m 755 target/release/snow_daemon "$install_dir/snow_daemon"
run install -m 755 target/release/snow_mcp_bridge "$bridge_bin"

run "$snow_bin" daemon restart
run "$snow_bin" daemon status
run python3 scripts/mcp_schema_smoke.py -- "$bridge_bin" --snow-bin "$snow_bin" --require-contract daemon-json-rpc-v1
