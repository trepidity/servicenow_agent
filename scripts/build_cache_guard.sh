#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
debug_dir="$repo_root/target/debug"
deps_dir="$debug_dir/deps"
max_entries="${SNOW_BUILD_CACHE_MAX_ENTRIES:-20000}"
mode="${1:---check}"

usage() {
  echo "usage: scripts/build_cache_guard.sh [--check|--clean]" >&2
}

case "$mode" in
  --check | --clean) ;;
  *)
    usage
    exit 2
    ;;
esac

if ! [[ "$max_entries" =~ ^[1-9][0-9]*$ ]]; then
  echo "SNOW_BUILD_CACHE_MAX_ENTRIES must be a positive integer" >&2
  exit 2
fi

if [[ ! -d "$deps_dir" ]]; then
  echo "build cache: empty"
  exit 0
fi

entry_count="$(find "$deps_dir" -mindepth 1 -maxdepth 1 -print | wc -l | tr -d ' ')"
if (( entry_count <= max_entries )); then
  echo "build cache: healthy ($entry_count dependency entries; limit $max_entries)"
  exit 0
fi

echo "build cache: stale ($entry_count dependency entries; limit $max_entries)" >&2
if [[ "$mode" == "--check" ]]; then
  echo "run scripts/build_cache_guard.sh --clean to remove generated debug artifacts" >&2
  exit 1
fi

if [[ "$debug_dir" != "$repo_root/target/debug" || ! -f "$repo_root/Cargo.toml" ]]; then
  echo "refusing to clean an unresolved build directory" >&2
  exit 2
fi

rm -rf -- "$debug_dir"
echo "build cache: removed $debug_dir (Cargo will recreate it on the next debug build)"
