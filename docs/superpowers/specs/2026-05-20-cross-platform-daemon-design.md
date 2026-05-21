# Cross-Platform Daemon, MCP, and IPC — Design

**Date:** 2026-05-20
**Status:** Approved (design); pending implementation plan
**Author:** Jared (with Claude)

## Goal

Make `servicenow_agent` — its daemon, MCP bridge, and IPC — run on **Windows,
Linux, and macOS** with a single behavior and a single primary code path.

Today the CLI builds on Windows but **daemon mode is Unix-only** (commit
`5b81ad4`). The daemon backs three things the user relies on: cached/shared
state, the MCP bridge backend, and the TUI backend. Those need a long-lived
process, so "just run every command directly" (current Windows behavior) is
insufficient — Windows needs a working daemon too.

## Non-Goals

- OS service-manager integration (systemd unit / Windows Service). Considered
  and rejected: per-OS divergence, install/elevation burden.
- Relocating existing macOS/Linux config. We deliberately preserve
  `~/.config/snow`.
- Changing the JSON-RPC protocol or the newline-delimited framing.

## What Makes the Daemon Unix-Only Today

Three seams, all in otherwise-portable code:

1. **IPC transport** — Unix domain sockets
   (`tokio::net::{UnixListener,UnixStream}`, `std::os::unix::net::UnixStream`)
   for CLI↔daemon and MCP-bridge↔daemon. Files:
   `snow_daemon/src/rpc.rs`, `snow_cli/src/tui_client.rs`,
   `snow_mcp/src/daemon_bridge.rs`, `snow_cli/src/daemon_cmd/{start,status}.rs`.
2. **Launch** — `fork()` / `setsid()` / `dup2_stdout/stderr` via `nix`
   (`snow_cli/src/daemon_cmd/start.rs`).
3. **Stop / liveness** — POSIX signals `kill(SIGTERM/SIGKILL)` and `kill(pid,0)`
   liveness probe (`snow_cli/src/daemon_cmd/{start,stop}.rs`,
   `snow_daemon/src/rpc.rs`).

Path handling is **not** a real blocker: `paths.rs` already uses
`dirs::home_dir().join(".config/snow")`, which resolves on all three platforms.

## Chosen Approach

A **portable local-socket daemon that clients lazily auto-spawn as a detached
child, with idle self-shutdown.** Platform differences are pushed down into two
thin, isolated seams: the **IPC endpoint name** and the **process-spawn flags**.

Rejected alternatives:
- **Loopback TCP + token** — portable but opens a local port and requires token
  auth, losing the filesystem/pipe permission model.
- **Hand-written `#[cfg(windows)]` named-pipe code** — doubles the
  platform-specific surface to maintain.
- **OS service managers** — see Non-Goals.

## Components

### 1. IPC layer — the `interprocess` seam

Replace all Unix-socket touch points with the `interprocess` crate's tokio
local sockets (Unix domain sockets on Unix; named pipes on Windows; one API).

- `snow_daemon/src/rpc.rs`: `tokio::net::{UnixListener,UnixStream}` →
  `interprocess::local_socket::tokio::{Listener, Stream}`. Newline-delimited
  JSON framing unchanged (interprocess streams are `AsyncRead`/`AsyncWrite`).
- `snow_cli/src/tui_client.rs`, `snow_mcp/src/daemon_bridge.rs`:
  `UnixStream::connect` → interprocess `Stream::connect`.
- `snow_cli/src/daemon_cmd/{start,status}.rs`: the `socket_alive()` probe →
  interprocess connect.

A new small module (`snow_daemon::ipc` or `snow_core::ipc`) owns endpoint-name
construction so the cfg lives in **one** place:

- **Unix:** filesystem name from `<config_dir>/daemon.sock` (today's layout).
- **Windows:** namespaced pipe name `snow-daemon-<env>` → `\\.\pipe\snow-daemon-<env>`.

`DaemonPaths.socket: PathBuf` becomes an `IpcEndpoint` value carrying the
correct per-OS name. Pidfile/statusfile/logfile/env-file remain filesystem
paths in the config dir.

### 2. Lifecycle — detached spawn + lazy auto-spawn

Replace `fork()`/`setsid()`/`dup2` with **re-executing the current binary** as a
detached child running a new hidden internal subcommand
`snow daemon __serve --env <env>`. That child calls the existing
`snow_daemon::run_blocking_rpc_only`, then writes the pidfile + JSON statusfile
itself (as today's forked child does).

- **Unix:** `std::process::Command` with `pre_exec(setsid)`; stdout/stderr
  redirected to the logfile `File`.
- **Windows:** `Command` with
  `.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP)`; stdout/stderr
  redirected to the logfile.

The spawn primitive lives in a shared helper (e.g.
`snow_cli::daemon_cmd::spawn`). **Lazy auto-spawn:** clients (TUI, MCP bridge,
daemon-routed CLI commands) that fail to connect call the helper, wait a bounded
time (≈60s) for the endpoint to become connectable, then connect.
`snow daemon start` calls the same helper for explicit pre-warming.

### 3. Lifecycle — stop / status / liveness

- **Liveness** = "endpoint connectable" (portable), with the pidfile as
  secondary metadata. Removes the Unix-only `kill(pid,0)` probe as the primary
  signal.
- **Stop** = graceful **`shutdown` RPC** over IPC. The daemon accept loop
  selects on a `tokio::sync::Notify` (or watch channel); on shutdown it stops
  accepting, drains in-flight requests, removes pid/status files, and exits.
  Fallback if the RPC fails: kill by PID, cfg-gated — `nix::kill` on Unix,
  `OpenProcess` + `TerminateProcess` on Windows.
- **status / logs** keep current behavior (read the same files).

All four subcommands (`start`/`stop`/`status`/`logs`) are un-gated and work on
every platform. `__serve` is a fifth, hidden subcommand.

### 4. Idle self-shutdown

The daemon tracks an active-connection count and a last-activity timestamp. A
tokio interval task (≈60s tick) triggers the **same graceful shutdown path**
when connections are zero and idle exceeds the timeout. Default **30 minutes**,
overridable via env (e.g. `SNOW_DAEMON_IDLE_SECS`) or flag; `snow daemon start`
accepts `--no-idle-timeout` for a pinned, pre-warmed daemon. Pure tokio timers,
fully portable.

### 5. Paths — Windows-only branch

**Decision:** keep macOS + Linux at `~/.config/snow` (no regression on the
user's current machine); add a **Windows-only** branch resolving to
`%APPDATA%\snow`. The exe-override (`<exe_dir>/snow_config`) and cwd fallback
(`.snow`) in `resolve_config_dir()` are untouched. (We rejected
`directories::ProjectDirs` everywhere because on macOS it returns
`~/Library/Application Support/...`, relocating the existing config.)

Concretely, in `snow_cli/src/daemon_cmd/paths.rs::resolve_config_dir()`:

```text
exe_dir/snow_config            (if exists)              — unchanged
  else, #[cfg(windows)] %APPDATA%\snow                  — NEW
  else, #[cfg(unix)]    ~/.config/snow                  — unchanged
  else cwd/.snow                                        — unchanged
```

## Data Flow

```
client (CLI / TUI / MCP bridge)
  └─ connect IpcEndpoint ──fail?──> spawn detached `snow daemon __serve`
                                      └─ wait ≤60s for endpoint
  └─ JSON-RPC over interprocess local socket  <──>  daemon accept loop
                                                      ├─ handle requests
                                                      ├─ Notify on `shutdown` RPC
                                                      └─ idle task → Notify
```

The MCP bridge (`snow_mcp_bridge`) is unchanged in shape: stdio MCP transport in
front, daemon IPC client behind. Its `--daemon-socket` arg becomes a
`--daemon-endpoint` name (or stays a path on Unix and is interpreted by the
shared `IpcEndpoint` builder).

## Error Handling

- **Connect failure** → auto-spawn, then retry within the bounded window; if the
  endpoint never comes up, return the existing clear daemon-unreachable error
  (no more "not supported on Windows" message).
- **Spawn failure** → surface the OS error from `Command::spawn`.
- **Stale pidfile / endpoint** → if the endpoint isn't connectable, treat as
  not-running and scrub stale pid/status files (as `start.rs` does today).
- **Graceful-shutdown timeout** → fall back to PID kill.

## Testing

- IPC round-trip over an interprocess endpoint (extends existing framing tests).
- Spawn helper: auto-spawn → connect → `shutdown` RPC → process gone (PID-kill
  fallback path gated per-OS).
- Idle-timeout: inject a short timeout, assert the daemon exits with no clients.
- Path resolution: assert Windows branch yields `%APPDATA%\snow` and Unix branch
  is unchanged (cfg-gated unit tests).
- **CI matrix gains a Windows job** running the daemon test suite (today daemon
  code is excluded from CI on Windows).

## Risks / Open Items

- `interprocess` tokio API surface and `Name`/namespaced-name ergonomics need
  verification against the current crate version during implementation.
- Windows pipe security: default ACLs on `\\.\pipe\` — confirm the pipe is
  restricted to the current user (named-pipe security descriptor).
- `pre_exec` is `unsafe`; keep it minimal and documented (matches existing
  `fork` SAFETY comments).
- Detached-child stdout/stderr redirection on Windows must not inherit the
  parent console.
