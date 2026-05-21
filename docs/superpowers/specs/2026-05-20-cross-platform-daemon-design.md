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

Path handling is **not** the main blocker, but it must be centralized during
implementation. The checkout currently duplicates `~/.config/snow` helpers in
CLI, daemon, and MCP bridge code; Windows support needs one shared resolver so
`%APPDATA%\snow` is applied consistently.

## Chosen Approach

A **portable local-socket daemon that clients lazily auto-spawn as a detached
child, with idle self-shutdown.** Platform differences are pushed down into
small, isolated seams: the **IPC endpoint name**, the **process-spawn flags**,
and the **config-root fallback**.

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

A new small module (`snow_core::ipc`, or a new tiny shared daemon-client crate)
owns endpoint-name construction and connect/listen helpers so the cfg lives in
**one** place and so `snow_mcp` never needs to depend on `snow_cli`.

- **Daemon identity decision:** keep the existing one-daemon-per-config-root
  behavior. The daemon's selected env is runtime metadata, not part of the
  daemon identity. This avoids a surprising split where Windows can run
  `test` and `prd` daemons concurrently but Unix cannot.
- **Unix:** filesystem name from `<config_dir>/daemon.sock` (today's layout).
- **Windows:** namespaced pipe name `snow-daemon-<scope>` →
  `\\.\pipe\snow-daemon-<scope>`, where `<scope>` is a stable sanitized/hash
  token derived from the resolved config dir, not from `SNOW_ENV`.

`DaemonPaths.socket: PathBuf` becomes an `IpcEndpoint` value carrying the
correct per-OS name. Pidfile/statusfile/logfile/env-file remain filesystem
paths in the config dir. If we later want concurrent per-env daemons, endpoint,
pidfile, statusfile, logfile, and auto-spawn targeting must all become
env-scoped together in a separate design.

`IpcEndpoint` is **not** a replacement for the daemon's filesystem root. The
daemon configuration must carry both:

- `endpoint: IpcEndpoint` — how clients connect.
- `config_dir` / `data_dir: PathBuf` — where pid/status/log/env/vault/planning
  state lives.

This matters on Windows because a named pipe has no filesystem parent, while
today daemon-owned state is derived from the Unix socket's parent directory.

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

The spawn primitive has two layers:

- A **shared daemon client layer** owns endpoint connect/retry/wait behavior and
  is usable by `snow_cli`, `snow_mcp`, and tests without introducing a
  `snow_mcp` → `snow_cli` dependency.
- A **CLI launcher layer** owns the actual `snow daemon __serve` command line,
  env selection, log redirection, status-file creation, and platform-specific
  detached-spawn flags.

**Lazy auto-spawn:** clients that fail to connect wait a bounded time (≈60s)
for the endpoint to become connectable after triggering startup.

- `snow daemon start` uses the CLI launcher directly for explicit pre-warming.
- `snow tui --daemon` and daemon-routed CLI commands can use the current
  executable path.
- `snow_mcp_bridge` must not link to `snow_cli`; it resolves the `snow` binary
  from `--snow-bin`, `SNOW_BIN`, a same-directory sibling, or `PATH`, then
  invokes the CLI launcher surface (`snow daemon start` or the hidden
  `snow daemon __serve` contract, as selected during implementation). If no
  binary can be resolved, it returns a clear "daemon unavailable and auto-spawn
  could not locate snow" error.

### 3. Lifecycle — stop / status / liveness

- **Liveness** = "endpoint connectable" (portable), with the pidfile as
  secondary metadata. Removes the Unix-only `kill(pid,0)` probe as the primary
  signal.
- **Stop** = graceful **`shutdown` RPC** over IPC. The daemon accept loop
  selects on a `tokio::sync::Notify` (or watch channel); on shutdown it stops
  accepting, waits for tracked in-flight connection tasks to finish up to a
  short bounded grace period, removes pid/status files, and exits. This requires
  task tracking because today's accept loop `spawn_local`s connection handlers
  and otherwise returns immediately on shutdown.
  Fallback if the RPC fails: kill by PID, cfg-gated — `nix::kill` on Unix,
  `OpenProcess` + `TerminateProcess` on Windows.
- **status / logs** keep current behavior (read the same files).

All four subcommands (`start`/`stop`/`status`/`logs`) are un-gated and work on
every platform. `__serve` is a fifth, hidden subcommand.

### 4. Idle self-shutdown

The daemon tracks active connection tasks and a last-activity timestamp. A
tokio interval task (≈60s tick) triggers the **same graceful shutdown path**
when connections are zero and idle exceeds the timeout. Default **30 minutes**,
overridable via env (e.g. `SNOW_DAEMON_IDLE_SECS`) or flag; `snow daemon start`
accepts `--no-idle-timeout` for a pinned, pre-warmed daemon. Pure tokio timers,
fully portable.

### 5. Paths — Windows-only branch

**Decision:** keep macOS + Linux at `~/.config/snow` (no regression on the
user's current machine); add a **Windows-only** branch resolving to
`%APPDATA%\snow`. The exe-override (`<exe_dir>/snow_config`) and cwd fallback
(`.snow`) are untouched. (We rejected `directories::ProjectDirs` everywhere
because on macOS it returns `~/Library/Application Support/...`, relocating the
existing config.)

There must be one shared config-dir resolver used by all current duplicate
helpers, not only `snow_cli/src/daemon_cmd/paths.rs`. The implementation plan
must update:

- `snow_cli/src/daemon_cmd/paths.rs::resolve_config_dir`.
- `snow_cli/src/main.rs::runtime_paths` and auth error-path hints.
- `snow_daemon/src/lib.rs::daemon_env_paths`, `default_vault_path`, and
  `default_socket_path`.
- `snow_mcp/src/bin/snow_mcp_bridge.rs::default_socket_path` or its replacement
  endpoint default.

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
shared `IpcEndpoint` builder). Auto-spawn support in the bridge is process-based
(`--snow-bin` / `SNOW_BIN` / sibling / `PATH` discovery), not a direct dependency
on `snow_cli`.

## Error Handling

- **Connect failure** → auto-spawn, then retry within the bounded window; if the
  endpoint never comes up, return the existing clear daemon-unreachable error
  (no more "not supported on Windows" message).
- **Spawn failure** → surface the OS error from `Command::spawn`.
- **Stale pidfile / endpoint** → if the endpoint isn't connectable, treat as
  not-running and scrub stale pid/status files (as `start.rs` does today).
- **Graceful-shutdown timeout** → fall back to PID kill.

## Cargo, Packaging, and CI

- Move `snow_daemon` out from `snow_cli`'s Unix-only dependency section once the
  daemon entrypoint is portable. Keep `nix` Unix-only and add any Windows process
  API dependency behind `cfg(windows)`.
- Add `interprocess` to the workspace dependency set and wire it into the
  crates that connect/listen on daemon IPC (`snow_daemon`, `snow_cli`,
  `snow_mcp`, or the shared daemon-client crate/module).
- Update release packaging so Windows ships every intended binary, not just
  `snow.exe`, once `snow_daemon` and `snow_mcp_bridge` compile there.
- Update stale docs/comments that currently say daemon mode is Unix-only or
  stops via SIGTERM/SIGKILL.

## Testing

- IPC round-trip over an interprocess endpoint (extends existing framing tests).
- Spawn helper: auto-spawn → connect → `shutdown` RPC → process gone (PID-kill
  fallback path gated per-OS).
- Idle-timeout: inject a short timeout, assert the daemon exits with no clients.
- Path resolution: assert Windows branch yields `%APPDATA%\snow` and Unix branch
  is unchanged (cfg-gated unit tests).
- Endpoint scoping: assert changing `SNOW_ENV` does not silently target a
  different daemon unless all runtime files are explicitly env-scoped.
- MCP bridge auto-spawn: assert missing daemon plus resolvable `snow` binary
  starts the daemon, and missing daemon plus unresolved `snow` binary fails with
  the documented clear error.
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
