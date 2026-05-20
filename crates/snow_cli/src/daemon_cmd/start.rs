//! `snow daemon start` — fork-and-detach the daemon as a background process.
//!
//! The flow:
//!   1. Inspect any existing pidfile + socket. If both indicate a healthy
//!      daemon, do nothing. If the pidfile is stale, scrub it.
//!   2. Rotate the log if it's grown beyond 10 MiB.
//!   3. `fork()`. The parent waits up to 60 s for the child's socket to
//!      become connectable, then prints `started, pid <N>`.
//!   4. The child calls `setsid()` to detach from the controlling terminal,
//!      redirects stdout+stderr to the logfile via `dup2`, writes the
//!      pidfile and JSON statusfile, then hands off to
//!      [`snow_daemon::run_blocking_rpc_only`]. The child never returns —
//!      it `exit()`s when the daemon shuts down.
//!
//! `run_blocking_rpc_only` is used (not `run_blocking`) because once the
//! child has detached its stdio onto the logfile, an MCP stdio transport
//! would just be reading/writing the log.

use std::fs::OpenOptions;
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use nix::sys::signal::{Signal, kill};
use nix::sys::wait::{WaitPidFlag, WaitStatus, waitpid};
use nix::unistd::{ForkResult, Pid, dup2_stderr, dup2_stdout, fork, setsid};

use super::paths::DaemonPaths;

const START_TIMEOUT: Duration = Duration::from_secs(60);

/// Run `snow daemon start`. Returns once the parent has confirmed the
/// daemon is reachable, or with an error if the child failed to come up.
pub fn run(env_name: &str) -> Result<()> {
    let paths = DaemonPaths::resolve()?;
    let env_file = load_env_file(env_name);
    set_selected_env(env_name);

    if let Some(pid) = read_pid(&paths)? {
        if process_alive(pid) && socket_alive(&paths) {
            if let Some(status_env) = read_status_env(&paths) {
                println!("already running, pid {pid}\n  env: {status_env}");
            } else {
                println!("already running, pid {pid}");
            }
            return Ok(());
        }
        // stale
        let _ = std::fs::remove_file(&paths.pidfile);
        let _ = std::fs::remove_file(&paths.statusfile);
    }

    rotate_log(&paths)?;

    // SAFETY: fork is unsafe in async-unsafe contexts; this CLI command runs
    // before tokio is initialized in the child, so it's safe.
    match unsafe { fork() }.context("fork")? {
        ForkResult::Parent { child } => {
            wait_for_socket(&paths, child, START_TIMEOUT).with_context(|| {
                format!("daemon child {} did not become reachable", child.as_raw())
            })?;
            println!("started, pid {}\n  env: {env_name}", child.as_raw());
            Ok(())
        }
        ForkResult::Child => {
            // Detach and run the daemon main.
            setsid().context("setsid")?;
            let log = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&paths.logfile)
                .with_context(|| format!("opening {}", paths.logfile.display()))?;
            // dup2 onto stdout/stderr. We deliberately keep the `log` File
            // alive on the stack until the daemon's run_blocking is invoked
            // (and beyond) so that dropping it doesn't close fds 1/2.
            let _ = dup2_stdout(&log);
            let _ = dup2_stderr(&log);

            let pid = std::process::id() as i32;
            let _ = std::fs::write(&paths.pidfile, format!("{pid}\n"));
            let started_at = chrono::Utc::now().to_rfc3339();
            let version = env!("CARGO_PKG_VERSION");
            let status = serde_json::json!({
                "pid": pid,
                "started_at": started_at,
                "version": version,
                "environment": env_name,
                "env_file": env_file.as_ref().map(|path| path.display().to_string()),
                "socket": paths.socket.display().to_string(),
            });
            if let Ok(s) = serde_json::to_string_pretty(&status) {
                let _ = std::fs::write(&paths.statusfile, s);
            }

            // Hand off to the daemon's library entry. This must NOT return
            // on success.
            let result = snow_daemon::run_blocking_rpc_only(&paths.socket);

            let _ = std::fs::remove_file(&paths.pidfile);
            let _ = std::fs::remove_file(&paths.statusfile);

            // Keep `log` alive until after the daemon exits.
            drop(log);

            match result {
                Ok(()) => std::process::exit(0),
                Err(e) => {
                    eprintln!("daemon exited with error: {e:#}");
                    std::process::exit(1);
                }
            }
        }
    }
}

fn read_status_env(paths: &DaemonPaths) -> Option<String> {
    std::fs::read_to_string(&paths.statusfile)
        .ok()
        .and_then(|s| serde_json::from_str::<serde_json::Value>(&s).ok())
        .and_then(|status| {
            status
                .get("environment")
                .and_then(|value| value.as_str())
                .map(ToOwned::to_owned)
        })
}

fn read_pid(paths: &DaemonPaths) -> Result<Option<i32>> {
    match std::fs::read_to_string(&paths.pidfile) {
        Ok(s) => Ok(s.trim().parse().ok()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

/// Probe whether `pid` corresponds to a live process by sending signal 0
/// (which only checks for existence + permission to signal).
pub(crate) fn process_alive(pid: i32) -> bool {
    kill(Pid::from_raw(pid), None).is_ok()
}

/// Probe whether the daemon's Unix socket accepts a connection.
///
/// `std::os::unix::net::UnixStream` has no `connect_timeout`, so we rely on
/// the kernel's default behavior: connect to a non-listening AF_UNIX path
/// fails immediately with `ECONNREFUSED` / `ENOENT`, so this is effectively
/// non-blocking in the failure case.
pub(crate) fn socket_alive(paths: &DaemonPaths) -> bool {
    UnixStream::connect(&paths.socket).is_ok()
}

fn wait_for_socket(paths: &DaemonPaths, child: Pid, timeout: Duration) -> Result<()> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if socket_alive(paths) {
            return Ok(());
        }
        match waitpid(child, Some(WaitPidFlag::WNOHANG)) {
            Ok(WaitStatus::StillAlive) => {}
            Ok(status) => bail!("daemon child exited before socket was ready: {status:?}"),
            Err(err) => bail!("failed to inspect daemon child: {err}"),
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    bail!(
        "timeout waiting for daemon socket {}",
        paths.socket.display()
    )
}

fn load_env_file(env_name: &str) -> Option<PathBuf> {
    let filename = format!(".env.{env_name}");
    let path = super::paths::config_path(&filename);
    if path.exists() {
        let _ = dotenvy::from_path(&path);
        return Some(path);
    }
    None
}

fn set_selected_env(env_name: &str) {
    // SAFETY: daemon lifecycle commands run before any Tokio runtime or worker
    // threads are started, then fork exactly once. This process-wide env write
    // is used to pass the selected environment into the child daemon.
    unsafe {
        std::env::set_var("SNOW_ENV", env_name);
    }
}

fn rotate_log(paths: &DaemonPaths) -> Result<()> {
    if let Ok(meta) = std::fs::metadata(&paths.logfile)
        && meta.len() > 10 * 1024 * 1024
    {
        let _ = std::fs::rename(&paths.logfile, &paths.logfile_rotated);
    }
    Ok(())
}

/// Send `sig` to `pid`. Shared with `stop.rs`.
pub(crate) fn send_signal(pid: i32, sig: Signal) -> Result<()> {
    kill(Pid::from_raw(pid), sig).context("sending signal")
}
