//! `snow daemon stop` — graceful shutdown over JSON-RPC, with PID fallback.

use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use serde_json::json;

use super::client::{endpoint_alive, request};
use super::paths::DaemonPaths;
use super::start::{read_pid, scrub_stale_runtime_files};

const STOP_TIMEOUT: Duration = Duration::from_secs(10);

/// Run `snow daemon stop`. Idempotent: if the daemon is not running,
/// prints `not running` and returns Ok.
pub fn run() -> Result<()> {
    let paths = DaemonPaths::resolve()?;
    let pid = read_pid(&paths)?;
    let reachable = endpoint_alive(&paths);

    if !reachable && pid.is_none() {
        println!("not running");
        return Ok(());
    }

    if reachable {
        match request(&paths, "shutdown", json!({})) {
            Ok(_) => {
                if wait_for_endpoint_down(&paths, STOP_TIMEOUT) {
                    scrub_stale_runtime_files(&paths);
                    print_stopped(pid);
                    return Ok(());
                }
                eprintln!(
                    "daemon did not exit after JSON-RPC shutdown, falling back to pid termination"
                );
            }
            Err(err) => {
                if wait_for_endpoint_down(&paths, Duration::from_millis(500)) {
                    scrub_stale_runtime_files(&paths);
                    print_stopped(pid);
                    return Ok(());
                }
                eprintln!("daemon shutdown RPC failed ({err:#}), falling back to pid termination");
            }
        }
    } else {
        eprintln!("daemon endpoint is unreachable, falling back to pid termination");
    }

    let Some(pid) = pid else {
        bail!("daemon endpoint did not shut down and no pidfile is available for fallback");
    };

    terminate_pid(pid, false)?;
    if wait_for_endpoint_down(&paths, Duration::from_secs(2)) {
        scrub_stale_runtime_files(&paths);
        println!("stopped, pid {pid}");
        return Ok(());
    }

    terminate_pid(pid, true)?;
    scrub_stale_runtime_files(&paths);
    println!("killed, pid {pid}");
    Ok(())
}

fn print_stopped(pid: Option<u32>) {
    if let Some(pid) = pid {
        println!("stopped, pid {pid}");
    } else {
        println!("stopped");
    }
}

fn wait_for_endpoint_down(paths: &DaemonPaths, timeout: Duration) -> bool {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if !endpoint_alive(paths) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    !endpoint_alive(paths)
}

#[cfg(unix)]
fn terminate_pid(pid: u32, force: bool) -> Result<()> {
    use nix::sys::signal::{Signal, kill};
    use nix::unistd::Pid;

    let signal = if force {
        Signal::SIGKILL
    } else {
        Signal::SIGTERM
    };
    let pid = i32::try_from(pid).context("pid exceeds Unix pid range")?;
    kill(Pid::from_raw(pid), signal).context("sending signal")
}

#[cfg(windows)]
fn terminate_pid(pid: u32, force: bool) -> Result<()> {
    let mut command = std::process::Command::new("taskkill");
    command.arg("/PID").arg(pid.to_string()).arg("/T");
    if force {
        command.arg("/F");
    }
    let status = command.status().context("running taskkill")?;
    if status.success() {
        Ok(())
    } else {
        bail!("taskkill exited with {status}")
    }
}

#[cfg(not(any(unix, windows)))]
fn terminate_pid(_pid: u32, _force: bool) -> Result<()> {
    bail!("pid termination fallback is not implemented on this platform")
}
