//! `snow daemon start` — re-exec this CLI as a detached daemon child.

use std::fs::OpenOptions;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

#[cfg(unix)]
use std::os::unix::process::CommandExt;
#[cfg(windows)]
use std::os::windows::process::CommandExt;

use anyhow::{Context, Result, bail};

use super::client::endpoint_alive;
use super::paths::{DaemonPaths, lock_runtime_files};

const START_TIMEOUT: Duration = Duration::from_secs(60);
#[cfg(windows)]
const DETACHED_PROCESS: u32 = 0x0000_0008;
#[cfg(windows)]
const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;

#[derive(Debug, Clone)]
pub(crate) enum StartOutcome {
    AlreadyRunning {
        pid: Option<u32>,
        environment: Option<String>,
    },
    Started {
        pid: u32,
    },
}

/// Run `snow daemon start`. Returns once the parent has confirmed the daemon
/// endpoint is reachable, or with an error if the child failed to come up.
pub fn run(env_name: &str, no_idle_timeout: bool) -> Result<()> {
    match ensure_running_with_idle(env_name, no_idle_timeout)? {
        StartOutcome::AlreadyRunning { pid, environment } => {
            match (pid, environment) {
                (Some(pid), Some(environment)) => {
                    println!("already running, pid {pid}\n  env: {environment}");
                }
                (Some(pid), None) => println!("already running, pid {pid}"),
                (None, Some(environment)) => println!("already running\n  env: {environment}"),
                (None, None) => println!("already running"),
            }
            Ok(())
        }
        StartOutcome::Started { pid } => {
            println!("started, pid {pid}\n  env: {env_name}");
            Ok(())
        }
    }
}

/// Auto-spawn entrypoint (lazy startup): brings the daemon up with the default
/// idle timeout so an unattended daemon eventually shuts itself down.
pub(crate) fn ensure_running(env_name: &str) -> Result<StartOutcome> {
    ensure_running_with_idle(env_name, false)
}

pub(crate) fn ensure_running_with_idle(
    env_name: &str,
    no_idle_timeout: bool,
) -> Result<StartOutcome> {
    let paths = DaemonPaths::resolve()?;
    if endpoint_alive(&paths) {
        return Ok(StartOutcome::AlreadyRunning {
            pid: read_pid(&paths)?,
            environment: read_status_env(&paths),
        });
    }

    scrub_stale_runtime_files(&paths);
    rotate_log(&paths)?;
    let mut child = spawn_detached_child(&paths, env_name, no_idle_timeout)?;
    wait_for_endpoint(&paths, &mut child, START_TIMEOUT)
        .with_context(|| format!("daemon child {} did not become reachable", child.id()))?;
    Ok(StartOutcome::Started { pid: child.id() })
}

fn spawn_detached_child(
    paths: &DaemonPaths,
    env_name: &str,
    no_idle_timeout: bool,
) -> Result<Child> {
    let exe = std::env::current_exe().context("resolving current snow executable")?;
    let log = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&paths.logfile)
        .with_context(|| format!("opening {}", paths.logfile.display()))?;
    let log_err = log
        .try_clone()
        .with_context(|| format!("cloning {}", paths.logfile.display()))?;

    let mut command = Command::new(exe);
    command
        .args(["daemon", "__serve", "--env", env_name])
        .env("SNOW_ENV", env_name)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(log_err));
    if no_idle_timeout {
        command.arg("--no-idle-timeout");
    }

    #[cfg(unix)]
    unsafe {
        command.pre_exec(|| {
            nix::unistd::setsid()
                .map(|_| ())
                .map_err(std::io::Error::other)
        });
    }

    #[cfg(windows)]
    {
        command.creation_flags(DETACHED_PROCESS | CREATE_NEW_PROCESS_GROUP);
    }

    command.spawn().context("spawning detached daemon child")
}

fn wait_for_endpoint(paths: &DaemonPaths, child: &mut Child, timeout: Duration) -> Result<()> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        if endpoint_alive(paths) {
            return Ok(());
        }
        if let Some(status) = child.try_wait().context("inspecting daemon child")? {
            bail!("daemon child exited before endpoint was ready: {status}");
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    bail!("timeout waiting for daemon endpoint {}", paths.endpoint)
}

pub(crate) fn read_status_env(paths: &DaemonPaths) -> Option<String> {
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

pub(crate) fn read_pid(paths: &DaemonPaths) -> Result<Option<u32>> {
    match std::fs::read_to_string(&paths.pidfile) {
        Ok(s) => Ok(s.trim().parse().ok()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e.into()),
    }
}

pub(crate) fn scrub_stale_runtime_files(paths: &DaemonPaths) {
    let Ok(_lock) = lock_runtime_files(paths) else {
        return;
    };
    let _ = std::fs::remove_file(&paths.pidfile);
    let _ = std::fs::remove_file(&paths.statusfile);
}

fn rotate_log(paths: &DaemonPaths) -> Result<()> {
    if let Ok(meta) = std::fs::metadata(&paths.logfile)
        && meta.len() > 10 * 1024 * 1024
    {
        let _ = std::fs::rename(&paths.logfile, &paths.logfile_rotated);
    }
    Ok(())
}
