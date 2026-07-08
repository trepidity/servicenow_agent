//! `snow daemon status` — read-only health probe.
//!
//! Reports state based primarily on endpoint connectability, with the pidfile
//! retained as secondary metadata:
//!  - `running`     — pidfile present and endpoint connects
//!  - `running`     — endpoint connects but pidfile is missing (metadata degraded)
//!  - `unreachable` — pidfile present but endpoint does not respond
//!  - `stopped`     — neither pidfile nor endpoint
//!
//! Also prints metadata from the JSON statusfile (version, started_at) when
//! available.

use anyhow::Result;
use serde::Deserialize;

use super::client::endpoint_alive;
use super::paths::DaemonPaths;

#[derive(Debug, Deserialize)]
struct StatusFile {
    #[allow(dead_code)]
    pid: u32,
    started_at: String,
    version: String,
    environment: Option<String>,
    #[allow(dead_code)]
    env_file: Option<String>,
    #[allow(dead_code)]
    socket: String,
}

/// Run `snow daemon status` and print the result to stdout.
pub fn run() -> Result<()> {
    let paths = DaemonPaths::resolve()?;

    let pid = std::fs::read_to_string(&paths.pidfile)
        .ok()
        .and_then(|s| s.trim().parse::<u32>().ok());

    let status: Option<StatusFile> = std::fs::read_to_string(&paths.statusfile)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok());

    let endpoint_alive = endpoint_alive(&paths);

    match (pid, endpoint_alive) {
        (Some(pid), true) => {
            let v = status.as_ref().map(|s| s.version.as_str()).unwrap_or("?");
            let started = status
                .as_ref()
                .map(|s| s.started_at.as_str())
                .unwrap_or("?");
            let env = status
                .as_ref()
                .and_then(|s| s.environment.as_deref())
                .unwrap_or("?");
            println!(
                "running\n  pid: {pid}\n  env: {env}\n  version: {v}\n  started: {started}\n  endpoint: {}",
                paths.endpoint
            );
        }
        (Some(pid), false) => {
            println!(
                "unreachable\n  pid: {pid} (alive but endpoint {} not connectable)",
                paths.endpoint
            );
        }
        (None, true) => {
            let v = status.as_ref().map(|s| s.version.as_str()).unwrap_or("?");
            let started = status
                .as_ref()
                .map(|s| s.started_at.as_str())
                .unwrap_or("?");
            let env = status
                .as_ref()
                .and_then(|s| s.environment.as_deref())
                .unwrap_or("?");
            println!(
                "running\n  pid: ? (missing pidfile)\n  env: {env}\n  version: {v}\n  started: {started}\n  endpoint: {}\n  warning: runtime metadata is incomplete",
                paths.endpoint
            );
        }
        (None, false) => {
            println!("stopped");
        }
    }
    Ok(())
}
