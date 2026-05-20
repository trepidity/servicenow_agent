//! `snow daemon status` — read-only health probe.
//!
//! Reports one of four states based on the pidfile and socket connectability:
//!  - `running`     — pidfile present and socket connects
//!  - `unreachable` — pidfile present but socket does not respond, or socket
//!    responds without a pidfile (something is half-up)
//!  - `stopped`     — neither pidfile nor socket
//!
//! Also prints metadata from the JSON statusfile (version, started_at) when
//! available.

use std::os::unix::net::UnixStream;

use anyhow::Result;
use serde::Deserialize;

use super::paths::DaemonPaths;

#[derive(Debug, Deserialize)]
struct StatusFile {
    #[allow(dead_code)]
    pid: i32,
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
        .and_then(|s| s.trim().parse::<i32>().ok());

    let status: Option<StatusFile> = std::fs::read_to_string(&paths.statusfile)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok());

    let socket_alive = UnixStream::connect(&paths.socket).is_ok();

    match (pid, socket_alive) {
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
                "running\n  pid: {pid}\n  env: {env}\n  version: {v}\n  started: {started}\n  socket: {}",
                paths.socket.display()
            );
        }
        (Some(pid), false) => {
            println!(
                "unreachable\n  pid: {pid} (alive but socket {} not connectable)",
                paths.socket.display()
            );
        }
        (None, true) => {
            println!(
                "unreachable\n  no pidfile but socket {} responds",
                paths.socket.display()
            );
        }
        (None, false) => {
            println!("stopped");
        }
    }
    Ok(())
}
