//! `snow daemon stop` — graceful shutdown via SIGTERM, with SIGKILL fallback.
//!
//! Reads the pidfile, sends SIGTERM, then polls every 100 ms for up to 10 s
//! waiting for the process to exit. If it doesn't, escalates to SIGKILL.
//! In all success paths, removes the pidfile and statusfile so the next
//! `status` returns `stopped`.

use std::time::{Duration, Instant};

use anyhow::Result;
use nix::sys::signal::Signal;

use super::paths::DaemonPaths;
use super::start::{process_alive, send_signal};

/// Run `snow daemon stop`. Idempotent: if the daemon is not running,
/// prints `not running` and returns Ok.
pub fn run() -> Result<()> {
    let paths = DaemonPaths::resolve()?;
    let pid = match std::fs::read_to_string(&paths.pidfile)
        .ok()
        .and_then(|s| s.trim().parse::<i32>().ok())
    {
        Some(p) => p,
        None => {
            println!("not running");
            return Ok(());
        }
    };

    let _ = send_signal(pid, Signal::SIGTERM);

    let deadline = Instant::now() + Duration::from_secs(10);
    while Instant::now() < deadline {
        if !process_alive(pid) {
            let _ = std::fs::remove_file(&paths.pidfile);
            let _ = std::fs::remove_file(&paths.statusfile);
            println!("stopped, pid {pid}");
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(100));
    }

    eprintln!("daemon did not exit on SIGTERM, escalating to SIGKILL");
    let _ = send_signal(pid, Signal::SIGKILL);
    let _ = std::fs::remove_file(&paths.pidfile);
    let _ = std::fs::remove_file(&paths.statusfile);
    println!("killed, pid {pid}");
    Ok(())
}
