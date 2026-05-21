//! `snow daemon ...` subcommand implementations.
//!
//! Each lifecycle action lives in its own module; [`dispatch`] is the
//! single entry point invoked from `main.rs`.

// `paths` is platform-neutral and used by the admin TUI and config helpers, so
// it stays compiled everywhere. The lifecycle modules below rely on Unix
// sockets and fork/setsid, so they (and `dispatch`) are Unix-only.
pub mod paths;

#[cfg(unix)]
pub mod logs;
#[cfg(unix)]
pub mod start;
#[cfg(unix)]
pub mod status;
#[cfg(unix)]
pub mod stop;

#[cfg(unix)]
use anyhow::Result;

#[cfg(unix)]
use crate::cli::DaemonCommand;

/// Route a parsed [`DaemonCommand`] to the appropriate module's `run` fn.
#[cfg(unix)]
pub fn dispatch(action: DaemonCommand, env_name: &str) -> Result<()> {
    match action {
        DaemonCommand::Start => start::run(env_name),
        DaemonCommand::Stop => stop::run(),
        DaemonCommand::Restart => {
            // Best-effort stop, then start fresh. Errors from stop are
            // intentionally swallowed: stop is idempotent and a missing
            // daemon should not block restart.
            let _ = stop::run();
            start::run(env_name)
        }
        DaemonCommand::Status => status::run(),
        DaemonCommand::Logs { follow, lines } => logs::run(follow, lines),
    }
}
