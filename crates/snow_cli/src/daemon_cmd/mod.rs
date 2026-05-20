//! `snow daemon ...` subcommand implementations.
//!
//! Each lifecycle action lives in its own module; [`dispatch`] is the
//! single entry point invoked from `main.rs`.

pub mod logs;
pub mod paths;
pub mod start;
pub mod status;
pub mod stop;

use anyhow::Result;

use crate::cli::DaemonCommand;

/// Route a parsed [`DaemonCommand`] to the appropriate module's `run` fn.
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
