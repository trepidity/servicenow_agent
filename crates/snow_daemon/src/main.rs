//! `snow_daemon` binary entrypoint — a thin wrapper around
//! [`snow_daemon::run_blocking`].
//!
//! All daemon construction and async-runtime setup lives in `lib.rs` so the
//! function can be invoked directly by `snow daemon start` after forking.
//! This binary keeps the historical direct-launch behavior: it resolves the
//! socket path (overridable via `SNOW_DAEMON_SOCKET`) and hands control to
//! `run_blocking`.

use std::path::PathBuf;

use anyhow::Result;
use clap::Parser;

#[derive(Debug, Parser)]
#[command(name = "snow_daemon")]
struct Args {
    /// Disable the MCP stdio transport and serve only JSON-RPC.
    #[arg(long)]
    rpc_only: bool,

    /// Unix socket path for JSON-RPC. Defaults to SNOW_DAEMON_SOCKET or ~/.config/snow/daemon.sock.
    #[arg(long, value_name = "PATH")]
    socket_path: Option<PathBuf>,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let socket = args
        .socket_path
        .or_else(|| std::env::var("SNOW_DAEMON_SOCKET").ok().map(PathBuf::from))
        .unwrap_or_else(snow_daemon::default_socket_path);

    if args.rpc_only {
        snow_daemon::run_blocking_rpc_only(&socket)
    } else {
        snow_daemon::run_blocking(&socket)
    }
}
