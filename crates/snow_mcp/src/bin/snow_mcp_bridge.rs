use std::path::PathBuf;

use anyhow::{Result, bail};
use snow_mcp::DaemonBackedMcpBridge;

const DEFAULT_CONTRACT_VERSION: &str = "daemon-json-rpc-v1";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse(std::env::args().skip(1))?;
    if args.help {
        print_help();
        return Ok(());
    }

    let bridge = DaemonBackedMcpBridge::new(
        std::sync::Arc::new(snow_mcp::UnixDaemonJsonRpcClient::new(args.daemon_socket)),
        snow_mcp::McpConfig::default(),
        args.required_contract,
    );
    bridge.serve_stdio().await?;
    Ok(())
}

#[derive(Debug)]
struct Args {
    daemon_socket: PathBuf,
    required_contract: String,
    help: bool,
}

impl Args {
    fn parse<I>(args: I) -> Result<Self>
    where
        I: IntoIterator<Item = String>,
    {
        let mut daemon_socket = None;
        let mut required_contract = DEFAULT_CONTRACT_VERSION.to_string();
        let mut help = false;
        let mut iter = args.into_iter();

        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--daemon-socket" | "--socket-path" => {
                    let Some(value) = iter.next() else {
                        bail!("{arg} requires a path");
                    };
                    daemon_socket = Some(PathBuf::from(expand_home(&value)));
                }
                "--require-contract" => {
                    let Some(value) = iter.next() else {
                        bail!("--require-contract requires a contract version");
                    };
                    required_contract = value;
                }
                "-h" | "--help" => help = true,
                other => bail!("unknown argument: {other}"),
            }
        }

        Ok(Self {
            daemon_socket: daemon_socket.unwrap_or_else(default_socket_path),
            required_contract,
            help,
        })
    }
}

fn default_socket_path() -> PathBuf {
    std::env::var_os("SNOW_DAEMON_SOCKET")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            let home = std::env::var_os("HOME")
                .map(PathBuf::from)
                .unwrap_or_else(|| PathBuf::from("."));
            home.join(".config/snow/daemon.sock")
        })
}

fn expand_home(value: &str) -> PathBuf {
    if value == "~" {
        return std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(value));
    }
    if let Some(rest) = value.strip_prefix("~/") {
        if let Some(home) = std::env::var_os("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }
    PathBuf::from(value)
}

fn print_help() {
    println!(
        "snow_mcp_bridge --daemon-socket <PATH> [--require-contract daemon-json-rpc-v1]\n\
         \n\
         Serves ServiceNow MCP over stdio by forwarding read/discovery calls to an already-running snow_daemon JSON-RPC socket."
    );
}
