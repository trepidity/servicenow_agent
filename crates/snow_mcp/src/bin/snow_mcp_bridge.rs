use std::ffi::OsString;
use std::path::{Path, PathBuf};

use anyhow::{Result, bail};
use snow_mcp::DaemonBackedMcpBridge;
use snow_mcp::daemon_bridge::{
    DaemonEndpoint, LocalSocketDaemonJsonRpcClient, ProcessDaemonAutoSpawn, default_daemon_endpoint,
};

const DEFAULT_CONTRACT_VERSION: &str = "daemon-json-rpc-v1";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse(std::env::args().skip(1))?;
    if args.help {
        print_help();
        return Ok(());
    }

    let daemon = match resolve_snow_binary(args.snow_bin) {
        Some(snow_bin) => LocalSocketDaemonJsonRpcClient::with_auto_spawn(
            args.daemon_endpoint,
            ProcessDaemonAutoSpawn::new(snow_bin),
        ),
        None => LocalSocketDaemonJsonRpcClient::with_unavailable_auto_spawn(
            args.daemon_endpoint,
            "searched --snow-bin, SNOW_BIN, same-directory sibling, and PATH",
        ),
    };

    let bridge = DaemonBackedMcpBridge::new(
        std::sync::Arc::new(daemon),
        snow_mcp::McpConfig::default(),
        args.required_contract,
    );
    bridge.serve_stdio().await?;
    Ok(())
}

#[derive(Debug)]
struct Args {
    daemon_endpoint: DaemonEndpoint,
    required_contract: String,
    snow_bin: Option<PathBuf>,
    help: bool,
}

impl Args {
    fn parse<I>(args: I) -> Result<Self>
    where
        I: IntoIterator<Item = String>,
    {
        let mut daemon_endpoint = None;
        let mut required_contract = DEFAULT_CONTRACT_VERSION.to_string();
        let mut snow_bin = None;
        let mut help = false;
        let mut iter = args.into_iter();

        while let Some(arg) = iter.next() {
            match arg.as_str() {
                "--daemon-socket" | "--socket-path" => {
                    let Some(value) = iter.next() else {
                        bail!("{arg} requires a path");
                    };
                    daemon_endpoint = Some(DaemonEndpoint::filesystem(expand_home(&value)));
                }
                "--daemon-endpoint" => {
                    let Some(value) = iter.next() else {
                        bail!("--daemon-endpoint requires an endpoint name");
                    };
                    daemon_endpoint = Some(DaemonEndpoint::parse(&value));
                }
                "--require-contract" => {
                    let Some(value) = iter.next() else {
                        bail!("--require-contract requires a contract version");
                    };
                    required_contract = value;
                }
                "--snow-bin" => {
                    let Some(value) = iter.next() else {
                        bail!("--snow-bin requires a path");
                    };
                    snow_bin = Some(expand_home(&value));
                }
                "-h" | "--help" => help = true,
                other => bail!("unknown argument: {other}"),
            }
        }

        Ok(Self {
            daemon_endpoint: daemon_endpoint.unwrap_or_else(default_daemon_endpoint),
            required_contract,
            snow_bin,
            help,
        })
    }
}

fn expand_home(value: &str) -> PathBuf {
    if value == "~" {
        return std::env::var_os("HOME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(value));
    }
    if let Some(rest) = value.strip_prefix("~/")
        && let Some(home) = std::env::var_os("HOME")
    {
        return PathBuf::from(home).join(rest);
    }
    PathBuf::from(value)
}

fn resolve_snow_binary(explicit: Option<PathBuf>) -> Option<PathBuf> {
    resolve_snow_binary_from(
        explicit,
        std::env::var_os("SNOW_BIN").map(PathBuf::from),
        std::env::current_exe().ok(),
        std::env::var_os("PATH"),
        |path| path.is_file(),
    )
}

fn resolve_snow_binary_from<F>(
    explicit: Option<PathBuf>,
    env_bin: Option<PathBuf>,
    current_exe: Option<PathBuf>,
    path_var: Option<OsString>,
    file_exists: F,
) -> Option<PathBuf>
where
    F: Fn(&Path) -> bool,
{
    if let Some(path) = explicit.and_then(non_empty_path) {
        return Some(path);
    }
    if let Some(path) = env_bin.and_then(non_empty_path) {
        return Some(path);
    }

    if let Some(exe) = current_exe
        && let Some(dir) = exe.parent()
    {
        let sibling = dir.join(snow_exe_name());
        if file_exists(&sibling) {
            return Some(sibling);
        }
    }

    path_var.and_then(|paths| {
        std::env::split_paths(&paths)
            .map(|dir| dir.join(snow_exe_name()))
            .find(|candidate| file_exists(candidate))
    })
}

fn non_empty_path(path: PathBuf) -> Option<PathBuf> {
    (!path.as_os_str().is_empty()).then_some(path)
}

fn snow_exe_name() -> String {
    format!("snow{}", std::env::consts::EXE_SUFFIX)
}

fn print_help() {
    println!(
        "snow_mcp_bridge [--daemon-endpoint <NAME>] [--daemon-socket <PATH>] [--snow-bin <PATH>] [--require-contract daemon-json-rpc-v1]\n\
         \n\
         Serves ServiceNow MCP over stdio by forwarding calls to snow_daemon JSON-RPC over a local socket. If the daemon is unavailable, the bridge tries to auto-spawn it via `snow daemon start`."
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_legacy_daemon_socket_as_filesystem_endpoint() {
        let args = Args::parse(["--daemon-socket".to_string(), "/tmp/snow.sock".to_string()])
            .expect("args parse");
        assert_eq!(
            args.daemon_endpoint,
            DaemonEndpoint::filesystem(PathBuf::from("/tmp/snow.sock"))
        );
    }

    #[test]
    fn parses_daemon_endpoint_as_namespaced_endpoint() {
        let args = Args::parse([
            "--daemon-endpoint".to_string(),
            "snow-daemon-test".to_string(),
        ])
        .expect("args parse");
        assert_eq!(
            args.daemon_endpoint,
            DaemonEndpoint::namespaced("snow-daemon-test")
        );
    }

    #[test]
    fn snow_binary_resolution_prefers_explicit_and_env() {
        let explicit = PathBuf::from("/custom/snow");
        let env_bin = PathBuf::from("/env/snow");
        let resolved = resolve_snow_binary_from(
            Some(explicit.clone()),
            Some(env_bin.clone()),
            None,
            None,
            |_| false,
        );
        assert_eq!(resolved, Some(explicit));

        let resolved = resolve_snow_binary_from(None, Some(env_bin.clone()), None, None, |_| false);
        assert_eq!(resolved, Some(env_bin));
    }

    #[test]
    fn snow_binary_resolution_falls_back_to_sibling_then_path() {
        let exe = PathBuf::from("/bridge/bin/snow_mcp_bridge");
        let sibling = PathBuf::from("/bridge/bin").join(snow_exe_name());
        let path_candidate = PathBuf::from("/usr/local/bin").join(snow_exe_name());
        let path_var = std::env::join_paths([PathBuf::from("/usr/local/bin")]).unwrap();

        let resolved = resolve_snow_binary_from(
            None,
            None,
            Some(exe.clone()),
            Some(path_var.clone()),
            |path| path == sibling,
        );
        assert_eq!(resolved, Some(sibling));

        let resolved = resolve_snow_binary_from(None, None, Some(exe), Some(path_var), |path| {
            path == path_candidate
        });
        assert_eq!(resolved, Some(path_candidate));
    }
}
