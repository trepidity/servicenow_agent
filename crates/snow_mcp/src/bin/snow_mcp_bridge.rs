use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Result, bail};
use snow_mcp::DaemonBackedMcpBridge;
use snow_mcp::daemon_bridge::{
    DaemonEndpoint, LocalSocketDaemonJsonRpcClient, ProcessDaemonAutoSpawn, default_daemon_endpoint,
};

const DEFAULT_CONTRACT_VERSION: &str = "daemon-json-rpc-v1";
const DEFAULT_DAEMON_ENV: &str = "prd";

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let args = Args::parse(std::env::args().skip(1))?;
    if args.help {
        print_help();
        return Ok(());
    }

    let config = bridge_config(&args.daemon_endpoint);
    let daemon = if args.auto_spawn {
        match resolve_snow_binary(args.snow_bin) {
            Some(snow_bin) => LocalSocketDaemonJsonRpcClient::with_auto_spawn(
                args.daemon_endpoint,
                ProcessDaemonAutoSpawn::new(snow_bin),
            ),
            None => LocalSocketDaemonJsonRpcClient::with_unavailable_auto_spawn(
                args.daemon_endpoint,
                "searched --snow-bin, SNOW_BIN, same-directory sibling, and PATH",
            ),
        }
    } else {
        LocalSocketDaemonJsonRpcClient::new(args.daemon_endpoint)
    };

    let bridge =
        DaemonBackedMcpBridge::new(std::sync::Arc::new(daemon), config, args.required_contract);
    bridge.serve_stdio().await?;
    Ok(())
}

#[derive(Debug)]
struct Args {
    daemon_endpoint: DaemonEndpoint,
    required_contract: String,
    snow_bin: Option<PathBuf>,
    auto_spawn: bool,
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
        let mut auto_spawn = true;
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
                "--no-auto-spawn" => auto_spawn = false,
                "-h" | "--help" => help = true,
                other => bail!("unknown argument: {other}"),
            }
        }

        Ok(Self {
            daemon_endpoint: daemon_endpoint.unwrap_or_else(default_daemon_endpoint),
            required_contract,
            snow_bin,
            auto_spawn,
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

fn bridge_config(endpoint: &DaemonEndpoint) -> snow_mcp::McpConfig {
    let mut config = snow_mcp::McpConfig::default();
    let label = bridge_environment(endpoint);
    config.environment =
        snow_mcp::McpEnvironment::snow_env(label.clone(), config.environment.instance_timezone);

    let policy_path = std::env::var_os("SNOW_MCP_POLICY_PATH")
        .map(PathBuf::from)
        .or_else(|| {
            daemon_env_file(endpoint)
                .and_then(|path| env_file_value(&path, "SNOW_MCP_POLICY_PATH").map(PathBuf::from))
        })
        .or_else(|| {
            bridge_env_file_for_label(&label)
                .and_then(|path| env_file_value(&path, "SNOW_MCP_POLICY_PATH").map(PathBuf::from))
        });
    if let Some(path) = policy_path {
        match fs::read_to_string(&path)
            .ok()
            .and_then(|input| snow_mcp::domain::policy::PolicyConfig::from_toml_str(&input).ok())
        {
            Some(policy) => {
                config.policy = policy;
            }
            None => {
                eprintln!(
                    "snow_mcp_bridge: failed to load MCP policy from {}",
                    path.display()
                );
            }
        }
    }

    config
}

fn bridge_environment(endpoint: &DaemonEndpoint) -> String {
    std::env::var("SNOW_ENV")
        .ok()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| daemon_status_value(endpoint, "environment"))
        .or_else(persisted_env_selection)
        .unwrap_or_else(|| DEFAULT_DAEMON_ENV.to_string())
}

fn persisted_env_selection() -> Option<String> {
    snow_core::ipc::resolve_config_dir()
        .ok()
        .and_then(|config_dir| fs::read_to_string(config_dir.join("env")).ok())
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn bridge_env_file_for_label(label: &str) -> Option<PathBuf> {
    bridge_env_file_candidates(label)
        .into_iter()
        .find(|path| path.exists())
}

fn bridge_env_file_candidates(label: &str) -> Vec<PathBuf> {
    let filename = format!(".env.{}", label.trim());
    let mut paths = Vec::new();

    if let Ok(exe) = std::env::current_exe()
        && let Some(dir) = exe.parent()
    {
        paths.push(dir.join(&filename));
    }
    if let Ok(config_dir) = snow_core::ipc::resolve_config_dir() {
        paths.push(config_dir.join(&filename));
        paths.push(config_dir.join(".env"));
    }
    paths.push(PathBuf::from(filename));

    paths
}

fn daemon_env_file(endpoint: &DaemonEndpoint) -> Option<PathBuf> {
    daemon_status_value(endpoint, "env_file").map(PathBuf::from)
}

fn daemon_status_value(endpoint: &DaemonEndpoint, field: &str) -> Option<String> {
    let status_path = endpoint.filesystem_path()?.parent()?.join("daemon.status");
    let input = fs::read_to_string(status_path).ok()?;
    let value: serde_json::Value = serde_json::from_str(&input).ok()?;
    value.get(field)?.as_str().map(ToOwned::to_owned)
}

fn env_file_value(path: &Path, key: &str) -> Option<String> {
    let input = fs::read_to_string(path).ok()?;
    input.lines().find_map(|line| {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            return None;
        }
        let (candidate, value) = line.split_once('=')?;
        (candidate.trim() == key).then(|| unquote_env_value(value.trim()))
    })
}

fn unquote_env_value(value: &str) -> String {
    value
        .strip_prefix('"')
        .and_then(|value| value.strip_suffix('"'))
        .or_else(|| {
            value
                .strip_prefix('\'')
                .and_then(|value| value.strip_suffix('\''))
        })
        .unwrap_or(value)
        .to_string()
}

fn print_help() {
    println!(
        "snow_mcp_bridge [--daemon-endpoint <NAME>] [--daemon-socket <PATH>] [--snow-bin <PATH>] [--no-auto-spawn] [--require-contract daemon-json-rpc-v1]\n\
         \n\
         Serves ServiceNow MCP over stdio by forwarding calls to snow_daemon JSON-RPC over a local socket. If the daemon is unavailable, the bridge tries to auto-spawn it via `snow daemon start` unless --no-auto-spawn is selected for an externally supervised daemon."
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
    fn parses_no_auto_spawn_for_externally_supervised_daemon() {
        let args = Args::parse(["--no-auto-spawn".to_string()]).expect("args parse");
        assert!(!args.auto_spawn);
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
