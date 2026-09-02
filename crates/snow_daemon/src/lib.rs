#![allow(clippy::arc_with_non_send_sync)]

//! Library entry for `snow_daemon`.
//!
//! `main.rs` is now a thin wrapper that resolves the endpoint path and calls
//! [`run_blocking`]. All shared types ([`DaemonState`], [`DaemonConfig`]) and
//! modules ([`jobs`], [`rpc`], [`transport`]) live here so the
//! `jobs` module can reference `crate::DaemonState` without forcing a
//! circular `crate::main` import, and so external callers can host the daemon
//! runtime without duplicating setup.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use servicenow_rs::prelude::{BasicAuth, ServiceNowClient};
use snow_core::{
    SnowCore,
    cache::policy::CachePolicyManager,
    config::{self as core_config, SnowConfig},
    credential::CredentialProvider,
    ipc::IpcEndpoint,
};

pub mod catalog_write;
pub mod change_write;
pub mod incident_bulk_write;
pub mod jobs;
pub mod knowledge_write;
pub mod resource_plan_write;
pub mod rpc;
pub mod story_write;
pub mod timecard_write;
pub mod transport;
pub mod work_note_write;

#[cfg(test)]
pub mod test_support;

use crate::jobs::JobRegistry;

pub(crate) const DEFAULT_DAEMON_ENV: &str = "prd";

/// Static configuration captured at daemon startup.
#[derive(Debug, Clone)]
pub struct DaemonConfig {
    /// Portable local-socket endpoint for JSON-RPC IPC.
    pub endpoint: IpcEndpoint,
    /// Directory for daemon-owned files. This is separate from the endpoint
    /// because Windows named pipes have no filesystem parent.
    pub data_dir: PathBuf,
    /// Selected MCP transport (`None` disables MCP entirely; rpc-only mode).
    pub mcp_transport: Option<snow_mcp::McpTransport>,
}

/// Shared daemon state, reference-counted and handed to every server task,
/// RPC handler, and background worker.
#[derive(Clone)]
pub struct DaemonState {
    /// The shared `SnowCore` runtime — vault, cache, ServiceNow client.
    pub core: Arc<SnowCore>,
    /// Registry of in-flight and recently-finished background jobs. Workers
    /// publish progress / log lines / status here; RPC handlers (added in
    /// Task 7) read from it to expose `start_job` / `get_job` / `list_jobs`
    /// / `cancel_job`.
    pub jobs: Arc<JobRegistry>,
    /// Directory for daemon-owned durable MCP planning state.
    pub data_dir: PathBuf,
    /// MCP governance configuration used by daemon-owned plan/apply handlers.
    pub mcp_config: snow_mcp::McpConfig,
    /// Atomically replaceable cache-policy snapshot captured from the fixed
    /// daemon configuration directory.
    pub cache_policy: Arc<CachePolicyManager>,
}

impl DaemonState {
    /// Construct a new shared state from a built `SnowCore`. Initializes a
    /// fresh `JobRegistry` for this daemon process.
    pub fn new(core: Arc<SnowCore>) -> Self {
        let data_dir = daemon_data_dir(core.config().daemon.socket_path.as_path());
        Self::new_with_data_dir(core, data_dir, mcp_config_from_env())
    }

    fn new_with_data_dir(
        core: Arc<SnowCore>,
        data_dir: PathBuf,
        mcp_config: snow_mcp::McpConfig,
    ) -> Self {
        let cache_policy = core.cache_policy_manager();
        Self::new_with_data_dir_and_policy(core, data_dir.clone(), mcp_config, cache_policy)
    }

    fn new_with_data_dir_and_policy(
        core: Arc<SnowCore>,
        data_dir: PathBuf,
        mcp_config: snow_mcp::McpConfig,
        cache_policy: Arc<CachePolicyManager>,
    ) -> Self {
        Self {
            core,
            jobs: Arc::new(JobRegistry::new()),
            data_dir,
            mcp_config,
            cache_policy,
        }
    }

    #[cfg(test)]
    pub fn with_data_dir(core: Arc<SnowCore>, data_dir: PathBuf) -> Self {
        let cache_policy = core.cache_policy_manager();
        Self {
            core,
            jobs: Arc::new(JobRegistry::new()),
            data_dir,
            mcp_config: snow_mcp::McpConfig::default(),
            cache_policy,
        }
    }

    #[cfg(test)]
    pub fn with_data_dir_and_mcp_config(
        core: Arc<SnowCore>,
        data_dir: PathBuf,
        mcp_config: snow_mcp::McpConfig,
    ) -> Self {
        let cache_policy = core.cache_policy_manager();
        Self {
            core,
            jobs: Arc::new(JobRegistry::new()),
            data_dir,
            mcp_config,
            cache_policy,
        }
    }
}

fn daemon_data_dir(socket_path: &Path) -> PathBuf {
    data_dir_from_socket_path(socket_path)
}

/// Daemon runtime: owns the configuration and shared state, and runs the
/// JSON-RPC server (and optional MCP stdio server) until shutdown.
#[derive(Clone)]
struct DaemonRuntime {
    config: DaemonConfig,
    state: Arc<DaemonState>,
}

impl DaemonRuntime {
    fn new(config: DaemonConfig, state: Arc<DaemonState>) -> Self {
        Self { config, state }
    }

    async fn run(self) -> Result<()> {
        let rpc_server = rpc::JsonRpcServer::new(Arc::clone(&self.state), self.config.endpoint)
            .with_idle_timeout(idle_timeout_from_env());

        match self.config.mcp_transport {
            Some(transport) => {
                let mcp_server = snow_mcp::McpServer::with_config(
                    Arc::clone(&self.state.core),
                    self.state.mcp_config.clone(),
                    transport,
                );
                tokio::try_join!(rpc_server.serve(), async move {
                    mcp_server.serve().await.map_err(anyhow::Error::from)
                })?;
                Ok(())
            }
            None => rpc_server.serve().await,
        }
    }
}

fn mcp_config_from_env() -> snow_mcp::McpConfig {
    let mut config = snow_mcp::McpConfig::default();
    match std::env::var("SNOW_ENV") {
        Ok(label) => {
            config.environment =
                snow_mcp::McpEnvironment::snow_env(label, config.environment.instance_timezone);
        }
        Err(_) => {
            config.environment.label = DEFAULT_DAEMON_ENV.to_string();
        }
    }

    if let Ok(path) = std::env::var("SNOW_MCP_POLICY_PATH") {
        match std::fs::read_to_string(&path)
            .ok()
            .and_then(|input| snow_mcp::domain::policy::PolicyConfig::from_toml_str(&input).ok())
        {
            Some(policy) => {
                config.policy = policy;
            }
            None => {
                eprintln!("snow_daemon: failed to load MCP policy from {path}");
            }
        }
    }

    config
}

/// Run the daemon synchronously, blocking the calling thread until shutdown.
///
/// This is the entry point used by the standalone `snow_daemon` binary and the
/// hidden detached CLI child entrypoint. The function owns its own
/// `current_thread` tokio runtime and `LocalSet` because `SnowCore` is `!Send`.
///
/// Loads `.env` from the current directory plus the selected snow config env
/// file, builds [`SnowCore`] from environment variables (`SERVICENOW_INSTANCE`,
/// `SERVICENOW_USERNAME`, and the shared credential provider precedence),
/// constructs a [`DaemonState`], and launches the RPC + MCP servers.
pub fn run_blocking(socket: &Path) -> Result<()> {
    let config = daemon_config_from_socket_path(socket, Some(snow_mcp::McpTransport::Stdio));
    run_blocking_with_config(config)
}

/// Like [`run_blocking`] but disables the MCP stdio transport. Used when the
/// daemon is started detached (no stdio attached) by `snow daemon start`.
pub fn run_blocking_rpc_only(socket: &Path) -> Result<()> {
    let config = daemon_config_from_socket_path(socket, None);
    run_blocking_with_config(config)
}

pub fn run_blocking_with_endpoint(
    endpoint: IpcEndpoint,
    data_dir: PathBuf,
    mcp_transport: Option<snow_mcp::McpTransport>,
) -> Result<()> {
    run_blocking_with_config(DaemonConfig {
        endpoint,
        data_dir,
        mcp_transport,
    })
}

/// Resolve the daemon idle-shutdown window from a raw `SNOW_DAEMON_IDLE_SECS`
/// value. An unset/blank/unparseable value falls back to the default window;
/// an explicit `0` disables idle shutdown (a pinned daemon).
fn parse_idle_timeout(raw: Option<&str>) -> Option<std::time::Duration> {
    match raw.map(str::trim) {
        Some(value) if !value.is_empty() => match value.parse::<u64>() {
            Ok(0) => None,
            Ok(secs) => Some(std::time::Duration::from_secs(secs)),
            Err(_) => Some(rpc::DEFAULT_IDLE_TIMEOUT),
        },
        _ => Some(rpc::DEFAULT_IDLE_TIMEOUT),
    }
}

/// Resolve the idle-shutdown window from the process environment.
fn idle_timeout_from_env() -> Option<std::time::Duration> {
    parse_idle_timeout(std::env::var("SNOW_DAEMON_IDLE_SECS").ok().as_deref())
}

fn run_blocking_with_config(config: DaemonConfig) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        load_daemon_env();
        let core = Arc::new(build_core(&config).await?);
        let cache_policy = core.cache_policy_manager();
        let state = Arc::new(DaemonState::new_with_data_dir_and_policy(
            core,
            config.data_dir.clone(),
            mcp_config_from_env(),
            cache_policy,
        ));
        let runtime = DaemonRuntime::new(config, state);
        tokio::task::LocalSet::new().run_until(runtime.run()).await
    })
}

fn daemon_config_from_socket_path(
    socket_path: &Path,
    mcp_transport: Option<snow_mcp::McpTransport>,
) -> DaemonConfig {
    let data_dir = data_dir_from_socket_path(socket_path);
    let endpoint = endpoint_from_socket_path(socket_path, &data_dir);

    DaemonConfig {
        endpoint,
        data_dir,
        mcp_transport,
    }
}

fn endpoint_from_socket_path(socket_path: &Path, _data_dir: &Path) -> IpcEndpoint {
    #[cfg(windows)]
    {
        let _ = socket_path;
        IpcEndpoint::for_config_dir(_data_dir)
    }

    #[cfg(not(windows))]
    {
        IpcEndpoint::Filesystem {
            path: socket_path.to_path_buf(),
        }
    }
}

fn data_dir_from_socket_path(socket_path: &Path) -> PathBuf {
    socket_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .map(Path::to_path_buf)
        .unwrap_or_else(default_config_dir)
}

fn load_daemon_env() {
    let default_env_path = dotenvy::dotenv().ok();

    let env_name = std::env::var("SNOW_ENV").unwrap_or_else(|_| DEFAULT_DAEMON_ENV.to_string());
    let mut loaded_path = None;
    for path in daemon_env_paths(&env_name) {
        if path.exists() {
            let _ = dotenvy::from_path(&path);
            loaded_path = Some(path);
            break;
        }
    }
    let loaded_path = loaded_path.or(default_env_path);

    match loaded_path {
        Some(path) => eprintln!(
            "snow_daemon loaded environment: env={env_name} file={}",
            path.display()
        ),
        None => eprintln!("snow_daemon loaded environment: env={env_name} file=(none)"),
    }
}

fn daemon_env_paths(env_name: &str) -> Vec<PathBuf> {
    let filename = format!(".env.{env_name}");
    let mut paths = Vec::new();

    if let Ok(exe) = std::env::current_exe()
        && let Some(dir) = exe.parent()
    {
        paths.push(dir.join(&filename));
    }
    let config_dir = default_config_dir();
    paths.push(config_dir.join(&filename));
    paths.push(config_dir.join(".env"));
    paths.push(PathBuf::from(filename));

    paths
}

async fn build_core(daemon_config: &DaemonConfig) -> Result<SnowCore> {
    let instance =
        std::env::var("SERVICENOW_INSTANCE").or_else(|_| std::env::var("SNOW_INSTANCE"))?;
    let username = snow_core::credential::resolve_username_from_runtime_env()?;
    let credential = CredentialProvider::from_runtime_env();
    let password = credential.resolve()?;
    let auth = BasicAuth::new(&username, password.as_str()).without_session();
    let write_auth = BasicAuth::new(&username, password.as_str()).without_session();
    let metadata_password = password.clone();
    drop(password);

    let client = ServiceNowClient::builder()
        .instance(&instance)
        .auth(auth)
        .build()
        .await?;
    let write_client = ServiceNowClient::builder()
        .instance(&instance)
        .auth(write_auth)
        .max_retries(0)
        .build()
        .await?;

    let mut config = SnowConfig {
        instance: core_config::InstanceConfig {
            url: instance,
            user: username.clone(),
            credential,
            portal: std::env::var("SNOW_PORTAL").unwrap_or_default(),
        },
        vault: core_config::VaultConfig {
            path: default_vault_path(),
        },
        daemon: core_config::DaemonConfig {
            socket_path: daemon_config
                .endpoint
                .filesystem_path()
                .map(Path::to_path_buf)
                .unwrap_or_else(|| daemon_config.data_dir.join("daemon.sock")),
            mcp_transport: if daemon_config.mcp_transport.is_some() {
                "stdio".to_string()
            } else {
                "disabled".to_string()
            },
        },
        ..Default::default()
    };
    config.apply_defaults();

    SnowCore::builder()
        .config(config.clone())
        .client(client)
        .write_client(write_client)
        .ui_metadata_basic_auth(username, metadata_password)
        .vault_path(config.vault.path)
        .build()
        .await
}

fn default_vault_path() -> PathBuf {
    default_config_dir().join("vault")
}

/// Compute the default legacy socket path for the daemon.
///
/// On Unix this is the filesystem socket path. On Windows it is only used to
/// derive the config directory and named-pipe scope.
pub fn default_socket_path() -> PathBuf {
    default_config_dir().join("daemon.sock")
}

fn default_config_dir() -> PathBuf {
    snow_core::ipc::resolve_config_dir().unwrap_or_else(|_| {
        std::env::current_dir()
            .unwrap_or_else(|_| PathBuf::from("."))
            .join(".snow")
    })
}

#[cfg(test)]
mod idle_timeout_tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn unset_uses_default() {
        assert_eq!(parse_idle_timeout(None), Some(rpc::DEFAULT_IDLE_TIMEOUT));
    }

    #[test]
    fn zero_disables() {
        assert_eq!(parse_idle_timeout(Some("0")), None);
    }

    #[test]
    fn positive_value_is_seconds() {
        assert_eq!(
            parse_idle_timeout(Some("90")),
            Some(Duration::from_secs(90))
        );
    }

    #[test]
    fn blank_or_garbage_uses_default() {
        assert_eq!(
            parse_idle_timeout(Some("  ")),
            Some(rpc::DEFAULT_IDLE_TIMEOUT)
        );
        assert_eq!(
            parse_idle_timeout(Some("nope")),
            Some(rpc::DEFAULT_IDLE_TIMEOUT)
        );
    }
}
