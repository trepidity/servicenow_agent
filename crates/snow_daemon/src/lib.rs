#![allow(clippy::arc_with_non_send_sync)]

//! Library entry for `snow_daemon`.
//!
//! `main.rs` is now a thin wrapper that resolves the socket path and calls
//! [`run_blocking`]. All shared types ([`DaemonState`], [`DaemonConfig`]) and
//! modules ([`jobs`], [`rpc`], [`transport`]) live here so the
//! `jobs` module can reference `crate::DaemonState` without forcing a
//! circular `crate::main` import, and so external callers (such as
//! `snow daemon start`) can spawn the daemon in-process.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use servicenow_rs::prelude::{BasicAuth, ServiceNowClient};
use snow_core::{
    SnowCore,
    config::{self as core_config, SnowConfig},
    credential::CredentialProvider,
};

pub mod jobs;
pub mod rpc;
pub mod story_write;
pub mod transport;

#[cfg(test)]
pub mod test_support;

use crate::jobs::JobRegistry;

/// Static configuration captured at daemon startup.
#[derive(Debug, Clone)]
pub struct DaemonConfig {
    /// Filesystem path of the JSON-RPC Unix socket.
    pub socket_path: PathBuf,
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
}

impl DaemonState {
    /// Construct a new shared state from a built `SnowCore`. Initializes a
    /// fresh `JobRegistry` for this daemon process.
    pub fn new(core: Arc<SnowCore>) -> Self {
        let data_dir = daemon_data_dir(core.config().daemon.socket_path.as_path());
        let mcp_config = mcp_config_from_env();
        Self {
            core,
            jobs: Arc::new(JobRegistry::new()),
            data_dir,
            mcp_config,
        }
    }

    #[cfg(test)]
    pub fn with_data_dir(core: Arc<SnowCore>, data_dir: PathBuf) -> Self {
        Self {
            core,
            jobs: Arc::new(JobRegistry::new()),
            data_dir,
            mcp_config: snow_mcp::McpConfig::default(),
        }
    }

    #[cfg(test)]
    pub fn with_data_dir_and_mcp_config(
        core: Arc<SnowCore>,
        data_dir: PathBuf,
        mcp_config: snow_mcp::McpConfig,
    ) -> Self {
        Self {
            core,
            jobs: Arc::new(JobRegistry::new()),
            data_dir,
            mcp_config,
        }
    }
}

fn daemon_data_dir(socket_path: &Path) -> PathBuf {
    if let Some(parent) = socket_path.parent()
        && !parent.as_os_str().is_empty()
    {
        return parent.to_path_buf();
    }

    if let Some(home) = std::env::var_os("HOME") {
        return PathBuf::from(home).join(".config/snow");
    }

    std::env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .join(".snow")
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
        let rpc_server = rpc::JsonRpcServer::new(Arc::clone(&self.state), self.config.socket_path);

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
            config.environment.label = "test".to_string();
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
/// This is the entry point used both by the standalone `snow_daemon` binary
/// and by `snow daemon start` (which forks and then calls this in the child).
/// The function owns its own `current_thread` tokio runtime and `LocalSet`
/// because `SnowCore` is `!Send`.
///
/// Loads `.env` from the current directory plus the selected snow config env
/// file, builds [`SnowCore`] from environment variables (`SNOW_INSTANCE`,
/// `SNOW_USER`, and the shared credential provider precedence), constructs a
/// [`DaemonState`], and launches the RPC + MCP servers.
pub fn run_blocking(socket: &Path) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        load_daemon_env();
        let config = DaemonConfig {
            socket_path: socket.to_path_buf(),
            mcp_transport: Some(snow_mcp::McpTransport::Stdio),
        };
        let core = Arc::new(build_core(&config).await?);
        let state = Arc::new(DaemonState::new(core));
        let runtime = DaemonRuntime::new(config, state);
        tokio::task::LocalSet::new().run_until(runtime.run()).await
    })
}

/// Like [`run_blocking`] but disables the MCP stdio transport. Used when the
/// daemon is started detached (no stdio attached) by `snow daemon start`.
pub fn run_blocking_rpc_only(socket: &Path) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async move {
        load_daemon_env();
        let config = DaemonConfig {
            socket_path: socket.to_path_buf(),
            mcp_transport: None,
        };
        let core = Arc::new(build_core(&config).await?);
        let state = Arc::new(DaemonState::new(core));
        let runtime = DaemonRuntime::new(config, state);
        tokio::task::LocalSet::new().run_until(runtime.run()).await
    })
}

fn load_daemon_env() {
    let default_env_path = dotenvy::dotenv().ok();

    let env_name = std::env::var("SNOW_ENV").unwrap_or_else(|_| "test".to_string());
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
    if let Some(home) = std::env::var_os("HOME") {
        let config_dir = PathBuf::from(home).join(".config/snow");
        paths.push(config_dir.join(&filename));
        paths.push(config_dir.join(".env"));
    }
    paths.push(PathBuf::from(filename));

    paths
}

async fn build_core(daemon_config: &DaemonConfig) -> Result<SnowCore> {
    let instance =
        std::env::var("SNOW_INSTANCE").or_else(|_| std::env::var("SERVICENOW_INSTANCE"))?;
    let username = std::env::var("SNOW_USER").or_else(|_| std::env::var("SERVICENOW_USERNAME"))?;
    let credential = CredentialProvider::from_runtime_env();
    let password = credential.resolve()?;

    let client = ServiceNowClient::builder()
        .instance(&instance)
        .auth(BasicAuth::new(&username, &password))
        .build()
        .await?;

    let mut config = SnowConfig {
        instance: core_config::InstanceConfig {
            url: instance,
            user: username,
            credential,
        },
        vault: core_config::VaultConfig {
            path: default_vault_path(),
        },
        daemon: core_config::DaemonConfig {
            socket_path: daemon_config.socket_path.clone(),
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
        .vault_path(config.vault.path)
        .build()
        .await
}

fn default_vault_path() -> PathBuf {
    if let Some(home) = std::env::var_os("HOME") {
        return PathBuf::from(home).join(".config/snow/vault");
    }
    PathBuf::from("./vault")
}

/// Compute the default Unix socket path for the daemon.
///
/// Uses `$HOME/.config/snow/daemon.sock` when `HOME` is set; otherwise falls
/// back to `./daemon.sock` in the current working directory.
pub fn default_socket_path() -> PathBuf {
    if let Some(home) = std::env::var_os("HOME") {
        return PathBuf::from(home).join(".config/snow/daemon.sock");
    }
    PathBuf::from("./daemon.sock")
}
