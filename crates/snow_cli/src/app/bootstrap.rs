use super::*;

pub(super) struct AuthContext {
    pub(super) env_name: String,
    pub(super) instance: String,
    pub(super) username: String,
    pub(super) credential: auth::CredentialProvider,
    pub(super) password: auth::SecretString,
}

#[derive(Debug, Clone)]
pub(super) struct RuntimePaths {
    pub(super) root: PathBuf,
    pub(super) vault: PathBuf,
    pub(super) database: PathBuf,
    pub(super) socket: PathBuf,
    pub(super) endpoint: snow_core::ipc::IpcEndpoint,
}

pub(super) fn runtime_paths() -> RuntimePaths {
    let root = daemon_cmd::paths::resolve_config_dir()
        .unwrap_or_else(|_| PathBuf::from(".").join(".snow"));
    let endpoint = snow_core::ipc::IpcEndpoint::for_config_dir(&root);
    RuntimePaths {
        vault: root.join("vault"),
        database: root.join("snow.db"),
        socket: root.join("daemon.sock"),
        endpoint,
        root,
    }
}

pub(super) fn selected_env_name(cli: &Cli) -> String {
    cli.env
        .clone()
        .or_else(|| std::env::var("SNOW_ENV").ok())
        .unwrap_or_else(|| daemon_cmd::paths::selected_env(None))
}

pub(super) fn selected_daemon_start_env_name(cli: &Cli) -> String {
    daemon_cmd::paths::selected_daemon_start_env(cli.env.as_deref())
}

pub(super) fn command_uses_daemon_auto_spawn(command: &Command) -> bool {
    matches!(
        command,
        Command::Tui {
            daemon,
            socket_path,
            ..
        } if *daemon || socket_path.is_some()
    ) || matches!(
        command,
        Command::BusinessApp { .. } | Command::Server { .. } | Command::Incident { .. }
    )
}

pub(super) fn command_uses_local_credentials(command: &Command) -> bool {
    match command {
        // First-class CMDB primitive commands go through the daemon (auto-spawned),
        // so the CLI process itself does not load local credentials.
        Command::Daemon { .. }
        | Command::Admin
        | Command::CacheInfo
        | Command::AdoptCacheOnlyProjection { .. }
        | Command::BusinessApp { .. }
        | Command::Server { .. }
        | Command::Incident { .. } => false,
        Command::Tui {
            daemon,
            socket_path,
            ..
        } if *daemon || socket_path.is_some() => false,
        Command::Knowledge {
            action: Some(KnowledgeCommand::Status),
            ..
        }
        | Command::Knowledge {
            action: Some(KnowledgeCommand::Tags { .. }),
            ..
        } => false,
        _ => true,
    }
}

pub(super) fn load_auth_context(cli: &Cli) -> Result<AuthContext, SnowError> {
    // Resolve environment: --env flag > SNOW_ENV env var > selected daemon env > "test".
    let env_name = selected_env_name(cli);
    let env_file = format!(".env.{env_name}");
    let env_path = config_path(&env_file);
    let config_dir_hint = daemon_cmd::paths::config_dir_hint();
    dotenvy::from_path(&env_path).map_err(|e| {
        SnowError::Api(format!(
            "Failed to load {env_file}: {e}.\n  Searched: next to executable, {config_dir_hint}, and current directory"
        ))
    })?;

    let instance = std::env::var("SERVICENOW_INSTANCE")
        .or_else(|_| std::env::var("SNOW_INSTANCE"))
        .map_err(|_| SnowError::Api("SERVICENOW_INSTANCE not set in .env file".to_string()))?;
    let username = auth::resolve_username_from_runtime_env()
        .map_err(|err| SnowError::AuthFailed(err.to_string()))?;
    let credential = auth::CredentialProvider::from_runtime_env();
    let password = credential
        .resolve()
        .map_err(|err| SnowError::AuthFailed(err.to_string()))?;

    Ok(AuthContext {
        env_name,
        instance,
        username,
        credential,
        password,
    })
}

pub(super) fn runtime_instance_url() -> Option<String> {
    std::env::var("SERVICENOW_INSTANCE")
        .or_else(|_| std::env::var("SNOW_INSTANCE"))
        .ok()
}

pub(super) fn allow_loopback_http_for_local_test(instance: &str) -> bool {
    std::env::var("SNOW_ALLOW_LOOPBACK_HTTP").is_ok_and(|value| value.eq_ignore_ascii_case("true"))
        && (instance.starts_with("http://127.0.0.1:") || instance.starts_with("http://localhost:"))
}

pub(super) async fn build_core(
    instance: &str,
    username: &str,
    credential: auth::CredentialProvider,
    metadata_password: snow_core::credential::SecretString,
    client: ServiceNowClient,
    database_path: Option<PathBuf>,
) -> Result<SnowCore, SnowError> {
    let paths = runtime_paths();
    let mut config = SnowConfig {
        instance: CoreInstanceConfig {
            url: instance.to_string(),
            user: username.to_string(),
            credential,
            portal: std::env::var("SNOW_PORTAL").unwrap_or_default(),
        },
        vault: CoreVaultConfig { path: paths.vault },
        transport: CoreTransportConfig::default(),
        cache: CoreCacheConfig {
            memory: MemoryCacheConfig {
                capacity: 1000,
                ttl_active: "1h".to_string(),
                ttl_resolved: "24h".to_string(),
                ttl_closed: "7d".to_string(),
            },
            ..Default::default()
        },
        daemon: CoreDaemonConfig {
            socket_path: paths.socket,
            mcp_transport: "stdio".to_string(),
        },
        ..Default::default()
    };
    config.apply_defaults();

    let mut builder = SnowCore::builder()
        .config(config.clone())
        .client(client)
        .ui_metadata_basic_auth(username, metadata_password)
        .vault_path(config.vault.path);
    if let Some(database_path) = database_path {
        builder = builder.database_path(database_path);
    }
    Ok(builder.build().await?)
}

pub(super) fn config_path(filename: &str) -> PathBuf {
    daemon_cmd::paths::config_path(filename)
}
