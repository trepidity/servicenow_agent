use crate::{admin, auth, cli, daemon_cmd, display, tui_app, tui_client};

use std::collections::{BTreeSet, HashMap};
use std::fmt::Write as _;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result as AnyhowResult;
use chrono::{DateTime, NaiveDate, TimeZone, Utc};
use colored::Colorize;
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use servicenow_rs::prelude::{
    BasicAuth, DisplayValue, JournalEntry, Order, PrefixRegistry, Record, ServiceNowClient,
};
use snow_core::cache::store::{CacheFormat, Store};
use snow_core::display as core_display;
use snow_core::enrich::{VtbContext, VtbSchema, enrich_vtb_context};
use snow_core::{
    ApprovalRecord, KnowledgeArticle, KnowledgeBaseSummary, KnowledgeCategorySummary,
    KnowledgeEmbeddingCoverage, KnowledgeSearchFilters, KnowledgeSearchHit, KnowledgeSearchMode,
    KnowledgeSemanticSearchFilters, KnowledgeSemanticStatus, OrphanPruneReport, RebuildReport,
    RepairReport, SemanticIndexSummary, SnowCore, TaskSlaReadability, TaskSlaStatus,
    TaskSlaSummaryView, TaskSlaView, VaultVerificationReport,
    config::{
        CacheConfig as CoreCacheConfig, DaemonConfig as CoreDaemonConfig,
        InstanceConfig as CoreInstanceConfig, MemoryCacheConfig, SnowConfig,
        TransportConfig as CoreTransportConfig, VaultConfig as CoreVaultConfig,
    },
    resource::timecard::{SetMode, TimeCard, TimeValue, TimecardSheet, WeekSelector, Weekday},
};

use crate::error::SnowError;
use cli::{
    AttachmentCommand, BusinessAppCommand, BusinessAppFilter, Cli, Command, KnowledgeCommand,
    KnowledgeSearchModeArg, KnowledgeSemanticCommand, KnowledgeTagLayer, ServerCommand,
    TimecardCommand,
};
use tui_client::{
    BusinessApplicationQueryArgs, BusinessApplicationQueryFilter, BusinessApplicationQueryPageArgs,
    BusinessApplicationServersArgs, BusinessApplicationServersCachedArgs,
    BusinessApplicationSyncArgs, BusinessApplicationsForServerArgs, DaemonRpcClient,
    ServerQueryArgs, TuiClient,
};

struct AuthContext {
    env_name: String,
    instance: String,
    username: String,
    credential: auth::CredentialProvider,
    password: auth::SecretString,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ShowTarget {
    Project,
    Demand,
    Incident,
    Request,
    RequestItem,
    Story,
    StoryTask,
    Task,
    Knowledge,
    ResourcePlan,
    PrivateTask,
    Change,
}

pub(crate) fn run_entry(cli: Cli) -> Result<(), SnowError> {
    // Daemon lifecycle commands launch or become the daemon process. Keep them
    // outside any Tokio runtime so the daemon child can build its own runtime.
    if let Command::Daemon { action } = cli.command {
        let explicit_env = match &action {
            cli::DaemonCommand::Serve { env: Some(env), .. } => Some(env.as_str()),
            _ => cli.env.as_deref(),
        };
        let env_name = match &action {
            cli::DaemonCommand::Start { .. }
            | cli::DaemonCommand::Restart
            | cli::DaemonCommand::Serve { .. } => {
                daemon_cmd::paths::selected_daemon_start_env(explicit_env)
            }
            _ => daemon_cmd::paths::selected_env(explicit_env),
        };
        return daemon_cmd::dispatch(action, &env_name).map_err(SnowError::from);
    }

    if matches!(cli.command, Command::CacheInfo) {
        return cmd_cache_info();
    }
    if matches!(cli.command, Command::RebuildCache) {
        return cmd_rebuild_cache_offline();
    }

    let auth_context = if command_uses_local_credentials(&cli.command) {
        Some(load_auth_context(&cli)?)
    } else {
        None
    };

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(SnowError::from)?;
    runtime.block_on(run(cli, auth_context))
}

async fn run(cli: Cli, auth_context: Option<AuthContext>) -> Result<(), SnowError> {
    if matches!(cli.command, Command::CacheInfo) {
        return cmd_cache_info();
    }
    // `snow admin` is handled before auth setup so it doesn't require a
    // populated `.env`.
    if matches!(cli.command, Command::Admin) {
        return admin::run()
            .await
            .map_err(|e| SnowError::Api(e.to_string()));
    }
    if let Some(result) = maybe_run_local_kb_command(&cli.command)? {
        return Ok(result);
    }

    let env_name = auth_context
        .as_ref()
        .map(|auth| auth.env_name.clone())
        .unwrap_or_else(|| {
            if command_uses_daemon_auto_spawn(&cli.command) {
                selected_daemon_start_env_name(&cli)
            } else {
                selected_env_name(&cli)
            }
        });

    if let Command::Tui {
        refresh,
        show_closed,
        daemon,
        socket_path,
    } = &cli.command
        && (*daemon || socket_path.is_some())
    {
        let paths = runtime_paths();
        let instance_url = runtime_instance_url();
        let endpoint = socket_path
            .clone()
            .map(|path| snow_core::ipc::IpcEndpoint::Filesystem { path })
            .unwrap_or_else(|| snow_core::ipc::IpcEndpoint::for_config_dir(&paths.root));
        let tui_client = Arc::new(TuiClient::remote_endpoint_with_auto_spawn(
            endpoint,
            instance_url,
            env_name.clone(),
        ));
        let identity = tui_client.runtime_identity(None).await?;
        return tui_app::run_tui(
            tui_client,
            &env_name,
            &identity.mode,
            &identity.username,
            refresh.map(Duration::from_secs),
            *show_closed,
        )
        .await;
    }

    // First-class CMDB primitive commands talk to the backend exclusively over the
    // daemon JSON-RPC interface (auto-spawning the daemon if needed), so they
    // are dispatched here before local-credential setup.
    if let Command::BusinessApp { action } = cli.command {
        let paths = runtime_paths();
        let instance_url = runtime_instance_url();
        let endpoint = snow_core::ipc::IpcEndpoint::for_config_dir(&paths.root);
        let client =
            DaemonRpcClient::with_endpoint_auto_spawn(endpoint, instance_url, env_name.clone());
        return cmd_business_app(&client, action).await;
    }
    if let Command::Server { action } = cli.command {
        let paths = runtime_paths();
        let instance_url = runtime_instance_url();
        let endpoint = snow_core::ipc::IpcEndpoint::for_config_dir(&paths.root);
        let client =
            DaemonRpcClient::with_endpoint_auto_spawn(endpoint, instance_url, env_name.clone());
        return cmd_server(&client, action).await;
    }

    let AuthContext {
        instance,
        username,
        credential,
        password,
        ..
    } = auth_context.ok_or_else(|| {
        SnowError::Api("credentials were not prepared for this command".to_string())
    })?;

    let client_auth = BasicAuth::new(&username, password.as_str()).without_session();
    let core_auth = BasicAuth::new(&username, password.as_str()).without_session();
    drop(password);

    // Build client
    let client = ServiceNowClient::builder()
        .instance(&instance)
        .auth(client_auth)
        .build()
        .await?;
    let core_client = ServiceNowClient::builder()
        .instance(&instance)
        .auth(core_auth)
        .build()
        .await?;
    let core = Arc::new(build_core(&instance, &username, credential, core_client).await?);
    let tui_client = Arc::new(TuiClient::local(Arc::clone(&core)));
    let identity = tui_client.runtime_identity(Some(&username)).await?;

    match cli.command {
        Command::Tui {
            refresh,
            show_closed,
            ..
        } => {
            tui_app::run_tui(
                tui_client,
                &env_name,
                &identity.mode,
                &identity.username,
                refresh.map(Duration::from_secs),
                show_closed,
            )
            .await
        }
        Command::Show {
            number,
            extras,
            resource_plan_state,
            smart,
            full,
        } => {
            cmd_show(
                core.as_ref(),
                &client,
                &username,
                &number,
                &extras,
                resource_plan_state.as_deref(),
                smart,
                full,
            )
            .await
        }
        Command::Tasks { number } => cmd_tasks_core(core.as_ref(), &number).await,
        Command::Sla { number } => cmd_sla(core.as_ref(), &number).await,
        Command::Approve { number, yes } => cmd_approve_core(core.as_ref(), &number, yes).await,
        Command::Reject {
            number,
            reason,
            yes,
        } => cmd_reject_core(core.as_ref(), &number, reason.unwrap_or_default(), yes).await,
        Command::Note {
            number,
            message,
            dry_run,
        } => cmd_note_core(core.as_ref(), &number, &message, dry_run).await,
        Command::Attachment { action } => cmd_attachment(core.as_ref(), action).await,
        Command::Timecard { action } => {
            cmd_timecard(Arc::clone(&core), &env_name, &instance, &username, action).await
        }
        Command::RepairVault => cmd_repair_vault(core.as_ref()).await,
        Command::RebuildCache => cmd_rebuild_cache(core.as_ref()).await,
        Command::VerifyVault => cmd_verify_vault(core.as_ref()).await,
        Command::PruneOrphans { dry_run } => cmd_prune_orphans(core.as_ref(), dry_run).await,
        Command::CacheInfo => unreachable!("handled before auth setup"),
        Command::Knowledge {
            number,
            fresh,
            action,
        } => cmd_knowledge(core.as_ref(), number, fresh, action).await,
        Command::Approval { number } => cmd_show_approval_runtime(core.as_ref(), &number).await,
        Command::BusinessApp { .. } => {
            unreachable!("business-app is dispatched before local-credential setup")
        }
        Command::Server { .. } => {
            unreachable!("server is dispatched before local-credential setup")
        }
        Command::Daemon { .. } | Command::Admin => {
            unreachable!("daemon and admin are dispatched before auth setup")
        }
    }
}

#[derive(Debug, Clone)]
struct RuntimePaths {
    root: PathBuf,
    vault: PathBuf,
    database: PathBuf,
    socket: PathBuf,
    endpoint: snow_core::ipc::IpcEndpoint,
}

fn runtime_paths() -> RuntimePaths {
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

fn selected_env_name(cli: &Cli) -> String {
    cli.env
        .clone()
        .or_else(|| std::env::var("SNOW_ENV").ok())
        .unwrap_or_else(|| daemon_cmd::paths::selected_env(None))
}

fn selected_daemon_start_env_name(cli: &Cli) -> String {
    daemon_cmd::paths::selected_daemon_start_env(cli.env.as_deref())
}

fn command_uses_daemon_auto_spawn(command: &Command) -> bool {
    matches!(
        command,
        Command::Tui {
            daemon,
            socket_path,
            ..
        } if *daemon || socket_path.is_some()
    ) || matches!(
        command,
        Command::BusinessApp { .. } | Command::Server { .. }
    )
}

fn command_uses_local_credentials(command: &Command) -> bool {
    match command {
        // First-class CMDB primitive commands go through the daemon (auto-spawned),
        // so the CLI process itself does not load local credentials.
        Command::Daemon { .. }
        | Command::Admin
        | Command::CacheInfo
        | Command::BusinessApp { .. }
        | Command::Server { .. } => false,
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

fn load_auth_context(cli: &Cli) -> Result<AuthContext, SnowError> {
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

fn runtime_instance_url() -> Option<String> {
    std::env::var("SERVICENOW_INSTANCE")
        .or_else(|_| std::env::var("SNOW_INSTANCE"))
        .ok()
}

async fn build_core(
    instance: &str,
    username: &str,
    credential: auth::CredentialProvider,
    client: ServiceNowClient,
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

    Ok(SnowCore::builder()
        .config(config.clone())
        .client(client)
        .vault_path(config.vault.path)
        .build()
        .await?)
}

fn cmd_cache_info() -> Result<(), SnowError> {
    let paths = runtime_paths();
    let database_exists = paths.database.exists();
    let vault_exists = paths.vault.exists();
    let cache_format = match Store::inspect_format(&paths.database) {
        Ok(CacheFormat::Absent) => "absent".to_string(),
        Ok(CacheFormat::Current) => "current".to_string(),
        Ok(CacheFormat::Incompatible { found }) => {
            format!("incompatible ({found}); run `snow rebuild-cache`")
        }
        Err(err) => format!("unreadable ({err})"),
    };

    println!("Runtime Root: {}", paths.root.display());
    println!("Vault Path: {}", paths.vault.display());
    println!("DB Path: {}", paths.database.display());
    println!("Daemon Endpoint: {}", paths.endpoint);
    println!("Legacy Socket Path: {}", paths.socket.display());
    println!("Vault Exists: {}", if vault_exists { "yes" } else { "no" });
    println!("DB Exists: {}", if database_exists { "yes" } else { "no" });
    println!("Cache Format: {cache_format}");
    Ok(())
}

fn cmd_rebuild_cache_offline() -> Result<(), SnowError> {
    let paths = runtime_paths();
    let report = snow_core::rebuild_cache_from_vault(&paths.vault, &paths.database)
        .map_err(|error| SnowError::Api(error.to_string()))?;
    print_rebuild_report(&report);
    Ok(())
}

fn maybe_run_local_kb_command(command: &Command) -> Result<Option<()>, SnowError> {
    let db_path = runtime_paths().database;
    let handled = match command {
        Command::Knowledge {
            action: Some(KnowledgeCommand::Status),
            ..
        } => {
            cmd_knowledge_status(&db_path)?;
            true
        }
        Command::Knowledge {
            action: Some(KnowledgeCommand::Tags { layer, min_count }),
            ..
        } => {
            cmd_knowledge_tags(&db_path, *layer, *min_count)?;
            true
        }
        Command::Knowledge {
            action: Some(KnowledgeCommand::Sync { .. }),
            ..
        } => false,
        _ => false,
    };
    Ok(handled.then_some(()))
}

async fn cmd_tasks_core(core: &SnowCore, number: &str) -> Result<(), SnowError> {
    let children = core.get_children(number).await?;
    if children.is_empty() {
        println!("No tasks found.");
        return Ok(());
    }
    for child in children {
        println!(
            "{}  {}  {}",
            child.number, child.state, child.short_description
        );
    }
    Ok(())
}

async fn cmd_sla(core: &SnowCore, number: &str) -> Result<(), SnowError> {
    let status = core.task_sla_status_for_number(number).await?;
    print_task_sla_status(&status);
    Ok(())
}

async fn cmd_approve_core(core: &SnowCore, number: &str, yes: bool) -> Result<(), SnowError> {
    if !yes && !confirm_action(&format!("Approve {number}?"))? {
        println!("Cancelled.");
        return Ok(());
    }
    core.approve(number, None).await?;
    println!("Approved {number}.");
    Ok(())
}

async fn cmd_reject_core(
    core: &SnowCore,
    number: &str,
    reason: String,
    yes: bool,
) -> Result<(), SnowError> {
    if !yes && !confirm_action(&format!("Reject {number}?"))? {
        println!("Cancelled.");
        return Ok(());
    }
    core.reject(number, &reason).await?;
    println!("Rejected {number}.");
    Ok(())
}

async fn cmd_note_core(
    core: &SnowCore,
    number: &str,
    message: &str,
    dry_run: bool,
) -> Result<(), SnowError> {
    if dry_run {
        println!("Dry run — no changes will be made.");
        println!("Would add note to {number}: {message}");
        return Ok(());
    }
    core.add_work_note(number, message).await?;
    println!("Added work note to {number}.");
    Ok(())
}

async fn cmd_attachment(core: &SnowCore, action: AttachmentCommand) -> Result<(), SnowError> {
    match action {
        AttachmentCommand::List { number } => {
            let Some(attachments) = core.list_attachments(&number).await? else {
                return Err(SnowError::NotFound(format!("{number} not found.")));
            };
            print_attachments(&number, &attachments);
            Ok(())
        }
        AttachmentCommand::Upload {
            number,
            path,
            file_name,
            content_type,
            dry_run,
            yes,
        } => {
            let metadata = std::fs::metadata(&path)?;
            if !metadata.is_file() {
                return Err(SnowError::Api(format!(
                    "{} is not a regular file",
                    path.display()
                )));
            }
            if metadata.len() == 0 {
                return Err(SnowError::Api(format!("{} is empty", path.display())));
            }

            let attachment_name = match file_name.as_deref() {
                Some(name) if !name.trim().is_empty() => name.to_string(),
                _ => path
                    .file_name()
                    .and_then(|name| name.to_str())
                    .ok_or_else(|| {
                        SnowError::Api(format!(
                            "{} does not have a valid file name",
                            path.display()
                        ))
                    })?
                    .to_string(),
            };
            let content_type =
                content_type.unwrap_or_else(|| infer_content_type(&path).to_string());

            let target = core
                .get_record_fresh(&number)
                .await?
                .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;

            if dry_run {
                println!("Dry run — no attachment will be uploaded.");
                println!("  Number:       {number}");
                println!("  Table:        {}", target.table);
                println!("  Sys ID:       {}", target.sys_id);
                println!("  File:         {}", path.display());
                println!("  File name:    {attachment_name}");
                println!("  Content-Type: {content_type}");
                println!("  Size:         {} bytes", metadata.len());
                return Ok(());
            }

            if !yes
                && !confirm_action(&format!(
                    "Upload {attachment_name} ({} bytes) to {number}?",
                    metadata.len()
                ))?
            {
                println!("Cancelled.");
                return Ok(());
            }

            let Some(attachment) = core
                .upload_attachment_file(&number, &path, Some(&attachment_name), Some(&content_type))
                .await?
            else {
                return Err(SnowError::NotFound(format!("{number} not found.")));
            };

            println!(
                "Uploaded {} to {} as attachment {}.",
                attachment.file_name, number, attachment.sys_id
            );
            Ok(())
        }
    }
}

fn print_attachments(number: &str, attachments: &[snow_core::AttachmentMetadata]) {
    if attachments.is_empty() {
        println!("No attachments found for {number}.");
        return;
    }

    println!("Attachments for {number}:");
    for attachment in attachments {
        let size = attachment
            .size_bytes
            .map(|bytes| bytes.to_string())
            .unwrap_or_else(|| "-".to_string());
        let created = attachment.sys_created_on.as_deref().unwrap_or("-");
        println!(
            "  {}  {}  {} bytes  {}  {}",
            attachment.sys_id, attachment.file_name, size, attachment.content_type, created
        );
    }
}

fn infer_content_type(path: &std::path::Path) -> &'static str {
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(|extension| extension.to_ascii_lowercase())
        .as_deref()
    {
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("png") => "image/png",
        Some("gif") => "image/gif",
        Some("pdf") => "application/pdf",
        Some("txt") | Some("log") => "text/plain",
        Some("json") => "application/json",
        Some("csv") => "text/csv",
        _ => "application/octet-stream",
    }
}

async fn cmd_timecard(
    core: Arc<SnowCore>,
    env_name: &str,
    instance: &str,
    username: &str,
    action: TimecardCommand,
) -> Result<(), SnowError> {
    match action {
        TimecardCommand::List { week } => {
            let week_selector = parse_week_selector(week.as_deref())?;
            let sheet = core.list_my_timecards(week_selector).await?;
            display::print_timecard_sheet(&sheet);
            write_timecard_index_cache(&sheet, env_name, instance, username)?;
            Ok(())
        }
        TimecardCommand::Set {
            card,
            day,
            hours,
            add,
            week,
            dry_run,
            yes,
            category,
            sun,
            mon,
            tue,
            wed,
            thu,
            fri,
            sat,
        } => {
            let updates = collect_timecard_updates(
                day.as_deref(),
                hours.as_deref(),
                [
                    (Weekday::Sun, sun.as_deref()),
                    (Weekday::Mon, mon.as_deref()),
                    (Weekday::Tue, tue.as_deref()),
                    (Weekday::Wed, wed.as_deref()),
                    (Weekday::Thu, thu.as_deref()),
                    (Weekday::Fri, fri.as_deref()),
                    (Weekday::Sat, sat.as_deref()),
                ],
            )?;
            let week = parse_week_selector(week.as_deref())?;
            let sheet = core.list_my_timecards(week).await?;
            let resolved = resolve_timecard_selector(
                &sheet,
                &card,
                category.as_deref(),
                env_name,
                instance,
                username,
            )?;
            let card_snapshot = sheet.cards[resolved.index].clone();
            print_timecard_update_preview(&card_snapshot, &updates, add, dry_run);
            warn_if_day_totals_exceed_24(&sheet, &card_snapshot.sys_id, &updates, add);
            if dry_run {
                println!("Dry run - no changes were made.");
                return Ok(());
            }
            if !yes && !confirm_action("Apply these timecard hour updates?")? {
                println!("Cancelled.");
                return Ok(());
            }

            for update in updates {
                let value = parse_time_value(&update.hours)?;
                let updated = core
                    .set_timecard_hours(&resolved.sys_id, update.day, value, set_mode(add))
                    .await?;
                println!(
                    "Updated {} {} to {} (total {}).",
                    display::timecard_task_display(&updated),
                    weekday_label(update.day),
                    display_hour(&updated.hours[weekday_index(update.day)]),
                    display_hour(&updated.total)
                );
            }
            let refreshed = core.list_my_timecards(week).await?;
            write_timecard_index_cache(&refreshed, env_name, instance, username)?;
            Ok(())
        }
        TimecardCommand::Edit { week } => {
            let week = parse_week_selector(week.as_deref())?;
            let editor_client = Arc::new(TuiClient::local(Arc::clone(&core)));
            let sheet = tui_app::run_timecard_editor(editor_client, week).await?;
            write_timecard_index_cache(&sheet, env_name, instance, username)?;
            Ok(())
        }
    }
}

#[derive(Debug)]
struct TimecardUpdate {
    day: Weekday,
    hours: String,
}

#[derive(Debug)]
struct ResolvedTimecard {
    sys_id: String,
    index: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum TimecardSelectorShape {
    SysId,
    Index(usize),
    Task,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TimecardIndexCache {
    entries: Vec<TimecardIndexCacheEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TimecardIndexCacheEntry {
    key: TimecardIndexCacheKey,
    sys_id: String,
    fingerprint: TimecardFingerprint,
    expires_at: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TimecardIndexCacheKey {
    env: String,
    instance: String,
    username: String,
    actor_user_sys_id: String,
    week_starts_on: String,
    sheet_sys_id: String,
    index: usize,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct TimecardFingerprint {
    task_sys_id: String,
    task_display: String,
    category: String,
    project_time_category: String,
    week_starts_on: String,
}

fn parse_week_selector(value: Option<&str>) -> Result<WeekSelector, SnowError> {
    match value {
        Some(value) => NaiveDate::parse_from_str(value, "%Y-%m-%d")
            .map(WeekSelector::Date)
            .map_err(|_| SnowError::Api("--week must be formatted as YYYY-MM-DD".to_string())),
        None => Ok(WeekSelector::Current),
    }
}

fn collect_timecard_updates(
    day: Option<&str>,
    hours: Option<&str>,
    day_flags: [(Weekday, Option<&str>); 7],
) -> Result<Vec<TimecardUpdate>, SnowError> {
    let mut updates = Vec::new();
    if day.is_some() || hours.is_some() {
        let day = day.ok_or_else(|| {
            SnowError::Api("single-day form requires both <day> and <hours>".to_string())
        })?;
        let hours = hours.ok_or_else(|| {
            SnowError::Api("single-day form requires both <day> and <hours>".to_string())
        })?;
        updates.push(TimecardUpdate {
            day: parse_weekday(day)?,
            hours: normalize_hours(hours)?,
        });
    }

    let mut flag_updates = Vec::new();
    for (day, value) in day_flags {
        if let Some(value) = value {
            flag_updates.push(TimecardUpdate {
                day,
                hours: normalize_hours(value)?,
            });
        }
    }

    if !updates.is_empty() && !flag_updates.is_empty() {
        return Err(SnowError::Api(
            "use either positional <day> <hours> or --sun/--mon/... flags, not both".to_string(),
        ));
    }
    updates.extend(flag_updates);
    if updates.is_empty() {
        return Err(SnowError::Api(
            "provide a day and hours, or one or more --sun/--mon/... flags".to_string(),
        ));
    }
    Ok(updates)
}

fn parse_weekday(value: &str) -> Result<Weekday, SnowError> {
    match value.trim().to_ascii_lowercase().as_str() {
        "sun" | "sunday" => Ok(Weekday::Sun),
        "mon" | "monday" => Ok(Weekday::Mon),
        "tue" | "tues" | "tuesday" => Ok(Weekday::Tue),
        "wed" | "wednesday" => Ok(Weekday::Wed),
        "thu" | "thur" | "thurs" | "thursday" => Ok(Weekday::Thu),
        "fri" | "friday" => Ok(Weekday::Fri),
        "sat" | "saturday" => Ok(Weekday::Sat),
        _ => Err(SnowError::Api(
            "unknown day; use sun, mon, tue, wed, thu, fri, or sat".to_string(),
        )),
    }
}

fn normalize_hours(value: &str) -> Result<String, SnowError> {
    let parsed = value
        .trim()
        .parse::<f64>()
        .map_err(|_| SnowError::Api(format!("invalid hours value {value:?}")))?;
    if !parsed.is_finite() || !(0.0..=24.0).contains(&parsed) {
        return Err(SnowError::Api(
            "hours must be a decimal value from 0 through 24".to_string(),
        ));
    }
    Ok(format_hours(parsed))
}

fn parse_time_value(hours: &str) -> Result<TimeValue, SnowError> {
    hours
        .parse::<TimeValue>()
        .map_err(|_| SnowError::Api(format!("invalid hours value {hours:?}")))
}

fn set_mode(add: bool) -> SetMode {
    if add { SetMode::Add } else { SetMode::Set }
}

fn resolve_timecard_selector(
    sheet: &TimecardSheet,
    selector: &str,
    category: Option<&str>,
    env_name: &str,
    instance: &str,
    username: &str,
) -> Result<ResolvedTimecard, SnowError> {
    match classify_timecard_selector(selector) {
        TimecardSelectorShape::SysId => sheet
            .cards
            .iter()
            .enumerate()
            .find(|(_, card)| card.sys_id.eq_ignore_ascii_case(selector))
            .map(|(index, card)| ResolvedTimecard {
                sys_id: card.sys_id.clone(),
                index,
            })
            .ok_or_else(|| {
                SnowError::NotFound(format!(
                    "time card {selector} is not present in the selected week"
                ))
            }),
        TimecardSelectorShape::Index(index) => {
            resolve_timecard_index(sheet, index, env_name, instance, username)
        }
        TimecardSelectorShape::Task => resolve_timecard_task(sheet, selector, category),
    }
}

fn classify_timecard_selector(selector: &str) -> TimecardSelectorShape {
    let trimmed = selector.trim();
    if trimmed.len() == 32 && trimmed.chars().all(|ch| ch.is_ascii_hexdigit()) {
        return TimecardSelectorShape::SysId;
    }
    if !trimmed.is_empty()
        && trimmed.chars().all(|ch| ch.is_ascii_digit())
        && let Ok(index) = trimmed.parse::<usize>()
    {
        return TimecardSelectorShape::Index(index);
    }
    TimecardSelectorShape::Task
}

fn resolve_timecard_task(
    sheet: &TimecardSheet,
    selector: &str,
    category: Option<&str>,
) -> Result<ResolvedTimecard, SnowError> {
    let mut matches = sheet
        .cards
        .iter()
        .enumerate()
        .filter(|(_, card)| task_matches(card, selector))
        .collect::<Vec<_>>();
    if let Some(category) = category {
        matches.retain(|(_, card)| category_matches(card, category));
    }
    match matches.as_slice() {
        [] => Err(SnowError::NotFound(format!(
            "no time card matched task {selector:?} in the selected week"
        ))),
        [(index, card)] => Ok(ResolvedTimecard {
            sys_id: card.sys_id.clone(),
            index: *index,
        }),
        _ => {
            let mut message =
                format!("task {selector:?} matched multiple time cards; pass --category:\n");
            for (index, card) in matches {
                let category = if card.category.trim().is_empty() {
                    card.category_label.as_str()
                } else {
                    card.category.as_str()
                };
                let _ = writeln!(
                    message,
                    "  {}  {}  category={}  sys_id={}",
                    index + 1,
                    display::timecard_task_display(card),
                    category,
                    card.sys_id
                );
            }
            Err(SnowError::Api(message))
        }
    }
}

fn resolve_timecard_index(
    sheet: &TimecardSheet,
    index: usize,
    env_name: &str,
    instance: &str,
    username: &str,
) -> Result<ResolvedTimecard, SnowError> {
    if index == 0 {
        return Err(SnowError::Api(
            "timecard list indexes start at 1".to_string(),
        ));
    }
    let key = cache_key_for(sheet, index, env_name, instance, username)?;
    let cache = read_timecard_index_cache();
    let now = Utc::now().timestamp();
    let Some(entry) = cache
        .entries
        .iter()
        .find(|entry| entry.key == key && entry.expires_at > now)
    else {
        return Err(SnowError::Api(format!(
            "time card index {index} is not in the short-lived cache; rerun `snow timecard list`"
        )));
    };

    let Some((fresh_index, card)) = sheet
        .cards
        .iter()
        .enumerate()
        .find(|(_, card)| card.sys_id == entry.sys_id)
    else {
        return Err(SnowError::Api(format!(
            "cached time card index {index} no longer exists; rerun `snow timecard list`"
        )));
    };
    let fresh_fingerprint = fingerprint_timecard(card);
    if fresh_fingerprint != entry.fingerprint {
        return Err(SnowError::Api(format!(
            "cached time card index {index} changed; rerun `snow timecard list`"
        )));
    }
    Ok(ResolvedTimecard {
        sys_id: card.sys_id.clone(),
        index: fresh_index,
    })
}

fn task_matches(card: &TimeCard, selector: &str) -> bool {
    let selector = selector.trim();
    card.task
        .as_ref()
        .map(|task| {
            task.number.eq_ignore_ascii_case(selector)
                || task.sys_id.eq_ignore_ascii_case(selector)
                || display::timecard_task_display(card).eq_ignore_ascii_case(selector)
        })
        .unwrap_or(false)
}

fn category_matches(card: &TimeCard, category: &str) -> bool {
    card.category.eq_ignore_ascii_case(category)
        || card.category_label.eq_ignore_ascii_case(category)
}

fn write_timecard_index_cache(
    sheet: &TimecardSheet,
    env_name: &str,
    instance: &str,
    username: &str,
) -> Result<(), SnowError> {
    if sheet.cards.is_empty() {
        return Ok(());
    }
    let now = Utc::now().timestamp();
    let expires_at = now + 10 * 60;
    let mut cache = read_timecard_index_cache();
    cache.entries.retain(|entry| entry.expires_at > now);

    let sheet_sys_id = sheet_sys_id(sheet);
    let actor_user_sys_id = actor_user_sys_id(sheet, username);
    cache.entries.retain(|entry| {
        !(entry.key.env == env_name
            && entry.key.instance == normalize_cache_token(instance)
            && entry.key.username == username
            && entry.key.actor_user_sys_id == actor_user_sys_id
            && entry.key.week_starts_on == sheet.week_starts_on
            && entry.key.sheet_sys_id == sheet_sys_id)
    });

    for (index, card) in sheet.cards.iter().enumerate() {
        cache.entries.push(TimecardIndexCacheEntry {
            key: TimecardIndexCacheKey {
                env: env_name.to_string(),
                instance: normalize_cache_token(instance),
                username: username.to_string(),
                actor_user_sys_id: actor_user_sys_id.clone(),
                week_starts_on: sheet.week_starts_on.clone(),
                sheet_sys_id: sheet_sys_id.clone(),
                index: index + 1,
            },
            sys_id: card.sys_id.clone(),
            fingerprint: fingerprint_timecard(card),
            expires_at,
        });
    }

    let path = timecard_index_cache_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let bytes = serde_json::to_vec_pretty(&cache).map_err(|err| SnowError::Api(err.to_string()))?;
    std::fs::write(path, bytes)?;
    Ok(())
}

fn read_timecard_index_cache() -> TimecardIndexCache {
    let path = timecard_index_cache_path();
    std::fs::read(path)
        .ok()
        .and_then(|bytes| serde_json::from_slice::<TimecardIndexCache>(&bytes).ok())
        .unwrap_or_else(|| TimecardIndexCache {
            entries: Vec::new(),
        })
}

fn timecard_index_cache_path() -> PathBuf {
    runtime_paths().root.join("timecard-index-cache.json")
}

fn cache_key_for(
    sheet: &TimecardSheet,
    index: usize,
    env_name: &str,
    instance: &str,
    username: &str,
) -> Result<TimecardIndexCacheKey, SnowError> {
    if sheet.cards.is_empty() {
        return Err(SnowError::NotFound(
            "no time cards found in the selected week".to_string(),
        ));
    }
    Ok(TimecardIndexCacheKey {
        env: env_name.to_string(),
        instance: normalize_cache_token(instance),
        username: username.to_string(),
        actor_user_sys_id: actor_user_sys_id(sheet, username),
        week_starts_on: sheet.week_starts_on.clone(),
        sheet_sys_id: sheet_sys_id(sheet),
        index,
    })
}

fn sheet_sys_id(sheet: &TimecardSheet) -> String {
    sheet
        .sheet
        .as_ref()
        .map(|sheet| sheet.sys_id.clone())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| "none".to_string())
}

fn actor_user_sys_id(sheet: &TimecardSheet, fallback: &str) -> String {
    sheet
        .cards
        .first()
        .map(|card| card.user.sys_id.clone())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| fallback.to_string())
}

fn normalize_cache_token(value: &str) -> String {
    value
        .trim()
        .trim_end_matches('/')
        .trim_start_matches("https://")
        .trim_start_matches("http://")
        .to_ascii_lowercase()
}

fn fingerprint_timecard(card: &TimeCard) -> TimecardFingerprint {
    let (task_sys_id, task_display) = card
        .task
        .as_ref()
        .map(|task| (task.sys_id.clone(), task.number.clone()))
        .unwrap_or_else(|| ("".to_string(), "".to_string()));
    TimecardFingerprint {
        task_sys_id,
        task_display,
        category: card.category.clone(),
        project_time_category: card.project_time_category.clone().unwrap_or_default(),
        week_starts_on: card.week_starts_on.clone(),
    }
}

fn print_timecard_update_preview(
    card: &TimeCard,
    updates: &[TimecardUpdate],
    add: bool,
    dry_run: bool,
) {
    if dry_run {
        println!("Dry run preview:");
    } else {
        println!("Timecard update preview:");
    }
    println!(
        "Card: {}  category={}  sys_id={}",
        display::timecard_task_display(card),
        if card.category.trim().is_empty() {
            card.category_label.as_str()
        } else {
            card.category.as_str()
        },
        card.sys_id
    );

    let mut projected_hours = card.hours.clone();
    for update in updates {
        let day_index = weekday_index(update.day);
        let current = parse_hour_value(&projected_hours[day_index]).unwrap_or(0.0);
        let requested = parse_hour_value(&update.hours).unwrap_or(0.0);
        let new_value = if add { current + requested } else { requested };
        println!(
            "  {:<3} {} -> {}",
            weekday_label(update.day),
            format_hours(current),
            format_hours(new_value)
        );
        projected_hours[day_index] = format_hours(new_value);
    }
    let projected_total = projected_hours
        .iter()
        .filter_map(|value| parse_hour_value(value))
        .sum::<f64>();
    println!(
        "  Total {} -> {}",
        display_hour(&card.total),
        format_hours(projected_total)
    );
}

fn warn_if_day_totals_exceed_24(
    sheet: &TimecardSheet,
    target_sys_id: &str,
    updates: &[TimecardUpdate],
    add: bool,
) {
    for update in updates {
        let day_index = weekday_index(update.day);
        let mut total = 0.0;
        for card in &sheet.cards {
            let mut value = parse_hour_value(&card.hours[day_index]).unwrap_or(0.0);
            if card.sys_id == target_sys_id {
                let requested = parse_hour_value(&update.hours).unwrap_or(0.0);
                value = if add { value + requested } else { requested };
            }
            total += value;
        }
        if total > 24.0 {
            println!(
                "Warning: {} total across listed cards would be {} hours.",
                weekday_label(update.day),
                format_hours(total)
            );
        }
    }
}

fn weekday_index(day: Weekday) -> usize {
    match day {
        Weekday::Sun => 0,
        Weekday::Mon => 1,
        Weekday::Tue => 2,
        Weekday::Wed => 3,
        Weekday::Thu => 4,
        Weekday::Fri => 5,
        Weekday::Sat => 6,
    }
}

fn weekday_label(day: Weekday) -> &'static str {
    match day {
        Weekday::Sun => "Sun",
        Weekday::Mon => "Mon",
        Weekday::Tue => "Tue",
        Weekday::Wed => "Wed",
        Weekday::Thu => "Thu",
        Weekday::Fri => "Fri",
        Weekday::Sat => "Sat",
    }
}

fn parse_hour_value(value: &str) -> Option<f64> {
    let value = value.trim();
    if value.is_empty() {
        return Some(0.0);
    }
    value.parse::<f64>().ok()
}

fn display_hour(value: &str) -> String {
    parse_hour_value(value)
        .map(format_hours)
        .unwrap_or_else(|| value.to_string())
}

fn format_hours(value: f64) -> String {
    let rounded = (value * 100.0).round() / 100.0;
    if rounded.fract().abs() < f64::EPSILON {
        format!("{}", rounded as i64)
    } else {
        let mut value = format!("{rounded:.2}");
        while value.contains('.') && value.ends_with('0') {
            value.pop();
        }
        if value.ends_with('.') {
            value.pop();
        }
        value
    }
}

async fn cmd_repair_vault(core: &SnowCore) -> Result<(), SnowError> {
    let report = core.repair_vault().await?;
    print_repair_report(&report);
    Ok(())
}

async fn cmd_rebuild_cache(core: &SnowCore) -> Result<(), SnowError> {
    let report = core.rebuild_cache()?;
    print_rebuild_report(&report);
    Ok(())
}

async fn cmd_verify_vault(core: &SnowCore) -> Result<(), SnowError> {
    let report = core.verify_vault()?;
    print_verification_report(&report);
    Ok(())
}

async fn cmd_prune_orphans(core: &SnowCore, dry_run: bool) -> Result<(), SnowError> {
    let report = core.prune_orphans(dry_run).await?;
    print_prune_report(&report);
    Ok(())
}

async fn cmd_knowledge(
    core: &SnowCore,
    number: Option<String>,
    fresh: bool,
    action: Option<KnowledgeCommand>,
) -> Result<(), SnowError> {
    if fresh && action.is_some() {
        return Err(SnowError::Api(
            "--fresh is only valid when showing a single knowledge article".to_string(),
        ));
    }

    match (number, action) {
        (Some(number), None) => cmd_show_knowledge_runtime(core, &number, fresh).await,
        (
            None,
            Some(KnowledgeCommand::Search {
                query,
                mode,
                knowledge_base,
                category,
                limit,
                min_score_millis,
            }),
        ) => match mode {
            KnowledgeSearchModeArg::Lexical => {
                if min_score_millis.is_some() {
                    return Err(SnowError::Api(
                        "--min-score-millis is only valid with --mode semantic or --mode hybrid"
                            .to_string(),
                    ));
                }
                let articles = core
                    .search_knowledge(
                        &query,
                        KnowledgeSearchFilters {
                            knowledge_base,
                            category,
                            limit,
                        },
                    )
                    .await?;
                if articles.is_empty() {
                    println!("No knowledge articles found.");
                    return Ok(());
                }

                for article in articles {
                    print_knowledge_article_summary(&article);
                    println!();
                }
                Ok(())
            }
            mode => {
                let hits = core
                    .search_knowledge_semantic(
                        &query,
                        KnowledgeSemanticSearchFilters {
                            knowledge_base,
                            category,
                            limit,
                            mode: mode.into(),
                            min_score_millis,
                        },
                    )
                    .await?;
                if hits.is_empty() {
                    println!("No knowledge articles found.");
                    return Ok(());
                }

                for hit in hits {
                    print_knowledge_search_hit(&hit);
                    println!();
                }
                Ok(())
            }
        },
        (None, Some(KnowledgeCommand::Bases)) => {
            let bases = core.list_knowledge_bases()?;
            if bases.is_empty() {
                println!("No knowledge bases found.");
                return Ok(());
            }

            for base in bases {
                print_knowledge_base_summary(&base);
            }
            Ok(())
        }
        (None, Some(KnowledgeCommand::Categories { knowledge_base })) => {
            let categories = core.list_categories(&knowledge_base)?;
            if categories.is_empty() {
                println!("No knowledge categories found.");
                return Ok(());
            }

            for category in categories {
                print_knowledge_category_summary(&category);
            }
            Ok(())
        }
        (None, Some(KnowledgeCommand::Sync { full, with_bodies })) => {
            let outcome = core.sync_knowledge(full, with_bodies).await?;
            if !outcome.accepted {
                return Err(SnowError::Api(
                    outcome
                        .details
                        .unwrap_or_else(|| "KB sync request was not accepted".to_string()),
                ));
            }
            print_knowledge_sync_outcome(&outcome);
            Ok(())
        }
        (None, Some(KnowledgeCommand::Tags { .. })) | (None, Some(KnowledgeCommand::Status)) => {
            unreachable!("handled before auth setup")
        }
        (
            None,
            Some(KnowledgeCommand::Semantic {
                action: KnowledgeSemanticCommand::Status,
            }),
        ) => {
            let status = core.knowledge_semantic_status().await?;
            print_knowledge_semantic_status(&status);
            Ok(())
        }
        (
            None,
            Some(KnowledgeCommand::Semantic {
                action: KnowledgeSemanticCommand::Rebuild { full },
            }),
        ) => {
            let summary = core.rebuild_knowledge_semantic_index(full).await?;
            print_knowledge_semantic_rebuild_summary(&summary);
            Ok(())
        }
        (Some(_), Some(_)) => Err(SnowError::Api(
            "knowledge article numbers and subcommands are mutually exclusive".to_string(),
        )),
        (None, None) => Err(SnowError::Api(
            "knowledge requires a number or a subcommand".to_string(),
        )),
    }
}

async fn cmd_show_knowledge_runtime(
    core: &SnowCore,
    number: &str,
    fresh: bool,
) -> Result<(), SnowError> {
    let article = if fresh {
        core.get_knowledge_article_fresh(number).await?
    } else {
        core.get_knowledge_article(number).await?
    };
    match article {
        Some(article) => print_knowledge_article(&article),
        None => println!("Knowledge article not found: {number}"),
    }
    Ok(())
}

async fn cmd_show_approval_runtime(core: &SnowCore, number: &str) -> Result<(), SnowError> {
    match core.get_approval(number).await? {
        Some(approval) => print_approval_record(&approval),
        None => println!("Approval not found: {number}"),
    }
    Ok(())
}

/// Convert parsed `--filter <field>:<op>:<value>` tokens into the daemon wire
/// shape, preserving their command-line order.
///
/// clap already validated each token (field/operator/value present, operator is
/// a known token) and preserved the order of the repeated `--filter` argument,
/// so this is a direct, total mapping with no re-parsing of the raw argv. The
/// emitted `BusinessApplicationQueryFilter` shape (field + operator + value) is
/// exactly what the daemon's `BusinessApplicationFieldFilter` expects on the
/// wire, so this change is transparent to the daemon.
fn business_app_filters_to_query(
    filters: Vec<BusinessAppFilter>,
) -> Vec<BusinessApplicationQueryFilter> {
    filters
        .into_iter()
        .map(|filter| BusinessApplicationQueryFilter {
            field: filter.field,
            operator: filter.operator,
            value: filter.value,
        })
        .collect()
}

async fn cmd_business_app_export_all(
    client: &DaemonRpcClient,
    format: business_app_export::BusinessAppExportFormat,
    output: PathBuf,
) -> Result<(), SnowError> {
    business_app_export::validate_output_parent(&output)?;
    let mut offset = 0;
    let mut records = Vec::new();
    let filters: &[BusinessApplicationQueryFilter] = &[];
    loop {
        let result = client
            .business_application_query_page_with_args(BusinessApplicationQueryPageArgs {
                query: BusinessApplicationQueryArgs {
                    text: None,
                    filters,
                    limit: Some(business_app_export::EXPORT_ALL_PAGE_SIZE),
                },
                offset: Some(offset),
            })
            .await?;
        let page_len =
            business_app_export::append_records_from_query_result(&mut records, &result)?;
        if page_len < business_app_export::EXPORT_ALL_PAGE_SIZE {
            break;
        }
        offset += page_len;
    }
    let result = serde_json::Value::Array(records);
    let bytes = business_app_export::serialize(&result, format)?;
    let exported_count = business_app_export::record_count(&result)?;
    business_app_export::write_file(&output, &bytes)?;
    println!(
        "Exported {exported_count} cached Business Applications to {}",
        output.display()
    );
    Ok(())
}

fn validate_business_app_export_all_options(
    text: Option<&str>,
    filter: &[BusinessAppFilter],
    limit: Option<usize>,
) -> Result<(), SnowError> {
    if text.is_some() || !filter.is_empty() || limit.is_some() {
        return Err(SnowError::Api(
            "business-app export --all accepts only --all, --format, and --output".to_string(),
        ));
    }
    Ok(())
}

fn validate_business_app_sync_all_options(
    name: Option<&str>,
    operational_state_not: Option<&str>,
) -> Result<(), SnowError> {
    if name.is_some() || operational_state_not.is_some() {
        return Err(SnowError::Api(
            "business-app sync --all does not accept bounded sync filters".to_string(),
        ));
    }
    Ok(())
}

async fn cmd_business_app_sync_all(
    client: &DaemonRpcClient,
    persist: bool,
    resolve_references: bool,
    reference_depth: Option<u32>,
    refresh_dictionary: bool,
    json: bool,
) -> Result<(), SnowError> {
    let summary = client
        .business_application_sync_with_args(BusinessApplicationSyncArgs {
            all: true,
            name: None,
            operational_state_not: None,
            persist,
            resolve_references,
            reference_depth,
            refresh_dictionary,
        })
        .await?;
    if json {
        print_full_dump_or_inline(&summary);
    } else {
        print!(
            "{}",
            display::format_business_application_summary_object(&summary)
        );
    }
    Ok(())
}

fn format_business_application_servers_result(result: &serde_json::Value) -> String {
    let payload = business_application_servers_payload(result);
    let summary = payload.get("relationship_summary");
    let servers = payload
        .get("servers")
        .and_then(serde_json::Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let server_count = summary
        .and_then(|summary| json_usize_from_paths(summary, &[&["servers_found"]]))
        .unwrap_or(servers.len());
    let max_depth = summary.and_then(|summary| json_display_from_paths(summary, &[&["max_depth"]]));
    let degraded_reasons = collect_business_application_servers_degraded_reasons(payload);

    // Fallback signals (present only when fallback_strategy != none).
    let fallback_used = summary
        .and_then(|summary| json_bool(summary, "fallback_used"))
        .unwrap_or(false);
    let fallback_requested = summary
        .and_then(|summary| summary.get("fallback_strategy"))
        .and_then(serde_json::Value::as_str)
        .is_some_and(|strategy| !strategy.is_empty() && strategy != "none");
    let cmdb_servers_found = summary.and_then(|summary| json_usize(summary, "cmdb_servers_found"));
    let fallback_group = summary
        .and_then(|summary| json_display_from_paths(summary, &[&["fallback_group_display_name"]]));

    let mut out = String::new();
    let app = payload
        .get("business_application")
        .map(format_business_application_servers_root)
        .unwrap_or_else(|| "-".to_string());
    let _ = writeln!(out, "Business Application: {app}");
    if fallback_used {
        let _ = writeln!(
            out,
            "Servers found: {server_count}  (via ci_owner_group fallback -- CMDB relationships unmapped)"
        );
        if let Some(group) = &fallback_group {
            let _ = writeln!(out, "Group: {group}");
        }
    } else {
        let _ = writeln!(out, "Servers found: {server_count}");
    }
    if let Some(max_depth) = &max_depth {
        let _ = writeln!(out, "Max depth: {max_depth}");
    }
    let _ = writeln!(
        out,
        "Completeness: {}",
        if degraded_reasons.is_empty() {
            "complete"
        } else {
            "partial"
        }
    );
    let _ = writeln!(
        out,
        "Degraded: {}",
        if degraded_reasons.is_empty() {
            "none".to_string()
        } else {
            degraded_reasons.join(", ")
        }
    );

    if servers.is_empty() {
        // Fallback requested but produced no servers: explain why so an empty
        // result is not mistaken for "no fallback attempted".
        if fallback_requested && !fallback_used {
            let _ = writeln!(out, "CMDB traversal: 0 servers found.");
            let _ = writeln!(
                out,
                "Fallback: ci_owner_group requested but BA has no u_ci_owner_group set."
            );
            return out;
        }
        if fallback_used {
            let _ = writeln!(out, "CMDB traversal: 0 servers found.");
            let _ = writeln!(
                out,
                "Fallback: ci_owner_group returned no servers for the BA's owner group."
            );
            return out;
        }
        match max_depth {
            Some(max_depth) => {
                let _ = writeln!(
                    out,
                    "No associated server CIs found within max depth {max_depth}."
                );
            }
            None => {
                let _ = writeln!(out, "No associated server CIs found.");
            }
        }
        return out;
    }

    for server in servers {
        let name = json_display_from_paths(
            server,
            &[
                &["name"],
                &["record", "short_description"],
                &["record", "number"],
                &["record", "sys_id"],
                &["sys_id"],
            ],
        )
        .unwrap_or_else(|| "-".to_string());
        let class_name = json_display_from_paths(server, &[&["class_name"], &["record", "table"]])
            .unwrap_or_else(|| "-".to_string());
        let ip_address =
            json_display_from_paths(server, &[&["ip_address"], &["fields", "ip_address"]])
                .unwrap_or_else(|| "-".to_string());
        let status = json_display_from_paths(
            server,
            &[
                &["operational_status"],
                &["fields", "operational_status"],
                &["fields", "install_status"],
            ],
        )
        .unwrap_or_else(|| "-".to_string());
        let _ = writeln!(out, "{name}  {class_name}  {ip_address}  {status}");
    }

    if fallback_used {
        let cmdb_count = cmdb_servers_found.unwrap_or(0);
        let _ = writeln!(
            out,
            "\nWarning: {cmdb_count} servers found via CMDB traversal. Results are from CI owner group\nfallback and may not reflect all servers supporting this application.\nCMDB relationships should be reviewed and populated."
        );
    }

    out
}

fn format_business_application_servers_cached_result(result: &serde_json::Value) -> String {
    let payload = business_application_servers_cached_payload(result);
    let servers = payload
        .get("servers")
        .and_then(serde_json::Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);

    let mut out = String::new();
    let app = payload
        .get("business_application")
        .map(format_business_application_servers_root)
        .unwrap_or_else(|| "-".to_string());
    let _ = writeln!(out, "Business Application: {app}");
    let _ = writeln!(out, "Cached servers found: {}", servers.len());

    if servers.is_empty() {
        let _ = writeln!(out, "No cached associated server CIs found.");
        return out;
    }

    for relationship in servers {
        let server = relationship.get("server").unwrap_or(relationship);
        let mut suffix = Vec::new();
        if let Some(depth) = json_usize(relationship, "min_depth") {
            suffix.push(format!("depth {depth}"));
        }
        if let Some(provenance) = json_display_from_paths(relationship, &[&["provenance"]]) {
            suffix.push(provenance);
        }
        if relationship
            .get("tombstoned_at")
            .is_some_and(|value| !value.is_null())
        {
            suffix.push("tombstoned".to_string());
        }
        let suffix = if suffix.is_empty() {
            String::new()
        } else {
            format!("  [{}]", suffix.join(", "))
        };
        let _ = writeln!(out, "{}{}", format_cached_server_row(server), suffix);
    }

    out
}

fn format_business_applications_for_server_result(result: &serde_json::Value) -> String {
    let payload = business_applications_for_server_payload(result);
    let servers = payload
        .get("servers")
        .and_then(serde_json::Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[]);
    let app_count: usize = servers
        .iter()
        .map(|server| {
            server
                .get("business_applications")
                .and_then(serde_json::Value::as_array)
                .map_or(0, Vec::len)
        })
        .sum();

    let mut out = String::new();
    let _ = writeln!(out, "Matched servers: {}", servers.len());
    let _ = writeln!(out, "Cached Business Applications found: {app_count}");

    if servers.is_empty() {
        let _ = writeln!(out, "No cached Server found.");
        return out;
    }

    for server_relationships in servers {
        let server = server_relationships
            .get("server")
            .unwrap_or(server_relationships);
        let _ = writeln!(out, "Server: {}", format_cached_server_identity(server));
        let applications = server_relationships
            .get("business_applications")
            .and_then(serde_json::Value::as_array)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        if applications.is_empty() {
            let _ = writeln!(out, "  No cached Business Applications found.");
            continue;
        }
        for relationship in applications {
            let app = relationship
                .get("business_application")
                .unwrap_or(relationship);
            let mut suffix = Vec::new();
            if let Some(depth) = json_usize(relationship, "min_depth") {
                suffix.push(format!("depth {depth}"));
            }
            if let Some(provenance) = json_display_from_paths(relationship, &[&["provenance"]]) {
                suffix.push(provenance);
            }
            if relationship
                .get("tombstoned_at")
                .is_some_and(|value| !value.is_null())
            {
                suffix.push("tombstoned".to_string());
            }
            let suffix = if suffix.is_empty() {
                String::new()
            } else {
                format!(" [{}]", suffix.join(", "))
            };
            let _ = writeln!(
                out,
                "  {}{}",
                format_business_application_servers_root(app),
                suffix
            );
        }
    }

    out
}

fn business_application_servers_payload(result: &serde_json::Value) -> &serde_json::Value {
    result.get("business_application_servers").unwrap_or(result)
}

fn business_application_servers_cached_payload(result: &serde_json::Value) -> &serde_json::Value {
    result
        .get("business_application_servers_cached")
        .unwrap_or(result)
}

fn business_applications_for_server_payload(result: &serde_json::Value) -> &serde_json::Value {
    result
        .get("business_applications_for_server")
        .unwrap_or(result)
}

fn format_business_application_servers_root(app: &serde_json::Value) -> String {
    let number = json_display_from_paths(app, &[&["number"], &["record", "number"]]);
    let name = json_display_from_paths(app, &[&["name"], &["record", "short_description"]]);
    match (number, name) {
        (Some(number), Some(name)) if number != name => format!("{number} {name}"),
        (Some(number), _) => number,
        (_, Some(name)) => name,
        _ => json_display_from_paths(app, &[&["sys_id"], &["record", "sys_id"]])
            .unwrap_or_else(|| "-".to_string()),
    }
}

fn format_cached_server_row(server: &serde_json::Value) -> String {
    let name = format_cached_server_identity(server);
    let class_name = json_display_from_paths(server, &[&["class_name"], &["record", "table"]])
        .unwrap_or_else(|| "-".to_string());
    let ip_address = json_display_from_paths(server, &[&["ip_address"], &["fields", "ip_address"]])
        .unwrap_or_else(|| "-".to_string());
    let status = json_display_from_paths(
        server,
        &[
            &["operational_status"],
            &["fields", "operational_status"],
            &["fields", "install_status"],
        ],
    )
    .unwrap_or_else(|| "-".to_string());
    format!("{name}  {class_name}  {ip_address}  {status}")
}

fn format_cached_server_identity(server: &serde_json::Value) -> String {
    json_display_from_paths(
        server,
        &[
            &["name"],
            &["record", "short_description"],
            &["record", "number"],
            &["record", "sys_id"],
            &["sys_id"],
        ],
    )
    .unwrap_or_else(|| "-".to_string())
}

fn collect_business_application_servers_degraded_reasons(
    payload: &serde_json::Value,
) -> Vec<String> {
    let mut reasons = BTreeSet::new();
    if let Some(summary) = payload.get("relationship_summary") {
        if json_bool(summary, "depth_limit_reached").unwrap_or(false) {
            reasons.insert("depth_limit_reached".to_string());
        }
        if json_bool(summary, "truncated").unwrap_or(false) {
            reasons.insert("truncated".to_string());
        }
        if json_usize(summary, "truncated_count").unwrap_or(0) > 0 {
            reasons.insert("truncated_count".to_string());
        }
        if json_usize(summary, "acl_restricted_count").unwrap_or(0) > 0 {
            reasons.insert("acl_restricted".to_string());
        }
        collect_degraded_reason_values(summary.get("degraded_reasons"), &mut reasons);
    }
    if payload
        .get("diagnostics")
        .and_then(serde_json::Value::as_array)
        .is_some_and(|diagnostics| !diagnostics.is_empty())
    {
        reasons.insert("diagnostics".to_string());
    }
    reasons.into_iter().collect()
}

fn collect_degraded_reason_values(
    value: Option<&serde_json::Value>,
    reasons: &mut BTreeSet<String>,
) {
    match value {
        Some(serde_json::Value::Array(values)) => {
            for value in values {
                if let Some(reason) = json_display_value(value) {
                    reasons.insert(reason);
                }
            }
        }
        Some(serde_json::Value::Object(map)) => {
            for (key, value) in map {
                let include = match value {
                    serde_json::Value::Bool(flag) => *flag,
                    serde_json::Value::Number(number) => number.as_u64().unwrap_or(0) > 0,
                    serde_json::Value::String(text) => !text.trim().is_empty(),
                    serde_json::Value::Array(values) => !values.is_empty(),
                    serde_json::Value::Object(values) => !values.is_empty(),
                    serde_json::Value::Null => false,
                };
                if include {
                    reasons.insert(key.clone());
                }
            }
        }
        _ => {}
    }
}

fn json_display_from_paths(value: &serde_json::Value, paths: &[&[&str]]) -> Option<String> {
    paths
        .iter()
        .find_map(|path| json_path(value, path).and_then(json_display_value))
}

fn json_usize_from_paths(value: &serde_json::Value, paths: &[&[&str]]) -> Option<usize> {
    paths.iter().find_map(|path| {
        json_path(value, path).and_then(|value| match value {
            serde_json::Value::Number(number) => number.as_u64().map(|value| value as usize),
            serde_json::Value::String(text) => text.trim().parse::<usize>().ok(),
            _ => None,
        })
    })
}

fn json_path<'a>(value: &'a serde_json::Value, path: &[&str]) -> Option<&'a serde_json::Value> {
    path.iter()
        .try_fold(value, |current, key| current.get(*key))
}

fn json_display_value(value: &serde_json::Value) -> Option<String> {
    match value {
        serde_json::Value::String(text) if !text.trim().is_empty() => Some(text.clone()),
        serde_json::Value::Number(number) => Some(number.to_string()),
        serde_json::Value::Bool(flag) => Some(flag.to_string()),
        serde_json::Value::Object(_) => value
            .get("display_value")
            .and_then(json_display_value)
            .or_else(|| value.get("value").and_then(json_display_value))
            .or_else(|| value.get("display_name").and_then(json_display_value)),
        _ => None,
    }
}

fn json_bool(value: &serde_json::Value, key: &str) -> Option<bool> {
    value.get(key).and_then(serde_json::Value::as_bool)
}

fn json_usize(value: &serde_json::Value, key: &str) -> Option<usize> {
    value.get(key).and_then(|value| match value {
        serde_json::Value::Number(number) => number.as_u64().map(|value| value as usize),
        serde_json::Value::String(text) => text.trim().parse::<usize>().ok(),
        _ => None,
    })
}

/// Dispatch the `snow business-app` subcommand family over the daemon client.
async fn cmd_business_app(
    client: &DaemonRpcClient,
    action: BusinessAppCommand,
) -> Result<(), SnowError> {
    match action {
        BusinessAppCommand::Get {
            sys_id,
            name,
            fresh,
            json,
            full,
        } => {
            if sys_id.is_none() && name.is_none() {
                return Err(SnowError::Api(
                    "business-app get requires --sys-id or --name".to_string(),
                ));
            }
            let app = client
                .business_application_get(sys_id.as_deref(), name.as_deref(), fresh)
                .await?;
            match app {
                Some(app) => {
                    if json {
                        print_json(&app)?;
                    } else {
                        print!("{}", display::format_business_application(&app, full));
                    }
                }
                None => println!("Business Application not found."),
            }
            Ok(())
        }
        BusinessAppCommand::Search {
            name,
            operational_state_not,
            limit,
            json,
            full,
        } => {
            let apps = client
                .business_application_search(
                    name.as_deref(),
                    operational_state_not.as_deref(),
                    limit,
                )
                .await?;
            if json {
                return print_json(&apps);
            }
            if apps.is_empty() {
                println!("No Business Applications found.");
                return Ok(());
            }
            for app in &apps {
                if full {
                    print!("{}", display::format_business_application(app, true));
                } else {
                    println!("{}", display::format_business_application_summary(app));
                }
                println!();
            }
            Ok(())
        }
        BusinessAppCommand::Query {
            filter,
            limit,
            json,
        } => {
            if filter.is_empty() {
                return Err(SnowError::Api(
                    "business-app query requires at least one --filter".to_string(),
                ));
            }
            let filters = business_app_filters_to_query(filter);
            let result = client.business_application_query(&filters, limit).await?;
            if json {
                print_full_dump_or_inline(&result);
            } else {
                // Query returns records in a backend-owned shape; render generically.
                print_full_dump_or_inline(&result);
            }
            Ok(())
        }
        BusinessAppCommand::Servers {
            number,
            sys_id,
            cached,
            for_server,
            max_depth,
            max_cis,
            max_edges,
            max_service_membership_associations,
            max_service_membership_pages,
            relationship_type,
            include_paths,
            fallback_strategy,
            no_persist,
            prune_stale,
            include_tombstoned,
            json,
        } => {
            let ba_selector_count = [number.is_some(), sys_id.is_some()]
                .into_iter()
                .filter(|selected| *selected)
                .count();
            if for_server.is_some() {
                if ba_selector_count != 0
                    || cached
                    || max_depth.is_some()
                    || max_cis.is_some()
                    || max_edges.is_some()
                    || max_service_membership_associations.is_some()
                    || max_service_membership_pages.is_some()
                    || !relationship_type.is_empty()
                    || include_paths
                    || no_persist
                    || prune_stale
                {
                    return Err(SnowError::Api(
                        "business-app servers --for-server cannot combine with BA selectors or live traversal flags".to_string(),
                    ));
                }
                let result = client
                    .business_applications_for_server(BusinessApplicationsForServerArgs {
                        sys_id: for_server.as_deref(),
                        include_tombstoned,
                    })
                    .await?;
                if json {
                    print_full_dump_or_inline(&result);
                } else {
                    print!(
                        "{}",
                        format_business_applications_for_server_result(&result)
                    );
                }
                return Ok(());
            }
            if cached {
                if ba_selector_count != 1 {
                    return Err(SnowError::Api(
                        "business-app servers --cached requires exactly one of --number or --sys-id"
                            .to_string(),
                    ));
                }
                let result = client
                    .business_application_servers_cached(BusinessApplicationServersCachedArgs {
                        number: number.as_deref(),
                        sys_id: sys_id.as_deref(),
                        include_tombstoned,
                    })
                    .await?;
                if json {
                    print_full_dump_or_inline(&result);
                } else {
                    print!(
                        "{}",
                        format_business_application_servers_cached_result(&result)
                    );
                }
                return Ok(());
            }
            if ba_selector_count != 1 {
                return Err(SnowError::Api(
                    "business-app servers requires exactly one of --number, --sys-id, or --for-server"
                        .to_string(),
                ));
            }
            if prune_stale && no_persist {
                return Err(SnowError::Api(
                    "business-app servers --prune-stale requires a persisting live traversal"
                        .to_string(),
                ));
            }
            if include_tombstoned {
                return Err(SnowError::Api(
                    "business-app servers --include-tombstoned is only valid with --cached or --for-server"
                        .to_string(),
                ));
            }
            let result = client
                .business_application_servers(BusinessApplicationServersArgs {
                    number: number.as_deref(),
                    sys_id: sys_id.as_deref(),
                    max_depth,
                    max_cis,
                    max_edges,
                    max_service_membership_associations,
                    max_service_membership_pages,
                    relationship_type: &relationship_type,
                    include_paths,
                    fallback_strategy: fallback_strategy.as_wire(),
                    persist: !no_persist,
                    prune_stale,
                })
                .await?;
            if json {
                print_full_dump_or_inline(&result);
            } else {
                print!("{}", format_business_application_servers_result(&result));
            }
            Ok(())
        }
        BusinessAppCommand::Export {
            all,
            format,
            output,
            text,
            filter,
            limit,
        } => {
            let export_format = match format {
                cli::BusinessAppExportFormat::Json => {
                    business_app_export::BusinessAppExportFormat::Json
                }
                cli::BusinessAppExportFormat::Jsonl => {
                    business_app_export::BusinessAppExportFormat::Jsonl
                }
                cli::BusinessAppExportFormat::Csv => {
                    business_app_export::BusinessAppExportFormat::Csv
                }
            };
            if all {
                validate_business_app_export_all_options(text.as_deref(), &filter, limit)?;
                return cmd_business_app_export_all(client, export_format, output).await;
            }
            business_app_export::validate_limit(limit)?;
            business_app_export::validate_output_parent(&output)?;
            business_app_export::validate_text(text.as_deref())?;
            let filters = business_app_filters_to_query(filter);
            let result = client
                .business_application_query_with_args(BusinessApplicationQueryArgs {
                    text: text.as_deref(),
                    filters: &filters,
                    limit,
                })
                .await?;
            let bytes = business_app_export::serialize(&result, export_format)?;
            let exported_count = business_app_export::record_count(&result)?;
            business_app_export::write_file(&output, &bytes)?;
            println!(
                "Exported {exported_count} Business Applications to {}",
                output.display()
            );
            Ok(())
        }
        BusinessAppCommand::Fields { refresh, json } => {
            let fields = client.business_application_fields(refresh).await?;
            if json {
                print_full_dump_or_inline(&fields);
            } else {
                print!("{}", display::format_business_application_fields(&fields));
            }
            Ok(())
        }
        BusinessAppCommand::Sync {
            all,
            name,
            operational_state_not,
            persist,
            resolve_references,
            reference_depth,
            refresh_dictionary,
            json,
        } => {
            if all {
                validate_business_app_sync_all_options(
                    name.as_deref(),
                    operational_state_not.as_deref(),
                )?;
                return cmd_business_app_sync_all(
                    client,
                    persist,
                    resolve_references,
                    reference_depth,
                    refresh_dictionary,
                    json,
                )
                .await;
            }
            let summary = client
                .business_application_sync(
                    name.as_deref(),
                    operational_state_not.as_deref(),
                    persist,
                    resolve_references,
                    reference_depth,
                    refresh_dictionary,
                )
                .await?;
            if json {
                print_full_dump_or_inline(&summary);
            } else {
                print!(
                    "{}",
                    display::format_business_application_summary_object(&summary)
                );
            }
            Ok(())
        }
    }
}

/// Dispatch the `snow server` subcommand family over the daemon client.
async fn cmd_server(client: &DaemonRpcClient, action: ServerCommand) -> Result<(), SnowError> {
    match action {
        ServerCommand::Get {
            sys_id,
            name,
            ip_address,
            fresh,
            json,
            full,
        } => {
            let selector_count = [sys_id.is_some(), name.is_some(), ip_address.is_some()]
                .into_iter()
                .filter(|selected| *selected)
                .count();
            if selector_count != 1 {
                return Err(SnowError::Api(
                    "server get requires exactly one of --sys-id, --name, or --ip-address"
                        .to_string(),
                ));
            }
            let server = client
                .server_get(
                    sys_id.as_deref(),
                    name.as_deref(),
                    ip_address.as_deref(),
                    fresh,
                )
                .await?;
            match server {
                Some(server) => {
                    if json {
                        print_json(&server)?;
                    } else {
                        print!("{}", display::format_server(&server, full));
                    }
                }
                None => println!("Server not found."),
            }
            Ok(())
        }
        ServerCommand::Search {
            name,
            ip_address,
            ci_owner_group,
            class,
            limit,
            json,
            full,
        } => {
            let servers = client
                .server_search(ServerQueryArgs {
                    text: None,
                    name: name.as_deref(),
                    ip_address: ip_address.as_deref(),
                    ci_owner_group: ci_owner_group.as_deref(),
                    class: class.as_deref(),
                    limit,
                })
                .await?;
            if json {
                return print_json(&servers);
            }
            if servers.is_empty() {
                println!("No Servers found.");
                return Ok(());
            }
            for server in &servers {
                if full {
                    print!("{}", display::format_server(server, true));
                } else {
                    println!("{}", display::format_server_summary(server));
                }
                println!();
            }
            Ok(())
        }
        ServerCommand::Query {
            text,
            name,
            ip_address,
            ci_owner_group,
            class,
            limit,
            json,
            full,
        } => {
            let servers = client
                .server_query(ServerQueryArgs {
                    text: text.as_deref(),
                    name: name.as_deref(),
                    ip_address: ip_address.as_deref(),
                    ci_owner_group: ci_owner_group.as_deref(),
                    class: class.as_deref(),
                    limit,
                })
                .await?;
            if json {
                return print_json(&servers);
            }
            if servers.is_empty() {
                println!("No Servers found.");
                return Ok(());
            }
            for server in &servers {
                if full {
                    print!("{}", display::format_server(server, true));
                } else {
                    println!("{}", display::format_server_summary(server));
                }
                println!();
            }
            Ok(())
        }
        ServerCommand::Fields { json } => {
            let fields = client.server_fields().await?;
            if json {
                print_full_dump_or_inline(&fields);
            } else {
                print!("{}", display::format_server_fields(&fields));
            }
            Ok(())
        }
    }
}

mod business_app_export {
    use super::SnowError;
    use serde_json::Value;
    use std::collections::BTreeSet;
    use std::io::Write;
    use std::path::Path;

    const MAX_EXPORT_LIMIT: usize = 500;
    pub(super) const EXPORT_ALL_PAGE_SIZE: usize = 500;
    const CSV_BASE_HEADERS: &[&str] = &[
        "record.sys_id",
        "record.number",
        "name",
        "record.short_description",
        "operational_state",
        "business_owner",
        "is_owner",
        "ci_owner_group",
        "primary_support_group",
        "primary_portfolio",
        "attested_date",
        "vault_relative_path",
        "browser_url",
    ];
    const CSV_BASE_SOURCE_FIELDS: &[&str] = &[
        "sys_id",
        "number",
        "name",
        "short_description",
        "operational_status",
        "business_owner",
        "it_application_owner",
        "managed_by_group",
        "support_group",
        "portfolio",
        "attested_date",
        "vault_relative_path",
        "browser_url",
    ];

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub(super) enum BusinessAppExportFormat {
        Json,
        Jsonl,
        Csv,
    }

    pub(super) fn validate_limit(limit: Option<usize>) -> Result<(), SnowError> {
        match limit {
            Some(0) => Err(SnowError::Api("`limit` must be at least 1".to_string())),
            Some(value) if value > MAX_EXPORT_LIMIT => Err(SnowError::Api(format!(
                "`limit` must be at most {MAX_EXPORT_LIMIT}"
            ))),
            _ => Ok(()),
        }
    }

    pub(super) fn validate_output_parent(output: &Path) -> Result<(), SnowError> {
        if output.file_name().is_none() {
            return Err(SnowError::Api(
                "business-app export --output must name a file".to_string(),
            ));
        }
        if output.is_dir() {
            return Err(SnowError::Api(format!(
                "business-app export --output must name a file, got directory: {}",
                output.display()
            )));
        }
        let parent = output
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        if !parent.exists() {
            return Err(SnowError::Api(format!(
                "business-app export output parent does not exist: {}",
                parent.display()
            )));
        }
        if !parent.is_dir() {
            return Err(SnowError::Api(format!(
                "business-app export output parent is not a directory: {}",
                parent.display()
            )));
        }
        Ok(())
    }

    pub(super) fn validate_text(text: Option<&str>) -> Result<(), SnowError> {
        if let Some(text) = text
            && text.trim().is_empty()
        {
            return Err(SnowError::Api(
                "business-app export --text must not be empty".to_string(),
            ));
        }
        Ok(())
    }

    pub(super) fn serialize(
        result: &Value,
        format: BusinessAppExportFormat,
    ) -> Result<Vec<u8>, SnowError> {
        let records = records_from_query_result(result)?;
        validate_record_objects(records)?;
        match format {
            BusinessAppExportFormat::Json => {
                serde_json::to_vec_pretty(&Value::Array(records.to_vec()))
                    .map_err(|err| SnowError::Api(err.to_string()))
            }
            BusinessAppExportFormat::Jsonl => serialize_jsonl(records),
            BusinessAppExportFormat::Csv => serialize_csv(records),
        }
    }

    pub(super) fn record_count(result: &Value) -> Result<usize, SnowError> {
        Ok(records_from_query_result(result)?.len())
    }

    pub(super) fn append_records_from_query_result(
        target: &mut Vec<Value>,
        result: &Value,
    ) -> Result<usize, SnowError> {
        let records = records_from_query_result(result)?;
        validate_record_objects(records)?;
        let count = records.len();
        target.extend(records.iter().cloned());
        Ok(count)
    }

    pub(super) fn write_file(output: &Path, bytes: &[u8]) -> Result<(), SnowError> {
        validate_output_parent(output)?;
        let parent = output
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .unwrap_or_else(|| Path::new("."));
        let file_name = output.file_name().ok_or_else(|| {
            SnowError::Api("business-app export --output must name a file".to_string())
        })?;
        let temp_name = format!(
            ".{}.{}.tmp",
            file_name.to_string_lossy(),
            uuid::Uuid::new_v4()
        );
        let temp_path = parent.join(temp_name);
        let write_result = (|| -> Result<(), SnowError> {
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&temp_path)?;
            file.write_all(bytes)?;
            file.sync_all()?;
            Ok(())
        })();

        if let Err(err) = write_result {
            let _ = std::fs::remove_file(&temp_path);
            return Err(err);
        }

        if let Err(err) = std::fs::rename(&temp_path, output) {
            let _ = std::fs::remove_file(&temp_path);
            return Err(SnowError::from(err));
        }
        Ok(())
    }

    fn records_from_query_result(result: &Value) -> Result<&[Value], SnowError> {
        match result {
            Value::Array(records) => Ok(records.as_slice()),
            Value::Object(map) => {
                for key in ["business_applications", "records", "results"] {
                    if let Some(value) = map.get(key) {
                        return value.as_array().map(Vec::as_slice).ok_or_else(|| {
                            SnowError::Api(format!(
                                "business_application_query field `{key}` was not an array"
                            ))
                        });
                    }
                }
                Err(SnowError::Api(
                    "business_application_query result did not contain an exportable array"
                        .to_string(),
                ))
            }
            _ => Err(SnowError::Api(
                "business_application_query result was not an exportable array".to_string(),
            )),
        }
    }

    fn validate_record_objects(records: &[Value]) -> Result<(), SnowError> {
        if let Some((index, _)) = records
            .iter()
            .enumerate()
            .find(|(_, record)| !record.is_object())
        {
            return Err(SnowError::Api(format!(
                "business_application_query record at index {index} was not an object"
            )));
        }
        Ok(())
    }

    fn serialize_jsonl(records: &[Value]) -> Result<Vec<u8>, SnowError> {
        let mut output = Vec::new();
        for record in records {
            serde_json::to_writer(&mut output, record)
                .map_err(|err| SnowError::Api(err.to_string()))?;
            output.push(b'\n');
        }
        Ok(output)
    }

    fn serialize_csv(records: &[Value]) -> Result<Vec<u8>, SnowError> {
        let headers = csv_headers(records);
        let mut output = String::new();
        write_csv_row(&mut output, &headers);
        for record in records {
            let row = headers
                .iter()
                .map(|header| csv_cell_for_header(record, header))
                .collect::<Vec<_>>();
            write_csv_row(&mut output, &row);
        }
        Ok(output.into_bytes())
    }

    fn csv_headers(records: &[Value]) -> Vec<String> {
        let mut headers = CSV_BASE_HEADERS
            .iter()
            .map(|header| (*header).to_string())
            .collect::<Vec<_>>();
        let mut projected_fields = BTreeSet::new();
        for record in records {
            if let Some(fields) = record.get("fields").and_then(Value::as_object) {
                for field in fields.keys() {
                    if !CSV_BASE_HEADERS.contains(&field.as_str())
                        && !CSV_BASE_SOURCE_FIELDS.contains(&field.as_str())
                    {
                        projected_fields.insert(field.clone());
                    }
                }
            }
        }
        headers.extend(projected_fields);
        headers
    }

    fn csv_cell_for_header(record: &Value, header: &str) -> String {
        match header {
            "record.sys_id" => value_text(record.get("record").and_then(|v| v.get("sys_id"))),
            "record.number" => value_text(record.get("record").and_then(|v| v.get("number"))),
            "name" => value_text(record.get("name")),
            "record.short_description" => value_text(
                record
                    .get("record")
                    .and_then(|v| v.get("short_description")),
            ),
            "operational_state" => value_text(record.get("operational_state")),
            "business_owner" => value_text(record.get("business_owner")),
            "is_owner" => value_text(record.get("is_owner")),
            "ci_owner_group" => value_text(record.get("ci_owner_group")),
            "primary_support_group" => value_text(record.get("primary_support_group")),
            "primary_portfolio" => value_text(record.get("primary_portfolio")),
            "attested_date" => value_text(record.get("attested_date")),
            "vault_relative_path" => value_text(record.get("vault_relative_path")),
            "browser_url" => value_text(record.get("browser_url")),
            field => value_text(
                record
                    .get("fields")
                    .and_then(Value::as_object)
                    .and_then(|fields| fields.get(field)),
            ),
        }
    }

    fn value_text(value: Option<&Value>) -> String {
        let Some(value) = value else {
            return String::new();
        };
        match value {
            Value::Null => String::new(),
            Value::String(text) => text.clone(),
            Value::Bool(value) => value.to_string(),
            Value::Number(value) => value.to_string(),
            Value::Array(_) => compact_json(value),
            Value::Object(map) => {
                for key in ["display_name", "display_value", "value"] {
                    if let Some(text) = map
                        .get(key)
                        .and_then(Value::as_str)
                        .filter(|text| !text.trim().is_empty())
                    {
                        return text.to_string();
                    }
                    if key == "value"
                        && let Some(raw_value) = map.get(key)
                        && !raw_value.is_null()
                    {
                        return value_text(Some(raw_value));
                    }
                }
                compact_json(value)
            }
        }
    }

    fn compact_json(value: &Value) -> String {
        serde_json::to_string(value).unwrap_or_else(|_| value.to_string())
    }

    fn write_csv_row(output: &mut String, cells: &[String]) {
        for (index, cell) in cells.iter().enumerate() {
            if index > 0 {
                output.push(',');
            }
            write_csv_cell(output, cell);
        }
        output.push('\n');
    }

    fn write_csv_cell(output: &mut String, cell: &str) {
        if cell.chars().any(|ch| matches!(ch, ',' | '"' | '\n' | '\r')) {
            output.push('"');
            for ch in cell.chars() {
                if ch == '"' {
                    output.push_str("\"\"");
                } else {
                    output.push(ch);
                }
            }
            output.push('"');
        } else {
            output.push_str(cell);
        }
    }
}

/// Serialize a value to pretty JSON and print it, surfacing serialization errors.
fn print_json<T: serde::Serialize>(value: &T) -> Result<(), SnowError> {
    let text =
        serde_json::to_string_pretty(value).map_err(|err| SnowError::Api(err.to_string()))?;
    println!("{text}");
    Ok(())
}

/// Print a `serde_json::Value` as pretty JSON to stdout.
fn print_full_dump_or_inline(value: &serde_json::Value) {
    println!(
        "{}",
        serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
    );
}

fn confirm_action(prompt: &str) -> Result<bool, SnowError> {
    print!("{prompt} [y/N]: ");
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(matches!(
        input.trim().to_ascii_lowercase().as_str(),
        "y" | "yes"
    ))
}

fn print_repair_report(report: &RepairReport) {
    println!("repair-vault");
    println!("scanned: {}", report.scanned_records);
    println!("repaired: {}", report.repaired_records);
    println!("skipped: {}", report.skipped_records);
}

fn print_rebuild_report(report: &RebuildReport) {
    println!("rebuild-cache");
    println!("scanned documents: {}", report.scanned_documents);
    println!("rebuilt records: {}", report.rebuilt_records);
}

fn print_prune_report(report: &OrphanPruneReport) {
    println!("prune-orphans");
    println!("dry run: {}", report.dry_run);
    println!("orphan rows scanned: {}", report.orphan_rows_scanned);
    println!("orphan rows pruned: {}", report.orphan_rows_pruned);
    for orphan in &report.orphan_rows {
        println!("{} {} {:?}", orphan.number, orphan.sys_id, orphan.file_path);
    }
}

fn print_verification_report(report: &VaultVerificationReport) {
    println!("verify-vault");
    println!("scanned documents: {}", report.scanned_documents);
    println!("active records: {}", report.active_records);
    println!("projected references: {}", report.projected_references);
    println!(
        "projected relationships: {}",
        report.projected_relationships
    );
    println!(
        "projected enrichment rows: {}",
        report.projected_enrichment_rows
    );
    println!("degraded reads: {}", report.degraded_reads.len());
    println!(
        "missing markdown rows: {}",
        report.missing_markdown_rows.len()
    );
    println!("orphan record rows: {}", report.orphan_record_rows.len());
    println!(
        "unprojectable documents: {}",
        report.unprojectable_documents.len()
    );
    println!("unindexed documents: {}", report.unindexed_documents.len());
}

fn print_knowledge_article(article: &KnowledgeArticle) {
    print!("{}", format_knowledge_article(article, true));
}

fn print_knowledge_article_summary(article: &KnowledgeArticle) {
    print!("{}", format_knowledge_article(article, false));
}

fn print_knowledge_search_hit(hit: &KnowledgeSearchHit) {
    print!("{}", format_knowledge_search_hit(hit));
}

fn print_task_sla_status(status: &TaskSlaStatus) {
    print!("{}", format_task_sla_status(status));
}

fn format_task_sla_status(status: &TaskSlaStatus) -> String {
    let mut out = String::new();

    match status.readable {
        TaskSlaReadability::ParentNotFound => {
            let _ = writeln!(out, "Task SLA: {}", status.record_number);
            let _ = writeln!(out, "Record not found: {}", status.record_number);
        }
        TaskSlaReadability::ReadableRows => {
            write_task_sla_heading_and_summary(&mut out, status);
            write_task_sla_rows(&mut out, &status.rows);
        }
        TaskSlaReadability::EmptyOrAclRestricted => {
            write_task_sla_heading_and_summary(&mut out, status);
            let _ = writeln!(
                out,
                "No readable Task SLA rows or none attached; ServiceNow may also return no rows when Task SLAs are ACL-restricted."
            );
        }
        TaskSlaReadability::NotApplicable => {
            write_task_sla_heading_and_summary(&mut out, status);
            let _ = writeln!(
                out,
                "Task SLAs do not apply to this record type: {}",
                display_or_unknown(&status.record_table)
            );
        }
    }

    out
}

fn write_task_sla_heading_and_summary(out: &mut String, status: &TaskSlaStatus) {
    let _ = writeln!(
        out,
        "Task SLA: {} ({})",
        status.record_number,
        display_or_unknown(&status.record_table)
    );
    write_task_sla_summary(out, &status.summary);
}

fn write_task_sla_summary(out: &mut String, summary: &TaskSlaSummaryView) {
    let _ = writeln!(out, "summary:");
    let _ = writeln!(out, "  total: {}", summary.total);
    let _ = writeln!(out, "  active: {}", summary.active);
    let _ = writeln!(out, "  breached: {}", summary.breached);
    let _ = writeln!(
        out,
        "  next breach: {}",
        format_task_sla_next_breach(summary.next_breach.as_ref())
    );
    let _ = writeln!(
        out,
        "  highest business elapsed: {}",
        core_display::format_business_elapsed(summary.highest_business_elapsed)
    );
}

fn write_task_sla_rows(out: &mut String, rows: &[TaskSlaView]) {
    let _ = writeln!(out, "rows:");
    for (idx, row) in rows.iter().enumerate() {
        let _ = writeln!(
            out,
            "  {}. {}",
            idx + 1,
            display_optional(row.name.as_deref())
        );
        let _ = writeln!(
            out,
            "     stage: {}",
            display_optional(row.stage.as_deref())
        );
        let _ = writeln!(out, "     active: {}", display_bool(row.active));
        let _ = writeln!(out, "     breached: {}", display_bool(row.breached));
        let _ = writeln!(
            out,
            "     planned end: {}",
            display_optional(row.planned_end_time.as_deref())
        );
        let _ = writeln!(
            out,
            "     business elapsed: {}",
            core_display::format_business_elapsed(row.business_elapsed_percentage)
        );
        let _ = writeln!(
            out,
            "     time left: {}",
            core_display::format_time_left(row.time_left.as_deref())
        );
    }
}

fn format_task_sla_next_breach(row: Option<&TaskSlaView>) -> String {
    let Some(row) = row else {
        return "-".to_string();
    };

    format!(
        "{} ({}, time left {})",
        display_optional(row.planned_end_time.as_deref()),
        display_optional(row.name.as_deref()),
        core_display::format_time_left(row.time_left.as_deref())
    )
}

fn display_bool(value: Option<bool>) -> &'static str {
    match value {
        Some(true) => "yes",
        Some(false) => "no",
        None => "-",
    }
}

fn display_optional(value: Option<&str>) -> &str {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("-")
}

fn display_or_unknown(value: &str) -> &str {
    let value = value.trim();
    if value.is_empty() { "unknown" } else { value }
}

fn format_knowledge_article(article: &KnowledgeArticle, include_body: bool) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "{} [{}] {}",
        article.record.number, article.record.state, article.record.short_description
    );
    let _ = writeln!(
        out,
        "knowledge base: {}",
        article.knowledge_base.display_name
    );
    let _ = writeln!(out, "category: {}", article.category.display_name);
    let _ = writeln!(out, "type: {}", article.article_type);
    if let Some(author) = &article.author {
        let _ = writeln!(out, "author: {} ({})", author.display_name, author.sys_id);
    }
    if let Some(published_at) = article.published_at {
        let _ = writeln!(out, "published: {}", published_at.to_rfc3339());
    }
    if let Some(valid_to) = article.valid_to {
        let _ = writeln!(out, "valid to: {valid_to}");
    }
    if include_body {
        if !article.record.description.is_empty() {
            out.push('\n');
            out.push_str("Summary:\n");
            out.push_str(&article.record.description);
            out.push('\n');
        }
        if !article.content.is_empty() {
            out.push('\n');
            out.push_str("Content:\n");
            out.push_str(&article.content);
            out.push('\n');
        }
    }
    out
}

fn format_knowledge_search_hit(hit: &KnowledgeSearchHit) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "mode: {}", format_knowledge_search_mode(hit.mode));
    let _ = writeln!(out, "score: {:.3}", hit.score);
    if let Some(semantic_score) = hit.semantic_score {
        let _ = writeln!(out, "semantic score: {:.3}", semantic_score);
    }
    if let Some(lexical_score) = hit.lexical_score {
        let _ = writeln!(out, "lexical score: {:.3}", lexical_score);
    }
    let _ = writeln!(out, "coverage: {}", format_embedding_coverage(hit.coverage));
    out.push('\n');
    out.push_str(&format_knowledge_article(&hit.article, false));
    out
}

fn print_knowledge_base_summary(base: &KnowledgeBaseSummary) {
    println!(
        "{} [{}] {} articles",
        base.display_name, base.sys_id, base.article_count
    );
}

fn print_knowledge_category_summary(category: &KnowledgeCategorySummary) {
    println!(
        "{} [{}] {} articles",
        category.display_name, category.sys_id, category.article_count
    );
}

fn print_knowledge_sync_outcome(outcome: &snow_core::KnowledgeSyncOutcome) {
    println!("knowledge sync");
    println!(
        "mode: {}",
        match outcome.mode {
            snow_core::KnowledgeSyncMode::Full => "full",
            snow_core::KnowledgeSyncMode::Incremental => "incremental",
        }
    );
    println!(
        "with bodies: {}",
        if outcome.with_bodies { "yes" } else { "no" }
    );
    println!("status: {}", outcome.status);
    if let Some(details) = &outcome.details {
        println!("details: {details}");
    }
}

fn print_knowledge_semantic_status(status: &KnowledgeSemanticStatus) {
    println!("knowledge semantic status");
    println!("enabled: {}", if status.enabled { "yes" } else { "no" });
    println!("provider: {}", status.provider);
    println!("model: {}", status.model);
    println!("dimensions: {}", status.dimensions);
    println!("active KB articles: {}", status.active_kb_articles);
    println!("metadata embeddings: {}", status.metadata_embeddings);
    println!("full text embeddings: {}", status.full_text_embeddings);
    println!("stale rows: {}", status.stale_rows);
    println!("orphan rows: {}", status.orphan_rows);
    println!(
        "last rebuild: {}",
        status
            .last_rebuild_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_else(|| "never".to_string())
    );
    println!(
        "last error: {}",
        status.last_error.clone().unwrap_or_else(|| "-".to_string())
    );
}

fn print_knowledge_semantic_rebuild_summary(summary: &SemanticIndexSummary) {
    println!("knowledge semantic rebuild");
    println!("full rebuild: {}", if summary.full { "yes" } else { "no" });
    println!("indexed rows: {}", summary.indexed_rows);
    println!("metadata embeddings: {}", summary.metadata_embeddings);
    println!("full text embeddings: {}", summary.full_text_embeddings);
    println!("stale rows: {}", summary.stale_rows);
    println!("orphan rows: {}", summary.orphan_rows);
    println!(
        "last rebuild: {}",
        summary
            .last_rebuild_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_else(|| "never".to_string())
    );
    println!(
        "last error: {}",
        summary
            .last_error
            .clone()
            .unwrap_or_else(|| "-".to_string())
    );
}

fn format_knowledge_search_mode(mode: KnowledgeSearchMode) -> &'static str {
    match mode {
        KnowledgeSearchMode::Lexical => "lexical",
        KnowledgeSearchMode::Semantic => "semantic",
        KnowledgeSearchMode::Hybrid => "hybrid",
    }
}

fn format_embedding_coverage(coverage: KnowledgeEmbeddingCoverage) -> &'static str {
    match coverage {
        KnowledgeEmbeddingCoverage::Metadata => "metadata",
        KnowledgeEmbeddingCoverage::FullText => "full_text",
    }
}

fn cmd_knowledge_status(db_path: &std::path::Path) -> Result<(), SnowError> {
    let status = load_knowledge_status(db_path)?;
    println!("knowledge status");
    println!("articles: {}", status.article_count);
    println!("bodies cached: {}", status.body_cached_count);
    println!("knowledge bases: {}", status.knowledge_base_count);
    println!("categories: {}", status.category_count);
    println!(
        "last full sync: {}",
        status
            .last_full_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_else(|| "never".to_string())
    );
    println!(
        "last incremental sync: {}",
        status
            .last_incremental_at
            .map(|value| value.to_rfc3339())
            .unwrap_or_else(|| "never".to_string())
    );
    println!(
        "watermark updated_at: {}",
        status
            .watermark_updated_at
            .unwrap_or_else(|| "-".to_string())
    );
    println!(
        "watermark sys_id: {}",
        status.watermark_sys_id.unwrap_or_else(|| "-".to_string())
    );
    println!(
        "sync lock held: {}",
        if status.lock_held { "yes" } else { "no" }
    );
    if let Some(lock_timestamp_ms) = status.lock_timestamp_ms {
        println!("lock timestamp ms: {lock_timestamp_ms}");
    }
    Ok(())
}

fn cmd_knowledge_tags(
    db_path: &std::path::Path,
    layer: KnowledgeTagLayer,
    min_count: usize,
) -> Result<(), SnowError> {
    let tags = load_knowledge_tags(db_path, layer, min_count)?;
    if tags.is_empty() {
        println!("No knowledge tags found.");
        return Ok(());
    }

    for tag in tags {
        println!(
            "{}  {}  {}",
            tag.tag,
            tag.count,
            format_tag_layers(&tag.layers)
        );
    }
    Ok(())
}

fn open_runtime_db(path: &std::path::Path) -> AnyhowResult<Connection> {
    let _ = Store::open(path)?;
    Ok(Connection::open(path)?)
}

fn load_knowledge_status(db_path: &std::path::Path) -> AnyhowResult<KnowledgeStatusSnapshot> {
    let conn = open_runtime_db(db_path)?;
    let article_count = scalar_count(
        &conn,
        r#"
        SELECT COUNT(*)
        FROM knowledge_articles ka
        INNER JOIN records r ON r.sys_id = ka.record_sys_id
        WHERE r.in_scope = 1
        "#,
    )?;
    let body_cached_count = scalar_count(
        &conn,
        r#"
        SELECT COUNT(*)
        FROM knowledge_articles ka
        INNER JOIN records r ON r.sys_id = ka.record_sys_id
        WHERE r.in_scope = 1
          AND ka.body_cached = 1
        "#,
    )?;
    let knowledge_base_count = scalar_count(
        &conn,
        r#"
        SELECT COUNT(DISTINCT ka.knowledge_base_sys_id)
        FROM knowledge_articles ka
        INNER JOIN records r ON r.sys_id = ka.record_sys_id
        WHERE r.in_scope = 1
        "#,
    )?;
    let category_count = scalar_count(
        &conn,
        r#"
        SELECT COUNT(DISTINCT ka.category_sys_id)
        FROM knowledge_articles ka
        INNER JOIN records r ON r.sys_id = ka.record_sys_id
        WHERE r.in_scope = 1
        "#,
    )?;

    let status = conn.query_row(
        r#"
        SELECT last_full_at, last_incr_at, watermark_updated_at, watermark_sys_id, kb_sync_lock
        FROM kb_sync_state
        WHERE id = 1
        "#,
        [],
        |row| {
            Ok(KnowledgeStatusSnapshot {
                article_count,
                body_cached_count,
                knowledge_base_count,
                category_count,
                last_full_at: row.get::<_, Option<i64>>(0)?.and_then(decode_runtime_ts),
                last_incremental_at: row.get::<_, Option<i64>>(1)?.and_then(decode_runtime_ts),
                watermark_updated_at: row.get(2)?,
                watermark_sys_id: row.get(3)?,
                lock_held: row.get::<_, Option<i64>>(4)?.is_some(),
                lock_timestamp_ms: row.get(4)?,
            })
        },
    )?;
    Ok(status)
}

fn load_knowledge_tags(
    db_path: &std::path::Path,
    layer: KnowledgeTagLayer,
    min_count: usize,
) -> AnyhowResult<Vec<KnowledgeTagSummary>> {
    if min_count == 0 {
        anyhow::bail!("--min-count must be at least 1");
    }

    let conn = open_runtime_db(db_path)?;
    let mut stmt = conn.prepare(
        r#"
        SELECT sn_tags, auto_tags, user_tags
        FROM knowledge_articles
        ORDER BY number
        "#,
    )?;
    let mut rows = stmt.query([])?;
    let mut counts: HashMap<String, usize> = HashMap::new();
    let mut layers_by_tag: HashMap<String, BTreeSet<KnowledgeTagLayer>> = HashMap::new();

    while let Some(row) = rows.next()? {
        let mut tags_for_article: HashMap<String, BTreeSet<KnowledgeTagLayer>> = HashMap::new();
        for (column, tag_layer) in [
            (row.get::<_, String>(0)?, KnowledgeTagLayer::Sn),
            (row.get::<_, String>(1)?, KnowledgeTagLayer::Auto),
            (row.get::<_, String>(2)?, KnowledgeTagLayer::User),
        ] {
            if !knowledge_tag_layer_matches(layer, tag_layer) {
                continue;
            }
            for tag in parse_tag_json(&column)? {
                let normalized = tag.trim().to_ascii_lowercase();
                if normalized.is_empty() {
                    continue;
                }
                tags_for_article
                    .entry(normalized)
                    .or_default()
                    .insert(tag_layer);
            }
        }

        for (tag, tag_layers) in tags_for_article {
            *counts.entry(tag.clone()).or_default() += 1;
            layers_by_tag.entry(tag).or_default().extend(tag_layers);
        }
    }

    let mut tags = counts
        .into_iter()
        .filter(|(_, count)| *count >= min_count)
        .map(|(tag, count)| KnowledgeTagSummary {
            layers: layers_by_tag
                .remove(&tag)
                .unwrap_or_default()
                .into_iter()
                .collect(),
            tag,
            count,
        })
        .collect::<Vec<_>>();
    tags.sort_by(|left, right| {
        right
            .count
            .cmp(&left.count)
            .then_with(|| left.tag.cmp(&right.tag))
    });
    Ok(tags)
}

fn knowledge_tag_layer_matches(requested: KnowledgeTagLayer, actual: KnowledgeTagLayer) -> bool {
    match requested {
        KnowledgeTagLayer::All => true,
        KnowledgeTagLayer::Sn => matches!(actual, KnowledgeTagLayer::Sn),
        KnowledgeTagLayer::Auto => matches!(actual, KnowledgeTagLayer::Auto),
        KnowledgeTagLayer::User => matches!(actual, KnowledgeTagLayer::User),
    }
}

fn format_tag_layers(layers: &[KnowledgeTagLayer]) -> String {
    layers
        .iter()
        .map(|layer| match layer {
            KnowledgeTagLayer::All => "all",
            KnowledgeTagLayer::Sn => "sn",
            KnowledgeTagLayer::Auto => "auto",
            KnowledgeTagLayer::User => "user",
        })
        .collect::<Vec<_>>()
        .join(",")
}

fn parse_tag_json(raw: &str) -> AnyhowResult<Vec<String>> {
    Ok(serde_json::from_str(raw)?)
}

fn scalar_count(conn: &Connection, query: &str) -> AnyhowResult<usize> {
    Ok(conn.query_row(query, [], |row| row.get::<_, i64>(0))? as usize)
}

fn decode_runtime_ts(raw: i64) -> Option<DateTime<Utc>> {
    if raw >= 1_000_000_000_000 {
        Utc.timestamp_millis_opt(raw).single()
    } else {
        Utc.timestamp_opt(raw, 0).single()
    }
}

#[derive(Debug, Clone)]
struct KnowledgeStatusSnapshot {
    article_count: usize,
    body_cached_count: usize,
    knowledge_base_count: usize,
    category_count: usize,
    last_full_at: Option<DateTime<Utc>>,
    last_incremental_at: Option<DateTime<Utc>>,
    watermark_updated_at: Option<String>,
    watermark_sys_id: Option<String>,
    lock_held: bool,
    lock_timestamp_ms: Option<i64>,
}

#[derive(Debug, Clone)]
struct KnowledgeTagSummary {
    tag: String,
    count: usize,
    layers: Vec<KnowledgeTagLayer>,
}

fn print_approval_record(approval: &ApprovalRecord) {
    println!(
        "{} [{}] {}",
        approval.record.number, approval.record.state, approval.record.short_description
    );
    println!("approver: {}", approval.approver.display_name);
    println!("target: {}", approval.target.number);
    match approval.routed_via {
        snow_core::ApprovalRoutedVia::Direct => println!("routed via: direct"),
        snow_core::ApprovalRoutedVia::Group => {
            let group = approval
                .approver_group
                .as_ref()
                .map(|group| group.display_name.as_str())
                .unwrap_or(approval.approver.display_name.as_str());
            println!("routed via: group ({group})");
        }
    }
    println!("requested at: {}", approval.requested_at);
    if let Some(due_date) = approval.due_date {
        println!("due date: {due_date}");
    }
}

#[allow(clippy::too_many_arguments)]
mod show;

use show::*;

async fn cmd_approve(
    client: &ServiceNowClient,
    username: &str,
    number: &str,
    skip_confirm: bool,
) -> Result<(), SnowError> {
    let (table, record) = get_by_number(client, number).await?;

    let title = record
        .get_str("short_description")
        .unwrap_or("(no description)");
    println!("{} — {}", number.bold(), title);

    let user_record = client
        .table("sys_user")
        .equals("user_name", username)
        .fields(&["sys_id"])
        .limit(1)
        .first()
        .await?
        .ok_or_else(|| {
            SnowError::UserNotFound(format!("User {username} not found in ServiceNow."))
        })?;

    if !skip_confirm {
        print!("Approve {number}? [y/N] ");
        io::stdout().flush().unwrap();
        let mut input = String::new();
        io::stdin().read_line(&mut input).unwrap();
        if input.trim().to_lowercase() != "y" {
            println!("Cancelled.");
            return Ok(());
        }
    }

    client
        .approve(&table, &record.sys_id, &user_record.sys_id)
        .execute()
        .await?;
    println!("{}", "Approved.".green().bold());
    Ok(())
}

async fn cmd_note(
    client: &ServiceNowClient,
    number: &str,
    message: &str,
    dry_run: bool,
) -> Result<(), SnowError> {
    let (table, record) = get_by_number(client, number).await?;

    if dry_run {
        println!("{}", "Dry run — no changes will be made.".yellow().bold());
        println!("  Table:  {table}");
        println!("  Sys ID: {}", record.sys_id);
        println!("  Number: {number}");
        println!("  Action: PATCH /api/now/table/{table}/{}", record.sys_id);
        println!("  Body:   {{\"work_notes\": {:?}}}", message);
        return Ok(());
    }

    client
        .add_work_note(&table, &record.sys_id, message)
        .await?;
    println!("{} Work note added to {}.", "Done.".green().bold(), number);
    Ok(())
}

fn config_path(filename: &str) -> PathBuf {
    daemon_cmd::paths::config_path(filename)
}

#[cfg(test)]
mod tests;
