mod bootstrap;
mod cache_command;
mod commands;
mod output;

use bootstrap::*;
use cache_command::*;
use commands::*;
use output::*;

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
    RepairReport, SemanticIndexSummary, ServiceNowCacheRebuildReport, SnowCore, TaskSlaReadability,
    TaskSlaStatus, TaskSlaSummaryView, TaskSlaView, VaultVerificationReport,
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
    if matches!(cli.command, Command::ImportCacheFromVault) {
        return cmd_import_cache_from_vault_offline();
    }
    if matches!(cli.command, Command::ResetCache) {
        return cmd_reset_cache_offline();
    }
    if matches!(cli.command, Command::RebuildCache) {
        ensure_cache_replacement_is_offline("rebuilding")?;
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
    let metadata_password = password.clone();
    drop(password);

    // Build client
    let mut client_builder = ServiceNowClient::builder()
        .instance(&instance)
        .auth(client_auth);
    let mut core_client_builder = ServiceNowClient::builder()
        .instance(&instance)
        .auth(core_auth);
    if allow_loopback_http_for_local_test(&instance) {
        client_builder = client_builder.allow_http();
        core_client_builder = core_client_builder.allow_http();
    }
    let client = client_builder.build().await?;
    let core_client = core_client_builder.build().await?;
    if matches!(cli.command, Command::RebuildCache) {
        return cmd_rebuild_cache_from_servicenow(
            &instance,
            &username,
            credential,
            metadata_password,
            core_client,
        )
        .await;
    }
    let core = Arc::new(
        build_core(
            &instance,
            &username,
            credential,
            metadata_password,
            core_client,
            None,
        )
        .await?,
    );
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
        Command::RebuildCache | Command::ImportCacheFromVault => {
            unreachable!("handled before normal core construction")
        }
        Command::ResetCache => unreachable!("handled before auth setup"),
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

#[allow(clippy::too_many_arguments)]
mod show;

use show::*;

#[cfg(test)]
mod tests;
