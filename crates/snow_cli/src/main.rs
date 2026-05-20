#![allow(clippy::arc_with_non_send_sync)]

mod admin;
mod auth;
mod cli;
mod daemon_cmd;
mod display;
mod error;
#[path = "tui/mod.rs"]
mod tui_app;
mod tui_client;

use std::collections::{BTreeSet, HashMap};
use std::fmt::Write as _;
use std::io::{self, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result as AnyhowResult;
use chrono::{DateTime, TimeZone, Utc};
use clap::Parser;
use colored::Colorize;
use rusqlite::Connection;
use servicenow_rs::prelude::{
    BasicAuth, DisplayValue, JournalEntry, Order, PrefixRegistry, Record, ServiceNowClient,
};
use snow_core::cache::store::Store;
use snow_core::display as core_display;
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
};

use cli::{
    Cli, Command, KnowledgeCommand, KnowledgeSearchModeArg, KnowledgeSemanticCommand,
    KnowledgeTagLayer,
};
use error::SnowError;
use tui_client::TuiClient;

struct AuthContext {
    env_name: String,
    instance: String,
    username: String,
    credential: auth::CredentialProvider,
    password: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ShowTarget {
    Project,
    Demand,
    Incident,
    Request,
    RequestItem,
    Story,
    Task,
    Knowledge,
    ResourcePlan,
    Change,
}

fn main() {
    let cli = Cli::parse();

    if let Err(e) = run_entry(cli) {
        eprintln!("{} {e}", "Error:".red().bold());
        std::process::exit(1);
    }
}

fn run_entry(cli: Cli) -> Result<(), SnowError> {
    // Daemon lifecycle commands fork the current process. Keep them outside any
    // Tokio runtime so the child can build the daemon runtime from a clean slate.
    if matches!(cli.command, Command::Daemon { .. }) {
        let Command::Daemon { action } = cli.command else {
            unreachable!()
        };
        let env_name = daemon_cmd::paths::selected_env(cli.env.as_deref());
        return daemon_cmd::dispatch(action, &env_name).map_err(SnowError::from);
    }

    if matches!(cli.command, Command::CacheInfo) {
        return cmd_cache_info();
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
        .unwrap_or_else(|| selected_env_name(&cli));

    if let Command::Tui {
        refresh,
        show_closed,
        daemon,
        socket_path,
    } = &cli.command
        && (*daemon || socket_path.is_some())
    {
        let paths = runtime_paths();
        let instance_url = std::env::var("SNOW_INSTANCE")
            .or_else(|_| std::env::var("SERVICENOW_INSTANCE"))
            .ok();
        let socket = socket_path.clone().unwrap_or(paths.socket);
        let tui_client = Arc::new(TuiClient::remote(socket, instance_url));
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

    let AuthContext {
        instance,
        username,
        credential,
        password,
        ..
    } = auth_context.ok_or_else(|| {
        SnowError::Api("credentials were not prepared for this command".to_string())
    })?;

    // Build client
    let client = ServiceNowClient::builder()
        .instance(&instance)
        .auth(BasicAuth::new(&username, &password))
        .build()
        .await?;
    let core_client = ServiceNowClient::builder()
        .instance(&instance)
        .auth(BasicAuth::new(&username, &password))
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
}

fn runtime_paths() -> RuntimePaths {
    let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
    let root = home.join(".config/snow");
    RuntimePaths {
        vault: root.join("vault"),
        database: root.join("snow.db"),
        socket: root.join("daemon.sock"),
        root,
    }
}

fn selected_env_name(cli: &Cli) -> String {
    cli.env
        .clone()
        .or_else(|| std::env::var("SNOW_ENV").ok())
        .unwrap_or_else(|| daemon_cmd::paths::selected_env(None))
}

fn command_uses_local_credentials(command: &Command) -> bool {
    match command {
        Command::Daemon { .. } | Command::Admin | Command::CacheInfo => false,
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
    let config_dir_hint = dirs::home_dir()
        .map(|d| d.join(".config/snow").display().to_string())
        .unwrap_or_default();
    dotenvy::from_path(&env_path).map_err(|e| {
        SnowError::Api(format!(
            "Failed to load {env_file}: {e}.\n  Searched: next to executable, {config_dir_hint}, and current directory"
        ))
    })?;

    let instance = std::env::var("SNOW_INSTANCE")
        .map_err(|_| SnowError::Api("SNOW_INSTANCE not set in .env file".to_string()))?;
    let username = std::env::var("SNOW_USER")
        .map_err(|_| SnowError::Api("SNOW_USER not set in .env file".to_string()))?;
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
    let schema_version = if database_exists {
        match Store::open(&paths.database).and_then(|store| store.schema_version()) {
            Ok(version) => version
                .map(|value| value.to_string())
                .unwrap_or_else(|| "n/a".to_string()),
            Err(err) => format!("unavailable ({err})"),
        }
    } else {
        "n/a".to_string()
    };

    println!("Runtime Root: {}", paths.root.display());
    println!("Vault Path: {}", paths.vault.display());
    println!("DB Path: {}", paths.database.display());
    println!("Socket Path: {}", paths.socket.display());
    println!("Vault Exists: {}", if vault_exists { "yes" } else { "no" });
    println!("DB Exists: {}", if database_exists { "yes" } else { "no" });
    println!("Schema Version: {schema_version}");
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
    println!("requested at: {}", approval.requested_at);
    if let Some(due_date) = approval.due_date {
        println!("due date: {due_date}");
    }
}

async fn cmd_show(
    core: &SnowCore,
    client: &ServiceNowClient,
    username: &str,
    number: &str,
    extras: &[String],
    resource_plan_state: Option<&str>,
    smart: bool,
    full: bool,
) -> Result<(), SnowError> {
    if is_show_sla_alias(extras) {
        return cmd_sla(core, number).await;
    }

    match classify_show_target(number) {
        ShowTarget::Project => {
            cmd_show_project(client, number, extras, resource_plan_state, full).await
        }
        ShowTarget::Demand => cmd_show_demand(client, number, extras, full).await,
        ShowTarget::Incident => cmd_show_incident(client, number, extras, full).await,
        ShowTarget::Request => cmd_show_request(client, number, extras, full).await,
        ShowTarget::RequestItem => cmd_show_request_item(client, number, extras, full).await,
        ShowTarget::Story => cmd_show_story(client, number, extras, full).await,
        ShowTarget::Task => cmd_show_task(client, number, extras, full).await,
        ShowTarget::Knowledge => {
            if full || !extras.is_empty() {
                cmd_show_knowledge(client, number, extras, full).await
            } else {
                cmd_show_knowledge_runtime(core, number, false).await
            }
        }
        ShowTarget::ResourcePlan => cmd_show_resource_plan(client, number, extras, full).await,
        ShowTarget::Change => cmd_show_change(client, username, number, extras, smart, full).await,
    }
}

fn is_show_sla_alias(extras: &[String]) -> bool {
    matches!(extras, [extra] if extra.eq_ignore_ascii_case("sla"))
}

fn classify_show_target(number: &str) -> ShowTarget {
    if number.starts_with("PRJ") {
        ShowTarget::Project
    } else if number.starts_with("DMND") {
        ShowTarget::Demand
    } else if number.starts_with("INC") {
        ShowTarget::Incident
    } else if number.starts_with("REQ") {
        ShowTarget::Request
    } else if number.starts_with("RITM") {
        ShowTarget::RequestItem
    } else if number.starts_with("STRY") {
        ShowTarget::Story
    } else if number.starts_with("SCTASK") || number.starts_with("TASK") {
        ShowTarget::Task
    } else if number.starts_with("KB") {
        ShowTarget::Knowledge
    } else if number.starts_with("RPLN") {
        ShowTarget::ResourcePlan
    } else {
        ShowTarget::Change
    }
}

/// Resolve a record number to its ServiceNow table name.
fn resolve_table(number: &str) -> Result<String, SnowError> {
    let registry = PrefixRegistry::default();
    registry
        .table_for_number(number)
        .map(|s| s.to_string())
        .ok_or_else(|| SnowError::Api(format!("Unknown record prefix in '{number}'.")))
}

/// Look up a record by number, resolving the table from the prefix.
async fn get_by_number(
    client: &ServiceNowClient,
    number: &str,
) -> Result<(String, Record), SnowError> {
    let table = resolve_table(number)?;
    let record = client
        .table(&table)
        .equals("number", number)
        .fields(&["sys_id", "number", "short_description", "state"])
        .display_value(DisplayValue::Display)
        .limit(1)
        .first()
        .await?
        .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;
    Ok((table, record))
}

/// Resolve friendly extra names to ServiceNow journal field names.
/// Returns (journal_fields, raw_fields) — journal fields are fetched from sys_journal_field,
/// raw fields are fetched as additional fields on the record itself.
fn resolve_extras(extras: &[String]) -> (Vec<&'static str>, Vec<String>) {
    let mut journal_fields = Vec::new();
    let mut raw_fields = Vec::new();
    for extra in extras {
        match extra.to_lowercase().as_str() {
            "activity" => {
                journal_fields.push("work_notes");
                journal_fields.push("comments");
            }
            "notes" | "worknotes" | "work_notes" => {
                journal_fields.push("work_notes");
            }
            "comments" => {
                journal_fields.push("comments");
            }
            _ => raw_fields.push(extra.clone()),
        }
    }
    journal_fields.dedup();
    (journal_fields, raw_fields)
}

async fn fetch_and_print_extras(
    client: &ServiceNowClient,
    record: &Record,
    table: &str,
    extras: &[String],
) -> Result<(), SnowError> {
    if extras.is_empty() {
        return Ok(());
    }
    let (journal_fields, raw_fields) = resolve_extras(extras);

    // Fetch journal fields using journal_inline (reads directly from record table,
    // avoids ACL-restricted sys_journal_field).
    if !journal_fields.is_empty() {
        let field_names: Vec<&str> = journal_fields.to_vec();
        let journal_record = client
            .journal_inline(table, &record.sys_id, &field_names)
            .first()
            .await?;
        if let Some(rec) = journal_record {
            let mut found = false;
            for jf in &journal_fields {
                if let Some(val) = rec.get_str(jf)
                    && !val.trim().is_empty()
                {
                    found = true;
                    display::print_multiline_field_pub(jf, Some(val));
                }
            }
            if !found {
                println!("\n{}", "No journal entries found.".dimmed());
            }
        } else {
            println!("\n{}", "No journal entries found.".dimmed());
        }
    }

    // Fetch raw fields by re-querying the record with those fields
    if !raw_fields.is_empty() {
        let field_names: Vec<&str> = raw_fields.iter().map(|s| s.as_str()).collect();
        let extra_record = client
            .table(table)
            .equals("sys_id", &record.sys_id)
            .fields(&field_names)
            .display_value(DisplayValue::Display)
            .limit(1)
            .first()
            .await?;
        if let Some(rec) = extra_record {
            for field in &raw_fields {
                display::print_multiline_field_pub(field, rec.get_str(field));
            }
        }
    }

    Ok(())
}

async fn cmd_show_change(
    client: &ServiceNowClient,
    username: &str,
    number: &str,
    extras: &[String],
    smart: bool,
    full: bool,
) -> Result<(), SnowError> {
    let (table, summary_record) = get_by_number(client, number).await?;
    if table != "change_request" {
        return Err(SnowError::NotFound(format!("{number} not found.")));
    }

    if full {
        let cr = client
            .table("change_request")
            .display_value(DisplayValue::Display)
            .get(&summary_record.sys_id)
            .await?;

        let tasks = client
            .table("change_task")
            .equals("change_request", &cr.sys_id)
            .display_value(DisplayValue::Display)
            .execute()
            .await?;

        let mut full_output = display::record_to_json(&cr);
        full_output["_tasks"] = serde_json::to_value(
            tasks
                .records
                .iter()
                .map(display::record_to_json)
                .collect::<Vec<_>>(),
        )
        .unwrap_or_default();
        display::print_full_dump(&full_output);
        return Ok(());
    }

    let fields = &[
        "sys_id",
        "number",
        "short_description",
        "state",
        "category",
        "start_date",
        "end_date",
        "assigned_to",
        "description",
        "change_plan",
        "implementation_plan",
        "backout_plan",
    ];
    let cr = client
        .table("change_request")
        .fields(fields)
        .display_value(DisplayValue::Display)
        .get(&summary_record.sys_id)
        .await?;
    display::print_change_summary(&cr);
    fetch_and_print_extras(client, &cr, "change_request", extras).await?;

    if smart {
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
        let user_sys_id = &user_record.sys_id;

        let task_result = client
            .table("change_task")
            .equals("change_request", &cr.sys_id)
            .display_value(DisplayValue::Display)
            .execute()
            .await?;
        let my_tasks: Vec<&Record> = task_result
            .records
            .iter()
            .filter(|r| {
                r.get_str("assigned_to")
                    .is_some_and(|a| a.contains(username))
            })
            .collect();

        if !my_tasks.is_empty() {
            println!("\n{}", "Your Tasks:".bold().underline());
            let refs: Vec<Record> = my_tasks.into_iter().cloned().collect();
            display::print_tasks(&refs);
        }

        let approval_result = client
            .table("sysapproval_approver")
            .equals("document_id", &cr.sys_id)
            .equals("approver", user_sys_id)
            .display_value(DisplayValue::Display)
            .execute()
            .await?;
        display::print_approval_records(&approval_result.records);
    }

    // Interactive prompt
    println!("\n[a] Approve  [d] More details  [n] Add note  [q] Quit");
    print!("> ");
    io::stdout().flush().unwrap();
    let mut input = String::new();
    io::stdin().read_line(&mut input).unwrap();
    match input.trim().to_lowercase().as_str() {
        "a" => cmd_approve(client, username, number, false).await?,
        "d" => {
            let cr = client
                .table("change_request")
                .display_value(DisplayValue::Display)
                .get(&summary_record.sys_id)
                .await?;
            display::print_full_dump(&display::record_to_json(&cr));
        }
        "n" => {
            print!("Note: ");
            io::stdout().flush().unwrap();
            let mut note = String::new();
            io::stdin().read_line(&mut note).unwrap();
            let note = note.trim();
            if !note.is_empty() {
                cmd_note(client, number, note, false).await?;
            }
        }
        _ => {}
    }

    Ok(())
}

async fn cmd_show_project(
    client: &ServiceNowClient,
    number: &str,
    extras: &[String],
    resource_plan_state: Option<&str>,
    full: bool,
) -> Result<(), SnowError> {
    if full {
        let project = client
            .table("pm_project")
            .equals("number", number)
            .display_value(DisplayValue::Display)
            .limit(1)
            .first()
            .await?
            .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;
        display::print_full_dump(&display::record_to_json(&project));
        return Ok(());
    }

    let fields = &[
        "sys_id",
        "number",
        "name",
        "short_description",
        "state",
        "demand",
        "project_manager",
        "start_date",
        "end_date",
        "percent_complete",
        "description",
        "goals",
        "business_case",
    ];
    let project = client
        .table("pm_project")
        .equals("number", number)
        .fields(fields)
        .display_value(DisplayValue::Display)
        .limit(1)
        .first()
        .await?
        .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;
    display::print_project_summary(&project);
    let resource_plans =
        fetch_project_resource_plans(client, &project, resource_plan_state).await?;
    print_project_resource_plans(&resource_plans, resource_plan_state);
    fetch_and_print_extras(client, &project, "pm_project", extras).await?;
    Ok(())
}

async fn fetch_project_resource_plans(
    client: &ServiceNowClient,
    project: &Record,
    state_filter: Option<&str>,
) -> Result<Vec<Record>, SnowError> {
    let fields = &[
        "sys_id",
        "number",
        "short_description",
        "state",
        "task",
        "resource_type",
        "user_resource",
        "group_resource",
        "start_date",
        "end_date",
        "planned_hours",
        "allocated_hours",
        "confirmed_hours",
    ];
    let plans = client
        .table("resource_plan")
        .equals("task", &project.sys_id)
        .fields(fields)
        .display_value(DisplayValue::Both)
        .order_by("number", Order::Asc)
        .limit(500)
        .execute()
        .await?
        .records
        .into_iter()
        .filter(|plan| resource_plan_matches_state(plan, state_filter))
        .collect();
    Ok(plans)
}

fn resource_plan_matches_state(plan: &Record, state_filter: Option<&str>) -> bool {
    let Some(state_filter) = state_filter
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return true;
    };
    [
        plan.get_str("state"),
        plan.get_display("state"),
        plan.get_raw("state"),
    ]
    .into_iter()
    .flatten()
    .any(|state| state.eq_ignore_ascii_case(state_filter))
}

fn print_project_resource_plans(plans: &[Record], state_filter: Option<&str>) {
    let filter = state_filter
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("All");
    println!("\nResource Plans (state: {filter})");
    if plans.is_empty() {
        println!("  No resource plans found.");
        return;
    }

    for plan in plans {
        let number = plan.get_str("number").unwrap_or("-");
        let state = plan
            .get_display("state")
            .or(plan.get_str("state"))
            .unwrap_or("-");
        let resource = plan
            .get_str("user_resource")
            .or(plan.get_str("group_resource"))
            .or(plan.get_str("resource_type"))
            .unwrap_or("-");
        let start = plan.get_str("start_date").unwrap_or("-");
        let end = plan.get_str("end_date").unwrap_or("-");
        let planned = plan.get_str("planned_hours").unwrap_or("-");
        let allocated = plan.get_str("allocated_hours").unwrap_or("-");
        println!(
            "  {number} [{state}] resource:{resource} window:{start}..{end} planned:{planned} allocated:{allocated}"
        );
    }
}

async fn cmd_show_demand(
    client: &ServiceNowClient,
    number: &str,
    extras: &[String],
    full: bool,
) -> Result<(), SnowError> {
    if full {
        let demand = client
            .table("dmn_demand")
            .equals("number", number)
            .display_value(DisplayValue::Display)
            .limit(1)
            .first()
            .await?
            .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;
        display::print_full_dump(&display::record_to_json(&demand));
        return Ok(());
    }

    let fields = &[
        "sys_id",
        "number",
        "short_description",
        "state",
        "priority",
        "requested_by",
        "start_date",
        "end_date",
        "description",
        "business_case",
    ];
    let demand = client
        .table("dmn_demand")
        .equals("number", number)
        .fields(fields)
        .display_value(DisplayValue::Display)
        .limit(1)
        .first()
        .await?
        .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;
    display::print_demand_summary(&demand);
    fetch_and_print_extras(client, &demand, "dmn_demand", extras).await?;
    Ok(())
}

async fn cmd_show_incident(
    client: &ServiceNowClient,
    number: &str,
    extras: &[String],
    full: bool,
) -> Result<(), SnowError> {
    let (table, summary_record) = get_by_number(client, number).await?;
    if table != "incident" {
        return Err(SnowError::NotFound(format!("{number} not found.")));
    }

    if full {
        let incident = client
            .table("incident")
            .display_value(DisplayValue::Display)
            .get(&summary_record.sys_id)
            .await?;
        display::print_full_dump(&display::record_to_json(&incident));
        return Ok(());
    }

    let fields = &[
        "sys_id",
        "number",
        "short_description",
        "state",
        "priority",
        "severity",
        "category",
        "subcategory",
        "assigned_to",
        "assignment_group",
        "caller_id",
        "opened_at",
        "resolved_at",
        "close_code",
        "description",
    ];
    let incident = client
        .table("incident")
        .fields(fields)
        .display_value(DisplayValue::Display)
        .get(&summary_record.sys_id)
        .await?;
    display::print_incident_summary(&incident);
    fetch_and_print_extras(client, &incident, "incident", extras).await?;
    Ok(())
}

async fn cmd_show_request_item(
    client: &ServiceNowClient,
    number: &str,
    extras: &[String],
    full: bool,
) -> Result<(), SnowError> {
    if full {
        let ritm = client
            .table("sc_req_item")
            .equals("number", number)
            .display_value(DisplayValue::Display)
            .limit(1)
            .first()
            .await?
            .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;
        display::print_full_dump(&display::record_to_json(&ritm));
        return Ok(());
    }

    let fields = &[
        "sys_id",
        "number",
        "short_description",
        "state",
        "priority",
        "assigned_to",
        "assignment_group",
        "request",
        "requested_for",
        "cat_item",
        "opened_at",
        "due_date",
        "stage",
        "approval",
        "description",
    ];
    let ritm = client
        .table("sc_req_item")
        .equals("number", number)
        .fields(fields)
        .display_value(DisplayValue::Display)
        .limit(1)
        .first()
        .await?
        .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;
    display::print_request_item_summary(&ritm);

    // Fetch and display catalog variables, resolving reference sys_ids to names
    let sys_id = ritm.get_str("sys_id").unwrap_or_default();
    let mut variables = client.catalog_variables(sys_id).await?;
    client.resolve_catalog_variables(&mut variables).await?;
    display::print_variables(&variables);

    // Fetch and display last 5 activity entries (Additional Comments only, no Email sent)
    let journal_rec = client
        .journal_inline("sc_req_item", sys_id, &["comments"])
        .first()
        .await?;
    if let Some(rec) = &journal_rec {
        let entries: Vec<JournalEntry> = rec
            .parse_journal("comments")
            .into_iter()
            .filter(|e| !e.is_email())
            .take(5)
            .collect();
        display::print_activity(&entries);
    }

    fetch_and_print_extras(client, &ritm, "sc_req_item", extras).await?;
    Ok(())
}

async fn cmd_show_request(
    client: &ServiceNowClient,
    number: &str,
    extras: &[String],
    full: bool,
) -> Result<(), SnowError> {
    if full {
        let req = client
            .table("sc_request")
            .equals("number", number)
            .display_value(DisplayValue::Display)
            .limit(1)
            .first()
            .await?
            .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;
        display::print_full_dump(&display::record_to_json(&req));
        return Ok(());
    }

    let fields = &[
        "sys_id",
        "number",
        "short_description",
        "state",
        "requested_for",
        "requested_by",
        "opened_at",
        "due_date",
        "stage",
        "approval",
        "description",
    ];
    let req = client
        .table("sc_request")
        .equals("number", number)
        .fields(fields)
        .display_value(DisplayValue::Display)
        .limit(1)
        .first()
        .await?
        .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;

    println!("Number: {}", req.get_str("number").unwrap_or(number));
    println!(
        "Title: {}",
        req.get_str("short_description")
            .unwrap_or("(no description)")
    );
    println!("State: {}", req.get_str("state").unwrap_or("-"));
    println!(
        "Requested For: {}",
        req.get_str("requested_for").unwrap_or("-")
    );
    println!(
        "Requested By: {}",
        req.get_str("requested_by").unwrap_or("-")
    );
    if let Some(opened_at) = req.get_str("opened_at") {
        println!("Opened: {opened_at}");
    }
    if let Some(due_date) = req.get_str("due_date") {
        println!("Due Date: {due_date}");
    }
    if let Some(stage) = req.get_str("stage") {
        println!("Stage: {stage}");
    }
    if let Some(approval) = req.get_str("approval") {
        println!("Approval: {approval}");
    }

    let description = req
        .get_str("description")
        .map(display::strip_html)
        .unwrap_or_default();
    if !description.trim().is_empty() {
        println!("\nDescription:\n{description}");
    }

    fetch_and_print_extras(client, &req, "sc_request", extras).await?;
    Ok(())
}

async fn cmd_show_task(
    client: &ServiceNowClient,
    number: &str,
    extras: &[String],
    full: bool,
) -> Result<(), SnowError> {
    if full {
        let task = client
            .table("sc_task")
            .equals("number", number)
            .display_value(DisplayValue::Display)
            .limit(1)
            .first()
            .await?
            .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;
        display::print_full_dump(&display::record_to_json(&task));
        return Ok(());
    }

    let fields = &[
        "sys_id",
        "number",
        "short_description",
        "state",
        "priority",
        "assigned_to",
        "assignment_group",
        "request_item",
        "opened_at",
        "due_date",
        "description",
    ];
    let task = client
        .table("sc_task")
        .equals("number", number)
        .fields(fields)
        .display_value(DisplayValue::Display)
        .limit(1)
        .first()
        .await?
        .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;
    display::print_task_summary(&task);
    fetch_and_print_extras(client, &task, "sc_task", extras).await?;
    Ok(())
}

async fn cmd_show_story(
    client: &ServiceNowClient,
    number: &str,
    extras: &[String],
    full: bool,
) -> Result<(), SnowError> {
    if full {
        let story = client
            .table("rm_story")
            .equals("number", number)
            .display_value(DisplayValue::Display)
            .limit(1)
            .first()
            .await?
            .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;
        display::print_full_dump(&display::record_to_json(&story));
        return Ok(());
    }

    let fields = &[
        "sys_id",
        "number",
        "short_description",
        "state",
        "priority",
        "assigned_to",
        "story_points",
        "blocked",
        "sprint",
        "product",
        "epic",
        "acceptance_criteria",
        "description",
    ];
    let story = client
        .table("rm_story")
        .equals("number", number)
        .fields(fields)
        .display_value(DisplayValue::Display)
        .limit(1)
        .first()
        .await?
        .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;
    display::print_story_summary(&story);

    // Fetch story tasks (rm_scrum_task) linked to this story
    let sys_id = story.get_str("sys_id").unwrap_or_default();
    let task_result = client
        .table("rm_scrum_task")
        .equals("story", sys_id)
        .fields(&[
            "sys_id",
            "number",
            "short_description",
            "state",
            "assigned_to",
        ])
        .display_value(DisplayValue::Display)
        .execute()
        .await?;

    if !task_result.records.is_empty() {
        // Fetch work notes for each task
        let mut tasks_with_notes: Vec<(Record, Vec<JournalEntry>)> = Vec::new();
        for task in &task_result.records {
            let journal_rec = client
                .journal_inline("rm_scrum_task", &task.sys_id, &["work_notes"])
                .first()
                .await?;
            let entries = if let Some(rec) = &journal_rec {
                rec.parse_journal("work_notes")
                    .into_iter()
                    .filter(|e| !e.is_email())
                    .take(3)
                    .collect()
            } else {
                Vec::new()
            };
            tasks_with_notes.push((task.clone(), entries));
        }
        display::print_story_tasks(&tasks_with_notes);
    }

    fetch_and_print_extras(client, &story, "rm_story", extras).await?;
    Ok(())
}

async fn cmd_show_knowledge(
    client: &ServiceNowClient,
    number: &str,
    extras: &[String],
    full: bool,
) -> Result<(), SnowError> {
    if full {
        let article = client
            .table("kb_knowledge")
            .equals("number", number)
            .display_value(DisplayValue::Display)
            .limit(1)
            .first()
            .await?
            .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;
        display::print_full_dump(&display::record_to_json(&article));
        return Ok(());
    }

    let fields = &[
        "sys_id",
        "number",
        "short_description",
        "workflow_state",
        "kb_category",
        "kb_knowledge_base",
        "author",
        "published",
        "valid_to",
        "text",
        "article_body",
        "description",
    ];
    let article = client
        .table("kb_knowledge")
        .equals("number", number)
        .fields(fields)
        .display_value(DisplayValue::Display)
        .limit(1)
        .first()
        .await?
        .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;

    println!("Number: {}", article.get_str("number").unwrap_or(number));
    println!(
        "Title: {}",
        article
            .get_str("short_description")
            .unwrap_or("(no description)")
    );
    println!(
        "State: {}",
        article
            .get_str("workflow_state")
            .or(article.get_str("state"))
            .unwrap_or("-")
    );
    println!(
        "Knowledge Base: {}",
        article.get_str("kb_knowledge_base").unwrap_or("-")
    );
    println!(
        "Category: {}",
        article.get_str("kb_category").unwrap_or("-")
    );
    println!("Author: {}", article.get_str("author").unwrap_or("-"));
    if let Some(published) = article.get_str("published") {
        println!("Published: {published}");
    }
    if let Some(valid_to) = article.get_str("valid_to") {
        println!("Valid To: {valid_to}");
    }

    let body = article
        .get_str("article_body")
        .or(article.get_str("text"))
        .or(article.get_str("description"))
        .map(display::strip_html)
        .unwrap_or_default();
    if !body.trim().is_empty() {
        println!("\nArticle:\n{body}");
    }

    fetch_and_print_extras(client, &article, "kb_knowledge", extras).await?;
    Ok(())
}

async fn cmd_show_resource_plan(
    client: &ServiceNowClient,
    number: &str,
    extras: &[String],
    full: bool,
) -> Result<(), SnowError> {
    if full {
        let plan = client
            .table("resource_plan")
            .equals("number", number)
            .display_value(DisplayValue::Display)
            .limit(1)
            .first()
            .await?
            .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;
        display::print_full_dump(&display::record_to_json(&plan));
        return Ok(());
    }

    let fields = &[
        "sys_id",
        "number",
        "short_description",
        "state",
        "task",
        "resource_type",
        "user_resource",
        "group_resource",
        "start_date",
        "end_date",
        "planned_hours",
        "allocated_hours",
        "confirmed_hours",
        "description",
    ];
    let plan = client
        .table("resource_plan")
        .equals("number", number)
        .fields(fields)
        .display_value(DisplayValue::Display)
        .limit(1)
        .first()
        .await?
        .ok_or_else(|| SnowError::NotFound(format!("{number} not found.")))?;

    println!("Number: {}", plan.get_str("number").unwrap_or(number));
    println!(
        "Title: {}",
        plan.get_str("short_description")
            .unwrap_or("(no description)")
    );
    println!("State: {}", plan.get_str("state").unwrap_or("-"));
    println!("Task: {}", plan.get_str("task").unwrap_or("-"));
    println!(
        "Resource Type: {}",
        plan.get_str("resource_type").unwrap_or("-")
    );
    println!(
        "User Resource: {}",
        plan.get_str("user_resource").unwrap_or("-")
    );
    println!(
        "Group Resource: {}",
        plan.get_str("group_resource").unwrap_or("-")
    );
    if let Some(start_date) = plan.get_str("start_date") {
        println!("Start Date: {start_date}");
    }
    if let Some(end_date) = plan.get_str("end_date") {
        println!("End Date: {end_date}");
    }
    if let Some(planned_hours) = plan.get_str("planned_hours") {
        println!("Planned Hours: {planned_hours}");
    }
    if let Some(allocated_hours) = plan.get_str("allocated_hours") {
        println!("Allocated Hours: {allocated_hours}");
    }
    if let Some(confirmed_hours) = plan.get_str("confirmed_hours") {
        println!("Confirmed Hours: {confirmed_hours}");
    }

    let description = plan
        .get_str("description")
        .map(display::strip_html)
        .unwrap_or_default();
    if !description.trim().is_empty() {
        println!("\nDescription:\n{description}");
    }

    fetch_and_print_extras(client, &plan, "resource_plan", extras).await?;
    Ok(())
}

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
mod tests {
    use super::{
        KnowledgeStatusSnapshot, ShowTarget, classify_show_target, format_task_sla_status,
        is_show_sla_alias, load_knowledge_status, load_knowledge_tags,
    };
    use crate::cli::KnowledgeTagLayer;
    use chrono::TimeZone;
    use rusqlite::Connection;
    use snow_core::cache::store::{KnowledgeArticleRow, RecordRow, Store};
    use snow_core::{
        CacheSource, FieldValue, KnowledgeArticle, Reference, ResourceType, SnowRecord,
        TaskSlaReadability, TaskSlaStatus, TaskSlaSummaryView, TaskSlaView,
    };
    use std::collections::HashMap;
    use tempfile::tempdir;

    #[test]
    fn classify_show_target_routes_inc_and_chg_correctly() {
        assert_eq!(classify_show_target("INC4924830"), ShowTarget::Incident);
        assert_eq!(classify_show_target("CHG0329219"), ShowTarget::Change);
    }

    #[test]
    fn classify_show_target_routes_known_special_cases() {
        assert_eq!(classify_show_target("REQ2684923"), ShowTarget::Request);
        assert_eq!(classify_show_target("KB0101565"), ShowTarget::Knowledge);
        assert_eq!(
            classify_show_target("RPLN0091599"),
            ShowTarget::ResourcePlan
        );
    }

    #[test]
    fn task_sla_show_alias_requires_one_sla_extra() {
        assert!(is_show_sla_alias(&["sla".to_string()]));
        assert!(is_show_sla_alias(&["SLA".to_string()]));
        assert!(!is_show_sla_alias(&[]));
        assert!(!is_show_sla_alias(&[
            "sla".to_string(),
            "activity".to_string()
        ]));
    }

    #[test]
    fn task_sla_output_formats_readable_rows_in_product_order() {
        let first = task_sla_view(
            "First response",
            Some("in_progress"),
            Some(true),
            Some(false),
            "2026-05-08 12:00:00",
            Some(65.25),
            Some(" 1970-01-01 04:00:00 "),
        );
        let second = task_sla_view(
            "Second response",
            Some("completed"),
            Some(false),
            Some(true),
            "2026-05-09 12:00:00",
            Some(90.0),
            None,
        );
        let status = task_sla_status(
            TaskSlaReadability::ReadableRows,
            vec![first.clone(), second],
            TaskSlaSummaryView {
                total: 2,
                active: 1,
                breached: 1,
                next_breach: Some(first),
                highest_business_elapsed: Some(90.0),
            },
        );

        let rendered = format_task_sla_status(&status);

        assert!(rendered.contains("Task SLA: TASK000001 (task)"));
        assert!(rendered.contains("summary:\n  total: 2\n  active: 1\n  breached: 1"));
        assert!(rendered.contains(
            "next breach: 2026-05-08 12:00:00 (First response, time left 1970-01-01 04:00:00)"
        ));
        assert!(rendered.contains("highest business elapsed: 90%"));
        assert!(rendered.contains("business elapsed: 65.2%"));
        let first_pos = rendered.find("1. First response").unwrap();
        let second_pos = rendered.find("2. Second response").unwrap();
        assert!(first_pos < second_pos, "{rendered}");
    }

    #[test]
    fn task_sla_output_preserves_empty_or_acl_ambiguity() {
        let status = task_sla_status(
            TaskSlaReadability::EmptyOrAclRestricted,
            Vec::new(),
            zero_task_sla_summary(),
        );

        let rendered = format_task_sla_status(&status);

        assert!(rendered.contains("total: 0"));
        assert!(rendered.contains("No readable Task SLA rows or none attached"));
        assert!(rendered.contains("ACL-restricted"));
    }

    #[test]
    fn task_sla_output_reports_parent_not_found() {
        let status = TaskSlaStatus {
            record_number: "TASK000404".to_string(),
            record_table: String::new(),
            record_sys_id: String::new(),
            rows: Vec::new(),
            summary: zero_task_sla_summary(),
            readable: TaskSlaReadability::ParentNotFound,
        };

        let rendered = format_task_sla_status(&status);

        assert!(rendered.contains("Record not found: TASK000404"));
        assert!(!rendered.contains("summary:"));
    }

    #[test]
    fn task_sla_output_reports_not_applicable_record_type() {
        let status = TaskSlaStatus {
            record_number: "KB000001".to_string(),
            record_table: "kb_knowledge".to_string(),
            record_sys_id: "kb-sys-1".to_string(),
            rows: Vec::new(),
            summary: zero_task_sla_summary(),
            readable: TaskSlaReadability::NotApplicable,
        };

        let rendered = format_task_sla_status(&status);

        assert!(rendered.contains("Task SLAs do not apply to this record type: kb_knowledge"));
    }

    #[test]
    fn formats_knowledge_article_details_with_metadata() {
        let article = KnowledgeArticle {
            record: SnowRecord {
                sys_id: "kb-sys".to_string(),
                number: "KB0105015".to_string(),
                table: "kb_knowledge".to_string(),
                resource_type: ResourceType::Knowledge,
                state: "published".to_string(),
                short_description: "Windows server admin access".to_string(),
                description: "Summary body".to_string(),
                fields: HashMap::from([(
                    "workflow_state".to_string(),
                    FieldValue {
                        value: "published".to_string(),
                        display_value: Some("Published".to_string()),
                    },
                )]),
                work_notes: Vec::new(),
                comments: Vec::new(),
                parent: None,
                children: Vec::new(),
                references: HashMap::new(),
                synced_at: chrono::Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
                source: CacheSource::Disk,
            },
            knowledge_base: Reference {
                sys_id: "kb-base".to_string(),
                table: "kb_knowledge_base".to_string(),
                display_name: "IT Operations".to_string(),
                extra: HashMap::new(),
            },
            category: Reference {
                sys_id: "kb-cat".to_string(),
                table: "kb_category".to_string(),
                display_name: "Security".to_string(),
                extra: HashMap::new(),
            },
            article_type: "text".to_string(),
            content: "Step 1: Request access.".to_string(),
            sn_tags: vec!["password".to_string()],
            auto_tags: vec!["authentication".to_string()],
            user_tags: vec!["runbook".to_string()],
            body_cached: true,
            published_at: Some(chrono::Utc.timestamp_opt(1_712_649_800, 0).unwrap()),
            author: Some(Reference {
                sys_id: "user-1".to_string(),
                table: "sys_user".to_string(),
                display_name: "Jared Jennings".to_string(),
                extra: HashMap::new(),
            }),
            valid_to: Some(chrono::NaiveDate::from_ymd_opt(2027, 1, 1).unwrap()),
        };

        let rendered = super::format_knowledge_article(&article, true);
        assert!(rendered.contains("KB0105015 [published] Windows server admin access"));
        assert!(rendered.contains("knowledge base: IT Operations"));
        assert!(rendered.contains("author: Jared Jennings (user-1)"));
        assert!(rendered.contains("published: "));
        assert!(rendered.contains("valid to: 2027-01-01"));
        assert!(rendered.contains("Summary:"));
        assert!(rendered.contains("Content:"));

        let summary_only = super::format_knowledge_article(&article, false);
        assert!(!summary_only.contains("Summary:"));
        assert!(!summary_only.contains("Content:"));
    }

    #[test]
    fn load_knowledge_status_and_tags_read_runtime_catalog() {
        let tempdir = tempdir().expect("tempdir");
        let db_path = tempdir.path().join("snow.db");
        let store = Store::open(&db_path).expect("store");
        let now = chrono::Utc.timestamp_opt(1_712_649_600, 0).unwrap();

        store
            .upsert_record(
                &RecordRow::active(
                    "kb-sys",
                    "KB001",
                    "kb_knowledge",
                    ResourceType::Knowledge,
                    now,
                ),
                "",
                "Sample body",
            )
            .expect("record");
        store
            .upsert_knowledge_article(&KnowledgeArticleRow {
                record_sys_id: "kb-sys".to_string(),
                number: "KB001".to_string(),
                title: "Sample KB".to_string(),
                workflow_state: "published".to_string(),
                knowledge_base_sys_id: "kb-base".to_string(),
                knowledge_base_name: "IT".to_string(),
                category_sys_id: "kb-cat".to_string(),
                category_name: "Accounts".to_string(),
                author_sys_id: None,
                author_name: None,
                published_at: Some("2026-04-10 09:00:00".to_string()),
                valid_to: None,
                article_type: "text".to_string(),
                sys_updated_on: Some("2026-04-10 09:00:00".to_string()),
                sn_tags: vec!["password".to_string()],
                auto_tags: vec!["authentication".to_string()],
                user_tags: vec!["runbook".to_string()],
                body_cached: true,
            })
            .expect("knowledge article");
        store
            .upsert_record(
                &RecordRow::active(
                    "kb-old",
                    "KB000",
                    "kb_knowledge",
                    ResourceType::Knowledge,
                    now,
                ),
                "",
                "Old body",
            )
            .expect("old record");
        store
            .upsert_knowledge_article(&KnowledgeArticleRow {
                record_sys_id: "kb-old".to_string(),
                number: "KB000".to_string(),
                title: "Old KB".to_string(),
                workflow_state: "published".to_string(),
                knowledge_base_sys_id: "kb-base".to_string(),
                knowledge_base_name: "IT".to_string(),
                category_sys_id: "kb-old-cat".to_string(),
                category_name: "Legacy".to_string(),
                author_sys_id: None,
                author_name: None,
                published_at: Some("2026-04-09 09:00:00".to_string()),
                valid_to: None,
                article_type: "text".to_string(),
                sys_updated_on: Some("2026-04-09 09:00:00".to_string()),
                sn_tags: vec!["legacy".to_string()],
                auto_tags: Vec::new(),
                user_tags: Vec::new(),
                body_cached: true,
            })
            .expect("old knowledge article");
        store
            .tombstone_record("kb-old", now)
            .expect("tombstone old record");

        let conn = Connection::open(&db_path).expect("connection");
        conn.execute(
            r#"
            UPDATE kb_sync_state
            SET last_full_at = 1712649800000,
                last_incr_at = 1712650100000,
                watermark_updated_at = '2026-04-10 09:00:00',
                watermark_sys_id = 'kb-sys',
                kb_sync_lock = 1712650200000
            WHERE id = 1
            "#,
            [],
        )
        .expect("seed kb state");

        let status: KnowledgeStatusSnapshot = load_knowledge_status(&db_path).expect("status");
        assert_eq!(status.article_count, 1);
        assert_eq!(status.body_cached_count, 1);
        assert_eq!(status.knowledge_base_count, 1);
        assert_eq!(status.category_count, 1);
        assert!(status.lock_held);

        let tags = load_knowledge_tags(&db_path, KnowledgeTagLayer::All, 1).expect("tags");
        assert!(tags.iter().any(|tag| tag.tag == "password"));
        assert!(tags.iter().any(|tag| tag.tag == "authentication"));
        assert!(tags.iter().any(|tag| tag.tag == "runbook"));
    }

    fn task_sla_status(
        readable: TaskSlaReadability,
        rows: Vec<TaskSlaView>,
        summary: TaskSlaSummaryView,
    ) -> TaskSlaStatus {
        TaskSlaStatus {
            record_number: "TASK000001".to_string(),
            record_table: "task".to_string(),
            record_sys_id: "task-sys-1".to_string(),
            rows,
            summary,
            readable,
        }
    }

    fn task_sla_view(
        name: &str,
        stage: Option<&str>,
        active: Option<bool>,
        breached: Option<bool>,
        planned_end_time: &str,
        business_elapsed_percentage: Option<f64>,
        time_left: Option<&str>,
    ) -> TaskSlaView {
        TaskSlaView {
            name: Some(name.to_string()),
            stage: stage.map(str::to_string),
            active,
            breached,
            planned_end_time: Some(planned_end_time.to_string()),
            business_elapsed_percentage,
            time_left: time_left.map(str::to_string),
        }
    }

    fn zero_task_sla_summary() -> TaskSlaSummaryView {
        TaskSlaSummaryView {
            total: 0,
            active: 0,
            breached: 0,
            next_breach: None,
            highest_business_elapsed: None,
        }
    }
}
