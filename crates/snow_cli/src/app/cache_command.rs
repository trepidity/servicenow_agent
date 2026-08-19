use super::*;
use crate::daemon_cmd::{client::endpoint_alive, paths::DaemonPaths};
use std::io::Write;
use std::sync::{Arc, Mutex};

pub(super) fn cmd_cache_info() -> Result<(), SnowError> {
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

pub(super) fn ensure_cache_replacement_is_offline(action: &str) -> Result<(), SnowError> {
    let daemon_paths = DaemonPaths::resolve().map_err(SnowError::from)?;
    if endpoint_alive(&daemon_paths) {
        return Err(SnowError::Api(format!(
            "daemon is running; stop it before {action} the cache"
        )));
    }
    Ok(())
}

pub(super) fn cmd_import_cache_from_vault_offline() -> Result<(), SnowError> {
    ensure_cache_replacement_is_offline("importing")?;
    let paths = runtime_paths();
    let report = snow_core::rebuild_cache_from_vault(&paths.vault, &paths.database)
        .map_err(|error| SnowError::Api(error.to_string()))?;
    print_rebuild_report(&report);
    Ok(())
}

pub(super) fn cmd_reset_cache_offline() -> Result<(), SnowError> {
    ensure_cache_replacement_is_offline("resetting")?;
    let paths = runtime_paths();
    snow_core::reset_cache(&paths.database).map_err(|error| SnowError::Api(error.to_string()))?;
    println!("reset-cache");
    println!("cache format: {}", snow_core::cache::store::CACHE_FORMAT_ID);
    println!("records: 0");
    Ok(())
}

pub(super) async fn cmd_rebuild_cache_from_servicenow(
    instance: &str,
    username: &str,
    credential: auth::CredentialProvider,
    metadata_password: snow_core::credential::SecretString,
    client: ServiceNowClient,
) -> Result<(), SnowError> {
    let paths = runtime_paths();
    let file_name = paths
        .database
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("snow.db");
    let staging = paths.database.with_file_name(format!(
        ".{file_name}.servicenow-rebuild-{}.tmp",
        uuid::Uuid::new_v4()
    ));
    let mut staging_guard = StagingCacheGuard::new(staging.clone());
    let progress_renderer = CacheRebuildProgressRenderer::new();
    progress_renderer
        .write_line("rebuild-cache: preparing ServiceNow staging cache")
        .map_err(render_progress_error)?;
    let progress_sink = progress_renderer.sink();

    let core = match build_core(
        instance,
        username,
        credential,
        metadata_password,
        client,
        Some(staging.clone()),
    )
    .await
    {
        Ok(core) => core,
        Err(error) => {
            render_pre_promotion_failure(&progress_renderer, &mut staging_guard)?;
            return Err(error);
        }
    };
    let report = core.rebuild_cache_from_servicenow(&progress_sink).await;
    drop(core);
    let report = match report {
        Ok(report) => report,
        Err(error) => {
            render_pre_promotion_failure(&progress_renderer, &mut staging_guard)?;
            return Err(SnowError::Api(format!("{error:#}")));
        }
    };
    progress_renderer
        .write_line("rebuild-cache: finalizing validated staging cache")
        .map_err(render_progress_error)?;
    if let Err(error) = snow_core::promote_rebuilt_cache(&staging, &paths.database) {
        render_promotion_failure(&progress_renderer, &mut staging_guard)?;
        return Err(SnowError::Api(format!("{error:#}")));
    }
    staging_guard.mark_promoted();
    progress_renderer
        .write_line(&format!(
            "rebuild-cache: complete tables={} pages={} records={}",
            report.tables.len(),
            report.pages,
            report.records
        ))
        .map_err(render_progress_error)?;
    print_servicenow_rebuild_report(&report);
    Ok(())
}

fn render_pre_promotion_failure(
    renderer: &CacheRebuildProgressRenderer,
    staging_guard: &mut StagingCacheGuard,
) -> Result<(), SnowError> {
    let staging_removed = staging_guard.cleanup().is_ok();
    renderer
        .render_pre_promotion_failure(staging_removed)
        .map_err(render_progress_error)
}

fn render_promotion_failure(
    renderer: &CacheRebuildProgressRenderer,
    staging_guard: &mut StagingCacheGuard,
) -> Result<(), SnowError> {
    let staging_removed = staging_guard.cleanup().is_ok();
    renderer
        .render_promotion_failure(staging_removed)
        .map_err(render_progress_error)
}

fn render_progress_error(error: std::io::Error) -> SnowError {
    SnowError::Api(format!("rendering rebuild progress: {error}"))
}

fn remove_cache_artifacts(database_path: &std::path::Path) -> std::io::Result<()> {
    remove_file_if_present(database_path)?;
    for suffix in ["-wal", "-shm", "-journal"] {
        let mut sidecar = database_path.as_os_str().to_os_string();
        sidecar.push(suffix);
        remove_file_if_present(&std::path::PathBuf::from(sidecar))?;
    }
    Ok(())
}

fn remove_file_if_present(path: &std::path::Path) -> std::io::Result<()> {
    match std::fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

struct StagingCacheGuard {
    path: std::path::PathBuf,
    cleaned: bool,
    promoted: bool,
}

impl StagingCacheGuard {
    fn new(path: std::path::PathBuf) -> Self {
        Self {
            path,
            cleaned: false,
            promoted: false,
        }
    }

    fn cleanup(&mut self) -> std::io::Result<()> {
        if self.cleaned || self.promoted {
            return Ok(());
        }
        remove_cache_artifacts(&self.path)?;
        self.cleaned = true;
        Ok(())
    }

    fn mark_promoted(&mut self) {
        self.promoted = true;
    }
}

impl Drop for StagingCacheGuard {
    fn drop(&mut self) {
        let _ = self.cleanup();
    }
}

#[derive(Clone, Default)]
struct CacheRebuildProgressRenderer {
    state: Arc<Mutex<CacheRebuildProgressState>>,
}

#[derive(Default)]
struct CacheRebuildProgressState {
    last_table: Option<CacheRebuildProgressLocation>,
}

#[derive(Clone)]
struct CacheRebuildProgressLocation {
    resource: String,
    table: String,
    page: Option<usize>,
}

impl CacheRebuildProgressRenderer {
    fn new() -> Self {
        Self::default()
    }

    fn sink(&self) -> snow_core::CacheRebuildProgressSink {
        let renderer = self.clone();
        Arc::new(move |event| renderer.render_event(event).map_err(anyhow::Error::from))
    }

    fn render_event(&self, event: snow_core::CacheRebuildProgressEvent) -> std::io::Result<()> {
        let line = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| std::io::Error::other("rebuild progress state lock poisoned"))?;
            match event {
                snow_core::CacheRebuildProgressEvent::Preparing => {
                    "rebuild-cache: preparing ServiceNow staging cache".to_string()
                }
                snow_core::CacheRebuildProgressEvent::Tables { tables, page_size } => {
                    format!("rebuild-cache: tables={tables} page_size={page_size}")
                }
                snow_core::CacheRebuildProgressEvent::ResolvingUserScope => {
                    "rebuild-cache: resolving configured user scope".to_string()
                }
                snow_core::CacheRebuildProgressEvent::UserScopeResolved => {
                    "rebuild-cache: configured user scope resolved".to_string()
                }
                snow_core::CacheRebuildProgressEvent::TableStarted {
                    index,
                    tables,
                    resource,
                    table,
                } => {
                    state.last_table = Some(CacheRebuildProgressLocation {
                        resource: resource.clone(),
                        table: table.clone(),
                        page: None,
                    });
                    format!("[{index}/{tables}] {resource} ({table}): start")
                }
                snow_core::CacheRebuildProgressEvent::RequestingPage {
                    index,
                    tables,
                    resource,
                    table,
                    page,
                } => {
                    state.last_table = Some(CacheRebuildProgressLocation {
                        resource: resource.clone(),
                        table: table.clone(),
                        page: Some(page),
                    });
                    format!("[{index}/{tables}] {resource} ({table}): requesting page={page}")
                }
                snow_core::CacheRebuildProgressEvent::PageProjected {
                    index,
                    tables,
                    resource,
                    table,
                    page,
                    page_records,
                    table_records,
                    total_records,
                } => format!(
                    "[{index}/{tables}] {resource} ({table}): page={page} page_records={page_records} table_records={table_records} total_records={total_records}"
                ),
                snow_core::CacheRebuildProgressEvent::TableCompleted {
                    index,
                    tables,
                    resource,
                    table,
                    pages,
                    records,
                } => format!(
                    "[{index}/{tables}] {resource} ({table}): complete pages={pages} records={records}"
                ),
            }
        };
        self.write_line(&line)
    }

    fn render_pre_promotion_failure(&self, staging_removed: bool) -> std::io::Result<()> {
        let location = self.last_location()?;
        if let Some(location) = location {
            if let Some(page) = location.page {
                self.write_line(&format!(
                    "rebuild-cache: failed during {} ({}) page={page}",
                    location.resource, location.table
                ))?;
            } else {
                self.write_line(&format!(
                    "rebuild-cache: failed during {} ({})",
                    location.resource, location.table
                ))?;
            }
        } else {
            self.write_line("rebuild-cache: failed before ServiceNow table processing")?;
        }
        if staging_removed {
            self.write_line("rebuild-cache: staging cache removed; current cache unchanged")
        } else {
            self.write_line("rebuild-cache: staging cache cleanup failed; current cache unchanged")
        }
    }

    fn render_promotion_failure(&self, staging_removed: bool) -> std::io::Result<()> {
        self.write_line("rebuild-cache: failed during staging cache promotion")?;
        if staging_removed {
            self.write_line("rebuild-cache: staging cache removed; current cache state not claimed")
        } else {
            self.write_line(
                "rebuild-cache: staging cache cleanup failed; current cache state not claimed",
            )
        }
    }

    fn last_location(&self) -> std::io::Result<Option<CacheRebuildProgressLocation>> {
        self.state
            .lock()
            .map_err(|_| std::io::Error::other("rebuild progress state lock poisoned"))
            .map(|state| state.last_table.clone())
    }

    fn write_line(&self, line: &str) -> std::io::Result<()> {
        let stderr = std::io::stderr();
        let mut stderr = stderr.lock();
        writeln!(stderr, "{line}")?;
        stderr.flush()
    }
}

pub(super) fn maybe_run_local_kb_command(command: &Command) -> Result<Option<()>, SnowError> {
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

pub(crate) fn cmd_knowledge_status(db_path: &std::path::Path) -> Result<(), SnowError> {
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

pub(crate) fn cmd_knowledge_tags(
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

pub(crate) fn open_runtime_db(path: &std::path::Path) -> AnyhowResult<Connection> {
    let _ = Store::open(path)?;
    Ok(Connection::open(path)?)
}

pub(crate) fn load_knowledge_status(
    db_path: &std::path::Path,
) -> AnyhowResult<KnowledgeStatusSnapshot> {
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

pub(crate) fn load_knowledge_tags(
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

pub(crate) fn knowledge_tag_layer_matches(
    requested: KnowledgeTagLayer,
    actual: KnowledgeTagLayer,
) -> bool {
    match requested {
        KnowledgeTagLayer::All => true,
        KnowledgeTagLayer::Sn => matches!(actual, KnowledgeTagLayer::Sn),
        KnowledgeTagLayer::Auto => matches!(actual, KnowledgeTagLayer::Auto),
        KnowledgeTagLayer::User => matches!(actual, KnowledgeTagLayer::User),
    }
}

pub(crate) fn format_tag_layers(layers: &[KnowledgeTagLayer]) -> String {
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

pub(crate) fn parse_tag_json(raw: &str) -> AnyhowResult<Vec<String>> {
    Ok(serde_json::from_str(raw)?)
}

pub(crate) fn scalar_count(conn: &Connection, query: &str) -> AnyhowResult<usize> {
    Ok(conn.query_row(query, [], |row| row.get::<_, i64>(0))? as usize)
}

pub(crate) fn decode_runtime_ts(raw: i64) -> Option<DateTime<Utc>> {
    if raw >= 1_000_000_000_000 {
        Utc.timestamp_millis_opt(raw).single()
    } else {
        Utc.timestamp_opt(raw, 0).single()
    }
}

#[derive(Debug, Clone)]
pub(crate) struct KnowledgeStatusSnapshot {
    pub(crate) article_count: usize,
    pub(crate) body_cached_count: usize,
    pub(crate) knowledge_base_count: usize,
    pub(crate) category_count: usize,
    pub(crate) last_full_at: Option<DateTime<Utc>>,
    pub(crate) last_incremental_at: Option<DateTime<Utc>>,
    pub(crate) watermark_updated_at: Option<String>,
    pub(crate) watermark_sys_id: Option<String>,
    pub(crate) lock_held: bool,
    pub(crate) lock_timestamp_ms: Option<i64>,
}

#[derive(Debug, Clone)]
pub(crate) struct KnowledgeTagSummary {
    pub(crate) tag: String,
    pub(crate) count: usize,
    pub(crate) layers: Vec<KnowledgeTagLayer>,
}
