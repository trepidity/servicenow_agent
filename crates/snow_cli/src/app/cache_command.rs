use super::*;

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

pub(super) fn cmd_rebuild_cache_offline() -> Result<(), SnowError> {
    let paths = runtime_paths();
    let report = snow_core::rebuild_cache_from_vault(&paths.vault, &paths.database)
        .map_err(|error| SnowError::Api(error.to_string()))?;
    print_rebuild_report(&report);
    Ok(())
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
