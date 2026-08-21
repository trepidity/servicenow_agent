use super::*;

/// Serialize a value to pretty JSON and print it, surfacing serialization errors.
pub(super) fn print_json<T: serde::Serialize>(value: &T) -> Result<(), SnowError> {
    let text =
        serde_json::to_string_pretty(value).map_err(|err| SnowError::Api(err.to_string()))?;
    println!("{text}");
    Ok(())
}

/// Print a `serde_json::Value` as pretty JSON to stdout.
pub(super) fn print_full_dump_or_inline(value: &serde_json::Value) {
    println!(
        "{}",
        serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
    );
}

pub(super) fn confirm_action(prompt: &str) -> Result<bool, SnowError> {
    print!("{prompt} [y/N]: ");
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    Ok(matches!(
        input.trim().to_ascii_lowercase().as_str(),
        "y" | "yes"
    ))
}

pub(super) fn print_repair_report(report: &RepairReport) {
    println!("repair-vault");
    println!("scanned: {}", report.scanned_records);
    println!("repaired: {}", report.repaired_records);
    println!("skipped: {}", report.skipped_records);
}

pub(super) fn print_rebuild_report(report: &RebuildReport) {
    println!("import-cache-from-vault");
    println!("scanned documents: {}", report.scanned_documents);
    println!("rebuilt records: {}", report.rebuilt_records);
}

pub(super) fn print_servicenow_rebuild_report(report: &ServiceNowCacheRebuildReport) {
    println!("rebuild-cache");
    println!("source: {}", report.source);
    println!("scope: {}", report.scope);
    for table in &report.tables {
        println!(
            "table: resource={} servicenow_table={} pages={} records={}",
            table.resource, table.table, table.pages, table.records
        );
    }
    println!("tables: {}", report.tables.len());
    println!("pages: {}", report.pages);
    println!("records: {}", report.records);
    println!("complete: {}", report.complete);
}

pub(super) fn print_prune_report(report: &OrphanPruneReport) {
    println!("prune-orphans");
    println!("dry run: {}", report.dry_run);
    println!("orphan rows scanned: {}", report.orphan_rows_scanned);
    println!("orphan rows pruned: {}", report.orphan_rows_pruned);
    for orphan in &report.orphan_rows {
        println!("{} {} {:?}", orphan.number, orphan.sys_id, orphan.file_path);
    }
}

pub(super) fn print_verification_report(report: &VaultVerificationReport) {
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

pub(super) fn print_knowledge_article(article: &KnowledgeArticle) {
    print!("{}", format_knowledge_article(article, true));
}

pub(super) fn print_knowledge_article_summary(article: &KnowledgeArticle) {
    print!("{}", format_knowledge_article(article, false));
}

pub(super) fn print_knowledge_search_hit(hit: &KnowledgeSearchHit) {
    print!("{}", format_knowledge_search_hit(hit));
}

pub(super) fn print_task_sla_status(status: &TaskSlaStatus) {
    print!("{}", format_task_sla_status(status));
}

pub(super) fn format_task_sla_status(status: &TaskSlaStatus) -> String {
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

pub(super) fn write_task_sla_heading_and_summary(out: &mut String, status: &TaskSlaStatus) {
    let _ = writeln!(
        out,
        "Task SLA: {} ({})",
        status.record_number,
        display_or_unknown(&status.record_table)
    );
    write_task_sla_summary(out, &status.summary);
}

pub(super) fn write_task_sla_summary(out: &mut String, summary: &TaskSlaSummaryView) {
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

pub(super) fn write_task_sla_rows(out: &mut String, rows: &[TaskSlaView]) {
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

pub(super) fn format_task_sla_next_breach(row: Option<&TaskSlaView>) -> String {
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

pub(super) fn display_bool(value: Option<bool>) -> &'static str {
    match value {
        Some(true) => "yes",
        Some(false) => "no",
        None => "-",
    }
}

pub(super) fn display_optional(value: Option<&str>) -> &str {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or("-")
}

pub(super) fn display_or_unknown(value: &str) -> &str {
    let value = value.trim();
    if value.is_empty() { "unknown" } else { value }
}

pub(super) fn format_knowledge_article(article: &KnowledgeArticle, include_body: bool) -> String {
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

pub(super) fn format_knowledge_search_hit(hit: &KnowledgeSearchHit) -> String {
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

pub(super) fn print_knowledge_base_summary(base: &KnowledgeBaseSummary) {
    println!(
        "{} [{}] {} articles",
        base.display_name, base.sys_id, base.article_count
    );
}

pub(super) fn print_knowledge_category_summary(category: &KnowledgeCategorySummary) {
    println!(
        "{} [{}] {} articles",
        category.display_name, category.sys_id, category.article_count
    );
}

pub(super) fn print_knowledge_sync_outcome(outcome: &snow_core::KnowledgeSyncOutcome) {
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

pub(super) fn print_knowledge_semantic_status(status: &KnowledgeSemanticStatus) {
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

pub(super) fn print_knowledge_semantic_rebuild_summary(summary: &SemanticIndexSummary) {
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

pub(super) fn format_knowledge_search_mode(mode: KnowledgeSearchMode) -> &'static str {
    match mode {
        KnowledgeSearchMode::Lexical => "lexical",
        KnowledgeSearchMode::Semantic => "semantic",
        KnowledgeSearchMode::Hybrid => "hybrid",
    }
}

pub(super) fn format_embedding_coverage(coverage: KnowledgeEmbeddingCoverage) -> &'static str {
    match coverage {
        KnowledgeEmbeddingCoverage::Metadata => "metadata",
        KnowledgeEmbeddingCoverage::FullText => "full_text",
    }
}
