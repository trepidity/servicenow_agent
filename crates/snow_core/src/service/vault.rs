//! `VaultService` — vault-backed cache repair, rebuild, verification, and
//! orphan pruning, extracted from the `SnowCore` god-object.
//!
//! Domain service extracted in Task 11 of the library boundary migration. Every
//! method body below is moved verbatim from its former `impl SnowCore` location
//! in `lib.rs`; the only edits are `self.<helper>` → `self.ctx.<helper>` for the
//! persistence/lifecycle primitives whose bodies live on [`CoreContext`].

use anyhow::Result;
use chrono::Utc;
use std::collections::BTreeMap;
use std::path::Path;

use crate::cache::store::{AliasRow, KeywordRow, Store, TagRow};
use crate::context::CoreContext;
use crate::convert::enrichment_origin_label;
use crate::enrich::derive_for_record;
use crate::helpers::project_runtime_document;
use crate::vault::{scan_documents, scan_documents_detailed};
use crate::{
    OrphanPruneReport, OrphanRecordRow, RebuildReport, RepairReport, UnindexedVaultDocument,
    VaultVerificationReport,
};
use crate::{
    ResourceType, document_assigned_to, document_content, document_tag_tokens, document_work_notes,
    record_row_from_runtime_record, serialize_vault_document,
};

/// Builds a fresh current-format projection beside `database_path` and replaces
/// the local cache only after the vault reconstruction completes.
pub fn rebuild_cache_from_vault(vault_path: &Path, database_path: &Path) -> Result<RebuildReport> {
    let entries = scan_documents(vault_path)?;
    let parent = database_path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent)?;
    let file_name = database_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("snow.db");
    let temporary = parent.join(format!(".{file_name}.rebuild-{}.tmp", uuid::Uuid::new_v4()));

    let result = (|| -> Result<RebuildReport> {
        let store = Store::open(&temporary)?;
        let scanned_documents = entries.len();
        let mut rebuilt_records = 0;
        for entry in entries {
            rebuild_document_into_store(&store, entry.document, entry.relative_path)?;
            rebuilt_records += 1;
        }
        drop(store);
        if Store::inspect_format(&temporary)? != crate::cache::store::CacheFormat::Current {
            anyhow::bail!("rebuilt cache did not validate as the current format");
        }
        Ok(RebuildReport {
            scanned_documents,
            rebuilt_records,
        })
    })();

    match result {
        Ok(report) => {
            std::fs::rename(&temporary, database_path)?;
            Ok(report)
        }
        Err(error) => {
            let _ = std::fs::remove_file(&temporary);
            Err(error)
        }
    }
}

fn rebuild_document_into_store(
    store: &Store,
    document: crate::vault::VaultDocument,
    relative_path: std::path::PathBuf,
) -> Result<()> {
    let record = document.record();
    let row = record_row_from_runtime_record(
        record,
        Some(relative_path),
        serialize_vault_document(&document).to_string(),
    );
    store.upsert_record_with_tags(
        &row,
        &document_work_notes(record),
        &document_content(&document),
        &document_tag_tokens(&document),
    )?;
    let projection = project_runtime_document(&document);
    store.replace_relationships(record.sys_id.as_str(), &projection.relationships)?;
    store.replace_references(&projection.references.into_values().collect::<Vec<_>>())?;
    if let Some(article) = projection.knowledge_article {
        store.upsert_knowledge_article(&article)?;
    }
    if record.resource_type == ResourceType::BusinessApplication {
        store.upsert_business_application_projection(record, None)?;
    }
    persist_enrichment(store, record)?;
    Ok(())
}

fn persist_enrichment(store: &Store, record: &crate::SnowRecord) -> Result<()> {
    let bundle = derive_for_record(record);
    let record_sys_id = record.sys_id.as_str();
    let tags = bundle
        .tags
        .into_iter()
        .map(|candidate| TagRow {
            record_sys_id: record_sys_id.to_string(),
            tag: candidate.value,
            source: enrichment_origin_label(candidate.origin).to_string(),
            weight: candidate.weight,
        })
        .collect::<Vec<_>>();
    let keywords = bundle
        .keywords
        .into_iter()
        .map(|candidate| KeywordRow {
            record_sys_id: record_sys_id.to_string(),
            keyword: candidate.value,
            source: enrichment_origin_label(candidate.origin).to_string(),
            weight: candidate.weight,
        })
        .collect::<Vec<_>>();
    let aliases = bundle
        .aliases
        .into_iter()
        .map(|candidate| AliasRow {
            record_sys_id: record_sys_id.to_string(),
            alias: candidate.value,
            kind: enrichment_origin_label(candidate.origin).to_string(),
            source: enrichment_origin_label(candidate.origin).to_string(),
        })
        .collect::<Vec<_>>();
    store.replace_tags(record_sys_id, &tags)?;
    store.replace_keywords(record_sys_id, &keywords)?;
    store.replace_aliases(record_sys_id, &aliases)?;
    Ok(())
}

#[derive(Clone)]
pub(crate) struct VaultService {
    ctx: CoreContext,
}

impl VaultService {
    pub(crate) fn new(ctx: CoreContext) -> Self {
        Self { ctx }
    }

    pub async fn repair_missing_vault_files(&self) -> Result<usize> {
        Ok(self.repair_vault().await?.repaired_records)
    }

    pub async fn repair_vault(&self) -> Result<RepairReport> {
        let rows = self.ctx.query.store().list_active_records(None)?;
        let mut repaired = 0usize;
        let mut skipped = 0usize;
        let scanned = rows.len();

        for row in rows.into_iter().filter(|row| row.file_path.is_none()) {
            let Some(document) = self
                .ctx
                .load_runtime_document(&row.number, &row.resource_type)
                .await?
            else {
                skipped += 1;
                continue;
            };
            let persisted = self.ctx.persist_runtime_document(&document)?;
            let mut repaired_row = row.clone();
            repaired_row.short_desc = Some(document.record().short_description.clone());
            repaired_row.description = Some(document.record().description.clone());
            repaired_row.assigned_to = document_assigned_to(document.record());
            repaired_row.parent_id = document
                .record()
                .parent
                .as_ref()
                .map(|parent| parent.sys_id.clone());
            repaired_row.file_path = Some(persisted.relative_path.to_string_lossy().into_owned());
            repaired_row.raw_json = serialize_vault_document(&document).to_string();

            self.ctx.query.store().upsert_record_with_tags(
                &repaired_row,
                &document_work_notes(document.record()),
                &document_content(&document),
                &document_tag_tokens(&document),
            )?;
            self.ctx.project_runtime_document(&document)?;
            self.ctx.persist_enrichment(document.record())?;
            repaired += 1;
        }

        Ok(RepairReport {
            scanned_records: scanned,
            repaired_records: repaired,
            skipped_records: skipped,
        })
    }

    pub fn rebuild_cache_from_vault(&self) -> Result<usize> {
        Ok(self.rebuild_cache()?.rebuilt_records)
    }

    pub fn rebuild_cache(&self) -> Result<RebuildReport> {
        let entries = scan_documents(&self.ctx.vault_path)?;
        let mut rebuilt = 0usize;
        let scanned = entries.len();

        for entry in entries {
            let document = entry.document;
            let row = record_row_from_runtime_record(
                document.record(),
                Some(entry.relative_path.clone()),
                serialize_vault_document(&document).to_string(),
            );
            self.ctx.query.store().upsert_record_with_tags(
                &row,
                &document_work_notes(document.record()),
                &document_content(&document),
                &document_tag_tokens(&document),
            )?;
            self.ctx.project_runtime_document(&document)?;
            self.ctx.persist_enrichment(document.record())?;
            rebuilt += 1;
        }

        Ok(RebuildReport {
            scanned_documents: scanned,
            rebuilt_records: rebuilt,
        })
    }

    pub fn verify_vault(&self) -> Result<VaultVerificationReport> {
        let scan_report = scan_documents_detailed(&self.ctx.vault_path)?;
        let entries = scan_report.entries;
        let rows = self.ctx.query.store().list_active_records(None)?;

        let mut indexed_sys_ids = BTreeMap::new();
        let mut missing_markdown_rows = Vec::new();
        let mut orphan_record_rows = Vec::new();
        for row in &rows {
            indexed_sys_ids.insert(row.sys_id.clone(), row.clone());
            match row.file_path.as_deref() {
                Some(relative_path) => {
                    let absolute_path = self.ctx.vault_path.join(relative_path);
                    if !absolute_path.exists() {
                        let orphan = OrphanRecordRow {
                            sys_id: row.sys_id.clone(),
                            number: row.number.clone(),
                            file_path: row.file_path.clone(),
                        };
                        missing_markdown_rows.push(orphan.clone());
                        orphan_record_rows.push(orphan);
                    }
                }
                None => orphan_record_rows.push(OrphanRecordRow {
                    sys_id: row.sys_id.clone(),
                    number: row.number.clone(),
                    file_path: None,
                }),
            }
        }

        let mut unindexed_documents = Vec::new();
        for entry in &entries {
            if !indexed_sys_ids.contains_key(&entry.record().sys_id) {
                unindexed_documents.push(UnindexedVaultDocument {
                    sys_id: entry.record().sys_id.clone(),
                    number: entry.record().number.clone(),
                    relative_path: entry.relative_path.clone(),
                });
            }
        }

        let projected_references = self.ctx.query.store().list_references()?.len();
        let projected_relationships = self.ctx.query.store().list_relationships()?.len();
        let mut projected_enrichment_rows = 0usize;
        for row in &rows {
            projected_enrichment_rows += self.ctx.query.store().list_tags(&row.sys_id)?.len();
            projected_enrichment_rows += self.ctx.query.store().list_keywords(&row.sys_id)?.len();
            projected_enrichment_rows += self.ctx.query.store().list_aliases(&row.sys_id)?.len();
        }

        Ok(VaultVerificationReport {
            scanned_documents: entries.len(),
            active_records: rows.len(),
            projected_references,
            projected_relationships,
            projected_enrichment_rows,
            degraded_reads: self.ctx.query.degraded_reads(),
            missing_markdown_rows,
            orphan_record_rows,
            unprojectable_documents: scan_report.failures,
            unindexed_documents,
        })
    }

    pub async fn prune_orphans(&self, dry_run: bool) -> Result<OrphanPruneReport> {
        let verification = self.verify_vault()?;
        let orphan_rows = verification.orphan_record_rows;
        let scanned = orphan_rows.len();
        if dry_run {
            return Ok(OrphanPruneReport {
                dry_run: true,
                orphan_rows_scanned: scanned,
                orphan_rows_pruned: 0,
                orphan_rows,
            });
        }

        let mut pruned = 0usize;
        for orphan in &orphan_rows {
            self.ctx.prune_record(&orphan.sys_id, Utc::now()).await?;
            pruned += 1;
        }

        Ok(OrphanPruneReport {
            dry_run: false,
            orphan_rows_scanned: scanned,
            orphan_rows_pruned: pruned,
            orphan_rows,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::*;
    use crate::vault::VaultDocument;
    use crate::{
        ResourceType, collect_journal_entries, record_row_from_servicenow, render_journal_entries,
    };
    use tempfile::TempDir;

    #[tokio::test]
    async fn repair_missing_vault_files_backfills_legacy_rows() {
        let tempdir = TempDir::new().expect("tempdir");
        let core = build_test_core(tempdir.path().join("vault")).await;
        let record = sample_change_task_record();
        let legacy_row = record_row_from_servicenow(&record).expect("legacy row");
        core.ctx
            .query
            .store()
            .upsert_record(
                &legacy_row,
                &render_journal_entries(&collect_journal_entries(&record, "work_notes")),
                legacy_row.description.as_deref().unwrap_or_default(),
            )
            .expect("upsert legacy row");

        let repaired = core
            .repair_missing_vault_files()
            .await
            .expect("repair legacy rows");
        assert_eq!(repaired, 1);

        let row = core
            .ctx
            .query
            .store()
            .get_record_by_number("CTASK001")
            .expect("row lookup")
            .expect("row");
        assert_eq!(row.file_path.as_deref(), Some("changes/CHG001/CTASK001.md"));
        assert!(
            core.vault_path()
                .join(row.file_path.as_deref().unwrap())
                .exists()
        );
    }

    #[tokio::test]
    async fn rebuild_cache_from_vault_rehydrates_sqlite_projection() {
        let tempdir = TempDir::new().expect("tempdir");
        let core = build_test_core(tempdir.path().join("vault")).await;
        let mut record = sample_projected_record();
        record.synced_at = Utc::now();
        core.ctx
            .vault
            .persist_record(&record)
            .expect("persist vault record");

        let rebuilt = core.rebuild_cache_from_vault().expect("rebuild cache");
        assert_eq!(rebuilt, 1);

        let row = core
            .ctx
            .query
            .store()
            .get_record_by_number("INC002")
            .expect("row lookup")
            .expect("row");
        assert_eq!(row.file_path.as_deref(), Some("incidents/INC002.md"));

        let loaded = core
            .get_record("INC002")
            .await
            .expect("query rebuilt record")
            .expect("record");
        assert_eq!(loaded.short_description, "Legacy incident");
        assert!(
            core.ctx
                .query
                .store()
                .list_keywords(&record.sys_id)
                .expect("keywords")
                .iter()
                .any(|row| row.keyword == "legacy")
        );
        let references = core
            .ctx
            .query
            .store()
            .list_references()
            .expect("references");
        assert!(references.iter().any(|row| row.sys_id == "parent-sys"));
        assert!(references.iter().any(|row| row.sys_id == "child-sys"));
        assert!(references.iter().any(|row| row.sys_id == "user-sys"));

        let relationships = core
            .ctx
            .query
            .store()
            .list_relationships()
            .expect("relationships");
        assert!(relationships.iter().any(|row| {
            row.source_id == record.sys_id
                && row.target_id == "parent-sys"
                && row.rel_type == "parent"
                && row.field_name == "parent"
        }));
        assert!(relationships.iter().any(|row| {
            row.source_id == record.sys_id
                && row.target_id == "child-sys"
                && row.rel_type == "child"
                && row.field_name == "children"
        }));
        assert!(relationships.iter().any(|row| {
            row.source_id == record.sys_id
                && row.target_id == "user-sys"
                && row.rel_type == "reference"
                && row.field_name == "assigned_to"
        }));
        assert!(matches!(
            core.ctx
                .load_runtime_document("INC002", &ResourceType::Incident)
                .await
                .expect("load runtime document"),
            Some(VaultDocument::Record(_))
        ));
    }

    #[tokio::test]
    async fn rebuild_cache_from_vault_rehydrates_knowledge_projection() {
        let tempdir = TempDir::new().expect("tempdir");
        let core = build_test_core(tempdir.path().join("vault")).await;
        let article = sample_projected_knowledge_article();
        core.ctx
            .vault
            .persist_knowledge_article(&article)
            .expect("persist knowledge article");

        let rebuilt = core.rebuild_cache_from_vault().expect("rebuild cache");
        assert_eq!(rebuilt, 1);

        let loaded = core
            .ctx
            .query
            .store()
            .get_knowledge_article(&article.record.sys_id)
            .expect("knowledge row lookup")
            .expect("knowledge row");
        assert_eq!(loaded.number, "KB002");
        assert_eq!(loaded.knowledge_base_name, "IT");
        assert_eq!(loaded.category_name, "Access");
        assert_eq!(loaded.author_name.as_deref(), Some("Casey User"));
        assert_eq!(loaded.published_at.as_deref(), Some("2026-04-10 09:00:00"));
        assert_eq!(loaded.valid_to.as_deref(), Some("2027-01-01"));
    }

    #[tokio::test]
    async fn verify_vault_reports_projection_and_orphans() {
        let tempdir = TempDir::new().expect("tempdir");
        let core = build_test_core(tempdir.path().join("vault")).await;
        let record = sample_projected_record();
        core.ctx
            .vault
            .persist_record(&record)
            .expect("persist vault record");
        core.rebuild_cache().expect("rebuild cache");

        let verification = core.verify_vault().expect("verify vault");
        assert_eq!(verification.scanned_documents, 1);
        assert_eq!(verification.active_records, 1);
        assert!(verification.projected_references >= 3);
        assert!(verification.projected_relationships >= 3);
        assert!(verification.projected_enrichment_rows > 0);
        assert!(verification.orphan_record_rows.is_empty());
        assert!(verification.unindexed_documents.is_empty());
        assert!(verification.unprojectable_documents.is_empty());
    }

    #[tokio::test]
    async fn prune_orphans_dry_run_and_execution_report_rows() {
        let tempdir = TempDir::new().expect("tempdir");
        let core = build_test_core(tempdir.path().join("vault")).await;
        let record = sample_change_task_record();
        let legacy_row = record_row_from_servicenow(&record).expect("legacy row");
        core.ctx
            .query
            .store()
            .upsert_record(
                &legacy_row,
                "",
                legacy_row.description.as_deref().unwrap_or_default(),
            )
            .expect("upsert legacy row");

        let dry_run = core.prune_orphans(true).await.expect("dry run");
        assert!(dry_run.dry_run);
        assert_eq!(dry_run.orphan_rows_scanned, 1);
        assert_eq!(dry_run.orphan_rows_pruned, 0);

        let executed = core.prune_orphans(false).await.expect("execute prune");
        assert!(!executed.dry_run);
        assert_eq!(executed.orphan_rows_pruned, 1);
        assert!(
            core.ctx
                .query
                .store()
                .get_record_by_number("CTASK001")
                .expect("lookup row")
                .is_none()
        );
    }
}
