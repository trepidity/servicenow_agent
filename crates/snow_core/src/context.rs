//! Shared runtime context for the snow_core services.
//!
//! [`CoreContext`] carries all shared runtime dependencies (client, cache,
//! query engine, vault, config) together with the low-level I/O primitives
//! that multiple domain services call. Centralizing these here lets each
//! per-domain service invoke `self.ctx.persist_record(...)` and friends without
//! taking a dependency on a sibling service.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use chrono::{DateTime, Utc};
use servicenow_rs::prelude::{
    DisplayValue, Error as SnowApiError, FieldValue as SnowFieldValue, Order, Record,
    ServiceNowClient,
};

use crate::cache::store::{AliasRow, KeywordRow, RecordRow, Store, TagRow};
use crate::cache::{CacheManager, policy::CacheTtlPolicy};
use crate::config::SnowConfig;
use crate::convert::{
    enrichment_origin_label, record_row_from_runtime_record, record_row_from_snow_record,
    serialize_vault_document,
};
use crate::enrich::derive_for_record;
use crate::helpers::{
    collect_journal_entries, document_content, document_tag_tokens, document_work_notes,
    first_non_empty_str, non_empty_owned, project_runtime_document, render_journal_entries,
    restore_staged_markdown, stage_markdown_for_prune,
};
use crate::query::QueryEngine;
use crate::reference::{collect_record_references, empty_reference, parent_record_ref};
use crate::resource;
use crate::resource::approval::ApprovalResource;
use crate::vault;
use crate::vault::VaultDocument;
use crate::vault::manager::VaultManager;
use crate::{
    ApprovalRecord, BusinessApplicationFieldAliases, FieldChoice, KnowledgeArticle, ResourceType,
    SnowRecord, UserRef, normalize_record_lookup_sys_id, normalize_record_lookup_table,
    table_for_builtin_record_number,
};

/// Shared runtime dependencies for all domain services, plus low-level I/O
/// primitives that are called by multiple services.
///
/// Constructed once in `SnowCoreBuilder::build()` and cloned into each
/// service. All fields are cheap to clone (Arc or known-Clone types).
#[derive(Clone)]
pub(crate) struct CoreContext {
    pub client: Arc<ServiceNowClient>,
    // Read by the per-domain services extracted in Tasks 8-11; the primitives
    // moved here in this task reach the store via `self.query.store()`.
    #[allow(dead_code)]
    pub store: Arc<Store>,
    pub query: Arc<QueryEngine>,
    pub cache: CacheManager,
    pub cache_policy: CacheTtlPolicy,
    pub vault: VaultManager,
    pub vault_path: PathBuf,
    pub config: Arc<SnowConfig>,
}

impl CoreContext {
    /// Fetch journal fields (`work_notes`, `comments`) with display values
    /// via [`ServiceNowClient::journal_inline`] and merge them into the record.
    ///
    /// Journal blobs can still come back empty under the detail projection, so
    /// this method performs a second query with `DisplayValue::Display` to
    /// retrieve the formatted journal blobs, then overwrites the corresponding
    /// fields on the mutable `Record`.
    pub(crate) async fn enrich_record_journals(&self, record: &mut Record) -> Result<()> {
        let table = record.table.clone();
        let sys_id = record.sys_id.clone();
        let journal_record = self
            .client
            .journal_inline(&table, &sys_id, &["work_notes", "comments"])
            .first()
            .await?;
        if let Some(journal_record) = journal_record {
            for field in &["work_notes", "comments"] {
                if let Some(value) = journal_record.get(field) {
                    let blob = value
                        .display_value
                        .as_deref()
                        .or_else(|| value.value.as_ref().and_then(|v| v.as_str()))
                        .unwrap_or("");
                    if !blob.trim().is_empty() {
                        record.set(
                            *field,
                            SnowFieldValue {
                                value: None,
                                display_value: Some(blob.to_string()),
                                link: None,
                            },
                        );
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn persist_document(&self, record: &Record) -> Result<PersistedRuntimeDocument> {
        match record.table.as_str() {
            "kb_knowledge" => {
                let article = self.build_knowledge_article(record)?;
                let persisted = self.vault.persist_knowledge_article(&article)?;
                Ok(PersistedRuntimeDocument::Knowledge {
                    article,
                    relative_path: persisted.relative_path,
                })
            }
            "sysapproval_approver" => {
                let approver = ApprovalResource::approver_reference(record)
                    .unwrap_or_else(|| empty_reference("sys_user"));
                let approval = ApprovalResource::from_servicenow(record, approver);
                let persisted = self.vault.persist_approval(&approval)?;
                Ok(PersistedRuntimeDocument::Approval {
                    approval,
                    relative_path: persisted.relative_path,
                })
            }
            _ => {
                let mut snow_record = SnowRecord::from_servicenow(record);
                snow_record.parent = parent_record_ref(record);
                snow_record.references =
                    if resource::business_application::is_business_application_alias(&record.table)
                    {
                        resource::business_application::collect_business_application_references(
                            record,
                            &BusinessApplicationFieldAliases::baseline_degraded(),
                        )
                    } else if resource::server::is_server_table(&record.table) {
                        resource::server::collect_server_references(record)
                    } else {
                        collect_record_references(record)
                    };
                let persisted = self.vault.persist_record(&snow_record)?;
                Ok(PersistedRuntimeDocument::Record {
                    record: snow_record,
                    relative_path: persisted.relative_path,
                })
            }
        }
    }

    pub(crate) fn persist_record(&self, record: &Record) -> Result<()> {
        let number = record.get_str("number").unwrap_or_default();
        self.cache.invalidate(number);
        let persisted = self.persist_document(record)?;
        let row = record_row_from_snow_record(
            persisted.record(),
            record,
            Some(persisted.relative_path().to_path_buf()),
        )?;
        let persisted_document = persisted.to_vault_document();
        self.query
            .store()
            .upsert_record_with_tags(
                &row,
                &render_journal_entries(&collect_journal_entries(record, "work_notes")),
                &document_content(&persisted_document),
                &document_tag_tokens(&persisted_document),
            )
            .map_err(anyhow::Error::from)?;
        if let Err(err) = self.record_kb_local_file_state(&persisted) {
            eprintln!(
                "snow_core: KB local file state refresh failed for {}: {err}",
                row.number
            );
        }
        if let Err(err) = self.project_runtime_document(&persisted_document) {
            eprintln!(
                "snow_core: projection refresh failed for {}: {err}",
                row.number
            );
        }
        if let Err(err) = self.persist_enrichment(persisted.record()) {
            eprintln!(
                "snow_core: enrichment refresh failed for {}: {err}",
                row.number
            );
        }
        self.cache.put(persisted.record().clone());
        Ok(())
    }

    /// Batch-persist multiple records, wrapping all SQLite writes in a single transaction.
    pub(crate) fn persist_records(&self, records: &[Record]) -> Result<()> {
        if records.is_empty() {
            return Ok(());
        }

        let mut entries = Vec::with_capacity(records.len());
        let mut persisted_docs = Vec::with_capacity(records.len());

        for record in records {
            let number = record.get_str("number").unwrap_or_default();
            self.cache.invalidate(number);
            let persisted = self.persist_document(record)?;
            let work_notes = render_journal_entries(&collect_journal_entries(record, "work_notes"));
            let row = record_row_from_snow_record(
                persisted.record(),
                record,
                Some(persisted.relative_path().to_path_buf()),
            )?;
            let persisted_document = persisted.to_vault_document();
            let content = document_content(&persisted_document);
            let tag_tokens = document_tag_tokens(&persisted_document);
            entries.push((row, work_notes, content, tag_tokens));
            persisted_docs.push(persisted);
        }

        let batch: Vec<(&RecordRow, &str, &str, &str)> = entries
            .iter()
            .map(|(row, wn, content, tag_tokens)| {
                (row, wn.as_str(), content.as_str(), tag_tokens.as_str())
            })
            .collect();
        self.query
            .store()
            .upsert_records(&batch)
            .map_err(anyhow::Error::from)?;
        for persisted in &persisted_docs {
            if let Err(err) = self.record_kb_local_file_state(persisted) {
                eprintln!("snow_core: KB local file state refresh failed: {err}");
            }
        }

        for persisted in &persisted_docs {
            self.cache.put(persisted.record().clone());
            if let Err(err) = self.project_runtime_document(&persisted.to_vault_document()) {
                eprintln!("snow_core: projection refresh failed: {err}");
            }
            if let Err(err) = self.persist_enrichment(persisted.record()) {
                eprintln!("snow_core: enrichment refresh failed: {err}");
            }
        }
        Ok(())
    }

    pub(crate) fn record_kb_local_file_state(
        &self,
        persisted: &PersistedRuntimeDocument,
    ) -> Result<()> {
        let PersistedRuntimeDocument::Knowledge {
            article,
            relative_path,
        } = persisted
        else {
            return Ok(());
        };
        let absolute_path = self.vault_path.join(relative_path);
        let modified_at_ms = fs::metadata(&absolute_path)?
            .modified()?
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|err| {
                anyhow::anyhow!("invalid file mtime for {}: {err}", absolute_path.display())
            })?
            .as_millis() as i64;
        self.query.store().upsert_kb_local_file_states(&[(
            article.record.sys_id.clone(),
            relative_path.to_string_lossy().into_owned(),
            modified_at_ms,
        )])?;
        Ok(())
    }

    pub(crate) fn project_runtime_document(&self, document: &VaultDocument) -> Result<()> {
        let projection = project_runtime_document(document);
        let store = self.query.store();
        store
            .replace_relationships(document.record().sys_id.as_str(), &projection.relationships)?;
        store.replace_references(&projection.references.into_values().collect::<Vec<_>>())?;
        if let Some(article) = &projection.knowledge_article {
            store.upsert_knowledge_article(article)?;
        }
        if document.record().resource_type == ResourceType::BusinessApplication {
            store.upsert_business_application_projection(document.record(), None)?;
        }
        Ok(())
    }

    pub(crate) fn persist_enrichment(&self, snow_record: &SnowRecord) -> Result<()> {
        let bundle = derive_for_record(snow_record);
        let store = self.query.store();
        let record_sys_id = snow_record.sys_id.as_str();

        let tags: Vec<TagRow> = bundle
            .tags
            .into_iter()
            .map(|candidate| TagRow {
                record_sys_id: record_sys_id.to_string(),
                tag: candidate.value,
                source: enrichment_origin_label(candidate.origin).to_string(),
                weight: candidate.weight,
            })
            .collect();
        let keywords: Vec<KeywordRow> = bundle
            .keywords
            .into_iter()
            .map(|candidate| KeywordRow {
                record_sys_id: record_sys_id.to_string(),
                keyword: candidate.value,
                source: enrichment_origin_label(candidate.origin).to_string(),
                weight: candidate.weight,
            })
            .collect();
        let aliases: Vec<AliasRow> = bundle
            .aliases
            .into_iter()
            .map(|candidate| AliasRow {
                record_sys_id: record_sys_id.to_string(),
                alias: candidate.value,
                kind: enrichment_origin_label(candidate.origin).to_string(),
                source: enrichment_origin_label(candidate.origin).to_string(),
            })
            .collect();

        store.replace_tags(record_sys_id, &tags)?;
        store.replace_keywords(record_sys_id, &keywords)?;
        store.replace_aliases(record_sys_id, &aliases)?;
        Ok(())
    }

    pub(crate) async fn get_record_by_table_sys_id_fresh_with_source(
        &self,
        table: &str,
        sys_id: &str,
    ) -> Result<Option<(Record, SnowRecord)>> {
        let mut record = self
            .client
            .table(table)
            .display_value(DisplayValue::Both)
            .get(sys_id)
            .await?;
        if record.sys_id.eq_ignore_ascii_case(sys_id) {
            record.sys_id = sys_id.to_string();
        }
        let number = if resource::business_application::is_business_application_alias(&record.table)
        {
            resource::business_application::business_application_number(&record)
        } else {
            record
                .get_raw("number")
                .or_else(|| record.get_str("number"))
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    anyhow::anyhow!(
                        "fresh {} row {} did not include a record number",
                        table,
                        sys_id
                    )
                })?
                .to_string()
        };

        if let Err(err) = self.enrich_record_journals(&mut record).await {
            eprintln!("snow_core: journal enrichment failed for {number}: {err}");
        }
        self.persist_record(&record)?;
        Ok(self
            .query
            .get_record(&number)
            .await?
            .map(|snow_record| (record, snow_record)))
    }

    pub(crate) async fn lookup_table_and_sys_id(
        &self,
        number: &str,
    ) -> Result<Option<(String, String)>> {
        let table = self.table_for_number(number).ok_or_else(|| {
            anyhow::anyhow!(
                "cannot resolve table for number '{number}' — unknown ServiceNow prefix"
            )
        })?;
        let Some(record) = self
            .client
            .table(&table)
            .equals("number", number)
            .first()
            .await?
        else {
            return Ok(None);
        };
        Ok(Some((record.table.clone(), record.sys_id.clone())))
    }

    pub(crate) async fn field_choices_for_table(
        &self,
        table: &str,
        field: &str,
    ) -> Result<Vec<FieldChoice>> {
        let response = self
            .client
            .table("sys_choice")
            .equals("name", table)
            .equals("element", field)
            .fields(&["value", "label", "sequence", "inactive", "terminal"])
            .display_value(DisplayValue::Display)
            .order_by("sequence", Order::Asc)
            .limit(200)
            .execute()
            .await?;

        let mut seen = HashSet::new();
        let mut choices = Vec::new();
        for record in response.records {
            if record
                .get_str("inactive")
                .is_some_and(|inactive| inactive.eq_ignore_ascii_case("true"))
            {
                continue;
            }
            let value = record.get_str("value").unwrap_or("").to_string();
            if value.is_empty() || !seen.insert(value.clone()) {
                continue;
            }
            choices.push(FieldChoice {
                label: record.get_str("label").unwrap_or(&value).to_string(),
                value,
                terminal: record
                    .get_str("terminal")
                    .is_some_and(|terminal| terminal.eq_ignore_ascii_case("true")),
            });
        }
        Ok(choices)
    }

    pub(crate) async fn table_ancestors(&self, table: &str) -> Result<Vec<String>> {
        let mut ancestors = Vec::new();
        let mut current = table.to_string();

        for _ in 0..8 {
            let record = self
                .client
                .table("sys_db_object")
                .equals("name", &current)
                .fields(&["name", "super_class"])
                .display_value(DisplayValue::Both)
                .limit(1)
                .first()
                .await?;

            let Some(record) = record else {
                break;
            };

            let Some(parent_sys_id) = record
                .get_raw("super_class")
                .or(record.get_str("super_class"))
            else {
                break;
            };
            if parent_sys_id.is_empty() {
                break;
            }

            let parent = self
                .client
                .table("sys_db_object")
                .equals("sys_id", parent_sys_id)
                .fields(&["name"])
                .display_value(DisplayValue::Both)
                .limit(1)
                .first()
                .await?;

            let Some(parent) =
                parent.and_then(|record| record.get_str("name").map(ToString::to_string))
            else {
                break;
            };

            if ancestors.iter().any(|seen| seen == &parent) {
                break;
            }

            current = parent.clone();
            ancestors.push(parent);
        }

        Ok(ancestors)
    }

    pub(crate) async fn resolve_user_sys_id(&self, user: &str) -> Result<String> {
        Ok(self.resolve_user_ref(user).await?.sys_id)
    }

    pub(crate) async fn resolve_user_ref(&self, user: &str) -> Result<UserRef> {
        let user = user.trim();
        let mut candidates = Vec::new();
        if user.contains('@') {
            candidates.push(("email", user));
            candidates.push(("user_name", user));
        } else {
            candidates.push(("user_name", user));
            candidates.push(("email", user));
        }

        for (field, value) in candidates {
            let Some(record) = self
                .client
                .table("sys_user")
                .equals(field, value)
                .fields(&["sys_id", "user_name", "email", "name"])
                .limit(1)
                .first()
                .await?
            else {
                continue;
            };

            return Ok(UserRef {
                sys_id: record.sys_id.clone(),
                user_name: non_empty_owned(record.get_str("user_name")),
                email: non_empty_owned(record.get_str("email")),
                display: first_non_empty_str([
                    record.get_str("name"),
                    record.get_str("user_name"),
                    record.get_str("email"),
                ])
                .unwrap_or(user)
                .to_string(),
            });
        }

        Err(anyhow::anyhow!("user not found: {user}"))
    }

    // Cross-cutting: called by both RecordService and ApprovalService; must NOT live on one service.
    pub(crate) async fn current_user_sys_id(&self) -> Result<String> {
        self.resolve_user_sys_id(&self.config.instance.user).await
    }

    pub(crate) async fn get_record_by_table_sys_id_fresh(
        &self,
        table: &str,
        sys_id: &str,
    ) -> Result<Option<SnowRecord>> {
        let table = normalize_record_lookup_table(table)?;
        let sys_id = normalize_record_lookup_sys_id(sys_id)?;
        match self
            .get_record_by_table_sys_id_fresh_with_source(&table, &sys_id)
            .await
        {
            Ok(record) => Ok(record.map(|(_, snow_record)| snow_record)),
            Err(err)
                if err
                    .downcast_ref::<SnowApiError>()
                    .is_some_and(|err| matches!(err, SnowApiError::Api { status: 404, .. })) =>
            {
                Ok(None)
            }
            Err(err) => Err(err),
        }
    }

    pub(crate) async fn get_record_fresh(&self, number: &str) -> Result<Option<SnowRecord>> {
        Ok(self
            .get_record_fresh_with_source(number)
            .await?
            .map(|(_, record)| record))
    }

    // Private helper — not pub(crate); only called by the methods above.
    async fn get_record_fresh_with_source(
        &self,
        number: &str,
    ) -> Result<Option<(Record, SnowRecord)>> {
        let table = self.table_for_number(number).ok_or_else(|| {
            anyhow::anyhow!(
                "cannot resolve table for number '{number}' — unknown ServiceNow prefix"
            )
        })?;
        let Some(mut record) = self
            .client
            .table(&table)
            .equals("number", number)
            .display_value(DisplayValue::Both)
            .first()
            .await?
        else {
            return Ok(None);
        };
        // Journal enrichment is best-effort: log and continue on failure
        // so the base record is always persisted even if journals are unavailable.
        if let Err(err) = self.enrich_record_journals(&mut record).await {
            eprintln!("snow_core: journal enrichment failed for {number}: {err}");
        }
        self.persist_record(&record)?;
        Ok(self
            .query
            .get_record(number)
            .await?
            .map(|snow_record| (record, snow_record)))
    }

    // `pub(crate)` so the `SnowCore::table_for_number` delegation wrapper (in a
    // different module) can reach it; `infer_table`/`lookup_table_and_sys_id`
    // call it from within this impl.
    pub(crate) fn table_for_number(&self, number: &str) -> Option<String> {
        self.client
            .table_for_number(number)
            .map(str::to_string)
            .or_else(|| table_for_builtin_record_number(number).map(str::to_string))
    }

    pub(crate) fn infer_table(&self, number: &str) -> String {
        self.table_for_number(number)
            .unwrap_or_else(|| "task".to_string())
    }

    // Needed by RecordService AND KnowledgeService (sync_knowledge → reconcile_full_knowledge_sync).
    pub(crate) fn tombstone_record(&self, sys_id: &str, when: DateTime<Utc>) -> Result<()> {
        if let Ok(Some(row)) = self.query.store().get_record_by_sys_id(sys_id) {
            self.cache.invalidate(&row.number);
        }
        self.query
            .store()
            .tombstone_record(sys_id, when)
            .map_err(anyhow::Error::from)
    }

    pub(crate) async fn prune_record(&self, sys_id: &str, when: DateTime<Utc>) -> Result<()> {
        let Some(row) = self.query.store().get_record_by_sys_id(sys_id)? else {
            return Ok(());
        };
        self.cache.invalidate(&row.number);
        let Some(record) = self.query.get_record(&row.number).await? else {
            return Ok(());
        };

        let markdown_path = self.vault.layout().record_path(&record);
        let staged_path = stage_markdown_for_prune(&markdown_path)?;
        let prune_result = self
            .query
            .store()
            .prune_record(sys_id, when)
            .map_err(anyhow::Error::from);

        match prune_result {
            Ok(()) => {
                if let Some(staged_path) = staged_path {
                    fs::remove_file(&staged_path)?;
                }
                Ok(())
            }
            Err(err) => {
                restore_staged_markdown(staged_path.as_ref())?;
                Err(err)
            }
        }
    }

    // Needed by BusinessApplicationService (sync) AND VaultService (rebuild).
    pub(crate) fn persist_snow_records(&self, records: &[SnowRecord]) -> Result<usize> {
        if records.is_empty() {
            return Ok(0);
        }

        let mut entries = Vec::with_capacity(records.len());
        let mut persisted_docs = Vec::with_capacity(records.len());

        for record in records {
            self.cache.invalidate(&record.number);
            let document = VaultDocument::Record(record.clone());
            let persisted = self.persist_runtime_document(&document)?;
            let row = record_row_from_runtime_record(
                record,
                Some(persisted.relative_path.clone()),
                serialize_vault_document(&document).to_string(),
            );
            let content = document_content(&document);
            let tag_tokens = document_tag_tokens(&document);
            let work_notes = document_work_notes(record);
            entries.push((row, work_notes, content, tag_tokens));
            persisted_docs.push(persisted);
        }

        let batch: Vec<(&RecordRow, &str, &str, &str)> = entries
            .iter()
            .map(|(row, work_notes, content, tag_tokens)| {
                (
                    row,
                    work_notes.as_str(),
                    content.as_str(),
                    tag_tokens.as_str(),
                )
            })
            .collect();
        self.query
            .store()
            .upsert_records(&batch)
            .map_err(anyhow::Error::from)?;

        for persisted in &persisted_docs {
            let document = &persisted.document;
            self.cache.put(document.record().clone());
            if let Err(err) = self.project_runtime_document(document) {
                eprintln!("snow_core: projection refresh failed: {err}");
            }
            if let Err(err) = self.persist_enrichment(document.record()) {
                eprintln!("snow_core: enrichment refresh failed: {err}");
            }
        }
        Ok(records.len())
    }

    // pub(crate): VaultService (a different module) calls these directly.
    // Distinct from persist_document, which takes &Record and builds KB/approval artifacts.
    pub(crate) fn persist_runtime_document(
        &self,
        document: &VaultDocument,
    ) -> Result<vault::rebuild::VaultDocumentEntry> {
        let persisted = match document {
            VaultDocument::Record(record) => self.vault.persist_record(record)?,
            VaultDocument::Knowledge(article) => self.vault.persist_knowledge_article(article)?,
            VaultDocument::Approval(approval) => self.vault.persist_approval(approval)?,
        };

        Ok(vault::rebuild::VaultDocumentEntry {
            absolute_path: persisted.path,
            relative_path: persisted.relative_path,
            document: document.clone(),
        })
    }

    pub(crate) async fn load_runtime_document(
        &self,
        number: &str,
        resource_type: &ResourceType,
    ) -> Result<Option<VaultDocument>> {
        match resource_type {
            ResourceType::Knowledge => self
                .query
                .get_knowledge_article(number)
                .await
                .map(|document| document.map(VaultDocument::Knowledge)),
            ResourceType::Approval => self
                .query
                .get_approval(number)
                .await
                .map(|document| document.map(VaultDocument::Approval)),
            _ => self
                .query
                .get_record(number)
                .await
                .map(|document| document.map(VaultDocument::Record)),
        }
    }
}

/// Outcome of persisting a runtime record to the vault: the typed document
/// plus its vault-relative path. Relocated from `lib.rs` in Task 11 because
/// [`CoreContext::persist_document`] constructs and returns it.
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub(crate) enum PersistedRuntimeDocument {
    Record {
        record: SnowRecord,
        relative_path: PathBuf,
    },
    Knowledge {
        article: KnowledgeArticle,
        relative_path: PathBuf,
    },
    Approval {
        approval: ApprovalRecord,
        relative_path: PathBuf,
    },
}

impl PersistedRuntimeDocument {
    fn record(&self) -> &SnowRecord {
        match self {
            Self::Record { record, .. } => record,
            Self::Knowledge { article, .. } => &article.record,
            Self::Approval { approval, .. } => &approval.record,
        }
    }

    fn relative_path(&self) -> &Path {
        match self {
            Self::Record { relative_path, .. } => relative_path.as_path(),
            Self::Knowledge { relative_path, .. } => relative_path.as_path(),
            Self::Approval { relative_path, .. } => relative_path.as_path(),
        }
    }

    fn to_vault_document(&self) -> VaultDocument {
        match self {
            Self::Record { record, .. } => VaultDocument::Record(record.clone()),
            Self::Knowledge { article, .. } => VaultDocument::Knowledge(article.clone()),
            Self::Approval { approval, .. } => VaultDocument::Approval(approval.clone()),
        }
    }
}
