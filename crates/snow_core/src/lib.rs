#![allow(clippy::arc_with_non_send_sync)]
// rusqlite::Connection is Send but not Sync; Store is only ever accessed
// from a single task at a time, so Arc<Store> is deliberate.

pub mod cache;
pub mod config;
pub(crate) mod convert;
pub mod credential;
pub mod display;
pub mod enrich;
pub(crate) mod helpers;
pub mod kb;
pub mod query;
pub(crate) mod reference;
pub mod refresh;
pub mod resource;
pub(crate) mod semantic;
pub mod sla;
pub mod vault;

// Re-export extracted functions so existing callers in this file (and tests
// that use `super::*`) continue to work without path changes.
pub(crate) use convert::*;
pub use credential::{CredentialError, CredentialProvider};
pub(crate) use helpers::*;
pub use kb::{
    KnowledgeStatus, KnowledgeSyncMode, KnowledgeSyncOutcome, KnowledgeTagLayer,
    KnowledgeTagSummary,
};
pub(crate) use reference::*;
pub use resource::story::{StoryWriteConcurrency, StoryWriteResult};
pub use sla::{
    TaskSlaParentRef, TaskSlaReadability, TaskSlaStatus, TaskSlaSummaryView, TaskSlaView,
    is_task_sla_applicable_table,
};

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use servicenow_rs::prelude::{
    DisplayValue, FieldValue as SnowFieldValue, Order, Record, ServiceNowClient,
    child_relation_for_table,
};

use crate::cache::store::{AliasRow, KeywordRow, RecordRow, TagRow};
use crate::enrich::derive_for_record;
use crate::query::filter::{ApprovalQuery, ListQuery};
use crate::resource::approval::ApprovalResource;
use crate::semantic::{
    EmbeddingProvider, OllamaEmbeddingProvider, content_hash, cosine_similarity,
    maybe_exact_kb_identifier, normalize_title_match, reciprocal_rank_fusion_score,
    render_embedding_input, sanitize_semantic_text,
};
use crate::vault::manager::VaultManager;
use crate::vault::{VaultDocument, VaultScanFailure, scan_documents, scan_documents_detailed};

const USER_RECORD_HYDRATE_LIMIT: u32 = 200;
const RESOURCE_PLAN_CHILD_FIELDS: &[&str] = &[
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
    "sys_updated_on",
];

fn child_relation_for_parent_table(table_name: &str) -> Option<(&'static str, &'static str)> {
    match table_name {
        "pm_project" | "dmn_demand" => Some(("resource_plan", "task")),
        _ => child_relation_for_table(table_name),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SnowRecord {
    pub sys_id: String,
    pub number: String,
    pub table: String,
    pub resource_type: ResourceType,
    pub state: String,
    pub short_description: String,
    pub description: String,
    pub fields: HashMap<String, FieldValue>,
    pub work_notes: Vec<JournalEntry>,
    pub comments: Vec<JournalEntry>,
    pub parent: Option<RecordRef>,
    pub children: Vec<RecordRef>,
    pub references: HashMap<String, Reference>,
    pub synced_at: DateTime<Utc>,
    pub source: CacheSource,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ResourceType {
    Task,
    Incident,
    Change,
    ChangeTask,
    Request,
    RequestTask,
    Project,
    Demand,
    ResourcePlan,
    Story,
    ScrumTask,
    Knowledge,
    Approval,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Reference {
    pub sys_id: String,
    pub table: String,
    pub display_name: String,
    #[serde(default)]
    pub extra: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RecordRef {
    pub sys_id: String,
    pub number: String,
    pub table: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SearchResult {
    pub record: RecordRef,
    pub snippet: String,
    pub score: i64,
    pub match_in: MatchField,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub matched_value: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reasons: Vec<SearchMatchReason>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SearchMatchReason {
    pub field: MatchField,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SearchScope {
    All,
    Knowledge,
    WorkNotes,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum MatchField {
    Number,
    ShortDescription,
    Description,
    WorkNotes,
    Content,
    Tag,
    Keyword,
    Alias,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum CacheSource {
    Memory,
    Disk,
    Api,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum DegradedReadReason {
    MissingFile,
    UnreadableFile { error: String },
    ParseFailure { error: String },
    RawJsonFallback,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct DegradedReadDiagnostic {
    pub sys_id: String,
    pub number: String,
    pub table: String,
    pub resource_type: ResourceType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub markdown_path: Option<PathBuf>,
    pub reason: DegradedReadReason,
    pub observed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FieldValue {
    pub value: String,
    pub display_value: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct JournalEntry {
    pub timestamp: DateTime<Utc>,
    pub author: String,
    pub body: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KnowledgeArticle {
    pub record: SnowRecord,
    pub knowledge_base: Reference,
    pub category: Reference,
    pub article_type: String,
    pub content: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sn_tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub auto_tags: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub user_tags: Vec<String>,
    #[serde(default, skip_serializing_if = "bool_is_false")]
    pub body_cached: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub published_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub author: Option<Reference>,
    pub valid_to: Option<NaiveDate>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct KnowledgeSearchFilters {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub knowledge_base: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeSearchMode {
    Lexical,
    Semantic,
    #[default]
    Hybrid,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct KnowledgeSemanticSearchFilters {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub knowledge_base: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub category: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default)]
    pub mode: KnowledgeSearchMode,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_score_millis: Option<u32>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum KnowledgeEmbeddingCoverage {
    Metadata,
    FullText,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct KnowledgeSearchHit {
    pub article: KnowledgeArticle,
    pub mode: KnowledgeSearchMode,
    pub score: f32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub semantic_score: Option<f32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub lexical_score: Option<f32>,
    pub coverage: KnowledgeEmbeddingCoverage,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KnowledgeSemanticStatus {
    pub enabled: bool,
    pub provider: String,
    pub model: String,
    pub dimensions: usize,
    pub active_kb_articles: usize,
    pub metadata_embeddings: usize,
    pub full_text_embeddings: usize,
    pub stale_rows: usize,
    pub orphan_rows: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_rebuild_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct SemanticIndexSummary {
    pub full: bool,
    pub indexed_rows: usize,
    pub metadata_embeddings: usize,
    pub full_text_embeddings: usize,
    pub stale_rows: usize,
    pub orphan_rows: usize,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_rebuild_at: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KnowledgeBaseSummary {
    pub sys_id: String,
    pub display_name: String,
    pub article_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct KnowledgeCategorySummary {
    pub sys_id: String,
    pub knowledge_base_sys_id: String,
    pub display_name: String,
    pub article_count: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FieldChoice {
    pub label: String,
    pub value: String,
    #[serde(default)]
    pub terminal: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ApprovalRecord {
    pub record: SnowRecord,
    pub approver: Reference,
    pub target: RecordRef,
    pub requested_at: DateTime<Utc>,
    pub due_date: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RepairReport {
    pub scanned_records: usize,
    pub repaired_records: usize,
    pub skipped_records: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct RebuildReport {
    pub scanned_documents: usize,
    pub rebuilt_records: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrphanRecordRow {
    pub sys_id: String,
    pub number: String,
    pub file_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UnindexedVaultDocument {
    pub sys_id: String,
    pub number: String,
    pub relative_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct OrphanPruneReport {
    pub dry_run: bool,
    pub orphan_rows_scanned: usize,
    pub orphan_rows_pruned: usize,
    pub orphan_rows: Vec<OrphanRecordRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct VaultVerificationReport {
    pub scanned_documents: usize,
    pub active_records: usize,
    pub projected_references: usize,
    pub projected_relationships: usize,
    pub projected_enrichment_rows: usize,
    pub degraded_reads: Vec<DegradedReadDiagnostic>,
    pub missing_markdown_rows: Vec<OrphanRecordRow>,
    pub orphan_record_rows: Vec<OrphanRecordRow>,
    pub unprojectable_documents: Vec<VaultScanFailure>,
    pub unindexed_documents: Vec<UnindexedVaultDocument>,
}

pub(crate) fn normalize_knowledge_article(mut article: KnowledgeArticle) -> KnowledgeArticle {
    let knowledge_base =
        normalize_reference_for_field("knowledge_base", article.knowledge_base.clone());
    let category = normalize_reference_for_field("category", article.category.clone());
    let author = article
        .author
        .clone()
        .map(|reference| normalize_reference_for_field("author", reference));

    article.knowledge_base = knowledge_base.clone();
    article.category = category.clone();
    article.author = author.clone();
    if let Some(field) = article.record.fields.get_mut("knowledge_base") {
        field.display_value =
            (!knowledge_base.display_name.is_empty()).then(|| knowledge_base.display_name.clone());
    }
    if let Some(field) = article.record.fields.get_mut("category") {
        field.display_value =
            (!category.display_name.is_empty()).then(|| category.display_name.clone());
    }
    if let Some(field) = article.record.fields.get_mut("author") {
        field.display_value = author
            .as_ref()
            .filter(|reference| !reference.display_name.is_empty())
            .map(|reference| reference.display_name.clone());
    }
    article
        .record
        .references
        .insert("knowledge_base".to_string(), knowledge_base);
    article
        .record
        .references
        .insert("category".to_string(), category);
    match author {
        Some(author) => {
            article
                .record
                .references
                .insert("author".to_string(), author);
        }
        None => {
            article.record.references.remove("author");
        }
    }
    article.sn_tags = normalize_tag_layer(std::mem::take(&mut article.sn_tags));
    article.auto_tags = normalize_tag_layer(std::mem::take(&mut article.auto_tags));
    article.user_tags = normalize_tag_layer(std::mem::take(&mut article.user_tags));
    article.body_cached = article.body_cached || !article.content.trim().is_empty();
    article
}

fn bool_is_false(value: &bool) -> bool {
    !*value
}

fn normalize_tag_layer(tags: Vec<String>) -> Vec<String> {
    let mut normalized = Vec::new();
    for tag in tags {
        let tag = tag.trim();
        if tag.is_empty() {
            continue;
        }
        if normalized.iter().any(|existing| existing == tag) {
            continue;
        }
        normalized.push(tag.to_string());
    }
    normalized
}

fn knowledge_article_matches_semantic_filters(
    article: &KnowledgeArticle,
    filters: &KnowledgeSemanticSearchFilters,
) -> bool {
    matches_semantic_reference_filter(filters.knowledge_base.as_deref(), &article.knowledge_base)
        && matches_semantic_reference_filter(filters.category.as_deref(), &article.category)
}

fn matches_semantic_reference_filter(filter: Option<&str>, reference: &Reference) -> bool {
    let Some(filter) = filter.map(str::trim).filter(|value| !value.is_empty()) else {
        return true;
    };
    filter == reference.sys_id || filter.eq_ignore_ascii_case(reference.display_name.as_str())
}

impl SnowRecord {
    pub fn from_servicenow(record: &Record) -> Self {
        let number = record.get_str("number").unwrap_or_default().to_string();
        let table = canonical_record_table_for_number(&record.table, &number);
        let fields = record
            .fields()
            .iter()
            .map(|(key, value)| {
                (
                    key.clone(),
                    FieldValue {
                        value: value
                            .value
                            .as_ref()
                            .map(|v| match v {
                                serde_json::Value::String(s) => s.clone(),
                                other => other.to_string(),
                            })
                            .unwrap_or_default(),
                        display_value: value.display_value.clone(),
                    },
                )
            })
            .collect();

        Self {
            sys_id: record.sys_id.clone(),
            number,
            table: table.clone(),
            resource_type: ResourceType::from_table(&table),
            state: record.get_str("state").unwrap_or_default().to_string(),
            short_description: record
                .get_str("short_description")
                .unwrap_or_default()
                .to_string(),
            description: record
                .get_str("description")
                .unwrap_or_default()
                .to_string(),
            fields,
            work_notes: record
                .parse_journal("work_notes")
                .into_iter()
                .map(JournalEntry::from_servicenow)
                .collect(),
            comments: record
                .parse_journal("comments")
                .into_iter()
                .map(JournalEntry::from_servicenow)
                .collect(),
            parent: None,
            children: Vec::new(),
            references: HashMap::new(),
            synced_at: Utc::now(),
            source: CacheSource::Api,
        }
    }
}

impl ResourceType {
    pub fn from_table(table: &str) -> Self {
        let table = canonical_record_table(table);
        match table.as_str() {
            "task" => Self::Task,
            "incident" => Self::Incident,
            "change_request" => Self::Change,
            "change_task" => Self::ChangeTask,
            "sc_req_item" | "request_item" => Self::Request,
            "sc_task" => Self::RequestTask,
            "pm_project" => Self::Project,
            "dmn_demand" => Self::Demand,
            "resource_plan" => Self::ResourcePlan,
            "rm_story" => Self::Story,
            "rm_scrum_task" => Self::ScrumTask,
            "kb_knowledge" => Self::Knowledge,
            "sysapproval_approver" => Self::Approval,
            _ => Self::Change,
        }
    }
}

pub(crate) fn canonical_record_table(table: &str) -> String {
    let normalized = normalize_table_name(table);
    if is_change_request_table(&normalized) {
        "change_request".to_string()
    } else {
        normalized
    }
}

pub(crate) fn canonical_record_table_for_number(table: &str, number: &str) -> String {
    let normalized = normalize_table_name(table);
    if is_change_request_table(&normalized) || is_change_request_number(number) {
        "change_request".to_string()
    } else {
        normalized
    }
}

fn normalize_table_name(table: &str) -> String {
    table.trim().to_ascii_lowercase().replace([' ', '-'], "_")
}

fn is_change_request_table(normalized: &str) -> bool {
    matches!(
        normalized,
        "change" | "change_request" | "normal_change" | "standard_change" | "emergency_change"
    ) || normalized.starts_with("change_request_")
}

fn is_change_request_number(number: &str) -> bool {
    number.trim().to_ascii_uppercase().starts_with("CHG")
}

impl JournalEntry {
    pub fn from_servicenow(entry: servicenow_rs::prelude::JournalEntry) -> Self {
        let timestamp =
            chrono::NaiveDateTime::parse_from_str(&entry.timestamp, "%Y-%m-%d %H:%M:%S")
                .map(|dt| DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
                .unwrap_or_else(|_| Utc::now());
        Self {
            timestamp,
            author: entry.author,
            body: entry.body,
        }
    }
}

impl SearchResult {
    pub fn with_score(mut self, score: i64) -> Self {
        self.score = score;
        self
    }

    pub fn with_explanation(
        mut self,
        matched_value: Option<String>,
        reasons: Vec<SearchMatchReason>,
    ) -> Self {
        self.matched_value = matched_value;
        self.reasons = reasons;
        self
    }
}

#[derive(Clone)]
pub struct SnowCore {
    config: config::SnowConfig,
    client: Arc<ServiceNowClient>,
    vault_path: PathBuf,
    vault: VaultManager,
    query: Arc<query::QueryEngine>,
    cache: cache::CacheManager,
}

impl SnowCore {
    pub fn builder() -> SnowCoreBuilder {
        SnowCoreBuilder::default()
    }

    pub fn config(&self) -> &config::SnowConfig {
        &self.config
    }

    pub fn client(&self) -> &Arc<ServiceNowClient> {
        &self.client
    }

    pub fn vault_path(&self) -> &Path {
        &self.vault_path
    }

    /// Look up a record by number, checking the in-memory L1 cache first
    /// and falling through to the SQLite-backed query engine on a miss.
    pub async fn get_record(&self, number: &str) -> Result<Option<SnowRecord>> {
        if let Some(record) = self.cache.get(number) {
            return Ok(Some(record));
        }
        let record = self.query.get_record(number).await?;
        if let Some(ref record) = record {
            self.cache.put(record.clone());
        }
        Ok(record)
    }

    /// Fetch a record from the live ServiceNow API with raw and display
    /// values, enrich it with journal content, and persist it into the cache,
    /// vault, and search index.
    ///
    /// Unlike [`get_record`], which reads from the local cache only, this
    /// method always hits the ServiceNow REST API. After fetching, it calls
    /// [`enrich_record_journals`] to backfill `work_notes` and `comments`
    /// (which come back empty under the default `DisplayValue::Raw` mode),
    /// then persists the enriched record through the full pipeline.
    ///
    /// Journal enrichment is best-effort — if the inline journal fetch fails
    /// (ACL, timeout, etc.), the base record is still persisted.
    pub async fn get_record_fresh(&self, number: &str) -> Result<Option<SnowRecord>> {
        Ok(self
            .get_record_fresh_with_source(number)
            .await?
            .map(|(_, record)| record))
    }

    async fn get_record_fresh_with_source(
        &self,
        number: &str,
    ) -> Result<Option<(Record, SnowRecord)>> {
        let table = self.client.table_for_number(number).ok_or_else(|| {
            anyhow::anyhow!(
                "cannot resolve table for number '{number}' — unknown ServiceNow prefix"
            )
        })?;
        let Some(mut record) = self
            .client
            .table(table)
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

    async fn get_record_by_table_sys_id_fresh_with_source(
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
        let number = record
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
            .to_string();

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

    /// Fetch journal fields (`work_notes`, `comments`) with display values
    /// via [`ServiceNowClient::journal_inline`] and merge them into the record.
    ///
    /// Journal blobs can still come back empty under the detail projection, so
    /// this method performs a second query with `DisplayValue::Display` to
    /// retrieve the formatted journal blobs, then overwrites the corresponding
    /// fields on the mutable `Record`.
    async fn enrich_record_journals(&self, record: &mut Record) -> Result<()> {
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

    pub fn tombstone_record(&self, sys_id: &str, when: DateTime<Utc>) -> Result<()> {
        if let Ok(Some(row)) = self.query.store().get_record_by_sys_id(sys_id) {
            self.cache.invalidate(&row.number);
        }
        self.query
            .store()
            .tombstone_record(sys_id, when)
            .map_err(anyhow::Error::from)
    }

    pub async fn prune_record(&self, sys_id: &str, when: DateTime<Utc>) -> Result<()> {
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

    pub async fn get_knowledge_article(&self, number: &str) -> Result<Option<KnowledgeArticle>> {
        self.query.get_knowledge_article(number).await
    }

    pub async fn get_knowledge_article_fresh(
        &self,
        number: &str,
    ) -> Result<Option<KnowledgeArticle>> {
        let Some(record) = self.client.get_by_number(number).await? else {
            return Ok(None);
        };
        self.persist_record(&record)?;
        let article = self.query.get_knowledge_article(number).await?;
        self.maybe_run_inline_semantic_rebuild("fresh knowledge article")
            .await;
        Ok(article)
    }

    pub async fn search_knowledge(
        &self,
        query: &str,
        filters: KnowledgeSearchFilters,
    ) -> Result<Vec<KnowledgeArticle>> {
        self.query.search_knowledge(query, filters).await
    }

    pub async fn search_knowledge_semantic(
        &self,
        query: &str,
        filters: KnowledgeSemanticSearchFilters,
    ) -> Result<Vec<KnowledgeSearchHit>> {
        match filters.mode {
            KnowledgeSearchMode::Lexical => {
                self.search_knowledge_lexical_hits(query, &filters).await
            }
            KnowledgeSearchMode::Semantic => {
                let config = &self.config.kb.semantic_search;
                let sanitized = sanitize_semantic_text(query, config.query_max_chars);
                if sanitized.is_empty() {
                    return Ok(Vec::new());
                }
                if let Some(number) = maybe_exact_kb_identifier(query) {
                    return self
                        .exact_kb_hit(&number, KnowledgeSearchMode::Semantic, &filters)
                        .await;
                }
                let provider = self.semantic_provider_from_config()?;
                self.semantic_only_hits(&sanitized, &filters, provider.as_ref())
                    .await
            }
            KnowledgeSearchMode::Hybrid => {
                let config = &self.config.kb.semantic_search;
                let sanitized = sanitize_semantic_text(query, config.query_max_chars);
                if sanitized.is_empty() {
                    return Ok(Vec::new());
                }
                if let Some(number) = maybe_exact_kb_identifier(query) {
                    return self
                        .exact_kb_hit(&number, KnowledgeSearchMode::Hybrid, &filters)
                        .await;
                }
                let provider = match self.semantic_provider_from_config() {
                    Ok(provider) => provider,
                    Err(_err) if config.hybrid_fallback_to_lexical => {
                        return self
                            .search_knowledge_lexical_hits(query, &filters)
                            .await
                            .map(|hits| {
                                hits.into_iter()
                                    .map(|mut hit| {
                                        hit.mode = KnowledgeSearchMode::Hybrid;
                                        hit
                                    })
                                    .collect()
                            });
                    }
                    Err(err) => return Err(err),
                };
                match self
                    .hybrid_hits(query, &sanitized, &filters, provider.as_ref())
                    .await
                {
                    Ok(hits) => Ok(hits),
                    Err(_err) if config.hybrid_fallback_to_lexical => self
                        .search_knowledge_lexical_hits(query, &filters)
                        .await
                        .map(|hits| {
                            hits.into_iter()
                                .map(|mut hit| {
                                    hit.mode = KnowledgeSearchMode::Hybrid;
                                    hit
                                })
                                .collect()
                        }),
                    Err(err) => Err(err),
                }
            }
        }
    }

    pub async fn knowledge_semantic_status(&self) -> Result<KnowledgeSemanticStatus> {
        let config = &self.config.kb.semantic_search;
        let embeddings = self.query.store().list_knowledge_embeddings()?;
        let articles = self.load_active_knowledge_articles_for_semantic().await?;
        let mut stale_rows = 0usize;
        let mut dimensions = 0usize;
        let by_sys_id = embeddings
            .iter()
            .map(|row| {
                if row.model == config.model && dimensions == 0 {
                    dimensions = row.dimensions;
                }
                (row.record_sys_id.as_str(), row)
            })
            .collect::<HashMap<_, _>>();

        for article in &articles {
            let Some(existing) = by_sys_id.get(article.record.sys_id.as_str()) else {
                continue;
            };
            let (input, coverage) =
                render_embedding_input(article, config.include_tags_in_embedding_input);
            if existing.model != config.model
                || existing.provider != config.provider
                || existing.coverage != coverage
                || existing.content_hash != content_hash(&input)
            {
                stale_rows += 1;
            }
        }

        let meta = self.query.store().knowledge_semantic_meta()?;
        Ok(KnowledgeSemanticStatus {
            enabled: config.enabled,
            provider: config.provider.clone(),
            model: config.model.clone(),
            dimensions,
            active_kb_articles: articles.len(),
            metadata_embeddings: self.query.store().count_knowledge_embeddings_by_coverage(
                &config.model,
                KnowledgeEmbeddingCoverage::Metadata,
            )?,
            full_text_embeddings: self.query.store().count_knowledge_embeddings_by_coverage(
                &config.model,
                KnowledgeEmbeddingCoverage::FullText,
            )?,
            stale_rows,
            orphan_rows: self.query.store().count_orphan_knowledge_embeddings()?,
            last_rebuild_at: meta.last_rebuild_at,
            last_error: meta.last_error,
        })
    }

    pub async fn rebuild_knowledge_semantic_index(
        &self,
        full: bool,
    ) -> Result<SemanticIndexSummary> {
        let provider = self.semantic_provider_from_config()?;
        self.rebuild_knowledge_semantic_index_with_provider(full, provider.as_ref())
            .await
    }

    pub fn list_knowledge_bases(&self) -> Result<Vec<KnowledgeBaseSummary>> {
        Ok(self
            .query
            .list_knowledge_bases()?
            .into_iter()
            .map(|row| KnowledgeBaseSummary {
                sys_id: row.knowledge_base_sys_id,
                display_name: row.knowledge_base_name,
                article_count: row.article_count,
            })
            .collect())
    }

    pub fn list_categories(
        &self,
        knowledge_base_sys_id: &str,
    ) -> Result<Vec<KnowledgeCategorySummary>> {
        Ok(self
            .query
            .list_knowledge_categories(knowledge_base_sys_id)?
            .into_iter()
            .map(|row| KnowledgeCategorySummary {
                sys_id: row.category_sys_id,
                knowledge_base_sys_id: row.knowledge_base_sys_id,
                display_name: row.category_name,
                article_count: row.article_count,
            })
            .collect())
    }

    pub async fn list_knowledge_articles(
        &self,
        knowledge_base_sys_id: Option<&str>,
        category_sys_id: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<KnowledgeArticle>> {
        let mut numbers = self
            .query
            .store()
            .list_active_records(Some(ResourceType::Knowledge))?
            .into_iter()
            .map(|row| row.number)
            .collect::<Vec<_>>();
        numbers.sort();

        let mut articles = Vec::new();
        for number in numbers {
            let Some(article) = self.query.get_knowledge_article(&number).await? else {
                continue;
            };
            if let Some(expected) = knowledge_base_sys_id
                && article.knowledge_base.sys_id != expected
            {
                continue;
            }
            if let Some(expected) = category_sys_id
                && article.category.sys_id != expected
            {
                continue;
            }
            articles.push(article);
            if let Some(limit) = limit
                && articles.len() >= limit
            {
                break;
            }
        }

        Ok(articles)
    }

    fn semantic_provider_from_config(&self) -> Result<Box<dyn EmbeddingProvider>> {
        let config = &self.config.kb.semantic_search;
        anyhow::ensure!(config.enabled, "semantic KB search is not enabled");
        anyhow::ensure!(
            !config.model.trim().is_empty(),
            "semantic KB search model is not configured"
        );
        match config.provider.trim() {
            "ollama" => {
                anyhow::ensure!(
                    !config.endpoint.trim().is_empty(),
                    "semantic KB search endpoint is not configured"
                );
                Ok(Box::new(OllamaEmbeddingProvider::new(config)))
            }
            other => Err(anyhow::anyhow!(
                "unsupported semantic embedding provider `{other}`"
            )),
        }
    }

    async fn rebuild_knowledge_semantic_index_with_provider(
        &self,
        full: bool,
        provider: &dyn EmbeddingProvider,
    ) -> Result<SemanticIndexSummary> {
        let config = &self.config.kb.semantic_search;
        let articles = self.load_active_knowledge_articles_for_semantic().await?;
        let existing = self
            .query
            .store()
            .list_knowledge_embeddings()?
            .into_iter()
            .map(|row| (row.record_sys_id.clone(), row))
            .collect::<HashMap<_, _>>();
        let mut pending = Vec::<(String, KnowledgeEmbeddingCoverage, String, String)>::new();

        for article in &articles {
            let (input, coverage) =
                render_embedding_input(article, config.include_tags_in_embedding_input);
            let hash = content_hash(&input);
            if !full
                && existing.get(&article.record.sys_id).is_some_and(|row| {
                    row.model == provider.model()
                        && row.provider == provider.provider()
                        && row.coverage == coverage
                        && row.content_hash == hash
                })
            {
                continue;
            }
            pending.push((article.record.sys_id.clone(), coverage, hash, input));
        }

        let mut indexed_rows = 0usize;
        for batch in pending.chunks(config.batch_size.max(1)) {
            let inputs = batch
                .iter()
                .map(|(_, _, _, input)| input.clone())
                .collect::<Vec<_>>();
            let vectors = match provider.embed(&inputs).await {
                Ok(vectors) => vectors,
                Err(err) => {
                    self.query
                        .store()
                        .set_knowledge_semantic_meta(None, Some(&err.to_string()))?;
                    return Err(err);
                }
            };
            anyhow::ensure!(
                vectors.len() == batch.len(),
                "semantic embedding provider returned {} vectors for {} inputs",
                vectors.len(),
                batch.len()
            );
            let now = Utc::now();
            for ((record_sys_id, coverage, hash, _), vector) in batch.iter().zip(vectors) {
                self.query.store().upsert_knowledge_embedding(
                    &crate::cache::store::KnowledgeEmbeddingRow {
                        record_sys_id: record_sys_id.clone(),
                        model: provider.model().to_string(),
                        provider: provider.provider().to_string(),
                        dimensions: vector.len(),
                        coverage: *coverage,
                        content_hash: hash.clone(),
                        vector,
                        updated_at: now,
                    },
                )?;
                indexed_rows += 1;
            }
        }

        self.query.store().prune_orphan_knowledge_embeddings()?;
        let completed_at = Some(Utc::now());
        self.query
            .store()
            .set_knowledge_semantic_meta(completed_at, None)?;
        let status = self.knowledge_semantic_status().await?;
        Ok(SemanticIndexSummary {
            full,
            indexed_rows,
            metadata_embeddings: status.metadata_embeddings,
            full_text_embeddings: status.full_text_embeddings,
            stale_rows: status.stale_rows,
            orphan_rows: status.orphan_rows,
            last_rebuild_at: status.last_rebuild_at,
            last_error: status.last_error,
        })
    }

    async fn maybe_run_inline_semantic_rebuild(&self, trigger: &str) {
        if !self.config.kb.semantic_search.enabled {
            return;
        }
        if let Err(err) = self.rebuild_knowledge_semantic_index(false).await {
            eprintln!("snow_core: semantic KB rebuild failed after {trigger}: {err}");
        }
    }

    async fn load_active_knowledge_articles_for_semantic(&self) -> Result<Vec<KnowledgeArticle>> {
        let rows = self
            .query
            .store()
            .list_active_records(Some(ResourceType::Knowledge))?;
        let mut seen = std::collections::HashSet::new();
        let mut articles = Vec::new();
        for row in rows {
            if !seen.insert(row.sys_id.clone()) {
                continue;
            }
            if let Some(article) = self.query.get_knowledge_article(&row.number).await?
                && article.record.sys_id == row.sys_id
            {
                articles.push(article);
            }
        }
        Ok(articles)
    }

    async fn exact_kb_hit(
        &self,
        number: &str,
        mode: KnowledgeSearchMode,
        filters: &KnowledgeSemanticSearchFilters,
    ) -> Result<Vec<KnowledgeSearchHit>> {
        let Some(article) = self.get_knowledge_article(number).await? else {
            return Ok(Vec::new());
        };
        if !knowledge_article_matches_semantic_filters(&article, filters) {
            return Ok(Vec::new());
        }
        let coverage = self
            .query
            .store()
            .get_knowledge_embedding(&article.record.sys_id)?
            .filter(|row| row.model == self.config.kb.semantic_search.model)
            .map(|row| row.coverage)
            .unwrap_or_else(|| {
                if article.body_cached {
                    KnowledgeEmbeddingCoverage::FullText
                } else {
                    KnowledgeEmbeddingCoverage::Metadata
                }
            });
        Ok(vec![KnowledgeSearchHit {
            article,
            mode,
            score: 1.0,
            semantic_score: None,
            lexical_score: Some(1.0),
            coverage,
        }])
    }

    async fn search_knowledge_lexical_hits(
        &self,
        query: &str,
        filters: &KnowledgeSemanticSearchFilters,
    ) -> Result<Vec<KnowledgeSearchHit>> {
        let articles = self
            .search_knowledge(
                query,
                KnowledgeSearchFilters {
                    knowledge_base: filters.knowledge_base.clone(),
                    category: filters.category.clone(),
                    limit: Some(
                        filters
                            .limit
                            .unwrap_or(self.config.kb.semantic_search.top_k),
                    ),
                },
            )
            .await?;
        Ok(articles
            .into_iter()
            .enumerate()
            .map(|(idx, article)| KnowledgeSearchHit {
                coverage: if article.body_cached {
                    KnowledgeEmbeddingCoverage::FullText
                } else {
                    KnowledgeEmbeddingCoverage::Metadata
                },
                article,
                mode: KnowledgeSearchMode::Lexical,
                score: reciprocal_rank_fusion_score(idx + 1),
                semantic_score: None,
                lexical_score: Some(reciprocal_rank_fusion_score(idx + 1)),
            })
            .collect())
    }

    async fn semantic_only_hits(
        &self,
        sanitized_query: &str,
        filters: &KnowledgeSemanticSearchFilters,
        provider: &dyn EmbeddingProvider,
    ) -> Result<Vec<KnowledgeSearchHit>> {
        let status = self.knowledge_semantic_status().await?;
        if status.metadata_embeddings + status.full_text_embeddings == 0 {
            return Ok(Vec::new());
        }
        let query_vector = provider
            .embed(&[sanitized_query.to_string()])
            .await?
            .into_iter()
            .next()
            .ok_or_else(|| {
                anyhow::anyhow!("semantic embedding provider returned no query vector")
            })?;
        let min_score = filters
            .min_score_millis
            .unwrap_or(self.config.kb.semantic_search.min_score_millis)
            as f32
            / 1000.0;
        let limit = filters
            .limit
            .unwrap_or(self.config.kb.semantic_search.top_k);
        let candidate_pool = self.config.kb.semantic_search.candidate_pool;
        let articles = self
            .load_active_knowledge_articles_for_semantic()
            .await?
            .into_iter()
            .filter(|article| knowledge_article_matches_semantic_filters(article, filters))
            .map(|article| (article.record.sys_id.clone(), article))
            .collect::<HashMap<_, _>>();

        let mut ranked = self
            .query
            .store()
            .list_knowledge_embeddings()?
            .into_iter()
            .filter(|row| {
                row.model == provider.model() && articles.contains_key(&row.record_sys_id)
            })
            .map(|row| {
                let score = cosine_similarity(&query_vector, &row.vector)?;
                Ok((row, score))
            })
            .collect::<Result<Vec<_>>>()?;
        ranked.sort_by(|left, right| {
            right
                .1
                .partial_cmp(&left.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| {
                    articles[&left.0.record_sys_id]
                        .record
                        .number
                        .cmp(&articles[&right.0.record_sys_id].record.number)
                })
        });

        Ok(ranked
            .into_iter()
            .filter(|(_, score)| *score >= min_score)
            .take(candidate_pool)
            .filter_map(|(row, score)| {
                articles
                    .get(&row.record_sys_id)
                    .cloned()
                    .map(|article| KnowledgeSearchHit {
                        article,
                        mode: KnowledgeSearchMode::Semantic,
                        score,
                        semantic_score: Some(score),
                        lexical_score: None,
                        coverage: row.coverage,
                    })
            })
            .take(limit)
            .collect())
    }

    async fn hybrid_hits(
        &self,
        query: &str,
        sanitized_query: &str,
        filters: &KnowledgeSemanticSearchFilters,
        provider: &dyn EmbeddingProvider,
    ) -> Result<Vec<KnowledgeSearchHit>> {
        let candidate_pool = self.config.kb.semantic_search.candidate_pool;
        let lexical_articles = self
            .search_knowledge(
                query,
                KnowledgeSearchFilters {
                    knowledge_base: filters.knowledge_base.clone(),
                    category: filters.category.clone(),
                    limit: Some(candidate_pool),
                },
            )
            .await?;
        let has_active_embeddings = self
            .query
            .store()
            .list_knowledge_embeddings()?
            .iter()
            .any(|row| row.model == provider.model());

        let mut merged = BTreeMap::<String, KnowledgeSearchHit>::new();
        for (idx, article) in lexical_articles.into_iter().enumerate() {
            merged.insert(
                article.record.sys_id.clone(),
                KnowledgeSearchHit {
                    coverage: if article.body_cached {
                        KnowledgeEmbeddingCoverage::FullText
                    } else {
                        KnowledgeEmbeddingCoverage::Metadata
                    },
                    article,
                    mode: KnowledgeSearchMode::Hybrid,
                    score: reciprocal_rank_fusion_score(idx + 1),
                    semantic_score: None,
                    lexical_score: Some(reciprocal_rank_fusion_score(idx + 1)),
                },
            );
        }

        if has_active_embeddings {
            let semantic_hits = self
                .semantic_only_hits(sanitized_query, filters, provider)
                .await?
                .into_iter()
                .take(candidate_pool)
                .collect::<Vec<_>>();
            for (idx, hit) in semantic_hits.into_iter().enumerate() {
                let entry = merged
                    .entry(hit.article.record.sys_id.clone())
                    .or_insert(hit.clone());
                entry.article = hit.article;
                entry.coverage = hit.coverage;
                entry.semantic_score = hit.semantic_score;
                entry.score += reciprocal_rank_fusion_score(idx + 1);
                if entry.lexical_score.is_none() {
                    entry.lexical_score = None;
                }
            }
        }

        let normalized_query = normalize_title_match(sanitized_query);
        let mut hits = merged.into_values().collect::<Vec<_>>();
        hits.sort_by(|left, right| {
            let left_exact =
                normalize_title_match(&left.article.record.short_description) == normalized_query;
            let right_exact =
                normalize_title_match(&right.article.record.short_description) == normalized_query;
            right_exact
                .cmp(&left_exact)
                .then_with(|| {
                    right
                        .score
                        .partial_cmp(&left.score)
                        .unwrap_or(std::cmp::Ordering::Equal)
                })
                .then_with(|| left.article.record.number.cmp(&right.article.record.number))
        });
        hits.truncate(
            filters
                .limit
                .unwrap_or(self.config.kb.semantic_search.top_k),
        );
        Ok(hits)
    }

    pub async fn get_approval(&self, number: &str) -> Result<Option<ApprovalRecord>> {
        self.query.get_approval(number).await
    }

    pub fn degraded_reads(&self) -> Vec<DegradedReadDiagnostic> {
        self.query.degraded_reads()
    }

    pub async fn repair_missing_vault_files(&self) -> Result<usize> {
        Ok(self.repair_vault().await?.repaired_records)
    }

    pub async fn repair_vault(&self) -> Result<RepairReport> {
        let rows = self.query.store().list_active_records(None)?;
        let mut repaired = 0usize;
        let mut skipped = 0usize;
        let scanned = rows.len();

        for row in rows.into_iter().filter(|row| row.file_path.is_none()) {
            let Some(document) = self
                .load_runtime_document(&row.number, &row.resource_type)
                .await?
            else {
                skipped += 1;
                continue;
            };
            let persisted = self.persist_runtime_document(&document)?;
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

            self.query.store().upsert_record_with_tags(
                &repaired_row,
                &document_work_notes(document.record()),
                &document_content(&document),
                &document_tag_tokens(&document),
            )?;
            self.project_runtime_document(&document)?;
            self.persist_enrichment(document.record())?;
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
        let entries = scan_documents(&self.vault_path)?;
        let mut rebuilt = 0usize;
        let scanned = entries.len();

        for entry in entries {
            let document = entry.document;
            let row = record_row_from_runtime_record(
                document.record(),
                Some(entry.relative_path.clone()),
                serialize_vault_document(&document).to_string(),
            );
            self.query.store().upsert_record_with_tags(
                &row,
                &document_work_notes(document.record()),
                &document_content(&document),
                &document_tag_tokens(&document),
            )?;
            self.project_runtime_document(&document)?;
            self.persist_enrichment(document.record())?;
            rebuilt += 1;
        }

        Ok(RebuildReport {
            scanned_documents: scanned,
            rebuilt_records: rebuilt,
        })
    }

    pub fn verify_vault(&self) -> Result<VaultVerificationReport> {
        let scan_report = scan_documents_detailed(&self.vault_path)?;
        let entries = scan_report.entries;
        let rows = self.query.store().list_active_records(None)?;

        let mut indexed_sys_ids = BTreeMap::new();
        let mut missing_markdown_rows = Vec::new();
        let mut orphan_record_rows = Vec::new();
        for row in &rows {
            indexed_sys_ids.insert(row.sys_id.clone(), row.clone());
            match row.file_path.as_deref() {
                Some(relative_path) => {
                    let absolute_path = self.vault_path.join(relative_path);
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

        let projected_references = self.query.store().list_references()?.len();
        let projected_relationships = self.query.store().list_relationships()?.len();
        let mut projected_enrichment_rows = 0usize;
        for row in &rows {
            projected_enrichment_rows += self.query.store().list_tags(&row.sys_id)?.len();
            projected_enrichment_rows += self.query.store().list_keywords(&row.sys_id)?.len();
            projected_enrichment_rows += self.query.store().list_aliases(&row.sys_id)?.len();
        }

        Ok(VaultVerificationReport {
            scanned_documents: entries.len(),
            active_records: rows.len(),
            projected_references,
            projected_relationships,
            projected_enrichment_rows,
            degraded_reads: self.degraded_reads(),
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
            self.prune_record(&orphan.sys_id, Utc::now()).await?;
            pruned += 1;
        }

        Ok(OrphanPruneReport {
            dry_run: false,
            orphan_rows_scanned: scanned,
            orphan_rows_pruned: pruned,
            orphan_rows,
        })
    }

    pub async fn get_children(&self, number: &str) -> Result<Vec<SnowRecord>> {
        let mut cached = self.query.get_children(number).await?;
        if !cached.is_empty() {
            return Ok(cached);
        }

        let Some(parent_record) = self.client.get_by_number(number).await? else {
            return Ok(Vec::new());
        };
        self.persist_record(&parent_record)?;

        let Some((child_table, child_link_field)) =
            child_relation_for_parent_table(&parent_record.table)
        else {
            return Ok(Vec::new());
        };

        let mut query = self
            .client
            .table(child_table)
            .equals(child_link_field, &parent_record.sys_id)
            .display_value(DisplayValue::Both)
            .limit(500);
        if child_table == "resource_plan" {
            query = query.fields(RESOURCE_PLAN_CHILD_FIELDS).dot_walk(&[
                "task.number",
                "task.short_description",
                "task.sys_class_name",
            ]);
        }

        let child_records = query.execute().await?;

        for child in &child_records.records {
            self.persist_record(child)?;
        }

        cached = self.query.get_children(number).await?;
        Ok(cached)
    }

    pub async fn list_records(&self) -> Result<Vec<SnowRecord>> {
        self.list_records_query(query::filter::ListQuery::new())
            .await
    }

    pub async fn list_records_query(&self, query: ListQuery) -> Result<Vec<SnowRecord>> {
        self.query.list_records(query).await
    }

    pub async fn my_tasks(&self) -> Result<Vec<SnowRecord>> {
        self.query.my_tasks().await
    }

    pub async fn current_user_sys_id(&self) -> Result<String> {
        self.resolve_user_sys_id(&self.config.instance.user).await
    }

    pub async fn my_tasks_fresh(&self) -> Result<Vec<SnowRecord>> {
        let user_sys_id = self.current_user_sys_id().await?;
        self.hydrate_user_records_filtered(
            "task",
            "assigned_to",
            &user_sys_id,
            &[
                "sys_id",
                "number",
                "short_description",
                "description",
                "state",
                "assigned_to",
                "assignment_group",
                "opened_at",
                "due_date",
                "parent",
                "sys_class_name",
                "sys_updated_on",
                "sys_mod_count",
                "work_notes",
            ],
            &["parent.number", "parent.sys_class_name"],
            &[("sys_class_name", "task")],
        )
        .await?;
        self.hydrate_user_records(
            "change_task",
            "assigned_to",
            &user_sys_id,
            &[
                "sys_id",
                "number",
                "short_description",
                "description",
                "state",
                "assigned_to",
                "planned_start_date",
                "planned_start",
                "work_start",
                "expected_start",
                "start_date",
                "change_request",
                "work_notes",
            ],
            &["change_request.number", "change_request.sys_class_name"],
        )
        .await?;
        self.hydrate_user_records(
            "rm_scrum_task",
            "assigned_to",
            &user_sys_id,
            &[
                "sys_id",
                "number",
                "short_description",
                "description",
                "state",
                "assigned_to",
                "due_date",
                "story",
                "work_notes",
            ],
            &["story.number", "story.sys_class_name"],
        )
        .await?;

        let mut records = Vec::new();
        for resource_type in [
            ResourceType::Task,
            ResourceType::ChangeTask,
            ResourceType::ScrumTask,
        ] {
            records.extend(
                self.query
                    .list_records(
                        ListQuery::new()
                            .resource_type(resource_type)
                            .assigned_to(user_sys_id.clone()),
                    )
                    .await?,
            );
        }
        sort_records_by_number(&mut records);
        Ok(records)
    }

    pub async fn my_stories_fresh(&self) -> Result<Vec<SnowRecord>> {
        let user_sys_id = self.current_user_sys_id().await?;
        self.hydrate_user_records(
            "rm_story",
            "assigned_to",
            &user_sys_id,
            &[
                "sys_id",
                "number",
                "short_description",
                "description",
                "state",
                "assigned_to",
                "story_points",
                "sprint",
                "start_date",
                "work_notes",
            ],
            &[],
        )
        .await?;

        let mut records = self
            .query
            .list_records(
                ListQuery::new()
                    .resource_type(ResourceType::Story)
                    .assigned_to(user_sys_id),
            )
            .await?;
        sort_records_by_number(&mut records);
        Ok(records)
    }

    pub async fn my_incidents_fresh(&self) -> Result<Vec<SnowRecord>> {
        let user_sys_id = self.current_user_sys_id().await?;
        let hydration = self
            .hydrate_user_records(
                "incident",
                "assigned_to",
                &user_sys_id,
                &[
                    "sys_id",
                    "number",
                    "short_description",
                    "description",
                    "state",
                    "priority",
                    "opened_at",
                    "assigned_to",
                    "active",
                    "work_notes",
                ],
                &[],
            )
            .await?;
        let active_scope_sys_ids = hydration.sys_ids.into_iter().collect::<HashSet<_>>();

        let mut records = self
            .query
            .list_records(
                ListQuery::new()
                    .resource_type(ResourceType::Incident)
                    .assigned_to(user_sys_id),
            )
            .await?;
        let now = Utc::now();
        for record in records.iter().filter(|record| {
            !is_open_user_work_record(record)
                || (hydration.active_scope_complete
                    && !active_scope_sys_ids.contains(&record.sys_id))
        }) {
            self.tombstone_record(&record.sys_id, now)?;
        }
        records.retain(is_open_user_work_record);
        if hydration.active_scope_complete {
            records.retain(|record| active_scope_sys_ids.contains(&record.sys_id));
        }
        sort_records_by_number(&mut records);
        Ok(records)
    }

    pub async fn my_approvals_fresh(&self) -> Result<Vec<ApprovalRecord>> {
        let user_sys_id = self.current_user_sys_id().await?;
        self.hydrate_pending_approvals(&user_sys_id).await?;
        let mut approvals = self
            .query
            .approvals(ApprovalQuery::pending().approver_sys_id(user_sys_id))
            .await?;
        approvals.sort_by(|left, right| left.target.number.cmp(&right.target.number));
        Ok(approvals)
    }

    pub async fn my_approvals(&self) -> Result<Vec<ApprovalRecord>> {
        self.query.my_approvals().await
    }

    pub async fn my_projects(&self) -> Result<Vec<SnowRecord>> {
        let mut records = Vec::new();
        for resource_type in [ResourceType::Project, ResourceType::Demand] {
            records.extend(
                self.query
                    .list_records(ListQuery::new().resource_type(resource_type))
                    .await?,
            );
        }
        sort_records_by_number(&mut records);
        Ok(records)
    }

    pub async fn my_projects_fresh(&self) -> Result<Vec<SnowRecord>> {
        let user_sys_id = self.current_user_sys_id().await?;
        self.hydrate_user_records(
            "pm_project",
            "project_manager",
            &user_sys_id,
            &[
                "sys_id",
                "number",
                "name",
                "short_description",
                "description",
                "state",
                "project_manager",
                "start_date",
                "end_date",
                "percent_complete",
                "work_notes",
            ],
            &[],
        )
        .await?;
        self.hydrate_user_records(
            "dmn_demand",
            "demand_manager",
            &user_sys_id,
            &[
                "sys_id",
                "number",
                "short_description",
                "description",
                "state",
                "priority",
                "requested_by",
                "demand_manager",
                "start_date",
                "end_date",
                "business_case",
                "work_notes",
            ],
            &[],
        )
        .await?;

        let mut records = Vec::new();
        for resource_type in [ResourceType::Project, ResourceType::Demand] {
            records.extend(
                self.query
                    .list_records(
                        ListQuery::new()
                            .resource_type(resource_type)
                            .assigned_to(user_sys_id.clone()),
                    )
                    .await?,
            );
        }
        sort_records_by_number(&mut records);
        Ok(records)
    }

    pub async fn search(&self, query: &str, scope: SearchScope) -> Result<Vec<SearchResult>> {
        self.query.search(query, scope).await
    }

    pub async fn search_by_tag(&self, tag: &str, scope: SearchScope) -> Result<Vec<SearchResult>> {
        self.query.search_by_tag(tag, scope).await
    }

    pub async fn search_by_keyword(
        &self,
        keyword: &str,
        scope: SearchScope,
    ) -> Result<Vec<SearchResult>> {
        self.query.search_by_keyword(keyword, scope).await
    }

    pub async fn search_by_alias(
        &self,
        alias: &str,
        scope: SearchScope,
    ) -> Result<Vec<SearchResult>> {
        self.query.search_by_alias(alias, scope).await
    }

    /// Full-text search across cached records with automatic live-fetch
    /// fallback for exact record numbers.
    ///
    /// First runs the cache-only enriched search. If no results are found and
    /// the query matches an exact ServiceNow record number pattern (e.g.
    /// `INC4992697`, `chg0325640`), normalizes the query to uppercase and
    /// attempts a live API fetch via [`get_record_fresh`]. If the live fetch
    /// succeeds, the record is promoted into cache/vault/index and the search
    /// is re-run against the now-populated index.
    ///
    /// Free-text queries never trigger the live-fetch fallback.
    pub async fn search_enriched(
        &self,
        query: &str,
        scope: SearchScope,
    ) -> Result<Vec<SearchResult>> {
        let results = self.query.search_enriched(query, scope.clone()).await?;
        if !results.is_empty() {
            return Ok(results);
        }
        // Exact record-number fallback: if the query looks like "INC4992697"
        // and the cache has no hits, try a live API fetch to hydrate the cache,
        // then re-run the search. This ensures exact-number lookups work even
        // when the index is cold.
        if query::is_exact_record_number(query) {
            let normalized = query.trim().to_uppercase();
            // Gate on table_for_number: we can only fetch if the prefix maps
            // to a known table (INC→incident, CHG→change_request, etc.)
            if self.client.table_for_number(&normalized).is_some()
                && let Ok(Some(_)) = self.get_record_fresh(&normalized).await
            {
                return self.query.search_enriched(&normalized, scope).await;
            }
        }
        Ok(results)
    }

    pub async fn create_rm_story(&self, payload: serde_json::Value) -> Result<StoryWriteResult> {
        self.create_story_write_record(resource::story::StoryResource::PARENT_TABLE, payload)
            .await
    }

    pub async fn update_rm_story(
        &self,
        sys_id: &str,
        payload: serde_json::Value,
    ) -> Result<StoryWriteResult> {
        self.update_story_write_record(
            resource::story::StoryResource::PARENT_TABLE,
            sys_id,
            payload,
        )
        .await
    }

    pub async fn create_rm_scrum_task(
        &self,
        payload: serde_json::Value,
    ) -> Result<StoryWriteResult> {
        self.create_story_write_record(resource::story::StoryResource::CHILD_TABLE, payload)
            .await
    }

    pub async fn update_rm_scrum_task(
        &self,
        sys_id: &str,
        payload: serde_json::Value,
    ) -> Result<StoryWriteResult> {
        self.update_story_write_record(resource::story::StoryResource::CHILD_TABLE, sys_id, payload)
            .await
    }

    async fn create_story_write_record(
        &self,
        table: &str,
        payload: serde_json::Value,
    ) -> Result<StoryWriteResult> {
        let written = self.client.table(table).create(payload).await?;
        self.refetch_story_write_result(table, &written.sys_id, &written)
            .await
    }

    async fn update_story_write_record(
        &self,
        table: &str,
        sys_id: &str,
        payload: serde_json::Value,
    ) -> Result<StoryWriteResult> {
        let written = self.client.table(table).update(sys_id, payload).await?;
        self.refetch_story_write_result(table, sys_id, &written)
            .await
    }

    async fn refetch_story_write_result(
        &self,
        table: &str,
        expected_sys_id: &str,
        write_response: &Record,
    ) -> Result<StoryWriteResult> {
        let expected_sys_id = expected_sys_id.trim();
        if expected_sys_id.is_empty() {
            return Err(anyhow::anyhow!(
                "{} write response did not include a sys_id to refetch",
                table
            ));
        }

        let Some((fresh_row, fresh_record)) = self
            .get_record_by_table_sys_id_fresh_with_source(table, expected_sys_id)
            .await?
        else {
            return Err(anyhow::anyhow!(
                "{} write response returned {}, but fresh refetch by sys_id {} found no row",
                table,
                write_response.sys_id,
                expected_sys_id
            ));
        };

        let fresh_table = canonical_record_table(&fresh_row.table);
        if fresh_table != table {
            return Err(anyhow::anyhow!(
                "{} write refetched sys_id {} from unexpected table {}",
                table,
                expected_sys_id,
                fresh_row.table
            ));
        }
        if fresh_row.sys_id != expected_sys_id {
            return Err(anyhow::anyhow!(
                "{} write refetched sys_id {}, expected {}",
                table,
                fresh_row.sys_id,
                expected_sys_id
            ));
        }

        resource::story::StoryResource::write_result_from_fresh_row(fresh_record, &fresh_row)
    }

    pub async fn add_work_note(&self, number: &str, text: &str) -> Result<Option<SnowRecord>> {
        let Some((table, sys_id)) = self.lookup_table_and_sys_id(number).await? else {
            return Ok(None);
        };
        self.client.add_work_note(&table, &sys_id, text).await?;
        self.get_record_fresh(number).await
    }

    pub async fn set_state(&self, number: &str, state: &str) -> Result<Option<SnowRecord>> {
        let Some((table, sys_id)) = self.lookup_table_and_sys_id(number).await? else {
            return Ok(None);
        };
        self.client
            .table(&table)
            .update(&sys_id, serde_json::json!({ "state": state }))
            .await?;
        self.get_record_fresh(number).await
    }

    pub async fn field_choices(&self, table: &str, field: &str) -> Result<Vec<FieldChoice>> {
        let mut choices = self.field_choices_for_table(table, field).await?;
        if choices.is_empty() {
            for ancestor in self.table_ancestors(table).await? {
                choices = self.field_choices_for_table(&ancestor, field).await?;
                if !choices.is_empty() {
                    break;
                }
            }
        }
        if choices.is_empty() && field == "state" && table != "task" {
            choices = self.field_choices_for_table("task", field).await?;
        }
        Ok(choices)
    }

    pub async fn reassign(&self, number: &str, user: &str) -> Result<Option<SnowRecord>> {
        let Some((table, sys_id)) = self.lookup_table_and_sys_id(number).await? else {
            return Ok(None);
        };
        let assignee_sys_id = self.resolve_user_sys_id(user).await?;
        self.client
            .table(&table)
            .update(
                &sys_id,
                serde_json::json!({ "assigned_to": assignee_sys_id }),
            )
            .await?;
        self.get_record_fresh(number).await
    }

    pub async fn approve(&self, number: &str, comment: Option<&str>) -> Result<Option<SnowRecord>> {
        let Some((table, sys_id)) = self.lookup_table_and_sys_id(number).await? else {
            return Ok(None);
        };
        let approver_sys_id = self.resolve_user_sys_id(&self.config.instance.user).await?;
        let mut builder = self.client.approve(&table, &sys_id, &approver_sys_id);
        if let Some(comment) = comment {
            builder = builder.comment(comment);
        }
        builder.execute().await?;
        self.get_record_fresh(number).await
    }

    pub async fn reject(&self, number: &str, reason: &str) -> Result<Option<SnowRecord>> {
        let Some((table, sys_id)) = self.lookup_table_and_sys_id(number).await? else {
            return Ok(None);
        };
        let approver_sys_id = self.resolve_user_sys_id(&self.config.instance.user).await?;
        self.client
            .reject(&table, &sys_id, &approver_sys_id)
            .comment(reason)
            .execute()
            .await?;
        self.get_record_fresh(number).await
    }

    pub fn browser_url(&self, number: &str) -> String {
        format!(
            "{}/nav_to.do?uri={}.do?sysparm_query=number={}",
            self.client.base_url(),
            self.infer_table(number),
            number
        )
    }

    pub fn vault_relative_path_for_sys_id(&self, sys_id: &str) -> Result<Option<String>> {
        Ok(self
            .query
            .store()
            .get_record_by_sys_id(sys_id)?
            .and_then(|row| row.file_path))
    }

    fn persist_record(&self, record: &Record) -> Result<()> {
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
    fn persist_records(&self, records: &[Record]) -> Result<()> {
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

    fn record_kb_local_file_state(&self, persisted: &PersistedRuntimeDocument) -> Result<()> {
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

    fn persist_document(&self, record: &Record) -> Result<PersistedRuntimeDocument> {
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
                snow_record.references = collect_record_references(record);
                let persisted = self.vault.persist_record(&snow_record)?;
                Ok(PersistedRuntimeDocument::Record {
                    record: snow_record,
                    relative_path: persisted.relative_path,
                })
            }
        }
    }

    fn project_runtime_document(&self, document: &VaultDocument) -> Result<()> {
        let projection = project_runtime_document(document);
        let store = self.query.store();
        store
            .replace_relationships(document.record().sys_id.as_str(), &projection.relationships)?;
        store.replace_references(&projection.references.into_values().collect::<Vec<_>>())?;
        if let Some(article) = &projection.knowledge_article {
            store.upsert_knowledge_article(article)?;
        }
        Ok(())
    }

    fn persist_runtime_document(
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

    async fn load_runtime_document(
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

    fn persist_enrichment(&self, snow_record: &SnowRecord) -> Result<()> {
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

    async fn lookup_table_and_sys_id(&self, number: &str) -> Result<Option<(String, String)>> {
        let Some(record) = self.client.get_by_number(number).await? else {
            return Ok(None);
        };
        Ok(Some((record.table.clone(), record.sys_id.clone())))
    }

    async fn field_choices_for_table(&self, table: &str, field: &str) -> Result<Vec<FieldChoice>> {
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

    async fn table_ancestors(&self, table: &str) -> Result<Vec<String>> {
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

    async fn resolve_user_sys_id(&self, user: &str) -> Result<String> {
        let record = self
            .client
            .table("sys_user")
            .equals("user_name", user)
            .fields(&["sys_id"])
            .limit(1)
            .first()
            .await?
            .ok_or_else(|| anyhow::anyhow!("user not found: {user}"))?;
        Ok(record.sys_id)
    }

    async fn hydrate_user_records(
        &self,
        table: &str,
        user_field: &str,
        user_sys_id: &str,
        fields: &[&str],
        dot_walk: &[&str],
    ) -> Result<HydratedRecords> {
        self.hydrate_user_records_filtered(table, user_field, user_sys_id, fields, dot_walk, &[])
            .await
    }

    async fn hydrate_user_records_filtered(
        &self,
        table: &str,
        user_field: &str,
        user_sys_id: &str,
        fields: &[&str],
        dot_walk: &[&str],
        filters: &[(&str, &str)],
    ) -> Result<HydratedRecords> {
        let mut query = self
            .client
            .table(table)
            .equals(user_field, user_sys_id)
            .fields(fields)
            .display_value(DisplayValue::Both)
            .order_by("sys_updated_on", Order::Desc)
            .limit(USER_RECORD_HYDRATE_LIMIT);
        if !dot_walk.is_empty() {
            query = query.dot_walk(dot_walk);
        }
        for (field, value) in filters {
            query = query.equals(field, value);
        }

        let (records, active_scope_complete) = match query.equals("active", "true").execute().await
        {
            Ok(result) => {
                let active_scope_complete =
                    result.records.len() < USER_RECORD_HYDRATE_LIMIT as usize;
                (
                    result
                        .records
                        .into_iter()
                        .filter(servicenow_record_is_open_user_work)
                        .collect::<Vec<_>>(),
                    active_scope_complete,
                )
            }
            Err(_) => {
                let mut fallback = self
                    .client
                    .table(table)
                    .equals(user_field, user_sys_id)
                    .fields(fields)
                    .display_value(DisplayValue::Both)
                    .order_by("sys_updated_on", Order::Desc)
                    .limit(USER_RECORD_HYDRATE_LIMIT);
                if !dot_walk.is_empty() {
                    fallback = fallback.dot_walk(dot_walk);
                }
                for (field, value) in filters {
                    fallback = fallback.equals(field, value);
                }
                (
                    fallback
                        .execute()
                        .await?
                        .records
                        .into_iter()
                        .filter(servicenow_record_is_open_user_work)
                        .collect(),
                    false,
                )
            }
        };

        let sys_ids = records
            .iter()
            .map(|record| record.sys_id.clone())
            .collect::<Vec<_>>();
        self.persist_records(&records)?;
        Ok(HydratedRecords {
            sys_ids,
            active_scope_complete,
        })
    }

    async fn hydrate_pending_approvals(&self, user_sys_id: &str) -> Result<()> {
        let records = self
            .client
            .table("sysapproval_approver")
            .equals("approver", user_sys_id)
            .equals("state", "requested")
            .fields(&[
                "sys_id",
                "number",
                "state",
                "approver",
                "source_table",
                "sysapproval",
                "document_id",
                "due_date",
                "sys_created_on",
            ])
            .dot_walk(&[
                "sysapproval.number",
                "sysapproval.short_description",
                "sysapproval.state",
                "sysapproval.sys_class_name",
            ])
            .display_value(DisplayValue::Both)
            .order_by("sys_created_on", Order::Desc)
            .limit(200)
            .execute()
            .await?
            .records;

        self.persist_records(&records)?;
        Ok(())
    }

    fn infer_table(&self, number: &str) -> String {
        self.client
            .table_for_number(number)
            .unwrap_or("task")
            .to_string()
    }
}

fn is_open_user_work_record(record: &SnowRecord) -> bool {
    !is_terminal_state(Some(record.state.as_str())) && !record_field_is_false(record, "active")
}

fn servicenow_record_is_open_user_work(record: &Record) -> bool {
    !is_terminal_state(record.get_display("state").or(record.get_str("state")))
        && !servicenow_record_field_is_false(record, "active")
}

fn record_field_is_false(record: &SnowRecord, field_name: &str) -> bool {
    let Some(field) = record.fields.get(field_name) else {
        return false;
    };
    [Some(field.value.as_str()), field.display_value.as_deref()]
        .into_iter()
        .flatten()
        .map(|value| value.trim().to_ascii_lowercase())
        .any(|value| matches!(value.as_str(), "false" | "0" | "no"))
}

fn servicenow_record_field_is_false(record: &Record, field_name: &str) -> bool {
    [record.get_raw(field_name), record.get_display(field_name)]
        .into_iter()
        .flatten()
        .map(|value| value.trim().to_ascii_lowercase())
        .any(|value| matches!(value.as_str(), "false" | "0" | "no"))
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct HydratedRecords {
    sys_ids: Vec<String>,
    active_scope_complete: bool,
}

#[derive(Default)]
pub struct SnowCoreBuilder {
    config: Option<config::SnowConfig>,
    client: Option<ServiceNowClient>,
    vault_path: Option<PathBuf>,
}

impl SnowCoreBuilder {
    pub fn config(mut self, config: config::SnowConfig) -> Self {
        self.config = Some(config);
        self
    }

    pub fn client(mut self, client: ServiceNowClient) -> Self {
        self.client = Some(client);
        self
    }

    pub fn vault_path(mut self, path: impl Into<PathBuf>) -> Self {
        self.vault_path = Some(path.into());
        self
    }

    pub async fn build(self) -> Result<SnowCore> {
        let config = self.config.unwrap_or_default();
        let vault_path = self
            .vault_path
            .or_else(|| {
                if config.vault.path.as_os_str().is_empty() {
                    None
                } else {
                    Some(config.vault.path.clone())
                }
            })
            .unwrap_or_else(|| PathBuf::from("~/.config/snow/vault"));
        let client = Arc::new(
            self.client
                .ok_or_else(|| anyhow::anyhow!("missing client"))?,
        );
        let db_path = vault_root_to_db_path(&vault_path);
        let vault = VaultManager::new(&vault_path);
        let query = Arc::new(query::QueryEngine::open_with_vault(&db_path, &vault_path)?);
        let cache = cache::CacheManager::open(&db_path, config.cache.memory.capacity)?;

        Ok(SnowCore {
            config,
            client,
            vault_path,
            vault,
            query,
            cache,
        })
    }
}

#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
enum PersistedRuntimeDocument {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ResourceType;
    use crate::query::QueryEngine;
    use crate::vault::VaultDocument;
    use chrono::TimeZone;
    use servicenow_rs::prelude::{
        BasicAuth, DisplayValue, Record, ServiceNowClient, parse_servicenow_timestamp,
    };
    use tempfile::TempDir;
    use wiremock::matchers::{body_partial_json, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    fn sample_change_task_record() -> Record {
        let json = serde_json::json!({
            "sys_id": "task-sys",
            "number": "CTASK001",
            "short_description": "Apply change",
            "description": "Patch the server",
            "state": "Open",
            "assigned_to": {
                "value": "user-sys",
                "display_value": "Casey User"
            },
            "change_request": {
                "value": "chg-sys",
                "display_value": "CHG001"
            },
            "change_request.number": "CHG001",
            "change_request.sys_class_name": "change_request",
            "sys_updated_on": "2026-04-09 10:11:12",
            "sys_mod_count": "7",
            "work_notes": "2026-04-09 10:11:12 - Casey User (Work notes)\nUpdated task\n"
        });
        Record::from_json("change_task", &json, DisplayValue::Both).expect("record")
    }

    #[test]
    fn change_display_tables_are_canonicalized_in_core_records() {
        let record = Record::from_json(
            "Change Request",
            &serde_json::json!({
                "sys_id": "chg-sys",
                "number": "CHG0332518",
                "short_description": "Normal change",
                "state": { "value": "1", "display_value": "Open" }
            }),
            DisplayValue::Both,
        )
        .expect("record");

        let snow_record = SnowRecord::from_servicenow(&record);

        assert_eq!(snow_record.table, "change_request");
        assert_eq!(snow_record.resource_type, ResourceType::Change);
    }

    #[test]
    fn change_subclasses_and_chg_numbers_map_to_change_request() {
        assert_eq!(
            ResourceType::from_table("change_request_normal"),
            ResourceType::Change
        );
        assert_eq!(
            canonical_record_table_for_number("Normal", "CHG0332518"),
            "change_request"
        );
    }

    fn sample_incident_record() -> SnowRecord {
        SnowRecord {
            sys_id: "inc-sys".to_string(),
            number: "INC001".to_string(),
            table: "incident".to_string(),
            resource_type: ResourceType::Incident,
            state: "Open".to_string(),
            short_description: "Legacy incident".to_string(),
            description: "Legacy body".to_string(),
            fields: HashMap::from([(
                "assigned_to".to_string(),
                FieldValue {
                    value: "user-sys".to_string(),
                    display_value: Some("Casey User".to_string()),
                },
            )]),
            work_notes: vec![JournalEntry {
                timestamp: Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
                author: "Casey User".to_string(),
                body: "Investigating.".to_string(),
            }],
            comments: Vec::new(),
            parent: None,
            children: Vec::new(),
            references: HashMap::new(),
            synced_at: Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
            source: CacheSource::Disk,
        }
    }

    async fn core_for_mock_server(server: &MockServer) -> (SnowCore, TempDir) {
        let client = ServiceNowClient::builder()
            .instance(server.uri())
            .auth(BasicAuth::new("test_user", "test_pass"))
            .allow_http()
            .build()
            .await
            .expect("client");

        let tempdir = TempDir::new().expect("tempdir");
        let core = SnowCore::builder()
            .client(client)
            .vault_path(tempdir.path().join("vault"))
            .build()
            .await
            .expect("core");

        (core, tempdir)
    }

    async fn mount_fresh_record_get(
        server: &MockServer,
        table: &str,
        sys_id: &str,
        record: serde_json::Value,
    ) {
        Mock::given(method("GET"))
            .and(path(format!("/api/now/table/{table}/{sys_id}")))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "result": record })),
            )
            .mount(server)
            .await;
    }

    async fn mount_empty_journal_fetch(server: &MockServer, table: &str, sys_id: &str) {
        Mock::given(method("GET"))
            .and(path(format!("/api/now/table/{table}")))
            .and(query_param("sysparm_display_value", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": sys_id,
                    "work_notes": "",
                    "comments": ""
                }]
            })))
            .mount(server)
            .await;
    }

    fn sample_projected_record() -> SnowRecord {
        let mut record = sample_incident_record();
        record.sys_id = "inc-projected".to_string();
        record.number = "INC002".to_string();
        record.parent = Some(RecordRef {
            sys_id: "parent-sys".to_string(),
            number: "CHG002".to_string(),
            table: "change_request".to_string(),
        });
        record.children = vec![RecordRef {
            sys_id: "child-sys".to_string(),
            number: "INC003".to_string(),
            table: "incident".to_string(),
        }];
        record.references.insert(
            "assigned_to".to_string(),
            Reference {
                sys_id: "user-sys".to_string(),
                table: "sys_user".to_string(),
                display_name: "Casey User".to_string(),
                extra: HashMap::new(),
            },
        );
        record
    }

    fn sample_projected_knowledge_article() -> KnowledgeArticle {
        let mut record = SnowRecord {
            sys_id: "kb-projected".to_string(),
            number: "KB002".to_string(),
            table: "kb_knowledge".to_string(),
            resource_type: ResourceType::Knowledge,
            state: "published".to_string(),
            short_description: "Windows Access Runbook".to_string(),
            description: "How to request and validate Windows admin access.".to_string(),
            fields: HashMap::from([
                (
                    "workflow_state".to_string(),
                    FieldValue {
                        value: "published".to_string(),
                        display_value: Some("Published".to_string()),
                    },
                ),
                (
                    "published".to_string(),
                    FieldValue {
                        value: "2026-04-10 09:00:00".to_string(),
                        display_value: Some("2026-04-10 09:00:00".to_string()),
                    },
                ),
                (
                    "author".to_string(),
                    FieldValue {
                        value: "user-kb".to_string(),
                        display_value: Some("Jared Jennings".to_string()),
                    },
                ),
            ]),
            work_notes: Vec::new(),
            comments: Vec::new(),
            parent: None,
            children: Vec::new(),
            references: HashMap::from([(
                "author".to_string(),
                Reference {
                    sys_id: "user-kb".to_string(),
                    table: "sys_user".to_string(),
                    display_name: "Jared Jennings".to_string(),
                    extra: HashMap::new(),
                },
            )]),
            synced_at: Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
            source: CacheSource::Disk,
        };
        record.references.insert(
            "knowledge_base".to_string(),
            Reference {
                sys_id: "kb-base".to_string(),
                table: "kb_knowledge_base".to_string(),
                display_name: "IT".to_string(),
                extra: HashMap::new(),
            },
        );
        record.references.insert(
            "category".to_string(),
            Reference {
                sys_id: "kb-cat".to_string(),
                table: "kb_category".to_string(),
                display_name: "Access".to_string(),
                extra: HashMap::new(),
            },
        );

        KnowledgeArticle {
            record,
            knowledge_base: Reference {
                sys_id: "kb-base".to_string(),
                table: "kb_knowledge_base".to_string(),
                display_name: "IT".to_string(),
                extra: HashMap::new(),
            },
            category: Reference {
                sys_id: "kb-cat".to_string(),
                table: "kb_category".to_string(),
                display_name: "Access".to_string(),
                extra: HashMap::new(),
            },
            article_type: "text".to_string(),
            content: "Step 1: Request access.\nStep 2: Validate group membership.".to_string(),
            sn_tags: vec!["access".to_string()],
            auto_tags: vec!["request".to_string()],
            user_tags: vec!["tier-1".to_string()],
            body_cached: true,
            published_at: Some(
                chrono::NaiveDateTime::parse_from_str("2026-04-10 09:00:00", "%Y-%m-%d %H:%M:%S")
                    .map(|dt| DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
                    .expect("published timestamp"),
            ),
            author: Some(Reference {
                sys_id: "user-kb".to_string(),
                table: "sys_user".to_string(),
                display_name: "Jared Jennings".to_string(),
                extra: HashMap::new(),
            }),
            valid_to: Some(chrono::NaiveDate::from_ymd_opt(2027, 1, 1).unwrap()),
        }
    }

    async fn build_test_core(vault_path: PathBuf) -> SnowCore {
        let server = MockServer::start().await;
        let client = ServiceNowClient::builder()
            .instance(server.uri())
            .auth(BasicAuth::new("test_user", "test_pass"))
            .allow_http()
            .build()
            .await
            .expect("client");

        SnowCore::builder()
            .client(client)
            .vault_path(vault_path)
            .build()
            .await
            .expect("core")
    }

    async fn build_semantic_test_core(vault_path: PathBuf) -> SnowCore {
        let server = MockServer::start().await;
        let client = ServiceNowClient::builder()
            .instance(server.uri())
            .auth(BasicAuth::new("test_user", "test_pass"))
            .allow_http()
            .build()
            .await
            .expect("client");
        let config = config::SnowConfig {
            kb: config::KbConfig {
                semantic_search: config::KbSemanticSearchConfig {
                    enabled: true,
                    provider: "stub".to_string(),
                    model: "stub-model".to_string(),
                    ..Default::default()
                },
                ..Default::default()
            },
            ..Default::default()
        };

        SnowCore::builder()
            .config(config)
            .client(client)
            .vault_path(vault_path)
            .build()
            .await
            .expect("core")
    }

    fn seed_projected_knowledge_article(core: &SnowCore, article: &KnowledgeArticle) {
        let document = VaultDocument::Knowledge(article.clone());
        let persisted = core
            .persist_runtime_document(&document)
            .expect("persist runtime document");
        let row = record_row_from_runtime_record(
            &article.record,
            Some(persisted.relative_path.clone()),
            serialize_vault_document(&document).to_string(),
        );
        core.query
            .store()
            .upsert_record_with_tags(
                &row,
                "",
                &document_content(&document),
                &document_tag_tokens(&document),
            )
            .expect("upsert record");
        core.project_runtime_document(&document)
            .expect("project runtime document");
    }

    #[test]
    fn record_row_uses_remote_metadata_and_parent_linkage() {
        let row = record_row_from_servicenow(&sample_change_task_record()).expect("row");
        assert_eq!(row.resource_type, ResourceType::ChangeTask);
        assert_eq!(row.assigned_to.as_deref(), Some("user-sys"));
        assert_eq!(row.parent_id.as_deref(), Some("chg-sys"));
        assert_eq!(
            row.etag.as_deref(),
            Some("sys_mod_count:7:updated:2026-04-09T10:11:12+00:00")
        );
        assert_eq!(
            row.sys_updated_on,
            parse_servicenow_timestamp(Some("2026-04-09 10:11:12")).unwrap()
        );
    }

    #[test]
    fn persisted_raw_json_round_trips_parent_and_journals() {
        let store = crate::cache::store::Store::open_in_memory().expect("store");
        let row = record_row_from_servicenow(&sample_change_task_record()).expect("row");
        store
            .upsert_record(
                &row,
                &render_journal_entries(&collect_journal_entries(
                    &sample_change_task_record(),
                    "work_notes",
                )),
                row.description.as_deref().unwrap_or_default(),
            )
            .expect("insert");

        let engine = QueryEngine::from_store(store);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let record = runtime
            .block_on(engine.get_record("CTASK001"))
            .expect("query")
            .expect("record");

        assert_eq!(
            record.parent.as_ref().map(|parent| parent.number.as_str()),
            Some("CHG001")
        );
        assert_eq!(
            record.parent.as_ref().map(|parent| parent.table.as_str()),
            Some("change_request")
        );
        assert_eq!(record.work_notes.len(), 1);
        assert_eq!(record.work_notes[0].author, "Casey User");
    }

    #[tokio::test]
    async fn get_record_fresh_persists_enrichment_rows() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/change_task"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "task-sys",
                    "number": "CTASK001",
                    "short_description": "VPN gateway investigation",
                    "description": "Investigate gateway drops",
                    "state": "Open",
                    "assigned_to": {
                        "value": "user-sys",
                        "display_value": "Casey User"
                    },
                    "change_request": {
                        "value": "chg-sys",
                        "display_value": "CHG001"
                    },
                    "change_request.number": "CHG001",
                    "change_request.sys_class_name": "change_request",
                    "sys_updated_on": "2026-04-09 10:11:12",
                    "sys_mod_count": "7",
                    "work_notes": "2026-04-09 10:11:12 - Casey User (Work notes)\nInvestigating gateway.\n"
                }]
            })))
            .mount(&server)
            .await;

        // Journal inline mock — matches the enrich_record_journals call
        Mock::given(method("GET"))
            .and(path("/api/now/table/change_task"))
            .and(query_param("sysparm_display_value", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "task-sys",
                    "work_notes": "",
                    "comments": ""
                }]
            })))
            .mount(&server)
            .await;

        let client = ServiceNowClient::builder()
            .instance(server.uri())
            .auth(BasicAuth::new("test_user", "test_pass"))
            .allow_http()
            .build()
            .await
            .expect("client");

        let tempdir = TempDir::new().expect("tempdir");
        let core = SnowCore::builder()
            .client(client)
            .vault_path(tempdir.path().join("vault"))
            .build()
            .await
            .expect("core");

        let record = core
            .get_record_fresh("CTASK001")
            .await
            .expect("fresh record")
            .expect("record");

        let store = core.query.store();
        let row = store
            .get_record_by_number("CTASK001")
            .expect("record row")
            .expect("record row present");
        assert_eq!(row.file_path.as_deref(), Some("changes/CHG001/CTASK001.md"));
        let tags = store.list_tags(&record.sys_id).expect("tags");
        assert!(!tags.is_empty());
        assert!(tags.iter().any(|row| row.tag == "vpn"));
        assert!(
            store
                .list_keywords(&record.sys_id)
                .expect("keywords")
                .iter()
                .any(|row| row.keyword == "gateway")
        );
        assert!(
            store
                .list_aliases(&record.sys_id)
                .expect("aliases")
                .iter()
                .any(|row| row.alias == "vpn gateway investigation")
        );

        let references = store.list_references().expect("references");
        assert!(references.iter().any(|row| row.sys_id == "user-sys"));
        assert!(references.iter().any(|row| row.sys_id == "chg-sys"));

        let relationships = store.list_relationships().expect("relationships");
        assert!(relationships.iter().any(|row| {
            row.source_id == record.sys_id
                && row.target_id == "chg-sys"
                && row.rel_type == "parent"
                && row.field_name == "parent"
        }));
        assert!(relationships.iter().any(|row| {
            row.source_id == record.sys_id
                && row.target_id == "user-sys"
                && row.rel_type == "reference"
                && row.field_name == "assigned_to"
        }));
    }

    #[tokio::test]
    async fn create_rm_story_round_trips_record() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/now/table/rm_story"))
            .and(body_partial_json(serde_json::json!({
                "short_description": "Build the board writer"
            })))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "result": {
                    "sys_id": "story-sys",
                    "number": "STRY0001234",
                    "short_description": "Build the board writer",
                    "sys_updated_on": "2026-05-19 09:00:00",
                    "sys_mod_count": "1"
                }
            })))
            .mount(&server)
            .await;
        mount_fresh_record_get(
            &server,
            "rm_story",
            "story-sys",
            serde_json::json!({
                "sys_id": "story-sys",
                "number": "STRY0001234",
                "short_description": "Build the board writer",
                "description": "Fresh story body",
                "state": "1",
                "sys_updated_on": "2026-05-19 09:01:00",
                "sys_mod_count": "2"
            }),
        )
        .await;
        mount_empty_journal_fetch(&server, "rm_story", "story-sys").await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .create_rm_story(serde_json::json!({
                "short_description": "Build the board writer"
            }))
            .await
            .expect("create story");

        assert_eq!(result.record.sys_id, "story-sys");
        assert_eq!(result.record.number, "STRY0001234");
        assert_eq!(result.record.resource_type, ResourceType::Story);
        assert_eq!(result.concurrency.sys_updated_on, "2026-05-19 09:01:00");
        assert_eq!(result.concurrency.sys_mod_count, Some(2));
    }

    #[tokio::test]
    async fn update_rm_story_captures_sys_updated_on() {
        let server = MockServer::start().await;

        Mock::given(method("PATCH"))
            .and(path("/api/now/table/rm_story/story-sys"))
            .and(body_partial_json(serde_json::json!({ "state": "2" })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": {
                    "sys_id": "story-sys",
                    "number": "STRY0001234",
                    "state": "2",
                    "sys_updated_on": "2026-05-19 09:02:00"
                }
            })))
            .mount(&server)
            .await;
        mount_fresh_record_get(
            &server,
            "rm_story",
            "story-sys",
            serde_json::json!({
                "sys_id": "story-sys",
                "number": "STRY0001234",
                "short_description": "Build the board writer",
                "description": "Fresh story body",
                "state": "2",
                "sys_updated_on": "2026-05-19 09:03:00",
                "sys_mod_count": "4"
            }),
        )
        .await;
        mount_empty_journal_fetch(&server, "rm_story", "story-sys").await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .update_rm_story("story-sys", serde_json::json!({ "state": "2" }))
            .await
            .expect("update story");

        assert_eq!(result.record.number, "STRY0001234");
        assert_eq!(result.record.state, "2");
        assert_eq!(result.concurrency.sys_updated_on, "2026-05-19 09:03:00");
        assert_eq!(result.concurrency.sys_mod_count, Some(4));
    }

    #[tokio::test]
    async fn create_rm_scrum_task_link_to_parent() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/now/table/rm_scrum_task"))
            .and(body_partial_json(serde_json::json!({
                "story": "story-sys",
                "short_description": "Wire apply handler"
            })))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "result": {
                    "sys_id": "task-sys",
                    "number": "STSK0002001",
                    "story": "story-sys"
                }
            })))
            .mount(&server)
            .await;
        mount_fresh_record_get(
            &server,
            "rm_scrum_task",
            "task-sys",
            serde_json::json!({
                "sys_id": "task-sys",
                "number": "STSK0002001",
                "short_description": "Wire apply handler",
                "description": "",
                "state": "1",
                "story": {
                    "value": "story-sys",
                    "display_value": "STRY0001234"
                },
                "sys_updated_on": "2026-05-19 09:04:00",
                "sys_mod_count": "1"
            }),
        )
        .await;
        mount_empty_journal_fetch(&server, "rm_scrum_task", "task-sys").await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .create_rm_scrum_task(serde_json::json!({
                "story": "story-sys",
                "short_description": "Wire apply handler"
            }))
            .await
            .expect("create task");

        assert_eq!(result.record.resource_type, ResourceType::ScrumTask);
        assert_eq!(
            result
                .record
                .fields
                .get("story")
                .map(|field| field.value.as_str()),
            Some("story-sys")
        );
        assert_eq!(
            result
                .record
                .fields
                .get("story")
                .and_then(|field| field.display_value.as_deref()),
            Some("STRY0001234")
        );
    }

    #[tokio::test]
    async fn create_rm_scrum_task_round_trips_estimate_fields() {
        let server = MockServer::start().await;

        Mock::given(method("POST"))
            .and(path("/api/now/table/rm_scrum_task"))
            .and(body_partial_json(serde_json::json!({
                "planned_hours": "4",
                "actual_hours": "1"
            })))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "result": {
                    "sys_id": "task-estimate-sys",
                    "number": "STSK0002002"
                }
            })))
            .mount(&server)
            .await;
        mount_fresh_record_get(
            &server,
            "rm_scrum_task",
            "task-estimate-sys",
            serde_json::json!({
                "sys_id": "task-estimate-sys",
                "number": "STSK0002002",
                "short_description": "Estimate task",
                "description": "",
                "state": "1",
                "planned_hours": "4",
                "actual_hours": "1",
                "sys_updated_on": "2026-05-19 09:05:00"
            }),
        )
        .await;
        mount_empty_journal_fetch(&server, "rm_scrum_task", "task-estimate-sys").await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .create_rm_scrum_task(serde_json::json!({
                "short_description": "Estimate task",
                "planned_hours": "4",
                "actual_hours": "1"
            }))
            .await
            .expect("create task");

        assert_eq!(
            result
                .record
                .fields
                .get("planned_hours")
                .map(|field| field.value.as_str()),
            Some("4")
        );
        assert_eq!(
            result
                .record
                .fields
                .get("actual_hours")
                .map(|field| field.value.as_str()),
            Some("1")
        );
        assert_eq!(result.concurrency.sys_updated_on, "2026-05-19 09:05:00");
        assert_eq!(result.concurrency.sys_mod_count, None);
    }

    #[tokio::test]
    async fn update_rm_scrum_task_estimate_fields() {
        let server = MockServer::start().await;

        Mock::given(method("PATCH"))
            .and(path("/api/now/table/rm_scrum_task/task-estimate-sys"))
            .and(body_partial_json(serde_json::json!({
                "planned_hours": "6",
                "actual_hours": "2"
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": {
                    "sys_id": "task-estimate-sys",
                    "number": "STSK0002002"
                }
            })))
            .mount(&server)
            .await;
        mount_fresh_record_get(
            &server,
            "rm_scrum_task",
            "task-estimate-sys",
            serde_json::json!({
                "sys_id": "task-estimate-sys",
                "number": "STSK0002002",
                "short_description": "Estimate task",
                "description": "",
                "state": "1",
                "planned_hours": "6",
                "actual_hours": "2",
                "sys_updated_on": "2026-05-19 09:06:00",
                "sys_mod_count": "5"
            }),
        )
        .await;
        mount_empty_journal_fetch(&server, "rm_scrum_task", "task-estimate-sys").await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .update_rm_scrum_task(
                "task-estimate-sys",
                serde_json::json!({
                    "planned_hours": "6",
                    "actual_hours": "2"
                }),
            )
            .await
            .expect("update task");

        assert_eq!(
            result
                .record
                .fields
                .get("planned_hours")
                .map(|field| field.value.as_str()),
            Some("6")
        );
        assert_eq!(
            result
                .record
                .fields
                .get("actual_hours")
                .map(|field| field.value.as_str()),
            Some("2")
        );
        assert_eq!(result.concurrency.sys_mod_count, Some(5));
    }

    #[test]
    fn story_write_does_not_expose_generic_create_record() {
        let source = include_str!("lib.rs");
        let generic_create = ["pub async fn ", "create_record"].concat();
        let generic_update = ["pub async fn ", "update_record"].concat();

        assert!(!source.contains(&generic_create));
        assert!(!source.contains(&generic_update));
    }

    #[tokio::test]
    async fn my_tasks_fresh_hydrates_base_task_records() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_user"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "user-sys"
                }]
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/task"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "task-sys",
                    "number": "TASK001",
                    "short_description": "Base task assignment",
                    "description": "Follow up on a generic task",
                    "state": "Open",
                    "assigned_to": {
                        "value": "user-sys",
                        "display_value": "Casey User"
                    },
                    "assignment_group": {
                        "value": "group-sys",
                        "display_value": "Platform Support"
                    },
                    "sys_class_name": "task",
                    "active": "true",
                    "sys_updated_on": "2026-04-09 10:11:12",
                    "sys_mod_count": "1",
                    "work_notes": ""
                }]
            })))
            .mount(&server)
            .await;

        for table in ["change_task", "rm_scrum_task"] {
            Mock::given(method("GET"))
                .and(path(format!("/api/now/table/{table}")))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "result": []
                })))
                .mount(&server)
                .await;
        }

        let client = ServiceNowClient::builder()
            .instance(server.uri())
            .auth(BasicAuth::new("test_user", "test_pass"))
            .allow_http()
            .build()
            .await
            .expect("client");

        let tempdir = TempDir::new().expect("tempdir");
        let core = SnowCore::builder()
            .client(client)
            .vault_path(tempdir.path().join("vault"))
            .build()
            .await
            .expect("core");

        let records = core.my_tasks_fresh().await.expect("fresh tasks");
        let task = records
            .iter()
            .find(|record| record.number == "TASK001")
            .expect("base task");

        assert_eq!(task.table, "task");
        assert_eq!(task.resource_type, ResourceType::Task);
        assert_eq!(
            task.fields
                .get("assignment_group")
                .and_then(|field| field.display_value.as_deref()),
            Some("Platform Support")
        );

        let row = core
            .query
            .store()
            .get_record_by_number_and_type("TASK001", ResourceType::Task)
            .expect("row query")
            .expect("cached task row");
        assert_eq!(row.table_name, "task");
        assert_eq!(row.assigned_to.as_deref(), Some("user-sys"));
    }

    #[tokio::test]
    async fn my_incidents_fresh_tombstones_closed_or_inactive_incidents() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_user"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "user-sys"
                }]
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/incident"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    {
                        "sys_id": "closed-inc-sys",
                        "number": "INC4908273",
                        "short_description": "Closed incident should not show",
                        "description": "",
                        "state": { "value": "7", "display_value": "Closed" },
                        "active": { "value": "false", "display_value": "false" },
                        "priority": "3",
                        "opened_at": "2026-03-14 18:23:35",
                        "assigned_to": {
                            "value": "user-sys",
                            "display_value": "Casey User"
                        },
                        "work_notes": "",
                        "sys_updated_on": "2026-03-23 01:00:02"
                    },
                    {
                        "sys_id": "open-inc-sys",
                        "number": "INC5018610",
                        "short_description": "Open incident should show",
                        "description": "",
                        "state": { "value": "-5", "display_value": "Pending" },
                        "active": { "value": "true", "display_value": "true" },
                        "priority": "3",
                        "opened_at": "2026-05-10 12:00:00",
                        "assigned_to": {
                            "value": "user-sys",
                            "display_value": "Casey User"
                        },
                        "work_notes": "",
                        "sys_updated_on": "2026-05-10 12:00:00"
                    }
                ]
            })))
            .mount(&server)
            .await;

        let client = ServiceNowClient::builder()
            .instance(server.uri())
            .auth(BasicAuth::new("test_user", "test_pass"))
            .allow_http()
            .build()
            .await
            .expect("client");

        let tempdir = TempDir::new().expect("tempdir");
        let core = SnowCore::builder()
            .client(client)
            .vault_path(tempdir.path().join("vault"))
            .build()
            .await
            .expect("core");

        let mut closed_cached = sample_incident_record();
        closed_cached.sys_id = "closed-inc-sys".to_string();
        closed_cached.number = "INC4908273".to_string();
        closed_cached.state = "Closed".to_string();
        closed_cached.fields.insert(
            "active".to_string(),
            FieldValue {
                value: "false".to_string(),
                display_value: Some("false".to_string()),
            },
        );
        let closed_document = VaultDocument::Record(closed_cached.clone());
        let closed_persisted = core
            .persist_runtime_document(&closed_document)
            .expect("persist closed cached incident");
        let closed_row = record_row_from_runtime_record(
            &closed_cached,
            Some(closed_persisted.relative_path.clone()),
            serialize_vault_document(&closed_document).to_string(),
        );
        core.query
            .store()
            .upsert_record_with_tags(
                &closed_row,
                "",
                &document_content(&closed_document),
                &document_tag_tokens(&closed_document),
            )
            .expect("seed closed cached incident");

        let mut stale_cached = sample_incident_record();
        stale_cached.sys_id = "stale-inc-sys".to_string();
        stale_cached.number = "INC4900000".to_string();
        stale_cached.state = "Pending".to_string();
        let stale_document = VaultDocument::Record(stale_cached.clone());
        let stale_persisted = core
            .persist_runtime_document(&stale_document)
            .expect("persist stale cached incident");
        let stale_row = record_row_from_runtime_record(
            &stale_cached,
            Some(stale_persisted.relative_path.clone()),
            serialize_vault_document(&stale_document).to_string(),
        );
        core.query
            .store()
            .upsert_record_with_tags(
                &stale_row,
                "",
                &document_content(&stale_document),
                &document_tag_tokens(&stale_document),
            )
            .expect("seed stale cached incident");

        let records = core.my_incidents_fresh().await.expect("fresh incidents");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].number, "INC5018610");

        let closed_row = core
            .query
            .store()
            .get_record_by_number_and_type("INC4908273", ResourceType::Incident)
            .expect("row query")
            .expect("closed incident row");
        assert!(!closed_row.in_scope);
        assert!(closed_row.tombstoned_at.is_some());

        let stale_row = core
            .query
            .store()
            .get_record_by_number_and_type("INC4900000", ResourceType::Incident)
            .expect("row query")
            .expect("stale incident row");
        assert!(!stale_row.in_scope);
        assert!(stale_row.tombstoned_at.is_some());
    }

    #[tokio::test]
    async fn repair_missing_vault_files_backfills_legacy_rows() {
        let tempdir = TempDir::new().expect("tempdir");
        let core = build_test_core(tempdir.path().join("vault")).await;
        let record = sample_change_task_record();
        let legacy_row = record_row_from_servicenow(&record).expect("legacy row");
        core.query
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
        let record = sample_projected_record();
        core.vault
            .persist_record(&record)
            .expect("persist vault record");

        let rebuilt = core.rebuild_cache_from_vault().expect("rebuild cache");
        assert_eq!(rebuilt, 1);

        let row = core
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
            core.query
                .store()
                .list_keywords(&record.sys_id)
                .expect("keywords")
                .iter()
                .any(|row| row.keyword == "legacy")
        );
        let references = core.query.store().list_references().expect("references");
        assert!(references.iter().any(|row| row.sys_id == "parent-sys"));
        assert!(references.iter().any(|row| row.sys_id == "child-sys"));
        assert!(references.iter().any(|row| row.sys_id == "user-sys"));

        let relationships = core
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
            core.load_runtime_document("INC002", &ResourceType::Incident)
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
        core.vault
            .persist_knowledge_article(&article)
            .expect("persist knowledge article");

        let rebuilt = core.rebuild_cache_from_vault().expect("rebuild cache");
        assert_eq!(rebuilt, 1);

        let loaded = core
            .query
            .store()
            .get_knowledge_article(&article.record.sys_id)
            .expect("knowledge row lookup")
            .expect("knowledge row");
        assert_eq!(loaded.number, "KB002");
        assert_eq!(loaded.knowledge_base_name, "IT");
        assert_eq!(loaded.category_name, "Access");
        assert_eq!(loaded.author_name.as_deref(), Some("Jared Jennings"));
        assert_eq!(loaded.published_at.as_deref(), Some("2026-04-10 09:00:00"));
        assert_eq!(loaded.valid_to.as_deref(), Some("2027-01-01"));
    }

    #[tokio::test]
    async fn semantic_rebuild_updates_status_with_stub_provider() {
        let tempdir = TempDir::new().expect("tempdir");
        let core = build_semantic_test_core(tempdir.path().join("vault")).await;
        let article = sample_projected_knowledge_article();
        seed_projected_knowledge_article(&core, &article);

        let provider = crate::semantic::StubEmbeddingProvider::new("stub-model", 12);
        let summary = core
            .rebuild_knowledge_semantic_index_with_provider(true, &provider)
            .await
            .expect("semantic rebuild");
        assert_eq!(summary.indexed_rows, 1);

        let status = core
            .knowledge_semantic_status()
            .await
            .expect("semantic status");
        assert!(status.enabled);
        assert_eq!(status.active_kb_articles, 1);
        assert_eq!(status.full_text_embeddings, 1);
        assert_eq!(status.metadata_embeddings, 0);
        assert_eq!(status.stale_rows, 0);
        assert_eq!(status.orphan_rows, 0);
        assert_eq!(status.model, "stub-model");
        assert_eq!(status.provider, "stub");
        assert_eq!(status.dimensions, 12);
    }

    #[tokio::test]
    async fn semantic_search_short_circuits_exact_kb_identifier_without_provider() {
        let tempdir = TempDir::new().expect("tempdir");
        let core = build_semantic_test_core(tempdir.path().join("vault")).await;
        let article = sample_projected_knowledge_article();
        seed_projected_knowledge_article(&core, &article);

        let hits = core
            .search_knowledge_semantic(
                "kb002",
                KnowledgeSemanticSearchFilters {
                    mode: KnowledgeSearchMode::Hybrid,
                    ..Default::default()
                },
            )
            .await
            .expect("semantic KB lookup");
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].article.record.number, "KB002");
        assert_eq!(hits[0].mode, KnowledgeSearchMode::Hybrid);
    }

    #[tokio::test]
    async fn knowledge_article_paths_normalize_unresolved_reference_labels() {
        let tempdir = TempDir::new().expect("tempdir");
        let core = build_test_core(tempdir.path().join("vault")).await;
        let unresolved_sys_id = "0e3952d41b7d15032d1ece5624bcb4e";
        let record = Record::from_json(
            "kb_knowledge",
            &serde_json::json!({
                "sys_id": "kb-normalized-sys",
                "number": "KB0105015",
                "short_description": "Submitting Access Requests",
                "description": "Request flow summary",
                "article_body": "Request flow body",
                "state": "published",
                "workflow_state": "published",
                "article_type": "text",
                "published": "2026-04-10 09:00:00",
                "knowledge_base": {
                    "value": "kb-base-sys",
                    "display_value": "Employee Services"
                },
                "category": {
                    "value": "kb-cat-sys",
                    "display_value": unresolved_sys_id
                },
                "author": {
                    "value": "user-sys",
                    "display_value": unresolved_sys_id
                }
            }),
            DisplayValue::Both,
        )
        .expect("knowledge record");

        core.persist_record(&record)
            .expect("persist knowledge record");

        let article = core
            .get_knowledge_article("KB0105015")
            .await
            .expect("get article")
            .expect("article present");
        assert_eq!(article.knowledge_base.display_name, "Employee Services");
        assert!(article.category.display_name.is_empty());
        assert_eq!(article.category.sys_id, "kb-cat-sys");
        assert_eq!(
            article
                .author
                .as_ref()
                .map(|author| author.display_name.as_str()),
            Some("")
        );
        assert_eq!(
            article
                .record
                .references
                .get("author")
                .map(|reference| reference.display_name.as_str()),
            Some("")
        );

        let search_results = core
            .search_knowledge(
                "request flow",
                KnowledgeSearchFilters {
                    limit: Some(10),
                    ..KnowledgeSearchFilters::default()
                },
            )
            .await
            .expect("search knowledge");
        let listed = core
            .list_knowledge_articles(Some("kb-base-sys"), Some("kb-cat-sys"), Some(10))
            .await
            .expect("list knowledge articles");

        assert_eq!(search_results.len(), 1);
        assert_eq!(listed.len(), 1);
        assert_eq!(
            search_results[0].knowledge_base.display_name,
            article.knowledge_base.display_name
        );
        assert_eq!(
            search_results[0].category.display_name,
            article.category.display_name
        );
        assert_eq!(
            search_results[0]
                .author
                .as_ref()
                .map(|author| author.display_name.as_str()),
            article
                .author
                .as_ref()
                .map(|author| author.display_name.as_str())
        );
        assert_eq!(
            listed[0].knowledge_base.display_name,
            article.knowledge_base.display_name
        );
        assert_eq!(
            listed[0].category.display_name,
            article.category.display_name
        );
        assert_eq!(
            listed[0]
                .author
                .as_ref()
                .map(|author| author.display_name.as_str()),
            article
                .author
                .as_ref()
                .map(|author| author.display_name.as_str())
        );

        let vault_relative = core
            .vault_relative_path_for_sys_id("kb-normalized-sys")
            .expect("vault path lookup")
            .expect("vault path present");
        let vault_markdown = std::fs::read_to_string(core.vault_path().join(vault_relative))
            .expect("read knowledge markdown");
        assert!(vault_markdown.contains("display_name: \"Employee Services\""));
        assert!(vault_markdown.contains("display_name: \"\""));
        assert!(!vault_markdown.contains(&format!("display_name: \"{unresolved_sys_id}\"")));
        assert!(!vault_markdown.contains(&format!("display_value: \"{unresolved_sys_id}\"")));
    }

    #[tokio::test]
    async fn verify_vault_reports_projection_and_orphans() {
        let tempdir = TempDir::new().expect("tempdir");
        let core = build_test_core(tempdir.path().join("vault")).await;
        let record = sample_projected_record();
        core.vault
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
        core.query
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
            core.query
                .store()
                .get_record_by_number("CTASK001")
                .expect("lookup row")
                .is_none()
        );
    }

    #[tokio::test]
    async fn tombstone_keeps_markdown_and_prune_removes_both_layers() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/change_task"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "task-sys",
                    "number": "CTASK001",
                    "short_description": "Apply change",
                    "description": "Patch the server",
                    "state": "Open",
                    "assigned_to": {
                        "value": "user-sys",
                        "display_value": "Casey User"
                    },
                    "change_request": {
                        "value": "chg-sys",
                        "display_value": "CHG001"
                    },
                    "change_request.number": "CHG001",
                    "change_request.sys_class_name": "change_request",
                    "sys_updated_on": "2026-04-09 10:11:12",
                    "sys_mod_count": "7",
                    "work_notes": "2026-04-09 10:11:12 - Casey User (Work notes)\nUpdated task\n"
                }]
            })))
            .mount(&server)
            .await;

        // Journal inline mock — matches the enrich_record_journals call
        Mock::given(method("GET"))
            .and(path("/api/now/table/change_task"))
            .and(query_param("sysparm_display_value", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "task-sys",
                    "work_notes": "",
                    "comments": ""
                }]
            })))
            .mount(&server)
            .await;

        let client = ServiceNowClient::builder()
            .instance(server.uri())
            .auth(BasicAuth::new("test_user", "test_pass"))
            .allow_http()
            .build()
            .await
            .expect("client");

        let tempdir = TempDir::new().expect("tempdir");
        let core = SnowCore::builder()
            .client(client)
            .vault_path(tempdir.path().join("vault"))
            .build()
            .await
            .expect("core");

        let record = core
            .get_record_fresh("CTASK001")
            .await
            .expect("fresh record")
            .expect("record");

        let store = core.query.store();
        let row = store
            .get_record_by_sys_id(&record.sys_id)
            .expect("record row")
            .expect("record row present");
        let markdown_path = core
            .vault_path()
            .join(row.file_path.as_deref().expect("file path"));

        assert!(markdown_path.exists());

        core.tombstone_record(&record.sys_id, Utc.timestamp_opt(1_712_650_100, 0).unwrap())
            .expect("tombstone");
        assert!(markdown_path.exists());
        assert_eq!(
            store
                .get_record_by_sys_id(&record.sys_id)
                .expect("tombstoned row")
                .expect("row still present")
                .lifecycle(),
            crate::cache::store::RecordLifecycle::Tombstoned
        );

        core.prune_record(&record.sys_id, Utc.timestamp_opt(1_712_650_200, 0).unwrap())
            .await
            .expect("prune");

        assert!(!markdown_path.exists());
        assert!(
            store
                .get_record_by_sys_id(&record.sys_id)
                .expect("pruned row lookup")
                .is_none()
        );
        assert!(store.list_tags(&record.sys_id).expect("tags").is_empty());
        assert!(
            store
                .list_keywords(&record.sys_id)
                .expect("keywords")
                .is_empty()
        );
        assert!(
            store
                .list_aliases(&record.sys_id)
                .expect("aliases")
                .is_empty()
        );
    }

    #[tokio::test]
    async fn get_children_live_hydrates_cache() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/change_request"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "chg-sys",
                    "number": "CHG0329219",
                    "short_description": "CWR Joiner Fixes",
                    "description": "Parent record",
                    "state": "Open",
                    "sys_updated_on": "2026-04-09 10:11:12"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/change_task"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "task-sys",
                    "number": { "value": "CTASK0660518", "display_value": "CTASK0660518" },
                    "short_description": { "value": "Pre-Implementation Testing", "display_value": "Pre-Implementation Testing" },
                    "description": { "value": "Document testing", "display_value": "Document testing" },
                    "state": { "value": "1", "display_value": "Open" },
                    "assigned_to": { "value": "user-sys", "display_value": "Tuan Le" },
                    "change_request": { "value": "chg-sys", "display_value": "CHG0329219" },
                    "change_request.number": "CHG0329219",
                    "change_request.sys_class_name": "change_request",
                    "sys_updated_on": { "value": "2026-04-09 10:12:13", "display_value": "2026-04-09 10:12:13" }
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = ServiceNowClient::builder()
            .instance(server.uri())
            .auth(BasicAuth::new("test_user", "test_pass"))
            .allow_http()
            .build()
            .await
            .expect("client");

        let tempdir = TempDir::new().expect("tempdir");
        let core = SnowCore::builder()
            .client(client)
            .vault_path(tempdir.path().join("vault"))
            .build()
            .await
            .expect("core");

        let first = core.get_children("CHG0329219").await.expect("first fetch");
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].number, "CTASK0660518");
        assert_eq!(
            first[0]
                .parent
                .as_ref()
                .map(|parent| parent.number.as_str()),
            Some("CHG0329219")
        );

        let second = core.get_children("CHG0329219").await.expect("cached fetch");
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].number, "CTASK0660518");
    }

    #[tokio::test]
    async fn get_children_live_hydrates_project_resource_plans() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/pm_project"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "project-sys",
                    "number": "PRJ0161206",
                    "short_description": "Resource visibility project",
                    "description": "Project record",
                    "state": { "value": "1", "display_value": "Open" },
                    "sys_updated_on": "2026-05-11 10:11:12"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/resource_plan"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "rpln-sys",
                    "number": { "value": "RPLN0089255", "display_value": "RPLN0089255" },
                    "short_description": { "value": "Nursing analytics allocation", "display_value": "Nursing analytics allocation" },
                    "description": { "value": "Resource plan record", "display_value": "Resource plan record" },
                    "state": { "value": "11", "display_value": "Allocated" },
                    "task": { "value": "project-sys", "display_value": "PRJ0161206" },
                    "task.number": "PRJ0161206",
                    "task.sys_class_name": "pm_project",
                    "resource_type": { "value": "group", "display_value": "Group" },
                    "group_resource": { "value": "group-sys", "display_value": "Project Delivery" },
                    "start_date": { "value": "2026-05-01", "display_value": "2026-05-01" },
                    "end_date": { "value": "2026-05-31", "display_value": "2026-05-31" },
                    "planned_hours": { "value": "80", "display_value": "80" },
                    "allocated_hours": { "value": "80", "display_value": "80" },
                    "confirmed_hours": { "value": "0", "display_value": "0" },
                    "sys_updated_on": { "value": "2026-05-11 10:12:13", "display_value": "2026-05-11 10:12:13" }
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = ServiceNowClient::builder()
            .instance(server.uri())
            .auth(BasicAuth::new("test_user", "test_pass"))
            .allow_http()
            .build()
            .await
            .expect("client");

        let tempdir = TempDir::new().expect("tempdir");
        let core = SnowCore::builder()
            .client(client)
            .vault_path(tempdir.path().join("vault"))
            .build()
            .await
            .expect("core");

        let children = core.get_children("PRJ0161206").await.expect("children");
        assert_eq!(children.len(), 1);
        assert_eq!(children[0].number, "RPLN0089255");
        assert_eq!(children[0].resource_type, ResourceType::ResourcePlan);
        assert_eq!(
            children[0].parent.as_ref().map(|parent| {
                (
                    parent.number.as_str(),
                    parent.table.as_str(),
                    parent.sys_id.as_str(),
                )
            }),
            Some(("PRJ0161206", "pm_project", "project-sys"))
        );
        assert_eq!(
            children[0]
                .fields
                .get("state")
                .and_then(|field| field.display_value.as_deref()),
            Some("Allocated")
        );
    }

    #[tokio::test]
    async fn field_choices_returns_active_unique_choices() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_choice"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    { "value": "1", "label": "New", "sequence": "100", "inactive": "false" },
                    { "value": "2", "label": "In Progress", "sequence": "200", "inactive": "false" },
                    { "value": "2", "label": "Duplicate", "sequence": "300", "inactive": "false" },
                    { "value": "7", "label": "Closed", "sequence": "400", "inactive": "true" }
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = ServiceNowClient::builder()
            .instance(server.uri())
            .auth(BasicAuth::new("test_user", "test_pass"))
            .allow_http()
            .build()
            .await
            .expect("client");

        let tempdir = TempDir::new().expect("tempdir");
        let core = SnowCore::builder()
            .client(client)
            .vault_path(tempdir.path().join("vault"))
            .build()
            .await
            .expect("core");

        let choices = core
            .field_choices("incident", "state")
            .await
            .expect("field choices");

        assert_eq!(
            choices,
            vec![
                FieldChoice {
                    value: "1".to_string(),
                    label: "New".to_string(),
                    terminal: false,
                },
                FieldChoice {
                    value: "2".to_string(),
                    label: "In Progress".to_string(),
                    terminal: false,
                },
            ]
        );
    }

    #[tokio::test]
    async fn my_projects_fresh_hydrates_projects_and_demands() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_user"))
            .and(query_param("sysparm_query", "user_name=test_user"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "user-sys"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/pm_project"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "prj-sys",
                    "number": "PRJ0001001",
                    "name": "Core network refresh",
                    "short_description": "Refresh the core network",
                    "description": "Project record",
                    "state": { "value": "1", "display_value": "Open" },
                    "project_manager": { "value": "user-sys", "display_value": "Casey User" },
                    "sys_updated_on": "2026-04-09 10:11:12"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/dmn_demand"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "dmnd-sys",
                    "number": "DMND0002002",
                    "short_description": "Branch switch upgrades",
                    "description": "Demand record",
                    "state": { "value": "1", "display_value": "Draft" },
                    "demand_manager": { "value": "user-sys", "display_value": "Casey User" },
                    "requested_by": { "value": "requester-sys", "display_value": "Requester" },
                    "sys_updated_on": "2026-04-09 10:12:13"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = ServiceNowClient::builder()
            .instance(server.uri())
            .auth(BasicAuth::new("test_user", "test_pass"))
            .allow_http()
            .build()
            .await
            .expect("client");

        let tempdir = TempDir::new().expect("tempdir");
        let mut config = config::SnowConfig::default();
        config.instance = config::InstanceConfig {
            url: server.uri(),
            user: "test_user".to_string(),
            credential: crate::CredentialProvider::Env,
        };
        config.vault = config::VaultConfig {
            path: tempdir.path().join("vault"),
        };
        config.apply_defaults();
        let core = SnowCore::builder()
            .config(config)
            .client(client)
            .vault_path(tempdir.path().join("vault"))
            .build()
            .await
            .expect("core");

        let records = core.my_projects_fresh().await.expect("fresh projects");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].number, "DMND0002002");
        assert_eq!(records[0].resource_type, ResourceType::Demand);
        assert_eq!(records[1].number, "PRJ0001001");
        assert_eq!(records[1].resource_type, ResourceType::Project);

        let cached = core.my_projects().await.expect("cached projects");
        assert_eq!(cached.len(), 2);

        let filtered = core
            .list_records_query(
                ListQuery::new()
                    .resource_type(ResourceType::Demand)
                    .assigned_to("user-sys"),
            )
            .await
            .expect("filtered demand list");
        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].number, "DMND0002002");
    }

    #[tokio::test]
    async fn search_enriched_falls_back_to_live_fetch_for_exact_number() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/incident"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "inc-sys-fallback",
                    "number": "INC4992697",
                    "short_description": "Switch port flapping",
                    "description": "Multiple ports down on core switch",
                    "state": "2",
                    "assigned_to": {
                        "value": "user-sys",
                        "display_value": "Jared Jennings"
                    }
                }]
            })))
            .mount(&server)
            .await;

        // Journal inline mock — matches the enrich_record_journals call
        Mock::given(method("GET"))
            .and(path("/api/now/table/incident"))
            .and(query_param("sysparm_display_value", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "inc-sys-fallback",
                    "work_notes": "",
                    "comments": ""
                }]
            })))
            .mount(&server)
            .await;

        let client = ServiceNowClient::builder()
            .instance(server.uri())
            .auth(BasicAuth::new("test_user", "test_pass"))
            .allow_http()
            .build()
            .await
            .expect("client");

        let tempdir = TempDir::new().expect("tempdir");
        let core = SnowCore::builder()
            .client(client)
            .vault_path(tempdir.path().join("vault"))
            .build()
            .await
            .expect("core");

        // First search — index is cold, triggers live fallback
        let results = core
            .search_enriched("INC4992697", SearchScope::All)
            .await
            .expect("search");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].record.number, "INC4992697");
        assert_eq!(results[0].match_in, MatchField::Number);

        // Second search — now cached, no additional API call (mock expects exactly 1)
        let results2 = core
            .search_enriched("INC4992697", SearchScope::All)
            .await
            .expect("search cached");
        assert_eq!(results2.len(), 1);
        assert_eq!(results2[0].record.number, "INC4992697");
    }

    #[tokio::test]
    async fn search_enriched_case_insensitive_exact_number() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/incident"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "inc-sys-case",
                    "number": "INC0001234",
                    "short_description": "Case test",
                    "description": "",
                    "state": "1",
                    "assigned_to": ""
                }]
            })))
            .mount(&server)
            .await;

        // Journal inline mock — matches the enrich_record_journals call
        Mock::given(method("GET"))
            .and(path("/api/now/table/incident"))
            .and(query_param("sysparm_display_value", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "inc-sys-case",
                    "work_notes": "",
                    "comments": ""
                }]
            })))
            .mount(&server)
            .await;

        let client = ServiceNowClient::builder()
            .instance(server.uri())
            .auth(BasicAuth::new("test_user", "test_pass"))
            .allow_http()
            .build()
            .await
            .expect("client");

        let tempdir = TempDir::new().expect("tempdir");
        let core = SnowCore::builder()
            .client(client)
            .vault_path(tempdir.path().join("vault"))
            .build()
            .await
            .expect("core");

        let results = core
            .search_enriched("inc0001234", SearchScope::All)
            .await
            .expect("search lowercase");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].record.number, "INC0001234");
    }

    #[tokio::test]
    async fn search_enriched_does_not_fallback_for_freetext() {
        let server = MockServer::start().await;

        // No mocks — any API call would panic
        let client = ServiceNowClient::builder()
            .instance(server.uri())
            .auth(BasicAuth::new("test_user", "test_pass"))
            .allow_http()
            .build()
            .await
            .expect("client");

        let tempdir = TempDir::new().expect("tempdir");
        let core = SnowCore::builder()
            .client(client)
            .vault_path(tempdir.path().join("vault"))
            .build()
            .await
            .expect("core");

        let results = core
            .search_enriched("multiple ports down", SearchScope::All)
            .await
            .expect("freetext search");
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn get_record_fresh_includes_journal_entries() {
        let server = MockServer::start().await;

        // Base record fetch requests both raw sys_ids and display values.
        Mock::given(method("GET"))
            .and(path("/api/now/table/incident"))
            .and(query_param("sysparm_query", "number=INC0099001"))
            .and(query_param("sysparm_display_value", "all"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "inc-journal-sys",
                    "number": "INC0099001",
                    "short_description": "Switch port flapping",
                    "description": "Multiple ports down",
                    "state": { "value": "2", "display_value": "In Progress" },
                    "assigned_to": { "value": "user-sys", "display_value": "Jared Jennings" },
                    "assignment_group": { "value": "group-sys", "display_value": "Network Operations" },
                    "work_notes": "",
                    "comments": ""
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        // Journal inline fetch — returns formatted blob with display values
        Mock::given(method("GET"))
            .and(path("/api/now/table/incident"))
            .and(query_param("sysparm_query", "sys_id=inc-journal-sys"))
            .and(query_param("sysparm_display_value", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "inc-journal-sys",
                    "work_notes": "2026-04-10 09:15:00 - Jared Jennings (Work notes)\nCurrent status: Smart hand ticket has been created for the FS to get the switch details.\n\n",
                    "comments": ""
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let client = ServiceNowClient::builder()
            .instance(server.uri())
            .auth(BasicAuth::new("test_user", "test_pass"))
            .allow_http()
            .build()
            .await
            .expect("client");

        let tempdir = TempDir::new().expect("tempdir");
        let core = SnowCore::builder()
            .client(client)
            .vault_path(tempdir.path().join("vault"))
            .build()
            .await
            .expect("core");

        let record = core
            .get_record_fresh("INC0099001")
            .await
            .expect("fresh record")
            .expect("record present");

        assert_eq!(record.work_notes.len(), 1);
        assert!(record.work_notes[0].body.contains("Smart hand ticket"));
        assert_eq!(record.work_notes[0].author, "Jared Jennings");
        assert_eq!(
            record
                .fields
                .get("assigned_to")
                .and_then(|field| field.display_value.as_deref()),
            Some("Jared Jennings")
        );
        assert_eq!(
            record
                .fields
                .get("assignment_group")
                .and_then(|field| field.display_value.as_deref()),
            Some("Network Operations")
        );
    }

    #[tokio::test]
    async fn get_record_fresh_succeeds_when_journal_fetch_fails() {
        let server = MockServer::start().await;

        // Base record fetch succeeds
        Mock::given(method("GET"))
            .and(path("/api/now/table/incident"))
            .and(query_param("sysparm_query", "number=INC0099002"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "inc-nojournals-sys",
                    "number": "INC0099002",
                    "short_description": "No journals available",
                    "description": "",
                    "state": "1",
                    "assigned_to": ""
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        // Journal inline fetch fails (500)
        Mock::given(method("GET"))
            .and(path("/api/now/table/incident"))
            .and(query_param("sysparm_query", "sys_id=inc-nojournals-sys"))
            .respond_with(ResponseTemplate::new(500))
            .expect(1)
            .mount(&server)
            .await;

        let client = ServiceNowClient::builder()
            .instance(server.uri())
            .auth(BasicAuth::new("test_user", "test_pass"))
            .allow_http()
            .build()
            .await
            .expect("client");

        let tempdir = TempDir::new().expect("tempdir");
        let core = SnowCore::builder()
            .client(client)
            .vault_path(tempdir.path().join("vault"))
            .build()
            .await
            .expect("core");

        // Should succeed even though journal fetch failed
        let record = core
            .get_record_fresh("INC0099002")
            .await
            .expect("fresh record")
            .expect("record present");

        assert_eq!(record.number, "INC0099002");
        assert!(record.work_notes.is_empty());
    }

    #[tokio::test]
    async fn get_record_fresh_writes_journal_entries_to_vault() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/incident"))
            .and(query_param("sysparm_query", "number=INC0099003"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "inc-vault-journal-sys",
                    "number": "INC0099003",
                    "short_description": "Vault journal test",
                    "description": "Testing vault rendering",
                    "state": "2",
                    "assigned_to": "",
                    "work_notes": "",
                    "comments": ""
                }]
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/incident"))
            .and(query_param("sysparm_query", "sys_id=inc-vault-journal-sys"))
            .and(query_param("sysparm_display_value", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "inc-vault-journal-sys",
                    "work_notes": "2026-04-10 09:15:00 - Operator (Work notes)\nSmart hand ticket created.\n\n2026-04-10 08:00:00 - Dispatch (Work notes)\nAssigned to field services.\n",
                    "comments": ""
                }]
            })))
            .mount(&server)
            .await;

        let client = ServiceNowClient::builder()
            .instance(server.uri())
            .auth(BasicAuth::new("test_user", "test_pass"))
            .allow_http()
            .build()
            .await
            .expect("client");

        let tempdir = TempDir::new().expect("tempdir");
        let vault_path = tempdir.path().join("vault");
        let core = SnowCore::builder()
            .client(client)
            .vault_path(vault_path.clone())
            .build()
            .await
            .expect("core");

        let record = core
            .get_record_fresh("INC0099003")
            .await
            .expect("fresh record")
            .expect("record");

        // Verify the record has journal entries
        assert_eq!(record.work_notes.len(), 2);

        // Verify the vault file was written with journal content
        let vault_relative = core
            .vault_relative_path_for_sys_id("inc-vault-journal-sys")
            .expect("vault lookup")
            .expect("vault path present");
        let vault_file = vault_path.join(&vault_relative);
        let content = std::fs::read_to_string(&vault_file).expect("read vault file");
        assert!(
            content.contains("Smart hand ticket created."),
            "vault should contain first work note body"
        );
        assert!(
            content.contains("Assigned to field services."),
            "vault should contain second work note body"
        );
        // The Work Notes section should not be empty; find it and verify _(none)_ is absent from it
        let work_notes_section = content
            .split("## Work Notes")
            .nth(1)
            .expect("vault should have a Work Notes section");
        assert!(
            !work_notes_section
                .split("\n## ")
                .next()
                .unwrap_or("")
                .contains("_(none)_"),
            "vault Work Notes section should not show _(none)_"
        );
    }
}
