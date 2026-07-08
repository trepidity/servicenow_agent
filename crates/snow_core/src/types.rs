//! Standalone data-type definitions extracted from `lib.rs`.
//!
//! These are the plain `pub struct` / `pub enum` record and domain types
//! (plus their directly-associated inherent `impl` blocks) that previously
//! lived inline in `lib.rs`. This is a pure relocation done as part of the
//! library boundary migration: no field, derive, or behavior changes.
//! Everything here remains reachable at its existing `snow_core::*` path via
//! the `pub use types::*;` re-export in `lib.rs`.
//!
//! A few of the domain types below (the `Knowledge*` types) are noted in the
//! migration plan as relocating again later to their eventual `service::*`
//! module homes; they land here first as an interim step, so only their type
//! definitions moved in this pass. `BusinessApplicationSearchParams` was one
//! such type — it has since relocated to `service::business_application`
//! (Task 10); see the re-export in `lib.rs`.

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use servicenow_rs::prelude::Record;
use std::collections::HashMap;
use std::path::PathBuf;

use crate::vault::VaultScanFailure;
use crate::{
    BusinessApplicationFieldAliases, Reference, ResourceType, bool_is_false,
    canonical_record_table_for_number, resource,
};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordLookup {
    Number(String),
    TableSysId { table: String, sys_id: String },
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

impl SnowRecord {
    pub fn from_servicenow(record: &Record) -> Self {
        let raw_number = record.get_str("number").unwrap_or_default().to_string();
        let is_business_application =
            resource::business_application::is_business_application_alias(&record.table);
        let is_server = resource::server::is_server_table(&record.table);
        let number = if is_business_application {
            resource::business_application::business_application_number(record)
        } else if is_server {
            resource::server::server_number(record)
        } else {
            raw_number
        };
        let table = canonical_record_table_for_number(&record.table, &number);
        let business_application_aliases = BusinessApplicationFieldAliases::baseline_degraded();
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
            state: if is_business_application {
                resource::business_application::business_application_state(
                    record,
                    &business_application_aliases,
                )
            } else if is_server {
                resource::server::server_state(record)
            } else {
                record.get_str("state").unwrap_or_default().to_string()
            },
            short_description: if is_business_application {
                resource::business_application::business_application_display_name(record)
            } else if is_server {
                resource::server::server_display_name(record)
            } else {
                record
                    .get_str("short_description")
                    .unwrap_or_default()
                    .to_string()
            },
            description: if is_business_application {
                resource::business_application::business_application_description(record)
            } else if is_server {
                resource::server::server_description(record)
            } else {
                record
                    .get_str("description")
                    .unwrap_or_default()
                    .to_string()
            },
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::normalize_record_lookup_table;
    use servicenow_rs::prelude::DisplayValue;

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
    fn business_application_resource_type_accepts_aliases() {
        assert_eq!(
            ResourceType::from_table("business_application"),
            ResourceType::BusinessApplication
        );
        assert_eq!(
            ResourceType::from_table("business-app"),
            ResourceType::BusinessApplication
        );
        assert_eq!(
            normalize_record_lookup_table("cmdb_ci_business_app").unwrap(),
            "cmdb_ci_business_app"
        );
        assert_eq!(
            normalize_record_lookup_table("business_application").unwrap(),
            "cmdb_ci_business_app"
        );
    }

    #[test]
    fn server_resource_type_accepts_aliases() {
        assert_eq!(ResourceType::from_table("server"), ResourceType::Server);
        assert_eq!(
            ResourceType::from_table("cmdb_ci_linux_server"),
            ResourceType::Server
        );
        assert_eq!(
            ResourceType::from_table("cmdb_ci_win_server"),
            ResourceType::Server
        );
        assert_eq!(
            normalize_record_lookup_table("server").unwrap(),
            "cmdb_ci_server"
        );
        assert_eq!(
            normalize_record_lookup_table("linux_server").unwrap(),
            "cmdb_ci_linux_server"
        );
        assert_eq!(
            normalize_record_lookup_table("windows_server").unwrap(),
            "cmdb_ci_win_server"
        );
    }
}
