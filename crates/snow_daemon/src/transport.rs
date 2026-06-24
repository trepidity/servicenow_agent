use std::cell::RefCell;
use std::collections::HashMap;

use anyhow::Result;
use chrono::{DateTime, NaiveDate, Utc};
use serde::Serialize;
use snow_core::{
    ApprovalRecord, ApprovalRoutedVia, CacheSource, FieldValue, JournalEntry, KnowledgeArticle,
    KnowledgeBaseSummary, KnowledgeCategorySummary, KnowledgeEmbeddingCoverage, KnowledgeSearchHit,
    KnowledgeSearchMode, KnowledgeSemanticStatus, KnowledgeStatus, KnowledgeSyncMode,
    KnowledgeSyncOutcome, KnowledgeTagLayer, KnowledgeTagSummary, MatchField, RecordRef, Reference,
    ResourcePlanRecord, ResourceType, SearchMatchReason, SearchResult, SemanticIndexSummary,
    SnowCore, SnowRecord,
};

pub struct DaemonTransport<'a> {
    core: &'a SnowCore,
    instance_url: Option<String>,
    vault_relative_paths: RefCell<HashMap<String, Option<String>>>,
}

impl<'a> DaemonTransport<'a> {
    pub fn new(core: &'a SnowCore) -> Self {
        let instance_url = normalize_instance_url(&core.config().instance.url);
        Self {
            core,
            instance_url,
            vault_relative_paths: RefCell::new(HashMap::new()),
        }
    }

    pub fn record(&self, record: &SnowRecord) -> Result<DaemonSnowRecord> {
        Ok(DaemonSnowRecord {
            sys_id: record.sys_id.clone(),
            number: record.number.clone(),
            table: record.table.clone(),
            resource_type: record.resource_type.clone().into(),
            state: record.state.clone(),
            short_description: record.short_description.clone(),
            description: record.description.clone(),
            fields: record
                .fields
                .iter()
                .map(|(key, value)| (key.clone(), value.clone().into()))
                .collect(),
            work_notes: record.work_notes.iter().cloned().map(Into::into).collect(),
            comments: record.comments.iter().cloned().map(Into::into).collect(),
            parent: record.parent.as_ref().map(|parent| self.record_ref(parent)),
            children: record
                .children
                .iter()
                .map(|child| self.record_ref(child))
                .collect(),
            references: record
                .references
                .iter()
                .map(|(key, reference)| (key.clone(), reference.clone().into()))
                .collect(),
            synced_at: record.synced_at,
            source: record.source.clone().into(),
            browser_url: self.browser_url(&record.table, &record.sys_id),
            vault_relative_path: self.vault_relative_path(&record.sys_id)?,
        })
    }

    pub fn resource_plan_record(&self, record: &mut ResourcePlanRecord) -> Result<()> {
        record.browser_url = self.browser_url("resource_plan", &record.sys_id);
        record.vault_relative_path = self.vault_relative_path(&record.sys_id)?;
        Ok(())
    }

    pub fn knowledge_article(&self, article: &KnowledgeArticle) -> Result<DaemonKnowledgeArticle> {
        let record = self.record(&article.record)?;
        Ok(DaemonKnowledgeArticle {
            browser_url: record.browser_url.clone(),
            vault_relative_path: record.vault_relative_path.clone(),
            record,
            knowledge_base: article.knowledge_base.clone().into(),
            category: article.category.clone().into(),
            article_type: article.article_type.clone(),
            content: article.content.clone(),
            sn_tags: article.sn_tags.clone(),
            auto_tags: article.auto_tags.clone(),
            user_tags: article.user_tags.clone(),
            body_cached: article.body_cached,
            published_at: article.published_at,
            author: article.author.clone().map(Into::into),
            valid_to: article.valid_to,
        })
    }

    pub fn approval(&self, approval: &ApprovalRecord) -> Result<DaemonApprovalRecord> {
        let record = self.record(&approval.record)?;
        Ok(DaemonApprovalRecord {
            browser_url: record.browser_url.clone(),
            vault_relative_path: record.vault_relative_path.clone(),
            record,
            approver: approval.approver.clone().into(),
            target: self.record_ref(&approval.target),
            requested_at: approval.requested_at,
            due_date: approval.due_date,
            routed_via: approval.routed_via.clone(),
            approver_group: approval.approver_group.clone().map(Into::into),
        })
    }

    pub fn business_application(&self, record: &SnowRecord) -> Result<DaemonBusinessApplication> {
        let record_dto = self.record(record)?;
        Ok(DaemonBusinessApplication {
            browser_url: record_dto.browser_url.clone(),
            vault_relative_path: record_dto.vault_relative_path.clone(),
            name: business_application_name(record),
            business_owner: business_application_reference(record, "business_owner", "sys_user"),
            is_owner: business_application_reference(record, "it_application_owner", "sys_user"),
            ci_owner_group: business_application_reference(
                record,
                "managed_by_group",
                "sys_user_group",
            ),
            primary_support_group: business_application_reference(
                record,
                "support_group",
                "sys_user_group",
            ),
            operational_state: record
                .fields
                .get("operational_status")
                .cloned()
                .map(Into::into),
            primary_portfolio: business_application_reference(record, "portfolio", ""),
            attested_date: record
                .fields
                .get("attested_date")
                .map(|field| field.value.clone())
                .filter(|value| !value.trim().is_empty()),
            fields: record_dto.fields.clone(),
            unresolved_references: Vec::new(),
            record: record_dto,
        })
    }

    pub fn server(&self, record: &SnowRecord) -> Result<DaemonServer> {
        let record_dto = self.record(record)?;
        Ok(DaemonServer {
            browser_url: record_dto.browser_url.clone(),
            vault_relative_path: record_dto.vault_relative_path.clone(),
            record: record_dto.clone(),
            name: server_name(record),
            ip_address: server_field(record, "ip_address"),
            class_name: server_field(record, "sys_class_name"),
            ci_owner_group: server_reference(record, "managed_by_group", "sys_user_group"),
            support_group: server_reference(record, "support_group", "sys_user_group"),
            operational_status: record
                .fields
                .get("operational_status")
                .cloned()
                .map(Into::into),
            fields: record_dto.fields.clone(),
        })
    }

    pub fn knowledge_search_hit(
        &self,
        hit: &KnowledgeSearchHit,
    ) -> Result<DaemonKnowledgeSearchHit> {
        Ok(DaemonKnowledgeSearchHit {
            article: self.knowledge_article(&hit.article)?,
            mode: hit.mode.into(),
            score: hit.score,
            semantic_score: hit.semantic_score,
            lexical_score: hit.lexical_score,
            coverage: hit.coverage.into(),
        })
    }

    pub async fn search_result(&self, result: &SearchResult) -> Result<DaemonSearchResult> {
        let record = if let Some(full_record) = self.core.get_record(&result.record.number).await? {
            let transport_record = self.record(&full_record)?;
            DaemonSearchRecord {
                sys_id: transport_record.sys_id,
                number: transport_record.number,
                table: transport_record.table,
                resource_type: transport_record.resource_type,
                state: transport_record.state,
                short_description: transport_record.short_description,
                browser_url: transport_record.browser_url,
                vault_relative_path: transport_record.vault_relative_path,
            }
        } else {
            DaemonSearchRecord {
                sys_id: result.record.sys_id.clone(),
                number: result.record.number.clone(),
                table: result.record.table.clone(),
                resource_type: ResourceType::from_table(&result.record.table).into(),
                state: String::new(),
                short_description: String::new(),
                browser_url: self.browser_url(&result.record.table, &result.record.sys_id),
                vault_relative_path: self.vault_relative_path(&result.record.sys_id)?,
            }
        };

        Ok(DaemonSearchResult {
            record,
            snippet: result.snippet.clone(),
            score: result.score,
            match_in: result.match_in.into(),
            matched_value: result.matched_value.clone(),
            reasons: result.reasons.iter().cloned().map(Into::into).collect(),
        })
    }

    pub fn knowledge_base_summary(
        &self,
        summary: KnowledgeBaseSummary,
    ) -> DaemonKnowledgeBaseSummary {
        DaemonKnowledgeBaseSummary {
            sys_id: summary.sys_id,
            display_name: summary.display_name,
            article_count: summary.article_count,
        }
    }

    pub fn knowledge_category_summary(
        &self,
        summary: KnowledgeCategorySummary,
    ) -> DaemonKnowledgeCategorySummary {
        DaemonKnowledgeCategorySummary {
            sys_id: summary.sys_id,
            knowledge_base_sys_id: summary.knowledge_base_sys_id,
            display_name: summary.display_name,
            article_count: summary.article_count,
        }
    }

    pub fn vault_path(&self) -> String {
        self.core.vault_path().to_string_lossy().into_owned()
    }

    fn record_ref(&self, reference: &RecordRef) -> DaemonRecordRef {
        DaemonRecordRef {
            sys_id: reference.sys_id.clone(),
            number: reference.number.clone(),
            table: reference.table.clone(),
        }
    }

    fn browser_url(&self, table: &str, sys_id: &str) -> Option<String> {
        self.instance_url
            .as_ref()
            .map(|base| format!("{base}/nav_to.do?uri={table}.do?sys_id={sys_id}"))
    }

    fn vault_relative_path(&self, sys_id: &str) -> Result<Option<String>> {
        if let Some(path) = self.vault_relative_paths.borrow().get(sys_id).cloned() {
            return Ok(path);
        }

        let path = self.core.vault_relative_path_for_sys_id(sys_id)?;
        self.vault_relative_paths
            .borrow_mut()
            .insert(sys_id.to_string(), path.clone());
        Ok(path)
    }
}

fn business_application_name(record: &SnowRecord) -> String {
    record
        .fields
        .get("name")
        .map(|field| field.display_value.as_ref().unwrap_or(&field.value).clone())
        .filter(|value| !value.trim().is_empty())
        .or_else(|| {
            (!record.short_description.trim().is_empty()).then(|| record.short_description.clone())
        })
        .unwrap_or_else(|| record.sys_id.clone())
}

fn business_application_reference(
    record: &SnowRecord,
    field_name: &str,
    default_table: &str,
) -> Option<DaemonReference> {
    let field = record.fields.get(field_name)?;
    let sys_id = field.value.trim();
    if sys_id.is_empty() {
        return None;
    }
    Some(DaemonReference {
        sys_id: sys_id.to_string(),
        table: default_table.to_string(),
        display_name: field
            .display_value
            .clone()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| sys_id.to_string()),
        extra: HashMap::new(),
    })
}

fn server_name(record: &SnowRecord) -> String {
    server_field(record, "name")
        .or_else(|| {
            (!record.short_description.trim().is_empty()).then(|| record.short_description.clone())
        })
        .unwrap_or_else(|| record.sys_id.clone())
}

fn server_field(record: &SnowRecord, field_name: &str) -> Option<String> {
    record.fields.get(field_name).and_then(|field| {
        field
            .display_value
            .clone()
            .or_else(|| Some(field.value.clone()))
            .filter(|value| !value.trim().is_empty())
    })
}

fn server_reference(
    record: &SnowRecord,
    field_name: &str,
    default_table: &str,
) -> Option<DaemonReference> {
    record
        .references
        .get(field_name)
        .map(|reference| reference.clone().into())
        .or_else(|| business_application_reference(record, field_name, default_table))
}

fn normalize_instance_url(instance_url: &str) -> Option<String> {
    let trimmed = instance_url.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        Some(trimmed.to_string())
    } else {
        Some(format!("https://{trimmed}"))
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DaemonSnowRecord {
    pub sys_id: String,
    pub number: String,
    pub table: String,
    pub resource_type: DaemonResourceType,
    pub state: String,
    pub short_description: String,
    pub description: String,
    pub fields: HashMap<String, DaemonFieldValue>,
    pub work_notes: Vec<DaemonJournalEntry>,
    pub comments: Vec<DaemonJournalEntry>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent: Option<DaemonRecordRef>,
    pub children: Vec<DaemonRecordRef>,
    pub references: HashMap<String, DaemonReference>,
    pub synced_at: DateTime<Utc>,
    pub source: DaemonCacheSource,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub browser_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vault_relative_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DaemonKnowledgeArticle {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub browser_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vault_relative_path: Option<String>,
    pub record: DaemonSnowRecord,
    pub knowledge_base: DaemonReference,
    pub category: DaemonReference,
    pub article_type: String,
    pub content: String,
    pub sn_tags: Vec<String>,
    pub auto_tags: Vec<String>,
    pub user_tags: Vec<String>,
    pub body_cached: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub published_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub author: Option<DaemonReference>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub valid_to: Option<NaiveDate>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DaemonApprovalRecord {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub browser_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vault_relative_path: Option<String>,
    pub record: DaemonSnowRecord,
    pub approver: DaemonReference,
    pub target: DaemonRecordRef,
    pub requested_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub due_date: Option<DateTime<Utc>>,
    pub routed_via: ApprovalRoutedVia,
    pub approver_group: Option<DaemonReference>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DaemonBusinessApplication {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub browser_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vault_relative_path: Option<String>,
    pub record: DaemonSnowRecord,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub business_owner: Option<DaemonReference>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_owner: Option<DaemonReference>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ci_owner_group: Option<DaemonReference>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_support_group: Option<DaemonReference>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operational_state: Option<DaemonFieldValue>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub primary_portfolio: Option<DaemonReference>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub attested_date: Option<String>,
    pub fields: HashMap<String, DaemonFieldValue>,
    pub unresolved_references: Vec<DaemonBusinessApplicationDiagnostic>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DaemonBusinessApplicationDiagnostic {
    pub field: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sys_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    pub diagnostic: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DaemonServer {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub browser_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vault_relative_path: Option<String>,
    pub record: DaemonSnowRecord,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ip_address: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub class_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ci_owner_group: Option<DaemonReference>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub support_group: Option<DaemonReference>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operational_status: Option<DaemonFieldValue>,
    pub fields: HashMap<String, DaemonFieldValue>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DaemonSearchResult {
    pub record: DaemonSearchRecord,
    pub snippet: String,
    pub score: i64,
    pub match_in: DaemonMatchField,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub matched_value: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub reasons: Vec<DaemonSearchMatchReason>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DaemonSearchRecord {
    pub sys_id: String,
    pub number: String,
    pub table: String,
    pub resource_type: DaemonResourceType,
    pub state: String,
    pub short_description: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub browser_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vault_relative_path: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DaemonKnowledgeBaseSummary {
    pub sys_id: String,
    pub display_name: String,
    pub article_count: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DaemonKnowledgeCategorySummary {
    pub sys_id: String,
    pub knowledge_base_sys_id: String,
    pub display_name: String,
    pub article_count: usize,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DaemonKnowledgeSyncMode {
    Full,
    Incremental,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
pub enum DaemonKnowledgeTagLayer {
    Sn,
    Auto,
    User,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DaemonKnowledgeTagSummary {
    pub tag: String,
    pub count: usize,
    pub layers: Vec<DaemonKnowledgeTagLayer>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DaemonKnowledgeStatus {
    pub article_count: usize,
    pub body_cached_count: usize,
    pub knowledge_base_count: usize,
    pub category_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_full_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_incremental_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub watermark_updated_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub watermark_sys_id: Option<String>,
    pub lock_held: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lock_timestamp_ms: Option<i64>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DaemonKnowledgeSyncOutcome {
    pub accepted: bool,
    pub mode: DaemonKnowledgeSyncMode,
    pub with_bodies: bool,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub details: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct DaemonKnowledgeSearchHit {
    pub article: DaemonKnowledgeArticle,
    pub mode: DaemonKnowledgeSearchMode,
    pub score: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub semantic_score: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lexical_score: Option<f32>,
    pub coverage: DaemonKnowledgeEmbeddingCoverage,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DaemonKnowledgeSearchMode {
    Lexical,
    Semantic,
    Hybrid,
}

#[derive(Debug, Clone, Copy, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DaemonKnowledgeEmbeddingCoverage {
    Metadata,
    FullText,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DaemonKnowledgeSemanticStatus {
    pub enabled: bool,
    pub provider: String,
    pub model: String,
    pub dimensions: usize,
    pub active_kb_articles: usize,
    pub metadata_embeddings: usize,
    pub full_text_embeddings: usize,
    pub stale_rows: usize,
    pub orphan_rows: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_rebuild_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DaemonSemanticIndexSummary {
    pub full: bool,
    pub indexed_rows: usize,
    pub metadata_embeddings: usize,
    pub full_text_embeddings: usize,
    pub stale_rows: usize,
    pub orphan_rows: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_rebuild_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DaemonFieldValue {
    pub value: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub display_value: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DaemonJournalEntry {
    pub timestamp: DateTime<Utc>,
    pub author: String,
    pub body: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DaemonReference {
    pub sys_id: String,
    pub table: String,
    pub display_name: String,
    pub extra: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DaemonRecordRef {
    pub sys_id: String,
    pub number: String,
    pub table: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct DaemonSearchMatchReason {
    pub field: DaemonMatchField,
    pub value: String,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DaemonResourceType {
    Task,
    Incident,
    Change,
    ChangeTask,
    Request,
    RequestTask,
    Project,
    Demand,
    DemandTask,
    ResourcePlan,
    Story,
    ScrumTask,
    Timecard,
    Knowledge,
    Approval,
    BusinessApplication,
    Server,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DaemonMatchField {
    Number,
    ShortDescription,
    Description,
    WorkNotes,
    Content,
    Tag,
    Keyword,
    Alias,
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DaemonCacheSource {
    Memory,
    Disk,
    Api,
}

impl From<FieldValue> for DaemonFieldValue {
    fn from(value: FieldValue) -> Self {
        Self {
            value: value.value,
            display_value: value.display_value,
        }
    }
}

impl From<JournalEntry> for DaemonJournalEntry {
    fn from(value: JournalEntry) -> Self {
        Self {
            timestamp: value.timestamp,
            author: value.author,
            body: value.body,
        }
    }
}

impl From<Reference> for DaemonReference {
    fn from(value: Reference) -> Self {
        Self {
            sys_id: value.sys_id,
            table: value.table,
            display_name: value.display_name,
            extra: value.extra,
        }
    }
}

impl From<SearchMatchReason> for DaemonSearchMatchReason {
    fn from(value: SearchMatchReason) -> Self {
        Self {
            field: value.field.into(),
            value: value.value,
        }
    }
}

impl From<KnowledgeSyncMode> for DaemonKnowledgeSyncMode {
    fn from(value: KnowledgeSyncMode) -> Self {
        match value {
            KnowledgeSyncMode::Full => Self::Full,
            KnowledgeSyncMode::Incremental => Self::Incremental,
        }
    }
}

impl From<KnowledgeTagLayer> for DaemonKnowledgeTagLayer {
    fn from(value: KnowledgeTagLayer) -> Self {
        match value {
            KnowledgeTagLayer::Sn => Self::Sn,
            KnowledgeTagLayer::Auto => Self::Auto,
            KnowledgeTagLayer::User => Self::User,
        }
    }
}

impl From<DaemonKnowledgeTagLayer> for KnowledgeTagLayer {
    fn from(value: DaemonKnowledgeTagLayer) -> Self {
        match value {
            DaemonKnowledgeTagLayer::Sn => Self::Sn,
            DaemonKnowledgeTagLayer::Auto => Self::Auto,
            DaemonKnowledgeTagLayer::User => Self::User,
        }
    }
}

impl From<KnowledgeTagSummary> for DaemonKnowledgeTagSummary {
    fn from(value: KnowledgeTagSummary) -> Self {
        Self {
            tag: value.tag,
            count: value.count,
            layers: value.layers.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<KnowledgeStatus> for DaemonKnowledgeStatus {
    fn from(value: KnowledgeStatus) -> Self {
        Self {
            article_count: value.article_count,
            body_cached_count: value.body_cached_count,
            knowledge_base_count: value.knowledge_base_count,
            category_count: value.category_count,
            last_full_at: value.last_full_at,
            last_incremental_at: value.last_incremental_at,
            watermark_updated_at: value.watermark_updated_at,
            watermark_sys_id: value.watermark_sys_id,
            lock_held: value.lock_held,
            lock_timestamp_ms: value.lock_timestamp_ms,
        }
    }
}

impl From<KnowledgeSyncOutcome> for DaemonKnowledgeSyncOutcome {
    fn from(value: KnowledgeSyncOutcome) -> Self {
        Self {
            accepted: value.accepted,
            mode: value.mode.into(),
            with_bodies: value.with_bodies,
            status: value.status,
            details: value.details,
        }
    }
}

impl From<KnowledgeSearchMode> for DaemonKnowledgeSearchMode {
    fn from(value: KnowledgeSearchMode) -> Self {
        match value {
            KnowledgeSearchMode::Lexical => Self::Lexical,
            KnowledgeSearchMode::Semantic => Self::Semantic,
            KnowledgeSearchMode::Hybrid => Self::Hybrid,
        }
    }
}

impl From<KnowledgeEmbeddingCoverage> for DaemonKnowledgeEmbeddingCoverage {
    fn from(value: KnowledgeEmbeddingCoverage) -> Self {
        match value {
            KnowledgeEmbeddingCoverage::Metadata => Self::Metadata,
            KnowledgeEmbeddingCoverage::FullText => Self::FullText,
        }
    }
}

impl From<KnowledgeSemanticStatus> for DaemonKnowledgeSemanticStatus {
    fn from(value: KnowledgeSemanticStatus) -> Self {
        Self {
            enabled: value.enabled,
            provider: value.provider,
            model: value.model,
            dimensions: value.dimensions,
            active_kb_articles: value.active_kb_articles,
            metadata_embeddings: value.metadata_embeddings,
            full_text_embeddings: value.full_text_embeddings,
            stale_rows: value.stale_rows,
            orphan_rows: value.orphan_rows,
            last_rebuild_at: value.last_rebuild_at,
            last_error: value.last_error,
        }
    }
}

impl From<SemanticIndexSummary> for DaemonSemanticIndexSummary {
    fn from(value: SemanticIndexSummary) -> Self {
        Self {
            full: value.full,
            indexed_rows: value.indexed_rows,
            metadata_embeddings: value.metadata_embeddings,
            full_text_embeddings: value.full_text_embeddings,
            stale_rows: value.stale_rows,
            orphan_rows: value.orphan_rows,
            last_rebuild_at: value.last_rebuild_at,
            last_error: value.last_error,
        }
    }
}

impl From<ResourceType> for DaemonResourceType {
    fn from(value: ResourceType) -> Self {
        match value {
            ResourceType::Task => Self::Task,
            ResourceType::Incident => Self::Incident,
            ResourceType::Change => Self::Change,
            ResourceType::ChangeTask => Self::ChangeTask,
            ResourceType::Request => Self::Request,
            ResourceType::RequestTask => Self::RequestTask,
            ResourceType::Project => Self::Project,
            ResourceType::Demand => Self::Demand,
            ResourceType::DemandTask => Self::DemandTask,
            ResourceType::ResourcePlan => Self::ResourcePlan,
            ResourceType::Story => Self::Story,
            ResourceType::ScrumTask => Self::ScrumTask,
            ResourceType::Timecard => Self::Timecard,
            ResourceType::Knowledge => Self::Knowledge,
            ResourceType::Approval => Self::Approval,
            ResourceType::BusinessApplication => Self::BusinessApplication,
            ResourceType::Server => Self::Server,
        }
    }
}

impl From<MatchField> for DaemonMatchField {
    fn from(value: MatchField) -> Self {
        match value {
            MatchField::Number => Self::Number,
            MatchField::ShortDescription => Self::ShortDescription,
            MatchField::Description => Self::Description,
            MatchField::WorkNotes => Self::WorkNotes,
            MatchField::Content => Self::Content,
            MatchField::Tag => Self::Tag,
            MatchField::Keyword => Self::Keyword,
            MatchField::Alias => Self::Alias,
        }
    }
}

impl From<CacheSource> for DaemonCacheSource {
    fn from(value: CacheSource) -> Self {
        match value {
            CacheSource::Memory => Self::Memory,
            CacheSource::Disk => Self::Disk,
            CacheSource::Api => Self::Api,
        }
    }
}
