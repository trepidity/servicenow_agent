use crate::{CatalogItem, KnowledgeEmbeddingCoverage, ResourceType};
use chrono::{DateTime, Utc};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CacheFormat {
    Absent,
    Current,
    Incompatible { found: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogProductProjectionRow {
    pub item: CatalogItem,
    pub last_refreshed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordLifecycle {
    Active,
    Tombstoned,
    Pruned,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VaultProjectionProvenance {
    VaultBacked,
    CacheOnly,
    LegacyUnknown,
}

impl VaultProjectionProvenance {
    pub(crate) const fn as_str(self) -> &'static str {
        match self {
            Self::VaultBacked => "vault_backed",
            Self::CacheOnly => "cache_only",
            Self::LegacyUnknown => "legacy_unknown",
        }
    }

    pub(crate) fn parse(value: &str) -> Option<Self> {
        match value {
            "vault_backed" => Some(Self::VaultBacked),
            "cache_only" => Some(Self::CacheOnly),
            "legacy_unknown" => Some(Self::LegacyUnknown),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordRow {
    pub sys_id: String,
    pub number: String,
    pub table_name: String,
    pub resource_type: ResourceType,
    pub state: Option<String>,
    pub short_desc: Option<String>,
    pub description: Option<String>,
    pub assigned_to: Option<String>,
    pub parent_id: Option<String>,
    pub file_path: Option<String>,
    pub synced_at: DateTime<Utc>,
    pub sys_updated_on: DateTime<Utc>,
    pub etag: Option<String>,
    pub in_scope: bool,
    pub last_seen_at: DateTime<Utc>,
    pub tombstoned_at: Option<DateTime<Utc>>,
    pub pruned_at: Option<DateTime<Utc>>,
    pub raw_json: String,
}

impl RecordRow {
    pub fn lifecycle(&self) -> RecordLifecycle {
        if self.pruned_at.is_some() {
            RecordLifecycle::Pruned
        } else if self.tombstoned_at.is_some() || !self.in_scope {
            RecordLifecycle::Tombstoned
        } else {
            RecordLifecycle::Active
        }
    }

    pub fn active(
        sys_id: impl Into<String>,
        number: impl Into<String>,
        table_name: impl Into<String>,
        resource_type: ResourceType,
        sys_updated_on: DateTime<Utc>,
    ) -> Self {
        let now = Utc::now();
        Self {
            sys_id: sys_id.into(),
            number: number.into(),
            table_name: table_name.into(),
            resource_type,
            state: None,
            short_desc: None,
            description: None,
            assigned_to: None,
            parent_id: None,
            file_path: None,
            synced_at: now,
            sys_updated_on,
            etag: None,
            in_scope: true,
            last_seen_at: now,
            tombstoned_at: None,
            pruned_at: None,
            raw_json: "{}".to_string(),
        }
    }

    pub fn tombstone(mut self, when: DateTime<Utc>) -> Self {
        self.in_scope = false;
        self.tombstoned_at = Some(when);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReferenceRow {
    pub sys_id: String,
    pub table_name: String,
    pub display_name: String,
    pub extra_json: String,
    pub synced_at: DateTime<Utc>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipRow {
    pub source_id: String,
    pub target_id: String,
    pub rel_type: String,
    pub field_name: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordReferenceRow {
    pub source_id: String,
    pub field_name: String,
    pub reference: ReferenceRow,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SyncStateRow {
    pub resource_type: String,
    pub last_full: Option<DateTime<Utc>>,
    pub last_incr: Option<DateTime<Utc>>,
    pub high_watermark: Option<DateTime<Utc>>,
    pub cursor: Option<String>,
    pub filter_hash: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TagRow {
    pub record_sys_id: String,
    pub tag: String,
    pub source: String,
    pub weight: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KeywordRow {
    pub record_sys_id: String,
    pub keyword: String,
    pub source: String,
    pub weight: f64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AliasRow {
    pub record_sys_id: String,
    pub alias: String,
    pub kind: String,
    pub source: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeArticleRow {
    pub record_sys_id: String,
    pub number: String,
    pub title: String,
    pub workflow_state: String,
    pub knowledge_base_sys_id: String,
    pub knowledge_base_name: String,
    pub category_sys_id: String,
    pub category_name: String,
    pub author_sys_id: Option<String>,
    pub author_name: Option<String>,
    pub published_at: Option<String>,
    pub valid_to: Option<String>,
    pub article_type: String,
    pub sys_updated_on: Option<String>,
    pub sn_tags: Vec<String>,
    pub auto_tags: Vec<String>,
    pub user_tags: Vec<String>,
    pub body_cached: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeIndexRow {
    pub record_sys_id: String,
    pub number: String,
    pub title: String,
    pub knowledge_base_sys_id: String,
    pub knowledge_base_name: String,
    pub category_sys_id: String,
    pub category_name: String,
    pub file_path: String,
    pub sn_tags: Vec<String>,
    pub auto_tags: Vec<String>,
    pub user_tags: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeLocalScanRow {
    pub record_sys_id: String,
    pub number: String,
    pub file_path: String,
    pub modified_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeBaseSummaryRow {
    pub knowledge_base_sys_id: String,
    pub knowledge_base_name: String,
    pub article_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeCategorySummaryRow {
    pub category_sys_id: String,
    pub category_name: String,
    pub knowledge_base_sys_id: String,
    pub article_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KbSyncStateRow {
    pub last_full_at: Option<DateTime<Utc>>,
    pub last_incr_at: Option<DateTime<Utc>>,
    pub watermark_updated_at: Option<String>,
    pub watermark_sys_id: Option<String>,
    pub kb_sync_lock: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeTagCountRow {
    pub tag: String,
    pub layer: String,
    pub article_count: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct KnowledgeEmbeddingRow {
    pub record_sys_id: String,
    pub model: String,
    pub provider: String,
    pub dimensions: usize,
    pub coverage: KnowledgeEmbeddingCoverage,
    pub content_hash: String,
    pub vector: Vec<f32>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KnowledgeSemanticMeta {
    pub last_rebuild_at: Option<DateTime<Utc>>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BusinessApplicationProjectionRow {
    pub record_sys_id: String,
    pub name: String,
    pub number: Option<String>,
    pub business_owner_sys_id: Option<String>,
    pub business_owner_name: Option<String>,
    pub is_owner_sys_id: Option<String>,
    pub is_owner_name: Option<String>,
    pub ci_owner_group_sys_id: Option<String>,
    pub ci_owner_group_name: Option<String>,
    pub primary_support_group_sys_id: Option<String>,
    pub primary_support_group_name: Option<String>,
    pub operational_status_value: Option<String>,
    pub operational_status_display: Option<String>,
    pub primary_portfolio_sys_id: Option<String>,
    pub primary_portfolio_name: Option<String>,
    pub primary_portfolio_table: Option<String>,
    pub attested_date: Option<String>,
    pub sys_updated_on: Option<String>,
    pub field_count: usize,
    pub reference_count: usize,
    pub unresolved_reference_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BusinessApplicationServerMembershipRow {
    pub ba_sys_id: String,
    pub server_sys_id: String,
    pub server_table: String,
    pub provenance: String,
    pub min_depth: usize,
    pub paths_json: String,
    pub discovered_at: DateTime<Utc>,
    pub last_seen_at: DateTime<Utc>,
    pub tombstoned_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BusinessApplicationServerInventoryHealthRow {
    pub ba_sys_id: String,
    pub run_started_at: DateTime<Utc>,
    pub run_completed_at: DateTime<Utc>,
    pub service_membership_status: String,
    pub relationship_status: String,
    pub inventory_status: String,
    pub summary_json: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectedFieldRow {
    pub owner_sys_id: String,
    pub field_name: String,
    pub field_label: Option<String>,
    pub field_type: Option<String>,
    pub value_text: Option<String>,
    pub display_value: Option<String>,
    pub value_number: Option<f64>,
    pub value_date: Option<String>,
    pub value_bool: Option<bool>,
    pub reference_sys_id: Option<String>,
    pub reference_table: Option<String>,
    pub raw_json: String,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BusinessApplicationFieldDictionaryRow {
    pub table_name: String,
    pub field_name: String,
    pub field_label: Option<String>,
    pub field_type: Option<String>,
    pub reference_table: Option<String>,
    pub choice: bool,
    pub mandatory: bool,
    pub read_only: bool,
    pub max_length: Option<i64>,
    pub active: bool,
    pub synced_at: DateTime<Utc>,
    pub raw_json: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PrimitiveResolutionStatus {
    Resolved,
    Unresolved,
    UnknownTable,
    NotFound,
    AclRestricted,
    Error,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrimitiveObjectRow {
    pub sys_id: String,
    pub table_name: String,
    pub resource_type: String,
    pub display_name: String,
    pub number: Option<String>,
    pub file_path: Option<String>,
    pub raw_json: String,
    pub synced_at: DateTime<Utc>,
    pub sys_updated_on: Option<String>,
    pub resolution_status: PrimitiveResolutionStatus,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedUserRow {
    pub sys_id: String,
    pub user_name: Option<String>,
    pub name: Option<String>,
    pub first_name: Option<String>,
    pub last_name: Option<String>,
    pub email: Option<String>,
    pub employee_number: Option<String>,
    pub active: Option<bool>,
    pub department: Option<String>,
    pub location: Option<String>,
    pub title: Option<String>,
    pub raw_json: String,
    pub synced_at: DateTime<Utc>,
    pub sys_updated_on: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CachedUserQueryRow {
    pub query_key: String,
    pub result_sys_ids: Vec<String>,
    pub synced_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
}
