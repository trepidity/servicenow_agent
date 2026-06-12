use super::policy::stable_reference_ttl;
use crate::{FieldValue, KnowledgeEmbeddingCoverage, ResourceType, SnowRecord};
use chrono::{DateTime, Duration, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use serde_json::{Map, Value};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::path::{Path, PathBuf};
use thiserror::Error;

pub const SCHEMA_VERSION: i64 = 11;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("sqlite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("invalid resource type: {0}")]
    InvalidResourceType(String),
    #[error("invalid schema version: {0}")]
    InvalidSchemaVersion(String),
    #[error("invalid query: {0}")]
    InvalidQuery(String),
    #[error("invalid timestamp: {0}")]
    InvalidTimestamp(i64),
    #[error("serde json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid embedding vector length: expected {expected} bytes, got {actual}")]
    InvalidEmbeddingVectorLength { expected: usize, actual: usize },
    #[error("embedding vector was not unit-length")]
    NonUnitEmbeddingVector,
}

pub type Result<T> = std::result::Result<T, StoreError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordLifecycle {
    Active,
    Tombstoned,
    Pruned,
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

pub struct Store {
    path: PathBuf,
    conn: Connection,
}

impl fmt::Debug for Store {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Store").field("path", &self.path).finish()
    }
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let conn = if path == Path::new(":memory:") {
            Connection::open_in_memory()?
        } else {
            Connection::open(path)?
        };

        let store = Self {
            path: path.to_path_buf(),
            conn,
        };
        store.bootstrap()?;
        Ok(store)
    }

    pub fn open_in_memory() -> Result<Self> {
        Self::open(":memory:")
    }

    pub fn bootstrap(&self) -> Result<()> {
        self.conn.pragma_update(None, "foreign_keys", "ON")?;
        self.conn.pragma_update(None, "journal_mode", "WAL")?;
        self.conn.pragma_update(None, "synchronous", "NORMAL")?;
        self.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS schema_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            "#,
        )?;
        let schema_version = self.schema_version()?.unwrap_or(0);
        self.create_schema_objects()?;
        if self.needs_v3_migration(schema_version)? {
            self.migrate_to_v3()?;
        }
        if self.needs_v5_migration(schema_version)? {
            self.migrate_to_v5()?;
        }
        if self.needs_v6_migration(schema_version)? {
            self.migrate_to_v6()?;
        }
        if self.needs_v7_migration(schema_version)? {
            self.migrate_to_v7()?;
        }
        if self.needs_v8_migration(schema_version)? {
            self.migrate_to_v8()?;
        }
        if self.needs_v9_migration(schema_version)? {
            self.migrate_to_v9()?;
        }
        if self.needs_v10_migration(schema_version)? {
            self.migrate_to_v10()?;
        }
        if self.needs_v11_migration(schema_version)? {
            self.migrate_to_v11()?;
        }
        self.create_post_migration_indexes()?;
        self.set_schema_version(SCHEMA_VERSION)?;
        Ok(())
    }

    fn create_schema_objects(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS records (
                sys_id TEXT PRIMARY KEY,
                number TEXT NOT NULL,
                table_name TEXT NOT NULL,
                resource_type TEXT NOT NULL,
                state TEXT,
                short_desc TEXT,
                description TEXT,
                assigned_to TEXT,
                parent_id TEXT,
                file_path TEXT,
                synced_at INTEGER NOT NULL,
                sys_updated_on INTEGER NOT NULL,
                etag TEXT,
                in_scope INTEGER NOT NULL DEFAULT 1 CHECK (in_scope IN (0, 1)),
                last_seen_at INTEGER NOT NULL,
                tombstoned_at INTEGER,
                pruned_at INTEGER,
                raw_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS "references" (
                sys_id TEXT PRIMARY KEY,
                table_name TEXT NOT NULL,
                display_name TEXT NOT NULL,
                extra_json TEXT NOT NULL DEFAULT '{}',
                synced_at INTEGER NOT NULL,
                expires_at INTEGER
            );

            CREATE TABLE IF NOT EXISTS relationships (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_id TEXT NOT NULL,
                target_id TEXT NOT NULL,
                rel_type TEXT NOT NULL,
                field_name TEXT NOT NULL,
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                UNIQUE(source_id, target_id, rel_type, field_name)
            );

            CREATE TABLE IF NOT EXISTS sync_state (
                resource_type TEXT PRIMARY KEY,
                last_full INTEGER,
                last_incr INTEGER,
                high_watermark INTEGER,
                cursor TEXT,
                filter_hash TEXT,
                updated_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now'))
            );

            CREATE TABLE IF NOT EXISTS record_tags (
                record_sys_id TEXT NOT NULL,
                tag TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT 'derived',
                weight REAL NOT NULL DEFAULT 1.0,
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                UNIQUE(record_sys_id, tag)
            );

            CREATE TABLE IF NOT EXISTS record_keywords (
                record_sys_id TEXT NOT NULL,
                keyword TEXT NOT NULL,
                source TEXT NOT NULL DEFAULT 'derived',
                weight REAL NOT NULL DEFAULT 1.0,
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                UNIQUE(record_sys_id, keyword)
            );

            CREATE TABLE IF NOT EXISTS record_aliases (
                record_sys_id TEXT NOT NULL,
                alias TEXT NOT NULL,
                kind TEXT NOT NULL DEFAULT 'derived',
                source TEXT NOT NULL DEFAULT 'derived',
                created_at INTEGER NOT NULL DEFAULT (strftime('%s', 'now')),
                UNIQUE(record_sys_id, alias)
            );

            CREATE TABLE IF NOT EXISTS knowledge_articles (
                record_sys_id TEXT PRIMARY KEY,
                number TEXT NOT NULL,
                title TEXT NOT NULL,
                workflow_state TEXT NOT NULL,
                knowledge_base_sys_id TEXT NOT NULL,
                knowledge_base_name TEXT NOT NULL,
                category_sys_id TEXT NOT NULL,
                category_name TEXT NOT NULL,
                author_sys_id TEXT,
                author_name TEXT,
                published_at TEXT,
                valid_to TEXT,
                article_type TEXT NOT NULL,
                sys_updated_on TEXT,
                sn_tags TEXT NOT NULL DEFAULT '[]',
                auto_tags TEXT NOT NULL DEFAULT '[]',
                user_tags TEXT NOT NULL DEFAULT '[]',
                body_cached INTEGER NOT NULL DEFAULT 0 CHECK (body_cached IN (0, 1))
            );

            CREATE VIRTUAL TABLE IF NOT EXISTS fts_records USING fts5(
                number,
                short_desc,
                description,
                work_notes,
                content,
                tag_tokens
            );

            CREATE TABLE IF NOT EXISTS kb_sync_state (
                id INTEGER PRIMARY KEY CHECK (id = 1),
                last_full_at INTEGER,
                last_incr_at INTEGER,
                watermark_updated_at TEXT,
                watermark_sys_id TEXT,
                kb_sync_lock INTEGER
            );

            CREATE TABLE IF NOT EXISTS kb_term_stats (
                term TEXT PRIMARY KEY,
                doc_freq INTEGER NOT NULL CHECK (doc_freq >= 0)
            );

            CREATE TABLE IF NOT EXISTS kb_article_terms (
                record_sys_id TEXT PRIMARY KEY,
                terms_json TEXT NOT NULL DEFAULT '[]'
            );

            CREATE TABLE IF NOT EXISTS kb_local_file_state (
                record_sys_id TEXT PRIMARY KEY,
                file_path TEXT NOT NULL,
                modified_at_ms INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS knowledge_article_embeddings (
                record_sys_id TEXT PRIMARY KEY
                    REFERENCES knowledge_articles(record_sys_id)
                    ON DELETE CASCADE,
                model TEXT NOT NULL,
                provider TEXT NOT NULL,
                dimensions INTEGER NOT NULL,
                coverage TEXT NOT NULL
                    CHECK (coverage IN ('metadata', 'full_text')),
                content_hash TEXT NOT NULL,
                vector_blob BLOB NOT NULL,
                updated_at INTEGER NOT NULL
            );

            CREATE TABLE IF NOT EXISTS business_applications (
                record_sys_id TEXT PRIMARY KEY
                    REFERENCES records(sys_id)
                    ON DELETE CASCADE,
                name TEXT NOT NULL,
                number TEXT,
                business_owner_sys_id TEXT,
                business_owner_name TEXT,
                is_owner_sys_id TEXT,
                is_owner_name TEXT,
                ci_owner_group_sys_id TEXT,
                ci_owner_group_name TEXT,
                primary_support_group_sys_id TEXT,
                primary_support_group_name TEXT,
                operational_status_value TEXT,
                operational_status_display TEXT,
                primary_portfolio_sys_id TEXT,
                primary_portfolio_name TEXT,
                primary_portfolio_table TEXT,
                attested_date TEXT,
                sys_updated_on TEXT,
                field_count INTEGER NOT NULL DEFAULT 0,
                reference_count INTEGER NOT NULL DEFAULT 0,
                unresolved_reference_count INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS business_application_fields (
                record_sys_id TEXT NOT NULL
                    REFERENCES records(sys_id)
                    ON DELETE CASCADE,
                field_name TEXT NOT NULL,
                field_label TEXT,
                field_type TEXT,
                value_text TEXT,
                display_value TEXT,
                value_number REAL,
                value_date TEXT,
                value_bool INTEGER CHECK (value_bool IN (0, 1)),
                reference_sys_id TEXT,
                reference_table TEXT,
                raw_json TEXT NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY(record_sys_id, field_name)
            );

            CREATE TABLE IF NOT EXISTS business_application_field_dictionary (
                table_name TEXT NOT NULL,
                field_name TEXT NOT NULL,
                field_label TEXT,
                field_type TEXT,
                reference_table TEXT,
                choice INTEGER NOT NULL DEFAULT 0 CHECK (choice IN (0, 1)),
                mandatory INTEGER NOT NULL DEFAULT 0 CHECK (mandatory IN (0, 1)),
                read_only INTEGER NOT NULL DEFAULT 0 CHECK (read_only IN (0, 1)),
                max_length INTEGER,
                active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
                synced_at INTEGER NOT NULL,
                raw_json TEXT NOT NULL DEFAULT '{}',
                PRIMARY KEY(table_name, field_name)
            );

            CREATE TABLE IF NOT EXISTS business_application_servers (
                ba_sys_id TEXT NOT NULL
                    REFERENCES records(sys_id)
                    ON DELETE CASCADE,
                server_sys_id TEXT NOT NULL
                    REFERENCES records(sys_id)
                    ON DELETE CASCADE,
                server_table TEXT NOT NULL,
                provenance TEXT NOT NULL,
                min_depth INTEGER NOT NULL CHECK (min_depth >= 0),
                paths_json TEXT NOT NULL DEFAULT '[]',
                discovered_at INTEGER NOT NULL,
                last_seen_at INTEGER NOT NULL,
                tombstoned_at INTEGER,
                PRIMARY KEY(ba_sys_id, server_sys_id, provenance)
            );

            CREATE TABLE IF NOT EXISTS business_application_server_inventory_health (
                ba_sys_id TEXT PRIMARY KEY
                    REFERENCES records(sys_id)
                    ON DELETE CASCADE,
                run_started_at INTEGER NOT NULL,
                run_completed_at INTEGER NOT NULL,
                service_membership_status TEXT NOT NULL
                    CHECK (service_membership_status IN (
                        'ok',
                        'not_attempted',
                        'acl_restricted',
                        'association_budget_exhausted',
                        'page_budget_exhausted'
                    )),
                relationship_status TEXT NOT NULL
                    CHECK (relationship_status IN (
                        'ok',
                        'depth_limited',
                        'edge_budget_exhausted',
                        'ci_budget_exhausted',
                        'acl_restricted',
                        'truncated'
                    )),
                inventory_status TEXT NOT NULL
                    CHECK (inventory_status IN (
                        'complete',
                        'service_membership_degraded',
                        'relationship_degraded',
                        'truncated',
                        'failed'
                    )),
                summary_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE TABLE IF NOT EXISTS primitive_objects (
                sys_id TEXT PRIMARY KEY,
                table_name TEXT NOT NULL,
                resource_type TEXT NOT NULL,
                display_name TEXT NOT NULL,
                number TEXT,
                file_path TEXT,
                raw_json TEXT NOT NULL DEFAULT '{}',
                synced_at INTEGER NOT NULL,
                sys_updated_on TEXT,
                resolution_status TEXT NOT NULL
                    CHECK (resolution_status IN ('resolved','unresolved','unknown_table','not_found','acl_restricted','error')),
                last_error TEXT
            );

            CREATE TABLE IF NOT EXISTS primitive_object_fields (
                primitive_sys_id TEXT NOT NULL
                    REFERENCES primitive_objects(sys_id)
                    ON DELETE CASCADE,
                field_name TEXT NOT NULL,
                field_label TEXT,
                field_type TEXT,
                value_text TEXT,
                display_value TEXT,
                value_number REAL,
                value_date TEXT,
                value_bool INTEGER CHECK (value_bool IN (0, 1)),
                reference_sys_id TEXT,
                reference_table TEXT,
                raw_json TEXT NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY(primitive_sys_id, field_name)
            );

            CREATE TABLE IF NOT EXISTS cached_users (
                sys_id TEXT PRIMARY KEY,
                user_name TEXT,
                name TEXT,
                first_name TEXT,
                last_name TEXT,
                email TEXT,
                employee_number TEXT,
                active INTEGER CHECK (active IN (0, 1)),
                department TEXT,
                location TEXT,
                title TEXT,
                raw_json TEXT NOT NULL DEFAULT '{}',
                synced_at INTEGER NOT NULL,
                sys_updated_on TEXT
            );

            CREATE TABLE IF NOT EXISTS cached_user_queries (
                query_key TEXT PRIMARY KEY,
                result_sys_ids_json TEXT NOT NULL DEFAULT '[]',
                synced_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_records_number ON records(number);
            CREATE INDEX IF NOT EXISTS idx_records_table_scope ON records(table_name, in_scope, number);
            CREATE INDEX IF NOT EXISTS idx_records_parent ON records(parent_id);
            CREATE INDEX IF NOT EXISTS idx_records_resource_type ON records(resource_type, in_scope);
            CREATE INDEX IF NOT EXISTS idx_records_updated ON records(sys_updated_on);
            CREATE INDEX IF NOT EXISTS idx_references_table ON "references"(table_name);
            CREATE INDEX IF NOT EXISTS idx_relationships_source ON relationships(source_id);
            CREATE INDEX IF NOT EXISTS idx_relationships_target ON relationships(target_id);
            CREATE INDEX IF NOT EXISTS idx_sync_state_updated ON sync_state(updated_at);
            CREATE INDEX IF NOT EXISTS idx_record_tags_tag ON record_tags(tag);
            CREATE INDEX IF NOT EXISTS idx_record_tags_record ON record_tags(record_sys_id);
            CREATE INDEX IF NOT EXISTS idx_record_keywords_keyword ON record_keywords(keyword);
            CREATE INDEX IF NOT EXISTS idx_record_keywords_record ON record_keywords(record_sys_id);
            CREATE INDEX IF NOT EXISTS idx_record_aliases_alias ON record_aliases(alias);
            CREATE INDEX IF NOT EXISTS idx_record_aliases_record ON record_aliases(record_sys_id);
            CREATE INDEX IF NOT EXISTS idx_knowledge_articles_base ON knowledge_articles(knowledge_base_sys_id, knowledge_base_name);
            CREATE INDEX IF NOT EXISTS idx_knowledge_articles_category ON knowledge_articles(knowledge_base_sys_id, category_sys_id, category_name);
            CREATE INDEX IF NOT EXISTS idx_knowledge_articles_number ON knowledge_articles(number);
            CREATE INDEX IF NOT EXISTS idx_kb_local_file_state_path ON kb_local_file_state(file_path);
            CREATE INDEX IF NOT EXISTS idx_kb_embeddings_model
                ON knowledge_article_embeddings(model, coverage);
            CREATE INDEX IF NOT EXISTS idx_ba_name ON business_applications(name);
            CREATE INDEX IF NOT EXISTS idx_ba_business_owner
                ON business_applications(business_owner_sys_id, business_owner_name);
            CREATE INDEX IF NOT EXISTS idx_ba_is_owner
                ON business_applications(is_owner_sys_id, is_owner_name);
            CREATE INDEX IF NOT EXISTS idx_ba_ci_owner_group
                ON business_applications(ci_owner_group_sys_id, ci_owner_group_name);
            CREATE INDEX IF NOT EXISTS idx_ba_support_group
                ON business_applications(primary_support_group_sys_id, primary_support_group_name);
            CREATE INDEX IF NOT EXISTS idx_ba_operational_status
                ON business_applications(operational_status_value, operational_status_display);
            CREATE INDEX IF NOT EXISTS idx_ba_portfolio
                ON business_applications(primary_portfolio_table, primary_portfolio_sys_id, primary_portfolio_name);
            CREATE INDEX IF NOT EXISTS idx_ba_attested_date ON business_applications(attested_date);
            CREATE INDEX IF NOT EXISTS idx_ba_fields_name_text
                ON business_application_fields(field_name, value_text);
            CREATE INDEX IF NOT EXISTS idx_ba_fields_name_display
                ON business_application_fields(field_name, display_value);
            CREATE INDEX IF NOT EXISTS idx_ba_fields_name_date
                ON business_application_fields(field_name, value_date);
            CREATE INDEX IF NOT EXISTS idx_ba_fields_name_number
                ON business_application_fields(field_name, value_number);
            CREATE INDEX IF NOT EXISTS idx_ba_fields_ref
                ON business_application_fields(field_name, reference_table, reference_sys_id);
            CREATE INDEX IF NOT EXISTS idx_ba_servers_ba
                ON business_application_servers(ba_sys_id, tombstoned_at, min_depth, server_sys_id);
            CREATE INDEX IF NOT EXISTS idx_ba_servers_server
                ON business_application_servers(server_sys_id, tombstoned_at, min_depth, ba_sys_id);
            CREATE INDEX IF NOT EXISTS idx_primitive_objects_table
                ON primitive_objects(table_name, display_name);
            CREATE INDEX IF NOT EXISTS idx_primitive_fields_name_text
                ON primitive_object_fields(field_name, value_text);
            CREATE INDEX IF NOT EXISTS idx_primitive_fields_name_display
                ON primitive_object_fields(field_name, display_value);
            CREATE INDEX IF NOT EXISTS idx_primitive_fields_name_date
                ON primitive_object_fields(field_name, value_date);
            CREATE INDEX IF NOT EXISTS idx_primitive_fields_name_number
                ON primitive_object_fields(field_name, value_number);
            CREATE INDEX IF NOT EXISTS idx_primitive_fields_ref
                ON primitive_object_fields(field_name, reference_table, reference_sys_id);
            CREATE INDEX IF NOT EXISTS idx_cached_users_user_name
                ON cached_users(user_name);
            CREATE INDEX IF NOT EXISTS idx_cached_users_email
                ON cached_users(email);
            CREATE INDEX IF NOT EXISTS idx_cached_users_employee_number
                ON cached_users(employee_number);
            CREATE INDEX IF NOT EXISTS idx_cached_users_name
                ON cached_users(name);
            CREATE INDEX IF NOT EXISTS idx_cached_users_first_last
                ON cached_users(first_name, last_name);
            CREATE INDEX IF NOT EXISTS idx_cached_user_queries_expires
                ON cached_user_queries(expires_at);
            "#,
        )?;
        self.conn
            .execute("INSERT OR IGNORE INTO kb_sync_state (id) VALUES (1)", [])?;
        Ok(())
    }

    fn create_post_migration_indexes(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            CREATE INDEX IF NOT EXISTS idx_kb_articles_updated
                ON knowledge_articles(sys_updated_on, record_sys_id);
            CREATE INDEX IF NOT EXISTS idx_kb_local_file_state_path
                ON kb_local_file_state(file_path);
            "#,
        )?;
        Ok(())
    }

    fn migrate_to_v3(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            PRAGMA foreign_keys=OFF;
            PRAGMA legacy_alter_table=ON;
            BEGIN IMMEDIATE;

            ALTER TABLE records RENAME TO records_v2;

            CREATE TABLE records (
                sys_id TEXT PRIMARY KEY,
                number TEXT NOT NULL,
                table_name TEXT NOT NULL,
                resource_type TEXT NOT NULL,
                state TEXT,
                short_desc TEXT,
                description TEXT,
                assigned_to TEXT,
                parent_id TEXT,
                file_path TEXT,
                synced_at INTEGER NOT NULL,
                sys_updated_on INTEGER NOT NULL,
                etag TEXT,
                in_scope INTEGER NOT NULL DEFAULT 1 CHECK (in_scope IN (0, 1)),
                last_seen_at INTEGER NOT NULL,
                tombstoned_at INTEGER,
                pruned_at INTEGER,
                raw_json TEXT NOT NULL DEFAULT '{}'
            );

            INSERT INTO records (
                rowid, sys_id, number, table_name, resource_type, state, short_desc, description,
                assigned_to, parent_id, file_path, synced_at, sys_updated_on, etag,
                in_scope, last_seen_at, tombstoned_at, pruned_at, raw_json
            )
            SELECT
                rowid, sys_id, number, table_name, resource_type, state, short_desc, description,
                assigned_to, parent_id, file_path, synced_at, sys_updated_on, etag,
                in_scope, last_seen_at, tombstoned_at, pruned_at, raw_json
            FROM records_v2;

            DROP TABLE records_v2;

            COMMIT;
            PRAGMA legacy_alter_table=OFF;
            PRAGMA foreign_keys=ON;
            "#,
        )?;
        Ok(())
    }

    fn migrate_to_v5(&self) -> Result<()> {
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<()> {
            self.add_column_if_missing("knowledge_articles", "sys_updated_on", "TEXT")?;
            self.add_column_if_missing(
                "knowledge_articles",
                "sn_tags",
                "TEXT NOT NULL DEFAULT '[]'",
            )?;
            self.add_column_if_missing(
                "knowledge_articles",
                "auto_tags",
                "TEXT NOT NULL DEFAULT '[]'",
            )?;
            self.add_column_if_missing(
                "knowledge_articles",
                "user_tags",
                "TEXT NOT NULL DEFAULT '[]'",
            )?;
            self.add_column_if_missing(
                "knowledge_articles",
                "body_cached",
                "INTEGER NOT NULL DEFAULT 0 CHECK (body_cached IN (0, 1))",
            )?;

            self.conn.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS kb_sync_state (
                    id INTEGER PRIMARY KEY CHECK (id = 1),
                    last_full_at INTEGER,
                    last_incr_at INTEGER,
                    watermark_updated_at TEXT,
                    watermark_sys_id TEXT,
                    kb_sync_lock INTEGER
                );
                CREATE INDEX IF NOT EXISTS idx_kb_articles_updated
                    ON knowledge_articles(sys_updated_on, record_sys_id);
                "#,
            )?;
            self.conn
                .execute("INSERT OR IGNORE INTO kb_sync_state (id) VALUES (1)", [])?;

            if !self.table_has_column("fts_records", "tag_tokens")? {
                self.conn.execute_batch(
                    r#"
                    ALTER TABLE fts_records RENAME TO fts_records_v4;
                    CREATE VIRTUAL TABLE fts_records USING fts5(
                        number,
                        short_desc,
                        description,
                        work_notes,
                        content,
                        tag_tokens
                    );
                    INSERT INTO fts_records(
                        rowid, number, short_desc, description, work_notes, content, tag_tokens
                    )
                    SELECT rowid, number, short_desc, description, work_notes, content, ''
                    FROM fts_records_v4;
                    DROP TABLE fts_records_v4;
                    "#,
                )?;
            }

            Ok(())
        })();
        match result {
            Ok(()) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    fn migrate_to_v6(&self) -> Result<()> {
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<()> {
            self.conn.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS kb_term_stats (
                    term TEXT PRIMARY KEY,
                    doc_freq INTEGER NOT NULL CHECK (doc_freq >= 0)
                );
                CREATE TABLE IF NOT EXISTS kb_article_terms (
                    record_sys_id TEXT PRIMARY KEY,
                    terms_json TEXT NOT NULL DEFAULT '[]'
                );
                CREATE TABLE IF NOT EXISTS kb_local_file_state (
                    record_sys_id TEXT PRIMARY KEY,
                    file_path TEXT NOT NULL,
                    modified_at_ms INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_kb_local_file_state_path
                    ON kb_local_file_state(file_path);
                "#,
            )?;
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    fn needs_v3_migration(&self, schema_version: i64) -> Result<bool> {
        Ok(schema_version > 0 && schema_version < 3)
    }

    fn needs_v5_migration(&self, schema_version: i64) -> Result<bool> {
        if schema_version > 0 && schema_version < 5 {
            return Ok(true);
        }
        if !self.table_exists("knowledge_articles")? || !self.table_exists("fts_records")? {
            return Ok(false);
        }
        Ok(
            !self.table_has_column("knowledge_articles", "sys_updated_on")?
                || !self.table_has_column("knowledge_articles", "sn_tags")?
                || !self.table_has_column("knowledge_articles", "auto_tags")?
                || !self.table_has_column("knowledge_articles", "user_tags")?
                || !self.table_has_column("knowledge_articles", "body_cached")?
                || !self.table_exists("kb_sync_state")?
                || !self.table_has_column("fts_records", "tag_tokens")?,
        )
    }

    fn needs_v6_migration(&self, schema_version: i64) -> Result<bool> {
        if schema_version > 0 && schema_version < 6 {
            return Ok(true);
        }
        Ok(!self.table_exists("kb_term_stats")?
            || !self.table_exists("kb_article_terms")?
            || !self.table_exists("kb_local_file_state")?)
    }

    fn migrate_to_v7(&self) -> Result<()> {
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<()> {
            self.conn.execute_batch(
                r#"
                CREATE TABLE IF NOT EXISTS knowledge_article_embeddings (
                    record_sys_id TEXT PRIMARY KEY
                        REFERENCES knowledge_articles(record_sys_id)
                        ON DELETE CASCADE,
                    model TEXT NOT NULL,
                    provider TEXT NOT NULL,
                    dimensions INTEGER NOT NULL,
                    coverage TEXT NOT NULL
                        CHECK (coverage IN ('metadata', 'full_text')),
                    content_hash TEXT NOT NULL,
                    vector_blob BLOB NOT NULL,
                    updated_at INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_kb_embeddings_model
                    ON knowledge_article_embeddings(model, coverage);
                "#,
            )?;
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    fn needs_v7_migration(&self, schema_version: i64) -> Result<bool> {
        if schema_version > 0 && schema_version < 7 {
            return Ok(true);
        }
        Ok(!self.table_exists("knowledge_article_embeddings")?)
    }

    fn migrate_to_v8(&self) -> Result<()> {
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<()> {
            self.create_v8_schema_objects()?;
            self.conn.execute(
                r#"
                UPDATE records
                SET resource_type = 'business_application'
                WHERE resource_type = 'cmdb_ci_business_app'
                   OR table_name = 'cmdb_ci_business_app'
                "#,
                [],
            )?;
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    fn needs_v8_migration(&self, schema_version: i64) -> Result<bool> {
        if schema_version > 0 && schema_version < 8 {
            return Ok(true);
        }
        Ok(!self.table_exists("business_applications")?
            || !self.table_exists("business_application_fields")?
            || !self.table_exists("business_application_field_dictionary")?
            || !self.table_exists("primitive_objects")?
            || !self.table_exists("primitive_object_fields")?
            || !self.index_exists("idx_ba_fields_ref")?
            || !self.index_exists("idx_primitive_fields_ref")?)
    }

    fn migrate_to_v9(&self) -> Result<()> {
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = self.create_v9_schema_objects();
        match result {
            Ok(()) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    fn needs_v9_migration(&self, schema_version: i64) -> Result<bool> {
        if schema_version > 0 && schema_version < 9 {
            return Ok(true);
        }
        Ok(!self.table_exists("cached_users")?
            || !self.table_exists("cached_user_queries")?
            || !self.index_exists("idx_cached_users_user_name")?
            || !self.index_exists("idx_cached_users_email")?
            || !self.index_exists("idx_cached_users_employee_number")?
            || !self.index_exists("idx_cached_users_name")?
            || !self.index_exists("idx_cached_users_first_last")?
            || !self.index_exists("idx_cached_user_queries_expires")?)
    }

    fn migrate_to_v10(&self) -> Result<()> {
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = self.create_v10_schema_objects();
        match result {
            Ok(()) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    fn needs_v10_migration(&self, schema_version: i64) -> Result<bool> {
        if schema_version > 0 && schema_version < 10 {
            return Ok(true);
        }
        Ok(!self.table_exists("business_application_servers")?
            || !self.index_exists("idx_ba_servers_ba")?
            || !self.index_exists("idx_ba_servers_server")?)
    }

    fn migrate_to_v11(&self) -> Result<()> {
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<()> {
            self.create_v11_schema_objects()?;
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    fn needs_v11_migration(&self, schema_version: i64) -> Result<bool> {
        if schema_version > 0 && schema_version < 11 {
            return Ok(true);
        }
        Ok(
            !self.table_exists("business_application_server_inventory_health")?
                || !self.index_exists("idx_ba_servers_one_live_pair")?,
        )
    }

    fn create_v8_schema_objects(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS business_applications (
                record_sys_id TEXT PRIMARY KEY
                    REFERENCES records(sys_id)
                    ON DELETE CASCADE,
                name TEXT NOT NULL,
                number TEXT,
                business_owner_sys_id TEXT,
                business_owner_name TEXT,
                is_owner_sys_id TEXT,
                is_owner_name TEXT,
                ci_owner_group_sys_id TEXT,
                ci_owner_group_name TEXT,
                primary_support_group_sys_id TEXT,
                primary_support_group_name TEXT,
                operational_status_value TEXT,
                operational_status_display TEXT,
                primary_portfolio_sys_id TEXT,
                primary_portfolio_name TEXT,
                primary_portfolio_table TEXT,
                attested_date TEXT,
                sys_updated_on TEXT,
                field_count INTEGER NOT NULL DEFAULT 0,
                reference_count INTEGER NOT NULL DEFAULT 0,
                unresolved_reference_count INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS business_application_fields (
                record_sys_id TEXT NOT NULL
                    REFERENCES records(sys_id)
                    ON DELETE CASCADE,
                field_name TEXT NOT NULL,
                field_label TEXT,
                field_type TEXT,
                value_text TEXT,
                display_value TEXT,
                value_number REAL,
                value_date TEXT,
                value_bool INTEGER CHECK (value_bool IN (0, 1)),
                reference_sys_id TEXT,
                reference_table TEXT,
                raw_json TEXT NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY(record_sys_id, field_name)
            );

            CREATE TABLE IF NOT EXISTS business_application_field_dictionary (
                table_name TEXT NOT NULL,
                field_name TEXT NOT NULL,
                field_label TEXT,
                field_type TEXT,
                reference_table TEXT,
                choice INTEGER NOT NULL DEFAULT 0 CHECK (choice IN (0, 1)),
                mandatory INTEGER NOT NULL DEFAULT 0 CHECK (mandatory IN (0, 1)),
                read_only INTEGER NOT NULL DEFAULT 0 CHECK (read_only IN (0, 1)),
                max_length INTEGER,
                active INTEGER NOT NULL DEFAULT 1 CHECK (active IN (0, 1)),
                synced_at INTEGER NOT NULL,
                raw_json TEXT NOT NULL DEFAULT '{}',
                PRIMARY KEY(table_name, field_name)
            );

            CREATE TABLE IF NOT EXISTS primitive_objects (
                sys_id TEXT PRIMARY KEY,
                table_name TEXT NOT NULL,
                resource_type TEXT NOT NULL,
                display_name TEXT NOT NULL,
                number TEXT,
                file_path TEXT,
                raw_json TEXT NOT NULL DEFAULT '{}',
                synced_at INTEGER NOT NULL,
                sys_updated_on TEXT,
                resolution_status TEXT NOT NULL
                    CHECK (resolution_status IN ('resolved','unresolved','unknown_table','not_found','acl_restricted','error')),
                last_error TEXT
            );

            CREATE TABLE IF NOT EXISTS primitive_object_fields (
                primitive_sys_id TEXT NOT NULL
                    REFERENCES primitive_objects(sys_id)
                    ON DELETE CASCADE,
                field_name TEXT NOT NULL,
                field_label TEXT,
                field_type TEXT,
                value_text TEXT,
                display_value TEXT,
                value_number REAL,
                value_date TEXT,
                value_bool INTEGER CHECK (value_bool IN (0, 1)),
                reference_sys_id TEXT,
                reference_table TEXT,
                raw_json TEXT NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY(primitive_sys_id, field_name)
            );

            CREATE INDEX IF NOT EXISTS idx_ba_name ON business_applications(name);
            CREATE INDEX IF NOT EXISTS idx_ba_business_owner
                ON business_applications(business_owner_sys_id, business_owner_name);
            CREATE INDEX IF NOT EXISTS idx_ba_is_owner
                ON business_applications(is_owner_sys_id, is_owner_name);
            CREATE INDEX IF NOT EXISTS idx_ba_ci_owner_group
                ON business_applications(ci_owner_group_sys_id, ci_owner_group_name);
            CREATE INDEX IF NOT EXISTS idx_ba_support_group
                ON business_applications(primary_support_group_sys_id, primary_support_group_name);
            CREATE INDEX IF NOT EXISTS idx_ba_operational_status
                ON business_applications(operational_status_value, operational_status_display);
            CREATE INDEX IF NOT EXISTS idx_ba_portfolio
                ON business_applications(primary_portfolio_table, primary_portfolio_sys_id, primary_portfolio_name);
            CREATE INDEX IF NOT EXISTS idx_ba_attested_date ON business_applications(attested_date);
            CREATE INDEX IF NOT EXISTS idx_ba_fields_name_text
                ON business_application_fields(field_name, value_text);
            CREATE INDEX IF NOT EXISTS idx_ba_fields_name_display
                ON business_application_fields(field_name, display_value);
            CREATE INDEX IF NOT EXISTS idx_ba_fields_name_date
                ON business_application_fields(field_name, value_date);
            CREATE INDEX IF NOT EXISTS idx_ba_fields_name_number
                ON business_application_fields(field_name, value_number);
            CREATE INDEX IF NOT EXISTS idx_ba_fields_ref
                ON business_application_fields(field_name, reference_table, reference_sys_id);
            CREATE INDEX IF NOT EXISTS idx_primitive_objects_table
                ON primitive_objects(table_name, display_name);
            CREATE INDEX IF NOT EXISTS idx_primitive_fields_name_text
                ON primitive_object_fields(field_name, value_text);
            CREATE INDEX IF NOT EXISTS idx_primitive_fields_name_display
                ON primitive_object_fields(field_name, display_value);
            CREATE INDEX IF NOT EXISTS idx_primitive_fields_name_date
                ON primitive_object_fields(field_name, value_date);
            CREATE INDEX IF NOT EXISTS idx_primitive_fields_name_number
                ON primitive_object_fields(field_name, value_number);
            CREATE INDEX IF NOT EXISTS idx_primitive_fields_ref
                ON primitive_object_fields(field_name, reference_table, reference_sys_id);
            "#,
        )?;
        Ok(())
    }

    fn create_v9_schema_objects(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS cached_users (
                sys_id TEXT PRIMARY KEY,
                user_name TEXT,
                name TEXT,
                first_name TEXT,
                last_name TEXT,
                email TEXT,
                employee_number TEXT,
                active INTEGER CHECK (active IN (0, 1)),
                department TEXT,
                location TEXT,
                title TEXT,
                raw_json TEXT NOT NULL DEFAULT '{}',
                synced_at INTEGER NOT NULL,
                sys_updated_on TEXT
            );

            CREATE TABLE IF NOT EXISTS cached_user_queries (
                query_key TEXT PRIMARY KEY,
                result_sys_ids_json TEXT NOT NULL DEFAULT '[]',
                synced_at INTEGER NOT NULL,
                expires_at INTEGER NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_cached_users_user_name
                ON cached_users(user_name);
            CREATE INDEX IF NOT EXISTS idx_cached_users_email
                ON cached_users(email);
            CREATE INDEX IF NOT EXISTS idx_cached_users_employee_number
                ON cached_users(employee_number);
            CREATE INDEX IF NOT EXISTS idx_cached_users_name
                ON cached_users(name);
            CREATE INDEX IF NOT EXISTS idx_cached_users_first_last
                ON cached_users(first_name, last_name);
            CREATE INDEX IF NOT EXISTS idx_cached_user_queries_expires
                ON cached_user_queries(expires_at);
            "#,
        )?;
        Ok(())
    }

    fn create_v10_schema_objects(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS business_application_servers (
                ba_sys_id TEXT NOT NULL
                    REFERENCES records(sys_id)
                    ON DELETE CASCADE,
                server_sys_id TEXT NOT NULL
                    REFERENCES records(sys_id)
                    ON DELETE CASCADE,
                server_table TEXT NOT NULL,
                provenance TEXT NOT NULL,
                min_depth INTEGER NOT NULL CHECK (min_depth >= 0),
                paths_json TEXT NOT NULL DEFAULT '[]',
                discovered_at INTEGER NOT NULL,
                last_seen_at INTEGER NOT NULL,
                tombstoned_at INTEGER,
                PRIMARY KEY(ba_sys_id, server_sys_id, provenance)
            );

            CREATE INDEX IF NOT EXISTS idx_ba_servers_ba
                ON business_application_servers(ba_sys_id, tombstoned_at, min_depth, server_sys_id);
            CREATE INDEX IF NOT EXISTS idx_ba_servers_server
                ON business_application_servers(server_sys_id, tombstoned_at, min_depth, ba_sys_id);
            "#,
        )?;
        Ok(())
    }

    fn create_v11_schema_objects(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS business_application_server_inventory_health (
                ba_sys_id TEXT PRIMARY KEY
                    REFERENCES records(sys_id)
                    ON DELETE CASCADE,
                run_started_at INTEGER NOT NULL,
                run_completed_at INTEGER NOT NULL,
                service_membership_status TEXT NOT NULL
                    CHECK (service_membership_status IN (
                        'ok',
                        'not_attempted',
                        'acl_restricted',
                        'association_budget_exhausted',
                        'page_budget_exhausted'
                    )),
                relationship_status TEXT NOT NULL
                    CHECK (relationship_status IN (
                        'ok',
                        'depth_limited',
                        'edge_budget_exhausted',
                        'ci_budget_exhausted',
                        'acl_restricted',
                        'truncated'
                    )),
                inventory_status TEXT NOT NULL
                    CHECK (inventory_status IN (
                        'complete',
                        'service_membership_degraded',
                        'relationship_degraded',
                        'truncated',
                        'failed'
                    )),
                summary_json TEXT NOT NULL DEFAULT '{}'
            );
            "#,
        )?;
        self.collapse_duplicate_live_business_application_server_memberships()?;
        self.conn.execute(
            r#"
            CREATE UNIQUE INDEX IF NOT EXISTS idx_ba_servers_one_live_pair
                ON business_application_servers(ba_sys_id, server_sys_id)
                WHERE tombstoned_at IS NULL
            "#,
            [],
        )?;
        Ok(())
    }

    fn collapse_duplicate_live_business_application_server_memberships(&self) -> Result<()> {
        self.conn.execute_batch(
            r#"
            DROP TABLE IF EXISTS temp.ba_server_live_pair_cleanup;
            CREATE TEMP TABLE ba_server_live_pair_cleanup AS
            SELECT
                ba_sys_id,
                server_sys_id,
                MIN(discovered_at) AS earliest_discovered_at,
                MAX(last_seen_at) AS latest_last_seen_at,
                MIN(min_depth) AS min_depth,
                COALESCE(
                    MAX(CASE WHEN provenance = 'both' THEN server_table END),
                    MAX(CASE WHEN provenance = 'relationship' THEN server_table END),
                    MAX(CASE WHEN provenance = 'service_membership' THEN server_table END),
                    MAX(server_table)
                ) AS server_table,
                COALESCE(
                    MAX(CASE WHEN provenance = 'both' THEN paths_json END),
                    MAX(CASE WHEN provenance = 'relationship' THEN paths_json END),
                    MAX(paths_json),
                    '[]'
                ) AS paths_json,
                CASE
                    WHEN SUM(CASE WHEN provenance = 'both' THEN 1 ELSE 0 END) > 0 THEN 'both'
                    WHEN SUM(CASE WHEN provenance = 'relationship' THEN 1 ELSE 0 END) > 0
                     AND SUM(CASE WHEN provenance = 'service_membership' THEN 1 ELSE 0 END) > 0 THEN 'both'
                    WHEN SUM(CASE WHEN provenance = 'relationship' THEN 1 ELSE 0 END) > 0 THEN 'relationship'
                    WHEN SUM(CASE WHEN provenance = 'service_membership' THEN 1 ELSE 0 END) > 0 THEN 'service_membership'
                    ELSE MIN(provenance)
                END AS desired_provenance
            FROM business_application_servers
            WHERE tombstoned_at IS NULL
            GROUP BY ba_sys_id, server_sys_id
            HAVING COUNT(*) > 1;

            INSERT INTO business_application_servers (
                ba_sys_id, server_sys_id, server_table, provenance, min_depth,
                paths_json, discovered_at, last_seen_at, tombstoned_at
            )
            SELECT
                ba_sys_id, server_sys_id, server_table, desired_provenance, min_depth,
                paths_json, earliest_discovered_at, latest_last_seen_at, NULL
            FROM ba_server_live_pair_cleanup
            WHERE desired_provenance = 'both'
            ON CONFLICT(ba_sys_id, server_sys_id, provenance) DO UPDATE SET
                server_table = excluded.server_table,
                min_depth = excluded.min_depth,
                paths_json = excluded.paths_json,
                discovered_at = MIN(business_application_servers.discovered_at, excluded.discovered_at),
                last_seen_at = MAX(business_application_servers.last_seen_at, excluded.last_seen_at),
                tombstoned_at = NULL;

            UPDATE business_application_servers
            SET
                discovered_at = (
                    SELECT earliest_discovered_at
                    FROM ba_server_live_pair_cleanup cleanup
                    WHERE cleanup.ba_sys_id = business_application_servers.ba_sys_id
                      AND cleanup.server_sys_id = business_application_servers.server_sys_id
                ),
                last_seen_at = MAX(
                    last_seen_at,
                    (
                        SELECT latest_last_seen_at
                        FROM ba_server_live_pair_cleanup cleanup
                        WHERE cleanup.ba_sys_id = business_application_servers.ba_sys_id
                          AND cleanup.server_sys_id = business_application_servers.server_sys_id
                    )
                ),
                tombstoned_at = NULL
            WHERE tombstoned_at IS NULL
              AND EXISTS (
                    SELECT 1
                    FROM ba_server_live_pair_cleanup cleanup
                    WHERE cleanup.ba_sys_id = business_application_servers.ba_sys_id
                      AND cleanup.server_sys_id = business_application_servers.server_sys_id
                      AND cleanup.desired_provenance = business_application_servers.provenance
              );

            UPDATE business_application_servers
            SET tombstoned_at = (
                SELECT latest_last_seen_at
                FROM ba_server_live_pair_cleanup cleanup
                WHERE cleanup.ba_sys_id = business_application_servers.ba_sys_id
                  AND cleanup.server_sys_id = business_application_servers.server_sys_id
            )
            WHERE tombstoned_at IS NULL
              AND EXISTS (
                    SELECT 1
                    FROM ba_server_live_pair_cleanup cleanup
                    WHERE cleanup.ba_sys_id = business_application_servers.ba_sys_id
                      AND cleanup.server_sys_id = business_application_servers.server_sys_id
                      AND cleanup.desired_provenance <> business_application_servers.provenance
              );

            DROP TABLE IF EXISTS temp.ba_server_live_pair_cleanup;
            "#,
        )?;
        Ok(())
    }

    fn add_column_if_missing(&self, table: &str, column: &str, definition: &str) -> Result<()> {
        if self.table_has_column(table, column)? {
            return Ok(());
        }
        self.conn.execute(
            &format!("ALTER TABLE {table} ADD COLUMN {column} {definition}"),
            [],
        )?;
        Ok(())
    }

    fn table_has_column(&self, table: &str, column: &str) -> Result<bool> {
        let mut stmt = self.conn.prepare(&format!("PRAGMA table_info({table})"))?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(1))?;
        for name in rows {
            if name? == column {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn table_exists(&self, table: &str) -> Result<bool> {
        let exists = self.conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type IN ('table', 'view') AND name = ?1",
            [table],
            |row| row.get::<_, i64>(0),
        )?;
        Ok(exists > 0)
    }

    fn index_exists(&self, index: &str) -> Result<bool> {
        let exists = self.conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = ?1",
            [index],
            |row| row.get::<_, i64>(0),
        )?;
        Ok(exists > 0)
    }

    pub fn schema_version(&self) -> Result<Option<i64>> {
        self.conn
            .query_row(
                "SELECT value FROM schema_meta WHERE key = 'schema_version'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|value| {
                value
                    .parse()
                    .map_err(|_| StoreError::InvalidSchemaVersion(value))
            })
            .transpose()
    }

    pub fn set_schema_version(&self, version: i64) -> Result<()> {
        self.conn.execute(
            "INSERT INTO schema_meta(key, value) VALUES ('schema_version', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            params![version.to_string()],
        )?;
        Ok(())
    }

    pub fn get_meta_value(&self, key: &str) -> Result<Option<String>> {
        self.conn
            .query_row(
                "SELECT value FROM schema_meta WHERE key = ?1",
                params![key],
                |row| row.get::<_, String>(0),
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn set_meta_value(&self, key: &str, value: Option<&str>) -> Result<()> {
        match value {
            Some(value) => {
                self.conn.execute(
                    "INSERT INTO schema_meta(key, value) VALUES (?1, ?2)
                     ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    params![key, value],
                )?;
            }
            None => {
                self.conn
                    .execute("DELETE FROM schema_meta WHERE key = ?1", params![key])?;
            }
        }
        Ok(())
    }

    pub fn upsert_record(&self, record: &RecordRow, work_notes: &str, content: &str) -> Result<()> {
        self.upsert_record_with_tags(record, work_notes, content, "")
    }

    pub fn upsert_record_with_tags(
        &self,
        record: &RecordRow,
        work_notes: &str,
        content: &str,
        tag_tokens: &str,
    ) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO records (
                sys_id, number, table_name, resource_type, state, short_desc, description,
                assigned_to, parent_id, file_path, synced_at, sys_updated_on, etag,
                in_scope, last_seen_at, tombstoned_at, pruned_at, raw_json
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7,
                ?8, ?9, ?10, ?11, ?12, ?13,
                ?14, ?15, ?16, ?17, ?18
            )
            ON CONFLICT(sys_id) DO UPDATE SET
                number = excluded.number,
                table_name = excluded.table_name,
                resource_type = excluded.resource_type,
                state = excluded.state,
                short_desc = excluded.short_desc,
                description = excluded.description,
                assigned_to = excluded.assigned_to,
                parent_id = excluded.parent_id,
                file_path = excluded.file_path,
                synced_at = excluded.synced_at,
                sys_updated_on = excluded.sys_updated_on,
                etag = excluded.etag,
                in_scope = excluded.in_scope,
                last_seen_at = excluded.last_seen_at,
                tombstoned_at = excluded.tombstoned_at,
                pruned_at = excluded.pruned_at,
                raw_json = excluded.raw_json
            "#,
            params![
                &record.sys_id,
                &record.number,
                &record.table_name,
                resource_type_to_str(&record.resource_type),
                &record.state,
                &record.short_desc,
                &record.description,
                &record.assigned_to,
                &record.parent_id,
                &record.file_path,
                to_ts(record.synced_at),
                to_ts(record.sys_updated_on),
                &record.etag,
                bool_to_i64(record.in_scope),
                to_ts(record.last_seen_at),
                opt_ts(record.tombstoned_at),
                opt_ts(record.pruned_at),
                &record.raw_json,
            ],
        )?;

        let rowid: i64 = self.conn.query_row(
            "SELECT rowid FROM records WHERE sys_id = ?1",
            params![&record.sys_id],
            |row| row.get(0),
        )?;

        self.conn
            .execute("DELETE FROM fts_records WHERE rowid = ?1", params![rowid])?;
        self.conn.execute(
            "INSERT INTO fts_records(rowid, number, short_desc, description, work_notes, content, tag_tokens)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                rowid,
                &record.number,
                record.short_desc.as_deref().unwrap_or(""),
                record.description.as_deref().unwrap_or(""),
                work_notes,
                content,
                tag_tokens,
            ],
        )?;
        Ok(())
    }

    /// Batch-upsert multiple records in a single SQLite transaction.
    ///
    /// Each entry is `(record_row, work_notes_text, fts_content, fts_tag_tokens)`.
    pub fn upsert_records(&self, entries: &[(&RecordRow, &str, &str, &str)]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = entries
            .iter()
            .try_for_each(|(row, work_notes, content, tag_tokens)| {
                self.upsert_record_with_tags(row, work_notes, content, tag_tokens)
            });
        match result {
            Ok(()) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    pub fn get_primary_record_by_number(&self, number: &str) -> Result<Option<RecordRow>> {
        self.conn
            .query_row(
                r#"
                SELECT sys_id, number, table_name, resource_type, state, short_desc, description,
                       assigned_to, parent_id, file_path, synced_at, sys_updated_on, etag,
                       in_scope, last_seen_at, tombstoned_at, pruned_at, raw_json
                FROM records
                WHERE number = ?1
                ORDER BY CASE WHEN resource_type = 'sysapproval_approver' THEN 1 ELSE 0 END,
                         CASE WHEN in_scope = 1 THEN 0 ELSE 1 END,
                         synced_at DESC,
                         sys_id ASC
                LIMIT 1
                "#,
                params![number],
                row_to_record_row,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn get_record_by_number(&self, number: &str) -> Result<Option<RecordRow>> {
        self.get_primary_record_by_number(number)
    }

    pub fn get_record_by_number_and_type(
        &self,
        number: &str,
        resource_type: ResourceType,
    ) -> Result<Option<RecordRow>> {
        self.conn
            .query_row(
                r#"
                SELECT sys_id, number, table_name, resource_type, state, short_desc, description,
                       assigned_to, parent_id, file_path, synced_at, sys_updated_on, etag,
                       in_scope, last_seen_at, tombstoned_at, pruned_at, raw_json
                FROM records
                WHERE number = ?1
                  AND resource_type = ?2
                ORDER BY CASE WHEN in_scope = 1 THEN 0 ELSE 1 END,
                         synced_at DESC,
                         sys_id ASC
                LIMIT 1
                "#,
                params![number, resource_type_to_str(&resource_type)],
                row_to_record_row,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn get_record_by_sys_id(&self, sys_id: &str) -> Result<Option<RecordRow>> {
        self.conn
            .query_row(
                r#"
                SELECT sys_id, number, table_name, resource_type, state, short_desc, description,
                       assigned_to, parent_id, file_path, synced_at, sys_updated_on, etag,
                       in_scope, last_seen_at, tombstoned_at, pruned_at, raw_json
                FROM records
                WHERE sys_id = ?1
                "#,
                params![sys_id],
                row_to_record_row,
            )
            .optional()
            .map_err(Into::into)
    }

    /// Get the stored ETag for a record by its number, if any.
    pub fn get_etag_by_number(&self, number: &str) -> Result<Option<String>> {
        self.conn
            .query_row(
                "SELECT etag FROM records WHERE number = ?1 AND in_scope = 1 LIMIT 1",
                params![number],
                |row| row.get::<_, Option<String>>(0),
            )
            .optional()
            .map(|opt| opt.flatten())
            .map_err(Into::into)
    }

    pub fn list_active_records(
        &self,
        resource_type: Option<ResourceType>,
    ) -> Result<Vec<RecordRow>> {
        let mut stmt = match resource_type {
            Some(_) => self.conn.prepare(
                r#"
                SELECT sys_id, number, table_name, resource_type, state, short_desc, description,
                       assigned_to, parent_id, file_path, synced_at, sys_updated_on, etag,
                       in_scope, last_seen_at, tombstoned_at, pruned_at, raw_json
                FROM records
                WHERE in_scope = 1 AND resource_type = ?1
                ORDER BY number
                "#,
            )?,
            None => self.conn.prepare(
                r#"
                SELECT sys_id, number, table_name, resource_type, state, short_desc, description,
                       assigned_to, parent_id, file_path, synced_at, sys_updated_on, etag,
                       in_scope, last_seen_at, tombstoned_at, pruned_at, raw_json
                FROM records
                WHERE in_scope = 1
                ORDER BY number
                "#,
            )?,
        };

        let rows = match resource_type {
            Some(kind) => {
                stmt.query_map(params![resource_type_to_str(&kind)], row_to_record_row)?
            }
            None => stmt.query_map([], row_to_record_row)?,
        };
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn list_records_by_resource_type(
        &self,
        resource_type: ResourceType,
    ) -> Result<Vec<RecordRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT sys_id, number, table_name, resource_type, state, short_desc, description,
                   assigned_to, parent_id, file_path, synced_at, sys_updated_on, etag,
                   in_scope, last_seen_at, tombstoned_at, pruned_at, raw_json
            FROM records
            WHERE resource_type = ?1
            ORDER BY number
            "#,
        )?;
        let rows = stmt.query_map(
            params![resource_type_to_str(&resource_type)],
            row_to_record_row,
        )?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    /// List active records with LIMIT and OFFSET for pagination.
    pub fn list_active_records_paginated(
        &self,
        resource_type: Option<ResourceType>,
        limit: usize,
        offset: usize,
    ) -> Result<Vec<RecordRow>> {
        let (sql, bind_resource_type) = match resource_type {
            Some(ref kind) => (
                r#"
                SELECT sys_id, number, table_name, resource_type, state, short_desc, description,
                       assigned_to, parent_id, file_path, synced_at, sys_updated_on, etag,
                       in_scope, last_seen_at, tombstoned_at, pruned_at, raw_json
                FROM records
                WHERE in_scope = 1 AND resource_type = ?1
                ORDER BY number
                LIMIT ?2 OFFSET ?3
                "#,
                Some(resource_type_to_str(kind).to_string()),
            ),
            None => (
                r#"
                SELECT sys_id, number, table_name, resource_type, state, short_desc, description,
                       assigned_to, parent_id, file_path, synced_at, sys_updated_on, etag,
                       in_scope, last_seen_at, tombstoned_at, pruned_at, raw_json
                FROM records
                WHERE in_scope = 1
                ORDER BY number
                LIMIT ?1 OFFSET ?2
                "#,
                None,
            ),
        };

        let mut stmt = self.conn.prepare(sql)?;
        let rows = match bind_resource_type {
            Some(ref kind) => stmt.query_map(
                params![kind, limit as i64, offset as i64],
                row_to_record_row,
            )?,
            None => stmt.query_map(params![limit as i64, offset as i64], row_to_record_row)?,
        };
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn upsert_reference(&self, reference: &ReferenceRow) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO "references" (
                sys_id, table_name, display_name, extra_json, synced_at, expires_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(sys_id) DO UPDATE SET
                table_name = excluded.table_name,
                display_name = excluded.display_name,
                extra_json = excluded.extra_json,
                synced_at = excluded.synced_at,
                expires_at = excluded.expires_at
            "#,
            params![
                &reference.sys_id,
                &reference.table_name,
                &reference.display_name,
                &reference.extra_json,
                to_ts(reference.synced_at),
                opt_ts(reference.expires_at),
            ],
        )?;
        Ok(())
    }

    pub fn replace_references(&self, references: &[ReferenceRow]) -> Result<()> {
        for reference in references {
            self.upsert_reference(reference)?;
        }
        Ok(())
    }

    pub fn upsert_relationship(&self, relationship: &RelationshipRow) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO relationships (source_id, target_id, rel_type, field_name)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(source_id, target_id, rel_type, field_name)
            DO UPDATE SET
                source_id = excluded.source_id,
                target_id = excluded.target_id,
                rel_type = excluded.rel_type,
                field_name = excluded.field_name
            "#,
            params![
                &relationship.source_id,
                &relationship.target_id,
                &relationship.rel_type,
                &relationship.field_name,
            ],
        )?;
        Ok(())
    }

    pub fn replace_relationships_for_source(
        &self,
        source_id: &str,
        relationships: &[RelationshipRow],
    ) -> Result<()> {
        self.conn.execute(
            "DELETE FROM relationships WHERE source_id = ?1",
            params![source_id],
        )?;
        for relationship in relationships {
            self.upsert_relationship(relationship)?;
        }
        Ok(())
    }

    pub fn list_relationships_for_source(&self, source_id: &str) -> Result<Vec<RelationshipRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT source_id, target_id, rel_type, field_name
            FROM relationships
            WHERE source_id = ?1
            ORDER BY field_name, target_id
            "#,
        )?;
        let rows = stmt.query_map(params![source_id], |row| {
            Ok(RelationshipRow {
                source_id: row.get(0)?,
                target_id: row.get(1)?,
                rel_type: row.get(2)?,
                field_name: row.get(3)?,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn upsert_business_application_server_membership(
        &self,
        membership: &BusinessApplicationServerMembershipRow,
    ) -> Result<()> {
        self.conn
            .execute_batch("SAVEPOINT ba_server_membership_upsert")?;
        let result = (|| -> Result<()> {
            if membership.tombstoned_at.is_none() {
                self.conn.execute(
                    r#"
                    UPDATE business_application_servers
                    SET tombstoned_at = ?3
                    WHERE ba_sys_id = ?1
                      AND server_sys_id = ?2
                      AND provenance <> ?4
                      AND tombstoned_at IS NULL
                    "#,
                    params![
                        &membership.ba_sys_id,
                        &membership.server_sys_id,
                        to_ts(membership.last_seen_at),
                        &membership.provenance,
                    ],
                )?;
            }

            self.conn.execute(
                r#"
                INSERT INTO business_application_servers (
                    ba_sys_id, server_sys_id, server_table, provenance, min_depth,
                    paths_json, discovered_at, last_seen_at, tombstoned_at
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6,
                    COALESCE((
                        SELECT MIN(discovered_at)
                        FROM business_application_servers
                        WHERE ba_sys_id = ?1
                          AND server_sys_id = ?2
                    ), ?7),
                    ?8, ?9
                )
                ON CONFLICT(ba_sys_id, server_sys_id, provenance) DO UPDATE SET
                    server_table = excluded.server_table,
                    min_depth = excluded.min_depth,
                    paths_json = excluded.paths_json,
                    discovered_at = MIN(business_application_servers.discovered_at, excluded.discovered_at),
                    last_seen_at = excluded.last_seen_at,
                    tombstoned_at = excluded.tombstoned_at
                "#,
                params![
                    &membership.ba_sys_id,
                    &membership.server_sys_id,
                    &membership.server_table,
                    &membership.provenance,
                    membership.min_depth as i64,
                    &membership.paths_json,
                    to_ts(membership.discovered_at),
                    to_ts(membership.last_seen_at),
                    opt_ts(membership.tombstoned_at),
                ],
            )?;

            let duplicate_live_pairs: i64 = self.conn.query_row(
                r#"
                SELECT COUNT(*)
                FROM (
                    SELECT 1
                    FROM business_application_servers
                    WHERE ba_sys_id = ?1
                      AND server_sys_id = ?2
                      AND tombstoned_at IS NULL
                    GROUP BY ba_sys_id, server_sys_id
                    HAVING COUNT(*) > 1
                )
                "#,
                params![&membership.ba_sys_id, &membership.server_sys_id],
                |row| row.get(0),
            )?;
            if duplicate_live_pairs > 0 {
                return Err(StoreError::InvalidQuery(format!(
                    "duplicate live BA/server membership rows for ba_sys_id={} server_sys_id={}",
                    membership.ba_sys_id, membership.server_sys_id
                )));
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.conn
                    .execute_batch("RELEASE ba_server_membership_upsert")?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch(
                    "ROLLBACK TO ba_server_membership_upsert; RELEASE ba_server_membership_upsert",
                );
                Err(err)
            }
        }
    }

    pub fn tombstone_stale_business_application_server_memberships(
        &self,
        ba_sys_id: &str,
        last_seen_before: DateTime<Utc>,
        tombstoned_at: DateTime<Utc>,
    ) -> Result<usize> {
        let updated = self.conn.execute(
            r#"
            UPDATE business_application_servers
            SET tombstoned_at = ?2
            WHERE ba_sys_id = ?1
              AND tombstoned_at IS NULL
              AND last_seen_at < ?3
            "#,
            params![
                ba_sys_id,
                opt_ts(Some(tombstoned_at)),
                to_ts(last_seen_before)
            ],
        )?;
        Ok(updated)
    }

    pub fn upsert_business_application_server_inventory_health(
        &self,
        health: &BusinessApplicationServerInventoryHealthRow,
    ) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO business_application_server_inventory_health (
                ba_sys_id, run_started_at, run_completed_at, service_membership_status,
                relationship_status, inventory_status, summary_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT(ba_sys_id) DO UPDATE SET
                run_started_at = excluded.run_started_at,
                run_completed_at = excluded.run_completed_at,
                service_membership_status = excluded.service_membership_status,
                relationship_status = excluded.relationship_status,
                inventory_status = excluded.inventory_status,
                summary_json = excluded.summary_json
            "#,
            params![
                &health.ba_sys_id,
                to_ts(health.run_started_at),
                to_ts(health.run_completed_at),
                &health.service_membership_status,
                &health.relationship_status,
                &health.inventory_status,
                &health.summary_json,
            ],
        )?;
        Ok(())
    }

    pub fn get_business_application_server_inventory_health(
        &self,
        ba_sys_id: &str,
    ) -> Result<Option<BusinessApplicationServerInventoryHealthRow>> {
        self.conn
            .query_row(
                r#"
                SELECT ba_sys_id, run_started_at, run_completed_at,
                       service_membership_status, relationship_status, inventory_status,
                       summary_json
                FROM business_application_server_inventory_health
                WHERE ba_sys_id = ?1
                "#,
                params![ba_sys_id],
                row_to_business_application_server_inventory_health,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn list_business_application_server_memberships_for_ba(
        &self,
        ba_sys_id: &str,
        include_tombstoned: bool,
    ) -> Result<Vec<BusinessApplicationServerMembershipRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT ba_sys_id, server_sys_id, server_table, provenance, min_depth,
                   paths_json, discovered_at, last_seen_at, tombstoned_at
            FROM business_application_servers
            WHERE ba_sys_id = ?1
              AND (?2 != 0 OR tombstoned_at IS NULL)
            ORDER BY min_depth ASC, server_table ASC, server_sys_id ASC, provenance ASC
            "#,
        )?;
        let rows = stmt.query_map(
            params![ba_sys_id, bool_to_i64(include_tombstoned)],
            row_to_business_application_server_membership,
        )?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn list_business_application_server_memberships_for_server(
        &self,
        server_sys_id: &str,
        include_tombstoned: bool,
    ) -> Result<Vec<BusinessApplicationServerMembershipRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT ba_sys_id, server_sys_id, server_table, provenance, min_depth,
                   paths_json, discovered_at, last_seen_at, tombstoned_at
            FROM business_application_servers
            WHERE server_sys_id = ?1
              AND (?2 != 0 OR tombstoned_at IS NULL)
            ORDER BY min_depth ASC, ba_sys_id ASC, provenance ASC
            "#,
        )?;
        let rows = stmt.query_map(
            params![server_sys_id, bool_to_i64(include_tombstoned)],
            row_to_business_application_server_membership,
        )?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn list_references_for_source(&self, source_id: &str) -> Result<Vec<RecordReferenceRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT relationships.source_id,
                   relationships.field_name,
                   "references".sys_id,
                   "references".table_name,
                   "references".display_name,
                   "references".extra_json,
                   "references".synced_at,
                   "references".expires_at
            FROM relationships
            INNER JOIN "references" ON "references".sys_id = relationships.target_id
            WHERE relationships.source_id = ?1
              AND relationships.rel_type = 'reference'
            ORDER BY relationships.field_name, "references".sys_id
            "#,
        )?;
        let rows = stmt.query_map(params![source_id], |row| {
            Ok(RecordReferenceRow {
                source_id: row.get(0)?,
                field_name: row.get(1)?,
                reference: ReferenceRow {
                    sys_id: row.get(2)?,
                    table_name: row.get(3)?,
                    display_name: row.get(4)?,
                    extra_json: row.get(5)?,
                    synced_at: from_ts(row.get(6)?).map_err(to_sqlite_err)?,
                    expires_at: row
                        .get::<_, Option<i64>>(7)?
                        .map(from_ts)
                        .transpose()
                        .map_err(to_sqlite_err)?,
                },
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn replace_relationships(
        &self,
        source_id: &str,
        relationships: &[RelationshipRow],
    ) -> Result<()> {
        self.conn.execute(
            "DELETE FROM relationships WHERE source_id = ?1",
            params![source_id],
        )?;
        for relationship in relationships {
            self.upsert_relationship(relationship)?;
        }
        Ok(())
    }

    pub fn set_sync_state(&self, row: &SyncStateRow) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO sync_state (
                resource_type, last_full, last_incr, high_watermark, cursor, filter_hash, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, strftime('%s', 'now'))
            ON CONFLICT(resource_type) DO UPDATE SET
                last_full = excluded.last_full,
                last_incr = excluded.last_incr,
                high_watermark = excluded.high_watermark,
                cursor = excluded.cursor,
                filter_hash = excluded.filter_hash,
                updated_at = excluded.updated_at
            "#,
            params![
                &row.resource_type,
                opt_ts(row.last_full),
                opt_ts(row.last_incr),
                opt_ts(row.high_watermark),
                &row.cursor,
                &row.filter_hash,
            ],
        )?;
        Ok(())
    }

    pub fn list_references(&self) -> Result<Vec<ReferenceRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT sys_id, table_name, display_name, extra_json, synced_at, expires_at
            FROM "references"
            ORDER BY table_name, display_name, sys_id
            "#,
        )?;
        let rows = stmt.query_map([], |row| {
            let synced_at = from_ts(row.get::<_, i64>(4)?).map_err(to_sqlite_err)?;
            Ok(ReferenceRow {
                sys_id: row.get(0)?,
                table_name: row.get(1)?,
                display_name: row.get(2)?,
                extra_json: row.get(3)?,
                synced_at,
                expires_at: row
                    .get::<_, Option<i64>>(5)?
                    .map(from_ts)
                    .transpose()
                    .map_err(to_sqlite_err)?,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn list_relationships(&self) -> Result<Vec<RelationshipRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT source_id, target_id, rel_type, field_name
            FROM relationships
            ORDER BY source_id, rel_type, field_name, target_id
            "#,
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(RelationshipRow {
                source_id: row.get(0)?,
                target_id: row.get(1)?,
                rel_type: row.get(2)?,
                field_name: row.get(3)?,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn upsert_business_application_projection(
        &self,
        record: &SnowRecord,
        raw_fields: Option<&Value>,
    ) -> Result<()> {
        if record.resource_type != ResourceType::BusinessApplication
            && record.table != "cmdb_ci_business_app"
        {
            return Err(StoreError::InvalidQuery(format!(
                "record {} is not a Business Application",
                record.sys_id
            )));
        }

        let raw_map = raw_fields
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_else(|| field_values_to_json_map(&record.fields));
        let dictionary = self.business_application_dictionary_map()?;
        let now = Utc::now();
        let fields = projected_fields_from_json(
            &record.sys_id,
            &raw_map,
            Some(&record.fields),
            &dictionary,
            now,
        )?;
        let projection = business_application_projection_from_fields(record, &fields);

        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<()> {
            self.upsert_business_application_projection_row(&projection)?;
            self.conn.execute(
                "DELETE FROM business_application_fields WHERE record_sys_id = ?1",
                params![&record.sys_id],
            )?;
            for field in &fields {
                self.upsert_business_application_field(field)?;
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    pub fn rebuild_business_application_projection_from_raw_json(
        &self,
        row: &RecordRow,
    ) -> Result<bool> {
        if row.resource_type != ResourceType::BusinessApplication
            && row.table_name != "cmdb_ci_business_app"
        {
            return Ok(false);
        }
        let raw = serde_json::from_str::<Value>(&row.raw_json)?;
        let Some(raw_map) = raw.as_object() else {
            return Ok(false);
        };
        let fields_map = raw_map
            .iter()
            .map(|(key, value)| (key.clone(), field_value_from_raw_json(value)))
            .collect::<HashMap<_, _>>();
        let record = SnowRecord {
            sys_id: row.sys_id.clone(),
            number: row.number.clone(),
            table: row.table_name.clone(),
            resource_type: ResourceType::BusinessApplication,
            state: row.state.clone().unwrap_or_default(),
            short_description: row.short_desc.clone().unwrap_or_default(),
            description: row.description.clone().unwrap_or_default(),
            fields: fields_map,
            work_notes: Vec::new(),
            comments: Vec::new(),
            parent: None,
            children: Vec::new(),
            references: HashMap::new(),
            synced_at: row.synced_at,
            source: crate::CacheSource::Disk,
        };
        self.upsert_business_application_projection(&record, Some(&raw))?;
        Ok(true)
    }

    pub fn get_business_application_projection(
        &self,
        record_sys_id: &str,
    ) -> Result<Option<BusinessApplicationProjectionRow>> {
        self.conn
            .query_row(
                r#"
                SELECT record_sys_id, name, number, business_owner_sys_id, business_owner_name,
                       is_owner_sys_id, is_owner_name, ci_owner_group_sys_id, ci_owner_group_name,
                       primary_support_group_sys_id, primary_support_group_name,
                       operational_status_value, operational_status_display,
                       primary_portfolio_sys_id, primary_portfolio_name, primary_portfolio_table,
                       attested_date, sys_updated_on, field_count, reference_count,
                       unresolved_reference_count
                FROM business_applications
                WHERE record_sys_id = ?1
                "#,
                params![record_sys_id],
                row_to_business_application_projection,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn list_business_application_fields(
        &self,
        record_sys_id: &str,
    ) -> Result<Vec<ProjectedFieldRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT record_sys_id, field_name, field_label, field_type, value_text,
                   display_value, value_number, value_date, value_bool, reference_sys_id,
                   reference_table, raw_json, updated_at
            FROM business_application_fields
            WHERE record_sys_id = ?1
            ORDER BY field_name
            "#,
        )?;
        let rows = stmt.query_map(params![record_sys_id], row_to_projected_field)?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn upsert_business_application_field_dictionary(
        &self,
        field: &BusinessApplicationFieldDictionaryRow,
    ) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO business_application_field_dictionary (
                table_name, field_name, field_label, field_type, reference_table, choice,
                mandatory, read_only, max_length, active, synced_at, raw_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)
            ON CONFLICT(table_name, field_name) DO UPDATE SET
                field_label = excluded.field_label,
                field_type = excluded.field_type,
                reference_table = excluded.reference_table,
                choice = excluded.choice,
                mandatory = excluded.mandatory,
                read_only = excluded.read_only,
                max_length = excluded.max_length,
                active = excluded.active,
                synced_at = excluded.synced_at,
                raw_json = excluded.raw_json
            "#,
            params![
                &field.table_name,
                &field.field_name,
                &field.field_label,
                &field.field_type,
                &field.reference_table,
                bool_to_i64(field.choice),
                bool_to_i64(field.mandatory),
                bool_to_i64(field.read_only),
                &field.max_length,
                bool_to_i64(field.active),
                to_ts(field.synced_at),
                &field.raw_json,
            ],
        )?;
        Ok(())
    }

    pub fn upsert_primitive_object(&self, object: &PrimitiveObjectRow) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO primitive_objects (
                sys_id, table_name, resource_type, display_name, number, file_path, raw_json,
                synced_at, sys_updated_on, resolution_status, last_error
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
            ON CONFLICT(sys_id) DO UPDATE SET
                table_name = excluded.table_name,
                resource_type = excluded.resource_type,
                display_name = excluded.display_name,
                number = excluded.number,
                file_path = excluded.file_path,
                raw_json = excluded.raw_json,
                synced_at = excluded.synced_at,
                sys_updated_on = excluded.sys_updated_on,
                resolution_status = excluded.resolution_status,
                last_error = excluded.last_error
            "#,
            params![
                &object.sys_id,
                &object.table_name,
                &object.resource_type,
                &object.display_name,
                &object.number,
                &object.file_path,
                &object.raw_json,
                to_ts(object.synced_at),
                &object.sys_updated_on,
                primitive_resolution_status_to_str(object.resolution_status),
                &object.last_error,
            ],
        )?;
        Ok(())
    }

    pub fn upsert_unresolved_primitive_stub(
        &self,
        sys_id: impl Into<String>,
        table_name: impl Into<String>,
        display_name: impl Into<String>,
        resolution_status: PrimitiveResolutionStatus,
        last_error: Option<String>,
    ) -> Result<()> {
        let table_name = table_name.into();
        let sys_id = sys_id.into();
        let display_name = display_name.into();
        let raw_json = serde_json::json!({
            "sys_id": sys_id,
            "table": table_name,
            "display_value": display_name,
            "resolution_status": primitive_resolution_status_to_str(resolution_status),
        })
        .to_string();
        self.upsert_primitive_object(&PrimitiveObjectRow {
            sys_id,
            table_name,
            resource_type: "referenced_record".to_string(),
            display_name,
            number: None,
            file_path: None,
            raw_json,
            synced_at: Utc::now(),
            sys_updated_on: None,
            resolution_status,
            last_error,
        })
    }

    pub fn upsert_primitive_object_field(
        &self,
        primitive_sys_id: &str,
        field: &ProjectedFieldRow,
    ) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO primitive_object_fields (
                primitive_sys_id, field_name, field_label, field_type, value_text,
                display_value, value_number, value_date, value_bool, reference_sys_id,
                reference_table, raw_json, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
            ON CONFLICT(primitive_sys_id, field_name) DO UPDATE SET
                field_label = excluded.field_label,
                field_type = excluded.field_type,
                value_text = excluded.value_text,
                display_value = excluded.display_value,
                value_number = excluded.value_number,
                value_date = excluded.value_date,
                value_bool = excluded.value_bool,
                reference_sys_id = excluded.reference_sys_id,
                reference_table = excluded.reference_table,
                raw_json = excluded.raw_json,
                updated_at = excluded.updated_at
            "#,
            params![
                primitive_sys_id,
                &field.field_name,
                &field.field_label,
                &field.field_type,
                &field.value_text,
                &field.display_value,
                &field.value_number,
                &field.value_date,
                field.value_bool.map(bool_to_i64),
                &field.reference_sys_id,
                &field.reference_table,
                &field.raw_json,
                to_ts(field.updated_at),
            ],
        )?;
        Ok(())
    }

    pub fn get_primitive_object(&self, sys_id: &str) -> Result<Option<PrimitiveObjectRow>> {
        self.conn
            .query_row(
                r#"
                SELECT sys_id, table_name, resource_type, display_name, number, file_path,
                       raw_json, synced_at, sys_updated_on, resolution_status, last_error
                FROM primitive_objects
                WHERE sys_id = ?1
                "#,
                params![sys_id],
                |row| {
                    Ok(PrimitiveObjectRow {
                        sys_id: row.get(0)?,
                        table_name: row.get(1)?,
                        resource_type: row.get(2)?,
                        display_name: row.get(3)?,
                        number: row.get(4)?,
                        file_path: row.get(5)?,
                        raw_json: row.get(6)?,
                        synced_at: from_ts(row.get(7)?).map_err(to_sqlite_err)?,
                        sys_updated_on: row.get(8)?,
                        resolution_status: primitive_resolution_status_from_str(
                            &row.get::<_, String>(9)?,
                        )
                        .map_err(to_sqlite_err)?,
                        last_error: row.get(10)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn upsert_cached_user(&self, user: &CachedUserRow) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO cached_users (
                sys_id, user_name, name, first_name, last_name, email, employee_number,
                active, department, location, title, raw_json, synced_at, sys_updated_on
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7,
                ?8, ?9, ?10, ?11, ?12, ?13, ?14
            )
            ON CONFLICT(sys_id) DO UPDATE SET
                user_name = excluded.user_name,
                name = excluded.name,
                first_name = excluded.first_name,
                last_name = excluded.last_name,
                email = excluded.email,
                employee_number = excluded.employee_number,
                active = excluded.active,
                department = excluded.department,
                location = excluded.location,
                title = excluded.title,
                raw_json = excluded.raw_json,
                synced_at = excluded.synced_at,
                sys_updated_on = excluded.sys_updated_on
            "#,
            params![
                &user.sys_id,
                &user.user_name,
                &user.name,
                &user.first_name,
                &user.last_name,
                &user.email,
                &user.employee_number,
                user.active.map(bool_to_i64),
                &user.department,
                &user.location,
                &user.title,
                &user.raw_json,
                to_ts(user.synced_at),
                &user.sys_updated_on,
            ],
        )?;
        Ok(())
    }

    pub fn get_cached_user(&self, sys_id: &str) -> Result<Option<CachedUserRow>> {
        self.conn
            .query_row(
                r#"
                SELECT sys_id, user_name, name, first_name, last_name, email, employee_number,
                       active, department, location, title, raw_json, synced_at, sys_updated_on
                FROM cached_users
                WHERE sys_id = ?1
                "#,
                params![sys_id],
                row_to_cached_user,
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn list_cached_users_by_sys_ids(&self, sys_ids: &[String]) -> Result<Vec<CachedUserRow>> {
        let mut users = Vec::new();
        for sys_id in sys_ids {
            if let Some(user) = self.get_cached_user(sys_id)? {
                users.push(user);
            }
        }
        Ok(users)
    }

    pub fn put_cached_user_query_result(
        &self,
        query_key: &str,
        result_sys_ids: &[String],
        synced_at: DateTime<Utc>,
    ) -> Result<CachedUserQueryRow> {
        self.put_cached_user_query_result_with_ttl(
            query_key,
            result_sys_ids,
            synced_at,
            cached_user_query_ttl(),
        )
    }

    pub fn put_cached_user_query_result_with_ttl(
        &self,
        query_key: &str,
        result_sys_ids: &[String],
        synced_at: DateTime<Utc>,
        ttl: Duration,
    ) -> Result<CachedUserQueryRow> {
        let row = CachedUserQueryRow {
            query_key: query_key.to_string(),
            result_sys_ids: result_sys_ids.to_vec(),
            synced_at,
            expires_at: synced_at + ttl,
        };
        self.upsert_cached_user_query_result(&row)?;
        Ok(row)
    }

    pub fn upsert_cached_user_query_result(&self, row: &CachedUserQueryRow) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO cached_user_queries (
                query_key, result_sys_ids_json, synced_at, expires_at
            ) VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(query_key) DO UPDATE SET
                result_sys_ids_json = excluded.result_sys_ids_json,
                synced_at = excluded.synced_at,
                expires_at = excluded.expires_at
            "#,
            params![
                &row.query_key,
                serde_json::to_string(&row.result_sys_ids)?,
                to_ts(row.synced_at),
                to_ts(row.expires_at),
            ],
        )?;
        Ok(())
    }

    pub fn get_cached_user_query_result(
        &self,
        query_key: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<CachedUserQueryRow>> {
        let row = self
            .conn
            .query_row(
                r#"
                SELECT query_key, result_sys_ids_json, synced_at, expires_at
                FROM cached_user_queries
                WHERE query_key = ?1
                "#,
                params![query_key],
                row_to_cached_user_query,
            )
            .optional()?;
        Ok(row.filter(|row| row.expires_at > now))
    }

    fn upsert_business_application_projection_row(
        &self,
        projection: &BusinessApplicationProjectionRow,
    ) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO business_applications (
                record_sys_id, name, number, business_owner_sys_id, business_owner_name,
                is_owner_sys_id, is_owner_name, ci_owner_group_sys_id, ci_owner_group_name,
                primary_support_group_sys_id, primary_support_group_name,
                operational_status_value, operational_status_display,
                primary_portfolio_sys_id, primary_portfolio_name, primary_portfolio_table,
                attested_date, sys_updated_on, field_count, reference_count,
                unresolved_reference_count
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11,
                ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21
            )
            ON CONFLICT(record_sys_id) DO UPDATE SET
                name = excluded.name,
                number = excluded.number,
                business_owner_sys_id = excluded.business_owner_sys_id,
                business_owner_name = excluded.business_owner_name,
                is_owner_sys_id = excluded.is_owner_sys_id,
                is_owner_name = excluded.is_owner_name,
                ci_owner_group_sys_id = excluded.ci_owner_group_sys_id,
                ci_owner_group_name = excluded.ci_owner_group_name,
                primary_support_group_sys_id = excluded.primary_support_group_sys_id,
                primary_support_group_name = excluded.primary_support_group_name,
                operational_status_value = excluded.operational_status_value,
                operational_status_display = excluded.operational_status_display,
                primary_portfolio_sys_id = excluded.primary_portfolio_sys_id,
                primary_portfolio_name = excluded.primary_portfolio_name,
                primary_portfolio_table = excluded.primary_portfolio_table,
                attested_date = excluded.attested_date,
                sys_updated_on = excluded.sys_updated_on,
                field_count = excluded.field_count,
                reference_count = excluded.reference_count,
                unresolved_reference_count = excluded.unresolved_reference_count
            "#,
            params![
                &projection.record_sys_id,
                &projection.name,
                &projection.number,
                &projection.business_owner_sys_id,
                &projection.business_owner_name,
                &projection.is_owner_sys_id,
                &projection.is_owner_name,
                &projection.ci_owner_group_sys_id,
                &projection.ci_owner_group_name,
                &projection.primary_support_group_sys_id,
                &projection.primary_support_group_name,
                &projection.operational_status_value,
                &projection.operational_status_display,
                &projection.primary_portfolio_sys_id,
                &projection.primary_portfolio_name,
                &projection.primary_portfolio_table,
                &projection.attested_date,
                &projection.sys_updated_on,
                projection.field_count as i64,
                projection.reference_count as i64,
                projection.unresolved_reference_count as i64,
            ],
        )?;
        Ok(())
    }

    fn upsert_business_application_field(&self, field: &ProjectedFieldRow) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO business_application_fields (
                record_sys_id, field_name, field_label, field_type, value_text,
                display_value, value_number, value_date, value_bool, reference_sys_id,
                reference_table, raw_json, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
            ON CONFLICT(record_sys_id, field_name) DO UPDATE SET
                field_label = excluded.field_label,
                field_type = excluded.field_type,
                value_text = excluded.value_text,
                display_value = excluded.display_value,
                value_number = excluded.value_number,
                value_date = excluded.value_date,
                value_bool = excluded.value_bool,
                reference_sys_id = excluded.reference_sys_id,
                reference_table = excluded.reference_table,
                raw_json = excluded.raw_json,
                updated_at = excluded.updated_at
            "#,
            params![
                &field.owner_sys_id,
                &field.field_name,
                &field.field_label,
                &field.field_type,
                &field.value_text,
                &field.display_value,
                &field.value_number,
                &field.value_date,
                field.value_bool.map(bool_to_i64),
                &field.reference_sys_id,
                &field.reference_table,
                &field.raw_json,
                to_ts(field.updated_at),
            ],
        )?;
        Ok(())
    }

    pub fn replace_tags(&self, record_sys_id: &str, tags: &[TagRow]) -> Result<()> {
        self.conn.execute(
            "DELETE FROM record_tags WHERE record_sys_id = ?1",
            params![record_sys_id],
        )?;
        for tag in tags {
            self.conn.execute(
                r#"
                INSERT INTO record_tags (record_sys_id, tag, source, weight)
                VALUES (?1, ?2, ?3, ?4)
                ON CONFLICT(record_sys_id, tag) DO UPDATE SET
                    source = excluded.source,
                    weight = excluded.weight
                "#,
                params![&tag.record_sys_id, &tag.tag, &tag.source, tag.weight],
            )?;
        }
        Ok(())
    }

    pub fn list_tags(&self, record_sys_id: &str) -> Result<Vec<TagRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT record_sys_id, tag, source, weight
            FROM record_tags
            WHERE record_sys_id = ?1
            ORDER BY tag
            "#,
        )?;
        let rows = stmt.query_map(params![record_sys_id], |row| {
            Ok(TagRow {
                record_sys_id: row.get(0)?,
                tag: row.get(1)?,
                source: row.get(2)?,
                weight: row.get(3)?,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn replace_keywords(&self, record_sys_id: &str, keywords: &[KeywordRow]) -> Result<()> {
        self.conn.execute(
            "DELETE FROM record_keywords WHERE record_sys_id = ?1",
            params![record_sys_id],
        )?;
        for keyword in keywords {
            self.conn.execute(
                r#"
                INSERT INTO record_keywords (record_sys_id, keyword, source, weight)
                VALUES (?1, ?2, ?3, ?4)
                ON CONFLICT(record_sys_id, keyword) DO UPDATE SET
                    source = excluded.source,
                    weight = excluded.weight
                "#,
                params![
                    &keyword.record_sys_id,
                    &keyword.keyword,
                    &keyword.source,
                    keyword.weight
                ],
            )?;
        }
        Ok(())
    }

    pub fn list_keywords(&self, record_sys_id: &str) -> Result<Vec<KeywordRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT record_sys_id, keyword, source, weight
            FROM record_keywords
            WHERE record_sys_id = ?1
            ORDER BY keyword
            "#,
        )?;
        let rows = stmt.query_map(params![record_sys_id], |row| {
            Ok(KeywordRow {
                record_sys_id: row.get(0)?,
                keyword: row.get(1)?,
                source: row.get(2)?,
                weight: row.get(3)?,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn replace_aliases(&self, record_sys_id: &str, aliases: &[AliasRow]) -> Result<()> {
        self.conn.execute(
            "DELETE FROM record_aliases WHERE record_sys_id = ?1",
            params![record_sys_id],
        )?;
        for alias in aliases {
            self.conn.execute(
                r#"
                INSERT INTO record_aliases (record_sys_id, alias, kind, source)
                VALUES (?1, ?2, ?3, ?4)
                ON CONFLICT(record_sys_id, alias) DO UPDATE SET
                    kind = excluded.kind,
                    source = excluded.source
                "#,
                params![
                    &alias.record_sys_id,
                    &alias.alias,
                    &alias.kind,
                    &alias.source
                ],
            )?;
        }
        Ok(())
    }

    pub fn list_aliases(&self, record_sys_id: &str) -> Result<Vec<AliasRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT record_sys_id, alias, kind, source
            FROM record_aliases
            WHERE record_sys_id = ?1
            ORDER BY alias
            "#,
        )?;
        let rows = stmt.query_map(params![record_sys_id], |row| {
            Ok(AliasRow {
                record_sys_id: row.get(0)?,
                alias: row.get(1)?,
                kind: row.get(2)?,
                source: row.get(3)?,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn upsert_knowledge_article(&self, article: &KnowledgeArticleRow) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO knowledge_articles (
                record_sys_id, number, title, workflow_state, knowledge_base_sys_id,
                knowledge_base_name, category_sys_id, category_name, author_sys_id,
                author_name, published_at, valid_to, article_type, sys_updated_on,
                sn_tags, auto_tags, user_tags, body_cached
            ) VALUES (
                ?1, ?2, ?3, ?4, ?5,
                ?6, ?7, ?8, ?9,
                ?10, ?11, ?12, ?13, ?14,
                ?15, ?16, ?17, ?18
            )
            ON CONFLICT(record_sys_id) DO UPDATE SET
                number = excluded.number,
                title = excluded.title,
                workflow_state = excluded.workflow_state,
                knowledge_base_sys_id = excluded.knowledge_base_sys_id,
                knowledge_base_name = excluded.knowledge_base_name,
                category_sys_id = excluded.category_sys_id,
                category_name = excluded.category_name,
                author_sys_id = excluded.author_sys_id,
                author_name = excluded.author_name,
                published_at = excluded.published_at,
                valid_to = excluded.valid_to,
                article_type = excluded.article_type,
                sys_updated_on = excluded.sys_updated_on,
                sn_tags = excluded.sn_tags,
                auto_tags = excluded.auto_tags,
                user_tags = excluded.user_tags,
                body_cached = excluded.body_cached
            "#,
            params![
                &article.record_sys_id,
                &article.number,
                &article.title,
                &article.workflow_state,
                &article.knowledge_base_sys_id,
                &article.knowledge_base_name,
                &article.category_sys_id,
                &article.category_name,
                &article.author_sys_id,
                &article.author_name,
                &article.published_at,
                &article.valid_to,
                &article.article_type,
                &article.sys_updated_on,
                serde_json::to_string(&article.sn_tags)?,
                serde_json::to_string(&article.auto_tags)?,
                serde_json::to_string(&article.user_tags)?,
                bool_to_i64(article.body_cached),
            ],
        )?;
        Ok(())
    }

    pub fn get_knowledge_article(
        &self,
        record_sys_id: &str,
    ) -> Result<Option<KnowledgeArticleRow>> {
        self.conn
            .query_row(
                r#"
                SELECT record_sys_id, number, title, workflow_state, knowledge_base_sys_id,
                       knowledge_base_name, category_sys_id, category_name, author_sys_id,
                       author_name, published_at, valid_to, article_type, sys_updated_on,
                       sn_tags, auto_tags, user_tags, body_cached
                FROM knowledge_articles
                WHERE record_sys_id = ?1
                "#,
                params![record_sys_id],
                |row| {
                    Ok(KnowledgeArticleRow {
                        record_sys_id: row.get(0)?,
                        number: row.get(1)?,
                        title: row.get(2)?,
                        workflow_state: row.get(3)?,
                        knowledge_base_sys_id: row.get(4)?,
                        knowledge_base_name: row.get(5)?,
                        category_sys_id: row.get(6)?,
                        category_name: row.get(7)?,
                        author_sys_id: row.get(8)?,
                        author_name: row.get(9)?,
                        published_at: row.get(10)?,
                        valid_to: row.get(11)?,
                        article_type: row.get(12)?,
                        sys_updated_on: row.get(13)?,
                        sn_tags: parse_string_vec_column(row, 14)?,
                        auto_tags: parse_string_vec_column(row, 15)?,
                        user_tags: parse_string_vec_column(row, 16)?,
                        body_cached: i64_to_bool(row.get(17)?),
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn list_knowledge_bases(&self) -> Result<Vec<KnowledgeBaseSummaryRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT ka.knowledge_base_sys_id, ka.knowledge_base_name, COUNT(*)
            FROM knowledge_articles ka
            INNER JOIN records r ON r.sys_id = ka.record_sys_id
            WHERE r.in_scope = 1
            GROUP BY knowledge_base_sys_id, knowledge_base_name
            ORDER BY knowledge_base_name, knowledge_base_sys_id
            "#,
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(KnowledgeBaseSummaryRow {
                knowledge_base_sys_id: row.get(0)?,
                knowledge_base_name: row.get(1)?,
                article_count: row.get::<_, i64>(2)? as usize,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn list_knowledge_categories(
        &self,
        knowledge_base_sys_id: &str,
    ) -> Result<Vec<KnowledgeCategorySummaryRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT ka.category_sys_id, ka.category_name, ka.knowledge_base_sys_id, COUNT(*)
            FROM knowledge_articles ka
            INNER JOIN records r ON r.sys_id = ka.record_sys_id
            WHERE ka.knowledge_base_sys_id = ?1
              AND r.in_scope = 1
            GROUP BY category_sys_id, category_name, knowledge_base_sys_id
            ORDER BY category_name, category_sys_id
            "#,
        )?;
        let rows = stmt.query_map(params![knowledge_base_sys_id], |row| {
            Ok(KnowledgeCategorySummaryRow {
                category_sys_id: row.get(0)?,
                category_name: row.get(1)?,
                knowledge_base_sys_id: row.get(2)?,
                article_count: row.get::<_, i64>(3)? as usize,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn count_knowledge_articles(&self) -> Result<usize> {
        self.conn
            .query_row(
                r#"
                SELECT COUNT(*)
                FROM knowledge_articles ka
                INNER JOIN records r ON r.sys_id = ka.record_sys_id
                WHERE r.in_scope = 1
                "#,
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|count| count as usize)
            .map_err(Into::into)
    }

    pub fn count_knowledge_articles_with_cached_body(&self) -> Result<usize> {
        self.conn
            .query_row(
                r#"
                SELECT COUNT(*)
                FROM knowledge_articles ka
                INNER JOIN records r ON r.sys_id = ka.record_sys_id
                WHERE r.in_scope = 1
                  AND ka.body_cached = 1
                "#,
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|count| count as usize)
            .map_err(Into::into)
    }

    pub fn upsert_knowledge_embedding(&self, row: &KnowledgeEmbeddingRow) -> Result<()> {
        let vector_blob = encode_embedding_vector(&row.vector)?;
        self.conn.execute(
            r#"
            INSERT INTO knowledge_article_embeddings (
                record_sys_id, model, provider, dimensions, coverage, content_hash,
                vector_blob, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            ON CONFLICT(record_sys_id) DO UPDATE SET
                model = excluded.model,
                provider = excluded.provider,
                dimensions = excluded.dimensions,
                coverage = excluded.coverage,
                content_hash = excluded.content_hash,
                vector_blob = excluded.vector_blob,
                updated_at = excluded.updated_at
            "#,
            params![
                &row.record_sys_id,
                &row.model,
                &row.provider,
                row.dimensions as i64,
                coverage_to_str(row.coverage),
                &row.content_hash,
                vector_blob,
                to_ts(row.updated_at),
            ],
        )?;
        Ok(())
    }

    pub fn get_knowledge_embedding(
        &self,
        record_sys_id: &str,
    ) -> Result<Option<KnowledgeEmbeddingRow>> {
        self.conn
            .query_row(
                r#"
                SELECT record_sys_id, model, provider, dimensions, coverage, content_hash,
                       vector_blob, updated_at
                FROM knowledge_article_embeddings
                WHERE record_sys_id = ?1
                "#,
                params![record_sys_id],
                |row| {
                    let dimensions = row.get::<_, i64>(3)? as usize;
                    let blob: Vec<u8> = row.get(6)?;
                    Ok(KnowledgeEmbeddingRow {
                        record_sys_id: row.get(0)?,
                        model: row.get(1)?,
                        provider: row.get(2)?,
                        dimensions,
                        coverage: coverage_from_str(&row.get::<_, String>(4)?)
                            .map_err(to_sqlite_err)?,
                        content_hash: row.get(5)?,
                        vector: decode_embedding_vector(&blob, dimensions)
                            .map_err(to_sqlite_err)?,
                        updated_at: from_ts(row.get(7)?).map_err(to_sqlite_err)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn list_knowledge_embeddings(&self) -> Result<Vec<KnowledgeEmbeddingRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT record_sys_id, model, provider, dimensions, coverage, content_hash,
                   vector_blob, updated_at
            FROM knowledge_article_embeddings
            ORDER BY record_sys_id
            "#,
        )?;
        let rows = stmt.query_map([], |row| {
            let dimensions = row.get::<_, i64>(3)? as usize;
            let blob: Vec<u8> = row.get(6)?;
            Ok(KnowledgeEmbeddingRow {
                record_sys_id: row.get(0)?,
                model: row.get(1)?,
                provider: row.get(2)?,
                dimensions,
                coverage: coverage_from_str(&row.get::<_, String>(4)?).map_err(to_sqlite_err)?,
                content_hash: row.get(5)?,
                vector: decode_embedding_vector(&blob, dimensions).map_err(to_sqlite_err)?,
                updated_at: from_ts(row.get(7)?).map_err(to_sqlite_err)?,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn delete_knowledge_embedding(&self, record_sys_id: &str) -> Result<()> {
        self.conn.execute(
            "DELETE FROM knowledge_article_embeddings WHERE record_sys_id = ?1",
            params![record_sys_id],
        )?;
        Ok(())
    }

    pub fn prune_orphan_knowledge_embeddings(&self) -> Result<usize> {
        let removed = self.conn.execute(
            r#"
            DELETE FROM knowledge_article_embeddings
            WHERE NOT EXISTS (
                SELECT 1
                FROM knowledge_articles ka
                WHERE ka.record_sys_id = knowledge_article_embeddings.record_sys_id
            )
            "#,
            [],
        )?;
        Ok(removed)
    }

    pub fn count_knowledge_embeddings_by_coverage(
        &self,
        model: &str,
        coverage: KnowledgeEmbeddingCoverage,
    ) -> Result<usize> {
        self.conn
            .query_row(
                r#"
                SELECT COUNT(*)
                FROM knowledge_article_embeddings
                WHERE model = ?1
                  AND coverage = ?2
                "#,
                params![model, coverage_to_str(coverage)],
                |row| row.get::<_, i64>(0),
            )
            .map(|count| count as usize)
            .map_err(Into::into)
    }

    pub fn count_orphan_knowledge_embeddings(&self) -> Result<usize> {
        self.conn
            .query_row(
                r#"
                SELECT COUNT(*)
                FROM knowledge_article_embeddings kae
                LEFT JOIN knowledge_articles ka
                  ON ka.record_sys_id = kae.record_sys_id
                WHERE ka.record_sys_id IS NULL
                "#,
                [],
                |row| row.get::<_, i64>(0),
            )
            .map(|count| count as usize)
            .map_err(Into::into)
    }

    pub fn knowledge_semantic_meta(&self) -> Result<KnowledgeSemanticMeta> {
        let last_rebuild_at = self
            .get_meta_value("kb_semantic_last_rebuild_at")?
            .map(|value| {
                value
                    .parse::<i64>()
                    .map_err(|_| StoreError::InvalidSchemaVersion(value.clone()))
                    .and_then(from_ts)
            })
            .transpose()?;
        let last_error = self.get_meta_value("kb_semantic_last_error")?;
        Ok(KnowledgeSemanticMeta {
            last_rebuild_at,
            last_error,
        })
    }

    pub fn set_knowledge_semantic_meta(
        &self,
        last_rebuild_at: Option<DateTime<Utc>>,
        last_error: Option<&str>,
    ) -> Result<()> {
        self.set_meta_value(
            "kb_semantic_last_rebuild_at",
            last_rebuild_at
                .map(|value| value.timestamp().to_string())
                .as_deref(),
        )?;
        self.set_meta_value("kb_semantic_last_error", last_error)?;
        Ok(())
    }

    pub fn list_active_knowledge_index_rows(&self) -> Result<Vec<KnowledgeIndexRow>> {
        self.list_active_knowledge_index_rows_filtered(None)
    }

    pub fn list_active_knowledge_index_rows_for_base(
        &self,
        knowledge_base_sys_id: &str,
    ) -> Result<Vec<KnowledgeIndexRow>> {
        self.list_active_knowledge_index_rows_filtered(Some(knowledge_base_sys_id))
    }

    fn list_active_knowledge_index_rows_filtered(
        &self,
        knowledge_base_sys_id: Option<&str>,
    ) -> Result<Vec<KnowledgeIndexRow>> {
        let (sql, params_vec) = if let Some(knowledge_base_sys_id) = knowledge_base_sys_id {
            (
                r#"
                SELECT ka.record_sys_id, ka.number, ka.title, ka.knowledge_base_sys_id,
                       ka.knowledge_base_name, ka.category_sys_id, ka.category_name, r.file_path,
                       ka.sn_tags, ka.auto_tags, ka.user_tags
                FROM knowledge_articles ka
                INNER JOIN records r ON r.sys_id = ka.record_sys_id
                WHERE r.in_scope = 1
                  AND r.file_path IS NOT NULL
                  AND ka.knowledge_base_sys_id = ?1
                ORDER BY ka.knowledge_base_name, ka.category_name, ka.title, ka.number
                "#,
                vec![knowledge_base_sys_id.to_string()],
            )
        } else {
            (
                r#"
                SELECT ka.record_sys_id, ka.number, ka.title, ka.knowledge_base_sys_id,
                       ka.knowledge_base_name, ka.category_sys_id, ka.category_name, r.file_path,
                       ka.sn_tags, ka.auto_tags, ka.user_tags
                FROM knowledge_articles ka
                INNER JOIN records r ON r.sys_id = ka.record_sys_id
                WHERE r.in_scope = 1
                  AND r.file_path IS NOT NULL
                ORDER BY ka.knowledge_base_name, ka.category_name, ka.title, ka.number
                "#,
                Vec::new(),
            )
        };

        let mut stmt = self.conn.prepare(sql)?;
        if let Some(value) = params_vec.first() {
            let rows = stmt.query_map(params![value], |row| {
                Ok(KnowledgeIndexRow {
                    record_sys_id: row.get(0)?,
                    number: row.get(1)?,
                    title: row.get(2)?,
                    knowledge_base_sys_id: row.get(3)?,
                    knowledge_base_name: row.get(4)?,
                    category_sys_id: row.get(5)?,
                    category_name: row.get(6)?,
                    file_path: row.get(7)?,
                    sn_tags: parse_string_vec_column(row, 8)?,
                    auto_tags: parse_string_vec_column(row, 9)?,
                    user_tags: parse_string_vec_column(row, 10)?,
                })
            })?;
            Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
        } else {
            let rows = stmt.query_map([], |row| {
                Ok(KnowledgeIndexRow {
                    record_sys_id: row.get(0)?,
                    number: row.get(1)?,
                    title: row.get(2)?,
                    knowledge_base_sys_id: row.get(3)?,
                    knowledge_base_name: row.get(4)?,
                    category_sys_id: row.get(5)?,
                    category_name: row.get(6)?,
                    file_path: row.get(7)?,
                    sn_tags: parse_string_vec_column(row, 8)?,
                    auto_tags: parse_string_vec_column(row, 9)?,
                    user_tags: parse_string_vec_column(row, 10)?,
                })
            })?;
            Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
        }
    }

    pub fn list_active_knowledge_local_scan_rows(&self) -> Result<Vec<KnowledgeLocalScanRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT r.sys_id, r.number, r.file_path, l.modified_at_ms
            FROM records r
            LEFT JOIN kb_local_file_state l ON l.record_sys_id = r.sys_id
            WHERE r.in_scope = 1
              AND r.resource_type = 'kb_knowledge'
              AND r.file_path IS NOT NULL
            ORDER BY r.number
            "#,
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(KnowledgeLocalScanRow {
                record_sys_id: row.get(0)?,
                number: row.get(1)?,
                file_path: row.get(2)?,
                modified_at_ms: row.get(3)?,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn update_knowledge_local_state(
        &self,
        record_sys_id: &str,
        user_tags: &[String],
        body_cached: bool,
    ) -> Result<()> {
        self.conn.execute(
            r#"
            UPDATE knowledge_articles
            SET user_tags = ?2,
                body_cached = ?3
            WHERE record_sys_id = ?1
            "#,
            params![
                record_sys_id,
                serde_json::to_string(user_tags)?,
                bool_to_i64(body_cached),
            ],
        )?;
        Ok(())
    }

    pub fn load_kb_term_stats(&self) -> Result<HashMap<String, usize>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT term, doc_freq
            FROM kb_term_stats
            ORDER BY term
            "#,
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)? as usize))
        })?;
        let mut stats = HashMap::new();
        for row in rows {
            let (term, doc_freq) = row?;
            stats.insert(term, doc_freq);
        }
        Ok(stats)
    }

    pub fn replace_kb_term_stats(&self, stats: &HashMap<String, usize>) -> Result<()> {
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<()> {
            self.conn.execute("DELETE FROM kb_term_stats", [])?;
            for (term, doc_freq) in stats {
                self.conn.execute(
                    r#"
                    INSERT INTO kb_term_stats (term, doc_freq)
                    VALUES (?1, ?2)
                    "#,
                    params![term, *doc_freq as i64],
                )?;
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    pub fn get_kb_article_terms(&self, record_sys_id: &str) -> Result<Vec<String>> {
        self.conn
            .query_row(
                r#"
                SELECT terms_json
                FROM kb_article_terms
                WHERE record_sys_id = ?1
                "#,
                params![record_sys_id],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|raw| serde_json::from_str::<Vec<String>>(&raw).map_err(Into::into))
            .transpose()
            .map(|terms| terms.unwrap_or_default())
    }

    pub fn replace_all_kb_article_terms(&self, entries: &[(String, Vec<String>)]) -> Result<()> {
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<()> {
            self.conn.execute("DELETE FROM kb_article_terms", [])?;
            for (record_sys_id, terms) in dedupe_kb_term_entries(entries) {
                self.conn.execute(
                    r#"
                    INSERT INTO kb_article_terms (record_sys_id, terms_json)
                    VALUES (?1, ?2)
                    "#,
                    params![record_sys_id, serde_json::to_string(&terms)?],
                )?;
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    pub fn replace_kb_article_terms_entries(
        &self,
        entries: &[(String, Vec<String>)],
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<()> {
            for (record_sys_id, terms) in dedupe_kb_term_entries(entries) {
                self.conn.execute(
                    r#"
                    INSERT INTO kb_article_terms (record_sys_id, terms_json)
                    VALUES (?1, ?2)
                    ON CONFLICT(record_sys_id) DO UPDATE SET
                        terms_json = excluded.terms_json
                    "#,
                    params![record_sys_id, serde_json::to_string(&terms)?],
                )?;
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    pub fn delete_kb_article_terms(&self, record_sys_ids: &[String]) -> Result<()> {
        if record_sys_ids.is_empty() {
            return Ok(());
        }
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<()> {
            for record_sys_id in record_sys_ids {
                self.conn.execute(
                    "DELETE FROM kb_article_terms WHERE record_sys_id = ?1",
                    params![record_sys_id],
                )?;
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    pub fn upsert_kb_local_file_states(&self, entries: &[(String, String, i64)]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        self.conn.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<()> {
            for (record_sys_id, file_path, modified_at_ms) in entries {
                self.conn.execute(
                    r#"
                    INSERT INTO kb_local_file_state (record_sys_id, file_path, modified_at_ms)
                    VALUES (?1, ?2, ?3)
                    ON CONFLICT(record_sys_id) DO UPDATE SET
                        file_path = excluded.file_path,
                        modified_at_ms = excluded.modified_at_ms
                    "#,
                    params![record_sys_id, file_path, modified_at_ms],
                )?;
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                self.conn.execute_batch("COMMIT")?;
                Ok(())
            }
            Err(err) => {
                let _ = self.conn.execute_batch("ROLLBACK");
                Err(err)
            }
        }
    }

    pub fn get_kb_sync_state(&self) -> Result<KbSyncStateRow> {
        self.conn
            .query_row(
                r#"
                SELECT last_full_at, last_incr_at, watermark_updated_at, watermark_sys_id, kb_sync_lock
                FROM kb_sync_state
                WHERE id = 1
                "#,
                [],
                |row| {
                    Ok(KbSyncStateRow {
                        last_full_at: row
                            .get::<_, Option<i64>>(0)?
                            .map(from_ts)
                            .transpose()
                            .map_err(to_sqlite_err)?,
                        last_incr_at: row
                            .get::<_, Option<i64>>(1)?
                            .map(from_ts)
                            .transpose()
                            .map_err(to_sqlite_err)?,
                        watermark_updated_at: row.get(2)?,
                        watermark_sys_id: row.get(3)?,
                        kb_sync_lock: row.get(4)?,
                    })
                },
            )
            .map_err(Into::into)
    }

    pub fn set_kb_sync_state(&self, row: &KbSyncStateRow) -> Result<()> {
        self.conn.execute(
            r#"
            INSERT INTO kb_sync_state (
                id, last_full_at, last_incr_at, watermark_updated_at, watermark_sys_id, kb_sync_lock
            ) VALUES (1, ?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(id) DO UPDATE SET
                last_full_at = excluded.last_full_at,
                last_incr_at = excluded.last_incr_at,
                watermark_updated_at = excluded.watermark_updated_at,
                watermark_sys_id = excluded.watermark_sys_id,
                kb_sync_lock = excluded.kb_sync_lock
            "#,
            params![
                opt_ts(row.last_full_at),
                opt_ts(row.last_incr_at),
                &row.watermark_updated_at,
                &row.watermark_sys_id,
                &row.kb_sync_lock,
            ],
        )?;
        Ok(())
    }

    pub fn acquire_kb_sync_lock(&self, now_ms: i64, stale_after_ms: i64) -> Result<bool> {
        self.conn
            .execute("INSERT OR IGNORE INTO kb_sync_state (id) VALUES (1)", [])?;
        let changed = self.conn.execute(
            r#"
            UPDATE kb_sync_state
            SET kb_sync_lock = ?1
            WHERE id = 1
              AND (kb_sync_lock IS NULL OR kb_sync_lock < ?2)
            "#,
            params![now_ms, now_ms - stale_after_ms],
        )?;
        Ok(changed > 0)
    }

    pub fn release_kb_sync_lock(&self) -> Result<()> {
        self.conn.execute(
            "UPDATE kb_sync_state SET kb_sync_lock = NULL WHERE id = 1",
            [],
        )?;
        Ok(())
    }

    pub fn list_knowledge_tags(
        &self,
        layer: &str,
        min_count: usize,
    ) -> Result<Vec<KnowledgeTagCountRow>> {
        let (sql, params_vec) = match layer {
            "sn" => (
                r#"
                SELECT tag, 'sn' AS layer, COUNT(*) AS article_count
                FROM (
                    SELECT ka.record_sys_id, LOWER(TRIM(json_each.value)) AS tag
                    FROM knowledge_articles ka
                    INNER JOIN records r ON r.sys_id = ka.record_sys_id
                    INNER JOIN json_each(ka.sn_tags)
                    WHERE r.in_scope = 1
                )
                WHERE tag <> ''
                GROUP BY tag
                HAVING COUNT(*) >= ?1
                ORDER BY article_count DESC, tag ASC
                "#,
                vec![min_count as i64],
            ),
            "auto" => (
                r#"
                SELECT tag, 'auto' AS layer, COUNT(*) AS article_count
                FROM (
                    SELECT ka.record_sys_id, LOWER(TRIM(json_each.value)) AS tag
                    FROM knowledge_articles ka
                    INNER JOIN records r ON r.sys_id = ka.record_sys_id
                    INNER JOIN json_each(ka.auto_tags)
                    WHERE r.in_scope = 1
                )
                WHERE tag <> ''
                GROUP BY tag
                HAVING COUNT(*) >= ?1
                ORDER BY article_count DESC, tag ASC
                "#,
                vec![min_count as i64],
            ),
            "user" => (
                r#"
                SELECT tag, 'user' AS layer, COUNT(*) AS article_count
                FROM (
                    SELECT ka.record_sys_id, LOWER(TRIM(json_each.value)) AS tag
                    FROM knowledge_articles ka
                    INNER JOIN records r ON r.sys_id = ka.record_sys_id
                    INNER JOIN json_each(ka.user_tags)
                    WHERE r.in_scope = 1
                )
                WHERE tag <> ''
                GROUP BY tag
                HAVING COUNT(*) >= ?1
                ORDER BY article_count DESC, tag ASC
                "#,
                vec![min_count as i64],
            ),
            _ => (
                r#"
                SELECT tag, 'all' AS layer, COUNT(*) AS article_count
                FROM (
                    SELECT ka.record_sys_id, LOWER(TRIM(json_each.value)) AS tag
                    FROM knowledge_articles ka
                    INNER JOIN records r ON r.sys_id = ka.record_sys_id
                    INNER JOIN json_each(ka.sn_tags)
                    WHERE r.in_scope = 1
                    UNION
                    SELECT ka.record_sys_id, LOWER(TRIM(json_each.value)) AS tag
                    FROM knowledge_articles ka
                    INNER JOIN records r ON r.sys_id = ka.record_sys_id
                    INNER JOIN json_each(ka.auto_tags)
                    WHERE r.in_scope = 1
                    UNION
                    SELECT ka.record_sys_id, LOWER(TRIM(json_each.value)) AS tag
                    FROM knowledge_articles ka
                    INNER JOIN records r ON r.sys_id = ka.record_sys_id
                    INNER JOIN json_each(ka.user_tags)
                    WHERE r.in_scope = 1
                )
                WHERE tag <> ''
                GROUP BY tag
                HAVING COUNT(*) >= ?1
                ORDER BY article_count DESC, tag ASC
                "#,
                vec![min_count as i64],
            ),
        };

        let mut stmt = self.conn.prepare(sql)?;
        let rows = stmt.query_map(params![params_vec[0]], |row| {
            Ok(KnowledgeTagCountRow {
                tag: row.get(0)?,
                layer: row.get(1)?,
                article_count: row.get::<_, i64>(2)? as usize,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn find_record_sys_ids_by_tag(&self, tag: &str, limit: usize) -> Result<Vec<String>> {
        self.find_record_sys_ids_by_enrichment("record_tags", "tag", tag, limit)
    }

    pub fn find_record_sys_ids_by_keyword(
        &self,
        keyword: &str,
        limit: usize,
    ) -> Result<Vec<String>> {
        self.find_record_sys_ids_by_enrichment("record_keywords", "keyword", keyword, limit)
    }

    pub fn find_record_sys_ids_by_alias(&self, alias: &str, limit: usize) -> Result<Vec<String>> {
        self.find_record_sys_ids_by_enrichment("record_aliases", "alias", alias, limit)
    }

    pub fn get_sync_state(&self, resource_type: &str) -> Result<Option<SyncStateRow>> {
        self.conn
            .query_row(
                r#"
                SELECT resource_type, last_full, last_incr, high_watermark, cursor, filter_hash
                FROM sync_state
                WHERE resource_type = ?1
                "#,
                params![resource_type],
                |row| {
                    Ok(SyncStateRow {
                        resource_type: row.get(0)?,
                        last_full: row
                            .get::<_, Option<i64>>(1)?
                            .map(from_ts)
                            .transpose()
                            .map_err(to_sqlite_err)?,
                        last_incr: row
                            .get::<_, Option<i64>>(2)?
                            .map(from_ts)
                            .transpose()
                            .map_err(to_sqlite_err)?,
                        high_watermark: row
                            .get::<_, Option<i64>>(3)?
                            .map(from_ts)
                            .transpose()
                            .map_err(to_sqlite_err)?,
                        cursor: row.get(4)?,
                        filter_hash: row.get(5)?,
                    })
                },
            )
            .optional()
            .map_err(Into::into)
    }

    pub fn tombstone_record(&self, sys_id: &str, when: DateTime<Utc>) -> Result<()> {
        self.conn.execute(
            r#"
            UPDATE records
            SET in_scope = 0,
                tombstoned_at = ?2,
                last_seen_at = ?2
            WHERE sys_id = ?1
            "#,
            params![sys_id, to_ts(when)],
        )?;
        Ok(())
    }

    pub fn prune_record(&self, sys_id: &str, when: DateTime<Utc>) -> Result<()> {
        self.conn.execute(
            r#"
            UPDATE records
            SET pruned_at = ?2
            WHERE sys_id = ?1
            "#,
            params![sys_id, to_ts(when)],
        )?;
        self.conn.execute(
            "DELETE FROM fts_records WHERE rowid = (SELECT rowid FROM records WHERE sys_id = ?1)",
            params![sys_id],
        )?;
        self.conn.execute(
            "DELETE FROM relationships WHERE source_id = ?1 OR target_id = ?1",
            params![sys_id],
        )?;
        self.conn.execute(
            "DELETE FROM record_tags WHERE record_sys_id = ?1",
            params![sys_id],
        )?;
        self.conn.execute(
            "DELETE FROM record_keywords WHERE record_sys_id = ?1",
            params![sys_id],
        )?;
        self.conn.execute(
            "DELETE FROM record_aliases WHERE record_sys_id = ?1",
            params![sys_id],
        )?;
        self.conn.execute(
            "DELETE FROM knowledge_articles WHERE record_sys_id = ?1",
            params![sys_id],
        )?;
        self.conn.execute(
            "DELETE FROM kb_article_terms WHERE record_sys_id = ?1",
            params![sys_id],
        )?;
        self.conn.execute(
            "DELETE FROM kb_local_file_state WHERE record_sys_id = ?1",
            params![sys_id],
        )?;
        self.conn
            .execute("DELETE FROM records WHERE sys_id = ?1", params![sys_id])?;
        Ok(())
    }

    pub fn count_active_records(&self) -> Result<i64> {
        let count = self.conn.query_row(
            "SELECT COUNT(*) FROM records WHERE in_scope = 1",
            [],
            |row| row.get(0),
        )?;
        Ok(count)
    }

    pub fn search_fts(&self, query: &str, limit: usize) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT records.number
            FROM fts_records
            JOIN records ON records.rowid = fts_records.rowid
            WHERE fts_records MATCH ?1 AND records.in_scope = 1
            ORDER BY bm25(fts_records)
            LIMIT ?2
            "#,
        )?;
        let rows = stmt.query_map(params![query, limit as i64], |row| row.get::<_, String>(0))?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    pub fn query_business_application_records(
        &self,
        query: &crate::query::filter::BusinessApplicationQuery,
    ) -> Result<Vec<RecordRow>> {
        let limit = query.limit.unwrap_or(20);
        if limit > 500 {
            return Err(StoreError::InvalidQuery(
                "Business Application local query limit cannot exceed 500".to_string(),
            ));
        }

        for filter in &query.filters {
            if !query.allow_unknown_fields
                && !self.business_application_field_exists(&filter.field)?
            {
                return Err(StoreError::InvalidQuery(format!(
                    "unknown Business Application field `{}`",
                    filter.field
                )));
            }
        }
        for sort in &query.sort {
            if !query.allow_unknown_fields
                && !self.business_application_field_exists(&sort.field)?
            {
                return Err(StoreError::InvalidQuery(format!(
                    "unknown Business Application sort field `{}`",
                    sort.field
                )));
            }
        }

        let sql = if query.include_tombstoned {
            r#"
            SELECT sys_id, number, table_name, resource_type, state, short_desc, description,
                   assigned_to, parent_id, file_path, synced_at, sys_updated_on, etag,
                   in_scope, last_seen_at, tombstoned_at, pruned_at, raw_json
            FROM records
            WHERE resource_type IN ('business_application', 'cmdb_ci_business_app')
               OR table_name = 'cmdb_ci_business_app'
            "#
        } else {
            r#"
            SELECT sys_id, number, table_name, resource_type, state, short_desc, description,
                   assigned_to, parent_id, file_path, synced_at, sys_updated_on, etag,
                   in_scope, last_seen_at, tombstoned_at, pruned_at, raw_json
            FROM records
            WHERE in_scope = 1
              AND (resource_type IN ('business_application', 'cmdb_ci_business_app')
                   OR table_name = 'cmdb_ci_business_app')
            "#
        };
        let mut stmt = self.conn.prepare(sql)?;
        let rows = stmt.query_map([], row_to_record_row)?;
        let mut candidates = Vec::new();
        for row in rows {
            let row = row?;
            if self
                .list_business_application_fields(&row.sys_id)?
                .is_empty()
            {
                let _ = self.rebuild_business_application_projection_from_raw_json(&row)?;
            }
            let projection = self.get_business_application_projection(&row.sys_id)?;
            let fields = self
                .list_business_application_fields(&row.sys_id)?
                .into_iter()
                .map(|field| (field.field_name.clone(), field))
                .collect::<BTreeMap<_, _>>();
            if !business_application_matches_query(&row, projection.as_ref(), &fields, query) {
                continue;
            }
            candidates.push((row, projection, fields));
        }

        candidates.sort_by(|left, right| {
            compare_business_application_candidates(left, right, &query.sort)
                .then_with(|| {
                    left.1
                        .as_ref()
                        .map(|row| row.name.as_str())
                        .unwrap_or(left.0.short_desc.as_deref().unwrap_or(""))
                        .cmp(
                            right
                                .1
                                .as_ref()
                                .map(|row| row.name.as_str())
                                .unwrap_or(right.0.short_desc.as_deref().unwrap_or("")),
                        )
                })
                .then_with(|| left.0.sys_id.cmp(&right.0.sys_id))
        });

        Ok(candidates
            .into_iter()
            .skip(query.offset.unwrap_or(0))
            .take(limit)
            .map(|(row, _, _)| row)
            .collect())
    }

    fn find_record_sys_ids_by_enrichment(
        &self,
        table: &str,
        column: &str,
        value: &str,
        limit: usize,
    ) -> Result<Vec<String>> {
        let sql = format!(
            r#"
            SELECT DISTINCT records.sys_id
            FROM {table}
            INNER JOIN records ON records.sys_id = {table}.record_sys_id
            WHERE {table}.{column} = ?1
              AND records.in_scope = 1
            ORDER BY records.number, records.sys_id
            LIMIT ?2
            "#
        );

        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params![value, limit as i64], |row| row.get::<_, String>(0))?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    fn business_application_field_exists(&self, field: &str) -> Result<bool> {
        if matches!(
            field,
            "sys_id"
                | "number"
                | "name"
                | "short_description"
                | "description"
                | "business_owner"
                | "it_application_owner"
                | "managed_by_group"
                | "support_group"
                | "operational_status"
                | "portfolio"
                | "attested_date"
                | "sys_updated_on"
        ) {
            return Ok(true);
        }
        let count = self.conn.query_row(
            r#"
            SELECT
                (SELECT COUNT(*) FROM business_application_fields WHERE field_name = ?1)
              + (SELECT COUNT(*) FROM business_application_field_dictionary WHERE field_name = ?1)
            "#,
            params![field],
            |row| row.get::<_, i64>(0),
        )?;
        Ok(count > 0)
    }

    fn business_application_dictionary_map(
        &self,
    ) -> Result<HashMap<String, BusinessApplicationFieldDictionaryRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT table_name, field_name, field_label, field_type, reference_table, choice,
                   mandatory, read_only, max_length, active, synced_at, raw_json
            FROM business_application_field_dictionary
            WHERE table_name = 'cmdb_ci_business_app'
               OR table_name = 'task'
               OR table_name = 'cmdb_ci'
            "#,
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(BusinessApplicationFieldDictionaryRow {
                table_name: row.get(0)?,
                field_name: row.get(1)?,
                field_label: row.get(2)?,
                field_type: row.get(3)?,
                reference_table: row.get(4)?,
                choice: i64_to_bool(row.get(5)?),
                mandatory: i64_to_bool(row.get(6)?),
                read_only: i64_to_bool(row.get(7)?),
                max_length: row.get(8)?,
                active: i64_to_bool(row.get(9)?),
                synced_at: from_ts(row.get(10)?).map_err(to_sqlite_err)?,
                raw_json: row.get(11)?,
            })
        })?;
        let mut map = HashMap::new();
        for row in rows {
            let row = row?;
            map.insert(row.field_name.clone(), row);
        }
        Ok(map)
    }

    /// List all cached dictionary rows for a single table, ordered by field name.
    ///
    /// Returns an empty vector when the dictionary has never been synced for the
    /// table. Callers treat an empty result as a dictionary cache miss and fall
    /// back to baseline/observed behavior (degraded-read mode).
    pub fn list_business_application_field_dictionary(
        &self,
        table_name: &str,
    ) -> Result<Vec<BusinessApplicationFieldDictionaryRow>> {
        let mut stmt = self.conn.prepare(
            r#"
            SELECT table_name, field_name, field_label, field_type, reference_table, choice,
                   mandatory, read_only, max_length, active, synced_at, raw_json
            FROM business_application_field_dictionary
            WHERE table_name = ?1
            ORDER BY field_name
            "#,
        )?;
        let rows = stmt.query_map(params![table_name], |row| {
            Ok(BusinessApplicationFieldDictionaryRow {
                table_name: row.get(0)?,
                field_name: row.get(1)?,
                field_label: row.get(2)?,
                field_type: row.get(3)?,
                reference_table: row.get(4)?,
                choice: i64_to_bool(row.get(5)?),
                mandatory: i64_to_bool(row.get(6)?),
                read_only: i64_to_bool(row.get(7)?),
                max_length: row.get(8)?,
                active: i64_to_bool(row.get(9)?),
                synced_at: from_ts(row.get(10)?).map_err(to_sqlite_err)?,
                raw_json: row.get(11)?,
            })
        })?;
        Ok(rows.collect::<std::result::Result<Vec<_>, _>>()?)
    }

    /// Merged dictionary across the Business Application table and its inherited
    /// tables, keyed by field name. Child-table rows win over ancestor rows when
    /// the same field name appears at multiple levels (closest definition wins).
    ///
    /// Returns an empty map on a full dictionary cache miss.
    pub fn business_application_dictionary_for_tables(
        &self,
        tables: &[String],
    ) -> Result<HashMap<String, BusinessApplicationFieldDictionaryRow>> {
        // Walk from the most-derived table (index 0) to the base so earlier
        // entries are not overwritten by ancestor definitions.
        let mut map: HashMap<String, BusinessApplicationFieldDictionaryRow> = HashMap::new();
        for table in tables {
            for row in self.list_business_application_field_dictionary(table)? {
                map.entry(row.field_name.clone()).or_insert(row);
            }
        }
        Ok(map)
    }

    /// Delete relationships where the source record is tombstoned or pruned.
    pub fn cleanup_orphaned_relationships(&self) -> Result<usize> {
        let count = self.conn.execute(
            r#"
            DELETE FROM relationships
            WHERE source_id IN (
                SELECT sys_id FROM records WHERE in_scope = 0
            )
            "#,
            [],
        )?;
        Ok(count)
    }

    /// Delete references not linked to any active record's relationships.
    pub fn cleanup_orphaned_references(&self) -> Result<usize> {
        let count = self.conn.execute(
            r#"
            DELETE FROM "references"
            WHERE sys_id NOT IN (
                SELECT target_id FROM relationships
            )
            "#,
            [],
        )?;
        Ok(count)
    }

    /// Delete tags, keywords, and aliases for tombstoned or pruned records.
    pub fn cleanup_orphaned_enrichments(&self) -> Result<usize> {
        let mut total = 0;
        for table in ["record_tags", "record_keywords", "record_aliases"] {
            let count = self.conn.execute(
                &format!(
                    r#"DELETE FROM "{table}" WHERE record_sys_id IN (
                        SELECT sys_id FROM records WHERE in_scope = 0
                    )"#
                ),
                [],
            )?;
            total += count;
        }
        Ok(total)
    }
}

fn dedupe_kb_term_entries(entries: &[(String, Vec<String>)]) -> BTreeMap<String, Vec<String>> {
    let mut deduped = BTreeMap::new();
    for (record_sys_id, terms) in entries {
        deduped.insert(record_sys_id.clone(), terms.clone());
    }
    deduped
}

fn row_to_record_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RecordRow> {
    let resource_type: String = row.get(3)?;
    let synced_at = from_ts(row.get::<_, i64>(10)?).map_err(to_sqlite_err)?;
    let sys_updated_on = from_ts(row.get::<_, i64>(11)?).map_err(to_sqlite_err)?;
    let last_seen_at = from_ts(row.get::<_, i64>(14)?).map_err(to_sqlite_err)?;
    Ok(RecordRow {
        sys_id: row.get(0)?,
        number: row.get(1)?,
        table_name: row.get(2)?,
        resource_type: str_to_resource_type(&resource_type).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Text, Box::new(e))
        })?,
        state: row.get(4)?,
        short_desc: row.get(5)?,
        description: row.get(6)?,
        assigned_to: row.get(7)?,
        parent_id: row.get(8)?,
        file_path: row.get(9)?,
        synced_at,
        sys_updated_on,
        etag: row.get(12)?,
        in_scope: row.get::<_, i64>(13)? != 0,
        last_seen_at,
        tombstoned_at: row
            .get::<_, Option<i64>>(15)?
            .map(from_ts)
            .transpose()
            .map_err(to_sqlite_err)?,
        pruned_at: row
            .get::<_, Option<i64>>(16)?
            .map(from_ts)
            .transpose()
            .map_err(to_sqlite_err)?,
        raw_json: row.get(17)?,
    })
}

fn row_to_business_application_projection(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<BusinessApplicationProjectionRow> {
    Ok(BusinessApplicationProjectionRow {
        record_sys_id: row.get(0)?,
        name: row.get(1)?,
        number: row.get(2)?,
        business_owner_sys_id: row.get(3)?,
        business_owner_name: row.get(4)?,
        is_owner_sys_id: row.get(5)?,
        is_owner_name: row.get(6)?,
        ci_owner_group_sys_id: row.get(7)?,
        ci_owner_group_name: row.get(8)?,
        primary_support_group_sys_id: row.get(9)?,
        primary_support_group_name: row.get(10)?,
        operational_status_value: row.get(11)?,
        operational_status_display: row.get(12)?,
        primary_portfolio_sys_id: row.get(13)?,
        primary_portfolio_name: row.get(14)?,
        primary_portfolio_table: row.get(15)?,
        attested_date: row.get(16)?,
        sys_updated_on: row.get(17)?,
        field_count: row.get::<_, i64>(18)? as usize,
        reference_count: row.get::<_, i64>(19)? as usize,
        unresolved_reference_count: row.get::<_, i64>(20)? as usize,
    })
}

fn row_to_business_application_server_membership(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<BusinessApplicationServerMembershipRow> {
    let discovered_at = from_ts(row.get::<_, i64>(6)?).map_err(to_sqlite_err)?;
    let last_seen_at = from_ts(row.get::<_, i64>(7)?).map_err(to_sqlite_err)?;
    Ok(BusinessApplicationServerMembershipRow {
        ba_sys_id: row.get(0)?,
        server_sys_id: row.get(1)?,
        server_table: row.get(2)?,
        provenance: row.get(3)?,
        min_depth: row.get::<_, i64>(4)? as usize,
        paths_json: row.get(5)?,
        discovered_at,
        last_seen_at,
        tombstoned_at: row
            .get::<_, Option<i64>>(8)?
            .map(from_ts)
            .transpose()
            .map_err(to_sqlite_err)?,
    })
}

fn row_to_business_application_server_inventory_health(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<BusinessApplicationServerInventoryHealthRow> {
    Ok(BusinessApplicationServerInventoryHealthRow {
        ba_sys_id: row.get(0)?,
        run_started_at: from_ts(row.get::<_, i64>(1)?).map_err(to_sqlite_err)?,
        run_completed_at: from_ts(row.get::<_, i64>(2)?).map_err(to_sqlite_err)?,
        service_membership_status: row.get(3)?,
        relationship_status: row.get(4)?,
        inventory_status: row.get(5)?,
        summary_json: row.get(6)?,
    })
}

fn row_to_projected_field(row: &rusqlite::Row<'_>) -> rusqlite::Result<ProjectedFieldRow> {
    Ok(ProjectedFieldRow {
        owner_sys_id: row.get(0)?,
        field_name: row.get(1)?,
        field_label: row.get(2)?,
        field_type: row.get(3)?,
        value_text: row.get(4)?,
        display_value: row.get(5)?,
        value_number: row.get(6)?,
        value_date: row.get(7)?,
        value_bool: row.get::<_, Option<i64>>(8)?.map(i64_to_bool),
        reference_sys_id: row.get(9)?,
        reference_table: row.get(10)?,
        raw_json: row.get(11)?,
        updated_at: from_ts(row.get(12)?).map_err(to_sqlite_err)?,
    })
}

fn row_to_cached_user(row: &rusqlite::Row<'_>) -> rusqlite::Result<CachedUserRow> {
    Ok(CachedUserRow {
        sys_id: row.get(0)?,
        user_name: row.get(1)?,
        name: row.get(2)?,
        first_name: row.get(3)?,
        last_name: row.get(4)?,
        email: row.get(5)?,
        employee_number: row.get(6)?,
        active: row.get::<_, Option<i64>>(7)?.map(i64_to_bool),
        department: row.get(8)?,
        location: row.get(9)?,
        title: row.get(10)?,
        raw_json: row.get(11)?,
        synced_at: from_ts(row.get(12)?).map_err(to_sqlite_err)?,
        sys_updated_on: row.get(13)?,
    })
}

fn row_to_cached_user_query(row: &rusqlite::Row<'_>) -> rusqlite::Result<CachedUserQueryRow> {
    let raw_ids: String = row.get(1)?;
    let result_sys_ids = serde_json::from_str::<Vec<String>>(&raw_ids).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Text, Box::new(err))
    })?;
    Ok(CachedUserQueryRow {
        query_key: row.get(0)?,
        result_sys_ids,
        synced_at: from_ts(row.get(2)?).map_err(to_sqlite_err)?,
        expires_at: from_ts(row.get(3)?).map_err(to_sqlite_err)?,
    })
}

fn business_application_projection_from_fields(
    record: &SnowRecord,
    fields: &[ProjectedFieldRow],
) -> BusinessApplicationProjectionRow {
    let field = |name: &str| fields.iter().find(|field| field.field_name == name);
    let reference = |name: &str| {
        field(name).map(|field| {
            (
                field
                    .reference_sys_id
                    .clone()
                    .or_else(|| field.value_text.clone()),
                field
                    .display_value
                    .clone()
                    .or_else(|| field.value_text.clone())
                    .filter(|value| !value.trim().is_empty()),
                field.reference_table.clone(),
            )
        })
    };
    let (business_owner_sys_id, business_owner_name, _) =
        reference("business_owner").unwrap_or((None, None, None));
    let (is_owner_sys_id, is_owner_name, _) =
        reference("it_application_owner").unwrap_or((None, None, None));
    let (ci_owner_group_sys_id, ci_owner_group_name, _) =
        reference("managed_by_group").unwrap_or((None, None, None));
    let (primary_support_group_sys_id, primary_support_group_name, _) =
        reference("support_group").unwrap_or((None, None, None));
    let (primary_portfolio_sys_id, primary_portfolio_name, primary_portfolio_table) =
        reference("portfolio").unwrap_or((None, None, None));
    let operational_status = field("operational_status");
    let reference_count = fields
        .iter()
        .filter(|field| field.reference_sys_id.is_some())
        .count();
    let name = field("name")
        .and_then(projected_field_display_or_value)
        .or_else(|| crate::non_empty_owned(Some(&record.short_description)))
        .unwrap_or_else(|| record.sys_id.clone());

    BusinessApplicationProjectionRow {
        record_sys_id: record.sys_id.clone(),
        name,
        number: crate::non_empty_owned(Some(&record.number)),
        business_owner_sys_id,
        business_owner_name,
        is_owner_sys_id,
        is_owner_name,
        ci_owner_group_sys_id,
        ci_owner_group_name,
        primary_support_group_sys_id,
        primary_support_group_name,
        operational_status_value: operational_status.and_then(|field| field.value_text.clone()),
        operational_status_display: operational_status
            .and_then(|field| field.display_value.clone()),
        primary_portfolio_sys_id,
        primary_portfolio_name,
        primary_portfolio_table,
        attested_date: field("attested_date").and_then(|field| {
            field
                .value_date
                .clone()
                .or_else(|| projected_field_display_or_value(field))
        }),
        sys_updated_on: field("sys_updated_on").and_then(|field| {
            field
                .value_date
                .clone()
                .or_else(|| projected_field_display_or_value(field))
        }),
        field_count: fields.len(),
        reference_count,
        unresolved_reference_count: 0,
    }
}

fn projected_fields_from_json(
    owner_sys_id: &str,
    raw_map: &Map<String, Value>,
    field_values: Option<&HashMap<String, FieldValue>>,
    dictionary: &HashMap<String, BusinessApplicationFieldDictionaryRow>,
    updated_at: DateTime<Utc>,
) -> Result<Vec<ProjectedFieldRow>> {
    let mut names = raw_map.keys().cloned().collect::<BTreeSet<_>>();
    if let Some(field_values) = field_values {
        for key in field_values.keys() {
            names.insert(key.clone());
        }
    }

    let mut rows = Vec::with_capacity(names.len());
    for field_name in &names {
        let raw = raw_map.get(field_name);
        let field_value = field_values.and_then(|fields| fields.get(field_name));
        let dictionary_row = dictionary.get(field_name);
        let raw_field = raw
            .cloned()
            .unwrap_or_else(|| field_value_json(field_value));
        let value_text = field_value
            .map(|field| field.value.clone())
            .or_else(|| raw.and_then(raw_value_text));
        let display_value = field_value
            .and_then(|field| field.display_value.clone())
            .or_else(|| raw.and_then(raw_display_value));
        let field_type = dictionary_row.and_then(|row| row.field_type.clone());
        let reference_table = dictionary_row
            .and_then(|row| row.reference_table.clone())
            .or_else(|| known_business_application_reference_table(field_name).map(str::to_string));
        let reference_sys_id = reference_table
            .as_ref()
            .and(value_text.as_deref())
            .filter(|value| is_sys_id(value))
            .map(str::to_string);
        let value_number = value_text
            .as_deref()
            .and_then(|value| value.trim().parse::<f64>().ok());
        let value_bool = parse_bool(value_text.as_deref());
        let value_date =
            projected_date_value(field_name, field_type.as_deref(), value_text.as_deref());

        rows.push(ProjectedFieldRow {
            owner_sys_id: owner_sys_id.to_string(),
            field_name: field_name.clone(),
            field_label: dictionary_row.and_then(|row| row.field_label.clone()),
            field_type,
            value_text: crate::non_empty_owned(value_text.as_deref()),
            display_value: crate::non_empty_owned(display_value.as_deref()),
            value_number,
            value_date,
            value_bool,
            reference_sys_id,
            reference_table,
            raw_json: raw_field.to_string(),
            updated_at,
        });
    }
    Ok(rows)
}

fn field_values_to_json_map(fields: &HashMap<String, FieldValue>) -> Map<String, Value> {
    fields
        .iter()
        .map(|(name, field)| (name.clone(), field_value_json(Some(field))))
        .collect()
}

fn field_value_json(field: Option<&FieldValue>) -> Value {
    let Some(field) = field else {
        return Value::Null;
    };
    let mut map = Map::new();
    map.insert("value".to_string(), Value::String(field.value.clone()));
    if let Some(display_value) = &field.display_value {
        map.insert(
            "display_value".to_string(),
            Value::String(display_value.clone()),
        );
    }
    Value::Object(map)
}

fn field_value_from_raw_json(value: &Value) -> FieldValue {
    FieldValue {
        value: raw_value_text(value).unwrap_or_default(),
        display_value: raw_display_value(value),
    }
}

fn raw_value_text(value: &Value) -> Option<String> {
    match value {
        Value::Object(map) => map.get("value").map(json_value_to_projection_string),
        Value::Null => None,
        other => Some(json_value_to_projection_string(other)),
    }
}

fn raw_display_value(value: &Value) -> Option<String> {
    match value {
        Value::Object(map) => map
            .get("display_value")
            .map(json_value_to_projection_string)
            .and_then(|value| crate::non_empty_owned(Some(&value))),
        _ => None,
    }
}

fn json_value_to_projection_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn projected_date_value(
    field_name: &str,
    field_type: Option<&str>,
    value: Option<&str>,
) -> Option<String> {
    let value = value?.trim();
    if value.is_empty() {
        return None;
    }
    let field_type = field_type.unwrap_or_default().to_ascii_lowercase();
    let name = field_name.to_ascii_lowercase();
    if field_type.contains("date") || name.ends_with("_date") || name.ends_with("_on") {
        return Some(value.replace(' ', "T"));
    }
    None
}

fn parse_bool(value: Option<&str>) -> Option<bool> {
    match value?.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Some(true),
        "false" | "0" | "no" => Some(false),
        _ => None,
    }
}

fn known_business_application_reference_table(field_name: &str) -> Option<&'static str> {
    match field_name {
        "business_owner" | "it_application_owner" | "owned_by" | "managed_by" => Some("sys_user"),
        "managed_by_group" | "support_group" | "assignment_group" => Some("sys_user_group"),
        "cmdb_ci" => Some("cmdb_ci"),
        _ => None,
    }
}

fn projected_field_display_or_value(field: &ProjectedFieldRow) -> Option<String> {
    field
        .display_value
        .clone()
        .or_else(|| field.value_text.clone())
        .filter(|value| !value.trim().is_empty())
}

fn is_sys_id(value: &str) -> bool {
    value.len() == 32 && value.chars().all(|ch| ch.is_ascii_hexdigit())
}

fn business_application_matches_query(
    row: &RecordRow,
    projection: Option<&BusinessApplicationProjectionRow>,
    fields: &BTreeMap<String, ProjectedFieldRow>,
    query: &crate::query::filter::BusinessApplicationQuery,
) -> bool {
    if let Some(text) = crate::non_empty_owned(query.text.as_deref()) {
        let text = text.to_ascii_lowercase();
        let haystack = business_application_search_text(row, projection, fields);
        if !haystack.to_ascii_lowercase().contains(&text) {
            return false;
        }
    }
    query
        .filters
        .iter()
        .all(|filter| business_application_field_matches(fields.get(&filter.field), filter))
}

fn business_application_search_text(
    row: &RecordRow,
    projection: Option<&BusinessApplicationProjectionRow>,
    fields: &BTreeMap<String, ProjectedFieldRow>,
) -> String {
    let mut values = Vec::new();
    values.push(row.number.as_str());
    if let Some(value) = row.short_desc.as_deref() {
        values.push(value);
    }
    if let Some(value) = row.description.as_deref() {
        values.push(value);
    }
    if let Some(projection) = projection {
        values.push(projection.name.as_str());
        for value in [
            projection.business_owner_name.as_deref(),
            projection.is_owner_name.as_deref(),
            projection.ci_owner_group_name.as_deref(),
            projection.primary_support_group_name.as_deref(),
            projection.primary_portfolio_name.as_deref(),
            projection.operational_status_display.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            values.push(value);
        }
    }
    for field in fields.values() {
        if let Some(value) = field.value_text.as_deref() {
            values.push(value);
        }
        if let Some(value) = field.display_value.as_deref() {
            values.push(value);
        }
    }
    values.join(" ")
}

fn business_application_field_matches(
    field: Option<&ProjectedFieldRow>,
    filter: &crate::query::filter::FieldFilter,
) -> bool {
    use crate::query::filter::FieldOperator;

    match filter.op {
        FieldOperator::IsEmpty => return field.map(projected_field_is_empty).unwrap_or(true),
        FieldOperator::IsNotEmpty => {
            return field
                .map(|field| !projected_field_is_empty(field))
                .unwrap_or(false);
        }
        _ => {}
    }

    let Some(field) = field else {
        return matches!(filter.op, FieldOperator::Ne);
    };

    match filter.op {
        FieldOperator::Eq => field_values(field)
            .iter()
            .any(|candidate| value_matches_exact(candidate, &filter.value)),
        FieldOperator::Ne => !field_values(field)
            .iter()
            .any(|candidate| value_matches_exact(candidate, &filter.value)),
        FieldOperator::Contains => value_as_string(&filter.value)
            .map(|needle| {
                let needle = needle.to_ascii_lowercase();
                field_values(field)
                    .iter()
                    .any(|candidate| candidate.to_ascii_lowercase().contains(&needle))
            })
            .unwrap_or(false),
        FieldOperator::StartsWith => value_as_string(&filter.value)
            .map(|needle| {
                let needle = needle.to_ascii_lowercase();
                field_values(field)
                    .iter()
                    .any(|candidate| candidate.to_ascii_lowercase().starts_with(&needle))
            })
            .unwrap_or(false),
        FieldOperator::In => filter
            .value
            .as_array()
            .map(|values| {
                values.iter().any(|value| {
                    field_values(field)
                        .iter()
                        .any(|candidate| value_matches_exact(candidate, value))
                })
            })
            .unwrap_or(false),
        FieldOperator::Gt => compare_projected_field(field, &filter.value)
            .map(|ordering| ordering == std::cmp::Ordering::Greater)
            .unwrap_or(false),
        FieldOperator::Gte => compare_projected_field(field, &filter.value)
            .map(|ordering| {
                ordering == std::cmp::Ordering::Greater || ordering == std::cmp::Ordering::Equal
            })
            .unwrap_or(false),
        FieldOperator::Lt => compare_projected_field(field, &filter.value)
            .map(|ordering| ordering == std::cmp::Ordering::Less)
            .unwrap_or(false),
        FieldOperator::Lte => compare_projected_field(field, &filter.value)
            .map(|ordering| {
                ordering == std::cmp::Ordering::Less || ordering == std::cmp::Ordering::Equal
            })
            .unwrap_or(false),
        FieldOperator::IsEmpty | FieldOperator::IsNotEmpty => unreachable!(),
    }
}

fn projected_field_is_empty(field: &ProjectedFieldRow) -> bool {
    field_values(field)
        .into_iter()
        .all(|value| value.trim().is_empty())
}

fn field_values(field: &ProjectedFieldRow) -> Vec<String> {
    [
        field.value_text.clone(),
        field.display_value.clone(),
        field.reference_sys_id.clone(),
    ]
    .into_iter()
    .flatten()
    .collect()
}

fn value_matches_exact(candidate: &str, value: &Value) -> bool {
    value_as_string(value)
        .map(|value| candidate.eq_ignore_ascii_case(value.as_str()))
        .unwrap_or(false)
}

fn value_as_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        Value::Null => Some(String::new()),
        _ => None,
    }
}

fn compare_projected_field(field: &ProjectedFieldRow, value: &Value) -> Option<std::cmp::Ordering> {
    if let (Some(left), Some(right)) = (
        field.value_number,
        value
            .as_f64()
            .or_else(|| value_as_string(value)?.parse::<f64>().ok()),
    ) {
        return left.partial_cmp(&right);
    }
    if let (Some(left), Some(right)) = (field.value_date.as_deref(), value_as_string(value)) {
        return Some(left.cmp(right.as_str()));
    }
    let left = field.value_text.as_deref()?;
    let right = value_as_string(value)?;
    Some(left.cmp(right.as_str()))
}

type BusinessApplicationCandidate = (
    RecordRow,
    Option<BusinessApplicationProjectionRow>,
    BTreeMap<String, ProjectedFieldRow>,
);

fn compare_business_application_candidates(
    left: &BusinessApplicationCandidate,
    right: &BusinessApplicationCandidate,
    sort: &[crate::query::filter::SortField],
) -> std::cmp::Ordering {
    use crate::query::filter::SortDirection;

    for sort_field in sort {
        let ordering = business_application_sort_value(left, &sort_field.field)
            .cmp(&business_application_sort_value(right, &sort_field.field));
        let ordering = match sort_field.direction {
            SortDirection::Asc => ordering,
            SortDirection::Desc => ordering.reverse(),
        };
        if ordering != std::cmp::Ordering::Equal {
            return ordering;
        }
    }
    std::cmp::Ordering::Equal
}

fn business_application_sort_value(
    candidate: &BusinessApplicationCandidate,
    field: &str,
) -> String {
    let (row, projection, fields) = candidate;
    match field {
        "sys_id" => return row.sys_id.clone(),
        "number" => return row.number.clone(),
        "short_description" => return row.short_desc.clone().unwrap_or_default(),
        "description" => return row.description.clone().unwrap_or_default(),
        "name" => {
            if let Some(projection) = projection {
                return projection.name.clone();
            }
        }
        _ => {}
    }
    fields
        .get(field)
        .and_then(projected_field_display_or_value)
        .unwrap_or_default()
}

fn primitive_resolution_status_to_str(status: PrimitiveResolutionStatus) -> &'static str {
    match status {
        PrimitiveResolutionStatus::Resolved => "resolved",
        PrimitiveResolutionStatus::Unresolved => "unresolved",
        PrimitiveResolutionStatus::UnknownTable => "unknown_table",
        PrimitiveResolutionStatus::NotFound => "not_found",
        PrimitiveResolutionStatus::AclRestricted => "acl_restricted",
        PrimitiveResolutionStatus::Error => "error",
    }
}

fn primitive_resolution_status_from_str(value: &str) -> Result<PrimitiveResolutionStatus> {
    match value {
        "resolved" => Ok(PrimitiveResolutionStatus::Resolved),
        "unresolved" => Ok(PrimitiveResolutionStatus::Unresolved),
        "unknown_table" => Ok(PrimitiveResolutionStatus::UnknownTable),
        "not_found" => Ok(PrimitiveResolutionStatus::NotFound),
        "acl_restricted" => Ok(PrimitiveResolutionStatus::AclRestricted),
        "error" => Ok(PrimitiveResolutionStatus::Error),
        other => Err(StoreError::InvalidSchemaVersion(other.to_string())),
    }
}

fn resource_type_to_str(resource_type: &ResourceType) -> &'static str {
    match resource_type {
        ResourceType::Task => "task",
        ResourceType::Incident => "incident",
        ResourceType::Change => "change_request",
        ResourceType::ChangeTask => "change_task",
        ResourceType::Request => "sc_request",
        ResourceType::RequestTask => "sc_task",
        ResourceType::Project => "pm_project",
        ResourceType::Demand => "dmn_demand",
        ResourceType::DemandTask => "dmn_demand_task",
        ResourceType::ResourcePlan => "resource_plan",
        ResourceType::Story => "rm_story",
        ResourceType::ScrumTask => "rm_scrum_task",
        ResourceType::Timecard => "time_card",
        ResourceType::Knowledge => "kb_knowledge",
        ResourceType::Approval => "sysapproval_approver",
        ResourceType::BusinessApplication => "business_application",
        ResourceType::Server => "server",
    }
}

fn str_to_resource_type(input: &str) -> std::result::Result<ResourceType, StoreError> {
    Ok(match input {
        "task" => ResourceType::Task,
        "incident" => ResourceType::Incident,
        "change_request" => ResourceType::Change,
        "change_task" => ResourceType::ChangeTask,
        "sc_request" => ResourceType::Request,
        "sc_task" => ResourceType::RequestTask,
        "pm_project" => ResourceType::Project,
        "dmn_demand" => ResourceType::Demand,
        "dmn_demand_task" => ResourceType::DemandTask,
        "resource_plan" => ResourceType::ResourcePlan,
        "rm_story" => ResourceType::Story,
        "rm_scrum_task" => ResourceType::ScrumTask,
        "time_card" => ResourceType::Timecard,
        "kb_knowledge" => ResourceType::Knowledge,
        "sysapproval_approver" => ResourceType::Approval,
        "business_application" | "business_app" | "cmdb_ci_business_app" => {
            ResourceType::BusinessApplication
        }
        "server" | "cmdb_ci_server" | "cmdb_ci_linux_server" | "cmdb_ci_win_server" => {
            ResourceType::Server
        }
        other => return Err(StoreError::InvalidResourceType(other.to_string())),
    })
}

fn to_ts(value: DateTime<Utc>) -> i64 {
    value.timestamp()
}

fn cached_user_query_ttl() -> Duration {
    stable_reference_ttl()
}

fn opt_ts(value: Option<DateTime<Utc>>) -> Option<i64> {
    value.map(|dt| dt.timestamp())
}

fn bool_to_i64(value: bool) -> i64 {
    if value { 1 } else { 0 }
}

fn i64_to_bool(value: i64) -> bool {
    value != 0
}

fn parse_string_vec_column(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<Vec<String>> {
    let raw: String = row.get(index)?;
    serde_json::from_str(&raw).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(index, rusqlite::types::Type::Text, Box::new(err))
    })
}

fn from_ts(value: i64) -> Result<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp(value, 0).ok_or(StoreError::InvalidTimestamp(value))
}

fn to_sqlite_err(err: StoreError) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(err))
}

fn coverage_to_str(value: KnowledgeEmbeddingCoverage) -> &'static str {
    match value {
        KnowledgeEmbeddingCoverage::Metadata => "metadata",
        KnowledgeEmbeddingCoverage::FullText => "full_text",
    }
}

fn coverage_from_str(value: &str) -> Result<KnowledgeEmbeddingCoverage> {
    match value {
        "metadata" => Ok(KnowledgeEmbeddingCoverage::Metadata),
        "full_text" => Ok(KnowledgeEmbeddingCoverage::FullText),
        other => Err(StoreError::InvalidSchemaVersion(other.to_string())),
    }
}

fn encode_embedding_vector(values: &[f32]) -> Result<Vec<u8>> {
    if !crate::semantic::is_unit_length(values) {
        return Err(StoreError::NonUnitEmbeddingVector);
    }
    let mut out = Vec::with_capacity(values.len() * 4);
    for value in values {
        out.extend_from_slice(&value.to_le_bytes());
    }
    Ok(out)
}

fn decode_embedding_vector(raw: &[u8], dimensions: usize) -> Result<Vec<f32>> {
    let expected = dimensions * 4;
    if raw.len() != expected {
        return Err(StoreError::InvalidEmbeddingVectorLength {
            expected,
            actual: raw.len(),
        });
    }
    let mut out = Vec::with_capacity(dimensions);
    for chunk in raw.chunks_exact(4) {
        out.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
    }
    if !crate::semantic::is_unit_length(&out) {
        return Err(StoreError::NonUnitEmbeddingVector);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use serde_json::Value;
    use std::collections::HashMap;
    use tempfile::tempdir;

    #[test]
    fn initializes_schema_and_persists_records() {
        let store = Store::open_in_memory().expect("store");
        let record = RecordRow::active(
            "8a4d2e0ec3577e5433b2b643e4013100",
            "INC0012345",
            "incident",
            ResourceType::Incident,
            Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
        );
        store
            .upsert_record(&record, "work note", "content")
            .expect("insert");

        let loaded = store
            .get_record_by_number("INC0012345")
            .expect("query")
            .expect("row");

        assert_eq!(loaded.number, "INC0012345");
        assert!(loaded.in_scope);
        assert_eq!(store.count_active_records().unwrap(), 1);
    }

    #[test]
    fn persists_base_task_resource_type() {
        let store = Store::open_in_memory().expect("store");
        let record = RecordRow::active(
            "task-sys",
            "TASK0012345",
            "task",
            ResourceType::Task,
            Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
        );

        store.upsert_record(&record, "", "").expect("insert task");

        let loaded = store
            .get_record_by_number_and_type("TASK0012345", ResourceType::Task)
            .expect("query")
            .expect("row");
        let listed = store
            .list_active_records(Some(ResourceType::Task))
            .expect("list tasks");

        assert_eq!(loaded.resource_type, ResourceType::Task);
        assert_eq!(loaded.table_name, "task");
        assert_eq!(listed.len(), 1);
        assert_eq!(listed[0].number, "TASK0012345");
    }

    fn business_application_record(sys_id: &str, name: &str, cost: &str) -> SnowRecord {
        let owner_sys_id = "6816f79cc0a8016401c5a33be04be441";
        SnowRecord {
            sys_id: sys_id.to_string(),
            number: format!("BA:{sys_id}"),
            table: "cmdb_ci_business_app".to_string(),
            resource_type: ResourceType::BusinessApplication,
            state: "1".to_string(),
            short_description: name.to_string(),
            description: "Business Application".to_string(),
            fields: HashMap::from([
                (
                    "sys_id".to_string(),
                    FieldValue {
                        value: sys_id.to_string(),
                        display_value: None,
                    },
                ),
                (
                    "name".to_string(),
                    FieldValue {
                        value: name.to_string(),
                        display_value: None,
                    },
                ),
                (
                    "business_owner".to_string(),
                    FieldValue {
                        value: owner_sys_id.to_string(),
                        display_value: Some("Jane Owner".to_string()),
                    },
                ),
                (
                    "operational_status".to_string(),
                    FieldValue {
                        value: "1".to_string(),
                        display_value: Some("Operational".to_string()),
                    },
                ),
                (
                    "attested_date".to_string(),
                    FieldValue {
                        value: "2026-05-01".to_string(),
                        display_value: None,
                    },
                ),
                (
                    "u_cost".to_string(),
                    FieldValue {
                        value: cost.to_string(),
                        display_value: None,
                    },
                ),
                (
                    "u_code".to_string(),
                    FieldValue {
                        value: "ABC-123".to_string(),
                        display_value: None,
                    },
                ),
                (
                    "u_region".to_string(),
                    FieldValue {
                        value: "north".to_string(),
                        display_value: Some("North".to_string()),
                    },
                ),
                (
                    "u_empty".to_string(),
                    FieldValue {
                        value: String::new(),
                        display_value: None,
                    },
                ),
            ]),
            work_notes: Vec::new(),
            comments: Vec::new(),
            parent: None,
            children: Vec::new(),
            references: HashMap::new(),
            synced_at: Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
            source: crate::CacheSource::Disk,
        }
    }

    fn insert_business_application(store: &Store, record: &SnowRecord) {
        let mut row = RecordRow::active(
            record.sys_id.clone(),
            record.number.clone(),
            "cmdb_ci_business_app",
            ResourceType::BusinessApplication,
            Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
        );
        row.short_desc = Some(record.short_description.clone());
        row.description = Some(record.description.clone());
        row.raw_json = serde_json::to_string(&field_values_to_json_map(&record.fields))
            .expect("raw business application json");
        store.upsert_record(&row, "", "").expect("insert BA row");
        let raw = serde_json::from_str::<Value>(&row.raw_json).expect("raw json");
        store
            .upsert_business_application_projection(record, Some(&raw))
            .expect("project BA row");
    }

    #[test]
    fn initializes_schema_v11_business_application_projection_user_cache_ba_server_and_health_tables()
     {
        let store = Store::open_in_memory().expect("store");

        assert_eq!(store.schema_version().expect("version"), Some(11));
        for table in [
            "business_applications",
            "business_application_fields",
            "business_application_field_dictionary",
            "business_application_servers",
            "business_application_server_inventory_health",
            "primitive_objects",
            "primitive_object_fields",
            "cached_users",
            "cached_user_queries",
        ] {
            assert!(store.table_exists(table).expect("table exists"), "{table}");
        }
        for index in [
            "idx_ba_name",
            "idx_ba_fields_ref",
            "idx_ba_servers_ba",
            "idx_ba_servers_server",
            "idx_ba_servers_one_live_pair",
            "idx_primitive_fields_ref",
            "idx_cached_users_user_name",
            "idx_cached_users_email",
            "idx_cached_users_employee_number",
            "idx_cached_users_name",
            "idx_cached_users_first_last",
            "idx_cached_user_queries_expires",
        ] {
            assert!(store.index_exists(index).expect("index exists"), "{index}");
        }
    }

    #[test]
    fn business_application_server_memberships_round_trip_by_ba_and_server() {
        let store = Store::open_in_memory().expect("store");
        let now = Utc.timestamp_opt(1_779_840_000, 0).unwrap();
        let ba = RecordRow::active(
            "54a4b61b6fe845000ed852a03f3ee4d0",
            "APM0000001",
            "cmdb_ci_business_app",
            ResourceType::BusinessApplication,
            now,
        );
        let server = RecordRow::active(
            "7f4a6e2f1c23456789abcdef01234567",
            "SRV0000001",
            "cmdb_ci_linux_server",
            ResourceType::Server,
            now,
        );
        store.upsert_record(&ba, "", "").expect("insert BA");
        store.upsert_record(&server, "", "").expect("insert server");
        let membership = BusinessApplicationServerMembershipRow {
            ba_sys_id: "54a4b61b6fe845000ed852a03f3ee4d0".to_string(),
            server_sys_id: "7f4a6e2f1c23456789abcdef01234567".to_string(),
            server_table: "cmdb_ci_linux_server".to_string(),
            provenance: "cmdb_rel_ci".to_string(),
            min_depth: 2,
            paths_json: serde_json::json!([{
                "edges": [{
                    "depth": 1,
                    "parent_sys_id": "54a4b61b6fe845000ed852a03f3ee4d0",
                    "child_sys_id": "7f4a6e2f1c23456789abcdef01234567",
                    "direction": "parent_to_child",
                    "relationship_type": {
                        "value": "Depends on::Used by"
                    }
                }]
            }])
            .to_string(),
            discovered_at: now,
            last_seen_at: now,
            tombstoned_at: None,
        };

        store
            .upsert_business_application_server_membership(&membership)
            .expect("upsert membership");

        let by_ba = store
            .list_business_application_server_memberships_for_ba(&membership.ba_sys_id, false)
            .expect("list by BA");
        assert_eq!(by_ba, vec![membership.clone()]);

        let by_server = store
            .list_business_application_server_memberships_for_server(
                &membership.server_sys_id,
                false,
            )
            .expect("list by server");
        assert_eq!(by_server, vec![membership]);
    }

    #[test]
    fn tombstones_stale_business_application_server_memberships_and_reactivates_on_upsert() {
        let store = Store::open_in_memory().expect("store");
        let old = Utc.timestamp_opt(1_779_840_000, 0).unwrap();
        let cutoff = Utc.timestamp_opt(1_779_843_600, 0).unwrap();
        let tombstoned_at = Utc.timestamp_opt(1_779_847_200, 0).unwrap();
        let ba = RecordRow::active(
            "54a4b61b6fe845000ed852a03f3ee4d0",
            "APM0000001",
            "cmdb_ci_business_app",
            ResourceType::BusinessApplication,
            old,
        );
        let server = RecordRow::active(
            "7f4a6e2f1c23456789abcdef01234567",
            "SRV0000001",
            "cmdb_ci_linux_server",
            ResourceType::Server,
            old,
        );
        store.upsert_record(&ba, "", "").expect("insert BA");
        store.upsert_record(&server, "", "").expect("insert server");
        let mut membership = BusinessApplicationServerMembershipRow {
            ba_sys_id: ba.sys_id.clone(),
            server_sys_id: server.sys_id.clone(),
            server_table: server.table_name.clone(),
            provenance: "relationship".to_string(),
            min_depth: 1,
            paths_json: "[]".to_string(),
            discovered_at: old,
            last_seen_at: old,
            tombstoned_at: None,
        };
        store
            .upsert_business_application_server_membership(&membership)
            .expect("upsert membership");

        let pruned = store
            .tombstone_stale_business_application_server_memberships(
                &ba.sys_id,
                cutoff,
                tombstoned_at,
            )
            .expect("tombstone stale");
        assert_eq!(pruned, 1);
        assert!(
            store
                .list_business_application_server_memberships_for_ba(&ba.sys_id, false)
                .expect("active memberships")
                .is_empty()
        );
        let tombstoned = store
            .list_business_application_server_memberships_for_ba(&ba.sys_id, true)
            .expect("tombstoned memberships");
        assert_eq!(tombstoned.len(), 1);
        assert_eq!(tombstoned[0].tombstoned_at, Some(tombstoned_at));

        membership.last_seen_at = cutoff;
        store
            .upsert_business_application_server_membership(&membership)
            .expect("reactivate membership");
        let active = store
            .list_business_application_server_memberships_for_ba(&ba.sys_id, false)
            .expect("active memberships after re-seen");
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].tombstoned_at, None);
    }

    #[test]
    fn business_application_server_membership_transition_keeps_one_live_pair_and_first_seen() {
        let store = Store::open_in_memory().expect("store");
        let first_seen = Utc.timestamp_opt(1_779_840_000, 0).unwrap();
        let later_seen = Utc.timestamp_opt(1_779_843_600, 0).unwrap();
        let ba = RecordRow::active(
            "54a4b61b6fe845000ed852a03f3ee4d0",
            "APM0000001",
            "cmdb_ci_business_app",
            ResourceType::BusinessApplication,
            first_seen,
        );
        let server = RecordRow::active(
            "7f4a6e2f1c23456789abcdef01234567",
            "SRV0000001",
            "cmdb_ci_linux_server",
            ResourceType::Server,
            first_seen,
        );
        store.upsert_record(&ba, "", "").expect("insert BA");
        store.upsert_record(&server, "", "").expect("insert server");

        let relationship = BusinessApplicationServerMembershipRow {
            ba_sys_id: ba.sys_id.clone(),
            server_sys_id: server.sys_id.clone(),
            server_table: server.table_name.clone(),
            provenance: "relationship".to_string(),
            min_depth: 2,
            paths_json: "[]".to_string(),
            discovered_at: first_seen,
            last_seen_at: first_seen,
            tombstoned_at: None,
        };
        store
            .upsert_business_application_server_membership(&relationship)
            .expect("insert relationship membership");

        let mut both = relationship.clone();
        both.provenance = "both".to_string();
        both.min_depth = 1;
        both.discovered_at = later_seen;
        both.last_seen_at = later_seen;
        store
            .upsert_business_application_server_membership(&both)
            .expect("transition to both");

        let active = store
            .list_business_application_server_memberships_for_ba(&ba.sys_id, false)
            .expect("active memberships");
        assert_eq!(active.len(), 1);
        assert_eq!(active[0].provenance, "both");
        assert_eq!(active[0].discovered_at, first_seen);
        assert_eq!(active[0].last_seen_at, later_seen);

        let all = store
            .list_business_application_server_memberships_for_ba(&ba.sys_id, true)
            .expect("all memberships");
        assert_eq!(all.len(), 2);
        let tombstoned = all
            .iter()
            .find(|row| row.provenance == "relationship")
            .expect("relationship history row");
        assert_eq!(tombstoned.tombstoned_at, Some(later_seen));
    }

    #[test]
    fn business_application_server_membership_unique_index_rejects_duplicate_live_pair() {
        let store = Store::open_in_memory().expect("store");
        let now = Utc.timestamp_opt(1_779_840_000, 0).unwrap();
        let ba = RecordRow::active(
            "54a4b61b6fe845000ed852a03f3ee4d0",
            "APM0000001",
            "cmdb_ci_business_app",
            ResourceType::BusinessApplication,
            now,
        );
        let server = RecordRow::active(
            "7f4a6e2f1c23456789abcdef01234567",
            "SRV0000001",
            "cmdb_ci_linux_server",
            ResourceType::Server,
            now,
        );
        store.upsert_record(&ba, "", "").expect("insert BA");
        store.upsert_record(&server, "", "").expect("insert server");
        store
            .conn
            .execute(
                r#"
                INSERT INTO business_application_servers (
                    ba_sys_id, server_sys_id, server_table, provenance, min_depth,
                    paths_json, discovered_at, last_seen_at, tombstoned_at
                ) VALUES (?1, ?2, ?3, 'relationship', 1, '[]', ?4, ?4, NULL)
                "#,
                params![&ba.sys_id, &server.sys_id, &server.table_name, to_ts(now)],
            )
            .expect("insert relationship");
        let err = store
            .conn
            .execute(
                r#"
                INSERT INTO business_application_servers (
                    ba_sys_id, server_sys_id, server_table, provenance, min_depth,
                    paths_json, discovered_at, last_seen_at, tombstoned_at
                ) VALUES (?1, ?2, ?3, 'service_membership', 1, '[]', ?4, ?4, NULL)
                "#,
                params![&ba.sys_id, &server.sys_id, &server.table_name, to_ts(now)],
            )
            .expect_err("duplicate live pair rejected");
        assert!(err.to_string().contains("UNIQUE constraint failed"));
    }

    #[test]
    fn business_application_server_inventory_health_round_trips() {
        let store = Store::open_in_memory().expect("store");
        let started = Utc.timestamp_opt(1_779_840_000, 0).unwrap();
        let completed = Utc.timestamp_opt(1_779_840_030, 0).unwrap();
        let ba = RecordRow::active(
            "54a4b61b6fe845000ed852a03f3ee4d0",
            "APM0000001",
            "cmdb_ci_business_app",
            ResourceType::BusinessApplication,
            started,
        );
        store.upsert_record(&ba, "", "").expect("insert BA");

        let health = BusinessApplicationServerInventoryHealthRow {
            ba_sys_id: ba.sys_id.clone(),
            run_started_at: started,
            run_completed_at: completed,
            service_membership_status: "acl_restricted".to_string(),
            relationship_status: "ok".to_string(),
            inventory_status: "service_membership_degraded".to_string(),
            summary_json: serde_json::json!({
                "service_membership_status": "acl_restricted",
                "acl_restricted_count": 1
            })
            .to_string(),
        };
        store
            .upsert_business_application_server_inventory_health(&health)
            .expect("upsert health");

        assert_eq!(
            store
                .get_business_application_server_inventory_health(&ba.sys_id)
                .expect("get health"),
            Some(health)
        );
    }

    #[test]
    fn cached_users_round_trip_and_list_by_ids() {
        let store = Store::open_in_memory().expect("store");
        let synced_at = Utc.timestamp_opt(1_779_840_000, 0).unwrap();
        let user = CachedUserRow {
            sys_id: "6816f79cc0a8016401c5a33be04be441".to_string(),
            user_name: Some("jowner".to_string()),
            name: Some("Jane Owner".to_string()),
            first_name: Some("Jane".to_string()),
            last_name: Some("Owner".to_string()),
            email: Some("jane.owner@example.test".to_string()),
            employee_number: Some("12345".to_string()),
            active: Some(true),
            department: Some("IT".to_string()),
            location: Some("Dallas".to_string()),
            title: Some("Director".to_string()),
            raw_json: serde_json::json!({
                "sys_id": "6816f79cc0a8016401c5a33be04be441",
                "user_name": "jowner"
            })
            .to_string(),
            synced_at,
            sys_updated_on: Some("2026-05-31 12:34:56".to_string()),
        };

        store.upsert_cached_user(&user).expect("upsert user");

        let loaded = store
            .get_cached_user(&user.sys_id)
            .expect("get user")
            .expect("cached user");
        assert_eq!(loaded, user);

        let listed = store
            .list_cached_users_by_sys_ids(&[
                "missing".to_string(),
                "6816f79cc0a8016401c5a33be04be441".to_string(),
            ])
            .expect("list users");
        assert_eq!(listed, vec![user]);
    }

    #[test]
    fn cached_user_query_results_use_seven_day_ttl() {
        let store = Store::open_in_memory().expect("store");
        let synced_at = Utc.timestamp_opt(1_779_840_000, 0).unwrap();
        let ids = vec![
            "6816f79cc0a8016401c5a33be04be441".to_string(),
            "8a4d2e0ec3577e5433b2b643e4013100".to_string(),
        ];

        let stored = store
            .put_cached_user_query_result("active:name=Jane", &ids, synced_at)
            .expect("put query");
        assert_eq!(stored.expires_at, synced_at + cached_user_query_ttl());

        let fresh = store
            .get_cached_user_query_result("active:name=Jane", synced_at + Duration::days(6))
            .expect("get fresh query")
            .expect("fresh query");
        assert_eq!(fresh.result_sys_ids, ids);

        let expired = store
            .get_cached_user_query_result("active:name=Jane", synced_at + Duration::days(7))
            .expect("get expired query");
        assert!(expired.is_none());
    }

    #[test]
    fn projects_business_application_unknown_fields_and_references() {
        let store = Store::open_in_memory().expect("store");
        let record = business_application_record("54a4b61b6fe845000ed852a03f3ee4d0", "Epic", "150");
        insert_business_application(&store, &record);

        let projection = store
            .get_business_application_projection(&record.sys_id)
            .expect("projection")
            .expect("projection row");
        let fields = store
            .list_business_application_fields(&record.sys_id)
            .expect("fields");

        assert_eq!(projection.name, "Epic");
        assert_eq!(
            projection.business_owner_sys_id.as_deref(),
            Some("6816f79cc0a8016401c5a33be04be441")
        );
        assert_eq!(
            projection.business_owner_name.as_deref(),
            Some("Jane Owner")
        );
        assert!(fields.iter().any(|field| field.field_name == "u_cost"));
        let custom = fields
            .iter()
            .find(|field| field.field_name == "u_region")
            .expect("custom field");
        assert_eq!(custom.value_text.as_deref(), Some("north"));
        assert_eq!(custom.display_value.as_deref(), Some("North"));
    }

    #[test]
    fn local_business_application_query_filters_all_projected_field_shapes() {
        use crate::query::filter::{BusinessApplicationQuery, FieldOperator, SortDirection};

        let store = Store::open_in_memory().expect("store");
        let epic = business_application_record("54a4b61b6fe845000ed852a03f3ee4d0", "Epic", "150");
        let other = business_application_record("74a4b61b6fe845000ed852a03f3ee4d1", "Other", "50");
        insert_business_application(&store, &epic);
        insert_business_application(&store, &other);

        let rows = store
            .query_business_application_records(
                &BusinessApplicationQuery::new()
                    .filter("name", FieldOperator::Contains, serde_json::json!("pic"))
                    .filter(
                        "business_owner",
                        FieldOperator::Eq,
                        serde_json::json!("6816f79cc0a8016401c5a33be04be441"),
                    )
                    .filter(
                        "business_owner",
                        FieldOperator::Contains,
                        serde_json::json!("Jane"),
                    )
                    .filter(
                        "operational_status",
                        FieldOperator::Eq,
                        serde_json::json!("Operational"),
                    )
                    .filter(
                        "attested_date",
                        FieldOperator::Gte,
                        serde_json::json!("2026-01-01"),
                    )
                    .filter("u_cost", FieldOperator::Gt, serde_json::json!(100))
                    .filter("u_cost", FieldOperator::Lte, serde_json::json!(200))
                    .filter("u_cost", FieldOperator::Ne, serde_json::json!(0))
                    .filter(
                        "u_code",
                        FieldOperator::StartsWith,
                        serde_json::json!("ABC"),
                    )
                    .filter(
                        "u_region",
                        FieldOperator::In,
                        serde_json::json!(["South", "North"]),
                    )
                    .filter("u_empty", FieldOperator::IsEmpty, Value::Null)
                    .filter("u_code", FieldOperator::IsNotEmpty, Value::Null)
                    .text("owner")
                    .sort_by("name", SortDirection::Desc)
                    .limit(10),
            )
            .expect("query BA records");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].sys_id, epic.sys_id);
    }

    #[test]
    fn stores_unresolved_primitive_stub() {
        let store = Store::open_in_memory().expect("store");

        store
            .upsert_unresolved_primitive_stub(
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "u_unknown_reference",
                "Unknown Owner",
                PrimitiveResolutionStatus::UnknownTable,
                Some("table not in primitive allowlist".to_string()),
            )
            .expect("stub");

        let primitive = store
            .get_primitive_object("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
            .expect("primitive")
            .expect("primitive row");
        assert_eq!(primitive.table_name, "u_unknown_reference");
        assert_eq!(
            primitive.resolution_status,
            PrimitiveResolutionStatus::UnknownTable
        );
        assert_eq!(primitive.display_name, "Unknown Owner");
    }

    #[test]
    fn business_application_resource_type_is_canonical_string_with_table_alias() {
        let store = Store::open_in_memory().expect("store");
        let record = RecordRow::active(
            "54a4b61b6fe845000ed852a03f3ee4d0",
            "BA:54a4b61b6fe845000ed852a03f3ee4d0",
            "cmdb_ci_business_app",
            ResourceType::BusinessApplication,
            Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
        );

        store
            .upsert_record(&record, "", "")
            .expect("insert business app");

        let stored_resource_type: String = store
            .conn
            .query_row(
                "SELECT resource_type FROM records WHERE sys_id = ?1",
                ["54a4b61b6fe845000ed852a03f3ee4d0"],
                |row| row.get(0),
            )
            .expect("stored resource type");
        assert_eq!(stored_resource_type, "business_application");

        let loaded = store
            .get_record_by_number_and_type(
                "BA:54a4b61b6fe845000ed852a03f3ee4d0",
                ResourceType::BusinessApplication,
            )
            .expect("query")
            .expect("row");
        assert_eq!(loaded.resource_type, ResourceType::BusinessApplication);
        assert_eq!(
            str_to_resource_type("cmdb_ci_business_app").unwrap(),
            ResourceType::BusinessApplication
        );
    }

    #[test]
    fn allows_duplicate_numbers_across_resource_types() {
        let store = Store::open_in_memory().expect("store");
        let change = RecordRow::active(
            "chg-sys",
            "CHG0012345",
            "change_request",
            ResourceType::Change,
            Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
        );
        let approval = RecordRow::active(
            "apr-sys",
            "CHG0012345",
            "sysapproval_approver",
            ResourceType::Approval,
            Utc.timestamp_opt(1_712_649_601, 0).unwrap(),
        );

        store.upsert_record(&change, "", "").expect("insert change");
        store
            .upsert_record(&approval, "", "")
            .expect("insert approval");

        let primary = store
            .get_primary_record_by_number("CHG0012345")
            .expect("query")
            .expect("row");
        let approval_row = store
            .get_record_by_number_and_type("CHG0012345", ResourceType::Approval)
            .expect("approval query")
            .expect("approval row");

        assert_eq!(primary.sys_id, "chg-sys");
        assert_eq!(approval_row.sys_id, "apr-sys");
        assert_eq!(store.count_active_records().expect("count"), 2);
    }

    #[test]
    fn migrates_legacy_schema_with_unique_number_constraint() {
        let tempdir = tempdir().expect("tempdir");
        let path = tempdir.path().join("snow.db");
        let conn = Connection::open(&path).expect("open legacy db");
        conn.execute_batch(
            r#"
            CREATE TABLE schema_meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            INSERT INTO schema_meta(key, value) VALUES ('schema_version', '2');

            CREATE TABLE records (
                sys_id TEXT PRIMARY KEY,
                number TEXT NOT NULL UNIQUE,
                table_name TEXT NOT NULL,
                resource_type TEXT NOT NULL,
                state TEXT,
                short_desc TEXT,
                description TEXT,
                assigned_to TEXT,
                parent_id TEXT,
                file_path TEXT,
                synced_at INTEGER NOT NULL,
                sys_updated_on INTEGER NOT NULL,
                etag TEXT,
                in_scope INTEGER NOT NULL DEFAULT 1 CHECK (in_scope IN (0, 1)),
                last_seen_at INTEGER NOT NULL,
                tombstoned_at INTEGER,
                pruned_at INTEGER,
                raw_json TEXT NOT NULL DEFAULT '{}'
            );

            CREATE VIRTUAL TABLE fts_records USING fts5(
                number,
                short_desc,
                description,
                work_notes,
                content
            );
            "#,
        )
        .expect("seed legacy schema");
        drop(conn);

        let store = Store::open(&path).expect("migrate store");
        assert_eq!(store.schema_version().expect("version"), Some(11));
        assert!(
            store
                .table_has_column("fts_records", "tag_tokens")
                .expect("fts tag_tokens column")
        );
        assert!(
            store
                .table_has_column("knowledge_articles", "sn_tags")
                .expect("knowledge sn_tags column")
        );
        assert!(
            store
                .table_has_column("knowledge_articles", "body_cached")
                .expect("knowledge body_cached column")
        );
        assert_eq!(
            store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM kb_sync_state WHERE id = 1",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .expect("kb sync seed row"),
            1
        );
        assert!(
            store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'kb_term_stats'",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .expect("kb term stats table exists")
                > 0
        );

        let change = RecordRow::active(
            "chg-sys",
            "CHG0012345",
            "change_request",
            ResourceType::Change,
            Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
        );
        let approval = RecordRow::active(
            "apr-sys",
            "CHG0012345",
            "sysapproval_approver",
            ResourceType::Approval,
            Utc.timestamp_opt(1_712_649_601, 0).unwrap(),
        );
        store.upsert_record(&change, "", "").expect("insert change");
        store
            .upsert_record(&approval, "", "")
            .expect("insert approval after migration");
    }

    #[test]
    fn migrates_unversioned_legacy_knowledge_schema_before_creating_updated_index() {
        let tempdir = tempdir().expect("tempdir");
        let path = tempdir.path().join("legacy-kb.db");
        let conn = Connection::open(&path).expect("open legacy kb db");
        conn.execute_batch(
            r#"
            CREATE TABLE knowledge_articles (
                record_sys_id TEXT PRIMARY KEY,
                number TEXT NOT NULL,
                title TEXT NOT NULL,
                workflow_state TEXT NOT NULL,
                knowledge_base_sys_id TEXT NOT NULL,
                knowledge_base_name TEXT NOT NULL,
                category_sys_id TEXT NOT NULL,
                category_name TEXT NOT NULL,
                author_sys_id TEXT,
                author_name TEXT,
                published_at TEXT,
                valid_to TEXT,
                article_type TEXT NOT NULL
            );

            CREATE VIRTUAL TABLE fts_records USING fts5(
                number,
                short_desc,
                description,
                work_notes,
                content
            );
            "#,
        )
        .expect("seed unversioned legacy kb schema");
        drop(conn);

        let store = Store::open(&path).expect("migrate unversioned legacy kb store");
        assert_eq!(store.schema_version().expect("version"), Some(11));
        assert!(
            store
                .table_has_column("knowledge_articles", "sys_updated_on")
                .expect("knowledge sys_updated_on column")
        );
        assert!(
            store
                .table_has_column("knowledge_articles", "sn_tags")
                .expect("knowledge sn_tags column")
        );
        assert!(
            store
                .table_has_column("knowledge_articles", "body_cached")
                .expect("knowledge body_cached column")
        );
        assert!(
            store
                .table_has_column("fts_records", "tag_tokens")
                .expect("fts tag_tokens column")
        );
        assert!(
            store
                .conn
                .query_row(
                    "SELECT COUNT(*) FROM sqlite_master WHERE type = 'index' AND name = 'idx_kb_articles_updated'",
                    [],
                    |row| row.get::<_, i64>(0)
                )
                .expect("kb updated index exists")
                > 0
        );
    }

    #[test]
    fn sync_state_round_trips() {
        let store = Store::open_in_memory().expect("store");
        let row = SyncStateRow {
            resource_type: "incident".to_string(),
            last_full: Some(Utc.timestamp_opt(1_712_649_600, 0).unwrap()),
            last_incr: Some(Utc.timestamp_opt(1_712_649_800, 0).unwrap()),
            high_watermark: Some(Utc.timestamp_opt(1_712_650_000, 0).unwrap()),
            cursor: Some("page-2".to_string()),
            filter_hash: Some("abc123".to_string()),
        };
        store.set_sync_state(&row).expect("sync state");
        let loaded = store
            .get_sync_state("incident")
            .expect("query")
            .expect("row");
        assert_eq!(loaded.cursor.as_deref(), Some("page-2"));
        assert_eq!(loaded.high_watermark.unwrap().timestamp(), 1_712_650_000);
    }

    #[test]
    fn enrichment_rows_round_trip_and_replace() {
        let store = Store::open_in_memory().expect("store");
        let record = RecordRow::active(
            "sys-1",
            "INC0000001",
            "incident",
            ResourceType::Incident,
            Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
        );
        store.upsert_record(&record, "", "").expect("insert");

        store
            .replace_tags(
                "sys-1",
                &[
                    TagRow {
                        record_sys_id: "sys-1".to_string(),
                        tag: "network".to_string(),
                        source: "derived".to_string(),
                        weight: 1.0,
                    },
                    TagRow {
                        record_sys_id: "sys-1".to_string(),
                        tag: "vpn".to_string(),
                        source: "manual".to_string(),
                        weight: 2.0,
                    },
                ],
            )
            .expect("replace tags");
        store
            .replace_keywords(
                "sys-1",
                &[KeywordRow {
                    record_sys_id: "sys-1".to_string(),
                    keyword: "connectivity".to_string(),
                    source: "derived".to_string(),
                    weight: 1.5,
                }],
            )
            .expect("replace keywords");
        store
            .replace_aliases(
                "sys-1",
                &[AliasRow {
                    record_sys_id: "sys-1".to_string(),
                    alias: "vpn issue".to_string(),
                    kind: "short_title".to_string(),
                    source: "derived".to_string(),
                }],
            )
            .expect("replace aliases");

        assert_eq!(store.list_tags("sys-1").expect("list tags").len(), 2);
        assert_eq!(
            store.list_keywords("sys-1").expect("list keywords")[0].keyword,
            "connectivity"
        );
        assert_eq!(
            store.list_aliases("sys-1").expect("list aliases")[0].alias,
            "vpn issue"
        );

        assert_eq!(
            store
                .find_record_sys_ids_by_tag("network", 10)
                .expect("find tag"),
            vec!["sys-1".to_string()]
        );
        assert_eq!(
            store
                .find_record_sys_ids_by_keyword("connectivity", 10)
                .expect("find keyword"),
            vec!["sys-1".to_string()]
        );
        assert_eq!(
            store
                .find_record_sys_ids_by_alias("vpn issue", 10)
                .expect("find alias"),
            vec!["sys-1".to_string()]
        );

        store
            .replace_tags(
                "sys-1",
                &[TagRow {
                    record_sys_id: "sys-1".to_string(),
                    tag: "incident".to_string(),
                    source: "derived".to_string(),
                    weight: 1.0,
                }],
            )
            .expect("replace tags again");
        assert_eq!(
            store
                .list_tags("sys-1")
                .expect("list replaced tags")
                .into_iter()
                .map(|row| row.tag)
                .collect::<Vec<_>>(),
            vec!["incident".to_string()]
        );
    }

    #[test]
    fn enrichment_lookups_are_exact_ordered_and_limited() {
        let store = Store::open_in_memory().expect("store");
        let first = RecordRow::active(
            "sys-1",
            "INC0000001",
            "incident",
            ResourceType::Incident,
            Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
        );
        let second = RecordRow::active(
            "sys-2",
            "INC0000002",
            "incident",
            ResourceType::Incident,
            Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
        );
        store.upsert_record(&first, "", "").expect("insert first");
        store.upsert_record(&second, "", "").expect("insert second");

        store
            .replace_tags(
                "sys-2",
                &[TagRow {
                    record_sys_id: "sys-2".to_string(),
                    tag: "shared".to_string(),
                    source: "derived".to_string(),
                    weight: 1.0,
                }],
            )
            .expect("insert second tag");
        store
            .replace_tags(
                "sys-1",
                &[TagRow {
                    record_sys_id: "sys-1".to_string(),
                    tag: "shared".to_string(),
                    source: "derived".to_string(),
                    weight: 1.0,
                }],
            )
            .expect("insert first tag");

        assert_eq!(
            store
                .find_record_sys_ids_by_tag("shared", 1)
                .expect("limited lookup"),
            vec!["sys-1".to_string()]
        );
        assert_eq!(
            store
                .find_record_sys_ids_by_tag("shared", 10)
                .expect("full lookup"),
            vec!["sys-1".to_string(), "sys-2".to_string()]
        );
        assert_eq!(
            store
                .find_record_sys_ids_by_tag("missing", 10)
                .expect("missing lookup"),
            Vec::<String>::new()
        );

        store
            .replace_keywords(
                "sys-2",
                &[KeywordRow {
                    record_sys_id: "sys-2".to_string(),
                    keyword: "network".to_string(),
                    source: "derived".to_string(),
                    weight: 1.0,
                }],
            )
            .expect("insert keyword");
        assert_eq!(
            store
                .find_record_sys_ids_by_keyword("network", 10)
                .expect("keyword lookup"),
            vec!["sys-2".to_string()]
        );

        store
            .replace_aliases(
                "sys-1",
                &[AliasRow {
                    record_sys_id: "sys-1".to_string(),
                    alias: "net issue".to_string(),
                    kind: "short_title".to_string(),
                    source: "derived".to_string(),
                }],
            )
            .expect("insert alias");
        assert_eq!(
            store
                .find_record_sys_ids_by_alias("net issue", 10)
                .expect("alias lookup"),
            vec!["sys-1".to_string()]
        );
    }

    #[test]
    fn knowledge_article_projection_round_trips_and_groups() {
        let store = Store::open_in_memory().expect("store");
        let first = RecordRow::active(
            "kb-1",
            "KB001",
            "kb_knowledge",
            ResourceType::Knowledge,
            Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
        );
        let second = RecordRow::active(
            "kb-2",
            "KB002",
            "kb_knowledge",
            ResourceType::Knowledge,
            Utc.timestamp_opt(1_712_649_700, 0).unwrap(),
        );
        store
            .upsert_record(&first, "", "gateway")
            .expect("insert first");
        store
            .upsert_record(&second, "", "access")
            .expect("insert second");

        store
            .upsert_knowledge_article(&KnowledgeArticleRow {
                record_sys_id: "kb-1".to_string(),
                number: "KB001".to_string(),
                title: "VPN Runbook".to_string(),
                workflow_state: "published".to_string(),
                knowledge_base_sys_id: "kb-base".to_string(),
                knowledge_base_name: "IT".to_string(),
                category_sys_id: "kb-cat-1".to_string(),
                category_name: "Network".to_string(),
                author_sys_id: Some("user-1".to_string()),
                author_name: Some("Casey User".to_string()),
                published_at: Some("2026-04-10 09:00:00".to_string()),
                valid_to: Some("2027-01-01".to_string()),
                article_type: "text".to_string(),
                sys_updated_on: Some("2026-04-10 09:00:00".to_string()),
                sn_tags: vec!["vpn".to_string()],
                auto_tags: vec!["gateway".to_string()],
                user_tags: vec!["runbook".to_string()],
                body_cached: true,
            })
            .expect("upsert first article");
        store
            .upsert_knowledge_article(&KnowledgeArticleRow {
                record_sys_id: "kb-2".to_string(),
                number: "KB002".to_string(),
                title: "Windows Access".to_string(),
                workflow_state: "published".to_string(),
                knowledge_base_sys_id: "kb-base".to_string(),
                knowledge_base_name: "IT".to_string(),
                category_sys_id: "kb-cat-2".to_string(),
                category_name: "Access".to_string(),
                author_sys_id: None,
                author_name: None,
                published_at: None,
                valid_to: None,
                article_type: "text".to_string(),
                sys_updated_on: None,
                sn_tags: Vec::new(),
                auto_tags: Vec::new(),
                user_tags: Vec::new(),
                body_cached: false,
            })
            .expect("upsert second article");

        let loaded = store
            .get_knowledge_article("kb-1")
            .expect("query")
            .expect("row");
        assert_eq!(loaded.title, "VPN Runbook");
        assert_eq!(loaded.author_name.as_deref(), Some("Casey User"));

        let bases = store.list_knowledge_bases().expect("bases");
        assert_eq!(
            bases,
            vec![KnowledgeBaseSummaryRow {
                knowledge_base_sys_id: "kb-base".to_string(),
                knowledge_base_name: "IT".to_string(),
                article_count: 2,
            }]
        );

        let categories = store
            .list_knowledge_categories("kb-base")
            .expect("categories");
        assert_eq!(categories.len(), 2);
        assert_eq!(categories[0].category_name, "Access");
        assert_eq!(categories[0].article_count, 1);
        assert_eq!(categories[1].category_name, "Network");
        assert_eq!(categories[1].article_count, 1);
    }

    #[test]
    fn fts_search_matches_tag_tokens() {
        let store = Store::open_in_memory().expect("store");
        let row = RecordRow::active(
            "kb-1",
            "KB001",
            "kb_knowledge",
            ResourceType::Knowledge,
            Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
        );
        store
            .upsert_record_with_tags(&row, "", "Gateway verification", "vpn runbook")
            .expect("insert tagged kb row");

        assert_eq!(
            store.search_fts("vpn", 10).expect("search vpn"),
            vec!["KB001".to_string()]
        );
        assert_eq!(
            store.search_fts("runbook", 10).expect("search runbook"),
            vec!["KB001".to_string()]
        );
    }

    #[test]
    fn knowledge_embedding_round_trips_and_counts_by_coverage() {
        let store = Store::open_in_memory().expect("store");
        let record = RecordRow::active(
            "kb-1",
            "KB001",
            "kb_knowledge",
            ResourceType::Knowledge,
            Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
        );
        store.upsert_record(&record, "", "").expect("insert record");
        store
            .upsert_knowledge_article(&KnowledgeArticleRow {
                record_sys_id: "kb-1".to_string(),
                number: "KB001".to_string(),
                title: "VPN Runbook".to_string(),
                workflow_state: "published".to_string(),
                knowledge_base_sys_id: "kb-base".to_string(),
                knowledge_base_name: "IT".to_string(),
                category_sys_id: "kb-cat".to_string(),
                category_name: "Network".to_string(),
                author_sys_id: None,
                author_name: None,
                published_at: None,
                valid_to: None,
                article_type: "text".to_string(),
                sys_updated_on: None,
                sn_tags: Vec::new(),
                auto_tags: Vec::new(),
                user_tags: Vec::new(),
                body_cached: true,
            })
            .expect("insert knowledge row");

        let vector = crate::semantic::normalize_unit_vector(&[3.0, 4.0]).expect("vector");
        store
            .upsert_knowledge_embedding(&KnowledgeEmbeddingRow {
                record_sys_id: "kb-1".to_string(),
                model: "stub".to_string(),
                provider: "stub".to_string(),
                dimensions: vector.len(),
                coverage: KnowledgeEmbeddingCoverage::FullText,
                content_hash: "abc".to_string(),
                vector: vector.clone(),
                updated_at: Utc.timestamp_opt(1_712_649_700, 0).unwrap(),
            })
            .expect("insert embedding");

        let loaded = store
            .get_knowledge_embedding("kb-1")
            .expect("get embedding")
            .expect("embedding row");
        assert_eq!(loaded.dimensions, 2);
        assert_eq!(loaded.coverage, KnowledgeEmbeddingCoverage::FullText);
        assert_eq!(loaded.vector, vector);
        assert_eq!(
            store
                .count_knowledge_embeddings_by_coverage(
                    "stub",
                    KnowledgeEmbeddingCoverage::FullText,
                )
                .expect("coverage count"),
            1
        );
    }

    #[test]
    fn knowledge_embedding_decode_rejects_dimension_mismatch() {
        let err = decode_embedding_vector(&[0_u8; 7], 2).expect_err("dimension mismatch");
        assert!(matches!(
            err,
            StoreError::InvalidEmbeddingVectorLength {
                expected: 8,
                actual: 7
            }
        ));
    }

    #[test]
    fn kb_article_term_writes_dedupe_duplicate_record_ids() {
        let store = Store::open_in_memory().expect("store");

        store
            .replace_all_kb_article_terms(&[
                ("kb-1".to_string(), vec!["vpn".to_string()]),
                ("kb-1".to_string(), vec!["runbook".to_string()]),
            ])
            .expect("replace all with duplicates");
        assert_eq!(
            store
                .get_kb_article_terms("kb-1")
                .expect("terms after replace all"),
            vec!["runbook".to_string()]
        );

        store
            .replace_kb_article_terms_entries(&[
                ("kb-1".to_string(), vec!["network".to_string()]),
                ("kb-1".to_string(), vec!["gateway".to_string()]),
            ])
            .expect("replace entries with duplicates");
        assert_eq!(
            store
                .get_kb_article_terms("kb-1")
                .expect("terms after replace entries"),
            vec!["gateway".to_string()]
        );
    }

    #[test]
    fn tombstone_and_prune_flow_updates_state() {
        let store = Store::open_in_memory().expect("store");
        let record = RecordRow::active(
            "sys-1",
            "INC0000001",
            "incident",
            ResourceType::Incident,
            Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
        );
        store.upsert_record(&record, "", "").expect("insert");
        store
            .replace_tags(
                "sys-1",
                &[TagRow {
                    record_sys_id: "sys-1".to_string(),
                    tag: "vpn".to_string(),
                    source: "derived".to_string(),
                    weight: 1.0,
                }],
            )
            .expect("insert tag");
        store
            .replace_keywords(
                "sys-1",
                &[KeywordRow {
                    record_sys_id: "sys-1".to_string(),
                    keyword: "network".to_string(),
                    source: "derived".to_string(),
                    weight: 1.0,
                }],
            )
            .expect("insert keyword");
        store
            .replace_aliases(
                "sys-1",
                &[AliasRow {
                    record_sys_id: "sys-1".to_string(),
                    alias: "vpn ticket".to_string(),
                    kind: "short_title".to_string(),
                    source: "derived".to_string(),
                }],
            )
            .expect("insert alias");
        store
            .upsert_knowledge_article(&KnowledgeArticleRow {
                record_sys_id: "sys-1".to_string(),
                number: "KB001".to_string(),
                title: "VPN Runbook".to_string(),
                workflow_state: "published".to_string(),
                knowledge_base_sys_id: "kb-base".to_string(),
                knowledge_base_name: "IT".to_string(),
                category_sys_id: "kb-cat".to_string(),
                category_name: "Network".to_string(),
                author_sys_id: None,
                author_name: None,
                published_at: None,
                valid_to: None,
                article_type: "text".to_string(),
                sys_updated_on: None,
                sn_tags: Vec::new(),
                auto_tags: Vec::new(),
                user_tags: Vec::new(),
                body_cached: false,
            })
            .expect("insert knowledge row");
        let tombstoned = record
            .clone()
            .tombstone(Utc.timestamp_opt(1_712_650_100, 0).unwrap());
        store
            .tombstone_record(&tombstoned.sys_id, tombstoned.tombstoned_at.unwrap())
            .expect("tombstone");
        assert_eq!(
            store
                .get_record_by_sys_id("sys-1")
                .unwrap()
                .unwrap()
                .lifecycle(),
            RecordLifecycle::Tombstoned
        );
        store
            .prune_record("sys-1", Utc.timestamp_opt(1_712_650_200, 0).unwrap())
            .expect("prune");
        assert!(store.get_record_by_sys_id("sys-1").unwrap().is_none());
        assert!(store.list_tags("sys-1").unwrap().is_empty());
        assert!(store.list_keywords("sys-1").unwrap().is_empty());
        assert!(store.list_aliases("sys-1").unwrap().is_empty());
        assert!(store.get_knowledge_article("sys-1").unwrap().is_none());
        assert!(
            store
                .find_record_sys_ids_by_tag("vpn", 10)
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .find_record_sys_ids_by_keyword("network", 10)
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .find_record_sys_ids_by_alias("vpn ticket", 10)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn test_upsert_records_batch() {
        let store = Store::open(":memory:").unwrap();
        let now = Utc::now();
        let rows: Vec<_> = (0..5)
            .map(|i| {
                RecordRow::active(
                    format!("sys-{i}"),
                    format!("INC{i:04}"),
                    "incident",
                    ResourceType::Incident,
                    now,
                )
            })
            .collect();

        let entries: Vec<(&RecordRow, &str, &str, &str)> =
            rows.iter().map(|row| (row, "", "", "")).collect();

        store.upsert_records(&entries).unwrap();

        let active = store
            .list_active_records(Some(ResourceType::Incident))
            .unwrap();
        assert_eq!(active.len(), 5);
    }

    #[test]
    fn test_list_active_records_paginated() {
        let store = Store::open_in_memory().expect("store");
        let now = Utc::now();
        for i in 0..10 {
            let row = RecordRow::active(
                format!("sys-{i}"),
                format!("INC{i:04}"),
                "incident",
                ResourceType::Incident,
                now,
            );
            store.upsert_record(&row, "", "").unwrap();
        }

        let page1 = store
            .list_active_records_paginated(Some(ResourceType::Incident), 5, 0)
            .unwrap();
        assert_eq!(page1.len(), 5);

        let page2 = store
            .list_active_records_paginated(Some(ResourceType::Incident), 5, 5)
            .unwrap();
        assert_eq!(page2.len(), 5);

        assert_ne!(page1[0].sys_id, page2[0].sys_id);
    }

    #[test]
    fn test_cleanup_orphaned_enrichments() {
        let store = Store::open_in_memory().unwrap();
        let now = Utc::now();

        let row = RecordRow::active("sys-1", "INC0001", "incident", ResourceType::Incident, now);
        store.upsert_record(&row, "", "").unwrap();

        store
            .replace_tags(
                "sys-1",
                &[TagRow {
                    record_sys_id: "sys-1".to_string(),
                    tag: "vpn".to_string(),
                    source: "derived".to_string(),
                    weight: 1.0,
                }],
            )
            .unwrap();
        store
            .replace_keywords(
                "sys-1",
                &[KeywordRow {
                    record_sys_id: "sys-1".to_string(),
                    keyword: "network".to_string(),
                    source: "derived".to_string(),
                    weight: 1.0,
                }],
            )
            .unwrap();

        // Tombstone the record (sets in_scope = 0)
        store.tombstone_record("sys-1", now).unwrap();

        // Cleanup should remove orphaned enrichments for tombstoned records
        let removed = store.cleanup_orphaned_enrichments().unwrap();
        assert!(removed > 0, "expected orphaned enrichments to be removed");
        assert!(store.list_tags("sys-1").unwrap().is_empty());
        assert!(store.list_keywords("sys-1").unwrap().is_empty());
    }

    #[test]
    fn test_cleanup_orphaned_relationships() {
        let store = Store::open_in_memory().unwrap();
        let now = Utc::now();

        let row = RecordRow::active("sys-1", "INC0001", "incident", ResourceType::Incident, now);
        store.upsert_record(&row, "", "").unwrap();

        store
            .upsert_relationship(&RelationshipRow {
                source_id: "sys-1".to_string(),
                target_id: "ref-999".to_string(),
                rel_type: "reference".to_string(),
                field_name: "assigned_to".to_string(),
            })
            .unwrap();

        // Tombstone the record so in_scope = 0
        store.tombstone_record("sys-1", now).unwrap();

        let removed = store.cleanup_orphaned_relationships().unwrap();
        assert_eq!(removed, 1);
    }

    #[test]
    fn test_cleanup_orphaned_references() {
        let store = Store::open_in_memory().unwrap();
        let now = Utc::now();

        // Insert a reference that has no corresponding relationship entry
        store
            .upsert_reference(&ReferenceRow {
                sys_id: "ref-orphan".to_string(),
                table_name: "sys_user".to_string(),
                display_name: "Orphan User".to_string(),
                extra_json: "{}".to_string(),
                synced_at: now,
                expires_at: None,
            })
            .unwrap();

        // cleanup_orphaned_references removes references not in any relationship's target_id
        let removed = store.cleanup_orphaned_references().unwrap();
        assert_eq!(removed, 1);
    }
}
