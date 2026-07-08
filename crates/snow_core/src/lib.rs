#![allow(clippy::arc_with_non_send_sync)]
// rusqlite::Connection is Send but not Sync; Store is only ever accessed
// from a single task at a time, so Arc<Store> is deliberate.

pub mod cache;
pub mod config;
pub(crate) mod context;
pub(crate) mod convert;
pub mod credential;
pub mod display;
pub mod enrich;
pub(crate) mod helpers;
pub mod ipc;
pub mod kb;
pub mod query;
pub(crate) mod reference;
pub mod refresh;
pub mod resource;
pub(crate) mod semantic;
pub(crate) mod service;
pub mod sla;
pub mod types;
pub mod vault;

// Re-export extracted functions so existing callers in this file (and tests
// that use `super::*`) continue to work without path changes.
pub use cache::policy::{
    CacheTtlPolicy, STABLE_REFERENCE_CACHE_TTL_DAYS, WORK_RECORD_CACHE_TTL_MINUTES,
    stable_reference_ttl, work_record_ttl,
};
pub(crate) use convert::*;
pub use credential::{CredentialError, CredentialProvider};
pub(crate) use helpers::*;
pub use kb::{
    KnowledgeStatus, KnowledgeSyncMode, KnowledgeSyncOutcome, KnowledgeTagLayer,
    KnowledgeTagSummary,
};
pub(crate) use reference::*;
pub use resource::business_application::{
    BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD,
    BUSINESS_APPLICATION_DEGRADED_REASON_CMDB_RELATIONSHIPS_UNMAPPED,
    BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_CIS, BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_DEPTH,
    BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_EDGES,
    BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_SERVICE_MEMBERSHIP_ASSOCIATIONS,
    BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_SERVICE_MEMBERSHIP_PAGES,
    BUSINESS_APPLICATION_SERVERS_DEFAULT_RELATIONSHIP_TYPES, BUSINESS_APPLICATION_SERVERS_MAX_CIS,
    BUSINESS_APPLICATION_SERVERS_MAX_DEPTH, BUSINESS_APPLICATION_SERVERS_MAX_EDGES,
    BUSINESS_APPLICATION_SERVERS_MAX_SERVICE_MEMBERSHIP_ASSOCIATIONS,
    BUSINESS_APPLICATION_SERVERS_MAX_SERVICE_MEMBERSHIP_PAGES,
    BUSINESS_APPLICATION_SERVICE_DISCOVERY_RELATIONSHIP_TYPES, BusinessApplication,
    BusinessApplicationFieldAliases, BusinessApplicationFieldValue,
    BusinessApplicationHydrationOptions, BusinessApplicationLookup,
    BusinessApplicationRelationshipDirection, BusinessApplicationRelationshipType,
    BusinessApplicationServerApplication, BusinessApplicationServerInventoryHealth,
    BusinessApplicationServerPath, BusinessApplicationServerPathEdge,
    BusinessApplicationServerPathEdgeSource, BusinessApplicationServerProvenance,
    BusinessApplicationServersCachedOptions, BusinessApplicationServersCachedParams,
    BusinessApplicationServersCachedResult, BusinessApplicationServersCachedSelector,
    BusinessApplicationServersOptions, BusinessApplicationServersParams,
    BusinessApplicationServersResult, BusinessApplicationServersSelector,
    BusinessApplicationServersSummary, BusinessApplicationSyncSummary,
    BusinessApplicationsForServerOptions, BusinessApplicationsForServerParams,
    BusinessApplicationsForServerResult, BusinessApplicationsForServerSelector,
    CachedBusinessApplicationForServer, CachedBusinessApplicationServer,
    CachedServerBusinessApplications, ChoiceValue, CiOwnerGroupRef, EndpointResolutionStatus,
    FallbackStrategy, ReferencePrimitiveDescriptor, ReferencePrimitiveType,
    ReferenceResolutionDiagnostic, ReferenceResolutionReason, ReferenceResolutionStatus,
    RelationshipKnowledgeStatus, ServerResultSource,
};
pub use resource::catalog::{CatalogChoice, CatalogItem, CatalogSubmitResult, CatalogVariable};
pub use resource::change::{ChangeWriteConcurrency, ChangeWriteResult};
pub use resource::resource_plan::{
    ResolvedResourceFilter, ResourcePlanListError, ResourcePlanListInput, ResourcePlanListResponse,
    ResourcePlanListWarning, ResourcePlanParentRef, ResourcePlanParentType,
    ResourcePlanQuerySummary, ResourcePlanRecord, ResourcePlanResource, ResourcePlanResourceRef,
    ResourcePlanResourceType, ResourcePlanState, ResourcePlanStateFilter,
    ResourcePlanWriteConcurrency, ResourcePlanWriteResult, TaskSelector, ValidatedListQuery,
    resource_plan_record_from_row, validate_list_input,
};
pub use resource::server::{
    LINUX_SERVER_TABLE, SERVER_RESOURCE_TYPE, SERVER_TABLE, SERVER_TABLES, Server, ServerLookup,
    ServerQuery, ServerSearchParams, WINDOWS_SERVER_TABLE,
};
pub use resource::story::{StoryWriteConcurrency, StoryWriteResult};
pub use resource::timecard::{
    CardSelector, SetMode, SimpleRef, TimeCard, TimeValue, TimecardSheet, UserRef, WeekSelector,
    Weekday,
};
pub use service::approval::{
    APPROVAL_GROUP_IN_BATCH_SIZE, ApprovalQuerySummary, ApprovalRecord, ApprovalRoutedVia,
    ListMyApprovalsResponse,
};
pub use service::user::{UserLookup, UserLookupResult, UserRecord, UserSearch};
pub use servicenow_rs::model::reference::{
    Reference, choose_reference_display_name, is_opaque_sys_id,
};
pub use servicenow_rs::model::resource::ResourceType;
pub use servicenow_rs::prelude::AttachmentMetadata;
pub use sla::{
    TaskSlaParentRef, TaskSlaReadability, TaskSlaStatus, TaskSlaSummaryView, TaskSlaView,
    is_task_sla_applicable_table,
};
pub use types::*;

use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use servicenow_rs::prelude::{
    DisplayValue, Error as SnowApiError, Operator, Order, Record, ServiceNowClient,
    child_relation_for_table,
};
use servicenow_rs::query::TableApi;

use crate::cache::store::{
    BusinessApplicationFieldDictionaryRow, BusinessApplicationServerInventoryHealthRow,
    BusinessApplicationServerMembershipRow, PrimitiveObjectRow, PrimitiveResolutionStatus,
    ProjectedFieldRow,
};
use crate::query::filter::{BusinessApplicationQuery, ListQuery};
use crate::resource::server::{SERVER_LEAF_TABLES, canonical_server_class, is_server_class};
use crate::semantic::{
    EmbeddingProvider, OllamaEmbeddingProvider, content_hash, cosine_similarity,
    maybe_exact_kb_identifier, normalize_title_match, reciprocal_rank_fusion_score,
    render_embedding_input, sanitize_semantic_text,
};
use crate::vault::manager::VaultManager;
use crate::vault::{VaultDocument, scan_documents, scan_documents_detailed};

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
    "notes",
    "sys_updated_on",
];
const RESOURCE_PLAN_LIST_FIELDS: &[&str] = &[
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
    "notes",
    "u_description",
    "sys_updated_on",
];
const RESOURCE_PLAN_LIST_DOT_WALK: &[&str] = &["task.number", "task.sys_class_name"];
const TIME_SHEET_FIELDS: &[&str] = &[
    "sys_id",
    "user",
    "user.user_name",
    "user.email",
    "user.name",
    "week_starts_on",
    "state",
];
const TIME_CARD_FIELDS: &[&str] = &[
    "sys_id",
    "time_sheet",
    "week_starts_on",
    "user",
    "user.user_name",
    "user.email",
    "user.name",
    "task",
    "task.number",
    "task.sys_class_name",
    "category",
    "project_time_category",
    "sunday",
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "total",
    "state",
    "sys_updated_on",
    "sys_mod_count",
];
const BUSINESS_APPLICATION_TABLE: &str = "cmdb_ci_business_app";
const BUSINESS_APPLICATION_DEFAULT_LIMIT: usize = 20;
const BUSINESS_APPLICATION_MAX_LIMIT: usize = 100;
const BUSINESS_APPLICATION_SYNC_ALL_PAGE_SIZE: usize = BUSINESS_APPLICATION_MAX_LIMIT;
const CMDB_CI_TABLE: &str = "cmdb_ci";
const CMDB_REL_CI_TABLE: &str = "cmdb_rel_ci";
const CMDB_REL_TYPE_TABLE: &str = "cmdb_rel_type";
const SVC_CI_ASSOC_TABLE: &str = "svc_ci_assoc";
const BUSINESS_APPLICATION_RELATIONSHIP_FIELDS: &[&str] = &[
    "sys_id",
    "parent",
    "child",
    "type",
    "parent.sys_class_name",
    "child.sys_class_name",
];
const BUSINESS_APPLICATION_CI_CLASS_FIELDS: &[&str] = &["sys_id", "name", "sys_class_name"];
const BUSINESS_APPLICATION_SERVICE_MEMBERSHIP_FIELDS: &[&str] = &[
    "sys_id",
    "service_id",
    "service_id.sys_class_name",
    "ci_id",
    "ci_id.sys_class_name",
];
/// Page size for paginated `cmdb_rel_ci` edge reads. Kept well below the typical
/// `glide.rest.table.max_record_count` server cap (default 10000) so the read
/// never relies on a single oversized request that the instance could silently
/// truncate. The paginator continues across pages until the `max_edges` budget
/// is reached or the result set is exhausted.
const BUSINESS_APPLICATION_RELATIONSHIP_PAGE_SIZE: usize = 1000;
const BUSINESS_APPLICATION_SERVICE_MEMBERSHIP_PAGE_SIZE: usize = 1000;

#[derive(Debug, Clone, PartialEq, Eq)]
struct BusinessApplicationRelationshipEdge {
    parent_sys_id: String,
    child_sys_id: String,
    parent_class: Option<String>,
    child_class: Option<String>,
    relationship_type: BusinessApplicationRelationshipType,
}

fn child_relation_for_parent_table(table_name: &str) -> Option<(&'static str, &'static str)> {
    match table_name {
        "pm_project" | "dmn_demand" => Some(("resource_plan", "task")),
        _ => child_relation_for_table(table_name),
    }
}

fn work_record_cache_is_fresh(
    record: &SnowRecord,
    now: DateTime<Utc>,
    ttl: chrono::Duration,
) -> bool {
    now.signed_duration_since(record.synced_at) <= ttl
}

impl BusinessApplicationSearchParams {
    pub fn validate(&self) -> Result<()> {
        self.validated_limit()?;
        for (name, value) in [
            ("attested_date", self.attested_date.as_deref()),
            (
                "attested_date_on_or_after",
                self.attested_date_on_or_after.as_deref(),
            ),
            (
                "attested_date_on_or_before",
                self.attested_date_on_or_before.as_deref(),
            ),
        ] {
            if let Some(value) = non_empty_owned(value)
                && parse_servicenow_date(Some(&value)).is_none()
            {
                anyhow::bail!("`{name}` must be YYYY-MM-DD");
            }
        }
        Ok(())
    }

    fn validated_limit(&self) -> Result<usize> {
        let limit = self.limit.unwrap_or(BUSINESS_APPLICATION_DEFAULT_LIMIT);
        if limit == 0 {
            anyhow::bail!("`limit` must be at least 1");
        }
        if limit > BUSINESS_APPLICATION_MAX_LIMIT {
            anyhow::bail!("`limit` must be at most {BUSINESS_APPLICATION_MAX_LIMIT}");
        }
        Ok(limit)
    }
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
pub struct FieldChoice {
    pub label: String,
    pub value: String,
    #[serde(default)]
    pub terminal: bool,
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

pub(crate) fn canonical_record_table(table: &str) -> String {
    let normalized = normalize_table_name(table);
    if resource::business_application::is_business_application_alias(&normalized) {
        BUSINESS_APPLICATION_TABLE.to_string()
    } else if resource::server::is_server_alias(&normalized) {
        match resource::server::canonical_server_table_alias(&normalized).as_str() {
            SERVER_RESOURCE_TYPE => SERVER_TABLE.to_string(),
            table => table.to_string(),
        }
    } else if is_change_request_table(&normalized) {
        "change_request".to_string()
    } else {
        normalized
    }
}

pub(crate) fn canonical_record_table_for_number(table: &str, number: &str) -> String {
    let normalized = normalize_table_name(table);
    if resource::business_application::is_business_application_alias(&normalized) {
        BUSINESS_APPLICATION_TABLE.to_string()
    } else if is_change_request_table(&normalized) || is_change_request_number(number) {
        "change_request".to_string()
    } else {
        normalized
    }
}

pub fn normalize_record_lookup_sys_id(sys_id: &str) -> Result<String> {
    let normalized = sys_id.trim().to_ascii_lowercase();
    if normalized.len() != 32 || !normalized.chars().all(|ch| ch.is_ascii_hexdigit()) {
        anyhow::bail!("sys_id must be exactly 32 ASCII hex characters");
    }
    Ok(normalized)
}

pub fn normalize_record_lookup_table(table: &str) -> Result<String> {
    let normalized = normalize_table_name(table);
    if resource::business_application::is_business_application_alias(&normalized) {
        return Ok(BUSINESS_APPLICATION_TABLE.to_string());
    }
    if resource::server::is_server_alias(&normalized) {
        return Ok(
            match resource::server::canonical_server_table_alias(&normalized).as_str() {
                SERVER_RESOURCE_TYPE => SERVER_TABLE.to_string(),
                table => table.to_string(),
            },
        );
    }
    if is_record_lookup_table_allowed(&normalized) {
        Ok(normalized)
    } else {
        anyhow::bail!("table `{}` is not allowed for record lookup", table.trim());
    }
}

pub fn is_record_lookup_table_allowed(table: &str) -> bool {
    matches!(
        table.trim().to_ascii_lowercase().as_str(),
        "dmn_demand"
            | "dmn_demand_task"
            | "resource_plan"
            | "pm_project"
            | "change_request"
            | "business_application"
            | "business_app"
            | "cmdb_ci_business_app"
            | "server"
            | "servers"
            | "cmdb_ci_server"
            | "cmdb_ci_linux_server"
            | "cmdb_ci_win_server"
    )
}

pub const RECORD_LOOKUP_ALLOWED_TABLES: &[&str] = &[
    "dmn_demand",
    "dmn_demand_task",
    "resource_plan",
    "pm_project",
    "change_request",
    "business_application",
    "business_app",
    "cmdb_ci_business_app",
    "server",
    "cmdb_ci_server",
    "cmdb_ci_linux_server",
    "cmdb_ci_win_server",
];

pub fn table_for_builtin_record_number(number: &str) -> Option<&'static str> {
    match record_number_prefix(number)?.as_str() {
        "DMNTSK" => Some("dmn_demand_task"),
        _ => None,
    }
}

fn resource_plan_parent_table_for_number(number: &str) -> Option<&'static str> {
    match record_number_prefix(number)?.as_str() {
        "DMND" => Some("dmn_demand"),
        "PRJ" => Some("pm_project"),
        _ => None,
    }
}

fn record_number_prefix(number: &str) -> Option<String> {
    let number = number.trim();
    if number.is_empty() {
        return None;
    }
    let prefix = number
        .chars()
        .take_while(|ch| ch.is_ascii_alphabetic())
        .collect::<String>();
    if prefix.is_empty() {
        None
    } else {
        Some(prefix.to_ascii_uppercase())
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

fn time_sheet_row_contains_date(row: &Record, date: NaiveDate) -> bool {
    parse_servicenow_date(
        row.get_raw("week_starts_on")
            .or_else(|| row.get_display("week_starts_on"))
            .or_else(|| row.get_str("week_starts_on")),
    )
    .map(|start| date >= start && date < start + chrono::Duration::days(7))
    .unwrap_or(false)
}

fn parse_servicenow_date(value: Option<&str>) -> Option<NaiveDate> {
    let value = value?.trim();
    let date = value.get(..10).unwrap_or(value);
    NaiveDate::parse_from_str(date, "%Y-%m-%d").ok()
}

fn apply_reference_name_or_sys_id_filter(
    query: TableApi,
    field: &str,
    value: Option<&str>,
) -> Result<TableApi> {
    let Some(value) = non_empty_owned(value) else {
        return Ok(query);
    };
    if let Ok(sys_id) = normalize_record_lookup_sys_id(&value) {
        Ok(query.equals(field, &sys_id))
    } else {
        Ok(query.contains(&format!("{field}.name"), &value))
    }
}

fn normalize_operational_state(value: &str) -> String {
    let normalized = value.trim().to_ascii_lowercase().replace(['_', '-'], " ");
    match normalized.as_str() {
        "operational" => "1".to_string(),
        "non operational" | "nonoperational" | "not operational" => "2".to_string(),
        "repair in progress" => "3".to_string(),
        "dr standby" | "disaster recovery standby" => "4".to_string(),
        "ready" => "5".to_string(),
        "retired" => "6".to_string(),
        _ => value.trim().to_string(),
    }
}

fn roll_up_business_application_summary(
    summary: &mut BusinessApplicationSyncSummary,
    applications: &[BusinessApplication],
) {
    for application in applications {
        // Resolved references are tracked on descriptors; unresolved references
        // surface as diagnostics. Count each independently so totals stay
        // meaningful even when a reference appears in both lists.
        summary.references_resolved += application
            .references
            .iter()
            .filter(|descriptor| {
                descriptor.resolution_status == ReferenceResolutionStatus::Resolved
            })
            .count();
        for diagnostic in &application.unresolved_references {
            summary.references_unresolved += 1;
            *summary
                .degraded_reasons
                .entry(diagnostic.reason.as_key().to_string())
                .or_insert(0) += 1;
            if diagnostic.reason.is_dictionary_unavailable() {
                summary.dictionary_degraded = true;
            }
        }
    }
}

/// Result of a single side-effect-free relationship-direction read.
///
/// Carries the collected `cmdb_rel_ci` rows plus the flags the merge step needs
/// to update the shared traversal accounting. Keeping this separate from
/// `summary`/`diagnostics` is what lets the two directions run concurrently:
/// neither read touches shared state, and the merge applies side effects in a
/// deterministic order.
struct BusinessApplicationDirectionRead {
    /// The `parent`/`child` field this read traversed (used for diagnostics).
    field: &'static str,
    /// Collected edge rows, already bounded to the per-read `remaining` budget.
    records: Vec<Record>,
    /// Set when the read consumed its budget while more pages remained.
    edge_limit_reached: bool,
    /// Set when a 401/403 ACL error stopped the read.
    acl_restricted: bool,
}

#[derive(Debug, Clone)]
struct BusinessApplicationServiceMembershipRead {
    records: Vec<Record>,
    pages_examined: usize,
    association_limit_reached: bool,
    page_limit_reached: bool,
    acl_restricted: bool,
}

impl BusinessApplicationServiceMembershipRead {
    fn new() -> Self {
        Self {
            records: Vec::new(),
            pages_examined: 0,
            association_limit_reached: false,
            page_limit_reached: false,
            acl_restricted: false,
        }
    }
}

#[derive(Debug, Clone)]
struct BusinessApplicationServerDiscovery {
    server: Server,
    provenance: BusinessApplicationServerProvenance,
    relationship_paths: Vec<BusinessApplicationServerPath>,
    service_membership_paths: Vec<BusinessApplicationServerPath>,
}

impl BusinessApplicationServerDiscovery {
    fn new(
        server: Server,
        provenance: BusinessApplicationServerProvenance,
        paths: Vec<BusinessApplicationServerPath>,
    ) -> Self {
        let mut discovery = Self {
            server,
            provenance,
            relationship_paths: Vec::new(),
            service_membership_paths: Vec::new(),
        };
        discovery.add_paths(provenance, paths);
        discovery
    }

    fn add_paths(
        &mut self,
        provenance: BusinessApplicationServerProvenance,
        paths: Vec<BusinessApplicationServerPath>,
    ) {
        self.provenance = self.provenance.merge(provenance);
        match provenance {
            BusinessApplicationServerProvenance::Relationship => {
                self.relationship_paths.extend(paths);
            }
            BusinessApplicationServerProvenance::ServiceMembership => {
                self.service_membership_paths.extend(paths);
            }
            BusinessApplicationServerProvenance::Both => {
                self.relationship_paths.extend(paths.clone());
                self.service_membership_paths.extend(paths);
            }
        }
    }

    fn paths(&self) -> Vec<BusinessApplicationServerPath> {
        let mut paths = self.relationship_paths.clone();
        paths.extend(self.service_membership_paths.clone());
        paths
    }
}

impl BusinessApplicationDirectionRead {
    fn new(field: &'static str) -> Self {
        Self {
            field,
            records: Vec::new(),
            edge_limit_reached: false,
            acl_restricted: false,
        }
    }
}

impl BusinessApplicationRelationshipEdge {
    fn key(&self) -> (String, String, String, Option<String>) {
        (
            self.parent_sys_id.clone(),
            self.child_sys_id.clone(),
            self.relationship_type.value.clone(),
            self.relationship_type.display_value.clone(),
        )
    }

    fn traversal_endpoint(
        &self,
        frontier: &HashSet<String>,
    ) -> Option<(String, String, BusinessApplicationRelationshipDirection)> {
        if frontier.contains(&self.parent_sys_id) {
            Some((
                self.parent_sys_id.clone(),
                self.child_sys_id.clone(),
                BusinessApplicationRelationshipDirection::ParentToChild,
            ))
        } else if frontier.contains(&self.child_sys_id) {
            Some((
                self.child_sys_id.clone(),
                self.parent_sys_id.clone(),
                BusinessApplicationRelationshipDirection::ChildToParent,
            ))
        } else {
            None
        }
    }

    /// Build a path edge for the route bookkeeping. The traversal endpoints
    /// (`from`/`to`) are NOT stored: they are derived on demand from
    /// `parent_sys_id`/`child_sys_id`/`direction` via
    /// [`BusinessApplicationServerPathEdge::from_sys_id`] /
    /// [`BusinessApplicationServerPathEdge::to_sys_id`], so the caller need only
    /// supply the BFS `direction` it crossed the edge in.
    fn path_edge(
        &self,
        depth: usize,
        direction: BusinessApplicationRelationshipDirection,
    ) -> BusinessApplicationServerPathEdge {
        BusinessApplicationServerPathEdge {
            depth,
            parent_sys_id: self.parent_sys_id.clone(),
            child_sys_id: self.child_sys_id.clone(),
            direction,
            relationship_type: self.relationship_type.clone(),
            edge_source: BusinessApplicationServerPathEdgeSource::Relationship,
        }
    }
}

fn business_application_relationship_edge_from_record(
    record: &Record,
    summary: &mut BusinessApplicationServersSummary,
    diagnostics: &mut Vec<ReferenceResolutionDiagnostic>,
) -> Option<BusinessApplicationRelationshipEdge> {
    let parent_sys_id = servicenow_reference_sys_id(record, "parent");
    let child_sys_id = servicenow_reference_sys_id(record, "child");
    let (Some(parent_sys_id), Some(child_sys_id)) = (parent_sys_id, child_sys_id) else {
        push_business_application_server_diagnostic(
            summary,
            diagnostics,
            "cmdb_rel_ci",
            CMDB_REL_CI_TABLE,
            "",
            ReferenceResolutionReason::ReferenceResolutionFailed,
            "relationship row was missing a readable parent or child reference",
        );
        return None;
    };

    Some(BusinessApplicationRelationshipEdge {
        parent_sys_id,
        child_sys_id,
        parent_class: servicenow_record_text(record, "parent.sys_class_name"),
        child_class: servicenow_record_text(record, "child.sys_class_name"),
        relationship_type: BusinessApplicationRelationshipType {
            value: servicenow_record_raw_text(record, "type").unwrap_or_default(),
            display_value: servicenow_record_display_text(record, "type"),
        },
    })
}

/// All CI sys_ids that lie along a recorded route chain, i.e. every CI the
/// route passes through. A chain is a list of edges; the CIs it touches are the
/// `from_sys_id` of the first edge plus the `to_sys_id` of each edge.
fn chain_node_sys_ids(chain: &[BusinessApplicationServerPathEdge]) -> HashSet<String> {
    let mut nodes = HashSet::new();
    if let Some(first) = chain.first() {
        nodes.insert(first.from_sys_id().to_string());
    }
    for edge in chain {
        nodes.insert(edge.to_sys_id().to_string());
    }
    nodes
}

/// Decide whether reaching `adjacent_sys_id` from `current_sys_id` is a true
/// back-edge (cycle) rather than an alternate forward path.
///
/// It is a back-edge when `adjacent_sys_id` is an ancestor of
/// `current_sys_id` — that is, the adjacent CI already lies on some route that
/// leads to the current CI, so following this edge would loop back up the graph.
/// If the adjacent CI does not appear on any of the current CI's chains, the
/// edge is a genuine alternate route to an already-visited node (a diamond
/// join), not a cycle.
///
/// When the current CI has no recorded chains (paths disabled or unknown), we
/// conservatively treat the re-visit as a back-edge to preserve the prior
/// cycle-counting behavior.
fn path_chains_contain_ancestor(
    current_chains: Option<&Vec<Vec<BusinessApplicationServerPathEdge>>>,
    adjacent_sys_id: &str,
    current_sys_id: &str,
) -> bool {
    let Some(chains) = current_chains else {
        return true;
    };
    if chains.is_empty() {
        return true;
    }
    // The current CI itself is trivially "on" its own path; reaching it again is
    // a self-loop / back-edge.
    if adjacent_sys_id == current_sys_id {
        return true;
    }
    chains
        .iter()
        .any(|chain| chain_node_sys_ids(chain).contains(adjacent_sys_id))
}

fn business_application_server_paths_for(
    paths_by_ci: &HashMap<String, Vec<Vec<BusinessApplicationServerPathEdge>>>,
    sys_id: &str,
) -> Vec<BusinessApplicationServerPath> {
    paths_by_ci
        .get(sys_id)
        .map(|chains| {
            chains
                .iter()
                .map(|chain| BusinessApplicationServerPath {
                    edges: chain.clone(),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn business_application_service_membership_paths_for(
    service_paths_by_ci: &HashMap<String, Vec<Vec<BusinessApplicationServerPathEdge>>>,
    service_sys_id: &str,
    server_sys_id: &str,
) -> Vec<BusinessApplicationServerPath> {
    let base_chains = service_paths_by_ci
        .get(service_sys_id)
        .cloned()
        .unwrap_or_default();
    if base_chains.is_empty() {
        return vec![BusinessApplicationServerPath {
            edges: vec![BusinessApplicationServerPathEdge {
                depth: 1,
                parent_sys_id: service_sys_id.to_string(),
                child_sys_id: server_sys_id.to_string(),
                direction: BusinessApplicationRelationshipDirection::ParentToChild,
                relationship_type: BusinessApplicationRelationshipType {
                    value: "service_member_of".to_string(),
                    display_value: Some("service_member_of".to_string()),
                },
                edge_source: BusinessApplicationServerPathEdgeSource::ServiceMembership,
            }],
        }];
    }

    base_chains
        .into_iter()
        .map(|mut chain| {
            let depth = chain.len() + 1;
            chain.push(BusinessApplicationServerPathEdge {
                depth,
                parent_sys_id: service_sys_id.to_string(),
                child_sys_id: server_sys_id.to_string(),
                direction: BusinessApplicationRelationshipDirection::ParentToChild,
                relationship_type: BusinessApplicationRelationshipType {
                    value: "service_member_of".to_string(),
                    display_value: Some("service_member_of".to_string()),
                },
                edge_source: BusinessApplicationServerPathEdgeSource::ServiceMembership,
            });
            BusinessApplicationServerPath { edges: chain }
        })
        .collect()
}

fn merge_business_application_server_discovery(
    discoveries: &mut BTreeMap<String, BusinessApplicationServerDiscovery>,
    server: Server,
    provenance: BusinessApplicationServerProvenance,
    paths: Vec<BusinessApplicationServerPath>,
) {
    let sys_id = server.record.sys_id.clone();
    discoveries
        .entry(sys_id)
        .and_modify(|entry| entry.add_paths(provenance, paths.clone()))
        .or_insert_with(|| BusinessApplicationServerDiscovery::new(server, provenance, paths));
}

/// Extend every recorded route chain of `parent_sys_id` by `edge` and append the
/// resulting chains to `child_sys_id`'s recorded routes.
///
/// On first discovery this seeds the child's routes; on a diamond join it adds
/// the alternate route(s) without disturbing the existing ones. The child's
/// existing chains are preserved (so multiple distinct parents accumulate).
fn extend_path_chains(
    paths_by_ci: &mut HashMap<String, Vec<Vec<BusinessApplicationServerPathEdge>>>,
    parent_sys_id: &str,
    child_sys_id: &str,
    edge: BusinessApplicationServerPathEdge,
) -> bool {
    let new_chains = extended_path_chains(paths_by_ci, parent_sys_id, edge);
    if new_chains.is_empty() {
        return false;
    }
    paths_by_ci
        .entry(child_sys_id.to_string())
        .or_default()
        .extend(new_chains);
    true
}

fn extended_path_chains(
    paths_by_ci: &HashMap<String, Vec<Vec<BusinessApplicationServerPathEdge>>>,
    parent_sys_id: &str,
    edge: BusinessApplicationServerPathEdge,
) -> Vec<Vec<BusinessApplicationServerPathEdge>> {
    let parent_chains = paths_by_ci.get(parent_sys_id).cloned().unwrap_or_default();
    if parent_chains.is_empty() {
        // Parent has no recorded chain (e.g. paths disabled): record a
        // single-edge chain so traversal bookkeeping still has one route.
        vec![vec![edge]]
    } else {
        parent_chains
            .into_iter()
            .filter_map(|mut chain| {
                if let Some(first) = chain.first()
                    && first.direction != edge.direction
                {
                    return None;
                }
                chain.push(edge.clone());
                Some(chain)
            })
            .collect()
    }
}

/// Emit any not-yet-recorded route chains for an already-collected server.
///
/// Used when a server is reached again via an alternate diamond route after it
/// was first hydrated: its newly-added chains must be reflected in
/// `server_paths`. Only chains beyond those already present are appended, so
/// repeated edges in a single level do not double-count.
fn emit_alternate_server_path(
    servers_by_sys_id: &BTreeMap<String, Server>,
    paths_by_ci: &HashMap<String, Vec<Vec<BusinessApplicationServerPathEdge>>>,
    server_paths: &mut BTreeMap<String, Vec<BusinessApplicationServerPath>>,
    sys_id: &str,
) {
    if !servers_by_sys_id.contains_key(sys_id) {
        return;
    }
    let Some(chains) = paths_by_ci.get(sys_id) else {
        return;
    };
    let entry = server_paths.entry(sys_id.to_string()).or_default();
    // Re-sync the emitted routes with the recorded chains: append the chains
    // that have not yet been emitted (the new alternate routes are at the tail).
    while entry.len() < chains.len() {
        let chain = &chains[entry.len()];
        entry.push(BusinessApplicationServerPath {
            edges: chain.clone(),
        });
    }
}

fn servicenow_record_display_text(record: &Record, field: &str) -> Option<String> {
    record
        .get_display(field)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn mark_business_application_edge_limit(
    summary: &mut BusinessApplicationServersSummary,
    diagnostics: &mut Vec<ReferenceResolutionDiagnostic>,
) {
    if summary.edge_limit_reached {
        return;
    }
    summary.edge_limit_reached = true;
    summary.mark_truncated(1);
    push_business_application_server_diagnostic(
        summary,
        diagnostics,
        "cmdb_rel_ci",
        CMDB_REL_CI_TABLE,
        "",
        ReferenceResolutionReason::FanoutLimitExceeded,
        "max_edges prevented reading more relationships",
    );
}

fn push_business_application_server_diagnostic(
    summary: &mut BusinessApplicationServersSummary,
    diagnostics: &mut Vec<ReferenceResolutionDiagnostic>,
    field: impl Into<String>,
    reference_table: impl Into<String>,
    reference_sys_id: impl Into<String>,
    reason: ReferenceResolutionReason,
    message: impl Into<String>,
) {
    summary.record_degraded_reason(&reason);
    diagnostics.push(ReferenceResolutionDiagnostic {
        field: field.into(),
        reference_table: reference_table.into(),
        reference_sys_id: reference_sys_id.into(),
        display_value: None,
        reason,
        message: Some(message.into()),
    });
}

fn is_business_application_reference_table_resolvable(table: &str) -> bool {
    matches!(table, "sys_user" | "sys_user_group" | "cmdb_ci")
        || table == BUSINESS_APPLICATION_TABLE
        || table.contains("portfolio")
}

fn is_application_service_class(class_name: Option<&str>) -> bool {
    let Some(class_name) = class_name.map(str::trim).filter(|value| !value.is_empty()) else {
        return false;
    };
    let normalized = class_name.to_ascii_lowercase();
    normalized == "cmdb_ci_service"
        || normalized.starts_with("cmdb_ci_service_")
        || normalized.contains("_application_service")
}

fn is_servicenow_acl_error(err: &SnowApiError) -> bool {
    match err {
        SnowApiError::Api { status, .. } if *status == 401 || *status == 403 => true,
        _ => {
            let message = err.to_string().to_ascii_lowercase();
            message.contains("authentication failed")
                || message.contains("forbidden")
                || message.contains("unauthorized")
        }
    }
}

/// Structured outcome for a live `server_get` fetch.
///
/// The read-through `server_get` contract distinguishes authoritative
/// not-found (ServiceNow confirmed the CI does not exist or is unreadable for
/// ACL reasons) from transient failures. Collapsing all of these into a single
/// `anyhow` error would make a network blip indistinguishable from a real
/// not-found, which the FR explicitly forbids. Each variant carries enough to
/// let the daemon and MCP layers map it to a distinct JSON-RPC error code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerGetError {
    /// ServiceNow confirmed the record does not exist (HTTP 404 on a sys_id
    /// read, or an empty exact-match result set for a name / IP query).
    NotFound,
    /// The caller's ACL prevents reading the record (HTTP 401/403 or an
    /// authentication/authorization failure). The CI may exist.
    AclRestricted(String),
    /// A transport-level / timeout failure reaching ServiceNow. Never
    /// conflated with not-found.
    Network(String),
    /// More than one CI matched an exact name / IP selector (duplicate CIs in
    /// CMDB). Surfaced as a structured disambiguation signal, not a generic
    /// internal error.
    Disambiguation { selector: String, matched: usize },
    /// A row was returned but could not be parsed into the typed `Server`
    /// shape.
    Hydration(String),
    /// Any other failure (validation, cache write, etc.).
    Other(String),
}

impl ServerGetError {
    /// Classify a `servicenow_rs` API error into the structured variant.
    /// (HTTP 404 is handled by the caller and never reaches here.)
    fn from_api(err: SnowApiError) -> Self {
        match &err {
            SnowApiError::Api {
                status: 401 | 403, ..
            } => Self::AclRestricted(err.to_string()),
            SnowApiError::Auth { .. } => Self::AclRestricted(err.to_string()),
            SnowApiError::Http(_) | SnowApiError::RateLimited { .. } => {
                Self::Network(err.to_string())
            }
            _ if is_servicenow_acl_error(&err) => Self::AclRestricted(err.to_string()),
            _ => Self::Other(err.to_string()),
        }
    }
}

impl std::fmt::Display for ServerGetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "server not found"),
            Self::AclRestricted(detail) => {
                write!(f, "server is ACL-restricted: {detail}")
            }
            Self::Network(detail) => write!(f, "network error reaching ServiceNow: {detail}"),
            Self::Disambiguation { selector, matched } => {
                write!(f, "multiple servers matched {selector} ({matched} matches)")
            }
            Self::Hydration(detail) => write!(f, "server record failed hydration: {detail}"),
            Self::Other(detail) => write!(f, "{detail}"),
        }
    }
}

impl std::error::Error for ServerGetError {}

/// Build a cached dictionary row from a `sys_dictionary` record.
///
/// Returns `None` for rows without a usable `element` (the ServiceNow field
/// name), which can happen for collection/placeholder dictionary rows. The
/// `table_name` is the table the query was scoped to so inherited fields are
/// attributed to the level that defines them.
fn dictionary_row_from_record(
    table_name: &str,
    record: &Record,
    synced_at: DateTime<Utc>,
) -> Option<BusinessApplicationFieldDictionaryRow> {
    let field_name = non_empty_owned(record.get_raw("element"))
        .or_else(|| non_empty_owned(record.get_str("element")))?;
    let internal_type = record_field_raw_or_display(record, "internal_type");
    let reference_table = non_empty_owned(record.get_raw("reference"))
        .or_else(|| record_field_display_or_raw(record, "reference"));
    let raw_json = serde_json::json!({
        "element": field_name,
        "column_label": record_field_display_or_raw(record, "column_label"),
        "internal_type": internal_type,
        "reference": reference_table,
        "choice": record_field_raw_or_display(record, "choice"),
        "mandatory": record_field_raw_or_display(record, "mandatory"),
        "read_only": record_field_raw_or_display(record, "read_only"),
        "max_length": record_field_raw_or_display(record, "max_length"),
        "active": record_field_raw_or_display(record, "active"),
    })
    .to_string();
    Some(BusinessApplicationFieldDictionaryRow {
        table_name: table_name.to_string(),
        field_name,
        field_label: record_field_display_or_raw(record, "column_label"),
        field_type: internal_type,
        // `choice` in sys_dictionary is a numeric flag ("1"/"3" => choice list);
        // treat any non-empty, non-zero value as a choice field.
        reference_table,
        choice: dictionary_flag_is_set(record, "choice"),
        mandatory: record_bool(record, "mandatory"),
        read_only: record_bool(record, "read_only"),
        max_length: record_field_raw_or_display(record, "max_length")
            .and_then(|value| value.parse::<i64>().ok()),
        active: record_bool(record, "active"),
        synced_at,
        raw_json,
    })
}

/// Interpret a `sys_dictionary` flag field. `choice` is numeric (0/1/2/3); any
/// non-empty, non-"0" value counts as set. Boolean-style flags ("true") also
/// count.
fn dictionary_flag_is_set(record: &Record, field: &str) -> bool {
    match record_field_raw_or_display(record, field) {
        Some(value) => {
            let value = value.trim().to_ascii_lowercase();
            !value.is_empty() && value != "0" && value != "false" && value != "no"
        }
        None => false,
    }
}

/// Promote the baseline alias map to dictionary-verified fields.
///
/// For each typed product label we keep the baseline ServiceNow field name when
/// the dictionary confirms it exists, otherwise we keep the baseline (the
/// dictionary may not expose every field to the authenticated account). The
/// Primary Portfolio reference target table is taken from the dictionary
/// `reference` value when present. `dictionary_version` is set to the latest
/// `synced_at` so callers can tell typed aliases were dictionary-verified.
fn business_application_aliases_from_dictionary(
    dictionary: &HashMap<String, BusinessApplicationFieldDictionaryRow>,
) -> BusinessApplicationFieldAliases {
    let mut aliases = BusinessApplicationFieldAliases::baseline();

    // Helper: keep the baseline field name if the dictionary knows it; otherwise
    // fall back to the first dictionary field whose label matches the product
    // label. This lets instance-specific custom `u_*` fields supersede the baseline.
    let resolve = |baseline: &str, labels: &[&str]| -> String {
        if dictionary.contains_key(baseline) {
            return baseline.to_string();
        }
        dictionary
            .values()
            .find(|row| {
                row.field_label
                    .as_deref()
                    .map(|label| {
                        let label = label.trim().to_ascii_lowercase();
                        labels.iter().any(|candidate| label == *candidate)
                    })
                    .unwrap_or(false)
            })
            .map(|row| row.field_name.clone())
            .unwrap_or_else(|| baseline.to_string())
    };

    aliases.business_owner = resolve("business_owner", &["business owner"]);
    aliases.is_owner = resolve(
        "it_application_owner",
        &["is owner", "it application owner"],
    );
    aliases.ci_owner_group = resolve("managed_by_group", &["ci owner group"]);
    aliases.primary_support_group = resolve("support_group", &["primary support group"]);
    aliases.operational_state = resolve("operational_status", &["operational state"]);
    aliases.primary_portfolio = resolve("portfolio", &["primary portfolio"]);
    aliases.attested_date = resolve("attested_date", &["attested date"]);

    // Discover the Primary Portfolio reference target table from the dictionary.
    aliases.primary_portfolio_table = dictionary
        .get(&aliases.primary_portfolio)
        .and_then(|row| row.reference_table.clone())
        .filter(|table| !table.is_empty());

    aliases.dictionary_version = dictionary
        .values()
        .map(|row| row.synced_at)
        .max()
        .map(|synced_at| synced_at.to_rfc3339());

    aliases
}

fn primitive_resource_type_name(primitive_type: &ReferencePrimitiveType) -> &'static str {
    match primitive_type {
        ReferencePrimitiveType::UserPrimitive => "user_primitive",
        ReferencePrimitiveType::GroupPrimitive => "group_primitive",
        ReferencePrimitiveType::PortfolioPrimitive => "portfolio_primitive",
        ReferencePrimitiveType::ConfigurationItemPrimitive => "configuration_item_primitive",
        ReferencePrimitiveType::ReferencedRecordPrimitive => "referenced_record_primitive",
    }
}

fn primitive_status_from_reference_status(
    status: ReferenceResolutionStatus,
) -> PrimitiveResolutionStatus {
    match status {
        ReferenceResolutionStatus::Resolved => PrimitiveResolutionStatus::Resolved,
        ReferenceResolutionStatus::Unresolved => PrimitiveResolutionStatus::Unresolved,
        ReferenceResolutionStatus::UnknownTable => PrimitiveResolutionStatus::UnknownTable,
        ReferenceResolutionStatus::NotFound => PrimitiveResolutionStatus::NotFound,
        ReferenceResolutionStatus::AclRestricted => PrimitiveResolutionStatus::AclRestricted,
        ReferenceResolutionStatus::Error => PrimitiveResolutionStatus::Error,
    }
}

fn reason_from_reference_status(status: ReferenceResolutionStatus) -> ReferenceResolutionReason {
    match status {
        ReferenceResolutionStatus::UnknownTable => ReferenceResolutionReason::UnknownReferenceTable,
        ReferenceResolutionStatus::NotFound => ReferenceResolutionReason::ReferenceNotFound,
        ReferenceResolutionStatus::AclRestricted => {
            ReferenceResolutionReason::ReferenceAclRestricted
        }
        ReferenceResolutionStatus::Error => ReferenceResolutionReason::ReferenceResolutionFailed,
        ReferenceResolutionStatus::Resolved | ReferenceResolutionStatus::Unresolved => {
            ReferenceResolutionReason::DictionaryUnavailable
        }
    }
}

fn reference_resolution_status_name(status: ReferenceResolutionStatus) -> &'static str {
    match status {
        ReferenceResolutionStatus::Resolved => "resolved",
        ReferenceResolutionStatus::Unresolved => "unresolved",
        ReferenceResolutionStatus::UnknownTable => "unknown_table",
        ReferenceResolutionStatus::NotFound => "not_found",
        ReferenceResolutionStatus::AclRestricted => "acl_restricted",
        ReferenceResolutionStatus::Error => "error",
    }
}

fn primitive_display_name(record: &Record, descriptor: &ReferencePrimitiveDescriptor) -> String {
    record_first_value(
        record,
        &[
            "name",
            "display_name",
            "number",
            "user_name",
            "email",
            "title",
            "short_description",
        ],
    )
    .or_else(|| descriptor.display_value.clone())
    .unwrap_or_else(|| descriptor.reference_sys_id.clone())
}

fn record_first_value(record: &Record, fields: &[&str]) -> Option<String> {
    fields.iter().find_map(|field| {
        record
            .get_display(field)
            .or_else(|| record.get_raw(field))
            .or_else(|| record.get_str(field))
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
    })
}

fn reference_primitive_relative_path(
    descriptor: &ReferencePrimitiveDescriptor,
    display_name: &str,
) -> PathBuf {
    let (dir, prefix) = match descriptor.primitive_type {
        ReferencePrimitiveType::UserPrimitive => (PathBuf::from("users"), "user".to_string()),
        ReferencePrimitiveType::GroupPrimitive => (PathBuf::from("groups"), "group".to_string()),
        ReferencePrimitiveType::PortfolioPrimitive => {
            (PathBuf::from("portfolios"), "portfolio".to_string())
        }
        ReferencePrimitiveType::ConfigurationItemPrimitive => {
            (PathBuf::from("configuration_items"), "ci".to_string())
        }
        ReferencePrimitiveType::ReferencedRecordPrimitive => {
            let table_slug = vault::layout::slugify(&descriptor.reference_table);
            (PathBuf::from("references").join(&table_slug), table_slug)
        }
    };
    let display_slug = vault::layout::slugify(display_name);
    let file_name = if display_slug.is_empty() {
        format!("{}_{}.md", prefix, descriptor.reference_sys_id)
    } else {
        format!(
            "{}_{}_{}.md",
            prefix, descriptor.reference_sys_id, display_slug
        )
    };
    dir.join(file_name)
}

fn render_reference_primitive_markdown(
    descriptor: &ReferencePrimitiveDescriptor,
    display_name: &str,
    status: ReferenceResolutionStatus,
    raw_json: &Value,
    diagnostic: Option<&str>,
) -> String {
    let mut out = String::new();
    out.push_str("---\n");
    out.push_str(&format!(
        "primitive_type: {}\n",
        yaml_json_string(primitive_resource_type_name(&descriptor.primitive_type))
    ));
    out.push_str(&format!(
        "resource_type: {}\n",
        yaml_json_string(primitive_resource_type_name(&descriptor.primitive_type))
    ));
    out.push_str(&format!(
        "sys_id: {}\n",
        yaml_json_string(&descriptor.reference_sys_id)
    ));
    out.push_str(&format!(
        "table: {}\n",
        yaml_json_string(&descriptor.reference_table)
    ));
    out.push_str(&format!(
        "display_name: {}\n",
        yaml_json_string(display_name)
    ));
    out.push_str(&format!(
        "source_field: {}\n",
        yaml_json_string(&descriptor.field)
    ));
    out.push_str(&format!(
        "resolution_status: {}\n",
        yaml_json_string(reference_resolution_status_name(status))
    ));
    if let Some(diagnostic) = diagnostic.filter(|value| !value.trim().is_empty()) {
        out.push_str(&format!("diagnostic: {}\n", yaml_json_string(diagnostic)));
    }
    out.push_str("---\n\n");
    out.push_str(&format!("# {}\n\n", display_name));
    out.push_str("```json\n");
    out.push_str(&serde_json::to_string_pretty(raw_json).unwrap_or_else(|_| raw_json.to_string()));
    out.push_str("\n```\n");
    out
}

fn primitive_projected_field(
    primitive_sys_id: &str,
    field_name: &str,
    raw_value: &Value,
    updated_at: DateTime<Utc>,
) -> ProjectedFieldRow {
    let value_text = json_field_value_text(raw_value);
    let display_value = raw_value
        .as_object()
        .and_then(|map| map.get("display_value"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let reference_sys_id = value_text
        .as_deref()
        .filter(|value| looks_like_servicenow_sys_id(value) && display_value.is_some())
        .map(ToOwned::to_owned);
    let reference_table = raw_value
        .as_object()
        .and_then(|map| map.get("link"))
        .and_then(Value::as_str)
        .and_then(reference_table_from_api_link);
    let number_text = value_text
        .as_deref()
        .and_then(|value| value.parse::<f64>().ok());
    let bool_value =
        value_text
            .as_deref()
            .and_then(|value| match value.to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" => Some(true),
                "false" | "0" | "no" => Some(false),
                _ => None,
            });
    let date_value = value_text
        .as_deref()
        .and_then(|value| value.get(..10))
        .and_then(|date| NaiveDate::parse_from_str(date, "%Y-%m-%d").ok())
        .map(|date| date.to_string());

    ProjectedFieldRow {
        owner_sys_id: primitive_sys_id.to_string(),
        field_name: field_name.to_string(),
        field_label: None,
        field_type: reference_sys_id.as_ref().map(|_| "reference".to_string()),
        value_text,
        display_value,
        value_number: number_text,
        value_date: date_value,
        value_bool: bool_value,
        reference_sys_id,
        reference_table,
        raw_json: raw_value.to_string(),
        updated_at,
    }
}

fn json_field_value_text(value: &Value) -> Option<String> {
    let scalar = value
        .as_object()
        .and_then(|map| map.get("value"))
        .unwrap_or(value);
    match scalar {
        Value::String(text) => {
            let trimmed = text.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_string())
        }
        Value::Null => None,
        Value::Bool(value) => Some(value.to_string()),
        Value::Number(value) => Some(value.to_string()),
        other => Some(other.to_string()),
    }
}

fn reference_table_from_api_link(link: &str) -> Option<String> {
    let marker = "/api/now/table/";
    let start = link.find(marker)? + marker.len();
    let table = link[start..].split('/').next()?.trim();
    (!table.is_empty()).then(|| table.to_string())
}

fn looks_like_servicenow_sys_id(value: &str) -> bool {
    let value = value.trim();
    value.len() == 32 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn yaml_json_string(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "\"\"".to_string())
}

fn push_unique_reference_diagnostic(
    diagnostics: &mut Vec<ReferenceResolutionDiagnostic>,
    diagnostic: ReferenceResolutionDiagnostic,
) {
    if diagnostics.iter().any(|existing| {
        existing.field == diagnostic.field
            && existing.reference_table == diagnostic.reference_table
            && existing.reference_sys_id == diagnostic.reference_sys_id
            && existing.reason == diagnostic.reason
    }) {
        return;
    }
    diagnostics.push(diagnostic);
}

#[derive(Clone)]
pub struct SnowCore {
    ctx: context::CoreContext,
    approvals: service::ApprovalService,
    users: service::UserService,
}

impl SnowCore {
    pub fn builder() -> SnowCoreBuilder {
        SnowCoreBuilder::default()
    }

    pub fn config(&self) -> &config::SnowConfig {
        &self.ctx.config
    }

    pub fn client(&self) -> &Arc<ServiceNowClient> {
        &self.ctx.client
    }

    pub fn vault_path(&self) -> &Path {
        &self.ctx.vault_path
    }

    pub async fn lookup_user(&self, lookup: UserLookup) -> Result<Option<UserLookupResult>> {
        self.users.lookup_user(lookup).await
    }

    pub async fn search_users(&self, params: UserSearch) -> Result<Vec<UserRecord>> {
        self.users.search_users(params).await
    }

    pub async fn search_business_applications(
        &self,
        params: BusinessApplicationSearchParams,
    ) -> Result<Vec<SnowRecord>> {
        Ok(self
            .search_business_applications_live(
                params,
                BusinessApplicationHydrationOptions::default(),
            )
            .await?
            .into_iter()
            .map(|business_application| business_application.record)
            .collect())
    }

    pub async fn get_business_application_fresh(
        &self,
        lookup: BusinessApplicationLookup,
        options: BusinessApplicationHydrationOptions,
    ) -> Result<Option<BusinessApplication>> {
        let aliases = self
            .resolve_business_application_aliases(options.refresh_dictionary)
            .await;
        let record = match lookup {
            BusinessApplicationLookup::SysId(sys_id) => match self
                .ctx
                .client
                .table(BUSINESS_APPLICATION_TABLE)
                .display_value(DisplayValue::Both)
                .exclude_reference_link(true)
                .get(&normalize_record_lookup_sys_id(&sys_id)?)
                .await
            {
                Ok(record) => Some(record),
                Err(SnowApiError::Api { status: 404, .. }) => None,
                Err(err) => return Err(err.into()),
            },
            BusinessApplicationLookup::ExactName(name) => {
                let name = non_empty_owned(Some(&name))
                    .ok_or_else(|| anyhow::anyhow!("Business Application name cannot be empty"))?;
                let records = self
                    .ctx
                    .client
                    .table(BUSINESS_APPLICATION_TABLE)
                    .equals("sys_class_name", BUSINESS_APPLICATION_TABLE)
                    .equals("name", &name)
                    .display_value(DisplayValue::Both)
                    .exclude_reference_link(true)
                    .limit(2)
                    .execute()
                    .await?
                    .records;
                if records.len() > 1 {
                    anyhow::bail!("multiple Business Applications matched name={name}");
                }
                records.into_iter().next()
            }
        };

        let Some(record) = record else {
            return Ok(None);
        };
        let mut business_application = BusinessApplication::from_servicenow(&record, &aliases)?;
        if options.persist {
            self.persist_record(&record)?;
            self.persist_business_application_reference_primitives(
                &mut business_application,
                &options,
            )
            .await?;
        }
        Ok(Some(business_application))
    }

    pub async fn search_business_applications_live(
        &self,
        params: BusinessApplicationSearchParams,
        options: BusinessApplicationHydrationOptions,
    ) -> Result<Vec<BusinessApplication>> {
        params.validate()?;
        let aliases = self
            .resolve_business_application_aliases(options.refresh_dictionary)
            .await;

        let mut query = self
            .ctx
            .client
            .table(BUSINESS_APPLICATION_TABLE)
            .equals("sys_class_name", BUSINESS_APPLICATION_TABLE)
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .limit(params.validated_limit()? as u32)
            .order_by("name", Order::Asc);

        if let Some(name) = non_empty_owned(params.name.as_deref()) {
            query = query.contains("name", &name);
        }
        query = apply_reference_name_or_sys_id_filter(
            query,
            &aliases.business_owner,
            params.business_owner.as_deref(),
        )?;
        query = apply_reference_name_or_sys_id_filter(
            query,
            &aliases.is_owner,
            params.is_owner.as_deref(),
        )?;
        query = apply_reference_name_or_sys_id_filter(
            query,
            &aliases.ci_owner_group,
            params.ci_owner_group.as_deref(),
        )?;
        query = apply_reference_name_or_sys_id_filter(
            query,
            &aliases.primary_support_group,
            params.primary_support_group.as_deref(),
        )?;
        query = apply_reference_name_or_sys_id_filter(
            query,
            &aliases.primary_portfolio,
            params.primary_portfolio.as_deref(),
        )?;
        if let Some(status) = non_empty_owned(params.operational_state.as_deref()) {
            query = query.equals(
                &aliases.operational_state,
                &normalize_operational_state(&status),
            );
        }
        if let Some(status) = non_empty_owned(params.operational_state_not.as_deref()) {
            query = query.not_equals(
                &aliases.operational_state,
                &normalize_operational_state(&status),
            );
        }
        if let Some(date) = non_empty_owned(params.attested_date.as_deref()) {
            query = query.equals(&aliases.attested_date, &date);
        }
        if let Some(date) = non_empty_owned(params.attested_date_on_or_after.as_deref()) {
            query = query.filter(&aliases.attested_date, Operator::GreaterThanOrEqual, &date);
        }
        if let Some(date) = non_empty_owned(params.attested_date_on_or_before.as_deref()) {
            query = query.filter(&aliases.attested_date, Operator::LessThanOrEqual, &date);
        }

        let records = query.execute().await?.records;
        self.hydrate_business_application_page(records, &aliases, &options)
            .await
    }

    pub async fn query_business_applications(
        &self,
        query: BusinessApplicationQuery,
    ) -> Result<Vec<SnowRecord>> {
        self.ctx.query.query_business_applications(query).await
    }

    pub async fn business_application_servers(
        &self,
        params: BusinessApplicationServersParams,
    ) -> Result<Option<BusinessApplicationServersResult>> {
        let options = params.validate()?;
        // Public callers express "use the default relationship-type set" by
        // leaving `relationship_type` empty; honor that by resolving the default
        // labels to stable identities when empty.
        self.business_application_servers_with_options(options, true)
            .await
    }

    /// Core graph traversal for [`Self::business_application_servers`].
    ///
    /// `defaults_when_empty` controls how an empty `options.relationship_type`
    /// is interpreted:
    /// - `true` (the public path): an empty allowlist means "use the default
    ///   relationship-type set", which is resolved to stable `cmdb_rel_type`
    ///   sys_ids once at the start of the traversal (see
    ///   [`Self::resolve_relationship_type_allowlist`]).
    /// - `false`: an empty allowlist is taken literally as "match all
    ///   relationship types" (no filtering, no resolution query).
    ///
    /// An explicitly-supplied non-empty allowlist is always used verbatim and is
    /// matched by both raw value (sys_id) and display label, preserving the
    /// ability for callers to pass either form.
    async fn business_application_servers_with_options(
        &self,
        options: BusinessApplicationServersOptions,
        defaults_when_empty: bool,
    ) -> Result<Option<BusinessApplicationServersResult>> {
        let Some(application) = self
            .resolve_business_application_servers_selector(&options)
            .await?
        else {
            return Ok(None);
        };

        // Resolve the effective allowlist to stable relationship-type identities
        // (sys_ids) before traversal. Matching on sys_ids instead of mutable
        // display labels means a renamed/localized `cmdb_rel_type` label no
        // longer silently drops every edge.
        let allowed_relationship_types = self
            .resolve_relationship_type_allowlist(&options, defaults_when_empty)
            .await?;
        let service_discovery_relationship_types =
            BUSINESS_APPLICATION_SERVICE_DISCOVERY_RELATIONSHIP_TYPES
                .iter()
                .map(|value| (*value).to_string())
                .collect::<Vec<_>>();
        let run_started_at = Utc::now();
        let mut summary = BusinessApplicationServersSummary::new(&options);
        let mut diagnostics = Vec::new();
        let mut visited = HashSet::from([application.record.sys_id.clone()]);
        let mut frontier = vec![application.record.sys_id.clone()];
        let mut ci_classes = HashMap::from([(
            application.record.sys_id.clone(),
            BUSINESS_APPLICATION_TABLE.to_string(),
        )]);
        // Maps a CI sys_id to every recorded edge-chain (route) from the root BA
        // to that CI. A simple tree gives each CI one chain; in a diamond a CI is
        // reachable via multiple parents, so we keep a Vec of alternate chains
        // (only grown beyond one when `include_paths` requests full route
        // reporting). The root has a single empty chain.
        let mut paths_by_ci: HashMap<String, Vec<Vec<BusinessApplicationServerPathEdge>>> =
            HashMap::from([(application.record.sys_id.clone(), vec![Vec::new()])]);
        let mut service_paths_by_ci: HashMap<String, Vec<Vec<BusinessApplicationServerPathEdge>>> =
            HashMap::from([(application.record.sys_id.clone(), vec![Vec::new()])]);
        let mut service_membership_seed_sys_ids = HashSet::new();
        let mut servers_by_sys_id: BTreeMap<String, Server> = BTreeMap::new();
        let mut server_paths: BTreeMap<String, Vec<BusinessApplicationServerPath>> =
            BTreeMap::new();
        // Per-traversal memo of the hierarchy-backed server-class decision, keyed
        // by `sys_class_name`. The cheap sync checks never reach here; this only
        // caches the result of the `sys_db_object` super_class descent so the same
        // unrecognized class is not re-queried across BFS levels.
        let mut server_class_cache: HashMap<String, bool> = HashMap::new();

        for depth in 1..=options.max_depth {
            if frontier.is_empty() || summary.edge_limit_reached || summary.ci_limit_reached {
                break;
            }

            let relationships = self
                .business_application_relationship_level(
                    &frontier,
                    &options,
                    &mut summary,
                    &mut diagnostics,
                )
                .await?;
            let frontier_set = frontier.iter().cloned().collect::<HashSet<_>>();
            let mut newly_seen = Vec::new();

            for relationship in relationships {
                if let Some(class_name) = relationship.parent_class.as_deref() {
                    ci_classes
                        .entry(relationship.parent_sys_id.clone())
                        .or_insert_with(|| class_name.to_string());
                }
                if let Some(class_name) = relationship.child_class.as_deref() {
                    ci_classes
                        .entry(relationship.child_sys_id.clone())
                        .or_insert_with(|| class_name.to_string());
                }

                if depth == 1
                    && relationship
                        .relationship_type
                        .matches_any(&service_discovery_relationship_types)
                    && let Some((current_sys_id, adjacent_sys_id, direction)) =
                        relationship.traversal_endpoint(&frontier_set)
                {
                    let adjacent_class = if adjacent_sys_id == relationship.parent_sys_id {
                        relationship.parent_class.as_deref()
                    } else {
                        relationship.child_class.as_deref()
                    };
                    if is_application_service_class(adjacent_class) {
                        let edge = relationship.path_edge(depth, direction);
                        if extend_path_chains(
                            &mut service_paths_by_ci,
                            &current_sys_id,
                            &adjacent_sys_id,
                            edge,
                        ) {
                            service_membership_seed_sys_ids.insert(adjacent_sys_id.clone());
                        } else {
                            summary.cycle_count += 1;
                            push_business_application_server_diagnostic(
                                &mut summary,
                                &mut diagnostics,
                                "cmdb_rel_ci",
                                CMDB_REL_CI_TABLE,
                                &adjacent_sys_id,
                                ReferenceResolutionReason::CycleDetected,
                                "service discovery skipped a mixed-direction route",
                            );
                        }
                    }
                }

                if !relationship
                    .relationship_type
                    .matches_any(&allowed_relationship_types)
                {
                    continue;
                }

                let Some((current_sys_id, adjacent_sys_id, direction)) =
                    relationship.traversal_endpoint(&frontier_set)
                else {
                    push_business_application_server_diagnostic(
                        &mut summary,
                        &mut diagnostics,
                        "cmdb_rel_ci",
                        CMDB_REL_CI_TABLE,
                        "",
                        ReferenceResolutionReason::ReferenceResolutionFailed,
                        "relationship did not include a current frontier endpoint",
                    );
                    continue;
                };

                if visited.contains(&adjacent_sys_id) {
                    // The adjacent CI was already discovered. Two cases:
                    //  (a) true cycle/back-edge: the adjacent CI is an ancestor
                    //      of the current CI along some recorded route, so this
                    //      edge points back up the graph; or
                    //  (b) alternate forward path (diamond): the adjacent CI is
                    //      reachable via a different parent than before.
                    // Case (b) is a genuine additional route to the same CI and,
                    // when `include_paths` is set, must be recorded so the plural
                    // `server_paths` reflects every route. Neither case re-enqueues
                    // the CI for traversal (it is already visited).
                    let is_back_edge = path_chains_contain_ancestor(
                        paths_by_ci.get(&current_sys_id),
                        &adjacent_sys_id,
                        &current_sys_id,
                    );
                    if is_back_edge {
                        summary.cycle_count += 1;
                        push_business_application_server_diagnostic(
                            &mut summary,
                            &mut diagnostics,
                            "cmdb_ci",
                            CMDB_CI_TABLE,
                            &adjacent_sys_id,
                            ReferenceResolutionReason::CycleDetected,
                            "relationship traversal skipped a CI that was already visited",
                        );
                    } else if options.include_paths || options.persist {
                        // Record the alternate route(s) into the adjacent CI by
                        // extending each of the current CI's chains with this edge.
                        let edge = relationship.path_edge(depth, direction.clone());
                        let extended = extend_path_chains(
                            &mut paths_by_ci,
                            &current_sys_id,
                            &adjacent_sys_id,
                            edge,
                        );
                        if extended {
                            // A server reached again via an alternate route needs its
                            // new route emitted now, since it will not pass through the
                            // per-depth server-hydration loop again.
                            if options.include_paths {
                                emit_alternate_server_path(
                                    &servers_by_sys_id,
                                    &paths_by_ci,
                                    &mut server_paths,
                                    &adjacent_sys_id,
                                );
                            }
                        } else {
                            summary.cycle_count += 1;
                            push_business_application_server_diagnostic(
                                &mut summary,
                                &mut diagnostics,
                                "cmdb_rel_ci",
                                CMDB_REL_CI_TABLE,
                                &adjacent_sys_id,
                                ReferenceResolutionReason::CycleDetected,
                                "relationship traversal skipped a mixed-direction alternate route",
                            );
                        }
                    } else {
                        // include_paths off: an alternate route adds no result, so
                        // it is neither a traversal target nor recorded.
                        summary.cycle_count += 1;
                    }
                    continue;
                }
                // `max_cis` bounds the CIs examined BEYOND the root BA. `visited`
                // is seeded with the root, so the non-root examined count is
                // `visited.len() - 1`. Truncate once that budget is exhausted, so
                // a caller passing the exact expected non-root count is not
                // silently short by one.
                if visited.len().saturating_sub(1) >= options.max_cis {
                    summary.ci_limit_reached = true;
                    summary.mark_truncated(1);
                    push_business_application_server_diagnostic(
                        &mut summary,
                        &mut diagnostics,
                        "cmdb_ci",
                        CMDB_CI_TABLE,
                        &adjacent_sys_id,
                        ReferenceResolutionReason::FanoutLimitExceeded,
                        "max_cis prevented expanding another CI",
                    );
                    continue;
                }

                let edge = relationship.path_edge(depth, direction);
                // First discovery: seed the adjacent CI's routes from the current
                // CI's routes extended by this edge.
                if !extend_path_chains(&mut paths_by_ci, &current_sys_id, &adjacent_sys_id, edge) {
                    summary.cycle_count += 1;
                    push_business_application_server_diagnostic(
                        &mut summary,
                        &mut diagnostics,
                        "cmdb_rel_ci",
                        CMDB_REL_CI_TABLE,
                        &adjacent_sys_id,
                        ReferenceResolutionReason::CycleDetected,
                        "relationship traversal skipped a mixed-direction route",
                    );
                    continue;
                }
                visited.insert(adjacent_sys_id.clone());
                newly_seen.push(adjacent_sys_id);
            }

            self.business_application_hydrate_ci_classes(
                &newly_seen,
                &mut ci_classes,
                &mut summary,
                &mut diagnostics,
            )
            .await?;

            let mut server_ids = Vec::new();
            let mut next_frontier = Vec::new();
            for sys_id in newly_seen {
                let Some(class_name) = ci_classes.get(&sys_id).cloned() else {
                    continue;
                };
                // Hierarchy-aware classification: any descendant of
                // `cmdb_ci_server` (not just the narrow alias list) is collected
                // as a server. Cheap exact/alias/naming checks run first (no
                // network); only genuinely unrecognized classes fall back to the
                // metadata-backed super_class descent. Non-server CIs continue
                // into the next BFS frontier.
                let is_server = self
                    .business_application_class_is_server(
                        &class_name,
                        &mut server_class_cache,
                        &mut summary,
                        &mut diagnostics,
                    )
                    .await?;
                if is_server {
                    server_ids.push(sys_id);
                } else {
                    next_frontier.push(sys_id);
                }
            }

            let servers = self
                .business_application_hydrate_servers(&server_ids, &mut summary, &mut diagnostics)
                .await?;
            for server in servers {
                let sys_id = server.record.sys_id.clone();
                // Record this server first so later-depth alternate-route
                // discoveries can emit additional paths for it.
                servers_by_sys_id.entry(sys_id.clone()).or_insert(server);
                if options.include_paths
                    && let Some(chains) = paths_by_ci.get(&sys_id)
                {
                    // Emit every recorded route to this server. In a diamond the
                    // server already carries multiple chains by the time it is
                    // hydrated.
                    let entry = server_paths.entry(sys_id.clone()).or_default();
                    for chain in chains {
                        entry.push(BusinessApplicationServerPath {
                            edges: chain.clone(),
                        });
                    }
                }
            }

            if depth == options.max_depth && !next_frontier.is_empty() {
                summary.depth_limit_reached = true;
                summary.mark_truncated(next_frontier.len());
                push_business_application_server_diagnostic(
                    &mut summary,
                    &mut diagnostics,
                    "cmdb_rel_ci",
                    CMDB_REL_CI_TABLE,
                    "",
                    ReferenceResolutionReason::FanoutLimitExceeded,
                    "max_depth prevented expanding the next BFS frontier",
                );
                break;
            }

            frontier = next_frontier;
        }

        summary.cis_examined = visited.len();
        let mut discoveries: BTreeMap<String, BusinessApplicationServerDiscovery> = BTreeMap::new();
        for server in servers_by_sys_id.into_values() {
            let paths =
                business_application_server_paths_for(&paths_by_ci, server.record.sys_id.as_str());
            merge_business_application_server_discovery(
                &mut discoveries,
                server,
                BusinessApplicationServerProvenance::Relationship,
                paths,
            );
        }

        let service_membership_servers = self
            .business_application_service_membership_servers(
                &service_membership_seed_sys_ids,
                &service_paths_by_ci,
                &options,
                &mut summary,
                &mut diagnostics,
                &mut server_class_cache,
            )
            .await?;
        for (server, paths) in service_membership_servers {
            merge_business_application_server_discovery(
                &mut discoveries,
                server,
                BusinessApplicationServerProvenance::ServiceMembership,
                paths,
            );
        }

        summary.servers_found = discoveries.len();
        let mut servers = Vec::with_capacity(discoveries.len());
        let mut server_provenance = BTreeMap::new();
        let mut persisted_server_paths = BTreeMap::new();
        let mut server_paths = BTreeMap::new();
        for (sys_id, discovery) in discoveries {
            server_provenance.insert(sys_id.clone(), discovery.provenance);
            let paths = discovery.paths();
            if !paths.is_empty() {
                persisted_server_paths.insert(sys_id.clone(), paths.clone());
            }
            if options.include_paths && !paths.is_empty() {
                server_paths.insert(sys_id.clone(), paths);
            }
            servers.push(discovery.server);
        }
        if options.persist {
            self.persist_business_application_server_traversal(
                &application.record,
                &servers,
                &server_provenance,
                &persisted_server_paths,
                run_started_at,
                &options,
                &mut summary,
            )?;
        }

        // CMDB-gap fallback. Runs strictly AFTER persistence so that
        // fallback-discovered servers are never written to the durable BA↔server
        // membership or inventory-health tables: the persisting call above sees
        // only the traversal `servers`/`server_provenance`, and a 0-server
        // traversal persists 0 membership rows regardless of what the fallback
        // returns. Fallback results are appended to the live response below and
        // tagged via `server_sources`; they are live-only by construction.
        let mut server_sources: BTreeMap<String, ServerResultSource> = BTreeMap::new();
        if options.fallback_strategy.is_enabled() {
            // Pre-fallback traversal count. Always present (even when the
            // fallback does not fire) once a strategy is requested.
            summary.cmdb_servers_found = Some(summary.servers_found);
            summary.fallback_strategy = Some(options.fallback_strategy.as_str().to_string());
        }
        if options.fallback_strategy == FallbackStrategy::CiOwnerGroup && summary.servers_found == 0
        {
            let fallback_servers = self
                .business_application_ci_owner_group_fallback(
                    &application,
                    &options,
                    &mut summary,
                    &mut diagnostics,
                    &mut server_class_cache,
                )
                .await?;
            for server in fallback_servers {
                let sys_id = server.record.sys_id.clone();
                server_sources.insert(sys_id, ServerResultSource::CiOwnerGroupFallback);
                servers.push(server);
            }
            // `servers_found` reflects the TOTAL servers returned (traversal +
            // fallback). In the fallback case the traversal count is 0, so this
            // equals the fallback count while `cmdb_servers_found` stays 0.
            summary.servers_found = servers.len();
        }

        let inventory_health = if options.persist {
            self.ctx
                .query
                .store()
                .get_business_application_server_inventory_health(&application.record.sys_id)?
                .map(|row| BusinessApplicationServerInventoryHealth {
                    ba_sys_id: row.ba_sys_id,
                    run_started_at: row.run_started_at,
                    run_completed_at: row.run_completed_at,
                    service_membership_status: row.service_membership_status,
                    relationship_status: row.relationship_status,
                    inventory_status: row.inventory_status,
                    summary: serde_json::from_str(&row.summary_json)
                        .unwrap_or_else(|_| Value::Object(serde_json::Map::new())),
                })
        } else {
            None
        };

        Ok(Some(BusinessApplicationServersResult {
            business_application: BusinessApplicationServerApplication::from(&application),
            servers,
            server_sources,
            server_provenance,
            inventory_health,
            relationship_summary: summary,
            diagnostics,
            server_paths,
        }))
    }

    /// `ci_owner_group` CMDB-gap fallback: when the `cmdb_rel_ci` traversal found
    /// 0 servers, query `cmdb_ci_server` by the BA's raw `u_ci_owner_group` field
    /// and return the matched servers, live-only (never persisted).
    ///
    /// Field mapping (the load-bearing correctness point): the group sys_id is
    /// sourced from the BA's raw `u_ci_owner_group` column via
    /// [`BusinessApplication::ci_owner_group_raw`] and filtered with an EXACT
    /// `u_ci_owner_group = <sys_id>` predicate on the server side. Neither side
    /// uses the `ci_owner_group`/`managed_by_group` typed alias, which is empty on
    /// live data.
    ///
    /// Bounds and degradation reuse the traversal contract:
    /// - the result set is bounded by `options.max_cis` and truncation is marked
    ///   via [`BusinessApplicationServersSummary::mark_truncated`];
    /// - HTTP 401/403 increments `acl_restricted_count` with a
    ///   `ReferenceAclRestricted` diagnostic and returns no servers
    ///   (`fallback_used = false`);
    /// - a BA with no `u_ci_owner_group`, or a tombstoned/unreadable group, emits a
    ///   structured diagnostic and returns `servers: []`, `fallback_used = false`
    ///   — never a panic or hard error;
    /// - returned rows are classified with the hierarchy-aware
    ///   [`is_server_class`], not `is_server_table`.
    async fn business_application_ci_owner_group_fallback(
        &self,
        application: &BusinessApplication,
        options: &BusinessApplicationServersOptions,
        summary: &mut BusinessApplicationServersSummary,
        diagnostics: &mut Vec<ReferenceResolutionDiagnostic>,
        server_class_cache: &mut HashMap<String, bool>,
    ) -> Result<Vec<Server>> {
        // 1. Source the group from the RAW `u_ci_owner_group` field. Absent/empty
        //    is a clean diagnostic, not an error: fallback simply does not fire.
        let Some(group) = application.ci_owner_group_raw() else {
            push_business_application_server_diagnostic(
                summary,
                diagnostics,
                BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD,
                BUSINESS_APPLICATION_TABLE,
                &application.record.sys_id,
                ReferenceResolutionReason::ReferenceNotFound,
                "ci_owner_group fallback requested but BA has no u_ci_owner_group set",
            );
            return Ok(Vec::new());
        };

        // 2. EXACT filter on the raw `u_ci_owner_group` column. The fallback page
        //    size is the BA traversal's `max_cis` budget (default 500, ceiling
        //    5000) — NOT ServerSearchParams' SERVER_MAX_LIMIT (100). One extra row
        //    beyond the budget is requested so over-budget result sets can be
        //    detected and truncation marked deterministically.
        let limit = options.max_cis;
        let fetch_limit = limit.saturating_add(1).min(u32::MAX as usize) as u32;
        let response = self
            .ctx
            .client
            .table(SERVER_TABLE)
            .equals(BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD, &group.sys_id)
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .no_count()
            .order_by("name", Order::Asc)
            .limit(fetch_limit)
            .execute()
            .await;

        let mut records = match response {
            Ok(response) => response.records,
            Err(SnowApiError::Api { status, .. }) if status == 401 || status == 403 => {
                summary.acl_restricted_count += 1;
                push_business_application_server_diagnostic(
                    summary,
                    diagnostics,
                    BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD,
                    SERVER_TABLE,
                    &group.sys_id,
                    ReferenceResolutionReason::ReferenceAclRestricted,
                    "ACL restricted ci_owner_group fallback server query",
                );
                return Ok(Vec::new());
            }
            Err(err) if is_servicenow_acl_error(&err) => {
                summary.acl_restricted_count += 1;
                push_business_application_server_diagnostic(
                    summary,
                    diagnostics,
                    BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD,
                    SERVER_TABLE,
                    &group.sys_id,
                    ReferenceResolutionReason::ReferenceAclRestricted,
                    "ACL restricted ci_owner_group fallback server query",
                );
                return Ok(Vec::new());
            }
            Err(SnowApiError::Api { status: 404, .. }) => {
                // Tombstoned/unreadable group: structured diagnostic, no servers,
                // no panic.
                push_business_application_server_diagnostic(
                    summary,
                    diagnostics,
                    BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD,
                    SERVER_TABLE,
                    &group.sys_id,
                    ReferenceResolutionReason::ReferenceNotFound,
                    "ci_owner_group fallback group is tombstoned or unreadable",
                );
                return Ok(Vec::new());
            }
            Err(err) => return Err(err.into()),
        };

        // The fallback fired: record the group identity. `fallback_used` stays true
        // even when the group owns zero servers (the data-quality gap is real).
        summary.fallback_used = true;
        summary.fallback_group_sys_id = Some(group.sys_id.clone());
        summary.fallback_group_display_name = group.display_name.clone();
        summary.record_cmdb_relationships_unmapped();

        // Bound to `max_cis`; mark truncation for any rows beyond the budget.
        if records.len() > limit {
            let skipped = records.len() - limit;
            records.truncate(limit);
            summary.mark_truncated(skipped);
        }

        let mut servers = Vec::with_capacity(records.len());
        for record in records {
            // Hierarchy-aware class gate. The query targets the base
            // `cmdb_ci_server` table, so non-servers should not appear; this is a
            // defensive consistency check using `is_server_class` (not the narrow
            // `is_server_table`) so custom `cmdb_ci_*_server` subclasses are kept.
            let class_name = servicenow_record_text(&record, "sys_class_name");
            let is_server = match class_name.as_deref() {
                Some(class_name) => {
                    self.business_application_class_is_server(
                        class_name,
                        server_class_cache,
                        summary,
                        diagnostics,
                    )
                    .await?
                }
                // No class on a base-table row is unexpected but harmless: the row
                // came from cmdb_ci_server, so treat it as a server.
                None => true,
            };
            if !is_server {
                continue;
            }
            servers.push(Server::from_servicenow(&record)?);
        }
        Ok(servers)
    }

    #[allow(clippy::too_many_arguments)]
    fn persist_business_application_server_traversal(
        &self,
        application: &SnowRecord,
        servers: &[Server],
        server_provenance: &BTreeMap<String, BusinessApplicationServerProvenance>,
        server_paths: &BTreeMap<String, Vec<BusinessApplicationServerPath>>,
        run_started_at: DateTime<Utc>,
        options: &BusinessApplicationServersOptions,
        summary: &mut BusinessApplicationServersSummary,
    ) -> Result<()> {
        self.persist_snow_records(std::slice::from_ref(application))?;
        let server_records = servers
            .iter()
            .map(|server| server.record.clone())
            .collect::<Vec<_>>();
        summary.persisted_servers = self.persist_snow_records(&server_records)?;

        let store = self.ctx.query.store();
        let now = Utc::now();
        for server in servers {
            let paths = server_paths
                .get(&server.record.sys_id)
                .cloned()
                .unwrap_or_default();
            let min_depth = paths
                .iter()
                .map(BusinessApplicationServerPath::depth)
                .min()
                .unwrap_or(0);
            let provenance = server_provenance
                .get(&server.record.sys_id)
                .copied()
                .unwrap_or(BusinessApplicationServerProvenance::Relationship);
            let server_table = server
                .class_name
                .as_deref()
                .and_then(|value| non_empty_owned(Some(value)))
                .or_else(|| non_empty_owned(Some(server.record.table.as_str())))
                .unwrap_or_else(|| SERVER_TABLE.to_string());
            let membership = BusinessApplicationServerMembershipRow {
                ba_sys_id: application.sys_id.clone(),
                server_sys_id: server.record.sys_id.clone(),
                server_table,
                provenance: provenance.as_str().to_string(),
                min_depth,
                paths_json: serde_json::to_string(&paths)?,
                discovered_at: now,
                last_seen_at: now,
                tombstoned_at: None,
            };
            store.upsert_business_application_server_membership(&membership)?;
            summary.membership_upserts += 1;
        }

        if options.prune_stale {
            summary.membership_pruned = store
                .tombstone_stale_business_application_server_memberships(
                    application.sys_id.as_str(),
                    run_started_at,
                    Utc::now(),
                )?;
        }
        let run_completed_at = Utc::now();
        let service_membership_status =
            Self::business_application_service_membership_health_status(summary);
        let relationship_status = Self::business_application_relationship_health_status(summary);
        let inventory_status = Self::business_application_inventory_health_status(
            relationship_status,
            &service_membership_status,
            summary,
        );
        summary.service_membership_status = Some(service_membership_status.to_string());
        summary.relationship_status = Some(relationship_status.to_string());
        summary.inventory_status = Some(inventory_status.to_string());
        store.upsert_business_application_server_inventory_health(
            &BusinessApplicationServerInventoryHealthRow {
                ba_sys_id: application.sys_id.clone(),
                run_started_at,
                run_completed_at,
                service_membership_status: service_membership_status.to_string(),
                relationship_status: relationship_status.to_string(),
                inventory_status: inventory_status.to_string(),
                summary_json: serde_json::to_string(summary)?,
            },
        )?;
        Ok(())
    }

    pub async fn get_business_application_servers(
        &self,
        params: BusinessApplicationServersParams,
    ) -> Result<Option<BusinessApplicationServersResult>> {
        self.business_application_servers(params).await
    }

    fn business_application_relationship_health_status(
        summary: &BusinessApplicationServersSummary,
    ) -> &'static str {
        if summary.depth_limit_reached {
            "depth_limited"
        } else if summary.edge_limit_reached {
            "edge_budget_exhausted"
        } else if summary.ci_limit_reached {
            "ci_budget_exhausted"
        } else if summary.relationship_acl_restricted_count > 0 {
            "acl_restricted"
        } else if summary.truncated {
            "truncated"
        } else {
            "ok"
        }
    }

    fn business_application_service_membership_health_status(
        summary: &BusinessApplicationServersSummary,
    ) -> String {
        if let Some(status) = summary.service_membership_status.as_deref() {
            return match status {
                "ok" => "ok",
                "acl_restricted" => "acl_restricted",
                "association_budget_exhausted" => "association_budget_exhausted",
                "page_budget_exhausted" => "page_budget_exhausted",
                "not_attempted" => "not_attempted",
                _ => "not_attempted",
            }
            .to_string();
        }
        if summary.service_membership_association_limit_reached {
            "association_budget_exhausted".to_string()
        } else if summary.service_membership_page_limit_reached {
            "page_budget_exhausted".to_string()
        } else {
            "not_attempted".to_string()
        }
    }

    fn business_application_inventory_health_status(
        relationship_status: &str,
        service_membership_status: &str,
        summary: &BusinessApplicationServersSummary,
    ) -> &'static str {
        if summary.truncated {
            "truncated"
        } else if relationship_status != "ok" {
            "relationship_degraded"
        } else if !matches!(service_membership_status, "ok" | "not_attempted") {
            "service_membership_degraded"
        } else {
            "complete"
        }
    }

    /// Resolve the relationship-type allowlist used to filter `cmdb_rel_ci`
    /// edges during traversal.
    ///
    /// Relationship-type matching keys off the *stable* `cmdb_rel_type` identity
    /// (sys_id), not the mutable/localizable display label. The behavior:
    ///
    /// - Explicit non-empty allowlist: returned verbatim. Callers may pass
    ///   sys_ids or labels; [`BusinessApplicationRelationshipType::matches_any`]
    ///   compares against both the edge raw value and display label.
    /// - Empty allowlist with `defaults_when_empty == false`: returns empty,
    ///   which `matches_any` treats as "match all".
    /// - Empty allowlist with `defaults_when_empty == true`: resolves the default
    ///   label set ([`BUSINESS_APPLICATION_SERVERS_DEFAULT_RELATIONSHIP_TYPES`])
    ///   to `cmdb_rel_type` sys_ids via a single `name IN (...)` query, so each
    ///   edge is matched by sys_id identity. If resolution fails or returns
    ///   nothing (e.g. ACL-restricted `cmdb_rel_type`), it falls back to the
    ///   default label strings so behavior is no worse than the prior label-only
    ///   matching.
    async fn resolve_relationship_type_allowlist(
        &self,
        options: &BusinessApplicationServersOptions,
        defaults_when_empty: bool,
    ) -> Result<Vec<String>> {
        if !options.relationship_type.is_empty() {
            // Explicit caller-supplied allowlist: use as-is.
            return Ok(options.relationship_type.clone());
        }
        if !defaults_when_empty {
            // Explicit empty allowlist => match all.
            return Ok(Vec::new());
        }

        let default_labels = BUSINESS_APPLICATION_SERVERS_DEFAULT_RELATIONSHIP_TYPES
            .iter()
            .map(|label| (*label).to_string())
            .collect::<Vec<_>>();
        let label_refs = default_labels
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();

        // One lookup per traversal: resolve the stored `name` of each default
        // relationship type to its sys_id. `cmdb_rel_type.name` is the configured
        // stored name (stable), whereas the edge display value can be localized.
        let response = self
            .ctx
            .client
            .table(CMDB_REL_TYPE_TABLE)
            .in_list("name", &label_refs)
            .fields(&["sys_id", "name"])
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .no_count()
            .limit(label_refs.len() as u32)
            .execute()
            .await;

        match response {
            Ok(response) => {
                let mut allowed = Vec::new();
                for record in response.records {
                    // `cmdb_rel_type.sys_id` is the stable relationship-type
                    // identity. Read the canonical record sys_id directly.
                    if let Ok(sys_id) = normalize_record_lookup_sys_id(&record.sys_id)
                        && !allowed.contains(&sys_id)
                    {
                        allowed.push(sys_id);
                    }
                }
                if allowed.is_empty() {
                    // Resolution returned nothing useful; fall back to label
                    // matching so the traversal is not silently emptied.
                    Ok(default_labels)
                } else {
                    Ok(allowed)
                }
            }
            // Degrade gracefully: if cmdb_rel_type cannot be read, fall back to
            // matching by the default label strings (prior behavior).
            Err(_) => Ok(default_labels),
        }
    }

    async fn resolve_business_application_servers_selector(
        &self,
        options: &BusinessApplicationServersOptions,
    ) -> Result<Option<BusinessApplication>> {
        let aliases = self.resolve_business_application_aliases(false).await;
        let record = match &options.selector {
            BusinessApplicationServersSelector::SysId(sys_id) => match self
                .ctx
                .client
                .table(BUSINESS_APPLICATION_TABLE)
                .display_value(DisplayValue::Both)
                .exclude_reference_link(true)
                .get(sys_id)
                .await
            {
                Ok(record) => Some(record),
                Err(SnowApiError::Api { status: 404, .. }) => None,
                Err(err) => return Err(err.into()),
            },
            BusinessApplicationServersSelector::Number(number) => {
                let records = self
                    .ctx
                    .client
                    .table(BUSINESS_APPLICATION_TABLE)
                    .equals("sys_class_name", BUSINESS_APPLICATION_TABLE)
                    .equals("number", number)
                    .display_value(DisplayValue::Both)
                    .exclude_reference_link(true)
                    .limit(2)
                    .execute()
                    .await?
                    .records;
                if records.len() > 1 {
                    anyhow::bail!("multiple Business Applications matched number={number}");
                }
                records.into_iter().next()
            }
        };

        let Some(record) = record else {
            return Ok(None);
        };
        Ok(Some(BusinessApplication::from_servicenow(
            &record, &aliases,
        )?))
    }

    async fn business_application_relationship_level(
        &self,
        frontier: &[String],
        options: &BusinessApplicationServersOptions,
        summary: &mut BusinessApplicationServersSummary,
        diagnostics: &mut Vec<ReferenceResolutionDiagnostic>,
    ) -> Result<Vec<BusinessApplicationRelationshipEdge>> {
        // The remaining `max_edges` budget for THIS level is fixed up front from
        // the current running total. Both directions (parent + child) are
        // independent remote reads, so we issue them concurrently. Each reader is
        // bounded by the full `remaining` budget (so neither direction alone can
        // exceed the level budget), then we MERGE deterministically below in a
        // fixed parent-then-child order. The merge — not the concurrent reads —
        // owns the shared `summary`/`max_edges` accounting, so the result is
        // identical to the previous sequential version and reproducible.
        if frontier.is_empty() || summary.edge_limit_reached {
            return Ok(Vec::new());
        }
        let remaining = options
            .max_edges
            .saturating_sub(summary.relationships_examined);
        if remaining == 0 {
            mark_business_application_edge_limit(summary, diagnostics);
            return Ok(Vec::new());
        }

        let (parent_read, child_read) = tokio::try_join!(
            self.business_application_relationship_direction_read("parent", frontier, remaining),
            self.business_application_relationship_direction_read("child", frontier, remaining),
        )?;

        // Merge in a stable order: parent first, then child. Parent consumes the
        // shared budget first (exactly as the old sequential code did), and the
        // child's contribution is then capped to whatever budget the parent left.
        let mut records = Vec::new();
        let mut budget = remaining;
        for read in [parent_read, child_read] {
            budget = self.merge_business_application_direction_read(
                read,
                budget,
                summary,
                diagnostics,
                &mut records,
            );
        }

        let mut edges = Vec::new();
        let mut seen = HashSet::new();
        for record in records {
            let Some(edge) =
                business_application_relationship_edge_from_record(&record, summary, diagnostics)
            else {
                continue;
            };
            if seen.insert(edge.key()) {
                edges.push(edge);
            }
        }
        Ok(edges)
    }

    /// Folds one direction's read result into the shared traversal accounting and
    /// returns the budget remaining for the next direction.
    ///
    /// This is the single place that mutates `summary`/`diagnostics` for the edge
    /// read, so the two concurrent direction reads stay side-effect-free and the
    /// budget is consumed in a deterministic order. The direction's collected rows
    /// are truncated to `budget`; if that truncation drops rows, or the read
    /// itself was truncated by its own bound while pages remained, the shared
    /// `edge_limit_reached` flag is set. ACL failures are surfaced exactly once
    /// per direction.
    fn merge_business_application_direction_read(
        &self,
        read: BusinessApplicationDirectionRead,
        budget: usize,
        summary: &mut BusinessApplicationServersSummary,
        diagnostics: &mut Vec<ReferenceResolutionDiagnostic>,
        records: &mut Vec<Record>,
    ) -> usize {
        if read.acl_restricted {
            summary.acl_restricted_count += 1;
            summary.relationship_acl_restricted_count += 1;
            push_business_application_server_diagnostic(
                summary,
                diagnostics,
                read.field,
                CMDB_REL_CI_TABLE,
                "",
                ReferenceResolutionReason::ReferenceAclRestricted,
                "ACL restricted cmdb_rel_ci traversal",
            );
        }

        let mut rows = read.records;
        // Cap this direction's rows at the budget left after the prior direction.
        let over_budget = rows.len() > budget;
        if over_budget {
            rows.truncate(budget);
            mark_business_application_edge_limit(summary, diagnostics);
        } else if read.edge_limit_reached {
            // The direction read consumed its own bound while pages remained; this
            // is a genuine truncation that survives the merge.
            mark_business_application_edge_limit(summary, diagnostics);
        }

        let consumed = rows.len();
        summary.relationships_examined += consumed;
        records.extend(rows);
        budget.saturating_sub(consumed)
    }

    /// Reads one relationship direction (`parent` or `child`) of the `cmdb_rel_ci`
    /// edges for the current frontier, bounded by `remaining` edges.
    ///
    /// This is a PURE read: it issues remote requests and returns the collected
    /// rows plus truncation/ACL flags, but mutates no shared traversal state. That
    /// lets the two directions run concurrently; all `summary`/`diagnostics`
    /// accounting is applied afterwards by
    /// [`Self::merge_business_application_direction_read`] in a deterministic
    /// order. The read stops once `remaining` rows are collected (flagging
    /// `edge_limit_reached` if pages still remain) or the result set is exhausted.
    async fn business_application_relationship_direction_read(
        &self,
        field: &'static str,
        frontier: &[String],
        remaining: usize,
    ) -> Result<BusinessApplicationDirectionRead> {
        let mut read = BusinessApplicationDirectionRead::new(field);
        if frontier.is_empty() || remaining == 0 {
            return Ok(read);
        }

        let frontier_refs = frontier.iter().map(String::as_str).collect::<Vec<_>>();
        // Paginate the edge read instead of issuing one large `limit(remaining+1)`
        // request. `servicenow_rs::execute()` does NOT auto-paginate, so a single
        // request can be silently capped server-side by
        // `glide.rest.table.max_record_count`, returning FEWER rows than exist
        // with no signal — edges would then vanish and `edge_limit_reached` would
        // stay false. The paginator walks pages until the result set is exhausted
        // or the `remaining` budget is consumed.
        let page_size = BUSINESS_APPLICATION_RELATIONSHIP_PAGE_SIZE.min(remaining.max(1));
        let mut paginator = self
            .ctx
            .client
            .table(CMDB_REL_CI_TABLE)
            .in_list(field, &frontier_refs)
            .fields(BUSINESS_APPLICATION_RELATIONSHIP_FIELDS)
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .no_count()
            .limit(page_size as u32)
            .paginate()?;

        let mut collected = 0usize;
        loop {
            if collected >= remaining {
                // Budget exhausted but the paginator may still have pages: this is
                // a genuine truncation, surface it via the flag for the merge step.
                if !paginator.is_done() {
                    read.edge_limit_reached = true;
                }
                break;
            }
            let page = match paginator.next_page().await {
                Ok(Some(page)) => page,
                Ok(None) => break,
                Err(err) if is_servicenow_acl_error(&err) => {
                    read.acl_restricted = true;
                    return Ok(read);
                }
                Err(err) => return Err(err.into()),
            };

            let mut rows = page.records;
            // Cap the running total at the remaining edge budget.
            let room = remaining - collected;
            let over_budget = rows.len() > room;
            if over_budget {
                rows.truncate(room);
                read.edge_limit_reached = true;
            }
            collected += rows.len();
            read.records.extend(rows);
            if over_budget {
                break;
            }
        }
        Ok(read)
    }

    async fn business_application_service_membership_servers(
        &self,
        service_sys_ids: &HashSet<String>,
        service_paths_by_ci: &HashMap<String, Vec<Vec<BusinessApplicationServerPathEdge>>>,
        options: &BusinessApplicationServersOptions,
        summary: &mut BusinessApplicationServersSummary,
        diagnostics: &mut Vec<ReferenceResolutionDiagnostic>,
        server_class_cache: &mut HashMap<String, bool>,
    ) -> Result<Vec<(Server, Vec<BusinessApplicationServerPath>)>> {
        if service_sys_ids.is_empty() {
            summary.service_membership_status = Some("not_attempted".to_string());
            return Ok(Vec::new());
        }

        let read = self
            .business_application_service_membership_read(service_sys_ids, options)
            .await?;
        summary.service_membership_pages_examined += read.pages_examined;
        if read.acl_restricted {
            summary.acl_restricted_count += 1;
            summary.service_membership_status = Some("acl_restricted".to_string());
            push_business_application_server_diagnostic(
                summary,
                diagnostics,
                "svc_ci_assoc",
                SVC_CI_ASSOC_TABLE,
                "",
                ReferenceResolutionReason::ReferenceAclRestricted,
                "ACL restricted svc_ci_assoc service membership read",
            );
            return Ok(Vec::new());
        }
        if read.association_limit_reached {
            summary.service_membership_association_limit_reached = true;
            summary.mark_truncated(1);
            push_business_application_server_diagnostic(
                summary,
                diagnostics,
                "svc_ci_assoc",
                SVC_CI_ASSOC_TABLE,
                "",
                ReferenceResolutionReason::FanoutLimitExceeded,
                "max_service_membership_associations prevented reading more associations",
            );
        }
        if read.page_limit_reached {
            summary.service_membership_page_limit_reached = true;
            summary.mark_truncated(1);
            push_business_application_server_diagnostic(
                summary,
                diagnostics,
                "svc_ci_assoc",
                SVC_CI_ASSOC_TABLE,
                "",
                ReferenceResolutionReason::FanoutLimitExceeded,
                "max_service_membership_pages prevented reading more association pages",
            );
        }
        summary.service_membership_associations_examined += read.records.len();

        let mut server_ids = Vec::new();
        let mut server_paths_by_sys_id: BTreeMap<String, Vec<BusinessApplicationServerPath>> =
            BTreeMap::new();
        let mut examined_member_cis = HashSet::new();
        for record in read.records {
            let Some(service_sys_id) = servicenow_reference_sys_id(&record, "service_id") else {
                push_business_application_server_diagnostic(
                    summary,
                    diagnostics,
                    "service_id",
                    SVC_CI_ASSOC_TABLE,
                    "",
                    ReferenceResolutionReason::ReferenceResolutionFailed,
                    "svc_ci_assoc row was missing a readable service_id reference",
                );
                continue;
            };
            let Some(ci_sys_id) = servicenow_reference_sys_id(&record, "ci_id") else {
                push_business_application_server_diagnostic(
                    summary,
                    diagnostics,
                    "ci_id",
                    SVC_CI_ASSOC_TABLE,
                    "",
                    ReferenceResolutionReason::ReferenceResolutionFailed,
                    "svc_ci_assoc row was missing a readable ci_id reference",
                );
                continue;
            };
            if examined_member_cis.insert(ci_sys_id.clone()) {
                if summary.cis_examined.saturating_sub(1) >= options.max_cis {
                    summary.ci_limit_reached = true;
                    summary.mark_truncated(1);
                    push_business_application_server_diagnostic(
                        summary,
                        diagnostics,
                        "ci_id",
                        SVC_CI_ASSOC_TABLE,
                        &ci_sys_id,
                        ReferenceResolutionReason::FanoutLimitExceeded,
                        "max_cis prevented examining another service member CI",
                    );
                    continue;
                }
                summary.cis_examined += 1;
            }

            let Some(class_name) = servicenow_record_text(&record, "ci_id.sys_class_name") else {
                summary.missing_ci_count += 1;
                push_business_application_server_diagnostic(
                    summary,
                    diagnostics,
                    "ci_id.sys_class_name",
                    SVC_CI_ASSOC_TABLE,
                    &ci_sys_id,
                    ReferenceResolutionReason::ReferenceResolutionFailed,
                    "svc_ci_assoc member CI class could not be read",
                );
                continue;
            };
            let is_server = self
                .business_application_class_is_server(
                    &class_name,
                    server_class_cache,
                    summary,
                    diagnostics,
                )
                .await?;
            if !is_server {
                continue;
            }
            if !server_ids.contains(&ci_sys_id) {
                server_ids.push(ci_sys_id.clone());
            }
            server_paths_by_sys_id
                .entry(ci_sys_id.clone())
                .or_default()
                .extend(business_application_service_membership_paths_for(
                    service_paths_by_ci,
                    &service_sys_id,
                    &ci_sys_id,
                ));
        }

        let servers = self
            .business_application_hydrate_servers(&server_ids, summary, diagnostics)
            .await?;
        if summary.service_membership_status.is_none() {
            if summary.service_membership_association_limit_reached {
                summary.service_membership_status =
                    Some("association_budget_exhausted".to_string());
            } else if summary.service_membership_page_limit_reached {
                summary.service_membership_status = Some("page_budget_exhausted".to_string());
            } else {
                summary.service_membership_status = Some("ok".to_string());
            }
        }

        Ok(servers
            .into_iter()
            .map(|server| {
                let paths = server_paths_by_sys_id
                    .remove(&server.record.sys_id)
                    .unwrap_or_default();
                (server, paths)
            })
            .collect())
    }

    async fn business_application_service_membership_read(
        &self,
        service_sys_ids: &HashSet<String>,
        options: &BusinessApplicationServersOptions,
    ) -> Result<BusinessApplicationServiceMembershipRead> {
        let mut read = BusinessApplicationServiceMembershipRead::new();
        if service_sys_ids.is_empty() {
            return Ok(read);
        }

        let mut service_refs = service_sys_ids
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        service_refs.sort_unstable();
        let page_size = BUSINESS_APPLICATION_SERVICE_MEMBERSHIP_PAGE_SIZE
            .min(options.max_service_membership_associations.max(1));
        let mut paginator = self
            .ctx
            .client
            .table(SVC_CI_ASSOC_TABLE)
            .in_list("service_id", &service_refs)
            .fields(BUSINESS_APPLICATION_SERVICE_MEMBERSHIP_FIELDS)
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .no_count()
            .limit(page_size as u32)
            .paginate()?;

        let mut collected = 0usize;
        loop {
            if collected >= options.max_service_membership_associations {
                if !paginator.is_done() {
                    read.association_limit_reached = true;
                }
                break;
            }
            if read.pages_examined >= options.max_service_membership_pages {
                if !paginator.is_done() {
                    read.page_limit_reached = true;
                }
                break;
            }
            let page = match paginator.next_page().await {
                Ok(Some(page)) => page,
                Ok(None) => break,
                Err(err) if is_servicenow_acl_error(&err) => {
                    read.acl_restricted = true;
                    return Ok(read);
                }
                Err(err) => return Err(err.into()),
            };
            read.pages_examined += 1;
            let mut rows = page.records;
            let room = options
                .max_service_membership_associations
                .saturating_sub(collected);
            let over_budget = rows.len() > room;
            if over_budget {
                rows.truncate(room);
                read.association_limit_reached = true;
            }
            collected += rows.len();
            read.records.extend(rows);
            if over_budget {
                break;
            }
        }
        Ok(read)
    }

    /// Decide whether a CMDB `sys_class_name` is a server, hierarchy-aware.
    ///
    /// Detection is layered cheapest-first:
    /// 1. The sync [`is_server_class`] heuristic (base table, known OOTB
    ///    subclasses, and the `cmdb_ci_*_server` naming convention) — no network.
    /// 2. For classes that pass none of those (instance-custom server subclasses
    ///    whose table name does NOT end in `_server`), a metadata-backed descent:
    ///    walk the `sys_db_object` `super_class` chain via [`Self::table_ancestors`]
    ///    and treat the class as a server iff `cmdb_ci_server` is an ancestor.
    ///
    /// The hierarchy result is memoized per `sys_class_name` in `server_class_cache`
    /// so the same unrecognized class is queried at most once per traversal.
    ///
    /// Degradation: if the metadata walk fails (e.g. ACL/network), the failure is
    /// surfaced as a `DictionaryUnavailable` diagnostic (never silent) and the
    /// class is treated as a NON-server. That is the non-destructive choice — the
    /// CI continues into the BFS frontier so its subtree is still explored, rather
    /// than being pruned as a leaf server. A class only reaches this fallback after
    /// failing every server-naming heuristic, so it is unlikely to be a server; and
    /// the negative cache entry stops repeated failing queries within the traversal.
    async fn business_application_class_is_server(
        &self,
        class_name: &str,
        server_class_cache: &mut HashMap<String, bool>,
        summary: &mut BusinessApplicationServersSummary,
        diagnostics: &mut Vec<ReferenceResolutionDiagnostic>,
    ) -> Result<bool> {
        // Cheap, network-free checks first.
        if is_server_class(class_name) {
            return Ok(true);
        }

        if let Some(cached) = server_class_cache.get(class_name) {
            return Ok(*cached);
        }

        // Metadata-backed super_class descent for unrecognized classes.
        let is_server = match self.table_ancestors(class_name).await {
            Ok(ancestors) => ancestors.iter().any(|ancestor| ancestor == SERVER_TABLE),
            Err(_) => {
                // Degrade without failing the traversal: surface the metadata gap
                // and fall back to NON-server (continue traversing through the CI).
                push_business_application_server_diagnostic(
                    summary,
                    diagnostics,
                    "sys_class_name",
                    "sys_db_object",
                    "",
                    ReferenceResolutionReason::DictionaryUnavailable,
                    "sys_db_object super_class lookup failed; class not classified as server",
                );
                false
            }
        };
        server_class_cache.insert(class_name.to_string(), is_server);
        Ok(is_server)
    }

    async fn business_application_hydrate_ci_classes(
        &self,
        sys_ids: &[String],
        ci_classes: &mut HashMap<String, String>,
        summary: &mut BusinessApplicationServersSummary,
        diagnostics: &mut Vec<ReferenceResolutionDiagnostic>,
    ) -> Result<()> {
        let pending = sys_ids
            .iter()
            .filter(|sys_id| !ci_classes.contains_key(sys_id.as_str()))
            .cloned()
            .collect::<Vec<_>>();
        if pending.is_empty() {
            return Ok(());
        }

        let pending_refs = pending.iter().map(String::as_str).collect::<Vec<_>>();
        let response = self
            .ctx
            .client
            .table(CMDB_CI_TABLE)
            .in_list("sys_id", &pending_refs)
            .fields(BUSINESS_APPLICATION_CI_CLASS_FIELDS)
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .no_count()
            .limit(pending.len() as u32)
            .execute()
            .await;

        let records = match response {
            Ok(response) => response.records,
            Err(SnowApiError::Api { status, .. }) if status == 401 || status == 403 => {
                summary.acl_restricted_count += pending.len();
                for sys_id in pending {
                    push_business_application_server_diagnostic(
                        summary,
                        diagnostics,
                        "sys_class_name",
                        CMDB_CI_TABLE,
                        &sys_id,
                        ReferenceResolutionReason::ReferenceAclRestricted,
                        "ACL restricted cmdb_ci class read",
                    );
                }
                return Ok(());
            }
            Err(err) => return Err(err.into()),
        };

        let mut found = HashSet::new();
        for record in records {
            let Some(sys_id) = servicenow_reference_sys_id(&record, "sys_id") else {
                continue;
            };
            if let Some(class_name) = servicenow_record_text(&record, "sys_class_name") {
                ci_classes.insert(sys_id.clone(), class_name);
                found.insert(sys_id);
            }
        }
        for sys_id in pending {
            if found.contains(&sys_id) {
                continue;
            }
            summary.missing_ci_count += 1;
            push_business_application_server_diagnostic(
                summary,
                diagnostics,
                "sys_class_name",
                CMDB_CI_TABLE,
                &sys_id,
                ReferenceResolutionReason::ReferenceNotFound,
                "CI class could not be read",
            );
        }
        Ok(())
    }

    async fn business_application_hydrate_servers(
        &self,
        sys_ids: &[String],
        summary: &mut BusinessApplicationServersSummary,
        diagnostics: &mut Vec<ReferenceResolutionDiagnostic>,
    ) -> Result<Vec<Server>> {
        let mut unique = Vec::new();
        let mut seen = HashSet::new();
        for sys_id in sys_ids {
            if seen.insert(sys_id) {
                unique.push(sys_id.clone());
            }
        }
        if unique.is_empty() {
            return Ok(Vec::new());
        }

        let sys_id_refs = unique.iter().map(String::as_str).collect::<Vec<_>>();
        // Query the BASE `cmdb_ci_server` table by `sys_id` only. In
        // ServiceNow's table-per-hierarchy model the base table transparently
        // returns rows of every subclass (linux, win, esx, aix, ...), so a
        // record exists here iff the CI is a true server. The previous
        // `sys_class_name IN SERVER_TABLES` filter was over-restrictive: it
        // excluded legitimate subclass servers whose class is not in the narrow
        // alias list, silently dropping them. Relying on the base-table query is
        // both correct (non-servers simply do not exist in `cmdb_ci_server`) and
        // hierarchy-complete.
        let response = self
            .ctx
            .client
            .table(SERVER_TABLE)
            .in_list("sys_id", &sys_id_refs)
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .no_count()
            .limit(unique.len() as u32)
            .execute()
            .await;

        let records = match response {
            Ok(response) => response.records,
            Err(SnowApiError::Api { status, .. }) if status == 401 || status == 403 => {
                summary.acl_restricted_count += unique.len();
                for sys_id in unique {
                    push_business_application_server_diagnostic(
                        summary,
                        diagnostics,
                        "sys_id",
                        SERVER_TABLE,
                        &sys_id,
                        ReferenceResolutionReason::ReferenceAclRestricted,
                        "ACL restricted server CI read",
                    );
                }
                return Ok(Vec::new());
            }
            Err(err) => return Err(err.into()),
        };

        let mut found = HashSet::new();
        let mut servers = Vec::new();
        for record in records {
            found.insert(record.sys_id.clone());
            servers.push(Server::from_servicenow(&record)?);
        }
        for sys_id in unique {
            if found.contains(&sys_id) {
                continue;
            }
            summary.missing_ci_count += 1;
            push_business_application_server_diagnostic(
                summary,
                diagnostics,
                "sys_id",
                SERVER_TABLE,
                &sys_id,
                ReferenceResolutionReason::ReferenceNotFound,
                "server CI could not be read",
            );
        }
        Ok(servers)
    }

    /// Force-live exact server fetch, persisting the hit to the local cache.
    ///
    /// Back-compat wrapper around [`SnowCore::get_server_live`] with
    /// `persist = true`; flattens the structured [`ServerGetError`] into the
    /// crate's `anyhow` error so existing callers (the daemon
    /// `server_get_fresh` RPC) keep their current signature. New read-through
    /// callers that need to distinguish not-found / ACL / network / duplicate
    /// should call [`SnowCore::get_server_live`] directly.
    pub async fn get_server_fresh(&self, lookup: ServerLookup) -> Result<Option<Server>> {
        match self.get_server_live(lookup, true).await {
            Ok(server) => Ok(server),
            Err(ServerGetError::NotFound) => Ok(None),
            Err(other) => Err(anyhow::anyhow!(other.to_string())),
        }
    }

    /// Live, exact-match server fetch against `cmdb_ci_server` (base table, so
    /// all server subclasses are returned transparently). This is the single
    /// shared live code path behind the read-through `server_get`: the daemon /
    /// CLI path calls it with `persist = true`, the MCP path with
    /// `persist = false` (mutation-free MCP per the completion plan's Work
    /// Package G boundary).
    ///
    /// Returns:
    /// - `Ok(Some(server))` when ServiceNow returns exactly one matching CI.
    /// - `Err(ServerGetError::NotFound)` only when ServiceNow confirms absence
    ///   (HTTP 404 for a sys_id read, or an empty result set for a name / IP
    ///   query). This is authoritative not-found.
    /// - `Err(ServerGetError::AclRestricted)` on HTTP 401/403 — the CI may
    ///   exist but the caller cannot read it; distinct from not-found.
    /// - `Err(ServerGetError::Network)` on a transport/timeout failure — never
    ///   conflated with not-found.
    /// - `Err(ServerGetError::Disambiguation)` when more than one CI matches an
    ///   exact name / IP selector (duplicate CIs in CMDB).
    /// - `Err(ServerGetError::Hydration)` when a returned row cannot be parsed
    ///   into the typed `Server` shape.
    ///
    /// When `persist` is `true` and a record is found, the raw record is written
    /// to the local cache via [`SnowCore::persist_record`].
    pub async fn get_server_live(
        &self,
        lookup: ServerLookup,
        persist: bool,
    ) -> std::result::Result<Option<Server>, ServerGetError> {
        let record = match lookup {
            ServerLookup::SysId(sys_id) => {
                let sys_id = normalize_record_lookup_sys_id(&sys_id)
                    .map_err(|err| ServerGetError::Other(err.to_string()))?;
                match self
                    .ctx
                    .client
                    .table(SERVER_TABLE)
                    .display_value(DisplayValue::Both)
                    .exclude_reference_link(true)
                    .get(&sys_id)
                    .await
                {
                    Ok(record) => Some(record),
                    Err(SnowApiError::Api { status: 404, .. }) => None,
                    Err(err) => return Err(ServerGetError::from_api(err)),
                }
            }
            ServerLookup::ExactName(name) => {
                let name = non_empty_owned(Some(&name)).ok_or_else(|| {
                    ServerGetError::Other("server name cannot be empty".to_string())
                })?;
                let records = self.server_exact_query("name", &name).await?;
                if records.len() > 1 {
                    return Err(ServerGetError::Disambiguation {
                        selector: format!("name={name}"),
                        matched: records.len(),
                    });
                }
                records.into_iter().next()
            }
            ServerLookup::IpAddress(ip_address) => {
                let ip_address = non_empty_owned(Some(&ip_address)).ok_or_else(|| {
                    ServerGetError::Other("server IP address cannot be empty".to_string())
                })?;
                let records = self.server_exact_query("ip_address", &ip_address).await?;
                if records.len() > 1 {
                    return Err(ServerGetError::Disambiguation {
                        selector: format!("ip_address={ip_address}"),
                        matched: records.len(),
                    });
                }
                records.into_iter().next()
            }
        };

        let Some(record) = record else {
            return Err(ServerGetError::NotFound);
        };
        let server = Server::from_servicenow(&record)
            .map_err(|err| ServerGetError::Hydration(err.to_string()))?;
        if persist {
            self.persist_record(&record)
                .map_err(|err| ServerGetError::Other(err.to_string()))?;
        }
        Ok(Some(server))
    }

    /// Issue a bounded exact-match (`field = value`) query against the base
    /// `cmdb_ci_server` table for the read-through live fallback. `limit: 2` is
    /// sufficient to detect duplicate-CI ambiguity without fetching the whole
    /// set. Transport / ACL failures are classified into [`ServerGetError`].
    async fn server_exact_query(
        &self,
        field: &str,
        value: &str,
    ) -> std::result::Result<Vec<Record>, ServerGetError> {
        let query = self
            .ctx
            .client
            .table(SERVER_TABLE)
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .limit(2)
            .order_by("name", Order::Asc)
            .equals(field, value);
        match query.execute().await {
            Ok(result) => Ok(result.records),
            Err(err) => Err(ServerGetError::from_api(err)),
        }
    }

    pub async fn search_servers(&self, params: ServerSearchParams) -> Result<Vec<SnowRecord>> {
        Ok(self
            .search_servers_live(params)
            .await?
            .into_iter()
            .map(|server| server.record)
            .collect())
    }

    pub async fn search_servers_live(&self, params: ServerSearchParams) -> Result<Vec<Server>> {
        params.validate()?;
        let records = self.server_base_query(params)?.execute().await?.records;
        let mut servers = Vec::with_capacity(records.len());
        for record in records {
            let server = Server::from_servicenow(&record)?;
            self.persist_record(&record)?;
            servers.push(server);
        }
        Ok(servers)
    }

    pub async fn query_servers(&self, query: ServerQuery) -> Result<Vec<SnowRecord>> {
        self.ctx.query.query_servers(query).await
    }

    pub async fn business_application_servers_cached(
        &self,
        params: BusinessApplicationServersCachedParams,
    ) -> Result<Option<BusinessApplicationServersCachedResult>> {
        self.ctx
            .query
            .business_application_servers_cached(params)
            .await
    }

    pub async fn business_applications_for_server(
        &self,
        params: BusinessApplicationsForServerParams,
    ) -> Result<Option<BusinessApplicationsForServerResult>> {
        self.ctx
            .query
            .business_applications_for_server(params)
            .await
    }

    fn server_base_query(&self, params: ServerSearchParams) -> Result<TableApi> {
        let mut query = self
            .ctx
            .client
            .table(SERVER_TABLE)
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .limit(params.validated_limit()? as u32)
            .order_by("name", Order::Asc);

        if let Some(class) = non_empty_owned(params.class.as_deref()) {
            let class = canonical_server_class(&class)?;
            if class != SERVER_TABLE {
                query = query.equals("sys_class_name", class);
            } else {
                query = query.in_list("sys_class_name", SERVER_LEAF_TABLES);
            }
        } else {
            query = query.in_list("sys_class_name", SERVER_LEAF_TABLES);
        }
        if let Some(name) = non_empty_owned(params.name.as_deref()) {
            query = query.contains("name", &name);
        }
        if let Some(ip_address) = non_empty_owned(params.ip_address.as_deref()) {
            query = query.equals("ip_address", &ip_address);
        }
        query = apply_reference_name_or_sys_id_filter(
            query,
            "managed_by_group",
            params.ci_owner_group.as_deref(),
        )?;
        Ok(query)
    }

    /// Run a live Business Application search+persist and aggregate a sync summary.
    ///
    /// Reuses [`Self::search_business_applications_live`] for the actual fetch and
    /// persistence (honoring `options.persist`), then rolls up reference
    /// resolution and dictionary-degradation status across the returned
    /// applications. `params` is optional; `None` produces the default bounded
    /// Business Application search ordered by `name`.
    ///
    /// This is a degraded-tolerant read: unresolved references and an
    /// unavailable dictionary are reported in the summary, never errors.
    pub async fn sync_business_applications(
        &self,
        params: Option<BusinessApplicationSearchParams>,
        options: BusinessApplicationHydrationOptions,
    ) -> Result<BusinessApplicationSyncSummary> {
        let params = params.unwrap_or_default();
        let page_size = params.validated_limit()?;
        // When the caller explicitly asks for a dictionary refresh, attempt it
        // up front so degraded status reflects the freshest metadata. The fetch
        // is best-effort: a failure leaves us in baseline/degraded mode. We do
        // the refresh here (and clear the flag for the live search) so it runs
        // exactly once per sync.
        let mut dictionary_refreshed = false;
        let mut live_options = options.clone();
        if options.refresh_dictionary {
            dictionary_refreshed = self.refresh_business_application_dictionary().await.is_ok();
            live_options.refresh_dictionary = false;
        }

        let applications = self
            .search_business_applications_live(params, live_options)
            .await?;

        let mut summary = BusinessApplicationSyncSummary {
            all: false,
            table: BUSINESS_APPLICATION_TABLE.to_string(),
            page_size,
            pages: usize::from(!applications.is_empty()),
            total_returned: applications.len(),
            total_applications: applications.len(),
            persisted: if options.persist {
                applications.len()
            } else {
                0
            },
            dictionary_refreshed,
            ..Default::default()
        };

        roll_up_business_application_summary(&mut summary, &applications);

        Ok(summary)
    }

    /// Drain every live Business Application page and persist each page before
    /// requesting the next one.
    ///
    /// This explicit full-inventory path uses the `servicenow_rs` paginator
    /// directly. It deliberately avoids `execute_all()` so persistence remains
    /// durable page by page and the whole live result set is never required in
    /// memory.
    pub async fn sync_all_business_applications(
        &self,
        options: BusinessApplicationHydrationOptions,
    ) -> Result<BusinessApplicationSyncSummary> {
        let mut dictionary_refreshed = false;
        let mut live_options = options.clone();
        if options.refresh_dictionary {
            dictionary_refreshed = self.refresh_business_application_dictionary().await.is_ok();
            live_options.refresh_dictionary = false;
        }

        let aliases = self
            .resolve_business_application_aliases(live_options.refresh_dictionary)
            .await;
        let mut paginator = self
            .ctx
            .client
            .table(BUSINESS_APPLICATION_TABLE)
            .equals("sys_class_name", BUSINESS_APPLICATION_TABLE)
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .limit(BUSINESS_APPLICATION_SYNC_ALL_PAGE_SIZE as u32)
            .order_by("name", Order::Asc)
            .order_by("sys_id", Order::Asc)
            .paginate()?;

        let mut summary = BusinessApplicationSyncSummary {
            all: true,
            table: BUSINESS_APPLICATION_TABLE.to_string(),
            page_size: BUSINESS_APPLICATION_SYNC_ALL_PAGE_SIZE,
            dictionary_refreshed,
            ..Default::default()
        };

        while let Some(page) = paginator.next_page().await? {
            if page.records.is_empty() {
                continue;
            }
            let applications = self
                .hydrate_business_application_page(page.records, &aliases, &live_options)
                .await?;

            summary.pages += 1;
            summary.total_returned += applications.len();
            summary.total_applications += applications.len();
            if options.persist {
                summary.persisted += applications.len();
            }
            roll_up_business_application_summary(&mut summary, &applications);
        }

        Ok(summary)
    }

    async fn hydrate_business_application_page(
        &self,
        records: Vec<Record>,
        aliases: &BusinessApplicationFieldAliases,
        options: &BusinessApplicationHydrationOptions,
    ) -> Result<Vec<BusinessApplication>> {
        let mut business_applications = Vec::with_capacity(records.len());
        for record in records {
            let mut business_application = BusinessApplication::from_servicenow(&record, aliases)?;
            if options.persist {
                self.persist_record(&record)?;
                self.persist_business_application_reference_primitives(
                    &mut business_application,
                    options,
                )
                .await?;
            }
            business_applications.push(business_application);
        }
        Ok(business_applications)
    }

    async fn persist_business_application_reference_primitives(
        &self,
        business_application: &mut BusinessApplication,
        options: &BusinessApplicationHydrationOptions,
    ) -> Result<()> {
        if !options.persist {
            return Ok(());
        }

        let mut diagnostics = Vec::new();
        let max_direct_references = 100usize;
        let total_references = business_application.references.len();

        for descriptor in business_application
            .references
            .iter_mut()
            .take(max_direct_references)
        {
            let can_resolve = options.resolve_references
                && options.reference_depth > 0
                && is_business_application_reference_table_resolvable(
                    descriptor.reference_table.as_str(),
                );

            if !can_resolve {
                if descriptor.resolution_status == ReferenceResolutionStatus::Resolved {
                    descriptor.resolution_status =
                        if is_business_application_reference_table_resolvable(
                            descriptor.reference_table.as_str(),
                        ) {
                            ReferenceResolutionStatus::Unresolved
                        } else {
                            ReferenceResolutionStatus::UnknownTable
                        };
                }
                let diagnostic = self.persist_reference_primitive_stub(descriptor, None)?;
                diagnostics.push(diagnostic);
                continue;
            }

            match self
                .ctx
                .client
                .table(descriptor.reference_table.as_str())
                .display_value(DisplayValue::Both)
                .exclude_reference_link(true)
                .get(descriptor.reference_sys_id.as_str())
                .await
            {
                Ok(record) => {
                    descriptor.resolution_status = ReferenceResolutionStatus::Resolved;
                    descriptor.diagnostic = None;
                    self.persist_resolved_reference_primitive(descriptor, &record)?;
                }
                Err(SnowApiError::Api { status: 404, .. }) => {
                    descriptor.resolution_status = ReferenceResolutionStatus::NotFound;
                    descriptor.diagnostic = Some("referenced record was not found".to_string());
                    diagnostics.push(self.persist_reference_primitive_stub(descriptor, None)?);
                }
                Err(err) => {
                    descriptor.resolution_status = ReferenceResolutionStatus::Error;
                    let message = err.to_string();
                    descriptor.diagnostic = Some(message.clone());
                    diagnostics
                        .push(self.persist_reference_primitive_stub(descriptor, Some(message))?);
                }
            }
        }

        if total_references > max_direct_references {
            diagnostics.push(ReferenceResolutionDiagnostic {
                field: "*".to_string(),
                reference_table: "*".to_string(),
                reference_sys_id: business_application.record.sys_id.clone(),
                display_value: Some(business_application.name.clone()),
                reason: ReferenceResolutionReason::FanoutLimitExceeded,
                message: Some(format!(
                    "resolved first {max_direct_references} Business Application references out of {total_references}"
                )),
            });
        }

        for diagnostic in diagnostics {
            push_unique_reference_diagnostic(
                &mut business_application.unresolved_references,
                diagnostic,
            );
        }

        Ok(())
    }

    fn persist_resolved_reference_primitive(
        &self,
        descriptor: &ReferencePrimitiveDescriptor,
        record: &Record,
    ) -> Result<()> {
        let raw_json = serialize_record_document(record);
        let display_name = primitive_display_name(record, descriptor);
        let relative_path = self.persist_reference_primitive_markdown(
            descriptor,
            &display_name,
            ReferenceResolutionStatus::Resolved,
            &raw_json,
            None,
        )?;
        let synced_at = Utc::now();
        self.ctx
            .query
            .store()
            .upsert_primitive_object(&PrimitiveObjectRow {
                sys_id: descriptor.reference_sys_id.clone(),
                table_name: descriptor.reference_table.clone(),
                resource_type: primitive_resource_type_name(&descriptor.primitive_type).to_string(),
                display_name,
                number: record_first_value(record, &["number", "user_name"]).or_else(|| {
                    record
                        .get_raw("number")
                        .or_else(|| record.get_display("number"))
                        .map(ToOwned::to_owned)
                }),
                file_path: Some(relative_path.to_string_lossy().into_owned()),
                raw_json: raw_json.to_string(),
                synced_at,
                sys_updated_on: record
                    .get_raw("sys_updated_on")
                    .or_else(|| record.get_display("sys_updated_on"))
                    .or_else(|| record.get_str("sys_updated_on"))
                    .map(ToOwned::to_owned),
                resolution_status: PrimitiveResolutionStatus::Resolved,
                last_error: None,
            })?;

        if let Some(raw_map) = raw_json.as_object() {
            for (field_name, raw_value) in raw_map {
                let field = primitive_projected_field(
                    &descriptor.reference_sys_id,
                    field_name,
                    raw_value,
                    synced_at,
                );
                self.ctx
                    .query
                    .store()
                    .upsert_primitive_object_field(&descriptor.reference_sys_id, &field)?;
            }
        }
        Ok(())
    }

    fn persist_reference_primitive_stub(
        &self,
        descriptor: &ReferencePrimitiveDescriptor,
        last_error: Option<String>,
    ) -> Result<ReferenceResolutionDiagnostic> {
        let status = descriptor.resolution_status;
        let display_name = descriptor
            .display_value
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(descriptor.reference_sys_id.as_str())
            .to_string();
        let raw_json = serde_json::json!({
            "sys_id": descriptor.reference_sys_id,
            "table": descriptor.reference_table,
            "display_value": display_name,
            "source_field": descriptor.field,
            "resolution_status": reference_resolution_status_name(status),
            "diagnostic": descriptor.diagnostic.as_deref().or(last_error.as_deref()),
        });
        let relative_path = self.persist_reference_primitive_markdown(
            descriptor,
            &display_name,
            status,
            &raw_json,
            descriptor.diagnostic.as_deref().or(last_error.as_deref()),
        )?;
        self.ctx
            .query
            .store()
            .upsert_primitive_object(&PrimitiveObjectRow {
                sys_id: descriptor.reference_sys_id.clone(),
                table_name: descriptor.reference_table.clone(),
                resource_type: primitive_resource_type_name(&descriptor.primitive_type).to_string(),
                display_name: display_name.clone(),
                number: None,
                file_path: Some(relative_path.to_string_lossy().into_owned()),
                raw_json: raw_json.to_string(),
                synced_at: Utc::now(),
                sys_updated_on: None,
                resolution_status: primitive_status_from_reference_status(status),
                last_error,
            })?;

        Ok(ReferenceResolutionDiagnostic {
            field: descriptor.field.clone(),
            reference_table: descriptor.reference_table.clone(),
            reference_sys_id: descriptor.reference_sys_id.clone(),
            display_value: descriptor.display_value.clone(),
            reason: reason_from_reference_status(status),
            message: descriptor.diagnostic.clone(),
        })
    }

    fn persist_reference_primitive_markdown(
        &self,
        descriptor: &ReferencePrimitiveDescriptor,
        display_name: &str,
        status: ReferenceResolutionStatus,
        raw_json: &Value,
        diagnostic: Option<&str>,
    ) -> Result<PathBuf> {
        let relative_path = reference_primitive_relative_path(descriptor, display_name);
        let absolute_path = self.ctx.vault_path.join(&relative_path);
        let contents = render_reference_primitive_markdown(
            descriptor,
            display_name,
            status,
            raw_json,
            diagnostic,
        );
        let persisted = self
            .ctx
            .vault
            .write_markdown_file(absolute_path, &contents)?;
        Ok(persisted.relative_path)
    }

    /// Look up a record by number, checking the in-memory L1 cache first
    /// and falling through to the SQLite-backed query engine on a miss.
    ///
    /// Cached work records are only served when their projection was synced
    /// within the local TTL. Stale cached rows are refreshed through the same
    /// live path as [`get_record_fresh`], which also persists the refreshed
    /// projection back into the cache.
    pub async fn get_record(&self, number: &str) -> Result<Option<SnowRecord>> {
        let now = Utc::now();
        if let Some(record) = self.ctx.cache.get(number) {
            if work_record_cache_is_fresh(&record, now, self.ctx.cache_policy.work_record_ttl()) {
                return Ok(Some(record));
            }
            self.ctx.cache.invalidate(number);
            return self.get_record_fresh(number).await;
        }
        let record = self.ctx.query.get_record(number).await?;
        if let Some(ref record) = record {
            if !work_record_cache_is_fresh(record, now, self.ctx.cache_policy.work_record_ttl()) {
                return self.get_record_fresh(number).await;
            }
            self.ctx.cache.put(record.clone());
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
        self.ctx.get_record_fresh(number).await
    }

    pub async fn get_record_by_lookup_fresh(
        &self,
        lookup: RecordLookup,
    ) -> Result<Option<SnowRecord>> {
        match lookup {
            RecordLookup::Number(number) => self.get_record_fresh(&number).await,
            RecordLookup::TableSysId { table, sys_id } => {
                self.get_record_by_table_sys_id_fresh(&table, &sys_id).await
            }
        }
    }

    pub async fn get_record_by_table_sys_id_fresh(
        &self,
        table: &str,
        sys_id: &str,
    ) -> Result<Option<SnowRecord>> {
        self.ctx
            .get_record_by_table_sys_id_fresh(table, sys_id)
            .await
    }

    async fn get_record_by_table_sys_id_fresh_with_source(
        &self,
        table: &str,
        sys_id: &str,
    ) -> Result<Option<(Record, SnowRecord)>> {
        self.ctx
            .get_record_by_table_sys_id_fresh_with_source(table, sys_id)
            .await
    }

    async fn enrich_record_journals(&self, record: &mut Record) -> Result<()> {
        self.ctx.enrich_record_journals(record).await
    }

    pub fn tombstone_record(&self, sys_id: &str, when: DateTime<Utc>) -> Result<()> {
        self.ctx.tombstone_record(sys_id, when)
    }

    pub async fn prune_record(&self, sys_id: &str, when: DateTime<Utc>) -> Result<()> {
        self.ctx.prune_record(sys_id, when).await
    }

    pub async fn get_knowledge_article(&self, number: &str) -> Result<Option<KnowledgeArticle>> {
        self.ctx.query.get_knowledge_article(number).await
    }

    pub async fn get_knowledge_article_cached_or_fresh(
        &self,
        number: &str,
    ) -> Result<Option<KnowledgeArticle>> {
        let cached = self.get_knowledge_article(number).await?;
        if cached.as_ref().is_some_and(|article| article.body_cached) {
            return Ok(cached);
        }

        match self.get_knowledge_article_fresh_inner(number, false).await {
            Ok(Some(fresh)) => Ok(Some(fresh)),
            Ok(None) => Ok(cached),
            Err(_) if cached.is_some() => Ok(cached),
            Err(err) => Err(err),
        }
    }

    pub async fn search_knowledge(
        &self,
        query: &str,
        filters: KnowledgeSearchFilters,
    ) -> Result<Vec<KnowledgeArticle>> {
        self.ctx.query.search_knowledge(query, filters).await
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
                let config = &self.ctx.config.kb.semantic_search;
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
                let config = &self.ctx.config.kb.semantic_search;
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
        let config = &self.ctx.config.kb.semantic_search;
        let embeddings = self.ctx.query.store().list_knowledge_embeddings()?;
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

        let meta = self.ctx.query.store().knowledge_semantic_meta()?;
        Ok(KnowledgeSemanticStatus {
            enabled: config.enabled,
            provider: config.provider.clone(),
            model: config.model.clone(),
            dimensions,
            active_kb_articles: articles.len(),
            metadata_embeddings: self
                .ctx
                .query
                .store()
                .count_knowledge_embeddings_by_coverage(
                    &config.model,
                    KnowledgeEmbeddingCoverage::Metadata,
                )?,
            full_text_embeddings: self
                .ctx
                .query
                .store()
                .count_knowledge_embeddings_by_coverage(
                    &config.model,
                    KnowledgeEmbeddingCoverage::FullText,
                )?,
            stale_rows,
            orphan_rows: self.ctx.query.store().count_orphan_knowledge_embeddings()?,
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
            .ctx
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
            .ctx
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
            .ctx
            .query
            .store()
            .list_active_records(Some(ResourceType::Knowledge))?
            .into_iter()
            .map(|row| row.number)
            .collect::<Vec<_>>();
        numbers.sort();

        let mut articles = Vec::new();
        for number in numbers {
            let Some(article) = self.ctx.query.get_knowledge_article(&number).await? else {
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
        let config = &self.ctx.config.kb.semantic_search;
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
        let config = &self.ctx.config.kb.semantic_search;
        let articles = self.load_active_knowledge_articles_for_semantic().await?;
        let existing = self
            .ctx
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
                    self.ctx
                        .query
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
                self.ctx.query.store().upsert_knowledge_embedding(
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

        self.ctx.query.store().prune_orphan_knowledge_embeddings()?;
        let completed_at = Some(Utc::now());
        self.ctx
            .query
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
        if !self.ctx.config.kb.semantic_search.enabled {
            return;
        }
        if let Err(err) = self.rebuild_knowledge_semantic_index(false).await {
            eprintln!("snow_core: semantic KB rebuild failed after {trigger}: {err}");
        }
    }

    async fn load_active_knowledge_articles_for_semantic(&self) -> Result<Vec<KnowledgeArticle>> {
        let rows = self
            .ctx
            .query
            .store()
            .list_active_records(Some(ResourceType::Knowledge))?;
        let mut seen = std::collections::HashSet::new();
        let mut articles = Vec::new();
        for row in rows {
            if !seen.insert(row.sys_id.clone()) {
                continue;
            }
            if let Some(article) = self.ctx.query.get_knowledge_article(&row.number).await?
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
            .ctx
            .query
            .store()
            .get_knowledge_embedding(&article.record.sys_id)?
            .filter(|row| row.model == self.ctx.config.kb.semantic_search.model)
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
                            .unwrap_or(self.ctx.config.kb.semantic_search.top_k),
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
            .unwrap_or(self.ctx.config.kb.semantic_search.min_score_millis)
            as f32
            / 1000.0;
        let limit = filters
            .limit
            .unwrap_or(self.ctx.config.kb.semantic_search.top_k);
        let candidate_pool = self.ctx.config.kb.semantic_search.candidate_pool;
        let articles = self
            .load_active_knowledge_articles_for_semantic()
            .await?
            .into_iter()
            .filter(|article| knowledge_article_matches_semantic_filters(article, filters))
            .map(|article| (article.record.sys_id.clone(), article))
            .collect::<HashMap<_, _>>();

        let mut ranked = self
            .ctx
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
        let candidate_pool = self.ctx.config.kb.semantic_search.candidate_pool;
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
            .ctx
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
                .unwrap_or(self.ctx.config.kb.semantic_search.top_k),
        );
        Ok(hits)
    }

    pub async fn get_approval(&self, number: &str) -> Result<Option<ApprovalRecord>> {
        self.approvals.get_approval(number).await
    }

    pub fn degraded_reads(&self) -> Vec<DegradedReadDiagnostic> {
        self.ctx.query.degraded_reads()
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

            self.ctx.query.store().upsert_record_with_tags(
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
        let mut cached = self.ctx.query.get_children(number).await?;
        if !cached.is_empty() {
            return Ok(cached);
        }

        let Some(parent_record) = self.ctx.client.get_by_number(number).await? else {
            return Ok(Vec::new());
        };
        self.persist_record(&parent_record)?;

        let Some((child_table, child_link_field)) =
            child_relation_for_parent_table(&parent_record.table)
        else {
            return Ok(Vec::new());
        };

        let mut query = self
            .ctx
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

        cached = self.ctx.query.get_children(number).await?;
        Ok(cached)
    }

    pub async fn resource_plan_list(
        &self,
        input: ResourcePlanListInput,
    ) -> Result<ResourcePlanListResponse> {
        let validated = validate_list_input(input)?;
        let resolved_task_sys_id = match &validated.task_selector {
            TaskSelector::Number(number) => {
                Some(self.resolve_resource_plan_parent_number(number).await?)
            }
            TaskSelector::SysId(sys_id) => Some(sys_id.clone()),
            TaskSelector::None => None,
        };

        let mut query = self
            .ctx
            .client
            .table("resource_plan")
            .fields(RESOURCE_PLAN_LIST_FIELDS)
            .dot_walk(RESOURCE_PLAN_LIST_DOT_WALK)
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .order_by("number", Order::Asc)
            .limit(validated.effective_limit as u32);

        if let Some(task_sys_id) = resolved_task_sys_id.as_deref() {
            query = query.equals("task", task_sys_id);
        }

        let resource_type_hint = match &validated.resource {
            ResolvedResourceFilter::Group(sys_id) => {
                query = query.equals("group_resource", sys_id);
                Some(ResourcePlanResourceType::Group)
            }
            ResolvedResourceFilter::User(sys_id) => {
                query = query.equals("user_resource", sys_id);
                Some(ResourcePlanResourceType::User)
            }
            ResolvedResourceFilter::TypeOnly(resource_type) => {
                query = query.equals("resource_type", resource_type.as_snow_str());
                Some(*resource_type)
            }
            ResolvedResourceFilter::None => None,
        };

        match validated.states.as_slice() {
            [] => {}
            [state] => {
                query = query.equals("state", state);
            }
            states => {
                let state_refs = states.iter().map(String::as_str).collect::<Vec<_>>();
                query = query.in_list("state", &state_refs);
            }
        }

        let records = query.execute().await?.records;
        let records = records
            .iter()
            .map(|record| resource_plan_record_from_row(record, resource_type_hint))
            .collect::<Vec<_>>();
        let total_returned = records.len();

        Ok(ResourcePlanListResponse {
            records,
            query_summary: ResourcePlanQuerySummary {
                filters_applied: validated.filters_applied,
                total_returned,
                limit: validated.effective_limit,
                truncated: total_returned == validated.effective_limit,
                warnings: validated.warnings,
            },
        })
    }

    pub async fn list_records(&self) -> Result<Vec<SnowRecord>> {
        self.list_records_query(query::filter::ListQuery::new())
            .await
    }

    pub async fn list_records_query(&self, query: ListQuery) -> Result<Vec<SnowRecord>> {
        self.ctx.query.list_records(query).await
    }

    pub async fn my_tasks(&self) -> Result<Vec<SnowRecord>> {
        self.ctx.query.my_tasks().await
    }

    pub async fn current_user_sys_id(&self) -> Result<String> {
        self.ctx.current_user_sys_id().await
    }

    pub async fn list_my_timecards(&self, week: WeekSelector) -> Result<TimecardSheet> {
        let actor = self
            .resolve_user_ref(&self.ctx.config.instance.user)
            .await?;
        let sheet_row = self.resolve_my_timecard_sheet(week, &actor).await?;
        let sheet_ref = SimpleRef {
            sys_id: sheet_row.sys_id.clone(),
            table: resource::timecard::TimecardResource::SHEET_TABLE.to_string(),
            display: sheet_row
                .get_display("week_starts_on")
                .or_else(|| sheet_row.get_raw("week_starts_on"))
                .or_else(|| sheet_row.get_str("week_starts_on"))
                .unwrap_or(sheet_row.sys_id.as_str())
                .to_string(),
        };
        let week_starts_on = sheet_row
            .get_raw("week_starts_on")
            .or_else(|| sheet_row.get_display("week_starts_on"))
            .or_else(|| sheet_row.get_str("week_starts_on"))
            .unwrap_or_default()
            .to_string();
        let state = sheet_row
            .get_display("state")
            .or_else(|| sheet_row.get_raw("state"))
            .or_else(|| sheet_row.get_str("state"))
            .unwrap_or_default()
            .to_string();

        let mut card_rows = self
            .ctx
            .client
            .table(resource::timecard::TimecardResource::TABLE)
            .equals("time_sheet", &sheet_ref.sys_id)
            .fields(TIME_CARD_FIELDS)
            .display_value(DisplayValue::Both)
            .order_by("task", Order::Asc)
            .limit(500)
            .execute()
            .await?
            .records;

        if card_rows.is_empty() && !week_starts_on.trim().is_empty() {
            card_rows = self
                .ctx
                .client
                .table(resource::timecard::TimecardResource::TABLE)
                .equals("user", &actor.sys_id)
                .equals("week_starts_on", &week_starts_on)
                .fields(TIME_CARD_FIELDS)
                .display_value(DisplayValue::Both)
                .order_by("task", Order::Asc)
                .limit(500)
                .execute()
                .await?
                .records;
        }

        let mut cards = Vec::new();
        for row in &card_rows {
            let card = resource::timecard::TimecardResource::from_servicenow(row)?;
            if card.is_owned_by(&actor.sys_id) {
                cards.push(card);
            }
        }

        Ok(TimecardSheet {
            sheet: Some(sheet_ref),
            week_starts_on,
            state,
            cards,
        })
    }

    pub async fn get_timecard_fresh(&self, sys_id: &str) -> Result<Option<TimeCard>> {
        let sys_id = sys_id.trim();
        if sys_id.is_empty() {
            return Err(anyhow::anyhow!("time card sys_id cannot be empty"));
        }
        let record = match self
            .ctx
            .client
            .table(resource::timecard::TimecardResource::TABLE)
            .fields(TIME_CARD_FIELDS)
            .display_value(DisplayValue::Both)
            .get(sys_id)
            .await
        {
            Ok(record) => record,
            Err(SnowApiError::Api { status: 404, .. }) => return Ok(None),
            Err(err) => return Err(err.into()),
        };
        Ok(Some(resource::timecard::TimecardResource::from_servicenow(
            &record,
        )?))
    }

    pub async fn set_timecard_hours(
        &self,
        sys_id: &str,
        day: Weekday,
        hours: TimeValue,
        mode: SetMode,
    ) -> Result<TimeCard> {
        let actor_sys_id = self.current_user_sys_id().await?;
        let card = self
            .get_timecard_fresh(sys_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("time card not found: {sys_id}"))?;

        self.ensure_timecard_write_allowed(&card, &actor_sys_id)?;

        let write_value = match mode {
            SetMode::Set => hours,
            SetMode::Add => {
                let current = resource::timecard::parse_existing_hours(card.day_hours(day))?;
                TimeValue::from_hours(current + hours.to_f64()?)?
            }
        };

        self.ctx
            .client
            .table(resource::timecard::TimecardResource::TABLE)
            .display_value(DisplayValue::Both)
            .update(
                sys_id.trim(),
                serde_json::json!({ day.field_name(): write_value.as_str() }),
            )
            .await?;

        let updated = self
            .get_timecard_fresh(sys_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("time card disappeared after update: {sys_id}"))?;
        if updated.sys_id != sys_id.trim() {
            return Err(anyhow::anyhow!(
                "time card update refetched sys_id {}, expected {}",
                updated.sys_id,
                sys_id
            ));
        }
        Ok(updated)
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
                self.ctx
                    .query
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
            .ctx
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
            .ctx
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
        self.approvals.my_approvals_fresh().await
    }

    pub async fn my_approvals_with_routing_fresh(&self) -> Result<ListMyApprovalsResponse> {
        self.approvals.my_approvals_with_routing_fresh().await
    }

    pub async fn my_approvals(&self) -> Result<Vec<ApprovalRecord>> {
        self.approvals.my_approvals().await
    }

    pub async fn my_projects(&self) -> Result<Vec<SnowRecord>> {
        let mut records = Vec::new();
        for resource_type in [ResourceType::Project, ResourceType::Demand] {
            records.extend(
                self.ctx
                    .query
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
                self.ctx
                    .query
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
        self.ctx.query.search(query, scope).await
    }

    pub async fn search_by_tag(&self, tag: &str, scope: SearchScope) -> Result<Vec<SearchResult>> {
        self.ctx.query.search_by_tag(tag, scope).await
    }

    pub async fn search_by_keyword(
        &self,
        keyword: &str,
        scope: SearchScope,
    ) -> Result<Vec<SearchResult>> {
        self.ctx.query.search_by_keyword(keyword, scope).await
    }

    pub async fn search_by_alias(
        &self,
        alias: &str,
        scope: SearchScope,
    ) -> Result<Vec<SearchResult>> {
        self.ctx.query.search_by_alias(alias, scope).await
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
        let results = self.ctx.query.search_enriched(query, scope.clone()).await?;
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
            if self.table_for_number(&normalized).is_some()
                && let Ok(Some(_)) = self.get_record_fresh(&normalized).await
            {
                return self.ctx.query.search_enriched(&normalized, scope).await;
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
        let written = self.ctx.client.table(table).create(payload).await?;
        self.refetch_story_write_result(table, &written.sys_id, &written)
            .await
    }

    async fn update_story_write_record(
        &self,
        table: &str,
        sys_id: &str,
        payload: serde_json::Value,
    ) -> Result<StoryWriteResult> {
        let written = self.ctx.client.table(table).update(sys_id, payload).await?;
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

    pub async fn create_change_request(
        &self,
        payload: serde_json::Value,
    ) -> Result<ChangeWriteResult> {
        self.create_change_write_record(resource::change::ChangeResource::PARENT_TABLE, payload)
            .await
    }

    pub async fn update_change_request(
        &self,
        sys_id: &str,
        payload: serde_json::Value,
    ) -> Result<ChangeWriteResult> {
        self.update_change_write_record(
            resource::change::ChangeResource::PARENT_TABLE,
            sys_id,
            payload,
        )
        .await
    }

    pub async fn create_change_task(
        &self,
        payload: serde_json::Value,
    ) -> Result<ChangeWriteResult> {
        self.create_change_write_record(resource::change::ChangeResource::CHILD_TABLE, payload)
            .await
    }

    pub async fn update_change_task(
        &self,
        sys_id: &str,
        payload: serde_json::Value,
    ) -> Result<ChangeWriteResult> {
        self.update_change_write_record(
            resource::change::ChangeResource::CHILD_TABLE,
            sys_id,
            payload,
        )
        .await
    }

    pub async fn create_resource_plan(
        &self,
        payload: serde_json::Value,
    ) -> Result<ResourcePlanWriteResult> {
        self.create_resource_plan_record(payload).await
    }

    pub async fn update_resource_plan(
        &self,
        sys_id: &str,
        payload: serde_json::Value,
    ) -> Result<ResourcePlanWriteResult> {
        self.update_resource_plan_record(sys_id, payload).await
    }

    async fn create_change_write_record(
        &self,
        table: &str,
        payload: serde_json::Value,
    ) -> Result<ChangeWriteResult> {
        let written = self.ctx.client.table(table).create(payload).await?;
        self.refetch_change_write_result(table, &written.sys_id, &written)
            .await
    }

    async fn update_change_write_record(
        &self,
        table: &str,
        sys_id: &str,
        payload: serde_json::Value,
    ) -> Result<ChangeWriteResult> {
        let written = self.ctx.client.table(table).update(sys_id, payload).await?;
        self.refetch_change_write_result(table, sys_id, &written)
            .await
    }

    async fn refetch_change_write_result(
        &self,
        table: &str,
        expected_sys_id: &str,
        write_response: &Record,
    ) -> Result<ChangeWriteResult> {
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

        resource::change::ChangeResource::write_result_from_fresh_row(fresh_record, &fresh_row)
    }

    async fn create_resource_plan_record(
        &self,
        payload: serde_json::Value,
    ) -> Result<ResourcePlanWriteResult> {
        let table = resource::resource_plan::ResourcePlanResource::TABLE;
        let written = self.ctx.client.table(table).create(payload).await?;
        self.refetch_resource_plan_result(&written.sys_id, &written)
            .await
    }

    async fn update_resource_plan_record(
        &self,
        sys_id: &str,
        payload: serde_json::Value,
    ) -> Result<ResourcePlanWriteResult> {
        let table = resource::resource_plan::ResourcePlanResource::TABLE;
        let written = self.ctx.client.table(table).update(sys_id, payload).await?;
        self.refetch_resource_plan_result(sys_id, &written).await
    }

    async fn refetch_resource_plan_result(
        &self,
        expected_sys_id: &str,
        write_response: &Record,
    ) -> Result<ResourcePlanWriteResult> {
        let table = resource::resource_plan::ResourcePlanResource::TABLE;
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

        resource::resource_plan::ResourcePlanResource::write_result_from_fresh_row(
            fresh_record,
            &fresh_row,
        )
    }

    pub async fn add_work_note(&self, number: &str, text: &str) -> Result<Option<SnowRecord>> {
        let Some((table, sys_id)) = self.lookup_table_and_sys_id(number).await? else {
            return Ok(None);
        };
        self.ctx.client.add_work_note(&table, &sys_id, text).await?;
        self.get_record_fresh(number).await
    }

    pub async fn search_catalog_items(&self, query: &str, limit: u32) -> Result<Vec<CatalogItem>> {
        let query = query.trim();
        if query.is_empty() {
            return Ok(Vec::new());
        }
        let limit = limit.clamp(1, 50);
        let records = self
            .ctx
            .client
            .table("sc_cat_item")
            .contains("name", query)
            .or_filter("short_description", Operator::Contains, query)
            .equals("active", "true")
            .fields(&[
                "sys_id",
                "name",
                "short_description",
                "sys_class_name",
                "active",
            ])
            .display_value(DisplayValue::Both)
            .limit(limit)
            .execute()
            .await?
            .records;

        Ok(records
            .into_iter()
            .map(|record| catalog_item_from_record(record, Vec::new()))
            .collect())
    }

    pub async fn get_catalog_item(&self, sys_id: &str) -> Result<CatalogItem> {
        let sys_id = normalize_record_lookup_sys_id(sys_id)?;
        let item = self
            .ctx
            .client
            .table("sc_cat_item")
            .fields(&[
                "sys_id",
                "name",
                "short_description",
                "sys_class_name",
                "active",
            ])
            .display_value(DisplayValue::Both)
            .get(&sys_id)
            .await?;
        let variables = self.catalog_item_variables(&sys_id).await?;
        Ok(catalog_item_from_record(item, variables))
    }

    async fn catalog_item_variables(&self, item_sys_id: &str) -> Result<Vec<CatalogVariable>> {
        let mut records = self
            .catalog_variable_rows("cat_item", &[item_sys_id])
            .await?;
        let variable_set_ids = self.catalog_variable_set_ids(item_sys_id).await?;
        if !variable_set_ids.is_empty() {
            let variable_set_refs = variable_set_ids
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            records.extend(
                self.catalog_variable_rows("variable_set", &variable_set_refs)
                    .await?,
            );
        }
        let mut seen = HashSet::new();
        records.retain(|record| seen.insert(record.sys_id.clone()));
        let choices = self
            .catalog_choices_for_variables(records.iter().map(|record| record.sys_id.as_str()))
            .await?;

        Ok(records
            .into_iter()
            .map(|record| catalog_variable_from_record(&record, &choices))
            .collect())
    }

    async fn catalog_variable_rows(&self, field: &str, values: &[&str]) -> Result<Vec<Record>> {
        if values.is_empty() {
            return Ok(Vec::new());
        }
        let mut query = self
            .ctx
            .client
            .table("item_option_new")
            .fields(&[
                "sys_id",
                "name",
                "question_text",
                "type",
                "mandatory",
                "default_value",
                "reference",
                "lookup_table",
                "list_table",
                "max_length",
                "order",
                "active",
            ])
            .display_value(DisplayValue::Both)
            .order_by("order", Order::Asc)
            .limit(1000);
        if values.len() == 1 {
            query = query.equals(field, values[0]);
        } else {
            query = query.in_list(field, values);
        }
        match query.equals("active", "true").execute().await {
            Ok(result) => Ok(result.records),
            Err(_) => {
                let mut fallback = self
                    .ctx
                    .client
                    .table("item_option_new")
                    .fields(&[
                        "sys_id",
                        "name",
                        "question_text",
                        "type",
                        "mandatory",
                        "default_value",
                        "reference",
                        "lookup_table",
                        "list_table",
                        "max_length",
                        "order",
                        "active",
                    ])
                    .display_value(DisplayValue::Both)
                    .order_by("order", Order::Asc)
                    .limit(1000);
                if values.len() == 1 {
                    fallback = fallback.equals(field, values[0]);
                } else {
                    fallback = fallback.in_list(field, values);
                }
                Ok(fallback.execute().await?.records)
            }
        }
    }

    async fn catalog_variable_set_ids(&self, item_sys_id: &str) -> Result<Vec<String>> {
        match self
            .ctx
            .client
            .table("io_set_item")
            .equals("sc_cat_item", item_sys_id)
            .fields(&["sys_id", "variable_set", "order"])
            .display_value(DisplayValue::Both)
            .order_by("order", Order::Asc)
            .limit(200)
            .execute()
            .await
        {
            Ok(result) => Ok(result
                .records
                .into_iter()
                .filter_map(|record| record_field_raw_or_display(&record, "variable_set"))
                .collect()),
            Err(_) => Ok(Vec::new()),
        }
    }

    async fn catalog_choices_for_variables<'a>(
        &self,
        variable_sys_ids: impl Iterator<Item = &'a str>,
    ) -> Result<HashMap<String, Vec<CatalogChoice>>> {
        let variable_sys_ids = variable_sys_ids
            .filter(|sys_id| !sys_id.trim().is_empty())
            .collect::<Vec<_>>();
        if variable_sys_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let choices = self
            .ctx
            .client
            .table("question_choice")
            .in_list("question", &variable_sys_ids)
            .fields(&["sys_id", "question", "value", "text", "order"])
            .display_value(DisplayValue::Both)
            .order_by("order", Order::Asc)
            .limit(1000)
            .execute()
            .await?
            .records;
        let mut grouped: HashMap<String, Vec<CatalogChoice>> = HashMap::new();
        for choice in choices {
            let Some(question) = record_field_raw_or_display(&choice, "question") else {
                continue;
            };
            let value = record_field_raw_or_display(&choice, "value").unwrap_or_default();
            let label =
                record_field_display_or_raw(&choice, "text").unwrap_or_else(|| value.clone());
            grouped
                .entry(question)
                .or_default()
                .push(CatalogChoice { value, label });
        }
        Ok(grouped)
    }

    pub async fn submit_catalog_request(
        &self,
        item_sys_id: &str,
        request_body: serde_json::Value,
    ) -> Result<CatalogSubmitResult> {
        let item_sys_id = normalize_record_lookup_sys_id(item_sys_id)?;
        let path = format!("/api/sn_sc/v1/servicecatalog/items/{item_sys_id}/order_now");
        let raw_result = self.ctx.client.post(&path, request_body).await?;
        let mut result = catalog_submit_result_from_response(
            item_sys_id.clone(),
            raw_result,
            self.ctx.client.base_url(),
            &self.ctx.config.instance.portal,
        );

        if let (Some(table), Some(sys_id)) = (result.table.as_deref(), result.sys_id.as_deref()) {
            let canonical_table = canonical_record_table(table);
            if matches!(canonical_table.as_str(), "sc_req_item" | "sc_request")
                && let Ok(Some((_, fresh))) = self
                    .get_record_by_table_sys_id_fresh_with_source(&canonical_table, sys_id)
                    .await
            {
                result.table = Some(fresh.table);
                result.sys_id = Some(fresh.sys_id);
                result.number = Some(fresh.number);
            }
        }
        if result.request_item_sys_id.is_none()
            && let Some(request_sys_id) = result.request_sys_id.clone()
            && let Some(mut ritm) = self.lookup_catalog_request_item(&request_sys_id).await
        {
            let ritm_sys_id = ritm.sys_id.clone();
            let ritm_number = ritm
                .get_raw("number")
                .or_else(|| ritm.get_str("number"))
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned);
            if let Err(err) = self.enrich_record_journals(&mut ritm).await {
                eprintln!(
                    "snow_core: journal enrichment failed for catalog request item {ritm_sys_id}: {err}"
                );
            }
            if let Err(err) = self.persist_record(&ritm) {
                eprintln!(
                    "snow_core: cache persist failed for catalog request item {ritm_sys_id}: {err}"
                );
            }
            result.table = Some("sc_req_item".to_string());
            result.sys_id = Some(ritm_sys_id.clone());
            result.number = ritm_number.clone();
            result.request_item_sys_id = Some(ritm_sys_id.clone());
            result.request_item_number = ritm_number;
            result.browser_url = Some(catalog_browser_url(
                self.ctx.client.base_url(),
                &self.ctx.config.instance.portal,
                "sc_req_item",
                &ritm_sys_id,
            ));
        }

        Ok(result)
    }

    async fn lookup_catalog_request_item(&self, request_sys_id: &str) -> Option<Record> {
        for attempt in 0..4 {
            match self
                .ctx
                .client
                .table("sc_req_item")
                .equals("request", request_sys_id)
                .display_value(DisplayValue::Both)
                .first()
                .await
            {
                Ok(Some(record)) => return Some(record),
                Ok(None) if attempt < 3 => {
                    tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                }
                Ok(None) => return None,
                Err(err) => {
                    eprintln!(
                        "snow_core: request item lookup failed for catalog request {request_sys_id}: {err}"
                    );
                    return None;
                }
            }
        }
        None
    }

    pub async fn list_attachments(&self, number: &str) -> Result<Option<Vec<AttachmentMetadata>>> {
        let Some((table, sys_id)) = self.lookup_table_and_sys_id(number).await? else {
            return Ok(None);
        };
        Ok(Some(
            self.ctx.client.list_attachments(&table, &sys_id).await?,
        ))
    }

    pub async fn upload_attachment_file(
        &self,
        number: &str,
        path: impl AsRef<Path>,
        file_name: Option<&str>,
        content_type: Option<&str>,
    ) -> Result<Option<AttachmentMetadata>> {
        let Some((table, sys_id)) = self.lookup_table_and_sys_id(number).await? else {
            return Ok(None);
        };
        Ok(Some(
            self.ctx
                .client
                .upload_attachment_file(&table, &sys_id, path, file_name, content_type)
                .await?,
        ))
    }

    pub async fn set_state(&self, number: &str, state: &str) -> Result<Option<SnowRecord>> {
        let Some((table, sys_id)) = self.lookup_table_and_sys_id(number).await? else {
            return Ok(None);
        };
        self.ctx
            .client
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
        self.ctx
            .client
            .table(&table)
            .update(
                &sys_id,
                serde_json::json!({ "assigned_to": assignee_sys_id }),
            )
            .await?;
        self.get_record_fresh(number).await
    }

    pub async fn approve(&self, number: &str, comment: Option<&str>) -> Result<Option<SnowRecord>> {
        self.approvals.approve(number, comment).await
    }

    pub async fn approve_approval(
        &self,
        approval_sys_id: &str,
        comment: Option<&str>,
    ) -> Result<Option<SnowRecord>> {
        self.approvals
            .approve_approval(approval_sys_id, comment)
            .await
    }

    pub async fn reject(&self, number: &str, reason: &str) -> Result<Option<SnowRecord>> {
        self.approvals.reject(number, reason).await
    }

    pub async fn reject_approval(
        &self,
        approval_sys_id: &str,
        reason: &str,
    ) -> Result<Option<SnowRecord>> {
        self.approvals
            .reject_approval(approval_sys_id, reason)
            .await
    }

    pub fn browser_url(&self, number: &str) -> String {
        format!(
            "{}/nav_to.do?uri={}.do?sysparm_query=number={}",
            self.ctx.client.base_url(),
            self.infer_table(number),
            number
        )
    }

    pub fn vault_relative_path_for_sys_id(&self, sys_id: &str) -> Result<Option<String>> {
        Ok(self
            .ctx
            .query
            .store()
            .get_record_by_sys_id(sys_id)?
            .and_then(|row| row.file_path))
    }

    fn persist_record(&self, record: &Record) -> Result<()> {
        self.ctx.persist_record(record)
    }

    fn persist_records(&self, records: &[Record]) -> Result<()> {
        self.ctx.persist_records(records)
    }

    fn persist_snow_records(&self, records: &[SnowRecord]) -> Result<usize> {
        self.ctx.persist_snow_records(records)
    }

    fn project_runtime_document(&self, document: &VaultDocument) -> Result<()> {
        self.ctx.project_runtime_document(document)
    }

    fn persist_runtime_document(
        &self,
        document: &VaultDocument,
    ) -> Result<vault::rebuild::VaultDocumentEntry> {
        self.ctx.persist_runtime_document(document)
    }

    async fn load_runtime_document(
        &self,
        number: &str,
        resource_type: &ResourceType,
    ) -> Result<Option<VaultDocument>> {
        self.ctx.load_runtime_document(number, resource_type).await
    }

    fn persist_enrichment(&self, snow_record: &SnowRecord) -> Result<()> {
        self.ctx.persist_enrichment(snow_record)
    }

    async fn lookup_table_and_sys_id(&self, number: &str) -> Result<Option<(String, String)>> {
        self.ctx.lookup_table_and_sys_id(number).await
    }

    async fn resolve_resource_plan_parent_number(&self, number: &str) -> Result<String> {
        let table = resource_plan_parent_table_for_number(number).ok_or_else(|| {
            ResourcePlanListError::InvalidParams(
                "parent_number must start with DMND or PRJ".to_string(),
            )
        })?;
        let Some(record) = self
            .ctx
            .client
            .table(table)
            .equals("number", number)
            .first()
            .await?
        else {
            anyhow::bail!("resource_plan parent {number} was not found");
        };
        Ok(record.sys_id)
    }

    fn table_for_number(&self, number: &str) -> Option<String> {
        self.ctx.table_for_number(number)
    }

    async fn field_choices_for_table(&self, table: &str, field: &str) -> Result<Vec<FieldChoice>> {
        self.ctx.field_choices_for_table(table, field).await
    }

    async fn table_ancestors(&self, table: &str) -> Result<Vec<String>> {
        self.ctx.table_ancestors(table).await
    }

    /// The Business Application table and all of its inherited tables, most
    /// derived first. Used to scope `sys_dictionary` queries and dictionary
    /// cache lookups. Inheritance traversal is bounded to 8 levels by
    /// [`Self::table_ancestors`].
    async fn business_application_dictionary_tables(&self) -> Result<Vec<String>> {
        let mut tables = vec![BUSINESS_APPLICATION_TABLE.to_string()];
        tables.extend(self.table_ancestors(BUSINESS_APPLICATION_TABLE).await?);
        Ok(tables)
    }

    /// Fetch live `sys_dictionary` metadata for `cmdb_ci_business_app` and its
    /// inherited tables, then upsert the active rows into the
    /// `business_application_field_dictionary` cache.
    ///
    /// Returns the number of dictionary rows persisted. A failure to reach the
    /// dictionary (or an empty result) is surfaced as an error/zero so callers
    /// can stay in degraded-read mode; it must never abort a normal BA read.
    pub async fn refresh_business_application_dictionary(&self) -> Result<usize> {
        let tables = self.business_application_dictionary_tables().await?;
        let synced_at = Utc::now();
        let mut persisted = 0usize;
        for table in &tables {
            // One query per table keeps each `name=<table>` scoped and lets a
            // single failing table degrade independently of the others.
            let records = self
                .ctx
                .client
                .table("sys_dictionary")
                .equals("name", table)
                .equals("active", "true")
                .display_value(DisplayValue::Both)
                .exclude_reference_link(true)
                .limit(2000)
                .execute()
                .await?
                .records;
            for record in records {
                let Some(row) = dictionary_row_from_record(table, &record, synced_at) else {
                    continue;
                };
                self.ctx
                    .query
                    .store()
                    .upsert_business_application_field_dictionary(&row)?;
                persisted += 1;
            }
        }
        Ok(persisted)
    }

    /// Read the cached, dictionary-verified field metadata for the Business
    /// Application table and its ancestors, keyed by ServiceNow field name.
    ///
    /// Returns an empty map on a dictionary cache miss (degraded-read mode).
    pub async fn business_application_dictionary(
        &self,
    ) -> Result<HashMap<String, BusinessApplicationFieldDictionaryRow>> {
        let tables = self.business_application_dictionary_tables().await?;
        Ok(self
            .ctx
            .query
            .store()
            .business_application_dictionary_for_tables(&tables)?)
    }

    /// Build the typed alias map for the Business Application primitive,
    /// promoting baseline aliases to dictionary-verified fields when cached
    /// `sys_dictionary` metadata is present.
    ///
    /// On a dictionary cache miss this returns
    /// [`BusinessApplicationFieldAliases::baseline_degraded`], which carries a
    /// `DictionaryUnavailable` diagnostic so the degradation is never silent.
    pub async fn business_application_aliases(&self) -> Result<BusinessApplicationFieldAliases> {
        let dictionary = self.business_application_dictionary().await?;
        if dictionary.is_empty() {
            return Ok(BusinessApplicationFieldAliases::baseline_degraded());
        }
        Ok(business_application_aliases_from_dictionary(&dictionary))
    }

    /// Resolve the Business Application alias map for a hydration run, optionally
    /// refreshing the dictionary first.
    ///
    /// When `refresh_dictionary` is set, a best-effort live dictionary fetch runs
    /// before resolving so freshly verified instance field names take effect. A
    /// failure to refresh or an empty cache yields the degraded baseline aliases
    /// (carrying a `DictionaryUnavailable` diagnostic) so reads never fail.
    async fn resolve_business_application_aliases(
        &self,
        refresh_dictionary: bool,
    ) -> BusinessApplicationFieldAliases {
        if refresh_dictionary {
            let _ = self.refresh_business_application_dictionary().await;
        }
        self.business_application_aliases()
            .await
            .unwrap_or_else(|_| BusinessApplicationFieldAliases::baseline_degraded())
    }

    async fn resolve_user_sys_id(&self, user: &str) -> Result<String> {
        self.ctx.resolve_user_sys_id(user).await
    }

    async fn resolve_user_ref(&self, user: &str) -> Result<UserRef> {
        self.ctx.resolve_user_ref(user).await
    }

    async fn resolve_my_timecard_sheet(
        &self,
        week: WeekSelector,
        actor: &UserRef,
    ) -> Result<Record> {
        self.resolve_my_timecard_sheet_at(week, actor, chrono::Local::now().date_naive())
            .await
    }

    /// Resolve the user's time sheet for `week`, treating `today` as the
    /// current date (injected so tests are deterministic).
    ///
    /// Both selectors fetch the user's recent sheets and match the target date
    /// against each sheet's `[week_starts_on, week_starts_on + 7d)` range on the
    /// client. This is deliberately independent of the instance's first-day-of-
    /// week: a Monday-start sheet is matched just as well as a Sunday-start one.
    ///
    /// The previous `Current` implementation filtered server-side on
    /// `week_starts_on=javascript:gs.beginningOfThisWeek()`. That dynamic value
    /// is a Sunday-based GMT datetime, so on a Monday-start instance it never
    /// equaled the sheet's `week_starts_on` date and every current-week lookup
    /// failed with "no time sheet found" — even though the sheet existed.
    async fn resolve_my_timecard_sheet_at(
        &self,
        week: WeekSelector,
        actor: &UserRef,
        today: NaiveDate,
    ) -> Result<Record> {
        let (date, label) = match week {
            WeekSelector::Current => (today, "current week".to_string()),
            WeekSelector::Date(date) => (date, date.to_string()),
        };

        let rows = self
            .ctx
            .client
            .table(resource::timecard::TimecardResource::SHEET_TABLE)
            .equals("user", &actor.sys_id)
            .fields(TIME_SHEET_FIELDS)
            .display_value(DisplayValue::Both)
            .order_by("week_starts_on", Order::Desc)
            .limit(80)
            .execute()
            .await?
            .records;

        rows.into_iter()
            .find(|row| time_sheet_row_contains_date(row, date))
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "No time sheet found for {label}. This command edits existing cards only; create them in the portal first."
                )
            })
    }

    fn ensure_timecard_write_allowed(&self, card: &TimeCard, actor_sys_id: &str) -> Result<()> {
        if !card.is_owned_by(actor_sys_id) {
            return Err(anyhow::anyhow!(
                "time card {} belongs to user {}, not the authenticated user",
                card.sys_id,
                card.user.sys_id
            ));
        }
        if !card.is_editable() {
            return Err(anyhow::anyhow!(
                "time card sheet is {}; recall it in the portal to edit",
                card.state
            ));
        }
        Ok(())
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
            .ctx
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
                    .ctx
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

    fn infer_table(&self, number: &str) -> String {
        self.ctx.infer_table(number)
    }
}

fn is_open_user_work_record(record: &SnowRecord) -> bool {
    !is_terminal_state(Some(record.state.as_str())) && !record_field_is_false(record, "active")
}

fn servicenow_record_is_open_user_work(record: &Record) -> bool {
    !is_terminal_state(record.get_display("state").or(record.get_str("state")))
        && !servicenow_record_field_is_false(record, "active")
}

fn catalog_item_from_record(record: Record, variables: Vec<CatalogVariable>) -> CatalogItem {
    let name = record
        .get_str("name")
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .unwrap_or(&record.sys_id)
        .to_string();
    let short_description = record
        .get_str("short_description")
        .unwrap_or_default()
        .to_string();
    let table = record
        .get_raw("sys_class_name")
        .or_else(|| record.get_display("sys_class_name"))
        .unwrap_or("sc_cat_item")
        .to_string();
    CatalogItem {
        sys_id: record.sys_id,
        name,
        short_description,
        table,
        variables,
    }
}

fn catalog_variable_from_record(
    record: &Record,
    choices: &HashMap<String, Vec<CatalogChoice>>,
) -> CatalogVariable {
    let name = record_field_raw_or_display(record, "name").unwrap_or_else(|| record.sys_id.clone());
    let label = record
        .get_display("question_text")
        .or_else(|| record.get_raw("question_text"))
        .filter(|value| !value.trim().is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| name.clone());
    CatalogVariable {
        sys_id: record.sys_id.clone(),
        name,
        label,
        variable_type: record_field_display_or_raw(record, "type").unwrap_or_default(),
        mandatory: record_bool(record, "mandatory"),
        default_value: record_field_raw_or_display(record, "default_value")
            .filter(|value| !value.trim().is_empty()),
        reference_table: record_field_raw_or_display(record, "reference")
            .filter(|value| !value.trim().is_empty()),
        lookup_table: record_field_raw_or_display(record, "lookup_table")
            .or_else(|| record_field_raw_or_display(record, "list_table"))
            .filter(|value| !value.trim().is_empty()),
        max_length: record_field_raw_or_display(record, "max_length")
            .and_then(|value| value.parse().ok()),
        choices: choices.get(&record.sys_id).cloned().unwrap_or_default(),
    }
}

fn record_field_raw_or_display(record: &Record, field: &str) -> Option<String> {
    record
        .get_raw(field)
        .or_else(|| record.get_display(field))
        .or_else(|| record.get_str(field))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn record_field_display_or_raw(record: &Record, field: &str) -> Option<String> {
    record
        .get_display(field)
        .or_else(|| record.get_raw(field))
        .or_else(|| record.get_str(field))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn record_bool(record: &Record, field: &str) -> bool {
    match record_field_raw_or_display(record, field)
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "true" | "1" | "yes" | "y" => true,
        "false" | "0" | "no" | "n" | "" => false,
        _ => false,
    }
}

fn catalog_submit_result_from_response(
    item_sys_id: String,
    raw_result: Value,
    base_url: &str,
    portal: &str,
) -> CatalogSubmitResult {
    let request_item = first_result_item(&raw_result);
    let request_item_sys_id = request_item.and_then(|item| string_at(item, &["sys_id"]));
    let request_item_number = request_item.and_then(|item| string_at(item, &["number"]));

    let request_sys_id = string_at(&raw_result, &["request_id"])
        .or_else(|| string_at(&raw_result, &["request", "sys_id"]))
        .or_else(|| {
            if table_at(&raw_result, &["table"]).as_deref() == Some("sc_request") {
                string_at(&raw_result, &["sys_id"])
            } else {
                None
            }
        });
    let request_number = string_at(&raw_result, &["request_number"])
        .or_else(|| string_at(&raw_result, &["request", "number"]))
        .or_else(|| {
            string_at(&raw_result, &["number"])
                .filter(|number| number.to_ascii_uppercase().starts_with("REQ"))
        });

    let result_table = request_item_sys_id
        .as_ref()
        .map(|_| "sc_req_item".to_string())
        .or_else(|| table_at(&raw_result, &["table"]))
        .or_else(|| table_at(&raw_result, &["request", "table"]));
    let result_sys_id = request_item_sys_id
        .clone()
        .or_else(|| string_at(&raw_result, &["sys_id"]))
        .or_else(|| request_sys_id.clone());
    let result_number = request_item_number
        .clone()
        .or_else(|| string_at(&raw_result, &["number"]))
        .or_else(|| request_number.clone());
    let browser_url = result_table
        .as_ref()
        .zip(result_sys_id.as_ref())
        .map(|(table, sys_id)| catalog_browser_url(base_url, portal, table, sys_id));

    CatalogSubmitResult {
        item_sys_id,
        table: result_table,
        sys_id: result_sys_id,
        number: result_number,
        request_table: request_sys_id.as_ref().map(|_| "sc_request".to_string()),
        request_sys_id,
        request_number,
        request_item_sys_id,
        request_item_number,
        browser_url,
        raw_result,
    }
}

fn catalog_browser_url(base_url: &str, portal: &str, table: &str, sys_id: &str) -> String {
    // The portal slug is instance-specific; fall back to the out-of-box `sp`
    // Service Portal when none is configured so the URL stays valid.
    let portal = portal.trim();
    let portal = if portal.is_empty() { "sp" } else { portal };
    format!(
        "{}/{portal}?id=ticket&table={table}&sys_id={sys_id}&view=sp",
        base_url.trim_end_matches('/')
    )
}

fn first_result_item(value: &Value) -> Option<&Value> {
    ["items", "request_items"]
        .into_iter()
        .find_map(|field| value.get(field).and_then(Value::as_array))
        .and_then(|items| items.first())
        .or_else(|| value.get("request_item"))
}

fn string_at(value: &Value, path: &[&str]) -> Option<String> {
    let mut cursor = value;
    for segment in path {
        cursor = cursor.get(*segment)?;
    }
    cursor
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn table_at(value: &Value, path: &[&str]) -> Option<String> {
    string_at(value, path).map(|table| canonical_record_table(&table))
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
        let store = cache.store().clone();
        let cache_policy = CacheTtlPolicy::from_ttl_strings(
            &config.cache.policy.stable_reference_ttl,
            &config.cache.policy.work_record_ttl,
        )?;

        let ctx = context::CoreContext {
            client,
            store,
            query,
            cache,
            cache_policy,
            vault,
            vault_path,
            config: Arc::new(config),
        };
        let approvals = service::ApprovalService::new(ctx.clone());
        let users = service::UserService::new(ctx.clone());
        Ok(SnowCore {
            ctx,
            approvals,
            users,
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
    use wiremock::matchers::{body_partial_json, method, path, query_param, query_param_contains};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    // pub(crate): also called from `service::approval`'s test module (Task 9),
    // which moved its approve/reject/my_approvals tests out of this file. This
    // fn stays here (not duplicated) because it also serializes dozens of
    // non-approval tests throughout this module that mutate shared state.
    pub(crate) async fn mock_server_test_lock() -> tokio::sync::MutexGuard<'static, ()> {
        static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
            .lock()
            .await
    }

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

    #[test]
    fn builtin_prefixes_include_demand_task_numbers() {
        assert_eq!(
            table_for_builtin_record_number("DMNTSK0001122"),
            Some("dmn_demand_task")
        );
        assert_eq!(
            table_for_builtin_record_number("dmntsk0001122"),
            Some("dmn_demand_task")
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

    // pub(crate): also called from `service::user`'s test module, which moved
    // its user-search test out of this file in Task 8. This fn stays here
    // (not duplicated) because it is shared fixture setup used by dozens of
    // non-user tests throughout this module.
    pub(crate) async fn core_for_mock_server(server: &MockServer) -> (SnowCore, TempDir) {
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

    // pub(crate): also called from `service::approval`'s test module (Task 9),
    // which moved its approve/reject/my_approvals tests out of this file. This
    // fn stays here (not duplicated) because it is shared fixture setup used
    // by other non-approval tests throughout this module.
    pub(crate) async fn core_for_mock_server_with_user(
        server: &MockServer,
        user: &str,
    ) -> (SnowCore, TempDir) {
        let client = ServiceNowClient::builder()
            .instance(server.uri())
            .auth(BasicAuth::new("test_user", "test_pass"))
            .allow_http()
            .build()
            .await
            .expect("client");

        let mut config = config::SnowConfig::default();
        config.instance.user = user.to_string();

        let tempdir = TempDir::new().expect("tempdir");
        let core = SnowCore::builder()
            .config(config)
            .client(client)
            .vault_path(tempdir.path().join("vault"))
            .build()
            .await
            .expect("core");

        (core, tempdir)
    }

    async fn mount_number_lookup(server: &MockServer, table: &str, number: &str, sys_id: &str) {
        Mock::given(method("GET"))
            .and(path(format!("/api/now/table/{table}")))
            .and(query_param("sysparm_query", format!("number={number}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": sys_id,
                    "number": number,
                    "short_description": "Attachment target",
                    "state": "Open"
                }]
            })))
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn business_application_search_builds_supported_filters() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "54a4b61b6fe845000ed852a03f3ee4d0",
                    "name": "Epic",
                    "short_description": "Epic",
                    "sys_class_name": "cmdb_ci_business_app",
                    "business_owner": { "value": "owner-sys", "display_value": "Jane Owner" },
                    "it_application_owner": { "value": "is-owner-sys", "display_value": "Alex IS" },
                    "managed_by_group": { "value": "ci-group-sys", "display_value": "CI Owners" },
                    "support_group": { "value": "support-group-sys", "display_value": "App Support" },
                    "operational_status": { "value": "1", "display_value": "Operational" },
                    "portfolio": { "value": "portfolio-sys", "display_value": "Clinical" },
                    "attested_date": "2026-05-01",
                    "u_custom_field": { "value": "raw-custom", "display_value": "Custom Display" },
                    "u_json_blob": { "value": { "kept": true }, "display_value": "Kept" }
                }]
            })))
            .mount(&server)
            .await;

        let (core, tempdir) = core_for_mock_server(&server).await;
        let records = core
            .search_business_applications(BusinessApplicationSearchParams {
                name: Some("Epic".to_string()),
                business_owner: Some("Jane Owner".to_string()),
                is_owner: Some("Alex IS".to_string()),
                ci_owner_group: Some("CI Owners".to_string()),
                primary_support_group: Some("App Support".to_string()),
                operational_state_not: Some("non-operational".to_string()),
                primary_portfolio: Some("Clinical".to_string()),
                attested_date: Some("2026-05-01".to_string()),
                limit: Some(2),
                ..Default::default()
            })
            .await
            .expect("business applications");

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].number, "BA:54a4b61b6fe845000ed852a03f3ee4d0");
        assert_eq!(records[0].table, "cmdb_ci_business_app");
        assert_eq!(records[0].resource_type, ResourceType::BusinessApplication);
        assert_eq!(records[0].fields["name"].value, "Epic");
        assert_eq!(records[0].fields["u_custom_field"].value, "raw-custom");
        assert_eq!(records[0].fields["u_json_blob"].value, "{\"kept\":true}");
        assert!(
            tempdir
                .path()
                .join("vault/business_applications/business_application_54a4b61b6fe845000ed852a03f3ee4d0_epic.md")
                .exists()
        );

        let requests = server.received_requests().await.expect("requests");
        let request = requests
            .iter()
            .find(|request| request.url.path() == "/api/now/table/cmdb_ci_business_app")
            .expect("business app request");
        let query = request
            .url
            .query_pairs()
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(
            query.get("sysparm_query").map(|value| value.as_ref()),
            Some(
                "sys_class_name=cmdb_ci_business_app^nameLIKEEpic^business_owner.nameLIKEJane Owner^it_application_owner.nameLIKEAlex IS^managed_by_group.nameLIKECI Owners^support_group.nameLIKEApp Support^portfolio.nameLIKEClinical^operational_status!=2^attested_date=2026-05-01^ORDERBYname"
            )
        );
        assert_eq!(
            query.get("sysparm_fields").map(|value| value.as_ref()),
            None
        );
        assert_eq!(
            query
                .get("sysparm_display_value")
                .map(|value| value.as_ref()),
            Some("all")
        );
        assert_eq!(
            query
                .get("sysparm_exclude_reference_link")
                .map(|value| value.as_ref()),
            Some("true")
        );
        assert_eq!(
            query.get("sysparm_limit").map(|value| value.as_ref()),
            Some("2")
        );
    }

    #[tokio::test]
    async fn server_search_builds_supported_filters() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "name": "app01.example.internal",
                    "ip_address": "192.0.2.10",
                    "sys_class_name": "cmdb_ci_linux_server",
                    "managed_by_group": {
                        "value": "11111111111111111111111111111111",
                        "display_value": "Platform Operations"
                    },
                    "support_group": {
                        "value": "22222222222222222222222222222222",
                        "display_value": "Server Support"
                    },
                    "operational_status": { "value": "1", "display_value": "Operational" },
                    "short_description": "Linux application server"
                }]
            })))
            .mount(&server)
            .await;

        let (core, tempdir) = core_for_mock_server(&server).await;
        let records = core
            .search_servers(ServerSearchParams {
                name: Some("app01".to_string()),
                ip_address: Some("192.0.2.10".to_string()),
                ci_owner_group: Some("Platform Operations".to_string()),
                class: Some("linux".to_string()),
                limit: Some(3),
            })
            .await
            .expect("servers");

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].number, "SERVER:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert_eq!(records[0].table, "cmdb_ci_server");
        assert_eq!(records[0].resource_type, ResourceType::Server);
        assert_eq!(records[0].fields["name"].value, "app01.example.internal");
        assert_eq!(records[0].fields["ip_address"].value, "192.0.2.10");
        assert_eq!(
            records[0].references["managed_by_group"].display_name,
            "Platform Operations"
        );
        let cached = core
            .query_servers(ServerQuery {
                ci_owner_group: Some("Platform Operations".to_string()),
                ..Default::default()
            })
            .await
            .expect("cached server query by CI owner group");
        assert_eq!(cached.len(), 1);
        assert_eq!(cached[0].sys_id, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert!(
            tempdir
                .path()
                .join("vault/servers/server_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_app01-example-internal.md")
                .exists()
        );

        let requests = server.received_requests().await.expect("requests");
        let request = requests
            .iter()
            .find(|request| request.url.path() == "/api/now/table/cmdb_ci_server")
            .expect("server request");
        let query = request
            .url
            .query_pairs()
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(
            query.get("sysparm_query").map(|value| value.as_ref()),
            Some(
                "sys_class_name=cmdb_ci_linux_server^nameLIKEapp01^ip_address=192.0.2.10^managed_by_group.nameLIKEPlatform Operations^ORDERBYname"
            )
        );
        assert_eq!(
            query.get("sysparm_fields").map(|value| value.as_ref()),
            None
        );
        assert_eq!(
            query
                .get("sysparm_display_value")
                .map(|value| value.as_ref()),
            Some("all")
        );
        assert_eq!(
            query
                .get("sysparm_exclude_reference_link")
                .map(|value| value.as_ref()),
            Some("true")
        );
        assert_eq!(
            query.get("sysparm_limit").map(|value| value.as_ref()),
            Some("3")
        );
    }

    // ----- server_get read-through (live fallback) -----
    //
    // These exercise the shared live path behind the read-through `server_get`
    // (`SnowCore::get_server_live`). All fixtures use RFC-5737 documentation IPs
    // and placeholder sys_ids/hostnames only.

    fn server_result_body(sys_id: &str, name: &str, ip: &str) -> serde_json::Value {
        server_result_body_with_class(sys_id, name, ip, "cmdb_ci_linux_server")
    }

    fn server_result_body_with_class(
        sys_id: &str,
        name: &str,
        ip: &str,
        class_name: &str,
    ) -> serde_json::Value {
        serde_json::json!({
            "result": [{
                "sys_id": sys_id,
                "name": name,
                "ip_address": ip,
                "sys_class_name": class_name,
                "operational_status": { "value": "1", "display_value": "Operational" }
            }]
        })
    }

    #[tokio::test]
    async fn server_get_live_by_name_cache_miss_hit_persists_and_caches() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let sys_id = "cccccccccccccccccccccccccccccccc";
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param("sysparm_limit", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(server_result_body(
                sys_id,
                "host01.example.internal",
                "192.0.2.20",
            )))
            .expect(1)
            .mount(&server)
            .await;

        let (core, tempdir) = core_for_mock_server(&server).await;
        let found = core
            .get_server_live(ServerLookup::exact_name("host01.example.internal"), true)
            .await
            .expect("live hit");
        let found = found.expect("some server");
        assert_eq!(found.record.sys_id, sys_id);

        // Persisted to cache: a subsequent cached query resolves it locally.
        let cached = core
            .query_servers(ServerQuery {
                name: Some("host01".to_string()),
                ..Default::default()
            })
            .await
            .expect("cached query");
        assert_eq!(cached.len(), 1);
        assert_eq!(cached[0].sys_id, sys_id);
        assert!(
            tempdir
                .path()
                .join("vault/servers")
                .read_dir()
                .map(|mut d| d.next().is_some())
                .unwrap_or(false)
        );
    }

    #[tokio::test]
    async fn server_get_live_by_name_queries_base_table_without_leaf_class_filter() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let sys_id = "cacacacacacacacacacacacacacacaca";
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param("sysparm_limit", "2"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(server_result_body_with_class(
                    sys_id,
                    "esx01.example.internal",
                    "192.0.2.24",
                    "cmdb_ci_esx_server",
                )),
            )
            .expect(1)
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let found = core
            .get_server_live(ServerLookup::exact_name("esx01.example.internal"), false)
            .await
            .expect("live hit")
            .expect("some server");
        assert_eq!(found.record.sys_id, sys_id);
        assert_eq!(found.class_name.as_deref(), Some("cmdb_ci_esx_server"));

        let requests = server.received_requests().await.expect("requests");
        let request = requests
            .iter()
            .find(|request| request.url.path() == "/api/now/table/cmdb_ci_server")
            .expect("server request");
        let query = request
            .url
            .query_pairs()
            .collect::<std::collections::HashMap<_, _>>();
        let sysparm_query = query
            .get("sysparm_query")
            .map(|value| value.as_ref())
            .expect("sysparm_query");
        assert_eq!(sysparm_query, "name=esx01.example.internal^ORDERBYname");
        assert!(!sysparm_query.contains("sys_class_name"));
    }

    #[tokio::test]
    async fn server_get_live_by_sys_id_cache_miss_hit() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let sys_id = "dddddddddddddddddddddddddddddddd";
        Mock::given(method("GET"))
            .and(path(format!("/api/now/table/cmdb_ci_server/{sys_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": {
                    "sys_id": sys_id,
                    "name": "host02.example.internal",
                    "ip_address": "192.0.2.21",
                    "sys_class_name": "cmdb_ci_linux_server",
                    "operational_status": { "value": "1", "display_value": "Operational" }
                }
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let found = core
            .get_server_live(ServerLookup::sys_id(sys_id).expect("sys_id"), true)
            .await
            .expect("live hit")
            .expect("some server");
        assert_eq!(found.record.sys_id, sys_id);
    }

    #[tokio::test]
    async fn server_get_live_by_ip_cache_miss_hit() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let sys_id = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param("sysparm_limit", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(server_result_body(
                sys_id,
                "host03.example.internal",
                "192.0.2.22",
            )))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let found = core
            .get_server_live(ServerLookup::ip_address("192.0.2.22"), true)
            .await
            .expect("live hit")
            .expect("some server");
        assert_eq!(found.record.sys_id, sys_id);
    }

    #[tokio::test]
    async fn server_get_live_by_name_empty_result_is_not_found() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "result": [] })),
            )
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let err = core
            .get_server_live(ServerLookup::exact_name("ghost.example.internal"), true)
            .await
            .expect_err("not found");
        assert_eq!(err, ServerGetError::NotFound);
    }

    #[tokio::test]
    async fn server_get_live_by_sys_id_404_is_not_found() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let sys_id = "ffffffffffffffffffffffffffffffff";
        Mock::given(method("GET"))
            .and(path(format!("/api/now/table/cmdb_ci_server/{sys_id}")))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "error": { "message": "No record found" }
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let err = core
            .get_server_live(ServerLookup::sys_id(sys_id).expect("sys_id"), true)
            .await
            .expect_err("not found");
        assert_eq!(err, ServerGetError::NotFound);
    }

    #[tokio::test]
    async fn server_get_live_acl_403_is_acl_restricted_not_not_found() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
                "error": { "message": "Insufficient rights" }
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let err = core
            .get_server_live(ServerLookup::exact_name("locked.example.internal"), true)
            .await
            .expect_err("acl");
        assert!(
            matches!(err, ServerGetError::AclRestricted(_)),
            "expected AclRestricted, got {err:?}"
        );
    }

    #[tokio::test]
    async fn server_get_live_network_failure_is_network_not_not_found() {
        let _guard = mock_server_test_lock().await;
        // Point at an address that refuses connections (127.0.0.1:1 is the
        // reserved tcpmux port, effectively always closed for our purposes) so
        // the transport call fails at the connection layer -> network error,
        // never a not-found.
        let client = ServiceNowClient::builder()
            .instance("http://127.0.0.1:1")
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

        let err = core
            .get_server_live(ServerLookup::exact_name("host04.example.internal"), true)
            .await
            .expect_err("network");
        assert!(
            matches!(err, ServerGetError::Network(_)),
            "expected Network, got {err:?}"
        );
    }

    #[tokio::test]
    async fn server_get_live_duplicate_name_is_disambiguation() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    {
                        "sys_id": "11111111111111111111111111111111",
                        "name": "dup.example.internal",
                        "sys_class_name": "cmdb_ci_linux_server"
                    },
                    {
                        "sys_id": "22222222222222222222222222222222",
                        "name": "dup.example.internal",
                        "sys_class_name": "cmdb_ci_win_server"
                    }
                ]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let err = core
            .get_server_live(ServerLookup::exact_name("dup.example.internal"), true)
            .await
            .expect_err("disambiguation");
        match err {
            ServerGetError::Disambiguation { selector, matched } => {
                assert_eq!(selector, "name=dup.example.internal");
                assert_eq!(matched, 2);
            }
            other => panic!("expected Disambiguation, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn server_get_live_no_persist_does_not_write_cache() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let sys_id = "99999999999999999999999999999999";
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .respond_with(ResponseTemplate::new(200).set_body_json(server_result_body(
                sys_id,
                "host05.example.internal",
                "192.0.2.30",
            )))
            .mount(&server)
            .await;

        let (core, tempdir) = core_for_mock_server(&server).await;
        let found = core
            .get_server_live(ServerLookup::exact_name("host05.example.internal"), false)
            .await
            .expect("live hit")
            .expect("some server");
        assert_eq!(found.record.sys_id, sys_id);

        // persist = false: nothing written to the local cache.
        let servers_dir = tempdir.path().join("vault/servers");
        let has_entries = servers_dir
            .read_dir()
            .map(|mut d| d.next().is_some())
            .unwrap_or(false);
        assert!(!has_entries, "MCP no-persist path must not write the cache");
        let cached = core
            .query_servers(ServerQuery {
                name: Some("host05".to_string()),
                ..Default::default()
            })
            .await
            .expect("cached query");
        assert!(cached.is_empty(), "no-persist record must not be cached");
    }

    #[tokio::test]
    async fn business_application_search_persists_resolved_reference_primitives() {
        let server = MockServer::start().await;
        let owner_sys_id = "6816f79cc0a8016401c5a33be04be441";
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "54a4b61b6fe845000ed852a03f3ee4d0",
                    "name": "Epic",
                    "sys_class_name": "cmdb_ci_business_app",
                    "business_owner": {
                        "value": owner_sys_id,
                        "display_value": "Jane Owner"
                    },
                    "operational_status": { "value": "1", "display_value": "Operational" }
                }]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/api/now/table/sys_user/{owner_sys_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": {
                    "sys_id": owner_sys_id,
                    "name": "Jane Owner",
                    "user_name": "jowner",
                    "email": "jane.owner@example.invalid",
                    "active": { "value": "true", "display_value": "true" },
                    "sys_updated_on": "2026-05-30 12:00:00"
                }
            })))
            .mount(&server)
            .await;

        let (core, tempdir) = core_for_mock_server(&server).await;
        let applications = core
            .search_business_applications_live(
                BusinessApplicationSearchParams {
                    name: Some("Epic".to_string()),
                    limit: Some(1),
                    ..Default::default()
                },
                BusinessApplicationHydrationOptions::default(),
            )
            .await
            .expect("business applications");

        assert_eq!(applications.len(), 1);
        assert!(
            applications[0]
                .unresolved_references
                .iter()
                .all(|diagnostic| { diagnostic.reference_sys_id != owner_sys_id })
        );
        let primitive = core
            .ctx
            .query
            .store()
            .get_primitive_object(owner_sys_id)
            .expect("primitive lookup")
            .expect("primitive object");
        assert_eq!(primitive.table_name, "sys_user");
        assert_eq!(primitive.resource_type, "user_primitive");
        assert_eq!(primitive.display_name, "Jane Owner");
        assert_eq!(
            primitive.resolution_status,
            PrimitiveResolutionStatus::Resolved
        );
        let file_path = primitive.file_path.expect("primitive vault path");
        assert!(file_path.starts_with("users/user_6816f79cc0a8016401c5a33be04be441_jane-owner.md"));
        assert!(tempdir.path().join("vault").join(file_path).exists());
    }

    #[tokio::test]
    async fn business_application_fresh_get_omits_sysparm_fields() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(
                "/api/now/table/cmdb_ci_business_app/54a4b61b6fe845000ed852a03f3ee4d0",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": {
                    "sys_id": "54a4b61b6fe845000ed852a03f3ee4d0",
                    "name": "Epic",
                    "sys_class_name": "cmdb_ci_business_app",
                    "operational_status": { "value": "1", "display_value": "Operational" },
                    "u_observed": "yes"
                }
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let business_application = core
            .get_business_application_fresh(
                BusinessApplicationLookup::sys_id("54A4B61B6FE845000ED852A03F3EE4D0").unwrap(),
                BusinessApplicationHydrationOptions::default(),
            )
            .await
            .expect("fresh get")
            .expect("business application");

        assert_eq!(business_application.name, "Epic");
        assert_eq!(
            business_application.record.number,
            "BA:54a4b61b6fe845000ed852a03f3ee4d0"
        );
        assert_eq!(
            business_application.fields["u_observed"].value,
            serde_json::json!("yes")
        );

        let requests = server.received_requests().await.expect("requests");
        let request = requests
            .iter()
            .find(|request| {
                request.url.path()
                    == "/api/now/table/cmdb_ci_business_app/54a4b61b6fe845000ed852a03f3ee4d0"
            })
            .expect("business app get request");
        let query = request
            .url
            .query_pairs()
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(query.get("sysparm_fields"), None);
        assert_eq!(
            query
                .get("sysparm_display_value")
                .map(|value| value.as_ref()),
            Some("all")
        );
        assert_eq!(
            query
                .get("sysparm_exclude_reference_link")
                .map(|value| value.as_ref()),
            Some("true")
        );
    }

    #[test]
    fn business_application_servers_params_validate_selector_and_bounds() {
        let options = BusinessApplicationServersParams {
            number: Some("apm0000001".to_string()),
            ..Default::default()
        }
        .validate()
        .expect("valid number selector");
        assert_eq!(
            options.selector,
            BusinessApplicationServersSelector::Number("APM0000001".to_string())
        );
        assert_eq!(options.max_depth, 2);
        assert_eq!(options.max_cis, 500);
        assert_eq!(options.max_edges, 2000);

        let err = BusinessApplicationServersParams::default()
            .validate()
            .expect_err("missing selector should fail")
            .to_string();
        assert!(err.contains("exactly one"));

        let err = BusinessApplicationServersParams {
            number: Some("APM0000001".to_string()),
            sys_id: Some("11111111111111111111111111111111".to_string()),
            ..Default::default()
        }
        .validate()
        .expect_err("dual selector should fail")
        .to_string();
        assert!(err.contains("exactly one"));

        let err = BusinessApplicationServersParams {
            number: Some("BA:11111111111111111111111111111111".to_string()),
            ..Default::default()
        }
        .validate()
        .expect_err("synthetic BA number should fail")
        .to_string();
        assert!(err.contains("BA:<sys_id>"));

        let err = BusinessApplicationServersParams {
            number: Some("APP0000001".to_string()),
            ..Default::default()
        }
        .validate()
        .expect_err("non-APM number should fail")
        .to_string();
        assert!(err.contains("<APM_NUMBER>"));

        let err = BusinessApplicationServersParams {
            sys_id: Some("11111111111111111111111111111111".to_string()),
            max_depth: Some(5),
            ..Default::default()
        }
        .validate()
        .expect_err("max depth should be bounded")
        .to_string();
        assert!(err.contains("at most 4"));
    }

    #[test]
    fn business_application_server_path_chains_reject_mixed_directions() {
        let root = "11111111111111111111111111111111";
        let middle = "22222222222222222222222222222222";
        let leaf = "33333333333333333333333333333333";
        let rel_type = BusinessApplicationRelationshipType {
            value: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
            display_value: Some("Depends on::Used by".to_string()),
        };
        let mut paths_by_ci: HashMap<String, Vec<Vec<BusinessApplicationServerPathEdge>>> =
            HashMap::from([(root.to_string(), vec![Vec::new()])]);

        assert!(extend_path_chains(
            &mut paths_by_ci,
            root,
            middle,
            BusinessApplicationServerPathEdge {
                depth: 1,
                parent_sys_id: root.to_string(),
                child_sys_id: middle.to_string(),
                direction: BusinessApplicationRelationshipDirection::ParentToChild,
                relationship_type: rel_type.clone(),
                edge_source: BusinessApplicationServerPathEdgeSource::Relationship,
            },
        ));

        assert!(!extend_path_chains(
            &mut paths_by_ci,
            middle,
            leaf,
            BusinessApplicationServerPathEdge {
                depth: 2,
                parent_sys_id: leaf.to_string(),
                child_sys_id: middle.to_string(),
                direction: BusinessApplicationRelationshipDirection::ChildToParent,
                relationship_type: rel_type,
                edge_source: BusinessApplicationServerPathEdgeSource::Relationship,
            },
        ));
        assert!(!paths_by_ci.contains_key(leaf));
    }

    #[tokio::test]
    async fn business_application_servers_batches_bfs_levels_and_hydrates_servers() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let app = "11111111111111111111111111111111";
        let service = "22222222222222222222222222222222";
        let linux = "33333333333333333333333333333333";
        let windows = "44444444444444444444444444444444";
        let component = "55555555555555555555555555555555";
        let rel_type = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let rel_row_1 = "99999999999999999999999999999991";
        let rel_row_2 = "99999999999999999999999999999992";
        let rel_row_3 = "99999999999999999999999999999993";
        let rel_row_4 = "99999999999999999999999999999994";

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .and(query_param(
                "sysparm_query",
                "sys_class_name=cmdb_ci_business_app^number=APM0000001",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": app,
                    "number": "APM0000001",
                    "name": "Application Alpha",
                    "sys_class_name": "cmdb_ci_business_app",
                    "operational_status": { "value": "1", "display_value": "Operational" }
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("parentIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row(rel_row_1, app, service, rel_type, "cmdb_ci_business_app", "cmdb_ci_service"),
                    relationship_row(rel_row_2, app, component, rel_type, "cmdb_ci_business_app", "cmdb_ci_appl"),
                    relationship_row(rel_row_3, app, linux, rel_type, "cmdb_ci_business_app", "cmdb_ci_linux_server")
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("childIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param("sysparm_query", format!("sys_idIN{linux}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [server_row(linux, "linux-alpha.example.com", "cmdb_ci_linux_server")]
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param(
                "sysparm_query",
                format!("parentIN{service},{component}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row(rel_row_4, component, windows, rel_type, "cmdb_ci_appl", "cmdb_ci_win_server")
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param(
                "sysparm_query",
                format!("childIN{service},{component}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param("sysparm_query", format!("sys_idIN{windows}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [server_row(windows, "windows-alpha.example.com", "cmdb_ci_win_server")]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                include_paths: true,
                persist: Some(true),
                ..Default::default()
            })
            .await
            .expect("business application servers")
            .expect("business application present");

        assert_eq!(result.business_application.sys_id, app);
        assert_eq!(result.servers.len(), 2);
        assert_eq!(result.relationship_summary.relationships_examined, 4);
        assert_eq!(result.relationship_summary.cis_examined, 5);
        assert_eq!(result.relationship_summary.servers_found, 2);
        assert_eq!(result.relationship_summary.persisted_servers, 2);
        assert_eq!(result.relationship_summary.membership_upserts, 2);
        assert_eq!(result.relationship_summary.membership_pruned, 0);
        assert!(!result.relationship_summary.truncated);
        assert_eq!(result.server_paths[linux][0].edges.len(), 1);
        assert_eq!(result.server_paths[windows][0].edges.len(), 2);
        assert_eq!(
            result.server_paths[linux][0].edges[0].direction,
            BusinessApplicationRelationshipDirection::ParentToChild
        );

        let serialized = serde_json::to_string(&result).expect("serialize result");
        assert!(!serialized.contains(rel_row_1));
        assert!(!serialized.contains(rel_row_2));
        assert!(!serialized.contains(rel_row_3));
        assert!(!serialized.contains(rel_row_4));

        let cached_forward = core
            .business_application_servers_cached(BusinessApplicationServersCachedParams {
                sys_id: Some(app.to_string()),
                ..Default::default()
            })
            .await
            .expect("cached forward lookup")
            .expect("business application cached");
        assert_eq!(cached_forward.business_application.sys_id, app);
        assert_eq!(cached_forward.servers.len(), 2);
        assert_eq!(cached_forward.servers[0].server.sys_id, linux);
        assert_eq!(cached_forward.servers[0].provenance, "relationship");
        assert_eq!(cached_forward.servers[0].min_depth, 1);
        assert_eq!(cached_forward.servers[0].paths[0].depth(), 1);
        assert_eq!(cached_forward.servers[1].server.sys_id, windows);
        assert_eq!(cached_forward.servers[1].min_depth, 2);
        assert_eq!(cached_forward.servers[1].paths[0].depth(), 2);
        assert_eq!(
            cached_forward.relationship_status,
            RelationshipKnowledgeStatus::KnownRelationships
        );
        let forward_health = cached_forward
            .inventory_health
            .as_ref()
            .expect("forward inventory health");
        assert_eq!(forward_health.ba_sys_id, app);
        assert_eq!(forward_health.service_membership_status, "not_attempted");
        assert_eq!(forward_health.relationship_status, "ok");
        assert_eq!(forward_health.inventory_status, "complete");

        let cached_reverse = core
            .business_applications_for_server(BusinessApplicationsForServerParams {
                name: Some("linux-alpha.example.com".to_string()),
                ..Default::default()
            })
            .await
            .expect("cached reverse lookup")
            .expect("server cached");
        assert_eq!(cached_reverse.servers.len(), 1);
        assert_eq!(cached_reverse.servers[0].server.sys_id, linux);
        assert_eq!(cached_reverse.servers[0].business_applications.len(), 1);
        assert_eq!(
            cached_reverse.servers[0].business_applications[0]
                .business_application
                .sys_id,
            app
        );
        assert_eq!(
            cached_reverse.servers[0].relationship_status,
            RelationshipKnowledgeStatus::KnownRelationships
        );
        assert_eq!(
            cached_reverse.servers[0].business_applications[0]
                .inventory_health
                .as_ref()
                .expect("reverse inventory health")
                .inventory_status,
            "complete"
        );

        let requests = server.received_requests().await.expect("requests");
        let relationship_queries = requests
            .iter()
            .filter(|request| request.url.path() == "/api/now/table/cmdb_rel_ci")
            .filter_map(|request| {
                request
                    .url
                    .query_pairs()
                    .find(|(key, _)| key == "sysparm_query")
                    .map(|(_, value)| value.to_string())
            })
            .collect::<Vec<_>>();
        assert_eq!(
            relationship_queries,
            vec![
                format!("parentIN{app}"),
                format!("childIN{app}"),
                format!("parentIN{service},{component}"),
                format!("childIN{service},{component}"),
            ]
        );
    }

    /// Fix #3 regression guard: in a diamond topology (`app -> A -> S` and
    /// `app -> B -> S`) the server `S` is reachable via two distinct parents.
    /// With `include_paths`, BOTH routes must be recorded (the plural
    /// `server_paths` Vec holds two entries), while `S` remains a single server
    /// result. The second discovery is an alternate forward path, not a cycle.
    #[tokio::test]
    async fn business_application_servers_records_diamond_alternate_paths() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let app = "11111111111111111111111111111111";
        let branch_a = "22222222222222222222222222222222";
        let branch_b = "33333333333333333333333333333333";
        let leaf = "44444444444444444444444444444444";
        let rel_type = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .and(query_param(
                "sysparm_query",
                "sys_class_name=cmdb_ci_business_app^number=APM0000001",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": app,
                    "number": "APM0000001",
                    "name": "Application Alpha",
                    "sys_class_name": "cmdb_ci_business_app"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        // Depth 1: app -> A and app -> B (two intermediate application CIs).
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("parentIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row("99999999999999999999999999999991", app, branch_a, rel_type, "cmdb_ci_business_app", "cmdb_ci_appl"),
                    relationship_row("99999999999999999999999999999992", app, branch_b, rel_type, "cmdb_ci_business_app", "cmdb_ci_appl")
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("childIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .expect(1)
            .mount(&server)
            .await;

        // Depth 2: both A and B point at the SAME leaf server S.
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param(
                "sysparm_query",
                format!("parentIN{branch_a},{branch_b}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row("99999999999999999999999999999993", branch_a, leaf, rel_type, "cmdb_ci_appl", "cmdb_ci_linux_server"),
                    relationship_row("99999999999999999999999999999994", branch_b, leaf, rel_type, "cmdb_ci_appl", "cmdb_ci_linux_server")
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param(
                "sysparm_query",
                format!("childIN{branch_a},{branch_b}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param("sysparm_query", format!("sys_idIN{leaf}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [server_row(leaf, "linux-leaf.example.com", "cmdb_ci_linux_server")]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                include_paths: true,
                ..Default::default()
            })
            .await
            .expect("business application servers")
            .expect("business application present");

        // One server, two distinct routes to it.
        assert_eq!(result.servers.len(), 1);
        assert_eq!(result.servers[0].record.sys_id, leaf);
        let paths = result
            .server_paths
            .get(leaf)
            .expect("leaf should have recorded paths");
        assert_eq!(paths.len(), 2, "both diamond routes must be recorded");
        // Each route is two edges long: app->branch then branch->leaf.
        assert!(paths.iter().all(|route| route.edges.len() == 2));
        // The two routes go through different branch CIs.
        let mut branches = paths
            .iter()
            .map(|route| route.edges[0].to_sys_id().to_string())
            .collect::<Vec<_>>();
        branches.sort();
        assert_eq!(branches, vec![branch_a.to_string(), branch_b.to_string()]);
    }

    /// Task #10: the path-edge traversal endpoints are no longer stored — they are
    /// derived from `parent_sys_id`/`child_sys_id`/`direction`. This pins that the
    /// derivation matches the old stored semantics for BOTH crossing directions,
    /// and that a path's depth equals its edge count.
    #[test]
    fn business_application_path_edge_derives_endpoints_from_direction() {
        let parent = "pppppppppppppppppppppppppppppppp";
        let child = "cccccccccccccccccccccccccccccccc";
        let rel = BusinessApplicationRelationshipEdge {
            parent_sys_id: parent.to_string(),
            child_sys_id: child.to_string(),
            parent_class: None,
            child_class: None,
            relationship_type: BusinessApplicationRelationshipType {
                value: "dep".to_string(),
                display_value: None,
            },
        };

        // Parent→child crossing: traversal entered at the parent, exited at child.
        let down = rel.path_edge(1, BusinessApplicationRelationshipDirection::ParentToChild);
        assert_eq!(down.from_sys_id(), parent);
        assert_eq!(down.to_sys_id(), child);

        // Child→parent crossing: the endpoints flip.
        let up = rel.path_edge(2, BusinessApplicationRelationshipDirection::ChildToParent);
        assert_eq!(up.from_sys_id(), child);
        assert_eq!(up.to_sys_id(), parent);

        // A path's depth/len is exactly its edge count.
        let path = BusinessApplicationServerPath {
            edges: vec![down, up],
        };
        assert_eq!(path.depth(), 2);
        assert_eq!(path.len(), 2);
        assert!(!path.is_empty());
        assert!(BusinessApplicationServerPath { edges: vec![] }.is_empty());
    }

    /// Fix #5: the paginated edge read must surface truncation when the
    /// `max_edges` budget is consumed while more edge pages remain. Here
    /// `max_edges = 2` and the first (and only fetched) page returns exactly two
    /// edges, filling the page; with `no_count()` the paginator cannot prove it
    /// is done, so reaching the budget with the paginator not exhausted sets
    /// `edge_limit_reached`. This is the boundary the old single-request guard
    /// (`rows.len() > remaining`) could never detect when a server-side cap
    /// returned fewer rows than requested.
    #[tokio::test]
    async fn business_application_servers_edge_budget_paginates_and_truncates() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let app = "11111111111111111111111111111111";
        let ci_one = "22222222222222222222222222222222";
        let ci_two = "33333333333333333333333333333333";
        let rel_type = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .and(query_param(
                "sysparm_query",
                "sys_class_name=cmdb_ci_business_app^number=APM0000001",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": app,
                    "number": "APM0000001",
                    "name": "Application Alpha",
                    "sys_class_name": "cmdb_ci_business_app"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        // First parent page is full (2 == page size derived from max_edges=2),
        // so the paginator believes more pages may exist. The budget caps here.
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("parentIN{app}")))
            .and(query_param("sysparm_offset", "0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row("99999999999999999999999999999991", app, ci_one, rel_type, "cmdb_ci_business_app", "cmdb_ci_appl"),
                    relationship_row("99999999999999999999999999999992", app, ci_two, rel_type, "cmdb_ci_business_app", "cmdb_ci_appl")
                ]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                max_edges: Some(2),
                ..Default::default()
            })
            .await
            .expect("business application servers")
            .expect("business application present");

        assert!(
            result.relationship_summary.edge_limit_reached,
            "consuming max_edges with pages remaining must set edge_limit_reached"
        );
        assert!(result.relationship_summary.truncated);
        assert_eq!(result.relationship_summary.relationships_examined, 2);
    }

    /// Task #8 regression: the parent and child directions are read CONCURRENTLY
    /// but share a single `max_edges` budget that must be consumed in a stable
    /// parent-then-child order. Here `max_edges = 2`, the parent direction returns
    /// one edge and the child direction returns two. The merge must credit the
    /// parent's single edge first, then cap the child's contribution to the one
    /// remaining unit of budget — yielding exactly two examined edges (never three)
    /// and flagging truncation. This proves concurrency does not double-count or
    /// exceed the shared budget, and that the merge order is deterministic.
    #[tokio::test]
    async fn business_application_servers_shared_edge_budget_splits_across_directions() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let app = "11111111111111111111111111111111";
        let ci_one = "22222222222222222222222222222222";
        let ci_two = "33333333333333333333333333333333";
        let ci_three = "44444444444444444444444444444444";
        let rel_type = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .and(query_param(
                "sysparm_query",
                "sys_class_name=cmdb_ci_business_app^number=APM0000001",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": app,
                    "number": "APM0000001",
                    "name": "Application Alpha",
                    "sys_class_name": "cmdb_ci_business_app"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        // Parent direction: a single edge. Merged first, it consumes one unit of
        // the shared budget (max_edges = 2).
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("parentIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row("99999999999999999999999999999991", app, ci_one, rel_type, "cmdb_ci_business_app", "cmdb_ci_appl")
                ]
            })))
            .mount(&server)
            .await;

        // Child direction: two edges, but only one unit of budget is left after the
        // parent merge, so exactly one of these survives the merge truncation.
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("childIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row("99999999999999999999999999999992", ci_two, app, rel_type, "cmdb_ci_appl", "cmdb_ci_business_app"),
                    relationship_row("99999999999999999999999999999993", ci_three, app, rel_type, "cmdb_ci_appl", "cmdb_ci_business_app")
                ]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                max_edges: Some(2),
                ..Default::default()
            })
            .await
            .expect("business application servers")
            .expect("business application present");

        assert_eq!(
            result.relationship_summary.relationships_examined, 2,
            "shared budget must cap combined parent+child edges at max_edges"
        );
        assert!(
            result.relationship_summary.edge_limit_reached,
            "truncating the child direction at the shared budget sets edge_limit_reached"
        );
        assert!(result.relationship_summary.truncated);
    }

    /// Fix #4: `max_cis` bounds the CIs examined BEYOND the root BA. With
    /// `max_cis = 1` and two adjacent CIs, exactly one non-root CI is examined
    /// and the second is truncated. The root BA does not consume the budget, so a
    /// caller asking for one CI is not silently short-changed to zero.
    #[tokio::test]
    async fn business_application_servers_reports_ci_limit_truncation() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let app = "11111111111111111111111111111111";
        let service = "22222222222222222222222222222222";
        let service_two = "55555555555555555555555555555555";
        let rel_type = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .and(query_param(
                "sysparm_query",
                "sys_class_name=cmdb_ci_business_app^number=APM0000001",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": app,
                    "number": "APM0000001",
                    "name": "Application Alpha",
                    "sys_class_name": "cmdb_ci_business_app"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;
        // Two adjacent non-root CIs; with max_cis=1 only the first is examined.
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("parentIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row(
                        "99999999999999999999999999999991",
                        app,
                        service,
                        rel_type,
                        "cmdb_ci_business_app",
                        "cmdb_ci_service"
                    ),
                    relationship_row(
                        "99999999999999999999999999999992",
                        app,
                        service_two,
                        rel_type,
                        "cmdb_ci_business_app",
                        "cmdb_ci_service"
                    )
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("childIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .expect(1)
            .mount(&server)
            .await;
        // The first examined CI (service) is hydrated as a non-server class and
        // expanded into the next depth; it has no further relationships.
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("parentIN{service}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("childIN{service}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                max_cis: Some(1),
                ..Default::default()
            })
            .await
            .expect("business application servers")
            .expect("business application present");

        assert!(result.servers.is_empty());
        assert!(result.relationship_summary.ci_limit_reached);
        assert!(result.relationship_summary.truncated);
        assert_eq!(result.relationship_summary.truncated_count, 1);
        // One non-root CI examined plus the root => 2 in cis_examined.
        assert_eq!(result.relationship_summary.cis_examined, 2);
        assert_eq!(
            result
                .relationship_summary
                .degraded_reasons
                .get("fanout_limit_exceeded")
                .copied(),
            Some(1)
        );
        // The SECOND CI (service_two) is the one truncated by the budget.
        assert!(result.diagnostics.iter().any(|diagnostic| {
            diagnostic.reason == ReferenceResolutionReason::FanoutLimitExceeded
                && diagnostic.reference_sys_id == service_two
        }));
    }

    /// Fix #1 regression guard: a `cmdb_ci_server` subclass that is NOT in the
    /// legacy `SERVER_TABLES` allowlist (here `cmdb_ci_esx_server`) must still be
    /// classified as a server and hydrated, not traversed through as an
    /// intermediate CI. The hydration query targets the base `cmdb_ci_server`
    /// table and must NOT pin `sys_class_name` to the allowlist, otherwise the
    /// subclass record would be filtered out server-side.
    #[tokio::test]
    async fn business_application_servers_collects_server_subclasses() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let app = "11111111111111111111111111111111";
        let esx = "66666666666666666666666666666666";
        let rel_type = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let rel_row = "99999999999999999999999999999991";

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .and(query_param(
                "sysparm_query",
                "sys_class_name=cmdb_ci_business_app^number=APM0000001",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": app,
                    "number": "APM0000001",
                    "name": "Application Alpha",
                    "sys_class_name": "cmdb_ci_business_app"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        // The BA depends on an ESX server subclass directly.
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("parentIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row(
                        rel_row,
                        app,
                        esx,
                        rel_type,
                        "cmdb_ci_business_app",
                        "cmdb_ci_esx_server"
                    )
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("childIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .expect(1)
            .mount(&server)
            .await;

        // Hydration must query the base server table by sys_id only (no
        // sys_class_name allowlist filter) so the ESX subclass row is returned.
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param("sysparm_query", format!("sys_idIN{esx}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [server_row(esx, "esx-alpha.example.com", "cmdb_ci_esx_server")]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                ..Default::default()
            })
            .await
            .expect("business application servers")
            .expect("business application present");

        assert_eq!(result.servers.len(), 1, "ESX subclass should be a server");
        assert_eq!(result.servers[0].record.sys_id, esx);
        assert_eq!(
            result.servers[0].class_name.as_deref(),
            Some("cmdb_ci_esx_server")
        );
        assert_eq!(result.relationship_summary.servers_found, 1);
    }

    /// Task #1-deeper: a server subclass whose table name does NOT end in
    /// `_server` and is in no allowlist (here `cmdb_ci_acme_compute`) is invisible
    /// to every cheap heuristic. It must still be recognized as a server via the
    /// metadata-backed `sys_db_object` super_class descent — the class extends
    /// `cmdb_ci_server`, so walking its ancestry reveals the server base table and
    /// the CI is collected/hydrated rather than traversed through.
    #[tokio::test]
    async fn business_application_servers_detects_custom_subclass_via_hierarchy() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let app = "11111111111111111111111111111111";
        let compute = "77777777777777777777777777777777";
        let rel_type = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let rel_row = "99999999999999999999999999999991";
        // sys_id of the cmdb_ci_server class row in sys_db_object.
        let server_class = "5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e5e";

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .and(query_param(
                "sysparm_query",
                "sys_class_name=cmdb_ci_business_app^number=APM0000001",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": app,
                    "number": "APM0000001",
                    "name": "Application Alpha",
                    "sys_class_name": "cmdb_ci_business_app"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        // The BA depends on a custom server subclass whose name does not end in
        // `_server`, so no cheap heuristic classifies it.
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("parentIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row(
                        rel_row,
                        app,
                        compute,
                        rel_type,
                        "cmdb_ci_business_app",
                        "cmdb_ci_acme_compute"
                    )
                ]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("childIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .mount(&server)
            .await;

        // super_class descent: cmdb_ci_acme_compute -> (super_class sys_id) ->
        // cmdb_ci_server, which terminates the walk.
        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_db_object"))
            .and(query_param("sysparm_query", "name=cmdb_ci_acme_compute"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{ "name": "cmdb_ci_acme_compute", "super_class": server_class }]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_db_object"))
            .and(query_param(
                "sysparm_query",
                format!("sys_id={server_class}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{ "name": "cmdb_ci_server" }]
            })))
            .mount(&server)
            .await;
        // Once cmdb_ci_server becomes the cursor, terminate the walk.
        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_db_object"))
            .and(query_param("sysparm_query", "name=cmdb_ci_server"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{ "name": "cmdb_ci_server", "super_class": "" }]
            })))
            .mount(&server)
            .await;

        // Hydration of the recognized server by sys_id against the base table.
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param("sysparm_query", format!("sys_idIN{compute}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [server_row(compute, "acme-compute-01.example.com", "cmdb_ci_acme_compute")]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                ..Default::default()
            })
            .await
            .expect("business application servers")
            .expect("business application present");

        assert_eq!(
            result.servers.len(),
            1,
            "custom subclass extending cmdb_ci_server must be detected via hierarchy"
        );
        assert_eq!(result.servers[0].record.sys_id, compute);
        assert_eq!(result.relationship_summary.servers_found, 1);
    }

    /// Fix #2 regression guard: with the default (unspecified) relationship-type
    /// filter, an edge must match by the stable `cmdb_rel_type` identity (sys_id)
    /// even when the instance's display label has been renamed/localized so it no
    /// longer equals any default label string. The traversal resolves the default
    /// label set to sys_ids once via a `cmdb_rel_type` lookup and matches on those.
    #[tokio::test]
    async fn business_application_servers_default_types_match_by_resolved_identity() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let app = "11111111111111111111111111111111";
        let linux = "33333333333333333333333333333333";
        // sys_id of the "Depends on::Used by" cmdb_rel_type on this instance.
        let depends_on = "dededededededededededededededede";
        let rel_row = "99999999999999999999999999999991";

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .and(query_param(
                "sysparm_query",
                "sys_class_name=cmdb_ci_business_app^number=APM0000001",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": app,
                    "number": "APM0000001",
                    "name": "Application Alpha",
                    "sys_class_name": "cmdb_ci_business_app"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        // The default-label resolution query against cmdb_rel_type returns the
        // sys_id of the "Depends on::Used by" type (by its stored name).
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_type"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    { "sys_id": depends_on, "name": "Depends on::Used by" }
                ]
            })))
            .mount(&server)
            .await;

        // The edge carries the resolved sys_id but a RENAMED display label that
        // does not equal any default label string.
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("parentIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row_typed(
                        rel_row,
                        app,
                        linux,
                        depends_on,
                        "Depende de::Usado por",
                        "cmdb_ci_business_app",
                        "cmdb_ci_linux_server"
                    )
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("childIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param("sysparm_query", format!("sys_idIN{linux}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [server_row(linux, "linux-alpha.example.com", "cmdb_ci_linux_server")]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                ..Default::default()
            })
            .await
            .expect("business application servers")
            .expect("business application present");

        assert_eq!(
            result.servers.len(),
            1,
            "default filter must match renamed-label edge by resolved sys_id"
        );
        assert_eq!(result.servers[0].record.sys_id, linux);
    }

    /// Fix #2: an explicitly-supplied EMPTY relationship-type allowlist means
    /// "match all", so an edge with an arbitrary type is still traversed and its
    /// server collected, and no cmdb_rel_type resolution query is required.
    #[tokio::test]
    async fn business_application_servers_explicit_empty_types_match_all() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let app = "11111111111111111111111111111111";
        let linux = "33333333333333333333333333333333";
        let weird_type = "cccccccccccccccccccccccccccccccc";
        let rel_row = "99999999999999999999999999999991";

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .and(query_param(
                "sysparm_query",
                "sys_class_name=cmdb_ci_business_app^number=APM0000001",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": app,
                    "number": "APM0000001",
                    "name": "Application Alpha",
                    "sys_class_name": "cmdb_ci_business_app"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        // An arbitrary, non-default relationship type with an unfamiliar label.
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("parentIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row_typed(
                        rel_row,
                        app,
                        linux,
                        weird_type,
                        "Some Custom Relationship",
                        "cmdb_ci_business_app",
                        "cmdb_ci_linux_server"
                    )
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("childIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param("sysparm_query", format!("sys_idIN{linux}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [server_row(linux, "linux-alpha.example.com", "cmdb_ci_linux_server")]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        // An explicit empty allowlist (caller passed `relationship_type: []`
        // explicitly via the options) means "match all".
        let options = BusinessApplicationServersOptions {
            selector: BusinessApplicationServersSelector::Number("APM0000001".to_string()),
            max_depth: 2,
            max_cis: 500,
            max_edges: 2000,
            max_service_membership_associations:
                BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_SERVICE_MEMBERSHIP_ASSOCIATIONS,
            max_service_membership_pages:
                BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_SERVICE_MEMBERSHIP_PAGES,
            relationship_type: Vec::new(),
            include_paths: false,
            fallback_strategy: FallbackStrategy::None,
            persist: false,
            prune_stale: false,
        };
        // defaults_when_empty = false => empty allowlist means "match all".
        let result = core
            .business_application_servers_with_options(options, false)
            .await
            .expect("business application servers")
            .expect("business application present");

        assert_eq!(
            result.servers.len(),
            1,
            "explicit empty allowlist must match all relationship types"
        );
    }

    fn service_membership_row(
        sys_id: &str,
        service_id: &str,
        ci_id: &str,
        ci_class_name: &str,
    ) -> serde_json::Value {
        serde_json::json!({
            "sys_id": sys_id,
            "service_id": {
                "value": service_id,
                "display_value": "Application Service"
            },
            "service_id.sys_class_name": "cmdb_ci_service",
            "ci_id": {
                "value": ci_id,
                "display_value": "Server CI"
            },
            "ci_id.sys_class_name": ci_class_name
        })
    }

    #[tokio::test]
    async fn business_application_servers_returns_service_membership_servers() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let app = "11111111111111111111111111111111";
        let service = "22222222222222222222222222222222";
        let linux = "33333333333333333333333333333333";
        let consumes = "cccccccccccccccccccccccccccccccc";

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .and(query_param(
                "sysparm_query",
                "sys_class_name=cmdb_ci_business_app^number=APM0000001",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": app,
                    "number": "APM0000001",
                    "name": "Application Alpha",
                    "sys_class_name": "cmdb_ci_business_app"
                }]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("parentIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row_typed(
                        "99999999999999999999999999999991",
                        app,
                        service,
                        consumes,
                        "Consumes::Consumed by",
                        "cmdb_ci_business_app",
                        "cmdb_ci_service"
                    )
                ]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("childIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/svc_ci_assoc"))
            .and(query_param(
                "sysparm_query",
                format!("service_idIN{service}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [service_membership_row(
                    "88888888888888888888888888888881",
                    service,
                    linux,
                    "cmdb_ci_linux_server"
                )]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param("sysparm_query", format!("sys_idIN{linux}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [server_row(linux, "linux-service.example.com", "cmdb_ci_linux_server")]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                include_paths: true,
                persist: Some(true),
                ..Default::default()
            })
            .await
            .expect("business application servers")
            .expect("business application present");

        assert_eq!(result.servers.len(), 1);
        assert_eq!(
            result.server_provenance.get(linux),
            Some(&BusinessApplicationServerProvenance::ServiceMembership)
        );
        let path = &result.server_paths[linux][0];
        assert_eq!(path.depth(), 2);
        assert_eq!(
            path.edges.last().map(|edge| &edge.edge_source),
            Some(&BusinessApplicationServerPathEdgeSource::ServiceMembership)
        );
        assert_eq!(
            path.edges
                .last()
                .map(|edge| edge.relationship_type.value.as_str()),
            Some("service_member_of")
        );
        assert_eq!(
            result
                .inventory_health
                .as_ref()
                .map(|health| health.service_membership_status.as_str()),
            Some("ok")
        );

        let cached = core
            .business_application_servers_cached(BusinessApplicationServersCachedParams {
                sys_id: Some(app.to_string()),
                ..Default::default()
            })
            .await
            .expect("cached forward lookup")
            .expect("business application cached");
        assert_eq!(cached.servers.len(), 1);
        assert_eq!(cached.servers[0].provenance, "service_membership");
        assert_eq!(cached.servers[0].min_depth, 2);
    }

    #[tokio::test]
    async fn business_application_servers_merges_relationship_and_service_membership_provenance() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let app = "11111111111111111111111111111111";
        let service = "22222222222222222222222222222222";
        let linux = "33333333333333333333333333333333";
        let runs = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let consumes = "cccccccccccccccccccccccccccccccc";

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .and(query_param(
                "sysparm_query",
                "sys_class_name=cmdb_ci_business_app^number=APM0000001",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": app,
                    "number": "APM0000001",
                    "name": "Application Alpha",
                    "sys_class_name": "cmdb_ci_business_app"
                }]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("parentIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row_typed(
                        "99999999999999999999999999999991",
                        app,
                        linux,
                        runs,
                        "Runs on::Runs",
                        "cmdb_ci_business_app",
                        "cmdb_ci_linux_server"
                    ),
                    relationship_row_typed(
                        "99999999999999999999999999999992",
                        app,
                        service,
                        consumes,
                        "Consumes::Consumed by",
                        "cmdb_ci_business_app",
                        "cmdb_ci_service"
                    )
                ]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("childIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/svc_ci_assoc"))
            .and(query_param(
                "sysparm_query",
                format!("service_idIN{service}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [service_membership_row(
                    "88888888888888888888888888888881",
                    service,
                    linux,
                    "cmdb_ci_linux_server"
                )]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param("sysparm_query", format!("sys_idIN{linux}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [server_row(linux, "linux-both.example.com", "cmdb_ci_linux_server")]
            })))
            .expect(2)
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                include_paths: true,
                persist: Some(true),
                ..Default::default()
            })
            .await
            .expect("business application servers")
            .expect("business application present");

        assert_eq!(result.servers.len(), 1);
        assert_eq!(
            result.server_provenance.get(linux),
            Some(&BusinessApplicationServerProvenance::Both)
        );
        assert_eq!(result.server_paths[linux].len(), 2);

        let cached = core
            .business_application_servers_cached(BusinessApplicationServersCachedParams {
                sys_id: Some(app.to_string()),
                ..Default::default()
            })
            .await
            .expect("cached forward lookup")
            .expect("business application cached");
        assert_eq!(cached.servers.len(), 1);
        assert_eq!(cached.servers[0].provenance, "both");
    }

    #[tokio::test]
    async fn business_application_servers_service_membership_acl_degrades_relationship_results() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let app = "11111111111111111111111111111111";
        let service = "22222222222222222222222222222222";
        let linux = "33333333333333333333333333333333";
        let runs = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let consumes = "cccccccccccccccccccccccccccccccc";

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .and(query_param(
                "sysparm_query",
                "sys_class_name=cmdb_ci_business_app^number=APM0000001",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": app,
                    "number": "APM0000001",
                    "name": "Application Alpha",
                    "sys_class_name": "cmdb_ci_business_app"
                }]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("parentIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row_typed(
                        "99999999999999999999999999999991",
                        app,
                        linux,
                        runs,
                        "Runs on::Runs",
                        "cmdb_ci_business_app",
                        "cmdb_ci_linux_server"
                    ),
                    relationship_row_typed(
                        "99999999999999999999999999999992",
                        app,
                        service,
                        consumes,
                        "Consumes::Consumed by",
                        "cmdb_ci_business_app",
                        "cmdb_ci_service"
                    )
                ]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("childIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/svc_ci_assoc"))
            .and(query_param(
                "sysparm_query",
                format!("service_idIN{service}"),
            ))
            .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
                "error": { "message": "Forbidden" },
                "status": "failure"
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param("sysparm_query", format!("sys_idIN{linux}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [server_row(linux, "linux-acl.example.com", "cmdb_ci_linux_server")]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                persist: Some(true),
                ..Default::default()
            })
            .await
            .expect("business application servers")
            .expect("business application present");

        assert_eq!(result.servers.len(), 1);
        let health = result.inventory_health.expect("inventory health");
        assert_eq!(health.service_membership_status, "acl_restricted");
        assert_eq!(health.relationship_status, "ok");
        assert_eq!(health.inventory_status, "service_membership_degraded");
    }

    #[tokio::test]
    async fn business_application_servers_service_membership_accepts_server_subclasses() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let app = "11111111111111111111111111111111";
        let service = "22222222222222222222222222222222";
        let esx = "66666666666666666666666666666666";
        let consumes = "cccccccccccccccccccccccccccccccc";

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .and(query_param(
                "sysparm_query",
                "sys_class_name=cmdb_ci_business_app^number=APM0000001",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": app,
                    "number": "APM0000001",
                    "name": "Application Alpha",
                    "sys_class_name": "cmdb_ci_business_app"
                }]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("parentIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    relationship_row_typed(
                        "99999999999999999999999999999991",
                        app,
                        service,
                        consumes,
                        "Consumes::Consumed by",
                        "cmdb_ci_business_app",
                        "cmdb_ci_service"
                    )
                ]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param("sysparm_query", format!("childIN{app}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/svc_ci_assoc"))
            .and(query_param(
                "sysparm_query",
                format!("service_idIN{service}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [service_membership_row(
                    "88888888888888888888888888888881",
                    service,
                    esx,
                    "cmdb_ci_esx_server"
                )]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param("sysparm_query", format!("sys_idIN{esx}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [server_row(esx, "esx-service.example.com", "cmdb_ci_esx_server")]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                ..Default::default()
            })
            .await
            .expect("business application servers")
            .expect("business application present");

        assert_eq!(result.servers.len(), 1);
        assert_eq!(result.servers[0].record.sys_id, esx);
        assert_eq!(
            result.server_provenance.get(esx),
            Some(&BusinessApplicationServerProvenance::ServiceMembership)
        );
    }

    // ---- ci_owner_group CMDB-gap fallback fixtures (Part 2) ----------------

    const FALLBACK_APP: &str = "11111111111111111111111111111111";
    const FALLBACK_GROUP: &str = "9999999999999999999999999999aaaa";
    const FALLBACK_LINUX: &str = "33333333333333333333333333333333";
    const FALLBACK_WINDOWS: &str = "44444444444444444444444444444444";

    /// Mount a BA record with the RAW `u_ci_owner_group` field populated and a
    /// present-but-empty `managed_by_group` alias — the live-data shape the
    /// field-mapping requirement guards against.
    async fn mount_fallback_ba(server: &MockServer, group_sys_id: Option<&str>) {
        let mut record = serde_json::json!({
            "sys_id": FALLBACK_APP,
            "number": "APM0000001",
            "name": "Application Alpha",
            "sys_class_name": "cmdb_ci_business_app",
            // The typed `ci_owner_group` alias maps here and is intentionally
            // empty on live data; the fallback must NOT read it.
            "managed_by_group": { "value": "", "display_value": "" }
        });
        if let Some(group_sys_id) = group_sys_id {
            record.as_object_mut().unwrap().insert(
                BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD.to_string(),
                serde_json::json!({ "value": group_sys_id, "display_value": "Owner Group" }),
            );
        }
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .and(query_param(
                "sysparm_query",
                "sys_class_name=cmdb_ci_business_app^number=APM0000001",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [record]
            })))
            .mount(server)
            .await;
    }

    /// Mount the two empty `cmdb_rel_ci` direction reads so the traversal finds 0
    /// servers (default depth 2 only issues the depth-1 frontier reads).
    async fn mount_empty_traversal(server: &MockServer) {
        for direction in ["parent", "child"] {
            Mock::given(method("GET"))
                .and(path("/api/now/table/cmdb_rel_ci"))
                .and(query_param(
                    "sysparm_query",
                    format!("{direction}IN{FALLBACK_APP}"),
                ))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "result": []
                })))
                .mount(server)
                .await;
        }
    }

    #[tokio::test]
    async fn ci_owner_group_fallback_returns_tagged_servers_when_traversal_empty() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        mount_fallback_ba(&server, Some(FALLBACK_GROUP)).await;
        mount_empty_traversal(&server).await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param(
                "sysparm_query",
                format!("{BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD}={FALLBACK_GROUP}^ORDERBYname"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    server_row(FALLBACK_LINUX, "linux-fallback.example.com", "cmdb_ci_linux_server"),
                    server_row(FALLBACK_WINDOWS, "windows-fallback.example.com", "cmdb_ci_win_server")
                ]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                fallback_strategy: FallbackStrategy::CiOwnerGroup,
                ..Default::default()
            })
            .await
            .expect("ba servers")
            .expect("ba present");

        assert_eq!(result.servers.len(), 2);
        for server in &result.servers {
            assert_eq!(
                result.server_sources.get(&server.record.sys_id),
                Some(&ServerResultSource::CiOwnerGroupFallback)
            );
        }
        let summary = &result.relationship_summary;
        assert!(summary.fallback_used);
        assert_eq!(summary.cmdb_servers_found, Some(0));
        assert_eq!(summary.servers_found, 2);
        assert_eq!(summary.fallback_strategy.as_deref(), Some("ci_owner_group"));
        assert_eq!(
            summary.fallback_group_sys_id.as_deref(),
            Some(FALLBACK_GROUP)
        );
        assert_eq!(
            summary.fallback_group_display_name.as_deref(),
            Some("Owner Group")
        );
        assert_eq!(
            summary
                .degraded_reasons
                .get(BUSINESS_APPLICATION_DEGRADED_REASON_CMDB_RELATIONSHIPS_UNMAPPED),
            Some(&1)
        );
        // Fallback never persists: no traversal servers, no membership upserts.
        assert_eq!(summary.persisted_servers, 0);
        assert_eq!(summary.membership_upserts, 0);
        assert!(result.server_provenance.is_empty());
    }

    #[tokio::test]
    async fn ci_owner_group_fallback_fires_even_though_managed_by_group_empty() {
        let _guard = mock_server_test_lock().await;
        // Field-mapping proof: the BA's managed_by_group alias is empty; the
        // fallback fires because it sources/filters on the RAW u_ci_owner_group.
        let server = MockServer::start().await;
        mount_fallback_ba(&server, Some(FALLBACK_GROUP)).await;
        mount_empty_traversal(&server).await;
        // The ONLY server query the fallback may issue is the exact raw-field
        // filter. A managed_by_group-based query would not match this mock and
        // the call would 404/timeout, failing the test.
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param(
                "sysparm_query",
                format!("{BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD}={FALLBACK_GROUP}^ORDERBYname"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [server_row(FALLBACK_LINUX, "linux-fallback.example.com", "cmdb_ci_linux_server")]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                fallback_strategy: FallbackStrategy::CiOwnerGroup,
                ..Default::default()
            })
            .await
            .expect("ba servers")
            .expect("ba present");

        assert_eq!(result.servers.len(), 1);
        assert!(result.relationship_summary.fallback_used);
    }

    #[tokio::test]
    async fn ci_owner_group_fallback_does_not_fire_when_traversal_finds_servers() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let rel_type = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        mount_fallback_ba(&server, Some(FALLBACK_GROUP)).await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param(
                "sysparm_query",
                format!("parentIN{FALLBACK_APP}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [relationship_row(
                    "99999999999999999999999999999991",
                    FALLBACK_APP,
                    FALLBACK_LINUX,
                    rel_type,
                    "cmdb_ci_business_app",
                    "cmdb_ci_linux_server"
                )]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_rel_ci"))
            .and(query_param(
                "sysparm_query",
                format!("childIN{FALLBACK_APP}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param("sysparm_query", format!("sys_idIN{FALLBACK_LINUX}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [server_row(FALLBACK_LINUX, "linux-real.example.com", "cmdb_ci_linux_server")]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                fallback_strategy: FallbackStrategy::CiOwnerGroup,
                ..Default::default()
            })
            .await
            .expect("ba servers")
            .expect("ba present");

        assert_eq!(result.servers.len(), 1);
        // Fallback did NOT fire: no fallback servers, no degraded gap reason.
        assert!(!result.relationship_summary.fallback_used);
        assert!(result.server_sources.is_empty());
        // cmdb_servers_found still reported (strategy requested) and equals the
        // traversal count.
        assert_eq!(result.relationship_summary.cmdb_servers_found, Some(1));
        assert_eq!(
            result.server_sources.get(FALLBACK_LINUX),
            None,
            "traversal servers are not tagged as fallback"
        );
    }

    #[tokio::test]
    async fn ci_owner_group_fallback_no_group_emits_clean_diagnostic() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        mount_fallback_ba(&server, None).await;
        mount_empty_traversal(&server).await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                fallback_strategy: FallbackStrategy::CiOwnerGroup,
                ..Default::default()
            })
            .await
            .expect("ba servers")
            .expect("ba present");

        assert!(result.servers.is_empty());
        assert!(!result.relationship_summary.fallback_used);
        assert_eq!(result.relationship_summary.cmdb_servers_found, Some(0));
        // Clean structured diagnostic, not an error.
        assert!(result.diagnostics.iter().any(|diagnostic| {
            diagnostic.field == BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD
        }));
    }

    #[tokio::test]
    async fn ci_owner_group_fallback_strategy_none_adds_no_fields() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        mount_fallback_ba(&server, Some(FALLBACK_GROUP)).await;
        mount_empty_traversal(&server).await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                // default: FallbackStrategy::None
                ..Default::default()
            })
            .await
            .expect("ba servers")
            .expect("ba present");

        assert!(result.servers.is_empty());
        assert!(result.server_sources.is_empty());
        let summary = &result.relationship_summary;
        assert!(!summary.fallback_used);
        assert_eq!(summary.cmdb_servers_found, None);
        assert_eq!(summary.fallback_strategy, None);
        assert_eq!(summary.fallback_group_sys_id, None);
        // No new fields appear in the default-path serialization.
        let serialized = serde_json::to_value(summary).expect("serialize summary");
        let object = serialized.as_object().expect("summary object");
        assert!(!object.contains_key("cmdb_servers_found"));
        assert!(!object.contains_key("fallback_used"));
        assert!(!object.contains_key("fallback_strategy"));
    }

    #[tokio::test]
    async fn ci_owner_group_fallback_group_with_zero_servers() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        mount_fallback_ba(&server, Some(FALLBACK_GROUP)).await;
        mount_empty_traversal(&server).await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param(
                "sysparm_query",
                format!(
                    "{BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD}={FALLBACK_GROUP}^ORDERBYname"
                ),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                fallback_strategy: FallbackStrategy::CiOwnerGroup,
                ..Default::default()
            })
            .await
            .expect("ba servers")
            .expect("ba present");

        assert!(result.servers.is_empty());
        // The owner group exists and was queried, so the data-quality gap is real:
        // fallback_used is true even though it returned no servers.
        assert!(result.relationship_summary.fallback_used);
        assert_eq!(result.relationship_summary.cmdb_servers_found, Some(0));
    }

    #[tokio::test]
    async fn ci_owner_group_fallback_acl_restricted_query() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        mount_fallback_ba(&server, Some(FALLBACK_GROUP)).await;
        mount_empty_traversal(&server).await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param(
                "sysparm_query",
                format!(
                    "{BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD}={FALLBACK_GROUP}^ORDERBYname"
                ),
            ))
            .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
                "error": { "message": "ACL restricted" }
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                fallback_strategy: FallbackStrategy::CiOwnerGroup,
                ..Default::default()
            })
            .await
            .expect("ba servers")
            .expect("ba present");

        assert!(result.servers.is_empty());
        assert!(!result.relationship_summary.fallback_used);
        assert!(result.relationship_summary.acl_restricted_count >= 1);
        assert!(result.diagnostics.iter().any(|diagnostic| {
            diagnostic.reason == ReferenceResolutionReason::ReferenceAclRestricted
        }));
    }

    #[tokio::test]
    async fn ci_owner_group_fallback_tombstoned_group_no_panic() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        mount_fallback_ba(&server, Some(FALLBACK_GROUP)).await;
        mount_empty_traversal(&server).await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param(
                "sysparm_query",
                format!(
                    "{BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD}={FALLBACK_GROUP}^ORDERBYname"
                ),
            ))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "error": { "message": "No record found" }
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                fallback_strategy: FallbackStrategy::CiOwnerGroup,
                ..Default::default()
            })
            .await
            .expect("ba servers")
            .expect("ba present");

        assert!(result.servers.is_empty());
        assert!(!result.relationship_summary.fallback_used);
        assert!(result.diagnostics.iter().any(|diagnostic| {
            diagnostic.field == BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD
                && diagnostic.reason == ReferenceResolutionReason::ReferenceNotFound
        }));
    }

    #[tokio::test]
    async fn ci_owner_group_fallback_writes_no_durable_membership_rows() {
        let _guard = mock_server_test_lock().await;
        // Load-bearing live-only assertion: a fallback-triggering run leaves the
        // durable BA↔server inventory tables unchanged. We run with persist=true
        // (the CLI/daemon default) so the only writes would come from the
        // traversal path; the fallback servers must not appear in the cached
        // forward/reverse projections or the inventory-health row.
        let server = MockServer::start().await;
        mount_fallback_ba(&server, Some(FALLBACK_GROUP)).await;
        mount_empty_traversal(&server).await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param(
                "sysparm_query",
                format!("{BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD}={FALLBACK_GROUP}^ORDERBYname"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [server_row(FALLBACK_LINUX, "linux-fallback.example.com", "cmdb_ci_linux_server")]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .business_application_servers(BusinessApplicationServersParams {
                number: Some("APM0000001".to_string()),
                fallback_strategy: FallbackStrategy::CiOwnerGroup,
                persist: Some(true),
                ..Default::default()
            })
            .await
            .expect("ba servers")
            .expect("ba present");

        // The live response carries the fallback server...
        assert_eq!(result.servers.len(), 1);
        assert!(result.relationship_summary.fallback_used);
        // ...but nothing was persisted: 0 membership upserts, 0 persisted servers.
        assert_eq!(result.relationship_summary.persisted_servers, 0);
        assert_eq!(result.relationship_summary.membership_upserts, 0);

        // The durable forward projection has NO servers for this BA.
        let cached_forward = core
            .business_application_servers_cached(BusinessApplicationServersCachedParams {
                sys_id: Some(FALLBACK_APP.to_string()),
                ..Default::default()
            })
            .await
            .expect("cached forward");
        if let Some(cached) = cached_forward {
            assert!(
                cached.servers.is_empty(),
                "fallback server must not be persisted to the forward projection"
            );
        }

        // The durable reverse projection has NO BA for the fallback server.
        let cached_reverse = core
            .business_applications_for_server(BusinessApplicationsForServerParams {
                sys_id: Some(FALLBACK_LINUX.to_string()),
                ..Default::default()
            })
            .await
            .expect("cached reverse");
        if let Some(cached) = cached_reverse {
            for entry in &cached.servers {
                assert!(
                    entry.business_applications.is_empty(),
                    "fallback server must not gain a durable BA association"
                );
            }
        }
    }

    fn relationship_row(
        sys_id: &str,
        parent: &str,
        child: &str,
        relationship_type: &str,
        parent_class: &str,
        child_class: &str,
    ) -> serde_json::Value {
        relationship_row_typed(
            sys_id,
            parent,
            child,
            relationship_type,
            "Depends on::Used by",
            parent_class,
            child_class,
        )
    }

    /// Like [`relationship_row`] but lets a test set the relationship type's
    /// display label independently of its sys_id, so Fix #2 can simulate a
    /// renamed/localized `cmdb_rel_type` label.
    fn relationship_row_typed(
        sys_id: &str,
        parent: &str,
        child: &str,
        relationship_type: &str,
        relationship_type_label: &str,
        parent_class: &str,
        child_class: &str,
    ) -> serde_json::Value {
        serde_json::json!({
            "sys_id": sys_id,
            "parent": { "value": parent, "display_value": "Parent CI" },
            "child": { "value": child, "display_value": "Child CI" },
            "type": {
                "value": relationship_type,
                "display_value": relationship_type_label
            },
            "parent.sys_class_name": parent_class,
            "child.sys_class_name": child_class
        })
    }

    fn server_row(sys_id: &str, name: &str, class_name: &str) -> serde_json::Value {
        serde_json::json!({
            "sys_id": sys_id,
            "name": name,
            "sys_class_name": class_name,
            "ip_address": "192.0.2.10",
            "operational_status": { "value": "1", "display_value": "Operational" }
        })
    }

    /// Mount a `sys_db_object` response so `table_ancestors` terminates with no
    /// parent (the Business Application table is treated as its own root).
    async fn mount_no_table_ancestors(server: &MockServer) {
        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_db_object"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{ "name": BUSINESS_APPLICATION_TABLE, "super_class": "" }]
            })))
            .mount(server)
            .await;
    }

    #[tokio::test]
    async fn business_application_dictionary_refresh_caches_metadata_and_promotes_aliases() {
        let server = MockServer::start().await;
        mount_no_table_ancestors(&server).await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_dictionary"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    {
                        "name": BUSINESS_APPLICATION_TABLE,
                        "element": "portfolio",
                        "column_label": "Primary Portfolio",
                        "internal_type": { "value": "reference", "display_value": "Reference" },
                        "reference": { "value": "pm_portfolio", "display_value": "Portfolio" },
                        "choice": "0",
                        "mandatory": "false",
                        "read_only": "false",
                        "max_length": "32",
                        "active": "true"
                    },
                    {
                        "name": BUSINESS_APPLICATION_TABLE,
                        "element": "operational_status",
                        "column_label": "Operational State",
                        "internal_type": { "value": "choice", "display_value": "Choice" },
                        "reference": "",
                        "choice": "1",
                        "mandatory": "false",
                        "read_only": "false",
                        "max_length": "40",
                        "active": "true"
                    }
                ]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let persisted = core
            .refresh_business_application_dictionary()
            .await
            .expect("dictionary refresh");
        assert_eq!(persisted, 2);

        let dictionary = core
            .business_application_dictionary()
            .await
            .expect("dictionary read");
        assert_eq!(
            dictionary
                .get("portfolio")
                .and_then(|row| row.reference_table.clone()),
            Some("pm_portfolio".to_string())
        );
        assert_eq!(
            dictionary
                .get("operational_status")
                .map(|row| row.field_type.clone()),
            Some(Some("choice".to_string()))
        );
        assert!(dictionary["operational_status"].choice);

        let aliases = core.business_application_aliases().await.expect("aliases");
        // Dictionary-verified: portfolio target table discovered, version set,
        // and no DictionaryUnavailable diagnostic.
        assert_eq!(aliases.primary_portfolio, "portfolio");
        assert_eq!(
            aliases.primary_portfolio_table,
            Some("pm_portfolio".to_string())
        );
        assert!(aliases.dictionary_version.is_some());
        assert!(aliases.diagnostics.is_empty());
    }

    #[tokio::test]
    async fn business_application_aliases_degrade_without_dictionary() {
        let server = MockServer::start().await;
        mount_no_table_ancestors(&server).await;
        let (core, _tempdir) = core_for_mock_server(&server).await;
        let aliases = core.business_application_aliases().await.expect("aliases");
        // Cache miss => baseline degraded with a DictionaryUnavailable diagnostic.
        assert_eq!(aliases.primary_portfolio, "portfolio");
        assert!(aliases.dictionary_version.is_none());
        assert!(aliases.diagnostics.iter().any(|diagnostic| {
            diagnostic.reason == ReferenceResolutionReason::DictionaryUnavailable
        }));
    }

    #[tokio::test]
    async fn business_application_sync_summarizes_run() {
        let server = MockServer::start().await;
        mount_no_table_ancestors(&server).await;
        let owner_sys_id = "6816f79cc0a8016401c5a33be04be441";
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "54a4b61b6fe845000ed852a03f3ee4d0",
                    "name": "Epic",
                    "sys_class_name": "cmdb_ci_business_app",
                    "business_owner": { "value": owner_sys_id, "display_value": "Jane Owner" },
                    "portfolio": { "value": "portfolio-sys-id-000000000000000", "display_value": "Clinical" },
                    "operational_status": { "value": "1", "display_value": "Operational" }
                }]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/api/now/table/sys_user/{owner_sys_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": {
                    "sys_id": owner_sys_id,
                    "name": "Jane Owner",
                    "user_name": "jowner",
                    "email": "jane.owner@example.invalid",
                    "active": { "value": "true", "display_value": "true" },
                    "sys_updated_on": "2026-05-30 12:00:00"
                }
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let summary = core
            .sync_business_applications(
                Some(BusinessApplicationSearchParams {
                    name: Some("Epic".to_string()),
                    limit: Some(5),
                    ..Default::default()
                }),
                BusinessApplicationHydrationOptions::default(),
            )
            .await
            .expect("sync summary");

        assert!(!summary.all);
        assert_eq!(summary.table, BUSINESS_APPLICATION_TABLE);
        assert_eq!(summary.page_size, 5);
        assert_eq!(summary.pages, 1);
        assert_eq!(summary.total_returned, 1);
        assert_eq!(summary.total_applications, 1);
        assert_eq!(summary.persisted, 1);
        // Owner reference resolves; the portfolio reference is degraded because
        // no dictionary was loaded for this run.
        assert!(summary.references_resolved >= 1);
        assert!(summary.dictionary_degraded);
        assert!(!summary.dictionary_refreshed);
        assert!(
            summary
                .degraded_reasons
                .contains_key("dictionary_unavailable")
        );
    }

    #[tokio::test]
    async fn business_application_sync_non_persistent_reports_zero_persisted() {
        let server = MockServer::start().await;
        mount_no_table_ancestors(&server).await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "54a4b61b6fe845000ed852a03f3ee4d0",
                    "name": "Epic",
                    "sys_class_name": "cmdb_ci_business_app",
                    "operational_status": { "value": "1", "display_value": "Operational" }
                }]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let summary = core
            .sync_business_applications(
                None,
                BusinessApplicationHydrationOptions {
                    persist: false,
                    ..Default::default()
                },
            )
            .await
            .expect("sync summary");

        assert_eq!(summary.total_applications, 1);
        assert_eq!(summary.total_returned, 1);
        assert_eq!(summary.persisted, 0);
    }

    fn business_application_page_record(index: usize) -> serde_json::Value {
        serde_json::json!({
            "sys_id": format!("{:032x}", index + 1),
            "name": format!("Application {:03}", index + 1),
            "number": format!("APP{:07}", index + 1),
            "sys_class_name": BUSINESS_APPLICATION_TABLE,
            "operational_status": { "value": "1", "display_value": "Operational" }
        })
    }

    #[tokio::test]
    async fn business_application_sync_all_drains_live_pages_and_persists_each_page() {
        let server = MockServer::start().await;
        mount_no_table_ancestors(&server).await;
        let first_page = (0..100)
            .map(business_application_page_record)
            .collect::<Vec<_>>();
        let second_page = vec![business_application_page_record(100)];

        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .and(query_param("sysparm_limit", "100"))
            .and(query_param("sysparm_offset", "0"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": first_page
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_business_app"))
            .and(query_param("sysparm_limit", "100"))
            .and(query_param("sysparm_offset", "100"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": second_page
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let summary = core
            .sync_all_business_applications(BusinessApplicationHydrationOptions::default())
            .await
            .expect("sync all summary");

        assert!(summary.all);
        assert_eq!(summary.table, BUSINESS_APPLICATION_TABLE);
        assert_eq!(summary.page_size, 100);
        assert_eq!(summary.pages, 2);
        assert_eq!(summary.total_returned, 101);
        assert_eq!(summary.total_applications, 101);
        assert_eq!(summary.persisted, 101);
        assert!(summary.dictionary_degraded);
        assert_eq!(
            summary
                .degraded_reasons
                .get("dictionary_unavailable")
                .copied(),
            Some(101)
        );

        let cached = core
            .query_business_applications(BusinessApplicationQuery {
                limit: Some(200),
                ..Default::default()
            })
            .await
            .expect("cached business applications");
        assert_eq!(cached.len(), 101);

        let requests = server.received_requests().await.expect("requests");
        let business_app_requests = requests
            .iter()
            .filter(|request| request.url.path() == "/api/now/table/cmdb_ci_business_app")
            .collect::<Vec<_>>();
        assert_eq!(business_app_requests.len(), 2);
        for request in business_app_requests {
            let query = request
                .url
                .query_pairs()
                .collect::<std::collections::HashMap<_, _>>();
            assert_eq!(
                query.get("sysparm_query").map(|value| value.as_ref()),
                Some("sys_class_name=cmdb_ci_business_app^ORDERBYname^ORDERBYsys_id")
            );
            assert_eq!(
                query
                    .get("sysparm_display_value")
                    .map(|value| value.as_ref()),
                Some("all")
            );
            assert_eq!(
                query
                    .get("sysparm_exclude_reference_link")
                    .map(|value| value.as_ref()),
                Some("true")
            );
        }
    }

    #[test]
    fn business_application_reference_discovery_uses_known_map() {
        let record = Record::from_json(
            BUSINESS_APPLICATION_TABLE,
            &serde_json::json!({
                "sys_id": "54a4b61b6fe845000ed852a03f3ee4d0",
                "name": "Epic",
                "business_owner": {
                    "value": "6816f79cc0a8016401c5a33be04be441",
                    "display_value": "Jane Owner"
                },
                "support_group": {
                    "value": "287ebd7da9fe198100f92cc8d1d2154e",
                    "display_value": "App Support"
                },
                "portfolio": {
                    "value": "46d44a23a9fe19810012d100cca80666",
                    "display_value": "Clinical"
                }
            }),
            DisplayValue::Both,
        )
        .expect("record");

        let business_application = BusinessApplication::from_servicenow(
            &record,
            &BusinessApplicationFieldAliases::baseline_degraded(),
        )
        .expect("business application");

        assert_eq!(
            business_application
                .business_owner
                .as_ref()
                .map(|reference| reference.table.as_str()),
            Some("sys_user")
        );
        assert_eq!(
            business_application
                .primary_support_group
                .as_ref()
                .map(|reference| reference.table.as_str()),
            Some("sys_user_group")
        );
        assert!(
            business_application
                .references
                .iter()
                .any(|reference| reference.field == "portfolio"
                    && reference.reference_table == "pm_portfolio"
                    && reference.resolution_status == ReferenceResolutionStatus::Resolved)
        );
    }

    #[tokio::test]
    async fn list_attachments_resolves_record_number() {
        let server = MockServer::start().await;
        mount_number_lookup(&server, "change_request", "CHG0010001", "chg-sys").await;
        Mock::given(method("GET"))
            .and(path("/api/now/attachment"))
            .and(query_param(
                "sysparm_query",
                "table_name=change_request^table_sys_id=chg-sys",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "att-sys",
                    "file_name": "evidence.jpeg",
                    "table_name": "change_request",
                    "table_sys_id": "chg-sys",
                    "content_type": "image/jpeg",
                    "size_bytes": "83338"
                }]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let attachments = core
            .list_attachments("CHG0010001")
            .await
            .expect("attachments")
            .expect("record");

        assert_eq!(attachments.len(), 1);
        assert_eq!(attachments[0].sys_id, "att-sys");
        assert_eq!(attachments[0].size_bytes, Some(83338));
    }

    #[tokio::test]
    async fn upload_attachment_file_resolves_record_number_and_posts_file() {
        let server = MockServer::start().await;
        mount_number_lookup(&server, "change_request", "CHG0010001", "chg-sys").await;
        Mock::given(method("POST"))
            .and(path("/api/now/attachment/file"))
            .and(query_param("table_name", "change_request"))
            .and(query_param("table_sys_id", "chg-sys"))
            .and(query_param("file_name", "evidence.jpeg"))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "result": {
                    "sys_id": "att-sys",
                    "file_name": "evidence.jpeg",
                    "table_name": "change_request",
                    "table_sys_id": "chg-sys",
                    "content_type": "image/jpeg",
                    "size_bytes": "4"
                }
            })))
            .mount(&server)
            .await;

        let (core, tempdir) = core_for_mock_server(&server).await;
        let path = tempdir.path().join("evidence.jpeg");
        std::fs::write(&path, b"jpeg").expect("write fixture");

        let attachment = core
            .upload_attachment_file(
                "CHG0010001",
                &path,
                Some("evidence.jpeg"),
                Some("image/jpeg"),
            )
            .await
            .expect("upload")
            .expect("record");

        assert_eq!(attachment.sys_id, "att-sys");
        assert_eq!(attachment.size_bytes, Some(4));

        let requests = server.received_requests().await.expect("requests");
        let upload = requests
            .iter()
            .find(|request| request.url.path() == "/api/now/attachment/file")
            .expect("upload request");
        assert_eq!(upload.body, b"jpeg");
    }

    #[tokio::test]
    async fn get_catalog_item_fetches_item_variables_and_choices() {
        let server = MockServer::start().await;
        let item_sys_id = "300d473b13f00c10906630128144b0d1";
        let variable_sys_id = "11111111111111111111111111111111";
        Mock::given(method("GET"))
            .and(path(format!("/api/now/table/sc_cat_item/{item_sys_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": {
                    "sys_id": item_sys_id,
                    "name": "Windows Server Administration Access Request (SAAR)",
                    "short_description": "Request server admin access",
                    "sys_class_name": "sc_cat_item",
                    "active": "true"
                }
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/item_option_new"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": variable_sys_id,
                    "name": "does_the_active_directory_group_exist",
                    "question_text": "Does the Active Directory group exist?",
                    "type": { "value": "5", "display_value": "Select Box" },
                    "mandatory": "true",
                    "default_value": "",
                    "order": "100"
                }]
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/question_choice"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "22222222222222222222222222222222",
                    "question": variable_sys_id,
                    "value": "Yes",
                    "text": "Yes",
                    "order": "100"
                }]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let item = core.get_catalog_item(item_sys_id).await.expect("item");

        assert_eq!(item.sys_id, item_sys_id);
        assert_eq!(
            item.name,
            "Windows Server Administration Access Request (SAAR)"
        );
        assert_eq!(item.variables.len(), 1);
        assert_eq!(
            item.variables[0].name,
            "does_the_active_directory_group_exist"
        );
        assert!(item.variables[0].mandatory);
        assert_eq!(item.variables[0].choices[0].value, "Yes");
    }

    #[tokio::test]
    async fn submit_catalog_request_posts_order_now_and_parses_request_item() {
        let server = MockServer::start().await;
        let item_sys_id = "300d473b13f00c10906630128144b0d1";
        let ritm_sys_id = "29ebf58b2b0dcbd0f7a2fe995e91bfb7";
        Mock::given(method("POST"))
            .and(path(format!(
                "/api/sn_sc/v1/servicecatalog/items/{item_sys_id}/order_now"
            )))
            .and(body_partial_json(serde_json::json!({
                "variables": {
                    "business_justification": "Needed for IAM server administration"
                }
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": {
                    "request_number": "REQ0010001",
                    "request_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "items": [{
                        "table": "sc_req_item",
                        "sys_id": ritm_sys_id,
                        "number": "RITM0010001"
                    }]
                }
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/api/now/table/sc_req_item/{ritm_sys_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": {
                    "sys_id": ritm_sys_id,
                    "number": "RITM0010001",
                    "short_description": "Windows Server Administration Access Request (SAAR)",
                    "state": "Open"
                }
            })))
            .mount(&server)
            .await;
        mount_empty_journal_fetch(&server, "sc_req_item", ritm_sys_id).await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .submit_catalog_request(
                item_sys_id,
                serde_json::json!({
                    "sysparm_quantity": "1",
                    "variables": {
                        "business_justification": "Needed for IAM server administration"
                    }
                }),
            )
            .await
            .expect("submit");

        assert_eq!(result.table.as_deref(), Some("sc_req_item"));
        assert_eq!(result.sys_id.as_deref(), Some(ritm_sys_id));
        assert_eq!(result.number.as_deref(), Some("RITM0010001"));
        assert_eq!(result.request_number.as_deref(), Some("REQ0010001"));
        let expected_url = format!(
            "{}/sp?id=ticket&table=sc_req_item&sys_id={ritm_sys_id}&view=sp",
            server.uri()
        );
        assert_eq!(result.browser_url.as_deref(), Some(expected_url.as_str()));
    }

    #[tokio::test]
    async fn submit_catalog_request_resolves_request_item_from_request_response() {
        let server = MockServer::start().await;
        let item_sys_id = "300d473b13f00c10906630128144b0d1";
        let request_sys_id = "2df34a472b810fd0f7a2fe995e91bf45";
        let ritm_sys_id = "29ebf58b2b0dcbd0f7a2fe995e91bfb7";
        Mock::given(method("POST"))
            .and(path(format!(
                "/api/sn_sc/v1/servicecatalog/items/{item_sys_id}/order_now"
            )))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": {
                    "table": "sc_request",
                    "sys_id": request_sys_id,
                    "request_id": request_sys_id,
                    "number": "REQ2688830",
                    "request_number": "REQ2688830"
                }
            })))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/api/now/table/sc_request/{request_sys_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": {
                    "sys_id": request_sys_id,
                    "number": "REQ2688830",
                    "short_description": "Windows Server Administration Access Request (SAAR)",
                    "state": "requested"
                }
            })))
            .mount(&server)
            .await;
        mount_empty_journal_fetch(&server, "sc_request", request_sys_id).await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/sc_req_item"))
            .and(query_param(
                "sysparm_query",
                format!("request={request_sys_id}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": ritm_sys_id,
                    "number": "RITM2688830",
                    "short_description": "Windows Server Administration Access Request (SAAR)",
                    "state": "Open"
                }]
            })))
            .mount(&server)
            .await;
        mount_empty_journal_fetch(&server, "sc_req_item", ritm_sys_id).await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .submit_catalog_request(item_sys_id, serde_json::json!({ "variables": {} }))
            .await
            .expect("submit");

        assert_eq!(result.table.as_deref(), Some("sc_req_item"));
        assert_eq!(result.sys_id.as_deref(), Some(ritm_sys_id));
        assert_eq!(result.number.as_deref(), Some("RITM2688830"));
        assert_eq!(result.request_number.as_deref(), Some("REQ2688830"));
        assert_eq!(result.request_item_number.as_deref(), Some("RITM2688830"));
        let expected_url = format!(
            "{}/sp?id=ticket&table=sc_req_item&sys_id={ritm_sys_id}&view=sp",
            server.uri()
        );
        assert_eq!(result.browser_url.as_deref(), Some(expected_url.as_str()));
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

    // pub(crate): also called from `service::approval`'s test module (Task 9),
    // which moved its approve/reject/my_approvals tests out of this file. This
    // fn stays here (not duplicated) because it is shared fixture setup used
    // by other non-approval tests throughout this module.
    pub(crate) async fn mount_empty_journal_fetch(server: &MockServer, table: &str, sys_id: &str) {
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

    fn timecard_record_json(monday: &str, state: &str) -> serde_json::Value {
        serde_json::json!({
            "sys_id": "card-sys",
            "time_sheet": {
                "value": "sheet-sys",
                "display_value": "2026-05-17"
            },
            "week_starts_on": "2026-05-17",
            "user": {
                "value": "user-sys",
                "display_value": "Test User"
            },
            "user.user_name": "test_user",
            "user.email": "test@example.com",
            "task": {
                "value": "task-sys",
                "display_value": "PRJ0161219"
            },
            "task.number": "PRJ0161219",
            "task.sys_class_name": "pm_project_task",
            "category": {
                "value": "project_work",
                "display_value": "Project/Project Task"
            },
            "project_time_category": "Development",
            "sunday": "0",
            "monday": monday,
            "tuesday": "0",
            "wednesday": "0",
            "thursday": "0",
            "friday": "0",
            "saturday": "0",
            "total": monday,
            "state": {
                "value": state,
                "display_value": state
            },
            "sys_updated_on": "2026-05-21 10:11:12",
            "sys_mod_count": "3"
        })
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
                        display_value: Some("Casey User".to_string()),
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
                    display_name: "Casey User".to_string(),
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
                display_name: "Casey User".to_string(),
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
        core.ctx
            .query
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

    fn seed_projected_record(core: &SnowCore, record: &SnowRecord) {
        let document = VaultDocument::Record(record.clone());
        let persisted = core
            .persist_runtime_document(&document)
            .expect("persist runtime document");
        let row = record_row_from_runtime_record(
            record,
            Some(persisted.relative_path.clone()),
            serialize_vault_document(&document).to_string(),
        );
        core.ctx
            .query
            .store()
            .upsert_record_with_tags(
                &row,
                &document_work_notes(record),
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

        let store = core.ctx.query.store();
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
    async fn get_record_refreshes_stale_cached_work_record() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/incident"))
            .and(query_param("sysparm_query", "number=INC002"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "inc-projected",
                    "number": "INC002",
                    "short_description": "Live incident title",
                    "description": "Live incident body",
                    "state": "In Progress",
                    "assigned_to": {
                        "value": "user-sys",
                        "display_value": "Casey User"
                    },
                    "sys_updated_on": "2026-04-09 10:11:12",
                    "sys_mod_count": "8",
                    "work_notes": ""
                }]
            })))
            .mount(&server)
            .await;
        mount_empty_journal_fetch(&server, "incident", "inc-projected").await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let mut cached = sample_projected_record();
        cached.synced_at =
            Utc::now() - chrono::Duration::minutes(WORK_RECORD_CACHE_TTL_MINUTES + 1);
        cached.short_description = "Stale cached title".to_string();
        seed_projected_record(&core, &cached);

        let record = core
            .get_record("INC002")
            .await
            .expect("record lookup")
            .expect("record");

        assert_eq!(record.short_description, "Live incident title");
        assert_eq!(record.description, "Live incident body");
        assert!(work_record_cache_is_fresh(
            &record,
            Utc::now(),
            work_record_ttl()
        ));

        let persisted = core
            .ctx
            .query
            .store()
            .get_record_by_number("INC002")
            .expect("persisted row")
            .expect("persisted row");
        assert_eq!(persisted.short_desc.as_deref(), Some("Live incident title"));

        let requests = server.received_requests().await.expect("requests");
        assert!(
            requests
                .iter()
                .any(|request| request.url.path() == "/api/now/table/incident")
        );
    }

    #[tokio::test]
    async fn get_record_by_table_sys_id_fresh_fetches_and_persists_demand() {
        let server = MockServer::start().await;
        let sys_id = "7f029b89c3e7565067bdfd73e40131a1";

        Mock::given(method("GET"))
            .and(path(format!("/api/now/table/dmn_demand/{sys_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": {
                    "sys_id": sys_id,
                    "number": "DMND0320098",
                    "short_description": "Network refresh demand",
                    "description": "Upgrade branch switching",
                    "state": "draft"
                }
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/dmn_demand"))
            .and(query_param("sysparm_display_value", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": sys_id,
                    "work_notes": "",
                    "comments": ""
                }]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let record = core
            .get_record_by_table_sys_id_fresh("dmn_demand", "7F029B89C3E7565067BDFD73E40131A1")
            .await
            .expect("fresh record")
            .expect("record");

        assert_eq!(record.number, "DMND0320098");
        assert_eq!(record.sys_id, sys_id);
        assert_eq!(record.resource_type, ResourceType::Demand);

        let cached = core
            .get_record("DMND0320098")
            .await
            .expect("cached record")
            .expect("persisted record");
        assert_eq!(cached.sys_id, sys_id);

        let requests = server.received_requests().await.expect("requests");
        assert!(
            requests
                .iter()
                .any(|request| request.url.path() == format!("/api/now/table/dmn_demand/{sys_id}"))
        );
    }

    #[tokio::test]
    async fn get_record_by_table_sys_id_fresh_allows_resource_plan() {
        let server = MockServer::start().await;
        let sys_id = "11111111111111111111111111111111";

        Mock::given(method("GET"))
            .and(path(format!("/api/now/table/resource_plan/{sys_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": {
                    "sys_id": sys_id,
                    "number": "RPLN0092386",
                    "short_description": "Identity Access Management plan",
                    "state": "allocated"
                }
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/resource_plan"))
            .and(query_param("sysparm_display_value", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": sys_id,
                    "work_notes": "",
                    "comments": ""
                }]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let record = core
            .get_record_by_table_sys_id_fresh("resource_plan", sys_id)
            .await
            .expect("fresh record")
            .expect("record");

        assert_eq!(record.number, "RPLN0092386");
        assert_eq!(record.resource_type, ResourceType::ResourcePlan);
    }

    #[tokio::test]
    async fn create_resource_plan_round_trips_record() {
        let server = MockServer::start().await;
        let sys_id = "11111111111111111111111111111111";

        Mock::given(method("POST"))
            .and(path("/api/now/table/resource_plan"))
            .and(body_partial_json(serde_json::json!({
                "task": "22222222222222222222222222222222",
                "group_resource": "33333333333333333333333333333333",
                "resource_type": "group",
                "state": "1",
                "planned_hours": 8.0,
                "notes": "Example allocation"
            })))
            .respond_with(ResponseTemplate::new(201).set_body_json(serde_json::json!({
                "result": {
                    "sys_id": sys_id,
                    "number": "<RPLN_NUMBER_CREATE>"
                }
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path(format!("/api/now/table/resource_plan/{sys_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": {
                    "sys_id": sys_id,
                    "number": "<RPLN_NUMBER_CREATE>",
                    "short_description": "Example allocation",
                    "state": { "value": "1", "display_value": "Planning" },
                    "sys_updated_on": "2026-06-24 10:11:12",
                    "sys_mod_count": "1"
                }
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/resource_plan"))
            .and(query_param("sysparm_display_value", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": sys_id,
                    "work_notes": "",
                    "comments": ""
                }]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .create_resource_plan(serde_json::json!({
                "task": "22222222222222222222222222222222",
                "group_resource": "33333333333333333333333333333333",
                "resource_type": "group",
                "state": "1",
                "planned_hours": 8.0,
                "notes": "Example allocation"
            }))
            .await
            .expect("create resource plan");

        assert_eq!(result.record.sys_id, sys_id);
        assert_eq!(result.record.number, "<RPLN_NUMBER_CREATE>");
        assert_eq!(result.concurrency.sys_updated_on, "2026-06-24 10:11:12");
        assert_eq!(result.concurrency.sys_mod_count, Some(1));
    }

    #[tokio::test]
    async fn update_resource_plan_captures_concurrency() {
        let server = MockServer::start().await;
        let sys_id = "44444444444444444444444444444444";

        Mock::given(method("PATCH"))
            .and(path(format!("/api/now/table/resource_plan/{sys_id}")))
            .and(body_partial_json(serde_json::json!({
                "state": "3",
                "planned_hours": 16.0
            })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": {
                    "sys_id": sys_id,
                    "number": "<RPLN_NUMBER_UPDATE>"
                }
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path(format!("/api/now/table/resource_plan/{sys_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": {
                    "sys_id": sys_id,
                    "number": "<RPLN_NUMBER_UPDATE>",
                    "short_description": "Example allocation update",
                    "state": { "value": "3", "display_value": "Allocated" },
                    "planned_hours": "16",
                    "sys_updated_on": "2026-06-24 10:12:13",
                    "sys_mod_count": "24"
                }
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/resource_plan"))
            .and(query_param("sysparm_display_value", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": sys_id,
                    "work_notes": "",
                    "comments": ""
                }]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let result = core
            .update_resource_plan(
                sys_id,
                serde_json::json!({
                    "state": "3",
                    "planned_hours": 16.0
                }),
            )
            .await
            .expect("update resource plan");

        assert_eq!(result.record.sys_id, sys_id);
        assert_eq!(result.record.number, "<RPLN_NUMBER_UPDATE>");
        assert_eq!(result.concurrency.sys_updated_on, "2026-06-24 10:12:13");
        assert_eq!(result.concurrency.sys_mod_count, Some(24));
    }

    #[tokio::test]
    async fn get_record_by_table_sys_id_fresh_allows_demand_task() {
        let server = MockServer::start().await;
        let sys_id = "22222222222222222222222222222222";

        Mock::given(method("GET"))
            .and(path(format!("/api/now/table/dmn_demand_task/{sys_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": {
                    "sys_id": sys_id,
                    "number": "DMNTSK0001122",
                    "short_description": "Review demand intake",
                    "state": "2"
                }
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/dmn_demand_task"))
            .and(query_param("sysparm_display_value", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": sys_id,
                    "work_notes": "",
                    "comments": ""
                }]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let record = core
            .get_record_by_table_sys_id_fresh("dmn_demand_task", sys_id)
            .await
            .expect("fresh record")
            .expect("record");

        assert_eq!(record.number, "DMNTSK0001122");
        assert_eq!(record.resource_type, ResourceType::DemandTask);
    }

    #[tokio::test]
    async fn set_timecard_hours_patches_single_day_and_refetches_without_number() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_user"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "user-sys",
                    "user_name": "test_user",
                    "email": "test@example.com",
                    "name": "Test User"
                }]
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/time_card/card-sys"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": timecard_record_json("6", "Pending")
            })))
            .mount(&server)
            .await;

        Mock::given(method("PATCH"))
            .and(path("/api/now/table/time_card/card-sys"))
            .and(body_partial_json(serde_json::json!({ "monday": "6" })))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": timecard_record_json("6", "Pending")
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server_with_user(&server, "test_user").await;
        let updated = core
            .set_timecard_hours(
                "card-sys",
                Weekday::Mon,
                TimeValue::parse("6.00").unwrap(),
                SetMode::Set,
            )
            .await
            .expect("set timecard hours");

        assert_eq!(updated.sys_id, "card-sys");
        assert_eq!(
            updated.task.as_ref().map(|task| task.number.as_str()),
            Some("PRJ0161219")
        );
        assert_eq!(updated.day_hours(Weekday::Mon), "6");
        assert_eq!(updated.sys_mod_count, Some(3));
    }

    fn time_sheet_row_json(week_starts_on: &str) -> serde_json::Value {
        serde_json::json!({
            "sys_id": format!("sheet-{week_starts_on}"),
            "week_starts_on": week_starts_on,
            "user": { "value": "user-sys", "display_value": "Test User" },
            "user.user_name": "test_user",
            "state": { "value": "Pending", "display_value": "Pending" }
        })
    }

    // Regression: the "current week" selector must find the user's existing
    // sheet via client-side week-range matching, independent of the instance's
    // first-day-of-week. It previously filtered server-side on
    // `week_starts_on=javascript:gs.beginningOfThisWeek()`, which returns a
    // Sunday-based GMT datetime that never equals a Monday-start time sheet's
    // `week_starts_on` date, so Monday-week instances got "no time sheet found".
    #[tokio::test]
    async fn current_week_selects_monday_start_sheet_containing_today() {
        let server = MockServer::start().await;
        // Server returns the user's recent sheets, newest first. All are
        // Monday-start (this instance's policy), so a Sunday-based filter
        // would never match.
        Mock::given(method("GET"))
            .and(path("/api/now/table/time_sheet"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    time_sheet_row_json("2026-05-25"),
                    time_sheet_row_json("2026-05-18"),
                    time_sheet_row_json("2026-05-11"),
                ]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server_with_user(&server, "test_user").await;
        let actor = UserRef {
            sys_id: "user-sys".to_string(),
            user_name: Some("test_user".to_string()),
            email: None,
            display: "Test User".to_string(),
        };
        // Friday in the Monday-start week of 2026-05-18.
        let today = chrono::NaiveDate::from_ymd_opt(2026, 5, 22).unwrap();

        let row = core
            .resolve_my_timecard_sheet_at(WeekSelector::Current, &actor, today)
            .await
            .expect("current sheet should resolve to the week containing today");

        assert_eq!(row.get_str("week_starts_on"), Some("2026-05-18"));
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
            .ctx
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
        core.ctx
            .query
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
        core.ctx
            .query
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
            .ctx
            .query
            .store()
            .get_record_by_number_and_type("INC4908273", ResourceType::Incident)
            .expect("row query")
            .expect("closed incident row");
        assert!(!closed_row.in_scope);
        assert!(closed_row.tombstoned_at.is_some());

        let stale_row = core
            .ctx
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
    async fn get_knowledge_article_fresh_requests_full_body_fields() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/kb_knowledge"))
            .and(query_param("sysparm_query", "number=KB0105015"))
            .and(query_param_contains("sysparm_fields", "article_body"))
            .and(query_param_contains("sysparm_fields", "text"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "kb-fresh-sys",
                    "number": "KB0105015",
                    "short_description": "Fresh KB title",
                    "description": "Fresh KB summary",
                    "article_body": "",
                    "text": "<p>Fresh KB body from text</p>",
                    "state": "published",
                    "workflow_state": "published",
                    "article_type": "text",
                    "published": "2026-04-22 10:00:00",
                    "valid_to": "",
                    "knowledge_base": {
                        "value": "kb-base-sys",
                        "display_value": "Knowledge Base"
                    },
                    "category": {
                        "value": "kb-cat-sys",
                        "display_value": "Standard"
                    },
                    "author": {
                        "value": "user-sys",
                        "display_value": "Knowledge Author"
                    }
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let article = core
            .get_knowledge_article_fresh("KB0105015")
            .await
            .expect("fresh article")
            .expect("article present");

        assert_eq!(article.record.number, "KB0105015");
        assert_eq!(article.content, "<p>Fresh KB body from text</p>");
        assert!(article.body_cached);
    }

    #[tokio::test]
    async fn get_knowledge_article_cached_or_fresh_repairs_missing_body() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/kb_knowledge"))
            .and(query_param("sysparm_query", "number=KB0105015"))
            .and(query_param_contains("sysparm_fields", "article_body"))
            .and(query_param_contains("sysparm_fields", "text"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "kb-body-miss-sys",
                    "number": "KB0105015",
                    "short_description": "Cached shell",
                    "description": "Cached summary only",
                    "article_body": "",
                    "text": "<p>Recovered KB body</p>",
                    "state": "published",
                    "workflow_state": "published",
                    "article_type": "text",
                    "published": "2026-04-22 10:00:00",
                    "knowledge_base": {
                        "value": "kb-base-sys",
                        "display_value": "Knowledge Base"
                    },
                    "category": {
                        "value": "kb-cat-sys",
                        "display_value": "Standard"
                    }
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let metadata_only_record = Record::from_json(
            "kb_knowledge",
            &serde_json::json!({
                "sys_id": "kb-body-miss-sys",
                "number": "KB0105015",
                "short_description": "Cached shell",
                "description": "Cached summary only",
                "state": "published",
                "workflow_state": "published",
                "article_type": "text",
                "published": "2026-04-22 10:00:00",
                "knowledge_base": {
                    "value": "kb-base-sys",
                    "display_value": "Knowledge Base"
                },
                "category": {
                    "value": "kb-cat-sys",
                    "display_value": "Standard"
                }
            }),
            DisplayValue::Both,
        )
        .expect("metadata-only knowledge record");
        core.persist_record(&metadata_only_record)
            .expect("persist metadata-only knowledge record");

        let cached = core
            .get_knowledge_article("KB0105015")
            .await
            .expect("cached article")
            .expect("cached article present");
        assert!(!cached.body_cached);

        let article = core
            .get_knowledge_article_cached_or_fresh("KB0105015")
            .await
            .expect("cached-or-fresh article")
            .expect("article present");

        assert_eq!(article.content, "<p>Recovered KB body</p>");
        assert!(article.body_cached);
    }

    #[tokio::test]
    async fn get_knowledge_article_cached_or_fresh_falls_back_on_live_error() {
        let server = MockServer::start().await;
        let (core, _tempdir) = core_for_mock_server(&server).await;
        let metadata_only_record = Record::from_json(
            "kb_knowledge",
            &serde_json::json!({
                "sys_id": "kb-body-miss-sys",
                "number": "KB0105015",
                "short_description": "Cached shell",
                "description": "Cached summary only",
                "state": "published",
                "workflow_state": "published",
                "article_type": "text"
            }),
            DisplayValue::Both,
        )
        .expect("metadata-only knowledge record");
        core.persist_record(&metadata_only_record)
            .expect("persist metadata-only knowledge record");

        let article = core
            .get_knowledge_article_cached_or_fresh("KB0105015")
            .await
            .expect("cached article despite live repair failure")
            .expect("cached article present");

        assert_eq!(article.record.number, "KB0105015");
        assert!(!article.body_cached);
        assert_eq!(article.record.description, "Cached summary only");
        assert!(article.content.is_empty());
    }

    #[tokio::test]
    async fn get_knowledge_article_cached_or_fresh_marks_empty_full_body_as_cached() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/kb_knowledge"))
            .and(query_param("sysparm_query", "number=KB0105016"))
            .and(query_param_contains("sysparm_fields", "article_body"))
            .and(query_param_contains("sysparm_fields", "text"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "kb-empty-body-sys",
                    "number": "KB0105016",
                    "short_description": "Cached shell",
                    "description": "Cached summary only",
                    "article_body": "",
                    "text": "",
                    "state": "published",
                    "workflow_state": "published",
                    "article_type": "text",
                    "published": "2026-04-22 10:00:00"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let metadata_only_record = Record::from_json(
            "kb_knowledge",
            &serde_json::json!({
                "sys_id": "kb-empty-body-sys",
                "number": "KB0105016",
                "short_description": "Cached shell",
                "description": "Cached summary only",
                "state": "published",
                "workflow_state": "published",
                "article_type": "text"
            }),
            DisplayValue::Both,
        )
        .expect("metadata-only knowledge record");
        core.persist_record(&metadata_only_record)
            .expect("persist metadata-only knowledge record");

        let repaired = core
            .get_knowledge_article_cached_or_fresh("KB0105016")
            .await
            .expect("repaired article")
            .expect("article present");
        assert!(repaired.body_cached);
        assert!(repaired.content.is_empty());

        let cached = core
            .get_knowledge_article_cached_or_fresh("KB0105016")
            .await
            .expect("cached article")
            .expect("article present");
        assert!(cached.body_cached);
        assert!(cached.content.is_empty());
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

        let store = core.ctx.query.store();
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
                    "notes": { "value": "Resource plan notes", "display_value": "Resource plan notes" },
                    "state": { "value": "3", "display_value": "Allocated" },
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
        assert_eq!(
            children[0]
                .fields
                .get("notes")
                .map(|field| field.value.as_str()),
            Some("Resource plan notes")
        );

        let requests = server.received_requests().await.expect("requests");
        let request = requests
            .iter()
            .find(|request| request.url.path() == "/api/now/table/resource_plan")
            .expect("resource plan request");
        let query = request
            .url
            .query_pairs()
            .collect::<std::collections::HashMap<_, _>>();
        let fields = query
            .get("sysparm_fields")
            .expect("resource_plan sysparm_fields");
        let requested_fields = fields.split(',').collect::<std::collections::HashSet<_>>();
        assert!(requested_fields.contains("notes"));
        assert!(!requested_fields.contains("description"));
    }

    #[tokio::test]
    async fn resource_plan_list_queries_task_and_state_in_once() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let task_sys_id = "00000000000000000000000000000010";
        let group_sys_id = "00000000000000000000000000000020";

        Mock::given(method("GET"))
            .and(path("/api/now/table/resource_plan"))
            .and(query_param_contains(
                "sysparm_query",
                format!("task={task_sys_id}"),
            ))
            .and(query_param_contains("sysparm_query", "stateIN1,3"))
            .and(query_param_contains(
                "sysparm_query",
                format!("group_resource={group_sys_id}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": { "value": "00000000000000000000000000000001" },
                    "number": { "value": "<RPLN_NUMBER>" },
                    "state": { "value": "3", "display_value": "Allocated" },
                    "task": { "value": task_sys_id },
                    "task.number": { "value": "<PRJ_NUMBER>" },
                    "task.sys_class_name": { "value": "pm_project" },
                    "resource_type": { "value": "group" },
                    "group_resource": {
                        "value": group_sys_id,
                        "display_value": "<GROUP_DISPLAY>"
                    },
                    "planned_hours": { "value": "32" },
                    "notes": { "value": "<NOTES>" },
                    "u_description": { "value": "<CONTEXT>" }
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let resp = core
            .resource_plan_list(ResourcePlanListInput {
                task_sys_id: Some(task_sys_id.to_string()),
                resource_sys_id: Some(group_sys_id.to_string()),
                resource_type: Some(ResourcePlanResourceType::Group),
                state: Some(ResourcePlanStateFilter::Multiple(vec![1, 3])),
                ..Default::default()
            })
            .await
            .expect("resource_plan_list");

        assert_eq!(resp.records.len(), 1);
        assert_eq!(resp.records[0].state.as_deref(), Some("3"));
        assert_eq!(resp.records[0].state_label.as_deref(), Some("Allocated"));
        assert_eq!(resp.records[0].notes.as_deref(), Some("<NOTES>"));
        assert_eq!(resp.records[0].context.as_deref(), Some("<CONTEXT>"));
        assert_eq!(
            resp.records[0]
                .parent
                .as_ref()
                .and_then(|parent| parent.table.as_deref()),
            Some("pm_project")
        );
        assert_eq!(resp.query_summary.total_returned, 1);
        assert!(!resp.query_summary.truncated);
        assert!(
            resp.query_summary
                .filters_applied
                .contains(&"task_sys_id".to_string())
        );
        assert!(
            resp.query_summary
                .filters_applied
                .contains(&"resource_sys_id".to_string())
        );
        assert!(
            resp.query_summary
                .filters_applied
                .contains(&"state".to_string())
        );

        let requests = server.received_requests().await.expect("requests");
        let resource_plan_requests = requests
            .iter()
            .filter(|request| request.url.path() == "/api/now/table/resource_plan")
            .collect::<Vec<_>>();
        assert_eq!(resource_plan_requests.len(), 1);
        let query = resource_plan_requests[0]
            .url
            .query_pairs()
            .collect::<std::collections::HashMap<_, _>>();
        let fields = query
            .get("sysparm_fields")
            .expect("resource_plan sysparm_fields");
        let requested_fields = fields.split(',').collect::<std::collections::HashSet<_>>();
        assert!(requested_fields.contains("notes"));
        assert!(requested_fields.contains("u_description"));
        assert!(requested_fields.contains("task.number"));
        assert!(requested_fields.contains("task.sys_class_name"));
        assert!(!requested_fields.contains("description"));
    }

    #[tokio::test]
    async fn resource_plan_list_resolves_parent_number_to_task_sys_id() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let parent_number = "PRJ_PLACEHOLDER";
        let parent_sys_id = "00000000000000000000000000000010";

        Mock::given(method("GET"))
            .and(path("/api/now/table/pm_project"))
            .and(query_param_contains(
                "sysparm_query",
                format!("number={parent_number}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": parent_sys_id,
                    "number": parent_number
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/resource_plan"))
            .and(query_param_contains(
                "sysparm_query",
                format!("task={parent_sys_id}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let resp = core
            .resource_plan_list(ResourcePlanListInput {
                parent_number: Some(parent_number.to_string()),
                ..Default::default()
            })
            .await
            .expect("resource_plan_list");

        assert_eq!(resp.records.len(), 0);
        assert!(
            resp.query_summary
                .filters_applied
                .contains(&"parent_number".to_string())
        );
    }

    #[tokio::test]
    async fn resource_plan_list_marks_truncated_when_rows_equal_limit() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let row_one = serde_json::json!({
            "sys_id": { "value": "00000000000000000000000000000001" },
            "number": { "value": "<RPLN_NUMBER_1>" },
            "state": { "value": "1", "display_value": "Planning" }
        });
        let row_two = serde_json::json!({
            "sys_id": { "value": "00000000000000000000000000000002" },
            "number": { "value": "<RPLN_NUMBER_2>" },
            "state": { "value": "3", "display_value": "Allocated" }
        });

        Mock::given(method("GET"))
            .and(path("/api/now/table/resource_plan"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [row_one, row_two]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let resp = core
            .resource_plan_list(ResourcePlanListInput {
                limit: Some(2),
                ..Default::default()
            })
            .await
            .expect("resource_plan_list");

        assert_eq!(resp.records.len(), 2);
        assert!(resp.query_summary.truncated);
        assert_eq!(resp.query_summary.limit, 2);
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
            portal: String::new(),
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
                        "display_value": "Casey User"
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
    async fn search_enriched_falls_back_to_live_fetch_for_demand_task_number() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/dmn_demand_task"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "dmntsk-sys-fallback",
                    "number": "DMNTSK0001122",
                    "short_description": "Review demand intake",
                    "description": "Demand task should hydrate from exact search",
                    "state": "2",
                    "parent": {
                        "value": "demand-parent-sys",
                        "display_value": "DMND0002002"
                    }
                }]
            })))
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/dmn_demand_task"))
            .and(query_param("sysparm_display_value", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "dmntsk-sys-fallback",
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
            .search_enriched("DMNTSK0001122", SearchScope::All)
            .await
            .expect("search");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].record.number, "DMNTSK0001122");
        assert_eq!(results[0].record.table, "dmn_demand_task");

        let record = core
            .get_record("DMNTSK0001122")
            .await
            .expect("cached record")
            .expect("record");
        assert_eq!(record.resource_type, ResourceType::DemandTask);
        assert_eq!(record.table, "dmn_demand_task");
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
                    "assigned_to": { "value": "user-sys", "display_value": "Casey User" },
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
                    "work_notes": "2026-04-10 09:15:00 - Casey User (Work notes)\nCurrent status: Smart hand ticket has been created for the FS to get the switch details.\n\n",
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
        assert_eq!(record.work_notes[0].author, "Casey User");
        assert_eq!(
            record
                .fields
                .get("assigned_to")
                .and_then(|field| field.display_value.as_deref()),
            Some("Casey User")
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
