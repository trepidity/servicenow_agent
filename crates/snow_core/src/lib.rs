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
pub use resource::incident::{
    INCIDENT_GROUP_LIST_DEFAULT_LIMIT, INCIDENT_GROUP_LIST_MAX_LIMIT,
    IncidentAssignmentGroupListError, IncidentAssignmentGroupListInput,
    IncidentAssignmentGroupPage, ResolvedIncidentState, ValidatedIncidentAssignmentGroupQuery,
    resolve_incident_state, validate_incident_assignment_group_input,
};
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
pub use service::business_application::BusinessApplicationSearchParams;
pub use service::server::ServerGetError;
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

pub(crate) use service::knowledge::normalize_knowledge_article;
pub use service::knowledge::{
    KnowledgeArticle, KnowledgeBaseSummary, KnowledgeCategorySummary, KnowledgeEmbeddingCoverage,
    KnowledgeSearchFilters, KnowledgeSearchHit, KnowledgeSearchMode,
    KnowledgeSemanticSearchFilters, KnowledgeSemanticStatus, SemanticIndexSummary,
};
pub use service::record::{
    RECORD_LOOKUP_ALLOWED_TABLES, is_record_lookup_table_allowed, normalize_record_lookup_sys_id,
    normalize_record_lookup_table, table_for_builtin_record_number,
};
pub(crate) use service::record::{canonical_record_table, canonical_record_table_for_number};

mod facade;
pub use facade::SnowCoreBuilder;

#[derive(Clone)]
pub struct SnowCore {
    ctx: context::CoreContext,
    users: service::UserService,
    approvals: service::ApprovalService,
    business_applications: service::BusinessApplicationService,
    servers: service::ServerService,
    records: service::RecordService,
    knowledge: service::KnowledgeService,
    vault_svc: service::VaultService,
    writes: service::WriteService,
}

mod tests;
