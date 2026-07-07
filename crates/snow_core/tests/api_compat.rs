// Compile-time contract: every public type from snow_core must remain
// importable at its existing path after any migration step.
//
// Imports only — no runtime assertions. A missing re-export causes this
// file to fail to compile, which fails `cargo test --workspace`.
//
// choose_reference_display_name and is_opaque_sys_id are promoted from
// pub(crate) to pub by Task 3; they are correctly listed here.
#![allow(unused_imports)]

use snow_core::{
    // Public constants
    APPROVAL_GROUP_IN_BATCH_SIZE,
    // Approval response types (inline in lib.rs)
    ApprovalQuerySummary, // lib.rs:787
    // Approval types (inline in lib.rs)
    ApprovalRecord,
    ApprovalRoutedVia, // lib.rs:775
    // Attachment (lib.rs:86)
    AttachmentMetadata,
    BUSINESS_APPLICATION_CI_OWNER_GROUP_RAW_FIELD,
    BUSINESS_APPLICATION_DEGRADED_REASON_CMDB_RELATIONSHIPS_UNMAPPED,
    BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_CIS,
    BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_DEPTH,
    BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_EDGES,
    BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_SERVICE_MEMBERSHIP_ASSOCIATIONS,
    BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_SERVICE_MEMBERSHIP_PAGES,
    BUSINESS_APPLICATION_SERVERS_DEFAULT_RELATIONSHIP_TYPES,
    BUSINESS_APPLICATION_SERVERS_MAX_CIS,
    BUSINESS_APPLICATION_SERVERS_MAX_DEPTH,
    BUSINESS_APPLICATION_SERVERS_MAX_EDGES,
    BUSINESS_APPLICATION_SERVERS_MAX_SERVICE_MEMBERSHIP_ASSOCIATIONS,
    BUSINESS_APPLICATION_SERVERS_MAX_SERVICE_MEMBERSHIP_PAGES,
    BUSINESS_APPLICATION_SERVICE_DISCOVERY_RELATIONSHIP_TYPES,
    // Business Application types (lib.rs:36) — key types; catches most regressions
    BusinessApplication,
    BusinessApplicationFieldAliases,
    BusinessApplicationFieldValue,
    BusinessApplicationHydrationOptions,
    BusinessApplicationLookup,
    BusinessApplicationRelationshipDirection,
    BusinessApplicationRelationshipType,
    // BusinessApplication search params
    BusinessApplicationSearchParams,
    BusinessApplicationServerApplication,
    BusinessApplicationServerInventoryHealth,
    BusinessApplicationServerPath,
    BusinessApplicationServerPathEdge,
    BusinessApplicationServerPathEdgeSource,
    BusinessApplicationServerProvenance,
    BusinessApplicationServersCachedOptions,
    BusinessApplicationServersCachedParams,
    BusinessApplicationServersCachedResult,
    BusinessApplicationServersCachedSelector,
    BusinessApplicationServersOptions,
    BusinessApplicationServersParams,
    BusinessApplicationServersResult,
    BusinessApplicationServersSelector,
    BusinessApplicationServersSummary,
    BusinessApplicationSyncSummary,
    BusinessApplicationsForServerOptions,
    BusinessApplicationsForServerParams,
    BusinessApplicationsForServerResult,
    BusinessApplicationsForServerSelector,
    CacheSource,
    // Cache policy (lib.rs:24)
    CacheTtlPolicy,
    CachedBusinessApplicationForServer,
    CachedBusinessApplicationServer,
    CachedServerBusinessApplications,
    // Timecard (lib.rs:82)
    CardSelector,
    // Catalog (lib.rs:67)
    CatalogChoice,
    CatalogItem,
    CatalogSubmitResult,
    CatalogVariable,
    // Change (lib.rs:68)
    ChangeWriteConcurrency,
    ChangeWriteResult,
    ChoiceValue,
    CiOwnerGroupRef,
    // credential types also re-exported at root (lib.rs:29)
    CredentialError,
    CredentialProvider,
    DegradedReadDiagnostic,
    DegradedReadReason,
    EndpointResolutionStatus,
    FallbackStrategy,
    FieldChoice,
    FieldValue,
    JournalEntry,
    // Knowledge search/semantic types (inline in lib.rs)
    KnowledgeArticle,
    KnowledgeBaseSummary,
    KnowledgeCategorySummary,
    KnowledgeEmbeddingCoverage, // lib.rs:702
    KnowledgeSearchFilters,
    KnowledgeSearchHit,
    KnowledgeSearchMode,
    KnowledgeSemanticSearchFilters, // lib.rs:687
    KnowledgeSemanticStatus,
    // Knowledge module types re-exported from kb module (lib.rs:31)
    KnowledgeStatus,
    KnowledgeSyncMode,
    KnowledgeSyncOutcome,
    KnowledgeTagLayer,
    KnowledgeTagSummary,
    LINUX_SERVER_TABLE,
    ListMyApprovalsResponse, // lib.rs:797
    MatchField,
    OrphanPruneReport,
    OrphanRecordRow,
    RECORD_LOOKUP_ALLOWED_TABLES,
    RebuildReport,
    RecordLookup,
    RecordRef,
    // Types promoted to pub from servicenow_rs in Tasks 2-4
    Reference,
    ReferencePrimitiveDescriptor,
    ReferencePrimitiveType,
    ReferenceResolutionDiagnostic,
    ReferenceResolutionReason,
    ReferenceResolutionStatus,
    RelationshipKnowledgeStatus,
    // Vault report types (inline in lib.rs:815-862)
    RepairReport,
    // Resource Plan (lib.rs:69)
    ResolvedResourceFilter,
    ResourcePlanListError,
    ResourcePlanListInput,
    ResourcePlanListResponse,
    ResourcePlanListWarning,
    ResourcePlanParentRef,
    ResourcePlanParentType,
    ResourcePlanQuerySummary,
    ResourcePlanRecord,
    ResourcePlanResource,
    ResourcePlanResourceRef,
    ResourcePlanResourceType,
    ResourcePlanState,
    ResourcePlanStateFilter,
    ResourcePlanWriteConcurrency,
    ResourcePlanWriteResult,
    ResourceType,
    SERVER_RESOURCE_TYPE,
    SERVER_TABLE,
    SERVER_TABLES,
    STABLE_REFERENCE_CACHE_TTL_DAYS,
    SearchMatchReason,
    SearchResult,
    SearchScope,
    SemanticIndexSummary, // lib.rs:737
    // Server (lib.rs:77)
    Server,
    // Server error type
    ServerGetError,
    ServerLookup,
    ServerQuery,
    ServerResultSource,
    ServerSearchParams,
    SetMode,
    SimpleRef,
    // Top-level structs and builders
    SnowCore,
    SnowCoreBuilder,
    // Core record types (inline in lib.rs)
    SnowRecord,
    // Story (lib.rs:81)
    StoryWriteConcurrency,
    StoryWriteResult,
    TaskSelector,
    // SLA (lib.rs:87)
    TaskSlaParentRef,
    TaskSlaReadability,
    TaskSlaStatus,
    TaskSlaSummaryView,
    TaskSlaView,
    TimeCard,
    TimeValue,
    TimecardSheet,
    UnindexedVaultDocument,
    // User
    UserLookup,
    UserLookupResult,
    UserRecord,
    UserRef,
    UserSearch,
    ValidatedListQuery,
    VaultVerificationReport,
    WINDOWS_SERVER_TABLE,
    WORK_RECORD_CACHE_TTL_MINUTES,
    WeekSelector,
    Weekday,
    choose_reference_display_name,
    // credential module shim (path import) — ALIASED because the same names
    // are imported unaliased from the crate root below; importing both paths
    // unaliased in one scope is E0252 (duplicate definition), a hard error.
    credential::CredentialError as CredentialErrorViaModulePath,
    credential::CredentialProvider as CredentialProviderViaModulePath,
    credential::SecretString, // module-path only; not re-exported at root
    is_opaque_sys_id,
    is_record_lookup_table_allowed,
    is_task_sla_applicable_table,
    // Public helper functions
    normalize_record_lookup_sys_id,
    normalize_record_lookup_table,
    resource_plan_record_from_row,
    stable_reference_ttl,
    table_for_builtin_record_number,
    validate_list_input,
    work_record_ttl,
};

// Public MODULE PATHS — external callers also import through these; the
// migration must not privatize a public module while keeping only the root
// re-export. Underscore imports (`as _`) prove a path resolves without
// binding a name, so they cannot collide with the root imports above.
//
// COMPLETE enumeration of snow_core's `pub mod` surface (lib.rs:5-20) plus
// every resource submodule (resource/mod.rs — all eleven are pub):
use snow_core::cache as _;
use snow_core::config as _;
use snow_core::credential as _;
use snow_core::display as _;
use snow_core::enrich as _;
use snow_core::ipc as _;
use snow_core::kb as _;
use snow_core::query as _;
use snow_core::refresh as _;
use snow_core::resource as _;
use snow_core::resource::approval as _;
use snow_core::resource::business_application as _;
use snow_core::resource::catalog as _;
use snow_core::resource::change as _;
use snow_core::resource::incident as _;
use snow_core::resource::knowledge as _;
use snow_core::resource::request as _;
use snow_core::resource::resource_plan as _;
use snow_core::resource::server as _;
use snow_core::resource::story as _;
use snow_core::resource::timecard as _;
use snow_core::sla as _;
use snow_core::vault as _;
// Representative in-module item spot-checks (module exists AND still holds
// its key symbols — items inside modules are NOT exhaustively enumerated):
use snow_core::cache::policy::CacheTtlPolicy as _;
use snow_core::config::SnowConfig as _;
use snow_core::kb::KnowledgeStatus as _;
use snow_core::resource::business_application::BusinessApplication as _;
use snow_core::resource::resource_plan::ResourcePlanListInput as _;
use snow_core::resource::server::{SERVER_TABLES as _, Server as _};
use snow_core::resource::timecard::UserRef as _;

/// Never executed. Compiles only while these SnowCore method signatures hold —
/// a delegation wrapper that drifts (wrong parameter or return type) breaks
/// this file even though no test calls the method at runtime. These are the
/// migration-critical methods; the exhaustive method check is snow_mcp /
/// snow_daemon / snow_cli compiling unchanged (Global Constraint).
#[allow(dead_code)]
async fn method_signature_witness(
    core: SnowCore,
    plan_input: ResourcePlanListInput,
    user_lookup: UserLookup,
    when: chrono::DateTime<chrono::Utc>,
) -> anyhow::Result<()> {
    let _: ResourcePlanListResponse = core.resource_plan_list(plan_input).await?;
    let _: Option<SnowRecord> = core
        .get_record_by_table_sys_id_fresh("incident", "0")
        .await?;
    let _: Option<SnowRecord> = core.get_record_fresh("INC0000001").await?;
    let _: String = core.current_user_sys_id().await?;
    let _: Option<UserLookupResult> = core.lookup_user(user_lookup).await?;
    let _: Vec<ApprovalRecord> = core.my_approvals().await?;
    core.tombstone_record("0", when)?;
    core.prune_record("0", when).await?;
    Ok(())
}

#[test]
fn api_surface_compiles() {
    // Intentionally empty — compilation is the test.
}
