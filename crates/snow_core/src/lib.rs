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

use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use servicenow_rs::prelude::ServiceNowClient;

use crate::cache::store::BusinessApplicationFieldDictionaryRow;
use crate::query::filter::{BusinessApplicationQuery, ListQuery};
use crate::vault::manager::VaultManager;

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
        self.business_applications
            .search_business_applications(params)
            .await
    }

    pub async fn get_business_application_fresh(
        &self,
        lookup: BusinessApplicationLookup,
        options: BusinessApplicationHydrationOptions,
    ) -> Result<Option<BusinessApplication>> {
        self.business_applications
            .get_business_application_fresh(lookup, options)
            .await
    }

    pub async fn search_business_applications_live(
        &self,
        params: BusinessApplicationSearchParams,
        options: BusinessApplicationHydrationOptions,
    ) -> Result<Vec<BusinessApplication>> {
        self.business_applications
            .search_business_applications_live(params, options)
            .await
    }

    pub async fn query_business_applications(
        &self,
        query: BusinessApplicationQuery,
    ) -> Result<Vec<SnowRecord>> {
        self.business_applications
            .query_business_applications(query)
            .await
    }

    pub async fn business_application_servers(
        &self,
        params: BusinessApplicationServersParams,
    ) -> Result<Option<BusinessApplicationServersResult>> {
        self.business_applications
            .business_application_servers(params)
            .await
    }

    pub async fn get_business_application_servers(
        &self,
        params: BusinessApplicationServersParams,
    ) -> Result<Option<BusinessApplicationServersResult>> {
        self.business_applications
            .get_business_application_servers(params)
            .await
    }

    pub async fn sync_business_applications(
        &self,
        params: Option<BusinessApplicationSearchParams>,
        options: BusinessApplicationHydrationOptions,
    ) -> Result<BusinessApplicationSyncSummary> {
        self.business_applications
            .sync_business_applications(params, options)
            .await
    }

    pub async fn sync_all_business_applications(
        &self,
        options: BusinessApplicationHydrationOptions,
    ) -> Result<BusinessApplicationSyncSummary> {
        self.business_applications
            .sync_all_business_applications(options)
            .await
    }

    pub async fn business_application_dictionary(
        &self,
    ) -> Result<HashMap<String, BusinessApplicationFieldDictionaryRow>> {
        self.business_applications
            .business_application_dictionary()
            .await
    }

    pub async fn business_application_aliases(&self) -> Result<BusinessApplicationFieldAliases> {
        self.business_applications
            .business_application_aliases()
            .await
    }

    pub async fn refresh_business_application_dictionary(&self) -> Result<usize> {
        self.business_applications
            .refresh_business_application_dictionary()
            .await
    }

    pub async fn get_server_fresh(&self, lookup: ServerLookup) -> Result<Option<Server>> {
        self.servers.get_server_fresh(lookup).await
    }

    pub async fn get_server_live(
        &self,
        lookup: ServerLookup,
        persist: bool,
    ) -> std::result::Result<Option<Server>, ServerGetError> {
        self.servers.get_server_live(lookup, persist).await
    }

    pub async fn search_servers(&self, params: ServerSearchParams) -> Result<Vec<SnowRecord>> {
        self.servers.search_servers(params).await
    }

    pub async fn search_servers_live(&self, params: ServerSearchParams) -> Result<Vec<Server>> {
        self.servers.search_servers_live(params).await
    }

    pub async fn query_servers(&self, query: ServerQuery) -> Result<Vec<SnowRecord>> {
        self.servers.query_servers(query).await
    }

    pub async fn business_application_servers_cached(
        &self,
        params: BusinessApplicationServersCachedParams,
    ) -> Result<Option<BusinessApplicationServersCachedResult>> {
        self.servers
            .business_application_servers_cached(params)
            .await
    }

    pub async fn business_applications_for_server(
        &self,
        params: BusinessApplicationsForServerParams,
    ) -> Result<Option<BusinessApplicationsForServerResult>> {
        self.servers.business_applications_for_server(params).await
    }

    pub async fn get_record(&self, number: &str) -> Result<Option<SnowRecord>> {
        self.records.get_record(number).await
    }

    pub async fn get_record_fresh(&self, number: &str) -> Result<Option<SnowRecord>> {
        self.records.get_record_fresh(number).await
    }

    pub async fn get_record_by_lookup_fresh(
        &self,
        lookup: RecordLookup,
    ) -> Result<Option<SnowRecord>> {
        self.records.get_record_by_lookup_fresh(lookup).await
    }

    pub async fn get_record_by_table_sys_id_fresh(
        &self,
        table: &str,
        sys_id: &str,
    ) -> Result<Option<SnowRecord>> {
        self.records
            .get_record_by_table_sys_id_fresh(table, sys_id)
            .await
    }

    pub fn tombstone_record(&self, sys_id: &str, when: DateTime<Utc>) -> Result<()> {
        self.records.tombstone_record(sys_id, when)
    }

    pub async fn prune_record(&self, sys_id: &str, when: DateTime<Utc>) -> Result<()> {
        self.records.prune_record(sys_id, when).await
    }

    pub async fn get_knowledge_article(&self, number: &str) -> Result<Option<KnowledgeArticle>> {
        self.knowledge.get_knowledge_article(number).await
    }

    pub async fn get_knowledge_article_cached_or_fresh(
        &self,
        number: &str,
    ) -> Result<Option<KnowledgeArticle>> {
        self.knowledge
            .get_knowledge_article_cached_or_fresh(number)
            .await
    }

    pub async fn search_knowledge(
        &self,
        query: &str,
        filters: KnowledgeSearchFilters,
    ) -> Result<Vec<KnowledgeArticle>> {
        self.knowledge.search_knowledge(query, filters).await
    }

    pub async fn search_knowledge_semantic(
        &self,
        query: &str,
        filters: KnowledgeSemanticSearchFilters,
    ) -> Result<Vec<KnowledgeSearchHit>> {
        self.knowledge
            .search_knowledge_semantic(query, filters)
            .await
    }

    pub async fn knowledge_semantic_status(&self) -> Result<KnowledgeSemanticStatus> {
        self.knowledge.knowledge_semantic_status().await
    }

    pub async fn rebuild_knowledge_semantic_index(
        &self,
        full: bool,
    ) -> Result<SemanticIndexSummary> {
        self.knowledge.rebuild_knowledge_semantic_index(full).await
    }

    pub fn list_knowledge_bases(&self) -> Result<Vec<KnowledgeBaseSummary>> {
        self.knowledge.list_knowledge_bases()
    }

    pub fn list_categories(
        &self,
        knowledge_base_sys_id: &str,
    ) -> Result<Vec<KnowledgeCategorySummary>> {
        self.knowledge.list_categories(knowledge_base_sys_id)
    }

    pub async fn list_knowledge_articles(
        &self,
        knowledge_base_sys_id: Option<&str>,
        category_sys_id: Option<&str>,
        limit: Option<usize>,
    ) -> Result<Vec<KnowledgeArticle>> {
        self.knowledge
            .list_knowledge_articles(knowledge_base_sys_id, category_sys_id, limit)
            .await
    }

    pub async fn get_approval(&self, number: &str) -> Result<Option<ApprovalRecord>> {
        self.approvals.get_approval(number).await
    }

    pub fn degraded_reads(&self) -> Vec<DegradedReadDiagnostic> {
        self.records.degraded_reads()
    }

    pub async fn repair_missing_vault_files(&self) -> Result<usize> {
        self.vault_svc.repair_missing_vault_files().await
    }

    pub async fn repair_vault(&self) -> Result<RepairReport> {
        self.vault_svc.repair_vault().await
    }

    pub fn rebuild_cache_from_vault(&self) -> Result<usize> {
        self.vault_svc.rebuild_cache_from_vault()
    }

    pub fn rebuild_cache(&self) -> Result<RebuildReport> {
        self.vault_svc.rebuild_cache()
    }

    pub fn verify_vault(&self) -> Result<VaultVerificationReport> {
        self.vault_svc.verify_vault()
    }

    pub async fn prune_orphans(&self, dry_run: bool) -> Result<OrphanPruneReport> {
        self.vault_svc.prune_orphans(dry_run).await
    }

    pub async fn get_children(&self, number: &str) -> Result<Vec<SnowRecord>> {
        self.records.get_children(number).await
    }

    pub async fn resource_plan_list(
        &self,
        input: ResourcePlanListInput,
    ) -> Result<ResourcePlanListResponse> {
        self.records.resource_plan_list(input).await
    }

    pub async fn list_records(&self) -> Result<Vec<SnowRecord>> {
        self.records.list_records().await
    }

    pub async fn list_records_query(&self, query: ListQuery) -> Result<Vec<SnowRecord>> {
        self.records.list_records_query(query).await
    }

    pub async fn my_tasks(&self) -> Result<Vec<SnowRecord>> {
        self.records.my_tasks().await
    }

    pub async fn current_user_sys_id(&self) -> Result<String> {
        self.records.current_user_sys_id().await
    }

    pub async fn list_my_timecards(&self, week: WeekSelector) -> Result<TimecardSheet> {
        self.writes.list_my_timecards(week).await
    }

    pub async fn get_timecard_fresh(&self, sys_id: &str) -> Result<Option<TimeCard>> {
        self.writes.get_timecard_fresh(sys_id).await
    }

    pub async fn set_timecard_hours(
        &self,
        sys_id: &str,
        day: Weekday,
        hours: TimeValue,
        mode: SetMode,
    ) -> Result<TimeCard> {
        self.writes
            .set_timecard_hours(sys_id, day, hours, mode)
            .await
    }

    pub async fn my_tasks_fresh(&self) -> Result<Vec<SnowRecord>> {
        self.records.my_tasks_fresh().await
    }

    pub async fn my_stories_fresh(&self) -> Result<Vec<SnowRecord>> {
        self.records.my_stories_fresh().await
    }

    pub async fn my_incidents_fresh(&self) -> Result<Vec<SnowRecord>> {
        self.records.my_incidents_fresh().await
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
        self.records.my_projects().await
    }

    pub async fn my_projects_fresh(&self) -> Result<Vec<SnowRecord>> {
        self.records.my_projects_fresh().await
    }

    pub async fn search(&self, query: &str, scope: SearchScope) -> Result<Vec<SearchResult>> {
        self.records.search(query, scope).await
    }

    pub async fn search_by_tag(&self, tag: &str, scope: SearchScope) -> Result<Vec<SearchResult>> {
        self.records.search_by_tag(tag, scope).await
    }

    pub async fn search_by_keyword(
        &self,
        keyword: &str,
        scope: SearchScope,
    ) -> Result<Vec<SearchResult>> {
        self.records.search_by_keyword(keyword, scope).await
    }

    pub async fn search_by_alias(
        &self,
        alias: &str,
        scope: SearchScope,
    ) -> Result<Vec<SearchResult>> {
        self.records.search_by_alias(alias, scope).await
    }

    pub async fn search_enriched(
        &self,
        query: &str,
        scope: SearchScope,
    ) -> Result<Vec<SearchResult>> {
        self.records.search_enriched(query, scope).await
    }

    pub async fn create_rm_story(&self, payload: serde_json::Value) -> Result<StoryWriteResult> {
        self.writes.create_rm_story(payload).await
    }

    pub async fn update_rm_story(
        &self,
        sys_id: &str,
        payload: serde_json::Value,
    ) -> Result<StoryWriteResult> {
        self.writes.update_rm_story(sys_id, payload).await
    }

    pub async fn create_rm_scrum_task(
        &self,
        payload: serde_json::Value,
    ) -> Result<StoryWriteResult> {
        self.writes.create_rm_scrum_task(payload).await
    }

    pub async fn update_rm_scrum_task(
        &self,
        sys_id: &str,
        payload: serde_json::Value,
    ) -> Result<StoryWriteResult> {
        self.writes.update_rm_scrum_task(sys_id, payload).await
    }

    pub async fn create_change_request(
        &self,
        payload: serde_json::Value,
    ) -> Result<ChangeWriteResult> {
        self.writes.create_change_request(payload).await
    }

    pub async fn update_change_request(
        &self,
        sys_id: &str,
        payload: serde_json::Value,
    ) -> Result<ChangeWriteResult> {
        self.writes.update_change_request(sys_id, payload).await
    }

    pub async fn create_change_task(
        &self,
        payload: serde_json::Value,
    ) -> Result<ChangeWriteResult> {
        self.writes.create_change_task(payload).await
    }

    pub async fn update_change_task(
        &self,
        sys_id: &str,
        payload: serde_json::Value,
    ) -> Result<ChangeWriteResult> {
        self.writes.update_change_task(sys_id, payload).await
    }

    pub async fn create_resource_plan(
        &self,
        payload: serde_json::Value,
    ) -> Result<ResourcePlanWriteResult> {
        self.writes.create_resource_plan(payload).await
    }

    pub async fn update_resource_plan(
        &self,
        sys_id: &str,
        payload: serde_json::Value,
    ) -> Result<ResourcePlanWriteResult> {
        self.writes.update_resource_plan(sys_id, payload).await
    }

    pub async fn add_work_note(&self, number: &str, text: &str) -> Result<Option<SnowRecord>> {
        self.records.add_work_note(number, text).await
    }

    pub async fn search_catalog_items(&self, query: &str, limit: u32) -> Result<Vec<CatalogItem>> {
        self.writes.search_catalog_items(query, limit).await
    }

    pub async fn get_catalog_item(&self, sys_id: &str) -> Result<CatalogItem> {
        self.writes.get_catalog_item(sys_id).await
    }

    pub async fn submit_catalog_request(
        &self,
        item_sys_id: &str,
        request_body: serde_json::Value,
    ) -> Result<CatalogSubmitResult> {
        self.writes
            .submit_catalog_request(item_sys_id, request_body)
            .await
    }

    pub async fn list_attachments(&self, number: &str) -> Result<Option<Vec<AttachmentMetadata>>> {
        self.writes.list_attachments(number).await
    }

    pub async fn upload_attachment_file(
        &self,
        number: &str,
        path: impl AsRef<Path>,
        file_name: Option<&str>,
        content_type: Option<&str>,
    ) -> Result<Option<AttachmentMetadata>> {
        self.writes
            .upload_attachment_file(number, path, file_name, content_type)
            .await
    }

    pub async fn set_state(&self, number: &str, state: &str) -> Result<Option<SnowRecord>> {
        self.records.set_state(number, state).await
    }

    pub async fn field_choices(&self, table: &str, field: &str) -> Result<Vec<FieldChoice>> {
        self.records.field_choices(table, field).await
    }

    pub async fn reassign(&self, number: &str, user: &str) -> Result<Option<SnowRecord>> {
        self.records.reassign(number, user).await
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
        self.records.browser_url(number)
    }

    pub fn vault_relative_path_for_sys_id(&self, sys_id: &str) -> Result<Option<String>> {
        self.records.vault_relative_path_for_sys_id(sys_id)
    }

    // ===== knowledge (kb.rs-origin) =====

    pub async fn get_knowledge_article_fresh(
        &self,
        number: &str,
    ) -> Result<Option<KnowledgeArticle>> {
        self.knowledge.get_knowledge_article_fresh(number).await
    }

    pub async fn sync_knowledge(
        &self,
        full: bool,
        with_bodies: bool,
    ) -> Result<KnowledgeSyncOutcome> {
        self.knowledge.sync_knowledge(full, with_bodies).await
    }

    pub fn knowledge_status(&self) -> Result<KnowledgeStatus> {
        self.knowledge.knowledge_status()
    }

    pub fn list_knowledge_tags(
        &self,
        layer: Option<KnowledgeTagLayer>,
        min_count: usize,
    ) -> Result<Vec<KnowledgeTagSummary>> {
        self.knowledge.list_knowledge_tags(layer, min_count)
    }
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
        let users = service::UserService::new(ctx.clone());
        let approvals = service::ApprovalService::new(ctx.clone());
        let business_applications = service::BusinessApplicationService::new(ctx.clone());
        let servers = service::ServerService::new(ctx.clone());
        let records = service::RecordService::new(ctx.clone());
        let knowledge = service::KnowledgeService::new(ctx.clone());
        let vault_svc = service::VaultService::new(ctx.clone());
        let writes = service::WriteService::new(ctx.clone());
        Ok(SnowCore {
            ctx,
            users,
            approvals,
            business_applications,
            servers,
            records,
            knowledge,
            vault_svc,
            writes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ResourceType;
    use crate::vault::VaultDocument;
    use chrono::TimeZone;
    use servicenow_rs::prelude::{BasicAuth, DisplayValue, Record, ServiceNowClient};
    use tempfile::TempDir;
    use wiremock::matchers::{method, path, query_param};
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

    pub(crate) fn sample_change_task_record() -> Record {
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

    pub(crate) fn sample_incident_record() -> SnowRecord {
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

    pub(crate) async fn mount_number_lookup(
        server: &MockServer,
        table: &str,
        number: &str,
        sys_id: &str,
    ) {
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

    pub(crate) async fn mount_fresh_record_get(
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

    pub(crate) fn timecard_record_json(monday: &str, state: &str) -> serde_json::Value {
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

    pub(crate) fn sample_projected_record() -> SnowRecord {
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

    pub(crate) fn sample_projected_knowledge_article() -> KnowledgeArticle {
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

    pub(crate) async fn build_test_core(vault_path: PathBuf) -> SnowCore {
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

    pub(crate) async fn build_semantic_test_core(vault_path: PathBuf) -> SnowCore {
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

    pub(crate) fn seed_projected_knowledge_article(core: &SnowCore, article: &KnowledgeArticle) {
        let document = VaultDocument::Knowledge(article.clone());
        let persisted = core
            .ctx
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
        core.ctx
            .project_runtime_document(&document)
            .expect("project runtime document");
    }

    pub(crate) fn seed_projected_record(core: &SnowCore, record: &SnowRecord) {
        let document = VaultDocument::Record(record.clone());
        let persisted = core
            .ctx
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
        core.ctx
            .project_runtime_document(&document)
            .expect("project runtime document");
    }

    pub(crate) fn time_sheet_row_json(week_starts_on: &str) -> serde_json::Value {
        serde_json::json!({
            "sys_id": format!("sheet-{week_starts_on}"),
            "week_starts_on": week_starts_on,
            "user": { "value": "user-sys", "display_value": "Test User" },
            "user.user_name": "test_user",
            "state": { "value": "Pending", "display_value": "Pending" }
        })
    }
}
