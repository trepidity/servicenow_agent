use super::*;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::Result;
use servicenow_rs::prelude::ServiceNowClient;

use crate::cache::store::BusinessApplicationFieldDictionaryRow;
use crate::query::filter::{BusinessApplicationQuery, ListQuery};
use crate::vault::manager::VaultManager;

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
