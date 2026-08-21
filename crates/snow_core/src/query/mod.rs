pub mod filter;
pub mod resolver;

use std::collections::{BTreeMap, HashMap};
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::{Map, Value};

use crate::cache::store::{
    BusinessApplicationServerInventoryHealthRow, BusinessApplicationServerMembershipRow,
    KnowledgeBaseSummaryRow, KnowledgeCategorySummaryRow, RecordLifecycle, RecordRow, Store,
};
use crate::query::filter::{ApprovalQuery, BusinessApplicationQuery, ListQuery};
use crate::query::resolver::ReferenceResolver;
use crate::vault::VaultManager;
use servicenow_rs::model::value::parse_servicenow_timestamp;

use crate::{
    ApprovalRecord, ApprovalRoutedVia, CacheSource, DegradedReadDiagnostic, DegradedReadReason,
    FieldValue, JournalEntry, KnowledgeArticle, KnowledgeSearchFilters, MatchField, RecordRef,
    Reference, ResourceType, SearchMatchReason, SearchResult, SearchScope, ServerQuery, SnowRecord,
    choose_reference_display_name, normalize_knowledge_article,
};
use crate::{
    BusinessApplicationServerInventoryHealth, BusinessApplicationServerPath,
    BusinessApplicationServersCachedParams, BusinessApplicationServersCachedResult,
    BusinessApplicationServersCachedSelector, BusinessApplicationsForServerParams,
    BusinessApplicationsForServerResult, BusinessApplicationsForServerSelector,
    CachedBusinessApplicationForServer, CachedBusinessApplicationServer,
    CachedServerBusinessApplications, EndpointResolutionStatus, RelationshipKnowledgeStatus,
};

#[derive(Debug, Clone)]
pub struct QueryEngine {
    store: Arc<Store>,
    resolver: ReferenceResolver,
    vault: Option<VaultManager>,
    degraded_reads: Arc<Mutex<BTreeMap<String, DegradedReadDiagnostic>>>,
}

impl QueryEngine {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self::from_store(Store::open(path)?))
    }

    pub fn open_with_vault(path: impl AsRef<Path>, vault_root: impl Into<PathBuf>) -> Result<Self> {
        Ok(Self::from_store_with_vault(
            Store::open(path)?,
            Some(vault_root.into()),
        ))
    }

    pub fn from_store(store: Store) -> Self {
        Self::from_store_with_vault(store, None)
    }

    pub fn from_store_with_vault(store: Store, vault_root: Option<PathBuf>) -> Self {
        Self {
            store: Arc::new(store),
            resolver: ReferenceResolver::default(),
            vault: vault_root.map(VaultManager::new),
            degraded_reads: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub fn with_resolver(store: Store, resolver: ReferenceResolver) -> Self {
        Self {
            store: Arc::new(store),
            resolver,
            vault: None,
            degraded_reads: Arc::new(Mutex::new(BTreeMap::new())),
        }
    }

    pub async fn get_record(&self, number: &str) -> Result<Option<SnowRecord>> {
        let Some(row) = self.store.get_primary_record_by_number(number)? else {
            return Ok(None);
        };
        Ok(Some(self.materialize_record(&row)?))
    }

    pub async fn get_record_fresh(&self, number: &str) -> Result<Option<SnowRecord>> {
        self.get_record(number).await
    }

    pub async fn get_knowledge_article(&self, number: &str) -> Result<Option<KnowledgeArticle>> {
        let Some(row) = self
            .store
            .get_record_by_number_and_type(number, ResourceType::Knowledge)?
        else {
            return Ok(None);
        };
        Ok(Some(self.materialize_knowledge_article(&row)?))
    }

    pub async fn search_knowledge(
        &self,
        query: &str,
        filters: KnowledgeSearchFilters,
    ) -> Result<Vec<KnowledgeArticle>> {
        let normalized_query = normalize_search_term(query);
        if normalized_query.is_empty() && query.trim().is_empty() {
            return Ok(Vec::new());
        }

        let limit = filters.limit.unwrap_or(20).max(1);
        let mut articles = Vec::new();

        for number in self.store.search_fts(query, limit.saturating_mul(4))? {
            let Some(article) = self.get_knowledge_article(&number).await? else {
                continue;
            };
            if !matches_knowledge_filters(&article, &filters) {
                continue;
            }
            articles.push(article);
            if articles.len() >= limit {
                break;
            }
        }

        Ok(articles)
    }

    pub async fn get_approval(&self, number: &str) -> Result<Option<ApprovalRecord>> {
        let Some(row) = self
            .store
            .get_record_by_number_and_type(number, ResourceType::Approval)?
        else {
            return Ok(None);
        };
        Ok(Some(self.materialize_approval(&row)?))
    }

    pub async fn get_children(&self, number: &str) -> Result<Vec<SnowRecord>> {
        let Some(parent) = self.get_record(number).await? else {
            return Ok(Vec::new());
        };
        self.list_records(ListQuery::new().parent_sys_id(parent.sys_id))
            .await
    }

    pub fn list_knowledge_bases(&self) -> Result<Vec<KnowledgeBaseSummaryRow>> {
        self.store.list_knowledge_bases().map_err(Into::into)
    }

    pub fn list_knowledge_categories(
        &self,
        knowledge_base_sys_id: &str,
    ) -> Result<Vec<KnowledgeCategorySummaryRow>> {
        self.store
            .list_knowledge_categories(knowledge_base_sys_id)
            .map_err(Into::into)
    }

    pub async fn list_records(&self, query: ListQuery) -> Result<Vec<SnowRecord>> {
        let rows = self
            .store
            .list_active_records(query.resource_type.clone())?;
        let mut records = rows
            .into_iter()
            .filter(|row| query.matches(row))
            .map(|row| self.materialize_record(&row))
            .collect::<Result<Vec<_>>>()?;

        if let Some(limit) = query.limit {
            records.truncate(limit);
        }

        Ok(records)
    }

    pub async fn my_tasks(&self) -> Result<Vec<SnowRecord>> {
        let task_types = [
            ResourceType::Task,
            ResourceType::Incident,
            ResourceType::Change,
            ResourceType::ChangeTask,
            ResourceType::Request,
            ResourceType::RequestTask,
            ResourceType::DemandTask,
            ResourceType::Story,
            ResourceType::ScrumTask,
        ];
        let rows = self.store.list_active_records(None)?;
        let mut records = rows
            .into_iter()
            .filter(|row| task_types.contains(&row.resource_type))
            .map(|row| self.materialize_record(&row))
            .collect::<Result<Vec<_>>>()?;
        records.sort_by(|left, right| left.number.cmp(&right.number));
        Ok(records)
    }

    pub async fn my_approvals(&self) -> Result<Vec<ApprovalRecord>> {
        self.approvals(ApprovalQuery::pending()).await
    }

    pub async fn query_business_applications(
        &self,
        query: BusinessApplicationQuery,
    ) -> Result<Vec<SnowRecord>> {
        let rows = self.store.query_business_application_records(&query)?;
        rows.into_iter()
            .map(|row| self.materialize_record(&row))
            .collect()
    }

    pub async fn query_servers(&self, query: ServerQuery) -> Result<Vec<SnowRecord>> {
        query.validate()?;
        let rows = self.store.list_active_records(Some(ResourceType::Server))?;
        let mut records = rows
            .into_iter()
            .map(|row| self.materialize_record(&row))
            .collect::<Result<Vec<_>>>()?
            .into_iter()
            .filter(|record| server_matches_query(record, &query))
            .collect::<Vec<_>>();
        records.sort_by_key(server_name);
        let offset = query.offset.unwrap_or(0).min(records.len());
        let limit = query.limit.unwrap_or(20);
        Ok(records.into_iter().skip(offset).take(limit).collect())
    }

    pub async fn business_application_servers_cached(
        &self,
        params: BusinessApplicationServersCachedParams,
    ) -> Result<Option<BusinessApplicationServersCachedResult>> {
        let options = params.validate()?;
        let Some(business_application) = self
            .resolve_cached_business_application(&options.selector, options.include_tombstoned)?
        else {
            return Ok(None);
        };

        let memberships = self
            .store
            .list_business_application_server_memberships_for_ba(
                &business_application.sys_id,
                options.include_tombstoned,
            )?;
        let inventory_health = self
            .store
            .get_business_application_server_inventory_health(&business_application.sys_id)?
            .map(business_application_inventory_health_from_row);
        let relationship_status =
            cached_relationship_status(!memberships.is_empty(), inventory_health.as_ref());
        let mut servers = Vec::with_capacity(memberships.len());
        for membership in memberships {
            let Some(server) = self.cached_record_by_sys_id(
                &membership.server_sys_id,
                ResourceType::Server,
                options.include_tombstoned,
            )?
            else {
                continue;
            };
            servers.push(CachedBusinessApplicationServer {
                server,
                server_table: membership.server_table.clone(),
                provenance: membership.provenance.clone(),
                min_depth: membership.min_depth,
                paths: parse_cached_business_application_server_paths(&membership)?,
                tombstoned_at: membership.tombstoned_at,
            });
        }
        servers.sort_by(|left, right| {
            left.min_depth
                .cmp(&right.min_depth)
                .then_with(|| server_name(&left.server).cmp(&server_name(&right.server)))
                .then_with(|| left.server.sys_id.cmp(&right.server.sys_id))
                .then_with(|| left.provenance.cmp(&right.provenance))
        });

        Ok(Some(BusinessApplicationServersCachedResult {
            business_application,
            servers,
            endpoint_status: EndpointResolutionStatus::CacheHit,
            relationship_status,
            inventory_health,
        }))
    }

    pub async fn business_applications_for_server(
        &self,
        params: BusinessApplicationsForServerParams,
    ) -> Result<Option<BusinessApplicationsForServerResult>> {
        let options = params.validate()?;
        let server_records =
            self.resolve_cached_servers(&options.selector, options.include_tombstoned)?;
        if server_records.is_empty() {
            return Ok(None);
        }

        let mut servers = Vec::with_capacity(server_records.len());
        for server in server_records {
            let memberships = self
                .store
                .list_business_application_server_memberships_for_server(
                    &server.sys_id,
                    options.include_tombstoned,
                )?;
            let mut business_applications = Vec::with_capacity(memberships.len());
            for membership in memberships {
                let Some(business_application) = self.cached_record_by_sys_id(
                    &membership.ba_sys_id,
                    ResourceType::BusinessApplication,
                    options.include_tombstoned,
                )?
                else {
                    continue;
                };
                let inventory_health = self
                    .store
                    .get_business_application_server_inventory_health(&membership.ba_sys_id)?
                    .map(business_application_inventory_health_from_row);
                business_applications.push(CachedBusinessApplicationForServer {
                    business_application,
                    provenance: membership.provenance.clone(),
                    min_depth: membership.min_depth,
                    paths: parse_cached_business_application_server_paths(&membership)?,
                    inventory_health,
                    tombstoned_at: membership.tombstoned_at,
                });
            }
            business_applications.sort_by(|left, right| {
                left.min_depth
                    .cmp(&right.min_depth)
                    .then_with(|| {
                        business_application_name(&left.business_application)
                            .cmp(&business_application_name(&right.business_application))
                    })
                    .then_with(|| {
                        left.business_application
                            .sys_id
                            .cmp(&right.business_application.sys_id)
                    })
                    .then_with(|| left.provenance.cmp(&right.provenance))
            });
            let relationship_status = cached_server_relationship_status(&business_applications);
            servers.push(CachedServerBusinessApplications {
                server,
                business_applications,
                endpoint_status: EndpointResolutionStatus::CacheHit,
                relationship_status,
            });
        }
        servers.sort_by(|left, right| {
            server_name(&left.server)
                .cmp(&server_name(&right.server))
                .then_with(|| left.server.sys_id.cmp(&right.server.sys_id))
        });

        let relationship_status = cached_result_relationship_status(&servers);
        Ok(Some(BusinessApplicationsForServerResult {
            servers,
            endpoint_status: EndpointResolutionStatus::CacheHit,
            relationship_status,
        }))
    }

    pub async fn approvals(&self, query: ApprovalQuery) -> Result<Vec<ApprovalRecord>> {
        let rows = self
            .store
            .list_active_records(Some(ResourceType::Approval))?;
        let mut approvals = rows
            .into_iter()
            .filter(|row| query.matches(row))
            .map(|row| self.materialize_approval(&row))
            .collect::<Result<Vec<_>>>()?;

        if let Some(limit) = query.limit {
            approvals.truncate(limit);
        }

        Ok(approvals)
    }

    pub async fn search(&self, query: &str, scope: SearchScope) -> Result<Vec<SearchResult>> {
        let numbers = self.store.search_fts(query, 20)?;
        let mut results = Vec::new();
        for number in numbers {
            let Some(row) = self.store.get_primary_record_by_number(&number)? else {
                continue;
            };
            let record = self.materialize_record(&row)?;
            if !matches_scope(&record, &scope) {
                continue;
            }
            let matched_value = record.short_description.clone();
            results.push(
                search_result_from_record(
                    &record,
                    MatchField::ShortDescription,
                    Some(matched_value.clone()),
                    vec![SearchMatchReason {
                        field: MatchField::ShortDescription,
                        value: matched_value,
                    }],
                )
                .with_score(0),
            );
        }
        Ok(results)
    }

    pub async fn search_by_tag(&self, tag: &str, scope: SearchScope) -> Result<Vec<SearchResult>> {
        self.search_by_enrichment(
            self.store.find_record_sys_ids_by_tag(tag, 20)?,
            scope,
            MatchField::Tag,
            tag.to_string(),
        )
        .await
    }

    pub async fn search_by_keyword(
        &self,
        keyword: &str,
        scope: SearchScope,
    ) -> Result<Vec<SearchResult>> {
        self.search_by_enrichment(
            self.store.find_record_sys_ids_by_keyword(keyword, 20)?,
            scope,
            MatchField::Keyword,
            keyword.to_string(),
        )
        .await
    }

    pub async fn search_by_alias(
        &self,
        alias: &str,
        scope: SearchScope,
    ) -> Result<Vec<SearchResult>> {
        self.search_by_enrichment(
            self.store.find_record_sys_ids_by_alias(alias, 20)?,
            scope,
            MatchField::Alias,
            alias.to_string(),
        )
        .await
    }

    pub async fn search_enriched(
        &self,
        query: &str,
        scope: SearchScope,
    ) -> Result<Vec<SearchResult>> {
        let normalized_query = normalize_search_term(query);
        if normalized_query.is_empty() && query.trim().is_empty() {
            return Ok(Vec::new());
        }

        let mut candidates = BTreeMap::<String, RankedSearchCandidate>::new();

        for number in self.store.search_fts(query, 20)? {
            let Some(record) = self.get_record(&number).await? else {
                continue;
            };
            if !matches_scope(&record, &scope) {
                continue;
            }
            let candidate = candidates
                .entry(record.sys_id.clone())
                .or_insert_with(|| RankedSearchCandidate::new(record));
            let matched_value = candidate.record.short_description.clone();
            candidate.add_contribution(
                FTS_SHORT_DESCRIPTION_SCORE,
                MatchField::ShortDescription,
                matched_value,
                None,
            );
            candidate.apply_exact_bonuses(&normalized_query);
        }

        if let Some(record) = self.get_record(query).await?
            && matches_scope(&record, &scope)
        {
            let candidate = candidates
                .entry(record.sys_id.clone())
                .or_insert_with(|| RankedSearchCandidate::new(record));
            let matched_value = candidate.record.number.clone();
            candidate.add_contribution(EXACT_NUMBER_BONUS, MatchField::Number, matched_value, None);
            candidate.apply_exact_bonuses(&normalized_query);
        }

        for sys_id in self
            .store
            .find_record_sys_ids_by_tag(&normalized_query, 20)?
        {
            self.add_exact_enrichment_candidate(
                &mut candidates,
                &sys_id,
                &normalized_query,
                &scope,
                EnrichmentMatch::Tag,
            )
            .await?;
        }

        for sys_id in self
            .store
            .find_record_sys_ids_by_alias(&normalized_query, 20)?
        {
            self.add_exact_enrichment_candidate(
                &mut candidates,
                &sys_id,
                &normalized_query,
                &scope,
                EnrichmentMatch::Alias,
            )
            .await?;
        }

        for sys_id in self
            .store
            .find_record_sys_ids_by_keyword(&normalized_query, 20)?
        {
            self.add_exact_enrichment_candidate(
                &mut candidates,
                &sys_id,
                &normalized_query,
                &scope,
                EnrichmentMatch::Keyword,
            )
            .await?;
        }

        let mut candidates: Vec<RankedSearchCandidate> = candidates.into_values().collect();
        candidates.sort_by(|left, right| {
            right
                .score
                .cmp(&left.score)
                .then_with(|| match_priority(right.match_in).cmp(&match_priority(left.match_in)))
                .then_with(|| right.source_priority.cmp(&left.source_priority))
                .then_with(|| {
                    right
                        .matched_value_priority
                        .cmp(&left.matched_value_priority)
                })
                .then_with(|| left.record.number.cmp(&right.record.number))
                .then_with(|| left.record.sys_id.cmp(&right.record.sys_id))
        });
        Ok(candidates
            .into_iter()
            .map(RankedSearchCandidate::into_result)
            .collect())
    }

    pub fn store(&self) -> &Store {
        self.store.as_ref()
    }

    pub fn resolver(&self) -> &ReferenceResolver {
        &self.resolver
    }

    pub fn degraded_reads(&self) -> Vec<DegradedReadDiagnostic> {
        let guard = self
            .degraded_reads
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        let mut diagnostics = guard.values().cloned().collect::<Vec<_>>();
        diagnostics.sort_by(|left, right| {
            left.number
                .cmp(&right.number)
                .then_with(|| left.sys_id.cmp(&right.sys_id))
                .then_with(|| left.observed_at.cmp(&right.observed_at))
        });
        diagnostics
    }

    fn materialize_record(&self, row: &RecordRow) -> Result<SnowRecord> {
        if let Some(record) = self.load_record_from_vault(row)? {
            return Ok(record);
        }
        self.materialize_record_from_json(row)
    }

    fn resolve_cached_business_application(
        &self,
        selector: &BusinessApplicationServersCachedSelector,
        include_tombstoned: bool,
    ) -> Result<Option<SnowRecord>> {
        match selector {
            BusinessApplicationServersCachedSelector::Number(number) => {
                let Some(row) = self
                    .store
                    .get_record_by_number_and_type(number, ResourceType::BusinessApplication)?
                else {
                    return Ok(None);
                };
                if !record_visible_for_cached_relationship_lookup(&row, include_tombstoned) {
                    return Ok(None);
                }
                Ok(Some(self.materialize_record(&row)?))
            }
            BusinessApplicationServersCachedSelector::SysId(sys_id) => self
                .cached_record_by_sys_id(
                    sys_id,
                    ResourceType::BusinessApplication,
                    include_tombstoned,
                ),
            BusinessApplicationServersCachedSelector::ExactName(name) => {
                let mut matches = self
                    .cached_resource_records(ResourceType::BusinessApplication, include_tombstoned)?
                    .into_iter()
                    .filter(|record| business_application_name(record) == name.trim())
                    .collect::<Vec<_>>();
                if matches.len() > 1 {
                    anyhow::bail!(
                        "multiple Business Applications matched name={}",
                        name.trim()
                    );
                }
                Ok(matches.pop())
            }
        }
    }

    fn resolve_cached_servers(
        &self,
        selector: &BusinessApplicationsForServerSelector,
        include_tombstoned: bool,
    ) -> Result<Vec<SnowRecord>> {
        match selector {
            BusinessApplicationsForServerSelector::SysId(sys_id) => Ok(self
                .cached_record_by_sys_id(sys_id, ResourceType::Server, include_tombstoned)?
                .into_iter()
                .collect()),
            BusinessApplicationsForServerSelector::ExactName(name) => Ok(self
                .cached_resource_records(ResourceType::Server, include_tombstoned)?
                .into_iter()
                .filter(|record| server_name(record) == name.trim())
                .collect()),
            BusinessApplicationsForServerSelector::IpAddress(ip_address) => Ok(self
                .cached_resource_records(ResourceType::Server, include_tombstoned)?
                .into_iter()
                .filter(|record| {
                    server_field_text(record, "ip_address")
                        .is_some_and(|value| value.eq_ignore_ascii_case(ip_address.trim()))
                })
                .collect()),
        }
    }

    fn cached_record_by_sys_id(
        &self,
        sys_id: &str,
        resource_type: ResourceType,
        include_tombstoned: bool,
    ) -> Result<Option<SnowRecord>> {
        let Some(row) = self.store.get_record_by_sys_id(sys_id)? else {
            return Ok(None);
        };
        if row.resource_type != resource_type
            || !record_visible_for_cached_relationship_lookup(&row, include_tombstoned)
        {
            return Ok(None);
        }
        Ok(Some(self.materialize_record(&row)?))
    }

    fn cached_resource_records(
        &self,
        resource_type: ResourceType,
        include_tombstoned: bool,
    ) -> Result<Vec<SnowRecord>> {
        let rows = if include_tombstoned {
            self.store
                .list_records_by_resource_type(resource_type.clone())?
        } else {
            self.store.list_active_records(Some(resource_type))?
        };
        rows.into_iter()
            .filter(|row| record_visible_for_cached_relationship_lookup(row, include_tombstoned))
            .map(|row| self.materialize_record(&row))
            .collect()
    }

    fn materialize_record_from_json(&self, row: &RecordRow) -> Result<SnowRecord> {
        let json =
            serde_json::from_str::<Value>(&row.raw_json).unwrap_or(Value::Object(Map::new()));
        let fields = collect_fields(&json);
        let references = collect_references(&self.resolver, &json, &row.table_name);
        let parent = row
            .parent_id
            .as_ref()
            .and_then(|_| {
                reference_to_record_ref(
                    &self.resolver,
                    "parent",
                    json.get("parent"),
                    Some("record"),
                )
            })
            .or_else(|| {
                row.parent_id.as_ref().map(|sys_id| RecordRef {
                    sys_id: sys_id.clone(),
                    number: String::new(),
                    table: String::new(),
                })
            });
        let work_notes = collect_journals(&json, "work_notes");
        let comments = collect_journals(&json, "comments");
        let children = self.children_for(&row.sys_id)?;

        Ok(SnowRecord {
            sys_id: row.sys_id.clone(),
            number: row.number.clone(),
            table: row.table_name.clone(),
            resource_type: row.resource_type.clone(),
            state: row.state.clone().unwrap_or_default(),
            short_description: row.short_desc.clone().unwrap_or_default(),
            description: row.description.clone().unwrap_or_default(),
            fields,
            work_notes,
            comments,
            parent,
            children,
            references,
            synced_at: row.synced_at,
            source: CacheSource::Disk,
        })
    }

    fn materialize_approval(&self, row: &RecordRow) -> Result<ApprovalRecord> {
        if let Some(approval) = self.load_approval_from_vault(row)? {
            return Ok(approval);
        }

        let record = self.materialize_record_from_json(row)?;
        let json =
            serde_json::from_str::<Value>(&row.raw_json).unwrap_or(Value::Object(Map::new()));
        let approver = reference_from_json(
            &self.resolver,
            "approver",
            json.get("approver"),
            Some("sys_user"),
        )
        .unwrap_or_else(|| Reference {
            sys_id: row.assigned_to.clone().unwrap_or_default(),
            table: "sys_user".to_string(),
            display_name: row.assigned_to.clone().unwrap_or_default(),
            extra: HashMap::new(),
        });
        let target = reference_to_record_ref(
            &self.resolver,
            "sysapproval",
            json.get("sysapproval"),
            Some("record"),
        )
        .unwrap_or_else(|| RecordRef {
            sys_id: row.sys_id.clone(),
            number: row.number.clone(),
            table: row.table_name.clone(),
        });
        let requested_at = parse_timestamp(&json, "sys_created_on")
            .or_else(|| parse_timestamp(&json, "opened_at"))
            .unwrap_or(row.synced_at);
        let due_date = parse_timestamp(&json, "due_date");

        Ok(ApprovalRecord {
            record,
            approver,
            target,
            requested_at,
            due_date,
            routed_via: ApprovalRoutedVia::Direct,
            approver_group: None,
        })
    }

    fn materialize_knowledge_article(&self, row: &RecordRow) -> Result<KnowledgeArticle> {
        if let Some(article) = self.load_knowledge_article_from_vault(row)? {
            return Ok(article);
        }

        let record = self.materialize_record_from_json(row)?;
        let projection = self.store.get_knowledge_article(&row.sys_id)?;
        let json =
            serde_json::from_str::<Value>(&row.raw_json).unwrap_or(Value::Object(Map::new()));
        let knowledge_base = reference_from_json(
            &self.resolver,
            "knowledge_base",
            json.get("knowledge_base"),
            Some("kb_knowledge_base"),
        )
        .or_else(|| record.references.get("knowledge_base").cloned())
        .unwrap_or_else(|| Reference {
            sys_id: String::new(),
            table: "kb_knowledge_base".to_string(),
            display_name: String::new(),
            extra: HashMap::new(),
        });
        let category = reference_from_json(
            &self.resolver,
            "category",
            json.get("category"),
            Some("kb_category"),
        )
        .or_else(|| record.references.get("category").cloned())
        .unwrap_or_else(|| Reference {
            sys_id: String::new(),
            table: "kb_category".to_string(),
            display_name: String::new(),
            extra: HashMap::new(),
        });
        let article_type =
            json_string_field(&json, "article_type").unwrap_or_else(|| "text".to_string());
        let content = knowledge_content_from_json(&json)
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| record.description.clone());
        let author = reference_from_json(
            &self.resolver,
            "author",
            json.get("author"),
            Some("sys_user"),
        )
        .or_else(|| record.references.get("author").cloned())
        .or_else(|| author_reference_from_fields(&record.fields));
        let published_at = json_string_field(&json, "published")
            .or_else(|| {
                record
                    .fields
                    .get("published")
                    .map(|value| value.value.clone())
            })
            .and_then(|value| parse_servicenow_timestamp(Some(&value)));
        let valid_to = json_string_field(&json, "valid_to")
            .and_then(|value| chrono::NaiveDate::parse_from_str(&value, "%Y-%m-%d").ok());
        let sn_tags = json_string_array_field(&json, "sn_tags").unwrap_or_else(|| {
            projection
                .as_ref()
                .map(|row| row.sn_tags.clone())
                .unwrap_or_default()
        });
        let auto_tags = json_string_array_field(&json, "auto_tags").unwrap_or_else(|| {
            projection
                .as_ref()
                .map(|row| row.auto_tags.clone())
                .unwrap_or_default()
        });
        let user_tags = json_string_array_field(&json, "user_tags").unwrap_or_else(|| {
            projection
                .as_ref()
                .map(|row| row.user_tags.clone())
                .unwrap_or_default()
        });
        let body_cached = json
            .get("body_cached")
            .and_then(Value::as_bool)
            .unwrap_or_else(|| {
                projection
                    .as_ref()
                    .map(|row| row.body_cached)
                    .unwrap_or_else(|| !content.trim().is_empty())
            });

        Ok(normalize_knowledge_article(KnowledgeArticle {
            record,
            knowledge_base,
            category,
            article_type,
            content,
            sn_tags,
            auto_tags,
            user_tags,
            body_cached,
            published_at,
            author,
            valid_to,
        }))
    }

    fn load_record_from_vault(&self, row: &RecordRow) -> Result<Option<SnowRecord>> {
        let Some(path) = self.markdown_path_for_row(row) else {
            self.record_degraded_read(row, None, DegradedReadReason::RawJsonFallback);
            return Ok(None);
        };
        let Some(vault) = &self.vault else {
            self.record_degraded_read(row, Some(path), DegradedReadReason::RawJsonFallback);
            return Ok(None);
        };

        let loaded = match row.resource_type {
            ResourceType::Knowledge => vault
                .read_knowledge_article(&path)
                .map(|article| article.record),
            ResourceType::Approval => vault.read_approval(&path).map(|approval| approval.record),
            _ => vault.read_record(&path),
        };

        match loaded {
            Ok(record) => {
                self.clear_degraded_read(&row.sys_id);
                Ok(Some(record))
            }
            Err(err) => {
                let reason = degraded_reason_from_error(&err);
                self.record_degraded_read(row, Some(path), reason);
                Ok(None)
            }
        }
    }

    fn load_approval_from_vault(&self, row: &RecordRow) -> Result<Option<ApprovalRecord>> {
        let Some(path) = self.markdown_path_for_row(row) else {
            self.record_degraded_read(row, None, DegradedReadReason::RawJsonFallback);
            return Ok(None);
        };
        let Some(vault) = &self.vault else {
            self.record_degraded_read(row, Some(path), DegradedReadReason::RawJsonFallback);
            return Ok(None);
        };

        match vault.read_approval(&path) {
            Ok(approval) => {
                self.clear_degraded_read(&row.sys_id);
                Ok(Some(approval))
            }
            Err(err) => {
                let reason = degraded_reason_from_error(&err);
                self.record_degraded_read(row, Some(path), reason);
                Ok(None)
            }
        }
    }

    fn load_knowledge_article_from_vault(
        &self,
        row: &RecordRow,
    ) -> Result<Option<KnowledgeArticle>> {
        let Some(path) = self.markdown_path_for_row(row) else {
            self.record_degraded_read(row, None, DegradedReadReason::RawJsonFallback);
            return Ok(None);
        };
        let Some(vault) = &self.vault else {
            self.record_degraded_read(row, Some(path), DegradedReadReason::RawJsonFallback);
            return Ok(None);
        };

        match vault.read_knowledge_article(&path) {
            Ok(article) => {
                self.clear_degraded_read(&row.sys_id);
                Ok(Some(article))
            }
            Err(err) => {
                let reason = degraded_reason_from_error(&err);
                self.record_degraded_read(row, Some(path), reason);
                Ok(None)
            }
        }
    }

    fn markdown_path_for_row(&self, row: &RecordRow) -> Option<PathBuf> {
        let relative = row.file_path.as_ref().map(PathBuf::from)?;
        Some(match &self.vault {
            Some(vault) => vault.layout().root().join(relative),
            None => relative,
        })
    }

    fn record_degraded_read(
        &self,
        row: &RecordRow,
        markdown_path: Option<PathBuf>,
        reason: DegradedReadReason,
    ) {
        let diagnostic = DegradedReadDiagnostic {
            sys_id: row.sys_id.clone(),
            number: row.number.clone(),
            table: row.table_name.clone(),
            resource_type: row.resource_type.clone(),
            markdown_path,
            reason,
            observed_at: Utc::now(),
        };
        let mut guard = self
            .degraded_reads
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        guard.insert(row.sys_id.clone(), diagnostic);
    }

    fn clear_degraded_read(&self, sys_id: &str) {
        let mut guard = self
            .degraded_reads
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        guard.remove(sys_id);
    }

    fn children_for(&self, parent_sys_id: &str) -> Result<Vec<RecordRef>> {
        let rows = self.store.list_active_records(None)?;
        let mut children = rows
            .into_iter()
            .filter(|row| row.parent_id.as_deref() == Some(parent_sys_id))
            .map(|row| RecordRef {
                sys_id: row.sys_id,
                number: row.number,
                table: row.table_name,
            })
            .collect::<Vec<_>>();
        children.sort_by(|a, b| a.number.cmp(&b.number));
        Ok(children)
    }

    async fn search_by_enrichment(
        &self,
        sys_ids: Vec<String>,
        scope: SearchScope,
        match_in: MatchField,
        matched_value: String,
    ) -> Result<Vec<SearchResult>> {
        let mut results = Vec::new();
        for sys_id in sys_ids {
            let Some(row) = self.store.get_record_by_sys_id(&sys_id)? else {
                continue;
            };
            let record = self.materialize_record(&row)?;
            if !matches_scope(&record, &scope) {
                continue;
            }
            results.push(search_result_with_match(
                &record,
                match_in,
                matched_value.clone(),
            ));
        }
        Ok(results)
    }

    async fn add_exact_enrichment_candidate(
        &self,
        candidates: &mut BTreeMap<String, RankedSearchCandidate>,
        sys_id: &str,
        normalized_query: &str,
        scope: &SearchScope,
        match_kind: EnrichmentMatch,
    ) -> Result<()> {
        let Some(row) = self.store.get_record_by_sys_id(sys_id)? else {
            return Ok(());
        };
        let record = self.materialize_record(&row)?;
        if !matches_scope(&record, scope) {
            return Ok(());
        }

        let candidate = candidates
            .entry(record.sys_id.clone())
            .or_insert_with(|| RankedSearchCandidate::new(record));

        match match_kind {
            EnrichmentMatch::Tag => {
                if let Some(tag) = self
                    .store
                    .list_tags(sys_id)?
                    .into_iter()
                    .find(|row| normalize_search_term(&row.tag) == normalized_query)
                {
                    candidate.add_contribution(
                        EXACT_TAG_SCORE,
                        MatchField::Tag,
                        tag.tag,
                        Some(tag.source.as_str()),
                    );
                }
            }
            EnrichmentMatch::Alias => {
                if let Some(alias) = self
                    .store
                    .list_aliases(sys_id)?
                    .into_iter()
                    .find(|row| normalize_search_term(&row.alias) == normalized_query)
                {
                    candidate.add_contribution(
                        EXACT_ALIAS_SCORE,
                        MatchField::Alias,
                        alias.alias,
                        Some(alias.source.as_str()),
                    );
                }
            }
            EnrichmentMatch::Keyword => {
                if let Some(keyword) = self
                    .store
                    .list_keywords(sys_id)?
                    .into_iter()
                    .find(|row| normalize_search_term(&row.keyword) == normalized_query)
                {
                    let score = keyword_score(keyword.weight);
                    candidate.add_contribution(
                        score,
                        MatchField::Keyword,
                        keyword.keyword,
                        Some(keyword.source.as_str()),
                    );
                }
            }
        }

        candidate.apply_exact_bonuses(normalized_query);
        Ok(())
    }
}

fn matches_scope(record: &SnowRecord, scope: &SearchScope) -> bool {
    match scope {
        SearchScope::All => true,
        SearchScope::Knowledge => record.resource_type == ResourceType::Knowledge,
        SearchScope::WorkNotes => !record.work_notes.is_empty(),
    }
}

fn server_matches_query(record: &SnowRecord, query: &ServerQuery) -> bool {
    server_class_matches(record, query.class.as_deref())
        && optional_contains(server_name(record), query.name.as_deref())
        && optional_eq(
            server_field_text(record, "ip_address"),
            query.ip_address.as_deref(),
        )
        && optional_contains(
            server_reference_text(record, "managed_by_group"),
            query.ci_owner_group.as_deref(),
        )
        && optional_contains(server_search_text(record), query.text.as_deref())
}

fn parse_cached_business_application_server_paths(
    membership: &BusinessApplicationServerMembershipRow,
) -> Result<Vec<BusinessApplicationServerPath>> {
    serde_json::from_str(&membership.paths_json).map_err(Into::into)
}

fn business_application_inventory_health_from_row(
    row: BusinessApplicationServerInventoryHealthRow,
) -> BusinessApplicationServerInventoryHealth {
    let summary = serde_json::from_str(&row.summary_json).unwrap_or(Value::Object(Map::new()));
    BusinessApplicationServerInventoryHealth {
        ba_sys_id: row.ba_sys_id,
        run_started_at: row.run_started_at,
        run_completed_at: row.run_completed_at,
        service_membership_status: row.service_membership_status,
        relationship_status: row.relationship_status,
        inventory_status: row.inventory_status,
        summary,
    }
}

fn cached_relationship_status(
    has_memberships: bool,
    health: Option<&BusinessApplicationServerInventoryHealth>,
) -> RelationshipKnowledgeStatus {
    match health {
        Some(health) if health.inventory_status == "complete" && has_memberships => {
            RelationshipKnowledgeStatus::KnownRelationships
        }
        Some(health) if health.inventory_status == "complete" => {
            RelationshipKnowledgeStatus::NoCachedRelationships
        }
        Some(_) => RelationshipKnowledgeStatus::Degraded,
        None if has_memberships => RelationshipKnowledgeStatus::KnownRelationships,
        None => RelationshipKnowledgeStatus::UnknownNotSynced,
    }
}

fn cached_server_relationship_status(
    business_applications: &[CachedBusinessApplicationForServer],
) -> RelationshipKnowledgeStatus {
    if business_applications.is_empty() {
        return RelationshipKnowledgeStatus::NoCachedRelationships;
    }
    if business_applications.iter().any(|application| {
        application
            .inventory_health
            .as_ref()
            .is_some_and(|health| health.inventory_status != "complete")
    }) {
        return RelationshipKnowledgeStatus::Degraded;
    }
    RelationshipKnowledgeStatus::KnownRelationships
}

fn cached_result_relationship_status(
    servers: &[CachedServerBusinessApplications],
) -> RelationshipKnowledgeStatus {
    if servers
        .iter()
        .any(|server| server.relationship_status == RelationshipKnowledgeStatus::Degraded)
    {
        return RelationshipKnowledgeStatus::Degraded;
    }
    if servers
        .iter()
        .any(|server| server.relationship_status == RelationshipKnowledgeStatus::KnownRelationships)
    {
        return RelationshipKnowledgeStatus::KnownRelationships;
    }
    RelationshipKnowledgeStatus::NoCachedRelationships
}

fn record_visible_for_cached_relationship_lookup(
    row: &RecordRow,
    include_tombstoned: bool,
) -> bool {
    if matches!(row.lifecycle(), RecordLifecycle::Pruned) {
        return false;
    }
    include_tombstoned || matches!(row.lifecycle(), RecordLifecycle::Active)
}

fn business_application_name(record: &SnowRecord) -> String {
    record
        .fields
        .get("name")
        .and_then(|field| {
            field
                .display_value
                .as_ref()
                .or(Some(&field.value))
                .filter(|value| !value.trim().is_empty())
                .cloned()
        })
        .or_else(|| {
            (!record.short_description.trim().is_empty()).then(|| record.short_description.clone())
        })
        .unwrap_or_else(|| record.sys_id.clone())
}

fn server_name(record: &SnowRecord) -> String {
    server_field_text(record, "name")
        .or_else(|| {
            (!record.short_description.trim().is_empty()).then(|| record.short_description.clone())
        })
        .unwrap_or_else(|| record.sys_id.clone())
}

fn server_search_text(record: &SnowRecord) -> String {
    [
        Some(server_name(record)),
        server_field_text(record, "ip_address"),
        server_field_text(record, "fqdn"),
        server_field_text(record, "sys_class_name"),
        server_reference_text(record, "managed_by_group"),
        server_reference_text(record, "support_group"),
    ]
    .into_iter()
    .flatten()
    .collect::<Vec<_>>()
    .join(" ")
}

fn server_class_matches(record: &SnowRecord, class: Option<&str>) -> bool {
    let Some(class) = class.map(str::trim).filter(|value| !value.is_empty()) else {
        return true;
    };
    let Ok(class) = crate::resource::server::canonical_server_class(class) else {
        return false;
    };
    class == crate::SERVER_TABLE
        || record.table == class
        || server_field_text(record, "sys_class_name").as_deref() == Some(class)
}

fn server_reference_text(record: &SnowRecord, field: &str) -> Option<String> {
    record
        .references
        .get(field)
        .map(|reference| format!("{} {}", reference.sys_id, reference.display_name))
        .or_else(|| server_field_text(record, field))
}

fn server_field_text(record: &SnowRecord, field: &str) -> Option<String> {
    record.fields.get(field).and_then(|value| {
        value
            .display_value
            .as_deref()
            .or(Some(value.value.as_str()))
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
    })
}

fn optional_contains(haystack: impl Into<Option<String>>, needle: Option<&str>) -> bool {
    let Some(needle) = needle.map(str::trim).filter(|value| !value.is_empty()) else {
        return true;
    };
    haystack.into().is_some_and(|haystack| {
        haystack
            .to_ascii_lowercase()
            .contains(&needle.to_ascii_lowercase())
    })
}

fn optional_eq(value: Option<String>, expected: Option<&str>) -> bool {
    let Some(expected) = expected.map(str::trim).filter(|value| !value.is_empty()) else {
        return true;
    };
    value.is_some_and(|value| value.trim().eq_ignore_ascii_case(expected))
}

fn matches_knowledge_filters(article: &KnowledgeArticle, filters: &KnowledgeSearchFilters) -> bool {
    matches_reference_filter(filters.knowledge_base.as_deref(), &article.knowledge_base)
        && matches_reference_filter(filters.category.as_deref(), &article.category)
}

fn matches_reference_filter(filter: Option<&str>, reference: &Reference) -> bool {
    let Some(filter) = filter.map(str::trim).filter(|value| !value.is_empty()) else {
        return true;
    };

    filter == reference.sys_id || filter.eq_ignore_ascii_case(reference.display_name.as_str())
}

fn knowledge_content_from_json(json: &Value) -> Option<String> {
    ["article_body", "text", "description"]
        .into_iter()
        .filter_map(|field| json_string_field(json, field))
        .map(|value| value.trim().to_string())
        .find(|value| !value.is_empty())
}

fn author_reference_from_fields(fields: &HashMap<String, FieldValue>) -> Option<Reference> {
    let author = fields.get("author")?;
    let sys_id = author.value.trim();
    if sys_id.is_empty() {
        return None;
    }

    Some(Reference {
        sys_id: sys_id.to_string(),
        table: "sys_user".to_string(),
        display_name: choose_reference_display_name([author.display_value.as_deref()]),
        extra: HashMap::new(),
    })
}

fn search_result_from_record(
    record: &SnowRecord,
    match_in: MatchField,
    matched_value: Option<String>,
    reasons: Vec<SearchMatchReason>,
) -> SearchResult {
    SearchResult {
        record: RecordRef {
            sys_id: record.sys_id.clone(),
            number: record.number.clone(),
            table: record.table.clone(),
        },
        snippet: record.short_description.clone(),
        score: 0,
        match_in,
        matched_value,
        reasons,
    }
}

fn search_result_with_match(
    record: &SnowRecord,
    match_in: MatchField,
    matched_value: String,
) -> SearchResult {
    search_result_from_record(
        record,
        match_in,
        Some(matched_value.clone()),
        vec![SearchMatchReason {
            field: match_in,
            value: matched_value,
        }],
    )
}

const FTS_SHORT_DESCRIPTION_SCORE: i64 = 40;
const EXACT_NUMBER_BONUS: i64 = 30;
const EXACT_SHORT_DESCRIPTION_BONUS: i64 = 15;
const EXACT_TAG_SCORE: i64 = 70;
const EXACT_ALIAS_SCORE: i64 = 100;
const KEYWORD_SCORE_SCALE: f64 = 50.0;

const SOURCE_BONUS_MANUAL: i64 = 20;
const SOURCE_BONUS_CURATED: i64 = 15;
const SOURCE_BONUS_DERIVED: i64 = 0;

const SOURCE_PRIORITY_MANUAL: i32 = 3;
const SOURCE_PRIORITY_CURATED: i32 = 2;
const SOURCE_PRIORITY_DERIVED: i32 = 1;
const SOURCE_PRIORITY_DEFAULT: i32 = 0;

fn normalize_search_term(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut prev_space = false;

    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            out.push(ch.to_ascii_lowercase());
            prev_space = false;
        } else if !prev_space {
            out.push(' ');
            prev_space = true;
        }
    }

    out.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn keyword_score(weight: f64) -> i64 {
    (KEYWORD_SCORE_SCALE * weight.max(0.0)).round() as i64
}

fn source_bonus(source: Option<&str>) -> i64 {
    match normalized_source(source).as_str() {
        "manual" => SOURCE_BONUS_MANUAL,
        "curated" => SOURCE_BONUS_CURATED,
        "derived" | "generated" | "system" | "inferred" => SOURCE_BONUS_DERIVED,
        _ => SOURCE_BONUS_DERIVED,
    }
}

fn source_priority(source: Option<&str>) -> i32 {
    match normalized_source(source).as_str() {
        "manual" => SOURCE_PRIORITY_MANUAL,
        "curated" => SOURCE_PRIORITY_CURATED,
        "derived" | "generated" | "system" | "inferred" => SOURCE_PRIORITY_DERIVED,
        _ => SOURCE_PRIORITY_DEFAULT,
    }
}

fn normalized_source(source: Option<&str>) -> String {
    source.unwrap_or_default().trim().to_ascii_lowercase()
}

fn match_priority(match_in: MatchField) -> i32 {
    match match_in {
        MatchField::Number => 5,
        MatchField::Alias => 4,
        MatchField::Tag => 3,
        MatchField::Keyword => 2,
        MatchField::ShortDescription => 1,
        MatchField::Description => 0,
        MatchField::WorkNotes => 0,
        MatchField::Content => 0,
    }
}

#[derive(Debug, Clone, Copy)]
enum EnrichmentMatch {
    Tag,
    Keyword,
    Alias,
}

#[derive(Debug, Clone)]
struct RankedSearchCandidate {
    record: SnowRecord,
    score: i64,
    match_in: MatchField,
    match_priority: i32,
    source_priority: i32,
    matched_value: Option<String>,
    matched_value_priority: i32,
    reasons: Vec<SearchMatchReason>,
    exact_bonuses_applied: bool,
}

impl RankedSearchCandidate {
    fn new(record: SnowRecord) -> Self {
        Self {
            record,
            score: 0,
            match_in: MatchField::ShortDescription,
            match_priority: match_priority(MatchField::ShortDescription),
            source_priority: SOURCE_PRIORITY_DEFAULT,
            matched_value: None,
            matched_value_priority: i32::MIN,
            reasons: Vec::new(),
            exact_bonuses_applied: false,
        }
    }

    fn add_contribution(
        &mut self,
        score: i64,
        match_in: MatchField,
        matched_value: String,
        source: Option<&str>,
    ) {
        self.score += score + source_bonus(source);
        let priority = match_priority(match_in);
        if priority > self.match_priority {
            self.match_priority = priority;
            self.match_in = match_in;
        }
        let source_priority = source_priority(source);
        if source_priority > self.source_priority {
            self.source_priority = source_priority;
        }
        self.push_reason(match_in, matched_value.clone());
        if priority > self.matched_value_priority {
            self.matched_value_priority = priority;
            self.matched_value = Some(matched_value);
        }
    }

    fn push_reason(&mut self, field: MatchField, value: String) {
        if self
            .reasons
            .iter()
            .any(|reason| reason.field == field && reason.value == value)
        {
            return;
        }
        self.reasons.push(SearchMatchReason { field, value });
    }

    fn apply_exact_bonuses(&mut self, normalized_query: &str) {
        if self.exact_bonuses_applied {
            return;
        }
        self.exact_bonuses_applied = true;

        if normalize_search_term(&self.record.number) == normalized_query {
            self.add_contribution(
                EXACT_NUMBER_BONUS,
                MatchField::Number,
                self.record.number.clone(),
                None,
            );
        }
        if normalize_search_term(&self.record.short_description) == normalized_query {
            self.add_contribution(
                EXACT_SHORT_DESCRIPTION_BONUS,
                MatchField::ShortDescription,
                self.record.short_description.clone(),
                None,
            );
        }
    }

    fn into_result(self) -> SearchResult {
        search_result_from_record(
            &self.record,
            self.match_in,
            self.matched_value,
            self.reasons,
        )
        .with_score(self.score)
    }
}

impl ListQuery {
    pub fn matches(&self, row: &RecordRow) -> bool {
        if !self.include_tombstoned && !row.in_scope {
            return false;
        }
        if let Some(resource_type) = &self.resource_type
            && &row.resource_type != resource_type
        {
            return false;
        }
        if let Some(number) = &self.number
            && &row.number != number
        {
            return false;
        }
        if !self.numbers.is_empty()
            && !self
                .numbers
                .iter()
                .any(|candidate| candidate == &row.number)
        {
            return false;
        }
        if !self.states.is_empty() {
            let Some(state) = row.state.as_deref() else {
                return false;
            };
            if !self.states.iter().any(|candidate| candidate == state) {
                return false;
            }
        }
        if let Some(assigned_to) = &self.assigned_to
            && row.assigned_to.as_deref() != Some(assigned_to.as_str())
        {
            return false;
        }
        if let Some(parent_sys_id) = &self.parent_sys_id
            && row.parent_id.as_deref() != Some(parent_sys_id.as_str())
        {
            return false;
        }
        true
    }
}

impl ApprovalQuery {
    pub fn matches(&self, row: &RecordRow) -> bool {
        if !self.include_tombstoned && !row.in_scope {
            return false;
        }
        if !self.states.is_empty() {
            let Some(state) = row.state.as_deref() else {
                return false;
            };
            if !self.states.iter().any(|candidate| candidate == state) {
                return false;
            }
        }
        if let Some(target_sys_id) = &self.target_sys_id
            && row.parent_id.as_deref() != Some(target_sys_id.as_str())
            && row.sys_id != *target_sys_id
        {
            return false;
        }
        if let Some(approver_sys_id) = &self.approver_sys_id {
            let json = serde_json::from_str::<Value>(&row.raw_json).unwrap_or(Value::Null);
            let Some(reference) = reference_from_json(
                &ReferenceResolver::default(),
                "approver",
                json.get("approver"),
                Some("sys_user"),
            ) else {
                return false;
            };
            if reference.sys_id != *approver_sys_id {
                return false;
            }
        }
        true
    }
}

fn collect_fields(json: &Value) -> HashMap<String, FieldValue> {
    let mut fields = HashMap::new();
    if let Some(map) = json.as_object() {
        for (key, value) in map {
            let field_value = field_value_from_json(value);
            fields.insert(key.clone(), field_value);
        }
    }
    fields
}

fn collect_references(
    resolver: &ReferenceResolver,
    json: &Value,
    table_name: &str,
) -> HashMap<String, Reference> {
    let mut references = HashMap::new();
    if let Some(map) = json.as_object() {
        for field in reference_fields(table_name) {
            if let Some(value) = map.get(*field)
                && let Some(reference) = reference_from_json(resolver, field, Some(value), None)
            {
                references.insert(field.to_string(), reference);
            }
        }
    }
    references
}

fn reference_fields(table_name: &str) -> &'static [&'static str] {
    match table_name {
        "kb_knowledge" => &["knowledge_base", "category", "author"],
        "sysapproval_approver" => &["approver", "sysapproval"],
        _ => &[
            "assigned_to",
            "assignment_group",
            "caller_id",
            "cmdb_ci",
            "requested_for",
            "request_item",
            "change_request",
            "story",
            "parent",
        ],
    }
}

fn reference_from_json(
    resolver: &ReferenceResolver,
    field_name: &str,
    value: Option<&Value>,
    default_table: Option<&str>,
) -> Option<Reference> {
    value.and_then(|value| resolver.resolve_json_reference(field_name, value, default_table))
}

fn reference_to_record_ref(
    resolver: &ReferenceResolver,
    field_name: &str,
    value: Option<&Value>,
    default_table: Option<&str>,
) -> Option<RecordRef> {
    value.and_then(|value| resolver.resolve_record_ref(field_name, value, default_table))
}

fn field_value_from_json(value: &Value) -> FieldValue {
    match value {
        Value::Object(map) => {
            let display_value = map
                .get("display_value")
                .and_then(Value::as_str)
                .map(str::to_string);
            let raw_value = map
                .get("value")
                .map(json_value_to_string)
                .unwrap_or_default();
            FieldValue {
                value: raw_value,
                display_value,
            }
        }
        Value::String(text) => FieldValue {
            value: text.clone(),
            display_value: None,
        },
        other => FieldValue {
            value: json_value_to_string(other),
            display_value: None,
        },
    }
}

fn collect_journals(json: &Value, field_name: &str) -> Vec<JournalEntry> {
    let Some(value) = json.get(field_name) else {
        return Vec::new();
    };
    match value {
        Value::String(blob) => parse_journal_blob(blob),
        Value::Array(entries) => entries
            .iter()
            .filter_map(parse_journal_entry_json)
            .collect(),
        _ => Vec::new(),
    }
}

/// Parse a ServiceNow journal text blob into individual [`JournalEntry`] values.
///
/// Normal journal blobs contain one or more entries, each preceded by a header
/// line in the format `YYYY-MM-DD HH:MM:SS - Author Name (Source Type)`.
///
/// Content that appears *before* the first header — or in a blob with no
/// headers at all — is emitted as an unattributed entry (empty author,
/// current timestamp) rather than being silently dropped. This handles
/// activity-stream content that lacks standard journal headers.
fn parse_journal_blob(blob: &str) -> Vec<JournalEntry> {
    let mut entries = Vec::new();
    let mut current_timestamp = None;
    let mut current_author = String::new();
    // Lines that appear before the first recognized header. These are
    // flushed as an unattributed entry when the first header is found,
    // or at the end if no headers exist in the blob.
    let mut preamble = Vec::new();
    let mut body = Vec::new();

    for line in blob.lines() {
        if let Some((timestamp, author)) = parse_journal_header(line) {
            // Flush preamble lines before first header as unattributed entry
            if current_timestamp.is_none() && !preamble.is_empty() {
                let text = preamble.join("\n").trim().to_string();
                if !text.is_empty() {
                    entries.push(JournalEntry {
                        timestamp: Utc::now(),
                        author: String::new(),
                        body: text,
                    });
                }
                preamble.clear();
            }
            if let Some(timestamp) = current_timestamp.take() {
                entries.push(JournalEntry {
                    timestamp,
                    author: current_author.clone(),
                    body: body.join("\n").trim().to_string(),
                });
                body.clear();
            }
            current_timestamp = Some(timestamp);
            current_author = author;
        } else if current_timestamp.is_none() {
            preamble.push(line.to_string());
        } else {
            body.push(line.to_string());
        }
    }

    if let Some(timestamp) = current_timestamp {
        entries.push(JournalEntry {
            timestamp,
            author: current_author,
            body: body.join("\n").trim().to_string(),
        });
    } else if !preamble.is_empty() {
        // Entire blob had no headers
        let text = preamble.join("\n").trim().to_string();
        if !text.is_empty() {
            entries.push(JournalEntry {
                timestamp: Utc::now(),
                author: String::new(),
                body: text,
            });
        }
    }

    entries
}

fn parse_journal_header(line: &str) -> Option<(DateTime<Utc>, String)> {
    if line.len() < 22 || !line.contains(" - ") {
        return None;
    }
    let timestamp = chrono::NaiveDateTime::parse_from_str(line.get(..19)?, "%Y-%m-%d %H:%M:%S")
        .ok()
        .map(|dt| DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))?;
    if line.get(19..22)? != " - " {
        return None;
    }
    let remainder = line.get(22..)?;
    let author = remainder.split('(').next()?.trim().to_string();
    Some((timestamp, author))
}

fn parse_journal_entry_json(value: &Value) -> Option<JournalEntry> {
    let map = value.as_object()?;
    let timestamp = map
        .get("timestamp")
        .and_then(Value::as_str)
        .and_then(parse_iso_timestamp)
        .unwrap_or_else(Utc::now);
    let author = map
        .get("author")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let body = map
        .get("body")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    Some(JournalEntry {
        timestamp,
        author,
        body,
    })
}

fn parse_timestamp(json: &Value, field_name: &str) -> Option<DateTime<Utc>> {
    let value = json.get(field_name)?;
    match value {
        Value::String(text) => parse_iso_timestamp(text),
        Value::Object(map) => map
            .get("value")
            .and_then(Value::as_str)
            .and_then(parse_iso_timestamp)
            .or_else(|| {
                map.get("display_value")
                    .and_then(Value::as_str)
                    .and_then(parse_iso_timestamp)
            }),
        _ => None,
    }
}

fn parse_iso_timestamp(input: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(input)
        .map(|dt| dt.with_timezone(&Utc))
        .ok()
        .or_else(|| {
            chrono::NaiveDateTime::parse_from_str(input, "%Y-%m-%d %H:%M:%S")
                .ok()
                .map(|dt| DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
        })
}

fn json_value_to_string(value: &Value) -> String {
    match value {
        Value::String(text) => text.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

fn json_string_field(json: &Value, field_name: &str) -> Option<String> {
    match json.get(field_name)? {
        Value::String(text) => Some(text.clone()),
        Value::Object(map) => map
            .get("value")
            .and_then(Value::as_str)
            .map(str::to_string)
            .or_else(|| {
                map.get("display_value")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            }),
        _ => None,
    }
}

fn json_string_array_field(json: &Value, field_name: &str) -> Option<Vec<String>> {
    let values = json.get(field_name)?.as_array()?;
    Some(
        values
            .iter()
            .filter_map(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
            .collect(),
    )
}

fn degraded_reason_from_error(err: &anyhow::Error) -> DegradedReadReason {
    for cause in err.chain() {
        if let Some(io_error) = cause.downcast_ref::<std::io::Error>() {
            return if io_error.kind() == ErrorKind::NotFound {
                DegradedReadReason::MissingFile
            } else {
                DegradedReadReason::UnreadableFile {
                    error: err.to_string(),
                }
            };
        }
    }

    DegradedReadReason::ParseFailure {
        error: err.to_string(),
    }
}

/// Returns `true` if the query looks like an exact ServiceNow record number.
///
/// Matches patterns like `INC4992697`, `CHG0325640`, `CTASK0657426` —
/// one or more ASCII letters followed by one or more digits, nothing else.
/// Case-insensitive.
pub fn is_exact_record_number(query: &str) -> bool {
    let trimmed = query.trim();
    if trimmed.is_empty() || trimmed.contains(' ') {
        return false;
    }
    let boundary = trimmed.find(|ch: char| ch.is_ascii_digit()).unwrap_or(0);
    if boundary == 0 || boundary == trimmed.len() {
        return false;
    }
    let (prefix, digits) = trimmed.split_at(boundary);
    prefix.chars().all(|ch| ch.is_ascii_alphabetic())
        && digits.chars().all(|ch| ch.is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::store::{
        AliasRow, BusinessApplicationServerMembershipRow, KeywordRow, RecordRow, Store, TagRow,
    };
    use crate::vault::VaultManager;
    use crate::{ApprovalRecord, CacheSource, FieldValue, KnowledgeArticle, RecordRef, Reference};
    use chrono::TimeZone;
    use std::collections::HashMap;
    use std::fs;
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;
    use std::path::Path;
    use tempfile::tempdir;

    fn sample_row(
        sys_id: &str,
        number: &str,
        table_name: &str,
        resource_type: ResourceType,
    ) -> RecordRow {
        RecordRow {
            sys_id: sys_id.to_string(),
            number: number.to_string(),
            table_name: table_name.to_string(),
            resource_type,
            state: Some("Open".to_string()),
            short_desc: Some("Short".to_string()),
            description: Some("Body".to_string()),
            assigned_to: Some("user-1".to_string()),
            parent_id: None,
            file_path: None,
            synced_at: Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
            sys_updated_on: Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
            etag: Some("1".to_string()),
            in_scope: true,
            last_seen_at: Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
            tombstoned_at: None,
            pruned_at: None,
            raw_json: serde_json::json!({
                "sys_id": sys_id,
                "number": number,
                "short_description": "Short",
                "description": "Body",
                "state": "Open",
                "assigned_to": {
                    "sys_id": "user-1",
                    "display_value": "User 1",
                    "table": "sys_user"
                }
            })
            .to_string(),
        }
    }

    fn sample_record(sys_id: &str, number: &str, resource_type: ResourceType) -> SnowRecord {
        SnowRecord {
            sys_id: sys_id.to_string(),
            number: number.to_string(),
            table: match resource_type {
                ResourceType::Task => "task".to_string(),
                ResourceType::Knowledge => "kb_knowledge".to_string(),
                ResourceType::Approval => "sysapproval_approver".to_string(),
                _ => "incident".to_string(),
            },
            resource_type,
            state: "Open".to_string(),
            short_description: "Vault Short".to_string(),
            description: "Vault Body".to_string(),
            fields: HashMap::from([(
                "assigned_to".to_string(),
                FieldValue {
                    value: "user-1".to_string(),
                    display_value: Some("User 1".to_string()),
                },
            )]),
            work_notes: Vec::new(),
            comments: Vec::new(),
            parent: None,
            children: Vec::new(),
            references: HashMap::new(),
            synced_at: Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
            source: CacheSource::Disk,
        }
    }

    fn sample_knowledge_article() -> KnowledgeArticle {
        KnowledgeArticle {
            record: SnowRecord {
                sys_id: "kb-sys".to_string(),
                number: "KB001".to_string(),
                table: "kb_knowledge".to_string(),
                resource_type: ResourceType::Knowledge,
                state: "published".to_string(),
                short_description: "VPN Runbook".to_string(),
                description: "KB summary".to_string(),
                fields: HashMap::from([(
                    "workflow_state".to_string(),
                    FieldValue {
                        value: "published".to_string(),
                        display_value: Some("published".to_string()),
                    },
                )]),
                work_notes: Vec::new(),
                comments: Vec::new(),
                parent: None,
                children: Vec::new(),
                references: HashMap::new(),
                synced_at: Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
                source: CacheSource::Disk,
            },
            knowledge_base: Reference {
                sys_id: "kb-base".to_string(),
                table: "kb_knowledge_base".to_string(),
                display_name: "IT".to_string(),
                extra: HashMap::new(),
            },
            category: Reference {
                sys_id: "kb-cat".to_string(),
                table: "kb_category".to_string(),
                display_name: "Network".to_string(),
                extra: HashMap::new(),
            },
            article_type: "text".to_string(),
            content: "Step 1: Verify the gateway.".to_string(),
            sn_tags: vec!["vpn".to_string()],
            auto_tags: vec!["gateway".to_string()],
            user_tags: vec!["runbook".to_string()],
            body_cached: true,
            published_at: Some(Utc.timestamp_opt(1_712_649_600, 0).unwrap()),
            author: Some(Reference {
                sys_id: "user-1".to_string(),
                table: "sys_user".to_string(),
                display_name: "Casey User".to_string(),
                extra: HashMap::new(),
            }),
            valid_to: Some(chrono::NaiveDate::from_ymd_opt(2027, 1, 1).unwrap()),
        }
    }

    fn sample_approval_record() -> ApprovalRecord {
        ApprovalRecord {
            record: SnowRecord {
                sys_id: "apr-sys".to_string(),
                number: "APR001".to_string(),
                table: "sysapproval_approver".to_string(),
                resource_type: ResourceType::Approval,
                state: "requested".to_string(),
                short_description: "CAB approval for CHG001".to_string(),
                description: String::new(),
                fields: HashMap::new(),
                work_notes: Vec::new(),
                comments: Vec::new(),
                parent: None,
                children: Vec::new(),
                references: HashMap::new(),
                synced_at: Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
                source: CacheSource::Disk,
            },
            approver: Reference {
                sys_id: "user-1".to_string(),
                table: "sys_user".to_string(),
                display_name: "User 1".to_string(),
                extra: HashMap::new(),
            },
            target: RecordRef {
                sys_id: "chg-sys".to_string(),
                number: "CHG001".to_string(),
                table: "change_request".to_string(),
            },
            requested_at: Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
            due_date: None,
            routed_via: ApprovalRoutedVia::Direct,
            approver_group: None,
        }
    }

    #[test]
    fn list_query_filters_rows() {
        let row = sample_row("sys-1", "INC001", "incident", ResourceType::Incident);
        assert!(
            ListQuery::new()
                .resource_type(ResourceType::Incident)
                .matches(&row)
        );
        assert!(
            !ListQuery::new()
                .resource_type(ResourceType::Change)
                .matches(&row)
        );
        assert!(ListQuery::new().assigned_to("user-1").matches(&row));
    }

    #[test]
    fn approval_query_filters_rows() {
        let mut row = sample_row(
            "sys-1",
            "APR001",
            "sysapproval_approver",
            ResourceType::Approval,
        );
        row.state = Some("requested".to_string());
        row.raw_json = serde_json::json!({
            "sys_id": "sys-1",
            "number": "APR001",
            "state": "requested",
            "approver": {
                "sys_id": "user-1",
                "display_value": "User 1",
                "table": "sys_user"
            }
        })
        .to_string();
        assert!(ApprovalQuery::pending().matches(&row));
    }

    #[test]
    fn query_engine_reads_records() {
        let store = Store::open_in_memory().expect("store");
        let row = sample_row("sys-1", "INC001", "incident", ResourceType::Incident);
        store.upsert_record(&row, "", "").expect("insert");
        let engine = QueryEngine::from_store(store);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let record = runtime
            .block_on(engine.get_record("INC001"))
            .expect("query")
            .expect("row");
        assert_eq!(record.number, "INC001");
        assert_eq!(record.resource_type, ResourceType::Incident);
    }

    #[test]
    fn query_engine_prefers_markdown_when_file_path_exists() {
        let tempdir = tempdir().expect("tempdir");
        let vault = VaultManager::new(tempdir.path());
        let store = Store::open_in_memory().expect("store");

        let record = sample_record("sys-1", "INC001", ResourceType::Incident);
        let path = vault.write_record(&record).expect("write record");
        let mut row = sample_row("sys-1", "INC001", "incident", ResourceType::Incident);
        row.short_desc = Some("SQLite Short".to_string());
        row.description = Some("SQLite Body".to_string());
        row.file_path = Some(
            vault
                .relative_path(path)
                .expect("relative path")
                .to_string_lossy()
                .into_owned(),
        );
        store.upsert_record(&row, "", "").expect("insert");

        let engine = QueryEngine::from_store_with_vault(store, Some(tempdir.path().to_path_buf()));
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let loaded = runtime
            .block_on(engine.get_record("INC001"))
            .expect("query")
            .expect("record");

        assert_eq!(loaded.short_description, "Vault Short");
        assert_eq!(loaded.description, "Vault Body");
    }

    #[test]
    fn query_engine_falls_back_to_raw_json_when_markdown_missing() {
        let tempdir = tempdir().expect("tempdir");
        let store = Store::open_in_memory().expect("store");
        let mut row = sample_row("sys-1", "INC001", "incident", ResourceType::Incident);
        row.file_path = Some("incidents/INC001.md".to_string());
        store.upsert_record(&row, "", "").expect("insert");

        let engine = QueryEngine::from_store_with_vault(store, Some(tempdir.path().to_path_buf()));
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let loaded = runtime
            .block_on(engine.get_record("INC001"))
            .expect("query")
            .expect("record");

        assert_eq!(loaded.short_description, "Short");
        assert_eq!(loaded.description, "Body");
    }

    #[test]
    fn query_engine_records_missing_markdown_file() {
        let tempdir = tempdir().expect("tempdir");
        let store = Store::open_in_memory().expect("store");
        let mut row = sample_row("sys-1", "INC001", "incident", ResourceType::Incident);
        row.file_path = Some("incidents/INC001.md".to_string());
        store.upsert_record(&row, "", "").expect("insert");

        let engine = QueryEngine::from_store_with_vault(store, Some(tempdir.path().to_path_buf()));
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let loaded = runtime
            .block_on(engine.get_record("INC001"))
            .expect("query")
            .expect("record");

        let expected_path = tempdir.path().join("incidents/INC001.md");
        assert_eq!(loaded.short_description, "Short");
        assert_eq!(engine.degraded_reads().len(), 1);
        let diagnostic = &engine.degraded_reads()[0];
        assert_eq!(diagnostic.number, "INC001");
        assert_eq!(diagnostic.reason, DegradedReadReason::MissingFile);
        assert_eq!(
            diagnostic.markdown_path.as_deref(),
            Some(expected_path.as_path())
        );
    }

    #[test]
    fn query_engine_records_raw_json_fallback_when_no_vault_is_available() {
        let store = Store::open_in_memory().expect("store");
        let mut row = sample_row("sys-1", "INC001", "incident", ResourceType::Incident);
        row.file_path = Some("incidents/INC001.md".to_string());
        store.upsert_record(&row, "", "").expect("insert");

        let engine = QueryEngine::from_store(store);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let loaded = runtime
            .block_on(engine.get_record("INC001"))
            .expect("query")
            .expect("record");

        assert_eq!(loaded.short_description, "Short");
        let degraded = engine.degraded_reads();
        assert_eq!(degraded.len(), 1);
        assert_eq!(degraded[0].reason, DegradedReadReason::RawJsonFallback);
        assert_eq!(
            degraded[0].markdown_path.as_deref(),
            Some(Path::new("incidents/INC001.md"))
        );
    }

    #[test]
    fn query_engine_records_parse_failure() {
        let tempdir = tempdir().expect("tempdir");
        let markdown_path = tempdir.path().join("incidents/INC001.md");
        fs::create_dir_all(markdown_path.parent().expect("parent")).expect("dir");
        fs::write(
            &markdown_path,
            "---\nnumber: INC001\nresource_type: incident\n",
        )
        .expect("write broken markdown");

        let store = Store::open_in_memory().expect("store");
        let mut row = sample_row("sys-1", "INC001", "incident", ResourceType::Incident);
        row.file_path = Some("incidents/INC001.md".to_string());
        store.upsert_record(&row, "", "").expect("insert");

        let engine = QueryEngine::from_store_with_vault(store, Some(tempdir.path().to_path_buf()));
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let loaded = runtime
            .block_on(engine.get_record("INC001"))
            .expect("query")
            .expect("record");

        assert_eq!(loaded.short_description, "Short");
        let degraded = engine.degraded_reads();
        assert_eq!(degraded.len(), 1);
        match &degraded[0].reason {
            DegradedReadReason::ParseFailure { error } => {
                assert!(error.contains("markdown missing closing frontmatter delimiter"));
            }
            other => panic!("unexpected reason: {:?}", other),
        }
        assert_eq!(
            degraded[0].markdown_path.as_deref(),
            Some(markdown_path.as_path())
        );
    }

    #[cfg(unix)]
    #[test]
    fn query_engine_records_unreadable_markdown_file() {
        let tempdir = tempdir().expect("tempdir");
        let markdown_path = tempdir.path().join("incidents/INC001.md");
        fs::create_dir_all(markdown_path.parent().expect("parent")).expect("dir");
        fs::write(&markdown_path, "not markdown").expect("write markdown");
        let mut permissions = fs::metadata(&markdown_path)
            .expect("metadata")
            .permissions();
        permissions.set_mode(0o000);
        fs::set_permissions(&markdown_path, permissions).expect("permissions");

        let store = Store::open_in_memory().expect("store");
        let mut row = sample_row("sys-1", "INC001", "incident", ResourceType::Incident);
        row.file_path = Some("incidents/INC001.md".to_string());
        store.upsert_record(&row, "", "").expect("insert");

        let engine = QueryEngine::from_store_with_vault(store, Some(tempdir.path().to_path_buf()));
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let loaded = runtime
            .block_on(engine.get_record("INC001"))
            .expect("query")
            .expect("record");

        assert_eq!(loaded.short_description, "Short");
        let degraded = engine.degraded_reads();
        assert_eq!(degraded.len(), 1);
        match &degraded[0].reason {
            DegradedReadReason::UnreadableFile { error } => {
                assert!(!error.trim().is_empty());
            }
            other => panic!("unexpected reason: {:?}", other),
        }
        assert_eq!(
            degraded[0].markdown_path.as_deref(),
            Some(markdown_path.as_path())
        );
    }

    #[test]
    fn query_engine_reads_knowledge_article_from_vault() {
        let tempdir = tempdir().expect("tempdir");
        let vault = VaultManager::new(tempdir.path());
        let store = Store::open_in_memory().expect("store");

        let article = sample_knowledge_article();
        let path = vault
            .write_knowledge_article(&article)
            .expect("write article");
        let mut row = sample_row("kb-sys", "KB001", "kb_knowledge", ResourceType::Knowledge);
        row.short_desc = Some("SQLite Summary".to_string());
        row.description = Some("SQLite Body".to_string());
        row.file_path = Some(
            vault
                .relative_path(path)
                .expect("relative path")
                .to_string_lossy()
                .into_owned(),
        );
        store.upsert_record(&row, "", "").expect("insert");

        let engine = QueryEngine::from_store_with_vault(store, Some(tempdir.path().to_path_buf()));
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let loaded = runtime
            .block_on(engine.get_knowledge_article("KB001"))
            .expect("query")
            .expect("article");

        assert_eq!(loaded.record.short_description, "VPN Runbook");
        assert_eq!(loaded.content, "Step 1: Verify the gateway.");
        assert_eq!(loaded.knowledge_base.display_name, "IT");
        assert_eq!(
            loaded
                .author
                .as_ref()
                .map(|author| author.display_name.as_str()),
            Some("Casey User")
        );
        assert_eq!(loaded.published_at, article.published_at);
    }

    #[test]
    fn query_engine_normalizes_knowledge_reference_labels_from_raw_json() {
        let store = Store::open_in_memory().expect("store");
        let unresolved_sys_id = "0e3952d41b7d15032d1ece5624bcb4e";
        let mut row = sample_row("kb-raw", "KB003", "kb_knowledge", ResourceType::Knowledge);
        row.short_desc = Some("Access guide".to_string());
        row.description = Some("How to request access".to_string());
        row.raw_json = serde_json::json!({
            "sys_id": "kb-raw",
            "number": "KB003",
            "short_description": "Access guide",
            "description": "How to request access",
            "state": "published",
            "workflow_state": "published",
            "article_body": "How to request access",
            "article_type": "text",
            "knowledge_base": {
                "sys_id": "kb-base",
                "display_value": unresolved_sys_id,
                "name": "IT Operations",
                "table": "kb_knowledge_base"
            },
            "category": {
                "sys_id": "kb-access",
                "display_value": unresolved_sys_id,
                "title": "Access Requests",
                "table": "kb_category"
            },
            "author": {
                "sys_id": "user-1",
                "display_value": unresolved_sys_id,
                "email": "casey.user@example.com",
                "table": "sys_user"
            }
        })
        .to_string();
        store
            .upsert_record(&row, "", "How to request access")
            .expect("insert");

        let engine = QueryEngine::from_store(store);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let loaded = runtime
            .block_on(engine.get_knowledge_article("KB003"))
            .expect("query")
            .expect("article");

        assert_eq!(loaded.knowledge_base.display_name, "IT Operations");
        assert_eq!(loaded.category.display_name, "Access Requests");
        assert_eq!(
            loaded
                .author
                .as_ref()
                .map(|author| author.display_name.as_str()),
            Some("casey.user@example.com")
        );
        assert_eq!(
            loaded
                .record
                .references
                .get("author")
                .map(|reference| reference.display_name.as_str()),
            Some("casey.user@example.com")
        );
    }

    #[test]
    fn query_engine_search_knowledge_applies_filters() {
        let store = Store::open_in_memory().expect("store");

        let mut first = sample_row("kb-1", "KB001", "kb_knowledge", ResourceType::Knowledge);
        first.short_desc = Some("VPN Runbook".to_string());
        first.description = Some("Gateway verification".to_string());
        first.raw_json = serde_json::json!({
            "sys_id": "kb-1",
            "number": "KB001",
            "short_description": "VPN Runbook",
            "description": "Gateway verification",
            "state": "published",
            "workflow_state": "published",
            "article_body": "Gateway verification",
            "article_type": "text",
            "published": "2026-04-10 09:00:00",
            "knowledge_base": { "sys_id": "kb-base", "display_value": "IT", "table": "kb_knowledge_base" },
            "category": { "sys_id": "kb-net", "display_value": "Network", "table": "kb_category" },
            "author": { "sys_id": "user-1", "display_value": "Casey User", "table": "sys_user" }
        })
        .to_string();
        store
            .upsert_record(&first, "", "Gateway verification")
            .expect("insert first");
        store
            .upsert_knowledge_article(&crate::cache::store::KnowledgeArticleRow {
                record_sys_id: "kb-1".to_string(),
                number: "KB001".to_string(),
                title: "VPN Runbook".to_string(),
                workflow_state: "published".to_string(),
                knowledge_base_sys_id: "kb-base".to_string(),
                knowledge_base_name: "IT".to_string(),
                category_sys_id: "kb-net".to_string(),
                category_name: "Network".to_string(),
                author_sys_id: Some("user-1".to_string()),
                author_name: Some("Casey User".to_string()),
                published_at: Some("2026-04-10 09:00:00".to_string()),
                valid_to: None,
                article_type: "text".to_string(),
                sys_updated_on: Some("2026-04-10 09:00:00".to_string()),
                sn_tags: vec!["vpn".to_string()],
                auto_tags: vec!["gateway".to_string()],
                user_tags: vec!["runbook".to_string()],
                body_cached: true,
            })
            .expect("project first");

        let mut second = sample_row("kb-2", "KB002", "kb_knowledge", ResourceType::Knowledge);
        second.short_desc = Some("Windows Access".to_string());
        second.description = Some("Gateway access instructions".to_string());
        second.raw_json = serde_json::json!({
            "sys_id": "kb-2",
            "number": "KB002",
            "short_description": "Windows Access",
            "description": "Gateway access instructions",
            "state": "published",
            "workflow_state": "published",
            "article_body": "Gateway access instructions",
            "article_type": "text",
            "knowledge_base": { "sys_id": "kb-base", "display_value": "IT", "table": "kb_knowledge_base" },
            "category": { "sys_id": "kb-access", "display_value": "Access", "table": "kb_category" }
        })
        .to_string();
        store
            .upsert_record(&second, "", "Gateway access instructions")
            .expect("insert second");
        store
            .upsert_knowledge_article(&crate::cache::store::KnowledgeArticleRow {
                record_sys_id: "kb-2".to_string(),
                number: "KB002".to_string(),
                title: "Windows Access".to_string(),
                workflow_state: "published".to_string(),
                knowledge_base_sys_id: "kb-base".to_string(),
                knowledge_base_name: "IT".to_string(),
                category_sys_id: "kb-access".to_string(),
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
            .expect("project second");

        let engine = QueryEngine::from_store(store);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let results = runtime
            .block_on(engine.search_knowledge(
                "gateway",
                KnowledgeSearchFilters {
                    knowledge_base: Some("kb-base".to_string()),
                    category: Some("Access".to_string()),
                    limit: Some(5),
                },
            ))
            .expect("search knowledge");

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].record.number, "KB002");
    }

    #[test]
    fn query_engine_reads_approval_from_vault() {
        let tempdir = tempdir().expect("tempdir");
        let vault = VaultManager::new(tempdir.path());
        let store = Store::open_in_memory().expect("store");

        let approval = sample_approval_record();
        let path = vault.write_approval(&approval).expect("write approval");
        let mut row = sample_row(
            "apr-sys",
            "APR001",
            "sysapproval_approver",
            ResourceType::Approval,
        );
        row.state = Some("requested".to_string());
        row.file_path = Some(
            vault
                .relative_path(path)
                .expect("relative path")
                .to_string_lossy()
                .into_owned(),
        );
        store.upsert_record(&row, "", "").expect("insert");

        let engine = QueryEngine::from_store_with_vault(store, Some(tempdir.path().to_path_buf()));
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let loaded = runtime
            .block_on(engine.my_approvals())
            .expect("query approvals");

        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].approver.display_name, "User 1");
        assert_eq!(loaded[0].target.number, "CHG001");
    }

    #[test]
    fn query_engine_prefers_non_approval_for_generic_number_lookup() {
        let store = Store::open_in_memory().expect("store");
        let mut change = sample_row("chg-sys", "CHG001", "change_request", ResourceType::Change);
        change.short_desc = Some("Primary change".to_string());
        let mut approval = sample_row(
            "apr-sys",
            "CHG001",
            "sysapproval_approver",
            ResourceType::Approval,
        );
        approval.short_desc = Some("Approval shadow".to_string());
        store.upsert_record(&change, "", "").expect("insert change");
        store
            .upsert_record(&approval, "", "")
            .expect("insert approval");

        let engine = QueryEngine::from_store(store);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let loaded = runtime
            .block_on(engine.get_record("CHG001"))
            .expect("query")
            .expect("record");

        assert_eq!(loaded.sys_id, "chg-sys");
        assert_eq!(loaded.short_description, "Primary change");
    }

    #[test]
    fn query_engine_searches_by_tag() {
        let store = Store::open_in_memory().expect("store");
        let row = sample_row("sys-1", "INC001", "incident", ResourceType::Incident);
        store.upsert_record(&row, "", "").expect("insert");
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
            .expect("replace tags");
        let engine = QueryEngine::from_store(store);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let results = runtime
            .block_on(engine.search_by_tag("vpn", SearchScope::All))
            .expect("search by tag");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].record.number, "INC001");
        assert_eq!(results[0].match_in, MatchField::Tag);
        assert_eq!(results[0].matched_value.as_deref(), Some("vpn"));
        assert_eq!(
            results[0].reasons,
            vec![SearchMatchReason {
                field: MatchField::Tag,
                value: "vpn".to_string(),
            }]
        );
    }

    #[test]
    fn query_engine_search_by_keyword_respects_scope() {
        let store = Store::open_in_memory().expect("store");
        let row = sample_row("sys-1", "KB001", "kb_knowledge", ResourceType::Knowledge);
        store.upsert_record(&row, "", "").expect("insert");
        store
            .replace_keywords(
                "sys-1",
                &[KeywordRow {
                    record_sys_id: "sys-1".to_string(),
                    keyword: "gateway".to_string(),
                    source: "derived".to_string(),
                    weight: 1.0,
                }],
            )
            .expect("replace keywords");
        let engine = QueryEngine::from_store(store);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let results = runtime
            .block_on(engine.search_by_keyword("gateway", SearchScope::Knowledge))
            .expect("search by keyword");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].record.number, "KB001");
        assert_eq!(results[0].match_in, MatchField::Keyword);
        assert_eq!(results[0].matched_value.as_deref(), Some("gateway"));
        assert_eq!(
            results[0].reasons,
            vec![SearchMatchReason {
                field: MatchField::Keyword,
                value: "gateway".to_string(),
            }]
        );

        let empty = runtime
            .block_on(engine.search_by_keyword("gateway", SearchScope::WorkNotes))
            .expect("search by keyword filtered");
        assert!(empty.is_empty());
    }

    #[test]
    fn query_engine_search_by_alias_returns_explanation() {
        let store = Store::open_in_memory().expect("store");
        let row = sample_row("sys-1", "INC001", "incident", ResourceType::Incident);
        store.upsert_record(&row, "", "").expect("insert");
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
        let engine = QueryEngine::from_store(store);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let results = runtime
            .block_on(engine.search_by_alias("vpn issue", SearchScope::All))
            .expect("search by alias");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].record.number, "INC001");
        assert_eq!(results[0].match_in, MatchField::Alias);
        assert_eq!(results[0].matched_value.as_deref(), Some("vpn issue"));
        assert_eq!(
            results[0].reasons,
            vec![SearchMatchReason {
                field: MatchField::Alias,
                value: "vpn issue".to_string(),
            }]
        );
    }

    #[test]
    fn query_engine_search_by_alias_returns_empty_for_missing_alias() {
        let store = Store::open_in_memory().expect("store");
        let row = sample_row("sys-1", "INC001", "incident", ResourceType::Incident);
        store.upsert_record(&row, "", "").expect("insert");
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
        let engine = QueryEngine::from_store(store);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let results = runtime
            .block_on(engine.search_by_alias("missing alias", SearchScope::All))
            .expect("search by alias");
        assert!(results.is_empty());
    }

    #[test]
    fn query_engine_search_enriched_merges_alias_and_keyword_scores() {
        let store = Store::open_in_memory().expect("store");
        let row = sample_row("sys-1", "INC001", "incident", ResourceType::Incident);
        let mut alias_row = row.clone();
        alias_row.short_desc = Some("Alpha".to_string());
        alias_row.description = Some("Alpha".to_string());
        store.upsert_record(&alias_row, "", "").expect("insert");
        store
            .replace_aliases(
                "sys-1",
                &[AliasRow {
                    record_sys_id: "sys-1".to_string(),
                    alias: "gateway".to_string(),
                    kind: "short_title".to_string(),
                    source: "derived".to_string(),
                }],
            )
            .expect("alias");
        store
            .replace_keywords(
                "sys-1",
                &[KeywordRow {
                    record_sys_id: "sys-1".to_string(),
                    keyword: "gateway".to_string(),
                    source: "derived".to_string(),
                    weight: 1.0,
                }],
            )
            .expect("keyword");
        let engine = QueryEngine::from_store(store);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let results = runtime
            .block_on(engine.search_enriched("gateway", SearchScope::All))
            .expect("search enriched");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].record.number, "INC001");
        assert_eq!(results[0].match_in, MatchField::Alias);
        assert_eq!(results[0].score, 150);
        assert_eq!(results[0].matched_value.as_deref(), Some("gateway"));
        assert_eq!(
            results[0]
                .reasons
                .iter()
                .map(|reason| (reason.field, reason.value.as_str()))
                .collect::<Vec<_>>(),
            vec![
                (MatchField::Alias, "gateway"),
                (MatchField::Keyword, "gateway"),
            ]
        );
    }

    #[test]
    fn query_engine_reads_cached_business_application_server_relationships_both_directions() {
        let store = Store::open_in_memory().expect("store");
        let now = Utc.timestamp_opt(1_779_840_000, 0).unwrap();
        let ba_sys_id = "54a4b61b6fe845000ed852a03f3ee4d0";
        let server_sys_id = "7f4a6e2f1c23456789abcdef01234567";

        let mut application = sample_row(
            ba_sys_id,
            "APM0000001",
            "cmdb_ci_business_app",
            ResourceType::BusinessApplication,
        );
        application.short_desc = Some("Payments Platform".to_string());
        application.raw_json = serde_json::json!({
            "sys_id": ba_sys_id,
            "number": "APM0000001",
            "name": "Payments Platform",
            "short_description": "Payments Platform",
            "business_owner": {
                "value": "6816f79cc0a8016401c5a33be04be441",
                "display_value": "Example Owner"
            }
        })
        .to_string();
        store
            .upsert_record(&application, "", "")
            .expect("insert application");

        let mut server = sample_row(
            server_sys_id,
            "SRV0000001",
            "cmdb_ci_linux_server",
            ResourceType::Server,
        );
        server.short_desc = Some("app-server-01".to_string());
        server.raw_json = serde_json::json!({
            "sys_id": server_sys_id,
            "number": "SRV0000001",
            "name": "app-server-01",
            "ip_address": "10.0.0.10",
            "sys_class_name": "cmdb_ci_linux_server"
        })
        .to_string();
        store.upsert_record(&server, "", "").expect("insert server");

        store
            .upsert_business_application_server_membership(
                &BusinessApplicationServerMembershipRow {
                    ba_sys_id: ba_sys_id.to_string(),
                    server_sys_id: server_sys_id.to_string(),
                    server_table: "cmdb_ci_linux_server".to_string(),
                    provenance: "cmdb_rel_ci".to_string(),
                    min_depth: 1,
                    paths_json: serde_json::json!([{
                        "edges": [{
                            "depth": 1,
                            "parent_sys_id": ba_sys_id,
                            "child_sys_id": server_sys_id,
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
                },
            )
            .expect("insert membership");

        let engine = QueryEngine::from_store(store);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");

        let forward = runtime
            .block_on(engine.business_application_servers_cached(
                BusinessApplicationServersCachedParams {
                    number: Some("APM0000001".to_string()),
                    ..Default::default()
                },
            ))
            .expect("forward query")
            .expect("application found");
        assert_eq!(forward.business_application.sys_id, ba_sys_id);
        assert_eq!(forward.servers.len(), 1);
        assert_eq!(server_name(&forward.servers[0].server), "app-server-01");
        assert_eq!(forward.servers[0].provenance, "cmdb_rel_ci");
        assert_eq!(forward.servers[0].min_depth, 1);
        assert_eq!(forward.servers[0].paths[0].depth(), 1);

        let reverse = runtime
            .block_on(engine.business_applications_for_server(
                BusinessApplicationsForServerParams {
                    name: Some("app-server-01".to_string()),
                    ..Default::default()
                },
            ))
            .expect("reverse query")
            .expect("server found");
        assert_eq!(reverse.servers.len(), 1);
        assert_eq!(reverse.servers[0].server.sys_id, server_sys_id);
        assert_eq!(reverse.servers[0].business_applications.len(), 1);
        assert_eq!(
            reverse.servers[0].business_applications[0]
                .business_application
                .number,
            "APM0000001"
        );
        assert_eq!(
            reverse.servers[0].business_applications[0].provenance,
            "cmdb_rel_ci"
        );
    }

    #[test]
    fn query_engine_search_enriched_prefers_manual_sources_and_explains_the_winner() {
        let store = Store::open_in_memory().expect("store");
        let manual_row = sample_row("sys-manual", "INC010", "incident", ResourceType::Incident);
        let derived_row = sample_row("sys-derived", "INC020", "incident", ResourceType::Incident);
        store
            .upsert_record(&manual_row, "", "")
            .expect("insert manual");
        store
            .upsert_record(&derived_row, "", "")
            .expect("insert derived");
        store
            .replace_tags(
                "sys-manual",
                &[TagRow {
                    record_sys_id: "sys-manual".to_string(),
                    tag: "vpn".to_string(),
                    source: "manual".to_string(),
                    weight: 1.0,
                }],
            )
            .expect("manual tag");
        store
            .replace_tags(
                "sys-derived",
                &[TagRow {
                    record_sys_id: "sys-derived".to_string(),
                    tag: "vpn".to_string(),
                    source: "derived".to_string(),
                    weight: 1.0,
                }],
            )
            .expect("derived tag");
        let engine = QueryEngine::from_store(store);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let results = runtime
            .block_on(engine.search_enriched("vpn", SearchScope::All))
            .expect("search enriched");
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].record.number, "INC010");
        assert_eq!(results[0].score, 90);
        assert_eq!(results[0].match_in, MatchField::Tag);
        assert_eq!(results[0].matched_value.as_deref(), Some("vpn"));
        assert_eq!(
            results[0].reasons,
            vec![SearchMatchReason {
                field: MatchField::Tag,
                value: "vpn".to_string(),
            }]
        );
        assert_eq!(results[1].record.number, "INC020");
        assert_eq!(results[1].score, 70);
        assert_eq!(results[1].match_in, MatchField::Tag);
    }

    #[test]
    fn query_engine_search_enriched_deduplicates_fts_and_tag_hits() {
        let store = Store::open_in_memory().expect("store");
        let row = sample_row("sys-1", "INC001", "incident", ResourceType::Incident);
        let mut fts_row = row.clone();
        fts_row.short_desc = Some("VPN outage".to_string());
        store.upsert_record(&fts_row, "", "").expect("insert");
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
            .expect("tag");
        let engine = QueryEngine::from_store(store);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let results = runtime
            .block_on(engine.search_enriched("vpn", SearchScope::All))
            .expect("search enriched");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].record.number, "INC001");
        assert_eq!(results[0].score, 110);
        assert_eq!(results[0].match_in, MatchField::Tag);
    }

    #[test]
    fn query_engine_search_enriched_orders_by_score_and_number() {
        let store = Store::open_in_memory().expect("store");
        let first = sample_row("sys-1", "INC001", "incident", ResourceType::Incident);
        let second = sample_row("sys-2", "INC002", "incident", ResourceType::Incident);
        store.upsert_record(&first, "", "").expect("insert first");
        store.upsert_record(&second, "", "").expect("insert second");
        let engine = QueryEngine::from_store(store);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let results = runtime
            .block_on(engine.search_enriched("Short", SearchScope::All))
            .expect("search enriched");
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].record.number, "INC001");
        assert_eq!(results[1].record.number, "INC002");
    }

    #[test]
    fn query_engine_search_enriched_tie_breaks_by_number_then_sys_id() {
        let store = Store::open_in_memory().expect("store");
        let first = sample_row("sys-a", "INC001", "incident", ResourceType::Incident);
        let second = sample_row("sys-b", "INC002", "incident", ResourceType::Incident);
        store.upsert_record(&first, "", "").expect("insert first");
        store.upsert_record(&second, "", "").expect("insert second");
        let engine = QueryEngine::from_store(store);
        let runtime = tokio::runtime::Runtime::new().expect("runtime");
        let results = runtime
            .block_on(engine.search_enriched("Short", SearchScope::All))
            .expect("search enriched");
        assert_eq!(results.len(), 2);
        assert_eq!(results[0].record.number, "INC001");
        assert_eq!(results[1].record.number, "INC002");
    }

    #[test]
    fn is_exact_record_number_accepts_valid_ids() {
        assert!(is_exact_record_number("INC4992697"));
        assert!(is_exact_record_number("CHG0325640"));
        assert!(is_exact_record_number("CTASK0657426"));
        assert!(is_exact_record_number("KB0105015"));
        assert!(is_exact_record_number("RITM1234567"));
        assert!(is_exact_record_number("PRJ0161206"));
        assert!(is_exact_record_number("DMNTSK0001122"));
        assert!(is_exact_record_number("STRY0423187"));
        assert!(is_exact_record_number("STSK0048820"));
        assert!(is_exact_record_number("REQ1234567"));
    }

    #[test]
    fn is_exact_record_number_rejects_non_ids() {
        assert!(!is_exact_record_number("multiple ports down"));
        assert!(!is_exact_record_number("INC"));
        assert!(!is_exact_record_number("1234567"));
        assert!(!is_exact_record_number(""));
        assert!(!is_exact_record_number("INC 4992697"));
        assert!(!is_exact_record_number("INC4992697 extra"));
        assert!(!is_exact_record_number("inc-4992697"));
    }

    #[test]
    fn is_exact_record_number_is_case_insensitive() {
        assert!(is_exact_record_number("inc4992697"));
        assert!(is_exact_record_number("Inc4992697"));
        assert!(is_exact_record_number("chg0325640"));
    }

    #[test]
    fn parse_journal_blob_emits_headerless_content() {
        let blob = "Current status: Smart hand ticket has been created for the FS to get the switch details.";
        let entries = parse_journal_blob(blob);
        assert_eq!(entries.len(), 1);
        assert_eq!(
            entries[0].body,
            "Current status: Smart hand ticket has been created for the FS to get the switch details."
        );
        assert!(entries[0].author.is_empty());
    }

    #[test]
    fn parse_journal_blob_emits_preamble_before_first_header() {
        let blob = "Orphaned preamble line\n\n2026-04-03 14:53:01 - Alice (Work notes)\nBody after header\n";
        let entries = parse_journal_blob(blob);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].body, "Orphaned preamble line");
        assert!(entries[0].author.is_empty());
        assert_eq!(entries[1].author, "Alice");
        assert_eq!(entries[1].body, "Body after header");
    }

    #[test]
    fn parse_journal_blob_normal_entries_unchanged() {
        let blob = "2026-04-03 14:53:01 - Alice (Work notes)\nFirst note\n\n2026-04-01 09:44:28 - Bob (Work notes)\nSecond note\n";
        let entries = parse_journal_blob(blob);
        assert_eq!(entries.len(), 2);
        assert_eq!(entries[0].author, "Alice");
        assert_eq!(entries[0].body, "First note");
        assert_eq!(entries[1].author, "Bob");
        assert_eq!(entries[1].body, "Second note");
    }
}
