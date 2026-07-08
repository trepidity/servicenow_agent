//! `RecordService` — generic record read/write, list/my-work hydration,
//! resource-plan listing, child expansion, field-choice lookup, state/assignee
//! mutation, and the search surface, extracted from the `SnowCore` god-object.
//!
//! Domain service extracted in Task 11 of the library boundary migration,
//! alongside `KnowledgeService`, `VaultService`, and `WriteService`. Every
//! method/helper/const/free-fn body below is moved verbatim from its former
//! `impl SnowCore` / free-fn location in `lib.rs`; the only edits are
//! `self.<helper>` → `self.ctx.<helper>` for helpers whose bodies live on
//! [`CoreContext`] (Task 6).
//!
//! The record-lookup normalizers (`normalize_record_lookup_sys_id`,
//! `normalize_record_lookup_table`, `is_record_lookup_table_allowed`,
//! `table_for_builtin_record_number`) stay `pub fn` and are re-exported from
//! `lib.rs` so external callers keep reaching them at `snow_core::*`.

use anyhow::Result;
use chrono::{DateTime, Utc};
use std::collections::HashSet;

use servicenow_rs::prelude::{DisplayValue, Order, Record, child_relation_for_table};

use crate::context::CoreContext;
use crate::query;
use crate::query::filter::ListQuery;
use crate::resource;
use crate::{
    BUSINESS_APPLICATION_TABLE, DegradedReadDiagnostic, FieldChoice, RecordLookup,
    ResolvedResourceFilter, ResourcePlanListError, ResourcePlanListInput, ResourcePlanListResponse,
    ResourcePlanQuerySummary, ResourcePlanResourceType, ResourceType, SERVER_RESOURCE_TYPE,
    SERVER_TABLE, SearchResult, SearchScope, SnowRecord, TaskSelector, is_terminal_state,
    resource_plan_record_from_row, sort_records_by_number, validate_list_input,
};

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

fn is_open_user_work_record(record: &SnowRecord) -> bool {
    !is_terminal_state(Some(record.state.as_str())) && !record_field_is_false(record, "active")
}

fn servicenow_record_is_open_user_work(record: &Record) -> bool {
    !is_terminal_state(record.get_display("state").or(record.get_str("state")))
        && !servicenow_record_field_is_false(record, "active")
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

#[derive(Clone)]
pub(crate) struct RecordService {
    ctx: CoreContext,
}

impl RecordService {
    pub(crate) fn new(ctx: CoreContext) -> Self {
        Self { ctx }
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

    pub fn tombstone_record(&self, sys_id: &str, when: DateTime<Utc>) -> Result<()> {
        self.ctx.tombstone_record(sys_id, when)
    }

    pub async fn prune_record(&self, sys_id: &str, when: DateTime<Utc>) -> Result<()> {
        self.ctx.prune_record(sys_id, when).await
    }

    pub fn degraded_reads(&self) -> Vec<DegradedReadDiagnostic> {
        self.ctx.query.degraded_reads()
    }

    pub async fn get_children(&self, number: &str) -> Result<Vec<SnowRecord>> {
        let mut cached = self.ctx.query.get_children(number).await?;
        if !cached.is_empty() {
            return Ok(cached);
        }

        let Some(parent_record) = self.ctx.client.get_by_number(number).await? else {
            return Ok(Vec::new());
        };
        self.ctx.persist_record(&parent_record)?;

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
            self.ctx.persist_record(child)?;
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
            if self.ctx.table_for_number(&normalized).is_some()
                && let Ok(Some(_)) = self.get_record_fresh(&normalized).await
            {
                return self.ctx.query.search_enriched(&normalized, scope).await;
            }
        }
        Ok(results)
    }

    pub async fn add_work_note(&self, number: &str, text: &str) -> Result<Option<SnowRecord>> {
        let Some((table, sys_id)) = self.ctx.lookup_table_and_sys_id(number).await? else {
            return Ok(None);
        };
        self.ctx.client.add_work_note(&table, &sys_id, text).await?;
        self.get_record_fresh(number).await
    }

    pub async fn set_state(&self, number: &str, state: &str) -> Result<Option<SnowRecord>> {
        let Some((table, sys_id)) = self.ctx.lookup_table_and_sys_id(number).await? else {
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
        let mut choices = self.ctx.field_choices_for_table(table, field).await?;
        if choices.is_empty() {
            for ancestor in self.ctx.table_ancestors(table).await? {
                choices = self.ctx.field_choices_for_table(&ancestor, field).await?;
                if !choices.is_empty() {
                    break;
                }
            }
        }
        if choices.is_empty() && field == "state" && table != "task" {
            choices = self.ctx.field_choices_for_table("task", field).await?;
        }
        Ok(choices)
    }

    pub async fn reassign(&self, number: &str, user: &str) -> Result<Option<SnowRecord>> {
        let Some((table, sys_id)) = self.ctx.lookup_table_and_sys_id(number).await? else {
            return Ok(None);
        };
        let assignee_sys_id = self.ctx.resolve_user_sys_id(user).await?;
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

    pub fn browser_url(&self, number: &str) -> String {
        format!(
            "{}/nav_to.do?uri={}.do?sysparm_query=number={}",
            self.ctx.client.base_url(),
            self.ctx.infer_table(number),
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
        self.ctx.persist_records(&records)?;
        Ok(HydratedRecords {
            sys_ids,
            active_scope_complete,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config;
    use crate::query::QueryEngine;
    use crate::tests::*;
    use crate::vault::VaultDocument;
    use crate::{
        FieldValue, MatchField, ResourcePlanStateFilter, SnowCore, WORK_RECORD_CACHE_TTL_MINUTES,
        collect_journal_entries, document_content, document_tag_tokens,
        record_row_from_runtime_record, record_row_from_servicenow, render_journal_entries,
        serialize_vault_document, work_record_ttl,
    };
    use chrono::TimeZone;
    use servicenow_rs::prelude::{BasicAuth, ServiceNowClient, parse_servicenow_timestamp};
    use tempfile::TempDir;
    use wiremock::matchers::{method, path, query_param, query_param_contains};
    use wiremock::{Mock, MockServer, ResponseTemplate};

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
            .ctx
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
            .ctx
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
