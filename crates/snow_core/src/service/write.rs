//! `WriteService` — mutating write paths: RM story / scrum-task, change request /
//! task, resource plan create/update; timecard read + hour edits; catalog search /
//! submit; and attachment listing / upload, extracted from the `SnowCore`
//! god-object.
//!
//! Domain service extracted in Task 11 of the library boundary migration. Every
//! method/helper/const/free-fn body below is moved verbatim from its former
//! `impl SnowCore` / free-fn location in `lib.rs`; the only edits are
//! `self.<helper>` → `self.ctx.<helper>` for helpers whose bodies live on
//! [`CoreContext`] (Task 6).

use anyhow::Result;
use chrono::{NaiveDate, Utc};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::path::Path;

use servicenow_rs::prelude::{
    AttachmentMetadata, DisplayValue, Error as SnowApiError, Operator, Order, Record,
};

use crate::context::CoreContext;
use crate::resource;
use crate::resource::catalog::{
    CatalogItemGetData, CatalogItemsSearchData, catalog_item_from_record,
};
use crate::{
    CatalogChoice, CatalogItem, CatalogSubmitResult, CatalogVariable, ChangeWriteConcurrency,
    ChangeWriteResult, Completeness, OperationEnvelope, PartialReason, ResourcePlanWriteResult,
    SetMode, SimpleRef, SnowRecord, StoryWriteResult, TimeCard, TimeValue, TimecardSheet, UserRef,
    WeekSelector, Weekday,
    cache::policy::{CacheMiss, CacheMode},
    canonical_record_table, normalize_record_lookup_sys_id, parse_servicenow_date, record_bool,
    record_field_display_or_raw, record_field_raw_or_display,
};

#[derive(Debug, thiserror::Error)]
pub enum IncidentWriteError {
    #[error(transparent)]
    Upstream(#[from] anyhow::Error),
    #[error("local Incident coherence failed after ServiceNow accepted the PATCH: {0}")]
    LocalCoherence(anyhow::Error),
}

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

fn time_sheet_row_contains_date(row: &Record, date: NaiveDate) -> bool {
    parse_servicenow_date(
        row.get_raw("week_starts_on")
            .or_else(|| row.get_display("week_starts_on"))
            .or_else(|| row.get_str("week_starts_on")),
    )
    .map(|start| date >= start && date < start + chrono::Duration::days(7))
    .unwrap_or(false)
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

#[derive(Clone)]
pub(crate) struct WriteService {
    ctx: CoreContext,
}

impl WriteService {
    pub(crate) fn new(ctx: CoreContext) -> Self {
        Self { ctx }
    }

    pub async fn list_my_timecards(&self, week: WeekSelector) -> Result<TimecardSheet> {
        let actor = self
            .ctx
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
        let actor_sys_id = self.ctx.current_user_sys_id().await?;
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
            .ctx
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

    /// Updates one Incident with no automatic PATCH retries and leaves the
    /// live-only Incident family with no local projection.
    pub async fn update_incident(
        &self,
        sys_id: &str,
        payload: serde_json::Value,
    ) -> std::result::Result<ChangeWriteResult, IncidentWriteError> {
        let write_client =
            self.ctx.write_client.as_ref().ok_or_else(|| {
                anyhow::anyhow!("governed Incident write client is not configured")
            })?;
        write_client
            .table("incident")
            .update(sys_id, payload)
            .await
            .map_err(anyhow::Error::from)?;
        let fresh_row = self
            .ctx
            .client
            .table("incident")
            .display_value(DisplayValue::Both)
            .get(sys_id)
            .await
            .map_err(anyhow::Error::from)
            .map_err(IncidentWriteError::LocalCoherence)?;
        let concurrency = ChangeWriteConcurrency::from_fresh_row(&fresh_row)
            .map_err(IncidentWriteError::LocalCoherence)?;
        let record = SnowRecord::from_servicenow(&fresh_row);
        self.ctx
            .prune_record(sys_id, Utc::now())
            .await
            .map_err(IncidentWriteError::LocalCoherence)?;
        Ok(ChangeWriteResult {
            record,
            concurrency,
        })
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
            .ctx
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
            .ctx
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

    pub async fn search_catalog_items(&self, query: &str, limit: u32) -> Result<Vec<CatalogItem>> {
        Ok(self
            .search_catalog_items_live(query, limit)
            .await?
            .into_iter()
            .map(|(_, item)| item)
            .collect())
    }

    pub async fn catalog_items_search_envelope(
        &self,
        query: &str,
        limit: u32,
    ) -> Result<OperationEnvelope<CatalogItemsSearchData>> {
        const OPERATION: &str = "catalog_items_search";
        const OBJECT: &str = "service_catalog_product";
        let rule = self
            .ctx
            .named_cache_policy
            .active()
            .rule_for(OPERATION, OBJECT);
        let limit = limit.clamp(1, 50);

        if rule.mode != CacheMode::Live {
            let cached = self
                .ctx
                .query
                .store()
                .search_narrowed_catalog_products(query, limit as usize)?;
            if !cached.is_empty() {
                let last_refreshed_at = cached
                    .iter()
                    .map(|row| row.last_refreshed_at)
                    .min()
                    .expect("non-empty catalog cache has a refresh timestamp");
                let fresh = rule.ttl.is_some_and(|ttl| {
                    cached
                        .iter()
                        .all(|row| row.last_refreshed_at + ttl > Utc::now())
                });
                if rule.mode == CacheMode::CacheOnly || fresh {
                    return Ok(OperationEnvelope::cached(
                        OPERATION,
                        last_refreshed_at,
                        Completeness::Partial {
                            reason: PartialReason::NarrowedProjection,
                        },
                        CatalogItemsSearchData {
                            items: cached.into_iter().map(|row| row.item).collect(),
                        },
                    ));
                }
            } else if rule.mode == CacheMode::CacheOnly {
                return Err(CacheMiss {
                    operation: OPERATION,
                    object: OBJECT,
                }
                .into());
            }
        }

        let live = self.search_catalog_items_live(query, limit).await?;
        let items = live
            .iter()
            .map(|(_, item)| item.clone())
            .collect::<Vec<_>>();
        if rule.mode == CacheMode::ReadThrough && !live.is_empty() {
            let refreshed_at = Utc::now();
            let records = live
                .iter()
                .map(|(record, _)| record.clone())
                .collect::<Vec<_>>();
            self.ctx.project_live_records_without_vault(&records)?;
            for item in &items {
                self.ctx
                    .query
                    .store()
                    .upsert_narrowed_catalog_product(item, refreshed_at)?;
            }
        }
        Ok(OperationEnvelope::live_partial(
            OPERATION,
            PartialReason::NarrowedProjection,
            CatalogItemsSearchData { items },
        ))
    }

    async fn search_catalog_items_live(
        &self,
        query: &str,
        limit: u32,
    ) -> Result<Vec<(Record, CatalogItem)>> {
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
            .map(|record| {
                let item = catalog_item_from_record(&record, Vec::new());
                (record, item)
            })
            .collect())
    }

    pub async fn get_catalog_item(&self, sys_id: &str) -> Result<CatalogItem> {
        Ok(self.get_catalog_item_live(sys_id).await?.1)
    }

    pub async fn catalog_item_get_envelope(
        &self,
        sys_id: &str,
    ) -> Result<OperationEnvelope<CatalogItemGetData>> {
        const OPERATION: &str = "catalog_item_get";
        const OBJECT: &str = "service_catalog_product";
        let sys_id = normalize_record_lookup_sys_id(sys_id)?;
        let rule = self
            .ctx
            .named_cache_policy
            .active()
            .rule_for(OPERATION, OBJECT);

        if rule.mode != CacheMode::Live {
            let cached = self
                .ctx
                .query
                .store()
                .get_complete_catalog_product(&sys_id)?;
            if let Some(cached) = cached {
                let fresh = rule
                    .ttl
                    .is_some_and(|ttl| cached.last_refreshed_at + ttl > Utc::now());
                if rule.mode == CacheMode::CacheOnly || fresh {
                    return Ok(OperationEnvelope::cached(
                        OPERATION,
                        cached.last_refreshed_at,
                        Completeness::Complete,
                        CatalogItemGetData { item: cached.item },
                    ));
                }
            } else if rule.mode == CacheMode::CacheOnly {
                return Err(CacheMiss {
                    operation: OPERATION,
                    object: OBJECT,
                }
                .into());
            }
        }

        let (record, item) = self.get_catalog_item_live(&sys_id).await?;
        if rule.mode == CacheMode::ReadThrough {
            let refreshed_at = Utc::now();
            self.ctx
                .project_live_records_without_vault(std::slice::from_ref(&record))?;
            self.ctx
                .query
                .store()
                .upsert_complete_catalog_product(&item, refreshed_at)?;
        }
        Ok(OperationEnvelope::live_complete(
            OPERATION,
            CatalogItemGetData { item },
        ))
    }

    async fn get_catalog_item_live(&self, sys_id: &str) -> Result<(Record, CatalogItem)> {
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
        let catalog_item = catalog_item_from_record(&item, variables);
        Ok((item, catalog_item))
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
                    .ctx
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
            if let Err(err) = self.ctx.enrich_record_journals(&mut ritm).await {
                eprintln!(
                    "snow_core: journal enrichment failed for catalog request item {ritm_sys_id}: {err}"
                );
            }
            if let Err(err) = self.ctx.persist_record(&ritm) {
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
        let Some((table, sys_id)) = self.ctx.lookup_table_and_sys_id(number).await? else {
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
        let Some((table, sys_id)) = self.ctx.lookup_table_and_sys_id(number).await? else {
            return Ok(None);
        };
        Ok(Some(
            self.ctx
                .client
                .upload_attachment_file(&table, &sys_id, path, file_name, content_type)
                .await?,
        ))
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ResourceType;
    use crate::tests::*;
    use wiremock::matchers::{body_partial_json, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

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
            .writes
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
        let source = include_str!("../lib.rs");
        let generic_create = ["pub async fn ", "create_record"].concat();
        let generic_update = ["pub async fn ", "update_record"].concat();

        assert!(!source.contains(&generic_create));
        assert!(!source.contains(&generic_update));
    }
}
