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
use chrono::{DateTime, Duration, NaiveDateTime, Utc};
use std::collections::{BTreeMap, HashMap, HashSet};

use servicenow_rs::prelude::{
    DisplayValue, Error as SnowApiError, Order, Record, child_relation_for_table,
};

use crate::context::CoreContext;
use crate::query;
use crate::query::filter::ListQuery;
use crate::resource;
use crate::{
    BUSINESS_APPLICATION_TABLE, CHANGE_REQUEST_QUERY_FIELDS, CHANGE_REQUEST_TASK_QUERY_FIELDS,
    ChangeRequestTaskListInput, DegradedReadDiagnostic, FieldChoice,
    INCIDENT_GROUP_LIST_DEFAULT_LIMIT, INCIDENT_GROUP_LIST_MAX_LIMIT, INCIDENT_QUERY_DEFAULT_LIMIT,
    INCIDENT_QUERY_FIELDS, INCIDENT_QUERY_MAX_LIMIT, INCIDENT_QUEUE_DEFAULT_SCAN_LIMIT,
    INCIDENT_QUEUE_MAX_KNOWN_SYS_IDS, INCIDENT_QUEUE_MAX_SCAN_LIMIT, IncidentAssignmentGroup,
    IncidentAssignmentGroupListInput, IncidentAssignmentGroupOperationsError,
    IncidentAssignmentGroupPage, IncidentAssignmentGroupQueueAggregates,
    IncidentAssignmentGroupQueueInput, IncidentAssignmentGroupQueueItem,
    IncidentAssignmentGroupQueuePage, IncidentGetData, IncidentGetInput, IncidentQueryData,
    IncidentQueryInput, IncidentQueueSlaRisk, IncidentQueueSortBy, IncidentQueueSortDirection,
    IncidentReadError, JournalEntry, MatchField, RecordLookup, RecordQueryError, RecordQueryInput,
    RecordQueryPage, RecordQuerySource, RecordRef, ResolvedResourceFilter, ResourcePlanListError,
    ResourcePlanListInput, ResourcePlanListResponse, ResourcePlanQuerySummary,
    ResourcePlanResourceType, ResourceType, SERVER_RESOURCE_TYPE, SERVER_TABLE,
    STORY_QUERY_DESCRIPTION_FIELDS, STORY_QUERY_FIELDS, SearchMatchReason, SearchResult,
    SearchScope, SnowRecord, TaskSelector, TaskSlaParentRef, TaskSlaReadability, TaskSlaStatus,
    TaskSlaSummaryView, ValidatedRecordQuery, is_terminal_state, resolve_incident_state,
    resolve_record_query_state, resource_plan_record_from_row, sort_records_by_number,
    validate_change_request_task_list, validate_incident_assignment_group_input,
    validate_list_input, validate_record_query,
};

const USER_RECORD_HYDRATE_LIMIT: u32 = 200;

/// Fields requested for the group-scoped Incident page.
///
/// Deliberately narrower than the fresh single-record path: this projection is
/// ephemeral and list-shaped, so journals are not requested. `active` and
/// `state` are required because both drive local rejection of terminal or
/// inactive rows.
const INCIDENT_GROUP_LIST_FIELDS: &[&str] = &[
    "sys_id",
    "number",
    "short_description",
    "state",
    "priority",
    "opened_at",
    "assigned_to",
    "assignment_group",
    "active",
    "sys_updated_on",
];

const INCIDENT_QUEUE_FIELDS: &[&str] = &[
    "sys_id",
    "number",
    "short_description",
    "description",
    "state",
    "priority",
    "impact",
    "urgency",
    "opened_at",
    "assigned_to",
    "assignment_group",
    "caller_id",
    "cmdb_ci",
    "business_service",
    "hold_reason",
    "active",
    "sys_updated_on",
    "sys_mod_count",
    "work_notes",
    "comments",
];

#[derive(Debug)]
struct ValidatedIncidentQuery {
    numbers: Vec<String>,
    assignment_group: Option<String>,
    assigned_to: Option<String>,
    caller_id: Option<String>,
    cmdb_ci: Option<String>,
    states: Vec<String>,
    priorities: Vec<String>,
    active: Option<bool>,
    opened_after: Option<String>,
    opened_before: Option<String>,
    updated_after: Option<String>,
    updated_before: Option<String>,
    limit: usize,
    cursor: Option<String>,
}

fn validate_incident_get_input(
    input: IncidentGetInput,
) -> std::result::Result<(Option<String>, Option<String>), IncidentReadError> {
    match (input.number, input.sys_id) {
        (Some(number), None) => {
            let number = number.trim().to_ascii_uppercase();
            if number.len() <= 3
                || !number.starts_with("INC")
                || !number[3..].bytes().all(|byte| byte.is_ascii_digit())
            {
                return Err(IncidentReadError::InvalidParams(
                    "number must match ^INC[0-9]+$".to_string(),
                ));
            }
            Ok((Some(number), None))
        }
        (None, Some(sys_id)) => normalize_record_lookup_sys_id(&sys_id)
            .map(|sys_id| (None, Some(sys_id)))
            .map_err(|error| IncidentReadError::InvalidParams(error.to_string())),
        _ => Err(IncidentReadError::InvalidParams(
            "exactly one of number or sys_id is required".to_string(),
        )),
    }
}

fn validate_incident_query_input(
    input: IncidentQueryInput,
) -> std::result::Result<ValidatedIncidentQuery, IncidentReadError> {
    let filters = input.filters;
    let numbers = match filters.numbers {
        Some(values) if values.is_empty() => {
            return Err(IncidentReadError::InvalidParams(
                "numbers must contain at least one value".to_string(),
            ));
        }
        Some(values) => normalize_incident_numbers(values)?,
        None => Vec::new(),
    };
    let states = match filters.states {
        Some(values) if values.is_empty() => {
            return Err(IncidentReadError::InvalidParams(
                "states must contain at least one value".to_string(),
            ));
        }
        Some(values) => normalize_unique_non_empty(values, "states", 20)?,
        None => Vec::new(),
    };
    let priorities = match filters.priorities {
        Some(values) if values.is_empty() => {
            return Err(IncidentReadError::InvalidParams(
                "priorities must contain at least one value".to_string(),
            ));
        }
        Some(values) => normalize_priorities(values)?,
        None => Vec::new(),
    };
    let assignment_group = normalize_optional_sys_id(filters.assignment_group, "assignment_group")?;
    let assigned_to = normalize_optional_sys_id(filters.assigned_to, "assigned_to")?;
    let caller_id = normalize_optional_sys_id(filters.caller_id, "caller_id")?;
    let cmdb_ci = normalize_optional_sys_id(filters.cmdb_ci, "cmdb_ci")?;
    let opened_after = validate_incident_timestamp(filters.opened_after, "opened_after")?;
    let opened_before = validate_incident_timestamp(filters.opened_before, "opened_before")?;
    let updated_after = validate_incident_timestamp(filters.updated_after, "updated_after")?;
    let updated_before = validate_incident_timestamp(filters.updated_before, "updated_before")?;
    validate_time_range(opened_after.as_deref(), opened_before.as_deref(), "opened")?;
    validate_time_range(
        updated_after.as_deref(),
        updated_before.as_deref(),
        "updated",
    )?;
    let limit = input.limit.unwrap_or(INCIDENT_QUERY_DEFAULT_LIMIT);
    if !(1..=INCIDENT_QUERY_MAX_LIMIT).contains(&limit) {
        return Err(IncidentReadError::InvalidParams(format!(
            "limit must be between 1 and {INCIDENT_QUERY_MAX_LIMIT}"
        )));
    }
    let cursor = input
        .cursor
        .map(|value| normalize_record_lookup_sys_id(&value))
        .transpose()
        .map_err(|error| IncidentReadError::InvalidParams(error.to_string()))?;
    Ok(ValidatedIncidentQuery {
        numbers,
        assignment_group,
        assigned_to,
        caller_id,
        cmdb_ci,
        states,
        priorities,
        active: filters.active,
        opened_after,
        opened_before,
        updated_after,
        updated_before,
        limit,
        cursor,
    })
}

fn normalize_incident_numbers(
    values: Vec<String>,
) -> std::result::Result<Vec<String>, IncidentReadError> {
    if values.len() > 20 {
        return Err(IncidentReadError::InvalidParams(
            "numbers accepts at most 20 values".to_string(),
        ));
    }
    let mut normalized = Vec::with_capacity(values.len());
    let mut seen = HashSet::with_capacity(values.len());
    for value in values {
        let (number, _) = validate_incident_get_input(IncidentGetInput {
            number: Some(value),
            sys_id: None,
        })?;
        let number = number.expect("number selector");
        if !seen.insert(number.clone()) {
            return Err(IncidentReadError::InvalidParams(
                "numbers must be unique after normalization".to_string(),
            ));
        }
        normalized.push(number);
    }
    Ok(normalized)
}

fn normalize_unique_non_empty(
    values: Vec<String>,
    field: &str,
    max: usize,
) -> std::result::Result<Vec<String>, IncidentReadError> {
    if values.len() > max {
        return Err(IncidentReadError::InvalidParams(format!(
            "{field} accepts at most {max} values"
        )));
    }
    let mut normalized = Vec::with_capacity(values.len());
    let mut seen = HashSet::with_capacity(values.len());
    for value in values {
        let value = value.trim().to_string();
        if value.is_empty() || !seen.insert(value.clone()) {
            return Err(IncidentReadError::InvalidParams(format!(
                "{field} values must be non-empty and unique"
            )));
        }
        normalized.push(value);
    }
    Ok(normalized)
}

fn normalize_priorities(values: Vec<u8>) -> std::result::Result<Vec<String>, IncidentReadError> {
    if values.len() > 5 {
        return Err(IncidentReadError::InvalidParams(
            "priorities accepts at most 5 values".to_string(),
        ));
    }
    let mut seen = HashSet::with_capacity(values.len());
    let mut normalized = Vec::with_capacity(values.len());
    for value in values {
        if !(1..=5).contains(&value) || !seen.insert(value) {
            return Err(IncidentReadError::InvalidParams(
                "priorities must contain unique integers from 1 through 5".to_string(),
            ));
        }
        normalized.push(value.to_string());
    }
    Ok(normalized)
}

fn normalize_optional_sys_id(
    value: Option<String>,
    field: &str,
) -> std::result::Result<Option<String>, IncidentReadError> {
    value
        .map(|value| normalize_record_lookup_sys_id(&value))
        .transpose()
        .map_err(|_| IncidentReadError::InvalidParams(format!("{field} must be a 32-hex sys_id")))
}

fn validate_incident_timestamp(
    value: Option<String>,
    field: &str,
) -> std::result::Result<Option<String>, IncidentReadError> {
    value
        .map(|value| {
            NaiveDateTime::parse_from_str(&value, "%Y-%m-%d %H:%M:%S")
                .map(|_| value)
                .map_err(|_| {
                    IncidentReadError::InvalidParams(format!(
                        "{field} must use YYYY-MM-DD HH:MM:SS"
                    ))
                })
        })
        .transpose()
}

fn validate_time_range(
    after: Option<&str>,
    before: Option<&str>,
    field: &str,
) -> std::result::Result<(), IncidentReadError> {
    if let (Some(after), Some(before)) = (after, before)
        && after >= before
    {
        return Err(IncidentReadError::InvalidParams(format!(
            "{field}_after must be earlier than {field}_before"
        )));
    }
    Ok(())
}

fn resolve_incident_states(
    requested: &[String],
    choices: &[FieldChoice],
) -> std::result::Result<Vec<String>, IncidentReadError> {
    let mut resolved = Vec::with_capacity(requested.len());
    let mut seen = HashSet::with_capacity(requested.len());
    for selector in requested {
        let raw_matches = choices
            .iter()
            .filter(|choice| choice.value == *selector)
            .collect::<Vec<_>>();
        let matches = if raw_matches.is_empty() {
            choices
                .iter()
                .filter(|choice| choice.label.eq_ignore_ascii_case(selector))
                .collect::<Vec<_>>()
        } else {
            raw_matches
        };
        if matches.len() != 1 {
            return Err(IncidentReadError::StateUnresolved {
                requested: selector.clone(),
                ambiguous: matches.len() > 1,
                unavailable: false,
                choices: choices.to_vec(),
            });
        }
        let value = matches[0].value.clone();
        if !seen.insert(value.clone()) {
            return Err(IncidentReadError::StateUnresolved {
                requested: selector.clone(),
                ambiguous: true,
                unavailable: false,
                choices: choices.to_vec(),
            });
        }
        resolved.push(value);
    }
    Ok(resolved)
}

fn classify_incident_api_error(error: &SnowApiError) -> IncidentReadError {
    match error {
        SnowApiError::Api {
            status: 401 | 403, ..
        }
        | SnowApiError::Auth {
            status: Some(401 | 403),
            ..
        } => IncidentReadError::AclDenied,
        SnowApiError::Http(_) => IncidentReadError::ServiceNowUnavailable,
        _ => IncidentReadError::ServiceNowError,
    }
}

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
    let normalized = table.trim().to_ascii_lowercase();
    RECORD_LOOKUP_ALLOWED_TABLES.contains(&normalized.as_str()) || normalized == "servers"
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
    // Private task (vtb_task) — table/sys_id lookup for get_record / get_work_notes.
    "vtb_task",
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

#[derive(Debug, Clone, PartialEq, Eq)]
struct ValidatedIncidentQueueInput {
    limit: usize,
    offset: usize,
    scan_limit: usize,
    known_sys_ids: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ResolvedIncidentAssignee {
    Unassigned,
    User(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum IncidentSlaBucket {
    Unavailable,
    Healthy,
    AtRisk,
    Breached,
}

impl IncidentSlaBucket {
    fn label(self) -> &'static str {
        match self {
            Self::Unavailable => "unavailable",
            Self::Healthy => "healthy",
            Self::AtRisk => "at_risk",
            Self::Breached => "breached",
        }
    }

    fn matches(self, filter: IncidentQueueSlaRisk) -> bool {
        matches!(filter, IncidentQueueSlaRisk::Any)
            || matches!(
                (self, filter),
                (Self::Unavailable, IncidentQueueSlaRisk::Unavailable)
                    | (Self::Healthy, IncidentQueueSlaRisk::Healthy)
                    | (Self::AtRisk, IncidentQueueSlaRisk::AtRisk)
                    | (Self::Breached, IncidentQueueSlaRisk::Breached)
            )
    }
}

fn incident_queue_invalid(message: impl Into<String>) -> anyhow::Error {
    IncidentAssignmentGroupOperationsError::InvalidParams(message.into()).into()
}

fn validate_incident_queue_input(
    input: &IncidentAssignmentGroupQueueInput,
) -> Result<ValidatedIncidentQueueInput> {
    if input.group.trim().is_empty() {
        return Err(incident_queue_invalid("group must not be empty"));
    }
    let limit = input.limit.unwrap_or(INCIDENT_GROUP_LIST_DEFAULT_LIMIT);
    if !(1..=INCIDENT_GROUP_LIST_MAX_LIMIT).contains(&limit) {
        return Err(incident_queue_invalid(format!(
            "limit must be between 1 and {INCIDENT_GROUP_LIST_MAX_LIMIT}"
        )));
    }
    let offset = input.offset.unwrap_or(0);
    let scan_limit = input
        .scan_limit
        .unwrap_or(INCIDENT_QUEUE_DEFAULT_SCAN_LIMIT);
    if !(1..=INCIDENT_QUEUE_MAX_SCAN_LIMIT).contains(&scan_limit) {
        return Err(incident_queue_invalid(format!(
            "scan_limit must be between 1 and {INCIDENT_QUEUE_MAX_SCAN_LIMIT}"
        )));
    }
    if offset >= scan_limit {
        return Err(incident_queue_invalid(
            "offset must be smaller than scan_limit",
        ));
    }
    if input.known_sys_ids.len() > INCIDENT_QUEUE_MAX_KNOWN_SYS_IDS {
        return Err(incident_queue_invalid(format!(
            "known_sys_ids must contain at most {INCIDENT_QUEUE_MAX_KNOWN_SYS_IDS} entries"
        )));
    }
    if !input.sla_at_risk_percentage.is_finite()
        || !(0.0..=100.0).contains(&input.sla_at_risk_percentage)
    {
        return Err(incident_queue_invalid(
            "sla_at_risk_percentage must be between 0 and 100",
        ));
    }
    if input
        .priorities
        .iter()
        .any(|priority| !(1..=5).contains(priority))
    {
        return Err(incident_queue_invalid(
            "priorities must contain only values from 1 through 5",
        ));
    }
    for (name, value) in [
        ("opened_after", input.opened_after.as_deref()),
        ("opened_before", input.opened_before.as_deref()),
        ("updated_since", input.updated_since.as_deref()),
        ("updated_before", input.updated_before.as_deref()),
        ("stale_before", input.stale_before.as_deref()),
    ] {
        if let Some(value) = value {
            chrono::NaiveDateTime::parse_from_str(value.trim(), "%Y-%m-%d %H:%M:%S").map_err(
                |_| incident_queue_invalid(format!("{name} must use YYYY-MM-DD HH:MM:SS")),
            )?;
        }
    }
    let mut known_sys_ids = Vec::with_capacity(input.known_sys_ids.len());
    let mut seen = HashSet::with_capacity(input.known_sys_ids.len());
    for sys_id in &input.known_sys_ids {
        let sys_id = normalize_record_lookup_sys_id(sys_id)?;
        if seen.insert(sys_id.clone()) {
            known_sys_ids.push(sys_id);
        }
    }
    Ok(ValidatedIncidentQueueInput {
        limit,
        offset,
        scan_limit,
        known_sys_ids,
    })
}

fn resolve_incident_queue_group(
    selector: &str,
    groups: &[IncidentAssignmentGroup],
) -> Result<IncidentAssignmentGroup> {
    let selector = selector.trim();
    let matches = groups
        .iter()
        .filter(|group| {
            group.sys_id.eq_ignore_ascii_case(selector) || group.name.eq_ignore_ascii_case(selector)
        })
        .cloned()
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [group] => Ok(group.clone()),
        [] => Err(IncidentAssignmentGroupOperationsError::GroupNotFound {
            requested: selector.to_string(),
            available: groups.to_vec(),
        }
        .into()),
        _ => Err(IncidentAssignmentGroupOperationsError::AmbiguousGroup {
            requested: selector.to_string(),
            matches,
        }
        .into()),
    }
}

fn unavailable_incident_sla(record: &SnowRecord) -> TaskSlaStatus {
    TaskSlaStatus {
        record_number: record.number.clone(),
        record_table: record.table.clone(),
        record_sys_id: record.sys_id.clone(),
        rows: Vec::new(),
        summary: TaskSlaSummaryView {
            total: 0,
            active: 0,
            breached: 0,
            next_breach: None,
            highest_business_elapsed: None,
        },
        readable: TaskSlaReadability::EmptyOrAclRestricted,
    }
}

fn incident_sla_bucket(status: &TaskSlaStatus, threshold: f64) -> IncidentSlaBucket {
    if status.readable != TaskSlaReadability::ReadableRows {
        IncidentSlaBucket::Unavailable
    } else if status.summary.breached > 0 {
        IncidentSlaBucket::Breached
    } else if status
        .summary
        .highest_business_elapsed
        .is_some_and(|percentage| percentage >= threshold)
    {
        IncidentSlaBucket::AtRisk
    } else {
        IncidentSlaBucket::Healthy
    }
}

fn latest_incident_activity(record: &SnowRecord) -> Option<JournalEntry> {
    record
        .work_notes
        .iter()
        .chain(record.comments.iter())
        .max_by_key(|entry| entry.timestamp)
        .cloned()
}

fn incident_field_value<'a>(record: &'a SnowRecord, field: &str) -> &'a str {
    record
        .fields
        .get(field)
        .map(|value| value.value.as_str())
        .unwrap_or_default()
}

fn incident_assignee_label(record: &SnowRecord) -> &str {
    record
        .references
        .get("assigned_to")
        .map(|reference| reference.display_name.as_str())
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("unassigned")
}

fn sort_incident_queue_items(
    items: &mut [IncidentAssignmentGroupQueueItem],
    sort_by: IncidentQueueSortBy,
    direction: IncidentQueueSortDirection,
    sla_threshold: f64,
) {
    items.sort_by(|left, right| {
        let ordering = match sort_by {
            IncidentQueueSortBy::Priority => incident_field_value(&left.record, "priority")
                .parse::<u8>()
                .unwrap_or(u8::MAX)
                .cmp(
                    &incident_field_value(&right.record, "priority")
                        .parse::<u8>()
                        .unwrap_or(u8::MAX),
                ),
            IncidentQueueSortBy::OpenedAt => incident_field_value(&left.record, "opened_at")
                .cmp(incident_field_value(&right.record, "opened_at")),
            IncidentQueueSortBy::UpdatedAt => incident_field_value(&left.record, "sys_updated_on")
                .cmp(incident_field_value(&right.record, "sys_updated_on")),
            IncidentQueueSortBy::Assignee => incident_assignee_label(&left.record)
                .to_ascii_lowercase()
                .cmp(&incident_assignee_label(&right.record).to_ascii_lowercase()),
            IncidentQueueSortBy::SlaRisk => incident_sla_bucket(&left.sla, sla_threshold)
                .cmp(&incident_sla_bucket(&right.sla, sla_threshold)),
        };
        let ordering = match direction {
            IncidentQueueSortDirection::Asc => ordering,
            IncidentQueueSortDirection::Desc => ordering.reverse(),
        };
        ordering.then_with(|| left.record.sys_id.cmp(&right.record.sys_id))
    });
}

fn incident_queue_aggregates(
    items: &[IncidentAssignmentGroupQueueItem],
    stale_before: &str,
    sla_threshold: f64,
    complete: bool,
) -> IncidentAssignmentGroupQueueAggregates {
    let mut aggregates = IncidentAssignmentGroupQueueAggregates {
        complete,
        ..Default::default()
    };
    for item in items {
        *aggregates
            .by_state
            .entry(item.record.state.clone())
            .or_default() += 1;
        *aggregates
            .by_priority
            .entry(incident_field_value(&item.record, "priority").to_string())
            .or_default() += 1;
        let assignee = incident_assignee_label(&item.record).to_string();
        *aggregates.by_assignee.entry(assignee).or_default() += 1;
        let sla = incident_sla_bucket(&item.sla, sla_threshold).label();
        *aggregates.by_sla_risk.entry(sla.to_string()).or_default() += 1;
        if incident_assignee_label(&item.record) == "unassigned" {
            aggregates.unassigned += 1;
        }
        if incident_field_value(&item.record, "sys_updated_on") < stale_before {
            aggregates.stale += 1;
        }
    }
    aggregates
}

#[derive(Clone)]
pub(crate) struct RecordService {
    ctx: CoreContext,
}

impl RecordService {
    pub(crate) fn new(ctx: CoreContext) -> Self {
        Self { ctx }
    }

    pub async fn invalidate_cache_target(&self, object: &str, sys_id: &str) -> Result<()> {
        let (resource_type, table) = cache_object_resource_type(object)?;
        let Some(row) = self.ctx.query.store().get_record_by_sys_id(sys_id)? else {
            return Ok(());
        };
        if row.resource_type != resource_type || table.is_some_and(|table| row.table_name != table)
        {
            anyhow::bail!("cached target does not belong to object `{object}`");
        }
        self.ctx.prune_record(sys_id, Utc::now()).await
    }

    pub async fn invalidate_cache_segment(&self, object: &str) -> Result<usize> {
        let (resource_type, table) = cache_object_resource_type(object)?;
        let records = self
            .ctx
            .query
            .list_records(
                ListQuery::new()
                    .resource_type(resource_type)
                    .include_tombstoned(true),
            )
            .await?;
        let records = records
            .into_iter()
            .filter(|record| table.is_none_or(|table| record.table == table))
            .collect::<Vec<_>>();
        let count = records.len();
        for record in records {
            self.ctx.prune_record(&record.sys_id, Utc::now()).await?;
        }
        Ok(count)
    }

    /// Look up a compatibility work record live without local cache I/O.
    pub async fn get_record(&self, number: &str) -> Result<Option<SnowRecord>> {
        let now = Utc::now();
        if let Some(record) = self.ctx.cache.get(number) {
            if now.signed_duration_since(record.synced_at)
                <= self.ctx.cache_policy.work_record_ttl()
            {
                return Ok(Some(record));
            }
            self.ctx.cache.invalidate(number);
            return self.get_record_fresh(number).await;
        }
        let record = self.ctx.query.get_record(number).await?;
        if let Some(ref record) = record {
            if now.signed_duration_since(record.synced_at) > self.ctx.cache_policy.work_record_ttl()
            {
                return self.get_record_fresh(number).await;
            }
            self.ctx.cache.put(record.clone());
        }
        Ok(record)
    }

    /// Fetch a record from the live ServiceNow API with raw and display values
    /// without persisting it into cache, vault, or search index.
    ///
    /// Journal enrichment is best-effort; the base live result is still
    /// returned when journals are unavailable.
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

    /// Executes one bounded, deterministic, live page for the two explicitly
    /// supported Mullet record kinds. The returned rows are ephemeral: this
    /// method never writes the cache, vault, or search index.
    pub async fn record_query(&self, input: RecordQueryInput) -> Result<RecordQueryPage> {
        let validated = validate_record_query(input)?;
        let (table, limit, cursor, mut query) = match validated {
            ValidatedRecordQuery::ChangeRequest {
                filters,
                limit,
                cursor,
            } => {
                let resolved_state = match filters.state.as_deref() {
                    Some(selector) => {
                        let choices = self.field_choices("change_request", "state").await?;
                        Some(resolve_record_query_state(
                            selector,
                            "change_request",
                            &choices,
                        )?)
                    }
                    None => None,
                };
                let mut query = self
                    .ctx
                    .client
                    .table("change_request")
                    .fields(CHANGE_REQUEST_QUERY_FIELDS)
                    .display_value(DisplayValue::Both)
                    .exclude_reference_link(true)
                    .limit(limit as u32);
                if let Some(value) = filters.assignment_group.as_deref() {
                    query = query.equals("assignment_group", value);
                }
                if let Some(value) = filters.assigned_to.as_deref() {
                    query = query.equals("assigned_to", value);
                }
                if let Some(value) = resolved_state.as_deref() {
                    query = query.equals("state", value);
                }
                if let Some(value) = filters.start_date_after.as_deref() {
                    query = query.greater_than("start_date", value);
                }
                if let Some(value) = filters.start_date_before.as_deref() {
                    query = query.less_than("start_date", value);
                }
                ("change_request", limit, cursor, query)
            }
            ValidatedRecordQuery::Story {
                filters,
                include_description,
                limit,
                cursor,
            } => {
                let filters = *filters;
                let resolved_states = match filters.states.as_deref() {
                    Some(selectors) => {
                        let choices = self.field_choices("rm_story", "state").await?;
                        let mut seen = HashSet::new();
                        let mut resolved = Vec::with_capacity(selectors.len());
                        for selector in selectors {
                            let value = resolve_record_query_state(selector, "rm_story", &choices)?;
                            if !seen.insert(value.clone()) {
                                return Err(RecordQueryError::UnresolvedState {
                                    requested: selector.clone(),
                                    table: "rm_story".to_string(),
                                    field: "state".to_string(),
                                    ambiguous: true,
                                    choices,
                                }
                                .into());
                            }
                            resolved.push(value);
                        }
                        Some(resolved)
                    }
                    None => None,
                };
                let mut fields = STORY_QUERY_FIELDS.to_vec();
                if include_description {
                    fields.extend_from_slice(STORY_QUERY_DESCRIPTION_FIELDS);
                }
                let mut query = self
                    .ctx
                    .client
                    .table("rm_story")
                    .fields(&fields)
                    .display_value(DisplayValue::Both)
                    .exclude_reference_link(true)
                    .limit(limit as u32);
                if let Some(value) = filters.assignment_group.as_deref() {
                    query = query.equals("assignment_group", value);
                }
                if let Some(value) = filters.assigned_to.as_deref() {
                    query = query.equals("assigned_to", value);
                }
                if let Some(value) = filters.story_owner.as_deref() {
                    query = query.equals("u_story_owner", value);
                }
                if let Some(value) = filters.lead_developer.as_deref() {
                    query = query.equals("u_lead_dev", value);
                }
                if let Some(values) = resolved_states.as_deref() {
                    if let [value] = values {
                        query = query.equals("state", value);
                    } else {
                        let values = values.iter().map(String::as_str).collect::<Vec<_>>();
                        query = query.in_list("state", &values);
                    }
                }
                if let Some(value) = filters.sprint.as_deref() {
                    query = query.equals("sprint", value);
                }
                if let Some(value) = filters.project.as_deref() {
                    query = query.equals("project", value);
                }
                if let Some(value) = filters.cmdb_ci.as_deref() {
                    query = query.equals("cmdb_ci", value);
                }
                if let Some(value) = filters.blocked {
                    query = query.equals("blocked", if value { "true" } else { "false" });
                }
                if let Some(value) = filters.due_date_after.as_deref() {
                    query = query.greater_than("due_date", value);
                }
                if let Some(value) = filters.due_date_before.as_deref() {
                    query = query.less_than("due_date", value);
                }
                if let Some(value) = filters.updated_after.as_deref() {
                    query = query.greater_than("sys_updated_on", value);
                }
                if let Some(values) = filters.numbers.as_deref() {
                    if let [value] = values {
                        query = query.equals("number", value);
                    } else {
                        let values = values.iter().map(String::as_str).collect::<Vec<_>>();
                        query = query.in_list("number", &values);
                    }
                }
                if let Some(value) = filters.text.as_deref() {
                    query = query.contains("short_description", value);
                }
                ("rm_story", limit, cursor, query)
            }
        };

        if let Some(cursor) = cursor.as_deref() {
            query = query.greater_than("sys_id", cursor);
        }
        query = query.order_by("sys_id", Order::Asc);
        let rows = query.execute().await?.records;
        let rows_inspected = rows.len();
        let complete = rows_inspected < limit;
        let next_cursor = if complete {
            None
        } else {
            rows.last().map(|row| row.sys_id.clone())
        };
        let records = rows
            .iter()
            .map(SnowRecord::from_servicenow)
            .collect::<Vec<_>>();
        debug_assert!(records.iter().all(|record| record.table == table));

        Ok(RecordQueryPage {
            records,
            next_cursor,
            complete,
            source: RecordQuerySource::Live,
            limit,
            rows_inspected,
        })
    }

    /// Returns one bounded, live page of CTASKs for exactly one Change Request.
    /// The result is ephemeral and never falls back to a cache or vault.
    pub async fn change_request_list_tasks(
        &self,
        input: ChangeRequestTaskListInput,
    ) -> Result<RecordQueryPage> {
        let validated = validate_change_request_task_list(input)?;
        let mut query = self
            .ctx
            .client
            .table("change_task")
            .fields(CHANGE_REQUEST_TASK_QUERY_FIELDS)
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .order_by("sys_id", Order::Asc)
            .limit(validated.limit as u32)
            .equals("change_request.number", &validated.change_request_number);
        if let Some(cursor) = validated.cursor.as_deref() {
            query = query.greater_than("sys_id", cursor);
        }
        let rows = query.execute().await?.records;
        let rows_inspected = rows.len();
        let complete = rows_inspected < validated.limit;
        let next_cursor = if complete {
            None
        } else {
            rows.last().map(|row| row.sys_id.clone())
        };
        let records = rows.iter().map(SnowRecord::from_servicenow).collect();
        Ok(RecordQueryPage {
            records,
            next_cursor,
            complete,
            source: RecordQuerySource::Live,
            limit: validated.limit,
            rows_inspected,
        })
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

    /// Lists one page of *active* Incidents assigned to `assignment_group`,
    /// optionally narrowed to one exact Incident state.
    ///
    /// Contract highlights, all from
    /// `docs/spec-incident-list-by-assignment-group.md`:
    ///
    /// - **Ephemeral.** Unlike [`Self::my_incidents_fresh`], nothing returned
    ///   here is persisted, cached, vaulted, or indexed. Group scope has no
    ///   tombstoning story, so nothing local is allowed to grow from it.
    /// - **`limit` counts requested rows.** Exactly one Table API request is
    ///   issued per page. `records` may be shorter than `rows_inspected`
    ///   because rows that are terminal or `active=false` are rejected
    ///   locally, mirroring the existing fresh-Incident semantics.
    /// - **Cursor anchors to the last ServiceNow row**, not the last surviving
    ///   record, so a page whose rows are entirely rejected locally still
    ///   advances instead of stalling the scan.
    /// - **Authorization is ServiceNow's.** This applies no scope narrowing of
    ///   its own; the runtime credential's ACLs decide what is visible.
    pub async fn incident_list_by_assignment_group(
        &self,
        input: IncidentAssignmentGroupListInput,
    ) -> Result<IncidentAssignmentGroupPage> {
        let validated = validate_incident_assignment_group_input(input)?;

        // Resolved before the Incident query so an unusable selector costs one
        // choice read instead of returning a wrongly-filtered page.
        let resolved_state = match validated.state_selector.as_deref() {
            Some(selector) => {
                let choices = self.field_choices("incident", "state").await?;
                Some(resolve_incident_state(selector, &choices)?)
            }
            None => None,
        };

        let mut query = self
            .ctx
            .client
            .table("incident")
            .fields(INCIDENT_GROUP_LIST_FIELDS)
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .order_by("sys_id", Order::Asc)
            .limit(validated.effective_limit as u32)
            .equals("assignment_group", &validated.assignment_group_sys_id)
            .equals("active", "true");

        if let Some(state) = resolved_state.as_ref() {
            query = query.equals("state", &state.value);
        }
        // Exclusive cursor: the previous page's last inspected row is not
        // returned twice.
        if let Some(cursor) = validated.cursor.as_deref() {
            query = query.greater_than("sys_id", cursor);
        }

        let rows = query.execute().await?.records;
        let rows_inspected = rows.len();
        let complete = rows_inspected < validated.effective_limit;
        let next_cursor = if complete {
            None
        } else {
            rows.last().map(|row| row.sys_id.clone())
        };

        let records = rows
            .iter()
            .filter(|row| servicenow_record_is_open_user_work(row))
            .map(resource::incident::IncidentResource::from_servicenow)
            .collect::<Vec<_>>();

        Ok(IncidentAssignmentGroupPage {
            records,
            next_cursor,
            complete,
            limit: validated.effective_limit,
            rows_inspected,
            state: resolved_state,
        })
    }

    /// Fetch one Incident live without consulting or updating local state.
    pub async fn incident_get(
        &self,
        input: IncidentGetInput,
    ) -> std::result::Result<crate::OperationEnvelope<IncidentGetData>, IncidentReadError> {
        let (number, sys_id) = validate_incident_get_input(input)?;
        let row = if let Some(sys_id) = sys_id {
            match self
                .ctx
                .client
                .table("incident")
                .display_value(DisplayValue::Both)
                .exclude_reference_link(true)
                .get(&sys_id)
                .await
            {
                Ok(row) => row,
                Err(SnowApiError::Api { status: 404, .. }) => {
                    return Err(IncidentReadError::NotFound);
                }
                Err(error) => return Err(classify_incident_api_error(&error)),
            }
        } else {
            let number = number.expect("validated selector");
            let result = self
                .ctx
                .client
                .table("incident")
                .display_value(DisplayValue::Both)
                .exclude_reference_link(true)
                .equals("number", &number)
                .limit(2)
                .execute()
                .await
                .map_err(|error| classify_incident_api_error(&error))?;
            match result.records.as_slice() {
                [] => return Err(IncidentReadError::LookupUnavailable),
                [row] => row.clone(),
                _ => return Err(IncidentReadError::NumberAmbiguous),
            }
        };

        Ok(crate::OperationEnvelope::live_complete(
            "incident_get",
            IncidentGetData {
                record: resource::incident::IncidentResource::native_fields(&row),
            },
        ))
    }

    /// Query one deterministic page of ACL-visible Incidents, live-only.
    pub async fn incident_query(
        &self,
        input: IncidentQueryInput,
    ) -> std::result::Result<crate::OperationEnvelope<IncidentQueryData>, IncidentReadError> {
        let validated = validate_incident_query_input(input)?;
        let resolved_states = if validated.states.is_empty() {
            Vec::new()
        } else {
            let choices = self.field_choices("incident", "state").await.map_err(|_| {
                IncidentReadError::StateUnresolved {
                    requested: validated.states[0].clone(),
                    ambiguous: false,
                    unavailable: true,
                    choices: Vec::new(),
                }
            })?;
            if choices.is_empty() {
                return Err(IncidentReadError::StateUnresolved {
                    requested: validated.states[0].clone(),
                    ambiguous: false,
                    unavailable: true,
                    choices,
                });
            }
            resolve_incident_states(&validated.states, &choices)?
        };

        let mut query = self
            .ctx
            .client
            .table("incident")
            .fields(INCIDENT_QUERY_FIELDS)
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .order_by("sys_id", Order::Asc)
            .limit(validated.limit as u32);

        if !validated.numbers.is_empty() {
            let values = validated
                .numbers
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            query = query.in_list("number", &values);
        }
        for (field, value) in [
            ("assignment_group", validated.assignment_group.as_deref()),
            ("assigned_to", validated.assigned_to.as_deref()),
            ("caller_id", validated.caller_id.as_deref()),
            ("cmdb_ci", validated.cmdb_ci.as_deref()),
        ] {
            if let Some(value) = value {
                query = query.equals(field, value);
            }
        }
        if !resolved_states.is_empty() {
            let values = resolved_states
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            query = query.in_list("state", &values);
        }
        if !validated.priorities.is_empty() {
            let values = validated
                .priorities
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            query = query.in_list("priority", &values);
        }
        if let Some(active) = validated.active {
            query = query.equals("active", if active { "true" } else { "false" });
        }
        for (field, value) in [
            ("opened_at", validated.opened_after.as_deref()),
            ("sys_updated_on", validated.updated_after.as_deref()),
        ] {
            if let Some(value) = value {
                query = query.greater_than(field, value);
            }
        }
        for (field, value) in [
            ("opened_at", validated.opened_before.as_deref()),
            ("sys_updated_on", validated.updated_before.as_deref()),
        ] {
            if let Some(value) = value {
                query = query.less_than(field, value);
            }
        }
        if let Some(cursor) = validated.cursor.as_deref() {
            query = query.greater_than("sys_id", cursor);
        }

        let rows = query
            .execute()
            .await
            .map_err(|error| classify_incident_api_error(&error))?
            .records;
        let rows_inspected = rows.len();
        let complete = rows_inspected < validated.limit;
        let next_cursor = if complete {
            None
        } else {
            rows.last().map(|row| row.sys_id.clone())
        };
        let data = IncidentQueryData {
            records: rows
                .iter()
                .map(resource::incident::IncidentResource::native_fields)
                .collect(),
            next_cursor,
            limit: validated.limit,
            rows_inspected,
        };
        Ok(if complete {
            crate::OperationEnvelope::live_complete("incident_query", data)
        } else {
            crate::OperationEnvelope::live_partial(
                "incident_query",
                crate::PartialReason::PageLimitReached,
                data,
            )
        })
    }

    /// Lists the authenticated user's active direct assignment-group memberships.
    pub async fn incident_assignment_groups(&self) -> Result<Vec<IncidentAssignmentGroup>> {
        const PAGE_SIZE: usize = 500;
        const MAX_PAGES: usize = 20;

        let user_sys_id = self.current_user_sys_id().await?;
        let mut paginator = self
            .ctx
            .client
            .table("sys_user_grmember")
            .equals("user", &user_sys_id)
            .fields(&["sys_id", "group"])
            .dot_walk(&["group.name", "group.active"])
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .no_count()
            .limit(500)
            .paginate()?;
        let mut groups = BTreeMap::new();
        let mut pages = 0usize;
        loop {
            if pages >= MAX_PAGES {
                if !paginator.is_done() {
                    anyhow::bail!(
                        "assignment-group membership lookup exceeded {} rows",
                        PAGE_SIZE * MAX_PAGES
                    );
                }
                break;
            }
            let Some(page) = paginator.next_page().await? else {
                break;
            };
            pages += 1;
            for row in page.records {
                if row
                    .get_str("group.active")
                    .is_some_and(|active| matches!(active.trim(), "false" | "0"))
                {
                    continue;
                }
                let Some(sys_id) = row
                    .get_raw("group")
                    .or_else(|| row.get_str("group"))
                    .and_then(|value| normalize_record_lookup_sys_id(value).ok())
                else {
                    continue;
                };
                let name = row
                    .get_str("group.name")
                    .or_else(|| row.get_display("group"))
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .unwrap_or(&sys_id)
                    .to_string();
                groups
                    .entry(sys_id.clone())
                    .or_insert(IncidentAssignmentGroup { sys_id, name });
            }
        }

        let mut groups = groups.into_values().collect::<Vec<_>>();
        groups.sort_by(|left, right| {
            left.name
                .to_ascii_lowercase()
                .cmp(&right.name.to_ascii_lowercase())
                .then_with(|| left.sys_id.cmp(&right.sys_id))
        });
        Ok(groups)
    }

    /// Returns a bounded operational queue for one authenticated-user group.
    pub async fn incident_assignment_group_queue(
        &self,
        input: IncidentAssignmentGroupQueueInput,
    ) -> Result<IncidentAssignmentGroupQueuePage> {
        let validated = validate_incident_queue_input(&input)?;
        let watermark_at = Utc::now();
        let watermark = watermark_at.format("%Y-%m-%d %H:%M:%S").to_string();
        let groups = self.incident_assignment_groups().await?;
        let group = resolve_incident_queue_group(&input.group, &groups)?;
        let resolved_state = match input.state.as_deref() {
            Some(selector) => {
                let choices = self.field_choices("incident", "state").await?;
                Some(resolve_incident_state(selector, &choices)?)
            }
            None => None,
        };
        let assigned_to = match input.assigned_to.as_deref().map(str::trim) {
            None | Some("") => None,
            Some(selector) if selector.eq_ignore_ascii_case("unassigned") => {
                Some(ResolvedIncidentAssignee::Unassigned)
            }
            Some(selector) if selector.eq_ignore_ascii_case("me") => Some(
                ResolvedIncidentAssignee::User(self.current_user_sys_id().await?),
            ),
            Some(selector) => {
                let sys_id = match normalize_record_lookup_sys_id(selector) {
                    Ok(sys_id) => sys_id,
                    Err(_) => self.ctx.resolve_user_sys_id(selector).await?,
                };
                Some(ResolvedIncidentAssignee::User(sys_id))
            }
        };

        let mut query = self
            .ctx
            .client
            .table("incident")
            .fields(INCIDENT_QUEUE_FIELDS)
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .equals("assignment_group", &group.sys_id)
            .equals("active", "true")
            .order_by("sys_id", Order::Asc)
            .no_count()
            .limit(200);
        if let Some(state) = resolved_state.as_ref() {
            query = query.equals("state", &state.value);
        }
        if let Some(assignee) = assigned_to.as_ref() {
            query = match assignee {
                ResolvedIncidentAssignee::Unassigned => query.is_empty_field("assigned_to"),
                ResolvedIncidentAssignee::User(sys_id) => query.equals("assigned_to", sys_id),
            };
        }
        let priority_values = input
            .priorities
            .iter()
            .map(u8::to_string)
            .collect::<Vec<_>>();
        let priority_refs = priority_values
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        if !priority_refs.is_empty() {
            query = query.in_list("priority", &priority_refs);
        }
        if let Some(value) = input.opened_after.as_deref() {
            query = query.greater_than("opened_at", value);
        }
        if let Some(value) = input.opened_before.as_deref() {
            query = query.less_than("opened_at", value);
        }
        if let Some(value) = input.updated_since.as_deref() {
            query = query.greater_than("sys_updated_on", value);
        }
        if let Some(value) = input.updated_before.as_deref() {
            query = query.less_than("sys_updated_on", value);
        }
        let stale_before = input.stale_before.clone().unwrap_or_else(|| {
            (watermark_at - Duration::hours(24))
                .format("%Y-%m-%d %H:%M:%S")
                .to_string()
        });
        if input.stale_only {
            query = query.less_than("sys_updated_on", &stale_before);
        }

        let mut paginator = query.paginate()?;
        let mut rows = Vec::with_capacity(validated.scan_limit.min(500));
        let mut scan_complete = false;
        while rows.len() < validated.scan_limit {
            let Some(page) = paginator.next_page().await? else {
                scan_complete = true;
                break;
            };
            let remaining = validated.scan_limit - rows.len();
            let page_len = page.records.len();
            rows.extend(page.records.into_iter().take(remaining));
            if page_len > remaining {
                break;
            }
            if paginator.is_done() {
                scan_complete = true;
                break;
            }
        }

        let rows_scanned = rows.len();
        let records = rows
            .iter()
            .filter(|row| servicenow_record_is_open_user_work(row))
            .map(resource::incident::IncidentResource::from_servicenow)
            .collect::<Vec<_>>();
        let parents = records
            .iter()
            .map(|record| TaskSlaParentRef {
                record_number: record.number.clone(),
                record_table: "incident".to_string(),
                record_sys_id: record.sys_id.clone(),
            })
            .collect::<Vec<_>>();
        let mut statuses = crate::sla::task_sla_statuses_for_parents(&self.ctx, &parents).await?;
        let mut items = records
            .into_iter()
            .map(|record| {
                let latest_activity = latest_incident_activity(&record);
                let sla = statuses
                    .remove(&record.sys_id)
                    .unwrap_or_else(|| unavailable_incident_sla(&record));
                IncidentAssignmentGroupQueueItem {
                    record,
                    sla,
                    latest_activity,
                }
            })
            .filter(|item| {
                incident_sla_bucket(&item.sla, input.sla_at_risk_percentage).matches(input.sla_risk)
            })
            .collect::<Vec<_>>();

        sort_incident_queue_items(
            &mut items,
            input.sort_by,
            input.sort_direction,
            input.sla_at_risk_percentage,
        );
        let aggregates = incident_queue_aggregates(
            &items,
            &stale_before,
            input.sla_at_risk_percentage,
            scan_complete,
        );
        let filtered_len = items.len();
        let page_items = items
            .into_iter()
            .skip(validated.offset)
            .take(validated.limit)
            .collect::<Vec<_>>();
        let consumed = validated.offset.saturating_add(page_items.len());
        let next_offset = (consumed < filtered_len).then_some(consumed);
        let complete = scan_complete && next_offset.is_none();
        let departed_sys_ids = self
            .incident_departures(&group.sys_id, &validated.known_sys_ids)
            .await?;

        Ok(IncidentAssignmentGroupQueuePage {
            group,
            items: page_items,
            offset: validated.offset,
            next_offset,
            complete,
            scan_complete,
            rows_scanned,
            watermark,
            departed_sys_ids,
            aggregates,
        })
    }

    async fn incident_departures(
        &self,
        group_sys_id: &str,
        known_sys_ids: &[String],
    ) -> Result<Vec<String>> {
        if known_sys_ids.is_empty() {
            return Ok(Vec::new());
        }
        let mut returned = HashMap::with_capacity(known_sys_ids.len());
        for chunk in known_sys_ids.chunks(100) {
            let refs = chunk.iter().map(String::as_str).collect::<Vec<_>>();
            let result = self
                .ctx
                .client
                .table("incident")
                .in_list("sys_id", &refs)
                .fields(&["sys_id", "state", "active", "assignment_group"])
                .display_value(DisplayValue::Both)
                .exclude_reference_link(true)
                .limit(u32::try_from(chunk.len())?)
                .execute()
                .await?;
            for row in result.records {
                returned.insert(row.sys_id.clone(), row);
            }
        }

        let mut departed = known_sys_ids
            .iter()
            .filter(|sys_id| {
                returned.get(*sys_id).is_none_or(|row| {
                    row.get_raw("assignment_group")
                        .or_else(|| row.get_str("assignment_group"))
                        != Some(group_sys_id)
                        || !servicenow_record_is_open_user_work(row)
                })
            })
            .cloned()
            .collect::<Vec<_>>();
        departed.sort();
        Ok(departed)
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

    /// Full-text search across cached records, with live-only exact work-record
    /// lookup before any local projection access.
    ///
    /// If the query matches an exact ServiceNow record number pattern (e.g.
    /// `INC4992697`, `chg0325640`), the compatibility operation is omitted from
    /// cache policy and therefore reads ServiceNow without a cache lookup or
    /// persistence. Other queries retain the local full-text behavior.
    ///
    /// Free-text queries never trigger the live-fetch fallback.
    pub async fn search_enriched(
        &self,
        query: &str,
        scope: SearchScope,
    ) -> Result<Vec<SearchResult>> {
        // Exact work-record numbers are policy omissions and therefore live
        // only. Resolve them before any local search so a seeded or stale
        // projection cannot intercept the request.
        if query::is_exact_record_number(query) {
            let normalized = query.trim().to_uppercase();
            // Gate on table_for_number: we can only fetch if the prefix maps
            // to a known table (INC→incident, CHG→change_request, etc.)
            if self.ctx.table_for_number(&normalized).is_some()
                && let Ok(Some(record)) = self
                    .ctx
                    .get_record_live_without_persistence(&normalized)
                    .await
            {
                return Ok(vec![SearchResult {
                    record: RecordRef {
                        sys_id: record.sys_id,
                        number: record.number.clone(),
                        table: record.table,
                    },
                    snippet: record.short_description,
                    score: 30,
                    match_in: MatchField::Number,
                    matched_value: Some(record.number.clone()),
                    reasons: vec![SearchMatchReason {
                        field: MatchField::Number,
                        value: record.number,
                    }],
                }]);
            }
            return Ok(Vec::new());
        }
        self.ctx.query.search_enriched(query, scope).await
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
        if choices.is_empty()
            && let Some(ui_metadata) = self.ctx.ui_metadata.as_ref()
            && let Ok(metadata_choices) = ui_metadata.field_choices(table, field).await
        {
            choices = metadata_choices;
        }
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

fn cache_object_resource_type(object: &str) -> Result<(ResourceType, Option<&'static str>)> {
    match object {
        "knowledge" => Ok((ResourceType::Knowledge, None)),
        "business_application" => Ok((ResourceType::BusinessApplication, None)),
        "server" => Ok((ResourceType::Server, None)),
        "service_catalog_product" => Ok((ResourceType::Unknown, Some("sc_cat_item"))),
        _ => anyhow::bail!("unknown cache object `{object}`"),
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
        CHANGE_REQUEST_QUERY_FIELDS, ChangeRequestQueryFilters, FieldValue, MatchField,
        RecordQueryInput, RecordQuerySource, ResourcePlanStateFilter,
        STORY_QUERY_DESCRIPTION_FIELDS, STORY_QUERY_FIELDS, SnowCore, StoryQueryFilters,
        WORK_RECORD_CACHE_TTL_MINUTES, collect_journal_entries, document_content,
        document_tag_tokens, record_row_from_runtime_record, record_row_from_servicenow,
        render_journal_entries, serialize_vault_document,
    };
    use crate::{
        INCIDENT_GROUP_LIST_DEFAULT_LIMIT, INCIDENT_GROUP_LIST_MAX_LIMIT,
        IncidentAssignmentGroupListError, ResolvedIncidentState,
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
    fn private_task_table_is_allowed_by_the_runtime_gate_and_public_schema() {
        assert!(is_record_lookup_table_allowed("vtb_task"));
        assert_eq!(
            normalize_record_lookup_table("VTB_TASK").unwrap(),
            "vtb_task"
        );
        assert!(RECORD_LOOKUP_ALLOWED_TABLES.contains(&"vtb_task"));
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
    async fn public_record_read_ignores_stale_projection_and_does_not_persist_live_result() {
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
            .get_record_live_without_persistence("INC002")
            .await
            .expect("record lookup")
            .expect("record");

        assert_eq!(record.short_description, "Live incident title");
        assert_eq!(record.description, "Live incident body");
        let persisted = core
            .ctx
            .query
            .store()
            .get_record_by_number("INC002")
            .expect("persisted row")
            .expect("persisted row");
        assert_eq!(persisted.short_desc.as_deref(), Some("Stale cached title"));

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
            .ctx
            .query
            .store()
            .get_record_by_number("DMND0320098")
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
    async fn public_invalidation_removes_exact_target_then_only_the_named_segment() {
        let server = MockServer::start().await;
        let (core, _tempdir) = core_for_mock_server(&server).await;
        let mut first = sample_projected_record();
        first.sys_id = "0000000000000000000000000000a001".to_string();
        first.number = "SRV0001".to_string();
        first.table = "cmdb_ci_server".to_string();
        first.resource_type = ResourceType::Server;
        let mut second = first.clone();
        second.sys_id = "0000000000000000000000000000a002".to_string();
        second.number = "SRV0002".to_string();
        let mut incident = sample_projected_record();
        incident.sys_id = "0000000000000000000000000000b001".to_string();
        incident.number = "INC9000001".to_string();
        core.ctx
            .persist_snow_records(&[first.clone(), second.clone(), incident.clone()])
            .expect("seed projections");

        let wrong_segment = core
            .invalidate_cache_target("server", &incident.sys_id)
            .await
            .expect_err("exact invalidation must reject a different object segment");
        assert!(wrong_segment.to_string().contains("does not belong"));

        core.invalidate_cache_target("server", &first.sys_id)
            .await
            .expect("exact invalidation");
        assert!(
            core.ctx
                .query
                .store()
                .get_record_by_sys_id(&first.sys_id)
                .expect("first lookup")
                .is_none()
        );
        assert!(
            core.ctx
                .query
                .store()
                .get_record_by_sys_id(&second.sys_id)
                .expect("second lookup")
                .is_some()
        );

        assert_eq!(
            core.invalidate_cache_segment("server")
                .await
                .expect("segment invalidation"),
            1
        );
        assert!(
            core.ctx
                .query
                .store()
                .get_record_by_sys_id(&second.sys_id)
                .expect("second lookup")
                .is_none()
        );
        assert!(
            core.ctx
                .query
                .store()
                .get_record_by_sys_id(&incident.sys_id)
                .expect("incident lookup")
                .is_some()
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
    async fn field_choices_falls_back_to_authenticated_ui_metadata_when_sys_choice_is_hidden() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_choice"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/ui/meta/change_request"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": {
                    "columns": {
                        "state": {
                            "choices": [
                                { "value": "1", "label": "New" },
                                { "value": "2", "label": "In Progress" },
                                { "value": "2", "label": "Duplicate" },
                                { "value": "", "label": "None" }
                            ]
                        }
                    }
                }
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
        let choices = SnowCore::builder()
            .client(client)
            .ui_metadata_basic_auth(
                "test_user",
                crate::credential::SecretString::new("test_pass".to_string()),
            )
            .vault_path(tempdir.path().join("vault"))
            .build()
            .await
            .expect("core")
            .field_choices("change_request", "state")
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
    async fn record_query_enforces_change_and_story_filters_and_exact_projections() {
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_choice"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    { "sys_id": "choice-one", "value": "1", "label": "New", "inactive": "false" },
                    { "sys_id": "choice-two", "value": "2", "label": "In Progress", "inactive": "false" }
                ]
            })))
            .expect(2)
            .mount(&server)
            .await;

        let change_query = concat!(
            "assignment_group=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "^assigned_to=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "^state=2",
            "^start_date>2026-08-01",
            "^start_date<2026-08-31",
            "^sys_id>11111111111111111111111111111111",
            "^ORDERBYsys_id"
        );
        Mock::given(method("GET"))
            .and(path("/api/now/table/change_request"))
            .and(query_param("sysparm_query", change_query))
            .and(query_param(
                "sysparm_fields",
                CHANGE_REQUEST_QUERY_FIELDS.join(","),
            ))
            .and(query_param("sysparm_display_value", "all"))
            .and(query_param("sysparm_limit", "1"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "22222222222222222222222222222222",
                    "number": "CHG0000001",
                    "short_description": "Bounded change",
                    "state": { "value": "2", "display_value": "In Progress" }
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let story_fields = STORY_QUERY_FIELDS
            .iter()
            .chain(STORY_QUERY_DESCRIPTION_FIELDS.iter())
            .copied()
            .collect::<Vec<_>>()
            .join(",");
        let story_query = concat!(
            "assignment_group=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "^assigned_to=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "^u_story_owner=cccccccccccccccccccccccccccccccc",
            "^u_lead_dev=dddddddddddddddddddddddddddddddd",
            "^stateIN1,2",
            "^sprint=eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
            "^project=ffffffffffffffffffffffffffffffff",
            "^cmdb_ci=99999999999999999999999999999999",
            "^blocked=true",
            "^due_date>2026-08-01",
            "^due_date<2026-08-31",
            "^sys_updated_on>2026-08-01 12:00:00",
            "^numberINSTRY1,STRY2",
            "^short_descriptionLIKEidentity",
            "^sys_id>22222222222222222222222222222222",
            "^ORDERBYsys_id"
        );
        Mock::given(method("GET"))
            .and(path("/api/now/table/rm_story"))
            .and(query_param("sysparm_query", story_query))
            .and(query_param("sysparm_fields", story_fields))
            .and(query_param("sysparm_display_value", "all"))
            .and(query_param("sysparm_limit", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "33333333333333333333333333333333",
                    "number": "STRY1",
                    "short_description": "Identity story",
                    "description": "Details",
                    "acceptance_criteria": "Accepted",
                    "state": { "value": "1", "display_value": "New" }
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
        let vault_path = tempdir.path().join("vault");
        let core = SnowCore::builder()
            .client(client)
            .vault_path(&vault_path)
            .build()
            .await
            .expect("core");

        let change_page = core
            .record_query(RecordQueryInput::ChangeRequest {
                filters: ChangeRequestQueryFilters {
                    assignment_group: Some("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA".to_string()),
                    assigned_to: Some("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string()),
                    state: Some("in progress".to_string()),
                    start_date_after: Some("2026-08-01".to_string()),
                    start_date_before: Some("2026-08-31".to_string()),
                },
                limit: Some(1),
                cursor: Some("11111111111111111111111111111111".to_string()),
            })
            .await
            .expect("change page");
        assert_eq!(change_page.rows_inspected, 1);
        assert!(!change_page.complete);
        assert_eq!(
            change_page.next_cursor.as_deref(),
            Some("22222222222222222222222222222222")
        );
        assert_eq!(change_page.source, RecordQuerySource::Live);

        let story_page = core
            .record_query(RecordQueryInput::Story {
                filters: Box::new(StoryQueryFilters {
                    assignment_group: Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string()),
                    assigned_to: Some("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb".to_string()),
                    story_owner: Some("cccccccccccccccccccccccccccccccc".to_string()),
                    lead_developer: Some("dddddddddddddddddddddddddddddddd".to_string()),
                    states: Some(vec!["New".to_string(), "2".to_string()]),
                    sprint: Some("eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee".to_string()),
                    project: Some("ffffffffffffffffffffffffffffffff".to_string()),
                    cmdb_ci: Some("99999999999999999999999999999999".to_string()),
                    blocked: Some(true),
                    due_date_after: Some("2026-08-01".to_string()),
                    due_date_before: Some("2026-08-31".to_string()),
                    updated_after: Some("2026-08-01 12:00:00".to_string()),
                    numbers: Some(vec!["stry1".to_string(), "STRY2".to_string()]),
                    text: Some(" identity ".to_string()),
                }),
                include_description: true,
                limit: Some(2),
                cursor: Some("22222222222222222222222222222222".to_string()),
            })
            .await
            .expect("story page");
        assert_eq!(story_page.rows_inspected, 1);
        assert!(story_page.complete);
        assert_eq!(story_page.next_cursor, None);
        assert_eq!(story_page.records[0].number, "STRY1");

        assert_eq!(
            core.ctx
                .query
                .list_records(ListQuery::new())
                .await
                .expect("cache query")
                .len(),
            0
        );
        assert!(
            !vault_path.exists()
                || std::fs::read_dir(&vault_path)
                    .expect("vault directory")
                    .next()
                    .is_none()
        );
    }

    #[tokio::test]
    async fn record_query_exact_multiple_requires_terminal_empty_page() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/rm_story"))
            .and(query_param("sysparm_query", "^ORDERBYsys_id"))
            .and(query_param("sysparm_fields", STORY_QUERY_FIELDS.join(",")))
            .and(query_param("sysparm_display_value", "all"))
            .and(query_param("sysparm_limit", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    { "sys_id": "11111111111111111111111111111111", "number": "STRY1" },
                    { "sys_id": "22222222222222222222222222222222", "number": "STRY2" }
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/rm_story"))
            .and(query_param(
                "sysparm_query",
                "sys_id>22222222222222222222222222222222^ORDERBYsys_id",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
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

        let first = core
            .record_query(RecordQueryInput::Story {
                filters: Box::new(StoryQueryFilters::default()),
                include_description: false,
                limit: Some(2),
                cursor: None,
            })
            .await
            .expect("first page");
        assert!(!first.complete);
        assert_eq!(
            first.next_cursor.as_deref(),
            Some("22222222222222222222222222222222")
        );

        let terminal = core
            .record_query(RecordQueryInput::Story {
                filters: Box::new(StoryQueryFilters::default()),
                include_description: false,
                limit: Some(2),
                cursor: first.next_cursor,
            })
            .await
            .expect("terminal page");
        assert!(terminal.complete);
        assert_eq!(terminal.rows_inspected, 0);
        assert_eq!(terminal.next_cursor, None);
    }

    #[tokio::test]
    async fn record_query_rejects_invalid_arguments_before_record_io() {
        let server = MockServer::start().await;
        let (core, _tempdir) = core_for_mock_server(&server).await;
        let invalid = [
            RecordQueryInput::Story {
                filters: Box::new(StoryQueryFilters::default()),
                include_description: false,
                limit: Some(0),
                cursor: None,
            },
            RecordQueryInput::Story {
                filters: Box::new(StoryQueryFilters::default()),
                include_description: false,
                limit: Some(201),
                cursor: None,
            },
            RecordQueryInput::Story {
                filters: Box::new(StoryQueryFilters {
                    due_date_after: Some("2026-08-31".to_string()),
                    due_date_before: Some("2026-08-01".to_string()),
                    ..Default::default()
                }),
                include_description: false,
                limit: None,
                cursor: None,
            },
            RecordQueryInput::Story {
                filters: Box::new(StoryQueryFilters {
                    states: Some(vec!["New".to_string(), " new ".to_string()]),
                    ..Default::default()
                }),
                include_description: false,
                limit: None,
                cursor: None,
            },
            RecordQueryInput::Story {
                filters: Box::new(StoryQueryFilters {
                    numbers: Some(vec!["INC1".to_string()]),
                    ..Default::default()
                }),
                include_description: false,
                limit: None,
                cursor: None,
            },
            RecordQueryInput::Story {
                filters: Box::new(StoryQueryFilters::default()),
                include_description: false,
                limit: None,
                cursor: Some("not-a-sys-id".to_string()),
            },
        ];

        for input in invalid {
            let error = core
                .record_query(input)
                .await
                .expect_err("invalid record query must fail");
            assert!(matches!(
                error.downcast_ref::<RecordQueryError>(),
                Some(RecordQueryError::InvalidParams(_))
            ));
        }
        assert!(
            server
                .received_requests()
                .await
                .expect("requests")
                .is_empty(),
            "invalid arguments must fail before any ServiceNow request"
        );
    }

    #[tokio::test]
    async fn record_query_state_correction_fails_before_record_io() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_choice"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    { "value": "1", "label": "New", "inactive": "false" },
                    { "value": "2", "label": "Pending", "inactive": "false" },
                    { "value": "3", "label": "pending", "inactive": "false" }
                ]
            })))
            .expect(3)
            .mount(&server)
            .await;
        let (core, _tempdir) = core_for_mock_server(&server).await;

        for (selectors, expected_ambiguous) in [
            (vec!["Missing".to_string()], false),
            (vec!["Pending".to_string()], true),
            (vec!["1".to_string(), "New".to_string()], true),
        ] {
            let error = core
                .record_query(RecordQueryInput::Story {
                    filters: Box::new(StoryQueryFilters {
                        states: Some(selectors),
                        ..Default::default()
                    }),
                    include_description: false,
                    limit: None,
                    cursor: None,
                })
                .await
                .expect_err("unresolved state must fail");
            assert!(matches!(
                error.downcast_ref::<RecordQueryError>(),
                Some(RecordQueryError::UnresolvedState {
                    ambiguous,
                    choices,
                    ..
                }) if *ambiguous == expected_ambiguous && choices.len() == 3
            ));
        }
        let requests = server.received_requests().await.expect("requests");
        assert!(
            requests
                .iter()
                .all(|request| request.url.path() == "/api/now/table/sys_choice"),
            "state correction must not issue a Story record request"
        );
    }

    #[tokio::test]
    async fn record_query_empty_state_choices_fail_closed_before_record_io() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_choice"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .expect(2)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_db_object"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": []
            })))
            .expect(1)
            .mount(&server)
            .await;
        let (core, _tempdir) = core_for_mock_server(&server).await;

        let error = core
            .record_query(RecordQueryInput::Story {
                filters: Box::new(StoryQueryFilters {
                    states: Some(vec!["1".to_string()]),
                    ..Default::default()
                }),
                include_description: false,
                limit: None,
                cursor: None,
            })
            .await
            .expect_err("empty live choices must fail closed");
        assert!(matches!(
            error.downcast_ref::<RecordQueryError>(),
            Some(RecordQueryError::UnresolvedState {
                ambiguous: false,
                choices,
                ..
            }) if choices.is_empty()
        ));
        let requests = server.received_requests().await.expect("requests");
        assert!(
            requests
                .iter()
                .all(|request| request.url.path() != "/api/now/table/rm_story"),
            "empty choices must not issue a Story record request"
        );
    }

    #[tokio::test]
    async fn record_query_uses_default_and_maximum_page_limits() {
        let server = MockServer::start().await;
        for limit in [50, 200] {
            Mock::given(method("GET"))
                .and(path("/api/now/table/rm_story"))
                .and(query_param("sysparm_query", "^ORDERBYsys_id"))
                .and(query_param("sysparm_limit", limit.to_string()))
                .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                    "result": []
                })))
                .expect(1)
                .mount(&server)
                .await;
        }
        let (core, _tempdir) = core_for_mock_server(&server).await;

        let default_page = core
            .record_query(RecordQueryInput::Story {
                filters: Box::new(StoryQueryFilters::default()),
                include_description: false,
                limit: None,
                cursor: None,
            })
            .await
            .expect("default page");
        assert_eq!(default_page.limit, 50);

        let maximum_page = core
            .record_query(RecordQueryInput::Story {
                filters: Box::new(StoryQueryFilters::default()),
                include_description: false,
                limit: Some(200),
                cursor: None,
            })
            .await
            .expect("maximum page");
        assert_eq!(maximum_page.limit, 200);
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

        // Exact work-record searches are live-only.
        let results = core
            .search_enriched("INC4992697", SearchScope::All)
            .await
            .expect("search");
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].record.number, "INC4992697");
        assert_eq!(results[0].match_in, MatchField::Number);

        // A second search also returns the live result without relying on a
        // projection created by the first request.
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

        assert!(
            core.ctx
                .query
                .get_record("DMNTSK0001122")
                .await
                .expect("inspect local projection")
                .is_none(),
            "exact work-record search must not persist"
        );
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

    // ---------------------------------------------------------------------
    // incident_list_by_assignment_group (T1)
    //
    // Authority: docs/spec-incident-list-by-assignment-group.md
    // ---------------------------------------------------------------------

    const TEST_GROUP_SYS_ID: &str = "0000000000000000000000000000ab01";

    fn incident_state_choices_body() -> serde_json::Value {
        serde_json::json!({
            "result": [
                { "value": "1", "label": "New", "sequence": "100", "inactive": "false" },
                { "value": "3", "label": "Pending", "sequence": "200", "inactive": "false" },
                { "value": "7", "label": "Closed", "sequence": "300", "inactive": "false" }
            ]
        })
    }

    fn incident_row(
        sys_id: &str,
        number: &str,
        state: (&str, &str),
        active: &str,
    ) -> serde_json::Value {
        serde_json::json!({
            "sys_id": { "value": sys_id },
            "number": { "value": number },
            "short_description": { "value": "Ticket" },
            "state": { "value": state.0, "display_value": state.1 },
            "active": { "value": active, "display_value": active },
            "assignment_group": {
                "value": TEST_GROUP_SYS_ID,
                "display_value": "<GROUP_DISPLAY>"
            }
        })
    }

    fn incident_requests(requests: &[wiremock::Request]) -> Vec<&wiremock::Request> {
        requests
            .iter()
            .filter(|request| request.url.path() == "/api/now/table/incident")
            .collect()
    }

    fn sysparm(request: &wiremock::Request, key: &str) -> Option<String> {
        request
            .url
            .query_pairs()
            .find(|(name, _)| name == key)
            .map(|(_, value)| value.into_owned())
    }

    /// L0 core consumer seam: an operator sees only active direct group
    /// memberships, with independently supplied names suitable for selecting a
    /// queue. Removing the membership lookup or returning inactive groups makes
    /// this fail.
    #[tokio::test]
    async fn incident_assignment_group_operations_discovers_active_memberships() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let user_sys_id = "0000000000000000000000000000ac01";

        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_user"))
            .and(query_param("sysparm_query", "user_name=test_user"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": user_sys_id,
                    "user_name": "test_user",
                    "name": "Example Operator"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_user_grmember"))
            .and(query_param_contains(
                "sysparm_query",
                format!("user={user_sys_id}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    {
                        "sys_id": "0000000000000000000000000000ad01",
                        "group": { "value": TEST_GROUP_SYS_ID, "display_value": "Example Operations" },
                        "group.name": "Example Operations",
                        "group.active": "true"
                    },
                    {
                        "sys_id": "0000000000000000000000000000ad02",
                        "group": { "value": "0000000000000000000000000000ab02", "display_value": "Retired Operations" },
                        "group.name": "Retired Operations",
                        "group.active": "false"
                    }
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server_with_user(&server, "test_user").await;
        let groups = core
            .incident_assignment_groups()
            .await
            .expect("active memberships");

        assert_eq!(
            groups,
            vec![IncidentAssignmentGroup {
                sys_id: TEST_GROUP_SYS_ID.to_string(),
                name: "Example Operations".to_string(),
            }]
        );
    }

    /// L0 core consumer seam: a team lead receives a membership-scoped queue
    /// ordered by SLA risk with context and literal handoff counts. Removing
    /// the SLA enrichment, operational projection, sort, or aggregation makes
    /// this fail.
    #[tokio::test]
    async fn incident_assignment_group_operations_returns_triage_context_and_handoff_counts() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let user_sys_id = "0000000000000000000000000000ac01";
        let incident_one = "0000000000000000000000000000ae01";
        let incident_two = "0000000000000000000000000000ae02";

        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_user"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{ "sys_id": user_sys_id, "user_name": "test_user", "name": "Example Operator" }]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_user_grmember"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "0000000000000000000000000000ad01",
                    "group": { "value": TEST_GROUP_SYS_ID, "display_value": "Example Operations" },
                    "group.name": "Example Operations",
                    "group.active": "true"
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/incident"))
            .and(query_param_contains(
                "sysparm_query",
                format!("assignment_group={TEST_GROUP_SYS_ID}"),
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    {
                        "sys_id": incident_one,
                        "number": "INC0000101",
                        "short_description": "Customer cannot sign in",
                        "description": "Authentication fails for the customer.",
                        "state": { "value": "1", "display_value": "New" },
                        "priority": { "value": "3", "display_value": "3 - Moderate" },
                        "impact": { "value": "2", "display_value": "2 - Medium" },
                        "urgency": { "value": "2", "display_value": "2 - Medium" },
                        "opened_at": "2026-08-17 08:00:00",
                        "sys_updated_on": "2026-08-17 08:30:00",
                        "sys_mod_count": "4",
                        "active": "true",
                        "assigned_to": "",
                        "assignment_group": { "value": TEST_GROUP_SYS_ID, "display_value": "Example Operations" },
                        "caller_id": { "value": "0000000000000000000000000000af01", "display_value": "Example Caller" },
                        "cmdb_ci": { "value": "0000000000000000000000000000af02", "display_value": "Example Service CI" },
                        "business_service": { "value": "0000000000000000000000000000af03", "display_value": "Example Service" },
                        "hold_reason": "",
                        "work_notes": "2026-08-17 08:25:00 - Example Operator (Work notes)\nInvestigating authentication.\n"
                    },
                    {
                        "sys_id": incident_two,
                        "number": "INC0000102",
                        "short_description": "Critical service interruption",
                        "description": "A shared service is unavailable.",
                        "state": { "value": "2", "display_value": "In Progress" },
                        "priority": { "value": "1", "display_value": "1 - Critical" },
                        "impact": { "value": "1", "display_value": "1 - High" },
                        "urgency": { "value": "1", "display_value": "1 - High" },
                        "opened_at": "2026-08-17 07:00:00",
                        "sys_updated_on": "2026-08-17 08:40:00",
                        "sys_mod_count": "7",
                        "active": "true",
                        "assigned_to": { "value": user_sys_id, "display_value": "Example Operator" },
                        "assignment_group": { "value": TEST_GROUP_SYS_ID, "display_value": "Example Operations" },
                        "caller_id": { "value": "0000000000000000000000000000af04", "display_value": "Example Caller Two" },
                        "cmdb_ci": { "value": "0000000000000000000000000000af05", "display_value": "Example Shared CI" },
                        "business_service": { "value": "0000000000000000000000000000af06", "display_value": "Example Shared Service" },
                        "hold_reason": "",
                        "work_notes": ""
                    }
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/task_sla"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    {
                        "sys_id": "0000000000000000000000000000b001",
                        "task": { "value": incident_one, "display_value": "INC0000101" },
                        "sla": { "value": "0000000000000000000000000000b101", "display_value": "Response SLA" },
                        "stage": "in_progress",
                        "active": "true",
                        "has_breached": "true",
                        "planned_end_time": "2026-08-17 08:20:00",
                        "business_percentage": "110"
                    },
                    {
                        "sys_id": "0000000000000000000000000000b002",
                        "task": { "value": incident_two, "display_value": "INC0000102" },
                        "sla": { "value": "0000000000000000000000000000b102", "display_value": "Resolution SLA" },
                        "stage": "in_progress",
                        "active": "true",
                        "has_breached": "false",
                        "planned_end_time": "2026-08-17 09:00:00",
                        "business_percentage": "85"
                    }
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;
        let departed_incident = "0000000000000000000000000000d001";
        Mock::given(method("GET"))
            .and(path("/api/now/table/incident"))
            .and(query_param_contains("sysparm_query", departed_incident))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": departed_incident,
                    "state": {"value":"2","display_value":"In Progress"},
                    "active": "true",
                    "assignment_group": {"value":"0000000000000000000000000000d999","display_value":"Another Group"}
                }]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server_with_user(&server, "test_user").await;
        let page = core
            .incident_assignment_group_queue(IncidentAssignmentGroupQueueInput {
                group: "example operations".to_string(),
                sort_by: IncidentQueueSortBy::SlaRisk,
                sort_direction: IncidentQueueSortDirection::Desc,
                limit: Some(10),
                updated_since: Some("2026-08-17 08:00:00".to_string()),
                known_sys_ids: vec![departed_incident.to_string()],
                ..Default::default()
            })
            .await
            .expect("operational queue");

        assert_eq!(page.group.sys_id, TEST_GROUP_SYS_ID);
        assert_eq!(page.items.len(), 2);
        assert_eq!(page.items[0].record.number, "INC0000101");
        assert_eq!(page.items[0].sla.summary.breached, 1);
        assert_eq!(
            page.items[0]
                .record
                .references
                .get("caller_id")
                .map(|reference| reference.display_name.as_str()),
            Some("Example Caller")
        );
        assert_eq!(
            page.items[0]
                .latest_activity
                .as_ref()
                .map(|activity| activity.body.as_str()),
            Some("Investigating authentication.")
        );
        assert_eq!(page.aggregates.by_priority.get("1"), Some(&1));
        assert_eq!(page.aggregates.by_priority.get("3"), Some(&1));
        assert_eq!(page.aggregates.by_sla_risk.get("breached"), Some(&1));
        assert_eq!(page.aggregates.by_sla_risk.get("at_risk"), Some(&1));
        assert_eq!(page.aggregates.unassigned, 1);
        assert!(page.scan_complete);
        assert!(page.complete);
        assert_eq!(page.departed_sys_ids, vec![departed_incident]);
        assert!(page.watermark.as_str() >= "2026-08-17 08:00:00");
    }

    /// A caller can page an entire assignment group: pages are `sys_id`-ordered,
    /// the cursor is exclusive so no record is returned twice, and only the
    /// final short page reports `complete`.
    #[tokio::test]
    async fn incident_group_page_scans_the_group_across_pages() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;

        // First page: a full page, so the scan is not complete.
        Mock::given(method("GET"))
            .and(path("/api/now/table/incident"))
            .and(query_param("sysparm_limit", "2"))
            .and(query_param_contains(
                "sysparm_query",
                format!("assignment_group={TEST_GROUP_SYS_ID}"),
            ))
            .and(query_param_contains("sysparm_query", "active=true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    incident_row("0000000000000000000000000000aa01", "<INC_1>", ("1", "New"), "true"),
                    incident_row("0000000000000000000000000000aa02", "<INC_2>", ("3", "Pending"), "true"),
                ]
            })))
            .up_to_n_times(1)
            .expect(1)
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let first = core
            .incident_list_by_assignment_group(IncidentAssignmentGroupListInput {
                assignment_group_sys_id: TEST_GROUP_SYS_ID.to_string(),
                limit: Some(2),
                ..Default::default()
            })
            .await
            .expect("first page");

        assert_eq!(first.records.len(), 2);
        assert_eq!(first.rows_inspected, 2);
        assert!(!first.complete, "a full page cannot claim the scan is done");
        assert_eq!(
            first.next_cursor.as_deref(),
            Some("0000000000000000000000000000aa02")
        );

        // Second page: the cursor is exclusive, and a short page ends the scan.
        server.reset().await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/incident"))
            .and(query_param_contains(
                "sysparm_query",
                "sys_id>0000000000000000000000000000aa02",
            ))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    incident_row("0000000000000000000000000000aa03", "<INC_3>", ("3", "Pending"), "true"),
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let second = core
            .incident_list_by_assignment_group(IncidentAssignmentGroupListInput {
                assignment_group_sys_id: TEST_GROUP_SYS_ID.to_string(),
                limit: Some(2),
                cursor: first.next_cursor.clone(),
                ..Default::default()
            })
            .await
            .expect("second page");

        assert_eq!(second.records.len(), 1);
        assert!(
            second.complete,
            "a short page means the scan reached the end"
        );
        assert_eq!(second.next_cursor, None);

        let numbers = first
            .records
            .iter()
            .chain(second.records.iter())
            .map(|record| record.number.as_str())
            .collect::<Vec<_>>();
        assert_eq!(numbers, vec!["<INC_1>", "<INC_2>", "<INC_3>"]);

        let requests = server.received_requests().await.expect("requests");
        let incident_requests = incident_requests(&requests);
        assert_eq!(incident_requests.len(), 1);
        assert_eq!(
            sysparm(incident_requests[0], "sysparm_query")
                .as_deref()
                .map(|query| query.contains("ORDERBYsys_id")),
            Some(true),
            "paging is only stable when ordered by sys_id"
        );
    }

    /// `limit` bounds ServiceNow rows requested, not records returned: rows that
    /// are terminal or inactive are rejected locally and the page is simply
    /// shorter.
    #[tokio::test]
    async fn incident_group_page_rejects_terminal_and_inactive_rows() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/incident"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    incident_row("0000000000000000000000000000aa01", "<INC_ACTIVE>", ("3", "Pending"), "true"),
                    incident_row("0000000000000000000000000000aa02", "<INC_INACTIVE>", ("3", "Pending"), "false"),
                    incident_row("0000000000000000000000000000aa03", "<INC_CLOSED>", ("7", "Closed"), "true"),
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let page = core
            .incident_list_by_assignment_group(IncidentAssignmentGroupListInput {
                assignment_group_sys_id: TEST_GROUP_SYS_ID.to_string(),
                limit: Some(10),
                ..Default::default()
            })
            .await
            .expect("page");

        assert_eq!(
            page.records
                .iter()
                .map(|record| record.number.as_str())
                .collect::<Vec<_>>(),
            vec!["<INC_ACTIVE>"]
        );
        assert_eq!(
            page.rows_inspected, 3,
            "`limit` counts rows ServiceNow returned"
        );
    }

    /// A page whose every row is rejected locally still advances, instead of
    /// stalling the scan on an empty page.
    #[tokio::test]
    async fn incident_group_page_advances_when_every_row_is_rejected() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/incident"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    incident_row("0000000000000000000000000000aa01", "<INC_1>", ("7", "Closed"), "true"),
                    incident_row("0000000000000000000000000000aa02", "<INC_2>", ("7", "Closed"), "true"),
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let page = core
            .incident_list_by_assignment_group(IncidentAssignmentGroupListInput {
                assignment_group_sys_id: TEST_GROUP_SYS_ID.to_string(),
                limit: Some(2),
                ..Default::default()
            })
            .await
            .expect("page");

        assert!(page.records.is_empty());
        assert!(!page.complete);
        assert_eq!(
            page.next_cursor.as_deref(),
            Some("0000000000000000000000000000aa02"),
            "cursor must anchor to the last ServiceNow row, not the last surviving record"
        );
    }

    /// An exact label and the raw value it maps to select the same state and
    /// produce the same ServiceNow filter; label matching is case-insensitive.
    #[tokio::test]
    async fn incident_group_page_resolves_state_label_and_raw_value_identically() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_choice"))
            .respond_with(ResponseTemplate::new(200).set_body_json(incident_state_choices_body()))
            .mount(&server)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/incident"))
            .and(query_param_contains("sysparm_query", "state=3"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    incident_row("0000000000000000000000000000aa01", "<INC_1>", ("3", "Pending"), "true"),
                ]
            })))
            .expect(2)
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let by_label = core
            .incident_list_by_assignment_group(IncidentAssignmentGroupListInput {
                assignment_group_sys_id: TEST_GROUP_SYS_ID.to_string(),
                state: Some("pending".to_string()),
                ..Default::default()
            })
            .await
            .expect("label page");
        let by_value = core
            .incident_list_by_assignment_group(IncidentAssignmentGroupListInput {
                assignment_group_sys_id: TEST_GROUP_SYS_ID.to_string(),
                state: Some("3".to_string()),
                ..Default::default()
            })
            .await
            .expect("raw page");

        let expected = Some(ResolvedIncidentState {
            value: "3".to_string(),
            label: "Pending".to_string(),
        });
        assert_eq!(by_label.state, expected);
        assert_eq!(by_value.state, expected);
        assert_eq!(by_label.records.len(), by_value.records.len());
    }

    /// An unusable state selector fails with the live choice list attached, so
    /// the caller can correct it without a second round trip — and no Incident
    /// query is issued with a wrong filter.
    #[tokio::test]
    async fn incident_group_page_reports_choices_for_an_unknown_state() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_choice"))
            .respond_with(ResponseTemplate::new(200).set_body_json(incident_state_choices_body()))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let err = core
            .incident_list_by_assignment_group(IncidentAssignmentGroupListInput {
                assignment_group_sys_id: TEST_GROUP_SYS_ID.to_string(),
                state: Some("Awaiting Vendor".to_string()),
                ..Default::default()
            })
            .await
            .expect_err("unknown state must fail");

        match err.downcast_ref::<IncidentAssignmentGroupListError>() {
            Some(IncidentAssignmentGroupListError::UnresolvedState {
                requested,
                ambiguous,
                choices,
            }) => {
                assert_eq!(requested, "Awaiting Vendor");
                assert!(!ambiguous);
                assert!(
                    choices.iter().any(|choice| choice.label == "Pending"),
                    "the correction path must carry the live choices"
                );
            }
            other => panic!("expected UnresolvedState, got {other:?}"),
        }

        let requests = server.received_requests().await.expect("requests");
        assert!(
            incident_requests(&requests).is_empty(),
            "an unresolved state must not reach the incident table"
        );
    }

    /// A label that maps to more than one value is ambiguous, not a coin flip.
    #[test]
    fn incident_state_resolution_rejects_an_ambiguous_label() {
        let choices = vec![
            FieldChoice {
                value: "3".to_string(),
                label: "Pending".to_string(),
                terminal: false,
            },
            FieldChoice {
                value: "-5".to_string(),
                label: "pending".to_string(),
                terminal: false,
            },
        ];

        let err = resolve_incident_state("Pending", &choices).expect_err("ambiguous label");
        assert!(matches!(
            err,
            IncidentAssignmentGroupListError::UnresolvedState {
                ambiguous: true,
                ..
            }
        ));
    }

    /// Malformed arguments fail before any I/O — a bad group, a bad cursor, or
    /// an out-of-range page size never reaches ServiceNow.
    #[tokio::test]
    async fn incident_group_page_rejects_bad_arguments_before_querying() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let (core, _tempdir) = core_for_mock_server(&server).await;

        let cases = vec![
            IncidentAssignmentGroupListInput {
                assignment_group_sys_id: String::new(),
                ..Default::default()
            },
            IncidentAssignmentGroupListInput {
                assignment_group_sys_id: "Network Support".to_string(),
                ..Default::default()
            },
            IncidentAssignmentGroupListInput {
                assignment_group_sys_id: TEST_GROUP_SYS_ID.to_string(),
                cursor: Some("not-a-sys-id".to_string()),
                ..Default::default()
            },
            IncidentAssignmentGroupListInput {
                assignment_group_sys_id: TEST_GROUP_SYS_ID.to_string(),
                limit: Some(0),
                ..Default::default()
            },
            IncidentAssignmentGroupListInput {
                assignment_group_sys_id: TEST_GROUP_SYS_ID.to_string(),
                limit: Some(INCIDENT_GROUP_LIST_MAX_LIMIT + 1),
                ..Default::default()
            },
        ];

        for input in cases {
            let err = core
                .incident_list_by_assignment_group(input.clone())
                .await
                .expect_err(&format!("{input:?} must be rejected"));
            assert!(
                matches!(
                    err.downcast_ref::<IncidentAssignmentGroupListError>(),
                    Some(IncidentAssignmentGroupListError::InvalidParams(_))
                ),
                "{input:?} must fail with structured invalid parameters, got {err}"
            );
        }

        let requests = server.received_requests().await.expect("requests");
        assert!(
            requests.is_empty(),
            "invalid arguments must fail before any ServiceNow request"
        );
    }

    /// An omitted `limit` requests the approved default page size rather than
    /// an unbounded scan.
    #[tokio::test]
    async fn incident_group_page_requests_the_default_page_size() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/incident"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "result": [] })),
            )
            .expect(1)
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let page = core
            .incident_list_by_assignment_group(IncidentAssignmentGroupListInput {
                assignment_group_sys_id: TEST_GROUP_SYS_ID.to_string(),
                ..Default::default()
            })
            .await
            .expect("page");

        assert_eq!(page.limit, INCIDENT_GROUP_LIST_DEFAULT_LIMIT);
        let requests = server.received_requests().await.expect("requests");
        assert_eq!(
            sysparm(incident_requests(&requests)[0], "sysparm_limit").as_deref(),
            Some(INCIDENT_GROUP_LIST_DEFAULT_LIMIT.to_string().as_str())
        );
    }

    /// Group-wide reads are ephemeral: a completed scan leaves the cache,
    /// store, and vault exactly as it found them.
    #[tokio::test]
    async fn incident_group_page_persists_nothing() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;

        Mock::given(method("GET"))
            .and(path("/api/now/table/incident"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    incident_row("0000000000000000000000000000aa01", "<INC_1>", ("3", "Pending"), "true"),
                ]
            })))
            .expect(1)
            .mount(&server)
            .await;

        let (core, tempdir) = core_for_mock_server(&server).await;
        let page = core
            .incident_list_by_assignment_group(IncidentAssignmentGroupListInput {
                assignment_group_sys_id: TEST_GROUP_SYS_ID.to_string(),
                ..Default::default()
            })
            .await
            .expect("page");
        assert_eq!(
            page.records.len(),
            1,
            "the scan must actually return a record"
        );

        let store = core.ctx.query.store();
        assert_eq!(
            store.count_active_records().expect("count"),
            0,
            "group reads must not populate the local store"
        );
        assert!(
            store
                .get_record_by_sys_id("0000000000000000000000000000aa01")
                .expect("lookup")
                .is_none()
        );
        assert!(
            core.ctx.cache.get("<INC_1>").is_none(),
            "group reads must not populate the work-record cache"
        );

        let vault_path = tempdir.path().join("vault");
        let vault_entries = std::fs::read_dir(&vault_path)
            .map(|entries| entries.count())
            .unwrap_or(0);
        assert_eq!(vault_entries, 0, "group reads must not write the vault");
    }
}
