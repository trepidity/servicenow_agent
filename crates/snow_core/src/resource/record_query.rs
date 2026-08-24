use chrono::{NaiveDate, NaiveDateTime};
use serde::{Deserialize, Serialize};

use crate::{FieldChoice, SnowRecord, normalize_record_lookup_sys_id};

pub const RECORD_QUERY_DEFAULT_LIMIT: usize = 50;
pub const RECORD_QUERY_MAX_LIMIT: usize = 200;

pub const CHANGE_REQUEST_QUERY_FIELDS: &[&str] = &[
    "sys_id",
    "number",
    "short_description",
    "state",
    "start_date",
    "end_date",
    "assigned_to",
    "assignment_group",
    "cmdb_ci",
];

/// Fixed, live projection for the Change Request child-read contract.
///
/// This is intentionally separate from the general Change Request query: the
/// only selector is a parent CHG number and the only rows are CTASK children.
pub const CHANGE_REQUEST_TASK_QUERY_FIELDS: &[&str] = &[
    "sys_id",
    "number",
    "short_description",
    "state",
    "due_date",
    "planned_start_date",
    "planned_end_date",
    "assigned_to",
    "assignment_group",
    "change_task_type",
    "cmdb_ci",
    "change_request",
];

pub const STORY_QUERY_FIELDS: &[&str] = &[
    "sys_id",
    "number",
    "short_description",
    "state",
    "sprint",
    "project",
    "cmdb_ci",
    "assigned_to",
    "assignment_group",
    "u_story_owner",
    "u_lead_dev",
    "u_points_est",
    "due_date",
    "desired_delivery_date",
    "blocked",
    "blocked_reason",
    "status",
    "sys_updated_on",
    "sys_updated_by",
];

pub const STORY_QUERY_DESCRIPTION_FIELDS: &[&str] = &["description", "acceptance_criteria"];

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "resource_type", rename_all = "snake_case", deny_unknown_fields)]
pub enum RecordQueryInput {
    ChangeRequest {
        #[serde(default)]
        filters: ChangeRequestQueryFilters,
        #[serde(default)]
        limit: Option<usize>,
        #[serde(default)]
        cursor: Option<String>,
    },
    Story {
        #[serde(default)]
        filters: Box<StoryQueryFilters>,
        #[serde(default)]
        include_description: bool,
        #[serde(default)]
        limit: Option<usize>,
        #[serde(default)]
        cursor: Option<String>,
    },
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ChangeRequestQueryFilters {
    #[serde(default)]
    pub assignment_group: Option<String>,
    #[serde(default)]
    pub assigned_to: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub start_date_after: Option<String>,
    #[serde(default)]
    pub start_date_before: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ChangeRequestTaskListInput {
    pub change_request_number: String,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedChangeRequestTaskListInput {
    pub change_request_number: String,
    pub limit: usize,
    pub cursor: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct StoryQueryFilters {
    #[serde(default)]
    pub assignment_group: Option<String>,
    #[serde(default)]
    pub assigned_to: Option<String>,
    #[serde(default)]
    pub story_owner: Option<String>,
    #[serde(default)]
    pub lead_developer: Option<String>,
    #[serde(default)]
    pub states: Option<Vec<String>>,
    #[serde(default)]
    pub sprint: Option<String>,
    #[serde(default)]
    pub project: Option<String>,
    #[serde(default)]
    pub cmdb_ci: Option<String>,
    #[serde(default)]
    pub blocked: Option<bool>,
    #[serde(default)]
    pub due_date_after: Option<String>,
    #[serde(default)]
    pub due_date_before: Option<String>,
    #[serde(default)]
    pub updated_after: Option<String>,
    #[serde(default)]
    pub numbers: Option<Vec<String>>,
    #[serde(default)]
    pub text: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct RecordQueryPage {
    pub records: Vec<SnowRecord>,
    pub next_cursor: Option<String>,
    pub complete: bool,
    pub source: RecordQuerySource,
    pub limit: usize,
    pub rows_inspected: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecordQuerySource {
    Live,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordQueryError {
    InvalidParams(String),
    UnresolvedState {
        requested: String,
        table: String,
        field: String,
        ambiguous: bool,
        choices: Vec<FieldChoice>,
    },
}

impl std::fmt::Display for RecordQueryError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidParams(message) => formatter.write_str(message),
            Self::UnresolvedState {
                requested,
                table,
                field,
                ambiguous,
                ..
            } => {
                let reason = if *ambiguous { "ambiguous" } else { "unknown" };
                write!(
                    formatter,
                    "selector `{requested}` is {reason} for `{table}.{field}`"
                )
            }
        }
    }
}

impl std::error::Error for RecordQueryError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ValidatedRecordQuery {
    ChangeRequest {
        filters: ChangeRequestQueryFilters,
        limit: usize,
        cursor: Option<String>,
    },
    Story {
        filters: Box<StoryQueryFilters>,
        include_description: bool,
        limit: usize,
        cursor: Option<String>,
    },
}

pub fn validate_record_query(
    input: RecordQueryInput,
) -> Result<ValidatedRecordQuery, RecordQueryError> {
    match input {
        RecordQueryInput::ChangeRequest {
            mut filters,
            limit,
            cursor,
        } => {
            filters.assignment_group =
                normalize_optional_sys_id("filters.assignment_group", filters.assignment_group)?;
            filters.assigned_to =
                normalize_optional_sys_id("filters.assigned_to", filters.assigned_to)?;
            filters.state = normalize_optional_text("filters.state", filters.state, 80)?;
            filters.start_date_after =
                normalize_optional_date("filters.start_date_after", filters.start_date_after)?;
            filters.start_date_before =
                normalize_optional_date("filters.start_date_before", filters.start_date_before)?;
            validate_date_range(
                "start_date",
                filters.start_date_after.as_deref(),
                filters.start_date_before.as_deref(),
            )?;
            Ok(ValidatedRecordQuery::ChangeRequest {
                filters,
                limit: validate_limit(limit)?,
                cursor: normalize_cursor(cursor)?,
            })
        }
        RecordQueryInput::Story {
            filters,
            include_description,
            limit,
            cursor,
        } => {
            let mut filters = *filters;
            for (name, value) in [
                ("filters.assignment_group", &mut filters.assignment_group),
                ("filters.assigned_to", &mut filters.assigned_to),
                ("filters.story_owner", &mut filters.story_owner),
                ("filters.lead_developer", &mut filters.lead_developer),
                ("filters.sprint", &mut filters.sprint),
                ("filters.project", &mut filters.project),
                ("filters.cmdb_ci", &mut filters.cmdb_ci),
            ] {
                *value = normalize_optional_sys_id(name, value.take())?;
            }
            filters.states = normalize_states(filters.states)?;
            filters.due_date_after =
                normalize_optional_date("filters.due_date_after", filters.due_date_after)?;
            filters.due_date_before =
                normalize_optional_date("filters.due_date_before", filters.due_date_before)?;
            validate_date_range(
                "due_date",
                filters.due_date_after.as_deref(),
                filters.due_date_before.as_deref(),
            )?;
            filters.updated_after = normalize_optional_timestamp(filters.updated_after)?;
            filters.numbers = normalize_story_numbers(filters.numbers)?;
            filters.text = normalize_optional_text("filters.text", filters.text, 200)?;
            Ok(ValidatedRecordQuery::Story {
                filters: Box::new(filters),
                include_description,
                limit: validate_limit(limit)?,
                cursor: normalize_cursor(cursor)?,
            })
        }
    }
}

pub fn validate_change_request_task_list(
    input: ChangeRequestTaskListInput,
) -> Result<ValidatedChangeRequestTaskListInput, RecordQueryError> {
    let change_request_number = input.change_request_number.trim().to_ascii_uppercase();
    if !change_request_number.starts_with("CHG") || change_request_number.len() <= 3 {
        return Err(RecordQueryError::InvalidParams(
            "`change_request_number` must be a non-empty CHG number".to_string(),
        ));
    }
    Ok(ValidatedChangeRequestTaskListInput {
        change_request_number,
        limit: validate_limit(input.limit)?,
        cursor: normalize_cursor(input.cursor)?,
    })
}

pub fn resolve_record_query_state(
    selector: &str,
    table: &str,
    choices: &[FieldChoice],
) -> Result<String, RecordQueryError> {
    if let Some(choice) = choices.iter().find(|choice| choice.value == selector) {
        return Ok(choice.value.clone());
    }
    let matches = choices
        .iter()
        .filter(|choice| choice.label.eq_ignore_ascii_case(selector))
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [choice] => Ok(choice.value.clone()),
        [] => Err(RecordQueryError::UnresolvedState {
            requested: selector.to_string(),
            table: table.to_string(),
            field: "state".to_string(),
            ambiguous: false,
            choices: choices.to_vec(),
        }),
        _ => Err(RecordQueryError::UnresolvedState {
            requested: selector.to_string(),
            table: table.to_string(),
            field: "state".to_string(),
            ambiguous: true,
            choices: choices.to_vec(),
        }),
    }
}

fn validate_limit(limit: Option<usize>) -> Result<usize, RecordQueryError> {
    match limit {
        None => Ok(RECORD_QUERY_DEFAULT_LIMIT),
        Some(0) => Err(RecordQueryError::InvalidParams(
            "`limit` must be at least 1".to_string(),
        )),
        Some(value) if value > RECORD_QUERY_MAX_LIMIT => Err(RecordQueryError::InvalidParams(
            format!("`limit` must be at most {RECORD_QUERY_MAX_LIMIT}"),
        )),
        Some(value) => Ok(value),
    }
}

fn normalize_cursor(cursor: Option<String>) -> Result<Option<String>, RecordQueryError> {
    normalize_optional_sys_id("cursor", cursor)
}

fn normalize_optional_sys_id(
    name: &str,
    value: Option<String>,
) -> Result<Option<String>, RecordQueryError> {
    value
        .map(|value| {
            normalize_record_lookup_sys_id(value.trim()).map_err(|error| {
                RecordQueryError::InvalidParams(format!("`{name}` must be a sys_id: {error}"))
            })
        })
        .transpose()
}

fn normalize_optional_text(
    name: &str,
    value: Option<String>,
    max_chars: usize,
) -> Result<Option<String>, RecordQueryError> {
    value
        .map(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                return Err(RecordQueryError::InvalidParams(format!(
                    "`{name}` must not be empty"
                )));
            }
            if trimmed.chars().count() > max_chars {
                return Err(RecordQueryError::InvalidParams(format!(
                    "`{name}` must contain at most {max_chars} characters"
                )));
            }
            Ok(trimmed.to_string())
        })
        .transpose()
}

fn normalize_optional_date(
    name: &str,
    value: Option<String>,
) -> Result<Option<String>, RecordQueryError> {
    value
        .map(|value| {
            let trimmed = value.trim();
            if trimmed.len() != 10 || NaiveDate::parse_from_str(trimmed, "%Y-%m-%d").is_err() {
                return Err(RecordQueryError::InvalidParams(format!(
                    "`{name}` must use YYYY-MM-DD"
                )));
            }
            Ok(trimmed.to_string())
        })
        .transpose()
}

fn normalize_optional_timestamp(value: Option<String>) -> Result<Option<String>, RecordQueryError> {
    value
        .map(|value| {
            let trimmed = value.trim();
            if trimmed.len() != 19
                || NaiveDateTime::parse_from_str(trimmed, "%Y-%m-%d %H:%M:%S").is_err()
            {
                return Err(RecordQueryError::InvalidParams(
                    "`filters.updated_after` must use YYYY-MM-DD HH:MM:SS".to_string(),
                ));
            }
            Ok(trimmed.to_string())
        })
        .transpose()
}

fn validate_date_range(
    name: &str,
    after: Option<&str>,
    before: Option<&str>,
) -> Result<(), RecordQueryError> {
    if let (Some(after), Some(before)) = (after, before)
        && after >= before
    {
        return Err(RecordQueryError::InvalidParams(format!(
            "`filters.{name}_after` must be earlier than `filters.{name}_before`"
        )));
    }
    Ok(())
}

fn normalize_states(states: Option<Vec<String>>) -> Result<Option<Vec<String>>, RecordQueryError> {
    let Some(states) = states else {
        return Ok(None);
    };
    if states.is_empty() || states.len() > 20 {
        return Err(RecordQueryError::InvalidParams(
            "`filters.states` must contain 1 to 20 values".to_string(),
        ));
    }
    let mut seen = std::collections::HashSet::new();
    let mut normalized = Vec::with_capacity(states.len());
    for state in states {
        let state = normalize_optional_text("filters.states[]", Some(state), 80)?
            .expect("provided state normalizes to Some");
        if !seen.insert(state.to_ascii_lowercase()) {
            return Err(RecordQueryError::InvalidParams(
                "`filters.states` contains duplicate values".to_string(),
            ));
        }
        normalized.push(state);
    }
    Ok(Some(normalized))
}

fn normalize_story_numbers(
    numbers: Option<Vec<String>>,
) -> Result<Option<Vec<String>>, RecordQueryError> {
    let Some(numbers) = numbers else {
        return Ok(None);
    };
    if numbers.is_empty() || numbers.len() > 20 {
        return Err(RecordQueryError::InvalidParams(
            "`filters.numbers` must contain 1 to 20 values".to_string(),
        ));
    }
    let mut seen = std::collections::HashSet::new();
    let mut normalized = Vec::with_capacity(numbers.len());
    for number in numbers {
        let number = number.trim().to_ascii_uppercase();
        let valid = number.strip_prefix("STRY").is_some_and(|digits| {
            !digits.is_empty() && digits.bytes().all(|byte| byte.is_ascii_digit())
        });
        if !valid {
            return Err(RecordQueryError::InvalidParams(
                "`filters.numbers[]` must match STRY followed by digits".to_string(),
            ));
        }
        if !seen.insert(number.clone()) {
            return Err(RecordQueryError::InvalidParams(
                "`filters.numbers` contains duplicate values".to_string(),
            ));
        }
        normalized.push(number);
    }
    Ok(Some(normalized))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn wire_input_rejects_unknown_and_cross_resource_properties() {
        for invalid in [
            serde_json::json!({"resource_type":"change_request","filter":"state=1"}),
            serde_json::json!({"resource_type":"change_request","include_description":true}),
            serde_json::json!({"resource_type":"change_request","filters":{"text":"x"}}),
            serde_json::json!({"resource_type":"story","filters":{"start_date_after":"2026-01-01"}}),
        ] {
            assert!(
                serde_json::from_value::<RecordQueryInput>(invalid).is_err(),
                "invalid cross-resource or unknown property was accepted"
            );
        }
    }
}
