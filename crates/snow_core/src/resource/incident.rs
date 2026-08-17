use chrono::Utc;
use serde::{Deserialize, Serialize};
use servicenow_rs::prelude::Record;
use std::collections::BTreeMap;

use crate::{
    CacheSource, FieldChoice, FieldValue, JournalEntry, RecordRef, Reference, ResourceType,
    SnowRecord, TaskSlaStatus, normalize_record_lookup_sys_id,
};

#[derive(Debug, Clone, Default)]
pub struct IncidentResource;

impl IncidentResource {
    pub fn from_servicenow(record: &Record) -> SnowRecord {
        let mut model = SnowRecord::from_servicenow(record);
        model.resource_type = ResourceType::Incident;
        model.table = "incident".to_string();
        model.synced_at = Utc::now();
        model.source = CacheSource::Api;
        for (field, reference) in [
            ("caller_id", Self::caller_reference(record)),
            ("assigned_to", Self::assigned_to_reference(record)),
            ("assignment_group", Self::assignment_group_reference(record)),
            ("cmdb_ci", Self::ci_reference(record)),
            (
                "business_service",
                reference_from_record(record, "business_service", "cmdb_ci_service"),
            ),
        ] {
            if let Some(reference) = reference {
                model.references.insert(field.to_string(), reference);
            }
        }
        model
    }

    pub fn record_ref(record: &Record) -> RecordRef {
        RecordRef {
            sys_id: record.sys_id.clone(),
            number: record.get_str("number").unwrap_or_default().to_string(),
            table: "incident".to_string(),
        }
    }

    pub fn caller_reference(record: &Record) -> Option<Reference> {
        reference_from_record(record, "caller_id", "sys_user")
    }

    pub fn assigned_to_reference(record: &Record) -> Option<Reference> {
        reference_from_record(record, "assigned_to", "sys_user")
    }

    pub fn assignment_group_reference(record: &Record) -> Option<Reference> {
        reference_from_record(record, "assignment_group", "sys_user_group")
    }

    pub fn ci_reference(record: &Record) -> Option<Reference> {
        reference_from_record(record, "cmdb_ci", "cmdb_ci")
    }

    pub fn work_notes(record: &Record) -> Vec<JournalEntry> {
        record
            .parse_journal("work_notes")
            .into_iter()
            .map(JournalEntry::from_servicenow)
            .collect()
    }

    pub fn comments(record: &Record) -> Vec<JournalEntry> {
        record
            .parse_journal("comments")
            .into_iter()
            .map(JournalEntry::from_servicenow)
            .collect()
    }

    pub fn fields(record: &Record) -> std::collections::HashMap<String, FieldValue> {
        record
            .fields()
            .iter()
            .map(|(key, value)| {
                (
                    key.clone(),
                    FieldValue {
                        value: value
                            .value
                            .as_ref()
                            .map(|v| match v {
                                serde_json::Value::String(s) => s.clone(),
                                other => other.to_string(),
                            })
                            .unwrap_or_default(),
                        display_value: value.display_value.clone(),
                    },
                )
            })
            .collect()
    }
}

fn reference_from_record(record: &Record, field: &str, table: &str) -> Option<Reference> {
    let sys_id = record.get_raw(field).or_else(|| record.get_str(field))?;
    let display_name = record.get_display(field).unwrap_or(sys_id).to_string();
    let mut extra = std::collections::HashMap::new();

    for dot_field in record.dot_walked_fields(field) {
        if let Some((prefix, _suffix)) = dot_field.0.split_once('.')
            && prefix == field
        {
            if let Some(display) = dot_field.1.display_str() {
                extra.insert(dot_field.0.to_string(), display.to_string());
            } else if let Some(raw) = dot_field.1.raw_str() {
                extra.insert(dot_field.0.to_string(), raw.to_string());
            }
        }
    }

    Some(Reference {
        sys_id: sys_id.to_string(),
        table: table.to_string(),
        display_name,
        extra,
    })
}

/// Default number of ServiceNow rows requested per page when the caller omits
/// `limit`.
///
/// Authority: `docs/spec-incident-list-by-assignment-group.md#decision-gaps-and-blockers`.
/// Mirrors [`crate::RESOURCE_PLAN_LIST_DEFAULT_LIMIT`], the closest existing
/// paged-list precedent in this crate.
pub const INCIDENT_GROUP_LIST_DEFAULT_LIMIT: usize = 50;

/// Largest `limit` this operation accepts. An over-maximum `limit` is a
/// structured invalid-parameter error, deliberately *not* a silent clamp, so a
/// caller never believes it received a larger page than it did.
///
/// Authority: `docs/spec-incident-list-by-assignment-group.md#decision-gaps-and-blockers`.
pub const INCIDENT_GROUP_LIST_MAX_LIMIT: usize = 200;

/// Default upper bound for the live Incident rows inspected by an operational
/// assignment-group queue call.
pub const INCIDENT_QUEUE_DEFAULT_SCAN_LIMIT: usize = 2_000;

/// Hard upper bound for one operational queue scan.
pub const INCIDENT_QUEUE_MAX_SCAN_LIMIT: usize = 5_000;

/// Largest caller baseline accepted for departure detection.
pub const INCIDENT_QUEUE_MAX_KNOWN_SYS_IDS: usize = 1_000;

/// One active direct assignment-group membership for the authenticated user.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct IncidentAssignmentGroup {
    pub sys_id: String,
    pub name: String,
}

/// Sort field for the bounded operational queue.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IncidentQueueSortBy {
    #[default]
    Priority,
    OpenedAt,
    UpdatedAt,
    Assignee,
    SlaRisk,
}

/// Sort direction for the bounded operational queue.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IncidentQueueSortDirection {
    #[default]
    Asc,
    Desc,
}

/// Optional SLA bucket filter.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IncidentQueueSlaRisk {
    #[default]
    Any,
    Healthy,
    AtRisk,
    Breached,
    Unavailable,
}

/// Bounded, membership-scoped operational Incident queue input.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct IncidentAssignmentGroupQueueInput {
    /// Exact group sys_id or exact case-insensitive name from the caller's
    /// active direct memberships.
    pub group: String,
    #[serde(default)]
    pub state: Option<String>,
    /// `me`, `unassigned`, a sys_id, username, or email.
    #[serde(default)]
    pub assigned_to: Option<String>,
    #[serde(default)]
    pub priorities: Vec<u8>,
    #[serde(default)]
    pub opened_after: Option<String>,
    #[serde(default)]
    pub opened_before: Option<String>,
    #[serde(default)]
    pub updated_since: Option<String>,
    #[serde(default)]
    pub updated_before: Option<String>,
    /// Threshold used by the stale bucket. Defaults to 24 hours before the
    /// queue call's watermark.
    #[serde(default)]
    pub stale_before: Option<String>,
    #[serde(default)]
    pub stale_only: bool,
    #[serde(default)]
    pub sla_risk: IncidentQueueSlaRisk,
    #[serde(default = "default_sla_at_risk_percentage")]
    pub sla_at_risk_percentage: f64,
    #[serde(default)]
    pub sort_by: IncidentQueueSortBy,
    #[serde(default)]
    pub sort_direction: IncidentQueueSortDirection,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub offset: Option<usize>,
    #[serde(default)]
    pub scan_limit: Option<usize>,
    /// Caller baseline used to detect records that left the queue.
    #[serde(default)]
    pub known_sys_ids: Vec<String>,
}

fn default_sla_at_risk_percentage() -> f64 {
    80.0
}

impl Default for IncidentAssignmentGroupQueueInput {
    fn default() -> Self {
        Self {
            group: String::new(),
            state: None,
            assigned_to: None,
            priorities: Vec::new(),
            opened_after: None,
            opened_before: None,
            updated_since: None,
            updated_before: None,
            stale_before: None,
            stale_only: false,
            sla_risk: IncidentQueueSlaRisk::Any,
            sla_at_risk_percentage: default_sla_at_risk_percentage(),
            sort_by: IncidentQueueSortBy::Priority,
            sort_direction: IncidentQueueSortDirection::Asc,
            limit: None,
            offset: None,
            scan_limit: None,
            known_sys_ids: Vec::new(),
        }
    }
}

/// One queue row with operational SLA and latest-activity context.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IncidentAssignmentGroupQueueItem {
    pub record: SnowRecord,
    pub sla: TaskSlaStatus,
    pub latest_activity: Option<JournalEntry>,
}

/// Handoff counts over the bounded filtered queue set.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct IncidentAssignmentGroupQueueAggregates {
    pub by_state: BTreeMap<String, usize>,
    pub by_priority: BTreeMap<String, usize>,
    pub by_assignee: BTreeMap<String, usize>,
    pub by_sla_risk: BTreeMap<String, usize>,
    pub unassigned: usize,
    pub stale: usize,
    /// False when the configured scan limit was reached before ServiceNow's
    /// result set was exhausted.
    pub complete: bool,
}

/// Operational assignment-group queue response.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IncidentAssignmentGroupQueuePage {
    pub group: IncidentAssignmentGroup,
    pub items: Vec<IncidentAssignmentGroupQueueItem>,
    pub offset: usize,
    pub next_offset: Option<usize>,
    pub complete: bool,
    pub scan_complete: bool,
    pub rows_scanned: usize,
    pub watermark: String,
    pub departed_sys_ids: Vec<String>,
    pub aggregates: IncidentAssignmentGroupQueueAggregates,
}

/// Structured selector and input failures for assignment-group operations.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IncidentAssignmentGroupOperationsError {
    InvalidParams(String),
    GroupNotFound {
        requested: String,
        available: Vec<IncidentAssignmentGroup>,
    },
    AmbiguousGroup {
        requested: String,
        matches: Vec<IncidentAssignmentGroup>,
    },
}

impl std::fmt::Display for IncidentAssignmentGroupOperationsError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidParams(message) => formatter.write_str(message),
            Self::GroupNotFound { requested, .. } => {
                write!(
                    formatter,
                    "assignment group `{requested}` is not an active membership"
                )
            }
            Self::AmbiguousGroup { requested, .. } => {
                write!(formatter, "assignment group `{requested}` is ambiguous")
            }
        }
    }
}

impl std::error::Error for IncidentAssignmentGroupOperationsError {}

/// Caller-supplied arguments for
/// `SnowCore::incident_list_by_assignment_group`.
///
/// `state` is an *exact* selector: either a raw ServiceNow `incident.state`
/// value (e.g. `"3"`) or an exact, case-insensitive choice label (e.g.
/// `"pending"`). Fuzzy/substring matching is explicitly out of scope.
///
/// `cursor` is the `next_cursor` returned by the previous page and is
/// exclusive: the next page starts strictly after that `sys_id`.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct IncidentAssignmentGroupListInput {
    pub assignment_group_sys_id: String,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub cursor: Option<String>,
}

/// A validated, pre-I/O view of [`IncidentAssignmentGroupListInput`].
///
/// Producing this value proves every argument was checked before any
/// ServiceNow request was issued; `state` is carried through unresolved
/// because resolving it needs a live `field_choices` read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ValidatedIncidentAssignmentGroupQuery {
    pub assignment_group_sys_id: String,
    pub state_selector: Option<String>,
    pub effective_limit: usize,
    pub cursor: Option<String>,
}

/// Structured failure modes for the group-scoped Incident read.
///
/// [`Self::UnresolvedState`] carries the live choice list so an agent caller
/// can correct a bad state selector without a second round trip.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IncidentAssignmentGroupListError {
    InvalidParams(String),
    UnresolvedState {
        requested: String,
        ambiguous: bool,
        choices: Vec<FieldChoice>,
    },
}

impl std::fmt::Display for IncidentAssignmentGroupListError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidParams(message) => formatter.write_str(message),
            Self::UnresolvedState {
                requested,
                ambiguous,
                choices,
            } => {
                let reason = if *ambiguous { "ambiguous" } else { "unknown" };
                let candidates = choices
                    .iter()
                    .map(|choice| format!("{} ({})", choice.label, choice.value))
                    .collect::<Vec<_>>()
                    .join(", ");
                write!(
                    formatter,
                    "`state` selector `{requested}` is {reason} for table `incident`; valid choices: {candidates}"
                )
            }
        }
    }
}

impl std::error::Error for IncidentAssignmentGroupListError {}

/// The exact Incident state a selector resolved to.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ResolvedIncidentState {
    pub value: String,
    pub label: String,
}

/// One page of active Incidents for an assignment group.
///
/// This projection is ephemeral by contract: nothing here is written to the
/// cache, vault, or search index
/// (`docs/spec-incident-list-by-assignment-group.md#decision-gaps-and-blockers`).
///
/// `rows_inspected` is the number of rows ServiceNow returned for the page and
/// is what `limit` bounds; `records` may be shorter after locally rejecting
/// terminal/inactive rows. `complete` is true only when ServiceNow returned
/// fewer rows than `limit` — it means the scan reached the end, never that the
/// result was a transactional snapshot.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IncidentAssignmentGroupPage {
    pub records: Vec<SnowRecord>,
    pub next_cursor: Option<String>,
    pub complete: bool,
    pub limit: usize,
    pub rows_inspected: usize,
    pub state: Option<ResolvedIncidentState>,
}

/// Validates every caller argument before any ServiceNow I/O.
///
/// Rejects a missing/malformed group `sys_id`, a malformed cursor (it is a
/// `sys_id`, so it gets the same shape check), and a `limit` of zero or above
/// [`INCIDENT_GROUP_LIST_MAX_LIMIT`].
pub fn validate_incident_assignment_group_input(
    input: IncidentAssignmentGroupListInput,
) -> std::result::Result<ValidatedIncidentAssignmentGroupQuery, IncidentAssignmentGroupListError> {
    let assignment_group_sys_id = normalize_record_lookup_sys_id(&input.assignment_group_sys_id)
        .map_err(|err| {
            IncidentAssignmentGroupListError::InvalidParams(format!(
                "`assignment_group_sys_id` is required and must be a sys_id: {err}"
            ))
        })?;

    let cursor = match input.cursor.as_deref().map(str::trim) {
        None | Some("") => None,
        Some(cursor) => Some(normalize_record_lookup_sys_id(cursor).map_err(|err| {
            IncidentAssignmentGroupListError::InvalidParams(format!(
                "`cursor` must be a sys_id returned as a previous `next_cursor`: {err}"
            ))
        })?),
    };

    let effective_limit = match input.limit {
        None => INCIDENT_GROUP_LIST_DEFAULT_LIMIT,
        Some(0) => {
            return Err(IncidentAssignmentGroupListError::InvalidParams(
                "`limit` must be at least 1".to_string(),
            ));
        }
        Some(limit) if limit > INCIDENT_GROUP_LIST_MAX_LIMIT => {
            return Err(IncidentAssignmentGroupListError::InvalidParams(format!(
                "`limit` must be at most {INCIDENT_GROUP_LIST_MAX_LIMIT}"
            )));
        }
        Some(limit) => limit,
    };

    let state_selector = match input.state.as_deref().map(str::trim) {
        None | Some("") => None,
        Some(state) => Some(state.to_string()),
    };

    Ok(ValidatedIncidentAssignmentGroupQuery {
        assignment_group_sys_id,
        state_selector,
        effective_limit,
        cursor,
    })
}

/// Resolves an exact state selector against the live Incident state choices.
///
/// A raw value match wins over a label match, so an instance whose labels
/// happen to look like values still resolves deterministically. Label matching
/// is case-insensitive because ServiceNow display values are capitalized.
/// Anything unmatched — or a label that maps to more than one value — fails
/// with the full choice list attached for correction.
pub fn resolve_incident_state(
    selector: &str,
    choices: &[FieldChoice],
) -> std::result::Result<ResolvedIncidentState, IncidentAssignmentGroupListError> {
    let selector = selector.trim();

    if let Some(choice) = choices.iter().find(|choice| choice.value == selector) {
        return Ok(ResolvedIncidentState {
            value: choice.value.clone(),
            label: choice.label.clone(),
        });
    }

    let matches = choices
        .iter()
        .filter(|choice| choice.label.eq_ignore_ascii_case(selector))
        .collect::<Vec<_>>();

    match matches.as_slice() {
        [choice] => Ok(ResolvedIncidentState {
            value: choice.value.clone(),
            label: choice.label.clone(),
        }),
        [] => Err(IncidentAssignmentGroupListError::UnresolvedState {
            requested: selector.to_string(),
            ambiguous: false,
            choices: choices.to_vec(),
        }),
        _ => Err(IncidentAssignmentGroupListError::UnresolvedState {
            requested: selector.to_string(),
            ambiguous: true,
            choices: choices.to_vec(),
        }),
    }
}
