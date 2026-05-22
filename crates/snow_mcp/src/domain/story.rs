use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use super::primitives::{RecordRef, UserRef};
use crate::domain::policy::BoardBinding;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Story {
    pub record: RecordRef,
    pub assigned_to: Option<UserRef>,
    pub state: String,
    pub state_display: String,
    pub sprint: Option<String>,
    pub story_points: Option<u32>,
    pub parent: Option<RecordRef>,
    pub task_count: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoryTask {
    pub record: RecordRef,
    pub parent_story: RecordRef,
    pub assigned_to: Option<UserRef>,
    pub state: String,
    pub state_display: String,
    pub short_description: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StoryCreatePayload {
    pub short_description: String,
    pub description: String,
    #[serde(default)]
    pub acceptance_criteria: Option<String>,
    #[serde(default)]
    pub priority: Option<String>,
    #[serde(default)]
    pub epic: Option<String>,
    #[serde(default)]
    pub story_points: Option<StoryPoints>,
    #[serde(default)]
    pub assigned_to: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StoryUpdatePayload {
    pub target_number: String,
    #[serde(default)]
    pub short_description: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub acceptance_criteria: Option<String>,
    #[serde(default)]
    pub priority: Option<String>,
    #[serde(default)]
    pub epic: Option<String>,
    #[serde(default)]
    pub story_points: Option<StoryPoints>,
    #[serde(default)]
    pub assigned_to: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub percent_complete: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StoryTaskCreatePayload {
    pub parent_story_number: String,
    pub short_description: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub assigned_to: Option<String>,
    #[serde(default)]
    pub priority: Option<String>,
    #[serde(default)]
    pub due_date: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub planned_hours: Option<f32>,
    #[serde(default)]
    pub actual_hours: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct StoryTaskUpdatePayload {
    pub target_number: String,
    #[serde(default)]
    pub short_description: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub assigned_to: Option<String>,
    #[serde(default)]
    pub priority: Option<String>,
    #[serde(default)]
    pub due_date: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub planned_hours: Option<f32>,
    #[serde(default)]
    pub actual_hours: Option<f32>,
    #[serde(default)]
    pub remaining_hours: Option<f32>,
    #[serde(default)]
    pub percent_complete: Option<f32>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum StoryPoints {
    Numeric(u32),
    Text(String),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "operation", content = "payload", rename_all = "snake_case")]
pub enum StoryPlannedChanges {
    StoryCreate(StoryCreatePayload),
    StoryUpdate(StoryUpdatePayload),
    StoryTaskCreate(StoryTaskCreatePayload),
    StoryTaskUpdate(StoryTaskUpdatePayload),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StoryTarget {
    pub table: String,
    pub number: String,
    #[serde(default)]
    pub sys_id: Option<String>,
}

impl StoryTarget {
    pub fn story(
        number: impl Into<String>,
        sys_id: Option<String>,
        binding: &BoardBinding,
    ) -> Self {
        Self {
            table: binding.story_table.clone(),
            number: number.into(),
            sys_id,
        }
    }

    pub fn task(number: impl Into<String>, sys_id: Option<String>, binding: &BoardBinding) -> Self {
        Self {
            table: binding.task_table.clone(),
            number: number.into(),
            sys_id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum InScopeFailureClause {
    ActiveFalse,
    AssignmentGroupMismatch,
    SprintOutOfAllowed,
    TaskParentStoryMismatch,
    TaskParentOutOfScope,
}

impl InScopeFailureClause {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::ActiveFalse => "active_false",
            Self::AssignmentGroupMismatch => "assignment_group_mismatch",
            Self::SprintOutOfAllowed => "sprint_out_of_allowed",
            Self::TaskParentStoryMismatch => "task_parent_story_mismatch",
            Self::TaskParentOutOfScope => "task_parent_out_of_scope",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum InScopeFailure {
    NotActive,
    WrongAssignmentGroup {
        expected: String,
        observed: String,
    },
    MissingSprint,
    SprintNotAllowed {
        observed: String,
        allowed: Vec<String>,
    },
    TaskParentStoryMismatch {
        expected: String,
        observed: String,
    },
    TaskParentOutOfScope {
        cause: Box<InScopeFailure>,
    },
}

impl InScopeFailure {
    pub fn clause(&self) -> InScopeFailureClause {
        match self {
            Self::NotActive => InScopeFailureClause::ActiveFalse,
            Self::WrongAssignmentGroup { .. } => InScopeFailureClause::AssignmentGroupMismatch,
            Self::MissingSprint | Self::SprintNotAllowed { .. } => {
                InScopeFailureClause::SprintOutOfAllowed
            }
            Self::TaskParentStoryMismatch { .. } => InScopeFailureClause::TaskParentStoryMismatch,
            Self::TaskParentOutOfScope { .. } => InScopeFailureClause::TaskParentOutOfScope,
        }
    }

    pub fn to_guard_failed_data(&self) -> GuardFailedData {
        let clause = self.clause();
        let (message, expected, observed) = match self {
            Self::NotActive => (
                "Story is inactive".to_string(),
                None,
                Some("false".to_string()),
            ),
            Self::WrongAssignmentGroup { expected, observed } => (
                format!(
                    "Story assignment_group '{observed}' does not match BoardBinding.assignment_group"
                ),
                Some(Value::String(expected.clone())),
                Some(observed.clone()),
            ),
            Self::MissingSprint => (
                "Story has no sprint and cannot be proven inside BoardBinding.allowed_sprints"
                    .to_string(),
                None,
                None,
            ),
            Self::SprintNotAllowed { observed, allowed } => (
                format!("Story sprint '{observed}' is not in BoardBinding.allowed_sprints"),
                Some(Value::Array(
                    allowed.iter().cloned().map(Value::String).collect(),
                )),
                Some(observed.clone()),
            ),
            Self::TaskParentStoryMismatch { expected, observed } => (
                format!("Story Task parent '{observed}' does not match fetched Story '{expected}'"),
                Some(Value::String(expected.clone())),
                Some(observed.clone()),
            ),
            Self::TaskParentOutOfScope { cause } => (
                format!(
                    "Story Task parent Story failed board scope guard: {}",
                    cause.clause().as_str()
                ),
                None,
                None,
            ),
        };

        GuardFailedData {
            code: ERROR_GUARD_FAILED.to_string(),
            clause,
            message,
            expected,
            observed,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct GuardFailedData {
    pub code: String,
    pub clause: InScopeFailureClause,
    pub message: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub expected: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InScopeSnapshot<'a> {
    pub active: bool,
    pub assignment_group_sys_id: &'a str,
    pub sprint_sys_id: Option<&'a str>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OwnedInScopeSnapshot {
    pub active: bool,
    pub assignment_group_sys_id: String,
    pub sprint_sys_id: Option<String>,
}

impl OwnedInScopeSnapshot {
    pub fn as_snapshot(&self) -> InScopeSnapshot<'_> {
        InScopeSnapshot {
            active: self.active,
            assignment_group_sys_id: &self.assignment_group_sys_id,
            sprint_sys_id: self.sprint_sys_id.as_deref(),
        }
    }
}

pub fn story_in_scope(
    snapshot: &InScopeSnapshot<'_>,
    binding: &BoardBinding,
) -> Result<(), InScopeFailure> {
    if !snapshot.active {
        return Err(InScopeFailure::NotActive);
    }

    if !equal_sys_id(snapshot.assignment_group_sys_id, &binding.assignment_group) {
        return Err(InScopeFailure::WrongAssignmentGroup {
            expected: binding.assignment_group.clone(),
            observed: snapshot.assignment_group_sys_id.to_string(),
        });
    }

    let Some(sprint) = snapshot.sprint_sys_id else {
        return Err(InScopeFailure::MissingSprint);
    };

    if !binding
        .allowed_sprints
        .iter()
        .any(|allowed| equal_sys_id(sprint, allowed))
    {
        return Err(InScopeFailure::SprintNotAllowed {
            observed: sprint.to_string(),
            allowed: binding.allowed_sprints.clone(),
        });
    }

    Ok(())
}

pub fn task_parent_in_scope(
    parent_snapshot: &InScopeSnapshot<'_>,
    task_parent_sys_id: &str,
    parent_sys_id: &str,
    binding: &BoardBinding,
) -> Result<(), InScopeFailure> {
    story_in_scope(parent_snapshot, binding).map_err(|cause| {
        InScopeFailure::TaskParentOutOfScope {
            cause: Box::new(cause),
        }
    })?;

    if !equal_sys_id(task_parent_sys_id, parent_sys_id) {
        return Err(InScopeFailure::TaskParentStoryMismatch {
            expected: parent_sys_id.to_string(),
            observed: task_parent_sys_id.to_string(),
        });
    }

    Ok(())
}

pub fn equal_sys_id(a: &str, b: &str) -> bool {
    a.trim().eq_ignore_ascii_case(b.trim())
}

pub fn is_sys_id(value: &str) -> bool {
    let trimmed = value.trim();
    trimmed.len() == 32 && trimmed.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub fn in_scope_snapshot_from_json(value: &Value) -> Option<OwnedInScopeSnapshot> {
    let assignment_group_sys_id = reference_sys_id_from_json(value, "assignment_group")?;
    let sprint_sys_id = reference_sys_id_from_json(value, "sprint");
    let active = bool_from_json_field(value, "active").unwrap_or(true);

    Some(OwnedInScopeSnapshot {
        active,
        assignment_group_sys_id,
        sprint_sys_id,
    })
}

pub fn in_scope_snapshot_from_snow_record(
    record: &snow_core::SnowRecord,
) -> Option<OwnedInScopeSnapshot> {
    let assignment_group_sys_id = record
        .references
        .get("assignment_group")
        .map(|reference| reference.sys_id.clone())
        .or_else(|| {
            record
                .fields
                .get("assignment_group")
                .map(|field| field.value.clone())
        })?;
    let sprint_sys_id = record
        .references
        .get("sprint")
        .map(|reference| reference.sys_id.clone())
        .or_else(|| record.fields.get("sprint").map(|field| field.value.clone()));
    let active = record
        .fields
        .get("active")
        .and_then(|field| parse_boolish(&field.value))
        .unwrap_or(true);

    Some(OwnedInScopeSnapshot {
        active,
        assignment_group_sys_id,
        sprint_sys_id,
    })
}

pub fn task_parent_sys_id_from_json(value: &Value) -> Option<String> {
    reference_sys_id_from_json(value, "story")
        .or_else(|| reference_sys_id_from_json(value, "parent_story"))
        .or_else(|| reference_sys_id_from_json(value, "parent"))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct StateChoice {
    pub value: String,
    pub label: String,
    pub terminal: bool,
}

impl StateChoice {
    pub fn allowed(value: impl Into<String>, label: impl Into<String>) -> Self {
        Self {
            value: value.into(),
            label: label.into(),
            terminal: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StateChoiceMatch {
    Raw { value: String },
    Label { label: String, value: String },
}

#[derive(Debug, Clone, Copy, Default)]
pub struct StateResolutionContext<'a> {
    pub supplied: Option<&'a str>,
    pub current_state_value: Option<&'a str>,
    pub current_state_terminal: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum FieldRejectionReason {
    #[serde(rename = "value_not_in_enum")]
    ValueNotInEnum,
}

impl FieldRejectionReason {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::ValueNotInEnum => "value_not_in_enum",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum StateResolution {
    Omitted,
    Resolved {
        value: String,
    },
    Mapped {
        label: String,
        value: String,
    },
    Required {
        candidates: Vec<StateChoice>,
        operator_note: String,
    },
    FieldRejected {
        field: String,
        reason: FieldRejectionReason,
        original: String,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum ResolveStateError {
    #[error("dictionary lookup failed for {table}.{field}: {message}")]
    Dictionary {
        table: String,
        field: String,
        message: String,
    },
}

#[async_trait::async_trait(?Send)]
pub trait DictionaryReader {
    async fn sys_choice(
        &self,
        table: &str,
        field: &str,
    ) -> Result<Vec<StateChoice>, ResolveStateError>;
}

pub fn match_cached_state_choice(
    supplied: &str,
    choices: &[StateChoice],
) -> Option<StateChoiceMatch> {
    let supplied = supplied.trim();
    choices.iter().find_map(|choice| {
        if choice.terminal {
            return None;
        }
        if choice.value.trim().eq_ignore_ascii_case(supplied) {
            return Some(StateChoiceMatch::Raw {
                value: choice.value.clone(),
            });
        }
        if choice.label.trim().eq_ignore_ascii_case(supplied) {
            return Some(StateChoiceMatch::Label {
                label: choice.label.clone(),
                value: choice.value.clone(),
            });
        }
        None
    })
}

pub fn resolve_state_from_cached_choices(
    context: StateResolutionContext<'_>,
    choices: &[StateChoice],
) -> StateResolution {
    let Some(supplied) = context
        .supplied
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return StateResolution::Omitted;
    };

    match match_cached_state_choice(supplied, choices) {
        Some(StateChoiceMatch::Raw { value }) => return StateResolution::Resolved { value },
        Some(StateChoiceMatch::Label { label, value }) => {
            return StateResolution::Mapped { label, value };
        }
        None => {}
    }

    if context.current_state_terminal {
        return StateResolution::Required {
            candidates: non_terminal_state_choices(choices),
            operator_note: "caller-supplied state did not map and prior record state is terminal"
                .to_string(),
        };
    }

    StateResolution::FieldRejected {
        field: "state".to_string(),
        reason: FieldRejectionReason::ValueNotInEnum,
        original: supplied.to_string(),
    }
}

pub async fn resolve_state(
    table: &str,
    context: StateResolutionContext<'_>,
    binding: &BoardBinding,
    dictionary: &dyn DictionaryReader,
) -> Result<StateResolution, ResolveStateError> {
    let mut choices = state_choices_from_binding(table, binding);
    if choices.is_empty() {
        choices = dictionary
            .sys_choice(table, "state")
            .await?
            .into_iter()
            .filter(|choice| !choice.terminal)
            .collect();
    }

    Ok(resolve_state_from_cached_choices(context, &choices))
}

pub fn state_choices_from_binding(table: &str, binding: &BoardBinding) -> Vec<StateChoice> {
    let allowed = if table == binding.story_table || table == "rm_story" {
        &binding.allowed_story_states
    } else if table == binding.task_table || table == "rm_scrum_task" {
        &binding.allowed_task_states
    } else {
        &binding.allowed_story_states
    };

    allowed
        .iter()
        .map(|value| StateChoice::allowed(value.clone(), value.clone()))
        .collect()
}

pub fn current_state_is_terminal(current_state_value: &str, choices: &[StateChoice]) -> bool {
    choices.iter().any(|choice| {
        choice.terminal
            && choice
                .value
                .trim()
                .eq_ignore_ascii_case(current_state_value.trim())
    })
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum AssigneeResolution {
    Supplied {
        sys_id: String,
    },
    DefaultedFromCaller {
        sys_id: String,
        source: ActorIdentitySource,
    },
    Unresolved,
    Ambiguous {
        candidate_sys_ids: Vec<String>,
    },
    ExplicitNull,
    Invalid {
        original: String,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ActorIdentitySource {
    EmailMatch,
    UserNameMatch,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Actor {
    pub subject: String,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub display_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UserLookupHit {
    pub sys_id: String,
}

#[async_trait::async_trait(?Send)]
pub trait UserLookup {
    async fn find_active_by_email(&self, email: &str) -> Vec<UserLookupHit>;
    async fn find_active_by_user_name(&self, user_name: &str) -> Vec<UserLookupHit>;
}

pub async fn resolve_assignee(
    supplied: Option<&Value>,
    actor: &Actor,
    lookup: &dyn UserLookup,
) -> AssigneeResolution {
    match supplied {
        Some(Value::Null) => AssigneeResolution::ExplicitNull,
        Some(Value::String(value)) if value.trim().eq_ignore_ascii_case("caller") => {
            resolve_caller_assignee(actor, lookup).await
        }
        Some(Value::String(value)) if is_sys_id(value) => AssigneeResolution::Supplied {
            sys_id: value.trim().to_string(),
        },
        Some(Value::String(value)) => AssigneeResolution::Invalid {
            original: value.clone(),
        },
        None => resolve_caller_assignee(actor, lookup).await,
        Some(other) => AssigneeResolution::Invalid {
            original: other.to_string(),
        },
    }
}

pub async fn resolve_caller_assignee(actor: &Actor, lookup: &dyn UserLookup) -> AssigneeResolution {
    let mut ambiguous_candidates = Vec::new();

    if let Some(email) = actor
        .email
        .as_deref()
        .map(str::trim)
        .filter(|email| !email.is_empty())
    {
        let email_hits = lookup.find_active_by_email(email).await;
        match email_hits.as_slice() {
            [hit] => {
                return AssigneeResolution::DefaultedFromCaller {
                    sys_id: hit.sys_id.clone(),
                    source: ActorIdentitySource::EmailMatch,
                };
            }
            [] => {}
            hits => ambiguous_candidates.extend(hits.iter().map(|hit| hit.sys_id.clone())),
        }
    }

    let user_name = actor.subject.trim();
    if !user_name.is_empty() {
        let user_name_hits = lookup.find_active_by_user_name(user_name).await;
        match user_name_hits.as_slice() {
            [hit] => {
                return AssigneeResolution::DefaultedFromCaller {
                    sys_id: hit.sys_id.clone(),
                    source: ActorIdentitySource::UserNameMatch,
                };
            }
            [] => {}
            hits => ambiguous_candidates.extend(hits.iter().map(|hit| hit.sys_id.clone())),
        }
    }

    if ambiguous_candidates.is_empty() {
        AssigneeResolution::Unresolved
    } else {
        AssigneeResolution::Ambiguous {
            candidate_sys_ids: dedupe_strings(ambiguous_candidates),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PiiEmailMask {
    pub email_local_part: String,
    pub domain_hash: String,
}

impl PiiEmailMask {
    pub fn receipt_identity(&self) -> String {
        format!("{}@<sha256:{}>", self.email_local_part, self.domain_hash)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AssigneeWarningData {
    pub email_local_part: String,
    pub domain_hash: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub candidate_sys_ids: Vec<String>,
}

pub const ERROR_GUARD_FAILED: &str = "GUARD_FAILED";
pub const ERROR_FIELD_REJECTED: &str = "FIELD_REJECTED";
pub const ERROR_STATE_REQUIRED: &str = "STATE_REQUIRED";
pub const WARNING_STATE_VALUE_UNMAPPED: &str = "STATE_VALUE_UNMAPPED";
pub const WARNING_ASSIGNEE_DEFAULTED_FROM_CALLER: &str = "ASSIGNEE_DEFAULTED_FROM_CALLER";
pub const WARNING_ASSIGNEE_UNRESOLVED: &str = "ASSIGNEE_UNRESOLVED";
pub const WARNING_ASSIGNEE_AMBIGUOUS: &str = "ASSIGNEE_AMBIGUOUS";
pub const WARNING_PRIORITY_VALUE_UNMAPPED: &str = "PRIORITY_VALUE_UNMAPPED";

pub fn mask_email_for_receipt(email: &str) -> PiiEmailMask {
    let (local, domain) = email.split_once('@').unwrap_or((email, ""));
    let domain = domain.trim().to_ascii_lowercase();
    let digest = Sha256::digest(domain.as_bytes());

    PiiEmailMask {
        email_local_part: local.trim().to_string(),
        domain_hash: hex::encode(digest)[..8].to_string(),
    }
}

pub fn hash_email_for_audit(email: &str) -> String {
    let normalized = email.trim().to_ascii_lowercase();
    let digest = Sha256::digest(normalized.as_bytes());
    hex::encode(digest)
}

pub fn assignee_defaulted_warning_message(mask: &PiiEmailMask) -> String {
    format!(
        "Substituted caller identity ({}) for omitted assignee.",
        mask.receipt_identity()
    )
}

pub fn assignee_unresolved_warning_message(mask: Option<&PiiEmailMask>) -> String {
    match mask {
        Some(mask) => format!(
            "Caller identity ({}) did not resolve to a sys_user; field left blank.",
            mask.receipt_identity()
        ),
        None => "Caller identity did not resolve to a sys_user; field left blank.".to_string(),
    }
}

pub fn assignee_ambiguous_warning_data(
    email: Option<&str>,
    candidate_sys_ids: Vec<String>,
) -> Option<AssigneeWarningData> {
    email.map(|email| {
        let mask = mask_email_for_receipt(email);
        AssigneeWarningData {
            email_local_part: mask.email_local_part,
            domain_hash: mask.domain_hash,
            candidate_sys_ids: dedupe_strings(candidate_sys_ids),
        }
    })
}

fn non_terminal_state_choices(choices: &[StateChoice]) -> Vec<StateChoice> {
    choices
        .iter()
        .filter(|choice| !choice.terminal)
        .cloned()
        .collect()
}

fn reference_sys_id_from_json(value: &Value, field: &str) -> Option<String> {
    field_value_from_json(value, field).and_then(string_from_reference_value)
}

fn bool_from_json_field(value: &Value, field: &str) -> Option<bool> {
    field_value_from_json(value, field).and_then(parse_boolish_value)
}

fn field_value_from_json<'a>(value: &'a Value, field: &str) -> Option<&'a Value> {
    value
        .get(field)
        .or_else(|| value.get("fields").and_then(|fields| fields.get(field)))
        .or_else(|| {
            value
                .get("references")
                .and_then(|references| references.get(field))
        })
}

fn string_from_reference_value(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Object(object) => object
            .get("sys_id")
            .and_then(Value::as_str)
            .or_else(|| object.get("value").and_then(Value::as_str))
            .map(str::to_string),
        _ => None,
    }
}

fn parse_boolish_value(value: &Value) -> Option<bool> {
    match value {
        Value::Bool(value) => Some(*value),
        Value::String(value) => parse_boolish(value),
        Value::Number(value) => value.as_i64().map(|value| value != 0),
        Value::Object(object) => object
            .get("value")
            .and_then(parse_boolish_value)
            .or_else(|| object.get("display_value").and_then(parse_boolish_value)),
        _ => None,
    }
}

fn parse_boolish(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" | "y" => Some(true),
        "false" | "0" | "no" | "n" => Some(false),
        _ => None,
    }
}

fn dedupe_strings(values: Vec<String>) -> Vec<String> {
    let mut deduped = Vec::new();
    for value in values {
        if !deduped.iter().any(|seen| seen == &value) {
            deduped.push(value);
        }
    }
    deduped
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::planner::compute_op_hash;
    use serde_json::json;

    const GROUP: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
    const SPRINT: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    const STORY: &str = "cccccccccccccccccccccccccccccccc";
    const OTHER: &str = "dddddddddddddddddddddddddddddddd";
    const USER: &str = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";

    fn binding() -> BoardBinding {
        BoardBinding {
            name: "Training board".to_string(),
            instance_host: "example.service-now.com".to_string(),
            board_sys_id: "board-1".to_string(),
            story_table: "rm_story".to_string(),
            task_table: "rm_scrum_task".to_string(),
            column_field: "state".to_string(),
            swim_lane_field: "assignment_group".to_string(),
            assignment_group: GROUP.to_string(),
            allowed_sprints: vec![SPRINT.to_string()],
            allow_production: false,
            allowed_story_states: Vec::new(),
            allowed_task_states: Vec::new(),
            allowed_priorities: Vec::new(),
        }
    }

    fn in_scope_snapshot() -> InScopeSnapshot<'static> {
        InScopeSnapshot {
            active: true,
            assignment_group_sys_id: GROUP,
            sprint_sys_id: Some(SPRINT),
        }
    }

    #[test]
    fn story_in_scope_passes_on_configured_board() {
        assert_eq!(story_in_scope(&in_scope_snapshot(), &binding()), Ok(()));
    }

    #[test]
    fn story_in_scope_failure_clauses_are_structured() {
        let inactive = InScopeSnapshot {
            active: false,
            ..in_scope_snapshot()
        };
        assert_eq!(
            story_in_scope(&inactive, &binding()).unwrap_err().clause(),
            InScopeFailureClause::ActiveFalse
        );

        let wrong_group = InScopeSnapshot {
            assignment_group_sys_id: OTHER,
            ..in_scope_snapshot()
        };
        assert_eq!(
            story_in_scope(&wrong_group, &binding())
                .unwrap_err()
                .clause(),
            InScopeFailureClause::AssignmentGroupMismatch
        );

        let missing_sprint = InScopeSnapshot {
            sprint_sys_id: None,
            ..in_scope_snapshot()
        };
        assert_eq!(
            story_in_scope(&missing_sprint, &binding())
                .unwrap_err()
                .clause(),
            InScopeFailureClause::SprintOutOfAllowed
        );

        let wrong_sprint = InScopeSnapshot {
            sprint_sys_id: Some(OTHER),
            ..in_scope_snapshot()
        };
        let data = story_in_scope(&wrong_sprint, &binding())
            .unwrap_err()
            .to_guard_failed_data();
        assert_eq!(data.clause.as_str(), "sprint_out_of_allowed");
        assert_eq!(data.observed.as_deref(), Some(OTHER));
    }

    #[test]
    fn task_parent_in_scope_rejects_cross_story_task() {
        let failure = task_parent_in_scope(&in_scope_snapshot(), OTHER, STORY, &binding())
            .expect_err("cross-story parent must be rejected");
        assert_eq!(
            failure.clause(),
            InScopeFailureClause::TaskParentStoryMismatch
        );

        let parent_out_of_scope = InScopeSnapshot {
            assignment_group_sys_id: OTHER,
            ..in_scope_snapshot()
        };
        let failure = task_parent_in_scope(&parent_out_of_scope, STORY, STORY, &binding())
            .expect_err("out-of-scope parent story must be rejected");
        assert_eq!(failure.clause(), InScopeFailureClause::TaskParentOutOfScope);
    }

    #[test]
    fn snapshots_extract_from_json_shapes() {
        let value = json!({
            "active": {"value": "true"},
            "references": {
                "assignment_group": {"sys_id": GROUP},
                "sprint": {"sys_id": SPRINT}
            }
        });

        let snapshot = in_scope_snapshot_from_json(&value).expect("snapshot");
        assert_eq!(story_in_scope(&snapshot.as_snapshot(), &binding()), Ok(()));
    }

    #[test]
    fn op_hash_stable_across_story_payloads() {
        let payload = StoryPlannedChanges::StoryCreate(StoryCreatePayload {
            short_description: "Create access review story".to_string(),
            description: "Need board write coverage".to_string(),
            acceptance_criteria: Some("Tests pass".to_string()),
            priority: Some("3".to_string()),
            epic: None,
            story_points: Some(StoryPoints::Numeric(3)),
            assigned_to: Some(USER.to_string()),
        });
        let first = serde_json::to_value(&payload).expect("payload serializes");
        let second = serde_json::to_value(&payload).expect("payload serializes again");

        assert_eq!(
            compute_op_hash("story_plan_create", None, &first),
            compute_op_hash("story_plan_create", None, &second)
        );
    }

    fn choices() -> Vec<StateChoice> {
        vec![
            StateChoice::allowed("2", "Ready"),
            StateChoice::allowed("3", "Work in Progress"),
            StateChoice {
                value: "7".to_string(),
                label: "Closed".to_string(),
                terminal: true,
            },
        ]
    }

    #[test]
    fn state_resolves_from_cached_raw_and_label() {
        assert_eq!(
            resolve_state_from_cached_choices(
                StateResolutionContext {
                    supplied: Some("2"),
                    current_state_value: None,
                    current_state_terminal: false,
                },
                &choices()
            ),
            StateResolution::Resolved {
                value: "2".to_string()
            }
        );

        assert_eq!(
            resolve_state_from_cached_choices(
                StateResolutionContext {
                    supplied: Some("work in progress"),
                    current_state_value: None,
                    current_state_terminal: false,
                },
                &choices()
            ),
            StateResolution::Mapped {
                label: "Work in Progress".to_string(),
                value: "3".to_string(),
            }
        );
    }

    #[test]
    fn state_required_vs_field_rejected_discriminator() {
        let required = resolve_state_from_cached_choices(
            StateResolutionContext {
                supplied: Some("not-a-state"),
                current_state_value: Some("7"),
                current_state_terminal: true,
            },
            &choices(),
        );
        assert!(matches!(required, StateResolution::Required { .. }));

        let rejected = resolve_state_from_cached_choices(
            StateResolutionContext {
                supplied: Some("not-a-state"),
                current_state_value: Some("2"),
                current_state_terminal: false,
            },
            &choices(),
        );
        assert_eq!(
            rejected,
            StateResolution::FieldRejected {
                field: "state".to_string(),
                reason: FieldRejectionReason::ValueNotInEnum,
                original: "not-a-state".to_string(),
            }
        );

        assert_eq!(
            resolve_state_from_cached_choices(
                StateResolutionContext {
                    supplied: None,
                    current_state_value: Some("7"),
                    current_state_terminal: true,
                },
                &choices()
            ),
            StateResolution::Omitted
        );
    }

    #[derive(Default)]
    struct MockLookup {
        email_hits: Vec<UserLookupHit>,
        user_name_hits: Vec<UserLookupHit>,
    }

    #[async_trait::async_trait(?Send)]
    impl UserLookup for MockLookup {
        async fn find_active_by_email(&self, _email: &str) -> Vec<UserLookupHit> {
            self.email_hits.clone()
        }

        async fn find_active_by_user_name(&self, _user_name: &str) -> Vec<UserLookupHit> {
            self.user_name_hits.clone()
        }
    }

    fn actor() -> Actor {
        Actor {
            subject: "first.last".to_string(),
            email: Some("first.last@example.com".to_string()),
            display_name: Some("First Last".to_string()),
        }
    }

    #[tokio::test]
    async fn assignee_sys_id_caller_null_and_ambiguous_behaviors() {
        assert_eq!(
            resolve_assignee(Some(&json!(USER)), &actor(), &MockLookup::default()).await,
            AssigneeResolution::Supplied {
                sys_id: USER.to_string()
            }
        );

        let lookup = MockLookup {
            email_hits: vec![UserLookupHit {
                sys_id: USER.to_string(),
            }],
            user_name_hits: Vec::new(),
        };
        assert_eq!(
            resolve_assignee(Some(&json!("caller")), &actor(), &lookup).await,
            AssigneeResolution::DefaultedFromCaller {
                sys_id: USER.to_string(),
                source: ActorIdentitySource::EmailMatch,
            }
        );

        assert_eq!(
            resolve_assignee(Some(&Value::Null), &actor(), &MockLookup::default()).await,
            AssigneeResolution::ExplicitNull
        );

        let ambiguous = MockLookup {
            email_hits: vec![
                UserLookupHit {
                    sys_id: USER.to_string(),
                },
                UserLookupHit {
                    sys_id: OTHER.to_string(),
                },
            ],
            user_name_hits: vec![UserLookupHit {
                sys_id: STORY.to_string(),
            }],
        };
        assert_eq!(
            resolve_assignee(None, &actor(), &ambiguous).await,
            AssigneeResolution::DefaultedFromCaller {
                sys_id: STORY.to_string(),
                source: ActorIdentitySource::UserNameMatch,
            }
        );

        let ambiguous = MockLookup {
            email_hits: vec![
                UserLookupHit {
                    sys_id: USER.to_string(),
                },
                UserLookupHit {
                    sys_id: OTHER.to_string(),
                },
            ],
            user_name_hits: vec![
                UserLookupHit {
                    sys_id: STORY.to_string(),
                },
                UserLookupHit {
                    sys_id: SPRINT.to_string(),
                },
            ],
        };
        assert_eq!(
            resolve_assignee(None, &actor(), &ambiguous).await,
            AssigneeResolution::Ambiguous {
                candidate_sys_ids: vec![
                    USER.to_string(),
                    OTHER.to_string(),
                    STORY.to_string(),
                    SPRINT.to_string()
                ],
            }
        );
    }

    #[test]
    fn pii_warning_masks_domain_and_keeps_local_part() {
        let mask = mask_email_for_receipt("first.last@Example.COM");
        assert_eq!(mask.email_local_part, "first.last");
        assert_eq!(mask.domain_hash, "a379a6f6");
        assert_eq!(
            assignee_defaulted_warning_message(&mask),
            "Substituted caller identity (first.last@<sha256:a379a6f6>) for omitted assignee."
        );

        let audit_hash = hash_email_for_audit("first.last@Example.COM");
        assert!(!audit_hash.contains("first.last"));
        assert!(!audit_hash.contains("example.com"));
        assert_eq!(audit_hash.len(), 64);
    }
}
