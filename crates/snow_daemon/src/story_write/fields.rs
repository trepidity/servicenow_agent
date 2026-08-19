use super::*;

pub(super) async fn validate_constrained_fields(
    tool: &str,
    payload: &mut Value,
    binding: &BoardBinding,
    state: &DaemonState,
    current_record: Option<&SnowRecord>,
) -> std::result::Result<Vec<ReceiptWarning>, PlanBuildError> {
    let mut fields = Vec::new();
    let mut warnings = Vec::new();
    let table = table_for_story_tool(tool, binding);
    if matches!(tool, "story_plan_create" | "story_apply_create") {
        validate_story_create_backlog_type(payload, &mut fields);
    }
    if let Some(priority) = payload.get("priority").and_then(Value::as_str) {
        let priority_lower = priority.to_ascii_lowercase();
        if priority_lower.contains("cancel") {
            fields.push(json!({
                "field": "priority",
                "reason": "value_constrained",
            }));
        } else {
            match normalize_choice_value(
                priority,
                &binding.allowed_priorities,
                state,
                table,
                "priority",
                true,
            )
            .await?
            {
                ChoiceValidation::Matched(value) => {
                    if let Some(object) = payload.as_object_mut() {
                        object.insert("priority".to_string(), Value::String(value));
                    }
                }
                ChoiceValidation::Constrained => {
                    fields.push(json!({
                        "field": "priority",
                        "reason": "value_constrained",
                    }));
                }
                ChoiceValidation::Unmapped => warnings.push(receipt_warning(
                    WARNING_PRIORITY_VALUE_UNMAPPED,
                    Some("priority"),
                    "Value not found in cached or dictionary-derived priority allowlist; passed to ServiceNow without translation.",
                    None,
                )),
            }
        }
    }

    if let Some(supplied_state) = payload.get("state").and_then(Value::as_str) {
        let allowed = if tool.contains("task") {
            &binding.allowed_task_states
        } else {
            &binding.allowed_story_states
        };
        let choices = state_choices_for_validation(allowed, state, table).await?;
        let current_state = current_record.and_then(record_state_value);
        let resolution = resolve_state_from_cached_choices(
            StateResolutionContext {
                supplied: Some(supplied_state),
                current_state_value: current_state.as_deref(),
                current_state_terminal: current_state
                    .as_deref()
                    .is_some_and(|value| choice_is_terminal(value, &choices)),
            },
            &choices,
        );
        match resolution {
            StateResolution::Resolved { value } | StateResolution::Mapped { value, .. } => {
                if let Some(object) = payload.as_object_mut() {
                    object.insert("state".to_string(), Value::String(value));
                }
            }
            StateResolution::Required {
                candidates,
                operator_note,
            } => {
                return Err(PlanBuildError::StateRequired(json!({
                    "code": "STATE_REQUIRED",
                    "field": "state",
                    "candidates": candidates,
                    "operator_note": operator_note,
                })));
            }
            StateResolution::FieldRejected {
                field,
                reason,
                original: _,
            } => {
                fields.push(json!({
                    "field": field,
                    "reason": reason.as_str(),
                }));
            }
            StateResolution::Omitted => {}
        }
    }

    if let Some(epic) = payload.get("epic").and_then(Value::as_str)
        && (!is_sys_id(epic) || !epic_exists(state, epic).await?)
    {
        fields.push(json!({
            "field": "epic",
            "reason": "value_not_in_enum",
        }));
    }

    if fields.is_empty() {
        Ok(warnings)
    } else {
        Err(field_rejected(fields))
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum ChoiceValidation {
    Matched(String),
    Constrained,
    Unmapped,
}

pub(super) async fn normalize_choice_value(
    supplied: &str,
    cached_allowed: &[String],
    state: &DaemonState,
    table: &str,
    field: &str,
    reject_cancel_values: bool,
) -> std::result::Result<ChoiceValidation, PlanBuildError> {
    if !cached_allowed.is_empty() {
        let matched = cached_allowed
            .iter()
            .find(|allowed| allowed.eq_ignore_ascii_case(supplied));
        return match matched {
            Some(allowed)
                if reject_cancel_values && allowed.to_ascii_lowercase().contains("cancel") =>
            {
                Ok(ChoiceValidation::Constrained)
            }
            Some(allowed)
                if reject_cancel_values
                    && dictionary_choice_is_constrained(state, table, field, supplied, allowed)
                        .await? =>
            {
                Ok(ChoiceValidation::Constrained)
            }
            Some(allowed) => Ok(ChoiceValidation::Matched(allowed.clone())),
            None => Ok(ChoiceValidation::Unmapped),
        };
    }

    let choices = state
        .core
        .field_choices(table, field)
        .await
        .map_err(PlanBuildError::Upstream)?;
    Ok(match_choice_validation(
        supplied,
        choices,
        reject_cancel_values,
    ))
}

pub(super) fn match_choice_validation(
    supplied: &str,
    choices: Vec<FieldChoice>,
    reject_cancel_values: bool,
) -> ChoiceValidation {
    for choice in choices {
        let cancel_choice = reject_cancel_values
            && (choice.label.to_ascii_lowercase().contains("cancel")
                || choice.value.to_ascii_lowercase().contains("cancel"));
        if choice.value.eq_ignore_ascii_case(supplied)
            || choice.label.eq_ignore_ascii_case(supplied)
        {
            return if cancel_choice {
                ChoiceValidation::Constrained
            } else {
                ChoiceValidation::Matched(choice.value)
            };
        }
    }
    ChoiceValidation::Unmapped
}

pub(super) async fn dictionary_choice_is_constrained(
    state: &DaemonState,
    table: &str,
    field: &str,
    supplied: &str,
    allowed: &str,
) -> std::result::Result<bool, PlanBuildError> {
    let choices = state
        .core
        .field_choices(table, field)
        .await
        .map_err(PlanBuildError::Upstream)?;
    Ok(choices.into_iter().any(|choice| {
        let matches_supplied = choice.value.eq_ignore_ascii_case(supplied)
            || choice.label.eq_ignore_ascii_case(supplied)
            || choice.value.eq_ignore_ascii_case(allowed)
            || choice.label.eq_ignore_ascii_case(allowed);
        matches_supplied
            && (choice.label.to_ascii_lowercase().contains("cancel")
                || choice.value.to_ascii_lowercase().contains("cancel"))
    }))
}

pub(super) async fn state_choices_for_validation(
    cached_allowed: &[String],
    state: &DaemonState,
    table: &str,
) -> std::result::Result<Vec<StateChoice>, PlanBuildError> {
    if !cached_allowed.is_empty() {
        return Ok(cached_allowed
            .iter()
            .cloned()
            .map(|spec| {
                let mut choice = StateChoice::allowed_spec(spec);
                choice.terminal =
                    is_terminal_choice(&choice.value) || is_terminal_choice(&choice.label);
                choice
            })
            .collect());
    }

    Ok(state
        .core
        .field_choices(table, "state")
        .await
        .map_err(PlanBuildError::Upstream)?
        .into_iter()
        .map(|choice| StateChoice {
            terminal: choice.terminal
                || is_terminal_choice(&choice.value)
                || is_terminal_choice(&choice.label),
            value: choice.value,
            label: choice.label,
        })
        .collect())
}

pub(super) fn choice_is_terminal(value: &str, choices: &[StateChoice]) -> bool {
    choices
        .iter()
        .any(|choice| choice.terminal && choice.value.eq_ignore_ascii_case(value))
}

pub(super) fn is_terminal_choice(value: &str) -> bool {
    let value = value.to_ascii_lowercase();
    [
        "closed",
        "complete",
        "completed",
        "cancel",
        "cancelled",
        "resolved",
    ]
    .iter()
    .any(|terminal| value.contains(terminal))
}

pub(super) fn record_state_value(record: &SnowRecord) -> Option<String> {
    record
        .fields
        .get("state")
        .map(|field| field.value.clone())
        .filter(|value| !value.trim().is_empty())
        .or_else(|| (!record.state.trim().is_empty()).then(|| record.state.clone()))
}

pub(super) fn field_rejected(fields: Vec<Value>) -> PlanBuildError {
    PlanBuildError::FieldRejected(fields)
}

pub(super) async fn enforce_apply_field_governance(
    tool: &str,
    plan: &OperationPlan,
    binding: &BoardBinding,
    state: &DaemonState,
    current_record: Option<&SnowRecord>,
) -> std::result::Result<(), PlanBuildError> {
    let payload = plan
        .planned_changes
        .as_object()
        .ok_or_else(|| PlanBuildError::Invalid("planned_changes".to_string()))?;
    let fields = field_governance_rejections(
        tool,
        payload,
        &state.mcp_config.policy,
        FieldGovernanceMode::WritePayload,
    );
    if !fields.is_empty() {
        return Err(field_rejected(fields));
    }

    let mut validation_payload = plan.planned_changes.clone();
    validate_constrained_fields(
        tool,
        &mut validation_payload,
        binding,
        state,
        current_record,
    )
    .await?;
    Ok(())
}

#[cfg(test)]
pub(super) fn reject_blocked_fields(
    id: Option<Value>,
    tool: &str,
    args: &Map<String, Value>,
) -> Option<JsonRpcResponse> {
    story_field_rejected_response(id, blocked_field_rejections(tool, args))
}

pub(super) fn reject_plan_input_fields(
    id: Option<Value>,
    tool: &str,
    args: &Map<String, Value>,
    state: &DaemonState,
) -> Option<JsonRpcResponse> {
    let fields = field_governance_rejections(
        tool,
        args,
        &state.mcp_config.policy,
        FieldGovernanceMode::PlanInput,
    );
    story_field_rejected_response(id, fields)
}

pub(super) fn story_field_rejected_response(
    id: Option<Value>,
    fields: Vec<Value>,
) -> Option<JsonRpcResponse> {
    if fields.is_empty() {
        None
    } else {
        Some(story_error(
            id,
            -32051,
            "FIELD_REJECTED",
            json!({ "code": "FIELD_REJECTED", "fields": fields }),
        ))
    }
}

#[cfg(test)]
pub(super) fn blocked_field_rejections(tool: &str, args: &Map<String, Value>) -> Vec<Value> {
    let blocked = blocked_fields_for_tool(tool);
    args.keys()
        .filter(|field| blocked.contains(field.as_str()))
        .map(|field| {
            json!({
                "field": field,
                "reason": "blocked_deny_list",
            })
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FieldGovernanceMode {
    PlanInput,
    WritePayload,
}

pub(super) fn field_governance_rejections(
    tool: &str,
    args: &Map<String, Value>,
    policy: &snow_mcp::domain::policy::PolicyConfig,
    mode: FieldGovernanceMode,
) -> Vec<Value> {
    let blocked = blocked_fields_for_tool(tool);
    let mut fields = Vec::new();
    let mut rejected = BTreeSet::new();

    for field in args.keys() {
        if field_governance_allows_without_policy(tool, field, mode) {
            continue;
        }
        if blocked.contains(field.as_str()) {
            rejected.insert(field.clone());
            fields.push(json!({
                "field": field,
                "reason": "blocked_deny_list",
            }));
        }
    }

    let apply_tool = apply_tool_for_plan_tool(tool);
    let allowlist = field_allowlist_for_apply_tool(policy, apply_tool);
    for field in args.keys() {
        if rejected.contains(field) {
            continue;
        }
        if metadata_fields().contains(&field.as_str()) {
            continue;
        }
        if field_governance_allows_without_policy(tool, field, mode) {
            continue;
        }
        if mode == FieldGovernanceMode::PlanInput
            && plan_selector_fields(tool).contains(&field.as_str())
        {
            continue;
        }
        if !allowlist.contains(field) {
            fields.push(json!({
                "field": field,
                "reason": "not_in_allowlist",
            }));
        }
    }

    fields
}

pub(super) fn field_governance_allows_without_policy(
    tool: &str,
    field: &str,
    mode: FieldGovernanceMode,
) -> bool {
    matches!(
        (mode, tool, field),
        (
            FieldGovernanceMode::PlanInput,
            "story_task_plan_create",
            "assignment_group"
        ) | (
            FieldGovernanceMode::WritePayload,
            "story_task_apply_create",
            "assignment_group"
        ) | (
            FieldGovernanceMode::WritePayload,
            "story_task_apply_create",
            "story"
        ) | (
            FieldGovernanceMode::WritePayload,
            "story_apply_create",
            "active"
        )
    )
}

pub(super) fn field_allowlist_for_apply_tool(
    policy: &snow_mcp::domain::policy::PolicyConfig,
    apply_tool: &str,
) -> BTreeSet<String> {
    if let Some(tool_policy) = policy.tools.get(apply_tool) {
        return tool_policy.field_allowlist.clone();
    }

    snow_mcp::domain::policy::PolicyConfig::read_only_default()
        .tools
        .get(apply_tool)
        .map(|tool_policy| tool_policy.field_allowlist.clone())
        .unwrap_or_default()
}

pub(super) fn metadata_fields() -> &'static [&'static str] {
    &["actor", "requester", "client", "session_id", "user_agent"]
}

pub(super) fn plan_selector_fields(tool: &str) -> &'static [&'static str] {
    match tool {
        "story_plan_update" | "story_task_plan_update" => &["number"],
        "story_task_plan_create" => &["parent_story_number"],
        _ => &[],
    }
}

pub(super) fn blocked_fields_for_tool(tool: &str) -> BTreeSet<&'static str> {
    let mut blocked = blocked_system_fields_for_story_tool(tool);
    if !story_tool_accepts_form_fields(tool) {
        blocked.extend([
            "parent",
            "cmdb_ci",
            "release_scrum",
            "team",
            "vendor",
            "assignment_group",
            "sprint",
            "story",
        ]);
    }
    if !matches!(tool, "story_plan_update" | "story_task_plan_update") {
        blocked.insert("number");
    }
    blocked
}

pub(super) fn blocked_system_fields_for_story_tool(tool: &str) -> BTreeSet<&'static str> {
    let mut blocked = BTreeSet::from([
        "sys_id",
        "sys_class_name",
        "sys_created_on",
        "sys_created_by",
        "sys_updated_on",
        "sys_updated_by",
        "sys_mod_count",
        "approval",
        "opened_by",
        "closed_by",
        "active",
    ]);
    if !matches!(tool, "story_plan_update" | "story_task_plan_update") {
        blocked.insert("number");
    }
    blocked
}

pub(super) fn story_tool_accepts_form_fields(tool: &str) -> bool {
    matches!(
        tool,
        "story_plan_create" | "story_plan_update" | "story_apply_create" | "story_apply_update"
    )
}

pub(super) fn require_string(
    args: &Map<String, Value>,
    field: &str,
) -> std::result::Result<(), PlanBuildError> {
    match args.get(field).and_then(Value::as_str) {
        Some(value) if !value.trim().is_empty() => Ok(()),
        _ => Err(PlanBuildError::Invalid(field.to_string())),
    }
}

pub(super) fn inject<T: serde::Serialize>(payload: &mut Value, field: &str, value: T) {
    if let Value::Object(object) = payload {
        object.insert(field.to_string(), json!(value));
    }
}
