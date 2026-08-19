use super::*;

pub(super) struct DaemonStores {
    pub(super) plan_store: SqlitePlanStore,
    pub(super) confirmation_store: SqliteConfirmationStore,
    pub(super) idempotency_store: SqliteIdempotencyStore,
    pub(super) audit_sink: SqliteAuditSink,
}

pub(super) fn stores(state: &DaemonState) -> Result<DaemonStores> {
    std::fs::create_dir_all(&state.data_dir)?;
    let path = story_store_path(state);
    Ok(DaemonStores {
        plan_store: SqlitePlanStore::open(&path)?,
        confirmation_store: SqliteConfirmationStore::open(&path)?,
        idempotency_store: SqliteIdempotencyStore::open(&path)?,
        audit_sink: SqliteAuditSink::open(&path)?,
    })
}

pub(super) fn story_store_path(state: &DaemonState) -> PathBuf {
    state.data_dir.join("mcp_story_write.sqlite3")
}

pub(super) fn board_binding(tool: &str, state: &DaemonState) -> Result<BoardBinding> {
    state
        .mcp_config
        .policy
        .validate_story_board_policy(tool, &state.mcp_config.environment)
        .map_err(|err| anyhow::anyhow!(err.to_string()))?
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("tool {tool} is not a governed Story board tool"))
}

pub(super) struct PlanInput {
    pub(super) builder: OperationPlanBuilder,
    pub(super) concurrency_token: Option<ConcurrencyToken>,
    pub(super) warnings: Vec<ReceiptWarning>,
}

#[derive(Debug)]
pub(super) enum PlanBuildError {
    Invalid(String),
    FieldRejected(Vec<Value>),
    GuardFailed(InScopeFailure),
    StateRequired(Value),
    NotFound(String),
    Upstream(anyhow::Error),
}

pub(super) async fn build_plan_input(
    tool: &str,
    args: &Map<String, Value>,
    binding: &BoardBinding,
    state: &DaemonState,
    actor: &StoryActor,
) -> std::result::Result<PlanInput, PlanBuildError> {
    let mut payload = Value::Object(args.clone());
    strip_non_writable_selector_fields(tool, &mut payload);
    let mut warnings = resolve_assignee_in_payload(tool, &mut payload, actor, state).await?;
    match tool {
        "story_plan_create" => {
            require_string(args, "short_description")?;
            require_string(args, "description")?;
            default_story_create_reference_fields(&mut payload, binding)?;
            default_story_create_backlog_type(&mut payload, &state.mcp_config.policy);
            warnings.extend(default_story_owner_from_actor(&mut payload, actor, state).await?);
            warnings.extend(warn_missing_story_create_optional_required_fields(&payload));
            inject(&mut payload, "active", true);
            warnings.extend(
                validate_constrained_fields(tool, &mut payload, binding, state, None).await?,
            );
            enforce_story_payload_scope(&payload, binding)?;
            Ok(PlanInput {
                builder: OperationPlanBuilder::new(tool).planned_changes(payload),
                concurrency_token: None,
                warnings,
            })
        }
        "story_task_plan_create" => {
            require_string(args, "parent_story_number")?;
            require_string(args, "short_description")?;
            let parent_number = args
                .get("parent_story_number")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let parent = state
                .core
                .get_record_fresh(parent_number)
                .await
                .map_err(PlanBuildError::Upstream)?
                .ok_or_else(|| {
                    PlanBuildError::NotFound(format!("parent story {parent_number} not found"))
                })?;
            enforce_story_record_scope(&parent, binding)?;
            let assignment_group =
                resolve_story_task_create_assignment_group(args, &parent, binding)?;
            inject(&mut payload, "story", parent.sys_id.clone());
            inject(&mut payload, "assignment_group", assignment_group);
            warnings.extend(
                validate_constrained_fields(tool, &mut payload, binding, state, None).await?,
            );
            enforce_task_parent_payload_scope(&payload, &parent, binding)?;
            Ok(PlanInput {
                builder: OperationPlanBuilder::new(tool)
                    .target(record_ref_from_snow(&parent))
                    .planned_changes(payload),
                concurrency_token: None,
                warnings,
            })
        }
        "story_plan_update" | "story_task_plan_update" => {
            require_string(args, "number")?;
            let number = args
                .get("number")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_string();
            let record = state
                .core
                .get_record_fresh(&number)
                .await
                .map_err(PlanBuildError::Upstream)?
                .ok_or_else(|| PlanBuildError::NotFound(format!("record {number} not found")))?;
            if tool == "story_plan_update" {
                enforce_story_record_scope(&record, binding)?;
            } else {
                enforce_task_record_scope(&record, binding)?;
            }
            warnings.extend(
                validate_constrained_fields(tool, &mut payload, binding, state, Some(&record))
                    .await?,
            );
            let concurrency_token = concurrency_from_record(&record);
            Ok(PlanInput {
                builder: OperationPlanBuilder::new(tool)
                    .target(record_ref_from_snow(&record))
                    .planned_changes(payload),
                concurrency_token,
                warnings,
            })
        }
        _ => Err(PlanBuildError::Invalid(format!(
            "unsupported Story plan tool {tool}"
        ))),
    }
}

pub(super) fn default_story_create_reference_fields(
    payload: &mut Value,
    binding: &BoardBinding,
) -> std::result::Result<(), PlanBuildError> {
    let Some(object) = payload.as_object_mut() else {
        return Ok(());
    };

    match object.get("assignment_group") {
        Some(supplied) => {
            if reference_sys_id_from_value(supplied).is_none() {
                return Err(field_rejected(vec![json!({
                    "field": "assignment_group",
                    "reason": "type_mismatch",
                })]));
            }
        }
        None => {
            object.insert(
                "assignment_group".to_string(),
                json!(binding.assignment_group.clone()),
            );
        }
    }

    match object.get("sprint") {
        Some(Value::String(value)) if value.trim().is_empty() => {
            object.remove("sprint");
        }
        Some(supplied) => {
            let Some(sprint) = reference_sys_id_from_value(supplied) else {
                return Err(field_rejected(vec![json!({
                    "field": "sprint",
                    "reason": "type_mismatch",
                })]));
            };
            if !is_sys_id(&sprint) {
                return Err(field_rejected(vec![json!({
                    "field": "sprint",
                    "reason": "value_not_in_enum",
                })]));
            }
        }
        None => {}
    }

    Ok(())
}

pub(super) fn default_story_create_backlog_type(
    payload: &mut Value,
    policy: &snow_mcp::domain::policy::PolicyConfig,
) {
    if !story_create_is_backlog_payload(payload)
        || !story_create_apply_field_allowed(policy, STORY_BACKLOG_TYPE_FIELD)
    {
        return;
    }

    if let Value::Object(object) = payload {
        object
            .entry(STORY_BACKLOG_TYPE_FIELD.to_string())
            .or_insert_with(|| json!(STORY_BACKLOG_TYPE_PRODUCT));
    }
}

pub(super) fn story_create_apply_field_allowed(
    policy: &snow_mcp::domain::policy::PolicyConfig,
    field: &str,
) -> bool {
    field_allowlist_for_apply_tool(policy, "story_apply_create").contains(field)
}

pub(super) fn story_create_is_backlog_payload(payload: &Value) -> bool {
    let Some(object) = payload.as_object() else {
        return false;
    };
    !object.contains_key("sprint") && !object.contains_key("epic")
}

pub(super) fn validate_story_create_backlog_type(payload: &mut Value, fields: &mut Vec<Value>) {
    let Some(value) = payload.get(STORY_BACKLOG_TYPE_FIELD) else {
        return;
    };

    match value.as_str().map(str::trim) {
        Some(value) if value.eq_ignore_ascii_case(STORY_BACKLOG_TYPE_PRODUCT) => {
            inject(
                payload,
                STORY_BACKLOG_TYPE_FIELD,
                STORY_BACKLOG_TYPE_PRODUCT,
            );
        }
        Some(_) => fields.push(json!({
            "field": STORY_BACKLOG_TYPE_FIELD,
            "reason": "value_not_in_enum",
        })),
        None => fields.push(json!({
            "field": STORY_BACKLOG_TYPE_FIELD,
            "reason": "type_mismatch",
        })),
    }
}

pub(super) fn story_create_payload_in_scope(
    payload: &Value,
    binding: &BoardBinding,
) -> std::result::Result<(), InScopeFailure> {
    let snapshot = in_scope_snapshot_from_json(payload).ok_or_else(|| {
        InScopeFailure::WrongAssignmentGroup {
            expected: binding.assignment_group.clone(),
            observed: String::new(),
        }
    })?;

    if snapshot.sprint_sys_id.is_some() {
        return story_in_scope(&snapshot.as_snapshot(), binding);
    }

    if !snapshot.active {
        return Err(InScopeFailure::NotActive);
    }
    if !equal_sys_id(&snapshot.assignment_group_sys_id, &binding.assignment_group) {
        return Err(InScopeFailure::WrongAssignmentGroup {
            expected: binding.assignment_group.clone(),
            observed: snapshot.assignment_group_sys_id,
        });
    }

    Ok(())
}

pub(super) fn warn_missing_story_create_optional_required_fields(
    payload: &Value,
) -> Vec<ReceiptWarning> {
    if reference_sys_id_from_json(payload, "cmdb_ci").is_some() {
        return Vec::new();
    }

    vec![receipt_warning(
        WARNING_MISSING_OPTIONAL_REQUIRED_FIELD,
        Some("cmdb_ci"),
        "Configuration Item was not supplied; ServiceNow may reject the Story create.",
        Some(json!({ "field": "cmdb_ci" })),
    )]
}

pub(super) fn story_scope_failure_field_rejections(failure: &InScopeFailure) -> Option<Vec<Value>> {
    let field = match failure {
        InScopeFailure::WrongAssignmentGroup { expected, observed } => json!({
            "field": "assignment_group",
            "reason": "not_in_allowlist",
            "expected": expected,
            "observed": observed,
        }),
        InScopeFailure::MissingSprint => json!({
            "field": "sprint",
            "reason": "missing_no_default",
            "remediation": "Either populate allowed_sprints in the board binding, or supply sprint=<sys_id> in the plan input",
        }),
        InScopeFailure::SprintNotAllowed { observed, allowed } => json!({
            "field": "sprint",
            "reason": "not_in_allowlist",
            "allowed": allowed,
            "observed": observed,
        }),
        InScopeFailure::NotActive => json!({
            "field": "active",
            "reason": "active_false",
        }),
        InScopeFailure::TaskParentOutOfScope { cause } => {
            return story_scope_failure_field_rejections(cause);
        }
        InScopeFailure::TaskParentStoryMismatch { .. } => return None,
    };
    Some(vec![field])
}

pub(super) fn strip_non_writable_selector_fields(tool: &str, payload: &mut Value) {
    let Some(object) = payload.as_object_mut() else {
        return;
    };
    for field in metadata_fields() {
        object.remove(*field);
    }
    match tool {
        "story_plan_update" | "story_task_plan_update" => {
            object.remove("number");
        }
        "story_task_plan_create" => {
            object.remove("parent_story_number");
        }
        _ => {}
    }
}

pub(super) fn reference_sys_id_from_json(value: &Value, field: &str) -> Option<String> {
    value.get(field).and_then(reference_sys_id_from_value)
}

pub(super) fn reference_sys_id_from_value(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.trim().to_string()).filter(|value| !value.is_empty()),
        Value::Object(object) => object
            .get("sys_id")
            .and_then(Value::as_str)
            .or_else(|| object.get("value").and_then(Value::as_str))
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned),
        _ => None,
    }
}

pub(super) fn object_args(params: &Value) -> Option<&Map<String, Value>> {
    params.as_object()
}

pub(super) fn string_param(params: &Value, field: &str) -> Option<String> {
    params
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

pub(super) fn actor_from_params(params: &Value, state: &DaemonState) -> String {
    params
        .get("actor")
        .and_then(|value| {
            value.as_str().map(ToOwned::to_owned).or_else(|| {
                value
                    .get("subject")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            })
        })
        .unwrap_or_else(|| state.core.config().instance.user.clone())
}

pub(super) fn requester_from_params(params: &Value, actor: &str) -> String {
    params
        .get("requester")
        .and_then(|value| {
            value.as_str().map(ToOwned::to_owned).or_else(|| {
                value
                    .get("subject")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
            })
        })
        .unwrap_or_else(|| actor.to_string())
}

pub(super) fn apply_tool_for_plan_tool(tool: &str) -> &str {
    match tool {
        "story_plan_create" => "story_apply_create",
        "story_plan_update" => "story_apply_update",
        "story_task_plan_create" => "story_task_apply_create",
        "story_task_plan_update" => "story_task_apply_update",
        other => other,
    }
}

pub(super) async fn handle_plan_get_impl(
    id: Option<Value>,
    params: &Value,
    state: &DaemonState,
) -> JsonRpcResponse {
    let Some(plan_id) = string_param(params, "plan_id") else {
        return story_error(
            id,
            -32051,
            "FIELD_REJECTED",
            json!({ "code": "FIELD_REJECTED", "fields": [{"field": "plan_id", "reason": "type_mismatch"}] }),
        );
    };

    match stores(state).map(|stores| (stores.plan_store, plan_id)) {
        Ok((plan_store, plan_id)) => match plan_store.get(&plan_id).await {
            Ok(Some(record)) => JsonRpcResponse::ok(id, json!({ "plan": record })),
            Ok(None) => story_error(
                id,
                -32056,
                "PLAN_NOT_FOUND",
                json!({
                    "code": "PLAN_NOT_FOUND",
                    "plan_id": plan_id,
                }),
            ),
            Err(err) => internal_error(id, err),
        },
        Err(err) => internal_error(id, err),
    }
}

pub(super) async fn handle_story_plan_impl(
    id: Option<Value>,
    tool: &str,
    params: &Value,
    state: &DaemonState,
) -> JsonRpcResponse {
    let binding = match board_binding(tool, state) {
        Ok(binding) => binding,
        Err(_) => {
            return story_error(
                id,
                -32051,
                "FIELD_REJECTED",
                json!({
                    "code": "FIELD_REJECTED",
                    "fields": [{"field": "story_board_id", "reason": "blocked_deny_list"}],
                }),
            );
        }
    };

    let args = match object_args(params) {
        Some(args) => args,
        None => {
            return story_error(
                id,
                -32051,
                "FIELD_REJECTED",
                json!({ "code": "FIELD_REJECTED", "fields": [{"field": "payload", "reason": "type_mismatch"}] }),
            );
        }
    };
    if !state.mcp_config.policy.is_tool_enabled(tool) {
        return story_error(
            id,
            -32051,
            "FIELD_REJECTED",
            json!({ "code": "FIELD_REJECTED", "fields": [{"field": "tool", "reason": "blocked_deny_list"}] }),
        );
    }
    if let Some(response) = reject_plan_input_fields(id.clone(), tool, args, state) {
        return response;
    }

    let story_actor = story_actor_from_params(params, state);
    if let Some(retry_after_seconds) = check_and_record_rate_limit(
        "story_plan",
        &story_actor.subject,
        tool,
        state.mcp_config.rate_limit.read_per_minute,
    ) {
        return audited_story_error(
            state,
            id,
            tool,
            -32058,
            "RATE_LIMITED",
            json!({ "code": "RATE_LIMITED", "retry_after_seconds": retry_after_seconds }),
            ResultStatus::Denied,
        )
        .await;
    }
    let plan_input = match build_plan_input(tool, args, &binding, state, &story_actor).await {
        Ok(input) => input,
        Err(PlanBuildError::Invalid(message)) => {
            return story_error(
                id,
                -32051,
                "FIELD_REJECTED",
                json!({ "code": "FIELD_REJECTED", "fields": [{"field": message, "reason": "type_mismatch"}] }),
            );
        }
        Err(PlanBuildError::FieldRejected(fields)) => {
            return story_error(
                id,
                -32051,
                "FIELD_REJECTED",
                json!({ "code": "FIELD_REJECTED", "fields": fields }),
            );
        }
        Err(PlanBuildError::GuardFailed(failure)) => {
            if let Some(fields) = story_scope_failure_field_rejections(&failure) {
                return story_error(
                    id,
                    -32051,
                    "FIELD_REJECTED",
                    json!({ "code": "FIELD_REJECTED", "fields": fields }),
                );
            }
            let data = match serde_json::to_value(failure.to_guard_failed_data()) {
                Ok(data) => data,
                Err(err) => return internal_error(id, err),
            };
            return story_error(id, -32050, "GUARD_FAILED", data);
        }
        Err(PlanBuildError::StateRequired(data)) => {
            return story_error(id, -32062, "STATE_REQUIRED", data);
        }
        Err(PlanBuildError::NotFound(message)) => {
            return story_error(
                id,
                -32004,
                "record not found",
                json!({ "details": message }),
            );
        }
        Err(PlanBuildError::Upstream(err)) => return internal_error(id, err),
    };

    let plan = plan_input.builder.build();
    let now = Utc::now();
    let expires_at = now + chrono::Duration::seconds(PLAN_TTL_SECONDS);
    let actor = story_actor.subject.clone();
    let requester = requester_from_params(params, &actor);
    let apply_tool = apply_tool_for_plan_tool(tool);
    let environment = state.mcp_config.environment.label.clone();
    let idempotency_key = Uuid::new_v4().to_string();

    let mut plan_json = match serde_json::to_value(&plan) {
        Ok(value) => value,
        Err(err) => return internal_error(id, err),
    };
    let receipt_warnings = receipt_warnings_for_caller(&plan_input.warnings);
    let audit_warning_email_hashes = audit_email_hashes_from_warnings(&plan_input.warnings);
    if let Value::Object(object) = &mut plan_json {
        object.insert(
            "apply_tool".to_string(),
            Value::String(apply_tool.to_string()),
        );
        object.insert(
            "receipt_warnings".to_string(),
            Value::Array(
                receipt_warnings
                    .iter()
                    .filter_map(|warning| serde_json::to_value(warning).ok())
                    .collect(),
            ),
        );
        object.insert(
            "audit_warning_email_sha256".to_string(),
            Value::Array(
                audit_warning_email_hashes
                    .iter()
                    .cloned()
                    .map(Value::String)
                    .collect(),
            ),
        );
    }

    let stores = match stores(state) {
        Ok(stores) => stores,
        Err(err) => return internal_error(id, err),
    };
    let key = IdempotencyKey {
        value: idempotency_key.clone(),
        source: IdempotencyKeySource::ServerDerived,
    };
    match stores
        .idempotency_store
        .check_and_record(
            &key,
            apply_tool,
            &plan.op_hash,
            state.mcp_config.policy.idempotency_window_seconds,
        )
        .await
    {
        Ok(IdempotencyOutcome::NewKey) => {}
        Ok(IdempotencyOutcome::Conflict { .. }) => {
            return story_error(
                id,
                -32052,
                "IDEMPOTENCY_CONFLICT",
                json!({
                    "code": "IDEMPOTENCY_CONFLICT",
                    "idempotency_key": idempotency_key,
                    "bound_op_hash": "",
                    "attempted_op_hash": plan.op_hash,
                }),
            );
        }
        Ok(IdempotencyOutcome::Replay(_)) | Ok(IdempotencyOutcome::Pending { .. }) => {
            return internal_error(id, "server-issued idempotency key was not unique");
        }
        Err(err) => return internal_error(id, err),
    }
    let record = PlanStoreRecord {
        plan_id: plan.plan_id.clone(),
        tool: plan.tool.clone(),
        actor: actor.clone(),
        op_hash: plan.op_hash.clone(),
        plan_json,
        concurrency_token: plan_input.concurrency_token.clone(),
        created_at: now,
        expires_at,
        state: PlanLifecycleState::Pending,
    };
    if let Err(err) = stores.plan_store.put(record).await {
        return internal_error(id, err);
    }
    if let Err(err) = append_audit_event(
        state,
        &plan.plan_id,
        None,
        tool,
        ResultStatus::Plan,
        Some(json!({ "op_hash": plan.op_hash })),
        None,
        None,
        Some((&actor, &requester)),
        None,
    )
    .await
    {
        return internal_error(id, err);
    }

    let binding = ConfirmationBinding {
        actor: actor.clone(),
        requester: requester.clone(),
        tool: apply_tool.to_string(),
        op_hash: plan.op_hash.clone(),
        environment,
    };
    let confirmation = match stores
        .confirmation_store
        .issue(&plan.plan_id, binding, PLAN_TTL_SECONDS as u64)
        .await
    {
        Ok(confirmation) => confirmation,
        Err(err) => return internal_error(id, err),
    };
    if let Err(err) = append_audit_event(
        state,
        &Uuid::new_v4().to_string(),
        Some(plan.plan_id.as_str()),
        "confirmation_issue",
        ResultStatus::Plan,
        None,
        None,
        None,
        Some((&actor, &requester)),
        None,
    )
    .await
    {
        return internal_error(id, err);
    }

    let mut result = json!({
        "plan_id": plan.plan_id,
        "op_hash": plan.op_hash,
        "preview": plan.planned_changes,
        "expires_at": expires_at.to_rfc3339(),
        "confirmation_token": confirmation.token_id,
        "idempotency_key": idempotency_key,
    });
    if let Some(concurrency_token) = plan_input.concurrency_token {
        result["concurrency_token"] = json!(concurrency_token);
    }

    JsonRpcResponse::ok(id, result)
}
