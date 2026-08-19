use super::*;

pub(super) async fn apply_plan(
    tool: &str,
    plan: &OperationPlan,
    state: &DaemonState,
) -> Result<snow_core::StoryWriteResult> {
    match tool {
        "story_apply_create" => {
            state
                .core
                .create_rm_story(plan.planned_changes.clone())
                .await
        }
        "story_task_apply_create" => {
            state
                .core
                .create_rm_scrum_task(plan.planned_changes.clone())
                .await
        }
        "story_apply_update" => {
            let target = plan
                .target
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("update plan missing target"))?;
            state
                .core
                .update_rm_story(&target.sys_id, plan.planned_changes.clone())
                .await
        }
        "story_task_apply_update" => {
            let target = plan
                .target
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("task update plan missing target"))?;
            state
                .core
                .update_rm_scrum_task(&target.sys_id, plan.planned_changes.clone())
                .await
        }
        _ => Err(anyhow::anyhow!("unsupported Story apply tool {tool}")),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) fn receipt_for_write(
    plan: &OperationPlan,
    tool: &str,
    audit_id: &str,
    apply_started_at: chrono::DateTime<Utc>,
    record: SnowRecord,
    concurrency_token: ConcurrencyToken,
    before_record: Option<&SnowRecord>,
    warnings: Vec<ReceiptWarning>,
    state: &DaemonState,
) -> OperationReceipt {
    let record_url = format!(
        "{}/nav_to.do?uri={}.do?sys_id={}",
        state.core.config().instance.url.trim_end_matches('/'),
        record.table,
        record.sys_id
    );
    let changed_fields = plan
        .planned_changes
        .as_object()
        .map(|object| {
            object
                .iter()
                .map(|(field, after)| FieldChange {
                    field: field.clone(),
                    before: before_record.and_then(|record| record_field_value(record, field)),
                    after: Some(after.clone()),
                })
                .collect()
        })
        .unwrap_or_default();

    OperationReceipt {
        plan_id: plan.plan_id.clone(),
        audit_id: audit_id.to_string(),
        parent_audit_id: plan.plan_id.clone(),
        tool: tool.to_string(),
        status: ReceiptStatus::Success,
        applied_changes_summary: Some(plan.planned_changes.clone()),
        service_now_metadata: Some(ServiceNowMetadata {
            sys_id: Some(record.sys_id.clone()),
            number: Some(record.number.clone()),
            transaction_id: None,
        }),
        idempotency_replay: false,
        completed_at: Utc::now(),
        op_hash: plan.op_hash.clone(),
        record_url: Some(record_url),
        record_snapshot: serde_json::to_value(&record).ok(),
        changed_fields,
        concurrency_token_observed: Some(concurrency_token),
        apply_started_at: Some(apply_started_at),
        error_code: None,
        warnings,
    }
}

pub(super) fn record_field_value(record: &SnowRecord, field: &str) -> Option<Value> {
    record
        .fields
        .get(field)
        .map(|value| Value::String(value.value.clone()))
        .or_else(|| {
            record
                .references
                .get(field)
                .map(|reference| Value::String(reference.sys_id.clone()))
        })
}

pub(super) fn concurrency_from_record(record: &SnowRecord) -> Option<ConcurrencyToken> {
    let sys_updated_on = record
        .fields
        .get("sys_updated_on")
        .map(|field| field.value.trim().to_string())
        .filter(|value| !value.is_empty())?;
    let sys_mod_count = record
        .fields
        .get("sys_mod_count")
        .and_then(|field| field.value.trim().parse::<i64>().ok());
    Some(ConcurrencyToken {
        sys_updated_on,
        sys_mod_count,
    })
}

pub(super) fn record_ref_from_snow(record: &SnowRecord) -> RecordRef {
    RecordRef {
        sys_id: record.sys_id.clone(),
        number: record.number.clone(),
        table: record.table.clone(),
    }
}

pub(super) async fn handle_story_apply_impl(
    id: Option<Value>,
    tool: &str,
    params: &Value,
    state: &DaemonState,
) -> JsonRpcResponse {
    if is_kill_switched() {
        return audited_story_error(
            state,
            id,
            tool,
            -32057,
            "KILL_SWITCH",
            json!({ "code": "KILL_SWITCH" }),
            ResultStatus::Denied,
        )
        .await;
    }
    let board_binding_cfg = match board_binding(tool, state) {
        Ok(binding) => binding,
        Err(_) => {
            return audited_story_error(
                state,
                id,
                tool,
                -32051,
                "FIELD_REJECTED",
                json!({ "code": "FIELD_REJECTED", "fields": [{"field": "story_board_id", "reason": "blocked_deny_list"}] }),
                ResultStatus::Denied,
            )
            .await;
        }
    };
    if !state.mcp_config.policy.is_tool_enabled(tool) {
        return audited_story_error(
            state,
            id,
            tool,
            -32051,
            "FIELD_REJECTED",
            json!({ "code": "FIELD_REJECTED", "fields": [{"field": "tool", "reason": "blocked_deny_list"}] }),
            ResultStatus::Denied,
        )
        .await;
    }

    let Some(plan_id) = string_param(params, "plan_id") else {
        return audited_story_error(
            state,
            id,
            tool,
            -32051,
            "FIELD_REJECTED",
            json!({ "code": "FIELD_REJECTED", "fields": [{"field": "plan_id", "reason": "type_mismatch"}] }),
            ResultStatus::Denied,
        )
        .await;
    };
    let Some(confirmation_token) = string_param(params, "confirmation_token") else {
        return audited_story_error(
            state,
            id,
            tool,
            -32051,
            "FIELD_REJECTED",
            json!({ "code": "FIELD_REJECTED", "fields": [{"field": "confirmation_token", "reason": "type_mismatch"}] }),
            ResultStatus::Denied,
        )
        .await;
    };
    let Some(idempotency_key) = string_param(params, "idempotency_key") else {
        return audited_story_error(
            state,
            id,
            tool,
            -32051,
            "FIELD_REJECTED",
            json!({ "code": "FIELD_REJECTED", "fields": [{"field": "idempotency_key", "reason": "type_mismatch"}] }),
            ResultStatus::Denied,
        )
        .await;
    };
    let key = IdempotencyKey {
        value: idempotency_key.clone(),
        source: IdempotencyKeySource::ServerDerived,
    };

    let stores = match stores(state) {
        Ok(stores) => stores,
        Err(err) => return internal_error(id, err),
    };
    let plan_record = match stores.plan_store.get(&plan_id).await {
        Ok(Some(record)) => record,
        Ok(None) => {
            return audited_story_error(
                state,
                id,
                tool,
                -32056,
                "PLAN_NOT_FOUND",
                json!({ "code": "PLAN_NOT_FOUND", "plan_id": plan_id }),
                ResultStatus::Error,
            )
            .await;
        }
        Err(err) => return internal_error(id, err),
    };
    let plan = match serde_json::from_value::<OperationPlan>(plan_record.plan_json.clone()) {
        Ok(plan) => plan,
        Err(err) => return internal_error(id, err),
    };
    let receipt_warnings = plan_record
        .plan_json
        .get("receipt_warnings")
        .cloned()
        .and_then(|value| serde_json::from_value::<Vec<ReceiptWarning>>(value).ok())
        .unwrap_or_default();
    let apply_tool = apply_tool_for_plan_tool(&plan.tool);
    if apply_tool != tool {
        return invalid_params(
            id,
            format!("plan {} is bound to {apply_tool}, not {tool}", plan.plan_id),
        );
    }

    let actor = actor_from_params(params, state);
    let confirmation_binding = ConfirmationBinding {
        actor: actor.clone(),
        requester: requester_from_params(params, &actor),
        tool: tool.to_string(),
        op_hash: plan.op_hash.clone(),
        environment: state.mcp_config.environment.label.clone(),
    };
    if let Some(retry_after_seconds) = check_rate_limit_only(
        "story_failed_confirmation",
        &actor,
        tool,
        state.mcp_config.rate_limit.failed_confirmations_per_minute,
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

    if let Ok(Some(record)) = stores.idempotency_store.lookup_record(&key, tool)
        && record.expires_at > Utc::now()
    {
        if record.op_hash != plan.op_hash {
            return audited_story_error(
                state,
                id,
                tool,
                -32052,
                "IDEMPOTENCY_CONFLICT",
                json!({
                    "code": "IDEMPOTENCY_CONFLICT",
                    "idempotency_key": idempotency_key,
                    "bound_op_hash": record.op_hash,
                    "attempted_op_hash": plan.op_hash,
                }),
                ResultStatus::Denied,
            )
            .await;
        }
        if let Some(mut receipt) = record.receipt {
            if let Err(err) = validate_replay_confirmation(
                &stores.confirmation_store,
                &confirmation_token,
                &confirmation_binding,
            ) {
                record_rate_limit_event("story_failed_confirmation", &actor, tool);
                return confirmation_invalid(state, id, tool, err).await;
            }
            receipt.idempotency_replay = true;
            return JsonRpcResponse::ok(id, json!(receipt));
        }
    }

    let plan_expired =
        plan_record.state == PlanLifecycleState::Expired || plan_record.expires_at <= Utc::now();
    let mut confirmation_already_consumed_for_pending = false;
    if let Ok(Some(record)) = stores.idempotency_store.lookup_record(&key, tool)
        && record.expires_at > Utc::now()
        && record.receipt.is_none()
    {
        match stores.confirmation_store.lookup(&confirmation_token) {
            Ok(Some(confirmation)) => {
                let already_consumed = confirmation.consumed;
                let validation = if already_consumed {
                    validate_confirmation_binding(&confirmation, &confirmation_binding)
                } else {
                    validate_confirmation(&confirmation, &confirmation_binding)
                };
                if let Err(err) = validation {
                    record_rate_limit_event("story_failed_confirmation", &actor, tool);
                    return confirmation_invalid(state, id, tool, err).await;
                }
                match resolve_pending_response(
                    state,
                    tool,
                    &key,
                    &plan,
                    &plan_record,
                    receipt_warnings.clone(),
                    (!already_consumed).then_some((
                        confirmation_token.as_str(),
                        &confirmation_binding,
                        actor.as_str(),
                        confirmation_binding.requester.as_str(),
                    )),
                )
                .await
                {
                    Ok(PendingResolution::Recovered(receipt)) => {
                        return JsonRpcResponse::ok(id, json!(receipt));
                    }
                    Ok(PendingResolution::Proceed) => {
                        confirmation_already_consumed_for_pending = already_consumed;
                    }
                    Ok(PendingResolution::NeedsOperator) => {
                        return pending_resolution_required(state, id, tool, &plan).await;
                    }
                    Err(err) => return internal_error(id, err),
                }
            }
            Ok(_) => {}
            Err(err) => return internal_error(id, err),
        }
    }
    if plan_expired {
        let _ = stores.plan_store.mark_expired(&plan_id).await;
        return audited_story_error(
            state,
            id,
            tool,
            -32055,
            "PLAN_EXPIRED",
            json!({
                "code": "PLAN_EXPIRED",
                "plan_id": plan_id,
                "expired_at": plan_record.expires_at.to_rfc3339(),
            }),
            ResultStatus::Denied,
        )
        .await;
    }
    if let Some(retry_after_seconds) = check_and_record_rate_limit(
        "story_apply",
        &actor,
        tool,
        state.mcp_config.rate_limit.write_per_minute,
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

    match stores
        .idempotency_store
        .check_and_record(
            &key,
            tool,
            &plan.op_hash,
            state.mcp_config.policy.idempotency_window_seconds,
        )
        .await
    {
        Ok(IdempotencyOutcome::NewKey) => {}
        Ok(IdempotencyOutcome::Replay(mut receipt)) => {
            if let Err(err) = validate_replay_confirmation(
                &stores.confirmation_store,
                &confirmation_token,
                &confirmation_binding,
            ) {
                record_rate_limit_event("story_failed_confirmation", &actor, tool);
                return confirmation_invalid(state, id, tool, err).await;
            }
            receipt.idempotency_replay = true;
            return JsonRpcResponse::ok(id, json!(receipt));
        }
        // Plan-time reservation stores the same op_hash with no receipt. This
        // Pending outcome is the normal handoff from plan to first apply.
        Ok(IdempotencyOutcome::Pending { .. }) => {}
        Ok(IdempotencyOutcome::Conflict { .. }) => {
            let bound = stores
                .idempotency_store
                .lookup_record(&key, tool)
                .ok()
                .flatten()
                .map(|record| record.op_hash)
                .unwrap_or_default();
            return audited_story_error(
                state,
                id,
                tool,
                -32052,
                "IDEMPOTENCY_CONFLICT",
                json!({
                    "code": "IDEMPOTENCY_CONFLICT",
                    "idempotency_key": idempotency_key,
                    "bound_op_hash": bound,
                    "attempted_op_hash": plan.op_hash,
                }),
                ResultStatus::Denied,
            )
            .await;
        }
        Err(err) => return internal_error(id, err),
    }

    if let Err(failure) = enforce_apply_guard(tool, &plan, &board_binding_cfg, state).await {
        if let Some(fields) = story_scope_failure_field_rejections(&failure) {
            return audited_story_error(
                state,
                id,
                tool,
                -32051,
                "FIELD_REJECTED",
                json!({ "code": "FIELD_REJECTED", "fields": fields }),
                ResultStatus::Denied,
            )
            .await;
        }
        let data = match serde_json::to_value(failure.to_guard_failed_data()) {
            Ok(data) => data,
            Err(err) => return internal_error(id, err),
        };
        return audited_story_error(
            state,
            id,
            tool,
            -32050,
            "GUARD_FAILED",
            data,
            ResultStatus::Denied,
        )
        .await;
    }

    if let Some(expected) = &plan_record.concurrency_token {
        let Some(supplied) = params
            .get("concurrency_token")
            .cloned()
            .and_then(|value| serde_json::from_value::<ConcurrencyToken>(value).ok())
        else {
            return audited_story_error(
                state,
                id,
                tool,
                -32061,
                "CONCURRENCY_TOKEN_INVALID",
                json!({ "code": "CONCURRENCY_TOKEN_INVALID", "reason": "missing concurrency_token" }),
                ResultStatus::Denied,
            )
            .await;
        };
        if &supplied != expected {
            return audited_story_error(
                state,
                id,
                tool,
                -32061,
                "CONCURRENCY_TOKEN_INVALID",
                json!({ "code": "CONCURRENCY_TOKEN_INVALID", "reason": "token does not match plan" }),
                ResultStatus::Denied,
            )
            .await;
        }
        if let Some(target) = &plan.target {
            match state.core.get_record_fresh(&target.number).await {
                Ok(Some(current)) => {
                    if let Some(observed) = concurrency_from_record(&current)
                        && &observed != expected
                    {
                        return audited_story_error(
                            state,
                            id,
                            tool,
                            -32053,
                            "CONCURRENCY_CONFLICT",
                            json!({
                                "code": "CONCURRENCY_CONFLICT",
                                "record_sys_id": target.sys_id,
                                "expected": expected,
                                "observed": observed,
                            }),
                            ResultStatus::Denied,
                        )
                        .await;
                    }
                }
                Ok(None) => {
                    return audited_story_error(
                        state,
                        id,
                        tool,
                        -32056,
                        "PLAN_NOT_FOUND",
                        json!({ "code": "PLAN_NOT_FOUND", "plan_id": plan.plan_id }),
                        ResultStatus::Error,
                    )
                    .await;
                }
                Err(err) => return internal_error(id, err),
            }
        }
    }

    let before_record = if tool.contains("_update") {
        match plan.target.as_ref() {
            Some(target) => match state.core.get_record_fresh(&target.number).await {
                Ok(record) => record,
                Err(err) => return internal_error(id, err),
            },
            None => None,
        }
    } else {
        None
    };
    if let Err(err) = enforce_apply_field_governance(
        tool,
        &plan,
        &board_binding_cfg,
        state,
        before_record.as_ref(),
    )
    .await
    {
        return plan_build_error_to_apply_response(state, id, tool, err).await;
    }
    if !confirmation_already_consumed_for_pending {
        match stores.confirmation_store.lookup(&confirmation_token) {
            Ok(Some(confirmation)) => {
                if let Err(err) = validate_confirmation(&confirmation, &confirmation_binding) {
                    record_rate_limit_event("story_failed_confirmation", &actor, tool);
                    return confirmation_invalid(state, id, tool, err).await;
                }
            }
            Ok(None) => {
                record_rate_limit_event("story_failed_confirmation", &actor, tool);
                return confirmation_invalid(state, id, tool, ConfirmationConsumeError::NotFound)
                    .await;
            }
            Err(err) => return internal_error(id, err),
        }
    }
    let apply_started_at = Utc::now();
    if let Err(err) = stores
        .idempotency_store
        .mark_apply_started(&key, tool)
        .await
    {
        return internal_error(id, err);
    }
    let write_result = match apply_plan(tool, &plan, state).await {
        Ok(result) => result,
        Err(err) => {
            match try_resolve_pending_receipt(
                state,
                tool,
                &plan,
                &plan_record,
                receipt_warnings.clone(),
            )
            .await
            {
                Ok(PendingRecovery::Recovered(receipt)) => {
                    if !confirmation_already_consumed_for_pending {
                        if let Err(err) = stores
                            .confirmation_store
                            .consume(&confirmation_token, &confirmation_binding)
                            .await
                        {
                            record_rate_limit_event("story_failed_confirmation", &actor, tool);
                            return confirmation_invalid(state, id, tool, err).await;
                        }
                        if let Err(err) = append_confirmation_consume_audit(
                            state,
                            plan.plan_id.as_str(),
                            &actor,
                            &confirmation_binding.requester,
                        )
                        .await
                        {
                            return internal_error(id, err);
                        }
                    }
                    if let Err(err) = stores.idempotency_store.save_receipt(&key, &receipt).await {
                        return internal_error(id, err);
                    }
                    let _ = stores.plan_store.mark_consumed(&plan.plan_id).await;
                    return JsonRpcResponse::ok(id, json!(receipt));
                }
                Ok(PendingRecovery::Proceed) => {
                    return audited_story_error(
                        state,
                        id,
                        tool,
                        -32059,
                        "UPSTREAM_ERROR",
                        json!({
                            "code": "UPSTREAM_ERROR",
                            "reason": err.to_string(),
                            "retry_after_seconds": 2,
                        }),
                        ResultStatus::Error,
                    )
                    .await;
                }
                Ok(PendingRecovery::NeedsOperator) => {
                    return pending_resolution_required(state, id, tool, &plan).await;
                }
                Err(err) => return internal_error(id, err),
            }
        }
    };
    if !confirmation_already_consumed_for_pending {
        if let Err(err) = stores
            .confirmation_store
            .consume(&confirmation_token, &confirmation_binding)
            .await
        {
            record_rate_limit_event("story_failed_confirmation", &actor, tool);
            return confirmation_invalid(state, id, tool, err).await;
        }
        if let Err(err) = append_confirmation_consume_audit(
            state,
            plan.plan_id.as_str(),
            &actor,
            &confirmation_binding.requester,
        )
        .await
        {
            return internal_error(id, err);
        }
    }
    let audit_id = Uuid::new_v4().to_string();
    let receipt = receipt_for_write(
        &plan,
        tool,
        &audit_id,
        apply_started_at,
        write_result.record,
        ConcurrencyToken {
            sys_updated_on: write_result.concurrency.sys_updated_on,
            sys_mod_count: write_result.concurrency.sys_mod_count,
        },
        before_record.as_ref(),
        receipt_warnings,
        state,
    );
    let audit_email_hashes = audit_email_hashes_from_plan(&plan_record.plan_json);
    let (audit_summary, applied_changes) = audit_summary_for_receipt(&receipt, &audit_email_hashes);
    if let Err(err) = append_audit_event(
        state,
        &audit_id,
        Some(plan.plan_id.as_str()),
        tool,
        ResultStatus::AppliedSuccess,
        Some(audit_summary),
        receipt.service_now_metadata.clone(),
        None,
        Some((&actor, &confirmation_binding.requester)),
        Some(applied_changes),
    )
    .await
    {
        return internal_error(id, err);
    }

    if let Err(err) = stores.idempotency_store.save_receipt(&key, &receipt).await {
        return internal_error(id, err);
    }
    let _ = stores.plan_store.mark_consumed(&plan.plan_id).await;

    JsonRpcResponse::ok(id, json!(receipt))
}
