use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

use anyhow::Result;
use chrono::Utc;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use snow_core::{FieldChoice, SnowRecord};
use snow_mcp::audit::{AuditSink, SqliteAuditSink};
use snow_mcp::domain::audit::{
    ActorIdentity, AppliedChange, AuditEvent, ClientIdentity, ErrorRow, PolicyDecisionRow,
    ResultStatus, ServiceNowMetadata,
};
use snow_mcp::domain::policy::BoardBinding;
use snow_mcp::domain::primitives::{IdempotencyKey, IdempotencyKeySource, RecordRef};
use snow_mcp::domain::story::{
    Actor as StoryActor, InScopeFailure, StateChoice, StateResolution, StateResolutionContext,
    WARNING_ASSIGNEE_AMBIGUOUS, WARNING_ASSIGNEE_DEFAULTED_FROM_CALLER,
    WARNING_ASSIGNEE_UNRESOLVED, WARNING_PRIORITY_VALUE_UNMAPPED, assignee_ambiguous_warning_data,
    assignee_defaulted_warning_message, assignee_unresolved_warning_message, equal_sys_id,
    hash_email_for_audit, in_scope_snapshot_from_json, in_scope_snapshot_from_snow_record,
    is_sys_id, mask_email_for_receipt, resolve_state_from_cached_choices, story_in_scope,
    task_parent_in_scope, task_parent_sys_id_from_json,
};
use snow_mcp::planner::{
    ConcurrencyToken, ConfirmationBinding, ConfirmationConsumeError, ConfirmationRecord,
    ConfirmationStore, FieldChange, IdempotencyOutcome, IdempotencyStore, OperationPlan,
    OperationPlanBuilder, OperationReceipt, PlanLifecycleState, PlanStore, PlanStoreRecord,
    ReceiptStatus, ReceiptWarning, SqliteConfirmationStore, SqliteIdempotencyStore,
    SqlitePlanStore,
};
use uuid::Uuid;

use crate::DaemonState;
use crate::rpc::JsonRpcResponse;

const PLAN_TTL_SECONDS: i64 = 600;
const RATE_LIMIT_WINDOW_SECONDS: i64 = 60;
const CREATE_RECOVERY_CLOCK_SKEW_SECONDS: i64 = 300;

pub async fn handle_plan_get(
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

pub async fn handle_story_plan(
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

pub async fn handle_story_apply(
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

struct DaemonStores {
    plan_store: SqlitePlanStore,
    confirmation_store: SqliteConfirmationStore,
    idempotency_store: SqliteIdempotencyStore,
    audit_sink: SqliteAuditSink,
}

fn stores(state: &DaemonState) -> Result<DaemonStores> {
    std::fs::create_dir_all(&state.data_dir)?;
    let path = story_store_path(state);
    Ok(DaemonStores {
        plan_store: SqlitePlanStore::open(&path)?,
        confirmation_store: SqliteConfirmationStore::open(&path)?,
        idempotency_store: SqliteIdempotencyStore::open(&path)?,
        audit_sink: SqliteAuditSink::open(&path)?,
    })
}

fn story_store_path(state: &DaemonState) -> PathBuf {
    state.data_dir.join("mcp_story_write.sqlite3")
}

fn board_binding(tool: &str, state: &DaemonState) -> Result<BoardBinding> {
    state
        .mcp_config
        .policy
        .validate_story_board_policy(tool, &state.mcp_config.environment)
        .map_err(|err| anyhow::anyhow!(err.to_string()))?
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("tool {tool} is not a governed Story board tool"))
}

struct PlanInput {
    builder: OperationPlanBuilder,
    concurrency_token: Option<ConcurrencyToken>,
    warnings: Vec<ReceiptWarning>,
}

#[derive(Debug)]
enum PlanBuildError {
    Invalid(String),
    FieldRejected(Vec<Value>),
    GuardFailed(InScopeFailure),
    StateRequired(Value),
    NotFound(String),
    Upstream(anyhow::Error),
}

async fn build_plan_input(
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
            inject(
                &mut payload,
                "assignment_group",
                binding.assignment_group.clone(),
            );
            if let Some(sprint) = binding.allowed_sprints.first() {
                inject(&mut payload, "sprint", sprint.clone());
            }
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
            inject(&mut payload, "story", parent.sys_id.clone());
            inject(
                &mut payload,
                "assignment_group",
                binding.assignment_group.clone(),
            );
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

fn strip_non_writable_selector_fields(tool: &str, payload: &mut Value) {
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

async fn resolve_assignee_in_payload(
    tool: &str,
    payload: &mut Value,
    actor: &StoryActor,
    state: &DaemonState,
) -> std::result::Result<Vec<ReceiptWarning>, PlanBuildError> {
    let Some(object) = payload.as_object_mut() else {
        return Ok(Vec::new());
    };

    let supplied = object.get("assigned_to").cloned();
    match supplied {
        Some(Value::Null) => Ok(Vec::new()),
        Some(Value::String(value)) if is_sys_id(&value) => Ok(Vec::new()),
        Some(Value::String(value)) if value.trim().eq_ignore_ascii_case("caller") => {
            default_assignee_from_actor(object, actor, state).await
        }
        Some(Value::String(_)) => Err(field_rejected(vec![json!({
            "field": "assigned_to",
            "reason": "value_not_in_enum",
        })])),
        Some(_) => Err(field_rejected(vec![json!({
            "field": "assigned_to",
            "reason": "type_mismatch",
        })])),
        None if tool.ends_with("_plan_update") => Ok(Vec::new()),
        None => default_assignee_from_actor(object, actor, state).await,
    }
}

async fn default_assignee_from_actor(
    payload: &mut Map<String, Value>,
    actor: &StoryActor,
    state: &DaemonState,
) -> std::result::Result<Vec<ReceiptWarning>, PlanBuildError> {
    let mut ambiguous = Vec::new();

    if let Some(email) = actor
        .email
        .as_deref()
        .map(str::trim)
        .filter(|email| !email.is_empty())
    {
        let hits = find_active_user_sys_ids(state, "email", email).await;
        match hits.as_slice() {
            [sys_id] => {
                payload.insert("assigned_to".to_string(), Value::String(sys_id.clone()));
                let mask = mask_email_for_receipt(email);
                return Ok(vec![receipt_warning(
                    WARNING_ASSIGNEE_DEFAULTED_FROM_CALLER,
                    Some("assigned_to"),
                    assignee_defaulted_warning_message(&mask),
                    Some(json!({
                        "email_local_part": mask.email_local_part,
                        "domain_hash": mask.domain_hash,
                        "email_sha256": hash_email_for_audit(email),
                        "source": "email_match",
                    })),
                )]);
            }
            [] => {}
            hits => ambiguous.extend(hits.iter().cloned()),
        }
    }

    let subject = actor.subject.trim();
    if !subject.is_empty() {
        let hits = find_active_user_sys_ids(state, "user_name", subject).await;
        match hits.as_slice() {
            [sys_id] => {
                payload.insert("assigned_to".to_string(), Value::String(sys_id.clone()));
                return Ok(vec![receipt_warning(
                    WARNING_ASSIGNEE_DEFAULTED_FROM_CALLER,
                    Some("assigned_to"),
                    "Substituted caller identity for omitted assignee.",
                    Some(json!({ "source": "user_name_match" })),
                )]);
            }
            [] => {}
            hits => ambiguous.extend(hits.iter().cloned()),
        }
    }

    payload.remove("assigned_to");
    if ambiguous.is_empty() {
        let mask = actor.email.as_deref().map(mask_email_for_receipt);
        let data = mask.as_ref().map(|mask| {
            json!({
                "email_local_part": mask.email_local_part,
                "domain_hash": mask.domain_hash,
                "email_sha256": actor.email.as_deref().map(hash_email_for_audit),
            })
        });
        Ok(vec![receipt_warning(
            WARNING_ASSIGNEE_UNRESOLVED,
            Some("assigned_to"),
            assignee_unresolved_warning_message(mask.as_ref()),
            data,
        )])
    } else {
        let warning_data =
            assignee_ambiguous_warning_data(actor.email.as_deref(), ambiguous.clone())
                .and_then(|data| {
                    let mut value = serde_json::to_value(data).ok()?;
                    if let Some(email) = actor.email.as_deref()
                        && let Some(object) = value.as_object_mut()
                    {
                        object.insert(
                            "email_sha256".to_string(),
                            Value::String(hash_email_for_audit(email)),
                        );
                    }
                    Some(value)
                })
                .or_else(|| Some(json!({ "candidate_sys_ids": ambiguous })));
        Ok(vec![receipt_warning(
            WARNING_ASSIGNEE_AMBIGUOUS,
            Some("assigned_to"),
            "Multiple sys_user records matched caller identity; field left blank. Specify assigned_to explicitly.",
            warning_data,
        )])
    }
}

async fn find_active_user_sys_ids(state: &DaemonState, field: &str, value: &str) -> Vec<String> {
    match state
        .core
        .client()
        .table("sys_user")
        .equals(field, value)
        .equals("active", "true")
        .fields(&["sys_id"])
        .limit(3)
        .execute()
        .await
    {
        Ok(response) => response
            .records
            .into_iter()
            .map(|record| record.sys_id)
            .collect(),
        Err(_) => Vec::new(),
    }
}

fn receipt_warning(
    code: impl Into<String>,
    field: Option<&str>,
    message: impl Into<String>,
    data: Option<Value>,
) -> ReceiptWarning {
    ReceiptWarning {
        code: code.into(),
        field: field.map(ToOwned::to_owned),
        message: message.into(),
        data,
    }
}

fn receipt_warnings_for_caller(warnings: &[ReceiptWarning]) -> Vec<ReceiptWarning> {
    warnings
        .iter()
        .cloned()
        .map(|mut warning| {
            if let Some(Value::Object(mut data)) = warning.data.take() {
                data.remove("email_sha256");
                warning.data = (!data.is_empty()).then_some(Value::Object(data));
            }
            warning
        })
        .collect()
}

fn audit_email_hashes_from_warnings(warnings: &[ReceiptWarning]) -> Vec<String> {
    warnings
        .iter()
        .filter_map(|warning| {
            warning
                .data
                .as_ref()
                .and_then(|data| data.get("email_sha256"))
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn audit_email_hashes_from_plan(plan_json: &Value) -> Vec<String> {
    plan_json
        .get("audit_warning_email_sha256")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(ToOwned::to_owned)
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

async fn validate_constrained_fields(
    tool: &str,
    payload: &mut Value,
    binding: &BoardBinding,
    state: &DaemonState,
    current_record: Option<&SnowRecord>,
) -> std::result::Result<Vec<ReceiptWarning>, PlanBuildError> {
    let mut fields = Vec::new();
    let mut warnings = Vec::new();
    let table = table_for_story_tool(tool, binding);
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
enum ChoiceValidation {
    Matched(String),
    Constrained,
    Unmapped,
}

async fn normalize_choice_value(
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

fn match_choice_validation(
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

async fn dictionary_choice_is_constrained(
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

async fn state_choices_for_validation(
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

fn choice_is_terminal(value: &str, choices: &[StateChoice]) -> bool {
    choices
        .iter()
        .any(|choice| choice.terminal && choice.value.eq_ignore_ascii_case(value))
}

fn is_terminal_choice(value: &str) -> bool {
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

fn record_state_value(record: &SnowRecord) -> Option<String> {
    record
        .fields
        .get("state")
        .map(|field| field.value.clone())
        .filter(|value| !value.trim().is_empty())
        .or_else(|| (!record.state.trim().is_empty()).then(|| record.state.clone()))
}

async fn epic_exists(
    state: &DaemonState,
    epic_sys_id: &str,
) -> std::result::Result<bool, PlanBuildError> {
    state
        .core
        .client()
        .table("rm_epic")
        .equals("sys_id", epic_sys_id)
        .fields(&["sys_id"])
        .limit(1)
        .first()
        .await
        .map(|record| record.is_some())
        .map_err(|err| PlanBuildError::Upstream(err.into()))
}

fn table_for_story_tool<'a>(tool: &str, binding: &'a BoardBinding) -> &'a str {
    if tool.contains("task") {
        &binding.task_table
    } else {
        &binding.story_table
    }
}

fn enforce_story_payload_scope(
    payload: &Value,
    binding: &BoardBinding,
) -> std::result::Result<(), PlanBuildError> {
    let snapshot = in_scope_snapshot_from_json(payload).ok_or_else(|| {
        PlanBuildError::GuardFailed(InScopeFailure::WrongAssignmentGroup {
            expected: binding.assignment_group.clone(),
            observed: String::new(),
        })
    })?;
    story_in_scope(&snapshot.as_snapshot(), binding).map_err(PlanBuildError::GuardFailed)
}

fn enforce_story_record_scope(
    record: &SnowRecord,
    binding: &BoardBinding,
) -> std::result::Result<(), PlanBuildError> {
    story_record_in_scope(record, binding).map_err(PlanBuildError::GuardFailed)
}

fn story_record_in_scope(
    record: &SnowRecord,
    binding: &BoardBinding,
) -> std::result::Result<(), InScopeFailure> {
    let snapshot = in_scope_snapshot_from_snow_record(record).ok_or_else(|| {
        InScopeFailure::WrongAssignmentGroup {
            expected: binding.assignment_group.clone(),
            observed: String::new(),
        }
    })?;
    story_in_scope(&snapshot.as_snapshot(), binding)
}

fn enforce_task_parent_payload_scope(
    payload: &Value,
    parent: &SnowRecord,
    binding: &BoardBinding,
) -> std::result::Result<(), PlanBuildError> {
    let snapshot = in_scope_snapshot_from_snow_record(parent).ok_or_else(|| {
        PlanBuildError::GuardFailed(InScopeFailure::TaskParentOutOfScope {
            cause: Box::new(InScopeFailure::WrongAssignmentGroup {
                expected: binding.assignment_group.clone(),
                observed: String::new(),
            }),
        })
    })?;
    let task_parent = task_parent_sys_id_from_json(payload).unwrap_or_default();
    task_parent_in_scope(
        &snapshot.as_snapshot(),
        &task_parent,
        &parent.sys_id,
        binding,
    )
    .map_err(PlanBuildError::GuardFailed)
}

fn enforce_task_record_scope(
    task: &SnowRecord,
    binding: &BoardBinding,
) -> std::result::Result<(), PlanBuildError> {
    task_record_in_scope(task, binding).map_err(PlanBuildError::GuardFailed)
}

fn task_record_in_scope(
    task: &SnowRecord,
    binding: &BoardBinding,
) -> std::result::Result<(), InScopeFailure> {
    if !task.table.trim().eq_ignore_ascii_case(&binding.task_table) {
        return Err(InScopeFailure::TaskParentStoryMismatch {
            expected: binding.task_table.clone(),
            observed: task.table.clone(),
        });
    }

    let snapshot = in_scope_snapshot_from_snow_record(task).ok_or_else(|| {
        InScopeFailure::WrongAssignmentGroup {
            expected: binding.assignment_group.clone(),
            observed: String::new(),
        }
    })?;

    if !task_assignment_group_allowed(&snapshot.assignment_group_sys_id, binding) {
        return Err(InScopeFailure::WrongAssignmentGroup {
            expected: expected_task_assignment_groups(binding),
            observed: snapshot.assignment_group_sys_id,
        });
    }

    Ok(())
}

fn task_assignment_group_allowed(observed: &str, binding: &BoardBinding) -> bool {
    equal_sys_id(observed, &binding.assignment_group)
        || binding
            .allowed_task_assignment_groups
            .iter()
            .any(|allowed| equal_sys_id(observed, allowed))
}

fn expected_task_assignment_groups(binding: &BoardBinding) -> String {
    let mut groups = vec![binding.assignment_group.clone()];
    for group in &binding.allowed_task_assignment_groups {
        if !group.trim().is_empty() && !groups.iter().any(|seen| equal_sys_id(seen, group)) {
            groups.push(group.clone());
        }
    }
    groups.join(",")
}

fn field_rejected(fields: Vec<Value>) -> PlanBuildError {
    PlanBuildError::FieldRejected(fields)
}

async fn enforce_apply_field_governance(
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

async fn apply_plan(
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

// Short-lived control-flow result, never stored in bulk; the size difference
// between variants is harmless here.
#[allow(clippy::large_enum_variant)]
enum PendingResolution {
    Recovered(OperationReceipt),
    Proceed,
    NeedsOperator,
}

async fn resolve_pending_response(
    state: &DaemonState,
    tool: &str,
    key: &IdempotencyKey,
    plan: &OperationPlan,
    plan_record: &PlanStoreRecord,
    warnings: Vec<ReceiptWarning>,
    consume_confirmation: Option<(&str, &ConfirmationBinding, &str, &str)>,
) -> Result<PendingResolution> {
    match try_resolve_pending_receipt(state, tool, plan, plan_record, warnings).await? {
        PendingRecovery::Recovered(receipt) => {
            let stores = stores(state)?;
            if let Some((token, binding, actor, requester)) = consume_confirmation {
                stores
                    .confirmation_store
                    .consume(token, binding)
                    .await
                    .map_err(|err| {
                        anyhow::anyhow!("confirmation recovery consume failed: {err}")
                    })?;
                append_confirmation_consume_audit(state, plan.plan_id.as_str(), actor, requester)
                    .await?;
            }
            stores.idempotency_store.save_receipt(key, &receipt).await?;
            let _ = stores.plan_store.mark_consumed(&plan.plan_id).await;
            let audit_email_hashes = audit_email_hashes_from_plan(&plan_record.plan_json);
            let (audit_summary, applied_changes) =
                audit_summary_for_receipt(&receipt, &audit_email_hashes);
            append_audit_event(
                state,
                &receipt.audit_id,
                Some(plan.plan_id.as_str()),
                tool,
                ResultStatus::Replay,
                Some(audit_summary),
                receipt.service_now_metadata.clone(),
                None,
                Some((plan_record.actor.as_str(), plan_record.actor.as_str())),
                Some(applied_changes),
            )
            .await?;
            Ok(PendingResolution::Recovered(receipt))
        }
        PendingRecovery::Proceed => Ok(PendingResolution::Proceed),
        PendingRecovery::NeedsOperator => Ok(PendingResolution::NeedsOperator),
    }
}

// Short-lived control-flow result, never stored in bulk; the size difference
// between variants is harmless here.
#[allow(clippy::large_enum_variant)]
enum PendingRecovery {
    Recovered(OperationReceipt),
    Proceed,
    NeedsOperator,
}

#[derive(Debug, PartialEq, Eq)]
enum CreatePendingDecision {
    Proceed,
    NeedsOperator,
}

fn classify_create_recovery_lookup(lookup: &CreateRecoveryLookup) -> CreatePendingDecision {
    match lookup {
        CreateRecoveryLookup::Found(_) | CreateRecoveryLookup::Ambiguous => {
            CreatePendingDecision::NeedsOperator
        }
        CreateRecoveryLookup::NoMatch => CreatePendingDecision::Proceed,
    }
}

#[derive(Debug, PartialEq, Eq)]
enum UpdatePendingDecision {
    AlreadyApplied,
    ProceedUnchangedToken,
    NeedsOperator,
}

fn classify_update_recovery_record(
    record: &SnowRecord,
    planned_changes: &Value,
    expected_token: Option<&ConcurrencyToken>,
) -> UpdatePendingDecision {
    if record_already_matches(record, planned_changes) {
        return UpdatePendingDecision::AlreadyApplied;
    }
    if expected_token
        .is_some_and(|expected| concurrency_from_record(record).as_ref() == Some(expected))
    {
        return UpdatePendingDecision::ProceedUnchangedToken;
    }
    UpdatePendingDecision::NeedsOperator
}

async fn try_resolve_pending_receipt(
    state: &DaemonState,
    tool: &str,
    plan: &OperationPlan,
    plan_record: &PlanStoreRecord,
    warnings: Vec<ReceiptWarning>,
) -> Result<PendingRecovery> {
    let recovered = match tool {
        "story_apply_create" | "story_task_apply_create" => {
            match find_recovered_create_record(state, tool, plan, plan_record).await? {
                CreateRecoveryLookup::Found(record) => Some(record),
                lookup => match classify_create_recovery_lookup(&lookup) {
                    CreatePendingDecision::Proceed => return Ok(PendingRecovery::Proceed),
                    CreatePendingDecision::NeedsOperator => {
                        return Ok(PendingRecovery::NeedsOperator);
                    }
                },
            }
        }
        "story_apply_update" | "story_task_apply_update" => {
            let Some(target) = &plan.target else {
                return Ok(PendingRecovery::NeedsOperator);
            };
            match state.core.get_record_fresh(&target.number).await? {
                Some(record) => match classify_update_recovery_record(
                    &record,
                    &plan.planned_changes,
                    plan_record.concurrency_token.as_ref(),
                ) {
                    UpdatePendingDecision::AlreadyApplied => Some(record),
                    UpdatePendingDecision::ProceedUnchangedToken => {
                        return Ok(PendingRecovery::Proceed);
                    }
                    UpdatePendingDecision::NeedsOperator => {
                        return Ok(PendingRecovery::NeedsOperator);
                    }
                },
                _ => return Ok(PendingRecovery::NeedsOperator),
            }
        }
        _ => None,
    };

    let Some(record) = recovered else {
        return Ok(PendingRecovery::NeedsOperator);
    };
    let concurrency = concurrency_from_record(&record).unwrap_or(ConcurrencyToken {
        sys_updated_on: String::new(),
        sys_mod_count: None,
    });
    Ok(PendingRecovery::Recovered(receipt_for_write(
        plan,
        tool,
        &Uuid::new_v4().to_string(),
        Utc::now(),
        record,
        concurrency,
        None,
        warnings,
        state,
    )))
}

async fn append_confirmation_consume_audit(
    state: &DaemonState,
    plan_id: &str,
    actor: &str,
    requester: &str,
) -> Result<()> {
    append_audit_event(
        state,
        &Uuid::new_v4().to_string(),
        Some(plan_id),
        "confirmation_consume",
        ResultStatus::Plan,
        None,
        None,
        None,
        Some((actor, requester)),
        None,
    )
    .await?;
    Ok(())
}

// Short-lived control-flow result, never stored in bulk; the size difference
// between variants is harmless here.
#[allow(clippy::large_enum_variant)]
enum CreateRecoveryLookup {
    Found(SnowRecord),
    NoMatch,
    Ambiguous,
}

async fn find_recovered_create_record(
    state: &DaemonState,
    tool: &str,
    plan: &OperationPlan,
    plan_record: &PlanStoreRecord,
) -> Result<CreateRecoveryLookup> {
    let Some(changes) = plan.planned_changes.as_object() else {
        return Ok(CreateRecoveryLookup::NoMatch);
    };
    let Some(short_description) = changes.get("short_description").and_then(Value::as_str) else {
        return Ok(CreateRecoveryLookup::NoMatch);
    };
    let created_after = create_recovery_created_after(plan_record);

    let records = match tool {
        "story_apply_create" => {
            let Some(assignment_group) = changes.get("assignment_group").and_then(Value::as_str)
            else {
                return Ok(CreateRecoveryLookup::NoMatch);
            };
            let Some(sprint) = changes.get("sprint").and_then(Value::as_str) else {
                return Ok(CreateRecoveryLookup::NoMatch);
            };
            state
                .core
                .client()
                .table("rm_story")
                .equals("assignment_group", assignment_group)
                .equals("sprint", sprint)
                .equals("short_description", short_description)
                .greater_than("sys_created_on", &created_after)
                .fields(&["number", "sys_id"])
                .limit(2)
                .execute()
                .await?
                .records
        }
        "story_task_apply_create" => {
            let Some(story) = changes.get("story").and_then(Value::as_str) else {
                return Ok(CreateRecoveryLookup::NoMatch);
            };
            state
                .core
                .client()
                .table("rm_scrum_task")
                .equals("story", story)
                .equals("short_description", short_description)
                .greater_than("sys_created_on", &created_after)
                .fields(&["number", "sys_id"])
                .limit(2)
                .execute()
                .await?
                .records
        }
        _ => return Ok(CreateRecoveryLookup::NoMatch),
    };

    if records.len() != 1 {
        return Ok(if records.is_empty() {
            CreateRecoveryLookup::NoMatch
        } else {
            CreateRecoveryLookup::Ambiguous
        });
    }
    let number = records[0]
        .get_raw("number")
        .or_else(|| records[0].get_str("number"))
        .map(str::to_string);
    match number {
        Some(number) => state.core.get_record_fresh(&number).await.map(|record| {
            record
                .map(CreateRecoveryLookup::Found)
                .unwrap_or(CreateRecoveryLookup::NoMatch)
        }),
        None => Ok(CreateRecoveryLookup::NoMatch),
    }
}

fn create_recovery_created_after(plan_record: &PlanStoreRecord) -> String {
    (plan_record.created_at - chrono::Duration::seconds(CREATE_RECOVERY_CLOCK_SKEW_SECONDS))
        .format("%Y-%m-%d %H:%M:%S")
        .to_string()
}

fn record_already_matches(record: &SnowRecord, planned_changes: &Value) -> bool {
    let Some(changes) = planned_changes.as_object() else {
        return false;
    };
    changes.iter().all(|(field, expected)| {
        record_field_value(record, field)
            .as_ref()
            .is_some_and(|observed| value_matches(observed, expected))
    })
}

fn value_matches(observed: &Value, expected: &Value) -> bool {
    match (observed, expected) {
        (Value::String(observed), Value::String(expected)) => observed == expected,
        (Value::String(observed), other) => observed == &other.to_string(),
        (other, Value::String(expected)) => &other.to_string() == expected,
        _ => observed == expected,
    }
}

async fn enforce_apply_guard(
    tool: &str,
    plan: &OperationPlan,
    binding: &BoardBinding,
    state: &DaemonState,
) -> std::result::Result<(), InScopeFailure> {
    match tool {
        "story_apply_create" => {
            let snapshot = in_scope_snapshot_from_json(&plan.planned_changes).ok_or_else(|| {
                InScopeFailure::WrongAssignmentGroup {
                    expected: binding.assignment_group.clone(),
                    observed: String::new(),
                }
            })?;
            story_in_scope(&snapshot.as_snapshot(), binding)
        }
        "story_task_apply_create" => {
            let target =
                plan.target
                    .as_ref()
                    .ok_or_else(|| InScopeFailure::TaskParentStoryMismatch {
                        expected: "parent_story".to_string(),
                        observed: String::new(),
                    })?;
            let parent = state
                .core
                .get_record_fresh(&target.number)
                .await
                .map_err(|_| InScopeFailure::TaskParentOutOfScope {
                    cause: Box::new(InScopeFailure::MissingSprint),
                })?
                .ok_or_else(|| InScopeFailure::TaskParentStoryMismatch {
                    expected: target.sys_id.clone(),
                    observed: String::new(),
                })?;
            let snapshot = in_scope_snapshot_from_snow_record(&parent).ok_or_else(|| {
                InScopeFailure::TaskParentOutOfScope {
                    cause: Box::new(InScopeFailure::WrongAssignmentGroup {
                        expected: binding.assignment_group.clone(),
                        observed: String::new(),
                    }),
                }
            })?;
            let task_parent =
                task_parent_sys_id_from_json(&plan.planned_changes).unwrap_or_default();
            task_parent_in_scope(
                &snapshot.as_snapshot(),
                &task_parent,
                &parent.sys_id,
                binding,
            )
        }
        "story_apply_update" => {
            let target =
                plan.target
                    .as_ref()
                    .ok_or_else(|| InScopeFailure::WrongAssignmentGroup {
                        expected: binding.assignment_group.clone(),
                        observed: String::new(),
                    })?;
            let current = state
                .core
                .get_record_fresh(&target.number)
                .await
                .map_err(|_| InScopeFailure::WrongAssignmentGroup {
                    expected: binding.assignment_group.clone(),
                    observed: String::new(),
                })?
                .ok_or_else(|| InScopeFailure::WrongAssignmentGroup {
                    expected: binding.assignment_group.clone(),
                    observed: String::new(),
                })?;
            story_record_in_scope(&current, binding)
        }
        "story_task_apply_update" => {
            let target =
                plan.target
                    .as_ref()
                    .ok_or_else(|| InScopeFailure::TaskParentStoryMismatch {
                        expected: "parent_story".to_string(),
                        observed: String::new(),
                    })?;
            let current = state
                .core
                .get_record_fresh(&target.number)
                .await
                .map_err(|_| InScopeFailure::TaskParentStoryMismatch {
                    expected: target.sys_id.clone(),
                    observed: String::new(),
                })?
                .ok_or_else(|| InScopeFailure::TaskParentStoryMismatch {
                    expected: target.sys_id.clone(),
                    observed: String::new(),
                })?;
            task_record_in_scope(&current, binding)
        }
        _ => Ok(()),
    }
}

#[allow(clippy::too_many_arguments)]
fn receipt_for_write(
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

fn record_field_value(record: &SnowRecord, field: &str) -> Option<Value> {
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

fn concurrency_from_record(record: &SnowRecord) -> Option<ConcurrencyToken> {
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

fn record_ref_from_snow(record: &SnowRecord) -> RecordRef {
    RecordRef {
        sys_id: record.sys_id.clone(),
        number: record.number.clone(),
        table: record.table.clone(),
    }
}

#[cfg(test)]
fn reject_blocked_fields(
    id: Option<Value>,
    tool: &str,
    args: &Map<String, Value>,
) -> Option<JsonRpcResponse> {
    story_field_rejected_response(id, blocked_field_rejections(tool, args))
}

fn reject_plan_input_fields(
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

fn story_field_rejected_response(id: Option<Value>, fields: Vec<Value>) -> Option<JsonRpcResponse> {
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
fn blocked_field_rejections(tool: &str, args: &Map<String, Value>) -> Vec<Value> {
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
enum FieldGovernanceMode {
    PlanInput,
    WritePayload,
}

fn field_governance_rejections(
    tool: &str,
    args: &Map<String, Value>,
    policy: &snow_mcp::domain::policy::PolicyConfig,
    mode: FieldGovernanceMode,
) -> Vec<Value> {
    let blocked = blocked_fields_for_tool(tool);
    let mut fields = Vec::new();
    let mut rejected = BTreeSet::new();

    for field in args.keys() {
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

fn field_allowlist_for_apply_tool(
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

fn metadata_fields() -> &'static [&'static str] {
    &["actor", "requester", "client", "session_id", "user_agent"]
}

fn plan_selector_fields(tool: &str) -> &'static [&'static str] {
    match tool {
        "story_plan_update" | "story_task_plan_update" => &["number"],
        "story_task_plan_create" => &["parent_story_number"],
        _ => &[],
    }
}

fn blocked_fields_for_tool(tool: &str) -> BTreeSet<&'static str> {
    let mut blocked = BTreeSet::from([
        "sys_id",
        "sys_class_name",
        "sys_created_on",
        "sys_created_by",
        "sys_updated_on",
        "sys_updated_by",
        "sys_mod_count",
        "approval",
        "parent",
        "cmdb_ci",
        "release_scrum",
        "team",
        "opened_by",
        "closed_by",
        "vendor",
        "assignment_group",
        "sprint",
        "story",
        "active",
    ]);
    if !matches!(tool, "story_plan_update" | "story_task_plan_update") {
        blocked.insert("number");
    }
    blocked
}

fn require_string(
    args: &Map<String, Value>,
    field: &str,
) -> std::result::Result<(), PlanBuildError> {
    match args.get(field).and_then(Value::as_str) {
        Some(value) if !value.trim().is_empty() => Ok(()),
        _ => Err(PlanBuildError::Invalid(field.to_string())),
    }
}

fn inject<T: serde::Serialize>(payload: &mut Value, field: &str, value: T) {
    if let Value::Object(object) = payload {
        object.insert(field.to_string(), json!(value));
    }
}

fn object_args(params: &Value) -> Option<&Map<String, Value>> {
    params.as_object()
}

fn string_param(params: &Value, field: &str) -> Option<String> {
    params
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn actor_from_params(params: &Value, state: &DaemonState) -> String {
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

fn requester_from_params(params: &Value, actor: &str) -> String {
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

fn apply_tool_for_plan_tool(tool: &str) -> &str {
    match tool {
        "story_plan_create" => "story_apply_create",
        "story_plan_update" => "story_apply_update",
        "story_task_plan_create" => "story_task_apply_create",
        "story_task_plan_update" => "story_task_apply_update",
        other => other,
    }
}

fn is_kill_switched() -> bool {
    std::env::var("SNOW_STORY_WRITE_KILL_SWITCH")
        .map(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false)
}

fn validate_confirmation(
    record: &ConfirmationRecord,
    binding: &ConfirmationBinding,
) -> std::result::Result<(), ConfirmationConsumeError> {
    validate_confirmation_binding(record, binding)?;
    if record.consumed {
        return Err(ConfirmationConsumeError::AlreadyConsumed);
    }
    Ok(())
}

fn validate_confirmation_binding(
    record: &ConfirmationRecord,
    binding: &ConfirmationBinding,
) -> std::result::Result<(), ConfirmationConsumeError> {
    if record.revoked {
        return Err(ConfirmationConsumeError::Revoked);
    }
    if record.expires_at <= Utc::now() {
        return Err(ConfirmationConsumeError::Expired);
    }
    for (field, stored, incoming) in [
        ("actor", record.actor.as_str(), binding.actor.as_str()),
        (
            "requester",
            record.requester.as_str(),
            binding.requester.as_str(),
        ),
        ("tool", record.tool.as_str(), binding.tool.as_str()),
        ("op_hash", record.op_hash.as_str(), binding.op_hash.as_str()),
        (
            "environment",
            record.environment.as_str(),
            binding.environment.as_str(),
        ),
    ] {
        if stored != incoming {
            return Err(ConfirmationConsumeError::BindingMismatch { field });
        }
    }
    Ok(())
}

fn validate_replay_confirmation(
    store: &SqliteConfirmationStore,
    token: &str,
    binding: &ConfirmationBinding,
) -> std::result::Result<(), ConfirmationConsumeError> {
    match store.lookup(token) {
        Ok(Some(record)) => validate_confirmation_replay_binding(&record, binding),
        Ok(None) | Err(_) => Err(ConfirmationConsumeError::NotFound),
    }
}

fn validate_confirmation_replay_binding(
    record: &ConfirmationRecord,
    binding: &ConfirmationBinding,
) -> std::result::Result<(), ConfirmationConsumeError> {
    if record.revoked {
        return Err(ConfirmationConsumeError::Revoked);
    }
    for (field, stored, incoming) in [
        ("actor", record.actor.as_str(), binding.actor.as_str()),
        (
            "requester",
            record.requester.as_str(),
            binding.requester.as_str(),
        ),
        ("tool", record.tool.as_str(), binding.tool.as_str()),
        ("op_hash", record.op_hash.as_str(), binding.op_hash.as_str()),
        (
            "environment",
            record.environment.as_str(),
            binding.environment.as_str(),
        ),
    ] {
        if stored != incoming {
            return Err(ConfirmationConsumeError::BindingMismatch { field });
        }
    }
    Ok(())
}

static RATE_LIMITS: OnceLock<Mutex<BTreeMap<String, Vec<chrono::DateTime<Utc>>>>> = OnceLock::new();

fn check_and_record_rate_limit(
    bucket: &str,
    actor: &str,
    tool: &str,
    limit_per_minute: u32,
) -> Option<i64> {
    rate_limit(bucket, actor, tool, limit_per_minute, true)
}

fn check_rate_limit_only(
    bucket: &str,
    actor: &str,
    tool: &str,
    limit_per_minute: u32,
) -> Option<i64> {
    rate_limit(bucket, actor, tool, limit_per_minute, false)
}

fn record_rate_limit_event(bucket: &str, actor: &str, tool: &str) {
    let _ = rate_limit(bucket, actor, tool, u32::MAX, true);
}

fn rate_limit(
    bucket: &str,
    actor: &str,
    tool: &str,
    limit_per_minute: u32,
    record_event: bool,
) -> Option<i64> {
    if limit_per_minute == 0 {
        return Some(RATE_LIMIT_WINDOW_SECONDS);
    }

    let now = Utc::now();
    let cutoff = now - chrono::Duration::seconds(RATE_LIMIT_WINDOW_SECONDS);
    let key = format!("{bucket}:{actor}:{tool}");
    let mut buckets = RATE_LIMITS
        .get_or_init(|| Mutex::new(BTreeMap::new()))
        .lock()
        .ok()?;
    let entries = buckets.entry(key).or_default();
    entries.retain(|timestamp| *timestamp > cutoff);
    if entries.len() >= limit_per_minute as usize {
        return entries.first().map(|oldest| {
            let reset_at = *oldest + chrono::Duration::seconds(RATE_LIMIT_WINDOW_SECONDS);
            (reset_at - now).num_seconds().max(1)
        });
    }
    if record_event {
        entries.push(now);
    }
    None
}

async fn confirmation_invalid(
    state: &DaemonState,
    id: Option<Value>,
    tool: &str,
    err: ConfirmationConsumeError,
) -> JsonRpcResponse {
    audited_story_error(
        state,
        id,
        tool,
        -32054,
        "CONFIRMATION_INVALID",
        json!({
            "code": "CONFIRMATION_INVALID",
            "reason": err.to_string(),
        }),
        ResultStatus::Denied,
    )
    .await
}

async fn pending_resolution_required(
    state: &DaemonState,
    id: Option<Value>,
    tool: &str,
    plan: &OperationPlan,
) -> JsonRpcResponse {
    audited_story_error(
        state,
        id,
        tool,
        -32060,
        "PENDING_RESOLUTION_REQUIRED",
        json!({
            "code": "PENDING_RESOLUTION_REQUIRED",
            "plan_id": plan.plan_id,
            "retry_after_seconds": 2,
        }),
        ResultStatus::Error,
    )
    .await
}

async fn audited_story_error(
    state: &DaemonState,
    id: Option<Value>,
    tool: &str,
    code: i64,
    message: &str,
    data: Value,
    status: ResultStatus,
) -> JsonRpcResponse {
    match append_audit_event(
        state,
        &Uuid::new_v4().to_string(),
        None,
        tool,
        status,
        None,
        None,
        Some(ErrorRow {
            code: message.to_string(),
            reason: audit_error_reason(message, &data),
            retryable: false,
            transient: false,
        }),
        None,
        None,
    )
    .await
    {
        Ok(_) => story_error(id, code, message, data),
        Err(err) => internal_error(id, err),
    }
}

fn audit_error_reason(message: &str, data: &Value) -> String {
    data.get("code")
        .and_then(Value::as_str)
        .unwrap_or(message)
        .to_string()
}

fn audit_summary_for_receipt(
    receipt: &OperationReceipt,
    audit_email_hashes: &[String],
) -> (Value, Vec<AppliedChange>) {
    let applied_changes = receipt
        .changed_fields
        .iter()
        .map(|change| {
            let free_text = is_free_text_field(&change.field);
            AppliedChange {
                field: change.field.clone(),
                old_hash: hash_audit_value(change.before.as_ref()),
                new_hash: hash_audit_value(change.after.as_ref()),
                redacted_preview: if free_text {
                    None
                } else {
                    change.after.as_ref().map(non_free_text_preview)
                },
            }
        })
        .collect::<Vec<_>>();

    (
        json!({
            "op_hash": receipt.op_hash,
            "warnings": audit_warnings(&receipt.warnings, audit_email_hashes),
            "changed_fields": applied_changes,
        }),
        applied_changes,
    )
}

fn audit_warnings(warnings: &[ReceiptWarning], audit_email_hashes: &[String]) -> Vec<Value> {
    warnings
        .iter()
        .map(|warning| {
            let mut value = json!({
                "code": warning.code,
                "field": warning.field,
            });
            if is_d5_caller_email_warning(&warning.code)
                && let Some(hash) = audit_email_hashes.first()
            {
                value["data"] = json!({ "email_sha256": hash });
            }
            value
        })
        .collect()
}

fn is_d5_caller_email_warning(code: &str) -> bool {
    matches!(
        code,
        WARNING_ASSIGNEE_DEFAULTED_FROM_CALLER
            | WARNING_ASSIGNEE_UNRESOLVED
            | WARNING_ASSIGNEE_AMBIGUOUS
    )
}

fn hash_audit_value(value: Option<&Value>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(
        value
            .map(|value| serde_json::to_vec(value).unwrap_or_default())
            .unwrap_or_default(),
    );
    hex::encode(hasher.finalize())
}

fn non_free_text_preview(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        other => other.to_string(),
    }
}

fn is_free_text_field(field: &str) -> bool {
    matches!(
        field,
        "short_description" | "description" | "acceptance_criteria"
    )
}

fn policy_decisions_for(
    tool: &str,
    status: ResultStatus,
    error: Option<&ErrorRow>,
) -> Vec<PolicyDecisionRow> {
    let gates = match error.map(|error| error.code.as_str()) {
        Some("FIELD_REJECTED") => vec![("FieldAllowlist", "Deny")],
        Some("GUARD_FAILED") | Some("STATE_REQUIRED") => vec![("Resolution", "Deny")],
        Some("IDEMPOTENCY_CONFLICT") | Some("PENDING_RESOLUTION_REQUIRED") => {
            vec![("Idempotency", "Deny")]
        }
        Some("CONCURRENCY_CONFLICT") | Some("CONCURRENCY_TOKEN_INVALID") => {
            vec![("ConcurrencyToken", "Deny")]
        }
        Some("CONFIRMATION_INVALID") | Some("PLAN_EXPIRED") => vec![("Confirmation", "Deny")],
        Some("KILL_SWITCH") => vec![("EnvironmentGate", "Deny")],
        Some("RATE_LIMITED") => vec![("RateLimit", "Deny")],
        _ if tool == "confirmation_issue" || tool == "confirmation_consume" => {
            vec![("Confirmation", "Allow")]
        }
        _ if status == ResultStatus::AppliedSuccess => vec![
            ("Confirmation", "Allow"),
            ("Idempotency", "Allow"),
            ("ConcurrencyToken", "Allow"),
            ("FieldAllowlist", "Allow"),
            ("RateLimit", "Allow"),
        ],
        _ if status == ResultStatus::Plan => {
            vec![("FieldAllowlist", "Allow"), ("EnvironmentGate", "Allow")]
        }
        _ => Vec::new(),
    };

    gates
        .into_iter()
        .map(|(gate, verdict)| PolicyDecisionRow {
            gate: gate.to_string(),
            verdict: verdict.to_string(),
            reason: None,
            remediation: None,
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
async fn append_audit_event(
    state: &DaemonState,
    audit_id: &str,
    parent_audit_id: Option<&str>,
    tool: &str,
    status: ResultStatus,
    planned_changes: Option<Value>,
    service_now_metadata: Option<ServiceNowMetadata>,
    error: Option<ErrorRow>,
    identities: Option<(&str, &str)>,
    applied_changes: Option<Vec<AppliedChange>>,
) -> snow_mcp::Result<AuditEvent> {
    let stores = stores(state).map_err(snow_mcp::Error::Anyhow)?;
    let (actor, requester) = identities.unwrap_or_else(|| {
        let configured = state.core.config().instance.user.as_str();
        (configured, configured)
    });
    let mut event = AuditEvent::new_plan(
        audit_id,
        state.mcp_config.environment.label.clone(),
        audit_identity(actor),
        audit_identity(requester),
        ClientIdentity {
            client_id: Some("snow_daemon".to_string()),
            user_agent: None,
            transport: "daemon_json_rpc".to_string(),
        },
        tool,
    );
    event.parent_audit_id = parent_audit_id.map(ToOwned::to_owned);
    event.result_status = status;
    event.policy_decisions = policy_decisions_for(tool, status, error.as_ref());
    event.normalized_arguments_redacted = planned_changes.clone().unwrap_or(Value::Null);
    event.planned_changes = planned_changes;
    event.applied_changes = applied_changes;
    event.service_now_metadata = service_now_metadata;
    event.error = error;
    stores.audit_sink.append(event).await
}

fn audit_identity(subject: &str) -> ActorIdentity {
    ActorIdentity {
        subject: subject.to_string(),
        display_name: subject.to_string(),
        source_claim: "mcp_request".to_string(),
    }
}

fn story_actor_from_params(params: &Value, state: &DaemonState) -> StoryActor {
    match params.get("actor") {
        Some(Value::Object(object)) => StoryActor {
            subject: object
                .get("subject")
                .and_then(Value::as_str)
                .unwrap_or_else(|| state.core.config().instance.user.as_str())
                .to_string(),
            email: object
                .get("email")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            display_name: object
                .get("display_name")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
        },
        Some(Value::String(subject)) => StoryActor {
            subject: subject.clone(),
            email: None,
            display_name: None,
        },
        _ => StoryActor {
            subject: state.core.config().instance.user.clone(),
            email: None,
            display_name: None,
        },
    }
}

fn story_error(id: Option<Value>, code: i64, message: &str, data: Value) -> JsonRpcResponse {
    JsonRpcResponse::error(id, code, message, Some(data))
}

fn invalid_params(id: Option<Value>, err: impl ToString) -> JsonRpcResponse {
    JsonRpcResponse::error(
        id,
        -32602,
        "invalid params",
        Some(json!({ "details": err.to_string() })),
    )
}

fn internal_error(id: Option<Value>, err: impl ToString) -> JsonRpcResponse {
    JsonRpcResponse::error(
        id,
        -32000,
        "internal error",
        Some(json!({ "details": err.to_string() })),
    )
}

async fn plan_build_error_to_apply_response(
    state: &DaemonState,
    id: Option<Value>,
    tool: &str,
    err: PlanBuildError,
) -> JsonRpcResponse {
    match err {
        PlanBuildError::Invalid(field) => {
            audited_story_error(
                state,
                id,
                tool,
                -32051,
                "FIELD_REJECTED",
                json!({ "code": "FIELD_REJECTED", "fields": [{"field": field, "reason": "type_mismatch"}] }),
                ResultStatus::Denied,
            )
            .await
        }
        PlanBuildError::FieldRejected(fields) => {
            audited_story_error(
                state,
                id,
                tool,
                -32051,
                "FIELD_REJECTED",
                json!({ "code": "FIELD_REJECTED", "fields": fields }),
                ResultStatus::Denied,
            )
            .await
        }
        PlanBuildError::GuardFailed(failure) => {
            let data = serde_json::to_value(failure.to_guard_failed_data())
                .unwrap_or_else(|err| json!({ "details": err.to_string() }));
            audited_story_error(
                state,
                id,
                tool,
                -32050,
                "GUARD_FAILED",
                data,
                ResultStatus::Denied,
            )
            .await
        }
        PlanBuildError::StateRequired(data) => {
            audited_story_error(
                state,
                id,
                tool,
                -32062,
                "STATE_REQUIRED",
                data,
                ResultStatus::Denied,
            )
            .await
        }
        PlanBuildError::NotFound(message) => {
            audited_story_error(
                state,
                id,
                tool,
                -32004,
                "record not found",
                json!({ "details": message }),
                ResultStatus::Error,
            )
            .await
        }
        PlanBuildError::Upstream(err) => internal_error(id, err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use snow_core::{CacheSource, FieldValue, ResourceType};
    use std::collections::HashMap;

    fn plan_record_created_at(created_at: chrono::DateTime<Utc>) -> PlanStoreRecord {
        PlanStoreRecord {
            plan_id: "plan-1".to_string(),
            tool: "story_plan_create".to_string(),
            actor: "actor-1".to_string(),
            op_hash: "hash-1".to_string(),
            plan_json: json!({}),
            concurrency_token: None,
            created_at,
            expires_at: created_at + chrono::Duration::seconds(PLAN_TTL_SECONDS),
            state: PlanLifecycleState::Pending,
        }
    }

    fn story_record(fields: &[(&str, &str)]) -> SnowRecord {
        let mut field_map = HashMap::new();
        for (field, value) in fields {
            field_map.insert(
                (*field).to_string(),
                FieldValue {
                    value: (*value).to_string(),
                    display_value: None,
                },
            );
        }

        SnowRecord {
            sys_id: "story-sys".to_string(),
            number: "STRY001".to_string(),
            table: "rm_story".to_string(),
            resource_type: ResourceType::Story,
            state: String::new(),
            short_description: String::new(),
            description: String::new(),
            fields: field_map,
            work_notes: Vec::new(),
            comments: Vec::new(),
            parent: None,
            children: Vec::new(),
            references: HashMap::new(),
            synced_at: Utc::now(),
            source: CacheSource::Api,
        }
    }

    fn task_record(fields: &[(&str, &str)]) -> SnowRecord {
        let mut record = story_record(fields);
        record.sys_id = "task-sys".to_string();
        record.number = "STSK001".to_string();
        record.table = "rm_scrum_task".to_string();
        record.resource_type = ResourceType::ScrumTask;
        record
    }

    fn test_board_binding() -> BoardBinding {
        BoardBinding {
            name: "training-board".to_string(),
            instance_host: "https://example.service-now.com".to_string(),
            board_sys_id: "board-sys".to_string(),
            story_table: "rm_story".to_string(),
            task_table: "rm_scrum_task".to_string(),
            column_field: "sprint".to_string(),
            swim_lane_field: "epic".to_string(),
            assignment_group: "group-sys".to_string(),
            allowed_task_assignment_groups: Vec::new(),
            allowed_sprints: vec!["sprint-sys".to_string()],
            allow_production: false,
            allowed_story_states: vec!["1".to_string()],
            allowed_task_states: vec!["1".to_string()],
            allowed_priorities: Vec::new(),
        }
    }

    #[test]
    fn update_number_is_selector_not_blocked_field() {
        let args = Map::from_iter([("number".to_string(), json!("STRY001"))]);
        assert!(reject_blocked_fields(None, "story_plan_update", &args).is_none());
        assert!(reject_blocked_fields(None, "story_task_plan_update", &args).is_none());

        let args = Map::from_iter([("assignment_group".to_string(), json!("group-sys"))]);
        let error = reject_blocked_fields(None, "story_plan_update", &args)
            .expect("assignment_group should be blocked")
            .error
            .expect("error");
        assert_eq!(error.message, "FIELD_REJECTED");
        assert_eq!(error.code, -32051);
    }

    #[test]
    fn update_number_is_allowed_only_as_plan_selector() {
        let policy = snow_mcp::domain::policy::PolicyConfig::from_toml_str(
            r#"
[mcp.tools.story_plan_update]
enabled = true
requires_confirmation = false
story_board_id = "board-sys"
"#,
        )
        .expect("policy");
        let args = Map::from_iter([
            ("number".to_string(), json!("STRY001")),
            ("short_description".to_string(), json!("Updated")),
            ("u_custom".to_string(), json!("not governed")),
        ]);

        let plan_rejections = field_governance_rejections(
            "story_plan_update",
            &args,
            &policy,
            FieldGovernanceMode::PlanInput,
        );
        assert_eq!(
            plan_rejections,
            vec![json!({"field": "u_custom", "reason": "not_in_allowlist"})]
        );

        let write_rejections = field_governance_rejections(
            "story_apply_update",
            &args,
            &policy,
            FieldGovernanceMode::WritePayload,
        );
        assert_eq!(
            write_rejections,
            vec![
                json!({"field": "number", "reason": "blocked_deny_list"}),
                json!({"field": "u_custom", "reason": "not_in_allowlist"}),
            ]
        );
    }

    #[test]
    fn configured_empty_apply_allowlist_rejects_writable_fields() {
        let policy = snow_mcp::domain::policy::PolicyConfig::from_toml_str(
            r#"
[mcp.tools.story_apply_update]
enabled = true
requires_confirmation = true
story_board_id = "board-sys"
"#,
        )
        .expect("policy");
        let args = Map::from_iter([
            ("number".to_string(), json!("STRY001")),
            ("short_description".to_string(), json!("Updated")),
        ]);

        let rejections = field_governance_rejections(
            "story_plan_update",
            &args,
            &policy,
            FieldGovernanceMode::PlanInput,
        );
        assert_eq!(
            rejections,
            vec![json!({"field": "short_description", "reason": "not_in_allowlist"})]
        );
    }

    #[test]
    fn selector_and_metadata_fields_are_not_sent_to_table_api() {
        let mut story_update = json!({
            "number": "STRY001",
            "short_description": "Updated",
            "actor": {"subject": "casey"}
        });
        strip_non_writable_selector_fields("story_plan_update", &mut story_update);
        assert_eq!(story_update["short_description"], json!("Updated"));
        assert!(story_update.get("number").is_none());
        assert!(story_update.get("actor").is_none());

        let mut task_create = json!({
            "parent_story_number": "STRY001",
            "short_description": "Task",
            "requester": "casey"
        });
        strip_non_writable_selector_fields("story_task_plan_create", &mut task_create);
        assert_eq!(task_create["short_description"], json!("Task"));
        assert!(task_create.get("parent_story_number").is_none());
        assert!(task_create.get("requester").is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn apply_field_governance_rejects_tampered_number_in_update_plan() {
        let fixture = crate::test_support::build_fixture_state()
            .await
            .expect("fixture");
        let plan = OperationPlanBuilder::new("story_plan_update")
            .target(RecordRef {
                sys_id: "story-sys".to_string(),
                number: "STRY001".to_string(),
                table: "rm_story".to_string(),
            })
            .planned_changes(json!({
                "number": "STRY999",
                "short_description": "Updated",
            }))
            .build();

        let err = enforce_apply_field_governance(
            "story_apply_update",
            &plan,
            &test_board_binding(),
            &fixture.state,
            None,
        )
        .await
        .expect_err("number must not be writable");

        match err {
            PlanBuildError::FieldRejected(fields) => {
                assert_eq!(
                    fields,
                    vec![json!({"field": "number", "reason": "blocked_deny_list"})]
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn apply_field_governance_enforces_state_constraints() {
        let fixture = crate::test_support::build_fixture_state()
            .await
            .expect("fixture");
        let plan = OperationPlanBuilder::new("story_plan_update")
            .target(RecordRef {
                sys_id: "story-sys".to_string(),
                number: "STRY001".to_string(),
                table: "rm_story".to_string(),
            })
            .planned_changes(json!({
                "state": "cancelled",
            }))
            .build();

        let err = enforce_apply_field_governance(
            "story_apply_update",
            &plan,
            &test_board_binding(),
            &fixture.state,
            None,
        )
        .await
        .expect_err("state outside the board choices must be rejected");

        match err {
            PlanBuildError::FieldRejected(fields) => {
                assert_eq!(
                    fields,
                    vec![json!({"field": "state", "reason": "value_not_in_enum"})]
                );
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn update_plan_does_not_default_assignee() {
        let fixture = crate::test_support::build_fixture_state()
            .await
            .expect("fixture");
        let actor = StoryActor {
            subject: "actor-1".to_string(),
            email: None,
            display_name: None,
        };
        let mut payload = json!({
            "state": "3",
        });

        let warnings = resolve_assignee_in_payload(
            "story_task_plan_update",
            &mut payload,
            &actor,
            &fixture.state,
        )
        .await
        .expect("assignee resolution");

        assert!(warnings.is_empty());
        assert!(payload.get("assigned_to").is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn apply_guard_rechecks_story_board_scope() {
        let fixture = crate::test_support::build_fixture_state()
            .await
            .expect("fixture");
        let plan = OperationPlanBuilder::new("story_plan_create")
            .planned_changes(json!({
                "short_description": "Out of scope",
                "description": "Wrong board",
                "assignment_group": "other-group",
                "sprint": "sprint-sys",
                "active": true,
            }))
            .build();

        let failure = enforce_apply_guard(
            "story_apply_create",
            &plan,
            &test_board_binding(),
            &fixture.state,
        )
        .await
        .expect_err("apply must re-check board scope");

        assert!(matches!(
            failure,
            InScopeFailure::WrongAssignmentGroup { .. }
        ));
    }

    #[test]
    fn task_update_scope_uses_task_record_not_parent_story_state() {
        let task = task_record(&[
            ("assignment_group", "group-sys"),
            ("active", "false"),
            ("state", "3"),
        ]);

        task_record_in_scope(&task, &test_board_binding())
            .expect("task update scope should not require an active parent Story");
    }

    #[test]
    fn task_update_scope_rejects_wrong_assignment_group() {
        let task = task_record(&[("assignment_group", "other-group")]);
        let failure = task_record_in_scope(&task, &test_board_binding())
            .expect_err("task update scope must remain board-bound");

        assert!(matches!(
            failure,
            InScopeFailure::WrongAssignmentGroup { .. }
        ));
    }

    #[test]
    fn task_update_scope_allows_configured_task_assignment_group() {
        let task = task_record(&[("assignment_group", "task-group")]);
        let mut binding = test_board_binding();
        binding.allowed_task_assignment_groups = vec!["task-group".to_string()];

        task_record_in_scope(&task, &binding)
            .expect("task update scope should allow configured task groups");
    }

    #[test]
    fn task_create_scope_still_requires_parent_story_in_scope() {
        let parent = story_record(&[
            ("assignment_group", "group-sys"),
            ("sprint", "sprint-sys"),
            ("active", "false"),
        ]);
        let payload = json!({
            "story": "story-sys",
            "short_description": "Task"
        });

        let failure = enforce_task_parent_payload_scope(&payload, &parent, &test_board_binding())
            .expect_err("task create must still be scoped by the parent Story");

        assert!(matches!(
            failure,
            PlanBuildError::GuardFailed(InScopeFailure::TaskParentOutOfScope { .. })
        ));
    }

    #[test]
    fn priority_choice_with_cancel_label_is_constrained_even_when_raw_value_matches() {
        let choices = vec![FieldChoice {
            label: "Cancelled".to_string(),
            value: "4".to_string(),
            terminal: true,
        }];

        assert_eq!(
            match_choice_validation("4", choices, true),
            ChoiceValidation::Constrained
        );
    }

    #[test]
    fn create_recovery_window_uses_spec_clock_skew() {
        let created_at = Utc.with_ymd_and_hms(2026, 5, 20, 12, 0, 0).unwrap();
        let record = plan_record_created_at(created_at);

        assert_eq!(
            create_recovery_created_after(&record),
            "2026-05-20 11:55:00"
        );
    }

    #[test]
    fn pending_create_no_match_retries_and_ambiguous_requires_operator() {
        assert_eq!(
            classify_create_recovery_lookup(&CreateRecoveryLookup::NoMatch),
            CreatePendingDecision::Proceed
        );
        assert_eq!(
            classify_create_recovery_lookup(&CreateRecoveryLookup::Ambiguous),
            CreatePendingDecision::NeedsOperator
        );
    }

    #[test]
    fn pending_update_distinguishes_applied_from_unchanged_token() {
        let record = story_record(&[
            ("priority", "2"),
            ("sys_updated_on", "2026-05-20 12:00:00"),
            ("sys_mod_count", "7"),
        ]);
        let expected_token = ConcurrencyToken {
            sys_updated_on: "2026-05-20 12:00:00".to_string(),
            sys_mod_count: Some(7),
        };

        assert_eq!(
            classify_update_recovery_record(
                &record,
                &json!({ "priority": "2" }),
                Some(&expected_token)
            ),
            UpdatePendingDecision::AlreadyApplied
        );
        assert_eq!(
            classify_update_recovery_record(
                &record,
                &json!({ "priority": "3" }),
                Some(&expected_token)
            ),
            UpdatePendingDecision::ProceedUnchangedToken
        );

        let advanced_token = ConcurrencyToken {
            sys_updated_on: "2026-05-20 12:01:00".to_string(),
            sys_mod_count: Some(8),
        };
        assert_eq!(
            classify_update_recovery_record(
                &record,
                &json!({ "priority": "3" }),
                Some(&advanced_token)
            ),
            UpdatePendingDecision::NeedsOperator
        );
    }

    #[test]
    fn audit_warnings_keep_only_full_email_hash_for_d5_warning_data() {
        let warnings = vec![ReceiptWarning {
            code: WARNING_ASSIGNEE_DEFAULTED_FROM_CALLER.to_string(),
            field: Some("assigned_to".to_string()),
            message:
                "Substituted caller identity (first.last@<sha256:a379a6f6>) for omitted assignee."
                    .to_string(),
            data: Some(json!({
                "email_local_part": "first.last",
                "domain_hash": "a379a6f6",
            })),
        }];

        let audit_values = audit_warnings(&warnings, &["full-email-hash".to_string()]);
        assert_eq!(
            audit_values,
            vec![json!({
                "code": WARNING_ASSIGNEE_DEFAULTED_FROM_CALLER,
                "field": "assigned_to",
                "data": { "email_sha256": "full-email-hash" },
            })]
        );
        let serialized = serde_json::to_string(&audit_values).expect("serialize audit warnings");
        assert!(!serialized.contains("email_local_part"));
        assert!(!serialized.contains("domain_hash"));
        assert!(!serialized.contains("first.last"));
        assert!(!serialized.contains("a379a6f6"));
    }
}
