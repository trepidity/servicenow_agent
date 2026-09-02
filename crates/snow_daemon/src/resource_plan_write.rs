use anyhow::Result;
use chrono::{NaiveDate, Utc};
use serde_json::{Map, Value, json};
use servicenow_rs::prelude::Error as SnowApiError;
use sha2::{Digest, Sha256};
use snow_core::{
    ResourcePlanDecision, ResourcePlanParentType, ResourcePlanResource, ResourcePlanResourceType,
    ResourcePlanState, ResourcePlanWriteResult, SnowRecord,
};
use snow_mcp::audit::{AuditSink, SqliteAuditSink};
use snow_mcp::domain::audit::{
    ActorIdentity, AppliedChange, AuditEvent, ClientIdentity, ErrorRow, PolicyDecisionRow,
    ResultStatus, ServiceNowMetadata,
};
use snow_mcp::domain::primitives::{IdempotencyKey, IdempotencyKeySource, RecordRef};
use snow_mcp::planner::{
    ConcurrencyToken, ConfirmationBinding, ConfirmationConsumeError, ConfirmationRecord,
    ConfirmationStore, FieldChange, IdempotencyOutcome, IdempotencyRecord, IdempotencyStore,
    OperationPlan, OperationPlanBuilder, OperationReceipt, PlanLifecycleState, PlanStore,
    PlanStoreRecord, ReceiptStatus, ReceiptWarning, SqliteConfirmationStore,
    SqliteIdempotencyStore, SqlitePlanStore,
};
use std::collections::BTreeSet;
use uuid::Uuid;

use crate::DaemonState;
use crate::rpc::JsonRpcResponse;

const PLAN_TTL_SECONDS: i64 = 600;

pub async fn handle_resource_plan_plan(
    id: Option<Value>,
    tool: &str,
    params: &Value,
    state: &DaemonState,
) -> JsonRpcResponse {
    if !state
        .mcp_config
        .policy
        .tool_enabled_in_environment(tool, &state.mcp_config.environment.label)
    {
        return field_rejected_response(
            id,
            vec![json!({"field": "tool", "reason": "blocked_deny_list"})],
        );
    }
    let apply_tool = apply_tool_for_plan_tool(tool);
    if !state
        .mcp_config
        .policy
        .tool_enabled_in_environment(apply_tool, &state.mcp_config.environment.label)
    {
        return rpc_error(
            id,
            -32040,
            "policy denied",
            json!({
                "details": "matching Resource Plan apply tool is disabled by current MCP policy",
                "tool": apply_tool,
            }),
        );
    }

    let Some(args) = params.as_object() else {
        return field_rejected_response(
            id,
            vec![json!({"field": "payload", "reason": "type_mismatch"})],
        );
    };
    if let Some(response) = reject_plan_input_fields(id.clone(), tool, args, state) {
        return response;
    }

    let plan_input = match build_plan_input(tool, args, state).await {
        Ok(input) => input,
        Err(PlanBuildError::Invalid(field)) => {
            return field_rejected_response(
                id,
                vec![json!({"field": field, "reason": "type_mismatch"})],
            );
        }
        Err(PlanBuildError::FieldRejected(fields)) => return field_rejected_response(id, fields),
        Err(PlanBuildError::NotFound(message)) => {
            return rpc_error(
                id,
                -32004,
                "record not found",
                json!({ "details": message }),
            );
        }
        Err(PlanBuildError::WrongType { selector, table }) => {
            return rpc_error(
                id,
                -32051,
                "FIELD_REJECTED",
                json!({
                    "code": "FIELD_REJECTED",
                    "fields": [{"field": selector, "reason": format!("wrong_table:{table}")}],
                }),
            );
        }
        Err(PlanBuildError::TerminalRecord(number)) => {
            return rpc_error(
                id,
                -32050,
                "GUARD_FAILED",
                json!({
                    "code": "GUARD_FAILED",
                    "reason": "terminal_record_skipped",
                    "number": number,
                }),
            );
        }
        Err(PlanBuildError::GuardFailed {
            number,
            reason,
            current_state,
        }) => {
            return rpc_error(
                id,
                -32050,
                "GUARD_FAILED",
                json!({
                    "code": "GUARD_FAILED",
                    "reason": reason,
                    "number": number,
                    "current_state": current_state,
                }),
            );
        }
        Err(PlanBuildError::Upstream(err)) => return internal_error(id, err),
    };

    let plan = plan_input.builder.build();
    let now = Utc::now();
    let expires_at = now + chrono::Duration::seconds(PLAN_TTL_SECONDS);
    let actor = actor_from_params(params, state);
    let requester = requester_from_params(params, &actor);
    let idempotency_key = Uuid::new_v4().to_string();

    let mut plan_json = match serde_json::to_value(&plan) {
        Ok(value) => value,
        Err(err) => return internal_error(id, err),
    };
    if let Value::Object(object) = &mut plan_json {
        object.insert(
            "apply_tool".to_string(),
            Value::String(apply_tool.to_string()),
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
            return rpc_error(
                id,
                -32052,
                "IDEMPOTENCY_CONFLICT",
                json!({
                    "code": "IDEMPOTENCY_CONFLICT",
                    "idempotency_key": idempotency_key,
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
        Some(plan.planned_changes.clone()),
        None,
        None,
        Some((&actor, &requester)),
        None,
    )
    .await
    {
        return internal_error(id, err);
    }

    let confirmation = match stores
        .confirmation_store
        .issue(
            &plan.plan_id,
            ConfirmationBinding {
                actor,
                requester,
                tool: apply_tool.to_string(),
                op_hash: plan.op_hash.clone(),
                environment: state.mcp_config.environment.label.clone(),
            },
            PLAN_TTL_SECONDS as u64,
        )
        .await
    {
        Ok(confirmation) => confirmation,
        Err(err) => return internal_error(id, err),
    };

    let mut result = json!({
        "plan_id": plan.plan_id,
        "op_hash": plan.op_hash,
        "preview": plan_input.preview,
        "expires_at": expires_at.to_rfc3339(),
        "confirmation_token": confirmation.token_id,
        "idempotency_key": idempotency_key,
    });
    if let Some(token) = plan_input.concurrency_token {
        result["concurrency_token"] = json!(token);
    }
    JsonRpcResponse::ok(id, result)
}

pub async fn handle_resource_plan_apply(
    id: Option<Value>,
    tool: &str,
    params: &Value,
    state: &DaemonState,
) -> JsonRpcResponse {
    if is_kill_switched() {
        return audited_error(
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
    if !state
        .mcp_config
        .policy
        .tool_enabled_in_environment(tool, &state.mcp_config.environment.label)
    {
        return audited_error(
            state,
            id,
            tool,
            -32051,
            "FIELD_REJECTED",
            json!({"code": "FIELD_REJECTED", "fields": [{"field": "tool", "reason": "blocked_deny_list"}]}),
            ResultStatus::Denied,
        )
        .await;
    }

    let Some(plan_id) = string_param(params, "plan_id") else {
        return field_rejected_apply(state, id, tool, "plan_id").await;
    };
    let Some(confirmation_token) = string_param(params, "confirmation_token") else {
        return field_rejected_apply(state, id, tool, "confirmation_token").await;
    };
    let Some(idempotency_key) = string_param(params, "idempotency_key") else {
        return field_rejected_apply(state, id, tool, "idempotency_key").await;
    };

    let stores = match stores(state) {
        Ok(stores) => stores,
        Err(err) => return internal_error(id, err),
    };
    let plan_record = match stores.plan_store.get(&plan_id).await {
        Ok(Some(record)) => record,
        Ok(None) => {
            return audited_error(
                state,
                id,
                tool,
                -32056,
                "PLAN_NOT_FOUND",
                json!({"code": "PLAN_NOT_FOUND", "plan_id": plan_id}),
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
    if apply_tool_for_plan_tool(&plan.tool) != tool {
        return invalid_params(
            id,
            format!(
                "plan {} is bound to {}, not {tool}",
                plan.plan_id,
                apply_tool_for_plan_tool(&plan.tool)
            ),
        );
    }
    if plan_record.state == PlanLifecycleState::Expired || plan_record.expires_at <= Utc::now() {
        let _ = stores.plan_store.mark_expired(&plan_id).await;
        return audited_error(
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

    let actor = actor_from_params(params, state);
    let requester = requester_from_params(params, &actor);
    let binding = ConfirmationBinding {
        actor: actor.clone(),
        requester: requester.clone(),
        tool: tool.to_string(),
        op_hash: plan.op_hash.clone(),
        environment: state.mcp_config.environment.label.clone(),
    };
    let key = IdempotencyKey {
        value: idempotency_key.clone(),
        source: IdempotencyKeySource::ServerDerived,
    };

    if let Ok(Some(record)) = stores.idempotency_store.lookup_record(&key, tool)
        && record.expires_at > Utc::now()
    {
        if record.op_hash != plan.op_hash {
            return audited_error(
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
                &binding,
            ) {
                return confirmation_invalid(state, id, tool, err).await;
            }
            receipt.idempotency_replay = true;
            return JsonRpcResponse::ok(id, json!(receipt));
        }
        if record.apply_started_at.is_some() {
            return pending_resolution_required(state, id, tool, &plan, &record).await;
        }
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
        Ok(IdempotencyOutcome::NewKey) | Ok(IdempotencyOutcome::Pending { .. }) => {}
        Ok(IdempotencyOutcome::Replay(mut receipt)) => {
            if let Err(err) = validate_replay_confirmation(
                &stores.confirmation_store,
                &confirmation_token,
                &binding,
            ) {
                return confirmation_invalid(state, id, tool, err).await;
            }
            receipt.idempotency_replay = true;
            return JsonRpcResponse::ok(id, json!(receipt));
        }
        Ok(IdempotencyOutcome::Conflict { .. }) => {
            return audited_error(
                state,
                id,
                tool,
                -32052,
                "IDEMPOTENCY_CONFLICT",
                json!({
                    "code": "IDEMPOTENCY_CONFLICT",
                    "idempotency_key": idempotency_key,
                    "attempted_op_hash": plan.op_hash,
                }),
                ResultStatus::Denied,
            )
            .await;
        }
        Err(err) => return internal_error(id, err),
    }

    if let Err(fields) = apply_field_rejections(tool, &plan, state) {
        return audited_error(
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

    let before_record = if is_update_tool(tool) {
        match plan.target.as_ref() {
            Some(target) => match state
                .core
                .get_record_by_table_sys_id_fresh(ResourcePlanResource::TABLE, &target.sys_id)
                .await
            {
                Ok(record) => record,
                Err(err) => return internal_error(id, err),
            },
            None => None,
        }
    } else {
        None
    };
    if let Some(expected) = &plan_record.concurrency_token {
        let Some(supplied) = params
            .get("concurrency_token")
            .cloned()
            .and_then(|value| serde_json::from_value::<ConcurrencyToken>(value).ok())
        else {
            return audited_error(
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
            return audited_error(
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
        if let Some(current) = before_record.as_ref()
            && let Some(observed) = concurrency_from_record(current)
            && &observed != expected
        {
            return audited_error(
                state,
                id,
                tool,
                -32053,
                "CONCURRENCY_CONFLICT",
                json!({
                    "code": "CONCURRENCY_CONFLICT",
                    "record_sys_id": current.sys_id,
                    "expected": expected,
                    "observed": observed,
                }),
                ResultStatus::Denied,
            )
            .await;
        }
    }

    match stores.confirmation_store.lookup(&confirmation_token) {
        Ok(Some(record)) => {
            if let Err(err) = validate_confirmation(&record, &binding) {
                return confirmation_invalid(state, id, tool, err).await;
            }
        }
        Ok(None) => {
            return confirmation_invalid(state, id, tool, ConfirmationConsumeError::NotFound).await;
        }
        Err(err) => return internal_error(id, err),
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
            let (code, message, data) = if is_permission_denied_error(&err) {
                (
                    -32063,
                    "UPSTREAM_PERMISSION_DENIED",
                    upstream_error_data(tool, &plan, err.to_string(), "UPSTREAM_PERMISSION_DENIED"),
                )
            } else {
                (
                    -32059,
                    "UPSTREAM_ERROR",
                    upstream_error_data(tool, &plan, err.to_string(), "UPSTREAM_ERROR"),
                )
            };
            return audited_error(state, id, tool, code, message, data, ResultStatus::Error).await;
        }
    };
    if let Err(err) = stores
        .confirmation_store
        .consume(&confirmation_token, &binding)
        .await
    {
        return confirmation_invalid(state, id, tool, err).await;
    }

    let audit_id = Uuid::new_v4().to_string();
    let receipt = receipt_for_write(
        &plan,
        tool,
        &audit_id,
        apply_started_at,
        write_result,
        before_record.as_ref(),
        state,
    )
    .await;
    let (audit_summary, applied_changes) = audit_summary_for_receipt(&receipt);
    let audit_status = if receipt.status == ReceiptStatus::Success {
        ResultStatus::AppliedSuccess
    } else {
        ResultStatus::AppliedPartial
    };
    if let Err(err) = append_audit_event(
        state,
        &audit_id,
        Some(plan.plan_id.as_str()),
        tool,
        audit_status,
        Some(audit_summary),
        receipt.service_now_metadata.clone(),
        None,
        Some((&actor, &requester)),
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
    let path = state.data_dir.join("mcp_story_write.sqlite3");
    Ok(DaemonStores {
        plan_store: SqlitePlanStore::open(&path)?,
        confirmation_store: SqliteConfirmationStore::open(&path)?,
        idempotency_store: SqliteIdempotencyStore::open(&path)?,
        audit_sink: SqliteAuditSink::open(&path)?,
    })
}

struct PlanInput {
    builder: OperationPlanBuilder,
    concurrency_token: Option<ConcurrencyToken>,
    preview: Value,
}

#[derive(Debug)]
enum PlanBuildError {
    Invalid(String),
    FieldRejected(Vec<Value>),
    NotFound(String),
    WrongType {
        selector: String,
        table: String,
    },
    TerminalRecord(String),
    GuardFailed {
        number: String,
        reason: &'static str,
        current_state: Option<String>,
    },
    Upstream(anyhow::Error),
}

async fn build_plan_input(
    tool: &str,
    args: &Map<String, Value>,
    state: &DaemonState,
) -> std::result::Result<PlanInput, PlanBuildError> {
    match tool {
        "resource_plan_plan_create" => build_create_plan_input(tool, args, state).await,
        "resource_plan_plan_update" => build_update_plan_input(tool, args, state).await,
        "resource_plan_plan_decision" => build_decision_plan_input(tool, args, state).await,
        _ => Err(PlanBuildError::Invalid(format!(
            "unsupported Resource Plan tool {tool}"
        ))),
    }
}

async fn build_create_plan_input(
    tool: &str,
    args: &Map<String, Value>,
    state: &DaemonState,
) -> std::result::Result<PlanInput, PlanBuildError> {
    let parent_sys_id = require_string(args, "parent_sys_id")?;
    let resource_sys_id = require_string(args, "resource_sys_id")?;
    let parent_type = ResourcePlanParentType::parse(&require_string(args, "parent_type")?)
        .ok_or_else(|| field_rejection("parent_type", "value_not_in_enum"))?;
    let resource_type = ResourcePlanResourceType::parse(&require_string(args, "resource_type")?)
        .ok_or_else(|| field_rejection("resource_type", "value_not_in_enum"))?;
    let state_value = parse_state(args.get("state"), "state")?;
    let planned_hours = parse_planned_hours(args.get("planned_hours"), "planned_hours")?;
    validate_optional_string(args, "notes")?;
    validate_optional_date(args, "start_date")?;
    validate_optional_date(args, "end_date")?;

    let parent = state
        .core
        .get_record_by_table_sys_id_fresh(parent_type.table_name(), &parent_sys_id)
        .await
        .map_err(PlanBuildError::Upstream)?
        .ok_or_else(|| {
            PlanBuildError::NotFound(format!(
                "{} parent {} not found",
                parent_type.table_name(),
                parent_sys_id
            ))
        })?;
    require_table(&parent, parent_type.table_name(), "parent_sys_id")?;

    let mut payload = Map::new();
    payload.insert("task".to_string(), json!(parent_sys_id));
    payload.insert(
        resource_assignment_field(resource_type).to_string(),
        json!(resource_sys_id),
    );
    payload.insert(
        "resource_type".to_string(),
        json!(resource_type_value(resource_type)),
    );
    payload.insert("state".to_string(), json!(state_value.raw().to_string()));
    payload.insert("planned_hours".to_string(), json!(planned_hours));
    copy_optional_field(args, &mut payload, "notes");
    copy_optional_field(args, &mut payload, "start_date");
    copy_optional_field(args, &mut payload, "end_date");

    Ok(PlanInput {
        builder: OperationPlanBuilder::new(tool)
            .target(record_ref_from_snow(&parent))
            .planned_changes(Value::Object(payload)),
        concurrency_token: None,
        preview: create_preview(
            args,
            parent_type,
            resource_type,
            &parent_sys_id,
            &resource_sys_id,
            state_value,
            planned_hours,
        ),
    })
}

async fn build_update_plan_input(
    tool: &str,
    args: &Map<String, Value>,
    state: &DaemonState,
) -> std::result::Result<PlanInput, PlanBuildError> {
    let has_sys_id = non_empty_arg(args, "sys_id");
    let has_number = non_empty_arg(args, "number");
    match (has_sys_id, has_number) {
        (true, true) | (false, false) => {
            return Err(PlanBuildError::FieldRejected(vec![json!({
                "field": "identity",
                "reason": "exactly_one_required",
            })]));
        }
        _ => {}
    }
    let record = if has_sys_id {
        let sys_id = require_string(args, "sys_id")?;
        state
            .core
            .get_record_by_table_sys_id_fresh(ResourcePlanResource::TABLE, &sys_id)
            .await
            .map_err(PlanBuildError::Upstream)?
            .ok_or_else(|| PlanBuildError::NotFound(format!("resource_plan {sys_id} not found")))?
    } else {
        let number = require_string(args, "number")?;
        state
            .core
            .get_record_fresh(&number)
            .await
            .map_err(PlanBuildError::Upstream)?
            .ok_or_else(|| PlanBuildError::NotFound(format!("record {number} not found")))?
    };
    require_table(&record, ResourcePlanResource::TABLE, "identity")?;
    if state
        .mcp_config
        .policy
        .tools
        .get(apply_tool_for_plan_tool(tool))
        .is_some_and(|policy| policy.skip_terminal_records)
        && record_is_terminal(&record)
    {
        return Err(PlanBuildError::TerminalRecord(record.number));
    }

    let mut payload = Map::new();
    if args.contains_key("state") {
        payload.insert(
            "state".to_string(),
            json!(parse_state(args.get("state"), "state")?.raw().to_string()),
        );
    }
    if args.contains_key("planned_hours") {
        payload.insert(
            "planned_hours".to_string(),
            json!(parse_planned_hours(
                args.get("planned_hours"),
                "planned_hours"
            )?),
        );
    }
    for field in ["notes", "start_date", "end_date"] {
        if args.contains_key(field) {
            if field == "notes" {
                validate_optional_string(args, field)?;
            } else {
                validate_optional_date(args, field)?;
            }
            copy_optional_field(args, &mut payload, field);
        }
    }
    if payload.is_empty() {
        return Err(PlanBuildError::FieldRejected(vec![json!({
            "field": "planned_changes",
            "reason": "empty_payload",
        })]));
    }
    let preview = json!({
        "target": {"sys_id": record.sys_id, "number": record.number},
        "current": current_fields(&record, payload.keys()),
        "proposed": proposed_fields(&record, &payload),
        "diff": diff_fields(&record, &payload),
    });

    Ok(PlanInput {
        builder: OperationPlanBuilder::new(tool)
            .target(record_ref_from_snow(&record))
            .planned_changes(Value::Object(payload)),
        concurrency_token: concurrency_from_record(&record),
        preview,
    })
}

async fn build_decision_plan_input(
    tool: &str,
    args: &Map<String, Value>,
    state: &DaemonState,
) -> std::result::Result<PlanInput, PlanBuildError> {
    let number = require_string(args, "number")?;
    let decision = match require_string(args, "decision")?.as_str() {
        "confirm" => ResourcePlanDecision::Confirm,
        "confirm_and_allocate" => ResourcePlanDecision::ConfirmAndAllocate,
        _ => return Err(field_rejection("decision", "value_not_in_enum")),
    };
    let record = state
        .core
        .get_record_fresh(&number)
        .await
        .map_err(PlanBuildError::Upstream)?
        .ok_or_else(|| PlanBuildError::NotFound(format!("record {number} not found")))?;
    require_table(&record, ResourcePlanResource::TABLE, "number")?;

    let current_state = resource_plan_state_from_record(&record);
    if current_state != Some(ResourcePlanState::Requested) {
        return Err(PlanBuildError::GuardFailed {
            number: record.number,
            reason: "decision_requires_requested",
            current_state: current_state
                .and_then(ResourcePlanState::label)
                .map(ToOwned::to_owned),
        });
    }

    let target_state = decision.target_state();
    let payload = json!({"state": target_state.raw().to_string()});
    let preview = json!({
        "target": {"sys_id": record.sys_id, "number": record.number},
        "decision": decision,
        "current_state": {
            "value": ResourcePlanState::Requested.raw().to_string(),
            "label": ResourcePlanState::Requested.label(),
        },
        "expected_state": {
            "value": target_state.raw().to_string(),
            "label": target_state.label(),
        },
        "expected_allocation": {
            "booking_type": decision.expected_booking_type(),
            "effect": match decision {
                ResourcePlanDecision::Confirm => "soft_allocations_created",
                ResourcePlanDecision::ConfirmAndAllocate => "hard_allocations_created",
            },
        },
    });

    Ok(PlanInput {
        builder: OperationPlanBuilder::new(tool)
            .target(record_ref_from_snow(&record))
            .planned_changes(payload),
        concurrency_token: concurrency_from_record(&record),
        preview,
    })
}

fn resource_plan_state_from_record(record: &SnowRecord) -> Option<ResourcePlanState> {
    record
        .fields
        .get("state")
        .map(|field| field.value.as_str())
        .filter(|value| !value.trim().is_empty())
        .or_else(|| (!record.state.trim().is_empty()).then_some(record.state.as_str()))
        .and_then(ResourcePlanState::parse)
}

fn non_empty_arg(args: &Map<String, Value>, field: &str) -> bool {
    args.get(field)
        .and_then(Value::as_str)
        .is_some_and(|value| !value.trim().is_empty())
}

fn create_preview(
    args: &Map<String, Value>,
    parent_type: ResourcePlanParentType,
    resource_type: ResourcePlanResourceType,
    parent_sys_id: &str,
    resource_sys_id: &str,
    state: ResourcePlanState,
    planned_hours: f64,
) -> Value {
    let mut preview = json!({
        "parent_sys_id": parent_sys_id,
        "parent_type": parent_type_value(parent_type),
        "parent_table": parent_type.table_name(),
        "task": parent_sys_id,
        "resource_sys_id": resource_sys_id,
        "resource_type": resource_type_value(resource_type),
        "state": state.raw().to_string(),
        "planned_hours": planned_hours,
    });
    if let Some(label) = state.label() {
        preview["state_label"] = json!(label);
    }
    for field in ["notes", "start_date", "end_date"] {
        if let Some(value) = args.get(field) {
            preview[field] = value.clone();
        }
    }
    preview
}

fn current_fields<'a>(
    record: &SnowRecord,
    fields: impl Iterator<Item = &'a String>,
) -> Map<String, Value> {
    fields
        .map(|field| {
            (
                field.clone(),
                record_field_value(record, field).unwrap_or(Value::Null),
            )
        })
        .collect()
}

fn proposed_fields(record: &SnowRecord, payload: &Map<String, Value>) -> Map<String, Value> {
    let mut proposed = current_fields(record, payload.keys());
    for (field, value) in payload {
        proposed.insert(field.clone(), value.clone());
        if field == "state"
            && let Some(state) = value.as_str().and_then(ResourcePlanState::parse)
            && let Some(label) = state.label()
        {
            proposed.insert("state_label".to_string(), json!(label));
        }
    }
    proposed
}

fn diff_fields(record: &SnowRecord, payload: &Map<String, Value>) -> Vec<Value> {
    payload
        .iter()
        .filter_map(|(field, after)| {
            let before = record_field_value(record, field);
            before
                .as_ref()
                .is_none_or(|before| !field_values_equal(field, before, after))
                .then(|| {
                    json!({
                        "field": field,
                        "from": before,
                        "to": after,
                    })
                })
        })
        .collect()
}

fn parse_state(
    value: Option<&Value>,
    field: &str,
) -> std::result::Result<ResourcePlanState, PlanBuildError> {
    value
        .and_then(Value::as_str)
        .and_then(ResourcePlanState::parse)
        .ok_or_else(|| field_rejection(field, "value_not_in_enum"))
}

fn parse_planned_hours(
    value: Option<&Value>,
    field: &str,
) -> std::result::Result<f64, PlanBuildError> {
    let Some(value) = value.and_then(Value::as_f64) else {
        return Err(PlanBuildError::Invalid(field.to_string()));
    };
    if value.is_finite() && value > 0.0 {
        Ok(value)
    } else {
        Err(field_rejection(field, "value_constrained"))
    }
}

fn validate_optional_string(
    args: &Map<String, Value>,
    field: &str,
) -> std::result::Result<(), PlanBuildError> {
    if !args.contains_key(field) {
        return Ok(());
    }
    args.get(field)
        .and_then(Value::as_str)
        .map(|_| ())
        .ok_or_else(|| PlanBuildError::Invalid(field.to_string()))
}

fn validate_optional_date(
    args: &Map<String, Value>,
    field: &str,
) -> std::result::Result<(), PlanBuildError> {
    if !args.contains_key(field) {
        return Ok(());
    }
    let Some(value) = args.get(field).and_then(Value::as_str) else {
        return Err(PlanBuildError::Invalid(field.to_string()));
    };
    NaiveDate::parse_from_str(value.trim(), "%Y-%m-%d")
        .map(|_| ())
        .map_err(|_| PlanBuildError::Invalid(field.to_string()))
}

fn copy_optional_field(args: &Map<String, Value>, payload: &mut Map<String, Value>, field: &str) {
    if let Some(value) = args.get(field) {
        payload.insert(field.to_string(), value.clone());
    }
}

fn field_rejection(field: &str, reason: &str) -> PlanBuildError {
    PlanBuildError::FieldRejected(vec![json!({"field": field, "reason": reason})])
}

fn require_table(
    record: &SnowRecord,
    expected: &str,
    selector: &str,
) -> std::result::Result<(), PlanBuildError> {
    if record.table == expected {
        Ok(())
    } else {
        Err(PlanBuildError::WrongType {
            selector: selector.to_string(),
            table: record.table.clone(),
        })
    }
}

fn record_is_terminal(record: &SnowRecord) -> bool {
    let state = record
        .fields
        .get("state")
        .map(|field| {
            format!(
                "{} {}",
                field.value.to_ascii_lowercase(),
                field
                    .display_value
                    .as_deref()
                    .unwrap_or_default()
                    .to_ascii_lowercase()
            )
        })
        .unwrap_or_else(|| record.state.to_ascii_lowercase());
    ["closed", "complete", "completed", "cancel", "cancelled"]
        .iter()
        .any(|terminal| state.contains(terminal))
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
    (!fields.is_empty()).then(|| field_rejected_response(id, fields))
}

fn apply_field_rejections(
    tool: &str,
    plan: &OperationPlan,
    state: &DaemonState,
) -> std::result::Result<(), Vec<Value>> {
    let payload = plan
        .planned_changes
        .as_object()
        .ok_or_else(|| vec![json!({"field": "planned_changes", "reason": "type_mismatch"})])?;
    let fields = field_governance_rejections(
        tool,
        payload,
        &state.mcp_config.policy,
        FieldGovernanceMode::WritePayload,
    );
    if fields.is_empty() {
        Ok(())
    } else {
        Err(fields)
    }
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
            fields.push(json!({"field": field, "reason": "blocked_deny_list"}));
        }
    }
    let apply_tool = apply_tool_for_plan_tool(tool);
    let allowlist = field_allowlist_for_apply_tool(policy, apply_tool);
    for field in args.keys() {
        if rejected.contains(field)
            || metadata_fields().contains(&field.as_str())
            || (mode == FieldGovernanceMode::PlanInput
                && plan_selector_fields(tool).contains(&field.as_str()))
        {
            continue;
        }
        if !allowlist.contains(field) {
            fields.push(json!({"field": field, "reason": "not_in_allowlist"}));
        }
    }
    fields
}

fn field_allowlist_for_apply_tool(
    policy: &snow_mcp::domain::policy::PolicyConfig,
    apply_tool: &str,
) -> BTreeSet<String> {
    policy
        .tools
        .get(apply_tool)
        .map(|tool_policy| tool_policy.field_allowlist.clone())
        .or_else(|| {
            snow_mcp::domain::policy::PolicyConfig::read_only_default()
                .tools
                .get(apply_tool)
                .map(|tool_policy| tool_policy.field_allowlist.clone())
        })
        .unwrap_or_default()
}

fn blocked_fields_for_tool(tool: &str) -> BTreeSet<&'static str> {
    let mut blocked = BTreeSet::from([
        "sys_class_name",
        "sys_created_on",
        "sys_created_by",
        "sys_updated_on",
        "sys_updated_by",
        "sys_mod_count",
        "parent",
        "parent_table",
        "year",
        "quarter",
        "work_notes",
    ]);
    if tool == "resource_plan_plan_create" {
        blocked.extend([
            "sys_id",
            "number",
            "task",
            "resource",
            "group_resource",
            "user_resource",
        ]);
    }
    if tool == "resource_plan_plan_update" {
        blocked.extend([
            "state",
            "task",
            "resource",
            "resource_type",
            "parent_sys_id",
            "parent_type",
            "resource_sys_id",
        ]);
    }
    if tool == "resource_plan_plan_decision" {
        blocked.extend([
            "sys_id",
            "state",
            "task",
            "resource",
            "resource_type",
            "planned_hours",
            "notes",
            "start_date",
            "end_date",
        ]);
    }
    blocked
}

fn metadata_fields() -> &'static [&'static str] {
    &["actor", "requester", "client", "session_id", "user_agent"]
}

fn plan_selector_fields(tool: &str) -> &'static [&'static str] {
    match tool {
        "resource_plan_plan_create" => &[
            "parent_sys_id",
            "parent_type",
            "resource_sys_id",
            "resource_type",
        ],
        "resource_plan_plan_update" => &["sys_id", "number"],
        "resource_plan_plan_decision" => &["number", "decision"],
        _ => &[],
    }
}

fn require_string(
    args: &Map<String, Value>,
    field: &str,
) -> std::result::Result<String, PlanBuildError> {
    args.get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .ok_or_else(|| PlanBuildError::Invalid(field.to_string()))
}

async fn apply_plan(
    tool: &str,
    plan: &OperationPlan,
    state: &DaemonState,
) -> Result<ResourcePlanWriteResult> {
    match tool {
        "resource_plan_apply_create" => {
            state
                .core
                .create_resource_plan(plan.planned_changes.clone())
                .await
        }
        "resource_plan_apply_update" => {
            let target = plan
                .target
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("resource_plan update plan missing target"))?;
            state
                .core
                .update_resource_plan(&target.sys_id, plan.planned_changes.clone())
                .await
        }
        "resource_plan_apply_decision" => {
            let target = plan
                .target
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("resource_plan decision plan missing target"))?;
            state
                .core
                .update_resource_plan(&target.sys_id, plan.planned_changes.clone())
                .await
        }
        _ => Err(anyhow::anyhow!(
            "unsupported Resource Plan apply tool {tool}"
        )),
    }
}

async fn receipt_for_write(
    plan: &OperationPlan,
    tool: &str,
    audit_id: &str,
    apply_started_at: chrono::DateTime<Utc>,
    write_result: ResourcePlanWriteResult,
    before_record: Option<&SnowRecord>,
    state: &DaemonState,
) -> OperationReceipt {
    let record = write_result.record;
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
                .filter_map(|(field, after)| {
                    let before = before_record.and_then(|record| record_field_value(record, field));
                    if before
                        .as_ref()
                        .is_some_and(|before| field_values_equal(field, before, after))
                    {
                        return None;
                    }
                    Some(FieldChange {
                        field: field.clone(),
                        before,
                        after: Some(after.clone()),
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    let mut receipt = OperationReceipt {
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
        concurrency_token_observed: Some(ConcurrencyToken {
            sys_updated_on: write_result.concurrency.sys_updated_on,
            sys_mod_count: write_result.concurrency.sys_mod_count,
        }),
        apply_started_at: Some(apply_started_at),
        error_code: None,
        warnings: Vec::new(),
    };

    if tool == "resource_plan_apply_decision" {
        apply_decision_evidence(&mut receipt, plan, &record, state).await;
    }
    receipt
}

async fn apply_decision_evidence(
    receipt: &mut OperationReceipt,
    plan: &OperationPlan,
    record: &SnowRecord,
    state: &DaemonState,
) {
    let decision = plan
        .planned_changes
        .get("state")
        .and_then(Value::as_str)
        .and_then(ResourcePlanState::parse)
        .and_then(|target| match target {
            ResourcePlanState::Confirmed => Some(ResourcePlanDecision::Confirm),
            ResourcePlanState::Allocated => Some(ResourcePlanDecision::ConfirmAndAllocate),
            _ => None,
        });
    let observed_state = resource_plan_state_from_record(record);
    let evidence_result = state
        .core
        .resource_plan_allocation_evidence(&record.sys_id)
        .await;

    let expected_state = decision.map(ResourcePlanDecision::target_state);
    let expected_booking_type = decision.map(ResourcePlanDecision::expected_booking_type);
    let (allocation_count, booking_types) = match evidence_result {
        Ok(evidence) => (evidence.allocation_count, evidence.booking_types),
        Err(_) => (0, Vec::new()),
    };
    let matching_allocation_count = expected_booking_type
        .filter(|expected| {
            !booking_types.is_empty()
                && booking_types
                    .iter()
                    .all(|value| value.eq_ignore_ascii_case(expected))
        })
        .map_or(0, |_| allocation_count);
    let verified = expected_state.is_some()
        && observed_state == expected_state
        && matching_allocation_count > 0;

    let evidence = json!({
        "decision": decision,
        "verified": verified,
        "expected_state": expected_state.map(|value| json!({
            "value": value.raw().to_string(),
            "label": value.label(),
        })),
        "observed_state": observed_state.map(|value| json!({
            "value": value.raw().to_string(),
            "label": value.label(),
        })),
        "expected_booking_type": expected_booking_type,
        "allocation_count": allocation_count,
        "matching_allocation_count": matching_allocation_count,
        "booking_types": booking_types,
    });
    let mut snapshot = receipt.record_snapshot.take().unwrap_or_else(|| json!({}));
    if let Some(object) = snapshot.as_object_mut() {
        object.insert("decision_evidence".to_string(), evidence);
    } else {
        snapshot = json!({"record": snapshot, "decision_evidence": evidence});
    }
    receipt.record_snapshot = Some(snapshot);

    if !verified {
        receipt.status = ReceiptStatus::Partial;
        receipt.error_code = Some("DECISION_POSTCONDITION_INCOMPLETE".to_string());
        receipt.warnings.push(ReceiptWarning {
            code: "DECISION_POSTCONDITION_INCOMPLETE".to_string(),
            field: Some("state".to_string()),
            message: "ServiceNow accepted the Resource Plan transition, but the expected state and allocation evidence were not both observed".to_string(),
            data: None,
        });
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

fn field_values_equal(field: &str, before: &Value, after: &Value) -> bool {
    if field == "planned_hours"
        && let (Some(before), Some(after)) = (value_as_f64(before), value_as_f64(after))
    {
        return (before - after).abs() < f64::EPSILON;
    }
    before == after
}

fn value_as_f64(value: &Value) -> Option<f64> {
    match value {
        Value::Number(number) => number.as_f64(),
        Value::String(value) => value.trim().parse::<f64>().ok(),
        _ => None,
    }
}

fn concurrency_from_record(record: &SnowRecord) -> Option<ConcurrencyToken> {
    let sys_updated_on = record
        .fields
        .get("sys_updated_on")?
        .value
        .trim()
        .to_string();
    if sys_updated_on.is_empty() {
        return None;
    }

    Some(ConcurrencyToken {
        sys_updated_on,
        sys_mod_count: record
            .fields
            .get("sys_mod_count")
            .and_then(|field| field.value.trim().parse::<i64>().ok()),
    })
}

fn record_ref_from_snow(record: &SnowRecord) -> RecordRef {
    RecordRef {
        sys_id: record.sys_id.clone(),
        number: record.number.clone(),
        table: record.table.clone(),
    }
}

fn apply_tool_for_plan_tool(tool: &str) -> &str {
    match tool {
        "resource_plan_plan_create" => "resource_plan_apply_create",
        "resource_plan_plan_update" => "resource_plan_apply_update",
        "resource_plan_plan_decision" => "resource_plan_apply_decision",
        other => other,
    }
}

fn is_update_tool(tool: &str) -> bool {
    matches!(
        tool,
        "resource_plan_apply_update" | "resource_plan_apply_decision"
    )
}

fn parent_type_value(parent_type: ResourcePlanParentType) -> &'static str {
    match parent_type {
        ResourcePlanParentType::Demand => "demand",
        ResourcePlanParentType::Project => "project",
    }
}

fn resource_type_value(resource_type: ResourcePlanResourceType) -> &'static str {
    match resource_type {
        ResourcePlanResourceType::Group => "group",
        ResourcePlanResourceType::User => "user",
    }
}

fn resource_assignment_field(resource_type: ResourcePlanResourceType) -> &'static str {
    match resource_type {
        ResourcePlanResourceType::Group => "group_resource",
        ResourcePlanResourceType::User => "user_resource",
    }
}

fn is_kill_switched() -> bool {
    std::env::var("SNOW_RESOURCE_PLAN_WRITE_KILL_SWITCH")
        .map(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false)
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

fn string_param(params: &Value, field: &str) -> Option<String> {
    params
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
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
        Ok(Some(record)) => validate_confirmation_binding(&record, binding),
        Ok(None) | Err(_) => Err(ConfirmationConsumeError::NotFound),
    }
}

async fn field_rejected_apply(
    state: &DaemonState,
    id: Option<Value>,
    tool: &str,
    field: &str,
) -> JsonRpcResponse {
    audited_error(
        state,
        id,
        tool,
        -32051,
        "FIELD_REJECTED",
        json!({"code": "FIELD_REJECTED", "fields": [{"field": field, "reason": "type_mismatch"}]}),
        ResultStatus::Denied,
    )
    .await
}

fn field_rejected_response(id: Option<Value>, fields: Vec<Value>) -> JsonRpcResponse {
    rpc_error(
        id,
        -32051,
        "FIELD_REJECTED",
        json!({"code": "FIELD_REJECTED", "fields": fields}),
    )
}

async fn confirmation_invalid(
    state: &DaemonState,
    id: Option<Value>,
    tool: &str,
    err: ConfirmationConsumeError,
) -> JsonRpcResponse {
    audited_error(
        state,
        id,
        tool,
        -32054,
        "CONFIRMATION_INVALID",
        json!({"code": "CONFIRMATION_INVALID", "reason": err.to_string()}),
        ResultStatus::Denied,
    )
    .await
}

async fn pending_resolution_required(
    state: &DaemonState,
    id: Option<Value>,
    tool: &str,
    plan: &OperationPlan,
    idempotency: &IdempotencyRecord,
) -> JsonRpcResponse {
    audited_error(
        state,
        id,
        tool,
        -32060,
        "PENDING_RESOLUTION_REQUIRED",
        json!({
            "code": "PENDING_RESOLUTION_REQUIRED",
            "reason": "an apply attempt for this idempotency key started but no receipt was recorded",
            "retryable": false,
            "transient": false,
            "plan_id": plan.plan_id,
            "op_hash": plan.op_hash,
            "idempotency_key": idempotency.key,
            "apply_started_at": idempotency.apply_started_at.map(|value| value.to_rfc3339()),
            "idempotency_expires_at": idempotency.expires_at.to_rfc3339(),
            "retry_after_seconds": 2,
        }),
        ResultStatus::Error,
    )
    .await
}

async fn audited_error(
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
        Some(error_row_from_data(message, &data)),
        None,
        None,
    )
    .await
    {
        Ok(_) => rpc_error(id, code, message, data),
        Err(err) => internal_error(id, err),
    }
}

fn upstream_error_data(tool: &str, plan: &OperationPlan, reason: String, code: &str) -> Value {
    json!({
        "code": code,
        "reason": reason,
        "retryable": false,
        "transient": false,
        "plan_id": plan.plan_id,
        "op_hash": plan.op_hash,
        "plan_tool": plan.tool,
        "apply_tool": tool,
        "service_now_table": ResourcePlanResource::TABLE,
        "service_now_operation": if tool == "resource_plan_apply_create" { "create" } else { "update" },
    })
}

fn is_permission_denied_error(err: &anyhow::Error) -> bool {
    if err.downcast_ref::<SnowApiError>().is_some_and(|err| {
        matches!(
            err,
            SnowApiError::Api {
                status: 401 | 403,
                ..
            }
        )
    }) {
        return true;
    }
    let text = err.to_string().to_ascii_lowercase();
    text.contains("403")
        || text.contains("401")
        || text.contains("forbidden")
        || text.contains("unauthorized")
}

fn error_row_from_data(message: &str, data: &Value) -> ErrorRow {
    ErrorRow {
        code: message.to_string(),
        reason: data
            .get("reason")
            .and_then(Value::as_str)
            .or_else(|| data.get("details").and_then(Value::as_str))
            .or_else(|| data.get("code").and_then(Value::as_str))
            .unwrap_or(message)
            .to_string(),
        retryable: data
            .get("retryable")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        transient: data
            .get("transient")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    }
}

fn audit_summary_for_receipt(receipt: &OperationReceipt) -> (Value, Vec<AppliedChange>) {
    let applied_changes = receipt
        .changed_fields
        .iter()
        .map(|change| AppliedChange {
            field: change.field.clone(),
            old_hash: hash_audit_value(change.before.as_ref()),
            new_hash: hash_audit_value(change.after.as_ref()),
            redacted_preview: if is_free_text_field(&change.field) {
                None
            } else {
                change.after.as_ref().map(non_free_text_preview)
            },
        })
        .collect::<Vec<_>>();
    (
        json!({"op_hash": receipt.op_hash, "changed_fields": applied_changes}),
        applied_changes,
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
    field == "notes"
}

fn policy_decisions_for(
    _tool: &str,
    status: ResultStatus,
    error: Option<&ErrorRow>,
) -> Vec<PolicyDecisionRow> {
    let gates = match error.map(|error| error.code.as_str()) {
        Some("FIELD_REJECTED") => vec![("FieldAllowlist", "Deny")],
        Some("GUARD_FAILED") => vec![("Resolution", "Deny")],
        Some("IDEMPOTENCY_CONFLICT") | Some("PENDING_RESOLUTION_REQUIRED") => {
            vec![("Idempotency", "Deny")]
        }
        Some("CONCURRENCY_CONFLICT") | Some("CONCURRENCY_TOKEN_INVALID") => {
            vec![("ConcurrencyToken", "Deny")]
        }
        Some("CONFIRMATION_INVALID") | Some("PLAN_EXPIRED") => vec![("Confirmation", "Deny")],
        Some("KILL_SWITCH") | Some("UPSTREAM_PERMISSION_DENIED") => {
            vec![("EnvironmentGate", "Deny")]
        }
        _ if status == ResultStatus::AppliedSuccess => vec![
            ("Confirmation", "Allow"),
            ("Idempotency", "Allow"),
            ("ConcurrencyToken", "Allow"),
            ("FieldAllowlist", "Allow"),
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

fn rpc_error(id: Option<Value>, code: i64, message: &str, data: Value) -> JsonRpcResponse {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn args(value: Value) -> Map<String, Value> {
        value.as_object().cloned().expect("object")
    }

    fn test_record_with_fields(fields: &[(&str, &str)]) -> SnowRecord {
        SnowRecord {
            sys_id: "11111111111111111111111111111111".to_string(),
            number: "<RPLN_NUMBER>".to_string(),
            table: ResourcePlanResource::TABLE.to_string(),
            resource_type: snow_core::ResourceType::ResourcePlan,
            state: String::new(),
            short_description: String::new(),
            description: String::new(),
            fields: fields
                .iter()
                .map(|(field, value)| {
                    (
                        (*field).to_string(),
                        snow_core::FieldValue {
                            value: (*value).to_string(),
                            display_value: None,
                        },
                    )
                })
                .collect(),
            work_notes: Vec::new(),
            comments: Vec::new(),
            parent: None,
            children: Vec::new(),
            references: std::collections::HashMap::new(),
            synced_at: Utc::now(),
            source: snow_core::CacheSource::Api,
        }
    }

    #[test]
    fn create_rejects_direct_task_and_parent_table() {
        let fields = field_governance_rejections(
            "resource_plan_plan_create",
            &args(json!({
                "parent_sys_id": "22222222222222222222222222222222",
                "parent_type": "project",
                "resource_sys_id": "33333333333333333333333333333333",
                "resource_type": "group",
                "state": "1",
                "planned_hours": 8.0,
                "task": "44444444444444444444444444444444",
                "resource": "33333333333333333333333333333333",
                "group_resource": "33333333333333333333333333333333",
                "parent_table": "pm_project"
            })),
            &snow_mcp::domain::policy::PolicyConfig::read_only_default(),
            FieldGovernanceMode::PlanInput,
        );
        assert!(fields.iter().any(|field| field["field"] == "task"));
        assert!(fields.iter().any(|field| field["field"] == "resource"));
        assert!(
            fields
                .iter()
                .any(|field| field["field"] == "group_resource")
        );
        assert!(fields.iter().any(|field| field["field"] == "parent_table"));
    }

    #[test]
    fn update_rejects_resource_and_task_changes() {
        let fields = field_governance_rejections(
            "resource_plan_plan_update",
            &args(json!({
                "number": "<RPLN_NUMBER>",
                "resource": "33333333333333333333333333333333",
                "resource_type": "group",
                "task": "22222222222222222222222222222222",
                "state": "3"
            })),
            &snow_mcp::domain::policy::PolicyConfig::read_only_default(),
            FieldGovernanceMode::PlanInput,
        );
        assert!(fields.iter().any(|field| field["field"] == "resource"));
        assert!(fields.iter().any(|field| field["field"] == "resource_type"));
        assert!(fields.iter().any(|field| field["field"] == "task"));
        assert!(fields.iter().any(|field| field["field"] == "state"));
    }

    #[test]
    fn state_unknown_int_passes_through() {
        let state = parse_state(Some(&json!("99")), "state").expect("state");
        assert_eq!(state, ResourcePlanState::Other(99));
        assert_eq!(state.label(), None);
    }

    #[test]
    fn planned_hours_must_be_positive() {
        assert!(parse_planned_hours(Some(&json!(0.0)), "planned_hours").is_err());
        assert!(parse_planned_hours(Some(&json!(1.5)), "planned_hours").is_ok());
    }

    #[test]
    fn planned_hours_diff_compares_numeric_equivalence() {
        let record = test_record_with_fields(&[("planned_hours", "16")]);
        let mut payload = Map::new();
        payload.insert("planned_hours".to_string(), json!(16.0));

        assert!(diff_fields(&record, &payload).is_empty());
        assert!(field_values_equal(
            "planned_hours",
            &Value::String("16".to_string()),
            &json!(16.0)
        ));
    }

    #[test]
    fn concurrency_from_record_rejects_blank_sys_updated_on() {
        let record = test_record_with_fields(&[("sys_updated_on", ""), ("sys_mod_count", "24")]);

        assert!(concurrency_from_record(&record).is_none());
    }

    #[test]
    fn permission_denied_error_detects_status_text() {
        let err = anyhow::anyhow!("ServiceNow API error 403 Forbidden");
        assert!(is_permission_denied_error(&err));
    }

    #[test]
    fn notes_is_a_free_text_audit_field() {
        assert!(is_free_text_field("notes"));
        assert!(!is_free_text_field("planned_hours"));
    }
}
