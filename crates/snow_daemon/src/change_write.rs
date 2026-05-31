use anyhow::Result;
use chrono::Utc;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use snow_core::SnowRecord;
use snow_mcp::audit::{AuditSink, SqliteAuditSink};
use snow_mcp::domain::audit::{
    ActorIdentity, AppliedChange, AuditEvent, ClientIdentity, ErrorRow, PolicyDecisionRow,
    ResultStatus, ServiceNowMetadata,
};
use snow_mcp::domain::primitives::{IdempotencyKey, IdempotencyKeySource, RecordRef};
use snow_mcp::planner::{
    ConcurrencyToken, ConfirmationBinding, ConfirmationConsumeError, ConfirmationRecord,
    ConfirmationStore, FieldChange, IdempotencyOutcome, IdempotencyStore, OperationPlan,
    OperationPlanBuilder, OperationReceipt, PlanLifecycleState, PlanStore, PlanStoreRecord,
    ReceiptStatus, SqliteConfirmationStore, SqliteIdempotencyStore, SqlitePlanStore,
};
use std::collections::BTreeSet;
use uuid::Uuid;

use crate::DaemonState;
use crate::rpc::JsonRpcResponse;

const PLAN_TTL_SECONDS: i64 = 600;

pub async fn handle_change_plan(
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
        return change_error(
            id,
            -32040,
            "policy denied",
            json!({
                "details": "matching Change apply tool is disabled by current MCP policy",
                "tool": apply_tool,
            }),
        );
    }

    let args = match params.as_object() {
        Some(args) => args,
        None => {
            return field_rejected_response(
                id,
                vec![json!({"field": "payload", "reason": "type_mismatch"})],
            );
        }
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
            return change_error(
                id,
                -32004,
                "record not found",
                json!({ "details": message }),
            );
        }
        Err(PlanBuildError::WrongType { number, table }) => {
            return change_error(
                id,
                -32051,
                "FIELD_REJECTED",
                json!({
                    "code": "FIELD_REJECTED",
                    "fields": [{"field": number, "reason": format!("wrong_table:{table}")}],
                }),
            );
        }
        Err(PlanBuildError::TerminalRecord(number)) => {
            return change_error(
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
            return change_error(
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
                actor: actor.clone(),
                requester: requester.clone(),
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
        "preview": plan.planned_changes,
        "expires_at": expires_at.to_rfc3339(),
        "confirmation_token": confirmation.token_id,
        "idempotency_key": idempotency_key,
    });
    if let Some(token) = plan_input.concurrency_token {
        result["concurrency_token"] = json!(token);
    }

    JsonRpcResponse::ok(id, result)
}

pub async fn handle_change_apply(
    id: Option<Value>,
    tool: &str,
    params: &Value,
    state: &DaemonState,
) -> JsonRpcResponse {
    if is_kill_switched() {
        return audited_change_error(
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
        return audited_change_error(
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
            return audited_change_error(
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
        return audited_change_error(
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
            return audited_change_error(
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
            return pending_resolution_required(state, id, tool, &plan).await;
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
            return audited_change_error(
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
        return audited_change_error(
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
            Some(target) => match state.core.get_record_fresh(&target.number).await {
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
            return audited_change_error(
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
            return audited_change_error(
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
            return audited_change_error(
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
            return audited_change_error(
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
        write_result.record,
        ConcurrencyToken {
            sys_updated_on: write_result.concurrency.sys_updated_on,
            sys_mod_count: write_result.concurrency.sys_mod_count,
        },
        before_record.as_ref(),
        state,
    );
    let (audit_summary, applied_changes) = audit_summary_for_receipt(&receipt);
    if let Err(err) = append_audit_event(
        state,
        &audit_id,
        Some(plan.plan_id.as_str()),
        tool,
        ResultStatus::AppliedSuccess,
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
}

#[derive(Debug)]
enum PlanBuildError {
    Invalid(String),
    FieldRejected(Vec<Value>),
    NotFound(String),
    WrongType { number: String, table: String },
    TerminalRecord(String),
    Upstream(anyhow::Error),
}

async fn build_plan_input(
    tool: &str,
    args: &Map<String, Value>,
    state: &DaemonState,
) -> std::result::Result<PlanInput, PlanBuildError> {
    let mut payload = Value::Object(args.clone());
    strip_non_writable_selector_fields(tool, &mut payload);
    reject_cancel_state(&payload)?;

    match tool {
        "change_request_plan_create" => {
            for field in [
                "short_description",
                "description",
                "assignment_group",
                "cmdb_ci",
                "start_date",
                "end_date",
                "change_plan",
                "backout_plan",
                "test_plan",
            ] {
                require_string(args, field)?;
            }
            Ok(PlanInput {
                builder: OperationPlanBuilder::new(tool).planned_changes(payload),
                concurrency_token: None,
            })
        }
        "change_task_plan_create" => {
            require_string(args, "parent_change_number")?;
            require_string(args, "short_description")?;
            let parent_number = args
                .get("parent_change_number")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let parent = state
                .core
                .get_record_fresh(parent_number)
                .await
                .map_err(PlanBuildError::Upstream)?
                .ok_or_else(|| {
                    PlanBuildError::NotFound(format!("parent change {parent_number} not found"))
                })?;
            require_table(&parent, "change_request")?;
            inject(&mut payload, "change_request", parent.sys_id.clone());
            Ok(PlanInput {
                builder: OperationPlanBuilder::new(tool)
                    .target(record_ref_from_snow(&parent))
                    .planned_changes(payload),
                concurrency_token: None,
            })
        }
        "change_request_plan_update" | "change_task_plan_update" => {
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
            require_table(
                &record,
                if tool == "change_request_plan_update" {
                    "change_request"
                } else {
                    "change_task"
                },
            )?;
            if state
                .mcp_config
                .policy
                .tools
                .get(apply_tool_for_plan_tool(tool))
                .is_some_and(|policy| policy.skip_terminal_records)
                && record_is_terminal(&record)
            {
                return Err(PlanBuildError::TerminalRecord(number));
            }
            Ok(PlanInput {
                builder: OperationPlanBuilder::new(tool)
                    .target(record_ref_from_snow(&record))
                    .planned_changes(payload),
                concurrency_token: concurrency_from_record(&record),
            })
        }
        _ => Err(PlanBuildError::Invalid(format!(
            "unsupported Change plan tool {tool}"
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
        "change_request_plan_update" | "change_task_plan_update" => {
            object.remove("number");
        }
        "change_task_plan_create" => {
            object.remove("parent_change_number");
        }
        _ => {}
    }
}

fn reject_cancel_state(payload: &Value) -> std::result::Result<(), PlanBuildError> {
    if payload
        .get("state")
        .and_then(Value::as_str)
        .is_some_and(|state| state.to_ascii_lowercase().contains("cancel"))
    {
        return Err(PlanBuildError::FieldRejected(vec![json!({
            "field": "state",
            "reason": "value_constrained",
        })]));
    }
    Ok(())
}

fn require_table(record: &SnowRecord, expected: &str) -> std::result::Result<(), PlanBuildError> {
    if record.table == expected {
        Ok(())
    } else {
        Err(PlanBuildError::WrongType {
            number: record.number.clone(),
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
    if fields.is_empty() {
        None
    } else {
        Some(field_rejected_response(id, fields))
    }
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
        "active",
        "closed_at",
        "closed_by",
        "close_code",
        "close_notes",
    ]);
    if !matches!(
        tool,
        "change_request_plan_update" | "change_task_plan_update"
    ) {
        blocked.insert("number");
    }
    blocked
}

fn metadata_fields() -> &'static [&'static str] {
    &["actor", "requester", "client", "session_id", "user_agent"]
}

fn plan_selector_fields(tool: &str) -> &'static [&'static str] {
    match tool {
        "change_request_plan_update" | "change_task_plan_update" => &["number"],
        "change_task_plan_create" => &["parent_change_number"],
        _ => &[],
    }
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

async fn apply_plan(
    tool: &str,
    plan: &OperationPlan,
    state: &DaemonState,
) -> Result<snow_core::ChangeWriteResult> {
    match tool {
        "change_request_apply_create" => {
            state
                .core
                .create_change_request(plan.planned_changes.clone())
                .await
        }
        "change_task_apply_create" => {
            state
                .core
                .create_change_task(plan.planned_changes.clone())
                .await
        }
        "change_request_apply_update" => {
            let target = plan
                .target
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("change update plan missing target"))?;
            state
                .core
                .update_change_request(&target.sys_id, plan.planned_changes.clone())
                .await
        }
        "change_task_apply_update" => {
            let target = plan
                .target
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("change task update plan missing target"))?;
            state
                .core
                .update_change_task(&target.sys_id, plan.planned_changes.clone())
                .await
        }
        _ => Err(anyhow::anyhow!("unsupported Change apply tool {tool}")),
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
        warnings: Vec::new(),
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

fn apply_tool_for_plan_tool(tool: &str) -> &str {
    match tool {
        "change_request_plan_create" => "change_request_apply_create",
        "change_request_plan_update" => "change_request_apply_update",
        "change_task_plan_create" => "change_task_apply_create",
        "change_task_plan_update" => "change_task_apply_update",
        other => other,
    }
}

fn is_update_tool(tool: &str) -> bool {
    matches!(
        tool,
        "change_request_apply_update" | "change_task_apply_update"
    )
}

fn is_kill_switched() -> bool {
    std::env::var("SNOW_CHANGE_WRITE_KILL_SWITCH")
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
    audited_change_error(
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
    change_error(
        id,
        -32051,
        "FIELD_REJECTED",
        json!({ "code": "FIELD_REJECTED", "fields": fields }),
    )
}

async fn confirmation_invalid(
    state: &DaemonState,
    id: Option<Value>,
    tool: &str,
    err: ConfirmationConsumeError,
) -> JsonRpcResponse {
    audited_change_error(
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
    audited_change_error(
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

async fn audited_change_error(
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
            reason: data
                .get("code")
                .and_then(Value::as_str)
                .unwrap_or(message)
                .to_string(),
            retryable: false,
            transient: false,
        }),
        None,
        None,
    )
    .await
    {
        Ok(_) => change_error(id, code, message, data),
        Err(err) => internal_error(id, err),
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
        json!({
            "op_hash": receipt.op_hash,
            "changed_fields": applied_changes,
        }),
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
    matches!(
        field,
        "short_description"
            | "description"
            | "implementation_plan"
            | "change_plan"
            | "backout_plan"
            | "test_plan"
            | "justification"
            | "work_notes"
    )
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
        Some("KILL_SWITCH") => vec![("EnvironmentGate", "Deny")],
        Some("RATE_LIMITED") => vec![("RateLimit", "Deny")],
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

fn change_error(id: Option<Value>, code: i64, message: &str, data: Value) -> JsonRpcResponse {
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

    #[test]
    fn update_number_is_selector_not_payload() {
        let mut payload = json!({
            "number": "CHG0001",
            "short_description": "Change"
        });
        strip_non_writable_selector_fields("change_request_plan_update", &mut payload);
        assert!(payload.get("number").is_none());
        assert_eq!(payload["short_description"], "Change");
    }

    #[test]
    fn create_number_is_blocked() {
        let fields = field_governance_rejections(
            "change_request_plan_create",
            &args(json!({"number": "CHG0001", "short_description": "Change"})),
            &snow_mcp::domain::policy::PolicyConfig::read_only_default(),
            FieldGovernanceMode::PlanInput,
        );
        assert!(fields.iter().any(|field| field["field"] == "number"));
    }

    #[test]
    fn cancel_state_is_constrained() {
        let payload = json!({"state": "Cancelled"});
        assert!(matches!(
            reject_cancel_state(&payload),
            Err(PlanBuildError::FieldRejected(_))
        ));
    }
}
