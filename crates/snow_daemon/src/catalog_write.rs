use std::collections::BTreeSet;

use anyhow::Result;
use chrono::Utc;
use serde_json::{Map, Value, json};
use snow_core::CatalogSubmitResult;
use snow_mcp::domain::audit::ServiceNowMetadata;
use snow_mcp::domain::primitives::{IdempotencyKey, IdempotencyKeySource, RecordRef};
use snow_mcp::planner::{
    ConfirmationBinding, ConfirmationConsumeError, ConfirmationRecord, ConfirmationStore,
    FieldChange, IdempotencyOutcome, IdempotencyStore, OperationPlan, OperationPlanBuilder,
    OperationReceipt, PlanLifecycleState, PlanStore, PlanStoreRecord, ReceiptStatus,
    SqliteConfirmationStore, SqliteIdempotencyStore, SqlitePlanStore,
};
use uuid::Uuid;

use crate::DaemonState;
use crate::rpc::JsonRpcResponse;

const PLAN_TTL_SECONDS: i64 = 600;
const CATALOG_PLAN_TOOL: &str = "catalog_plan_request";
const CATALOG_SUBMIT_TOOL: &str = "catalog_submit_request";

pub async fn handle_catalog_item_get(
    id: Option<Value>,
    params: &Value,
    state: &DaemonState,
) -> JsonRpcResponse {
    let Some(item_sys_id) =
        string_param(params, "sys_id").or_else(|| string_param(params, "item_sys_id"))
    else {
        return field_rejected(
            id,
            vec![json!({"field": "sys_id", "reason": "type_mismatch"})],
        );
    };
    match state.core.catalog_item_get_envelope(&item_sys_id).await {
        Ok(envelope) => match serde_json::to_value(envelope) {
            Ok(value) => JsonRpcResponse::ok(id, value),
            Err(err) => internal_error(id, err),
        },
        Err(err) => catalog_read_error(id, err),
    }
}

pub async fn handle_catalog_items_search(
    id: Option<Value>,
    params: &Value,
    state: &DaemonState,
) -> JsonRpcResponse {
    let Some(query) = string_param(params, "query") else {
        return field_rejected(
            id,
            vec![json!({"field": "query", "reason": "type_mismatch"})],
        );
    };
    let limit = params
        .get("limit")
        .and_then(Value::as_u64)
        .unwrap_or(10)
        .min(50) as u32;
    match state
        .core
        .catalog_items_search_envelope(&query, limit)
        .await
    {
        Ok(envelope) => match serde_json::to_value(envelope) {
            Ok(value) => JsonRpcResponse::ok(id, value),
            Err(err) => internal_error(id, err),
        },
        Err(err) => catalog_read_error(id, err),
    }
}

pub async fn handle_catalog_plan_request(
    id: Option<Value>,
    params: &Value,
    state: &DaemonState,
) -> JsonRpcResponse {
    if !state
        .mcp_config
        .policy
        .tool_enabled_in_environment(CATALOG_SUBMIT_TOOL, &state.mcp_config.environment.label)
    {
        return catalog_error(
            id,
            -32040,
            "policy denied",
            json!({
                "details": "catalog submit tool is disabled by current MCP policy",
                "tool": CATALOG_SUBMIT_TOOL,
            }),
        );
    }

    let args = match params.as_object() {
        Some(args) => args,
        None => {
            return field_rejected(
                id,
                vec![json!({"field": "payload", "reason": "type_mismatch"})],
            );
        }
    };
    if let Some(response) = reject_plan_input_fields(id.clone(), args) {
        return response;
    }

    let Some(item_sys_id) =
        string_param(params, "item_sys_id").or_else(|| string_param(params, "sys_id"))
    else {
        return field_rejected(
            id,
            vec![json!({"field": "item_sys_id", "reason": "type_mismatch"})],
        );
    };
    let variables = match params.get("variables").and_then(Value::as_object) {
        Some(variables) => variables.clone(),
        None => {
            return field_rejected(
                id,
                vec![json!({"field": "variables", "reason": "type_mismatch"})],
            );
        }
    };

    let item = match state.core.get_catalog_item(&item_sys_id).await {
        Ok(item) => item,
        Err(err) => return internal_error(id, err),
    };
    let request_body = match build_order_now_body(params, variables.clone()) {
        Ok(body) => body,
        Err(fields) => return field_rejected(id, fields),
    };
    let target = RecordRef {
        sys_id: item.sys_id.clone(),
        number: item.name.clone(),
        table: item.table.clone(),
    };
    let plan = OperationPlanBuilder::new(CATALOG_PLAN_TOOL)
        .target(target.clone())
        .planned_changes(json!({
            "item_sys_id": item.sys_id,
            "item_name": item.name,
            "variables": variables,
            "request_body": request_body,
        }))
        .build();
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
            Value::String(CATALOG_SUBMIT_TOOL.to_string()),
        );
    }

    let stores = match stores(state) {
        Ok(stores) => stores,
        Err(err) => return internal_error(id, err),
    };
    let record = PlanStoreRecord {
        plan_id: plan.plan_id.clone(),
        tool: plan.tool.clone(),
        actor: actor.clone(),
        op_hash: plan.op_hash.clone(),
        plan_json,
        concurrency_token: None,
        created_at: now,
        expires_at,
        state: PlanLifecycleState::Pending,
    };
    if let Err(err) = stores.plan_store.put(record).await {
        return internal_error(id, err);
    }

    let confirmation = match stores
        .confirmation_store
        .issue(
            &plan.plan_id,
            ConfirmationBinding {
                actor: actor.clone(),
                requester: requester.clone(),
                tool: CATALOG_SUBMIT_TOOL.to_string(),
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

    JsonRpcResponse::ok(
        id,
        json!({
            "plan_id": plan.plan_id,
            "op_hash": plan.op_hash,
            "target": target,
            "preview": plan.planned_changes,
            "expires_at": expires_at.to_rfc3339(),
            "confirmation_token": confirmation.token_id,
            "idempotency_key": idempotency_key,
            "requires_confirmation": true,
        }),
    )
}

pub async fn handle_catalog_submit_request(
    id: Option<Value>,
    params: &Value,
    state: &DaemonState,
) -> JsonRpcResponse {
    if !state
        .mcp_config
        .policy
        .tool_enabled_in_environment(CATALOG_SUBMIT_TOOL, &state.mcp_config.environment.label)
    {
        return catalog_error(
            id,
            -32040,
            "policy denied",
            json!({
                "details": "catalog submit tool is disabled by current MCP policy",
                "tool": CATALOG_SUBMIT_TOOL,
            }),
        );
    }

    let Some(plan_id) = string_param(params, "plan_id") else {
        return field_rejected(
            id,
            vec![json!({"field": "plan_id", "reason": "type_mismatch"})],
        );
    };
    let Some(confirmation_token) = string_param(params, "confirmation_token") else {
        return field_rejected(
            id,
            vec![json!({"field": "confirmation_token", "reason": "type_mismatch"})],
        );
    };
    let Some(idempotency_key) = string_param(params, "idempotency_key") else {
        return field_rejected(
            id,
            vec![json!({"field": "idempotency_key", "reason": "type_mismatch"})],
        );
    };

    let stores = match stores(state) {
        Ok(stores) => stores,
        Err(err) => return internal_error(id, err),
    };
    let record = match stores.plan_store.get(&plan_id).await {
        Ok(Some(record)) => record,
        Ok(None) => {
            return catalog_error(id, -32004, "plan not found", json!({ "plan_id": plan_id }));
        }
        Err(err) => return internal_error(id, err),
    };
    if record.state != PlanLifecycleState::Pending {
        return catalog_error(
            id,
            -32053,
            "PLAN_NOT_PENDING",
            json!({ "plan_id": plan_id, "state": record.state }),
        );
    }
    if record.expires_at <= Utc::now() {
        let _ = stores.plan_store.mark_expired(&plan_id).await;
        return catalog_error(id, -32053, "PLAN_EXPIRED", json!({ "plan_id": plan_id }));
    }

    let plan = match serde_json::from_value::<OperationPlan>(record.plan_json.clone()) {
        Ok(plan) => plan,
        Err(err) => return internal_error(id, err),
    };
    if plan.tool != CATALOG_PLAN_TOOL {
        return catalog_error(
            id,
            -32053,
            "PLAN_TOOL_MISMATCH",
            json!({ "plan_id": plan_id, "tool": plan.tool }),
        );
    }
    let actor = actor_from_params(params, state);
    let requester = requester_from_params(params, &actor);
    let binding = ConfirmationBinding {
        actor,
        requester,
        tool: CATALOG_SUBMIT_TOOL.to_string(),
        op_hash: plan.op_hash.clone(),
        environment: state.mcp_config.environment.label.clone(),
    };
    match stores.confirmation_store.lookup(&confirmation_token) {
        Ok(Some(confirmation)) => {
            if let Err(err) = validate_confirmation(&confirmation, &binding) {
                return confirmation_invalid(id, err);
            }
        }
        Ok(None) | Err(_) => return confirmation_invalid(id, ConfirmationConsumeError::NotFound),
    }
    let key = IdempotencyKey {
        value: idempotency_key,
        source: IdempotencyKeySource::Client,
    };

    match stores
        .idempotency_store
        .check_and_record(
            &key,
            CATALOG_SUBMIT_TOOL,
            &plan.op_hash,
            state.mcp_config.policy.idempotency_window_seconds,
        )
        .await
    {
        Ok(IdempotencyOutcome::NewKey) => {}
        Ok(IdempotencyOutcome::Replay(receipt)) => {
            if let Err(err) = validate_replay_confirmation(
                &stores.confirmation_store,
                &confirmation_token,
                &binding,
            ) {
                return confirmation_invalid(id, err);
            }
            return JsonRpcResponse::ok(id, json!(*receipt));
        }
        Ok(IdempotencyOutcome::Pending {
            retryable,
            transient,
        }) => {
            return catalog_error(
                id,
                -32053,
                "IDEMPOTENCY_PENDING",
                json!({"retryable": retryable, "transient": transient}),
            );
        }
        Ok(IdempotencyOutcome::Conflict { reason }) => {
            return catalog_error(
                id,
                -32052,
                "IDEMPOTENCY_CONFLICT",
                json!({"reason": reason, "attempted_op_hash": plan.op_hash}),
            );
        }
        Err(err) => return internal_error(id, err),
    }

    if let Err(err) = stores
        .confirmation_store
        .consume(&confirmation_token, &binding)
        .await
    {
        return confirmation_invalid(id, err);
    }
    let apply_started_at = Utc::now();
    if let Err(err) = stores
        .idempotency_store
        .mark_apply_started(&key, CATALOG_SUBMIT_TOOL)
        .await
    {
        return internal_error(id, err);
    }

    let item_sys_id = match plan
        .planned_changes
        .get("item_sys_id")
        .and_then(Value::as_str)
    {
        Some(item_sys_id) => item_sys_id,
        None => return internal_error(id, "catalog plan missing item_sys_id"),
    };
    let request_body = match plan.planned_changes.get("request_body") {
        Some(body) => body.clone(),
        None => return internal_error(id, "catalog plan missing request_body"),
    };
    let submit_result = match state
        .core
        .submit_catalog_request(item_sys_id, request_body)
        .await
    {
        Ok(result) => result,
        Err(err) => return internal_error(id, err),
    };
    let audit_id = Uuid::new_v4().to_string();
    let receipt = receipt_for_catalog_submit(&plan, &audit_id, apply_started_at, submit_result);
    if let Err(err) = stores.idempotency_store.save_receipt(&key, &receipt).await {
        return internal_error(id, err);
    }
    let _ = stores.plan_store.mark_consumed(&plan.plan_id).await;

    JsonRpcResponse::ok(id, json!(receipt))
}

fn build_order_now_body(
    params: &Value,
    variables: Map<String, Value>,
) -> std::result::Result<Value, Vec<Value>> {
    let quantity = optional_string(params, "quantity")
        .or_else(|| optional_string(params, "sysparm_quantity"))
        .unwrap_or_else(|| "1".to_string());
    let item_guid = optional_string(params, "item_guid")
        .or_else(|| optional_string(params, "sysparm_item_guid"))
        .unwrap_or_else(|| Uuid::new_v4().simple().to_string());
    if quantity.trim().is_empty() {
        return Err(vec![json!({"field": "quantity", "reason": "empty"})]);
    }

    let get_portal_messages = optional_bool_or_string(params, "get_portal_messages")
        .unwrap_or_else(|| Value::String("true".to_string()));
    let no_validation = optional_bool_or_string(params, "sysparm_no_validation")
        .or_else(|| optional_bool_or_string(params, "no_validation"))
        .unwrap_or_else(|| Value::String("true".to_string()));
    let engagement_channel =
        optional_string(params, "engagement_channel").unwrap_or_else(|| "ec".to_string());
    let referrer = params.get("referrer").cloned().unwrap_or(Value::Null);
    let ai_referred_fields = params
        .get("sysparm_ai_referred_fields")
        .cloned()
        .unwrap_or_else(|| Value::String("{}".to_string()));

    Ok(json!({
        "sysparm_quantity": quantity,
        "variables": Value::Object(variables),
        "sysparm_item_guid": item_guid,
        "get_portal_messages": get_portal_messages,
        "sysparm_no_validation": no_validation,
        "engagement_channel": engagement_channel,
        "referrer": referrer,
        "sysparm_ai_referred_fields": ai_referred_fields,
    }))
}

fn receipt_for_catalog_submit(
    plan: &OperationPlan,
    audit_id: &str,
    apply_started_at: chrono::DateTime<Utc>,
    submit_result: CatalogSubmitResult,
) -> OperationReceipt {
    let record_url = submit_result.browser_url.clone();
    OperationReceipt {
        plan_id: plan.plan_id.clone(),
        audit_id: audit_id.to_string(),
        parent_audit_id: plan.plan_id.clone(),
        tool: CATALOG_SUBMIT_TOOL.to_string(),
        status: ReceiptStatus::Success,
        applied_changes_summary: Some(json!({
            "catalog_item": submit_result.item_sys_id,
            "table": submit_result.table,
            "sys_id": submit_result.sys_id,
            "number": submit_result.number,
            "request_number": submit_result.request_number,
            "request_item_number": submit_result.request_item_number,
        })),
        service_now_metadata: Some(ServiceNowMetadata {
            sys_id: submit_result.sys_id.clone(),
            number: submit_result.number.clone(),
            transaction_id: None,
        }),
        idempotency_replay: false,
        completed_at: Utc::now(),
        op_hash: plan.op_hash.clone(),
        record_url,
        record_snapshot: serde_json::to_value(&submit_result).ok(),
        changed_fields: vec![FieldChange {
            field: "catalog_request".to_string(),
            before: None,
            after: Some(submit_result.raw_result),
        }],
        concurrency_token_observed: None,
        apply_started_at: Some(apply_started_at),
        error_code: None,
        warnings: Vec::new(),
    }
}

struct CatalogStores {
    plan_store: SqlitePlanStore,
    confirmation_store: SqliteConfirmationStore,
    idempotency_store: SqliteIdempotencyStore,
}

fn stores(state: &DaemonState) -> Result<CatalogStores> {
    std::fs::create_dir_all(&state.data_dir)?;
    let path = state.data_dir.join("mcp_catalog_write.sqlite3");
    Ok(CatalogStores {
        plan_store: SqlitePlanStore::open(&path)?,
        confirmation_store: SqliteConfirmationStore::open(&path)?,
        idempotency_store: SqliteIdempotencyStore::open(&path)?,
    })
}

fn reject_plan_input_fields(
    id: Option<Value>,
    args: &Map<String, Value>,
) -> Option<JsonRpcResponse> {
    let allowed = BTreeSet::from([
        "item_sys_id",
        "sys_id",
        "variables",
        "quantity",
        "sysparm_quantity",
        "item_guid",
        "sysparm_item_guid",
        "get_portal_messages",
        "sysparm_no_validation",
        "no_validation",
        "engagement_channel",
        "referrer",
        "sysparm_ai_referred_fields",
    ]);
    let metadata = BTreeSet::from(["actor", "requester", "client", "session_id", "user_agent"]);
    let fields = args
        .keys()
        .filter(|field| !allowed.contains(field.as_str()) && !metadata.contains(field.as_str()))
        .map(|field| json!({ "field": field, "reason": "not_in_allowlist" }))
        .collect::<Vec<_>>();
    if fields.is_empty() {
        None
    } else {
        Some(field_rejected(id, fields))
    }
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

fn string_param(params: &Value, field: &str) -> Option<String> {
    params
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn optional_string(params: &Value, field: &str) -> Option<String> {
    params.get(field).and_then(|value| match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        _ => None,
    })
}

fn optional_bool_or_string(params: &Value, field: &str) -> Option<Value> {
    params.get(field).and_then(|value| match value {
        Value::Bool(_) | Value::String(_) => Some(value.clone()),
        _ => None,
    })
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

fn field_rejected(id: Option<Value>, fields: Vec<Value>) -> JsonRpcResponse {
    catalog_error(
        id,
        -32051,
        "FIELD_REJECTED",
        json!({ "code": "FIELD_REJECTED", "fields": fields }),
    )
}

fn confirmation_invalid(id: Option<Value>, err: ConfirmationConsumeError) -> JsonRpcResponse {
    catalog_error(
        id,
        -32050,
        "CONFIRMATION_INVALID",
        json!({ "code": "CONFIRMATION_INVALID", "reason": err.to_string() }),
    )
}

fn catalog_error(id: Option<Value>, code: i64, message: &str, data: Value) -> JsonRpcResponse {
    JsonRpcResponse::error(id, code, message, Some(data))
}

fn catalog_read_error(id: Option<Value>, err: anyhow::Error) -> JsonRpcResponse {
    if let Some(miss) = err.downcast_ref::<snow_core::cache::policy::CacheMiss>() {
        return catalog_error(
            id,
            -32072,
            "cache miss",
            json!({
                "code": "CACHE_MISS",
                "operation": miss.operation,
                "object": miss.object,
            }),
        );
    }
    internal_error(id, err)
}

fn internal_error(id: Option<Value>, err: impl ToString) -> JsonRpcResponse {
    let details = err.to_string();
    JsonRpcResponse::error(
        id,
        -32000,
        "internal error",
        Some(json!({ "details": details })),
    )
}
