//! Governed draft-only Knowledge article creation.
//!
//! Publishing and lifecycle transitions intentionally do not exist in this
//! module. A caller must plan and confirm a create; the core write path then
//! refetches the record and proves it is still in `draft`.

use anyhow::Result;
use chrono::Utc;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use snow_mcp::audit::{AuditSink, SqliteAuditSink};
use snow_mcp::domain::audit::{
    ActorIdentity, AuditEvent, ClientIdentity, PolicyDecisionRow, ResultStatus, ServiceNowMetadata,
};
use snow_mcp::domain::primitives::{IdempotencyKey, IdempotencyKeySource};
use snow_mcp::planner::{
    ConfirmationBinding, ConfirmationConsumeError, ConfirmationStore, FieldChange,
    IdempotencyOutcome, IdempotencyStore, OperationPlan, OperationPlanBuilder, OperationReceipt,
    PlanLifecycleState, PlanStore, PlanStoreRecord, ReceiptStatus, SqliteConfirmationStore,
    SqliteIdempotencyStore, SqlitePlanStore,
};
use std::collections::BTreeSet;
use uuid::Uuid;

use crate::DaemonState;
use crate::rpc::JsonRpcResponse;

const PLAN_TTL_SECONDS: i64 = 600;
const KNOWLEDGE_DRAFT_PLAN_TOOL: &str = "knowledge_plan_create_draft";
const KNOWLEDGE_DRAFT_APPLY_TOOL: &str = "knowledge_apply_create_draft";
const MAX_TITLE_LENGTH: usize = 160;
const MAX_BODY_LENGTH: usize = 120_000;

pub async fn handle_knowledge_plan_create_draft(
    id: Option<Value>,
    params: &Value,
    state: &DaemonState,
) -> JsonRpcResponse {
    if let Some(response) = policy_gate(id.clone(), state) {
        return response;
    }
    let input = match parse_plan_input(id.clone(), params) {
        Ok(input) => input,
        Err(response) => return response,
    };

    let plan = OperationPlanBuilder::new(KNOWLEDGE_DRAFT_PLAN_TOOL)
        .planned_changes(json!({
            "short_description": input.short_description,
            "text": input.text,
            "kb_knowledge_base": input.knowledge_base_sys_id,
            "kb_category": input.category_sys_id,
            "workflow_state": "draft",
        }))
        .build();
    let now = Utc::now();
    let expires_at = now + chrono::Duration::seconds(PLAN_TTL_SECONDS);
    let actor = actor_from_params(params, state);
    let requester = requester_from_params(params, &actor);
    let idempotency_key = Uuid::new_v4().to_string();
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
            KNOWLEDGE_DRAFT_APPLY_TOOL,
            &plan.op_hash,
            state.mcp_config.policy.idempotency_window_seconds,
        )
        .await
    {
        Ok(IdempotencyOutcome::NewKey) => {}
        Ok(IdempotencyOutcome::Conflict { .. }) => {
            return knowledge_error(
                id,
                -32052,
                "IDEMPOTENCY_CONFLICT",
                json!({"code": "IDEMPOTENCY_CONFLICT", "idempotency_key": idempotency_key}),
            );
        }
        Ok(IdempotencyOutcome::Replay(_)) | Ok(IdempotencyOutcome::Pending { .. }) => {
            return internal_error(id, "server-issued idempotency key was not unique");
        }
        Err(err) => return internal_error(id, err),
    }

    let mut plan_json = match serde_json::to_value(&plan) {
        Ok(value) => value,
        Err(err) => return internal_error(id, err),
    };
    plan_json["apply_tool"] = Value::String(KNOWLEDGE_DRAFT_APPLY_TOOL.to_string());
    if let Err(err) = stores
        .plan_store
        .put(PlanStoreRecord {
            plan_id: plan.plan_id.clone(),
            tool: plan.tool.clone(),
            actor: actor.clone(),
            op_hash: plan.op_hash.clone(),
            plan_json,
            concurrency_token: None,
            created_at: now,
            expires_at,
            state: PlanLifecycleState::Pending,
        })
        .await
    {
        return internal_error(id, err);
    }
    if let Err(err) = append_audit(
        state,
        &plan.plan_id,
        None,
        KNOWLEDGE_DRAFT_PLAN_TOOL,
        ResultStatus::Plan,
        Some(redacted_plan_change(&plan)),
        None,
        Some((&actor, &requester)),
    )
    .await
    {
        return internal_error(id, err);
    }
    let binding = ConfirmationBinding {
        actor,
        requester,
        tool: KNOWLEDGE_DRAFT_APPLY_TOOL.to_string(),
        op_hash: plan.op_hash.clone(),
        environment: state.mcp_config.environment.label.clone(),
    };
    let confirmation = match stores
        .confirmation_store
        .issue(&plan.plan_id, binding, PLAN_TTL_SECONDS as u64)
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
            "preview": preview_for(&plan),
            "expires_at": expires_at.to_rfc3339(),
            "confirmation_token": confirmation.token_id,
            "idempotency_key": idempotency_key,
            "requires_confirmation": true,
        }),
    )
}

pub async fn handle_knowledge_apply_create_draft(
    id: Option<Value>,
    params: &Value,
    state: &DaemonState,
) -> JsonRpcResponse {
    if let Some(response) = policy_gate(id.clone(), state) {
        return audited_error(state, response, ResultStatus::Denied).await;
    }
    let Some(plan_id) = string_param(params, "plan_id") else {
        return audited_field_error(state, id, "plan_id").await;
    };
    let Some(confirmation_token) = string_param(params, "confirmation_token") else {
        return audited_field_error(state, id, "confirmation_token").await;
    };
    let Some(idempotency_key) = string_param(params, "idempotency_key") else {
        return audited_field_error(state, id, "idempotency_key").await;
    };
    let stores = match stores(state) {
        Ok(stores) => stores,
        Err(err) => return internal_error(id, err),
    };
    let plan_record = match stores.plan_store.get(&plan_id).await {
        Ok(Some(record)) => record,
        Ok(None) => {
            return audited_knowledge_error(
                state,
                id,
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
        Ok(plan) if plan.tool == KNOWLEDGE_DRAFT_PLAN_TOOL => plan,
        Ok(plan) => {
            return knowledge_error(
                id,
                -32602,
                "invalid params",
                json!({"details": format!("plan {} is bound to {}", plan.plan_id, plan.tool)}),
            );
        }
        Err(err) => return internal_error(id, err),
    };
    let actor = actor_from_params(params, state);
    let requester = requester_from_params(params, &actor);
    let binding = ConfirmationBinding {
        actor: actor.clone(),
        requester: requester.clone(),
        tool: KNOWLEDGE_DRAFT_APPLY_TOOL.to_string(),
        op_hash: plan.op_hash.clone(),
        environment: state.mcp_config.environment.label.clone(),
    };
    let key = IdempotencyKey {
        value: idempotency_key.clone(),
        source: IdempotencyKeySource::ServerDerived,
    };

    if let Ok(Some(record)) = stores
        .idempotency_store
        .lookup_record(&key, KNOWLEDGE_DRAFT_APPLY_TOOL)
        && record.expires_at > Utc::now()
    {
        if record.op_hash != plan.op_hash {
            return audited_knowledge_error(
                state,
                id,
                -32052,
                "IDEMPOTENCY_CONFLICT",
                json!({"code": "IDEMPOTENCY_CONFLICT", "idempotency_key": idempotency_key}),
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
                return confirmation_invalid(state, id, err).await;
            }
            receipt.idempotency_replay = true;
            return JsonRpcResponse::ok(id, json!(receipt));
        }
        if record.apply_started_at.is_some() {
            return pending_resolution_required(state, id, &plan).await;
        }
    }
    if plan_record.state == PlanLifecycleState::Expired || plan_record.expires_at <= Utc::now() {
        let _ = stores.plan_store.mark_expired(&plan_id).await;
        return audited_knowledge_error(
            state,
            id,
            -32055,
            "PLAN_EXPIRED",
            json!({"code": "PLAN_EXPIRED", "plan_id": plan_id}),
            ResultStatus::Denied,
        )
        .await;
    }
    match stores
        .idempotency_store
        .check_and_record(
            &key,
            KNOWLEDGE_DRAFT_APPLY_TOOL,
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
                return confirmation_invalid(state, id, err).await;
            }
            receipt.idempotency_replay = true;
            return JsonRpcResponse::ok(id, json!(receipt));
        }
        Ok(IdempotencyOutcome::Conflict { .. }) => {
            return audited_knowledge_error(
                state,
                id,
                -32052,
                "IDEMPOTENCY_CONFLICT",
                json!({"code": "IDEMPOTENCY_CONFLICT", "idempotency_key": idempotency_key}),
                ResultStatus::Denied,
            )
            .await;
        }
        Err(err) => return internal_error(id, err),
    }
    match stores.confirmation_store.lookup(&confirmation_token) {
        Ok(Some(record)) => {
            if let Err(err) = validate_confirmation(&record, &binding) {
                return confirmation_invalid(state, id, err).await;
            }
        }
        Ok(None) => {
            return confirmation_invalid(state, id, ConfirmationConsumeError::NotFound).await;
        }
        Err(err) => return internal_error(id, err),
    }
    let input = match input_from_plan(&plan) {
        Ok(input) => input,
        Err(err) => return internal_error(id, err),
    };
    let apply_started_at = Utc::now();
    if let Err(err) = stores
        .idempotency_store
        .mark_apply_started(&key, KNOWLEDGE_DRAFT_APPLY_TOOL)
        .await
    {
        return internal_error(id, err);
    }
    let created = match state
        .core
        .create_knowledge_draft(
            &input.short_description,
            &input.text,
            &input.knowledge_base_sys_id,
            input.category_sys_id.as_deref(),
        )
        .await
    {
        Ok(created) => created,
        Err(_) => return pending_resolution_required(state, id, &plan).await,
    };
    if let Err(err) = stores
        .confirmation_store
        .consume(&confirmation_token, &binding)
        .await
    {
        return confirmation_invalid(state, id, err).await;
    }
    let audit_id = Uuid::new_v4().to_string();
    let receipt = receipt_for_draft(&plan, &audit_id, apply_started_at, &created, state);
    if let Err(err) = append_audit(
        state,
        &audit_id,
        Some(&plan.plan_id),
        KNOWLEDGE_DRAFT_APPLY_TOOL,
        ResultStatus::AppliedSuccess,
        Some(redacted_plan_change(&plan)),
        receipt.service_now_metadata.clone(),
        Some((&actor, &requester)),
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

#[derive(Clone)]
struct DraftInput {
    short_description: String,
    text: String,
    knowledge_base_sys_id: String,
    category_sys_id: Option<String>,
}

fn parse_plan_input(
    id: Option<Value>,
    params: &Value,
) -> std::result::Result<DraftInput, JsonRpcResponse> {
    let Some(args) = params.as_object() else {
        return Err(field_rejected(
            id,
            vec![json!({"field": "payload", "reason": "type_mismatch"})],
        ));
    };
    let allowed = BTreeSet::from([
        "short_description",
        "text",
        "knowledge_base_sys_id",
        "category_sys_id",
        "actor",
        "requester",
        "client",
        "session_id",
        "user_agent",
    ]);
    let rejected = args
        .keys()
        .filter(|field| !allowed.contains(field.as_str()))
        .map(|field| json!({"field": field, "reason": "not_in_allowlist"}))
        .collect::<Vec<_>>();
    if !rejected.is_empty() {
        return Err(field_rejected(id, rejected));
    }
    let short_description =
        bounded_string(args, "short_description", MAX_TITLE_LENGTH).map_err(|reason| {
            field_rejected(
                id.clone(),
                vec![json!({"field": "short_description", "reason": reason})],
            )
        })?;
    let text = bounded_string(args, "text", MAX_BODY_LENGTH).map_err(|reason| {
        field_rejected(id.clone(), vec![json!({"field": "text", "reason": reason})])
    })?;
    let knowledge_base_sys_id = sys_id(args, "knowledge_base_sys_id").map_err(|reason| {
        field_rejected(
            id.clone(),
            vec![json!({"field": "knowledge_base_sys_id", "reason": reason})],
        )
    })?;
    let category_sys_id = match args.get("category_sys_id") {
        Some(_) => Some(sys_id(args, "category_sys_id").map_err(|reason| {
            field_rejected(
                id.clone(),
                vec![json!({"field": "category_sys_id", "reason": reason})],
            )
        })?),
        None => None,
    };
    Ok(DraftInput {
        short_description,
        text,
        knowledge_base_sys_id,
        category_sys_id,
    })
}

fn input_from_plan(plan: &OperationPlan) -> Result<DraftInput> {
    let changes = plan
        .planned_changes
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("knowledge draft plan changes are not an object"))?;
    let mut changes = changes.clone();
    changes.remove("workflow_state");
    if let Some(knowledge_base_sys_id) = changes.remove("kb_knowledge_base") {
        changes.insert("knowledge_base_sys_id".to_string(), knowledge_base_sys_id);
    }
    if let Some(category_sys_id) = changes.remove("kb_category")
        && !category_sys_id.is_null()
    {
        changes.insert("category_sys_id".to_string(), category_sys_id);
    }
    let params = Value::Object(changes);
    parse_plan_input(None, &params)
        .map_err(|_| anyhow::anyhow!("knowledge draft plan has invalid fields"))
}

fn bounded_string(
    args: &Map<String, Value>,
    field: &str,
    max: usize,
) -> std::result::Result<String, &'static str> {
    let value = args
        .get(field)
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or("type_mismatch")?;
    if value.len() > max {
        return Err("too_long");
    }
    Ok(value.to_string())
}

fn sys_id(args: &Map<String, Value>, field: &str) -> std::result::Result<String, &'static str> {
    let value = bounded_string(args, field, 32)?;
    if value.len() == 32 && value.as_bytes().iter().all(u8::is_ascii_hexdigit) {
        Ok(value)
    } else {
        Err("invalid_sys_id")
    }
}

fn preview_for(plan: &OperationPlan) -> Value {
    let title = plan
        .planned_changes
        .get("short_description")
        .cloned()
        .unwrap_or(Value::Null);
    let base = plan
        .planned_changes
        .get("kb_knowledge_base")
        .cloned()
        .unwrap_or(Value::Null);
    let category = plan
        .planned_changes
        .get("kb_category")
        .cloned()
        .unwrap_or(Value::Null);
    let text = plan.planned_changes.get("text");
    json!({
        "short_description": title,
        "kb_knowledge_base": base,
        "kb_category": category,
        "workflow_state": "draft",
        "text_sha256": hash_value(text),
    })
}

fn redacted_plan_change(plan: &OperationPlan) -> Value {
    preview_for(plan)
}

fn hash_value(value: Option<&Value>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(
        value
            .map(|value| serde_json::to_vec(value).unwrap_or_default())
            .unwrap_or_default(),
    );
    hex::encode(hasher.finalize())
}

fn policy_gate(id: Option<Value>, state: &DaemonState) -> Option<JsonRpcResponse> {
    if state.mcp_config.policy.tool_enabled_in_environment(
        KNOWLEDGE_DRAFT_APPLY_TOOL,
        &state.mcp_config.environment.label,
    ) {
        None
    } else {
        Some(knowledge_error(
            id,
            -32040,
            "policy denied",
            json!({
                "details": "knowledge draft apply tool is disabled by current MCP policy",
                "tool": KNOWLEDGE_DRAFT_APPLY_TOOL,
            }),
        ))
    }
}

struct Stores {
    plan_store: SqlitePlanStore,
    confirmation_store: SqliteConfirmationStore,
    idempotency_store: SqliteIdempotencyStore,
    audit_sink: SqliteAuditSink,
}

fn stores(state: &DaemonState) -> Result<Stores> {
    std::fs::create_dir_all(&state.data_dir)?;
    let path = state.data_dir.join("mcp_knowledge_write.sqlite3");
    Ok(Stores {
        plan_store: SqlitePlanStore::open(&path)?,
        confirmation_store: SqliteConfirmationStore::open(&path)?,
        idempotency_store: SqliteIdempotencyStore::open(&path)?,
        audit_sink: SqliteAuditSink::open(&path)?,
    })
}

fn actor_from_params(params: &Value, state: &DaemonState) -> String {
    params
        .get("actor")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| state.core.config().instance.user.clone())
}

fn requester_from_params(params: &Value, actor: &str) -> String {
    params
        .get("requester")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
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
    record: &snow_mcp::planner::ConfirmationRecord,
    binding: &ConfirmationBinding,
) -> std::result::Result<(), ConfirmationConsumeError> {
    if record.revoked {
        return Err(ConfirmationConsumeError::Revoked);
    }
    if record.consumed {
        return Err(ConfirmationConsumeError::AlreadyConsumed);
    }
    if record.expires_at <= Utc::now() {
        return Err(ConfirmationConsumeError::Expired);
    }
    validate_binding(record, binding)
}

fn validate_replay_confirmation(
    store: &SqliteConfirmationStore,
    token: &str,
    binding: &ConfirmationBinding,
) -> std::result::Result<(), ConfirmationConsumeError> {
    match store.lookup(token) {
        Ok(Some(record)) => validate_binding(&record, binding),
        Ok(None) | Err(_) => Err(ConfirmationConsumeError::NotFound),
    }
}

fn validate_binding(
    record: &snow_mcp::planner::ConfirmationRecord,
    binding: &ConfirmationBinding,
) -> std::result::Result<(), ConfirmationConsumeError> {
    for (field, stored, supplied) in [
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
        if stored != supplied {
            return Err(ConfirmationConsumeError::BindingMismatch { field });
        }
    }
    Ok(())
}

fn receipt_for_draft(
    plan: &OperationPlan,
    audit_id: &str,
    apply_started_at: chrono::DateTime<Utc>,
    created: &snow_core::KnowledgeDraftWriteResult,
    state: &DaemonState,
) -> OperationReceipt {
    let title = plan
        .planned_changes
        .get("short_description")
        .cloned()
        .unwrap_or(Value::Null);
    let base = plan
        .planned_changes
        .get("kb_knowledge_base")
        .cloned()
        .unwrap_or(Value::Null);
    let state_value = Value::String(created.workflow_state.clone());
    OperationReceipt {
        plan_id: plan.plan_id.clone(),
        audit_id: audit_id.to_string(),
        parent_audit_id: plan.plan_id.clone(),
        tool: KNOWLEDGE_DRAFT_APPLY_TOOL.to_string(),
        status: ReceiptStatus::Success,
        applied_changes_summary: Some(
            json!({"short_description": title, "kb_knowledge_base": base, "workflow_state": state_value, "text_sha256": hash_value(plan.planned_changes.get("text"))}),
        ),
        service_now_metadata: Some(ServiceNowMetadata {
            sys_id: Some(created.record.sys_id.clone()),
            number: Some(created.record.number.clone()),
            transaction_id: None,
        }),
        idempotency_replay: false,
        completed_at: Utc::now(),
        op_hash: plan.op_hash.clone(),
        record_url: Some(format!(
            "{}/nav_to.do?uri=kb_knowledge.do?sys_id={}",
            state.core.config().instance.url.trim_end_matches('/'),
            created.record.sys_id
        )),
        record_snapshot: None,
        changed_fields: vec![
            FieldChange {
                field: "short_description".to_string(),
                before: None,
                after: Some(
                    plan.planned_changes
                        .get("short_description")
                        .cloned()
                        .unwrap_or(Value::Null),
                ),
            },
            FieldChange {
                field: "kb_knowledge_base".to_string(),
                before: None,
                after: Some(
                    plan.planned_changes
                        .get("kb_knowledge_base")
                        .cloned()
                        .unwrap_or(Value::Null),
                ),
            },
            FieldChange {
                field: "workflow_state".to_string(),
                before: None,
                after: Some(state_value),
            },
        ],
        concurrency_token_observed: None,
        apply_started_at: Some(apply_started_at),
        error_code: None,
        warnings: Vec::new(),
    }
}

async fn append_audit(
    state: &DaemonState,
    audit_id: &str,
    parent_audit_id: Option<&str>,
    tool: &str,
    status: ResultStatus,
    changes: Option<Value>,
    metadata: Option<ServiceNowMetadata>,
    identities: Option<(&str, &str)>,
) -> Result<()> {
    let stores = stores(state)?;
    let (actor, requester) = identities.unwrap_or_else(|| {
        let user = state.core.config().instance.user.as_str();
        (user, user)
    });
    let mut event = AuditEvent::new_plan(
        audit_id,
        state.mcp_config.environment.label.clone(),
        identity(actor),
        identity(requester),
        ClientIdentity {
            client_id: Some("snow_daemon".to_string()),
            user_agent: None,
            transport: "daemon_json_rpc".to_string(),
        },
        tool,
    );
    event.parent_audit_id = parent_audit_id.map(ToOwned::to_owned);
    event.result_status = status;
    event.policy_decisions = policy_decisions(status);
    event.normalized_arguments_redacted = changes.clone().unwrap_or(Value::Null);
    event.planned_changes = changes;
    event.service_now_metadata = metadata;
    stores
        .audit_sink
        .append(event)
        .await
        .map(|_| ())
        .map_err(Into::into)
}

fn identity(subject: &str) -> ActorIdentity {
    ActorIdentity {
        subject: subject.to_string(),
        display_name: subject.to_string(),
        source_claim: "mcp_request".to_string(),
    }
}

fn policy_decisions(status: ResultStatus) -> Vec<PolicyDecisionRow> {
    let verdict = if status == ResultStatus::AppliedSuccess || status == ResultStatus::Plan {
        "Allow"
    } else {
        "Deny"
    };
    vec![PolicyDecisionRow {
        gate: "Confirmation".to_string(),
        verdict: verdict.to_string(),
        reason: None,
        remediation: None,
    }]
}

async fn confirmation_invalid(
    state: &DaemonState,
    id: Option<Value>,
    err: ConfirmationConsumeError,
) -> JsonRpcResponse {
    audited_knowledge_error(
        state,
        id,
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
    plan: &OperationPlan,
) -> JsonRpcResponse {
    audited_knowledge_error(state, id, -32060, "PENDING_RESOLUTION_REQUIRED", json!({"code": "PENDING_RESOLUTION_REQUIRED", "plan_id": plan.plan_id, "retry_after_seconds": 2}), ResultStatus::Error).await
}

async fn audited_field_error(
    state: &DaemonState,
    id: Option<Value>,
    field: &str,
) -> JsonRpcResponse {
    audited_knowledge_error(
        state,
        id,
        -32051,
        "FIELD_REJECTED",
        json!({"code": "FIELD_REJECTED", "fields": [{"field": field, "reason": "type_mismatch"}]}),
        ResultStatus::Denied,
    )
    .await
}

async fn audited_error(
    state: &DaemonState,
    response: JsonRpcResponse,
    status: ResultStatus,
) -> JsonRpcResponse {
    let _ = append_audit(
        state,
        &Uuid::new_v4().to_string(),
        None,
        KNOWLEDGE_DRAFT_APPLY_TOOL,
        status,
        None,
        None,
        None,
    )
    .await;
    response
}

async fn audited_knowledge_error(
    state: &DaemonState,
    id: Option<Value>,
    code: i64,
    message: &str,
    data: Value,
    status: ResultStatus,
) -> JsonRpcResponse {
    let _ = append_audit(
        state,
        &Uuid::new_v4().to_string(),
        None,
        KNOWLEDGE_DRAFT_APPLY_TOOL,
        status,
        None,
        None,
        None,
    )
    .await;
    knowledge_error(id, code, message, data)
}

fn field_rejected(id: Option<Value>, fields: Vec<Value>) -> JsonRpcResponse {
    knowledge_error(
        id,
        -32051,
        "FIELD_REJECTED",
        json!({"code": "FIELD_REJECTED", "fields": fields}),
    )
}
fn knowledge_error(id: Option<Value>, code: i64, message: &str, data: Value) -> JsonRpcResponse {
    JsonRpcResponse::error(id, code, message, Some(data))
}
fn internal_error(id: Option<Value>, err: impl ToString) -> JsonRpcResponse {
    JsonRpcResponse::error(
        id,
        -32000,
        "internal error",
        Some(json!({"details": err.to_string()})),
    )
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use serde_json::json;
    use wiremock::matchers::{body_partial_json, method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    use crate::rpc::{JsonRpcRequest, dispatch};
    use crate::test_support::build_fixture_state_at_instance;

    fn enabled_state(fixture: &crate::test_support::FixtureState) -> Arc<DaemonState> {
        let mut policy = snow_mcp::domain::policy::PolicyConfig::default();
        policy
            .tools
            .get_mut(KNOWLEDGE_DRAFT_APPLY_TOOL)
            .expect("knowledge apply policy")
            .enabled = true;
        Arc::new(DaemonState::with_data_dir_and_mcp_config(
            Arc::clone(&fixture.state.core),
            fixture.tempdir.path().join("knowledge-draft-write"),
            snow_mcp::McpConfig {
                environment: snow_mcp::McpEnvironment::explicit_config("test", "America/Chicago"),
                policy,
                ..Default::default()
            },
        ))
    }

    #[tokio::test(flavor = "current_thread")]
    async fn daemon_plan_and_apply_create_a_verified_draft_without_returning_body_text() {
        let instance = MockServer::start().await;
        let article_sys_id = "11111111111111111111111111111111";
        let knowledge_base_sys_id = "22222222222222222222222222222222";
        let body = "<h2>Purpose</h2><p>Use predictable environment names.</p>";

        Mock::given(method("POST"))
            .and(path("/api/now/table/kb_knowledge"))
            .and(body_partial_json(json!({
                "short_description": "Environment Naming Policy",
                "text": body,
                "kb_knowledge_base": knowledge_base_sys_id,
                "workflow_state": "draft",
            })))
            .respond_with(ResponseTemplate::new(201).set_body_json(json!({
                "result": {"sys_id": article_sys_id, "number": "KB0012345"}
            })))
            .expect(1)
            .mount(&instance)
            .await;
        Mock::given(method("GET"))
            .and(path(format!("/api/now/table/kb_knowledge/{article_sys_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": {
                    "sys_id": article_sys_id,
                    "number": "KB0012345",
                    "short_description": "Environment Naming Policy",
                    "workflow_state": "draft",
                    "kb_knowledge_base": {"value": knowledge_base_sys_id, "display_value": "Example Knowledge"}
                }
            })))
            .expect(1)
            .mount(&instance)
            .await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/kb_knowledge"))
            .and(query_param("sysparm_display_value", "true"))
            .respond_with(ResponseTemplate::new(200).set_body_json(json!({
                "result": [{"sys_id": article_sys_id, "work_notes": "", "comments": ""}]
            })))
            .mount(&instance)
            .await;

        let fixture = build_fixture_state_at_instance(&instance.uri())
            .await
            .expect("fixture");
        let state = enabled_state(&fixture);
        let plan = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: KNOWLEDGE_DRAFT_PLAN_TOOL.to_string(),
                params: json!({
                    "short_description": "Environment Naming Policy",
                    "text": body,
                    "knowledge_base_sys_id": knowledge_base_sys_id,
                }),
                id: Some(json!(1)),
            },
            &state,
        )
        .await;
        assert!(plan.error.is_none(), "{plan:?}");
        let plan = plan.result.expect("plan result");
        assert!(plan["preview"]["text_sha256"].is_string());
        assert_ne!(plan["preview"].to_string(), body);

        let applied = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: KNOWLEDGE_DRAFT_APPLY_TOOL.to_string(),
                params: json!({
                    "plan_id": plan["plan_id"],
                    "confirmation_token": plan["confirmation_token"],
                    "idempotency_key": plan["idempotency_key"],
                }),
                id: Some(json!(2)),
            },
            &state,
        )
        .await;
        assert!(applied.error.is_none(), "{applied:?}");
        let receipt = applied.result.expect("receipt");
        assert_eq!(receipt["status"], "success");
        assert_eq!(receipt["service_now_metadata"]["number"], "KB0012345");
        assert_eq!(
            receipt["applied_changes_summary"]["workflow_state"],
            "draft"
        );
        assert!(
            !receipt["changed_fields"]
                .as_array()
                .expect("changed fields")
                .iter()
                .any(|field| field["field"] == "text")
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn plan_rejects_publish_fields_before_a_write_can_be_planned() {
        let fixture = build_fixture_state_at_instance("http://localhost")
            .await
            .expect("fixture");
        let state = enabled_state(&fixture);
        let response = handle_knowledge_plan_create_draft(
            Some(json!(1)),
            &json!({
                "short_description": "Example",
                "text": "<p>body</p>",
                "knowledge_base_sys_id": "22222222222222222222222222222222",
                "workflow_state": "published",
            }),
            &state,
        )
        .await;
        let error = response.error.expect("reject publish field");
        assert_eq!(error.code, -32051);
        assert_eq!(
            error.data.expect("typed error")["fields"][0]["field"],
            "workflow_state"
        );
    }
}
