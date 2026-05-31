use std::collections::BTreeSet;
use std::path::PathBuf;

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
    ConfirmationBinding, ConfirmationConsumeError, ConfirmationRecord, ConfirmationStore,
    FieldChange, IdempotencyOutcome, IdempotencyStore, OperationPlan, OperationPlanBuilder,
    OperationReceipt, PlanLifecycleState, PlanStore, PlanStoreRecord, ReceiptStatus,
    SqliteConfirmationStore, SqliteIdempotencyStore, SqlitePlanStore,
};
use uuid::Uuid;

use crate::DaemonState;
use crate::rpc::JsonRpcResponse;

const PLAN_TTL_SECONDS: i64 = 600;
const WORK_NOTE_PLAN_TOOL: &str = "work_note_plan_add";
const WORK_NOTE_APPLY_TOOL: &str = "work_note_apply_add";

pub async fn handle_work_note_plan_add(
    id: Option<Value>,
    params: &Value,
    state: &DaemonState,
) -> JsonRpcResponse {
    if !state
        .mcp_config
        .policy
        .tool_enabled_in_environment(WORK_NOTE_APPLY_TOOL, &state.mcp_config.environment.label)
    {
        return work_note_error(
            id,
            -32040,
            "policy denied",
            json!({
                "details": "work-note apply tool is disabled by current MCP policy",
                "tool": WORK_NOTE_APPLY_TOOL,
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

    let Some(number) = string_param(params, "number") else {
        return field_rejected(
            id,
            vec![json!({"field": "number", "reason": "type_mismatch"})],
        );
    };
    let text = match note_text(args) {
        Ok(text) => text,
        Err(fields) => return field_rejected(id, fields),
    };

    let record = match get_record_cached_or_fresh(state, &number).await {
        Ok(Some(record)) => record,
        Ok(None) => return JsonRpcResponse::error(id, -32004, "record not found", None),
        Err(err) => return internal_error(id, err),
    };

    let plan = OperationPlanBuilder::new(WORK_NOTE_PLAN_TOOL)
        .target(record_ref_from_snow(&record))
        .planned_changes(json!({ "work_notes": text }))
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
            Value::String(WORK_NOTE_APPLY_TOOL.to_string()),
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
            WORK_NOTE_APPLY_TOOL,
            &plan.op_hash,
            state.mcp_config.policy.idempotency_window_seconds,
        )
        .await
    {
        Ok(IdempotencyOutcome::NewKey) => {}
        Ok(IdempotencyOutcome::Conflict { .. }) => {
            return work_note_error(
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

    let record_row = PlanStoreRecord {
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
    if let Err(err) = stores.plan_store.put(record_row).await {
        return internal_error(id, err);
    }

    if let Err(err) = append_audit_event(
        state,
        &plan.plan_id,
        None,
        WORK_NOTE_PLAN_TOOL,
        ResultStatus::Plan,
        Some(redacted_work_note_change(&plan)),
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
                tool: WORK_NOTE_APPLY_TOOL.to_string(),
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
    if let Err(err) = append_confirmation_audit(
        state,
        plan.plan_id.as_str(),
        "confirmation_issue",
        &actor,
        &requester,
    )
    .await
    {
        return internal_error(id, err);
    }

    JsonRpcResponse::ok(
        id,
        json!({
            "plan_id": plan.plan_id,
            "op_hash": plan.op_hash,
            "target": {
                "sys_id": record.sys_id,
                "number": record.number,
                "table": record.table,
            },
            "preview": plan.planned_changes,
            "recent_work_note_count": record.work_notes.len(),
            "expires_at": expires_at.to_rfc3339(),
            "confirmation_token": confirmation.token_id,
            "idempotency_key": idempotency_key,
            "requires_confirmation": true,
        }),
    )
}

pub async fn handle_work_note_apply_add(
    id: Option<Value>,
    params: &Value,
    state: &DaemonState,
) -> JsonRpcResponse {
    if !state
        .mcp_config
        .policy
        .tool_enabled_in_environment(WORK_NOTE_APPLY_TOOL, &state.mcp_config.environment.label)
    {
        return audited_work_note_error(
            state,
            id,
            -32040,
            "policy denied",
            json!({
                "details": "work-note apply tool is disabled by current MCP policy",
                "tool": WORK_NOTE_APPLY_TOOL,
            }),
            ResultStatus::Denied,
        )
        .await;
    }

    let Some(plan_id) = string_param(params, "plan_id") else {
        return audited_work_note_error(
            state,
            id,
            -32051,
            "FIELD_REJECTED",
            json!({"code": "FIELD_REJECTED", "fields": [{"field": "plan_id", "reason": "type_mismatch"}]}),
            ResultStatus::Denied,
        )
        .await;
    };
    let Some(confirmation_token) = string_param(params, "confirmation_token") else {
        return audited_work_note_error(
            state,
            id,
            -32051,
            "FIELD_REJECTED",
            json!({"code": "FIELD_REJECTED", "fields": [{"field": "confirmation_token", "reason": "type_mismatch"}]}),
            ResultStatus::Denied,
        )
        .await;
    };
    let Some(idempotency_key) = string_param(params, "idempotency_key") else {
        return audited_work_note_error(
            state,
            id,
            -32051,
            "FIELD_REJECTED",
            json!({"code": "FIELD_REJECTED", "fields": [{"field": "idempotency_key", "reason": "type_mismatch"}]}),
            ResultStatus::Denied,
        )
        .await;
    };

    let stores = match stores(state) {
        Ok(stores) => stores,
        Err(err) => return internal_error(id, err),
    };
    let plan_record = match stores.plan_store.get(&plan_id).await {
        Ok(Some(record)) => record,
        Ok(None) => {
            return audited_work_note_error(
                state,
                id,
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
    if plan.tool != WORK_NOTE_PLAN_TOOL {
        return work_note_error(
            id,
            -32602,
            "invalid params",
            json!({ "details": format!("plan {} is bound to {}", plan.plan_id, plan.tool) }),
        );
    }

    let actor = actor_from_params(params, state);
    let requester = requester_from_params(params, &actor);
    let confirmation_binding = ConfirmationBinding {
        actor: actor.clone(),
        requester: requester.clone(),
        tool: WORK_NOTE_APPLY_TOOL.to_string(),
        op_hash: plan.op_hash.clone(),
        environment: state.mcp_config.environment.label.clone(),
    };
    let key = IdempotencyKey {
        value: idempotency_key.clone(),
        source: IdempotencyKeySource::ServerDerived,
    };

    if let Ok(Some(record)) = stores
        .idempotency_store
        .lookup_record(&key, WORK_NOTE_APPLY_TOOL)
        && record.expires_at > Utc::now()
    {
        if record.op_hash != plan.op_hash {
            return audited_work_note_error(
                state,
                id,
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
        return audited_work_note_error(
            state,
            id,
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

    match stores
        .idempotency_store
        .check_and_record(
            &key,
            WORK_NOTE_APPLY_TOOL,
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
                &confirmation_binding,
            ) {
                return confirmation_invalid(state, id, err).await;
            }
            receipt.idempotency_replay = true;
            return JsonRpcResponse::ok(id, json!(receipt));
        }
        Ok(IdempotencyOutcome::Conflict { .. }) => {
            return audited_work_note_error(
                state,
                id,
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

    match stores.confirmation_store.lookup(&confirmation_token) {
        Ok(Some(confirmation)) => {
            if let Err(err) = validate_confirmation(&confirmation, &confirmation_binding) {
                return confirmation_invalid(state, id, err).await;
            }
        }
        Ok(None) => {
            return confirmation_invalid(state, id, ConfirmationConsumeError::NotFound).await;
        }
        Err(err) => return internal_error(id, err),
    }

    let Some(target) = plan.target.as_ref() else {
        return internal_error(id, "work-note plan missing target");
    };
    let Some(text) = plan
        .planned_changes
        .get("work_notes")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
    else {
        return internal_error(id, "work-note plan missing work_notes");
    };

    let apply_started_at = Utc::now();
    if let Err(err) = stores
        .idempotency_store
        .mark_apply_started(&key, WORK_NOTE_APPLY_TOOL)
        .await
    {
        return internal_error(id, err);
    }

    let record = match state.core.add_work_note(&target.number, &text).await {
        Ok(Some(record)) => record,
        Ok(None) => {
            return audited_work_note_error(
                state,
                id,
                -32004,
                "record not found",
                json!({ "details": format!("record {} not found", target.number) }),
                ResultStatus::Error,
            )
            .await;
        }
        Err(err) => {
            return pending_resolution_required_with_reason(state, id, &plan, err.to_string())
                .await;
        }
    };

    if let Err(err) = stores
        .confirmation_store
        .consume(&confirmation_token, &confirmation_binding)
        .await
    {
        return confirmation_invalid(state, id, err).await;
    }
    if let Err(err) = append_confirmation_audit(
        state,
        plan.plan_id.as_str(),
        "confirmation_consume",
        &actor,
        &requester,
    )
    .await
    {
        return internal_error(id, err);
    }

    let audit_id = Uuid::new_v4().to_string();
    let receipt = receipt_for_work_note(&plan, &audit_id, apply_started_at, record, &text, state);
    let applied_changes = vec![AppliedChange {
        field: "work_notes".to_string(),
        old_hash: hash_json_value(None),
        new_hash: hash_json_value(Some(&Value::String(text))),
        redacted_preview: None,
    }];
    if let Err(err) = append_audit_event(
        state,
        &audit_id,
        Some(plan.plan_id.as_str()),
        WORK_NOTE_APPLY_TOOL,
        ResultStatus::AppliedSuccess,
        Some(
            receipt
                .applied_changes_summary
                .clone()
                .unwrap_or(Value::Null),
        ),
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

struct WorkNoteStores {
    plan_store: SqlitePlanStore,
    confirmation_store: SqliteConfirmationStore,
    idempotency_store: SqliteIdempotencyStore,
    audit_sink: SqliteAuditSink,
}

fn stores(state: &DaemonState) -> Result<WorkNoteStores> {
    std::fs::create_dir_all(&state.data_dir)?;
    let path = work_note_store_path(state);
    Ok(WorkNoteStores {
        plan_store: SqlitePlanStore::open(&path)?,
        confirmation_store: SqliteConfirmationStore::open(&path)?,
        idempotency_store: SqliteIdempotencyStore::open(&path)?,
        audit_sink: SqliteAuditSink::open(&path)?,
    })
}

fn work_note_store_path(state: &DaemonState) -> PathBuf {
    state.data_dir.join("mcp_story_write.sqlite3")
}

async fn get_record_cached_or_fresh(
    state: &DaemonState,
    number: &str,
) -> Result<Option<SnowRecord>> {
    match state.core.get_record(number).await? {
        Some(record) => Ok(Some(record)),
        None => state.core.get_record_fresh(number).await,
    }
}

fn reject_plan_input_fields(
    id: Option<Value>,
    args: &Map<String, Value>,
) -> Option<JsonRpcResponse> {
    let allowed = BTreeSet::from(["number", "text", "work_notes"]);
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

fn note_text(args: &Map<String, Value>) -> std::result::Result<String, Vec<Value>> {
    let text = optional_non_empty_string(args, "text");
    let work_notes = optional_non_empty_string(args, "work_notes");
    match (text, work_notes) {
        (Ok(Some(text)), Ok(Some(work_notes))) if text != work_notes => Err(vec![json!({
            "field": "work_notes",
            "reason": "conflicting_alias",
        })]),
        (Ok(Some(text)), _) => Ok(text),
        (_, Ok(Some(work_notes))) => Ok(work_notes),
        (Err(field), _) | (_, Err(field)) => Err(vec![field]),
        _ => Err(vec![json!({"field": "text", "reason": "type_mismatch"})]),
    }
}

fn optional_non_empty_string(
    args: &Map<String, Value>,
    field: &str,
) -> std::result::Result<Option<String>, Value> {
    match args.get(field) {
        Some(Value::String(value)) if !value.trim().is_empty() => Ok(Some(value.clone())),
        Some(_) => Err(json!({"field": field, "reason": "type_mismatch"})),
        None => Ok(None),
    }
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

fn validate_confirmation(
    record: &ConfirmationRecord,
    binding: &ConfirmationBinding,
) -> std::result::Result<(), ConfirmationConsumeError> {
    validate_confirmation_binding(record, binding)?;
    if record.consumed {
        return Err(ConfirmationConsumeError::AlreadyConsumed);
    }
    if record.expires_at <= Utc::now() {
        return Err(ConfirmationConsumeError::Expired);
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

fn validate_confirmation_binding(
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

fn receipt_for_work_note(
    plan: &OperationPlan,
    audit_id: &str,
    apply_started_at: chrono::DateTime<Utc>,
    record: SnowRecord,
    text: &str,
    state: &DaemonState,
) -> OperationReceipt {
    let record_url = format!(
        "{}/nav_to.do?uri={}.do?sys_id={}",
        state.core.config().instance.url.trim_end_matches('/'),
        record.table,
        record.sys_id
    );
    let text_value = Value::String(text.to_string());
    OperationReceipt {
        plan_id: plan.plan_id.clone(),
        audit_id: audit_id.to_string(),
        parent_audit_id: plan.plan_id.clone(),
        tool: WORK_NOTE_APPLY_TOOL.to_string(),
        status: ReceiptStatus::Success,
        applied_changes_summary: Some(json!({
            "field": "work_notes",
            "new_hash": hash_json_value(Some(&text_value)),
        })),
        service_now_metadata: Some(ServiceNowMetadata {
            sys_id: Some(record.sys_id.clone()),
            number: Some(record.number.clone()),
            transaction_id: None,
        }),
        idempotency_replay: false,
        completed_at: Utc::now(),
        op_hash: plan.op_hash.clone(),
        record_url: Some(record_url),
        record_snapshot: None,
        changed_fields: vec![FieldChange {
            field: "work_notes".to_string(),
            before: None,
            after: Some(text_value),
        }],
        concurrency_token_observed: None,
        apply_started_at: Some(apply_started_at),
        error_code: None,
        warnings: Vec::new(),
    }
}

fn redacted_work_note_change(plan: &OperationPlan) -> Value {
    let text = plan
        .planned_changes
        .get("work_notes")
        .cloned()
        .unwrap_or(Value::Null);
    json!({
        "target": plan.target,
        "field": "work_notes",
        "new_hash": hash_json_value(Some(&text)),
    })
}

fn record_ref_from_snow(record: &SnowRecord) -> RecordRef {
    RecordRef {
        sys_id: record.sys_id.clone(),
        number: record.number.clone(),
        table: record.table.clone(),
    }
}

fn hash_json_value(value: Option<&Value>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(
        value
            .map(|value| serde_json::to_vec(value).unwrap_or_default())
            .unwrap_or_default(),
    );
    hex::encode(hasher.finalize())
}

async fn append_confirmation_audit(
    state: &DaemonState,
    plan_id: &str,
    tool: &str,
    actor: &str,
    requester: &str,
) -> Result<()> {
    append_audit_event(
        state,
        &Uuid::new_v4().to_string(),
        Some(plan_id),
        tool,
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
) -> Result<AuditEvent> {
    let stores = stores(state)?;
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
    event.policy_decisions = policy_decisions_for(status, error.as_ref());
    event.normalized_arguments_redacted = planned_changes.clone().unwrap_or(Value::Null);
    event.planned_changes = planned_changes;
    event.applied_changes = applied_changes;
    event.service_now_metadata = service_now_metadata;
    event.error = error;
    stores.audit_sink.append(event).await.map_err(Into::into)
}

fn policy_decisions_for(status: ResultStatus, error: Option<&ErrorRow>) -> Vec<PolicyDecisionRow> {
    let gates = match error.map(|error| error.code.as_str()) {
        Some("FIELD_REJECTED") => vec![("FieldAllowlist", "Deny")],
        Some("CONFIRMATION_INVALID") | Some("PLAN_EXPIRED") => vec![("Confirmation", "Deny")],
        Some("IDEMPOTENCY_CONFLICT") | Some("PENDING_RESOLUTION_REQUIRED") => {
            vec![("Idempotency", "Deny")]
        }
        Some("policy denied") => vec![("EnvironmentGate", "Deny")],
        _ if status == ResultStatus::AppliedSuccess => vec![
            ("Confirmation", "Allow"),
            ("Idempotency", "Allow"),
            ("FieldAllowlist", "Allow"),
        ],
        _ if status == ResultStatus::Plan => vec![("FieldAllowlist", "Allow")],
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

fn audit_identity(subject: &str) -> ActorIdentity {
    ActorIdentity {
        subject: subject.to_string(),
        display_name: subject.to_string(),
        source_claim: "mcp_request".to_string(),
    }
}

async fn confirmation_invalid(
    state: &DaemonState,
    id: Option<Value>,
    err: ConfirmationConsumeError,
) -> JsonRpcResponse {
    audited_work_note_error(
        state,
        id,
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
    plan: &OperationPlan,
) -> JsonRpcResponse {
    audited_work_note_error(
        state,
        id,
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

async fn pending_resolution_required_with_reason(
    state: &DaemonState,
    id: Option<Value>,
    plan: &OperationPlan,
    reason: String,
) -> JsonRpcResponse {
    audited_work_note_error(
        state,
        id,
        -32060,
        "PENDING_RESOLUTION_REQUIRED",
        json!({
            "code": "PENDING_RESOLUTION_REQUIRED",
            "plan_id": plan.plan_id,
            "reason": reason,
            "retry_after_seconds": 2,
        }),
        ResultStatus::Error,
    )
    .await
}

async fn audited_work_note_error(
    state: &DaemonState,
    id: Option<Value>,
    code: i64,
    message: &str,
    data: Value,
    status: ResultStatus,
) -> JsonRpcResponse {
    let _ = append_audit_event(
        state,
        &Uuid::new_v4().to_string(),
        None,
        WORK_NOTE_APPLY_TOOL,
        status,
        None,
        None,
        Some(ErrorRow {
            code: message.to_string(),
            reason: data.to_string(),
            retryable: false,
            transient: false,
        }),
        None,
        None,
    )
    .await;
    work_note_error(id, code, message, data)
}

fn field_rejected(id: Option<Value>, fields: Vec<Value>) -> JsonRpcResponse {
    work_note_error(
        id,
        -32051,
        "FIELD_REJECTED",
        json!({ "code": "FIELD_REJECTED", "fields": fields }),
    )
}

fn work_note_error(id: Option<Value>, code: i64, message: &str, data: Value) -> JsonRpcResponse {
    JsonRpcResponse::error(id, code, message, Some(data))
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

    #[tokio::test(flavor = "current_thread")]
    async fn work_note_plan_add_accepts_explicit_work_notes_field() {
        let fixture = crate::test_support::build_fixture_state()
            .await
            .expect("fixture");

        let response = handle_work_note_plan_add(
            Some(json!(1)),
            &json!({
                "number": "CHG001",
                "work_notes": "Implementation is queued for validation."
            }),
            &fixture.state,
        )
        .await;

        assert!(response.error.is_none(), "{response:?}");
        let result = response.result.expect("result");
        assert_eq!(result["target"]["number"], json!("CHG001"));
        assert_eq!(
            result["preview"]["work_notes"],
            json!("Implementation is queued for validation.")
        );
        assert!(result["confirmation_token"].as_str().is_some());
        assert!(result["idempotency_key"].as_str().is_some());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn work_note_plan_add_rejects_description_alias() {
        let fixture = crate::test_support::build_fixture_state()
            .await
            .expect("fixture");

        let response = handle_work_note_plan_add(
            Some(json!(1)),
            &json!({
                "number": "CHG001",
                "description": "This must not be treated as a journal note."
            }),
            &fixture.state,
        )
        .await;

        let error = response.error.expect("description should be rejected");
        assert_eq!(error.code, -32051);
        assert_eq!(error.message, "FIELD_REJECTED");
        assert_eq!(
            error.data.expect("data")["fields"],
            json!([{"field": "description", "reason": "not_in_allowlist"}])
        );
    }
}
