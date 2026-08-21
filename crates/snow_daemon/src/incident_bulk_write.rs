//! Governed finite bulk Incident updates (B-OPS-08 / T-OPS-04).

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use snow_core::{FieldValue, IncidentGetInput, IncidentReadError};
use snow_mcp::domain::audit::{ErrorRow, ResultStatus};
use snow_mcp::planner::{
    ConfirmationBinding, ConfirmationConsumeError, ConfirmationStore, SqliteConfirmationStore,
};
use uuid::Uuid;

use crate::DaemonState;
use crate::rpc::{JsonRpcError, JsonRpcResponse};

const PLAN_TOOL: &str = "incident_bulk_plan_update";
const APPLY_TOOL: &str = "incident_bulk_apply_update";
const PLAN_TTL_SECONDS: i64 = 600;
const MIN_TARGETS: usize = 3;
const MAX_TARGETS: usize = 25;
const JOURNAL_MAX_CHARS: usize = 16_000;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BulkPlanRequest {
    #[serde(default)]
    shared_patch: Option<IncidentPatch>,
    targets: Vec<BulkTargetRequest>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BulkTargetRequest {
    #[serde(default)]
    number: Option<String>,
    #[serde(default)]
    sys_id: Option<String>,
    #[serde(default)]
    patch: Option<IncidentPatch>,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct IncidentPatch {
    #[serde(default)]
    assigned_to: Option<String>,
    #[serde(default)]
    assignment_group: Option<String>,
    #[serde(default)]
    state: Option<String>,
    #[serde(default)]
    work_notes: Option<String>,
    #[serde(default)]
    comments: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct BulkConcurrencyToken {
    sys_id: String,
    sys_updated_on: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct BulkApplyRequest {
    plan_id: String,
    confirmation_token: String,
    idempotency_key: String,
    concurrency_tokens: Vec<BulkConcurrencyToken>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BulkPlan {
    plan_id: String,
    op_hash: String,
    actor: String,
    requester: String,
    environment: String,
    preview: BulkPreview,
    created_at: DateTime<Utc>,
    expires_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BulkPreview {
    targets: Vec<BulkPlannedTarget>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BulkPlannedTarget {
    target: BulkTargetIdentity,
    patch: BTreeMap<String, String>,
    concurrency_token: BulkPreviewConcurrencyToken,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BulkTargetIdentity {
    number: String,
    sys_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BulkPreviewConcurrencyToken {
    sys_updated_on: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BulkReceipt {
    plan_id: String,
    audit_id: String,
    parent_audit_id: String,
    tool: String,
    status: BulkReceiptStatus,
    op_hash: String,
    idempotency_replay: bool,
    target_results: Vec<BulkTargetResult>,
    applied_count: usize,
    failed_count: usize,
    not_attempted_count: usize,
    cache_coherent: bool,
    apply_started_at: DateTime<Utc>,
    completed_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum BulkReceiptStatus {
    Success,
    Partial,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BulkTargetResult {
    number: String,
    sys_id: String,
    status: BulkTargetStatus,
    changed_fields: Vec<BulkFieldChange>,
    observed_sys_updated_on: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    error_code: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum BulkTargetStatus {
    Applied,
    Failed,
    NotAttempted,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct BulkFieldChange {
    field: String,
    before_hash: String,
    after_hash: String,
}

pub async fn handle_plan(
    id: Option<Value>,
    params: &Value,
    state: &DaemonState,
) -> JsonRpcResponse {
    if let Some(response) = policy_denial(id.clone(), state, PLAN_TOOL, true) {
        return response;
    }
    // Planning authority is intentionally coupled to matching apply authority.
    if let Some(response) = policy_denial(id.clone(), state, APPLY_TOOL, true) {
        return response;
    }
    let request: BulkPlanRequest = match serde_json::from_value(params.clone()) {
        Ok(request) => request,
        Err(error) => return invalid_params(id, error),
    };
    let max_targets = match configured_bulk_target_limit(state) {
        Ok(max_targets) => max_targets,
        Err(reason) => return denied(id, "MAX_TARGETS_INVALID", reason),
    };
    if request.targets.len() < MIN_TARGETS || request.targets.len() > max_targets {
        return denied(
            id,
            "TARGET_COUNT_INVALID",
            format!("bulk Incident targets must be between {MIN_TARGETS} and {max_targets}"),
        );
    }

    let shared = match normalize_patch(request.shared_patch.as_ref(), state).await {
        Ok(patch) => patch,
        Err(error) => return build_error_response(id, error),
    };
    let mut planned = Vec::with_capacity(request.targets.len());
    let mut seen = BTreeSet::new();
    for target in request.targets {
        let selector = match target_selector(&target) {
            Ok(selector) => selector,
            Err(error) => return build_error_response(id, error),
        };
        let target_patch = match normalize_patch(target.patch.as_ref(), state).await {
            Ok(patch) => patch,
            Err(error) => return build_error_response(id, error),
        };
        if let Some(field) = shared
            .keys()
            .find(|field| target_patch.contains_key(*field))
        {
            return denied(
                id,
                "PATCH_OVERLAP",
                format!("field `{field}` appears in shared_patch and target patch"),
            );
        }
        let mut effective_patch = shared.clone();
        effective_patch.extend(target_patch);
        if effective_patch.is_empty() {
            return denied(
                id,
                "EMPTY_PATCH",
                "every target requires at least one effective patch field",
            );
        }
        if let Some(field) = effective_patch
            .keys()
            .find(|field| !field_allowed(state, field))
        {
            return denied(
                id,
                "FIELD_REJECTED",
                format!("field `{field}` is not enabled by policy"),
            );
        }
        let record = match read_target(state, selector).await {
            Ok(record) => record,
            Err(error) => return build_error_response(id, error),
        };
        if !seen.insert(record.sys_id.clone()) {
            return denied(
                id,
                "DUPLICATE_TARGET",
                "targets resolve to the same canonical sys_id",
            );
        }
        planned.push(BulkPlannedTarget {
            target: BulkTargetIdentity {
                number: record.number,
                sys_id: record.sys_id,
            },
            patch: effective_patch,
            concurrency_token: BulkPreviewConcurrencyToken {
                sys_updated_on: record.sys_updated_on,
            },
        });
    }
    planned.sort_by(|left, right| left.target.sys_id.cmp(&right.target.sys_id));

    let now = Utc::now();
    let expires_at = now + chrono::Duration::seconds(PLAN_TTL_SECONDS);
    let actor = state.core.config().instance.user.clone();
    let requester = actor.clone();
    let preview = BulkPreview { targets: planned };
    let op_hash = operation_hash(&preview);
    let plan = BulkPlan {
        plan_id: Uuid::new_v4().to_string(),
        op_hash: op_hash.clone(),
        actor: actor.clone(),
        requester: requester.clone(),
        environment: state.mcp_config.environment.label.clone(),
        preview,
        created_at: now,
        expires_at,
    };
    let store = match BulkStore::open(store_path(state)) {
        Ok(store) => store,
        Err(error) => return internal_error(id, error),
    };
    if let Err(error) = store.put_plan(&plan) {
        return internal_error(id, error);
    }
    let confirmation_store = match SqliteConfirmationStore::open(store_path(state)) {
        Ok(store) => store,
        Err(error) => return internal_error(id, error),
    };
    let confirmation = match confirmation_store
        .issue(
            &plan.plan_id,
            ConfirmationBinding {
                actor,
                requester,
                tool: APPLY_TOOL.to_string(),
                op_hash: op_hash.clone(),
                environment: state.mcp_config.environment.label.clone(),
            },
            PLAN_TTL_SECONDS as u64,
        )
        .await
    {
        Ok(confirmation) => confirmation,
        Err(error) => return internal_error(id, error),
    };
    let idempotency_key = Uuid::new_v4().to_string();
    if let Err(error) = store.reserve_idempotency(
        &idempotency_key,
        APPLY_TOOL,
        &op_hash,
        state.mcp_config.policy.idempotency_window_seconds,
    ) {
        return internal_error(id, error);
    }
    let audit_summary = redacted_plan_summary(&plan);
    if let Err(error) = crate::change_write::append_audit_event(
        state,
        &plan.plan_id,
        None,
        PLAN_TOOL,
        ResultStatus::Plan,
        Some(audit_summary),
        None,
        None,
        Some((&plan.actor, &plan.requester)),
        None,
    )
    .await
    {
        return internal_error(id, error);
    }

    JsonRpcResponse::ok(
        id,
        json!({
            "plan_id": plan.plan_id,
            "op_hash": plan.op_hash,
            "apply_tool": APPLY_TOOL,
            "preview": plan.preview,
            "expires_at": plan.expires_at.to_rfc3339(),
            "confirmation_token": confirmation.token_id,
            "idempotency_key": idempotency_key,
        }),
    )
}

pub async fn handle_apply(
    id: Option<Value>,
    params: &Value,
    state: &DaemonState,
) -> JsonRpcResponse {
    if kill_switched() {
        return denied(id, "KILL_SWITCH", "Incident writes are kill-switched");
    }
    if let Some(response) = policy_denial(id.clone(), state, APPLY_TOOL, true) {
        return response;
    }
    if let Some(response) = policy_denial(id.clone(), state, PLAN_TOOL, true) {
        return response;
    }
    let request: BulkApplyRequest = match serde_json::from_value(params.clone()) {
        Ok(request) => request,
        Err(error) => return invalid_params(id, error),
    };
    let store = match BulkStore::open(store_path(state)) {
        Ok(store) => store,
        Err(error) => return internal_error(id, error),
    };
    let plan = match store.get_plan(&request.plan_id) {
        Ok(Some(plan)) => plan,
        Ok(None) => return denied(id, "PLAN_NOT_FOUND", "bulk plan was not found"),
        Err(error) => return internal_error(id, error),
    };
    if plan.expires_at <= Utc::now() {
        return denied(id, "PLAN_EXPIRED", "bulk plan expired");
    }
    let max_targets = match configured_bulk_target_limit(state) {
        Ok(limit) => limit,
        Err(reason) => return denied(id, "MAX_TARGETS_INVALID", reason),
    };
    if plan.preview.targets.len() < MIN_TARGETS || plan.preview.targets.len() > max_targets {
        return denied(
            id,
            "TARGET_COUNT_INVALID",
            "planned target count is no longer allowed",
        );
    }
    if let Err(reason) = validate_supplied_tokens(&request.concurrency_tokens, &plan) {
        return denied(id, "CONCURRENCY_TOKEN_INVALID", reason);
    }
    let current_actor = state.core.config().instance.user.clone();
    let binding = ConfirmationBinding {
        actor: current_actor.clone(),
        requester: current_actor,
        tool: APPLY_TOOL.to_string(),
        op_hash: plan.op_hash.clone(),
        environment: state.mcp_config.environment.label.clone(),
    };
    for (field, planned, current) in [
        ("actor", plan.actor.as_str(), binding.actor.as_str()),
        (
            "requester",
            plan.requester.as_str(),
            binding.requester.as_str(),
        ),
        (
            "environment",
            plan.environment.as_str(),
            binding.environment.as_str(),
        ),
    ] {
        if planned != current {
            return confirmation_error(id, ConfirmationConsumeError::BindingMismatch { field });
        }
    }

    match store.lookup_idempotency(&request.idempotency_key, APPLY_TOOL) {
        Ok(Some(record)) if record.op_hash != plan.op_hash => {
            return denied(
                id,
                "IDEMPOTENCY_CONFLICT",
                "idempotency key is bound to another operation hash",
            );
        }
        Ok(Some(record)) => {
            if let Some(error) = record.terminal_error {
                if let Err(validation_error) = validate_confirmation_replay(
                    &request.confirmation_token,
                    &binding,
                    store_path(state),
                ) {
                    return confirmation_error(id, validation_error);
                }
                return JsonRpcResponse {
                    jsonrpc: "2.0".to_string(),
                    result: None,
                    error: Some(error),
                    id,
                };
            }
            if let Some(mut receipt) = record.receipt {
                if let Err(error) = validate_confirmation_replay(
                    &request.confirmation_token,
                    &binding,
                    store_path(state),
                ) {
                    return confirmation_error(id, error);
                }
                receipt.idempotency_replay = true;
                return JsonRpcResponse::ok(id, json!(receipt));
            }
            if record.apply_started_at.is_some() {
                return denied(
                    id,
                    "PENDING_RESOLUTION_REQUIRED",
                    "an apply attempt started without a durable final receipt",
                );
            }
        }
        Ok(None) => {
            return denied(
                id,
                "IDEMPOTENCY_CONFLICT",
                "idempotency key is not bound to this plan",
            );
        }
        Err(error) => return internal_error(id, error),
    }

    let confirmation_store = match SqliteConfirmationStore::open(store_path(state)) {
        Ok(store) => store,
        Err(error) => return internal_error(id, error),
    };
    let confirmation = match confirmation_store.lookup(&request.confirmation_token) {
        Ok(Some(confirmation)) => confirmation,
        Ok(None) => return confirmation_error(id, ConfirmationConsumeError::NotFound),
        Err(error) => return internal_error(id, error),
    };
    if let Err(error) = validate_confirmation_record(&confirmation, &binding) {
        return confirmation_error(id, error);
    }

    // All-target preflight is complete before the durable pending marker and
    // before the first PATCH. Every failure here therefore proves zero writes.
    for target in &plan.preview.targets {
        let current =
            match read_target(state, TargetSelector::SysId(target.target.sys_id.clone())).await {
                Ok(current) => current,
                Err(error) => return build_error_response(id, error),
            };
        if current.sys_updated_on != target.concurrency_token.sys_updated_on {
            return JsonRpcResponse::error(
                id,
                -32053,
                "CONCURRENCY_CONFLICT",
                Some(json!({
                    "code": "CONCURRENCY_CONFLICT",
                    "reason": format!("target {} changed after planning", target.target.sys_id),
                })),
            );
        }
    }

    let attempt_audit_id = Uuid::new_v4().to_string();
    if let Err(error) = crate::change_write::append_audit_event(
        state,
        &attempt_audit_id,
        Some(&plan.plan_id),
        APPLY_TOOL,
        ResultStatus::Plan,
        Some(redacted_plan_summary(&plan)),
        None,
        None,
        Some((&binding.actor, &binding.requester)),
        None,
    )
    .await
    {
        return internal_error(id, error);
    }
    let apply_started_at = Utc::now();
    match store.mark_apply_started(&request.idempotency_key, APPLY_TOOL) {
        Ok(true) => {}
        Ok(false) => {
            return denied(
                id,
                "PENDING_RESOLUTION_REQUIRED",
                "another apply attempt already acquired this idempotency key",
            );
        }
        Err(error) => return pending_after_store_failure(id, error),
    }
    if let Err(error) = confirmation_store
        .consume(&request.confirmation_token, &binding)
        .await
    {
        if let Err(clear_error) = store.clear_apply_started(&request.idempotency_key, APPLY_TOOL) {
            return pending_after_store_failure(id, clear_error);
        }
        return confirmation_error(id, error);
    }
    let audit_id = Uuid::new_v4().to_string();
    let mut receipt = initial_receipt(&plan, &audit_id, apply_started_at);
    if let Err(error) = store.save_progress(&request.idempotency_key, &receipt) {
        return pending_after_store_failure(id, error);
    }

    let mut failure: Option<(String, bool)> = None;
    let mut failure_diagnostic: Option<String> = None;
    for index in 0..plan.preview.targets.len() {
        let target = &plan.preview.targets[index];
        let current =
            match read_target(state, TargetSelector::SysId(target.target.sys_id.clone())).await {
                Ok(current) => current,
                Err(error) => {
                    failure = Some((error.public_code().to_string(), false));
                    mark_failed_and_remaining(&mut receipt, index, error.public_code(), "");
                    break;
                }
            };
        if current.sys_updated_on != target.concurrency_token.sys_updated_on {
            failure = Some(("CONCURRENCY_CONFLICT".to_string(), false));
            mark_failed_and_remaining(
                &mut receipt,
                index,
                "CONCURRENCY_CONFLICT",
                &current.sys_updated_on,
            );
            break;
        }

        let patch_json = json!(target.patch);
        let written = match state
            .core
            .update_incident_without_retry(&target.target.sys_id, patch_json)
            .await
        {
            Ok(written) => written,
            Err(error) => {
                failure = Some(("UPSTREAM_ERROR".to_string(), false));
                let sensitive = target.patch.values().cloned().collect::<Vec<_>>();
                failure_diagnostic = Some(crate::change_write::safe_upstream_diagnostic(
                    &error, &sensitive,
                ));
                mark_failed_and_remaining(
                    &mut receipt,
                    index,
                    "UPSTREAM_ERROR",
                    &current.sys_updated_on,
                );
                if let Err(error) = store.save_progress(&request.idempotency_key, &receipt) {
                    return pending_after_store_failure(id, error);
                }
                break;
            }
        };

        let observed = if let Some(observed) = written.get_str("sys_updated_on") {
            observed.to_string()
        } else {
            match read_target(state, TargetSelector::SysId(target.target.sys_id.clone())).await {
                Ok(post_write) => post_write.sys_updated_on,
                Err(_) => {
                    failure = Some(("LOCAL_COHERENCE_FAILED".to_string(), true));
                    receipt.cache_coherent = false;
                    // The post-write token is unavailable, so retain the truthful last
                    // observation from the immediate pre-PATCH recheck and type the
                    // outcome as a coherence failure rather than presenting it as a
                    // successful post-write observation.
                    mark_applied(
                        &mut receipt,
                        index,
                        target,
                        &current.fields,
                        &current.sys_updated_on,
                    );
                    mark_not_attempted_after(&mut receipt, index);
                    if let Err(error) = store.save_progress(&request.idempotency_key, &receipt) {
                        return pending_after_store_failure(id, error);
                    }
                    break;
                }
            }
        };

        let coherence = state
            .core
            .replace_incident_projection_after_write(&target.target.sys_id)
            .await;
        if coherence.is_err() {
            failure = Some(("LOCAL_COHERENCE_FAILED".to_string(), true));
            receipt.cache_coherent = false;
            mark_applied(&mut receipt, index, target, &current.fields, &observed);
            mark_not_attempted_after(&mut receipt, index);
            if let Err(error) = store.save_progress(&request.idempotency_key, &receipt) {
                return pending_after_store_failure(id, error);
            }
            break;
        }

        mark_applied(&mut receipt, index, target, &current.fields, &observed);
        if let Err(error) = store.save_progress(&request.idempotency_key, &receipt) {
            return pending_after_store_failure(id, error);
        }
    }

    receipt.completed_at = Utc::now();
    recount(&mut receipt);
    if let Some((failure_code, _)) = failure.as_ref()
        && receipt.applied_count == 0
    {
        let (code, message) = match failure_code.as_str() {
            "CONCURRENCY_CONFLICT" => (-32053, "CONCURRENCY_CONFLICT"),
            "ACL_DENIED" => (-32003, "ACL_DENIED"),
            "SERVICENOW_UNAVAILABLE" => (-32001, "SERVICENOW_UNAVAILABLE"),
            "INCIDENT_STATE_UNRESOLVED" => (-32602, "INCIDENT_STATE_UNRESOLVED"),
            "SERVICENOW_ERROR" => (-32000, "SERVICENOW_ERROR"),
            "UPSTREAM_ERROR" => (-32059, "UPSTREAM_ERROR"),
            _ => (-32059, "UPSTREAM_ERROR"),
        };
        let public_reason = failure_diagnostic
            .clone()
            .unwrap_or_else(|| "bulk execution stopped before any target was applied".to_string());
        let error = JsonRpcError {
            code,
            message: message.to_string(),
            data: Some(json!({
                "code": message,
                "reason": public_reason,
            })),
        };
        if let Err(audit_error) = crate::change_write::append_audit_event(
            state,
            &audit_id,
            Some(&plan.plan_id),
            APPLY_TOOL,
            ResultStatus::Error,
            Some(redacted_receipt_summary(&receipt)),
            None,
            Some(ErrorRow {
                code: message.to_string(),
                reason: failure_diagnostic.clone().unwrap_or(public_reason),
                retryable: false,
                transient: false,
            }),
            Some((&binding.actor, &binding.requester)),
            None,
        )
        .await
        {
            return pending_after_store_failure(id, audit_error);
        }
        if let Err(store_error) = store.save_terminal_error(&request.idempotency_key, &error) {
            return pending_after_store_failure(id, store_error);
        }
        return JsonRpcResponse {
            jsonrpc: "2.0".to_string(),
            result: None,
            error: Some(error),
            id,
        };
    }
    if failure.is_some() {
        receipt.status = BulkReceiptStatus::Partial;
    }
    let audit_status = if failure.is_some() {
        ResultStatus::AppliedPartial
    } else {
        ResultStatus::AppliedSuccess
    };
    if let Err(error) = crate::change_write::append_audit_event(
        state,
        &audit_id,
        Some(&plan.plan_id),
        APPLY_TOOL,
        audit_status,
        Some(redacted_receipt_summary(&receipt)),
        None,
        failure.as_ref().map(|(code, _)| ErrorRow {
            code: failure
                .as_ref()
                .map(|(code, _)| code.clone())
                .unwrap_or_default(),
            reason: failure_diagnostic.clone().unwrap_or_else(|| code.clone()),
            retryable: false,
            transient: false,
        }),
        Some((&plan.actor, &plan.requester)),
        None,
    )
    .await
    {
        return pending_after_store_failure(id, error);
    }
    if let Err(error) = store.save_final_receipt(&request.idempotency_key, APPLY_TOOL, &receipt) {
        return pending_after_store_failure(id, error);
    }
    let _ = store.mark_plan_consumed(&plan.plan_id);

    if let Some((failure_code, upstream_applied)) = failure {
        return JsonRpcResponse::error(
            id,
            -32046,
            "PARTIAL_FAILURE",
            Some(json!({
                "code": "PARTIAL_FAILURE",
                "failure_code": failure_code,
                "upstream_applied": upstream_applied,
                "upstream_diagnostic": failure_diagnostic,
                "receipt": receipt,
            })),
        );
    }
    JsonRpcResponse::ok(id, json!(receipt))
}

#[derive(Debug)]
enum BuildError {
    Invalid(&'static str, String),
    Read(IncidentReadError),
}

impl BuildError {
    fn public_code(&self) -> &'static str {
        match self {
            Self::Invalid(code, _) => code,
            Self::Read(IncidentReadError::NotFound) => "INCIDENT_NOT_FOUND",
            Self::Read(IncidentReadError::NumberAmbiguous) => "INCIDENT_NUMBER_AMBIGUOUS",
            Self::Read(IncidentReadError::LookupUnavailable) => "INCIDENT_LOOKUP_UNAVAILABLE",
            Self::Read(IncidentReadError::AclDenied) => "ACL_DENIED",
            Self::Read(IncidentReadError::ServiceNowUnavailable) => "SERVICENOW_UNAVAILABLE",
            Self::Read(IncidentReadError::StateUnresolved { .. }) => "INCIDENT_STATE_UNRESOLVED",
            Self::Read(IncidentReadError::InvalidParams(_)) => "INVALID_PARAMS",
            Self::Read(IncidentReadError::ServiceNowError) => "SERVICENOW_ERROR",
        }
    }
}

enum TargetSelector {
    Number(String),
    SysId(String),
}

struct ResolvedTarget {
    number: String,
    sys_id: String,
    sys_updated_on: String,
    fields: BTreeMap<String, FieldValue>,
}

fn target_selector(target: &BulkTargetRequest) -> Result<TargetSelector, BuildError> {
    match (target.number.as_deref(), target.sys_id.as_deref()) {
        (Some(number), None) if valid_incident_number(number) => {
            Ok(TargetSelector::Number(number.to_ascii_uppercase()))
        }
        (None, Some(sys_id)) if valid_sys_id(sys_id) => {
            Ok(TargetSelector::SysId(sys_id.to_ascii_lowercase()))
        }
        _ => Err(BuildError::Invalid(
            "INVALID_SELECTOR",
            "each target requires exactly one valid number or sys_id".to_string(),
        )),
    }
}

async fn read_target(
    state: &DaemonState,
    selector: TargetSelector,
) -> Result<ResolvedTarget, BuildError> {
    let input = match selector {
        TargetSelector::Number(number) => IncidentGetInput {
            number: Some(number),
            sys_id: None,
        },
        TargetSelector::SysId(sys_id) => IncidentGetInput {
            number: None,
            sys_id: Some(sys_id),
        },
    };
    let envelope = state
        .core
        .incident_get(input)
        .await
        .map_err(BuildError::Read)?;
    let fields = envelope.data.record;
    let value = |name: &str| {
        fields
            .get(name)
            .map(|field| field.value.trim().to_string())
            .filter(|value| !value.is_empty())
    };
    let number = value("number").ok_or_else(|| {
        BuildError::Invalid("LOOKUP_INCOMPLETE", "target omitted number".to_string())
    })?;
    let sys_id = value("sys_id").ok_or_else(|| {
        BuildError::Invalid("LOOKUP_INCOMPLETE", "target omitted sys_id".to_string())
    })?;
    let sys_updated_on = value("sys_updated_on").ok_or_else(|| {
        BuildError::Invalid(
            "LOOKUP_INCOMPLETE",
            "target omitted sys_updated_on".to_string(),
        )
    })?;
    Ok(ResolvedTarget {
        number,
        sys_id: sys_id.to_ascii_lowercase(),
        sys_updated_on,
        fields,
    })
}

async fn normalize_patch(
    patch: Option<&IncidentPatch>,
    state: &DaemonState,
) -> Result<BTreeMap<String, String>, BuildError> {
    let Some(patch) = patch else {
        return Ok(BTreeMap::new());
    };
    let mut normalized = BTreeMap::new();
    for (field, value) in [
        ("assigned_to", patch.assigned_to.as_deref()),
        ("assignment_group", patch.assignment_group.as_deref()),
    ] {
        if let Some(value) = value {
            let value = value.trim();
            if !valid_sys_id(value) {
                return Err(BuildError::Invalid(
                    "INVALID_REFERENCE",
                    format!("{field} must be an exact 32-hex sys_id"),
                ));
            }
            normalized.insert(field.to_string(), value.to_ascii_lowercase());
        }
    }
    if let Some(state_selector) = patch.state.as_deref() {
        let state_selector = state_selector.trim();
        if state_selector.is_empty() || state_selector.to_ascii_lowercase().contains("cancel") {
            return Err(BuildError::Invalid(
                "STATE_REJECTED",
                "state is empty or cancellation is forbidden".to_string(),
            ));
        }
        let choices = state
            .core
            .field_choices("incident", "state")
            .await
            .map_err(|_| BuildError::Read(IncidentReadError::ServiceNowError))?;
        let choice = snow_core::resolve_incident_state(state_selector, &choices).map_err(|_| {
            BuildError::Invalid(
                "INCIDENT_STATE_UNRESOLVED",
                "state is not an exact raw value or live choice label".to_string(),
            )
        })?;
        if choice.value.to_ascii_lowercase().contains("cancel")
            || choice.label.to_ascii_lowercase().contains("cancel")
        {
            return Err(BuildError::Invalid(
                "STATE_REJECTED",
                "cancellation states are forbidden".to_string(),
            ));
        }
        normalized.insert("state".to_string(), choice.value);
    }
    for (field, value) in [
        ("work_notes", patch.work_notes.as_deref()),
        ("comments", patch.comments.as_deref()),
    ] {
        if let Some(value) = value {
            if value.trim().is_empty() || value.chars().count() > JOURNAL_MAX_CHARS {
                return Err(BuildError::Invalid(
                    "JOURNAL_REJECTED",
                    format!("{field} must contain 1..={JOURNAL_MAX_CHARS} characters"),
                ));
            }
            normalized.insert(field.to_string(), value.to_string());
        }
    }
    Ok(normalized)
}

fn configured_bulk_target_limit(state: &DaemonState) -> Result<usize, String> {
    let max_targets = [PLAN_TOOL, APPLY_TOOL]
        .into_iter()
        .map(|tool| {
            state
                .mcp_config
                .policy
                .tools
                .get(tool)
                .and_then(|policy| policy.max_targets)
                .ok_or_else(|| format!("{tool} requires explicit max_targets"))
        })
        .collect::<Result<Vec<_>, _>>()?
        .into_iter()
        .min()
        .ok_or_else(|| "bulk policy requires explicit max_targets".to_string())?;
    let max_targets = usize::try_from(max_targets)
        .map_err(|_| "max_targets does not fit this platform".to_string())?;
    if !(MIN_TARGETS..=MAX_TARGETS).contains(&max_targets) {
        return Err(format!(
            "max_targets must be between {MIN_TARGETS} and {MAX_TARGETS}"
        ));
    }
    Ok(max_targets)
}

fn field_allowed(state: &DaemonState, field: &str) -> bool {
    [PLAN_TOOL, APPLY_TOOL].into_iter().all(|tool| {
        state
            .mcp_config
            .policy
            .tools
            .get(tool)
            .is_some_and(|policy| policy.field_allowlist.contains(field))
    })
}

fn policy_denial(
    id: Option<Value>,
    state: &DaemonState,
    tool: &str,
    require_environment: bool,
) -> Option<JsonRpcResponse> {
    let policy = state.mcp_config.policy.tools.get(tool);
    let enabled = policy.is_some_and(|policy| policy.enabled);
    let environment_enabled = policy.is_some_and(|policy| {
        policy
            .environments
            .iter()
            .any(|environment| environment == &state.mcp_config.environment.label)
    });
    if !enabled || (require_environment && !environment_enabled) {
        Some(denied(
            id,
            "POLICY_DENIED",
            format!("{tool} is disabled for the named environment"),
        ))
    } else {
        None
    }
}

fn operation_hash(preview: &BulkPreview) -> String {
    let mut hasher = Sha256::new();
    hasher.update(APPLY_TOOL.as_bytes());
    hasher.update(serde_json::to_vec(preview).unwrap_or_default());
    hex::encode(hasher.finalize())
}

fn validate_supplied_tokens(
    tokens: &[BulkConcurrencyToken],
    plan: &BulkPlan,
) -> Result<(), String> {
    if tokens.len() != plan.preview.targets.len() {
        return Err(
            "concurrency_tokens must contain every planned target exactly once".to_string(),
        );
    }
    let expected = plan
        .preview
        .targets
        .iter()
        .map(|target| BulkConcurrencyToken {
            sys_id: target.target.sys_id.clone(),
            sys_updated_on: target.concurrency_token.sys_updated_on.clone(),
        })
        .collect::<Vec<_>>();
    if tokens != expected {
        return Err("concurrency_tokens do not byte-match canonical plan order".to_string());
    }
    Ok(())
}

fn validate_confirmation_record(
    record: &snow_mcp::planner::ConfirmationRecord,
    binding: &ConfirmationBinding,
) -> Result<(), ConfirmationConsumeError> {
    if record.revoked {
        return Err(ConfirmationConsumeError::Revoked);
    }
    if record.expires_at <= Utc::now() {
        return Err(ConfirmationConsumeError::Expired);
    }
    if record.consumed {
        return Err(ConfirmationConsumeError::AlreadyConsumed);
    }
    for (field, stored, expected) in [
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
        if stored != expected {
            return Err(ConfirmationConsumeError::BindingMismatch { field });
        }
    }
    Ok(())
}

fn validate_confirmation_replay(
    token: &str,
    binding: &ConfirmationBinding,
    path: PathBuf,
) -> Result<(), ConfirmationConsumeError> {
    let store =
        SqliteConfirmationStore::open(path).map_err(|_| ConfirmationConsumeError::NotFound)?;
    let record = store
        .lookup(token)
        .map_err(|_| ConfirmationConsumeError::NotFound)?
        .ok_or(ConfirmationConsumeError::NotFound)?;
    // A consumed token is valid only for replay of the same bound receipt.
    if record.revoked || record.expires_at <= Utc::now() {
        return validate_confirmation_record(&record, binding);
    }
    for (field, stored, expected) in [
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
        if stored != expected {
            return Err(ConfirmationConsumeError::BindingMismatch { field });
        }
    }
    Ok(())
}

fn initial_receipt(plan: &BulkPlan, audit_id: &str, started: DateTime<Utc>) -> BulkReceipt {
    BulkReceipt {
        plan_id: plan.plan_id.clone(),
        audit_id: audit_id.to_string(),
        parent_audit_id: plan.plan_id.clone(),
        tool: APPLY_TOOL.to_string(),
        status: BulkReceiptStatus::Success,
        op_hash: plan.op_hash.clone(),
        idempotency_replay: false,
        target_results: plan
            .preview
            .targets
            .iter()
            .map(|target| BulkTargetResult {
                number: target.target.number.clone(),
                sys_id: target.target.sys_id.clone(),
                status: BulkTargetStatus::NotAttempted,
                changed_fields: Vec::new(),
                observed_sys_updated_on: target.concurrency_token.sys_updated_on.clone(),
                error_code: None,
            })
            .collect(),
        applied_count: 0,
        failed_count: 0,
        not_attempted_count: plan.preview.targets.len(),
        cache_coherent: true,
        apply_started_at: started,
        completed_at: started,
    }
}

fn mark_applied(
    receipt: &mut BulkReceipt,
    index: usize,
    target: &BulkPlannedTarget,
    before: &BTreeMap<String, FieldValue>,
    observed: &str,
) {
    let changed_fields = target
        .patch
        .iter()
        .map(|(field, value)| BulkFieldChange {
            field: field.clone(),
            before_hash: hash_value(before.get(field).map(|value| value.value.as_str())),
            after_hash: hash_value(Some(value)),
        })
        .collect();
    receipt.target_results[index] = BulkTargetResult {
        number: target.target.number.clone(),
        sys_id: target.target.sys_id.clone(),
        status: BulkTargetStatus::Applied,
        changed_fields,
        observed_sys_updated_on: observed.to_string(),
        error_code: None,
    };
    recount(receipt);
}

fn mark_failed_and_remaining(receipt: &mut BulkReceipt, index: usize, code: &str, observed: &str) {
    receipt.target_results[index].status = BulkTargetStatus::Failed;
    receipt.target_results[index].error_code = Some(code.to_string());
    if !observed.is_empty() {
        receipt.target_results[index].observed_sys_updated_on = observed.to_string();
    }
    mark_not_attempted_after(receipt, index);
    recount(receipt);
}

fn mark_not_attempted_after(receipt: &mut BulkReceipt, index: usize) {
    for target in receipt.target_results.iter_mut().skip(index + 1) {
        target.status = BulkTargetStatus::NotAttempted;
        target.changed_fields.clear();
        target.error_code = None;
    }
    recount(receipt);
}

fn recount(receipt: &mut BulkReceipt) {
    receipt.applied_count = receipt
        .target_results
        .iter()
        .filter(|target| matches!(target.status, BulkTargetStatus::Applied))
        .count();
    receipt.failed_count = receipt
        .target_results
        .iter()
        .filter(|target| matches!(target.status, BulkTargetStatus::Failed))
        .count();
    receipt.not_attempted_count = receipt
        .target_results
        .iter()
        .filter(|target| matches!(target.status, BulkTargetStatus::NotAttempted))
        .count();
}

fn hash_value(value: Option<&str>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.unwrap_or_default().as_bytes());
    hex::encode(hasher.finalize())
}

fn redacted_plan_summary(plan: &BulkPlan) -> Value {
    json!({
        "op_hash": plan.op_hash,
        "targets": plan.preview.targets.iter().map(|target| json!({
            "number": target.target.number,
            "sys_id": target.target.sys_id,
            "fields": target.patch.keys().collect::<Vec<_>>(),
        })).collect::<Vec<_>>()
    })
}

fn redacted_receipt_summary(receipt: &BulkReceipt) -> Value {
    json!({
        "op_hash": receipt.op_hash,
        "status": receipt.status,
        "applied_count": receipt.applied_count,
        "failed_count": receipt.failed_count,
        "not_attempted_count": receipt.not_attempted_count,
        "cache_coherent": receipt.cache_coherent,
    })
}

fn valid_incident_number(value: &str) -> bool {
    let value = value.trim();
    value.starts_with("INC")
        && value.len() > 3
        && value.as_bytes()[3..].iter().all(u8::is_ascii_digit)
}

fn valid_sys_id(value: &str) -> bool {
    value.len() == 32 && value.as_bytes().iter().all(u8::is_ascii_hexdigit)
}

fn build_error_response(id: Option<Value>, error: BuildError) -> JsonRpcResponse {
    let code = error.public_code();
    let reason = match error {
        BuildError::Invalid(_, reason) => reason,
        BuildError::Read(error) => error.to_string(),
    };
    denied(id, code, reason)
}

fn denied(id: Option<Value>, code: &str, reason: impl Into<String>) -> JsonRpcResponse {
    JsonRpcResponse::error(
        id,
        match code {
            "INCIDENT_NOT_FOUND" => -32004,
            "INCIDENT_NUMBER_AMBIGUOUS" => -32005,
            "INCIDENT_LOOKUP_UNAVAILABLE" => -32007,
            "ACL_DENIED" => -32003,
            "SERVICENOW_UNAVAILABLE" => -32001,
            "SERVICENOW_ERROR" => -32000,
            "INCIDENT_STATE_UNRESOLVED" | "INVALID_PARAMS" | "INVALID_SELECTOR" => -32602,
            _ => -32051,
        },
        code,
        Some(json!({"code": code, "reason": reason.into()})),
    )
}

fn invalid_params(id: Option<Value>, error: impl ToString) -> JsonRpcResponse {
    JsonRpcResponse::error(
        id,
        -32602,
        "invalid params",
        Some(json!({"code": "INVALID_PARAMS", "reason": error.to_string()})),
    )
}

fn confirmation_error(id: Option<Value>, error: ConfirmationConsumeError) -> JsonRpcResponse {
    JsonRpcResponse::error(
        id,
        -32054,
        "CONFIRMATION_INVALID",
        Some(json!({"code": "CONFIRMATION_INVALID", "reason": error.to_string()})),
    )
}

fn pending_after_store_failure(id: Option<Value>, error: impl ToString) -> JsonRpcResponse {
    JsonRpcResponse::error(
        id,
        -32060,
        "PENDING_RESOLUTION_REQUIRED",
        Some(json!({
            "code": "PENDING_RESOLUTION_REQUIRED",
            "reason": "a target outcome could not be durably recorded",
            "details": error.to_string(),
        })),
    )
}

fn internal_error(id: Option<Value>, error: impl ToString) -> JsonRpcResponse {
    JsonRpcResponse::error(
        id,
        -32000,
        "internal error",
        Some(json!({"details": error.to_string()})),
    )
}

fn kill_switched() -> bool {
    [
        "SNOW_INCIDENT_WRITE_KILL_SWITCH",
        "SNOW_MCP_WRITE_KILL_SWITCH",
    ]
    .into_iter()
    .any(|name| {
        std::env::var(name)
            .map(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
            .unwrap_or(false)
    })
}

fn store_path(state: &DaemonState) -> PathBuf {
    state.data_dir.join("mcp_story_write.sqlite3")
}

struct BulkIdempotencyRecord {
    op_hash: String,
    receipt: Option<BulkReceipt>,
    terminal_error: Option<JsonRpcError>,
    apply_started_at: Option<String>,
    expires_at: DateTime<Utc>,
}

struct BulkStore {
    connection: Connection,
}

impl BulkStore {
    fn open(path: impl AsRef<Path>) -> anyhow::Result<Self> {
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent)?;
        }
        let connection = Connection::open(path)?;
        connection.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS incident_bulk_plans (
              plan_id TEXT PRIMARY KEY,
              plan_json TEXT NOT NULL,
              expires_at TEXT NOT NULL,
              consumed INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS incident_bulk_idempotency (
              key TEXT NOT NULL,
              tool TEXT NOT NULL,
              op_hash TEXT NOT NULL,
              receipt_json TEXT,
              terminal_error_json TEXT,
              progress_json TEXT,
              apply_started_at TEXT,
              created_at TEXT NOT NULL,
              expires_at TEXT NOT NULL,
              PRIMARY KEY (key, tool)
            );
            "#,
        )?;
        let has_terminal_error = connection
            .prepare("PRAGMA table_info(incident_bulk_idempotency)")?
            .query_map([], |row| row.get::<_, String>(1))?
            .collect::<rusqlite::Result<Vec<_>>>()?
            .iter()
            .any(|column| column == "terminal_error_json");
        if !has_terminal_error {
            connection.execute(
                "ALTER TABLE incident_bulk_idempotency ADD COLUMN terminal_error_json TEXT",
                [],
            )?;
        }
        Ok(Self { connection })
    }

    fn put_plan(&self, plan: &BulkPlan) -> anyhow::Result<()> {
        self.connection.execute(
            "INSERT INTO incident_bulk_plans (plan_id, plan_json, expires_at) VALUES (?1, ?2, ?3)",
            params![
                plan.plan_id,
                serde_json::to_string(plan)?,
                plan.expires_at.to_rfc3339()
            ],
        )?;
        Ok(())
    }

    fn get_plan(&self, plan_id: &str) -> anyhow::Result<Option<BulkPlan>> {
        let json: Option<String> = self
            .connection
            .query_row(
                "SELECT plan_json FROM incident_bulk_plans WHERE plan_id = ?1",
                [plan_id],
                |row| row.get(0),
            )
            .optional()?;
        json.map(|json| serde_json::from_str(&json).map_err(Into::into))
            .transpose()
    }

    fn mark_plan_consumed(&self, plan_id: &str) -> anyhow::Result<()> {
        self.connection.execute(
            "UPDATE incident_bulk_plans SET consumed = 1 WHERE plan_id = ?1",
            [plan_id],
        )?;
        Ok(())
    }

    fn reserve_idempotency(
        &self,
        key: &str,
        tool: &str,
        op_hash: &str,
        window_seconds: u64,
    ) -> anyhow::Result<()> {
        let now = Utc::now();
        let window_seconds = i64::try_from(window_seconds)
            .map_err(|_| anyhow::anyhow!("idempotency window is too large"))?;
        let expires_at = now + chrono::Duration::seconds(window_seconds);
        self.connection.execute(
            "INSERT INTO incident_bulk_idempotency (key, tool, op_hash, created_at, expires_at) VALUES (?1, ?2, ?3, ?4, ?5)",
            params![key, tool, op_hash, now.to_rfc3339(), expires_at.to_rfc3339()],
        )?;
        Ok(())
    }

    fn lookup_idempotency(
        &self,
        key: &str,
        tool: &str,
    ) -> anyhow::Result<Option<BulkIdempotencyRecord>> {
        self.connection
            .query_row(
                "SELECT op_hash, receipt_json, terminal_error_json, apply_started_at, expires_at FROM incident_bulk_idempotency WHERE key = ?1 AND tool = ?2",
                params![key, tool],
                |row| {
                    let receipt: Option<String> = row.get(1)?;
                    let terminal_error: Option<String> = row.get(2)?;
                    Ok((
                        row.get::<_, String>(0)?,
                        receipt,
                        terminal_error,
                        row.get::<_, Option<String>>(3)?,
                        row.get::<_, String>(4)?,
                    ))
                },
            )
            .optional()?
            .map(|(op_hash, receipt, terminal_error, apply_started_at, expires_at)| {
                Ok(BulkIdempotencyRecord {
                    op_hash,
                    receipt: receipt
                        .map(|receipt| serde_json::from_str(&receipt))
                        .transpose()?,
                    terminal_error: terminal_error
                        .map(|error| serde_json::from_str(&error))
                        .transpose()?,
                    apply_started_at,
                    expires_at: DateTime::parse_from_rfc3339(&expires_at)?.with_timezone(&Utc),
                })
            })
            .transpose()
            .and_then(|record| {
                if record.as_ref().is_some_and(|record| record.expires_at <= Utc::now()) {
                    self.connection.execute(
                        "DELETE FROM incident_bulk_idempotency WHERE key = ?1 AND tool = ?2",
                        params![key, tool],
                    )?;
                    Ok(None)
                } else {
                    Ok(record)
                }
            })
    }

    fn mark_apply_started(&self, key: &str, tool: &str) -> anyhow::Result<bool> {
        let changed = self.connection.execute(
            "UPDATE incident_bulk_idempotency SET apply_started_at = ?1 WHERE key = ?2 AND tool = ?3 AND apply_started_at IS NULL AND receipt_json IS NULL AND terminal_error_json IS NULL",
            params![Utc::now().to_rfc3339(), key, tool],
        )?;
        Ok(changed == 1)
    }

    fn clear_apply_started(&self, key: &str, tool: &str) -> anyhow::Result<()> {
        self.connection.execute(
            "UPDATE incident_bulk_idempotency SET apply_started_at = NULL WHERE key = ?1 AND tool = ?2 AND receipt_json IS NULL AND terminal_error_json IS NULL",
            params![key, tool],
        )?;
        Ok(())
    }

    fn save_progress(&self, key: &str, receipt: &BulkReceipt) -> anyhow::Result<()> {
        self.connection.execute(
            "UPDATE incident_bulk_idempotency SET progress_json = ?1 WHERE key = ?2 AND tool = ?3",
            params![serde_json::to_string(receipt)?, key, APPLY_TOOL],
        )?;
        Ok(())
    }

    fn save_final_receipt(
        &self,
        key: &str,
        tool: &str,
        receipt: &BulkReceipt,
    ) -> anyhow::Result<()> {
        self.connection.execute(
            "UPDATE incident_bulk_idempotency SET receipt_json = ?1, progress_json = ?1 WHERE key = ?2 AND tool = ?3",
            params![serde_json::to_string(receipt)?, key, tool],
        )?;
        Ok(())
    }

    fn save_terminal_error(&self, key: &str, error: &JsonRpcError) -> anyhow::Result<()> {
        self.connection.execute(
            "UPDATE incident_bulk_idempotency SET terminal_error_json = ?1 WHERE key = ?2 AND tool = ?3",
            params![serde_json::to_string(error)?, key, APPLY_TOOL],
        )?;
        Ok(())
    }
}
