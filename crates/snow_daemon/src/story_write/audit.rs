use super::*;

pub(super) fn receipt_warning(
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

pub(super) fn receipt_warnings_for_caller(warnings: &[ReceiptWarning]) -> Vec<ReceiptWarning> {
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

pub(super) fn audit_email_hashes_from_warnings(warnings: &[ReceiptWarning]) -> Vec<String> {
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

pub(super) fn audit_email_hashes_from_plan(plan_json: &Value) -> Vec<String> {
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

pub(super) async fn append_confirmation_consume_audit(
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

pub(super) async fn pending_resolution_required(
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

pub(super) async fn audited_story_error(
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

pub(super) fn audit_error_reason(message: &str, data: &Value) -> String {
    data.get("code")
        .and_then(Value::as_str)
        .unwrap_or(message)
        .to_string()
}

pub(super) fn audit_summary_for_receipt(
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

pub(super) fn audit_warnings(
    warnings: &[ReceiptWarning],
    audit_email_hashes: &[String],
) -> Vec<Value> {
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

pub(super) fn is_d5_caller_email_warning(code: &str) -> bool {
    matches!(
        code,
        WARNING_ASSIGNEE_DEFAULTED_FROM_CALLER
            | WARNING_ASSIGNEE_UNRESOLVED
            | WARNING_ASSIGNEE_AMBIGUOUS
    )
}

pub(super) fn hash_audit_value(value: Option<&Value>) -> String {
    let mut hasher = Sha256::new();
    hasher.update(
        value
            .map(|value| serde_json::to_vec(value).unwrap_or_default())
            .unwrap_or_default(),
    );
    hex::encode(hasher.finalize())
}

pub(super) fn non_free_text_preview(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        other => other.to_string(),
    }
}

pub(super) fn is_free_text_field(field: &str) -> bool {
    matches!(
        field,
        "short_description" | "description" | "acceptance_criteria"
    )
}

pub(super) fn policy_decisions_for(
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
pub(super) async fn append_audit_event(
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

pub(super) fn audit_identity(subject: &str) -> ActorIdentity {
    ActorIdentity {
        subject: subject.to_string(),
        display_name: subject.to_string(),
        source_claim: "mcp_request".to_string(),
    }
}

pub(super) fn story_actor_from_params(params: &Value, state: &DaemonState) -> StoryActor {
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
