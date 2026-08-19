use super::*;

#[allow(clippy::large_enum_variant)]
pub(super) enum PendingResolution {
    Recovered(OperationReceipt),
    Proceed,
    NeedsOperator,
}

pub(super) async fn resolve_pending_response(
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

#[allow(clippy::large_enum_variant)]
pub(super) enum PendingRecovery {
    Recovered(OperationReceipt),
    Proceed,
    NeedsOperator,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum CreatePendingDecision {
    Proceed,
    NeedsOperator,
}

pub(super) fn classify_create_recovery_lookup(
    lookup: &CreateRecoveryLookup,
) -> CreatePendingDecision {
    match lookup {
        CreateRecoveryLookup::Found(_) | CreateRecoveryLookup::Ambiguous => {
            CreatePendingDecision::NeedsOperator
        }
        CreateRecoveryLookup::NoMatch => CreatePendingDecision::Proceed,
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(super) enum UpdatePendingDecision {
    AlreadyApplied,
    ProceedUnchangedToken,
    NeedsOperator,
}

pub(super) fn classify_update_recovery_record(
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

pub(super) async fn try_resolve_pending_receipt(
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

#[allow(clippy::large_enum_variant)]
pub(super) enum CreateRecoveryLookup {
    Found(SnowRecord),
    NoMatch,
    Ambiguous,
}

pub(super) async fn find_recovered_create_record(
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

pub(super) fn create_recovery_created_after(plan_record: &PlanStoreRecord) -> String {
    (plan_record.created_at - chrono::Duration::seconds(CREATE_RECOVERY_CLOCK_SKEW_SECONDS))
        .format("%Y-%m-%d %H:%M:%S")
        .to_string()
}

pub(super) fn record_already_matches(record: &SnowRecord, planned_changes: &Value) -> bool {
    let Some(changes) = planned_changes.as_object() else {
        return false;
    };
    changes.iter().all(|(field, expected)| {
        record_field_value(record, field)
            .as_ref()
            .is_some_and(|observed| value_matches(observed, expected))
    })
}

pub(super) fn value_matches(observed: &Value, expected: &Value) -> bool {
    match (observed, expected) {
        (Value::String(observed), Value::String(expected)) => observed == expected,
        (Value::String(observed), other) => observed == &other.to_string(),
        (other, Value::String(expected)) => &other.to_string() == expected,
        _ => observed == expected,
    }
}
