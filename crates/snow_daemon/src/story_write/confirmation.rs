use super::*;

pub(super) async fn enforce_apply_guard(
    tool: &str,
    plan: &OperationPlan,
    binding: &BoardBinding,
    state: &DaemonState,
) -> std::result::Result<(), InScopeFailure> {
    // When allowed_sprints is empty the board binding is unrestricted — any
    // story on any sprint/board is permitted.  Skip the board-membership guard
    // entirely so writes to stories that live on a different squad board are
    // not rejected.  A non-empty allowed_sprints list re-enables the full
    // scope check, constraining writes to the configured board only.
    if binding.allowed_sprints.is_empty() {
        return Ok(());
    }

    match tool {
        "story_apply_create" => story_create_payload_in_scope(&plan.planned_changes, binding),
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
            )?;
            enforce_task_assignment_group_payload_scope(&plan.planned_changes, binding)
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

pub(super) fn is_kill_switched() -> bool {
    std::env::var("SNOW_STORY_WRITE_KILL_SWITCH")
        .map(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false)
}

pub(super) fn validate_confirmation(
    record: &ConfirmationRecord,
    binding: &ConfirmationBinding,
) -> std::result::Result<(), ConfirmationConsumeError> {
    validate_confirmation_binding(record, binding)?;
    if record.consumed {
        return Err(ConfirmationConsumeError::AlreadyConsumed);
    }
    Ok(())
}

pub(super) fn validate_confirmation_binding(
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

pub(super) fn validate_replay_confirmation(
    store: &SqliteConfirmationStore,
    token: &str,
    binding: &ConfirmationBinding,
) -> std::result::Result<(), ConfirmationConsumeError> {
    match store.lookup(token) {
        Ok(Some(record)) => validate_confirmation_replay_binding(&record, binding),
        Ok(None) | Err(_) => Err(ConfirmationConsumeError::NotFound),
    }
}

pub(super) fn validate_confirmation_replay_binding(
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

pub(super) fn check_and_record_rate_limit(
    bucket: &str,
    actor: &str,
    tool: &str,
    limit_per_minute: u32,
) -> Option<i64> {
    rate_limit(bucket, actor, tool, limit_per_minute, true)
}

pub(super) fn check_rate_limit_only(
    bucket: &str,
    actor: &str,
    tool: &str,
    limit_per_minute: u32,
) -> Option<i64> {
    rate_limit(bucket, actor, tool, limit_per_minute, false)
}

pub(super) fn record_rate_limit_event(bucket: &str, actor: &str, tool: &str) {
    let _ = rate_limit(bucket, actor, tool, u32::MAX, true);
}

pub(super) fn rate_limit(
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

pub(super) async fn confirmation_invalid(
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
