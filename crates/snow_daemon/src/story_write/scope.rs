use super::*;

pub(super) async fn epic_exists(
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

pub(super) fn table_for_story_tool<'a>(tool: &str, binding: &'a BoardBinding) -> &'a str {
    if tool.contains("task") {
        &binding.task_table
    } else {
        &binding.story_table
    }
}

pub(super) fn enforce_story_payload_scope(
    payload: &Value,
    binding: &BoardBinding,
) -> std::result::Result<(), PlanBuildError> {
    // Unrestricted binding — no board scope enforcement.
    if binding.allowed_sprints.is_empty() {
        return Ok(());
    }
    story_create_payload_in_scope(payload, binding).map_err(|failure| {
        story_scope_failure_field_rejections(&failure)
            .map(field_rejected)
            .unwrap_or(PlanBuildError::GuardFailed(failure))
    })
}

pub(super) fn enforce_story_record_scope(
    record: &SnowRecord,
    binding: &BoardBinding,
) -> std::result::Result<(), PlanBuildError> {
    // Unrestricted binding — no board scope enforcement.
    if binding.allowed_sprints.is_empty() {
        return Ok(());
    }
    story_record_in_scope(record, binding).map_err(PlanBuildError::GuardFailed)
}

pub(super) fn story_record_in_scope(
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

pub(super) fn enforce_task_parent_payload_scope(
    payload: &Value,
    parent: &SnowRecord,
    binding: &BoardBinding,
) -> std::result::Result<(), PlanBuildError> {
    // Unrestricted binding — no board scope enforcement.
    if binding.allowed_sprints.is_empty() {
        return Ok(());
    }
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

pub(super) fn enforce_task_record_scope(
    task: &SnowRecord,
    binding: &BoardBinding,
) -> std::result::Result<(), PlanBuildError> {
    // Unrestricted binding — no board scope enforcement.
    if binding.allowed_sprints.is_empty() {
        return Ok(());
    }
    task_record_in_scope(task, binding).map_err(PlanBuildError::GuardFailed)
}

pub(super) fn task_record_in_scope(
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

pub(super) fn resolve_story_task_create_assignment_group(
    args: &Map<String, Value>,
    parent: &SnowRecord,
    binding: &BoardBinding,
) -> std::result::Result<String, PlanBuildError> {
    if let Some(supplied) = args.get("assignment_group") {
        let Some(assignment_group) = reference_sys_id_from_value(supplied) else {
            return Err(PlanBuildError::Invalid("assignment_group".to_string()));
        };
        let assignment_group = assignment_group.trim();
        if assignment_group.is_empty() {
            return Err(PlanBuildError::Invalid("assignment_group".to_string()));
        }
        return ensure_task_assignment_group_allowed(assignment_group, binding)
            .map(|_| assignment_group.to_string())
            .map_err(PlanBuildError::GuardFailed);
    }

    let parent_assignment_group = in_scope_snapshot_from_snow_record(parent)
        .map(|snapshot| snapshot.assignment_group_sys_id)
        .filter(|assignment_group| task_assignment_group_allowed(assignment_group, binding));

    Ok(parent_assignment_group.unwrap_or_else(|| binding.assignment_group.clone()))
}

pub(super) fn enforce_task_assignment_group_payload_scope(
    payload: &Value,
    binding: &BoardBinding,
) -> std::result::Result<(), InScopeFailure> {
    let Some(assignment_group) = reference_sys_id_from_json(payload, "assignment_group") else {
        return Ok(());
    };
    ensure_task_assignment_group_allowed(&assignment_group, binding)
}

pub(super) fn ensure_task_assignment_group_allowed(
    observed: &str,
    binding: &BoardBinding,
) -> std::result::Result<(), InScopeFailure> {
    if task_assignment_group_allowed(observed, binding) {
        Ok(())
    } else {
        Err(InScopeFailure::WrongAssignmentGroup {
            expected: expected_task_assignment_groups(binding),
            observed: observed.to_string(),
        })
    }
}

pub(super) fn task_assignment_group_allowed(observed: &str, binding: &BoardBinding) -> bool {
    equal_sys_id(observed, &binding.assignment_group)
        || binding
            .allowed_task_assignment_groups
            .iter()
            .any(|allowed| equal_sys_id(observed, allowed))
}

pub(super) fn expected_task_assignment_groups(binding: &BoardBinding) -> String {
    let mut groups = vec![binding.assignment_group.clone()];
    for group in &binding.allowed_task_assignment_groups {
        if !group.trim().is_empty() && !groups.iter().any(|seen| equal_sys_id(seen, group)) {
            groups.push(group.clone());
        }
    }
    groups.join(",")
}
