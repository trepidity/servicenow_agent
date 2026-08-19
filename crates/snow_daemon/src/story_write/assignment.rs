use super::*;

pub(super) async fn default_story_owner_from_actor(
    payload: &mut Value,
    actor: &StoryActor,
    state: &DaemonState,
) -> std::result::Result<Vec<ReceiptWarning>, PlanBuildError> {
    let Some(object) = payload.as_object_mut() else {
        return Ok(Vec::new());
    };
    if object.get("u_story_owner").is_some() {
        return Ok(Vec::new());
    }

    if let Some(sys_id) = resolve_actor_sys_user_sys_id(actor, state).await {
        object.insert("u_story_owner".to_string(), Value::String(sys_id));
        return Ok(vec![receipt_warning(
            WARNING_STORY_OWNER_DEFAULTED_FROM_CALLER,
            Some("u_story_owner"),
            "Substituted caller identity for omitted story owner.",
            Some(json!({ "source": "caller_identity" })),
        )]);
    }

    Ok(Vec::new())
}

pub(super) async fn resolve_actor_sys_user_sys_id(
    actor: &StoryActor,
    state: &DaemonState,
) -> Option<String> {
    if let Some(email) = actor
        .email
        .as_deref()
        .map(str::trim)
        .filter(|email| !email.is_empty())
        && let [sys_id] = find_active_user_sys_ids(state, "email", email)
            .await
            .as_slice()
    {
        return Some(sys_id.clone());
    }

    let subject = actor.subject.trim();
    if !subject.is_empty() {
        if is_sys_id(subject) {
            return Some(subject.to_string());
        }
        if let [sys_id] = find_active_user_sys_ids(state, "user_name", subject)
            .await
            .as_slice()
        {
            return Some(sys_id.clone());
        }
    }

    None
}

pub(super) async fn resolve_assignee_in_payload(
    tool: &str,
    payload: &mut Value,
    actor: &StoryActor,
    state: &DaemonState,
) -> std::result::Result<Vec<ReceiptWarning>, PlanBuildError> {
    let Some(object) = payload.as_object_mut() else {
        return Ok(Vec::new());
    };

    let supplied = object.get("assigned_to").cloned();
    match supplied {
        Some(Value::Null) => Ok(Vec::new()),
        Some(Value::String(value)) if is_sys_id(&value) => Ok(Vec::new()),
        Some(Value::String(value)) if value.trim().eq_ignore_ascii_case("caller") => {
            default_assignee_from_actor(object, actor, state).await
        }
        Some(Value::String(_)) => Err(field_rejected(vec![json!({
            "field": "assigned_to",
            "reason": "value_not_in_enum",
        })])),
        Some(_) => Err(field_rejected(vec![json!({
            "field": "assigned_to",
            "reason": "type_mismatch",
        })])),
        None if tool.ends_with("_plan_update") => Ok(Vec::new()),
        None => default_assignee_from_actor(object, actor, state).await,
    }
}

pub(super) async fn default_assignee_from_actor(
    payload: &mut Map<String, Value>,
    actor: &StoryActor,
    state: &DaemonState,
) -> std::result::Result<Vec<ReceiptWarning>, PlanBuildError> {
    let mut ambiguous = Vec::new();

    if let Some(email) = actor
        .email
        .as_deref()
        .map(str::trim)
        .filter(|email| !email.is_empty())
    {
        let hits = find_active_user_sys_ids(state, "email", email).await;
        match hits.as_slice() {
            [sys_id] => {
                payload.insert("assigned_to".to_string(), Value::String(sys_id.clone()));
                let mask = mask_email_for_receipt(email);
                return Ok(vec![receipt_warning(
                    WARNING_ASSIGNEE_DEFAULTED_FROM_CALLER,
                    Some("assigned_to"),
                    assignee_defaulted_warning_message(&mask),
                    Some(json!({
                        "email_local_part": mask.email_local_part,
                        "domain_hash": mask.domain_hash,
                        "email_sha256": hash_email_for_audit(email),
                        "source": "email_match",
                    })),
                )]);
            }
            [] => {}
            hits => ambiguous.extend(hits.iter().cloned()),
        }
    }

    let subject = actor.subject.trim();
    if !subject.is_empty() {
        let hits = find_active_user_sys_ids(state, "user_name", subject).await;
        match hits.as_slice() {
            [sys_id] => {
                payload.insert("assigned_to".to_string(), Value::String(sys_id.clone()));
                return Ok(vec![receipt_warning(
                    WARNING_ASSIGNEE_DEFAULTED_FROM_CALLER,
                    Some("assigned_to"),
                    "Substituted caller identity for omitted assignee.",
                    Some(json!({ "source": "user_name_match" })),
                )]);
            }
            [] => {}
            hits => ambiguous.extend(hits.iter().cloned()),
        }
    }

    payload.remove("assigned_to");
    if ambiguous.is_empty() {
        let mask = actor.email.as_deref().map(mask_email_for_receipt);
        let data = mask.as_ref().map(|mask| {
            json!({
                "email_local_part": mask.email_local_part,
                "domain_hash": mask.domain_hash,
                "email_sha256": actor.email.as_deref().map(hash_email_for_audit),
            })
        });
        Ok(vec![receipt_warning(
            WARNING_ASSIGNEE_UNRESOLVED,
            Some("assigned_to"),
            assignee_unresolved_warning_message(mask.as_ref()),
            data,
        )])
    } else {
        let warning_data =
            assignee_ambiguous_warning_data(actor.email.as_deref(), ambiguous.clone())
                .and_then(|data| {
                    let mut value = serde_json::to_value(data).ok()?;
                    if let Some(email) = actor.email.as_deref()
                        && let Some(object) = value.as_object_mut()
                    {
                        object.insert(
                            "email_sha256".to_string(),
                            Value::String(hash_email_for_audit(email)),
                        );
                    }
                    Some(value)
                })
                .or_else(|| Some(json!({ "candidate_sys_ids": ambiguous })));
        Ok(vec![receipt_warning(
            WARNING_ASSIGNEE_AMBIGUOUS,
            Some("assigned_to"),
            "Multiple sys_user records matched caller identity; field left blank. Specify assigned_to explicitly.",
            warning_data,
        )])
    }
}

pub(super) async fn find_active_user_sys_ids(
    state: &DaemonState,
    field: &str,
    value: &str,
) -> Vec<String> {
    match state
        .core
        .client()
        .table("sys_user")
        .equals(field, value)
        .equals("active", "true")
        .fields(&["sys_id"])
        .limit(3)
        .execute()
        .await
    {
        Ok(response) => response
            .records
            .into_iter()
            .map(|record| record.sys_id)
            .collect(),
        Err(_) => Vec::new(),
    }
}
