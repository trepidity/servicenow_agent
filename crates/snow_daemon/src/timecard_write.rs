use std::collections::BTreeSet;
use std::path::PathBuf;

use anyhow::{Result, anyhow};
use chrono::Utc;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use snow_core::resource::timecard::{SetMode, TimeCard, TimeValue, WeekSelector, Weekday};
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
use uuid::Uuid;

use crate::DaemonState;
use crate::rpc::JsonRpcResponse;

const PLAN_TTL_SECONDS: i64 = 600;
const TIME_CARD_TABLE: &str = "time_card";
const TIMECARD_PLAN_TOOL: &str = "timecard_plan_set_hours";
const TIMECARD_APPLY_TOOL: &str = "timecard_apply_set_hours";
const DAY_FIELDS: &[&str] = &[
    "sunday",
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
];

pub async fn handle_timecard_list(
    id: Option<Value>,
    params: &Value,
    state: &DaemonState,
) -> JsonRpcResponse {
    let week = match parse_week_selector(params) {
        Ok(week) => week,
        Err(err) => return invalid_params(id, err),
    };

    match state.core.list_my_timecards(week).await {
        Ok(sheet) => JsonRpcResponse::ok(id, json!({ "sheet": sheet })),
        Err(err) => internal_error(id, err),
    }
}

pub async fn handle_timecard_set_hours(
    id: Option<Value>,
    params: &Value,
    state: &DaemonState,
) -> JsonRpcResponse {
    let input = match parse_set_hours_input(params) {
        Ok(input) => input,
        Err(err) => return invalid_params(id, err),
    };
    let hours = match parse_time_value(&input.hours) {
        Ok(hours) => hours,
        Err(err) => return invalid_params(id, err),
    };

    match state
        .core
        .set_timecard_hours(&input.sys_id, input.day, hours, input.mode)
        .await
    {
        Ok(card) => JsonRpcResponse::ok(id, json!({ "card": card })),
        Err(err) => internal_error(id, err),
    }
}

pub async fn handle_timecard_plan_set_hours(
    id: Option<Value>,
    params: &Value,
    state: &DaemonState,
) -> JsonRpcResponse {
    if !state.mcp_config.policy.is_tool_enabled(TIMECARD_PLAN_TOOL) {
        return timecard_error(
            id,
            -32051,
            "FIELD_REJECTED",
            json!({ "code": "FIELD_REJECTED", "fields": [{"field": "tool", "reason": "blocked_deny_list"}] }),
        );
    }

    let input = match parse_set_hours_input(params) {
        Ok(input) => input,
        Err(err) => {
            return timecard_error(
                id,
                -32051,
                "FIELD_REJECTED",
                json!({ "code": "FIELD_REJECTED", "fields": [{"field": err.to_string(), "reason": "type_mismatch"}] }),
            );
        }
    };
    if let Err(response) = reject_non_day_field(id.clone(), state, &input.day_field) {
        return response;
    }

    let actor = actor_from_params(params, state);
    let requester = requester_from_params(params, &actor);
    let actor_user_sys_id = match resolve_actor_user_sys_id(state, &actor).await {
        Ok(sys_id) => sys_id,
        Err(err) => return internal_error(id, err),
    };

    let card = match state.core.get_timecard_fresh(&input.sys_id).await {
        Ok(Some(card)) => card,
        Ok(None) => {
            return timecard_error(
                id,
                -32004,
                "record not found",
                json!({ "details": format!("time_card {} not found", input.sys_id) }),
            );
        }
        Err(err) => return internal_error(id, err),
    };
    let fingerprint = card_fingerprint(&card);
    if let Err(failure) =
        timecard_in_scope(&card, &actor_user_sys_id, &input.day_field, &fingerprint)
    {
        return timecard_error(id, -32050, "GUARD_FAILED", failure.to_json());
    }

    let from = day_hours(&card, &input.day);
    let to = match preview_to_hours(&from, &input.hours, &input.mode) {
        Ok(to) => to,
        Err(err) => {
            return timecard_error(
                id,
                -32051,
                "FIELD_REJECTED",
                json!({ "code": "FIELD_REJECTED", "fields": [{"field": "hours", "reason": err.to_string()}] }),
            );
        }
    };
    let total_after = match preview_total_after(&card, input.day, &to) {
        Ok(total) => total,
        Err(err) => return internal_error(id, err),
    };
    let target = RecordRef {
        sys_id: card.sys_id.clone(),
        number: card
            .task
            .as_ref()
            .map(|task| task.number.clone())
            .filter(|number| !number.trim().is_empty())
            .unwrap_or_else(|| card.sys_id.clone()),
        table: TIME_CARD_TABLE.to_string(),
    };
    let planned_changes = json!({
        "kind": "TimecardSetHours",
        "card_sys_id": card.sys_id,
        "day": input.day_name,
        "day_field": input.day_field,
        "from": from,
        "to": to,
        "hours": input.hours,
        "mode": mode_name(&input.mode),
        "actor_user_sys_id": actor_user_sys_id,
        "card_fingerprint": fingerprint,
        "preview": {
            "day": input.day_field,
            "from": from,
            "to": to,
            "total_before": card.total,
            "total_after": total_after,
        }
    });
    let plan = OperationPlanBuilder::new(TIMECARD_PLAN_TOOL)
        .target(target)
        .planned_changes(planned_changes)
        .build();
    let concurrency_token = concurrency_from_card(&card);
    let now = Utc::now();
    let expires_at = now + chrono::Duration::seconds(PLAN_TTL_SECONDS);
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
            TIMECARD_APPLY_TOOL,
            &plan.op_hash,
            state.mcp_config.policy.idempotency_window_seconds,
        )
        .await
    {
        Ok(IdempotencyOutcome::NewKey) => {}
        Ok(_) => return internal_error(id, "server-issued idempotency key was not unique"),
        Err(err) => return internal_error(id, err),
    }

    let mut plan_json = match serde_json::to_value(&plan) {
        Ok(value) => value,
        Err(err) => return internal_error(id, err),
    };
    if let Value::Object(object) = &mut plan_json {
        object.insert(
            "apply_tool".to_string(),
            Value::String(TIMECARD_APPLY_TOOL.to_string()),
        );
    }

    let record = PlanStoreRecord {
        plan_id: plan.plan_id.clone(),
        tool: plan.tool.clone(),
        actor: actor.clone(),
        op_hash: plan.op_hash.clone(),
        plan_json,
        concurrency_token: concurrency_token.clone(),
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
        TIMECARD_PLAN_TOOL,
        ResultStatus::Plan,
        Some(json!({ "op_hash": plan.op_hash })),
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
                tool: TIMECARD_APPLY_TOOL.to_string(),
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
            "preview": plan.planned_changes,
            "expires_at": expires_at.to_rfc3339(),
            "confirmation_token": confirmation.token_id,
            "idempotency_key": idempotency_key,
            "concurrency_token": concurrency_token,
        }),
    )
}

pub async fn handle_timecard_apply_set_hours(
    id: Option<Value>,
    params: &Value,
    state: &DaemonState,
) -> JsonRpcResponse {
    if is_kill_switched() {
        return audited_timecard_error(
            state,
            id,
            -32057,
            "KILL_SWITCH",
            json!({ "code": "KILL_SWITCH" }),
            ResultStatus::Denied,
        )
        .await;
    }
    if let Err(err) = state
        .mcp_config
        .policy
        .validate_timecard_policy(TIMECARD_APPLY_TOOL, &state.mcp_config.environment)
    {
        return audited_timecard_error(
            state,
            id,
            -32051,
            "FIELD_REJECTED",
            json!({ "code": "FIELD_REJECTED", "fields": [{"field": "environment", "reason": err.to_string()}] }),
            ResultStatus::Denied,
        )
        .await;
    }
    if !state.mcp_config.policy.is_tool_enabled(TIMECARD_APPLY_TOOL) {
        return audited_timecard_error(
            state,
            id,
            -32051,
            "FIELD_REJECTED",
            json!({ "code": "FIELD_REJECTED", "fields": [{"field": "tool", "reason": "blocked_deny_list"}] }),
            ResultStatus::Denied,
        )
        .await;
    }

    let Some(plan_id) = string_param(params, "plan_id") else {
        return field_rejected_apply(state, id, "plan_id").await;
    };
    let Some(confirmation_token) = string_param(params, "confirmation_token") else {
        return field_rejected_apply(state, id, "confirmation_token").await;
    };
    let Some(idempotency_key) = string_param(params, "idempotency_key") else {
        return field_rejected_apply(state, id, "idempotency_key").await;
    };
    let key = IdempotencyKey {
        value: idempotency_key.clone(),
        source: IdempotencyKeySource::ServerDerived,
    };
    let stores = match stores(state) {
        Ok(stores) => stores,
        Err(err) => return internal_error(id, err),
    };
    let plan_record = match stores.plan_store.get(&plan_id).await {
        Ok(Some(record)) => record,
        Ok(None) => {
            return audited_timecard_error(
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
    if plan.tool != TIMECARD_PLAN_TOOL {
        return invalid_params(
            id,
            format!(
                "plan {} is bound to {}, not timecard",
                plan.plan_id, plan.tool
            ),
        );
    }

    let actor = actor_from_params(params, state);
    let requester = requester_from_params(params, &actor);
    let confirmation_binding = ConfirmationBinding {
        actor: actor.clone(),
        requester: requester.clone(),
        tool: TIMECARD_APPLY_TOOL.to_string(),
        op_hash: plan.op_hash.clone(),
        environment: state.mcp_config.environment.label.clone(),
    };

    if let Ok(Some(record)) = stores
        .idempotency_store
        .lookup_record(&key, TIMECARD_APPLY_TOOL)
        && record.expires_at > Utc::now()
    {
        if record.op_hash != plan.op_hash {
            return idempotency_conflict(
                state,
                id,
                &idempotency_key,
                &record.op_hash,
                &plan.op_hash,
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
    }

    if plan_record.state == PlanLifecycleState::Expired || plan_record.expires_at <= Utc::now() {
        let _ = stores.plan_store.mark_expired(&plan_id).await;
        return audited_timecard_error(
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
            TIMECARD_APPLY_TOOL,
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
            let bound = stores
                .idempotency_store
                .lookup_record(&key, TIMECARD_APPLY_TOOL)
                .ok()
                .flatten()
                .map(|record| record.op_hash)
                .unwrap_or_default();
            return idempotency_conflict(state, id, &idempotency_key, &bound, &plan.op_hash).await;
        }
        Err(err) => return internal_error(id, err),
    }

    let planned = match PlannedTimecardSetHours::from_plan(&plan) {
        Ok(planned) => planned,
        Err(err) => return internal_error(id, err),
    };
    if let Err(response) = reject_non_day_field(id.clone(), state, &planned.day_field) {
        return response;
    }
    let card = match state.core.get_timecard_fresh(&planned.card_sys_id).await {
        Ok(Some(card)) => card,
        Ok(None) => {
            return audited_timecard_error(
                state,
                id,
                -32056,
                "PLAN_NOT_FOUND",
                json!({ "code": "PLAN_NOT_FOUND", "plan_id": plan.plan_id }),
                ResultStatus::Error,
            )
            .await;
        }
        Err(err) => return internal_error(id, err),
    };
    if let Err(failure) = timecard_in_scope(
        &card,
        &planned.actor_user_sys_id,
        &planned.day_field,
        &planned.card_fingerprint,
    ) {
        return audited_timecard_error(
            state,
            id,
            -32050,
            "GUARD_FAILED",
            failure.to_json(),
            ResultStatus::Denied,
        )
        .await;
    }

    let Some(expected_token) = plan_record.concurrency_token.as_ref() else {
        return audited_timecard_error(
            state,
            id,
            -32061,
            "CONCURRENCY_TOKEN_INVALID",
            json!({ "code": "CONCURRENCY_TOKEN_INVALID", "reason": "plan missing concurrency_token" }),
            ResultStatus::Denied,
        )
        .await;
    };
    let Some(supplied_token) = params
        .get("concurrency_token")
        .cloned()
        .and_then(|value| serde_json::from_value::<ConcurrencyToken>(value).ok())
    else {
        return audited_timecard_error(
            state,
            id,
            -32061,
            "CONCURRENCY_TOKEN_INVALID",
            json!({ "code": "CONCURRENCY_TOKEN_INVALID", "reason": "missing concurrency_token" }),
            ResultStatus::Denied,
        )
        .await;
    };
    if &supplied_token != expected_token {
        return audited_timecard_error(
            state,
            id,
            -32061,
            "CONCURRENCY_TOKEN_INVALID",
            json!({ "code": "CONCURRENCY_TOKEN_INVALID", "reason": "token does not match plan" }),
            ResultStatus::Denied,
        )
        .await;
    }
    let observed_token = concurrency_from_card(&card);
    if observed_token.as_ref() != Some(expected_token) {
        return audited_timecard_error(
            state,
            id,
            -32053,
            "CONCURRENCY_CONFLICT",
            json!({
                "code": "CONCURRENCY_CONFLICT",
                "record_sys_id": planned.card_sys_id,
                "expected": expected_token,
                "observed": observed_token,
            }),
            ResultStatus::Denied,
        )
        .await;
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

    let apply_started_at = Utc::now();
    if let Err(err) = stores
        .idempotency_store
        .mark_apply_started(&key, TIMECARD_APPLY_TOOL)
        .await
    {
        return internal_error(id, err);
    }
    let hours = match parse_time_value(&planned.hours) {
        Ok(hours) => hours,
        Err(err) => return invalid_params(id, err),
    };
    let written = match state
        .core
        .set_timecard_hours(&planned.card_sys_id, planned.day, hours, planned.mode)
        .await
    {
        Ok(card) => card,
        Err(err) => {
            return audited_timecard_error(
                state,
                id,
                -32059,
                "UPSTREAM_ERROR",
                json!({ "code": "UPSTREAM_ERROR", "reason": err.to_string(), "retry_after_seconds": 2 }),
                ResultStatus::Error,
            )
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

    let audit_id = Uuid::new_v4().to_string();
    let receipt = receipt_for_card(
        &plan,
        apply_started_at,
        &written,
        concurrency_from_card(&written).unwrap_or_else(empty_concurrency_token),
        Some(planned.from),
    );
    if let Err(err) = append_audit_event(
        state,
        &audit_id,
        Some(plan.plan_id.as_str()),
        TIMECARD_APPLY_TOOL,
        ResultStatus::AppliedSuccess,
        receipt.applied_changes_summary.clone(),
        receipt.service_now_metadata.clone(),
        None,
        Some((&actor, &requester)),
        Some(applied_changes_for_receipt(&receipt)),
    )
    .await
    {
        return internal_error(id, err);
    }
    let mut receipt = receipt;
    receipt.audit_id = audit_id;

    if let Err(err) = stores.idempotency_store.save_receipt(&key, &receipt).await {
        return internal_error(id, err);
    }
    let _ = stores.plan_store.mark_consumed(&plan.plan_id).await;

    JsonRpcResponse::ok(id, json!(receipt))
}

struct SetHoursInput {
    sys_id: String,
    day: Weekday,
    day_name: String,
    day_field: String,
    hours: String,
    mode: SetMode,
}

fn parse_set_hours_input(params: &Value) -> Result<SetHoursInput> {
    let sys_id = string_param(params, "sys_id").ok_or_else(|| anyhow!("sys_id"))?;
    let day_name = string_param(params, "day").ok_or_else(|| anyhow!("day"))?;
    let day = parse_weekday(&day_name)?;
    let day_field = weekday_field(&day).to_string();
    let hours = hours_param(params).ok_or_else(|| anyhow!("hours"))?;
    let mode = match string_param(params, "mode")
        .unwrap_or_else(|| "set".to_string())
        .to_ascii_lowercase()
        .as_str()
    {
        "set" => SetMode::Set,
        "add" => SetMode::Add,
        _ => return Err(anyhow!("mode")),
    };
    Ok(SetHoursInput {
        sys_id,
        day,
        day_name,
        day_field,
        hours,
        mode,
    })
}

struct PlannedTimecardSetHours {
    card_sys_id: String,
    day: Weekday,
    day_field: String,
    hours: String,
    mode: SetMode,
    from: String,
    to: String,
    actor_user_sys_id: String,
    card_fingerprint: String,
}

impl PlannedTimecardSetHours {
    fn from_plan(plan: &OperationPlan) -> Result<Self> {
        let object = plan
            .planned_changes
            .as_object()
            .ok_or_else(|| anyhow!("planned_changes"))?;
        let day_name = object
            .get("day")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("day"))?;
        Ok(Self {
            card_sys_id: string_value(object, "card_sys_id")?,
            day: parse_weekday(day_name)?,
            day_field: string_value(object, "day_field")?,
            hours: string_value(object, "hours")?,
            mode: match string_value(object, "mode")?.as_str() {
                "set" => SetMode::Set,
                "add" => SetMode::Add,
                _ => return Err(anyhow!("mode")),
            },
            from: string_value(object, "from")?,
            to: string_value(object, "to")?,
            actor_user_sys_id: string_value(object, "actor_user_sys_id")?,
            card_fingerprint: string_value(object, "card_fingerprint")?,
        })
    }
}

fn string_value(object: &Map<String, Value>, field: &str) -> Result<String> {
    object
        .get(field)
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!(field.to_string()))
}

fn parse_week_selector(params: &Value) -> Result<WeekSelector> {
    let Some(week) = string_param(params, "week") else {
        return Ok(WeekSelector::Current);
    };
    let date = chrono::NaiveDate::parse_from_str(&week, "%Y-%m-%d")?;
    Ok(WeekSelector::Date(date))
}

fn parse_weekday(day: &str) -> Result<Weekday> {
    Ok(match day.trim().to_ascii_lowercase().as_str() {
        "sun" | "sunday" => Weekday::Sun,
        "mon" | "monday" => Weekday::Mon,
        "tue" | "tuesday" => Weekday::Tue,
        "wed" | "wednesday" => Weekday::Wed,
        "thu" | "thursday" => Weekday::Thu,
        "fri" | "friday" => Weekday::Fri,
        "sat" | "saturday" => Weekday::Sat,
        _ => return Err(anyhow!("day")),
    })
}

fn weekday_field(day: &Weekday) -> &'static str {
    match day {
        Weekday::Sun => "sunday",
        Weekday::Mon => "monday",
        Weekday::Tue => "tuesday",
        Weekday::Wed => "wednesday",
        Weekday::Thu => "thursday",
        Weekday::Fri => "friday",
        Weekday::Sat => "saturday",
    }
}

fn weekday_index(day: &Weekday) -> usize {
    match day {
        Weekday::Sun => 0,
        Weekday::Mon => 1,
        Weekday::Tue => 2,
        Weekday::Wed => 3,
        Weekday::Thu => 4,
        Weekday::Fri => 5,
        Weekday::Sat => 6,
    }
}

fn hours_param(params: &Value) -> Option<String> {
    params.get("hours").and_then(|value| {
        value
            .as_str()
            .map(ToOwned::to_owned)
            .or_else(|| value.as_f64().map(|number| trim_decimal(number)))
    })
}

fn parse_time_value(hours: &str) -> Result<TimeValue> {
    hours.parse::<TimeValue>().map_err(|_| anyhow!("hours"))
}

fn preview_to_hours(from: &str, hours: &str, mode: &SetMode) -> Result<String> {
    let incoming = parse_hours_f64(hours)?;
    let value = match mode {
        SetMode::Set => incoming,
        SetMode::Add => parse_hours_f64(from)? + incoming,
    };
    if !(0.0..=24.0).contains(&value) {
        return Err(anyhow!("value_constrained"));
    }
    Ok(trim_decimal(value))
}

fn preview_total_after(card: &TimeCard, day: Weekday, to: &str) -> Result<String> {
    let mut total = 0.0;
    for (index, value) in card.hours.iter().enumerate() {
        total += if index == day.index() {
            parse_hours_f64(to)?
        } else {
            parse_hours_f64(value)?
        };
    }
    Ok(trim_decimal(total))
}

fn parse_hours_f64(value: &str) -> Result<f64> {
    let parsed = value.trim().parse::<f64>()?;
    if parsed < 0.0 || parsed > 24.0 {
        return Err(anyhow!("value_constrained"));
    }
    Ok(parsed)
}

fn trim_decimal(value: f64) -> String {
    let mut text = format!("{value:.2}");
    while text.contains('.') && text.ends_with('0') {
        text.pop();
    }
    if text.ends_with('.') {
        text.pop();
    }
    text
}

fn mode_name(mode: &SetMode) -> &'static str {
    match mode {
        SetMode::Set => "set",
        SetMode::Add => "add",
    }
}

fn day_hours(card: &TimeCard, day: &Weekday) -> String {
    card.hours
        .get(weekday_index(day))
        .cloned()
        .unwrap_or_else(|| "0".to_string())
}

fn concurrency_from_card(card: &TimeCard) -> Option<ConcurrencyToken> {
    let sys_updated_on = card.sys_updated_on.trim().to_string();
    if sys_updated_on.is_empty() {
        None
    } else {
        Some(ConcurrencyToken {
            sys_updated_on,
            sys_mod_count: card.sys_mod_count,
        })
    }
}

fn empty_concurrency_token() -> ConcurrencyToken {
    ConcurrencyToken {
        sys_updated_on: String::new(),
        sys_mod_count: None,
    }
}

fn card_fingerprint(card: &TimeCard) -> String {
    let task_sys_id = card
        .task
        .as_ref()
        .map(|task| task.sys_id.as_str())
        .unwrap_or("");
    let task_number = card
        .task
        .as_ref()
        .map(|task| task.number.as_str())
        .unwrap_or("");
    let payload = json!({
        "task_sys_id": task_sys_id,
        "task_display": task_number,
        "category": card.category,
        "project_time_category": card.project_time_category,
        "week_starts_on": card.week_starts_on,
    });
    let mut hasher = Sha256::new();
    hasher.update(serde_json::to_vec(&payload).unwrap_or_default());
    hex::encode(hasher.finalize())
}

#[derive(Debug, PartialEq, Eq)]
enum TimecardScopeFailure {
    WrongUser { expected: String, observed: String },
    NonEditableState { observed: String },
    FieldNotAllowed { field: String },
    FingerprintMismatch { expected: String, observed: String },
}

impl TimecardScopeFailure {
    fn to_json(&self) -> Value {
        match self {
            Self::WrongUser { expected, observed } => json!({
                "code": "GUARD_FAILED",
                "reason": "timecard_user_mismatch",
                "expected_user_sys_id": expected,
                "observed_user_sys_id": observed,
            }),
            Self::NonEditableState { observed } => json!({
                "code": "GUARD_FAILED",
                "reason": "timecard_state_not_editable",
                "observed_state": observed,
                "remediation": "Recall the time sheet in the portal before editing.",
            }),
            Self::FieldNotAllowed { field } => json!({
                "code": "GUARD_FAILED",
                "reason": "timecard_day_field_not_allowed",
                "field": field,
            }),
            Self::FingerprintMismatch { expected, observed } => json!({
                "code": "GUARD_FAILED",
                "reason": "timecard_fingerprint_mismatch",
                "expected": expected,
                "observed": observed,
            }),
        }
    }
}

fn timecard_in_scope(
    card: &TimeCard,
    actor_user_sys_id: &str,
    day_field: &str,
    expected_fingerprint: &str,
) -> std::result::Result<(), TimecardScopeFailure> {
    if !DAY_FIELDS.contains(&day_field) {
        return Err(TimecardScopeFailure::FieldNotAllowed {
            field: day_field.to_string(),
        });
    }
    if card.user.sys_id != actor_user_sys_id {
        return Err(TimecardScopeFailure::WrongUser {
            expected: actor_user_sys_id.to_string(),
            observed: card.user.sys_id.clone(),
        });
    }
    if !is_editable_state(&card.state) {
        return Err(TimecardScopeFailure::NonEditableState {
            observed: card.state.clone(),
        });
    }
    let observed = card_fingerprint(card);
    if observed != expected_fingerprint {
        return Err(TimecardScopeFailure::FingerprintMismatch {
            expected: expected_fingerprint.to_string(),
            observed,
        });
    }
    Ok(())
}

fn is_editable_state(state: &str) -> bool {
    matches!(
        state.trim().to_ascii_lowercase().as_str(),
        "pending" | "rejected" | "recalled"
    )
}

async fn resolve_actor_user_sys_id(state: &DaemonState, actor: &str) -> Result<String> {
    let actor = actor.trim();
    let mut lookups = Vec::new();
    if actor.contains('@') {
        lookups.push(("email", actor));
        lookups.push(("user_name", actor));
    } else {
        lookups.push(("user_name", actor));
        lookups.push(("email", actor));
    }
    for (field, value) in lookups {
        if let Some(record) = state
            .core
            .client()
            .table("sys_user")
            .equals(field, value)
            .equals("active", "true")
            .fields(&["sys_id"])
            .limit(1)
            .first()
            .await?
        {
            return Ok(record.sys_id);
        }
    }
    Err(anyhow!(
        "could not resolve actor sys_user sys_id for {actor}"
    ))
}

fn reject_non_day_field(
    id: Option<Value>,
    state: &DaemonState,
    day_field: &str,
) -> std::result::Result<(), JsonRpcResponse> {
    let allowlist = state
        .mcp_config
        .policy
        .tools
        .get(TIMECARD_APPLY_TOOL)
        .map(|policy| policy.field_allowlist.clone())
        .unwrap_or_else(default_day_allowlist);
    if allowlist.contains(day_field) {
        Ok(())
    } else {
        Err(timecard_error(
            id,
            -32051,
            "FIELD_REJECTED",
            json!({ "code": "FIELD_REJECTED", "fields": [{"field": day_field, "reason": "not_in_allowlist"}] }),
        ))
    }
}

fn default_day_allowlist() -> BTreeSet<String> {
    DAY_FIELDS
        .iter()
        .map(|field| (*field).to_string())
        .collect()
}

fn receipt_for_card(
    plan: &OperationPlan,
    apply_started_at: chrono::DateTime<Utc>,
    card: &TimeCard,
    concurrency_token: ConcurrencyToken,
    before: Option<String>,
) -> OperationReceipt {
    let planned = PlannedTimecardSetHours::from_plan(plan).ok();
    let after = planned
        .as_ref()
        .map(|planned| planned.to.clone())
        .or_else(|| {
            planned
                .as_ref()
                .map(|planned| day_hours(card, &planned.day))
        });
    let changed_fields = planned
        .as_ref()
        .map(|planned| {
            vec![FieldChange {
                field: planned.day_field.clone(),
                before: before.map(Value::String),
                after: after.clone().map(Value::String),
            }]
        })
        .unwrap_or_default();

    OperationReceipt {
        plan_id: plan.plan_id.clone(),
        audit_id: Uuid::new_v4().to_string(),
        parent_audit_id: plan.plan_id.clone(),
        tool: TIMECARD_APPLY_TOOL.to_string(),
        status: ReceiptStatus::Success,
        applied_changes_summary: Some(plan.planned_changes.clone()),
        service_now_metadata: Some(ServiceNowMetadata {
            sys_id: Some(card.sys_id.clone()),
            number: None,
            transaction_id: None,
        }),
        idempotency_replay: false,
        completed_at: Utc::now(),
        op_hash: plan.op_hash.clone(),
        record_url: Some(format!("time_card.do?sys_id={}", card.sys_id)),
        record_snapshot: serde_json::to_value(card).ok(),
        changed_fields,
        concurrency_token_observed: Some(concurrency_token),
        apply_started_at: Some(apply_started_at),
        error_code: None,
        warnings: Vec::new(),
    }
}

fn applied_changes_for_receipt(receipt: &OperationReceipt) -> Vec<AppliedChange> {
    receipt
        .changed_fields
        .iter()
        .map(|change| AppliedChange {
            field: change.field.clone(),
            old_hash: hash_audit_value(change.before.as_ref()),
            new_hash: hash_audit_value(change.after.as_ref()),
            redacted_preview: change.after.as_ref().map(non_free_text_preview),
        })
        .collect()
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

struct DaemonStores {
    plan_store: SqlitePlanStore,
    confirmation_store: SqliteConfirmationStore,
    idempotency_store: SqliteIdempotencyStore,
    audit_sink: SqliteAuditSink,
}

fn stores(state: &DaemonState) -> Result<DaemonStores> {
    std::fs::create_dir_all(&state.data_dir)?;
    let path = store_path(state);
    Ok(DaemonStores {
        plan_store: SqlitePlanStore::open(&path)?,
        confirmation_store: SqliteConfirmationStore::open(&path)?,
        idempotency_store: SqliteIdempotencyStore::open(&path)?,
        audit_sink: SqliteAuditSink::open(&path)?,
    })
}

fn store_path(state: &DaemonState) -> PathBuf {
    state.data_dir.join("mcp_story_write.sqlite3")
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
    field: &str,
) -> JsonRpcResponse {
    audited_timecard_error(
        state,
        id,
        -32051,
        "FIELD_REJECTED",
        json!({ "code": "FIELD_REJECTED", "fields": [{"field": field, "reason": "type_mismatch"}] }),
        ResultStatus::Denied,
    )
    .await
}

async fn confirmation_invalid(
    state: &DaemonState,
    id: Option<Value>,
    err: ConfirmationConsumeError,
) -> JsonRpcResponse {
    audited_timecard_error(
        state,
        id,
        -32054,
        "CONFIRMATION_INVALID",
        json!({ "code": "CONFIRMATION_INVALID", "reason": err.to_string() }),
        ResultStatus::Denied,
    )
    .await
}

async fn idempotency_conflict(
    state: &DaemonState,
    id: Option<Value>,
    idempotency_key: &str,
    bound_op_hash: &str,
    attempted_op_hash: &str,
) -> JsonRpcResponse {
    audited_timecard_error(
        state,
        id,
        -32052,
        "IDEMPOTENCY_CONFLICT",
        json!({
            "code": "IDEMPOTENCY_CONFLICT",
            "idempotency_key": idempotency_key,
            "bound_op_hash": bound_op_hash,
            "attempted_op_hash": attempted_op_hash,
        }),
        ResultStatus::Denied,
    )
    .await
}

async fn audited_timecard_error(
    state: &DaemonState,
    id: Option<Value>,
    code: i64,
    message: &str,
    data: Value,
    status: ResultStatus,
) -> JsonRpcResponse {
    match append_audit_event(
        state,
        &Uuid::new_v4().to_string(),
        None,
        TIMECARD_APPLY_TOOL,
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
        Ok(_) => timecard_error(id, code, message, data),
        Err(err) => internal_error(id, err),
    }
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
    event.policy_decisions = policy_decisions_for(status, error.as_ref());
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

fn policy_decisions_for(status: ResultStatus, error: Option<&ErrorRow>) -> Vec<PolicyDecisionRow> {
    let gates = match error.map(|error| error.code.as_str()) {
        Some("FIELD_REJECTED") | Some("GUARD_FAILED") => vec![("FieldAllowlist", "Deny")],
        Some("IDEMPOTENCY_CONFLICT") => vec![("Idempotency", "Deny")],
        Some("CONCURRENCY_CONFLICT") | Some("CONCURRENCY_TOKEN_INVALID") => {
            vec![("ConcurrencyToken", "Deny")]
        }
        Some("CONFIRMATION_INVALID") | Some("PLAN_EXPIRED") => vec![("Confirmation", "Deny")],
        Some("KILL_SWITCH") => vec![("EnvironmentGate", "Deny")],
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

fn is_kill_switched() -> bool {
    std::env::var("SNOW_TIMECARD_WRITE_KILL_SWITCH")
        .or_else(|_| std::env::var("SNOW_MCP_WRITE_KILL_SWITCH"))
        .map(|value| matches!(value.to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
        .unwrap_or(false)
}

fn timecard_error(id: Option<Value>, code: i64, message: &str, data: Value) -> JsonRpcResponse {
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
    use std::sync::Arc;

    use crate::test_support::{
        build_fixture_state, build_fixture_state_at_instance, spawn_json_http_server,
    };
    use snow_core::RecordRef as CoreRecordRef;
    use snow_core::resource::timecard::{SimpleRef, UserRef};
    use snow_mcp::config::McpEnvironment;
    use snow_mcp::domain::policy::PolicyConfig;

    #[test]
    fn editable_state_guard_is_case_insensitive() {
        assert!(is_editable_state("Pending"));
        assert!(is_editable_state("rejected"));
        assert!(is_editable_state("RECALLED"));
        assert!(!is_editable_state("Submitted"));
        assert!(!is_editable_state("Approved"));
    }

    #[test]
    fn preview_set_and_add_hours_are_normalized() {
        assert_eq!(preview_to_hours("1", "8.00", &SetMode::Set).unwrap(), "8");
        assert_eq!(
            preview_to_hours("1.25", "0.50", &SetMode::Add).unwrap(),
            "1.75"
        );
        assert!(preview_to_hours("23", "2", &SetMode::Add).is_err());
    }

    #[test]
    fn plan_preview_recomputes_total_after() {
        let card = sample_card("2", "2026-05-21 10:11:12", Some(3));

        assert_eq!(
            preview_total_after(&card, Weekday::Mon, "8").expect("total"),
            "8"
        );
        assert_eq!(
            preview_total_after(&card, Weekday::Tue, "1.5").expect("total"),
            "3.5"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn apply_conflicts_when_concurrency_changes_even_if_target_value_matches() {
        let planned_card = sample_card("8", "2026-05-21 10:11:12", Some(3));
        let expected_token = concurrency_from_card(&planned_card).expect("expected token");
        let observed_card_json = timecard_record_json("8", "Pending", "2026-05-21 10:11:13", "4");
        let (instance_url, request_rx) =
            spawn_json_http_server(json!({ "result": observed_card_json }))
                .await
                .expect("mock server");
        let fixture = build_fixture_state_at_instance(&instance_url)
            .await
            .expect("fixture");
        let state = DaemonState::with_data_dir_and_mcp_config(
            Arc::clone(&fixture.state.core),
            fixture.tempdir.path().join("timecard-apply-conflict"),
            enabled_timecard_mcp_config(),
        );
        let plan = timecard_plan(&planned_card, "8");
        let stores = stores(&state).expect("stores");
        stores
            .plan_store
            .put(PlanStoreRecord {
                plan_id: plan.plan_id.clone(),
                tool: plan.tool.clone(),
                actor: "tester".to_string(),
                op_hash: plan.op_hash.clone(),
                plan_json: serde_json::to_value(&plan).expect("plan json"),
                concurrency_token: Some(expected_token.clone()),
                created_at: Utc::now(),
                expires_at: Utc::now() + chrono::Duration::seconds(PLAN_TTL_SECONDS),
                state: PlanLifecycleState::Pending,
            })
            .await
            .expect("put plan");
        drop(stores);

        let response = handle_timecard_apply_set_hours(
            Some(json!(1)),
            &json!({
                "plan_id": plan.plan_id,
                "confirmation_token": "not-needed-before-conflict",
                "idempotency_key": "idem-conflict",
                "concurrency_token": expected_token,
            }),
            &state,
        )
        .await;

        let error = response.error.expect("concurrency error");
        assert_eq!(error.code, -32053);
        assert_eq!(error.message, "CONCURRENCY_CONFLICT");
        assert!(
            request_rx
                .await
                .expect("request line")
                .starts_with("GET /api/now/table/time_card/card-sys")
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn apply_idempotency_replay_requires_matching_confirmation_and_returns_replay_receipt() {
        let fixture = build_fixture_state().await.expect("fixture");
        let state = DaemonState::with_data_dir_and_mcp_config(
            Arc::clone(&fixture.state.core),
            fixture.tempdir.path().join("timecard-apply-replay"),
            enabled_timecard_mcp_config(),
        );
        let card = sample_card("8", "2026-05-21 10:11:12", Some(3));
        let expected_token = concurrency_from_card(&card).expect("expected token");
        let plan = timecard_plan(&card, "8");
        let stores = stores(&state).expect("stores");
        stores
            .plan_store
            .put(PlanStoreRecord {
                plan_id: plan.plan_id.clone(),
                tool: plan.tool.clone(),
                actor: "tester".to_string(),
                op_hash: plan.op_hash.clone(),
                plan_json: serde_json::to_value(&plan).expect("plan json"),
                concurrency_token: Some(expected_token.clone()),
                created_at: Utc::now(),
                expires_at: Utc::now() + chrono::Duration::seconds(PLAN_TTL_SECONDS),
                state: PlanLifecycleState::Pending,
            })
            .await
            .expect("put plan");
        let binding = ConfirmationBinding {
            actor: "tester".to_string(),
            requester: "tester".to_string(),
            tool: TIMECARD_APPLY_TOOL.to_string(),
            op_hash: plan.op_hash.clone(),
            environment: "test".to_string(),
        };
        let token = stores
            .confirmation_store
            .issue(&plan.plan_id, binding, 600)
            .await
            .expect("confirmation");
        let key = IdempotencyKey {
            value: "idem-replay".to_string(),
            source: IdempotencyKeySource::ServerDerived,
        };
        assert!(matches!(
            stores
                .idempotency_store
                .check_and_record(&key, TIMECARD_APPLY_TOOL, &plan.op_hash, 600)
                .await
                .expect("record idempotency"),
            IdempotencyOutcome::NewKey
        ));
        let receipt = receipt_for_card(
            &plan,
            Utc::now(),
            &card,
            expected_token.clone(),
            Some("2".to_string()),
        );
        stores
            .idempotency_store
            .save_receipt(&key, &receipt)
            .await
            .expect("save receipt");
        drop(stores);

        let response = handle_timecard_apply_set_hours(
            Some(json!(1)),
            &json!({
                "plan_id": plan.plan_id,
                "confirmation_token": token.token_id,
                "idempotency_key": "idem-replay",
            }),
            &state,
        )
        .await;

        assert!(response.error.is_none(), "{:?}", response.error);
        let result = response.result.expect("receipt");
        assert_eq!(result["idempotency_replay"], json!(true));
        assert_eq!(result["plan_id"], json!(receipt.plan_id));
    }

    fn enabled_timecard_mcp_config() -> snow_mcp::McpConfig {
        let mut policy = PolicyConfig::default();
        policy
            .tools
            .get_mut(TIMECARD_APPLY_TOOL)
            .expect("timecard apply policy")
            .enabled = true;
        snow_mcp::McpConfig {
            environment: McpEnvironment::explicit_config("test", "America/Chicago"),
            policy,
            ..Default::default()
        }
    }

    fn sample_card(monday: &str, sys_updated_on: &str, sys_mod_count: Option<i64>) -> TimeCard {
        TimeCard {
            sys_id: "card-sys".to_string(),
            time_sheet: Some(SimpleRef {
                sys_id: "sheet-sys".to_string(),
                table: "time_sheet".to_string(),
                display: "2026-05-17".to_string(),
            }),
            week_starts_on: "2026-05-17".to_string(),
            user: UserRef {
                sys_id: "user-sys".to_string(),
                user_name: Some("test_user".to_string()),
                email: Some("test@example.com".to_string()),
                display: "Test User".to_string(),
            },
            task: Some(CoreRecordRef {
                sys_id: "task-sys".to_string(),
                number: "PRJ0161219".to_string(),
                table: "pm_project_task".to_string(),
            }),
            category: "project_work".to_string(),
            category_label: "Project/Project Task".to_string(),
            project_time_category: Some("Development".to_string()),
            hours: [
                "0".to_string(),
                monday.to_string(),
                "0".to_string(),
                "0".to_string(),
                "0".to_string(),
                "0".to_string(),
                "0".to_string(),
            ],
            total: monday.to_string(),
            state: "Pending".to_string(),
            sys_updated_on: sys_updated_on.to_string(),
            sys_mod_count,
        }
    }

    fn timecard_plan(card: &TimeCard, to: &str) -> OperationPlan {
        OperationPlanBuilder::new(TIMECARD_PLAN_TOOL)
            .target(RecordRef {
                sys_id: card.sys_id.clone(),
                number: "PRJ0161219".to_string(),
                table: TIME_CARD_TABLE.to_string(),
            })
            .planned_changes(json!({
                "kind": "TimecardSetHours",
                "card_sys_id": card.sys_id.clone(),
                "day": "monday",
                "day_field": "monday",
                "from": "2",
                "to": to,
                "hours": to,
                "mode": "set",
                "actor_user_sys_id": card.user.sys_id.clone(),
                "card_fingerprint": card_fingerprint(card),
            }))
            .build()
    }

    fn timecard_record_json(
        monday: &str,
        state: &str,
        sys_updated_on: &str,
        sys_mod_count: &str,
    ) -> Value {
        json!({
            "sys_id": "card-sys",
            "time_sheet": {
                "value": "sheet-sys",
                "display_value": "2026-05-17"
            },
            "week_starts_on": "2026-05-17",
            "user": {
                "value": "user-sys",
                "display_value": "Test User"
            },
            "user.user_name": "test_user",
            "user.email": "test@example.com",
            "task": {
                "value": "task-sys",
                "display_value": "PRJ0161219"
            },
            "task.number": "PRJ0161219",
            "task.sys_class_name": "pm_project_task",
            "category": {
                "value": "project_work",
                "display_value": "Project/Project Task"
            },
            "project_time_category": "Development",
            "sunday": "0",
            "monday": monday,
            "tuesday": "0",
            "wednesday": "0",
            "thursday": "0",
            "friday": "0",
            "saturday": "0",
            "total": monday,
            "state": {
                "value": state,
                "display_value": state
            },
            "sys_updated_on": sys_updated_on,
            "sys_mod_count": sys_mod_count,
        })
    }
}
