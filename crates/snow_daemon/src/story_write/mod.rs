use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::{Mutex, OnceLock};

use anyhow::Result;
use chrono::Utc;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use snow_core::{FieldChoice, SnowRecord};
use snow_mcp::audit::{AuditSink, SqliteAuditSink};
use snow_mcp::domain::audit::{
    ActorIdentity, AppliedChange, AuditEvent, ClientIdentity, ErrorRow, PolicyDecisionRow,
    ResultStatus, ServiceNowMetadata,
};
use snow_mcp::domain::policy::BoardBinding;
use snow_mcp::domain::primitives::{IdempotencyKey, IdempotencyKeySource, RecordRef};
use snow_mcp::domain::story::{
    Actor as StoryActor, InScopeFailure, StateChoice, StateResolution, StateResolutionContext,
    WARNING_ASSIGNEE_AMBIGUOUS, WARNING_ASSIGNEE_DEFAULTED_FROM_CALLER,
    WARNING_ASSIGNEE_UNRESOLVED, WARNING_PRIORITY_VALUE_UNMAPPED, assignee_ambiguous_warning_data,
    assignee_defaulted_warning_message, assignee_unresolved_warning_message, equal_sys_id,
    hash_email_for_audit, in_scope_snapshot_from_json, in_scope_snapshot_from_snow_record,
    is_sys_id, mask_email_for_receipt, resolve_state_from_cached_choices, story_in_scope,
    task_parent_in_scope, task_parent_sys_id_from_json,
};
use snow_mcp::planner::{
    ConcurrencyToken, ConfirmationBinding, ConfirmationConsumeError, ConfirmationRecord,
    ConfirmationStore, FieldChange, IdempotencyOutcome, IdempotencyStore, OperationPlan,
    OperationPlanBuilder, OperationReceipt, PlanLifecycleState, PlanStore, PlanStoreRecord,
    ReceiptStatus, ReceiptWarning, SqliteConfirmationStore, SqliteIdempotencyStore,
    SqlitePlanStore,
};
use uuid::Uuid;

use crate::DaemonState;
use crate::rpc::JsonRpcResponse;

const PLAN_TTL_SECONDS: i64 = 600;
const RATE_LIMIT_WINDOW_SECONDS: i64 = 60;
const CREATE_RECOVERY_CLOCK_SKEW_SECONDS: i64 = 300;
const WARNING_STORY_OWNER_DEFAULTED_FROM_CALLER: &str = "STORY_OWNER_DEFAULTED_FROM_CALLER";
const WARNING_MISSING_OPTIONAL_REQUIRED_FIELD: &str = "MISSING_OPTIONAL_REQUIRED_FIELD";
const STORY_BACKLOG_TYPE_FIELD: &str = "backlog_type";
const STORY_BACKLOG_TYPE_PRODUCT: &str = "product";

// Short-lived control-flow result, never stored in bulk; the size difference
// between variants is harmless here.

// Short-lived control-flow result, never stored in bulk; the size difference
// between variants is harmless here.

// Short-lived control-flow result, never stored in bulk; the size difference
// between variants is harmless here.

static RATE_LIMITS: OnceLock<Mutex<BTreeMap<String, Vec<chrono::DateTime<Utc>>>>> = OnceLock::new();

fn story_error(id: Option<Value>, code: i64, message: &str, data: Value) -> JsonRpcResponse {
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

async fn plan_build_error_to_apply_response(
    state: &DaemonState,
    id: Option<Value>,
    tool: &str,
    err: PlanBuildError,
) -> JsonRpcResponse {
    match err {
        PlanBuildError::Invalid(field) => {
            audited_story_error(
                state,
                id,
                tool,
                -32051,
                "FIELD_REJECTED",
                json!({ "code": "FIELD_REJECTED", "fields": [{"field": field, "reason": "type_mismatch"}] }),
                ResultStatus::Denied,
            )
            .await
        }
        PlanBuildError::FieldRejected(fields) => {
            audited_story_error(
                state,
                id,
                tool,
                -32051,
                "FIELD_REJECTED",
                json!({ "code": "FIELD_REJECTED", "fields": fields }),
                ResultStatus::Denied,
            )
            .await
        }
        PlanBuildError::GuardFailed(failure) => {
            let data = serde_json::to_value(failure.to_guard_failed_data())
                .unwrap_or_else(|err| json!({ "details": err.to_string() }));
            audited_story_error(
                state,
                id,
                tool,
                -32050,
                "GUARD_FAILED",
                data,
                ResultStatus::Denied,
            )
            .await
        }
        PlanBuildError::StateRequired(data) => {
            audited_story_error(
                state,
                id,
                tool,
                -32062,
                "STATE_REQUIRED",
                data,
                ResultStatus::Denied,
            )
            .await
        }
        PlanBuildError::NotFound(message) => {
            audited_story_error(
                state,
                id,
                tool,
                -32004,
                "record not found",
                json!({ "details": message }),
                ResultStatus::Error,
            )
            .await
        }
        PlanBuildError::Upstream(err) => internal_error(id, err),
    }
}

pub async fn handle_plan_get(
    id: Option<Value>,
    params: &Value,
    state: &DaemonState,
) -> JsonRpcResponse {
    plan::handle_plan_get_impl(id, params, state).await
}

pub async fn handle_story_plan(
    id: Option<Value>,
    tool: &str,
    params: &Value,
    state: &DaemonState,
) -> JsonRpcResponse {
    plan::handle_story_plan_impl(id, tool, params, state).await
}

pub async fn handle_story_apply(
    id: Option<Value>,
    tool: &str,
    params: &Value,
    state: &DaemonState,
) -> JsonRpcResponse {
    apply::handle_story_apply_impl(id, tool, params, state).await
}

mod apply;
mod assignment;
mod audit;
mod confirmation;
mod fields;
mod plan;
mod recovery;
mod scope;

use apply::*;
use assignment::*;
use audit::*;
use confirmation::*;
use fields::*;
use plan::*;
use recovery::*;
use scope::*;

#[cfg(test)]
mod tests;
