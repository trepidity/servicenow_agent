use serde_json::{Value, json};

use crate::tools::registry::{ToolMetadata, ToolRegistry, object_schema};

pub fn register(registry: &mut ToolRegistry) {
    add_timecard_tool(
        registry,
        "timecard_list",
        "List the authenticated user's time cards for a week",
        timecard_list_input_schema(),
        object_schema(),
        true,
        false,
    );
    add_timecard_tool(
        registry,
        "timecard_plan_set_hours",
        "Plan a governed update to one day on an existing time card",
        plan_set_hours_input_schema(),
        plan_output_schema(),
        true,
        false,
    );
    add_timecard_tool(
        registry,
        "timecard_apply_set_hours",
        "Apply a confirmed governed time-card hours plan",
        apply_set_hours_input_schema(),
        receipt_output_schema(),
        false,
        true,
    );
}

fn add_timecard_tool(
    registry: &mut ToolRegistry,
    name: &str,
    description: &str,
    input_schema: Value,
    output_schema: Value,
    default_enabled: bool,
    requires_confirmation: bool,
) {
    registry.add(ToolMetadata {
        name: name.to_string(),
        description: description.to_string(),
        input_schema,
        output_schema,
        default_enabled,
        requires_confirmation,
    });
}

fn timecard_list_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "week": {
                "type": "string",
                "format": "date",
                "description": "Optional date within the target week; omitted means current week"
            }
        }
    })
}

fn plan_set_hours_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "sys_id": {
                "type": "string",
                "pattern": "^[0-9a-fA-F]{32}$"
            },
            "day": {
                "type": "string",
                "enum": ["sun", "sunday", "mon", "monday", "tue", "tuesday", "wed", "wednesday", "thu", "thursday", "fri", "friday", "sat", "saturday"]
            },
            "hours": {
                "oneOf": [
                    {"type": "string", "pattern": "^[0-9]+(\\.[0-9]{1,2})?$"},
                    {"type": "number", "minimum": 0, "maximum": 24}
                ]
            },
            "mode": {
                "type": "string",
                "enum": ["set", "add"],
                "default": "set"
            }
        },
        "required": ["sys_id", "day", "hours"]
    })
}

fn apply_set_hours_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "plan_id": {"type": "string"},
            "confirmation_token": {"type": "string"},
            "idempotency_key": {"type": "string"},
            "concurrency_token": {"type": "object"}
        },
        "required": [
            "plan_id",
            "confirmation_token",
            "idempotency_key",
            "concurrency_token"
        ]
    })
}

fn plan_output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "plan_id": {"type": "string"},
            "op_hash": {"type": "string"},
            "preview": {"type": "object"},
            "expires_at": {"type": "string", "format": "date-time"},
            "confirmation_token": {"type": "string"},
            "idempotency_key": {"type": "string"},
            "concurrency_token": {"type": "object"}
        },
        "required": [
            "plan_id",
            "op_hash",
            "preview",
            "expires_at",
            "confirmation_token",
            "idempotency_key",
            "concurrency_token"
        ]
    })
}

fn receipt_output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "plan_id": {"type": "string"},
            "audit_id": {"type": "string"},
            "parent_audit_id": {"type": "string"},
            "tool": {"type": "string"},
            "status": {"type": "string"},
            "applied_changes_summary": {"type": "object"},
            "service_now_metadata": {"type": "object"},
            "idempotency_replay": {"type": "boolean"},
            "completed_at": {"type": "string", "format": "date-time"},
            "op_hash": {"type": "string"},
            "record_url": {"type": "string"},
            "record_snapshot": {"type": "object"},
            "changed_fields": {"type": "array", "items": {"type": "object"}},
            "concurrency_token_observed": {"type": "object"},
            "apply_started_at": {"type": "string", "format": "date-time"},
            "error_code": {"type": "string"},
            "warnings": {"type": "array", "items": {"type": "object"}}
        },
        "required": [
            "plan_id",
            "audit_id",
            "parent_audit_id",
            "tool",
            "status",
            "idempotency_replay",
            "completed_at"
        ]
    })
}
