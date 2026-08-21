use crate::tools::registry::{ToolMetadata, ToolRegistry};
use serde_json::{Value, json};

pub fn register(registry: &mut ToolRegistry) {
    add_tool(
        registry,
        "incident_plan_update",
        "Plan a governed Incident claim, unassign, group transfer, state update, or work note",
        incident_plan_update_input_schema(),
        plan_output_schema(true),
        true,
        false,
    );
    add_tool(
        registry,
        "incident_apply_update",
        "Apply a confirmed, concurrency-safe Incident update plan",
        apply_update_input_schema(),
        receipt_output_schema(),
        false,
        true,
    );
    add_tool(
        registry,
        "incident_bulk_plan_update",
        "Plan a governed 3..=25 target Incident bulk update",
        incident_bulk_plan_input_schema(),
        incident_bulk_plan_output_schema(),
        false,
        false,
    );
    add_tool(
        registry,
        "incident_bulk_apply_update",
        "Apply a confirmed, concurrency-safe Incident bulk update plan",
        incident_bulk_apply_input_schema(),
        incident_bulk_receipt_output_schema(),
        false,
        true,
    );
    add_tool(
        registry,
        "change_request_plan_create",
        "Plan creation of a governed Change Request",
        change_request_plan_create_input_schema(),
        plan_output_schema(false),
        true,
        false,
    );
    add_tool(
        registry,
        "change_request_apply_create",
        "Apply a confirmed governed Change Request creation plan",
        apply_create_input_schema(),
        receipt_output_schema(),
        false,
        true,
    );
    add_tool(
        registry,
        "change_request_plan_update",
        "Plan update of a governed Change Request",
        change_request_plan_update_input_schema(),
        plan_output_schema(true),
        true,
        false,
    );
    add_tool(
        registry,
        "change_request_apply_update",
        "Apply a confirmed governed Change Request update plan",
        apply_update_input_schema(),
        receipt_output_schema(),
        false,
        true,
    );
    add_tool(
        registry,
        "change_task_plan_create",
        "Plan creation of a governed Change Task",
        change_task_plan_create_input_schema(),
        plan_output_schema(false),
        true,
        false,
    );
    add_tool(
        registry,
        "change_task_apply_create",
        "Apply a confirmed governed Change Task creation plan",
        apply_create_input_schema(),
        receipt_output_schema(),
        false,
        true,
    );
    add_tool(
        registry,
        "change_task_plan_update",
        "Plan update of a governed Change Task",
        change_task_plan_update_input_schema(),
        plan_output_schema(true),
        true,
        false,
    );
    add_tool(
        registry,
        "change_task_apply_update",
        "Apply a confirmed governed Change Task update plan",
        apply_update_input_schema(),
        receipt_output_schema(),
        false,
        true,
    );
}

fn incident_plan_update_input_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "number": {"type": "string", "pattern": "^INC\\d+$"},
            "assigned_to": {"type": "string", "description": "Use me to claim, unassigned to clear, or an active user sys_id, username, or email."},
            "assignment_group": {"type": "string", "description": "Exact active membership group name or sys_id."},
            "state": {"type": "string", "description": "Exact state value or case-insensitive choice label."},
            "work_notes": {"type": "string", "minLength": 1, "maxLength": 16000}
            ,"comments": {"type": "string", "minLength": 1, "maxLength": 16000}
        },
        "required": ["number"]
    })
}

fn incident_patch_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "assigned_to": {"type": "string", "pattern": "^[0-9a-fA-F]{32}$"},
            "assignment_group": {"type": "string", "pattern": "^[0-9a-fA-F]{32}$"},
            "state": {"type": "string", "minLength": 1},
            "work_notes": {"type": "string", "minLength": 1, "maxLength": 16000},
            "comments": {"type": "string", "minLength": 1, "maxLength": 16000}
        }
    })
}

fn incident_bulk_plan_input_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "shared_patch": incident_patch_schema(),
            "targets": {
                "type": "array",
                "minItems": 3,
                "maxItems": 25,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "properties": {
                        "number": {"type": "string", "pattern": "^INC[0-9]+$"},
                        "sys_id": {"type": "string", "pattern": "^[0-9a-fA-F]{32}$"},
                        "patch": incident_patch_schema()
                    }
                }
            }
        },
        "required": ["targets"]
    })
}

fn incident_bulk_apply_input_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "plan_id": {"type": "string", "minLength": 1},
            "confirmation_token": {"type": "string", "minLength": 1},
            "idempotency_key": {"type": "string", "minLength": 1},
            "concurrency_tokens": {
                "type": "array",
                "minItems": 3,
                "maxItems": 25,
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "properties": {
                        "sys_id": {"type": "string", "pattern": "^[0-9a-f]{32}$"},
                        "sys_updated_on": {"type": "string", "minLength": 1}
                    },
                    "required": ["sys_id", "sys_updated_on"]
                }
            }
        },
        "required": ["plan_id", "confirmation_token", "idempotency_key", "concurrency_tokens"]
    })
}

fn incident_bulk_plan_output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "plan_id": {"type": "string"},
            "op_hash": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
            "apply_tool": {"type": "string", "const": "incident_bulk_apply_update"},
            "preview": {"type": "object"},
            "expires_at": {"type": "string", "format": "date-time"},
            "confirmation_token": {"type": "string"},
            "idempotency_key": {"type": "string"}
        },
        "required": ["plan_id", "op_hash", "apply_tool", "preview", "expires_at", "confirmation_token", "idempotency_key"]
    })
}

fn incident_bulk_receipt_output_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "plan_id": {"type": "string"},
            "audit_id": {"type": "string"},
            "parent_audit_id": {"type": "string"},
            "tool": {"type": "string"},
            "status": {"type": "string", "enum": ["success", "partial"]},
            "op_hash": {"type": "string"},
            "idempotency_replay": {"type": "boolean"},
            "target_results": {"type": "array", "items": {"type": "object"}},
            "applied_count": {"type": "integer"},
            "failed_count": {"type": "integer"},
            "not_attempted_count": {"type": "integer"},
            "cache_coherent": {"type": "boolean"},
            "apply_started_at": {"type": "string", "format": "date-time"},
            "completed_at": {"type": "string", "format": "date-time"}
        },
        "required": ["plan_id", "audit_id", "parent_audit_id", "tool", "status", "op_hash", "idempotency_replay", "target_results", "applied_count", "failed_count", "not_attempted_count", "cache_coherent", "apply_started_at", "completed_at"]
    })
}

fn add_tool(
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

fn change_request_plan_create_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": change_request_properties(false),
        "required": ["short_description", "description", "assignment_group", "cmdb_ci", "start_date", "end_date", "change_plan", "backout_plan", "test_plan"],
    })
}

fn change_request_plan_update_input_schema() -> Value {
    let mut properties = change_request_properties(true);
    properties["number"] = json!({"type": "string", "pattern": "^CHG\\d+$"});
    json!({
        "type": "object",
        "properties": properties,
        "required": ["number"],
    })
}

fn change_task_plan_create_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "parent_change_number": {"type": "string", "pattern": "^CHG\\d+$"},
            "short_description": {"type": "string", "minLength": 1, "maxLength": 240},
            "description": {"type": "string", "maxLength": 16000},
            "assignment_group": {"type": "string"},
            "assigned_to": {"type": "string"},
            "start_date": {"type": "string"},
            "end_date": {"type": "string"},
            "state": {"type": "string"},
            "work_notes": {"type": "string", "minLength": 1, "maxLength": 16000}
        },
        "required": ["parent_change_number", "short_description"],
    })
}

fn change_task_plan_update_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "number": {"type": "string", "pattern": "^CTASK\\d+$"},
            "short_description": {"type": "string", "maxLength": 240},
            "description": {"type": "string", "maxLength": 16000},
            "assignment_group": {"type": "string"},
            "assigned_to": {"type": "string"},
            "start_date": {"type": "string"},
            "end_date": {"type": "string"},
            "state": {"type": "string"},
            "work_notes": {"type": "string", "minLength": 1, "maxLength": 16000}
        },
        "required": ["number"],
    })
}

fn change_request_properties(include_state: bool) -> Value {
    let mut properties = json!({
        "short_description": {"type": "string", "minLength": 1, "maxLength": 240},
        "description": {"type": "string", "maxLength": 16000},
        "type": {"type": "string"},
        "category": {"type": "string"},
        "assignment_group": {"type": "string"},
        "assigned_to": {"type": "string"},
        "cmdb_ci": {"type": "string"},
        "start_date": {"type": "string"},
        "end_date": {"type": "string"},
        "implementation_plan": {"type": "string", "maxLength": 32000},
        "change_plan": {"type": "string", "maxLength": 32000},
        "backout_plan": {"type": "string", "maxLength": 32000},
        "test_plan": {"type": "string", "maxLength": 32000},
        "risk": {"type": "string"},
        "impact": {"type": "string"},
        "justification": {"type": "string", "maxLength": 16000},
        "requested_by": {"type": "string"},
        "requested_by_date": {"type": "string"},
        "u_subcategory": {"type": "string"},
        "u_division": {"type": "string"},
        "u_does_this_change_need_cmdb_update": {"type": "string"},
        "work_notes": {"type": "string", "minLength": 1, "maxLength": 16000}
    });
    if include_state {
        properties["state"] = json!({"type": "string"});
    }
    properties
}

fn apply_create_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "plan_id": {"type": "string"},
            "confirmation_token": {"type": "string"},
            "idempotency_key": {"type": "string"}
        },
        "required": ["plan_id", "confirmation_token", "idempotency_key"],
    })
}

fn apply_update_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "plan_id": {"type": "string"},
            "confirmation_token": {"type": "string"},
            "idempotency_key": {"type": "string"},
            "concurrency_token": {"type": "object"}
        },
        "required": ["plan_id", "confirmation_token", "idempotency_key", "concurrency_token"],
    })
}

fn plan_output_schema(requires_concurrency_token: bool) -> Value {
    let mut schema = json!({
        "type": "object",
        "properties": {
            "plan_id": {"type": "string"},
            "op_hash": {"type": "string"},
            "preview": {"type": "object"},
            "expires_at": {"type": "string", "format": "date-time"},
            "confirmation_token": {"type": "string"},
            "idempotency_key": {"type": "string"}
        },
        "required": ["plan_id", "op_hash", "preview", "expires_at", "confirmation_token", "idempotency_key"],
    });
    if requires_concurrency_token {
        schema["properties"]["concurrency_token"] = json!({"type": "object"});
        schema["required"]
            .as_array_mut()
            .expect("plan output required must be an array")
            .push(json!("concurrency_token"));
    }
    schema
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
        "required": ["plan_id", "audit_id", "parent_audit_id", "tool", "status", "idempotency_replay", "completed_at"],
    })
}
