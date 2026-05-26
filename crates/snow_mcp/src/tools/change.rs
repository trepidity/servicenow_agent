use crate::tools::registry::{ToolMetadata, ToolRegistry};
use serde_json::{Value, json};

pub fn register(registry: &mut ToolRegistry) {
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
        "required": ["short_description", "description", "assignment_group", "cmdb_ci", "start_date", "end_date", "implementation_plan", "backout_plan", "test_plan"],
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
        "backout_plan": {"type": "string", "maxLength": 32000},
        "test_plan": {"type": "string", "maxLength": 32000},
        "risk": {"type": "string"},
        "impact": {"type": "string"},
        "justification": {"type": "string", "maxLength": 16000},
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
