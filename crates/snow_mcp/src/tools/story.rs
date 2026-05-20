use serde_json::{Value, json};

use crate::tools::registry::{ToolMetadata, ToolRegistry, number_arg_schema, object_schema};

pub fn register(registry: &mut ToolRegistry) {
    for name in ["story_get", "story_tasks_list"] {
        registry.add(ToolMetadata {
            name: name.to_string(),
            description: "Retrieve story context and story tasks".to_string(),
            input_schema: number_arg_schema(),
            output_schema: object_schema(),
            default_enabled: true,
            requires_confirmation: false,
        });
    }

    add_story_tool(
        registry,
        "story_plan_create",
        "Plan creation of a board-scoped Story",
        story_plan_create_input_schema(),
        plan_output_schema(false),
        true,
        false,
    );
    add_story_tool(
        registry,
        "story_apply_create",
        "Apply a confirmed board-scoped Story creation plan",
        apply_create_input_schema(),
        receipt_output_schema(),
        false,
        true,
    );
    add_story_tool(
        registry,
        "story_plan_update",
        "Plan update of a board-scoped Story",
        story_plan_update_input_schema(),
        plan_output_schema(true),
        true,
        false,
    );
    add_story_tool(
        registry,
        "story_apply_update",
        "Apply a confirmed board-scoped Story update plan",
        apply_update_input_schema(),
        receipt_output_schema(),
        false,
        true,
    );
    add_story_tool(
        registry,
        "story_task_plan_create",
        "Plan creation of a board-scoped Story task",
        story_task_plan_create_input_schema(),
        plan_output_schema(false),
        true,
        false,
    );
    add_story_tool(
        registry,
        "story_task_apply_create",
        "Apply a confirmed board-scoped Story task creation plan",
        apply_create_input_schema(),
        receipt_output_schema(),
        false,
        true,
    );
    add_story_tool(
        registry,
        "story_task_plan_update",
        "Plan update of a board-scoped Story task",
        story_task_plan_update_input_schema(),
        plan_output_schema(true),
        true,
        false,
    );
    add_story_tool(
        registry,
        "story_task_apply_update",
        "Apply a confirmed board-scoped Story task update plan",
        apply_update_input_schema(),
        receipt_output_schema(),
        false,
        true,
    );
}

fn add_story_tool(
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

fn story_plan_create_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "short_description": {"type": "string", "minLength": 1, "maxLength": 240},
            "description": {"type": "string", "maxLength": 16000},
            "acceptance_criteria": {"type": "string", "maxLength": 16000},
            "priority": {"type": "string"},
            "epic": {"type": "string"},
            "story_points": {
                "oneOf": [
                    {"type": "string"},
                    {"type": "integer", "minimum": 0}
                ]
            },
            "assigned_to": {"type": "string"}
        },
        "required": ["short_description", "description"]
    })
}

fn story_plan_update_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "number": {"type": "string", "pattern": "^STRY\\d+$"},
            "short_description": {"type": "string", "maxLength": 240},
            "description": {"type": "string", "maxLength": 16000},
            "acceptance_criteria": {"type": "string", "maxLength": 16000},
            "priority": {"type": "string"},
            "epic": {"type": "string"},
            "story_points": {
                "oneOf": [
                    {"type": "string"},
                    {"type": "integer", "minimum": 0}
                ]
            },
            "assigned_to": {"type": "string"},
            "state": {"type": "string"}
        },
        "required": ["number"]
    })
}

fn story_task_plan_create_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "parent_story_number": {"type": "string", "pattern": "^STRY\\d+$"},
            "short_description": {"type": "string", "minLength": 1, "maxLength": 240},
            "description": {"type": "string", "maxLength": 16000},
            "assigned_to": {"type": "string"},
            "priority": {"type": "string"},
            "due_date": {"type": "string", "format": "date"},
            "state": {"type": "string"},
            "planned_hours": {"type": "number", "minimum": 0},
            "actual_hours": {"type": "number", "minimum": 0}
        },
        "required": ["parent_story_number", "short_description"]
    })
}

fn story_task_plan_update_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "number": {"type": "string", "pattern": "^SCTASK\\d+$"},
            "short_description": {"type": "string", "maxLength": 240},
            "description": {"type": "string", "maxLength": 16000},
            "assigned_to": {"type": "string"},
            "priority": {"type": "string"},
            "due_date": {"type": "string", "format": "date"},
            "state": {"type": "string"},
            "planned_hours": {"type": "number", "minimum": 0},
            "actual_hours": {"type": "number", "minimum": 0}
        },
        "required": ["number"]
    })
}

fn apply_create_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "plan_id": {"type": "string"},
            "confirmation_token": {"type": "string"},
            "idempotency_key": {"type": "string"}
        },
        "required": ["plan_id", "confirmation_token", "idempotency_key"]
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
        "required": [
            "plan_id",
            "confirmation_token",
            "idempotency_key",
            "concurrency_token"
        ]
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
        "required": [
            "plan_id",
            "op_hash",
            "preview",
            "expires_at",
            "confirmation_token",
            "idempotency_key"
        ]
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
