use serde_json::{Value, json};

use crate::tools::registry::{ToolMetadata, ToolRegistry, object_schema};

pub fn register(registry: &mut ToolRegistry) {
    registry.add(ToolMetadata {
        name: "work_note_plan_add".to_string(),
        description: "Preview a work-note addition using the record-field journal path".to_string(),
        input_schema: work_note_plan_add_input_schema(),
        output_schema: object_schema(),
        default_enabled: true,
        requires_confirmation: false,
    });
    registry.add(ToolMetadata {
        name: "work_note_apply_add".to_string(),
        description: "Apply a confirmed work-note addition".to_string(),
        input_schema: work_note_apply_add_input_schema(),
        output_schema: object_schema(),
        default_enabled: false,
        requires_confirmation: true,
    });
}

fn work_note_plan_add_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "number": {"type": "string", "pattern": "^[A-Z]+\\d+$"},
            "text": {"type": "string", "minLength": 1, "maxLength": 16000},
            "work_notes": {"type": "string", "minLength": 1, "maxLength": 16000}
        },
        "required": ["number", "work_notes"]
    })
}

fn work_note_apply_add_input_schema() -> Value {
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
