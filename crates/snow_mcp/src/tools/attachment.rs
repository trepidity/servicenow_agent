use serde_json::{Value, json};

use crate::tools::registry::{ToolMetadata, ToolRegistry, object_schema};

pub fn register(registry: &mut ToolRegistry) {
    registry.add(ToolMetadata {
        name: "attachment_list".to_string(),
        description: "List attachments on a ServiceNow record".to_string(),
        input_schema: attachment_list_input_schema(),
        output_schema: object_schema(),
        default_enabled: true,
        requires_confirmation: false,
    });
    registry.add(ToolMetadata {
        name: "attachment_upload".to_string(),
        description: "Upload a local file as an attachment on a ServiceNow record".to_string(),
        input_schema: attachment_upload_input_schema(),
        output_schema: object_schema(),
        default_enabled: false,
        requires_confirmation: true,
    });
}

fn attachment_list_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "number": {"type": "string", "pattern": "^[A-Z]+\\d+$"}
        },
        "required": ["number"]
    })
}

fn attachment_upload_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "number": {"type": "string", "pattern": "^[A-Z]+\\d+$"},
            "path": {"type": "string", "minLength": 1},
            "file_name": {"type": "string", "minLength": 1},
            "content_type": {"type": "string", "minLength": 1},
            "confirm_upload": {"type": "boolean", "const": true},
            "idempotency_key": {"type": "string", "minLength": 1}
        },
        "required": ["number", "path", "confirm_upload", "idempotency_key"]
    })
}
