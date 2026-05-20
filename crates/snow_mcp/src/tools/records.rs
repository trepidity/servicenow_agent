use serde_json::json;

use crate::tools::registry::{ToolMetadata, ToolRegistry, number_arg_schema, object_schema};

pub fn register(registry: &mut ToolRegistry) {
    for (name, description, input_schema) in [
        (
            "get_record",
            "Retrieve a ServiceNow record by number",
            number_arg_schema(),
        ),
        (
            "get_approval",
            "Get an approval through the typed runtime path",
            number_arg_schema(),
        ),
        (
            "search_records",
            "Full-text search across records",
            json!({"type":"object","properties":{"query":{"type":"string"},"scope":{"type":"string","enum":["all","knowledge","work_notes"]},"limit":{"type":"integer","minimum":1}},"required":["query"]}),
        ),
        (
            "list_records",
            "List records with optional daemon-side filters",
            json!({"type":"object","properties":{"resource_type":{"type":"string"},"parent_number":{"type":"string"},"assigned_to":{"type":"string"},"limit":{"type":"integer","minimum":1}}}),
        ),
        (
            "list_my_tasks",
            "List active tasks assigned to current user",
            object_schema(),
        ),
        (
            "list_my_approvals",
            "List pending approvals",
            object_schema(),
        ),
        (
            "list_my_projects",
            "List active projects and demands for current user",
            object_schema(),
        ),
        (
            "get_children",
            "Get child tasks for a parent record",
            number_arg_schema(),
        ),
        (
            "get_work_notes",
            "Get work notes for a record",
            number_arg_schema(),
        ),
    ] {
        registry.add(ToolMetadata {
            name: name.to_string(),
            description: description.to_string(),
            input_schema,
            output_schema: object_schema(),
            default_enabled: true,
            requires_confirmation: false,
        });
    }
}
