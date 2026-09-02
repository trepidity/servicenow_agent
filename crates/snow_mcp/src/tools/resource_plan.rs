use crate::tools::records::{RESOURCE_PLAN_LOOKUP_TABLES, record_lookup_arg_schema};
use crate::tools::registry::{ToolMetadata, ToolRegistry, object_schema};
use serde_json::{Value, json};

pub fn register(registry: &mut ToolRegistry) {
    registry.add(ToolMetadata {
        name: "resource_plan_get".to_string(),
        description: "Retrieve a resource plan by number or resource_plan sys_id".to_string(),
        input_schema: record_lookup_arg_schema(RESOURCE_PLAN_LOOKUP_TABLES),
        output_schema: object_schema(),
        default_enabled: true,
        requires_confirmation: false,
    });
    registry.add(ToolMetadata {
        name: "resource_plan_list".to_string(),
        description: "List resource plans filtered by parent task, resource, and raw state. Read-only; issues one resource_plan Table API query.".to_string(),
        input_schema: resource_plan_list_arg_schema(),
        output_schema: object_schema(),
        default_enabled: true,
        requires_confirmation: false,
    });
    add_tool(
        registry,
        "resource_plan_plan_create",
        "Plan creation of a governed Resource Plan",
        resource_plan_plan_create_input_schema(),
        plan_output_schema(false),
        true,
        false,
    );
    add_tool(
        registry,
        "resource_plan_apply_create",
        "Apply a confirmed governed Resource Plan creation plan",
        apply_create_input_schema(),
        receipt_output_schema(),
        false,
        true,
    );
    add_tool(
        registry,
        "resource_plan_plan_update",
        "Plan update of a governed Resource Plan",
        resource_plan_plan_update_input_schema(),
        plan_output_schema(true),
        true,
        false,
    );
    add_tool(
        registry,
        "resource_plan_apply_update",
        "Apply a confirmed governed Resource Plan update plan",
        apply_update_input_schema(),
        receipt_output_schema(),
        false,
        true,
    );
    add_tool(
        registry,
        "resource_plan_plan_decision",
        "Plan a governed Resource Plan confirmation decision",
        resource_plan_plan_decision_input_schema(),
        plan_output_schema(true),
        true,
        false,
    );
    add_tool(
        registry,
        "resource_plan_apply_decision",
        "Apply a confirmed governed Resource Plan decision",
        apply_update_input_schema(),
        receipt_output_schema(),
        false,
        true,
    );
}

pub fn resource_plan_list_arg_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "description": "All filters optional. parent_number XOR task_sys_id. resource_sys_id requires resource_type.",
        "properties": {
            "parent_number": { "type": "string" },
            "task_sys_id": { "type": "string", "pattern": "^[0-9a-fA-F]{32}$" },
            "resource_sys_id": { "type": "string", "pattern": "^[0-9a-fA-F]{32}$" },
            "resource_type": { "type": "string", "enum": ["group", "user"] },
            "state": {
                "oneOf": [
                    { "type": "integer" },
                    { "type": "array", "items": { "type": "integer" }, "minItems": 1 }
                ]
            },
            "limit": { "type": "integer", "minimum": 1, "maximum": 200 }
        }
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

fn resource_plan_plan_create_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "parent_sys_id": {"type": "string", "minLength": 32, "maxLength": 32},
            "parent_type": {"type": "string", "enum": ["demand", "project"]},
            "resource_sys_id": {"type": "string", "minLength": 32, "maxLength": 32},
            "resource_type": {"type": "string", "enum": ["group", "user"]},
            "state": {"type": "string"},
            "planned_hours": {"type": "number", "exclusiveMinimum": 0},
            "notes": {"type": "string", "maxLength": 4000},
            "start_date": {"type": "string", "format": "date"},
            "end_date": {"type": "string", "format": "date"}
        },
        "required": ["parent_sys_id", "parent_type", "resource_sys_id", "resource_type", "state", "planned_hours"],
    })
}

fn resource_plan_plan_update_input_schema() -> Value {
    json!({
        "type": "object",
        "description": "Plan an update to exactly one Resource Plan. Exactly one of sys_id or number is required; providing both or neither is rejected.",
        "properties": {
            "sys_id": {
                "type": "string",
                "minLength": 32,
                "maxLength": 32,
                "description": "Target resource_plan sys_id. Use exactly one of sys_id or number."
            },
            "number": {
                "type": "string",
                "pattern": "^RPLN\\d+$",
                "description": "Target Resource Plan number. Use exactly one of sys_id or number."
            },
            "state": {"type": "string"},
            "planned_hours": {"type": "number", "exclusiveMinimum": 0},
            "notes": {"type": "string", "maxLength": 4000},
            "start_date": {"type": "string", "format": "date"},
            "end_date": {"type": "string", "format": "date"}
        }
    })
}

fn resource_plan_plan_decision_input_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "number": {
                "type": "string",
                "pattern": "^RPLN\\d+$"
            },
            "decision": {
                "type": "string",
                "enum": ["confirm", "confirm_and_allocate"]
            }
        },
        "required": ["number", "decision"]
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

#[cfg(test)]
mod tests {
    use super::*;

    fn tool<'a>(registry: &'a ToolRegistry, name: &str) -> &'a ToolMetadata {
        registry
            .metadata()
            .iter()
            .find(|tool| tool.name == name)
            .expect("tool registered")
    }

    #[test]
    fn registers_resource_plan_write_tools() {
        let registry = ToolRegistry::default();
        for name in [
            "resource_plan_plan_create",
            "resource_plan_apply_create",
            "resource_plan_plan_update",
            "resource_plan_apply_update",
            "resource_plan_plan_decision",
            "resource_plan_apply_decision",
        ] {
            assert!(registry.metadata().iter().any(|tool| tool.name == name));
        }
    }

    #[test]
    fn registers_resource_plan_list_as_default_enabled_read_tool() {
        let registry = ToolRegistry::default();
        let tool = tool(&registry, "resource_plan_list");
        assert!(tool.default_enabled);
        assert!(!tool.requires_confirmation);
    }

    #[test]
    fn list_schema_has_corrected_properties() {
        let schema = resource_plan_list_arg_schema();
        let props = schema["properties"].as_object().unwrap();
        for key in [
            "parent_number",
            "task_sys_id",
            "resource_sys_id",
            "resource_type",
            "state",
            "limit",
        ] {
            assert!(props.contains_key(key), "missing {key}");
        }
        for absent in ["parent_type", "parent_table", "year", "quarter"] {
            assert!(!props.contains_key(absent), "{absent} must not be present");
        }
    }

    #[test]
    fn apply_tools_default_disabled() {
        let registry = ToolRegistry::default();
        assert!(tool(&registry, "resource_plan_plan_create").default_enabled);
        assert!(!tool(&registry, "resource_plan_plan_create").requires_confirmation);
        assert!(tool(&registry, "resource_plan_plan_update").default_enabled);
        assert!(!tool(&registry, "resource_plan_plan_update").requires_confirmation);
        assert!(!tool(&registry, "resource_plan_apply_create").default_enabled);
        assert!(tool(&registry, "resource_plan_apply_create").requires_confirmation);
        assert!(!tool(&registry, "resource_plan_apply_update").default_enabled);
        assert!(tool(&registry, "resource_plan_apply_update").requires_confirmation);
        assert!(tool(&registry, "resource_plan_plan_decision").default_enabled);
        assert!(!tool(&registry, "resource_plan_plan_decision").requires_confirmation);
        assert!(!tool(&registry, "resource_plan_apply_decision").default_enabled);
        assert!(tool(&registry, "resource_plan_apply_decision").requires_confirmation);
    }

    #[test]
    fn write_schemas_omit_work_notes_year_quarter_and_parent_table() {
        let registry = ToolRegistry::default();
        for name in ["resource_plan_plan_create", "resource_plan_plan_update"] {
            let schema = &tool(&registry, name).input_schema;
            let properties = schema["properties"].as_object().expect("properties");
            for omitted in ["work_notes", "year", "quarter", "parent_table"] {
                assert!(
                    !properties.contains_key(omitted),
                    "{name} exposes {omitted}"
                );
            }
        }
    }

    #[test]
    fn plan_schemas_have_date_window() {
        let registry = ToolRegistry::default();
        for name in ["resource_plan_plan_create", "resource_plan_plan_update"] {
            let properties = tool(&registry, name).input_schema["properties"]
                .as_object()
                .expect("properties");
            assert_eq!(properties["start_date"]["format"], json!("date"));
            assert_eq!(properties["end_date"]["format"], json!("date"));
        }
    }

    #[test]
    fn plan_create_parent_type_enum_is_demand_project() {
        let registry = ToolRegistry::default();
        assert_eq!(
            tool(&registry, "resource_plan_plan_create").input_schema["properties"]["parent_type"]
                ["enum"],
            json!(["demand", "project"])
        );
    }

    #[test]
    fn plan_create_schema_requires_core_fields() {
        let registry = ToolRegistry::default();
        assert_eq!(
            tool(&registry, "resource_plan_plan_create").input_schema["required"],
            json!([
                "parent_sys_id",
                "parent_type",
                "resource_sys_id",
                "resource_type",
                "state",
                "planned_hours"
            ])
        );
    }

    #[test]
    fn write_input_schemas_avoid_top_level_composition() {
        let registry = ToolRegistry::default();
        for name in [
            "resource_plan_plan_create",
            "resource_plan_apply_create",
            "resource_plan_plan_update",
            "resource_plan_apply_update",
            "resource_plan_plan_decision",
            "resource_plan_apply_decision",
        ] {
            let schema = &tool(&registry, name).input_schema;
            for keyword in ["oneOf", "anyOf", "allOf"] {
                assert!(
                    schema.get(keyword).is_none(),
                    "{name} input schema must not use top-level {keyword}"
                );
            }
        }
    }

    #[test]
    fn plan_update_schema_documents_exactly_one_identity_selector() {
        let registry = ToolRegistry::default();
        let schema = &tool(&registry, "resource_plan_plan_update").input_schema;

        assert!(
            schema["description"]
                .as_str()
                .expect("description")
                .contains("Exactly one of sys_id or number is required")
        );
        assert!(
            schema["properties"]["sys_id"]["description"]
                .as_str()
                .expect("sys_id description")
                .contains("exactly one")
        );
        assert!(
            schema["properties"]["number"]["description"]
                .as_str()
                .expect("number description")
                .contains("exactly one")
        );
    }
}
