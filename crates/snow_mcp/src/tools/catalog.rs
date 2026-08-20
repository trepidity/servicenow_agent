use serde_json::{Value, json};

use crate::tools::registry::{ToolMetadata, ToolRegistry, object_schema};

pub fn register(registry: &mut ToolRegistry) {
    add_catalog_tool(
        registry,
        "catalog_items_search",
        "Search Service Catalog items",
        catalog_items_search_input_schema(),
        object_schema(),
        true,
        false,
    );
    add_catalog_tool(
        registry,
        "catalog_item_get",
        "Get a Service Catalog item",
        catalog_item_get_input_schema(),
        object_schema(),
        true,
        false,
    );
    add_catalog_tool(
        registry,
        "catalog_plan_request",
        "Plan a Service Catalog request",
        catalog_plan_request_input_schema(),
        object_schema(),
        true,
        false,
    );
    add_catalog_tool(
        registry,
        "catalog_submit_request",
        "Submit a confirmed Service Catalog request",
        catalog_submit_request_input_schema(),
        object_schema(),
        false,
        true,
    );
}

fn add_catalog_tool(
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

fn catalog_items_search_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "query": {"type": "string", "minLength": 1},
            "limit": {"type": "integer", "minimum": 1, "maximum": 50}
        },
        "required": ["query"]
    })
}

fn catalog_item_get_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "sys_id": {"type": "string", "pattern": "^[0-9a-fA-F]{32}$"},
            "item_sys_id": {"type": "string", "pattern": "^[0-9a-fA-F]{32}$"}
        },
        "required": ["sys_id"]
    })
}

fn catalog_plan_request_input_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "item_sys_id": {"type": "string", "pattern": "^[0-9a-fA-F]{32}$"},
            "sys_id": {"type": "string", "pattern": "^[0-9a-fA-F]{32}$"},
            "variables": {"type": "object", "additionalProperties": true},
            "quantity": {
                "oneOf": [
                    {"type": "string"},
                    {"type": "integer", "minimum": 1}
                ]
            },
            "sysparm_quantity": {
                "oneOf": [
                    {"type": "string"},
                    {"type": "integer", "minimum": 1}
                ]
            },
            "item_guid": {"type": "string"},
            "sysparm_item_guid": {"type": "string"},
            "get_portal_messages": {
                "oneOf": [
                    {"type": "string"},
                    {"type": "boolean"}
                ]
            },
            "sysparm_no_validation": {
                "oneOf": [
                    {"type": "string"},
                    {"type": "boolean"}
                ]
            },
            "no_validation": {
                "oneOf": [
                    {"type": "string"},
                    {"type": "boolean"}
                ]
            },
            "engagement_channel": {"type": "string"},
            "referrer": {},
            "sysparm_ai_referred_fields": {}
        },
        "required": ["item_sys_id", "variables"]
    })
}

fn catalog_submit_request_input_schema() -> Value {
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
