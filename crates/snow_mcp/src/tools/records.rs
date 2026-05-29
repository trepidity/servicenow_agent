use serde_json::{Value, json};
use snow_core::{RECORD_LOOKUP_ALLOWED_TABLES, normalize_record_lookup_sys_id};

use crate::tools::registry::{ToolMetadata, ToolRegistry, number_arg_schema, object_schema};
use crate::{Error, Result};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RecordLookup {
    Number(String),
    TableSysId { table: String, sys_id: String },
}

pub const RESOURCE_PLAN_LOOKUP_TABLES: &[&str] = &["resource_plan"];

pub fn register(registry: &mut ToolRegistry) {
    for (name, description, input_schema) in [
        (
            "get_record",
            "Retrieve a ServiceNow record by number or allowed table/sys_id",
            record_lookup_arg_schema(RECORD_LOOKUP_ALLOWED_TABLES),
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
            "user_lookup",
            "Resolve one active ServiceNow user by login, email, employee number, sys_id, or inferred query",
            user_lookup_arg_schema(),
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
            "Get work notes for a record by number or allowed table/sys_id",
            record_lookup_arg_schema(RECORD_LOOKUP_ALLOWED_TABLES),
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

pub fn record_lookup_arg_schema(allowed_tables: &[&str]) -> Value {
    json!({
        "type": "object",
        "description": "Provide either number, or table and sys_id together. Runtime validation rejects missing, mixed, or partial lookup modes.",
        "properties": {
            "number": {
                "type": "string",
                "description": "ServiceNow record number, for example TASK3497879"
            },
            "table": {
                "type": "string",
                "enum": allowed_tables,
                "description": "Allowed table name for sys_id lookup"
            },
            "sys_id": {
                "type": "string",
                "pattern": "^[0-9a-fA-F]{32}$",
                "description": "32-character ServiceNow sys_id; must be paired with table"
            }
        },
        "additionalProperties": false
    })
}

fn user_lookup_arg_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "description": "Provide exactly one of query, user_name, email, employee_number, or sys_id. query infers sys_id/email/login lookup order. active defaults to true.",
        "properties": {
            "query": {
                "type": "string",
                "description": "User identifier. Non-email values try user_name, then email, then employee_number."
            },
            "user_name": {
                "type": "string",
                "description": "Exact ServiceNow sys_user.user_name value"
            },
            "email": {
                "type": "string",
                "description": "Exact ServiceNow sys_user.email value"
            },
            "employee_number": {
                "type": "string",
                "description": "Exact ServiceNow sys_user.employee_number value"
            },
            "sys_id": {
                "type": "string",
                "pattern": "^[0-9a-fA-F]{32}$",
                "description": "Exact sys_user sys_id"
            },
            "active": {
                "type": "boolean",
                "default": true,
                "description": "Filter by sys_user.active. Omitted means true."
            }
        }
    })
}

pub fn parse_record_lookup(args: &Value, allowed_tables: &[&str]) -> Result<RecordLookup> {
    let object = args
        .as_object()
        .ok_or_else(|| Error::InvalidParams("arguments must be an object".to_string()))?;

    let number = object.get("number").and_then(Value::as_str).map(str::trim);
    let table = object.get("table").and_then(Value::as_str).map(str::trim);
    let sys_id = object.get("sys_id").and_then(Value::as_str).map(str::trim);

    let has_number = number.is_some_and(|value| !value.is_empty());
    let has_table = table.is_some_and(|value| !value.is_empty());
    let has_sys_id = sys_id.is_some_and(|value| !value.is_empty());

    if has_number && (has_table || has_sys_id) {
        return Err(Error::InvalidParams(
            "provide either number or table + sys_id, not both".to_string(),
        ));
    }

    if has_number {
        return Ok(RecordLookup::Number(number.unwrap().to_string()));
    }

    if has_table != has_sys_id {
        return Err(Error::InvalidParams(
            "table and sys_id must be provided together".to_string(),
        ));
    }

    if !has_table && !has_sys_id {
        return Err(Error::InvalidParams(
            "missing record lookup; provide number or table + sys_id".to_string(),
        ));
    }

    let table = table.unwrap().to_ascii_lowercase();
    if !allowed_tables.iter().any(|allowed| *allowed == table) {
        return Err(Error::InvalidParams(format!(
            "table `{table}` is not allowed for this record lookup"
        )));
    }

    let sys_id = normalize_record_lookup_sys_id(sys_id.unwrap())
        .map_err(|err| Error::InvalidParams(err.to_string()))?;
    Ok(RecordLookup::TableSysId { table, sys_id })
}
