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
            "Retrieve a generic ServiceNow record by number or allowed table/sys_id. Do not use for APM Business Application numbers such as APM0002456; use business_application_query for APM-number lookup, or business_application_get/search when you have sys_id, exact name, or BA filters.",
            record_lookup_arg_schema(RECORD_LOOKUP_ALLOWED_TABLES),
        ),
        (
            "get_approval",
            "Get an approval through the typed runtime path",
            number_arg_schema(),
        ),
        (
            "search_records",
            "Full-text search across generic records. Do not use for APM Business Application numbers such as APM0002456; use business_application_query/search for Business Application routing.",
            search_records_arg_schema(),
        ),
        (
            "user_lookup",
            "Resolve one active ServiceNow user by login, email, employee number, sys_id, or inferred query",
            user_lookup_arg_schema(),
        ),
        (
            "user_search",
            "Live-search active ServiceNow users by first name, last name, name substring, or email substring",
            user_search_arg_schema(),
        ),
        (
            "business_application_get",
            "Get a locally cached Business Application by sys_id or exact name. For an APM number such as APM0002456, use business_application_query with a number filter.",
            business_application_get_arg_schema(),
        ),
        (
            "business_application_search",
            "Live-search Business Applications by name, owner, support group, portfolio, operational state, or attested date. For exact APM numbers such as APM0002456, prefer business_application_query with field=number.",
            business_application_search_arg_schema(),
        ),
        (
            "business_application_query",
            "Query locally projected Business Application fields. Use this for APM identifiers such as APM0002456 by filtering field=number, op=eq, value=APM0002456.",
            business_application_query_arg_schema(),
        ),
        (
            "business_application_fields",
            "List observed Business Application fields from the local projection, including owner-related fields when field mapping for an APM lookup is unclear.",
            business_application_fields_arg_schema(),
        ),
        (
            "server_get",
            "Get a cached Server by sys_id, exact name, or IP address",
            server_get_arg_schema(),
        ),
        (
            "server_search",
            "Live-search Windows and Linux Servers by name, IP address, CI owner group, or class",
            server_search_arg_schema(),
        ),
        (
            "server_query",
            "Query cached Windows and Linux Servers by name, IP address, CI owner group, class, or text",
            server_query_arg_schema(),
        ),
        (
            "server_fields",
            "List observed Server fields from the local cache",
            server_fields_arg_schema(),
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

    registry.add(ToolMetadata {
        name: "approval_approve".to_string(),
        description: "Approve a ServiceNow approval target by record number through the current user's approval row".to_string(),
        input_schema: number_arg_schema(),
        output_schema: object_schema(),
        default_enabled: false,
        requires_confirmation: true,
    });
    registry.add(ToolMetadata {
        name: "approval_reject".to_string(),
        description: "Reject a ServiceNow approval target by record number through the current user's approval row".to_string(),
        input_schema: approval_reject_arg_schema(),
        output_schema: object_schema(),
        default_enabled: false,
        requires_confirmation: true,
    });
}

pub fn record_lookup_arg_schema(allowed_tables: &[&str]) -> Value {
    json!({
        "type": "object",
        "description": "Provide either number, or table and sys_id together. Runtime validation rejects missing, mixed, or partial lookup modes. Do not use this generic lookup for APM Business Application numbers such as APM0002456; use business_application_query with field=number.",
        "properties": {
            "number": {
                "type": "string",
                "description": "Generic ServiceNow work-record number, for example TASK3497879. Not for APM Business Application identifiers such as APM0002456; route those to business_application_query/search."
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

pub fn search_records_arg_schema() -> Value {
    json!({
        "type": "object",
        "description": "Generic full-text record search. Do not use for APM Business Application identifiers such as APM0002456; route APM lookups to business_application_query or business_application_search.",
        "properties": {
            "query": {
                "type": "string",
                "description": "Search text for generic records. Not for exact APM Business Application numbers such as APM0002456; use business_application_query filters instead."
            },
            "scope": {
                "type": "string",
                "enum": ["all", "knowledge", "work_notes"]
            },
            "limit": {
                "type": "integer",
                "minimum": 1
            }
        },
        "required": ["query"],
        "additionalProperties": false
    })
}

fn approval_reject_arg_schema() -> Value {
    json!({
        "type": "object",
        "properties": {
            "number": {
                "type": "string"
            },
            "reason": {
                "type": "string",
                "minLength": 1
            }
        },
        "required": ["number", "reason"]
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

pub fn user_search_arg_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "description": "Provide at least one of first_name, last_name, name_contains, or email_contains. active defaults to true.",
        "properties": {
            "first_name": {
                "type": "string",
                "description": "Exact sys_user.first_name value"
            },
            "last_name": {
                "type": "string",
                "description": "Exact sys_user.last_name value"
            },
            "name_contains": {
                "type": "string",
                "description": "Substring match against sys_user.name"
            },
            "email_contains": {
                "type": "string",
                "description": "Substring match against sys_user.email"
            },
            "limit": {
                "type": "integer",
                "minimum": 1,
                "maximum": 100,
                "default": 20
            },
            "active": {
                "type": "boolean",
                "default": true,
                "description": "Filter by sys_user.active. Omitted means true."
            }
        }
    })
}

pub fn business_application_get_arg_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "description": "Provide exactly one of sys_id or exact Business Application name. APM numbers such as APM0002456 are Business Application identifiers but are not accepted by this get schema; use business_application_query with filters[{field:\"number\",op:\"eq\",value:\"APM0002456\"}]. Runtime validation enforces the exactly-one rule.",
        "properties": {
            "sys_id": {
                "type": "string",
                "pattern": "^[0-9a-fA-F]{32}$",
                "description": "32-character cmdb_ci_business_app sys_id."
            },
            "name": {
                "type": "string",
                "description": "Exact Business Application name, not an APM number. For APM0002456-style identifiers, use business_application_query field=number."
            },
            "persist": {
                "type": "boolean",
                "default": true
            },
            "resolve_references": {
                "type": "boolean",
                "default": true
            },
            "reference_depth": {
                "type": "integer",
                "minimum": 0,
                "maximum": 2,
                "default": 1
            },
            "refresh_dictionary": {
                "type": "boolean",
                "default": false
            }
        }
    })
}

pub fn business_application_search_arg_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "description": "Search cmdb_ci_business_app. Reference filters accept a sys_id or a display-name substring. For exact APM numbers such as APM0002456, use business_application_query with field=number, op=eq. Live search persists by default when supported by the daemon/core contract.",
        "properties": {
            "name": {
                "type": "string",
                "description": "Business Application name substring, not the APM number. For APM0002456-style identifiers, use business_application_query field=number."
            },
            "business_owner": {
                "type": "string",
                "description": "Business Owner display-name substring or sys_id"
            },
            "is_owner": {
                "type": "string",
                "description": "IS Owner / IT Application Owner display-name substring or sys_id"
            },
            "ci_owner_group": {
                "type": "string",
                "description": "CI owner group display-name substring or sys_id"
            },
            "primary_support_group": {
                "type": "string",
                "description": "Primary Support Group display-name substring or sys_id"
            },
            "operational_state": {
                "type": "string",
                "description": "Operational state/status label or raw choice value"
            },
            "operational_state_not": {
                "type": "string",
                "description": "Operational state/status label or raw choice value to exclude"
            },
            "primary_portfolio": {
                "type": "string",
                "description": "Primary Portfolio display-name substring or sys_id"
            },
            "attested_date": {
                "type": "string",
                "pattern": "^\\d{4}-\\d{2}-\\d{2}$",
                "description": "Exact Attested Date"
            },
            "attested_date_on_or_after": {
                "type": "string",
                "pattern": "^\\d{4}-\\d{2}-\\d{2}$"
            },
            "attested_date_on_or_before": {
                "type": "string",
                "pattern": "^\\d{4}-\\d{2}-\\d{2}$"
            },
            "limit": {
                "type": "integer",
                "minimum": 1,
                "maximum": 100,
                "default": 20
            },
            "persist": {
                "type": "boolean",
                "default": true
            },
            "resolve_references": {
                "type": "boolean",
                "default": true
            },
            "reference_depth": {
                "type": "integer",
                "minimum": 0,
                "maximum": 2,
                "default": 1
            },
            "refresh_dictionary": {
                "type": "boolean",
                "default": false
            }
        }
    })
}

pub fn business_application_query_arg_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "description": "Query locally projected Business Applications. Use this for APM identifiers such as APM0002456 by filtering the ServiceNow number field. Filters use ServiceNow field names or convenience aliases such as business_owner, is_owner, ci_owner_group, primary_support_group, operational_state, primary_portfolio.",
        "properties": {
            "text": {
                "type": "string",
                "description": "Free-text Business Application query. For exact APM numbers such as APM0002456, prefer filters with field=number and op=eq."
            },
            "filters": {
                "type": "array",
                "description": "Business Application field filters. For user requests like owner for APM0002456, use [{\"field\":\"number\",\"op\":\"eq\",\"value\":\"APM0002456\"}] before reading owner-related fields.",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["field", "op"],
                    "properties": {
                        "field": {
                            "type": "string",
                            "description": "ServiceNow field name or alias. Use number for APM identifiers such as APM0002456."
                        },
                        "op": {
                            "type": "string",
                            "enum": ["eq", "ne", "contains", "starts_with", "in", "is_empty", "is_not_empty", "gt", "gte", "lt", "lte"],
                            "description": "Comparison operator. Use eq for exact APM number matches."
                        },
                        "value": {
                            "description": "Filter value. For APM number lookups, provide the identifier such as APM0002456."
                        }
                    }
                }
            },
            "include_tombstoned": { "type": "boolean", "default": false },
            "limit": { "type": "integer", "minimum": 1, "maximum": 500, "default": 20 },
            "offset": { "type": "integer", "minimum": 0, "default": 0 },
            "sort": {
                "type": "array",
                "items": {
                    "type": "object",
                    "additionalProperties": false,
                    "required": ["field"],
                    "properties": {
                        "field": { "type": "string" },
                        "direction": { "type": "string", "enum": ["asc", "desc"], "default": "asc" }
                    }
                }
            }
        }
    })
}

pub fn business_application_fields_arg_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "description": "List Business Application field metadata. Use this when an APM lookup succeeds but owner-field mapping is unclear.",
        "properties": {
            "refresh_dictionary": {
                "type": "boolean",
                "default": false,
                "description": "Refresh dictionary metadata to clarify owner-related fields such as business_owner, is_owner, ci_owner_group, and primary_support_group."
            }
        }
    })
}

pub fn server_get_arg_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "description": "Provide exactly one of sys_id, exact Server name, or IP address. Runtime validation enforces that exactly-one rule.",
        "properties": {
            "sys_id": {
                "type": "string",
                "pattern": "^[0-9a-fA-F]{32}$"
            },
            "name": { "type": "string" },
            "ip_address": { "type": "string" }
        }
    })
}

pub fn server_search_arg_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "description": "Search cmdb_ci_server for Windows and Linux server classes. CI owner group accepts a sys_id or display-name substring. Live search persists returned records.",
        "properties": {
            "name": {
                "type": "string",
                "description": "Server name substring"
            },
            "ip_address": {
                "type": "string",
                "description": "Exact server IP address"
            },
            "ci_owner_group": {
                "type": "string",
                "description": "CI owner group display-name substring or sys_id"
            },
            "class": {
                "type": "string",
                "enum": ["linux", "windows", "cmdb_ci_linux_server", "cmdb_ci_win_server", "cmdb_ci_server"]
            },
            "limit": {
                "type": "integer",
                "minimum": 1,
                "maximum": 100,
                "default": 20
            }
        }
    })
}

pub fn server_query_arg_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "description": "Query cached Windows and Linux Servers. Use ci_owner_group to list all cached servers owned by a group.",
        "properties": {
            "text": { "type": "string" },
            "name": { "type": "string" },
            "ip_address": { "type": "string" },
            "ci_owner_group": { "type": "string" },
            "class": {
                "type": "string",
                "enum": ["linux", "windows", "cmdb_ci_linux_server", "cmdb_ci_win_server", "cmdb_ci_server"]
            },
            "limit": { "type": "integer", "minimum": 1, "maximum": 500, "default": 20 },
            "offset": { "type": "integer", "minimum": 0, "default": 0 }
        }
    })
}

pub fn server_fields_arg_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {}
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
