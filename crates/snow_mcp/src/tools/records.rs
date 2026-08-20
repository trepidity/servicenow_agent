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
            "business_application_servers",
            "Read servers associated with a Business Application by Business Application number or cmdb_ci_business_app sys_id using bounded live CMDB relationship traversal. Traversal-only: does not persist, prune, or write vault data.",
            business_application_servers_arg_schema(),
        ),
        (
            "business_application_servers_cached",
            "Read cached server relationships for one Business Application by Business Application number, exact name, or cmdb_ci_business_app sys_id. Cache-only: does not call ServiceNow and does not mutate the cache.",
            business_application_servers_cached_arg_schema(),
        ),
        (
            "business_applications_for_server",
            "Cache-only reverse lookup: read cached Business Application relationships for one server by exact server name, IP address, or sys_id. Does not call ServiceNow and does not mutate the cache.",
            business_applications_for_server_arg_schema(),
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
            "incident_fields",
            "Discover readable and writable Incident fields, choices, references, and paging support from ServiceNow",
            incident_fields_arg_schema(),
        ),
        (
            "list_records",
            "List records with optional daemon-side filters",
            json!({"type":"object","additionalProperties":false,"properties":{"resource_type":{"type":"string"},"parent_number":{"type":"string"},"assigned_to":{"type":"string"},"limit":{"type":"integer","minimum":1}}}),
        ),
        (
            "record_query",
            "Query one bounded live page of Change Requests or Stories with typed allowlisted filters and stable cursor paging.",
            record_query_arg_schema(),
        ),
        (
            "list_my_tasks",
            "List active tasks assigned to current user",
            object_schema(),
        ),
        (
            "list_my_approvals",
            "List pending direct and group-routed approvals for the current user",
            json!({"type":"object","additionalProperties":false,"properties":{}}),
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
        (
            "incident_list_by_assignment_group",
            "List one page of active Incidents for an assignment group sys_id, optionally narrowed to one exact state (raw value or exact label such as Pending). Read-only and ephemeral: nothing is cached or persisted. Page through by passing the returned next_cursor until complete is true; an unresolved state returns the valid choices for correction.",
            incident_list_by_assignment_group_arg_schema(),
        ),
        (
            "incident_assignment_groups",
            "List the authenticated user's active direct Incident assignment-group memberships.",
            json!({"type":"object","additionalProperties":false,"properties":{}}),
        ),
        (
            "incident_assignment_group_queue",
            "Read a membership-scoped operational Incident queue with triage filters, operational context, SLA risk, handoff counts, watermark, and departure detection.",
            incident_assignment_group_queue_arg_schema(),
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
        description: "Approve a ServiceNow approval by approval_sys_id from list_my_approvals, or by target record number".to_string(),
        input_schema: approval_action_arg_schema(),
        output_schema: object_schema(),
        default_enabled: false,
        requires_confirmation: true,
    });
    registry.add(ToolMetadata {
        name: "approval_reject".to_string(),
        description: "Reject a ServiceNow approval by approval_sys_id from list_my_approvals, or by target record number".to_string(),
        input_schema: approval_reject_arg_schema(),
        output_schema: object_schema(),
        default_enabled: false,
        requires_confirmation: true,
    });
}

pub fn record_query_arg_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {
            "resource_type": {
                "type": "string",
                "enum": ["change_request", "story"]
            },
            "filters": {
                "type": "object",
                "additionalProperties": false,
                "properties": {
                    "assignment_group": { "type": "string", "pattern": "^[0-9a-fA-F]{32}$" },
                    "assigned_to": { "type": "string", "pattern": "^[0-9a-fA-F]{32}$" },
                    "state": { "type": "string", "minLength": 1, "maxLength": 80 },
                    "start_date_after": { "type": "string", "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}$" },
                    "start_date_before": { "type": "string", "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}$" },
                    "story_owner": { "type": "string", "pattern": "^[0-9a-fA-F]{32}$" },
                    "lead_developer": { "type": "string", "pattern": "^[0-9a-fA-F]{32}$" },
                    "states": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": 20,
                        "uniqueItems": true,
                        "items": { "type": "string", "minLength": 1, "maxLength": 80 }
                    },
                    "sprint": { "type": "string", "pattern": "^[0-9a-fA-F]{32}$" },
                    "project": { "type": "string", "pattern": "^[0-9a-fA-F]{32}$" },
                    "cmdb_ci": { "type": "string", "pattern": "^[0-9a-fA-F]{32}$" },
                    "blocked": { "type": "boolean" },
                    "due_date_after": { "type": "string", "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}$" },
                    "due_date_before": { "type": "string", "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2}$" },
                    "updated_after": { "type": "string", "pattern": "^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}$" },
                    "numbers": {
                        "type": "array",
                        "minItems": 1,
                        "maxItems": 20,
                        "uniqueItems": true,
                        "items": { "type": "string", "pattern": "^[sS][tT][rR][yY][0-9]+$" }
                    },
                    "text": { "type": "string", "minLength": 1, "maxLength": 200 }
                }
            },
            "include_description": { "type": "boolean", "default": false },
            "limit": { "type": "integer", "minimum": 1, "maximum": 200, "default": 50 },
            "cursor": {
                "type": ["string", "null"],
                "pattern": "^[0-9a-fA-F]{32}$"
            }
        },
        "required": ["resource_type"]
    })
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

fn approval_action_arg_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "description": "Provide exactly one of number or approval_sys_id. Prefer approval_sys_id from list_my_approvals.records[].record.sys_id for the shortest caller-scoped approval path.",
        "properties": {
            "number": {
                "type": "string",
                "description": "Target record number, for example CHANGE0010001. This legacy path resolves the target and then finds the current user's pending approval row."
            },
            "approval_sys_id": {
                "type": "string",
                "pattern": "^[0-9a-fA-F]{32}$",
                "description": "sysapproval_approver.sys_id returned by list_my_approvals.records[].record.sys_id."
            }
        }
    })
}

fn approval_reject_arg_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "description": "Provide exactly one of number or approval_sys_id plus a rejection reason. Prefer approval_sys_id from list_my_approvals.records[].record.sys_id for the shortest caller-scoped rejection path.",
        "properties": {
            "number": {
                "type": "string",
                "description": "Target record number. This legacy path resolves the target and then finds the current user's pending approval row."
            },
            "approval_sys_id": {
                "type": "string",
                "pattern": "^[0-9a-fA-F]{32}$",
                "description": "sysapproval_approver.sys_id returned by list_my_approvals.records[].record.sys_id."
            },
            "reason": {
                "type": "string",
                "minLength": 1
            }
        },
        "required": ["reason"]
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

pub fn business_application_servers_arg_schema() -> Value {
    // Bound literals are derived from the canonical snow_core constants so the
    // advertised JSON schema can never drift from the values that
    // `snow_core::BusinessApplicationServersParams::validate` actually enforces.
    let max_depth = snow_core::BUSINESS_APPLICATION_SERVERS_MAX_DEPTH;
    let default_depth = snow_core::BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_DEPTH;
    let max_cis = snow_core::BUSINESS_APPLICATION_SERVERS_MAX_CIS;
    let default_cis = snow_core::BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_CIS;
    let max_edges = snow_core::BUSINESS_APPLICATION_SERVERS_MAX_EDGES;
    let default_edges = snow_core::BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_EDGES;
    let max_service_membership_associations =
        snow_core::BUSINESS_APPLICATION_SERVERS_MAX_SERVICE_MEMBERSHIP_ASSOCIATIONS;
    let default_service_membership_associations =
        snow_core::BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_SERVICE_MEMBERSHIP_ASSOCIATIONS;
    let max_service_membership_pages =
        snow_core::BUSINESS_APPLICATION_SERVERS_MAX_SERVICE_MEMBERSHIP_PAGES;
    let default_service_membership_pages =
        snow_core::BUSINESS_APPLICATION_SERVERS_DEFAULT_MAX_SERVICE_MEMBERSHIP_PAGES;
    json!({
        "type": "object",
        "additionalProperties": false,
        "description": "Reads Server CIs associated with one Business Application via bounded live CMDB relationship traversal. Provide exactly one of number or sys_id. Runtime validation enforces the selector XOR and traversal bounds. Traversal-only: does not persist, prune, or write vault data. Server detection is class-hierarchy aware: any CMDB class extending cmdb_ci_server (the canonical cmdb_ci_server/cmdb_ci_linux_server/cmdb_ci_win_server tables plus subclasses such as cmdb_ci_esx_server, cmdb_ci_aix_server, cmdb_ci_solaris_server, and instance-specific cmdb_ci_*_server classes) is returned as a server.",
        "properties": {
            "number": {
                "type": "string",
                "description": "Business Application number, for example <APM_NUMBER>. Not a local BA:<sys_id> fallback identifier."
            },
            "sys_id": {
                "type": "string",
                "pattern": "^[0-9a-fA-F]{32}$",
                "description": "32-character cmdb_ci_business_app sys_id."
            },
            "max_depth": {
                "type": "integer",
                "minimum": 1,
                "maximum": max_depth,
                "default": default_depth,
                "description": format!("Maximum relationship-traversal depth (BFS hops) from the root Business Application. Range 1-{max_depth}, default {default_depth}.")
            },
            "max_cis": {
                "type": "integer",
                "minimum": 1,
                "maximum": max_cis,
                "default": default_cis,
                "description": format!("Maximum number of configuration items examined BEYOND the root Business Application. The root BA is excluded from this budget, so up to max_cis non-root CIs may be examined before traversal truncates. Range 1-{max_cis}, default {default_cis}.")
            },
            "max_edges": {
                "type": "integer",
                "minimum": 1,
                "maximum": max_edges,
                "default": default_edges,
                "description": format!("Maximum number of cmdb_rel_ci relationship edges examined across the whole traversal. Edge reads are paginated and continue until this budget is consumed or the result set is exhausted, so large graphs are not silently undercounted. Range 1-{max_edges}, default {default_edges}.")
            },
            "max_service_membership_associations": {
                "type": "integer",
                "minimum": 1,
                "maximum": max_service_membership_associations,
                "default": default_service_membership_associations,
                "description": format!("Maximum number of svc_ci_assoc service-membership associations examined across the whole traversal. Range 1-{max_service_membership_associations}, default {default_service_membership_associations}.")
            },
            "max_service_membership_pages": {
                "type": "integer",
                "minimum": 1,
                "maximum": max_service_membership_pages,
                "default": default_service_membership_pages,
                "description": format!("Maximum number of svc_ci_assoc service-membership pages examined across the whole traversal. Range 1-{max_service_membership_pages}, default {default_service_membership_pages}.")
            },
            "relationship_type": {
                "type": "array",
                "items": {
                    "type": "string"
                },
                "default": [],
                "description": "Optional allowlist of CMDB relationship types (cmdb_rel_type names or sys_ids) that gate which edges are traversed. When omitted/empty, the default set (Depends on::Used by, Runs on::Runs, Contains::Contained by, Hosted on::Hosts, Instantiates::Instantiated by, Members::Member of) is used, resolved to stable cmdb_rel_type sys_id identities so a renamed or localized display label still matches. An explicit non-empty list is matched against both each edge's raw value and its display label."
            },
            "include_paths": {
                "type": "boolean",
                "default": false,
                "description": "When true, the result includes server_paths: every route (chain of relationship edges) from the root Business Application to each server, reporting multiple alternate paths when a server is reachable via different parents (diamond topology). One server result is still returned per server regardless of path count. Default false."
            },
            "fallback_strategy": {
                "type": "string",
                "enum": ["none", "ci_owner_group"],
                "default": "none",
                "description": "Strategy to use when the CMDB relationship traversal finds 0 servers. 'none' (default) preserves current behavior exactly: a 0-server traversal returns servers:[] with no new fields. 'ci_owner_group' queries cmdb_ci_server by the BA's raw u_ci_owner_group field when traversal returns empty, returning servers tagged source:ci_owner_group_fallback (live-only, never persisted) and surfacing the CMDB data-quality gap via relationship_summary.degraded_reasons.cmdb_relationships_unmapped. The fallback never fires when traversal finds one or more servers."
            }
        }
    })
}

pub fn business_application_servers_cached_arg_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "description": "Reads locally cached Server relationships for one Business Application. Provide exactly one of number, sys_id, or exact name. This tool is cache-only: it does not call ServiceNow, does not run live traversal, and does not persist or prune data.",
        "properties": {
            "number": {
                "type": "string",
                "description": "Business Application number, for example <APM_NUMBER>."
            },
            "sys_id": {
                "type": "string",
                "pattern": "^[0-9a-fA-F]{32}$",
                "description": "32-character cmdb_ci_business_app sys_id."
            },
            "name": {
                "type": "string",
                "description": "Exact Business Application name. Ambiguous duplicate names are rejected."
            },
            "include_tombstoned": {
                "type": "boolean",
                "default": false,
                "description": "Include tombstoned relationship rows and tombstoned endpoint records. Pruned records are never returned."
            }
        }
    })
}

pub fn business_applications_for_server_arg_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "description": "Cache-only reverse lookup for locally cached Business Application relationships for one Server. Provide exactly one of sys_id, exact name, or ip_address. Exact name can return multiple cached server matches. This tool does not call ServiceNow, does not run live traversal, and does not persist or prune data.",
        "properties": {
            "sys_id": {
                "type": "string",
                "pattern": "^[0-9a-fA-F]{32}$",
                "description": "32-character Server sys_id."
            },
            "name": {
                "type": "string",
                "description": "Exact Server name. Duplicate cached names return multiple matched servers."
            },
            "ip_address": {
                "type": "string",
                "description": "Exact Server IP address."
            },
            "include_tombstoned": {
                "type": "boolean",
                "default": false,
                "description": "Include tombstoned relationship rows and tombstoned endpoint records. Pruned records are never returned."
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

/// Argument schema for `incident_fields`.
///
/// Deliberately empty: the Incident table is fixed by the operation, and
/// accepting a caller-supplied table would turn typed metadata discovery into
/// the generic table browser this contract forbids.
pub fn incident_fields_arg_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {}
    })
}

pub fn server_fields_arg_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "properties": {}
    })
}

/// Argument schema for `incident_list_by_assignment_group`.
///
/// A plain object with no top-level composition, so the schema smoke check and
/// strict MCP clients both accept it. `additionalProperties: false` mirrors the
/// core input's `deny_unknown_fields`, and `limit`'s bounds mirror
/// [`snow_core::INCIDENT_GROUP_LIST_MAX_LIMIT`].
pub fn incident_list_by_assignment_group_arg_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["assignment_group_sys_id"],
        "description": "Lists active Incidents for one assignment group. Page with next_cursor until complete is true. Results reflect the credential's ServiceNow ACLs; this tool applies no scope narrowing of its own.",
        "properties": {
            "assignment_group_sys_id": {
                "type": "string",
                "pattern": "^[0-9a-fA-F]{32}$",
                "description": "sys_user_group sys_id. Group names are not accepted."
            },
            "state": {
                "type": "string",
                "description": "Exact Incident state: a raw value such as 3, or an exact case-insensitive label such as Pending. Substring matching is not supported."
            },
            "limit": {
                "type": "integer",
                "minimum": 1,
                "maximum": snow_core::INCIDENT_GROUP_LIST_MAX_LIMIT,
                "description": "ServiceNow rows requested for this page (default 50). Returned records may be fewer after inactive and closed rows are dropped."
            },
            "cursor": {
                "type": "string",
                "pattern": "^[0-9a-fA-F]{32}$",
                "description": "next_cursor from the previous page. Exclusive: paging resumes after that sys_id."
            }
        }
    })
}

pub fn incident_assignment_group_queue_arg_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": ["group"],
        "properties": {
            "group": {"type":"string","minLength":1,"description":"Exact active membership group name or sys_id."},
            "state": {"type":"string"},
            "assigned_to": {"type":"string","description":"me, unassigned, or an active user sys_id, username, or email."},
            "priorities": {"type":"array","maxItems":5,"uniqueItems":true,"items":{"type":"integer","minimum":1,"maximum":5}},
            "opened_after": {"type":"string","description":"YYYY-MM-DD HH:MM:SS"},
            "opened_before": {"type":"string","description":"YYYY-MM-DD HH:MM:SS"},
            "updated_since": {"type":"string","description":"Prior watermark or YYYY-MM-DD HH:MM:SS delta lower bound."},
            "updated_before": {"type":"string","description":"YYYY-MM-DD HH:MM:SS"},
            "stale_before": {"type":"string","description":"YYYY-MM-DD HH:MM:SS; defaults to 24 hours before this call."},
            "stale_only": {"type":"boolean","default":false},
            "sla_risk": {"type":"string","enum":["any","healthy","at_risk","breached","unavailable"],"default":"any"},
            "sla_at_risk_percentage": {"type":"number","minimum":0,"maximum":100,"default":80},
            "sort_by": {"type":"string","enum":["priority","opened_at","updated_at","assignee","sla_risk"],"default":"priority"},
            "sort_direction": {"type":"string","enum":["asc","desc"],"default":"asc"},
            "limit": {"type":"integer","minimum":1,"maximum":200},
            "offset": {"type":"integer","minimum":0},
            "scan_limit": {"type":"integer","minimum":1,"maximum":5000},
            "known_sys_ids": {"type":"array","maxItems":1000,"items":{"type":"string","pattern":"^[0-9a-fA-F]{32}$"}}
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
