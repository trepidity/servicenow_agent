use super::*;

pub(in crate::rpc) const CONTRACT_VERSION: &str = "daemon-json-rpc-v1";

pub(in crate::rpc) const SUPPORTED_RPC_METHODS: &[&str] = &[
    "contract_info",
    "cache_policy_validate",
    "cache_policy_reload",
    "ping",
    "get_record",
    "get_record_fresh",
    "get_article",
    "get_article_fresh",
    "task_sla_status",
    "task_sla_status_for_tasks",
    "search_records",
    "user_lookup",
    "user_search",
    "business_application_get",
    "business_application_get_fresh",
    "business_application_search",
    "business_application_query",
    "business_application_servers",
    "business_application_servers_cached",
    "business_applications_for_server",
    "business_application_sync",
    "business_application_fields",
    "resource_plan_list",
    "incident_list_by_assignment_group",
    "incident_get",
    "incident_query",
    "incident_assignment_groups",
    "incident_fields",
    "incident_assignment_group_queue",
    "server_get",
    "server_get_fresh",
    "server_search",
    "server_query",
    "server_fields",
    "search_knowledge",
    "kb_semantic_search",
    "list_knowledge_bases",
    "list_categories",
    "list_knowledge_articles",
    "get_approval",
    "catalog_items_search",
    "catalog_item_get",
    "catalog_plan_request",
    "catalog_submit_request",
    "get_children",
    "change_request_list_tasks",
    "get_work_notes",
    "list_records",
    "record_query",
    "list_my_tasks",
    "list_my_approvals",
    "list_my_projects",
    "list_my_stories",
    "list_my_incidents",
    "vault_path",
    "add_work_note",
    "attachment_list",
    "attachment_upload",
    "set_state",
    "field_choices",
    "approval_approve",
    "approval_reject",
    "get_degraded_reads",
    "cache_info",
    "repair_vault",
    "verify_vault",
    "prune_orphans",
    "refresh_all",
    "kb_sync",
    "kb_list_tags",
    "kb_status",
    "kb_semantic_status",
    "kb_semantic_rebuild",
    "scheduler.status",
    "scheduler.trigger_now",
    "start_job",
    "get_job",
    "list_jobs",
    "cancel_job",
    "plan_get",
    "work_note_plan_add",
    "work_note_apply_add",
    "change_request_plan_create",
    "change_request_apply_create",
    "change_request_plan_update",
    "change_request_apply_update",
    "change_task_plan_create",
    "change_task_apply_create",
    "change_task_plan_update",
    "change_task_apply_update",
    "incident_plan_update",
    "incident_apply_update",
    "incident_bulk_plan_update",
    "incident_bulk_apply_update",
    "resource_plan_plan_create",
    "resource_plan_apply_create",
    "resource_plan_plan_update",
    "resource_plan_apply_update",
    "story_plan_create",
    "story_apply_create",
    "story_plan_update",
    "story_apply_update",
    "story_task_plan_create",
    "story_task_apply_create",
    "story_task_plan_update",
    "story_task_apply_update",
    "timecard_list",
    "timecard_set_hours",
    "timecard_plan_set_hours",
    "timecard_apply_set_hours",
    "shutdown",
];

pub(in crate::rpc) const DEPRECATED_RPC_ALIASES: &[(&str, &str)] = &[
    ("get_knowledge_article", "get_article"),
    ("get_knowledge_article_fresh", "get_article_fresh"),
    ("my_tasks", "list_my_tasks"),
    ("my_tasks_fresh", "list_my_tasks"),
    ("my_approvals", "list_my_approvals"),
    ("my_approvals_fresh", "list_my_approvals"),
    ("my_projects", "list_my_projects"),
    ("my_projects_fresh", "list_my_projects"),
    ("my_stories_fresh", "list_my_stories"),
    ("my_incidents_fresh", "list_my_incidents"),
    ("approve", "approval_approve"),
    ("reject", "approval_reject"),
];

pub(in crate::rpc) fn contract_info(state: &DaemonState) -> Value {
    let env_label =
        std::env::var("SNOW_ENV").unwrap_or_else(|_| crate::DEFAULT_DAEMON_ENV.to_string());
    let instance_host = normalize_instance_host(&state.core.config().instance.url);
    let (mcp_mode, mcp_transport) =
        normalize_mcp_availability(state.core.config().daemon.mcp_transport.as_str());
    let deprecated_aliases = DEPRECATED_RPC_ALIASES
        .iter()
        .map(|(method, replacement)| {
            json!({
                "method": method,
                "replacement": replacement,
            })
        })
        .collect::<Vec<_>>();

    json!({
        "contract_version": CONTRACT_VERSION,
        "daemon_version": env!("CARGO_PKG_VERSION"),
        "instance_host": instance_host,
        "supported_methods": SUPPORTED_RPC_METHODS,
        "deprecated_aliases": deprecated_aliases,
        "environment": {
            "label": env_label,
            "instance_host": instance_host,
            "username": state.core.config().instance.user,
        },
        "warming_model": "passive",
        "mcp_availability": {
            "mode": mcp_mode,
            "transport": mcp_transport,
        },
    })
}

pub(in crate::rpc) fn normalize_instance_host(instance_url: &str) -> Option<String> {
    let trimmed = instance_url.trim();
    if trimmed.is_empty() {
        return None;
    }

    let without_scheme = trimmed
        .split_once("://")
        .map(|(_, rest)| rest)
        .unwrap_or(trimmed);
    let authority = without_scheme
        .split(['/', '?', '#'])
        .next()
        .unwrap_or("")
        .trim_end_matches('/');
    let host_port = authority
        .rsplit_once('@')
        .map(|(_, host)| host)
        .unwrap_or(authority);

    let host = if let Some(rest) = host_port.strip_prefix('[') {
        rest.split(']').next().unwrap_or("")
    } else {
        host_port.split(':').next().unwrap_or("")
    };

    if host.is_empty() {
        None
    } else {
        Some(host.to_string())
    }
}

pub(in crate::rpc) fn normalize_mcp_availability(
    configured_transport: &str,
) -> (&'static str, &'static str) {
    match configured_transport {
        "stdio" => ("local_stdio", "stdio"),
        "disabled" | "" => ("disabled", "disabled"),
        "http" => ("future_remote_transport", "http"),
        "sse" => ("future_remote_transport", "sse"),
        _ => ("unknown", "unknown"),
    }
}

pub(in crate::rpc) fn internal_error(id: Option<Value>, err: impl ToString) -> JsonRpcResponse {
    JsonRpcResponse::error(
        id,
        -32000,
        "internal error",
        Some(json!({ "details": err.to_string() })),
    )
}

/// JSON-RPC code for an unresolvable record-number prefix (caller mistake).
pub(in crate::rpc) const UNKNOWN_PREFIX_CODE: i64 = -32006;

/// Map core lookup failures that are caller mistakes (unknown prefix) to a
/// structured JSON-RPC error instead of `internal_error` (-32000).
pub(in crate::rpc) fn invalid_params(id: Option<Value>, err: impl ToString) -> JsonRpcResponse {
    JsonRpcResponse::error(
        id,
        -32602,
        "invalid params",
        Some(json!({ "details": err.to_string() })),
    )
}

impl JsonRpcResponse {
    pub(crate) fn ok(id: Option<Value>, result: Value) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            result: Some(result),
            error: None,
            id,
        }
    }

    pub(crate) fn error(id: Option<Value>, code: i64, message: &str, data: Option<Value>) -> Self {
        Self {
            jsonrpc: "2.0".to_string(),
            result: None,
            error: Some(JsonRpcError {
                code,
                message: message.to_string(),
                data,
            }),
            id,
        }
    }
}

pub(in crate::rpc) async fn dispatch_system(
    method: RpcMethod,
    id: Option<Value>,
    request: &JsonRpcRequest,
    state: &Arc<DaemonState>,
    _transport: &DaemonTransport<'_>,
) -> JsonRpcResponse {
    match method {
        RpcMethod::ContractInfo => JsonRpcResponse::ok(id, contract_info(state.as_ref())),
        RpcMethod::CachePolicyValidate | RpcMethod::CachePolicyReload => {
            if request
                .params
                .as_object()
                .is_none_or(|params| !params.is_empty())
            {
                return invalid_params(id, "cache-policy lifecycle accepts exactly {}");
            }
            let result = if method == RpcMethod::CachePolicyValidate {
                state.cache_policy.validate().and_then(|value| {
                    serde_json::to_value(value).map_err(|err| {
                        snow_core::cache::policy::CachePolicyError::Invalid {
                            field: None,
                            rule: None,
                            reason: err.to_string(),
                        }
                    })
                })
            } else {
                state.cache_policy.reload().and_then(|value| {
                    serde_json::to_value(value).map_err(|err| {
                        snow_core::cache::policy::CachePolicyError::Invalid {
                            field: None,
                            rule: None,
                            reason: err.to_string(),
                        }
                    })
                })
            };
            match result {
                Ok(value) => JsonRpcResponse::ok(id, value),
                Err(snow_core::cache::policy::CachePolicyError::Invalid {
                    field,
                    rule,
                    reason,
                }) => {
                    let mut data = json!({ "code": "CACHE_POLICY_INVALID", "reason": reason });
                    let map = data.as_object_mut().expect("JSON object literal");
                    if let Some(field) = field {
                        map.insert("field".to_string(), json!(field));
                    }
                    if let Some(rule) = rule {
                        map.insert("rule".to_string(), json!(rule));
                    }
                    JsonRpcResponse::error(id, -32070, "cache policy invalid", Some(data))
                }
                Err(snow_core::cache::policy::CachePolicyError::Io { kind, source }) => {
                    JsonRpcResponse::error(
                        id,
                        -32071,
                        "cache policy I/O failed",
                        Some(
                            json!({ "code": "CACHE_POLICY_IO", "kind": kind, "reason": source.to_string() }),
                        ),
                    )
                }
            }
        }
        RpcMethod::Ping => JsonRpcResponse::ok(id, json!({ "ok": true })),
        RpcMethod::Shutdown => JsonRpcResponse::ok(id, json!({ "status": "shutting_down" })),
        RpcMethod::CatalogItemsSearch => {
            crate::catalog_write::handle_catalog_items_search(id, &request.params, state).await
        }
        RpcMethod::CatalogItemGet => {
            crate::catalog_write::handle_catalog_item_get(id, &request.params, state).await
        }
        RpcMethod::RefreshAll => JsonRpcResponse::ok(id, json!({ "status": "queued" })),
        RpcMethod::SchedulerStatus => JsonRpcResponse::ok(id, json!({ "status": "available" })),
        RpcMethod::SchedulerTriggerNow => JsonRpcResponse::ok(id, json!({ "status": "queued" })),
        RpcMethod::PlanGet => crate::story_write::handle_plan_get(id, &request.params, state).await,
        RpcMethod::CatalogPlanRequest => {
            crate::catalog_write::handle_catalog_plan_request(id, &request.params, state).await
        }
        RpcMethod::CatalogSubmitRequest => {
            crate::catalog_write::handle_catalog_submit_request(id, &request.params, state).await
        }
        RpcMethod::WorkNotePlanAdd => {
            crate::work_note_write::handle_work_note_plan_add(id, &request.params, state).await
        }
        RpcMethod::WorkNoteApplyAdd => {
            crate::work_note_write::handle_work_note_apply_add(id, &request.params, state).await
        }
        RpcMethod::ChangeRequestPlanCreate
        | RpcMethod::ChangeRequestPlanUpdate
        | RpcMethod::ChangeTaskPlanCreate
        | RpcMethod::ChangeTaskPlanUpdate
        | RpcMethod::IncidentPlanUpdate => {
            crate::change_write::handle_change_plan(id, &request.method, &request.params, state)
                .await
        }
        RpcMethod::IncidentBulkPlanUpdate => {
            crate::incident_bulk_write::handle_plan(id, &request.params, state).await
        }
        RpcMethod::ChangeRequestApplyCreate
        | RpcMethod::ChangeRequestApplyUpdate
        | RpcMethod::ChangeTaskApplyCreate
        | RpcMethod::ChangeTaskApplyUpdate
        | RpcMethod::IncidentApplyUpdate => {
            crate::change_write::handle_change_apply(id, &request.method, &request.params, state)
                .await
        }
        RpcMethod::IncidentBulkApplyUpdate => {
            crate::incident_bulk_write::handle_apply(id, &request.params, state).await
        }
        RpcMethod::ResourcePlanPlanCreate | RpcMethod::ResourcePlanPlanUpdate => {
            crate::resource_plan_write::handle_resource_plan_plan(
                id,
                &request.method,
                &request.params,
                state,
            )
            .await
        }
        RpcMethod::ResourcePlanApplyCreate | RpcMethod::ResourcePlanApplyUpdate => {
            crate::resource_plan_write::handle_resource_plan_apply(
                id,
                &request.method,
                &request.params,
                state,
            )
            .await
        }
        RpcMethod::StoryPlanCreate
        | RpcMethod::StoryPlanUpdate
        | RpcMethod::StoryTaskPlanCreate
        | RpcMethod::StoryTaskPlanUpdate => {
            crate::story_write::handle_story_plan(id, &request.method, &request.params, state).await
        }
        RpcMethod::StoryApplyCreate
        | RpcMethod::StoryApplyUpdate
        | RpcMethod::StoryTaskApplyCreate
        | RpcMethod::StoryTaskApplyUpdate => {
            crate::story_write::handle_story_apply(id, &request.method, &request.params, state)
                .await
        }
        RpcMethod::TimecardList => {
            crate::timecard_write::handle_timecard_list(id, &request.params, state).await
        }
        RpcMethod::TimecardSetHours => {
            crate::timecard_write::handle_timecard_set_hours(id, &request.params, state).await
        }
        RpcMethod::TimecardPlanSetHours => {
            crate::timecard_write::handle_timecard_plan_set_hours(id, &request.params, state).await
        }
        RpcMethod::TimecardApplySetHours => {
            crate::timecard_write::handle_timecard_apply_set_hours(id, &request.params, state).await
        }
        RpcMethod::Unknown => JsonRpcResponse::error(id, -32601, "method not found", None),
        _ => unreachable!("method routed to the wrong RPC feature handler"),
    }
}
