use super::*;

#[derive(Debug, Deserialize)]
pub(in crate::rpc) struct SetStateParams {
    pub(in crate::rpc) number: String,
    pub(in crate::rpc) state: String,
}

#[derive(Debug, Deserialize)]
pub(in crate::rpc) struct FieldChoicesParams {
    pub(in crate::rpc) table: String,
    pub(in crate::rpc) field: String,
}

#[derive(Debug, Deserialize)]
pub(in crate::rpc) struct AttachmentUploadParams {
    pub(in crate::rpc) number: String,
    pub(in crate::rpc) path: PathBuf,
    #[serde(default)]
    pub(in crate::rpc) file_name: Option<String>,
    #[serde(default)]
    pub(in crate::rpc) content_type: Option<String>,
}

pub(in crate::rpc) fn daemon_record_response(
    id: Option<Value>,
    transport: &DaemonTransport<'_>,
    record: &SnowRecord,
) -> JsonRpcResponse {
    match transport.record(record) {
        Ok(record) => JsonRpcResponse::ok(id, json!({ "record": record })),
        Err(err) => internal_error(id, err),
    }
}

pub(in crate::rpc) fn daemon_live_record_response(
    id: Option<Value>,
    transport: &DaemonTransport<'_>,
    record: &SnowRecord,
) -> JsonRpcResponse {
    match transport.live_record(record) {
        Ok(record) => JsonRpcResponse::ok(id, json!({ "record": record })),
        Err(err) => internal_error(id, err),
    }
}

pub(in crate::rpc) async fn daemon_record_response_with_private_task_context(
    id: Option<Value>,
    transport: &DaemonTransport<'_>,
    record: &SnowRecord,
) -> JsonRpcResponse {
    match transport.record_with_private_task_context(record).await {
        Ok(record) => JsonRpcResponse::ok(id, json!({ "record": record })),
        Err(err) => internal_error(id, err),
    }
}

pub(in crate::rpc) fn extract_number(params: &Value) -> Result<String> {
    match params {
        Value::Object(map) => map
            .get("number")
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| anyhow!("missing required field `number`")),
        _ => Err(anyhow!("expected object params")),
    }
}

pub(crate) fn extract_record_lookup(params: &Value) -> Result<RecordLookup> {
    let Value::Object(map) = params else {
        return Err(anyhow!("expected object params"));
    };

    let number = map.get("number").and_then(Value::as_str);
    let table = map.get("table").and_then(Value::as_str);
    let sys_id = map.get("sys_id").and_then(Value::as_str);

    match (number, table, sys_id) {
        (Some(number), None, None) => Ok(RecordLookup::Number(number.to_owned())),
        (None, Some(table), Some(sys_id)) => Ok(RecordLookup::TableSysId {
            table: snow_core::normalize_record_lookup_table(table)?,
            sys_id: snow_core::normalize_record_lookup_sys_id(sys_id)?,
        }),
        (Some(_), Some(_), _) | (Some(_), _, Some(_)) => Err(anyhow!(
            "provide either `number` or `table` + `sys_id`, not both"
        )),
        (None, None, Some(_)) => Err(anyhow!(
            "missing required lookup: provide either `number` or `table` + `sys_id`"
        )),
        (None, Some(_), None) => Err(anyhow!("missing required field `sys_id`")),
        (None, None, None) => Err(anyhow!(
            "missing required lookup: provide either `number` or `table` + `sys_id`"
        )),
    }
}

pub(in crate::rpc) fn extract_string(params: &Value, field: &str) -> Result<String> {
    match params {
        Value::Object(map) => map
            .get(field)
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| anyhow!("missing required field `{field}`")),
        _ => Err(anyhow!("expected object params")),
    }
}

pub(in crate::rpc) fn extract_task_sla_parent_refs(
    params: &Value,
) -> Result<Vec<TaskSlaParentRef>> {
    let params: TaskSlaStatusForTasksParams = serde_json::from_value(params.clone())?;
    Ok(params.parents)
}

#[derive(Debug, Clone, Deserialize, Default)]
pub(in crate::rpc) struct SearchRecordsParams {
    pub(in crate::rpc) query: String,
    #[serde(default)]
    pub(in crate::rpc) scope: Option<String>,
    #[serde(default)]
    pub(in crate::rpc) limit: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
pub(in crate::rpc) struct TaskSlaStatusForTasksParams {
    pub(in crate::rpc) parents: Vec<TaskSlaParentRef>,
}

#[derive(Debug, Clone, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub(in crate::rpc) struct ListRecordsParams {
    #[serde(default)]
    pub(in crate::rpc) resource_type: Option<String>,
    #[serde(default)]
    pub(in crate::rpc) parent_number: Option<String>,
    #[serde(default)]
    pub(in crate::rpc) assigned_to: Option<String>,
    #[serde(default)]
    pub(in crate::rpc) limit: Option<usize>,
}

pub(in crate::rpc) fn extract_search_records_params(params: &Value) -> Result<SearchRecordsParams> {
    let params: SearchRecordsParams = serde_json::from_value(params.clone())?;
    if params.query.trim().is_empty() {
        return Err(anyhow!("missing required field `query`"));
    }
    Ok(params)
}

pub(in crate::rpc) fn extract_user_lookup_params(params: &Value) -> Result<snow_core::UserLookup> {
    let params: snow_core::UserLookup = serde_json::from_value(params.clone())?;
    params.validate_selector()?;
    Ok(params)
}

pub(in crate::rpc) fn extract_user_search_params(params: &Value) -> Result<snow_core::UserSearch> {
    let params: snow_core::UserSearch = serde_json::from_value(params.clone())?;
    params.validate()?;
    Ok(params)
}

pub(in crate::rpc) fn extract_resource_plan_list_params(
    params: &Value,
) -> Result<snow_core::ResourcePlanListInput> {
    let input: snow_core::ResourcePlanListInput = serde_json::from_value(params.clone())?;
    snow_core::validate_list_input(input.clone())?;
    Ok(input)
}

pub(in crate::rpc) fn record_query_error_response(
    id: Option<Value>,
    err: anyhow::Error,
) -> JsonRpcResponse {
    match err.downcast_ref::<snow_core::RecordQueryError>() {
        Some(snow_core::RecordQueryError::InvalidParams(_)) => invalid_params(id, err),
        Some(snow_core::RecordQueryError::UnresolvedState {
            requested,
            table,
            field,
            ambiguous,
            choices,
        }) => JsonRpcResponse::error(
            id,
            -32602,
            "invalid params",
            Some(json!({
                "details": err.to_string(),
                "requested": requested,
                "table": table,
                "field": field,
                "ambiguous": ambiguous,
                "choices": choices
                    .iter()
                    .map(|choice| json!({ "value": choice.value, "label": choice.label }))
                    .collect::<Vec<_>>(),
            })),
        ),
        None => internal_error(id, err),
    }
}

pub(in crate::rpc) fn extract_list_records_params(params: &Value) -> Result<ListRecordsParams> {
    Ok(serde_json::from_value(params.clone())?)
}

pub(in crate::rpc) fn parse_resource_type(resource_type: &str) -> Result<ResourceType> {
    let normalized = resource_type.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "task" => Ok(ResourceType::Task),
        "incident" => Ok(ResourceType::Incident),
        "change" | "change_request" => Ok(ResourceType::Change),
        "change_task" => Ok(ResourceType::ChangeTask),
        "request" | "sc_req_item" | "request_item" => Ok(ResourceType::Request),
        "request_task" | "sc_task" => Ok(ResourceType::RequestTask),
        "project" | "pm_project" => Ok(ResourceType::Project),
        "demand" | "dmn_demand" => Ok(ResourceType::Demand),
        "demand_task" | "dmn_demand_task" | "dmntsk" => Ok(ResourceType::DemandTask),
        "resource_plan" | "resourceplan" | "rpln" => Ok(ResourceType::ResourcePlan),
        "story" | "rm_story" => Ok(ResourceType::Story),
        "scrum_task" | "rm_scrum_task" => Ok(ResourceType::ScrumTask),
        "knowledge" | "kb_knowledge" => Ok(ResourceType::Knowledge),
        "approval" | "sysapproval_approver" => Ok(ResourceType::Approval),
        "business_application" | "business_app" | "cmdb_ci_business_app" => {
            Ok(ResourceType::BusinessApplication)
        }
        "server"
        | "servers"
        | "cmdb_ci_server"
        | "cmdb_ci_linux_server"
        | "cmdb_ci_win_server"
        | "linux_server"
        | "windows_server" => Ok(ResourceType::Server),
        "private_task" | "vtb_task" => Ok(ResourceType::PrivateTask),
        _ => Err(anyhow!("unsupported resource_type `{resource_type}`")),
    }
}

pub(in crate::rpc) async fn get_record_cached_or_fresh(
    core: &SnowCore,
    number: &str,
) -> Result<Option<SnowRecord>> {
    match core.get_record(number).await? {
        Some(record) => Ok(Some(record)),
        None => core.get_record_fresh(number).await,
    }
}

pub(crate) async fn get_record_by_lookup_cached_or_fresh(
    core: &SnowCore,
    lookup: RecordLookup,
) -> Result<Option<SnowRecord>> {
    match lookup {
        RecordLookup::Number(number) => get_record_cached_or_fresh(core, &number).await,
        RecordLookup::TableSysId { table, sys_id } => {
            core.get_record_by_table_sys_id_fresh(&table, &sys_id).await
        }
    }
}

pub(in crate::rpc) fn wrap_records_response(
    id: Option<Value>,
    transport: &DaemonTransport<'_>,
    records: Vec<snow_core::SnowRecord>,
) -> JsonRpcResponse {
    let mut record_dtos = Vec::with_capacity(records.len());
    for record in records {
        match transport.record(&record) {
            Ok(record) => record_dtos.push(record),
            Err(err) => return internal_error(id, err),
        }
    }
    JsonRpcResponse::ok(id, json!({ "records": record_dtos }))
}

pub(in crate::rpc) fn wrap_list_my_approvals_response(
    id: Option<Value>,
    transport: &DaemonTransport<'_>,
    response: snow_core::ListMyApprovalsResponse,
) -> JsonRpcResponse {
    let mut approval_dtos = Vec::with_capacity(response.records.len());
    for approval in response.records {
        match transport.approval(&approval) {
            Ok(approval) => approval_dtos.push(approval),
            Err(err) => return internal_error(id, err),
        }
    }
    JsonRpcResponse::ok(
        id,
        json!({
            "records": approval_dtos,
            "query_summary": response.query_summary,
        }),
    )
}

/// Map core lookup failures that are caller mistakes (unknown prefix) to a
/// structured JSON-RPC error instead of `internal_error` (-32000).
pub(in crate::rpc) fn map_record_lookup_error(
    id: Option<Value>,
    err: impl ToString,
) -> JsonRpcResponse {
    let details = err.to_string();
    if is_unknown_prefix_error_message(&details) {
        return JsonRpcResponse::error(
            id,
            UNKNOWN_PREFIX_CODE,
            "unknown record prefix",
            Some(json!({ "details": details })),
        );
    }
    internal_error(id, details)
}

pub(in crate::rpc) fn is_unknown_prefix_error_message(message: &str) -> bool {
    // Prefer a typed snow_core error when one exists; until then match the
    // stable substring from CoreContext::get_record_fresh_with_source.
    message.contains("unknown ServiceNow prefix")
}

pub(in crate::rpc) async fn dispatch_records(
    method: RpcMethod,
    id: Option<Value>,
    request: &JsonRpcRequest,
    state: &Arc<DaemonState>,
    transport: &DaemonTransport<'_>,
) -> JsonRpcResponse {
    match method {
        RpcMethod::GetRecord => match extract_record_lookup(&request.params) {
            Ok(RecordLookup::Number(number)) => {
                match state
                    .core
                    .get_record_live_without_persistence(&number)
                    .await
                {
                    Ok(Some(record)) => daemon_live_record_response(id, transport, &record),
                    Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
                    Err(err) => map_record_lookup_error(id, err),
                }
            }
            Ok(RecordLookup::TableSysId { table, sys_id }) => match state
                .core
                .get_record_by_table_sys_id_live_without_persistence(&table, &sys_id)
                .await
            {
                Ok(Some(record)) => daemon_live_record_response(id, transport, &record),
                Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
                Err(err) => map_record_lookup_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::GetRecordFresh => match extract_record_lookup(&request.params) {
            Ok(RecordLookup::Number(number)) => match state.core.get_record_fresh(&number).await {
                Ok(Some(record)) => {
                    daemon_record_response_with_private_task_context(id, transport, &record).await
                }
                Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
                Err(err) => map_record_lookup_error(id, err),
            },
            Ok(RecordLookup::TableSysId { table, sys_id }) => match state
                .core
                .get_record_by_table_sys_id_fresh(&table, &sys_id)
                .await
            {
                Ok(Some(record)) => {
                    daemon_record_response_with_private_task_context(id, transport, &record).await
                }
                Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
                Err(err) => map_record_lookup_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::TaskSlaStatus => match extract_number(&request.params) {
            Ok(number) => match state.core.task_sla_status_for_number(&number).await {
                Ok(status) => JsonRpcResponse::ok(id, json!({ "status": status })),
                Err(err) => internal_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::TaskSlaStatusForTasks => match extract_task_sla_parent_refs(&request.params) {
            Ok(parents) => match state.core.task_sla_status_for_tasks(&parents).await {
                Ok(statuses) => JsonRpcResponse::ok(id, json!({ "statuses": statuses })),
                Err(err) => internal_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::SearchRecords => match extract_search_records_params(&request.params) {
            Ok(params) => {
                let exact_live = snow_core::query::is_exact_record_number(&params.query);
                match state
                    .core
                    .search_enriched(&params.query, parse_search_scope(params.scope.as_deref()))
                    .await
                {
                    Ok(results) => {
                        let mut search_results = Vec::new();
                        for result in results.into_iter().take(params.limit.unwrap_or(20)) {
                            if exact_live {
                                search_results.push(transport.live_search_result(&result));
                            } else {
                                match transport.search_result(&result).await {
                                    Ok(result) => search_results.push(result),
                                    Err(err) => return internal_error(id, err),
                                }
                            }
                        }
                        JsonRpcResponse::ok(id, json!({ "results": search_results }))
                    }
                    Err(err) => internal_error(id, err),
                }
            }
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::UserLookup => match extract_user_lookup_params(&request.params) {
            Ok(params) => match state.core.lookup_user(params).await {
                Ok(Some(result)) => JsonRpcResponse::ok(id, json!(result)),
                Ok(None) => JsonRpcResponse::error(id, -32004, "user not found", None),
                Err(err) => internal_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::UserSearch => match extract_user_search_params(&request.params) {
            Ok(params) => match state.core.search_users(params).await {
                Ok(users) => JsonRpcResponse::ok(id, json!({ "users": users })),
                Err(err) => internal_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::ResourcePlanList => match extract_resource_plan_list_params(&request.params) {
            Ok(params) => match state.core.resource_plan_list(params).await {
                Ok(mut response) => {
                    for record in &mut response.records {
                        if let Err(err) = transport.resource_plan_record(record) {
                            return internal_error(id, err);
                        }
                    }
                    JsonRpcResponse::ok(id, json!(response))
                }
                Err(err) => internal_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        // Group-scoped Incident page. Arguments and successful results map 1:1
        // onto the core contract so direct MCP and daemon-backed MCP consumers
        // receive the same record/page shape. The daemon adds only the
        // structured invalid-parameter mapping, never filtering or transport
        // enrichment.
        // Authority: docs/spec-incident-list-by-assignment-group.md#scope.
        RpcMethod::GetChildren => match extract_number(&request.params) {
            Ok(number) => match state.core.get_children(&number).await {
                Ok(records) => {
                    let mut record_dtos = Vec::with_capacity(records.len());
                    for record in records {
                        match transport.record(&record) {
                            Ok(record) => record_dtos.push(record),
                            Err(err) => return internal_error(id, err),
                        }
                    }
                    JsonRpcResponse::ok(id, json!({ "records": record_dtos, "parent": number }))
                }
                Err(err) => internal_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        // Narrow live CTASK child read for Change setup. Unlike get_children,
        // this never reads the local vault and reports page completeness.
        RpcMethod::ChangeRequestListTasks => {
            match serde_json::from_value::<snow_core::ChangeRequestTaskListInput>(
                request.params.clone(),
            ) {
                Ok(input) => match snow_core::validate_change_request_task_list(input.clone()) {
                    Ok(_) => match state.core.change_request_list_tasks(input).await {
                        Ok(page) => {
                            let completeness = if page.complete {
                                json!({ "kind": "complete" })
                            } else {
                                json!({ "kind": "partial", "reason": "page_limit_reached" })
                            };
                            JsonRpcResponse::ok(
                                id,
                                json!({
                                    "operation": "change_request_list_tasks",
                                    "source": { "kind": "live" },
                                    "completeness": completeness,
                                    "data": page,
                                }),
                            )
                        }
                        Err(err) => internal_error(id, err),
                    },
                    Err(err) => invalid_params(id, err),
                },
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::GetWorkNotes => match extract_record_lookup(&request.params) {
            Ok(lookup) => {
                match get_record_by_lookup_cached_or_fresh(state.core.as_ref(), lookup).await {
                    Ok(Some(record)) => match transport.record(&record) {
                        Ok(record) => {
                            JsonRpcResponse::ok(id, json!({ "work_notes": record.work_notes }))
                        }
                        Err(err) => internal_error(id, err),
                    },
                    Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
                    Err(err) => map_record_lookup_error(id, err),
                }
            }
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::ListRecords => match extract_list_records_params(&request.params) {
            Ok(params) => {
                let mut query = ListQuery::new();
                if let Some(resource_type) = params.resource_type.as_deref() {
                    match parse_resource_type(resource_type) {
                        Ok(resource_type) => query = query.resource_type(resource_type),
                        Err(err) => return invalid_params(id, err),
                    }
                }
                if let Some(assigned_to) = params.assigned_to {
                    query = query.assigned_to(assigned_to);
                }
                if let Some(limit) = params.limit {
                    query = query.limit(limit);
                }
                if let Some(parent_number) = params.parent_number {
                    match state.core.get_record(&parent_number).await {
                        Ok(Some(parent)) => {
                            query = query.parent_sys_id(parent.sys_id);
                        }
                        Ok(None) => {
                            return JsonRpcResponse::error(
                                id,
                                -32004,
                                "parent record not found",
                                None,
                            );
                        }
                        Err(err) => return internal_error(id, err),
                    }
                }

                match state.core.list_records_query(query).await {
                    Ok(records) => {
                        let mut record_dtos = Vec::with_capacity(records.len());
                        for record in records {
                            match transport.record(&record) {
                                Ok(record) => record_dtos.push(record),
                                Err(err) => return internal_error(id, err),
                            }
                        }
                        JsonRpcResponse::ok(id, json!({ "records": record_dtos }))
                    }
                    Err(err) => internal_error(id, err),
                }
            }
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::RecordQuery => {
            match serde_json::from_value::<snow_core::RecordQueryInput>(request.params.clone()) {
                Ok(input) => match snow_core::validate_record_query(input.clone()) {
                    Ok(_) => match state.core.record_query(input).await {
                        Ok(page) => JsonRpcResponse::ok(id, json!(page)),
                        Err(err) => record_query_error_response(id, err),
                    },
                    Err(err) => invalid_params(id, err),
                },
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::MyTasks => match state.core.my_tasks().await {
            Ok(records) => wrap_records_response(id, transport, records),
            Err(err) => internal_error(id, err),
        },
        RpcMethod::MyTasksFresh => match state.core.my_tasks_fresh().await {
            Ok(records) => wrap_records_response(id, transport, records),
            Err(err) => internal_error(id, err),
        },
        RpcMethod::ListMyTasks => match state.core.my_tasks().await {
            Ok(records) if !records.is_empty() => wrap_records_response(id, transport, records),
            Ok(_) | Err(_) => match state.core.my_tasks_fresh().await {
                Ok(records) => wrap_records_response(id, transport, records),
                Err(err) => internal_error(id, err),
            },
        },
        RpcMethod::MyApprovals => match state.core.my_approvals_with_routing_fresh().await {
            Ok(response) => wrap_list_my_approvals_response(id, transport, response),
            Err(err) => internal_error(id, err),
        },
        RpcMethod::MyApprovalsFresh => match state.core.my_approvals_with_routing_fresh().await {
            Ok(response) => wrap_list_my_approvals_response(id, transport, response),
            Err(err) => internal_error(id, err),
        },
        RpcMethod::ListMyApprovals => match state.core.my_approvals_with_routing_fresh().await {
            Ok(response) => wrap_list_my_approvals_response(id, transport, response),
            Err(err) => internal_error(id, err),
        },
        RpcMethod::MyProjects => match state.core.my_projects().await {
            Ok(records) => wrap_records_response(id, transport, records),
            Err(err) => internal_error(id, err),
        },
        RpcMethod::MyProjectsFresh => match state.core.my_projects_fresh().await {
            Ok(records) => wrap_records_response(id, transport, records),
            Err(err) => internal_error(id, err),
        },
        RpcMethod::ListMyProjects => match state.core.my_projects().await {
            Ok(records) if !records.is_empty() => wrap_records_response(id, transport, records),
            Ok(_) | Err(_) => match state.core.my_projects_fresh().await {
                Ok(records) => wrap_records_response(id, transport, records),
                Err(err) => internal_error(id, err),
            },
        },
        RpcMethod::MyStoriesFresh | RpcMethod::ListMyStories => {
            match state.core.my_stories_fresh().await {
                Ok(records) => wrap_records_response(id, transport, records),
                Err(err) => internal_error(id, err),
            }
        }
        RpcMethod::MyIncidentsFresh | RpcMethod::ListMyIncidents => {
            match state.core.my_incidents_fresh().await {
                Ok(records) => wrap_records_response(id, transport, records),
                Err(err) => internal_error(id, err),
            }
        }
        RpcMethod::AddWorkNote => match (
            extract_number(&request.params),
            extract_string(&request.params, "text"),
        ) {
            (Ok(number), Ok(text)) => match state.core.add_work_note(&number, &text).await {
                Ok(Some(record)) => match transport.record(&record) {
                    Ok(record) => JsonRpcResponse::ok(id, json!({ "record": record })),
                    Err(err) => internal_error(id, err),
                },
                Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
                Err(err) => internal_error(id, err),
            },
            (Err(err), _) | (_, Err(err)) => invalid_params(id, err),
        },
        RpcMethod::AttachmentList => match extract_number(&request.params) {
            Ok(number) => match state.core.list_attachments(&number).await {
                Ok(Some(attachments)) => {
                    JsonRpcResponse::ok(id, json!({ "attachments": attachments }))
                }
                Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
                Err(err) => internal_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::AttachmentUpload => {
            match serde_json::from_value::<AttachmentUploadParams>(request.params.clone()) {
                Ok(params) => match state
                    .core
                    .upload_attachment_file(
                        &params.number,
                        &params.path,
                        params.file_name.as_deref(),
                        params.content_type.as_deref(),
                    )
                    .await
                {
                    Ok(Some(attachment)) => {
                        JsonRpcResponse::ok(id, json!({ "attachment": attachment }))
                    }
                    Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
                    Err(err) => internal_error(id, err),
                },
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::SetState => {
            match serde_json::from_value::<SetStateParams>(request.params.clone()) {
                Ok(params) => match state.core.set_state(&params.number, &params.state).await {
                    Ok(Some(record)) => match transport.record(&record) {
                        Ok(record) => JsonRpcResponse::ok(id, json!({ "record": record })),
                        Err(err) => internal_error(id, err),
                    },
                    Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
                    Err(err) => internal_error(id, err),
                },
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::FieldChoices => {
            match serde_json::from_value::<FieldChoicesParams>(request.params.clone()) {
                Ok(params) => match state.core.field_choices(&params.table, &params.field).await {
                    Ok(choices) => JsonRpcResponse::ok(id, json!({ "choices": choices })),
                    Err(err) => internal_error(id, err),
                },
                Err(err) => invalid_params(id, err),
            }
        }
        _ => unreachable!("method routed to the wrong RPC feature handler"),
    }
}
