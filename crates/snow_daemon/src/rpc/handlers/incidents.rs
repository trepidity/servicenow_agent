use super::*;

/// Closed parameter object for the fixed-table `incident_fields` operation.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IncidentFieldsParams {}

/// Deserializes and pre-validates group-scoped Incident page arguments.
///
/// Validation runs here as well as inside the core so a malformed group
/// `sys_id`, cursor, or page size surfaces as `-32602 invalid params` rather
/// than an internal error.
pub(in crate::rpc) fn extract_incident_list_by_assignment_group_params(
    params: &Value,
) -> Result<snow_core::IncidentAssignmentGroupListInput> {
    let input: snow_core::IncidentAssignmentGroupListInput =
        serde_json::from_value(params.clone())?;
    snow_core::validate_incident_assignment_group_input(input.clone())?;
    Ok(input)
}

/// Maps a group-scoped Incident page failure onto the JSON-RPC error contract.
///
/// An unresolved state carries the live choice list through as structured
/// `data` so an agent can correct its selector without a second round trip;
/// anything that is not a caller-argument problem stays an internal error.
pub(in crate::rpc) fn incident_group_list_error_response(
    id: Option<Value>,
    err: anyhow::Error,
) -> JsonRpcResponse {
    match err.downcast_ref::<snow_core::IncidentAssignmentGroupListError>() {
        Some(snow_core::IncidentAssignmentGroupListError::InvalidParams(_)) => {
            invalid_params(id, err)
        }
        Some(snow_core::IncidentAssignmentGroupListError::UnresolvedState {
            requested,
            ambiguous,
            choices,
        }) => JsonRpcResponse::error(
            id,
            -32602,
            "invalid params",
            Some(json!({
                "details": err.to_string(),
                "field": "state",
                "requested": requested,
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

pub(in crate::rpc) fn incident_read_error_response(
    id: Option<Value>,
    error: snow_core::IncidentReadError,
) -> JsonRpcResponse {
    use snow_core::IncidentReadError;
    match error {
        IncidentReadError::InvalidParams(details) => JsonRpcResponse::error(
            id,
            -32602,
            "invalid params",
            Some(json!({ "code": "INVALID_PARAMS", "details": details })),
        ),
        IncidentReadError::StateUnresolved {
            requested,
            ambiguous,
            unavailable,
            choices,
        } => JsonRpcResponse::error(
            id,
            -32602,
            "incident state unresolved",
            Some(json!({
                "code": "INCIDENT_STATE_UNRESOLVED",
                "requested": requested,
                "table": "incident",
                "field": "state",
                "ambiguous": ambiguous,
                "unavailable": unavailable,
                "choices": choices.into_iter().map(|choice| json!({
                    "value": choice.value,
                    "label": choice.label
                })).collect::<Vec<_>>()
            })),
        ),
        IncidentReadError::NotFound => JsonRpcResponse::error(
            id,
            -32004,
            "incident not found",
            Some(json!({ "code": "INCIDENT_NOT_FOUND" })),
        ),
        IncidentReadError::NumberAmbiguous => JsonRpcResponse::error(
            id,
            -32005,
            "incident number is ambiguous",
            Some(json!({ "code": "INCIDENT_NUMBER_AMBIGUOUS" })),
        ),
        IncidentReadError::LookupUnavailable => JsonRpcResponse::error(
            id,
            -32007,
            "incident lookup is unavailable",
            Some(json!({ "code": "INCIDENT_LOOKUP_UNAVAILABLE" })),
        ),
        IncidentReadError::AclDenied => JsonRpcResponse::error(
            id,
            -32003,
            "access denied",
            Some(json!({ "code": "ACL_DENIED" })),
        ),
        IncidentReadError::ServiceNowUnavailable => JsonRpcResponse::error(
            id,
            -32001,
            "ServiceNow unavailable",
            Some(json!({ "code": "SERVICENOW_UNAVAILABLE" })),
        ),
        IncidentReadError::ServiceNowError => JsonRpcResponse::error(
            id,
            -32000,
            "ServiceNow error",
            Some(json!({ "code": "SERVICENOW_ERROR" })),
        ),
    }
}

pub(in crate::rpc) async fn dispatch_incidents(
    method: RpcMethod,
    id: Option<Value>,
    request: &JsonRpcRequest,
    state: &Arc<DaemonState>,
    _transport: &DaemonTransport<'_>,
) -> JsonRpcResponse {
    match method {
        RpcMethod::IncidentGet => {
            match serde_json::from_value::<snow_core::IncidentGetInput>(request.params.clone()) {
                Ok(params) => match state.core.incident_get(params).await {
                    Ok(envelope) => JsonRpcResponse::ok(id, json!(envelope)),
                    Err(error) => incident_read_error_response(id, error),
                },
                Err(error) => JsonRpcResponse::error(
                    id,
                    -32602,
                    "invalid params",
                    Some(json!({ "code": "INVALID_PARAMS", "details": error.to_string() })),
                ),
            }
        }
        RpcMethod::IncidentQuery => {
            match serde_json::from_value::<snow_core::IncidentQueryInput>(request.params.clone()) {
                Ok(params) => match state.core.incident_query(params).await {
                    Ok(envelope) => JsonRpcResponse::ok(id, json!(envelope)),
                    Err(error) => incident_read_error_response(id, error),
                },
                Err(error) => JsonRpcResponse::error(
                    id,
                    -32602,
                    "invalid params",
                    Some(json!({ "code": "INVALID_PARAMS", "details": error.to_string() })),
                ),
            }
        }
        RpcMethod::IncidentListByAssignmentGroup => {
            match extract_incident_list_by_assignment_group_params(&request.params) {
                Ok(params) => match state.core.incident_list_by_assignment_group(params).await {
                    Ok(page) => JsonRpcResponse::ok(
                        id,
                        json!({
                            "records": page.records,
                            "next_cursor": page.next_cursor,
                            "complete": page.complete,
                            "limit": page.limit,
                            "rows_inspected": page.rows_inspected,
                            "state": page.state,
                        }),
                    ),
                    Err(err) => incident_group_list_error_response(id, err),
                },
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::IncidentAssignmentGroups => {
            match state.core.incident_assignment_groups().await {
                Ok(groups) => JsonRpcResponse::ok(id, json!({"groups": groups})),
                Err(err) => internal_error(id, err),
            }
        }
        // The envelope is serialized whole rather than reassembled field by
        // field, so every transport emits the exact source/completeness
        // contract and no path can quietly drop or rename part of it.
        RpcMethod::IncidentFields => {
            match serde_json::from_value::<IncidentFieldsParams>(request.params.clone()) {
                Ok(_) => match state.core.incident_fields().await {
                    Ok(envelope) => JsonRpcResponse::ok(id, json!(envelope)),
                    Err(err) => internal_error(id, err),
                },
                Err(err) => invalid_params(id, err),
            }
        }
        RpcMethod::IncidentAssignmentGroupQueue => {
            match serde_json::from_value::<snow_core::IncidentAssignmentGroupQueueInput>(
                request.params.clone(),
            ) {
                Ok(params) => match state.core.incident_assignment_group_queue(params).await {
                    Ok(page) => JsonRpcResponse::ok(id, json!(page)),
                    Err(err)
                        if err
                            .downcast_ref::<snow_core::IncidentAssignmentGroupOperationsError>()
                            .is_some() =>
                    {
                        invalid_params(id, err)
                    }
                    Err(err) => internal_error(id, err),
                },
                Err(err) => invalid_params(id, err),
            }
        }
        _ => unreachable!("method routed to the wrong RPC feature handler"),
    }
}
