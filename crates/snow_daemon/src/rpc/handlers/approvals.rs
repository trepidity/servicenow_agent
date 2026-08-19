use super::*;

pub(in crate::rpc) async fn handle_approval_approve(
    id: Option<Value>,
    params: &Value,
    state: &DaemonState,
    transport: &DaemonTransport<'_>,
) -> JsonRpcResponse {
    if !approval_tool_enabled(state, "approval_approve") {
        return approval_policy_denied(id, "approval_approve");
    }

    let target = match extract_approval_action_target(params) {
        Ok(target) => target,
        Err(err) => return invalid_params(id, err),
    };
    let result = match target {
        ApprovalActionTarget::Number(number) => state.core.approve(&number, None).await,
        ApprovalActionTarget::ApprovalSysId(approval_sys_id) => {
            state.core.approve_approval(&approval_sys_id, None).await
        }
    };
    match result {
        Ok(Some(record)) => daemon_record_response(id, transport, &record),
        Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
        Err(err) => internal_error(id, err),
    }
}

pub(in crate::rpc) async fn handle_approval_reject(
    id: Option<Value>,
    params: &Value,
    state: &DaemonState,
    transport: &DaemonTransport<'_>,
) -> JsonRpcResponse {
    if !approval_tool_enabled(state, "approval_reject") {
        return approval_policy_denied(id, "approval_reject");
    }

    let target = match extract_approval_action_target(params) {
        Ok(target) => target,
        Err(err) => return invalid_params(id, err),
    };
    let reason = match extract_string(params, "reason") {
        Ok(reason) => reason,
        Err(err) => return invalid_params(id, err),
    };
    let result = match target {
        ApprovalActionTarget::Number(number) => state.core.reject(&number, &reason).await,
        ApprovalActionTarget::ApprovalSysId(approval_sys_id) => {
            state.core.reject_approval(&approval_sys_id, &reason).await
        }
    };
    match result {
        Ok(Some(record)) => daemon_record_response(id, transport, &record),
        Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
        Err(err) => internal_error(id, err),
    }
}

pub(in crate::rpc) fn approval_tool_enabled(state: &DaemonState, tool: &str) -> bool {
    state
        .mcp_config
        .policy
        .tool_enabled_in_environment(tool, &state.mcp_config.environment.label)
}

pub(in crate::rpc) fn approval_policy_denied(id: Option<Value>, tool: &str) -> JsonRpcResponse {
    JsonRpcResponse::error(
        id,
        -32040,
        "policy denied",
        Some(json!({
            "details": "approval action tool is disabled by current MCP policy",
            "tool": tool,
        })),
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::rpc) enum ApprovalActionTarget {
    Number(String),
    ApprovalSysId(String),
}

pub(in crate::rpc) fn extract_approval_action_target(
    params: &Value,
) -> Result<ApprovalActionTarget> {
    let Value::Object(map) = params else {
        return Err(anyhow!("expected object params"));
    };
    let number = map.get("number").and_then(Value::as_str);
    let approval_sys_id = map.get("approval_sys_id").and_then(Value::as_str);

    match (number, approval_sys_id) {
        (Some(number), None) => Ok(ApprovalActionTarget::Number(number.to_owned())),
        (None, Some(approval_sys_id)) => Ok(ApprovalActionTarget::ApprovalSysId(
            snow_core::normalize_record_lookup_sys_id(approval_sys_id)?,
        )),
        (Some(_), Some(_)) => Err(anyhow!(
            "provide either `number` or `approval_sys_id`, not both"
        )),
        (None, None) => Err(anyhow!(
            "missing required lookup: provide either `number` or `approval_sys_id`"
        )),
    }
}

pub(in crate::rpc) async fn dispatch_approvals(
    method: RpcMethod,
    id: Option<Value>,
    request: &JsonRpcRequest,
    state: &Arc<DaemonState>,
    transport: &DaemonTransport<'_>,
) -> JsonRpcResponse {
    match method {
        RpcMethod::GetApproval => match extract_number(&request.params) {
            Ok(number) => match state.core.get_approval(&number).await {
                Ok(Some(approval)) => match transport.approval(&approval) {
                    Ok(approval_dto) => JsonRpcResponse::ok(
                        id,
                        json!({
                            "approval": approval_dto,
                            "markdown": render_approval_record(&approval),
                        }),
                    ),
                    Err(err) => internal_error(id, err),
                },
                Ok(None) => JsonRpcResponse::error(id, -32004, "approval not found", None),
                Err(err) => internal_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::Approve => handle_approval_approve(id, &request.params, state, transport).await,
        RpcMethod::ApprovalApprove => {
            handle_approval_approve(id, &request.params, state, transport).await
        }
        RpcMethod::Reject => handle_approval_reject(id, &request.params, state, transport).await,
        RpcMethod::ApprovalReject => {
            handle_approval_reject(id, &request.params, state, transport).await
        }
        _ => unreachable!("method routed to the wrong RPC feature handler"),
    }
}
