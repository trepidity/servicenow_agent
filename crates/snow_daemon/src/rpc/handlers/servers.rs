use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::rpc) enum ServerLookup {
    SysId(String),
    Name(String),
    IpAddress(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(in crate::rpc) struct ServerGetRpcParams {
    pub(in crate::rpc) lookup: ServerLookup,
    pub(in crate::rpc) persist: bool,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub(in crate::rpc) struct ServerFieldsParams {}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub(in crate::rpc) struct ServerFieldSummary {
    pub(in crate::rpc) field: String,
    pub(in crate::rpc) observed_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(in crate::rpc) sample_value: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(in crate::rpc) sample_display_value: Option<String>,
}

pub(in crate::rpc) fn extract_server_lookup_params(params: &Value) -> Result<ServerLookup> {
    let Value::Object(map) = params else {
        return Err(anyhow!("expected object params"));
    };
    let sys_id = map.get("sys_id").and_then(Value::as_str).map(str::trim);
    let name = map.get("name").and_then(Value::as_str).map(str::trim);
    let ip_address = map.get("ip_address").and_then(Value::as_str).map(str::trim);
    match (
        sys_id.filter(|value| !value.is_empty()),
        name.filter(|value| !value.is_empty()),
        ip_address.filter(|value| !value.is_empty()),
    ) {
        (Some(sys_id), None, None) => Ok(ServerLookup::SysId(
            snow_core::normalize_record_lookup_sys_id(sys_id)?,
        )),
        (None, Some(name), None) => Ok(ServerLookup::Name(name.to_string())),
        (None, None, Some(ip_address)) => Ok(ServerLookup::IpAddress(ip_address.to_string())),
        (None, None, None) => Err(anyhow!(
            "missing required lookup: provide `sys_id`, `name`, or `ip_address`"
        )),
        _ => Err(anyhow!(
            "provide exactly one of `sys_id`, `name`, or `ip_address`"
        )),
    }
}

pub(in crate::rpc) fn extract_server_get_params(params: &Value) -> Result<ServerGetRpcParams> {
    let Value::Object(map) = params else {
        return Err(anyhow!("expected object params"));
    };
    let persist = match map.get("persist") {
        Some(Value::Bool(value)) => *value,
        Some(_) => return Err(anyhow!("`persist` must be a boolean")),
        None => true,
    };
    Ok(ServerGetRpcParams {
        lookup: extract_server_lookup_params(params)?,
        persist,
    })
}

pub(in crate::rpc) fn core_server_lookup(lookup: ServerLookup) -> Result<snow_core::ServerLookup> {
    Ok(match lookup {
        ServerLookup::SysId(sys_id) => snow_core::ServerLookup::sys_id(sys_id)?,
        ServerLookup::Name(name) => snow_core::ServerLookup::exact_name(name),
        ServerLookup::IpAddress(ip_address) => snow_core::ServerLookup::ip_address(ip_address),
    })
}

pub(in crate::rpc) fn extract_server_search_params(
    params: &Value,
) -> Result<snow_core::ServerSearchParams> {
    let params: snow_core::ServerSearchParams = serde_json::from_value(params.clone())?;
    params.validate()?;
    Ok(params)
}

pub(in crate::rpc) fn extract_server_query_params(
    params: &Value,
) -> Result<snow_core::ServerQuery> {
    let params: snow_core::ServerQuery = serde_json::from_value(params.clone())?;
    params.validate()?;
    Ok(params)
}

pub(in crate::rpc) fn extract_server_fields_params(params: &Value) -> Result<ServerFieldsParams> {
    Ok(serde_json::from_value(params.clone())?)
}

pub(in crate::rpc) async fn get_server_cached(
    core: &SnowCore,
    lookup: &ServerLookup,
) -> Result<Option<SnowRecord>> {
    let records = core
        .list_records_query(
            ListQuery::new()
                .resource_type(ResourceType::Server)
                .include_tombstoned(false),
        )
        .await?;
    Ok(records.into_iter().find(|record| match lookup {
        ServerLookup::SysId(sys_id) => record.sys_id.eq_ignore_ascii_case(sys_id),
        ServerLookup::Name(name) => server_name(record) == name.trim(),
        ServerLookup::IpAddress(ip_address) => server_field(record, "ip_address")
            .is_some_and(|value| value.eq_ignore_ascii_case(ip_address.trim())),
    }))
}

pub(in crate::rpc) async fn server_fields(
    core: &SnowCore,
    _params: ServerFieldsParams,
) -> Result<Vec<ServerFieldSummary>> {
    let records = core
        .list_records_query(ListQuery::new().resource_type(ResourceType::Server))
        .await?;
    let mut fields = std::collections::BTreeMap::<String, ServerFieldSummary>::new();
    for record in records {
        for (name, value) in record.fields {
            let entry = fields.entry(name.clone()).or_insert(ServerFieldSummary {
                field: name,
                observed_count: 0,
                sample_value: None,
                sample_display_value: None,
            });
            entry.observed_count += 1;
            if entry.sample_value.is_none() && !value.value.trim().is_empty() {
                entry.sample_value = Some(value.value);
            }
            if entry.sample_display_value.is_none() {
                entry.sample_display_value = value
                    .display_value
                    .filter(|display| !display.trim().is_empty());
            }
        }
    }
    Ok(fields.into_values().collect())
}

pub(in crate::rpc) fn server_name(record: &SnowRecord) -> String {
    server_field(record, "name")
        .or_else(|| {
            (!record.short_description.trim().is_empty()).then(|| record.short_description.clone())
        })
        .unwrap_or_else(|| record.sys_id.clone())
}

pub(in crate::rpc) fn server_field(record: &SnowRecord, field: &str) -> Option<String> {
    record.fields.get(field).and_then(|value| {
        value
            .display_value
            .clone()
            .or_else(|| Some(value.value.clone()))
            .filter(|value| !value.trim().is_empty())
    })
}

/// Map a structured [`snow_core::ServerGetError`] from the live `server_get`
/// fallback onto a distinct JSON-RPC error. A confirmed not-found is the only
/// `-32004`; ACL, network/timeout, and duplicate-CI disambiguation each get
/// their own code so callers never mistake a transient failure for a genuine
/// not-found.
pub(in crate::rpc) fn server_get_error_response(
    id: Option<Value>,
    err: snow_core::ServerGetError,
) -> JsonRpcResponse {
    use snow_core::ServerGetError;
    match err {
        ServerGetError::NotFound => JsonRpcResponse::error(id, -32004, "server not found", None),
        ServerGetError::AclRestricted(detail) => JsonRpcResponse::error(
            id,
            -32003,
            "server is ACL-restricted",
            Some(json!({ "details": detail })),
        ),
        ServerGetError::Network(detail) => JsonRpcResponse::error(
            id,
            -32001,
            "network error reaching ServiceNow",
            Some(json!({ "details": detail })),
        ),
        ServerGetError::Disambiguation { selector, matched } => JsonRpcResponse::error(
            id,
            -32005,
            "multiple servers matched selector",
            Some(json!({ "selector": selector, "matched": matched })),
        ),
        ServerGetError::Hydration(detail) => internal_error(id, detail),
        ServerGetError::Other(detail) => internal_error(id, detail),
    }
}

pub(in crate::rpc) async fn dispatch_servers(
    method: RpcMethod,
    id: Option<Value>,
    request: &JsonRpcRequest,
    state: &Arc<DaemonState>,
    transport: &DaemonTransport<'_>,
) -> JsonRpcResponse {
    match method {
        RpcMethod::ServerGet => match extract_server_get_params(&request.params) {
            Ok(params) => match get_server_cached(state.core.as_ref(), &params.lookup).await {
                // Cache hit: return the cached record without a live query.
                Ok(Some(record)) => match transport.server(&record) {
                    Ok(server) => {
                        let record_dto = server.record.clone();
                        JsonRpcResponse::ok(
                            id,
                            json!({
                                "server": server,
                                "record": record_dto,
                                "markdown": render_snow_record(&record),
                            }),
                        )
                    }
                    Err(err) => internal_error(id, err),
                },
                // Cache miss: fall through to the live exact fetch. On the
                // CLI/daemon path we persist the hit (read primitive contract);
                // a confirmed 404 is the only -32004, transient/ACL failures map
                // to distinct codes.
                Ok(None) => match core_server_lookup(params.lookup) {
                    Ok(core_lookup) => {
                        match state
                            .core
                            .get_server_live(core_lookup, params.persist)
                            .await
                        {
                            Ok(Some(server)) => match transport.server(&server.record) {
                                Ok(server_dto) => {
                                    let record_dto = server_dto.record.clone();
                                    JsonRpcResponse::ok(
                                        id,
                                        json!({
                                            "server": server_dto,
                                            "record": record_dto,
                                            "markdown": render_snow_record(&server.record),
                                        }),
                                    )
                                }
                                Err(err) => internal_error(id, err),
                            },
                            // get_server_live never returns Ok(None); NotFound is
                            // an Err variant. Treat the impossible case as 404.
                            Ok(None) => {
                                JsonRpcResponse::error(id, -32004, "server not found", None)
                            }
                            Err(err) => server_get_error_response(id, err),
                        }
                    }
                    Err(err) => invalid_params(id, err),
                },
                Err(err) => internal_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::ServerGetFresh => match extract_server_lookup_params(&request.params) {
            Ok(lookup) => match core_server_lookup(lookup) {
                Ok(lookup) => match state.core.get_server_fresh(lookup).await {
                    Ok(Some(server)) => match transport.server(&server.record) {
                        Ok(server_dto) => {
                            let record_dto = server_dto.record.clone();
                            JsonRpcResponse::ok(
                                id,
                                json!({
                                    "server": server_dto,
                                    "record": record_dto,
                                    "markdown": render_snow_record(&server.record),
                                }),
                            )
                        }
                        Err(err) => internal_error(id, err),
                    },
                    Ok(None) => JsonRpcResponse::error(id, -32004, "server not found", None),
                    Err(err) => internal_error(id, err),
                },
                Err(err) => invalid_params(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::ServerSearch => match extract_server_search_params(&request.params) {
            Ok(params) => match state.core.search_servers_live(params).await {
                Ok(servers) => {
                    let mut server_dtos = Vec::with_capacity(servers.len());
                    let mut record_dtos = Vec::with_capacity(servers.len());
                    for server in servers {
                        match transport.server(&server.record) {
                            Ok(server_dto) => {
                                record_dtos.push(server_dto.record.clone());
                                server_dtos.push(server_dto);
                            }
                            Err(err) => return internal_error(id, err),
                        }
                    }
                    JsonRpcResponse::ok(
                        id,
                        json!({
                            "servers": server_dtos,
                            "records": record_dtos,
                        }),
                    )
                }
                Err(err) => internal_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::ServerQuery => match extract_server_query_params(&request.params) {
            Ok(params) => match state.core.query_servers(params).await {
                Ok(records) => {
                    let mut servers = Vec::with_capacity(records.len());
                    for record in records {
                        match transport.server(&record) {
                            Ok(server) => servers.push(server),
                            Err(err) => return internal_error(id, err),
                        }
                    }
                    JsonRpcResponse::ok(id, json!({ "servers": servers }))
                }
                Err(err) => internal_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        RpcMethod::ServerFields => match extract_server_fields_params(&request.params) {
            Ok(params) => match server_fields(state.core.as_ref(), params).await {
                Ok(fields) => JsonRpcResponse::ok(id, json!({ "fields": fields })),
                Err(err) => internal_error(id, err),
            },
            Err(err) => invalid_params(id, err),
        },
        _ => unreachable!("method routed to the wrong RPC feature handler"),
    }
}
