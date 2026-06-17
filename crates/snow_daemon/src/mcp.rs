use std::future::Future;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use snow_core::{ResourceType, SearchScope, SnowCore, SnowRecord, query::filter::ListQuery};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::signal;

use crate::transport::{
    DaemonKnowledgeSemanticStatus, DaemonKnowledgeStatus, DaemonKnowledgeSyncOutcome,
    DaemonKnowledgeTagSummary, DaemonSemanticIndexSummary,
};
use crate::{
    DaemonState,
    rpc::{
        JsonRpcRequest, JsonRpcResponse, extract_kb_list_tags_params,
        extract_kb_semantic_search_filters, extract_kb_sync_params, extract_record_lookup,
        get_record_by_lookup_cached_or_fresh,
    },
    transport::DaemonTransport,
};
use snow_core::vault::markdown::{
    render_approval_record, render_knowledge_article, render_snow_record,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum McpTransport {
    Stdio,
    Sse,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpTool {
    pub name: String,
    pub description: String,
    pub input_schema: Value,
    pub output_schema: Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct McpResource {
    pub name: String,
    pub uri: String,
    pub description: String,
    pub mime_type: String,
}

#[derive(Clone)]
pub struct McpServer {
    state: Arc<DaemonState>,
    transport: McpTransport,
}

impl McpServer {
    pub fn new(state: Arc<DaemonState>, transport: McpTransport) -> Self {
        Self { state, transport }
    }

    pub async fn serve(self) -> Result<()> {
        match self.transport {
            McpTransport::Stdio => self.serve_stdio().await,
            McpTransport::Sse => self.serve_sse().await,
        }
    }

    pub async fn serve_stdio(self) -> Result<()> {
        self.serve_streams(
            BufReader::new(tokio::io::stdin()),
            tokio::io::stdout(),
            signal::ctrl_c(),
        )
        .await
    }

    pub async fn serve_sse(self) -> Result<()> {
        let _ = signal::ctrl_c().await;
        Ok(())
    }

    pub async fn serve_streams<R, W, F>(
        &self,
        mut reader: R,
        mut writer: W,
        shutdown: F,
    ) -> Result<()>
    where
        R: AsyncBufRead + Unpin,
        W: AsyncWrite + Unpin,
        F: Future<Output = Result<(), std::io::Error>>,
    {
        tokio::pin!(shutdown);
        let mut line = String::new();

        loop {
            tokio::select! {
                _ = &mut shutdown => break,
                read = reader.read_line(&mut line) => {
                    let read = read?;
                    if read == 0 {
                        break;
                    }
                    let request = serde_json::from_str::<JsonRpcRequest>(&line)
                        .map_err(|err| anyhow!(err))?;
                    let response = self.dispatch(request).await;
                    let payload = serde_json::to_vec(&response)?;
                    writer.write_all(&payload).await?;
                    writer.write_all(b"\n").await?;
                    writer.flush().await?;
                    line.clear();
                }
            }
        }

        Ok(())
    }

    async fn dispatch(&self, request: JsonRpcRequest) -> JsonRpcResponse {
        let id = request.id.clone();
        match request.method.as_str() {
            "initialize" => JsonRpcResponse::ok(
                id,
                json!({
                    "protocolVersion": "2024-11-05",
                    "serverInfo": { "name": "snow_daemon", "version": env!("CARGO_PKG_VERSION") },
                    "capabilities": {
                        "tools": { "listChanged": false },
                        "resources": { "subscribe": false, "listChanged": false }
                    }
                }),
            ),
            "tools/list" => JsonRpcResponse::ok(id, json!({ "tools": Self::tools_catalog() })),
            "resources/list" => {
                JsonRpcResponse::ok(id, json!({ "resources": Self::resource_catalog() }))
            }
            "tools/call" => self.call_tool(id, &request.params).await,
            "resources/read" => self.read_resource(id, &request.params).await,
            _ => JsonRpcResponse::error(id, -32601, "method not found", None),
        }
    }

    async fn call_tool(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let Some(name) = params.get("name").and_then(Value::as_str) else {
            return JsonRpcResponse::error(
                id,
                -32602,
                "invalid params",
                Some(json!({ "details": "missing tool name" })),
            );
        };

        match name {
            "get_record" => self.call_get_record(id, params).await,
            "get_approval" => self.call_get_approval(id, params).await,
            "list_my_tasks" => self.call_list_my_tasks(id).await,
            "list_my_approvals" => self.call_list_my_approvals(id).await,
            "list_my_projects" => self.call_list_my_projects(id).await,
            "get_children" => self.call_get_children(id, params).await,
            "search_records" => self.call_search(id, params, SearchScope::All).await,
            "user_lookup" => self.call_user_lookup(id, params).await,
            "business_application_get" => self.call_business_application_get(id, params).await,
            "business_application_search" => {
                self.call_business_application_search(id, params).await
            }
            "business_application_query" => self.call_business_application_query(id, params).await,
            "business_application_servers" => {
                self.call_business_application_servers(id, params).await
            }
            "business_application_servers_cached" => {
                self.call_business_application_servers_cached(id, params).await
            }
            "business_applications_for_server" => {
                self.call_business_applications_for_server(id, params).await
            }
            "business_application_fields" => {
                self.call_business_application_fields(id, params).await
            }
            "server_get" => self.call_server_get(id, params).await,
            "server_search" => self.call_server_search(id, params).await,
            "server_query" => self.call_server_query(id, params).await,
            "server_fields" => self.call_server_fields(id, params).await,
            "search_knowledge" => self.call_search_knowledge(id, params).await,
            "kb_semantic_search" => self.call_kb_semantic_search(id, params).await,
            "get_article" => self.call_get_article(id, params).await,
            "list_records" => self.call_list_records(id, params).await,
            "list_knowledge_bases" => self.call_list_knowledge_bases(id).await,
            "list_categories" => self.call_list_categories(id, params).await,
            "list_knowledge_articles" => self.call_list_knowledge_articles(id, params).await,
            "vault_path" => self.call_vault_path(id).await,
            "kb_sync" => self.call_kb_sync(id, params).await,
            "kb_list_tags" => self.call_kb_list_tags(id, params).await,
            "kb_status" => self.call_kb_status(id).await,
            "kb_semantic_status" => self.call_kb_semantic_status(id).await,
            "kb_semantic_rebuild" => self.call_kb_semantic_rebuild(id, params).await,
            "repair_vault" => self.call_repair_vault(id).await,
            "rebuild_cache" => self.call_rebuild_cache(id).await,
            "verify_vault" => self.call_verify_vault(id).await,
            "get_work_notes" => self.call_get_work_notes(id, params).await,
            _ => JsonRpcResponse::error(id, -32601, "tool not found", None),
        }
    }

    async fn read_resource(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let Some(uri) = params.get("uri").and_then(Value::as_str) else {
            return JsonRpcResponse::error(
                id,
                -32602,
                "invalid params",
                Some(json!({ "details": "missing uri" })),
            );
        };

        match uri {
            u if u.starts_with("snow://records/") => self.read_record_resource(id, u).await,
            u if u.starts_with("snow://knowledge/") => self.read_knowledge_resource(id, u).await,
            "snow://dashboard" => {
                let transport = DaemonTransport::new(self.state.core.as_ref());
                let tasks = match self.state.core.my_tasks().await {
                    Ok(records) => match wrap_records(&transport, records) {
                        Ok(records) => records,
                        Err(err) => return service_failure(id, err),
                    },
                    Err(err) => return service_failure(id, err),
                };
                let approvals = match self.state.core.my_approvals_with_routing_fresh().await {
                    Ok(response) => match wrap_approvals(&transport, response.records) {
                        Ok(records) => records,
                        Err(err) => return service_failure(id, err),
                    },
                    Err(err) => return service_failure(id, err),
                };
                JsonRpcResponse::ok(
                    id,
                    json!({
                        "uri": uri,
                        "mimeType": "application/json",
                        "json": {
                            "tasks": tasks,
                            "approvals": approvals
                        }
                    }),
                )
            }
            _ => JsonRpcResponse::error(id, -32601, "resource not found", None),
        }
    }

    async fn call_get_record(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let transport = DaemonTransport::new(self.state.core.as_ref());
        let Some(arguments) = params.get("arguments") else {
            return JsonRpcResponse::error(
                id,
                -32602,
                "invalid params",
                Some(json!({ "details": "missing arguments" })),
            );
        };
        let lookup = match extract_record_lookup(arguments) {
            Ok(lookup) => lookup,
            Err(err) => {
                return JsonRpcResponse::error(
                    id,
                    -32602,
                    "invalid params",
                    Some(json!({ "details": err.to_string() })),
                );
            }
        };

        match get_record_by_lookup_cached_or_fresh(self.state.core.as_ref(), lookup).await {
            Ok(Some(record)) => match transport.record(&record) {
                Ok(record) => JsonRpcResponse::ok(id, json!({ "record": record })),
                Err(err) => service_failure(id, err),
            },
            Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_get_children(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let transport = DaemonTransport::new(self.state.core.as_ref());
        let Some(arguments) = params.get("arguments") else {
            return JsonRpcResponse::error(
                id,
                -32602,
                "invalid params",
                Some(json!({ "details": "missing arguments" })),
            );
        };
        let Some(number) = arguments.get("number").and_then(Value::as_str) else {
            return JsonRpcResponse::error(
                id,
                -32602,
                "invalid params",
                Some(json!({ "details": "missing number" })),
            );
        };

        match self.state.core.get_children(number).await {
            Ok(records) => match wrap_records(&transport, records) {
                Ok(records) => {
                    JsonRpcResponse::ok(id, json!({ "records": records, "parent": number }))
                }
                Err(err) => service_failure(id, err),
            },
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_list_my_tasks(&self, id: Option<Value>) -> JsonRpcResponse {
        let transport = DaemonTransport::new(self.state.core.as_ref());
        match self.state.core.my_tasks().await {
            Ok(records) if !records.is_empty() => match wrap_records(&transport, records) {
                Ok(records) => JsonRpcResponse::ok(id, json!({ "records": records })),
                Err(err) => service_failure(id, err),
            },
            Ok(_) | Err(_) => match self.state.core.my_tasks_fresh().await {
                Ok(records) => match wrap_records(&transport, records) {
                    Ok(records) => JsonRpcResponse::ok(id, json!({ "records": records })),
                    Err(err) => service_failure(id, err),
                },
                Err(err) => service_failure(id, err),
            },
        }
    }

    async fn call_list_my_approvals(&self, id: Option<Value>) -> JsonRpcResponse {
        let transport = DaemonTransport::new(self.state.core.as_ref());
        match self.state.core.my_approvals_with_routing_fresh().await {
            Ok(response) => match wrap_approvals(&transport, response.records) {
                Ok(records) => JsonRpcResponse::ok(
                    id,
                    json!({
                        "records": records,
                        "query_summary": response.query_summary,
                    }),
                ),
                Err(err) => service_failure(id, err),
            },
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_list_my_projects(&self, id: Option<Value>) -> JsonRpcResponse {
        let transport = DaemonTransport::new(self.state.core.as_ref());
        match self.state.core.my_projects().await {
            Ok(records) if !records.is_empty() => match wrap_records(&transport, records) {
                Ok(records) => JsonRpcResponse::ok(id, json!({ "records": records })),
                Err(err) => service_failure(id, err),
            },
            Ok(_) | Err(_) => match self.state.core.my_projects_fresh().await {
                Ok(records) => match wrap_records(&transport, records) {
                    Ok(records) => JsonRpcResponse::ok(id, json!({ "records": records })),
                    Err(err) => service_failure(id, err),
                },
                Err(err) => service_failure(id, err),
            },
        }
    }

    async fn call_search(
        &self,
        id: Option<Value>,
        params: &Value,
        scope: SearchScope,
    ) -> JsonRpcResponse {
        let transport = DaemonTransport::new(self.state.core.as_ref());
        let Some(arguments) = params.get("arguments") else {
            return JsonRpcResponse::error(
                id,
                -32602,
                "invalid params",
                Some(json!({ "details": "missing arguments" })),
            );
        };
        let Some(query) = arguments.get("query").and_then(Value::as_str) else {
            return JsonRpcResponse::error(
                id,
                -32602,
                "invalid params",
                Some(json!({ "details": "missing query" })),
            );
        };

        let effective_scope = arguments
            .get("scope")
            .and_then(Value::as_str)
            .map(parse_search_scope)
            .unwrap_or(scope);
        let limit = arguments
            .get("limit")
            .and_then(Value::as_u64)
            .map(|value| value as usize)
            .unwrap_or(20);

        match self
            .state
            .core
            .search_enriched(query, effective_scope)
            .await
        {
            Ok(results) => {
                let mut transport_results = Vec::new();
                for result in results.into_iter().take(limit) {
                    match transport.search_result(&result).await {
                        Ok(result) => transport_results.push(result),
                        Err(err) => return service_failure(id, err),
                    }
                }
                JsonRpcResponse::ok(id, json!({ "results": transport_results }))
            }
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_user_lookup(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let parsed: snow_core::UserLookup = match serde_json::from_value(arguments) {
            Ok(parsed) => parsed,
            Err(err) => return invalid_params(id, err),
        };
        if let Err(err) = parsed.validate_selector() {
            return invalid_params(id, err);
        }

        match self.state.core.lookup_user(parsed).await {
            Ok(Some(result)) => JsonRpcResponse::ok(id, json!(result)),
            Ok(None) => JsonRpcResponse::error(id, -32004, "user not found", None),
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_business_application_search(
        &self,
        id: Option<Value>,
        params: &Value,
    ) -> JsonRpcResponse {
        let transport = DaemonTransport::new(self.state.core.as_ref());
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let search_arguments = strip_business_application_hydration(arguments);
        let parsed: snow_core::BusinessApplicationSearchParams =
            match serde_json::from_value(search_arguments) {
                Ok(parsed) => parsed,
                Err(err) => return invalid_params(id, err),
            };
        if let Err(err) = parsed.validate() {
            return invalid_params(id, err);
        }

        match self.state.core.search_business_applications(parsed).await {
            Ok(records) => {
                let mut applications = Vec::with_capacity(records.len());
                let mut record_dtos = Vec::with_capacity(records.len());
                for record in records {
                    match transport.business_application(&record) {
                        Ok(application) => {
                            record_dtos.push(application.record.clone());
                            applications.push(application);
                        }
                        Err(err) => return service_failure(id, err),
                    }
                }
                JsonRpcResponse::ok(
                    id,
                    json!({
                        "business_applications": applications,
                        "records": record_dtos
                    }),
                )
            }
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_business_application_get(
        &self,
        id: Option<Value>,
        params: &Value,
    ) -> JsonRpcResponse {
        let transport = DaemonTransport::new(self.state.core.as_ref());
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let Value::Object(map) = arguments else {
            return invalid_params(id, "arguments must be an object");
        };
        let sys_id = map.get("sys_id").and_then(Value::as_str).map(str::trim);
        let name = map.get("name").and_then(Value::as_str).map(str::trim);
        let records = match self
            .state
            .core
            .list_records_query(ListQuery::new().resource_type(ResourceType::BusinessApplication))
            .await
        {
            Ok(records) => records,
            Err(err) => return service_failure(id, err),
        };
        let record = match (
            sys_id.filter(|value| !value.is_empty()),
            name.filter(|value| !value.is_empty()),
        ) {
            (Some(sys_id), None) => match snow_core::normalize_record_lookup_sys_id(sys_id) {
                Ok(sys_id) => records
                    .into_iter()
                    .find(|record| record.sys_id.eq_ignore_ascii_case(&sys_id)),
                Err(err) => return invalid_params(id, err),
            },
            (None, Some(name)) => records
                .into_iter()
                .find(|record| business_application_name(record) == name),
            (Some(_), Some(_)) => return invalid_params(id, "provide exactly one of sys_id or name"),
            (None, None) => return invalid_params(id, "missing required lookup: sys_id or name"),
        };
        match record {
            Some(record) => match transport.business_application(&record) {
                Ok(application) => JsonRpcResponse::ok(
                    id,
                    json!({
                        "business_application": application,
                        "markdown": render_snow_record(&record),
                    }),
                ),
                Err(err) => service_failure(id, err),
            },
            None => JsonRpcResponse::error(id, -32004, "business application not found", None),
        }
    }

    async fn call_business_application_query(
        &self,
        id: Option<Value>,
        params: &Value,
    ) -> JsonRpcResponse {
        let transport = DaemonTransport::new(self.state.core.as_ref());
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let mut query: snow_core::query::filter::BusinessApplicationQuery =
            match serde_json::from_value(arguments) {
                Ok(query) => query,
                Err(err) => return invalid_params(id, err),
            };
        if query.limit == Some(0) || query.limit.unwrap_or(20) > 500 {
            return invalid_params(id, "`limit` must be between 1 and 500");
        }
        query.allow_unknown_fields = true;
        let records = match self.state.core.query_business_applications(query).await {
            Ok(records) => records,
            Err(err) => return service_failure(id, err),
        };
        let mut applications = Vec::new();
        for record in records {
            match transport.business_application(&record) {
                Ok(application) => applications.push(application),
                Err(err) => return service_failure(id, err),
            }
        }
        JsonRpcResponse::ok(id, json!({ "business_applications": applications }))
    }

    async fn call_business_application_servers(
        &self,
        id: Option<Value>,
        params: &Value,
    ) -> JsonRpcResponse {
        let transport = DaemonTransport::new(self.state.core.as_ref());
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let parsed = match parse_business_application_servers_mcp_arguments(arguments) {
            Ok(parsed) => parsed,
            Err(err) => return invalid_params(id, err),
        };

        let result = match self.state.core.business_application_servers(parsed).await {
            Ok(Some(result)) => result,
            Ok(None) => {
                return JsonRpcResponse::error(id, -32004, "business application not found", None);
            }
            Err(err) => return service_failure(id, err),
        };

        let mut servers = Vec::with_capacity(result.servers.len());
        for server in result.servers {
            match transport.server(&server.record) {
                Ok(mut server_value) => {
                    if let (Some(source), Some(object)) = (
                        result.server_sources.get(&server.record.sys_id),
                        server_value.as_object_mut(),
                    ) {
                        object.insert("source".to_string(), json!(source.as_str()));
                    }
                    servers.push(server_value);
                }
                Err(err) => return service_failure(id, err),
            }
        }

        JsonRpcResponse::ok(
            id,
            json!({
                "business_application": result.business_application,
                "servers": servers,
                "server_provenance": result.server_provenance,
                "inventory_health": result.inventory_health,
                "relationship_summary": result.relationship_summary,
                "diagnostics": result.diagnostics,
                "server_paths": result.server_paths,
            }),
        )
    }

    async fn call_business_application_servers_cached(
        &self,
        id: Option<Value>,
        params: &Value,
    ) -> JsonRpcResponse {
        let transport = DaemonTransport::new(self.state.core.as_ref());
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let parsed: snow_core::BusinessApplicationServersCachedParams =
            match serde_json::from_value(arguments) {
                Ok(parsed) => parsed,
                Err(err) => return invalid_params(id, err),
            };
        if let Err(err) = parsed.validate() {
            return invalid_params(id, err);
        }
        let result = match self
            .state
            .core
            .business_application_servers_cached(parsed)
            .await
        {
            Ok(Some(result)) => result,
            Ok(None) => {
                return JsonRpcResponse::error(
                    id,
                    -32004,
                    "business application not found",
                    Some(json!({
                        "endpoint_status": "live_confirmation_not_attempted",
                        "relationship_status": "unknown_not_synced"
                    })),
                );
            }
            Err(err) => return service_failure(id, err),
        };
        let business_application = match transport.business_application(&result.business_application) {
            Ok(application) => application,
            Err(err) => return service_failure(id, err),
        };
        let mut servers = Vec::with_capacity(result.servers.len());
        for relationship in result.servers {
            let server = match transport.server(&relationship.server) {
                Ok(server) => server,
                Err(err) => return service_failure(id, err),
            };
            servers.push(json!({
                "server": server,
                "server_table": relationship.server_table,
                "provenance": relationship.provenance,
                "min_depth": relationship.min_depth,
                "paths": relationship.paths,
                "tombstoned_at": relationship.tombstoned_at,
            }));
        }
        JsonRpcResponse::ok(
            id,
            json!({
                "business_application": business_application,
                "servers": servers,
                "endpoint_status": result.endpoint_status,
                "relationship_status": result.relationship_status,
                "inventory_health": result.inventory_health,
            }),
        )
    }

    async fn call_business_applications_for_server(
        &self,
        id: Option<Value>,
        params: &Value,
    ) -> JsonRpcResponse {
        let transport = DaemonTransport::new(self.state.core.as_ref());
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let parsed: snow_core::BusinessApplicationsForServerParams =
            match serde_json::from_value(arguments) {
                Ok(parsed) => parsed,
                Err(err) => return invalid_params(id, err),
            };
        if let Err(err) = parsed.validate() {
            return invalid_params(id, err);
        }
        let result = match self
            .state
            .core
            .business_applications_for_server(parsed)
            .await
        {
            Ok(Some(result)) => result,
            Ok(None) => {
                return JsonRpcResponse::error(
                    id,
                    -32004,
                    "server not found",
                    Some(json!({
                        "endpoint_status": "live_confirmation_not_attempted",
                        "relationship_status": "unknown_not_synced"
                    })),
                );
            }
            Err(err) => return service_failure(id, err),
        };
        let mut servers = Vec::with_capacity(result.servers.len());
        for server_relationships in result.servers {
            let server = match transport.server(&server_relationships.server) {
                Ok(server) => server,
                Err(err) => return service_failure(id, err),
            };
            let mut business_applications =
                Vec::with_capacity(server_relationships.business_applications.len());
            for relationship in server_relationships.business_applications {
                let business_application =
                    match transport.business_application(&relationship.business_application) {
                        Ok(application) => application,
                        Err(err) => return service_failure(id, err),
                    };
                    business_applications.push(json!({
                        "business_application": business_application,
                        "provenance": relationship.provenance,
                        "min_depth": relationship.min_depth,
                        "paths": relationship.paths,
                        "inventory_health": relationship.inventory_health,
                        "tombstoned_at": relationship.tombstoned_at,
                    }));
            }
            servers.push(json!({
                "server": server,
                "business_applications": business_applications,
                "endpoint_status": server_relationships.endpoint_status,
                "relationship_status": server_relationships.relationship_status,
            }));
        }
        JsonRpcResponse::ok(
            id,
            json!({
                "servers": servers,
                "endpoint_status": result.endpoint_status,
                "relationship_status": result.relationship_status,
            }),
        )
    }

    async fn call_business_application_fields(
        &self,
        id: Option<Value>,
        _params: &Value,
    ) -> JsonRpcResponse {
        let records = match self
            .state
            .core
            .list_records_query(ListQuery::new().resource_type(ResourceType::BusinessApplication))
            .await
        {
            Ok(records) => records,
            Err(err) => return service_failure(id, err),
        };
        let mut fields = std::collections::BTreeMap::<String, usize>::new();
        for record in records {
            for name in record.fields.keys() {
                *fields.entry(name.clone()).or_default() += 1;
            }
        }
        JsonRpcResponse::ok(
            id,
            json!({
                "fields": fields
                    .into_iter()
                    .map(|(field, observed_count)| json!({ "field": field, "observed_count": observed_count }))
                    .collect::<Vec<_>>()
            }),
        )
    }

    async fn call_server_search(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let transport = DaemonTransport::new(self.state.core.as_ref());
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let parsed: snow_core::ServerSearchParams = match serde_json::from_value(arguments) {
            Ok(parsed) => parsed,
            Err(err) => return invalid_params(id, err),
        };
        if let Err(err) = parsed.validate() {
            return invalid_params(id, err);
        }
        match self.state.core.search_servers(parsed).await {
            Ok(records) => {
                let mut servers = Vec::with_capacity(records.len());
                let mut record_dtos = Vec::with_capacity(records.len());
                for record in records {
                    match transport.server(&record) {
                        Ok(server) => {
                            record_dtos.push(server.record.clone());
                            servers.push(server);
                        }
                        Err(err) => return service_failure(id, err),
                    }
                }
                JsonRpcResponse::ok(id, json!({ "servers": servers, "records": record_dtos }))
            }
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_server_get(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let transport = DaemonTransport::new(self.state.core.as_ref());
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let Value::Object(map) = arguments else {
            return invalid_params(id, "arguments must be an object");
        };
        let sys_id = map.get("sys_id").and_then(Value::as_str).map(str::trim);
        let name = map.get("name").and_then(Value::as_str).map(str::trim);
        let ip_address = map.get("ip_address").and_then(Value::as_str).map(str::trim);
        let records = match self
            .state
            .core
            .list_records_query(ListQuery::new().resource_type(ResourceType::Server))
            .await
        {
            Ok(records) => records,
            Err(err) => return service_failure(id, err),
        };
        let (record, core_lookup) = match (
            sys_id.filter(|value| !value.is_empty()),
            name.filter(|value| !value.is_empty()),
            ip_address.filter(|value| !value.is_empty()),
        ) {
            (Some(sys_id), None, None) => match snow_core::normalize_record_lookup_sys_id(sys_id) {
                Ok(sys_id) => {
                    let hit = records
                        .into_iter()
                        .find(|record| record.sys_id.eq_ignore_ascii_case(&sys_id));
                    (hit, snow_core::ServerLookup::SysId(sys_id))
                }
                Err(err) => return invalid_params(id, err),
            },
            (None, Some(name), None) => {
                let hit = records
                    .into_iter()
                    .find(|record| server_name(record) == name);
                (hit, snow_core::ServerLookup::exact_name(name))
            }
            (None, None, Some(ip_address)) => {
                let hit = records.into_iter().find(|record| {
                    server_field(record, "ip_address")
                        .is_some_and(|value| value.eq_ignore_ascii_case(ip_address))
                });
                (hit, snow_core::ServerLookup::ip_address(ip_address))
            }
            (None, None, None) => {
                return invalid_params(id, "missing required lookup: sys_id, name, or ip_address");
            }
            _ => return invalid_params(id, "provide exactly one of sys_id, name, or ip_address"),
        };
        if let Some(record) = record {
            return match transport.server(&record) {
                Ok(server) => JsonRpcResponse::ok(
                    id,
                    json!({
                        "server": server,
                        "markdown": render_snow_record(&record),
                    }),
                ),
                Err(err) => service_failure(id, err),
            };
        }
        match self.state.core.get_server_live(core_lookup, false).await {
            Ok(Some(server)) => match transport.server(&server.record) {
                Ok(server_json) => JsonRpcResponse::ok(
                    id,
                    json!({
                        "server": server_json,
                        "markdown": render_snow_record(&server.record),
                    }),
                ),
                Err(err) => service_failure(id, err),
            },
            Ok(None) => JsonRpcResponse::error(id, -32004, "server not found", None),
            Err(err) => server_get_error_response(id, err),
        }
    }

    async fn call_server_query(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let transport = DaemonTransport::new(self.state.core.as_ref());
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let query: snow_core::ServerQuery = match serde_json::from_value(arguments) {
            Ok(query) => query,
            Err(err) => return invalid_params(id, err),
        };
        if let Err(err) = query.validate() {
            return invalid_params(id, err);
        }
        let records = match self.state.core.query_servers(query).await {
            Ok(records) => records,
            Err(err) => return service_failure(id, err),
        };
        let mut servers = Vec::new();
        for record in records {
            match transport.server(&record) {
                Ok(server) => servers.push(server),
                Err(err) => return service_failure(id, err),
            }
        }
        JsonRpcResponse::ok(id, json!({ "servers": servers }))
    }

    async fn call_server_fields(&self, id: Option<Value>, _params: &Value) -> JsonRpcResponse {
        let records = match self
            .state
            .core
            .list_records_query(ListQuery::new().resource_type(ResourceType::Server))
            .await
        {
            Ok(records) => records,
            Err(err) => return service_failure(id, err),
        };
        let mut fields = std::collections::BTreeMap::<String, usize>::new();
        for record in records {
            for name in record.fields.keys() {
                *fields.entry(name.clone()).or_default() += 1;
            }
        }
        JsonRpcResponse::ok(
            id,
            json!({
                "fields": fields
                    .into_iter()
                    .map(|(field, observed_count)| json!({ "field": field, "observed_count": observed_count }))
                    .collect::<Vec<_>>()
            }),
        )
    }

    async fn call_search_knowledge(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let transport = DaemonTransport::new(self.state.core.as_ref());
        let Some(arguments) = params.get("arguments") else {
            return JsonRpcResponse::error(
                id,
                -32602,
                "invalid params",
                Some(json!({ "details": "missing arguments" })),
            );
        };
        let Some(query) = arguments.get("query").and_then(Value::as_str) else {
            return JsonRpcResponse::error(
                id,
                -32602,
                "invalid params",
                Some(json!({ "details": "missing query" })),
            );
        };

        let filters = snow_core::KnowledgeSearchFilters {
            knowledge_base: arguments
                .get("knowledge_base")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            category: arguments
                .get("category")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned),
            limit: arguments
                .get("limit")
                .and_then(Value::as_u64)
                .map(|value| value as usize),
        };

        match self.state.core.search_knowledge(query, filters).await {
            Ok(articles) => match wrap_articles(&transport, articles) {
                Ok(articles) => JsonRpcResponse::ok(id, json!({ "articles": articles })),
                Err(err) => service_failure(id, err),
            },
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_get_article(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let transport = DaemonTransport::new(self.state.core.as_ref());
        let Some(arguments) = params.get("arguments") else {
            return JsonRpcResponse::error(
                id,
                -32602,
                "invalid params",
                Some(json!({ "details": "missing arguments" })),
            );
        };
        let Some(number) = arguments.get("number").and_then(Value::as_str) else {
            return JsonRpcResponse::error(
                id,
                -32602,
                "invalid params",
                Some(json!({ "details": "missing number" })),
            );
        };
        let fresh = arguments
            .get("fresh")
            .and_then(Value::as_bool)
            .unwrap_or(false);

        let article = if fresh {
            self.state.core.get_knowledge_article_fresh(number).await
        } else {
            self.state.core.get_knowledge_article(number).await
        };

        match article {
            Ok(Some(article)) => match transport.knowledge_article(&article) {
                Ok(article_dto) => JsonRpcResponse::ok(
                    id,
                    json!({ "article": article_dto, "markdown": render_knowledge_article(&article) }),
                ),
                Err(err) => service_failure(id, err),
            },
            Ok(None) => JsonRpcResponse::error(id, -32004, "knowledge article not found", None),
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_kb_semantic_search(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let transport = DaemonTransport::new(self.state.core.as_ref());
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));

        match extract_kb_semantic_search_filters(&arguments) {
            Ok((query, filters)) => match self
                .state
                .core
                .search_knowledge_semantic(&query, filters)
                .await
            {
                Ok(hits) => {
                    let mut hit_dtos = Vec::with_capacity(hits.len());
                    for hit in hits {
                        match transport.knowledge_search_hit(&hit) {
                            Ok(hit) => hit_dtos.push(hit),
                            Err(err) => return service_failure(id, err),
                        }
                    }
                    JsonRpcResponse::ok(id, json!({ "hits": hit_dtos }))
                }
                Err(err) => service_failure(id, err),
            },
            Err(err) => invalid_params(id, err),
        }
    }

    async fn call_list_records(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let transport = DaemonTransport::new(self.state.core.as_ref());
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let parsed: ListRecordsArguments = match serde_json::from_value(arguments) {
            Ok(parsed) => parsed,
            Err(err) => return invalid_params(id, err),
        };

        let mut query = ListQuery::new();
        if let Some(resource_type) = parsed.resource_type.as_deref() {
            match parse_resource_type(resource_type) {
                Ok(resource_type) => query = query.resource_type(resource_type),
                Err(err) => return invalid_params(id, err),
            }
        }
        if let Some(assigned_to) = parsed.assigned_to {
            query = query.assigned_to(assigned_to);
        }
        if let Some(limit) = parsed.limit {
            query = query.limit(limit);
        }
        if let Some(parent_number) = parsed.parent_number {
            match self.state.core.get_record(&parent_number).await {
                Ok(Some(parent)) => {
                    query = query.parent_sys_id(parent.sys_id);
                }
                Ok(None) => {
                    return JsonRpcResponse::error(id, -32004, "parent record not found", None);
                }
                Err(err) => return service_failure(id, err),
            }
        }

        match self.state.core.list_records_query(query).await {
            Ok(records) => match wrap_records(&transport, records) {
                Ok(records) => JsonRpcResponse::ok(id, json!({ "records": records })),
                Err(err) => service_failure(id, err),
            },
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_list_knowledge_bases(&self, id: Option<Value>) -> JsonRpcResponse {
        let transport = DaemonTransport::new(self.state.core.as_ref());
        match self.state.core.list_knowledge_bases() {
            Ok(bases) => JsonRpcResponse::ok(
                id,
                json!({
                    "bases": bases
                        .into_iter()
                        .map(|base| transport.knowledge_base_summary(base))
                        .collect::<Vec<_>>()
                }),
            ),
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_list_categories(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let transport = DaemonTransport::new(self.state.core.as_ref());
        let Some(arguments) = params.get("arguments") else {
            return JsonRpcResponse::error(
                id,
                -32602,
                "invalid params",
                Some(json!({ "details": "missing arguments" })),
            );
        };
        let Some(knowledge_base_sys_id) = arguments
            .get("knowledge_base_sys_id")
            .and_then(Value::as_str)
        else {
            return JsonRpcResponse::error(
                id,
                -32602,
                "invalid params",
                Some(json!({ "details": "missing knowledge_base_sys_id" })),
            );
        };

        match self.state.core.list_categories(knowledge_base_sys_id) {
            Ok(categories) => JsonRpcResponse::ok(
                id,
                json!({
                    "categories": categories
                        .into_iter()
                        .map(|category| transport.knowledge_category_summary(category))
                        .collect::<Vec<_>>()
                }),
            ),
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_list_knowledge_articles(
        &self,
        id: Option<Value>,
        params: &Value,
    ) -> JsonRpcResponse {
        let transport = DaemonTransport::new(self.state.core.as_ref());
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let parsed: ListKnowledgeArticlesArguments = match serde_json::from_value(arguments) {
            Ok(parsed) => parsed,
            Err(err) => return invalid_params(id, err),
        };

        match self
            .state
            .core
            .list_knowledge_articles(
                parsed.knowledge_base_sys_id.as_deref(),
                parsed.category_sys_id.as_deref(),
                parsed.limit,
            )
            .await
        {
            Ok(articles) => match wrap_articles(&transport, articles) {
                Ok(articles) => JsonRpcResponse::ok(id, json!({ "articles": articles })),
                Err(err) => service_failure(id, err),
            },
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_vault_path(&self, id: Option<Value>) -> JsonRpcResponse {
        let transport = DaemonTransport::new(self.state.core.as_ref());
        JsonRpcResponse::ok(id, json!({ "path": transport.vault_path() }))
    }

    async fn call_kb_sync(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        match extract_kb_sync_params(&arguments) {
            Ok(parsed) => match self
                .state
                .core
                .sync_knowledge(parsed.full, parsed.with_bodies)
                .await
            {
                Ok(sync) => JsonRpcResponse::ok(
                    id,
                    json!({ "sync": DaemonKnowledgeSyncOutcome::from(sync) }),
                ),
                Err(err) => service_failure(id, err),
            },
            Err(err) => invalid_params(id, err),
        }
    }

    async fn call_kb_list_tags(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        match extract_kb_list_tags_params(&arguments) {
            Ok(parsed) => {
                let layer = match parsed.layer.as_deref() {
                    Some("all") | None => None,
                    Some(layer) => match crate::rpc::core_kb_tag_layer(layer) {
                        Ok(layer) => Some(layer),
                        Err(err) => return invalid_params(id, err),
                    },
                };
                match self.state.core.list_knowledge_tags(layer, parsed.min_count) {
                    Ok(tags) => JsonRpcResponse::ok(
                        id,
                        json!({
                            "tags": tags
                                .into_iter()
                                .map(DaemonKnowledgeTagSummary::from)
                                .collect::<Vec<_>>()
                        }),
                    ),
                    Err(err) => service_failure(id, err),
                }
            }
            Err(err) => invalid_params(id, err),
        }
    }

    async fn call_kb_status(&self, id: Option<Value>) -> JsonRpcResponse {
        match self.state.core.knowledge_status() {
            Ok(status) => {
                JsonRpcResponse::ok(id, json!({ "status": DaemonKnowledgeStatus::from(status) }))
            }
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_kb_semantic_status(&self, id: Option<Value>) -> JsonRpcResponse {
        match self.state.core.knowledge_semantic_status().await {
            Ok(status) => JsonRpcResponse::ok(
                id,
                json!({ "status": DaemonKnowledgeSemanticStatus::from(status) }),
            ),
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_kb_semantic_rebuild(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let full = params
            .get("arguments")
            .and_then(|arguments| arguments.get("full"))
            .and_then(Value::as_bool)
            .unwrap_or(false);

        match self.state.core.rebuild_knowledge_semantic_index(full).await {
            Ok(summary) => JsonRpcResponse::ok(
                id,
                json!({ "summary": DaemonSemanticIndexSummary::from(summary) }),
            ),
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_get_approval(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let transport = DaemonTransport::new(self.state.core.as_ref());
        let Some(arguments) = params.get("arguments") else {
            return JsonRpcResponse::error(
                id,
                -32602,
                "invalid params",
                Some(json!({ "details": "missing arguments" })),
            );
        };
        let Some(number) = arguments.get("number").and_then(Value::as_str) else {
            return JsonRpcResponse::error(
                id,
                -32602,
                "invalid params",
                Some(json!({ "details": "missing number" })),
            );
        };

        match self.state.core.get_approval(number).await {
            Ok(Some(approval)) => match transport.approval(&approval) {
                Ok(approval_dto) => JsonRpcResponse::ok(
                    id,
                    json!({ "approval": approval_dto, "markdown": render_approval_record(&approval) }),
                ),
                Err(err) => service_failure(id, err),
            },
            Ok(None) => JsonRpcResponse::error(id, -32004, "approval not found", None),
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_repair_vault(&self, id: Option<Value>) -> JsonRpcResponse {
        match self.state.core.repair_vault().await {
            Ok(report) => JsonRpcResponse::ok(id, json!({ "report": report })),
            Err(err) => JsonRpcResponse::error(
                id,
                -32000,
                "service failure",
                Some(json!({ "details": err.to_string() })),
            ),
        }
    }

    async fn call_rebuild_cache(&self, id: Option<Value>) -> JsonRpcResponse {
        match self.state.core.rebuild_cache() {
            Ok(report) => JsonRpcResponse::ok(id, json!({ "report": report })),
            Err(err) => JsonRpcResponse::error(
                id,
                -32000,
                "service failure",
                Some(json!({ "details": err.to_string() })),
            ),
        }
    }

    async fn call_verify_vault(&self, id: Option<Value>) -> JsonRpcResponse {
        match self.state.core.verify_vault() {
            Ok(report) => JsonRpcResponse::ok(id, json!({ "report": report })),
            Err(err) => JsonRpcResponse::error(
                id,
                -32000,
                "service failure",
                Some(json!({ "details": err.to_string() })),
            ),
        }
    }

    async fn call_get_work_notes(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let transport = DaemonTransport::new(self.state.core.as_ref());
        let Some(arguments) = params.get("arguments") else {
            return JsonRpcResponse::error(
                id,
                -32602,
                "invalid params",
                Some(json!({ "details": "missing arguments" })),
            );
        };
        let lookup = match extract_record_lookup(arguments) {
            Ok(lookup) => lookup,
            Err(err) => {
                return JsonRpcResponse::error(
                    id,
                    -32602,
                    "invalid params",
                    Some(json!({ "details": err.to_string() })),
                );
            }
        };

        match get_record_by_lookup_cached_or_fresh(self.state.core.as_ref(), lookup).await {
            Ok(Some(record)) => match transport.record(&record) {
                Ok(record) => JsonRpcResponse::ok(id, json!({ "work_notes": record.work_notes })),
                Err(err) => service_failure(id, err),
            },
            Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
            Err(err) => service_failure(id, err),
        }
    }

    async fn read_record_resource(&self, id: Option<Value>, uri: &str) -> JsonRpcResponse {
        let number = uri.trim_start_matches("snow://records/");
        match get_record_cached_or_fresh(self.state.core.as_ref(), number).await {
            Ok(Some(record)) => JsonRpcResponse::ok(
                id,
                json!({
                    "uri": uri,
                    "mimeType": "text/markdown",
                    "text": render_snow_record(&record)
                }),
            ),
            Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
            Err(err) => JsonRpcResponse::error(
                id,
                -32000,
                "service failure",
                Some(json!({ "details": err.to_string() })),
            ),
        }
    }

    async fn read_knowledge_resource(&self, id: Option<Value>, uri: &str) -> JsonRpcResponse {
        let number = uri.trim_start_matches("snow://knowledge/");
        match self.state.core.get_knowledge_article(number).await {
            Ok(Some(article)) => JsonRpcResponse::ok(
                id,
                json!({
                    "uri": uri,
                    "mimeType": "text/markdown",
                    "text": render_knowledge_article(&article)
                }),
            ),
            Ok(None) => JsonRpcResponse::error(id, -32004, "knowledge article not found", None),
            Err(err) => JsonRpcResponse::error(
                id,
                -32000,
                "service failure",
                Some(json!({ "details": err.to_string() })),
            ),
        }
    }

    fn tools_catalog() -> Vec<McpTool> {
        vec![
            McpTool {
                name: "get_record".to_string(),
                description: "Retrieve a generic ServiceNow record by number or allowed table/sys_id. Do not use for APM Business Application numbers such as APM0002456; use business_application_query for APM-number lookup, or business_application_get/search when you have sys_id, exact name, or BA filters.".to_string(),
                input_schema: snow_mcp::tools::records::record_lookup_arg_schema(
                    snow_core::RECORD_LOOKUP_ALLOWED_TABLES,
                ),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "get_approval".to_string(),
                description: "Get an approval through the typed runtime path".to_string(),
                input_schema: json!({"type":"object","properties":{"number":{"type":"string"}},"required":["number"]}),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "search_records".to_string(),
                description: "Full-text search across generic records. Do not use for APM Business Application numbers such as APM0002456; use business_application_query/search for Business Application routing.".to_string(),
                input_schema: snow_mcp::tools::records::search_records_arg_schema(),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "user_lookup".to_string(),
                description: "Resolve one active ServiceNow user by login, email, employee number, sys_id, or inferred query".to_string(),
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "query":{"type":"string"},
                        "user_name":{"type":"string"},
                        "email":{"type":"string"},
                        "employee_number":{"type":"string"},
                        "sys_id":{"type":"string","pattern":"^[0-9a-fA-F]{32}$"},
                        "active":{"type":"boolean","default":true}
                    }
                }),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "business_application_get".to_string(),
                description: "Get a locally cached Business Application by sys_id or exact name. For an APM number such as APM0002456, use business_application_query with a number filter."
                    .to_string(),
                input_schema: snow_mcp::tools::records::business_application_get_arg_schema(),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "business_application_search".to_string(),
                description: "Live-search Business Applications by name, owner, support group, portfolio, operational state, or attested date. For exact APM numbers such as APM0002456, prefer business_application_query with field=number.".to_string(),
                input_schema: snow_mcp::tools::records::business_application_search_arg_schema(),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "business_application_query".to_string(),
                description: "Query locally projected Business Application fields. Use this for APM identifiers such as APM0002456 by filtering field=number, op=eq, value=APM0002456.".to_string(),
                input_schema: snow_mcp::tools::records::business_application_query_arg_schema(),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "business_application_servers".to_string(),
                description: "Read servers associated with a Business Application by Business Application number or cmdb_ci_business_app sys_id using bounded live CMDB relationship traversal. Traversal-only: does not persist, prune, or write vault data.".to_string(),
                input_schema: snow_mcp::tools::records::business_application_servers_arg_schema(),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "business_application_servers_cached".to_string(),
                description: "Read cached server relationships for one Business Application by Business Application number, exact name, or cmdb_ci_business_app sys_id. Cache-only: does not call ServiceNow and does not mutate the cache.".to_string(),
                input_schema: snow_mcp::tools::records::business_application_servers_cached_arg_schema(),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "business_applications_for_server".to_string(),
                description: "Cache-only reverse lookup: read cached Business Application relationships for one server by exact server name, IP address, or sys_id. Does not call ServiceNow and does not mutate the cache.".to_string(),
                input_schema: snow_mcp::tools::records::business_applications_for_server_arg_schema(),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "business_application_fields".to_string(),
                description: "List observed Business Application fields from the local projection, including owner-related fields when field mapping for an APM lookup is unclear."
                    .to_string(),
                input_schema: snow_mcp::tools::records::business_application_fields_arg_schema(),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "server_get".to_string(),
                description: "Get a cached Server by sys_id, exact name, or IP address".to_string(),
                input_schema: snow_mcp::tools::records::server_get_arg_schema(),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "server_search".to_string(),
                description: "Live-search Windows and Linux Servers by name, IP address, CI owner group, or class".to_string(),
                input_schema: snow_mcp::tools::records::server_search_arg_schema(),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "server_query".to_string(),
                description: "Query cached Windows and Linux Servers".to_string(),
                input_schema: snow_mcp::tools::records::server_query_arg_schema(),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "server_fields".to_string(),
                description: "List observed Server fields from the local cache".to_string(),
                input_schema: snow_mcp::tools::records::server_fields_arg_schema(),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "search_knowledge".to_string(),
                description: "Daemon-native knowledge search. Product-facing MCP callers should prefer knowledge_search when available.".to_string(),
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "query":{"type":"string"},
                        "knowledge_base":{"type":"string"},
                        "category":{"type":"string"},
                        "limit":{"type":"integer","minimum":1}
                    },
                    "required":["query"]
                }),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "get_article".to_string(),
                description: "Daemon-native article fetch. Product-facing MCP callers should prefer knowledge_fetch when available.".to_string(),
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "number":{"type":"string"},
                        "fresh":{"type":"boolean","description":"Read the article directly from ServiceNow instead of cache"}
                    },
                    "required":["number"]
                }),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "kb_semantic_search".to_string(),
                description: "Search knowledge through the semantic KB surface.".to_string(),
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "query":{"type":"string"},
                        "knowledge_base":{"type":"string"},
                        "category":{"type":"string"},
                        "limit":{"type":"integer","minimum":1},
                        "mode":{"type":"string","enum":["lexical","semantic","hybrid"]},
                        "min_score_millis":{"type":"integer","minimum":0}
                    },
                    "required":["query"]
                }),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "list_records".to_string(),
                description: "List records with optional daemon-side filters".to_string(),
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "resource_type":{"type":"string"},
                        "parent_number":{"type":"string"},
                        "assigned_to":{"type":"string"},
                        "limit":{"type":"integer","minimum":1}
                    }
                }),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "list_knowledge_bases".to_string(),
                description: "List knowledge bases available in the local cache".to_string(),
                input_schema: json!({"type":"object"}),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "list_categories".to_string(),
                description: "List categories for a knowledge base".to_string(),
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "knowledge_base_sys_id":{"type":"string"}
                    },
                    "required":["knowledge_base_sys_id"]
                }),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "list_knowledge_articles".to_string(),
                description:
                    "List knowledge articles with optional knowledge base and category filters"
                        .to_string(),
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "knowledge_base_sys_id":{"type":"string"},
                        "category_sys_id":{"type":"string"},
                        "limit":{"type":"integer","minimum":1}
                    }
                }),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "list_my_tasks".to_string(),
                description: "List active tasks assigned to current user".to_string(),
                input_schema: json!({"type":"object"}),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "list_my_approvals".to_string(),
                description:
                    "List pending direct and group-routed approvals for the current user"
                        .to_string(),
                input_schema: json!({"type":"object","additionalProperties":false,"properties":{}}),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "list_my_projects".to_string(),
                description: "List active projects and demands for current user".to_string(),
                input_schema: json!({"type":"object"}),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "get_children".to_string(),
                description: "Get child tasks for a parent record".to_string(),
                input_schema: json!({"type":"object","properties":{"number":{"type":"string"}},"required":["number"]}),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "get_work_notes".to_string(),
                description: "Get work notes for a record by number or allowed table/sys_id"
                    .to_string(),
                input_schema: snow_mcp::tools::records::record_lookup_arg_schema(
                    snow_core::RECORD_LOOKUP_ALLOWED_TABLES,
                ),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "vault_path".to_string(),
                description: "Return the daemon vault root path".to_string(),
                input_schema: json!({"type":"object"}),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "kb_sync".to_string(),
                description: "Invoke the KB sync surface with full or incremental options."
                    .to_string(),
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "full":{"type":"boolean"},
                        "with_bodies":{"type":"boolean"}
                    }
                }),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "kb_list_tags".to_string(),
                description: "List aggregated KB tags from the local cache.".to_string(),
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "layer":{"type":"string","enum":["all","sn","auto","user"]},
                        "min_count":{"type":"integer","minimum":1}
                    }
                }),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "kb_status".to_string(),
                description:
                    "Show KB sync timestamps and cache coverage from the local SQLite catalog."
                        .to_string(),
                input_schema: json!({"type":"object"}),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "kb_semantic_status".to_string(),
                description: "Show semantic KB index coverage and rebuild health.".to_string(),
                input_schema: json!({"type":"object"}),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "kb_semantic_rebuild".to_string(),
                description: "Rebuild the semantic KB index.".to_string(),
                input_schema: json!({
                    "type":"object",
                    "properties":{
                        "full":{"type":"boolean"}
                    }
                }),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "repair_vault".to_string(),
                description: "Repair missing vault files from cached runtime data".to_string(),
                input_schema: json!({"type":"object"}),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "rebuild_cache".to_string(),
                description: "Rebuild the cache projection from vault markdown".to_string(),
                input_schema: json!({"type":"object"}),
                output_schema: json!({"type":"object"}),
            },
            McpTool {
                name: "verify_vault".to_string(),
                description: "Verify vault and cache parity".to_string(),
                input_schema: json!({"type":"object"}),
                output_schema: json!({"type":"object"}),
            },
        ]
    }

    fn resource_catalog() -> Vec<McpResource> {
        vec![
            McpResource {
                name: "ServiceNow record".to_string(),
                uri: "snow://records/{number}".to_string(),
                description: "Record as markdown".to_string(),
                mime_type: "text/markdown".to_string(),
            },
            McpResource {
                name: "ServiceNow knowledge article".to_string(),
                uri: "snow://knowledge/{number}".to_string(),
                description: "Knowledge article as markdown".to_string(),
                mime_type: "text/markdown".to_string(),
            },
            McpResource {
                name: "ServiceNow dashboard".to_string(),
                uri: "snow://dashboard".to_string(),
                description: "Current user's task summary".to_string(),
                mime_type: "application/json".to_string(),
            },
        ]
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
struct ListRecordsArguments {
    #[serde(default)]
    resource_type: Option<String>,
    #[serde(default)]
    parent_number: Option<String>,
    #[serde(default)]
    assigned_to: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct ListKnowledgeArticlesArguments {
    #[serde(default)]
    knowledge_base_sys_id: Option<String>,
    #[serde(default)]
    category_sys_id: Option<String>,
    #[serde(default)]
    limit: Option<usize>,
}

fn wrap_records(
    transport: &DaemonTransport<'_>,
    records: Vec<snow_core::SnowRecord>,
) -> Result<Vec<crate::transport::DaemonSnowRecord>> {
    records
        .into_iter()
        .map(|record| transport.record(&record))
        .collect()
}

fn wrap_approvals(
    transport: &DaemonTransport<'_>,
    approvals: Vec<snow_core::ApprovalRecord>,
) -> Result<Vec<crate::transport::DaemonApprovalRecord>> {
    approvals
        .into_iter()
        .map(|approval| transport.approval(&approval))
        .collect()
}

fn wrap_articles(
    transport: &DaemonTransport<'_>,
    articles: Vec<snow_core::KnowledgeArticle>,
) -> Result<Vec<crate::transport::DaemonKnowledgeArticle>> {
    articles
        .into_iter()
        .map(|article| transport.knowledge_article(&article))
        .collect()
}

fn parse_resource_type(resource_type: &str) -> Result<ResourceType> {
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
        "server" | "servers" | "cmdb_ci_server" | "cmdb_ci_linux_server"
        | "cmdb_ci_win_server" | "linux_server" | "windows_server" => Ok(ResourceType::Server),
        _ => Err(anyhow!("unsupported resource_type `{resource_type}`")),
    }
}

fn parse_search_scope(scope: &str) -> SearchScope {
    match scope.trim().to_ascii_lowercase().as_str() {
        "knowledge" => SearchScope::Knowledge,
        "work_notes" => SearchScope::WorkNotes,
        _ => SearchScope::All,
    }
}

fn strip_business_application_hydration(mut arguments: Value) -> Value {
    if let Value::Object(map) = &mut arguments {
        map.remove("persist");
        map.remove("resolve_references");
        map.remove("reference_depth");
        map.remove("refresh_dictionary");
    }
    arguments
}

fn parse_business_application_servers_mcp_arguments(
    arguments: Value,
) -> std::result::Result<snow_core::BusinessApplicationServersParams, String> {
    if let Value::Object(map) = &arguments {
        for forbidden in ["persist", "prune_stale"] {
            if map.contains_key(forbidden) {
                return Err(format!(
                    "`{forbidden}` is not accepted by MCP business_application_servers"
                ));
            }
        }
    }
    let params: snow_core::BusinessApplicationServersParams =
        serde_json::from_value(arguments).map_err(|err| err.to_string())?;
    params.validate().map_err(|err| err.to_string())?;
    Ok(params)
}

fn business_application_name(record: &SnowRecord) -> String {
    record
        .fields
        .get("name")
        .and_then(|field| {
            field
                .display_value
                .as_ref()
                .or(Some(&field.value))
                .filter(|value| !value.trim().is_empty())
                .cloned()
        })
        .or_else(|| {
            (!record.short_description.trim().is_empty()).then(|| record.short_description.clone())
        })
        .unwrap_or_else(|| record.sys_id.clone())
}

fn server_name(record: &SnowRecord) -> String {
    server_field(record, "name")
        .or_else(|| {
            (!record.short_description.trim().is_empty()).then(|| record.short_description.clone())
        })
        .unwrap_or_else(|| record.sys_id.clone())
}

fn server_field(record: &SnowRecord, field: &str) -> Option<String> {
    record.fields.get(field).and_then(|value| {
        value
            .display_value
            .as_ref()
            .or(Some(&value.value))
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
    })
}

async fn get_record_cached_or_fresh(core: &SnowCore, number: &str) -> Result<Option<SnowRecord>> {
    match core.get_record(number).await? {
        Some(record) => Ok(Some(record)),
        None => core.get_record_fresh(number).await,
    }
}

fn invalid_params(id: Option<Value>, err: impl ToString) -> JsonRpcResponse {
    JsonRpcResponse::error(
        id,
        -32602,
        "invalid params",
        Some(json!({ "details": err.to_string() })),
    )
}

fn service_failure(id: Option<Value>, err: impl ToString) -> JsonRpcResponse {
    JsonRpcResponse::error(
        id,
        -32000,
        "service failure",
        Some(json!({ "details": err.to_string() })),
    )
}

fn server_get_error_response(
    id: Option<Value>,
    err: snow_core::ServerGetError,
) -> JsonRpcResponse {
    use snow_core::ServerGetError;
    match err {
        ServerGetError::NotFound => {
            JsonRpcResponse::error(id, -32004, "server not found", None)
        }
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
        ServerGetError::Hydration(detail) => service_failure(id, detail),
        ServerGetError::Other(detail) => service_failure(id, detail),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{
        build_fixture_state, build_fixture_state_at_instance, spawn_json_http_server,
    };
    use rusqlite::Connection;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
    use tokio::io::{duplex, split};

    #[test]
    fn tool_catalog_contains_core_tools() {
        let tools = McpServer::tools_catalog();
        let get_record = tools
            .iter()
            .find(|tool| tool.name == "get_record")
            .expect("get_record tool");
        assert!(
            get_record.input_schema.get("required").is_none(),
            "get_record must accept number or table+sys_id without schema-level required fields"
        );
        assert!(
            get_record.input_schema["properties"]["table"]["enum"]
                .as_array()
                .expect("table enum")
                .contains(&json!("change_request"))
        );
        assert!(tools.iter().any(|tool| tool.name == "user_lookup"));
        assert!(tools.iter().any(|tool| tool.name == "list_my_approvals"));
        assert!(tools.iter().any(|tool| tool.name == "list_my_projects"));
        assert!(tools.iter().any(|tool| tool.name == "verify_vault"));
        assert!(
            tools
                .iter()
                .any(|tool| tool.name == "business_application_servers")
        );
        assert!(
            tools
                .iter()
                .any(|tool| tool.name == "business_application_servers_cached")
        );
        assert!(
            tools
                .iter()
                .any(|tool| tool.name == "business_applications_for_server")
        );
        assert!(tools.iter().any(|tool| tool.name == "list_knowledge_bases"));
        assert!(tools.iter().any(|tool| tool.name == "list_categories"));
        assert!(tools.iter().any(|tool| tool.name == "kb_sync"));
        assert!(tools.iter().any(|tool| tool.name == "kb_list_tags"));
        assert!(tools.iter().any(|tool| tool.name == "kb_status"));
        assert!(tools.iter().any(|tool| tool.name == "kb_semantic_search"));
        assert!(tools.iter().any(|tool| tool.name == "kb_semantic_status"));
        assert!(tools.iter().any(|tool| tool.name == "kb_semantic_rebuild"));
        let get_article = tools
            .iter()
            .find(|tool| tool.name == "get_article")
            .unwrap();
        assert_eq!(
            get_article
                .input_schema
                .get("properties")
                .and_then(|props| props.get("fresh"))
                .and_then(|fresh| fresh.get("type"))
                .and_then(Value::as_str),
            Some("boolean")
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn daemon_mcp_get_record_fetches_change_request_by_table_sys_id() {
        let sys_id = "0123456789abcdef0123456789abcdef";
        let response = json!({
            "result": {
                "sys_id": sys_id,
                "number": "<CHG_NUMBER>",
                "short_description": "<SHORT_DESC>",
                "description": "<CHANGE_DESCRIPTION>",
                "state": "scheduled"
            }
        });
        let (instance_url, request_rx) =
            spawn_json_http_server(response).await.expect("http server");
        let fixture = build_fixture_state_at_instance(&instance_url)
            .await
            .expect("fixture");
        let server = McpServer::new(Arc::clone(&fixture.state), McpTransport::Stdio);

        let response = server
            .dispatch(JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: "tools/call".to_string(),
                params: json!({
                    "name": "get_record",
                    "arguments": {
                        "table": "change_request",
                        "sys_id": "0123456789ABCDEF0123456789ABCDEF"
                    }
                }),
                id: Some(json!(1)),
            })
            .await;

        let record = response
            .result
            .expect("record result")
            .get("record")
            .cloned()
            .expect("wrapped record");
        assert_eq!(record.get("sys_id").and_then(Value::as_str), Some(sys_id));
        assert_eq!(
            record.get("number").and_then(Value::as_str),
            Some("<CHG_NUMBER>")
        );
        assert_eq!(
            record.get("resource_type").and_then(Value::as_str),
            Some("change")
        );

        let request_line = request_rx.await.expect("request line");
        assert!(request_line.contains("/api/now/table/change_request/"));
        assert!(request_line.contains(sys_id));
    }

    #[test]
    fn resource_catalog_contains_expected_entries() {
        let resources = McpServer::resource_catalog();
        assert!(
            resources
                .iter()
                .any(|resource| resource.name == "ServiceNow dashboard"
                    && resource.uri == "snow://dashboard")
        );
        assert!(
            resources
                .iter()
                .any(|resource| resource.name == "ServiceNow record"
                    && resource.uri == "snow://records/{number}")
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn mcp_stdio_round_trip_exercises_tools_and_resources() {
        let fixture = build_fixture_state().await.expect("fixture");
        seed_kb_runtime_state(&fixture).expect("seed kb runtime state");
        let server = McpServer::new(Arc::clone(&fixture.state), McpTransport::Stdio);
        tokio::task::LocalSet::new()
            .run_until(async move {
                let (client_side, server_side) = duplex(4096);
                let (server_reader, server_writer) = split(server_side);

                tokio::task::spawn_local(async move {
                    let _ = server
                        .serve_streams(
                            BufReader::new(server_reader),
                            server_writer,
                            std::future::pending::<Result<(), std::io::Error>>(),
                        )
                        .await;
                });

                let (client_reader, mut client_writer) = split(client_side);
                let mut client_reader = BufReader::new(client_reader);

                client_writer
                    .write_all(br#"{"jsonrpc":"2.0","method":"initialize","id":1}"#)
                    .await
                    .expect("initialize write");
                client_writer.write_all(b"\n").await.expect("newline");

                client_writer
                    .write_all(br#"{"jsonrpc":"2.0","method":"tools/call","params":{"name":"get_record","arguments":{"number":"CHG001"}},"id":2}"#)
                    .await
                    .expect("get_record write");
                client_writer.write_all(b"\n").await.expect("newline");

                client_writer
                    .write_all(br#"{"jsonrpc":"2.0","method":"tools/call","params":{"name":"search_records","arguments":{"query":"Database"}},"id":3}"#)
                    .await
                    .expect("search write");
                client_writer.write_all(b"\n").await.expect("newline");
                client_writer
                    .write_all(br#"{"jsonrpc":"2.0","method":"tools/call","params":{"name":"search_knowledge","arguments":{"query":"database","knowledge_base":"IT"}},"id":7}"#)
                    .await
                    .expect("knowledge search write");
                client_writer.write_all(b"\n").await.expect("newline");
                client_writer
                    .write_all(br#"{"jsonrpc":"2.0","method":"tools/call","params":{"name":"kb_semantic_search","arguments":{"query":"database","knowledge_base":"IT","mode":"lexical","limit":5}},"id":11}"#)
                    .await
                    .expect("semantic knowledge search write");
                client_writer.write_all(b"\n").await.expect("newline");

                client_writer
                    .write_all(br#"{"jsonrpc":"2.0","method":"resources/read","params":{"uri":"snow://records/CHG001"},"id":4}"#)
                    .await
                    .expect("resource write");
                client_writer.write_all(b"\n").await.expect("newline");
                client_writer
                    .write_all(br#"{"jsonrpc":"2.0","method":"tools/call","params":{"name":"verify_vault","arguments":{}},"id":5}"#)
                    .await
                    .expect("verify write");
                client_writer.write_all(b"\n").await.expect("newline");
                client_writer
                    .write_all(br#"{"jsonrpc":"2.0","method":"tools/call","params":{"name":"get_approval","arguments":{"number":"APR001"}},"id":6}"#)
                    .await
                    .expect("approval write");
                client_writer.write_all(b"\n").await.expect("newline");
                client_writer
                    .write_all(br#"{"jsonrpc":"2.0","method":"tools/call","params":{"name":"kb_status","arguments":{}},"id":8}"#)
                    .await
                    .expect("kb status write");
                client_writer.write_all(b"\n").await.expect("newline");
                client_writer
                    .write_all(br#"{"jsonrpc":"2.0","method":"tools/call","params":{"name":"kb_list_tags","arguments":{"layer":"all","min_count":1}},"id":9}"#)
                    .await
                    .expect("kb tags write");
                client_writer.write_all(b"\n").await.expect("newline");
                client_writer
                    .write_all(br#"{"jsonrpc":"2.0","method":"tools/call","params":{"name":"kb_sync","arguments":{"full":true,"with_bodies":true}},"id":10}"#)
                    .await
                    .expect("kb sync write");
                client_writer.write_all(b"\n").await.expect("newline");
                client_writer
                    .write_all(br#"{"jsonrpc":"2.0","method":"tools/call","params":{"name":"kb_semantic_status","arguments":{}},"id":12}"#)
                    .await
                    .expect("semantic status write");
                client_writer.write_all(b"\n").await.expect("newline");
                client_writer
                    .write_all(br#"{"jsonrpc":"2.0","method":"tools/call","params":{"name":"kb_semantic_rebuild","arguments":{"full":true}},"id":13}"#)
                    .await
                    .expect("semantic rebuild write");
                client_writer.write_all(b"\n").await.expect("newline");

                let mut line = String::new();
                client_reader.read_line(&mut line).await.expect("initialize read");
                assert!(line.contains("protocolVersion"));
                line.clear();

                client_reader.read_line(&mut line).await.expect("get_record read");
                assert!(line.contains("CHG001"));
                line.clear();

                client_reader.read_line(&mut line).await.expect("search read");
                assert!(line.contains("Database patch") || line.contains("Database restart procedure"));
                line.clear();

                client_reader.read_line(&mut line).await.expect("knowledge search read");
                assert!(line.contains("KB001"));
                assert!(line.contains("Database restart procedure"));
                line.clear();

                client_reader
                    .read_line(&mut line)
                    .await
                    .expect("semantic search read");
                assert!(line.contains("\"hits\""));
                assert!(line.contains("\"mode\":\"lexical\""));
                assert!(line.contains("KB001"));
                line.clear();

                client_reader.read_line(&mut line).await.expect("resource read");
                assert!(line.contains("Database patch"));
                line.clear();

                client_reader.read_line(&mut line).await.expect("verify read");
                assert!(line.contains("scanned_documents"));
                line.clear();

                client_reader.read_line(&mut line).await.expect("approval read");
                assert!(line.contains("APR001"));
                line.clear();

                client_reader.read_line(&mut line).await.expect("kb status read");
                assert!(line.contains("body_cached_count"));
                line.clear();

                client_reader.read_line(&mut line).await.expect("kb tags read");
                assert!(line.contains("database"));
                line.clear();

                client_reader.read_line(&mut line).await.expect("kb sync read");
                assert!(line.contains("\"message\":\"service failure\""));
                line.clear();

                client_reader
                    .read_line(&mut line)
                    .await
                    .expect("semantic status read");
                assert!(line.contains("\"enabled\":false"));
                line.clear();

                client_reader
                    .read_line(&mut line)
                    .await
                    .expect("semantic rebuild read");
                assert!(line.contains("\"message\":\"service failure\""));
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn get_article_fresh_routes_through_live_service_now() {
        let response = json!({
            "result": [
                {
                    "sys_id": "kb-fresh-sys",
                    "number": "KB0105015",
                    "short_description": "Fresh KB title",
                    "description": "Fresh KB summary",
                    "article_body": "Fresh KB body",
                    "text": "Fresh KB body",
                    "state": "published",
                    "workflow_state": "published",
                    "article_type": "text",
                    "valid_to": "2027-01-01",
                    "knowledge_base": {
                        "sys_id": "kb-base-sys",
                        "value": "kb-base-sys",
                        "display_value": "IT Operations",
                        "table": "kb_knowledge_base"
                    },
                    "category": {
                        "sys_id": "kb-cat-sys",
                        "value": "kb-cat-sys",
                        "display_value": "Networking",
                        "table": "kb_category"
                    }
                }
            ]
        });
        let (instance_url, request_rx) =
            spawn_json_http_server(response).await.expect("http server");
        let fixture = build_fixture_state_at_instance(&instance_url)
            .await
            .expect("fixture");
        let server = McpServer::new(Arc::clone(&fixture.state), McpTransport::Stdio);

        tokio::task::LocalSet::new()
            .run_until(async move {
                let (client_side, server_side) = duplex(4096);
                let (server_reader, server_writer) = split(server_side);

                tokio::task::spawn_local(async move {
                    let _ = server
                        .serve_streams(
                            BufReader::new(server_reader),
                            server_writer,
                            std::future::pending::<Result<(), std::io::Error>>(),
                        )
                        .await;
                });

                let (client_reader, mut client_writer) = split(client_side);
                let mut client_reader = BufReader::new(client_reader);

                client_writer
                    .write_all(br#"{"jsonrpc":"2.0","method":"tools/call","params":{"name":"get_article","arguments":{"number":"KB0105015","fresh":true}},"id":1}"#)
                    .await
                    .expect("get_article write");
                client_writer.write_all(b"\n").await.expect("newline");

                let mut line = String::new();
                client_reader.read_line(&mut line).await.expect("read response");
                assert!(line.contains("KB0105015"));
                assert!(line.contains("Fresh KB body"));

                let request_line = request_rx.await.expect("request line");
                assert!(request_line.contains("/api/now/table/kb_knowledge"));
                assert!(request_line.contains("KB0105015"));
            })
            .await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn read_record_resource_refreshes_uncached_demand() {
        let response = json!({
            "result": [
                {
                    "sys_id": "dmnd-sys",
                    "number": "DMND0320098",
                    "short_description": "Network refresh demand",
                    "description": "Upgrade branch switching",
                    "state": "draft"
                }
            ]
        });
        let (instance_url, request_rx) =
            spawn_json_http_server(response).await.expect("http server");
        let fixture = build_fixture_state_at_instance(&instance_url)
            .await
            .expect("fixture");
        let server = McpServer::new(Arc::clone(&fixture.state), McpTransport::Stdio);

        tokio::task::LocalSet::new()
            .run_until(async move {
                let (client_side, server_side) = duplex(4096);
                let (server_reader, server_writer) = split(server_side);

                tokio::task::spawn_local(async move {
                    let _ = server
                        .serve_streams(
                            BufReader::new(server_reader),
                            server_writer,
                            std::future::pending::<Result<(), std::io::Error>>(),
                        )
                        .await;
                });

                let (client_reader, mut client_writer) = split(client_side);
                let mut client_reader = BufReader::new(client_reader);

                client_writer
                    .write_all(
                        br#"{"jsonrpc":"2.0","method":"resources/read","params":{"uri":"snow://records/DMND0320098"},"id":1}"#,
                    )
                    .await
                    .expect("resource read write");
                client_writer.write_all(b"\n").await.expect("newline");

                let mut line = String::new();
                client_reader.read_line(&mut line).await.expect("resource read");
                assert!(line.contains("DMND0320098"));
                assert!(line.contains("Network refresh demand"));
                assert!(line.contains("Upgrade branch switching"));

                let request_line = request_rx.await.expect("request line");
                assert!(request_line.contains("/api/now/table/dmn_demand"));
                assert!(request_line.contains("DMND0320098"));
            })
            .await;
    }

    fn seed_kb_runtime_state(
        fixture: &crate::test_support::FixtureState,
    ) -> std::result::Result<(), rusqlite::Error> {
        let conn = Connection::open(fixture.tempdir.path().join("snow.db"))?;
        conn.execute(
            r#"
            UPDATE kb_sync_state
            SET last_full_at = 1712649800000,
                last_incr_at = 1712650100000,
                watermark_updated_at = '2026-04-10 09:00:00',
                watermark_sys_id = 'kb-sys',
                kb_sync_lock = 1712650200000
            WHERE id = 1
            "#,
            [],
        )?;
        Ok(())
    }
}
