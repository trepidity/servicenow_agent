use std::future::Future;
use std::sync::Arc;

use serde::Deserialize;
use serde_json::{Map, Value, json};
use snow_core::query::filter::ListQuery;
use snow_core::vault::markdown::{
    render_approval_record, render_knowledge_article, render_snow_record,
};
use snow_core::{
    CacheSource, FieldValue, JournalEntry, KnowledgeArticle, KnowledgeEmbeddingCoverage,
    KnowledgeSearchFilters, KnowledgeSearchHit, KnowledgeSearchMode,
    KnowledgeSemanticSearchFilters, KnowledgeTagLayer, RecordRef, Reference, ResourceType,
    SearchScope, SnowCore, SnowRecord,
};
use tokio::io::{AsyncBufRead, AsyncWrite, BufReader};
use tokio::signal;

use crate::config::McpConfig;
use crate::domain::policy::is_write_tool;
use crate::planner::is_governed_write_tool;
use crate::planner::knowledge_grounding::{KbGroundedPlanner, content_hash};
use crate::protocol::schema::{
    JsonRpcRequest, JsonRpcResponse, PolicyDescribeReport, ToolCapabilitiesReport,
};
use crate::tools::ToolRegistry;
use crate::tools::records::{RESOURCE_PLAN_LOOKUP_TABLES, RecordLookup, parse_record_lookup};
use crate::transport::McpTransport;
use crate::{Error, Result};

#[derive(Debug, Clone, Default)]
pub struct ServeOptions;

#[derive(Clone)]
pub struct McpServer {
    core: Arc<SnowCore>,
    config: McpConfig,
    transport: McpTransport,
    registry: ToolRegistry,
}

impl McpServer {
    pub fn new(core: Arc<SnowCore>) -> Self {
        Self::with_config(core, McpConfig::default(), McpTransport::Stdio)
    }

    pub fn with_transport(core: Arc<SnowCore>, transport: McpTransport) -> Self {
        Self::with_config(core, McpConfig::default(), transport)
    }

    pub fn with_config(core: Arc<SnowCore>, config: McpConfig, transport: McpTransport) -> Self {
        Self {
            core,
            config,
            transport,
            registry: ToolRegistry::new(),
        }
    }

    pub fn builder(core: Arc<SnowCore>) -> McpServerBuilder {
        McpServerBuilder {
            core,
            config: McpConfig::default(),
            transport: McpTransport::Stdio,
        }
    }

    pub async fn serve(self) -> Result<()> {
        match self.transport {
            McpTransport::Stdio => self.serve_stdio().await,
            McpTransport::Http | McpTransport::Sse => {
                signal::ctrl_c().await?;
                Ok(())
            }
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

    pub async fn serve_streams<R, W, F>(&self, reader: R, writer: W, shutdown: F) -> Result<()>
    where
        R: AsyncBufRead + Unpin,
        W: AsyncWrite + Unpin,
        F: Future<Output = std::result::Result<(), std::io::Error>>,
    {
        crate::transport::stdio::StdioTransport::serve_streams(self, reader, writer, shutdown).await
    }

    pub async fn dispatch(&self, request: JsonRpcRequest) -> JsonRpcResponse {
        let id = request.id.clone();
        match request.method.as_str() {
            "initialize" => JsonRpcResponse::ok(
                id,
                json!({
                    "protocolVersion": "2024-11-05",
                    "serverInfo": { "name": "snow_mcp", "version": env!("CARGO_PKG_VERSION") },
                    "capabilities": {
                        "tools": { "listChanged": false },
                        "resources": { "subscribe": false, "listChanged": false }
                    }
                }),
            ),
            "tools/list" => {
                JsonRpcResponse::ok(id, json!({ "tools": self.registry.descriptors() }))
            }
            "resources/list" => {
                JsonRpcResponse::ok(id, json!({ "resources": self.registry.resources() }))
            }
            "tools/call" => self.call_tool(id, &request.params).await,
            "resources/read" => self.read_resource(id, &request.params).await,
            "tool_capabilities" => self.tool_capabilities_response(id),
            "policy_describe" => self.policy_describe_response(id),
            "redaction_rules_describe" => JsonRpcResponse::ok(
                id,
                json!({"rules_version": self.config.redaction.rules_version, "mode": self.config.redaction.phi_in_work_notes}),
            ),
            _ => JsonRpcResponse::error(id, -32601, "method not found", None),
        }
    }

    async fn call_tool(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let Some(name) = params.get("name").and_then(Value::as_str) else {
            return invalid_params(id, "missing tool name");
        };
        if is_governed_write_tool(name) {
            return daemon_required_for_write(id, name, "daemon required for governed write");
        }
        if name == "plan_get" {
            return daemon_required_for_write(id, name, "daemon required for plan persistence");
        }
        if is_write_tool(name) && !self.config.policy.is_tool_enabled(name) {
            return JsonRpcResponse::error(
                id,
                -32040,
                "policy denied",
                Some(
                    json!({ "details": "write tool is disabled by current MCP policy", "tool": name }),
                ),
            );
        }

        match name {
            "get_record" => self.call_get_record(id, params).await,
            "get_approval" => self.call_get_approval(id, params).await,
            "list_my_tasks" => self.call_list_my_tasks(id).await,
            "list_my_approvals" => self.call_list_my_approvals(id).await,
            "list_my_projects" => self.call_list_my_projects(id).await,
            "get_children" => self.call_get_children(id, params).await,
            "search_records" => self.call_search(id, params, SearchScope::All).await,
            "user_lookup" => self.call_user_lookup(id, params).await,
            "user_search" => self.call_user_search(id, params).await,
            "business_application_get" => self.call_business_application_get(id, params).await,
            "business_application_search" => {
                self.call_business_application_search(id, params).await
            }
            "business_application_query" => self.call_business_application_query(id, params).await,
            "business_application_servers" => {
                self.call_business_application_servers(id, params).await
            }
            "business_application_fields" => {
                self.call_business_application_fields(id, params).await
            }
            "server_get" => self.call_server_get(id, params).await,
            "server_search" => self.call_server_search(id, params).await,
            "server_query" => self.call_server_query(id, params).await,
            "server_fields" => self.call_server_fields(id, params).await,
            "search_knowledge" | "knowledge_search" => self.call_search_knowledge(id, params).await,
            "kb_semantic_search" => self.call_kb_semantic_search(id, params).await,
            "get_article" | "knowledge_fetch" => self.call_get_article(id, params).await,
            "knowledge_answer" => self.call_knowledge_answer(id, params).await,
            "knowledge_grounded_plan" => self.call_knowledge_grounded_plan(id, params).await,
            "list_records" => self.call_list_records(id, params).await,
            "list_knowledge_bases" => self.call_list_knowledge_bases(id),
            "list_categories" => self.call_list_categories(id, params),
            "list_knowledge_articles" => self.call_list_knowledge_articles(id, params).await,
            "vault_path" => JsonRpcResponse::ok(id, json!({ "path": self.core.vault_path() })),
            "kb_sync" => self.call_kb_sync(id, params).await,
            "kb_list_tags" => self.call_kb_list_tags(id, params),
            "kb_status" => self.call_kb_status(id),
            "kb_semantic_status" => self.call_kb_semantic_status(id).await,
            "kb_semantic_rebuild" => self.call_kb_semantic_rebuild(id, params).await,
            "repair_vault" => self.call_repair_vault(id).await,
            "rebuild_cache" => self.call_rebuild_cache(id),
            "verify_vault" => self.call_verify_vault(id),
            "get_work_notes" => self.call_get_work_notes(id, params).await,
            "attachment_list" => self.call_attachment_list(id, params).await,
            "resource_plan_get" => {
                self.call_get_record_with_allowed_tables(id, params, RESOURCE_PLAN_LOOKUP_TABLES)
                    .await
            }
            "story_get" => self.call_get_record_number(id, params).await,
            "story_tasks_list" => self.call_get_children(id, params).await,
            "timecard_list" => self.call_timecard_list(id, params).await,
            "work_note_plan_add" => self.call_work_note_plan_add(id, params).await,
            "catalog_items_search" => self.call_catalog_items_search(id, params).await,
            "catalog_item_get" => self.call_catalog_item_get(id, params).await,
            "plan_get" => self.not_implemented(id, name, "plan persistence is not enabled"),
            "audit_event_get" | "audit_events_search" | "audit_chain_verify" => self
                .not_implemented(
                    id,
                    name,
                    "audit read tools require an audit store configured",
                ),
            "policy_describe" => self.policy_describe_response(id),
            "tool_capabilities" => self.tool_capabilities_response(id),
            "redaction_rules_describe" => JsonRpcResponse::ok(
                id,
                json!({"rules_version": self.config.redaction.rules_version, "phi_in_work_notes": self.config.redaction.phi_in_work_notes}),
            ),
            _ => JsonRpcResponse::error(id, -32601, "tool not found", None),
        }
    }

    async fn read_resource(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let Some(uri) = params.get("uri").and_then(Value::as_str) else {
            return invalid_params(id, "missing uri");
        };

        match uri {
            u if u.starts_with("snow://records/") => {
                let number = u.trim_start_matches("snow://records/");
                match get_record_cached_or_fresh(self.core.as_ref(), number).await {
                    Ok(Some(record)) => JsonRpcResponse::ok(
                        id,
                        json!({"uri": uri, "mimeType": "text/markdown", "text": render_snow_record(&record)}),
                    ),
                    Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
                    Err(err) => service_failure(id, err),
                }
            }
            u if u.starts_with("snow://knowledge/") => {
                let number = u.trim_start_matches("snow://knowledge/");
                match self.core.get_knowledge_article(number).await {
                    Ok(Some(article)) => JsonRpcResponse::ok(
                        id,
                        json!({"uri": uri, "mimeType": "text/markdown", "text": render_knowledge_article(&article)}),
                    ),
                    Ok(None) => {
                        JsonRpcResponse::error(id, -32004, "knowledge article not found", None)
                    }
                    Err(err) => service_failure(id, err),
                }
            }
            "snow://dashboard" => {
                let tasks = match self.core.my_tasks().await {
                    Ok(records) => records,
                    Err(err) => return service_failure(id, err),
                };
                let approvals = match self.core.my_approvals().await {
                    Ok(records) => records,
                    Err(err) => return service_failure(id, err),
                };
                JsonRpcResponse::ok(
                    id,
                    json!({"uri": uri, "mimeType": "application/json", "json": {"tasks": tasks, "approvals": approvals}}),
                )
            }
            _ => JsonRpcResponse::error(id, -32601, "resource not found", None),
        }
    }

    async fn call_get_record(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        self.call_get_record_with_allowed_tables(
            id,
            params,
            snow_core::RECORD_LOOKUP_ALLOWED_TABLES,
        )
        .await
    }

    async fn call_get_record_with_allowed_tables(
        &self,
        id: Option<Value>,
        params: &Value,
        allowed_tables: &[&str],
    ) -> JsonRpcResponse {
        let Some(arguments) = params.get("arguments") else {
            return invalid_params(id, "missing arguments");
        };
        let lookup = match parse_record_lookup(arguments, allowed_tables) {
            Ok(lookup) => lookup,
            Err(err) => return invalid_params(id, err.to_string()),
        };

        match lookup {
            RecordLookup::Number(number) => self.get_record_by_number_response(id, &number).await,
            RecordLookup::TableSysId { table, sys_id } => {
                self.get_record_by_table_sys_id_response(id, &table, &sys_id)
                    .await
            }
        }
    }

    async fn call_get_record_number(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        if params
            .get("arguments")
            .and_then(Value::as_object)
            .is_some_and(|args| args.contains_key("table") || args.contains_key("sys_id"))
        {
            return invalid_params(id, "tool only accepts number-based lookups");
        }
        let Ok(number) = extract_argument_string(params, "number") else {
            return invalid_params(id, "missing number");
        };
        self.get_record_by_number_response(id, &number).await
    }

    async fn get_record_by_number_response(
        &self,
        id: Option<Value>,
        number: &str,
    ) -> JsonRpcResponse {
        match get_record_cached_or_fresh(self.core.as_ref(), number).await {
            Ok(Some(record)) => JsonRpcResponse::ok(id, json!({ "record": record })),
            Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
            Err(err) => service_failure(id, err),
        }
    }

    async fn get_record_by_table_sys_id_response(
        &self,
        id: Option<Value>,
        table: &str,
        sys_id: &str,
    ) -> JsonRpcResponse {
        match self
            .core
            .get_record_by_table_sys_id_fresh(table, sys_id)
            .await
        {
            Ok(Some(record)) => JsonRpcResponse::ok(id, json!({ "record": record })),
            Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_get_approval(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let Ok(number) = extract_argument_string(params, "number") else {
            return invalid_params(id, "missing number");
        };
        match self.core.get_approval(&number).await {
            Ok(Some(approval)) => JsonRpcResponse::ok(
                id,
                json!({"approval": approval, "markdown": render_approval_record(&approval)}),
            ),
            Ok(None) => JsonRpcResponse::error(id, -32004, "approval not found", None),
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_get_children(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        if params
            .get("arguments")
            .and_then(Value::as_object)
            .is_some_and(|args| args.contains_key("table") || args.contains_key("sys_id"))
        {
            return invalid_params(id, "tool only accepts number-based lookups");
        }
        let Ok(number) = extract_argument_string(params, "number") else {
            return invalid_params(id, "missing number");
        };
        match self.core.get_children(&number).await {
            Ok(records) => JsonRpcResponse::ok(id, json!({ "records": records, "parent": number })),
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_list_my_tasks(&self, id: Option<Value>) -> JsonRpcResponse {
        match self.core.my_tasks().await {
            Ok(records) if !records.is_empty() => {
                JsonRpcResponse::ok(id, json!({ "records": records }))
            }
            Ok(_) | Err(_) => match self.core.my_tasks_fresh().await {
                Ok(records) => JsonRpcResponse::ok(id, json!({ "records": records })),
                Err(err) => service_failure(id, err),
            },
        }
    }

    async fn call_list_my_approvals(&self, id: Option<Value>) -> JsonRpcResponse {
        match self.core.my_approvals().await {
            Ok(records) if !records.is_empty() => {
                JsonRpcResponse::ok(id, json!({ "records": records }))
            }
            Ok(_) | Err(_) => match self.core.my_approvals_fresh().await {
                Ok(records) => JsonRpcResponse::ok(id, json!({ "records": records })),
                Err(err) => service_failure(id, err),
            },
        }
    }

    async fn call_timecard_list(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let week = match extract_week_selector_from_arguments(params) {
            Ok(week) => week,
            Err(err) => return invalid_params(id, err),
        };
        match self.core.list_my_timecards(week).await {
            Ok(sheet) => JsonRpcResponse::ok(id, json!({ "time_sheet": sheet })),
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_list_my_projects(&self, id: Option<Value>) -> JsonRpcResponse {
        match self.core.my_projects().await {
            Ok(records) if !records.is_empty() => {
                JsonRpcResponse::ok(id, json!({ "records": records }))
            }
            Ok(_) | Err(_) => match self.core.my_projects_fresh().await {
                Ok(records) => JsonRpcResponse::ok(id, json!({ "records": records })),
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
        let Some(arguments) = params.get("arguments") else {
            return invalid_params(id, "missing arguments");
        };
        let Some(query) = arguments.get("query").and_then(Value::as_str) else {
            return invalid_params(id, "missing query");
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

        match self.core.search_enriched(query, effective_scope).await {
            Ok(results) => JsonRpcResponse::ok(
                id,
                json!({ "results": results.into_iter().take(limit).collect::<Vec<_>>() }),
            ),
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

        match self.core.lookup_user(parsed).await {
            Ok(Some(result)) => JsonRpcResponse::ok(id, json!(result)),
            Ok(None) => JsonRpcResponse::error(id, -32004, "user not found", None),
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_user_search(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let parsed: snow_core::UserSearch = match serde_json::from_value(arguments) {
            Ok(parsed) => parsed,
            Err(err) => return invalid_params(id, err),
        };
        if let Err(err) = parsed.validate() {
            return invalid_params(id, err);
        }

        match self.core.search_users(parsed).await {
            Ok(users) => JsonRpcResponse::ok(id, json!({ "users": users })),
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_business_application_search(
        &self,
        id: Option<Value>,
        params: &Value,
    ) -> JsonRpcResponse {
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
        match self.core.search_business_applications(parsed).await {
            Ok(records) => {
                let mut business_applications = Vec::new();
                let mut record_values = Vec::new();
                for record in records {
                    match self.business_application_json(&record) {
                        Ok(application) => {
                            if let Some(record_value) = application.get("record").cloned() {
                                record_values.push(record_value);
                            }
                            business_applications.push(application);
                        }
                        Err(err) => return service_failure(id, err),
                    }
                }
                JsonRpcResponse::ok(
                    id,
                    json!({
                        "business_applications": business_applications,
                        "records": record_values
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
            (Some(_), Some(_)) => {
                return invalid_params(id, "provide exactly one of sys_id or name");
            }
            (None, None) => return invalid_params(id, "missing required lookup: sys_id or name"),
        };
        match record {
            Some(record) => match self.business_application_json(&record) {
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
        let records = match self.core.query_business_applications(query).await {
            Ok(records) => records,
            Err(err) => return service_failure(id, err),
        };
        let mut applications = Vec::new();
        for record in records {
            match self.business_application_json(&record) {
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
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let parsed = match parse_business_application_servers_arguments(arguments) {
            Ok(parsed) => parsed,
            Err(err) => return invalid_params(id, err),
        };

        let result = match self
            .core
            .business_application_servers(core_business_application_servers_params(parsed))
            .await
        {
            Ok(Some(result)) => result,
            Ok(None) => {
                return JsonRpcResponse::error(id, -32004, "business application not found", None);
            }
            Err(err) => return service_failure(id, err),
        };

        let mut servers = Vec::with_capacity(result.servers.len());
        for server in result.servers {
            match self.server_json(&server.record) {
                Ok(server) => servers.push(server),
                Err(err) => return service_failure(id, err),
            }
        }

        JsonRpcResponse::ok(
            id,
            json!({
                "business_application": result.business_application,
                "servers": servers,
                "relationship_summary": result.relationship_summary,
                "diagnostics": result.diagnostics,
                "server_paths": result.server_paths,
            }),
        )
    }

    async fn call_business_application_fields(
        &self,
        id: Option<Value>,
        _params: &Value,
    ) -> JsonRpcResponse {
        let records = match self
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
        match self.core.search_servers(parsed).await {
            Ok(records) => {
                let mut servers = Vec::new();
                let mut record_values = Vec::new();
                for record in records {
                    match self.server_json(&record) {
                        Ok(server) => {
                            if let Some(record_value) = server.get("record").cloned() {
                                record_values.push(record_value);
                            }
                            servers.push(server);
                        }
                        Err(err) => return service_failure(id, err),
                    }
                }
                JsonRpcResponse::ok(id, json!({ "servers": servers, "records": record_values }))
            }
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_server_get(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
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
            .core
            .list_records_query(ListQuery::new().resource_type(ResourceType::Server))
            .await
        {
            Ok(records) => records,
            Err(err) => return service_failure(id, err),
        };
        let record = match (
            sys_id.filter(|value| !value.is_empty()),
            name.filter(|value| !value.is_empty()),
            ip_address.filter(|value| !value.is_empty()),
        ) {
            (Some(sys_id), None, None) => match snow_core::normalize_record_lookup_sys_id(sys_id) {
                Ok(sys_id) => records
                    .into_iter()
                    .find(|record| record.sys_id.eq_ignore_ascii_case(&sys_id)),
                Err(err) => return invalid_params(id, err),
            },
            (None, Some(name), None) => records
                .into_iter()
                .find(|record| server_name(record) == name),
            (None, None, Some(ip_address)) => records.into_iter().find(|record| {
                server_field(record, "ip_address")
                    .is_some_and(|value| value.eq_ignore_ascii_case(ip_address))
            }),
            (None, None, None) => {
                return invalid_params(id, "missing required lookup: sys_id, name, or ip_address");
            }
            _ => return invalid_params(id, "provide exactly one of sys_id, name, or ip_address"),
        };
        match record {
            Some(record) => match self.server_json(&record) {
                Ok(server) => JsonRpcResponse::ok(
                    id,
                    json!({
                        "server": server,
                        "markdown": render_snow_record(&record),
                    }),
                ),
                Err(err) => service_failure(id, err),
            },
            None => JsonRpcResponse::error(id, -32004, "server not found", None),
        }
    }

    async fn call_server_query(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
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
        let records = match self.core.query_servers(query).await {
            Ok(records) => records,
            Err(err) => return service_failure(id, err),
        };
        let mut servers = Vec::new();
        for record in records {
            match self.server_json(&record) {
                Ok(server) => servers.push(server),
                Err(err) => return service_failure(id, err),
            }
        }
        JsonRpcResponse::ok(id, json!({ "servers": servers }))
    }

    async fn call_server_fields(&self, id: Option<Value>, _params: &Value) -> JsonRpcResponse {
        let records = match self
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
        let Some(arguments) = params.get("arguments") else {
            return invalid_params(id, "missing arguments");
        };
        if arguments
            .get("mode")
            .and_then(Value::as_str)
            .is_some_and(|mode| mode != "lexical")
        {
            return self.call_kb_semantic_search(id, params).await;
        }
        let Some(query) = arguments.get("query").and_then(Value::as_str) else {
            return invalid_params(id, "missing query");
        };
        let filters = KnowledgeSearchFilters {
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
        match self.core.search_knowledge(query, filters).await {
            Ok(articles) => JsonRpcResponse::ok(id, json!({ "articles": articles })),
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_get_article(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let Ok(number) = extract_argument_string(params, "number") else {
            return invalid_params(id, "missing number");
        };
        let fresh = params
            .get("arguments")
            .and_then(|arguments| arguments.get("fresh"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let article = if fresh {
            self.core.get_knowledge_article_fresh(&number).await
        } else {
            self.core.get_knowledge_article(&number).await
        };
        match article {
            Ok(Some(article)) => JsonRpcResponse::ok(
                id,
                json!({"article": article, "markdown": render_knowledge_article(&article), "content_hash": content_hash(&article.content)}),
            ),
            Ok(None) => JsonRpcResponse::error(id, -32004, "knowledge article not found", None),
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_kb_semantic_search(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let Some(query) = arguments.get("query").and_then(Value::as_str) else {
            return invalid_params(id, "missing query");
        };
        let mut filters =
            match serde_json::from_value::<KnowledgeSemanticSearchFilters>(arguments.clone()) {
                Ok(filters) => filters,
                Err(err) => return invalid_params(id, err),
            };
        if filters.mode == KnowledgeSearchMode::Hybrid
            && arguments.get("mode").and_then(Value::as_str).is_none()
        {
            filters.mode = KnowledgeSearchMode::Lexical;
        }
        match self.core.search_knowledge_semantic(query, filters).await {
            Ok(hits) => {
                let mut wrapped_hits = Vec::with_capacity(hits.len());
                for hit in hits {
                    match self.knowledge_search_hit_json(&hit) {
                        Ok(hit) => wrapped_hits.push(hit),
                        Err(err) => return service_failure(id, err),
                    }
                }
                JsonRpcResponse::ok(id, json!({ "hits": wrapped_hits }))
            }
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_knowledge_answer(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let Some(arguments) = params.get("arguments") else {
            return invalid_params(id, "missing arguments");
        };
        let Some(query) = arguments.get("query").and_then(Value::as_str) else {
            return invalid_params(id, "missing query");
        };
        let limit = arguments
            .get("limit")
            .and_then(Value::as_u64)
            .map(|value| value as usize)
            .unwrap_or(5);
        let filters = KnowledgeSearchFilters {
            limit: Some(limit),
            ..Default::default()
        };
        match self.core.search_knowledge(query, filters).await {
            Ok(articles) => {
                let citations = articles
                    .iter()
                    .map(|article| {
                        json!({
                            "article_number": article.record.number,
                            "sys_id": article.record.sys_id,
                            "title": article.record.short_description,
                            "knowledge_base": article.knowledge_base.display_name,
                            "content_hash": content_hash(&article.content),
                        })
                    })
                    .collect::<Vec<_>>();
                JsonRpcResponse::ok(id, json!({ "articles": articles, "citations": citations }))
            }
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_knowledge_grounded_plan(
        &self,
        id: Option<Value>,
        params: &Value,
    ) -> JsonRpcResponse {
        let Some(arguments) = params.get("arguments") else {
            return invalid_params(id, "missing arguments");
        };
        let Some(query) = arguments.get("query").and_then(Value::as_str) else {
            return invalid_params(id, "missing query");
        };
        let filters = KnowledgeSearchFilters {
            limit: Some(5),
            ..Default::default()
        };
        match self.core.search_knowledge(query, filters).await {
            Ok(articles) => {
                let bodies = articles
                    .iter()
                    .map(|article| article.content.clone())
                    .collect::<Vec<_>>();
                let verdict = KbGroundedPlanner {
                    freshness_days: self.config.policy.kb_freshness_days,
                }
                .evaluate(Vec::new(), &bodies);
                JsonRpcResponse::ok(
                    id,
                    json!({"status": "needs_review", "evidence_verdict": verdict, "articles": articles}),
                )
            }
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_list_records(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
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
            match self.core.get_record(&parent_number).await {
                Ok(Some(parent)) => query = query.parent_sys_id(parent.sys_id),
                Ok(None) => {
                    return JsonRpcResponse::error(id, -32004, "parent record not found", None);
                }
                Err(err) => return service_failure(id, err),
            }
        }
        match self.core.list_records_query(query).await {
            Ok(records) => JsonRpcResponse::ok(id, json!({ "records": records })),
            Err(err) => service_failure(id, err),
        }
    }

    fn call_list_knowledge_bases(&self, id: Option<Value>) -> JsonRpcResponse {
        match self.core.list_knowledge_bases() {
            Ok(bases) => JsonRpcResponse::ok(id, json!({ "bases": bases })),
            Err(err) => service_failure(id, err),
        }
    }

    fn call_list_categories(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let Ok(knowledge_base_sys_id) = extract_argument_string(params, "knowledge_base_sys_id")
        else {
            return invalid_params(id, "missing knowledge_base_sys_id");
        };
        match self.core.list_categories(&knowledge_base_sys_id) {
            Ok(categories) => JsonRpcResponse::ok(id, json!({ "categories": categories })),
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_list_knowledge_articles(
        &self,
        id: Option<Value>,
        params: &Value,
    ) -> JsonRpcResponse {
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let parsed: ListKnowledgeArticlesArguments = match serde_json::from_value(arguments) {
            Ok(parsed) => parsed,
            Err(err) => return invalid_params(id, err),
        };
        match self
            .core
            .list_knowledge_articles(
                parsed.knowledge_base_sys_id.as_deref(),
                parsed.category_sys_id.as_deref(),
                parsed.limit,
            )
            .await
        {
            Ok(articles) => JsonRpcResponse::ok(id, json!({ "articles": articles })),
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_kb_sync(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let full = arguments
            .get("full")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        let with_bodies = arguments
            .get("with_bodies")
            .and_then(Value::as_bool)
            .unwrap_or(false);
        match self.core.sync_knowledge(full, with_bodies).await {
            Ok(sync) => JsonRpcResponse::ok(id, json!({ "sync": sync })),
            Err(err) => service_failure(id, err),
        }
    }

    fn call_kb_list_tags(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let arguments = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let layer = match arguments.get("layer").and_then(Value::as_str) {
            Some("all") | None => None,
            Some("sn") => Some(KnowledgeTagLayer::Sn),
            Some("auto") => Some(KnowledgeTagLayer::Auto),
            Some("user") => Some(KnowledgeTagLayer::User),
            Some(other) => return invalid_params(id, format!("unsupported tag layer `{other}`")),
        };
        let min_count = arguments
            .get("min_count")
            .and_then(Value::as_u64)
            .map(|value| value as usize)
            .unwrap_or(1);
        match self.core.list_knowledge_tags(layer, min_count) {
            Ok(tags) => JsonRpcResponse::ok(id, json!({ "tags": tags })),
            Err(err) => service_failure(id, err),
        }
    }

    fn call_kb_status(&self, id: Option<Value>) -> JsonRpcResponse {
        match self.core.knowledge_status() {
            Ok(status) => JsonRpcResponse::ok(id, json!({ "status": status })),
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_kb_semantic_status(&self, id: Option<Value>) -> JsonRpcResponse {
        match self.core.knowledge_semantic_status().await {
            Ok(status) => JsonRpcResponse::ok(id, json!({ "status": status })),
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_kb_semantic_rebuild(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let full = params
            .get("arguments")
            .and_then(|arguments| arguments.get("full"))
            .and_then(Value::as_bool)
            .unwrap_or(false);
        match self.core.rebuild_knowledge_semantic_index(full).await {
            Ok(summary) => JsonRpcResponse::ok(id, json!({ "summary": summary })),
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_repair_vault(&self, id: Option<Value>) -> JsonRpcResponse {
        match self.core.repair_vault().await {
            Ok(report) => JsonRpcResponse::ok(id, json!({ "report": report })),
            Err(err) => service_failure(id, err),
        }
    }

    fn call_rebuild_cache(&self, id: Option<Value>) -> JsonRpcResponse {
        match self.core.rebuild_cache() {
            Ok(report) => JsonRpcResponse::ok(id, json!({ "report": report })),
            Err(err) => service_failure(id, err),
        }
    }

    fn call_verify_vault(&self, id: Option<Value>) -> JsonRpcResponse {
        match self.core.verify_vault() {
            Ok(report) => JsonRpcResponse::ok(id, json!({ "report": report })),
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_get_work_notes(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let Some(arguments) = params.get("arguments") else {
            return invalid_params(id, "missing arguments");
        };
        let lookup = match parse_record_lookup(arguments, snow_core::RECORD_LOOKUP_ALLOWED_TABLES) {
            Ok(lookup) => lookup,
            Err(err) => return invalid_params(id, err.to_string()),
        };
        match get_record_by_lookup_cached_or_fresh(self.core.as_ref(), lookup).await {
            Ok(Some(record)) => JsonRpcResponse::ok(id, json!({ "work_notes": record.work_notes })),
            Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_attachment_list(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let Ok(number) = extract_argument_string(params, "number") else {
            return invalid_params(id, "missing number");
        };
        match self.core.list_attachments(&number).await {
            Ok(Some(attachments)) => JsonRpcResponse::ok(id, json!({ "attachments": attachments })),
            Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_work_note_plan_add(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let Ok(number) = extract_argument_string(params, "number") else {
            return invalid_params(id, "missing number");
        };
        let note = params
            .get("arguments")
            .and_then(|arguments| arguments.get("text"))
            .and_then(Value::as_str)
            .unwrap_or("");
        match get_record_cached_or_fresh(self.core.as_ref(), &number).await {
            Ok(Some(record)) => JsonRpcResponse::ok(
                id,
                json!({
                    "target": {"sys_id": record.sys_id, "number": record.number, "table": record.table},
                    "recent_work_note_count": record.work_notes.len(),
                    "field": "work_notes",
                    "preview": note,
                    "requires_confirmation": true
                }),
            ),
            Ok(None) => JsonRpcResponse::error(id, -32004, "record not found", None),
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_catalog_items_search(
        &self,
        id: Option<Value>,
        params: &Value,
    ) -> JsonRpcResponse {
        let Some(arguments) = params.get("arguments") else {
            return invalid_params(id, "missing arguments");
        };
        let Some(query) = arguments.get("query").and_then(Value::as_str) else {
            return invalid_params(id, "missing query");
        };
        let limit = arguments
            .get("limit")
            .and_then(Value::as_u64)
            .unwrap_or(10)
            .min(50) as u32;
        match self.core.search_catalog_items(query, limit).await {
            Ok(items) => JsonRpcResponse::ok(id, json!({ "items": items })),
            Err(err) => service_failure(id, err),
        }
    }

    async fn call_catalog_item_get(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let Some(arguments) = params.get("arguments") else {
            return invalid_params(id, "missing arguments");
        };
        let Some(sys_id) = arguments
            .get("sys_id")
            .or_else(|| arguments.get("item_sys_id"))
            .and_then(Value::as_str)
        else {
            return invalid_params(id, "missing sys_id");
        };
        match self.core.get_catalog_item(sys_id).await {
            Ok(item) => JsonRpcResponse::ok(id, json!({ "item": item })),
            Err(err) => service_failure(id, err),
        }
    }

    fn tool_capabilities_response(&self, id: Option<Value>) -> JsonRpcResponse {
        let tools =
            self.registry
                .metadata()
                .iter()
                .map(|metadata| {
                    metadata.capability(self.config.policy.tool_enabled_in_environment(
                        &metadata.name,
                        &self.config.environment.label,
                    ))
                })
                .collect::<Vec<_>>();
        JsonRpcResponse::ok(
            id,
            json!(ToolCapabilitiesReport {
                environment: self.config.environment.label.clone(),
                default_mode: self.config.policy.default_mode.clone(),
                tools,
            }),
        )
    }

    fn policy_describe_response(&self, id: Option<Value>) -> JsonRpcResponse {
        JsonRpcResponse::ok(
            id,
            json!(PolicyDescribeReport {
                environment: self.config.environment.label.clone(),
                default_mode: self.config.policy.default_mode.clone(),
                roles: self.config.policy.roles.keys().cloned().collect(),
                write_tools_enabled: self
                    .registry
                    .metadata()
                    .iter()
                    .filter(|metadata| {
                        is_write_tool(&metadata.name)
                            && self.config.policy.tool_enabled_in_environment(
                                &metadata.name,
                                &self.config.environment.label,
                            )
                    })
                    .map(|metadata| metadata.name.clone())
                    .collect(),
                kb_freshness_days: self.config.policy.kb_freshness_days,
                idempotency_window_seconds: self.config.policy.idempotency_window_seconds,
                phi_in_work_notes: self.config.policy.phi_in_work_notes.clone(),
                evaluation_order: crate::domain::policy::PolicyGate::evaluation_order()
                    .into_iter()
                    .map(|gate| gate.as_name().to_string())
                    .collect(),
            }),
        )
    }

    fn knowledge_search_hit_json(&self, hit: &KnowledgeSearchHit) -> Result<Value> {
        let mut hit_json = Map::new();
        hit_json.insert(
            "article".to_string(),
            self.knowledge_article_json(&hit.article)?,
        );
        hit_json.insert(
            "mode".to_string(),
            Value::String(knowledge_search_mode_json(hit.mode).to_string()),
        );
        hit_json.insert("score".to_string(), json!(hit.score));
        if let Some(semantic_score) = hit.semantic_score {
            hit_json.insert("semantic_score".to_string(), json!(semantic_score));
        }
        if let Some(lexical_score) = hit.lexical_score {
            hit_json.insert("lexical_score".to_string(), json!(lexical_score));
        }
        hit_json.insert(
            "coverage".to_string(),
            Value::String(knowledge_coverage_json(hit.coverage).to_string()),
        );
        Ok(Value::Object(hit_json))
    }

    fn knowledge_article_json(&self, article: &KnowledgeArticle) -> Result<Value> {
        let record = self.record_json(&article.record)?;
        let browser_url = record.get("browser_url").cloned().unwrap_or(Value::Null);
        let vault_relative_path = record
            .get("vault_relative_path")
            .cloned()
            .unwrap_or(Value::Null);
        let mut article_json = Map::new();
        if !browser_url.is_null() {
            article_json.insert("browser_url".to_string(), browser_url);
        }
        if !vault_relative_path.is_null() {
            article_json.insert("vault_relative_path".to_string(), vault_relative_path);
        }
        article_json.insert("record".to_string(), record);
        article_json.insert(
            "knowledge_base".to_string(),
            reference_json(&article.knowledge_base),
        );
        article_json.insert("category".to_string(), reference_json(&article.category));
        article_json.insert(
            "article_type".to_string(),
            Value::String(article.article_type.clone()),
        );
        article_json.insert(
            "content".to_string(),
            Value::String(article.content.clone()),
        );
        article_json.insert("sn_tags".to_string(), json!(article.sn_tags));
        article_json.insert("auto_tags".to_string(), json!(article.auto_tags));
        article_json.insert("user_tags".to_string(), json!(article.user_tags));
        article_json.insert("body_cached".to_string(), Value::Bool(article.body_cached));
        if let Some(published_at) = article.published_at {
            article_json.insert("published_at".to_string(), json!(published_at));
        }
        if let Some(author) = &article.author {
            article_json.insert("author".to_string(), reference_json(author));
        }
        if let Some(valid_to) = article.valid_to {
            article_json.insert("valid_to".to_string(), json!(valid_to));
        }
        Ok(Value::Object(article_json))
    }

    fn record_json(&self, record: &SnowRecord) -> Result<Value> {
        let mut record_json = Map::new();
        record_json.insert("sys_id".to_string(), Value::String(record.sys_id.clone()));
        record_json.insert("number".to_string(), Value::String(record.number.clone()));
        record_json.insert("table".to_string(), Value::String(record.table.clone()));
        record_json.insert(
            "resource_type".to_string(),
            Value::String(resource_type_json(&record.resource_type).to_string()),
        );
        record_json.insert("state".to_string(), Value::String(record.state.clone()));
        record_json.insert(
            "short_description".to_string(),
            Value::String(record.short_description.clone()),
        );
        record_json.insert(
            "description".to_string(),
            Value::String(record.description.clone()),
        );
        record_json.insert(
            "fields".to_string(),
            Value::Object(
                record
                    .fields
                    .iter()
                    .map(|(key, value)| (key.clone(), field_value_json(value)))
                    .collect(),
            ),
        );
        record_json.insert(
            "work_notes".to_string(),
            Value::Array(record.work_notes.iter().map(journal_entry_json).collect()),
        );
        record_json.insert(
            "comments".to_string(),
            Value::Array(record.comments.iter().map(journal_entry_json).collect()),
        );
        if let Some(parent) = &record.parent {
            record_json.insert("parent".to_string(), record_ref_json(parent));
        }
        record_json.insert(
            "children".to_string(),
            Value::Array(record.children.iter().map(record_ref_json).collect()),
        );
        record_json.insert(
            "references".to_string(),
            Value::Object(
                record
                    .references
                    .iter()
                    .map(|(key, reference)| (key.clone(), reference_json(reference)))
                    .collect(),
            ),
        );
        record_json.insert("synced_at".to_string(), json!(record.synced_at));
        record_json.insert(
            "source".to_string(),
            Value::String(cache_source_json(&record.source).to_string()),
        );
        if let Some(browser_url) = self.browser_url(&record.table, &record.sys_id) {
            record_json.insert("browser_url".to_string(), Value::String(browser_url));
        }
        if let Some(path) = self.core.vault_relative_path_for_sys_id(&record.sys_id)? {
            record_json.insert("vault_relative_path".to_string(), Value::String(path));
        }
        Ok(Value::Object(record_json))
    }

    fn business_application_json(&self, record: &SnowRecord) -> Result<Value> {
        let mut application = Map::new();
        let record_json = self.record_json(record)?;
        application.insert("record".to_string(), record_json.clone());
        application.insert(
            "name".to_string(),
            Value::String(business_application_name(record)),
        );
        for (json_name, field_name, table) in [
            ("business_owner", "business_owner", "sys_user"),
            ("is_owner", "it_application_owner", "sys_user"),
            ("ci_owner_group", "managed_by_group", "sys_user_group"),
            ("primary_support_group", "support_group", "sys_user_group"),
            ("primary_portfolio", "portfolio", ""),
        ] {
            if let Some(reference) = business_application_reference_json(record, field_name, table)
            {
                application.insert(json_name.to_string(), reference);
            }
        }
        if let Some(field) = record.fields.get("operational_status") {
            application.insert("operational_state".to_string(), field_value_json(field));
        }
        if let Some(field) = record.fields.get("attested_date") {
            application.insert(
                "attested_date".to_string(),
                Value::String(field.value.clone()),
            );
        }
        application.insert(
            "fields".to_string(),
            Value::Object(
                record
                    .fields
                    .iter()
                    .map(|(key, value)| (key.clone(), field_value_json(value)))
                    .collect(),
            ),
        );
        application.insert(
            "unresolved_references".to_string(),
            Value::Array(Vec::new()),
        );
        if let Some(browser_url) = record_json.get("browser_url").cloned() {
            application.insert("browser_url".to_string(), browser_url);
        }
        if let Some(path) = record_json.get("vault_relative_path").cloned() {
            application.insert("vault_relative_path".to_string(), path);
        }
        Ok(Value::Object(application))
    }

    fn server_json(&self, record: &SnowRecord) -> Result<Value> {
        let mut server = Map::new();
        let record_json = self.record_json(record)?;
        server.insert("record".to_string(), record_json.clone());
        server.insert("name".to_string(), Value::String(server_name(record)));
        if let Some(value) = server_field(record, "ip_address") {
            server.insert("ip_address".to_string(), Value::String(value));
        }
        if let Some(value) = server_field(record, "sys_class_name") {
            server.insert("class_name".to_string(), Value::String(value));
        }
        for (json_name, field_name, table) in [
            ("ci_owner_group", "managed_by_group", "sys_user_group"),
            ("support_group", "support_group", "sys_user_group"),
        ] {
            if let Some(reference) = server_reference_json(record, field_name, table) {
                server.insert(json_name.to_string(), reference);
            }
        }
        if let Some(field) = record.fields.get("operational_status") {
            server.insert("operational_status".to_string(), field_value_json(field));
        }
        server.insert(
            "fields".to_string(),
            Value::Object(
                record
                    .fields
                    .iter()
                    .map(|(key, value)| (key.clone(), field_value_json(value)))
                    .collect(),
            ),
        );
        if let Some(browser_url) = record_json.get("browser_url").cloned() {
            server.insert("browser_url".to_string(), browser_url);
        }
        if let Some(path) = record_json.get("vault_relative_path").cloned() {
            server.insert("vault_relative_path".to_string(), path);
        }
        Ok(Value::Object(server))
    }

    fn browser_url(&self, table: &str, sys_id: &str) -> Option<String> {
        let base = normalize_instance_url(&self.core.config().instance.url)?;
        Some(format!("{base}/nav_to.do?uri={table}.do?sys_id={sys_id}"))
    }

    fn not_implemented(&self, id: Option<Value>, tool: &str, details: &str) -> JsonRpcResponse {
        JsonRpcResponse::error(
            id,
            -32000,
            "not implemented",
            Some(json!({ "tool": tool, "details": details })),
        )
    }
}

pub struct McpServerBuilder {
    core: Arc<SnowCore>,
    config: McpConfig,
    transport: McpTransport,
}

impl McpServerBuilder {
    pub fn config(mut self, config: McpConfig) -> Self {
        self.config = config;
        self
    }

    pub fn transport(mut self, transport: McpTransport) -> Self {
        self.transport = transport;
        self
    }

    pub fn build(self) -> McpServer {
        McpServer::with_config(self.core, self.config, self.transport)
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
#[serde(deny_unknown_fields)]
struct BusinessApplicationServersArguments {
    #[serde(default)]
    number: Option<String>,
    #[serde(default)]
    sys_id: Option<String>,
    #[serde(default)]
    max_depth: Option<usize>,
    #[serde(default)]
    max_cis: Option<usize>,
    #[serde(default)]
    max_edges: Option<usize>,
    #[serde(default)]
    relationship_type: Vec<String>,
    #[serde(default)]
    include_paths: bool,
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
        "server"
        | "servers"
        | "cmdb_ci_server"
        | "cmdb_ci_linux_server"
        | "cmdb_ci_win_server"
        | "linux_server"
        | "windows_server" => Ok(ResourceType::Server),
        _ => Err(Error::InvalidParams(format!(
            "unsupported resource_type `{resource_type}`"
        ))),
    }
}

fn parse_search_scope(scope: &str) -> SearchScope {
    match scope.trim().to_ascii_lowercase().as_str() {
        "knowledge" => SearchScope::Knowledge,
        "work_notes" => SearchScope::WorkNotes,
        _ => SearchScope::All,
    }
}

async fn get_record_cached_or_fresh(
    core: &SnowCore,
    number: &str,
) -> anyhow::Result<Option<SnowRecord>> {
    match core.get_record(number).await? {
        Some(record) => Ok(Some(record)),
        None => core.get_record_fresh(number).await,
    }
}

async fn get_record_by_lookup_cached_or_fresh(
    core: &SnowCore,
    lookup: RecordLookup,
) -> anyhow::Result<Option<SnowRecord>> {
    match lookup {
        RecordLookup::Number(number) => get_record_cached_or_fresh(core, &number).await,
        RecordLookup::TableSysId { table, sys_id } => {
            core.get_record_by_table_sys_id_fresh(&table, &sys_id).await
        }
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

fn parse_business_application_servers_arguments(
    arguments: Value,
) -> std::result::Result<BusinessApplicationServersArguments, String> {
    let mut parsed: BusinessApplicationServersArguments =
        serde_json::from_value(arguments).map_err(|err| err.to_string())?;
    parsed.number = normalize_optional_string(parsed.number);
    parsed.sys_id = match normalize_optional_string(parsed.sys_id) {
        Some(sys_id) => Some(
            snow_core::normalize_record_lookup_sys_id(&sys_id).map_err(|err| err.to_string())?,
        ),
        None => None,
    };

    match (parsed.number.as_deref(), parsed.sys_id.as_deref()) {
        (Some(_), Some(_)) => return Err("provide exactly one of `number` or `sys_id`".to_string()),
        (None, None) => {
            return Err("missing required lookup: provide `number` or `sys_id`".to_string());
        }
        _ => {}
    }

    if let Some(number) = parsed.number.as_deref()
        && number.trim_start().starts_with("BA:")
    {
        return Err(
            "`number` must be a real Business Application number, not a local BA:<sys_id> fallback"
                .to_string(),
        );
    }

    validate_optional_limit(parsed.max_depth, "max_depth", 1, 4)?;
    validate_optional_limit(parsed.max_cis, "max_cis", 1, 5000)?;
    validate_optional_limit(parsed.max_edges, "max_edges", 1, 20000)?;

    let mut relationship_type = Vec::with_capacity(parsed.relationship_type.len());
    for value in parsed.relationship_type {
        let value = value.trim();
        if value.is_empty() {
            return Err("`relationship_type` values must not be empty".to_string());
        }
        relationship_type.push(value.to_string());
    }
    parsed.relationship_type = relationship_type;

    let _ = parsed.include_paths;
    Ok(parsed)
}

fn core_business_application_servers_params(
    args: BusinessApplicationServersArguments,
) -> snow_core::BusinessApplicationServersParams {
    snow_core::BusinessApplicationServersParams {
        number: args.number,
        sys_id: args.sys_id,
        max_depth: args.max_depth,
        max_cis: args.max_cis,
        max_edges: args.max_edges,
        relationship_type: args.relationship_type,
        include_paths: args.include_paths,
    }
}

fn normalize_optional_string(value: Option<String>) -> Option<String> {
    value
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn validate_optional_limit(
    value: Option<usize>,
    field: &str,
    minimum: usize,
    maximum: usize,
) -> std::result::Result<(), String> {
    if let Some(value) = value {
        if value < minimum {
            return Err(format!("`{field}` must be at least {minimum}"));
        }
        if value > maximum {
            return Err(format!("`{field}` must be at most {maximum}"));
        }
    }
    Ok(())
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

fn business_application_reference_json(
    record: &SnowRecord,
    field_name: &str,
    table: &str,
) -> Option<Value> {
    let field = record.fields.get(field_name)?;
    let sys_id = field.value.trim();
    if sys_id.is_empty() {
        return None;
    }
    Some(json!({
        "sys_id": sys_id,
        "table": table,
        "display_name": field
            .display_value
            .as_deref()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or(sys_id),
        "extra": {},
    }))
}

fn server_name(record: &SnowRecord) -> String {
    server_field(record, "name")
        .or_else(|| {
            (!record.short_description.trim().is_empty()).then(|| record.short_description.clone())
        })
        .unwrap_or_else(|| record.sys_id.clone())
}

fn server_field(record: &SnowRecord, field_name: &str) -> Option<String> {
    record.fields.get(field_name).and_then(|field| {
        field
            .display_value
            .as_ref()
            .or(Some(&field.value))
            .map(|value| value.trim())
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
    })
}

fn server_reference_json(record: &SnowRecord, field_name: &str, table: &str) -> Option<Value> {
    if let Some(reference) = record.references.get(field_name) {
        return Some(json!({
            "sys_id": reference.sys_id,
            "table": reference.table,
            "display_name": reference.display_name,
            "extra": reference.extra,
        }));
    }
    business_application_reference_json(record, field_name, table)
}

fn field_value_json(value: &FieldValue) -> Value {
    let mut field = Map::new();
    field.insert("value".to_string(), Value::String(value.value.clone()));
    if let Some(display_value) = &value.display_value {
        field.insert(
            "display_value".to_string(),
            Value::String(display_value.clone()),
        );
    }
    Value::Object(field)
}

fn journal_entry_json(entry: &JournalEntry) -> Value {
    json!({
        "timestamp": entry.timestamp,
        "author": entry.author,
        "body": entry.body,
    })
}

fn reference_json(reference: &Reference) -> Value {
    json!({
        "sys_id": reference.sys_id,
        "table": reference.table,
        "display_name": reference.display_name,
        "extra": reference.extra,
    })
}

fn record_ref_json(reference: &RecordRef) -> Value {
    json!({
        "sys_id": reference.sys_id,
        "number": reference.number,
        "table": reference.table,
    })
}

fn knowledge_search_mode_json(mode: KnowledgeSearchMode) -> &'static str {
    match mode {
        KnowledgeSearchMode::Lexical => "lexical",
        KnowledgeSearchMode::Semantic => "semantic",
        KnowledgeSearchMode::Hybrid => "hybrid",
    }
}

fn knowledge_coverage_json(coverage: KnowledgeEmbeddingCoverage) -> &'static str {
    match coverage {
        KnowledgeEmbeddingCoverage::Metadata => "metadata",
        KnowledgeEmbeddingCoverage::FullText => "full_text",
    }
}

fn resource_type_json(resource_type: &ResourceType) -> &'static str {
    match resource_type {
        ResourceType::Task => "task",
        ResourceType::Incident => "incident",
        ResourceType::Change => "change",
        ResourceType::ChangeTask => "change_task",
        ResourceType::Request => "request",
        ResourceType::RequestTask => "request_task",
        ResourceType::Project => "project",
        ResourceType::Demand => "demand",
        ResourceType::DemandTask => "demand_task",
        ResourceType::ResourcePlan => "resource_plan",
        ResourceType::Story => "story",
        ResourceType::ScrumTask => "scrum_task",
        ResourceType::Timecard => "timecard",
        ResourceType::Knowledge => "knowledge",
        ResourceType::Approval => "approval",
        ResourceType::BusinessApplication => "business_application",
        ResourceType::Server => "server",
    }
}

fn cache_source_json(source: &CacheSource) -> &'static str {
    match source {
        CacheSource::Memory => "memory",
        CacheSource::Disk => "disk",
        CacheSource::Api => "api",
    }
}

fn normalize_instance_url(instance_url: &str) -> Option<String> {
    let trimmed = instance_url.trim().trim_end_matches('/');
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        Some(trimmed.to_string())
    } else {
        Some(format!("https://{trimmed}"))
    }
}

fn extract_argument_string(params: &Value, key: &str) -> Result<String> {
    params
        .get("arguments")
        .and_then(|arguments| arguments.get(key))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .ok_or_else(|| Error::InvalidParams(format!("missing {key}")))
}

fn extract_week_selector_from_arguments(
    params: &Value,
) -> Result<snow_core::resource::timecard::WeekSelector> {
    let Some(week) = params
        .get("arguments")
        .and_then(|arguments| arguments.get("week"))
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|week| !week.is_empty())
    else {
        return Ok(snow_core::resource::timecard::WeekSelector::Current);
    };
    let date = chrono::NaiveDate::parse_from_str(week, "%Y-%m-%d")
        .map_err(|_| Error::InvalidParams("week must be YYYY-MM-DD".to_string()))?;
    Ok(snow_core::resource::timecard::WeekSelector::Date(date))
}

fn invalid_params(id: Option<Value>, err: impl ToString) -> JsonRpcResponse {
    JsonRpcResponse::error(
        id,
        -32602,
        "invalid params",
        Some(json!({ "details": err.to_string() })),
    )
}

fn daemon_required_for_write(id: Option<Value>, tool: &str, details: &str) -> JsonRpcResponse {
    JsonRpcResponse::error(
        id,
        -32044,
        "DAEMON_REQUIRED_FOR_WRITE",
        Some(json!({
            "tool_name": tool,
            "reason": "daemon_not_attached",
            "details": details,
        })),
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
