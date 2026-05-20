use std::collections::HashSet;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use serde_json::{Value, json};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use tokio::signal;
use tokio::sync::Mutex;

use crate::config::McpConfig;
use crate::domain::policy::is_write_tool;
use crate::planner::is_governed_story_tool;
use crate::protocol::schema::{
    JsonRpcRequest, JsonRpcResponse, ToolCapabilitiesReport, ToolCapability,
};
use crate::tools::ToolRegistry;
use crate::{Error, Result};

const JSON_RPC_VERSION: &str = "2.0";
const DEFAULT_CONTRACT_VERSION: &str = "daemon-json-rpc-v1";

const BRIDGE_TOOL_METHODS: &[(&str, &str)] = &[
    ("get_record", "get_record"),
    ("get_approval", "get_approval"),
    ("search_records", "search_records"),
    ("list_records", "list_records"),
    ("list_my_tasks", "list_my_tasks"),
    ("list_my_approvals", "list_my_approvals"),
    ("list_my_projects", "list_my_projects"),
    ("get_children", "get_children"),
    ("get_work_notes", "get_work_notes"),
    ("resource_plan_get", "get_record"),
    ("story_get", "get_record"),
    ("story_tasks_list", "get_children"),
    ("story_plan_create", "story_plan_create"),
    ("story_apply_create", "story_apply_create"),
    ("story_plan_update", "story_plan_update"),
    ("story_apply_update", "story_apply_update"),
    ("story_task_plan_create", "story_task_plan_create"),
    ("story_task_apply_create", "story_task_apply_create"),
    ("story_task_plan_update", "story_task_plan_update"),
    ("story_task_apply_update", "story_task_apply_update"),
    ("plan_get", "plan_get"),
    ("search_knowledge", "search_knowledge"),
    ("knowledge_search", "search_knowledge"),
    ("kb_semantic_search", "kb_semantic_search"),
    ("get_article", "get_article"),
    ("knowledge_fetch", "get_article"),
    ("list_knowledge_bases", "list_knowledge_bases"),
    ("list_categories", "list_categories"),
    ("list_knowledge_articles", "list_knowledge_articles"),
    ("vault_path", "vault_path"),
];

const LOCAL_GOVERNANCE_TOOLS: &[&str] = &[
    "policy_describe",
    "tool_capabilities",
    "redaction_rules_describe",
];

#[async_trait]
pub trait DaemonJsonRpcClient: Send + Sync {
    async fn request(&self, method: &str, params: Value) -> Result<Value>;
}

#[derive(Debug)]
pub struct UnixDaemonJsonRpcClient {
    socket_path: PathBuf,
    next_id: AtomicU64,
}

impl UnixDaemonJsonRpcClient {
    pub fn new(socket_path: impl Into<PathBuf>) -> Self {
        Self {
            socket_path: socket_path.into(),
            next_id: AtomicU64::new(1),
        }
    }

    pub fn socket_path(&self) -> &Path {
        &self.socket_path
    }
}

#[async_trait]
impl DaemonJsonRpcClient for UnixDaemonJsonRpcClient {
    async fn request(&self, method: &str, params: Value) -> Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let payload = json!({
            "jsonrpc": JSON_RPC_VERSION,
            "id": id,
            "method": method,
            "params": params,
        });

        let stream = UnixStream::connect(&self.socket_path).await?;
        let (reader, mut writer) = stream.into_split();
        writer
            .write_all(serde_json::to_string(&payload)?.as_bytes())
            .await?;
        writer.write_all(b"\n").await?;
        writer.flush().await?;

        let mut reader = BufReader::new(reader);
        let mut line = String::new();
        if reader.read_line(&mut line).await? == 0 {
            return Err(Error::InvalidParams(
                "daemon closed JSON-RPC connection before responding".to_string(),
            ));
        }

        let response: Value = serde_json::from_str(&line)?;
        if response.get("jsonrpc").and_then(Value::as_str) != Some(JSON_RPC_VERSION) {
            return Err(Error::InvalidParams(
                "daemon returned invalid JSON-RPC version".to_string(),
            ));
        }
        if response.get("id").and_then(Value::as_u64) != Some(id) {
            return Err(Error::InvalidParams(
                "daemon returned mismatched JSON-RPC id".to_string(),
            ));
        }
        if let Some(error) = response.get("error") {
            return Err(Error::DaemonJsonRpc {
                code: error.get("code").and_then(Value::as_i64).unwrap_or(-32000),
                message: error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("daemon request failed")
                    .to_string(),
                data: error.get("data").cloned(),
            });
        }
        Ok(response.get("result").cloned().unwrap_or(Value::Null))
    }
}

#[derive(Debug, Clone)]
struct DaemonContract {
    environment: String,
    supported_methods: HashSet<String>,
    deprecated_methods: HashSet<String>,
}

#[derive(Clone)]
pub struct DaemonBackedMcpBridge {
    daemon: Arc<dyn DaemonJsonRpcClient>,
    config: McpConfig,
    registry: ToolRegistry,
    required_contract_version: String,
    contract: Arc<Mutex<Option<DaemonContract>>>,
}

impl DaemonBackedMcpBridge {
    pub fn new(
        daemon: Arc<dyn DaemonJsonRpcClient>,
        config: McpConfig,
        required_contract_version: impl Into<String>,
    ) -> Self {
        Self {
            daemon,
            config,
            registry: ToolRegistry::new(),
            required_contract_version: required_contract_version.into(),
            contract: Arc::new(Mutex::new(None)),
        }
    }

    pub fn from_socket(socket_path: impl Into<PathBuf>) -> Self {
        Self::new(
            Arc::new(UnixDaemonJsonRpcClient::new(socket_path)),
            McpConfig::default(),
            DEFAULT_CONTRACT_VERSION,
        )
    }

    pub async fn serve_stdio(self) -> Result<()> {
        self.serve_streams(
            BufReader::new(tokio::io::stdin()),
            tokio::io::stdout(),
            signal::ctrl_c(),
        )
        .await
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
        F: Future<Output = std::result::Result<(), std::io::Error>>,
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
                    let response = match serde_json::from_str::<JsonRpcRequest>(&line) {
                        Ok(request) => self.dispatch(request).await,
                        Err(err) => JsonRpcResponse::error(
                            None,
                            -32700,
                            "parse error",
                            Some(json!({ "details": err.to_string() })),
                        ),
                    };
                    if response.id.is_some() {
                        writer.write_all(serde_json::to_vec(&response)?.as_slice()).await?;
                        writer.write_all(b"\n").await?;
                        writer.flush().await?;
                    }
                    line.clear();
                }
            }
        }

        Ok(())
    }

    pub async fn dispatch(&self, request: JsonRpcRequest) -> JsonRpcResponse {
        let id = request.id.clone();
        match request.method.as_str() {
            "initialize" => JsonRpcResponse::ok(
                id,
                json!({
                    "protocolVersion": "2024-11-05",
                    "serverInfo": {
                        "name": "snow_mcp_bridge",
                        "version": env!("CARGO_PKG_VERSION")
                    },
                    "capabilities": {
                        "tools": { "listChanged": false },
                        "resources": { "subscribe": false, "listChanged": false }
                    }
                }),
            ),
            "notifications/initialized" => JsonRpcResponse::ok(None, Value::Null),
            "tools/list" => match self.available_tools().await {
                Ok(tools) => JsonRpcResponse::ok(id, json!({ "tools": tools })),
                Err(err) => service_failure(id, err),
            },
            "resources/list" => match self.available_resources().await {
                Ok(resources) => JsonRpcResponse::ok(id, json!({ "resources": resources })),
                Err(err) => service_failure(id, err),
            },
            "tools/call" => self.call_tool(id, &request.params).await,
            "resources/read" => self.read_resource(id, &request.params).await,
            "tool_capabilities" => match self.tool_capabilities().await {
                Ok(report) => JsonRpcResponse::ok(id, json!(report)),
                Err(err) => service_failure(id, err),
            },
            "policy_describe" => self.policy_describe_response(id),
            "redaction_rules_describe" => JsonRpcResponse::ok(
                id,
                json!({
                    "rules_version": self.config.redaction.rules_version,
                    "mode": self.config.redaction.phi_in_work_notes,
                }),
            ),
            _ => JsonRpcResponse::error(id, -32601, "method not found", None),
        }
    }

    async fn call_tool(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let Some(name) = params.get("name").and_then(Value::as_str) else {
            return invalid_params(id, "missing tool name");
        };

        match name {
            "tool_capabilities" => match self.tool_capabilities().await {
                Ok(report) => return JsonRpcResponse::ok(id, tool_call_result(json!(report))),
                Err(err) => return service_failure(id, err),
            },
            "policy_describe" => {
                return JsonRpcResponse::ok(id, tool_call_result(self.policy_describe_payload()));
            }
            "redaction_rules_describe" => {
                return JsonRpcResponse::ok(
                    id,
                    tool_call_result(json!({
                        "rules_version": self.config.redaction.rules_version,
                        "phi_in_work_notes": self.config.redaction.phi_in_work_notes,
                    })),
                );
            }
            _ => {}
        }

        if is_write_tool(name) && !is_governed_story_tool(name) {
            return JsonRpcResponse::error(
                id,
                -32040,
                "policy denied",
                Some(json!({ "details": "daemon-backed MCP bridge is read-only", "tool": name })),
            );
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

        let Some(method) = canonical_daemon_method(name) else {
            return JsonRpcResponse::error(id, -32601, "tool not found", None);
        };

        let contract = match self.contract().await {
            Ok(contract) => contract,
            Err(err) => return service_failure(id, err),
        };
        if !method_available(&contract, method) {
            return JsonRpcResponse::error(
                id,
                -32041,
                "daemon method unavailable",
                Some(json!({ "tool": name, "daemon_method": method })),
            );
        }

        let args = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        match self
            .daemon
            .request(method, daemon_params_for(method, args))
            .await
        {
            Ok(result) => JsonRpcResponse::ok(id, tool_call_result(result)),
            Err(err) => service_failure(id, err),
        }
    }

    async fn read_resource(&self, id: Option<Value>, params: &Value) -> JsonRpcResponse {
        let Some(uri) = params.get("uri").and_then(Value::as_str) else {
            return invalid_params(id, "missing uri");
        };

        let request = if let Some(number) = uri.strip_prefix("snow://records/") {
            Some(("get_record", json!({ "number": number })))
        } else if let Some(number) = uri.strip_prefix("snow://knowledge/") {
            Some(("get_article", json!({ "number": number })))
        } else if uri == "snow://dashboard" {
            Some(("dashboard", json!({})))
        } else {
            None
        };

        let Some((method, params)) = request else {
            return JsonRpcResponse::error(id, -32601, "resource not found", None);
        };
        let contract = match self.contract().await {
            Ok(contract) => contract,
            Err(err) => return service_failure(id, err),
        };
        let dashboard_methods = ["list_my_tasks", "list_my_approvals", "list_my_projects"];
        let method_is_available = if method == "dashboard" {
            dashboard_methods
                .iter()
                .any(|method| method_available(&contract, method))
        } else {
            method_available(&contract, method)
        };
        if !method_is_available {
            return JsonRpcResponse::error(
                id,
                -32041,
                "daemon method unavailable",
                Some(json!({ "resource": uri, "daemon_method": method })),
            );
        }

        let result = if method == "dashboard" {
            let mut dashboard = serde_json::Map::new();
            for dashboard_method in dashboard_methods {
                if method_available(&contract, dashboard_method) {
                    match self.daemon.request(dashboard_method, json!({})).await {
                        Ok(result) => {
                            dashboard.insert(dashboard_key(dashboard_method).to_string(), result);
                        }
                        Err(err) => return service_failure(id, err),
                    }
                }
            }
            Ok(Value::Object(dashboard))
        } else {
            self.daemon.request(method, params).await
        };

        match result {
            Ok(result) => {
                let text = result
                    .get("markdown")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned)
                    .unwrap_or_else(|| serde_json::to_string_pretty(&result).unwrap_or_default());
                let mime_type = if result.get("markdown").is_some() {
                    "text/markdown"
                } else {
                    "application/json"
                };
                JsonRpcResponse::ok(
                    id,
                    json!({ "uri": uri, "mimeType": mime_type, "text": text }),
                )
            }
            Err(err) => service_failure(id, err),
        }
    }

    async fn available_tools(&self) -> Result<Vec<crate::protocol::schema::McpToolDescriptor>> {
        let contract = self.contract().await?;
        Ok(self
            .registry
            .metadata()
            .iter()
            .filter(|tool| {
                LOCAL_GOVERNANCE_TOOLS.contains(&tool.name.as_str())
                    || canonical_daemon_method(&tool.name)
                        .map(|method| method_available(&contract, method))
                        .unwrap_or(false)
            })
            .map(|tool| tool.descriptor())
            .collect())
    }

    async fn available_resources(
        &self,
    ) -> Result<Vec<crate::protocol::schema::McpResourceDescriptor>> {
        let contract = self.contract().await?;
        Ok(self
            .registry
            .resources()
            .into_iter()
            .filter(|resource| {
                if resource.uri.starts_with("snow://records/") {
                    method_available(&contract, "get_record")
                } else if resource.uri.starts_with("snow://knowledge/") {
                    method_available(&contract, "get_article")
                } else {
                    method_available(&contract, "list_my_tasks")
                        && method_available(&contract, "list_my_approvals")
                }
            })
            .collect())
    }

    async fn tool_capabilities(&self) -> Result<ToolCapabilitiesReport> {
        let contract = self.contract().await?;
        let mut tools = Vec::new();
        for tool in self.registry.metadata() {
            let enabled = LOCAL_GOVERNANCE_TOOLS.contains(&tool.name.as_str())
                || canonical_daemon_method(&tool.name)
                    .map(|method| method_available(&contract, method))
                    .unwrap_or(false);
            if enabled {
                tools.push(ToolCapability {
                    name: tool.name.clone(),
                    enabled,
                    mode: if is_write_tool(&tool.name) {
                        "write".to_string()
                    } else {
                        "read".to_string()
                    },
                    read_only: true,
                    requires_confirmation: tool.requires_confirmation,
                });
            }
        }

        Ok(ToolCapabilitiesReport {
            environment: contract.environment,
            default_mode: "read_only".to_string(),
            tools,
        })
    }

    fn policy_describe_payload(&self) -> Value {
        json!(
            self.config
                .policy
                .policy_describe(self.config.redaction.rules_version.clone())
        )
    }

    fn policy_describe_response(&self, id: Option<Value>) -> JsonRpcResponse {
        JsonRpcResponse::ok(id, self.policy_describe_payload())
    }

    async fn contract(&self) -> Result<DaemonContract> {
        let mut guard = self.contract.lock().await;
        if let Some(contract) = guard.clone() {
            return Ok(contract);
        }

        let raw = self.daemon.request("contract_info", json!({})).await?;
        let contract_version = raw
            .get("contract_version")
            .and_then(Value::as_str)
            .ok_or_else(|| {
                Error::InvalidParams("contract_info missing contract_version".to_string())
            })?
            .to_string();
        if contract_version != self.required_contract_version {
            return Err(Error::InvalidParams(format!(
                "incompatible daemon contract: expected {}, got {contract_version}",
                self.required_contract_version
            )));
        }

        let supported_methods = raw
            .get("supported_methods")
            .and_then(Value::as_array)
            .map(|methods| {
                methods
                    .iter()
                    .filter_map(Value::as_str)
                    .map(ToOwned::to_owned)
                    .collect()
            })
            .unwrap_or_default();
        let deprecated_methods = raw
            .get("deprecated_aliases")
            .map(deprecated_method_set)
            .unwrap_or_default();
        let environment = raw
            .get("environment")
            .and_then(|env| env.get("label"))
            .and_then(Value::as_str)
            .unwrap_or(self.config.environment.label.as_str())
            .to_string();

        let contract = DaemonContract {
            environment,
            supported_methods,
            deprecated_methods,
        };
        *guard = Some(contract.clone());
        Ok(contract)
    }
}

fn canonical_daemon_method(tool: &str) -> Option<&'static str> {
    BRIDGE_TOOL_METHODS
        .iter()
        .find_map(|(name, method)| (*name == tool).then_some(*method))
}

fn method_available(contract: &DaemonContract, method: &str) -> bool {
    contract.supported_methods.contains(method) && !contract.deprecated_methods.contains(method)
}

fn dashboard_key(method: &str) -> &'static str {
    match method {
        "list_my_tasks" => "tasks",
        "list_my_approvals" => "approvals",
        "list_my_projects" => "projects",
        _ => "unknown",
    }
}

fn daemon_params_for(method: &str, args: Value) -> Value {
    let mut object = match args {
        Value::Object(map) => map,
        _ => return json!({}),
    };

    if method == "get_record" && !object.contains_key("number") {
        if let Some(record_number) = object.get("record_number").and_then(Value::as_str) {
            object.insert(
                "number".to_string(),
                Value::String(record_number.to_string()),
            );
        }
    }
    if method == "get_children" && !object.contains_key("number") {
        let parent = object
            .get("parent_number")
            .or_else(|| object.get("parent_record_number"))
            .and_then(Value::as_str)
            .map(ToOwned::to_owned);
        if let Some(parent) = parent {
            object.insert("number".to_string(), Value::String(parent));
        }
    }

    Value::Object(object)
}

/// Wrap a daemon/governance payload in the MCP `tools/call` result envelope.
///
/// MCP clients expect `tools/call` results to carry a `content` array; a bare
/// payload renders as empty output. The raw value is preserved under
/// `structuredContent` for structured consumers.
fn tool_call_result(value: Value) -> Value {
    let text = value
        .get("markdown")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| serde_json::to_string_pretty(&value).unwrap_or_default());
    json!({
        "content": [{ "type": "text", "text": text }],
        "structuredContent": value,
        "isError": false,
    })
}

fn deprecated_method_set(value: &Value) -> HashSet<String> {
    match value {
        Value::Array(items) => items
            .iter()
            .filter_map(|item| {
                item.as_str().map(ToOwned::to_owned).or_else(|| {
                    item.get("method")
                        .and_then(Value::as_str)
                        .map(ToOwned::to_owned)
                })
            })
            .collect(),
        Value::Object(map) => map.keys().cloned().collect(),
        _ => HashSet::new(),
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

fn service_failure(id: Option<Value>, err: Error) -> JsonRpcResponse {
    match err {
        Error::DaemonJsonRpc {
            code,
            message,
            data,
        } => JsonRpcResponse::error(id, code, message, data),
        err => JsonRpcResponse::error(
            id,
            -32000,
            "daemon bridge failure",
            Some(json!({ "details": err.to_string() })),
        ),
    }
}
