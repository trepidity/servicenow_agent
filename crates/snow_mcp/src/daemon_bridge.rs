use std::collections::HashSet;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{Value, json};
use snow_core::ipc::{IpcEndpoint, IpcStream, default_endpoint_from_env};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::signal;
use tokio::sync::Mutex;
use tokio::time::{Instant, sleep, timeout};

use crate::config::McpConfig;
use crate::domain::policy::is_write_tool;
use crate::planner::is_governed_write_tool;
use crate::protocol::frame::{
    FrameRead, RESULT_TOO_LARGE_CODE, read_frame, request_too_large_data,
};
use crate::protocol::schema::{
    JsonRpcRequest, JsonRpcResponse, PolicyDescribeReport, ToolCapabilitiesReport, ToolCapability,
};
use crate::tools::ToolRegistry;
use crate::tools::records::{RESOURCE_PLAN_LOOKUP_TABLES, RecordLookup, parse_record_lookup};
use crate::{Error, Result};

const JSON_RPC_VERSION: &str = "2.0";
const DEFAULT_CONTRACT_VERSION: &str = "daemon-json-rpc-v1";
const DEFAULT_AUTOSPAWN_TIMEOUT: Duration = Duration::from_secs(60);
const AUTOSPAWN_RETRY_INTERVAL: Duration = Duration::from_millis(200);

const BRIDGE_TOOL_METHODS: &[(&str, &str)] = &[
    ("get_record", "get_record"),
    ("get_approval", "get_approval"),
    ("approval_approve", "approval_approve"),
    ("approval_reject", "approval_reject"),
    ("catalog_items_search", "catalog_items_search"),
    ("catalog_item_get", "catalog_item_get"),
    ("catalog_plan_request", "catalog_plan_request"),
    ("catalog_submit_request", "catalog_submit_request"),
    ("search_records", "search_records"),
    ("user_lookup", "user_lookup"),
    ("user_search", "user_search"),
    ("business_application_get", "business_application_get"),
    ("business_application_search", "business_application_search"),
    ("business_application_query", "business_application_query"),
    (
        "business_application_servers",
        "business_application_servers",
    ),
    (
        "business_application_servers_cached",
        "business_application_servers_cached",
    ),
    (
        "business_applications_for_server",
        "business_applications_for_server",
    ),
    ("business_application_fields", "business_application_fields"),
    ("server_get", "server_get"),
    ("server_search", "server_search"),
    ("server_query", "server_query"),
    ("server_fields", "server_fields"),
    ("incident_fields", "incident_fields"),
    ("incident_get", "incident_get"),
    ("incident_query", "incident_query"),
    ("list_records", "list_records"),
    ("record_query", "record_query"),
    ("list_my_tasks", "list_my_tasks"),
    ("list_my_approvals", "list_my_approvals"),
    ("list_my_projects", "list_my_projects"),
    ("get_children", "get_children"),
    ("get_work_notes", "get_work_notes"),
    ("attachment_list", "attachment_list"),
    ("attachment_upload", "attachment_upload"),
    ("resource_plan_get", "get_record"),
    ("resource_plan_list", "resource_plan_list"),
    (
        "incident_list_by_assignment_group",
        "incident_list_by_assignment_group",
    ),
    ("incident_assignment_groups", "incident_assignment_groups"),
    (
        "incident_assignment_group_queue",
        "incident_assignment_group_queue",
    ),
    ("incident_plan_update", "incident_plan_update"),
    ("incident_apply_update", "incident_apply_update"),
    ("incident_bulk_plan_update", "incident_bulk_plan_update"),
    ("incident_bulk_apply_update", "incident_bulk_apply_update"),
    ("resource_plan_plan_create", "resource_plan_plan_create"),
    ("resource_plan_apply_create", "resource_plan_apply_create"),
    ("resource_plan_plan_update", "resource_plan_plan_update"),
    ("resource_plan_apply_update", "resource_plan_apply_update"),
    ("resource_plan_plan_decision", "resource_plan_plan_decision"),
    (
        "resource_plan_apply_decision",
        "resource_plan_apply_decision",
    ),
    ("story_get", "get_record"),
    ("story_tasks_list", "get_children"),
    ("change_request_plan_create", "change_request_plan_create"),
    ("change_request_apply_create", "change_request_apply_create"),
    ("change_request_plan_update", "change_request_plan_update"),
    ("change_request_apply_update", "change_request_apply_update"),
    ("change_task_plan_create", "change_task_plan_create"),
    ("change_task_apply_create", "change_task_apply_create"),
    ("change_task_plan_update", "change_task_plan_update"),
    ("change_task_apply_update", "change_task_apply_update"),
    ("story_plan_create", "story_plan_create"),
    ("story_apply_create", "story_apply_create"),
    ("story_plan_update", "story_plan_update"),
    ("story_apply_update", "story_apply_update"),
    ("story_task_plan_create", "story_task_plan_create"),
    ("story_task_apply_create", "story_task_apply_create"),
    ("story_task_plan_update", "story_task_plan_update"),
    ("story_task_apply_update", "story_task_apply_update"),
    ("timecard_list", "timecard_list"),
    ("timecard_plan_set_hours", "timecard_plan_set_hours"),
    ("timecard_apply_set_hours", "timecard_apply_set_hours"),
    ("work_note_plan_add", "work_note_plan_add"),
    ("work_note_apply_add", "work_note_apply_add"),
    ("knowledge_plan_create_draft", "knowledge_plan_create_draft"),
    (
        "knowledge_apply_create_draft",
        "knowledge_apply_create_draft",
    ),
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

pub type DaemonEndpoint = IpcEndpoint;

#[derive(Debug)]
pub struct ProcessDaemonAutoSpawn {
    snow_bin: PathBuf,
    start_timeout: Duration,
    retry_timeout: Duration,
    retry_interval: Duration,
    spawn_lock: Mutex<()>,
}

impl ProcessDaemonAutoSpawn {
    pub fn new(snow_bin: impl Into<PathBuf>) -> Self {
        Self {
            snow_bin: snow_bin.into(),
            start_timeout: DEFAULT_AUTOSPAWN_TIMEOUT,
            retry_timeout: DEFAULT_AUTOSPAWN_TIMEOUT,
            retry_interval: AUTOSPAWN_RETRY_INTERVAL,
            spawn_lock: Mutex::new(()),
        }
    }

    pub fn with_timeouts(
        snow_bin: impl Into<PathBuf>,
        start_timeout: Duration,
        retry_timeout: Duration,
        retry_interval: Duration,
    ) -> Self {
        Self {
            snow_bin: snow_bin.into(),
            start_timeout,
            retry_timeout,
            retry_interval,
            spawn_lock: Mutex::new(()),
        }
    }

    pub fn snow_bin(&self) -> &Path {
        &self.snow_bin
    }

    async fn start(&self) -> Result<()> {
        let _guard = self.spawn_lock.lock().await;
        let output = timeout(self.start_timeout, {
            let mut command = Command::new(&self.snow_bin);
            command.arg("daemon").arg("start").stdin(Stdio::null());
            command.output()
        })
        .await
        .map_err(|_| {
            Error::InvalidParams(format!(
                "daemon unavailable and auto-spawn timed out running {} daemon start",
                self.snow_bin.display()
            ))
        })?
        .map_err(|err| {
            Error::InvalidParams(format!(
                "daemon unavailable and auto-spawn failed to execute {} daemon start: {err}",
                self.snow_bin.display()
            ))
        })?;

        if !output.status.success() {
            return Err(Error::InvalidParams(format!(
                "daemon unavailable and auto-spawn command failed: {} daemon start exited with {}{}",
                self.snow_bin.display(),
                output.status,
                command_output_summary(&output.stdout, &output.stderr)
            )));
        }
        Ok(())
    }
}

#[derive(Debug)]
enum AutoSpawnMode {
    Process(ProcessDaemonAutoSpawn),
    Unavailable(String),
}

#[derive(Debug)]
pub struct LocalSocketDaemonJsonRpcClient {
    endpoint: DaemonEndpoint,
    next_id: AtomicU64,
    auto_spawn: Option<AutoSpawnMode>,
}

pub type UnixDaemonJsonRpcClient = LocalSocketDaemonJsonRpcClient;

impl LocalSocketDaemonJsonRpcClient {
    pub fn new(endpoint: impl Into<DaemonEndpoint>) -> Self {
        Self {
            endpoint: endpoint.into(),
            next_id: AtomicU64::new(1),
            auto_spawn: None,
        }
    }

    pub fn with_auto_spawn(
        endpoint: impl Into<DaemonEndpoint>,
        auto_spawn: ProcessDaemonAutoSpawn,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            next_id: AtomicU64::new(1),
            auto_spawn: Some(AutoSpawnMode::Process(auto_spawn)),
        }
    }

    pub fn with_unavailable_auto_spawn(
        endpoint: impl Into<DaemonEndpoint>,
        reason: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            next_id: AtomicU64::new(1),
            auto_spawn: Some(AutoSpawnMode::Unavailable(reason.into())),
        }
    }

    pub fn from_socket_path(socket_path: impl Into<PathBuf>) -> Self {
        Self::new(DaemonEndpoint::from_socket_path(socket_path))
    }

    pub fn endpoint(&self) -> &DaemonEndpoint {
        &self.endpoint
    }

    pub fn socket_path(&self) -> Option<&Path> {
        self.endpoint.socket_path()
    }

    async fn connect(&self) -> Result<IpcStream> {
        let first_error = match self.try_connect().await {
            Ok(stream) => return Ok(stream),
            Err(err) => err,
        };

        match &self.auto_spawn {
            Some(AutoSpawnMode::Process(auto_spawn)) => {
                auto_spawn.start().await?;
                self.wait_for_connect(
                    first_error,
                    auto_spawn.retry_timeout,
                    auto_spawn.retry_interval,
                )
                .await
            }
            Some(AutoSpawnMode::Unavailable(reason)) => Err(Error::InvalidParams(format!(
                "daemon unavailable and auto-spawn could not locate snow ({reason}); endpoint: {}; connect error: {first_error}",
                self.endpoint
            ))),
            None => Err(Error::Io(first_error)),
        }
    }

    async fn wait_for_connect(
        &self,
        first_error: std::io::Error,
        retry_timeout: Duration,
        retry_interval: Duration,
    ) -> Result<IpcStream> {
        let first_error = first_error.to_string();
        let deadline = Instant::now() + retry_timeout;
        let mut last_error: String;

        loop {
            match self.try_connect().await {
                Ok(stream) => return Ok(stream),
                Err(err) => last_error = err.to_string(),
            }

            let now = Instant::now();
            if now >= deadline {
                break;
            }

            let remaining = deadline - now;
            sleep(retry_interval.min(remaining)).await;
        }

        Err(Error::InvalidParams(format!(
            "daemon unavailable after auto-spawn retry window; endpoint: {}; first connect error: {first_error}; last connect error: {last_error}",
            self.endpoint
        )))
    }

    async fn try_connect(&self) -> std::io::Result<IpcStream> {
        self.endpoint.connect().await
    }
}

#[async_trait]
impl DaemonJsonRpcClient for LocalSocketDaemonJsonRpcClient {
    async fn request(&self, method: &str, params: Value) -> Result<Value> {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let payload = json!({
            "jsonrpc": JSON_RPC_VERSION,
            "id": id,
            "method": method,
            "params": params,
        });

        let stream = self.connect().await?;
        let (reader, mut writer) = tokio::io::split(stream);
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

pub fn default_daemon_endpoint() -> DaemonEndpoint {
    default_endpoint_from_env()
        .unwrap_or_else(|_| DaemonEndpoint::filesystem(PathBuf::from(".snow").join("daemon.sock")))
}

fn command_output_summary(stdout: &[u8], stderr: &[u8]) -> String {
    let stderr = String::from_utf8_lossy(stderr).trim().to_string();
    let stdout = String::from_utf8_lossy(stdout).trim().to_string();
    if stderr.is_empty() && stdout.is_empty() {
        return String::new();
    }

    let details = if stderr.is_empty() {
        stdout
    } else if stdout.is_empty() {
        stderr
    } else {
        format!("stderr: {stderr}; stdout: {stdout}")
    };
    format!(": {}", details.chars().take(500).collect::<String>())
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
            Arc::new(LocalSocketDaemonJsonRpcClient::from_socket_path(
                socket_path,
            )),
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
        let mut frame = Vec::new();

        loop {
            tokio::select! {
                _ = &mut shutdown => break,
                read = read_frame(&mut reader, &mut frame) => {
                    let frame_status = read?;
                    let close_after_response = frame_status == FrameRead::TooLarge;
                    let response = match frame_status {
                        FrameRead::Eof => break,
                        FrameRead::TooLarge => JsonRpcResponse::error(
                            None,
                            RESULT_TOO_LARGE_CODE,
                            "request frame exceeds maximum size",
                            Some(request_too_large_data()),
                        ),
                        FrameRead::Frame => match serde_json::from_slice::<JsonRpcRequest>(&frame) {
                        Ok(request) => self.dispatch(request).await,
                        Err(err) => JsonRpcResponse::error(
                            None,
                            -32700,
                            "parse error",
                            Some(json!({ "details": err.to_string() })),
                        ),
                        },
                    };
                    if response.id.is_some() || close_after_response {
                        writer.write_all(crate::transport::stdio::bounded_payload(response)?.as_slice()).await?;
                        writer.write_all(b"\n").await?;
                        writer.flush().await?;
                    }
                    if close_after_response {
                        break;
                    }
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
            "policy_describe" => match self.policy_describe_payload().await {
                Ok(payload) => JsonRpcResponse::ok(id, payload),
                Err(err) => service_failure(id, err),
            },
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
                return match self.policy_describe_payload().await {
                    Ok(payload) => JsonRpcResponse::ok(id, tool_call_result(payload)),
                    Err(err) => service_failure(id, err),
                };
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

        if is_write_tool(name) && !is_governed_write_tool(name) {
            return JsonRpcResponse::error(
                id,
                -32040,
                "policy denied",
                Some(json!({ "details": "daemon-backed MCP bridge is read-only", "tool": name })),
            );
        }
        // The daemon owns governed write policy. The bridge only blocks generic
        // or otherwise ungoverned writes so its static default config cannot
        // drift away from the daemon's deploy-time policy file.
        let Some(base_method) = canonical_daemon_method(name) else {
            return JsonRpcResponse::error(id, -32601, "tool not found", None);
        };

        let args = params
            .get("arguments")
            .cloned()
            .unwrap_or_else(|| json!({}));
        let method = if matches!(name, "get_article" | "knowledge_fetch")
            && args.get("fresh").and_then(Value::as_bool) == Some(true)
        {
            "get_article_fresh"
        } else {
            base_method
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

        if name == "attachment_upload" {
            if !self
                .config
                .policy
                .tool_enabled_in_environment(name, &self.config.environment.label)
            {
                return JsonRpcResponse::error(
                    id,
                    -32040,
                    "policy denied",
                    Some(json!({ "tool": name, "details": "attachment uploads are disabled" })),
                );
            }
            if args.get("confirm_upload").and_then(Value::as_bool) != Some(true) {
                return invalid_params(id, "attachment_upload requires confirm_upload=true");
            }
        }
        let daemon_params = match daemon_params_for_tool(name, method, args) {
            Ok(params) => params,
            Err(err) => return invalid_params(id, err.to_string()),
        };
        match self.daemon.request(method, daemon_params).await {
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
            let available = LOCAL_GOVERNANCE_TOOLS.contains(&tool.name.as_str())
                || canonical_daemon_method(&tool.name)
                    .map(|method| method_available(&contract, method))
                    .unwrap_or(false);
            if available {
                tools.push(ToolCapability {
                    name: tool.name.clone(),
                    enabled: self
                        .config
                        .policy
                        .tool_enabled_in_environment(&tool.name, &contract.environment),
                    mode: if is_write_tool(&tool.name) {
                        "write".to_string()
                    } else {
                        "read".to_string()
                    },
                    read_only: !is_write_tool(&tool.name),
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

    async fn policy_describe_payload(&self) -> Result<Value> {
        let mut report: PolicyDescribeReport = self
            .config
            .policy
            .policy_describe(self.config.redaction.rules_version.clone());
        let capabilities = self.tool_capabilities().await?;
        report.environment = capabilities.environment;
        report.default_mode = capabilities.default_mode;
        let available_writes: HashSet<String> = capabilities
            .tools
            .into_iter()
            .filter(|tool| tool.enabled && tool.mode == "write")
            .map(|tool| tool.name)
            .collect();
        report.write_tools_enabled = self
            .config
            .policy
            .tools
            .iter()
            .filter(|(name, policy)| {
                policy.enabled && is_write_tool(name) && available_writes.contains(*name)
            })
            .map(|(name, _)| name.clone())
            .collect();
        Ok(json!(report))
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

fn daemon_params_for_tool(tool: &str, method: &str, args: Value) -> Result<Value> {
    if tool == "incident_fields" {
        let _: IncidentFieldsArguments = serde_json::from_value(args)?;
        return Ok(json!({}));
    }

    let mut object = match args {
        Value::Object(map) => map,
        _ => return Ok(json!({})),
    };

    normalize_number_alias(&mut object);

    match tool {
        "get_record" | "get_work_notes" => {
            return lookup_params(parse_record_lookup_value(
                Value::Object(object),
                snow_core::RECORD_LOOKUP_ALLOWED_TABLES,
            )?);
        }
        "resource_plan_get" => {
            return lookup_params(parse_record_lookup_value(
                Value::Object(object),
                RESOURCE_PLAN_LOOKUP_TABLES,
            )?);
        }
        "story_get" => {
            reject_table_sys_id(&object, "story_get")?;
        }
        "story_tasks_list" => {
            reject_table_sys_id(&object, "story_tasks_list")?;
        }
        "business_application_servers" => {
            object.remove("prune_stale");
            object.insert("persist".to_string(), json!(false));
        }
        "server_get" => {
            object.insert("persist".to_string(), json!(false));
        }
        _ => {}
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
    if method == "attachment_upload" {
        object.remove("confirm_upload");
        object.remove("idempotency_key");
    }

    Ok(Value::Object(object))
}

/// Closed argument object for the fixed-table `incident_fields` bridge tool.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IncidentFieldsArguments {}

fn normalize_number_alias(object: &mut serde_json::Map<String, Value>) {
    if !object.contains_key("number")
        && let Some(record_number) = object.get("record_number").and_then(Value::as_str)
    {
        object.insert(
            "number".to_string(),
            Value::String(record_number.to_string()),
        );
    }
}

fn reject_table_sys_id(object: &serde_json::Map<String, Value>, tool: &str) -> Result<()> {
    if object.contains_key("table") || object.contains_key("sys_id") {
        return Err(Error::InvalidParams(format!(
            "{tool} only accepts number-based lookups"
        )));
    }
    Ok(())
}

fn parse_record_lookup_value(args: Value, allowed_tables: &[&str]) -> Result<RecordLookup> {
    parse_record_lookup(&args, allowed_tables)
}

fn lookup_params(lookup: RecordLookup) -> Result<Value> {
    Ok(match lookup {
        RecordLookup::Number(number) => json!({ "number": number }),
        RecordLookup::TableSysId { table, sys_id } => {
            json!({ "table": table, "sys_id": sys_id })
        }
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct MockDaemon {
        calls: Mutex<Vec<(String, Value)>>,
        supported_methods: Vec<&'static str>,
    }

    impl MockDaemon {
        fn with_supported_methods(supported_methods: Vec<&'static str>) -> Self {
            Self {
                calls: Mutex::new(Vec::new()),
                supported_methods,
            }
        }
    }

    #[async_trait]
    impl DaemonJsonRpcClient for MockDaemon {
        async fn request(&self, method: &str, params: Value) -> Result<Value> {
            if method == "contract_info" {
                return Ok(json!({
                    "contract_version": DEFAULT_CONTRACT_VERSION,
                    "environment": { "label": "test" },
                    "supported_methods": if self.supported_methods.is_empty() {
                        json!(["story_task_apply_update"])
                    } else {
                        json!(self.supported_methods)
                    },
                    "deprecated_aliases": [],
                }));
            }

            self.calls
                .lock()
                .await
                .push((method.to_string(), params.clone()));
            Ok(json!({ "method": method, "params": params }))
        }
    }

    #[tokio::test]
    async fn governed_writes_are_forwarded_to_daemon_policy() {
        let daemon = Arc::new(MockDaemon::default());
        let bridge = DaemonBackedMcpBridge::new(
            daemon.clone(),
            McpConfig::default(),
            DEFAULT_CONTRACT_VERSION,
        );

        let response = bridge
            .dispatch(JsonRpcRequest {
                jsonrpc: JSON_RPC_VERSION.to_string(),
                method: "tools/call".to_string(),
                params: json!({
                    "name": "story_task_apply_update",
                    "arguments": {
                        "plan_id": "plan-1",
                        "confirmation_token": "confirm-1",
                        "idempotency_key": "idem-1",
                    }
                }),
                id: Some(json!(1)),
            })
            .await;

        assert!(response.error.is_none(), "{response:?}");
        let calls = daemon.calls.lock().await;
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "story_task_apply_update");
    }

    #[tokio::test]
    async fn policy_describe_reflects_daemon_governed_writes() {
        let mut config = McpConfig::default();
        config
            .policy
            .tools
            .get_mut("story_task_apply_update")
            .expect("story task apply policy")
            .enabled = true;
        let bridge = DaemonBackedMcpBridge::new(
            Arc::new(MockDaemon::default()),
            config,
            DEFAULT_CONTRACT_VERSION,
        );

        let response = bridge
            .dispatch(JsonRpcRequest {
                jsonrpc: JSON_RPC_VERSION.to_string(),
                method: "tools/call".to_string(),
                params: json!({
                    "name": "policy_describe",
                    "arguments": {}
                }),
                id: Some(json!(1)),
            })
            .await;

        let structured = response
            .result
            .as_ref()
            .and_then(|result| result.get("structuredContent"))
            .expect("structured policy response");
        let writes = structured
            .get("write_tools_enabled")
            .and_then(Value::as_array)
            .expect("write tools array");
        assert!(writes.contains(&json!("story_task_apply_update")));
    }

    #[tokio::test]
    async fn business_application_tools_are_gated_by_daemon_contract() {
        let daemon = Arc::new(MockDaemon::with_supported_methods(vec![
            "business_application_get",
            "business_application_search",
            "business_application_query",
            "business_application_servers",
            "business_application_servers_cached",
            "business_applications_for_server",
            "business_application_fields",
        ]));
        let bridge =
            DaemonBackedMcpBridge::new(daemon, McpConfig::default(), DEFAULT_CONTRACT_VERSION);

        let response = bridge
            .dispatch(JsonRpcRequest {
                jsonrpc: JSON_RPC_VERSION.to_string(),
                method: "tools/list".to_string(),
                params: json!({}),
                id: Some(json!(1)),
            })
            .await;

        let tools = response
            .result
            .as_ref()
            .and_then(|result| result.get("tools"))
            .and_then(Value::as_array)
            .expect("tools");
        let names = tools
            .iter()
            .filter_map(|tool| tool.get("name").and_then(Value::as_str))
            .collect::<Vec<_>>();
        assert!(names.contains(&"business_application_get"));
        assert!(names.contains(&"business_application_search"));
        assert!(names.contains(&"business_application_query"));
        assert!(names.contains(&"business_application_servers"));
        assert!(names.contains(&"business_application_servers_cached"));
        assert!(names.contains(&"business_applications_for_server"));
        assert!(names.contains(&"business_application_fields"));
        assert!(!names.contains(&"get_record"));

        let get = tools
            .iter()
            .find(|tool| tool["name"] == "business_application_get")
            .expect("business_application_get advertised");
        assert!(
            get["description"]
                .as_str()
                .expect("business_application_get description")
                .contains("APM0002456")
        );
        assert!(get["inputSchema"]["properties"].get("number").is_none());

        let query = tools
            .iter()
            .find(|tool| tool["name"] == "business_application_query")
            .expect("business_application_query advertised");
        assert!(
            query["inputSchema"]["properties"]["filters"]["description"]
                .as_str()
                .expect("business_application_query filters description")
                .contains("owner for APM0002456")
        );
    }

    #[tokio::test]
    async fn business_application_search_forwards_hydration_options() {
        let daemon = Arc::new(MockDaemon::with_supported_methods(vec![
            "business_application_search",
        ]));
        let bridge = DaemonBackedMcpBridge::new(
            daemon.clone(),
            McpConfig::default(),
            DEFAULT_CONTRACT_VERSION,
        );

        let response = bridge
            .dispatch(JsonRpcRequest {
                jsonrpc: JSON_RPC_VERSION.to_string(),
                method: "tools/call".to_string(),
                params: json!({
                    "name": "business_application_search",
                    "arguments": {
                        "name": "Epic",
                        "persist": false,
                        "resolve_references": false,
                        "reference_depth": 2,
                        "refresh_dictionary": true
                    }
                }),
                id: Some(json!(1)),
            })
            .await;

        assert!(response.error.is_none(), "{response:?}");
        let calls = daemon.calls.lock().await;
        assert_eq!(calls[0].0, "business_application_search");
        assert_eq!(calls[0].1["persist"], json!(false));
        assert_eq!(calls[0].1["resolve_references"], json!(false));
        assert_eq!(calls[0].1["reference_depth"], json!(2));
        assert_eq!(calls[0].1["refresh_dictionary"], json!(true));
    }

    #[tokio::test]
    async fn business_application_servers_bridge_forces_no_persist() {
        let daemon = Arc::new(MockDaemon::with_supported_methods(vec![
            "business_application_servers",
        ]));
        let bridge = DaemonBackedMcpBridge::new(
            daemon.clone(),
            McpConfig::default(),
            DEFAULT_CONTRACT_VERSION,
        );

        let response = bridge
            .dispatch(JsonRpcRequest {
                jsonrpc: JSON_RPC_VERSION.to_string(),
                method: "tools/call".to_string(),
                params: json!({
                    "name": "business_application_servers",
                    "arguments": {
                        "number": "APM0000001",
                        "max_service_membership_associations": 10,
                        "max_service_membership_pages": 2,
                        "persist": true,
                        "prune_stale": true
                    }
                }),
                id: Some(json!(1)),
            })
            .await;

        assert!(response.error.is_none(), "{response:?}");
        let calls = daemon.calls.lock().await;
        assert_eq!(calls[0].0, "business_application_servers");
        assert_eq!(calls[0].1["persist"], json!(false));
        assert!(calls[0].1.get("prune_stale").is_none());
        assert_eq!(calls[0].1["max_service_membership_associations"], json!(10));
        assert_eq!(calls[0].1["max_service_membership_pages"], json!(2));
    }

    #[tokio::test]
    async fn server_get_bridge_forces_no_persist() {
        let daemon = Arc::new(MockDaemon::with_supported_methods(vec!["server_get"]));
        let bridge = DaemonBackedMcpBridge::new(
            daemon.clone(),
            McpConfig::default(),
            DEFAULT_CONTRACT_VERSION,
        );

        let response = bridge
            .dispatch(JsonRpcRequest {
                jsonrpc: JSON_RPC_VERSION.to_string(),
                method: "tools/call".to_string(),
                params: json!({
                    "name": "server_get",
                    "arguments": {
                        "name": "host11.example.internal"
                    }
                }),
                id: Some(json!(1)),
            })
            .await;

        assert!(response.error.is_none(), "{response:?}");
        let calls = daemon.calls.lock().await;
        assert_eq!(calls[0].0, "server_get");
        assert_eq!(calls[0].1["name"], json!("host11.example.internal"));
        assert_eq!(calls[0].1["persist"], json!(false));
    }

    #[tokio::test]
    async fn user_search_forwards_to_daemon_method() {
        let daemon = Arc::new(MockDaemon::with_supported_methods(vec!["user_search"]));
        let bridge = DaemonBackedMcpBridge::new(
            daemon.clone(),
            McpConfig::default(),
            DEFAULT_CONTRACT_VERSION,
        );

        let response = bridge
            .dispatch(JsonRpcRequest {
                jsonrpc: JSON_RPC_VERSION.to_string(),
                method: "tools/call".to_string(),
                params: json!({
                    "name": "user_search",
                    "arguments": {
                        "name_contains": "Smith",
                        "email_contains": "@example.com",
                        "limit": 5,
                        "active": false
                    }
                }),
                id: Some(json!(1)),
            })
            .await;

        assert!(response.error.is_none(), "{response:?}");
        let calls = daemon.calls.lock().await;
        assert_eq!(calls[0].0, "user_search");
        assert_eq!(
            calls[0].1,
            json!({
                "name_contains": "Smith",
                "email_contains": "@example.com",
                "limit": 5,
                "active": false
            })
        );
    }

    #[tokio::test]
    async fn attachment_upload_is_denied_when_policy_disabled() {
        let daemon = Arc::new(MockDaemon::with_supported_methods(vec![
            "attachment_upload",
        ]));
        let bridge = DaemonBackedMcpBridge::new(
            daemon.clone(),
            McpConfig::default(),
            DEFAULT_CONTRACT_VERSION,
        );

        let response = bridge
            .dispatch(JsonRpcRequest {
                jsonrpc: JSON_RPC_VERSION.to_string(),
                method: "tools/call".to_string(),
                params: json!({
                    "name": "attachment_upload",
                    "arguments": {
                        "number": "CHG0010001",
                        "path": "/tmp/evidence.jpeg",
                        "confirm_upload": true,
                        "idempotency_key": "idem-1",
                    }
                }),
                id: Some(json!(1)),
            })
            .await;

        let error = response.error.expect("policy denied");
        assert_eq!(error.code, -32040);
        assert!(daemon.calls.lock().await.is_empty());
    }

    #[tokio::test]
    async fn attachment_upload_requires_confirmation_and_forwards_clean_daemon_params() {
        let daemon = Arc::new(MockDaemon::with_supported_methods(vec![
            "attachment_upload",
        ]));
        let mut config = McpConfig::default();
        config
            .policy
            .tools
            .get_mut("attachment_upload")
            .expect("attachment upload policy")
            .enabled = true;
        let bridge = DaemonBackedMcpBridge::new(daemon.clone(), config, DEFAULT_CONTRACT_VERSION);

        let missing_confirmation = bridge
            .dispatch(JsonRpcRequest {
                jsonrpc: JSON_RPC_VERSION.to_string(),
                method: "tools/call".to_string(),
                params: json!({
                    "name": "attachment_upload",
                    "arguments": {
                        "number": "CHG0010001",
                        "path": "/tmp/evidence.jpeg",
                        "idempotency_key": "idem-1",
                    }
                }),
                id: Some(json!(1)),
            })
            .await;
        assert_eq!(missing_confirmation.error.expect("error").code, -32602);

        let response = bridge
            .dispatch(JsonRpcRequest {
                jsonrpc: JSON_RPC_VERSION.to_string(),
                method: "tools/call".to_string(),
                params: json!({
                    "name": "attachment_upload",
                    "arguments": {
                        "number": "CHG0010001",
                        "path": "/tmp/evidence.jpeg",
                        "file_name": "evidence.jpeg",
                        "content_type": "image/jpeg",
                        "confirm_upload": true,
                        "idempotency_key": "idem-1",
                    }
                }),
                id: Some(json!(2)),
            })
            .await;

        assert!(response.error.is_none(), "{response:?}");
        let calls = daemon.calls.lock().await;
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].0, "attachment_upload");
        assert_eq!(calls[0].1["number"], json!("CHG0010001"));
        assert_eq!(calls[0].1["path"], json!("/tmp/evidence.jpeg"));
        assert_eq!(calls[0].1["file_name"], json!("evidence.jpeg"));
        assert_eq!(calls[0].1["content_type"], json!("image/jpeg"));
        assert!(calls[0].1.get("confirm_upload").is_none());
        assert!(calls[0].1.get("idempotency_key").is_none());
    }

    #[test]
    fn bridge_maps_resource_plan_write_tools() {
        assert_eq!(
            canonical_daemon_method("resource_plan_list"),
            Some("resource_plan_list")
        );
        for tool in [
            "resource_plan_plan_create",
            "resource_plan_apply_create",
            "resource_plan_plan_update",
            "resource_plan_apply_update",
            "resource_plan_plan_decision",
            "resource_plan_apply_decision",
        ] {
            assert_eq!(canonical_daemon_method(tool), Some(tool));
        }
    }
}
