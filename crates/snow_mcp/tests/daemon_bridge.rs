use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{Value, json};
use snow_mcp::{
    DaemonBackedMcpBridge, DaemonJsonRpcClient, Error, JsonRpcRequest, McpConfig, Result,
    domain::policy::ToolPolicy,
};
use tokio::io::BufReader;
use tokio::sync::Mutex;

#[derive(Clone)]
struct MockDaemon {
    calls: Arc<Mutex<Vec<(String, Value)>>>,
    contract: Value,
}

impl MockDaemon {
    fn new(contract: Value) -> Self {
        Self {
            calls: Arc::new(Mutex::new(Vec::new())),
            contract,
        }
    }

    async fn method_names(&self) -> Vec<String> {
        self.calls
            .lock()
            .await
            .iter()
            .map(|(method, _)| method.clone())
            .collect()
    }
}

#[async_trait]
impl DaemonJsonRpcClient for MockDaemon {
    async fn request(&self, method: &str, params: Value) -> Result<Value> {
        self.calls
            .lock()
            .await
            .push((method.to_string(), params.clone()));

        match method {
            "contract_info" => Ok(self.contract.clone()),
            "get_record" => Ok(json!({
                "record": {
                    "number": params.get("number").and_then(Value::as_str).unwrap_or("UNKNOWN"),
                    "sys_id": "0123456789abcdef0123456789abcdef"
                }
            })),
            "get_children" => Ok(json!({
                "records": [{
                    "number": "STSK0010001",
                    "parent": params.get("number").and_then(Value::as_str).unwrap_or("UNKNOWN")
                }]
            })),
            "get_article" => Ok(json!({
                "article": {
                    "number": params.get("number").and_then(Value::as_str).unwrap_or("UNKNOWN")
                },
                "markdown": "# KB article"
            })),
            "search_knowledge" => Ok(json!({
                "articles": [{
                    "number": "KB001",
                    "short_description": params.get("query").and_then(Value::as_str).unwrap_or("")
                }]
            })),
            "list_my_tasks" => Ok(json!({ "records": [{ "number": "TASK001" }] })),
            "list_my_approvals" => Ok(json!({ "records": [{ "number": "APR001" }] })),
            "list_my_projects" => Ok(json!({ "records": [{ "number": "PRJ001" }] })),
            "story_apply_create"
                if params.get("plan_id").and_then(Value::as_str) == Some("error-plan") =>
            {
                Err(Error::DaemonJsonRpc {
                    code: -32059,
                    message: "PENDING_RESOLUTION_REQUIRED".to_string(),
                    data: Some(json!({
                        "code": "PENDING_RESOLUTION_REQUIRED",
                        "plan_id": "error-plan"
                    })),
                })
            }
            "work_note_plan_add" => Ok(json!({
                "plan_id": "work-note-plan-1",
                "target": {
                    "number": params.get("number").and_then(Value::as_str).unwrap_or("UNKNOWN"),
                    "table": "rm_story"
                },
                "preview": {
                    "work_notes": params
                        .get("work_notes")
                        .or_else(|| params.get("text"))
                        .cloned()
                        .unwrap_or(Value::Null)
                },
                "confirmation_token": "confirmation-1",
                "idempotency_key": "idem-1"
            })),
            "work_note_apply_add" => Ok(json!({
                "plan_id": params.get("plan_id").and_then(Value::as_str).unwrap_or("UNKNOWN"),
                "tool": "work_note_apply_add",
                "status": "success"
            })),
            _ => Ok(json!({ "ok": true, "method": method })),
        }
    }
}

fn contract(methods: &[&str]) -> Value {
    json!({
        "contract_version": "daemon-json-rpc-v1",
        "daemon_version": "0.1.0",
        "supported_methods": methods,
        "deprecated_aliases": [],
        "environment": { "label": "test" },
        "warming_model": "passive",
        "mcp_availability": { "mode": "disabled", "transport": "disabled" }
    })
}

fn bridge(daemon: MockDaemon) -> DaemonBackedMcpBridge {
    DaemonBackedMcpBridge::new(Arc::new(daemon), McpConfig::default(), "daemon-json-rpc-v1")
}

fn bridge_with_config(daemon: MockDaemon, config: McpConfig) -> DaemonBackedMcpBridge {
    DaemonBackedMcpBridge::new(Arc::new(daemon), config, "daemon-json-rpc-v1")
}

fn request(method: &str, params: Value) -> JsonRpcRequest {
    JsonRpcRequest {
        jsonrpc: "2.0".to_string(),
        method: method.to_string(),
        params,
        id: Some(json!(1)),
    }
}

#[tokio::test]
async fn bridge_translates_logical_mcp_tools_to_daemon_methods() {
    let daemon = MockDaemon::new(contract(&[
        "contract_info",
        "get_record",
        "get_children",
        "get_article",
        "search_knowledge",
    ]));
    let server = bridge(daemon.clone());

    let response = server
        .dispatch(request(
            "tools/call",
            json!({
                "name": "resource_plan_get",
                "arguments": { "record_number": "RPLN0092386" }
            }),
        ))
        .await;
    assert!(response.error.is_none(), "{response:?}");
    assert_eq!(
        response.result.unwrap()["structuredContent"]["record"]["number"],
        json!("RPLN0092386")
    );
    assert_eq!(
        daemon.method_names().await,
        vec!["contract_info".to_string(), "get_record".to_string()]
    );

    let response = server
        .dispatch(request(
            "tools/call",
            json!({
                "name": "story_tasks_list",
                "arguments": { "parent_record_number": "STRY0010001" }
            }),
        ))
        .await;
    assert!(response.error.is_none(), "{response:?}");
    assert_eq!(
        response.result.unwrap()["structuredContent"]["records"][0]["parent"],
        json!("STRY0010001")
    );
}

#[tokio::test]
async fn bridge_tool_call_wraps_result_in_mcp_content_envelope() {
    let daemon = MockDaemon::new(contract(&["contract_info", "get_record"]));
    let server = bridge(daemon);

    let response = server
        .dispatch(request(
            "tools/call",
            json!({
                "name": "get_record",
                "arguments": { "number": "INC5091131" }
            }),
        ))
        .await;

    assert!(response.error.is_none(), "{response:?}");
    let result = response.result.unwrap();

    let content = result["content"]
        .as_array()
        .expect("tools/call result must include a content array");
    assert_eq!(content[0]["type"], json!("text"));
    let text = content[0]["text"]
        .as_str()
        .expect("content entry must carry text");
    assert!(
        text.contains("INC5091131"),
        "content text should carry the record payload: {text}"
    );
    assert_eq!(result["isError"], json!(false));
    assert_eq!(
        result["structuredContent"]["record"]["number"],
        json!("INC5091131")
    );
}

#[tokio::test]
async fn bridge_tool_call_wraps_local_governance_tools() {
    let daemon = MockDaemon::new(contract(&["contract_info", "get_record"]));
    let server = bridge(daemon);

    for name in [
        "tool_capabilities",
        "policy_describe",
        "redaction_rules_describe",
    ] {
        let response = server
            .dispatch(request("tools/call", json!({ "name": name })))
            .await;
        assert!(response.error.is_none(), "{name}: {response:?}");
        let result = response.result.unwrap();
        assert!(
            result["content"].as_array().is_some(),
            "{name} tools/call result missing content array: {result}"
        );
        assert_eq!(result["isError"], json!(false), "{name}");
    }
}

#[tokio::test]
async fn bridge_forwards_enabled_governed_story_apply_tool() {
    let daemon = MockDaemon::new(contract(&["contract_info", "story_apply_create"]));
    let mut config = McpConfig::default();
    config.policy.tools.insert(
        "story_apply_create".to_string(),
        ToolPolicy {
            enabled: true,
            requires_confirmation: true,
            requires_kb_evidence: false,
            ..ToolPolicy::default()
        },
    );
    let server = bridge_with_config(daemon.clone(), config);

    let response = server
        .dispatch(request(
            "tools/call",
            json!({
                "name": "story_apply_create",
                "arguments": {
                    "plan_id": "plan-1",
                    "confirmation_token": "confirmation-1",
                    "idempotency_key": "idem-1"
                }
            }),
        ))
        .await;

    assert!(response.error.is_none(), "{response:?}");
    assert_eq!(
        daemon.method_names().await,
        vec![
            "contract_info".to_string(),
            "story_apply_create".to_string()
        ]
    );
}

#[tokio::test]
async fn bridge_forwards_enabled_governed_timecard_apply_tool() {
    let daemon = MockDaemon::new(contract(&["contract_info", "timecard_apply_set_hours"]));
    let mut config = McpConfig::default();
    config.policy.tools.insert(
        "timecard_apply_set_hours".to_string(),
        ToolPolicy {
            enabled: true,
            requires_confirmation: true,
            requires_kb_evidence: false,
            ..ToolPolicy::default()
        },
    );
    let server = bridge_with_config(daemon.clone(), config);

    let response = server
        .dispatch(request(
            "tools/call",
            json!({
                "name": "timecard_apply_set_hours",
                "arguments": {
                    "plan_id": "plan-1",
                    "confirmation_token": "confirmation-1",
                    "idempotency_key": "idem-1",
                    "concurrency_token": {
                        "sys_updated_on": "2026-05-21 12:00:00",
                        "sys_mod_count": 1
                    }
                }
            }),
        ))
        .await;

    assert!(response.error.is_none(), "{response:?}");
    assert_eq!(
        daemon.method_names().await,
        vec![
            "contract_info".to_string(),
            "timecard_apply_set_hours".to_string()
        ]
    );
}

#[tokio::test]
async fn bridge_forwards_work_note_plan_and_apply_tools() {
    let daemon = MockDaemon::new(contract(&[
        "contract_info",
        "work_note_plan_add",
        "work_note_apply_add",
    ]));
    let server = bridge(daemon.clone());

    let response = server
        .dispatch(request(
            "tools/call",
            json!({
                "name": "work_note_plan_add",
                "arguments": {
                    "number": "STRY0424335",
                    "work_notes": "Adding implementation status."
                }
            }),
        ))
        .await;
    assert!(response.error.is_none(), "{response:?}");
    assert_eq!(
        response.result.unwrap()["structuredContent"]["target"]["number"],
        json!("STRY0424335")
    );

    let response = server
        .dispatch(request(
            "tools/call",
            json!({
                "name": "work_note_apply_add",
                "arguments": {
                    "plan_id": "work-note-plan-1",
                    "confirmation_token": "confirmation-1",
                    "idempotency_key": "idem-1"
                }
            }),
        ))
        .await;
    assert!(response.error.is_none(), "{response:?}");
    assert_eq!(
        response.result.unwrap()["structuredContent"]["tool"],
        json!("work_note_apply_add")
    );
    assert_eq!(
        daemon.method_names().await,
        vec![
            "contract_info".to_string(),
            "work_note_plan_add".to_string(),
            "work_note_apply_add".to_string()
        ]
    );
}

#[tokio::test]
async fn bridge_preserves_structured_daemon_story_errors() {
    let daemon = MockDaemon::new(contract(&["contract_info", "story_apply_create"]));
    let mut config = McpConfig::default();
    config.policy.tools.insert(
        "story_apply_create".to_string(),
        ToolPolicy {
            enabled: true,
            requires_confirmation: true,
            requires_kb_evidence: false,
            ..ToolPolicy::default()
        },
    );
    let server = bridge_with_config(daemon, config);

    let response = server
        .dispatch(request(
            "tools/call",
            json!({
                "name": "story_apply_create",
                "arguments": {
                    "plan_id": "error-plan",
                    "confirmation_token": "confirmation-1",
                    "idempotency_key": "idem-1"
                }
            }),
        ))
        .await;

    let error = response
        .error
        .expect("daemon Story write error should pass through");
    assert_eq!(error.code, -32059);
    assert_eq!(error.message, "PENDING_RESOLUTION_REQUIRED");
    assert_eq!(
        error.data.unwrap()["code"],
        json!("PENDING_RESOLUTION_REQUIRED")
    );
}

#[tokio::test]
async fn bridge_filters_tools_against_daemon_contract() {
    let daemon = MockDaemon::new(contract(&["contract_info", "get_record"]));
    let server = bridge(daemon);

    let response = server.dispatch(request("tools/list", json!({}))).await;
    assert!(response.error.is_none(), "{response:?}");
    let result = response.result.unwrap();
    let tools = result["tools"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|tool| tool.get("name").and_then(Value::as_str))
        .collect::<Vec<_>>();

    assert!(tools.contains(&"get_record"));
    assert!(tools.contains(&"resource_plan_get"));
    assert!(tools.contains(&"story_get"));
    assert!(tools.contains(&"tool_capabilities"));
    assert!(!tools.contains(&"knowledge_fetch"));
}

#[tokio::test]
async fn bridge_handles_initialized_notification_without_response() {
    let daemon = MockDaemon::new(contract(&["contract_info", "get_record"]));
    let server = bridge(daemon);
    let input = concat!(
        r#"{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}"#,
        "\n",
        r#"{"jsonrpc":"2.0","method":"notifications/initialized","params":{}}"#,
        "\n"
    );
    let mut output = Vec::new();

    server
        .serve_streams(
            BufReader::new(input.as_bytes()),
            &mut output,
            std::future::pending::<std::io::Result<()>>(),
        )
        .await
        .unwrap();

    let output = String::from_utf8(output).unwrap();
    let lines = output.lines().collect::<Vec<_>>();
    assert_eq!(lines.len(), 1);
    let response: Value = serde_json::from_str(lines[0]).unwrap();
    assert_eq!(response["id"], json!(1));
}

#[tokio::test]
async fn bridge_advertised_dashboard_resource_is_readable() {
    let daemon = MockDaemon::new(contract(&[
        "contract_info",
        "list_my_tasks",
        "list_my_approvals",
        "list_my_projects",
    ]));
    let server = bridge(daemon.clone());

    let resources = server.dispatch(request("resources/list", json!({}))).await;
    assert!(resources.error.is_none(), "{resources:?}");
    assert!(
        resources.result.unwrap()["resources"]
            .as_array()
            .unwrap()
            .iter()
            .any(|resource| resource["uri"] == json!("snow://dashboard"))
    );

    let response = server
        .dispatch(request(
            "resources/read",
            json!({ "uri": "snow://dashboard" }),
        ))
        .await;

    assert!(response.error.is_none(), "{response:?}");
    assert!(
        response.result.unwrap()["text"]
            .as_str()
            .unwrap()
            .contains("TASK001")
    );
    assert_eq!(
        daemon.method_names().await,
        vec![
            "contract_info".to_string(),
            "list_my_tasks".to_string(),
            "list_my_approvals".to_string(),
            "list_my_projects".to_string()
        ]
    );
}

#[tokio::test]
async fn bridge_refuses_unsupported_translated_tools() {
    let daemon = MockDaemon::new(contract(&["contract_info", "get_record"]));
    let server = bridge(daemon.clone());

    let response = server
        .dispatch(request(
            "tools/call",
            json!({
                "name": "knowledge_fetch",
                "arguments": { "number": "KB001" }
            }),
        ))
        .await;

    let error = response.error.expect("unsupported tool should fail");
    assert_eq!(error.code, -32041);
    assert_eq!(
        daemon.method_names().await,
        vec!["contract_info".to_string()]
    );
}

#[tokio::test]
async fn bridge_requires_compatible_daemon_contract() {
    let mut incompatible = contract(&["contract_info", "get_record"]);
    incompatible["contract_version"] = json!("daemon-json-rpc-v2");
    let daemon = MockDaemon::new(incompatible);
    let server = bridge(daemon);

    let response = server.dispatch(request("tools/list", json!({}))).await;

    let error = response.error.expect("incompatible contract should fail");
    assert_eq!(error.code, -32000);
    assert!(
        error.data.unwrap()["details"]
            .as_str()
            .unwrap()
            .contains("incompatible daemon contract")
    );
}
