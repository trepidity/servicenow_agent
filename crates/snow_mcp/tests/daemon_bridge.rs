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
                    "table": params.get("table").and_then(Value::as_str),
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

fn assert_no_top_level_schema_composition(tool_name: &str, schema: &Value) {
    for keyword in ["oneOf", "anyOf", "allOf"] {
        assert!(
            schema.get(keyword).is_none(),
            "{tool_name} inputSchema must not use top-level {keyword}"
        );
    }
}

#[tokio::test]
async fn bridge_forwards_generic_get_record_table_sys_id_lookup() {
    let daemon = MockDaemon::new(contract(&["contract_info", "get_record"]));
    let server = bridge(daemon.clone());
    let sys_id = "7F029B89C3E7565067BDFD73E40131A1";

    let response = server
        .dispatch(request(
            "tools/call",
            json!({
                "name": "get_record",
                "arguments": { "table": "dmn_demand", "sys_id": sys_id }
            }),
        ))
        .await;

    assert!(response.error.is_none(), "{response:?}");
    let calls = daemon.calls.lock().await;
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[1].0, "get_record");
    assert_eq!(
        calls[1].1,
        json!({
            "table": "dmn_demand",
            "sys_id": "7f029b89c3e7565067bdfd73e40131a1"
        })
    );
}

#[tokio::test]
async fn bridge_forwards_user_lookup() {
    let daemon = MockDaemon::new(contract(&["contract_info", "user_lookup"]));
    let server = bridge(daemon.clone());

    let response = server
        .dispatch(request(
            "tools/call",
            json!({
                "name": "user_lookup",
                "arguments": { "query": "JOW2145" }
            }),
        ))
        .await;

    assert!(response.error.is_none(), "{response:?}");
    let calls = daemon.calls.lock().await;
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[1].0, "user_lookup");
    assert_eq!(calls[1].1, json!({ "query": "JOW2145" }));
}

#[tokio::test]
async fn bridge_forwards_get_work_notes_table_sys_id_lookup() {
    let daemon = MockDaemon::new(contract(&["contract_info", "get_work_notes"]));
    let server = bridge(daemon.clone());
    let sys_id = "7F029B89C3E7565067BDFD73E40131A1";

    let response = server
        .dispatch(request(
            "tools/call",
            json!({
                "name": "get_work_notes",
                "arguments": { "table": "dmn_demand_task", "sys_id": sys_id }
            }),
        ))
        .await;

    assert!(response.error.is_none(), "{response:?}");
    let calls = daemon.calls.lock().await;
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[1].0, "get_work_notes");
    assert_eq!(
        calls[1].1,
        json!({
            "table": "dmn_demand_task",
            "sys_id": "7f029b89c3e7565067bdfd73e40131a1"
        })
    );
}

#[tokio::test]
async fn bridge_rejects_demand_table_lookup_for_resource_plan_get() {
    let daemon = MockDaemon::new(contract(&["contract_info", "get_record"]));
    let server = bridge(daemon.clone());

    let response = server
        .dispatch(request(
            "tools/call",
            json!({
                "name": "resource_plan_get",
                "arguments": {
                    "table": "dmn_demand",
                    "sys_id": "7f029b89c3e7565067bdfd73e40131a1"
                }
            }),
        ))
        .await;

    let error = response
        .error
        .expect("resource_plan_get should reject demand");
    assert_eq!(error.code, -32602);
    assert!(
        error.data.unwrap()["details"]
            .as_str()
            .unwrap()
            .contains("table `dmn_demand` is not allowed")
    );
    assert_eq!(
        daemon.method_names().await,
        vec!["contract_info".to_string()]
    );
}

#[tokio::test]
async fn bridge_keeps_story_tools_number_only() {
    let daemon = MockDaemon::new(contract(&["contract_info", "get_record", "get_children"]));
    let server = bridge(daemon.clone());

    let response = server
        .dispatch(request(
            "tools/call",
            json!({
                "name": "story_get",
                "arguments": {
                    "number": "STRY0010001",
                    "table": "resource_plan",
                    "sys_id": "7f029b89c3e7565067bdfd73e40131a1"
                }
            }),
        ))
        .await;
    assert_eq!(
        response.error.expect("story_get rejects table").code,
        -32602
    );

    let response = server
        .dispatch(request(
            "tools/call",
            json!({
                "name": "story_tasks_list",
                "arguments": {
                    "number": "STRY0010001",
                    "table": "resource_plan",
                    "sys_id": "7f029b89c3e7565067bdfd73e40131a1"
                }
            }),
        ))
        .await;
    assert_eq!(
        response.error.expect("story_tasks_list rejects table").code,
        -32602
    );
    assert_eq!(
        daemon.method_names().await,
        vec!["contract_info".to_string()]
    );
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
async fn bridge_forwards_catalog_plan_and_submit_tools() {
    let daemon = MockDaemon::new(contract(&[
        "contract_info",
        "catalog_plan_request",
        "catalog_submit_request",
    ]));
    let server = bridge(daemon.clone());

    let response = server
        .dispatch(request(
            "tools/call",
            json!({
                "name": "catalog_plan_request",
                "arguments": {
                    "item_sys_id": "300d473b13f00c10906630128144b0d1",
                    "variables": {
                        "business_justification": "Needed for IAM server administration"
                    },
                    "quantity": "1"
                }
            }),
        ))
        .await;
    assert!(response.error.is_none(), "{response:?}");

    let response = server
        .dispatch(request(
            "tools/call",
            json!({
                "name": "catalog_submit_request",
                "arguments": {
                    "plan_id": "catalog-plan-1",
                    "confirmation_token": "confirmation-1",
                    "idempotency_key": "idem-1"
                }
            }),
        ))
        .await;
    assert!(response.error.is_none(), "{response:?}");

    let calls = daemon.calls.lock().await;
    assert_eq!(calls[1].0, "catalog_plan_request");
    assert_eq!(
        calls[1].1["variables"]["business_justification"],
        json!("Needed for IAM server administration")
    );
    assert_eq!(calls[2].0, "catalog_submit_request");
    assert_eq!(calls[2].1["plan_id"], json!("catalog-plan-1"));
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
    let tools = result["tools"].as_array().unwrap();
    for tool in tools {
        let name = tool["name"].as_str().expect("tool name");
        let input_schema = &tool["inputSchema"];
        assert_eq!(input_schema["type"], "object", "{name}");
        assert_no_top_level_schema_composition(name, input_schema);
    }
    let names = tools
        .iter()
        .filter_map(|tool| tool.get("name").and_then(Value::as_str))
        .collect::<Vec<_>>();

    assert!(names.contains(&"get_record"));
    assert!(names.contains(&"resource_plan_get"));
    assert!(names.contains(&"story_get"));
    assert!(names.contains(&"tool_capabilities"));
    assert!(!names.contains(&"knowledge_fetch"));

    let get_record = tools
        .iter()
        .find(|tool| tool["name"] == "get_record")
        .expect("get_record advertised");
    assert!(
        get_record["description"]
            .as_str()
            .expect("get_record description")
            .contains("APM0002456")
    );
    assert!(
        get_record["inputSchema"]["properties"]["number"]["description"]
            .as_str()
            .expect("get_record number description")
            .contains("business_application_query")
    );
    assert_eq!(
        get_record["inputSchema"]["properties"]["table"]["enum"],
        json!([
            "dmn_demand",
            "dmn_demand_task",
            "resource_plan",
            "pm_project",
            "business_application",
            "business_app",
            "cmdb_ci_business_app",
            "server",
            "cmdb_ci_server",
            "cmdb_ci_linux_server",
            "cmdb_ci_win_server"
        ])
    );
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
