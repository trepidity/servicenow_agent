#[path = "support/record_query.rs"]
mod record_query_support;

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

    async fn calls(&self) -> Vec<(String, Value)> {
        self.calls.lock().await.clone()
    }
}

fn incident_group_page_payload() -> Value {
    json!({
        "records": [{
            "sys_id": "0000000000000000000000000000aa01",
            "number": "<INC_1>",
            "table": "incident",
            "resource_type": "incident",
            "state": "Pending",
            "short_description": "Ticket",
            "description": "",
            "fields": {},
            "work_notes": [],
            "comments": [],
            "parent": null,
            "children": [],
            "references": {},
            "synced_at": "2026-08-17T00:00:00Z",
            "source": "servicenow"
        }],
        "next_cursor": "0000000000000000000000000000aa01",
        "complete": false,
        "limit": 25,
        "rows_inspected": 25,
        "state": { "value": "3", "label": "Pending" }
    })
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
            "get_article_fresh" => Ok(json!({
                "article": {
                    "number": params.get("number").and_then(Value::as_str).unwrap_or("UNKNOWN"),
                    "body_cached": true
                },
                "markdown": "# Fresh KB article"
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
            "incident_list_by_assignment_group"
                if params.get("state").and_then(Value::as_str) == Some("Awaiting Vendor") =>
            {
                Err(Error::DaemonJsonRpc {
                    code: -32602,
                    message: "invalid params".to_string(),
                    data: Some(json!({
                        "field": "state",
                        "requested": "Awaiting Vendor",
                        "ambiguous": false,
                        "choices": [{ "value": "3", "label": "Pending" }]
                    })),
                })
            }
            "incident_list_by_assignment_group" => Ok(incident_group_page_payload()),
            "record_query" => Ok(record_query_support::expected_page()),
            "list_records" if params.get("filter").is_some() => Err(Error::DaemonJsonRpc {
                code: -32602,
                message: "invalid params".to_string(),
                data: Some(json!({ "details": "unknown field `filter`" })),
            }),
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
                "arguments": { "table": "change_request", "sys_id": sys_id }
            }),
        ))
        .await;

    assert!(response.error.is_none(), "{response:?}");
    let result = response.result.expect("MCP tool result");
    assert_eq!(
        result["structuredContent"],
        json!({
            "record": {
                "number": "UNKNOWN",
                "table": "change_request",
                "sys_id": "0123456789abcdef0123456789abcdef"
            }
        }),
        "the bridge must preserve the daemon's record contract"
    );
    assert_eq!(
        result["content"][0]["type"],
        json!("text"),
        "a successful MCP call needs a readable content envelope"
    );
    let calls = daemon.calls.lock().await;
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[1].0, "get_record");
    assert_eq!(
        calls[1].1,
        json!({
            "table": "change_request",
            "sys_id": "7f029b89c3e7565067bdfd73e40131a1"
        })
    );
}

/// State-correction data is a caller-recovery contract, not an implementation
/// detail. The bridge must preserve daemon JSON-RPC error code and data.
#[tokio::test]
async fn bridge_preserves_incident_group_state_correction_data() {
    let daemon = MockDaemon::new(contract(&[
        "contract_info",
        "incident_list_by_assignment_group",
    ]));
    let server = bridge(daemon);

    let response = server
        .dispatch(request(
            "tools/call",
            json!({
                "name": "incident_list_by_assignment_group",
                "arguments": {
                    "assignment_group_sys_id": "0000000000000000000000000000ab01",
                    "state": "Awaiting Vendor"
                }
            }),
        ))
        .await;

    let error = response.error.expect("unknown state must be forwarded");
    assert_eq!(error.code, -32602);
    let data = error.data.expect("state correction data");
    assert_eq!(data["field"], json!("state"));
    assert_eq!(data["requested"], json!("Awaiting Vendor"));
    assert_eq!(
        data["choices"],
        json!([{ "value": "3", "label": "Pending" }])
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
                "arguments": { "query": "USER1234" }
            }),
        ))
        .await;

    assert!(response.error.is_none(), "{response:?}");
    let calls = daemon.calls.lock().await;
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[1].0, "user_lookup");
    assert_eq!(calls[1].1, json!({ "query": "USER1234" }));
}

#[tokio::test]
async fn bridge_forwards_business_application_servers_flat_params() {
    let daemon = MockDaemon::new(contract(&["contract_info", "business_application_servers"]));
    let server = bridge(daemon.clone());

    let response = server
        .dispatch(request(
            "tools/call",
            json!({
                "name": "business_application_servers",
                "arguments": {
                    "number": "<APM_NUMBER>",
                    "max_depth": 2,
                    "max_cis": 500,
                    "max_edges": 2000,
                    "max_service_membership_associations": 3000,
                    "max_service_membership_pages": 30,
                    "relationship_type": ["<RELATIONSHIP_TYPE>"],
                    "include_paths": true
                }
            }),
        ))
        .await;

    assert!(response.error.is_none(), "{response:?}");
    let calls = daemon.calls.lock().await;
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[1].0, "business_application_servers");
    assert_eq!(
        calls[1].1,
        json!({
            "number": "<APM_NUMBER>",
            "max_depth": 2,
            "max_cis": 500,
            "max_edges": 2000,
            "max_service_membership_associations": 3000,
            "max_service_membership_pages": 30,
            "relationship_type": ["<RELATIONSHIP_TYPE>"],
            "include_paths": true,
            "persist": false
        })
    );
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
async fn bridge_forwards_resource_plan_list_flat_params() {
    let daemon = MockDaemon::new(contract(&["contract_info", "resource_plan_list"]));
    let server = bridge(daemon.clone());

    let response = server
        .dispatch(request(
            "tools/call",
            json!({
                "name": "resource_plan_list",
                "arguments": {
                    "task_sys_id": "00000000000000000000000000000010",
                    "resource_type": "group",
                    "resource_sys_id": "00000000000000000000000000000020",
                    "state": [1, 3],
                    "limit": 25
                }
            }),
        ))
        .await;

    assert!(response.error.is_none(), "{response:?}");
    let calls = daemon.calls.lock().await;
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[1].0, "resource_plan_list");
    assert_eq!(
        calls[1].1,
        json!({
            "task_sys_id": "00000000000000000000000000000010",
            "resource_type": "group",
            "resource_sys_id": "00000000000000000000000000000020",
            "state": [1, 3],
            "limit": 25
        })
    );
}

/// The bridge forwards every page argument to the daemon unchanged — it does
/// not reshape, drop, or default the caller's paging and state selectors.
///
/// Authority: docs/spec-incident-list-by-assignment-group.md#scope
#[tokio::test]
async fn bridge_forwards_incident_group_page_params_unchanged() {
    let daemon = MockDaemon::new(contract(&[
        "contract_info",
        "incident_list_by_assignment_group",
    ]));
    let server = bridge(daemon.clone());

    let arguments = json!({
        "assignment_group_sys_id": "0000000000000000000000000000ab01",
        "state": "Pending",
        "limit": 25,
        "cursor": "0000000000000000000000000000aa02"
    });
    let response = server
        .dispatch(request(
            "tools/call",
            json!({
                "name": "incident_list_by_assignment_group",
                "arguments": arguments.clone()
            }),
        ))
        .await;

    assert!(response.error.is_none(), "{response:?}");
    let calls = daemon.calls.lock().await;
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[1].0, "incident_list_by_assignment_group");
    assert_eq!(calls[1].1, arguments);
}

#[tokio::test]
async fn bridge_capability_gates_and_forwards_record_query_unchanged() {
    let unavailable = bridge(MockDaemon::new(contract(&["contract_info"])));
    let unavailable_response = unavailable
        .dispatch(request(
            "tools/call",
            json!({"name":"record_query","arguments":{"resource_type":"story"}}),
        ))
        .await;
    assert_eq!(
        unavailable_response.error.expect("unavailable").code,
        -32041
    );

    let daemon = MockDaemon::new(contract(&["contract_info", "record_query"]));
    let server = bridge(daemon.clone());
    let arguments = json!({
        "resource_type": "story",
        "filters": { "text": "identity" },
        "include_description": true,
        "limit": 2,
        "cursor": "00000000000000000000000000000001"
    });
    let response = server
        .dispatch(request(
            "tools/call",
            json!({"name":"record_query","arguments":arguments.clone()}),
        ))
        .await;
    assert!(response.error.is_none(), "{response:?}");
    assert_eq!(
        response.result.expect("result")["structuredContent"],
        record_query_support::expected_page()
    );
    let calls = daemon.calls().await;
    assert_eq!(calls[1], ("record_query".to_string(), arguments));
}

#[tokio::test]
async fn bridge_preserves_legacy_list_filter_rejection() {
    let daemon = MockDaemon::new(contract(&["contract_info", "list_records"]));
    let server = bridge(daemon.clone());
    let response = server
        .dispatch(request(
            "tools/call",
            json!({"name":"list_records","arguments":{"filter":"state=1"}}),
        ))
        .await;
    assert_eq!(response.error.expect("must reject").code, -32602);
    assert_eq!(
        daemon.calls().await[1],
        ("list_records".to_string(), json!({"filter":"state=1"}))
    );
}

#[tokio::test]
async fn bridge_forwards_operational_incident_queue_params_unchanged() {
    let daemon = MockDaemon::new(contract(&[
        "contract_info",
        "incident_assignment_group_queue",
    ]));
    let server = bridge(daemon.clone());
    let arguments = json!({
        "group": "Example Operations",
        "assigned_to": "unassigned",
        "sla_risk": "at_risk",
        "updated_since": "2026-08-17 10:00:00",
        "known_sys_ids": ["0000000000000000000000000000ab01"]
    });
    let response = server
        .dispatch(request(
            "tools/call",
            json!({
                "name": "incident_assignment_group_queue",
                "arguments": arguments.clone()
            }),
        ))
        .await;

    assert!(response.error.is_none(), "{response:?}");
    let calls = daemon.calls.lock().await;
    assert_eq!(calls.len(), 2);
    assert_eq!(calls[1].0, "incident_assignment_group_queue");
    assert_eq!(calls[1].1, arguments);
}

/// A daemon whose contract lacks the method must not have the tool called
/// against it — the bridge advertises only what the daemon supports.
#[tokio::test]
async fn bridge_refuses_incident_group_page_when_daemon_lacks_the_method() {
    let daemon = MockDaemon::new(contract(&["contract_info"]));
    let server = bridge(daemon.clone());

    let response = server
        .dispatch(request(
            "tools/call",
            json!({
                "name": "incident_list_by_assignment_group",
                "arguments": { "assignment_group_sys_id": "0000000000000000000000000000ab01" }
            }),
        ))
        .await;

    let error = response.error.expect("unavailable method should error");
    assert_eq!(error.code, -32041);
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
async fn bridge_forwards_approval_actions_to_daemon_policy_methods() {
    let daemon = MockDaemon::new(contract(&[
        "contract_info",
        "approval_approve",
        "approval_reject",
    ]));
    let server = bridge(daemon.clone());

    let response = server
        .dispatch(request(
            "tools/call",
            json!({
                "name": "approval_approve",
                "arguments": {
                    "approval_sys_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                }
            }),
        ))
        .await;
    assert!(response.error.is_none(), "{response:?}");
    assert_eq!(
        response.result.unwrap()["structuredContent"]["method"],
        json!("approval_approve")
    );

    let response = server
        .dispatch(request(
            "tools/call",
            json!({
                "name": "approval_reject",
                "arguments": {
                    "number": "RITM0010001",
                    "reason": "Insufficient justification."
                }
            }),
        ))
        .await;
    assert!(response.error.is_none(), "{response:?}");
    assert_eq!(
        response.result.unwrap()["structuredContent"]["method"],
        json!("approval_reject")
    );
    assert_eq!(
        daemon.method_names().await,
        vec![
            "contract_info".to_string(),
            "approval_approve".to_string(),
            "approval_reject".to_string()
        ]
    );
    assert_eq!(
        daemon.calls().await[1].1["approval_sys_id"],
        json!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
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
            "change_request",
            "business_application",
            "business_app",
            "cmdb_ci_business_app",
            "server",
            "cmdb_ci_server",
            "cmdb_ci_linux_server",
            "cmdb_ci_win_server",
            "vtb_task"
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
            .any(|resource| resource["name"] == json!("ServiceNow dashboard")
                && resource["uri"] == json!("snow://dashboard"))
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
async fn bridge_routes_knowledge_fetch_fresh_to_fresh_daemon_method() {
    let daemon = MockDaemon::new(contract(&[
        "contract_info",
        "get_article",
        "get_article_fresh",
    ]));
    let server = bridge(daemon.clone());

    let response = server
        .dispatch(request(
            "tools/call",
            json!({
                "name": "knowledge_fetch",
                "arguments": {
                    "number": "KB001",
                    "fresh": true
                }
            }),
        ))
        .await;

    assert!(response.error.is_none(), "{response:?}");
    let calls = daemon.calls().await;
    assert_eq!(calls[1].0, "get_article_fresh");
    assert_eq!(calls[1].1["number"], json!("KB001"));
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
