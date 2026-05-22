mod support;

use serde_json::json;
use snow_mcp::{JsonRpcRequest, McpServer, planner::is_governed_write_tool};

#[tokio::test]
async fn representative_read_tool_calls_round_trip() {
    let fixture = support::build_fixture_state().await.expect("fixture");
    let server = McpServer::new(fixture.core);

    let get_record = call(&server, "get_record", json!({ "number": "CHG001" }), 1).await;
    assert_eq!(get_record["record"]["number"], "CHG001");
    assert_eq!(get_record["record"]["short_description"], "Database patch");

    let search_records = call(&server, "search_records", json!({ "query": "Database" }), 2).await;
    assert!(
        search_records["results"]
            .as_array()
            .expect("results")
            .iter()
            .any(|result| result["record"]["number"] == "CHG001")
    );

    let search_knowledge = call(
        &server,
        "search_knowledge",
        json!({ "query": "database", "knowledge_base": "IT" }),
        3,
    )
    .await;
    assert_eq!(search_knowledge["articles"][0]["record"]["number"], "KB001");

    let capabilities = call(&server, "tool_capabilities", json!({}), 4).await;
    assert_eq!(capabilities["default_mode"], "read_only");
    assert!(
        capabilities["tools"]
            .as_array()
            .expect("capabilities")
            .iter()
            .all(|tool| tool["read_only"] == true)
    );
}

#[tokio::test]
async fn foreground_refuses_governed_write_tools_with_daemon_required() {
    let fixture = support::build_fixture_state().await.expect("fixture");
    let server = McpServer::new(fixture.core);

    for (idx, name) in [
        "story_plan_create",
        "story_apply_create",
        "story_plan_update",
        "story_apply_update",
        "story_task_plan_create",
        "story_task_apply_create",
        "story_task_plan_update",
        "story_task_apply_update",
        "timecard_plan_set_hours",
        "timecard_apply_set_hours",
        "work_note_plan_add",
        "work_note_apply_add",
    ]
    .iter()
    .enumerate()
    {
        assert!(is_governed_write_tool(name), "{name}");
        let response = server
            .dispatch(JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: "tools/call".to_string(),
                params: json!({ "name": name, "arguments": {} }),
                id: Some(json!(idx + 10)),
            })
            .await;
        let error = response
            .error
            .unwrap_or_else(|| panic!("expected daemon-required error for {name}"));
        assert_eq!(error.code, -32044, "{name}");
        assert_eq!(error.message, "DAEMON_REQUIRED_FOR_WRITE", "{name}");
        assert_eq!(error.data.unwrap()["reason"], "daemon_not_attached");
    }
}

async fn call(
    server: &McpServer,
    name: &str,
    arguments: serde_json::Value,
    id: i64,
) -> serde_json::Value {
    server
        .dispatch(JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "tools/call".to_string(),
            params: json!({ "name": name, "arguments": arguments }),
            id: Some(json!(id)),
        })
        .await
        .result
        .unwrap_or_else(|| panic!("result for {name}"))
}
