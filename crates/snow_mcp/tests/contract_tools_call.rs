mod support;

use serde_json::json;
use snow_mcp::{
    JsonRpcRequest, McpServer,
    domain::policy::is_write_tool,
    planner::is_governed_write_tool,
    tools::records::{RESOURCE_PLAN_LOOKUP_TABLES, RecordLookup, parse_record_lookup},
};

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
    let tools = capabilities["tools"].as_array().expect("capabilities");
    for tool in tools {
        let name = tool["name"].as_str().expect("tool name");
        assert_eq!(tool["read_only"], json!(!is_write_tool(name)));
    }
    let ba_servers = tools
        .iter()
        .find(|tool| tool["name"] == "business_application_servers")
        .expect("business_application_servers capability");
    assert_eq!(ba_servers["mode"], json!("read"));
    assert_eq!(ba_servers["read_only"], json!(true));
    assert_eq!(ba_servers["requires_confirmation"], json!(false));
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
        "change_request_plan_create",
        "change_request_apply_create",
        "change_request_plan_update",
        "change_request_apply_update",
        "change_task_plan_create",
        "change_task_apply_create",
        "change_task_plan_update",
        "change_task_apply_update",
        "timecard_plan_set_hours",
        "timecard_apply_set_hours",
        "work_note_plan_add",
        "work_note_apply_add",
        "catalog_plan_request",
        "catalog_submit_request",
        "approval_approve",
        "approval_reject",
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

#[test]
fn foreground_record_lookup_parser_accepts_generic_table_sys_id_and_rejects_mixed_modes() {
    let lookup = parse_record_lookup(
        &json!({
            "number": "TASK3497879"
        }),
        snow_core::RECORD_LOOKUP_ALLOWED_TABLES,
    )
    .expect("number lookup");
    assert_eq!(lookup, RecordLookup::Number("TASK3497879".to_string()));

    let lookup = parse_record_lookup(
        &json!({
            "table": "dmn_demand",
            "sys_id": "7F029B89C3E7565067BDFD73E40131A1"
        }),
        snow_core::RECORD_LOOKUP_ALLOWED_TABLES,
    )
    .expect("generic lookup");
    assert_eq!(
        lookup,
        RecordLookup::TableSysId {
            table: "dmn_demand".to_string(),
            sys_id: "7f029b89c3e7565067bdfd73e40131a1".to_string(),
        }
    );

    let lookup = parse_record_lookup(
        &json!({
            "table": "dmn_demand_task",
            "sys_id": "7F029B89C3E7565067BDFD73E40131A1"
        }),
        snow_core::RECORD_LOOKUP_ALLOWED_TABLES,
    )
    .expect("demand task lookup");
    assert_eq!(
        lookup,
        RecordLookup::TableSysId {
            table: "dmn_demand_task".to_string(),
            sys_id: "7f029b89c3e7565067bdfd73e40131a1".to_string(),
        }
    );

    let lookup = parse_record_lookup(
        &json!({
            "table": "CHANGE_REQUEST",
            "sys_id": "7F029B89C3E7565067BDFD73E40131A1"
        }),
        snow_core::RECORD_LOOKUP_ALLOWED_TABLES,
    )
    .expect("change_request lookup");
    assert_eq!(
        lookup,
        RecordLookup::TableSysId {
            table: "change_request".to_string(),
            sys_id: "7f029b89c3e7565067bdfd73e40131a1".to_string(),
        }
    );

    let lookup = parse_record_lookup(
        &json!({
            "table": "resource_plan",
            "sys_id": "7F029B89C3E7565067BDFD73E40131A1"
        }),
        RESOURCE_PLAN_LOOKUP_TABLES,
    )
    .expect("resource_plan lookup");
    assert_eq!(
        lookup,
        RecordLookup::TableSysId {
            table: "resource_plan".to_string(),
            sys_id: "7f029b89c3e7565067bdfd73e40131a1".to_string(),
        }
    );

    let err = parse_record_lookup(
        &json!({
            "number": "DMND0012345",
            "table": "dmn_demand",
            "sys_id": "7f029b89c3e7565067bdfd73e40131a1"
        }),
        snow_core::RECORD_LOOKUP_ALLOWED_TABLES,
    )
    .expect_err("mixed number plus table/sys_id should be invalid");
    assert!(err.to_string().contains("provide either number"));

    let err = parse_record_lookup(
        &json!({
            "table": "dmn_demand"
        }),
        snow_core::RECORD_LOOKUP_ALLOWED_TABLES,
    )
    .expect_err("partial table/sys_id lookup should be invalid");
    assert!(err.to_string().contains("table and sys_id"));

    let err = parse_record_lookup(
        &json!({
            "table": "incident",
            "sys_id": "7f029b89c3e7565067bdfd73e40131a1"
        }),
        snow_core::RECORD_LOOKUP_ALLOWED_TABLES,
    )
    .expect_err("unsupported table should be invalid");
    assert!(err.to_string().contains("not allowed"));
}

#[tokio::test]
async fn foreground_rejects_table_sys_id_for_resource_plan_wrong_table_and_story_tools() {
    let fixture = support::build_fixture_state().await.expect("fixture");
    let server = McpServer::new(fixture.core);

    let response = raw_call(
        &server,
        "resource_plan_get",
        json!({
            "table": "dmn_demand",
            "sys_id": "7f029b89c3e7565067bdfd73e40131a1"
        }),
        40,
    )
    .await;
    assert_eq!(
        response
            .error
            .expect("resource_plan_get rejects demand")
            .code,
        -32602
    );

    let response = raw_call(
        &server,
        "story_get",
        json!({
            "number": "STRY0010001",
            "table": "resource_plan",
            "sys_id": "7f029b89c3e7565067bdfd73e40131a1"
        }),
        41,
    )
    .await;
    assert_eq!(
        response.error.expect("story_get rejects table lookup").code,
        -32602
    );

    let response = raw_call(
        &server,
        "story_tasks_list",
        json!({
            "number": "STRY0010001",
            "table": "resource_plan",
            "sys_id": "7f029b89c3e7565067bdfd73e40131a1"
        }),
        42,
    )
    .await;
    assert_eq!(
        response
            .error
            .expect("story_tasks_list rejects table lookup")
            .code,
        -32602
    );
}

#[tokio::test]
async fn foreground_resource_plan_list_rejects_invalid_filters() {
    let fixture = support::build_fixture_state().await.expect("fixture");
    let server = McpServer::new(fixture.core);

    let response = raw_call(
        &server,
        "resource_plan_list",
        json!({
            "parent_number": "PRJ_PLACEHOLDER",
            "task_sys_id": "00000000000000000000000000000001"
        }),
        43,
    )
    .await;
    assert_eq!(
        response
            .error
            .expect("parent selectors must be exclusive")
            .code,
        -32602
    );

    let response = raw_call(
        &server,
        "resource_plan_list",
        json!({
            "resource_sys_id": "00000000000000000000000000000002"
        }),
        44,
    )
    .await;
    assert_eq!(
        response
            .error
            .expect("resource_sys_id requires resource_type")
            .code,
        -32602
    );

    let response = raw_call(&server, "resource_plan_list", json!({ "state": [] }), 45).await;
    assert_eq!(
        response.error.expect("empty state array rejected").code,
        -32602
    );

    let response = raw_call(
        &server,
        "resource_plan_list",
        json!({ "parent_number": "BAD_PARENT" }),
        46,
    )
    .await;
    assert_eq!(
        response.error.expect("unknown parent prefix rejected").code,
        -32602
    );
}

/// The direct (daemon-less) transport enforces the same argument contract as
/// the daemon path: bad group, bad cursor, out-of-range page size, and unknown
/// arguments are all caller errors, refused before any ServiceNow call.
///
/// Authority: docs/spec-incident-list-by-assignment-group.md#scope
#[tokio::test]
async fn foreground_incident_group_page_rejects_invalid_arguments() {
    let fixture = support::build_fixture_state().await.expect("fixture");
    let server = McpServer::new(fixture.core);
    let group = "0000000000000000000000000000ab01";

    for (id, arguments, why) in [
        (60, json!({}), "assignment_group_sys_id is required"),
        (
            61,
            json!({ "assignment_group_sys_id": "Network Support" }),
            "group names are not accepted",
        ),
        (
            62,
            json!({ "assignment_group_sys_id": group, "limit": 201 }),
            "limit above the maximum is rejected, not clamped",
        ),
        (
            63,
            json!({ "assignment_group_sys_id": group, "cursor": "nope" }),
            "cursor must be a sys_id",
        ),
        (
            64,
            json!({ "assignment_group_sys_id": group, "assignment_group": group }),
            "unknown arguments fail closed",
        ),
    ] {
        let response = raw_call(&server, "incident_list_by_assignment_group", arguments, id).await;
        assert_eq!(response.error.expect(why).code, -32602, "{why}");
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

async fn raw_call(
    server: &McpServer,
    name: &str,
    arguments: serde_json::Value,
    id: i64,
) -> snow_mcp::JsonRpcResponse {
    server
        .dispatch(JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "tools/call".to_string(),
            params: json!({ "name": name, "arguments": arguments }),
            id: Some(json!(id)),
        })
        .await
}
