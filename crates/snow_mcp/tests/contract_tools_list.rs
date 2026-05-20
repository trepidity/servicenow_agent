mod support;

use serde_json::json;
use snow_mcp::{JsonRpcRequest, McpServer, tools::ToolRegistry};

const STORY_PLAN_TOOLS: &[&str] = &[
    "story_plan_create",
    "story_plan_update",
    "story_task_plan_create",
    "story_task_plan_update",
];

const STORY_APPLY_TOOLS: &[&str] = &[
    "story_apply_create",
    "story_apply_update",
    "story_task_apply_create",
    "story_task_apply_update",
];

const STORY_WRITE_TOOLS: &[&str] = &[
    "story_plan_create",
    "story_apply_create",
    "story_plan_update",
    "story_apply_update",
    "story_task_plan_create",
    "story_task_apply_create",
    "story_task_plan_update",
    "story_task_apply_update",
];

#[tokio::test]
async fn tools_list_contains_daemon_read_parity_tools_with_schema_shape() {
    let fixture = support::build_fixture_state().await.expect("fixture");
    let server = McpServer::new(fixture.core);

    let response = server
        .dispatch(JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "tools/list".to_string(),
            params: json!({}),
            id: Some(json!(1)),
        })
        .await;

    let result = response.result.expect("result");
    let tools = result["tools"].as_array().expect("tools array");
    for expected in [
        "get_record",
        "get_approval",
        "search_records",
        "search_knowledge",
        "get_article",
        "kb_semantic_search",
        "list_records",
        "list_knowledge_bases",
        "list_categories",
        "list_knowledge_articles",
        "list_my_tasks",
        "list_my_approvals",
        "list_my_projects",
        "get_children",
        "get_work_notes",
        "story_get",
        "story_tasks_list",
        "story_plan_create",
        "story_apply_create",
        "story_plan_update",
        "story_apply_update",
        "story_task_plan_create",
        "story_task_apply_create",
        "story_task_plan_update",
        "story_task_apply_update",
        "vault_path",
        "kb_sync",
        "kb_list_tags",
        "kb_status",
        "kb_semantic_status",
        "kb_semantic_rebuild",
        "repair_vault",
        "rebuild_cache",
        "verify_vault",
        "tool_capabilities",
        "policy_describe",
    ] {
        let tool = tools
            .iter()
            .find(|tool| tool["name"] == expected)
            .unwrap_or_else(|| panic!("missing tool {expected}"));
        assert!(tool["description"].as_str().is_some());
        assert_eq!(tool["inputSchema"]["type"], "object");
        assert_eq!(tool["outputSchema"]["type"], "object");
        assert_eq!(tool["schema_version"], "1.0");
    }
}

#[test]
fn eight_story_tools_registered() {
    let registry = ToolRegistry::new();

    for expected in STORY_WRITE_TOOLS {
        let tool = registry
            .metadata()
            .iter()
            .find(|tool| tool.name == *expected)
            .unwrap_or_else(|| panic!("missing Story board write tool {expected}"));
        assert_eq!(tool.input_schema["type"], "object");
        assert_eq!(tool.output_schema["type"], "object");
    }

    for expected in ["story_get", "story_tasks_list"] {
        let tool = registry
            .metadata()
            .iter()
            .find(|tool| tool.name == expected)
            .unwrap_or_else(|| panic!("missing Story read tool {expected}"));
        assert_eq!(
            tool.input_schema,
            json!({"type":"object","properties":{"number":{"type":"string"}},"required":["number"]})
        );
        assert_eq!(tool.output_schema, json!({"type":"object"}));
        assert!(tool.default_enabled);
        assert!(!tool.requires_confirmation);
    }
}

#[test]
fn apply_default_enabled_false() {
    let registry = ToolRegistry::new();

    for expected in STORY_PLAN_TOOLS {
        let tool = registry
            .metadata()
            .iter()
            .find(|tool| tool.name == *expected)
            .unwrap_or_else(|| panic!("missing Story plan tool {expected}"));
        assert!(tool.default_enabled, "{expected} should default enabled");
        assert!(
            !tool.requires_confirmation,
            "{expected} should not require confirmation"
        );
    }

    for expected in STORY_APPLY_TOOLS {
        let tool = registry
            .metadata()
            .iter()
            .find(|tool| tool.name == *expected)
            .unwrap_or_else(|| panic!("missing Story apply tool {expected}"));
        assert!(!tool.default_enabled, "{expected} should default disabled");
        assert!(
            tool.requires_confirmation,
            "{expected} should require confirmation"
        );
    }
}

#[test]
fn no_generic_create_record_registered() {
    let registry = ToolRegistry::new();
    let names = registry
        .metadata()
        .iter()
        .map(|tool| tool.name.as_str())
        .collect::<Vec<_>>();

    for unexpected in ["create_record", "update_record", "delete_record"] {
        assert!(
            !names.contains(&unexpected),
            "{unexpected} must not be exposed as an MCP tool"
        );
    }
}

#[test]
fn schemas_include_required_fields_for_create_update_apply() {
    let registry = ToolRegistry::new();
    let tool = |name: &str| {
        registry
            .metadata()
            .iter()
            .find(|tool| tool.name == name)
            .unwrap_or_else(|| panic!("missing tool {name}"))
    };

    let story_create = &tool("story_plan_create").input_schema;
    assert_eq!(story_create["type"], "object");
    assert_eq!(
        story_create["required"],
        json!(["short_description", "description"])
    );
    assert_eq!(
        story_create["properties"]["short_description"]["type"],
        "string"
    );
    assert_eq!(story_create["properties"]["description"]["type"], "string");

    let story_update = &tool("story_plan_update").input_schema;
    assert_eq!(story_update["type"], "object");
    assert_eq!(story_update["required"], json!(["number"]));
    assert_eq!(
        story_update["properties"]["number"]["pattern"],
        "^STRY\\d+$"
    );

    let task_create = &tool("story_task_plan_create").input_schema;
    assert_eq!(task_create["type"], "object");
    assert_eq!(
        task_create["required"],
        json!(["parent_story_number", "short_description"])
    );
    assert_eq!(
        task_create["properties"]["parent_story_number"]["pattern"],
        "^STRY\\d+$"
    );

    let task_update = &tool("story_task_plan_update").input_schema;
    assert_eq!(task_update["type"], "object");
    assert_eq!(task_update["required"], json!(["number"]));
    assert_eq!(
        task_update["properties"]["number"]["pattern"],
        "^SCTASK\\d+$"
    );

    for apply in ["story_apply_create", "story_task_apply_create"] {
        let schema = &tool(apply).input_schema;
        assert_eq!(schema["type"], "object");
        assert_eq!(
            schema["required"],
            json!(["plan_id", "confirmation_token", "idempotency_key"])
        );
    }

    for apply in ["story_apply_update", "story_task_apply_update"] {
        let schema = &tool(apply).input_schema;
        assert_eq!(schema["type"], "object");
        assert_eq!(
            schema["required"],
            json!([
                "plan_id",
                "confirmation_token",
                "idempotency_key",
                "concurrency_token"
            ])
        );
        assert_eq!(schema["properties"]["concurrency_token"]["type"], "object");
    }

    assert_eq!(tool("story_apply_create").output_schema["type"], "object");
    assert_eq!(
        tool("story_apply_create").output_schema["properties"]["plan_id"]["type"],
        "string"
    );
    assert_eq!(tool("story_plan_update").output_schema["type"], "object");
    assert_eq!(
        tool("story_plan_update").output_schema["required"],
        json!([
            "plan_id",
            "op_hash",
            "preview",
            "expires_at",
            "confirmation_token",
            "idempotency_key",
            "concurrency_token"
        ])
    );
}
