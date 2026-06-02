mod support;

use serde_json::json;
use snow_mcp::{JsonRpcRequest, McpServer, domain::policy::PolicyConfig, tools::ToolRegistry};

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

const CHANGE_PLAN_TOOLS: &[&str] = &[
    "change_request_plan_create",
    "change_request_plan_update",
    "change_task_plan_create",
    "change_task_plan_update",
];

const CHANGE_APPLY_TOOLS: &[&str] = &[
    "change_request_apply_create",
    "change_request_apply_update",
    "change_task_apply_create",
    "change_task_apply_update",
];

const CHANGE_WRITE_TOOLS: &[&str] = &[
    "change_request_plan_create",
    "change_request_apply_create",
    "change_request_plan_update",
    "change_request_apply_update",
    "change_task_plan_create",
    "change_task_apply_create",
    "change_task_plan_update",
    "change_task_apply_update",
];

const TIMECARD_TOOLS: &[&str] = &[
    "timecard_list",
    "timecard_plan_set_hours",
    "timecard_apply_set_hours",
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
        "user_lookup",
        "user_search",
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
        "catalog_items_search",
        "catalog_item_get",
        "catalog_plan_request",
        "catalog_submit_request",
        "attachment_list",
        "attachment_upload",
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
        "change_request_plan_create",
        "change_request_apply_create",
        "change_request_plan_update",
        "change_request_apply_update",
        "change_task_plan_create",
        "change_task_apply_create",
        "change_task_plan_update",
        "change_task_apply_update",
        "timecard_list",
        "timecard_plan_set_hours",
        "timecard_apply_set_hours",
        "work_note_plan_add",
        "work_note_apply_add",
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
        assert_no_top_level_schema_composition(&tool["inputSchema"]);
        assert_eq!(tool["outputSchema"]["type"], "object");
        assert_eq!(tool["schema_version"], "1.0");
    }
}

fn assert_no_top_level_schema_composition(schema: &serde_json::Value) {
    for keyword in ["oneOf", "anyOf", "allOf"] {
        assert!(
            schema.get(keyword).is_none(),
            "tool inputSchema must not use top-level {keyword}"
        );
    }
}

#[test]
fn get_record_schema_advertises_number_or_allowed_table_sys_id_lookup() {
    let registry = ToolRegistry::new();
    let tool = registry
        .metadata()
        .iter()
        .find(|tool| tool.name == "get_record")
        .expect("get_record registered");

    assert_eq!(tool.input_schema["type"], "object");
    assert_eq!(tool.input_schema["additionalProperties"], json!(false));
    assert_no_top_level_schema_composition(&tool.input_schema);
    assert!(tool.input_schema.get("required").is_none());
    assert_eq!(
        tool.input_schema["properties"]["number"]["type"],
        json!("string")
    );
    assert_eq!(
        tool.input_schema["properties"]["table"]["enum"],
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
    assert_eq!(
        tool.input_schema["properties"]["sys_id"]["pattern"],
        json!("^[0-9a-fA-F]{32}$")
    );
}

#[test]
fn generic_record_tools_warn_away_from_apm_business_application_numbers() {
    let registry = ToolRegistry::new();
    let get_record = registry
        .metadata()
        .iter()
        .find(|tool| tool.name == "get_record")
        .expect("get_record registered");
    let search_records = registry
        .metadata()
        .iter()
        .find(|tool| tool.name == "search_records")
        .expect("search_records registered");

    for text in [
        get_record.description.as_str(),
        get_record.input_schema["description"]
            .as_str()
            .expect("get_record schema description"),
        get_record.input_schema["properties"]["number"]["description"]
            .as_str()
            .expect("get_record number description"),
        search_records.description.as_str(),
        search_records.input_schema["description"]
            .as_str()
            .expect("search_records schema description"),
        search_records.input_schema["properties"]["query"]["description"]
            .as_str()
            .expect("search_records query description"),
    ] {
        assert!(text.contains("APM0002456"), "{text}");
        assert!(
            text.contains("business_application_query")
                || text.contains("business_application_search"),
            "{text}"
        );
    }
}

#[test]
fn business_application_schemas_advertise_apm_number_routing() {
    let registry = ToolRegistry::new();
    let tool = |name: &str| {
        registry
            .metadata()
            .iter()
            .find(|tool| tool.name == name)
            .unwrap_or_else(|| panic!("missing tool {name}"))
    };

    let get = tool("business_application_get");
    assert!(get.description.contains("APM0002456"));
    assert!(
        get.input_schema["properties"].get("number").is_none(),
        "business_application_get does not accept APM number directly"
    );
    assert!(
        get.input_schema["description"]
            .as_str()
            .expect("BA get schema description")
            .contains("business_application_query")
    );
    assert!(
        get.input_schema["properties"]["name"]["description"]
            .as_str()
            .expect("BA get name description")
            .contains("APM0002456")
    );

    let search = tool("business_application_search");
    assert!(search.description.contains("APM0002456"));
    assert!(
        search.input_schema["properties"]["name"]["description"]
            .as_str()
            .expect("BA search name description")
            .contains("business_application_query")
    );

    let query = tool("business_application_query");
    assert!(query.description.contains("APM0002456"));
    assert!(
        query.input_schema["properties"]["filters"]["description"]
            .as_str()
            .expect("BA query filters description")
            .contains("owner for APM0002456")
    );
    assert_eq!(
        query.input_schema["properties"]["filters"]["items"]["properties"]["field"]["description"],
        json!("ServiceNow field name or alias. Use number for APM identifiers such as APM0002456.")
    );
    assert!(
        query.input_schema["properties"]["filters"]["items"]["properties"]["value"]["description"]
            .as_str()
            .expect("BA query filter value description")
            .contains("APM0002456")
    );

    let fields = tool("business_application_fields");
    assert!(fields.description.contains("owner-related fields"));
    assert!(
        fields.input_schema["description"]
            .as_str()
            .expect("BA fields schema description")
            .contains("owner-field mapping")
    );
}

#[test]
fn user_lookup_schema_advertises_exact_user_selectors() {
    let registry = ToolRegistry::new();
    let tool = registry
        .metadata()
        .iter()
        .find(|tool| tool.name == "user_lookup")
        .expect("user_lookup registered");

    assert_eq!(tool.input_schema["type"], "object");
    assert_eq!(tool.input_schema["additionalProperties"], json!(false));
    assert_no_top_level_schema_composition(&tool.input_schema);
    assert!(tool.input_schema.get("required").is_none());
    for field in ["query", "user_name", "email", "employee_number"] {
        assert_eq!(
            tool.input_schema["properties"][field]["type"],
            json!("string")
        );
    }
    assert_eq!(
        tool.input_schema["properties"]["sys_id"]["pattern"],
        json!("^[0-9a-fA-F]{32}$")
    );
    assert_eq!(
        tool.input_schema["properties"]["active"]["default"],
        json!(true)
    );
}

#[test]
fn user_search_schema_advertises_multi_user_filters() {
    let registry = ToolRegistry::new();
    let tool = registry
        .metadata()
        .iter()
        .find(|tool| tool.name == "user_search")
        .expect("user_search registered");

    assert_eq!(tool.input_schema["type"], "object");
    assert_eq!(tool.input_schema["additionalProperties"], json!(false));
    assert_no_top_level_schema_composition(&tool.input_schema);
    assert!(tool.input_schema.get("required").is_none());
    for field in ["first_name", "last_name", "name_contains", "email_contains"] {
        assert_eq!(
            tool.input_schema["properties"][field]["type"],
            json!("string")
        );
    }
    assert_eq!(
        tool.input_schema["properties"]["limit"]["minimum"],
        json!(1)
    );
    assert_eq!(
        tool.input_schema["properties"]["limit"]["maximum"],
        json!(100)
    );
    assert_eq!(
        tool.input_schema["properties"]["limit"]["default"],
        json!(20)
    );
    assert_eq!(
        tool.input_schema["properties"]["active"]["default"],
        json!(true)
    );
    assert!(tool.default_enabled);
    assert!(!tool.requires_confirmation);
}

#[test]
fn resource_plan_schema_only_allows_resource_plan_table_sys_id_lookup() {
    let registry = ToolRegistry::new();
    let tool = registry
        .metadata()
        .iter()
        .find(|tool| tool.name == "resource_plan_get")
        .expect("resource_plan_get registered");

    assert_eq!(tool.input_schema["type"], "object");
    assert_eq!(tool.input_schema["additionalProperties"], json!(false));
    assert_no_top_level_schema_composition(&tool.input_schema);
    assert!(tool.input_schema.get("required").is_none());
    assert_eq!(
        tool.input_schema["properties"]["number"]["type"],
        json!("string")
    );
    assert_eq!(
        tool.input_schema["properties"]["table"]["enum"],
        json!(["resource_plan"])
    );
}

#[test]
fn timecard_tools_registered_with_expected_posture() {
    let registry = ToolRegistry::new();
    let tool = |name: &str| {
        registry
            .metadata()
            .iter()
            .find(|tool| tool.name == name)
            .unwrap_or_else(|| panic!("missing timecard tool {name}"))
    };

    for expected in TIMECARD_TOOLS {
        assert_eq!(tool(expected).input_schema["type"], "object");
        assert_eq!(tool(expected).output_schema["type"], "object");
    }

    assert!(tool("timecard_list").default_enabled);
    assert!(!tool("timecard_list").requires_confirmation);
    assert!(tool("timecard_plan_set_hours").default_enabled);
    assert!(!tool("timecard_plan_set_hours").requires_confirmation);
    assert!(!tool("timecard_apply_set_hours").default_enabled);
    assert!(tool("timecard_apply_set_hours").requires_confirmation);
    assert_eq!(
        tool("timecard_apply_set_hours").input_schema["required"],
        json!([
            "plan_id",
            "confirmation_token",
            "idempotency_key",
            "concurrency_token"
        ])
    );
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
fn eight_change_tools_registered() {
    let registry = ToolRegistry::new();

    for expected in CHANGE_WRITE_TOOLS {
        let tool = registry
            .metadata()
            .iter()
            .find(|tool| tool.name == *expected)
            .unwrap_or_else(|| panic!("missing Change write tool {expected}"));
        assert_eq!(tool.input_schema["type"], "object");
        assert_eq!(tool.output_schema["type"], "object");
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

    for expected in CHANGE_PLAN_TOOLS {
        let tool = registry
            .metadata()
            .iter()
            .find(|tool| tool.name == *expected)
            .unwrap_or_else(|| panic!("missing Change plan tool {expected}"));
        assert!(tool.default_enabled, "{expected} should default enabled");
        assert!(
            !tool.requires_confirmation,
            "{expected} should not require confirmation"
        );
    }

    for expected in CHANGE_APPLY_TOOLS {
        let tool = registry
            .metadata()
            .iter()
            .find(|tool| tool.name == *expected)
            .unwrap_or_else(|| panic!("missing Change apply tool {expected}"));
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
    for field in [
        "cmdb_ci",
        "u_story_owner",
        "sprint",
        "assignment_group",
        "parent",
        "vendor",
        "team",
        "release_scrum",
        "state",
        "u_impacted_users",
        "u_release_notes",
        "u_lead_dev",
        "u_division",
        "u_region",
        "u_location",
        "u_type",
        "u_moscow",
        "classification",
        "product",
        "release",
        "project",
        "theme",
        "u_points_est",
    ] {
        assert!(
            story_create["properties"].get(field).is_some(),
            "story_plan_create missing {field}"
        );
    }
    assert_eq!(story_create["properties"]["due_date"]["format"], "date");
    assert_eq!(
        story_create["properties"]["u_desired_delivery_date"]["format"],
        "date"
    );
    assert_eq!(
        story_create["properties"]["u_points_est"]["oneOf"][1]["type"],
        "integer"
    );

    let story_update = &tool("story_plan_update").input_schema;
    assert_eq!(story_update["type"], "object");
    assert_eq!(story_update["required"], json!(["number"]));
    assert_eq!(
        story_update["properties"]["number"]["pattern"],
        "^STRY\\d+$"
    );
    assert_eq!(
        story_update["properties"]["percent_complete"]["maximum"],
        json!(100)
    );
    for field in [
        "cmdb_ci",
        "u_story_owner",
        "sprint",
        "assignment_group",
        "parent",
        "vendor",
        "team",
        "release_scrum",
        "u_impacted_users",
        "u_release_notes",
        "u_lead_dev",
        "u_division",
        "u_region",
        "u_location",
        "u_type",
        "u_moscow",
        "classification",
        "due_date",
        "u_desired_delivery_date",
        "product",
        "release",
        "project",
        "theme",
        "u_points_est",
    ] {
        assert!(
            story_update["properties"].get(field).is_some(),
            "story_plan_update missing {field}"
        );
    }

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
    assert_eq!(
        task_create["properties"]["assignment_group"]["type"],
        "string"
    );

    let task_update = &tool("story_task_plan_update").input_schema;
    assert_eq!(task_update["type"], "object");
    assert_eq!(task_update["required"], json!(["number"]));
    assert_eq!(task_update["properties"]["number"]["pattern"], "^STSK\\d+$");
    assert_eq!(
        task_update["properties"]["remaining_hours"]["minimum"],
        json!(0)
    );
    assert_eq!(
        task_update["properties"]["percent_complete"]["maximum"],
        json!(100)
    );

    let change_create = &tool("change_request_plan_create").input_schema;
    assert_eq!(change_create["type"], "object");
    assert_eq!(
        change_create["required"],
        json!([
            "short_description",
            "description",
            "assignment_group",
            "cmdb_ci",
            "start_date",
            "end_date",
            "change_plan",
            "backout_plan",
            "test_plan"
        ])
    );
    assert_eq!(change_create["properties"]["change_plan"]["type"], "string");
    assert_eq!(
        change_create["properties"]["implementation_plan"]["type"],
        "string"
    );
    for field in [
        "requested_by",
        "u_subcategory",
        "u_division",
        "u_does_this_change_need_cmdb_update",
    ] {
        assert_eq!(
            change_create["properties"][field]["type"], "string",
            "missing field {field}"
        );
    }

    let change_update = &tool("change_request_plan_update").input_schema;
    assert_eq!(change_update["type"], "object");
    assert_eq!(change_update["required"], json!(["number"]));
    assert_eq!(
        change_update["properties"]["number"]["pattern"],
        "^CHG\\d+$"
    );
    assert_eq!(change_update["properties"]["state"]["type"], "string");

    let change_task_create = &tool("change_task_plan_create").input_schema;
    assert_eq!(change_task_create["type"], "object");
    assert_eq!(
        change_task_create["required"],
        json!(["parent_change_number", "short_description"])
    );
    assert_eq!(
        change_task_create["properties"]["parent_change_number"]["pattern"],
        "^CHG\\d+$"
    );

    let change_task_update = &tool("change_task_plan_update").input_schema;
    assert_eq!(change_task_update["type"], "object");
    assert_eq!(change_task_update["required"], json!(["number"]));
    assert_eq!(
        change_task_update["properties"]["number"]["pattern"],
        "^CTASK\\d+$"
    );

    let work_note_plan = &tool("work_note_plan_add").input_schema;
    assert_eq!(work_note_plan["type"], "object");
    assert_eq!(work_note_plan["required"], json!(["number", "work_notes"]));
    assert_eq!(work_note_plan["properties"]["work_notes"]["type"], "string");
    assert!(work_note_plan.get("anyOf").is_none());
    assert_eq!(
        tool("work_note_apply_add").input_schema["required"],
        json!(["plan_id", "confirmation_token", "idempotency_key"])
    );

    let attachment_list = &tool("attachment_list").input_schema;
    assert_eq!(attachment_list["type"], "object");
    assert_eq!(attachment_list["required"], json!(["number"]));
    assert_eq!(
        attachment_list["properties"]["number"]["pattern"],
        "^[A-Z]+\\d+$"
    );

    let attachment_upload = &tool("attachment_upload").input_schema;
    assert_eq!(attachment_upload["type"], "object");
    assert_eq!(
        attachment_upload["required"],
        json!(["number", "path", "confirm_upload", "idempotency_key"])
    );
    assert_eq!(
        attachment_upload["properties"]["confirm_upload"]["const"],
        true
    );

    for apply in [
        "story_apply_create",
        "story_task_apply_create",
        "change_request_apply_create",
        "change_task_apply_create",
    ] {
        let schema = &tool(apply).input_schema;
        assert_eq!(schema["type"], "object");
        assert_eq!(
            schema["required"],
            json!(["plan_id", "confirmation_token", "idempotency_key"])
        );
    }

    for apply in [
        "story_apply_update",
        "story_task_apply_update",
        "change_request_apply_update",
        "change_task_apply_update",
    ] {
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

#[test]
fn policy_defaults_allow_full_story_form_fields() {
    let cfg = PolicyConfig::default();
    let create = &cfg
        .tools
        .get("story_apply_create")
        .expect("story create policy")
        .field_allowlist;
    let update = &cfg
        .tools
        .get("story_apply_update")
        .expect("story update policy")
        .field_allowlist;

    for field in [
        "short_description",
        "description",
        "acceptance_criteria",
        "cmdb_ci",
        "u_story_owner",
        "sprint",
        "assignment_group",
        "parent",
        "vendor",
        "team",
        "release_scrum",
        "state",
        "u_impacted_users",
        "u_release_notes",
        "u_lead_dev",
        "u_division",
        "u_region",
        "u_location",
        "u_type",
        "u_moscow",
        "classification",
        "due_date",
        "u_desired_delivery_date",
        "product",
        "release",
        "project",
        "theme",
        "priority",
        "epic",
        "story_points",
        "u_points_est",
        "assigned_to",
    ] {
        assert!(
            create.contains(field),
            "story_apply_create allowlist missing {field}"
        );
        assert!(
            update.contains(field),
            "story_apply_update allowlist missing {field}"
        );
    }
    assert!(update.contains("percent_complete"));
}
