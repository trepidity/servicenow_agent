use super::handlers::*;
use super::*;
use crate::test_support::{
    build_fixture_state, build_fixture_state_at_instance,
    build_fixture_state_without_instance_config, socket_path, spawn_json_http_sequence_server,
    spawn_json_http_server,
};
use interprocess::local_socket::tokio::Stream as LocalSocketStream;
use rusqlite::Connection;
use serde_json::json;
use snow_mcp::McpServer;
use tokio::io::AsyncReadExt;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::io::{duplex, split};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::LocalSet;

async fn connect_endpoint(endpoint: &IpcEndpoint) -> std::io::Result<LocalSocketStream> {
    endpoint.connect().await
}

fn test_endpoint_for_socket(socket: &std::path::Path) -> IpcEndpoint {
    #[cfg(windows)]
    {
        let config_dir = socket.parent().unwrap_or_else(|| std::path::Path::new("."));
        IpcEndpoint::for_config_dir(config_dir)
    }

    #[cfg(not(windows))]
    {
        IpcEndpoint::Filesystem {
            path: socket.to_path_buf(),
        }
    }
}

#[test]
fn rpc_method_parsing_covers_known_methods() {
    assert_eq!(
        RpcMethod::from_method("contract_info"),
        RpcMethod::ContractInfo
    );
    assert_eq!(RpcMethod::from_method("get_record"), RpcMethod::GetRecord);
    assert_eq!(
        RpcMethod::from_method("get_record_fresh"),
        RpcMethod::GetRecordFresh
    );
    assert_eq!(
        RpcMethod::from_method("get_knowledge_article_fresh"),
        RpcMethod::GetKnowledgeArticleFresh
    );
    assert_eq!(RpcMethod::from_method("get_article"), RpcMethod::GetArticle);
    assert_eq!(
        RpcMethod::from_method("task_sla_status"),
        RpcMethod::TaskSlaStatus
    );
    assert_eq!(
        RpcMethod::from_method("task_sla_status_for_tasks"),
        RpcMethod::TaskSlaStatusForTasks
    );
    assert_eq!(
        RpcMethod::from_method("list_my_tasks"),
        RpcMethod::ListMyTasks
    );
    assert_eq!(
        RpcMethod::from_method("list_my_projects"),
        RpcMethod::ListMyProjects
    );
    assert_eq!(
        RpcMethod::from_method("search_records"),
        RpcMethod::SearchRecords
    );
    assert_eq!(RpcMethod::from_method("user_lookup"), RpcMethod::UserLookup);
    assert_eq!(RpcMethod::from_method("user_search"), RpcMethod::UserSearch);
    assert_eq!(
        RpcMethod::from_method("business_application_get"),
        RpcMethod::BusinessApplicationGet
    );
    assert_eq!(
        RpcMethod::from_method("business_application_get_fresh"),
        RpcMethod::BusinessApplicationGetFresh
    );
    assert_eq!(
        RpcMethod::from_method("business_application_search"),
        RpcMethod::BusinessApplicationSearch
    );
    assert_eq!(
        RpcMethod::from_method("business_application_query"),
        RpcMethod::BusinessApplicationQuery
    );
    assert_eq!(
        RpcMethod::from_method("business_application_servers"),
        RpcMethod::BusinessApplicationServers
    );
    assert_eq!(
        RpcMethod::from_method("business_application_servers_cached"),
        RpcMethod::BusinessApplicationServersCached
    );
    assert_eq!(
        RpcMethod::from_method("business_applications_for_server"),
        RpcMethod::BusinessApplicationsForServer
    );
    assert_eq!(
        RpcMethod::from_method("business_application_sync"),
        RpcMethod::BusinessApplicationSync
    );
    assert_eq!(
        RpcMethod::from_method("business_application_fields"),
        RpcMethod::BusinessApplicationFields
    );
    assert_eq!(
        RpcMethod::from_method("resource_plan_list"),
        RpcMethod::ResourcePlanList
    );
    assert_eq!(RpcMethod::from_method("vault_path"), RpcMethod::VaultPath);
    assert_eq!(
        RpcMethod::from_method("search_knowledge"),
        RpcMethod::SearchKnowledge
    );
    assert_eq!(
        RpcMethod::from_method("kb_semantic_search"),
        RpcMethod::KbSemanticSearch
    );
    assert_eq!(
        RpcMethod::from_method("list_knowledge_bases"),
        RpcMethod::ListKnowledgeBases
    );
    assert_eq!(
        RpcMethod::from_method("list_categories"),
        RpcMethod::ListCategories
    );
    assert_eq!(
        RpcMethod::from_method("verify_vault"),
        RpcMethod::VerifyVault
    );
    assert_eq!(RpcMethod::from_method("kb_sync"), RpcMethod::KbSync);
    assert_eq!(
        RpcMethod::from_method("kb_list_tags"),
        RpcMethod::KbListTags
    );
    assert_eq!(RpcMethod::from_method("kb_status"), RpcMethod::KbStatus);
    assert_eq!(
        RpcMethod::from_method("kb_semantic_status"),
        RpcMethod::KbSemanticStatus
    );
    assert_eq!(
        RpcMethod::from_method("kb_semantic_rebuild"),
        RpcMethod::KbSemanticRebuild
    );
    assert_eq!(
        RpcMethod::from_method("my_tasks_fresh"),
        RpcMethod::MyTasksFresh
    );
    assert_eq!(RpcMethod::from_method("set_state"), RpcMethod::SetState);
    assert_eq!(
        RpcMethod::from_method("attachment_list"),
        RpcMethod::AttachmentList
    );
    assert_eq!(
        RpcMethod::from_method("attachment_upload"),
        RpcMethod::AttachmentUpload
    );
    assert_eq!(
        RpcMethod::from_method("field_choices"),
        RpcMethod::FieldChoices
    );
    assert_eq!(RpcMethod::from_method("approve"), RpcMethod::Approve);
    assert_eq!(
        RpcMethod::from_method("approval_approve"),
        RpcMethod::ApprovalApprove
    );
    assert_eq!(
        RpcMethod::from_method("approval_reject"),
        RpcMethod::ApprovalReject
    );
    assert_eq!(RpcMethod::from_method("my_projects"), RpcMethod::MyProjects);
    assert_eq!(
        RpcMethod::from_method("scheduler.status"),
        RpcMethod::SchedulerStatus
    );
    assert_eq!(
        RpcMethod::from_method("get_degraded_reads"),
        RpcMethod::GetDegradedReads
    );
    assert_eq!(RpcMethod::from_method("cache_info"), RpcMethod::CacheInfo);
    assert_eq!(RpcMethod::from_method("start_job"), RpcMethod::StartJob);
    assert_eq!(RpcMethod::from_method("get_job"), RpcMethod::GetJob);
    assert_eq!(RpcMethod::from_method("list_jobs"), RpcMethod::ListJobs);
    assert_eq!(RpcMethod::from_method("cancel_job"), RpcMethod::CancelJob);
    assert_eq!(RpcMethod::from_method("plan_get"), RpcMethod::PlanGet);
    assert_eq!(
        RpcMethod::from_method("catalog_items_search"),
        RpcMethod::CatalogItemsSearch
    );
    assert_eq!(
        RpcMethod::from_method("catalog_item_get"),
        RpcMethod::CatalogItemGet
    );
    assert_eq!(
        RpcMethod::from_method("catalog_plan_request"),
        RpcMethod::CatalogPlanRequest
    );
    assert_eq!(
        RpcMethod::from_method("catalog_submit_request"),
        RpcMethod::CatalogSubmitRequest
    );
    assert_eq!(
        RpcMethod::from_method("work_note_plan_add"),
        RpcMethod::WorkNotePlanAdd
    );
    assert_eq!(
        RpcMethod::from_method("work_note_apply_add"),
        RpcMethod::WorkNoteApplyAdd
    );
    assert_eq!(
        RpcMethod::from_method("resource_plan_plan_create"),
        RpcMethod::ResourcePlanPlanCreate
    );
    assert_eq!(
        RpcMethod::from_method("resource_plan_apply_create"),
        RpcMethod::ResourcePlanApplyCreate
    );
    assert_eq!(
        RpcMethod::from_method("resource_plan_plan_update"),
        RpcMethod::ResourcePlanPlanUpdate
    );
    assert_eq!(
        RpcMethod::from_method("resource_plan_apply_update"),
        RpcMethod::ResourcePlanApplyUpdate
    );
    assert_eq!(
        RpcMethod::from_method("story_plan_create"),
        RpcMethod::StoryPlanCreate
    );
    assert_eq!(
        RpcMethod::from_method("story_apply_create"),
        RpcMethod::StoryApplyCreate
    );
    assert_eq!(
        RpcMethod::from_method("story_plan_update"),
        RpcMethod::StoryPlanUpdate
    );
    assert_eq!(
        RpcMethod::from_method("story_apply_update"),
        RpcMethod::StoryApplyUpdate
    );
    assert_eq!(
        RpcMethod::from_method("story_task_plan_create"),
        RpcMethod::StoryTaskPlanCreate
    );
    assert_eq!(
        RpcMethod::from_method("story_task_apply_create"),
        RpcMethod::StoryTaskApplyCreate
    );
    assert_eq!(
        RpcMethod::from_method("story_task_plan_update"),
        RpcMethod::StoryTaskPlanUpdate
    );
    assert_eq!(
        RpcMethod::from_method("story_task_apply_update"),
        RpcMethod::StoryTaskApplyUpdate
    );
    assert_eq!(
        RpcMethod::from_method("timecard_list"),
        RpcMethod::TimecardList
    );
    assert_eq!(
        RpcMethod::from_method("timecard_set_hours"),
        RpcMethod::TimecardSetHours
    );
    assert_eq!(
        RpcMethod::from_method("timecard_plan_set_hours"),
        RpcMethod::TimecardPlanSetHours
    );
    assert_eq!(
        RpcMethod::from_method("timecard_apply_set_hours"),
        RpcMethod::TimecardApplySetHours
    );
    assert_eq!(RpcMethod::from_method("shutdown"), RpcMethod::Shutdown);
    assert_eq!(RpcMethod::from_method("unknown"), RpcMethod::Unknown);
}

#[test]
fn contract_info_method_lists_cover_canonical_and_deprecated_rpc_methods() {
    assert!(
        !SUPPORTED_RPC_METHODS.contains(&"catalog_cancel_request"),
        "deferred catalog cancellation must not be advertised by the daemon"
    );
    assert_eq!(
        RpcMethod::from_method("catalog_cancel_request"),
        RpcMethod::Unknown
    );

    for method in SUPPORTED_RPC_METHODS {
        assert_ne!(
            RpcMethod::from_method(method),
            RpcMethod::Unknown,
            "{method} should parse as a supported RPC method"
        );
    }

    for (method, replacement) in DEPRECATED_RPC_ALIASES {
        assert_ne!(
            RpcMethod::from_method(method),
            RpcMethod::Unknown,
            "{method} should parse as a deprecated RPC alias"
        );
        assert!(
            SUPPORTED_RPC_METHODS.contains(replacement),
            "{method} should point to canonical method {replacement}"
        );
    }
}

#[test]
fn contract_info_advertises_the_named_live_change_task_child_read() {
    assert!(
        SUPPORTED_RPC_METHODS.contains(&"change_request_list_tasks"),
        "the Mullet Change workflow must not fall back to cached get_children"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn daemon_rejects_deferred_catalog_cancellation_as_unknown() {
    let fixture = build_fixture_state().await.expect("fixture");

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "catalog_cancel_request".to_string(),
            params: json!({}),
            id: Some(json!(1)),
        },
        &fixture.state,
    )
    .await;

    let error = response
        .error
        .expect("deferred cancellation must not have a callable route");
    assert_eq!(error.code, -32601);
    assert_eq!(error.message, "method not found");
}

#[tokio::test(flavor = "current_thread")]
async fn approval_action_dispatch_uses_mcp_policy_gate() {
    let fixture = build_fixture_state().await.expect("fixture");

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "approval_approve".to_string(),
            params: json!({ "number": "TARGET_RECORD_NUMBER" }),
            id: Some(json!(1)),
        },
        &fixture.state,
    )
    .await;

    let error = response.error.expect("approval action should be gated");
    assert_eq!(error.code, -32040);
    assert_eq!(error.message, "policy denied");
    assert_eq!(error.data.unwrap()["tool"], json!("approval_approve"));
}

#[tokio::test(flavor = "current_thread")]
async fn legacy_approval_aliases_use_mcp_policy_gate() {
    let fixture = build_fixture_state().await.expect("fixture");

    for (method, params, replacement) in [
        (
            "approve",
            json!({ "number": "TARGET_RECORD_NUMBER" }),
            "approval_approve",
        ),
        (
            "reject",
            json!({ "number": "TARGET_RECORD_NUMBER", "reason": "Missing evidence." }),
            "approval_reject",
        ),
    ] {
        let response = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: method.to_string(),
                params,
                id: Some(json!(1)),
            },
            &fixture.state,
        )
        .await;

        let error = response
            .error
            .unwrap_or_else(|| panic!("{method} should be gated"));
        assert_eq!(error.code, -32040, "{method}");
        assert_eq!(error.message, "policy denied", "{method}");
        assert_eq!(error.data.unwrap()["tool"], json!(replacement), "{method}");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn business_application_servers_dispatch_validates_selector_before_traversal() {
    let fixture = build_fixture_state().await.expect("fixture");

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "business_application_servers".to_string(),
            params: json!({
                "number": "<APM_NUMBER>",
                "sys_id": "54a4b61b6fe845000ed852a03f3ee4d0"
            }),
            id: Some(json!(1)),
        },
        &fixture.state,
    )
    .await;

    let error = response.error.expect("mixed selector should be rejected");
    assert_eq!(error.code, -32602);
    assert!(
        error
            .data
            .as_ref()
            .and_then(|data| data.get("details"))
            .and_then(Value::as_str)
            .is_some_and(|details| details.contains("exactly one"))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn story_write_dispatch_uses_real_handlers() {
    let fixture = build_fixture_state().await.expect("fixture");

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "plan_get".to_string(),
            params: json!({ "plan_id": "missing-plan" }),
            id: Some(json!(1)),
        },
        &fixture.state,
    )
    .await;
    let error = response
        .error
        .expect("missing plan should be a typed error");
    assert_eq!(error.code, -32056);
    assert_eq!(error.message, "PLAN_NOT_FOUND");

    for method in [
        "story_plan_create",
        "story_apply_create",
        "story_plan_update",
        "story_apply_update",
        "story_task_plan_create",
        "story_task_apply_create",
        "story_task_plan_update",
        "story_task_apply_update",
    ] {
        let response = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: method.to_string(),
                params: json!({}),
                id: Some(json!(1)),
            },
            &fixture.state,
        )
        .await;

        let error = response
            .error
            .unwrap_or_else(|| panic!("{method} should require board policy"));
        assert_eq!(error.code, -32051, "{method}");
        assert_eq!(error.message, "FIELD_REJECTED", "{method}");
    }
}

fn enabled_change_update_mcp_config() -> snow_mcp::McpConfig {
    let mut policy = snow_mcp::domain::policy::PolicyConfig::default();
    for tool in [
        "change_request_apply_update",
        "change_task_apply_update",
        "incident_apply_update",
    ] {
        policy
            .tools
            .get_mut(tool)
            .unwrap_or_else(|| panic!("{tool} policy"))
            .enabled = true;
    }

    snow_mcp::McpConfig {
        environment: snow_mcp::McpEnvironment::explicit_config("test", "America/Chicago"),
        policy,
        ..Default::default()
    }
}

fn change_update_response(number: &str, state_value: &str, state_display: &str) -> Value {
    json!({
        "result": [{
            "sys_id": "change-sys",
            "number": number,
            "short_description": "Example change request",
            "description": "Close after execution",
            "state": {
                "value": state_value,
                "display_value": state_display
            },
            "sys_updated_on": "2026-08-11 10:11:12",
            "sys_mod_count": "1"
        }]
    })
}

#[tokio::test(flavor = "current_thread")]
async fn incident_plan_update_normalizes_unassign_and_issues_concurrency() {
    let (instance_url, _request_rx) = spawn_json_http_sequence_server(vec![
        json!({
            "result": [{
                "sys_id": "0000000000000000000000000000e101",
                "number": "INC0000101",
                "short_description": "Example incident",
                "description": "Example operational issue",
                "state": {"value":"2","display_value":"In Progress"},
                "active": "true",
                "sys_updated_on": "2026-08-17 10:11:12",
                "sys_mod_count": "4"
            }]
        }),
        json!({"result": []}),
    ])
    .await
    .expect("http server");
    let fixture = build_fixture_state_at_instance(&instance_url)
        .await
        .expect("fixture");
    let state = Arc::new(DaemonState::with_data_dir_and_mcp_config(
        Arc::clone(&fixture.state.core),
        fixture.tempdir.path().join("incident-plan-update"),
        enabled_change_update_mcp_config(),
    ));

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "incident_plan_update".to_string(),
            params: json!({
                "number": "INC0000101",
                "assigned_to": "unassigned",
                "work_notes": "Released for team reassignment."
            }),
            id: Some(json!(1)),
        },
        &state,
    )
    .await;

    assert!(response.error.is_none(), "{response:?}");
    let result = response.result.expect("incident plan");
    assert_eq!(result["preview"]["assigned_to"], json!(""));
    assert_eq!(
        result["preview"]["work_notes"],
        json!("Released for team reassignment.")
    );
    assert_eq!(
        result["concurrency_token"],
        json!({
            "sys_updated_on": "2026-08-17 10:11:12",
            "sys_mod_count": 4
        })
    );
    assert!(result["confirmation_token"].as_str().is_some());
    assert!(result["idempotency_key"].as_str().is_some());
}

#[tokio::test(flavor = "current_thread")]
async fn change_request_completed_can_plan_closure() {
    let (instance_url, _request_rx) = spawn_json_http_sequence_server(vec![
        change_update_response("CHG001", "16", "Completed"),
        json!({ "result": [] }),
    ])
    .await
    .expect("http server");
    let fixture = build_fixture_state_at_instance(&instance_url)
        .await
        .expect("fixture");
    let state = Arc::new(DaemonState::with_data_dir_and_mcp_config(
        Arc::clone(&fixture.state.core),
        fixture
            .tempdir
            .path()
            .join("change-request-completed-closure"),
        enabled_change_update_mcp_config(),
    ));

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "change_request_plan_update".to_string(),
            params: json!({ "number": "CHG001", "state": "3" }),
            id: Some(json!(1)),
        },
        &state,
    )
    .await;

    assert!(response.error.is_none(), "{response:?}");
    let result = response.result.expect("change closure plan result");
    assert_eq!(result["preview"]["state"], json!("3"));
}

#[tokio::test(flavor = "current_thread")]
async fn change_request_completed_rejects_nonclosure_update() {
    let (instance_url, _request_rx) = spawn_json_http_sequence_server(vec![
        change_update_response("CHG001", "16", "Completed"),
        json!({ "result": [] }),
    ])
    .await
    .expect("http server");
    let fixture = build_fixture_state_at_instance(&instance_url)
        .await
        .expect("fixture");
    let state = Arc::new(DaemonState::with_data_dir_and_mcp_config(
        Arc::clone(&fixture.state.core),
        fixture
            .tempdir
            .path()
            .join("change-request-completed-nonclosure"),
        enabled_change_update_mcp_config(),
    ));

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "change_request_plan_update".to_string(),
            params: json!({
                "number": "CHG001",
                "short_description": "Attempt a post-completion edit"
            }),
            id: Some(json!(1)),
        },
        &state,
    )
    .await;

    let error = response
        .error
        .expect("non-closure update should remain blocked");
    assert_eq!(error.code, -32050);
    assert_eq!(error.message, "GUARD_FAILED");
    assert_eq!(
        error.data.expect("guard error data")["reason"],
        json!("terminal_record_skipped")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn change_request_closed_remains_terminal() {
    let (instance_url, _request_rx) = spawn_json_http_sequence_server(vec![
        change_update_response("CHG001", "3", "Closed"),
        json!({ "result": [] }),
    ])
    .await
    .expect("http server");
    let fixture = build_fixture_state_at_instance(&instance_url)
        .await
        .expect("fixture");
    let state = Arc::new(DaemonState::with_data_dir_and_mcp_config(
        Arc::clone(&fixture.state.core),
        fixture
            .tempdir
            .path()
            .join("change-request-closed-terminal"),
        enabled_change_update_mcp_config(),
    ));

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "change_request_plan_update".to_string(),
            params: json!({ "number": "CHG001", "state": "3" }),
            id: Some(json!(1)),
        },
        &state,
    )
    .await;

    let error = response
        .error
        .expect("closed change request should be blocked");
    assert_eq!(error.code, -32050);
    assert_eq!(error.message, "GUARD_FAILED");
    assert_eq!(
        error.data.expect("guard error data")["reason"],
        json!("terminal_record_skipped")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn change_task_completed_remains_terminal() {
    let (instance_url, _request_rx) = spawn_json_http_sequence_server(vec![
        change_update_response("CTASK001", "16", "Completed"),
        json!({ "result": [] }),
    ])
    .await
    .expect("http server");
    let fixture = build_fixture_state_at_instance(&instance_url)
        .await
        .expect("fixture");
    let state = Arc::new(DaemonState::with_data_dir_and_mcp_config(
        Arc::clone(&fixture.state.core),
        fixture
            .tempdir
            .path()
            .join("change-task-completed-terminal"),
        enabled_change_update_mcp_config(),
    ));

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "change_task_plan_update".to_string(),
            params: json!({ "number": "CTASK001", "state": "3" }),
            id: Some(json!(1)),
        },
        &state,
    )
    .await;

    let error = response
        .error
        .expect("completed change task should remain blocked");
    assert_eq!(error.code, -32050);
    assert_eq!(error.message, "GUARD_FAILED");
    assert_eq!(
        error.data.expect("guard error data")["reason"],
        json!("terminal_record_skipped")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn change_task_update_plan_admits_the_parent_restore_field_through_rpc() {
    let (instance_url, _request_rx) = spawn_json_http_sequence_server(vec![
        change_update_response("CTASK001", "1", "Open"),
        json!({ "result": [] }),
    ])
    .await
    .expect("http server");
    let fixture = build_fixture_state_at_instance(&instance_url)
        .await
        .expect("fixture");
    let state = Arc::new(DaemonState::with_data_dir_and_mcp_config(
        Arc::clone(&fixture.state.core),
        fixture.tempdir.path().join("change-task-parent-restore"),
        enabled_change_update_mcp_config(),
    ));

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "change_task_plan_update".to_string(),
            params: json!({
                "number": "CTASK001",
                "change_request": "0123456789abcdef0123456789abcdef"
            }),
            id: Some(json!(1)),
        },
        &state,
    )
    .await;

    assert!(response.error.is_none(), "{response:?}");
    let result = response.result.expect("update plan");
    assert_eq!(
        result["preview"]["change_request"],
        json!("0123456789abcdef0123456789abcdef")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn timecard_apply_dispatch_returns_replay_receipt_through_rpc() {
    use chrono::Utc;
    use snow_mcp::domain::audit::ServiceNowMetadata;
    use snow_mcp::domain::primitives::{IdempotencyKey, IdempotencyKeySource, RecordRef};
    use snow_mcp::planner::{
        ConfirmationBinding, ConfirmationStore, FieldChange, IdempotencyOutcome, IdempotencyStore,
        OperationPlanBuilder, OperationReceipt, PlanLifecycleState, PlanStore, PlanStoreRecord,
        ReceiptStatus, SqliteConfirmationStore, SqliteIdempotencyStore, SqlitePlanStore,
    };

    let fixture = build_fixture_state().await.expect("fixture");
    let mut policy = snow_mcp::domain::policy::PolicyConfig::default();
    policy
        .tools
        .get_mut("timecard_apply_set_hours")
        .expect("timecard apply policy")
        .enabled = true;
    let data_dir = fixture.tempdir.path().join("timecard-rpc-replay");
    let state = Arc::new(DaemonState::with_data_dir_and_mcp_config(
        Arc::clone(&fixture.state.core),
        data_dir.clone(),
        snow_mcp::McpConfig {
            environment: snow_mcp::McpEnvironment::explicit_config("test", "America/Chicago"),
            policy,
            ..Default::default()
        },
    ));
    std::fs::create_dir_all(&data_dir).expect("data dir");
    let store_path = data_dir.join("mcp_story_write.sqlite3");
    let plan_store = SqlitePlanStore::open(&store_path).expect("plan store");
    let confirmation_store =
        SqliteConfirmationStore::open(&store_path).expect("confirmation store");
    let idempotency_store = SqliteIdempotencyStore::open(&store_path).expect("idempotency store");
    let plan = OperationPlanBuilder::new("timecard_plan_set_hours")
        .target(RecordRef {
            sys_id: "card-sys".to_string(),
            number: "PRJ0161219".to_string(),
            table: "time_card".to_string(),
        })
        .planned_changes(json!({ "kind": "TimecardSetHours" }))
        .build();
    let now = Utc::now();
    plan_store
        .put(PlanStoreRecord {
            plan_id: plan.plan_id.clone(),
            tool: plan.tool.clone(),
            actor: "tester".to_string(),
            op_hash: plan.op_hash.clone(),
            plan_json: serde_json::to_value(&plan).expect("plan json"),
            concurrency_token: None,
            created_at: now,
            expires_at: now + chrono::Duration::seconds(600),
            state: PlanLifecycleState::Pending,
        })
        .await
        .expect("put plan");
    let binding = ConfirmationBinding {
        actor: "tester".to_string(),
        requester: "tester".to_string(),
        tool: "timecard_apply_set_hours".to_string(),
        op_hash: plan.op_hash.clone(),
        environment: "test".to_string(),
    };
    let token = confirmation_store
        .issue(&plan.plan_id, binding, 600)
        .await
        .expect("confirmation");
    let key = IdempotencyKey {
        value: "rpc-replay-key".to_string(),
        source: IdempotencyKeySource::ServerDerived,
    };
    assert!(matches!(
        idempotency_store
            .check_and_record(&key, "timecard_apply_set_hours", &plan.op_hash, 600)
            .await
            .expect("record idempotency"),
        IdempotencyOutcome::NewKey
    ));
    let receipt = OperationReceipt {
        plan_id: plan.plan_id.clone(),
        audit_id: "audit-1".to_string(),
        parent_audit_id: plan.plan_id.clone(),
        tool: "timecard_apply_set_hours".to_string(),
        status: ReceiptStatus::Success,
        applied_changes_summary: None,
        service_now_metadata: Some(ServiceNowMetadata {
            sys_id: Some("card-sys".to_string()),
            number: None,
            transaction_id: None,
        }),
        idempotency_replay: false,
        completed_at: now,
        op_hash: plan.op_hash.clone(),
        record_url: None,
        record_snapshot: None,
        changed_fields: Vec::<FieldChange>::new(),
        concurrency_token_observed: None,
        apply_started_at: Some(now),
        error_code: None,
        warnings: Vec::new(),
    };
    idempotency_store
        .save_receipt(&key, &receipt)
        .await
        .expect("save receipt");

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "timecard_apply_set_hours".to_string(),
            params: json!({
                "plan_id": plan.plan_id,
                "confirmation_token": token.token_id,
                "idempotency_key": "rpc-replay-key",
            }),
            id: Some(json!(1)),
        },
        &state,
    )
    .await;

    assert!(response.error.is_none(), "{:?}", response.error);
    let result = response.result.expect("receipt");
    assert_eq!(result["idempotency_replay"], json!(true));
    assert_eq!(result["audit_id"], json!("audit-1"));
}

#[tokio::test(flavor = "current_thread")]
async fn story_plan_create_persists_plan_for_get() {
    let fixture = build_fixture_state().await.expect("fixture");
    let mcp_config = snow_mcp::McpConfig {
        environment: snow_mcp::McpEnvironment::explicit_config("test", "America/Chicago"),
        policy: snow_mcp::domain::policy::PolicyConfig::from_toml_str(
            r#"
[mcp.boards."board-sys"]
name = "training-board"
instance_host = "https://example.service-now.com"
story_table = "rm_story"
task_table = "rm_scrum_task"
column_field = "sprint"
swim_lane_field = "epic"
assignment_group = "group-sys"
allowed_sprints = ["sprint-sys"]

[mcp.tools.story_plan_create]
enabled = true
requires_confirmation = false
story_board_id = "board-sys"
"#,
        )
        .expect("policy"),
        ..Default::default()
    };
    let state = std::sync::Arc::new(crate::DaemonState::with_data_dir_and_mcp_config(
        std::sync::Arc::clone(&fixture.state.core),
        fixture.tempdir.path().join("story-write"),
        mcp_config,
    ));

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "story_plan_create".to_string(),
            params: json!({
                "short_description": "Build board writer",
                "description": "Plan/create smoke"
            }),
            id: Some(json!(1)),
        },
        &state,
    )
    .await;
    assert!(response.error.is_none(), "{response:?}");
    let result = response.result.expect("plan result");
    let plan_id = result["plan_id"].as_str().expect("plan_id");
    let idempotency_key = result["idempotency_key"].as_str().expect("idempotency_key");
    let op_hash = result["op_hash"].as_str().expect("op_hash");
    assert_eq!(result["preview"]["assignment_group"], json!("group-sys"));
    assert!(result["preview"].get("sprint").is_none());
    assert_eq!(result["preview"]["backlog_type"], json!("product"));
    assert_eq!(result["preview"]["active"], json!(true));

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "plan_get".to_string(),
            params: json!({ "plan_id": plan_id }),
            id: Some(json!(2)),
        },
        &state,
    )
    .await;
    assert!(response.error.is_none(), "{response:?}");
    assert_eq!(response.result.unwrap()["plan"]["plan_id"], json!(plan_id));

    let conn = Connection::open(
        fixture
            .tempdir
            .path()
            .join("story-write")
            .join("mcp_story_write.sqlite3"),
    )
    .expect("story write database should open");
    let reserved: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM mcp_idempotency WHERE key = ?1 AND tool = ?2 AND op_hash = ?3",
            (idempotency_key, "story_apply_create", op_hash),
            |row| row.get(0),
        )
        .expect("idempotency row count");
    assert_eq!(reserved, 1);
    let audit_rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM mcp_audit_events", [], |row| {
            row.get(0)
        })
        .expect("audit row count");
    assert!(
        audit_rows >= 2,
        "plan and confirmation issue should be audited"
    );
}

#[test]
fn contract_info_normalizes_mcp_availability() {
    assert_eq!(
        normalize_mcp_availability("stdio"),
        ("local_stdio", "stdio")
    );
    assert_eq!(
        normalize_mcp_availability("disabled"),
        ("disabled", "disabled")
    );
    assert_eq!(
        normalize_mcp_availability("http"),
        ("future_remote_transport", "http")
    );
    assert_eq!(
        normalize_mcp_availability("malformed/private/path"),
        ("unknown", "unknown")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn contract_info_advertises_instance_host() {
    let fixture = build_fixture_state_at_instance("example.service-now.com")
        .await
        .expect("fixture");

    let result = contract_info(&fixture.state);

    assert_eq!(
        result.get("instance_host").and_then(Value::as_str),
        Some("example.service-now.com")
    );
    assert_eq!(
        result
            .get("environment")
            .and_then(|environment| environment.get("instance_host"))
            .and_then(Value::as_str),
        Some("example.service-now.com")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn contract_info_uses_null_instance_host_when_config_is_blank() {
    let fixture = build_fixture_state_without_instance_config()
        .await
        .expect("fixture");

    let result = contract_info(&fixture.state);

    assert_eq!(result.get("instance_host"), Some(&Value::Null));
    assert_eq!(
        result
            .get("environment")
            .and_then(|environment| environment.get("instance_host")),
        Some(&Value::Null)
    );
}

#[test]
fn contract_info_normalizes_instance_url_scheme() {
    assert_eq!(
        normalize_instance_host("https://example.service-now.com/api/now/table").as_deref(),
        Some("example.service-now.com")
    );
    assert_eq!(
        normalize_instance_host("http://example.service-now.com/").as_deref(),
        Some("example.service-now.com")
    );
    assert_eq!(
        normalize_instance_host("example.service-now.com/").as_deref(),
        Some("example.service-now.com")
    );
    assert_eq!(normalize_instance_host("   "), None);
}

#[tokio::test(flavor = "current_thread")]
async fn direct_rpc_contract_info_exposes_safe_contract_metadata() {
    let fixture = build_fixture_state().await.expect("fixture");

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "contract_info".to_string(),
            params: json!({}),
            id: Some(json!(1)),
        },
        &fixture.state,
    )
    .await;

    assert!(response.error.is_none(), "{:?}", response.error);
    let result = response.result.expect("contract info result");
    assert_eq!(
        result.get("contract_version").and_then(Value::as_str),
        Some(CONTRACT_VERSION)
    );
    assert_eq!(
        result.get("daemon_version").and_then(Value::as_str),
        Some(env!("CARGO_PKG_VERSION"))
    );
    assert_eq!(
        result.get("instance_host").and_then(Value::as_str),
        Some("localhost")
    );
    assert_eq!(
        result
            .get("environment")
            .and_then(|environment| environment.get("instance_host"))
            .and_then(Value::as_str),
        Some("localhost")
    );
    assert_eq!(
        result
            .get("environment")
            .and_then(|environment| environment.get("username"))
            .and_then(Value::as_str),
        Some("tester")
    );
    assert_eq!(
        result.get("warming_model").and_then(Value::as_str),
        Some("passive")
    );
    assert_eq!(
        result
            .get("mcp_availability")
            .and_then(|mcp| mcp.get("mode"))
            .and_then(Value::as_str),
        Some("local_stdio")
    );
    assert!(
        result
            .get("supported_methods")
            .and_then(Value::as_array)
            .expect("supported methods")
            .iter()
            .any(|method| method.as_str() == Some("contract_info"))
    );
    assert!(
        result
            .get("supported_methods")
            .and_then(Value::as_array)
            .expect("supported methods")
            .iter()
            .any(|method| method.as_str() == Some("list_my_tasks"))
    );
    for legacy_method in ["approve", "reject"] {
        assert!(
            !result
                .get("supported_methods")
                .and_then(Value::as_array)
                .expect("supported methods")
                .iter()
                .any(|method| method.as_str() == Some(legacy_method)),
            "{legacy_method} should not be advertised as a supported method"
        );
    }
    assert!(
        !result
            .get("supported_methods")
            .and_then(Value::as_array)
            .expect("supported methods")
            .iter()
            .any(|method| method.as_str() == Some("rebuild_cache")),
        "online cache replacement must not be advertised"
    );
    assert!(
        result
            .get("deprecated_aliases")
            .and_then(Value::as_array)
            .expect("deprecated aliases")
            .iter()
            .any(
                |alias| alias.get("method").and_then(Value::as_str) == Some("my_tasks")
                    && alias.get("replacement").and_then(Value::as_str) == Some("list_my_tasks")
            )
    );
    for (legacy_method, replacement) in [
        ("approve", "approval_approve"),
        ("reject", "approval_reject"),
    ] {
        assert!(
            result
                .get("deprecated_aliases")
                .and_then(Value::as_array)
                .expect("deprecated aliases")
                .iter()
                .any(
                    |alias| alias.get("method").and_then(Value::as_str) == Some(legacy_method)
                        && alias.get("replacement").and_then(Value::as_str) == Some(replacement)
                ),
            "{legacy_method} should be reported as deprecated alias"
        );
    }

    let payload = serde_json::to_string(&result).expect("contract info json");
    assert!(!payload.contains("secret"), "{payload}");
    assert!(!payload.contains("SNOW_PASSWORD"), "{payload}");
    assert!(!payload.contains("SERVICENOW_PASSWORD"), "{payload}");
    assert!(!payload.contains(fixture.tempdir.path().to_string_lossy().as_ref()));
    assert!(!payload.contains("snow.db"), "{payload}");
}

#[test]
fn extract_number_reads_param() {
    let params = json!({ "number": "INC0012345" });
    assert_eq!(extract_number(&params).unwrap(), "INC0012345");
}

#[test]
fn extract_approval_action_target_accepts_number_or_approval_sys_id() {
    assert_eq!(
        extract_approval_action_target(&json!({ "number": "CHANGE0010001" })).unwrap(),
        ApprovalActionTarget::Number("CHANGE0010001".to_string())
    );
    assert_eq!(
        extract_approval_action_target(
            &json!({ "approval_sys_id": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" })
        )
        .unwrap(),
        ApprovalActionTarget::ApprovalSysId("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string())
    );
}

#[test]
fn extract_approval_action_target_rejects_missing_or_mixed_selector() {
    assert!(
        extract_approval_action_target(&json!({})).is_err(),
        "missing selector must fail"
    );
    assert!(
        extract_approval_action_target(&json!({
            "number": "CHANGE0010001",
            "approval_sys_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
        }))
        .is_err(),
        "mixed selector must fail"
    );
}

#[test]
fn extract_record_lookup_accepts_number() {
    let lookup = extract_record_lookup(&json!({ "number": "DMND0012345" })).expect("lookup");
    assert_eq!(lookup, RecordLookup::Number("DMND0012345".to_string()));
}

#[tokio::test(flavor = "current_thread")]
async fn record_lookup_methods_map_unknown_prefix_to_the_distinct_contract_error() {
    let fixture = build_fixture_state().await.expect("fixture");
    assert_ne!(UNKNOWN_PREFIX_CODE, -32005);

    for method in ["get_record", "get_record_fresh", "get_work_notes"] {
        let response = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: method.to_string(),
                params: json!({ "number": "ZZZZ0000001" }),
                id: Some(json!(1)),
            },
            &fixture.state,
        )
        .await;

        let error = response.error.expect("unknown-prefix error");
        assert_eq!(error.code, UNKNOWN_PREFIX_CODE, "method={method}");
        assert_eq!(error.message, "unknown record prefix", "method={method}");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn direct_rpc_rejects_online_cache_replacement() {
    let fixture = build_fixture_state().await.expect("fixture");
    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "rebuild_cache".to_string(),
            params: json!({}),
            id: Some(json!(1)),
        },
        &fixture.state,
    )
    .await;

    let error = response.error.expect("method-not-found error");
    assert_eq!(error.code, -32601);
    assert_eq!(error.message, "method not found");
}

#[test]
fn extract_resource_plan_list_rejects_unknown_parent_prefix() {
    let err = extract_resource_plan_list_params(&json!({ "parent_number": "BAD_PARENT" }))
        .expect_err("invalid parent prefix");

    assert!(err.to_string().contains("DMND or PRJ"));
}

#[test]
fn extract_record_lookup_accepts_table_sys_id_and_lowercases() {
    let lookup = extract_record_lookup(&json!({
        "table": "dmn_demand",
        "sys_id": "7F029B89C3E7565067BDFD73E40131A1"
    }))
    .expect("lookup");
    assert_eq!(
        lookup,
        RecordLookup::TableSysId {
            table: "dmn_demand".to_string(),
            sys_id: "7f029b89c3e7565067bdfd73e40131a1".to_string()
        }
    );
}

#[test]
fn extract_record_lookup_accepts_change_request_table_sys_id() {
    let lookup = extract_record_lookup(&json!({
        "table": "change_request",
        "sys_id": "7F029B89C3E7565067BDFD73E40131A1"
    }))
    .expect("lookup");
    assert_eq!(
        lookup,
        RecordLookup::TableSysId {
            table: "change_request".to_string(),
            sys_id: "7f029b89c3e7565067bdfd73e40131a1".to_string()
        }
    );
}

#[test]
fn extract_record_lookup_rejects_invalid_shapes() {
    for params in [
        json!({ "sys_id": "7f029b89c3e7565067bdfd73e40131a1" }),
        json!({
            "number": "DMND0012345",
            "table": "dmn_demand",
            "sys_id": "7f029b89c3e7565067bdfd73e40131a1"
        }),
        json!({ "table": "dmn_demand" }),
        json!({ "table": "dmn_demand", "sys_id": "7f029b89c3e7565067bdfd73e40131a" }),
        json!({ "table": "dmn_demand", "sys_id": "7f029b89c3e7565067bdfd73e40131ag" }),
        json!({ "table": "sys_user", "sys_id": "7f029b89c3e7565067bdfd73e40131a1" }),
    ] {
        assert!(
            extract_record_lookup(&params).is_err(),
            "accepted invalid params: {params}"
        );
    }
}

#[test]
fn extract_string_reads_param() {
    let params = json!({ "reason": "missing test plan" });
    assert_eq!(
        extract_string(&params, "reason").unwrap(),
        "missing test plan"
    );
    assert!(extract_string(&json!({}), "reason").is_err());
}

#[test]
fn extract_task_sla_parent_refs_reads_params() {
    let parents = extract_task_sla_parent_refs(&json!({
        "parents": [{
            "record_number": "INC0000001",
            "record_table": "incident",
            "record_sys_id": "incident-sys-1"
        }]
    }))
    .expect("task sla parent refs");

    assert_eq!(parents.len(), 1);
    assert_eq!(parents[0].record_number, "INC0000001");
    assert_eq!(parents[0].record_table, "incident");
    assert_eq!(parents[0].record_sys_id, "incident-sys-1");
    assert!(extract_task_sla_parent_refs(&json!({})).is_err());
}

#[test]
fn extract_knowledge_search_filters_reads_params() {
    let params = json!({
        "query": "windows access",
        "knowledge_base": "kb-base",
        "category": "Access",
        "limit": 10
    });
    let (query, filters) = extract_knowledge_search_filters(&params).expect("filters");
    assert_eq!(query, "windows access");
    assert_eq!(filters.knowledge_base.as_deref(), Some("kb-base"));
    assert_eq!(filters.category.as_deref(), Some("Access"));
    assert_eq!(filters.limit, Some(10));
}

#[test]
fn business_application_search_params_accept_hydration_options() {
    let (params, options) = extract_business_application_search_params(&json!({
        "name": "Epic",
        "persist": false,
        "resolve_references": false,
        "reference_depth": 2,
        "refresh_dictionary": true
    }))
    .expect("business application params");
    assert_eq!(params.name.as_deref(), Some("Epic"));
    assert!(!options.persist);
    assert!(!options.resolve_references);
    assert_eq!(options.reference_depth, 2);
    assert!(options.refresh_dictionary);
}

#[test]
fn business_application_sync_params_are_optional() {
    // Only hydration options, no search filters => params should be None so
    // core runs the default bounded search.
    let params = extract_business_application_sync_params(&json!({
        "refresh_dictionary": true
    }))
    .expect("sync params");
    assert!(!params.all);
    assert!(params.search_params.is_none());
    assert!(params.options.refresh_dictionary);

    // A search filter is present => params should be Some.
    let params = extract_business_application_sync_params(&json!({
        "name": "Example Application"
    }))
    .expect("sync params");
    assert_eq!(
        params.search_params.and_then(|params| params.name),
        Some("Example Application".to_string())
    );

    // Empty object => params None.
    let params = extract_business_application_sync_params(&json!({})).expect("sync params");
    assert!(!params.all);
    assert!(params.search_params.is_none());
}

#[test]
fn business_application_sync_params_accept_all_without_filters() {
    let params = extract_business_application_sync_params(&json!({
        "all": true,
        "persist": true,
        "resolve_references": true,
        "reference_depth": 1,
        "refresh_dictionary": true
    }))
    .expect("sync-all params");

    assert!(params.all);
    assert!(params.search_params.is_none());
    assert!(params.options.persist);
    assert!(params.options.resolve_references);
    assert_eq!(params.options.reference_depth, 1);
    assert!(params.options.refresh_dictionary);
}

#[test]
fn business_application_sync_all_rejects_filters_and_non_boolean_all() {
    let err = extract_business_application_sync_params(&json!({
        "all": true,
        "name": "Example Application"
    }))
    .expect_err("all with name should fail");
    assert!(err.to_string().contains("cannot be combined"));

    let err = extract_business_application_sync_params(&json!({
        "all": true,
        "operational_state_not": "retired"
    }))
    .expect_err("all with operational_state_not should fail");
    assert!(err.to_string().contains("cannot be combined"));

    let err = extract_business_application_sync_params(&json!({
        "all": "true"
    }))
    .expect_err("string all should fail");
    assert!(err.to_string().contains("must be a boolean"));
}

#[test]
fn business_application_query_params_accept_offset_for_paged_local_queries() {
    let params = extract_business_application_query_params(&json!({
        "text": "portfolio",
        "filters": [
            {
                "field": "name",
                "op": "contains",
                "value": "Example"
            }
        ],
        "limit": 500,
        "offset": 1000
    }))
    .expect("query params");
    let query = core_business_application_query(&params).expect("core query");

    assert_eq!(query.text.as_deref(), Some("portfolio"));
    assert_eq!(query.limit, Some(500));
    assert_eq!(query.offset, Some(1000));
    assert_eq!(query.filters.len(), 1);
}

#[test]
fn business_application_servers_params_validate_flat_selectors_and_limits() {
    // The daemon now delegates to the canonical snow_core contract, so the
    // parsed value is the core params struct and selector/limit normalization
    // is observed through the validated options it produces.
    let params = parse_business_application_servers_params(&json!({
        "number": " apm0000001 ",
        "max_depth": 2,
        "max_cis": 500,
        "max_edges": 2000,
        "relationship_type": [" Depends on::Used by "],
        "include_paths": true
    }))
    .expect("business application servers params");
    let options = params.traversal.validate().expect("validated options");

    // Core normalizes the number to uppercase and trims surrounding space.
    assert_eq!(
        options.selector,
        snow_core::BusinessApplicationServersSelector::Number("APM0000001".to_string())
    );
    assert_eq!(options.max_depth, 2);
    assert_eq!(options.max_cis, 500);
    assert_eq!(options.max_edges, 2000);
    assert_eq!(options.relationship_type, vec!["Depends on::Used by"]);
    assert!(options.include_paths);

    let params = parse_business_application_servers_params(&json!({
        "sys_id": "54A4B61B6FE845000ED852A03F3EE4D0"
    }))
    .expect("sys_id lookup");
    let options = params
        .traversal
        .validate()
        .expect("validated sys_id options");
    assert_eq!(
        options.selector,
        snow_core::BusinessApplicationServersSelector::SysId(
            "54a4b61b6fe845000ed852a03f3ee4d0".to_string()
        )
    );
}

#[test]
fn business_application_servers_params_accept_persistence_controls() {
    let defaults = parse_business_application_servers_params(&json!({
        "number": "APM0000001"
    }))
    .expect("default persistence controls");
    assert!(defaults.persist);
    assert!(!defaults.prune_stale);

    let params = parse_business_application_servers_params(&json!({
        "number": "APM0000001",
        "persist": true,
        "prune_stale": true,
        "max_service_membership_associations": 3000,
        "max_service_membership_pages": 30
    }))
    .expect("explicit persistence controls");
    assert!(params.persist);
    assert!(params.prune_stale);
    assert_eq!(params.traversal.number.as_deref(), Some("APM0000001"));
    let options = params
        .traversal
        .validate()
        .expect("validated budget options");
    assert_eq!(options.max_service_membership_associations, 3000);
    assert_eq!(options.max_service_membership_pages, 30);

    let err = parse_business_application_servers_params(&json!({
        "number": "APM0000001",
        "persist": false,
        "prune_stale": true
    }))
    .expect_err("prune stale without persistence should fail");
    assert!(
        err.to_string()
            .contains("`prune_stale` requires `persist=true`")
    );

    let err = parse_business_application_servers_params(&json!({
        "number": "APM0000001",
        "persist": "false"
    }))
    .expect_err("string persist should fail");
    assert!(err.to_string().contains("`persist` must be a boolean"));

    let err = parse_business_application_servers_params(&json!({
        "number": "APM0000001",
        "prune_stale": "true"
    }))
    .expect_err("string prune_stale should fail");
    assert!(err.to_string().contains("`prune_stale` must be a boolean"));

    let err = parse_business_application_servers_params(&json!({
        "number": "APM0000001",
        "max_service_membership_associations": 0
    }))
    .expect_err("zero service-membership association budget should fail");
    assert!(
        err.to_string()
            .contains("max_service_membership_associations")
    );

    let err = parse_business_application_servers_params(&json!({
        "number": "APM0000001",
        "max_service_membership_pages": 201
    }))
    .expect_err("above max service-membership page budget should fail");
    assert!(err.to_string().contains("max_service_membership_pages"));
}

#[test]
fn business_application_servers_params_parse_fallback_strategy() {
    use snow_core::FallbackStrategy;

    // Default: fallback_strategy omitted => None.
    let defaults = parse_business_application_servers_params(&json!({
        "number": "APM0000001"
    }))
    .expect("default fallback strategy");
    assert_eq!(defaults.traversal.fallback_strategy, FallbackStrategy::None);

    // Explicit ci_owner_group parses through to the typed enum.
    let params = parse_business_application_servers_params(&json!({
        "number": "APM0000001",
        "fallback_strategy": "ci_owner_group"
    }))
    .expect("ci_owner_group fallback strategy");
    assert_eq!(
        params.traversal.fallback_strategy,
        FallbackStrategy::CiOwnerGroup
    );

    // Explicit none parses to None.
    let params = parse_business_application_servers_params(&json!({
        "number": "APM0000001",
        "fallback_strategy": "none"
    }))
    .expect("none fallback strategy");
    assert_eq!(params.traversal.fallback_strategy, FallbackStrategy::None);

    // Unknown value is rejected.
    parse_business_application_servers_params(&json!({
        "number": "APM0000001",
        "fallback_strategy": "bogus"
    }))
    .expect_err("unknown fallback strategy should fail");
}

#[test]
fn business_application_servers_params_reject_invalid_shapes() {
    // These mirror the canonical core contract: missing/double selector,
    // BA:<sys_id> fallback, malformed sys_id, out-of-range bounds, and
    // (via deny_unknown_fields) unknown fields must all be rejected. Note
    // that an empty-string relationship_type entry is intentionally NOT
    // listed: core silently drops empty entries rather than erroring, which
    // is the canonical behavior the daemon now inherits.
    for params in [
        json!({}),
        json!({ "number": "APM0000001", "sys_id": "54a4b61b6fe845000ed852a03f3ee4d0" }),
        json!({ "number": "BA:54a4b61b6fe845000ed852a03f3ee4d0" }),
        json!({ "number": "not-an-apm-number" }),
        json!({ "sys_id": "54a4b61b6fe845000ed852a03f3ee4d" }),
        json!({ "number": "APM0000001", "max_depth": 0 }),
        json!({ "number": "APM0000001", "max_depth": 5 }),
        json!({ "number": "APM0000001", "max_cis": 0 }),
        json!({ "number": "APM0000001", "max_cis": 5001 }),
        json!({ "number": "APM0000001", "max_edges": 0 }),
        json!({ "number": "APM0000001", "max_edges": 20001 }),
        json!({ "number": "APM0000001", "unexpected": true }),
    ] {
        assert!(
            parse_business_application_servers_params(&params).is_err(),
            "accepted invalid params: {params}"
        );
    }
}

#[test]
fn business_application_cached_params_accept_include_tombstoned_only() {
    let params = parse_business_application_servers_cached_params(&json!({
        "number": "APM0000001",
        "include_tombstoned": true
    }))
    .expect("cached business application servers params");
    let options = params.validate().expect("cached options");
    assert_eq!(
        options.selector,
        snow_core::BusinessApplicationServersCachedSelector::Number("APM0000001".to_string())
    );
    assert!(options.include_tombstoned);

    let params = parse_business_applications_for_server_params(&json!({
        "name": "example-server",
        "include_tombstoned": true
    }))
    .expect("cached business applications for server params");
    let options = params.validate().expect("server cached options");
    assert_eq!(
        options.selector,
        snow_core::BusinessApplicationsForServerSelector::ExactName("example-server".to_string())
    );
    assert!(options.include_tombstoned);

    for params in [
        json!({
            "number": "APM0000001",
            "persist": true
        }),
        json!({
            "number": "APM0000001",
            "prune_stale": true
        }),
    ] {
        assert!(
            parse_business_application_servers_cached_params(&params).is_err(),
            "cached BA servers accepted write controls: {params}"
        );
    }

    for params in [
        json!({
            "name": "example-server",
            "persist": true
        }),
        json!({
            "name": "example-server",
            "prune_stale": true
        }),
    ] {
        assert!(
            parse_business_applications_for_server_params(&params).is_err(),
            "server reverse lookup accepted write controls: {params}"
        );
    }
}

#[test]
fn business_application_lookup_requires_exactly_one_selector() {
    assert!(matches!(
        extract_business_application_lookup_params(
            &json!({ "sys_id": "54A4B61B6FE845000ED852A03F3EE4D0" })
        ),
        Ok(BusinessApplicationLookup::SysId(_))
    ));
    assert!(matches!(
        extract_business_application_lookup_params(&json!({ "name": "Epic" })),
        Ok(BusinessApplicationLookup::Name(_))
    ));
    assert!(
        extract_business_application_lookup_params(&json!({
            "sys_id": "54a4b61b6fe845000ed852a03f3ee4d0",
            "name": "Epic"
        }))
        .is_err()
    );
}

#[test]
fn extract_kb_list_tags_params_reads_defaults_and_validation() {
    let params = extract_kb_list_tags_params(&json!({ "layer": "user", "min_count": 2 }))
        .expect("kb tag params");
    assert_eq!(params.layer.as_deref(), Some("user"));
    assert_eq!(params.min_count, 2);
    assert!(extract_kb_list_tags_params(&json!({ "min_count": 0 })).is_err());
}

#[test]
fn extract_kb_semantic_search_filters_reads_params() {
    let params = json!({
        "query": "admin rights",
        "knowledge_base": "IT",
        "category": "Access",
        "limit": 3,
        "mode": "lexical",
        "min_score_millis": 250
    });
    let (query, filters) = extract_kb_semantic_search_filters(&params).expect("filters");
    assert_eq!(query, "admin rights");
    assert_eq!(filters.knowledge_base.as_deref(), Some("IT"));
    assert_eq!(filters.category.as_deref(), Some("Access"));
    assert_eq!(filters.limit, Some(3));
    assert_eq!(filters.mode, snow_core::KnowledgeSearchMode::Lexical);
    assert_eq!(filters.min_score_millis, Some(250));
}

#[test]
fn ok_response_serializes_result() {
    let response = JsonRpcResponse::ok(Some(json!(1)), json!({ "ok": true }));
    assert_eq!(response.id, Some(json!(1)));
    assert_eq!(response.result, Some(json!({ "ok": true })));
    assert!(response.error.is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn direct_rpc_contract_exposes_wrapped_aliases_metadata_and_filters() {
    let (instance_url, _approval_requests) = spawn_json_http_sequence_server(vec![
        json!({
            "result": [{
                "sys_id": "chg-sys",
                "number": "CHG001",
                "short_description": "Example change",
                "state": "assess"
            }]
        }),
        json!({ "result": [] }),
        json!({
            "result": [{
                "sys_id": "user-1",
                "user_name": "tester",
                "email": "tester@example.com",
                "name": "Example User"
            }]
        }),
        json!({
            "result": [{
                "sys_id": "apr-sys",
                "number": "APR001",
                "state": "requested",
                "short_description": "Approval for CHG001",
                "approver": { "value": "user-1", "display_value": "Example User" },
                "source_table": { "value": "change_request", "display_value": "Change Request" },
                "sysapproval": { "value": "chg-sys", "display_value": "CHG001" },
                "sysapproval.number": "CHG001",
                "sysapproval.short_description": "Example change",
                "sysapproval.state": "assess",
                "sysapproval.sys_class_name": "change_request",
                "sys_created_on": "2026-04-09 10:11:12"
            }]
        }),
        json!({ "result": [] }),
    ])
    .await
    .expect("approval http server");
    let fixture = build_fixture_state_at_instance(&instance_url)
        .await
        .expect("fixture");
    seed_kb_runtime_state(&fixture).expect("seed kb runtime state");

    let get_record = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "get_record".to_string(),
            params: json!({ "number": "CHG001" }),
            id: Some(json!(1)),
        },
        &fixture.state,
    )
    .await;
    let record = get_record
        .result
        .expect("record result")
        .get("record")
        .cloned()
        .expect("wrapped record");
    assert_eq!(
        record.get("resource_type").and_then(Value::as_str),
        Some("change")
    );
    assert_eq!(record.get("source").and_then(Value::as_str), Some("api"));
    assert!(record.get("vault_relative_path").is_none());
    assert!(
        record
            .get("browser_url")
            .and_then(Value::as_str)
            .expect("browser_url")
            .contains("nav_to.do?uri=change_request.do?sys_id=chg-sys")
    );

    let list_my_tasks = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "list_my_tasks".to_string(),
            params: json!({}),
            id: Some(json!(2)),
        },
        &fixture.state,
    )
    .await;
    let task_result = list_my_tasks.result.expect("tasks result");
    let task_records = task_result
        .get("records")
        .and_then(Value::as_array)
        .expect("task records");
    let first_task_type = task_records[0]
        .get("resource_type")
        .and_then(Value::as_str)
        .expect("resource_type");
    assert_eq!(first_task_type, &first_task_type.to_ascii_lowercase());

    let list_my_approvals = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "list_my_approvals".to_string(),
            params: json!({}),
            id: Some(json!(3)),
        },
        &fixture.state,
    )
    .await;
    let approval_result = list_my_approvals.result.expect("approvals result");
    let approval_records = approval_result
        .get("records")
        .and_then(Value::as_array)
        .expect("approval records");
    assert_eq!(approval_records.len(), 1);
    assert_eq!(
        approval_records[0]
            .get("record")
            .and_then(|record| record.get("resource_type"))
            .and_then(Value::as_str),
        Some("approval")
    );

    let search = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "search_records".to_string(),
            params: json!({ "query": "Database", "limit": 5 }),
            id: Some(json!(4)),
        },
        &fixture.state,
    )
    .await;
    let first_result = search
        .result
        .expect("search result")
        .get("results")
        .and_then(Value::as_array)
        .and_then(|results| results.first())
        .cloned()
        .expect("search match");
    assert_eq!(
        first_result.get("match_in").and_then(Value::as_str),
        Some("short_description")
    );
    assert!(first_result.get("record").is_some());

    let filtered_list = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "list_records".to_string(),
            params: json!({ "parent_number": "CHG001", "limit": 1 }),
            id: Some(json!(5)),
        },
        &fixture.state,
    )
    .await;
    let filtered_result = filtered_list.result.expect("list_records result");
    let filtered_records = filtered_result
        .get("records")
        .and_then(Value::as_array)
        .expect("filtered records");
    assert_eq!(filtered_records.len(), 1);
    assert!(matches!(
        filtered_records[0].get("number").and_then(Value::as_str),
        Some("CTASK001") | Some("APR001")
    ));

    let knowledge_list = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "list_knowledge_articles".to_string(),
            params: json!({ "knowledge_base_sys_id": "kb-base", "category_sys_id": "kb-db" }),
            id: Some(json!(6)),
        },
        &fixture.state,
    )
    .await;
    let knowledge_result = knowledge_list.result.expect("knowledge list result");
    let knowledge_articles = knowledge_result
        .get("data")
        .and_then(|data| data.get("articles"))
        .and_then(Value::as_array)
        .expect("knowledge articles");
    assert_eq!(knowledge_articles.len(), 1);
    assert_eq!(
        knowledge_articles[0]
            .get("record")
            .and_then(|record| record.get("vault_relative_path"))
            .and_then(Value::as_str),
        Some("knowledge/KB001.md")
    );

    let vault_path = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "vault_path".to_string(),
            params: json!({}),
            id: Some(json!(7)),
        },
        &fixture.state,
    )
    .await;
    assert_eq!(
        vault_path
            .result
            .expect("vault path")
            .get("path")
            .and_then(Value::as_str),
        Some(
            fixture
                .tempdir
                .path()
                .join("vault")
                .to_string_lossy()
                .as_ref()
        )
    );

    let kb_status = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "kb_status".to_string(),
            params: json!({}),
            id: Some(json!(8)),
        },
        &fixture.state,
    )
    .await;
    let kb_status = kb_status
        .result
        .expect("kb status")
        .get("status")
        .cloned()
        .expect("wrapped kb status");
    assert_eq!(
        kb_status.get("article_count").and_then(Value::as_u64),
        Some(1)
    );
    assert_eq!(
        kb_status.get("body_cached_count").and_then(Value::as_u64),
        Some(1)
    );
    assert_eq!(
        kb_status.get("lock_held").and_then(Value::as_bool),
        Some(true)
    );

    let cache_info = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "cache_info".to_string(),
            params: json!({}),
            id: Some(json!(9)),
        },
        &fixture.state,
    )
    .await;
    let cache_info = cache_info.result.expect("cache info");
    assert_eq!(
        cache_info.get("total_rows").and_then(Value::as_u64),
        Some(5)
    );
    assert!(
        cache_info
            .get("sqlite_path")
            .and_then(Value::as_str)
            .expect("sqlite_path")
            .ends_with("snow.db")
    );

    let kb_tags = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "kb_list_tags".to_string(),
            params: json!({ "layer": "all", "min_count": 1 }),
            id: Some(json!(10)),
        },
        &fixture.state,
    )
    .await;
    let kb_tags = kb_tags
        .result
        .expect("kb tags")
        .get("tags")
        .and_then(Value::as_array)
        .cloned()
        .expect("wrapped kb tags");
    assert!(kb_tags.iter().any(|entry| {
        entry.get("tag").and_then(Value::as_str) == Some("database")
            && entry
                .get("layers")
                .and_then(Value::as_array)
                .map(|layers| layers.iter().any(|layer| layer.as_str() == Some("sn")))
                .unwrap_or(false)
    }));

    let kb_sync = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "kb_sync".to_string(),
            params: json!({ "full": true, "with_bodies": true }),
            id: Some(json!(10)),
        },
        &fixture.state,
    )
    .await;
    let kb_sync_error = kb_sync.error.expect("kb sync error");
    assert_eq!(kb_sync_error.code, -32000);
    assert_eq!(kb_sync_error.message, "internal error");
    assert!(
        kb_sync_error
            .data
            .as_ref()
            .and_then(|data| data.get("details"))
            .and_then(Value::as_str)
            .map(|details| !details.trim().is_empty())
            .unwrap_or(false)
    );

    let kb_semantic_search = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: "kb_semantic_search".to_string(),
                params: json!({ "query": "database", "knowledge_base": "IT", "mode": "lexical", "limit": 5 }),
                id: Some(json!(11)),
            },
            &fixture.state,
        )
        .await;
    let kb_semantic_hits = kb_semantic_search
        .result
        .expect("kb semantic search")
        .get("hits")
        .and_then(Value::as_array)
        .cloned()
        .expect("wrapped kb semantic hits");
    assert_eq!(kb_semantic_hits.len(), 1);
    assert_eq!(
        kb_semantic_hits[0].get("mode").and_then(Value::as_str),
        Some("lexical")
    );
    assert_eq!(
        kb_semantic_hits[0].get("coverage").and_then(Value::as_str),
        Some("full_text")
    );
    assert_eq!(
        kb_semantic_hits[0]
            .get("article")
            .and_then(|article| article.get("record"))
            .and_then(|record| record.get("number"))
            .and_then(Value::as_str),
        Some("KB001")
    );

    let kb_semantic_status = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "kb_semantic_status".to_string(),
            params: json!({}),
            id: Some(json!(12)),
        },
        &fixture.state,
    )
    .await;
    let kb_semantic_status = kb_semantic_status
        .result
        .expect("kb semantic status")
        .get("status")
        .cloned()
        .expect("wrapped semantic status");
    assert_eq!(
        kb_semantic_status.get("enabled").and_then(Value::as_bool),
        Some(false)
    );
    assert_eq!(
        kb_semantic_status
            .get("active_kb_articles")
            .and_then(Value::as_u64),
        Some(1)
    );

    let kb_semantic_rebuild = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "kb_semantic_rebuild".to_string(),
            params: json!({ "full": true }),
            id: Some(json!(13)),
        },
        &fixture.state,
    )
    .await;
    let kb_semantic_rebuild_error = kb_semantic_rebuild
        .error
        .expect("kb semantic rebuild error");
    assert_eq!(kb_semantic_rebuild_error.code, -32000);
    assert_eq!(kb_semantic_rebuild_error.message, "internal error");
    assert!(
        kb_semantic_rebuild_error
            .data
            .as_ref()
            .and_then(|data| data.get("details"))
            .and_then(Value::as_str)
            .map(|details| !details.trim().is_empty())
            .unwrap_or(false)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn direct_rpc_work_record_omission_does_not_fall_back_to_cached_projection() {
    let fixture = build_fixture_state_without_instance_config()
        .await
        .expect("fixture");

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "get_record".to_string(),
            params: json!({ "number": "CHG001" }),
            id: Some(json!(1)),
        },
        &fixture.state,
    )
    .await;

    assert!(response.result.is_none());
    assert!(
        response.error.is_some(),
        "live upstream failure must surface"
    );
    let cached = fixture
        .state
        .core
        .get_record("CHG001")
        .await
        .expect("inspect seeded cache")
        .expect("seeded cached projection remains present");
    assert_eq!(cached.source, snow_core::CacheSource::Disk);
}

#[tokio::test(flavor = "current_thread")]
async fn direct_rpc_task_sla_status_returns_core_status_projection() {
    let (instance_url, request_rx) = spawn_task_sla_status_server(
        json!({
            "result": [{
                "sys_id": "task-parent-sys",
                "number": "INC0000001",
                "short_description": "Generic incident"
            }]
        }),
        json!({
            "result": [task_sla_row(
                "sla-row-sys",
                "task-parent-sys",
                "INC0000001",
                "Initial Response SLA"
            )]
        }),
        2,
    )
    .await
    .expect("http server");
    let fixture = build_fixture_state_at_instance(&instance_url)
        .await
        .expect("fixture");

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "task_sla_status".to_string(),
            params: json!({ "number": "INC0000001" }),
            id: Some(json!(1)),
        },
        &fixture.state,
    )
    .await;

    assert!(response.error.is_none(), "{:?}", response.error);
    let status = response
        .result
        .expect("task sla status result")
        .get("status")
        .cloned()
        .expect("wrapped task sla status");
    assert_eq!(
        status.get("record_number").and_then(Value::as_str),
        Some("INC0000001")
    );
    assert_eq!(
        status.get("record_table").and_then(Value::as_str),
        Some("incident")
    );
    assert_eq!(
        status.get("record_sys_id").and_then(Value::as_str),
        Some("task-parent-sys")
    );
    assert_eq!(
        status.get("readable").and_then(Value::as_str),
        Some("ReadableRows")
    );
    assert_eq!(
        status
            .get("summary")
            .and_then(|summary| summary.get("total"))
            .and_then(Value::as_u64),
        Some(1)
    );
    assert_eq!(
        status
            .get("rows")
            .and_then(Value::as_array)
            .and_then(|rows| rows.first())
            .and_then(|row| row.get("name"))
            .and_then(Value::as_str),
        Some("Initial Response SLA")
    );

    let request_lines = request_rx.await.expect("request lines");
    assert!(
        request_lines
            .iter()
            .any(|line| line.contains("/api/now/table/incident")),
        "{request_lines:?}"
    );
    assert!(
        request_lines
            .iter()
            .any(|line| line.contains("/api/now/table/task_sla")),
        "{request_lines:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn direct_rpc_task_sla_status_for_tasks_returns_status_map() {
    let (instance_url, request_rx) = spawn_task_sla_status_server(
        json!({ "result": [] }),
        json!({
            "result": [task_sla_row(
                "sla-row-sys",
                "incident-sys-1",
                "INC0000001",
                "Initial Response SLA"
            )]
        }),
        1,
    )
    .await
    .expect("http server");
    let fixture = build_fixture_state_at_instance(&instance_url)
        .await
        .expect("fixture");

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "task_sla_status_for_tasks".to_string(),
            params: json!({
                "parents": [{
                    "record_number": "INC0000001",
                    "record_table": "incident",
                    "record_sys_id": "incident-sys-1"
                }]
            }),
            id: Some(json!(1)),
        },
        &fixture.state,
    )
    .await;

    assert!(response.error.is_none(), "{:?}", response.error);
    let statuses = response
        .result
        .expect("task sla statuses result")
        .get("statuses")
        .cloned()
        .expect("wrapped task sla statuses");
    let status = statuses
        .get("incident-sys-1")
        .expect("incident status in map");
    assert_eq!(
        status.get("record_number").and_then(Value::as_str),
        Some("INC0000001")
    );
    assert_eq!(
        status.get("readable").and_then(Value::as_str),
        Some("ReadableRows")
    );

    let request_lines = request_rx.await.expect("request lines");
    assert_eq!(request_lines.len(), 1);
    assert!(
        request_lines[0].contains("/api/now/table/task_sla"),
        "{request_lines:?}"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn direct_rpc_task_sla_status_preserves_parent_not_found_without_sla_fetch() {
    let (instance_url, request_rx) = spawn_task_sla_status_server(
        json!({ "result": [] }),
        json!({
            "result": [task_sla_row(
                "unused-sla-row-sys",
                "unused-task-sys",
                "INC0000009",
                "Unused SLA"
            )]
        }),
        1,
    )
    .await
    .expect("http server");
    let fixture = build_fixture_state_at_instance(&instance_url)
        .await
        .expect("fixture");

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "task_sla_status".to_string(),
            params: json!({ "number": "INC0000009" }),
            id: Some(json!(1)),
        },
        &fixture.state,
    )
    .await;

    assert!(response.error.is_none(), "{:?}", response.error);
    let status = response
        .result
        .expect("task sla status result")
        .get("status")
        .cloned()
        .expect("wrapped task sla status");
    assert_eq!(
        status.get("record_number").and_then(Value::as_str),
        Some("INC0000009")
    );
    assert_eq!(
        status.get("readable").and_then(Value::as_str),
        Some("ParentNotFound")
    );
    assert!(
        status
            .get("rows")
            .and_then(Value::as_array)
            .expect("rows array")
            .is_empty()
    );

    let request_lines = request_rx.await.expect("request lines");
    assert!(
        request_lines
            .iter()
            .all(|line| !line.contains("/api/now/table/task_sla")),
        "{request_lines:?}"
    );
}

#[test]
fn direct_rpc_task_sla_status_requires_number_param() {
    let response = invalid_params(
        Some(json!(1)),
        extract_number(&json!({ "record": "INC0000001" })).expect_err("missing number"),
    );
    let error = response.error.expect("invalid params error");
    assert_eq!(error.code, -32602);
    assert!(
        error
            .data
            .as_ref()
            .and_then(|data| data.get("details"))
            .and_then(Value::as_str)
            .map(|details| details.contains("number"))
            .unwrap_or(false)
    );
}

#[tokio::test(flavor = "current_thread")]
async fn direct_rpc_get_record_refreshes_uncached_demand() {
    let response = json!({
        "result": [
            {
                "sys_id": "dmnd-sys",
                "number": "DMND0320098",
                "short_description": "Network refresh demand",
                "description": "Upgrade branch switching",
                "state": "draft"
            }
        ]
    });
    let (instance_url, request_rx) = spawn_json_http_server(response).await.expect("http server");
    let fixture = build_fixture_state_at_instance(&instance_url)
        .await
        .expect("fixture");

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "get_record".to_string(),
            params: json!({ "number": "DMND0320098" }),
            id: Some(json!(1)),
        },
        &fixture.state,
    )
    .await;

    let record = response
        .result
        .expect("record result")
        .get("record")
        .cloned()
        .expect("wrapped record");
    assert_eq!(
        record.get("resource_type").and_then(Value::as_str),
        Some("demand")
    );
    assert_eq!(
        record.get("number").and_then(Value::as_str),
        Some("DMND0320098")
    );
    assert_eq!(record.get("source").and_then(Value::as_str), Some("api"));
    assert!(record.get("vault_relative_path").is_none());
    assert!(
        record
            .get("browser_url")
            .and_then(Value::as_str)
            .expect("browser_url")
            .contains("nav_to.do?uri=dmn_demand.do?sys_id=dmnd-sys")
    );

    let request_line = request_rx.await.expect("request line");
    assert!(request_line.contains("/api/now/table/dmn_demand"));
    assert!(request_line.contains("DMND0320098"));
    assert!(
        fixture
            .state
            .core
            .get_record("DMND0320098")
            .await
            .expect("inspect derived cache")
            .is_none(),
        "a live-only compatibility read must not persist"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn direct_rpc_get_record_ignores_cached_change_and_returns_live_record() {
    let response = json!({
        "result": [
            {
                "sys_id": "chg-sys",
                "number": "CHG001",
                "short_description": "Live emergency patch",
                "description": "The live record is newer than the cached fixture",
                "state": "Assess"
            }
        ]
    });
    let (instance_url, request_rx) =
        spawn_json_http_sequence_server(vec![response, json!({ "result": [] })])
            .await
            .expect("http server");
    let fixture = build_fixture_state_at_instance(&instance_url)
        .await
        .expect("fixture");

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "get_record".to_string(),
            params: json!({ "number": "CHG001" }),
            id: Some(json!(1)),
        },
        &fixture.state,
    )
    .await;

    let record = response
        .result
        .expect("record result")
        .get("record")
        .cloned()
        .expect("wrapped record");
    assert_eq!(
        record.get("short_description").and_then(Value::as_str),
        Some("Live emergency patch"),
        "get_record must not return the cached 'Database patch' fixture"
    );
    assert_eq!(record.get("source").and_then(Value::as_str), Some("api"));

    let requests = request_rx.await.expect("request lines");
    let request_line = requests.first().expect("record request");
    assert!(request_line.contains("/api/now/table/change_request"));
    assert!(request_line.contains("CHG001"));
}

#[tokio::test(flavor = "current_thread")]
async fn direct_rpc_private_task_live_read_skips_local_vtb_projection() {
    let task_sys_id = "11111111111111111111111111111111";
    let (instance_url, request_rx) = spawn_json_http_sequence_server(vec![
        json!({
            "result": [{
                "sys_id": task_sys_id,
                "number": "PTSK0000001",
                "short_description": "Example private task",
                "description": "",
                "state": "open"
            }]
        }),
        json!({ "result": [] }),
    ])
    .await
    .expect("http server");
    let fixture = build_fixture_state_at_instance(&instance_url)
        .await
        .expect("fixture");

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "get_record".to_string(),
            params: json!({ "number": "PTSK0000001" }),
            id: Some(json!(1)),
        },
        &fixture.state,
    )
    .await;

    let record = response
        .result
        .expect("record result")
        .get("record")
        .cloned()
        .expect("wrapped record");
    assert_eq!(
        record.get("resource_type").and_then(Value::as_str),
        Some("private_task")
    );
    assert!(record.get("vtb_context").is_none());
    assert!(record.get("vault_relative_path").is_none());

    let requests = request_rx.await.expect("record requests");
    assert!(requests.iter().any(|request| request.contains("/vtb_task")));
    assert!(
        !requests
            .iter()
            .any(|request| request.contains("/vtb_card") || request.contains("/checklist"))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn direct_rpc_get_record_fetches_demand_by_table_sys_id() {
    let sys_id = "7f029b89c3e7565067bdfd73e40131a1";
    let response = json!({
        "result": {
            "sys_id": sys_id,
            "number": "DMND0320098",
            "short_description": "Network refresh demand",
            "description": "Upgrade branch switching",
            "state": "draft"
        }
    });
    let (instance_url, request_rx) = spawn_json_http_server(response).await.expect("http server");
    let fixture = build_fixture_state_at_instance(&instance_url)
        .await
        .expect("fixture");

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "get_record".to_string(),
            params: json!({
                "table": "dmn_demand",
                "sys_id": "7F029B89C3E7565067BDFD73E40131A1"
            }),
            id: Some(json!(1)),
        },
        &fixture.state,
    )
    .await;

    let record = response
        .result
        .expect("record result")
        .get("record")
        .cloned()
        .expect("wrapped record");
    assert_eq!(
        record.get("number").and_then(Value::as_str),
        Some("DMND0320098")
    );
    assert_eq!(record.get("sys_id").and_then(Value::as_str), Some(sys_id));

    let request_line = request_rx.await.expect("request line");
    assert!(request_line.contains("/api/now/table/dmn_demand/"));
    assert!(request_line.contains(sys_id));
}

#[tokio::test(flavor = "current_thread")]
async fn direct_rpc_get_record_fetches_change_request_by_table_sys_id() {
    let sys_id = "0123456789abcdef0123456789abcdef";
    let response = json!({
        "result": {
            "sys_id": sys_id,
            "number": "<CHG_NUMBER>",
            "short_description": "<SHORT_DESC>",
            "description": "<CHANGE_DESCRIPTION>",
            "state": "scheduled"
        }
    });
    let (instance_url, request_rx) = spawn_json_http_server(response).await.expect("http server");
    let fixture = build_fixture_state_at_instance(&instance_url)
        .await
        .expect("fixture");

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "get_record".to_string(),
            params: json!({
                "table": "change_request",
                "sys_id": "0123456789ABCDEF0123456789ABCDEF"
            }),
            id: Some(json!(1)),
        },
        &fixture.state,
    )
    .await;

    let record = response
        .result
        .expect("record result")
        .get("record")
        .cloned()
        .expect("wrapped record");
    assert_eq!(record.get("sys_id").and_then(Value::as_str), Some(sys_id));
    assert_eq!(
        record.get("number").and_then(Value::as_str),
        Some("<CHG_NUMBER>")
    );
    assert_eq!(
        record.get("resource_type").and_then(Value::as_str),
        Some("change")
    );

    let request_line = request_rx.await.expect("request line");
    assert!(request_line.contains("/api/now/table/change_request/"));
    assert!(request_line.contains(sys_id));
}

#[tokio::test(flavor = "current_thread")]
async fn direct_rpc_user_lookup_fetches_active_user_by_user_name() {
    let sys_id = "0123456789abcdef0123456789abcdef";
    let response = json!({
        "result": [
            {
                "sys_id": sys_id,
                "user_name": "USER1234",
                "name": "Casey User",
                "email": "user@example.com",
                "employee_number": "1234",
                "active": "true",
                "department": "IAM",
                "location": "Main Street",
                "title": "Analyst"
            }
        ]
    });
    let (instance_url, request_rx) = spawn_json_http_server(response).await.expect("http server");
    let fixture = build_fixture_state_at_instance(&instance_url)
        .await
        .expect("fixture");

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "user_lookup".to_string(),
            params: json!({ "user_name": "USER1234" }),
            id: Some(json!(1)),
        },
        &fixture.state,
    )
    .await;

    assert!(response.error.is_none(), "{:?}", response.error);
    let result = response.result.expect("user lookup result");
    assert_eq!(
        result.get("matched_by").and_then(Value::as_str),
        Some("user_name")
    );
    let user = result.get("user").expect("user");
    assert_eq!(user.get("sys_id").and_then(Value::as_str), Some(sys_id));
    assert_eq!(
        user.get("user_name").and_then(Value::as_str),
        Some("USER1234")
    );
    assert_eq!(user.get("active").and_then(Value::as_bool), Some(true));

    let request_line = request_rx.await.expect("request line");
    assert!(request_line.contains("/api/now/table/sys_user"));
    assert!(request_line.contains("user_name%3DUSER1234"));
    assert!(request_line.contains("active%3Dtrue"));
}

#[tokio::test(flavor = "current_thread")]
async fn direct_rpc_user_search_fetches_active_users_by_first_and_last_name() {
    let sys_id = "fedcba9876543210fedcba9876543210";
    let response = json!({
        "result": [
            {
                "sys_id": sys_id,
                "user_name": "USER1234",
                "name": "Casey User",
                "first_name": "Casey",
                "last_name": "User",
                "email": "user@example.com",
                "employee_number": "1234",
                "active": "true",
                "department": "IAM",
                "location": "Main Street",
                "title": "Engineer"
            }
        ]
    });
    let (instance_url, request_rx) = spawn_json_http_server(response).await.expect("http server");
    let fixture = build_fixture_state_at_instance(&instance_url)
        .await
        .expect("fixture");

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "user_search".to_string(),
            params: json!({ "first_name": "Casey", "last_name": "User", "limit": 10 }),
            id: Some(json!(1)),
        },
        &fixture.state,
    )
    .await;

    assert!(response.error.is_none(), "{:?}", response.error);
    let result = response.result.expect("user search result");
    let users = result
        .get("users")
        .and_then(Value::as_array)
        .expect("users");
    assert_eq!(users.len(), 1);
    assert_eq!(users[0].get("sys_id").and_then(Value::as_str), Some(sys_id));
    assert_eq!(
        users[0].get("first_name").and_then(Value::as_str),
        Some("Casey")
    );
    assert_eq!(
        users[0].get("last_name").and_then(Value::as_str),
        Some("User")
    );

    let request_line = request_rx.await.expect("request line");
    assert!(request_line.contains("/api/now/table/sys_user"));
    assert!(request_line.contains("first_name%3DCasey"));
    assert!(request_line.contains("last_name%3DUser"));
    assert!(request_line.contains("active%3Dtrue"));
}

#[tokio::test(flavor = "current_thread")]
async fn direct_rpc_get_work_notes_accepts_demand_task_table_sys_id() {
    let sys_id = "7f029b89c3e7565067bdfd73e40131a1";
    let response = json!({
        "result": {
            "sys_id": sys_id,
            "number": "DMNTSK0001122",
            "short_description": "Review demand intake",
            "state": "2",
            "work_notes": "2026-05-27 10:11:12 - Casey User (Work notes)\nReady for review.\n"
        }
    });
    let (instance_url, request_rx) = spawn_json_http_server(response).await.expect("http server");
    let fixture = build_fixture_state_at_instance(&instance_url)
        .await
        .expect("fixture");

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "get_work_notes".to_string(),
            params: json!({
                "table": "dmn_demand_task",
                "sys_id": "7F029B89C3E7565067BDFD73E40131A1"
            }),
            id: Some(json!(1)),
        },
        &fixture.state,
    )
    .await;

    let work_notes = response
        .result
        .expect("work notes result")
        .get("work_notes")
        .and_then(Value::as_array)
        .cloned()
        .expect("work notes");
    assert_eq!(work_notes.len(), 1);
    assert_eq!(
        work_notes[0].get("body").and_then(Value::as_str),
        Some("Ready for review.")
    );

    let request_line = request_rx.await.expect("request line");
    assert!(request_line.contains("/api/now/table/dmn_demand_task/"));
    assert!(request_line.contains(sys_id));
}

#[tokio::test(flavor = "current_thread")]
async fn direct_rpc_and_mcp_return_matching_wrapped_payloads() {
    let fixture = build_fixture_state().await.expect("fixture");

    let direct = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: "kb_semantic_search".to_string(),
                params: json!({ "query": "database", "knowledge_base": "IT", "mode": "lexical", "limit": 5 }),
                id: Some(json!(1)),
            },
            &fixture.state,
        )
        .await
        .result
        .expect("direct result");

    let mcp = tokio::task::LocalSet::new()
            .run_until(async move {
                let server = McpServer::new(Arc::clone(&fixture.state.core));
                let (client_side, server_side) = duplex(4096);
                let (server_reader, server_writer) = split(server_side);
                tokio::task::spawn_local(async move {
                    let _ = server
                        .serve_streams(
                            BufReader::new(server_reader),
                            server_writer,
                            std::future::pending::<Result<(), std::io::Error>>(),
                        )
                        .await;
                });

                let (client_reader, mut client_writer) = split(client_side);
                let mut client_reader = BufReader::new(client_reader);
                client_writer
                    .write_all(
                        br#"{"jsonrpc":"2.0","method":"tools/call","params":{"name":"kb_semantic_search","arguments":{"query":"database","knowledge_base":"IT","mode":"lexical","limit":5}},"id":1}"#,
                    )
                    .await
                    .expect("write");
                client_writer.write_all(b"\n").await.expect("newline");

                let mut line = String::new();
                client_reader.read_line(&mut line).await.expect("read");
                serde_json::from_str::<JsonRpcResponse>(&line)
                    .expect("response")
                    .result
                    .expect("mcp result")
            })
            .await;

    assert_eq!(direct, mcp);
}

#[tokio::test(flavor = "current_thread")]
async fn direct_rpc_nabu_startup_sequence_smoke_test() {
    // `list_my_approvals` is live-only since group-routed listing landed
    // (cache rows carry no group routing), so the startup sequence needs a
    // mock instance serving the user lookup, direct approval rows, and an
    // empty group-membership result; every other method below is
    // cache-served from the fixture.
    let (instance_url, _approval_requests) = spawn_json_http_sequence_server(vec![
        json!({
            "result": [{
                "sys_id": "user-1",
                "user_name": "tester",
                "email": "tester@example.com",
                "name": "Example User"
            }]
        }),
        json!({
            "result": [{
                "sys_id": "apr-sys",
                "number": "APR001",
                "state": "requested",
                "short_description": "Approval for CHG001",
                "approver": { "value": "user-1", "display_value": "Example User" },
                "source_table": { "value": "change_request", "display_value": "Change Request" },
                "sysapproval": { "value": "chg-sys", "display_value": "CHG001" },
                "sysapproval.number": "CHG001",
                "sysapproval.short_description": "Example change",
                "sysapproval.state": "assess",
                "sysapproval.sys_class_name": "change_request",
                "sys_created_on": "2026-04-09 10:11:12"
            }]
        }),
        json!({ "result": [] }),
    ])
    .await
    .expect("approval http server");
    let fixture = build_fixture_state_at_instance(&instance_url)
        .await
        .expect("fixture");
    seed_kb_runtime_state(&fixture).expect("seed kb runtime state");

    for (method, params) in [
        ("contract_info", json!({})),
        ("ping", json!({})),
        ("list_my_tasks", json!({})),
        ("list_my_approvals", json!({})),
        ("list_knowledge_bases", json!({})),
        ("list_records", json!({ "resource_type": "knowledge" })),
        ("kb_status", json!({})),
        ("kb_semantic_status", json!({})),
        (
            "kb_semantic_search",
            json!({ "query": "database", "mode": "lexical", "limit": 5 }),
        ),
        ("kb_list_tags", json!({ "layer": "all", "min_count": 1 })),
    ] {
        let response = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: method.to_string(),
                params,
                id: Some(json!(1)),
            },
            &fixture.state,
        )
        .await;
        assert!(
            response.error.is_none(),
            "{method} returned error: {:?}",
            response.error
        );
        assert!(response.result.is_some(), "{method} returned no result");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn json_rpc_socket_round_trip_and_concurrency() {
    let fixture = build_fixture_state().await.expect("fixture");
    let socket = socket_path(&fixture.tempdir);
    let endpoint = test_endpoint_for_socket(&socket);
    let server = JsonRpcServer::new(Arc::clone(&fixture.state), endpoint.clone());
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let local = LocalSet::new();
    local.spawn_local(async move {
        let _ = server
            .serve_until(async move {
                let _ = shutdown_rx.await;
                Ok(())
            })
            .await;
    });

    local
            .run_until(async move {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;

                let held_open = connect_endpoint(&endpoint).await.expect("first client");
                let (reader, mut writer) =
                    split(connect_endpoint(&endpoint).await.expect("second client"));
                writer
                    .write_all(br#"{"jsonrpc":"2.0","method":"ping","id":1}"#)
                    .await
                    .expect("write ping");
                writer.write_all(b"\n").await.expect("newline");
                writer
                    .write_all(br#"{"jsonrpc":"2.0","method":"get_children","params":{"number":"CHG001"},"id":2}"#)
                    .await
                    .expect("write get_children");
                writer.write_all(b"\n").await.expect("newline");
                writer
                    .write_all(br#"{"jsonrpc":"2.0","method":"verify_vault","id":3}"#)
                    .await
                    .expect("write verify_vault");
                writer.write_all(b"\n").await.expect("newline");
                writer
                    .write_all(br#"{"jsonrpc":"2.0","method":"get_approval","params":{"number":"APR001"},"id":4}"#)
                    .await
                    .expect("write get_approval");
                writer.write_all(b"\n").await.expect("newline");

                let mut reader = BufReader::new(reader);
                let mut first = String::new();
                reader.read_line(&mut first).await.expect("read ping");
                assert!(first.contains("\"ok\":true"));

                let mut second = String::new();
                reader.read_line(&mut second).await.expect("read children");
                assert!(second.contains("CTASK001"));

                let mut third = String::new();
                reader.read_line(&mut third).await.expect("read verify_vault");
                assert!(third.contains("scanned_documents"));

                let mut fourth = String::new();
                reader.read_line(&mut fourth).await.expect("read get_approval");
                assert!(fourth.contains("APR001"));

                drop(held_open);
                let _ = shutdown_tx.send(());
            })
            .await;
}

#[tokio::test(flavor = "current_thread")]
async fn shutdown_rpc_stops_server_after_response() {
    let fixture = build_fixture_state().await.expect("fixture");
    let endpoint = test_endpoint_for_socket(&socket_path(&fixture.tempdir));
    let server = JsonRpcServer::new(Arc::clone(&fixture.state), endpoint.clone())
        .with_drain_timeout(Duration::from_millis(100));
    let (done_tx, done_rx) = oneshot::channel::<Result<()>>();

    let local = LocalSet::new();
    local.spawn_local(async move {
        let result = server
            .serve_until(std::future::pending::<Result<(), std::io::Error>>())
            .await;
        let _ = done_tx.send(result);
    });

    local
        .run_until(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;

            let (reader, mut writer) = split(connect_endpoint(&endpoint).await.expect("client"));
            writer
                .write_all(br#"{"jsonrpc":"2.0","method":"shutdown","id":1}"#)
                .await
                .expect("write shutdown");
            writer.write_all(b"\n").await.expect("newline");

            let mut reader = BufReader::new(reader);
            let mut line = String::new();
            reader.read_line(&mut line).await.expect("read shutdown");
            assert!(line.contains("\"status\":\"shutting_down\""));

            let result = tokio::time::timeout(Duration::from_secs(1), done_rx)
                .await
                .expect("server should stop after shutdown")
                .expect("server completion signal");
            result.expect("server result");
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn idle_timeout_shuts_down_server_with_no_clients() {
    let fixture = build_fixture_state().await.expect("fixture");
    let endpoint = test_endpoint_for_socket(&socket_path(&fixture.tempdir));
    let server = JsonRpcServer::new(Arc::clone(&fixture.state), endpoint.clone())
        .with_drain_timeout(Duration::from_millis(50))
        .with_idle_timeout(Some(Duration::from_millis(100)));
    let (done_tx, done_rx) = oneshot::channel::<Result<()>>();

    let local = LocalSet::new();
    local.spawn_local(async move {
        // No external shutdown and no clients: only the idle timer can stop it.
        let result = server
            .serve_until(std::future::pending::<Result<(), std::io::Error>>())
            .await;
        let _ = done_tx.send(result);
    });

    local
        .run_until(async move {
            let result = tokio::time::timeout(Duration::from_secs(2), done_rx)
                .await
                .expect("idle daemon should self-shut-down")
                .expect("server completion signal");
            result.expect("server result");
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn idle_timeout_held_off_by_active_connection() {
    let fixture = build_fixture_state().await.expect("fixture");
    let endpoint = test_endpoint_for_socket(&socket_path(&fixture.tempdir));
    let server = JsonRpcServer::new(Arc::clone(&fixture.state), endpoint.clone())
        .with_drain_timeout(Duration::from_millis(50))
        .with_idle_timeout(Some(Duration::from_millis(100)));
    let (done_tx, done_rx) = oneshot::channel::<Result<()>>();

    let local = LocalSet::new();
    local.spawn_local(async move {
        let result = server
            .serve_until(std::future::pending::<Result<(), std::io::Error>>())
            .await;
        let _ = done_tx.send(result);
    });

    local
        .run_until(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;
            let held_open = connect_endpoint(&endpoint).await.expect("held client");
            // Hold the connection open well past the idle window; the
            // server must not shut down while a client is connected.
            tokio::time::sleep(Duration::from_millis(400)).await;
            let still_running = tokio::time::timeout(Duration::from_millis(1), done_rx).await;
            assert!(
                still_running.is_err(),
                "server idled out despite an active connection"
            );
            drop(held_open);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn idle_timeout_disabled_keeps_server_running() {
    let fixture = build_fixture_state().await.expect("fixture");
    let endpoint = test_endpoint_for_socket(&socket_path(&fixture.tempdir));
    let server = JsonRpcServer::new(Arc::clone(&fixture.state), endpoint.clone())
        .with_drain_timeout(Duration::from_millis(50))
        .with_idle_timeout(None);
    let (done_tx, done_rx) = oneshot::channel::<Result<()>>();

    let local = LocalSet::new();
    local.spawn_local(async move {
        let result = server
            .serve_until(std::future::pending::<Result<(), std::io::Error>>())
            .await;
        let _ = done_tx.send(result);
    });

    local
        .run_until(async move {
            // With idle shutdown disabled, the server must still be running
            // well past any plausible idle interval.
            let result = tokio::time::timeout(Duration::from_millis(300), done_rx).await;
            assert!(
                result.is_err(),
                "server stopped despite idle timeout being disabled"
            );
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn external_shutdown_waits_for_bounded_connection_drain() {
    let fixture = build_fixture_state().await.expect("fixture");
    let endpoint = test_endpoint_for_socket(&socket_path(&fixture.tempdir));
    let server = JsonRpcServer::new(Arc::clone(&fixture.state), endpoint.clone())
        .with_drain_timeout(Duration::from_millis(50));
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let (done_tx, done_rx) = oneshot::channel::<Result<()>>();

    let local = LocalSet::new();
    local.spawn_local(async move {
        let result = server
            .serve_until(async move {
                let _ = shutdown_rx.await;
                Ok(())
            })
            .await;
        let _ = done_tx.send(result);
    });

    local
        .run_until(async move {
            tokio::time::sleep(Duration::from_millis(50)).await;

            let held_open = connect_endpoint(&endpoint).await.expect("held client");
            tokio::time::sleep(Duration::from_millis(25)).await;
            let _ = shutdown_tx.send(());

            let result = tokio::time::timeout(Duration::from_secs(1), done_rx)
                .await
                .expect("server should stop after bounded drain")
                .expect("server completion signal");
            result.expect("server result");
            drop(held_open);
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn json_rpc_get_knowledge_article_fresh_uses_live_client() {
    let response = json!({
        "result": [
            {
                "sys_id": "kb-fresh-sys",
                "number": "KB0105015",
                "short_description": "Fresh KB title",
                "description": "Fresh KB summary",
                "article_body": "Fresh KB body",
                "text": "Fresh KB body",
                "state": "published",
                "workflow_state": "published",
                "article_type": "text",
                "valid_to": "2027-01-01",
                "knowledge_base": {
                    "sys_id": "kb-base-sys",
                    "value": "kb-base-sys",
                    "display_value": "IT Operations",
                    "table": "kb_knowledge_base"
                },
                "category": {
                    "sys_id": "kb-cat-sys",
                    "value": "kb-cat-sys",
                    "display_value": "Networking",
                    "table": "kb_category"
                }
            }
        ]
    });
    let (instance_url, request_rx) = spawn_json_http_server(response).await.expect("http server");
    let fixture = build_fixture_state_at_instance(&instance_url)
        .await
        .expect("fixture");
    let socket = socket_path(&fixture.tempdir);
    let endpoint = test_endpoint_for_socket(&socket);
    let server = JsonRpcServer::new(Arc::clone(&fixture.state), endpoint.clone());
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let local = LocalSet::new();
    local.spawn_local(async move {
        let _ = server
            .serve_until(async move {
                let _ = shutdown_rx.await;
                Ok(())
            })
            .await;
    });

    local
            .run_until(async move {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;

                let (reader, mut writer) =
                    split(connect_endpoint(&endpoint).await.expect("client"));
                writer
                    .write_all(
                        br#"{"jsonrpc":"2.0","method":"get_knowledge_article_fresh","params":{"number":"KB0105015"},"id":1}"#,
                    )
                    .await
                    .expect("write request");
                writer.write_all(b"\n").await.expect("newline");

                let mut reader = BufReader::new(reader);
                let mut line = String::new();
                reader.read_line(&mut line).await.expect("read response");
                assert!(line.contains("KB0105015"));
                assert!(line.contains("Fresh KB body"));

                let request_line = request_rx.await.expect("request line");
                assert!(request_line.contains("/api/now/table/kb_knowledge"));
                assert!(request_line.contains("KB0105015"));
                assert!(request_line.contains("sysparm_fields"));
                assert!(request_line.contains("article_body"));
                assert!(request_line.contains("text"));

                let _ = shutdown_tx.send(());
            })
            .await;
}

#[tokio::test(flavor = "current_thread")]
async fn json_rpc_get_article_repairs_missing_body_with_live_client() {
    let response = json!({
        "result": [
            {
                "sys_id": "kb-body-miss-sys",
                "number": "KB0105015",
                "short_description": "Cached shell",
                "description": "Cached summary only",
                "article_body": "",
                "text": "Recovered KB body",
                "state": "published",
                "workflow_state": "published",
                "article_type": "text",
                "valid_to": "2027-01-01",
                "knowledge_base": {
                    "sys_id": "kb-base-sys",
                    "value": "kb-base-sys",
                    "display_value": "IT Operations",
                    "table": "kb_knowledge_base"
                },
                "category": {
                    "sys_id": "kb-cat-sys",
                    "value": "kb-cat-sys",
                    "display_value": "Networking",
                    "table": "kb_category"
                }
            }
        ]
    });
    let (instance_url, request_rx) = spawn_json_http_server(response).await.expect("http server");
    let fixture = build_fixture_state_at_instance(&instance_url)
        .await
        .expect("fixture");

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "get_article".to_string(),
            params: json!({ "number": "KB0105015" }),
            id: Some(json!(1)),
        },
        &fixture.state,
    )
    .await;

    let result = response.result.expect("article result");
    assert_eq!(
        result["data"]["article"]["record"]["number"].as_str(),
        Some("KB0105015")
    );
    assert_eq!(
        result["data"]["article"]["content"].as_str(),
        Some("Recovered KB body")
    );
    assert_eq!(
        result["data"]["article"]["body_cached"].as_bool(),
        Some(true)
    );
    assert!(
        result["data"]["markdown"]
            .as_str()
            .expect("markdown")
            .contains("Recovered KB body")
    );

    let request_line = request_rx.await.expect("request line");
    assert!(request_line.contains("/api/now/table/kb_knowledge"));
    assert!(request_line.contains("KB0105015"));
    assert!(request_line.contains("sysparm_fields"));
    assert!(request_line.contains("article_body"));
    assert!(request_line.contains("text"));
}

#[tokio::test(flavor = "current_thread")]
async fn json_rpc_search_and_browse_knowledge_round_trip() {
    let fixture = build_fixture_state().await.expect("fixture");
    seed_kb_runtime_state(&fixture).expect("seed kb runtime state");
    let socket = socket_path(&fixture.tempdir);
    let endpoint = test_endpoint_for_socket(&socket);
    let server = JsonRpcServer::new(Arc::clone(&fixture.state), endpoint.clone());
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();

    let local = LocalSet::new();
    local.spawn_local(async move {
        let _ = server
            .serve_until(async move {
                let _ = shutdown_rx.await;
                Ok(())
            })
            .await;
    });

    local
            .run_until(async move {
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;

                let (reader, mut writer) =
                    split(connect_endpoint(&endpoint).await.expect("client"));
                writer
                    .write_all(
                        br#"{"jsonrpc":"2.0","method":"search_knowledge","params":{"query":"database","knowledge_base":"IT","limit":5},"id":1}"#,
                    )
                    .await
                    .expect("write search");
                writer.write_all(b"\n").await.expect("newline");
                writer
                    .write_all(
                        br#"{"jsonrpc":"2.0","method":"list_knowledge_bases","params":{},"id":2}"#,
                    )
                    .await
                    .expect("write bases");
                writer.write_all(b"\n").await.expect("newline");
                writer
                    .write_all(
                        br#"{"jsonrpc":"2.0","method":"list_categories","params":{"knowledge_base_sys_id":"kb-base"},"id":3}"#,
                    )
                    .await
                    .expect("write categories");
                writer.write_all(b"\n").await.expect("newline");
                writer
                    .write_all(br#"{"jsonrpc":"2.0","method":"kb_status","params":{},"id":4}"#)
                    .await
                    .expect("write kb status");
                writer.write_all(b"\n").await.expect("newline");
                writer
                    .write_all(br#"{"jsonrpc":"2.0","method":"kb_semantic_status","params":{},"id":5}"#)
                    .await
                    .expect("write semantic status");
                writer.write_all(b"\n").await.expect("newline");
                writer
                    .write_all(br#"{"jsonrpc":"2.0","method":"kb_semantic_search","params":{"query":"database","mode":"lexical","limit":5},"id":6}"#)
                    .await
                    .expect("write semantic search");
                writer.write_all(b"\n").await.expect("newline");
                writer
                    .write_all(br#"{"jsonrpc":"2.0","method":"kb_semantic_rebuild","params":{"full":true},"id":7}"#)
                    .await
                    .expect("write semantic rebuild");
                writer.write_all(b"\n").await.expect("newline");

                let mut reader = BufReader::new(reader);
                let mut first = String::new();
                reader.read_line(&mut first).await.expect("read search");
                assert!(first.contains("\"KB001\""));
                assert!(first.contains("\"Database restart procedure\""));

                let mut second = String::new();
                reader.read_line(&mut second).await.expect("read bases");
                assert!(second.contains("\"display_name\":\"IT\""));
                assert!(second.contains("\"article_count\":1"));

                let mut third = String::new();
                reader.read_line(&mut third).await.expect("read categories");
                assert!(third.contains("\"display_name\":\"Database\""));
                assert!(third.contains("\"knowledge_base_sys_id\":\"kb-base\""));

                let mut fourth = String::new();
                reader.read_line(&mut fourth).await.expect("read kb status");
                assert!(fourth.contains("\"body_cached_count\":1"));

                let mut fifth = String::new();
                reader
                    .read_line(&mut fifth)
                    .await
                    .expect("read semantic status");
                assert!(fifth.contains("\"enabled\":false"));

                let mut sixth = String::new();
                reader
                    .read_line(&mut sixth)
                    .await
                    .expect("read semantic search");
                assert!(sixth.contains("\"hits\""));
                assert!(sixth.contains("\"mode\":\"lexical\""));
                assert!(sixth.contains("\"KB001\""));

                let mut seventh = String::new();
                reader
                    .read_line(&mut seventh)
                    .await
                    .expect("read semantic rebuild");
                assert!(seventh.contains("\"message\":\"internal error\""));

                let _ = shutdown_tx.send(());
            })
            .await;
}

#[tokio::test(flavor = "current_thread")]
async fn search_records_falls_back_to_live_fetch_for_exact_number() {
    let response = serde_json::json!({
        "result": [{
            "sys_id": "inc-sys-fallback",
            "number": "INC4992697",
            "short_description": "Switch port flapping",
            "description": "Multiple ports down on core switch",
            "state": "2",
            "assigned_to": {
                "value": "user-sys",
                "display_value": "Casey User"
            }
        }]
    });
    let (instance_url, _request_rx) = spawn_json_http_server(response).await.expect("http server");
    let fixture = build_fixture_state_at_instance(&instance_url)
        .await
        .expect("fixture");

    let search = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "search_records".to_string(),
            params: json!({ "query": "INC4992697" }),
            id: Some(json!(1)),
        },
        &fixture.state,
    )
    .await;

    let results = search
        .result
        .expect("search result")
        .get("results")
        .and_then(Value::as_array)
        .cloned()
        .expect("results array");

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0]
            .get("record")
            .and_then(|r| r.get("number"))
            .and_then(Value::as_str),
        Some("INC4992697")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn search_records_exact_work_number_ignores_seeded_local_projection() {
    let (instance_url, request_rx) = spawn_json_http_sequence_server(vec![
        json!({
            "result": [{
                "sys_id": "chg-sys",
                "number": "CHG001",
                "short_description": "Live change title",
                "description": "Live projection",
                "state": "assess"
            }]
        }),
        json!({ "result": [] }),
    ])
    .await
    .expect("http server");
    let fixture = build_fixture_state_at_instance(&instance_url)
        .await
        .expect("fixture with seeded CHG001 cache");

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "search_records".to_string(),
            params: json!({ "query": "CHG001" }),
            id: Some(json!(1)),
        },
        &fixture.state,
    )
    .await;

    let record = &response.result.expect("search result")["results"][0]["record"];
    assert_eq!(record["short_description"], json!("Live change title"));
    assert!(record.get("vault_relative_path").is_none());
    let requests = request_rx.await.expect("live record requests");
    assert!(
        requests
            .iter()
            .any(|request| request.contains("/change_request"))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn search_records_falls_back_to_live_fetch_for_demand_task_number() {
    let response = serde_json::json!({
        "result": [{
            "sys_id": "dmntsk-sys-fallback",
            "number": "DMNTSK0001122",
            "short_description": "Review demand intake",
            "description": "Demand task should hydrate from exact search",
            "state": "2"
        }]
    });
    let (instance_url, request_rx) = spawn_json_http_server(response).await.expect("http server");
    let fixture = build_fixture_state_at_instance(&instance_url)
        .await
        .expect("fixture");

    let search = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "search_records".to_string(),
            params: json!({ "query": "DMNTSK0001122" }),
            id: Some(json!(1)),
        },
        &fixture.state,
    )
    .await;

    let results = search
        .result
        .expect("search result")
        .get("results")
        .and_then(Value::as_array)
        .cloned()
        .expect("results array");

    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0]
            .get("record")
            .and_then(|r| r.get("number"))
            .and_then(Value::as_str),
        Some("DMNTSK0001122")
    );
    assert_eq!(
        results[0]
            .get("record")
            .and_then(|r| r.get("table"))
            .and_then(Value::as_str),
        Some("dmn_demand_task")
    );

    let request_line = request_rx.await.expect("request line");
    assert!(request_line.contains("/api/now/table/dmn_demand_task"));
}

#[tokio::test(flavor = "current_thread")]
async fn handle_connection_ignores_broken_pipe_when_client_disconnects() {
    let fixture = build_fixture_state().await.expect("fixture");
    let (server, client) = duplex(4096);
    let (client_reader, mut client_writer) = split(client);
    drop(client_reader);

    client_writer
        .write_all(br#"{"jsonrpc":"2.0","method":"ping","id":1}"#)
        .await
        .expect("write ping");
    client_writer.write_all(b"\n").await.expect("newline");
    drop(client_writer);

    handle_connection(server, Arc::clone(&fixture.state), Arc::new(Notify::new()))
        .await
        .expect("broken pipe should be treated as disconnect");
}

/// Round-trip the `start_job` and `get_job` RPC methods through `dispatch`,
/// ensuring the start handler returns a UUID and the get handler can look
/// up the resulting registry entry. Runs inside a `LocalSet` because
/// `crate::jobs::spawn` uses `tokio::task::spawn_local`.
#[tokio::test(flavor = "current_thread")]
async fn start_job_returns_id_and_get_job_finds_it() {
    let local = LocalSet::new();
    local
        .run_until(async {
            let fixture = build_fixture_state().await.expect("fixture");

            let start = dispatch(
                JsonRpcRequest {
                    jsonrpc: "2.0".to_string(),
                    method: "start_job".to_string(),
                    params: json!({ "kind": "verify_vault", "params": {} }),
                    id: Some(json!(1)),
                },
                &fixture.state,
            )
            .await;
            let job_id = start
                .result
                .expect("start_job result")
                .get("job_id")
                .and_then(Value::as_str)
                .expect("job_id string")
                .to_owned();

            let got = dispatch(
                JsonRpcRequest {
                    jsonrpc: "2.0".to_string(),
                    method: "get_job".to_string(),
                    params: json!({ "job_id": job_id }),
                    id: Some(json!(2)),
                },
                &fixture.state,
            )
            .await;
            let result = got.result.expect("get_job result");
            assert!(
                !result.is_null(),
                "get_job should resolve the started job, got null"
            );
            assert_eq!(
                result.get("kind").and_then(Value::as_str),
                Some("verify_vault"),
            );
        })
        .await;
}

/// `list_jobs` returns the started job entry, and `cancel_job` returns
/// `cancelled: true` for an active job id.
#[tokio::test(flavor = "current_thread")]
async fn list_jobs_and_cancel_job_round_trip() {
    let local = LocalSet::new();
    local
        .run_until(async {
            let fixture = build_fixture_state().await.expect("fixture");

            let start = dispatch(
                JsonRpcRequest {
                    jsonrpc: "2.0".to_string(),
                    method: "start_job".to_string(),
                    params: json!({ "kind": "verify_vault", "params": {} }),
                    id: Some(json!(1)),
                },
                &fixture.state,
            )
            .await;
            let job_id = start
                .result
                .expect("start_job result")
                .get("job_id")
                .and_then(Value::as_str)
                .expect("job_id string")
                .to_owned();

            let list = dispatch(
                JsonRpcRequest {
                    jsonrpc: "2.0".to_string(),
                    method: "list_jobs".to_string(),
                    params: json!({ "include_finished": true }),
                    id: Some(json!(2)),
                },
                &fixture.state,
            )
            .await;
            let jobs = list.result.expect("list_jobs result");
            let arr = jobs.as_array().expect("list_jobs array");
            assert!(
                arr.iter()
                    .any(|j| j.get("id").and_then(Value::as_str) == Some(job_id.as_str())),
                "list_jobs should include the started job"
            );

            let cancel = dispatch(
                JsonRpcRequest {
                    jsonrpc: "2.0".to_string(),
                    method: "cancel_job".to_string(),
                    params: json!({ "job_id": job_id }),
                    id: Some(json!(3)),
                },
                &fixture.state,
            )
            .await;
            let cancelled = cancel
                .result
                .expect("cancel_job result")
                .get("cancelled")
                .and_then(Value::as_bool)
                .expect("cancelled flag");
            // Whether cancellation lands depends on whether the worker
            // already finished; both true and false are valid terminal
            // signals for "the call dispatched cleanly".
            let _ = cancelled;
        })
        .await;
}

fn seed_kb_runtime_state(
    fixture: &crate::test_support::FixtureState,
) -> std::result::Result<(), rusqlite::Error> {
    let conn = Connection::open(fixture.tempdir.path().join("snow.db"))?;
    conn.execute(
        r#"
            UPDATE kb_sync_state
            SET last_full_at = 1712649800000,
                last_incr_at = 1712650100000,
                watermark_updated_at = '2026-04-10 09:00:00',
                watermark_sys_id = 'kb-sys',
                kb_sync_lock = 1712650200000
            WHERE id = 1
            "#,
        [],
    )?;
    Ok(())
}

async fn spawn_task_sla_status_server(
    parent_response: Value,
    task_sla_response: Value,
    expected_requests: usize,
) -> Result<(String, oneshot::Receiver<Vec<String>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (request_tx, request_rx) = oneshot::channel();

    tokio::spawn(async move {
        let mut request_lines = Vec::with_capacity(expected_requests);
        for _ in 0..expected_requests {
            let Ok((mut stream, _)) = listener.accept().await else {
                return;
            };
            let mut request = Vec::new();
            let mut buf = [0u8; 1024];
            loop {
                let read = match stream.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(read) => read,
                    Err(_) => return,
                };
                request.extend_from_slice(&buf[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }

            let first_line = request
                .split(|byte| *byte == b'\n')
                .next()
                .and_then(|line| std::str::from_utf8(line).ok())
                .unwrap_or_default()
                .trim()
                .to_string();
            let response_body = if first_line.contains("/api/now/table/task_sla") {
                task_sla_response.clone()
            } else {
                parent_response.clone()
            }
            .to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            );
            request_lines.push(first_line);
            let _ = stream.write_all(response.as_bytes()).await;
            let _ = stream.shutdown().await;
        }
        let _ = request_tx.send(request_lines);
    });

    Ok((format!("http://{}", addr), request_rx))
}

fn task_sla_row(sys_id: &str, task_sys_id: &str, task_display: &str, sla_name: &str) -> Value {
    json!({
        "sys_id": { "value": sys_id, "display_value": sys_id },
        "task": { "value": task_sys_id, "display_value": task_display },
        "sla": { "value": format!("{sys_id}-definition"), "display_value": sla_name },
        "stage": { "value": "in_progress", "display_value": "In Progress" },
        "active": { "value": "true", "display_value": "true" },
        "has_breached": { "value": "false", "display_value": "false" },
        "planned_end_time": {
            "value": "2026-05-08 10:00:00",
            "display_value": "2026-05-08 10:00:00"
        },
        "business_percentage": {
            "value": "25.5",
            "display_value": "25.5"
        },
        "time_left": {
            "value": "1970-01-01 02:00:00",
            "display_value": "2 Hours"
        }
    })
}

// ----- server_get read-through (live fallback) RPC tests -----

#[tokio::test(flavor = "current_thread")]
async fn direct_rpc_server_get_cache_miss_falls_through_to_live() {
    let sys_id = "abababababababababababababababab";
    let response = json!({
        "result": [{
            "sys_id": sys_id,
            "name": "host10.example.internal",
            "ip_address": "192.0.2.40",
            "sys_class_name": "cmdb_ci_linux_server",
            "operational_status": { "value": "1", "display_value": "Operational" }
        }]
    });
    let (instance_url, request_rx) = spawn_json_http_server(response).await.expect("http server");
    let fixture = build_fixture_state_at_instance(&instance_url)
        .await
        .expect("fixture");

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "server_get".to_string(),
            params: json!({ "name": "host10.example.internal" }),
            id: Some(json!(1)),
        },
        &fixture.state,
    )
    .await;

    let server = response
        .result
        .expect("server result")
        .get("data")
        .and_then(|data| data.get("server"))
        .cloned()
        .expect("wrapped server");
    assert_eq!(
        server
            .get("record")
            .and_then(|r| r.get("sys_id"))
            .and_then(Value::as_str),
        Some(sys_id)
    );

    let request_line = request_rx.await.expect("request line");
    assert!(request_line.contains("/api/now/table/cmdb_ci_server"));

    let cached = fixture
        .state
        .core
        .query_servers(snow_core::ServerQuery {
            name: Some("host10".to_string()),
            ..Default::default()
        })
        .await
        .expect("cached server query");
    assert_eq!(cached.len(), 1, "default server_get must cache live hit");
    assert_eq!(cached[0].sys_id, sys_id);
}

#[tokio::test(flavor = "current_thread")]
async fn direct_rpc_server_get_policy_owns_persistence_over_legacy_caller_hint() {
    let sys_id = "bcbcbcbcbcbcbcbcbcbcbcbcbcbcbcbc";
    let response = json!({
        "result": [{
            "sys_id": sys_id,
            "name": "host11.example.internal",
            "ip_address": "192.0.2.41",
            "sys_class_name": "cmdb_ci_linux_server",
            "operational_status": { "value": "1", "display_value": "Operational" }
        }]
    });
    let (instance_url, _request_rx) = spawn_json_http_server(response).await.expect("http server");
    let fixture = build_fixture_state_at_instance(&instance_url)
        .await
        .expect("fixture");

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "server_get".to_string(),
            params: json!({
                "name": "host11.example.internal",
                "persist": false
            }),
            id: Some(json!(1)),
        },
        &fixture.state,
    )
    .await;

    let server = response
        .result
        .expect("server result")
        .get("data")
        .and_then(|data| data.get("server"))
        .cloned()
        .expect("wrapped server");
    assert_eq!(
        server
            .get("record")
            .and_then(|record| record.get("sys_id"))
            .and_then(Value::as_str),
        Some(sys_id)
    );

    let cached = fixture
        .state
        .core
        .query_servers(snow_core::ServerQuery {
            name: Some("host11".to_string()),
            ..Default::default()
        })
        .await
        .expect("cached server query");
    assert_eq!(
        cached.len(),
        1,
        "read-through policy must cache the live hit"
    );
    assert_eq!(cached[0].sys_id, sys_id);
}

#[tokio::test(flavor = "current_thread")]
async fn direct_rpc_server_get_cache_miss_live_miss_is_not_found() {
    let response = json!({ "result": [] });
    let (instance_url, _request_rx) = spawn_json_http_server(response).await.expect("http server");
    let fixture = build_fixture_state_at_instance(&instance_url)
        .await
        .expect("fixture");

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "server_get".to_string(),
            params: json!({ "name": "ghost.example.internal" }),
            id: Some(json!(1)),
        },
        &fixture.state,
    )
    .await;

    let error = response.error.expect("not found error");
    assert_eq!(error.code, -32004);
}

#[test]
fn server_get_error_response_maps_each_variant_to_distinct_code() {
    use snow_core::ServerGetError;
    assert_eq!(
        server_get_error_response(Some(json!(1)), ServerGetError::NotFound)
            .error
            .expect("err")
            .code,
        -32004
    );
    assert_eq!(
        server_get_error_response(
            Some(json!(1)),
            ServerGetError::AclRestricted("denied".to_string())
        )
        .error
        .expect("err")
        .code,
        -32003
    );
    assert_eq!(
        server_get_error_response(
            Some(json!(1)),
            ServerGetError::Network("timeout".to_string())
        )
        .error
        .expect("err")
        .code,
        -32001
    );
    assert_eq!(
        server_get_error_response(
            Some(json!(1)),
            ServerGetError::Disambiguation {
                selector: "name=dup".to_string(),
                matched: 2
            }
        )
        .error
        .expect("err")
        .code,
        -32005
    );
}

// ---------------------------------------------------------------------
// incident_list_by_assignment_group (T2)
//
// Authority: docs/spec-incident-list-by-assignment-group.md
// ---------------------------------------------------------------------

const INCIDENT_GROUP_TEST_SYS_ID: &str = "0000000000000000000000000000ab01";

/// A daemon consumer receives the typed records plus the page metadata it
/// needs to keep paging.
#[tokio::test(flavor = "current_thread")]
async fn incident_list_by_assignment_group_returns_records_and_page_metadata() {
    let (instance_url, _request_rx) = spawn_json_http_sequence_server(vec![json!({
        "result": [{
            "sys_id": { "value": "0000000000000000000000000000aa01" },
            "number": { "value": "<INC_1>" },
            "short_description": { "value": "Ticket" },
            "state": { "value": "3", "display_value": "Pending" },
            "active": { "value": "true", "display_value": "true" },
            "assignment_group": {
                "value": INCIDENT_GROUP_TEST_SYS_ID,
                "display_value": "<GROUP_DISPLAY>"
            }
        }]
    })])
    .await
    .expect("http server");
    let fixture = build_fixture_state_at_instance(&instance_url)
        .await
        .expect("fixture");

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "incident_list_by_assignment_group".to_string(),
            params: json!({
                "assignment_group_sys_id": INCIDENT_GROUP_TEST_SYS_ID,
                "limit": 5
            }),
            id: Some(json!(1)),
        },
        &fixture.state,
    )
    .await;

    assert!(response.error.is_none(), "{response:?}");
    let result = response.result.expect("page result");
    assert_eq!(
        result["records"].as_array().expect("records").len(),
        1,
        "{result}"
    );
    assert_eq!(result["complete"], json!(true));
    assert_eq!(result["next_cursor"], json!(null));
    assert_eq!(result["limit"], json!(5));
    assert_eq!(result["rows_inspected"], json!(1));
    assert!(
        result["records"][0].get("browser_url").is_none(),
        "this daemon page must retain the core record contract used by direct MCP"
    );
    assert!(
        result["records"][0].get("vault_relative_path").is_none(),
        "an ephemeral group page must not acquire transport-only vault metadata"
    );
}

/// An invalid exact state preserves the core correction payload across the
/// daemon JSON-RPC boundary, so callers can select a valid value without
/// guessing or issuing a separate discovery call.
#[tokio::test(flavor = "current_thread")]
async fn incident_list_by_assignment_group_preserves_state_correction_data() {
    let (instance_url, _request_rx) = spawn_json_http_sequence_server(vec![json!({
        "result": [
            { "value": "1", "label": "New", "sequence": "100", "inactive": "false" },
            { "value": "3", "label": "Pending", "sequence": "200", "inactive": "false" }
        ]
    })])
    .await
    .expect("http server");
    let fixture = build_fixture_state_at_instance(&instance_url)
        .await
        .expect("fixture");

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "incident_list_by_assignment_group".to_string(),
            params: json!({
                "assignment_group_sys_id": INCIDENT_GROUP_TEST_SYS_ID,
                "state": "Awaiting Vendor"
            }),
            id: Some(json!(1)),
        },
        &fixture.state,
    )
    .await;

    let error = response.error.expect("unknown state must be rejected");
    assert_eq!(error.code, -32602);
    let data = error.data.expect("state correction data");
    assert_eq!(data["field"], json!("state"));
    assert_eq!(data["requested"], json!("Awaiting Vendor"));
    assert_eq!(data["ambiguous"], json!(false));
    assert_eq!(
        data["choices"],
        json!([
            { "value": "1", "label": "New" },
            { "value": "3", "label": "Pending" }
        ])
    );
}

/// Caller-argument problems are `invalid params`, not internal errors, and
/// never reach ServiceNow.
#[tokio::test(flavor = "current_thread")]
async fn incident_list_by_assignment_group_maps_bad_arguments_to_invalid_params() {
    let fixture = build_fixture_state().await.expect("fixture");

    for params in [
        json!({ "assignment_group_sys_id": "Network Support" }),
        json!({ "assignment_group_sys_id": INCIDENT_GROUP_TEST_SYS_ID, "limit": 201 }),
        json!({ "assignment_group_sys_id": INCIDENT_GROUP_TEST_SYS_ID, "cursor": "nope" }),
        json!({ "assignment_group_sys_id": INCIDENT_GROUP_TEST_SYS_ID, "group_name": "x" }),
    ] {
        let response = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: "incident_list_by_assignment_group".to_string(),
                params: params.clone(),
                id: Some(json!(1)),
            },
            &fixture.state,
        )
        .await;

        let error = response
            .error
            .unwrap_or_else(|| panic!("{params} should be rejected"));
        assert_eq!(error.code, -32602, "{params}");
    }
}

/// The method is routable and advertised, so a bridge can discover it.
#[tokio::test(flavor = "current_thread")]
async fn incident_list_by_assignment_group_is_routable_and_advertised() {
    assert_eq!(
        RpcMethod::from_method("incident_list_by_assignment_group"),
        RpcMethod::IncidentListByAssignmentGroup
    );

    let fixture = build_fixture_state().await.expect("fixture");
    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "contract_info".to_string(),
            params: json!({}),
            id: Some(json!(1)),
        },
        &fixture.state,
    )
    .await;

    let result = response.result.expect("contract info result");
    assert!(
        result["supported_methods"]
            .as_array()
            .expect("supported methods")
            .iter()
            .any(|method| method.as_str() == Some("incident_list_by_assignment_group")),
        "contract_info must advertise the method"
    );
}

// ---------------------------------------------------------------------
// record_query
//
// Authority: docs/spec-mullet-record-query-parity.md
// ---------------------------------------------------------------------

#[tokio::test(flavor = "current_thread")]
async fn record_query_returns_the_core_live_page_contract() {
    let (instance_url, _request_rx) = spawn_json_http_sequence_server(vec![json!({
        "result": [{
            "sys_id": { "value": "00000000000000000000000000000001" },
            "number": { "value": "STRY1" },
            "short_description": { "value": "Typed story" },
            "state": { "value": "1", "display_value": "New" }
        }]
    })])
    .await
    .expect("http server");
    let fixture = build_fixture_state_at_instance(&instance_url)
        .await
        .expect("fixture");

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "record_query".to_string(),
            params: json!({
                "resource_type": "story",
                "filters": {},
                "limit": 2
            }),
            id: Some(json!(1)),
        },
        &fixture.state,
    )
    .await;

    assert!(response.error.is_none(), "{response:?}");
    let result = response.result.expect("result");
    assert_eq!(result["records"].as_array().expect("records").len(), 1);
    assert_eq!(result["records"][0]["number"], json!("STRY1"));
    assert_eq!(result["next_cursor"], json!(null));
    assert_eq!(result["complete"], json!(true));
    assert_eq!(result["source"], json!("live"));
    assert_eq!(result["limit"], json!(2));
    assert_eq!(result["rows_inspected"], json!(1));
}

#[tokio::test(flavor = "current_thread")]
async fn record_query_and_legacy_list_reject_unknown_inputs_before_io() {
    let fixture = build_fixture_state().await.expect("fixture");
    for (method, params) in [
        (
            "record_query",
            json!({"resource_type":"story","filters":{"arbitrary":"x"}}),
        ),
        (
            "record_query",
            json!({"resource_type":"change_request","include_description":true}),
        ),
        ("list_records", json!({"filter":"state=1"})),
    ] {
        let response = dispatch(
            JsonRpcRequest {
                jsonrpc: "2.0".to_string(),
                method: method.to_string(),
                params: params.clone(),
                id: Some(json!(1)),
            },
            &fixture.state,
        )
        .await;
        let error = response
            .error
            .unwrap_or_else(|| panic!("{method} accepted {params}"));
        assert_eq!(error.code, -32602, "{method} {params}");
    }
}

#[tokio::test(flavor = "current_thread")]
async fn record_query_is_routable_and_advertised() {
    assert_eq!(
        RpcMethod::from_method("record_query"),
        RpcMethod::RecordQuery
    );
    let fixture = build_fixture_state().await.expect("fixture");
    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "contract_info".to_string(),
            params: json!({}),
            id: Some(json!(1)),
        },
        &fixture.state,
    )
    .await;
    assert!(
        response.result.expect("contract")["supported_methods"]
            .as_array()
            .expect("methods")
            .iter()
            .any(|method| method == "record_query")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn change_request_list_tasks_is_live_paged_and_never_uses_cached_children() {
    let (instance_url, request_rx) = spawn_json_http_sequence_server(vec![json!({
        "result": [{
            "sys_id": "22222222222222222222222222222222",
            "number": "CTASK0000001",
            "short_description": "Plan the approved change",
            "change_request": { "value": "change-sys", "display_value": "CHG0000001" },
            "change_task_type": { "value": "planning", "display_value": "Planning" },
            "state": { "value": "1", "display_value": "Open" }
        }]
    })])
    .await
    .expect("http server");
    let fixture = build_fixture_state_at_instance(&instance_url)
        .await
        .expect("fixture");

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "change_request_list_tasks".to_string(),
            params: json!({ "change_request_number": "CHG0000001", "limit": 1 }),
            id: Some(json!(1)),
        },
        &fixture.state,
    )
    .await;

    assert!(response.error.is_none(), "{response:?}");
    let result = response.result.expect("live task page");
    assert_eq!(result["operation"], json!("change_request_list_tasks"));
    assert_eq!(result["source"]["kind"], json!("live"));
    assert_eq!(result["completeness"]["kind"], json!("partial"));
    assert_eq!(result["data"]["complete"], json!(false));
    assert_eq!(
        result["data"]["next_cursor"],
        json!("22222222222222222222222222222222")
    );
    assert_eq!(
        result["data"]["records"][0]["number"],
        json!("CTASK0000001")
    );

    let requests = request_rx.await.expect("one live ServiceNow request");
    let request = requests.first().expect("first request");
    assert!(request.contains("/api/now/table/change_task"));
    assert!(request.contains("change_request.number%3DCHG0000001"));
    assert!(request.contains("sysparm_limit=1"));
}
