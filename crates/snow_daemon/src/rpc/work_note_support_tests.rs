//! L0 JSON-RPC regression tests for governed work-note field support.
//!
//! These drive `work_note_plan_add` through the daemon dispatcher against a
//! local ServiceNow fake. The mutation they catch is skipping or weakening the
//! live `sys_dictionary` proof that the resolved record table supports
//! `work_notes`.

use std::sync::Arc;

use serde_json::json;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::DaemonState;
use crate::rpc::{JsonRpcRequest, dispatch};
use crate::test_support::build_fixture_state_at_instance;

fn work_note_enabled_state(fixture: &crate::test_support::FixtureState) -> Arc<DaemonState> {
    let mut policy = snow_mcp::domain::policy::PolicyConfig::default();
    policy
        .tools
        .get_mut("work_note_apply_add")
        .expect("work-note policy")
        .enabled = true;

    Arc::new(DaemonState::with_data_dir_and_mcp_config(
        Arc::clone(&fixture.state.core),
        fixture.tempdir.path().join("work-note-field-support"),
        snow_mcp::McpConfig {
            environment: snow_mcp::McpEnvironment::explicit_config("test", "America/Chicago"),
            policy,
            ..Default::default()
        },
    ))
}

async fn plan_work_note(state: &Arc<DaemonState>, number: &str) -> crate::rpc::JsonRpcResponse {
    dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "work_note_plan_add".to_string(),
            params: json!({
                "number": number,
                "work_notes": "Validated against the resolved record table."
            }),
            id: Some(json!(1)),
        },
        state,
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn daemon_rpc_work_note_plan_add_succeeds_when_resolved_table_supports_work_notes() {
    let instance = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_dictionary"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{
                "name": "change_request",
                "element": "work_notes",
                "internal_type": "journal_input",
                "read_only": "false",
                "choice": "0",
                "active": "true"
            }]
        })))
        .expect(1)
        .mount(&instance)
        .await;
    mount_no_ancestors(&instance, "change_request").await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");
    let state = work_note_enabled_state(&fixture);

    let response = plan_work_note(&state, "CHG001").await;

    assert!(response.error.is_none(), "{response:?}");
    let result = response.result.expect("plan");
    assert_eq!(result["target"]["table"], json!("change_request"));
    assert_eq!(
        result["preview"]["work_notes"],
        json!("Validated against the resolved record table.")
    );
    assert!(result["confirmation_token"].is_string());
    assert!(result["idempotency_key"].is_string());
}

#[tokio::test(flavor = "current_thread")]
async fn daemon_rpc_work_note_plan_add_refuses_when_resolved_table_lacks_work_notes() {
    let instance = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_dictionary"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{
                "name": "resource_plan",
                "element": "short_description",
                "internal_type": "string",
                "read_only": "false",
                "choice": "0",
                "active": "true"
            }]
        })))
        .mount(&instance)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_db_object"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{
                "name": "resource_plan",
                "super_class": ""
            }]
        })))
        .mount(&instance)
        .await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");
    let state = work_note_enabled_state(&fixture);

    let response = plan_work_note(&state, "RPLN001").await;

    let error = response.error.expect("unsupported table must be refused");
    assert_eq!(error.code, -32053);
    assert_eq!(error.message, "WORK_NOTES_UNSUPPORTED");
    assert_eq!(
        error.data.expect("typed error data"),
        json!({
            "code": "WORK_NOTES_UNSUPPORTED",
            "field": "work_notes",
            "table": "resource_plan"
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn daemon_rpc_work_note_plan_add_refuses_when_field_discovery_is_unavailable() {
    let instance = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_dictionary"))
        .respond_with(ResponseTemplate::new(403).set_body_json(json!({
            "error": { "message": "Access denied" }
        })))
        .mount(&instance)
        .await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");
    let state = work_note_enabled_state(&fixture);

    let response = plan_work_note(&state, "CHG001").await;

    let error = response
        .error
        .expect("unavailable discovery must be refused");
    assert_eq!(error.code, -32054);
    assert_eq!(error.message, "WORK_NOTES_DISCOVERY_UNAVAILABLE");
    assert_eq!(
        error.data.expect("typed error data"),
        json!({
            "code": "WORK_NOTES_DISCOVERY_UNAVAILABLE",
            "field": "work_notes",
            "table": "change_request"
        })
    );
}

#[tokio::test(flavor = "current_thread")]
async fn daemon_rpc_work_note_apply_add_rechecks_field_discovery_before_writing() {
    let instance = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_dictionary"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{
                "name": "change_request",
                "element": "work_notes",
                "internal_type": "journal_input",
                "read_only": "false",
                "choice": "0",
                "active": "true"
            }]
        })))
        .up_to_n_times(1)
        .expect(1)
        .mount(&instance)
        .await;
    mount_no_ancestors(&instance, "change_request").await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");
    let state = work_note_enabled_state(&fixture);

    let plan = plan_work_note(&state, "CHG001").await;
    let result = plan.result.expect("work-note plan");

    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_dictionary"))
        .respond_with(ResponseTemplate::new(403).set_body_json(json!({
            "error": { "message": "Access denied" }
        })))
        .expect(1)
        .mount(&instance)
        .await;

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "work_note_apply_add".to_string(),
            params: json!({
                "plan_id": result["plan_id"],
                "confirmation_token": result["confirmation_token"],
                "idempotency_key": result["idempotency_key"]
            }),
            id: Some(json!(2)),
        },
        &state,
    )
    .await;

    let error = response
        .error
        .expect("apply must stop before the ServiceNow write");
    assert_eq!(error.code, -32054);
    assert_eq!(error.message, "WORK_NOTES_DISCOVERY_UNAVAILABLE");
    assert_eq!(
        error.data.expect("typed error data")["code"],
        json!("WORK_NOTES_DISCOVERY_UNAVAILABLE")
    );
}

async fn mount_no_ancestors(server: &MockServer, table: &str) {
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_db_object"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{ "name": table, "super_class": "" }]
        })))
        .mount(server)
        .await;
}
