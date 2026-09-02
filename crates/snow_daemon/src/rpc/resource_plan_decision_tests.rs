//! L0 daemon JSON-RPC tests for governed Resource Plan decisions.

use std::collections::BTreeSet;
use std::sync::Arc;

use serde_json::{Value, json};
use wiremock::matchers::{body_json, method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use super::{JsonRpcRequest, JsonRpcResponse, dispatch};
use crate::DaemonState;
use crate::test_support::{FixtureState, build_fixture_state_at_instance};

const PLAN_NUMBER: &str = "RPLN0000001";
const PLAN_SYS_ID: &str = "11111111111111111111111111111111";

fn decision_enabled_state(fixture: &FixtureState) -> Arc<DaemonState> {
    use snow_mcp::domain::policy::{PolicyConfig, ToolPolicy};

    let environments = vec!["test".to_string()];
    let mut policy = PolicyConfig::default();
    policy.tools.insert(
        "resource_plan_plan_decision".to_string(),
        ToolPolicy {
            enabled: true,
            environments: environments.clone(),
            ..ToolPolicy::default()
        },
    );
    policy.tools.insert(
        "resource_plan_apply_decision".to_string(),
        ToolPolicy {
            enabled: true,
            requires_confirmation: true,
            field_allowlist: BTreeSet::from(["state".to_string()]),
            environments,
            confirmation_ttl_seconds: Some(600),
            ..ToolPolicy::default()
        },
    );

    Arc::new(DaemonState::with_data_dir_and_mcp_config(
        Arc::clone(&fixture.state.core),
        fixture.tempdir.path().join("resource-plan-decision"),
        snow_mcp::McpConfig {
            environment: snow_mcp::McpEnvironment::explicit_config("test", "America/Chicago"),
            policy,
            ..Default::default()
        },
    ))
}
async fn plan_confirm(state: &Arc<DaemonState>) -> JsonRpcResponse {
    dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "resource_plan_plan_decision".to_string(),
            params: json!({
                "number": PLAN_NUMBER,
                "decision": "confirm"
            }),
            id: Some(json!(1)),
        },
        state,
    )
    .await
}

fn resource_plan_row(state: &str, state_label: &str, updated_on: &str) -> Value {
    json!({
        "sys_id": PLAN_SYS_ID,
        "number": PLAN_NUMBER,
        "short_description": "Public-safe resource plan fixture",
        "state": {"value": state, "display_value": state_label},
        "planned_hours": "60",
        "sys_updated_on": updated_on,
        "sys_mod_count": if state == "2" { "7" } else { "8" },
        "work_notes": "",
        "comments": ""
    })
}

async fn mount_plan_lookup(instance: &MockServer, state: &str, label: &str, updated_on: &str) {
    Mock::given(method("GET"))
        .and(path("/api/now/table/resource_plan"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [resource_plan_row(state, label, updated_on)]
        })))
        .mount(instance)
        .await;
}

async fn mount_apply_transition(instance: &MockServer, include_allocation: bool) {
    Mock::given(method("GET"))
        .and(path(format!("/api/now/table/resource_plan/{PLAN_SYS_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": resource_plan_row("2", "Requested", "2026-08-20 12:00:00")
        })))
        .up_to_n_times(1)
        .expect(1)
        .mount(instance)
        .await;
    Mock::given(method("PATCH"))
        .and(path(format!("/api/now/table/resource_plan/{PLAN_SYS_ID}")))
        .and(body_json(json!({"state": "11"})))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": resource_plan_row("11", "Confirmed", "2026-08-20 12:01:00")
        })))
        .expect(1)
        .mount(instance)
        .await;
    Mock::given(method("GET"))
        .and(path(format!("/api/now/table/resource_plan/{PLAN_SYS_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": resource_plan_row("11", "Confirmed", "2026-08-20 12:01:00")
        })))
        .mount(instance)
        .await;

    let allocations = if include_allocation {
        vec![json!({
            "sys_id": "22222222222222222222222222222222",
            "resource_plan": PLAN_SYS_ID,
            "booking_type": {"value": "1", "display_value": "Soft"}
        })]
    } else {
        Vec::new()
    };
    Mock::given(method("GET"))
        .and(path("/api/now/table/resource_allocation"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result": allocations})))
        .expect(1)
        .mount(instance)
        .await;
}

fn apply_params(plan: &Value) -> Value {
    json!({
        "plan_id": plan["plan_id"],
        "confirmation_token": plan["confirmation_token"],
        "idempotency_key": plan["idempotency_key"],
        "concurrency_token": plan["concurrency_token"]
    })
}

#[tokio::test(flavor = "current_thread")]
async fn daemon_rpc_confirm_plans_then_applies_state_and_soft_allocation_postconditions() {
    let instance = MockServer::start().await;
    mount_plan_lookup(&instance, "2", "Requested", "2026-08-20 12:00:00").await;
    mount_apply_transition(&instance, true).await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");
    let state = decision_enabled_state(&fixture);

    let plan_response = plan_confirm(&state).await;
    assert!(plan_response.error.is_none(), "{plan_response:?}");
    let plan = plan_response.result.expect("decision plan");
    assert_eq!(plan["preview"]["decision"], "confirm");
    assert_eq!(
        plan["preview"]["current_state"],
        json!({"value": "2", "label": "Requested"})
    );
    assert_eq!(
        plan["preview"]["expected_state"],
        json!({"value": "11", "label": "Confirmed"})
    );
    assert_eq!(
        plan["preview"]["expected_allocation"]["booking_type"],
        "Soft"
    );
    assert!(plan["confirmation_token"].is_string());
    assert!(plan["idempotency_key"].is_string());
    assert!(plan["concurrency_token"].is_object());

    let apply_response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "resource_plan_apply_decision".to_string(),
            params: apply_params(&plan),
            id: Some(json!(2)),
        },
        &state,
    )
    .await;

    assert!(apply_response.error.is_none(), "{apply_response:?}");
    let receipt = apply_response.result.expect("decision receipt");
    assert_eq!(receipt["status"], "success");
    assert_eq!(
        receipt["record_snapshot"]["decision_evidence"]["verified"],
        true
    );
    assert_eq!(
        receipt["record_snapshot"]["decision_evidence"]["observed_state"],
        json!({"value": "11", "label": "Confirmed"})
    );
    assert_eq!(
        receipt["record_snapshot"]["decision_evidence"]["allocation_count"],
        1
    );
    assert_eq!(
        receipt["record_snapshot"]["decision_evidence"]["matching_allocation_count"],
        1
    );
    assert_eq!(
        receipt["record_snapshot"]["decision_evidence"]["booking_types"],
        json!(["Soft"])
    );
}

#[tokio::test(flavor = "current_thread")]
async fn daemon_rpc_confirm_refuses_a_non_requested_resource_plan_without_writing() {
    let instance = MockServer::start().await;
    mount_plan_lookup(&instance, "11", "Confirmed", "2026-08-20 12:01:00").await;
    Mock::given(method("PATCH"))
        .and(path(format!("/api/now/table/resource_plan/{PLAN_SYS_ID}")))
        .respond_with(ResponseTemplate::new(200))
        .expect(0)
        .mount(&instance)
        .await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");
    let state = decision_enabled_state(&fixture);

    let response = plan_confirm(&state).await;

    let error = response.error.expect("non-Requested plan must be refused");
    assert_eq!(error.code, -32050);
    assert_eq!(error.message, "GUARD_FAILED");
    assert_eq!(
        error.data.expect("guard data")["reason"],
        "decision_requires_requested"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn daemon_rpc_keeps_confirm_and_allocate_distinct_from_confirm() {
    let instance = MockServer::start().await;
    mount_plan_lookup(&instance, "2", "Requested", "2026-08-20 12:00:00").await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");
    let state = decision_enabled_state(&fixture);

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "resource_plan_plan_decision".to_string(),
            params: json!({
                "number": PLAN_NUMBER,
                "decision": "confirm_and_allocate"
            }),
            id: Some(json!(1)),
        },
        &state,
    )
    .await;

    assert!(response.error.is_none(), "{response:?}");
    let plan = response.result.expect("decision plan");
    assert_eq!(plan["preview"]["decision"], "confirm_and_allocate");
    assert_eq!(
        plan["preview"]["expected_state"],
        json!({"value": "3", "label": "Allocated"})
    );
    assert_eq!(
        plan["preview"]["expected_allocation"]["booking_type"],
        "Hard"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn daemon_rpc_decision_rejects_raw_state_injection_before_service_now_io() {
    let fixture = build_fixture_state_at_instance("http://127.0.0.1:9")
        .await
        .expect("fixture");
    let state = decision_enabled_state(&fixture);

    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "resource_plan_plan_decision".to_string(),
            params: json!({
                "number": PLAN_NUMBER,
                "decision": "confirm",
                "state": "3"
            }),
            id: Some(json!(1)),
        },
        &state,
    )
    .await;

    let error = response
        .error
        .expect("raw state injection must be rejected");
    assert_eq!(error.code, -32051);
    assert_eq!(error.message, "FIELD_REJECTED");
    assert_eq!(
        error.data.expect("field data")["fields"][0]["field"],
        "state"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn daemon_rpc_confirm_reports_partial_when_soft_allocations_are_missing() {
    let instance = MockServer::start().await;
    mount_plan_lookup(&instance, "2", "Requested", "2026-08-20 12:00:00").await;
    mount_apply_transition(&instance, false).await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");
    let state = decision_enabled_state(&fixture);

    let plan = plan_confirm(&state).await.result.expect("decision plan");
    let apply_response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "resource_plan_apply_decision".to_string(),
            params: apply_params(&plan),
            id: Some(json!(2)),
        },
        &state,
    )
    .await;

    assert!(apply_response.error.is_none(), "{apply_response:?}");
    let receipt = apply_response.result.expect("partial decision receipt");
    assert_eq!(receipt["status"], "partial");
    assert_eq!(receipt["error_code"], "DECISION_POSTCONDITION_INCOMPLETE");
    assert_eq!(
        receipt["record_snapshot"]["decision_evidence"]["verified"],
        false
    );
    assert_eq!(
        receipt["record_snapshot"]["decision_evidence"]["allocation_count"],
        0
    );
}
