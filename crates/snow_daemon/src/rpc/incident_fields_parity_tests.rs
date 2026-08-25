//! T-OPS-01 closure gate: transport parity for `incident_fields`.
//!
//! FND-OPS-001 requires that one selected resource prove *identical* discovered
//! metadata, native record body, source, and completeness through every
//! declared consumer transport. This drives the core API, the daemon JSON-RPC
//! dispatcher, and the direct MCP server against a single fake ServiceNow
//! instance and asserts the three produce the exact independently-authored
//! envelope JSON.
//!
//! Exact-envelope equality is the deliberate bar. Asserting field-by-field would let a
//! transport add, drop, or rename something no assertion happened to cover —
//! which is exactly how transports drift apart in practice.
//!
//! This lives in the crate's unit-test tree rather than `tests/` because both
//! `test_support` and the RPC `dispatch` entry point are crate-internal.
//! Widening production visibility to accommodate a test would be the wrong
//! trade: the seam under test is the consumer-visible envelope, not the module
//! boundary.

use std::sync::Arc;

use serde_json::{Value, json};
use snow_mcp::{DaemonBackedMcpBridge, JsonRpcRequest as McpRequest, McpServer};
use tokio::sync::oneshot;
use tokio::task::LocalSet;
use wiremock::matchers::{method, path};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::DaemonState;
use crate::rpc::{JsonRpcRequest, dispatch};
use crate::test_support::{build_fixture_state, build_fixture_state_at_instance, socket_path};

/// Stand up a fake instance answering the inheritance probe, the dictionary
/// read, and the choice read for the Incident table.
async fn mount_incident_metadata(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_db_object"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{ "name": "incident", "super_class": "" }]
        })))
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_dictionary"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [
                {
                    "name": "incident",
                    "element": "short_description",
                    "column_label": "Short description",
                    "internal_type": { "value": "string", "display_value": "String" },
                    "reference": "",
                    "choice": "0",
                    "read_only": "false",
                    "active": "true"
                },
                {
                    "name": "incident",
                    "element": "assignment_group",
                    "column_label": "Assignment group",
                    "internal_type": { "value": "reference", "display_value": "Reference" },
                    "reference": { "value": "sys_user_group", "display_value": "Group" },
                    "choice": "0",
                    "read_only": "false",
                    "active": "true"
                },
                {
                    "name": "incident",
                    "element": "state",
                    "column_label": "State",
                    "internal_type": { "value": "integer", "display_value": "Integer" },
                    "reference": "",
                    "choice": "1",
                    "read_only": "false",
                    "active": "true"
                }
            ]
        })))
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_choice"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [
                { "value": "1", "label": "New", "sequence": "1", "inactive": "false", "terminal": "false" },
                { "value": "7", "label": "Closed", "sequence": "2", "inactive": "false", "terminal": "true" }
            ]
        })))
        .mount(server)
        .await;
}

/// Call `incident_fields` over daemon JSON-RPC and return the result payload.
async fn via_daemon_rpc(state: &Arc<DaemonState>) -> Value {
    let response = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "incident_fields".to_string(),
            params: json!({}),
            id: Some(json!(1)),
        },
        state,
    )
    .await;
    assert!(
        response.error.is_none(),
        "daemon JSON-RPC must route incident_fields: {:?}",
        response.error
    );
    response.result.expect("daemon result")
}

/// Call `incident_fields` over direct MCP and return the result payload.
async fn via_direct_mcp(core: Arc<snow_core::SnowCore>) -> Value {
    let server = McpServer::new(core);
    let response = server
        .dispatch(McpRequest {
            jsonrpc: "2.0".to_string(),
            method: "tools/call".to_string(),
            params: json!({ "name": "incident_fields", "arguments": {} }),
            id: Some(json!(1)),
        })
        .await;
    assert!(
        response.error.is_none(),
        "direct MCP must route incident_fields: {:?}",
        response.error
    );
    response.result.expect("direct MCP result")
}

fn expected_incident_envelope() -> Value {
    let assignment_group = json!({
        "name": "assignment_group",
        "label": "Assignment group",
        "kind": "reference",
        "reference_table": "sys_user_group",
        "choices": { "status": "unavailable", "reason": "not_supported_by_operation" }
    });
    let short_description = json!({
        "name": "short_description",
        "label": "Short description",
        "kind": "string",
        "choices": { "status": "unavailable", "reason": "not_supported_by_operation" }
    });
    let state = json!({
        "name": "state",
        "label": "State",
        "kind": "integer",
        "choices": {
            "status": "available",
            "value": [
                { "label": "New", "value": "1", "terminal": false },
                { "label": "Closed", "value": "7", "terminal": true }
            ]
        }
    });
    json!({
        "operation": "incident_fields",
        "source": { "kind": "live" },
        "completeness": { "kind": "complete" },
        "data": {
            "resource_type": "Incident",
            "table": "incident",
            "readable_fields": {
                "status": "available",
                "value": [assignment_group.clone(), short_description.clone(), state.clone()]
            },
            "writable_fields": {
                "status": "available",
                "value": [assignment_group, short_description, state]
            },
            "paging": { "mode": "cursor", "default_limit": 50, "max_limit": 200 }
        }
    })
}

#[tokio::test(flavor = "multi_thread")]
async fn incident_fields_reports_identical_envelopes_across_transports() {
    let instance = MockServer::start().await;
    mount_incident_metadata(&instance).await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");

    let via_core = serde_json::to_value(
        fixture
            .state
            .core
            .incident_fields()
            .await
            .expect("core descriptor"),
    )
    .expect("serialize core envelope");
    let via_rpc = via_daemon_rpc(&fixture.state).await;
    let via_mcp = via_direct_mcp(Arc::clone(&fixture.state.core)).await;

    let expected = expected_incident_envelope();
    assert_eq!(
        via_core, expected,
        "core must match the independent contract"
    );
    assert_eq!(
        via_rpc, expected,
        "daemon must match the independent contract"
    );
    assert_eq!(
        via_mcp, expected,
        "direct MCP must match the independent contract"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn incident_fields_bridge_calls_the_real_daemon_transport_without_envelope_drift() {
    let instance = MockServer::start().await;
    mount_incident_metadata(&instance).await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");
    let socket = socket_path(&fixture.tempdir);
    let endpoint = snow_core::ipc::IpcEndpoint::from_socket_path(&socket);
    let server = crate::rpc::JsonRpcServer::new(Arc::clone(&fixture.state), endpoint);
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
            let bridge = DaemonBackedMcpBridge::from_socket(socket);
            let response = bridge
                .dispatch(McpRequest {
                    jsonrpc: "2.0".to_string(),
                    method: "tools/call".to_string(),
                    params: json!({ "name": "incident_fields", "arguments": {} }),
                    id: Some(json!(1)),
                })
                .await;
            let result = response.result.expect("bridge tool result");

            assert_eq!(
                result["structuredContent"],
                expected_incident_envelope(),
                "daemon-backed MCP must preserve the independently authored envelope"
            );
            let _ = shutdown_tx.send(());
        })
        .await;
}

#[tokio::test]
async fn incident_fields_rejects_unknown_arguments_before_servicenow_io() {
    let fixture = build_fixture_state().await.expect("fixture");
    let rpc = dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "incident_fields".to_string(),
            params: json!({ "table": "sys_user" }),
            id: Some(json!(1)),
        },
        &fixture.state,
    )
    .await;
    assert_eq!(rpc.error.expect("daemon invalid params").code, -32602);

    let direct = McpServer::new(Arc::clone(&fixture.state.core))
        .dispatch(McpRequest {
            jsonrpc: "2.0".to_string(),
            method: "tools/call".to_string(),
            params: json!({
                "name": "incident_fields",
                "arguments": { "table": "sys_user" }
            }),
            id: Some(json!(2)),
        })
        .await;
    assert_eq!(
        direct.error.expect("direct MCP invalid params").code,
        -32602
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn incident_fields_envelope_states_operation_source_and_completeness() {
    let instance = MockServer::start().await;
    mount_incident_metadata(&instance).await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");

    let envelope = via_daemon_rpc(&fixture.state).await;

    assert_eq!(envelope["operation"], "incident_fields");
    // Metadata is not a cache-eligible object, so a cached source here would
    // mean the operation silently consulted a cache it must never touch.
    assert_eq!(envelope["source"]["kind"], "live");
    assert_eq!(envelope["completeness"]["kind"], "complete");
    assert_eq!(envelope["data"]["table"], "incident");
}

#[tokio::test(flavor = "multi_thread")]
async fn incident_fields_preserves_native_servicenow_values_on_every_transport() {
    let instance = MockServer::start().await;
    mount_incident_metadata(&instance).await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");

    for (label, envelope) in [
        ("daemon", via_daemon_rpc(&fixture.state).await),
        ("mcp", via_direct_mcp(Arc::clone(&fixture.state.core)).await),
    ] {
        let readable = envelope["data"]["readable_fields"]["value"]
            .as_array()
            .unwrap_or_else(|| panic!("{label}: readable fields must be available"))
            .clone();

        let group = readable
            .iter()
            .find(|field| field["name"] == "assignment_group")
            .unwrap_or_else(|| panic!("{label}: assignment_group missing"));
        // Native ServiceNow naming and typing, not a Snow-side rewrite.
        assert_eq!(group["kind"], "reference", "{label}: native internal_type");
        assert_eq!(
            group["reference_table"], "sys_user_group",
            "{label}: native reference target"
        );

        let state = readable
            .iter()
            .find(|field| field["name"] == "state")
            .unwrap_or_else(|| panic!("{label}: state missing"));
        assert_eq!(
            state["choices"]["value"][1]["terminal"], true,
            "{label}: terminal choice flag survives the transport"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn incident_fields_distinguishes_acl_denial_from_an_empty_table() {
    let instance = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_db_object"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{ "name": "incident", "super_class": "" }]
        })))
        .mount(&instance)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_dictionary"))
        .respond_with(ResponseTemplate::new(403).set_body_json(json!({
            "error": { "message": "Forbidden", "detail": "" }
        })))
        .mount(&instance)
        .await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");

    let envelope = via_daemon_rpc(&fixture.state).await;

    assert_eq!(
        envelope["data"]["readable_fields"]["status"], "unavailable",
        "an ACL denial must never render as a discovered empty field list"
    );
    assert_eq!(envelope["data"]["readable_fields"]["reason"], "acl_denied");
}
