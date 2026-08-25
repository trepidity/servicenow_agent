//! T-OPS-03 L0/L1 seams: daemon JSON-RPC, direct MCP, and real-socket bridge.

use std::sync::Arc;

use serde_json::{Value, json};
use snow_mcp::{DaemonBackedMcpBridge, JsonRpcRequest as McpRequest, McpServer};
use tokio::sync::oneshot;
use tokio::task::LocalSet;
use wiremock::matchers::{method, path, query_param_contains};
use wiremock::{Mock, MockServer, ResponseTemplate};

use crate::DaemonState;
use crate::rpc::{JsonRpcRequest, dispatch};
use crate::test_support::{build_fixture_state, build_fixture_state_at_instance, socket_path};

const FIRST: &str = "00000000000000000000000000000001";
const SECOND: &str = "00000000000000000000000000000002";

fn row(sys_id: &str, number: &str) -> Value {
    json!({
        "sys_id": { "value": sys_id, "display_value": sys_id },
        "number": { "value": number, "display_value": number },
        "short_description": { "value": "Native summary", "display_value": "Native summary" },
        "state": { "value": "2", "display_value": "In Progress" },
        "active": { "value": "true", "display_value": "true" },
        "priority": { "value": "3", "display_value": "3 - Moderate" }
    })
}

async fn rpc(
    state: &Arc<DaemonState>,
    operation: &str,
    params: Value,
) -> crate::rpc::JsonRpcResponse {
    dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: operation.to_string(),
            params,
            id: Some(json!(1)),
        },
        state,
    )
    .await
}

async fn direct(
    core: Arc<snow_core::SnowCore>,
    operation: &str,
    arguments: Value,
) -> snow_mcp::JsonRpcResponse {
    McpServer::new(core)
        .dispatch(McpRequest {
            jsonrpc: "2.0".to_string(),
            method: "tools/call".to_string(),
            params: json!({ "name": operation, "arguments": arguments }),
            id: Some(json!(1)),
        })
        .await
}

#[tokio::test(flavor = "multi_thread")]
async fn incident_get_is_byte_identical_for_both_selectors_and_direct_transports() {
    let instance = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/api/now/table/incident/{FIRST}")))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({ "result": row(FIRST, "INC0000001") })),
        )
        .mount(&instance)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/incident"))
        .and(query_param_contains("sysparm_query", "number=INC0000001"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({ "result": [row(FIRST, "INC0000001")] })),
        )
        .mount(&instance)
        .await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");
    let expected = json!({
        "operation": "incident_get",
        "source": { "kind": "live" },
        "completeness": { "kind": "complete" },
        "data": { "record": row(FIRST, "INC0000001") }
    });
    // The fake's field objects are exactly the public FieldValue wire shape.
    for selector in [
        json!({ "sys_id": FIRST }),
        json!({ "number": "inc0000001" }),
    ] {
        let daemon = rpc(&fixture.state, "incident_get", selector.clone()).await;
        let direct = direct(Arc::clone(&fixture.state.core), "incident_get", selector).await;
        assert_eq!(daemon.result.as_ref(), Some(&expected));
        assert_eq!(direct.result.as_ref(), Some(&expected));
        assert_eq!(
            serde_json::to_vec(daemon.result.as_ref().expect("daemon envelope"))
                .expect("daemon bytes"),
            serde_json::to_vec(direct.result.as_ref().expect("direct envelope"))
                .expect("direct bytes")
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn incident_get_distinguishes_ambiguity_acl_uncertainty_not_found_and_acl_denial() {
    for (body, expected_code) in [
        (
            json!({ "result": [row(FIRST, "INC0000001"), row(SECOND, "INC0000001")] }),
            -32005,
        ),
        (json!({ "result": [] }), -32007),
    ] {
        let instance = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/incident"))
            .respond_with(ResponseTemplate::new(200).set_body_json(body))
            .mount(&instance)
            .await;
        let fixture = build_fixture_state_at_instance(&instance.uri())
            .await
            .expect("fixture");
        let response = rpc(
            &fixture.state,
            "incident_get",
            json!({ "number": "INC0000001" }),
        )
        .await;
        assert_eq!(
            response.error.expect("structured error").code,
            expected_code
        );
    }

    for (status, expected_code) in [(404, -32004), (403, -32003), (500, -32000)] {
        let instance = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path(format!("/api/now/table/incident/{FIRST}")))
            .respond_with(ResponseTemplate::new(status).set_body_json(json!({
                "error": { "message": "redacted", "detail": "" }
            })))
            .mount(&instance)
            .await;
        let fixture = build_fixture_state_at_instance(&instance.uri())
            .await
            .expect("fixture");
        let response = rpc(&fixture.state, "incident_get", json!({ "sys_id": FIRST })).await;
        assert_eq!(
            response.error.expect("structured error").code,
            expected_code
        );
    }

    let fixture = build_fixture_state_at_instance("http://127.0.0.1:9")
        .await
        .expect("unreachable fixture");
    let response = rpc(&fixture.state, "incident_get", json!({ "sys_id": FIRST })).await;
    assert_eq!(response.error.expect("network error").code, -32001);
}

#[tokio::test(flavor = "multi_thread")]
async fn incident_query_uses_fixed_projection_and_truthful_terminal_paging() {
    let instance = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/incident"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [row(FIRST, "INC0000001"), row(SECOND, "INC0000002")]
        })))
        .expect(2)
        .mount(&instance)
        .await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");
    let params = json!({ "limit": 2 });
    let daemon = rpc(&fixture.state, "incident_query", params.clone()).await;
    let direct = direct(Arc::clone(&fixture.state.core), "incident_query", params).await;
    assert_eq!(
        serde_json::to_vec(daemon.result.as_ref().expect("daemon envelope")).expect("daemon bytes"),
        serde_json::to_vec(direct.result.as_ref().expect("direct envelope")).expect("direct bytes")
    );
    let result = daemon.result.expect("query result");
    assert_eq!(
        result["completeness"],
        json!({ "kind": "partial", "reason": "page_limit_reached" })
    );
    assert_eq!(result["data"]["next_cursor"], SECOND);
    assert_eq!(result["data"]["rows_inspected"], 2);

    let request = instance.received_requests().await.expect("requests")[0].clone();
    let query = request.url.query().unwrap_or_default();
    assert!(
        query.contains("ORDERBYsys_id"),
        "fixed ascending sys_id order: {query}"
    );
    let fields = request
        .url
        .query_pairs()
        .find_map(|(key, value)| (key == "sysparm_fields").then(|| value.into_owned()))
        .expect("fixed projection");
    assert_eq!(
        fields,
        "sys_id,number,short_description,state,active,priority,impact,urgency,opened_at,resolved_at,closed_at,caller_id,assigned_to,assignment_group,cmdb_ci,business_service,category,subcategory,sys_created_on,sys_updated_on,sys_updated_by"
    );
    assert!(
        !fields
            .split(',')
            .any(|field| matches!(field, "description" | "work_notes" | "comments"))
    );

    instance.reset().await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/incident"))
        .and(query_param_contains(
            "sysparm_query",
            format!("sys_id>{SECOND}"),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "result": [] })))
        .expect(1)
        .mount(&instance)
        .await;
    let terminal = rpc(
        &fixture.state,
        "incident_query",
        json!({ "limit": 2, "cursor": SECOND }),
    )
    .await
    .result
    .expect("terminal page");
    assert_eq!(terminal["completeness"], json!({ "kind": "complete" }));
    assert_eq!(terminal["data"]["records"], json!([]));
    assert_eq!(terminal["data"]["next_cursor"], Value::Null);
}

#[tokio::test(flavor = "multi_thread")]
async fn incident_query_rejects_invalid_input_and_unresolved_state_before_incident_io() {
    let fixture = build_fixture_state().await.expect("fixture");
    for params in [
        json!({ "limit": 201 }),
        json!({ "filters": { "numbers": ["INC1", "inc1"] } }),
        json!({ "filters": { "states": [] } }),
        json!({ "filters": { "opened_after": "2026-02-30 00:00:00" } }),
        json!({ "sort": "number" }),
    ] {
        let response = rpc(&fixture.state, "incident_query", params).await;
        assert_eq!(response.error.expect("invalid params").code, -32602);
    }
    for params in [json!({}), json!({ "number": "INC1", "sys_id": FIRST })] {
        let response = rpc(&fixture.state, "incident_get", params).await;
        assert_eq!(
            response.error.expect("exclusive selector rejection").code,
            -32602
        );
    }
    drop(fixture);

    let instance = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_choice"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{ "value": "1", "label": "New", "sequence": "1", "inactive": "false", "terminal": "false" }]
        })))
        .mount(&instance)
        .await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");
    let response = rpc(
        &fixture.state,
        "incident_query",
        json!({ "filters": { "states": ["Awaiting Vendor"] } }),
    )
    .await;
    let error = response.error.expect("state correction");
    assert_eq!(error.code, -32602);
    assert_eq!(
        error.data.as_ref().and_then(|data| data["code"].as_str()),
        Some("INCIDENT_STATE_UNRESOLVED")
    );
    assert!(
        instance
            .received_requests()
            .await
            .expect("requests")
            .iter()
            .all(|request| request.url.path() != "/api/now/table/incident")
    );
    drop(fixture);
    instance.reset().await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_choice"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "result": [] })))
        .mount(&instance)
        .await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");
    let unavailable = rpc(
        &fixture.state,
        "incident_query",
        json!({ "filters": { "states": ["1"] } }),
    )
    .await
    .error
    .expect("empty choice set fails closed");
    assert_eq!(unavailable.code, -32602);
    assert_eq!(
        unavailable.data.as_ref().map(|data| &data["unavailable"]),
        Some(&json!(true))
    );
}

#[tokio::test(flavor = "current_thread")]
async fn incident_get_bridge_calls_the_real_daemon_socket_without_envelope_drift() {
    let instance = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/api/now/table/incident/{FIRST}")))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({ "result": row(FIRST, "INC0000001") })),
        )
        .mount(&instance)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/incident"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "result": [] })))
        .mount(&instance)
        .await;
    let fixture = build_fixture_state_at_instance(&instance.uri())
        .await
        .expect("fixture");
    let socket = socket_path(&fixture.tempdir);
    let server = crate::rpc::JsonRpcServer::new(
        Arc::clone(&fixture.state),
        snow_core::ipc::IpcEndpoint::from_socket_path(&socket),
    );
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
            let response = DaemonBackedMcpBridge::from_socket(socket.clone())
                .dispatch(McpRequest {
                    jsonrpc: "2.0".to_string(),
                    method: "tools/call".to_string(),
                    params: json!({ "name": "incident_get", "arguments": { "sys_id": FIRST } }),
                    id: Some(json!(1)),
                })
                .await;
            let result = response.result.expect("bridge result");
            assert_eq!(result["structuredContent"]["operation"], "incident_get");
            assert_eq!(
                result["structuredContent"]["data"]["record"]["number"]["value"],
                "INC0000001"
            );
            let query = DaemonBackedMcpBridge::from_socket(socket.clone())
                .dispatch(McpRequest {
                    jsonrpc: "2.0".to_string(),
                    method: "tools/call".to_string(),
                    params: json!({ "name": "incident_query", "arguments": {} }),
                    id: Some(json!(2)),
                })
                .await
                .result
                .expect("bridge query result");
            assert_eq!(query["structuredContent"]["operation"], "incident_query");
            assert_eq!(query["structuredContent"]["data"]["records"], json!([]));
            assert_eq!(
                query["structuredContent"]["completeness"],
                json!({ "kind": "complete" })
            );
            let _ = shutdown_tx.send(());
        })
        .await;
}
