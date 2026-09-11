//! L0: real daemon dispatch, real HTTP client, provider-only fake.
use super::{JsonRpcRequest, dispatch};
use crate::test_support::build_fixture_state_with_ui_metadata;
use serde_json::{Value, json};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path},
};

const ID: &str = "abcdef0123456789abcdef0123456789";

#[tokio::test(flavor = "current_thread")]
async fn record_resolution_does_not_claim_parent_projection_is_a_resolved_class() {
    let instance = MockServer::start().await;
    mount(
        &instance,
        &format!("/api/now/table/task/{ID}"),
        json!({"sys_id":ID,"short_description":"Class field hidden"}),
    )
    .await;
    let fixture = build_fixture_state_with_ui_metadata(&instance.uri())
        .await
        .unwrap();
    let response = resolve(&fixture.state, json!({"sys_id":ID})).await;
    assert_eq!(
        response
            .error
            .expect("parent projection is not type proof")
            .code,
        -32060
    );
}

async fn resolve(
    state: &std::sync::Arc<crate::DaemonState>,
    params: Value,
) -> super::JsonRpcResponse {
    dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".into(),
            method: "record_resolve".into(),
            params,
            id: Some(json!(1)),
        },
        state,
    )
    .await
}

async fn mount(instance: &MockServer, endpoint: &str, result: Value) {
    Mock::given(method("GET"))
        .and(path(endpoint))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result":result})))
        .mount(instance)
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn record_resolution_discovers_story_class_and_refetches_leaf_model() {
    let instance = MockServer::start().await;
    mount(
        &instance,
        &format!("/api/now/table/task/{ID}"),
        json!({"sys_id":ID,"sys_class_name":{"value":"rm_story","display_value":"Story"}}),
    )
    .await;
    mount(&instance, &format!("/api/now/table/rm_story/{ID}"), json!({"sys_id":ID,"sys_class_name":"rm_story","number":"STRY0000001","u_estimate":{"value":13,"display_value":"13 points"}})).await;
    mount(
        &instance,
        "/api/now/ui/meta/rm_story",
        json!({"columns":{"u_estimate":{"name":"u_estimate","type":"integer","label":"Estimate"}}}),
    )
    .await;
    let fixture = build_fixture_state_with_ui_metadata(&instance.uri())
        .await
        .unwrap();
    let response = resolve(&fixture.state, json!({"sys_id": ID.to_uppercase()})).await;
    assert!(response.error.is_none(), "{response:?}");
    let result = response.result.unwrap();
    assert_eq!(result["status"], "resolved");
    assert_eq!(result["record"]["table"], "rm_story");
    assert_eq!(result["record"]["resource_type"], "story");
    assert_eq!(result["record"]["fields"]["u_estimate"]["value"], 13);
    assert_eq!(
        result["record"]["fields"]["u_estimate"]["display_value"],
        "13 points"
    );
    assert_eq!(
        result["record"]["data_model"]["columns"]["u_estimate"]["type"],
        "integer"
    );
    assert!(
        instance
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.method == "GET")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn record_resolution_discovers_custom_catalog_table_without_a_builtin_type() {
    let instance = MockServer::start().await;
    mount(
        &instance,
        "/api/now/table/sys_db_object",
        json!([{"sys_id":"00000000000000000000000000000001","name":"x_example_widget"}]),
    )
    .await;
    mount(&instance, &format!("/api/now/table/x_example_widget/{ID}"), json!({"sys_id":ID,"u_payload":{"value":{"enabled":true,"count":7},"display_value":"Active"}})).await;
    mount(
        &instance,
        "/api/now/ui/meta/x_example_widget",
        json!({"columns":{"u_payload":{"name":"u_payload","type":"json"}}}),
    )
    .await;
    let fixture = build_fixture_state_with_ui_metadata(&instance.uri())
        .await
        .unwrap();
    let response = resolve(&fixture.state, json!({"sys_id":ID})).await;
    assert!(response.error.is_none(), "{response:?}");
    let record = &response.result.unwrap()["record"];
    assert_eq!(record["table"], "x_example_widget");
    assert_eq!(record["resource_type"], "dynamic");
    assert_eq!(
        record["fields"]["u_payload"]["value"],
        json!({"enabled":true,"count":7})
    );
    assert_eq!(record["data_model"]["status"], "available");
}

#[tokio::test(flavor = "current_thread")]
async fn record_resolution_rejects_mixed_or_malformed_selectors_without_http() {
    let instance = MockServer::start().await;
    let fixture = build_fixture_state_with_ui_metadata(&instance.uri())
        .await
        .unwrap();
    for params in [
        json!({"sys_id":"invalid"}),
        json!({"sys_id":ID,"table":"task"}),
        json!({"sys_id":ID,"number":"STRY0000001"}),
        json!({"sys_id":ID,"cursor":"unknown"}),
    ] {
        let response = resolve(&fixture.state, params).await;
        assert_eq!(response.error.expect("invalid selector").code, -32602);
    }
    assert!(instance.received_requests().await.unwrap().is_empty());
}

async fn discovery_instance() -> MockServer {
    let instance = MockServer::start().await;
    Mock::given(method("GET")).respond_with(|request: &wiremock::Request| {
        let endpoint = request.url.path();
        if endpoint == "/api/now/table/sys_db_object" {
            let query = request.url.query_pairs().find(|(key, _)| key == "sysparm_query").map(|(_, value)| value.into_owned()).unwrap_or_default();
            let rows: Vec<Value> = if query.contains("name>") {
                vec![json!({"sys_id":"catalog-widget","name":"x_example_widget"})]
            } else {
                (0..32).map(|index| json!({"sys_id":format!("catalog-{index}"),"name":format!("u_example_{index:02}")})).collect()
            };
            return ResponseTemplate::new(200).set_body_json(json!({"result":rows}));
        }
        let payload = if endpoint == format!("/api/now/table/task/{ID}") {
            Some(json!({"sys_id":ID,"sys_class_name":"rm_story"}))
        } else if endpoint == format!("/api/now/table/rm_story/{ID}") {
            Some(json!({"sys_id":ID,"sys_class_name":"rm_story","number":"STRY0000001","u_estimate":{"value":13,"display_value":"13 points"}}))
        } else if endpoint == "/api/now/table/sys_user/11111111111111111111111111111111" {
            Some(json!({"sys_id":"11111111111111111111111111111111","user_name":"example.user"}))
        } else if endpoint == "/api/now/table/x_example_widget/22222222222222222222222222222222" {
            Some(json!({"sys_id":"22222222222222222222222222222222","u_payload":"é🍀".repeat(32000)}))
        } else if endpoint.starts_with("/api/now/ui/meta/") {
            Some(json!({"columns":{"u_estimate":{"name":"u_estimate","type":"integer"},"user_name":{"name":"user_name","type":"string"},"u_payload":{"name":"u_payload","type":"string"}}}))
        } else { None };
        payload.map_or(ResponseTemplate::new(404), |result| ResponseTemplate::new(200).set_body_json(json!({"result":result})))
    }).mount(&instance).await;
    instance
}

#[tokio::test(flavor = "current_thread")]
async fn record_resolution_continues_with_bound_cursor_and_returns_truthful_exhaustion() {
    let instance = discovery_instance().await;
    let fixture = build_fixture_state_with_ui_metadata(&instance.uri())
        .await
        .unwrap();
    let missing = "33333333333333333333333333333333";
    let first = resolve(&fixture.state, json!({"sys_id":missing}))
        .await
        .result
        .unwrap();
    assert_eq!(first["status"], "searching");
    assert_eq!(first["scanned_tables"], 32);
    let cursor = first["cursor"].as_str().expect("opaque continuation");
    let count = instance.received_requests().await.unwrap().len();
    let wrong = resolve(&fixture.state, json!({"sys_id":ID,"cursor":cursor})).await;
    assert_eq!(wrong.error.unwrap().code, -32602);
    assert_eq!(instance.received_requests().await.unwrap().len(), count);
    let terminal = resolve(&fixture.state, json!({"sys_id":missing,"cursor":cursor}))
        .await
        .result
        .unwrap();
    assert_eq!(terminal["status"], "not_found_or_inaccessible");
    assert_eq!(terminal["scanned_tables"], 41);
    let consumed = resolve(&fixture.state, json!({"sys_id":missing,"cursor":cursor})).await;
    assert_eq!(consumed.error.unwrap().code, -32602);
}

#[tokio::test(flavor = "current_thread")]
async fn record_resolution_fails_closed_on_wrong_identity_unsafe_class_and_conflicting_classes() {
    for (task, cmdb) in [
        (
            json!({"sys_id":"ffffffffffffffffffffffffffffffff","sys_class_name":"task"}),
            None,
        ),
        (json!({"sys_id":ID,"sys_class_name":"../incident"}), None),
        (
            json!({"sys_id":ID,"sys_class_name":"rm_story"}),
            Some(json!({"sys_id":ID,"sys_class_name":"cmdb_ci"})),
        ),
    ] {
        let instance = MockServer::start().await;
        mount(&instance, &format!("/api/now/table/task/{ID}"), task).await;
        if let Some(cmdb) = cmdb {
            mount(&instance, &format!("/api/now/table/cmdb_ci/{ID}"), cmdb).await;
        }
        let fixture = build_fixture_state_with_ui_metadata(&instance.uri())
            .await
            .unwrap();
        assert_eq!(
            resolve(&fixture.state, json!({"sys_id":ID}))
                .await
                .error
                .expect("integrity failure")
                .code,
            -32061
        );
    }
}

#[tokio::test(flavor = "current_thread")]
async fn record_resolution_retains_readable_data_when_metadata_is_denied_but_blocks_denied_catalog()
{
    let instance = MockServer::start().await;
    mount(
        &instance,
        &format!("/api/now/table/sys_user/{ID}"),
        json!({"sys_id":ID,"user_name":"example.user"}),
    )
    .await;
    Mock::given(method("GET"))
        .and(path("/api/now/ui/meta/sys_user"))
        .respond_with(ResponseTemplate::new(403))
        .mount(&instance)
        .await;
    let fixture = build_fixture_state_with_ui_metadata(&instance.uri())
        .await
        .unwrap();
    let result = resolve(&fixture.state, json!({"sys_id":ID}))
        .await
        .result
        .unwrap();
    assert_eq!(
        result["record"]["fields"]["user_name"]["value"],
        "example.user"
    );
    assert_eq!(result["record"]["data_model"]["status"], "unavailable");
    instance.reset().await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_db_object"))
        .respond_with(ResponseTemplate::new(403))
        .mount(&instance)
        .await;
    assert_eq!(
        resolve(&fixture.state, json!({"sys_id":ID}))
            .await
            .error
            .expect("catalog unavailable, not absent")
            .code,
        -32060
    );
}

#[tokio::test(flavor = "current_thread")]
async fn record_resolution_daemon_direct_and_bridge_have_identical_models() {
    use snow_mcp::{DaemonBackedMcpBridge, JsonRpcRequest as McpRequest, McpServer};
    let instance = discovery_instance().await;
    let fixture = build_fixture_state_with_ui_metadata(&instance.uri())
        .await
        .unwrap();
    let request = || McpRequest {
        jsonrpc: "2.0".into(),
        method: "tools/call".into(),
        params: json!({"name":"record_resolve","arguments":{"sys_id":ID}}),
        id: Some(json!(1)),
    };
    let daemon = resolve(&fixture.state, json!({"sys_id":ID}))
        .await
        .result
        .unwrap();
    let direct = McpServer::new(std::sync::Arc::clone(&fixture.state.core))
        .dispatch(request())
        .await;
    assert_eq!(direct.result.as_ref(), Some(&daemon));
    let socket = fixture.tempdir.path().join("resolver.sock");
    let server = super::JsonRpcServer::new(
        std::sync::Arc::clone(&fixture.state),
        snow_core::ipc::IpcEndpoint::filesystem(&socket),
    );
    tokio::task::LocalSet::new()
        .run_until(async {
            let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
            let task = tokio::task::spawn_local(server.serve_until(async {
                let _ = stopped.await;
                Ok(())
            }));
            let result = tokio::time::timeout(std::time::Duration::from_secs(15), async {
                while !socket.exists() {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                DaemonBackedMcpBridge::from_socket(socket)
                    .dispatch(request())
                    .await
            })
            .await;
            Mock::given(method("GET")).and(path(format!("/api/now/table/rm_story/{ID}")))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result":{"sys_id":ID,"sys_class_name":"rm_story","u_quoted":"\"".repeat(26000)}})))
                .with_priority(1).mount(&instance).await;
            let escaped = snow_mcp::DaemonBackedMcpBridge::from_socket(fixture.tempdir.path().join("resolver.sock"))
                .dispatch(request()).await;
            let _ = stop.send(());
            task.await.unwrap().unwrap();
            let bridge = result.unwrap().result.unwrap();
            assert_eq!(bridge["structuredContent"], daemon);
            assert!(escaped.error.is_none(), "{escaped:?}");
            assert!(serde_json::to_vec(&escaped).unwrap().len() <= snow_mcp::protocol::frame::MAX_JSON_RPC_RESPONSE_BYTES,
                "bridge duplicates structured/text content; escaped fields must still fit its stdio frame");
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "cross-repository release gate; run Mullet test:servicenow-e2e"]
async fn daemon_identifier_end_to_end_via_mullet_stdio() {
    let instance = discovery_instance().await;
    let fixture = build_fixture_state_with_ui_metadata(&instance.uri())
        .await
        .unwrap();
    let socket = fixture.tempdir.path().join("resolver-e2e.sock");
    let server = super::JsonRpcServer::new(
        std::sync::Arc::clone(&fixture.state),
        snow_core::ipc::IpcEndpoint::filesystem(&socket),
    );
    tokio::task::LocalSet::new()
        .run_until(async {
            let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
            let task = tokio::task::spawn_local(server.serve_until(async {
                let _ = stopped.await;
                Ok(())
            }));
            let output = tokio::time::timeout(std::time::Duration::from_secs(45), async {
                while !socket.exists() {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
                tokio::process::Command::new(
                    std::env::var("MULLET_STORY_TEST_NODE").expect("Node executable"),
                )
                .args(["--import", "tsx", "tests/helpers/sys-id-mcp-driver.ts"])
                .arg(&socket)
                .current_dir(std::env::var("MULLET_STORY_TEST_ROOT").expect("Mullet checkout"))
                .kill_on_drop(true)
                .output()
                .await
            })
            .await;
            let _ = stop.send(());
            task.await.unwrap().unwrap();
            let output = output.expect("E2E deadline").expect("MCP process");
            println!("{}", String::from_utf8_lossy(&output.stdout));
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(
                instance
                    .received_requests()
                    .await
                    .unwrap()
                    .iter()
                    .all(|request| request.method == "GET")
            );
        })
        .await;
}
