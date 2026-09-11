//! L0: unregistered types and prefixes through daemon, direct MCP and bridge.
use super::{JsonRpcRequest, dispatch};
use crate::test_support::build_fixture_state_with_ui_metadata;
use serde_json::{Value, json};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

const ID: &str = "11111111111111111111111111111111";
const TABLE: &str = "x_example_planning_note";

async fn instance() -> MockServer {
    let instance = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_number"))
        .and(query_param("sysparm_query", "prefix=XPN"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"result":[{"prefix":"XPN","category":TABLE}]})),
        )
        .mount(&instance)
        .await;
    Mock::given(method("GET")).and(path("/api/now/table/sys_db_object"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result":[{"sys_id":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","name":TABLE,"label":"Planning Note"}]}))).mount(&instance).await;
    Mock::given(method("GET")).and(path(format!("/api/now/table/{TABLE}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result":[{"sys_id":ID,"number":"XPN0000001","short_description":"Example plan","sys_class_name":TABLE}]}))).mount(&instance).await;
    Mock::given(method("GET")).and(path(format!("/api/now/table/{TABLE}/{ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result":{"sys_id":ID,"number":"XPN0000001","short_description":"Example plan","sys_class_name":TABLE}}))).mount(&instance).await;
    Mock::given(method("GET")).and(path(format!("/api/now/ui/meta/{TABLE}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result":{"columns":{"number":{"type":"string"},"short_description":{"type":"string"}}}}))).mount(&instance).await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_dictionary"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"result":[{"element":"short_description"}]})),
        )
        .mount(&instance)
        .await;
    instance
}

async fn rpc(
    state: &std::sync::Arc<crate::DaemonState>,
    method: &str,
    args: Value,
) -> super::JsonRpcResponse {
    dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".into(),
            method: method.into(),
            params: args,
            id: Some(json!(1)),
        },
        state,
    )
    .await
}

#[tokio::test(flavor = "current_thread")]
async fn prefix_label_and_custom_table_resolve_without_registered_surfaces() {
    use snow_mcp::{DaemonBackedMcpBridge, JsonRpcRequest as McpRequest, McpServer};
    let instance = instance().await;
    let fixture = build_fixture_state_with_ui_metadata(&instance.uri())
        .await
        .unwrap();
    let socket = fixture.tempdir.path().join("types.sock");
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
            tokio::time::timeout(std::time::Duration::from_secs(10), async {
                while !socket.exists() {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            for selector in ["XPN", "Planning Note", TABLE] {
                let args = json!({"resource_type":selector});
                let result = rpc(&fixture.state, "record_resolve", args.clone()).await;
                assert!(result.error.is_none(), "{result:?}");
                let result = result.result.unwrap();
                assert_eq!(result["status"], "type_matches");
                assert_eq!(result["types"][0]["table"], TABLE);
                assert_eq!(result["types"][0]["label"], "Planning Note");
                let request = || McpRequest {
                    jsonrpc: "2.0".into(),
                    method: "tools/call".into(),
                    params: json!({"name":"record_resolve","arguments":args}),
                    id: Some(json!(1)),
                };
                assert_eq!(
                    McpServer::new(std::sync::Arc::clone(&fixture.state.core))
                        .dispatch(request())
                        .await
                        .result,
                    Some(result.clone())
                );
                assert_eq!(
                    DaemonBackedMcpBridge::from_socket(socket.clone())
                        .dispatch(request())
                        .await
                        .result
                        .unwrap()["structuredContent"],
                    result
                );
            }
            for args in [
                json!({"number":"XPN0000001"}),
                json!({"table":TABLE,"sys_id":ID}),
            ] {
                let result = rpc(&fixture.state, "get_record", args.clone()).await;
                assert!(result.error.is_none(), "{result:?}");
                assert_eq!(result.result.unwrap()["record"]["sys_id"], ID);
                let request = || McpRequest {
                    jsonrpc: "2.0".into(),
                    method: "tools/call".into(),
                    params: json!({"name":"get_record","arguments":args}),
                    id: Some(json!(1)),
                };
                assert_eq!(
                    McpServer::new(std::sync::Arc::clone(&fixture.state.core))
                        .dispatch(request())
                        .await
                        .result
                        .unwrap()["record"]["sys_id"],
                    ID
                );
                assert_eq!(
                    DaemonBackedMcpBridge::from_socket(socket.clone())
                        .dispatch(request())
                        .await
                        .result
                        .unwrap()["structuredContent"]["record"]["sys_id"],
                    ID
                );
            }
            let result = rpc(
                &fixture.state,
                "record_resolve",
                json!({"number":"XPN0000001"}),
            )
            .await;
            assert!(result.error.is_none(), "{result:?}");
            assert_eq!(result.result.unwrap()["record"]["table"], TABLE);
            let result = rpc(
                &fixture.state,
                "record_resolve",
                json!({"resource_type":"Planning Note","name":"Example plan"}),
            )
            .await;
            assert!(result.error.is_none(), "{result:?}");
            assert_eq!(result.result.unwrap()["records"][0]["sys_id"], ID);
            let _ = stop.send(());
            task.await.unwrap().unwrap();
        })
        .await;
    assert!(
        instance
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|r| r.method == "GET")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn type_discovery_retains_duplicate_labels_and_rejects_unrelated_prefix_metadata() {
    let instance = MockServer::start().await;
    Mock::given(method("GET")).and(path("/api/now/table/sys_db_object"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result":[{"name":"x_example_one","label":"Planning Note"},{"name":"x_example_two","label":"Planning Note"}]}))).mount(&instance).await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_number"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"result":[{"prefix":"OTHER","category":TABLE}]})),
        )
        .mount(&instance)
        .await;
    let fixture = build_fixture_state_with_ui_metadata(&instance.uri())
        .await
        .unwrap();
    let result = rpc(
        &fixture.state,
        "record_resolve",
        json!({"resource_type":"Planning Note"}),
    )
    .await
    .result
    .unwrap();
    assert_eq!(result["types"].as_array().unwrap().len(), 2);
    assert_eq!(
        rpc(
            &fixture.state,
            "record_resolve",
            json!({"resource_type":"Planning Note","name":"Example plan"})
        )
        .await
        .error
        .unwrap()
        .code,
        -32602
    );
    assert_eq!(
        rpc(
            &fixture.state,
            "record_resolve",
            json!({"number":"XPN0000001"})
        )
        .await
        .error
        .unwrap()
        .code,
        -32061
    );
    assert!(
        instance
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|r| r.url.path() == "/api/now/table/sys_db_object"
                || r.url.path() == "/api/now/table/sys_number")
    );
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "cross-repository release gate"]
async fn daemon_types_end_to_end_via_mullet_stdio() {
    let instance = instance().await;
    let fixture = build_fixture_state_with_ui_metadata(&instance.uri())
        .await
        .unwrap();
    let socket = fixture.tempdir.path().join("types-e2e.sock");
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
                tokio::process::Command::new(std::env::var("MULLET_STORY_TEST_NODE").unwrap())
                    .args(["--import", "tsx", "tests/helpers/types-mcp-driver.ts"])
                    .arg(&socket)
                    .current_dir(std::env::var("MULLET_STORY_TEST_ROOT").unwrap())
                    .kill_on_drop(true)
                    .output()
                    .await
            })
            .await;
            let _ = stop.send(());
            task.await.unwrap().unwrap();
            let output = output.unwrap().unwrap();
            println!("{}", String::from_utf8_lossy(&output.stdout));
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
        })
        .await;
    assert!(
        instance
            .received_requests()
            .await
            .unwrap()
            .iter()
            .all(|request| request.method == "GET")
    );
}
