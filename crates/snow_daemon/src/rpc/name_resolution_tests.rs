//! L0: generic names through daemon, direct MCP and the real-socket MCP bridge.
use super::{JsonRpcRequest, dispatch};
use crate::test_support::build_fixture_state_with_ui_metadata;
use serde_json::{Value, json};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

const NAME: &str = "Example Name";
const ID: &str = "11111111111111111111111111111111";

async fn rpc(state: &std::sync::Arc<crate::DaemonState>, args: Value) -> super::JsonRpcResponse {
    dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".into(),
            method: "record_resolve".into(),
            params: args,
            id: Some(json!(1)),
        },
        state,
    )
    .await
}

async fn model(instance: &MockServer, table: &str, field: &str) {
    Mock::given(method("GET"))
        .and(path(format!("/api/now/ui/meta/{table}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            json!({"result":{"columns":{field:{"type":"string"},"sys_id":{"type":"GUID"}}}}),
        ))
        .mount(instance)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_dictionary"))
        .and(query_param(
            "sysparm_query",
            format!("name={table}^display=true"),
        ))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"result":[{"element":field}]})),
        )
        .mount(instance)
        .await;
}

async fn instance() -> MockServer {
    let instance = MockServer::start().await;
    for (table, field) in [
        ("rm_sprint", "short_description"),
        ("rm_release_scrum", "short_description"),
        ("sys_user_group", "name"),
        ("sys_user", "name"),
        ("x_example_widget", "u_caption"),
    ] {
        model(&instance, table, field).await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_db_object"))
            .and(move |request: &wiremock::Request| {
                request.url.query_pairs().any(|(key, value)| {
                    key == "sysparm_query"
                        && value
                            .split("^OR")
                            .any(|clause| clause == format!("name={table}"))
                })
            })
            .respond_with(
                ResponseTemplate::new(200)
                    .set_body_json(json!({"result":[{"name":table,"label":table}]})),
            )
            .mount(&instance)
            .await;

        Mock::given(method("GET"))
            .and(path(format!("/api/now/table/{table}")))
            .and(query_param(
                "sysparm_query",
                format!("{field}={NAME}^ORDERBYsys_id"),
            ))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(
                    json!({"result":[{"sys_id":ID,"sys_class_name":table,field:NAME}]}),
                ),
            )
            .mount(&instance)
            .await;
    }
    instance
}

#[tokio::test(flavor = "current_thread")]
async fn names_for_references_and_custom_display_fields_have_transport_parity() {
    use snow_mcp::{DaemonBackedMcpBridge, JsonRpcRequest as McpRequest, McpServer};
    let instance = instance().await;
    let fixture = build_fixture_state_with_ui_metadata(&instance.uri())
        .await
        .unwrap();
    let socket = fixture.tempdir.path().join("names.sock");
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
            for (table, kind) in [
                ("rm_sprint", "sprint"),
                ("rm_release_scrum", "release"),
                ("sys_user_group", "assignment_group"),
                ("sys_user", "user"),
                ("x_example_widget", "x_example_widget"),
            ] {
                let args = json!({"name":NAME,"table":table});
                let request = || McpRequest {
                    jsonrpc: "2.0".into(),
                    method: "tools/call".into(),
                    params: json!({"name":"record_resolve","arguments":args}),
                    id: Some(json!(1)),
                };
                let daemon = rpc(&fixture.state, args.clone()).await;
                assert!(daemon.error.is_none(), "{daemon:?}");
                let daemon = daemon.result.unwrap();
                assert_eq!(daemon["records"][0]["sys_id"], ID);
                assert_eq!(daemon["records"][0]["table"], table);
                assert_eq!(daemon["records"][0]["resource_type"], kind);
                assert_eq!(daemon["complete"], true);
                assert_eq!(daemon["match_count"], 1);
                let direct = McpServer::new(std::sync::Arc::clone(&fixture.state.core))
                    .dispatch(request())
                    .await;
                assert_eq!(direct.result.as_ref(), Some(&daemon));
                let bridge = DaemonBackedMcpBridge::from_socket(socket.clone())
                    .dispatch(request())
                    .await;
                assert_eq!(bridge.result.unwrap()["structuredContent"], daemon);
            }
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
            .all(|request| request.method == "GET")
    );
}

#[tokio::test(flavor = "current_thread")]
async fn name_candidates_page_without_skipping_and_cursors_bind_to_the_entire_selector() {
    let instance = MockServer::start().await;
    model(&instance, "sys_user_group", "name").await;
    let rows = (1..=20)
        .map(|id| json!({"sys_id":format!("{id:032x}"),"name":NAME}))
        .collect::<Vec<_>>();
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_user_group"))
        .and(query_param(
            "sysparm_query",
            format!("name={NAME}^ORDERBYsys_id"),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result":rows})))
        .mount(&instance)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_user_group"))
        .and(query_param(
            "sysparm_query",
            format!("name={NAME}^sys_id>00000000000000000000000000000014^ORDERBYsys_id"),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(
            json!({"result":[{"sys_id":"00000000000000000000000000000015","name":NAME}]}),
        ))
        .mount(&instance)
        .await;
    let fixture = build_fixture_state_with_ui_metadata(&instance.uri())
        .await
        .unwrap();
    let first = rpc(
        &fixture.state,
        json!({"name":NAME,"table":"sys_user_group"}),
    )
    .await
    .result
    .unwrap();
    assert_eq!(first["complete"], false);
    assert_eq!(first["records"].as_array().unwrap().len(), 20);
    for args in [
        json!({"name":"Different","table":"sys_user_group","cursor":first["cursor"]}),
        json!({"name":NAME,"table":"sys_user","cursor":first["cursor"]}),
        json!({"sys_id":ID,"cursor":first["cursor"]}),
    ] {
        assert_eq!(rpc(&fixture.state, args).await.error.unwrap().code, -32602);
    }
    for _ in 0..2 {
        let last = rpc(
            &fixture.state,
            json!({"name":NAME,"table":"sys_user_group","cursor":first["cursor"]}),
        )
        .await
        .result
        .unwrap();
        assert_eq!(
            last["records"][0]["sys_id"],
            "00000000000000000000000000000015"
        );
        assert_eq!(last["match_count"], 21);
        assert_eq!(last["complete"], true);
        assert_eq!(last["cursor"], Value::Null);
    }
}

#[tokio::test(flavor = "current_thread")]
async fn name_lookup_refuses_unfiltered_provider_results_and_preserves_acl_uncertainty() {
    let instance = MockServer::start().await;
    model(&instance, "sys_user_group", "name").await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_user_group"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"result":[{"sys_id":ID,"name":"Wrong name"}]})),
        )
        .mount(&instance)
        .await;
    let fixture = build_fixture_state_with_ui_metadata(&instance.uri())
        .await
        .unwrap();
    assert_eq!(
        rpc(
            &fixture.state,
            json!({"name":NAME,"table":"sys_user_group"})
        )
        .await
        .error
        .unwrap()
        .code,
        -32061
    );
    instance.reset().await;
    model(&instance, "sys_user_group", "name").await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_user_group"))
        .respond_with(ResponseTemplate::new(403))
        .mount(&instance)
        .await;
    assert_eq!(
        rpc(
            &fixture.state,
            json!({"name":NAME,"table":"sys_user_group"})
        )
        .await
        .error
        .unwrap()
        .code,
        -32060
    );
}

#[tokio::test(flavor = "current_thread")]
async fn name_alone_discovers_custom_tables_and_reports_unreadable_scopes() {
    let instance = MockServer::start().await;
    model(&instance, "x_example_widget", "u_caption").await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_db_object"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"result":[{"name":"x_example_widget"}]})),
        )
        .mount(&instance)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/x_example_widget"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"result":[{"sys_id":ID,"u_caption":NAME}]})),
        )
        .mount(&instance)
        .await;
    let fixture = build_fixture_state_with_ui_metadata(&instance.uri())
        .await
        .unwrap();
    let first = rpc(&fixture.state, json!({"name":NAME}))
        .await
        .result
        .unwrap();
    assert_eq!(first["records"], json!([]));
    assert_eq!(first["complete"], false);
    let mut last = first;
    for _ in 0..5 {
        if last["cursor"].is_null() {
            break;
        }
        last = rpc(&fixture.state, json!({"name":NAME,"cursor":last["cursor"]}))
            .await
            .result
            .unwrap();
    }
    assert_eq!(last["records"][0]["table"], "x_example_widget");
    assert_eq!(
        last["complete"], false,
        "unreadable priority tables forbid exhaustive claims"
    );
    assert_eq!(last["unreadable_tables"], 7);
    assert_eq!(last["cursor"], Value::Null);
}

#[tokio::test(flavor = "current_thread")]
async fn inherited_custom_display_names_and_empty_results_remain_truthful() {
    let instance = MockServer::start().await;
    model(&instance, "x_example_widget", "u_caption").await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_dictionary"))
        .and(query_param(
            "sysparm_query",
            "name=x_example_widget^display=true",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result":[]})))
        .with_priority(1)
        .mount(&instance)
        .await;
    Mock::given(method("GET")).and(path("/api/now/table/sys_db_object"))
        .and(query_param("sysparm_query", "name=x_example_widget"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result":[{"name":"x_example_widget","super_class":ID,"super_class.name":"x_example_base","super_class.super_class.name":""}]})))
        .mount(&instance).await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_dictionary"))
        .and(query_param(
            "sysparm_query",
            "name=x_example_base^display=true",
        ))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"result":[{"name":"x_example_base","element":"u_caption"}]})),
        )
        .mount(&instance)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/x_example_widget"))
        .and(query_param(
            "sysparm_query",
            format!("u_caption={NAME}^ORDERBYsys_id"),
        ))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"result":[{"sys_id":ID,"u_caption":NAME}]})),
        )
        .mount(&instance)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/x_example_widget"))
        .and(query_param(
            "sysparm_query",
            "u_caption=Missing Name^ORDERBYsys_id",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result":[]})))
        .mount(&instance)
        .await;
    let fixture = build_fixture_state_with_ui_metadata(&instance.uri())
        .await
        .unwrap();
    let found = rpc(
        &fixture.state,
        json!({"name":NAME,"table":"x_example_widget"}),
    )
    .await
    .result
    .unwrap();
    assert_eq!(found["records"][0]["matched_fields"], json!(["u_caption"]));
    assert_eq!(found["complete"], true);
    let missing = rpc(
        &fixture.state,
        json!({"name":"Missing Name","table":"x_example_widget"}),
    )
    .await
    .result
    .unwrap();
    assert_eq!(missing["records"], json!([]));
    assert_eq!(missing["match_count"], 0);
    assert_eq!(missing["complete"], true);
}

#[tokio::test(flavor = "current_thread")]
async fn name_lookup_allows_live_metadata_latency_before_querying_exact_candidates() {
    let instance = MockServer::start().await;
    model(&instance, "sys_user_group", "name").await;
    Mock::given(method("GET")).and(path("/api/now/ui/meta/sys_user_group"))
        .respond_with(ResponseTemplate::new(200).set_delay(std::time::Duration::from_millis(3200))
            .set_body_json(json!({"result":{"columns":{"name":{"type":"string"},"sys_id":{"type":"GUID"}}}})))
        .with_priority(1).mount(&instance).await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_user_group"))
        .and(query_param(
            "sysparm_query",
            format!("name={NAME}^ORDERBYsys_id"),
        ))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(json!({"result":[{"sys_id":ID,"name":NAME}]})),
        )
        .mount(&instance)
        .await;
    let fixture = build_fixture_state_with_ui_metadata(&instance.uri())
        .await
        .unwrap();
    let response = rpc(
        &fixture.state,
        json!({"name":NAME,"table":"sys_user_group"}),
    )
    .await;
    assert!(response.error.is_none(), "{response:?}");
    assert_eq!(response.result.unwrap()["records"][0]["sys_id"], ID);
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "cross-repository release gate"]
async fn daemon_names_end_to_end_via_mullet_stdio() {
    let instance = instance().await;
    let fixture = build_fixture_state_with_ui_metadata(&instance.uri())
        .await
        .unwrap();
    let socket = fixture.tempdir.path().join("name-e2e.sock");
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
                    .args(["--import", "tsx", "tests/helpers/name-mcp-driver.ts"])
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
