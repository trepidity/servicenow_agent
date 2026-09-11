//! L0: live child enumeration through daemon and both MCP transports.
use super::{JsonRpcRequest, dispatch};
use crate::test_support::build_fixture_state_with_ui_metadata;
use serde_json::{Value, json};
use wiremock::{
    Mock, MockServer, ResponseTemplate,
    matchers::{method, path, query_param},
};

const PARENT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const CHILD: &str = "11111111111111111111111111111111";

fn stable(mut value: Value) -> Value {
    for record in value["records"].as_array_mut().unwrap() {
        record.as_object_mut().unwrap().remove("synced_at");
    }
    value
}

async fn rpc(state: &std::sync::Arc<crate::DaemonState>, args: Value) -> super::JsonRpcResponse {
    dispatch(
        JsonRpcRequest {
            jsonrpc: "2.0".into(),
            method: "record_query".into(),
            params: args,
            id: Some(json!(1)),
        },
        state,
    )
    .await
}

async fn instance() -> MockServer {
    let instance = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/task"))
        .and(query_param("sysparm_query", "number=PRJ0000001"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(json!({"result":[{"sys_id":PARENT,"number":"PRJ0000001"}]})),
        )
        .mount(&instance)
        .await;
    for (table, relation, number) in [
        ("resource_plan", format!("task={PARENT}"), "RPLN0000001"),
        (
            "pm_project_task",
            format!("parent={PARENT}^ORtop_task={PARENT}"),
            "PRJTASK0000001",
        ),
        ("task", format!("parent={PARENT}"), "TASK0000001"),
    ] {
        for (suffix, rows) in [
            (
                String::new(),
                json!([{"sys_id":CHILD,"number":number,"task":PARENT,"parent":PARENT,"top_task":PARENT,"group_resource":"bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}]),
            ),
            (format!("^sys_id>{CHILD}"), json!([])),
        ] {
            Mock::given(method("GET"))
                .and(path(format!("/api/now/table/{table}")))
                .and(query_param(
                    "sysparm_query",
                    format!("{relation}{suffix}^ORDERBYsys_id"),
                ))
                .and(query_param("sysparm_limit", "1"))
                .respond_with(ResponseTemplate::new(200).set_body_json(json!({"result":rows})))
                .mount(&instance)
                .await;
        }
    }
    instance
}

#[tokio::test(flavor = "current_thread")]
async fn child_queries_page_all_groups_and_tasks_with_mcp_parity() {
    use snow_mcp::{DaemonBackedMcpBridge, JsonRpcRequest as McpRequest, McpServer};
    let instance = instance().await;
    let fixture = build_fixture_state_with_ui_metadata(&instance.uri())
        .await
        .unwrap();
    let socket = fixture.tempdir.path().join("children.sock");
    let server = super::JsonRpcServer::new(
        std::sync::Arc::clone(&fixture.state),
        snow_core::ipc::IpcEndpoint::filesystem(&socket),
    );
    tokio::task::LocalSet::new().run_until(async {
        let (stop,stopped)=tokio::sync::oneshot::channel::<()>();
        let task=tokio::task::spawn_local(server.serve_until(async { let _=stopped.await; Ok(()) }));
        tokio::time::timeout(std::time::Duration::from_secs(10),async { while !socket.exists() {tokio::time::sleep(std::time::Duration::from_millis(10)).await;} }).await.unwrap();
        for kind in ["resource_plan","project_task","task"] {
            let args=json!({"resource_type":kind,"filters":{"parent_number":"PRJ0000001"},"limit":1});
            let response=rpc(&fixture.state,args.clone()).await;
            assert!(response.error.is_none(),"{response:?}");
            let result=response.result.unwrap();
            assert_eq!(result["records"][0]["sys_id"],CHILD);
            assert_eq!(result["complete"],false);
            assert_eq!(result["source"],"live");
            assert_eq!(result["next_cursor"],CHILD);
            let request=|| McpRequest {jsonrpc:"2.0".into(),method:"tools/call".into(),params:json!({"name":"record_query","arguments":args}),id:Some(json!(1))};
            let direct=McpServer::new(std::sync::Arc::clone(&fixture.state.core)).dispatch(request()).await;
            assert_eq!(stable(direct.result.unwrap()),stable(result.clone()));
            let bridge=DaemonBackedMcpBridge::from_socket(socket.clone()).dispatch(request()).await;
            assert_eq!(stable(bridge.result.unwrap()["structuredContent"].clone()),stable(result));
            let last=rpc(&fixture.state,json!({"resource_type":kind,"filters":{"parent_sys_id":PARENT},"limit":1,"cursor":CHILD})).await.result.unwrap();
            assert_eq!(last["complete"],true);
            assert_eq!(last["records"],json!([]));
            assert_eq!(last["next_cursor"],Value::Null);
        }
        let _=stop.send(());
        task.await.unwrap().unwrap();
    }).await;
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
async fn child_queries_reject_unscoped_mixed_and_injected_parents_before_io() {
    let instance = MockServer::start().await;
    let fixture = build_fixture_state_with_ui_metadata(&instance.uri())
        .await
        .unwrap();
    for filters in [
        json!({}),
        json!({"parent_number":"PRJ0000001^ORactive=true"}),
        json!({"parent_sys_id":"bad"}),
        json!({"parent_number":"PRJ0000001","parent_sys_id":PARENT}),
        json!({"parent_number":"PRJ0000001","assignment_group":PARENT}),
    ] {
        let result = rpc(
            &fixture.state,
            json!({"resource_type":"resource_plan","filters":filters}),
        )
        .await;
        assert_eq!(result.error.unwrap().code, -32602);
    }
    assert!(instance.received_requests().await.unwrap().is_empty());
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "cross-repository release gate"]
async fn daemon_children_end_to_end_via_mullet_stdio() {
    let instance = instance().await;
    let fixture = build_fixture_state_with_ui_metadata(&instance.uri())
        .await
        .unwrap();
    let socket = fixture.tempdir.path().join("children-e2e.sock");
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
                    .args(["--import", "tsx", "tests/helpers/children-mcp-driver.ts"])
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
