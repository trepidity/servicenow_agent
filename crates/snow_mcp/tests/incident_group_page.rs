//! Consumer-level MCP contract tests for assignment-group Incident pages.

use std::sync::Arc;

use serde_json::{Value, json};
use servicenow_rs::prelude::{BasicAuth, ServiceNowClient};
use snow_core::SnowCore;
use snow_mcp::{JsonRpcRequest, McpServer};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

const GROUP_SYS_ID: &str = "0000000000000000000000000000ab01";

async fn spawn_json_http_server(body: Value) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let address = listener.local_addr().expect("address");

    tokio::spawn(async move {
        let Ok((mut stream, _)) = listener.accept().await else {
            return;
        };
        let mut request = Vec::new();
        let mut buffer = [0u8; 1024];
        while let Ok(read) = stream.read(&mut buffer).await {
            if read == 0 {
                return;
            }
            request.extend_from_slice(&buffer[..read]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        let payload = body.to_string();
        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            payload.len(),
            payload
        );
        let _ = stream.write_all(response.as_bytes()).await;
        let _ = stream.shutdown().await;
    });

    format!("http://{address}")
}

async fn server_at(instance_url: &str) -> (McpServer, TempDir) {
    let tempdir = tempfile::tempdir().expect("tempdir");
    let client = ServiceNowClient::builder()
        .instance(instance_url)
        .allow_http()
        .auth(BasicAuth::new("tester", "secret"))
        .build()
        .await
        .expect("client");
    let core = SnowCore::builder()
        .client(client)
        .vault_path(tempdir.path().join("vault"))
        .build()
        .await
        .expect("core");
    #[allow(clippy::arc_with_non_send_sync)]
    let core = Arc::new(core);
    (McpServer::new(core), tempdir)
}

async fn call(server: &McpServer, arguments: Value) -> snow_mcp::JsonRpcResponse {
    server
        .dispatch(JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "tools/call".to_string(),
            params: json!({
                "name": "incident_list_by_assignment_group",
                "arguments": arguments
            }),
            id: Some(json!(1)),
        })
        .await
}

#[tokio::test]
async fn direct_mcp_incident_group_page_returns_the_shared_core_page_contract() {
    let instance_url = spawn_json_http_server(json!({
        "result": [{
            "sys_id": { "value": "0000000000000000000000000000aa01" },
            "number": { "value": "<INC_1>" },
            "short_description": { "value": "Ticket" },
            "state": { "value": "3", "display_value": "Pending" },
            "active": { "value": "true", "display_value": "true" },
            "assignment_group": { "value": GROUP_SYS_ID, "display_value": "<GROUP_DISPLAY>" }
        }]
    }))
    .await;
    let (server, _tempdir) = server_at(&instance_url).await;

    let response = call(
        &server,
        json!({ "assignment_group_sys_id": GROUP_SYS_ID, "limit": 25 }),
    )
    .await;

    assert!(response.error.is_none(), "{response:?}");
    let page = response.result.expect("page result");
    assert_eq!(
        page["records"][0]["sys_id"],
        json!("0000000000000000000000000000aa01")
    );
    assert_eq!(page["records"][0]["number"], json!("<INC_1>"));
    assert!(page["records"][0].get("browser_url").is_none());
    assert_eq!(page["complete"], json!(true));
    assert_eq!(page["next_cursor"], Value::Null);
    assert_eq!(page["limit"], json!(25));
    assert_eq!(page["rows_inspected"], json!(1));
}

#[tokio::test]
async fn direct_mcp_incident_group_page_returns_state_choices_for_correction() {
    let instance_url = spawn_json_http_server(json!({
        "result": [
            { "value": "1", "label": "New", "sequence": "100", "inactive": "false" },
            { "value": "3", "label": "Pending", "sequence": "200", "inactive": "false" }
        ]
    }))
    .await;
    let (server, _tempdir) = server_at(&instance_url).await;

    let response = call(
        &server,
        json!({
            "assignment_group_sys_id": GROUP_SYS_ID,
            "state": "Awaiting Vendor"
        }),
    )
    .await;

    let error = response.error.expect("unknown state must be rejected");
    assert_eq!(error.code, -32602);
    let data = error.data.expect("state correction data");
    assert_eq!(data["field"], json!("state"));
    assert_eq!(data["requested"], json!("Awaiting Vendor"));
    assert_eq!(
        data["choices"],
        json!([
            { "value": "1", "label": "New" },
            { "value": "3", "label": "Pending" }
        ])
    );
}
