//! Consumer-level direct MCP tests for the strict live record_query contract.

#[path = "support/record_query.rs"]
mod record_query_support;

use std::sync::Arc;

use serde_json::{Value, json};
use servicenow_rs::prelude::{BasicAuth, ServiceNowClient};
use snow_core::SnowCore;
use snow_mcp::{JsonRpcRequest, McpServer};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

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

#[tokio::test]
async fn direct_mcp_record_query_returns_the_canonical_page_payload() {
    let instance_url = spawn_json_http_server(json!({
        "result": [{
            "sys_id": { "value": "00000000000000000000000000000001" },
            "number": { "value": "STRY1" },
            "short_description": { "value": "Typed story" },
            "state": { "value": "1", "display_value": "New" }
        }]
    }))
    .await;
    let (server, _tempdir) = server_at(&instance_url).await;
    let response = server
        .dispatch(JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "tools/call".to_string(),
            params: json!({
                "name": "record_query",
                "arguments": { "resource_type": "story", "limit": 2 }
            }),
            id: Some(json!(1)),
        })
        .await;

    assert!(response.error.is_none(), "{response:?}");
    let mut actual = response.result.expect("page");
    actual["records"][0]
        .as_object_mut()
        .expect("record")
        .remove("synced_at");
    assert_eq!(actual, record_query_support::expected_page());
}
