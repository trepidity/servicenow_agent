//! MCP `server_get` read-through (live fallback) integration tests.
//!
//! These verify the MCP-side contract from Part 1 of the
//! `2026-06-06-server-get-live-fallback` FR:
//!
//! - cache miss -> live exact hit -> record returned, and crucially the local
//!   cache is NOT written (mutation-free MCP per the completion plan's Work
//!   Package G `persist = false` boundary);
//! - cache miss -> live miss -> clean `-32004` not-found.
//!
//! All fixtures use RFC-5737 documentation IPs and placeholder sys_ids /
//! hostnames only.

use std::sync::Arc;

use serde_json::{Value, json};
use servicenow_rs::prelude::{BasicAuth, ServiceNowClient};
use snow_core::query::filter::ListQuery;
use snow_core::{ResourceType, SnowCore};
use snow_mcp::{JsonRpcRequest, JsonRpcResponse, McpServer};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

/// Spin up a one-shot HTTP server that replies to the first request with
/// `body` as a 200 JSON response, then closes. Returns the base URL.
async fn spawn_json_http_server(body: Value) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");

    tokio::spawn(async move {
        if let Ok((mut stream, _)) = listener.accept().await {
            let mut buf = [0u8; 1024];
            // Drain just enough of the request to reach the header terminator.
            let mut request = Vec::new();
            loop {
                match stream.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(n) => {
                        request.extend_from_slice(&buf[..n]);
                        if request.windows(4).any(|w| w == b"\r\n\r\n") {
                            break;
                        }
                    }
                    Err(_) => return,
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
        }
    });

    format!("http://{addr}")
}

async fn core_at_instance(instance_url: &str) -> (Arc<SnowCore>, TempDir) {
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
    (core, tempdir)
}

async fn call(server: &McpServer, arguments: Value) -> JsonRpcResponse {
    server
        .dispatch(JsonRpcRequest {
            jsonrpc: "2.0".to_string(),
            method: "tools/call".to_string(),
            params: json!({ "name": "server_get", "arguments": arguments }),
            id: Some(json!(1)),
        })
        .await
}

#[tokio::test]
async fn mcp_server_get_cache_miss_live_hit_returns_but_does_not_persist() {
    let sys_id = "cdcdcdcdcdcdcdcdcdcdcdcdcdcdcdcd";
    let instance_url = spawn_json_http_server(json!({
        "result": [{
            "sys_id": sys_id,
            "name": "host20.example.internal",
            "ip_address": "192.0.2.50",
            "sys_class_name": "cmdb_ci_linux_server",
            "operational_status": { "value": "1", "display_value": "Operational" }
        }]
    }))
    .await;
    let (core, tempdir) = core_at_instance(&instance_url).await;
    let server = McpServer::new(Arc::clone(&core));

    let response = call(&server, json!({ "name": "host20.example.internal" })).await;
    let result = response.result.expect("server result");
    assert_eq!(result["server"]["record"]["sys_id"].as_str(), Some(sys_id));

    // persist = false on the MCP path: no cache row, no vault write.
    let cached = core
        .list_records_query(ListQuery::new().resource_type(ResourceType::Server))
        .await
        .expect("cache query");
    assert!(
        cached.is_empty(),
        "MCP server_get must not persist the live record to the cache"
    );
    let servers_dir = tempdir.path().join("vault/servers");
    let wrote_vault = servers_dir
        .read_dir()
        .map(|mut d| d.next().is_some())
        .unwrap_or(false);
    assert!(!wrote_vault, "MCP server_get must not write the vault");
}

#[tokio::test]
async fn mcp_server_get_cache_miss_live_miss_is_not_found() {
    let instance_url = spawn_json_http_server(json!({ "result": [] })).await;
    let (core, _tempdir) = core_at_instance(&instance_url).await;
    let server = McpServer::new(core);

    let response = call(&server, json!({ "name": "ghost.example.internal" })).await;
    let error = response.error.expect("not found error");
    assert_eq!(error.code, -32004);
}
