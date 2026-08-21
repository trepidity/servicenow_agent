//! T-OPS-03 L0 consumer seam: compiled CLI -> real daemon socket -> local ServiceNow fake.
#![cfg(unix)]

use std::fs;
use std::process::Command;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::{Value, json};
use servicenow_rs::prelude::{BasicAuth, ServiceNowClient};
use snow_core::SnowCore;
use snow_core::config::{
    DaemonConfig as CoreDaemonConfig, InstanceConfig, SnowConfig, VaultConfig,
};
use snow_core::credential::CredentialProvider;
use snow_core::ipc::IpcEndpoint;
use snow_daemon::{DaemonState, rpc::JsonRpcServer};
use tokio::sync::oneshot;
use tokio::task::LocalSet;
use wiremock::matchers::{method, path, query_param_contains};
use wiremock::{Mock, MockServer, ResponseTemplate};

const INCIDENT_SYS_ID: &str = "0123456789abcdef0123456789abcdef";

#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::arc_with_non_send_sync)] // Production daemon API uses Arc on a LocalSet.
async fn compiled_cli_gets_and_queries_native_incidents_through_the_real_daemon() {
    let instance = MockServer::start().await;
    Mock::given(method("GET"))
        .and(path(format!("/api/now/table/incident/{INCIDENT_SYS_ID}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": {
                "sys_id": { "value": INCIDENT_SYS_ID, "display_value": INCIDENT_SYS_ID },
                "number": { "value": "INC0012345", "display_value": "INC0012345" },
                "state": { "value": "2", "display_value": "In Progress" },
                "caller_id": { "value": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "display_value": "Example User" }
            }
        })))
        .mount(&instance)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/incident"))
        .and(query_param_contains("sysparm_query", "numberININC0012345,INC0012346"))
        .and(query_param_contains("sysparm_query", "priorityIN2,3"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{
                "sys_id": { "value": INCIDENT_SYS_ID, "display_value": INCIDENT_SYS_ID },
                "number": { "value": "INC0012345", "display_value": "INC0012345" },
                "short_description": { "value": "Native summary", "display_value": "Native summary" },
                "priority": { "value": "2", "display_value": "2 - High" }
            }]
        })))
        .expect(1)
        .mount(&instance)
        .await;

    let home = tempfile::tempdir().expect("temporary home");
    let config_dir = home.path().join(".config/snow");
    fs::create_dir_all(&config_dir).expect("config directory");
    let socket = config_dir.join("daemon.sock");
    let vault = config_dir.join("vault");
    fs::create_dir_all(&vault).expect("vault directory");
    let client = ServiceNowClient::builder()
        .instance(instance.uri())
        .allow_http()
        .auth(BasicAuth::new("user@example.com", "test-password"))
        .build()
        .await
        .expect("local ServiceNow client");
    let mut config = SnowConfig {
        instance: InstanceConfig {
            url: instance.uri(),
            user: "user@example.com".to_string(),
            credential: CredentialProvider::Env,
            portal: String::new(),
        },
        vault: VaultConfig {
            path: vault.clone(),
        },
        daemon: CoreDaemonConfig {
            socket_path: socket.clone(),
            mcp_transport: "disabled".to_string(),
        },
        ..Default::default()
    };
    config.apply_defaults();
    let core = SnowCore::builder()
        .config(config)
        .client(client)
        .vault_path(vault)
        .build()
        .await
        .expect("Snow core");
    let server = JsonRpcServer::new(
        Arc::new(DaemonState::new(Arc::new(core))),
        IpcEndpoint::from_socket_path(&socket),
    )
    .with_idle_timeout(None);
    let (shutdown_tx, shutdown_rx) = oneshot::channel::<()>();
    let local = LocalSet::new();
    local.spawn_local(async move {
        server
            .serve_until(async move {
                let _ = shutdown_rx.await;
                Ok(())
            })
            .await
            .expect("daemon server");
    });

    let home_path = home.path().to_path_buf();
    let snow = env!("CARGO_BIN_EXE_snow");
    let (output, query_output) = local
        .run_until(async move {
            wait_for_daemon(&socket).await;
            let get_home = home_path.clone();
            let output = tokio::task::spawn_blocking(move || {
                Command::new(snow)
                    .args([
                        "--env",
                        "test",
                        "incident",
                        "get",
                        "--sys-id",
                        INCIDENT_SYS_ID,
                        "--json",
                    ])
                    .env("HOME", get_home)
                    .env("SNOW_ENV", "test")
                    .output()
                    .expect("run compiled snow CLI")
            })
            .await
            .expect("CLI task");
            let query_home = home_path.clone();
            let query_output = tokio::task::spawn_blocking(move || {
                Command::new(snow)
                    .args([
                        "--env",
                        "test",
                        "incident",
                        "query",
                        "--number",
                        "INC0012345",
                        "--number",
                        "INC0012346",
                        "--priority",
                        "2",
                        "--priority",
                        "3",
                        "--active",
                        "true",
                        "--limit",
                        "2",
                        "--json",
                    ])
                    .env("HOME", query_home)
                    .env("SNOW_ENV", "test")
                    .output()
                    .expect("run compiled snow query CLI")
            })
            .await
            .expect("query CLI task");
            let _ = shutdown_tx.send(());
            (output, query_output)
        })
        .await;

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let actual: Value = serde_json::from_slice(&output.stdout).expect("CLI JSON envelope");
    assert_eq!(
        actual,
        json!({
            "operation": "incident_get",
            "source": { "kind": "live" },
            "completeness": { "kind": "complete" },
            "data": {
                "record": {
                    "caller_id": { "value": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "display_value": "Example User" },
                    "number": { "value": "INC0012345", "display_value": "INC0012345" },
                    "state": { "value": "2", "display_value": "In Progress" },
                    "sys_id": { "value": INCIDENT_SYS_ID, "display_value": INCIDENT_SYS_ID }
                }
            }
        })
    );
    assert!(
        query_output.status.success(),
        "query stderr: {}",
        String::from_utf8_lossy(&query_output.stderr)
    );
    let query_actual: Value =
        serde_json::from_slice(&query_output.stdout).expect("CLI query JSON envelope");
    assert_eq!(
        query_actual,
        json!({
            "operation": "incident_query",
            "source": { "kind": "live" },
            "completeness": { "kind": "complete" },
            "data": {
                "records": [{
                    "number": { "value": "INC0012345", "display_value": "INC0012345" },
                    "priority": { "value": "2", "display_value": "2 - High" },
                    "short_description": { "value": "Native summary", "display_value": "Native summary" },
                    "sys_id": { "value": INCIDENT_SYS_ID, "display_value": INCIDENT_SYS_ID }
                }],
                "next_cursor": null,
                "limit": 2,
                "rows_inspected": 1
            }
        })
    );
}

async fn wait_for_daemon(socket: &std::path::Path) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if socket.exists() {
            return;
        }
        assert!(Instant::now() < deadline, "daemon socket was not created");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
