//! L0 compiled-CLI contract for `snow incident fields`.
#![cfg(unix)]

use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::Path;
use std::process::Command;
use std::sync::Arc;
use std::thread;
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
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[test]
fn compiled_cli_preserves_the_incident_descriptor_envelope() {
    let tmp = tempfile::tempdir().expect("tempdir");
    let expected = expected_envelope();
    let daemon = bind_daemon(tmp.path(), expected.clone());

    let output = std::process::Command::new(env!("CARGO_BIN_EXE_snow"))
        .args(["incident", "fields", "--json"])
        .env("HOME", tmp.path())
        .output()
        .expect("run compiled snow binary");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let actual: Value = serde_json::from_slice(&output.stdout).expect("CLI JSON envelope");
    assert_eq!(actual, expected);
    daemon.join().expect("daemon fixture");
}

/// L0 CLI seam: compiled CLI process -> real daemon socket/server -> local
/// ServiceNow fake.
///
/// This is deliberately broader than the socket-contract test above. Removing
/// the CLI route, daemon route, live dictionary discovery, hierarchy traversal,
/// or exact envelope serialization must break one consumer-visible assertion.
#[tokio::test(flavor = "multi_thread")]
#[allow(clippy::arc_with_non_send_sync)] // Production daemon API uses Arc on a LocalSet.
async fn compiled_cli_discovers_incident_metadata_through_the_real_daemon() {
    let instance = MockServer::start().await;
    mount_incident_metadata(&instance).await;

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
    let output = local
        .run_until(async move {
            wait_for_daemon(&socket).await;
            let output = tokio::task::spawn_blocking(move || {
                Command::new(snow)
                    .args(["--env", "test", "incident", "fields", "--json"])
                    .env("HOME", home_path)
                    .env("SNOW_ENV", "test")
                    .output()
                    .expect("run compiled snow CLI")
            })
            .await
            .expect("CLI task");
            let _ = shutdown_tx.send(());
            output
        })
        .await;

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let actual: Value = serde_json::from_slice(&output.stdout).expect("CLI JSON envelope");
    assert_eq!(actual, expected_live_envelope());
}

fn bind_daemon(home: &Path, envelope: Value) -> thread::JoinHandle<()> {
    let config_dir = home.join(".config/snow");
    std::fs::create_dir_all(&config_dir).expect("config dir");
    let listener = UnixListener::bind(config_dir.join("daemon.sock")).expect("daemon socket");
    thread::spawn(move || {
        let (stream, _) = listener.accept().expect("CLI connection");
        let mut request = String::new();
        BufReader::new(stream.try_clone().expect("clone stream"))
            .read_line(&mut request)
            .expect("read request");
        let request: Value = serde_json::from_str(&request).expect("request JSON");
        assert_eq!(request["method"], "incident_fields");
        assert_eq!(request["params"], json!({}));

        let response = json!({ "jsonrpc": "2.0", "id": request["id"], "result": envelope });
        writeln!(
            &stream,
            "{}",
            serde_json::to_string(&response).expect("response JSON")
        )
        .expect("write response");
    })
}

fn expected_envelope() -> Value {
    json!({
        "operation": "incident_fields",
        "source": { "kind": "live" },
        "completeness": { "kind": "complete" },
        "data": {
            "resource_type": "Incident",
            "table": "incident",
            "readable_fields": {
                "status": "available",
                "value": [{
                    "name": "short_description",
                    "label": "Short description",
                    "kind": "string",
                    "choices": {
                        "status": "unavailable",
                        "reason": "not_supported_by_operation"
                    }
                }]
            },
            "writable_fields": {
                "status": "available",
                "value": [{
                    "name": "short_description",
                    "label": "Short description",
                    "kind": "string",
                    "choices": {
                        "status": "unavailable",
                        "reason": "not_supported_by_operation"
                    }
                }]
            },
            "paging": { "mode": "cursor", "default_limit": 50, "max_limit": 200 }
        }
    })
}

fn expected_live_envelope() -> Value {
    let number = json!({
        "name": "number",
        "label": "Number",
        "kind": "string",
        "choices": {
            "status": "unavailable",
            "reason": "not_supported_by_operation"
        }
    });
    let short_description = json!({
        "name": "short_description",
        "label": "Short description",
        "kind": "string",
        "choices": {
            "status": "unavailable",
            "reason": "not_supported_by_operation"
        }
    });
    let state = json!({
        "name": "state",
        "label": "State",
        "kind": "integer",
        "choices": {
            "status": "available",
            "value": [
                { "label": "Open", "value": "1", "terminal": false },
                { "label": "Closed", "value": "3", "terminal": true }
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
                "value": [number, short_description.clone(), state.clone()]
            },
            "writable_fields": {
                "status": "available",
                "value": [short_description, state]
            },
            "paging": { "mode": "cursor", "default_limit": 50, "max_limit": 200 }
        }
    })
}

async fn mount_incident_metadata(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_db_object"))
        .and(query_param("sysparm_query", "name=incident"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{
                "name": "incident",
                "super_class": { "value": "task-table-sys-id", "display_value": "Task" }
            }]
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_db_object"))
        .and(query_param("sysparm_query", "sys_id=task-table-sys-id"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{ "name": "task" }]
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_db_object"))
        .and(query_param("sysparm_query", "name=task"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{ "name": "task", "super_class": "" }]
        })))
        .mount(server)
        .await;

    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_dictionary"))
        .and(query_param("sysparm_query", "name=incident^active=true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [
                {
                    "name": "incident",
                    "element": "number",
                    "column_label": "Number",
                    "internal_type": { "value": "string", "display_value": "String" },
                    "reference": "",
                    "choice": "0",
                    "read_only": "true",
                    "active": "true"
                },
                {
                    "name": "incident",
                    "element": "short_description",
                    "column_label": "Short description",
                    "internal_type": { "value": "string", "display_value": "String" },
                    "reference": "",
                    "choice": "0",
                    "read_only": "false",
                    "active": "true"
                }
            ]
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_dictionary"))
        .and(query_param("sysparm_query", "name=task^active=true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [{
                "name": "task",
                "element": "state",
                "column_label": "State",
                "internal_type": { "value": "integer", "display_value": "Integer" },
                "reference": "",
                "choice": "1",
                "read_only": "false",
                "active": "true"
            }]
        })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_choice"))
        .and(query_param(
            "sysparm_query",
            "name=incident^element=state^ORDERBYsequence",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({ "result": [] })))
        .mount(server)
        .await;
    Mock::given(method("GET"))
        .and(path("/api/now/table/sys_choice"))
        .and(query_param(
            "sysparm_query",
            "name=task^element=state^ORDERBYsequence",
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(json!({
            "result": [
                { "value": "1", "label": "Open", "sequence": "1", "inactive": "false", "terminal": "false" },
                { "value": "3", "label": "Closed", "sequence": "2", "inactive": "false", "terminal": "true" }
            ]
        })))
        .mount(server)
        .await;
}

async fn wait_for_daemon(socket: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        if UnixStream::connect(socket).is_ok() {
            return;
        }
        assert!(Instant::now() < deadline, "daemon did not become reachable");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}
