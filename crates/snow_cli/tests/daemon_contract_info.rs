//! L0 CLI contract for `snow daemon contract-info`.
//!
//! The compiled binary talks to a temporary daemon IPC endpoint. These cases
//! catch accidental expansion into a generic RPC client or leakage of daemon
//! runtime metadata into the operator-facing contract report.
#![cfg(unix)]

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixListener;
use std::path::Path;
use std::thread;

use serde_json::{Value, json};

// The OS test sandbox occasionally rejects simultaneous Unix socket binds.
// This consumer seam needs one temporary daemon endpoint at a time.
static DAEMON_SOCKET_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[test]
fn contract_info_prints_only_the_sanitized_daemon_contract() {
    let _socket_lock = socket_lock();
    let tmp = tempfile::tempdir().unwrap();
    let listener = bind_daemon(tmp.path(), contract_response());

    let output = run_contract_info(tmp.path());

    assert!(output.status.success(), "stderr: {}", text(&output.stderr));
    let report: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        report,
        json!({
            "contract_version": "daemon-json-rpc-v1",
            "daemon_version": "test-version",
            "supported_methods": ["contract_info", "get_record"],
            "deprecated_aliases": [{"method": "old_get", "replacement": "get_record"}],
            "environment": {"label": "test"},
            "mcp_availability": {"mode": "enabled", "transport": "stdio"},
        })
    );
    listener.join().unwrap();
}

#[test]
fn contract_info_rejects_an_incompatible_daemon_contract() {
    let _socket_lock = socket_lock();
    let tmp = tempfile::tempdir().unwrap();
    let mut response = contract_response();
    response["result"]["contract_version"] = json!("daemon-json-rpc-v2");
    let listener = bind_daemon(tmp.path(), response);

    let output = run_contract_info(tmp.path());

    assert!(!output.status.success());
    assert!(
        text(&output.stderr).contains("incompatible daemon contract"),
        "stderr: {}",
        text(&output.stderr)
    );
    listener.join().unwrap();
}

#[test]
fn contract_info_rejects_a_malformed_daemon_contract() {
    let _socket_lock = socket_lock();
    let tmp = tempfile::tempdir().unwrap();
    let mut response = contract_response();
    response["result"]
        .as_object_mut()
        .unwrap()
        .remove("supported_methods");
    let listener = bind_daemon(tmp.path(), response);

    let output = run_contract_info(tmp.path());

    assert!(!output.status.success());
    assert!(
        text(&output.stderr).contains("malformed daemon contract"),
        "stderr: {}",
        text(&output.stderr)
    );
    listener.join().unwrap();
}

#[test]
fn contract_info_fails_when_the_daemon_is_unreachable() {
    let tmp = tempfile::tempdir().unwrap();

    let output = run_contract_info(tmp.path());

    assert!(!output.status.success());
    assert!(
        text(&output.stderr).contains("daemon unreachable"),
        "stderr: {}",
        text(&output.stderr)
    );
}

#[test]
fn compiled_cli_inventory_keeps_deferred_cancellation_out_of_help() {
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_snow"))
        .arg("--help")
        .output()
        .unwrap();

    assert!(output.status.success(), "stderr: {}", text(&output.stderr));
    let help = text(&output.stdout);
    assert!(help.contains("attachment"));
    assert!(!help.contains("catalog_cancel_request"));
}

fn run_contract_info(home: &Path) -> std::process::Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_snow"))
        .args(["daemon", "contract-info"])
        .env("HOME", home)
        .output()
        .unwrap()
}

fn bind_daemon(home: &Path, response: Value) -> thread::JoinHandle<()> {
    let config_dir = home.join(".config/snow");
    std::fs::create_dir_all(&config_dir).unwrap();
    let listener = UnixListener::bind(config_dir.join("daemon.sock")).unwrap();
    thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        let mut reader = BufReader::new(stream.try_clone().unwrap());
        let mut request = String::new();
        reader.read_line(&mut request).unwrap();
        let request: Value = serde_json::from_str(&request).unwrap();
        assert_eq!(request["method"], "contract_info");
        assert_eq!(request["params"], json!({}));

        let mut stream = stream;
        writeln!(stream, "{}", serde_json::to_string(&response).unwrap()).unwrap();
    })
}

fn contract_response() -> Value {
    json!({
        "jsonrpc": "2.0",
        "id": 1,
        "result": {
            "contract_version": "daemon-json-rpc-v1",
            "daemon_version": "test-version",
            "supported_methods": ["contract_info", "get_record"],
            "deprecated_aliases": [{"method": "old_get", "replacement": "get_record"}],
            "environment": {
                "label": "test",
                "instance_host": "hostile.example.invalid",
                "username": "hostile-user"
            },
            "mcp_availability": {"mode": "enabled", "transport": "stdio"},
            "endpoint": "/hostile/path",
            "policy": {"hostile": "secret"}
        }
    })
}

fn text(bytes: &[u8]) -> String {
    String::from_utf8_lossy(bytes).into_owned()
}

fn socket_lock() -> std::sync::MutexGuard<'static, ()> {
    DAEMON_SOCKET_LOCK
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
