//! L0 compiled-CLI contract for `snow incident fields`.
#![cfg(unix)]

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixListener;
use std::path::Path;
use std::thread;

use serde_json::{Value, json};

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
            "writable_fields": { "status": "unavailable", "reason": "acl_denied" },
            "paging": { "mode": "cursor", "default_limit": 50, "max_limit": 200 }
        }
    })
}
