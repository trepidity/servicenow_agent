//! L0 compiled-CLI seam for the fixed cache-policy lifecycle.

use std::process::Command;

#[cfg(unix)]
use std::io::{BufRead, BufReader, Write};
#[cfg(unix)]
use std::os::unix::net::UnixListener;

#[test]
fn compiled_cli_advertises_fixed_cache_policy_lifecycle() {
    let output = Command::new(env!("CARGO_BIN_EXE_snow"))
        .arg("--help")
        .output()
        .expect("run compiled snow binary");

    assert!(output.status.success(), "status: {}", output.status);
    let stdout = String::from_utf8(output.stdout).expect("utf8 stdout");
    assert!(
        stdout.contains("cache-policy"),
        "compiled CLI must advertise the fixed cache-policy lifecycle; stdout: {stdout}"
    );
}

#[cfg(unix)]
#[test]
fn compiled_cli_sends_fixed_empty_validate_request_and_preserves_exact_result() {
    let home = tempfile::tempdir().expect("home");
    let config_dir = home.path().join(".config/snow");
    std::fs::create_dir_all(&config_dir).expect("config dir");
    let listener = UnixListener::bind(config_dir.join("daemon.sock")).expect("daemon socket");
    let daemon = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("CLI connection");
        let mut request = String::new();
        BufReader::new(stream.try_clone().expect("clone stream"))
            .read_line(&mut request)
            .expect("request line");
        let request: serde_json::Value = serde_json::from_str(&request).expect("JSON-RPC request");
        assert_eq!(request["method"], "cache_policy_validate");
        assert_eq!(request["params"], serde_json::json!({}));
        let response = "{\"jsonrpc\":\"2.0\",\"result\":{\"version\":1,\"source\":\"built_in_defaults\",\"rule_count\":4,\"fingerprint\":\"aa83e29dc9c4f2b46fc0f9912dda4614f7ffb0301c05470bb16774d40ebcb145\"},\"id\":1}\n";
        stream.write_all(response.as_bytes()).expect("response");
    });

    let output = Command::new(env!("CARGO_BIN_EXE_snow"))
        .args(["cache-policy", "validate", "--json"])
        .env("HOME", home.path())
        .output()
        .expect("run compiled snow binary");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout).expect("stdout"),
        concat!(
            "{\"fingerprint\":\"aa83e29dc9c4f2b46fc0f9912dda4614f7ffb0301c05470bb16774d40ebcb145\",",
            "\"rule_count\":4,\"source\":\"built_in_defaults\",\"version\":1}\n"
        )
    );
    daemon.join().expect("daemon fixture");
}

#[cfg(unix)]
#[test]
fn compiled_cli_sends_fixed_empty_reload_request_and_preserves_exact_result() {
    let home = tempfile::tempdir().expect("home");
    let config_dir = home.path().join(".config/snow");
    std::fs::create_dir_all(&config_dir).expect("config dir");
    let listener = UnixListener::bind(config_dir.join("daemon.sock")).expect("daemon socket");
    let daemon = std::thread::spawn(move || {
        let (mut stream, _) = listener.accept().expect("CLI connection");
        let mut request = String::new();
        BufReader::new(stream.try_clone().expect("clone stream"))
            .read_line(&mut request)
            .expect("request line");
        let request: serde_json::Value = serde_json::from_str(&request).expect("JSON-RPC request");
        assert_eq!(request["method"], "cache_policy_reload");
        assert_eq!(request["params"], serde_json::json!({}));
        let response = concat!(
            "{\"jsonrpc\":\"2.0\",\"result\":{\"version\":1,",
            "\"source\":\"built_in_defaults\",\"rule_count\":4,",
            "\"previous_fingerprint\":\"aa83e29dc9c4f2b46fc0f9912dda4614f7ffb0301c05470bb16774d40ebcb145\",",
            "\"fingerprint\":\"aa83e29dc9c4f2b46fc0f9912dda4614f7ffb0301c05470bb16774d40ebcb145\",",
            "\"changed\":false},\"id\":1}\n"
        );
        stream.write_all(response.as_bytes()).expect("response");
    });

    let output = Command::new(env!("CARGO_BIN_EXE_snow"))
        .args(["cache-policy", "reload", "--json"])
        .env("HOME", home.path())
        .output()
        .expect("run compiled snow binary");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&output.stdout).expect("JSON stdout"),
        serde_json::json!({
            "version": 1,
            "source": "built_in_defaults",
            "rule_count": 4,
            "previous_fingerprint": "aa83e29dc9c4f2b46fc0f9912dda4614f7ffb0301c05470bb16774d40ebcb145",
            "fingerprint": "aa83e29dc9c4f2b46fc0f9912dda4614f7ffb0301c05470bb16774d40ebcb145",
            "changed": false
        })
    );
    daemon.join().expect("daemon fixture");
}

#[cfg(unix)]
fn run_real_daemon_with_policy(home: &std::path::Path) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_snow"))
        .args(["daemon", "__serve", "--env", "test"])
        .env("HOME", home)
        .env("SERVICENOW_INSTANCE", "https://example.service-now.com")
        .env("SERVICENOW_USERNAME", "user@example.com")
        .env("SERVICENOW_PASSWORD", "test-password")
        .env("SNOW_DAEMON_IDLE_SECS", "1")
        .output()
        .expect("run real daemon process")
}

#[cfg(unix)]
#[test]
fn real_daemon_process_fails_startup_for_existing_invalid_policy() {
    let home = tempfile::tempdir().expect("home");
    let config_dir = home.path().join(".config/snow");
    std::fs::create_dir_all(&config_dir).expect("config dir");
    std::fs::write(
        config_dir.join("cache-policy.toml"),
        "version = 1\n[objects.*]\nmode = \"live\"\n",
    )
    .expect("invalid policy");

    let output = run_real_daemon_with_policy(home.path());
    assert!(!output.status.success(), "invalid policy must fail startup");
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("cache policy") || stderr.contains("CACHE_POLICY_INVALID"),
        "stderr must identify cache-policy startup failure: {stderr}"
    );
    assert!(
        !config_dir.join("daemon.sock").exists(),
        "failed startup must not expose a daemon socket"
    );
}

#[cfg(unix)]
#[test]
fn real_daemon_process_fails_startup_for_existing_unreadable_policy() {
    let home = tempfile::tempdir().expect("home");
    let config_dir = home.path().join(".config/snow");
    std::fs::create_dir_all(config_dir.join("cache-policy.toml"))
        .expect("directory at fixed policy path");

    let output = run_real_daemon_with_policy(home.path());
    assert!(
        !output.status.success(),
        "unreadable policy must fail startup"
    );
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("cache policy") || stderr.contains("CACHE_POLICY_IO"),
        "stderr must identify cache-policy startup failure: {stderr}"
    );
    assert!(
        !config_dir.join("daemon.sock").exists(),
        "failed startup must not expose a daemon socket"
    );
}
