//! L0 startup proof: the compiled foreground daemon, isolated config, and a real
//! credential-helper subprocess. No provider network or operator credentials.
#![cfg(unix)]

use std::io::{BufRead, BufReader, Write};
use std::os::unix::{fs::PermissionsExt, net::UnixStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

struct Fixture {
    dir: tempfile::TempDir,
    child: Child,
}

impl Fixture {
    fn start(helper: &str) -> Self {
        let dir = tempfile::tempdir().unwrap();
        let bin = dir.path().join("snow");
        std::fs::hard_link(env!("CARGO_BIN_EXE_snow"), &bin).unwrap();
        std::fs::create_dir(dir.path().join("snow_config")).unwrap();
        let op = dir.path().join("op");
        std::fs::write(
            &op,
            format!("#!/bin/sh\nprintf '%s\\n' \"$$\" > \"$STARTUP_HELPER_PID\"\n{helper}\n"),
        )
        .unwrap();
        std::fs::set_permissions(&op, std::fs::Permissions::from_mode(0o700)).unwrap();
        let child = Command::new(bin)
            .args(["daemon", "__serve", "--env", "test", "--no-idle-timeout"])
            .current_dir(dir.path())
            .env_clear()
            .env("PATH", format!("{}:/usr/bin:/bin", dir.path().display()))
            .env("SERVICENOW_INSTANCE", "https://example.service-now.com")
            .env("OP_ITEM_ID", "example-item")
            .env("OP_VAULT", "example-vault")
            .env("SNOW_DAEMON_CREDENTIAL_TIMEOUT_SECS", "1")
            .env("STARTUP_HELPER_PID", dir.path().join("helper.pid"))
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap();
        Self { dir, child }
    }

    fn wait_for_exit(&mut self) -> bool {
        let deadline = Instant::now() + Duration::from_secs(4);
        while Instant::now() < deadline {
            if self.child.try_wait().unwrap().is_some() {
                return true;
            }
            std::thread::sleep(Duration::from_millis(20));
        }
        false
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
        // Also clean up a helper left behind by a broken pre-repair daemon.
        if let Ok(pid) = std::fs::read_to_string(self.dir.path().join("helper.pid"))
            && let Ok(pid) = pid.trim().parse::<i32>()
        {
            let _ = nix::sys::signal::kill(
                nix::unistd::Pid::from_raw(pid),
                nix::sys::signal::Signal::SIGKILL,
            );
        }
    }
}

#[test]
fn foreground_daemon_bounds_a_hung_credential_helper_and_reaps_it() {
    let mut fixture = Fixture::start("exec /bin/sleep 30");
    assert!(
        fixture.wait_for_exit(),
        "credential startup must fail within its bound, not hang before binding the socket"
    );
    assert!(!fixture.child.try_wait().unwrap().unwrap().success());
    let helper_pid = std::fs::read_to_string(fixture.dir.path().join("helper.pid")).unwrap();
    assert!(
        nix::sys::signal::kill(
            nix::unistd::Pid::from_raw(helper_pid.trim().parse().unwrap()),
            None
        )
        .is_err(),
        "timed-out credential helper must be reaped"
    );
    assert!(!fixture.dir.path().join("snow_config/daemon.pid").exists());
    let stderr = std::io::read_to_string(fixture.child.stderr.take().unwrap()).unwrap();
    assert!(stderr.contains("credential lookup timed out"), "{stderr}");
    assert!(
        !stderr.contains("example-item"),
        "credential identities must not leak in diagnostics"
    );
}

#[test]
fn foreground_daemon_uses_one_atomic_credential_read_and_answers_contract_rpc() {
    let mut fixture = Fixture::start(
        r#"
case "$*" in
  'item get example-item --vault example-vault --fields label=username,label=password --reveal --format json')
    printf '%s' '[{"label":"password","value":"example-secret"},{"label":"username","value":"example-user"}]' ;;
  *) printf '%s' 'unexpected separate credential read' >&2; exit 9 ;;
esac
"#,
    );
    let socket = fixture.dir.path().join("snow_config/daemon.sock");
    let deadline = Instant::now() + Duration::from_secs(4);
    let mut stream = loop {
        if let Ok(stream) = UnixStream::connect(&socket) {
            break stream;
        }
        if fixture.child.try_wait().unwrap().is_some() || Instant::now() >= deadline {
            panic!("daemon did not become ready using the paired credential response");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    stream
        .set_read_timeout(Some(Duration::from_secs(2)))
        .unwrap();
    stream
        .write_all(b"{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"contract_info\",\"params\":{}}\n")
        .unwrap();
    let mut response = String::new();
    BufReader::new(stream).read_line(&mut response).unwrap();
    let response: serde_json::Value = serde_json::from_str(&response).unwrap();
    assert_eq!(response["result"]["contract_version"], "daemon-json-rpc-v1");
    assert!(!response.to_string().contains("example-secret"));
}

#[test]
fn foreground_daemon_refuses_malformed_credentials_without_logging_secrets() {
    let mut fixture =
        Fixture::start("printf '%s' '[{\"label\":\"password\",\"value\":\"example-secret\"}]'");
    assert!(
        fixture.wait_for_exit(),
        "malformed credentials must fail promptly"
    );
    assert!(!fixture.child.try_wait().unwrap().unwrap().success());
    let stderr = std::io::read_to_string(fixture.child.stderr.take().unwrap()).unwrap();
    assert!(stderr.contains("credential response"), "{stderr}");
    assert!(!stderr.contains("example-secret"), "{stderr}");
}
