//! `snow daemon status` smoke test — exercises pidfile and no-pidfile branches.
//!
//! `HOME` is overridden to a tempdir so [`DaemonPaths::resolve`] reports
//! `~tmp/.config/snow/daemon.pid` (which doesn't exist) and the status probe
//! takes the relevant isolated branch without touching the operator's daemon.
//!
//! This smoke path binds a Unix socket directly, so it only applies on Unix.
#![cfg(unix)]

use std::io::ErrorKind;
use std::os::unix::net::UnixListener;
use std::path::Path;
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn status_reports_stopped_when_no_pidfile() {
    let tmp = tempfile::tempdir().unwrap();
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_snow"))
        .arg("daemon")
        .arg("status")
        .env("HOME", tmp.path())
        .output()
        .unwrap();
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert!(stdout.starts_with("stopped"), "stdout was: {stdout}");
}

#[test]
fn status_reports_running_when_endpoint_responds_without_pidfile() {
    let tmp = tempfile::tempdir().unwrap();
    let config_dir = tmp.path().join(".config/snow");
    std::fs::create_dir_all(&config_dir).unwrap();

    let socket = config_dir.join("daemon.sock");
    let listener = bind_accepting_socket(&socket);

    let out = std::process::Command::new(env!("CARGO_BIN_EXE_snow"))
        .arg("daemon")
        .arg("status")
        .env("HOME", tmp.path())
        .output()
        .unwrap();
    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert!(
        stdout.starts_with("running"),
        "stdout did not report running\n{stdout}"
    );
    assert!(
        stdout.contains("missing pidfile"),
        "stdout did not report missing pidfile\n{stdout}"
    );
    listener.join().unwrap();
}

#[test]
fn status_reports_running_environment_from_statusfile() {
    let tmp = tempfile::tempdir().unwrap();
    let config_dir = tmp.path().join(".config/snow");
    std::fs::create_dir_all(&config_dir).unwrap();

    let socket = config_dir.join("daemon.sock");
    let listener = bind_accepting_socket(&socket);
    let pid = std::process::id();
    std::fs::write(config_dir.join("daemon.pid"), format!("{pid}\n")).unwrap();
    std::fs::write(
        config_dir.join("daemon.status"),
        format!(
            r#"{{
  "pid": {pid},
  "started_at": "2026-05-11T00:00:00Z",
  "version": "test",
  "environment": "prd",
  "env_file": "{}",
  "socket": "{}"
}}"#,
            config_dir.join(".env.prd").display(),
            socket.display()
        ),
    )
    .unwrap();

    let out = std::process::Command::new(env!("CARGO_BIN_EXE_snow"))
        .arg("daemon")
        .arg("status")
        .env("HOME", tmp.path())
        .output()
        .unwrap();
    assert!(out.status.success());
    let stdout = String::from_utf8(out.stdout).unwrap();
    assert!(
        stdout.contains("env: prd"),
        "stdout did not include env: prd\n{stdout}"
    );
    listener.join().unwrap();
}

fn bind_accepting_socket(path: &Path) -> thread::JoinHandle<()> {
    let listener = UnixListener::bind(path).unwrap();
    listener.set_nonblocking(true).unwrap();
    thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match listener.accept() {
                Ok((_stream, _addr)) => return,
                Err(err) if err.kind() == ErrorKind::WouldBlock => {
                    if Instant::now() >= deadline {
                        return;
                    }
                    thread::sleep(Duration::from_millis(10));
                }
                Err(_) => return,
            }
        }
    })
}
