//! `snow daemon status` smoke test — exercises the no-pidfile branch.
//!
//! `HOME` is overridden to a tempdir so [`DaemonPaths::resolve`] reports
//! `~tmp/.config/snow/daemon.pid` (which doesn't exist) and the status
//! probe takes the `(None, false)` branch and prints `stopped`.
//!
//! Daemon mode is Unix-only, so this test does not apply on Windows.
#![cfg(unix)]

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
fn status_reports_running_environment_from_statusfile() {
    let tmp = tempfile::tempdir().unwrap();
    let config_dir = tmp.path().join(".config/snow");
    std::fs::create_dir_all(&config_dir).unwrap();

    let socket = config_dir.join("daemon.sock");
    let _listener = std::os::unix::net::UnixListener::bind(&socket).unwrap();
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
}
