//! End-to-end daemon lifecycle test: start → status → stop → status.
//!
//! Marked `#[ignore]` because `snow_daemon::run_blocking_rpc_only` builds a
//! real `SnowCore`, which requires:
//!   - `SNOW_INSTANCE` (or `SERVICENOW_INSTANCE`)
//!   - `SNOW_USER` (or `SERVICENOW_USERNAME`)
//!   - `SNOW_PASSWORD` (or `SERVICENOW_PASSWORD`), OR `OP_ITEM_ID` plus a
//!     working `op` CLI session that can fetch the password.
//!
//! These cannot be assumed in CI or unconfigured developer machines, and
//! the plan explicitly permits `#[ignore]` as a fallback rather than
//! adding a `--no-network` mode to the daemon. To run locally:
//!
//!     cargo test -p snow_cli --test daemon_lifecycle -- --ignored --nocapture
//!
//! The test overrides `HOME` to a tempdir so pidfile/socket/log all live
//! under `<tmp>/.config/snow/`. The spawned daemon inherits the same `HOME`
//! and reads `.env.prd` etc. from `<tmp>/.config/snow/`, so to run this
//! standalone you'd need to plant a `.env` there or export the SNOW_* vars
//! into the test environment.

use std::process::Command;
use std::time::{Duration, Instant};

#[test]
#[ignore = "boots a real daemon that requires ServiceNow credentials; run with --ignored locally"]
fn daemon_start_status_stop_roundtrip() {
    let tmp = tempfile::tempdir().unwrap();

    let snow = env!("CARGO_BIN_EXE_snow");

    // start
    let out = Command::new(snow)
        .args(["daemon", "start"])
        .env("HOME", tmp.path())
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "start failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(String::from_utf8_lossy(&out.stdout).contains("started"));

    // status loops a couple of times because socket readiness is ms-level
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut got_running = false;
    while Instant::now() < deadline {
        let out = Command::new(snow)
            .args(["daemon", "status"])
            .env("HOME", tmp.path())
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&out.stdout);
        if stdout.starts_with("running") {
            got_running = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(got_running, "status never reported running");

    // stop
    let out = Command::new(snow)
        .args(["daemon", "stop"])
        .env("HOME", tmp.path())
        .output()
        .unwrap();
    assert!(out.status.success());
    assert!(
        String::from_utf8_lossy(&out.stdout).contains("stopped")
            || String::from_utf8_lossy(&out.stdout).contains("killed")
    );

    // status reports stopped
    let out = Command::new(snow)
        .args(["daemon", "status"])
        .env("HOME", tmp.path())
        .output()
        .unwrap();
    assert!(String::from_utf8_lossy(&out.stdout).starts_with("stopped"));
}
