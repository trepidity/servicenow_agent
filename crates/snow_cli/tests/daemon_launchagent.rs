#![cfg(unix)]

use std::{
    env, fs,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::Command,
};

use tempfile::TempDir;

fn repository_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .expect("snow_cli must be nested below the repository root")
        .to_path_buf()
}

fn write_executable(path: &Path, contents: &str) {
    fs::create_dir_all(path.parent().expect("executable path has a parent"))
        .expect("create executable parent");
    fs::write(path, contents).expect("write executable");
    let mut permissions = fs::metadata(path)
        .expect("read executable metadata")
        .permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).expect("mark executable");
}

fn fake_launchctl(path: &Path) {
    write_executable(
        path,
        r#"#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$SNOW_TEST_CALL_LOG"
if [[ "${1:-}" == "print" ]]; then
  printf 'state = running\npid = 1234\n'
fi
"#,
    );
}

fn write_ready_snow(path: &Path) {
    write_executable(path, "#!/usr/bin/env bash\nprintf 'running\\n'\n");
}

#[test]
fn installer_creates_a_foreground_non_idle_launchagent() {
    let temporary = TempDir::new().expect("temporary directory");
    let root = temporary.path();
    let install_dir = root.join("install");
    let snow_binary = install_dir.join("snow");
    let launch_agents = root.join("LaunchAgents");
    let log_dir = root.join("Logs");
    let tools = root.join("tools");
    let call_log = root.join("calls.log");
    let launchctl = tools.join("launchctl");
    let plutil = tools.join("plutil");

    write_ready_snow(&snow_binary);
    fake_launchctl(&launchctl);
    fake_launchctl(&plutil);

    let output = Command::new("bash")
        .arg(repository_root().join("scripts/manage_daemon_launchagent.sh"))
        .arg("install")
        .env("SNOW_RELEASE_INSTALL_DIR", &install_dir)
        .env("SNOW_DAEMON_LAUNCH_AGENTS_DIR", &launch_agents)
        .env("SNOW_DAEMON_LOG_DIR", &log_dir)
        .env("SNOW_LAUNCHCTL_BIN", &launchctl)
        .env("SNOW_PLUTIL_BIN", &plutil)
        .env("SNOW_TEST_CALL_LOG", &call_log)
        .output()
        .expect("run daemon service installer");

    assert!(
        output.status.success(),
        "installer failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let plist = fs::read_to_string(launch_agents.join("com.servicenow-agent.snow-daemon.plist"))
        .expect("service plist");
    assert!(plist.contains(&format!("<string>{}</string>", snow_binary.display())));
    assert!(plist.contains("<string>daemon</string>"));
    assert!(plist.contains("<string>__serve</string>"));
    assert!(plist.contains("<string>--no-idle-timeout</string>"));
    assert!(plist.contains("<key>RunAtLoad</key>\n    <true/>"));
    assert!(plist.contains("<key>SuccessfulExit</key>\n        <false/>"));
    assert!(plist.contains(
        "<key>PATH</key>\n        <string>/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin</string>"
    ));
    assert!(!plist.contains("<string>start</string>"));
    assert!(!plist.contains("<string>restart</string>"));

    let calls = fs::read_to_string(call_log).expect("launchctl call log");
    assert!(calls.contains("-lint"));
    assert!(calls.contains("bootout gui/"));
    assert!(calls.contains("bootstrap gui/"));
    assert!(calls.contains("enable gui/"));
    assert!(calls.contains("kickstart -k gui/"));
    assert!(calls.contains("print gui/"));
}

#[test]
fn installer_fails_if_launchd_never_reports_a_live_process() {
    let temporary = TempDir::new().expect("temporary directory");
    let root = temporary.path();
    let install_dir = root.join("install");
    let launch_agents = root.join("LaunchAgents");
    let log_dir = root.join("Logs");
    let tools = root.join("tools");
    let call_log = root.join("calls.log");
    let launchctl = tools.join("launchctl");
    let plutil = tools.join("plutil");

    write_executable(&install_dir.join("snow"), "#!/usr/bin/env bash\nexit 0\n");
    write_executable(
        &launchctl,
        r#"#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$SNOW_TEST_CALL_LOG"
if [[ "${1:-}" == "print" ]]; then
  printf 'state = spawn scheduled\n'
fi
"#,
    );
    fake_launchctl(&plutil);

    let output = Command::new("bash")
        .arg(repository_root().join("scripts/manage_daemon_launchagent.sh"))
        .arg("install")
        .env("SNOW_RELEASE_INSTALL_DIR", &install_dir)
        .env("SNOW_DAEMON_LAUNCH_AGENTS_DIR", &launch_agents)
        .env("SNOW_DAEMON_LOG_DIR", &log_dir)
        .env("SNOW_LAUNCHCTL_BIN", &launchctl)
        .env("SNOW_PLUTIL_BIN", &plutil)
        .env("SNOW_DAEMON_START_TIMEOUT_SECS", "0")
        .env("SNOW_TEST_CALL_LOG", &call_log)
        .output()
        .expect("run daemon service installer");

    assert!(
        !output.status.success(),
        "installer must reject a non-running service"
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("did not reach running state"));
}

#[test]
fn installer_fails_if_the_daemon_endpoint_never_becomes_reachable() {
    let temporary = TempDir::new().expect("temporary directory");
    let root = temporary.path();
    let install_dir = root.join("install");
    let launch_agents = root.join("LaunchAgents");
    let log_dir = root.join("Logs");
    let tools = root.join("tools");
    let call_log = root.join("calls.log");
    let launchctl = tools.join("launchctl");
    let plutil = tools.join("plutil");

    write_executable(
        &install_dir.join("snow"),
        "#!/usr/bin/env bash\nprintf 'unreachable\\n'\n",
    );
    fake_launchctl(&launchctl);
    fake_launchctl(&plutil);

    let output = Command::new("bash")
        .arg(repository_root().join("scripts/manage_daemon_launchagent.sh"))
        .arg("install")
        .env("SNOW_RELEASE_INSTALL_DIR", &install_dir)
        .env("SNOW_DAEMON_LAUNCH_AGENTS_DIR", &launch_agents)
        .env("SNOW_DAEMON_LOG_DIR", &log_dir)
        .env("SNOW_LAUNCHCTL_BIN", &launchctl)
        .env("SNOW_PLUTIL_BIN", &plutil)
        .env("SNOW_DAEMON_ENDPOINT_TIMEOUT_SECS", "0")
        .env("SNOW_TEST_CALL_LOG", &call_log)
        .output()
        .expect("run daemon service installer");

    assert!(
        !output.status.success(),
        "installer must reject a service whose endpoint is not connectable"
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains("endpoint did not become reachable"));
}

#[test]
fn installer_retries_a_transient_launchd_bootstrap_failure() {
    let temporary = TempDir::new().expect("temporary directory");
    let root = temporary.path();
    let install_dir = root.join("install");
    let launch_agents = root.join("LaunchAgents");
    let log_dir = root.join("Logs");
    let tools = root.join("tools");
    let call_log = root.join("calls.log");
    let bootstrap_attempts = root.join("bootstrap-attempts");
    let launchctl = tools.join("launchctl");
    let plutil = tools.join("plutil");

    write_ready_snow(&install_dir.join("snow"));
    write_executable(
        &launchctl,
        r#"#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$SNOW_TEST_CALL_LOG"
if [[ "${1:-}" == "bootstrap" ]]; then
  attempts=0
  if [[ -f "$SNOW_TEST_BOOTSTRAP_ATTEMPTS" ]]; then
    attempts="$(cat "$SNOW_TEST_BOOTSTRAP_ATTEMPTS")"
  fi
  attempts=$((attempts + 1))
  printf '%s\n' "$attempts" > "$SNOW_TEST_BOOTSTRAP_ATTEMPTS"
  if [[ "$attempts" == "1" ]]; then
    exit 5
  fi
fi
if [[ "${1:-}" == "print" ]]; then
  printf 'state = running\npid = 1234\n'
fi
"#,
    );
    fake_launchctl(&plutil);

    let output = Command::new("bash")
        .arg(repository_root().join("scripts/manage_daemon_launchagent.sh"))
        .arg("install")
        .env("SNOW_RELEASE_INSTALL_DIR", &install_dir)
        .env("SNOW_DAEMON_LAUNCH_AGENTS_DIR", &launch_agents)
        .env("SNOW_DAEMON_LOG_DIR", &log_dir)
        .env("SNOW_LAUNCHCTL_BIN", &launchctl)
        .env("SNOW_PLUTIL_BIN", &plutil)
        .env("SNOW_DAEMON_BOOTSTRAP_TIMEOUT_SECS", "2")
        .env("SNOW_TEST_CALL_LOG", &call_log)
        .env("SNOW_TEST_BOOTSTRAP_ATTEMPTS", &bootstrap_attempts)
        .output()
        .expect("run daemon service installer");

    assert!(
        output.status.success(),
        "installer should retry a transient bootstrap error: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read_to_string(bootstrap_attempts)
            .expect("bootstrap attempts")
            .trim(),
        "2"
    );
}

#[test]
fn installer_drains_legacy_daemon_owners_before_launching_the_service() {
    let temporary = TempDir::new().expect("temporary directory");
    let root = temporary.path();
    let install_dir = root.join("install");
    let snow_binary = install_dir.join("snow");
    let launch_agents = root.join("LaunchAgents");
    let log_dir = root.join("Logs");
    let tools = root.join("tools");
    let call_log = root.join("calls.log");
    let kill_log = root.join("kill.log");
    let owner_state = root.join("owner-state");
    let launchctl = tools.join("launchctl");
    let plutil = tools.join("plutil");
    let ps = tools.join("ps");
    let kill = tools.join("kill");

    write_ready_snow(&snow_binary);
    fake_launchctl(&launchctl);
    fake_launchctl(&plutil);
    write_executable(
        &ps,
        r#"#!/usr/bin/env bash
set -euo pipefail
printf '123 %s daemon __serve --env prd\n' "$SNOW_TEST_SNOW_BINARY"
printf '456 /unrelated/snow daemon __serve --env prd\n'
"#,
    );
    write_executable(
        &kill,
        r#"#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$SNOW_TEST_KILL_LOG"
if [[ "${1:-}" == "-TERM" ]]; then
  : > "$SNOW_TEST_OWNER_STATE"
  exit 0
fi
if [[ "${1:-}" == "-0" && -f "$SNOW_TEST_OWNER_STATE" ]]; then
  exit 1
fi
exit 0
"#,
    );
    fs::write(&kill_log, "").expect("create kill log");

    let output = Command::new("bash")
        .arg(repository_root().join("scripts/manage_daemon_launchagent.sh"))
        .arg("install")
        .env("SNOW_RELEASE_INSTALL_DIR", &install_dir)
        .env("SNOW_DAEMON_LAUNCH_AGENTS_DIR", &launch_agents)
        .env("SNOW_DAEMON_LOG_DIR", &log_dir)
        .env("SNOW_LAUNCHCTL_BIN", &launchctl)
        .env("SNOW_PLUTIL_BIN", &plutil)
        .env("SNOW_DAEMON_PS_BIN", &ps)
        .env("SNOW_DAEMON_KILL_BIN", &kill)
        .env("SNOW_DAEMON_OWNER_DRAIN_TIMEOUT_SECS", "0")
        .env("SNOW_TEST_CALL_LOG", &call_log)
        .env("SNOW_TEST_SNOW_BINARY", &snow_binary)
        .env("SNOW_TEST_KILL_LOG", &kill_log)
        .env("SNOW_TEST_OWNER_STATE", &owner_state)
        .output()
        .expect("run daemon service installer");

    assert!(
        output.status.success(),
        "installer should drain matching legacy owners: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let kills = fs::read_to_string(kill_log).expect("kill log");
    assert!(kills.contains("-TERM 123"));
    assert!(!kills.contains("456"));
}

#[test]
fn release_build_delegates_daemon_restart_to_the_service_manager() {
    let temporary = TempDir::new().expect("temporary directory");
    let root = temporary.path();
    let tools = root.join("tools");
    let call_log = root.join("calls.log");
    let install_dir = root.join("install");
    let service_manager = root.join("service-manager");

    write_executable(
        &tools.join("cargo"),
        r#"#!/usr/bin/env bash
set -euo pipefail
printf 'cargo %s\n' "$*" >> "$SNOW_TEST_CALL_LOG"
"#,
    );
    write_executable(
        &tools.join("install"),
        r#"#!/usr/bin/env bash
set -euo pipefail
destination="${!#}"
/bin/mkdir -p "$(/usr/bin/dirname "$destination")"
printf '#!/usr/bin/env bash\nprintf "snow %%s\\n" "$*" >> "$SNOW_TEST_CALL_LOG"\n' > "$destination"
/bin/chmod 755 "$destination"
"#,
    );
    write_executable(
        &tools.join("python3"),
        r#"#!/usr/bin/env bash
set -euo pipefail
printf 'python3 %s\n' "$*" >> "$SNOW_TEST_CALL_LOG"
"#,
    );
    write_executable(
        &service_manager,
        r#"#!/usr/bin/env bash
set -euo pipefail
test -x "$SNOW_RELEASE_INSTALL_DIR/snow"
printf 'service-manager %s\n' "$*" >> "$SNOW_TEST_CALL_LOG"
"#,
    );

    let inherited_path = env::var_os("PATH").expect("PATH is set");
    let mut paths = env::split_paths(&inherited_path).collect::<Vec<_>>();
    paths.insert(0, tools.clone());
    let path = env::join_paths(paths).expect("compose PATH");
    let output = Command::new("bash")
        .arg(repository_root().join("scripts/build_release.sh"))
        .current_dir(repository_root())
        .env("PATH", path)
        .env("SNOW_RELEASE_INSTALL_DIR", &install_dir)
        .env("SNOW_DAEMON_SERVICE_MANAGER", &service_manager)
        .env("SNOW_TEST_CALL_LOG", &call_log)
        .output()
        .expect("run release build script");

    assert!(
        output.status.success(),
        "release build failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let calls = fs::read_to_string(call_log).expect("release call log");
    assert!(calls.contains("service-manager install"));
    assert!(calls.contains("snow daemon status"));
    assert!(calls.contains("--no-auto-spawn"));
    assert!(
        !calls.contains("snow daemon restart"),
        "release build must hand lifecycle ownership to launchd: {calls}"
    );
}
