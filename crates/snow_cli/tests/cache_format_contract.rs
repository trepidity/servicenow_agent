//! L0 CLI cache-format contract.

use std::fs;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::Duration;

#[cfg(unix)]
use std::io::ErrorKind;
#[cfg(unix)]
use std::os::unix::net::UnixListener;
#[cfg(unix)]
use std::time::Instant;

use chrono::{TimeZone, Utc};
use rusqlite::Connection;
use snow_core::{
    ResourceType,
    cache::store::{RecordRow, Store},
};
use wiremock::matchers::{method, path_regex, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

#[test]
fn cache_info_reports_incompatible_cache_without_mutating_it() {
    let home = tempfile::tempdir().expect("temporary home");
    let config_dir = home.path().join(".config/snow");
    fs::create_dir_all(&config_dir).expect("config directory");
    let database = config_dir.join("snow.db");
    let connection = Connection::open(&database).expect("legacy database");
    connection
        .execute_batch(
            "CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);\n             INSERT INTO schema_meta(key, value) VALUES ('schema_version', '2');",
        )
        .expect("legacy schema marker");
    drop(connection);
    let before = fs::read(&database).expect("legacy database bytes");

    let output = Command::new(env!("CARGO_BIN_EXE_snow"))
        .arg("cache-info")
        .env("HOME", home.path())
        .output()
        .expect("run snow cache-info");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("utf-8 stdout");
    assert!(
        stdout.contains("Cache Format: incompatible"),
        "stdout: {stdout}"
    );
    assert!(stdout.contains("rebuild-cache"), "stdout: {stdout}");
    assert_eq!(
        fs::read(&database).expect("database bytes after cache-info"),
        before
    );
}

#[test]
fn reset_cache_replaces_an_incompatible_cache_without_reading_the_vault() {
    let home = tempfile::tempdir().expect("temporary home");
    let config_dir = home.path().join(".config/snow");
    let vault = config_dir.join("vault");
    fs::create_dir_all(&vault).expect("vault directory");
    let invalid_vault_document = b"not a supported vault document";
    let vault_document = vault.join("invalid.md");
    fs::write(&vault_document, invalid_vault_document).expect("invalid vault document");

    let database = config_dir.join("snow.db");
    let connection = Connection::open(&database).expect("legacy database");
    connection
        .execute_batch(
            "CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);\n             INSERT INTO schema_meta(key, value) VALUES ('schema_version', '11');",
        )
        .expect("legacy schema marker");
    drop(connection);
    let wal = config_dir.join("snow.db-wal");
    let shared_memory = config_dir.join("snow.db-shm");
    let journal = config_dir.join("snow.db-journal");
    fs::write(&wal, b"stale write-ahead log").expect("stale WAL");
    fs::write(&shared_memory, b"stale shared memory").expect("stale shared memory");
    fs::write(&journal, b"stale rollback journal").expect("stale rollback journal");

    let output = Command::new(env!("CARGO_BIN_EXE_snow"))
        .arg("reset-cache")
        .env("HOME", home.path())
        .output()
        .expect("run snow reset-cache");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8(output.stdout).expect("utf-8 stdout"),
        "reset-cache\ncache format: snow-cache-v1\nrecords: 0\n"
    );
    assert_eq!(
        fs::read(&vault_document).expect("vault document after reset"),
        invalid_vault_document
    );
    assert!(!wal.exists(), "reset must remove the prior cache WAL");
    assert!(
        !shared_memory.exists(),
        "reset must remove the prior cache shared-memory file"
    );
    assert!(
        !journal.exists(),
        "reset must remove the prior cache journal"
    );
    let store = Store::open(&database).expect("reset cache");
    assert_eq!(store.count_active_records().expect("record count"), 0);
}

#[cfg(unix)]
#[test]
fn reset_cache_refuses_to_replace_a_cache_while_the_daemon_is_reachable() {
    let home = tempfile::tempdir().expect("temporary home");
    let config_dir = home.path().join(".config/snow");
    fs::create_dir_all(&config_dir).expect("config directory");
    let database = config_dir.join("snow.db");
    let connection = Connection::open(&database).expect("legacy database");
    connection
        .execute_batch(
            "CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);\n             INSERT INTO schema_meta(key, value) VALUES ('schema_version', '11');",
        )
        .expect("legacy schema marker");
    drop(connection);
    let before = fs::read(&database).expect("legacy database bytes");
    let listener = bind_accepting_socket(&config_dir.join("daemon.sock"));

    let output = Command::new(env!("CARGO_BIN_EXE_snow"))
        .arg("reset-cache")
        .env("HOME", home.path())
        .output()
        .expect("run snow reset-cache");

    listener.join().expect("daemon endpoint probe");
    assert!(!output.status.success(), "reset unexpectedly succeeded");
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("daemon is running; stop it before resetting the cache"),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read(&database).expect("database after rejected reset"),
        before
    );
}

#[cfg(unix)]
fn bind_accepting_socket(path: &Path) -> thread::JoinHandle<()> {
    let listener = UnixListener::bind(path).expect("bind daemon socket");
    listener
        .set_nonblocking(true)
        .expect("nonblocking daemon socket");
    thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            match listener.accept() {
                Ok((_stream, _addr)) => return,
                Err(error) if error.kind() == ErrorKind::WouldBlock => {
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

#[tokio::test(flavor = "multi_thread")]
async fn rebuild_cache_drains_servicenow_pages_without_reading_the_vault() {
    let home = tempfile::tempdir().expect("temporary home");
    let config_dir = home.path().join(".config/snow");
    let vault = config_dir.join("vault");
    fs::create_dir_all(&vault).expect("vault directory");
    let malformed_vault = vault.join("malformed.md");
    fs::write(&malformed_vault, "not a supported vault document")
        .expect("malformed vault document");
    let database = config_dir.join("snow.db");
    let store = Store::open(&database).expect("current cache");
    let stale = RecordRow::active(
        "22222222222222222222222222222222",
        "INC0099999",
        "incident",
        ResourceType::Incident,
        Utc.timestamp_opt(1_700_000_000, 0)
            .single()
            .expect("timestamp"),
    );
    store.upsert_record(&stale, "", "").expect("stale row");
    drop(store);

    let server = MockServer::start().await;
    mount_default_empty_table_response(&server).await;
    mount_current_user_response(&server).await;
    let first_page = (0..100)
        .map(|index| incident_json(index, "first"))
        .collect::<Vec<_>>();
    Mock::given(method("GET"))
        .and(path_regex(r"/api/now/table/incident"))
        .and(query_param("sysparm_offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": first_page
        })))
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(r"/api/now/table/change_request"))
        .and(query_param("sysparm_offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [incident_json(101, "short terminal")]
        })))
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(r"/api/now/table/incident"))
        .and(query_param("sysparm_offset", "100"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [incident_json(100, "terminal")]
        })))
        .with_priority(1)
        .mount(&server)
        .await;

    write_test_environment(&config_dir, &server.uri());
    let output = run_cache_command(home.path(), "rebuild-cache");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("utf-8 stdout");
    assert!(stdout.contains("source: ServiceNow"), "stdout: {stdout}");
    assert!(stdout.contains("records: 102"), "stdout: {stdout}");
    assert!(stdout.contains("complete: true"), "stdout: {stdout}");
    assert!(
        stdout.contains("table: resource=incident servicenow_table=incident pages=2 records=101"),
        "stdout: {stdout}"
    );
    assert!(
        stdout.contains("table: resource=change servicenow_table=change_request pages=1 records=1"),
        "stdout: {stdout}"
    );
    let stderr = String::from_utf8(output.stderr).expect("utf-8 stderr");
    assert!(
        stderr.contains("[1/12] incident (incident): requesting page=1"),
        "stderr: {stderr}"
    );
    assert!(
        stderr.contains("[1/12] incident (incident): requesting page=2"),
        "stderr: {stderr}"
    );
    assert!(
        stderr.contains("[2/12] change (change_request): requesting page=1"),
        "stderr: {stderr}"
    );
    assert!(
        !stderr.contains("[2/12] change (change_request): requesting page=2"),
        "short terminal page must not create a synthetic request: {stderr}"
    );
    assert!(
        stderr.contains("[3/12] change_task (change_task): requesting page=1"),
        "stderr: {stderr}"
    );
    assert!(
        stderr.contains("[3/12] change_task (change_task): complete pages=0 records=0"),
        "stderr: {stderr}"
    );
    let store = Store::open(&database).expect("rebuilt cache");
    assert_eq!(store.count_active_records().expect("record count"), 102);
    assert!(
        store
            .get_record_by_sys_id("00000000000000000000000000000064")
            .expect("terminal live record lookup")
            .is_some(),
        "terminal page record was not projected"
    );
    assert_eq!(
        fs::read_to_string(&malformed_vault).expect("vault document after rebuild"),
        "not a supported vault document"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rebuild_cache_streams_a_projected_page_before_a_delayed_final_table_response() {
    let home = tempfile::tempdir().expect("temporary home");
    let config_dir = home.path().join(".config/snow");
    fs::create_dir_all(config_dir.join("vault")).expect("vault directory");
    let server = MockServer::start().await;
    mount_default_empty_table_response(&server).await;
    mount_current_user_response(&server).await;
    Mock::given(method("GET"))
        .and(path_regex(r"/api/now/table/incident"))
        .and(query_param("sysparm_offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [incident_json(0, "streaming")]
        })))
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(r"/api/now/table/cmdb_ci_business_app"))
        .and(query_param("sysparm_offset", "0"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({ "result": [] }))
                .set_delay(Duration::from_secs(30)),
        )
        .with_priority(1)
        .mount(&server)
        .await;
    write_test_environment(&config_dir, &server.uri());

    let mut child = spawn_cache_command(home.path(), "rebuild-cache");
    let stderr = child.stderr.take().expect("piped stderr");
    let (observation_tx, observation_rx) = mpsc::channel();
    let reader = thread::spawn(move || {
        let mut reader = BufReader::new(stderr);
        let mut captured = String::new();
        loop {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => {
                    let _ = observation_tx.send(Err(captured));
                    return;
                }
                Ok(_) => {
                    if line.contains("[1/12] incident (incident): page=1 page_records=1") {
                        let _ = observation_tx.send(Ok(()));
                        return;
                    }
                    captured.push_str(&line);
                }
                Err(error) => {
                    let _ = observation_tx.send(Err(format!("{captured}{error}")));
                    return;
                }
            }
        }
    });
    let observation = observation_rx.recv_timeout(Duration::from_secs(10));
    let still_running = child.try_wait().expect("inspect child status").is_none();
    let _ = child.kill();
    let _ = child.wait();
    reader.join().expect("join stderr reader");

    assert_eq!(
        observation,
        Ok(Ok(())),
        "expected projected-page progress before terminal response"
    );
    assert!(
        still_running,
        "rebuild exited before the delayed response completed"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rebuild_cache_projects_unicode_work_notes_without_panicking() {
    let home = tempfile::tempdir().expect("temporary home");
    let config_dir = home.path().join(".config/snow");
    fs::create_dir_all(config_dir.join("vault")).expect("vault directory");
    let server = MockServer::start().await;
    mount_default_empty_table_response(&server).await;
    mount_current_user_response(&server).await;
    Mock::given(method("GET"))
        .and(path_regex(r"/api/now/table/incident"))
        .and(query_param("sysparm_offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [incident_json_with_work_notes(0, "unicode")]
        })))
        .with_priority(1)
        .mount(&server)
        .await;
    write_test_environment(&config_dir, &server.uri());

    let output = run_cache_command(home.path(), "rebuild-cache");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let database = config_dir.join("snow.db");
    let store = Store::open(&database).expect("rebuilt cache");
    assert_eq!(store.count_active_records().expect("record count"), 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn rebuild_cache_preserves_current_cache_when_a_later_servicenow_page_fails() {
    let home = tempfile::tempdir().expect("temporary home");
    let config_dir = home.path().join(".config/snow");
    fs::create_dir_all(config_dir.join("vault")).expect("vault directory");

    let database = config_dir.join("snow.db");
    let store = Store::open(&database).expect("current cache");
    let retained = RecordRow::active(
        "33333333333333333333333333333333",
        "INC0099998",
        "incident",
        ResourceType::Incident,
        Utc.timestamp_opt(1_700_000_000, 0)
            .single()
            .expect("timestamp"),
    );
    store
        .upsert_record(&retained, "", "")
        .expect("retained row");
    drop(store);
    let before = fs::read(&database).expect("current cache bytes");

    let server = MockServer::start().await;
    mount_default_empty_table_response(&server).await;
    mount_current_user_response(&server).await;
    let first_page = (0..100)
        .map(|index| incident_json(index, "first"))
        .collect::<Vec<_>>();
    Mock::given(method("GET"))
        .and(path_regex(r"/api/now/table/incident"))
        .and(query_param("sysparm_offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": first_page
        })))
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(r"/api/now/table/incident"))
        .and(query_param("sysparm_offset", "100"))
        .respond_with(ResponseTemplate::new(503).set_body_json(serde_json::json!({
            "error": {"message": "temporary upstream failure"}
        })))
        .with_priority(1)
        .mount(&server)
        .await;

    write_test_environment(&config_dir, &server.uri());
    let output = run_cache_command(home.path(), "rebuild-cache");

    assert!(
        !output.status.success(),
        "stdout: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let stderr = String::from_utf8(output.stderr).expect("utf-8 stderr");
    assert!(
        stderr.contains("temporary upstream failure"),
        "stderr: {stderr}"
    );
    assert!(
        stderr.contains("[1/12] incident (incident): requesting page=2"),
        "stderr: {stderr}"
    );
    assert!(
        stderr.contains("rebuild-cache: failed during incident (incident) page=2"),
        "stderr: {stderr}"
    );
    assert!(
        stderr.contains("rebuild-cache: staging cache removed; current cache unchanged"),
        "stderr: {stderr}"
    );
    assert_eq!(
        fs::read(&database).expect("current cache bytes after failed rebuild"),
        before
    );
    let store = Store::open(&database).expect("preserved current cache");
    assert_eq!(store.count_active_records().expect("record count"), 1);
    let staging_artifacts = fs::read_dir(&config_dir)
        .expect("config directory")
        .filter_map(Result::ok)
        .filter(|entry| {
            entry
                .file_name()
                .to_string_lossy()
                .contains(".snow.db.servicenow-rebuild-")
        })
        .count();
    assert_eq!(staging_artifacts, 0, "staging cache was not removed");
}

#[test]
fn import_cache_from_vault_keeps_the_old_authority_explicit() {
    let home = tempfile::tempdir().expect("temporary home");
    let config_dir = home.path().join(".config/snow");
    let vault = config_dir.join("vault");
    fs::create_dir_all(&vault).expect("vault directory");
    fs::write(vault.join("invalid.md"), "not a supported vault document")
        .expect("invalid vault document");

    let output = run_cache_command(home.path(), "import-cache-from-vault");

    assert!(
        !output.status.success(),
        "vault import unexpectedly succeeded"
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("failed to parse vault markdown"),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[cfg(unix)]
#[test]
fn rebuild_cache_refuses_to_replace_a_cache_while_the_daemon_is_reachable() {
    let home = tempfile::tempdir().expect("temporary home");
    let config_dir = home.path().join(".config/snow");
    fs::create_dir_all(config_dir.join("vault")).expect("vault directory");
    let database = config_dir.join("snow.db");
    let store = Store::open(&database).expect("current cache");
    let retained = RecordRow::active(
        "44444444444444444444444444444444",
        "INC0099997",
        "incident",
        ResourceType::Incident,
        Utc.timestamp_opt(1_700_000_000, 0)
            .single()
            .expect("timestamp"),
    );
    store
        .upsert_record(&retained, "", "")
        .expect("retained row");
    drop(store);
    let before = fs::read(&database).expect("current cache bytes");
    let listener = bind_accepting_socket(&config_dir.join("daemon.sock"));

    let output = run_cache_command(home.path(), "rebuild-cache");

    listener.join().expect("daemon endpoint probe");
    assert!(!output.status.success(), "rebuild unexpectedly succeeded");
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("daemon is running; stop it before rebuilding the cache"),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read(&database).expect("database after rejection"),
        before
    );
}

fn run_cache_command(home: &Path, command: &str) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_snow"))
        .arg(command)
        .env("HOME", home)
        .env("SNOW_ENV", "test")
        .output()
        .unwrap_or_else(|error| panic!("run snow {command}: {error}"))
}

fn spawn_cache_command(home: &Path, command: &str) -> std::process::Child {
    Command::new(env!("CARGO_BIN_EXE_snow"))
        .arg(command)
        .env("HOME", home)
        .env("SNOW_ENV", "test")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap_or_else(|error| panic!("spawn snow {command}: {error}"))
}

fn write_test_environment(config_dir: &Path, instance: &str) {
    fs::write(
        config_dir.join(".env.test"),
        format!(
            "SERVICENOW_INSTANCE={instance}\nSERVICENOW_USERNAME=user@example.com\nSERVICENOW_PASSWORD=test-password\nSNOW_ALLOW_LOOPBACK_HTTP=true\n"
        ),
    )
    .expect("test environment");
}

async fn mount_default_empty_table_response(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path_regex(r"/api/now/table/.*"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": []
        })))
        .with_priority(100)
        .mount(server)
        .await;
}

async fn mount_current_user_response(server: &MockServer) {
    Mock::given(method("GET"))
        .and(path_regex(r"/api/now/table/sys_user"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [{
                "sys_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                "user_name": "user@example.com"
            }]
        })))
        .with_priority(1)
        .mount(server)
        .await;
}

fn incident_json(index: usize, description: &str) -> serde_json::Value {
    serde_json::json!({
        "sys_id": format!("{index:032x}"),
        "number": format!("INC{index:07}"),
        "short_description": format!("Generic {description} incident {index}"),
        "description": "Public-safe test record",
        "state": "2",
        "active": "true",
        "assigned_to": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "sys_updated_on": "2026-08-19 12:00:00"
    })
}

fn incident_json_with_work_notes(index: usize, description: &str) -> serde_json::Value {
    let mut record = incident_json(index, description);
    record["work_notes"] =
        serde_json::json!("Escalation note x — generic public-safe follow-up for the queue");
    record
}
