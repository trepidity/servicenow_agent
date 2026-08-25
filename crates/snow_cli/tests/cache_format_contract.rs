//! L0 CLI cache-format contract.

use std::fs::{self, OpenOptions};
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
use fs2::FileExt;
use rusqlite::Connection;
use snow_core::{
    ResourceType,
    cache::store::{RecordRow, Store, VaultProjectionProvenance},
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
fn cache_info_rejects_pre_catalog_v1_cache_without_promoting_generic_rows() {
    let home = tempfile::tempdir().expect("temporary home");
    let config_dir = home.path().join(".config/snow");
    fs::create_dir_all(&config_dir).expect("config directory");
    let database = config_dir.join("snow.db");
    let connection = Connection::open(&database).expect("v1 database");
    connection
        .execute_batch(
            "CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);\n             INSERT INTO schema_meta(key, value) VALUES ('cache_format', 'snow-cache-v1');",
        )
        .expect("v1 cache marker");
    drop(connection);
    let before = fs::read(&database).expect("v1 database bytes");

    let output = Command::new(env!("CARGO_BIN_EXE_snow"))
        .arg("cache-info")
        .env("HOME", home.path())
        .output()
        .expect("run snow cache-info");

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("utf-8 stdout");
    assert!(
        stdout.contains("Cache Format: incompatible (cache format marker \"snow-cache-v1\")"),
        "stdout: {stdout}"
    );
    assert!(stdout.contains("rebuild-cache"), "stdout: {stdout}");
    assert_eq!(
        fs::read(database).expect("v1 database after cache-info"),
        before
    );
}

#[test]
fn cache_info_rejects_v2_marker_when_typed_catalog_projection_is_absent() {
    let home = tempfile::tempdir().expect("temporary home");
    let config_dir = home.path().join(".config/snow");
    fs::create_dir_all(&config_dir).expect("config directory");
    let database = config_dir.join("snow.db");
    let connection = Connection::open(&database).expect("incomplete v2 database");
    connection
        .execute_batch(
            "CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);\n             INSERT INTO schema_meta(key, value) VALUES ('cache_format', 'snow-cache-v2');",
        )
        .expect("v2 cache marker");
    drop(connection);
    let before = fs::read(&database).expect("incomplete v2 database bytes");

    let output = Command::new(env!("CARGO_BIN_EXE_snow"))
        .arg("cache-info")
        .env("HOME", home.path())
        .output()
        .expect("run snow cache-info");

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).expect("utf-8 stdout");
    assert!(
        stdout.contains(
            "Cache Format: incompatible (snow-cache-v2 missing typed catalog projection)"
        ),
        "stdout: {stdout}"
    );
    assert!(stdout.contains("rebuild-cache"), "stdout: {stdout}");
    assert_eq!(
        fs::read(database).expect("incomplete v2 database after cache-info"),
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

    let stale_cache_artifacts = [
        config_dir.join(".snow.db.reset-11111111-1111-4111-8111-111111111111.tmp"),
        config_dir.join(".snow.db.rebuild-33333333-3333-4333-8333-333333333333.tmp"),
        config_dir.join(".snow.db.servicenow-rebuild-22222222-2222-4222-8222-222222222222.tmp"),
    ]
    .into_iter()
    .flat_map(|database| {
        let mut artifacts = vec![database.clone()];
        for suffix in ["-wal", "-shm", "-journal"] {
            let mut sidecar = database.as_os_str().to_os_string();
            sidecar.push(suffix);
            artifacts.push(sidecar.into());
        }
        artifacts
    })
    .collect::<Vec<_>>();
    for artifact in &stale_cache_artifacts {
        fs::write(artifact, b"stale Snow-owned cache artifact")
            .expect("stale Snow-owned cache artifact");
    }
    let unrelated_temporary = config_dir.join("operator-notes.tmp");
    fs::write(&unrelated_temporary, b"unrelated temporary file").expect("unrelated temporary file");
    let near_match = config_dir.join(".snow.db.servicenow-rebuild-not-a-uuid.tmp");
    fs::write(&near_match, b"unrelated near-match").expect("unrelated near-match temporary file");

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
        "reset-cache\ncache format: snow-cache-v2\nrecords: 0\n"
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
    for artifact in stale_cache_artifacts {
        assert!(
            !artifact.exists(),
            "reset left Snow-owned cache artifact {}",
            artifact.display()
        );
    }
    assert_eq!(
        fs::read(&unrelated_temporary).expect("unrelated temporary file after reset"),
        b"unrelated temporary file"
    );
    assert_eq!(
        fs::read(&near_match).expect("unrelated near-match after reset"),
        b"unrelated near-match"
    );
    let store = Store::open(&database).expect("reset cache");
    assert_eq!(store.count_active_records().expect("record count"), 0);
}

#[test]
fn reset_cache_refuses_while_another_cache_maintenance_command_is_active() {
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

    let maintenance_lock = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(config_dir.join("cache.maintenance.lock"))
        .expect("cache maintenance lock file");
    FileExt::lock_exclusive(&maintenance_lock).expect("hold cache maintenance lock");

    let output = Command::new(env!("CARGO_BIN_EXE_snow"))
        .arg("reset-cache")
        .env("HOME", home.path())
        .output()
        .expect("run snow reset-cache");

    assert!(!output.status.success(), "overlapping reset succeeded");
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("another cache maintenance command is active"),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read(&database).expect("database after rejected reset"),
        before
    );
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
        "CHG0099999",
        "change_request",
        ResourceType::Change,
        Utc.timestamp_opt(1_700_000_000, 0)
            .single()
            .expect("timestamp"),
    );
    store.upsert_record(&stale, "", "").expect("stale row");
    drop(store);

    let server = MockServer::start().await;
    mount_default_empty_table_response(&server).await;
    mount_current_user_response(&server).await;
    let first_page = (0..250)
        .map(|index| change_request_json(index, "first"))
        .collect::<Vec<_>>();
    Mock::given(method("GET"))
        .and(path_regex(r"/api/now/table/change_request"))
        .and(query_param("sysparm_offset", "0"))
        .and(query_param("sysparm_limit", "250"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": first_page
        })))
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(r"/api/now/table/change_request"))
        .and(query_param("sysparm_offset", "250"))
        .and(query_param("sysparm_limit", "250"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [change_request_json(250, "terminal")]
        })))
        .with_priority(1)
        .mount(&server)
        .await;

    write_test_environment(&config_dir, &server.uri());
    let output = run_cache_command_with_args(
        home.path(),
        &[
            "rebuild-cache",
            "--page-limit",
            "250",
            "--timeout-seconds",
            "30",
        ],
    );

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8(output.stdout).expect("utf-8 stdout");
    assert!(stdout.contains("source: ServiceNow"), "stdout: {stdout}");
    assert!(stdout.contains("records: 251"), "stdout: {stdout}");
    assert!(stdout.contains("complete: true"), "stdout: {stdout}");
    assert!(
        stdout
            .contains("table: resource=change servicenow_table=change_request pages=2 records=251"),
        "stdout: {stdout}"
    );
    let stderr = String::from_utf8(output.stderr).expect("utf-8 stderr");
    assert!(
        stderr.contains("[1/3] change (change_request): requesting page=1"),
        "stderr: {stderr}"
    );
    assert!(
        stderr.contains("[1/3] change (change_request): requesting page=2"),
        "stderr: {stderr}"
    );
    let store = Store::open(&database).expect("rebuilt cache");
    assert_eq!(store.count_active_records().expect("record count"), 251);
    assert!(
        store
            .get_record_by_sys_id("000000000000000000000000000000fa")
            .expect("terminal live record lookup")
            .is_some(),
        "terminal page record was not projected"
    );
    let requests = server.received_requests().await.expect("requests");
    assert!(
        requests
            .iter()
            .all(|request| request.url.path() != "/api/now/table/cmdb_ci_server"),
        "default rebuild requested servers: {requests:?}"
    );
    assert_eq!(
        fs::read_to_string(&malformed_vault).expect("vault document after rebuild"),
        "not a supported vault document"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rebuild_cache_defaults_never_read_or_project_work_records() {
    let home = tempfile::tempdir().expect("temporary home");
    let config_dir = home.path().join(".config/snow");
    fs::create_dir_all(config_dir.join("vault")).expect("vault directory");
    let server = MockServer::start().await;
    mount_default_empty_table_response(&server).await;
    fs::write(
        config_dir.join(".env.test"),
        format!(
            "SERVICENOW_INSTANCE={}\nSERVICENOW_USERNAME=user@example.com\nSERVICENOW_PASSWORD=test-password\nSNOW_ALLOW_LOOPBACK_HTTP=true\n",
            server.uri()
        ),
    )
    .expect("test environment");

    let output = run_cache_command(home.path(), "rebuild-cache");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let requests = server.received_requests().await.expect("requests");
    assert!(
        requests.iter().all(|request| {
            !matches!(
                request.url.path(),
                "/api/now/table/incident" | "/api/now/table/change_request"
            )
        }),
        "default rebuild performed work-record I/O: {requests:?}"
    );
    let store = Store::open(config_dir.join("snow.db")).expect("rebuilt cache");
    assert_eq!(store.count_active_records().expect("record count"), 0);
}

#[tokio::test(flavor = "multi_thread")]
async fn rebuild_cache_without_knowledge_scope_performs_zero_knowledge_io() {
    let home = tempfile::tempdir().expect("temporary home");
    let config_dir = home.path().join(".config/snow");
    fs::create_dir_all(config_dir.join("vault")).expect("vault directory");
    let server = MockServer::start().await;
    mount_default_empty_table_response(&server).await;
    fs::write(
        config_dir.join(".env.test"),
        format!(
            "SERVICENOW_INSTANCE={}\nSERVICENOW_USERNAME=user@example.com\nSERVICENOW_PASSWORD=test-password\nSNOW_ALLOW_LOOPBACK_HTTP=true\n",
            server.uri()
        ),
    )
    .expect("test environment");

    let output = run_cache_command(home.path(), "rebuild-cache");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let requests = server.received_requests().await.expect("requests");
    assert!(
        requests
            .iter()
            .all(|request| request.url.path() != "/api/now/table/kb_knowledge"),
        "unscoped rebuild performed Knowledge I/O: {requests:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rebuild_cache_projects_only_the_policy_selected_knowledge_base() {
    const SELECTED_BASE: &str = "11111111111111111111111111111111";
    const OTHER_BASE: &str = "22222222222222222222222222222222";
    const SELECTED_ARTICLE: &str = "33333333333333333333333333333333";
    const OTHER_ARTICLE: &str = "44444444444444444444444444444444";

    let home = tempfile::tempdir().expect("temporary home");
    let config_dir = home.path().join(".config/snow");
    fs::create_dir_all(config_dir.join("vault")).expect("vault directory");
    fs::write(
        config_dir.join("cache-policy.toml"),
        format!("version = 1\n[rebuild.knowledge]\nknowledge_base_sys_id = \"{SELECTED_BASE}\"\n"),
    )
    .expect("cache policy");

    let server = MockServer::start().await;
    mount_default_empty_table_response(&server).await;
    Mock::given(method("GET"))
        .and(path_regex(r"/api/now/table/kb_knowledge"))
        .and(query_param(
            "sysparm_query",
            format!("kb_knowledge_base={SELECTED_BASE}^ORDERBYsys_id"),
        ))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [knowledge_json(
                SELECTED_ARTICLE,
                "KB0000001",
                SELECTED_BASE,
                "Example IT Knowledge"
            )]
        })))
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(r"/api/now/table/kb_knowledge"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [
                knowledge_json(
                    SELECTED_ARTICLE,
                    "KB0000001",
                    SELECTED_BASE,
                    "Example IT Knowledge"
                ),
                knowledge_json(
                    OTHER_ARTICLE,
                    "KB0000002",
                    OTHER_BASE,
                    "Example Other Knowledge"
                )
            ]
        })))
        .with_priority(10)
        .mount(&server)
        .await;
    fs::write(
        config_dir.join(".env.test"),
        format!(
            "SERVICENOW_INSTANCE={}\nSERVICENOW_USERNAME=user@example.com\nSERVICENOW_PASSWORD=test-password\nSNOW_ALLOW_LOOPBACK_HTTP=true\n",
            server.uri()
        ),
    )
    .expect("test environment");

    let output = run_cache_command(home.path(), "rebuild-cache");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let requests = server.received_requests().await.expect("requests");
    let knowledge_requests = requests
        .iter()
        .filter(|request| request.url.path() == "/api/now/table/kb_knowledge")
        .collect::<Vec<_>>();
    assert_eq!(knowledge_requests.len(), 1, "requests: {requests:?}");
    assert!(
        knowledge_requests[0].url.query_pairs().any(|(key, value)| {
            key == "sysparm_query"
                && value == format!("kb_knowledge_base={SELECTED_BASE}^ORDERBYsys_id")
        }),
        "Knowledge request was not base-scoped: {:?}",
        knowledge_requests[0]
    );
    assert!(
        knowledge_requests[0]
            .url
            .query_pairs()
            .any(|(key, value)| key == "sysparm_limit" && value == "500"),
        "Knowledge request did not use 500-record rebuild pages: {:?}",
        knowledge_requests[0]
    );

    let store = Store::open(config_dir.join("snow.db")).expect("rebuilt cache");
    assert_eq!(store.count_active_records().expect("record count"), 1);
    assert!(
        store
            .get_record_by_sys_id(SELECTED_ARTICLE)
            .expect("selected lookup")
            .is_some()
    );
    assert!(
        store
            .get_record_by_sys_id(OTHER_ARTICLE)
            .expect("other lookup")
            .is_none(),
        "row from another Knowledge base entered the rebuilt cache"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn rebuild_cache_command_scope_overrides_the_policy_knowledge_base_and_adds_category() {
    const POLICY_BASE: &str = "11111111111111111111111111111111";
    const COMMAND_BASE: &str = "22222222222222222222222222222222";
    const COMMAND_CATEGORY: &str = "33333333333333333333333333333333";

    let home = tempfile::tempdir().expect("temporary home");
    let config_dir = home.path().join(".config/snow");
    fs::create_dir_all(config_dir.join("vault")).expect("vault directory");
    fs::write(
        config_dir.join("cache-policy.toml"),
        format!("version = 1\n[rebuild.knowledge]\nknowledge_base_sys_id = \"{POLICY_BASE}\"\n"),
    )
    .expect("cache policy");
    let server = MockServer::start().await;
    mount_default_empty_table_response(&server).await;
    fs::write(
        config_dir.join(".env.test"),
        format!(
            "SERVICENOW_INSTANCE={}\nSERVICENOW_USERNAME=user@example.com\nSERVICENOW_PASSWORD=test-password\nSNOW_ALLOW_LOOPBACK_HTTP=true\n",
            server.uri()
        ),
    )
    .expect("test environment");

    let output = run_cache_command_with_args(
        home.path(),
        &[
            "rebuild-cache",
            "--knowledge-base",
            COMMAND_BASE,
            "--knowledge-category",
            COMMAND_CATEGORY,
        ],
    );

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let requests = server.received_requests().await.expect("requests");
    let knowledge_requests = requests
        .iter()
        .filter(|request| request.url.path() == "/api/now/table/kb_knowledge")
        .collect::<Vec<_>>();
    assert_eq!(knowledge_requests.len(), 1, "requests: {requests:?}");
    let query = knowledge_requests[0]
        .url
        .query_pairs()
        .find(|(key, _)| key == "sysparm_query")
        .map(|(_, value)| value.into_owned())
        .expect("knowledge query");
    assert!(
        query.contains(&format!("kb_knowledge_base={COMMAND_BASE}")),
        "command base was not used: {query}"
    );
    assert!(
        query.contains(&format!("kb_category={COMMAND_CATEGORY}")),
        "command category was not used: {query}"
    );
    assert!(
        !query.contains(POLICY_BASE),
        "policy base was not overridden: {query}"
    );
}

#[test]
fn rebuild_cache_rejects_a_category_without_a_knowledge_base() {
    let home = tempfile::tempdir().expect("temporary home");
    let output = run_cache_command_with_args(
        home.path(),
        &[
            "rebuild-cache",
            "--knowledge-category",
            "33333333333333333333333333333333",
        ],
    );

    assert!(!output.status.success(), "rebuild unexpectedly started");
    let stderr = String::from_utf8(output.stderr).expect("utf-8 stderr");
    assert!(stderr.contains("--knowledge-base"), "stderr: {stderr}");
}

#[tokio::test(flavor = "multi_thread")]
async fn rebuild_cache_projects_catalog_rows_only_as_timestamped_narrowed_products() {
    let home = tempfile::tempdir().expect("temporary home");
    let config_dir = home.path().join(".config/snow");
    fs::create_dir_all(config_dir.join("vault")).expect("vault directory");
    let server = MockServer::start().await;
    mount_default_empty_table_response(&server).await;
    Mock::given(method("GET"))
        .and(path_regex(r"/api/now/table/sc_cat_item"))
        .and(query_param("sysparm_offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [{
                "sys_id": "300d473b13f00c10906630128144b0d1",
                "name": "Example Access Request",
                "short_description": "Request example access",
                "sys_class_name": "sc_cat_item",
                "active": "true",
                "sys_updated_on": "2026-08-20 12:00:00"
            }]
        })))
        .with_priority(1)
        .mount(&server)
        .await;
    fs::write(
        config_dir.join(".env.test"),
        format!(
            "SERVICENOW_INSTANCE={}\nSERVICENOW_USERNAME=user@example.com\nSERVICENOW_PASSWORD=test-password\nSNOW_ALLOW_LOOPBACK_HTTP=true\n",
            server.uri()
        ),
    )
    .expect("test environment");

    let output = run_cache_command(home.path(), "rebuild-cache");
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let store = Store::open(config_dir.join("snow.db")).expect("rebuilt cache");
    assert!(
        store
            .get_complete_catalog_product("300d473b13f00c10906630128144b0d1")
            .expect("complete lookup")
            .is_none(),
        "narrowed rebuild row must never be promoted to a complete product"
    );
    let narrowed = store
        .search_narrowed_catalog_products("Access", 10)
        .expect("narrowed catalog search");
    assert_eq!(narrowed.len(), 1);
    assert_eq!(narrowed[0].item.name, "Example Access Request");
    assert!(narrowed[0].item.variables.is_empty());
    assert!(narrowed[0].last_refreshed_at <= Utc::now());
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
        .and(path_regex(r"/api/now/table/change_request"))
        .and(query_param("sysparm_offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [change_request_json(0, "streaming")]
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
                    if line.contains("[1/3] change (change_request): page=1 page_records=1") {
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
async fn rebuild_cache_applies_the_per_request_timeout_override() {
    let home = tempfile::tempdir().expect("temporary home");
    let config_dir = home.path().join(".config/snow");
    fs::create_dir_all(config_dir.join("vault")).expect("vault directory");
    let server = MockServer::start().await;
    mount_default_empty_table_response(&server).await;
    mount_current_user_response(&server).await;
    Mock::given(method("GET"))
        .and(path_regex(r"/api/now/table/change_request"))
        .and(query_param("sysparm_offset", "0"))
        .respond_with(
            ResponseTemplate::new(200)
                .set_body_json(serde_json::json!({ "result": [] }))
                .set_delay(Duration::from_secs(2)),
        )
        .with_priority(1)
        .mount(&server)
        .await;
    write_test_environment(&config_dir, &server.uri());

    let output =
        run_cache_command_with_args(home.path(), &["rebuild-cache", "--timeout-seconds", "1"]);

    assert!(!output.status.success(), "rebuild unexpectedly succeeded");
    let stderr = String::from_utf8(output.stderr).expect("utf-8 stderr");
    assert!(stderr.contains("operation timed out"), "stderr: {stderr}");
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
        .and(path_regex(r"/api/now/table/change_request"))
        .and(query_param("sysparm_offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [change_request_json_with_work_notes(0, "unicode")]
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
        "CHG0099998",
        "change_request",
        ResourceType::Change,
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
    let first_page = (0..1_000)
        .map(|index| change_request_json(index, "first"))
        .collect::<Vec<_>>();
    Mock::given(method("GET"))
        .and(path_regex(r"/api/now/table/change_request"))
        .and(query_param("sysparm_offset", "0"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": first_page
        })))
        .with_priority(1)
        .mount(&server)
        .await;
    Mock::given(method("GET"))
        .and(path_regex(r"/api/now/table/change_request"))
        .and(query_param("sysparm_offset", "1000"))
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
        stderr.contains("[1/3] change (change_request): requesting page=2"),
        "stderr: {stderr}"
    );
    assert!(
        stderr.contains("rebuild-cache: failed during change (change_request) page=2"),
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

#[test]
fn adopt_cache_only_projection_preserves_rows_and_marks_legacy_no_vault_records() {
    let home = tempfile::tempdir().expect("temporary home");
    let config_dir = home.path().join(".config/snow");
    fs::create_dir_all(&config_dir).expect("config directory");
    let database = config_dir.join("snow.db");
    let connection = Connection::open(&database).expect("pre-provenance cache");
    connection
        .execute_batch(
            "CREATE TABLE schema_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);\n             INSERT INTO schema_meta(key, value) VALUES ('cache_format', 'snow-cache-v2');\n             CREATE TABLE catalog_products_complete (id TEXT);\n             CREATE TABLE catalog_products_narrowed (id TEXT);\n             CREATE TABLE records (\n                 sys_id TEXT PRIMARY KEY,\n                 number TEXT NOT NULL,\n                 file_path TEXT,\n                 in_scope INTEGER NOT NULL,\n                 pruned_at INTEGER\n             );\n             INSERT INTO records(sys_id, number, file_path, in_scope, pruned_at)\n             VALUES ('55555555555555555555555555555555', 'KB0012345', NULL, 1, NULL);",
        )
        .expect("pre-provenance cache schema");
    drop(connection);

    let output =
        run_cache_command_with_args(home.path(), &["adopt-cache-only-projection", "--yes"]);

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("adopted cache-only records: 1"),
        "stdout: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    let store = Store::open(&database).expect("adopted cache");
    assert_eq!(store.count_active_records().expect("record count"), 1);
    assert_eq!(
        store
            .active_record_vault_provenance()
            .expect("provenance")
            .get("55555555555555555555555555555555"),
        Some(&VaultProjectionProvenance::CacheOnly)
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
    run_cache_command_with_args(home, &[command])
}

fn run_cache_command_with_args(home: &Path, args: &[&str]) -> std::process::Output {
    Command::new(env!("CARGO_BIN_EXE_snow"))
        .args(args)
        .env("HOME", home)
        .env("SNOW_ENV", "test")
        .output()
        .unwrap_or_else(|error| panic!("run snow {}: {error}", args.join(" ")))
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
        config_dir.join("cache-policy.toml"),
        "version = 1\n[objects.change_request]\nmode = \"read_through\"\nttl = \"1h\"\n",
    )
    .expect("cache policy");
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

fn change_request_json(index: usize, description: &str) -> serde_json::Value {
    serde_json::json!({
        "sys_id": format!("{index:032x}"),
        "number": format!("CHG{index:07}"),
        "short_description": format!("Generic {description} change {index}"),
        "description": "Public-safe test record",
        "state": "2",
        "active": "true",
        "assigned_to": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "sys_updated_on": "2026-08-19 12:00:00"
    })
}

fn change_request_json_with_work_notes(index: usize, description: &str) -> serde_json::Value {
    let mut record = change_request_json(index, description);
    record["work_notes"] =
        serde_json::json!("Escalation note x — generic public-safe follow-up for the queue");
    record
}

fn knowledge_json(
    sys_id: &str,
    number: &str,
    knowledge_base_sys_id: &str,
    knowledge_base_display: &str,
) -> serde_json::Value {
    serde_json::json!({
        "sys_id": sys_id,
        "number": number,
        "short_description": "Generic knowledge article",
        "text": "Public-safe example knowledge body",
        "workflow_state": "published",
        "knowledge_base": {
            "value": knowledge_base_sys_id,
            "display_value": knowledge_base_display
        },
        "sys_updated_on": "2026-08-21 12:00:00"
    })
}
