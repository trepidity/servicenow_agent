//! L0 CLI cache-format contract.

use std::fs;
use std::process::Command;

use chrono::{TimeZone, Utc};
use rusqlite::Connection;
use snow_core::{
    ResourceType,
    cache::store::{RecordRow, Store},
};

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
fn rebuild_cache_replaces_a_current_cache_and_removes_stale_rows() {
    let home = tempfile::tempdir().expect("temporary home");
    let config_dir = home.path().join(".config/snow");
    fs::create_dir_all(config_dir.join("vault")).expect("vault directory");
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

    let output = Command::new(env!("CARGO_BIN_EXE_snow"))
        .arg("rebuild-cache")
        .env("HOME", home.path())
        .output()
        .expect("run snow rebuild-cache");

    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let store = Store::open(&database).expect("rebuilt cache");
    assert_eq!(store.count_active_records().expect("record count"), 0);
}

#[test]
fn rebuild_cache_preserves_current_cache_when_vault_rebuild_fails() {
    let home = tempfile::tempdir().expect("temporary home");
    let config_dir = home.path().join(".config/snow");
    let vault = config_dir.join("vault");
    fs::create_dir_all(&vault).expect("vault directory");
    fs::write(vault.join("invalid.md"), "not a supported vault document")
        .expect("invalid vault document");

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

    let output = Command::new(env!("CARGO_BIN_EXE_snow"))
        .arg("rebuild-cache")
        .env("HOME", home.path())
        .output()
        .expect("run snow rebuild-cache");

    assert!(
        !output.status.success(),
        "stdout: {}",
        String::from_utf8_lossy(&output.stdout)
    );
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("failed to parse vault markdown"),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        fs::read(&database).expect("current cache bytes after failed rebuild"),
        before
    );
    let store = Store::open(&database).expect("preserved current cache");
    assert_eq!(store.count_active_records().expect("record count"), 1);
}
