use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Result;
use chrono::Utc;
use serde_json::Value;
use servicenow_rs::prelude::{BasicAuth, ServiceNowClient};
use snow_core::CredentialProvider;
use snow_core::cache::store::{KnowledgeArticleRow, RecordRow};
use snow_core::config::{
    DaemonConfig as CoreDaemonConfig, InstanceConfig, SnowConfig, VaultConfig,
};
use snow_core::query::QueryEngine;
use snow_core::{ResourceType, SnowCore};
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::DaemonState;

pub struct FixtureState {
    _guard: tokio::sync::MutexGuard<'static, ()>,
    pub tempdir: TempDir,
    pub state: Arc<DaemonState>,
}

pub async fn build_fixture_state() -> Result<FixtureState> {
    build_fixture_state_at_instance("http://localhost").await
}

pub async fn build_fixture_state_at_instance(instance_url: &str) -> Result<FixtureState> {
    build_fixture_state_with_config_instance(instance_url, instance_url).await
}

pub async fn build_fixture_state_without_instance_config() -> Result<FixtureState> {
    build_fixture_state_with_config_instance("http://localhost", "").await
}

async fn build_fixture_state_with_config_instance(
    client_instance_url: &str,
    config_instance_url: &str,
) -> Result<FixtureState> {
    let guard = fixture_state_lock().await;
    let tempdir = tempfile::tempdir()?;
    let vault_path = tempdir.path().join("vault");

    let client = ServiceNowClient::builder()
        .instance(client_instance_url)
        .allow_http()
        .auth(BasicAuth::new("tester", "secret"))
        .build()
        .await?;
    let write_client = ServiceNowClient::builder()
        .instance(client_instance_url)
        .allow_http()
        .auth(BasicAuth::new("tester", "secret"))
        .max_retries(0)
        .build()
        .await?;

    let mut config = SnowConfig {
        instance: InstanceConfig {
            url: config_instance_url.to_string(),
            user: "tester".to_string(),
            credential: CredentialProvider::Env,
            portal: String::new(),
        },
        vault: VaultConfig {
            path: vault_path.clone(),
        },
        daemon: CoreDaemonConfig {
            socket_path: tempdir.path().join("snow.sock"),
            mcp_transport: "stdio".to_string(),
        },
        ..Default::default()
    };
    config.apply_defaults();

    let core = SnowCore::builder()
        .config(config.clone())
        .client(client)
        .write_client(write_client)
        .vault_path(vault_path)
        .build()
        .await?;

    let db_path = tempdir.path().join("snow.db");
    let engine = QueryEngine::open(db_path)?;
    let now = Utc::now();

    let change = RecordRow {
        sys_id: "chg-sys".to_string(),
        number: "CHG001".to_string(),
        table_name: "change_request".to_string(),
        resource_type: ResourceType::Change,
        state: Some("Open".to_string()),
        short_desc: Some("Database patch".to_string()),
        description: Some("Patch production database".to_string()),
        assigned_to: Some("user-1".to_string()),
        parent_id: None,
        file_path: Some("changes/CHG001.md".to_string()),
        synced_at: now,
        sys_updated_on: now,
        etag: Some("etag-1".to_string()),
        in_scope: true,
        last_seen_at: now,
        tombstoned_at: None,
        pruned_at: None,
        raw_json: serde_json::json!({
            "sys_id": "chg-sys",
            "number": "CHG001",
            "short_description": "Database patch",
            "description": "Patch production database",
            "state": { "value": "1", "display_value": "Open" },
            "assigned_to": { "sys_id": "user-1", "value": "user-1", "display_value": "Casey User", "table": "sys_user" },
            "work_notes": "2026-04-09 10:11:12 - Casey User (Work notes)\nPatched the primary node.\n"
        })
        .to_string(),
    };
    engine.store().upsert_record(
        &change,
        "2026-04-09 10:11:12 - Casey User\nPatched the primary node.",
        "Patch production database",
    )?;

    let change_task = RecordRow {
        sys_id: "ctask-sys".to_string(),
        number: "CTASK001".to_string(),
        table_name: "change_task".to_string(),
        resource_type: ResourceType::ChangeTask,
        state: Some("Open".to_string()),
        short_desc: Some("Validate replication".to_string()),
        description: Some("Check replica lag".to_string()),
        assigned_to: Some("user-1".to_string()),
        parent_id: Some("chg-sys".to_string()),
        file_path: None,
        synced_at: now,
        sys_updated_on: now,
        etag: Some("etag-2".to_string()),
        in_scope: true,
        last_seen_at: now,
        tombstoned_at: None,
        pruned_at: None,
        raw_json: serde_json::json!({
            "sys_id": "ctask-sys",
            "number": "CTASK001",
            "short_description": "Validate replication",
            "description": "Check replica lag",
            "state": { "value": "1", "display_value": "Open" },
            "parent": { "sys_id": "chg-sys", "value": "chg-sys", "display_value": "CHG001", "number": "CHG001", "table": "change_request" }
        })
        .to_string(),
    };
    engine
        .store()
        .upsert_record(&change_task, "", "Check replica lag")?;

    let approval = RecordRow {
        sys_id: "apr-sys".to_string(),
        number: "APR001".to_string(),
        table_name: "sysapproval_approver".to_string(),
        resource_type: ResourceType::Approval,
        state: Some("requested".to_string()),
        short_desc: Some("Approval for CHG001".to_string()),
        description: Some("Approval pending".to_string()),
        assigned_to: Some("user-1".to_string()),
        parent_id: Some("chg-sys".to_string()),
        file_path: None,
        synced_at: now,
        sys_updated_on: now,
        etag: Some("etag-3".to_string()),
        in_scope: true,
        last_seen_at: now,
        tombstoned_at: None,
        pruned_at: None,
        raw_json: serde_json::json!({
            "sys_id": "apr-sys",
            "number": "APR001",
            "state": "requested",
            "approver": { "sys_id": "user-1", "value": "user-1", "display_value": "Casey User", "table": "sys_user" },
            "sysapproval": { "sys_id": "chg-sys", "value": "chg-sys", "display_value": "CHG001", "number": "CHG001", "table": "change_request" },
            "sys_created_on": "2026-04-09 10:11:12"
        })
        .to_string(),
    };
    engine
        .store()
        .upsert_record(&approval, "", "Approval pending")?;

    let knowledge = RecordRow {
        sys_id: "kb-sys".to_string(),
        number: "KB001".to_string(),
        table_name: "kb_knowledge".to_string(),
        resource_type: ResourceType::Knowledge,
        state: Some("published".to_string()),
        short_desc: Some("Database restart procedure".to_string()),
        description: Some("How to restart the database cluster".to_string()),
        assigned_to: None,
        parent_id: None,
        file_path: Some("knowledge/KB001.md".to_string()),
        synced_at: now,
        sys_updated_on: now,
        etag: Some("etag-4".to_string()),
        in_scope: true,
        last_seen_at: now,
        tombstoned_at: None,
        pruned_at: None,
        raw_json: serde_json::json!({
            "sys_id": "kb-sys",
            "number": "KB001",
            "short_description": "Database restart procedure",
            "description": "How to restart the database cluster",
            "state": "published",
            "workflow_state": "published",
            "article_body": "How to restart the database cluster",
            "published": "2026-04-10 09:00:00",
            "knowledge_base": {
                "sys_id": "kb-base",
                "value": "kb-base",
                "display_value": "IT",
                "table": "kb_knowledge_base"
            },
            "category": {
                "sys_id": "kb-db",
                "value": "kb-db",
                "display_value": "Database",
                "table": "kb_category"
            },
            "author": {
                "sys_id": "user-1",
                "value": "user-1",
                "display_value": "Casey User",
                "table": "sys_user"
            }
        })
        .to_string(),
    };
    engine
        .store()
        .upsert_record(&knowledge, "", "How to restart the database cluster")?;
    engine
        .store()
        .upsert_knowledge_article(&KnowledgeArticleRow {
            record_sys_id: "kb-sys".to_string(),
            number: "KB001".to_string(),
            title: "Database restart procedure".to_string(),
            workflow_state: "published".to_string(),
            knowledge_base_sys_id: "kb-base".to_string(),
            knowledge_base_name: "IT".to_string(),
            category_sys_id: "kb-db".to_string(),
            category_name: "Database".to_string(),
            author_sys_id: Some("user-1".to_string()),
            author_name: Some("Casey User".to_string()),
            published_at: Some("2026-04-10 09:00:00".to_string()),
            valid_to: None,
            article_type: "text".to_string(),
            sys_updated_on: Some("2026-04-10 09:00:00".to_string()),
            sn_tags: vec!["database".to_string(), "restart".to_string()],
            auto_tags: vec!["runbook".to_string()],
            user_tags: vec!["cached".to_string()],
            body_cached: true,
        })?;

    let data_dir = tempdir.path().to_path_buf();
    Ok(FixtureState {
        _guard: guard,
        tempdir,
        state: Arc::new(DaemonState::with_data_dir_and_mcp_config(
            Arc::new(core),
            data_dir,
            snow_mcp::McpConfig::default(),
        )),
    })
}

async fn fixture_state_lock() -> tokio::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

pub async fn spawn_json_http_server(
    response_body: Value,
) -> Result<(String, oneshot::Receiver<String>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (request_tx, request_rx) = oneshot::channel();

    tokio::spawn(async move {
        if let Ok((mut stream, _)) = listener.accept().await {
            let mut request = Vec::new();
            let mut buf = [0u8; 1024];
            loop {
                let read = match stream.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(read) => read,
                    Err(_) => return,
                };
                request.extend_from_slice(&buf[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }

            if let Some(first_line) = request
                .split(|byte| *byte == b'\n')
                .next()
                .and_then(|line| std::str::from_utf8(line).ok())
            {
                let _ = request_tx.send(first_line.trim().to_string());
            }

            let body = response_body.to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes()).await;
            let _ = stream.shutdown().await;
        }
    });

    Ok((format!("http://{}", addr), request_rx))
}

pub async fn spawn_json_http_sequence_server(
    response_bodies: Vec<Value>,
) -> Result<(String, oneshot::Receiver<Vec<String>>)> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let (request_tx, request_rx) = oneshot::channel();

    tokio::spawn(async move {
        let mut request_lines = Vec::new();
        for response_body in response_bodies {
            let Ok((mut stream, _)) = listener.accept().await else {
                break;
            };
            let mut request = Vec::new();
            let mut buf = [0u8; 1024];
            loop {
                let read = match stream.read(&mut buf).await {
                    Ok(0) => break,
                    Ok(read) => read,
                    Err(_) => return,
                };
                request.extend_from_slice(&buf[..read]);
                if request.windows(4).any(|window| window == b"\r\n\r\n") {
                    break;
                }
            }

            if let Some(first_line) = request
                .split(|byte| *byte == b'\n')
                .next()
                .and_then(|line| std::str::from_utf8(line).ok())
            {
                request_lines.push(first_line.trim().to_string());
            }

            let body = response_body.to_string();
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            let _ = stream.write_all(response.as_bytes()).await;
            let _ = stream.shutdown().await;
        }
        let _ = request_tx.send(request_lines);
    });

    Ok((format!("http://{}", addr), request_rx))
}

pub fn socket_path(tempdir: &TempDir) -> PathBuf {
    tempdir.path().join("snow.sock")
}
