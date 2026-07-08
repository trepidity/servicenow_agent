#![cfg(test)]

use super::*;
use crate::ResourceType;
use crate::vault::VaultDocument;
use chrono::{DateTime, TimeZone, Utc};
use servicenow_rs::prelude::{BasicAuth, DisplayValue, Record, ServiceNowClient};
use std::collections::HashMap;
use std::path::PathBuf;
use tempfile::TempDir;
use wiremock::matchers::{method, path, query_param};
use wiremock::{Mock, MockServer, ResponseTemplate};

// pub(crate): also called from `service::approval`'s test module (Task 9),
// which moved its approve/reject/my_approvals tests out of this file. This
// fn stays here (not duplicated) because it also serializes dozens of
// non-approval tests throughout this module that mutate shared state.
pub(crate) async fn mock_server_test_lock() -> tokio::sync::MutexGuard<'static, ()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

pub(crate) fn sample_change_task_record() -> Record {
    let json = serde_json::json!({
        "sys_id": "task-sys",
        "number": "CTASK001",
        "short_description": "Apply change",
        "description": "Patch the server",
        "state": "Open",
        "assigned_to": {
            "value": "user-sys",
            "display_value": "Casey User"
        },
        "change_request": {
            "value": "chg-sys",
            "display_value": "CHG001"
        },
        "change_request.number": "CHG001",
        "change_request.sys_class_name": "change_request",
        "sys_updated_on": "2026-04-09 10:11:12",
        "sys_mod_count": "7",
        "work_notes": "2026-04-09 10:11:12 - Casey User (Work notes)\nUpdated task\n"
    });
    Record::from_json("change_task", &json, DisplayValue::Both).expect("record")
}

pub(crate) fn sample_incident_record() -> SnowRecord {
    SnowRecord {
        sys_id: "inc-sys".to_string(),
        number: "INC001".to_string(),
        table: "incident".to_string(),
        resource_type: ResourceType::Incident,
        state: "Open".to_string(),
        short_description: "Legacy incident".to_string(),
        description: "Legacy body".to_string(),
        fields: HashMap::from([(
            "assigned_to".to_string(),
            FieldValue {
                value: "user-sys".to_string(),
                display_value: Some("Casey User".to_string()),
            },
        )]),
        work_notes: vec![JournalEntry {
            timestamp: Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
            author: "Casey User".to_string(),
            body: "Investigating.".to_string(),
        }],
        comments: Vec::new(),
        parent: None,
        children: Vec::new(),
        references: HashMap::new(),
        synced_at: Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
        source: CacheSource::Disk,
    }
}

// pub(crate): also called from `service::user`'s test module, which moved
// its user-search test out of this file in Task 8. This fn stays here
// (not duplicated) because it is shared fixture setup used by dozens of
// non-user tests throughout this module.
pub(crate) async fn core_for_mock_server(server: &MockServer) -> (SnowCore, TempDir) {
    let client = ServiceNowClient::builder()
        .instance(server.uri())
        .auth(BasicAuth::new("test_user", "test_pass"))
        .allow_http()
        .build()
        .await
        .expect("client");

    let tempdir = TempDir::new().expect("tempdir");
    let core = SnowCore::builder()
        .client(client)
        .vault_path(tempdir.path().join("vault"))
        .build()
        .await
        .expect("core");

    (core, tempdir)
}

// pub(crate): also called from `service::approval`'s test module (Task 9),
// which moved its approve/reject/my_approvals tests out of this file. This
// fn stays here (not duplicated) because it is shared fixture setup used
// by other non-approval tests throughout this module.
pub(crate) async fn core_for_mock_server_with_user(
    server: &MockServer,
    user: &str,
) -> (SnowCore, TempDir) {
    let client = ServiceNowClient::builder()
        .instance(server.uri())
        .auth(BasicAuth::new("test_user", "test_pass"))
        .allow_http()
        .build()
        .await
        .expect("client");

    let mut config = config::SnowConfig::default();
    config.instance.user = user.to_string();

    let tempdir = TempDir::new().expect("tempdir");
    let core = SnowCore::builder()
        .config(config)
        .client(client)
        .vault_path(tempdir.path().join("vault"))
        .build()
        .await
        .expect("core");

    (core, tempdir)
}

pub(crate) async fn mount_number_lookup(
    server: &MockServer,
    table: &str,
    number: &str,
    sys_id: &str,
) {
    Mock::given(method("GET"))
        .and(path(format!("/api/now/table/{table}")))
        .and(query_param("sysparm_query", format!("number={number}")))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [{
                "sys_id": sys_id,
                "number": number,
                "short_description": "Attachment target",
                "state": "Open"
            }]
        })))
        .mount(server)
        .await;
}

pub(crate) async fn mount_fresh_record_get(
    server: &MockServer,
    table: &str,
    sys_id: &str,
    record: serde_json::Value,
) {
    Mock::given(method("GET"))
        .and(path(format!("/api/now/table/{table}/{sys_id}")))
        .respond_with(
            ResponseTemplate::new(200).set_body_json(serde_json::json!({ "result": record })),
        )
        .mount(server)
        .await;
}

// pub(crate): also called from `service::approval`'s test module (Task 9),
// which moved its approve/reject/my_approvals tests out of this file. This
// fn stays here (not duplicated) because it is shared fixture setup used
// by other non-approval tests throughout this module.
pub(crate) async fn mount_empty_journal_fetch(server: &MockServer, table: &str, sys_id: &str) {
    Mock::given(method("GET"))
        .and(path(format!("/api/now/table/{table}")))
        .and(query_param("sysparm_display_value", "true"))
        .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
            "result": [{
                "sys_id": sys_id,
                "work_notes": "",
                "comments": ""
            }]
        })))
        .mount(server)
        .await;
}

pub(crate) fn timecard_record_json(monday: &str, state: &str) -> serde_json::Value {
    serde_json::json!({
        "sys_id": "card-sys",
        "time_sheet": {
            "value": "sheet-sys",
            "display_value": "2026-05-17"
        },
        "week_starts_on": "2026-05-17",
        "user": {
            "value": "user-sys",
            "display_value": "Test User"
        },
        "user.user_name": "test_user",
        "user.email": "test@example.com",
        "task": {
            "value": "task-sys",
            "display_value": "PRJ0161219"
        },
        "task.number": "PRJ0161219",
        "task.sys_class_name": "pm_project_task",
        "category": {
            "value": "project_work",
            "display_value": "Project/Project Task"
        },
        "project_time_category": "Development",
        "sunday": "0",
        "monday": monday,
        "tuesday": "0",
        "wednesday": "0",
        "thursday": "0",
        "friday": "0",
        "saturday": "0",
        "total": monday,
        "state": {
            "value": state,
            "display_value": state
        },
        "sys_updated_on": "2026-05-21 10:11:12",
        "sys_mod_count": "3"
    })
}

pub(crate) fn sample_projected_record() -> SnowRecord {
    let mut record = sample_incident_record();
    record.sys_id = "inc-projected".to_string();
    record.number = "INC002".to_string();
    record.parent = Some(RecordRef {
        sys_id: "parent-sys".to_string(),
        number: "CHG002".to_string(),
        table: "change_request".to_string(),
    });
    record.children = vec![RecordRef {
        sys_id: "child-sys".to_string(),
        number: "INC003".to_string(),
        table: "incident".to_string(),
    }];
    record.references.insert(
        "assigned_to".to_string(),
        Reference {
            sys_id: "user-sys".to_string(),
            table: "sys_user".to_string(),
            display_name: "Casey User".to_string(),
            extra: HashMap::new(),
        },
    );
    record
}

pub(crate) fn sample_projected_knowledge_article() -> KnowledgeArticle {
    let mut record = SnowRecord {
        sys_id: "kb-projected".to_string(),
        number: "KB002".to_string(),
        table: "kb_knowledge".to_string(),
        resource_type: ResourceType::Knowledge,
        state: "published".to_string(),
        short_description: "Windows Access Runbook".to_string(),
        description: "How to request and validate Windows admin access.".to_string(),
        fields: HashMap::from([
            (
                "workflow_state".to_string(),
                FieldValue {
                    value: "published".to_string(),
                    display_value: Some("Published".to_string()),
                },
            ),
            (
                "published".to_string(),
                FieldValue {
                    value: "2026-04-10 09:00:00".to_string(),
                    display_value: Some("2026-04-10 09:00:00".to_string()),
                },
            ),
            (
                "author".to_string(),
                FieldValue {
                    value: "user-kb".to_string(),
                    display_value: Some("Casey User".to_string()),
                },
            ),
        ]),
        work_notes: Vec::new(),
        comments: Vec::new(),
        parent: None,
        children: Vec::new(),
        references: HashMap::from([(
            "author".to_string(),
            Reference {
                sys_id: "user-kb".to_string(),
                table: "sys_user".to_string(),
                display_name: "Casey User".to_string(),
                extra: HashMap::new(),
            },
        )]),
        synced_at: Utc.timestamp_opt(1_712_649_600, 0).unwrap(),
        source: CacheSource::Disk,
    };
    record.references.insert(
        "knowledge_base".to_string(),
        Reference {
            sys_id: "kb-base".to_string(),
            table: "kb_knowledge_base".to_string(),
            display_name: "IT".to_string(),
            extra: HashMap::new(),
        },
    );
    record.references.insert(
        "category".to_string(),
        Reference {
            sys_id: "kb-cat".to_string(),
            table: "kb_category".to_string(),
            display_name: "Access".to_string(),
            extra: HashMap::new(),
        },
    );

    KnowledgeArticle {
        record,
        knowledge_base: Reference {
            sys_id: "kb-base".to_string(),
            table: "kb_knowledge_base".to_string(),
            display_name: "IT".to_string(),
            extra: HashMap::new(),
        },
        category: Reference {
            sys_id: "kb-cat".to_string(),
            table: "kb_category".to_string(),
            display_name: "Access".to_string(),
            extra: HashMap::new(),
        },
        article_type: "text".to_string(),
        content: "Step 1: Request access.\nStep 2: Validate group membership.".to_string(),
        sn_tags: vec!["access".to_string()],
        auto_tags: vec!["request".to_string()],
        user_tags: vec!["tier-1".to_string()],
        body_cached: true,
        published_at: Some(
            chrono::NaiveDateTime::parse_from_str("2026-04-10 09:00:00", "%Y-%m-%d %H:%M:%S")
                .map(|dt| DateTime::<Utc>::from_naive_utc_and_offset(dt, Utc))
                .expect("published timestamp"),
        ),
        author: Some(Reference {
            sys_id: "user-kb".to_string(),
            table: "sys_user".to_string(),
            display_name: "Casey User".to_string(),
            extra: HashMap::new(),
        }),
        valid_to: Some(chrono::NaiveDate::from_ymd_opt(2027, 1, 1).unwrap()),
    }
}

pub(crate) async fn build_test_core(vault_path: PathBuf) -> SnowCore {
    let server = MockServer::start().await;
    let client = ServiceNowClient::builder()
        .instance(server.uri())
        .auth(BasicAuth::new("test_user", "test_pass"))
        .allow_http()
        .build()
        .await
        .expect("client");

    SnowCore::builder()
        .client(client)
        .vault_path(vault_path)
        .build()
        .await
        .expect("core")
}

pub(crate) async fn build_semantic_test_core(vault_path: PathBuf) -> SnowCore {
    let server = MockServer::start().await;
    let client = ServiceNowClient::builder()
        .instance(server.uri())
        .auth(BasicAuth::new("test_user", "test_pass"))
        .allow_http()
        .build()
        .await
        .expect("client");
    let config = config::SnowConfig {
        kb: config::KbConfig {
            semantic_search: config::KbSemanticSearchConfig {
                enabled: true,
                provider: "stub".to_string(),
                model: "stub-model".to_string(),
                ..Default::default()
            },
            ..Default::default()
        },
        ..Default::default()
    };

    SnowCore::builder()
        .config(config)
        .client(client)
        .vault_path(vault_path)
        .build()
        .await
        .expect("core")
}

pub(crate) fn seed_projected_knowledge_article(core: &SnowCore, article: &KnowledgeArticle) {
    let document = VaultDocument::Knowledge(article.clone());
    let persisted = core
        .ctx
        .persist_runtime_document(&document)
        .expect("persist runtime document");
    let row = record_row_from_runtime_record(
        &article.record,
        Some(persisted.relative_path.clone()),
        serialize_vault_document(&document).to_string(),
    );
    core.ctx
        .query
        .store()
        .upsert_record_with_tags(
            &row,
            "",
            &document_content(&document),
            &document_tag_tokens(&document),
        )
        .expect("upsert record");
    core.ctx
        .project_runtime_document(&document)
        .expect("project runtime document");
}

pub(crate) fn seed_projected_record(core: &SnowCore, record: &SnowRecord) {
    let document = VaultDocument::Record(record.clone());
    let persisted = core
        .ctx
        .persist_runtime_document(&document)
        .expect("persist runtime document");
    let row = record_row_from_runtime_record(
        record,
        Some(persisted.relative_path.clone()),
        serialize_vault_document(&document).to_string(),
    );
    core.ctx
        .query
        .store()
        .upsert_record_with_tags(
            &row,
            &document_work_notes(record),
            &document_content(&document),
            &document_tag_tokens(&document),
        )
        .expect("upsert record");
    core.ctx
        .project_runtime_document(&document)
        .expect("project runtime document");
}

pub(crate) fn time_sheet_row_json(week_starts_on: &str) -> serde_json::Value {
    serde_json::json!({
        "sys_id": format!("sheet-{week_starts_on}"),
        "week_starts_on": week_starts_on,
        "user": { "value": "user-sys", "display_value": "Test User" },
        "user.user_name": "test_user",
        "state": { "value": "Pending", "display_value": "Pending" }
    })
}
