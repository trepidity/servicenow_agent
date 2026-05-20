//! Type conversions and serialization helpers.
//!
//! This module contains free functions that convert between ServiceNow API
//! types, internal runtime types (`SnowRecord`, `FieldValue`, `Reference`),
//! and cache/store row types (`RecordRow`, `ReferenceRow`, etc.).

use std::collections::HashMap;
use std::path::PathBuf;

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde_json::{Map, Value};

use servicenow_rs::prelude::{Record, parse_servicenow_timestamp};

use crate::cache::store::{KnowledgeArticleRow, RecordRow, ReferenceRow};
use crate::enrich::EnrichmentOrigin;
use crate::reference::{
    choose_reference_display_name, extract_reference_sys_id, normalize_reference_for_field,
    parent_record_ref, parent_reference_for_table,
};
use crate::{
    FieldValue, JournalEntry, KnowledgeArticle, RecordRef, Reference, SnowRecord, VaultDocument,
};

/// Converts a `servicenow_rs` `Record` into a `RecordRow` for cache storage.
///
/// This is the primary path used when persisting a freshly-fetched API record.
#[cfg(test)]
pub(crate) fn record_row_from_servicenow(record: &Record) -> Result<RecordRow> {
    let snow_record = SnowRecord::from_servicenow(record);
    record_row_from_snow_record(&snow_record, record, None)
}

/// Converts a `SnowRecord` + raw `Record` pair into a `RecordRow`.
///
/// Merges runtime metadata (sys_updated_on, etag) from the raw record with
/// domain-level fields (number, state, parent) from the `SnowRecord`.
pub(crate) fn record_row_from_snow_record(
    snow_record: &SnowRecord,
    record: &Record,
    file_path: Option<PathBuf>,
) -> Result<RecordRow> {
    let synced_at = snow_record.synced_at;
    let sys_updated_on = parse_servicenow_timestamp(
        record
            .get_raw("sys_updated_on")
            .or_else(|| record.get_str("sys_updated_on")),
    )
    .unwrap_or(synced_at);
    let raw_json = serialize_record_document(record).to_string();
    let parent = parent_record_ref(record);

    Ok(RecordRow {
        sys_id: snow_record.sys_id.clone(),
        number: snow_record.number.clone(),
        table_name: snow_record.table.clone(),
        resource_type: snow_record.resource_type.clone(),
        state: Some(snow_record.state.clone()),
        short_desc: Some(snow_record.short_description.clone()),
        description: Some(snow_record.description.clone()),
        assigned_to: record_owner_sys_id(record),
        parent_id: parent.map(|reference| reference.sys_id),
        file_path: file_path.map(|path| path.to_string_lossy().into_owned()),
        synced_at,
        sys_updated_on,
        etag: Some(build_record_validator(record, sys_updated_on)),
        in_scope: true,
        last_seen_at: synced_at,
        tombstoned_at: None,
        pruned_at: None,
        raw_json,
    })
}

fn record_owner_sys_id(record: &Record) -> Option<String> {
    for field_name in ["assigned_to", "project_manager", "demand_manager"] {
        if let Some(value) = extract_reference_sys_id(record, field_name) {
            return Some(value);
        }
    }
    None
}

/// Converts a `SnowRecord` into a `RecordRow` for vault rebuilds.
///
/// Unlike `record_row_from_snow_record`, this does not require the original
/// `Record` — it works purely from the runtime `SnowRecord` already stored
/// in the vault.
pub(crate) fn record_row_from_runtime_record(
    record: &SnowRecord,
    file_path: Option<PathBuf>,
    raw_json: String,
) -> RecordRow {
    let synced_at = record.synced_at;
    RecordRow {
        sys_id: record.sys_id.clone(),
        number: record.number.clone(),
        table_name: record.table.clone(),
        resource_type: record.resource_type.clone(),
        state: Some(record.state.clone()),
        short_desc: Some(record.short_description.clone()),
        description: Some(record.description.clone()),
        assigned_to: crate::helpers::document_assigned_to(record),
        parent_id: record.parent.as_ref().map(|parent| parent.sys_id.clone()),
        file_path: file_path.map(|path| path.to_string_lossy().into_owned()),
        synced_at,
        sys_updated_on: synced_at,
        etag: Some(format!("vault:{}", synced_at.to_rfc3339())),
        in_scope: true,
        last_seen_at: synced_at,
        tombstoned_at: None,
        pruned_at: None,
        raw_json,
    }
}

/// Serializes a raw `Record` to JSON for vault storage.
pub(crate) fn serialize_record_document(record: &Record) -> Value {
    let mut map = Map::new();
    for (field_name, field_value) in record.fields() {
        map.insert(
            field_name.clone(),
            field_value_to_json(record, field_name, field_value),
        );
    }

    if let Some((field_name, default_table)) = parent_reference_for_table(&record.table)
        && let Some(parent_value) =
            crate::reference::build_reference_json(record, field_name, default_table)
    {
        map.entry("parent".to_string()).or_insert(parent_value);
    }

    Value::Object(map)
}

/// Converts a `FieldValue` from `servicenow_rs` to JSON.
///
/// Handles dot-walked fields by merging them into the base field object.
pub(crate) fn field_value_to_json(
    record: &Record,
    field_name: &str,
    field_value: &servicenow_rs::prelude::FieldValue,
) -> Value {
    let dot_walked = record.dot_walked_fields(field_name);
    if dot_walked.is_empty() {
        return base_field_value_json(field_value);
    }

    let mut map = match base_field_value_json(field_value) {
        Value::Object(map) => map,
        scalar => {
            let mut map = Map::new();
            map.insert("value".to_string(), scalar);
            map
        }
    };

    for (dot_key, dot_value) in dot_walked {
        let suffix = &dot_key[field_name.len() + 1..];
        let nested = dot_value
            .display_value
            .as_ref()
            .map(|value| Value::String(value.clone()))
            .or_else(|| dot_value.value.clone())
            .unwrap_or(Value::Null);
        map.insert(suffix.to_string(), nested);
    }

    Value::Object(map)
}

/// Base case for field value JSON conversion — handles value, display_value, link.
pub(crate) fn base_field_value_json(field_value: &servicenow_rs::prelude::FieldValue) -> Value {
    let mut map = Map::new();
    if let Some(value) = &field_value.value {
        map.insert("value".to_string(), value.clone());
    }
    if let Some(display_value) = &field_value.display_value {
        map.insert(
            "display_value".to_string(),
            Value::String(display_value.clone()),
        );
    }
    if let Some(link) = &field_value.link {
        map.insert("link".to_string(), Value::String(link.clone()));
    }

    if map.is_empty() {
        Value::Null
    } else if map.len() == 1
        && map.contains_key("value")
        && field_value.display_value.is_none()
        && field_value.link.is_none()
    {
        map.remove("value").unwrap_or(Value::Null)
    } else {
        Value::Object(map)
    }
}

/// Serializes a `VaultDocument` to JSON for cache storage.
pub(crate) fn serialize_vault_document(document: &VaultDocument) -> Value {
    match document {
        VaultDocument::Record(record) => serialize_cached_record_document(record),
        VaultDocument::Knowledge(article) => {
            let mut map = match serialize_cached_record_document(&article.record) {
                Value::Object(map) => map,
                _ => Map::new(),
            };
            map.insert(
                "knowledge_base".to_string(),
                reference_to_json(&article.knowledge_base),
            );
            map.insert("category".to_string(), reference_to_json(&article.category));
            map.insert(
                "article_type".to_string(),
                Value::String(article.article_type.clone()),
            );
            map.insert("text".to_string(), Value::String(article.content.clone()));
            if let Some(published_at) = article.published_at {
                map.insert(
                    "published".to_string(),
                    Value::String(
                        published_at
                            .naive_utc()
                            .format("%Y-%m-%d %H:%M:%S")
                            .to_string(),
                    ),
                );
            }
            if let Some(author) = &article.author {
                map.insert("author".to_string(), reference_to_json(author));
            }
            if let Some(valid_to) = article.valid_to {
                map.insert("valid_to".to_string(), Value::String(valid_to.to_string()));
            }
            if !article.sn_tags.is_empty() {
                map.insert(
                    "sn_tags".to_string(),
                    Value::Array(article.sn_tags.iter().cloned().map(Value::String).collect()),
                );
            }
            if !article.auto_tags.is_empty() {
                map.insert(
                    "auto_tags".to_string(),
                    Value::Array(
                        article
                            .auto_tags
                            .iter()
                            .cloned()
                            .map(Value::String)
                            .collect(),
                    ),
                );
            }
            if !article.user_tags.is_empty() {
                map.insert(
                    "user_tags".to_string(),
                    Value::Array(
                        article
                            .user_tags
                            .iter()
                            .cloned()
                            .map(Value::String)
                            .collect(),
                    ),
                );
            }
            map.insert("body_cached".to_string(), Value::Bool(article.body_cached));
            Value::Object(map)
        }
        VaultDocument::Approval(approval) => {
            let mut map = match serialize_cached_record_document(&approval.record) {
                Value::Object(map) => map,
                _ => Map::new(),
            };
            map.insert(
                "approver".to_string(),
                reference_to_json(&approval.approver),
            );
            map.insert(
                "sysapproval".to_string(),
                record_ref_to_json(&approval.target),
            );
            map.insert(
                "sys_created_on".to_string(),
                Value::String(approval.requested_at.to_rfc3339()),
            );
            if let Some(due_date) = approval.due_date {
                map.insert("due_date".to_string(), Value::String(due_date.to_rfc3339()));
            }
            Value::Object(map)
        }
    }
}

/// Serializes a `SnowRecord` to JSON for cache storage.
pub(crate) fn serialize_cached_record_document(record: &SnowRecord) -> Value {
    let mut map = Map::new();
    map.insert("sys_id".to_string(), Value::String(record.sys_id.clone()));
    map.insert("number".to_string(), Value::String(record.number.clone()));
    map.insert(
        "short_description".to_string(),
        Value::String(record.short_description.clone()),
    );
    map.insert(
        "description".to_string(),
        Value::String(record.description.clone()),
    );
    map.insert("state".to_string(), Value::String(record.state.clone()));

    for (field_name, value) in &record.fields {
        map.insert(field_name.clone(), field_value_to_cached_json(value));
    }
    for (field_name, reference) in &record.references {
        map.insert(field_name.clone(), reference_to_json(reference));
    }
    if let Some(parent) = &record.parent {
        map.insert("parent".to_string(), record_ref_to_json(parent));
    }
    if !record.work_notes.is_empty() {
        map.insert(
            "work_notes".to_string(),
            Value::Array(
                record
                    .work_notes
                    .iter()
                    .map(journal_entry_to_json)
                    .collect(),
            ),
        );
    }
    if !record.comments.is_empty() {
        map.insert(
            "comments".to_string(),
            Value::Array(record.comments.iter().map(journal_entry_to_json).collect()),
        );
    }
    Value::Object(map)
}

/// Converts a `FieldValue` to JSON for cached records.
pub(crate) fn field_value_to_cached_json(value: &FieldValue) -> Value {
    let mut map = Map::new();
    map.insert("value".to_string(), Value::String(value.value.clone()));
    if let Some(display_value) = &value.display_value {
        map.insert(
            "display_value".to_string(),
            Value::String(display_value.clone()),
        );
    }
    Value::Object(map)
}

/// Converts a `Reference` to JSON.
pub(crate) fn reference_to_json(reference: &Reference) -> Value {
    let mut map = Map::new();
    map.insert(
        "sys_id".to_string(),
        Value::String(reference.sys_id.clone()),
    );
    map.insert("value".to_string(), Value::String(reference.sys_id.clone()));
    map.insert(
        "display_value".to_string(),
        Value::String(reference.display_name.clone()),
    );
    map.insert("table".to_string(), Value::String(reference.table.clone()));
    Value::Object(map)
}

/// Converts a `RecordRef` to JSON.
pub(crate) fn record_ref_to_json(reference: &RecordRef) -> Value {
    let mut map = Map::new();
    map.insert(
        "sys_id".to_string(),
        Value::String(reference.sys_id.clone()),
    );
    map.insert("value".to_string(), Value::String(reference.sys_id.clone()));
    map.insert(
        "display_value".to_string(),
        Value::String(reference.number.clone()),
    );
    map.insert(
        "number".to_string(),
        Value::String(reference.number.clone()),
    );
    map.insert("table".to_string(), Value::String(reference.table.clone()));
    Value::Object(map)
}

/// Converts a `JournalEntry` to JSON.
pub(crate) fn journal_entry_to_json(entry: &JournalEntry) -> Value {
    serde_json::json!({
        "timestamp": entry.timestamp.to_rfc3339(),
        "author": entry.author,
        "body": entry.body,
    })
}

/// Builds a validation string from a `Record` for ETag-like freshness checks.
pub(crate) fn build_record_validator(record: &Record, sys_updated_on: DateTime<Utc>) -> String {
    if let Some(sys_mod_count) = record
        .get_raw("sys_mod_count")
        .or_else(|| record.get_str("sys_mod_count"))
        .filter(|value| !value.trim().is_empty())
    {
        return format!(
            "sys_mod_count:{sys_mod_count}:updated:{}",
            sys_updated_on.to_rfc3339()
        );
    }
    format!("updated:{}", sys_updated_on.to_rfc3339())
}

/// Converts a `KnowledgeArticle` to a `KnowledgeArticleRow` for cache storage.
pub(crate) fn knowledge_article_row_from_article(
    article: &KnowledgeArticle,
) -> KnowledgeArticleRow {
    let author = article
        .author
        .clone()
        .or_else(|| article.record.references.get("author").cloned())
        .or_else(|| author_reference_from_fields(&article.record.fields))
        .map(|reference| normalize_reference_for_field("author", reference));
    let published_at = article
        .published_at
        .map(|value| value.naive_utc().format("%Y-%m-%d %H:%M:%S").to_string())
        .or_else(|| {
            article
                .record
                .fields
                .get("published")
                .map(|field| field.value.trim().to_string())
                .filter(|value| !value.is_empty())
        });
    let sys_updated_on = article
        .record
        .fields
        .get("sys_updated_on")
        .map(|field| field.value.trim().to_string())
        .filter(|value| !value.is_empty());

    KnowledgeArticleRow {
        record_sys_id: article.record.sys_id.clone(),
        number: article.record.number.clone(),
        title: article.record.short_description.clone(),
        workflow_state: article.record.state.clone(),
        knowledge_base_sys_id: article.knowledge_base.sys_id.clone(),
        knowledge_base_name: article.knowledge_base.display_name.clone(),
        category_sys_id: article.category.sys_id.clone(),
        category_name: article.category.display_name.clone(),
        author_sys_id: author.as_ref().map(|reference| reference.sys_id.clone()),
        author_name: author.map(|reference| reference.display_name),
        published_at,
        valid_to: article.valid_to.map(|date| date.to_string()),
        article_type: article.article_type.clone(),
        sys_updated_on,
        sn_tags: article.sn_tags.clone(),
        auto_tags: article.auto_tags.clone(),
        user_tags: article.user_tags.clone(),
        body_cached: article.body_cached,
    }
}

/// Converts a `Reference` to a `ReferenceRow` for cache storage.
pub(crate) fn reference_row_from_reference(
    reference: &Reference,
    synced_at: DateTime<Utc>,
) -> ReferenceRow {
    ReferenceRow {
        sys_id: reference.sys_id.clone(),
        table_name: reference.table.clone(),
        display_name: reference.display_name.clone(),
        extra_json: serde_json::to_string(&reference.extra).unwrap_or_else(|_| "{}".to_string()),
        synced_at,
        expires_at: None,
    }
}

/// Converts a `RecordRef` to a `ReferenceRow` for cache storage.
pub(crate) fn reference_row_from_record_ref(
    reference: &RecordRef,
    synced_at: DateTime<Utc>,
) -> ReferenceRow {
    ReferenceRow {
        sys_id: reference.sys_id.clone(),
        table_name: reference.table.clone(),
        display_name: reference.number.clone(),
        extra_json: "{}".to_string(),
        synced_at,
        expires_at: None,
    }
}

/// Extracts an author `Reference` from a record's fields map.
pub(crate) fn author_reference_from_fields(
    fields: &HashMap<String, FieldValue>,
) -> Option<Reference> {
    let author = fields.get("author")?;
    let sys_id = author.value.trim();
    if sys_id.is_empty() {
        return None;
    }

    Some(Reference {
        sys_id: sys_id.to_string(),
        table: "sys_user".to_string(),
        display_name: choose_reference_display_name([author.display_value.as_deref()]),
        extra: HashMap::new(),
    })
}

/// Maps an `EnrichmentOrigin` to a human-readable string label.
pub(crate) fn enrichment_origin_label(origin: EnrichmentOrigin) -> &'static str {
    match origin {
        EnrichmentOrigin::ResourceType => "resource_type",
        EnrichmentOrigin::State => "state",
        EnrichmentOrigin::Title => "title",
        EnrichmentOrigin::Description => "description",
        EnrichmentOrigin::WorkNotes => "work_notes",
        EnrichmentOrigin::Comments => "comments",
        EnrichmentOrigin::Reference => "reference",
        EnrichmentOrigin::Number => "number",
        EnrichmentOrigin::Token => "token",
    }
}
