//! Reference extraction and normalization for ServiceNow records.
//!
//! This module provides functions that extract, build, and normalize
//! reference fields from raw `Record` objects into the internal
//! `Reference` and `RecordRef` types.

use std::collections::HashMap;

use serde_json::{Map, Value};

use servicenow_rs::prelude::{
    Record, parent_reference_field, reference_default_table, reference_fields_for_table,
};

use crate::{RecordRef, Reference};

/// Checks if a string looks like a raw ServiceNow sys_id (hex, 30-32 chars).
///
/// Used to detect unresolved display values that are just opaque identifiers
/// rather than human-readable labels.
pub(crate) fn is_opaque_sys_id(value: &str) -> bool {
    let trimmed = value.trim();
    (30..=32).contains(&trimmed.len()) && trimmed.bytes().all(|byte| byte.is_ascii_hexdigit())
}

/// Picks the best display name from a list of candidates.
///
/// Skips empty strings and opaque sys_ids, returning the first
/// human-readable candidate or an empty string if none qualify.
pub(crate) fn choose_reference_display_name<'a>(
    candidates: impl IntoIterator<Item = Option<&'a str>>,
) -> String {
    candidates
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|candidate| !candidate.is_empty() && !is_opaque_sys_id(candidate))
        .unwrap_or_default()
        .to_string()
}

/// Normalizes a reference for a specific field by resolving the best display name.
///
/// For fields like `knowledge_base`, `category`, and `author`, this checks
/// multiple extra fields (name, label, title, etc.) to find a human-readable
/// display name.
pub(crate) fn normalize_reference_for_field(
    field_name: &str,
    mut reference: Reference,
) -> Reference {
    if matches!(field_name, "knowledge_base" | "category" | "author") {
        reference.display_name = choose_reference_display_name([
            Some(reference.display_name.as_str()),
            reference.extra.get("name").map(String::as_str),
            reference.extra.get("label").map(String::as_str),
            reference.extra.get("title").map(String::as_str),
            reference.extra.get("user_name").map(String::as_str),
            reference.extra.get("email").map(String::as_str),
            reference.extra.get("number").map(String::as_str),
        ]);
    }
    reference
}

/// Extracts the sys_id from a reference field on a raw `Record`.
pub(crate) fn extract_reference_sys_id(record: &Record, field: &str) -> Option<String> {
    record
        .get_raw(field)
        .or_else(|| record.get_str(field))
        .filter(|value| !value.trim().is_empty())
        .map(str::to_string)
}

/// Collects all references from a `Record` based on the table's known reference fields.
pub(crate) fn collect_record_references(record: &Record) -> HashMap<String, Reference> {
    let mut references = HashMap::new();
    for field_name in reference_fields_for_table(&record.table) {
        if let Some(reference) =
            reference_from_record_field(record, field_name, reference_default_table(field_name))
        {
            references.insert(field_name.to_string(), reference);
        }
    }
    references
}

pub(crate) fn parent_reference_for_table(table_name: &str) -> Option<(&'static str, &'static str)> {
    match table_name {
        "resource_plan" => Some(("task", "task")),
        _ => parent_reference_field(table_name),
    }
}

/// Extracts a single `Reference` from a named field on a raw `Record`.
pub(crate) fn reference_from_record_field(
    record: &Record,
    field_name: &str,
    default_table: Option<&str>,
) -> Option<Reference> {
    let default_table = default_table.unwrap_or(field_name);
    let value = build_reference_json(record, field_name, default_table)?;
    let map = value.as_object()?;
    let sys_id = map.get("sys_id").and_then(Value::as_str)?.to_string();
    let display_name = choose_reference_display_name([
        map.get("display_value").and_then(Value::as_str),
        map.get("name").and_then(Value::as_str),
        map.get("label").and_then(Value::as_str),
        map.get("title").and_then(Value::as_str),
        map.get("user_name").and_then(Value::as_str),
        map.get("email").and_then(Value::as_str),
        map.get("number").and_then(Value::as_str),
    ]);
    let table = map
        .get("table")
        .and_then(Value::as_str)
        .unwrap_or(default_table)
        .to_string();
    let mut extra = HashMap::new();
    for (key, value) in map {
        if matches!(
            key.as_str(),
            "sys_id" | "value" | "display_value" | "number" | "table"
        ) {
            continue;
        }
        if let Some(text) = value.as_str() {
            extra.insert(key.clone(), text.to_string());
        } else {
            extra.insert(key.clone(), value.to_string());
        }
    }

    Some(Reference {
        sys_id,
        table,
        display_name,
        extra,
    })
}

/// Builds a JSON object for a reference field, including dot-walked sub-fields.
pub(crate) fn build_reference_json(
    record: &Record,
    field_name: &str,
    default_table: &str,
) -> Option<Value> {
    let sys_id = extract_reference_sys_id(record, field_name)?;
    let mut map = Map::new();
    map.insert("sys_id".to_string(), Value::String(sys_id.clone()));
    map.insert("value".to_string(), Value::String(sys_id));

    if let Some(display_value) = record
        .get_display(field_name)
        .or_else(|| record.get_str(&format!("{field_name}.number")))
        .or_else(|| record.get_str(&format!("{field_name}.name")))
        .filter(|value| !value.trim().is_empty())
    {
        map.insert(
            "display_value".to_string(),
            Value::String(display_value.to_string()),
        );
    }
    if let Some(number) = record
        .get_str(&format!("{field_name}.number"))
        .filter(|value| !value.trim().is_empty())
    {
        map.insert("number".to_string(), Value::String(number.to_string()));
    }
    for suffix in ["name", "label", "title", "user_name", "email"] {
        if let Some(value) = record
            .get_str(&format!("{field_name}.{suffix}"))
            .filter(|value| !value.trim().is_empty())
        {
            map.insert(suffix.to_string(), Value::String(value.to_string()));
        }
    }
    let table = record
        .get_str(&format!("{field_name}.sys_class_name"))
        .or_else(|| record.get_str("source_table"))
        .filter(|value| !value.trim().is_empty())
        .unwrap_or(default_table);
    map.insert("table".to_string(), Value::String(table.to_string()));

    Some(Value::Object(map))
}

/// Constructs a `RecordRef` for the parent record from a child record.
pub(crate) fn parent_record_ref(record: &Record) -> Option<RecordRef> {
    let (field_name, default_table) = parent_reference_for_table(&record.table)?;
    let value = build_reference_json(record, field_name, default_table)?;
    let map = value.as_object()?;
    let sys_id = map
        .get("sys_id")
        .and_then(Value::as_str)
        .or_else(|| map.get("value").and_then(Value::as_str))?
        .to_string();
    let number = map
        .get("number")
        .and_then(Value::as_str)
        .or_else(|| map.get("display_value").and_then(Value::as_str))
        .unwrap_or_default()
        .to_string();
    let table = map
        .get("table")
        .and_then(Value::as_str)
        .or_else(|| map.get("sys_class_name").and_then(Value::as_str))
        .unwrap_or(default_table)
        .to_string();

    Some(RecordRef {
        sys_id,
        number,
        table,
    })
}

/// Creates an empty `Reference` with the given table name.
pub(crate) fn empty_reference(table: &str) -> Reference {
    Reference {
        sys_id: String::new(),
        table: table.to_string(),
        display_name: String::new(),
        extra: HashMap::new(),
    }
}
