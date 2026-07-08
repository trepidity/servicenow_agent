//! Small utility functions used across the snow_core crate.
//!
//! These are stateless helpers for record sorting, journal rendering,
//! vault path management, and runtime document projection.

use std::collections::{BTreeMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::Result;
use chrono::{DateTime, Utc};

use serde_json::Value;
use servicenow_rs::prelude::Record;

use crate::cache::store::{KnowledgeArticleRow, ReferenceRow, RelationshipRow};
use crate::convert::{
    knowledge_article_row_from_article, reference_row_from_record_ref, reference_row_from_reference,
};
use crate::vault::VaultDocument;
use crate::{
    JournalEntry, RecordRef, Reference, SnowRecord, normalize_knowledge_article,
    normalize_record_lookup_sys_id,
};

/// Trims a borrowed string and returns it as an owned `Option<String>`,
/// dropping `None` and whitespace-only inputs.
///
/// This is the canonical crate-internal "trim to optional owned string" helper.
/// It collapses several previously-duplicated copies (`non_empty_string`) that
/// all shared identical semantics: map `Option<&str>` → trim → drop empty →
/// own. Callers that hold an owned `String` can pass `Some(value.as_str())`.
pub(crate) fn non_empty_owned(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

/// Returns the first non-empty, trimmed string from the provided candidates.
///
/// Iterates the candidates in order, skipping `None` and whitespace-only
/// values, and returns the first trimmed `&str` that has content.
pub(crate) fn first_non_empty_str<'a>(
    values: impl IntoIterator<Item = Option<&'a str>>,
) -> Option<&'a str> {
    values
        .into_iter()
        .flatten()
        .map(str::trim)
        .find(|value| !value.is_empty())
}

/// Extracts a `Record` field's raw value as a normalized sys_id.
///
/// Relocated from `lib.rs` in Task 9 (ApprovalService extraction): the
/// approval helpers need this, but so does non-approval business-application
/// relationship parsing elsewhere in `lib.rs`, so it lives here rather than
/// being moved into (or duplicated in) `service::approval`.
pub(crate) fn servicenow_reference_sys_id(record: &Record, field: &str) -> Option<String> {
    let value = record
        .get(field)
        .and_then(|field_value| field_value.value.as_ref())
        .and_then(Value::as_str)
        .or_else(|| record.get_raw(field))
        .or_else(|| record.get_str(field))?;
    normalize_record_lookup_sys_id(value).ok()
}

/// Extracts a `Record` field's display text, falling back to raw/string value.
///
/// Relocated from `lib.rs` in Task 9 alongside [`servicenow_reference_sys_id`]
/// for the same reason: shared by approval and non-approval callers.
pub(crate) fn servicenow_record_text(record: &Record, field: &str) -> Option<String> {
    record
        .get_display(field)
        .or_else(|| record.get_raw(field))
        .or_else(|| record.get_str(field))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

/// Extracts a `Record` field's raw/string value, preferring raw over display.
///
/// Relocated from `lib.rs` in Task 9 alongside [`servicenow_reference_sys_id`]
/// for the same reason: shared by approval and non-approval callers.
pub(crate) fn servicenow_record_raw_text(record: &Record, field: &str) -> Option<String> {
    record
        .get_raw(field)
        .or_else(|| record.get_str(field))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

/// Checks if a state label represents a terminal (closed/completed) state.
pub(crate) fn is_terminal_state(label: Option<&str>) -> bool {
    let Some(label) = label else {
        return false;
    };
    let label = label.to_ascii_lowercase();
    [
        "closed",
        "resolved",
        "cancel",
        "complete",
        "completed",
        "rejected",
    ]
    .iter()
    .any(|needle| label.contains(needle))
}

/// Renders journal entries to a plain-text string.
pub(crate) fn render_journal_entries(entries: &[JournalEntry]) -> String {
    entries
        .iter()
        .map(|entry| format!("{} - {}\n{}", entry.timestamp, entry.author, entry.body))
        .collect::<Vec<_>>()
        .join("\n\n")
}

/// Parses journal entries from a raw `Record` field.
pub(crate) fn collect_journal_entries(record: &Record, field: &str) -> Vec<JournalEntry> {
    record
        .parse_journal(field)
        .into_iter()
        .map(JournalEntry::from_servicenow)
        .collect()
}

/// Sorts records by their number field in ascending order.
pub(crate) fn sort_records_by_number(records: &mut [SnowRecord]) {
    records.sort_by(|left, right| left.number.cmp(&right.number));
}

/// Extracts the assigned_to sys_id from a `SnowRecord`.
///
/// Checks the references map first, then falls back to the raw field value.
pub(crate) fn document_assigned_to(record: &SnowRecord) -> Option<String> {
    for field_name in ["assigned_to", "project_manager", "demand_manager"] {
        if let Some(value) = record
            .references
            .get(field_name)
            .map(|reference| reference.sys_id.clone())
            .or_else(|| {
                record
                    .fields
                    .get(field_name)
                    .map(|field| field.value.clone())
                    .filter(|value| !value.trim().is_empty())
            })
        {
            return Some(value);
        }
    }
    None
}

/// Renders work notes from a `SnowRecord` to a plain-text string.
pub(crate) fn document_work_notes(record: &SnowRecord) -> String {
    render_journal_entries(&record.work_notes)
}

/// Renders content from a `VaultDocument` — article body, description, etc.
pub(crate) fn document_content(document: &VaultDocument) -> String {
    match document {
        VaultDocument::Knowledge(article) => article.content.clone(),
        VaultDocument::Approval(approval) => approval.record.description.clone(),
        VaultDocument::Record(record) => record.description.clone(),
    }
}

/// Renders knowledge tags into a normalized token string for FTS.
pub(crate) fn document_tag_tokens(document: &VaultDocument) -> String {
    match document {
        VaultDocument::Knowledge(article) => {
            let mut seen = HashSet::new();
            article
                .sn_tags
                .iter()
                .chain(article.auto_tags.iter())
                .chain(article.user_tags.iter())
                .filter_map(|tag| {
                    let token = tag.trim().to_ascii_lowercase();
                    if token.is_empty() || !seen.insert(token.clone()) {
                        return None;
                    }
                    Some(token)
                })
                .collect::<Vec<_>>()
                .join(" ")
        }
        _ => String::new(),
    }
}

/// Stages a markdown file for pruning by renaming it with a `.pruning` suffix.
///
/// Returns the staged path if the file existed, or `None` if it did not.
pub(crate) fn stage_markdown_for_prune(path: &Path) -> Result<Option<PathBuf>> {
    if !path.exists() {
        return Ok(None);
    }

    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("vault");
    let staged_path = path.with_file_name(format!("{file_name}.pruning"));
    fs::rename(path, &staged_path)?;
    Ok(Some(staged_path))
}

/// Restores a staged markdown file by removing the `.pruning` suffix.
pub(crate) fn restore_staged_markdown(staged_path: Option<&PathBuf>) -> Result<()> {
    let Some(staged_path) = staged_path else {
        return Ok(());
    };
    if !staged_path.exists() {
        return Ok(());
    }
    let original_name = staged_path
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_suffix(".pruning"))
        .unwrap_or("vault.md");
    let original_path = staged_path.with_file_name(original_name);
    fs::rename(staged_path, original_path)?;
    Ok(())
}

/// Converts a vault root path to the corresponding database path.
pub(crate) fn vault_root_to_db_path(vault_path: &Path) -> PathBuf {
    vault_path
        .parent()
        .map(|parent| parent.join("snow.db"))
        .unwrap_or_else(|| PathBuf::from("snow.db"))
}

/// Internal projection result for a runtime document.
#[derive(Debug, Default)]
pub(crate) struct RuntimeProjection {
    pub(crate) references: BTreeMap<String, ReferenceRow>,
    pub(crate) relationships: Vec<RelationshipRow>,
    pub(crate) knowledge_article: Option<KnowledgeArticleRow>,
}

impl RuntimeProjection {
    /// Adds a `ReferenceRow` to the projection.
    fn add_reference_row(&mut self, row: ReferenceRow) {
        self.references.insert(row.sys_id.clone(), row);
    }

    /// Adds a `Reference` to the projection.
    fn add_reference(&mut self, reference: &Reference, synced_at: DateTime<Utc>) {
        self.add_reference_row(reference_row_from_reference(reference, synced_at));
    }

    /// Adds a `RecordRef` to the projection.
    fn add_record_ref(&mut self, reference: &RecordRef, synced_at: DateTime<Utc>) {
        self.add_reference_row(reference_row_from_record_ref(reference, synced_at));
    }

    /// Adds a relationship edge to the projection.
    fn add_relationship(
        &mut self,
        source_id: &str,
        target_id: &str,
        rel_type: &str,
        field_name: &str,
    ) {
        self.relationships.push(RelationshipRow {
            source_id: source_id.to_string(),
            target_id: target_id.to_string(),
            rel_type: rel_type.to_string(),
            field_name: field_name.to_string(),
        });
    }
}

/// Projects a `VaultDocument` into a `RuntimeProjection`.
///
/// Extracts references, relationships, and (for knowledge articles)
/// article metadata rows for cache storage.
pub(crate) fn project_runtime_document(document: &VaultDocument) -> RuntimeProjection {
    let mut projection = RuntimeProjection::default();
    match document {
        VaultDocument::Record(record) => {
            project_record_projection(&mut projection, record, true);
        }
        VaultDocument::Knowledge(article) => {
            let article = normalize_knowledge_article(article.clone());
            project_record_projection(&mut projection, &article.record, false);
            projection.knowledge_article = Some(knowledge_article_row_from_article(&article));
        }
        VaultDocument::Approval(approval) => {
            let synced_at = approval.record.synced_at;
            projection.add_reference(&approval.approver, synced_at);
            projection
                .add_reference_row(reference_row_from_record_ref(&approval.target, synced_at));
            projection.add_relationship(
                &approval.record.sys_id,
                &approval.target.sys_id,
                "approval_target",
                "sysapproval",
            );
        }
    }
    projection
}

/// Projects a single record's references, children, and parent into the projection.
pub(crate) fn project_record_projection(
    projection: &mut RuntimeProjection,
    record: &SnowRecord,
    include_reference_relationships: bool,
) {
    let synced_at = record.synced_at;
    let parent_sys_id = record.parent.as_ref().map(|parent| parent.sys_id.as_str());

    if let Some(parent) = &record.parent {
        projection.add_record_ref(parent, synced_at);
        projection.add_relationship(&record.sys_id, &parent.sys_id, "parent", "parent");
    }

    for child in &record.children {
        projection.add_record_ref(child, synced_at);
        projection.add_relationship(&record.sys_id, &child.sys_id, "child", "children");
    }

    for (field_name, reference) in &record.references {
        if field_name == "parent" || parent_sys_id == Some(reference.sys_id.as_str()) {
            continue;
        }
        projection.add_reference(reference, synced_at);
        if include_reference_relationships {
            projection.add_relationship(&record.sys_id, &reference.sys_id, "reference", field_name);
        }
    }
}
