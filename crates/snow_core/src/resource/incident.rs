use chrono::Utc;
use servicenow_rs::prelude::Record;

use crate::{
    CacheSource, FieldValue, JournalEntry, RecordRef, Reference, ResourceType, SnowRecord,
};

#[derive(Debug, Clone, Default)]
pub struct IncidentResource;

impl IncidentResource {
    pub fn from_servicenow(record: &Record) -> SnowRecord {
        let mut model = SnowRecord::from_servicenow(record);
        model.resource_type = ResourceType::Incident;
        model.table = "incident".to_string();
        model.synced_at = Utc::now();
        model.source = CacheSource::Api;
        model
    }

    pub fn record_ref(record: &Record) -> RecordRef {
        RecordRef {
            sys_id: record.sys_id.clone(),
            number: record.get_str("number").unwrap_or_default().to_string(),
            table: "incident".to_string(),
        }
    }

    pub fn caller_reference(record: &Record) -> Option<Reference> {
        reference_from_record(record, "caller_id", "sys_user")
    }

    pub fn assigned_to_reference(record: &Record) -> Option<Reference> {
        reference_from_record(record, "assigned_to", "sys_user")
    }

    pub fn assignment_group_reference(record: &Record) -> Option<Reference> {
        reference_from_record(record, "assignment_group", "sys_user_group")
    }

    pub fn ci_reference(record: &Record) -> Option<Reference> {
        reference_from_record(record, "cmdb_ci", "cmdb_ci")
    }

    pub fn work_notes(record: &Record) -> Vec<JournalEntry> {
        record
            .parse_journal("work_notes")
            .into_iter()
            .map(JournalEntry::from_servicenow)
            .collect()
    }

    pub fn comments(record: &Record) -> Vec<JournalEntry> {
        record
            .parse_journal("comments")
            .into_iter()
            .map(JournalEntry::from_servicenow)
            .collect()
    }

    pub fn fields(record: &Record) -> std::collections::HashMap<String, FieldValue> {
        record
            .fields()
            .iter()
            .map(|(key, value)| {
                (
                    key.clone(),
                    FieldValue {
                        value: value
                            .value
                            .as_ref()
                            .map(|v| match v {
                                serde_json::Value::String(s) => s.clone(),
                                other => other.to_string(),
                            })
                            .unwrap_or_default(),
                        display_value: value.display_value.clone(),
                    },
                )
            })
            .collect()
    }
}

fn reference_from_record(record: &Record, field: &str, table: &str) -> Option<Reference> {
    let sys_id = record.get_raw(field).or_else(|| record.get_str(field))?;
    let display_name = record.get_display(field).unwrap_or(sys_id).to_string();
    let mut extra = std::collections::HashMap::new();

    for dot_field in record.dot_walked_fields(field) {
        if let Some((prefix, _suffix)) = dot_field.0.split_once('.')
            && prefix == field
        {
            if let Some(display) = dot_field.1.display_str() {
                extra.insert(dot_field.0.to_string(), display.to_string());
            } else if let Some(raw) = dot_field.1.raw_str() {
                extra.insert(dot_field.0.to_string(), raw.to_string());
            }
        }
    }

    Some(Reference {
        sys_id: sys_id.to_string(),
        table: table.to_string(),
        display_name,
        extra,
    })
}
