use chrono::Utc;
use servicenow_rs::model::value::parse_servicenow_timestamp;
use servicenow_rs::prelude::Record;

use crate::{
    ApprovalRecord, CacheSource, FieldValue, RecordRef, Reference, ResourceType, SnowRecord,
};

#[derive(Debug, Clone, Default)]
pub struct ApprovalResource;

impl ApprovalResource {
    pub fn from_servicenow(record: &Record, approver: Reference) -> ApprovalRecord {
        let target = target_reference(record);
        ApprovalRecord {
            record: Self::record_model(record, &approver, &target),
            approver,
            target,
            requested_at: parse_servicenow_timestamp(record.get_str("sys_created_on"))
                .unwrap_or_else(Utc::now),
            due_date: parse_servicenow_timestamp(record.get_str("due_date")),
        }
    }

    pub fn record_model(record: &Record, approver: &Reference, target: &RecordRef) -> SnowRecord {
        let mut model = SnowRecord::from_servicenow(record);
        model.resource_type = ResourceType::Approval;
        model.table = "sysapproval_approver".to_string();
        model.synced_at = Utc::now();
        model.source = CacheSource::Api;
        model
            .references
            .insert("approver".to_string(), approver.clone());
        model.references.insert(
            "sysapproval".to_string(),
            Reference {
                sys_id: target.sys_id.clone(),
                table: target.table.clone(),
                display_name: target.number.clone(),
                extra: std::collections::HashMap::new(),
            },
        );
        model
    }

    pub fn approver_reference(record: &Record) -> Option<Reference> {
        reference_from_record(record, "approver", "sys_user")
    }

    pub fn field_values(record: &Record) -> std::collections::HashMap<String, FieldValue> {
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

fn target_reference(record: &Record) -> RecordRef {
    let sysapproval_sys_id = record
        .get_raw("sysapproval")
        .or_else(|| record.get_str("sysapproval"))
        .unwrap_or_default()
        .to_string();
    let number = record
        .get_str("sysapproval.number")
        .or_else(|| record.get_str("document_id"))
        .unwrap_or_default()
        .to_string();
    let table =
        crate::canonical_record_table_for_number(target_table(record).unwrap_or_default(), &number);

    RecordRef {
        sys_id: sysapproval_sys_id,
        number,
        table,
    }
}

fn target_table(record: &Record) -> Option<&str> {
    non_empty(record.get_raw("source_table"))
        .or_else(|| non_empty(record.get_raw("sysapproval.sys_class_name")))
        .or_else(|| non_empty(record.get_str("source_table")))
        .or_else(|| non_empty(record.get_str("sysapproval.sys_class_name")))
}

fn non_empty(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

fn reference_from_record(record: &Record, field: &str, table: &str) -> Option<Reference> {
    let sys_id = record.get_raw(field).or_else(|| record.get_str(field))?;
    let display_name = record.get_display(field).unwrap_or(sys_id).to_string();

    Some(Reference {
        sys_id: sys_id.to_string(),
        table: table.to_string(),
        display_name,
        extra: std::collections::HashMap::new(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use servicenow_rs::model::value::FieldValue as SnFieldValue;

    fn set_field(record: &mut Record, field: &str, value: &str, display_value: &str) {
        record.set(
            field,
            SnFieldValue {
                value: Some(serde_json::json!(value)),
                display_value: Some(display_value.to_string()),
                link: None,
            },
        );
    }

    #[test]
    fn approval_target_uses_raw_source_table_over_display_label() {
        let mut record = Record::new("sysapproval_approver", "apr-sys");
        set_field(&mut record, "sysapproval", "chg-sys", "CHG0332518");
        set_field(
            &mut record,
            "sysapproval.number",
            "CHG0332518",
            "CHG0332518",
        );
        set_field(
            &mut record,
            "source_table",
            "change_request",
            "Change Request",
        );
        set_field(
            &mut record,
            "sysapproval.sys_class_name",
            "change_request_normal",
            "Normal Change",
        );

        let target = target_reference(&record);

        assert_eq!(target.sys_id, "chg-sys");
        assert_eq!(target.number, "CHG0332518");
        assert_eq!(target.table, "change_request");
    }

    #[test]
    fn approval_target_falls_back_to_raw_sys_class_name() {
        let mut record = Record::new("sysapproval_approver", "apr-sys");
        set_field(&mut record, "sysapproval", "chg-sys", "CHG0332518");
        set_field(
            &mut record,
            "sysapproval.number",
            "CHG0332518",
            "CHG0332518",
        );
        set_field(
            &mut record,
            "sysapproval.sys_class_name",
            "change_request",
            "Change Request",
        );

        let target = target_reference(&record);

        assert_eq!(target.table, "change_request");
    }

    #[test]
    fn approval_target_canonicalizes_display_table_label() {
        let mut record = Record::new("sysapproval_approver", "apr-sys");
        set_field(&mut record, "sysapproval", "chg-sys", "CHG0332518");
        set_field(
            &mut record,
            "sysapproval.number",
            "CHG0332518",
            "CHG0332518",
        );
        set_field(
            &mut record,
            "source_table",
            "Change Request",
            "Change Request",
        );

        let target = target_reference(&record);

        assert_eq!(target.table, "change_request");
    }
}
