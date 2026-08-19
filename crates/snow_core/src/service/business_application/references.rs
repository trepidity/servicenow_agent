use super::*;

pub(super) fn is_application_service_class(class_name: Option<&str>) -> bool {
    let Some(class_name) = class_name.map(str::trim).filter(|value| !value.is_empty()) else {
        return false;
    };
    let normalized = class_name.to_ascii_lowercase();
    normalized == "cmdb_ci_service"
        || normalized.starts_with("cmdb_ci_service_")
        || normalized.contains("_application_service")
}

pub(super) fn primitive_resource_type_name(
    primitive_type: &ReferencePrimitiveType,
) -> &'static str {
    match primitive_type {
        ReferencePrimitiveType::UserPrimitive => "user_primitive",
        ReferencePrimitiveType::GroupPrimitive => "group_primitive",
        ReferencePrimitiveType::PortfolioPrimitive => "portfolio_primitive",
        ReferencePrimitiveType::ConfigurationItemPrimitive => "configuration_item_primitive",
        ReferencePrimitiveType::ReferencedRecordPrimitive => "referenced_record_primitive",
    }
}

pub(super) fn primitive_status_from_reference_status(
    status: ReferenceResolutionStatus,
) -> PrimitiveResolutionStatus {
    match status {
        ReferenceResolutionStatus::Resolved => PrimitiveResolutionStatus::Resolved,
        ReferenceResolutionStatus::Unresolved => PrimitiveResolutionStatus::Unresolved,
        ReferenceResolutionStatus::UnknownTable => PrimitiveResolutionStatus::UnknownTable,
        ReferenceResolutionStatus::NotFound => PrimitiveResolutionStatus::NotFound,
        ReferenceResolutionStatus::AclRestricted => PrimitiveResolutionStatus::AclRestricted,
        ReferenceResolutionStatus::Error => PrimitiveResolutionStatus::Error,
    }
}

pub(super) fn reason_from_reference_status(
    status: ReferenceResolutionStatus,
) -> ReferenceResolutionReason {
    match status {
        ReferenceResolutionStatus::UnknownTable => ReferenceResolutionReason::UnknownReferenceTable,
        ReferenceResolutionStatus::NotFound => ReferenceResolutionReason::ReferenceNotFound,
        ReferenceResolutionStatus::AclRestricted => {
            ReferenceResolutionReason::ReferenceAclRestricted
        }
        ReferenceResolutionStatus::Error => ReferenceResolutionReason::ReferenceResolutionFailed,
        ReferenceResolutionStatus::Resolved | ReferenceResolutionStatus::Unresolved => {
            ReferenceResolutionReason::DictionaryUnavailable
        }
    }
}

pub(super) fn reference_resolution_status_name(status: ReferenceResolutionStatus) -> &'static str {
    match status {
        ReferenceResolutionStatus::Resolved => "resolved",
        ReferenceResolutionStatus::Unresolved => "unresolved",
        ReferenceResolutionStatus::UnknownTable => "unknown_table",
        ReferenceResolutionStatus::NotFound => "not_found",
        ReferenceResolutionStatus::AclRestricted => "acl_restricted",
        ReferenceResolutionStatus::Error => "error",
    }
}

pub(super) fn primitive_display_name(
    record: &Record,
    descriptor: &ReferencePrimitiveDescriptor,
) -> String {
    record_first_value(
        record,
        &[
            "name",
            "display_name",
            "number",
            "user_name",
            "email",
            "title",
            "short_description",
        ],
    )
    .or_else(|| descriptor.display_value.clone())
    .unwrap_or_else(|| descriptor.reference_sys_id.clone())
}

pub(super) fn record_first_value(record: &Record, fields: &[&str]) -> Option<String> {
    fields.iter().find_map(|field| {
        record
            .get_display(field)
            .or_else(|| record.get_raw(field))
            .or_else(|| record.get_str(field))
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
    })
}

pub(super) fn reference_primitive_relative_path(
    descriptor: &ReferencePrimitiveDescriptor,
    display_name: &str,
) -> PathBuf {
    let (dir, prefix) = match descriptor.primitive_type {
        ReferencePrimitiveType::UserPrimitive => (PathBuf::from("users"), "user".to_string()),
        ReferencePrimitiveType::GroupPrimitive => (PathBuf::from("groups"), "group".to_string()),
        ReferencePrimitiveType::PortfolioPrimitive => {
            (PathBuf::from("portfolios"), "portfolio".to_string())
        }
        ReferencePrimitiveType::ConfigurationItemPrimitive => {
            (PathBuf::from("configuration_items"), "ci".to_string())
        }
        ReferencePrimitiveType::ReferencedRecordPrimitive => {
            let table_slug = vault::layout::slugify(&descriptor.reference_table);
            (PathBuf::from("references").join(&table_slug), table_slug)
        }
    };
    let display_slug = vault::layout::slugify(display_name);
    let file_name = if display_slug.is_empty() {
        format!("{}_{}.md", prefix, descriptor.reference_sys_id)
    } else {
        format!(
            "{}_{}_{}.md",
            prefix, descriptor.reference_sys_id, display_slug
        )
    };
    dir.join(file_name)
}

pub(super) fn render_reference_primitive_markdown(
    descriptor: &ReferencePrimitiveDescriptor,
    display_name: &str,
    status: ReferenceResolutionStatus,
    raw_json: &Value,
    diagnostic: Option<&str>,
) -> String {
    let mut out = String::new();
    out.push_str("---\n");
    out.push_str(&format!(
        "primitive_type: {}\n",
        yaml_json_string(primitive_resource_type_name(&descriptor.primitive_type))
    ));
    out.push_str(&format!(
        "resource_type: {}\n",
        yaml_json_string(primitive_resource_type_name(&descriptor.primitive_type))
    ));
    out.push_str(&format!(
        "sys_id: {}\n",
        yaml_json_string(&descriptor.reference_sys_id)
    ));
    out.push_str(&format!(
        "table: {}\n",
        yaml_json_string(&descriptor.reference_table)
    ));
    out.push_str(&format!(
        "display_name: {}\n",
        yaml_json_string(display_name)
    ));
    out.push_str(&format!(
        "source_field: {}\n",
        yaml_json_string(&descriptor.field)
    ));
    out.push_str(&format!(
        "resolution_status: {}\n",
        yaml_json_string(reference_resolution_status_name(status))
    ));
    if let Some(diagnostic) = diagnostic.filter(|value| !value.trim().is_empty()) {
        out.push_str(&format!("diagnostic: {}\n", yaml_json_string(diagnostic)));
    }
    out.push_str("---\n\n");
    out.push_str(&format!("# {}\n\n", display_name));
    out.push_str("```json\n");
    out.push_str(&serde_json::to_string_pretty(raw_json).unwrap_or_else(|_| raw_json.to_string()));
    out.push_str("\n```\n");
    out
}

pub(super) fn primitive_projected_field(
    primitive_sys_id: &str,
    field_name: &str,
    raw_value: &Value,
    updated_at: DateTime<Utc>,
) -> ProjectedFieldRow {
    let value_text = json_field_value_text(raw_value);
    let display_value = raw_value
        .as_object()
        .and_then(|map| map.get("display_value"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned);
    let reference_sys_id = value_text
        .as_deref()
        .filter(|value| looks_like_servicenow_sys_id(value) && display_value.is_some())
        .map(ToOwned::to_owned);
    let reference_table = raw_value
        .as_object()
        .and_then(|map| map.get("link"))
        .and_then(Value::as_str)
        .and_then(reference_table_from_api_link);
    let number_text = value_text
        .as_deref()
        .and_then(|value| value.parse::<f64>().ok());
    let bool_value =
        value_text
            .as_deref()
            .and_then(|value| match value.to_ascii_lowercase().as_str() {
                "true" | "1" | "yes" => Some(true),
                "false" | "0" | "no" => Some(false),
                _ => None,
            });
    let date_value = value_text
        .as_deref()
        .and_then(|value| value.get(..10))
        .and_then(|date| NaiveDate::parse_from_str(date, "%Y-%m-%d").ok())
        .map(|date| date.to_string());

    ProjectedFieldRow {
        owner_sys_id: primitive_sys_id.to_string(),
        field_name: field_name.to_string(),
        field_label: None,
        field_type: reference_sys_id.as_ref().map(|_| "reference".to_string()),
        value_text,
        display_value,
        value_number: number_text,
        value_date: date_value,
        value_bool: bool_value,
        reference_sys_id,
        reference_table,
        raw_json: raw_value.to_string(),
        updated_at,
    }
}

pub(super) fn json_field_value_text(value: &Value) -> Option<String> {
    let scalar = value
        .as_object()
        .and_then(|map| map.get("value"))
        .unwrap_or(value);
    match scalar {
        Value::String(text) => {
            let trimmed = text.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_string())
        }
        Value::Null => None,
        Value::Bool(value) => Some(value.to_string()),
        Value::Number(value) => Some(value.to_string()),
        other => Some(other.to_string()),
    }
}

pub(super) fn reference_table_from_api_link(link: &str) -> Option<String> {
    let marker = "/api/now/table/";
    let start = link.find(marker)? + marker.len();
    let table = link[start..].split('/').next()?.trim();
    (!table.is_empty()).then(|| table.to_string())
}

pub(super) fn looks_like_servicenow_sys_id(value: &str) -> bool {
    let value = value.trim();
    value.len() == 32 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

pub(super) fn yaml_json_string(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| "\"\"".to_string())
}

pub(super) fn push_unique_reference_diagnostic(
    diagnostics: &mut Vec<ReferenceResolutionDiagnostic>,
    diagnostic: ReferenceResolutionDiagnostic,
) {
    if diagnostics.iter().any(|existing| {
        existing.field == diagnostic.field
            && existing.reference_table == diagnostic.reference_table
            && existing.reference_sys_id == diagnostic.reference_sys_id
            && existing.reason == diagnostic.reason
    }) {
        return;
    }
    diagnostics.push(diagnostic);
}
