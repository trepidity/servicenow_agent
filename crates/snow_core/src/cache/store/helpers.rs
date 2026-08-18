use super::*;

pub(super) fn dedupe_kb_term_entries(
    entries: &[(String, Vec<String>)],
) -> BTreeMap<String, Vec<String>> {
    let mut deduped = BTreeMap::new();
    for (record_sys_id, terms) in entries {
        deduped.insert(record_sys_id.clone(), terms.clone());
    }
    deduped
}

pub(super) fn row_to_record_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<RecordRow> {
    let resource_type: String = row.get(3)?;
    let synced_at = from_ts(row.get::<_, i64>(10)?).map_err(to_sqlite_err)?;
    let sys_updated_on = from_ts(row.get::<_, i64>(11)?).map_err(to_sqlite_err)?;
    let last_seen_at = from_ts(row.get::<_, i64>(14)?).map_err(to_sqlite_err)?;
    Ok(RecordRow {
        sys_id: row.get(0)?,
        number: row.get(1)?,
        table_name: row.get(2)?,
        resource_type: str_to_resource_type(&resource_type).map_err(|e| {
            rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Text, Box::new(e))
        })?,
        state: row.get(4)?,
        short_desc: row.get(5)?,
        description: row.get(6)?,
        assigned_to: row.get(7)?,
        parent_id: row.get(8)?,
        file_path: row.get(9)?,
        synced_at,
        sys_updated_on,
        etag: row.get(12)?,
        in_scope: row.get::<_, i64>(13)? != 0,
        last_seen_at,
        tombstoned_at: row
            .get::<_, Option<i64>>(15)?
            .map(from_ts)
            .transpose()
            .map_err(to_sqlite_err)?,
        pruned_at: row
            .get::<_, Option<i64>>(16)?
            .map(from_ts)
            .transpose()
            .map_err(to_sqlite_err)?,
        raw_json: row.get(17)?,
    })
}

pub(super) fn row_to_business_application_projection(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<BusinessApplicationProjectionRow> {
    Ok(BusinessApplicationProjectionRow {
        record_sys_id: row.get(0)?,
        name: row.get(1)?,
        number: row.get(2)?,
        business_owner_sys_id: row.get(3)?,
        business_owner_name: row.get(4)?,
        is_owner_sys_id: row.get(5)?,
        is_owner_name: row.get(6)?,
        ci_owner_group_sys_id: row.get(7)?,
        ci_owner_group_name: row.get(8)?,
        primary_support_group_sys_id: row.get(9)?,
        primary_support_group_name: row.get(10)?,
        operational_status_value: row.get(11)?,
        operational_status_display: row.get(12)?,
        primary_portfolio_sys_id: row.get(13)?,
        primary_portfolio_name: row.get(14)?,
        primary_portfolio_table: row.get(15)?,
        attested_date: row.get(16)?,
        sys_updated_on: row.get(17)?,
        field_count: row.get::<_, i64>(18)? as usize,
        reference_count: row.get::<_, i64>(19)? as usize,
        unresolved_reference_count: row.get::<_, i64>(20)? as usize,
    })
}

pub(super) fn row_to_business_application_server_membership(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<BusinessApplicationServerMembershipRow> {
    let discovered_at = from_ts(row.get::<_, i64>(6)?).map_err(to_sqlite_err)?;
    let last_seen_at = from_ts(row.get::<_, i64>(7)?).map_err(to_sqlite_err)?;
    Ok(BusinessApplicationServerMembershipRow {
        ba_sys_id: row.get(0)?,
        server_sys_id: row.get(1)?,
        server_table: row.get(2)?,
        provenance: row.get(3)?,
        min_depth: row.get::<_, i64>(4)? as usize,
        paths_json: row.get(5)?,
        discovered_at,
        last_seen_at,
        tombstoned_at: row
            .get::<_, Option<i64>>(8)?
            .map(from_ts)
            .transpose()
            .map_err(to_sqlite_err)?,
    })
}

pub(super) fn row_to_business_application_server_inventory_health(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<BusinessApplicationServerInventoryHealthRow> {
    Ok(BusinessApplicationServerInventoryHealthRow {
        ba_sys_id: row.get(0)?,
        run_started_at: from_ts(row.get::<_, i64>(1)?).map_err(to_sqlite_err)?,
        run_completed_at: from_ts(row.get::<_, i64>(2)?).map_err(to_sqlite_err)?,
        service_membership_status: row.get(3)?,
        relationship_status: row.get(4)?,
        inventory_status: row.get(5)?,
        summary_json: row.get(6)?,
    })
}

pub(super) fn row_to_projected_field(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<ProjectedFieldRow> {
    Ok(ProjectedFieldRow {
        owner_sys_id: row.get(0)?,
        field_name: row.get(1)?,
        field_label: row.get(2)?,
        field_type: row.get(3)?,
        value_text: row.get(4)?,
        display_value: row.get(5)?,
        value_number: row.get(6)?,
        value_date: row.get(7)?,
        value_bool: row.get::<_, Option<i64>>(8)?.map(i64_to_bool),
        reference_sys_id: row.get(9)?,
        reference_table: row.get(10)?,
        raw_json: row.get(11)?,
        updated_at: from_ts(row.get(12)?).map_err(to_sqlite_err)?,
    })
}

pub(super) fn row_to_cached_user(row: &rusqlite::Row<'_>) -> rusqlite::Result<CachedUserRow> {
    Ok(CachedUserRow {
        sys_id: row.get(0)?,
        user_name: row.get(1)?,
        name: row.get(2)?,
        first_name: row.get(3)?,
        last_name: row.get(4)?,
        email: row.get(5)?,
        employee_number: row.get(6)?,
        active: row.get::<_, Option<i64>>(7)?.map(i64_to_bool),
        department: row.get(8)?,
        location: row.get(9)?,
        title: row.get(10)?,
        raw_json: row.get(11)?,
        synced_at: from_ts(row.get(12)?).map_err(to_sqlite_err)?,
        sys_updated_on: row.get(13)?,
    })
}

pub(super) fn row_to_cached_user_query(
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<CachedUserQueryRow> {
    let raw_ids: String = row.get(1)?;
    let result_sys_ids = serde_json::from_str::<Vec<String>>(&raw_ids).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(1, rusqlite::types::Type::Text, Box::new(err))
    })?;
    Ok(CachedUserQueryRow {
        query_key: row.get(0)?,
        result_sys_ids,
        synced_at: from_ts(row.get(2)?).map_err(to_sqlite_err)?,
        expires_at: from_ts(row.get(3)?).map_err(to_sqlite_err)?,
    })
}

pub(super) fn business_application_projection_from_fields(
    record: &SnowRecord,
    fields: &[ProjectedFieldRow],
) -> BusinessApplicationProjectionRow {
    let field = |name: &str| fields.iter().find(|field| field.field_name == name);
    let reference = |name: &str| {
        field(name).map(|field| {
            (
                field
                    .reference_sys_id
                    .clone()
                    .or_else(|| field.value_text.clone()),
                field
                    .display_value
                    .clone()
                    .or_else(|| field.value_text.clone())
                    .filter(|value| !value.trim().is_empty()),
                field.reference_table.clone(),
            )
        })
    };
    let (business_owner_sys_id, business_owner_name, _) =
        reference("business_owner").unwrap_or((None, None, None));
    let (is_owner_sys_id, is_owner_name, _) =
        reference("it_application_owner").unwrap_or((None, None, None));
    let (ci_owner_group_sys_id, ci_owner_group_name, _) =
        reference("managed_by_group").unwrap_or((None, None, None));
    let (primary_support_group_sys_id, primary_support_group_name, _) =
        reference("support_group").unwrap_or((None, None, None));
    let (primary_portfolio_sys_id, primary_portfolio_name, primary_portfolio_table) =
        reference("portfolio").unwrap_or((None, None, None));
    let operational_status = field("operational_status");
    let reference_count = fields
        .iter()
        .filter(|field| field.reference_sys_id.is_some())
        .count();
    let name = field("name")
        .and_then(projected_field_display_or_value)
        .or_else(|| crate::non_empty_owned(Some(&record.short_description)))
        .unwrap_or_else(|| record.sys_id.clone());

    BusinessApplicationProjectionRow {
        record_sys_id: record.sys_id.clone(),
        name,
        number: crate::non_empty_owned(Some(&record.number)),
        business_owner_sys_id,
        business_owner_name,
        is_owner_sys_id,
        is_owner_name,
        ci_owner_group_sys_id,
        ci_owner_group_name,
        primary_support_group_sys_id,
        primary_support_group_name,
        operational_status_value: operational_status.and_then(|field| field.value_text.clone()),
        operational_status_display: operational_status
            .and_then(|field| field.display_value.clone()),
        primary_portfolio_sys_id,
        primary_portfolio_name,
        primary_portfolio_table,
        attested_date: field("attested_date").and_then(|field| {
            field
                .value_date
                .clone()
                .or_else(|| projected_field_display_or_value(field))
        }),
        sys_updated_on: field("sys_updated_on").and_then(|field| {
            field
                .value_date
                .clone()
                .or_else(|| projected_field_display_or_value(field))
        }),
        field_count: fields.len(),
        reference_count,
        unresolved_reference_count: 0,
    }
}

pub(super) fn projected_fields_from_json(
    owner_sys_id: &str,
    raw_map: &Map<String, Value>,
    field_values: Option<&HashMap<String, FieldValue>>,
    dictionary: &HashMap<String, BusinessApplicationFieldDictionaryRow>,
    updated_at: DateTime<Utc>,
) -> Result<Vec<ProjectedFieldRow>> {
    let mut names = raw_map.keys().cloned().collect::<BTreeSet<_>>();
    if let Some(field_values) = field_values {
        for key in field_values.keys() {
            names.insert(key.clone());
        }
    }

    let mut rows = Vec::with_capacity(names.len());
    for field_name in &names {
        let raw = raw_map.get(field_name);
        let field_value = field_values.and_then(|fields| fields.get(field_name));
        let dictionary_row = dictionary.get(field_name);
        let raw_field = raw
            .cloned()
            .unwrap_or_else(|| field_value_json(field_value));
        let value_text = field_value
            .map(|field| field.value.clone())
            .or_else(|| raw.and_then(raw_value_text));
        let display_value = field_value
            .and_then(|field| field.display_value.clone())
            .or_else(|| raw.and_then(raw_display_value));
        let field_type = dictionary_row.and_then(|row| row.field_type.clone());
        let reference_table = dictionary_row
            .and_then(|row| row.reference_table.clone())
            .or_else(|| known_business_application_reference_table(field_name).map(str::to_string));
        let reference_sys_id = reference_table
            .as_ref()
            .and(value_text.as_deref())
            .filter(|value| is_sys_id(value))
            .map(str::to_string);
        let value_number = value_text
            .as_deref()
            .and_then(|value| value.trim().parse::<f64>().ok());
        let value_bool = parse_bool(value_text.as_deref());
        let value_date =
            projected_date_value(field_name, field_type.as_deref(), value_text.as_deref());

        rows.push(ProjectedFieldRow {
            owner_sys_id: owner_sys_id.to_string(),
            field_name: field_name.clone(),
            field_label: dictionary_row.and_then(|row| row.field_label.clone()),
            field_type,
            value_text: crate::non_empty_owned(value_text.as_deref()),
            display_value: crate::non_empty_owned(display_value.as_deref()),
            value_number,
            value_date,
            value_bool,
            reference_sys_id,
            reference_table,
            raw_json: raw_field.to_string(),
            updated_at,
        });
    }
    Ok(rows)
}

pub(super) fn field_values_to_json_map(fields: &HashMap<String, FieldValue>) -> Map<String, Value> {
    fields
        .iter()
        .map(|(name, field)| (name.clone(), field_value_json(Some(field))))
        .collect()
}

pub(super) fn field_value_json(field: Option<&FieldValue>) -> Value {
    let Some(field) = field else {
        return Value::Null;
    };
    let mut map = Map::new();
    map.insert("value".to_string(), Value::String(field.value.clone()));
    if let Some(display_value) = &field.display_value {
        map.insert(
            "display_value".to_string(),
            Value::String(display_value.clone()),
        );
    }
    Value::Object(map)
}

pub(super) fn field_value_from_raw_json(value: &Value) -> FieldValue {
    FieldValue {
        value: raw_value_text(value).unwrap_or_default(),
        display_value: raw_display_value(value),
    }
}

pub(super) fn raw_value_text(value: &Value) -> Option<String> {
    match value {
        Value::Object(map) => map.get("value").map(json_value_to_projection_string),
        Value::Null => None,
        other => Some(json_value_to_projection_string(other)),
    }
}

pub(super) fn raw_display_value(value: &Value) -> Option<String> {
    match value {
        Value::Object(map) => map
            .get("display_value")
            .map(json_value_to_projection_string)
            .and_then(|value| crate::non_empty_owned(Some(&value))),
        _ => None,
    }
}

pub(super) fn json_value_to_projection_string(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

pub(super) fn projected_date_value(
    field_name: &str,
    field_type: Option<&str>,
    value: Option<&str>,
) -> Option<String> {
    let value = value?.trim();
    if value.is_empty() {
        return None;
    }
    let field_type = field_type.unwrap_or_default().to_ascii_lowercase();
    let name = field_name.to_ascii_lowercase();
    if field_type.contains("date") || name.ends_with("_date") || name.ends_with("_on") {
        return Some(value.replace(' ', "T"));
    }
    None
}

pub(super) fn parse_bool(value: Option<&str>) -> Option<bool> {
    match value?.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Some(true),
        "false" | "0" | "no" => Some(false),
        _ => None,
    }
}

pub(super) fn known_business_application_reference_table(field_name: &str) -> Option<&'static str> {
    match field_name {
        "business_owner" | "it_application_owner" | "owned_by" | "managed_by" => Some("sys_user"),
        "managed_by_group" | "support_group" | "assignment_group" => Some("sys_user_group"),
        "cmdb_ci" => Some("cmdb_ci"),
        _ => None,
    }
}

pub(super) fn projected_field_display_or_value(field: &ProjectedFieldRow) -> Option<String> {
    field
        .display_value
        .clone()
        .or_else(|| field.value_text.clone())
        .filter(|value| !value.trim().is_empty())
}

pub(super) fn is_sys_id(value: &str) -> bool {
    value.len() == 32 && value.chars().all(|ch| ch.is_ascii_hexdigit())
}

pub(super) fn business_application_matches_query(
    row: &RecordRow,
    projection: Option<&BusinessApplicationProjectionRow>,
    fields: &BTreeMap<String, ProjectedFieldRow>,
    query: &crate::query::filter::BusinessApplicationQuery,
) -> bool {
    if let Some(text) = crate::non_empty_owned(query.text.as_deref()) {
        let text = text.to_ascii_lowercase();
        let haystack = business_application_search_text(row, projection, fields);
        if !haystack.to_ascii_lowercase().contains(&text) {
            return false;
        }
    }
    query
        .filters
        .iter()
        .all(|filter| business_application_field_matches(fields.get(&filter.field), filter))
}

pub(super) fn business_application_search_text(
    row: &RecordRow,
    projection: Option<&BusinessApplicationProjectionRow>,
    fields: &BTreeMap<String, ProjectedFieldRow>,
) -> String {
    let mut values = Vec::new();
    values.push(row.number.as_str());
    if let Some(value) = row.short_desc.as_deref() {
        values.push(value);
    }
    if let Some(value) = row.description.as_deref() {
        values.push(value);
    }
    if let Some(projection) = projection {
        values.push(projection.name.as_str());
        for value in [
            projection.business_owner_name.as_deref(),
            projection.is_owner_name.as_deref(),
            projection.ci_owner_group_name.as_deref(),
            projection.primary_support_group_name.as_deref(),
            projection.primary_portfolio_name.as_deref(),
            projection.operational_status_display.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            values.push(value);
        }
    }
    for field in fields.values() {
        if let Some(value) = field.value_text.as_deref() {
            values.push(value);
        }
        if let Some(value) = field.display_value.as_deref() {
            values.push(value);
        }
    }
    values.join(" ")
}

pub(super) fn business_application_field_matches(
    field: Option<&ProjectedFieldRow>,
    filter: &crate::query::filter::FieldFilter,
) -> bool {
    use crate::query::filter::FieldOperator;

    match filter.op {
        FieldOperator::IsEmpty => return field.map(projected_field_is_empty).unwrap_or(true),
        FieldOperator::IsNotEmpty => {
            return field
                .map(|field| !projected_field_is_empty(field))
                .unwrap_or(false);
        }
        _ => {}
    }

    let Some(field) = field else {
        return matches!(filter.op, FieldOperator::Ne);
    };

    match filter.op {
        FieldOperator::Eq => field_values(field)
            .iter()
            .any(|candidate| value_matches_exact(candidate, &filter.value)),
        FieldOperator::Ne => !field_values(field)
            .iter()
            .any(|candidate| value_matches_exact(candidate, &filter.value)),
        FieldOperator::Contains => value_as_string(&filter.value)
            .map(|needle| {
                let needle = needle.to_ascii_lowercase();
                field_values(field)
                    .iter()
                    .any(|candidate| candidate.to_ascii_lowercase().contains(&needle))
            })
            .unwrap_or(false),
        FieldOperator::StartsWith => value_as_string(&filter.value)
            .map(|needle| {
                let needle = needle.to_ascii_lowercase();
                field_values(field)
                    .iter()
                    .any(|candidate| candidate.to_ascii_lowercase().starts_with(&needle))
            })
            .unwrap_or(false),
        FieldOperator::In => filter
            .value
            .as_array()
            .map(|values| {
                values.iter().any(|value| {
                    field_values(field)
                        .iter()
                        .any(|candidate| value_matches_exact(candidate, value))
                })
            })
            .unwrap_or(false),
        FieldOperator::Gt => compare_projected_field(field, &filter.value)
            .map(|ordering| ordering == std::cmp::Ordering::Greater)
            .unwrap_or(false),
        FieldOperator::Gte => compare_projected_field(field, &filter.value)
            .map(|ordering| {
                ordering == std::cmp::Ordering::Greater || ordering == std::cmp::Ordering::Equal
            })
            .unwrap_or(false),
        FieldOperator::Lt => compare_projected_field(field, &filter.value)
            .map(|ordering| ordering == std::cmp::Ordering::Less)
            .unwrap_or(false),
        FieldOperator::Lte => compare_projected_field(field, &filter.value)
            .map(|ordering| {
                ordering == std::cmp::Ordering::Less || ordering == std::cmp::Ordering::Equal
            })
            .unwrap_or(false),
        FieldOperator::IsEmpty | FieldOperator::IsNotEmpty => unreachable!(),
    }
}

pub(super) fn projected_field_is_empty(field: &ProjectedFieldRow) -> bool {
    field_values(field)
        .into_iter()
        .all(|value| value.trim().is_empty())
}

pub(super) fn field_values(field: &ProjectedFieldRow) -> Vec<String> {
    [
        field.value_text.clone(),
        field.display_value.clone(),
        field.reference_sys_id.clone(),
    ]
    .into_iter()
    .flatten()
    .collect()
}

pub(super) fn value_matches_exact(candidate: &str, value: &Value) -> bool {
    value_as_string(value)
        .map(|value| candidate.eq_ignore_ascii_case(value.as_str()))
        .unwrap_or(false)
}

pub(super) fn value_as_string(value: &Value) -> Option<String> {
    match value {
        Value::String(value) => Some(value.clone()),
        Value::Number(value) => Some(value.to_string()),
        Value::Bool(value) => Some(value.to_string()),
        Value::Null => Some(String::new()),
        _ => None,
    }
}

pub(super) fn compare_projected_field(
    field: &ProjectedFieldRow,
    value: &Value,
) -> Option<std::cmp::Ordering> {
    if let (Some(left), Some(right)) = (
        field.value_number,
        value
            .as_f64()
            .or_else(|| value_as_string(value)?.parse::<f64>().ok()),
    ) {
        return left.partial_cmp(&right);
    }
    if let (Some(left), Some(right)) = (field.value_date.as_deref(), value_as_string(value)) {
        return Some(left.cmp(right.as_str()));
    }
    let left = field.value_text.as_deref()?;
    let right = value_as_string(value)?;
    Some(left.cmp(right.as_str()))
}

pub(super) type BusinessApplicationCandidate = (
    RecordRow,
    Option<BusinessApplicationProjectionRow>,
    BTreeMap<String, ProjectedFieldRow>,
);

pub(super) fn compare_business_application_candidates(
    left: &BusinessApplicationCandidate,
    right: &BusinessApplicationCandidate,
    sort: &[crate::query::filter::SortField],
) -> std::cmp::Ordering {
    use crate::query::filter::SortDirection;

    for sort_field in sort {
        let ordering = business_application_sort_value(left, &sort_field.field)
            .cmp(&business_application_sort_value(right, &sort_field.field));
        let ordering = match sort_field.direction {
            SortDirection::Asc => ordering,
            SortDirection::Desc => ordering.reverse(),
        };
        if ordering != std::cmp::Ordering::Equal {
            return ordering;
        }
    }
    std::cmp::Ordering::Equal
}

pub(super) fn business_application_sort_value(
    candidate: &BusinessApplicationCandidate,
    field: &str,
) -> String {
    let (row, projection, fields) = candidate;
    match field {
        "sys_id" => return row.sys_id.clone(),
        "number" => return row.number.clone(),
        "short_description" => return row.short_desc.clone().unwrap_or_default(),
        "description" => return row.description.clone().unwrap_or_default(),
        "name" => {
            if let Some(projection) = projection {
                return projection.name.clone();
            }
        }
        _ => {}
    }
    fields
        .get(field)
        .and_then(projected_field_display_or_value)
        .unwrap_or_default()
}

pub(super) fn primitive_resolution_status_to_str(
    status: PrimitiveResolutionStatus,
) -> &'static str {
    match status {
        PrimitiveResolutionStatus::Resolved => "resolved",
        PrimitiveResolutionStatus::Unresolved => "unresolved",
        PrimitiveResolutionStatus::UnknownTable => "unknown_table",
        PrimitiveResolutionStatus::NotFound => "not_found",
        PrimitiveResolutionStatus::AclRestricted => "acl_restricted",
        PrimitiveResolutionStatus::Error => "error",
    }
}

pub(super) fn primitive_resolution_status_from_str(
    value: &str,
) -> Result<PrimitiveResolutionStatus> {
    match value {
        "resolved" => Ok(PrimitiveResolutionStatus::Resolved),
        "unresolved" => Ok(PrimitiveResolutionStatus::Unresolved),
        "unknown_table" => Ok(PrimitiveResolutionStatus::UnknownTable),
        "not_found" => Ok(PrimitiveResolutionStatus::NotFound),
        "acl_restricted" => Ok(PrimitiveResolutionStatus::AclRestricted),
        "error" => Ok(PrimitiveResolutionStatus::Error),
        other => Err(StoreError::InvalidSchemaVersion(other.to_string())),
    }
}

pub(super) fn resource_type_to_str(resource_type: &ResourceType) -> &'static str {
    match resource_type {
        ResourceType::Task => "task",
        ResourceType::Incident => "incident",
        ResourceType::Change => "change_request",
        ResourceType::ChangeTask => "change_task",
        ResourceType::Request => "sc_request",
        ResourceType::RequestTask => "sc_task",
        ResourceType::Project => "pm_project",
        ResourceType::Demand => "dmn_demand",
        ResourceType::DemandTask => "dmn_demand_task",
        ResourceType::ResourcePlan => "resource_plan",
        ResourceType::Story => "rm_story",
        ResourceType::ScrumTask => "rm_scrum_task",
        ResourceType::Timecard => "time_card",
        ResourceType::Knowledge => "kb_knowledge",
        ResourceType::Approval => "sysapproval_approver",
        ResourceType::BusinessApplication => "business_application",
        ResourceType::Server => "server",
        ResourceType::PrivateTask => "private_task",
        ResourceType::Unknown => "unknown",
    }
}

pub(super) fn str_to_resource_type(input: &str) -> std::result::Result<ResourceType, StoreError> {
    Ok(match input {
        "task" => ResourceType::Task,
        "incident" => ResourceType::Incident,
        "change_request" => ResourceType::Change,
        "change_task" => ResourceType::ChangeTask,
        "sc_request" => ResourceType::Request,
        "sc_task" => ResourceType::RequestTask,
        "pm_project" => ResourceType::Project,
        "dmn_demand" => ResourceType::Demand,
        "dmn_demand_task" => ResourceType::DemandTask,
        "resource_plan" => ResourceType::ResourcePlan,
        "rm_story" => ResourceType::Story,
        "rm_scrum_task" => ResourceType::ScrumTask,
        "time_card" => ResourceType::Timecard,
        "kb_knowledge" => ResourceType::Knowledge,
        "sysapproval_approver" => ResourceType::Approval,
        "business_application" | "business_app" | "cmdb_ci_business_app" => {
            ResourceType::BusinessApplication
        }
        "server" | "cmdb_ci_server" | "cmdb_ci_linux_server" | "cmdb_ci_win_server" => {
            ResourceType::Server
        }
        "private_task" | "vtb_task" => ResourceType::PrivateTask,
        "unknown" => ResourceType::Unknown,
        other => return Err(StoreError::InvalidResourceType(other.to_string())),
    })
}

pub(super) fn to_ts(value: DateTime<Utc>) -> i64 {
    value.timestamp()
}

pub(super) fn cached_user_query_ttl() -> Duration {
    stable_reference_ttl()
}

pub(super) fn opt_ts(value: Option<DateTime<Utc>>) -> Option<i64> {
    value.map(|dt| dt.timestamp())
}

pub(super) fn bool_to_i64(value: bool) -> i64 {
    if value { 1 } else { 0 }
}

pub(super) fn i64_to_bool(value: i64) -> bool {
    value != 0
}

pub(super) fn parse_string_vec_column(
    row: &rusqlite::Row<'_>,
    index: usize,
) -> rusqlite::Result<Vec<String>> {
    let raw: String = row.get(index)?;
    serde_json::from_str(&raw).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(index, rusqlite::types::Type::Text, Box::new(err))
    })
}

pub(super) fn from_ts(value: i64) -> Result<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp(value, 0).ok_or(StoreError::InvalidTimestamp(value))
}

pub(super) fn to_sqlite_err(err: StoreError) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(err))
}

pub(super) fn coverage_to_str(value: KnowledgeEmbeddingCoverage) -> &'static str {
    match value {
        KnowledgeEmbeddingCoverage::Metadata => "metadata",
        KnowledgeEmbeddingCoverage::FullText => "full_text",
    }
}

pub(super) fn coverage_from_str(value: &str) -> Result<KnowledgeEmbeddingCoverage> {
    match value {
        "metadata" => Ok(KnowledgeEmbeddingCoverage::Metadata),
        "full_text" => Ok(KnowledgeEmbeddingCoverage::FullText),
        other => Err(StoreError::InvalidSchemaVersion(other.to_string())),
    }
}

pub(super) fn encode_embedding_vector(values: &[f32]) -> Result<Vec<u8>> {
    if !crate::semantic::is_unit_length(values) {
        return Err(StoreError::NonUnitEmbeddingVector);
    }
    let mut out = Vec::with_capacity(values.len() * 4);
    for value in values {
        out.extend_from_slice(&value.to_le_bytes());
    }
    Ok(out)
}

pub(super) fn decode_embedding_vector(raw: &[u8], dimensions: usize) -> Result<Vec<f32>> {
    let expected = dimensions * 4;
    if raw.len() != expected {
        return Err(StoreError::InvalidEmbeddingVectorLength {
            expected,
            actual: raw.len(),
        });
    }
    let mut out = Vec::with_capacity(dimensions);
    for chunk in raw.chunks_exact(4) {
        out.push(f32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
    }
    if !crate::semantic::is_unit_length(&out) {
        return Err(StoreError::NonUnitEmbeddingVector);
    }
    Ok(out)
}
