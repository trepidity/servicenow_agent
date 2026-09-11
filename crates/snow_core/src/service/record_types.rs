//! Shared read-side translation. Surfaces pass selectors; ServiceNow owns tables.
use super::record_resolver::{RecordResolveError as Error, actual_table};
use crate::context::CoreContext;
use serde::Serialize;
use servicenow_rs::prelude::{DisplayValue, Error as ApiError, Operator, Record};
use std::collections::BTreeSet;

// Compatibility names for existing domain operations, not table admission rules.
const COMPATIBILITY_TYPES: &[(&str, &str)] = &[
    ("rm_story", "story"),
    ("rm_sprint", "sprint"),
    ("rm_release_scrum", "release"),
    ("sys_user_group", "assignment_group"),
    ("rm_scrum_task", "story_task"),
    ("sc_request", "request"),
    ("sc_req_item", "request_item"),
    ("sc_task", "request_task"),
    ("pm_project", "project"),
    ("pm_project_task", "project_task"),
    ("dmn_demand", "demand"),
    ("dmn_demand_task", "demand_task"),
    ("time_card", "timecard"),
    ("sys_user", "user"),
    ("sysapproval_approver", "approval"),
    ("kb_knowledge", "knowledge_article"),
    ("cmdb_ci_business_app", "business_application"),
];

pub(super) fn resource_type(table: &str) -> &str {
    COMPATIBILITY_TYPES
        .iter()
        .find(|(name, _)| *name == table)
        .map_or(table, |(_, kind)| *kind)
}

#[derive(Debug, Clone, Serialize)]
pub struct RecordType {
    pub table: String,
    pub label: String,
    pub resource_type: String,
    pub source: String,
}

pub(crate) fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 160
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'_')
}

fn literal(value: &str) -> Result<&str, Error> {
    let value = value.trim();
    if value.is_empty()
        || value.len() > 160
        || value.chars().any(char::is_control)
        || value.contains('^')
        || value.contains("${")
        || value.to_ascii_lowercase().contains("javascript:")
    {
        return Err(Error::InvalidParams(
            "record type must be a literal table name, label, reference field, or number prefix"
                .into(),
        ));
    }
    Ok(value)
}

async fn table_definition(
    ctx: &CoreContext,
    table: &str,
    source: &str,
) -> Result<RecordType, Error> {
    if !identifier(table) {
        return Err(Error::Integrity(
            "metadata returned an invalid table".into(),
        ));
    }
    let page = ctx
        .client
        .table("sys_db_object")
        .equals("name", table)
        .fields(&["name", "label"])
        .limit(2)
        .no_count()
        .execute()
        .await
        .map_err(|_| Error::Unavailable("table definition is unreadable".into()))?;
    if !page.errors.is_empty()
        || page.records.len() != 1
        || page.records[0].get_raw("name") != Some(table)
    {
        return Err(Error::Integrity(
            "table definition is missing, ambiguous, or did not match".into(),
        ));
    }
    let label = page.records[0]
        .get_raw("label")
        .filter(|s| !s.is_empty())
        .ok_or_else(|| Error::Unavailable("table label is unreadable".into()))?;
    Ok(RecordType {
        table: table.into(),
        label: label.into(),
        resource_type: resource_type(table).into(),
        source: source.into(),
    })
}

pub(crate) async fn describe_table(ctx: &CoreContext, table: &str) -> Result<RecordType, Error> {
    table_definition(ctx, table, "table_metadata").await
}

/// Prefix metadata is authoritative when present and readable. Installations may
/// hide or omit it; retain the core client's configured/standard prefixes then.
/// This fallback is reported to type-discovery callers and never lives in MCP.
async fn prefix_tables(
    ctx: &CoreContext,
    prefix: &str,
) -> Result<(Vec<String>, &'static str), Error> {
    let page = ctx
        .client
        .table("sys_number")
        .equals("prefix", prefix)
        .fields(&["prefix", "category"])
        .limit(21)
        .no_count()
        .execute()
        .await;
    let page = match page {
        Ok(page) => page,
        Err(
            ApiError::Auth {
                status: Some(403), ..
            }
            | ApiError::Api {
                status: 403 | 404, ..
            },
        ) => {
            return ctx.table_for_number(&format!("{prefix}0"))
                .map(|table|(vec![table],"core_prefix_fallback"))
                .ok_or_else(||Error::Unavailable("number-prefix metadata is unreadable and the core has no configured prefix mapping".into()));
        }
        Err(_) => {
            return Err(Error::Unavailable(
                "number-prefix metadata is unavailable".into(),
            ));
        }
    };
    if !page.errors.is_empty() || page.records.len() > 20 {
        return Err(Error::Unavailable(
            "number-prefix metadata is incomplete".into(),
        ));
    }
    let mut tables = BTreeSet::new();
    for row in page.records {
        if !row
            .get_raw("prefix")
            .is_some_and(|s| s.eq_ignore_ascii_case(prefix))
        {
            return Err(Error::Integrity(
                "number-prefix metadata did not match".into(),
            ));
        }
        let table = row
            .get_raw("category")
            .filter(|s| identifier(s))
            .ok_or_else(|| Error::Integrity("number-prefix metadata omitted its table".into()))?;
        tables.insert(table.to_owned());
    }
    if tables.is_empty()
        && let Some(table) = ctx.table_for_number(&format!("{prefix}0"))
    {
        return Ok((vec![table], "core_prefix_fallback"));
    }
    Ok((tables.into_iter().collect(), "number_metadata"))
}

pub(crate) async fn resolve_type(
    ctx: &CoreContext,
    selector: &str,
) -> Result<Vec<RecordType>, Error> {
    let selector = literal(selector)?;
    let is_prefix = selector.len() <= 40
        && selector
            .bytes()
            .all(|c| c.is_ascii_uppercase() || c.is_ascii_digit());
    if is_prefix
        && let Ok((tables, source)) = prefix_tables(ctx, selector).await
        && !tables.is_empty()
    {
        let mut result = Vec::new();
        for table in tables {
            result.push(table_definition(ctx, &table, source).await?);
        }
        return Ok(result);
    }
    let table_name = selector.to_ascii_lowercase();
    let label = selector.replace('_', " ");
    let alias = COMPATIBILITY_TYPES
        .iter()
        .find(|(_, kind)| *kind == table_name)
        .map(|(table, _)| *table);
    let mut query = ctx
        .client
        .table("sys_db_object")
        .fields(&["name", "label"])
        .equals("name", &table_name)
        .or_filter("label", Operator::Equals, &label)
        .limit(21)
        .no_count();
    if let Some(alias) = alias {
        query = query.or_filter("name", Operator::Equals, alias);
    }
    let page = query
        .execute()
        .await
        .map_err(|_| Error::Unavailable("table catalog is unreadable".into()))?;
    if !page.errors.is_empty() || page.records.len() > 20 {
        return Err(Error::Unavailable(
            "table selector is too broad or metadata is incomplete".into(),
        ));
    }
    let mut result = Vec::new();
    for row in page.records {
        let table = row
            .get_raw("name")
            .filter(|s| identifier(s))
            .ok_or_else(|| Error::Integrity("table catalog returned an invalid name".into()))?;
        let actual_label = row
            .get_raw("label")
            .filter(|s| !s.is_empty())
            .ok_or_else(|| Error::Unavailable("table label is unreadable".into()))?;
        if table != table_name && Some(table) != alias && !actual_label.eq_ignore_ascii_case(&label)
        {
            return Err(Error::Integrity(
                "table catalog returned an unrelated type".into(),
            ));
        }
        result.push(RecordType {
            table: table.into(),
            label: actual_label.into(),
            resource_type: resource_type(table).into(),
            source: "table_metadata".into(),
        });
    }
    if !result.is_empty() {
        return Ok(result);
    }
    // Reference field names/labels (e.g. a lead developer) identify their target
    // table through dictionary metadata, including instance-defined fields.
    let page = ctx
        .client
        .table("sys_dictionary")
        .fields(&["element", "column_label", "reference"])
        .equals("element", &table_name)
        .or_filter("column_label", Operator::Equals, &label)
        .limit(101)
        .no_count()
        .execute()
        .await
        .map_err(|_| Error::Unavailable("reference field metadata is unreadable".into()))?;
    if !page.errors.is_empty() || page.records.len() > 100 {
        return Err(Error::Unavailable("reference selector is too broad".into()));
    }
    let mut tables = BTreeSet::new();
    for row in page.records {
        if row.get_raw("element") != Some(table_name.as_str())
            && !row
                .get_raw("column_label")
                .is_some_and(|s| s.eq_ignore_ascii_case(&label))
        {
            return Err(Error::Integrity("reference metadata did not match".into()));
        }
        if let Some(table) = row.get_raw("reference").filter(|s| !s.is_empty()) {
            if !identifier(table) {
                return Err(Error::Integrity(
                    "reference metadata returned an invalid table".into(),
                ));
            }
            tables.insert(table.to_owned());
        }
    }
    for table in tables {
        result.push(table_definition(ctx, &table, "reference_metadata").await?);
    }
    if result.is_empty() && selector.bytes().all(|c| c.is_ascii_alphabetic()) {
        let (tables, source) = prefix_tables(ctx, &selector.to_ascii_uppercase()).await?;
        for table in tables {
            result.push(table_definition(ctx, &table, source).await?);
        }
    }
    Ok(result)
}

pub(crate) async fn unique_type(ctx: &CoreContext, selector: &str) -> Result<RecordType, Error> {
    let mut types = resolve_type(ctx, selector).await?;
    if types.len() != 1 {
        return Err(Error::InvalidParams("type selector is missing or ambiguous; call record_resolve with resource_type only and select its canonical table".into()));
    }
    Ok(types.remove(0))
}

pub(crate) async fn lookup_number(
    ctx: &CoreContext,
    number: &str,
) -> Result<Option<Record>, Error> {
    lookup_number_scoped(ctx, number, None).await
}

pub(crate) async fn lookup_number_scoped(
    ctx: &CoreContext,
    number: &str,
    scope: Option<&str>,
) -> Result<Option<Record>, Error> {
    let number = literal(number)?.to_ascii_uppercase();
    if number.len() > 80
        || !number
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'_')
        || !number.as_bytes().last().is_some_and(u8::is_ascii_digit)
    {
        return Err(Error::InvalidParams(
            "number must be a ServiceNow record number".into(),
        ));
    }
    let prefix = number.trim_end_matches(|c: char| c.is_ascii_digit());
    if prefix.is_empty() {
        return Err(Error::InvalidParams(
            "record number requires a prefix".into(),
        ));
    }
    let mapping = if let Some(table) = scope {
        if !identifier(table) {
            return Err(Error::InvalidParams("invalid table scope".into()));
        }
        Ok((vec![table.to_owned()], "explicit_table"))
    } else {
        prefix_tables(ctx, prefix).await
    };
    let tables = match &mapping {
        Err(Error::Integrity(message)) => return Err(Error::Integrity(message.clone())),
        Err(Error::InvalidParams(message)) => return Err(Error::InvalidParams(message.clone())),
        Ok((tables, _)) if !tables.is_empty() => tables.clone(),
        _ => vec!["task".into()],
    };
    let mut found: Option<Record> = None;
    for table in tables {
        let page = ctx
            .client
            .table(&table)
            .equals("number", &number)
            .display_value(DisplayValue::Both)
            .limit(2)
            .no_count()
            .execute()
            .await
            .map_err(|_| Error::Unavailable("number lookup table is unreadable".into()))?;
        if !page.errors.is_empty() || page.records.len() > 1 {
            return Err(Error::Integrity(
                "record number is ambiguous or provider returned a partial page".into(),
            ));
        }
        for mut row in page.records {
            if row.get_raw("number") != Some(number.as_str()) {
                return Err(Error::Integrity(
                    "number lookup returned an unrelated record".into(),
                ));
            }
            let actual = actual_table(&row)?;
            if actual != table {
                let expected_id = row.sys_id.clone();
                row = ctx
                    .client
                    .table(&actual)
                    .get(&row.sys_id)
                    .await
                    .map_err(|_| {
                        Error::Unavailable("number's actual class is unreadable".into())
                    })?;
                if row.sys_id != expected_id
                    || row.get_raw("number") != Some(number.as_str())
                    || actual_table(&row)? != actual
                {
                    return Err(Error::Integrity(
                        "record changed during number resolution".into(),
                    ));
                }
            }
            if found
                .as_ref()
                .is_some_and(|other| other.sys_id != row.sys_id)
            {
                return Err(Error::Integrity(
                    "record number matches multiple types".into(),
                ));
            }
            found = Some(row);
        }
    }
    if found.is_none()
        && let Err(error) = mapping
    {
        return Err(error);
    }
    Ok(found)
}
