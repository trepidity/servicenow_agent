//! Exact, metadata-verified names; candidates remain references for record_resolve.
use super::record_resolver::{RecordResolution, RecordResolveError as Error, resource_type};
use crate::context::CoreContext;
use serde::Serialize;
use servicenow_rs::prelude::{DisplayValue, Operator, Order};
use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::Mutex, time::timeout};

const PRIORITY: &[&str] = &[
    "rm_sprint",
    "rm_release_scrum",
    "sys_user_group",
    "sys_user",
    "task",
    "cmdb_ci",
    "kb_knowledge",
];
const NAME_FIELDS: &[&str] = &[
    "name",
    "u_name",
    "x_name",
    "short_description",
    "title",
    "number",
    "user_name",
    "email",
];
const ROW_LIMIT: u32 = 20;
const TABLE_BUDGET: usize = 2;
const TTL: Duration = Duration::from_secs(300);

#[derive(Debug, Serialize)]
pub struct RecordNameMatch {
    pub sys_id: String,
    pub table: String,
    pub resource_type: String,
    pub name: String,
    pub number: Option<String>,
    pub matched_fields: Vec<String>,
}

#[derive(Clone)]
struct Search {
    name: String,
    table: Option<String>,
    pending: VecDeque<String>,
    catalog_after: String,
    catalog_complete: bool,
    row_after: Option<String>,
    seen: HashSet<String>,
    scanned: usize,
    unreadable: usize,
    touched: Instant,
}

#[derive(Clone)]
pub(crate) struct RecordNameResolver {
    ctx: CoreContext,
    searches: Arc<Mutex<HashMap<String, Search>>>,
}

impl RecordNameResolver {
    pub(crate) fn new(ctx: CoreContext) -> Self {
        Self {
            ctx,
            searches: Arc::default(),
        }
    }

    pub(crate) async fn resolve(
        &self,
        name: String,
        table: Option<String>,
        cursor: Option<String>,
    ) -> Result<RecordResolution, Error> {
        let name = name.trim().to_owned();
        if name.is_empty()
            || name.len() > 200
            || name.chars().any(char::is_control)
            || name.contains('^')
            || name.to_ascii_lowercase().contains("javascript:")
            || name.contains("${")
        {
            return Err(Error::InvalidParams(
                "name must be 1-200 bytes of literal text without encoded-query syntax".into(),
            ));
        }
        let table = table.map(|value| value.trim().to_ascii_lowercase());
        if table.as_deref().is_some_and(|value| !identifier(value)) {
            return Err(Error::InvalidParams(
                "table must be a ServiceNow table identifier".into(),
            ));
        }
        let mut search = {
            let mut searches = self.searches.lock().await;
            searches.retain(|_, state| state.touched.elapsed() < TTL);
            if let Some(cursor) = &cursor {
                searches
                    .get(cursor)
                    .filter(|state| state.name == name && state.table == table)
                    .cloned()
                    .ok_or_else(|| {
                        Error::InvalidParams(
                            "invalid, expired, or cross-selector name cursor".into(),
                        )
                    })?
            } else {
                if searches.len() >= 32 {
                    return Err(Error::Unavailable(
                        "name discovery is at capacity; retry after active searches expire".into(),
                    ));
                }
                Search {
                    name: name.clone(),
                    table: table.clone(),
                    pending: table
                        .iter()
                        .cloned()
                        .chain(if table.is_none() {
                            PRIORITY.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>()
                        } else {
                            vec![]
                        })
                        .collect(),
                    catalog_after: String::new(),
                    catalog_complete: table.is_some(),
                    row_after: None,
                    seen: HashSet::new(),
                    scanned: 0,
                    unreadable: 0,
                    touched: Instant::now(),
                }
            }
        };
        let mut records = Vec::new();
        for _ in 0..TABLE_BUDGET {
            if search.pending.is_empty() && !search.catalog_complete {
                let mut query = self
                    .ctx
                    .client
                    .table("sys_db_object")
                    .fields(&["name"])
                    .order_by("name", Order::Asc)
                    .limit(32)
                    .no_count();
                if !search.catalog_after.is_empty() {
                    query = query.greater_than("name", &search.catalog_after);
                }
                let page = timeout(Duration::from_secs(3), query.execute())
                    .await
                    .map_err(|_| Error::Unavailable("table catalog discovery timed out".into()))?
                    .map_err(|_| {
                        Error::Unavailable(
                            "table catalog is unreadable; name discovery cannot finish".into(),
                        )
                    })?;
                if !page.errors.is_empty() {
                    return Err(Error::Unavailable(
                        "table catalog returned a partial page".into(),
                    ));
                }
                search.catalog_complete = page.records.len() < 32;
                for row in page.records {
                    let table = row
                        .get_raw("name")
                        .filter(|name| identifier(name) && *name > search.catalog_after.as_str())
                        .ok_or_else(|| {
                            Error::Integrity(
                                "table catalog returned an invalid or non-progressing name".into(),
                            )
                        })?;
                    search.catalog_after = table.into();
                    if !PRIORITY.contains(&table) {
                        search.pending.push_back(table.into());
                    }
                }
            }
            let Some(current) = search.pending.front().cloned() else {
                continue;
            };
            let result = timeout(
                Duration::from_secs(10),
                self.search_table(&current, &name, search.row_after.as_deref()),
            )
            .await;
            match result {
                Ok(Ok((matches, next, metadata_complete))) => {
                    if search.row_after.is_none() {
                        search.scanned += 1;
                        if !metadata_complete {
                            search.unreadable += 1;
                        }
                    }
                    for candidate in matches {
                        if search.seen.insert(candidate.sys_id.clone()) {
                            records.push(candidate);
                        }
                    }
                    if search.seen.len() > 10000 {
                        return Err(Error::Unavailable(
                            "name has too many matches; narrow the table".into(),
                        ));
                    }
                    search.row_after = next;
                    if search.row_after.is_none() {
                        search.pending.pop_front();
                    }
                }
                Ok(Err(Error::Integrity(message))) => return Err(Error::Integrity(message)),
                failure => {
                    if table.is_some() {
                        return Err(match failure {
                            Ok(Err(error)) => error,
                            _ => Error::Unavailable("name lookup timed out while discovering metadata or querying records".into()),
                        });
                    }
                    search.scanned += 1;
                    search.unreadable += 1;
                    search.row_after = None;
                    search.pending.pop_front();
                }
            }
            if !records.is_empty() {
                break;
            }
        }
        let exhausted = search.pending.is_empty() && search.catalog_complete;
        // Retain immutable input cursor snapshots: replay cannot skip candidates.
        let next_cursor = if exhausted {
            None
        } else {
            Some(uuid::Uuid::new_v4().to_string())
        };
        let result = RecordResolution::NameMatches {
            name,
            records,
            complete: exhausted && search.unreadable == 0,
            cursor: next_cursor.clone(),
            match_count: search.seen.len(),
            scanned_tables: search.scanned,
            unreadable_tables: search.unreadable,
        };
        if let Some(next) = next_cursor {
            search.touched = Instant::now();
            let mut searches = self.searches.lock().await;
            searches.retain(|_, state| state.touched.elapsed() < TTL);
            if searches.len() >= 128 {
                return Err(Error::Unavailable(
                    "name cursor storage is at capacity; retry after expiry".into(),
                ));
            }
            searches.insert(next, search);
        }
        Ok(result)
    }

    async fn search_table(
        &self,
        table: &str,
        name: &str,
        after: Option<&str>,
    ) -> Result<(Vec<RecordNameMatch>, Option<String>, bool), Error> {
        let metadata = self
            .ctx
            .ui_metadata
            .as_ref()
            .ok_or_else(|| Error::Unavailable("live metadata client is unavailable".into()))?
            .record_model(table)
            .await
            .map_err(|_| Error::Unavailable("live table model is unreadable".into()))?;
        let columns = metadata
            .as_object()
            .ok_or_else(|| Error::Integrity("table model omitted columns".into()))?;
        let mut fields: BTreeSet<String> = NAME_FIELDS
            .iter()
            .filter(|field| columns.contains_key(**field))
            .map(|field| (*field).into())
            .collect();
        for (field, column) in columns {
            if column
                .get("display")
                .is_some_and(|value| value == true || value == "true")
                && identifier(field)
            {
                fields.insert(field.clone());
            }
        }
        // The dictionary owns custom display fields, including inherited definitions.
        let (display, metadata_complete) = self.display_field(table).await;
        if let Some(field) = display {
            if !identifier(&field) || !columns.contains_key(&field) {
                return Err(Error::Integrity(
                    "display field is absent from the live table model".into(),
                ));
            }
            fields.insert(field);
        }
        if fields.is_empty() {
            return if metadata_complete {
                Ok((vec![], None, true))
            } else {
                Err(Error::Unavailable(
                    "no readable name field definition".into(),
                ))
            };
        }
        let mut projection = fields.iter().map(String::as_str).collect::<Vec<_>>();
        projection.extend(["sys_id", "sys_class_name"]);
        if columns.contains_key("number") && !fields.contains("number") {
            projection.push("number");
        }
        let mut query = self
            .ctx
            .client
            .table(table)
            .fields(&projection)
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .limit(ROW_LIMIT)
            .no_count()
            .order_by("sys_id", Order::Asc);
        for (index, field) in fields.iter().enumerate() {
            query = if index == 0 {
                query.equals(field, name)
            } else {
                query.or_filter(field, Operator::Equals, name)
            };
        }
        if let Some(after) = after {
            query = query.greater_than("sys_id", after);
        }
        let page = query
            .execute()
            .await
            .map_err(|_| Error::Unavailable("name query is unreadable".into()))?;
        if !page.errors.is_empty() || page.records.len() > ROW_LIMIT as usize {
            return Err(Error::Unavailable(
                "name query returned a partial or oversized page".into(),
            ));
        }
        let next = if page.records.len() == ROW_LIMIT as usize {
            page.records.last().map(|row| row.sys_id.clone())
        } else {
            None
        };
        let mut previous = after.unwrap_or("").to_owned();
        let mut matches = Vec::new();
        for row in page.records {
            let id = crate::normalize_record_lookup_sys_id(&row.sys_id)
                .map_err(|_| Error::Integrity("name query returned an invalid sys_id".into()))?;
            if id <= previous {
                return Err(Error::Integrity(
                    "name query returned a non-progressing page".into(),
                ));
            }
            previous = id.clone();
            let actual = row
                .get_raw("sys_class_name")
                .filter(|value| !value.is_empty())
                .unwrap_or(table);
            if !identifier(actual) {
                return Err(Error::Integrity(
                    "name query returned an invalid object class".into(),
                ));
            }
            let matched_fields = fields
                .iter()
                .filter(|field| {
                    row.get_raw(field)
                        .is_some_and(|value| value.to_lowercase() == name.to_lowercase())
                })
                .cloned()
                .collect::<Vec<_>>();
            if matched_fields.is_empty() {
                return Err(Error::Integrity(
                    "provider returned a record that does not match the exact name".into(),
                ));
            }
            let number = row
                .get_raw("number")
                .filter(|value| !value.is_empty())
                .map(str::to_owned);
            if number.as_ref().is_some_and(|value| value.len() > 200) {
                return Err(Error::Integrity(
                    "record number exceeds the lookup response bound".into(),
                ));
            }
            matches.push(RecordNameMatch {
                sys_id: id,
                table: actual.into(),
                resource_type: resource_type(actual).into(),
                name: name.into(),
                number,
                matched_fields,
            });
        }
        Ok((matches, next, metadata_complete))
    }

    async fn display_field(&self, table: &str) -> (Option<String>, bool) {
        // Prefer the nearest explicit definition. A single catalog read exposes
        // the remaining ancestry; do not issue two network requests per level.
        let own = self
            .ctx
            .client
            .table("sys_dictionary")
            .fields(&["element"])
            .equals("name", table)
            .equals("display", "true")
            .limit(2)
            .no_count()
            .execute()
            .await;
        let Ok(own) = own else {
            return (None, false);
        };
        if !own.errors.is_empty() || own.records.len() > 1 {
            return (None, false);
        }
        if let Some(row) = own.records.first() {
            return (
                row.get_raw("element").map(str::to_owned),
                row.get_raw("element").is_some(),
            );
        }
        let paths = (1..=16)
            .map(|depth| format!("{}name", "super_class.".repeat(depth)))
            .collect::<Vec<_>>();
        let path_refs = paths.iter().map(String::as_str).collect::<Vec<_>>();
        let catalog = self
            .ctx
            .client
            .table("sys_db_object")
            .fields(&["name", "super_class"])
            .dot_walk(&path_refs)
            .equals("name", table)
            .limit(2)
            .no_count()
            .execute()
            .await;
        let Ok(catalog) = catalog else {
            return (None, false);
        };
        if !catalog.errors.is_empty() || catalog.records.len() != 1 {
            return (None, false);
        }
        let row = &catalog.records[0];
        if row.get_raw("name") != Some(table) {
            return (None, false);
        }
        if row.get_raw("super_class") == Some("") {
            return (None, true);
        }
        let mut parents = Vec::new();
        let mut ended = false;
        for path in &paths {
            match row.get_raw(path) {
                Some("") => {
                    ended = true;
                    break;
                }
                Some(parent)
                    if identifier(parent) && parent != table && !parents.contains(&parent) =>
                {
                    parents.push(parent)
                }
                _ => return (None, false),
            }
        }
        if !ended || parents.is_empty() {
            return (None, false);
        }
        let query = self
            .ctx
            .client
            .table("sys_dictionary")
            .fields(&["name", "element"]);
        let query = if let [parent] = parents.as_slice() {
            query.equals("name", parent)
        } else {
            query.in_list("name", &parents)
        };
        let inherited = query
            .equals("display", "true")
            .limit(17)
            .no_count()
            .execute()
            .await;
        let Ok(inherited) = inherited else {
            return (None, false);
        };
        if !inherited.errors.is_empty() || inherited.records.len() >= 17 {
            return (None, false);
        }
        for parent in parents {
            let matching = inherited
                .records
                .iter()
                .filter(|row| row.get_raw("name") == Some(parent))
                .collect::<Vec<_>>();
            if matching.len() > 1 {
                return (None, false);
            }
            if let Some(row) = matching.first() {
                return (
                    row.get_raw("element").map(str::to_owned),
                    row.get_raw("element").is_some(),
                );
            }
        }
        (None, true)
    }
}

fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 160
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}
