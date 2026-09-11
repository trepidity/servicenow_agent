//! Read-only, catalog-driven identifier discovery. The caller never selects a table.
use crate::context::CoreContext;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use servicenow_rs::prelude::{DisplayValue, Error as ApiError, Order, Record};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{sync::Mutex, task::JoinSet, time::timeout};

const PRIORITY_TABLES: &[&str] = &[
    "task",
    "cmdb_ci",
    "sys_user",
    "sys_user_group",
    "kb_knowledge",
    "sys_metadata",
    "resource_plan",
    "sysapproval_approver",
];
const PAGE_SIZE: u32 = 32;
const PROBE_BUDGET: usize = 32;
const CONCURRENCY: usize = 8;
const IO_DEADLINE: Duration = Duration::from_secs(3);
const CURSOR_TTL: Duration = Duration::from_secs(300);
const MAX_SESSIONS: usize = 32;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RecordResolveInput {
    pub sys_id: Option<String>,
    pub number: Option<String>,
    pub resource_type: Option<String>,
    pub name: Option<String>,
    pub table: Option<String>,
    pub cursor: Option<String>,
}

#[derive(Debug, thiserror::Error)]
pub enum RecordResolveError {
    #[error("{0}")]
    InvalidParams(String),
    #[error("{0}")]
    Unavailable(String),
    #[error("{0}")]
    Integrity(String),
}

#[derive(Debug, Serialize)]
pub struct ResolvedRecord {
    pub sys_id: String,
    pub table: String,
    pub resource_type: String,
    // Preserve provider JSON values, display values, and reference links. Do not
    // coerce arbitrary tables into SnowRecord's string-only field projection.
    pub fields: Value,
    pub data_model: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_type: Option<super::record_types::RecordType>,
}

#[derive(Debug, Serialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum RecordResolution {
    TypeMatches {
        types: Vec<super::record_types::RecordType>,
        complete: bool,
    },
    NumberNotFoundOrInaccessible {
        number: String,
    },
    NameMatches {
        name: String,
        records: Vec<super::record_name_resolver::RecordNameMatch>,
        complete: bool,
        cursor: Option<String>,
        match_count: usize,
        scanned_tables: usize,
        unreadable_tables: usize,
    },
    Document {
        sys_id: String,
        table: String,
        resource_type: String,
        content: String,
        content_hash: String,
        offset_bytes: usize,
        byte_length: usize,
        cursor: Option<String>,
    },
    Resolved {
        record: ResolvedRecord,
    },
    Searching {
        sys_id: String,
        cursor: String,
        scanned_tables: usize,
        unreadable_tables: usize,
    },
    NotFoundOrInaccessible {
        sys_id: String,
        scanned_tables: usize,
        unreadable_tables: usize,
    },
}

#[derive(Clone)]
struct SearchState {
    sys_id: String,
    pending: VecDeque<String>,
    after: String,
    catalog_complete: bool,
    scanned: usize,
    unreadable: usize,
    touched: Instant,
}

struct DocumentState {
    sys_id: String,
    table: String,
    resource_type: String,
    content: String,
    content_hash: String,
    page_offsets: HashMap<String, usize>,
    complete: bool,
    touched: Instant,
}

#[derive(Clone)]
pub(crate) struct RecordResolver {
    ctx: CoreContext,
    sessions: Arc<Mutex<HashMap<String, SearchState>>>,
    documents: Arc<Mutex<HashMap<String, DocumentState>>>,
    names: super::record_name_resolver::RecordNameResolver,
}

impl RecordResolver {
    pub(crate) fn new(ctx: CoreContext) -> Self {
        Self {
            names: super::record_name_resolver::RecordNameResolver::new(ctx.clone()),
            ctx,
            sessions: Arc::new(Mutex::new(HashMap::new())),
            documents: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub(crate) async fn resolve(
        &self,
        input: RecordResolveInput,
    ) -> Result<RecordResolution, RecordResolveError> {
        let selectors = usize::from(input.sys_id.is_some())
            + usize::from(input.name.is_some())
            + usize::from(input.number.is_some());
        if selectors > 1 {
            return Err(RecordResolveError::InvalidParams(
                "provide one of name, number, or sys_id".into(),
            ));
        }
        if selectors == 0 {
            if input.cursor.is_some() || (input.resource_type.is_some() && input.table.is_some()) {
                return Err(RecordResolveError::InvalidParams(
                    "type discovery requires only resource_type or table".into(),
                ));
            }
            let selector = input
                .resource_type
                .as_deref()
                .or(input.table.as_deref())
                .ok_or_else(|| {
                    RecordResolveError::InvalidParams(
                        "name, number, sys_id, or resource_type is required".into(),
                    )
                })?;
            return Ok(RecordResolution::TypeMatches {
                types: super::record_types::resolve_type(&self.ctx, selector).await?,
                complete: true,
            });
        }
        let mut scope = input.table.clone();
        if let Some(hint) = input
            .resource_type
            .as_deref()
            .filter(|hint| *hint != "record")
        {
            let resolved = super::record_types::unique_type(&self.ctx, hint).await?;
            if scope.as_ref().is_some_and(|table| table != &resolved.table) {
                return Err(RecordResolveError::InvalidParams(
                    "table does not match resolved resource_type".into(),
                ));
            }
            scope = Some(resolved.table);
        }
        if let Some(name) = input.name {
            if input.sys_id.is_some() {
                return Err(RecordResolveError::InvalidParams(
                    "provide name or sys_id, not both".into(),
                ));
            }
            return self.names.resolve(name, scope, input.cursor).await;
        }
        if let Some(number) = input.number {
            if input.cursor.is_some() {
                return Err(RecordResolveError::InvalidParams(
                    "continue a numbered record document with its returned sys_id and cursor"
                        .into(),
                ));
            }
            let Some(record) =
                super::record_types::lookup_number_scoped(&self.ctx, &number, scope.as_deref())
                    .await?
            else {
                return Ok(RecordResolution::NumberNotFoundOrInaccessible { number });
            };
            if scope.as_ref().is_some_and(|table| table != &record.table) {
                return Err(RecordResolveError::InvalidParams(
                    "number does not match the resolved type".into(),
                ));
            }
            return self.finish_record(record, None).await;
        }
        let sys_id =
            crate::normalize_record_lookup_sys_id(input.sys_id.as_deref().ok_or_else(|| {
                RecordResolveError::InvalidParams("name or sys_id is required".into())
            })?)
            .map_err(|error| RecordResolveError::InvalidParams(error.to_string()))?;
        {
            let mut documents = self.documents.lock().await;
            documents.retain(|_, state| state.touched.elapsed() < CURSOR_TTL);
            if let Some(cursor) = &input.cursor
                && let Some(state) = documents
                    .values_mut()
                    .find(|state| state.page_offsets.contains_key(cursor))
            {
                if state.sys_id != sys_id {
                    return Err(RecordResolveError::InvalidParams(
                        "document cursor belongs to a different identifier".into(),
                    ));
                }
                let offset = state.page_offsets[cursor];
                return Ok(document_chunk(state, offset));
            }
        }
        if let Some(table) = scope {
            if !super::record_types::identifier(&table) {
                return Err(RecordResolveError::InvalidParams(
                    "invalid table identifier".into(),
                ));
            }
            let record = self
                .ctx
                .client
                .table(&table)
                .get(&sys_id)
                .await
                .map_err(|_| {
                    RecordResolveError::Unavailable(
                        "record table is unreadable or record was not found".into(),
                    )
                })?;
            validate_identity(&record, &sys_id)?;
            if actual_table(&record)? != table {
                return Err(RecordResolveError::InvalidParams("sys_id does not match the resolved type; omit the hint to discover its actual class".into()));
            }
            return self.finish_record(record, input.cursor.as_deref()).await;
        }
        let mut search = {
            let mut sessions = self.sessions.lock().await;
            sessions.retain(|_, state| state.touched.elapsed() < CURSOR_TTL);
            if let Some(cursor) = &input.cursor {
                sessions.get(cursor).filter(|state| state.sys_id == sys_id).cloned()
                    .ok_or_else(|| RecordResolveError::InvalidParams("invalid, expired, or cross-identifier resolution cursor; start again with sys_id only".into()))?
            } else {
                if sessions.len() >= MAX_SESSIONS {
                    return Err(RecordResolveError::Unavailable("identifier discovery is at capacity; retry after active searches finish or expire".into()));
                }
                SearchState {
                    sys_id: sys_id.clone(),
                    pending: PRIORITY_TABLES
                        .iter()
                        .map(|table| (*table).to_owned())
                        .collect(),
                    after: String::new(),
                    catalog_complete: false,
                    scanned: 0,
                    unreadable: 0,
                    touched: Instant::now(),
                }
            }
        };
        let mut remaining = PROBE_BUDGET;
        while remaining > 0 {
            if search.pending.is_empty() {
                if search.catalog_complete {
                    break;
                }
                let mut query = self
                    .ctx
                    .client
                    .table("sys_db_object")
                    .fields(&["sys_id", "name"])
                    .order_by("name", Order::Asc)
                    .limit(PAGE_SIZE);
                if !search.after.is_empty() {
                    query = query.greater_than("name", &search.after);
                }
                let page = timeout(IO_DEADLINE, query.execute()).await
                    .map_err(|_| RecordResolveError::Unavailable("table catalog discovery timed out; retry this lookup".into()))?
                    .map_err(|_| RecordResolveError::Unavailable("table catalog is unavailable to the configured identity; resolution cannot complete".into()))?;
                if !page.errors.is_empty() {
                    return Err(RecordResolveError::Unavailable(
                        "table catalog returned a partial page".into(),
                    ));
                }
                search.catalog_complete = page.records.len() < PAGE_SIZE as usize;
                for entry in page.records {
                    let name = entry.get_raw("name").ok_or_else(|| {
                        RecordResolveError::Integrity("table catalog omitted a table name".into())
                    })?;
                    if !identifier(name) || name <= search.after.as_str() {
                        return Err(RecordResolveError::Integrity(
                            "table catalog returned an invalid or non-progressing name".into(),
                        ));
                    }
                    search.after = name.to_owned();
                    if !PRIORITY_TABLES.contains(&name) {
                        search.pending.push_back(name.to_owned());
                    }
                }
                // Even pages containing only priority tables consume budget.
                if search.pending.is_empty() {
                    remaining = remaining.saturating_sub(1);
                    continue;
                }
            }
            let count = CONCURRENCY.min(remaining).min(search.pending.len());
            let mut probes = JoinSet::new();
            for table in search.pending.drain(..count) {
                let client = Arc::clone(&self.ctx.client);
                let id = sys_id.clone();
                probes.spawn(async move {
                    let result = timeout(
                        IO_DEADLINE,
                        client
                            .table(&table)
                            .display_value(DisplayValue::Both)
                            .get(&id),
                    )
                    .await;
                    (table, result)
                });
            }
            remaining -= count;
            search.scanned += count;
            let mut matches = Vec::new();
            while let Some(joined) = probes.join_next().await {
                let (_, result) = joined.map_err(|_| {
                    RecordResolveError::Unavailable("record discovery worker failed".into())
                })?;
                match result {
                    Ok(Ok(record)) => matches.push(record),
                    Ok(Err(ApiError::Api { status: 404, .. })) => {}
                    _ => search.unreadable += 1,
                }
            }
            if !matches.is_empty() {
                let mut classes = HashSet::new();
                for record in &matches {
                    validate_identity(record, &sys_id)?;
                    classes.insert(actual_table(record)?);
                }
                if classes.len() != 1 {
                    return Err(RecordResolveError::Integrity(
                        "sys_id returned conflicting object classes; resolution is ambiguous"
                            .into(),
                    ));
                }
                let mut record = matches.remove(0);
                let table = actual_table(&record)?;
                if table != record.table {
                    record = timeout(
                        IO_DEADLINE,
                        self.ctx
                            .client
                            .table(&table)
                            .display_value(DisplayValue::Both)
                            .get(&sys_id),
                    )
                    .await
                    .map_err(|_| {
                        RecordResolveError::Unavailable("resolved class read timed out".into())
                    })?
                    .map_err(|_| {
                        RecordResolveError::Unavailable(
                            "resolved class is not readable; parent data is not a complete object"
                                .into(),
                        )
                    })?;
                    validate_identity(&record, &sys_id)?;
                    if actual_table(&record)? != table {
                        return Err(RecordResolveError::Integrity(
                            "class changed during identifier resolution".into(),
                        ));
                    }
                }
                let model = match &self.ctx.ui_metadata {
                    Some(client) => match client.record_model(&table).await {
                        Ok(columns) => {
                            json!({"status":"available", "table":table, "source":"live_ui_metadata", "columns":columns})
                        }
                        Err(_) => {
                            json!({"status":"unavailable", "table":table, "reason":"live_metadata_unreadable"})
                        }
                    },
                    None => {
                        json!({"status":"unavailable", "table":table, "reason":"metadata_client_not_configured"})
                    }
                };
                if let Some(cursor) = &input.cursor {
                    self.sessions.lock().await.remove(cursor);
                }
                let fields = serde_json::to_value(record.fields()).map_err(|_| {
                    RecordResolveError::Integrity("record fields could not be represented".into())
                })?;
                return self
                    .bounded_record(
                        ResolvedRecord {
                            sys_id,
                            resource_type: resource_type(&table).into(),
                            table,
                            fields,
                            data_model: model,
                            data_type: super::record_types::describe_table(
                                &self.ctx,
                                &record.table,
                            )
                            .await
                            .ok(),
                        },
                        input.cursor.as_deref(),
                    )
                    .await;
            }
        }
        if search.pending.is_empty() && search.catalog_complete {
            if let Some(cursor) = &input.cursor {
                self.sessions.lock().await.remove(cursor);
            }
            return Ok(RecordResolution::NotFoundOrInaccessible {
                sys_id,
                scanned_tables: search.scanned,
                unreadable_tables: search.unreadable,
            });
        }
        let cursor = input
            .cursor
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        search.touched = Instant::now();
        let outcome = RecordResolution::Searching {
            sys_id,
            cursor: cursor.clone(),
            scanned_tables: search.scanned,
            unreadable_tables: search.unreadable,
        };
        let mut sessions = self.sessions.lock().await;
        if !sessions.contains_key(&cursor) && sessions.len() >= MAX_SESSIONS {
            return Err(RecordResolveError::Unavailable(
                "identifier discovery is at capacity".into(),
            ));
        }
        sessions.insert(cursor, search);
        Ok(outcome)
    }

    async fn finish_record(
        &self,
        record: Record,
        cursor: Option<&str>,
    ) -> Result<RecordResolution, RecordResolveError> {
        validate_identity(&record, &record.sys_id)?;
        let table = actual_table(&record)?;
        let data_model = match &self.ctx.ui_metadata {
            Some(client) => match client.record_model(&table).await {
                Ok(columns) => {
                    json!({"status":"available","table":table,"source":"live_ui_metadata","columns":columns})
                }
                Err(_) => {
                    json!({"status":"unavailable","table":table,"reason":"live_metadata_unreadable"})
                }
            },
            None => {
                json!({"status":"unavailable","table":table,"reason":"metadata_client_not_configured"})
            }
        };
        let data_type = super::record_types::describe_table(&self.ctx, &table)
            .await
            .ok();
        self.bounded_record(
            ResolvedRecord {
                sys_id: record.sys_id.clone(),
                resource_type: resource_type(&table).into(),
                table,
                fields: serde_json::to_value(record.fields()).map_err(|_| {
                    RecordResolveError::Integrity("record fields could not be represented".into())
                })?,
                data_model,
                data_type,
            },
            cursor,
        )
        .await
    }

    async fn bounded_record(
        &self,
        record: ResolvedRecord,
        discovery_cursor: Option<&str>,
    ) -> Result<RecordResolution, RecordResolveError> {
        let content = serde_json::to_string(&record).map_err(|_| {
            RecordResolveError::Integrity("resolved record cannot be serialized".into())
        })?;
        // The MCP bridge includes both structured data and escaped JSON text.
        // Budget for that amplification below the shared 128 KiB frame limit.
        if content.len() <= 32 * 1024 {
            return Ok(RecordResolution::Resolved { record });
        }
        if content.len() > 16 * 1024 * 1024 {
            return Err(RecordResolveError::Unavailable(
                "resolved document exceeds the 16 MiB session limit; no data was truncated".into(),
            ));
        }
        let mut documents = self.documents.lock().await;
        documents.retain(|_, state| state.touched.elapsed() < CURSOR_TTL);
        if documents.len() >= MAX_SESSIONS
            && let Some(oldest) = documents
                .iter()
                .filter(|(_, state)| state.complete)
                .min_by_key(|(_, state)| state.touched)
                .map(|(key, _)| key.clone())
        {
            documents.remove(&oldest);
        }
        if documents.len() >= MAX_SESSIONS {
            return Err(RecordResolveError::Unavailable(
                "resolved document sessions are at capacity".into(),
            ));
        }
        let cursor = uuid::Uuid::new_v4().to_string();
        let mut state = DocumentState {
            sys_id: record.sys_id,
            table: record.table,
            resource_type: record.resource_type,
            content_hash: format!(
                "sha256:{}",
                Sha256::digest(content.as_bytes())
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect::<String>()
            ),
            content,
            page_offsets: discovery_cursor
                .map(|cursor| (cursor.to_owned(), 0))
                .into_iter()
                .collect(),
            complete: false,
            touched: Instant::now(),
        };
        let result = document_chunk(&mut state, 0);
        documents.insert(cursor, state);
        Ok(result)
    }
}

fn document_chunk(state: &mut DocumentState, start: usize) -> RecordResolution {
    let mut end = (start + 8192).min(state.content.len());
    while !state.content.is_char_boundary(end) {
        end -= 1;
    }
    state.touched = Instant::now();
    let next_cursor = if end < state.content.len() {
        let existing = state
            .page_offsets
            .iter()
            .find(|(_, offset)| **offset == end)
            .map(|(token, _)| token.clone());
        Some(existing.unwrap_or_else(|| {
            let token = uuid::Uuid::new_v4().to_string();
            state.page_offsets.insert(token.clone(), end);
            token
        }))
    } else {
        state.complete = true;
        None
    };
    RecordResolution::Document {
        sys_id: state.sys_id.clone(),
        table: state.table.clone(),
        resource_type: state.resource_type.clone(),
        content: state.content[start..end].to_owned(),
        content_hash: state.content_hash.clone(),
        offset_bytes: start,
        byte_length: state.content.len(),
        cursor: next_cursor,
    }
}

fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 160
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
}

fn validate_identity(record: &Record, expected: &str) -> Result<(), RecordResolveError> {
    if record.sys_id.eq_ignore_ascii_case(expected) {
        Ok(())
    } else {
        Err(RecordResolveError::Integrity(
            "provider returned a different sys_id".into(),
        ))
    }
}

pub(super) fn actual_table(record: &Record) -> Result<String, RecordResolveError> {
    if matches!(record.table.as_str(), "task" | "cmdb_ci" | "sys_metadata")
        && record.get_raw("sys_class_name").is_none_or(str::is_empty)
    {
        return Err(RecordResolveError::Unavailable(
            "parent record is readable but its actual object class is hidden; complete type resolution is unavailable".into(),
        ));
    }
    let table = match record.get_raw("sys_class_name") {
        Some("") | None
            if !record.has_field("sys_class_name")
                || record.get_raw("sys_class_name") == Some("") =>
        {
            record.table.as_str()
        }
        Some(value) => value,
        None => {
            return Err(RecordResolveError::Integrity(
                "provider returned an unreadable object class".into(),
            ));
        }
    };
    if identifier(table) {
        Ok(table.to_owned())
    } else {
        Err(RecordResolveError::Integrity(
            "provider returned an invalid object class".into(),
        ))
    }
}

// Classification is descriptive, never an admission list. Unrecognized tables
// retain their exact provider table and live model under the dynamic category.
pub(super) fn resource_type(table: &str) -> &str {
    super::record_types::resource_type(table)
}
