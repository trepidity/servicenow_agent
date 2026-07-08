//! `UserService` — user lookup and search, extracted from the `SnowCore`
//! god-object.
//!
//! This is the first domain service extracted in the library boundary
//! migration (Task 8); it sets the pattern for the remaining service
//! extractions (Tasks 9-11). Every method/helper body below is moved
//! verbatim from its former `impl SnowCore` / free-fn location in
//! `lib.rs` — only the receiver changed, from `SnowCore`'s `ctx` field to
//! `UserService`'s own (identically-named and identically-typed) `ctx`
//! field, so no field-access rewriting was needed.

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use servicenow_rs::prelude::{DisplayValue, Order, Record};

use crate::cache::store::CachedUserRow;
use crate::context::CoreContext;
use crate::convert::serialize_record_document;
use crate::helpers::{first_non_empty_str, non_empty_owned};
use crate::normalize_record_lookup_sys_id;

const USER_LOOKUP_FIELDS: &[&str] = &[
    "sys_id",
    "user_name",
    "name",
    "first_name",
    "last_name",
    "email",
    "employee_number",
    "active",
    "department",
    "location",
    "title",
];
const USER_SEARCH_DEFAULT_LIMIT: usize = 20;
const USER_SEARCH_MAX_LIMIT: usize = 100;

#[derive(Debug, Clone, PartialEq, Eq)]
struct UserLookupCandidate {
    field: &'static str,
    value: String,
}

fn stable_reference_cache_is_fresh(
    synced_at: DateTime<Utc>,
    now: DateTime<Utc>,
    ttl: chrono::Duration,
) -> bool {
    now.signed_duration_since(synced_at) <= ttl
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub struct UserLookup {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub employee_number: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sys_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(deny_unknown_fields)]
pub struct UserSearch {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name_contains: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email_contains: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub limit: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active: Option<bool>,
}

impl UserLookup {
    pub fn validate_selector(&self) -> Result<()> {
        let count = [
            self.query.as_deref(),
            self.user_name.as_deref(),
            self.email.as_deref(),
            self.employee_number.as_deref(),
            self.sys_id.as_deref(),
        ]
        .into_iter()
        .filter(|value| value.is_some_and(|value| !value.trim().is_empty()))
        .count();
        if count == 1 {
            Ok(())
        } else if count == 0 {
            anyhow::bail!(
                "missing required user lookup: provide exactly one of query, user_name, email, employee_number, or sys_id"
            )
        } else {
            anyhow::bail!(
                "ambiguous user lookup: provide exactly one of query, user_name, email, employee_number, or sys_id"
            )
        }
    }
}

impl UserSearch {
    pub fn validate(&self) -> Result<()> {
        self.validated_limit()?;
        let count = [
            self.first_name.as_deref(),
            self.last_name.as_deref(),
            self.name_contains.as_deref(),
            self.email_contains.as_deref(),
        ]
        .into_iter()
        .filter(|value| value.is_some_and(|value| !value.trim().is_empty()))
        .count();
        if count == 0 {
            anyhow::bail!(
                "missing required user search: provide at least one of first_name, last_name, name_contains, or email_contains"
            );
        }
        Ok(())
    }

    pub fn validated_limit(&self) -> Result<usize> {
        let limit = self.limit.unwrap_or(USER_SEARCH_DEFAULT_LIMIT);
        if limit == 0 {
            anyhow::bail!("`limit` must be at least 1");
        }
        if limit > USER_SEARCH_MAX_LIMIT {
            anyhow::bail!("`limit` must be at most {USER_SEARCH_MAX_LIMIT}");
        }
        Ok(limit)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UserLookupResult {
    pub matched_by: String,
    pub user: UserRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UserRecord {
    pub sys_id: String,
    pub table: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub first_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub email: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub employee_number: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub active: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub department: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub location: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    pub display: String,
}

fn user_lookup_candidates(lookup: &UserLookup) -> Result<Vec<UserLookupCandidate>> {
    if let Some(sys_id) = non_empty_owned(lookup.sys_id.as_deref()) {
        return Ok(vec![UserLookupCandidate {
            field: "sys_id",
            value: normalize_record_lookup_sys_id(&sys_id)?,
        }]);
    }
    if let Some(user_name) = non_empty_owned(lookup.user_name.as_deref()) {
        return Ok(vec![UserLookupCandidate {
            field: "user_name",
            value: user_name,
        }]);
    }
    if let Some(email) = non_empty_owned(lookup.email.as_deref()) {
        return Ok(vec![UserLookupCandidate {
            field: "email",
            value: email,
        }]);
    }
    if let Some(employee_number) = non_empty_owned(lookup.employee_number.as_deref()) {
        return Ok(vec![UserLookupCandidate {
            field: "employee_number",
            value: employee_number,
        }]);
    }

    let Some(query) = non_empty_owned(lookup.query.as_deref()) else {
        anyhow::bail!(
            "missing required user lookup: provide exactly one of query, user_name, email, employee_number, or sys_id"
        );
    };

    if query.contains('@') {
        return Ok(vec![
            UserLookupCandidate {
                field: "email",
                value: query.clone(),
            },
            UserLookupCandidate {
                field: "user_name",
                value: query,
            },
        ]);
    }
    if let Ok(sys_id) = normalize_record_lookup_sys_id(&query) {
        return Ok(vec![UserLookupCandidate {
            field: "sys_id",
            value: sys_id,
        }]);
    }

    Ok(vec![
        UserLookupCandidate {
            field: "user_name",
            value: query.clone(),
        },
        UserLookupCandidate {
            field: "email",
            value: query.clone(),
        },
        UserLookupCandidate {
            field: "employee_number",
            value: query,
        },
    ])
}

fn user_record_from_record(record: &Record) -> UserRecord {
    let display = first_non_empty_str([
        record.get_str("name"),
        record.get_str("user_name"),
        record.get_str("email"),
    ])
    .unwrap_or(record.sys_id.as_str())
    .to_string();

    UserRecord {
        sys_id: record.sys_id.clone(),
        table: "sys_user".to_string(),
        user_name: non_empty_owned(record.get_str("user_name")),
        name: non_empty_owned(record.get_str("name")),
        first_name: non_empty_owned(record.get_str("first_name")),
        last_name: non_empty_owned(record.get_str("last_name")),
        email: non_empty_owned(record.get_str("email")),
        employee_number: non_empty_owned(record.get_str("employee_number")),
        active: bool_field(record, "active"),
        department: non_empty_owned(record.get_str("department")),
        location: non_empty_owned(record.get_str("location")),
        title: non_empty_owned(record.get_str("title")),
        display,
    }
}

fn user_record_from_cached_row(row: &CachedUserRow) -> UserRecord {
    let display = first_non_empty_str([
        row.name.as_deref(),
        row.user_name.as_deref(),
        row.email.as_deref(),
    ])
    .unwrap_or(row.sys_id.as_str())
    .to_string();

    UserRecord {
        sys_id: row.sys_id.clone(),
        table: "sys_user".to_string(),
        user_name: row.user_name.clone(),
        name: row.name.clone(),
        first_name: row.first_name.clone(),
        last_name: row.last_name.clone(),
        email: row.email.clone(),
        employee_number: row.employee_number.clone(),
        active: row.active,
        department: row.department.clone(),
        location: row.location.clone(),
        title: row.title.clone(),
        display,
    }
}

fn cached_user_row_from_record(record: &Record, synced_at: DateTime<Utc>) -> CachedUserRow {
    CachedUserRow {
        sys_id: record.sys_id.clone(),
        user_name: non_empty_owned(record.get_str("user_name")),
        name: non_empty_owned(record.get_str("name")),
        first_name: non_empty_owned(record.get_str("first_name")),
        last_name: non_empty_owned(record.get_str("last_name")),
        email: non_empty_owned(record.get_str("email")),
        employee_number: non_empty_owned(record.get_str("employee_number")),
        active: bool_field(record, "active"),
        department: non_empty_owned(record.get_str("department")),
        location: non_empty_owned(record.get_str("location")),
        title: non_empty_owned(record.get_str("title")),
        raw_json: serialize_record_document(record).to_string(),
        synced_at,
        sys_updated_on: record
            .get_raw("sys_updated_on")
            .or_else(|| record.get_str("sys_updated_on"))
            .map(ToOwned::to_owned),
    }
}

fn cache_key_value(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn user_lookup_cache_key(field: &str, value: &str, active: bool) -> String {
    format!(
        "lookup|active={}|{}={}",
        active,
        field,
        cache_key_value(value)
    )
}

fn user_search_cache_key(params: &UserSearch) -> Result<String> {
    params.validate()?;
    let mut parts = vec![format!("active={}", params.active.unwrap_or(true))];
    if let Some(value) = non_empty_owned(params.first_name.as_deref()) {
        parts.push(format!("first_name={}", cache_key_value(&value)));
    }
    if let Some(value) = non_empty_owned(params.last_name.as_deref()) {
        parts.push(format!("last_name={}", cache_key_value(&value)));
    }
    if let Some(value) = non_empty_owned(params.name_contains.as_deref()) {
        parts.push(format!("name_contains={}", cache_key_value(&value)));
    }
    if let Some(value) = non_empty_owned(params.email_contains.as_deref()) {
        parts.push(format!("email_contains={}", cache_key_value(&value)));
    }
    parts.push(format!("limit={}", params.validated_limit()?));
    Ok(format!("search|{}", parts.join("|")))
}

fn bool_field(record: &Record, field: &str) -> Option<bool> {
    let value = record.get(field)?;
    value
        .value
        .as_ref()
        .and_then(Value::as_bool)
        .or_else(|| {
            value
                .value
                .as_ref()
                .and_then(Value::as_str)
                .and_then(parse_bool)
        })
        .or_else(|| value.display_value.as_deref().and_then(parse_bool))
}

fn parse_bool(value: &str) -> Option<bool> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" | "1" | "yes" => Some(true),
        "false" | "0" | "no" => Some(false),
        _ => None,
    }
}

#[derive(Clone)]
pub(crate) struct UserService {
    ctx: CoreContext,
}

impl UserService {
    pub(crate) fn new(ctx: CoreContext) -> Self {
        Self { ctx }
    }

    pub async fn lookup_user(&self, lookup: UserLookup) -> Result<Option<UserLookupResult>> {
        lookup.validate_selector()?;
        let candidates = user_lookup_candidates(&lookup)?;
        let active = lookup.active.unwrap_or(true);
        let now = Utc::now();

        for candidate in candidates {
            let query_key = user_lookup_cache_key(candidate.field, &candidate.value, active);
            if let Some(users) = self.cached_users_for_query_key(&query_key, now)? {
                match users.as_slice() {
                    [] => continue,
                    [user] => {
                        return Ok(Some(UserLookupResult {
                            matched_by: candidate.field.to_string(),
                            user: user.clone(),
                        }));
                    }
                    _ => {
                        anyhow::bail!(
                            "multiple cached sys_user records matched {}={}",
                            candidate.field,
                            candidate.value
                        )
                    }
                }
            }
            let response = self
                .ctx
                .client
                .table("sys_user")
                .equals(candidate.field, &candidate.value)
                .equals("active", if active { "true" } else { "false" })
                .fields(USER_LOOKUP_FIELDS)
                .display_value(DisplayValue::Both)
                .limit(2)
                .execute()
                .await?;

            if response.records.is_empty() {
                self.persist_user_query_cache(&query_key, &[], Utc::now())?;
                continue;
            }
            if response.records.len() > 1 {
                anyhow::bail!(
                    "multiple sys_user records matched {}={}",
                    candidate.field,
                    candidate.value
                );
            }

            let mut users =
                self.persist_user_query_cache(&query_key, &response.records, Utc::now())?;
            let user = users
                .pop()
                .expect("single user persisted after non-empty response");
            return Ok(Some(UserLookupResult {
                matched_by: candidate.field.to_string(),
                user,
            }));
        }

        Ok(None)
    }

    pub async fn search_users(&self, params: UserSearch) -> Result<Vec<UserRecord>> {
        params.validate()?;
        let query_key = user_search_cache_key(&params)?;
        let now = Utc::now();
        if let Some(users) = self.cached_users_for_query_key(&query_key, now)? {
            return Ok(users);
        }
        let mut query = self
            .ctx
            .client
            .table("sys_user")
            .equals(
                "active",
                if params.active.unwrap_or(true) {
                    "true"
                } else {
                    "false"
                },
            )
            .fields(USER_LOOKUP_FIELDS)
            .display_value(DisplayValue::Both)
            .limit(params.validated_limit()? as u32)
            .order_by("name", Order::Asc);

        if let Some(first_name) = non_empty_owned(params.first_name.as_deref()) {
            query = query.equals("first_name", &first_name);
        }
        if let Some(last_name) = non_empty_owned(params.last_name.as_deref()) {
            query = query.equals("last_name", &last_name);
        }
        if let Some(name) = non_empty_owned(params.name_contains.as_deref()) {
            query = query.contains("name", &name);
        }
        if let Some(email) = non_empty_owned(params.email_contains.as_deref()) {
            query = query.contains("email", &email);
        }

        let records = query.execute().await?.records;
        self.persist_user_query_cache(&query_key, &records, Utc::now())
    }

    fn cached_users_for_query_key(
        &self,
        query_key: &str,
        now: DateTime<Utc>,
    ) -> Result<Option<Vec<UserRecord>>> {
        let Some(query_row) = self
            .ctx
            .query
            .store()
            .get_cached_user_query_result(query_key, now)?
        else {
            return Ok(None);
        };
        if query_row.result_sys_ids.is_empty() {
            return Ok(Some(Vec::new()));
        }
        let cached_rows = self
            .ctx
            .query
            .store()
            .list_cached_users_by_sys_ids(&query_row.result_sys_ids)?;
        if cached_rows.len() != query_row.result_sys_ids.len()
            || cached_rows.iter().any(|row| {
                !stable_reference_cache_is_fresh(
                    row.synced_at,
                    now,
                    self.ctx.cache_policy.stable_reference_ttl(),
                )
            })
        {
            return Ok(None);
        }
        Ok(Some(
            cached_rows
                .iter()
                .map(user_record_from_cached_row)
                .collect(),
        ))
    }

    fn persist_user_query_cache(
        &self,
        query_key: &str,
        records: &[Record],
        synced_at: DateTime<Utc>,
    ) -> Result<Vec<UserRecord>> {
        let mut users = Vec::with_capacity(records.len());
        let mut sys_ids = Vec::with_capacity(records.len());
        for record in records {
            let user = user_record_from_record(record);
            self.ctx
                .query
                .store()
                .upsert_cached_user(&cached_user_row_from_record(record, synced_at))?;
            sys_ids.push(user.sys_id.clone());
            users.push(user);
        }
        self.ctx
            .query
            .store()
            .put_cached_user_query_result_with_ttl(
                query_key,
                &sys_ids,
                synced_at,
                self.ctx.cache_policy.stable_reference_ttl(),
            )?;
        Ok(users)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::core_for_mock_server;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn user_search_builds_live_first_and_last_name_filters() {
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/sys_user"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "fedcba9876543210fedcba9876543210",
                    "user_name": "USER1234",
                    "name": "Casey User",
                    "first_name": "Casey",
                    "last_name": "User",
                    "email": "user@example.com",
                    "employee_number": "1234",
                    "active": "true",
                    "department": "IAM",
                    "location": "Main Street",
                    "title": "Engineer"
                }]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let users = core
            .search_users(UserSearch {
                first_name: Some("Casey".to_string()),
                last_name: Some("User".to_string()),
                limit: Some(10),
                ..Default::default()
            })
            .await
            .expect("users");

        assert_eq!(users.len(), 1);
        assert_eq!(users[0].first_name.as_deref(), Some("Casey"));
        assert_eq!(users[0].last_name.as_deref(), Some("User"));

        let cached_users = core
            .search_users(UserSearch {
                first_name: Some("Casey".to_string()),
                last_name: Some("User".to_string()),
                limit: Some(10),
                ..Default::default()
            })
            .await
            .expect("cached users");
        assert_eq!(cached_users, users);

        let requests = server.received_requests().await.expect("requests");
        let user_requests = requests
            .iter()
            .filter(|request| request.url.path() == "/api/now/table/sys_user")
            .collect::<Vec<_>>();
        assert_eq!(user_requests.len(), 1);
        let request = user_requests[0];
        let query = request
            .url
            .query_pairs()
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(
            query.get("sysparm_query").map(|value| value.as_ref()),
            Some("active=true^first_name=Casey^last_name=User^ORDERBYname")
        );
        assert_eq!(
            query.get("sysparm_limit").map(|value| value.as_ref()),
            Some("10")
        );
        assert_eq!(
            query
                .get("sysparm_display_value")
                .map(|value| value.as_ref()),
            Some("all")
        );
    }
}
