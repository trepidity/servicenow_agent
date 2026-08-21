//! `ServerService` — CMDB server lookup and search (exact-match live fetch,
//! bounded search, cached BA/server association reads), extracted from the
//! `SnowCore` god-object.
//!
//! Fourth domain service extracted in the library boundary migration
//! (Task 10), alongside `BusinessApplicationService`, following the pattern
//! `UserService` (Task 8) and `ApprovalService` (Task 9) established. Every
//! method/helper/type/const body below is moved verbatim from its former
//! `impl SnowCore` / free-fn location in `lib.rs`.
//!
//! `apply_reference_name_or_sys_id_filter` and `is_servicenow_acl_error` are
//! shared with `BusinessApplicationService` (Task 10's other extraction), so
//! they were relocated to `crate::helpers` rather than duplicated or
//! privatized here.

use anyhow::Result;
use servicenow_rs::prelude::{DisplayValue, Error as SnowApiError, Operator, Order, Record};
use servicenow_rs::query::TableApi;

use crate::context::CoreContext;
use crate::helpers::{
    apply_reference_name_or_sys_id_filter, is_servicenow_acl_error, non_empty_owned,
};
use crate::resource::business_application::{
    BusinessApplicationServersCachedParams, BusinessApplicationServersCachedResult,
    BusinessApplicationsForServerParams, BusinessApplicationsForServerResult,
};
use crate::resource::server::{
    SERVER_DEFAULT_LIMIT, SERVER_LEAF_TABLES, SERVER_TABLE, Server, ServerLookup, ServerQuery,
    ServerSearchParams, canonical_server_class,
};
use crate::{SnowRecord, normalize_record_lookup_sys_id};

/// Structured outcome for a live `server_get` fetch.
///
/// The read-through `server_get` contract distinguishes authoritative
/// not-found (ServiceNow confirmed the CI does not exist or is unreadable for
/// ACL reasons) from transient failures. Collapsing all of these into a single
/// `anyhow` error would make a network blip indistinguishable from a real
/// not-found, which the FR explicitly forbids. Each variant carries enough to
/// let the daemon and MCP layers map it to a distinct JSON-RPC error code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerGetError {
    /// ServiceNow confirmed the record does not exist (HTTP 404 on a sys_id
    /// read, or an empty exact-match result set for a name / IP query).
    NotFound,
    /// The caller's ACL prevents reading the record (HTTP 401/403 or an
    /// authentication/authorization failure). The CI may exist.
    AclRestricted(String),
    /// A transport-level / timeout failure reaching ServiceNow. Never
    /// conflated with not-found.
    Network(String),
    /// More than one CI matched an exact name / IP selector (duplicate CIs in
    /// CMDB). Surfaced as a structured disambiguation signal, not a generic
    /// internal error.
    Disambiguation { selector: String, matched: usize },
    /// A row was returned but could not be parsed into the typed `Server`
    /// shape.
    Hydration(String),
    /// Any other failure (validation, cache write, etc.).
    Other(String),
}

impl ServerGetError {
    /// Classify a `servicenow_rs` API error into the structured variant.
    /// (HTTP 404 is handled by the caller and never reaches here.)
    fn from_api(err: SnowApiError) -> Self {
        match &err {
            SnowApiError::Api {
                status: 401 | 403, ..
            } => Self::AclRestricted(err.to_string()),
            SnowApiError::Auth { .. } => Self::AclRestricted(err.to_string()),
            SnowApiError::Http(_) | SnowApiError::RateLimited { .. } => {
                Self::Network(err.to_string())
            }
            _ if is_servicenow_acl_error(&err) => Self::AclRestricted(err.to_string()),
            _ => Self::Other(err.to_string()),
        }
    }
}

impl std::fmt::Display for ServerGetError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound => write!(f, "server not found"),
            Self::AclRestricted(detail) => {
                write!(f, "server is ACL-restricted: {detail}")
            }
            Self::Network(detail) => write!(f, "network error reaching ServiceNow: {detail}"),
            Self::Disambiguation { selector, matched } => {
                write!(f, "multiple servers matched {selector} ({matched} matches)")
            }
            Self::Hydration(detail) => write!(f, "server record failed hydration: {detail}"),
            Self::Other(detail) => write!(f, "{detail}"),
        }
    }
}

impl std::error::Error for ServerGetError {}

#[derive(Clone)]
pub(crate) struct ServerService {
    ctx: CoreContext,
}

impl ServerService {
    pub(crate) fn new(ctx: CoreContext) -> Self {
        Self { ctx }
    }

    /// Force-live exact server fetch, persisting the hit to the local cache.
    ///
    /// Back-compat wrapper around [`SnowCore::get_server_live`] with
    /// `persist = true`; flattens the structured [`ServerGetError`] into the
    /// crate's `anyhow` error so existing callers (the daemon
    /// `server_get_fresh` RPC) keep their current signature. New read-through
    /// callers that need to distinguish not-found / ACL / network / duplicate
    /// should call [`SnowCore::get_server_live`] directly.
    pub async fn get_server_fresh(&self, lookup: ServerLookup) -> Result<Option<Server>> {
        match self.get_server_live(lookup, true).await {
            Ok(server) => Ok(server),
            Err(ServerGetError::NotFound) => Ok(None),
            Err(other) => Err(anyhow::anyhow!(other.to_string())),
        }
    }

    /// Live, exact-match server fetch against `cmdb_ci_server` (base table, so
    /// all server subclasses are returned transparently). This is the single
    /// shared live code path behind the read-through `server_get`: the daemon /
    /// CLI path calls it with `persist = true`, the MCP path with
    /// `persist = false` (mutation-free MCP per the completion plan's Work
    /// Package G boundary).
    ///
    /// Returns:
    /// - `Ok(Some(server))` when ServiceNow returns exactly one matching CI.
    /// - `Err(ServerGetError::NotFound)` only when ServiceNow confirms absence
    ///   (HTTP 404 for a sys_id read, or an empty result set for a name / IP
    ///   query). This is authoritative not-found.
    /// - `Err(ServerGetError::AclRestricted)` on HTTP 401/403 — the CI may
    ///   exist but the caller cannot read it; distinct from not-found.
    /// - `Err(ServerGetError::Network)` on a transport/timeout failure — never
    ///   conflated with not-found.
    /// - `Err(ServerGetError::Disambiguation)` when more than one CI matches an
    ///   exact name / IP selector (duplicate CIs in CMDB).
    /// - `Err(ServerGetError::Hydration)` when a returned row cannot be parsed
    ///   into the typed `Server` shape.
    ///
    /// When `persist` is `true` and a record is found, the raw record is written
    /// to the local cache via [`SnowCore::persist_record`].
    pub async fn get_server_live(
        &self,
        lookup: ServerLookup,
        persist: bool,
    ) -> std::result::Result<Option<Server>, ServerGetError> {
        let record = match lookup {
            ServerLookup::SysId(sys_id) => {
                let sys_id = normalize_record_lookup_sys_id(&sys_id)
                    .map_err(|err| ServerGetError::Other(err.to_string()))?;
                match self
                    .ctx
                    .client
                    .table(SERVER_TABLE)
                    .display_value(DisplayValue::Both)
                    .exclude_reference_link(true)
                    .get(&sys_id)
                    .await
                {
                    Ok(record) => Some(record),
                    Err(SnowApiError::Api { status: 404, .. }) => None,
                    Err(err) => return Err(ServerGetError::from_api(err)),
                }
            }
            ServerLookup::ExactName(name) => {
                let name = non_empty_owned(Some(&name)).ok_or_else(|| {
                    ServerGetError::Other("server name cannot be empty".to_string())
                })?;
                let records = self.server_exact_query("name", &name).await?;
                if records.len() > 1 {
                    return Err(ServerGetError::Disambiguation {
                        selector: format!("name={name}"),
                        matched: records.len(),
                    });
                }
                records.into_iter().next()
            }
            ServerLookup::IpAddress(ip_address) => {
                let ip_address = non_empty_owned(Some(&ip_address)).ok_or_else(|| {
                    ServerGetError::Other("server IP address cannot be empty".to_string())
                })?;
                let records = self.server_exact_query("ip_address", &ip_address).await?;
                if records.len() > 1 {
                    return Err(ServerGetError::Disambiguation {
                        selector: format!("ip_address={ip_address}"),
                        matched: records.len(),
                    });
                }
                records.into_iter().next()
            }
        };

        let Some(record) = record else {
            return Err(ServerGetError::NotFound);
        };
        let server = Server::from_servicenow(&record)
            .map_err(|err| ServerGetError::Hydration(err.to_string()))?;
        if persist {
            self.ctx
                .persist_record(&record)
                .map_err(|err| ServerGetError::Other(err.to_string()))?;
        }
        Ok(Some(server))
    }

    /// Issue a bounded exact-match (`field = value`) query against the base
    /// `cmdb_ci_server` table for the read-through live fallback. `limit: 2` is
    /// sufficient to detect duplicate-CI ambiguity without fetching the whole
    /// set. Transport / ACL failures are classified into [`ServerGetError`].
    async fn server_exact_query(
        &self,
        field: &str,
        value: &str,
    ) -> std::result::Result<Vec<Record>, ServerGetError> {
        let query = self
            .ctx
            .client
            .table(SERVER_TABLE)
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .limit(2)
            .order_by("name", Order::Asc)
            .equals(field, value);
        match query.execute().await {
            Ok(result) => Ok(result.records),
            Err(err) => Err(ServerGetError::from_api(err)),
        }
    }

    pub async fn search_servers(&self, params: ServerSearchParams) -> Result<Vec<SnowRecord>> {
        Ok(self
            .search_servers_live(params)
            .await?
            .into_iter()
            .map(|server| server.record)
            .collect())
    }

    pub async fn search_servers_live(&self, params: ServerSearchParams) -> Result<Vec<Server>> {
        self.search_servers_policy_live(params, true).await
    }

    /// Execute the fixed typed Server search against ServiceNow, optionally
    /// persisting the complete live records for read-through policy.
    pub async fn search_servers_policy_live(
        &self,
        params: ServerSearchParams,
        persist: bool,
    ) -> Result<Vec<Server>> {
        params.validate()?;
        let records = self.server_base_query(params)?.execute().await?.records;
        let mut servers = Vec::with_capacity(records.len());
        for record in records {
            let server = Server::from_servicenow(&record)?;
            if persist {
                self.ctx.persist_record(&record)?;
            }
            servers.push(server);
        }
        Ok(servers)
    }

    pub async fn query_servers(&self, query: ServerQuery) -> Result<Vec<SnowRecord>> {
        self.ctx.query.query_servers(query).await
    }

    /// Execute the fixed typed Server query against ServiceNow. This is the
    /// live counterpart to the narrowed local projection used by cache modes.
    pub async fn query_servers_policy_live(
        &self,
        query: ServerQuery,
        persist: bool,
    ) -> Result<Vec<Server>> {
        query.validate()?;
        let limit = query.limit.unwrap_or(SERVER_DEFAULT_LIMIT);
        let offset = query.offset.unwrap_or(0);
        let mut api = self
            .server_query_base_query(&query)?
            .limit(u32::try_from(limit).unwrap_or(u32::MAX))
            .offset(u32::try_from(offset).unwrap_or(u32::MAX));

        if let Some(text) = non_empty_owned(query.text.as_deref()) {
            api = api
                .contains("name", &text)
                .or_filter("short_description", Operator::Contains, &text)
                .or_filter("ip_address", Operator::Contains, &text);
        }

        let records = api.execute().await?.records;
        let mut servers = Vec::with_capacity(records.len());
        for record in records {
            let server = Server::from_servicenow(&record)?;
            if persist {
                self.ctx.persist_record(&record)?;
            }
            servers.push(server);
        }
        Ok(servers)
    }

    pub async fn business_application_servers_cached(
        &self,
        params: BusinessApplicationServersCachedParams,
    ) -> Result<Option<BusinessApplicationServersCachedResult>> {
        self.ctx
            .query
            .business_application_servers_cached(params)
            .await
    }

    pub async fn business_applications_for_server(
        &self,
        params: BusinessApplicationsForServerParams,
    ) -> Result<Option<BusinessApplicationsForServerResult>> {
        self.ctx
            .query
            .business_applications_for_server(params)
            .await
    }

    fn server_base_query(&self, params: ServerSearchParams) -> Result<TableApi> {
        let limit = params.validated_limit()?;
        let query_params = ServerQuery {
            name: params.name,
            ip_address: params.ip_address,
            ci_owner_group: params.ci_owner_group,
            class: params.class,
            ..Default::default()
        };
        Ok(self
            .server_query_base_query(&query_params)?
            .limit(u32::try_from(limit).unwrap_or(u32::MAX)))
    }

    fn server_query_base_query(&self, params: &ServerQuery) -> Result<TableApi> {
        let mut query = self
            .ctx
            .client
            .table(SERVER_TABLE)
            .display_value(DisplayValue::Both)
            .exclude_reference_link(true)
            .order_by("name", Order::Asc);

        if let Some(class) = non_empty_owned(params.class.as_deref()) {
            let class = canonical_server_class(&class)?;
            if class != SERVER_TABLE {
                query = query.equals("sys_class_name", class);
            } else {
                query = query.in_list("sys_class_name", SERVER_LEAF_TABLES);
            }
        } else {
            query = query.in_list("sys_class_name", SERVER_LEAF_TABLES);
        }
        if let Some(name) = non_empty_owned(params.name.as_deref()) {
            query = query.contains("name", &name);
        }
        if let Some(ip_address) = non_empty_owned(params.ip_address.as_deref()) {
            query = query.equals("ip_address", &ip_address);
        }
        query = apply_reference_name_or_sys_id_filter(
            query,
            "managed_by_group",
            params.ci_owner_group.as_deref(),
        )?;
        Ok(query)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{core_for_mock_server, mock_server_test_lock};
    use crate::{ResourceType, SnowCore};
    use servicenow_rs::prelude::{BasicAuth, ServiceNowClient};
    use tempfile::TempDir;
    use wiremock::matchers::{method, path, query_param};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[tokio::test]
    async fn server_search_builds_supported_filters() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [{
                    "sys_id": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
                    "name": "app01.example.internal",
                    "ip_address": "192.0.2.10",
                    "sys_class_name": "cmdb_ci_linux_server",
                    "managed_by_group": {
                        "value": "11111111111111111111111111111111",
                        "display_value": "Platform Operations"
                    },
                    "support_group": {
                        "value": "22222222222222222222222222222222",
                        "display_value": "Server Support"
                    },
                    "operational_status": { "value": "1", "display_value": "Operational" },
                    "short_description": "Linux application server"
                }]
            })))
            .mount(&server)
            .await;

        let (core, tempdir) = core_for_mock_server(&server).await;
        let records = core
            .search_servers(ServerSearchParams {
                name: Some("app01".to_string()),
                ip_address: Some("192.0.2.10".to_string()),
                ci_owner_group: Some("Platform Operations".to_string()),
                class: Some("linux".to_string()),
                limit: Some(3),
            })
            .await
            .expect("servers");

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].number, "SERVER:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert_eq!(records[0].table, "cmdb_ci_server");
        assert_eq!(records[0].resource_type, ResourceType::Server);
        assert_eq!(records[0].fields["name"].value, "app01.example.internal");
        assert_eq!(records[0].fields["ip_address"].value, "192.0.2.10");
        assert_eq!(
            records[0].references["managed_by_group"].display_name,
            "Platform Operations"
        );
        let cached = core
            .query_servers(ServerQuery {
                ci_owner_group: Some("Platform Operations".to_string()),
                ..Default::default()
            })
            .await
            .expect("cached server query by CI owner group");
        assert_eq!(cached.len(), 1);
        assert_eq!(cached[0].sys_id, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert!(
            tempdir
                .path()
                .join("vault/servers/server_aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_app01-example-internal.md")
                .exists()
        );

        let requests = server.received_requests().await.expect("requests");
        let request = requests
            .iter()
            .find(|request| request.url.path() == "/api/now/table/cmdb_ci_server")
            .expect("server request");
        let query = request
            .url
            .query_pairs()
            .collect::<std::collections::HashMap<_, _>>();
        assert_eq!(
            query.get("sysparm_query").map(|value| value.as_ref()),
            Some(
                "sys_class_name=cmdb_ci_linux_server^nameLIKEapp01^ip_address=192.0.2.10^managed_by_group.nameLIKEPlatform Operations^ORDERBYname"
            )
        );
        assert_eq!(
            query.get("sysparm_fields").map(|value| value.as_ref()),
            None
        );
        assert_eq!(
            query
                .get("sysparm_display_value")
                .map(|value| value.as_ref()),
            Some("all")
        );
        assert_eq!(
            query
                .get("sysparm_exclude_reference_link")
                .map(|value| value.as_ref()),
            Some("true")
        );
        assert_eq!(
            query.get("sysparm_limit").map(|value| value.as_ref()),
            Some("3")
        );
    }

    // ----- server_get read-through (live fallback) -----
    //
    // These exercise the shared live path behind the read-through `server_get`
    // (`SnowCore::get_server_live`). All fixtures use RFC-5737 documentation IPs
    // and placeholder sys_ids/hostnames only.

    fn server_result_body(sys_id: &str, name: &str, ip: &str) -> serde_json::Value {
        server_result_body_with_class(sys_id, name, ip, "cmdb_ci_linux_server")
    }

    fn server_result_body_with_class(
        sys_id: &str,
        name: &str,
        ip: &str,
        class_name: &str,
    ) -> serde_json::Value {
        serde_json::json!({
            "result": [{
                "sys_id": sys_id,
                "name": name,
                "ip_address": ip,
                "sys_class_name": class_name,
                "operational_status": { "value": "1", "display_value": "Operational" }
            }]
        })
    }

    #[tokio::test]
    async fn server_get_live_by_name_cache_miss_hit_persists_and_caches() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let sys_id = "cccccccccccccccccccccccccccccccc";
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param("sysparm_limit", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(server_result_body(
                sys_id,
                "host01.example.internal",
                "192.0.2.20",
            )))
            .expect(1)
            .mount(&server)
            .await;

        let (core, tempdir) = core_for_mock_server(&server).await;
        let found = core
            .get_server_live(ServerLookup::exact_name("host01.example.internal"), true)
            .await
            .expect("live hit");
        let found = found.expect("some server");
        assert_eq!(found.record.sys_id, sys_id);

        // Persisted to cache: a subsequent cached query resolves it locally.
        let cached = core
            .query_servers(ServerQuery {
                name: Some("host01".to_string()),
                ..Default::default()
            })
            .await
            .expect("cached query");
        assert_eq!(cached.len(), 1);
        assert_eq!(cached[0].sys_id, sys_id);
        assert!(
            tempdir
                .path()
                .join("vault/servers")
                .read_dir()
                .map(|mut d| d.next().is_some())
                .unwrap_or(false)
        );
    }

    #[tokio::test]
    async fn server_get_live_by_name_queries_base_table_without_leaf_class_filter() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let sys_id = "cacacacacacacacacacacacacacacaca";
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param("sysparm_limit", "2"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(server_result_body_with_class(
                    sys_id,
                    "esx01.example.internal",
                    "192.0.2.24",
                    "cmdb_ci_esx_server",
                )),
            )
            .expect(1)
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let found = core
            .get_server_live(ServerLookup::exact_name("esx01.example.internal"), false)
            .await
            .expect("live hit")
            .expect("some server");
        assert_eq!(found.record.sys_id, sys_id);
        assert_eq!(found.class_name.as_deref(), Some("cmdb_ci_esx_server"));

        let requests = server.received_requests().await.expect("requests");
        let request = requests
            .iter()
            .find(|request| request.url.path() == "/api/now/table/cmdb_ci_server")
            .expect("server request");
        let query = request
            .url
            .query_pairs()
            .collect::<std::collections::HashMap<_, _>>();
        let sysparm_query = query
            .get("sysparm_query")
            .map(|value| value.as_ref())
            .expect("sysparm_query");
        assert_eq!(sysparm_query, "name=esx01.example.internal^ORDERBYname");
        assert!(!sysparm_query.contains("sys_class_name"));
    }

    #[tokio::test]
    async fn server_get_live_by_sys_id_cache_miss_hit() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let sys_id = "dddddddddddddddddddddddddddddddd";
        Mock::given(method("GET"))
            .and(path(format!("/api/now/table/cmdb_ci_server/{sys_id}")))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": {
                    "sys_id": sys_id,
                    "name": "host02.example.internal",
                    "ip_address": "192.0.2.21",
                    "sys_class_name": "cmdb_ci_linux_server",
                    "operational_status": { "value": "1", "display_value": "Operational" }
                }
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let found = core
            .get_server_live(ServerLookup::sys_id(sys_id).expect("sys_id"), true)
            .await
            .expect("live hit")
            .expect("some server");
        assert_eq!(found.record.sys_id, sys_id);
    }

    #[tokio::test]
    async fn server_get_live_by_ip_cache_miss_hit() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let sys_id = "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee";
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .and(query_param("sysparm_limit", "2"))
            .respond_with(ResponseTemplate::new(200).set_body_json(server_result_body(
                sys_id,
                "host03.example.internal",
                "192.0.2.22",
            )))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let found = core
            .get_server_live(ServerLookup::ip_address("192.0.2.22"), true)
            .await
            .expect("live hit")
            .expect("some server");
        assert_eq!(found.record.sys_id, sys_id);
    }

    #[tokio::test]
    async fn server_get_live_by_name_empty_result_is_not_found() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .respond_with(
                ResponseTemplate::new(200).set_body_json(serde_json::json!({ "result": [] })),
            )
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let err = core
            .get_server_live(ServerLookup::exact_name("ghost.example.internal"), true)
            .await
            .expect_err("not found");
        assert_eq!(err, ServerGetError::NotFound);
    }

    #[tokio::test]
    async fn server_get_live_by_sys_id_404_is_not_found() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let sys_id = "ffffffffffffffffffffffffffffffff";
        Mock::given(method("GET"))
            .and(path(format!("/api/now/table/cmdb_ci_server/{sys_id}")))
            .respond_with(ResponseTemplate::new(404).set_body_json(serde_json::json!({
                "error": { "message": "No record found" }
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let err = core
            .get_server_live(ServerLookup::sys_id(sys_id).expect("sys_id"), true)
            .await
            .expect_err("not found");
        assert_eq!(err, ServerGetError::NotFound);
    }

    #[tokio::test]
    async fn server_get_live_acl_403_is_acl_restricted_not_not_found() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .respond_with(ResponseTemplate::new(403).set_body_json(serde_json::json!({
                "error": { "message": "Insufficient rights" }
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let err = core
            .get_server_live(ServerLookup::exact_name("locked.example.internal"), true)
            .await
            .expect_err("acl");
        assert!(
            matches!(err, ServerGetError::AclRestricted(_)),
            "expected AclRestricted, got {err:?}"
        );
    }

    #[tokio::test]
    async fn server_get_live_network_failure_is_network_not_not_found() {
        let _guard = mock_server_test_lock().await;
        // Point at an address that refuses connections (127.0.0.1:1 is the
        // reserved tcpmux port, effectively always closed for our purposes) so
        // the transport call fails at the connection layer -> network error,
        // never a not-found.
        let client = ServiceNowClient::builder()
            .instance("http://127.0.0.1:1")
            .auth(BasicAuth::new("test_user", "test_pass"))
            .allow_http()
            .build()
            .await
            .expect("client");
        let tempdir = TempDir::new().expect("tempdir");
        let core = SnowCore::builder()
            .client(client)
            .vault_path(tempdir.path().join("vault"))
            .build()
            .await
            .expect("core");

        let err = core
            .get_server_live(ServerLookup::exact_name("host04.example.internal"), true)
            .await
            .expect_err("network");
        assert!(
            matches!(err, ServerGetError::Network(_)),
            "expected Network, got {err:?}"
        );
    }

    #[tokio::test]
    async fn server_get_live_duplicate_name_is_disambiguation() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .respond_with(ResponseTemplate::new(200).set_body_json(serde_json::json!({
                "result": [
                    {
                        "sys_id": "11111111111111111111111111111111",
                        "name": "dup.example.internal",
                        "sys_class_name": "cmdb_ci_linux_server"
                    },
                    {
                        "sys_id": "22222222222222222222222222222222",
                        "name": "dup.example.internal",
                        "sys_class_name": "cmdb_ci_win_server"
                    }
                ]
            })))
            .mount(&server)
            .await;

        let (core, _tempdir) = core_for_mock_server(&server).await;
        let err = core
            .get_server_live(ServerLookup::exact_name("dup.example.internal"), true)
            .await
            .expect_err("disambiguation");
        match err {
            ServerGetError::Disambiguation { selector, matched } => {
                assert_eq!(selector, "name=dup.example.internal");
                assert_eq!(matched, 2);
            }
            other => panic!("expected Disambiguation, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn server_get_live_no_persist_does_not_write_cache() {
        let _guard = mock_server_test_lock().await;
        let server = MockServer::start().await;
        let sys_id = "99999999999999999999999999999999";
        Mock::given(method("GET"))
            .and(path("/api/now/table/cmdb_ci_server"))
            .respond_with(ResponseTemplate::new(200).set_body_json(server_result_body(
                sys_id,
                "host05.example.internal",
                "192.0.2.30",
            )))
            .mount(&server)
            .await;

        let (core, tempdir) = core_for_mock_server(&server).await;
        let found = core
            .get_server_live(ServerLookup::exact_name("host05.example.internal"), false)
            .await
            .expect("live hit")
            .expect("some server");
        assert_eq!(found.record.sys_id, sys_id);

        // persist = false: nothing written to the local cache.
        let servers_dir = tempdir.path().join("vault/servers");
        let has_entries = servers_dir
            .read_dir()
            .map(|mut d| d.next().is_some())
            .unwrap_or(false);
        assert!(!has_entries, "MCP no-persist path must not write the cache");
        let cached = core
            .query_servers(ServerQuery {
                name: Some("host05".to_string()),
                ..Default::default()
            })
            .await
            .expect("cached query");
        assert!(cached.is_empty(), "no-persist record must not be cached");
    }
}
